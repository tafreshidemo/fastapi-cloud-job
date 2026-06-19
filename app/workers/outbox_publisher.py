from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Protocol
from uuid import UUID

from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.db.uow import UnitOfWork, UnitOfWorkFactory
from app.domain.enums import JobStatus
from app.models.outbox_event import OutboxEvent
from app.schemas.outbox import DispatchOutboxMessage

logger = logging.getLogger(__name__)

MAX_OUTBOX_BACKOFF_SECONDS = 300
MAX_OUTBOX_ERROR_LENGTH = 1000


class DispatchMessagePublisher(Protocol):
    async def publish_dispatch(
        self,
        *,
        outbox_event: OutboxEvent,
        message: DispatchOutboxMessage,
    ) -> None: ...


class OutboxPublisher:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        dispatch_publisher: DispatchMessagePublisher,
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._dispatch_publisher = dispatch_publisher
        self._settings = settings
        self._clock = clock or UtcClock()

    async def publish_due_batch_once(self) -> int:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            claimed_events = await uow.outbox.claim_due_batch(
                now,
                limit=self._settings.outbox_batch_size,
            )
            claimed_event_ids = [event.id for event in claimed_events]
        processed_count = 0
        for event_id in claimed_event_ids:
            processed = await self._process_claimed_event_once(event_id)
            if processed:
                processed_count += 1
        return processed_count

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            processed_count = await self.publish_due_batch_once()
            if processed_count == 0:
                if stop_event is None:
                    await asyncio.sleep(self._settings.outbox_poll_interval_seconds)
                else:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self._settings.outbox_poll_interval_seconds,
                        )
                    except TimeoutError:
                        continue

    async def _process_claimed_event_once(self, event_id: UUID) -> bool:
        async with self._uow_factory() as uow:
            locked_event = await uow.outbox.get_by_id_for_update_unpublished(event_id)
            if locked_event is None:
                return False

            job = await uow.jobs.get_by_id_for_update(locked_event.aggregate_id)
            if job is None:
                logger.info(
                    "outbox_event_discarded_missing_job",
                    extra={
                        "event_id": str(locked_event.id),
                        "job_id": str(locked_event.aggregate_id),
                    },
                )
                await uow.outbox.mark_published(locked_event.id, self._clock.now())
                await uow.commit()
                return True

            if job.status not in {JobStatus.PENDING, JobStatus.QUEUED}:
                await self._discard_non_dispatchable_event(
                    uow=uow,
                    event=locked_event,
                    status=job.status,
                )
                await uow.commit()
                return True

            try:
                message = DispatchOutboxMessage.model_validate(locked_event.payload)
                if job.status is JobStatus.PENDING:
                    queued = await uow.jobs.mark_queued(job.id, self._clock.now())
                    if not queued:
                        await self._discard_non_dispatchable_event(
                            uow=uow,
                            event=locked_event,
                            status=job.status,
                        )
                        await uow.commit()
                        return True
                await self._dispatch_publisher.publish_dispatch(
                    outbox_event=locked_event,
                    message=message,
                )
            except Exception as exc:
                await self._record_failure_after_rollback(
                    uow=uow,
                    event_id=locked_event.id,
                    event_type=locked_event.event_type,
                    publish_attempts=locked_event.publish_attempts,
                    error=str(exc),
                )
                return True

            await uow.outbox.mark_published(locked_event.id, self._clock.now())
            await uow.commit()
            return True

    async def _discard_non_dispatchable_event(
        self,
        *,
        uow: UnitOfWork,
        event: OutboxEvent,
        status: JobStatus,
    ) -> None:
        await uow.job_logs.create_system_log(
            event.aggregate_id,
            level="info",
            message=f"Dispatch skipped because job is not dispatchable (status={status.value})",
        )
        logger.info(
            "outbox_event_discarded",
            extra={
                "event_id": str(event.id),
                "job_id": str(event.aggregate_id),
                "job_status": status.value,
            },
        )
        await uow.outbox.mark_published(event.id, self._clock.now())

    async def _record_failure_after_rollback(
        self,
        *,
        uow: UnitOfWork,
        event_id: UUID,
        event_type: str,
        publish_attempts: int,
        error: str,
    ) -> None:
        await uow.rollback()
        next_available_at = self._clock.now() + timedelta(
            seconds=self._compute_retry_delay_seconds(publish_attempts + 1)
        )
        error_message = self._truncate_error(error)
        logger.warning(
            "outbox_publish_failed",
            extra={
                "event_id": str(event_id),
                "event_type": event_type,
                "publish_attempts": publish_attempts + 1,
                "next_available_at": next_available_at.isoformat(),
            },
        )
        async with self._uow_factory() as failure_uow:
            await failure_uow.outbox.record_publish_failure(
                event_id,
                error=error_message,
                next_available_at=next_available_at,
            )
            await failure_uow.commit()

    @staticmethod
    def _truncate_error(error: str) -> str:
        if len(error) <= MAX_OUTBOX_ERROR_LENGTH:
            return error
        return error[: MAX_OUTBOX_ERROR_LENGTH - 3] + "..."

    @staticmethod
    # Keep publisher retries quick at first, but cap the delay so a bad message does not disappear for too long.
    def _compute_retry_delay_seconds(next_attempt_number: int) -> int:
        return int(min(2 ** max(next_attempt_number - 1, 0), MAX_OUTBOX_BACKOFF_SECONDS))
