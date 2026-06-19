from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol

from pydantic import ValidationError

from app.application.dto import JobExecutionDTO
from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.db.uow import UnitOfWorkFactory
from app.domain.enums import JobStatus
from app.schemas.outbox import DispatchOutboxMessage
from app.workers.handlers import (
    JobCancellationRequestedError,
    JobExecutionError,
    JobHandlerRegistry,
    MissingJobHandlerError,
)
from app.workers.post_commit import (
    JobPostCommitNotification,
    NoOpWorkerPostCommitNotifier,
    WorkerPostCommitNotifier,
)

logger = logging.getLogger(__name__)

CAPACITY_DEFER_SECONDS = 3
MAX_JOB_ERROR_LENGTH = 1000
MAX_TOTAL_JOB_ATTEMPTS = 4
RETRY_BACKOFF_SECONDS_BY_ATTEMPT = {
    1: 5,
    2: 15,
    3: 30,
}


class AckableMessage(Protocol):
    body: bytes
    message_id: str | None
    correlation_id: str | None

    async def ack(self) -> None: ...

    async def reject(self, *, requeue: bool = False) -> None: ...


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    job: JobExecutionDTO
    execution_token: uuid.UUID
    attempt_number: int


class JobWorker:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        handler_registry: JobHandlerRegistry,
        settings: Settings,
        *,
        worker_id: str,
        clock: Clock | None = None,
        post_commit_notifier: WorkerPostCommitNotifier | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._handler_registry = handler_registry
        self._settings = settings
        self._worker_id = worker_id
        self._clock = clock or UtcClock()
        self._post_commit_notifier = post_commit_notifier or NoOpWorkerPostCommitNotifier()

    async def handle_message(self, message: AckableMessage) -> None:
        try:
            dispatch_message = DispatchOutboxMessage.model_validate_json(message.body)
        except ValidationError:
            logger.warning(
                "worker_rejected_invalid_dispatch_message",
                extra={
                    "message_id": message.message_id,
                    "correlation_id": message.correlation_id,
                },
            )
            await message.reject(requeue=False)
            return

        claim = await self._claim_execution(dispatch_message)
        if claim is None:
            await message.ack()
            return

        await self._execute_claim(claim)
        await message.ack()

    async def _claim_execution(
        self,
        dispatch_message: DispatchOutboxMessage,
    ) -> ExecutionClaim | None:
        now = self._clock.now()
        execution_token = uuid.uuid4()

        async with self._uow_factory() as uow:
            owner_id = await uow.jobs.get_owner_id(dispatch_message.job_id)
            if owner_id is None:
                logger.info(
                    "worker_ack_missing_job",
                    extra={"job_id": str(dispatch_message.job_id)},
                )
                return None

            user = await uow.users.lock_by_id(owner_id)
            if user is None:
                logger.info(
                    "worker_ack_missing_owner",
                    extra={
                        "job_id": str(dispatch_message.job_id),
                        "owner_id": str(owner_id),
                    },
                )
                return None

            job = await uow.jobs.get_execution_candidate_for_update(dispatch_message.job_id)
            if job is None:
                logger.info(
                    "worker_ack_missing_job_after_owner_lock",
                    extra={"job_id": str(dispatch_message.job_id)},
                )
                return None

            if job.status is JobStatus.PENDING:
                logger.info(
                    "worker_ack_pending_dispatch_message",
                    extra={"job_id": str(job.id)},
                )
                return None

            if job.status is JobStatus.RUNNING:
                logger.info(
                    "worker_ack_duplicate_running_dispatch_message",
                    extra={"job_id": str(job.id)},
                )
                return None

            if job.status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}:
                logger.info(
                    "worker_ack_terminal_dispatch_message",
                    extra={
                        "job_id": str(job.id),
                        "job_status": job.status.value,
                    },
                )
                return None

            next_attempt_number = job.attempt_count + 1
            if next_attempt_number > self._max_total_attempts(job):
                failed = await uow.jobs.mark_failed_without_execution(
                    job.id,
                    current_status=JobStatus.QUEUED,
                    now=now,
                    error="Job exceeded the maximum execution attempts",
                )
                if failed:
                    await uow.job_logs.create_system_log(
                        job.id,
                        level="error",
                        message=(
                            "Job failed before execution because it exceeded the maximum attempts"
                        ),
                    )
                    await uow.commit()
                    await self._notify_post_commit(
                        job_id=job.id,
                        owner_id=job.owner_id,
                        status=JobStatus.FAILED,
                        attempt_count=job.attempt_count,
                        committed_at=now,
                    )
                return None

            running_count = await uow.jobs.count_running_for_owner(owner_id)
            if running_count >= self._settings.job_max_running_per_user:
                defer_until = now + timedelta(seconds=CAPACITY_DEFER_SECONDS)
                deferred_event = await uow.outbox.create_dispatch_event_if_absent(
                    job_id=job.id,
                    available_at=defer_until,
                )
                if deferred_event is not None:
                    await uow.job_logs.create_system_log(
                        job.id,
                        level="info",
                        message=(
                            "Job dispatch deferred because the owner reached the running-job limit"
                        ),
                    )
                await uow.commit()
                await self._notify_post_commit(
                    job_id=job.id,
                    owner_id=job.owner_id,
                    status=JobStatus.QUEUED,
                    attempt_count=job.attempt_count,
                    committed_at=now,
                )
                logger.info(
                    "worker_deferred_for_capacity",
                    extra={
                        "job_id": str(job.id),
                        "owner_id": str(owner_id),
                        "available_at": defer_until.isoformat(),
                    },
                )
                return None

            claimed = await uow.jobs.mark_running(
                job.id,
                worker_id=self._worker_id,
                execution_token=execution_token,
                now=now,
                lease_expires_at=now + timedelta(seconds=self._settings.job_lease_seconds),
            )
            if not claimed:
                logger.info(
                    "worker_ack_claim_race_lost",
                    extra={"job_id": str(job.id)},
                )
                return None

            await uow.job_logs.create_system_log(
                job.id,
                level="info",
                message="Job execution started",
            )
            await uow.commit()
            await self._notify_post_commit(
                job_id=job.id,
                owner_id=job.owner_id,
                status=JobStatus.RUNNING,
                attempt_count=next_attempt_number,
                committed_at=now,
            )
            return ExecutionClaim(
                job=job,
                execution_token=execution_token,
                attempt_number=next_attempt_number,
            )

    async def _execute_claim(self, claim: ExecutionClaim) -> None:
        try:
            handler = self._handler_registry.get(claim.job.type)
            await handler.execute(
                claim.job.payload,
                cancellation_requested=lambda: self._is_cancellation_requested(claim),
            )
        except JobCancellationRequestedError:
            await self._mark_cancelled(claim)
        except MissingJobHandlerError as exc:
            await self._mark_failed(claim, str(exc))
        except JobExecutionError as exc:
            await self._finalize_failure(claim, str(exc))
        except Exception as exc:
            logger.exception(
                "worker_handler_crashed",
                extra={"job_id": str(claim.job.id), "job_type": claim.job.type.value},
            )
            await self._finalize_failure(claim, str(exc))
        else:
            await self._mark_completed(claim)

    async def _is_cancellation_requested(self, claim: ExecutionClaim) -> bool:
        async with self._uow_factory() as uow:
            current_job = await uow.jobs.get_execution_candidate_for_update(claim.job.id)
            if current_job is None:
                return True
            if current_job.execution_token != claim.execution_token:
                return True
            return current_job.cancel_requested_at is not None

    async def _mark_completed(self, claim: ExecutionClaim) -> None:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            completed = await uow.jobs.mark_completed(
                claim.job.id,
                execution_token=claim.execution_token,
                now=now,
            )
            if completed:
                await uow.job_logs.create_system_log(
                    claim.job.id,
                    level="info",
                    message="Job completed",
                )
                await uow.commit()
                await self._notify_post_commit(
                    job_id=claim.job.id,
                    owner_id=claim.job.owner_id,
                    status=JobStatus.COMPLETED,
                    attempt_count=claim.attempt_number,
                    committed_at=now,
                )
                return

        logger.warning(
            "worker_skipped_stale_completion",
            extra={
                "job_id": str(claim.job.id),
                "execution_token": str(claim.execution_token),
            },
        )

    async def _mark_cancelled(self, claim: ExecutionClaim) -> None:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            cancelled = await uow.jobs.mark_cancelled(
                claim.job.id,
                execution_token=claim.execution_token,
                now=now,
            )
            if not cancelled:
                logger.info(
                    "worker_ignored_stale_cancellation_finalize",
                    extra={"job_id": str(claim.job.id)},
                )
                return
            await uow.job_logs.create_system_log(
                claim.job.id,
                level="info",
                message=f"Job cancelled during attempt {claim.attempt_number}",
            )
            await uow.commit()
            await self._notify_post_commit(
                job_id=claim.job.id,
                owner_id=claim.job.owner_id,
                status=JobStatus.CANCELLED,
                attempt_count=claim.attempt_number,
                committed_at=now,
            )

    async def _finalize_failure(self, claim: ExecutionClaim, error: str) -> None:
        if claim.attempt_number >= self._max_total_attempts(claim.job):
            await self._mark_failed(claim, error)
            return
        await self._schedule_retry(claim, error)

    async def _mark_failed(self, claim: ExecutionClaim, error: str) -> None:
        now = self._clock.now()
        error_message = self._truncate_error(error)

        async with self._uow_factory() as uow:
            failed = await uow.jobs.mark_failed(
                claim.job.id,
                execution_token=claim.execution_token,
                now=now,
                error=error_message,
            )
            if failed:
                await uow.job_logs.create_system_log(
                    claim.job.id,
                    level="error",
                    message=(
                        f"Job attempt {claim.attempt_number} became a final failure: "
                        f"{error_message}"
                    ),
                )
                await uow.commit()
                await self._notify_post_commit(
                    job_id=claim.job.id,
                    owner_id=claim.job.owner_id,
                    status=JobStatus.FAILED,
                    attempt_count=claim.attempt_number,
                    committed_at=now,
                )
                return

        logger.warning(
            "worker_skipped_stale_failure",
            extra={
                "job_id": str(claim.job.id),
                "execution_token": str(claim.execution_token),
            },
        )

    async def _schedule_retry(self, claim: ExecutionClaim, error: str) -> None:
        now = self._clock.now()
        error_message = self._truncate_error(error)
        retry_delay_seconds = self._retry_backoff_seconds(claim.attempt_number)
        retry_available_at = now + timedelta(seconds=retry_delay_seconds)

        async with self._uow_factory() as uow:
            requeued = await uow.jobs.requeue_running(
                claim.job.id,
                execution_token=claim.execution_token,
                now=now,
                error=error_message,
            )
            if requeued:
                await uow.outbox.create_dispatch_event_if_absent(
                    job_id=claim.job.id,
                    available_at=retry_available_at,
                )
                await uow.job_logs.create_system_log(
                    claim.job.id,
                    level="warning",
                    message=(
                        f"Job attempt {claim.attempt_number} failed and was queued "
                        f"for retry in {retry_delay_seconds} seconds: {error_message}"
                    ),
                )
                await uow.commit()
                await self._notify_post_commit(
                    job_id=claim.job.id,
                    owner_id=claim.job.owner_id,
                    status=JobStatus.QUEUED,
                    attempt_count=claim.attempt_number,
                    committed_at=now,
                )
                return

        logger.warning(
            "worker_skipped_stale_retry",
            extra={
                "job_id": str(claim.job.id),
                "execution_token": str(claim.execution_token),
            },
        )

    @staticmethod
    def _truncate_error(error: str) -> str:
        if len(error) <= MAX_JOB_ERROR_LENGTH:
            return error
        return error[: MAX_JOB_ERROR_LENGTH - 3] + "..."

    @staticmethod
    def _max_total_attempts(job: JobExecutionDTO) -> int:
        return min(job.max_retries + 1, MAX_TOTAL_JOB_ATTEMPTS)

    @staticmethod
    def _retry_backoff_seconds(attempt_number: int) -> int:
        return RETRY_BACKOFF_SECONDS_BY_ATTEMPT[attempt_number]

    async def _notify_post_commit(
        self,
        *,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
        status: JobStatus,
        attempt_count: int,
        committed_at: datetime,
    ) -> None:
        try:
            await self._post_commit_notifier.notify_job_state_changed(
                JobPostCommitNotification(
                    job_id=job_id,
                    owner_id=owner_id,
                    status=status,
                    attempt_count=attempt_count,
                    committed_at=committed_at,
                )
            )
        except Exception:
            logger.warning(
                "worker_post_commit_notification_failed",
                extra={
                    "job_id": str(job_id),
                    "owner_id": str(owner_id),
                    "job_status": status.value,
                },
                exc_info=True,
            )


MessageConsumer = Callable[[AckableMessage], Awaitable[None]]
