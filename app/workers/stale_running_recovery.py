from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from app.application.dto import JobExecutionDTO
from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.db.uow import UnitOfWork, UnitOfWorkFactory
from app.domain.enums import JobStatus
from app.workers.post_commit import (
    JobPostCommitNotification,
    NoOpWorkerPostCommitNotifier,
    WorkerPostCommitNotifier,
)

logger = logging.getLogger(__name__)

RECOVERY_FINAL_FAILURE_MESSAGE = "Job failed after stale running recovery exhausted retries"
RECOVERY_REQUEUE_MESSAGE = "Job recovered from stale running state and queued for retry"


class StaleRunningRecovery:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        settings: Settings,
        *,
        clock: Clock | None = None,
        post_commit_notifier: WorkerPostCommitNotifier | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._settings = settings
        self._clock = clock or UtcClock()
        self._post_commit_notifier = post_commit_notifier or NoOpWorkerPostCommitNotifier()

    async def recover_due_batch_once(self) -> int:
        now = self._clock.now()
        notifications: list[JobPostCommitNotification] = []
        async with self._uow_factory() as uow:
            stale_jobs = await uow.jobs.claim_expired_running_jobs_for_update(
                now,
                limit=self._settings.recovery_batch_size,
            )

            recovered_count = 0
            for stale_job in stale_jobs:
                notification = await self._recover_stale_job(uow=uow, job=stale_job, now=now)
                if notification is not None:
                    recovered_count += 1
                    notifications.append(notification)

            if recovered_count > 0:
                await uow.commit()

        for notification in notifications:
            await self._notify_post_commit(notification)

        return recovered_count

    async def run_forever(self, *, stop_event: asyncio.Event | None = None) -> None:
        while stop_event is None or not stop_event.is_set():
            recovered_count = await self.recover_due_batch_once()
            if recovered_count == 0:
                if stop_event is None:
                    await asyncio.sleep(self._settings.recovery_poll_interval_seconds)
                else:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(),
                            timeout=self._settings.recovery_poll_interval_seconds,
                        )
                    except TimeoutError:
                        continue

    async def _recover_stale_job(
        self,
        *,
        uow: UnitOfWork,
        job: JobExecutionDTO,
        now: datetime,
    ) -> JobPostCommitNotification | None:
        if job.execution_token is None:
            return None

        owner = await uow.users.lock_by_id(job.owner_id)
        if owner is None:
            return None

        next_attempt_number = job.attempt_count + 1
        if next_attempt_number > self._max_total_attempts(job):
            failed = await uow.jobs.mark_failed(
                job.id,
                execution_token=job.execution_token,
                now=now,
                error=RECOVERY_FINAL_FAILURE_MESSAGE,
            )
            if not failed:
                return None
            await uow.job_logs.create_system_log(
                job.id,
                level="error",
                message=(
                    f"Job attempt {job.attempt_count} became a final failure during "
                    f"stale running recovery: {RECOVERY_FINAL_FAILURE_MESSAGE}"
                ),
            )
            return JobPostCommitNotification(
                job_id=job.id,
                owner_id=job.owner_id,
                status=JobStatus.FAILED,
                attempt_count=job.attempt_count,
                committed_at=now,
            )

        requeued = await uow.jobs.requeue_running(
            job.id,
            execution_token=job.execution_token,
            now=now,
            error=RECOVERY_REQUEUE_MESSAGE,
        )
        if not requeued:
            return None

        await uow.outbox.create_dispatch_event_if_absent(job_id=job.id, available_at=now)
        await uow.job_logs.create_system_log(
            job.id,
            level="warning",
            message=(
                f"Job attempt {job.attempt_count} was recovered from stale running "
                f"state and queued for retry"
            ),
        )
        return JobPostCommitNotification(
            job_id=job.id,
            owner_id=job.owner_id,
            status=JobStatus.QUEUED,
            attempt_count=job.attempt_count,
            committed_at=now,
        )

    @staticmethod
    def _max_total_attempts(job: JobExecutionDTO) -> int:
        return min(job.max_retries + 1, 4)

    async def _notify_post_commit(self, notification: JobPostCommitNotification) -> None:
        try:
            await self._post_commit_notifier.notify_job_state_changed(notification)
        except Exception:
            logger.warning(
                "stale_running_recovery_post_commit_notification_failed",
                extra={
                    "job_id": str(notification.job_id),
                    "owner_id": str(notification.owner_id),
                    "job_status": notification.status.value,
                },
                exc_info=True,
            )
