from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import uuid4

from app.application.caching.job_list_cache import JobListCache, NoOpJobListCache
from app.domain.enums import JobStatus
from app.infrastructure.redis.job_status_pubsub import RedisJobStatusPubSub
from app.schemas.job_events import JobStatusEvent


@dataclass(frozen=True, slots=True)
class JobPostCommitNotification:
    job_id: uuid.UUID
    owner_id: uuid.UUID
    status: JobStatus
    attempt_count: int
    committed_at: datetime
    cancel_requested_at: datetime | None = None


class WorkerPostCommitNotifier(Protocol):
    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None: ...


class NoOpWorkerPostCommitNotifier:
    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
        del notification


class CompositeWorkerPostCommitNotifier:
    def __init__(self, notifiers: tuple[WorkerPostCommitNotifier, ...]) -> None:
        self._notifiers = notifiers

    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
        for notifier in self._notifiers:
            try:
                await notifier.notify_job_state_changed(notification)
            except Exception:
                continue


class CacheInvalidatingWorkerPostCommitNotifier:
    def __init__(self, job_list_cache: JobListCache | None = None) -> None:
        self._job_list_cache = job_list_cache or NoOpJobListCache()

    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
        await self._job_list_cache.invalidate_owner(notification.owner_id)


class RedisPublishingWorkerPostCommitNotifier:
    def __init__(self, job_status_pubsub: RedisJobStatusPubSub) -> None:
        self._job_status_pubsub = job_status_pubsub

    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
        await self._job_status_pubsub.publish(
            JobStatusEvent(
                event_id=uuid4(),
                job_id=notification.job_id,
                status=notification.status,
                attempt_count=notification.attempt_count,
                occurred_at=notification.committed_at,
                cancel_requested_at=notification.cancel_requested_at,
            )
        )
