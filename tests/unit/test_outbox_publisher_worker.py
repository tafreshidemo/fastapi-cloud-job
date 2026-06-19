from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Self, cast

import pytest

from app.application.dto import StoredJobDTO
from app.core.config import Settings
from app.db.uow import UnitOfWorkFactory
from app.domain.enums import JobStatus, JobType
from app.models.outbox_event import OutboxEvent
from app.workers.outbox_publisher import OutboxPublisher


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class FakeDispatchPublisher:
    def __init__(self, *, error: Exception | None = None) -> None:
        self._error = error
        self.publish_calls: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def publish_dispatch(self, *, outbox_event: OutboxEvent, message) -> None:  # type: ignore[no-untyped-def]
        if self._error is not None:
            raise self._error
        self.publish_calls.append((outbox_event.id, message.model_dump(mode="json")))


class FakeOutboxRepository:
    def __init__(self, event: OutboxEvent | None) -> None:
        self._claimed_event = event
        self._locked_event = event
        self.claimed_batch_limits: list[int] = []
        self.marked_published: list[uuid.UUID] = []
        self.recorded_failures: list[tuple[uuid.UUID, str, datetime]] = []

    async def claim_due_batch(self, now: datetime, limit: int) -> list[OutboxEvent]:
        self.claimed_batch_limits.append(limit)
        if (
            self._claimed_event is None
            or self._claimed_event.available_at > now
            or self._claimed_event.published_at is not None
        ):
            return []
        event = self._claimed_event
        self._claimed_event = None
        return [event]

    async def get_by_id_for_update_unpublished(self, event_id: uuid.UUID) -> OutboxEvent | None:
        if self._locked_event is None:
            return None
        if self._locked_event.id != event_id or self._locked_event.published_at is not None:
            return None
        event = self._locked_event
        self._locked_event = None
        return event

    async def mark_published(self, event_id: uuid.UUID, now: datetime) -> None:
        del now
        self.marked_published.append(event_id)

    async def record_publish_failure(
        self,
        event_id: uuid.UUID,
        error: str,
        next_available_at: datetime,
    ) -> None:
        self.recorded_failures.append((event_id, error, next_available_at))


class FakeJobRepository:
    def __init__(self, job: StoredJobDTO | None, *, queue_transition_allowed: bool = True) -> None:
        self._job = job
        self._queue_transition_allowed = queue_transition_allowed
        self.mark_queued_calls: list[uuid.UUID] = []

    async def get_by_id_for_update(self, job_id: uuid.UUID) -> StoredJobDTO | None:
        if self._job is None or self._job.id != job_id:
            return None
        return self._job

    async def mark_queued(self, job_id: uuid.UUID, now: datetime) -> bool:
        del now
        self.mark_queued_calls.append(job_id)
        return self._queue_transition_allowed


class FakeJobLogRepository:
    def __init__(self) -> None:
        self.logs: list[tuple[uuid.UUID, str, str]] = []

    async def create_system_log(self, job_id: uuid.UUID, *, level: str, message: str) -> None:
        self.logs.append((job_id, level, message))


class FakeUoW:
    def __init__(
        self,
        *,
        outbox: FakeOutboxRepository,
        jobs: FakeJobRepository,
        job_logs: FakeJobLogRepository,
        commit_error: Exception | None = None,
    ) -> None:
        self.outbox = outbox
        self.jobs = jobs
        self.job_logs = job_logs
        self.commit_calls = 0
        self.rollback_calls = 0
        self._commit_error = commit_error

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb

    async def commit(self) -> None:
        self.commit_calls += 1
        if self._commit_error is not None:
            raise self._commit_error

    async def rollback(self) -> None:
        self.rollback_calls += 1


def build_settings() -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job",
        REDIS_URL="redis://127.0.0.1:6379/0",
        RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/",
        JWT_SECRET="x" * 32,
        OUTBOX_BATCH_SIZE=1,
        OUTBOX_POLL_INTERVAL_SECONDS=1,
    )


def build_event(now: datetime) -> OutboxEvent:
    event = OutboxEvent(
        aggregate_id=uuid.uuid4(),
        event_type="job.dispatch",
        payload={},
        available_at=now - timedelta(seconds=1),
        publish_attempts=0,
    )
    event.id = uuid.uuid4()
    event.payload = {
        "event_id": str(event.id),
        "job_id": str(event.aggregate_id),
        "kind": "dispatch",
        "created_at": now.isoformat(),
    }
    return event


def build_job(job_id: uuid.UUID, *, status: JobStatus) -> StoredJobDTO:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    return StoredJobDTO(
        id=job_id,
        owner_id=uuid.uuid4(),
        type=JobType.SLEEP,
        payload={"duration_seconds": 5},
        status=status,
        idempotency_key="idem",
        request_hash="a" * 64,
        created_at=now,
        updated_at=now,
    )


def build_publisher(
    processing_uow: FakeUoW,
    *,
    dispatch_publisher: FakeDispatchPublisher,
    failure_uow: FakeUoW | None = None,
    now: datetime,
) -> tuple[OutboxPublisher, list[FakeUoW]]:
    claim_uow = FakeUoW(
        outbox=processing_uow.outbox,
        jobs=processing_uow.jobs,
        job_logs=processing_uow.job_logs,
    )
    uows = [claim_uow, processing_uow]
    if failure_uow is not None:
        uows.append(failure_uow)

    def uow_factory() -> FakeUoW:
        return uows.pop(0)

    publisher = OutboxPublisher(
        uow_factory=cast(UnitOfWorkFactory, uow_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(),
        clock=FixedClock(now),
    )
    tracked_uows = [claim_uow, processing_uow]
    if failure_uow is not None:
        tracked_uows.append(failure_uow)
    return publisher, tracked_uows


@pytest.mark.asyncio
async def test_publisher_confirm_is_required_before_marking_event_published() -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    event = build_event(now)
    primary_uow = FakeUoW(
        outbox=FakeOutboxRepository(event),
        jobs=FakeJobRepository(build_job(event.aggregate_id, status=JobStatus.PENDING)),
        job_logs=FakeJobLogRepository(),
    )
    dispatch_publisher = FakeDispatchPublisher()
    publisher, _uows = build_publisher(primary_uow, dispatch_publisher=dispatch_publisher, now=now)

    await publisher.publish_due_batch_once()

    assert dispatch_publisher.publish_calls == [
        (
            event.id,
            {
                "event_id": str(event.id),
                "job_id": str(event.aggregate_id),
                "kind": "dispatch",
                "created_at": "2026-06-18T12:00:00Z",
            },
        )
    ]
    assert primary_uow.jobs.mark_queued_calls == [event.aggregate_id]
    assert primary_uow.outbox.claimed_batch_limits == [1]
    assert primary_uow.outbox.marked_published == [event.id]


@pytest.mark.asyncio
async def test_publish_failure_rolls_back_staged_state_and_records_retryable_failure() -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    event = build_event(now)
    primary_uow = FakeUoW(
        outbox=FakeOutboxRepository(event),
        jobs=FakeJobRepository(build_job(event.aggregate_id, status=JobStatus.PENDING)),
        job_logs=FakeJobLogRepository(),
    )
    failure_uow = FakeUoW(
        outbox=FakeOutboxRepository(None),
        jobs=FakeJobRepository(None),
        job_logs=FakeJobLogRepository(),
    )
    dispatch_publisher = FakeDispatchPublisher(error=RuntimeError("rabbit unavailable"))
    publisher, uows = build_publisher(
        primary_uow,
        dispatch_publisher=dispatch_publisher,
        failure_uow=failure_uow,
        now=now,
    )

    await publisher.publish_due_batch_once()

    assert primary_uow.jobs.mark_queued_calls == [event.aggregate_id]
    assert primary_uow.outbox.claimed_batch_limits == [1]
    assert primary_uow.rollback_calls == 1
    assert primary_uow.outbox.marked_published == []
    assert failure_uow.outbox.recorded_failures == [
        (event.id, "rabbit unavailable", now + timedelta(seconds=1))
    ]


@pytest.mark.asyncio
async def test_cancelled_or_terminal_job_event_is_discarded_without_publish() -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    event = build_event(now)
    primary_uow = FakeUoW(
        outbox=FakeOutboxRepository(event),
        jobs=FakeJobRepository(build_job(event.aggregate_id, status=JobStatus.CANCELLED)),
        job_logs=FakeJobLogRepository(),
    )
    dispatch_publisher = FakeDispatchPublisher()
    publisher, _uows = build_publisher(primary_uow, dispatch_publisher=dispatch_publisher, now=now)

    await publisher.publish_due_batch_once()

    assert dispatch_publisher.publish_calls == []
    assert primary_uow.jobs.mark_queued_calls == []
    assert primary_uow.outbox.claimed_batch_limits == [1]
    assert primary_uow.outbox.marked_published == [event.id]
    assert primary_uow.job_logs.logs == [
        (
            event.aggregate_id,
            "info",
            "Dispatch skipped because job is not dispatchable (status=cancelled)",
        )
    ]


@pytest.mark.asyncio
async def test_commit_failure_after_publish_leaves_no_invalid_state_recorded() -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    event = build_event(now)
    primary_uow = FakeUoW(
        outbox=FakeOutboxRepository(event),
        jobs=FakeJobRepository(build_job(event.aggregate_id, status=JobStatus.PENDING)),
        job_logs=FakeJobLogRepository(),
        commit_error=RuntimeError("commit failed"),
    )
    dispatch_publisher = FakeDispatchPublisher()
    publisher, _uows = build_publisher(primary_uow, dispatch_publisher=dispatch_publisher, now=now)

    with pytest.raises(RuntimeError, match="commit failed"):
        await publisher.publish_due_batch_once()

    assert dispatch_publisher.publish_calls == [
        (
            event.id,
            {
                "event_id": str(event.id),
                "job_id": str(event.aggregate_id),
                "kind": "dispatch",
                "created_at": "2026-06-18T12:00:00Z",
            },
        )
    ]
    assert primary_uow.jobs.mark_queued_calls == [event.aggregate_id]
    assert primary_uow.outbox.claimed_batch_limits == [1]
    assert primary_uow.outbox.marked_published == [event.id]
    assert primary_uow.rollback_calls == 0


@pytest.mark.asyncio
async def test_missing_job_event_is_discarded_without_retry() -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    event = build_event(now)
    primary_uow = FakeUoW(
        outbox=FakeOutboxRepository(event),
        jobs=FakeJobRepository(None),
        job_logs=FakeJobLogRepository(),
    )
    dispatch_publisher = FakeDispatchPublisher()
    publisher, _uows = build_publisher(primary_uow, dispatch_publisher=dispatch_publisher, now=now)

    await publisher.publish_due_batch_once()

    assert dispatch_publisher.publish_calls == []
    assert primary_uow.outbox.marked_published == [event.id]
    assert primary_uow.outbox.recorded_failures == []
