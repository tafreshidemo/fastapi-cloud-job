from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
import pytest
from alembic.config import Config
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from alembic import command
from app.core.config import Settings
from app.db.session import create_session_factory
from app.db.uow import SqlAlchemyUnitOfWork, create_uow_factory
from app.domain.enums import JobStatus, JobType, UserRole
from app.models.job import Job
from app.models.outbox_event import OutboxEvent
from app.models.user import User
from app.schemas.outbox import DispatchOutboxMessage
from app.workers.handlers import JobHandlerRegistry, build_job_handler_registry
from app.workers.job_worker import CAPACITY_DEFER_SECONDS, JobWorker
from app.workers.post_commit import JobPostCommitNotification, WorkerPostCommitNotifier
from app.workers.stale_running_recovery import StaleRunningRecovery
from tests.support import (
    get_test_database_url,
    get_test_rabbitmq_url,
    get_test_redis_url,
    reset_test_database,
)

TEST_DATABASE_URL = get_test_database_url()


class FixedClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now


class FakeIncomingMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.message_id: str | None = None
        self.correlation_id: str | None = None
        self.ack_calls = 0
        self.reject_calls: list[bool] = []

    async def ack(self) -> None:
        self.ack_calls += 1

    async def reject(self, *, requeue: bool = False) -> None:
        self.reject_calls.append(requeue)


class BlockingSuccessHandler:
    job_type = "success"

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.execute_calls = 0

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        del payload
        self.execute_calls += 1
        self.started.set()
        await self.release.wait()


class ImmediateSuccessHandler:
    job_type = "success"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        del payload


class ImmediateFailureHandler:
    job_type = "failure"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        message = payload.get("message", "boom")
        raise RuntimeError(str(message))


class RecordingPostCommitNotifier:
    def __init__(self, *, error: Exception | None = None) -> None:
        self._error = error
        self.notifications: list[JobPostCommitNotification] = []

    async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
        self.notifications.append(notification)
        if self._error is not None:
            raise self._error


@pytest.fixture
def database_url() -> str:
    return TEST_DATABASE_URL


@pytest.fixture
def alembic_config(monkeypatch: pytest.MonkeyPatch, database_url: str) -> Iterator[Config]:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("REDIS_URL", get_test_redis_url())
    monkeypatch.setenv("RABBITMQ_URL", get_test_rabbitmq_url())
    monkeypatch.setenv("JWT_SECRET", "x" * 32)

    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    yield config


@pytest.fixture
def session_factory(
    alembic_config: Config,
    database_url: str,
) -> Iterator[async_sessionmaker[AsyncSession]]:
    asyncio.run(reset_test_database(database_url))
    command.upgrade(alembic_config, "head")
    engine = create_async_engine(database_url)
    try:
        yield create_session_factory(engine)
    finally:
        asyncio.run(engine.dispose())
def build_settings(database_url: str) -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL=database_url,
        REDIS_URL=get_test_redis_url(),
        RABBITMQ_URL=get_test_rabbitmq_url(),
        JWT_SECRET="x" * 32,
        JOB_MAX_RUNNING_PER_USER=3,
        JOB_LEASE_SECONDS=60,
    )


async def create_owner(session_factory: async_sessionmaker[AsyncSession]) -> User:
    owner = User(
        email=f"{uuid.uuid4()}@example.com",
        password_hash="hashed-password",
        role=UserRole.USER.value,
        is_active=True,
    )
    async with session_factory() as session:
        session.add(owner)
        await session.commit()
        await session.refresh(owner)
    return owner


async def create_job(
    session_factory: async_sessionmaker[AsyncSession],
    owner_id: uuid.UUID,
    *,
    job_type: str = "success",
    payload: dict[str, object] | None = None,
    status: JobStatus = JobStatus.QUEUED,
    execution_token: uuid.UUID | None = None,
) -> Job:
    job = Job(
        owner_id=owner_id,
        type=job_type,
        payload=payload or {},
        status=status.value,
        idempotency_key=f"idem-{uuid.uuid4()}",
        request_hash="a" * 64,
        attempt_count=0,
        max_retries=3,
        execution_token=execution_token,
        worker_id="worker-existing" if status is JobStatus.RUNNING else None,
        lease_expires_at=(
            datetime(2026, 6, 18, 12, 5, tzinfo=UTC) if status is JobStatus.RUNNING else None
        ),
        started_at=(
            datetime(2026, 6, 18, 12, 0, tzinfo=UTC) if status is JobStatus.RUNNING else None
        ),
    )
    async with session_factory() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


async def fetch_job(session_factory: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> Job:
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def fetch_jobs_for_owner(
    session_factory: async_sessionmaker[AsyncSession],
    owner_id: uuid.UUID,
) -> list[Job]:
    async with session_factory() as session:
        jobs = await session.scalars(
            select(Job).where(Job.owner_id == owner_id).order_by(Job.created_at.asc(), Job.id.asc())
        )
        return list(jobs)


async def fetch_logs(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID,
) -> list[str]:
    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        logs = await uow.job_logs.list_entries_for_job(job_id)
    return [log.message for log in logs]


async def count_outbox_events(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID,
) -> list[OutboxEvent]:
    async with session_factory() as session:
        events = await session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.aggregate_id == job_id)
            .order_by(OutboxEvent.created_at.asc(), OutboxEvent.id.asc())
        )
        return list(events)


async def fetch_unpublished_outbox_events(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID,
) -> list[OutboxEvent]:
    async with session_factory() as session:
        events = await session.scalars(
            select(OutboxEvent)
            .where(
                OutboxEvent.aggregate_id == job_id,
                OutboxEvent.published_at.is_(None),
            )
            .order_by(
                OutboxEvent.available_at.asc(),
                OutboxEvent.created_at.asc(),
                OutboxEvent.id.asc(),
            )
        )
        return list(events)


async def request_job_cancellation(
    session_factory: async_sessionmaker[AsyncSession],
    job_id: uuid.UUID,
    *,
    requested_at: datetime,
) -> None:
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        job.cancel_requested_at = requested_at
        await session.commit()


def build_message(job_id: uuid.UUID, *, created_at: datetime) -> FakeIncomingMessage:
    body = (
        DispatchOutboxMessage(
            event_id=uuid.uuid4(),
            job_id=job_id,
            kind="dispatch",
            created_at=created_at,
        )
        .model_dump_json()
        .encode("utf-8")
    )
    return FakeIncomingMessage(body)


def build_worker(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    *,
    now: datetime,
    handler_registry: JobHandlerRegistry | None = None,
    post_commit_notifier: WorkerPostCommitNotifier | None = None,
) -> JobWorker:
    return JobWorker(
        uow_factory=create_uow_factory(session_factory),
        handler_registry=handler_registry or build_job_handler_registry(),
        settings=build_settings(database_url),
        worker_id="worker-a",
        clock=FixedClock(now),
        post_commit_notifier=post_commit_notifier,
    )


async def wait_for_running_count(
    session_factory: async_sessionmaker[AsyncSession],
    owner_id: uuid.UUID,
    expected_count: int,
) -> None:
    for _ in range(100):
        jobs = await fetch_jobs_for_owner(session_factory, owner_id)
        running_count = sum(1 for job in jobs if job.status == JobStatus.RUNNING.value)
        if running_count == expected_count:
            return
        await asyncio.sleep(0.02)
    raise AssertionError(f"Owner {owner_id} did not reach running_count={expected_count}")


@pytest.mark.asyncio
async def test_worker_completes_queued_job_and_acks_message(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")
    message = build_message(job.id, created_at=now)
    notifier = RecordingPostCommitNotifier()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        post_commit_notifier=notifier,
    )

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    log_messages = await fetch_logs(session_factory, job.id)
    assert message.ack_calls == 1
    assert message.reject_calls == []
    assert refreshed_job.status == JobStatus.COMPLETED.value
    assert refreshed_job.execution_token is None
    assert refreshed_job.worker_id is None
    assert refreshed_job.attempt_count == 1
    assert log_messages == ["Job execution started", "Job completed"]
    assert [notification.status for notification in notifier.notifications] == [
        JobStatus.RUNNING,
        JobStatus.COMPLETED,
    ]


@pytest.mark.asyncio
async def test_worker_queues_retry_after_first_failed_attempt_and_acks_message(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="failure",
        payload={"message": "boom"},
    )
    message = build_message(job.id, created_at=now)
    worker = build_worker(session_factory, database_url, now=now)

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    log_messages = await fetch_logs(session_factory, job.id)
    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert refreshed_job.attempt_count == 1
    assert refreshed_job.last_error == "boom"
    assert len(retry_events) == 1
    assert retry_events[0].available_at == now + timedelta(seconds=5)
    assert log_messages == [
        "Job execution started",
        "Job attempt 1 failed and was queued for retry in 5 seconds: boom",
    ]


@pytest.mark.asyncio
async def test_worker_failure_schedules_retry_through_outbox_with_5_15_30_backoff(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    base_time = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="failure",
        payload={"message": "boom"},
    )
    worker = build_worker(
        session_factory,
        database_url,
        now=base_time,
        handler_registry=JobHandlerRegistry((ImmediateFailureHandler(),)),
    )

    await worker.handle_message(build_message(job.id, created_at=base_time))
    first_retry_job = await fetch_job(session_factory, job.id)
    first_retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert first_retry_job.status == JobStatus.QUEUED.value
    assert first_retry_job.attempt_count == 1
    assert len(first_retry_events) == 1
    assert first_retry_events[0].available_at == base_time + timedelta(seconds=5)

    async with session_factory() as session:
        event = await session.get(OutboxEvent, first_retry_events[0].id)
        assert event is not None
        event.published_at = base_time + timedelta(seconds=5)
        await session.commit()

    second_time = base_time + timedelta(seconds=5)
    second_worker = build_worker(
        session_factory,
        database_url,
        now=second_time,
        handler_registry=JobHandlerRegistry((ImmediateFailureHandler(),)),
    )
    await second_worker.handle_message(build_message(job.id, created_at=second_time))

    second_retry_job = await fetch_job(session_factory, job.id)
    second_retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert second_retry_job.status == JobStatus.QUEUED.value
    assert second_retry_job.attempt_count == 2
    assert len(second_retry_events) == 1
    assert second_retry_events[0].available_at == second_time + timedelta(seconds=15)

    async with session_factory() as session:
        event = await session.get(OutboxEvent, second_retry_events[0].id)
        assert event is not None
        event.published_at = second_time + timedelta(seconds=15)
        await session.commit()

    third_time = second_time + timedelta(seconds=15)
    third_worker = build_worker(
        session_factory,
        database_url,
        now=third_time,
        handler_registry=JobHandlerRegistry((ImmediateFailureHandler(),)),
    )
    await third_worker.handle_message(build_message(job.id, created_at=third_time))

    third_retry_job = await fetch_job(session_factory, job.id)
    third_retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert third_retry_job.status == JobStatus.QUEUED.value
    assert third_retry_job.attempt_count == 3
    assert len(third_retry_events) == 1
    assert third_retry_events[0].available_at == third_time + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_fourth_failed_attempt_marks_job_failed_without_retry_event(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="failure",
        payload={"message": "boom"},
    )

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.attempt_count = 3
        await session.commit()

    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((ImmediateFailureHandler(),)),
    )
    await worker.handle_message(build_message(job.id, created_at=now))

    refreshed_job = await fetch_job(session_factory, job.id)
    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert refreshed_job.status == JobStatus.FAILED.value
    assert refreshed_job.attempt_count == 4
    assert retry_events == []


@pytest.mark.asyncio
async def test_retry_outbox_event_is_not_duplicated_for_stale_failure_finalize(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    blocking_handler = BlockingSuccessHandler()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((blocking_handler,)),
    )
    job = await create_job(session_factory, owner.id, job_type="success")
    message = build_message(job.id, created_at=now)

    task = asyncio.create_task(worker.handle_message(message))
    await blocking_handler.started.wait()

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        original_token = persisted_job.execution_token
        assert original_token is not None
        persisted_job.execution_token = uuid.uuid4()
        persisted_job.worker_id = "replacement"
        await session.commit()

    blocking_handler.release.set()
    await task

    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert retry_events == []


@pytest.mark.asyncio
async def test_worker_defers_when_owner_is_at_running_capacity(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    for _ in range(3):
        await create_job(
            session_factory,
            owner.id,
            job_type="success",
            status=JobStatus.RUNNING,
            execution_token=uuid.uuid4(),
        )
    queued_job = await create_job(session_factory, owner.id, job_type="success")
    message = build_message(queued_job.id, created_at=now)
    notifier = RecordingPostCommitNotifier()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        post_commit_notifier=notifier,
    )

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, queued_job.id)
    log_messages = await fetch_logs(session_factory, queued_job.id)
    outbox_events = await count_outbox_events(session_factory, queued_job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert len(outbox_events) == 1
    assert outbox_events[0].available_at == now + timedelta(seconds=CAPACITY_DEFER_SECONDS)
    assert log_messages == ["Job dispatch deferred because the owner reached the running-job limit"]
    assert [notification.status for notification in notifier.notifications] == [JobStatus.QUEUED]


@pytest.mark.asyncio
async def test_worker_does_not_create_duplicate_capacity_defer_events_for_duplicate_messages(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    for _ in range(3):
        await create_job(
            session_factory,
            owner.id,
            job_type="success",
            status=JobStatus.RUNNING,
            execution_token=uuid.uuid4(),
        )
    queued_job = await create_job(session_factory, owner.id, job_type="success")
    worker = build_worker(session_factory, database_url, now=now)

    first_message = build_message(queued_job.id, created_at=now)
    second_message = build_message(queued_job.id, created_at=now)

    await worker.handle_message(first_message)
    await worker.handle_message(second_message)

    outbox_events = await count_outbox_events(session_factory, queued_job.id)
    log_messages = await fetch_logs(session_factory, queued_job.id)
    assert first_message.ack_calls == 1
    assert second_message.ack_calls == 1
    assert len(outbox_events) == 1
    assert log_messages == ["Job dispatch deferred because the owner reached the running-job limit"]


@pytest.mark.asyncio
async def test_worker_marks_queued_job_failed_when_next_attempt_exceeds_limit(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.attempt_count = 4
        await session.commit()

    worker = build_worker(session_factory, database_url, now=now)
    message = build_message(job.id, created_at=now)

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    log_messages = await fetch_logs(session_factory, job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.FAILED.value
    assert refreshed_job.attempt_count == 4
    assert log_messages == ["Job failed before execution because it exceeded the maximum attempts"]


@pytest.mark.asyncio
async def test_worker_cooperatively_cancels_running_sleep_job(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="sleep",
        payload={"duration_seconds": 1},
    )
    message = build_message(job.id, created_at=now)
    worker = build_worker(session_factory, database_url, now=now)

    task = asyncio.create_task(worker.handle_message(message))
    await wait_for_running_count(session_factory, owner.id, 1)
    await request_job_cancellation(
        session_factory,
        job.id,
        requested_at=now + timedelta(milliseconds=250),
    )
    await task

    refreshed_job = await fetch_job(session_factory, job.id)
    log_messages = await fetch_logs(session_factory, job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.CANCELLED.value
    assert refreshed_job.execution_token is None
    assert log_messages == [
        "Job execution started",
        "Job cancelled during attempt 1",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_log_messages"),
    [
        (JobStatus.PENDING, []),
        (JobStatus.RUNNING, []),
        (JobStatus.COMPLETED, []),
        (JobStatus.CANCELLED, []),
    ],
)
async def test_worker_acks_non_dispatchable_messages_without_mutating_jobs(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    status: JobStatus,
    expected_log_messages: list[str],
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="success",
        status=status,
        execution_token=uuid.uuid4() if status is JobStatus.RUNNING else None,
    )
    message = build_message(job.id, created_at=now)
    worker = build_worker(session_factory, database_url, now=now)

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    log_messages = await fetch_logs(session_factory, job.id)
    assert message.ack_calls == 1
    assert message.reject_calls == []
    assert refreshed_job.status == status.value
    assert log_messages == expected_log_messages


@pytest.mark.asyncio
async def test_duplicate_message_while_job_is_running_does_not_start_second_execution(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    blocking_handler = BlockingSuccessHandler()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((blocking_handler,)),
    )
    job = await create_job(session_factory, owner.id, job_type="success")

    first_message = build_message(job.id, created_at=now)
    duplicate_message = build_message(job.id, created_at=now)

    first_task = asyncio.create_task(worker.handle_message(first_message))
    await blocking_handler.started.wait()
    await wait_for_running_count(session_factory, owner.id, expected_count=1)

    await worker.handle_message(duplicate_message)

    blocking_handler.release.set()
    await first_task

    refreshed_job = await fetch_job(session_factory, job.id)
    assert blocking_handler.execute_calls == 1
    assert duplicate_message.ack_calls == 1
    assert refreshed_job.status == JobStatus.COMPLETED.value


@pytest.mark.asyncio
async def test_four_simultaneous_worker_claims_allow_only_three_running_per_user(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner_one = await create_owner(session_factory)
    owner_two = await create_owner(session_factory)
    owner_one_jobs = [
        await create_job(session_factory, owner_one.id, job_type="success") for _ in range(4)
    ]
    owner_two_jobs = [
        await create_job(session_factory, owner_two.id, job_type="success") for _ in range(3)
    ]
    blocking_handler = BlockingSuccessHandler()
    handler_registry = JobHandlerRegistry((blocking_handler,))

    workers = [
        build_worker(
            session_factory,
            database_url,
            now=now,
            handler_registry=handler_registry,
        )
        for _ in range(7)
    ]
    messages = [build_message(job.id, created_at=now) for job in [*owner_one_jobs, *owner_two_jobs]]

    tasks = [
        asyncio.create_task(worker.handle_message(message))
        for worker, message in zip(workers, messages, strict=True)
    ]

    await wait_for_running_count(session_factory, owner_one.id, expected_count=3)
    await wait_for_running_count(session_factory, owner_two.id, expected_count=3)

    owner_one_refreshed = await fetch_jobs_for_owner(session_factory, owner_one.id)
    owner_two_refreshed = await fetch_jobs_for_owner(session_factory, owner_two.id)
    assert sum(1 for job in owner_one_refreshed if job.status == JobStatus.RUNNING.value) == 3
    assert sum(1 for job in owner_two_refreshed if job.status == JobStatus.RUNNING.value) == 3
    assert sum(1 for job in owner_one_refreshed if job.status == JobStatus.QUEUED.value) == 1

    blocking_handler.release.set()
    await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_stale_execution_token_cannot_finalize_job_after_reclaim(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    blocking_handler = BlockingSuccessHandler()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((blocking_handler,)),
    )
    job = await create_job(session_factory, owner.id, job_type="success")
    message = build_message(job.id, created_at=now)

    task = asyncio.create_task(worker.handle_message(message))
    await blocking_handler.started.wait()
    await wait_for_running_count(session_factory, owner.id, expected_count=1)

    replacement_token = uuid.uuid4()
    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.execution_token = replacement_token
        persisted_job.worker_id = "worker-b"
        await session.commit()

    blocking_handler.release.set()
    await task

    refreshed_job = await fetch_job(session_factory, job.id)
    assert refreshed_job.status == JobStatus.RUNNING.value
    assert refreshed_job.execution_token == replacement_token
    assert refreshed_job.worker_id == "worker-b"


@pytest.mark.asyncio
async def test_stale_running_recovery_requeues_job_through_outbox_without_duplicate_event(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="success",
        status=JobStatus.RUNNING,
        execution_token=uuid.uuid4(),
    )

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.lease_expires_at = now - timedelta(seconds=1)
        await session.commit()

    recovery = StaleRunningRecovery(
        uow_factory=create_uow_factory(session_factory),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    recovered_count = await recovery.recover_due_batch_once()
    duplicate_recovered_count = await recovery.recover_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert recovered_count == 1
    assert duplicate_recovered_count == 0
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert refreshed_job.execution_token is None
    assert len(retry_events) == 1
    assert retry_events[0].available_at == now


@pytest.mark.asyncio
async def test_two_recovery_loops_do_not_recover_same_stale_running_row(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="success",
        status=JobStatus.RUNNING,
        execution_token=uuid.uuid4(),
    )

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.lease_expires_at = now - timedelta(seconds=1)
        await session.commit()

    first_recovery = StaleRunningRecovery(
        uow_factory=create_uow_factory(session_factory),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    second_recovery = StaleRunningRecovery(
        uow_factory=create_uow_factory(session_factory),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    recovered_counts = await asyncio.gather(
        first_recovery.recover_due_batch_once(),
        second_recovery.recover_due_batch_once(),
    )

    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert sorted(recovered_counts) == [0, 1]
    assert len(retry_events) == 1


@pytest.mark.asyncio
async def test_stale_running_recovery_marks_fourth_attempt_failed(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(
        session_factory,
        owner.id,
        job_type="success",
        status=JobStatus.RUNNING,
        execution_token=uuid.uuid4(),
    )

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.attempt_count = 4
        persisted_job.lease_expires_at = now - timedelta(seconds=1)
        await session.commit()

    recovery = StaleRunningRecovery(
        uow_factory=create_uow_factory(session_factory),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    recovered_count = await recovery.recover_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert recovered_count == 1
    assert refreshed_job.status == JobStatus.FAILED.value
    assert retry_events == []


@pytest.mark.asyncio
async def test_unknown_handler_marks_job_failed_without_crashing_consumer(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type=JobType.SUCCESS.value)

    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry(()),
    )
    message = build_message(job.id, created_at=now)

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.FAILED.value
    assert "No handler is registered" in (refreshed_job.last_error or "")


@pytest.mark.asyncio
async def test_worker_rejects_invalid_dispatch_payload_without_requeue(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    worker = build_worker(session_factory, database_url, now=now)
    message = FakeIncomingMessage(b'{"kind":"dispatch"}')

    await worker.handle_message(message)

    assert message.ack_calls == 0
    assert message.reject_calls == [False]


@pytest.mark.asyncio
async def test_worker_does_not_ack_when_claim_commit_fails(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")
    notifier = RecordingPostCommitNotifier()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((ImmediateSuccessHandler(),)),
        post_commit_notifier=notifier,
    )
    message = build_message(job.id, created_at=now)
    original_commit = SqlAlchemyUnitOfWork.commit
    commit_calls = 0

    async def fail_first_commit(self: SqlAlchemyUnitOfWork) -> None:
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 1:
            raise RuntimeError("claim commit failed")
        await original_commit(self)

    monkeypatch.setattr(SqlAlchemyUnitOfWork, "commit", fail_first_commit)

    with pytest.raises(RuntimeError, match="claim commit failed"):
        await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    assert message.ack_calls == 0
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert notifier.notifications == []


@pytest.mark.asyncio
async def test_worker_does_not_ack_when_finalize_commit_fails(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")
    notifier = RecordingPostCommitNotifier()
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((ImmediateSuccessHandler(),)),
        post_commit_notifier=notifier,
    )
    message = build_message(job.id, created_at=now)
    original_commit = SqlAlchemyUnitOfWork.commit
    commit_calls = 0

    async def fail_second_commit(self: SqlAlchemyUnitOfWork) -> None:
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise RuntimeError("finalize commit failed")
        await original_commit(self)

    monkeypatch.setattr(SqlAlchemyUnitOfWork, "commit", fail_second_commit)

    with pytest.raises(RuntimeError, match="finalize commit failed"):
        await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    assert message.ack_calls == 0
    assert refreshed_job.status == JobStatus.RUNNING.value
    assert [notification.status for notification in notifier.notifications] == [JobStatus.RUNNING]


@pytest.mark.asyncio
async def test_finalize_commit_failure_redelivery_is_eventually_recovered_by_stale_running_recovery(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        handler_registry=JobHandlerRegistry((ImmediateSuccessHandler(),)),
    )
    message = build_message(job.id, created_at=now)
    original_commit = SqlAlchemyUnitOfWork.commit
    commit_calls = 0

    async def fail_second_commit(self: SqlAlchemyUnitOfWork) -> None:
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise RuntimeError("finalize commit failed")
        await original_commit(self)

    monkeypatch.setattr(SqlAlchemyUnitOfWork, "commit", fail_second_commit)

    with pytest.raises(RuntimeError, match="finalize commit failed"):
        await worker.handle_message(message)

    redelivered_message = build_message(job.id, created_at=now)
    await worker.handle_message(redelivered_message)

    async with session_factory() as session:
        persisted_job = await session.get(Job, job.id)
        assert persisted_job is not None
        persisted_job.lease_expires_at = now - timedelta(seconds=1)
        await session.commit()

    recovery = StaleRunningRecovery(
        uow_factory=create_uow_factory(session_factory),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    recovered_count = await recovery.recover_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    retry_events = await fetch_unpublished_outbox_events(session_factory, job.id)
    assert redelivered_message.ack_calls == 1
    assert recovered_count == 1
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert len(retry_events) == 1


@pytest.mark.asyncio
async def test_post_commit_notifier_failure_does_not_roll_back_committed_job_state(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, job_type="success")
    message = build_message(job.id, created_at=now)
    notifier = RecordingPostCommitNotifier(error=RuntimeError("redis down"))
    worker = build_worker(
        session_factory,
        database_url,
        now=now,
        post_commit_notifier=notifier,
    )

    await worker.handle_message(message)

    refreshed_job = await fetch_job(session_factory, job.id)
    assert message.ack_calls == 1
    assert refreshed_job.status == JobStatus.COMPLETED.value
    assert [notification.status for notification in notifier.notifications] == [
        JobStatus.RUNNING,
        JobStatus.COMPLETED,
    ]
