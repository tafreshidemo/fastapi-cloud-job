from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from alembic.config import Config
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from alembic import command
from app.core.config import Settings
from app.db.session import create_session_factory
from app.db.uow import SqlAlchemyUnitOfWork, create_uow_factory
from app.domain.enums import JobStatus, UserRole
from app.models.job import Job
from app.models.outbox_event import OutboxEvent
from app.models.user import User
from app.workers.outbox_publisher import OutboxPublisher
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


class RecordingDispatchPublisher:
    def __init__(self, *, error: Exception | None = None, delay_seconds: float = 0.0) -> None:
        self._error = error
        self._delay_seconds = delay_seconds
        self.calls: list[tuple[uuid.UUID, uuid.UUID]] = []

    async def publish_dispatch(self, *, outbox_event: OutboxEvent, message) -> None:  # type: ignore[no-untyped-def]
        if self._delay_seconds > 0:
            await asyncio.sleep(self._delay_seconds)
        if self._error is not None:
            raise self._error
        self.calls.append((outbox_event.id, message.job_id))


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
        OUTBOX_BATCH_SIZE=10,
        OUTBOX_POLL_INTERVAL_SECONDS=1,
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
    return owner


async def create_job(
    session_factory: async_sessionmaker[AsyncSession],
    owner_id: uuid.UUID,
    *,
    status: JobStatus = JobStatus.PENDING,
) -> Job:
    job = Job(
        owner_id=owner_id,
        type="sleep",
        payload={"duration_seconds": 5},
        status=status.value,
        idempotency_key=f"idem-{uuid.uuid4()}",
        request_hash="a" * 64,
        attempt_count=0,
        max_retries=3,
    )
    async with session_factory() as session:
        session.add(job)
        await session.commit()
        await session.refresh(job)
    return job


async def create_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    job_id: uuid.UUID,
    available_at: datetime,
) -> OutboxEvent:
    event_id = uuid.uuid4()
    event = OutboxEvent(
        id=event_id,
        aggregate_id=job_id,
        event_type="job.dispatch",
        payload={
            "event_id": str(event_id),
            "job_id": str(job_id),
            "kind": "dispatch",
            "created_at": available_at.isoformat(),
        },
        available_at=available_at,
        publish_attempts=0,
    )
    async with session_factory() as session:
        session.add(event)
        await session.commit()
        await session.refresh(event)
    return event


async def fetch_job(session_factory: async_sessionmaker[AsyncSession], job_id: uuid.UUID) -> Job:
    async with session_factory() as session:
        job = await session.get(Job, job_id)
        assert job is not None
        return job


async def fetch_outbox_event(
    session_factory: async_sessionmaker[AsyncSession],
    event_id: uuid.UUID,
) -> OutboxEvent:
    async with session_factory() as session:
        event = await session.get(OutboxEvent, event_id)
        assert event is not None
        return event


@pytest.mark.asyncio
async def test_rabbitmq_unavailable_leaves_job_pending_and_outbox_retryable(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=RecordingDispatchPublisher(error=RuntimeError("rabbit unavailable")),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    processed_count = await publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert processed_count == 1
    assert refreshed_job.status == JobStatus.PENDING.value
    assert refreshed_event.published_at is None
    assert refreshed_event.publish_attempts == 1
    assert refreshed_event.last_error == "rabbit unavailable"
    assert refreshed_event.available_at == now + timedelta(seconds=1)


@pytest.mark.asyncio
async def test_rabbitmq_recovery_eventually_publishes_and_moves_job_to_queued(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    failing_publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=RecordingDispatchPublisher(error=RuntimeError("rabbit unavailable")),
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    await failing_publisher.publish_due_batch_once()

    retry_time = now + timedelta(seconds=2)
    successful_dispatch = RecordingDispatchPublisher()
    successful_publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=successful_dispatch,
        settings=build_settings(database_url),
        clock=FixedClock(retry_time),
    )

    processed_count = await successful_publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert processed_count == 1
    assert successful_dispatch.calls == [(event.id, job.id)]
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert refreshed_event.published_at == retry_time
    assert refreshed_event.last_error is None


@pytest.mark.asyncio
async def test_two_publisher_processes_cannot_claim_same_outbox_row(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    dispatch_publisher = RecordingDispatchPublisher(delay_seconds=0.1)
    first_publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    second_publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    processed_counts = await asyncio.gather(
        first_publisher.publish_due_batch_once(),
        second_publisher.publish_due_batch_once(),
    )

    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert sorted(processed_counts) == [0, 1]
    assert dispatch_publisher.calls == [(event.id, job.id)]
    assert refreshed_event.published_at == now


@pytest.mark.asyncio
async def test_cancelled_job_event_is_discarded_and_not_published(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, status=JobStatus.CANCELLED)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    dispatch_publisher = RecordingDispatchPublisher()
    publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    processed_count = await publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert processed_count == 1
    assert dispatch_publisher.calls == []
    assert refreshed_job.status == JobStatus.CANCELLED.value
    assert refreshed_event.published_at == now


@pytest.mark.asyncio
async def test_queued_job_deferred_dispatch_event_is_published_without_requeueing_job(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id, status=JobStatus.QUEUED)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    dispatch_publisher = RecordingDispatchPublisher()
    publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )

    processed_count = await publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert processed_count == 1
    assert dispatch_publisher.calls == [(event.id, job.id)]
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert refreshed_event.published_at == now


@pytest.mark.asyncio
async def test_commit_failure_after_publish_keeps_db_state_unchanged_and_allows_duplicate_publish(
    session_factory: async_sessionmaker[AsyncSession],
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    owner = await create_owner(session_factory)
    job = await create_job(session_factory, owner.id)
    event = await create_outbox_event(
        session_factory,
        job_id=job.id,
        available_at=now - timedelta(seconds=1),
    )
    dispatch_publisher = RecordingDispatchPublisher()
    publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now),
    )
    original_commit = SqlAlchemyUnitOfWork.commit
    commit_failures_remaining = 1

    async def fail_once_commit(self: SqlAlchemyUnitOfWork) -> None:
        nonlocal commit_failures_remaining
        if commit_failures_remaining > 0:
            commit_failures_remaining -= 1
            raise RuntimeError("commit failed")
        await original_commit(self)

    monkeypatch.setattr(SqlAlchemyUnitOfWork, "commit", fail_once_commit)

    with pytest.raises(RuntimeError, match="commit failed"):
        await publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert dispatch_publisher.calls == [(event.id, job.id)]
    assert refreshed_job.status == JobStatus.PENDING.value
    assert refreshed_event.published_at is None
    assert refreshed_event.publish_attempts == 0
    assert refreshed_event.last_error is None

    monkeypatch.setattr(SqlAlchemyUnitOfWork, "commit", original_commit)
    retry_publisher = OutboxPublisher(
        uow_factory=create_uow_factory(session_factory),
        dispatch_publisher=dispatch_publisher,
        settings=build_settings(database_url),
        clock=FixedClock(now + timedelta(seconds=1)),
    )

    processed_count = await retry_publisher.publish_due_batch_once()

    refreshed_job = await fetch_job(session_factory, job.id)
    refreshed_event = await fetch_outbox_event(session_factory, event.id)
    assert processed_count == 1
    assert dispatch_publisher.calls == [(event.id, job.id), (event.id, job.id)]
    assert refreshed_job.status == JobStatus.QUEUED.value
    assert refreshed_event.published_at == now + timedelta(seconds=1)
