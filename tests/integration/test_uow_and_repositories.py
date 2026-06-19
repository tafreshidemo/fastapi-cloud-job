from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from alembic.config import Config
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from alembic import command
from app.api.dependencies.infrastructure import get_session_factory, get_uow_factory
from app.db.session import create_session_factory
from app.db.uow import SqlAlchemyUnitOfWork, create_uow_factory
from app.domain.enums import JobEventType, JobStatus, UserRole
from app.models.job import Job
from app.models.job_log import JobLog
from app.models.outbox_event import OutboxEvent
from app.models.user import User
from tests.support import (
    get_test_database_url,
    get_test_rabbitmq_url,
    get_test_redis_url,
    reset_test_database,
)

TEST_DATABASE_URL = get_test_database_url()


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


def build_job(owner_id: uuid.UUID) -> Job:
    job = Job(
        owner_id=owner_id,
        type="sleep",
        payload={"duration_seconds": 5},
        status=JobStatus.PENDING.value,
        idempotency_key=f"idem-{uuid.uuid4()}",
        request_hash="a" * 64,
        attempt_count=0,
        max_retries=3,
    )
    job.id = uuid.uuid4()
    return job


def build_log(job_id: uuid.UUID) -> JobLog:
    return JobLog(
        job_id=job_id,
        attempt_number=1,
        level="INFO",
        message="job created",
    )


def build_outbox(job_id: uuid.UUID) -> OutboxEvent:
    now = datetime.now(UTC)
    return OutboxEvent(
        aggregate_id=job_id,
        event_type=JobEventType.DISPATCH.value,
        payload={"job_id": str(job_id)},
        available_at=now,
        publish_attempts=0,
    )


@pytest.mark.asyncio
async def test_uow_commit_persists_writes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    job = build_job(owner.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.jobs.add(job)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_job = await uow.jobs.get_by_id(job.id)
        assert persisted_job is not None
        assert persisted_job.owner_id == owner.id
        assert persisted_job.status == JobStatus.PENDING.value


@pytest.mark.asyncio
async def test_uow_rolls_back_on_exception(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    job = build_job(owner.id)

    with pytest.raises(RuntimeError):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.jobs.add(job)
            raise RuntimeError("force rollback")

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_job = await uow.jobs.get_by_id(job.id)

    assert persisted_job is None


@pytest.mark.asyncio
async def test_job_log_and_outbox_are_all_or_nothing_on_commit_failure(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    missing_owner_id = uuid.uuid4()
    job = build_job(missing_owner_id)
    log = build_log(job.id)
    outbox = build_outbox(job.id)

    with pytest.raises(IntegrityError):
        async with SqlAlchemyUnitOfWork(session_factory) as uow:
            await uow.jobs.add(job)
            await uow.job_logs.add(log)
            await uow.outbox.add(outbox)
            await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_job = await uow.jobs.get_by_id(job.id)
        persisted_logs = await uow.job_logs.list_for_job(job.id)
        claimed_outbox = await uow.outbox.claim_due_batch(datetime.now(UTC), limit=10)

    assert persisted_job is None
    assert persisted_logs == []
    assert all(event.id != outbox.id for event in claimed_outbox)


@pytest.mark.asyncio
async def test_repository_queries_do_not_commit(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner_email = f"{uuid.uuid4()}@example.com"
    owner = User(
        email=owner_email,
        password_hash="hashed-password",
        role=UserRole.USER.value,
        is_active=True,
    )

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.users.add(owner)
        fetched_owner = await uow.users.get_by_email(owner_email)
        assert fetched_owner is not None
        assert fetched_owner.email == owner_email

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_owner = await uow.users.get_by_email(owner_email)

    assert persisted_owner is None


@pytest.mark.asyncio
async def test_outbox_claim_due_batch_uses_skip_locked(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    job = build_job(owner.id)
    outbox = build_outbox(job.id)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.jobs.add(job)
        await uow.outbox.add(outbox)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as first_uow:
        first_claim = await first_uow.outbox.claim_due_batch(datetime.now(UTC), limit=1)
        async with SqlAlchemyUnitOfWork(session_factory) as second_uow:
            second_claim = await second_uow.outbox.claim_due_batch(datetime.now(UTC), limit=1)

        assert [event.id for event in first_claim] == [outbox.id]
        assert second_claim == []


@pytest.mark.asyncio
async def test_job_repository_keyset_queries_and_running_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    now = datetime.now(UTC)
    older_job = build_job(owner.id)
    older_job.created_at = now - timedelta(minutes=2)
    older_job.updated_at = older_job.created_at

    newer_job = build_job(owner.id)
    newer_job.created_at = now - timedelta(minutes=1)
    newer_job.updated_at = newer_job.created_at
    newer_job.status = JobStatus.RUNNING.value

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.jobs.add(older_job)
        await uow.jobs.add(newer_job)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        first_page = await uow.jobs.list_for_owner_keyset(owner.id, cursor=None, limit=1)
        running_count = await uow.jobs.count_running_for_owner(owner.id)
        assert [job_item.id for job_item in first_page] == [newer_job.id]
        assert running_count == 1


@pytest.mark.asyncio
async def test_job_repository_mark_queued_only_updates_pending_jobs(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    pending_job = build_job(owner.id)
    cancelled_job = build_job(owner.id)
    cancelled_job.status = JobStatus.CANCELLED.value
    now = datetime.now(UTC)

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.jobs.add(pending_job)
        await uow.jobs.add(cancelled_job)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        pending_updated = await uow.jobs.mark_queued(pending_job.id, now)
        cancelled_updated = await uow.jobs.mark_queued(cancelled_job.id, now)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_pending = await uow.jobs.get_by_id(pending_job.id)
        persisted_cancelled = await uow.jobs.get_by_id(cancelled_job.id)

    assert pending_updated is True
    assert cancelled_updated is False
    assert persisted_pending is not None
    assert persisted_pending.status == JobStatus.QUEUED
    assert persisted_cancelled is not None
    assert persisted_cancelled.status == JobStatus.CANCELLED


@pytest.mark.asyncio
async def test_job_repository_execution_token_fencing_updates_only_matching_running_claim(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    owner = await create_owner(session_factory)
    job = build_job(owner.id)
    now = datetime.now(UTC)
    claim_token = uuid.uuid4()
    stale_token = uuid.uuid4()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        await uow.jobs.add(job)
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        queued = await uow.jobs.mark_queued(job.id, now)
        claimed = await uow.jobs.mark_running(
            job.id,
            worker_id="worker-a",
            execution_token=claim_token,
            now=now,
            lease_expires_at=now + timedelta(seconds=60),
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        stale_completion = await uow.jobs.mark_completed(
            job.id,
            execution_token=stale_token,
            now=now + timedelta(seconds=1),
        )
        matching_completion = await uow.jobs.mark_completed(
            job.id,
            execution_token=claim_token,
            now=now + timedelta(seconds=2),
        )
        await uow.commit()

    async with SqlAlchemyUnitOfWork(session_factory) as uow:
        persisted_job = await uow.jobs.get_execution_candidate_for_update(job.id)

    assert queued is True
    assert claimed is True
    assert stale_completion is False
    assert matching_completion is True
    assert persisted_job is not None
    assert persisted_job.status == JobStatus.COMPLETED
    assert persisted_job.execution_token is None
    assert persisted_job.worker_id is None


@pytest.mark.asyncio
async def test_uow_reuses_injected_session_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    first_uow = SqlAlchemyUnitOfWork(session_factory)
    second_uow = SqlAlchemyUnitOfWork(session_factory)

    assert first_uow._session_factory is session_factory
    assert second_uow._session_factory is session_factory

    async with first_uow, second_uow:
        assert first_uow._session is not None
        assert second_uow._session is not None
        assert first_uow._session.bind is second_uow._session.bind


def test_uow_requires_injected_session_factory() -> None:
    with pytest.raises(TypeError):
        SqlAlchemyUnitOfWork()  # type: ignore[call-arg]


def test_managed_uow_factory_reuses_application_session_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    class AppState:
        def __init__(self) -> None:
            self.db_session_factory = session_factory
            self.uow_factory = create_uow_factory(session_factory)

    class AppStub:
        def __init__(self) -> None:
            self.state = AppState()

    app = AppStub()
    resolved_session_factory = get_session_factory(app)
    resolved_uow_factory = get_uow_factory(app)
    first_uow = resolved_uow_factory()
    second_uow = resolved_uow_factory()

    assert resolved_session_factory is session_factory
    assert isinstance(first_uow, SqlAlchemyUnitOfWork)
    assert isinstance(second_uow, SqlAlchemyUnitOfWork)
    assert first_uow._session_factory is session_factory
    assert second_uow._session_factory is session_factory
