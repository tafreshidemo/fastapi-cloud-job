from __future__ import annotations

import asyncio
import base64
import json
import uuid
from collections.abc import AsyncIterator, Iterator
from datetime import UTC, datetime
from typing import cast

import asyncpg  # type: ignore[import-untyped]
import httpx
import pytest
from alembic.config import Config
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from redis.asyncio import Redis

from alembic import command
from app.api.dependencies.infrastructure import get_job_status_pubsub_from_request
from app.api.dependencies.services import get_job_service
from app.application.dto import CreateJobRateLimitReservationDTO, StoredJobDTO
from app.application.rate_limits import CreateJobRateLimiter
from app.application.services.jobs import JobService
from app.core.config import Settings
from app.core.security import hash_password
from app.domain.enums import JobStatus
from app.infrastructure.redis.job_status_pubsub import (
    JobStatusPubSubError,
    RedisJobStatusPubSub,
)
from app.infrastructure.redis.rate_limiter import RateLimitExceededError
from app.repositories.jobs import SqlAlchemyJobRepository
from app.repositories.outbox import SqlAlchemyOutboxRepository
from app.schemas.job_events import JobStatusEvent
from app.workers.post_commit import (
    JobPostCommitNotification,
    RedisPublishingWorkerPostCommitNotifier,
)
from tests.support import (
    get_test_database_url,
    get_test_rabbitmq_url,
    get_test_redis_url,
    reset_test_database,
    to_asyncpg_dsn,
)

TEST_DATABASE_URL = get_test_database_url()


class AllowAllRateLimiter:
    async def reserve(self, user_id: uuid.UUID) -> CreateJobRateLimitReservationDTO | None:
        del user_id
        return None

    async def release(self, reservation: CreateJobRateLimitReservationDTO) -> None:
        del reservation


class CountingRateLimiter:
    def __init__(self, limit: int, *, reset_epoch: int = 1_700_000_060) -> None:
        self._limit = limit
        self._counts: dict[uuid.UUID, int] = {}
        self.reserve_calls: dict[uuid.UUID, int] = {}
        self.release_calls = 0
        self._reset_epoch = reset_epoch

    async def reserve(self, user_id: uuid.UUID) -> CreateJobRateLimitReservationDTO | None:
        self.reserve_calls[user_id] = self.reserve_calls.get(user_id, 0) + 1
        count = self._counts.get(user_id, 0) + 1
        self._counts[user_id] = count
        if count > self._limit:
            raise RateLimitExceededError(
                limit=self._limit,
                retry_after_seconds=30,
                reset_epoch=self._reset_epoch,
            )
        return CreateJobRateLimitReservationDTO(
            key=f"test-rate-limit:{user_id}",
            limit=self._limit,
            used=count,
            remaining=max(self._limit - count, 0),
            retry_after_seconds=30,
            reset_epoch=self._reset_epoch,
        )

    async def release(self, reservation: CreateJobRateLimitReservationDTO) -> None:
        self.release_calls += 1
        user_id = uuid.UUID(reservation.key.removeprefix("test-rate-limit:"))
        self._counts[user_id] = max(self._counts.get(user_id, 1) - 1, 0)


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
def settings(database_url: str) -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL=database_url,
        REDIS_URL=get_test_redis_url(),
        RABBITMQ_URL=get_test_rabbitmq_url(),
        JWT_SECRET="x" * 32,
    )


@pytest.fixture
def app(
    settings: Settings,
    alembic_config: Config,
    database_url: str,
) -> Iterator[FastAPI]:
    asyncio.run(reset_test_database(database_url))
    command.upgrade(alembic_config, "head")

    from app.main import create_app

    app = create_app(settings)
    app.state.test_rate_limiter = AllowAllRateLimiter()

    def override_job_service() -> JobService:
        return JobService(
            uow_factory=app.state.uow_factory,
            settings=cast(Settings, app.state.settings),
            rate_limiter=cast(CreateJobRateLimiter, app.state.test_rate_limiter),
            job_list_cache=app.state.job_list_cache,
            post_commit_notifier=app.state.worker_post_commit_notifier,
        )

    app.dependency_overrides[get_job_service] = override_job_service
    yield app


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client

async def insert_user(
    database_url: str,
    *,
    email: str,
    password: str,
    role: str = "user",
) -> uuid.UUID:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        user_id = uuid.uuid4()
        now = datetime.now(UTC)
        await connection.execute(
            """
            INSERT INTO users (id, email, password_hash, role, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, TRUE, $5, $6)
            """,
            user_id,
            email,
            hash_password(password),
            role,
            now,
            now,
        )
        return user_id
    finally:
        await connection.close()


def authorization_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def login_user(client: TestClient, *, email: str, password: str) -> str:
    response = client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200
    return str(response.json()["access_token"])


async def login_user_async(
    client: httpx.AsyncClient,
    *,
    email: str,
    password: str,
) -> str:
    response = await client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200
    return str(response.json()["access_token"])


async def fetch_job_by_owner_and_key(
    database_url: str,
    *,
    owner_id: uuid.UUID,
    idempotency_key: str,
) -> asyncpg.Record | None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        return await connection.fetchrow(
            """
            SELECT id, owner_id, status, type, idempotency_key
            FROM jobs
            WHERE owner_id = $1 AND idempotency_key = $2
            """,
            owner_id,
            idempotency_key,
        )
    finally:
        await connection.close()


async def count_job_logs_for_job(database_url: str, job_id: uuid.UUID) -> int:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        return int(
            await connection.fetchval(
                "SELECT COUNT(*) FROM job_logs WHERE job_id = $1",
                job_id,
            )
        )
    finally:
        await connection.close()


async def count_outbox_events_for_job(database_url: str, job_id: uuid.UUID) -> int:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        return int(
            await connection.fetchval(
                "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1",
                job_id,
            )
        )
    finally:
        await connection.close()


async def fetch_outbox_event_for_job(database_url: str, job_id: uuid.UUID) -> asyncpg.Record | None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        return await connection.fetchrow(
            """
            SELECT event_type, payload, published_at
            FROM outbox_events
            WHERE aggregate_id = $1
            """,
            job_id,
        )
    finally:
        await connection.close()


async def count_rows_by_idempotency_key(
    database_url: str,
    *,
    idempotency_key: str,
) -> tuple[int, int, int]:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        job_count = int(
            await connection.fetchval(
                "SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1",
                idempotency_key,
            )
        )
        log_count = int(await connection.fetchval("SELECT COUNT(*) FROM job_logs"))
        outbox_count = int(await connection.fetchval("SELECT COUNT(*) FROM outbox_events"))
        return job_count, log_count, outbox_count
    finally:
        await connection.close()


async def update_job_status(
    database_url: str,
    *,
    job_id: uuid.UUID,
    status: str,
    worker_id: str | None = None,
    execution_token: uuid.UUID | None = None,
    cancel_requested_at: datetime | None = None,
    started_at: datetime | None = None,
) -> None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        await connection.execute(
            """
            UPDATE jobs
            SET status = $2,
                worker_id = $3,
                execution_token = $4,
                cancel_requested_at = $5,
                started_at = $6,
                lease_expires_at = $6,
                updated_at = NOW()
            WHERE id = $1
            """,
            job_id,
            status,
            worker_id,
            execution_token,
            cancel_requested_at,
            started_at,
        )
    finally:
        await connection.close()


async def delete_job(database_url: str, job_id: uuid.UUID) -> None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        await connection.execute("DELETE FROM jobs WHERE id = $1", job_id)
    finally:
        await connection.close()


async def read_sse_event(lines: AsyncIterator[str]) -> dict[str, object]:
    event_name: str | None = None
    event_id: str | None = None
    data_lines: list[str] = []

    async for line in lines:
        if not line:
            if event_name is not None and data_lines:
                return {
                    "id": event_id,
                    "event": event_name,
                    "data": json.loads("".join(data_lines)),
                }
            continue
        if line.startswith(":"):
            continue
        if line.startswith("id: "):
            event_id = line.removeprefix("id: ")
            continue
        if line.startswith("event: "):
            event_name = line.removeprefix("event: ")
            continue
        if line.startswith("data: "):
            data_lines.append(line.removeprefix("data: "))

    raise AssertionError("SSE stream closed before a full event was received")


def read_sse_event_sync(lines: Iterator[str]) -> dict[str, object]:
    event_name: str | None = None
    event_id: str | None = None
    data_lines: list[str] = []

    for line in lines:
        if not line:
            if event_name is not None and data_lines:
                return {
                    "id": event_id,
                    "event": event_name,
                    "data": json.loads("".join(data_lines)),
                }
            continue
        if line.startswith(":"):
            continue
        if line.startswith("id: "):
            event_id = line.removeprefix("id: ")
            continue
        if line.startswith("event: "):
            event_name = line.removeprefix("event: ")
            continue
        if line.startswith("data: "):
            data_lines.append(line.removeprefix("data: "))

    raise AssertionError("SSE stream closed before a full event was received")


class FakeJobStatusPubSub:
    def __init__(
        self,
        events: list[dict[str, object]] | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self._events = list(events or [])
        self._error = error
        self.subscribed_job_ids: list[uuid.UUID] = []
        self.closed = False

    async def publish(self, event: object) -> None:
        del event

    @staticmethod
    def channel_name(job_id: uuid.UUID) -> str:
        return f"jobs:events:{job_id}"

    async def _iterator(self) -> AsyncIterator[object]:
        if self._error is not None:
            raise self._error
        for event in self._events:
            yield event

    def subscribe(self, job_id: uuid.UUID):  # type: ignore[no-untyped-def]
        self.subscribed_job_ids.append(job_id)
        parent = self

        class _Subscription:
            async def __aenter__(self) -> AsyncIterator[object]:
                return parent._iterator()

            async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
                del exc_type, exc, tb
                parent.closed = True

        return _Subscription()


async def set_jobs_created_at(
    database_url: str,
    *,
    job_ids: list[uuid.UUID],
    created_at: datetime,
) -> None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        for job_id in job_ids:
            await connection.execute(
                """
                UPDATE jobs
                SET created_at = $2,
                    updated_at = $2
                WHERE id = $1
                """,
                job_id,
                created_at,
            )
    finally:
        await connection.close()


async def insert_job_logs(
    database_url: str,
    *,
    job_id: uuid.UUID,
    total: int,
    created_at: datetime,
) -> None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        for index in range(total):
            await connection.execute(
                """
                INSERT INTO job_logs (job_id, attempt_number, level, message, created_at)
                VALUES ($1, NULL, 'info', $2, $3)
                """,
                job_id,
                f"log-{index:03d}",
                created_at,
            )
    finally:
        await connection.close()


def create_job_request(
    client: TestClient,
    *,
    token: str,
    idempotency_key: str,
    payload: dict[str, object],
) -> Response:
    return client.post(
        "/jobs",
        json=payload,
        headers={
            **authorization_header(token),
            "Idempotency-Key": idempotency_key,
        },
    )


def test_first_create_returns_201_with_pending(
    client: TestClient,
    database_url: str,
) -> None:
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": 5}},
        headers={
            **authorization_header(token),
            "Idempotency-Key": "job-1",
        },
    )

    assert response.status_code == 201
    assert response.headers["Idempotency-Replayed"] == "false"
    payload = response.json()
    assert payload["status"] == "pending"
    assert payload["type"] == "sleep"
    assert payload["owner_id"] == str(owner_id)

    job_id = uuid.UUID(str(payload["id"]))
    job_logs_count = asyncio.run(count_job_logs_for_job(database_url, job_id))
    outbox_count = asyncio.run(count_outbox_events_for_job(database_url, job_id))
    outbox_event = asyncio.run(fetch_outbox_event_for_job(database_url, job_id))

    assert job_logs_count == 1
    assert outbox_count == 1
    assert outbox_event is not None
    assert outbox_event["event_type"] == "job.dispatch"
    payload_data = outbox_event["payload"]
    if isinstance(payload_data, str):
        payload_data = json.loads(payload_data)
    assert payload_data["job_id"] == str(job_id)
    assert payload_data["kind"] == "dispatch"
    assert uuid.UUID(str(payload_data["event_id"]))
    assert datetime.fromisoformat(str(payload_data["created_at"]))
    assert set(payload_data) == {"event_id", "job_id", "kind", "created_at"}
    assert outbox_event["published_at"] is None


def test_same_key_and_same_payload_returns_existing_job(
    client: TestClient,
    database_url: str,
) -> None:
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )
    token = login_user(client, email="user@example.com", password="secret-password")
    headers = {
        **authorization_header(token),
        "Idempotency-Key": "job-1",
    }

    first_response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": 5}},
        headers=headers,
    )
    second_response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": 5}},
        headers=headers,
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 200
    assert first_response.headers["Idempotency-Replayed"] == "false"
    assert second_response.headers["Idempotency-Replayed"] == "true"
    assert first_response.json()["id"] == second_response.json()["id"]

    job_record = asyncio.run(
        fetch_job_by_owner_and_key(database_url, owner_id=owner_id, idempotency_key="job-1")
    )
    assert job_record is not None

    job_id = uuid.UUID(str(job_record["id"]))
    assert asyncio.run(count_job_logs_for_job(database_url, job_id)) == 1
    assert asyncio.run(count_outbox_events_for_job(database_url, job_id)) == 1


def test_job_detail_and_logs_are_available_to_owner(
    client: TestClient,
    database_url: str,
) -> None:
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )
    token = login_user(client, email="user@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-detail-owner",
        payload={"type": "sleep", "payload": {"duration_seconds": 5}},
    )
    job_id = create_response.json()["id"]

    detail_response = client.get(
        f"/jobs/{job_id}",
        headers=authorization_header(token),
    )
    logs_response = client.get(
        f"/jobs/{job_id}/logs",
        headers=authorization_header(token),
    )

    assert detail_response.status_code == 200
    assert detail_response.json()["id"] == job_id
    assert detail_response.json()["owner_id"] == str(owner_id)
    assert logs_response.status_code == 200
    assert len(logs_response.json()) == 1
    assert logs_response.json()[0]["job_id"] == job_id
    assert logs_response.json()[0]["message"] == "Job created"


def test_job_detail_and_logs_are_forbidden_to_other_user_via_service_scoped_lookup(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="owner@example.com", password="secret-password"))
    asyncio.run(insert_user(database_url, email="other@example.com", password="secret-password"))
    owner_token = login_user(client, email="owner@example.com", password="secret-password")
    other_token = login_user(client, email="other@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=owner_token,
        idempotency_key="job-other-user",
        payload={"type": "sleep", "payload": {"duration_seconds": 5}},
    )
    job_id = create_response.json()["id"]

    detail_response = client.get(
        f"/jobs/{job_id}",
        headers=authorization_header(other_token),
    )
    logs_response = client.get(
        f"/jobs/{job_id}/logs",
        headers=authorization_header(other_token),
    )

    assert detail_response.status_code == 404
    assert detail_response.json() == {
        "error": {
            "code": "RESOURCE_NOT_FOUND",
            "message": "Job not found",
            "details": None,
        }
    }
    assert logs_response.status_code == 404
    assert logs_response.json() == {
        "error": {
            "code": "RESOURCE_NOT_FOUND",
            "message": "Job not found",
            "details": None,
        }
    }


def test_job_detail_and_logs_are_available_to_admin(
    client: TestClient,
    database_url: str,
) -> None:
    owner_id = asyncio.run(
        insert_user(database_url, email="owner@example.com", password="secret-password")
    )
    asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )
    owner_token = login_user(client, email="owner@example.com", password="secret-password")
    admin_token = login_user(client, email="admin@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=owner_token,
        idempotency_key="job-admin-read",
        payload={"type": "sleep", "payload": {"duration_seconds": 5}},
    )
    job_id = create_response.json()["id"]

    detail_response = client.get(
        f"/jobs/{job_id}",
        headers=authorization_header(admin_token),
    )
    logs_response = client.get(
        f"/jobs/{job_id}/logs",
        headers=authorization_header(admin_token),
    )

    assert detail_response.status_code == 200
    assert detail_response.json()["owner_id"] == str(owner_id)
    assert logs_response.status_code == 200
    assert len(logs_response.json()) == 1


def test_job_events_stream_returns_snapshot_event(
    app: FastAPI,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    with TestClient(app) as client:
        token = login_user(client, email="user@example.com", password="secret-password")
        create_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={
                **authorization_header(token),
                "Idempotency-Key": "job-sse-live",
            },
        )
        assert create_response.status_code == 201
        job_id = uuid.UUID(str(create_response.json()["id"]))

        with client.stream(
            "GET",
            f"/jobs/{job_id}/events",
            headers=authorization_header(token),
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            lines = response.iter_lines()

            snapshot_event = read_sse_event_sync(lines)
            assert snapshot_event["event"] == "job.status"
            assert snapshot_event["data"]["job_id"] == str(job_id)
            assert snapshot_event["data"]["status"] == "pending"
            assert snapshot_event["data"]["cancel_requested_at"] is None
            assert snapshot_event["data"]["attempt_count"] == 0


def test_job_events_stream_initial_terminal_snapshot_closes(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")
    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-sse-terminal-snapshot",
        payload={"type": "success", "payload": {}},
    )
    job_id = uuid.UUID(str(create_response.json()["id"]))
    asyncio.run(update_job_status(database_url, job_id=job_id, status="cancelled"))

    with client.stream(
        "GET",
        f"/jobs/{job_id}/events",
        headers=authorization_header(token),
    ) as response:
        lines = response.iter_lines()
        snapshot_event = read_sse_event_sync(lines)

        assert snapshot_event["data"]["status"] == "cancelled"
        with pytest.raises(StopIteration):
            next(lines)


@pytest.mark.asyncio
async def test_job_events_stream_is_authorized_for_owner_and_admin(
    app: FastAPI,
    database_url: str,
) -> None:
    await insert_user(database_url, email="owner@example.com", password="secret-password")
    await insert_user(database_url, email="other@example.com", password="secret-password")
    await insert_user(
        database_url,
        email="admin@example.com",
        password="secret-password",
        role="admin",
    )

    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with (
            httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as owner_client,
            httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as other_client,
            httpx.AsyncClient(
                transport=transport,
                base_url="http://testserver",
            ) as admin_client,
        ):
            owner_token = await login_user_async(
                owner_client,
                email="owner@example.com",
                password="secret-password",
            )
            other_token = await login_user_async(
                other_client,
                email="other@example.com",
                password="secret-password",
            )
            admin_token = await login_user_async(
                admin_client,
                email="admin@example.com",
                password="secret-password",
            )
            create_response = await owner_client.post(
                "/jobs",
                json={"type": "success", "payload": {}},
                headers={
                    **authorization_header(owner_token),
                    "Idempotency-Key": "job-sse-auth",
                },
            )
            assert create_response.status_code == 201
            job_id = create_response.json()["id"]

            unauthorized_response = await other_client.get(
                f"/jobs/{job_id}/events",
                headers=authorization_header(other_token),
            )
            assert unauthorized_response.status_code == 404

            async with admin_client.stream(
                "GET",
                f"/jobs/{job_id}/events",
                headers=authorization_header(admin_token),
            ) as admin_stream:
                assert admin_stream.status_code == 200
                lines = admin_stream.aiter_lines()
                snapshot_event = await asyncio.wait_for(read_sse_event(lines), timeout=5)
                assert snapshot_event["data"]["job_id"] == job_id
                assert snapshot_event["data"]["status"] == "pending"
                assert snapshot_event["data"]["attempt_count"] == 0


def test_job_events_stream_terminal_live_event_closes_and_releases_subscription(
    app: FastAPI,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    fake_pubsub = FakeJobStatusPubSub(
        events=[
            JobStatusEvent(
                event_id=uuid.uuid4(),
                job_id=uuid.uuid4(),
                status=JobStatus.CANCELLED,
                attempt_count=1,
                occurred_at=datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
                cancel_requested_at=None,
            )
        ]
    )
    app.dependency_overrides[get_job_status_pubsub_from_request] = lambda: fake_pubsub

    try:
        with TestClient(app) as client:
            token = login_user(client, email="user@example.com", password="secret-password")
            create_response = create_job_request(
                client,
                token=token,
                idempotency_key="job-sse-terminal-live",
                payload={"type": "success", "payload": {}},
            )
            job_id = uuid.UUID(str(create_response.json()["id"]))
            fake_pubsub._events[0] = JobStatusEvent(
                event_id=uuid.uuid4(),
                job_id=job_id,
                status=JobStatus.CANCELLED,
                attempt_count=1,
                occurred_at=datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
                cancel_requested_at=None,
            )

            with client.stream(
                "GET",
                f"/jobs/{job_id}/events",
                headers=authorization_header(token),
            ) as response:
                assert response.status_code == 200
                lines = response.iter_lines()
                snapshot_event = read_sse_event_sync(lines)
                terminal_event = read_sse_event_sync(lines)

                assert snapshot_event["data"]["status"] == "pending"
                assert terminal_event["data"]["status"] == "cancelled"
                with pytest.raises(StopIteration):
                    next(lines)

        assert fake_pubsub.closed is True
    finally:
        app.dependency_overrides.pop(get_job_status_pubsub_from_request, None)


def test_job_events_stream_redis_failure_emits_error_hint_and_closes(
    app: FastAPI,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    fake_pubsub = FakeJobStatusPubSub(error=JobStatusPubSubError("redis down"))
    app.dependency_overrides[get_job_status_pubsub_from_request] = lambda: fake_pubsub

    try:
        with TestClient(app) as client:
            token = login_user(client, email="user@example.com", password="secret-password")
            create_response = create_job_request(
                client,
                token=token,
                idempotency_key="job-sse-redis-failure",
                payload={"type": "success", "payload": {}},
            )
            job_id = create_response.json()["id"]

            with client.stream(
                "GET",
                f"/jobs/{job_id}/events",
                headers=authorization_header(token),
            ) as response:
                assert response.status_code == 200
                lines = response.iter_lines()
                snapshot_event = read_sse_event_sync(lines)
                error_line = read_sse_event_sync(lines)

                assert snapshot_event["data"]["status"] == "pending"
                assert error_line["event"] == "stream.error"
                assert error_line["data"]["code"] == "reconnect"
                with pytest.raises(StopIteration):
                    next(lines)
        assert fake_pubsub.closed is True
    finally:
        app.dependency_overrides.pop(get_job_status_pubsub_from_request, None)


def test_job_events_stream_client_disconnect_releases_subscription(
    app: FastAPI,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    class IdlePubSub(FakeJobStatusPubSub):
        async def _iterator(self) -> AsyncIterator[object]:
            while True:
                await asyncio.sleep(1)
                if False:
                    yield None

    fake_pubsub = IdlePubSub()
    app.dependency_overrides[get_job_status_pubsub_from_request] = lambda: fake_pubsub

    try:
        with TestClient(app) as client:
            token = login_user(client, email="user@example.com", password="secret-password")
            create_response = create_job_request(
                client,
                token=token,
                idempotency_key="job-sse-disconnect",
                payload={"type": "success", "payload": {}},
            )
            job_id = create_response.json()["id"]

            with client.stream(
                "GET",
                f"/jobs/{job_id}/events",
                headers=authorization_header(token),
            ) as response:
                assert response.status_code == 200
                lines = response.iter_lines()
                snapshot_event = read_sse_event_sync(lines)
                assert snapshot_event["data"]["status"] == "pending"

        assert fake_pubsub.closed is True
    finally:
        app.dependency_overrides.pop(get_job_status_pubsub_from_request, None)


def test_create_and_cancel_publish_post_commit_notifications(
    app: FastAPI,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    class RecordingNotifier:
        def __init__(self) -> None:
            self.notifications: list[JobPostCommitNotification] = []

        async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
            self.notifications.append(notification)

    recorder = RecordingNotifier()

    with TestClient(app) as client:
        app.state.worker_post_commit_notifier = recorder
        token = login_user(client, email="user@example.com", password="secret-password")
        create_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={
                **authorization_header(token),
                "Idempotency-Key": "job-sse-notifier",
            },
        )
        assert create_response.status_code == 201
        job_id = create_response.json()["id"]

        cancel_response = client.post(
            f"/jobs/{job_id}/cancel",
            headers=authorization_header(token),
        )
        assert cancel_response.status_code == 200

    assert [notification.status for notification in recorder.notifications] == [
        JobStatus.PENDING,
        JobStatus.CANCELLED,
    ]
    assert [notification.attempt_count for notification in recorder.notifications] == [0, 0]


def test_failed_create_does_not_emit_pre_commit_notification(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    class RecordingNotifier:
        def __init__(self) -> None:
            self.notifications: list[JobPostCommitNotification] = []

        async def notify_job_state_changed(self, notification: JobPostCommitNotification) -> None:
            self.notifications.append(notification)

    recorder = RecordingNotifier()

    async def fail_create_dispatch_event(
        self: SqlAlchemyOutboxRepository,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> object:
        del self, job_id, available_at
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(
        SqlAlchemyOutboxRepository,
        "create_dispatch_event",
        fail_create_dispatch_event,
    )

    with TestClient(app, raise_server_exceptions=False) as client:
        app.state.worker_post_commit_notifier = recorder
        token = login_user(client, email="user@example.com", password="secret-password")
        response = create_job_request(
            client,
            token=token,
            idempotency_key="job-sse-precommit",
            payload={"type": "success", "payload": {}},
        )

    assert response.status_code == 500
    assert recorder.notifications == []


@pytest.mark.asyncio
async def test_redis_job_status_pubsub_delivers_live_event() -> None:
    redis_client = Redis.from_url(get_test_redis_url())
    job_id = uuid.uuid4()
    owner_id = uuid.uuid4()
    try:
        notifier = RedisPublishingWorkerPostCommitNotifier(RedisJobStatusPubSub(redis_client))
        async with RedisJobStatusPubSub(redis_client).subscribe(job_id) as events:

            async def publish() -> None:
                await asyncio.sleep(0.1)
                await notifier.notify_job_state_changed(
                    JobPostCommitNotification(
                        job_id=job_id,
                        owner_id=owner_id,
                        status=JobStatus.RUNNING,
                        attempt_count=1,
                        committed_at=datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
                    )
                )

            publish_task = asyncio.create_task(publish())
            event = await asyncio.wait_for(events.__anext__(), timeout=5)
            await publish_task

        assert event.job_id == job_id
        assert event.status == JobStatus.RUNNING
        assert event.attempt_count == 1
    finally:
        await redis_client.aclose()


def test_job_status_pubsub_channel_name_matches() -> None:
    job_id = uuid.uuid4()
    assert RedisJobStatusPubSub.channel_name(job_id) == f"jobs:events:{job_id}"


@pytest.mark.parametrize("initial_status", ["pending", "queued"])
def test_cancel_pending_or_queued_job_marks_it_cancelled(
    client: TestClient,
    database_url: str,
    initial_status: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-cancel-pending",
        payload={"type": "sleep", "payload": {"duration_seconds": 5}},
    )
    job_id = uuid.UUID(str(create_response.json()["id"]))
    if initial_status == "queued":
        asyncio.run(
            update_job_status(
                database_url,
                job_id=job_id,
                status="queued",
            )
        )

    cancel_response = client.post(
        f"/jobs/{job_id}/cancel",
        headers=authorization_header(token),
    )

    assert cancel_response.status_code == 200
    assert cancel_response.json()["status"] == "cancelled"
    logs_response = client.get(f"/jobs/{job_id}/logs", headers=authorization_header(token))
    assert [entry["message"] for entry in logs_response.json()] == [
        "Job created",
        "Job cancelled before execution",
    ]


def test_cancel_running_job_sets_cancel_requested_at_and_is_deterministic(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-cancel-running",
        payload={"type": "sleep", "payload": {"duration_seconds": 5}},
    )
    job_id = uuid.UUID(str(create_response.json()["id"]))
    started_at = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    execution_token = uuid.uuid4()
    asyncio.run(
        update_job_status(
            database_url,
            job_id=job_id,
            status="running",
            worker_id="worker-a",
            execution_token=execution_token,
            started_at=started_at,
        )
    )

    first_response = client.post(
        f"/jobs/{job_id}/cancel",
        headers=authorization_header(token),
    )
    second_response = client.post(
        f"/jobs/{job_id}/cancel",
        headers=authorization_header(token),
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert first_response.json()["status"] == "running"
    assert first_response.json()["cancel_requested_at"] is not None
    assert (
        second_response.json()["cancel_requested_at"]
        == first_response.json()["cancel_requested_at"]
    )
    logs_response = client.get(f"/jobs/{job_id}/logs", headers=authorization_header(token))
    assert [entry["message"] for entry in logs_response.json()] == [
        "Job created",
        "Job cancellation requested",
    ]


@pytest.mark.parametrize("terminal_status", ["completed", "failed"])
def test_cancel_terminal_job_returns_409(
    client: TestClient,
    database_url: str,
    terminal_status: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=token,
        idempotency_key=f"job-cancel-{terminal_status}",
        payload={"type": "success", "payload": {}},
    )
    job_id = uuid.UUID(str(create_response.json()["id"]))
    asyncio.run(update_job_status(database_url, job_id=job_id, status=terminal_status))

    response = client.post(f"/jobs/{job_id}/cancel", headers=authorization_header(token))

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "JOB_NOT_CANCELLABLE",
            "message": "Job cannot be cancelled in its current state",
            "details": None,
        }
    }


def test_cancel_already_cancelled_job_is_idempotent(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-cancel-already-cancelled",
        payload={"type": "success", "payload": {}},
    )
    job_id = create_response.json()["id"]

    first_response = client.post(f"/jobs/{job_id}/cancel", headers=authorization_header(token))
    second_response = client.post(f"/jobs/{job_id}/cancel", headers=authorization_header(token))

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert first_response.json() == second_response.json()
    assert second_response.json()["status"] == "cancelled"


def test_cancel_is_owner_scoped_and_admin_allowed(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="owner@example.com", password="secret-password"))
    asyncio.run(insert_user(database_url, email="other@example.com", password="secret-password"))
    asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )
    owner_token = login_user(client, email="owner@example.com", password="secret-password")
    other_token = login_user(client, email="other@example.com", password="secret-password")
    admin_token = login_user(client, email="admin@example.com", password="secret-password")

    create_response = create_job_request(
        client,
        token=owner_token,
        idempotency_key="job-cancel-auth",
        payload={"type": "success", "payload": {}},
    )
    job_id = create_response.json()["id"]

    denied_response = client.post(
        f"/jobs/{job_id}/cancel",
        headers=authorization_header(other_token),
    )
    admin_response = client.post(
        f"/jobs/{job_id}/cancel",
        headers=authorization_header(admin_token),
    )

    assert denied_response.status_code == 404
    assert admin_response.status_code == 200
    assert admin_response.json()["status"] == "cancelled"


def test_get_jobs_lists_only_owner_jobs_with_cursor_pagination(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="owner@example.com", password="secret-password"))
    asyncio.run(insert_user(database_url, email="other@example.com", password="secret-password"))
    owner_token = login_user(client, email="owner@example.com", password="secret-password")
    other_token = login_user(client, email="other@example.com", password="secret-password")

    first = create_job_request(
        client,
        token=owner_token,
        idempotency_key="owner-job-1",
        payload={"type": "success", "payload": {}},
    ).json()
    second = create_job_request(
        client,
        token=owner_token,
        idempotency_key="owner-job-2",
        payload={"type": "success", "payload": {}},
    ).json()
    create_job_request(
        client,
        token=other_token,
        idempotency_key="other-job-1",
        payload={"type": "success", "payload": {}},
    )

    first_page = client.get("/jobs?limit=1", headers=authorization_header(owner_token))

    assert first_page.status_code == 200
    assert len(first_page.json()["items"]) == 1
    assert first_page.json()["items"][0]["id"] == second["id"]
    assert first_page.json()["next_cursor"] is not None

    second_page = client.get(
        f"/jobs?limit=1&cursor={first_page.json()['next_cursor']}",
        headers=authorization_header(owner_token),
    )

    assert second_page.status_code == 200
    assert [item["id"] for item in second_page.json()["items"]] == [first["id"]]
    assert second_page.json()["next_cursor"] is None


def test_get_jobs_keyset_pagination_is_stable_with_equal_created_at(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")
    created_ids = [
        uuid.UUID(
            str(
                create_job_request(
                    client,
                    token=token,
                    idempotency_key=f"stable-page-{index}",
                    payload={"type": "success", "payload": {}},
                ).json()["id"]
            )
        )
        for index in range(3)
    ]
    same_timestamp = datetime(2026, 6, 18, 12, 0, tzinfo=UTC)
    asyncio.run(set_jobs_created_at(database_url, job_ids=created_ids, created_at=same_timestamp))

    seen_ids: list[str] = []
    cursor: str | None = None
    while True:
        path = "/jobs?limit=1" if cursor is None else f"/jobs?limit=1&cursor={cursor}"
        response = client.get(path, headers=authorization_header(token))
        assert response.status_code == 200
        items = response.json()["items"]
        seen_ids.extend(item["id"] for item in items)
        cursor = response.json()["next_cursor"]
        if cursor is None:
            break

    assert len(seen_ids) == 3
    assert len(set(seen_ids)) == 3
    assert set(seen_ids) == {str(job_id) for job_id in created_ids}


def test_get_jobs_admin_sees_all_jobs(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user1@example.com", password="secret-password"))
    asyncio.run(insert_user(database_url, email="user2@example.com", password="secret-password"))
    asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )
    user1_token = login_user(client, email="user1@example.com", password="secret-password")
    user2_token = login_user(client, email="user2@example.com", password="secret-password")
    admin_token = login_user(client, email="admin@example.com", password="secret-password")

    create_job_request(
        client,
        token=user1_token,
        idempotency_key="admin-list-user1",
        payload={"type": "success", "payload": {}},
    )
    create_job_request(
        client,
        token=user2_token,
        idempotency_key="admin-list-user2",
        payload={"type": "success", "payload": {}},
    )

    response = client.get("/jobs", headers=authorization_header(admin_token))

    assert response.status_code == 200
    assert len(response.json()["items"]) == 2


def test_get_jobs_rejects_invalid_cursor(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.get("/jobs?cursor=not-a-valid-cursor", headers=authorization_header(token))

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "INVALID_CURSOR",
            "message": "Cursor is invalid",
            "details": None,
        }
    }


@pytest.mark.parametrize(
    "cursor",
    [
        base64.urlsafe_b64encode(b"{}").decode("ascii"),
        base64.urlsafe_b64encode(
            json.dumps(
                {"v": 2, "created_at": "2026-06-18T12:00:00+00:00", "id": str(uuid.uuid4())}
            ).encode("utf-8")
        ).decode("ascii"),
        base64.urlsafe_b64encode(
            json.dumps({"v": 1, "created_at": "bad-date", "id": str(uuid.uuid4())}).encode("utf-8")
        ).decode("ascii"),
        base64.urlsafe_b64encode(
            json.dumps(
                {
                    "v": 1,
                    "created_at": "2026-06-18T12:00:00+00:00",
                    "id": "bad-uuid",
                }
            ).encode("utf-8")
        ).decode("ascii"),
        base64.urlsafe_b64encode(
            json.dumps(
                {
                    "v": 1,
                    "created_at": "2026-06-18T12:00:00+00:00",
                    "id": str(uuid.uuid4()),
                    "extra": True,
                }
            ).encode("utf-8")
        ).decode("ascii"),
    ],
)
def test_get_jobs_rejects_strictly_invalid_cursor_payloads(
    client: TestClient,
    database_url: str,
    cursor: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.get(f"/jobs?cursor={cursor}", headers=authorization_header(token))

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_CURSOR"


def test_get_jobs_cache_is_invalidated_by_create_and_cancel(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    first_create = create_job_request(
        client,
        token=token,
        idempotency_key="cache-job-1",
        payload={"type": "success", "payload": {}},
    ).json()
    cached_response = client.get("/jobs", headers=authorization_header(token))
    assert [item["id"] for item in cached_response.json()["items"]] == [first_create["id"]]

    asyncio.run(delete_job(database_url, uuid.UUID(str(first_create["id"]))))
    stale_cached_response = client.get("/jobs", headers=authorization_header(token))
    assert [item["id"] for item in stale_cached_response.json()["items"]] == [first_create["id"]]

    second_create = create_job_request(
        client,
        token=token,
        idempotency_key="cache-job-2",
        payload={"type": "success", "payload": {}},
    ).json()
    invalidated_response = client.get("/jobs", headers=authorization_header(token))
    assert [item["id"] for item in invalidated_response.json()["items"]] == [second_create["id"]]

    cancel_response = client.post(
        f"/jobs/{second_create['id']}/cancel",
        headers=authorization_header(token),
    )
    assert cancel_response.status_code == 200
    refreshed_response = client.get("/jobs", headers=authorization_header(token))
    assert refreshed_response.json()["items"][0]["status"] == "cancelled"


def test_get_job_logs_are_capped_and_ordered(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")
    create_response = create_job_request(
        client,
        token=token,
        idempotency_key="job-logs-cap",
        payload={"type": "success", "payload": {}},
    )
    job_id = uuid.UUID(str(create_response.json()["id"]))
    asyncio.run(
        insert_job_logs(
            database_url,
            job_id=job_id,
            total=550,
            created_at=datetime(2099, 1, 1, 0, 0, tzinfo=UTC),
        )
    )

    response = client.get(f"/jobs/{job_id}/logs", headers=authorization_header(token))

    assert response.status_code == 200
    assert len(response.json()) == 500
    assert response.json()[0]["message"] == "Job created"
    assert response.json()[-1]["message"] == "log-498"


def test_get_jobs_cache_hit_avoids_db_query_and_scopes_user_admin_separately(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )
    recorded_cache_keys: list[str] = []

    with TestClient(app) as client:
        user_token = login_user(client, email="user@example.com", password="secret-password")
        admin_token = login_user(client, email="admin@example.com", password="secret-password")

        original_set = app.state.job_list_cache._redis.set

        async def recording_set(name: str, value: str, *args: object, **kwargs: object) -> object:
            recorded_cache_keys.append(name)
            return await original_set(name, value, *args, **kwargs)

        monkeypatch.setattr(app.state.job_list_cache._redis, "set", recording_set)

        create_job_request(
            client,
            token=user_token,
            idempotency_key="cache-scope-job",
            payload={"type": "success", "payload": {}},
        )
        user_first = client.get("/jobs", headers=authorization_header(user_token))
        admin_first = client.get("/jobs", headers=authorization_header(admin_token))
        assert user_first.status_code == 200
        assert admin_first.status_code == 200

        async def fail_owner_query(*args: object, **kwargs: object) -> object:
            raise AssertionError("owner DB query should not run on cache hit")

        async def fail_admin_query(*args: object, **kwargs: object) -> object:
            raise AssertionError("admin DB query should not run on cache hit")

        monkeypatch.setattr(SqlAlchemyJobRepository, "list_for_owner_keyset", fail_owner_query)
        monkeypatch.setattr(SqlAlchemyJobRepository, "list_all_keyset", fail_admin_query)

        user_cached = client.get("/jobs", headers=authorization_header(user_token))
        admin_cached = client.get("/jobs", headers=authorization_header(admin_token))

    assert user_cached.status_code == 200
    assert admin_cached.status_code == 200
    assert user_cached.json() == user_first.json()
    assert admin_cached.json() == admin_first.json()
    assert any(key.startswith("jobs:list:user:") for key in recorded_cache_keys)
    assert any(key.startswith("jobs:list:admin:") for key in recorded_cache_keys)


def test_get_jobs_redis_failure_falls_back_to_db(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    with TestClient(app) as client:
        token = login_user(client, email="user@example.com", password="secret-password")
        create_job_request(
            client,
            token=token,
            idempotency_key="cache-fallback-job",
            payload={"type": "success", "payload": {}},
        )

        async def fail_redis_get(*args: object, **kwargs: object) -> object:
            raise RuntimeError("redis down")

        monkeypatch.setattr(app.state.job_list_cache._redis, "get", fail_redis_get)
        response = client.get("/jobs", headers=authorization_header(token))

    assert response.status_code == 200
    assert len(response.json()["items"]) == 1


@pytest.mark.asyncio
async def test_worker_post_commit_notifier_invalidates_user_and_admin_versions(
    app: FastAPI,
    database_url: str,
) -> None:
    owner_id = await insert_user(database_url, email="user@example.com", password="secret-password")
    async with app.router.lifespan_context(app):
        notifier = app.state.worker_post_commit_notifier
        redis_client = app.state.redis_client

        await notifier.notify_job_state_changed(
            JobPostCommitNotification(
                job_id=uuid.uuid4(),
                owner_id=owner_id,
                status=JobStatus.RUNNING,
                attempt_count=1,
                committed_at=datetime(2026, 6, 18, 12, 0, tzinfo=UTC),
            )
        )

        owner_version = await redis_client.get(f"jobs:list:version:user:{owner_id}")
        admin_version = await redis_client.get("jobs:list:version:admin")
    assert owner_version is not None
    assert admin_version is not None


def test_first_ten_genuinely_new_create_requests_are_accepted_and_eleventh_is_rejected(
    app: FastAPI,
    database_url: str,
) -> None:
    app.state.test_rate_limiter = CountingRateLimiter(limit=10, reset_epoch=1_700_000_120)
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    with TestClient(app) as client:
        token = login_user(client, email="user@example.com", password="secret-password")
        payload = {"type": "success", "payload": {}}
        responses = [
            client.post(
                "/jobs",
                json=payload,
                headers={
                    **authorization_header(token),
                    "Idempotency-Key": f"job-rate-limited-{index}",
                },
            )
            for index in range(11)
        ]

    assert all(response.status_code == 201 for response in responses[:10])
    rejected_response = responses[10]
    assert rejected_response.status_code == 429
    assert rejected_response.json() == {
        "error": {
            "code": "RATE_LIMIT_EXCEEDED",
            "message": "Create job rate limit exceeded",
            "details": None,
        }
    }
    assert rejected_response.headers["Retry-After"] == "30"
    assert rejected_response.headers["X-RateLimit-Limit"] == "10"
    assert rejected_response.headers["X-RateLimit-Remaining"] == "0"
    assert rejected_response.headers["X-RateLimit-Reset"] == "1700000120"


def test_create_job_rate_limit_is_scoped_per_user(
    app: FastAPI,
    database_url: str,
) -> None:
    app.state.test_rate_limiter = CountingRateLimiter(limit=1)
    asyncio.run(insert_user(database_url, email="user1@example.com", password="secret-password"))
    asyncio.run(insert_user(database_url, email="user2@example.com", password="secret-password"))

    with TestClient(app) as client:
        user1_token = login_user(client, email="user1@example.com", password="secret-password")
        user2_token = login_user(client, email="user2@example.com", password="secret-password")
        payload = {"type": "success", "payload": {}}

        user1_first = client.post(
            "/jobs",
            json=payload,
            headers={
                **authorization_header(user1_token),
                "Idempotency-Key": "user1-job",
            },
        )
        user1_second = client.post(
            "/jobs",
            json=payload,
            headers={
                **authorization_header(user1_token),
                "Idempotency-Key": "user1-job-2",
            },
        )
        user2_first = client.post(
            "/jobs",
            json=payload,
            headers={
                **authorization_header(user2_token),
                "Idempotency-Key": "user2-job",
            },
        )

    assert user1_first.status_code == 201
    assert user1_second.status_code == 429
    assert user2_first.status_code == 201


def test_idempotent_replay_does_not_consume_rate_limit_quota(
    app: FastAPI,
    database_url: str,
) -> None:
    rate_limiter = CountingRateLimiter(limit=2)
    app.state.test_rate_limiter = rate_limiter
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )

    with TestClient(app) as client:
        token = login_user(client, email="user@example.com", password="secret-password")
        first_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-1"},
        )
        replay_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-1"},
        )
        second_new_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-2"},
        )
        third_new_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-3"},
        )

    assert first_response.status_code == 201
    assert replay_response.status_code == 200
    assert second_new_response.status_code == 201
    assert third_new_response.status_code == 429
    assert rate_limiter.reserve_calls[owner_id] == 3


def test_idempotency_conflict_does_not_consume_rate_limit_quota(
    app: FastAPI,
    database_url: str,
) -> None:
    rate_limiter = CountingRateLimiter(limit=2)
    app.state.test_rate_limiter = rate_limiter
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )

    with TestClient(app) as client:
        token = login_user(client, email="user@example.com", password="secret-password")
        first_response = client.post(
            "/jobs",
            json={"type": "sleep", "payload": {"duration_seconds": 5}},
            headers={**authorization_header(token), "Idempotency-Key": "job-1"},
        )
        conflict_response = client.post(
            "/jobs",
            json={"type": "sleep", "payload": {"duration_seconds": 7}},
            headers={**authorization_header(token), "Idempotency-Key": "job-1"},
        )
        second_new_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-2"},
        )
        third_new_response = client.post(
            "/jobs",
            json={"type": "success", "payload": {}},
            headers={**authorization_header(token), "Idempotency-Key": "job-3"},
        )

    assert first_response.status_code == 201
    assert conflict_response.status_code == 409
    assert second_new_response.status_code == 201
    assert third_new_response.status_code == 429
    assert rate_limiter.reserve_calls[owner_id] == 3


def test_same_key_and_different_payload_returns_409(
    client: TestClient,
    database_url: str,
) -> None:
    owner_id = asyncio.run(
        insert_user(database_url, email="user@example.com", password="secret-password")
    )
    token = login_user(client, email="user@example.com", password="secret-password")
    headers = {
        **authorization_header(token),
        "Idempotency-Key": "job-1",
    }

    client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": 5}},
        headers=headers,
    )
    response = client.post(
        "/jobs", json={"type": "sleep", "payload": {"duration_seconds": 7}}, headers=headers
    )

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "IDEMPOTENCY_KEY_REUSED",
            "message": "Idempotency key conflicts with a different request",
            "details": None,
        }
    }
    job_record = asyncio.run(
        fetch_job_by_owner_and_key(database_url, owner_id=owner_id, idempotency_key="job-1")
    )
    assert job_record is not None


async def test_concurrent_duplicate_requests_create_one_job_and_one_outbox_event(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rate_limiter = CountingRateLimiter(limit=10)
    app.state.test_rate_limiter = rate_limiter
    owner_id = await insert_user(
        database_url,
        email="user@example.com",
        password="secret-password",
    )
    original_get_by_idempotency_key = SqlAlchemyJobRepository.get_by_idempotency_key
    barrier = asyncio.Barrier(2)
    pending_reads = 0

    async def synchronized_get_by_idempotency_key(
        self: SqlAlchemyJobRepository,
        owner_id_arg: uuid.UUID,
        key: str,
    ) -> StoredJobDTO | None:
        nonlocal pending_reads
        job = await original_get_by_idempotency_key(self, owner_id_arg, key)
        if owner_id_arg == owner_id and key == "job-1" and job is None and pending_reads < 2:
            pending_reads += 1
            await barrier.wait()
        return job

    monkeypatch.setattr(
        SqlAlchemyJobRepository,
        "get_by_idempotency_key",
        synchronized_get_by_idempotency_key,
    )

    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as async_client:
            token = await login_user_async(
                async_client,
                email="user@example.com",
                password="secret-password",
            )
            headers = {
                **authorization_header(token),
                "Idempotency-Key": "job-1",
            }
            payload = {"type": "sleep", "payload": {"duration_seconds": 5}}
            responses = await asyncio.gather(
                async_client.post("/jobs", json=payload, headers=headers),
                async_client.post("/jobs", json=payload, headers=headers),
            )

    status_codes = sorted(response.status_code for response in responses)
    replayed_values = sorted(response.headers["Idempotency-Replayed"] for response in responses)
    job_record = await fetch_job_by_owner_and_key(
        database_url,
        owner_id=owner_id,
        idempotency_key="job-1",
    )

    assert status_codes == [200, 201]
    assert replayed_values == ["false", "true"]
    assert rate_limiter.reserve_calls[owner_id] == 2
    assert rate_limiter.release_calls == 1
    assert job_record is not None

    job_id = uuid.UUID(str(job_record["id"]))
    assert await count_job_logs_for_job(database_url, job_id) == 1
    assert await count_outbox_events_for_job(database_url, job_id) == 1


async def test_real_postgres_backed_concurrent_duplicate_requests_create_one_job(
    app: FastAPI,
    database_url: str,
) -> None:
    owner_id = await insert_user(
        database_url,
        email="user@example.com",
        password="secret-password",
    )

    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as async_client:
            token = await login_user_async(
                async_client,
                email="user@example.com",
                password="secret-password",
            )
            headers = {
                **authorization_header(token),
                "Idempotency-Key": "job-real-concurrent",
            }
            payload = {"type": "sleep", "payload": {"duration_seconds": 5}}
            responses = await asyncio.gather(
                *[async_client.post("/jobs", json=payload, headers=headers) for _ in range(5)]
            )

    status_codes = sorted(response.status_code for response in responses)
    replayed_values = sorted(response.headers["Idempotency-Replayed"] for response in responses)
    job_record = await fetch_job_by_owner_and_key(
        database_url,
        owner_id=owner_id,
        idempotency_key="job-real-concurrent",
    )

    assert status_codes == [200, 200, 200, 200, 201]
    assert replayed_values == ["false", "true", "true", "true", "true"]
    assert job_record is not None

    job_id = uuid.UUID(str(job_record["id"]))
    assert await count_job_logs_for_job(database_url, job_id) == 1
    assert await count_outbox_events_for_job(database_url, job_id) == 1


def test_failed_job_creation_creates_neither_joblog_nor_outboxevent(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rate_limiter = CountingRateLimiter(limit=10)
    app.state.test_rate_limiter = rate_limiter
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))

    async def fail_create_dispatch_event(
        self: SqlAlchemyOutboxRepository,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> object:
        del self, job_id, available_at
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(
        SqlAlchemyOutboxRepository,
        "create_dispatch_event",
        fail_create_dispatch_event,
    )

    with TestClient(app, raise_server_exceptions=False) as test_client:
        token = login_user(test_client, email="user@example.com", password="secret-password")
        response = test_client.post(
            "/jobs",
            json={"type": "sleep", "payload": {"duration_seconds": 5}},
            headers={
                **authorization_header(token),
                "Idempotency-Key": "job-1",
            },
        )

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An internal server error occurred",
            "details": None,
        }
    }
    assert rate_limiter.release_calls == 1
    assert asyncio.run(count_rows_by_idempotency_key(database_url, idempotency_key="job-1")) == (
        0,
        0,
        0,
    )


def test_invalid_handler_payload_is_rejected_before_persistence(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={
            "type": "sleep",
            "payload": {"duration_seconds": 5, "command": "rm -rf /"},
        },
        headers={
            **authorization_header(token),
            "Idempotency-Key": "job-1",
        },
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
    assert asyncio.run(count_rows_by_idempotency_key(database_url, idempotency_key="job-1")) == (
        0,
        0,
        0,
    )


def test_sleep_duration_seconds_between_1_and_30_is_accepted(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": 30}},
        headers={**authorization_header(token), "Idempotency-Key": "job-30"},
    )

    assert response.status_code == 201


@pytest.mark.parametrize("duration_seconds", [0, 31])
def test_sleep_duration_seconds_out_of_range_is_rejected(
    client: TestClient,
    database_url: str,
    duration_seconds: int,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"duration_seconds": duration_seconds}},
        headers={**authorization_header(token), "Idempotency-Key": f"job-{duration_seconds}"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"


def test_old_sleep_field_name_seconds_is_rejected(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "sleep", "payload": {"seconds": 5}},
        headers={**authorization_header(token), "Idempotency-Key": "job-old-field"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"


def test_failure_payload_with_valid_message_is_accepted(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "failure", "payload": {"message": "expected failure"}},
        headers={**authorization_header(token), "Idempotency-Key": "job-failure"},
    )

    assert response.status_code == 201


def test_failure_message_longer_than_500_is_rejected(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "failure", "payload": {"message": "x" * 501}},
        headers={**authorization_header(token), "Idempotency-Key": "job-failure-long"},
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "INVALID_REQUEST"


@pytest.mark.parametrize(
    "key",
    [
        " " * 3,
        "x" * 129,
    ],
)
def test_invalid_idempotency_key_values_are_rejected(
    client: TestClient,
    database_url: str,
    key: str,
) -> None:
    asyncio.run(insert_user(database_url, email="user@example.com", password="secret-password"))
    token = login_user(client, email="user@example.com", password="secret-password")

    response = client.post(
        "/jobs",
        json={"type": "success", "payload": {}},
        headers={**authorization_header(token), "Idempotency-Key": key},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_REQUEST"
