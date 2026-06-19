from __future__ import annotations

import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Self, cast

import pytest

from app.api.routes.jobs import validate_idempotency_key
from app.application.caching.job_list_cache import NoOpJobListCache
from app.application.dto import (
    CreateJobRateLimitReservationDTO,
    CurrentUserDTO,
    JobLogEntryDTO,
    StoredJobDTO,
)
from app.application.rate_limits import CreateJobRateLimiter
from app.application.services.jobs import JobService
from app.core.config import Settings
from app.core.exceptions import InvalidRequestError, ResourceNotFoundError
from app.db.uow import UnitOfWorkFactory
from app.domain.enums import JobStatus, JobType, UserRole
from app.workers.post_commit import NoOpWorkerPostCommitNotifier


def test_application_services_do_not_import_sqlalchemy_or_sessions() -> None:
    service_paths = [
        Path("app/application/services/auth.py"),
        Path("app/application/services/jobs.py"),
    ]

    for service_path in service_paths:
        source = service_path.read_text(encoding="utf-8")
        assert "sqlalchemy" not in source
        assert "AsyncSession" not in source
        assert "app.models." not in source
        assert "from app.models" not in source


@pytest.mark.parametrize("value", ["ключ", " " * 3, "x" * 129])
def test_validate_idempotency_key_rejects_invalid_values(value: str) -> None:
    with pytest.raises(InvalidRequestError) as exc_info:
        validate_idempotency_key(value)

    assert exc_info.value.status_code == 400


class FakeJobRepo:
    def __init__(self, stored_job: StoredJobDTO) -> None:
        self._stored_job = stored_job
        self.get_by_id_calls = 0
        self.get_by_id_for_owner_calls = 0

    async def get_by_id(self, job_id: uuid.UUID) -> StoredJobDTO | None:
        self.get_by_id_calls += 1
        return self._stored_job if self._stored_job.id == job_id else None

    async def get_by_id_for_owner(
        self,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> StoredJobDTO | None:
        self.get_by_id_for_owner_calls += 1
        if self._stored_job.id == job_id and self._stored_job.owner_id == owner_id:
            return self._stored_job
        return None


class FakeJobLogRepo:
    def __init__(self, entries: list[JobLogEntryDTO]) -> None:
        self._entries = entries

    async def list_entries_for_job(self, job_id: uuid.UUID) -> list[JobLogEntryDTO]:
        return [entry for entry in self._entries if entry.job_id == job_id]


class FakeUoW:
    def __init__(
        self,
        *,
        jobs: FakeJobRepo,
        job_logs: FakeJobLogRepo,
    ) -> None:
        self.jobs = jobs
        self.job_logs = job_logs

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        del exc_type, exc, tb


class NoOpRateLimiter:
    async def reserve(self, user_id: uuid.UUID) -> CreateJobRateLimitReservationDTO | None:
        del user_id
        return None

    async def release(self, reservation: CreateJobRateLimitReservationDTO) -> None:
        del reservation


def build_job_service(stored_job: StoredJobDTO) -> tuple[JobService, FakeJobRepo]:
    job_repo = FakeJobRepo(stored_job)
    job_log_repo = FakeJobLogRepo(
        [
            JobLogEntryDTO(
                id=1,
                job_id=stored_job.id,
                attempt_number=None,
                level="info",
                message="Job created",
                created_at=stored_job.created_at,
            )
        ]
    )

    def uow_factory() -> FakeUoW:
        return FakeUoW(jobs=job_repo, job_logs=job_log_repo)

    settings = Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job",
        REDIS_URL="redis://127.0.0.1:6379/0",
        RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/",
        JWT_SECRET="x" * 32,
    )
    return (
        JobService(
            uow_factory=cast(UnitOfWorkFactory, uow_factory),
            settings=settings,
            rate_limiter=cast(CreateJobRateLimiter, NoOpRateLimiter()),
            job_list_cache=NoOpJobListCache(),
            post_commit_notifier=NoOpWorkerPostCommitNotifier(),
        ),
        job_repo,
    )


def build_stored_job() -> StoredJobDTO:
    now = datetime.now(UTC)
    owner_id = uuid.uuid4()
    return StoredJobDTO(
        id=uuid.uuid4(),
        owner_id=owner_id,
        type=JobType.SLEEP,
        payload={"duration_seconds": 5},
        status=JobStatus.PENDING,
        idempotency_key="job-1",
        request_hash="a" * 64,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.asyncio
async def test_job_service_uses_owner_scoped_lookup_for_normal_user() -> None:
    stored_job = build_stored_job()
    service, job_repo = build_job_service(stored_job)
    current_user = CurrentUserDTO(
        id=stored_job.owner_id,
        email="user@example.com",
        role=UserRole.USER,
        is_active=True,
    )

    job = await service.get_job_detail(current_user=current_user, job_id=stored_job.id)

    assert job.id == stored_job.id
    assert job_repo.get_by_id_for_owner_calls == 1
    assert job_repo.get_by_id_calls == 0


@pytest.mark.asyncio
async def test_job_service_uses_unrestricted_lookup_for_admin() -> None:
    stored_job = build_stored_job()
    service, job_repo = build_job_service(stored_job)
    current_user = CurrentUserDTO(
        id=uuid.uuid4(),
        email="admin@example.com",
        role=UserRole.ADMIN,
        is_active=True,
    )

    logs = await service.list_job_logs(current_user=current_user, job_id=stored_job.id)

    assert len(logs) == 1
    assert job_repo.get_by_id_calls == 1
    assert job_repo.get_by_id_for_owner_calls == 0


@pytest.mark.asyncio
async def test_job_service_raises_not_found_when_user_accesses_another_users_job() -> None:
    stored_job = build_stored_job()
    service, _ = build_job_service(stored_job)
    current_user = CurrentUserDTO(
        id=uuid.uuid4(),
        email="user@example.com",
        role=UserRole.USER,
        is_active=True,
    )

    with pytest.raises(ResourceNotFoundError):
        await service.get_job_detail(current_user=current_user, job_id=stored_job.id)
