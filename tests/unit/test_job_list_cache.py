from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest

from app.application.dto import JobListPageDTO, JobPublicDTO
from app.core.config import Settings
from app.domain.enums import JobStatus, JobType
from app.infrastructure.redis import job_list_cache as job_list_cache_module
from app.infrastructure.redis.job_list_cache import RedisJobListCache


class RaisingRedis:
    async def get(self, key: str) -> None:
        del key
        raise RuntimeError("redis unavailable")

    async def set(self, key: str, value: str, *, ex: int) -> None:
        del key, value, ex
        raise RuntimeError("redis unavailable")

    def pipeline(self, *, transaction: bool = True) -> Any:
        del transaction
        raise RuntimeError("redis unavailable")


@pytest.mark.asyncio
async def test_job_list_cache_failures_are_fail_open(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job",
        REDIS_URL="redis://127.0.0.1:6379/0",
        RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/",
        JWT_SECRET="x" * 32,
    )
    cache = RedisJobListCache(RaisingRedis(), settings)
    now = datetime.now(UTC)
    warning_calls: list[str] = []
    page = JobListPageDTO(
        items=[
            JobPublicDTO(
                id=uuid.uuid4(),
                owner_id=uuid.uuid4(),
                type=JobType.SUCCESS,
                payload={},
                status=JobStatus.PENDING,
                idempotency_key="job-1",
                created_at=now,
                updated_at=now,
            )
        ],
        next_cursor=None,
    )

    def capture_warning(message: str, *args: Any, **kwargs: Any) -> None:
        del args, kwargs
        warning_calls.append(message)

    monkeypatch.setattr(job_list_cache_module.logger, "warning", capture_warning)

    assert await cache.get_page(owner_id=uuid.uuid4(), cursor=None, limit=20) is None
    await cache.set_page(owner_id=uuid.uuid4(), cursor=None, limit=20, page=page)
    await cache.invalidate_owner(uuid.uuid4())

    assert warning_calls == [
        "job_list_cache_get_failed",
        "job_list_cache_set_failed",
        "job_list_cache_invalidation_failed",
    ]
