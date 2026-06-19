from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from app.core.config import Settings
from app.infrastructure.redis import rate_limiter as rate_limiter_module
from app.infrastructure.redis.rate_limiter import (
    KEY_EXPIRY_MARGIN_SECONDS,
    RateLimitExceededError,
    RedisCreateJobRateLimiter,
)


class FakeClock:
    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self) -> datetime:
        return self._now

    def set(self, now: datetime) -> None:
        self._now = now


class FakeRedisClient:
    def __init__(self) -> None:
        self._counts: dict[str, int] = {}
        self.expiry_seconds_by_key: dict[str, int] = {}
        self.raise_on_eval = False

    async def eval(self, script: str, numkeys: int, *keys_and_args: object) -> object:
        if self.raise_on_eval:
            raise RuntimeError("redis unavailable")
        del numkeys
        key = str(keys_and_args[0])
        if "DECR" in script:
            current_count = self._counts.get(key, 0)
            if current_count <= 1:
                self._counts.pop(key, None)
                return 0
            current_count -= 1
            self._counts[key] = current_count
            return current_count

        expiry_seconds = int(keys_and_args[1])
        current_count = self._counts.get(key, 0) + 1
        self._counts[key] = current_count
        self.expiry_seconds_by_key.setdefault(key, expiry_seconds)
        return [current_count, expiry_seconds]


def build_settings() -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job",
        REDIS_URL="redis://127.0.0.1:6379/0",
        RABBITMQ_URL="amqp://guest:guest@127.0.0.1:5672/",
        JWT_SECRET="x" * 32,
        CREATE_JOB_RATE_LIMIT=10,
        CREATE_JOB_RATE_WINDOW_SECONDS=60,
    )


def build_limiter(
    client: FakeRedisClient,
    *,
    now: datetime,
) -> tuple[RedisCreateJobRateLimiter, FakeClock]:
    clock = FakeClock(now)
    limiter = RedisCreateJobRateLimiter(client, build_settings(), clock=clock)
    return limiter, clock


@pytest.mark.asyncio
async def test_first_ten_new_requests_are_accepted_and_eleventh_is_rejected() -> None:
    client = FakeRedisClient()
    limiter, _clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    user_id = uuid.uuid4()

    for _ in range(10):
        reservation = await limiter.reserve(user_id)
        assert reservation is not None

    with pytest.raises(RateLimitExceededError) as exc_info:
        await limiter.reserve(user_id)

    assert exc_info.value.headers == {
        "Retry-After": "60",
        "X-RateLimit-Limit": "10",
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Reset": "1781697660",
    }


@pytest.mark.asyncio
async def test_new_fixed_window_resets_the_limit() -> None:
    client = FakeRedisClient()
    limiter, clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    user_id = uuid.uuid4()

    for _ in range(10):
        await limiter.reserve(user_id)

    with pytest.raises(RateLimitExceededError):
        await limiter.reserve(user_id)

    clock.set(datetime(2026, 6, 17, 12, 1, tzinfo=UTC))
    reservation = await limiter.reserve(user_id)

    assert reservation is not None
    assert reservation.used == 1
    assert reservation.remaining == 9


@pytest.mark.asyncio
async def test_different_users_have_independent_limits() -> None:
    client = FakeRedisClient()
    limiter, _clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    first_user = uuid.uuid4()
    second_user = uuid.uuid4()

    for _ in range(10):
        await limiter.reserve(first_user)

    with pytest.raises(RateLimitExceededError):
        await limiter.reserve(first_user)

    reservation = await limiter.reserve(second_user)

    assert reservation is not None
    assert reservation.used == 1


@pytest.mark.asyncio
async def test_fixed_window_key_contains_user_and_window_start_epoch() -> None:
    client = FakeRedisClient()
    limiter, _clock = build_limiter(
        client,
        now=datetime(2026, 6, 17, 12, 0, 5, tzinfo=UTC),
    )
    user_id = uuid.uuid4()

    reservation = await limiter.reserve(user_id)

    assert reservation is not None
    expected_window_start = int(datetime(2026, 6, 17, 12, 0, tzinfo=UTC).timestamp())
    assert reservation.key == f"rate:create-job:{user_id}:{expected_window_start}"
    assert client.expiry_seconds_by_key[reservation.key] == 60 + KEY_EXPIRY_MARGIN_SECONDS


@pytest.mark.asyncio
async def test_release_returns_quota_to_the_current_window() -> None:
    client = FakeRedisClient()
    limiter, _clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    user_id = uuid.uuid4()

    reservation = await limiter.reserve(user_id)
    assert reservation is not None

    await limiter.release(reservation)

    new_reservation = await limiter.reserve(user_id)
    assert new_reservation is not None
    assert new_reservation.used == 1


@pytest.mark.asyncio
async def test_redis_reserve_failure_is_fail_open_and_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeRedisClient()
    client.raise_on_eval = True
    limiter, _clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    warning_calls: list[str] = []

    def capture_warning(message: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        warning_calls.append(message)

    monkeypatch.setattr(rate_limiter_module.logger, "warning", capture_warning)
    reservation = await limiter.reserve(uuid.uuid4())

    assert reservation is None
    assert warning_calls == ["create_job_rate_limit_fail_open"]


@pytest.mark.asyncio
async def test_redis_release_failure_is_fail_open_and_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeRedisClient()
    limiter, _clock = build_limiter(client, now=datetime(2026, 6, 17, 12, 0, tzinfo=UTC))
    reservation = await limiter.reserve(uuid.uuid4())
    assert reservation is not None
    client.raise_on_eval = True
    warning_calls: list[str] = []

    def capture_warning(message: str, *args: object, **kwargs: object) -> None:
        del args, kwargs
        warning_calls.append(message)

    monkeypatch.setattr(rate_limiter_module.logger, "warning", capture_warning)
    await limiter.release(reservation)

    assert warning_calls == ["create_job_rate_limit_fail_open"]
