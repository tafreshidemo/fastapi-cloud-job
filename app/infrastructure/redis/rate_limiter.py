from __future__ import annotations

import logging
from collections.abc import Awaitable
from datetime import UTC, datetime
from typing import Any, Protocol, cast
from uuid import UUID

from app.application.dto import CreateJobRateLimitReservationDTO
from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.core.exceptions import ApplicationError

logger = logging.getLogger(__name__)

KEY_EXPIRY_MARGIN_SECONDS = 5

CREATE_JOB_RATE_LIMIT_SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
local ttl = redis.call('TTL', KEYS[1])
return {current, ttl}
"""

RELEASE_CREATE_JOB_RATE_LIMIT_SCRIPT = """
local current = redis.call('GET', KEYS[1])
if not current then
  return 0
end
current = tonumber(current)
if current <= 1 then
  redis.call('DEL', KEYS[1])
  return 0
end
current = redis.call('DECR', KEYS[1])
return current
"""


class RateLimitExceededError(ApplicationError):
    status_code = 429
    code = "RATE_LIMIT_EXCEEDED"
    message = "Create job rate limit exceeded"

    def __init__(self, *, limit: int, retry_after_seconds: int, reset_epoch: int) -> None:
        super().__init__(
            headers={
                "Retry-After": str(retry_after_seconds),
                "X-RateLimit-Limit": str(limit),
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Reset": str(reset_epoch),
            }
        )


class RedisScriptExecutor(Protocol):
    def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> Awaitable[Any]: ...


class RedisCreateJobRateLimiter:
    def __init__(
        self,
        client: RedisScriptExecutor,
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._client = client
        self._settings = settings
        self._clock = clock or UtcClock()

    async def reserve(self, user_id: UUID) -> CreateJobRateLimitReservationDTO | None:
        now = self._clock.now()
        key, reset_epoch = self._build_window_key(user_id, now)
        expiry_seconds = self._settings.create_job_rate_window_seconds + KEY_EXPIRY_MARGIN_SECONDS
        retry_after_seconds = max(reset_epoch - int(now.timestamp()), 0)
        try:
            raw_result = await self._client.eval(
                CREATE_JOB_RATE_LIMIT_SCRIPT,
                1,
                key,
                expiry_seconds,
            )
        except Exception:
            self._log_fail_open("reserve", user_id=user_id, key=key)
            return None

        current_count, _ttl = cast(tuple[object, object] | list[object], raw_result)
        used = int(cast(int | str, current_count))
        remaining = max(self._settings.create_job_rate_limit - used, 0)
        reservation = CreateJobRateLimitReservationDTO(
            key=key,
            limit=self._settings.create_job_rate_limit,
            used=used,
            remaining=remaining,
            retry_after_seconds=retry_after_seconds,
            reset_epoch=reset_epoch,
        )
        if used > self._settings.create_job_rate_limit:
            raise RateLimitExceededError(
                limit=reservation.limit,
                retry_after_seconds=reservation.retry_after_seconds,
                reset_epoch=reservation.reset_epoch,
            )
        return reservation

    async def release(self, reservation: CreateJobRateLimitReservationDTO) -> None:
        try:
            await self._client.eval(
                RELEASE_CREATE_JOB_RATE_LIMIT_SCRIPT,
                1,
                reservation.key,
            )
        except Exception:
            self._log_fail_open("release", key=reservation.key)

    def _build_window_key(self, user_id: UUID, now: datetime) -> tuple[str, int]:
        now_epoch = int(now.astimezone(UTC).timestamp())
        window_size = self._settings.create_job_rate_window_seconds
        window_start_epoch = now_epoch - (now_epoch % window_size)
        reset_epoch = window_start_epoch + window_size
        return f"rate:create-job:{user_id}:{window_start_epoch}", reset_epoch

    def _log_fail_open(self, operation: str, **context: object) -> None:
        logger.warning(
            "create_job_rate_limit_fail_open",
            extra={
                "operation": operation,
                **context,
            },
            exc_info=True,
        )
