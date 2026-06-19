from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, cast

from aio_pika import RobustConnection, connect_robust

STARTUP_RETRY_BASE_SECONDS = 1
STARTUP_RETRY_MAX_SECONDS = 30
STARTUP_RETRY_ATTEMPTS = 5


class AsyncSleeper(Protocol):
    async def __call__(self, delay: float) -> None: ...


async def connect_robust_with_retry(
    url: str,
    *,
    connect: Callable[..., Awaitable[Any]] = connect_robust,
    sleep: AsyncSleeper = asyncio.sleep,
    attempts: int = STARTUP_RETRY_ATTEMPTS,
) -> RobustConnection:
    last_error: Exception | None = None

    for attempt_number in range(1, attempts + 1):
        try:
            return cast(RobustConnection, await connect(url))
        except Exception as exc:
            last_error = exc
            if attempt_number == attempts:
                break
            retry_delay = min(
                STARTUP_RETRY_BASE_SECONDS * (2 ** (attempt_number - 1)),
                STARTUP_RETRY_MAX_SECONDS,
            )
            await sleep(retry_delay)

    assert last_error is not None
    raise last_error
