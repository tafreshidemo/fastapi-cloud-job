from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.domain.handlers import JobHandler
from app.workers.handlers.base import JobCancellationRequestedError

SLEEP_CHUNK_SECONDS = 0.25


class SleepJobHandler(JobHandler):
    job_type = "sleep"

    async def execute(
        self,
        payload: Mapping[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        remaining_seconds = float(payload["duration_seconds"])
        while remaining_seconds > 0:
            if cancellation_requested is not None and await cancellation_requested():
                raise JobCancellationRequestedError("Job cancellation requested")
            sleep_seconds = min(remaining_seconds, SLEEP_CHUNK_SECONDS)
            await asyncio.sleep(sleep_seconds)
            remaining_seconds -= sleep_seconds
