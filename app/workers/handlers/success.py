from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.domain.handlers import JobHandler


class SuccessJobHandler(JobHandler):
    job_type = "success"

    async def execute(
        self,
        payload: Mapping[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        del payload
