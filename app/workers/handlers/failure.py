from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from app.domain.handlers import JobHandler
from app.workers.handlers.base import JobExecutionError

DEFAULT_FAILURE_MESSAGE = "Failure handler requested job failure"


class FailureJobHandler(JobHandler):
    job_type = "failure"

    async def execute(
        self,
        payload: Mapping[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        message = payload.get("message")
        if isinstance(message, str) and message:
            raise JobExecutionError(message)
        raise JobExecutionError(DEFAULT_FAILURE_MESSAGE)
