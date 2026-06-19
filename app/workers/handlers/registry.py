from __future__ import annotations

from collections.abc import Iterable

from app.domain.enums import JobType
from app.domain.handlers import JobHandler


class MissingJobHandlerError(LookupError):
    """Raised when no handler exists for a validated job type."""


class JobHandlerRegistry:
    def __init__(self, handlers: Iterable[JobHandler]) -> None:
        self._handlers = {handler.job_type: handler for handler in handlers}

    def get(self, job_type: JobType) -> JobHandler:
        handler = self._handlers.get(job_type.value)
        if handler is None:
            raise MissingJobHandlerError(f"No handler is registered for {job_type.value!r}")
        return handler


def build_job_handler_registry() -> JobHandlerRegistry:
    from app.workers.handlers.failure import FailureJobHandler
    from app.workers.handlers.sleep import SleepJobHandler
    from app.workers.handlers.success import SuccessJobHandler

    return JobHandlerRegistry(
        handlers=(
            SleepJobHandler(),
            SuccessJobHandler(),
            FailureJobHandler(),
        )
    )
