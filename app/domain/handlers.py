from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol


class JobHandler(Protocol):
    job_type: str

    async def execute(
        self,
        payload: Mapping[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None: ...
