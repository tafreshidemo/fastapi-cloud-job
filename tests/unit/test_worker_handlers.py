from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from app.domain.enums import JobType
from app.workers.handlers import JobCancellationRequestedError
from app.workers.handlers.registry import JobHandlerRegistry, MissingJobHandlerError
from app.workers.handlers.sleep import SLEEP_CHUNK_SECONDS, SleepJobHandler


class DummyHandler:
    job_type = "success"

    async def execute(
        self,
        payload: dict[str, Any],
        *,
        cancellation_requested: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        del cancellation_requested
        del payload


@pytest.mark.asyncio
async def test_sleep_handler_uses_small_sleep_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    observed_sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        observed_sleeps.append(seconds)

    monkeypatch.setattr("app.workers.handlers.sleep.asyncio.sleep", fake_sleep)

    handler = SleepJobHandler()
    await handler.execute({"duration_seconds": 1})

    assert len(observed_sleeps) == 4
    assert all(seconds <= SLEEP_CHUNK_SECONDS for seconds in observed_sleeps)
    assert observed_sleeps == [0.25, 0.25, 0.25, 0.25]


@pytest.mark.asyncio
async def test_sleep_handler_raises_when_cancellation_is_requested() -> None:
    handler = SleepJobHandler()

    async def cancellation_requested() -> bool:
        return True

    with pytest.raises(JobCancellationRequestedError):
        await handler.execute(
            {"duration_seconds": 1},
            cancellation_requested=cancellation_requested,
        )


def test_handler_registry_rejects_unknown_job_type_clearly() -> None:
    registry = JobHandlerRegistry((DummyHandler(),))

    with pytest.raises(MissingJobHandlerError, match="No handler is registered"):
        registry.get(JobType.FAILURE)
