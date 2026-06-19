from __future__ import annotations

import pytest

from app.infrastructure.rabbitmq.connection import connect_robust_with_retry


class FakeConnection:
    pass


@pytest.mark.asyncio
async def test_connect_robust_with_retry_retries_with_bounded_backoff() -> None:
    calls = 0
    sleeps: list[float] = []

    async def fake_connect(url: str) -> FakeConnection:
        nonlocal calls
        calls += 1
        assert url == "amqp://guest:guest@127.0.0.1:5672/"
        if calls < 4:
            raise RuntimeError("rabbit unavailable")
        return FakeConnection()

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    connection = await connect_robust_with_retry(
        "amqp://guest:guest@127.0.0.1:5672/",
        connect=fake_connect,
        sleep=fake_sleep,
        attempts=5,
    )

    assert isinstance(connection, FakeConnection)
    assert sleeps == [1, 2, 4]


@pytest.mark.asyncio
async def test_connect_robust_with_retry_raises_last_error_when_attempts_exhausted() -> None:
    async def fake_connect(url: str) -> FakeConnection:
        del url
        raise RuntimeError("still unavailable")

    async def fake_sleep(delay: float) -> None:
        del delay

    with pytest.raises(RuntimeError, match="still unavailable"):
        await connect_robust_with_retry(
            "amqp://guest:guest@127.0.0.1:5672/",
            connect=fake_connect,
            sleep=fake_sleep,
            attempts=3,
        )
