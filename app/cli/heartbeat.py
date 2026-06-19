from __future__ import annotations

import asyncio
from pathlib import Path


def touch_heartbeat(path: str) -> None:
    heartbeat_path = Path(path)
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)
    heartbeat_path.touch()


async def touch_heartbeat_forever(
    *,
    path: str,
    interval_seconds: int,
    stop_event: asyncio.Event,
) -> None:
    while not stop_event.is_set():
        touch_heartbeat(path)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue
