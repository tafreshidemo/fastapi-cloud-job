from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


def install_shutdown_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            signal.signal(sig, lambda _signum, _frame: stop_event.set())


async def run_background_loop(
    *,
    run: Callable[[asyncio.Event], Awaitable[None]],
    service_name: str,
) -> None:
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)
    logger.info("%s_started", service_name)
    try:
        await run(stop_event)
    finally:
        logger.info("%s_stopped", service_name)
