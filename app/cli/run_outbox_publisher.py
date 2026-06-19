from __future__ import annotations

import asyncio
import os

from app.cli.heartbeat import touch_heartbeat_forever
from app.cli.runtime import run_background_loop
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import create_database_runtime
from app.db.uow import create_uow_factory
from app.infrastructure.rabbitmq.connection import connect_robust_with_retry
from app.infrastructure.rabbitmq.topology import RabbitMQDispatchPublisher, RabbitMQTopology
from app.workers.outbox_publisher import OutboxPublisher


async def run(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    settings.validate_runtime()
    configure_logging(settings.log_level)
    runtime = create_database_runtime(settings)
    connection = await connect_robust_with_retry(settings.rabbitmq_url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        await channel.set_qos(prefetch_count=settings.outbox_batch_size)
        topology = RabbitMQTopology()
        exchange = await topology.declare(channel)
        publisher = OutboxPublisher(
            uow_factory=create_uow_factory(runtime.session_factory),
            dispatch_publisher=RabbitMQDispatchPublisher(exchange),
            settings=settings,
        )
        heartbeat_path = os.getenv("HEALTHCHECK_FILE")
        heartbeat_task = None
        if heartbeat_path:
            heartbeat_task = asyncio.create_task(
                touch_heartbeat_forever(
                    path=heartbeat_path,
                    interval_seconds=5,
                    stop_event=stop_event,
                )
            )
        try:
            await publisher.run_forever(stop_event=stop_event)
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)
    finally:
        await connection.close()
        await runtime.engine.dispose()


def main() -> None:
    asyncio.run(run_background_loop(run=run, service_name="outbox_publisher"))


if __name__ == "__main__":
    main()
