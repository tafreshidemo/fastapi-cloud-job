from __future__ import annotations

import asyncio
import os
import socket

from aio_pika.abc import AbstractIncomingMessage
from redis.asyncio import Redis

from app.cli.heartbeat import touch_heartbeat_forever
from app.cli.runtime import install_shutdown_handlers
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import create_database_runtime
from app.db.uow import create_uow_factory
from app.infrastructure.rabbitmq.connection import connect_robust_with_retry
from app.infrastructure.rabbitmq.topology import (
    JOBS_EXECUTE_QUEUE_NAME,
    RabbitMQTopology,
    execute_queue_arguments,
)
from app.infrastructure.redis.job_list_cache import RedisJobListCache
from app.infrastructure.redis.job_status_pubsub import RedisJobStatusPubSub
from app.workers.handlers import build_job_handler_registry
from app.workers.job_worker import JobWorker
from app.workers.post_commit import (
    CacheInvalidatingWorkerPostCommitNotifier,
    CompositeWorkerPostCommitNotifier,
    RedisPublishingWorkerPostCommitNotifier,
)
from app.workers.stale_running_recovery import StaleRunningRecovery


async def main() -> None:
    settings = get_settings()
    settings.validate_runtime()
    configure_logging(settings.log_level)

    database_runtime = create_database_runtime(settings)
    rabbitmq_connection = await connect_robust_with_retry(settings.rabbitmq_url)
    redis_client = Redis.from_url(settings.redis_url)
    worker_id = socket.gethostname()
    stop_event = asyncio.Event()
    install_shutdown_handlers(stop_event)

    try:
        channel = await rabbitmq_connection.channel()
        await channel.set_qos(prefetch_count=4)
        topology = RabbitMQTopology()
        await topology.declare(channel)
        execute_queue = await channel.declare_queue(
            JOBS_EXECUTE_QUEUE_NAME,
            durable=True,
            arguments=execute_queue_arguments(),
        )
        notifier = CompositeWorkerPostCommitNotifier(
            (
                CacheInvalidatingWorkerPostCommitNotifier(
                    RedisJobListCache(redis_client, settings)
                ),
                RedisPublishingWorkerPostCommitNotifier(RedisJobStatusPubSub(redis_client)),
            )
        )

        worker = JobWorker(
            uow_factory=create_uow_factory(database_runtime.session_factory),
            handler_registry=build_job_handler_registry(),
            settings=settings,
            worker_id=worker_id,
            post_commit_notifier=notifier,
        )
        recovery = StaleRunningRecovery(
            uow_factory=create_uow_factory(database_runtime.session_factory),
            settings=settings,
            post_commit_notifier=notifier,
        )

        async def consume(message: AbstractIncomingMessage) -> None:
            await worker.handle_message(message)

        consumer_tag = await execute_queue.consume(consume, no_ack=False)
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
        recovery_task = asyncio.create_task(recovery.run_forever(stop_event=stop_event))
        await stop_event.wait()
        await execute_queue.cancel(consumer_tag)
        recovery_task.cancel()
        tasks = [recovery_task]
        if heartbeat_task is not None:
            heartbeat_task.cancel()
            tasks.append(heartbeat_task)
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await redis_client.aclose()
        await rabbitmq_connection.close()
        await database_runtime.engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
