from __future__ import annotations

import asyncio

from redis.asyncio import Redis

from app.cli.runtime import run_background_loop
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import create_database_runtime
from app.db.uow import create_uow_factory
from app.infrastructure.redis.job_list_cache import RedisJobListCache
from app.infrastructure.redis.job_status_pubsub import RedisJobStatusPubSub
from app.workers.post_commit import (
    CacheInvalidatingWorkerPostCommitNotifier,
    CompositeWorkerPostCommitNotifier,
    RedisPublishingWorkerPostCommitNotifier,
)
from app.workers.stale_running_recovery import StaleRunningRecovery


async def run(stop_event: asyncio.Event) -> None:
    settings = get_settings()
    settings.validate_runtime()
    configure_logging(settings.log_level)
    runtime = create_database_runtime(settings)
    redis_client = Redis.from_url(settings.redis_url)
    try:
        notifier = CompositeWorkerPostCommitNotifier(
            (
                CacheInvalidatingWorkerPostCommitNotifier(
                    RedisJobListCache(redis_client, settings)
                ),
                RedisPublishingWorkerPostCommitNotifier(RedisJobStatusPubSub(redis_client)),
            )
        )
        recovery = StaleRunningRecovery(
            uow_factory=create_uow_factory(runtime.session_factory),
            settings=settings,
            post_commit_notifier=notifier,
        )
        await recovery.run_forever(stop_event=stop_event)
    finally:
        await redis_client.aclose()
        await runtime.engine.dispose()


def main() -> None:
    asyncio.run(run_background_loop(run=run, service_name="stale_running_recovery"))


if __name__ == "__main__":
    main()
