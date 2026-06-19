from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from uuid import UUID

from redis.asyncio import Redis

from app.schemas.job_events import JobStatusEvent

logger = logging.getLogger(__name__)

JOB_STATUS_CHANNEL_PREFIX = "jobs:events"


class JobStatusPubSubError(RuntimeError):
    pass


class RedisJobStatusPubSub:
    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    async def publish(self, event: JobStatusEvent) -> None:
        await self._redis.publish(
            self.channel_name(event.job_id),
            event.model_dump_json(),
        )

    @asynccontextmanager
    async def subscribe(self, job_id: UUID) -> AsyncIterator[AsyncIterator[JobStatusEvent]]:
        pubsub = cast(Any, self._redis.pubsub())
        await pubsub.subscribe(self.channel_name(job_id))

        async def event_iterator() -> AsyncIterator[JobStatusEvent]:
            try:
                while True:
                    try:
                        message = await pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=1.0,
                        )
                    except Exception as exc:
                        raise JobStatusPubSubError("Redis pubsub receive failed") from exc
                    if message is None:
                        await asyncio.sleep(0)
                        continue
                    data = message["data"]
                    if isinstance(data, bytes):
                        payload = data.decode("utf-8")
                    elif isinstance(data, str):
                        payload = data
                    else:
                        payload = json.dumps(data)
                    try:
                        yield JobStatusEvent.model_validate_json(payload)
                    except Exception:
                        logger.warning(
                            "job_status_pubsub_invalid_message",
                            extra={"job_id": str(job_id)},
                            exc_info=True,
                        )
            finally:
                await pubsub.unsubscribe(self.channel_name(job_id))
                await pubsub.aclose()

        try:
            yield event_iterator()
        finally:
            await pubsub.aclose()

    @staticmethod
    def channel_name(job_id: UUID) -> str:
        return f"{JOB_STATUS_CHANNEL_PREFIX}:{job_id}"
