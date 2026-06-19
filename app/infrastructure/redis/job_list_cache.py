from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from uuid import UUID

from redis.asyncio import Redis

from app.application.caching.job_list_cache import JobListCache
from app.application.dto import JobListPageDTO, JobPublicDTO
from app.core.config import Settings
from app.domain.enums import JobStatus, JobType

logger = logging.getLogger(__name__)

ADMIN_LIST_VERSION_KEY = "jobs:list:version:admin"
OWNER_LIST_VERSION_KEY = "jobs:list:version:user:{owner_id}"
VERSION_TTL_SECONDS = 7 * 24 * 60 * 60


class RedisJobListCache(JobListCache):
    def __init__(self, redis: Redis, settings: Settings) -> None:
        self._redis = redis
        self._cache_ttl_seconds = settings.cache_ttl_seconds

    async def get_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
    ) -> JobListPageDTO | None:
        try:
            version = await self._get_version(owner_id)
            payload = await self._redis.get(self._cache_key(owner_id, version, cursor, limit))
        except Exception:
            logger.warning(
                "job_list_cache_get_failed",
                extra={"owner_id": str(owner_id) if owner_id is not None else None},
                exc_info=True,
            )
            return None
        if payload is None:
            return None
        return _decode_page(payload)

    async def set_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
        page: JobListPageDTO,
    ) -> None:
        try:
            version = await self._get_version(owner_id)
            await self._redis.set(
                self._cache_key(owner_id, version, cursor, limit),
                _encode_page(page),
                ex=self._cache_ttl_seconds,
            )
        except Exception:
            logger.warning(
                "job_list_cache_set_failed",
                extra={"owner_id": str(owner_id) if owner_id is not None else None},
                exc_info=True,
            )

    async def invalidate_owner(self, owner_id: UUID) -> None:
        owner_key = OWNER_LIST_VERSION_KEY.format(owner_id=owner_id)
        try:
            async with self._redis.pipeline(transaction=True) as pipeline:
                pipeline.incr(owner_key)
                pipeline.expire(owner_key, VERSION_TTL_SECONDS)
                pipeline.incr(ADMIN_LIST_VERSION_KEY)
                pipeline.expire(ADMIN_LIST_VERSION_KEY, VERSION_TTL_SECONDS)
                await pipeline.execute()
        except Exception:
            logger.warning(
                "job_list_cache_invalidation_failed",
                extra={"owner_id": str(owner_id)},
                exc_info=True,
            )

    async def _get_version(self, owner_id: UUID | None) -> int:
        key = (
            ADMIN_LIST_VERSION_KEY
            if owner_id is None
            else OWNER_LIST_VERSION_KEY.format(owner_id=owner_id)
        )
        value = await self._redis.get(key)
        if value is None:
            return 0
        return int(value)

    @staticmethod
    def _cache_key(
        owner_id: UUID | None,
        version: int,
        cursor: str | None,
        limit: int,
    ) -> str:
        cursor_hash = hashlib.sha256((cursor or "").encode("utf-8")).hexdigest()
        if owner_id is None:
            return f"jobs:list:admin:v:{version}:cursor:{cursor_hash}:limit:{limit}"
        return f"jobs:list:user:{owner_id}:v:{version}:cursor:{cursor_hash}:limit:{limit}"


def _encode_page(page: JobListPageDTO) -> str:
    return json.dumps(
        {
            "items": [
                {
                    "id": str(item.id),
                    "owner_id": str(item.owner_id),
                    "type": item.type.value,
                    "payload": item.payload,
                    "status": item.status.value,
                    "idempotency_key": item.idempotency_key,
                    "created_at": item.created_at.isoformat(),
                    "updated_at": item.updated_at.isoformat(),
                    "cancel_requested_at": (
                        item.cancel_requested_at.isoformat()
                        if item.cancel_requested_at is not None
                        else None
                    ),
                }
                for item in page.items
            ],
            "next_cursor": page.next_cursor,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_page(payload: bytes | str) -> JobListPageDTO:
    decoded = json.loads(payload)
    return JobListPageDTO(
        items=[
            JobPublicDTO(
                id=UUID(item["id"]),
                owner_id=UUID(item["owner_id"]),
                type=JobType(item["type"]),
                payload=item["payload"],
                status=JobStatus(item["status"]),
                idempotency_key=item["idempotency_key"],
                created_at=datetime.fromisoformat(item["created_at"]),
                updated_at=datetime.fromisoformat(item["updated_at"]),
                cancel_requested_at=(
                    datetime.fromisoformat(item["cancel_requested_at"])
                    if item["cancel_requested_at"] is not None
                    else None
                ),
            )
            for item in decoded["items"]
        ],
        next_cursor=decoded["next_cursor"],
    )
