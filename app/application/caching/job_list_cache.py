from __future__ import annotations

from typing import Protocol
from uuid import UUID

from app.application.dto import JobListPageDTO


class JobListCache(Protocol):
    async def get_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
    ) -> JobListPageDTO | None: ...

    async def set_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
        page: JobListPageDTO,
    ) -> None: ...

    async def invalidate_owner(self, owner_id: UUID) -> None: ...


class NoOpJobListCache:
    async def get_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
    ) -> JobListPageDTO | None:
        del owner_id, cursor, limit
        return None

    async def set_page(
        self,
        *,
        owner_id: UUID | None,
        cursor: str | None,
        limit: int,
        page: JobListPageDTO,
    ) -> None:
        del owner_id, cursor, limit, page

    async def invalidate_owner(self, owner_id: UUID) -> None:
        del owner_id
