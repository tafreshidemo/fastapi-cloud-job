from __future__ import annotations

import uuid
from datetime import datetime
from typing import Protocol, cast

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utc_now
from app.domain.enums import JobEventType
from app.models.outbox_event import OutboxEvent
from app.schemas.outbox import DispatchOutboxMessage


class OutboxRepository(Protocol):
    async def add(self, event: OutboxEvent) -> None: ...

    async def create_dispatch_event(
        self,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> OutboxEvent: ...

    async def create_dispatch_event_if_absent(
        self,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> OutboxEvent | None: ...

    async def claim_due_batch(self, now: datetime, limit: int) -> list[OutboxEvent]: ...

    async def get_by_id_for_update_unpublished(self, event_id: uuid.UUID) -> OutboxEvent | None: ...

    async def mark_published(self, event_id: uuid.UUID, now: datetime) -> None: ...

    # Store publish failure details in the database so the next publisher loop can retry later.
    async def record_publish_failure(
        self,
        event_id: uuid.UUID,
        error: str,
        next_available_at: datetime,
    ) -> None: ...


class SqlAlchemyOutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # Add a prepared outbox event without committing; transaction boundaries stay in the Unit of Work.
    async def add(self, event: OutboxEvent) -> None:
        self._session.add(event)

    async def create_dispatch_event(
        self,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> OutboxEvent:
        created_at = utc_now()
        event_id = uuid.uuid4()
        event = OutboxEvent(
            id=event_id,
            aggregate_id=job_id,
            event_type=JobEventType.DISPATCH.value,
            payload=DispatchOutboxMessage(
                event_id=event_id,
                job_id=job_id,
                kind="dispatch",
                created_at=created_at,
            ).model_dump(mode="json"),
            available_at=available_at,
            publish_attempts=0,
            created_at=created_at,
            updated_at=created_at,
        )
        self._session.add(event)
        return event

    async def create_dispatch_event_if_absent(
        self,
        *,
        job_id: uuid.UUID,
        available_at: datetime,
    ) -> OutboxEvent | None:
        statement = select(OutboxEvent.id).where(
            OutboxEvent.aggregate_id == job_id,
            OutboxEvent.event_type == JobEventType.DISPATCH.value,
            OutboxEvent.published_at.is_(None),
        )
        existing_event_id = await self._session.scalar(statement)
        if existing_event_id is not None:
            return None
        return await self.create_dispatch_event(job_id=job_id, available_at=available_at)

    async def claim_due_batch(self, now: datetime, limit: int) -> list[OutboxEvent]:
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.published_at.is_(None),
                OutboxEvent.available_at <= now,
            )
            .order_by(OutboxEvent.available_at.asc(), OutboxEvent.created_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(await self._session.scalars(statement))

    async def get_by_id_for_update_unpublished(self, event_id: uuid.UUID) -> OutboxEvent | None:
        statement = (
            select(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.published_at.is_(None),
            )
            .with_for_update(skip_locked=True)
        )
        return cast(OutboxEvent | None, await self._session.scalar(statement))

    async def mark_published(self, event_id: uuid.UUID, now: datetime) -> None:
        statement = (
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                published_at=now,
                last_error=None,
                updated_at=now,
            )
        )
        await self._session.execute(statement)

    async def record_publish_failure(
        self,
        event_id: uuid.UUID,
        error: str,
        next_available_at: datetime,
    ) -> None:
        statement = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.published_at.is_(None),
            )
            .values(
                publish_attempts=OutboxEvent.publish_attempts + 1,
                last_error=error,
                available_at=next_available_at,
                updated_at=utc_now(),
            )
        )
        await self._session.execute(statement)
