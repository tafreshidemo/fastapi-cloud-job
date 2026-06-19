from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from app.domain.enums import JobEventType


@dataclass(frozen=True, slots=True)
class OutboxDispatchEvent:
    event_id: UUID
    job_id: UUID
    event_type: JobEventType
    created_at: datetime
