from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.domain.enums import JobStatus


class JobStatusEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    job_id: UUID
    status: JobStatus
    attempt_count: int
    occurred_at: datetime
    cancel_requested_at: datetime | None = None
