from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class DispatchOutboxMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    job_id: UUID
    kind: Literal["dispatch"]
    created_at: datetime
