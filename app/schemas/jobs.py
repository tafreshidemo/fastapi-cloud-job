from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.domain.enums import JobStatus, JobType


class SleepJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    duration_seconds: int = Field(ge=1, le=30)


class EmptyJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SleepJobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["sleep"]
    payload: SleepJobPayload


class SuccessJobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["success"]
    payload: EmptyJobPayload


class FailureJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str | None = Field(default=None, max_length=500)


class FailureJobCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["failure"]
    payload: FailureJobPayload


JobCreateRequest = Annotated[
    SleepJobCreateRequest | SuccessJobCreateRequest | FailureJobCreateRequest,
    Field(discriminator="type"),
]


class JobResponse(BaseModel):
    id: UUID
    owner_id: UUID
    type: JobType
    payload: dict[str, object]
    status: JobStatus
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    cancel_requested_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class JobLogResponse(BaseModel):
    id: int
    job_id: UUID
    attempt_number: int | None
    level: str
    message: str
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class JobListResponse(BaseModel):
    items: list[JobResponse]
    next_cursor: str | None
