from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.domain.enums import JobStatus, JobType, UserRole


@dataclass(frozen=True, slots=True)
class UserPublicDTO:
    id: uuid.UUID
    email: str
    role: UserRole
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class LoginResultDTO:
    access_token: str
    token_type: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class CurrentUserDTO:
    id: uuid.UUID
    email: str
    role: UserRole
    is_active: bool


@dataclass(frozen=True, slots=True)
class JobPublicDTO:
    id: uuid.UUID
    owner_id: uuid.UUID
    type: JobType
    payload: dict[str, Any]
    status: JobStatus
    idempotency_key: str
    created_at: datetime
    updated_at: datetime
    cancel_requested_at: datetime | None = None
    attempt_count: int = 0


@dataclass(frozen=True, slots=True)
class JobCreateResultDTO:
    job: JobPublicDTO
    created: bool


@dataclass(frozen=True, slots=True)
class CreateJobRateLimitReservationDTO:
    key: str
    limit: int
    used: int
    remaining: int
    retry_after_seconds: int
    reset_epoch: int


@dataclass(frozen=True, slots=True)
class StoredJobDTO:
    id: uuid.UUID
    owner_id: uuid.UUID
    type: JobType
    payload: dict[str, Any]
    status: JobStatus
    idempotency_key: str
    request_hash: str
    created_at: datetime
    updated_at: datetime
    cancel_requested_at: datetime | None = None
    attempt_count: int = 0


@dataclass(frozen=True, slots=True)
class JobExecutionDTO:
    id: uuid.UUID
    owner_id: uuid.UUID
    type: JobType
    payload: dict[str, Any]
    status: JobStatus
    attempt_count: int
    max_retries: int
    worker_id: str | None
    execution_token: uuid.UUID | None
    lease_expires_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    cancel_requested_at: datetime | None
    last_error: str | None


@dataclass(frozen=True, slots=True)
class JobLogEntryDTO:
    id: int
    job_id: uuid.UUID
    attempt_number: int | None
    level: str
    message: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class JobListPageDTO:
    items: list[JobPublicDTO]
    next_cursor: str | None
