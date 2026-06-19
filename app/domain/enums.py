from __future__ import annotations

from enum import StrEnum


class UserRole(StrEnum):
    USER = "user"
    ADMIN = "admin"


class JobStatus(StrEnum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobType(StrEnum):
    SLEEP = "sleep"
    SUCCESS = "success"
    FAILURE = "failure"


class JobEventType(StrEnum):
    DISPATCH = "job.dispatch"
