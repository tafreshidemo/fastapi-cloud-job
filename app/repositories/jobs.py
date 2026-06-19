from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, cast

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from app.application.dto import JobExecutionDTO, StoredJobDTO
from app.core.exceptions import DuplicateJobIdempotencyKeyError
from app.domain.enums import JobStatus, JobType
from app.models.job import Job


@dataclass(frozen=True, slots=True)
class JobListCursor:
    created_at: datetime
    job_id: uuid.UUID


class JobRepository(Protocol):
    async def add(self, job: Job) -> None: ...

    async def create_pending_job(
        self,
        owner_id: uuid.UUID,
        job_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        max_retries: int,
    ) -> StoredJobDTO: ...

    async def get_by_id(self, job_id: uuid.UUID) -> StoredJobDTO | None: ...

    async def get_by_id_for_owner(
        self,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> StoredJobDTO | None: ...

    async def get_by_id_for_update(self, job_id: uuid.UUID) -> StoredJobDTO | None: ...

    async def get_by_id_for_owner_for_update(
        self,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> StoredJobDTO | None: ...

    async def get_execution_candidate_for_update(
        self,
        job_id: uuid.UUID,
    ) -> JobExecutionDTO | None: ...

    async def mark_queued(self, job_id: uuid.UUID, now: datetime) -> bool: ...

    async def mark_running(
        self,
        job_id: uuid.UUID,
        *,
        worker_id: str,
        execution_token: uuid.UUID,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool: ...

    async def mark_completed(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
    ) -> bool: ...

    # Finalize a running job as failed only when the worker still owns the current execution token.
    async def mark_failed(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
        error: str,
    ) -> bool: ...

    async def requeue_running(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
        error: str,
    ) -> bool: ...

    async def mark_failed_without_execution(
        self,
        job_id: uuid.UUID,
        *,
        current_status: JobStatus,
        now: datetime,
        error: str,
    ) -> bool: ...

    async def get_owner_id(self, job_id: uuid.UUID) -> uuid.UUID | None: ...

    async def get_by_idempotency_key(
        self,
        owner_id: uuid.UUID,
        key: str,
    ) -> StoredJobDTO | None: ...

    async def count_running_for_owner(self, owner_id: uuid.UUID) -> int: ...

    async def list_for_owner_keyset(
        self,
        owner_id: uuid.UUID,
        cursor: JobListCursor | None,
        limit: int,
    ) -> list[StoredJobDTO]: ...

    async def list_all_keyset(
        self,
        cursor: JobListCursor | None,
        limit: int,
    ) -> list[StoredJobDTO]: ...

    async def find_expired_running_jobs(self, now: datetime, limit: int) -> list[StoredJobDTO]: ...

    async def claim_expired_running_jobs_for_update(
        self,
        now: datetime,
        limit: int,
    ) -> list[JobExecutionDTO]: ...

    async def cancel_pending_or_queued(self, job_id: uuid.UUID, *, now: datetime) -> bool: ...

    async def request_running_cancellation(
        self,
        job_id: uuid.UUID,
        *,
        now: datetime,
    ) -> bool: ...

    async def mark_cancelled(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
    ) -> bool: ...


class SqlAlchemyJobRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def add(self, job: Job) -> None:
        self._session.add(job)

    async def create_pending_job(
        self,
        owner_id: uuid.UUID,
        job_type: str,
        payload: dict[str, Any],
        idempotency_key: str,
        request_hash: str,
        max_retries: int,
    ) -> StoredJobDTO:
        job = Job(
            owner_id=owner_id,
            type=job_type,
            payload=payload,
            status=JobStatus.PENDING.value,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            attempt_count=0,
            max_retries=max_retries,
        )
        self._session.add(job)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            if _is_duplicate_job_idempotency_key_error(exc):
                raise DuplicateJobIdempotencyKeyError() from exc
            raise
        return _to_stored_job_dto(job)

    async def get_by_id(self, job_id: uuid.UUID) -> StoredJobDTO | None:
        job = await self._session.get(Job, job_id)
        if job is None:
            return None
        return _to_stored_job_dto(job)

    async def get_by_id_for_owner(
        self,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> StoredJobDTO | None:
        statement = select(Job).where(
            Job.id == job_id,
            Job.owner_id == owner_id,
        )
        job = cast(Job | None, await self._session.scalar(statement))
        if job is None:
            return None
        return _to_stored_job_dto(job)

    async def get_by_id_for_update(self, job_id: uuid.UUID) -> StoredJobDTO | None:
        statement = select(Job).where(Job.id == job_id).with_for_update()
        job = cast(Job | None, await self._session.scalar(statement))
        if job is None:
            return None
        return _to_stored_job_dto(job)

    async def get_by_id_for_owner_for_update(
        self,
        job_id: uuid.UUID,
        owner_id: uuid.UUID,
    ) -> StoredJobDTO | None:
        statement = (
            select(Job)
            .where(
                Job.id == job_id,
                Job.owner_id == owner_id,
            )
            .with_for_update()
        )
        job = cast(Job | None, await self._session.scalar(statement))
        if job is None:
            return None
        return _to_stored_job_dto(job)

    async def get_execution_candidate_for_update(
        self,
        job_id: uuid.UUID,
    ) -> JobExecutionDTO | None:
        statement = select(Job).where(Job.id == job_id).with_for_update()
        job = cast(Job | None, await self._session.scalar(statement))
        if job is None:
            return None
        return _to_job_execution_dto(job)

    async def mark_queued(self, job_id: uuid.UUID, now: datetime) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.PENDING.value,
            )
            .values(
                status=JobStatus.QUEUED.value,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def mark_running(
        self,
        job_id: uuid.UUID,
        *,
        worker_id: str,
        execution_token: uuid.UUID,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.QUEUED.value,
            )
            .values(
                status=JobStatus.RUNNING.value,
                worker_id=worker_id,
                execution_token=execution_token,
                lease_expires_at=lease_expires_at,
                started_at=func.coalesce(Job.started_at, now),
                finished_at=None,
                last_error=None,
                attempt_count=Job.attempt_count + 1,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def mark_completed(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.execution_token == execution_token,
            )
            .values(
                status=JobStatus.COMPLETED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=now,
                last_error=None,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def mark_failed(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
        error: str,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.execution_token == execution_token,
            )
            .values(
                status=JobStatus.FAILED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=now,
                last_error=error,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def requeue_running(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
        error: str,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.execution_token == execution_token,
            )
            .values(
                status=JobStatus.QUEUED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=None,
                last_error=error,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def mark_failed_without_execution(
        self,
        job_id: uuid.UUID,
        *,
        current_status: JobStatus,
        now: datetime,
        error: str,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == current_status.value,
            )
            .values(
                status=JobStatus.FAILED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=now,
                last_error=error,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def cancel_pending_or_queued(self, job_id: uuid.UUID, *, now: datetime) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status.in_([JobStatus.PENDING.value, JobStatus.QUEUED.value]),
            )
            .values(
                status=JobStatus.CANCELLED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=now,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def request_running_cancellation(
        self,
        job_id: uuid.UUID,
        *,
        now: datetime,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.cancel_requested_at.is_(None),
            )
            .values(
                cancel_requested_at=now,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def mark_cancelled(
        self,
        job_id: uuid.UUID,
        *,
        execution_token: uuid.UUID,
        now: datetime,
    ) -> bool:
        statement = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING.value,
                Job.execution_token == execution_token,
            )
            .values(
                status=JobStatus.CANCELLED.value,
                worker_id=None,
                execution_token=None,
                lease_expires_at=None,
                finished_at=now,
                updated_at=now,
            )
        )
        result = cast(CursorResult[Any], await self._session.execute(statement))
        return (result.rowcount or 0) == 1

    async def get_owner_id(self, job_id: uuid.UUID) -> uuid.UUID | None:
        statement = select(Job.owner_id).where(Job.id == job_id)
        return cast(uuid.UUID | None, await self._session.scalar(statement))

    async def get_by_idempotency_key(self, owner_id: uuid.UUID, key: str) -> StoredJobDTO | None:
        statement = select(Job).where(
            Job.owner_id == owner_id,
            Job.idempotency_key == key,
        )
        job = cast(Job | None, await self._session.scalar(statement))
        if job is None:
            return None
        return _to_stored_job_dto(job)

    async def count_running_for_owner(self, owner_id: uuid.UUID) -> int:
        statement = (
            select(func.count())
            .select_from(Job)
            .where(
                Job.owner_id == owner_id,
                Job.status == JobStatus.RUNNING.value,
            )
        )
        return int(await self._session.scalar(statement) or 0)

    async def list_for_owner_keyset(
        self,
        owner_id: uuid.UUID,
        cursor: JobListCursor | None,
        limit: int,
    ) -> list[StoredJobDTO]:
        statement = (
            select(Job)
            .where(Job.owner_id == owner_id)
            .order_by(Job.created_at.desc(), Job.id.desc())
            .limit(limit)
        )
        if cursor is not None:
            statement = statement.where(self._keyset_clause(cursor))
        return [_to_stored_job_dto(job) for job in await self._session.scalars(statement)]

    async def list_all_keyset(self, cursor: JobListCursor | None, limit: int) -> list[StoredJobDTO]:
        statement = select(Job).order_by(Job.created_at.desc(), Job.id.desc()).limit(limit)
        if cursor is not None:
            statement = statement.where(self._keyset_clause(cursor))
        return [_to_stored_job_dto(job) for job in await self._session.scalars(statement)]

    async def find_expired_running_jobs(self, now: datetime, limit: int) -> list[StoredJobDTO]:
        statement = (
            select(Job)
            .where(
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at.is_not(None),
                Job.lease_expires_at < now,
            )
            .order_by(Job.lease_expires_at.asc(), Job.created_at.asc(), Job.id.asc())
            .limit(limit)
        )
        return [_to_stored_job_dto(job) for job in await self._session.scalars(statement)]

    async def claim_expired_running_jobs_for_update(
        self,
        now: datetime,
        limit: int,
    ) -> list[JobExecutionDTO]:
        statement = (
            select(Job)
            .where(
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at.is_not(None),
                Job.lease_expires_at < now,
            )
            .order_by(Job.lease_expires_at.asc(), Job.created_at.asc(), Job.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return [_to_job_execution_dto(job) for job in await self._session.scalars(statement)]

    @staticmethod
    def _keyset_clause(cursor: JobListCursor) -> ColumnElement[bool]:
        return or_(
            Job.created_at < cursor.created_at,
            and_(Job.created_at == cursor.created_at, Job.id < cursor.job_id),
        )


def _is_duplicate_job_idempotency_key_error(exc: IntegrityError) -> bool:
    original_error = exc.orig
    constraint_name = cast(str | None, getattr(original_error, "constraint_name", None))
    sqlstate = cast(
        str | None,
        getattr(original_error, "sqlstate", None) or getattr(original_error, "pgcode", None),
    )

    if sqlstate != "23505":
        return False
    if constraint_name is not None:
        return constraint_name == "uq_jobs_owner_id_idempotency_key"

    error_text = str(original_error).lower()
    return "jobs" in error_text and "idempotency_key" in error_text


def _to_stored_job_dto(job: Job) -> StoredJobDTO:
    return StoredJobDTO(
        id=job.id,
        owner_id=job.owner_id,
        type=JobType(job.type),
        payload=job.payload,
        status=JobStatus(job.status),
        idempotency_key=job.idempotency_key,
        request_hash=job.request_hash,
        created_at=job.created_at,
        updated_at=job.updated_at,
        cancel_requested_at=job.cancel_requested_at,
        attempt_count=job.attempt_count,
    )


def _to_job_execution_dto(job: Job) -> JobExecutionDTO:
    return JobExecutionDTO(
        id=job.id,
        owner_id=job.owner_id,
        type=JobType(job.type),
        payload=job.payload,
        status=JobStatus(job.status),
        attempt_count=job.attempt_count,
        max_retries=job.max_retries,
        worker_id=job.worker_id,
        execution_token=job.execution_token,
        lease_expires_at=job.lease_expires_at,
        started_at=job.started_at,
        finished_at=job.finished_at,
        cancel_requested_at=job.cancel_requested_at,
        last_error=job.last_error,
    )
