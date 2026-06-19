from __future__ import annotations

import uuid
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.application.dto import JobLogEntryDTO
from app.models.job_log import JobLog

JOB_LOG_LIST_LIMIT = 500


class JobLogRepository(Protocol):
    async def add(self, log: JobLog) -> None: ...

    async def create_job_created_log(self, job_id: uuid.UUID) -> JobLog: ...

    async def create_system_log(self, job_id: uuid.UUID, *, level: str, message: str) -> JobLog: ...

    async def list_entries_for_job(self, job_id: uuid.UUID) -> list[JobLogEntryDTO]: ...

    async def list_for_job(self, job_id: uuid.UUID) -> list[JobLog]: ...


class SqlAlchemyJobLogRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # The Unit of Work owns the commit; the repository only attaches the log entry to the session.
    async def add(self, log: JobLog) -> None:
        self._session.add(log)

    async def create_job_created_log(self, job_id: uuid.UUID) -> JobLog:
        log = JobLog(
            job_id=job_id,
            attempt_number=None,
            level="info",
            message="Job created",
        )
        self._session.add(log)
        return log

    # System logs are not tied to a worker attempt; they describe lifecycle decisions made by the app.
    async def create_system_log(self, job_id: uuid.UUID, *, level: str, message: str) -> JobLog:
        log = JobLog(
            job_id=job_id,
            attempt_number=None,
            level=level,
            message=message,
        )
        self._session.add(log)
        return log

    async def list_entries_for_job(self, job_id: uuid.UUID) -> list[JobLogEntryDTO]:
        statement = (
            select(JobLog)
            .where(JobLog.job_id == job_id)
            .order_by(JobLog.created_at.asc(), JobLog.id.asc())
            .limit(JOB_LOG_LIST_LIMIT)
        )
        return [_to_job_log_entry_dto(log) for log in await self._session.scalars(statement)]

    async def list_for_job(self, job_id: uuid.UUID) -> list[JobLog]:
        statement = (
            select(JobLog)
            .where(JobLog.job_id == job_id)
            .order_by(JobLog.created_at.asc(), JobLog.id.asc())
            .limit(JOB_LOG_LIST_LIMIT)
        )
        return list(await self._session.scalars(statement))


def _to_job_log_entry_dto(log: JobLog) -> JobLogEntryDTO:
    return JobLogEntryDTO(
        id=log.id,
        job_id=log.job_id,
        attempt_number=log.attempt_number,
        level=log.level,
        message=log.message,
        created_at=log.created_at,
    )
