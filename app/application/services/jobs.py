from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime
from typing import Any
from uuid import UUID

from app.application.caching.job_list_cache import JobListCache, NoOpJobListCache
from app.application.dto import (
    CreateJobRateLimitReservationDTO,
    CurrentUserDTO,
    JobCreateResultDTO,
    JobListPageDTO,
    JobLogEntryDTO,
    JobPublicDTO,
    StoredJobDTO,
)
from app.application.rate_limits import CreateJobRateLimiter
from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.core.exceptions import (
    DuplicateJobIdempotencyKeyError,
    IdempotencyConflictError,
    InvalidCursorError,
    JobNotCancellableError,
    ResourceNotFoundError,
)
from app.db.uow import UnitOfWork, UnitOfWorkFactory
from app.domain.enums import JobStatus, JobType, UserRole
from app.repositories.jobs import JobListCursor
from app.workers.post_commit import (
    JobPostCommitNotification,
    NoOpWorkerPostCommitNotifier,
    WorkerPostCommitNotifier,
)

DEFAULT_LIST_LIMIT = 20
MAX_LIST_LIMIT = 100


class JobService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        settings: Settings,
        rate_limiter: CreateJobRateLimiter,
        *,
        job_list_cache: JobListCache | None = None,
        post_commit_notifier: WorkerPostCommitNotifier | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._settings = settings
        self._rate_limiter = rate_limiter
        self._job_list_cache = job_list_cache or NoOpJobListCache()
        self._post_commit_notifier = post_commit_notifier or NoOpWorkerPostCommitNotifier()
        self._clock = clock or UtcClock()

    async def create_job(
        self,
        *,
        current_user: CurrentUserDTO,
        idempotency_key: str,
        job_type: JobType,
        payload: dict[str, Any],
    ) -> JobCreateResultDTO:
        request_hash = _build_request_hash(job_type=job_type, payload=payload)
        rate_limit_reservation: CreateJobRateLimitReservationDTO | None = None

        async with self._uow_factory() as uow:
            existing_job = await uow.jobs.get_by_idempotency_key(current_user.id, idempotency_key)
            if existing_job is not None:
                return JobCreateResultDTO(
                    job=self._validate_existing_job(
                        existing_job=existing_job,
                        request_hash=request_hash,
                    ),
                    created=False,
                )

            rate_limit_reservation = await self._rate_limiter.reserve(current_user.id)
            try:
                created_job = await uow.jobs.create_pending_job(
                    owner_id=current_user.id,
                    job_type=job_type.value,
                    payload=payload,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    max_retries=self._settings.job_max_retries,
                )
                await uow.job_logs.create_job_created_log(created_job.id)
                await uow.outbox.create_dispatch_event(
                    job_id=created_job.id,
                    available_at=self._clock.now(),
                )
                await uow.commit()
            except DuplicateJobIdempotencyKeyError:
                if rate_limit_reservation is not None:
                    await self._rate_limiter.release(rate_limit_reservation)
                await uow.rollback()
            except Exception:
                if rate_limit_reservation is not None:
                    await self._rate_limiter.release(rate_limit_reservation)
                raise
            else:
                await self._job_list_cache.invalidate_owner(current_user.id)
                created_job_public = _to_job_public_dto(created_job)
                await self._notify_post_commit(created_job_public)
                return JobCreateResultDTO(job=created_job_public, created=True)

        return JobCreateResultDTO(
            job=await self._load_existing_job(
                owner_id=current_user.id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            ),
            created=False,
        )

    async def get_job_detail(self, *, current_user: CurrentUserDTO, job_id: UUID) -> JobPublicDTO:
        return _to_job_public_dto(
            await self._load_authorized_job(current_user=current_user, job_id=job_id)
        )

    async def list_job_logs(
        self,
        *,
        current_user: CurrentUserDTO,
        job_id: UUID,
    ) -> list[JobLogEntryDTO]:
        await self._load_authorized_job(current_user=current_user, job_id=job_id)
        async with self._uow_factory() as uow:
            return await uow.job_logs.list_entries_for_job(job_id)

    async def cancel_job(
        self,
        *,
        current_user: CurrentUserDTO,
        job_id: UUID,
    ) -> JobPublicDTO:
        now = self._clock.now()
        async with self._uow_factory() as uow:
            job = await self._load_authorized_job_for_update(
                uow=uow,
                current_user=current_user,
                job_id=job_id,
            )

            if job.status in {JobStatus.PENDING, JobStatus.QUEUED}:
                cancelled = await uow.jobs.cancel_pending_or_queued(job.id, now=now)
                if cancelled:
                    await uow.job_logs.create_system_log(
                        job.id,
                        level="info",
                        message="Job cancelled before execution",
                    )
                    await uow.commit()
                    await self._job_list_cache.invalidate_owner(job.owner_id)
                    refreshed = await self._load_existing_job_by_id(job.id)
                    await self._notify_post_commit(refreshed)
                    return refreshed

            elif job.status is JobStatus.RUNNING:
                requested = await uow.jobs.request_running_cancellation(job.id, now=now)
                if requested:
                    await uow.job_logs.create_system_log(
                        job.id,
                        level="info",
                        message="Job cancellation requested",
                    )
                    await uow.commit()
                    await self._job_list_cache.invalidate_owner(job.owner_id)
                    refreshed = await self._load_existing_job_by_id(job.id)
                    await self._notify_post_commit(refreshed)
                    return refreshed
            elif job.status in {JobStatus.COMPLETED, JobStatus.FAILED}:
                raise JobNotCancellableError()

            return _to_job_public_dto(job)

        raise RuntimeError("unreachable")

    async def list_jobs(
        self,
        *,
        current_user: CurrentUserDTO,
        cursor: str | None,
        limit: int | None,
    ) -> JobListPageDTO:
        normalized_limit = _normalize_limit(limit)
        cached_page = await self._job_list_cache.get_page(
            owner_id=None if current_user.role is UserRole.ADMIN else current_user.id,
            cursor=cursor,
            limit=normalized_limit,
        )
        if cached_page is not None:
            return cached_page

        decoded_cursor = _decode_cursor(cursor) if cursor is not None else None
        async with self._uow_factory() as uow:
            if current_user.role is UserRole.ADMIN:
                jobs = await uow.jobs.list_all_keyset(decoded_cursor, normalized_limit + 1)
            else:
                jobs = await uow.jobs.list_for_owner_keyset(
                    current_user.id,
                    decoded_cursor,
                    normalized_limit + 1,
                )

        page = _build_job_list_page(jobs, normalized_limit)
        await self._job_list_cache.set_page(
            owner_id=None if current_user.role is UserRole.ADMIN else current_user.id,
            cursor=cursor,
            limit=normalized_limit,
            page=page,
        )
        return page

    @staticmethod
    def _validate_existing_job(existing_job: StoredJobDTO, request_hash: str) -> JobPublicDTO:
        if existing_job.request_hash != request_hash:
            raise IdempotencyConflictError()
        return _to_job_public_dto(existing_job)

    async def _load_existing_job(
        self,
        *,
        owner_id: UUID,
        idempotency_key: str,
        request_hash: str,
    ) -> JobPublicDTO:
        async with self._uow_factory() as uow:
            existing_job = await uow.jobs.get_by_idempotency_key(owner_id, idempotency_key)
            if existing_job is None:
                raise RuntimeError("Expected an existing job after idempotency conflict")
            return self._validate_existing_job(existing_job, request_hash)

    async def _load_existing_job_by_id(self, job_id: UUID) -> JobPublicDTO:
        async with self._uow_factory() as uow:
            job = await uow.jobs.get_by_id(job_id)
            if job is None:
                raise ResourceNotFoundError("Job not found")
            return _to_job_public_dto(job)

    async def _load_authorized_job(
        self,
        *,
        current_user: CurrentUserDTO,
        job_id: UUID,
    ) -> StoredJobDTO:
        async with self._uow_factory() as uow:
            return await self._load_authorized_job_for_update(
                uow=uow,
                current_user=current_user,
                job_id=job_id,
                lock=False,
            )

    async def _load_authorized_job_for_update(
        self,
        *,
        uow: UnitOfWork,
        current_user: CurrentUserDTO,
        job_id: UUID,
        lock: bool = True,
    ) -> StoredJobDTO:
        if current_user.role == UserRole.ADMIN:
            job = await (
                uow.jobs.get_by_id_for_update(job_id) if lock else uow.jobs.get_by_id(job_id)
            )
        else:
            job = await (
                uow.jobs.get_by_id_for_owner_for_update(job_id, current_user.id)
                if lock
                else uow.jobs.get_by_id_for_owner(job_id, current_user.id)
            )
        if job is None:
            raise ResourceNotFoundError("Job not found")
        return job

    async def _notify_post_commit(self, job: JobPublicDTO) -> None:
        try:
            await self._post_commit_notifier.notify_job_state_changed(
                JobPostCommitNotification(
                    job_id=job.id,
                    owner_id=job.owner_id,
                    status=job.status,
                    attempt_count=job.attempt_count,
                    committed_at=job.updated_at,
                    cancel_requested_at=job.cancel_requested_at,
                )
            )
        except Exception:
            return


def _to_job_public_dto(job: StoredJobDTO) -> JobPublicDTO:
    return JobPublicDTO(
        id=job.id,
        owner_id=job.owner_id,
        type=job.type,
        payload=job.payload,
        status=job.status,
        idempotency_key=job.idempotency_key,
        created_at=job.created_at,
        updated_at=job.updated_at,
        cancel_requested_at=job.cancel_requested_at,
        attempt_count=job.attempt_count,
    )


def _build_request_hash(*, job_type: JobType, payload: dict[str, Any]) -> str:
    canonical_payload = {
        "type": job_type.value,
        "payload": payload,
    }
    encoded_payload = json.dumps(
        canonical_payload,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded_payload).hexdigest()


def _normalize_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_LIST_LIMIT
    return max(1, min(limit, MAX_LIST_LIMIT))


# Fetch one extra row upstream, then trim here to decide whether another cursor should be returned.
def _build_job_list_page(jobs: list[StoredJobDTO], limit: int) -> JobListPageDTO:
    page_jobs = jobs[:limit]
    next_cursor = None
    if len(jobs) > limit:
        last_job = page_jobs[-1]
        next_cursor = _encode_cursor(
            JobListCursor(created_at=last_job.created_at, job_id=last_job.id)
        )
    return JobListPageDTO(
        items=[_to_job_public_dto(job) for job in page_jobs],
        next_cursor=next_cursor,
    )


def _encode_cursor(cursor: JobListCursor) -> str:
    raw = json.dumps(
        {
            "v": 1,
            "created_at": cursor.created_at.isoformat(),
            "id": str(cursor.job_id),
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")

# Be strict when decoding cursors so malformed client input never turns into a loose database query.
def _decode_cursor(cursor: str) -> JobListCursor:
    try:
        payload = base64.urlsafe_b64decode(cursor.encode("ascii"))
        decoded = json.loads(payload)
        if not isinstance(decoded, dict):
            raise ValueError("cursor payload must be an object")
        if set(decoded) != {"v", "created_at", "id"}:
            raise ValueError("cursor payload shape is invalid")
        if decoded["v"] != 1:
            raise ValueError("cursor version is invalid")
        return JobListCursor(
            created_at=datetime.fromisoformat(decoded["created_at"]),
            job_id=UUID(decoded["id"]),
        )
    except Exception as exc:
        raise InvalidCursorError() from exc
