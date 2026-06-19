from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response, status
from fastapi.responses import StreamingResponse

from app.api.dependencies.auth import CurrentUserDep
from app.api.dependencies.infrastructure import get_job_status_pubsub_from_request
from app.api.dependencies.services import get_job_service
from app.application.dto import JobPublicDTO
from app.application.services.jobs import JobService
from app.core.exceptions import InvalidRequestError
from app.domain.enums import JobStatus, JobType
from app.infrastructure.redis.job_status_pubsub import JobStatusPubSubError, RedisJobStatusPubSub
from app.schemas.job_events import JobStatusEvent
from app.schemas.jobs import JobCreateRequest, JobListResponse, JobLogResponse, JobResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])
JobServiceDep = Annotated[JobService, Depends(get_job_service)]
JobStatusPubSubDep = Annotated[RedisJobStatusPubSub, Depends(get_job_status_pubsub_from_request)]
RawIdempotencyKeyHeader = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        description="Required idempotency key for job creation. Printable ASCII, 1-128 characters.",
    ),
]
SSE_HEARTBEAT_SECONDS = 15.0


def validate_idempotency_key(idempotency_key: str) -> str:
    if len(idempotency_key) > 128:
        raise InvalidRequestError("Idempotency-Key must be at most 128 characters")
    if not all(32 <= ord(character) <= 126 for character in idempotency_key):
        raise InvalidRequestError("Idempotency-Key must contain printable ASCII characters only")
    if not idempotency_key.strip():
        raise InvalidRequestError("Idempotency-Key must not be blank")
    return idempotency_key


def get_idempotency_key(idempotency_key: RawIdempotencyKeyHeader) -> str:
    return validate_idempotency_key(idempotency_key)


IdempotencyKeyDep = Annotated[str, Depends(get_idempotency_key)]


@router.post(
    "",
    response_model=JobResponse,
    summary="Create a job",
    responses={
        200: {"description": "Job replayed from an existing idempotent request"},
        201: {"description": "Job created"},
        400: {"description": "Invalid request"},
        409: {"description": "Idempotency key reused with a different payload"},
        429: {
            "description": "Rate limit exceeded",
            "headers": {
                "Retry-After": {"schema": {"type": "integer"}},
                "X-RateLimit-Limit": {"schema": {"type": "integer"}},
                "X-RateLimit-Remaining": {"schema": {"type": "integer"}},
                "X-RateLimit-Reset": {"schema": {"type": "integer"}},
            },
        },
    },
)
async def create_job(
    payload: JobCreateRequest,
    response: Response,
    current_user: CurrentUserDep,
    idempotency_key: IdempotencyKeyDep,
    job_service: JobServiceDep,
) -> JobResponse:
    result = await job_service.create_job(
        current_user=current_user,
        idempotency_key=idempotency_key,
        job_type=JobType(cast(str, payload.type)),
        payload=payload.payload.model_dump(mode="json"),
    )
    response.status_code = status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    response.headers["Idempotency-Replayed"] = "false" if result.created else "true"
    return JobResponse.model_validate(result.job)


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job details",
    responses={409: {"description": "Job state prevents the requested access"}},
)
async def get_job(
    job_id: UUID,
    current_user: CurrentUserDep,
    job_service: JobServiceDep,
) -> JobResponse:
    job = await job_service.get_job_detail(current_user=current_user, job_id=job_id)
    return JobResponse.model_validate(job)


@router.get(
    "",
    response_model=JobListResponse,
    summary="List jobs",
    description="Cursor-based, keyset-ordered listing of jobs scoped to the current user or admin.",
    responses={
        400: {"description": "Invalid cursor"},
        429: {
            "description": "Rate limit exceeded",
            "headers": {
                "Retry-After": {"schema": {"type": "integer"}},
                "X-RateLimit-Limit": {"schema": {"type": "integer"}},
                "X-RateLimit-Remaining": {"schema": {"type": "integer"}},
                "X-RateLimit-Reset": {"schema": {"type": "integer"}},
            },
        },
    },
)
async def list_jobs(
    current_user: CurrentUserDep,
    job_service: JobServiceDep,
    cursor: str | None = Query(default=None),
    limit: int | None = Query(
        default=None,
        ge=1,
        le=100,
        description="Maximum number of jobs to return.",
    ),
) -> JobListResponse:
    page = await job_service.list_jobs(current_user=current_user, cursor=cursor, limit=limit)
    return JobListResponse(
        items=[JobResponse.model_validate(job) for job in page.items],
        next_cursor=page.next_cursor,
    )


@router.get(
    "/{job_id}/logs",
    response_model=list[JobLogResponse],
    summary="List job logs",
)
async def get_job_logs(
    job_id: UUID,
    current_user: CurrentUserDep,
    job_service: JobServiceDep,
) -> list[JobLogResponse]:
    logs = await job_service.list_job_logs(current_user=current_user, job_id=job_id)
    return [JobLogResponse.model_validate(log) for log in logs]


@router.post(
    "/{job_id}/cancel",
    response_model=JobResponse,
    summary="Cancel a job",
    responses={409: {"description": "Job is not cancellable"}},
)
async def cancel_job(
    job_id: UUID,
    current_user: CurrentUserDep,
    job_service: JobServiceDep,
) -> JobResponse:
    job = await job_service.cancel_job(current_user=current_user, job_id=job_id)
    return JobResponse.model_validate(job)


@router.get(
    "/{job_id}/events",
    responses={200: {"content": {"text/event-stream": {}}}},
    summary="Stream job status events",
    description="Server-Sent Events stream of job updates for the authenticated owner or admin.",
)
async def stream_job_events(
    job_id: UUID,
    request: Request,
    current_user: CurrentUserDep,
    job_service: JobServiceDep,
    job_status_pubsub: JobStatusPubSubDep,
) -> StreamingResponse:
    current_job = await job_service.get_job_detail(current_user=current_user, job_id=job_id)

    async def event_stream() -> AsyncIterator[str]:
        snapshot_event = _job_snapshot_event(current_job)
        yield _format_sse_event(snapshot_event)
        if _is_terminal_status(snapshot_event.status):
            return
        async with job_status_pubsub.subscribe(job_id) as events:
            while True:
                if await request.is_disconnected():
                    return
                try:
                    event = await asyncio.wait_for(
                        events.__anext__(),
                        timeout=SSE_HEARTBEAT_SECONDS,
                    )
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                except JobStatusPubSubError:
                    yield _format_stream_error_event("reconnect")
                    return
                except StopAsyncIteration:
                    break
                yield _format_sse_event(event)
                if _is_terminal_status(event.status):
                    return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _job_snapshot_event(job: JobPublicDTO) -> JobStatusEvent:
    return JobStatusEvent(
        event_id=uuid.uuid4(),
        job_id=job.id,
        status=job.status,
        attempt_count=job.attempt_count,
        occurred_at=job.updated_at,
        cancel_requested_at=job.cancel_requested_at,
    )


def _format_sse_event(event: JobStatusEvent) -> str:
    return (
        f"id: {event.event_id}\n"
        "event: job.status\n"
        f"data: {json.dumps(event.model_dump(mode='json'), separators=(',', ':'))}\n\n"
    )


def _format_stream_error_event(code: str) -> str:
    return f"event: stream.error\ndata: {json.dumps({'code': code})}\n\n"


def _is_terminal_status(status: JobStatus) -> bool:
    return status in {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED}
