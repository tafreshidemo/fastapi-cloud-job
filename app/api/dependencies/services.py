from __future__ import annotations

from typing import cast

from fastapi import Request

from app.application.caching.job_list_cache import JobListCache
from app.application.rate_limits import CreateJobRateLimiter
from app.application.services.auth import AuthService
from app.application.services.jobs import JobService
from app.core.config import Settings
from app.workers.post_commit import WorkerPostCommitNotifier


def get_settings_from_request(request: Request) -> Settings:
    return cast(Settings, request.app.state.settings)


def get_auth_service(request: Request) -> AuthService:
    settings = get_settings_from_request(request)
    uow_factory = request.app.state.uow_factory
    return AuthService(uow_factory=uow_factory, settings=settings)


def get_job_service(request: Request) -> JobService:
    settings = get_settings_from_request(request)
    uow_factory = request.app.state.uow_factory
    rate_limiter = request.app.state.create_job_rate_limiter
    job_list_cache = request.app.state.job_list_cache
    post_commit_notifier = request.app.state.worker_post_commit_notifier
    return JobService(
        uow_factory=uow_factory,
        settings=settings,
        rate_limiter=cast(CreateJobRateLimiter, rate_limiter),
        job_list_cache=cast(JobListCache, job_list_cache),
        post_commit_notifier=cast(WorkerPostCommitNotifier, post_commit_notifier),
    )
