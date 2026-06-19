from __future__ import annotations

from typing import cast

from fastapi import FastAPI, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.application.caching.job_list_cache import JobListCache
from app.application.rate_limits import CreateJobRateLimiter
from app.db.uow import UnitOfWorkFactory
from app.infrastructure.redis.job_status_pubsub import RedisJobStatusPubSub
from app.workers.post_commit import WorkerPostCommitNotifier


def get_db_engine(app: FastAPI) -> AsyncEngine:
    return cast(AsyncEngine, app.state.db_engine)


def get_session_factory(app: FastAPI) -> async_sessionmaker[AsyncSession]:
    return cast(async_sessionmaker[AsyncSession], app.state.db_session_factory)


def get_uow_factory(app: FastAPI) -> UnitOfWorkFactory:
    return cast(UnitOfWorkFactory, app.state.uow_factory)


def get_redis_client(app: FastAPI) -> Redis:
    return cast(Redis, app.state.redis_client)


def get_create_job_rate_limiter(app: FastAPI) -> CreateJobRateLimiter:
    return cast(CreateJobRateLimiter, app.state.create_job_rate_limiter)


def get_worker_post_commit_notifier(app: FastAPI) -> WorkerPostCommitNotifier:
    return cast(WorkerPostCommitNotifier, app.state.worker_post_commit_notifier)


def get_job_list_cache(app: FastAPI) -> JobListCache:
    return cast(JobListCache, app.state.job_list_cache)


def get_job_status_pubsub(app: FastAPI) -> RedisJobStatusPubSub:
    return cast(RedisJobStatusPubSub, app.state.job_status_pubsub)


def get_session_factory_from_request(request: Request) -> async_sessionmaker[AsyncSession]:
    return get_session_factory(request.app)


def get_uow_factory_from_request(request: Request) -> UnitOfWorkFactory:
    return get_uow_factory(request.app)


def get_redis_client_from_request(request: Request) -> Redis:
    return get_redis_client(request.app)


def get_create_job_rate_limiter_from_request(request: Request) -> CreateJobRateLimiter:
    return get_create_job_rate_limiter(request.app)


def get_worker_post_commit_notifier_from_request(request: Request) -> WorkerPostCommitNotifier:
    return get_worker_post_commit_notifier(request.app)


def get_job_list_cache_from_request(request: Request) -> JobListCache:
    return get_job_list_cache(request.app)


def get_job_status_pubsub_from_request(request: Request) -> RedisJobStatusPubSub:
    return get_job_status_pubsub(request.app)
