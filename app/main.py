import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request, Response
from redis.asyncio import Redis
from starlette.middleware.base import RequestResponseEndpoint

from app.api.error_handlers import register_error_handlers
from app.api.routes.auth import router as auth_router
from app.api.routes.health import router as health_router
from app.api.routes.jobs import router as jobs_router
from app.core.config import Settings, get_settings
from app.core.logging import configure_logging
from app.core.request_context import REQUEST_ID_HEADER, reset_request_id, set_request_id
from app.db.session import create_database_runtime
from app.db.uow import create_uow_factory
from app.infrastructure.redis.job_list_cache import RedisJobListCache
from app.infrastructure.redis.job_status_pubsub import RedisJobStatusPubSub
from app.infrastructure.redis.rate_limiter import RedisCreateJobRateLimiter
from app.workers.post_commit import (
    CacheInvalidatingWorkerPostCommitNotifier,
    CompositeWorkerPostCommitNotifier,
    RedisPublishingWorkerPostCommitNotifier,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = app.state.settings
    settings.validate_runtime()
    configure_logging(settings.log_level)
    app.state.is_ready = False
    database_runtime = create_database_runtime(settings)
    redis_client = Redis.from_url(settings.redis_url)
    app.state.db_engine = database_runtime.engine
    app.state.db_session_factory = database_runtime.session_factory
    app.state.uow_factory = create_uow_factory(database_runtime.session_factory)
    app.state.redis_client = redis_client
    app.state.create_job_rate_limiter = RedisCreateJobRateLimiter(redis_client, settings)
    app.state.job_list_cache = RedisJobListCache(redis_client, settings)
    app.state.job_status_pubsub = RedisJobStatusPubSub(redis_client)
    app.state.worker_post_commit_notifier = CompositeWorkerPostCommitNotifier(
        (
            CacheInvalidatingWorkerPostCommitNotifier(app.state.job_list_cache),
            RedisPublishingWorkerPostCommitNotifier(app.state.job_status_pubsub),
        )
    )
    app.state.is_ready = True
    try:
        yield
    finally:
        app.state.is_ready = False
        await redis_client.aclose()
        await database_runtime.engine.dispose()


def create_app(settings: Settings | None = None) -> FastAPI:
    app = FastAPI(
        title="Cloud Resource Management / Asynchronous Job Execution API",
        lifespan=lifespan,
    )
    app.state.settings = settings or get_settings()

    @app.middleware("http")
    async def request_context_middleware(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or str(uuid4())
        request.state.request_id = request_id
        token = set_request_id(request_id)
        try:
            response = await call_next(request)
            logger.info(
                "request_completed",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "status_code": response.status_code,
                },
            )
            response.headers[REQUEST_ID_HEADER] = request_id
            return response
        finally:
            reset_request_id(token)

    register_error_handlers(app)
    app.include_router(health_router)
    app.include_router(auth_router)
    app.include_router(jobs_router)
    return app


app = create_app()
