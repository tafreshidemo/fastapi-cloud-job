from __future__ import annotations

import logging

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.dependencies.infrastructure import (
    get_redis_client,
    get_session_factory,
)

router = APIRouter(prefix="/health", tags=["health"])
logger = logging.getLogger(__name__)


@router.get("/live")
async def live() -> dict[str, str]:
    return {"status": "alive"}


@router.get("/ready", response_model=None)
async def ready(request: Request) -> JSONResponse:
    if not bool(getattr(request.app.state, "is_ready", False)):
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "checks": None},
        )

    session_factory = get_session_factory(request.app)
    redis_client = get_redis_client(request.app)

    try:
        await _check_database(session_factory)
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "not_ready", "checks": None},
        )

    redis_status = "ok"
    try:
        await redis_client.ping()
    except Exception:
        redis_status = "degraded"
        logger.warning("readiness_redis_degraded")

    return JSONResponse(
        content={
            "status": "ready",
            "checks": {
                "database": "ok",
                "redis": redis_status,
            },
        }
    )


async def _check_database(session_factory: async_sessionmaker[AsyncSession]) -> None:
    async with session_factory() as session:
        await session.execute(text("SELECT 1"))
