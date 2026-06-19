from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.db.session import create_database_runtime


def test_application_can_be_imported_without_network_services(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/app")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    monkeypatch.setenv("JWT_SECRET", "x" * 32)

    from app.main import create_app

    app = create_app()

    assert isinstance(app, FastAPI)


def test_application_lifespan_creates_managed_db_runtime() -> None:
    settings = Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/app",
        REDIS_URL="redis://localhost:6379/0",
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
        JWT_SECRET="x" * 32,
    )

    from app.main import create_app

    app = create_app(settings)

    with TestClient(app):
        assert app.state.db_engine is not None
        assert app.state.db_session_factory is not None
        assert app.state.uow_factory is not None


async def _dispose_runtime(settings: Settings) -> None:
    runtime = create_database_runtime(settings)
    try:
        assert runtime.engine is not None
        assert runtime.session_factory is not None
    finally:
        await runtime.engine.dispose()


def test_create_database_runtime_requires_explicit_settings() -> None:
    settings = Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/app",
        REDIS_URL="redis://localhost:6379/0",
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
        JWT_SECRET="x" * 32,
    )

    import asyncio

    asyncio.run(_dispose_runtime(settings))
