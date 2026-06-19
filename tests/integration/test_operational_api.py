from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.core.config import Settings
from app.core.logging import JsonLogFormatter
from app.core.request_context import REQUEST_ID_HEADER, reset_request_id, set_request_id


class FakeRedis:
    def __init__(self, *, fail_ping: bool = False) -> None:
        self._fail_ping = fail_ping
        self.closed = False

    async def ping(self) -> bool:
        if self._fail_ping:
            raise RuntimeError("redis down")
        return True

    async def aclose(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self, *, fail_execute: bool = False) -> None:
        self._fail_execute = fail_execute

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def execute(self, statement) -> None:
        del statement
        if self._fail_execute:
            raise RuntimeError("database down")


class FakeSessionFactory:
    def __init__(self, *, fail_execute: bool = False) -> None:
        self._fail_execute = fail_execute

    def __call__(self) -> FakeSession:
        return FakeSession(fail_execute=self._fail_execute)


@pytest.fixture
def settings() -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL="postgresql+asyncpg://user:pass@localhost:5432/app",
        REDIS_URL="redis://localhost:6379/0",
        RABBITMQ_URL="amqp://guest:guest@localhost:5672/",
        JWT_SECRET="x" * 32,
    )


@pytest.fixture
def app(settings: Settings) -> Iterator[FastAPI]:
    from app.main import create_app

    app = create_app(settings)

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("secret traceback text")

    yield app


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app, raise_server_exceptions=False) as test_client:
        app.state.redis_client = FakeRedis()
        app.state.db_session_factory = FakeSessionFactory()
        yield test_client


def test_health_endpoints_are_available(client: TestClient) -> None:
    live_response = client.get("/health/live")
    ready_response = client.get("/health/ready")

    assert live_response.status_code == 200
    assert live_response.json() == {"status": "alive"}
    assert ready_response.status_code == 200
    assert ready_response.json() == {
        "status": "ready",
        "checks": {
            "database": "ok",
            "redis": "ok",
        },
    }


def test_readiness_returns_ready_when_redis_is_degraded(
    client: TestClient,
    app: FastAPI,
) -> None:
    app.state.redis_client = FakeRedis(fail_ping=True)

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "checks": {
            "database": "ok",
            "redis": "degraded",
        },
    }


def test_readiness_returns_503_when_database_fails(
    client: TestClient,
    app: FastAPI,
) -> None:
    app.state.db_session_factory = FakeSessionFactory(fail_execute=True)

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "not_ready", "checks": None}


def test_unexpected_error_uses_stable_shape_and_request_id_header(client: TestClient) -> None:
    response = client.get("/boom", headers={REQUEST_ID_HEADER: "req-123"})

    assert response.status_code == 500
    assert response.headers[REQUEST_ID_HEADER] == "req-123"
    assert response.json() == {
        "error": {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An internal server error occurred",
            "details": None,
        }
    }
    assert "secret traceback text" not in response.text


def test_json_log_formatter_includes_request_id() -> None:
    token = set_request_id("req-456")
    try:
        record = logging.LogRecord(
            name="app.test",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.request_id = "req-456"
        payload = json.loads(JsonLogFormatter().format(record))
    finally:
        reset_request_id(token)

    assert payload["message"] == "hello"
    assert payload["request_id"] == "req-456"
    assert payload["logger"] == "app.test"


def test_json_log_formatter_redacts_sensitive_extra_fields() -> None:
    record = logging.LogRecord(
        name="app.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )
    record.password = "super-secret"
    record.jwt_token = "abc123"
    record.redis_url = "redis://secret@localhost:6379/0"
    record.safe_value = "visible"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["extra"] == {
        "jwt_token": "[REDACTED]",
        "password": "[REDACTED]",
        "redis_url": "[REDACTED]",
        "safe_value": "visible",
    }
