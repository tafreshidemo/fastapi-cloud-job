from __future__ import annotations

import os
from urllib.parse import urlsplit, urlunsplit

import asyncpg  # type: ignore[import-untyped]

DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job_test"
DEFAULT_TEST_REDIS_URL = "redis://127.0.0.1:6379/1"
DEFAULT_TEST_RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/"
DEFAULT_TEST_RABBITMQ_RUNTIME_URL = "amqp://guest:guest@127.0.0.1:5672/"


def get_test_database_url() -> str:
    return os.getenv("TEST_DATABASE_URL") or DEFAULT_TEST_DATABASE_URL


def get_test_redis_url() -> str:
    return os.getenv("TEST_REDIS_URL") or os.getenv("REDIS_URL") or DEFAULT_TEST_REDIS_URL


def get_test_rabbitmq_url() -> str:
    return os.getenv("TEST_RABBITMQ_URL") or os.getenv("RABBITMQ_URL") or DEFAULT_TEST_RABBITMQ_URL


def get_test_rabbitmq_runtime_url() -> str:
    return (
        os.getenv("TEST_RABBITMQ_RUNTIME_URL")
        or os.getenv("TEST_RABBITMQ_URL")
        or os.getenv("RABBITMQ_URL")
        or DEFAULT_TEST_RABBITMQ_RUNTIME_URL
    )


def to_asyncpg_dsn(database_url: str) -> str:
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


def get_database_name(database_url: str) -> str:
    parsed = urlsplit(to_asyncpg_dsn(database_url))
    database_name = parsed.path.lstrip("/")
    if not database_name:
        raise RuntimeError("Database URL is missing a database name")
    return database_name


def require_test_database(database_url: str) -> str:
    database_name = get_database_name(database_url)
    if database_name == "cloud_job" or not database_name.endswith("_test"):
        raise RuntimeError(f"Refusing to reset non-test database: {database_name}")
    return database_name


def _maintenance_dsn(database_url: str) -> str:
    parsed = urlsplit(to_asyncpg_dsn(database_url))
    return urlunsplit(parsed._replace(path="/postgres"))


async def ensure_test_database_exists(database_url: str) -> None:
    database_name = require_test_database(database_url)
    connection = await asyncpg.connect(_maintenance_dsn(database_url))
    try:
        database_exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database_name,
        )
        if not database_exists:
            await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()


async def reset_test_database(database_url: str) -> None:
    require_test_database(database_url)
    await ensure_test_database_exists(database_url)
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        await connection.execute("DROP SCHEMA public CASCADE")
        await connection.execute("CREATE SCHEMA public")
        await connection.execute("GRANT ALL ON SCHEMA public TO postgres")
        await connection.execute("GRANT ALL ON SCHEMA public TO public")
    finally:
        await connection.close()
