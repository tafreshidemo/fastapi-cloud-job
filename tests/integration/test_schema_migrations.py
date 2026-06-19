from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator

import asyncpg
import pytest
from alembic.config import Config
from sqlalchemy import SmallInteger, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from alembic import command
from app.models.job import Job
from app.models.job_log import JobLog
from app.models.user import User
from tests.support import (
    get_test_database_url,
    get_test_rabbitmq_url,
    get_test_redis_url,
    reset_test_database,
)

TEST_DATABASE_URL = get_test_database_url()


@pytest.fixture
def database_url() -> str:
    return TEST_DATABASE_URL


@pytest.fixture
def alembic_config(monkeypatch: pytest.MonkeyPatch, database_url: str) -> Iterator[Config]:
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DATABASE_URL", database_url)
    monkeypatch.setenv("REDIS_URL", get_test_redis_url())
    monkeypatch.setenv("RABBITMQ_URL", get_test_rabbitmq_url())
    monkeypatch.setenv("JWT_SECRET", "x" * 32)

    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    yield config

def fetch_schema_details(database_url: str) -> dict[str, object]:
    async def _fetch() -> dict[str, object]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                return await connection.run_sync(_inspect_database)
        finally:
            await engine.dispose()

    return asyncio.run(_fetch())


def _inspect_database(sync_connection) -> dict[str, object]:
    schema_inspector = inspect(sync_connection)
    table_names = set(schema_inspector.get_table_names())
    return {
        "tables": table_names,
        "users_column_details": columns(schema_inspector, "users", table_names),
        "jobs_column_details": columns(schema_inspector, "jobs", table_names),
        "job_logs_column_details": columns(schema_inspector, "job_logs", table_names),
        "outbox_column_details": columns(schema_inspector, "outbox_events", table_names),
        "users_columns": column_names(schema_inspector, "users", table_names),
        "jobs_columns": column_names(schema_inspector, "jobs", table_names),
        "job_logs_columns": column_names(schema_inspector, "job_logs", table_names),
        "outbox_columns": column_names(schema_inspector, "outbox_events", table_names),
        "jobs_checks": check_constraints(schema_inspector, "jobs", table_names),
        "users_checks": check_constraints(schema_inspector, "users", table_names),
        "jobs_foreign_keys": foreign_keys(schema_inspector, "jobs", table_names),
        "job_logs_foreign_keys": foreign_keys(schema_inspector, "job_logs", table_names),
        "jobs_unique_constraints": unique_constraints(schema_inspector, "jobs", table_names),
        "jobs_indexes": indexes(schema_inspector, "jobs", table_names),
        "job_logs_indexes": indexes(schema_inspector, "job_logs", table_names),
        "outbox_indexes": indexes(schema_inspector, "outbox_events", table_names),
        "pg_indexes": pg_indexes(sync_connection),
    }


def columns(
    schema_inspector, table_name: str, table_names: set[str]
) -> dict[str, dict[str, object]]:
    if table_name not in table_names:
        return {}
    return {
        column["name"]: {
            "type": column["type"],
            "nullable": column["nullable"],
        }
        for column in schema_inspector.get_columns(table_name)
    }


def column_names(schema_inspector, table_name: str, table_names: set[str]) -> set[str]:
    if table_name not in table_names:
        return set()
    return {column["name"] for column in schema_inspector.get_columns(table_name)}


def check_constraints(schema_inspector, table_name: str, table_names: set[str]) -> set[str]:
    if table_name not in table_names:
        return set()
    return {
        constraint["sqltext"] for constraint in schema_inspector.get_check_constraints(table_name)
    }


def foreign_keys(
    schema_inspector, table_name: str, table_names: set[str]
) -> list[dict[str, object]]:
    if table_name not in table_names:
        return []
    return list(schema_inspector.get_foreign_keys(table_name))


def unique_constraints(
    schema_inspector,
    table_name: str,
    table_names: set[str],
) -> list[dict[str, object]]:
    if table_name not in table_names:
        return []
    return list(schema_inspector.get_unique_constraints(table_name))


def indexes(schema_inspector, table_name: str, table_names: set[str]) -> list[dict[str, object]]:
    if table_name not in table_names:
        return []
    return list(schema_inspector.get_indexes(table_name))


def pg_indexes(sync_connection) -> dict[str, str]:
    rows = sync_connection.execute(
        text(
            """
            SELECT indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY indexname
            """
        )
    )
    return {row.indexname: row.indexdef for row in rows}


def test_upgrade_creates_expected_schema(alembic_config: Config, database_url: str) -> None:
    asyncio.run(reset_test_database(database_url))

    command.upgrade(alembic_config, "head")

    details = fetch_schema_details(database_url)

    assert details["tables"] == {"alembic_version", "job_logs", "jobs", "outbox_events", "users"}
    assert details["users_columns"] == {
        "id",
        "email",
        "password_hash",
        "role",
        "is_active",
        "created_at",
        "updated_at",
    }
    assert details["jobs_columns"] == {
        "id",
        "owner_id",
        "type",
        "payload",
        "status",
        "idempotency_key",
        "request_hash",
        "attempt_count",
        "max_retries",
        "cancel_requested_at",
        "worker_id",
        "execution_token",
        "lease_expires_at",
        "started_at",
        "finished_at",
        "last_error",
        "created_at",
        "updated_at",
    }
    assert details["job_logs_columns"] == {
        "id",
        "job_id",
        "attempt_number",
        "level",
        "message",
        "created_at",
    }
    assert details["outbox_columns"] == {
        "id",
        "aggregate_id",
        "event_type",
        "payload",
        "available_at",
        "publish_attempts",
        "published_at",
        "last_error",
        "created_at",
        "updated_at",
    }

    jobs_column_details = details["jobs_column_details"]
    for field_name in (
        "cancel_requested_at",
        "lease_expires_at",
        "started_at",
        "finished_at",
    ):
        column = jobs_column_details[field_name]
        assert column["nullable"] is True
        assert getattr(column["type"], "timezone", False) is True

    job_logs_column_details = details["job_logs_column_details"]
    assert isinstance(job_logs_column_details["attempt_number"]["type"], SmallInteger)
    assert job_logs_column_details["attempt_number"]["nullable"] is True

    assert any("user" in check and "admin" in check for check in details["users_checks"])
    assert "attempt_count >= 0" in details["jobs_checks"]
    assert "max_retries = 3" in details["jobs_checks"]
    assert any("pending" in check for check in details["jobs_checks"])

    jobs_foreign_keys = details["jobs_foreign_keys"]
    assert len(jobs_foreign_keys) == 1
    assert jobs_foreign_keys[0]["referred_table"] == "users"
    assert jobs_foreign_keys[0]["options"]["ondelete"] == "RESTRICT"

    job_logs_foreign_keys = details["job_logs_foreign_keys"]
    assert len(job_logs_foreign_keys) == 1
    assert job_logs_foreign_keys[0]["referred_table"] == "jobs"
    assert job_logs_foreign_keys[0]["options"]["ondelete"] == "CASCADE"

    unique_constraints = details["jobs_unique_constraints"]
    assert any(
        constraint["column_names"] == ["owner_id", "idempotency_key"]
        for constraint in unique_constraints
    )
    assert any(
        constraint["name"] == "uq_jobs_owner_id_idempotency_key"
        for constraint in unique_constraints
    )

    job_index_names = {index["name"] for index in details["jobs_indexes"]}
    assert {
        "ix_jobs_owner_id_created_at_id",
        "ix_jobs_created_at_id",
        "ix_jobs_owner_id_running",
        "ix_jobs_lease_expires_at_running",
        "ix_jobs_status_created_at",
    }.issubset(job_index_names)

    job_log_index_names = {index["name"] for index in details["job_logs_indexes"]}
    assert "ix_job_logs_job_id_created_at_id" in job_log_index_names

    outbox_index_names = {index["name"] for index in details["outbox_indexes"]}
    assert "ix_outbox_events_available_at_created_at_unpublished" in outbox_index_names

    index_definitions = details["pg_indexes"]
    expected_owner_keyset_index = (
        "CREATE INDEX ix_jobs_owner_id_created_at_id ON public.jobs "
        "USING btree (owner_id, created_at DESC, id DESC)"
    )
    expected_global_keyset_index = (
        "CREATE INDEX ix_jobs_created_at_id ON public.jobs USING btree (created_at DESC, id DESC)"
    )
    expected_running_owner_index = (
        "CREATE INDEX ix_jobs_owner_id_running ON public.jobs "
        "USING btree (owner_id) WHERE ((status)::text = 'running'::text)"
    )
    expected_running_lease_index = (
        "CREATE INDEX ix_jobs_lease_expires_at_running ON public.jobs "
        "USING btree (lease_expires_at) WHERE ((status)::text = 'running'::text)"
    )
    expected_outbox_pending_index = (
        "CREATE INDEX ix_outbox_events_available_at_created_at_unpublished "
        "ON public.outbox_events USING btree (available_at, created_at) "
        "WHERE (published_at IS NULL)"
    )
    assert index_definitions["ix_jobs_owner_id_created_at_id"] == expected_owner_keyset_index
    assert index_definitions["ix_jobs_created_at_id"] == expected_global_keyset_index
    assert index_definitions["ix_jobs_owner_id_running"] == expected_running_owner_index
    assert index_definitions["ix_jobs_lease_expires_at_running"] == expected_running_lease_index
    assert (
        index_definitions["ix_outbox_events_available_at_created_at_unpublished"]
        == expected_outbox_pending_index
    )


def test_models_match_required_schema_types() -> None:
    job_table = Job.__table__.c
    for field_name in (
        "cancel_requested_at",
        "lease_expires_at",
        "started_at",
        "finished_at",
    ):
        column = job_table[field_name]
        assert column.nullable is True
        assert getattr(column.type, "timezone", False) is True

    assert isinstance(JobLog.__table__.c.attempt_number.type, SmallInteger)


def test_upgrade_downgrade_round_trip(alembic_config: Config, database_url: str) -> None:
    asyncio.run(reset_test_database(database_url))

    command.upgrade(alembic_config, "head")
    command.downgrade(alembic_config, "base")

    details_after_downgrade = fetch_schema_details(database_url)
    assert details_after_downgrade["tables"] == {"alembic_version"}

    command.upgrade(alembic_config, "head")

    details_after_reupgrade = fetch_schema_details(database_url)
    assert "users" in details_after_reupgrade["tables"]
    assert "jobs" in details_after_reupgrade["tables"]


def test_duplicate_owner_idempotency_key_is_rejected(
    alembic_config: Config,
    database_url: str,
) -> None:
    asyncio.run(reset_test_database(database_url))
    command.upgrade(alembic_config, "head")

    async def _exercise_duplicate_constraint() -> None:
        engine = create_async_engine(database_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        owner_id = uuid.uuid4()

        try:
            async with session_factory() as session:
                session.add(
                    User(
                        id=owner_id,
                        email="user@example.com",
                        password_hash="hashed-password",
                        role="user",
                        is_active=True,
                    )
                )
                await session.commit()

            async with session_factory() as session:
                session.add_all(
                    [
                        Job(
                            owner_id=owner_id,
                            type="sleep",
                            payload={"duration_seconds": 5},
                            status="pending",
                            idempotency_key="same-key",
                            request_hash="a" * 64,
                            attempt_count=0,
                            max_retries=3,
                        ),
                        Job(
                            owner_id=owner_id,
                            type="sleep",
                            payload={"duration_seconds": 10},
                            status="pending",
                            idempotency_key="same-key",
                            request_hash="b" * 64,
                            attempt_count=0,
                            max_retries=3,
                        ),
                    ]
                )

                with pytest.raises(IntegrityError):
                    await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(_exercise_duplicate_constraint())
