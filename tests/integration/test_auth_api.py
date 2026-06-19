from __future__ import annotations

import asyncio
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Annotated

import asyncpg  # type: ignore[import-untyped]
import httpx
import jwt
import pytest
from alembic.config import Config
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from alembic import command
from app.api.dependencies.auth import (
    require_admin_user,
    require_owner_or_admin_user,
)
from app.application.dto import CurrentUserDTO
from app.core.config import Settings
from app.core.security import hash_password
from app.repositories.users import SqlAlchemyUserRepository
from tests.support import (
    get_test_database_url,
    get_test_rabbitmq_url,
    get_test_redis_url,
    reset_test_database,
    to_asyncpg_dsn,
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


@pytest.fixture
def settings(database_url: str) -> Settings:
    return Settings(
        APP_ENV="test",
        LOG_LEVEL="INFO",
        DATABASE_URL=database_url,
        REDIS_URL=get_test_redis_url(),
        RABBITMQ_URL=get_test_rabbitmq_url(),
        JWT_SECRET="x" * 32,
    )


@pytest.fixture
def app(
    settings: Settings,
    alembic_config: Config,
    database_url: str,
) -> Iterator[FastAPI]:
    asyncio.run(reset_test_database(database_url))
    command.upgrade(alembic_config, "head")

    from app.main import create_app

    app = create_app(settings)

    @app.get("/protected/{owner_id}")
    async def protected_resource(
        owner_id: uuid.UUID,
        current_user: Annotated[CurrentUserDTO, Depends(require_owner_or_admin_user)],
    ) -> dict[str, str]:
        del owner_id
        return {"user_id": str(current_user.id)}

    @app.get("/admin-only")
    async def admin_only_route(
        current_user: Annotated[CurrentUserDTO, Depends(require_admin_user)],
    ) -> dict[str, str]:
        return {"role": current_user.role.value}

    yield app


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client

async def insert_user(
    database_url: str,
    *,
    email: str,
    password: str,
    role: str = "user",
    is_active: bool = True,
) -> uuid.UUID:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        user_id = uuid.uuid4()
        now = datetime.now(UTC)
        await connection.execute(
            """
            INSERT INTO users (id, email, password_hash, role, is_active, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            user_id,
            email,
            hash_password(password),
            role,
            is_active,
            now,
            now,
        )
        return user_id
    finally:
        await connection.close()


async def deactivate_user(
    database_url: str,
    user_id: uuid.UUID,
) -> None:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        await connection.execute(
            "UPDATE users SET is_active = FALSE WHERE id = $1",
            user_id,
        )
    finally:
        await connection.close()


async def count_users_by_email(database_url: str, email: str) -> int:
    connection = await asyncpg.connect(to_asyncpg_dsn(database_url))
    try:
        return int(
            await connection.fetchval(
                "SELECT COUNT(*) FROM users WHERE email = $1",
                email,
            )
        )
    finally:
        await connection.close()


def authorization_header(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_register_success(client: TestClient) -> None:
    response = client.post(
        "/auth/register",
        json={"email": "User@Example.com", "password": "secret-password"},
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["email"] == "user@example.com"
    assert payload["role"] == "user"
    assert "password_hash" not in payload


def test_login_success(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    response = client.post(
        "/auth/login",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["token_type"] == "bearer"
    claims = jwt.decode(
        payload["access_token"],
        "x" * 32,
        algorithms=["HS256"],
    )
    assert set(claims) == {"sub", "role", "iat", "exp", "jti"}
    assert claims["role"] == "user"


def test_invalid_password(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    response = client.post(
        "/auth/login",
        json={"email": "user@example.com", "password": "wrong-password"},
    )

    assert response.status_code == 401


def test_duplicate_email(client: TestClient) -> None:
    client.post(
        "/auth/register",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    response = client.post(
        "/auth/register",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    assert response.status_code == 409


def test_concurrent_duplicate_registration_returns_single_success_and_single_conflict(
    app: FastAPI,
    database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_get_by_email = SqlAlchemyUserRepository.get_by_email
    target_email = "race@example.com"
    barrier = asyncio.Barrier(2)
    pending_reads = 0

    async def synchronized_get_by_email(
        self: SqlAlchemyUserRepository,
        email: str,
    ) -> object | None:
        nonlocal pending_reads
        user = await original_get_by_email(self, email)
        if email == target_email and user is None and pending_reads < 2:
            pending_reads += 1
            await barrier.wait()
        return user

    monkeypatch.setattr(
        SqlAlchemyUserRepository,
        "get_by_email",
        synchronized_get_by_email,
    )

    async def run_requests() -> tuple[httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as async_client:
            payload = {"email": target_email, "password": "secret-password"}
            return await asyncio.gather(
                async_client.post("/auth/register", json=payload),
                async_client.post("/auth/register", json=payload),
            )

    with TestClient(app):
        responses = asyncio.run(run_requests())

    status_codes = sorted(response.status_code for response in responses)
    user_count = asyncio.run(count_users_by_email(database_url, target_email))

    assert status_codes == [201, 409]
    assert user_count == 1


def test_expired_and_invalid_jwt(
    client: TestClient,
    database_url: str,
) -> None:
    user = asyncio.run(
        insert_user(
            database_url,
            email="user@example.com",
            password="secret-password",
        )
    )
    expired_token = jwt.encode(
        {
            "sub": str(user),
            "role": "user",
            "iat": int((datetime.now(UTC) - timedelta(minutes=10)).timestamp()),
            "exp": int((datetime.now(UTC) - timedelta(minutes=5)).timestamp()),
            "jti": str(uuid.uuid4()),
        },
        "x" * 32,
        algorithm="HS256",
    )

    expired_response = client.get(
        f"/protected/{user}",
        headers=authorization_header(expired_token),
    )
    invalid_response = client.get(
        f"/protected/{user}",
        headers=authorization_header("invalid-token"),
    )

    assert expired_response.status_code == 401
    assert invalid_response.status_code == 401


def test_public_registration_cannot_set_admin_role(client: TestClient) -> None:
    response = client.post(
        "/auth/register",
        json={
            "email": "user@example.com",
            "password": "secret-password",
            "role": "admin",
        },
    )

    assert response.status_code == 422


def test_inactive_account_is_denied(
    client: TestClient,
    database_url: str,
) -> None:
    user = asyncio.run(
        insert_user(
            database_url,
            email="user@example.com",
            password="secret-password",
            is_active=False,
        )
    )

    login_response = client.post(
        "/auth/login",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    token = jwt.encode(
        {
            "sub": str(user),
            "role": "user",
            "iat": int(datetime.now(UTC).timestamp()),
            "exp": int((datetime.now(UTC) + timedelta(minutes=30)).timestamp()),
            "jti": str(uuid.uuid4()),
        },
        "x" * 32,
        algorithm="HS256",
    )
    protected_response = client.get(
        f"/protected/{user}",
        headers=authorization_header(token),
    )

    assert login_response.status_code == 403
    assert protected_response.status_code == 403


def test_user_cannot_access_another_users_resource(
    client: TestClient,
    database_url: str,
) -> None:
    user = asyncio.run(
        insert_user(
            database_url,
            email="user@example.com",
            password="secret-password",
        )
    )
    other_user = asyncio.run(
        insert_user(
            database_url,
            email="other@example.com",
            password="secret-password",
        )
    )

    login_response = client.post(
        "/auth/login",
        json={"email": "user@example.com", "password": "secret-password"},
    )
    response = client.get(
        f"/protected/{other_user}",
        headers=authorization_header(login_response.json()["access_token"]),
    )

    assert response.status_code == 403
    assert user != other_user


def test_admin_authorization_path_is_allowed(
    client: TestClient,
    database_url: str,
) -> None:
    admin = asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )
    user = asyncio.run(
        insert_user(
            database_url,
            email="user@example.com",
            password="secret-password",
        )
    )

    login_response = client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "secret-password"},
    )
    response = client.get(
        f"/protected/{user}",
        headers=authorization_header(login_response.json()["access_token"]),
    )

    assert response.status_code == 200
    assert admin != user


def test_admin_only_route_allows_admin(
    client: TestClient,
    database_url: str,
) -> None:
    asyncio.run(
        insert_user(
            database_url,
            email="admin@example.com",
            password="secret-password",
            role="admin",
        )
    )

    login_response = client.post(
        "/auth/login",
        json={"email": "admin@example.com", "password": "secret-password"},
    )
    response = client.get(
        "/admin-only",
        headers=authorization_header(login_response.json()["access_token"]),
    )

    assert response.status_code == 200
    assert response.json() == {"role": "admin"}


def test_admin_only_route_denies_normal_user(
    client: TestClient,
) -> None:
    client.post(
        "/auth/register",
        json={"email": "user@example.com", "password": "secret-password"},
    )

    login_response = client.post(
        "/auth/login",
        json={"email": "user@example.com", "password": "secret-password"},
    )
    response = client.get(
        "/admin-only",
        headers=authorization_header(login_response.json()["access_token"]),
    )

    assert response.status_code == 403
