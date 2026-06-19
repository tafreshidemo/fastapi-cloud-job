from __future__ import annotations

import uuid
from typing import Protocol, cast

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.exceptions import DuplicateEmailError
from app.models.user import User


class UserRepository(Protocol):
    async def add(self, user: User) -> None: ...

    async def create_user(
        self,
        email: str,
        password_hash: str,
        role: str,
        is_active: bool,
    ) -> User: ...

    async def create_or_update_admin(self, email: str, password_hash: str) -> User: ...

    async def get_by_id(self, user_id: uuid.UUID) -> User | None: ...

    async def get_by_email(self, email: str) -> User | None: ...

    # Used by worker-side flows that need a stable per-user lock before changing job capacity/state.
    async def lock_by_id(self, user_id: uuid.UUID) -> User | None: ...


class SqlAlchemyUserRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # Keep this small for tests and admin bootstrap paths that already build the User model themselves.
    async def add(self, user: User) -> None:
        self._session.add(user)

    async def create_user(
        self,
        email: str,
        password_hash: str,
        role: str,
        is_active: bool,
    ) -> User:
        user = User(
            email=email,
            password_hash=password_hash,
            role=role,
            is_active=is_active,
        )
        self._session.add(user)
        try:
            await self._session.flush()
        except IntegrityError as exc:
            if _is_duplicate_email_integrity_error(exc):
                raise DuplicateEmailError() from exc
            raise
        return user

    async def create_or_update_admin(self, email: str, password_hash: str) -> User:
        statement = select(User).where(User.email == email).with_for_update()
        user = cast(User | None, await self._session.scalar(statement))
        if user is None:
            user = User(
                email=email,
                password_hash=password_hash,
                role="admin",
                is_active=True,
            )
            self._session.add(user)
        else:
            user.password_hash = password_hash
            user.role = "admin"
            user.is_active = True
        await self._session.flush()
        return user

    async def get_by_id(self, user_id: uuid.UUID) -> User | None:
        return await self._session.get(User, user_id)

    async def get_by_email(self, email: str) -> User | None:
        statement = select(User).where(User.email == email)
        return cast(User | None, await self._session.scalar(statement))

    async def lock_by_id(self, user_id: uuid.UUID) -> User | None:
        statement = select(User).where(User.id == user_id).with_for_update()
        return cast(User | None, await self._session.scalar(statement))


def _is_duplicate_email_integrity_error(exc: IntegrityError) -> bool:
    original_error = exc.orig
    constraint_name = cast(str | None, getattr(original_error, "constraint_name", None))
    sqlstate = cast(
        str | None,
        getattr(original_error, "sqlstate", None) or getattr(original_error, "pgcode", None),
    )

    if sqlstate != "23505":
        return False
    if constraint_name is not None:
        return constraint_name == "uq_users_email"

    error_text = str(original_error).lower()
    return "users" in error_text and "email" in error_text
