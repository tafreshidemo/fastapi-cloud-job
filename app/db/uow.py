from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import Protocol, Self

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.repositories.job_logs import JobLogRepository, SqlAlchemyJobLogRepository
from app.repositories.jobs import JobRepository, SqlAlchemyJobRepository
from app.repositories.outbox import OutboxRepository, SqlAlchemyOutboxRepository
from app.repositories.users import SqlAlchemyUserRepository, UserRepository


class UnitOfWork(Protocol):
    users: UserRepository
    jobs: JobRepository
    job_logs: JobLogRepository
    outbox: OutboxRepository

    async def __aenter__(self) -> Self: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None: ...

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...


UnitOfWorkFactory = Callable[[], UnitOfWork]


class SqlAlchemyUnitOfWork:
    users: UserRepository
    jobs: JobRepository
    job_logs: JobLogRepository
    outbox: OutboxRepository

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None
        self._committed = False

    async def __aenter__(self) -> Self:
        self._session = self._session_factory()
        self._committed = False
        self.users = SqlAlchemyUserRepository(self._session)
        self.jobs = SqlAlchemyJobRepository(self._session)
        self.job_logs = SqlAlchemyJobLogRepository(self._session)
        self.outbox = SqlAlchemyOutboxRepository(self._session)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._session is None:
            return
        try:
            if exc_type is not None or not self._committed:
                await self._session.rollback()
        finally:
            await self._session.close()
            self._session = None

    async def commit(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork is not active")
        await self._session.commit()
        self._committed = True

    async def rollback(self) -> None:
        if self._session is None:
            raise RuntimeError("UnitOfWork is not active")
        await self._session.rollback()
        self._committed = False

# Build a fresh Unit of Work per use case so sessions do not leak across requests or worker loops.
def create_uow_factory(
    session_factory: async_sessionmaker[AsyncSession],
) -> UnitOfWorkFactory:
    def factory() -> UnitOfWork:
        return SqlAlchemyUnitOfWork(session_factory)

    return factory
