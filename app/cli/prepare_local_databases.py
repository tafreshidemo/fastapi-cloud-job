from __future__ import annotations

import asyncio
import os
from urllib.parse import urlsplit

import asyncpg  # type: ignore[import-untyped]
from asyncpg import UniqueViolationError


def main() -> None:
    asyncio.run(run())


async def run() -> None:
    runtime_database_url = os.getenv("DATABASE_URL")
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not runtime_database_url and not test_database_url:
        return

    database_urls = [url for url in (runtime_database_url, test_database_url) if url]
    if not database_urls:
        return

    admin_source = urlsplit(
        database_urls[0].replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    maintenance_dsn = admin_source._replace(path="/postgres").geturl()

    connection = await asyncpg.connect(maintenance_dsn)
    try:
        for database_url in database_urls:
            parsed = urlsplit(database_url.replace("postgresql+asyncpg://", "postgresql://", 1))
            database_name = parsed.path.lstrip("/")
            if not database_name:
                continue
            database_exists = await connection.fetchval(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                database_name,
            )
            if not database_exists:
                try:
                    await connection.execute(f'CREATE DATABASE "{database_name}"')
                except UniqueViolationError:
                    pass
    finally:
        await connection.close()


if __name__ == "__main__":
    main()
