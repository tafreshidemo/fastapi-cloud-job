from __future__ import annotations

import argparse
import asyncio
import getpass

from app.application.services.auth import AuthService
from app.core.config import get_settings
from app.core.logging import configure_logging
from app.db.session import create_database_runtime
from app.db.uow import create_uow_factory


async def run(email: str, password: str) -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    runtime = create_database_runtime(settings)
    try:
        auth_service = AuthService(
            uow_factory=create_uow_factory(runtime.session_factory),
            settings=settings,
        )
        await auth_service.create_or_update_admin(email=email, password=password)
    finally:
        await runtime.engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Create or update an admin user")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password")
    args = parser.parse_args()
    password = args.password or getpass.getpass("Admin password: ")
    asyncio.run(run(email=args.email, password=password))


if __name__ == "__main__":
    main()
