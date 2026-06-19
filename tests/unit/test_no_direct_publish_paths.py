from __future__ import annotations

from pathlib import Path


def test_api_and_worker_handlers_do_not_import_or_publish_to_rabbitmq_directly() -> None:
    restricted_paths = [
        Path("app/api"),
        Path("app/application/services"),
        Path("app/workers/handlers"),
    ]

    for root_path in restricted_paths:
        for source_path in root_path.rglob("*.py"):
            source = source_path.read_text(encoding="utf-8")
            assert "aio_pika" not in source
            assert "connect_robust" not in source
            assert ".publish(" not in source
