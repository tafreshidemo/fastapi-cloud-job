from __future__ import annotations

import pytest

from tests.support import DEFAULT_TEST_DATABASE_URL, get_test_database_url, require_test_database


def test_get_test_database_url_uses_safe_test_database_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("TEST_DATABASE_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    assert get_test_database_url() == DEFAULT_TEST_DATABASE_URL


def test_require_test_database_refuses_runtime_database() -> None:
    with pytest.raises(RuntimeError, match="Refusing to reset non-test database: cloud_job"):
        require_test_database("postgresql+asyncpg://postgres:postgres@127.0.0.1:54329/cloud_job")
