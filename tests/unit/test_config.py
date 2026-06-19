from __future__ import annotations

from app.core.config import ProductionSettingsError, Settings, get_settings


def test_settings_parse_valid_environment(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("LOG_LEVEL", "info")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/app")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    monkeypatch.setenv("JWT_SECRET", "x" * 32)
    get_settings.cache_clear()

    settings = Settings()

    assert settings.app_env == "development"
    assert settings.log_level == "INFO"
    assert settings.jwt_algorithm == "HS256"
    assert settings.job_max_running_per_user == 3
    assert settings.job_max_retries == 3


def test_missing_critical_secret_fails_with_readable_error(monkeypatch) -> None:
    monkeypatch.delenv("JWT_SECRET", raising=False)
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5432/app")
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
    get_settings.cache_clear()

    try:
        get_settings()
    except RuntimeError as exc:
        assert "JWT_SECRET" in str(exc)
    else:
        raise AssertionError("Expected get_settings() to fail when JWT_SECRET is missing")


def test_production_settings_reject_placeholder_secret(monkeypatch) -> None:
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://user:pass@db:5432/app")
    monkeypatch.setenv("REDIS_URL", "redis://redis:6379/0")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    monkeypatch.setenv("JWT_SECRET", "change-me")

    settings = Settings()

    try:
        settings.validate_runtime()
    except ProductionSettingsError as exc:
        assert "JWT_SECRET" in str(exc)
    else:
        raise AssertionError(
            "Expected production runtime validation to reject placeholder JWT_SECRET"
        )
