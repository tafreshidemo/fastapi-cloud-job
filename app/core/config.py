from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProductionSettingsError(ValueError):
    """Raised when production configuration is unsafe."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_env: Literal["development", "test", "production"] = Field(
        alias="APP_ENV",
    )
    log_level: str = Field(alias="LOG_LEVEL")
    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")
    rabbitmq_url: str = Field(alias="RABBITMQ_URL")
    jwt_secret: str = Field(alias="JWT_SECRET", min_length=1)
    jwt_algorithm: Literal["HS256"] = Field(default="HS256", alias="JWT_ALGORITHM")
    access_token_ttl_minutes: int = Field(default=30, alias="ACCESS_TOKEN_TTL_MINUTES", ge=1)
    job_max_running_per_user: int = Field(default=3, alias="JOB_MAX_RUNNING_PER_USER", ge=1)
    job_max_retries: int = Field(default=3, alias="JOB_MAX_RETRIES", ge=0)
    job_lease_seconds: int = Field(default=60, alias="JOB_LEASE_SECONDS", ge=1)
    outbox_poll_interval_seconds: int = Field(
        default=1,
        alias="OUTBOX_POLL_INTERVAL_SECONDS",
        ge=1,
    )
    outbox_batch_size: int = Field(default=50, alias="OUTBOX_BATCH_SIZE", ge=1)
    recovery_poll_interval_seconds: int = Field(
        default=5,
        alias="RECOVERY_POLL_INTERVAL_SECONDS",
        ge=1,
    )
    recovery_batch_size: int = Field(default=50, alias="RECOVERY_BATCH_SIZE", ge=1)
    cache_ttl_seconds: int = Field(default=30, alias="CACHE_TTL_SECONDS", ge=1)
    create_job_rate_limit: int = Field(default=10, alias="CREATE_JOB_RATE_LIMIT", ge=1)
    create_job_rate_window_seconds: int = Field(
        default=60,
        alias="CREATE_JOB_RATE_WINDOW_SECONDS",
        ge=1,
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        allowed = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
        if normalized not in allowed:
            allowed_levels = ", ".join(sorted(allowed))
            raise ValueError(f"LOG_LEVEL must be one of: {allowed_levels}")
        return normalized

    @field_validator("database_url", "redis_url", "rabbitmq_url", "jwt_secret")
    @classmethod
    def strip_non_empty(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped

    # Fail fast in production if the app is still using development-grade secrets.
    def validate_runtime(self) -> None:
        if self.app_env != "production":
            return
        if self.jwt_secret.lower() in {
            "change-me",
            "change-me-to-a-long-random-secret",
            "dev-secret",
            "secret",
        }:
            raise ProductionSettingsError(
                "JWT_SECRET must not use a placeholder value in production"
            )
        if len(self.jwt_secret) < 32:
            raise ProductionSettingsError("JWT_SECRET must be at least 32 characters in production")


# Cache parsed settings so request handlers and background workers share the same validated config.
@lru_cache
def get_settings() -> Settings:
    try:
        return Settings()  # type: ignore[call-arg]
    except ValidationError as exc:
        missing = [error["loc"][0] for error in exc.errors() if error["type"] == "missing"]
        if missing:
            field_names = ", ".join(sorted(str(field) for field in missing))
            raise RuntimeError(f"Missing required environment settings: {field_names}") from exc
        raise RuntimeError(f"Invalid application settings: {exc}") from exc
