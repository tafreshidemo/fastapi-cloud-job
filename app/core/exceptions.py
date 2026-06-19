from __future__ import annotations

from typing import Any


class ApplicationError(Exception):
    status_code = 400
    code = "APPLICATION_ERROR"
    message = "Application error"

    def __init__(
        self,
        message: str | None = None,
        *,
        details: dict[str, Any] | list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        resolved_message = message or self.message
        super().__init__(resolved_message)
        self.message = resolved_message
        self.details = details
        self.headers = headers


class ConflictError(ApplicationError):
    status_code = 409
    code = "CONFLICT"
    message = "Conflict"


class DuplicateEmailError(ConflictError):
    code = "EMAIL_ALREADY_REGISTERED"
    message = "Email is already registered"


class DuplicateJobIdempotencyKeyError(ConflictError):
    code = "IDEMPOTENCY_KEY_REUSED"
    message = "Idempotency key is already in use"


class IdempotencyConflictError(ConflictError):
    code = "IDEMPOTENCY_KEY_REUSED"
    message = "Idempotency key conflicts with a different request"


class JobNotCancellableError(ConflictError):
    code = "JOB_NOT_CANCELLABLE"
    message = "Job cannot be cancelled in its current state"


class ResourceNotFoundError(ApplicationError):
    status_code = 404
    code = "RESOURCE_NOT_FOUND"
    message = "Resource not found"


class AuthenticationError(ApplicationError):
    status_code = 401
    code = "AUTHENTICATION_FAILED"
    message = "Authentication failed"


class InactiveUserError(ApplicationError):
    status_code = 403
    code = "INACTIVE_USER"
    message = "User account is inactive"


class AuthorizationError(ApplicationError):
    status_code = 403
    code = "ACCESS_DENIED"
    message = "Access denied"


class InvalidRequestError(ApplicationError):
    status_code = 400
    code = "INVALID_REQUEST"
    message = "Request is invalid"


class InvalidCursorError(ApplicationError):
    status_code = 400
    code = "INVALID_CURSOR"
    message = "Cursor is invalid"
