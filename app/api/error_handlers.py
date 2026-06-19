from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.core.exceptions import ApplicationError
from app.core.request_context import REQUEST_ID_HEADER, get_request_id

logger = logging.getLogger(__name__)


def _resolve_request_id(request: Request) -> str | None:
    state_request_id = getattr(request.state, "request_id", None)
    if isinstance(state_request_id, str) and state_request_id:
        return state_request_id
    return get_request_id()


def register_error_handlers(app: FastAPI) -> None:
    def build_error_response(
        request: Request,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | list[dict[str, Any]] | None = None,
        headers: dict[str, str] | None = None,
    ) -> JSONResponse:
        response_headers = dict(headers or {})
        request_id = _resolve_request_id(request)
        if request_id is not None:
            response_headers[REQUEST_ID_HEADER] = request_id
        return JSONResponse(
            status_code=status_code,
            content={
                "error": {
                    "code": code,
                    "message": message,
                    "details": details,
                }
            },
            headers=response_headers,
        )

    @app.exception_handler(ApplicationError)
    async def handle_application_error(
        request: Request,
        exc: ApplicationError,
    ) -> JSONResponse:
        logger.info(
            "application_error",
            extra={
                "error_code": exc.code,
                "status_code": exc.status_code,
                "path": request.url.path,
                "method": request.method,
            },
        )
        return build_error_response(
            request,
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
            headers=exc.headers,
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        logger.info(
            "http_error",
            extra={
                "status_code": exc.status_code,
                "path": request.url.path,
                "method": request.method,
            },
        )
        code_map = {
            401: "AUTHENTICATION_REQUIRED",
            403: "ACCESS_DENIED",
            404: "NOT_FOUND",
            422: "INVALID_REQUEST",
        }
        message_map = {
            401: "Authentication credentials were not provided",
            403: "Access denied",
            404: "Resource not found",
            422: "Request validation failed",
        }
        return build_error_response(
            request,
            status_code=exc.status_code,
            code=code_map.get(exc.status_code, "HTTP_ERROR"),
            message=message_map.get(exc.status_code, "Request failed"),
            details=None,
        )

    @app.exception_handler(RequestValidationError)
    async def handle_request_validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        logger.info(
            "request_validation_error",
            extra={
                "path": request.url.path,
                "method": request.method,
                "error_count": len(exc.errors()),
            },
        )
        return build_error_response(
            request,
            status_code=422,
            code="INVALID_REQUEST",
            message="Request validation failed",
            details=None,
        )

    @app.exception_handler(Exception)
    async def handle_unexpected_exception(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        logger.exception(
            "unexpected_error",
            extra={
                "path": request.url.path,
                "method": request.method,
            },
        )
        return build_error_response(
            request,
            status_code=500,
            code="INTERNAL_SERVER_ERROR",
            message="An internal server error occurred",
            details=None,
        )
