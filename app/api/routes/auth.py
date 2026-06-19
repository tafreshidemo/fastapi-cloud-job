from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status

from app.api.dependencies.services import get_auth_service
from app.application.services.auth import AuthService
from app.schemas.auth import LoginRequest, LoginResponse, RegisterRequest, UserResponse

router = APIRouter(prefix="/auth", tags=["auth"])
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


@router.post(
    "/register",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user",
    description="Public registration always creates a user with role `user`.",
    responses={
        409: {"description": "Email already exists"},
    },
)
async def register(
    payload: RegisterRequest,
    auth_service: AuthServiceDep,
) -> UserResponse:
    user = await auth_service.register_user(
        email=payload.email,
        password=payload.password,
    )
    return UserResponse.model_validate(user)


@router.post(
    "/login",
    response_model=LoginResponse,
    summary="Authenticate a user",
    description="Authenticate with email and password and return a bearer access token.",
)
async def login(
    payload: LoginRequest,
    auth_service: AuthServiceDep,
) -> LoginResponse:
    result = await auth_service.login_user(
        email=payload.email,
        password=payload.password,
    )
    return LoginResponse.model_validate(result)
