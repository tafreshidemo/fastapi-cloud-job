from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.api.dependencies.services import get_auth_service
from app.application.dto import CurrentUserDTO
from app.application.services.auth import AuthService
from app.core.exceptions import AuthorizationError
from app.domain.enums import UserRole

bearer_scheme = HTTPBearer(auto_error=False)
CredentialsDep = Annotated[HTTPAuthorizationCredentials | None, Depends(bearer_scheme)]
AuthServiceDep = Annotated[AuthService, Depends(get_auth_service)]


async def get_current_user(
    credentials: CredentialsDep,
    auth_service: AuthServiceDep,
) -> CurrentUserDTO:
    if credentials is None:
        raise HTTPException(status_code=401, detail="Authentication credentials were not provided")
    return await auth_service.get_current_user(credentials.credentials)


CurrentUserDep = Annotated[CurrentUserDTO, Depends(get_current_user)]


def ensure_owner_or_admin(current_user: CurrentUserDTO, owner_id: UUID) -> None:
    if current_user.role == UserRole.ADMIN:
        return
    if current_user.id == owner_id:
        return
    raise AuthorizationError("You do not have access to this resource")


async def require_owner_or_admin_user(
    owner_id: UUID,
    current_user: CurrentUserDep,
) -> CurrentUserDTO:
    ensure_owner_or_admin(current_user=current_user, owner_id=owner_id)
    return current_user


async def require_admin_user(
    current_user: CurrentUserDep,
) -> CurrentUserDTO:
    if current_user.role != UserRole.ADMIN:
        raise AuthorizationError("Admin access is required")
    return current_user
