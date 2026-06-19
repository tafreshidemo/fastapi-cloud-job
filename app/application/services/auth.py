from __future__ import annotations

from app.application.dto import CurrentUserDTO, LoginResultDTO, UserPublicDTO
from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.core.exceptions import AuthenticationError, DuplicateEmailError, InactiveUserError
from app.core.security import JwtTokenManager, hash_password, normalize_email, verify_password
from app.db.uow import UnitOfWorkFactory
from app.domain.enums import UserRole


class AuthService:
    def __init__(
        self,
        uow_factory: UnitOfWorkFactory,
        settings: Settings,
        clock: Clock | None = None,
    ) -> None:
        self._uow_factory = uow_factory
        self._clock = clock or UtcClock()
        self._token_manager = JwtTokenManager(settings, self._clock)

    async def register_user(self, email: str, password: str) -> UserPublicDTO:
        normalized_email = normalize_email(email)
        password_hash = hash_password(password)

        async with self._uow_factory() as uow:
            existing_user = await uow.users.get_by_email(normalized_email)
            if existing_user is not None:
                raise DuplicateEmailError()

            user = await uow.users.create_user(
                email=normalized_email,
                password_hash=password_hash,
                role=UserRole.USER.value,
                is_active=True,
            )
            await uow.commit()

        return UserPublicDTO(
            id=user.id,
            email=user.email,
            role=UserRole(user.role),
            is_active=user.is_active,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )

    async def login_user(self, email: str, password: str) -> LoginResultDTO:
        normalized_email = normalize_email(email)

        async with self._uow_factory() as uow:
            user = await uow.users.get_by_email(normalized_email)
            if user is None:
                raise AuthenticationError("Invalid email or password")
            user_id = user.id
            user_role = UserRole(user.role)
            is_active = user.is_active
            password_hash = user.password_hash

        if not verify_password(password, password_hash):
            raise AuthenticationError("Invalid email or password")
        if not is_active:
            raise InactiveUserError()

        token, expires_at = self._token_manager.create_access_token(
            user_id=user_id,
            role=user_role,
        )
        return LoginResultDTO(
            access_token=token,
            token_type="bearer",
            expires_at=expires_at,
        )

    async def get_current_user(self, access_token: str) -> CurrentUserDTO:
        claims = self._token_manager.decode_access_token(access_token)

        async with self._uow_factory() as uow:
            user = await uow.users.get_by_id(claims.subject)
            if user is None:
                raise AuthenticationError("Access token is invalid")
            if not user.is_active:
                raise InactiveUserError()

            return CurrentUserDTO(
                id=user.id,
                email=user.email,
                role=UserRole(user.role),
                is_active=user.is_active,
            )

    async def create_or_update_admin(self, email: str, password: str) -> UserPublicDTO:
        normalized_email = normalize_email(email)
        password_hash = hash_password(password)

        async with self._uow_factory() as uow:
            user = await uow.users.create_or_update_admin(
                email=normalized_email,
                password_hash=password_hash,
            )
            await uow.commit()

        return UserPublicDTO(
            id=user.id,
            email=user.email,
            role=UserRole(user.role),
            is_active=user.is_active,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )
