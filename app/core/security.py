from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import jwt
from jwt import ExpiredSignatureError, InvalidTokenError
from pwdlib import PasswordHash

from app.core.clock import Clock, UtcClock
from app.core.config import Settings
from app.core.exceptions import AuthenticationError
from app.domain.enums import UserRole

_PASSWORD_HASHER = PasswordHash.recommended()


def normalize_email(email: str) -> str:
    return email.strip().lower()


def hash_password(password: str) -> str:
    return _PASSWORD_HASHER.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    return _PASSWORD_HASHER.verify(password, password_hash)


@dataclass(frozen=True, slots=True)
class AccessTokenPayload:
    subject: uuid.UUID
    role: UserRole
    issued_at: datetime
    expires_at: datetime
    jwt_id: str


class JwtTokenManager:
    def __init__(self, settings: Settings, clock: Clock | None = None) -> None:
        self._settings = settings
        self._clock = clock or UtcClock()

    def create_access_token(self, user_id: uuid.UUID, role: UserRole) -> tuple[str, datetime]:
        issued_at = self._clock.now()
        expires_at = issued_at + timedelta(minutes=self._settings.access_token_ttl_minutes)
        payload = {
            "sub": str(user_id),
            "role": role.value,
            "iat": int(issued_at.timestamp()),
            "exp": int(expires_at.timestamp()),
            "jti": str(uuid.uuid4()),
        }
        token = jwt.encode(
            payload,
            self._settings.jwt_secret,
            algorithm=self._settings.jwt_algorithm,
        )
        return token, expires_at

    def decode_access_token(self, token: str) -> AccessTokenPayload:
        try:
            payload = jwt.decode(
                token,
                self._settings.jwt_secret,
                algorithms=[self._settings.jwt_algorithm],
            )
        except ExpiredSignatureError as exc:
            raise AuthenticationError("Access token has expired") from exc
        except InvalidTokenError as exc:
            raise AuthenticationError("Access token is invalid") from exc

        try:
            return AccessTokenPayload(
                subject=uuid.UUID(payload["sub"]),
                role=UserRole(payload["role"]),
                issued_at=datetime.fromtimestamp(payload["iat"], UTC),
                expires_at=datetime.fromtimestamp(payload["exp"], UTC),
                jwt_id=str(payload["jti"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AuthenticationError("Access token is invalid") from exc
