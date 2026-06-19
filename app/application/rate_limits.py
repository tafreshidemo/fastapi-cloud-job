from __future__ import annotations

from typing import Protocol
from uuid import UUID

from app.application.dto import CreateJobRateLimitReservationDTO


class CreateJobRateLimiter(Protocol):
    async def reserve(self, user_id: UUID) -> CreateJobRateLimitReservationDTO | None: ...

    async def release(self, reservation: CreateJobRateLimitReservationDTO) -> None: ...
