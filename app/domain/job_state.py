from __future__ import annotations

from collections.abc import Iterable

from app.domain.enums import JobStatus


class JobStateTransitionError(ValueError):
    """Raised when a job state transition is not allowed."""


class JobStatePolicy:
    _allowed_transitions = {
        JobStatus.PENDING: {JobStatus.QUEUED, JobStatus.CANCELLED},
        JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.CANCELLED},
        JobStatus.RUNNING: {
            JobStatus.COMPLETED,
            JobStatus.QUEUED,
            JobStatus.FAILED,
            JobStatus.CANCELLED,
        },
        JobStatus.COMPLETED: set(),
        JobStatus.FAILED: set(),
        JobStatus.CANCELLED: set(),
    }

    @classmethod
    def can_transition(cls, current: JobStatus, target: JobStatus) -> bool:
        return target in cls._allowed_transitions[current]

    @classmethod
    def ensure_transition(cls, current: JobStatus, target: JobStatus) -> None:
        if cls.can_transition(current, target):
            return
        raise JobStateTransitionError(
            f"Cannot transition job from {current.value!r} to {target.value!r}"
        )

    @classmethod
    def next_states(cls, current: JobStatus) -> Iterable[JobStatus]:
        return tuple(cls._allowed_transitions[current])
