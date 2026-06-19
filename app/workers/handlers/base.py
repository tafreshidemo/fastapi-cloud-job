from __future__ import annotations


class JobExecutionError(RuntimeError):
    """Raised when a job handler fails in an expected business way."""


class JobCancellationRequestedError(RuntimeError):
    """Raised when a handler observes a cooperative cancellation request."""
