class TaskException(Exception):
    pass


class TaskFailed(TaskException):
    """Raise inside a plugin to signal a permanent failure. No retries."""

    pass


class TaskSkipped(TaskException):
    """Raise inside a plugin to skip this task and mark it SKIPPED."""

    pass


class TaskRetry(TaskException):
    """Raise inside a plugin to explicitly request a retry.

    Consumes one retry slot the same as an unhandled exception, but
    communicates intent clearly. If no retries remain the task fails.
    """

    pass
