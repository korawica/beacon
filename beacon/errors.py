class TaskException(Exception):
    pass


class TaskFailed(TaskException):
    pass


class TaskSkipped(TaskException):
    pass
