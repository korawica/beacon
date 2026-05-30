class ActionException(Exception):
    pass


class ActionFailed(ActionException):
    pass


class ActionSkipped(ActionException):
    pass
