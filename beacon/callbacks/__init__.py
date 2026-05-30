from collections.abc import Callable

from pydantic import BaseModel, Field


class BaseDagCallback(BaseModel):
    hook: Callable = Field()


class OnStart(BaseDagCallback): ...


class OnFailure(BaseDagCallback): ...


class OnSuccess(BaseDagCallback): ...


class OnComplete(BaseDagCallback): ...


class BaseTaskCallback(BaseModel):
    hook: Callable = Field()


class OnTaskStart(BaseTaskCallback): ...


class OnTaskFailure(BaseTaskCallback): ...


class OnTaskRetry(BaseTaskCallback): ...


class OnTaskSuccess(BaseTaskCallback): ...


class OnTaskComplete(BaseTaskCallback): ...
