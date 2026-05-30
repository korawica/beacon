from enum import Enum


class TriggerRule(Enum):
    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    ONE_DONE = "one_done"
