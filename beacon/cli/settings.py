"""Effective settings, sourced from ``BEACON_*`` env vars.

A single source of truth so ``beacon config show`` and the rest of the
CLI agree on what's in effect. Each setting tracks its origin
(``env`` vs ``default``) so the operator can audit what's set.
"""

import os
from dataclasses import dataclass
from typing import Any

# (env_var, default, cast)
_SPEC: list[tuple[str, Any, type]] = [
    ("BEACON_METADATA_PATH", "./metadata.db", str),
    ("BEACON_LOG_DIR", "./logs", str),
    ("BEACON_LOG_LEVEL", "INFO", str),
    ("BEACON_LOG_SINK", "file", str),
    ("BEACON_LOG_BATCH_SIZE", 100, int),
    ("BEACON_LOG_FLUSH_INTERVAL_MS", 500, int),
    ("BEACON_SCHEDULER_TICK_SECONDS", 5, int),
    ("BEACON_SCHEDULER_MAX_CONCURRENT_RUNS", 8, int),
]


@dataclass(frozen=True, slots=True)
class Setting:
    name: str
    value: Any
    source: str  # "env" | "default"


def load_settings() -> dict[str, Setting]:
    """Return all known settings with their effective value + source."""
    out: dict[str, Setting] = {}
    for name, default, cast in _SPEC:
        raw = os.environ.get(name)
        if raw is None:
            out[name] = Setting(name, default, "default")
        else:
            try:
                out[name] = Setting(name, cast(raw), "env")
            except TypeError, ValueError:
                out[name] = Setting(name, raw, "env")  # leave raw if bad cast
    return out


def get(name: str) -> Any:
    """Fetch one setting's value."""
    return load_settings()[name].value
