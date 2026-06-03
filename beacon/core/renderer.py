"""Beacon Jinja renderer — small, sandboxed, two-pass.

Beacon renders templates at exactly two well-defined points:

1. **Trigger time** — when the scheduler builds the ``TaskContext``. The
   ``vars()`` macro is bound to the deployment's variables stage and
   ``params`` is bound to the Deployment params. Output is concrete values
   that are stored in ``TaskContext.inputs``.

2. **Pre-execute time** — at scheduler enqueue, ``outputs.*`` from upstream
   tasks is bound and any remaining templated strings are resolved.

Plugins **never** render templates themselves; they always receive concrete
values. This is why we don't need ``NativeEnvironment``, ``DebugUndefined``
"preserve" mode, file loaders, or any of the Airflow-era machinery the
previous renderer carried.
"""

from typing import Any

from jinja2 import StrictUndefined
from jinja2.sandbox import SandboxedEnvironment

from beacon.utils import is_jinja

__all__ = ("Renderer", "render_value")


class Renderer:
    """Lean Jinja renderer.

    A renderer is created once per render pass (trigger time, execute time)
    with a fixed context, then walked recursively over inputs.
    """

    __slots__ = ("env", "ctx")

    def __init__(self, ctx: dict[str, Any] | None = None) -> None:
        self.env = SandboxedEnvironment(
            undefined=StrictUndefined,
            extensions=("jinja2.ext.do",),
            autoescape=False,
            cache_size=0,
        )
        self.ctx: dict[str, Any] = ctx or {}

    def render(self, value: Any) -> Any:
        """Recursively render ``value``. Non-string scalars pass through."""
        if isinstance(value, str):
            return self._render_string(value)
        if isinstance(value, list):
            return [self.render(v) for v in value]
        if isinstance(value, tuple):
            return tuple(self.render(v) for v in value)
        if isinstance(value, dict):
            return {k: self.render(v) for k, v in value.items()}
        return value

    def _render_string(self, value: str) -> str:
        if not is_jinja(value, pure=False):
            return value
        tmpl = self.env.from_string(value)
        return tmpl.render(**self.ctx)


def render_value(value: Any, ctx: dict[str, Any]) -> Any:
    """Convenience helper for one-off rendering."""
    return Renderer(ctx).render(value)
