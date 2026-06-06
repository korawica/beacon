"""Beacon Jinja renderer — small, sandboxed, native-typed, two-pass.

Beacon renders templates at exactly two well-defined points:

1. **Trigger time** — when the scheduler builds the ``TaskContext``. The
   ``vars()`` function is bound to the deployment's variable scope and
   ``secrets()`` reads from environment variables. Output is concrete values
   that are stored in ``TaskContext.inputs``.

2. **Pre-execute time** — at scheduler enqueue, ``outputs.*`` from upstream
   tasks is bound and any remaining templated strings are resolved.

**Real types are preserved.** A pure-expression template returns the
underlying Python value (int / float / bool / list / dict / None / ...),
not its ``str()``. This is what ``jinja2.nativetypes.NativeEnvironment``
provides, combined with ``SandboxedEnvironment`` so dunder/attribute
attacks remain blocked.

Examples::

    "{{ x }}" with x = 5        → 5            (int, not "5")
    "{{ x }}" with x = [1, 2]   → [1, 2]       (list, not "[1, 2]")
    "{{ x }}" with x = False    → False        (bool, not "False")
    "{{ x }}" with x = None     → None         (NoneType, not "None")
    "prefix-{{ x }}" x = 5      → "prefix-5"   (mixed → str, correct)

Plugins **never** render templates themselves; they always receive
concrete, correctly-typed values. The renderer is the only Jinja contact
point in the system.

Template functions:

``vars(key, default=None)``
    Access variables from the scoped variable chain. Supports nested keys
    via dot notation::

        {{ vars("bucket") }}                    → "my-bucket"
        {{ vars("db.host") }}                   → nested: vars["db"]["host"]
        {{ vars("missing", "fallback") }}       → "fallback"

``secrets(key)``
    Access environment variables. Use for API keys, passwords, etc::

        {{ secrets("API_KEY") }}                → os.environ["API_KEY"]
        {{ secrets("DB_PASSWORD") }}            → os.environ["DB_PASSWORD"]
"""

import os
from typing import Any

from jinja2 import StrictUndefined, Undefined
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import SandboxedEnvironment

from beacon.utils import is_jinja

__all__ = ("Renderer", "make_vars_func", "make_secrets_func")


class _SandboxedNativeEnvironment(NativeEnvironment, SandboxedEnvironment):
    """Native-typed return values + sandbox restrictions.

    MRO matters: ``NativeEnvironment`` first so ``template_class`` resolves
    to ``NativeTemplate`` (which post-processes the rendered string through
    ``ast.literal_eval``). ``SandboxedEnvironment`` second contributes the
    ``is_safe_attribute`` / ``is_safe_callable`` guards that block
    ``__class__.__mro__`` style escapes.
    """


# Module-level shared environment. Compiling a Jinja template is the
# expensive part; the env's internal LRU caches parsed templates so
# repeated renders of the same string (e.g. across many tasks of the same
# DAG, or across many runs of the same deployment) skip the parse step.
# The env is stateless w.r.t. context, so sharing is safe.
_ENV = _SandboxedNativeEnvironment(
    undefined=StrictUndefined,
    extensions=("jinja2.ext.do",),
    autoescape=False,
    cache_size=400,
)


def make_vars_func(
    variables: dict[str, Any],
    *,
    unresolved_sentinel: str = "<unresolved>",
) -> callable:
    """Create a ``vars(key, default=None)`` function for Jinja templates.

    Supports nested key access via dot notation::

        vars("bucket")              → variables["bucket"]
        vars("db.host")             → variables["db"]["host"]
        vars("missing", "default")  → "default" if key not found

    Args:
        variables: The variable dict to read from.
        unresolved_sentinel: Value to return when key is missing and no
            default is provided. Used by ``beacon plan`` to show unresolved
            variables without failing.

    Returns:
        A callable ``vars(key: str, default: Any = None) -> Any``.
    """

    def vars_func(key: str, default: Any = None) -> Any:
        """Access variables with optional nested key support."""
        if "." in key:
            # Nested access: "db.host" → variables["db"]["host"]
            parts = key.split(".")
            value = variables
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    # Key path not found
                    if default is not None:
                        return default
                    return f"{unresolved_sentinel}: vars('{key}')"
            return value
        else:
            # Simple access
            if key in variables:
                return variables[key]
            if default is not None:
                return default
            return f"{unresolved_sentinel}: vars('{key}')"

    return vars_func


def make_secrets_func(prefix: str | None = None) -> callable:
    """Create a ``secrets(key)`` function for Jinja templates.

    Reads from ``os.environ``. Optionally filters by prefix::

        secrets("API_KEY")          → os.environ["API_KEY"]
        secrets("DB_PASSWORD")      → os.environ["DB_PASSWORD"]

    Args:
        prefix: Optional prefix to strip from keys. E.g., prefix="BEACON_"
            means ``secrets("API_KEY")`` reads ``BEACON_API_KEY``.

    Returns:
        A callable ``secrets(key: str) -> str | None``.
    """

    def secrets_func(key: str) -> str | None:
        """Access environment variables."""
        env_key = f"{prefix}{key}" if prefix else key
        value = os.environ.get(env_key)
        if value is None:
            return f"<unresolved: secrets('{key}')>"
        return value

    return secrets_func


class Renderer:
    """Lean Jinja renderer.

    A renderer is created once per render pass (trigger time, execute time)
    with a fixed context, then walked recursively over inputs.
    """

    __slots__ = ("ctx",)

    def __init__(self, ctx: dict[str, Any] | None = None) -> None:
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

    def _render_string(self, value: str) -> Any:
        """Render a single string template.

        Returns whatever Python value the template evaluates to (real
        types for pure expressions, ``str`` for mixed templates and
        non-literal results). Non-template strings pass through unchanged.
        """
        if not is_jinja(value):
            return value
        tmpl = _ENV.from_string(value)
        result = tmpl.render(**self.ctx)
        # NativeEnvironment can return an Undefined directly (when the
        # whole template is just `{{ missing }}`), bypassing
        # StrictUndefined's __str__ guard. Force the error.
        if isinstance(result, Undefined):
            str(result)  # raises UndefinedError for StrictUndefined
        return result
