"""Remote plugin execution via uv isolated environments.

Remote plugins run in **completely isolated** virtual environments managed by
uv — they never touch Beacon's own dependencies. This mirrors how GitHub
Actions work: the runner installs the action's deps on the fly, completely
separate from the calling project.

Ref formats
-----------
``uses: "org/repo@version"``
    Fetched from GitHub::

        git+https://github.com/org/repo@version

``uses: "package-name@version"``
    Fetched from PyPI::

        package-name==version

uv caches environments by their dependency hash, so the second call with the
same ref reuses the cached env and does NOT re-download or re-install.

Plugin package requirements
---------------------------
The remote package must expose its plugin(s) via the ``beacon.plugins``
entry-point group in its ``pyproject.toml``::

    [project.entry-points."beacon.plugins"]
    "my-org/gcs-plugin" = "my_package.gcs:run"

The value is ``module_path:callable`` where callable is either:

* **A function** ``run(inputs: dict, context: dict) -> dict | None``
  — simple, no Beacon dependency needed.
* **A class** that behaves like a Beacon plugin (has an ``execute`` method).
  May extend ``BasePlugin`` (adds Beacon as a dep) or be a plain class.

Raise strategy from inside a remote plugin
-------------------------------------------
Use ``sys.exit(code)`` to signal control flow to Beacon:

* ``sys.exit(0)``  — success (default when the function returns normally)
* ``sys.exit(1)``  — generic failure, retry up to ``retries`` (default on
  any uncaught exception)
* ``sys.exit(2)``  — permanent failure, skip retries (equivalent to
  ``raise TaskFailed(...)``)
* ``sys.exit(3)``  — task skipped (equivalent to ``raise TaskSkipped(...)``)
* ``sys.exit(4)``  — retry requested explicitly (equivalent to
  ``raise TaskRetry(...)``)
"""

import asyncio
import json
import logging
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger("beacon.remote_plugin")

# ---------------------------------------------------------------------------
# Ref pattern
# ---------------------------------------------------------------------------

# Matches:
#   org/repo@version     e.g. my-org/gcs-plugin@1.2.0
#   package@version      e.g. beacon-gcs@1.2.0
_REMOTE_REF_RE = re.compile(
    r"^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?"
    r"(/[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)?"
    r"@[a-zA-Z0-9][a-zA-Z0-9._+\-]*$"
)

# ---------------------------------------------------------------------------
# Exit code contract (mirroring Beacon's raise strategy)
# ---------------------------------------------------------------------------

EXIT_SUCCESS = 0
"""Function returned normally."""

EXIT_FAILURE = 1
"""Unhandled exception or generic failure — retry up to ``retries``."""

EXIT_TASK_FAILED = 2
"""Permanent failure — exhaust retries immediately (like ``raise TaskFailed``)."""

EXIT_TASK_SKIPPED = 3
"""Mark task SKIPPED (like ``raise TaskSkipped``)."""

EXIT_TASK_RETRY = 4
"""Explicit retry request — consume one retry slot (like ``raise TaskRetry``)."""

# ---------------------------------------------------------------------------
# Runner script template (PEP 723 header ensures project deps are ignored)
# ---------------------------------------------------------------------------

_RUNNER_TMPL = """\
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
# Beacon remote plugin runner — generated, do not edit.
import json as _json
import sys as _sys
import asyncio as _asyncio
import inspect as _inspect
from importlib.metadata import entry_points as _eps

_EXIT_SUCCESS   = 0
_EXIT_FAILURE   = 1
_EXIT_FAILED    = 2
_EXIT_SKIPPED   = 3
_EXIT_RETRY     = 4

_plugin_name = _sys.argv[1]
_inputs      = _json.loads(_sys.argv[2]) if len(_sys.argv) > 2 else {{}}
_context     = _json.loads(_sys.argv[3]) if len(_sys.argv) > 3 else {{}}

# Discover plugin via beacon.plugins entry points
_plugin_ref = None
for _ep in _eps(group="beacon.plugins"):
    if _ep.name == _plugin_name:
        _plugin_ref = _ep.load()
        break

if _plugin_ref is None:
    print(f"No beacon.plugins entry point named {{_plugin_name!r}}", file=_sys.stderr)
    _sys.exit(_EXIT_FAILURE)

try:
    if _inspect.isclass(_plugin_ref):
        # Class-based plugin (Pydantic BasePlugin or plain class)
        try:
            _instance = _plugin_ref.model_validate(_inputs)
        except AttributeError:
            _instance = _plugin_ref(**_inputs)
        _execute = _instance.execute
        if _inspect.iscoroutinefunction(_execute):
            _result = _asyncio.run(_execute(_context))
        else:
            _result = _execute(_context)
    else:
        # Function-based: run(inputs, context) -> dict | None
        if _inspect.iscoroutinefunction(_plugin_ref):
            _result = _asyncio.run(_plugin_ref(_inputs, _context))
        else:
            _result = _plugin_ref(_inputs, _context)

    if isinstance(_result, dict):
        print(_json.dumps(_result, default=str))
    _sys.exit(_EXIT_SUCCESS)

except SystemExit:
    raise  # pass through explicit sys.exit() from the plugin

except Exception as _exc:
    print(f"{{type(_exc).__name__}}: {{_exc}}", file=_sys.stderr)
    _sys.exit(_EXIT_FAILURE)
"""


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def is_remote_ref(uses: str) -> bool:
    """Return ``True`` if *uses* looks like a remote plugin reference.

    >>> is_remote_ref("py")
    False
    >>> is_remote_ref("my-org/gcs-plugin@1.2.0")
    True
    >>> is_remote_ref("beacon-gcs@2.0.0")
    True
    """
    return bool(_REMOTE_REF_RE.match(uses))


def ref_to_plugin_name(ref: str) -> str:
    """Strip the ``@version`` suffix to get the entry-point lookup key.

    >>> ref_to_plugin_name("my-org/gcs-plugin@1.2.0")
    'my-org/gcs-plugin'
    >>> ref_to_plugin_name("beacon-gcs@2.0.0")
    'beacon-gcs'
    """
    return ref.rsplit("@", 1)[0]


async def run_remote_plugin(
    ref: str,
    inputs: dict[str, Any],
    context: dict[str, Any],
) -> tuple[dict[str, Any] | None, int]:
    """Execute a remote plugin in an isolated uv environment.

    Uses ``uv run --with <install_spec>`` so the plugin's dependencies are
    installed into a temporary virtual environment that is completely separate
    from Beacon's own virtualenv.  uv caches these envs by dependency hash, so
    repeat calls with the same *ref* are fast.

    Command executed::

        uv run --with <install_spec> /tmp/beacon_remote_<uuid>/runner.py \\
            <plugin_name> <inputs_json> <context_json>

    Args:
        ref:     ``org/repo@version`` (GitHub) or ``package@version`` (PyPI).
        inputs:  Task inputs dict (will be JSON-serialized).
        context: Runtime context dict (will be serialized — datetimes become
                 ISO strings, the ``logger`` key is excluded).

    Returns:
        ``(result_dict_or_None, exit_code)`` where exit_code follows the
        contract defined by :data:`EXIT_SUCCESS` … :data:`EXIT_TASK_RETRY`.

    Raises:
        RuntimeError: when ``uv`` exits with an unexpected code or cannot be
            found on PATH.
    """
    install_spec = _ref_to_install_spec(ref)
    plugin_name = ref_to_plugin_name(ref)
    serializable_ctx = _serialize_context(context)

    try:
        inputs_json = json.dumps(inputs, default=str)
        context_json = json.dumps(serializable_ctx, default=str)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Cannot JSON-serialize inputs/context for remote plugin {ref!r}: {exc}"
        ) from exc

    with tempfile.TemporaryDirectory(prefix="beacon_remote_") as tmpdir:
        runner = Path(tmpdir) / "runner.py"
        runner.write_text(_RUNNER_TMPL)

        logger.info(
            "Running remote plugin %r in isolated uv env: uv run --with %s",
            ref,
            install_spec,
        )

        proc = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--with",
            install_spec,
            str(runner),
            plugin_name,
            inputs_json,
            context_json,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env={**os.environ},
        )
        stdout_bytes, stderr_bytes = await proc.communicate()

        if stderr_bytes:
            for line in stderr_bytes.decode(errors="replace").splitlines():
                logger.debug("[remote:%s] %s", ref, line)

        exit_code = proc.returncode
        stdout_text = stdout_bytes.decode(errors="replace").strip()

        logger.debug(
            "Remote plugin %r exited %d stdout=%r",
            ref,
            exit_code,
            stdout_text[:200] if stdout_text else "",
        )

        result: dict[str, Any] | None = None
        if stdout_text and exit_code == EXIT_SUCCESS:
            try:
                parsed = json.loads(stdout_text)
                result = parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                logger.warning(
                    "Remote plugin %r produced non-JSON stdout: %r",
                    ref,
                    stdout_text[:200],
                )

        return result, exit_code


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ref_to_install_spec(ref: str) -> str:
    """Convert a beacon remote ref to a uv/pip install spec.

    >>> _ref_to_install_spec("my-org/gcs-plugin@1.2.0")
    'git+https://github.com/my-org/gcs-plugin@1.2.0'
    >>> _ref_to_install_spec("beacon-gcs@2.0.0")
    'beacon-gcs==2.0.0'
    """
    name, version = ref.rsplit("@", 1)
    if "/" in name:
        return f"git+https://github.com/{name}@{version}"
    else:
        return f"{name}=={version}"


def _serialize_context(context: dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-safe copy of a context dict.

    * Excludes the ``logger`` key (not serializable).
    * Converts ``datetime`` values to ISO 8601 strings.
    * Passes everything else through as-is (assumed JSON-safe).
    """
    result: dict[str, Any] = {}
    for key, val in context.items():
        if key == "logger":
            continue
        if isinstance(val, datetime):
            result[key] = val.isoformat()
        else:
            result[key] = val
    return result
