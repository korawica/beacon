"""Python Plugin.

Executes a user-defined Python function from a file or inline code string.

Isolated execution with uv (PEP 723):
    When a .py script contains a PEP 723 inline metadata block, Beacon
    automatically runs it via ``uv run`` in an isolated virtual environment::

        # /// script
        # requires-python = ">=3.11"
        # dependencies = ["pandas>=2.0", "requests"]
        # ///

        def main(source: str):
            import pandas as pd
            return {"rows": 100}

    The function's return dict is captured as JSON via stdout and returned
    to Beacon as task outputs.  No extra configuration is needed.

Template search path:
    Files ending in .py are searched in:
      1. <dag_folder>/assets/  (DAG-local assets, higher priority)
      2. <bundle_root>/assets/ (bundle-global assets)
"""

import asyncio
import json
import logging
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any, ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin
from .....core.context import build_runtime_dict
from .....core.template import render_template_file, render_template_string
from .....runtime import (
    RuntimeContext,
    _clear_runtime_context,
    _set_runtime_context,
)

if TYPE_CHECKING:
    from .....core import Context

logger = logging.getLogger("beacon.plugin.py")

# Matches a PEP 723 inline script metadata block.
_PEP723_RE = re.compile(
    r"(^# /// script\s*\n(?:#[^\n]*\n)*?^# ///\s*$)",
    re.MULTILINE,
)

# Wrapper run by uv: imports user module, calls function, prints JSON result.
_UV_WRAPPER_TMPL = """\
{pep723_block}
import json as _json
import sys as _sys
import importlib.util as _iu

_spec = _iu.spec_from_file_location("_beacon_user_module", {script_path!r})
_mod = _iu.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_func = getattr(_mod, {function!r})
_kwargs = _json.loads(_sys.argv[1]) if len(_sys.argv) > 1 else {{}}
_result = _func(**_kwargs)
if _result is not None:
    print(_json.dumps(_result, default=str))
"""


def _extract_pep723_block(code: str) -> str | None:
    """Return the raw ``# /// script ... # ///`` block from code, or None."""
    m = _PEP723_RE.search(code)
    return m.group(1) if m else None


class PythonPlugin(BasePlugin, plugin_name="py"):
    template_ext: ClassVar[tuple[str, ...]] = (".py",)

    py_statement: str = Field(
        description=(
            "Path to Python file (ending in .py) or inline Python code string. "
            "Files with a PEP 723 '# /// script' block are run via 'uv run' "
            "in an isolated virtual environment."
        )
    )
    py_function: str = Field(
        default="main",
        description="Function name to call in the Python file or code",
    )
    py_teardown: str | None = Field(
        default=None,
        description="Function name to call as teardown (always, after success or failure)",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Keyword arguments passed to the function",
    )
    env: dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to set during execution",
    )

    async def execute(self, context: Context) -> dict[str, Any] | None:
        """Execute the user's Python function.

        When the resolved script contains a PEP 723 ``# /// script`` metadata
        block the function is run via ``uv run`` in an isolated environment
        with the declared dependencies installed automatically.  Otherwise the
        function is executed in-process (original behaviour).
        """
        template_context = self._build_template_context(context)
        rendered_code, source_name = self._render_code(template_context)

        original_env = self._apply_env(self.env)
        runtime_ctx = self._build_runtime_ctx(context)
        _set_runtime_context(runtime_ctx)

        try:
            pep723_block = _extract_pep723_block(rendered_code)

            if pep723_block is not None:
                logger.info(
                    "PEP 723 metadata detected in %s -- running via 'uv run'",
                    source_name,
                )
                result = await _run_uv_isolated(
                    rendered_code=rendered_code,
                    pep723_block=pep723_block,
                    function=self.py_function,
                    params=self.params,
                    env=self.env,
                    source_name=source_name,
                )
            else:
                module = self._import_code(rendered_code)
                func = getattr(module, self.py_function, None)
                if func is None:
                    raise AttributeError(
                        f"Function {self.py_function!r} not found in {source_name}"
                    )
                if not callable(func):
                    raise TypeError(
                        f"{self.py_function!r} in {source_name} is not callable"
                    )
                logger.info(
                    "Executing %s:%s with params=%s",
                    source_name,
                    self.py_function,
                    list(self.params.keys()),
                )
                result = func(**self.params)
                if asyncio.iscoroutine(result):
                    result = await result
                result = result if isinstance(result, dict) else None

        finally:
            self._restore_env(original_env)
            _clear_runtime_context()

        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_template_context(self, context: Context) -> dict:
        from .....core.renderer import make_vars_func, make_secrets_func

        vars_func = make_vars_func(context.get("variables", {}))
        secrets_func = make_secrets_func()

        return {
            "vars": vars_func,
            "secrets": secrets_func,
            "runtime": build_runtime_dict(
                run_id=context.get("run_id", ""),
                dag_id=context.get("dag_id", ""),
                task_id=context.get("task_id", ""),
                run_date=context.get("run_date"),
                logical_date=context.get("logical_date"),
                data_interval_start=context.get("data_interval_start"),
                data_interval_end=context.get("data_interval_end"),
                attempt_number=context.get("attempt_number", 1),
            ),
            "outputs": context.get("upstream_outputs", {}),
        }

    def _render_code(self, template_context: dict) -> tuple[str, str]:
        """Render py_statement; return (rendered_code, source_name)."""
        stripped = self.py_statement.strip()
        is_file = stripped.endswith(".py")
        is_absolute = stripped.startswith("/") or (
            len(stripped) > 1 and stripped[1] == ":"
        )

        if is_file:
            if is_absolute:
                py_path = Path(stripped)
                if not py_path.exists():
                    raise FileNotFoundError(f"File not found: {py_path}")
                code = py_path.read_text()
                return render_template_string(code, template_context), stripped
            else:
                return (
                    render_template_file(stripped, template_context),
                    stripped,
                )
        else:
            return (
                render_template_string(self.py_statement, template_context),
                "<inline>",
            )

    def _build_runtime_ctx(self, context: Context) -> RuntimeContext:
        task_logger = logging.getLogger(
            f"beacon.task.{context.get('dag_id', '')}.{context.get('task_id', '')}"
        )
        return RuntimeContext(
            run_id=context.get("run_id", ""),
            dag_id=context.get("dag_id", ""),
            task_id=context.get("task_id", ""),
            attempt_number=context.get("attempt_number", 1),
            variables=context.get("variables", {}),
            upstream_outputs=context.get("upstream_outputs", {}),
            run_date=context.get("run_date"),
            logical_date=context.get("logical_date"),
            data_interval_start=context.get("data_interval_start"),
            data_interval_end=context.get("data_interval_end"),
            logger=task_logger,
        )

    @staticmethod
    def _apply_env(env: dict[str, str]) -> dict[str, str | None]:
        original: dict[str, str | None] = {}
        for key, value in env.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value
        return original

    @staticmethod
    def _restore_env(original: dict[str, str | None]) -> None:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @staticmethod
    def _import_code(code: str):
        """Import Python code as a transient in-process module."""
        import uuid

        module_name = f"_beacon_inline_{uuid.uuid4().hex[:8]}"
        module = type(sys)(module_name)
        module.__file__ = "<inline>"
        module.__dict__["__name__"] = module_name
        exec(code, module.__dict__)
        return module

    async def teardown(self, context: Context) -> None:
        """Call ``py_teardown`` function if declared.

        Runs ALWAYS after execute -- success, failure, or timeout.
        Uses the same uv isolation path when PEP 723 metadata is present.
        """
        if not self.py_teardown:
            return

        template_context = self._build_template_context(context)
        try:
            rendered_code, source_name = self._render_code(template_context)
        except Exception as exc:
            logger.warning("Teardown skipped (could not render code): %s", exc)
            return

        runtime_ctx = self._build_runtime_ctx(context)
        _set_runtime_context(runtime_ctx)

        try:
            pep723_block = _extract_pep723_block(rendered_code)
            if pep723_block is not None:
                await _run_uv_isolated(
                    rendered_code=rendered_code,
                    pep723_block=pep723_block,
                    function=self.py_teardown,
                    params=self.params,
                    env=self.env,
                    source_name=source_name,
                )
            else:
                module = self._import_code(rendered_code)
                func = getattr(module, self.py_teardown, None)
                if func is None:
                    logger.warning(
                        "Teardown function %r not found in %s",
                        self.py_teardown,
                        source_name,
                    )
                    return
                logger.info(
                    "Running teardown %s:%s", source_name, self.py_teardown
                )
                result = func(**self.params)
                if asyncio.iscoroutine(result):
                    await result
        finally:
            _clear_runtime_context()


# ---------------------------------------------------------------------------
# uv-isolated execution (PEP 723)
# ---------------------------------------------------------------------------


async def _run_uv_isolated(
    *,
    rendered_code: str,
    pep723_block: str,
    function: str,
    params: dict[str, Any],
    env: dict[str, str],
    source_name: str,
) -> dict[str, Any] | None:
    """Run a PEP 723 script via ``uv run`` in an isolated virtual environment.

    Writes the rendered code and a thin wrapper to temp files, then runs::

        uv run /tmp/beacon_uv_<uuid>/beacon_wrapper.py '<json_params>'

    The wrapper calls ``function(**params)`` and prints the result as JSON to
    stdout. Beacon reads stdout and returns it as the task output dict.

    The user's script only needs a PEP 723 header and a ``main()`` function --
    no ``if __name__ == '__main__'`` boilerplate required.

    Args:
        rendered_code: Fully Jinja-rendered script content (includes PEP 723 block).
        pep723_block:  The extracted ``# /// script ... # ///`` block.
        function:      Name of the function to call.
        params:        Keyword arguments to pass (JSON-serialized for subprocess).
        env:           Extra environment variables for the subprocess.
        source_name:   Human-readable source label for log messages.

    Returns:
        Parsed JSON dict from stdout, or ``None`` if the function returned nothing.
    """
    with tempfile.TemporaryDirectory(prefix="beacon_uv_") as tmpdir:
        tmp = Path(tmpdir)

        # Write the rendered user module so the wrapper can import it.
        user_module = tmp / "user_module.py"
        user_module.write_text(rendered_code)

        try:
            params_json = json.dumps(params, default=str)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Cannot JSON-serialize params for uv execution: {exc}"
            ) from exc

        wrapper_code = _UV_WRAPPER_TMPL.format(
            pep723_block=pep723_block,
            script_path=str(user_module),
            function=function,
        )
        wrapper = tmp / "beacon_wrapper.py"
        wrapper.write_text(wrapper_code)

        subprocess_env = {**os.environ, **env}

        logger.debug(
            "uv run wrapper for %s:%s params_keys=%s",
            source_name,
            function,
            list(params.keys()),
        )

        proc = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            str(wrapper),
            params_json,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=subprocess_env,
        )
        stdout_bytes, stderr_bytes = await proc.communicate()

        if stderr_bytes:
            for line in stderr_bytes.decode(errors="replace").splitlines():
                logger.debug("[uv] %s", line)

        if proc.returncode != 0:
            stderr_text = stderr_bytes.decode(errors="replace").strip()
            raise RuntimeError(
                f"uv run failed for {source_name}:{function} "
                f"(exit {proc.returncode}).\n{stderr_text}"
            )

        stdout_text = stdout_bytes.decode(errors="replace").strip()
        if not stdout_text:
            return None

        try:
            result = json.loads(stdout_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Expected JSON from {source_name}:{function}, "
                f"got: {stdout_text!r}"
            ) from exc

        return result if isinstance(result, dict) else None
