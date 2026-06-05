"""Python Plugin.

Executes a user-defined Python function from a file or inline code string.

Usage (YAML):
    tasks:
      - id: process
        type: task
        uses: py
        inputs:
          py_statement: scripts/transform.py    # Searched in assets/ directories
          py_function: main                     # optional, defaults to "main"

      - id: inline
        type: task
        uses: py
        inputs:
          py_statement: |
            def main():
                return {"status": "done"}
          py_function: main

Template search path:
    Files ending in .py are searched in:
      1. <dag_folder>/assets/  (DAG-local assets, higher priority)
      2. <bundle_root>/assets/ (bundle-global assets)

    The file path should NOT include "assets/" prefix:
      ✅ py_statement: scripts/transform.py
      ❌ py_statement: assets/scripts/transform.py
      ❌ py_statement: ./scripts/transform.py

Template features:
    Files are rendered with full Jinja support including:
      - {{ params.x }}          variable interpolation
      - {% if condition %}...{% endif %}   conditionals
      - {% for item in list %}...{% endfor %}  loops
      - {% extends "base.py" %} template inheritance
      - {% include "partials/header.py" %}  includes
      - {% raw %}{{ literal }}{% endraw %}  escape Jinja

    Use {% raw %}...{% endraw %} for literal braces in Python code:
      # In transform.py
      def main():
          template = "{% raw %}{{ params.source }}{% endraw %}"  # Literal braces
          return {"template": template}
"""

import asyncio
import logging
import os
import sys
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


class PythonPlugin(BasePlugin, plugin_name="py"):
    template_ext: ClassVar[tuple[str, ...]] = (".py",)

    py_statement: str = Field(
        description="Path to Python file (ending in .py) or inline Python code string"
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

        Steps:
            1. Determine if py_statement is a file path or inline code
            2. If file: render with Jinja FileSystemLoader (supports extends/include)
            3. If inline: render as Jinja string
            4. Set environment variables
            5. Set runtime context (available via load_context())
            6. Execute the code
            7. Call the target function with params as kwargs
            8. Return the result (if dict) as task outputs
        """
        # Build template context
        template_context = {
            "params": context.get("params", {}),
            "vars": lambda n: context.get("vars", {}).get(
                n, f"<unresolved: vars('{n}')>"
            ),
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

        # Determine if we have a file path or inline code
        stripped = self.py_statement.strip()
        is_file = stripped.endswith(".py")
        is_absolute = stripped.startswith("/") or (
            len(stripped) > 1 and stripped[1] == ":"
        )

        if is_file:
            if is_absolute:
                # Absolute path: read file directly and render as string
                # (for backwards compatibility and special cases)
                from pathlib import Path

                py_path = Path(stripped)
                if not py_path.exists():
                    raise FileNotFoundError(f"File not found: {py_path}")
                code = py_path.read_text()
                rendered_code = render_template_string(code, template_context)
            else:
                # Relative path: use FileSystemLoader for full Jinja support
                # Path is searched in assets/ directories (no "assets/" prefix needed)
                template_name = stripped
                rendered_code = render_template_file(
                    template_name, template_context
                )
        else:
            # Inline code: render as string
            rendered_code = render_template_string(
                self.py_statement, template_context
            )

        # Set environment variables
        original_env: dict[str, str | None] = {}
        for key, value in self.env.items():
            original_env[key] = os.environ.get(key)
            os.environ[key] = value

        # Build and set runtime context
        task_logger = logging.getLogger(
            f"beacon.task.{context.get('dag_id', '')}.{context.get('task_id', '')}"
        )
        runtime_ctx = RuntimeContext(
            run_id=context.get("run_id", ""),
            dag_id=context.get("dag_id", ""),
            task_id=context.get("task_id", ""),
            attempt_number=context.get("attempt_number", 1),
            params=self.params,
            upstream_outputs=context.get("upstream_outputs", {}),
            run_date=context.get("run_date"),
            logical_date=context.get("logical_date"),
            data_interval_start=context.get("data_interval_start"),
            data_interval_end=context.get("data_interval_end"),
            logger=task_logger,
        )
        _set_runtime_context(runtime_ctx)

        try:
            # Execute the rendered code
            module = self._import_code(rendered_code)
            source_name = self.py_statement if is_file else "<inline>"

            # Get the target function
            func = getattr(module, self.py_function, None)
            if func is None:
                raise AttributeError(
                    f"Function {self.py_function!r} not found in {source_name}"
                )
            if not callable(func):
                raise TypeError(
                    f"{self.py_function!r} in {source_name} is not callable"
                )

            # Call the function with params as kwargs
            logger.info(
                "Executing %s:%s with params=%s",
                source_name,
                self.py_function,
                list(self.params.keys()),
            )
            result = func(**self.params)

            # Handle async functions
            if asyncio.iscoroutine(result):
                result = await result

            return result if isinstance(result, dict) else None

        finally:
            # Restore environment
            for key, original in original_env.items():
                if original is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = original

            # Clear runtime context
            _clear_runtime_context()

    @staticmethod
    def _import_code(code: str):
        """Import inline Python code as a module.

        Args:
            code: Python code string (already rendered with Jinja if needed)
        """
        import uuid

        module_name = f"_beacon_inline_{uuid.uuid4().hex[:8]}"

        module = type(sys)(module_name)
        module.__file__ = "<inline>"
        module.__dict__["__name__"] = module_name

        exec(code, module.__dict__)
        return module

    async def teardown(self, context: Context) -> None:
        """Call ``py_teardown`` function if declared.

        Runs ALWAYS after execute — success, failure, or timeout.
        Has access to the same ``load_context()`` runtime context.
        """
        if not self.py_teardown:
            return

        # Build template context
        template_context = {
            "params": context.get("params", {}),
            "vars": lambda n: context.get("vars", {}).get(
                n, f"<unresolved: vars('{n}')>"
            ),
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

        # Determine if we have a file path or inline code
        stripped = self.py_statement.strip()
        is_file = stripped.endswith(".py")
        is_absolute = stripped.startswith("/") or (
            len(stripped) > 1 and stripped[1] == ":"
        )

        if is_file:
            try:
                if is_absolute:
                    # Absolute path: read file directly
                    from pathlib import Path

                    py_path = Path(stripped)
                    if not py_path.exists():
                        raise FileNotFoundError(f"File not found: {py_path}")
                    code = py_path.read_text()
                    rendered_code = render_template_string(
                        code, template_context
                    )
                else:
                    template_name = stripped
                    rendered_code = render_template_file(
                        template_name, template_context
                    )
            except Exception as exc:
                logger.warning("Teardown skipped: %s", exc)
                return
        else:
            rendered_code = render_template_string(
                self.py_statement, template_context
            )

        # Re-set runtime context so teardown function can use load_context()
        task_logger = logging.getLogger(
            f"beacon.task.{context.get('dag_id', '')}.{context.get('task_id', '')}"
        )
        runtime_ctx = RuntimeContext(
            run_id=context.get("run_id", ""),
            dag_id=context.get("dag_id", ""),
            task_id=context.get("task_id", ""),
            attempt_number=context.get("attempt_number", 1),
            params=self.params,
            upstream_outputs=context.get("upstream_outputs", {}),
            run_date=context.get("run_date"),
            logical_date=context.get("logical_date"),
            data_interval_start=context.get("data_interval_start"),
            data_interval_end=context.get("data_interval_end"),
            logger=task_logger,
        )
        _set_runtime_context(runtime_ctx)

        try:
            module = self._import_code(rendered_code)

            func = getattr(module, self.py_teardown, None)
            if func is None:
                source_name = self.py_statement if is_file else "<inline>"
                logger.warning(
                    "Teardown function %r not found in %s",
                    self.py_teardown,
                    source_name,
                )
                return
            source_name = self.py_statement if is_file else "<inline>"
            logger.info("Running teardown %s:%s", source_name, self.py_teardown)
            result = func(**self.params)
            if asyncio.iscoroutine(result):
                await result
        finally:
            _clear_runtime_context()
