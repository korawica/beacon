"""Python Plugin.

Executes a user-defined Python function from a file or inline code string.

Usage (YAML):
    tasks:
      - id: process
        type: task
        uses: py
        inputs:
          py_statement: ./my_script.py    # File path (ends with .py)
          py_function: main               # optional, defaults to "main"
          params:
            source_system: "{{ params.source_system }}"

      - id: inline
        type: task
        uses: py
        inputs:
          py_statement: |
            def main():
                return {"status": "done"}
          py_function: main

The user's function is called with `params` as keyword arguments.
Inside the function, `load_context()` provides access to logger, run_id, etc.

User file example:
    from beacon.runtime import load_context

    def main(source_system: str):
        ctx = load_context()
        ctx.logger.info("Processing %s", source_system)
        return {"rows_processed": 100}

Template rendering in files:
    When py_statement ends with .py, the file contents are rendered with Jinja.
    Use {{ params.x }} to inject values, or {{{{ params.x }}}} for literal braces.

    # transform.py
    def main():
        source = "{{ params.source }}"  # Rendered to actual value
        return {"source": source}
"""

import asyncio
import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import Any, ClassVar, TYPE_CHECKING

from pydantic import Field

from .....core import BasePlugin
from .....core.assets import resolve_asset
from .....runtime import (
    RuntimeContext,
    _clear_runtime_context,
    _set_runtime_context,
)

if TYPE_CHECKING:
    from .....core import Context

logger = logging.getLogger("beacon.plugin.py")


class PythonPlugin(BasePlugin):
    """Python Plugin.

    Executes a Python function from a file or inline code string. The function
    receives `params` as keyword arguments and can access runtime context via
    `load_context()`.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: task
            uses: py
            inputs:
              py_statement: ./example.py
              py_function: main
              params:
                source_system: my_source
        ```
    """

    plugin_name: ClassVar[str] = "py"
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
            2. If file: load, render with Jinja, and prepare for execution
            3. If inline: render with Jinja and prepare for execution
            4. Set environment variables
            5. Set runtime context (available via load_context())
            6. Execute the code
            7. Call the target function with params as kwargs
            8. Return the result (if dict) as task outputs
        """
        # Determine if we have a file path or inline code
        is_file = self.py_statement.strip().endswith(".py")

        if is_file:
            # File path: resolve via bundle-aware asset lookup
            py_path = resolve_asset(self.py_statement)
            code = py_path.read_text()
        else:
            # Inline code: use as-is (no file resolution)
            code = self.py_statement
            py_path = None

        # Render the code with Jinja (for both file and inline cases)
        rendered_code = self._render_code(code, context)

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
            # Execute the code and get the module
            if is_file and py_path:
                module = self._import_file(py_path, rendered_code)
                source_name = py_path.name
            else:
                module = self._import_code(rendered_code)
                source_name = "<inline>"

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
    def _render_code(code: str, context: Context) -> str:
        """Render code string with Jinja.

        Uses the same Renderer as the scheduler but for code contents.
        Supports both file contents and inline code.

        Args:
            code: Python code string with optional Jinja templates
            context: Runtime context with params, vars, outputs, runtime

        Returns:
            Rendered code string with Jinja templates resolved
        """
        from beacon.core.renderer import Renderer

        renderer = Renderer(
            {
                "params": context.get("params", {}),
                "vars": lambda n: context.get("vars", {}).get(
                    n, f"<unresolved: vars('{n}')>"
                ),
                "runtime": context.get("runtime", {}),
                "outputs": context.get("upstream_outputs", {}),
            }
        )
        return renderer.render(code)

    @staticmethod
    def _import_file(path: Path, rendered_code: str | None = None):
        """Import a Python file as a module without polluting sys.modules.

        Args:
            path: Path to the Python file
            rendered_code: If provided, use this rendered code instead of
                reading from file. Useful when code was pre-rendered with Jinja.
        """
        module_name = f"_beacon_user_{path.stem}"

        if rendered_code is not None:
            # Use rendered code directly via exec
            module = type(sys)(module_name)
            module.__file__ = str(path)
            module.__dict__["__name__"] = module_name

            # Add parent dir to sys.path so relative imports work
            parent = str(path.parent)
            added_to_path = parent not in sys.path
            if added_to_path:
                sys.path.insert(0, parent)

            try:
                exec(rendered_code, module.__dict__)
                return module
            finally:
                if added_to_path:
                    sys.path.remove(parent)
        else:
            # Original behavior: load from file
            spec = importlib.util.spec_from_file_location(module_name, path)
            if spec is None or spec.loader is None:
                raise ImportError(f"Cannot load module from {path}")

            parent = str(path.parent)
            added_to_path = parent not in sys.path
            if added_to_path:
                sys.path.insert(0, parent)

            try:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return module
            finally:
                if added_to_path:
                    sys.path.remove(parent)

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

        # Determine if we have a file path or inline code
        is_file = self.py_statement.strip().endswith(".py")

        if is_file:
            try:
                py_path = resolve_asset(self.py_statement)
            except FileNotFoundError as exc:
                logger.warning("Teardown skipped: %s", exc)
                return
            code = py_path.read_text()
        else:
            code = self.py_statement
            py_path = None

        # Render the code with Jinja
        rendered_code = self._render_code(code, context)

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
            if is_file and py_path:
                module = self._import_file(py_path, rendered_code)
            else:
                module = self._import_code(rendered_code)

            func = getattr(module, self.py_teardown, None)
            if func is None:
                source_name = py_path.name if py_path else "<inline>"
                logger.warning(
                    "Teardown function %r not found in %s",
                    self.py_teardown,
                    source_name,
                )
                return
            source_name = py_path.name if py_path else "<inline>"
            logger.info("Running teardown %s:%s", source_name, self.py_teardown)
            result = func(**self.params)
            if asyncio.iscoroutine(result):
                await result
        finally:
            _clear_runtime_context()
