"""Python Plugin.

Executes a user-defined Python function from a file.

Usage (YAML):
    tasks:
      - id: process
        type: task
        uses: py
        inputs:
          py_file: ./my_script.py
          py_function: main          # optional, defaults to "main"
          params:
            source_system: "{{ params.source_system }}"

The user's function is called with `params` as keyword arguments.
Inside the function, `load_context()` provides access to logger, run_id, etc.

User file example:
    from beacon.runtime import load_context

    def main(source_system: str):
        ctx = load_context()
        ctx.logger.info("Processing %s", source_system)
        return {"rows_processed": 100}
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

    Executes a Python function from a file. The function receives `params`
    as keyword arguments and can access runtime context via `load_context()`.

    !!! example

        ```yaml
        tasks:
          - id: example
            type: task
            uses: py
            inputs:
              py_file: ./example.py
              py_function: main
              params:
                source_system: my_source
        ```
    """

    plugin_name: ClassVar[str] = "py"

    py_file: str = Field(description="Path to the Python file to execute")
    py_function: str = Field(
        default="main",
        description="Function name to call in the Python file",
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
            1. Set environment variables
            2. Set runtime context (available via load_context())
            3. Import the Python file as a module
            4. Call the target function with params as kwargs
            5. Return the result (if dict) as task outputs
        """
        # Resolve file path via the bundle-aware asset lookup
        # (local dag assets first, then bundle-global assets, else raise).
        py_path = resolve_asset(self.py_file)

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
            # Import the Python file as a module
            module = self._import_file(py_path)

            # Get the target function
            func = getattr(module, self.py_function, None)
            if func is None:
                raise AttributeError(
                    f"Function {self.py_function!r} not found in {py_path.name}"
                )
            if not callable(func):
                raise TypeError(
                    f"{self.py_function!r} in {py_path.name} is not callable"
                )

            # Call the function with params as kwargs
            logger.info(
                "Executing %s:%s with params=%s",
                py_path.name,
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
    def _import_file(path: Path):
        """Import a Python file as a module without polluting sys.modules."""
        module_name = f"_beacon_user_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load module from {path}")

        # Add parent dir to sys.path so relative imports in user file work
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

    async def teardown(self, context: Context) -> None:
        """Call ``py_teardown`` function if declared.

        Runs ALWAYS after execute — success, failure, or timeout.
        Has access to the same ``load_context()`` runtime context.
        """
        if not self.py_teardown:
            return

        try:
            py_path = resolve_asset(self.py_file)
        except FileNotFoundError as exc:
            logger.warning("Teardown skipped: %s", exc)
            return

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
            module = self._import_file(py_path)
            func = getattr(module, self.py_teardown, None)
            if func is None:
                logger.warning(
                    "Teardown function %r not found in %s",
                    self.py_teardown,
                    py_path.name,
                )
                return
            logger.info(
                "Running teardown %s:%s", py_path.name, self.py_teardown
            )
            result = func(**self.params)
            if asyncio.iscoroutine(result):
                await result
        finally:
            _clear_runtime_context()
