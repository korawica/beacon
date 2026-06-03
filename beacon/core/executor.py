"""Executors.

An executor is responsible for running a task instance in a specific
environment. It receives a TaskContext (from metadata store), executes the
plugin, and writes the result back to the metadata store.

The executor abstraction allows beacon to run tasks locally (for dev),
in Docker containers, Kubernetes pods, AWS Batch, or Cloud Batch — all with
the same task lifecycle.

Flow:
    Scheduler enqueues TaskContext → Queue → Executor picks up
    Executor: read TaskContext → start_attempt → run plugin → finish_attempt
    Executor: write updated TaskContext back to metadata store
"""

import asyncio
import logging
import traceback
from abc import ABC, abstractmethod

from .context import Context
from .plugin import PLUGINS_REGISTRY, BasePlugin
from .task_context import AttemptStatus, TaskContext
from ..errors import TaskFailed, TaskSkipped

logger = logging.getLogger("beacon.executor")


class BaseExecutor(ABC):
    """Base Executor.

    All executors implement `run_task` which:
      1. Reads the TaskContext
      2. Resolves the plugin
      3. Calls plugin.execute()
      4. Updates the TaskContext with the result
    """

    executor_type: str = "base"

    @abstractmethod
    async def run_task(self, task_ctx: TaskContext) -> TaskContext:
        """Execute a task and return the updated TaskContext.

        This is the method that remote executors override to run in their
        specific environment (subprocess, container, pod, etc.).
        """
        raise NotImplementedError

    def _resolve_plugin(self, plugin_name: str) -> type[BasePlugin]:
        """Resolve plugin class from registry."""
        if plugin_name not in PLUGINS_REGISTRY:
            raise NotImplementedError(
                f"Plugin {plugin_name!r} not found in registry."
            )
        return PLUGINS_REGISTRY[plugin_name]


class LocalExecutor(BaseExecutor):
    """Local Executor.

    Runs tasks in the current async event loop. Used for development
    and single-machine deployments.
    """

    executor_type: str = "local"

    async def run_task(self, task_ctx: TaskContext) -> TaskContext:
        """Execute task in the local process."""
        plugin_cls = self._resolve_plugin(task_ctx.plugin_name)

        # Instantiate plugin with task inputs (already fully rendered by the
        # scheduler — plugins never see Jinja).
        plugin_instance = plugin_cls.model_validate(task_ctx.inputs)

        # Start attempt tracking first so attempt_number is correct in Context
        task_ctx.start_attempt(
            executor=self.executor_type,
            executor_ref=None,
        )

        # Build lightweight Context for the plugin
        context: Context = {
            "run_id": task_ctx.run_id,
            "dag_id": task_ctx.dag_id,
            "task_id": task_ctx.task_id,
            "run_date": task_ctx.run_date,
            "logical_date": task_ctx.logical_date,
            "data_interval_start": task_ctx.data_interval_start,
            "data_interval_end": task_ctx.data_interval_end,
            "params": task_ctx.params,
            "attempt_number": task_ctx.attempt_number,
            "upstream_outputs": task_ctx.upstream_outputs,
        }

        try:
            # Run with optional timeout
            if task_ctx.execution_timeout:
                async with asyncio.timeout(task_ctx.execution_timeout):
                    result = await plugin_instance.execute(context)
            else:
                result = await plugin_instance.execute(context)

            # Success
            task_ctx.finish_attempt(
                state=AttemptStatus.SUCCESS,
                outputs=result if isinstance(result, dict) else {},
            )

        except TimeoutError:
            task_ctx.finish_attempt(
                state=AttemptStatus.TIMED_OUT,
                error=f"Execution timed out after {task_ctx.execution_timeout}s",
            )

        except TaskSkipped as exc:
            # Plugin determined task should be skipped — no retry
            task_ctx.finish_attempt(
                state=AttemptStatus.SKIPPED,
                error=str(exc) if str(exc) else None,
            )
            task_ctx.retries = 0

        except TaskFailed as exc:
            # Permanent failure — exhaust retries so worker won't retry
            task_ctx.finish_attempt(
                state=AttemptStatus.FAILED,
                error=str(exc),
                error_traceback=traceback.format_exc(),
            )
            task_ctx.retries = 0

        except Exception as exc:
            task_ctx.finish_attempt(
                state=AttemptStatus.FAILED,
                error=str(exc),
                error_traceback=traceback.format_exc(),
            )

        return task_ctx
