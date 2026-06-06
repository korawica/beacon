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
from contextlib import nullcontext

from .context import Context
from .plugin import PLUGINS_REGISTRY, BasePlugin
from .task_context import AttemptStatus, TaskContext
from ..errors import TaskFailed, TaskRetry, TaskSkipped
from ..logging import (
    capture_stdout_stderr,
    should_capture_stdout,
    task_log_context,
)

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
        """Resolve plugin class from local registry.

        Remote refs (``org/repo@version``) are handled separately in
        :meth:`LocalExecutor.run_task` via :func:`run_remote_plugin` and
        never reach this method.
        """
        if plugin_name in PLUGINS_REGISTRY:
            return PLUGINS_REGISTRY[plugin_name]

        raise NotImplementedError(
            f"Plugin {plugin_name!r} not found in registry."
        )


class LocalExecutor(BaseExecutor):
    """Local Executor.

    Runs tasks in the current async event loop. Used for development
    and single-machine deployments.
    """

    executor_type: str = "local"

    async def run_task(self, task_ctx: TaskContext) -> TaskContext:
        """Execute task in the local process.

        ANY exception that surfaces from plugin lookup, model validation,
        or ``plugin.execute()`` is captured and recorded as a failed
        attempt on ``task_ctx``. ``run_task`` never re-raises — the worker
        relies on the returned ``task_ctx`` to drive state transitions.
        """
        # Start attempt FIRST so the caller (worker) always has an attempt
        # to inspect — even when plugin resolution or model validation blows
        # up. Without this, a missing plugin would leave the worker with no
        # attempt and no terminal state to react to (i.e., DAG hang).
        task_ctx.start_attempt(
            executor=self.executor_type,
            executor_ref=None,
        )

        task_logger = logging.getLogger(
            f"beacon.task.{task_ctx.dag_id}.{task_ctx.task_id}"
        )

        plugin_instance: BasePlugin | None = None
        context: Context = {
            "run_id": task_ctx.run_id,
            "dag_id": task_ctx.dag_id,
            "task_id": task_ctx.task_id,
            "run_date": task_ctx.run_date,
            "logical_date": task_ctx.logical_date,
            "data_interval_start": task_ctx.data_interval_start,
            "data_interval_end": task_ctx.data_interval_end,
            "variables": task_ctx.variables,
            "attempt_number": task_ctx.attempt_number,
            "upstream_outputs": task_ctx.upstream_outputs,
            "logger": task_logger,
        }

        stdout_cm = (
            capture_stdout_stderr(task_logger)
            if should_capture_stdout()
            else nullcontext()
        )

        from .remote_plugin import (
            EXIT_SUCCESS,
            EXIT_TASK_FAILED,
            EXIT_TASK_RETRY,
            EXIT_TASK_SKIPPED,
            is_remote_ref,
            run_remote_plugin,
        )

        # --- Remote plugin: run in isolated uv env, communicate via exit code ---
        if is_remote_ref(task_ctx.plugin_name):
            try:
                with task_log_context(
                    task_ctx.dag_id,
                    task_ctx.run_id,
                    task_ctx.task_id,
                    task_ctx.attempt_number,
                ):
                    if task_ctx.execution_timeout:
                        async with asyncio.timeout(task_ctx.execution_timeout):
                            result, exit_code = await run_remote_plugin(
                                task_ctx.plugin_name,
                                task_ctx.inputs,
                                context,  # type: ignore[arg-type]
                            )
                    else:
                        result, exit_code = await run_remote_plugin(
                            task_ctx.plugin_name,
                            task_ctx.inputs,
                            context,  # type: ignore[arg-type]
                        )

                if exit_code == EXIT_SUCCESS:
                    task_ctx.finish_attempt(
                        state=AttemptStatus.SUCCESS,
                        outputs=result or {},
                    )
                elif exit_code == EXIT_TASK_SKIPPED:
                    task_ctx.finish_attempt(state=AttemptStatus.SKIPPED)
                    task_ctx.retries = 0
                elif exit_code == EXIT_TASK_FAILED:
                    task_ctx.finish_attempt(
                        state=AttemptStatus.FAILED,
                        error=f"Remote plugin {task_ctx.plugin_name!r} exited with TASK_FAILED",
                    )
                    task_ctx.retries = 0
                elif exit_code == EXIT_TASK_RETRY:
                    task_ctx.finish_attempt(
                        state=AttemptStatus.FAILED,
                        error=f"Remote plugin {task_ctx.plugin_name!r} requested retry",
                    )
                    # Do NOT exhaust retries — let worker retry naturally.
                else:
                    # EXIT_FAILURE or any unexpected non-zero exit
                    task_ctx.finish_attempt(
                        state=AttemptStatus.FAILED,
                        error=(
                            f"Remote plugin {task_ctx.plugin_name!r} "
                            f"exited with code {exit_code}"
                        ),
                    )

            except TimeoutError:
                task_ctx.finish_attempt(
                    state=AttemptStatus.TIMED_OUT,
                    error=f"Execution timed out after {task_ctx.execution_timeout}s",
                )
            except Exception as exc:
                task_ctx.finish_attempt(
                    state=AttemptStatus.FAILED,
                    error=str(exc),
                    error_traceback=traceback.format_exc(),
                )

            return task_ctx

        # --- Local in-process plugin ---
        try:
            plugin_cls = self._resolve_plugin(task_ctx.plugin_name)
            plugin_instance = plugin_cls.model_validate(task_ctx.inputs)

            with (
                task_log_context(
                    task_ctx.dag_id,
                    task_ctx.run_id,
                    task_ctx.task_id,
                    task_ctx.attempt_number,
                ),
                stdout_cm,
            ):
                if task_ctx.execution_timeout:
                    async with asyncio.timeout(task_ctx.execution_timeout):
                        result = await plugin_instance.execute(context)
                else:
                    result = await plugin_instance.execute(context)

            task_ctx.finish_attempt(
                state=AttemptStatus.SUCCESS,
                outputs=(
                    result
                    if isinstance(result, dict)
                    else {"_result": result}
                    if result is not None
                    else {}
                ),
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

        except TaskRetry as exc:
            # Plugin explicitly requests a retry — behaves like a normal
            # failure so the worker's retry logic applies naturally.
            task_ctx.finish_attempt(
                state=AttemptStatus.FAILED,
                error=str(exc) if str(exc) else "TaskRetry: retry requested",
                error_traceback=traceback.format_exc(),
            )
            # Note: do NOT exhaust retries — worker decides whether to retry.

        except NotImplementedError as exc:
            # Plugin not registered: a permanent failure, no point retrying.
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

        finally:
            # Teardown only if the plugin was successfully instantiated.
            if plugin_instance is not None:
                try:
                    await plugin_instance.teardown(context)
                except Exception as td_exc:  # noqa: BLE001
                    logger.warning(
                        "Plugin teardown error for %s/%s: %s",
                        task_ctx.dag_id,
                        task_ctx.task_id,
                        td_exc,
                    )

        return task_ctx
