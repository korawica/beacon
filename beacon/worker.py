"""Async Worker.

The worker dequeues task messages, dispatches them to the executor,
manages state transitions, retries, and per-task callbacks.

For full-DAG orchestration (graph traversal, branch/short-circuit
propagation, teardown, DAG callbacks) see :mod:`beacon.runner`.

Usage:
    from beacon.metadata import LocalMetadata
    from beacon.worker import Worker

    meta = LocalMetadata("./metadata.db")
    worker = Worker(meta)
    await worker.submit(task_ctx)
    await worker.run()  # blocks until shutdown()
"""

import asyncio
import logging
from dataclasses import dataclass, field

from .callback import OnTaskEvent
from .core.context import MetadataProtocol, build_runtime_dict
from .core.executor import BaseExecutor, LocalExecutor
from .core.state import TaskState
from .core.task_context import AttemptStatus, TaskContext

logger = logging.getLogger("beacon.worker")

_SHUTDOWN_SENTINEL: object = object()


@dataclass
class _TaskMessage:
    """Internal message queued for execution."""

    task_ctx: TaskContext
    callbacks: list[OnTaskEvent] = field(default_factory=list)
    upstream_task_ids: list[str] = field(default_factory=list)
    on_terminal: object | None = None
    """Optional async callable ``async def(task_ctx, final_state) -> None``.

    Invoked exactly once when the task reaches a terminal state. Used by
    :class:`beacon.runner.DagRunner` to react to task completion
    without polling the metadata store.
    """


class Worker:
    """Async worker that processes a task queue with retry and callbacks."""

    def __init__(
        self,
        metadata: MetadataProtocol,
        executor: BaseExecutor | None = None,
        max_concurrent: int = 100,
    ) -> None:
        self.metadata = metadata
        self.executor = executor or LocalExecutor()
        self.max_concurrent = max_concurrent
        self._queue: asyncio.Queue = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._tasks: set[asyncio.Task] = set()

    async def submit(
        self,
        task_ctx: TaskContext,
        callbacks: list[OnTaskEvent] | None = None,
        upstream_task_ids: list[str] | None = None,
        on_terminal: object | None = None,
    ) -> None:
        """Submit a task for execution.

        Args:
            task_ctx: The task context to execute.
            callbacks: Callbacks to fire on events.
            upstream_task_ids: Task IDs whose outputs should be available
                via ``{{ outputs.task_id.key }}`` in downstream inputs.
            on_terminal: Optional async callable
                ``async def(task_ctx, TaskState) -> None`` invoked once when
                the task reaches a terminal state.
        """
        await self.metadata.put_task_context(
            task_ctx.run_id, task_ctx.dag_id, task_ctx.task_id, task_ctx
        )
        await self.metadata.set_task_state(
            task_ctx.run_id, task_ctx.dag_id, task_ctx.task_id, TaskState.QUEUED
        )

        msg = _TaskMessage(
            task_ctx=task_ctx,
            callbacks=callbacks or [],
            upstream_task_ids=upstream_task_ids or [],
            on_terminal=on_terminal,
        )
        await self._queue.put(msg)
        logger.info("Queued task %s/%s", task_ctx.dag_id, task_ctx.task_id)

    async def run(self) -> None:
        """Main worker loop. Blocks until :meth:`shutdown` is called."""
        self._running = True
        logger.info("Worker started (max_concurrent=%d)", self.max_concurrent)

        while self._running:
            msg = await self._queue.get()
            if msg is _SHUTDOWN_SENTINEL:
                break
            task = asyncio.create_task(self._process(msg))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Worker stopped")

    async def shutdown(self) -> None:
        """Signal worker to stop after current tasks complete."""
        self._running = False
        await self._queue.put(_SHUTDOWN_SENTINEL)

    async def _process(self, msg: _TaskMessage) -> None:
        """Process a single task message."""
        async with self._semaphore:
            task_ctx = msg.task_ctx
            run_id = task_ctx.run_id
            dag_id = task_ctx.dag_id
            task_id = task_ctx.task_id

            if msg.upstream_task_ids:
                await self._resolve_upstream_outputs(
                    task_ctx, msg.upstream_task_ids
                )

            await self.metadata.set_task_state(
                run_id, dag_id, task_id, TaskState.RUNNING
            )
            await self._fire(msg.callbacks, "start", task_ctx)

            # Executors are contract-bound to never raise — every error is
            # captured as a failed attempt on the returned task_ctx. This
            # ``except`` is a defense-in-depth safety net so a buggy
            # third-party executor cannot deadlock the DagRunner by
            # killing the per-task coroutine before on_terminal fires.
            try:
                task_ctx = await self.executor.run_task(task_ctx)
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Executor crashed for %s/%s: %s", dag_id, task_id, exc
                )
                # If the executor crashed AFTER opening an attempt, close it;
                # otherwise synthesize one so the worker's normal "failed →
                # FAILED" path always has something to react to.
                last = task_ctx.last_attempt
                if last is None or last.state == AttemptStatus.RUNNING:
                    if last is None:
                        task_ctx.start_attempt(executor="unknown")
                    task_ctx.finish_attempt(
                        state=AttemptStatus.FAILED,
                        error=f"executor crashed: {exc}",
                    )
                task_ctx.retries = 0
            await self.metadata.put_task_context(
                run_id, dag_id, task_id, task_ctx
            )

            final_state = await self._resolve_final_state(msg, task_ctx)

        if final_state is None:
            return  # task was re-queued for retry — terminal handler runs later

        await self._notify_terminal(msg, task_ctx, final_state)

    async def _resolve_final_state(
        self, msg: _TaskMessage, task_ctx: TaskContext
    ) -> TaskState | None:
        """Apply attempt result → metadata state. Returns terminal state
        or None when the task was re-enqueued for a retry."""
        run_id, dag_id, task_id = (
            task_ctx.run_id,
            task_ctx.dag_id,
            task_ctx.task_id,
        )
        last = task_ctx.last_attempt

        if last and last.state == AttemptStatus.SUCCESS:
            await self.metadata.set_task_state(
                run_id, dag_id, task_id, TaskState.SUCCESS
            )
            await self._fire(msg.callbacks, "success", task_ctx)
            logger.info("Task %s/%s succeeded", dag_id, task_id)
            return TaskState.SUCCESS

        if last and last.state == AttemptStatus.SKIPPED:
            await self.metadata.set_task_state(
                run_id, dag_id, task_id, TaskState.SKIPPED
            )
            await self._fire(msg.callbacks, "skipped", task_ctx)
            logger.info("Task %s/%s skipped", dag_id, task_id)
            return TaskState.SKIPPED

        # Failed — check retries
        if task_ctx.has_retries_left:
            await self.metadata.set_task_state(
                run_id, dag_id, task_id, TaskState.UP_FOR_RETRY
            )
            await self._fire(msg.callbacks, "retry", task_ctx)
            delay = task_ctx.next_retry_delay
            logger.info(
                "Task %s/%s retry in %.1fs (attempt %d/%d)",
                dag_id,
                task_id,
                delay,
                task_ctx.attempt_number,
                task_ctx.retries + 1,
            )
            msg.task_ctx = task_ctx
            retry_task = asyncio.create_task(self._schedule_retry(msg, delay))
            self._tasks.add(retry_task)
            retry_task.add_done_callback(self._tasks.discard)
            return None

        await self.metadata.set_task_state(
            run_id, dag_id, task_id, TaskState.FAILED
        )
        await self._fire(msg.callbacks, "failure", task_ctx)
        logger.error(
            "Task %s/%s failed after %d attempts",
            dag_id,
            task_id,
            task_ctx.attempt_number,
        )
        return TaskState.FAILED

    async def _notify_terminal(
        self,
        msg: _TaskMessage,
        task_ctx: TaskContext,
        final_state: TaskState,
    ) -> None:
        """Fire the per-task on_terminal hook if registered."""
        cb = msg.on_terminal
        if cb is None:
            return
        try:
            await cb(task_ctx, final_state)  # type: ignore[misc]
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "on_terminal callback failed for %s/%s: %s",
                task_ctx.dag_id,
                task_ctx.task_id,
                exc,
            )

    async def _schedule_retry(self, msg: _TaskMessage, delay: float) -> None:
        """Wait for retry delay then re-enqueue. Runs outside semaphore."""
        if delay > 0:
            await asyncio.sleep(delay)
        task_ctx = msg.task_ctx
        await self.metadata.set_task_state(
            task_ctx.run_id, task_ctx.dag_id, task_ctx.task_id, TaskState.QUEUED
        )
        await self._queue.put(msg)

    @staticmethod
    async def _fire(
        callbacks: list[OnTaskEvent], event: str, task_ctx: TaskContext
    ) -> None:
        """Fire callbacks matching the event."""
        for cb in callbacks:
            if cb.on_event != event:
                continue
            try:
                await cb.notify(task_ctx, event)
            except Exception as exc:  # noqa: BLE001
                logger.error("Callback error (%s): %s", event, exc)

    async def _resolve_upstream_outputs(
        self, task_ctx: TaskContext, upstream_ids: list[str]
    ) -> None:
        """Load upstream outputs into ``task_ctx.upstream_outputs`` and
        re-render ``task_ctx.inputs`` so ``{{ outputs.X.Y }}`` resolves to
        concrete values before the plugin sees them."""
        if not upstream_ids:
            return
        # Fast path when the store exposes a Pydantic-skipping outputs reader.
        get_outputs = getattr(self.metadata, "get_task_outputs", None)
        if callable(get_outputs):
            results = await asyncio.gather(
                *(
                    get_outputs(task_ctx.run_id, task_ctx.dag_id, uid)
                    for uid in upstream_ids
                )
            )
            for uid, outputs in zip(upstream_ids, results):
                if outputs:
                    task_ctx.upstream_outputs[uid] = outputs
        else:
            results = await asyncio.gather(
                *(
                    self.metadata.get_task_context(
                        task_ctx.run_id, task_ctx.dag_id, uid
                    )
                    for uid in upstream_ids
                )
            )
            for uid, upstream_ctx in zip(upstream_ids, results):
                if upstream_ctx and upstream_ctx.outputs:
                    task_ctx.upstream_outputs[uid] = upstream_ctx.outputs

        # Late-bind outputs in any remaining Jinja in inputs.
        from .core.renderer import Renderer, make_vars_func, make_secrets_func

        # Re-create vars/secrets functions for late binding
        vars_func = make_vars_func(task_ctx.variables)
        secrets_func = make_secrets_func()

        renderer = Renderer(
            {
                "vars": vars_func,
                "secrets": secrets_func,
                "outputs": task_ctx.upstream_outputs,
                "runtime": build_runtime_dict(
                    run_id=task_ctx.run_id,
                    dag_id=task_ctx.dag_id,
                    task_id=task_ctx.task_id,
                    run_date=task_ctx.run_date,
                    logical_date=task_ctx.logical_date,
                    data_interval_start=task_ctx.data_interval_start,
                    data_interval_end=task_ctx.data_interval_end,
                    attempt_number=task_ctx.attempt_number + 1,
                ),
            }
        )
        try:
            task_ctx.inputs = renderer.render(task_ctx.inputs)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Late-bind render failed for %s/%s: %s",
                task_ctx.dag_id,
                task_ctx.task_id,
                exc,
            )
