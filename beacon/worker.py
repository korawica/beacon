"""Async Worker.

The worker dequeues task messages, dispatches them to the executor,
manages state transitions, retries, and callbacks.

Usage:
    from beacon.metadata import JsonMetadata  # or SqliteMetadata, etc.

    metadata = JsonMetadata("./metadata.db")
    worker = Worker(metadata)

    # Submit tasks
    await worker.submit(task_ctx, action)

    # Run worker (blocks until shutdown)
    await worker.run()
"""

import asyncio
import logging
from dataclasses import dataclass, field

from .callback import OnTaskEvent
from .core.context import MetadataProtocol
from .core.executor import BaseExecutor, LocalExecutor
from .core.state import TaskState
from .core.task_context import AttemptStatus, TaskContext

logger = logging.getLogger("beacon.worker")


@dataclass
class _TaskMessage:
    """Internal message queued for execution."""

    task_ctx: TaskContext
    callbacks: list[OnTaskEvent] = field(default_factory=list)
    upstream_task_ids: list[str] = field(default_factory=list)


class Worker:
    """Async worker that processes task queue with retry and callbacks."""

    def __init__(
        self,
        metadata: MetadataProtocol,
        executor: BaseExecutor | None = None,
        max_concurrent: int = 10,
    ) -> None:
        self.metadata = metadata
        self.executor = executor or LocalExecutor()
        self._queue: asyncio.Queue[_TaskMessage | None] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._running = False
        self._tasks: set[asyncio.Task] = set()

    async def submit(
        self,
        task_ctx: TaskContext,
        callbacks: list[OnTaskEvent] | None = None,
        upstream_task_ids: list[str] | None = None,
    ) -> None:
        """Submit a task for execution.

        Args:
            task_ctx: The task context to execute.
            callbacks: Callbacks to fire on events.
            upstream_task_ids: Task IDs whose outputs should be available
                via {{ outputs.task_id.key }} in downstream inputs.
        """
        # Persist initial state
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
        )
        await self._queue.put(msg)
        logger.info("Queued task %s/%s", task_ctx.dag_id, task_ctx.task_id)

    async def run(self) -> None:
        """Main worker loop. Blocks until shutdown() is called."""
        self._running = True
        logger.info(
            "Worker started (max_concurrent=%d)", self._semaphore._value
        )

        while self._running:
            try:
                msg = await asyncio.wait_for(self._queue.get(), timeout=0.5)
            except TimeoutError:
                continue

            if msg is None:  # Shutdown signal
                break

            task = asyncio.create_task(self._process(msg))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        # Wait for in-flight tasks
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Worker stopped")

    async def shutdown(self) -> None:
        """Signal worker to stop after current tasks complete."""
        self._running = False
        await self._queue.put(None)

    async def _process(self, msg: _TaskMessage) -> None:
        """Process a single task message."""
        async with self._semaphore:
            task_ctx = msg.task_ctx
            run_id = task_ctx.run_id
            dag_id = task_ctx.dag_id
            task_id = task_ctx.task_id

            # --- Resolve upstream outputs ---
            if msg.upstream_task_ids:
                await self._resolve_upstream_outputs(
                    task_ctx, msg.upstream_task_ids
                )

            # --- RUNNING ---
            await self.metadata.set_task_state(
                run_id, dag_id, task_id, TaskState.RUNNING
            )
            await self._fire(msg.callbacks, "start", task_ctx)

            # --- Execute ---
            task_ctx = await self.executor.run_task(task_ctx)

            # --- Persist updated context ---
            await self.metadata.put_task_context(
                run_id, dag_id, task_id, task_ctx
            )

            # --- Evaluate ---
            last = task_ctx.last_attempt
            if last and last.state == AttemptStatus.SUCCESS:
                await self.metadata.set_task_state(
                    run_id, dag_id, task_id, TaskState.SUCCESS
                )
                await self._fire(msg.callbacks, "success", task_ctx)
                logger.info("Task %s/%s succeeded", dag_id, task_id)
                return

            if last and last.state == AttemptStatus.SKIPPED:
                await self.metadata.set_task_state(
                    run_id, dag_id, task_id, TaskState.SKIPPED
                )
                await self._fire(msg.callbacks, "skipped", task_ctx)
                logger.info("Task %s/%s skipped", dag_id, task_id)
                return

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
                # Schedule retry OUTSIDE semaphore to not block a slot
                retry_task = asyncio.create_task(
                    self._schedule_retry(msg, delay)
                )
                self._tasks.add(retry_task)
                retry_task.add_done_callback(self._tasks.discard)
                return

            # No retries left — FAILED
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
            if cb.on_event == event:
                try:
                    await cb.notify(task_ctx, event)
                except Exception as exc:
                    logger.error("Callback error (%s): %s", event, exc)

    async def _resolve_upstream_outputs(
        self, task_ctx: TaskContext, upstream_ids: list[str]
    ) -> None:
        """Load outputs from upstream tasks into task_ctx.upstream_outputs."""
        for uid in upstream_ids:
            upstream_ctx = await self.metadata.get_task_context(
                task_ctx.run_id, task_ctx.dag_id, uid
            )
            if upstream_ctx and upstream_ctx.outputs:
                task_ctx.upstream_outputs[uid] = upstream_ctx.outputs
