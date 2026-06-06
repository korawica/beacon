"""Deployment Scheduler.

A small, single-process async loop that:

  1. **Drains pending manual triggers** from the metadata store
     (written by ``beacon trigger ...``) and fires a DagRun for each.
  2. **Ticks every enabled cron Deployment** and fires the next scheduled
     run when its cron expression is due.

The scheduler shares its metadata store with the CLI — that's the IPC
layer (see ``LocalMetadata.enqueue_trigger`` /
``drain_triggers``). No socket, no API.

Semantics
---------
* **Catch-up scheduling.** When ``Deployment.catch_up=True`` and a
  deployment is created with a past ``start_date``, the scheduler fires
  all missed cron ticks in ASC order (oldest first). Respects
  ``max_active_runs`` to limit concurrent backfill runs.
* **No catch-up (default).** When ``catch_up=False``, only the most
  recent cron tick is scheduled. If the scheduler was down across N
  missed ticks, those earlier ticks are dropped.
* **Max active runs.** ``Deployment.max_active_runs`` limits concurrent
  in-flight runs per deployment. Useful for backfill and high-frequency
  schedules.
* **Global concurrency.** ``BEACON_SCHEDULER_MAX_CONCURRENT_RUNS``
  limits total in-flight runs across all deployments.
* **Bundle is loaded once at startup.** Send ``SIGHUP`` to re-read the
  bundle (or just restart). A future ``beacon sync`` integration can
  swap in new DAG versions live.

Run-id convention (from ``beacon.runner``)::

    manual-{dag_id}-{uuid}            → manual via CLI / API
    backfill-{dag_id}-{timestamp}     → backfill
    scheduled-{dag_id}-{timestamp}    → this scheduler
"""

import asyncio
import logging
import signal
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from croniter import croniter

from .core.bundle import LocalBundle
from .core.variables import VariableScope, merge_with_overrides
from .metadata import LocalMetadata
from .models.dag import Dag
from .runner import DagRunner

logger = logging.getLogger("beacon.scheduler")


def _validate_trigger_variables(
    dep: dict, trigger_vars: dict[str, Any]
) -> list[str]:
    """Validate trigger variables against deployment requirements.

    Returns a list of error messages. Empty list means valid.
    """
    errors: list[str] = []
    requirements = dep.get("variable_requirements", {})
    if not requirements:
        return errors

    deployment_overrides = dep.get("variable_overrides", {})

    for key, spec in requirements.items():
        has_default = spec.get("has_default", False)
        provided = key in trigger_vars or key in deployment_overrides

        if not has_default and not provided:
            errors.append(f"Required variable {key!r} not provided")

    return errors


def _parse_iso(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


class DeploymentScheduler:
    """Cron + manual-trigger loop sharing LocalMetadata with the CLI."""

    def __init__(
        self,
        bundle_path: str | Path,
        meta: LocalMetadata,
        *,
        tick_seconds: int = 5,
        max_concurrent_runs: int = 8,
    ) -> None:
        self.bundle_path = Path(bundle_path).resolve()
        self.meta = meta
        self.tick_seconds = tick_seconds
        self._sem = asyncio.Semaphore(max_concurrent_runs)
        self._stop = asyncio.Event()
        self._reload_requested = False
        self._dags: dict[str, Dag] = {}
        self._variable_scope: VariableScope | None = None
        self._bundle_root: Path | None = None
        # Track active runs: {deployment_id: set(run_id)}
        self._active_runs: dict[str, set[str]] = {}
        self._tasks: set[asyncio.Task[Any]] = set()

    # --- bundle loading ----------------------------------------------------

    def reload(self) -> None:
        """(Re)load every DAG + plugin from the bundle directory."""
        # Local import: scheduler is imported by cli.commands.scheduler_cmd,
        # and cli.loader imports parts of beacon — keep the cycle broken.
        from .cli.loader import _load_dags_from_file

        bundle = LocalBundle(name=self.bundle_path.name, path=self.bundle_path)
        bundle.load_plugins()
        dags: dict[str, Dag] = {}
        for f in bundle.discover_dags():
            for d in _load_dags_from_file(f):
                d._bundle_root = bundle.path
                dags[d.id] = d
        self._dags = dags
        self._variable_scope = bundle.variable_scope
        self._bundle_root = bundle.path
        logger.info(
            "Loaded %d DAG(s) from %s: %s",
            len(dags),
            self.bundle_path,
            sorted(dags),
        )

    # --- main loop ---------------------------------------------------------

    async def run(self) -> None:
        """Tick forever until SIGTERM/SIGINT. Drains in-flight runs on exit."""
        self.reload()
        self._install_signal_handlers()

        # Crash recovery: find and resume orphaned runs
        await self._recover_on_startup()

        logger.info(
            "scheduler started: bundle=%s tick=%ss",
            self.bundle_path,
            self.tick_seconds,
        )
        try:
            while not self._stop.is_set():
                if self._reload_requested:
                    self._reload_requested = False
                    try:
                        self.reload()
                    except Exception as exc:  # noqa: BLE001
                        logger.error("Reload failed: %s", exc)
                try:
                    await self._tick()
                except Exception as exc:  # noqa: BLE001
                    logger.error("Tick failed: %s", exc)
                try:
                    await asyncio.wait_for(
                        self._stop.wait(), timeout=self.tick_seconds
                    )
                except TimeoutError:
                    pass
        finally:
            logger.info(
                "scheduler stopping; awaiting %d in-flight run(s)",
                len(self._tasks),
            )
            if self._tasks:
                await asyncio.gather(*self._tasks, return_exceptions=True)
            logger.info("scheduler stopped")

    async def _recover_on_startup(self) -> None:
        """Find and recover orphaned runs from a previous crash.

        Detects zombie tasks (RUNNING without heartbeat) and resumes
        active runs that were interrupted.
        """
        from .core.recovery import recover_active_runs

        logger.info("Checking for orphaned runs to recover...")

        recoverable = await recover_active_runs(
            meta=self.meta,
            dags=self._dags,
            variables_scope=None,  # variables are per-run, already persisted
        )

        if not recoverable:
            logger.info("No orphaned runs found")
            return

        logger.info("Found %d orphaned run(s) to resume", len(recoverable))

        for run_info in recoverable:
            dag_id = run_info["dag_id"]
            run_id = run_info["run_id"]
            dag = self._dags.get(dag_id)

            if dag is None:
                logger.warning(
                    "Cannot resume run %s: DAG %s not in bundle", run_id, dag_id
                )
                continue

            logger.info(
                "Resuming orphaned run: %s/%s (%d tasks pending)",
                dag_id,
                run_id,
                len(run_info["non_terminal_tasks"]),
            )

            task = asyncio.create_task(
                self._run_one(
                    deployment_id=None,  # not tied to a deployment
                    dag=dag,
                    run_id=run_id,
                    logical_date=run_info.get("logical_date") or datetime.now(),
                    variables=run_info.get("variables") or {},
                    trigger="recovered",
                )
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    async def _tick(self) -> None:
        now = datetime.now()

        # 1. Drain pending manual triggers.
        for t in await self.meta.drain_triggers():
            await self._fire(
                deployment_id=t["deployment_id"],
                override_variables=t.get("variables") or {},
                logical_date=now,
                trigger="manual",
            )

        # 2. Cron tick every enabled deployment with a cron.
        for dep in await self.meta.list_deployments():
            if not dep.get("enabled", True):
                continue
            if not dep.get("cron"):
                continue
            await self._maybe_schedule(dep, now)

    async def _maybe_schedule(self, dep: dict[str, Any], now: datetime) -> None:
        """Fire scheduled run(s) for a deployment.

        Catch-up behavior (when catch_up=True):
            If the deployment was just created or the scheduler was down,
            fire all missed cron ticks from start_date up to now.

        No catch-up (catch_up=False, default):
            Only fire the most recent cron tick if not already scheduled.

        Max active runs:
            If max_active_runs is set, limit concurrent in-flight runs
            for this deployment.
        """
        dep_id = dep["id"]
        catch_up = dep.get("catch_up", False)
        max_active = dep.get("max_active_runs")  # None = unlimited
        last = _parse_iso(dep.get("_scheduler", {}).get("last_scheduled_at"))
        start = _parse_iso(dep.get("start_date"))
        end = _parse_iso(dep.get("end_date"))

        # Check max_active_runs limit
        if max_active is not None:
            active_count = self._deployment_active_count(dep_id)
            if active_count >= max_active:
                logger.debug(
                    "Skip %s: max_active_runs (%d) reached", dep_id, max_active
                )
                return

        try:
            cron = croniter(dep["cron"], now)
        except (ValueError, KeyError) as exc:
            logger.error("Bad cron on deployment %r: %s", dep_id, exc)
            return

        if catch_up and last is None and start is not None:
            # First schedule for catch_up=True deployment: fire all missed runs
            await self._schedule_catch_up(
                dep, cron, start, end, now, max_active
            )
        else:
            # Normal tick: fire at most one run (most recent cron tick)
            await self._schedule_one(dep, cron, last, start, end, now)

    def _deployment_active_count(self, dep_id: str) -> int:
        """Count active runs for a deployment (used for max_active_runs)."""
        return len(self._active_runs.get(dep_id, set()))

    async def _schedule_catch_up(
        self,
        dep: dict[str, Any],
        cron: croniter,
        start: datetime,
        end: datetime | None,
        now: datetime,
        max_active: int | None,
    ) -> None:
        """Schedule all missed runs for a catch_up=True deployment.

        Generates cron ticks from start_date to now (exclusive) in ASC order,
        then fires them. Respects max_active_runs by batching.
        """
        dep_id = dep["id"]
        ticks: list[datetime] = []

        # Get all cron ticks from start to now
        current = start
        while current < now:
            if end is not None and current > end:
                break
            ticks.append(current)
            # Get next cron tick
            try:
                cron.set_current(current)
                current = cron.get_next(datetime)
            except Exception:
                break

        if not ticks:
            return

        logger.info(
            "Catch-up for %s: %d missed run(s) from %s to %s",
            dep_id,
            len(ticks),
            ticks[0].isoformat(),
            ticks[-1].isoformat(),
        )

        # Fire runs in order, respecting max_active_runs
        fired = 0
        for logical_date in ticks:
            if max_active is not None:
                active = self._deployment_active_count(dep_id)
                if active >= max_active:
                    logger.info(
                        "Catch-up paused for %s: max_active_runs (%d) reached, "
                        "%d run(s) remaining",
                        dep_id,
                        max_active,
                        len(ticks) - fired,
                    )
                    # Store remaining ticks for later
                    await self.meta.update_deployment_scheduler_state(
                        dep_id,
                        last_scheduled_at=ticks[fired - 1]
                        if fired > 0
                        else None,
                    )
                    return

            await self._fire(
                deployment_id=dep_id,
                override_variables={},
                logical_date=logical_date,
                trigger="scheduled",
            )
            fired += 1

        # Update last_scheduled_at to the last fired tick
        if fired > 0:
            await self.meta.update_deployment_scheduler_state(
                dep_id, last_scheduled_at=ticks[fired - 1]
            )

    async def _schedule_one(
        self,
        dep: dict[str, Any],
        cron: croniter,
        last: datetime | None,
        start: datetime | None,
        end: datetime | None,
        now: datetime,
    ) -> None:
        """Schedule a single run for the most recent cron tick (no catch-up)."""
        dep_id = dep["id"]

        # The most recent cron tick at-or-before ``now``.
        due: datetime = cron.get_prev(datetime)

        # Honor start/end window.
        if start is not None and due < start:
            return
        if end is not None and due > end:
            return

        # Already fired this tick (or a later one).
        if last is not None and due <= last:
            return

        await self._fire(
            deployment_id=dep_id,
            override_variables={},
            logical_date=due,
            trigger="scheduled",
        )
        await self.meta.update_deployment_scheduler_state(
            dep_id, last_scheduled_at=due
        )

    # --- run fan-out -------------------------------------------------------

    async def _fire(
        self,
        *,
        deployment_id: str,
        override_variables: dict[str, Any],
        logical_date: datetime,
        trigger: str,
    ) -> str | None:
        """Fire a DagRun for a deployment.

        Returns:
            run_id if fired, None if skipped (e.g., already in-flight)
        """
        dep = await self.meta.get_deployment(deployment_id)
        if dep is None:
            logger.warning("Trigger for unknown deployment %r", deployment_id)
            return None

        # Check max_active_runs limit
        max_active = dep.get("max_active_runs")
        if max_active is not None:
            active_count = self._deployment_active_count(deployment_id)
            if active_count >= max_active:
                logger.info(
                    "Skip %s (%s): max_active_runs (%d) reached",
                    deployment_id,
                    trigger,
                    max_active,
                )
                return None

        # Validate trigger variables against requirements (defense in depth)
        validation_errors = _validate_trigger_variables(dep, override_variables)
        if validation_errors:
            logger.error(
                "Invalid trigger for %s: %s",
                deployment_id,
                "; ".join(validation_errors),
            )
            return None

        dag = self._dags.get(dep["dag_id"])
        if dag is None:
            logger.error(
                "DAG %r not loaded for deployment %r (sync the bundle?)",
                dep["dag_id"],
                deployment_id,
            )
            return None

        # Resolve variables = scoped bundle vars + deployment overrides + run-time overrides.
        variables: dict[str, Any] = {}
        if self._variable_scope is not None and dag._source_file is not None:
            try:
                scoped = self._variable_scope.resolve_for(dag._source_file)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Variable scope resolution failed for dag %r: %s",
                    dag.id,
                    exc,
                )
                scoped = {}
            variables = merge_with_overrides(
                scoped, dep.get("variable_overrides") or {}
            )
        # Apply run-time overrides (from manual trigger)
        variables = {**variables, **override_variables}

        if trigger in ("scheduled", "recovered"):
            run_id = (
                f"scheduled-{dag.id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )
        else:
            run_id = f"manual-{dag.id}-{uuid.uuid4().hex[:8]}"

        # Track active run
        self._active_runs.setdefault(deployment_id, set()).add(run_id)

        task = asyncio.create_task(
            self._run_one(
                deployment_id=deployment_id,
                dag=dag,
                run_id=run_id,
                logical_date=logical_date,
                variables=variables,
                trigger=trigger,
            )
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

        return run_id

    async def _run_one(
        self,
        *,
        deployment_id: str | None,
        dag: Dag,
        run_id: str,
        logical_date: datetime,
        variables: dict[str, Any],
        trigger: str,
    ) -> None:
        try:
            async with self._sem:
                logger.info(
                    "RUN %s (%s) → dag=%s run_id=%s",
                    deployment_id or "recovered",
                    trigger,
                    dag.id,
                    run_id,
                )
                runner = DagRunner(
                    dag,
                    meta=self.meta,
                    variables=variables,
                    bundle_root=self._bundle_root,
                )
                result = await runner.run(
                    variables=variables,
                    run_id=run_id,
                    logical_date=logical_date,
                )
                logger.info(
                    "DONE %s → %s (%s)",
                    deployment_id,
                    result.state,
                    run_id,
                )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Run failed for deployment %s: %s", deployment_id, exc
            )
        finally:
            # Remove from active runs tracking
            if deployment_id and deployment_id in self._active_runs:
                self._active_runs[deployment_id].discard(run_id)
                if not self._active_runs[deployment_id]:
                    del self._active_runs[deployment_id]

    # --- signals -----------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass  # Windows / restricted envs
        try:
            loop.add_signal_handler(signal.SIGHUP, self._request_reload)
        except AttributeError, NotImplementedError:
            pass

    def _request_reload(self) -> None:
        logger.info("SIGHUP received: reload requested")
        self._reload_requested = True
