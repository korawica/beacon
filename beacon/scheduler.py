"""Deployment Scheduler.

A small, single-process async loop that:

  1. **Drains pending manual triggers** from the metadata store
     (written by ``beacon trigger ...``) and fires a DagRun for each.
  2. **Ticks every enabled cron Deployment** and fires the next scheduled
     run when its cron expression is due.

The scheduler shares its metadata store with the CLI — that's the IPC
layer (see ``JsonMetadata.enqueue_trigger`` /
``drain_triggers``). No socket, no API.

Semantics
---------
* **No catch-up.** If the scheduler was down across N missed ticks, only
  the *next* tick fires. This matches ``Deployment.catch_up=False`` and
  is currently the only supported mode.
* **One concurrent run per deployment.** If a deployment's previous run
  is still in flight when the next cron tick is due, the new tick is
  skipped (and lost — no queue). A global concurrency cap
  (``BEACON_SCHEDULER_MAX_CONCURRENT_RUNS``) limits total in-flight runs
  across all deployments.
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
from .metadata import JsonMetadata
from .models.dag import Dag
from .runner import DagRunner

logger = logging.getLogger("beacon.scheduler")


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
    """Cron + manual-trigger loop sharing JsonMetadata with the CLI."""

    def __init__(
        self,
        bundle_path: str | Path,
        meta: JsonMetadata,
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
        self._in_flight: set[str] = set()  # deployment ids currently running
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
                dags[d.id] = d
        self._dags = dags
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

    async def _tick(self) -> None:
        now = datetime.now()

        # 1. Drain pending manual triggers.
        for t in await self.meta.drain_triggers():
            await self._fire(
                deployment_id=t["deployment_id"],
                override_params=t.get("params") or {},
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
        """Fire at most ONE run if the most recent cron tick is due.

        No catch-up: if N ticks were missed (because the scheduler was
        down, or because we just upserted a deployment with a far-past
        start_date), only the most recent at-or-before ``now`` is
        considered; the earlier missed ticks are dropped.
        """
        last = _parse_iso(dep.get("_scheduler", {}).get("last_scheduled_at"))
        start = _parse_iso(dep.get("start_date"))
        end = _parse_iso(dep.get("end_date"))
        try:
            # The most recent cron tick at-or-before ``now``.
            due: datetime = croniter(dep["cron"], now).get_prev(datetime)
        except (ValueError, KeyError) as exc:
            logger.error("Bad cron on deployment %r: %s", dep.get("id"), exc)
            return

        # Honor start/end window.
        if start is not None and due < start:
            return
        if end is not None and due > end:
            return
        # Already fired this tick (or a later one).
        if last is not None and due <= last:
            return

        await self._fire(
            deployment_id=dep["id"],
            override_params={},
            logical_date=due,
            trigger="scheduled",
        )
        await self.meta.update_deployment_scheduler_state(
            dep["id"], last_scheduled_at=due
        )

    # --- run fan-out -------------------------------------------------------

    async def _fire(
        self,
        *,
        deployment_id: str,
        override_params: dict[str, Any],
        logical_date: datetime,
        trigger: str,
    ) -> None:
        if deployment_id in self._in_flight:
            logger.info(
                "Skip %s (%s): previous run still in-flight",
                deployment_id,
                trigger,
            )
            return
        dep = await self.meta.get_deployment(deployment_id)
        if dep is None:
            logger.warning("Trigger for unknown deployment %r", deployment_id)
            return
        dag = self._dags.get(dep["dag_id"])
        if dag is None:
            logger.error(
                "DAG %r not loaded for deployment %r (sync the bundle?)",
                dep["dag_id"],
                deployment_id,
            )
            return

        params = {**(dep.get("params") or {}), **override_params}
        if trigger == "scheduled":
            run_id = (
                f"scheduled-{dag.id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )
        else:
            run_id = f"manual-{dag.id}-{uuid.uuid4().hex[:8]}"

        self._in_flight.add(deployment_id)
        task = asyncio.create_task(
            self._run_one(
                deployment_id=deployment_id,
                dag=dag,
                run_id=run_id,
                logical_date=logical_date,
                params=params,
                trigger=trigger,
            )
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _run_one(
        self,
        *,
        deployment_id: str,
        dag: Dag,
        run_id: str,
        logical_date: datetime,
        params: dict[str, Any],
        trigger: str,
    ) -> None:
        try:
            async with self._sem:
                logger.info(
                    "RUN %s (%s) → dag=%s run_id=%s",
                    deployment_id,
                    trigger,
                    dag.id,
                    run_id,
                )
                runner = DagRunner(dag, meta=self.meta)
                result = await runner.run(
                    params=params,
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
            self._in_flight.discard(deployment_id)

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
