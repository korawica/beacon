"""Tests for the DeploymentScheduler.

We drive the scheduler directly (no subprocess) to keep tests fast and
deterministic. Manual triggers are exercised end-to-end; the cron path
is tested with a 1-second cron and a short observation window.
"""

import asyncio
import textwrap
import time
from datetime import datetime, timedelta
from pathlib import Path


from beacon.metadata import JsonMetadata
from beacon.scheduler import DeploymentScheduler


DAG_FAST = """
from beacon import Dag, Task
dag = Dag(id="hello", actions=[Task(id="t", uses="empty")])
"""


def _write_bundle(tmp_path: Path) -> Path:
    dags = tmp_path / "dags"
    dags.mkdir()
    (dags / "d.py").write_text(textwrap.dedent(DAG_FAST).lstrip())
    return tmp_path


async def _drive(sched: DeploymentScheduler, *, ticks: int) -> None:
    """Run ``ticks`` scheduler ticks back-to-back, then drain in-flight."""
    sched.reload()
    for _ in range(ticks):
        await sched._tick()
    # Wait for any background DagRunner tasks to finish.
    if sched._tasks:
        await asyncio.gather(*sched._tasks, return_exceptions=True)


# ---------- manual trigger path -------------------------------------------


def test_manual_trigger_fires_a_dag_run(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment({"id": "d1", "dag_id": "hello"})
        await meta.enqueue_trigger("d1", params={})
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())

    runs = asyncio.run(meta.list_dag_runs("hello"))
    assert len(runs) == 1
    assert runs[0]["state"] == "success"
    assert runs[0]["run_id"].startswith("manual-hello-")


def test_unknown_deployment_trigger_is_skipped_not_fatal(
    tmp_path: Path,
) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.enqueue_trigger("ghost", params={})
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


def test_unknown_dag_is_skipped_not_fatal(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        # Deployment references a dag id that the bundle doesn't have.
        await meta.upsert_deployment({"id": "d1", "dag_id": "missing"})
        await meta.enqueue_trigger("d1")
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


# ---------- cron path -----------------------------------------------------


def test_cron_due_fires_and_advances_bookkeeping(tmp_path: Path) -> None:
    """A cron whose next tick is in the past must fire exactly once and
    persist ``last_scheduled_at`` so the next tick is in the future."""
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment(
            {
                "id": "d1",
                "dag_id": "hello",
                "cron": "* * * * *",  # every minute
                "enabled": True,
                # Start in the past so the next cron tick is overdue.
                "start_date": (
                    datetime.now() - timedelta(minutes=5)
                ).isoformat(),
            }
        )
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())

    runs = asyncio.run(meta.list_dag_runs("hello"))
    assert len(runs) == 1
    assert runs[0]["run_id"].startswith("scheduled-hello-")
    dep = asyncio.run(meta.get_deployment("d1"))
    assert dep["_scheduler"]["last_scheduled_at"] is not None


def test_cron_not_due_does_not_fire(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment(
            {
                "id": "d1",
                "dag_id": "hello",
                "cron": "0 0 1 1 *",  # once a year
                "enabled": True,
                "start_date": datetime.now().isoformat(),
            }
        )
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=2)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


def test_disabled_deployment_skipped(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment(
            {
                "id": "d1",
                "dag_id": "hello",
                "cron": "* * * * *",
                "enabled": False,
                "start_date": (
                    datetime.now() - timedelta(minutes=5)
                ).isoformat(),
            }
        )
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


def test_end_date_stops_scheduling(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment(
            {
                "id": "d1",
                "dag_id": "hello",
                "cron": "* * * * *",
                "enabled": True,
                "start_date": (datetime.now() - timedelta(days=2)).isoformat(),
                "end_date": (datetime.now() - timedelta(days=1)).isoformat(),
            }
        )
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


def test_bad_cron_logs_and_does_not_crash(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment(
            {
                "id": "d1",
                "dag_id": "hello",
                "cron": "definitely not cron",
                "enabled": True,
            }
        )
        sched = DeploymentScheduler(bundle, meta)
        await _drive(sched, ticks=1)

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


# ---------- concurrency / lifecycle ---------------------------------------


def test_in_flight_deployment_skips_overlapping_tick(tmp_path: Path) -> None:
    """If a deployment's previous run hasn't finished, a same-tick fire
    is dropped (no queue). We simulate by pre-marking _in_flight."""
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        await meta.upsert_deployment({"id": "d1", "dag_id": "hello"})
        await meta.enqueue_trigger("d1")
        sched = DeploymentScheduler(bundle, meta)
        sched.reload()
        sched._in_flight.add("d1")  # simulate prior run still going
        await sched._tick()
        # No real run kicked off because _in_flight had "d1".
        assert sched._tasks == set()

    asyncio.run(go())
    assert asyncio.run(meta.list_dag_runs()) == []


def test_run_loop_exits_on_stop(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path)
    meta = JsonMetadata(tmp_path / "m")

    async def go() -> None:
        sched = DeploymentScheduler(bundle, meta, tick_seconds=10)

        async def stopper() -> None:
            await asyncio.sleep(0.1)
            sched._stop.set()

        start = time.monotonic()
        await asyncio.gather(sched.run(), stopper())
        # If shutdown is honored, this returns in ~0.1s, not 10s.
        assert time.monotonic() - start < 2.0

    asyncio.run(go())
