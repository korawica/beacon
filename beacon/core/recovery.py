"""Crash recovery for Beacon scheduler and worker.

When a Beacon scheduler/worker pod crashes, tasks in RUNNING state become
orphans ("zombies"). This module provides:

1. **Heartbeat tracking** - Tasks write heartbeats while RUNNING
2. **Zombie detection** - Find tasks with stale heartbeats
3. **Run recovery** - Resume or fail orphaned DagRuns on startup

This is similar to Airflow's "zombie task detection" but simpler because
Beacon's async model means all task state is in the metadata store.

Recovery Flow
-------------
On scheduler startup:

    1. List all active DagRuns (state=running)
    2. For each active run, check for zombie tasks:
       - RUNNING with no heartbeat for N seconds → FAILED
       - QUEUED for too long → re-queue or FAILED
    3. Resume runs that have non-terminal tasks

Heartbeat Protocol
------------------
Workers write ``heartbeat_at`` to the task state file:

    {
      "run_id": "...",
      "dag_id": "...",
      "task_id": "...",
      "state": "running",
      "heartbeat_at": "2026-06-06T10:30:45.123456",
      "updated_at": "2026-06-06T10:30:45.123456"
    }

If ``heartbeat_at`` is older than ``zombie_threshold_seconds``, the task
is marked FAILED and the run can be resumed.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from .state import TaskState, TERMINAL_STATES

if TYPE_CHECKING:
    from ..models.dag import Dag
    from ..metadata.json_store import LocalMetadata

logger = logging.getLogger("beacon.recovery")

# Default: 5 minutes without heartbeat = zombie
DEFAULT_ZOMBIE_THRESHOLD_SECONDS = 300


async def detect_zombie_tasks(
    meta: LocalMetadata,
    zombie_threshold_seconds: int = DEFAULT_ZOMBIE_THRESHOLD_SECONDS,
) -> list[dict]:
    """Find all tasks that appear RUNNING but haven't sent a heartbeat.

    Args:
        meta: Metadata store
        zombie_threshold_seconds: Seconds without heartbeat before zombie

    Returns:
        List of zombie task info dicts with keys:
        - run_id, dag_id, task_id
        - state (should always be RUNNING)
        - heartbeat_at (stale timestamp)
        - seconds_since_heartbeat
    """
    zombies: list[dict] = []
    cutoff = datetime.now() - timedelta(seconds=zombie_threshold_seconds)

    active_runs = await meta.list_active_runs()
    for run in active_runs:
        run_id = run["run_id"]
        dag_id = run["dag_id"]

        states = await meta.get_all_task_states(run_id, dag_id)
        for task_id, state in states.items():
            if state != TaskState.RUNNING:
                continue

            # Read full task state to get heartbeat
            task_state_data = await meta.get_task_state_with_heartbeat(
                run_id, dag_id, task_id
            )
            if not task_state_data:
                # No state file at all - definitely zombie
                zombies.append(
                    {
                        "run_id": run_id,
                        "dag_id": dag_id,
                        "task_id": task_id,
                        "state": str(state),
                        "heartbeat_at": None,
                        "seconds_since_heartbeat": zombie_threshold_seconds + 1,
                    }
                )
                continue

            heartbeat_at = task_state_data.get("heartbeat_at")
            if heartbeat_at:
                try:
                    heartbeat_dt = datetime.fromisoformat(heartbeat_at)
                except ValueError:
                    heartbeat_dt = None
            else:
                heartbeat_dt = None

            if heartbeat_dt is None or heartbeat_dt < cutoff:
                seconds_since = (
                    (datetime.now() - heartbeat_dt).total_seconds()
                    if heartbeat_dt
                    else zombie_threshold_seconds + 1
                )
                zombies.append(
                    {
                        "run_id": run_id,
                        "dag_id": dag_id,
                        "task_id": task_id,
                        "state": str(state),
                        "heartbeat_at": heartbeat_at,
                        "seconds_since_heartbeat": seconds_since,
                    }
                )

    return zombies


async def kill_zombies(
    meta: LocalMetadata,
    zombies: list[dict] | None = None,
    zombie_threshold_seconds: int = DEFAULT_ZOMBIE_THRESHOLD_SECONDS,
) -> int:
    """Mark all zombie tasks as FAILED.

    Args:
        meta: Metadata store
        zombies: Pre-detected zombies (if None, will detect)
        zombie_threshold_seconds: For detection if zombies not provided

    Returns:
        Number of tasks marked FAILED
    """
    if zombies is None:
        zombies = await detect_zombie_tasks(meta, zombie_threshold_seconds)

    killed = 0
    for z in zombies:
        logger.warning(
            "Killing zombie task: %s/%s/%s (last heartbeat: %s, %.0fs ago)",
            z["dag_id"],
            z["run_id"],
            z["task_id"],
            z["heartbeat_at"],
            z["seconds_since_heartbeat"],
        )
        await meta.set_task_state(
            z["run_id"],
            z["dag_id"],
            z["task_id"],
            TaskState.FAILED,
        )
        killed += 1

    return killed


async def recover_active_runs(
    meta: LocalMetadata,
    dags: dict[str, Dag],
    variables_scope: dict[str, dict] | None = None,
    zombie_threshold_seconds: int = DEFAULT_ZOMBIE_THRESHOLD_SECONDS,
) -> list[dict]:
    """Find and recover orphaned DagRuns.

    This is typically called on scheduler startup to resume runs that
    were interrupted by a crash.

    Args:
        meta: Metadata store
        dags: Loaded DAG definitions {dag_id: Dag}
        variables_scope: Optional variables per dag_id
        zombie_threshold_seconds: Zombie detection threshold

    Returns:
        List of recovered run info dicts
    """
    # Step 1: Kill zombie tasks
    zombies = await detect_zombie_tasks(meta, zombie_threshold_seconds)
    killed = await kill_zombies(meta, zombies)
    if killed:
        logger.info("Killed %d zombie task(s)", killed)

    # Step 2: Find active runs with non-terminal tasks
    active_runs = await meta.list_active_runs()
    recoverable: list[dict] = []

    for run in active_runs:
        run_id = run["run_id"]
        dag_id = run["dag_id"]

        states = await meta.get_all_task_states(run_id, dag_id)

        # Check if run has any non-terminal tasks
        non_terminal = [
            tid for tid, state in states.items() if state not in TERMINAL_STATES
        ]

        if not non_terminal:
            # All tasks terminal but DagRun still "running" - finalize it
            all_success = all(s == TaskState.SUCCESS for s in states.values())
            final_state = "success" if all_success else "failed"
            await meta.update_dag_run_state(run_id, dag_id, final_state)
            logger.info(
                "Finalized orphan DagRun: %s/%s → %s",
                dag_id,
                run_id,
                final_state,
            )
            continue

        dag = dags.get(dag_id)
        if dag is None:
            logger.warning(
                "Cannot recover run %s: DAG %s not loaded", run_id, dag_id
            )
            continue

        recoverable.append(
            {
                "run_id": run_id,
                "dag_id": dag_id,
                "variables": run.get("variables", {}),
                "logical_date": run.get("logical_date"),
                "non_terminal_tasks": non_terminal,
            }
        )

    return recoverable


async def heartbeat_loop(
    meta: LocalMetadata,
    run_id: str,
    dag_id: str,
    task_id: str,
    interval_seconds: int = 30,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Write heartbeats for a RUNNING task.

    Should be run as a background coroutine while the task executes.

    Args:
        meta: Metadata store
        run_id, dag_id, task_id: Task identity
        interval_seconds: How often to write heartbeat
        stop_event: Event to stop the loop (e.g., on task completion)
    """
    while True:
        if stop_event and stop_event.is_set():
            break

        try:
            await meta.update_task_heartbeat(run_id, dag_id, task_id)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Heartbeat failed for %s/%s/%s: %s",
                dag_id,
                run_id,
                task_id,
                exc,
            )

        if stop_event is None:
            # Single heartbeat
            break

        await asyncio.sleep(interval_seconds)
