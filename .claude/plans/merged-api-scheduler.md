# Implementation Plan: Merged API Server + Scheduler with Coordination

## Goal

Implement a merged API server + scheduler that can scale horizontally (multiple instances) with proper coordination to prevent duplicate runs and race conditions.

## Problem Statement

Current `DeploymentScheduler` is single-process:
- `_active_runs` is in-memory → not shared across instances
- No coordination when firing scheduled runs → duplicates possible
- No coordination when draining triggers → multiple instances could process same trigger
- `last_scheduled_at` update is not atomic with run creation

## Solution: Coordination via Metadata Store

Use the metadata store as the coordination layer. Each operation that could race becomes atomic:

1. **`try_create_dag_run()`** - Creates run only if `(dag_id, logical_date)` doesn't exist
2. **`claim_trigger()`** - Atomically claims a trigger (marks as "processing")
3. **`try_schedule_tick()`** - Atomically updates `last_scheduled_at` only if newer

This works with `LocalMetadata` (file-based locks) and extends naturally to `SqliteMetadata`/`PostgresMetadata` (UNIQUE constraints).

---

## Implementation Steps

### Step 1: Extend MetadataProtocol with Coordination Methods

**File:** `beacon/core/protocols.py`

Add new methods:

```python
async def try_create_dag_run(
    self,
    run_id: str,
    dag_id: str,
    dag_version: str,
    logical_date: datetime,
    state: str = "running",
    variables: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    """Atomically create a DagRun only if (dag_id, logical_date) doesn't exist.

    Returns:
        (created, run_id) - created=True if we won the race
    """
    ...

async def claim_trigger(
    self,
    trigger_id: str,
    deployment_id: str,
    claimed_by: str,
) -> bool:
    """Atomically claim a trigger for processing.

    Returns:
        True if claim succeeded, False if already claimed by another instance
    """
    ...

async def try_update_scheduler_state(
    self,
    deployment_id: str,
    last_scheduled_at: datetime,
    expected_previous: datetime | None = None,
) -> bool:
    """Atomically update last_scheduled_at only if newer.

    Returns:
        True if update succeeded, False if another instance updated first
    """
    ...
```

### Step 2: Implement Coordination in LocalMetadata

**File:** `beacon/metadata/local_store.py`

**Strategy:** Use lock files (`.lock`) for coordination:

```
metadata.db/
├── .locks/
│   ├── dag_run_{dag_id}_{logical_date}.lock
│   ├── trigger_{trigger_id}.lock
│   └── scheduler_{deployment_id}.lock
```

**Implementation:**

```python
import fcntl  # Unix file locking

class LocalMetadata(BaseMetadata):
    def __init__(self, base_path: str | Path = "./metadata.db") -> None:
        # ... existing init ...
        self._locks_dir = self.base_path / ".locks"
        self._locks_dir.mkdir(parents=True, exist_ok=True)

    async def try_create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        logical_date: datetime,
        state: str = "running",
        variables: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        """Create run atomically with uniqueness check on (dag_id, logical_date)."""
        # For scheduled runs, check if a run already exists for this logical_date
        if logical_date:
            lock_key = f"{dag_id}_{logical_date.strftime('%Y%m%dT%H%M%S')}"
            lock_path = self._locks_dir / f"scheduled_{lock_key}.lock"

            # Try to acquire exclusive lock
            try:
                fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

                # Check if run already exists
                existing = await self._find_run_by_logical_date(dag_id, logical_date)
                if existing:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                    os.close(fd)
                    return (False, existing["run_id"])

                # Create the run
                await self.create_dag_run(
                    run_id=run_id,
                    dag_id=dag_id,
                    dag_version=dag_version,
                    state=state,
                    logical_date=logical_date,
                    variables=variables,
                )

                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
                return (True, run_id)
            except BlockingIOError:
                # Another instance has the lock
                return (False, "")
        else:
            # Manual triggers don't need deduplication
            await self.create_dag_run(...)
            return (True, run_id)
```

### Step 3: Update DeploymentScheduler to Use Coordination

**File:** `beacon/scheduler.py`

Key changes:

```python
class DeploymentScheduler:
    def __init__(
        self,
        bundle_path: str | Path,
        meta: LocalMetadata,
        *,
        instance_id: str | None = None,  # NEW: unique instance identifier
        tick_seconds: int = 5,
        max_concurrent_runs: int = 8,
    ) -> None:
        self.instance_id = instance_id or f"inst-{uuid.uuid4().hex[:8]}"
        # ... rest of init ...

    async def _schedule_one(
        self,
        dep: dict[str, Any],
        cron: croniter,
        last: datetime | None,
        start: datetime | None,
        end: datetime | None,
        now: datetime,
    ) -> None:
        """Schedule a single run with coordination."""
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

        # COORDINATION: Try to claim this scheduled tick
        claimed = await self.meta.try_update_scheduler_state(
            dep_id,
            last_scheduled_at=due,
        )

        if not claimed:
            logger.debug(
                "Skip %s: another instance already scheduled tick at %s",
                dep_id, due
            )
            return

        # We won the race - fire the run
        await self._fire(
            deployment_id=dep_id,
            override_variables={},
            logical_date=due,
            trigger="scheduled",
        )
```

### Step 4: Create API Server Module

**File:** `beacon/api/__init__.py` (new)

```python
"""Beacon API Server.

A FastAPI-based REST API that also runs the scheduler loop.
Can be scaled horizontally with coordination via metadata store.
"""

from .app import create_app
from .server import run_server

__all__ = ["create_app", "run_server"]
```

**File:** `beacon/api/app.py` (new)

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any
import asyncio
from pathlib import Path

from ..metadata import LocalMetadata
from ..scheduler import DeploymentScheduler


class TriggerRequest(BaseModel):
    deployment_id: str
    variables: dict[str, Any] | None = None


class TriggerResponse(BaseModel):
    trigger_id: str
    message: str


def create_app(
    bundle_path: Path,
    meta: LocalMetadata,
    scheduler: DeploymentScheduler | None = None,
) -> FastAPI:
    """Create FastAPI app with optional embedded scheduler."""

    app = FastAPI(title="Beacon API", version="0.1.0")

    # Store scheduler in app state
    app.state.scheduler = scheduler
    app.state.meta = meta
    app.state.bundle_path = bundle_path

    @app.get("/health")
    async def health():
        return {"status": "ok", "instance_id": scheduler.instance_id if scheduler else None}

    @app.post("/triggers", response_model=TriggerResponse)
    async def create_trigger(req: TriggerRequest):
        """Create a manual trigger for a deployment."""
        dep = await meta.get_deployment(req.deployment_id)
        if not dep:
            raise HTTPException(404, f"Deployment {req.deployment_id} not found")

        trigger_id = await meta.enqueue_trigger(
            req.deployment_id,
            req.variables,
        )
        return TriggerResponse(
            trigger_id=trigger_id,
            message=f"Trigger enqueued for {req.deployment_id}",
        )

    @app.get("/deployments")
    async def list_deployments():
        """List all deployments."""
        deps = await meta.list_deployments()
        return {"deployments": deps}

    @app.get("/deployments/{deployment_id}")
    async def get_deployment(deployment_id: str):
        """Get a deployment by ID."""
        dep = await meta.get_deployment(deployment_id)
        if not dep:
            raise HTTPException(404, f"Deployment {deployment_id} not found")
        return dep

    @app.get("/runs")
    async def list_runs(dag_id: str | None = None, limit: int = 50):
        """List recent DAG runs."""
        runs = await meta.list_dag_runs(dag_id=dag_id, limit=limit)
        return {"runs": runs}

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str, dag_id: str):
        """Get a specific run."""
        run = await meta.get_dag_run(run_id, dag_id)
        if not run:
            raise HTTPException(404, f"Run {run_id} not found")
        return run

    return app
```

**File:** `beacon/api/server.py` (new)

```python
import asyncio
import logging
import signal
from pathlib import Path

import uvicorn

from ..metadata import LocalMetadata
from ..scheduler import DeploymentScheduler
from .app import create_app

logger = logging.getLogger("beacon.api")


async def run_server(
    bundle_path: Path,
    meta: LocalMetadata,
    host: str = "0.0.0.0",
    port: int = 8080,
    tick_seconds: int = 5,
    max_concurrent_runs: int = 8,
    instance_id: str | None = None,
) -> None:
    """Run the merged API + Scheduler server."""

    # Create scheduler instance
    scheduler = DeploymentScheduler(
        bundle_path=bundle_path,
        meta=meta,
        tick_seconds=tick_seconds,
        max_concurrent_runs=max_concurrent_runs,
        instance_id=instance_id,
    )

    # Load DAGs
    scheduler.reload()

    # Create FastAPI app
    app = create_app(bundle_path, meta, scheduler)

    # Start scheduler in background
    scheduler_task = asyncio.create_task(scheduler.run())

    # Setup signal handlers
    stop_event = asyncio.Event()

    def handle_shutdown():
        logger.info("Shutdown signal received")
        stop_event.set()
        scheduler._stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_shutdown)

    # Run uvicorn
    config = uvicorn.Config(app, host=host, port=port, loop="asyncio")
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        # Wait for scheduler to finish
        scheduler._stop.set()
        await scheduler_task


def main():
    """CLI entry point for beacon-api command."""
    import argparse
    from ..cli.settings import get

    parser = argparse.ArgumentParser(description="Beacon API Server")
    parser.add_argument("bundle", help="Path to bundle directory")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8080, help="Port to bind")
    parser.add_argument(
        "--metadata-path",
        default=get("BEACON_METADATA_PATH"),
        help="Metadata store path",
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        help="Unique instance identifier (auto-generated if not set)",
    )
    args = parser.parse_args()

    meta = LocalMetadata(args.metadata_path)

    asyncio.run(
        run_server(
            bundle_path=Path(args.bundle),
            meta=meta,
            host=args.host,
            port=args.port,
            instance_id=args.instance_id,
        )
    )


if __name__ == "__main__":
    main()
```

### Step 5: Add CLI Command

**File:** `beacon/cli/commands/api_cmd.py` (new)

```python
import asyncio
import click
from pathlib import Path

from ...api import run_server
from ...metadata import LocalMetadata
from ..settings import get


@click.command("api")
@click.argument("bundle", type=click.Path(exists=True))
@click.option("--host", default="0.0.0.0", help="Host to bind")
@click.option("--port", default=8080, help="Port to bind")
@click.option(
    "--metadata-path",
    default=None,
    help="Metadata store path (default: BEACON_METADATA_PATH)",
)
@click.option(
    "--tick-seconds",
    default=None,
    type=int,
    help="Scheduler tick interval",
)
@click.option(
    "--max-concurrent",
    default=None,
    type=int,
    help="Max concurrent runs",
)
@click.option(
    "--instance-id",
    default=None,
    help="Unique instance ID for coordination",
)
def api(
    bundle: str,
    host: str,
    port: int,
    metadata_path: str | None,
    tick_seconds: int | None,
    max_concurrent: int | None,
    instance_id: str | None,
):
    """Run the Beacon API server with embedded scheduler.

    Can be scaled horizontally - multiple instances coordinate via
    the metadata store to prevent duplicate runs.
    """
    from ...api import run_server

    meta = LocalMetadata(metadata_path or get("BEACON_METADATA_PATH"))

    asyncio.run(
        run_server(
            bundle_path=Path(bundle),
            meta=meta,
            host=host,
            port=port,
            tick_seconds=tick_seconds or get("BEACON_SCHEDULER_TICK_SECONDS"),
            max_concurrent_runs=max_concurrent or get("BEACON_SCHEDULER_MAX_CONCURRENT_RUNS"),
            instance_id=instance_id,
        )
    )
```

### Step 6: Update `beacon/cli/main.py`

Add the new `api` command to the CLI:

```python
from .commands.api_cmd import api

cli.add_command(api)
```

### Step 7: Update `pyproject.toml`

Add FastAPI and uvicorn as optional dependencies:

```toml
[project.optional-dependencies]
api = ["fastapi>=0.109.0", "uvicorn>=0.27.0"]
```

---

## Testing Plan

### Unit Tests

1. **`test_coordination.py`** - Test `try_create_dag_run()` deduplication
2. **`test_scheduler_coordination.py`** - Test multiple scheduler instances don't create duplicate runs
3. **`test_api.py`** - Test API endpoints

### Integration Tests

1. Start 2 API servers with same metadata store
2. Create deployment with cron schedule
3. Wait for scheduled tick
4. Verify only 1 run was created (not 2)

---

## Files to Create/Modify

### New Files
- `beacon/api/__init__.py`
- `beacon/api/app.py`
- `beacon/api/server.py`
- `beacon/cli/commands/api_cmd.py`
- `tests/test_coordination.py`

### Modified Files
- `beacon/core/protocols.py` - Add coordination methods
- `beacon/metadata/local_store.py` - Implement coordination
- `beacon/scheduler.py` - Use coordination methods
- `beacon/cli/main.py` - Add `api` command
- `pyproject.toml` - Add optional dependencies

---

## Rollout Strategy

1. **Phase 1:** Implement coordination in `LocalMetadata` + update `DeploymentScheduler`
   - Existing `beacon scheduler` command continues to work
   - Single instance works as before
   - Multiple instances now coordinate correctly

2. **Phase 2:** Add API server
   - New `beacon api` command
   - Runs scheduler + REST API
   - Can scale horizontally

3. **Phase 3:** Update `SqliteMetadata` (when implemented)
   - Use `UNIQUE` constraints for coordination
   - More efficient than file locks

---

## Summary

| Aspect | Before | After |
|--------|--------|-------|
| Scheduler instances | 1 only | N (horizontal scale) |
| Coordination | None | Via metadata store |
| API | None (CLI only) | REST API |
| Active run tracking | In-memory | Persisted in metadata |
| Duplicate prevention | None | Atomic `try_create_dag_run` |
