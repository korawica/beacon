"""SQLite metadata store implementation.

A production-ready alternative to LocalMetadata for single-node deployments.
Uses SQLite for ACID guarantees and better query performance at scale.

When to use:
- LocalMetadata: Development, <1000 DAGs, file-based simplicity
- SqliteMetadata: Production single-node, 1000+ DAGs, need ACID
- PostgresMetadata: Multi-node, distributed, high availability

Implementation Notes
--------------------
This is a sketch showing how to implement MetadataProtocol for SQLite.
Full implementation would include:
- Connection pooling
- Migration system
- Proper error handling
- WAL mode for concurrency
"""

import json
import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

from ..core.protocols import BaseMetadata
from ..core.state import TaskState
from ..core.task_context import TaskContext

if TYPE_CHECKING:
    import aiosqlite

logger = logging.getLogger("beacon.metadata.sqlite")

# SQL schema
_SCHEMA = """
-- DagRuns
CREATE TABLE IF NOT EXISTS dag_runs (
    run_id TEXT NOT NULL,
    dag_id TEXT NOT NULL,
    dag_version TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'running',
    logical_date TEXT,
    variables TEXT,  -- JSON
    created_at TEXT NOT NULL,
    ended_at TEXT,
    PRIMARY KEY (run_id, dag_id)
);

CREATE INDEX IF NOT EXISTS idx_dag_runs_dag_id ON dag_runs(dag_id);
CREATE INDEX IF NOT EXISTS idx_dag_runs_state ON dag_runs(state);

-- TaskContexts
CREATE TABLE IF NOT EXISTS task_contexts (
    run_id TEXT NOT NULL,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    context TEXT NOT NULL,  -- JSON
    PRIMARY KEY (run_id, dag_id, task_id),
    FOREIGN KEY (run_id, dag_id) REFERENCES dag_runs(run_id, dag_id)
);

-- TaskStates
CREATE TABLE IF NOT EXISTS task_states (
    run_id TEXT NOT NULL,
    dag_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    state TEXT NOT NULL,
    heartbeat_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, dag_id, task_id),
    FOREIGN KEY (run_id, dag_id) REFERENCES dag_runs(run_id, dag_id)
);

CREATE INDEX IF NOT EXISTS idx_task_states_heartbeat ON task_states(heartbeat_at);

-- Deployments
CREATE TABLE IF NOT EXISTS deployments (
    id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    dag_version TEXT,
    cron TEXT,
    timezone TEXT DEFAULT 'UTC',
    start_date TEXT,
    end_date TEXT,
    catch_up INTEGER DEFAULT 0,
    max_active_runs INTEGER,
    variable_overrides TEXT,  -- JSON
    variable_requirements TEXT,  -- JSON
    owners TEXT,  -- JSON array
    labels TEXT,  -- JSON
    enabled INTEGER DEFAULT 1,
    _scheduler TEXT,  -- JSON for scheduler state
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Triggers
CREATE TABLE IF NOT EXISTS triggers (
    trigger_id TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    variables TEXT,  -- JSON
    created_at TEXT NOT NULL,
    PRIMARY KEY (trigger_id, deployment_id),
    FOREIGN KEY (deployment_id) REFERENCES deployments(id)
);

CREATE INDEX IF NOT EXISTS idx_triggers_deployment ON triggers(deployment_id);
"""


class SqliteMetadata(BaseMetadata):
    """SQLite-backed metadata store.

    Implements MetadataProtocol for production single-node deployments.
    ACID-compliant with better query performance than JSON files.

    Example:
        meta = SqliteMetadata("/var/beacon/metadata.db")
        await meta.create_dag_run("run-1", "my-dag", "v1")

    Migration from LocalMetadata:
        # Export from JSON
        beacon export --metadata-path ./metadata.db --output backup.json

        # Import to SQLite
        beacon import --metadata-path ./metadata.db --input backup.json
    """

    def __init__(self, db_path: str = "./beacon.db") -> None:
        super().__init__()
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def _get_db(self) -> aiosqlite.Connection:
        """Get or create database connection."""
        if self._db is None:
            import aiosqlite

            self._db = await aiosqlite.connect(self.db_path)
            # Enable WAL mode for better concurrency
            await self._db.execute("PRAGMA journal_mode=WAL")
            # Create schema
            await self._db.executescript(_SCHEMA)
            await self._db.commit()
        return self._db

    async def close(self) -> None:
        """Close database connection."""
        if self._db:
            await self._db.close()
            self._db = None

    # =========================================================================
    # DagRun Operations
    # =========================================================================

    async def create_dag_run(
        self,
        run_id: str,
        dag_id: str,
        dag_version: str,
        state: str = "running",
        logical_date: datetime | None = None,
        variables: dict[str, Any] | None = None,
    ) -> None:
        db = await self._get_db()
        await db.execute(
            """
            INSERT INTO dag_runs (run_id, dag_id, dag_version, state, logical_date, variables, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                dag_id,
                dag_version,
                state,
                logical_date.isoformat() if logical_date else None,
                json.dumps(variables) if variables else None,
                datetime.now().isoformat(),
            ),
        )
        await db.commit()
        self._track_active_run(dag_id, run_id)

    async def get_dag_run(
        self, run_id: str, dag_id: str
    ) -> dict[str, Any] | None:
        db = await self._get_db()
        async with db.execute(
            "SELECT * FROM dag_runs WHERE run_id = ? AND dag_id = ?",
            (run_id, dag_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return dict(row)
            return None

    async def update_dag_run_state(
        self, run_id: str, dag_id: str, state: str
    ) -> None:
        db = await self._get_db()
        ended_at = (
            datetime.now().isoformat()
            if state in ("success", "failed")
            else None
        )
        await db.execute(
            """
            UPDATE dag_runs SET state = ?, ended_at = ?
            WHERE run_id = ? AND dag_id = ?
            """,
            (state, ended_at, run_id, dag_id),
        )
        await db.commit()
        if state in ("success", "failed"):
            self._untrack_active_run(dag_id, run_id)

    async def list_dag_runs(
        self,
        dag_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        db = await self._get_db()
        if dag_id:
            query = "SELECT * FROM dag_runs WHERE dag_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (dag_id, limit)
        else:
            query = "SELECT * FROM dag_runs ORDER BY created_at DESC LIMIT ?"
            params = (limit,)

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def list_active_runs(
        self, dag_id: str | None = None
    ) -> list[dict[str, Any]]:
        """List active runs from database (not just in-memory tracking)."""
        db = await self._get_db()
        if dag_id:
            query = (
                "SELECT * FROM dag_runs WHERE dag_id = ? AND state = 'running'"
            )
            params = (dag_id,)
        else:
            query = "SELECT * FROM dag_runs WHERE state = 'running'"
            params = ()

        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    # =========================================================================
    # TaskState Operations
    # =========================================================================

    async def set_task_state(
        self, run_id: str, dag_id: str, task_id: str, state: TaskState
    ) -> None:
        db = await self._get_db()
        now = datetime.now().isoformat()
        await db.execute(
            """
            INSERT INTO task_states (run_id, dag_id, task_id, state, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(run_id, dag_id, task_id) DO UPDATE SET
                state = excluded.state,
                updated_at = excluded.updated_at
            """,
            (run_id, dag_id, task_id, str(state), now),
        )
        await db.commit()
        self._cache_put(f"{run_id}:{task_id}", state)

    async def get_task_state(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskState | None:
        # Check cache first
        cached = self._cache_get(f"{run_id}:{task_id}")
        if cached is not None:
            return cached

        db = await self._get_db()
        async with db.execute(
            "SELECT state FROM task_states WHERE run_id = ? AND dag_id = ? AND task_id = ?",
            (run_id, dag_id, task_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                state = TaskState(row["state"])
                self._cache_put(f"{run_id}:{task_id}", state)
                return state
            return None

    async def get_all_task_states(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskState]:
        db = await self._get_db()
        async with db.execute(
            "SELECT task_id, state FROM task_states WHERE run_id = ? AND dag_id = ?",
            (run_id, dag_id),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row["task_id"]: TaskState(row["state"]) for row in rows}

    async def get_task_state_with_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any] | None:
        db = await self._get_db()
        async with db.execute(
            "SELECT * FROM task_states WHERE run_id = ? AND dag_id = ? AND task_id = ?",
            (run_id, dag_id, task_id),
        ) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def update_task_heartbeat(
        self, run_id: str, dag_id: str, task_id: str
    ) -> None:
        db = await self._get_db()
        now = datetime.now().isoformat()
        await db.execute(
            """
            INSERT INTO task_states (run_id, dag_id, task_id, state, heartbeat_at, updated_at)
            VALUES (?, ?, ?, 'running', ?, ?)
            ON CONFLICT(run_id, dag_id, task_id) DO UPDATE SET
                heartbeat_at = excluded.heartbeat_at,
                updated_at = excluded.updated_at
            """,
            (run_id, dag_id, task_id, now, now),
        )
        await db.commit()

    # =========================================================================
    # TaskContext Operations
    # =========================================================================

    async def put_task_context(
        self, run_id: str, dag_id: str, task_id: str, task_ctx: TaskContext
    ) -> None:
        db = await self._get_db()
        await db.execute(
            """
            INSERT INTO task_contexts (run_id, dag_id, task_id, context)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(run_id, dag_id, task_id) DO UPDATE SET context = excluded.context
            """,
            (run_id, dag_id, task_id, task_ctx.model_dump_json()),
        )
        await db.commit()

    async def get_task_context(
        self, run_id: str, dag_id: str, task_id: str
    ) -> TaskContext | None:
        db = await self._get_db()
        async with db.execute(
            "SELECT context FROM task_contexts WHERE run_id = ? AND dag_id = ? AND task_id = ?",
            (run_id, dag_id, task_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return TaskContext.model_validate_json(row["context"])
            return None

    async def get_all_task_contexts(
        self, run_id: str, dag_id: str
    ) -> dict[str, TaskContext]:
        db = await self._get_db()
        async with db.execute(
            "SELECT task_id, context FROM task_contexts WHERE run_id = ? AND dag_id = ?",
            (run_id, dag_id),
        ) as cursor:
            rows = await cursor.fetchall()
            return {
                row["task_id"]: TaskContext.model_validate_json(row["context"])
                for row in rows
            }

    async def get_task_outputs(
        self, run_id: str, dag_id: str, task_id: str
    ) -> dict[str, Any]:
        db = await self._get_db()
        async with db.execute(
            "SELECT context->>'$.outputs' as outputs FROM task_contexts WHERE run_id = ? AND dag_id = ? AND task_id = ?",
            (run_id, dag_id, task_id),
        ) as cursor:
            row = await cursor.fetchone()
            if row and row["outputs"]:
                return json.loads(row["outputs"])
            return {}

    async def clear_task(self, run_id: str, dag_id: str, task_id: str) -> None:
        """Reset task for re-execution."""
        await self._get_db()
        # Reset context (clear attempts and outputs)
        ctx = await self.get_task_context(run_id, dag_id, task_id)
        if ctx:
            ctx.attempts = []
            ctx.outputs = {}
            await self.put_task_context(run_id, dag_id, task_id, ctx)
        # Reset state
        await self.set_task_state(run_id, dag_id, task_id, TaskState.NONE)

    # =========================================================================
    # Deployment Operations
    # =========================================================================

    async def upsert_deployment(self, deployment: dict[str, Any]) -> None:
        db = await self._get_db()
        now = datetime.now().isoformat()
        await db.execute(
            """
            INSERT INTO deployments (
                id, dag_id, dag_version, cron, timezone, start_date, end_date,
                catch_up, max_active_runs, variable_overrides, variable_requirements,
                owners, labels, enabled, _scheduler, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                dag_id = excluded.dag_id,
                dag_version = excluded.dag_version,
                cron = excluded.cron,
                timezone = excluded.timezone,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                catch_up = excluded.catch_up,
                max_active_runs = excluded.max_active_runs,
                variable_overrides = excluded.variable_overrides,
                variable_requirements = excluded.variable_requirements,
                owners = excluded.owners,
                labels = excluded.labels,
                enabled = excluded.enabled,
                updated_at = excluded.updated_at
            """,
            (
                deployment["id"],
                deployment["dag_id"],
                deployment.get("dag_version"),
                deployment.get("cron"),
                deployment.get("timezone", "UTC"),
                deployment.get("start_date"),
                deployment.get("end_date"),
                int(deployment.get("catch_up", False)),
                deployment.get("max_active_runs"),
                json.dumps(deployment.get("variable_overrides")),
                json.dumps(deployment.get("variable_requirements")),
                json.dumps(deployment.get("owners", [])),
                json.dumps(deployment.get("labels", {})),
                int(deployment.get("enabled", True)),
                json.dumps(deployment.get("_scheduler", {})),
                now,
                now,
            ),
        )
        await db.commit()

    async def get_deployment(self, deployment_id: str) -> dict[str, Any] | None:
        db = await self._get_db()
        async with db.execute(
            "SELECT * FROM deployments WHERE id = ?",
            (deployment_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return self._row_to_deployment(row)
            return None

    async def list_deployments(self) -> list[dict[str, Any]]:
        db = await self._get_db()
        async with db.execute(
            "SELECT * FROM deployments ORDER BY id"
        ) as cursor:
            rows = await cursor.fetchall()
            return [self._row_to_deployment(row) for row in rows]

    async def delete_deployment(self, deployment_id: str) -> bool:
        db = await self._get_db()
        cursor = await db.execute(
            "DELETE FROM deployments WHERE id = ?",
            (deployment_id,),
        )
        await db.commit()
        return cursor.rowcount > 0

    async def update_deployment_scheduler_state(
        self,
        deployment_id: str,
        *,
        last_scheduled_at: datetime,
    ) -> None:
        db = await self._get_db()
        # Get current scheduler state
        async with db.execute(
            "SELECT _scheduler FROM deployments WHERE id = ?",
            (deployment_id,),
        ) as cursor:
            row = await cursor.fetchone()
            scheduler = (
                json.loads(row["_scheduler"])
                if row and row["_scheduler"]
                else {}
            )

        scheduler["last_scheduled_at"] = last_scheduled_at.isoformat()
        await db.execute(
            "UPDATE deployments SET _scheduler = ?, updated_at = ? WHERE id = ?",
            (json.dumps(scheduler), datetime.now().isoformat(), deployment_id),
        )
        await db.commit()

    def _row_to_deployment(self, row: dict) -> dict[str, Any]:
        """Convert database row to deployment dict."""
        return {
            "id": row["id"],
            "dag_id": row["dag_id"],
            "dag_version": row["dag_version"],
            "cron": row["cron"],
            "timezone": row["timezone"],
            "start_date": row["start_date"],
            "end_date": row["end_date"],
            "catch_up": bool(row["catch_up"]),
            "max_active_runs": row["max_active_runs"],
            "variable_overrides": json.loads(row["variable_overrides"] or "{}"),
            "variable_requirements": json.loads(
                row["variable_requirements"] or "{}"
            ),
            "owners": json.loads(row["owners"] or "[]"),
            "labels": json.loads(row["labels"] or "{}"),
            "enabled": bool(row["enabled"]),
            "_scheduler": json.loads(row["_scheduler"] or "{}"),
        }

    # =========================================================================
    # Trigger Operations
    # =========================================================================

    async def enqueue_trigger(
        self,
        deployment_id: str,
        variables: dict[str, Any] | None = None,
    ) -> str:
        import uuid

        trigger_id = uuid.uuid4().hex[:12]
        db = await self._get_db()
        await db.execute(
            """
            INSERT INTO triggers (trigger_id, deployment_id, variables, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                trigger_id,
                deployment_id,
                json.dumps(variables or {}),
                datetime.now().isoformat(),
            ),
        )
        await db.commit()
        return trigger_id

    async def drain_triggers(
        self, deployment_id: str | None = None
    ) -> list[dict[str, Any]]:
        db = await self._get_db()
        if deployment_id:
            async with db.execute(
                "SELECT * FROM triggers WHERE deployment_id = ? ORDER BY created_at",
                (deployment_id,),
            ) as cursor:
                rows = await cursor.fetchall()
            await db.execute(
                "DELETE FROM triggers WHERE deployment_id = ?",
                (deployment_id,),
            )
        else:
            async with db.execute(
                "SELECT * FROM triggers ORDER BY created_at"
            ) as cursor:
                rows = await cursor.fetchall()
            await db.execute("DELETE FROM triggers")
        await db.commit()
        return [
            {
                "trigger_id": row["trigger_id"],
                "deployment_id": row["deployment_id"],
                "variables": json.loads(row["variables"] or "{}"),
                "created_at": row["created_at"],
            }
            for row in rows
        ]
