# Beacon Architecture Review: Production Scale (10,000 DAGs/hour)

**Date:** 2026-06-06
**Status:** Assessment for production readiness

---

## Executive Summary

Beacon's architecture is fundamentally sound for production workloads. However, **10,000 DAGs per hour** is at the upper boundary of the Phase 2 design target. The current Phase 1 implementation has specific bottlenecks that will surface at this scale.

**Verdict:** Phase 1 (single-node file-based) → **Not recommended** for 10K DAGs/hour.
Phase 2 (service mode with SQLite) → **Possible with tuning**.
Phase 3 (distributed with Postgres) → **Required** for sustained 10K DAGs/hour.

---

## 1. Current Architecture Analysis

### 1.1 Metadata Store: `LocalMetadata` (File-based JSON)

**Design:**
```
metadata.db/
├── dag_runs/{dag_id}/{run_id}.json
├── task_contexts/{dag_id}/{run_id}/{task_id}.json
├── task_states/{dag_id}/{run_id}/{task_id}.json
├── deployments/{deployment_id}.json
└── triggers/{deployment_id}/{trigger_id}.json
```

**Strengths:**
- Sharded by `dag_id` — no flat 100K-file directories
- Atomic writes via temp file + `os.replace`
- LRU cache for task states (4096 entries)
- Async I/O via `asyncio.to_thread`

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| File writes per run | ~3N files (N = tasks per DAG) | At 10K runs/hour with avg 5 tasks = **150K file writes/hour** |
| File reads per run | ~2N files (state checks) | **100K file reads/hour** |
| Directory operations | `glob()`, `iterdir()` per list operation | O(dag_runs_dir) scan on `list_dag_runs()` |
| Thread pool pressure | Every read/write uses `asyncio.to_thread` | Default thread pool may saturate |

**Critical code path** (`local_store.py:231-258`):
```python
async def get_all_task_states(self, run_id: str, dag_id: str) -> dict[str, TaskState]:
    run_dir = self._task_states_dir / dag_id / run_id
    # ... glob() + parallel reads
```

This is called by `DagRunner` on every state query. At 10K concurrent runs,
this becomes a filesystem storm.

### 1.2 Scheduler: `DeploymentScheduler`

**Design:**
- Single-process async loop
- Tick every 5 seconds
- Drains manual triggers + cron ticks
- In-memory `_in_flight` set (one run per deployment max)

**Strengths:**
- Stateless — can restart without data loss
- Simple concurrency control via `asyncio.Semaphore`

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| Cron checks per tick | All enabled deployments | `list_deployments()` reads ALL deployment files every 5s |
| Trigger drain | All pending triggers | `drain_triggers()` scans ALL trigger directories |

**Critical code path** (`scheduler.py:164-169`):
```python
for dep in await self.meta.list_deployments():
    if not dep.get("enabled", True):
        continue
    if not dep.get("cron"):
        continue
    await self._maybe_schedule(dep, now)
```

With 1,000 deployments, this is 1,000 file reads every 5 seconds = 200 reads/second just for cron checks.

### 1.3 Worker: `Worker`

**Design:**
- Async queue (`asyncio.Queue`)
- Semaphore for concurrency control (default 100)
- Per-task callbacks + retry scheduling

**Strengths:**
- No polling — event-driven via queue
- Clean separation from runner logic

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| Concurrent tasks | 100 (configurable) | May need higher for I/O-bound workloads |
| Retry scheduling | `asyncio.create_task` per retry | Unbounded task creation on mass failures |

**No critical issue here** — the worker scales with event loop capacity.

### 1.4 Runner: `DagRunner`

**Design:**
- Graph traversal with wake-on-completion
- Two-phase execution (normal → teardown)
- Branch/short-circuit propagation

**Strengths:**
- Single-pass state evaluation
- No polling — uses `asyncio.Event` for wake

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| State queries | Per-task scheduling decision | `get_all_task_states()` called in tight loop |
| Output resolution | Upstream read per downstream task | File read storm on fan-out DAGs |

**Critical code path** (`runner.py:477-505`):
```python
async def _enqueue_ready(...):
    for tid in sorted(candidate_ids):
        # ...
        dep_states = [local_states[d] for d in deps if d in local_states]
        # Falls back to metadata query for missing states
```

The `local_states` dict is an optimization, but on `resume=True` it reads from
metadata, causing file reads.

---

## 2. Scaling Analysis: 10,000 DAGs/hour

### 2.1 Throughput Requirements

| Metric | Value |
|--------|-------|
| DAG runs/hour | 10,000 |
| DAG runs/second | 2.78 |
| Avg tasks per DAG | 5 (assumed) |
| Task executions/second | 13.9 |
| File writes/second | ~42 (3 files per task) |
| File reads/second | ~28 (2 reads per task) |

### 2.2 Filesystem Limits

Modern SSDs can handle 10K+ IOPS. However:

| Issue | Impact |
|-------|--------|
| Directory entry cache | `glob()` and `iterdir()` are O(n) in entries |
| Metadata updates | `utimens` calls on every file write |
| Thread pool queueing | `asyncio.to_thread` has limited workers (default: `min(32, os.cpu_count() + 4)`) |

**Estimated saturation point:** ~3,000-5,000 DAGs/hour on single-node file-based store.

### 2.3 Memory Pressure

| Component | Memory per run | At 100 concurrent runs |
|-----------|----------------|------------------------|
| `TaskContext` | ~2KB per task | ~1MB (5 tasks × 100 runs) |
| State cache | 4096 entries × ~100 bytes | ~400KB |
| `_active_runs` index | ~100 bytes per run | ~10KB |
| DAG definitions | Loaded once | ~1-5MB total |

**Memory is NOT a bottleneck** — Beacon is memory-efficient.

### 2.4 CPU Pressure

| Operation | CPU cost |
|-----------|----------|
| JSON serialize/deserialize | Moderate (Pydantic validation) |
| Jinja rendering | Low (cached templates) |
| Graph traversal | Negligible |
| Plugin execution | Depends on plugin |

**CPU is NOT a bottleneck** for orchestration — plugin work dominates.

---

## 3. Recommendations for 10K DAGs/hour

### 3.1 Phase 1 Improvements (File-based, quick wins)

1. **Batch state queries**: `get_all_task_states()` is already batched, but
   `get_task_state()` does individual reads. Ensure runner uses batch path.

2. **Increase worker concurrency**:
   ```python
   Worker(meta, max_concurrent=500)  # Default 100 may be too low
   ```

3. **Tune thread pool**:
   ```python
   import asyncio
   asyncio.get_running_loop().set_default_executor(
       concurrent.futures.ThreadPoolExecutor(max_workers=64)
   )
   ```

4. **Add deployment cache**:
   ```python
   # In LocalMetadata
   self._deployment_cache: dict[str, dict] = {}
   ```
   Cache deployment reads for the scheduler's tick loop.

5. **Add trigger index**:
   ```python
   # In LocalMetadata
   self._pending_trigger_count: dict[str, int] = {}  # deployment_id -> count
   ```
   Skip `drain_triggers()` directory scan when count is 0.

### 3.2 Phase 2 Requirements (SQLite backend)

**Required for 10K DAGs/hour:**

```python
class SqliteMetadata:
    """SQLite backend with WAL mode."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
```

**Why SQLite works:**
- Single database file — no directory scanning
- Built-in query cache
- WAL mode allows concurrent reads
- Indexes on `(run_id, dag_id, task_id)` for fast lookups

**Estimated capacity:** 10,000-50,000 DAGs/hour on SQLite with proper tuning.

### 3.3 Phase 3 Requirements (Distributed)

**Required for >50K DAGs/hour or multi-node:**

| Component | Technology |
|-----------|------------|
| Metadata | PostgreSQL |
| Queue | Redis / SQS |
| Scheduler | Single leader (singleton) |
| Workers | N replicas (horizontal scale) |
| Bundle sync | Git-based with CI/CD |

---

## 4. Specific Code Issues Found

### 4.1 Potential deadlock in retry scheduling

**File:** `worker.py:219-221`
```python
retry_task = asyncio.create_task(self._schedule_retry(msg, delay))
self._tasks.add(retry_task)
retry_task.add_done_callback(self._tasks.discard)
```

If `_schedule_retry` raises before the sleep, the task completes but the
message is lost. Should wrap in try/except.

### 4.2 Missing index on triggers

**File:** `local_store.py:375-400`
```python
async def drain_triggers(self, deployment_id: str | None = None):
    if deployment_id is not None:
        dirs = [self._triggers_dir / deployment_id]
    else:
        dirs = [d for d in self._triggers_dir.iterdir() if d.is_dir()]
```

When `deployment_id=None`, this scans ALL deployment trigger directories.
At 1,000 deployments, this is 1,000 directory checks per tick.

**Fix:** Maintain an in-memory index of pending triggers.

### 4.3 Deployment list on every tick

**File:** `scheduler.py:164`
```python
for dep in await self.meta.list_deployments():
```

This reads ALL deployment files every tick. Should cache.

### 4.4 No cleanup of completed runs

**File:** `local_store.py` — no method to purge old runs.

Over time, `dag_runs/` directory grows unbounded. Need a cleanup job.

---

## 5. Recommended Deployment Topology for 10K DAGs/hour

### Single-node (Phase 2 with SQLite)

```
┌─────────────────────────────────────────────────────────┐
│  Single Process (beacon serve)                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │ Scheduler   │  │ Worker      │  │ API Server  │     │
│  │ (1 coro)    │  │ (500 concur)│  │ (FastAPI)   │     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
│         └────────────────┼────────────────┘             │
│                          ▼                              │
│              ┌───────────────────────┐                  │
│              │  SQLite (WAL mode)    │                  │
│              │  64MB cache           │                  │
│              └───────────────────────┘                  │
│                          │                              │
│              ┌───────────────────────┐                  │
│              │  Logs (JSONL)         │                  │
│              │  Rotate daily         │                  │
│              └───────────────────────┘                  │
└─────────────────────────────────────────────────────────┘
```

**Configuration:**
```bash
BEACON_METADATA_PATH=/var/beacon/metadata.db
BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=100
# Worker concurrency: 500 (hardcoded or config)
# SQLite: WAL mode, 64MB cache
```

**Expected capacity:** 10,000-20,000 DAGs/hour

---

## 6. Action Items

### Immediate (Phase 1 hardening)

1. [ ] Add deployment cache to `LocalMetadata`
2. [ ] Add trigger count index to avoid unnecessary scans
3. [ ] Add `purge_old_runs(retention_days=30)` method
4. [ ] Increase default worker concurrency to 200+
5. [ ] Add retry task error handling

### Phase 2 (SQLite)

1. [ ] Implement `SqliteMetadata` with WAL mode
2. [ ] Add migration tool: JSON → SQLite
3. [ ] Benchmark with 10K concurrent runs

### Monitoring

1. [ ] Add metrics: runs/hour, task latency, queue depth
2. [ ] Add health check: metadata store response time
3. [ ] Add alert: scheduler tick lag > 2x tick_seconds

---

## 7. Conclusion

Beacon's **architecture is sound** — the async-first, stateless design scales
well. The current limitation is the **file-based metadata store**, which is
appropriate for dev/small workloads but will bottleneck at 10K DAGs/hour.

**Recommendation:**
- For <3K DAGs/hour: Phase 1 with tuning
- For 3K-20K DAGs/hour: Phase 2 with SQLite
- For >20K DAGs/hour: Phase 3 with PostgreSQL

The remote plugin feature you implemented is well-designed and does not
introduce scaling concerns — uv environment caching is efficient.
