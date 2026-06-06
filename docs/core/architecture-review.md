# Beacon Architecture Review: Production Scale (10,000 DAGs/hour)

**Date:** 2026-06-06
**Status:** Assessment for production readiness (updated with multi-instance coordination)

---

## Executive Summary

Beacon's architecture is fundamentally sound for production workloads. With the new **merged API server + scheduler with coordination**, Beacon can now scale horizontally to handle 10,000+ DAGs/hour.

**Verdict:**
- Phase 1 (single-node file-based) → **Not recommended** for 10K DAGs/hour.
- Phase 2 with coordination (current) → **Recommended** — run multiple instances with shared metadata.
- Phase 3 (Postgres) → **Required** for >50K DAGs/hour or shared-nothing scaling.

---

## 1. Current Architecture Analysis

### 1.1 Metadata Store: `LocalMetadata` (File-based JSON + Coordination)

**Design:**
```
metadata.db/
├── dag_runs/{dag_id}/{run_id}.json
├── task_contexts/{dag_id}/{run_id}/{task_id}.json
├── task_states/{dag_id}/{run_id}/{task_id}.json
├── deployments/{deployment_id}.json
├── triggers/{deployment_id}/{trigger_id}.json
└── .locks/                          ← NEW: coordination locks
    ├── scheduled_{dag_id}_{logical_date}.lock
    ├── deployment_{deployment_id}.lock
    └── trigger_{trigger_id}.lock
```

**Strengths:**
- Sharded by `dag_id` — no flat 100K-file directories
- Atomic writes via temp file + `os.replace`
- LRU cache for task states (4096 entries)
- Async I/O via `asyncio.to_thread`
- **NEW: File-based coordination** — `fcntl.flock` for multi-instance support

**Coordination Methods (NEW):**

| Method | Purpose | How It Works |
|--------|---------|--------------|
| `try_create_scheduled_run()` | Prevent duplicate runs | Lock + check (dag_id, logical_date) uniqueness |
| `try_update_scheduler_state()` | Claim scheduler tick | Lock + atomic update if newer |
| `try_claim_trigger()` | Claim manual trigger | Lock + mark with instance_id |
| `drain_triggers_with_claim()` | Drain claimed triggers | Return only triggers claimed by this instance |

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| File writes per run | ~3N files (N = tasks per DAG) | At 10K runs/hour with avg 5 tasks = **150K file writes/hour** |
| File reads per run | ~2N files (state checks) | **100K file reads/hour** |
| Lock contention | One lock per scheduled run | May cause delays under high concurrency |

**Mitigation:** Run multiple instances, each handling a subset of deployments. Coordination ensures no duplicates.

### 1.2 Scheduler: `DeploymentScheduler`

**Design:**
- Single-process async loop (now runs inside API server)
- Tick every 5 seconds
- Drains manual triggers + cron ticks
- **NEW: Coordination-aware** — uses `try_*` methods for multi-instance safety

**Strengths:**
- Stateless — can restart without data loss
- Simple concurrency control via `asyncio.Semaphore`
- **NEW: Can run multiple instances** — coordination prevents duplicate runs

**Coordination Flow:**

```text
┌─────────────────────────────────────────────────────────────────┐
│                    Scheduler Tick (Instance A)                  │
├─────────────────────────────────────────────────────────────────┤
│  1. Drain triggers with claim:                                  │
│     triggers = await meta.drain_triggers_with_claim("inst-A")   │
│     → Returns only triggers claimed by this instance            │
│                                                                  │
│  2. For each enabled deployment:                                │
│     a. Evaluate cron → logical_date                             │
│     b. claimed = await meta.try_update_scheduler_state(...)     │
│        → If False, another instance already claimed this tick   │
│     c. If claimed:                                              │
│        created = await meta.try_create_scheduled_run(...)       │
│        → If False, another instance created the run             │
└─────────────────────────────────────────────────────────────────┘
```

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| Cron checks per tick | All enabled deployments | `list_deployments()` reads ALL deployment files every 5s |
| Lock acquisition | Per scheduled run | High contention under heavy load |

**Recommendation:** Add deployment cache to avoid repeated file reads.

### 1.3 Worker: `Worker`

**Design:**
- Async queue (`asyncio.Queue`)
- Semaphore for concurrency control (default 8)
- Per-task callbacks + retry scheduling

**Strengths:**
- No polling — event-driven via queue
- Clean separation from runner logic

**Bottlenecks at 10K DAGs/hour:**

| Metric | Calculation | Issue |
|--------|-------------|-------|
| Concurrent tasks | 8 (configurable via `BEACON_SCHEDULER_MAX_CONCURRENT_RUNS`) | May need higher for I/O-bound workloads |

**Recommendation:** Increase to 100-500 for high-throughput workloads.

### 1.4 API Server: `beacon api`

**Design:**
- FastAPI application with embedded scheduler
- REST endpoints for triggers, deployments, runs
- Runs in the same process as scheduler + worker

**Strengths:**
- Single process to operate
- Horizontal scaling with coordination
- Standard REST API for integration

**Endpoints:**

| Endpoint | Purpose |
|----------|---------|
| `GET /health` | Liveness + instance_id |
| `POST /triggers` | Create manual trigger |
| `GET/POST/DELETE /deployments` | Deployment CRUD |
| `GET /runs`, `GET /runs/active` | Run inspection |

---

## 2. Multi-Instance Architecture

### 2.1 Horizontal Scaling Pattern

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                 Merged API Server + Scheduler (N instances)             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
│  │ Instance 1   │  │ Instance 2   │  │ Instance 3   │                   │
│  │ (inst-a1b2)  │  │ (inst-c3d4)  │  │ (inst-e5f6)  │                   │
│  │              │  │              │  │              │                   │
│  │ - REST API   │  │ - REST API   │  │ - REST API   │                   │
│  │ - Scheduler  │  │ - Scheduler  │  │ - Scheduler  │                   │
│  │ - Worker     │  │ - Worker     │  │ - Worker     │                   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                   │
│         │                 │                 │                            │
│         └─────────────────┼─────────────────┘                            │
│                           │                                              │
│                           ▼                                              │
│                  ┌─────────────────┐                                     │
│                  │ LocalMetadata   │                                     │
│                  │ (shared path)   │                                     │
│                  │                 │                                     │
│                  │ .locks/         │                                     │
│                  │  scheduled_*    │ → Run deduplication                 │
│                  │  deployment_*   │ → Tick coordination                │
│                  │  trigger_*      │ → Trigger claiming                 │
│                  └─────────────────┘                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Coordination Guarantees

| Scenario | What Happens |
|----------|--------------|
| Two instances try to schedule the same tick | Only one succeeds via `try_update_scheduler_state` |
| Two instances try to create the same run | Only one succeeds via `try_create_scheduled_run` |
| Two instances drain the same trigger | Only one claims it via `try_claim_trigger` |
| Instance crashes mid-operation | Lock is released, another instance can proceed |

### 2.3 Deployment Pattern

```bash
# Instance 1
beacon api ./bundle --port 8080 --instance-id inst-1 &

# Instance 2
beacon api ./bundle --port 8081 --instance-id inst-2 &

# Load balancer in front of :8080 and :8081
```

**Key insight:** All instances share the same metadata directory (NFS, shared disk, etc.)

---

## 3. Scaling Analysis: 10,000 DAGs/hour

### 3.1 Throughput Requirements

| Metric | Value |
|--------|-------|
| DAG runs/hour | 10,000 |
| DAG runs/second | 2.78 |
| Avg tasks per DAG | 5 (assumed) |
| Task executions/second | 13.9 |
| File writes/second | ~42 (3 files per task) |
| File reads/second | ~28 (2 reads per task) |
| Coordination locks/second | ~2.78 (one per scheduled run) |

### 3.2 Recommended Configuration for 10K DAGs/hour

```bash
# Per-instance configuration
BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=100
BEACON_SCHEDULER_TICK_SECONDS=5

# Run 3-5 instances
beacon api ./bundle --port 8080 --instance-id inst-1 --max-concurrent 100 &
beacon api ./bundle --port 8081 --instance-id inst-2 --max-concurrent 100 &
beacon api ./bundle --port 8082 --instance-id inst-3 --max-concurrent 100 &
```

**Expected per-instance load:**
- ~3,300 DAG runs/hour
- ~17 task executions/second
- ~50 file writes/second
- Well within filesystem capacity

### 3.3 Memory Pressure

| Component | Memory per run | At 100 concurrent runs |
|-----------|----------------|------------------------|
| `TaskContext` | ~2KB per task | ~1MB (5 tasks × 100 runs) |
| State cache | 4096 entries × ~100 bytes | ~400KB |
| `_active_runs` index | ~100 bytes per run | ~10KB |
| DAG definitions | Loaded once | ~1-5MB total |

**Memory is NOT a bottleneck** — Beacon is memory-efficient.

### 3.4 CPU Pressure

| Operation | CPU cost |
|-----------|----------|
| JSON serialize/deserialize | Moderate (Pydantic validation) |
| Jinja rendering | Low (cached templates) |
| Graph traversal | Negligible |
| Lock acquisition | Low (fcntl is fast) |
| Plugin execution | Depends on plugin |

**CPU is NOT a bottleneck** for orchestration — plugin work dominates.

---

## 4. Recommendations for 10K DAGs/hour

### 4.1 Current Implementation (with coordination)

1. **Run 3-5 instances** with shared metadata directory
2. **Increase worker concurrency** to 100+ per instance
3. **Add deployment cache** to avoid repeated file reads
4. **Monitor lock contention** — if lock wait times are high, add more instances

### 4.2 Phase 2 Improvements (SQLite backend)

**SqliteMetadata** will further improve coordination:

```python
class SqliteMetadata:
    """SQLite backend with WAL mode + UNIQUE constraints."""

    async def try_create_scheduled_run(self, ...):
        # Uses INSERT ... ON CONFLICT DO NOTHING
        await self.conn.execute("""
            INSERT INTO dag_runs (run_id, dag_id, logical_date, ...)
            VALUES ($1, $2, $3, ...)
            ON CONFLICT (dag_id, logical_date) DO NOTHING
        """)
```

**Benefits over file locks:**
- Faster coordination (no separate lock files)
- Database-level uniqueness enforcement
- WAL mode allows concurrent reads

### 4.3 Phase 3 Requirements (Postgres)

**Required for >50K DAGs/hour or multi-node:**

| Component | Technology |
|-----------|------------|
| Metadata | PostgreSQL with `UNIQUE` constraints |
| Queue | Redis / SQS (optional, for task distribution) |
| Bundle sync | Git-based with CI/CD |

---

## 5. Specific Code Issues Found

### 5.1 Deployment list on every tick (still present)

**File:** `scheduler.py:_tick()`
```python
for dep in await self.meta.list_deployments():
```

This reads ALL deployment files every tick. Should cache.

**Fix:** Add in-memory deployment cache with TTL.

### 5.2 No cleanup of completed runs

**File:** `local_store.py` — no method to purge old runs.

Over time, `dag_runs/` directory grows unbounded. Need a cleanup job.

**Fix:** Add `beacon gc --keep-days N` command.

### 5.3 Lock files not cleaned up

**File:** `local_store.py:.locks/`

Lock files are created but not cleaned up after use. Over time, this
could accumulate many small files.

**Fix:** Remove lock files after successful operation (optional, not
required for correctness).

---

## 6. Recommended Deployment Topology for 10K DAGs/hour

### Multi-instance with shared metadata (current)

```
┌─────────────────────────────────────────────────────────────────┐
│  Load Balancer (nginx / ALB / GCLB)                             │
│  Routes to :8080, :8081, :8082                                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Instance 1   │  │ Instance 2   │  │ Instance 3   │
│ API + Sched  │  │ API + Sched  │  │ API + Sched  │
│ Worker: 100  │  │ Worker: 100  │  │ Worker: 100  │
│ Port 8080    │  │ Port 8081    │  │ Port 8082    │
└──────┬───────┘  └──────┬───────┘  └──────┬───────┘
       └─────────────────┼─────────────────┘
                         │
                         ▼
              ┌───────────────────────┐
              │  Shared Filesystem    │
              │  (NFS / EFS / etc.)   │
              │                       │
              │  metadata.db/         │
              │    dag_runs/          │
              │    .locks/            │
              └───────────────────────┘
```

**Configuration:**
```bash
# Per instance
BEACON_METADATA_PATH=/shared/beacon/metadata.db
BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=100
BEACON_LOG_DIR=/shared/beacon/logs
```

**Expected capacity:** 10,000-20,000 DAGs/hour

---

## 7. Action Items

### Immediate (Phase 2 hardening with coordination)

1. [x] Multi-instance coordination via file locks
2. [x] `try_create_scheduled_run` for run deduplication
3. [x] `try_update_scheduler_state` for tick coordination
4. [x] `drain_triggers_with_claim` for trigger coordination
5. [x] Tests: 10 coordination tests + 11 multi-scheduler tests
6. [ ] Add deployment cache to `LocalMetadata`
7. [ ] Add `purge_old_runs(retention_days=30)` method
8. [ ] Increase default worker concurrency to 100+
9. [ ] Add retry task error handling

### Phase 2 (SQLite)

1. [ ] Implement `SqliteMetadata` with WAL mode
2. [ ] Add `UNIQUE` constraints for coordination
3. [ ] Add migration tool: JSON → SQLite
4. [ ] Benchmark with 10K concurrent runs

### Monitoring

1. [ ] Add metrics: runs/hour, task latency, queue depth
2. [ ] Add health check: metadata store response time
3. [ ] Add alert: scheduler tick lag > 2x tick_seconds
4. [ ] Add metrics: lock wait time, coordination failures

---

## 8. Conclusion

Beacon's **architecture is sound and now supports horizontal scaling**.
The merged API server + scheduler with coordination enables multiple
instances to run concurrently without duplicate runs.

**Recommendation:**
- For <5K DAGs/hour: Single instance with coordination
- For 5K-20K DAGs/hour: 3-5 instances with shared metadata (current)
- For >20K DAGs/hour: SqliteMetadata or PostgresMetadata with UNIQUE constraints

The coordination primitives (`try_create_scheduled_run`,
`try_update_scheduler_state`, `try_claim_trigger`) are the key
enablers for horizontal scaling. They work with `LocalMetadata` (file
locks) and will work with `SqliteMetadata`/`PostgresMetadata` (UNIQUE
constraints) without code changes to the scheduler.
