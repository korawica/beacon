# Beacon Roadmap — Path to v1.0

Single source for: positioning, non-goals, exit criteria, status of
every deliverable, ordered work with Definition of Done. Design
rationale lives in [`reference.md`](./reference.md). Bundle / deploy
mechanics in [`deploy.md`](./deploy.md).

**Target scale:** 10,000 DAG runs/hour, 50,000+ task executions/hour

Status legend: ✅ Done · 🟡 In progress · ⬜ Pending · 🚫 Cut (non-goal)

---

## 1. Positioning

Beacon is for teams that need production-grade workflow orchestration
without Apache Airflow's operational complexity. The promise:

- `pip install beacon` → working scheduler in 60 seconds
- One YAML or Python file → a deployed DAG
- Horizontal scaling with automatic coordination (no duplicate runs)
- Async-first; sensors don't waste worker slots
- Single process to start, multiple for scale

---

## 2. Non-Goals (the discipline)

Things beacon will **never** ship in core:

| Non-goal                                 | Why                                  | If you need it        |
|------------------------------------------|--------------------------------------|-----------------------|
| Web UI in v1                             | API + CLI cover ops workflows        | Phase 3               |
| TOML/YAML config file                    | 10 env vars is manageable            | `BEACON_*` env vars   |
| Built-in git auto-pull                   | 2-line systemd timer does it         | Systemd / cron / CI   |
| Secrets adapter                          | `os.environ.get()` works             | Platform secret store |
| RBAC                                     | Re-implements auth proxies           | oauth2-proxy / IAP    |
| OIDC in v1                               | Basic + bearer covers 90%            | Phase 3               |
| Audit table                              | Log lines at action sites are enough | grep logs             |
| DAG editor UI                            | DAGs are code                        | Use IDE + git         |
| Plugin marketplace                       | Quality bar collapses                | Write custom plugins  |
| Dynamic task mapping at arbitrary points | Graph becomes unpredictable          | `foreach_task` only   |
| XCom-style data shuttle                  | Encourages tight coupling            | Return small dicts    |
| SLA misses + priority weights            | Airflow complexity                   | Cancel late runs      |

---

## 3. v1.0 Exit Criteria

A team can do all of the following without our help:

1. **Deploy** — `beacon deploy` registers DAGs + deployments from bundle
2. **Persist** — Metadata in SQLite survives restart with no data loss
3. **Schedule** — DAGs run on cron, catchup honored, timezones honored
4. **Scale** — Multiple `beacon api` instances coordinate, no duplicate runs
5. **Observe** — `/health`, `/metrics`, structured logs, `beacon logs` CLI
6. **Configure** — All knobs are `BEACON_*` env vars, no config file
7. **Recover** — `kill -9` restarts cleanly; in-flight tasks recovered
8. **Self-heal** — Stuck tasks detected; logs rotated; metadata GC'd
9. **Secure** — Basic auth or bearer token; rate limiting; TLS ready
10. **Test locally** — `dag.plan()`, `dag.test()`, `dag.run()` match production

---

## 4. Status Snapshot

### Phase 0 — Core Loop ✅

| Deliverable                           | Status  |
|---------------------------------------|---------|
| `PythonPlugin.execute()` end-to-end   | ✅       |
| TaskContext + Attempt + LocalExecutor | ✅       |
| Bundle plugin auto-discovery          | ✅       |
| `load_context()` for user Python      | ✅       |
| Task state machine                    | ✅       |

### Phase 1 — Library Complete ✅

| Deliverable                                   | Status  |
|-----------------------------------------------|---------|
| Callback system with registry                 | ✅       |
| Async worker with retry                       | ✅       |
| `MetadataProtocol` + `LocalMetadata`          | ✅       |
| `TaskFailed` / `TaskSkipped` exceptions       | ✅       |
| `Deployment` model (reusable DAG)             | ✅       |
| `Dag.run()` / `test()` / `plan()`             | ✅       |
| `DagRunner` (trigger rules, branch, teardown) | ✅       |
| Setup & Teardown                              | ✅       |
| Jinja renderer (sandboxed)                    | ✅       |

### Phase 1.5 — Local-First ✅

| Deliverable                 | Status   |
|-----------------------------|----------|
| Structured logging pipeline | ✅        |
| `beacon` CLI (11 commands)  | ✅        |

### Phase 2 — Production Deployment

| #    | Deliverable                                     | Status  | Priority     |
|------|-------------------------------------------------|---------|--------------|
| 2.1  | `SqliteMetadata` (WAL, coordination via UNIQUE) | ⬜       | **Critical** |
| 2.2  | Deployment Scheduler (cron, catchup)            | ✅       | —            |
| 2.3  | `LocalBundle` sync                              | 🟡      | High         |
| 2.4  | `beacon api` (merged scheduler + API)           | ✅       | —            |
| 2.5  | Multi-instance coordination                     | ✅       | —            |
| 2.6  | Stuck-task detector                             | ⬜       | **Critical** |
| 2.7  | Log rotation                                    | ⬜       | **Critical** |
| 2.8  | Metadata GC                                     | ⬜       | **Critical** |
| 2.9  | Prometheus metrics (8 metrics)                  | ⬜       | **Critical** |
| 2.10 | API rate limiting                               | ⬜       | High         |
| 2.11 | Auth (basic + bearer)                           | ⬜       | High         |
| 2.12 | Bulk operations API                             | ⬜       | Medium       |
| 2.13 | Deployment caching                              | ⬜       | Medium       |

### Phase 3 — Scale-Out (gated on real users)

| #   | Deliverable                                      | Status   | Priority   |
|-----|--------------------------------------------------|----------|------------|
| 3.1 | `PostgresMetadata` (asyncpg, UNIQUE constraints) | ⬜        | High       |
| 3.2 | Redis queue (optional, for distributed workers)  | ⬜        | Medium     |
| 3.3 | OIDC at API                                      | ⬜        | Medium     |
| 3.4 | Web UI v1 (read-mostly)                          | ⬜        | Low        |
| 3.5 | `DockerExecutor`                                 | ⬜        | Low        |
| 3.6 | `KubernetesExecutor`                             | ⬜        | Low        |
| 3.7 | `foreach_task` action                            | ⬜        | Low        |

---

## 5. Phase 2 — Detailed DoD

### 5.1 — `SqliteMetadata` ⬜ Critical

**Why critical:** File-based metadata bottlenecks at scale. SQLite with WAL
mode handles 10K DAGs/hour easily.

**Schema:**
```sql
CREATE TABLE dag_runs (
    run_id TEXT PRIMARY KEY,
    dag_id TEXT NOT NULL,
    logical_date TEXT,
    state TEXT NOT NULL,
    ...
    UNIQUE (dag_id, logical_date)  -- Coordination!
);

CREATE TABLE task_states (
    run_id TEXT,
    dag_id TEXT,
    task_id TEXT,
    state TEXT NOT NULL,
    heartbeat_at TEXT,
    PRIMARY KEY (run_id, dag_id, task_id)
);
```

**DoD:**
- [ ] Implements `MetadataProtocol` 1:1
- [ ] WAL mode, `synchronous=NORMAL`
- [ ] `UNIQUE` constraints for coordination (replaces file locks)
- [ ] Migration: `LocalMetadata` → `SqliteMetadata`
- [ ] Bench: 10K runs/hour sustained on laptop

### 5.2 — Deployment Scheduler ✅

Already shipped. See `beacon/scheduler.py`.

### 5.3 — `LocalBundle` Sync 🟡

**DoD:**
- [x] `beacon sync PATH` re-reads bundle atomically
- [x] `dag_version` from content hash
- [x] Plugin re-registration idempotent
- [ ] `POST /sync` API endpoint
- [ ] Tests: version coexistence with in-flight runs

### 5.4 — `beacon api` ✅

Already shipped. See `beacon/api/`.

```bash
beacon api ./bundle --port 8080 --instance-id inst-1
```

### 5.5 — Multi-Instance Coordination ✅

Already shipped. Coordination methods:

| Method                         | Purpose                |
|--------------------------------|------------------------|
| `try_create_scheduled_run()`   | Prevent duplicate runs |
| `try_update_scheduler_state()` | Claim scheduler tick   |
| `drain_triggers_with_claim()`  | Claim triggers         |

### 5.6 — Stuck-Task Detector ⬜ Critical

**Why critical:** At 10K DAGs/hour, stuck tasks will happen. Detection prevents
pipeline lockup.

**DoD:**
- [ ] Background scan every 60s for `RUNNING` tasks
- [ ] No heartbeat for `stuck_after_seconds` → FAILED
- [ ] Metric: `beacon_tasks_marked_stuck_total`
- [ ] Configurable via `BEACON_STUCK_AFTER_SECONDS`

### 5.7 — Log Rotation ⬜ Critical

**Why critical:** At 50K task executions/hour, logs grow fast. Rotation prevents
disk fill.

**DoD:**
- [ ] `BEACON_LOG_MAX_FILE_MB` (default 50)
- [ ] `BEACON_LOG_MAX_AGE_DAYS` (default 30)
- [ ] Rotate to `.1`, `.2`, ... keep N backups
- [ ] Background sweep deletes old files

### 5.8 — Metadata GC ⬜ Critical

**Why critical:** Completed runs accumulate. GC prevents unbounded growth.

**DoD:**
- [ ] `beacon gc --keep-days N [--dry-run]`
- [ ] Deletes terminal-state runs older than N days
- [ ] Never deletes non-terminal runs
- [ ] Systemd timer recipe in docs

### 5.9 — Prometheus Metrics ⬜ Critical

**Why critical:** Can't operate at scale without visibility.

**Metrics (bounded cardinality):**
```text
beacon_scheduler_heartbeat_timestamp
beacon_dag_runs_total{state}
beacon_task_attempts_total{state}
beacon_task_attempt_duration_seconds
beacon_queue_depth
beacon_worker_concurrent
beacon_tasks_stuck_total
beacon_coordination_conflicts_total
```

**DoD:**
- [ ] `/metrics` endpoint
- [ ] No per-dag-id, per-run-id labels (cardinality bomb)
- [ ] Grafana dashboard in `docs/operations/`

### 5.10 — API Rate Limiting ⬜ High

**Why needed:** Prevent API abuse at scale.

**DoD:**
- [ ] In-memory token bucket per endpoint
- [ ] Configurable: `BEACON_API_RATE_LIMIT` (default: 100/min)
- [ ] Apply to `/triggers`, `/clear`, `/mark`, `/cancel`
- [ ] Return `429 Too Many Requests`

### 5.11 — Auth (Basic + Bearer) ⬜ High

**DoD:**
- [ ] `BEACON_API_AUTH=basic` or `bearer`
- [ ] `BEACON_API_USERS=user:pass,user2:pass2` (basic)
- [ ] `BEACON_API_TOKEN=secret` (bearer)
- [ ] Optional (no auth if not configured)

### 5.12 — Bulk Operations API ⬜ Medium

**Why useful:** At 10K DAGs, bulk operations save time.

**New endpoints:**
```text
POST /triggers/bulk          # Trigger multiple deployments
POST /runs/cancel-bulk       # Cancel multiple runs
POST /deployments/enable-bulk # Enable/disable multiple
```

**DoD:**
- [ ] Accept `deployment_ids: [...]` array
- [ ] Return per-item results (success/failure)
- [ ] Max 100 items per request

### 5.13 — Deployment Caching ⬜ Medium

**Why needed:** `list_deployments()` reads all files every tick. Cache prevents
filesystem storm.

**DoD:**
- [ ] In-memory cache with TTL (default 30s)
- [ ] Invalidate on `upsert_deployment`
- [ ] Metric: `beacon_deployment_cache_hits`

---

## 6. Phase 3 — Scale-Out

**Start only after 3+ teams running Phase 2 in production.**

### 6.1 — `PostgresMetadata` ⬜

**DoD:**
- [ ] `asyncpg` connection pool
- [ ] `UNIQUE` constraints for coordination
- [ ] Migration: `SqliteMetadata` → `PostgresMetadata`
- [ ] Bench: 1000 task transitions/s with 5 workers

### 6.2 — Redis Queue ⬜

**Why optional:** For distributed workers across nodes.

**DoD:**
- [ ] `RedisQueue` implements same interface as `asyncio.Queue`
- [ ] `BEACON_QUEUE_TYPE=redis` + `BEACON_REDIS_URL`
- [ ] Workers pull from shared queue

### 6.3 — OIDC at API ⬜

**DoD:**
- [ ] `authlib` or `python-jose`
- [ ] Config: issuer, audience, JWKS
- [ ] Coexists with basic/bearer

### 6.4 — Web UI v1 ⬜

**Pages:**
1. Deployments list → runs
2. Run detail → task graph
3. Task detail → logs

**DoD:**
- [ ] React + TanStack Query
- [ ] ≤300 KB gzipped
- [ ] Served by API at `/ui/*`

### 6.5 — `DockerExecutor` ⬜

**DoD:**
- [ ] Per-task `image:` configurable
- [ ] `TaskContext` via `BEACON_TASK_CONTEXT` env var
- [ ] Container logs to unified pipeline
- [ ] Cleanup on cancel

### 6.6 — `KubernetesExecutor` ⬜

**DoD:**
- [ ] Pod spec configurable
- [ ] Watch pod status async
- [ ] No CRDs, no operator pattern

### 6.7 — `foreach_task` ⬜

**DoD:**
- [ ] `for_each: "{{ params.items }}"` at trigger time
- [ ] N task instances, each with own `TaskContext`
- [ ] Downstream receives list of outputs

---

## 7. Removed / Deferred

| Item                   | Reason                              | Alternative        |
|------------------------|-------------------------------------|--------------------|
| `GitBundle` auto-pull  | 2-line systemd timer is simpler     | Systemd timer      |
| TOML config file       | 10 env vars is manageable           | Env vars           |
| Secrets adapter        | `os.environ.get()` works            | Platform secrets   |
| Audit table            | Log lines are enough                | grep logs          |
| DAG editor UI          | DAGs are code                       | IDE + git          |
| Connection/Variable UI | Couples secrets to UI               | Env vars           |
| SLA misses + priority  | Airflow complexity                  | Cancel late runs   |
| Multi-tenant scheduler | One instance per team               | Multiple instances |
| Distributed tracing    | Nice to have, not critical          | Structured logs    |
| Run prioritization     | Complexity > value at current scale | Queue ordering     |

---

## 8. Scaling Guide

### <5K DAGs/hour

```bash
# Single instance
beacon api ./bundle --port 8080 --max-concurrent 50
```

### 5K-20K DAGs/hour

```bash
# 3 instances, shared metadata
beacon api ./bundle --port 8080 --instance-id inst-1 --max-concurrent 100 &
beacon api ./bundle --port 8081 --instance-id inst-2 --max-concurrent 100 &
beacon api ./bundle --port 8082 --instance-id inst-3 --max-concurrent 100 &

# Shared filesystem for metadata
BEACON_METADATA_PATH=/shared/beacon/metadata.db
```

### 20K+ DAGs/hour

```bash
# Postgres metadata + Redis queue
BEACON_METADATA_TYPE=postgres
BEACON_POSTGRES_URL=postgres://...
BEACON_QUEUE_TYPE=redis
BEACON_REDIS_URL=redis://...
```

---

## 9. Testing Standards

| Layer        | Proves                 |
|--------------|------------------------|
| Unit         | Pure logic correctness |
| Functional   | Component integration  |
| e2e          | Full DAG execution     |
| Coordination | Multi-instance races   |
| Bench        | Throughput regressions |

**Rule:** Every feature ships with tests.

---

## 10. Performance Budgets

| Metric            | Target                         |
|-------------------|--------------------------------|
| DAG parse         | ≤50ms for 100 tasks            |
| Scheduler tick    | ≤100ms with 1000 active runs   |
| Task transition   | ≤50ms (excluding plugin)       |
| Coordination lock | ≤10ms                          |
| Worker memory     | ≤200MB at 200 concurrent tasks |

---

## 11. What "Done" Looks Like

At v1.0, a data engineer can:

1. Write a `dag.yml`, drop it in a repo
2. Run `beacon sync` from systemd timer
3. See deployments trigger on schedule
4. Run 3-5 instances for horizontal scale (no duplicates)
5. Hit API to pause/trigger/cancel runs
6. Stream per-attempt logs with `beacon logs`
7. Monitor via `/metrics` + Grafana
8. Trust that stuck tasks are detected, logs rotated, old runs GC'd

**All without a UI, config file, or Airflow concepts.**
