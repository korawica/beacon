# Beacon Roadmap — Status + Path to v1.0

Single source for: positioning, non-goals, exit criteria, status of
every deliverable, ordered work with Definition of Done. Design
rationale lives in [`reference.md`](./reference.md). Bundle / deploy
mechanics in [`deploy.md`](./deploy.md).

Status legend: ✅ Done · 🟡 In progress · ⬜ Pending · 🚫 Cut (non-goal)

---

## 1. Positioning

Beacon is for the **80% of teams that need a workflow orchestrator but
don't need Apache Airflow**. The promise:

- `pip install beacon` → working scheduler in 60 seconds.
- One YAML or Python file → a deployed DAG.
- One process to operate. One config surface. One log pipeline.
- Async-first; sensors don't waste worker slots.
- **Horizontally scalable** — run multiple API instances with automatic
  coordination to prevent duplicate runs.

If a team needs multi-tenant scheduler HA, RBAC, lineage, datasets,
dynamic task mapping in arbitrary places, an editor UI, or a
200-provider ecosystem — **use Airflow**. We will not chase parity.

---

## 2. Non-Goals (the discipline)

Things beacon will **never** ship in core. These are the lines that
turn a lean tool into another Airflow.

| Non-goal                                            | Why                                                                      | If you need it                          |
|-----------------------------------------------------|--------------------------------------------------------------------------|-----------------------------------------|
| Multi-tenant scheduler isolation                    | Adds RBAC + namespace complexity; one beacon instance per team is enough | Run multiple beacon instances per team  |
| Web UI in v1                                        | Huge scope; API + CLI cover every must-have ops workflow                 | Use the API; UI is Phase 3              |
| TOML / YAML config file in v1                       | ~10 env vars is manageable                                               | `BEACON_*` env vars                     |
| Built-in git auto-pull (`GitBundle`)                | 2 lines of `git pull && beacon sync` in systemd does it better           | Systemd timer / cron / your CI          |
| Secrets adapter (`SecretsProvider`)                 | `os.environ.get()` in user code works today; zero customers asking       | Env vars + your platform's secret store |
| Connection / Variable editor UI                     | Couples secrets to the UI; security risk                                 | Env vars or external secret manager     |
| DAG editor UI                                       | DAGs are code; edit in your IDE + git                                    | Use git + `beacon sync`                 |
| RBAC with row-level permissions                     | Re-implements auth proxies                                               | Front beacon with oauth2-proxy / IAP    |
| Audit table for admin actions                       | Log lines at action sites are enough                                     | grep the audit log lines                |
| OIDC auth in v1                                     | Basic + bearer-token covers 90%; OIDC is Phase 3                         | Use basic-auth or bearer-token in v1    |
| Email / Slack / PagerDuty built-in                  | One-off plugins; not core                                                | Write a 30-line callback plugin         |
| Lineage / Datasets / OpenLineage emission           | Cross-system coupling, slow-moving spec                                  | Emit events from a callback if needed   |
| Triggerer / deferrable operators                    | Async-first eliminates the need                                          | n/a — already solved                    |
| Plugin marketplace with hundreds of providers       | Quality bar collapses; we curate ~10 standard ones                       | Write a custom plugin (one class)       |
| SLA misses + DAG-level pools + priority weights     | Airflow-style scheduler complexity for ambiguous wins                    | Cancel late runs from a callback        |
| TaskGroups with arbitrary nesting visualization     | UI complexity > value                                                    | Use flat DAGs or one-level Groups       |
| XCom-style cross-task data shuttle                  | Encourages tight coupling                                                | Return small dicts; pass big-data refs  |
| `BashOperator`-style implicit shell exec primitives | Footgun                                                                  | Write a `py` task that calls subprocess |
| Full REST API parity with Airflow                   | Surface area we can't maintain                                           | Focused, documented endpoints only      |

**Rule of thumb:** every feature must justify itself with *what
use-case breaks without it?* If the answer is "people might want it
someday", the answer is **no**.

---

## 3. v1.0 Exit Criteria ("production-ready")

A team can do all of the following without our help:

1. **Deploy from a checkout** — `beacon deploy /path/to/repo` registers
   DAGs + deployments from a `LocalBundle`. Teams put
   `git pull && beacon sync` in systemd/cron — built-in git polling is
   **not** v1. `--var` pins a deployment to its current `dag_version`;
   non-pinned deployments auto-roll on sync.
2. **Persist beyond restart** — metadata in SQLite survives full
   process restart with no data loss.
3. **Schedule by cron** — DAGs run on schedule, catchup honored,
   timezones honored, missed runs visible.
4. **Operate one process** — `beacon api` runs scheduler + worker(s)
   + API server. Graceful shutdown drains in-flight tasks. **Multiple
   instances can run with automatic coordination.**
5. **Observe** — `/health`, `/metrics` (Prometheus, ~5 metrics),
   structured logs in one pipeline (✅ done), `beacon logs` CLI
   streams per-attempt JSONL.
6. **Configure via env vars** — all knobs are `BEACON_*` env vars. No
   config file in v1. No DB-stored config ever.
7. **Recover** — `kill -9` the process; restart; in-flight tasks are
   correctly marked failed or resumed. Ops can cancel a runaway task
   and mark a stuck task as success/skipped/failed via the API.
8. **Self-heal** — stuck-task detector flips zombie tasks; log
   rotation keeps disks from filling; metadata GC keeps the DB from
   growing forever.
9. **Test locally** — `dag.plan()`, `dag.test()`, `dag.run()` work
   identically to production (✅ done).
10. **Auth** — basic auth or static bearer-token at the API. Nothing
    fancy. (OIDC is post-v1.)
11. **Docs cover the happy path** — install → first deployment →
    monitoring → upgrade, in under 30 minutes of reading.
12. **Scale horizontally** — run multiple `beacon api` instances with
    coordination via metadata store to prevent duplicate runs.

If any point above is **not** met, beacon is not v1.0.

**Explicitly NOT in v1:** web UI, git auto-pull, OIDC, TOML config,
secrets adapter, audit log.

---

## 4. Status Snapshot

### Phase 0 — Core loop (foundation)

| Deliverable                                       | Status  | Validates                       |
|---------------------------------------------------|---------|---------------------------------|
| `PythonPlugin.execute()` end-to-end               | ✅       | Core loop                       |
| TaskContext + Attempt + LocalExecutor             | ✅       | State persistence + retry       |
| Bundle plugin auto-discovery (`./plugins/`)       | ✅       | Custom plugin deployment        |
| `load_context()` for user Python files            | ✅       | Runtime access without coupling |
| Task state machine with valid transitions         | ✅       | Enforced lifecycle              |

### Phase 1 — Library complete

| Deliverable                                                                                | Status   | Validates                                            |
|--------------------------------------------------------------------------------------------|----------|------------------------------------------------------|
| Callback system with registry resolution                                                   | ✅        | Callback parity Python/YAML                          |
| Async worker with retry scheduling                                                         | ✅        | Full lifecycle transitions                           |
| `MetadataProtocol` + `LocalMetadata` (sharded)                                             | ✅        | Pluggable persistence, 1000+ DAGs                    |
| `TaskFailed` / `TaskSkipped` exception support                                             | ✅        | Plugin-driven retry/skip control                     |
| `Deployment` model (reusable DAG + per-env config)                                         | ✅        | DAG reuse without duplication                        |
| `Dag.run()` / `Dag.test()` / `Dag.plan()` methods                                          | ✅        | Developer workflow (validate → test → run)           |
| `DagRunner` (trigger rules, branch, teardown, DAG callbacks; renamed from `LocalScheduler`) | ✅        | Full DAG lifecycle in-process                        |
| `DagRunner.clear()` + `resume=True` + `Dag.clear()` (backfill / fix-and-rerun)             | ✅        | Clear past task, re-execute keeping upstream outputs |
| Setup & Teardown (`teardown` field + scheduler)                                            | ✅        | Cluster/staging lifecycle                            |
| Lean Jinja renderer (SandboxedEnvironment only)                                            | ✅        | Two-pass: trigger-time + execute-time                |

### Phase 1.5 — Close the loop on local-first

| #     | Deliverable                                                                              | Status | DoD § |
|-------|------------------------------------------------------------------------------------------|--------|-------|
| 1.5.1 | Structured logging pipeline (unified, batched, JSONL, file/memory sinks)                 | ✅      | §5.1  |
| 1.5.2 | `beacon` CLI (`plan`, `test`, `run`, `deploy`, `sync`, `list`, `logs`, `config show`, …) | ✅      | §5.2  |

### Phase 2 — Production deployment

| #    | Deliverable                                                                                 | Status | DoD § |
|------|---------------------------------------------------------------------------------------------|--------|-------|
| 2.1  | `SqliteMetadata` (default for `beacon api`, WAL, single writer)                             | ⬜      | §5.3  |
| 2.2  | Deployment Scheduler (cron, catchup, timezone, backfill)                                    | ✅      | §5.4  |
| 2.3  | `LocalBundle` sync (`beacon sync` CLI + `POST /sync`, content-hash version)                 | 🟡     | §5.5  |
| 2.3a | Bundle policy — scoped variables, asset resolution, pinned deployments, `beacon deployment` | ✅      | §5.6  |
| 2.4  | `beacon api` process model (scheduler + worker + api, SIGTERM)                              | ✅      | §5.7  |
| 2.5  | API server — 14 endpoints incl. cancel / mark-state / clear (basic + bearer auth)           | ✅      | §5.8  |
| 2.6  | Multi-instance coordination (file locks, no duplicate runs)                                 | ✅      | §5.9  |
| 2.7  | Operational self-healing — stuck-task detector + log rotation + metadata GC                 | ⬜      | §5.10 |
| 2.8  | Prometheus metrics (8 metrics, bounded cardinality, dashboard)                              | ⬜      | §5.11 |

### Phase 3 — Scale-out & UI (gated on real users)

**Do not start until at least 3 teams are running Phase 2 in production.**

| #   | Deliverable                                              | Status | DoD § |
|-----|----------------------------------------------------------|--------|-------|
| 3.1 | `PostgresMetadata` (asyncpg, coordination via UNIQUE)    | ⬜      | §6.1  |
| 3.2 | `DockerExecutor` (per-task image, env-var TaskContext)   | ⬜      | §6.2  |
| 3.3 | `KubernetesExecutor` (deferred, official client only)    | ⬜      | §6.3  |
| 3.4 | OIDC at the API (alongside basic + bearer)               | ⬜      | §6.5  |
| 3.5 | Web UI v1 — read-mostly SPA (≤300 KB gzipped, no editor) | ⬜      | §6.6  |

### Phase 4 — Ecosystem (conditional)

**Each item requires a committed maintainer before work starts.**

| Deliverable                                            | Status | Gate                            |
|--------------------------------------------------------|--------|---------------------------------|
| Remote plugin registry (`org/name@version` resolution) | ⬜      | Multiple orgs building plugins  |
| `BatchExecutor` (AWS Batch / Cloud Batch)              | ⬜      | Concrete need beyond Docker/K8s |
| `GcsLogSink`                                           | ⬜      | First user request (~50 LOC)    |
| `ElasticsearchLogSink`                                 | ⬜      | First user request (~50 LOC)    |

### v1.0 ships when

All Phase 1.5 + Phase 2 rows are ✅ **AND** every item in §3 is
demonstrable end-to-end on a fresh machine. Phase 3 and 4 are post-v1
and only proceed on demand.

---

## 5. Phase 1.5 + Phase 2 — Detailed DoD

### 5.1 — Structured logging pipeline ✅

Already shipped. See `beacon/logging.py`.

### 5.2 — `beacon` CLI (entry point) ✅

A single CLI replaces ad-hoc Python scripts.

```text
beacon plan  PATH
beacon test    PATH [--params k=v ...]
beacon run     PATH [--params k=v ...]
beacon deploy  PATH [--cron ...] [--params ...] [--var k=v ...]
                                                 # --var pins to dag_version
beacon deployment sync DEPLOYMENT_ID|--all --bundle PATH
beacon deployment diff DEPLOYMENT_ID --bundle PATH
beacon sync    PATH                              # auto-rolls non-pinned
beacon list    [dags|deployments|runs]           # `deployments` shows version + [pinned]/[stale]
beacon logs    DAG_ID TASK_ID [--run RUN_ID | --logical-date YYYY-MM-DD]
beacon api     [--port PORT] [--instance-id ID]  # merged API + scheduler
beacon config  show
```

**DoD checklist:**
- [x] `beacon --help` lists all 12 commands with consistent flag style
      (`--metadata-path`, `--dag-id`, `--param`, `--var`, `--bundle`).
- [x] All commands non-zero on failure, stable exit-code contract
      documented in `beacon/cli/main.py`: `0` success, `1`
      runtime/operational failure, `2` invocation error
      (click `UsageError`). Enforced by `tests/functional/test_cli_*`
      and `tests/e2e/test_cli_entry_point.py`.
- [x] **Decision: stay on click, not typer.** Stylistic rewrite, zero
      functional gain.
- [x] `beacon logs` resolves `--logical-date` → `run_id` via metadata
      (whole-day or exact ISO timestamp).
- [x] `beacon config show` prints every `BEACON_*` env var with
      effective value and source (`(env)` / `(default)`).
- [x] Subprocess smoke test (`tests/e2e/test_cli_entry_point.py`)
      verifies entry point and exit codes.

**Closeout fixes:**
- `LocalBundle.discover_dags()` no longer picks up `variables.yml` or
  `global_variables.yml` as DAG files.
- `beacon sync` stamps a `dag_version` on a pinned deployment that has
  none yet (first deploy); pinning only exempts from *subsequent*
  auto-rolls.

**Cut from Phase 1.5:** `beacon.toml` config file; `SecretsProvider`
adapter — both can be re-evaluated on concrete user request.

### 5.3 — `SqliteMetadata` (single-node persistence)

Replaces `LocalMetadata` as the default for `beacon api`.

**Schema (5 tables):**

```sql
CREATE TABLE dag (id, version, serialized BLOB, created_at);
CREATE TABLE deployment (id, dag_id, dag_version, cron, ...);
CREATE TABLE dag_run (run_id, dag_id, state, logical_date, ...);
CREATE TABLE task_state (run_id, dag_id, task_id, state, updated_at);
CREATE TABLE task_context (run_id, dag_id, task_id, json);
```

**DoD checklist:**
- [ ] Implements `MetadataProtocol` 1:1; passes the same test suite as
      `LocalMetadata`.
- [ ] WAL mode enabled, `synchronous=NORMAL`, single writer pattern.
- [ ] In-process connection pool; async via `asyncio.to_thread`.
- [ ] Migration script: `LocalMetadata` → `SqliteMetadata` (one-shot).
- [ ] Bench: 1000 DAGs × 10 tasks × 100 runs schedules in < 30 s on a
      laptop; sustained ≥ 200 task transitions/s.
- [ ] `kill -9` mid-write leaves DB readable on restart (atomicity).
- [ ] **Coordination methods** use `UNIQUE` constraints and
      `ON CONFLICT DO NOTHING` for multi-instance support.

**Non-goals:** sharding, read-replicas, online backup tool (use
sqlite's own `.backup`).

### 5.4 — Deployment Scheduler (cron-driven) ✅

Shipped. See `beacon/scheduler.py`.

**Implemented:**
- [x] Reads `enabled=True` deployments with cron expressions.
- [x] Honors `start_date`, `end_date`, `timezone`, `catch_up` (bool).
- [x] Computes `logical_date` + `data_interval_*` correctly.
- [x] `max_active_runs` per deployment enforced.
- [x] Backfill via catch_up scheduling (batched, oldest-first).
- [x] Misfire policy: catch_up queues missed; no-catch_up skips.
- [x] Idempotent: restarting the scheduler does not duplicate runs
      (via coordination methods).
- [x] Tests: 31 coordination + scheduler tests passing.

**Non-goals:** event-driven triggers, dataset-aware scheduling,
calendars with holidays.

### 5.5 — `LocalBundle` sync (no git in core)

Beacon reads DAGs + plugins from a directory. **Deployment is the
user's responsibility**: a 2-line systemd timer runs
`git pull && beacon sync /path/to/repo`.

**DoD checklist:**
- [x] `beacon sync PATH` re-reads a `LocalBundle` and registers
      new/changed DAGs + plugins atomically.
- [x] `dag_version` derived from content hash of DAG + plugin + asset
      + variables files in the bundle.
- [ ] Old DAG versions retained side-by-side; in-flight runs unaffected
      (pinned to their `dag_version`). *Today: in-flight runs are
      stamped with their `dag_version` at trigger time, but bundle
      history is not yet kept side-by-side — rollback via
      `git checkout` + sync.*
- [x] Plugin re-registration is idempotent and version-tagged.
- [ ] `POST /sync { path }` API endpoint mirrors the CLI.
- [ ] Tests: sync → deploy → modify → sync again → new version
      coexists with in-flight runs on old version.
- [x] Docs include a systemd-timer recipe (see `deploy.md`).

**Cut:** `GitBundle` (`git+ssh://` / `git+https://`) and webhook
receiver — `git pull` in user-owned automation does the same in 2
lines. Re-evaluate if 3+ teams ask AND can't use the systemd-timer
pattern.

### 5.6 — Bundle policy ✅

Bundle layout, variable scoping, asset lookup, deployment pinning are
codified in [`deploy.md`](./deploy.md). This sub-item is the
runtime + CLI work that enforces it.

**DoD checklist:**
- [x] Bundle layout per `deploy.md`:
      `dags/<group>/<dag>/{dag.yml,variables.yml,assets/}` +
      `dags/[<group>/]global_variables.yml` + bundle-root `assets/`.
- [x] `VariableScope` resolves per-DAG dict, closest-scope-wins
      precedence: deployment `--var` > dag `variables.yml` > group
      `global_variables.yml` > bundle `global_variables.yml`.
      Shallow per-top-level-key merge.
- [x] Asset resolution: `py_statement: <name>` tried as
      `<dag_folder>/assets/<name>`, then `<bundle_root>/assets/<name>`,
      else `FileNotFoundError`. Absolute paths still resolve.
- [x] `Deployment.variable_overrides` replaces `variables_ref`;
      `Deployment.is_pinned` returns `True` iff any override stored.
- [x] `beacon sync` auto-rolls non-pinned deployments; pinned stay on
      old version and report `stale`.
- [x] `beacon deployment sync <id>` / `--all --bundle PATH` accepts a
      new `dag_version` for pinned deployments.
- [x] `beacon deployment diff <id> --bundle PATH` previews resolved
      variables after sync (marks `--var` overrides).
- [x] `beacon list deployments [--bundle PATH]` shows stored
      `dag_version` and flags `[pinned]` / `[stale (bundle: …)]`.
- [x] Scheduler computes scoped variable dict per fire and threads it
      (plus `bundle_root`) into `DagRunner`.
- [x] `Dag.run()` auto-resolves scoped variables when the loader has
      set `_source_file` + `_bundle_root` (explicit `variables=` wins).
- [x] `py` plugin resolves `py_statement` via bundle-aware asset lookup
      (ContextVar pushed by `DagRunner` — no executor signature change).
- [x] `beacon sync` emits soft `WARNING` for folders holding more than
      one DAG file.
- [x] Unit + e2e tests green (338/338 passing).

**Non-goals (still):**
- Promoting one bundle's deployments to another env by file copy
  (use `scripts/deploy_<env>.sh` re-application).
- Deep-merge of nested-dict variable values (shallow only).
- Auto-discovery of which deployments would be affected by a partial
  bundle change beyond `dag_version` equality.

### 5.7 — Process model: `beacon api` ✅

Shipped. See `beacon/api/` module.

```text
beacon api ./bundle --port 8080 --instance-id inst-1
├── scheduler (asyncio)
├── worker (asyncio, max_concurrent N)
└── api server (FastAPI + uvicorn)
```

**DoD checklist:**
- [x] All three components in one process by default.
- [x] `--instance-id` flag for unique instance identification.
- [x] `SIGTERM` → stop accepting new tasks → wait for in-flight ≤
      grace period → exit.
- [x] `/health` returns 200 with instance_id.
- [x] Multiple instances can run concurrently with coordination.
- [x] systemd unit file pattern documented.

**Non-goals:** built-in multi-process orchestration (use systemd /
docker compose / k8s), Windows service installer.

### 5.8 — API Server ✅

Shipped. See `beacon/api/app.py`.

**~14 endpoints.** Every endpoint supports a real ops workflow.

```text
GET    /health                                       # liveness + instance_id

GET    /deployments                                  # list
GET    /deployments/{id}
POST   /deployments                                  # create/update
DELETE /deployments/{id}
PATCH  /deployments/{id}/enable                      # resume
PATCH  /deployments/{id}/disable                     # pause

POST   /triggers                                     # manual trigger
GET    /runs                                         # list/filter
GET    /runs/{run_id}
GET    /runs/active                                  # active runs
```

**DoD checklist:**
- [x] FastAPI app with Pydantic models.
- [x] Trigger endpoint creates manual trigger.
- [x] Deployment CRUD operations.
- [x] Run listing and detail endpoints.
- [x] `/health` endpoint with instance_id.
- [ ] Basic auth + static bearer-token auth.
- [ ] Rate-limit on `/triggers`.
- [ ] `cancel`, `clear`, `mark` endpoints for ops controls.

**Non-goals:** GraphQL, websockets, server-rendered HTML, write
endpoints for DAG/Deployment **definitions** (those live in the
bundle), OIDC (post-v1), connection/variable CRUD (env vars only).

### 5.9 — Multi-Instance Coordination ✅

Shipped. See `beacon/metadata/local_store.py` and
`beacon/core/protocols.py`.

**Coordination Methods:**

| Method | Purpose | Implementation |
|--------|---------|----------------|
| `try_create_scheduled_run()` | Prevent duplicate scheduled runs | File lock + uniqueness check on (dag_id, logical_date) |
| `try_update_scheduler_state()` | Claim scheduler tick | File lock + atomic update if newer |
| `try_claim_trigger()` | Claim manual trigger | File lock + claim with instance_id |
| `drain_triggers_with_claim()` | Drain claimed triggers | Returns only triggers claimed by this instance |

**DoD checklist:**
- [x] `LocalMetadata` implements all coordination methods.
- [x] File-based locks in `.locks/` directory.
- [x] `fcntl.flock` for cross-process locking (Unix).
- [x] `DeploymentScheduler` uses coordination for scheduled runs.
- [x] `DeploymentScheduler` uses coordination for trigger draining.
- [x] Tests: 10 coordination tests + 11 multi-scheduler tests passing.
- [x] Documentation in `reference.md`.

**Non-goals:** Windows support for file locks (use single instance or
SqliteMetadata/PostgresMetadata), leader election (all instances are
equal, coordination is at the operation level).

### 5.10 — Operational self-healing

Three small, high-value components. Each prevents a class of
production pages.

#### 5.10.1 Stuck-task detector

- [ ] Background task in scheduler scans `RUNNING` tasks every
      `stuck_check_interval_seconds` (default 60).
- [ ] Any task whose `running_for_seconds > execution_timeout × 2`
      (or global `stuck_after_seconds` when no timeout) → FAILED with
      `error="stuck: no heartbeat"`.
- [ ] Metric `beacon_tasks_marked_stuck_total`.
- [ ] Disabled if both globals are unset.

#### 5.10.2 Log rotation in `LocalFileSink`

- [ ] Env vars: `BEACON_LOG_MAX_FILE_MB` (default 50),
      `BEACON_LOG_MAX_AGE_DAYS` (default 30).
- [ ] On batch flush, if file size > limit, rotate to
      `attempt_N.jsonl.1` (then `.2`…), keep
      `BEACON_LOG_BACKUP_COUNT` (default 5).
- [ ] Periodic background sweep deletes files older than max-age-days.
- [ ] Tests: rotation triggers, age sweep deletes, in-flight writes
      don't lose records during rotation.

#### 5.10.3 Metadata GC

- [ ] `beacon gc --keep-days N [--dry-run]` deletes `dag_run`,
      `task_state`, `task_context` rows older than N days for runs in
      terminal state.
- [ ] Non-terminal runs NEVER deleted.
- [ ] Documented systemd-timer recipe for nightly GC.
- [ ] Tests: GC idempotent; preserves active runs.

**Non-goals:** per-DAG retention (one global policy), uploading old
logs to S3/GCS before delete, in-process scheduled GC (cron/systemd
does this fine).

### 5.11 — Metrics (Prometheus)

**Surface: 8 metrics, small / stable / ops-meaningful.**

```text
beacon_scheduler_heartbeat_timestamp   gauge
beacon_dag_runs_total{state}           counter
beacon_task_attempts_total{state}      counter
beacon_task_attempt_duration_seconds   histogram
beacon_queue_depth                     gauge
beacon_worker_in_flight                gauge
beacon_tasks_marked_stuck_total        counter
beacon_log_records_dropped_total       counter
```

**DoD checklist:**
- [ ] `/metrics` exposed by the API server.
- [ ] Counter names match Prometheus conventions.
- [ ] **Cardinality bounded**: no per-task-id, per-run-id,
      per-dag-id labels. Only `state` (low cardinality).
- [ ] Grafana dashboard JSON committed in `docs/operations/`.

**Non-goals:** OpenTelemetry traces, per-task labels (cardinality
bomb), custom bucket configs, per-metadata-op timings.

### Cut from Phase 2

- **`GitBundle` auto-pull / webhook** — see §5.5.
- **Web UI** — deferred to §6.6.
- **OIDC auth** — basic + bearer covers 90%.
- **Audit table for admin actions** — log line on mark/clear/trigger
  is enough.

---

## 6. Phase 3 — Detailed DoD (gated on real users)

### 6.1 — `PostgresMetadata`

- [ ] All `MetadataProtocol` tests pass.
- [ ] Connection pooling via `asyncpg`.
- [ ] **Coordination via `UNIQUE` constraints and `ON CONFLICT DO NOTHING`.**
- [ ] Migration script: `SqliteMetadata` → `PostgresMetadata`.
- [ ] Bench: scheduler sustains 1000 task transitions/s with 5 worker
      processes.

**Non-goals:** sharding, read-replicas in the protocol, ORM (raw SQL +
asyncpg; the schema is 5 tables).

### 6.2 — `DockerExecutor`

- [ ] Per-task image configurable on the Task model (`image: ...`).
- [ ] `TaskContext` passed via `BEACON_TASK_CONTEXT` env var (JSON).
- [ ] Container entrypoint: `beacon-runner execute --from-env`.
- [ ] Container stdout/stderr piped line-by-line into the unified log
      pipeline (tagged with task attempt).
- [ ] Resource limits (`cpus`, `memory`) honored.
- [ ] Cleanup on cancellation; no orphan containers.
- [ ] e2e: a DAG with mixed local + docker tasks runs end-to-end.

**Non-goals:** building images at runtime, docker-compose-style
multi-container tasks, custom registries beyond docker auth defaults.

### 6.3 — `KubernetesExecutor` (deferred)

Same contract as `DockerExecutor`; reuses `BEACON_TASK_CONTEXT` runner.

- [ ] Pod spec configurable; sensible defaults.
- [ ] Watches pod status via async; no polling.
- [ ] Pod logs streamed to logging pipeline.
- [ ] Cleanup on cancel + on pod completion.
- [ ] No CRDs. No operator pattern. Official Python client only.

**Non-goals:** custom CRDs, helm chart with knobs for everything,
multi-namespace tenancy.

### 6.4 — OIDC at the API

- [ ] One library (`authlib` or `python-jose`); no custom JWT.
- [ ] Config: issuer URL, audience, JWKS cache.
- [ ] Coexists with basic-auth (different routes/headers as
      appropriate).
- [ ] Documented "front beacon with oauth2-proxy" alternative.

**Non-goals:** group/role mapping, RBAC (still cut per §2), SSO admin UI.

### 6.5 — Web UI v1 (read-mostly)

Pages:
1. Deployments list (status, last run, next run, owner) — primary entry.
2. Deployment detail → recent runs.
3. Run detail → task graph (simple DAG layout) + state colors.
4. Task detail → attempts tabs → log viewer (streams JSONL).
5. DAGs list (advanced; rarely visited).

**DoD checklist:**
- [ ] Vite + React + TanStack Query. No SSR. Tailwind or Pico only.
- [ ] Bundle ≤ 300 KB gzipped.
- [ ] Served by the API server (`/ui/*`).
- [ ] Works behind a reverse proxy at a sub-path.
- [ ] No client-side state machine beyond TanStack Query cache.
- [ ] Lighthouse score ≥ 90 on the runs list page.
- [ ] Pause/resume + manual trigger + clear + mark + cancel work from
      the UI (mirrors §5.8).

**Non-goals:** dark-mode toggles, customizable layouts, drag-to-edit
DAGs, mobile views, i18n, DAG/Deployment editing.

### Cut from Phase 3 (was previously planned)

- **Audit log of admin actions** — log line at action site is enough.
- **Operational hardening "as one item"** — split into §5.10 (Phase 2)
  because without these v1 isn't production-ready.

---

## 7. Phase 4 — Ecosystem (only on demand)

Skip entirely unless real users push for these. Resist.

- **Remote plugin registry** (`org/name@version`) — only if multiple
  orgs build & share plugins.
- **`BatchExecutor`** (AWS Batch / Cloud Batch) — only on concrete
  need beyond Docker/K8s.
- **`GcsLogSink` / `ElasticsearchLogSink`** — protocol is ready;
  implement on first user request. Each ~50 LOC.

**DoD per item:** at least one production user committing to maintain
it, OR a maintainer with time to own it. No speculative builds.

---

## 8. Cross-Cutting Engineering Standards

### 8.1 Testing strategy

| Layer        | Tooling      | Proves                                       |
|--------------|--------------|----------------------------------------------|
| Unit         | pytest       | Pure logic correctness                       |
| Functional   | pytest       | Component integration without I/O boundaries |
| e2e          | pytest       | Real DAG → real metadata → real logs         |
| Coordination | pytest       | Multi-instance race conditions               |
| Bench        | pytest-bench | Throughput regressions caught in CI          |
| Smoke (prod) | shell        | Post-deploy `beacon plan examples/*` passes  |

**Rule:** every new public API ships with one unit + one e2e test in
the same PR.

### 8.2 Performance budgets (CI-enforced)

- DAG parse: ≤ 50 ms for a 100-task DAG.
- Scheduler heartbeat: ≤ 100 ms with 1000 active runs.
- Task transition end-to-end (queue → run → success → next):
  ≤ 50 ms excluding plugin runtime.
- Worker memory: ≤ 200 MB at 200 concurrent in-flight tasks.
- **Coordination lock acquisition: ≤ 10 ms per operation.**

Violations in a PR → red CI.

### 8.3 Dependency discipline

- Core deps cap: **≤ 8** runtime dependencies. Today: 5.
- Add only with justification; one removed before any added once at cap.
- Optional extras for everything else (`beacon[gcs]`, `beacon[postgres]`,
  `beacon[k8s]`, `beacon[api]`).
- No transitive heavy deps (no pandas, numpy, sqlalchemy).

### 8.4 Documentation discipline

Every Phase-2+ feature ships with:

- [ ] A user-facing how-to page (≤ 200 words + working example).
- [ ] A `docs/operations/` runbook entry (failure modes + recovery).
- [ ] OpenAPI / CLI `--help` text complete.

If docs don't land in the same PR, the feature is not done.

### 8.5 Release process

- SemVer. `0.x` until Phase 2 complete; `1.0` when all v1 exit
  criteria (§3) are met.
- Each minor release ships a migration note (even if empty).
- Schema migrations forward-only + reversible **for one prior minor**.

---

## 9. Sequencing Rationale

**Phase 1.5 first** — library is solid; what was missing is the
*ergonomic entry point* (a CLI) that turns a library into a tool.
TOML config + secrets adapter were cut as nice-to-have — env vars and
`os.environ.get` cover the same use cases for zero additional code.

**Phase 2 sub-order:**
1. `SqliteMetadata` is the foundation; everything else writes to it.
2. `DeploymentScheduler` needs persistent metadata to be useful. ✅
3. `LocalBundle` sync (CLI + `POST /sync`) gives the scheduler DAGs.
   Git auto-pull cut — a 2-line systemd timer does it better.
4. `beacon api` ties (1)–(3) into one process. ✅
5. API server exposes (1)–(4) plus ops controls (cancel/mark/clear)
   that prevent 3 AM pages. ✅
6. **Multi-instance coordination** shipped with Phase 2 — enables
   horizontal scaling without duplicate runs. ✅
7. **Self-healing ships in Phase 2, not Phase 3** — without these
   the operator IS the self-healing, which fails our "won't page you
   at 3 AM" bar.
8. Metrics last so we measure what's actually deployed.

**No Web UI in v1** because shipping a UI is enormous scope. The API
+ OpenAPI page + CLI cover every workflow. Document as a v1
limitation; UI lands in Phase 3.

**Phase 3 deferred** because the cost of building remote executors,
Postgres, foreach, OIDC, and a UI without real-user input is exactly
how tools accidentally become Airflow.

**Phase 4 explicitly conditional** to defend against feature creep
from GitHub issues.

---

## 10. What "Done" Looks Like

When v1.0 ships, a data engineer can: write a `dag.yml`, drop it in a
checked-out repo, run `beacon sync` from a systemd timer, see the
deployment trigger on schedule, hit the API to pause/trigger/cancel a
run, stream per-attempt JSONL logs with `beacon logs`, and trust that
stuck tasks won't lock pipelines and disks won't fill — all backed by
one process (or multiple for horizontal scaling), one SQLite file,
env-var config, the same `dag.plan()` they ran on their laptop, and a
documented systemd-timer recipe for deployment. **Multiple instances
coordinate automatically to prevent duplicate runs.** **Nothing in that
sentence references Airflow concepts, a UI, or a config file.** That is
the bar.
