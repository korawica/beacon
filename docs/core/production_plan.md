# Production Plan — Path to Beacon v1.0

> Companion to [`implement_plan.md`](./implement_plan.md) (status table) and
> [`design.md`](./design.md) (architecture). This document defines **what
> "production-ready" means for beacon**, the **discipline of what we will
> *not* build**, and the **ordered work with Definition of Done** for every
> remaining deliverable.

---

## 1. Positioning

Beacon is for the **80% of teams that need a workflow orchestrator but
don't need Apache Airflow**. The promise:

- `pip install beacon` → working scheduler in 60 seconds.
- One YAML or Python file → a deployed DAG.
- One process to operate. One config file. One log pipeline.
- Async-first; sensors don't waste worker slots.

If your team needs multi-tenant scheduler HA, RBAC, lineage, datasets,
dynamic task mapping in arbitrary places, an editor UI, or a 200-provider
ecosystem — **use Airflow**. We will not chase parity.

---

## 2. Non-Goals (the discipline)

Things beacon will **never** ship in core. These are the lines that, when
crossed, turn a lean tool into another Airflow.

| Non-goal                                            | Why                                                                       | If you need it                          |
|-----------------------------------------------------|---------------------------------------------------------------------------|-----------------------------------------|
| Multi-scheduler HA (active-active)                  | Adds DB-locking + leader election; one scheduler per deployment is enough | Run multiple beacon instances per team  |
| Web UI in v1                                        | Huge scope; API + CLI cover every must-have ops workflow                  | Use the API; UI is Phase 3              |
| TOML / YAML config file in v1                       | ~10 env vars is manageable                                                | `BEACON_*` env vars                     |
| Built-in git auto-pull (`GitBundle`)                | 2 lines of `git pull && beacon sync` in systemd does it better            | Systemd timer / cron / your CI          |
| Secrets adapter (`SecretsProvider`)                 | `os.environ.get()` in user code works today; zero customers asking        | Env vars + your platform's secret store |
| Connection / Variable editor UI                     | Couples secrets to the UI; security risk                                  | Env vars or external secret manager     |
| DAG editor UI                                       | DAGs are code; edit in your IDE + git                                     | Use git + `beacon sync`                 |
| RBAC with row-level permissions                     | Re-implements auth proxies                                                | Front beacon with oauth2-proxy / IAP    |
| Audit table for admin actions                       | Log lines at action sites are enough; dedicated audit is compliance-tier  | grep the audit log lines                |
| OIDC auth in v1                                     | Basic + bearer-token covers 90%; OIDC is Phase 3                          | Use basic-auth or bearer-token in v1    |
| Email / Slack / PagerDuty built-in                  | One-off plugins; not core                                                 | Write a 30-line callback plugin         |
| Lineage / Datasets / OpenLineage emission           | Cross-system coupling, slow-moving spec                                   | Emit events from a callback if needed   |
| Triggerer / deferrable operators                    | Async-first eliminates the need                                           | n/a — already solved                    |
| Plugin marketplace with hundreds of providers       | Quality bar collapses; we curate ~10 standard ones                        | Write a custom plugin (one class)       |
| SLA misses + DAG-level pools + priority weights     | Airflow-style scheduler complexity for ambiguous wins                     | Cancel late runs from a callback        |
| TaskGroups with arbitrary nesting visualization     | UI complexity > value                                                     | Use flat DAGs or one-level Groups       |
| XCom-style cross-task data shuttle                  | Encourages tight coupling                                                 | Return small dicts; pass big data refs  |
| Dynamic task mapping at arbitrary points            | Graph becomes unpredictable / un-paginable                                | Use `foreach_task` only                 |
| `BashOperator`-style implicit shell exec primitives | Footgun; encourages anti-patterns                                         | Write a `py` task that calls subprocess |
| Full REST API parity with Airflow                   | Surface area we can't maintain                                            | Focused, documented endpoints only      |

**Rule of thumb:** every feature must justify itself with: *what use-case
breaks without it?* If the answer is "people might want it someday",
the answer is **no**.

---

## 3. Definition of "Production-Ready" (v1.0 exit criteria)

A team can do all of the following without our help:

1. **Deploy from a checkout** — `beacon deploy /path/to/repo` registers
   DAGs + deployments from a `LocalBundle`. Teams put `git pull && beacon
   sync` in a systemd timer or cron — built-in git polling is **not** v1.
   Per-deployment variable overrides via `--var` pin the deployment to
   its current `dag_version`; non-pinned deployments auto-roll on sync.
2. **Persist beyond restart** — metadata in SQLite survives full process
   restart with no data loss.
3. **Schedule by cron** — DAGs run on schedule, catchup honored,
   timezones honored, missed runs visible.
4. **Operate one process** — `beacon serve` runs scheduler + worker(s) +
   API server. Graceful shutdown drains in-flight tasks.
5. **Observe** — `/healthz`, `/metrics` (Prometheus, ~5 metrics),
   structured logs in one pipeline (✅ done), `beacon logs` CLI streams
   per-attempt JSONL.
6. **Configure via env vars** — all knobs are `BEACON_*` env vars. No
   config file in v1. No DB-stored config ever.
7. **Recover** — kill -9 the process; restart; in-flight tasks are
   correctly marked failed or resumed. **Ops can cancel a runaway task**
   and **mark a stuck task as success/skipped/failed** via the API.
8. **Self-heal** — stuck-task detector flips zombie tasks; log rotation
   keeps disks from filling; metadata GC keeps the DB from growing
   forever.
9. **Test locally** — `dag.dryrun()`, `dag.test()`, `dag.run()` work
   identically to production (✅ done).
10. **Auth** — basic auth or static bearer-token at the API. Nothing
    fancy, nothing bespoke. (OIDC is post-v1.)
11. **Docs cover the happy path** — install → first deployment →
    monitoring → upgrade, in under 30 minutes of reading.

If a point above is **not** met, beacon is not v1.0.

**Explicitly NOT in v1**: web UI, git auto-pull, OIDC, TOML config file,
secrets adapter, audit log. Each has a documented rationale below or in
§2 (non-goals).

---

## 4. Roadmap

Three phases. Each phase ends with a usable, shippable thing — no
half-built features straddling releases.

### Legend

- **DoD checklist** — every box must tick before the item is "done".
- **Non-goals** — what we explicitly DON'T do in this item.
- **Out of scope here** — work that's related but lives in a different
  item, to prevent scope creep.

---

### Phase 1.5 — Close the loop on local-first (1 week)

The library works; add the one ergonomic piece that turns a library
into a tool.

#### 1.5.1 — Structured logging pipeline ✅

Already shipped. See `beacon/logging.py`.

#### 1.5.2 — `beacon` CLI (entry point)

A single CLI replaces ad-hoc Python scripts.

**Commands (v1):**

```text
beacon dryrun  PATH                              # parse + render, no execution
beacon test    PATH [--params k=v ...]           # run in temp metadata
beacon run     PATH [--params k=v ...]           # local run, persistent metadata
beacon deploy  PATH [--cron ...] [--params ...] [--var k=v ...]
                                                 # register a Deployment; --var
                                                 # pins it to its dag_version
beacon deployment sync DEPLOYMENT_ID|--all --bundle PATH
                                                 # accept a new dag_version for
                                                 # a pinned deployment
beacon deployment diff DEPLOYMENT_ID --bundle PATH
                                                 # preview variable resolution
                                                 # for a pinned deployment
beacon sync    PATH                              # re-read a LocalBundle from disk;
                                                 # auto-rolls non-pinned deployments
beacon list    [dags|deployments|runs]           # `deployments` shows version +
                                                 # [pinned] / [stale] flags
beacon logs    DAG_ID TASK_ID [--run RUN_ID | --logical-date YYYY-MM-DD]
                                                 # tail or dump JSONL from logging store
beacon serve   [--scheduler] [--worker N] [--api]
                                                 # production process(es)
beacon config  show                              # dump effective env-var config
```

**DoD checklist:**
- [x] `beacon --help` lists all 11 commands with consistent flag style
      (`--metadata-path`, `--dag-id`, `--param`, `--var`, `--bundle`
      naming used uniformly across commands).
- [x] All commands exit with non-zero on failure and follow a stable
      exit-code contract documented in `beacon/cli/main.py`:
      `0` success, `1` runtime/operational failure, `2` invocation
      error (click `UsageError`). Enforced by tests in
      `tests/functional/test_cli_*` and `tests/e2e/test_cli_entry_point.py`.
- [x] **Decision: stay on click, not typer.** The whole CLI is built on
      click; typer would be a stylistic rewrite with zero functional
      gain (click also satisfies the original DoD intent: one declarative
      typed CLI framework, not raw argparse). The DoD wording is the
      thing that changed, not the dependency.
- [x] `beacon logs` resolves `--logical-date` → `run_id` via metadata
      (matches whole-day or exact ISO timestamp). Covered by
      `tests/functional/test_cli_config_and_logs.py`.
- [x] `beacon config show` prints every `BEACON_*` env var with its
      effective value and source (`(env)` / `(default)`). Covered by
      `tests/functional/test_cli_config_and_logs.py`.
- [x] Subprocess smoke test for the installed entry point
      (`tests/e2e/test_cli_entry_point.py`) — verifies `beacon --help`,
      `beacon config show`, and the unknown-command + bad-path exit
      codes from a real shell invocation. **The full `beacon serve`
      subprocess test is deferred to §2.4** (the process model lives
      there); when §2.4 lands it adds the deploy / observe / kill /
      restart / verify-state scenario on top of this entry-point test.

**Closeout fixes caught by the new tests:**
- `LocalBundle.discover_dags()` no longer picks up `variables.yml` or
  `global_variables.yml` as DAG files (those filenames are reserved).
- `beacon sync` now stamps a `dag_version` on a pinned deployment that
  has none yet (first deploy); pinning only exempts a deployment from
  *subsequent* auto-rolls, not from the initial stamp.

**Non-goals:** TUI dashboards, fancy progress bars, autocompletion install
scripts (document `--completion bash` flag, don't auto-install),
**TOML / YAML config files** — env vars only in v1.

#### Cut from Phase 1.5

- **`beacon.toml` config file** — 10-ish env vars is manageable.
  Re-evaluate if config surface grows past 20.
- **Secrets adapter (`SecretsProvider`)** — `os.environ.get(...)` in user
  `py` code works today and has zero customers asking for more.
  Re-evaluate when the first user requests Vault/Secrets Manager
  integration with a concrete use case.

---

### Phase 2 — Production deployment (4–6 weeks)

What it takes to put beacon on a server and forget about it.

#### 2.1 — `SqliteMetadata` (single-node persistence)

Replace `JsonMetadata` as the default for `beacon serve`.

**Schema (5 tables, indexed for the scheduler hot path):**

```sql
CREATE TABLE dag (id, version, serialized BLOB, created_at);
CREATE TABLE deployment (id, dag_id, dag_version, cron, ...);
CREATE TABLE dag_run (run_id, dag_id, state, logical_date, ...);
CREATE TABLE task_state (run_id, dag_id, task_id, state, updated_at);
CREATE TABLE task_context (run_id, dag_id, task_id, json);
```

**DoD checklist:**
- [ ] Implements `MetadataProtocol` 1:1; passes the same test suite as
      `JsonMetadata`.
- [ ] WAL mode enabled, `synchronous=NORMAL`, single writer pattern.
- [ ] In-process connection pool; async via `asyncio.to_thread`.
- [ ] Migration script: `JsonMetadata` → `SqliteMetadata` (one-shot).
- [ ] Bench: 1000 DAGs × 10 tasks × 100 runs schedules in < 30 s on a
      laptop, sustained throughput ≥ 200 task transitions/s.
- [ ] kill -9 mid-write leaves the DB readable on restart (atomicity).

**Non-goals:** sharding, read-replicas, online backup tool (use sqlite's
own `.backup`).

---

#### 2.2 — Deployment Scheduler (cron-driven)

Currently only `Dag.run()` exists. Add a scheduler that watches
`Deployment` records and triggers runs on schedule.

**DoD checklist:**
- [ ] Reads `enabled=True` deployments where `next_run <= now`.
- [ ] Honors `start_date`, `end_date`, `timezone`, `catchup` (bool).
- [ ] Computes `logical_date` + `data_interval_*` correctly across DST.
- [ ] `max_concurrent_runs_per_dag` enforced (default 1, configurable).
- [ ] Backfill: `beacon backfill DEPLOYMENT_ID --from D1 --to D2`
      produces runs equivalent to scheduled runs.
- [ ] Misfire policy: if scheduler was down, queue missed runs (catchup)
      OR skip to next (no-catchup). Both tested.
- [ ] Idempotent: restarting the scheduler does not create duplicate runs.
- [ ] Tests: timezone matrix (UTC, America/New_York, Asia/Bangkok) + DST.

**Non-goals:** event-driven triggers, dataset-aware scheduling, calendars
with holidays (use cron's `?` workarounds).

---

#### 2.3 — `LocalBundle` sync (no git in core)

Beacon reads DAGs + plugins from a directory on disk. **Deployment is
the user's responsibility**: a 2-line systemd timer or cron job runs
`git pull && beacon sync /path/to/repo` on whatever cadence they want.

**DoD checklist:**
- [x] `beacon sync PATH` re-reads a `LocalBundle` and registers any
      new/changed DAGs + plugins atomically.
- [x] `dag_version` derived from content hash of the DAG + plugin +
      asset + variables files in the bundle.
- [ ] Old DAG versions retained; in-flight runs unaffected (pinned to
      their `dag_version`). *(Today: in-flight runs are stamped with
      their `dag_version` at trigger time, but bundle history is not
      yet kept side-by-side — rollback works via `git checkout` + sync.)*
- [x] Plugin re-registration is idempotent and version-tagged.
- [ ] `POST /sync { path }` API endpoint mirrors the CLI.
- [ ] Tests: sync → deploy → modify → sync again → new version coexists
      with in-flight runs on old version.
- [x] Docs include a systemd-timer recipe (see `docs/core/deploy.md`).

**Cut (was previously planned):**
- **`GitBundle` with `git+ssh://` / `git+https://` URLs** — `git pull`
  in user-owned automation does the same job in 2 lines, without
  beacon needing to handle SSH keys, HTTPS auth, or repo poll loops.
- **Webhook receiver** — write a 10-line endpoint in your gateway that
  hits `POST /sync`.

Re-evaluate built-in git support if 3+ teams ask for it AND can't use
the systemd-timer pattern.

---

#### 2.3a — Bundle policy: scoped variables, asset resolution, pinned deployments

The bundle layout, variable scoping, asset lookup, and deployment
pinning rules are now the **package policy** documented in
`docs/core/deploy.md`. This sub-item codifies the runtime + CLI work
that enforces it.

**DoD checklist:**
- [x] Bundle layout per `docs/core/deploy.md`:
      `dags/<group>/<dag>/{dag.yml,variables.yml,assets/}` +
      `dags/[<group>/]global_variables.yml` + bundle-root `assets/`.
- [x] `VariableScope` resolves a per-DAG dict with closest-scope-wins
      precedence: deployment `--var` > dag `variables.yml` > group
      `global_variables.yml` (any ancestor) > bundle
      `global_variables.yml`. Shallow per-top-level-key merge.
- [x] Asset resolution: `py_file: <name>` is tried first as
      `<dag_folder>/assets/<name>`, then `<bundle_root>/assets/<name>`,
      else raises `FileNotFoundError` listing both paths. Absolute
      paths still resolve to themselves.
- [x] `Deployment.variable_overrides` field replaces `variables_ref`;
      `Deployment.is_pinned` returns `True` iff any override is stored.
- [x] `beacon sync` auto-rolls **non-pinned** deployments to the new
      `dag_version`; pinned deployments stay on their old version and
      are reported as `stale`.
- [x] `beacon deployment sync <id>` / `--all --bundle PATH` accepts a
      new `dag_version` for pinned deployments.
- [x] `beacon deployment diff <id> --bundle PATH` previews the
      resolved variable set after sync (marks `--var` overrides).
- [x] `beacon list deployments [--bundle PATH]` shows the stored
      `dag_version` and flags `[pinned]` / `[stale (bundle: …)]`.
- [x] Scheduler computes the scoped variable dict per fire and threads
      it (plus `bundle_root`) into `DagRunner`.
- [x] `Dag.run()` auto-resolves scoped variables when the loader has
      set the dag's `_source_file` + `_bundle_root` (explicit
      `variables=` still wins).
- [x] `py` plugin resolves `py_file` via the bundle-aware asset lookup
      (ContextVar pushed by `DagRunner` — no executor signature change).
- [x] `beacon sync` emits a soft `WARNING` for folders that hold more
      than one DAG file (policy: one DAG per folder).
- [x] Unit + e2e tests green (`tests/unit/test_deployment.py` updated
      for `variable_overrides` + `is_pinned`; 338/338 passing).

**Non-goals (still):**
- Promoting one bundle's deployments to another environment by file
  copy (use `scripts/deploy_<env>.sh` re-application — see deploy.md).
- Deep-merge of nested-dict variable values (shallow only — predictable).
- Auto-discovery of which deployments would be affected by a partial
  bundle change beyond `dag_version` equality.

---

#### 2.4 — Process model: `beacon serve`

One supervised process tree, graceful shutdown.

```text
beacon serve
├── scheduler (asyncio)
├── worker (asyncio, max_concurrent N)
└── api server (uvicorn)
```

**DoD checklist:**
- [ ] All three components run in one process by default; `--scheduler`,
      `--worker`, `--api` flags can isolate.
- [ ] `SIGTERM` → stop accepting new tasks, wait for in-flight ≤ grace
      period, then `SIGKILL` remaining. Tested.
- [ ] `/healthz` returns 200 only when scheduler heartbeat is fresh.
- [ ] `/readyz` returns 200 only after metadata migrations + bundle load.
- [ ] systemd unit file in `docs/operations/`.

**Non-goals:** built-in multi-process orchestration (use systemd /
docker compose / k8s), Windows service installer.

---

#### 2.5 — API Server (read-mostly + ops controls)

Focused FastAPI surface. **~14 endpoints, not 80.** Every endpoint
exists to support a real ops workflow.

```text
GET    /healthz                                       # liveness
GET    /readyz                                        # readiness
GET    /metrics                                       # Prometheus

GET    /dags                                          # list
GET    /dags/{id}                                     # detail + version
GET    /deployments                                   # list
GET    /deployments/{id}
PATCH  /deployments/{id}      { enabled: bool }       # pause/resume

POST   /deployments/{id}/trigger { params: {...} }    # manual run
GET    /runs?dag_id=...&state=...                     # list/filter (cursor pagination)
GET    /runs/{run_id}                                 # detail + task graph
POST   /runs/{run_id}/cancel                          # cancel running run
GET    /runs/{run_id}/tasks/{task_id}                 # task detail + attempts
GET    /runs/{run_id}/tasks/{task_id}/logs?attempt=N  # stream JSONL
POST   /runs/{run_id}/tasks/{task_id}/clear           # re-queue (clears state)
                                                      # body: { downstream: bool }
                                                      # core API: dag.clear() / DagRunner.clear()
POST   /runs/{run_id}/tasks/{task_id}/mark            # { state: success|skipped|failed }
                                                      # manual ops override
POST   /sync                  { path: "/path" }       # re-read LocalBundle
```

**DoD checklist:**
- [ ] OpenAPI spec auto-generated from FastAPI; served at `/docs`.
- [ ] Pagination on list endpoints (cursor, not offset).
- [ ] Logs endpoint streams (chunked JSONL); does not buffer entire file
      in memory.
- [ ] Basic auth + static bearer-token auth. Both via one library.
- [ ] Rate-limit on `/trigger`, `/sync`, `/cancel`, `/clear`, `/mark`
      (in-memory token bucket).
- [ ] `cancel` sends a cooperative cancel to the worker; if the task
      doesn't honor it within `cancel_grace_seconds`, mark it FAILED.
- [ ] `mark` is audited via a log line (operator + endpoint + before/after
      state) — no separate audit table.
- [ ] Every endpoint has a contract test + an example in docs.

**Non-goals:** GraphQL, websockets, server-rendered HTML, write
endpoints for DAG/Deployment **definitions** (those live in the bundle),
OIDC (post-v1), connection/variable CRUD endpoints (env vars only).

---

#### 2.6 — Operational self-healing (the "won't page you at 3 AM" tier)

Three small, high-value components. Each one prevents a class of
production pages that the scheduler/worker can't recover from on its
own.

##### 2.6.1 Stuck-task detector

**DoD checklist:**
- [ ] Background task in the scheduler scans `RUNNING` tasks every
      `stuck_check_interval_seconds` (default 60).
- [ ] Any task whose `running_for_seconds > execution_timeout × 2` (or
      a global `stuck_after_seconds` when no timeout is set) →
      transitioned to `FAILED` with `error="stuck: no heartbeat"`.
- [ ] Documented metric `beacon_tasks_marked_stuck_total`.
- [ ] Disabled if both globals are unset (`stuck_after_seconds=None`).

##### 2.6.2 Log rotation in `LocalFileSink`

**DoD checklist:**
- [ ] Env vars: `BEACON_LOG_MAX_FILE_MB` (default 50),
      `BEACON_LOG_MAX_AGE_DAYS` (default 30).
- [ ] On batch flush, if file size > limit, rotate to `attempt_N.jsonl.1`
      (then `.2`, …), keeping `BEACON_LOG_BACKUP_COUNT` (default 5).
- [ ] Periodic background sweep deletes files older than max-age-days.
- [ ] Tests: rotation triggers, age sweep deletes, in-flight writes
      don't lose records during rotation.

##### 2.6.3 Metadata GC

**DoD checklist:**
- [ ] `beacon gc --keep-days N [--dry-run]` deletes `dag_run`,
      `task_state`, `task_context` rows older than N days for runs in
      terminal state.
- [ ] Runs in non-terminal state are NEVER deleted.
- [ ] Documented systemd-timer recipe for nightly GC.
- [ ] Tests: GC is idempotent; preserves active runs.

**Non-goals:** retention by DAG (one global policy), uploading old logs
to S3/GCS before delete (out of scope; users can pre-tar themselves),
in-process scheduled GC (cron/systemd does this fine).

---

#### 2.7 — Metrics (Prometheus)

**Metrics surface (small, stable, ops-meaningful — 8 total):**

```text
beacon_scheduler_heartbeat_timestamp           gauge
beacon_dag_runs_total{state}                   counter
beacon_task_attempts_total{state}              counter
beacon_task_attempt_duration_seconds           histogram
beacon_queue_depth                             gauge
beacon_worker_in_flight                        gauge
beacon_tasks_marked_stuck_total                counter
beacon_log_records_dropped_total               counter
```

**DoD checklist:**
- [ ] `/metrics` exposed by the API server.
- [ ] All counter names match Prometheus conventions.
- [ ] **Cardinality bounded**: no per-task-id, per-run-id, per-dag-id
      labels. Only `state` (low cardinality) is allowed.
- [ ] Grafana dashboard JSON committed in `docs/operations/`.

**Non-goals:** OpenTelemetry traces (add later if asked), per-task
labels (cardinality bomb), histograms with custom bucket configs,
per-metadata-op timings (drop the `beacon_metadata_op_seconds` that
was in an earlier draft — measure it once at startup, ship the result
in docs).

---

#### Cut from Phase 2

- **`GitBundle` auto-pull / webhook** — see §2.3 rationale.
- **Web UI** — deferred to post-v1 (§3.6 below). v1 ships with API + CLI;
  the API + `/docs` OpenAPI page + `beacon logs` covers every workflow.
  Document this loudly in the v1 release notes.
- **OIDC auth** — basic + bearer-token covers 90%. OIDC is enterprise.
- **Audit table for admin actions** — log line on mark/clear/trigger is
  enough. A dedicated audit subsystem is compliance-tier.

---

### Phase 3 — Scale-out & UI (only when 2.x lands real users)

Defer all of this until at least 3 teams are running Phase 2 in production
and we know which knobs they actually turn.

#### 3.1 — `PostgresMetadata`

For multi-node deployments. Same protocol as SQLite.

**DoD checklist:**
- [ ] All `MetadataProtocol` tests pass.
- [ ] Connection pooling via `asyncpg`.
- [ ] One advisory lock for scheduler leader-election (simple, not Raft).
- [ ] Migration script: `SqliteMetadata` → `PostgresMetadata`.
- [ ] Bench: scheduler sustains 1000 task transitions/s with 5 worker
      processes.

**Non-goals:** sharding, read-replicas in the protocol, ORM (use raw SQL
+ asyncpg; the schema is 5 tables).

---

#### 3.2 — `DockerExecutor`

Each task runs in a fresh container. Good for isolation + reproducible
plugin envs.

**DoD checklist:**
- [ ] Per-task image configurable on the Task model (`image: ...`).
- [ ] `TaskContext` passed via `BEACON_TASK_CONTEXT` env var (JSON).
- [ ] Container entrypoint: `beacon-runner execute --from-env`.
- [ ] Container stdout/stderr piped line-by-line into the unified log
      pipeline (tagged with task attempt).
- [ ] Resource limits (`cpus`, `memory`) honored.
- [ ] Cleanup on cancellation; no orphan containers.
- [ ] e2e: a DAG with mixed local + docker tasks runs end-to-end.

**Non-goals:** building images at runtime, docker-compose-style multi-
container tasks, custom registries beyond docker auth defaults.

---

#### 3.3 — `KubernetesExecutor` (optional, deferred)

Only if a paying user / multiple OSS users ask. Implements the same
contract as `DockerExecutor`. Reuses the `BEACON_TASK_CONTEXT` env-var
runner.

**DoD checklist:**
- [ ] Pod spec configurable; sensible defaults.
- [ ] Watches pod status via async; no polling.
- [ ] Pod logs streamed to logging pipeline.
- [ ] Cleanup on cancel + on pod completion.
- [ ] No CRDs. No operator pattern. Just the official Python client.

**Non-goals:** custom CRDs, helm chart with knobs for everything,
multi-namespace tenancy.

---

#### 3.4 — `foreach_task` (dynamic fan-out)

Already designed in `design.md`. Implement the resolution + scheduling.

**DoD checklist:**
- [ ] `for_each: "{{ params.items }}"` resolves at trigger time.
- [ ] N task instances generated; each gets its own `TaskContext`.
- [ ] Downstream of a foreach can declare `upstream: [foreach_id]` and
      receives a list of upstream outputs.
- [ ] Empty list → all instances skipped, downstream still runs (or
      configurable trigger rule).
- [ ] CLI / API expose fan-out group as one logical node with N
      instances.

**Non-goals:** mapping over upstream outputs of unknown size at run time
(use a `py` task that emits a fixed-size next-step DAG instead),
nested foreach.

---

#### 3.5 — OIDC at the API

Add OIDC bearer-token validation alongside the basic + static-token auth
shipped in Phase 2.

**DoD checklist:**
- [ ] One library (`authlib` or `python-jose`), no custom JWT code.
- [ ] Config: issuer URL, audience, JWKS cache.
- [ ] Mode coexists with basic-auth (different routes / headers as
      appropriate).
- [ ] Documented "front beacon with oauth2-proxy" alternative for teams
      that don't want OIDC in-process.

**Non-goals:** group/role mapping, RBAC (still cut per §2), SSO admin UI.

---

#### 3.6 — Web UI v1 (read-mostly)

Minimal static SPA, deferred from Phase 2 to keep v1 lean.

Pages:
1. Deployments list (status, last run, next run, owner) — primary entry.
2. Deployment detail → recent runs.
3. Run detail → task graph (simple DAG layout, no zoom UI) + state colors.
4. Task detail → attempts tabs → log viewer (streams JSONL).
5. DAGs list (advanced; rarely visited).

**DoD checklist:**
- [ ] Vite + React + TanStack Query. No SSR. No custom CSS framework
      beyond Tailwind or Pico.
- [ ] Bundle ≤ 300 KB gzipped.
- [ ] Served by the API server (`/ui/*`) so deployment is one process.
- [ ] Works behind a reverse proxy at a sub-path.
- [ ] No client-side state machine beyond TanStack Query cache.
- [ ] Lighthouse score ≥ 90 on the runs list page.
- [ ] Pause/resume + manual trigger + clear + mark + cancel work from
      the UI (mirrors §2.5).

**Non-goals:** dark-mode toggles, customizable layouts, drag-to-edit DAGs,
mobile-optimized views (it's an ops tool), i18n, DAG/Deployment editing.

---

#### Cut from Phase 3 (was previously planned)

- **Audit log of admin actions** — a log line at the action site is
  enough. A dedicated audit table is compliance-tier; out of scope.
- **Operational hardening "as one item"** — split: stuck-task detector,
  log rotation, and metadata GC are now Phase 2 (§2.6) because they
  prevent on-call pages; without them v1 isn't production-ready.

---

### Phase 4 — Ecosystem (only if demand exists)

Skip entirely unless real users push for these. Resist.

- **Remote plugin registry** (`org/name@version` resolution) — only if
  multiple orgs build & share plugins.
- **`BatchExecutor`** (AWS Batch / Cloud Batch) — only if a user has a
  concrete need we can't solve with Docker/K8s.
- **`GcsLogSink` / `ElasticsearchLogSink`** — the protocol is ready;
  implement when first user asks. Each is ~50 LOC.

**DoD per ecosystem item:** at least one production user committing to
maintain it, OR a maintainer with time to own it. No speculative builds.

---

## 5. Cross-Cutting Engineering Standards

These apply to every item above.

### 5.1 Testing strategy (already in place; codify)

| Layer        | Tooling      | What it proves                                |
|--------------|--------------|-----------------------------------------------|
| Unit         | pytest       | Pure logic correctness                        |
| Functional   | pytest       | Component integration without I/O boundaries  |
| e2e          | pytest       | Real DAG → real metadata → real logs          |
| Bench        | pytest-bench | Throughput regressions caught in CI           |
| Smoke (prod) | shell        | Post-deploy `beacon dryrun examples/*` passes |

**Rule:** every new public API ships with one unit + one e2e test in the
same PR.

### 5.2 Performance budgets (CI enforced)

- DAG parse: ≤ 50 ms for a 100-task DAG.
- Scheduler heartbeat: ≤ 100 ms with 1000 active runs.
- Task transition end-to-end (queue → run → success → next): ≤ 50 ms
  excluding plugin runtime.
- Worker memory: ≤ 200 MB at 200 concurrent in-flight tasks.

Violations of these budgets in a PR → red CI.

### 5.3 Dependency discipline

- Core deps cap: ≤ **8** runtime dependencies. Today: 3.
  - Add only with justification; one removed before any added once at cap.
- Optional extras for everything else (`beacon[gcs]`, `beacon[postgres]`,
  `beacon[k8s]`).
- No transitive heavy deps (no pandas, no numpy, no sqlalchemy).

### 5.4 Documentation discipline

Every Phase-2+ feature ships with:

- [ ] A user-facing how-to page (≤ 200 words + working example).
- [ ] A `docs/operations/` runbook entry (failure modes + recovery).
- [ ] OpenAPI / CLI `--help` text complete.

If docs don't land in the same PR, the feature is not done.

### 5.5 Release process

- SemVer. `0.x` until Phase 2 complete; `1.0` when all v1 exit criteria
  (§3) are met.
- Each minor release ships a migration note (even if empty).
- Schema migrations are forward-only + reversible **for one prior minor**.

---

## 6. Sequencing rationale

Why this order, in one paragraph each.

**Phase 1.5 first** because: the library is solid; what's missing is the
*ergonomic entry point* (a CLI) that turns a library into a tool. One
deliverable. High ROI. Anything more elaborate (TOML config, secrets
adapter) was cut as nice-to-have — env vars and `os.environ.get` cover
the same use cases for zero additional code.

**Phase 2 in the listed sub-order** because:
1. `SqliteMetadata` is the foundation; everything else writes to it.
2. `DeploymentScheduler` needs persistent metadata to be useful.
3. `LocalBundle` sync (CLI + `POST /sync`) gives the scheduler DAGs;
   git auto-pull was cut because a 2-line systemd timer does it better.
4. `beacon serve` ties (1)–(3) into one process.
5. API server exposes (1)–(4) plus the ops controls (cancel, mark,
   clear) that prevent 3 AM pages.
6. **Self-healing (stuck-task, log rotation, metadata GC)** ships in
   Phase 2, not Phase 3, because without these the operator IS the
   self-healing — and that fails our "won't page you at 3 AM" bar.
7. Metrics last so we measure what's actually deployed.

**No Web UI in v1** because shipping a UI is enormous scope. The API +
OpenAPI page + CLI cover every workflow. Document it as a v1 limitation;
UI lands in Phase 3.

**Phase 3 deferred** because the cost of building remote executors,
Postgres, foreach, OIDC, and a UI without real-user input is exactly
how tools accidentally become Airflow.

**Phase 4 explicitly conditional** to defend against feature creep from
GitHub issues.

---

## 7. What "Done" looks like (one paragraph)

When v1.0 ships, a data engineer can: write a `dag.yml`, drop it in a
checked-out repo, run `beacon sync` from a systemd timer, see the
deployment trigger on schedule, hit the API to pause/trigger/cancel a
run, stream per-attempt JSONL logs with `beacon logs`, and trust that
stuck tasks won't lock pipelines and disks won't fill — all backed by
one process, one SQLite file, env-var config, the same `dag.dryrun()`
they ran on their laptop, and a documented systemd-timer recipe for
deployment. **Nothing in that sentence references Airflow concepts, a
UI, or a config file.** That is the bar.
