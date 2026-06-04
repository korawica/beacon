# Implement Plan

Status table for every deliverable. Source of truth for sequencing and
acceptance criteria lives in [`production_plan.md`](./production_plan.md);
design rationale lives in [`design.md`](./design.md).

Legend: ✅ Done · 🟡 In progress · ⬜ Pending · 🚫 Cut (see non-goals)

---

## Phase 0 — Core loop (foundation)

| Deliverable                                       | Status  | Validates                       |
|---------------------------------------------------|---------|---------------------------------|
| `PythonPlugin.execute()` end-to-end               | ✅       | Core loop                       |
| TaskContext + Attempt + LocalExecutor             | ✅       | State persistence + retry       |
| Bundle plugin auto-discovery (`./plugins/`)       | ✅       | Custom plugin deployment        |
| `load_context()` for user Python files            | ✅       | Runtime access without coupling |
| Task state machine with valid transitions         | ✅       | Enforced lifecycle              |

## Phase 1 — Library complete

| Deliverable                                                     | Status  | Validates                                  |
|-----------------------------------------------------------------|---------|--------------------------------------------|
| Callback system with registry resolution                        | ✅       | Callback parity Python/YAML                |
| Async worker with retry scheduling                              | ✅       | Full lifecycle transitions                 |
| `MetadataProtocol` + `JsonMetadata` (sharded)                   | ✅       | Pluggable persistence, 1000+ DAGs          |
| `TaskFailed` / `TaskSkipped` exception support                  | ✅       | Plugin-driven retry/skip control           |
| `Deployment` model (reusable DAG + per-env config)              | ✅       | DAG reuse without duplication              |
| `Dag.run()` / `Dag.test()` / `Dag.dryrun()` methods             | ✅       | Developer workflow (validate → test → run) |
| LocalScheduler (trigger rules, branch, teardown, DAG callbacks) | ✅       | Full DAG lifecycle in-process              |
| Setup & Teardown (`teardown` field + scheduler)                 | ✅       | Cluster/staging lifecycle                  |
| Lean Jinja renderer (SandboxedEnvironment only)                 | ✅       | Two-pass: trigger-time + execute-time      |

## Phase 1.5 — Close the loop on local-first

See [`production_plan.md` §4 Phase 1.5](./production_plan.md#phase-15--close-the-loop-on-local-first-1-week).

| #     | Deliverable                                                                                      | Status  | DoD ref           |
|-------|--------------------------------------------------------------------------------------------------|---------|-------------------|
| 1.5.1 | Structured logging pipeline (unified, batched, JSONL, file/memory sinks)                         | ✅       | production §1.5.1 |
| 1.5.2 | `beacon` CLI (`dryrun`, `test`, `run`, `deploy`, `sync`, `list`, `logs`, `serve`, `config show`) | ⬜       | production §1.5.2 |

**Cut from Phase 1.5** (nice-to-have, not should-have):
- `beacon.toml` config file — env vars only in v1.
- `SecretsProvider` adapter — `os.environ.get()` in user code suffices.

## Phase 2 — Production deployment

See [`production_plan.md` §4 Phase 2](./production_plan.md#phase-2--production-deployment-46-weeks).
Items are strictly ordered — each unlocks the next.

| #   | Deliverable                                                                       | Status  | DoD ref         |
|-----|-----------------------------------------------------------------------------------|---------|-----------------|
| 2.1 | `SqliteMetadata` (default for `beacon serve`, WAL, single writer)                 | ⬜       | production §2.1 |
| 2.2 | Deployment Scheduler (cron, catchup, timezone, backfill)                          | ⬜       | production §2.2 |
| 2.3 | `LocalBundle` sync (`beacon sync` CLI + `POST /sync`, content-hash version)       | ⬜       | production §2.3 |
| 2.4 | `beacon serve` process model (scheduler + worker + api, SIGTERM)                  | ⬜       | production §2.4 |
| 2.5 | API server — 14 endpoints incl. cancel / mark-state / clear (basic + bearer auth) | ⬜       | production §2.5 |
| 2.6 | Operational self-healing — stuck-task detector + log rotation + metadata GC       | ⬜       | production §2.6 |
| 2.7 | Prometheus metrics (8 metrics, bounded cardinality, dashboard)                    | ⬜       | production §2.7 |

**Cut from Phase 2** (nice-to-have, not should-have):
- `GitBundle` auto-pull / webhook — replaced by user-owned `git pull && beacon sync` in systemd/cron.
- Web UI — deferred to Phase 3; v1 ships with API + CLI.
- OIDC at the API — basic + bearer auth covers v1.
- Audit table for admin actions — log lines at action sites are enough.

## Phase 3 — Scale-out & UI (gated on real users)

See [`production_plan.md` §4 Phase 3](./production_plan.md#phase-3--scale-out--ui-only-when-2x-lands-real-users).
**Do not start until at least 3 teams are running Phase 2 in production.**

| #   | Deliverable                                              | Status  | DoD ref         |
|-----|----------------------------------------------------------|---------|-----------------|
| 3.1 | `PostgresMetadata` (asyncpg, advisory-lock leader)       | ⬜       | production §3.1 |
| 3.2 | `DockerExecutor` (per-task image, env-var TaskContext)   | ⬜       | production §3.2 |
| 3.3 | `KubernetesExecutor` (deferred, official client only)    | ⬜       | production §3.3 |
| 3.4 | `foreach_task` action type (fan-out, no nested mapping)  | ⬜       | production §3.4 |
| 3.5 | OIDC at the API (alongside basic + bearer)               | ⬜       | production §3.5 |
| 3.6 | Web UI v1 — read-mostly SPA (≤300 KB gzipped, no editor) | ⬜       | production §3.6 |

## Phase 4 — Ecosystem (conditional)

See [`production_plan.md` §4 Phase 4](./production_plan.md#phase-4--ecosystem-only-if-demand-exists).
**Each item requires a committed maintainer before work starts.**

| Deliverable                                            | Status  | Gate                            |
|--------------------------------------------------------|---------|---------------------------------|
| Remote plugin registry (`org/name@version` resolution) | ⬜       | Multiple orgs building plugins  |
| `BatchExecutor` (AWS Batch / Cloud Batch)              | ⬜       | Concrete need beyond Docker/K8s |
| `GcsLogSink`                                           | ⬜       | First user request (~50 LOC)    |
| `ElasticsearchLogSink`                                 | ⬜       | First user request (~50 LOC)    |

## Cut from scope (non-goals)

Intentionally **not** on the roadmap. See
[`production_plan.md` §2](./production_plan.md#2-non-goals-the-discipline)
for the rationale and the user-facing alternative for each.

| Item                                            | Status |
|-------------------------------------------------|--------|
| Multi-scheduler HA (active-active)              | 🚫     |
| Web UI in v1                                    | 🚫     |
| TOML / YAML config file in v1                   | 🚫     |
| Built-in git auto-pull (`GitBundle`)            | 🚫     |
| Secrets adapter (`SecretsProvider`)             | 🚫     |
| Connection / Variable editor UI                 | 🚫     |
| DAG editor UI                                   | 🚫     |
| RBAC with row-level permissions                 | 🚫     |
| Audit table for admin actions                   | 🚫     |
| OIDC auth in v1                                 | 🚫     |
| Email / Slack / PagerDuty built-in              | 🚫     |
| Lineage / Datasets / OpenLineage core emission  | 🚫     |
| Triggerer / deferrable operators                | 🚫     |
| Plugin marketplace (curated standard set only)  | 🚫     |
| SLA misses + DAG-level pools + priority weights | 🚫     |
| Arbitrary TaskGroup nesting visualization       | 🚫     |
| XCom-style cross-task data shuttle              | 🚫     |
| Dynamic task mapping outside `foreach_task`     | 🚫     |
| `BashOperator`-style implicit shell primitives  | 🚫     |
| Full REST API parity with Airflow               | 🚫     |

---

## v1.0 Exit Criteria

v1.0 ships when **all** Phase 1.5 + Phase 2 rows are ✅ AND every item
in [`production_plan.md` §3](./production_plan.md#3-definition-of-production-ready-v10-exit-criteria)
is demonstrable end-to-end on a fresh machine. Phase 3 and 4 items are
post-v1 and only proceed on demand.
