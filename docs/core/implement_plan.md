# Implement Plan

| Phase   | Deliverable                                             | Status   | Validates                                  |
|---------|---------------------------------------------------------|----------|--------------------------------------------|
| **0**   | `PythonPlugin.execute()` end-to-end                     | ✅ Done   | Core loop                                  |
| **0**   | TaskContext + Attempt + LocalExecutor                   | ✅ Done   | State persistence + retry                  |
| **0**   | Bundle plugin auto-discovery (`./plugins/`)             | ✅ Done   | Custom plugin deployment                   |
| **0**   | `load_context()` for user Python files                  | ✅ Done   | Runtime access without coupling            |
| **0**   | Task state machine with valid transitions               | ✅ Done   | Enforced lifecycle                         |
| **1**   | Callback system with registry resolution                | ✅ Done   | Callback parity Python/YAML                |
| **1**   | Async worker with retry scheduling                      | ✅ Done   | Full lifecycle transitions                 |
| **1**   | `MetadataProtocol` + `JsonMetadata` (sharded)           | ✅ Done   | Pluggable persistence, 1000+ DAGs          |
| **1**   | `TaskFailed` / `TaskSkipped` exception support          | ✅ Done   | Plugin-driven retry/skip control           |
| **1**   | `Deployment` model (reusable DAG + per-env config)      | ✅ Done   | DAG reuse without duplication              |
| **1**   | `Dag.run()` / `Dag.test()` / `Dag.dryrun()` methods    | ✅ Done   | Developer workflow (validate → test → run) |
| **1**   | LocalScheduler (trigger rules, branch, teardown, DAG callbacks) | ✅ Done | Full DAG lifecycle in-process       |
| **1**   | Setup & Teardown (`teardown` field + scheduler)         | ✅ Done   | Cluster/staging lifecycle                  |
| **1**   | Lean Jinja renderer (SandboxedEnvironment only)         | ✅ Done   | Two-pass: trigger-time + execute-time      |
| **2**   | GitBundle sync (webhook + git pull)                     | Pending  | Production deployment                      |
| **2**   | DockerExecutor / KubernetesExecutor                     | Pending  | Remote execution                           |
| **2**   | `foreach_task` action type                              | Pending  | Dynamic parallelism                        |
| **2**   | Deployment scheduler (cron + metadata-based versioning) | Pending  | Scale to 10k DAGs                          |
| **2**   | API Server (FastAPI) + Deployment CRUD endpoints        | Pending  | Programmatic control plane                 |
| **3**   | Remote plugin registry (`org/name@version`)             | Pending  | Ecosystem growth                           |
| **3**   | Web UI (Deployments list + DAG viewer + log viewer)     | Pending  | Observability                              |
| **3**   | BatchExecutor (AWS Batch / Cloud Batch)                 | Pending  | Cloud-native execution                     |
| **3**   | `SqliteMetadata` / `PostgresMetadata`                   | Pending  | Production-grade persistence               |
