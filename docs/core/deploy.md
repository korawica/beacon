# Deploy Flow

How to package and operate Beacon workflows.

**Core principle:** the bundle on disk is **code only** — DAGs, plugins,
hooks, and supporting assets (Python files, SQL, JSON). **Deployments
live exclusively in the metadata store** and are created/updated/deleted
via the CLI (or, later, the API or UI). If you delete the metadata
store, every Deployment is gone — by design.

This split mirrors the [Design – DAG vs Deployment](./design.md#dag-vs-deployment-reuse-model)
reuse model: the same DAG template can have many Deployments, each
binding it to a different cron, params, and environment stage.

---

## Repository Structure

A bundle is a directory (typically a Git repo) holding **only the assets
the scheduler needs to load**. There is no `deployments/` folder.

```text
my-workflow-repo/
├── dags/                       # Reusable DAG templates (.yml or .py)
│   ├── extract_load_table.yml
│   ├── ml_training.py
│   └── reports/
│       └── daily_report.yml
├── plugins/                    # Custom plugins (auto-discovered)
│   ├── gcs_extract.py
│   └── bigquery_load.py
├── scripts/                    # Python files used by `uses: py`
│   ├── transform.py
│   └── validate.py
└── variables.yml               # Stage variables (dev / stg / prod)
```

DAGs are a flat collection referenced by `id`. There is no per-workflow
folder.

---

## Bundle Files

### DAG file (`./dags/extract_load_table.yml`)

A DAG is a **template** — no schedule, no concrete params, no stage. It
declares the param schema and the action graph. The same DAG file is
identical across dev / staging / prod.

```yaml
id: extract-load-table
desc: Extract from a source DB and load into a warehouse table

params:
  - name: source_system
    type: str
  - name: target_table
    type: str
  - name: run_date
    type: str
    default: "{{ runtime.logical_date | fmt('%Y-%m-%d') }}"

actions:
  - id: extract
    type: task
    uses: py
    inputs:
      py_file: ../scripts/extract.py
      params:
        source: "{{ params.source_system }}"
        date: "{{ params.run_date }}"

  - id: transform
    type: task
    upstream: [extract]
    uses: py
    inputs:
      py_file: ../scripts/transform.py
    retries: 2
    retry_delay: 30

  - id: load
    type: task
    upstream: [transform]
    uses: bigquery-load
    inputs:
      project: "{{ vars('gcp_project') }}"
      dataset: "{{ vars('dataset') }}"
      table: "{{ params.target_table }}"

callbacks:
  - on_event: failure
    hook: json-file
    inputs:
      alert_dir: "{{ vars('alert_path') }}"
```

### Variables file (`./variables.yml`)

Stage variables shared across all DAGs. The active stage is chosen
per-Deployment by setting `variables_ref` at `beacon deploy` time.

```yaml
type: variable
stages:
  dev:
    gcp_project: my-project-dev
    dataset:     raw_dev
    alert_path:  /tmp/alerts
  stg:
    gcp_project: my-project-stg
    dataset:     raw_stg
    alert_path:  /var/beacon/alerts
  prod:
    gcp_project: my-project-prod
    dataset:     raw_prod
    alert_path:  /var/beacon/alerts
```

### Custom plugin (`./plugins/gcs_extract.py`)

```python
from typing import ClassVar, Any
from beacon import BasePlugin, Context
from beacon.errors import TaskFailed


class GcsExtractPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-extract"

    bucket: str
    prefix: str

    async def execute(self, context: Context) -> dict[str, Any]:
        from google.cloud import storage
        client = storage.Client()
        blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
        if not blobs:
            raise TaskFailed(
                f"No files found at gs://{self.bucket}/{self.prefix}"
            )
        return {"files_found": len(blobs)}
```

No pip install, no provider package — drop a `.py` into `./plugins/` and
the bundle loader registers it automatically on next sync.

---

## Deployment Lifecycle (Metadata Store)

Deployments **never** live on disk. The CLI is the editor:

```bash
# Create / update a Deployment.
beacon deploy \
    --id daily-customers-from-postgres \
    --dag-id extract-load-table \
    --cron "0 2 * * *" \
    --timezone Asia/Bangkok \
    --variables-ref prod \
    --param source_system=postgres \
    --param target_table=customers \
    --owner data-platform

# A second Deployment can reuse the same DAG with different params.
beacon deploy \
    --id hourly-orders-from-mysql \
    --dag-id extract-load-table \
    --cron "0 * * * *" \
    --timezone Asia/Bangkok \
    --variables-ref prod \
    --param source_system=mysql \
    --param target_table=orders

# Inspect.
beacon list deployments

# Manually trigger a run (with optional one-off param override).
beacon trigger daily-customers-from-postgres
beacon trigger daily-customers-from-postgres --param target_table=customers_backfill
```

Re-running `beacon deploy --id X ...` with the same id **updates** the
record in place. Scheduler bookkeeping (`last_scheduled_at`) is preserved
across updates so changing a cron does not replay history.

### Consequence: ephemeral by design

If the metadata store is deleted (or you migrate to a fresh server),
every Deployment is gone. **Keep your `beacon deploy ...` invocations in
source control** — a `scripts/deploy_<env>.sh`, a Makefile target, or a
CI step. Re-applying the script rebuilds the environment from a clean
metadata store.

```bash
# scripts/deploy_prod.sh — checked in, run after metadata reset or
# whenever Deployment definitions change.
set -euo pipefail
export BEACON_METADATA_PATH=/srv/beacon-prod/metadata

beacon deploy --id daily-customers-from-postgres --dag-id extract-load-table \
    --cron "0 2 * * *" --variables-ref prod \
    --param source_system=postgres --param target_table=customers

beacon deploy --id hourly-orders-from-mysql --dag-id extract-load-table \
    --cron "0 * * * *" --variables-ref prod \
    --param source_system=mysql --param target_table=orders
```

---

## Sync Flow (bundle → scheduler)

The scheduler loads DAGs + plugins from a bundle directory at startup
and on `SIGHUP`. **It never reads deployments from disk**; it polls the
metadata store on every tick.

```text
┌────────────┐    git pull     ┌──────────────┐
│  Git repo  │ ──────────────► │ ./my-bundle/ │
└────────────┘                 │   dags/      │
                               │   plugins/   │
                               │   scripts/   │
                               └──────┬───────┘
                                      │ beacon sync       (validate)
                                      │ beacon scheduler PATH  (run)
                                      ▼
                          ┌─────────────────────────┐
                          │   beacon scheduler      │
                          │  (loads DAGs + plugins) │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   metadata store        │  ◄── beacon deploy
                          │   - deployments/        │  ◄── beacon trigger
                          │   - triggers/   (queue) │
                          │   - dag_runs/           │
                          └─────────────────────────┘
```

### Validation — `beacon sync`

`beacon sync PATH` reloads plugins and dry-runs every DAG in the bundle.
It exits non-zero if anything fails to parse — wire it into your CI
step or pre-restart hook.

```bash
beacon sync ./my-workflow-repo   # exits 1 on parse / dryrun failure
```

### Live re-load

```bash
# Pull, validate, reload (no scheduler restart).
git -C /srv/beacon/bundle pull
beacon sync /srv/beacon/bundle && kill -HUP $(pgrep -f 'beacon scheduler')
```

A systemd timer running this every minute is the supported "auto-sync"
recipe; **there is no built-in git poller in core** (see
[production_plan.md §2.3](./production_plan.md)).

---

## Environment Strategy

Beacon uses **branch-based deployment**: one Beacon instance per
environment, each pointed at a different bundle checkout. The bundle is
identical across environments (same DAGs, same plugins); environments
differ only in:

1. **Which stage of `variables.yml` is referenced** — set per Deployment
   via `--variables-ref`.
2. **Which Deployments exist** in that environment's metadata store.

| Environment | Bundle ref       | Metadata store          | Deployments use          |
|-------------|------------------|-------------------------|--------------------------|
| **dev**     | `develop` branch | `beacon-dev` (separate) | `--variables-ref dev`  |
| **staging** | `main` branch    | `beacon-stg` (separate) | `--variables-ref stg`  |
| **prod**    | `v*` tag         | `beacon-prod` (separate)| `--variables-ref prod` |

```text
feature/* ──► develop ──► main ──► tag v1.2.0
                 │          │           │
                 ▼          ▼           ▼
            beacon-dev  beacon-stg  beacon-prod
            (bundle =   (bundle =   (bundle = v1.2.0,
             develop)    main)       pinned)
```

### Promoting a Deployment to a new environment

Deployments do not promote — you re-apply your `scripts/deploy_<env>.sh`
against the target environment's Beacon. There is no "copy a YAML to the
prod folder" step.

```bash
BEACON_METADATA_PATH=/srv/beacon-prod/metadata ./scripts/deploy_prod.sh
```

---

## Versioning & Rollback

Every DAG load tags the records with a `dag_version` derived from the
bundle's content hash. In-flight runs are **pinned to their
`dag_version`** — a sync that ships a new version does not perturb runs
that were already executing.

### Rollback the bundle

```bash
git -C /srv/beacon/bundle checkout v1.1.0
beacon sync /srv/beacon/bundle && kill -HUP $(pgrep -f 'beacon scheduler')
```

Only **new** runs use the rolled-back DAGs. Deployments themselves do
not need to change.

### Pinning a Deployment to an older DAG version

`Deployment.dag_version` is an optional field intended for this case.
The CLI flag to set it is not wired up yet — until then, rolling back
the bundle is the operative mechanism.

---

## CI/CD Integration

The repository contains DAG / plugin code. CI's job is:

1. Lint + unit-test the repo.
2. Validate the bundle parses cleanly (`beacon sync` — exits non-zero on
   failure).
3. On merge / tag, ship the bundle to the target server (rsync, scp,
   git pull, container image bake — your choice).

CI **does not** edit Deployment records — those are owned by your
`scripts/deploy_<env>.sh` and re-applied only when intentional.

### Validate on every PR

```yaml
# .github/workflows/validate.yml
name: Validate bundle
on:
  pull_request:
  push: { branches: [develop, main] }

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install beacon
        run: pip install beacon
      - name: Validate every DAG
        run: beacon sync .
```

### Push bundle on tag (production)

```yaml
# .github/workflows/deploy-prod.yml
name: Push bundle to prod
on:
  push: { tags: ['v*'] }

jobs:
  push:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate
        run: pip install beacon && beacon sync .
      - name: Rsync to prod
        run: rsync -az --delete ./ deploy@beacon-prod:/srv/beacon/bundle/
      - name: Reload prod scheduler
        run: |
          ssh deploy@beacon-prod \
            'beacon sync /srv/beacon/bundle && kill -HUP $(pgrep -f "beacon scheduler")'
```

The script that authors / updates Deployments runs **separately** —
either on demand (a developer ran it from their laptop) or as a CI step
that explicitly opts in (e.g. only on a manual workflow_dispatch). Avoid
re-running it on every commit; Deployments rarely need to change.

---

## Cheat-sheet

| Want to…                              | Do                                            |
|---------------------------------------|-----------------------------------------------|
| Add / change a DAG                    | Edit `dags/*.yml`, commit, `beacon sync PATH` |
| Add / change a plugin                 | Drop `.py` in `plugins/`, `beacon sync PATH`  |
| Add a Deployment                      | `beacon deploy --id ... --dag-id ...`         |
| Change a Deployment's cron / params   | `beacon deploy --id <same> ...` again         |
| Pause a Deployment                    | `beacon deploy --id ... --disabled` *(re-deploy without `--disabled` to re-enable)* |
| Trigger an ad-hoc run                 | `beacon trigger <deployment-id>`              |
| Inspect what's deployed               | `beacon list deployments`                     |
| Inspect recent runs                   | `beacon list runs --dag-id <id>`              |
| Tail per-attempt logs                 | `beacon logs DAG_ID TASK_ID --run RUN_ID -f`  |
| Reload the bundle without restart     | `kill -HUP $(pgrep -f 'beacon scheduler')`    |
| Rebuild a fresh env from scratch      | Re-apply `scripts/deploy_<env>.sh`            |
