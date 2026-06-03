# Deploy Flow

How to package and deploy Beacon workflows. A bundle separates **DAG templates**
(`./dags/`) from **Deployments** (`./deployments/`) — one DAG can have many
Deployments, each binding it to a specific cron, params, stage, and identity.

See also: [Design – DAG vs Deployment](./design.md#dag-vs-deployment-reuse-model).

---

## Repository Structure

```text
my-workflow-repo/
├── dags/                       # Reusable DAG templates
│   ├── extract_load_table.yml
│   ├── ml_training.py
│   └── reports/
│       └── daily_report.yml
├── deployments/                # Bindings of a DAG → cron + params + stage
│   ├── daily_customers_postgres.yml
│   ├── hourly_orders_mysql.yml
│   └── manual_snowflake_backfill.yml
├── plugins/                    # Custom plugins (auto-discovered)
│   ├── gcs_extract.py
│   └── bigquery_load.py
├── scripts/                    # Python files used by `uses: py`
│   ├── transform.py
│   └── validate.py
└── variables.yml               # Stage variables (dev / stg / prod)
```

There is **no** per-workflow folder. DAGs and Deployments are flat collections
referenced by `id`. A `Deployment.dag_id` resolves to a `Dag.id`.

---

## File Definitions

### DAG File (`./dags/extract_load_table.yml`)

A DAG is a **template** — no schedule, no concrete params, no stage. It declares
the params schema and the action graph.

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

### Deployment File (`./deployments/daily_customers_postgres.yml`)

A Deployment binds a DAG to a specific schedule, params, and stage. Its `id`
is what appears in the UI's Deployments list.

```yaml
id: daily-customers-from-postgres
dag_id: extract-load-table    # must resolve to a known Dag
dag_version: null              # null = latest; or pin a commit SHA / tag

cron: "0 2 * * *"
timezone: Asia/Bangkok
start_date: "2026-01-01"
end_date: null
catch_up: false
enabled: true

variables_ref: prod            # which stage in variables.yml to use

params:
  source_system: postgres
  target_table: customers

owners: [data-platform]
labels:
  domain: crm
  tier: critical
```

A second Deployment can reuse the same DAG with different params:

```yaml
# ./deployments/hourly_orders_mysql.yml
id: hourly-orders-from-mysql
dag_id: extract-load-table
cron: "0 * * * *"
timezone: Asia/Bangkok
variables_ref: prod
params:
  source_system: mysql
  target_table: orders
```

### Variables File (`./variables.yml`)

Stage variables shared across all DAGs and Deployments. The active stage is
chosen per-Deployment via `variables_ref`.

```yaml
type: variable
stages:
  dev:
    gcp_project: my-project-dev
    dataset: raw_dev
    alert_path: /tmp/alerts
  stg:
    gcp_project: my-project-stg
    dataset: raw_stg
    alert_path: /var/beacon/alerts
  prod:
    gcp_project: my-project-prod
    dataset: raw_prod
    alert_path: /var/beacon/alerts
```

### Plugin Code (`./plugins/gcs_extract.py`)

```python
from typing import ClassVar, Any
from beacon.core import BasePlugin, Context
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

No pip install, no provider package — drop a `.py` in `./plugins/`, the bundle
loader registers it automatically.

---

## Environment Strategy

Beacon uses **branch-based deployment** with GitBundle. Each environment runs a
separate Beacon instance pointed at a specific branch / ref.

| Environment | Branch/Ref      | Trigger                     | `variables_ref` per Deployment |
|-------------|-----------------|-----------------------------|--------------------------------|
| **dev**     | `develop`       | Push to develop             | `dev`                          |
| **staging** | `main`          | Merge PR to main            | `stg`                          |
| **prod**    | `v*` tag        | Tag from main (e.g. v1.2.0) | `prod`                         |

```text
feature/* ──> develop ──> main ──> tag v1.2.0
                 │          │           │
                 v          v           v
             beacon-dev  beacon-stg  beacon-prod
             (auto sync) (auto sync) (manual tag)
```

A team typically maintains **separate copies of the deployment files per
environment** — `daily-customers-from-postgres.yml` in the prod branch sets
`variables_ref: prod`; in dev it sets `variables_ref: dev`. The DAGs themselves
are identical across environments.

---

## GitBundle Configuration

Each Beacon instance loads one bundle. The bundle pulls from Git and reparses
on each version change.

```yaml
# beacon-dev.yml
bundle:
  type: git
  name: data-workflows
  repo_url: git@github.com:my-org/my-data-workflows.git
  branch: develop
  sync_interval: 60          # seconds, poll for new commits
```

```yaml
# beacon-prod.yml
bundle:
  type: git
  name: data-workflows
  repo_url: git@github.com:my-org/my-data-workflows.git
  branch: main
  ref: v1.2.0                 # pin to tag for production
```

---

## Sync Flow

### Development (auto-deploy on push)

```text
Developer pushes to `develop`
    │
    v
beacon-dev (polling every 60s):
    1. git fetch → detect new commit on develop
    2. git checkout develop
    3. dag_version = commit SHA (e.g. abc1234)
    4. bundle.load_plugins()    → register ./plugins/*.py
    5. bundle.discover_dags()   → parse ./dags/**.yml,.py
       - validate plugins exist, no cycles
       - store DagRecord(id, version, serialized_dag)
    6. bundle.discover_deployments() → parse ./deployments/**.yml
       - validate dag_id resolves to a known Dag
       - store DeploymentRecord
    7. Old DAG versions preserved (running instances unaffected)
```

### Production (deploy on tag)

```text
Release manager tags main:  git tag v1.2.0 && git push --tags
    │
    v
beacon-prod (webhook or polling):
    1. git fetch --tags → detect new tag
    2. git checkout v1.2.0
    3. dag_version = "v1.2.0"
    4. Reparse plugins / dags / deployments (same flow as dev)
    5. Running DagRun (v1.1.0) preserved via TaskContext.dag_version
```

---

## Variable Resolution Order

Variables are merged with priority (last wins):

```text
variables.yml at bundle root        (lowest priority)
    └── (future) per-domain variables.yml
            └── (future) per-DAG variables.yml   (highest priority)
```

All resolved against the active stage selected by `Deployment.variables_ref`.

---

## Deploy Commands

### CLI — deploy bundle

```bash
# Deploy the bundle (parse + register all DAGs and Deployments)
beacon bundle sync \
    --bundle ./my-workflow-repo \
    --name data-workflows

# Or from Git
beacon bundle sync \
    --bundle git@github.com:my-org/my-data-workflows.git \
    --ref v1.2.0 \
    --name data-workflows
```

### CLI — manage a Deployment

```bash
# Enable / disable a deployment without touching files
beacon deployment enable daily-customers-from-postgres
beacon deployment disable daily-customers-from-postgres

# Trigger an ad-hoc run (uses the deployment's params + variables_ref)
beacon deployment trigger daily-customers-from-postgres

# Override params for one run
beacon deployment trigger daily-customers-from-postgres \
    --params target_table=customers_backfill
```

---

## Rollback

Because every parse tags `DagRecord` with a version (commit SHA / git tag),
rollback is just re-pointing the bundle:

```bash
# Rollback prod to previous tag
beacon bundle sync --ref v1.1.0
```

Or pin in the bundle config:

```yaml
# beacon-prod.yml
bundle:
  ref: v1.1.0   # rollback: was v1.2.0
```

Running DagRuns on v1.2.0 continue unaffected — each `TaskContext` carries
`dag_version: v1.2.0`. Only **new** runs use the rolled-back version. To pin a
specific Deployment to an old DAG version independently of the bundle pointer,
set `Deployment.dag_version`.

---

## CI/CD Integration

### Production deploy on tag (GitHub Actions)

```yaml
# .github/workflows/deploy-prod.yml
name: Deploy to Production
on:
  push:
    tags: ['v*']

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Validate bundle
        run: beacon bundle validate --bundle .

      - name: Sync bundle to beacon-prod
        run: |
          beacon bundle sync \
            --bundle . \
            --ref ${{ github.ref_name }}
        env:
          BEACON_API_URL:   ${{ secrets.BEACON_PROD_URL }}
          BEACON_API_TOKEN: ${{ secrets.BEACON_PROD_TOKEN }}
```

### Dev auto-deploy on push to `develop`

```yaml
# .github/workflows/deploy-dev.yml
name: Deploy to Dev
on:
  push:
    branches: [develop]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Sync bundle to beacon-dev
        run: beacon bundle sync --bundle .
        env:
          BEACON_API_URL:   ${{ secrets.BEACON_DEV_URL }}
          BEACON_API_TOKEN: ${{ secrets.BEACON_DEV_TOKEN }}
```
