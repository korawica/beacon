# Deploy Flow

How to deploy beacon workflows using GitBundle with environment separation.

---

## Repository Structure

```text
my-data-workflows/
├── plugins/                          # Shared custom plugins
│   ├── gcs/
│   │   └── sensor.py
│   └── bigquery/
│       └── loader.py
├── workflows/
│   ├── domain_a/
│   │   ├── etl_pipeline/
│   │   │   ├── workflow.yml          # DAG definition
│   │   │   ├── variables.yml         # Stage-specific vars (dev/prod)
│   │   │   └── scripts/
│   │   │       └── transform.py
│   │   └── global_variables.yml      # Shared vars for domain_a
│   └── domain_b/
│       └── reporting/
│           ├── workflow.yml
│           └── variables.yml
└── global_variables.yml              # Shared vars for all domains
```

---

## Environment Strategy

Beacon uses **branch-based deployment** with GitBundle:

| Environment | Branch/Ref      | Trigger                     | Variables Stage  |
|-------------|-----------------|-----------------------------|------------------|
| **dev**     | `develop`       | Push to develop             | `stages.dev`     |
| **staging** | `main`          | Merge PR to main            | `stages.stg`     |
| **prod**    | `v*` tag        | Tag from main (e.g. v1.2.0) | `stages.prod`    |

```text
┌────────────────────────────────────────────────────────────────────┐
│ Git Flow                                                            │
├────────────────────────────────────────────────────────────────────┤
│                                                                    │
│  feature/* ──> develop ──> main ──> tag v1.2.0                      │
│                   │          │           │                           │
│                   v          v           v                           │
│               beacon-dev  beacon-stg  beacon-prod                   │
│               (auto sync) (auto sync) (manual tag)                  │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

---

## GitBundle Configuration

Each environment runs a separate beacon instance with its own GitBundle config:

```yaml
# beacon-dev.yml
bundle:
  type: git
  name: data-workflows
  repo_url: git@github.com:my-org/my-data-workflows.git
  branch: develop
  sync_interval: 60  # seconds, poll for changes
  variables_stage: dev

# beacon-prod.yml
bundle:
  type: git
  name: data-workflows
  repo_url: git@github.com:my-org/my-data-workflows.git
  branch: main
  ref: v1.2.0          # pin to tag for production
  variables_stage: prod
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
    3. version = commit SHA (e.g. abc1234)
    4. Load plugins/ → register custom plugins
    5. Parse workflows/**/*.yml with stage=dev
       - vars('bucket') → resolves from variables.yml stages.dev
    6. Store DAGs versioned as abc1234
    7. Running tasks on old version unaffected
```

### Production (deploy on tag)

```text
Release manager tags main: `git tag v1.2.0`
    │
    v
beacon-prod (webhook or polling):
    1. git fetch --tags → detect new tag
    2. git checkout v1.2.0
    3. version = "v1.2.0"
    4. Load plugins/ → register custom plugins
    5. Parse workflows/**/*.yml with stage=prod
       - vars('bucket') → resolves from variables.yml stages.prod
    6. Store DAGs versioned as v1.2.0
    7. Old DAG version (v1.1.0) preserved for running instances
```

---

## File Definitions

### Workflow File

```yaml
# workflows/domain_a/etl_pipeline/workflow.yml
id: etl-pipeline
desc: Extract and transform data from source systems
params:
  - name: source_system
    type: str
    default: example
  - name: run_date
    type: str
    default: "{{ runtime.logical_date | fmt('%Y-%m-%d') }}"
tasks:
  - id: extract
    type: task
    uses: gcs-sensor
    inputs:
      bucket: "{{ vars('bucket') }}"
      prefix: "raw/{{ params.source_system }}/{{ params.run_date }}"

  - id: transform
    type: task
    uses: py
    upstream: [extract]
    inputs:
      py_file: ./scripts/transform.py
      py_function: main
      params:
        source_system: "{{ params.source_system }}"
        bucket: "{{ vars('bucket') }}"
    retries: 2
    retry_delay: 30

  - id: load
    type: task
    uses: bigquery-load
    upstream: [transform]
    inputs:
      project: "{{ vars('gcp_project') }}"
      dataset: "{{ vars('dataset') }}"
      table: "{{ params.source_system }}_raw"

callbacks:
  - on_event: failure
    hook: json-file
    inputs:
      alert_dir: "{{ vars('alert_path') }}"
```

### Variables File

```yaml
# workflows/domain_a/etl_pipeline/variables.yml
type: variable
stages:
  dev:
    bucket: data-lake-dev
    gcp_project: my-project-dev
    dataset: raw_dev
    alert_path: /tmp/alerts
  stg:
    bucket: data-lake-stg
    gcp_project: my-project-stg
    dataset: raw_stg
    alert_path: /var/beacon/alerts
  prod:
    bucket: data-lake-prod
    gcp_project: my-project-prod
    dataset: raw_prod
    alert_path: /var/beacon/alerts
```

### Global Variables

```yaml
# global_variables.yml (shared across all workflows)
type: variable
stages:
  dev:
    environment: development
    log_level: debug
  stg:
    environment: staging
    log_level: info
  prod:
    environment: production
    log_level: warning
```

### Plugin Code

```python
# plugins/gcs/sensor.py
from typing import ClassVar, Any
from beacon.core import BasePlugin, Context


class GcsSensorPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-sensor"

    bucket: str
    prefix: str

    async def execute(self, context: Context) -> dict[str, Any]:
        from google.cloud import storage
        client = storage.Client()
        blobs = list(client.list_blobs(self.bucket, prefix=self.prefix))
        if not blobs:
            raise FileNotFoundError(
                f"No files found at gs://{self.bucket}/{self.prefix}"
            )
        return {"files_found": len(blobs)}
```

---

## Variable Resolution Order

Variables are merged with priority (last wins):

```text
global_variables.yml          (lowest priority)
    └── workflows/domain/global_variables.yml
            └── workflows/domain/workflow-id/variables.yml  (highest priority)
```

All resolved against the active `stage` (dev/stg/prod) set by the bundle config.

---

## Deploy Commands

### Manual Deploy (CLI)

```bash
# Deploy to dev
beacon deploy etl-pipeline \
    --bundle ./my-data-workflows \
    --stage dev

# Deploy to prod with specific tag
beacon deploy etl-pipeline \
    --bundle git@github.com:my-org/my-data-workflows.git \
    --ref v1.2.0 \
    --stage prod \
    --schedule "0 2 * * *" \
    --timezone "Asia/Bangkok"
```

### Schedule Configuration

```bash
beacon deploy etl-pipeline \
    --schedule "0 2 * * *" \
    --start-date "2024-01-01" \
    --timezone "Asia/Bangkok" \
    --catchup false \
    --params source_system=orders
```

---

## Rollback

Since every deploy is tagged with a version (commit SHA or git tag),
rollback is simply re-deploying an older version:

```bash
# Rollback prod to previous tag
beacon deploy etl-pipeline --ref v1.1.0 --stage prod
```

Or in GitBundle config, pin to the previous tag:

```yaml
# beacon-prod.yml
bundle:
  ref: v1.1.0  # rollback: was v1.2.0
```

Running DAG instances on v1.2.0 continue unaffected (their TaskContext has `dag_version: v1.2.0`). Only new runs use the rolled-back version.

---

## CI/CD Integration

### GitHub Actions Example

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

      - name: Validate DAGs
        run: beacon validate --bundle . --stage prod

      - name: Deploy
        run: |
          beacon deploy --all \
            --bundle . \
            --stage prod \
            --ref ${{ github.ref_name }}
        env:
          BEACON_API_URL: ${{ secrets.BEACON_PROD_URL }}
          BEACON_API_TOKEN: ${{ secrets.BEACON_PROD_TOKEN }}
```

### Development (auto-deploy on develop)

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
      - name: Deploy to dev
        run: beacon deploy --all --bundle . --stage dev
        env:
          BEACON_API_URL: ${{ secrets.BEACON_DEV_URL }}
```
