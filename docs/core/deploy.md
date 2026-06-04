# Deploy Flow

How to package and operate Beacon workflows.

**Core principle:** the bundle on disk is **code + variables** — DAGs,
plugins, hooks, supporting assets (Python files, SQL, JSON), and the
default variable values for each scope. **Deployments live exclusively
in the metadata store** and are created/updated/deleted via the CLI
(or, later, the API or UI). If you delete the metadata store, every
Deployment is gone — by design.

This split mirrors the [Reference – Dag vs Deployment](./reference.md#dag-vs-deployment)
reuse model: the same DAG template can have many Deployments, each
binding it to a different cron, params, variable overrides, and
environment.

> **Policy decisions baked into this document.** These are firm
> conventions for the bundle layout. If you disagree with any of them
> push back — they're documented here precisely so they can be
> red-lined in one place.
>
> 1. **Stages are dropped.** Variable files are flat key→value maps.
>    Environments (dev / stg / prod) differ by which Beacon instance
>    (and which bundle checkout + metadata store) is running — not by
>    a `stages:` block inside a file.
> 2. **Variable scope is the path.** A `global_variables.yml` applies
>    only to DAGs **below** its folder. `dags/group/global_variables.yml`
>    does not affect siblings of `group/`.
> 3. **Precedence (highest → lowest):** deployment `--var` →
>    `dags/<group>/<dag>/variables.yml` →
>    `dags/<group>/global_variables.yml` →
>    `dags/global_variables.yml`.
> 4. **Shallow override** per top-level key. The whole value at a key
>    is replaced; there is no deep-merge of nested dicts.
> 5. **Asset paths are looked up local-first, then global, then raise.**
>    `py_file: transform.py` is resolved by trying
>    `dags/<group>/<dag>/assets/transform.py` first, then
>    `<bundle>/assets/transform.py`. If neither exists the task fails
>    with `FileNotFoundError`. The same lookup applies to any nested
>    subpath (e.g. `py_file: sub/transform.py`).
> 6. **DAG identity** is the explicit `id:` field inside `dag.yml`.
>    The folder path is organisational; it is not the id.
> 7. **One DAG per folder**, named `dag.yml` (or `dag.py`).
> 8. **Pinned deployments.** A Deployment that has at least one
>    `--var` override stored in metadata is **pinned**: a `beacon sync`
>    that ships a new bundle version does **not** auto-roll it. The
>    operator must run `beacon deployment sync <id>` (or `--all`) to
>    accept the new `dag_version`. Deployments with no `--var`
>    overrides auto-roll to the latest bundle version.

---

## Repository Structure

A bundle is a directory (typically a Git repo). There is no `deployments/` folder
— Deployments live in the metadata store that was created by CLI or API.

```text
my-team-repo/
 ├── dags/                                # DAG templates (.yml or .py)
 │   ├── global_variables.yml             # bundle-wide variable defaults
 │   └── group/
 │       ├── global_variables.yml         # group-wide variable defaults
 │       └── dag_name/
 │           ├── dag.yml                  # the DAG template
 │           ├── variables.yml            # dag-scope variable defaults
 │           └── assets/                  # dag-local files for `uses: py`
 │               ├── transform.py
 │               ├── ml_training.py
 │               └── validate.py
 ├── plugins/                             # Custom plugins (auto-discovered)
 │   ├── actions/                         # Custom action types (e.g. `uses: gcs_extract`)
 │   │   ├── gcs_extract.py
 │   │   └── bigquery_load.py
 │   └── callbacks/                       # Custom callbacks (e.g. `on_event: failure, hook: ms_team`)
 │       └── ms_team.py
 └── assets/                              # bundle-global files for `uses: py`
     ├── transform.py
     └── validate.py
```

Rules in one line each:

- One DAG per `dag_name/` folder; the file is `dag.yml`.
- A `variables.yml` next to a `dag.yml` only feeds that DAG.
- A `global_variables.yml` only feeds DAGs in its subtree.
- `py_file: transform.py` is resolved by trying the DAG's local
  `assets/transform.py` first, then the bundle-root
  `assets/transform.py`; if neither exists the task fails.
- `plugins/` is flat (or nested for organization) and autoloaded.

---

## Bundle Files

### DAG file (`dags/sales/extract_load_table/dag.yml`)

A DAG is a **template** file — no schedule, no environment, no concrete
runtime params. It declares the param schema and the action graph. The
same `dag.yml` is bit-identical across dev / staging / prod environments.

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
      py_file: extract.py                 # tried as ./assets/extract.py first
      params:
        source: "{{ params.source_system }}"
        date: "{{ params.run_date }}"

  - id: transform
    type: task
    upstream: [extract]
    uses: py
    inputs:
      py_file: transform.py               # falls back to /assets/transform.py
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

### Variables files

Each file is a flat key→value mapping. There is no `stages:` block.

```yaml
# dags/global_variables.yml — bundle-wide defaults
gcp_project: my-project
dataset:     raw
alert_path:  /var/beacon/alerts
```

```yaml
# dags/sales/global_variables.yml — overrides for every DAG in sales/
dataset: sales_raw
```

```yaml
# dags/sales/extract_load_table/variables.yml — overrides for this DAG
alert_path: /var/beacon/alerts/sales-extract
```

At trigger time, `vars('dataset')` for `extract-load-table` resolves
to `sales_raw` — the closest-scope file wins.

### Deployment-level overrides (no file)

Per-deployment overrides are passed at `beacon deploy` time and stored
in the metadata store, not on disk:

```bash
beacon deploy --id daily-customers-from-postgres \
    --dag-id extract-load-table \
    --var alert_path=/var/beacon/alerts/oncall-pager
```

This is the highest-precedence layer. Storing it in metadata also
**pins** the deployment — see [Pinned deployments](#pinned-deployments).

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

No pip install, no provider package — drop a `.py` into `./plugins/`
and the bundle loader registers it automatically on next sync.

---

## Variable Resolution

```text
                  ┌─────────────────────────────────────────┐
HIGHEST PRECEDENCE│ deployment overrides   (`--var k=v`)    │
                  ├─────────────────────────────────────────┤
                  │ dag variables.yml      (same folder)    │
                  ├─────────────────────────────────────────┤
                  │ group global_variables.yml              │
                  │   (any ancestor folder under dags/)     │
                  ├─────────────────────────────────────────┤
LOWEST PRECEDENCE │ dags/global_variables.yml               │
                  └─────────────────────────────────────────┘
```

- Resolution is **shallow**: the closest scope that defines a key
  wins, and replaces the whole value (no nested-dict merge).
- Resolution happens once at trigger time and the resulting flat dict
  is bound to the DagRun. Re-deploying after a run is in-flight does
  not perturb that run.
- A missing key renders as `<unresolved: vars('name')>` (same
  behaviour as today's `Renderer`).

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
    --param source_system=postgres \
    --param target_table=customers \
    --owner data-platform

# A second Deployment can reuse the same DAG with different params,
# and override a single variable for that deployment only.
beacon deploy \
    --id hourly-orders-from-mysql \
    --dag-id extract-load-table \
    --cron "0 * * * *" \
    --timezone Asia/Bangkok \
    --param source_system=mysql \
    --param target_table=orders \
    --var alert_path=/var/beacon/alerts/orders-oncall

# Inspect.
beacon list deployments

# Manually trigger a run (with optional one-off param override).
beacon trigger daily-customers-from-postgres
beacon trigger daily-customers-from-postgres --param target_table=customers_backfill
```

Re-running `beacon deploy --id X ...` with the same id **updates** the
record in place. Scheduler bookkeeping (`last_scheduled_at`) is
preserved across updates so changing a cron does not replay history.

### Pinned deployments

A deployment is **pinned** the moment it stores at least one `--var`
override in metadata. Pinning changes one thing only: when a fresh
bundle (a new `dag_version`) is synced, the deployment is **not**
auto-rolled to the new version. It stays bound to the `dag_version`
it was last accepted on.

| Deployment state                | Behaviour on `beacon sync`                     |
|---------------------------------|------------------------------------------------|
| No `--var` overrides            | Auto-rolls to the new `dag_version`            |
| Has `--var` overrides (pinned)  | Stays on old `dag_version`; flagged as `stale` |

To promote a pinned deployment to a new bundle version:

```bash
# Preview the variable diff first.
beacon deployment diff daily-customers-from-postgres

# Accept the new dag_version for one deployment…
beacon deployment sync daily-customers-from-postgres

# …or for everything currently stale.
beacon deployment sync --all
```

`beacon list deployments` shows `dag_version` and marks stale ones:

```text
ID                                  DAG_ID                 VERSION   STATE
daily-customers-from-postgres       extract-load-table     a1b2c3d   stale (bundle: e5f6a7b)
hourly-orders-from-mysql            extract-load-table     e5f6a7b   ok
```

### Consequence: ephemeral by design

If the metadata store is deleted (or you migrate to a fresh server),
every Deployment is gone. **Keep your `beacon deploy ...` invocations
in source control** — a `scripts/deploy_<env>.sh`, a Makefile target,
or a CI step. Re-applying the script rebuilds the environment from a
clean metadata store.

```bash
# scripts/deploy_prod.sh — checked in, run after metadata reset or
# whenever Deployment definitions change.
set -euo pipefail
export BEACON_METADATA_PATH=/srv/beacon-prod/metadata

beacon deploy --id daily-customers-from-postgres --dag-id extract-load-table \
    --cron "0 2 * * *" \
    --param source_system=postgres --param target_table=customers

beacon deploy --id hourly-orders-from-mysql --dag-id extract-load-table \
    --cron "0 * * * *" \
    --param source_system=mysql --param target_table=orders \
    --var alert_path=/var/beacon/alerts/orders-oncall
```

---

## Sync Flow (bundle → scheduler)

The scheduler loads DAGs + plugins + variable files from a bundle
directory at startup and on `SIGHUP`. **It never reads deployments
from disk**; it polls the metadata store on every tick.

```text
┌────────────┐    git pull     ┌──────────────┐
│  Git repo  │ ──────────────► │ ./my-bundle/ │
└────────────┘                 │   dags/      │  (DAGs + variables files)
                               │   plugins/   │
                               │   assets/    │
                               └──────┬───────┘
                                      │ beacon sync       (validate)
                                      │ beacon scheduler PATH  (run)
                                      ▼
                          ┌─────────────────────────┐
                          │   beacon scheduler      │
                          │  (loads DAGs + plugins  │
                          │   + variable scopes)    │
                          └────────────┬────────────┘
                                       │
                                       ▼
                          ┌─────────────────────────┐
                          │   metadata store        │  ◄── beacon deploy
                          │   - deployments/        │  ◄── beacon trigger
                          │   - triggers/   (queue) │  ◄── beacon deployment sync
                          │   - dag_runs/           │
                          └─────────────────────────┘
```

### Validation — `beacon sync`

`beacon sync PATH` reloads plugins, loads every variables file, and
dry-runs every DAG in the bundle. It exits non-zero if anything fails
to parse — wire it into your CI step or pre-restart hook.

```bash
beacon sync ./my-workflow-repo   # exits 1 on parse / dryrun failure
```

Sync also computes the new `dag_version`. Deployments without `--var`
overrides are rolled forward in metadata automatically; pinned
deployments are left on their old version and marked stale.

### Live re-load

```bash
# Pull, validate, reload (no scheduler restart).
git -C /srv/beacon/bundle pull
beacon sync /srv/beacon/bundle && kill -HUP $(pgrep -f 'beacon scheduler')
```

A systemd timer running this every minute is the supported "auto-sync"
recipe; **there is no built-in git poller in core** (see
[roadmap.md §5.5](./roadmap.md)).

---

## Environment Strategy

Beacon uses **branch-based deployment**: one Beacon instance per
environment, each pointed at a different bundle checkout + metadata
store. The bundle is identical across environments (same DAGs, same
plugins, same variable files); environments differ only in:

1. **Which bundle checkout is loaded** (develop / main / tagged
   release).
2. **Which metadata store backs the scheduler** (each env has its
   own).
3. **Which Deployments exist** in that environment's metadata store —
   typically created with environment-specific `--var` overrides.

| Environment | Bundle ref       | Metadata store              |
|-------------|------------------|-----------------------------|
| **dev**     | `develop` branch | `beacon-dev` (separate)     |
| **staging** | `main` branch    | `beacon-stg` (separate)     |
| **prod**    | `v*` tag         | `beacon-prod` (separate)    |

```text
feature/* ──► develop ──► main ──► tag v1.2.0
                 │          │           │
                 ▼          ▼           ▼
            beacon-dev  beacon-stg  beacon-prod
            (bundle =   (bundle =   (bundle = v1.2.0,
             develop)    main)       pinned)
```

### Promoting a Deployment to a new environment

Deployments do not promote — you re-apply your
`scripts/deploy_<env>.sh` against the target environment's Beacon.
There is no "copy a YAML to the prod folder" step.

```bash
BEACON_METADATA_PATH=/srv/beacon-prod/metadata ./scripts/deploy_prod.sh
```

---

## Versioning & Rollback

Every bundle sync computes a `dag_version` from the bundle's content
hash. In-flight runs are **pinned to their `dag_version`** — a sync
that ships a new version does not perturb runs that were already
executing.

### Rollback the bundle

```bash
git -C /srv/beacon/bundle checkout v1.1.0
beacon sync /srv/beacon/bundle && kill -HUP $(pgrep -f 'beacon scheduler')
```

Only **new** runs use the rolled-back DAGs. Non-pinned deployments
follow the rollback; pinned deployments stay on whatever version they
last accepted (run `beacon deployment sync` to align them).

### Pinning a Deployment to an older DAG version

`Deployment.dag_version` is the field that records the bound version.
For non-pinned deployments it's overwritten on every sync; for pinned
deployments it changes only via `beacon deployment sync <id>`.

---

## CI/CD Integration

The repository contains DAG / plugin / asset / variable code. CI's job
is:

1. Lint + unit-test the repo.
2. Validate the bundle parses cleanly (`beacon sync` — exits non-zero
   on failure).
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

After a prod sync, any pinned deployment is marked `stale`. Operators
review with `beacon deployment diff` and accept with
`beacon deployment sync <id>` — there is no auto-roll for pinned
deployments.

---

## Cheat-sheet

| Want to…                                  | Do                                                                                                 |
|-------------------------------------------|----------------------------------------------------------------------------------------------------|
| Add / change a DAG                        | Edit `dags/<group>/<dag>/dag.yml`, commit, `beacon sync PATH`                                      |
| Add a dag-local asset                     | Drop file in `dags/<group>/<dag>/assets/`, reference as bare filename in `py_file:`                |
| Add a bundle-global asset                 | Drop file in `assets/`, reference as bare filename in `py_file:` (used when no local file matches) |
| Change a default variable                 | Edit the closest `variables.yml` / `global_variables.yml`, `beacon sync`                           |
| Add / change a plugin                     | Drop `.py` in `plugins/`, `beacon sync PATH`                                                       |
| Add a Deployment                          | `beacon deploy --id ... --dag-id ...`                                                              |
| Per-deployment variable override          | `beacon deploy --id ... --var key=value`                                                           |
| Change a Deployment's cron / params       | `beacon deploy --id <same> ...` again                                                              |
| Pause a Deployment                        | `beacon deploy --id ... --disabled` *(re-deploy without `--disabled` to re-enable)*                |
| Trigger an ad-hoc run                     | `beacon trigger <deployment-id>`                                                                   |
| Inspect what's deployed                   | `beacon list deployments`                                                                          |
| See which deployments need promotion      | `beacon list deployments` (look for `stale`)                                                       |
| Preview variable diff before promoting    | `beacon deployment diff <id>`                                                                      |
| Accept new `dag_version` for a pinned dep | `beacon deployment sync <id>`                                                                      |
| Inspect recent runs                       | `beacon list runs --dag-id <id>`                                                                   |
| Tail per-attempt logs                     | `beacon logs DAG_ID TASK_ID --run RUN_ID -f`                                                       |
| Reload the bundle without restart         | `kill -HUP $(pgrep -f 'beacon scheduler')`                                                         |
| Rebuild a fresh env from scratch          | Re-apply `scripts/deploy_<env>.sh`                                                                 |
