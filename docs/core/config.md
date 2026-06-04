# Configuration

**Beacon is configured by environment variables. There is no
`beacon.toml`, no `beacon.yml`, and no database-stored config.** That
is a deliberate choice — see
[`production_plan.md` §2](./production_plan.md#2-non-goals-the-discipline)
(TOML / YAML config file in v1 is a non-goal) and
[§3 exit criterion #6](./production_plan.md#3-definition-of-production-ready-v10-exit-criteria)
("all knobs are `BEACON_*` env vars. No config file in v1. No
DB-stored config ever.").

If the config surface ever grows past ~20 env vars we'll re-evaluate.
Today there are 8.

---

## Inspecting effective config

`beacon config show` prints every known setting, its effective value,
and where the value came from (`env` if you set the env var, `default`
otherwise):

```text
$ beacon config show
BEACON_METADATA_PATH                  ./metadata.db   (default)
BEACON_LOG_DIR                        /var/beacon/logs   (env)
BEACON_LOG_LEVEL                      INFO            (default)
BEACON_LOG_SINK                       file            (default)
BEACON_LOG_BATCH_SIZE                 100             (default)
BEACON_LOG_FLUSH_INTERVAL_MS          500             (default)
BEACON_SCHEDULER_TICK_SECONDS         5               (default)
BEACON_SCHEDULER_MAX_CONCURRENT_RUNS  8               (default)
```

Use this as the **first** debugging step when a setting "doesn't seem
to apply" — it shows what the running process actually sees.

---

## Reference

The single source of truth for this list is the `_SPEC` tuple in
[`beacon/cli/settings.py`](../../beacon/cli/settings.py). Update this
page whenever you add a setting there.

### Metadata

| Variable                | Default          | Type | Purpose                                                                                                                                                                                       |
|-------------------------|------------------|------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `BEACON_METADATA_PATH`  | `./metadata.db`  | str  | Directory used by `JsonMetadata` for `dag_runs/`, `task_contexts/`, `task_states/`, `deployments/`, `triggers/`. Every CLI command that reads or writes metadata uses this when unspecified. |

### Logging

| Variable                       | Default  | Type | Purpose                                                                                                            |
|--------------------------------|----------|------|--------------------------------------------------------------------------------------------------------------------|
| `BEACON_LOG_DIR`               | `./logs` | str  | Root directory the unified logging pipeline writes JSONL into (`{LOG_DIR}/{dag_id}/{run_id}/{task_id}/attempt_N.jsonl`). Consumed by `beacon logs`. |
| `BEACON_LOG_LEVEL`             | `INFO`   | str  | Minimum log level emitted (`DEBUG` / `INFO` / `WARNING` / `ERROR`).                                                |
| `BEACON_LOG_SINK`              | `file`   | str  | Backend for the logging pipeline. `file` writes to `BEACON_LOG_DIR`; `memory` keeps records in-process (testing). |
| `BEACON_LOG_BATCH_SIZE`        | `100`    | int  | Records buffered before a flush.                                                                                   |
| `BEACON_LOG_FLUSH_INTERVAL_MS` | `500`    | int  | Maximum delay (ms) before the buffer flushes even when not full.                                                  |

### Scheduler

| Variable                                | Default | Type | Purpose                                                                                            |
|-----------------------------------------|---------|------|----------------------------------------------------------------------------------------------------|
| `BEACON_SCHEDULER_TICK_SECONDS`         | `5`     | int  | Loop period for the deployment scheduler — how often it drains manual triggers + checks cron due. |
| `BEACON_SCHEDULER_MAX_CONCURRENT_RUNS`  | `8`     | int  | Hard cap on in-flight DagRuns across all deployments inside a single scheduler process.            |

---

## Setting them

### Shell session

```bash
export BEACON_METADATA_PATH=/srv/beacon/metadata
export BEACON_LOG_DIR=/var/log/beacon
beacon scheduler /srv/beacon/bundle
```

### systemd unit

```ini
# /etc/systemd/system/beacon-scheduler.service
[Service]
Environment="BEACON_METADATA_PATH=/srv/beacon/metadata"
Environment="BEACON_LOG_DIR=/var/log/beacon"
Environment="BEACON_SCHEDULER_TICK_SECONDS=5"
Environment="BEACON_SCHEDULER_MAX_CONCURRENT_RUNS=16"
ExecStart=/usr/local/bin/beacon scheduler /srv/beacon/bundle
Restart=always
```

### Docker / Compose

```yaml
services:
  beacon:
    image: beacon:latest
    environment:
      BEACON_METADATA_PATH: /var/beacon/metadata
      BEACON_LOG_DIR:       /var/beacon/logs
    volumes:
      - beacon-meta:/var/beacon/metadata
      - beacon-logs:/var/beacon/logs
```

### Kubernetes

```yaml
env:
  - name: BEACON_METADATA_PATH
    value: /var/beacon/metadata
  - name: BEACON_LOG_DIR
    value: /var/beacon/logs
  - name: BEACON_SCHEDULER_MAX_CONCURRENT_RUNS
    value: "16"
```

### Per-command override (no env var needed)

A few commands accept the matching value as a flag for one-off runs.
These take precedence over the env var:

| Env var                | Flag (where supported)                                            |
|------------------------|--------------------------------------------------------------------|
| `BEACON_METADATA_PATH` | `--metadata-path PATH` (`deploy`, `sync`, `list`, `trigger`, …)  |
| `BEACON_LOG_DIR`       | `--log-dir PATH` (`logs`)                                          |

---

## What about secrets?

Beacon **does not** ship a secrets adapter
([§1.5.2 "Cut"](./production_plan.md#cut-from-phase-15)). Inside a
`uses: py` task, read secrets with `os.environ.get("MY_API_KEY")` and
let your platform (Vault, AWS Secrets Manager, Kubernetes secrets,
1Password, your shell) put them in the environment. This keeps
secrets out of the metadata store by construction.

---

## What about per-DAG / per-deployment config?

That is **not** what env vars are for. Use:

- **DAG params** — declared in `dag.yml`, supplied per-deployment via
  `beacon deploy --param key=value`.
- **Scoped variables** — `dags/[<group>/]global_variables.yml` +
  `dags/<group>/<dag>/variables.yml`, layered with per-deployment
  `beacon deploy --var key=value`. See
  [`deploy.md`](./deploy.md#variable-resolution).
