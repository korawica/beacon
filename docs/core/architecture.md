# Architecture

An architecture overview of this beacon package.

## Components

```text
Local --> Client --> UI (Web Server) <---> API Server  <---> Async Worker
                                            ^      ^           |
                                            |      |           |
                                            v      |           |(Append-only)
                                   Metadata Store  |           |
                                                   |           |
                                                   v           v
                                                  Logging Store
```

**Components**:

- Client (e.g. CLI, SDK)
- Web Server (Frontend)
- API Server (API Server)
- Async Worker (e.g. Local, Celery, Kubernetes)
  - LocalWorker: using by default, run tasks in local process, not distributed
  - (Experiment) CeleryWorker: run tasks in distributed Celery workers, persistent
  - (Experiment) KubernetesWorker: run tasks in distributed Kubernetes pods, persistent
- Metadata Store (e.g. Sqlite, Postgres)
  - MemoryMetadata: using by default, store metadata in memory, not persistent
  - (Experiment) SqliteMetadata: store metadata in local sqlite file, persistent
  - (Experiment) PostgresMetadata: store metadata in Postgres database, persistent
- Logging Store (e.g. Cloud Storage, S3, Elasticsearch)
  - MemoryLogging: using by default, store logs in memory, not persistent
  - LocalFileLogging: store logs in local file, not persistent

## DAG Parsing Flow

- DAG parsing from Bundle will tag version with its bundle version

  - `LocalBundle`: file content hash -> generate version
  - `GitBundle`: commit hash -> generate version
  - `GCSBundle`: file version -> generate version

## Model Flow

```text
Plugin --> Action --> Group? --> Dag --> Schedule
```

- **Plugin**: A reusable model that already implemented the logic of action
- **Action**: A unit of work that uses a plugin and defines the inputs
- **Group**: A collection of tasks that can be treated as a single unit (optional)
- **Dag**: A directed acyclic graph that defines the workflow, including tasks and their dependencies
- **Schedule**: A schedule that defines when the workflow should run and Dag parameters
