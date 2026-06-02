# Architecture

## Components

```text
Client --> UI (Web Server) <---> API Server  <---> Async Worker
                                  |                  |
                                  |                  |
                                  v                  v
                                 [      Database      ]
```

**Components**:

- Client (e.g. CLI, SDK)
- Web Server (Frontend)
- API Server (API Server)
- Async Worker (e.g. Local, Celery, Kubernetes)
- Metadata Database (e.g. Sqlite, Postgres)

## Model Flow

```text
Plugin --> Task --> Group? --> Dag --> Schedule
```

- **Plugin**: A reusable model that already implemented the logic of action
- **Task**: A unit of work that uses a plugin and defines the inputs
- **Group**: A collection of tasks that can be treated as a single unit (optional)
- **Dag**: A directed acyclic graph that defines the workflow, including tasks and their dependencies
- **Schedule**: A schedule that defines when the workflow should run and Dag parameters
