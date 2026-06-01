# Architecture

```text
Client --> GCS/Git Remote --> Sync to Server --> Server --> UI
```

**Components**:

- Client (e.g. CLI, SDK)
- Web Server (Frontend)
- Backend Server (API Server with Async Worker)
- Metadata Store (e.g. Sqlite, Postgres)

## Development Journey

```text
User --> 1.) Client DAG with YAML/Python
         2.) Develop DAG
         3.) Test & Validate DAG
         4.) Sync to Development
         5.) UI
         6.) Manual Trigger
         6.1) Back to 1.) if it not ready
         7.) Review & Sync to Production
         8.) UI
         9.) Sync Schedule
         10.) Monitor
```
