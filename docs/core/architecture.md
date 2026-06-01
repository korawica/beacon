# Architecture

```text
Client --> GCS/Git Remote --> Sync to Server --> Server --> UI
```

## Development Journey

```text
User --> 1.) Client DAG
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
