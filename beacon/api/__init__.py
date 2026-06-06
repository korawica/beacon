"""Beacon API Server.

A FastAPI-based REST API that also runs the scheduler loop.
Can be scaled horizontally with coordination via metadata store.

Usage
-----
Run via CLI::

    beacon api ./my-bundle --port 8080

Or programmatically::

    from beacon.api import run_server
    from beacon.metadata import LocalMetadata
    from pathlib import Path
    import asyncio

    meta = LocalMetadata("./metadata.db")
    asyncio.run(run_server(
        bundle_path=Path("./my-bundle"),
        meta=meta,
        port=8080,
    ))

Multi-Instance Coordination
---------------------------
When running multiple API server instances, they coordinate via the
metadata store to prevent duplicate runs:

1. Scheduled runs: Only one instance fires a run for each (deployment, logical_date)
2. Manual triggers: Each trigger is claimed by exactly one instance

This allows horizontal scaling without duplicates.
"""

from .app import create_app
from .server import run_server

__all__ = ["create_app", "run_server"]
