"""CLI command for running the Beacon API server.

This command starts a merged API server + scheduler that can be
scaled horizontally with coordination via the metadata store.
"""

import asyncio
from pathlib import Path

import click

from ...api import run_server
from ...metadata import LocalMetadata
from ..settings import get


@click.command("api")
@click.argument(
    "bundle",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
)
@click.option(
    "--host",
    default="0.0.0.0",
    help="Host to bind (default: 0.0.0.0)",
)
@click.option(
    "--port",
    default=8080,
    type=int,
    help="Port to bind (default: 8080)",
)
@click.option(
    "--metadata-path",
    default=None,
    help="Metadata store path (default: BEACON_METADATA_PATH or ./metadata.db)",
)
@click.option(
    "--instance-id",
    default=None,
    help="Unique instance ID for coordination (auto-generated if not set)",
)
@click.option(
    "--tick-seconds",
    default=None,
    type=int,
    help="Scheduler tick interval in seconds",
)
@click.option(
    "--max-concurrent",
    default=None,
    type=int,
    help="Maximum concurrent DAG runs",
)
@click.option(
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Log level (default: INFO)",
)
def api(
    bundle: str,
    host: str,
    port: int,
    metadata_path: str | None,
    instance_id: str | None,
    tick_seconds: int | None,
    max_concurrent: int | None,
    log_level: str,
) -> None:
    """Run the Beacon API server with embedded scheduler.

    This starts both a REST API server and the scheduler loop in the
    same process. Multiple instances can be run for horizontal scaling
    with coordination via the metadata store to prevent duplicate runs.

    \b
    BUNDLE is the path to the DAG bundle directory.

    \b
    Examples:
        # Run a single instance
        beacon api ./my-bundle --port 8080

        # Run multiple instances for horizontal scaling
        beacon api ./my-bundle --port 8080 --instance-id inst-1 &
        beacon api ./my-bundle --port 8081 --instance-id inst-2 &

        # With custom metadata path
        beacon api ./my-bundle --metadata-path /var/lib/beacon/metadata
    """
    import logging

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Get defaults from settings
    meta_path = metadata_path or get("BEACON_METADATA_PATH")
    tick = tick_seconds or get("BEACON_SCHEDULER_TICK_SECONDS")
    max_conc = max_concurrent or get("BEACON_SCHEDULER_MAX_CONCURRENT_RUNS")

    # Create metadata store
    meta = LocalMetadata(meta_path)

    # Run the server
    asyncio.run(
        run_server(
            bundle_path=Path(bundle),
            meta=meta,
            host=host,
            port=port,
            tick_seconds=tick,
            max_concurrent_runs=max_conc,
            instance_id=instance_id,
        )
    )
