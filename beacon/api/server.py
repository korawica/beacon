"""Server runner for Beacon API with embedded scheduler.

This module provides the main entry point for running the merged
API server + scheduler. It starts both the FastAPI server and the
scheduler loop in the same process.

Multi-Instance Support
----------------------
Multiple instances can be run simultaneously. They coordinate via
the metadata store to prevent duplicate runs:

    # Terminal 1
    beacon api ./my-bundle --port 8080 --instance-id inst-1

    # Terminal 2
    beacon api ./my-bundle --port 8081 --instance-id inst-2

Both instances will serve API requests and run the scheduler loop,
but only one will fire each scheduled run.
"""

import asyncio
import logging
import signal
from pathlib import Path

from ..metadata import LocalMetadata
from ..scheduler import DeploymentScheduler
from .app import create_app

logger = logging.getLogger("beacon.api")


async def run_server(
    bundle_path: Path,
    meta: LocalMetadata,
    *,
    host: str = "0.0.0.0",
    port: int = 8080,
    tick_seconds: int = 5,
    max_concurrent_runs: int = 8,
    instance_id: str | None = None,
) -> None:
    """Run the merged API + Scheduler server.

    This starts both the FastAPI HTTP server and the scheduler loop
    in the same process. Multiple instances can be run for horizontal
    scaling with coordination via the metadata store.

    Args:
        bundle_path: Path to the DAG bundle directory
        meta: Metadata store instance
        host: Host to bind the HTTP server
        port: Port to bind the HTTP server
        tick_seconds: Scheduler tick interval in seconds
        max_concurrent_runs: Maximum concurrent DAG runs
        instance_id: Unique instance identifier (auto-generated if not set)
    """
    # Import uvicorn here to avoid import errors when not installed
    import uvicorn

    # Create scheduler instance
    scheduler = DeploymentScheduler(
        bundle_path=bundle_path,
        meta=meta,
        tick_seconds=tick_seconds,
        max_concurrent_runs=max_concurrent_runs,
        instance_id=instance_id,
    )

    # Load DAGs
    scheduler.reload()

    # Create FastAPI app
    app = create_app(bundle_path, meta, scheduler)

    # Start scheduler in background
    scheduler_task = asyncio.create_task(scheduler.run())

    # Setup signal handlers for graceful shutdown
    stop_event = asyncio.Event()

    def handle_shutdown() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()
        scheduler._stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, handle_shutdown)
        except NotImplementedError:
            pass  # Windows / restricted envs

    # Configure uvicorn
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        loop="asyncio",
        log_level="info",
    )
    server = uvicorn.Server(config)

    logger.info(
        "Starting Beacon API server on %s:%s (instance=%s)",
        host,
        port,
        scheduler.instance_id,
    )

    try:
        await server.serve()
    finally:
        # Ensure scheduler stops
        scheduler._stop.set()
        try:
            await asyncio.wait_for(scheduler_task, timeout=30.0)
        except TimeoutError:
            logger.warning("Scheduler did not stop gracefully, cancelling")
            scheduler_task.cancel()
            try:
                await scheduler_task
            except asyncio.CancelledError:
                pass


def main() -> None:
    """CLI entry point for beacon-api command."""
    import argparse
    import os

    parser = argparse.ArgumentParser(
        description="Beacon API Server with embedded scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run a single instance
  beacon-api ./my-bundle --port 8080

  # Run multiple instances for horizontal scaling
  beacon-api ./my-bundle --port 8080 --instance-id inst-1 &
  beacon-api ./my-bundle --port 8081 --instance-id inst-2 &

  # With custom metadata path
  beacon-api ./my-bundle --metadata-path /var/lib/beacon/metadata

Environment Variables:
  BEACON_METADATA_PATH     Default metadata store path
  BEACON_SCHEDULER_TICK_SECONDS   Default tick interval
  BEACON_SCHEDULER_MAX_CONCURRENT_RUNS   Default max concurrent runs
""",
    )
    parser.add_argument(
        "bundle",
        help="Path to bundle directory containing DAGs and plugins",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port to bind (default: 8080)",
    )
    parser.add_argument(
        "--metadata-path",
        default=None,
        help="Metadata store path (default: BEACON_METADATA_PATH or ./metadata.db)",
    )
    parser.add_argument(
        "--instance-id",
        default=None,
        help="Unique instance ID for coordination (auto-generated if not set)",
    )
    parser.add_argument(
        "--tick-seconds",
        type=int,
        default=None,
        help="Scheduler tick interval in seconds",
    )
    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=None,
        help="Maximum concurrent DAG runs",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level (default: INFO)",
    )
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Get defaults from environment
    metadata_path = args.metadata_path or os.environ.get(
        "BEACON_METADATA_PATH", "./metadata.db"
    )
    tick_seconds = args.tick_seconds or int(
        os.environ.get("BEACON_SCHEDULER_TICK_SECONDS", "5")
    )
    max_concurrent = args.max_concurrent or int(
        os.environ.get("BEACON_SCHEDULER_MAX_CONCURRENT_RUNS", "8")
    )

    # Create metadata store
    meta = LocalMetadata(metadata_path)

    # Run the server
    asyncio.run(
        run_server(
            bundle_path=Path(args.bundle),
            meta=meta,
            host=args.host,
            port=args.port,
            tick_seconds=tick_seconds,
            max_concurrent_runs=max_concurrent,
            instance_id=args.instance_id,
        )
    )


if __name__ == "__main__":
    main()
