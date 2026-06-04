"""``beacon scheduler PATH`` — long-running cron + manual-trigger loop."""

import asyncio
import logging

import click

from ...logging import configure_logging
from ...metadata import LocalMetadata
from ...scheduler import DeploymentScheduler
from ..settings import get


@click.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--metadata-path",
    default=None,
    help="Defaults to $BEACON_METADATA_PATH.",
)
@click.option(
    "--tick-seconds",
    default=None,
    type=int,
    help="Defaults to $BEACON_SCHEDULER_TICK_SECONDS.",
)
@click.option(
    "--max-concurrent",
    default=None,
    type=int,
    help="Defaults to $BEACON_SCHEDULER_MAX_CONCURRENT_RUNS.",
)
def scheduler(
    path: str,
    metadata_path: str | None,
    tick_seconds: int | None,
    max_concurrent: int | None,
) -> None:
    """Run the deployment scheduler against the bundle at PATH.

    Polls the shared metadata store for pending triggers and cron-due
    deployments. SIGTERM / SIGINT stops cleanly; SIGHUP reloads the
    bundle.
    """
    # Wire structured logging early so we get framework + task records.
    configure_logging()
    logging.getLogger("beacon.scheduler").info(
        "starting scheduler with bundle=%s", path
    )

    meta = LocalMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    sched = DeploymentScheduler(
        path,
        meta,
        tick_seconds=tick_seconds or get("BEACON_SCHEDULER_TICK_SECONDS"),
        max_concurrent_runs=(
            max_concurrent or get("BEACON_SCHEDULER_MAX_CONCURRENT_RUNS")
        ),
    )
    asyncio.run(sched.run())
