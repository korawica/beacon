"""``beacon trigger DEPLOYMENT_ID`` — enqueue a manual run request."""

import asyncio
import sys

import click

from ...metadata import JsonMetadata
from ..settings import get
from ._shared import parse_kv_options


@click.command()
@click.argument("deployment_id")
@click.option(
    "--param",
    "params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override param for this run (repeatable).",
)
@click.option(
    "--metadata-path",
    default=None,
    help="Defaults to $BEACON_METADATA_PATH.",
)
def trigger(
    deployment_id: str,
    params: tuple[str, ...],
    metadata_path: str | None,
) -> None:
    """Enqueue a manual trigger request for DEPLOYMENT_ID.

    Writes a pending-trigger file to the shared metadata store. The
    ``beacon scheduler`` process picks it up on the next tick and spawns
    a DagRun. No API server / socket required.
    """
    meta = JsonMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    dep = asyncio.run(meta.get_deployment(deployment_id))
    if dep is None:
        click.echo(f"Unknown deployment: {deployment_id!r}", err=True)
        sys.exit(1)
    tid = asyncio.run(
        meta.enqueue_trigger(deployment_id, parse_kv_options(params))
    )
    click.echo(f"trigger enqueued: {tid}  (deployment={deployment_id})")
