"""``beacon deploy`` — create/update a Deployment record in metadata."""

import asyncio
import sys

import click
from croniter import croniter

from ...metadata import JsonMetadata
from ...models.deployment import Deployment
from ..settings import get
from ._shared import parse_kv_options


@click.command()
@click.option("--id", "deployment_id", required=True, help="Deployment id.")
@click.option("--dag-id", required=True, help="Dag id this deployment uses.")
@click.option(
    "--cron",
    default=None,
    help="Cron expression. Omit for manual-trigger-only deployments.",
)
@click.option("--timezone", default="UTC", show_default=True)
@click.option("--desc", default=None, help="Human-readable description.")
@click.option(
    "--variables-ref",
    default=None,
    help="Stage name in variables.yml used by vars() templating.",
)
@click.option(
    "--param",
    "params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Default param value (repeatable).",
)
@click.option(
    "--owner",
    "owners",
    multiple=True,
    metavar="NAME",
    help="Owner (repeatable).",
)
@click.option(
    "--disabled",
    is_flag=True,
    default=False,
    help="Create the deployment but mark it disabled (not scheduled).",
)
@click.option(
    "--metadata-path",
    default=None,
    help="Defaults to $BEACON_METADATA_PATH.",
)
def deploy(
    deployment_id: str,
    dag_id: str,
    cron: str | None,
    timezone: str,
    desc: str | None,
    variables_ref: str | None,
    params: tuple[str, ...],
    owners: tuple[str, ...],
    disabled: bool,
    metadata_path: str | None,
) -> None:
    """Register (or update) a Deployment in metadata.

    DAGs live in the bundle; Deployments live in metadata only. Re-running
    ``beacon deploy --id ...`` with the same id replaces the record but
    preserves scheduler bookkeeping (``last_scheduled_at``).
    """
    if cron is not None and not croniter.is_valid(cron):
        click.echo(f"Invalid cron expression: {cron!r}", err=True)
        sys.exit(2)

    dep = Deployment(
        id=deployment_id,
        dag_id=dag_id,
        cron=cron,
        timezone=timezone,
        desc=desc,
        enabled=not disabled,
        variables_ref=variables_ref,
        params=parse_kv_options(params),
        owners=list(owners),
    )

    meta = JsonMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    asyncio.run(meta.upsert_deployment(dep.model_dump()))
    click.echo(f"Deployment {dep.id!r} → dag={dep.dag_id} cron={dep.cron!r}")
    click.echo(f"enabled={dep.enabled}  metadata={meta.base_path}")
