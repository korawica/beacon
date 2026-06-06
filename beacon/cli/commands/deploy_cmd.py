"""``beacon deploy`` — create/update a Deployment record in metadata."""

import asyncio
import sys
from typing import TYPE_CHECKING

import click
from croniter import croniter

from ...metadata import LocalMetadata
from ...models.deployment import Deployment
from ...plan import plan
from ..settings import get
from ._shared import parse_kv_options

if TYPE_CHECKING:
    from ...models.dag import Dag


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
    "--var",
    "variable_overrides",
    multiple=True,
    metavar="KEY=VALUE",
    help=(
        "Per-deployment variable override (repeatable). Highest precedence "
        "in the scope chain. Storing any override marks the deployment as "
        "'pinned': it is not auto-rolled when ``beacon sync`` ships a new "
        "bundle version — use ``beacon deployment sync`` to accept."
    ),
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
@click.option(
    "--bundle",
    "bundle_path",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help=(
        "Bundle directory to analyze DAG variable requirements. "
        "When provided, extracts required variables from the DAG and stores "
        "them on the deployment for trigger-time validation."
    ),
)
def deploy(
    deployment_id: str,
    dag_id: str,
    cron: str | None,
    timezone: str,
    desc: str | None,
    variable_overrides: tuple[str, ...],
    owners: tuple[str, ...],
    disabled: bool,
    metadata_path: str | None,
    bundle_path: str | None,
) -> None:
    """Register (or update) a Deployment in metadata.

    DAGs live in the bundle; Deployments live in metadata only. Re-running
    ``beacon deploy --id ...`` with the same id replaces the record but
    preserves scheduler bookkeeping (``last_scheduled_at``).
    """
    if cron is not None and not croniter.is_valid(cron):
        click.echo(f"Invalid cron expression: {cron!r}", err=True)
        sys.exit(2)

    # Analyze DAG for variable requirements if bundle is provided
    variable_requirements: dict[str, dict] = {}
    if bundle_path:
        dag = _load_dag_for_deployment(dag_id, bundle_path)
        if dag:
            plan_result = plan(dag, variables={})
            variable_requirements = {
                v.key: {
                    "has_default": v.has_default,
                    **(
                        {"default_value": v.default_value}
                        if v.has_default
                        else {}
                    ),
                }
                for v in plan_result.required_variables
            }
            if plan_result.required_variables:
                required_str = ", ".join(
                    f"{v.key}{'?' if v.has_default else ''}"
                    for v in plan_result.required_variables
                )
                click.echo(f"Variable requirements: {required_str}")
        else:
            click.echo(
                f"Warning: DAG {dag_id!r} not found in bundle, "
                f"skipping variable analysis",
                err=True,
            )

    dep = Deployment(
        id=deployment_id,
        dag_id=dag_id,
        cron=cron,
        timezone=timezone,
        desc=desc,
        enabled=not disabled,
        variable_overrides=parse_kv_options(variable_overrides),
        variable_requirements=variable_requirements,
        owners=list(owners),
    )

    meta = LocalMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    asyncio.run(meta.upsert_deployment(dep.model_dump()))
    pinned = " [pinned]" if dep.is_pinned else ""
    click.echo(
        f"Deployment {dep.id!r} → dag={dep.dag_id} cron={dep.cron!r}{pinned}"
    )
    click.echo(f"enabled={dep.enabled}  metadata={meta.base_path}")


def _load_dag_for_deployment(dag_id: str, bundle_path: str) -> Dag | None:
    """Load a specific DAG from bundle for variable analysis."""
    from ..loader import load_dags

    dags = load_dags(bundle_path)
    for d in dags:
        if d.id == dag_id:
            return d
    return None
