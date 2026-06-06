"""``beacon trigger DEPLOYMENT_ID`` — enqueue a manual run request."""

import asyncio
import sys

import click

from ...metadata import LocalMetadata
from ..settings import get
from ._shared import parse_kv_options


@click.command()
@click.argument("deployment_id")
@click.option(
    "--var",
    "variables",
    multiple=True,
    metavar="KEY=VALUE",
    help="Override variable for this run (repeatable).",
)
@click.option(
    "--metadata-path",
    default=None,
    help="Defaults to $BEACON_METADATA_PATH.",
)
def trigger(
    deployment_id: str,
    variables: tuple[str, ...],
    metadata_path: str | None,
) -> None:
    """Enqueue a manual trigger request for DEPLOYMENT_ID.

    Writes a pending-trigger file to the shared metadata store. The
    ``beacon scheduler`` process picks it up on the next tick and spawns
    a DagRun. No API server / socket required.
    """
    meta = LocalMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    dep = asyncio.run(meta.get_deployment(deployment_id))
    if dep is None:
        click.echo(f"Unknown deployment: {deployment_id!r}", err=True)
        sys.exit(1)

    parsed_vars = parse_kv_options(variables)

    # Validate variables against deployment requirements
    errors = _validate_trigger_variables(dep, parsed_vars)
    if errors:
        for e in errors:
            click.echo(f"Error: {e}", err=True)
        click.echo(
            "Hint: Required variables can be set with --var KEY=VALUE",
            err=True,
        )
        sys.exit(1)

    tid = asyncio.run(meta.enqueue_trigger(deployment_id, parsed_vars))
    click.echo(f"trigger enqueued: {tid}  (deployment={deployment_id})")


def _validate_trigger_variables(
    dep: dict, trigger_vars: dict[str, str]
) -> list[str]:
    """Validate trigger variables against deployment requirements.

    Returns a list of error messages. Empty list means valid.
    """
    errors: list[str] = []
    requirements = dep.get("variable_requirements", {})
    if not requirements:
        # No requirements tracked - allow any variables
        return errors

    deployment_overrides = dep.get("variable_overrides", {})

    for key, spec in requirements.items():
        has_default = spec.get("has_default", False)
        # Check if provided via trigger or deployment overrides
        provided = key in trigger_vars or key in deployment_overrides

        if not has_default and not provided:
            errors.append(
                f"Required variable {key!r} not provided. Use --var {key}=VALUE"
            )

    return errors
