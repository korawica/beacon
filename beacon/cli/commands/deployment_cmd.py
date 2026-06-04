"""``beacon deployment <subcommand>`` — operate on individual Deployments.

Today this covers:

* ``beacon deployment sync <id>`` / ``--all`` — accept a new bundle
  ``dag_version`` for a pinned deployment (deployments with ``--var``
  overrides). Non-pinned deployments auto-roll on ``beacon sync`` and
  do not need this command.
* ``beacon deployment diff <id>`` — preview the variable-resolution
  diff a pinned deployment would see between its current bundle
  version and the bundle on disk.
"""

import asyncio
import sys
from pathlib import Path
from typing import Any

import click

from ...core.bundle import LocalBundle
from ...core.variables import merge_with_overrides
from ...metadata import JsonMetadata
from ...models.deployment import Deployment
from ..loader import _load_dags_from_file
from ..settings import get


@click.group("deployment")
def deployment_cmd() -> None:
    """Operate on individual Deployments stored in metadata."""


# --- shared helpers -------------------------------------------------------


def _load_meta(metadata_path: str | None) -> JsonMetadata:
    return JsonMetadata(metadata_path or get("BEACON_METADATA_PATH"))


def _load_bundle(bundle_path: str) -> LocalBundle:
    p = Path(bundle_path).resolve()
    return LocalBundle(name=p.name, path=p)


def _index_dag_source_files(bundle: LocalBundle) -> dict[str, Path]:
    """Map ``dag_id`` → file it was loaded from."""
    out: dict[str, Path] = {}
    for f in bundle.discover_dags():
        for d in _load_dags_from_file(f):
            out[d.id] = f
    return out


def _resolved_variables_for(
    dep: Deployment, bundle: LocalBundle
) -> dict[str, Any]:
    """Compute the full variable dict a deployment would see right now."""
    source_files = _index_dag_source_files(bundle)
    src = source_files.get(dep.dag_id)
    if src is None:
        return dict(dep.variable_overrides)
    scoped = bundle.variable_scope.resolve_for(src)
    return merge_with_overrides(scoped, dep.variable_overrides)


# --- subcommands ----------------------------------------------------------


@deployment_cmd.command("sync")
@click.argument("deployment_id", required=False, default=None)
@click.option(
    "--all",
    "sync_all",
    is_flag=True,
    default=False,
    help="Sync every pinned + stale deployment.",
)
@click.option(
    "--bundle",
    "bundle_path",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Bundle directory whose dag_version should be accepted.",
)
@click.option("--metadata-path", default=None)
def deployment_sync(
    deployment_id: str | None,
    sync_all: bool,
    bundle_path: str,
    metadata_path: str | None,
) -> None:
    """Accept a new ``dag_version`` for one (or all) pinned deployments."""
    if not deployment_id and not sync_all:
        click.echo("Provide a DEPLOYMENT_ID or --all.", err=True)
        sys.exit(2)
    if deployment_id and sync_all:
        click.echo("Pass either DEPLOYMENT_ID or --all, not both.", err=True)
        sys.exit(2)

    bundle = _load_bundle(bundle_path)
    new_version = bundle.version
    meta = _load_meta(metadata_path)

    if deployment_id:
        targets = [deployment_id]
    else:
        deps = asyncio.run(meta.list_deployments())
        targets = [
            d["id"]
            for d in deps
            if d.get("variable_overrides")
            and d.get("dag_version") != new_version
        ]

    if not targets:
        click.echo("(nothing to sync)")
        return

    asyncio.run(_apply_sync(meta, targets, new_version))


async def _apply_sync(
    meta: JsonMetadata, deployment_ids: list[str], new_version: str
) -> None:
    for did in deployment_ids:
        raw = await meta.get_deployment(did)
        if raw is None:
            click.echo(f"  ! {did}: not found", err=True)
            continue
        dep = Deployment.model_validate(
            {k: v for k, v in raw.items() if k != "_scheduler"}
        )
        previous = dep.dag_version
        dep.dag_version = new_version
        await meta.upsert_deployment(dep.model_dump())
        click.echo(f"  ✓ {did}: {previous or '—'} → {new_version}")


@deployment_cmd.command("diff")
@click.argument("deployment_id")
@click.option(
    "--bundle",
    "bundle_path",
    required=True,
    type=click.Path(exists=True, file_okay=False),
    help="Bundle directory to diff against.",
)
@click.option("--metadata-path", default=None)
def deployment_diff(
    deployment_id: str, bundle_path: str, metadata_path: str | None
) -> None:
    """Preview what would change if ``deployment_id`` were synced now."""
    bundle = _load_bundle(bundle_path)
    meta = _load_meta(metadata_path)
    raw = asyncio.run(meta.get_deployment(deployment_id))
    if raw is None:
        click.echo(f"Unknown deployment: {deployment_id!r}", err=True)
        sys.exit(1)
    dep = Deployment.model_validate(
        {k: v for k, v in raw.items() if k != "_scheduler"}
    )

    click.echo(f"deployment: {dep.id}")
    click.echo(f"  dag_id:           {dep.dag_id}")
    click.echo(f"  pinned:           {dep.is_pinned}")
    click.echo(f"  current version:  {dep.dag_version or '(unset)'}")
    click.echo(f"  bundle version:   {bundle.version}")

    if dep.dag_version == bundle.version:
        click.echo("  status:           up-to-date")
        return
    click.echo("  status:           stale")

    resolved_now = _resolved_variables_for(dep, bundle)
    click.echo("  resolved vars after sync (highest layer = --var overrides):")
    if not resolved_now:
        click.echo("    (none)")
        return
    for k in sorted(resolved_now):
        marker = "*" if k in dep.variable_overrides else " "
        click.echo(f"    {marker} {k} = {resolved_now[k]!r}")
    click.echo("  (* = comes from this deployment's --var overrides)")
