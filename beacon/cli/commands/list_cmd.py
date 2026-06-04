"""``beacon list <kind>`` — dags / deployments / runs."""

import asyncio
import sys

import click

from ...metadata import JsonMetadata
from ..loader import load_dags
from ..settings import get


@click.group("list")
def list_cmd() -> None:
    """List dags / deployments / runs."""


@list_cmd.command("dags")
@click.argument("path", type=click.Path(exists=True, dir_okay=True))
def list_dags(path: str) -> None:
    """List every DAG discovered at PATH (file or bundle dir)."""
    dags = load_dags(path)
    if not dags:
        click.echo(f"No DAGs at {path}", err=True)
        sys.exit(1)
    for d in dags:
        n = sum(1 for _ in d.actions)
        click.echo(f"{d.id}  (actions={n}  project={d.project})")


@list_cmd.command("deployments")
@click.option("--metadata-path", default=None)
@click.option(
    "--bundle",
    "bundle_path",
    default=None,
    type=click.Path(exists=True, file_okay=False),
    help=(
        "Bundle directory. When given, deployments whose dag_version "
        "differs from the bundle's current version are marked 'stale'."
    ),
)
def list_deployments(
    metadata_path: str | None, bundle_path: str | None
) -> None:
    """List every Deployment in metadata."""
    meta = JsonMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    deps = asyncio.run(meta.list_deployments())
    if not deps:
        click.echo("(no deployments)")
        return

    bundle_version: str | None = None
    if bundle_path:
        from ...core.bundle import LocalBundle

        bundle_version = LocalBundle(name="bundle", path=bundle_path).version

    for d in deps:
        flags: list[str] = []
        if not d.get("enabled", True):
            flags.append("disabled")
        if d.get("variable_overrides"):
            flags.append("pinned")
        version = d.get("dag_version") or "—"
        if (
            bundle_version is not None
            and d.get("dag_version")
            and d["dag_version"] != bundle_version
        ):
            flags.append(f"stale (bundle: {bundle_version})")
        suffix = ("  [" + ", ".join(flags) + "]") if flags else ""
        click.echo(
            f"{d['id']:<30} dag={d['dag_id']:<20} "
            f"version={version:<13} cron={d.get('cron')!r}{suffix}"
        )


@list_cmd.command("runs")
@click.option("--dag-id", default=None, help="Filter by dag id.")
@click.option("--limit", default=20, show_default=True, type=int)
@click.option("--metadata-path", default=None)
def list_runs(
    dag_id: str | None, limit: int, metadata_path: str | None
) -> None:
    """List recent DAG runs, newest first."""
    meta = JsonMetadata(metadata_path or get("BEACON_METADATA_PATH"))
    runs = asyncio.run(meta.list_dag_runs(dag_id=dag_id, limit=limit))
    if not runs:
        click.echo("(no runs)")
        return
    for r in runs:
        click.echo(
            f"{r['run_id']:<40} dag={r['dag_id']:<20} state={r['state']}"
        )
