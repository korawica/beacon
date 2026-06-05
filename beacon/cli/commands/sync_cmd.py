"""``beacon sync PATH`` — re-read a LocalBundle and validate it.

Sync is the boundary between bundle (on disk) and Deployments (in
metadata). It:

  * loads custom plugins from ``{PATH}/plugins/``
  * imports + plans (validates) every DAG in the bundle
  * computes the new ``dag_version`` and rolls **non-pinned** deployments
    forward (pinned deployments — those with stored ``--var`` overrides
    — are left on their old ``dag_version`` and reported as ``stale``)

Anything failing validation exits non-zero so a CI step or systemd
timer can detect broken bundles before they hit the scheduler.
"""

import asyncio
import sys
from pathlib import Path

import click

from ...core.bundle import LocalBundle
from ...plan import plan as run_plan
from ...metadata import LocalMetadata
from ...models.deployment import Deployment
from ..loader import _load_dags_from_file
from ..settings import get


@click.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
@click.option(
    "--metadata-path",
    default=None,
    help=(
        "Metadata store path. Defaults to $BEACON_METADATA_PATH. If unset, "
        "sync only validates the bundle and does not roll any deployments."
    ),
)
def sync(path: str, metadata_path: str | None) -> None:
    """Validate every DAG + load every plugin in the bundle at PATH.

    When a metadata store is reachable, also rolls every non-pinned
    deployment to the new ``dag_version`` and reports pinned/stale ones.
    """
    p = Path(path).resolve()
    bundle = LocalBundle(name=p.name, path=p)
    plugins = bundle.load_plugins()
    click.echo(f"plugins loaded: {plugins or '(none)'}")

    dag_files = bundle.discover_dags()
    if not dag_files:
        click.echo("No DAG files found.", err=True)
        sys.exit(1)

    failures: list[str] = []
    valid_dag_ids: set[str] = set()
    total = 0
    # Track folders that contain multiple DAG files for a soft warning.
    files_per_folder: dict[Path, set[Path]] = {}
    for f in dag_files:
        produced = 0
        for dag in _load_dags_from_file(f):
            produced += 1
            total += 1
            result = run_plan(dag)
            mark = "✓" if result.is_valid else "✗"
            click.echo(f"  {mark} {dag.id}  ({f.name})")
            if result.is_valid:
                valid_dag_ids.add(dag.id)
            else:
                failures.append(dag.id)
        if produced:
            files_per_folder.setdefault(f.parent, set()).add(f)

    # Policy: one DAG per folder (docs/core/deploy.md §7). Warn — don't fail.
    multi = {d: fs for d, fs in files_per_folder.items() if len(fs) > 1}
    if multi:
        click.echo("WARNING: folders with multiple DAG files (policy: one):")
        for folder, fs in sorted(multi.items()):
            click.echo(
                f"  - {folder.relative_to(p)}: "
                + ", ".join(sorted(f.name for f in fs))
            )

    click.echo(
        f"bundle {bundle.name!r} version={bundle.version} "
        f"dags={total} failed={len(failures)}"
    )

    # Roll non-pinned deployments forward in metadata.
    meta_path = metadata_path or get("BEACON_METADATA_PATH")
    if meta_path and not failures:
        rolled, pinned_stale, unknown = asyncio.run(
            _roll_deployments(meta_path, bundle.version, valid_dag_ids)
        )
        if rolled:
            click.echo(
                f"deployments rolled to {bundle.version}: "
                + ", ".join(sorted(rolled))
            )
        if pinned_stale:
            click.echo(
                "deployments pinned (stale): "
                + ", ".join(sorted(pinned_stale))
                + "  — run `beacon deployment sync` to accept the new version"
            )
        if unknown:
            click.echo(
                "WARNING: deployments reference unknown dag_id(s): "
                + ", ".join(sorted(unknown))
            )

    if failures:
        sys.exit(1)


async def _roll_deployments(
    metadata_path: str,
    new_version: str,
    valid_dag_ids: set[str],
) -> tuple[list[str], list[str], list[str]]:
    """Return ``(rolled, pinned_stale, unknown_dag)`` deployment-id lists."""
    meta = LocalMetadata(metadata_path)
    deployments = await meta.list_deployments()
    rolled: list[str] = []
    pinned_stale: list[str] = []
    unknown: list[str] = []
    for raw in deployments:
        # Strip scheduler bookkeeping before model_validate; upsert
        # preserves it on the write back.
        dep = Deployment.model_validate(
            {k: v for k, v in raw.items() if k != "_scheduler"}
        )
        if dep.dag_id not in valid_dag_ids:
            unknown.append(dep.id)
            continue
        # Pinned deployments are exempt from *subsequent* auto-rolls only:
        # a first-time deployment (no dag_version yet) still gets stamped
        # by this sync so it has a known starting point.
        if dep.is_pinned and dep.dag_version is not None:
            if dep.dag_version != new_version:
                pinned_stale.append(dep.id)
            continue
        if dep.dag_version == new_version:
            continue
        dep.dag_version = new_version
        await meta.upsert_deployment(dep.model_dump())
        rolled.append(dep.id)
    return rolled, pinned_stale, unknown
