"""``beacon sync PATH`` — re-read a LocalBundle and validate it.

Without persistent DAG storage (that's Phase 2.1 / SqliteMetadata), sync
is currently a *validation* + *plugin-load* pass. It:

  * loads custom plugins from ``{PATH}/plugins/``
  * imports every DAG file
  * dry-runs every DAG to confirm it parses + renders cleanly

Anything failing exits non-zero so a CI step or systemd timer can detect
broken bundles before they hit the scheduler.
"""

import sys
from pathlib import Path

import click

from ...core.bundle import LocalBundle
from ...dryrun import dryrun as run_dryrun
from ..loader import _load_dags_from_file


@click.command()
@click.argument("path", type=click.Path(exists=True, file_okay=False))
def sync(path: str) -> None:
    """Validate every DAG + load every plugin in the bundle at PATH."""
    p = Path(path).resolve()
    bundle = LocalBundle(name=p.name, path=p)
    plugins = bundle.load_plugins()
    click.echo(f"plugins loaded: {plugins or '(none)'}")

    dag_files = bundle.discover_dags()
    if not dag_files:
        click.echo("No DAG files found.", err=True)
        sys.exit(1)

    failures: list[str] = []
    total = 0
    for f in dag_files:
        for dag in _load_dags_from_file(f):
            total += 1
            result = run_dryrun(dag)
            mark = "✓" if result.is_valid else "✗"
            click.echo(f"  {mark} {dag.id}  ({f.name})")
            if not result.is_valid:
                failures.append(dag.id)

    click.echo(
        f"bundle {bundle.name!r} version={bundle.version} "
        f"dags={total} failed={len(failures)}"
    )
    if failures:
        sys.exit(1)
