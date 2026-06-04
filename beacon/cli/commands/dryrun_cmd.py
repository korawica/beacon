"""``beacon dryrun PATH`` — parse + render every DAG. No execution."""

import sys

import click

from ..loader import load_dags


@click.command()
@click.argument("path", type=click.Path(exists=True, dir_okay=True))
@click.option(
    "--dag-id",
    default=None,
    help="Restrict dryrun to a single DAG id.",
)
def dryrun(path: str, dag_id: str | None) -> None:
    """Validate and render DAG(s) at PATH without executing any task."""
    dags = load_dags(path)
    if dag_id is not None:
        dags = [d for d in dags if d.id == dag_id]
        if not dags:
            click.echo(f"DAG {dag_id!r} not found at {path}", err=True)
            sys.exit(1)
    if not dags:
        click.echo(f"No DAGs found at {path}", err=True)
        sys.exit(1)

    from ...dryrun import dryrun as run_dryrun

    failures = 0
    for dag in dags:
        click.echo(f"=== {dag.id} ===")
        result = run_dryrun(dag)
        click.echo(result.print())
        if not result.is_valid:
            failures += 1

    if failures:
        click.echo(f"{failures} DAG(s) failed validation", err=True)
        sys.exit(1)
