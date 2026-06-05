"""``beacon plan PATH`` — parse + render every DAG. No execution."""

import sys

import click

from ..loader import load_dags


@click.command()
@click.argument("path", type=click.Path(exists=True, dir_okay=True))
@click.option(
    "--dag-id",
    default=None,
    help="Restrict plan to a single DAG id.",
)
@click.option(
    "--logical-date",
    default=None,
    help="Simulated logical date for template rendering (ISO 8601).",
)
@click.option(
    "--cron",
    default=None,
    help="Cron expression used to compute data_interval_start/end from --logical-date.",
)
def plan(
    path: str,
    dag_id: str | None,
    logical_date: str | None,
    cron: str | None,
) -> None:
    """Validate and render DAG(s) at PATH — shows resolved inputs without executing."""
    dags = load_dags(path)
    if dag_id is not None:
        dags = [d for d in dags if d.id == dag_id]
        if not dags:
            click.echo(f"DAG {dag_id!r} not found at {path}", err=True)
            sys.exit(1)
    if not dags:
        click.echo(f"No DAGs found at {path}", err=True)
        sys.exit(1)

    parsed_date = None
    if logical_date is not None:
        from datetime import datetime

        try:
            parsed_date = datetime.fromisoformat(logical_date)
        except ValueError:
            click.echo(
                f"Invalid --logical-date {logical_date!r}: expected ISO 8601 format",
                err=True,
            )
            sys.exit(2)

    from ...plan import plan as run_plan

    failures = 0
    for dag in dags:
        result = run_plan(dag, logical_date=parsed_date, cron=cron)
        click.echo(str(result))
        if not result.is_valid:
            failures += 1

    if failures:
        click.echo(f"{failures} DAG(s) failed validation", err=True)
        sys.exit(1)
