"""``beacon test PATH`` — run a DAG in a fresh temp metadata dir."""

import sys

import click

from ..loader import load_one_dag
from ._shared import parse_kv_options


@click.command()
@click.argument("path", type=click.Path(exists=True, dir_okay=True))
@click.option("--dag-id", default=None, help="Pick a DAG when PATH has many.")
@click.option(
    "--param",
    "params",
    multiple=True,
    metavar="KEY=VALUE",
    help="Run param (repeatable).",
)
@click.option(
    "--var",
    "variables",
    multiple=True,
    metavar="KEY=VALUE",
    help="Variable for vars() templating (repeatable).",
)
def test(
    path: str,
    dag_id: str | None,
    params: tuple[str, ...],
    variables: tuple[str, ...],
) -> None:
    """Execute a DAG against a throwaway temp metadata dir."""
    dag = load_one_dag(path, dag_id=dag_id)
    result = dag.run(
        params=parse_kv_options(params),
        variables=parse_kv_options(variables),
    )
    click.echo(f"run_id : {result['run_id']}")
    click.echo(f"state  : {result['state']}")
    for tid, state in result["states"].items():
        click.echo(f"  {tid}: {state}")
    sys.exit(0 if result["state"] == "success" else 1)
