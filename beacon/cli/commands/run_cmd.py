"""``beacon run PATH`` — run a DAG against persistent metadata."""

import sys

import click

from ..loader import load_one_dag
from ..settings import get
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
@click.option(
    "--metadata-path",
    default=None,
    help=(
        "Where to persist metadata. "
        "Defaults to $BEACON_METADATA_PATH (./metadata.db)."
    ),
)
def run(
    path: str,
    dag_id: str | None,
    params: tuple[str, ...],
    variables: tuple[str, ...],
    metadata_path: str | None,
) -> None:
    """Run a DAG locally against persistent metadata."""
    dag = load_one_dag(path, dag_id=dag_id)
    meta_path = metadata_path or get("BEACON_METADATA_PATH")
    result = dag.run(
        params=parse_kv_options(params),
        variables=parse_kv_options(variables),
        metadata_path=meta_path,
    )
    click.echo(f"run_id : {result['run_id']}")
    click.echo(f"state  : {result['state']}")
    click.echo(f"meta   : {meta_path}")
    sys.exit(0 if result["state"] == "success" else 1)
