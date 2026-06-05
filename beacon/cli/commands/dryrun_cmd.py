"""``beacon dryrun`` — deprecated alias for ``beacon plan``."""

import click

from .plan_cmd import plan


@click.command(
    name="dryrun",
    hidden=True,
    deprecated=True,
    epilog="Use ``beacon plan`` instead.",
)
@click.argument("path", type=click.Path(exists=True, dir_okay=True))
@click.option("--dag-id", default=None)
@click.option("--logical-date", default=None)
@click.option("--cron", default=None)
@click.pass_context
def dryrun(
    ctx: click.Context,
    path: str,
    dag_id: str | None,
    logical_date: str | None,
    cron: str | None,
) -> None:
    """Deprecated — use ``beacon plan`` instead."""
    ctx.invoke(
        plan, path=path, dag_id=dag_id, logical_date=logical_date, cron=cron
    )
