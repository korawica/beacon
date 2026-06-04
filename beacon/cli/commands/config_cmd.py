"""``beacon config show`` — dump effective BEACON_* settings."""

import click

from ..settings import load_settings


@click.group()
def config() -> None:
    """Inspect beacon configuration."""


@config.command("show")
def show() -> None:
    """Print every BEACON_* env var with its effective value and source."""
    settings = load_settings()
    name_w = max(len(n) for n in settings)
    val_w = max(len(str(s.value)) for s in settings.values())
    for s in settings.values():
        click.echo(f"{s.name:<{name_w}}  {str(s.value):<{val_w}}  ({s.source})")
