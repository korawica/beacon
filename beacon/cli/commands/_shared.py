"""Shared helpers for CLI command modules."""

import click


def parse_kv_options(values: tuple[str, ...]) -> dict[str, str]:
    """Parse repeated ``--param k=v`` flags into a dict."""
    out: dict[str, str] = {}
    for raw in values:
        if "=" not in raw:
            raise click.BadParameter(f"Expected 'key=value' but got {raw!r}")
        k, _, v = raw.partition("=")
        out[k.strip()] = v
    return out
