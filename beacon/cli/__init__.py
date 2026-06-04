"""Beacon CLI entry point.

Exposes the root ``cli`` Click group. Wired in ``pyproject.toml`` as
``beacon = "beacon.cli:cli"``.
"""

from .main import cli

__all__ = ("cli",)
