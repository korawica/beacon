"""Backward-compatibility shim — use ``beacon.plan`` instead.

``from beacon.dryrun import dryrun`` continues to work, but the canonical
home is now ``beacon.plan``. This module will be removed in a future
release.
"""

from .plan import (  # noqa: F401
    PlanIssue as DryRunIssue,
    PlanResult as DryRunResult,
    PlannedTask as ResolvedTask,
    plan as dryrun,
)

__all__ = ("dryrun", "DryRunIssue", "DryRunResult", "ResolvedTask")
