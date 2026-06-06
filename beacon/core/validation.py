"""Validation utilities for DAG planning.

This module provides detection and validation functions used during DAG
planning to identify required variables, secrets, and other dependencies.

These utilities are used by `beacon.plan.plan()` and can be reused by
other validation/analysis tools.
"""

import ast
import os
import re
from dataclasses import dataclass
from typing import Any

__all__ = (
    "RequiredVariable",
    "RequiredSecret",
    "detect_required_variables",
    "detect_required_secrets",
)


@dataclass
class RequiredVariable:
    """A variable required by the DAG.

    Attributes:
        key: Variable name (supports dot notation for nested keys).
        has_default: Whether a default value was provided in the template.
        found_in: Where the variable was found (e.g., "variables" if provided).
        default_value: The default value if has_default is True.
    """

    key: str
    has_default: bool
    found_in: str | None
    default_value: Any = None


@dataclass
class RequiredSecret:
    """A secret (environment variable) required by the DAG.

    Attributes:
        key: Environment variable name.
        found_in: Where the secret was found (e.g., "environment" if set).
    """

    key: str
    found_in: str | None


def detect_required_variables(
    all_inputs: list[tuple[str, Any]],
    variables: dict[str, Any],
) -> list[RequiredVariable]:
    """Extract required variables from Jinja templates.

    Parses ``{{ vars("key") }}`` and ``{{ vars("key", "default") }}`` patterns
    to determine what variables the DAG needs.

    Args:
        all_inputs: List of (task_id, value) pairs for all input values.
        variables: The variables dict to check against.

    Returns:
        Sorted list of RequiredVariable instances.
    """
    # Pattern matches: vars("key") or vars("key", "default")
    # Also matches: vars("nested.key")
    vars_pattern = re.compile(
        r'vars\s*\(\s*["\']([^"\']+)["\'](?:\s*,\s*([^)]+))?\s*\)'
    )

    required: dict[str, RequiredVariable] = {}

    for task_id, value in all_inputs:
        if not isinstance(value, str):
            continue

        for match in vars_pattern.finditer(value):
            key = match.group(1)
            default_arg = match.group(2)

            # Check if nested key exists in variables
            found_in = None
            if "." in key:
                parts = key.split(".")
                v = variables
                found = True
                for part in parts:
                    if isinstance(v, dict) and part in v:
                        v = v[part]
                    else:
                        found = False
                        break
                if found:
                    found_in = "variables"
            elif key in variables:
                found_in = "variables"

            if key not in required:
                required[key] = RequiredVariable(
                    key=key,
                    has_default=default_arg is not None,
                    found_in=found_in,
                    default_value=ast.literal_eval(default_arg.strip())
                    if default_arg and default_arg.strip()
                    else None,
                )
            else:
                # If any usage has no default, the variable is required
                if default_arg is None:
                    required[key].has_default = False
                if found_in and required[key].found_in is None:
                    required[key].found_in = found_in

    return sorted(required.values(), key=lambda v: v.key)


def detect_required_secrets(
    all_inputs: list[tuple[str, Any]],
) -> list[RequiredSecret]:
    """Extract required secrets from Jinja templates.

    Parses ``{{ secrets("KEY") }}`` patterns to determine what
    environment variables the DAG needs.

    Args:
        all_inputs: List of (task_id, value) pairs for all input values.

    Returns:
        Sorted list of RequiredSecret instances.
    """
    # Pattern matches: secrets("KEY")
    secrets_pattern = re.compile(r'secrets\s*\(\s*["\']([^"\']+)["\']\s*\)')

    required: dict[str, RequiredSecret] = {}

    for task_id, value in all_inputs:
        if not isinstance(value, str):
            continue

        for match in secrets_pattern.finditer(value):
            key = match.group(1)
            found_in = "environment" if key in os.environ else None

            if key not in required:
                required[key] = RequiredSecret(key=key, found_in=found_in)

    return sorted(required.values(), key=lambda s: s.key)
