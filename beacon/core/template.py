"""Template file rendering with Jinja FileSystemLoader.

This module provides template rendering that supports Jinja's full feature set
including `{% extends %}`, `{% include %}`, and `{% import %}`.

Template search path (in order):
    1. <dag_folder>/assets/    (DAG-local assets)
    2. <bundle_root>/assets/   (bundle-global assets)

Usage:
    rendered = render_template_file(
        "queries/transform.sql",
        context={"params": {...}, "vars": {...}},
    )
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from jinja2 import FileSystemLoader, StrictUndefined, Undefined
from jinja2.nativetypes import NativeEnvironment
from jinja2.sandbox import SandboxedEnvironment

from .assets import get_bundle_context, BundleContext

if TYPE_CHECKING:
    pass

logger = logging.getLogger("beacon.core.template")


class _SandboxedNativeEnvironment(NativeEnvironment, SandboxedEnvironment):
    """Native-typed return values + sandbox restrictions.

    Used for inline template strings where we want native types.
    MRO matters: ``NativeEnvironment`` first so ``template_class`` resolves
    to ``NativeTemplate``. ``SandboxedEnvironment`` second contributes the
    ``is_safe_attribute`` / ``is_safe_callable`` guards.
    """

    pass


class _SandboxedEnvironment(SandboxedEnvironment):
    """Standard sandboxed environment for file templates.

    Used for file templates with complex Jinja features like for loops,
    extends, includes. Returns string output (not native types).
    """

    pass


def _get_template_search_paths() -> list[Path]:
    """Get template search paths from current bundle context.

    Returns paths in priority order:
        1. DAG-local assets directory
        2. Bundle-global assets directory

    Returns empty list if no bundle context is set.
    """
    ctx = get_bundle_context()
    if ctx is None:
        logger.debug("No bundle context set, no template search paths")
        return []

    paths: list[Path] = []

    # 1. DAG-local assets (highest priority)
    if ctx.dag_source_file is not None:
        dag_assets = ctx.dag_source_file.parent / "assets"
        if dag_assets.exists() and dag_assets.is_dir():
            paths.append(dag_assets)

    # 2. Bundle-global assets
    if ctx.bundle_root is not None:
        bundle_assets = ctx.bundle_root / "assets"
        if bundle_assets.exists() and bundle_assets.is_dir():
            # Avoid duplicates if DAG-local == bundle-global
            if bundle_assets not in paths:
                paths.append(bundle_assets)

    logger.debug("Template search paths: %s", paths)
    return paths


def render_template_file(
    template_name: str,
    context: dict,
    *,
    bundle_ctx: BundleContext | None = None,
) -> str:
    """Render a template file using Jinja's FileSystemLoader.

    This supports full Jinja features including:
        - {% extends "base.sql" %}  -- template inheritance
        - {% include "partials/header.sql" %}  -- includes
        - {% import "macros.sql" as m %}  -- macros

    Args:
        template_name: Template file name relative to assets/ directory
            (e.g., "queries/transform.sql", not "./queries/transform.sql")
        context: Dictionary with template variables (params, vars, runtime, outputs)
        bundle_ctx: Optional explicit bundle context. Falls back to current context.

    Returns:
        Rendered template string.

    Raises:
        TemplateNotFound: If template doesn't exist in any search path.
        UndefinedError: If a template variable is undefined.
        SecurityError: If sandbox restrictions are violated.
    """
    # Set explicit bundle context if provided
    if bundle_ctx is not None:
        from .assets import set_bundle_context, reset_bundle_context

        token = set_bundle_context(bundle_ctx)
        try:
            return _render_template_file_impl(template_name, context)
        finally:
            reset_bundle_context(token)
    else:
        return _render_template_file_impl(template_name, context)


def _render_template_file_impl(template_name: str, context: dict) -> str:
    """Internal implementation of template rendering."""
    from jinja2 import TemplateNotFound

    search_paths = _get_template_search_paths()

    if not search_paths:
        raise TemplateNotFound(
            template_name,
            message="No template search paths available. "
            "Ensure bundle context is set with assets/ directories.",
        )

    # Use standard SandboxedEnvironment for file templates
    # (not NativeEnvironment - it breaks complex templates with for loops)
    env = _SandboxedEnvironment(
        loader=FileSystemLoader([str(p) for p in search_paths]),
        undefined=StrictUndefined,
        extensions=("jinja2.ext.do",),
        autoescape=False,
    )

    # Load and render template
    template = env.get_template(template_name)
    result = template.render(**context)

    # Handle Undefined result
    if isinstance(result, Undefined):
        str(result)  # Raises UndefinedError for StrictUndefined

    return result


def render_template_string(template_string: str, context: dict) -> str:
    """Render a template string with Jinja.

    This is for inline code/templates that don't come from files.
    Uses the same sandbox and native type support as file templates.

    Args:
        template_string: Template content as a string
        context: Dictionary with template variables

    Returns:
        Rendered string.
    """
    env = _SandboxedNativeEnvironment(
        undefined=StrictUndefined,
        extensions=("jinja2.ext.do",),
        autoescape=False,
    )

    template = env.from_string(template_string)
    result = template.render(**context)

    if isinstance(result, Undefined):
        str(result)

    return result
