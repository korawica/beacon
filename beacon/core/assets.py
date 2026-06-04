"""Asset path resolution + execution-time bundle context.

Policy (see ``docs/core/deploy.md``):

    A ``py_statement`` (or other asset path) is looked up in this order:

      1. ``<dag_folder>/assets/<py_statement>``     (DAG-local assets)
      2. ``<bundle_root>/assets/<py_statement>``    (bundle-global assets)
      3. raise ``FileNotFoundError``

The bundle context (bundle_root + DAG source file) is pushed onto a
:class:`contextvars.ContextVar` by :class:`beacon.runner.DagRunner`
before triggering tasks, so plugins (notably ``PythonPlugin``) can
resolve relative asset paths without explicit plumbing through the
``Context`` typed-dict.

For backwards compatibility, if no bundle context is set (e.g. running
a DAG from a single ``.py`` file with no surrounding bundle) the
caller-provided path is returned as-is (resolved against CWD).
"""

from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

ASSETS_DIR = "assets"


@dataclass(frozen=True)
class BundleContext:
    """Bundle layout hints needed to resolve assets at run time."""

    bundle_root: Path | None
    dag_source_file: Path | None


_current_bundle: ContextVar[BundleContext | None] = ContextVar(
    "beacon_bundle_context", default=None
)


def set_bundle_context(ctx: BundleContext | None) -> object:
    """Push a bundle context. Returns a token usable for :func:`reset`."""
    return _current_bundle.set(ctx)


def reset_bundle_context(token: object) -> None:
    """Pop a previously set bundle context."""
    _current_bundle.reset(token)  # type: ignore[arg-type]


def get_bundle_context() -> BundleContext | None:
    """Return the current bundle context, or ``None`` if unset."""
    return _current_bundle.get()


def resolve_asset(
    path: str | Path,
    *,
    bundle_ctx: BundleContext | None = None,
) -> Path:
    """Resolve an asset path using the documented lookup policy.

    The lookup is **only** triggered for relative paths. Absolute paths
    are returned as-is so user code can still point at a specific file
    on disk when needed.

    Raises:
        FileNotFoundError: when both lookup locations are missing.
    """
    raw = Path(path)
    if raw.is_absolute():
        if not raw.exists():
            raise FileNotFoundError(f"Asset not found: {raw}")
        return raw

    ctx = bundle_ctx if bundle_ctx is not None else get_bundle_context()

    # If we don't have any bundle context (e.g. ad-hoc single-file run),
    # fall back to a CWD-relative resolve. Preserves prior behaviour.
    if ctx is None or (ctx.bundle_root is None and ctx.dag_source_file is None):
        resolved = raw.resolve()
        if not resolved.exists():
            raise FileNotFoundError(f"Asset not found: {resolved}")
        return resolved

    tried: list[Path] = []

    # 1. DAG-local assets.
    if ctx.dag_source_file is not None:
        local = (ctx.dag_source_file.parent / ASSETS_DIR / raw).resolve()
        if local.exists():
            return local
        tried.append(local)

    # 2. Bundle-global assets.
    if ctx.bundle_root is not None:
        bundle = (ctx.bundle_root / ASSETS_DIR / raw).resolve()
        if bundle.exists():
            return bundle
        tried.append(bundle)

    raise FileNotFoundError(
        "Asset not found in any known location. Tried:\n  - "
        + "\n  - ".join(str(p) for p in tried)
    )
