"""Remote plugin resolution.

Supports installing plugins directly from GitHub or PyPI via uv:

    uses: "org/repo@version"     → git+https://github.com/org/repo@version
    uses: "package-name@version" → package-name==version  (PyPI)

The installed package must declare its plugins via the ``beacon.plugins``
entry-point group::

    [project.entry-points."beacon.plugins"]
    "my-org/gcs-plugin" = "my_package.plugins:GcsCopyPlugin"

The entry-point key becomes the plugin registry name (with @version stripped
for the lookup) — consistent with how GitHub Actions reference actions:
``uses: actions/checkout@v4`` → action name is ``actions/checkout``.

Installation is performed exactly once per Python process per ref. Results are
cached in :data:`_INSTALLED_REFS` so subsequent tasks with the same
``org/repo@version`` do not trigger a reinstall.
"""

import importlib.metadata
import logging
import re
import subprocess

logger = logging.getLogger("beacon.remote_plugin")

# Matches:
#   org/repo@version     e.g. my-org/gcs-plugin@1.2.0
#   package@version      e.g. beacon-gcs@1.2.0
_REMOTE_REF_RE = re.compile(
    r"^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?"
    r"(/[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?)?"
    r"@[a-zA-Z0-9][a-zA-Z0-9._+\-]*$"
)

# Refs already installed this process (cache — avoids reinstalling on every task).
_INSTALLED_REFS: set[str] = set()


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def is_remote_ref(uses: str) -> bool:
    """Return ``True`` if *uses* looks like a remote plugin reference.

    >>> is_remote_ref("py")
    False
    >>> is_remote_ref("my-org/gcs-plugin@1.2.0")
    True
    >>> is_remote_ref("beacon-gcs@2.0.0")
    True
    """
    return bool(_REMOTE_REF_RE.match(uses))


def ref_to_plugin_name(ref: str) -> str:
    """Strip the ``@version`` suffix to get the registry lookup key.

    >>> ref_to_plugin_name("my-org/gcs-plugin@1.2.0")
    'my-org/gcs-plugin'
    >>> ref_to_plugin_name("beacon-gcs@2.0.0")
    'beacon-gcs'
    """
    return ref.rsplit("@", 1)[0]


def install_and_register(ref: str) -> list[str]:
    """Install a remote plugin via ``uv pip install`` and register it.

    Idempotent — calling with the same *ref* twice is a no-op after the first
    call (the in-process cache prevents a second ``uv pip install``).

    Args:
        ref: ``org/repo@version`` (GitHub) or ``package@version`` (PyPI).

    Returns:
        Sorted list of newly registered plugin names, or ``[]`` when the ref
        was already installed this process.

    Raises:
        RuntimeError: when ``uv pip install`` exits non-zero.
        FileNotFoundError: when ``uv`` is not on PATH.
    """
    from .plugin import PLUGINS_REGISTRY

    if ref in _INSTALLED_REFS:
        return []

    install_spec = _ref_to_install_spec(ref)
    logger.info(
        "Installing remote plugin %r — running: uv pip install %s",
        ref,
        install_spec,
    )

    before = set(PLUGINS_REGISTRY)

    proc = subprocess.run(
        ["uv", "pip", "install", install_spec],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Failed to install remote plugin {ref!r}.\n"
            f"Command: uv pip install {install_spec}\n"
            f"stderr: {proc.stderr.strip()}"
        )

    _load_entry_points()
    _INSTALLED_REFS.add(ref)

    after = set(PLUGINS_REGISTRY)
    newly = sorted(after - before)
    if newly:
        logger.info("Remote plugin %r registered: %s", ref, newly)
    else:
        logger.warning(
            "Remote plugin %r installed but no new plugins were registered. "
            'Does the package declare [project.entry-points."beacon.plugins"]?',
            ref,
        )
    return newly


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ref_to_install_spec(ref: str) -> str:
    """Convert a beacon remote ref to a pip/uv install spec.

    >>> _ref_to_install_spec("my-org/gcs-plugin@1.2.0")
    'git+https://github.com/my-org/gcs-plugin@1.2.0'
    >>> _ref_to_install_spec("beacon-gcs@2.0.0")
    'beacon-gcs==2.0.0'
    """
    name, version = ref.rsplit("@", 1)
    if "/" in name:
        # GitHub-style: org/repo@version
        return f"git+https://github.com/{name}@{version}"
    else:
        # PyPI-style: package@version
        return f"{name}=={version}"


def _load_entry_points() -> None:
    """Scan ``beacon.plugins`` entry points and register any new plugins.

    Entry point format (in the plugin package's ``pyproject.toml``)::

        [project.entry-points."beacon.plugins"]
        "my-org/gcs-plugin" = "my_package.plugins:GcsCopyPlugin"

    The entry-point *name* (left side) becomes the plugin registry key.
    The entry-point *value* (right side) is ``module:ClassName``.
    """
    from .plugin import register_plugin

    try:
        eps = importlib.metadata.entry_points(group="beacon.plugins")
    except Exception as exc:  # noqa: BLE001
        logger.debug("entry_points lookup failed: %s", exc)
        return

    for ep in eps:
        try:
            cls = ep.load()
            register_plugin(cls, ep.name, allow_override=True)
            logger.debug(
                "Entry-point plugin registered: %r → %s",
                ep.name,
                cls.__qualname__,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to load beacon.plugins entry point %r: %s", ep.name, exc
            )
