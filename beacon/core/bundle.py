"""Bundles.

A bundle is a source of DAG definitions and custom plugins. It represents
a deployable unit — typically a Git repository with this structure:

    my-workflow-repo/
    ├── dags/
    │   ├── hello_world.yml
    │   └── etl_pipeline.py
    └── plugins/
        └── my_custom_plugin.py

The bundle is responsible for:
  1. Discovering and loading custom plugins from ./plugins
  2. Parsing DAG definitions from ./dags
  3. Computing a version tag (content hash, git SHA, etc.)

Production flow (GitBundle):
  - Team merges to main branch
  - GitBundle sync detects new commit → pulls repo
  - Bundle.load() auto-discovers plugins → registers them
  - Bundle.load() parses DAGs → stores versioned in metadata
"""

import hashlib
import importlib.util
import logging
import sys
from pathlib import Path
from typing import Any

logger = logging.getLogger("beacon.bundle")

BUNDLE_MEMORY_DAGS: dict = {}


class BaseBundle:
    """Base Bundle.

    This bundle will be the default bundle for loaded data on memory.
    """

    def __init__(self): ...


class LocalBundle:
    """Local Bundle.

    Loads DAGs and plugins from a local directory.

    Expected structure:
        {path}/
        ├── dags/       # DAG definitions (.yml or .py)
        └── plugins/    # Custom plugins (auto-registered)

    Or flat structure (all files at root):
        {path}/
        ├── dag.yml
        └── my_plugin.py
    """

    def __init__(self, name: str, path: str | Path) -> None:
        self.name = name
        self.path = Path(path).resolve()
        self._version: str | None = None

    @property
    def dags_path(self) -> Path:
        """Path to DAG definitions directory."""
        dags = self.path / "dags"
        return dags if dags.is_dir() else self.path

    @property
    def plugins_path(self) -> Path | None:
        """Path to custom plugins directory, or None if not present."""
        plugins = self.path / "plugins"
        return plugins if plugins.is_dir() else None

    @property
    def version(self) -> str:
        """Compute bundle version from file content hashes."""
        if self._version is None:
            self._version = self._compute_version()
        return self._version  # type: ignore[return-value]

    def load_plugins(self) -> list[str]:
        """Discover and register custom plugins from the plugins directory.

        Scans ./plugins for .py files, imports them, and any BasePlugin
        subclass defined in them will auto-register via PluginMeta.

        Returns:
            List of registered plugin names.
        """
        if self.plugins_path is None:
            return []

        registered: list[str] = []
        parent = str(self.plugins_path)

        # Add plugins dir to sys.path so inter-plugin imports work
        if parent not in sys.path:
            sys.path.insert(0, parent)

        for py_file in sorted(self.plugins_path.glob("**/*.py")):
            if py_file.name.startswith("_"):
                continue

            module_name = f"_beacon_bundle_{self.name}_{py_file.stem}"
            try:
                spec = importlib.util.spec_from_file_location(
                    module_name, py_file
                )
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)

                # Check what got registered
                from .plugin import PLUGINS_REGISTRY

                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and hasattr(attr, "plugin_name")
                        and getattr(attr, "plugin_name", None)
                        in PLUGINS_REGISTRY
                    ):
                        registered.append(attr.plugin_name)

                logger.debug("Loaded plugin file: %s", py_file.name)
            except Exception as exc:
                logger.error("Failed to load plugin %s: %s", py_file.name, exc)

        if registered:
            logger.info(
                "Bundle %r registered plugins: %s",
                self.name,
                registered,
            )
        return registered

    def discover_dags(self) -> list[Path]:
        """Find all DAG definition files in the dags directory.

        Returns:
            List of paths to .yml/.yaml/.py DAG files.
        """
        dags: list[Path] = []
        for pattern in ("**/*.yml", "**/*.yaml", "**/*.py"):
            for f in sorted(self.dags_path.glob(pattern)):
                if not f.name.startswith("_"):
                    dags.append(f)
        return dags

    def _compute_version(self) -> str:
        """Compute version hash from all files in the bundle."""
        hasher = hashlib.sha256()
        for f in sorted(self.path.rglob("*")):
            if f.is_file() and not f.name.startswith("."):
                hasher.update(f.read_bytes())
        return hasher.hexdigest()[:12]


class GitBundle:
    """Git Bundle.

    Syncs from a Git repository. The API server pulls the repo on webhook
    or polling, then loads plugins and parses DAGs.

    Production flow:
        1. API server receives webhook (push to main)
        2. git pull → local checkout at {sync_path}/{name}
        3. version = commit SHA
        4. load_plugins() → register custom plugins
        5. discover_dags() → parse and store in metadata

    Attributes:
        name: Bundle identifier.
        repo_url: Git repository URL.
        branch: Target branch to sync from.
        sync_path: Local path where repo is checked out.
    """

    def __init__(
        self,
        name: str,
        repo_url: str,
        branch: str = "main",
        sync_path: str | Path = "/tmp/beacon/bundles",
        sub_path: str | None = None,
    ) -> None:
        self.name = name
        self.repo_url = repo_url
        self.branch = branch
        self.sync_path = Path(sync_path) / name
        self.sub_path = sub_path  # e.g. "workflows/" within the repo
        self._local: LocalBundle | None = None

    @property
    def version(self) -> str:
        """Git commit SHA as version (set after sync)."""
        # In real implementation: git rev-parse HEAD
        return self._local.version if self._local else "unsynced"

    @property
    def local(self) -> LocalBundle:
        """Get the local bundle after sync."""
        if self._local is None:
            root = self.sync_path
            if self.sub_path:
                root = root / self.sub_path
            self._local = LocalBundle(name=self.name, path=root)
        return self.local

    def load_plugins(self) -> list[str]:
        """Load plugins from the synced repo."""
        return self.local.load_plugins()

    def discover_dags(self) -> list[Path]:
        """Discover DAGs from the synced repo."""
        return self.local.discover_dags()


class GcsBundle:
    """GCS Bundle."""

    def __init__(
        self,
        name: str,
        ref: str,
        connection: Any,
    ) -> None:
        self.name = name
        self.ref = ref
        self.connection = connection
