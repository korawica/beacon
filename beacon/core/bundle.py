"""Bundles.

A bundle is a source of DAG definitions, plugins, assets, and the
default variable values that go with them. It represents a deployable
unit — typically a Git repository with this structure (see
``docs/core/deploy.md`` for the full policy)::

    my-workflow-repo/
    ├── dags/
    │   ├── global_variables.yml          # bundle-wide variable defaults
    │   └── group/
    │       ├── global_variables.yml      # group-scope variable defaults
    │       └── dag_name/
    │           ├── dag.yml
    │           ├── variables.yml         # dag-scope variable defaults
    │           └── assets/               # dag-local files for ``uses: py``
    ├── plugins/                          # auto-discovered custom plugins
    └── assets/                           # bundle-global files for ``uses: py``

The bundle is responsible for:
  1. Discovering and loading custom plugins from ``./plugins``
  2. Parsing DAG definitions from ``./dags``
  3. Exposing the scoped :class:`VariableScope` for variable resolution
  4. Computing a version tag (content hash) used to detect drift
"""

import hashlib
import importlib.util
import logging
import sys
from pathlib import Path

from .variables import VariableScope

logger = logging.getLogger("beacon.bundle")


class LocalBundle:
    """Local Bundle — loads DAGs and plugins from a local directory.

    Expected structure (see ``docs/core/deploy.md``)::

        {path}/
        ├── dags/       # DAG definitions (.yml or .py) + variables files
        ├── plugins/    # Custom plugins (auto-registered)
        └── assets/     # Bundle-global asset files

    A flat layout (single ``dag.yml`` at ``path``) is still tolerated for
    one-off / ad-hoc runs; variable scoping then has no effect.
    """

    def __init__(self, name: str, path: str | Path) -> None:
        self.name = name
        self.path = Path(path).resolve()
        self._version: str | None = None
        self._variable_scope: VariableScope | None = None

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
        """Compute bundle version from file content hashes (cached)."""
        if self._version is None:
            self._version = self._compute_version()
        return self._version

    @property
    def variable_scope(self) -> VariableScope:
        """Lazy :class:`VariableScope` rooted at ``dags_path``."""
        if self._variable_scope is None:
            self._variable_scope = VariableScope(dags_root=self.dags_path)
        return self._variable_scope

    def load_plugins(self) -> list[str]:
        """Discover and register custom plugins from the plugins directory.

        Scans ``./plugins`` for ``.py`` files, imports them, and reports
        plugin names that were registered as a result. Detection uses a
        before/after snapshot of :data:`PLUGINS_REGISTRY` so we don't have
        to walk module attributes.
        """
        if self.plugins_path is None:
            return []

        from .plugin import PLUGINS_REGISTRY

        registered: list[str] = []
        parent = str(self.plugins_path)

        if parent not in sys.path:
            sys.path.insert(0, parent)

        plugins_root = self.plugins_path
        for py_file in sorted(plugins_root.rglob("*.py")):
            if py_file.name.startswith("_"):
                continue

            rel = py_file.relative_to(plugins_root).with_suffix("")
            module_name = f"_beacon_bundle_{self.name}_" + "_".join(rel.parts)

            try:
                spec = importlib.util.spec_from_file_location(
                    module_name, py_file
                )
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)

                before = set(PLUGINS_REGISTRY)
                spec.loader.exec_module(module)
                after = set(PLUGINS_REGISTRY)

                newly = sorted(after - before)
                registered.extend(newly)
                logger.debug(
                    "Loaded plugin file: %s (new=%s)", py_file.name, newly
                )
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to load plugin %s: %s", py_file.name, exc)

        if registered:
            logger.info(
                "Bundle %r registered plugins: %s", self.name, registered
            )
        return registered

    def discover_dags(self) -> list[Path]:
        """Find all DAG definition files in the dags directory.

        Reserved filenames are skipped: ``global_variables.yml`` (any
        scope) and ``variables.yml`` (dag scope) carry default
        variable values, not DAG definitions.
        """
        from .variables import DAG_VARIABLES_FILE, GLOBAL_VARIABLES_FILE

        reserved = {DAG_VARIABLES_FILE, GLOBAL_VARIABLES_FILE}
        dags: list[Path] = []
        for pattern in ("**/*.yml", "**/*.yaml", "**/*.py"):
            for f in sorted(self.dags_path.glob(pattern)):
                if f.name.startswith("_") or f.name in reserved:
                    continue
                dags.append(f)
        return dags

    def _compute_version(self) -> str:
        """Compute version hash from all files in the bundle.

        Hash inputs are the relative path + file size + mtime_ns. This is
        intentionally cheaper than reading every file's content while still
        invalidating on any file modification.
        """
        hasher = hashlib.sha256()
        for f in sorted(self.path.rglob("*")):
            if not f.is_file() or f.name.startswith("."):
                continue
            rel = f.relative_to(self.path).as_posix()
            stat = f.stat()
            hasher.update(f"{rel}|{stat.st_size}|{stat.st_mtime_ns}\n".encode())
        return hasher.hexdigest()[:12]


class GitBundle:
    """Git Bundle — syncs from a Git repository.

    The actual ``git pull`` is delegated to whatever process drives the
    sync (a CLI command, a webhook handler, etc.). This class only owns
    the in-memory view: where the checkout lives, and a :class:`LocalBundle`
    that points into it.
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
    def local(self) -> LocalBundle:
        """Return (and lazily create) the local view of the sync checkout."""
        if self._local is None:
            root = self.sync_path
            if self.sub_path:
                root = root / self.sub_path
            self._local = LocalBundle(name=self.name, path=root)
        return self._local

    @property
    def version(self) -> str:
        """Bundle version (file-hash based)."""
        return self.local.version

    def load_plugins(self) -> list[str]:
        """Load plugins from the synced repo."""
        return self.local.load_plugins()

    def discover_dags(self) -> list[Path]:
        """Discover DAGs from the synced repo."""
        return self.local.discover_dags()
