"""Scoped variable resolution for bundles.

A bundle declares default variable values at three scopes (highest →
lowest precedence):

* dag scope    — ``dags/<group>/<dag>/variables.yml``
* group scope  — ``dags/<group>/global_variables.yml`` (and any ancestor)
* bundle scope — ``dags/global_variables.yml``

A deployment can layer per-deployment overrides on top of all three at
trigger time. Resolution is **shallow per top-level key**: the closest
scope that defines a key wins and replaces the whole value.

A single :class:`VariableScope` is loaded per bundle. Per-DAG resolution
walks from the DAG's source file up to ``dags_root`` collecting files in
"closest-first" order, then merges.
"""

from pathlib import Path
from typing import Any

import yaml

DAG_VARIABLES_FILE = "variables.yml"
GLOBAL_VARIABLES_FILE = "global_variables.yml"


def _load_yaml_mapping(path: Path) -> dict[str, Any]:
    """Read a YAML file expected to be a flat mapping. Empty file → ``{}``."""
    if not path.is_file():
        return {}
    raw = yaml.safe_load(path.read_text()) or {}
    if not isinstance(raw, dict):
        raise ValueError(
            f"Variables file {path} must be a YAML mapping, got "
            f"{type(raw).__name__}"
        )
    return raw


class VariableScope:
    """Per-bundle index of variable files keyed by their scope folder."""

    def __init__(self, dags_root: Path) -> None:
        self.dags_root = Path(dags_root).resolve()

    def resolve_for(self, dag_source_file: Path) -> dict[str, Any]:
        """Merge variable files for one DAG. Closer scope wins.

        ``dag_source_file`` is the path to the DAG's ``dag.yml`` /
        ``dag.py``. The returned dict is a fresh shallow merge — never
        a reference to a file's parsed dict.
        """
        dag_file = Path(dag_source_file).resolve()
        layers = self._collect_layers(dag_file)
        merged: dict[str, Any] = {}
        # ``layers`` is ordered lowest-precedence → highest.
        for layer in layers:
            merged.update(layer)
        return merged

    # --- internals --------------------------------------------------------

    def _collect_layers(self, dag_file: Path) -> list[dict[str, Any]]:
        """Return scope files for ``dag_file``, lowest precedence first.

        Order:
            1. ``dags_root/global_variables.yml``
            2. every ``global_variables.yml`` between ``dags_root`` and
               the DAG's folder, root-down (so the deepest wins)
            3. ``<dag_folder>/variables.yml`` (highest)
        """
        if not self._is_under_dags_root(dag_file):
            # DAG file lives outside ``dags/`` — treat as having no scope.
            return []
        dag_dir = dag_file.parent
        layers: list[dict[str, Any]] = []

        # Walk dags_root → dag_dir (inclusive), root-down, collecting
        # any global_variables.yml at each level.
        chain = self._ancestor_chain(dag_dir)
        for folder in chain:
            layers.append(_load_yaml_mapping(folder / GLOBAL_VARIABLES_FILE))

        # Then the DAG-local variables.yml.
        layers.append(_load_yaml_mapping(dag_dir / DAG_VARIABLES_FILE))
        return layers

    def _ancestor_chain(self, dag_dir: Path) -> list[Path]:
        """``[dags_root, ..., dag_dir]`` (inclusive, root-down)."""
        parts: list[Path] = []
        current = dag_dir.resolve()
        while True:
            parts.append(current)
            if current == self.dags_root:
                break
            parent = current.parent
            if parent == current:
                # Defensive: we walked past the filesystem root without
                # hitting dags_root. ``_is_under_dags_root`` should have
                # guarded against this.
                return []
            current = parent
        return list(reversed(parts))

    def _is_under_dags_root(self, dag_file: Path) -> bool:
        try:
            dag_file.relative_to(self.dags_root)
            return True
        except ValueError:
            return False


def merge_with_overrides(
    scoped: dict[str, Any], overrides: dict[str, Any] | None
) -> dict[str, Any]:
    """Apply deployment-level overrides on top of a scoped dict (shallow)."""
    merged = dict(scoped)
    if overrides:
        merged.update(overrides)
    return merged
