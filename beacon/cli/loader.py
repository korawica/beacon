"""DAG / Bundle loading for the CLI.

Handles three input shapes:
  * a single ``.py`` file  → import it; collect every ``Dag`` instance
  * a single ``.yml/.yaml`` file → parse YAML; build ``Dag.model_validate``
  * a directory → treat as a ``LocalBundle``; load plugins; load all DAGs

The loader is deliberately minimal — no fancy module-name munging beyond
what's needed to keep imports collision-free.
"""

import importlib.util
import sys
import uuid
from pathlib import Path
from typing import Any

import yaml

from ..core.bundle import LocalBundle
from ..models.dag import Dag


def load_dags(path: str | Path) -> list[Dag]:
    """Load every Dag at ``path`` (single file or bundle directory)."""
    p = Path(path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"No such path: {p}")
    if p.is_file():
        return _load_dags_from_file(p)
    # Directory → LocalBundle
    bundle = LocalBundle(name=p.name, path=p)
    bundle.load_plugins()
    dags: list[Dag] = []
    for f in bundle.discover_dags():
        dags.extend(_load_dags_from_file(f))
    return dags


def load_one_dag(path: str | Path, dag_id: str | None = None) -> Dag:
    """Load a single Dag. If ``dag_id`` is None, requires exactly one."""
    dags = load_dags(path)
    if not dags:
        raise ValueError(f"No DAGs found at {path}")
    if dag_id is not None:
        for d in dags:
            if d.id == dag_id:
                return d
        raise ValueError(
            f"DAG {dag_id!r} not found at {path}. "
            f"Available: {[d.id for d in dags]}"
        )
    if len(dags) > 1:
        raise ValueError(
            f"Multiple DAGs at {path}; pass --dag-id. "
            f"Available: {[d.id for d in dags]}"
        )
    return dags[0]


# --- internals -------------------------------------------------------------


def _load_dags_from_file(path: Path) -> list[Dag]:
    if path.suffix == ".py":
        return _load_py(path)
    if path.suffix in (".yml", ".yaml"):
        return _load_yaml(path)
    return []


def _load_py(path: Path) -> list[Dag]:
    """Import a .py file and collect any ``Dag`` instances defined in it."""
    mod_name = f"_beacon_cli_{path.stem}_{uuid.uuid4().hex[:6]}"
    spec = importlib.util.spec_from_file_location(mod_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    try:
        spec.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(mod_name, None)
        raise
    return [v for v in vars(module).values() if isinstance(v, Dag)]


def _load_yaml(path: Path) -> list[Dag]:
    """Parse YAML doc(s). One file may contain a single mapping or a list."""
    text = path.read_text()
    docs: list[Any] = []
    for doc in yaml.safe_load_all(text):
        if doc is None:
            continue
        if isinstance(doc, list):
            docs.extend(doc)
        else:
            docs.append(doc)
    dags: list[Dag] = []
    for raw in docs:
        if not isinstance(raw, dict):
            continue
        # Only consume DAG-shaped docs; ignore foreign types (e.g. variables).
        if raw.get("type", "dag") != "dag":
            continue
        dags.append(Dag.model_validate(raw))
    return dags
