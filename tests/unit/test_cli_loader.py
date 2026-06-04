"""Tests for ``beacon.cli.loader``."""

import textwrap
from pathlib import Path

import pytest

from beacon.cli.loader import load_dags, load_one_dag
from beacon.models.dag import Dag


def _write(path: Path, body: str) -> Path:
    path.write_text(textwrap.dedent(body).lstrip())
    return path


# ---------- .py loader -----------------------------------------------------


def test_load_dags_from_single_py(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.py",
        """
        from beacon import Dag, Task
        dag = Dag(id="x", actions=[Task(id="t", uses="empty")])
        """,
    )
    dags = load_dags(f)
    assert len(dags) == 1
    assert dags[0].id == "x"
    assert isinstance(dags[0], Dag)


def test_load_dags_from_py_with_multiple(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.py",
        """
        from beacon import Dag, Task
        a = Dag(id="a", actions=[Task(id="t", uses="empty")])
        b = Dag(id="b", actions=[Task(id="t", uses="empty")])
        """,
    )
    ids = sorted(d.id for d in load_dags(f))
    assert ids == ["a", "b"]


# ---------- .yml loader ----------------------------------------------------


def test_load_dags_from_yaml(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.yml",
        """
        id: y1
        actions:
          - id: t
            type: task
            uses: empty
        """,
    )
    dags = load_dags(f)
    assert len(dags) == 1
    assert dags[0].id == "y1"


def test_yaml_ignores_non_dag_docs(tmp_path: Path) -> None:
    """Foreign top-level docs (e.g. variables) are skipped, not errored."""
    f = _write(
        tmp_path / "mixed.yml",
        """
        type: variable
        stages: {dev: {}}
        ---
        id: ok
        actions:
          - id: t
            type: task
            uses: empty
        """,
    )
    dags = load_dags(f)
    assert [d.id for d in dags] == ["ok"]


# ---------- bundle directory ----------------------------------------------


def test_load_dags_from_directory(tmp_path: Path) -> None:
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    _write(
        dags_dir / "a.py",
        """
        from beacon import Dag, Task
        a = Dag(id="a", actions=[Task(id="t", uses="empty")])
        """,
    )
    _write(
        dags_dir / "b.yml",
        """
        id: b
        actions:
          - id: t
            type: task
            uses: empty
        """,
    )
    ids = sorted(d.id for d in load_dags(tmp_path))
    assert ids == ["a", "b"]


# ---------- error cases ----------------------------------------------------


def test_missing_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_dags(tmp_path / "nope.py")


def test_load_one_dag_ambiguous(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.py",
        """
        from beacon import Dag, Task
        a = Dag(id="a", actions=[Task(id="t", uses="empty")])
        b = Dag(id="b", actions=[Task(id="t", uses="empty")])
        """,
    )
    with pytest.raises(ValueError, match="Multiple DAGs"):
        load_one_dag(f)


def test_load_one_dag_by_id(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.py",
        """
        from beacon import Dag, Task
        a = Dag(id="a", actions=[Task(id="t", uses="empty")])
        b = Dag(id="b", actions=[Task(id="t", uses="empty")])
        """,
    )
    assert load_one_dag(f, dag_id="b").id == "b"


def test_load_one_dag_unknown_id(tmp_path: Path) -> None:
    f = _write(
        tmp_path / "d.py",
        """
        from beacon import Dag, Task
        a = Dag(id="a", actions=[Task(id="t", uses="empty")])
        """,
    )
    with pytest.raises(ValueError, match="not found"):
        load_one_dag(f, dag_id="missing")


def test_load_one_dag_empty_file(tmp_path: Path) -> None:
    f = _write(tmp_path / "d.py", "# no dags here\n")
    with pytest.raises(ValueError, match="No DAGs"):
        load_one_dag(f)
