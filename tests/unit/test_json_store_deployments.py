"""Tests for the deployment + trigger-queue methods on LocalMetadata."""

import asyncio
from datetime import datetime
from pathlib import Path

import pytest

from beacon.metadata import LocalMetadata


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture()
def meta(tmp_path: Path) -> LocalMetadata:
    return LocalMetadata(tmp_path)


# ---------- deployments ----------------------------------------------------


def test_upsert_get_list_delete(meta: LocalMetadata) -> None:
    _run(
        meta.upsert_deployment(
            {"id": "d1", "dag_id": "etl", "cron": "* * * * *"}
        )
    )
    got = _run(meta.get_deployment("d1"))
    assert got is not None
    assert got["dag_id"] == "etl"
    assert got["_scheduler"] == {}

    listed = _run(meta.list_deployments())
    assert [d["id"] for d in listed] == ["d1"]

    assert _run(meta.delete_deployment("d1")) is True
    assert _run(meta.get_deployment("d1")) is None
    assert _run(meta.delete_deployment("d1")) is False


def test_upsert_preserves_scheduler_state(meta: LocalMetadata) -> None:
    _run(meta.upsert_deployment({"id": "d1", "dag_id": "etl"}))
    _run(
        meta.update_deployment_scheduler_state(
            "d1", last_scheduled_at=datetime(2026, 1, 2, 3, 0)
        )
    )
    # Re-upsert (e.g., user changed the cron) — bookkeeping must survive.
    _run(
        meta.upsert_deployment(
            {"id": "d1", "dag_id": "etl", "cron": "0 * * * *"}
        )
    )
    got = _run(meta.get_deployment("d1"))
    assert got["_scheduler"]["last_scheduled_at"] == "2026-01-02T03:00:00"
    assert got["cron"] == "0 * * * *"


def test_get_missing_deployment(meta: LocalMetadata) -> None:
    assert _run(meta.get_deployment("nope")) is None


def test_list_empty_deployments(meta: LocalMetadata) -> None:
    assert _run(meta.list_deployments()) == []


# ---------- trigger queue --------------------------------------------------


def test_enqueue_and_drain_triggers(meta: LocalMetadata) -> None:
    t1 = _run(meta.enqueue_trigger("d1", params={"x": 1}))
    t2 = _run(meta.enqueue_trigger("d1", params={"x": 2}))
    assert t1 != t2

    drained = _run(meta.drain_triggers("d1"))
    assert len(drained) == 2
    assert {d["trigger_id"] for d in drained} == {t1, t2}
    # Drain is destructive.
    assert _run(meta.drain_triggers("d1")) == []


def test_drain_all_deployments(meta: LocalMetadata) -> None:
    _run(meta.enqueue_trigger("d1"))
    _run(meta.enqueue_trigger("d2"))
    drained = _run(meta.drain_triggers())
    assert {d["deployment_id"] for d in drained} == {"d1", "d2"}


def test_drain_empty(meta: LocalMetadata) -> None:
    assert _run(meta.drain_triggers("never")) == []


# ---------- list_dag_runs --------------------------------------------------


def test_list_dag_runs(meta: LocalMetadata) -> None:
    _run(
        meta.create_dag_run(
            run_id="r1",
            dag_id="d",
            dag_version="v",
            logical_date=datetime.now(),
        )
    )
    _run(
        meta.create_dag_run(
            run_id="r2",
            dag_id="d",
            dag_version="v",
            logical_date=datetime.now(),
        )
    )
    runs = _run(meta.list_dag_runs("d"))
    assert sorted(r["run_id"] for r in runs) == ["r1", "r2"]
    assert _run(meta.list_dag_runs("missing")) == []
    # No filter → all.
    all_runs = _run(meta.list_dag_runs())
    assert len(all_runs) == 2


def test_list_dag_runs_respects_limit(meta: LocalMetadata) -> None:
    for i in range(5):
        _run(
            meta.create_dag_run(
                run_id=f"r{i}",
                dag_id="d",
                dag_version="v",
                logical_date=datetime.now(),
            )
        )
    runs = _run(meta.list_dag_runs("d", limit=2))
    assert len(runs) == 2
