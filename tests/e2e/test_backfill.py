"""End-to-end tests for Dag.backfill — running a DAG once per cron tick
across a date range, with skip / reset semantics for existing runs.
"""

from datetime import datetime
from typing import Any, ClassVar

import pytest

from beacon import BasePlugin, Dag, Task


# --- Counter plugin --------------------------------------------------------

_RUN_LOG: list[dict[str, Any]] = []


class _RunLogger(BasePlugin):
    """Records (run_id, task_id) on every execution."""

    plugin_name: ClassVar[str] = "_backfill_logger"
    label: str = "x"

    async def execute(self, context):
        _RUN_LOG.append(
            {
                "run_id": context["run_id"],
                "task_id": context["task_id"],
                "logical_date": context["logical_date"],
                "label": self.label,
            }
        )
        return {"label": self.label}


@pytest.fixture(autouse=True)
def _reset_log():
    _RUN_LOG.clear()
    yield


def _make_dag() -> Dag:
    return Dag(
        id="bf-dag",
        owners=["de"],
        actions=[
            Task(id="extract", uses="_backfill_logger", inputs={"label": "E"}),
            Task(
                id="load",
                uses="_backfill_logger",
                upstream=["extract"],
                inputs={"label": "L"},
            ),
        ],
    )


# ---------------------------------------------------------------------------
# 1. Daily backfill produces one run per day
# ---------------------------------------------------------------------------


def test_backfill_daily_produces_one_run_per_day(tmp_path):
    dag = _make_dag()
    results = dag.backfill(
        start_date=datetime(2026, 1, 1),
        end_date=datetime(2026, 1, 5),
        cron="0 0 * * *",
        metadata_path=str(tmp_path / "meta"),
    )

    # 5 days, all succeeded
    assert len(results) == 5
    assert all(r["state"] == "success" for r in results)

    # Logical dates are midnight on each day
    expected_dates = [datetime(2026, 1, d) for d in range(1, 6)]
    assert [r["logical_date"] for r in results] == expected_dates

    # run_ids are deterministic + unique per day
    run_ids = [r["run_id"] for r in results]
    assert len(set(run_ids)) == 5
    assert run_ids[0] == "backfill-bf-dag-20260101T000000"

    # Each run executed both tasks once → 10 executions total
    assert len(_RUN_LOG) == 10
    by_run = {}
    for entry in _RUN_LOG:
        by_run.setdefault(entry["run_id"], []).append(entry["task_id"])
    for tasks in by_run.values():
        assert sorted(tasks) == ["extract", "load"]


# ---------------------------------------------------------------------------
# 2. Hourly backfill across one day produces 24 runs
# ---------------------------------------------------------------------------


def test_backfill_hourly_cron(tmp_path):
    dag = _make_dag()
    results = dag.backfill(
        start_date=datetime(2026, 1, 1, 0, 0),
        end_date=datetime(2026, 1, 1, 23, 0),
        cron="0 * * * *",  # top of every hour
        metadata_path=str(tmp_path / "meta"),
    )
    assert len(results) == 24
    assert results[0]["logical_date"] == datetime(2026, 1, 1, 0, 0)
    assert results[-1]["logical_date"] == datetime(2026, 1, 1, 23, 0)


# ---------------------------------------------------------------------------
# 3. Re-running the same range skips existing runs by default
# ---------------------------------------------------------------------------


def test_backfill_skips_existing_runs_by_default(tmp_path):
    dag = _make_dag()
    meta_path = str(tmp_path / "meta")

    # First pass — 3 fresh runs.
    first = dag.backfill(
        start_date=datetime(2026, 2, 1),
        end_date=datetime(2026, 2, 3),
        cron="0 0 * * *",
        metadata_path=meta_path,
    )
    assert all(r["state"] == "success" for r in first)
    assert len(_RUN_LOG) == 6  # 3 days × 2 tasks

    # Second pass over the same range — all skipped, no new executions.
    second = dag.backfill(
        start_date=datetime(2026, 2, 1),
        end_date=datetime(2026, 2, 3),
        cron="0 0 * * *",
        metadata_path=meta_path,
    )
    assert all(r["state"] == "skipped" for r in second)
    assert len(_RUN_LOG) == 6  # unchanged


# ---------------------------------------------------------------------------
# 4. reset_existing=True clears + re-runs everything in the range
# ---------------------------------------------------------------------------


def test_backfill_reset_existing_reruns_all(tmp_path):
    dag = _make_dag()
    meta_path = str(tmp_path / "meta")

    dag.backfill(
        start_date=datetime(2026, 3, 1),
        end_date=datetime(2026, 3, 2),
        cron="0 0 * * *",
        metadata_path=meta_path,
    )
    assert len(_RUN_LOG) == 4  # 2 days × 2 tasks

    # Reset → re-execute both days.
    results = dag.backfill(
        start_date=datetime(2026, 3, 1),
        end_date=datetime(2026, 3, 2),
        cron="0 0 * * *",
        metadata_path=meta_path,
        reset_existing=True,
    )
    assert all(r["state"] == "success" for r in results)
    assert len(_RUN_LOG) == 8  # original 4 + reset 4


# ---------------------------------------------------------------------------
# 5. Partial overlap: 5-day range with 3 days already run
# ---------------------------------------------------------------------------


def test_backfill_partial_overlap_only_runs_missing_days(tmp_path):
    dag = _make_dag()
    meta_path = str(tmp_path / "meta")

    # Days 1-3 first.
    dag.backfill(
        start_date=datetime(2026, 4, 1),
        end_date=datetime(2026, 4, 3),
        cron="0 0 * * *",
        metadata_path=meta_path,
    )
    assert len(_RUN_LOG) == 6  # 3 × 2

    # Now extend to days 1-5 (default: skip existing).
    results = dag.backfill(
        start_date=datetime(2026, 4, 1),
        end_date=datetime(2026, 4, 5),
        cron="0 0 * * *",
        metadata_path=meta_path,
    )
    states = [r["state"] for r in results]
    assert states == ["skipped", "skipped", "skipped", "success", "success"]
    # Only days 4 and 5 actually executed → +4 entries.
    assert len(_RUN_LOG) == 10


# ---------------------------------------------------------------------------
# 6. Params are persisted on the original run; reset uses originals
# ---------------------------------------------------------------------------


def test_backfill_reset_preserves_original_params(tmp_path):
    """When backfill resets an existing run, the rerun must use the
    ORIGINAL params (persisted in metadata), not whatever the second
    backfill invocation passed. This matches Dag.clear + resume semantics."""

    class _ParamLogger(BasePlugin):
        plugin_name: ClassVar[str] = "_param_logger"
        source: str = ""

        async def execute(self, context):
            _RUN_LOG.append(
                {"run_id": context["run_id"], "source": self.source}
            )
            return {"source": self.source}

    dag = Dag(
        id="bf-params",
        owners=["de"],
        actions=[
            Task(
                id="t",
                uses="_param_logger",
                inputs={"source": "{{ params.source }}"},
            ),
        ],
    )
    meta_path = str(tmp_path / "meta")

    # Original run with source="A"
    dag.backfill(
        start_date=datetime(2026, 5, 1),
        end_date=datetime(2026, 5, 1),
        cron="0 0 * * *",
        params={"source": "A"},
        metadata_path=meta_path,
    )
    assert _RUN_LOG[-1]["source"] == "A"

    # Reset with WRONG params on second invocation.
    dag.backfill(
        start_date=datetime(2026, 5, 1),
        end_date=datetime(2026, 5, 1),
        cron="0 0 * * *",
        params={"source": "DIFFERENT"},  # ignored on reset
        metadata_path=meta_path,
        reset_existing=True,
    )
    # The rerun used the persisted original "A".
    assert _RUN_LOG[-1]["source"] == "A"


# ---------------------------------------------------------------------------
# 7. Validation: end_date < start_date raises
# ---------------------------------------------------------------------------


def test_backfill_invalid_range_raises(tmp_path):
    dag = _make_dag()
    with pytest.raises(ValueError, match="before start_date"):
        dag.backfill(
            start_date=datetime(2026, 1, 5),
            end_date=datetime(2026, 1, 1),
            cron="0 0 * * *",
            metadata_path=str(tmp_path / "meta"),
        )


# ---------------------------------------------------------------------------
# 8. Empty range (no cron tick falls in range) returns []
# ---------------------------------------------------------------------------


def test_backfill_empty_range_returns_empty(tmp_path):
    dag = _make_dag()
    # Daily cron in a 1-second window between days → no tick.
    results = dag.backfill(
        start_date=datetime(2026, 1, 1, 0, 0, 1),
        end_date=datetime(2026, 1, 1, 23, 59, 59),
        cron="0 0 * * *",  # only fires at 00:00
        metadata_path=str(tmp_path / "meta"),
    )
    assert results == []
    assert _RUN_LOG == []
