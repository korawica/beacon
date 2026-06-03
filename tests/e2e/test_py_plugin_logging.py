"""End-to-end test: user code inside the ``py`` plugin calls
``ctx.logger.info(...)`` and the unified logging pipeline captures it.

The on-disk layout is the contract the future API server will read:

    {base}/{dag_id}/{run_id}/{task_id}/attempt_{N}.jsonl

API server lookup by (dag_id, task_id, logical_date):
    1. Resolve ``logical_date`` -> ``run_id`` via the metadata store
       (``DagRun`` records carry ``logical_date``).
    2. Read JSONL at the path above for the requested attempt.
"""

import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

import pytest

from beacon.core import LocalExecutor, TaskContext
from beacon.core.task_context import AttemptStatus
from beacon.logging import (
    BeaconLogHandler,
    InMemorySink,
    LocalFileSink,
    configure_logging,
    shutdown_logging,
)

# Ensure py plugin is registered
from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa: F401


USER_SCRIPT_LOGGING = """\
from beacon.runtime import load_context

def main(source_system: str):
    ctx = load_context()
    ctx.logger.info("processing %s", source_system)
    ctx.logger.warning("slow source: %s", source_system)
    ctx.logger.error("simulated error line for %s", source_system)
    return {"ok": True}
"""

USER_SCRIPT_FAIL_THEN_LOG = """\
from beacon.runtime import load_context

def main():
    ctx = load_context()
    ctx.logger.info("attempt %d running", ctx.attempt_number)
    if ctx.attempt_number == 1:
        raise RuntimeError("transient")
    ctx.logger.info("attempt %d succeeded", ctx.attempt_number)
    return {"ok": True}
"""


@pytest.fixture(autouse=True)
def _reset_logging():
    yield
    shutdown_logging()
    root = logging.getLogger("beacon")
    for h in list(root.handlers):
        if isinstance(h, BeaconLogHandler):
            root.removeHandler(h)


@pytest.fixture
def tmp_script(tmp_path):
    def _write(content: str, name: str = "task_script.py") -> str:
        p = tmp_path / name
        p.write_text(content)
        return str(p)

    return _write


def _make_task_context(py_file: str, **inputs_override) -> TaskContext:
    inputs = {
        "py_file": py_file,
        "py_function": "main",
        "params": {"source_system": "postgres"},
        "env": {},
        **inputs_override,
    }
    return TaskContext(
        run_id="run-log-001",
        dag_id="logging-dag",
        task_id="ingest",
        dag_version="v1",
        run_date=datetime(2026, 6, 4, 0, 0, 0),
        logical_date=datetime(2026, 6, 3, 0, 0, 0),
        data_interval_start=datetime(2026, 6, 3, 0, 0, 0),
        data_interval_end=datetime(2026, 6, 4, 0, 0, 0),
        params={"source_system": "postgres"},
        inputs=inputs,
        plugin_name="py",
        retries=1,
        retry_delay=0,
    )


def _wait_for(cond, timeout: float = 2.0):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


# ---------------------------------------------------------------------------
# In-memory sink: ctx.logger calls inside the py plugin are tagged correctly
# ---------------------------------------------------------------------------


def test_py_plugin_ctx_logger_captured_with_task_tags(tmp_script):
    sink = InMemorySink()
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    script = tmp_script(USER_SCRIPT_LOGGING)
    task_ctx = _make_task_context(script)

    result = asyncio.run(LocalExecutor().run_task(task_ctx))
    assert result.last_attempt.state == AttemptStatus.SUCCESS

    # 3 ctx.logger calls from user code must be captured + tagged
    assert _wait_for(
        lambda: sum(1 for r in sink.records if r.source == "task") >= 3
    )

    task_records = [r for r in sink.records if r.source == "task"]
    msgs = {r.msg for r in task_records}
    assert "processing postgres" in msgs
    assert "slow source: postgres" in msgs
    assert "simulated error line for postgres" in msgs

    # Every task record must carry the (dag, run, task, attempt) tags so
    # the API server can serve logs by these keys.
    for rec in task_records:
        assert rec.dag_id == "logging-dag"
        assert rec.run_id == "run-log-001"
        assert rec.task_id == "ingest"
        assert rec.attempt == 1
        # logger name comes from beacon.runtime / py plugin path
        assert rec.logger.startswith("beacon")


# ---------------------------------------------------------------------------
# File sink: on-disk JSONL is the artifact the API server will serve
# ---------------------------------------------------------------------------


def test_py_plugin_ctx_logger_written_to_attempt_jsonl(tmp_script, tmp_path):
    logs_dir = tmp_path / "logs"
    sink = LocalFileSink(logs_dir)
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    script = tmp_script(USER_SCRIPT_LOGGING)
    task_ctx = _make_task_context(script)

    asyncio.run(LocalExecutor().run_task(task_ctx))

    shutdown_logging()  # drain + close file handles

    expected = (
        logs_dir
        / task_ctx.dag_id
        / task_ctx.run_id
        / task_ctx.task_id
        / "attempt_1.jsonl"
    )
    assert expected.exists(), f"expected log file at {expected}"

    records = [json.loads(line) for line in expected.read_text().splitlines()]
    msgs = [r["msg"] for r in records]
    assert "processing postgres" in msgs
    assert "slow source: postgres" in msgs
    assert "simulated error line for postgres" in msgs

    # Each record carries level + tags — what the UI / API server needs.
    levels_seen = {r["level"] for r in records}
    assert {"INFO", "WARNING", "ERROR"}.issubset(levels_seen)
    for rec in records:
        assert rec["dag_id"] == "logging-dag"
        assert rec["run_id"] == "run-log-001"
        assert rec["task_id"] == "ingest"
        assert rec["attempt"] == 1


# ---------------------------------------------------------------------------
# Retry produces a separate attempt_N.jsonl file (per design)
# ---------------------------------------------------------------------------


def test_retry_writes_separate_attempt_files(tmp_script, tmp_path):
    logs_dir = tmp_path / "logs"
    sink = LocalFileSink(logs_dir)
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    script = tmp_script(USER_SCRIPT_FAIL_THEN_LOG)
    task_ctx = _make_task_context(script, params={})
    task_ctx.params = {}
    task_ctx.inputs["params"] = {}

    executor = LocalExecutor()

    # Attempt 1: fails
    result = asyncio.run(executor.run_task(task_ctx))
    assert result.last_attempt.state == AttemptStatus.FAILED
    assert result.has_retries_left is True

    # Attempt 2: succeeds (same TaskContext, retry incremented)
    result = asyncio.run(executor.run_task(result))
    assert result.attempt_number == 2
    assert result.last_attempt.state == AttemptStatus.SUCCESS

    shutdown_logging()

    base = logs_dir / task_ctx.dag_id / task_ctx.run_id / task_ctx.task_id
    attempt1 = base / "attempt_1.jsonl"
    attempt2 = base / "attempt_2.jsonl"
    assert attempt1.exists() and attempt2.exists()

    recs1 = [json.loads(line) for line in attempt1.read_text().splitlines()]
    recs2 = [json.loads(line) for line in attempt2.read_text().splitlines()]

    assert all(r["attempt"] == 1 for r in recs1)
    assert all(r["attempt"] == 2 for r in recs2)
    assert any("attempt 1 running" in r["msg"] for r in recs1)
    assert any("attempt 2 running" in r["msg"] for r in recs2)
    assert any("attempt 2 succeeded" in r["msg"] for r in recs2)
    # attempt 1's log MUST NOT contain attempt-2 messages
    assert not any("attempt 2" in r["msg"] for r in recs1)


# ---------------------------------------------------------------------------
# Simulated API-server-style lookup: resolve logs from (dag_id, logical_date,
# task_id, attempt). The mapping logical_date -> run_id comes from the
# metadata store; once resolved, the file path is deterministic.
# ---------------------------------------------------------------------------


def test_api_style_lookup_by_dag_logical_date_task(tmp_script, tmp_path):
    logs_dir = tmp_path / "logs"
    sink = LocalFileSink(logs_dir)
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    script = tmp_script(USER_SCRIPT_LOGGING)
    task_ctx = _make_task_context(script)

    asyncio.run(LocalExecutor().run_task(task_ctx))
    shutdown_logging()

    # --- simulate what the API server will do ---
    # 1. Caller asks: GET /dags/{dag_id}/logs?logical_date=...&task_id=...&attempt=1
    requested_dag_id = task_ctx.dag_id
    requested_logical_date = task_ctx.logical_date
    requested_task_id = task_ctx.task_id
    requested_attempt = 1

    # 2. Server resolves logical_date -> run_id. In a real server this is a
    #    metadata query; here we simulate with the known TaskContext.
    assert task_ctx.logical_date == requested_logical_date
    resolved_run_id = task_ctx.run_id

    # 3. Server reads the deterministic log path.
    log_path: Path = (
        logs_dir
        / requested_dag_id
        / resolved_run_id
        / requested_task_id
        / f"attempt_{requested_attempt}.jsonl"
    )
    assert log_path.exists()

    # 4. Server streams JSONL back to the caller.
    payload = [json.loads(line) for line in log_path.read_text().splitlines()]
    assert payload, "expected at least one record"
    assert payload[0]["dag_id"] == requested_dag_id
    assert payload[0]["task_id"] == requested_task_id
    assert payload[0]["attempt"] == requested_attempt
