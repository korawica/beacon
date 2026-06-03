"""Tests for the unified beacon logging pipeline."""

import json
import logging
import time
from pathlib import Path

import pytest

from beacon.logging import (
    BatchingDispatcher,
    BeaconLogHandler,
    InMemorySink,
    LocalFileSink,
    capture_stdout_stderr,
    configure_logging,
    shutdown_logging,
    task_log_context,
)


@pytest.fixture(autouse=True)
def _reset_logging():
    yield
    shutdown_logging()
    # Detach any leftover BeaconLogHandlers
    root = logging.getLogger("beacon")
    for h in list(root.handlers):
        if isinstance(h, BeaconLogHandler):
            root.removeHandler(h)


def _wait_for(condition, timeout: float = 2.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def test_record_flows_through_dispatcher_to_memory_sink():
    sink = InMemorySink()
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    logging.getLogger("beacon.test").info("hello %s", "world")

    assert _wait_for(lambda: len(sink.records) == 1)
    rec = sink.records[0]
    assert rec.msg == "hello world"
    assert rec.logger == "beacon.test"
    assert rec.source == "framework"  # no task_log_context pushed
    assert rec.dag_id is None


def test_task_log_context_tags_records():
    sink = InMemorySink()
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    with task_log_context("dag1", "run1", "task1", attempt=2):
        logging.getLogger("beacon.task").error("boom")

    assert _wait_for(lambda: len(sink.records) == 1)
    rec = sink.records[0]
    assert rec.source == "task"
    assert (rec.dag_id, rec.run_id, rec.task_id, rec.attempt) == (
        "dag1",
        "run1",
        "task1",
        2,
    )
    assert rec.level == "ERROR"


def test_batching_flushes_on_size_threshold():
    sink = InMemorySink()
    # Large flush interval — only size should trigger.
    configure_logging(sink=sink, batch_size=5, flush_interval_ms=60_000)
    log = logging.getLogger("beacon.test")

    for i in range(5):
        log.info("msg %d", i)

    assert _wait_for(lambda: len(sink.records) == 5, timeout=2.0)


def test_batching_flushes_on_time_threshold():
    sink = InMemorySink()
    configure_logging(sink=sink, batch_size=1000, flush_interval_ms=100)
    log = logging.getLogger("beacon.test")

    log.info("only one")
    # Must arrive after ~100 ms even though batch_size not reached.
    assert _wait_for(lambda: len(sink.records) == 1, timeout=1.0)


def test_local_file_sink_writes_jsonl_sharded_by_task(tmp_path: Path):
    sink = LocalFileSink(tmp_path)
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)
    log = logging.getLogger("beacon.task")

    with task_log_context("d1", "r1", "t1", attempt=1):
        log.info("a")
        log.warning("b")
    with task_log_context("d1", "r1", "t1", attempt=2):
        log.info("retry")
    log.info("framework-level")  # no task ctx -> framework.jsonl

    shutdown_logging()  # drain + close files

    a1 = tmp_path / "d1" / "r1" / "t1" / "attempt_1.jsonl"
    a2 = tmp_path / "d1" / "r1" / "t1" / "attempt_2.jsonl"
    fw = tmp_path / "framework.jsonl"

    assert a1.exists() and a2.exists() and fw.exists()
    lines_a1 = [json.loads(line) for line in a1.read_text().splitlines()]
    assert [r["msg"] for r in lines_a1] == ["a", "b"]
    assert all(r["source"] == "task" for r in lines_a1)
    assert all(r["dag_id"] == "d1" for r in lines_a1)

    lines_a2 = [json.loads(line) for line in a2.read_text().splitlines()]
    assert [r["msg"] for r in lines_a2] == ["retry"]

    fw_records = [json.loads(line) for line in fw.read_text().splitlines()]
    assert any(r["msg"] == "framework-level" for r in fw_records)


def test_capture_stdout_stderr_routes_into_logger():
    sink = InMemorySink()
    configure_logging(sink=sink, batch_size=1, flush_interval_ms=50)

    log = logging.getLogger("beacon.task")
    with task_log_context("d", "r", "t", attempt=1):
        with capture_stdout_stderr(log):
            print("via stdout")
            print("via stderr", flush=True, file=__import__("sys").stderr)

    assert _wait_for(lambda: len(sink.records) >= 2, timeout=2.0)
    msgs = [r.msg for r in sink.records]
    assert "via stdout" in msgs
    assert "via stderr" in msgs


def test_reconfigure_swaps_sink_cleanly():
    s1 = InMemorySink()
    configure_logging(sink=s1, batch_size=1, flush_interval_ms=50)
    logging.getLogger("beacon.x").info("first")
    assert _wait_for(lambda: len(s1.records) == 1)

    s2 = InMemorySink()
    configure_logging(sink=s2, batch_size=1, flush_interval_ms=50)
    logging.getLogger("beacon.x").info("second")
    assert _wait_for(lambda: len(s2.records) == 1)
    # s1 must not receive anything after swap.
    assert all(r.msg != "second" for r in s1.records)


def test_dispatcher_drops_when_queue_full_without_blocking():
    sink = InMemorySink()
    dispatcher = BatchingDispatcher(
        sink, batch_size=10_000, flush_interval=60.0, max_queue=10
    )
    try:
        from beacon.logging import LogRecord

        for i in range(50):
            dispatcher.submit(
                LogRecord(
                    ts=time.time(),
                    level="INFO",
                    msg=str(i),
                    logger="t",
                    source="framework",
                )
            )
        # Should not raise / hang. Some records dropped.
        assert dispatcher.dropped > 0
    finally:
        dispatcher.shutdown()
