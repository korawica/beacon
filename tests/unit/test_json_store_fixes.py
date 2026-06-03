"""Unit tests for JsonMetadata post-improvements."""

import asyncio
from datetime import datetime


from beacon.core import TaskContext, TaskState
from beacon.metadata import JsonMetadata


def _ctx(task_id: str = "t1") -> TaskContext:
    return TaskContext(
        run_id="r1",
        dag_id="d1",
        task_id=task_id,
        dag_version="v",
        run_date=datetime(2026, 1, 1),
        logical_date=datetime(2026, 1, 1),
        data_interval_start=datetime(2026, 1, 1),
        data_interval_end=datetime(2026, 1, 2),
        inputs={},
        plugin_name="empty",
    )


def test_get_missing_does_not_race_toctou(tmp_path):
    """_async_read should handle missing files without TOCTOU."""
    meta = JsonMetadata(tmp_path)
    assert (
        asyncio.run(meta.get_task_state("missing-run", "missing-dag", "t"))
        is None
    )
    assert (
        asyncio.run(meta.get_task_context("missing-run", "missing-dag", "t"))
        is None
    )
    assert asyncio.run(meta.get_dag_run("missing-run", "missing-dag")) is None


def test_lru_cache_eviction(tmp_path, monkeypatch):
    """When cache exceeds _CACHE_SIZE, oldest entry is dropped (LRU)."""
    from beacon.metadata import json_store

    monkeypatch.setattr(json_store, "_CACHE_SIZE", 3)
    meta = JsonMetadata(tmp_path)

    async def run():
        for i in range(5):
            await meta.set_task_state("r", "d", f"t{i}", TaskState.SUCCESS)

    asyncio.run(run())
    # Only last 3 keys remain
    assert len(meta._state_cache) == 3
    assert "r:t0" not in meta._state_cache
    assert "r:t4" in meta._state_cache


def test_get_all_task_states_is_parallel(tmp_path):
    meta = JsonMetadata(tmp_path)

    async def run():
        for i in range(10):
            await meta.set_task_state("r", "d", f"t{i}", TaskState.SUCCESS)
        # Clear cache to force file reads
        meta._state_cache.clear()
        states = await meta.get_all_task_states("r", "d")
        return states

    result = asyncio.run(run())
    assert len(result) == 10
    assert all(s == TaskState.SUCCESS for s in result.values())


def test_evict_run_from_cache(tmp_path):
    meta = JsonMetadata(tmp_path)

    async def run():
        await meta.set_task_state("r1", "d", "t1", TaskState.SUCCESS)
        await meta.set_task_state("r2", "d", "t1", TaskState.SUCCESS)

    asyncio.run(run())
    assert "r1:t1" in meta._state_cache
    meta.evict_run_from_cache("r1")
    assert "r1:t1" not in meta._state_cache
    assert "r2:t1" in meta._state_cache


def test_active_runs_index_updates_on_terminal(tmp_path):
    meta = JsonMetadata(tmp_path)

    async def run():
        await meta.create_dag_run("r1", "d1", "v1")
        await meta.create_dag_run("r2", "d1", "v1")
        active = await meta.list_active_runs("d1")
        assert len(active) == 2

        await meta.update_dag_run_state("r1", "d1", "success")
        active = await meta.list_active_runs("d1")
        assert len(active) == 1
        assert active[0]["run_id"] == "r2"

    asyncio.run(run())
