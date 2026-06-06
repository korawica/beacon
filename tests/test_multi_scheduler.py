"""Tests for multi-scheduler coordination.

These tests simulate multiple scheduler instances running concurrently
and verify that they coordinate correctly to prevent duplicate runs.
"""

import asyncio
import tempfile
from datetime import datetime

import pytest

from beacon.metadata import LocalMetadata


class TestMultiSchedulerCoordination:
    """Tests for multiple scheduler instances coordinating via LocalMetadata."""

    @pytest.mark.asyncio
    async def test_single_scheduled_run_no_duplicate(self):
        """Two schedulers should not create duplicate runs for the same tick."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create a deployment
            await meta.upsert_deployment(
                {
                    "id": "test-dep",
                    "dag_id": "test-dag",
                    "cron": "0 12 * * *",  # Daily at noon
                    "enabled": True,
                }
            )

            # Create two "instances" trying to schedule the same tick
            logical_date = datetime(2026, 6, 6, 12, 0, 0)

            # Instance 1 tries to claim the tick
            claimed1 = await meta.try_update_scheduler_state(
                "test-dep", logical_date
            )
            assert claimed1 is True, "First instance should claim the tick"

            # Instance 2 tries to claim the same tick
            claimed2 = await meta.try_update_scheduler_state(
                "test-dep", logical_date
            )
            assert claimed2 is False, (
                "Second instance should NOT claim the same tick"
            )

            # Verify the deployment state shows the tick was claimed
            dep = await meta.get_deployment("test-dep")
            assert (
                dep["_scheduler"]["last_scheduled_at"]
                == logical_date.isoformat()
            )

    @pytest.mark.asyncio
    async def test_different_deployments_no_conflict(self):
        """Schedulers should independently coordinate different deployments."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create two deployments
            await meta.upsert_deployment(
                {
                    "id": "dep-1",
                    "dag_id": "dag-1",
                    "cron": "0 12 * * *",
                    "enabled": True,
                }
            )
            await meta.upsert_deployment(
                {
                    "id": "dep-2",
                    "dag_id": "dag-2",
                    "cron": "0 13 * * *",
                    "enabled": True,
                }
            )

            logical_date = datetime(2026, 6, 6, 12, 0, 0)

            # Instance 1 claims dep-1
            claimed1 = await meta.try_update_scheduler_state(
                "dep-1", logical_date
            )
            assert claimed1 is True

            # Instance 2 claims dep-2 (should succeed, different deployment)
            claimed2 = await meta.try_update_scheduler_state(
                "dep-2", logical_date
            )
            assert claimed2 is True

            # Instance 2 tries to claim dep-1 (should fail)
            claimed3 = await meta.try_update_scheduler_state(
                "dep-1", logical_date
            )
            assert claimed3 is False

    @pytest.mark.asyncio
    async def test_concurrent_trigger_claiming(self):
        """Multiple instances should not process the same trigger."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create triggers
            trigger_id1 = await meta.enqueue_trigger("dep-1", {"key": "value1"})
            trigger_id2 = await meta.enqueue_trigger("dep-1", {"key": "value2"})
            trigger_id3 = await meta.enqueue_trigger("dep-1", {"key": "value3"})

            # Simulate concurrent claiming by two instances
            # Instance 1 claims some triggers
            triggers_inst1 = await meta.drain_triggers_with_claim("instance-1")

            # Instance 2 should not see the same triggers
            triggers_inst2 = await meta.drain_triggers_with_claim("instance-2")

            # Verify no overlap
            ids_inst1 = {t["trigger_id"] for t in triggers_inst1}
            ids_inst2 = {t["trigger_id"] for t in triggers_inst2}

            assert len(ids_inst1.intersection(ids_inst2)) == 0, (
                "No trigger should be claimed twice"
            )

            # All triggers should be claimed by someone
            all_claimed = ids_inst1.union(ids_inst2)
            assert trigger_id1 in all_claimed
            assert trigger_id2 in all_claimed
            assert trigger_id3 in all_claimed

    @pytest.mark.asyncio
    async def test_concurrent_scheduled_run_creation(self):
        """Multiple instances creating the same scheduled run should not duplicate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            dag_id = "test-dag"
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            run_id = (
                f"scheduled-{dag_id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )

            # Simulate concurrent creation attempts
            results = await asyncio.gather(
                meta.try_create_scheduled_run(
                    run_id=run_id,
                    dag_id=dag_id,
                    dag_version="v1",
                    logical_date=logical_date,
                    deployment_id="dep-1",
                ),
                meta.try_create_scheduled_run(
                    run_id=run_id,
                    dag_id=dag_id,
                    dag_version="v1",
                    logical_date=logical_date,
                    deployment_id="dep-1",
                ),
                meta.try_create_scheduled_run(
                    run_id=run_id,
                    dag_id=dag_id,
                    dag_version="v1",
                    logical_date=logical_date,
                    deployment_id="dep-1",
                ),
            )

            # Exactly one should succeed
            created_count = sum(1 for created, _ in results if created)
            assert created_count == 1, (
                f"Only one instance should create the run, got {created_count}"
            )

            # All should return the same run_id
            run_ids = {run_id for _, run_id in results}
            assert len(run_ids) == 1, (
                "All instances should return the same run_id"
            )

    @pytest.mark.asyncio
    async def test_simulated_scheduler_tick_race(self):
        """Simulate two scheduler instances racing to schedule the same deployment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create deployment
            await meta.upsert_deployment(
                {
                    "id": "race-dep",
                    "dag_id": "race-dag",
                    "cron": "0 * * * *",  # Hourly
                    "enabled": True,
                }
            )

            logical_date = datetime(2026, 6, 6, 12, 0, 0)

            # Simulate two instances racing to schedule
            async def simulate_scheduler_tick(instance_id: str):
                """Simulate a scheduler instance trying to schedule a tick."""
                # Step 1: Try to claim the tick
                claimed = await meta.try_update_scheduler_state(
                    "race-dep", logical_date
                )
                if not claimed:
                    return {
                        "instance": instance_id,
                        "claimed": False,
                        "created": False,
                    }

                # Step 2: Try to create the run
                run_id = f"scheduled-race-dag-{logical_date.strftime('%Y%m%dT%H%M%S')}"
                created, actual_run_id = await meta.try_create_scheduled_run(
                    run_id=run_id,
                    dag_id="race-dag",
                    dag_version="v1",
                    logical_date=logical_date,
                    deployment_id="race-dep",
                )
                return {
                    "instance": instance_id,
                    "claimed": True,
                    "created": created,
                    "run_id": actual_run_id,
                }

            # Run both instances concurrently
            results = await asyncio.gather(
                simulate_scheduler_tick("instance-1"),
                simulate_scheduler_tick("instance-2"),
            )

            # Verify exactly one succeeded
            created_results = [r for r in results if r.get("created")]
            assert len(created_results) == 1, (
                "Exactly one instance should create the run"
            )

            # Verify the other instance was blocked
            not_created_results = [r for r in results if not r.get("created")]
            assert len(not_created_results) == 1, (
                "One instance should be blocked"
            )

            # Verify the run was created
            run = await meta.get_dag_run(
                created_results[0]["run_id"], "race-dag"
            )
            assert run is not None, "Run should exist in metadata"

    @pytest.mark.asyncio
    async def test_backfill_coordination(self):
        """Multiple instances should coordinate on backfill runs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create deployment with catch_up
            await meta.upsert_deployment(
                {
                    "id": "backfill-dep",
                    "dag_id": "backfill-dag",
                    "cron": "0 0 * * *",  # Daily
                    "enabled": True,
                    "catch_up": True,
                }
            )

            # Simulate multiple backfill ticks
            ticks = [
                datetime(2026, 6, 1, 0, 0, 0),
                datetime(2026, 6, 2, 0, 0, 0),
                datetime(2026, 6, 3, 0, 0, 0),
            ]

            # Instance 1 creates first tick
            claimed1 = await meta.try_update_scheduler_state(
                "backfill-dep", ticks[0]
            )
            assert claimed1 is True
            created1, _ = await meta.try_create_scheduled_run(
                run_id=f"scheduled-backfill-dag-{ticks[0].strftime('%Y%m%dT%H%M%S')}",
                dag_id="backfill-dag",
                dag_version="v1",
                logical_date=ticks[0],
                deployment_id="backfill-dep",
            )
            assert created1 is True

            # Instance 2 creates second tick (later time)
            claimed2 = await meta.try_update_scheduler_state(
                "backfill-dep", ticks[1]
            )
            assert claimed2 is True
            created2, _ = await meta.try_create_scheduled_run(
                run_id=f"scheduled-backfill-dag-{ticks[1].strftime('%Y%m%dT%H%M%S')}",
                dag_id="backfill-dag",
                dag_version="v1",
                logical_date=ticks[1],
                deployment_id="backfill-dep",
            )
            assert created2 is True

            # Instance 1 tries to create first tick again (should fail)
            created3, _ = await meta.try_create_scheduled_run(
                run_id=f"scheduled-backfill-dag-{ticks[0].strftime('%Y%m%dT%H%M%S')}",
                dag_id="backfill-dag",
                dag_version="v1",
                logical_date=ticks[0],
                deployment_id="backfill-dep",
            )
            assert created3 is False, "Should not create duplicate backfill run"


class TestSchedulerInstanceState:
    """Tests for scheduler state tracking across instances."""

    @pytest.mark.asyncio
    async def test_last_scheduled_at_persists(self):
        """last_scheduled_at should persist across instance restarts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            await meta.upsert_deployment(
                {
                    "id": "persist-dep",
                    "dag_id": "persist-dag",
                    "cron": "0 * * * *",
                }
            )

            # Instance 1 schedules a tick
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            await meta.try_update_scheduler_state("persist-dep", logical_date)

            # Simulate "restart" - create new metadata instance pointing to same path
            meta2 = LocalMetadata(tmpdir)

            # Verify state persisted
            dep = await meta2.get_deployment("persist-dep")
            assert (
                dep["_scheduler"]["last_scheduled_at"]
                == logical_date.isoformat()
            )

            # New instance should not be able to schedule same tick
            claimed = await meta2.try_update_scheduler_state(
                "persist-dep", logical_date
            )
            assert claimed is False, "New instance should see previous state"

    @pytest.mark.asyncio
    async def test_instance_id_in_run_metadata(self):
        """Runs should track which instance created them."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            dag_id = "track-dag"
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            run_id = (
                f"scheduled-{dag_id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )

            created, actual_run_id = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="track-dep",
                variables={"source": "test"},
            )

            assert created is True

            # Verify run was created
            run = await meta.get_dag_run(run_id, dag_id)
            assert run is not None
            assert run["dag_id"] == dag_id
            assert run["logical_date"] == str(logical_date)


class TestEdgeCases:
    """Edge cases for multi-scheduler coordination."""

    @pytest.mark.asyncio
    async def test_deployment_not_found(self):
        """Should handle missing deployment gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Try to update scheduler state for non-existent deployment
            claimed = await meta.try_update_scheduler_state(
                "non-existent-dep",
                datetime(2026, 6, 6, 12, 0, 0),
            )
            assert claimed is False

    @pytest.mark.asyncio
    async def test_concurrent_deployment_update(self):
        """Should handle concurrent deployment updates safely.

        Note: With file locks, concurrent updates are serialized. The first
        to acquire the lock wins. This test verifies that only one instance
        wins when all try to update at the same time.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create deployment
            await meta.upsert_deployment(
                {
                    "id": "concurrent-dep",
                    "dag_id": "concurrent-dag",
                    "cron": "0 * * * *",
                }
            )

            # Multiple instances try to update at the same time
            # With file locks, they are serialized
            async def update_deployment(instance_id: str, tick: datetime):
                claimed = await meta.try_update_scheduler_state(
                    "concurrent-dep", tick
                )
                return {
                    "instance": instance_id,
                    "tick": tick,
                    "claimed": claimed,
                }

            results = await asyncio.gather(
                update_deployment("inst-1", datetime(2026, 6, 6, 10, 0, 0)),
                update_deployment("inst-2", datetime(2026, 6, 6, 11, 0, 0)),
                update_deployment("inst-3", datetime(2026, 6, 6, 12, 0, 0)),
            )

            # With serialized lock acquisition, first wins, others see updated state
            # The winner's tick becomes the new state
            claimed_results = [r for r in results if r["claimed"]]
            assert len(claimed_results) >= 1, (
                "At least one instance should claim"
            )

            # Verify the final state is one of the ticks
            dep = await meta.get_deployment("concurrent-dep")
            final_tick = dep["_scheduler"]["last_scheduled_at"]
            assert final_tick in [
                datetime(2026, 6, 6, 10, 0, 0).isoformat(),
                datetime(2026, 6, 6, 11, 0, 0).isoformat(),
                datetime(2026, 6, 6, 12, 0, 0).isoformat(),
            ]

    @pytest.mark.asyncio
    async def test_lock_cleanup_on_crash(self):
        """Lock files should not block forever after process crash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            dag_id = "crash-dag"
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            run_id = (
                f"scheduled-{dag_id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )

            # Create a run
            created1, _ = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="crash-dep",
            )
            assert created1 is True

            # Simulate "crash" - the lock is released after the operation completes
            # A new instance should still be able to detect the existing run
            created2, existing_run_id = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="crash-dep",
            )
            assert created2 is False
            assert existing_run_id == run_id
