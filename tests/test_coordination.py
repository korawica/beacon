"""Tests for multi-instance coordination in LocalMetadata.

These tests verify that the coordination primitives work correctly
to prevent duplicate runs when multiple scheduler instances are running.
"""

import tempfile
from datetime import datetime

import pytest

from beacon.metadata import LocalMetadata


class TestScheduledRunCoordination:
    """Tests for try_create_scheduled_run coordination."""

    @pytest.mark.asyncio
    async def test_first_create_succeeds(self):
        """First instance to create a scheduled run should succeed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)
            dag_id = "test-dag"
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            run_id = (
                f"scheduled-{dag_id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )

            created, actual_run_id = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="test-deployment",
                variables={"key": "value"},
            )

            assert created is True
            assert actual_run_id == run_id

    @pytest.mark.asyncio
    async def test_duplicate_create_fails(self):
        """Second create for the same (dag_id, logical_date) should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)
            dag_id = "test-dag"
            logical_date = datetime(2026, 6, 6, 12, 0, 0)
            run_id = (
                f"scheduled-{dag_id}-{logical_date.strftime('%Y%m%dT%H%M%S')}"
            )

            # First create
            created1, run_id1 = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="test-deployment",
            )
            assert created1 is True

            # Second create (same dag_id and logical_date)
            created2, run_id2 = await meta.try_create_scheduled_run(
                run_id=run_id,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date,
                deployment_id="test-deployment",
            )
            assert created2 is False
            assert run_id2 == run_id1  # Returns existing run_id

    @pytest.mark.asyncio
    async def test_different_logical_date_succeeds(self):
        """Different logical_date should create different runs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)
            dag_id = "test-dag"

            # First run at 12:00
            logical_date1 = datetime(2026, 6, 6, 12, 0, 0)
            run_id1 = (
                f"scheduled-{dag_id}-{logical_date1.strftime('%Y%m%dT%H%M%S')}"
            )
            created1, _ = await meta.try_create_scheduled_run(
                run_id=run_id1,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date1,
                deployment_id="test-deployment",
            )
            assert created1 is True

            # Second run at 13:00
            logical_date2 = datetime(2026, 6, 6, 13, 0, 0)
            run_id2 = (
                f"scheduled-{dag_id}-{logical_date2.strftime('%Y%m%dT%H%M%S')}"
            )
            created2, _ = await meta.try_create_scheduled_run(
                run_id=run_id2,
                dag_id=dag_id,
                dag_version="v1",
                logical_date=logical_date2,
                deployment_id="test-deployment",
            )
            assert created2 is True


class TestSchedulerStateCoordination:
    """Tests for try_update_scheduler_state coordination."""

    @pytest.mark.asyncio
    async def test_first_update_succeeds(self):
        """First update to scheduler state should succeed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            await meta.upsert_deployment(
                {
                    "id": "test-dep",
                    "dag_id": "test-dag",
                    "cron": "0 12 * * *",
                }
            )

            updated = await meta.try_update_scheduler_state(
                "test-dep",
                datetime(2026, 6, 6, 12, 0, 0),
            )
            assert updated is True

    @pytest.mark.asyncio
    async def test_same_time_update_fails(self):
        """Update to same time should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            await meta.upsert_deployment(
                {
                    "id": "test-dep",
                    "dag_id": "test-dag",
                    "cron": "0 12 * * *",
                }
            )

            logical_date = datetime(2026, 6, 6, 12, 0, 0)

            # First update
            updated1 = await meta.try_update_scheduler_state(
                "test-dep", logical_date
            )
            assert updated1 is True

            # Same time update
            updated2 = await meta.try_update_scheduler_state(
                "test-dep", logical_date
            )
            assert updated2 is False

    @pytest.mark.asyncio
    async def test_later_time_update_succeeds(self):
        """Update to later time should succeed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            await meta.upsert_deployment(
                {
                    "id": "test-dep",
                    "dag_id": "test-dag",
                    "cron": "0 12 * * *",
                }
            )

            # First update
            updated1 = await meta.try_update_scheduler_state(
                "test-dep",
                datetime(2026, 6, 6, 12, 0, 0),
            )
            assert updated1 is True

            # Later time update
            updated2 = await meta.try_update_scheduler_state(
                "test-dep",
                datetime(2026, 6, 7, 12, 0, 0),
            )
            assert updated2 is True

    @pytest.mark.asyncio
    async def test_earlier_time_update_fails(self):
        """Update to earlier time should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            await meta.upsert_deployment(
                {
                    "id": "test-dep",
                    "dag_id": "test-dag",
                    "cron": "0 12 * * *",
                }
            )

            # First update
            updated1 = await meta.try_update_scheduler_state(
                "test-dep",
                datetime(2026, 6, 7, 12, 0, 0),
            )
            assert updated1 is True

            # Earlier time update
            updated2 = await meta.try_update_scheduler_state(
                "test-dep",
                datetime(2026, 6, 6, 12, 0, 0),
            )
            assert updated2 is False


class TestTriggerCoordination:
    """Tests for try_claim_trigger coordination."""

    @pytest.mark.asyncio
    async def test_first_claim_succeeds(self):
        """First instance to claim a trigger should succeed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create a trigger
            trigger_id = await meta.enqueue_trigger(
                "test-deployment", {"key": "value"}
            )

            # Claim it
            claimed = await meta.try_claim_trigger(
                trigger_id, "test-deployment", "instance-1"
            )
            assert claimed is True

    @pytest.mark.asyncio
    async def test_second_claim_fails(self):
        """Second claim for same trigger should fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create a trigger
            trigger_id = await meta.enqueue_trigger(
                "test-deployment", {"key": "value"}
            )

            # First claim
            claimed1 = await meta.try_claim_trigger(
                trigger_id, "test-deployment", "instance-1"
            )
            assert claimed1 is True

            # Second claim
            claimed2 = await meta.try_claim_trigger(
                trigger_id, "test-deployment", "instance-2"
            )
            assert claimed2 is False

    @pytest.mark.asyncio
    async def test_drain_triggers_with_claim(self):
        """drain_triggers_with_claim should only return claimed triggers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            meta = LocalMetadata(tmpdir)

            # Create two triggers
            trigger_id1 = await meta.enqueue_trigger(
                "test-deployment", {"key": "value1"}
            )
            trigger_id2 = await meta.enqueue_trigger(
                "test-deployment", {"key": "value2"}
            )

            # Drain with instance-1
            triggers = await meta.drain_triggers_with_claim("instance-1")

            # Should get all triggers (no other instance claimed them)
            assert len(triggers) == 2
            trigger_ids = {t["trigger_id"] for t in triggers}
            assert trigger_id1 in trigger_ids
            assert trigger_id2 in trigger_ids
