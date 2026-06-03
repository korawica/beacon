"""Test: Deployment model — reuse one DAG with many deployments.

Validates the core "DAG reuse" concept that distinguishes Beacon from Airflow:

  - A `Dag` is a reusable template (defines tasks + param schema).
  - A `Deployment` binds a `Dag` to specific runtime config (cron, params,
    variables_ref, start/end_date) and has its own UI identity.
  - Multiple Deployments can reference the same `dag_id` with different
    params — each produces independent runs and shows under its own name.
"""

from datetime import datetime

import pytest
from pydantic import ValidationError

from beacon.models.dag import Dag
from beacon.models.deployment import Deployment
from beacon.models.param import Param
from beacon.models.task import Task


# --- Reusable DAG template (defined once) ---


def _reusable_etl_dag() -> Dag:
    """A reusable ETL DAG that takes (source_name, target_table, columns)."""
    return Dag(
        id="extract-load-table",
        desc="Reusable extract→load pipeline parameterised by source/target",
        owners=["data-platform"],
        params=[
            Param(name="source_name", type="str", default=None),
            Param(name="target_table", type="str", default=None),
            Param(name="columns", type="array", default=[]),
        ],
        tasks=[
            Task(
                id="extract",
                uses="empty",
                inputs={
                    "source": "{{ params.source_name }}",
                    "cols_count": 0,
                },
            ),
            Task(
                id="load",
                uses="empty",
                upstream=["extract"],
                inputs={
                    "table": "{{ params.target_table }}",
                },
            ),
        ],
    )


class TestDeploymentReuse:
    """Validate that one DAG can be deployed many times with distinct configs."""

    def test_two_deployments_share_one_dag(self):
        """Two Deployments referencing the same dag_id are independent identities."""
        dag = _reusable_etl_dag()

        deploy_customers = Deployment(
            id="daily-customers-from-postgres",
            dag_id=dag.id,
            cron="0 2 * * *",
            timezone="UTC",
            start_date=datetime(2026, 1, 1),
            params={
                "source_name": "postgres_main",
                "target_table": "customers",
                "columns": ["id", "email", "created_at"],
            },
            variables_ref="prod",
        )

        deploy_orders = Deployment(
            id="hourly-orders-from-mysql",
            dag_id=dag.id,
            cron="0 * * * *",
            timezone="Asia/Bangkok",
            start_date=datetime(2026, 6, 1),
            params={
                "source_name": "mysql_replica",
                "target_table": "orders",
                "columns": ["order_id", "customer_id", "total"],
            },
            variables_ref="prod",
        )

        # Same DAG reference
        assert deploy_customers.dag_id == deploy_orders.dag_id == dag.id

        # Distinct UI identities
        assert deploy_customers.id != deploy_orders.id

        # Distinct runtime config
        assert deploy_customers.cron != deploy_orders.cron
        assert deploy_customers.params != deploy_orders.params

    def test_deployment_with_no_cron_is_manual_only(self):
        """Deployment without cron is valid (manual-trigger-only)."""
        d = Deployment(
            id="adhoc-backfill",
            dag_id="extract-load-table",
            start_date=datetime(2026, 1, 1),
            params={"source_name": "snowflake", "target_table": "events"},
        )
        assert d.cron is None
        assert d.enabled is True

    def test_deployment_dag_version_pin(self):
        """Deployment can pin to a specific DAG version."""
        d = Deployment(
            id="frozen-deployment",
            dag_id="extract-load-table",
            dag_version="v1.2.3",
            start_date=datetime(2026, 1, 1),
            params={"source_name": "x", "target_table": "y"},
        )
        assert d.dag_version == "v1.2.3"

    def test_deployment_can_be_disabled(self):
        """A Deployment can be paused via enabled=False."""
        d = Deployment(
            id="paused-deployment",
            dag_id="extract-load-table",
            cron="0 0 * * *",
            start_date=datetime(2026, 1, 1),
            params={"source_name": "x", "target_table": "y"},
            enabled=False,
        )
        assert d.enabled is False


class TestDeploymentValidation:
    """Validate Deployment model rules."""

    def test_end_date_must_be_after_start_date(self):
        with pytest.raises(ValidationError) as exc_info:
            Deployment(
                id="bad-dates",
                dag_id="any-dag",
                start_date=datetime(2026, 6, 1),
                end_date=datetime(2026, 1, 1),  # before start
                params={},
            )
        assert "end_date" in str(exc_info.value)

    def test_end_date_equal_to_start_date_is_invalid(self):
        with pytest.raises(ValidationError):
            Deployment(
                id="bad-dates-equal",
                dag_id="any-dag",
                start_date=datetime(2026, 6, 1),
                end_date=datetime(2026, 6, 1),
                params={},
            )

    def test_dag_id_is_required(self):
        with pytest.raises(ValidationError):
            Deployment(
                id="missing-dag-id",
                start_date=datetime(2026, 1, 1),
                params={},
            )

    def test_id_is_required(self):
        with pytest.raises(ValidationError):
            Deployment(
                dag_id="some-dag",
                start_date=datetime(2026, 1, 1),
                params={},
            )

    def test_defaults(self):
        """Verify sensible defaults."""
        d = Deployment(
            id="default-test",
            dag_id="any-dag",
            start_date=datetime(2026, 1, 1),
        )
        assert d.type == "deployment"
        assert d.timezone == "UTC"
        assert d.catch_up is False
        assert d.enabled is True
        assert d.dag_version is None
        assert d.cron is None
        assert d.end_date is None
        assert d.params == {}
        assert d.variables_ref is None
        assert d.owners == []
        assert d.labels == {}


class TestDeploymentSerialization:
    """Verify Deployment serializes cleanly for metadata storage."""

    def test_round_trip_via_json(self):
        d = Deployment(
            id="serialize-test",
            dag_id="extract-load-table",
            cron="0 5 * * *",
            timezone="UTC",
            start_date=datetime(2026, 1, 1),
            end_date=datetime(2026, 12, 31),
            params={"source_name": "pg", "target_table": "users"},
            variables_ref="prod",
            labels={"team": "data-platform", "tier": "critical"},
            owners=["alice@example.com"],
        )

        as_json = d.model_dump_json()
        restored = Deployment.model_validate_json(as_json)

        assert restored.id == d.id
        assert restored.dag_id == d.dag_id
        assert restored.params == d.params
        assert restored.labels == d.labels
        assert restored.owners == d.owners
        assert restored.variables_ref == d.variables_ref
