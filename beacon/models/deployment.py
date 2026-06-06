"""Deployment Model.

A Deployment binds a reusable DAG template to a specific runtime configuration:
schedule (cron), variables (stage-specific config), and identity (shown in the UI).
The same DAG can have many Deployments — each one represents a distinct production binding.

This is the equivalent of Prefect's `Deployment`. It is the model that fixes
Airflow's "one DAG file = one schedule" coupling.

Example:
    Dag(id="extract-load-table")

    Deployment(id="daily-customers-from-postgres", dag_id="extract-load-table",
               cron="0 2 * * *", variable_overrides={"source_system": "postgres"})

    Deployment(id="hourly-orders-from-mysql", dag_id="extract-load-table",
               cron="0 * * * *", variable_overrides={"source_system": "mysql"})
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class Deployment(BaseModel):
    """A deployment of a reusable DAG with specific runtime configuration.

    Multiple Deployments can reference the same `dag_id` with different
    variables and schedules. Each Deployment has its own identity
    in the UI and produces independent DagRuns.
    """

    id: str = Field(
        description=(
            "Deployment identity (shown in UI). Unique across all deployments. "
            "Example: 'daily-customers-from-postgres'"
        )
    )
    type: Literal["deployment"] = Field(default="deployment")
    dag_id: str = Field(
        description="Reference to the reusable Dag.id this deployment uses"
    )
    dag_version: str | None = Field(
        default=None,
        description=(
            "Optional pin to a specific DAG version. If None, the latest "
            "deployed version is used."
        ),
    )
    desc: str | None = Field(
        default=None,
        description="Human-readable description of this deployment",
    )
    enabled: bool = Field(
        default=True,
        description="Whether this deployment is active (eligible for scheduling)",
    )

    # --- Scheduling ---
    cron: str | None = Field(
        default=None,
        description=(
            "Cron expression for scheduled runs. None = manual-trigger only."
        ),
    )
    timezone: str = Field(
        default="UTC", description="IANA timezone for cron evaluation"
    )
    start_date: datetime | None = Field(
        default=None,
        description=(
            "No runs scheduled before this date. Optional for manual-only "
            "deployments (``cron=None``)."
        ),
    )
    end_date: datetime | None = Field(
        default=None,
        description="No runs scheduled after this date. None = no end.",
    )
    catch_up: bool = Field(
        default=False,
        description=(
            "If true, schedule all missed runs since start_date when the "
            "scheduler starts (or deployment is created). If false (default), "
            "only the most recent cron tick is scheduled."
        ),
    )
    max_active_runs: int | None = Field(
        default=None,
        description=(
            "Maximum number of concurrent runs for this deployment. "
            "None = unlimited. When catch_up=True, limits how many "
            "backfill runs can be in-flight at once."
        ),
    )

    # --- Runtime configuration ---
    variable_overrides: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Per-deployment variable overrides. Highest precedence in the "
            "scope chain (deployment > dag variables.yml > group "
            "global_variables.yml > bundle global_variables.yml). "
            "Storing any override marks the deployment as 'pinned' — see "
            "``is_pinned``."
        ),
    )

    # --- Labels / Ownership ---
    variable_requirements: dict[str, dict] = Field(
        default_factory=dict,
        description=(
            "Variable requirements extracted from DAG templates at deploy time. "
            "Keys are variable names, values contain: "
            "'has_default' (bool) and optionally 'default_value'. "
            "Used by trigger command to validate custom variables before runtime."
        ),
    )

    owners: list[str] = Field(
        default_factory=list,
        description="Owners of this deployment (overrides Dag.owners if set)",
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description="Free-form labels for filtering/grouping in UI",
    )

    @model_validator(mode="after")
    def _validate_date_range(self) -> Deployment:
        if (
            self.end_date is not None
            and self.start_date is not None
            and self.end_date <= self.start_date
        ):
            raise ValueError(
                f"end_date ({self.end_date}) must be after "
                f"start_date ({self.start_date})"
            )
        return self

    @property
    def is_pinned(self) -> bool:
        """A Deployment is pinned iff it stores any variable override.

        Pinned deployments are not auto-rolled to a new ``dag_version``
        on ``beacon sync`` — see ``docs/core/deploy.md``.
        """
        return bool(self.variable_overrides)
