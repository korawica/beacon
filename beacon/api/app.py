"""FastAPI application factory for Beacon API.

This module creates the FastAPI app with all endpoints for managing
deployments, triggers, and runs. The app can optionally embed a
scheduler for the merged API+Scheduler architecture.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from ..metadata import LocalMetadata
from ..scheduler import DeploymentScheduler


class TriggerRequest(BaseModel):
    """Request body for creating a manual trigger."""

    deployment_id: str
    variables: dict[str, Any] | None = None


class TriggerResponse(BaseModel):
    """Response for trigger creation."""

    trigger_id: str
    deployment_id: str
    message: str


class DeploymentCreate(BaseModel):
    """Request body for creating/updating a deployment."""

    id: str
    dag_id: str
    cron: str | None = None
    timezone: str = "UTC"
    start_date: datetime | None = None
    end_date: datetime | None = None
    catch_up: bool = False
    max_active_runs: int | None = None
    enabled: bool = True
    variable_overrides: dict[str, Any] | None = None
    owners: list[str] | None = None
    labels: dict[str, str] | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str
    instance_id: str | None
    bundle_path: str | None


def create_app(
    bundle_path: Path,
    meta: LocalMetadata,
    scheduler: DeploymentScheduler | None = None,
) -> Any:
    """Create FastAPI app with optional embedded scheduler.

    Args:
        bundle_path: Path to the DAG bundle directory
        meta: Metadata store instance
        scheduler: Optional scheduler instance (for merged API+Scheduler)

    Returns:
        FastAPI application
    """
    # Lazy import to avoid dependency issues
    from fastapi import FastAPI, HTTPException

    app = FastAPI(
        title="Beacon API",
        description="Workflow orchestration API with embedded scheduler",
        version="0.1.0",
    )

    # Store references in app state
    app.state.scheduler = scheduler
    app.state.meta = meta
    app.state.bundle_path = bundle_path

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Health check endpoint."""
        return HealthResponse(
            status="ok",
            instance_id=scheduler.instance_id if scheduler else None,
            bundle_path=str(bundle_path) if bundle_path else None,
        )

    # =========================================================================
    # Triggers
    # =========================================================================

    @app.post("/triggers", response_model=TriggerResponse)
    async def create_trigger(req: TriggerRequest) -> TriggerResponse:
        """Create a manual trigger for a deployment.

        The trigger will be picked up by the scheduler on its next tick
        and executed as a DagRun.
        """
        dep = await meta.get_deployment(req.deployment_id)
        if not dep:
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {req.deployment_id} not found",
            )

        trigger_id = await meta.enqueue_trigger(
            req.deployment_id,
            req.variables,
        )
        return TriggerResponse(
            trigger_id=trigger_id,
            deployment_id=req.deployment_id,
            message=f"Trigger enqueued for {req.deployment_id}",
        )

    # =========================================================================
    # Deployments
    # =========================================================================

    @app.get("/deployments")
    async def list_deployments() -> dict[str, Any]:
        """List all deployments."""
        deps = await meta.list_deployments()
        return {"deployments": deps, "count": len(deps)}

    @app.get("/deployments/{deployment_id}")
    async def get_deployment(deployment_id: str) -> dict[str, Any]:
        """Get a deployment by ID."""
        dep = await meta.get_deployment(deployment_id)
        if not dep:
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {deployment_id} not found",
            )
        return dep

    @app.post("/deployments")
    async def create_deployment(req: DeploymentCreate) -> dict[str, Any]:
        """Create or update a deployment."""
        dep_dict = req.model_dump(exclude_none=True)
        await meta.upsert_deployment(dep_dict)
        return {"id": req.id, "message": f"Deployment {req.id} created/updated"}

    @app.delete("/deployments/{deployment_id}")
    async def delete_deployment(deployment_id: str) -> dict[str, Any]:
        """Delete a deployment."""
        deleted = await meta.delete_deployment(deployment_id)
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {deployment_id} not found",
            )
        return {"id": deployment_id, "message": "Deployment deleted"}

    @app.patch("/deployments/{deployment_id}/enable")
    async def enable_deployment(deployment_id: str) -> dict[str, Any]:
        """Enable a deployment."""
        dep = await meta.get_deployment(deployment_id)
        if not dep:
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {deployment_id} not found",
            )
        dep["enabled"] = True
        await meta.upsert_deployment(dep)
        return {"id": deployment_id, "enabled": True}

    @app.patch("/deployments/{deployment_id}/disable")
    async def disable_deployment(deployment_id: str) -> dict[str, Any]:
        """Disable a deployment."""
        dep = await meta.get_deployment(deployment_id)
        if not dep:
            raise HTTPException(
                status_code=404,
                detail=f"Deployment {deployment_id} not found",
            )
        dep["enabled"] = False
        await meta.upsert_deployment(dep)
        return {"id": deployment_id, "enabled": False}

    # =========================================================================
    # Runs
    # =========================================================================

    @app.get("/runs")
    async def list_runs(
        dag_id: str | None = None, limit: int = 50
    ) -> dict[str, Any]:
        """List recent DAG runs."""
        runs = await meta.list_dag_runs(dag_id=dag_id, limit=limit)
        return {"runs": runs, "count": len(runs)}

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str, dag_id: str) -> dict[str, Any]:
        """Get a specific run."""
        run = await meta.get_dag_run(run_id, dag_id)
        if not run:
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found",
            )
        return run

    @app.get("/runs/active")
    async def list_active_runs(dag_id: str | None = None) -> dict[str, Any]:
        """List active (non-terminal) runs."""
        runs = await meta.list_active_runs(dag_id=dag_id)
        return {"runs": runs, "count": len(runs)}

    return app
