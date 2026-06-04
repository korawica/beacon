"""Test bundle plugin discovery — simulates production GitBundle deployment.

Simulates this repo structure:
    my-workflow-repo/
    ├── dags/
    │   └── etl.yml
    └── plugins/
        └── custom_gcs.py   (custom plugin: uses="gcs-extract")
"""

import asyncio
from datetime import datetime

import pytest

from beacon.core import (
    PLUGINS_REGISTRY,
    LocalExecutor,
    TaskContext,
)
from beacon.core.task_context import AttemptStatus
from beacon.core.bundle import LocalBundle


@pytest.fixture
def workflow_repo(tmp_path):
    """Create a fake workflow repo with dags/ and plugins/ directories."""
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    plugins_dir = tmp_path / "plugins"
    plugins_dir.mkdir()

    # Custom plugin — same as what a team would write
    (plugins_dir / "custom_gcs.py").write_text("""\
from typing import ClassVar, Any
from beacon.core import BasePlugin, Context


class GcsExtractPlugin(BasePlugin):
    plugin_name: ClassVar[str] = "gcs-extract"

    bucket: str
    prefix: str

    async def execute(self, context: Context) -> dict[str, Any]:
        # Simulate GCS extraction
        return {"bucket": self.bucket, "prefix": self.prefix, "files": 3}
""")

    # DAG definition
    (dags_dir / "etl.yml").write_text("""\
id: etl-pipeline
owners: [de-team]
tasks:
  - id: extract
    type: task
    uses: gcs-extract
    inputs:
      bucket: my-data-lake
      prefix: raw/2026-06-03
""")

    # A py file for the py plugin
    (dags_dir / "transform.py").write_text("""\
from beacon import load_context

def main(source: str):
    ctx = load_context()
    ctx.logger.info("Transforming from %s", source)
    return {"transformed": True, "source": source}
""")

    return tmp_path


def test_bundle_discovers_plugins(workflow_repo):
    """Bundle.load_plugins() should register custom plugins."""
    bundle = LocalBundle(name="etl-repo", path=workflow_repo)

    # Before loading — custom plugin not in registry
    assert "gcs-extract" not in PLUGINS_REGISTRY

    # Load plugins from bundle
    registered = bundle.load_plugins()

    # After loading — custom plugin registered
    assert "gcs-extract" in registered
    assert "gcs-extract" in PLUGINS_REGISTRY


def test_bundle_discovers_dags(workflow_repo):
    """Bundle.discover_dags() finds all DAG files."""
    bundle = LocalBundle(name="etl-repo", path=workflow_repo)
    dags = bundle.discover_dags()

    names = [d.name for d in dags]
    assert "etl.yml" in names
    assert "transform.py" in names


def test_bundle_version(workflow_repo):
    """Bundle version is a content hash."""
    bundle = LocalBundle(name="etl-repo", path=workflow_repo)
    v1 = bundle.version
    assert len(v1) == 12  # sha256[:12]

    # Modify a file → version changes
    (workflow_repo / "dags" / "new.yml").write_text("id: new")
    bundle2 = LocalBundle(name="etl-repo", path=workflow_repo)
    assert bundle2.version != v1


def test_custom_plugin_executes_via_executor(workflow_repo):
    """Custom plugin loaded from bundle runs through LocalExecutor."""
    bundle = LocalBundle(name="etl-repo", path=workflow_repo)
    bundle.load_plugins()

    task_ctx = TaskContext(
        run_id="run-001",
        dag_id="etl-pipeline",
        task_id="extract",
        dag_version=bundle.version,
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={},
        inputs={"bucket": "my-data-lake", "prefix": "raw/2026-06-03"},
        plugin_name="gcs-extract",
    )

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.SUCCESS
    assert result.outputs == {
        "bucket": "my-data-lake",
        "prefix": "raw/2026-06-03",
        "files": 3,
    }


def test_py_plugin_with_bundle_dags(workflow_repo):
    """py plugin runs a file from the dags/ directory."""
    bundle = LocalBundle(name="etl-repo", path=workflow_repo)
    bundle.load_plugins()

    py_file = str(workflow_repo / "dags" / "transform.py")
    task_ctx = TaskContext(
        run_id="run-002",
        dag_id="etl-pipeline",
        task_id="transform",
        dag_version=bundle.version,
        run_date=datetime(2026, 6, 3),
        logical_date=datetime(2026, 6, 3),
        data_interval_start=datetime(2026, 6, 3),
        data_interval_end=datetime(2026, 6, 4),
        params={"source": "gcs"},
        inputs={
            "py_file": py_file,
            "py_function": "main",
            "params": {"source": "gcs"},
        },
        plugin_name="py",
    )

    # Need to ensure py plugin is registered
    from beacon.providers.standard.plugins.task.python import PythonPlugin  # noqa

    executor = LocalExecutor()
    result = asyncio.run(executor.run_task(task_ctx))

    assert result.last_attempt.state == AttemptStatus.SUCCESS
    assert result.outputs == {"transformed": True, "source": "gcs"}
