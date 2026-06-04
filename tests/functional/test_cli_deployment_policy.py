"""Functional tests for the deploy-flow policy layer:

* ``beacon deploy --var`` stores overrides + marks deployment pinned.
* ``beacon sync`` auto-rolls non-pinned deployments and leaves pinned
  ones stale.
* ``beacon list deployments --bundle`` shows ``[pinned]`` / ``[stale]``.
* ``beacon deployment diff --bundle`` previews resolved variables.
* ``beacon deployment sync`` accepts the new ``dag_version``.

Exercises the round-trip without subprocess (CliRunner; matches the
existing convention in ``test_cli_commands_v2.py``).
"""

import asyncio
import textwrap
from pathlib import Path

import pytest
from click.testing import CliRunner

from beacon.cli import cli
from beacon.metadata import JsonMetadata


DAG = """
from beacon import Dag, Task
dag = Dag(id="hello", actions=[Task(id="t", uses="empty")])
"""


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def _build_bundle(tmp_path: Path) -> Path:
    bundle = tmp_path / "bundle"
    dag_dir = bundle / "dags" / "g" / "hello"
    dag_dir.mkdir(parents=True)
    (dag_dir / "dag.py").write_text(textwrap.dedent(DAG).lstrip())
    (bundle / "dags" / "global_variables.yml").write_text("foo: bundle\n")
    (bundle / "dags" / "g" / "global_variables.yml").write_text("bar: group\n")
    (dag_dir / "variables.yml").write_text("baz: dag\n")
    return bundle


def _deploy(
    runner: CliRunner,
    *,
    meta: Path,
    did: str,
    extra: list[str] | None = None,
) -> None:
    args = [
        "deploy",
        "--id",
        did,
        "--dag-id",
        "hello",
        "--metadata-path",
        str(meta),
    ]
    if extra:
        args.extend(extra)
    res = runner.invoke(cli, args)
    assert res.exit_code == 0, res.output


# --- pinning & auto-roll on sync -----------------------------------------


def test_deploy_with_var_pins_and_sync_leaves_it_stale(
    runner: CliRunner, tmp_path: Path
) -> None:
    bundle = _build_bundle(tmp_path)
    meta = tmp_path / "meta"

    # 1. Two deployments — one pinned, one not.
    _deploy(runner, meta=meta, did="plain")
    _deploy(runner, meta=meta, did="pinned", extra=["--var", "alert=/oncall"])

    # 2. First sync — both should get rolled to current bundle version
    #    (pinned starts with no dag_version, so initial sync still sets it).
    res = runner.invoke(
        cli, ["sync", str(bundle), "--metadata-path", str(meta)]
    )
    assert res.exit_code == 0, res.output

    deps = {
        d["id"]: d for d in asyncio.run(JsonMetadata(meta).list_deployments())
    }
    v1 = deps["plain"]["dag_version"]
    assert deps["pinned"]["dag_version"] == v1
    assert deps["pinned"]["variable_overrides"] == {"alert": "/oncall"}

    # 3. Mutate the bundle → new dag_version.
    (bundle / "dags" / "g" / "hello" / "dag.py").write_text(
        textwrap.dedent(DAG).lstrip() + "# bump\n"
    )

    res = runner.invoke(
        cli, ["sync", str(bundle), "--metadata-path", str(meta)]
    )
    assert res.exit_code == 0, res.output
    assert "pinned" in res.output
    assert "rolled" in res.output

    deps = {
        d["id"]: d for d in asyncio.run(JsonMetadata(meta).list_deployments())
    }
    v2 = deps["plain"]["dag_version"]
    assert v2 != v1, "non-pinned should auto-roll"
    assert deps["pinned"]["dag_version"] == v1, (
        "pinned must stay on old version"
    )


# --- list deployments shows flags ----------------------------------------


def test_list_deployments_shows_pinned_and_stale_flags(
    runner: CliRunner, tmp_path: Path
) -> None:
    bundle = _build_bundle(tmp_path)
    meta = tmp_path / "meta"
    _deploy(runner, meta=meta, did="plain")
    _deploy(runner, meta=meta, did="pinned", extra=["--var", "k=v"])
    runner.invoke(cli, ["sync", str(bundle), "--metadata-path", str(meta)])

    # Mutate bundle so the pinned one becomes stale.
    (bundle / "dags" / "g" / "hello" / "dag.py").write_text(
        textwrap.dedent(DAG).lstrip() + "# bump\n"
    )
    runner.invoke(cli, ["sync", str(bundle), "--metadata-path", str(meta)])

    res = runner.invoke(
        cli,
        [
            "list",
            "deployments",
            "--metadata-path",
            str(meta),
            "--bundle",
            str(bundle),
        ],
    )
    assert res.exit_code == 0, res.output
    assert "[pinned" in res.output  # one of the rows
    assert "stale" in res.output


# --- deployment diff / sync ----------------------------------------------


def test_deployment_diff_and_sync_round_trip(
    runner: CliRunner, tmp_path: Path
) -> None:
    bundle = _build_bundle(tmp_path)
    meta = tmp_path / "meta"
    _deploy(runner, meta=meta, did="pinned", extra=["--var", "baz=override"])
    runner.invoke(cli, ["sync", str(bundle), "--metadata-path", str(meta)])

    # Stamp v1, then bump bundle.
    deps = asyncio.run(JsonMetadata(meta).list_deployments())
    v1 = deps[0]["dag_version"]
    (bundle / "dags" / "g" / "hello" / "dag.py").write_text(
        textwrap.dedent(DAG).lstrip() + "# bump\n"
    )
    runner.invoke(cli, ["sync", str(bundle), "--metadata-path", str(meta)])

    # diff shows resolved chain with the override marked.
    res = runner.invoke(
        cli,
        [
            "deployment",
            "diff",
            "pinned",
            "--bundle",
            str(bundle),
            "--metadata-path",
            str(meta),
        ],
    )
    assert res.exit_code == 0, res.output
    assert "stale" in res.output
    assert "baz" in res.output
    assert "override" in res.output  # the --var value, not "dag"
    assert "* baz" in res.output  # marker for deployment override

    # sync accepts the new version.
    res = runner.invoke(
        cli,
        [
            "deployment",
            "sync",
            "pinned",
            "--bundle",
            str(bundle),
            "--metadata-path",
            str(meta),
        ],
    )
    assert res.exit_code == 0, res.output

    deps = asyncio.run(JsonMetadata(meta).list_deployments())
    v2 = deps[0]["dag_version"]
    assert v2 != v1
    assert deps[0]["variable_overrides"] == {"baz": "override"}


def test_deployment_sync_requires_id_or_all(
    runner: CliRunner, tmp_path: Path
) -> None:
    bundle = _build_bundle(tmp_path)
    res = runner.invoke(
        cli,
        ["deployment", "sync", "--bundle", str(bundle)],
    )
    assert res.exit_code == 2, res.output
    assert "DEPLOYMENT_ID" in res.output or "--all" in res.output


# --- sync warns on multi-DAG folders -------------------------------------


def test_sync_warns_on_multi_dag_folder(
    runner: CliRunner, tmp_path: Path
) -> None:
    bundle = _build_bundle(tmp_path)
    # Drop a second DAG file in the same folder (policy violation).
    second = bundle / "dags" / "g" / "hello" / "other.py"
    second.write_text(
        textwrap.dedent(
            """
            from beacon import Dag, Task
            dag2 = Dag(id="other", actions=[Task(id="t", uses="empty")])
            """
        ).lstrip()
    )
    res = runner.invoke(cli, ["sync", str(bundle)])
    assert res.exit_code == 0, res.output  # warn, don't fail
    assert "WARNING" in res.output
    assert "multiple DAG files" in res.output
