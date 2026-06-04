"""``beacon logs`` — tail / dump per-attempt JSONL from the log store."""

import asyncio
import sys
import time
from datetime import datetime
from pathlib import Path

import click

from ...metadata import LocalMetadata
from ..settings import get


@click.command()
@click.argument("dag_id")
@click.argument("task_id")
@click.option(
    "--run",
    "run_id",
    default=None,
    help="DagRun id. Required unless --logical-date is given.",
)
@click.option(
    "--logical-date",
    default=None,
    help="ISO date (YYYY-MM-DD or full ISO). Looks up the matching run_id.",
)
@click.option(
    "--attempt",
    default=None,
    type=int,
    help="Attempt number. Defaults to the latest attempt found.",
)
@click.option(
    "-f",
    "--follow",
    is_flag=True,
    default=False,
    help="Stream new lines as they arrive.",
)
@click.option(
    "--log-dir",
    default=None,
    help="Defaults to $BEACON_LOG_DIR.",
)
@click.option(
    "--metadata-path",
    default=None,
    help="Defaults to $BEACON_METADATA_PATH (used for --logical-date lookup).",
)
def logs(
    dag_id: str,
    task_id: str,
    run_id: str | None,
    logical_date: str | None,
    attempt: int | None,
    follow: bool,
    log_dir: str | None,
    metadata_path: str | None,
) -> None:
    """Show JSONL logs for DAG_ID / TASK_ID."""
    if not run_id and not logical_date:
        click.echo("Pass either --run RUN_ID or --logical-date DATE.", err=True)
        sys.exit(2)

    if run_id is None:
        run_id = _resolve_run_id(
            dag_id,
            logical_date,  # type: ignore[arg-type]
            metadata_path or get("BEACON_METADATA_PATH"),
        )
        if run_id is None:
            click.echo(
                f"No run for dag={dag_id} logical-date={logical_date}",
                err=True,
            )
            sys.exit(1)
        click.echo(f"# resolved run_id={run_id}", err=True)

    base = Path(log_dir or get("BEACON_LOG_DIR"))
    task_dir = base / dag_id / run_id / task_id
    if not task_dir.exists():
        click.echo(f"No log dir: {task_dir}", err=True)
        sys.exit(1)

    if attempt is None:
        attempt = _latest_attempt(task_dir)
        if attempt is None:
            click.echo(f"No attempts found in {task_dir}", err=True)
            sys.exit(1)

    path = task_dir / f"attempt_{attempt}.jsonl"
    if not path.exists():
        click.echo(f"No such file: {path}", err=True)
        sys.exit(1)

    if follow:
        _tail_follow(path)
    else:
        click.echo(path.read_text(), nl=False)


# --- helpers --------------------------------------------------------------


def _latest_attempt(task_dir: Path) -> int | None:
    nums: list[int] = []
    for f in task_dir.glob("attempt_*.jsonl"):
        stem = f.stem  # attempt_N
        try:
            nums.append(int(stem.split("_", 1)[1]))
        except IndexError, ValueError:
            continue
    return max(nums) if nums else None


def _resolve_run_id(
    dag_id: str, logical_date_str: str, metadata_path: str
) -> str | None:
    target = _parse_loose_date(logical_date_str)
    if target is None:
        click.echo(f"Bad --logical-date: {logical_date_str!r}", err=True)
        sys.exit(2)
    meta = LocalMetadata(metadata_path)
    runs = asyncio.run(meta.list_dag_runs(dag_id=dag_id, limit=1000))
    for r in runs:
        ld = r.get("logical_date")
        if not ld:
            continue
        parsed = _parse_loose_date(str(ld))
        if parsed is None:
            continue
        if _same_day_or_exact(parsed, target):
            return r["run_id"]
    return None


def _parse_loose_date(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _same_day_or_exact(a: datetime, b: datetime) -> bool:
    """Match an exact ISO date OR same calendar date when only YYYY-MM-DD given."""
    if a.hour == b.hour == 0 and a.minute == b.minute == 0:
        return a.date() == b.date()
    if b.hour == 0 and b.minute == 0 and b.second == 0:
        return a.date() == b.date()
    return a == b


def _tail_follow(path: Path) -> None:
    """Print existing content, then poll for appends. Ctrl-C to stop."""
    with path.open("r") as f:
        click.echo(f.read(), nl=False)
        try:
            while True:
                line = f.readline()
                if line:
                    click.echo(line, nl=False)
                else:
                    time.sleep(0.2)
        except KeyboardInterrupt:
            return
