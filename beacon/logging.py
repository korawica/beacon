"""Beacon unified logging pipeline.

One pipeline handles both framework logs (``beacon.worker``,
``beacon.runner``, ``beacon.executor`` ...) and task logs
(``ctx.logger.info(...)`` inside plugins / user Python).

Records are tagged with ``dag_id`` / ``run_id`` / ``task_id`` /
``attempt`` via a ``ContextVar`` pushed by the executor, then handed to a
background **batching dispatcher** that flushes to a configured
``LogSink`` (local file, in-memory, remote).

Design choices:

- Sinks are **sync**. The dispatcher runs on a dedicated ``threading.Thread``
  so logging never blocks the asyncio loop. An async sink wraps its
  coroutine with ``asyncio.run`` internally.
- Records are **JSON Lines** — one JSON object per line, ES-friendly.
- Dispatcher flushes on EITHER ``batch_size`` records OR every
  ``flush_interval_ms`` (default 500 ms). Worst-case loss on hard crash
  is one flush interval.
- ``LocalFileSink`` keeps an LRU of open file handles (default 256) so
  thousands of concurrent tasks don't exhaust file descriptors.
- Configured via env vars (``BEACON_LOG_*``) with a programmatic override
  ``configure_logging(...)``. Calling configure twice is safe; the prior
  dispatcher is drained and replaced.

Backends ready out-of-the-box:

- ``file``  -> ``LocalFileSink`` (JSONL, sharded by dag/run/task/attempt)
- ``memory`` -> ``InMemorySink`` (tests)

Remote backends (``gcs``, ``elasticsearch``) raise a clear error pointing
at the optional extra to install — the dispatcher / handler / record
format are the same so a remote sink only needs to implement
``write_batch``.
"""

import json
import logging
import os
import queue
import sys
import threading
import time
import traceback
from collections import OrderedDict
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import IO, Any, Protocol
from collections.abc import Iterator

__all__ = (
    "BatchingDispatcher",
    "BeaconLogHandler",
    "InMemorySink",
    "LocalFileSink",
    "LogRecord",
    "LogSink",
    "capture_stdout_stderr",
    "configure_logging",
    "get_dispatcher",
    "should_capture_stdout",
    "shutdown_logging",
    "task_log_context",
)


# --------------------------------------------------------------------------
# Record + ContextVar tagging
# --------------------------------------------------------------------------


@dataclass(slots=True)
class LogRecord:
    """A normalized log record.

    Serialized as one JSON object per line.
    """

    ts: float
    level: str
    msg: str
    logger: str
    source: str  # "task" | "framework"
    dag_id: str | None = None
    run_id: str | None = None
    task_id: str | None = None
    attempt: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)
    exc_info: str | None = None


_TASK_TAGS: ContextVar[dict[str, Any] | None] = ContextVar(
    "beacon_log_task_tags", default=None
)


@contextmanager
def task_log_context(
    dag_id: str, run_id: str, task_id: str, attempt: int
) -> Iterator[None]:
    """Push task tags so every log emitted in this scope is attributed."""
    token = _TASK_TAGS.set(
        {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "attempt": attempt,
        }
    )
    try:
        yield
    finally:
        _TASK_TAGS.reset(token)


# --------------------------------------------------------------------------
# Sink protocol + built-ins
# --------------------------------------------------------------------------


class LogSink(Protocol):
    """A sink consumes batches of records. Called from a background thread."""

    def write_batch(self, records: list[LogRecord]) -> None: ...

    def close(self) -> None: ...


class InMemorySink:
    """Collects records in memory. For tests and debugging."""

    def __init__(self) -> None:
        self.records: list[LogRecord] = []
        self._lock = threading.Lock()

    def write_batch(self, records: list[LogRecord]) -> None:
        with self._lock:
            self.records.extend(records)

    def close(self) -> None:
        pass

    def clear(self) -> None:
        with self._lock:
            self.records.clear()


class LocalFileSink:
    """Append JSONL to per-task files; framework records to a single file.

    Layout::

        {base}/{dag_id}/{run_id}/{task_id}/attempt_{N}.jsonl   # task logs
        {base}/framework.jsonl                                  # everything else

    Maintains an LRU of open file handles (default 256) so we don't fd-leak
    under 1000+ concurrent tasks.
    """

    def __init__(
        self, base_dir: str | Path = "./logs", max_open_handles: int = 256
    ) -> None:
        self.base = Path(base_dir)
        self.base.mkdir(parents=True, exist_ok=True)
        self.max_open = max_open_handles
        self._handles: OrderedDict[Path, IO[str]] = OrderedDict()

    def _path_for(self, rec: LogRecord) -> Path:
        if rec.source == "task" and rec.dag_id and rec.run_id and rec.task_id:
            return (
                self.base
                / rec.dag_id
                / rec.run_id
                / rec.task_id
                / f"attempt_{rec.attempt or 1}.jsonl"
            )
        return self.base / "framework.jsonl"

    def _handle(self, path: Path) -> IO[str]:
        f = self._handles.get(path)
        if f is not None:
            self._handles.move_to_end(path)
            return f
        path.parent.mkdir(parents=True, exist_ok=True)
        f = open(path, "a", encoding="utf-8")  # noqa: SIM115
        self._handles[path] = f
        # Evict LRU
        while len(self._handles) > self.max_open:
            _, old = self._handles.popitem(last=False)
            try:
                old.close()
            except OSError:
                pass
        return f

    def write_batch(self, records: list[LogRecord]) -> None:
        per_file: dict[Path, list[str]] = {}
        for rec in records:
            per_file.setdefault(self._path_for(rec), []).append(
                json.dumps(asdict(rec), default=str) + "\n"
            )
        for path, lines in per_file.items():
            f = self._handle(path)
            f.writelines(lines)
            f.flush()  # 1-flush-per-batch, not per-record

    def close(self) -> None:
        for f in self._handles.values():
            try:
                f.close()
            except OSError:
                pass
        self._handles.clear()


def _missing_extra(extra: str, pkg: str) -> LogSink:
    raise RuntimeError(
        f"Backend requires optional extra: pip install 'beacon[{extra}]' "
        f"(missing package: {pkg})"
    )


# --------------------------------------------------------------------------
# Batching dispatcher
# --------------------------------------------------------------------------


class BatchingDispatcher:
    """Thread-backed batching dispatcher.

    Flushes when ``len(buffer) >= batch_size`` OR every ``flush_interval``
    seconds, whichever comes first.
    """

    _STOP: object = object()

    def __init__(
        self,
        sink: LogSink,
        batch_size: int = 100,
        flush_interval: float = 0.5,
        max_queue: int = 100_000,
    ) -> None:
        self.sink = sink
        self.batch_size = max(1, batch_size)
        self.flush_interval = max(0.01, flush_interval)
        self._q: queue.Queue[Any] = queue.Queue(maxsize=max_queue)
        self._dropped = 0
        self._thread = threading.Thread(
            target=self._run, name="beacon-log-dispatcher", daemon=True
        )
        self._thread.start()

    def submit(self, record: LogRecord) -> None:
        try:
            self._q.put_nowait(record)
        except queue.Full:
            # Backpressure: drop and account. Visible via ``dropped``.
            self._dropped += 1

    @property
    def dropped(self) -> int:
        return self._dropped

    def _run(self) -> None:
        buf: list[LogRecord] = []
        deadline = time.monotonic() + self.flush_interval
        while True:
            timeout = max(0.0, deadline - time.monotonic())
            try:
                item = self._q.get(timeout=timeout) if timeout > 0 else None
            except queue.Empty:
                item = None

            if item is self._STOP:
                # Drain remaining items
                while True:
                    try:
                        more = self._q.get_nowait()
                    except queue.Empty:
                        break
                    if more is self._STOP:
                        continue
                    buf.append(more)
                self._flush(buf)
                return

            if isinstance(item, LogRecord):
                buf.append(item)

            now = time.monotonic()
            if len(buf) >= self.batch_size or now >= deadline:
                self._flush(buf)
                buf = []
                deadline = now + self.flush_interval

    def _flush(self, buf: list[LogRecord]) -> None:
        if not buf:
            return
        try:
            self.sink.write_batch(buf)
        except Exception as exc:  # noqa: BLE001
            sys.stderr.write(
                f"[beacon.logging] sink {type(self.sink).__name__} "
                f"failed: {exc}; {len(buf)} records lost\n"
            )

    def shutdown(self, timeout: float = 5.0) -> None:
        self._q.put(self._STOP)
        self._thread.join(timeout=timeout)
        try:
            self.sink.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------
# logging.Handler bridge
# --------------------------------------------------------------------------


_STD_LOGREC_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "message",
        "asctime",
        "taskName",
    }
)


class BeaconLogHandler(logging.Handler):
    """Adapter from ``logging.LogRecord`` -> beacon ``LogRecord`` -> dispatcher.

    Reads ContextVar tags so framework code that uses
    ``logging.getLogger("beacon.worker")`` gets tagged automatically when
    the executor pushes ``task_log_context``.
    """

    def __init__(self, dispatcher: BatchingDispatcher) -> None:
        super().__init__()
        self.dispatcher = dispatcher

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        try:
            tags = _TASK_TAGS.get()
            exc_text: str | None = None
            if record.exc_info:
                exc_text = "".join(traceback.format_exception(*record.exc_info))
            extra = {
                k: v
                for k, v in record.__dict__.items()
                if k not in _STD_LOGREC_ATTRS and not k.startswith("_")
            }
            rec = LogRecord(
                ts=record.created,
                level=record.levelname,
                msg=record.getMessage(),
                logger=record.name,
                source="task" if tags else "framework",
                dag_id=(tags or {}).get("dag_id"),
                run_id=(tags or {}).get("run_id"),
                task_id=(tags or {}).get("task_id"),
                attempt=(tags or {}).get("attempt"),
                extra=extra,
                exc_info=exc_text,
            )
            self.dispatcher.submit(rec)
        except Exception:  # noqa: BLE001
            self.handleError(record)


# --------------------------------------------------------------------------
# stdout / stderr capture (opt-in)
# --------------------------------------------------------------------------


class _LineBufferedLogWriter:
    """File-like writer that calls ``log_fn(line)`` per complete line."""

    def __init__(self, log_fn: Any) -> None:
        self._log = log_fn
        self._buf = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        self._buf += s
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line:
                self._log(line)
        return len(s)

    def flush(self) -> None:
        if self._buf:
            self._log(self._buf)
            self._buf = ""

    def isatty(self) -> bool:  # pragma: no cover
        return False


@contextmanager
def capture_stdout_stderr(logger: logging.Logger) -> Iterator[None]:
    """Redirect ``sys.stdout`` / ``sys.stderr`` line-by-line into ``logger``.

    NOTE: ``sys.stdout`` is process-global. If multiple tasks execute
    concurrently in the same process, their ``print()`` output will mix.
    Use this only for serial execution or remote executors where each task
    has its own process. Disabled by default.
    """
    out = _LineBufferedLogWriter(logger.info)
    err = _LineBufferedLogWriter(logger.error)
    with redirect_stdout(out), redirect_stderr(err):
        try:
            yield
        finally:
            out.flush()
            err.flush()


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


_dispatcher: BatchingDispatcher | None = None
_handler: BeaconLogHandler | None = None
_capture_stdout: bool = False


def _make_sink(backend: str, base_dir: str | Path) -> LogSink:
    backend = backend.lower()
    if backend == "memory":
        return InMemorySink()
    if backend == "file":
        return LocalFileSink(base_dir)
    if backend == "gcs":
        try:
            from .providers.standard.logging.gcs_sink import (  # type: ignore
                GcsLogSink,
            )
        except ImportError:
            return _missing_extra("gcs", "google-cloud-storage")
        return GcsLogSink()  # type: ignore[call-arg]
    if backend == "elasticsearch":
        try:
            from .providers.standard.logging.es_sink import (  # type: ignore
                ElasticsearchLogSink,
            )
        except ImportError:
            return _missing_extra("elasticsearch", "elasticsearch")
        return ElasticsearchLogSink()  # type: ignore[call-arg]
    raise ValueError(f"Unknown log backend: {backend!r}")


def configure_logging(
    backend: str | None = None,
    base_dir: str | Path | None = None,
    batch_size: int | None = None,
    flush_interval_ms: int | None = None,
    level: int | str | None = None,
    sink: LogSink | None = None,
    capture_stdout: bool | None = None,
    attach_to: str = "beacon",
) -> BatchingDispatcher:
    """Configure the beacon logging pipeline.

    Idempotent — calling twice drains the old dispatcher and installs a
    new one. Reads env vars unless overridden by arguments:

    - ``BEACON_LOG_BACKEND``      (default: ``file``)
    - ``BEACON_LOG_DIR``          (default: ``./logs``)
    - ``BEACON_LOG_BATCH_SIZE``   (default: ``100``)
    - ``BEACON_LOG_FLUSH_MS``     (default: ``500``)
    - ``BEACON_LOG_LEVEL``        (default: ``INFO``)
    - ``BEACON_LOG_STDOUT_CAPTURE`` (default: ``0``)
    """
    global _dispatcher, _handler, _capture_stdout

    backend = backend or os.getenv("BEACON_LOG_BACKEND", "file")
    base_dir = base_dir or os.getenv("BEACON_LOG_DIR", "./logs")
    batch_size = batch_size or int(os.getenv("BEACON_LOG_BATCH_SIZE", "100"))
    flush_interval_ms = flush_interval_ms or int(
        os.getenv("BEACON_LOG_FLUSH_MS", "500")
    )
    level = (
        level if level is not None else os.getenv("BEACON_LOG_LEVEL", "INFO")
    )
    if capture_stdout is None:
        capture_stdout = os.getenv(
            "BEACON_LOG_STDOUT_CAPTURE", "0"
        ).lower() not in ("0", "false", "no", "")
    _capture_stdout = bool(capture_stdout)

    chosen_sink = sink if sink is not None else _make_sink(backend, base_dir)

    # Replace existing dispatcher (drains + closes)
    if _dispatcher is not None:
        _dispatcher.shutdown()

    _dispatcher = BatchingDispatcher(
        sink=chosen_sink,
        batch_size=batch_size,
        flush_interval=flush_interval_ms / 1000.0,
    )

    root = logging.getLogger(attach_to)
    # Remove any existing BeaconLogHandler so we don't double-emit
    for h in list(root.handlers):
        if isinstance(h, BeaconLogHandler):
            root.removeHandler(h)

    _handler = BeaconLogHandler(_dispatcher)
    lvl = level if isinstance(level, int) else logging.getLevelName(str(level))
    _handler.setLevel(lvl)
    root.addHandler(_handler)
    if root.level == logging.NOTSET or root.level > lvl:
        root.setLevel(lvl)

    return _dispatcher


def shutdown_logging() -> None:
    """Drain and close the global dispatcher. Safe to call multiple times."""
    global _dispatcher, _handler
    if _dispatcher is not None:
        _dispatcher.shutdown()
        _dispatcher = None
    if _handler is not None:
        for name in ("beacon",):
            logging.getLogger(name).removeHandler(_handler)
        _handler = None


def get_dispatcher() -> BatchingDispatcher | None:
    """Return the currently-configured dispatcher (or ``None``)."""
    return _dispatcher


def should_capture_stdout() -> bool:
    """Whether stdout/stderr capture is enabled (only true if explicitly configured)."""
    return _capture_stdout
