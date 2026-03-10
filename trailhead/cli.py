"""trailhead-cli — ship, import, and manage logs on AWS CloudWatch."""
from __future__ import annotations

import json
import os
import re
import select
import signal
import sqlite3
import stat
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import boto3
import typer
from botocore.exceptions import ClientError
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

console = Console()
app = typer.Typer(
    name="trailhead-cli",
    help=(
        "Ship logs to AWS CloudWatch.\n\n"
        "Typical workflow:\n\n"
        "  1. trailhead-cli create-group --owner mysite_access\n\n"
        "  2. trailhead-cli ship /var/log/nginx/access.log -o mysite_access --follow\n\n"
        "Or bulk-import an existing file:\n\n"
        "  trailhead-cli import-logs app.log --owner myapp\n\n"
        "Run any command with --help for full options."
    ),
    no_args_is_help=True,
    rich_markup_mode="rich",
)

# CloudWatch PutLogEvents limits
_MAX_EVENTS = 10_000
_MAX_BYTES = 1_048_576
_OVERHEAD = 26

# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------
_ISO_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:?\d{2})?)"
)


def _epoch_ms_now() -> int:
    return int(time.time() * 1000)


def _try_parse_ts(raw) -> int | None:
    if isinstance(raw, (int, float)):
        return int(raw * 1000) if raw < 1e12 else int(raw)
    if isinstance(raw, str):
        m = _ISO_RE.search(raw)
        if m:
            s = m.group(1).replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except ValueError:
                pass
    return None


# ---------------------------------------------------------------------------
# CloudWatch batched uploader (sync, using boto3)
# ---------------------------------------------------------------------------
class _CWUploader:
    def __init__(self, client, log_group: str, log_stream: str):
        self._client = client
        self.log_group = log_group
        self.log_stream = log_stream
        self._buf: list[dict] = []
        self._buf_bytes = 0
        self.total = 0

    def add(self, ts_ms: int, message: str) -> None:
        ev_bytes = len(message.encode()) + _OVERHEAD
        if self._buf and (
            len(self._buf) >= _MAX_EVENTS
            or (self._buf_bytes + ev_bytes) > _MAX_BYTES
        ):
            self.flush()
        self._buf.append({"timestamp": ts_ms, "message": message})
        self._buf_bytes += ev_bytes

    def flush(self) -> int:
        if not self._buf:
            return 0
        events = sorted(self._buf, key=lambda e: e["timestamp"])
        n = len(events)
        self._client.put_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=events,
        )
        self.total += n
        self._buf.clear()
        self._buf_bytes = 0
        return n


def _make_uploader(
    region: str,
    log_group: str,
    stream_name: str | None,
    create_group: bool,
) -> _CWUploader:
    client = boto3.client("logs", region_name=region)

    if create_group:
        try:
            client.create_log_group(logGroupName=log_group)
            console.print(f"[green]Created log group:[/] {log_group}")
        except client.exceptions.ResourceAlreadyExistsException:
            pass
    else:
        try:
            resp = client.describe_log_groups(logGroupNamePrefix=log_group, limit=1)
            if not any(g["logGroupName"] == log_group for g in resp.get("logGroups", [])):
                console.print(
                    f"[red]Log group not found:[/] {log_group}\n"
                    f"  Create it first:  [bold]trailhead-cli create-group --owner <owner>[/]\n"
                    f"  Or add [bold]--create-group[/] to auto-create during import."
                )
                raise typer.Exit(1)
        except ClientError as exc:
            console.print(f"[red]AWS error:[/] {exc}")
            raise typer.Exit(1)

    if stream_name is None:
        ts = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        stream_name = f"{ts}/{uuid.uuid4().hex[:12]}"

    client.create_log_stream(logGroupName=log_group, logStreamName=stream_name)
    console.print(f"[dim]Log stream:[/] {log_group} / {stream_name}")
    return _CWUploader(client, log_group, stream_name)


# ---------------------------------------------------------------------------
# File readers  — yield (timestamp_ms | None, message)
# ---------------------------------------------------------------------------
def _read_text_lines(path: Path) -> Iterator[tuple[int | None, str]]:
    with open(path, errors="replace") as fh:
        for line in fh:
            line = line.rstrip("\n\r")
            if not line:
                continue
            ts = _try_parse_ts(line)
            yield ts, line


def _read_jsonl(path: Path) -> Iterator[tuple[int | None, str]]:
    with open(path, "rb") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                yield None, raw.decode(errors="replace")
                continue
            ts_raw = obj.pop("timestamp", None)
            ts = _try_parse_ts(ts_raw) if ts_raw is not None else None
            yield ts, json.dumps(obj, separators=(",", ":"))


def _read_sqlite(
    path: Path,
    table: str,
    query: str | None,
    ts_col: str,
    msg_col: str | None,
) -> Iterator[tuple[int | None, str]]:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    sql = query or f"SELECT * FROM [{table}]"
    for row in conn.execute(sql):
        d = dict(row)
        ts = _try_parse_ts(d.pop(ts_col, None))
        if msg_col and msg_col in d:
            message = str(d[msg_col])
        else:
            message = json.dumps(d, separators=(",", ":"), default=str)
        yield ts, message
    conn.close()


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
_IMPORT_EPILOG = (
    "[dim]Examples:\n\n"
    "  trailhead-cli import-logs app.log --owner myapp\n\n"
    "  trailhead-cli import-logs events.jsonl --owner billing -r us-west-2\n\n"
    "  trailhead-cli import-logs app.db -o backend -f sqlite --table events\n\n"
    "Format is auto-detected from extension (.log→text, .jsonl→jsonl, .db→sqlite).[/]"
)


@app.command(epilog=_IMPORT_EPILOG)
def import_logs(
    file: Path = typer.Argument(..., help="File to import (.log, .jsonl, .sqlite, .db)"),
    owner: str = typer.Option(
        ..., "--owner", "-o", help="Owner tag — log group is {prefix}/{owner}"
    ),
    region: str = typer.Option("us-east-1", "--region", "-r", help="AWS region"),
    log_group_prefix: str = typer.Option(
        "/trailhead", "--prefix", help="Log group prefix [dim]\\[default: /trailhead][/]",
        show_default=False,
    ),
    create_group: bool = typer.Option(
        False, "--create-group", help="Auto-create the log group if it doesn't exist"
    ),
    fmt: Optional[str] = typer.Option(
        None, "--format", "-f",
        help="Force file format: text, jsonl, sqlite",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Parse and validate only — don't upload"
    ),
    log_group_override: Optional[str] = typer.Option(
        None, "--log-group", help="Full log group name (overrides --prefix/--owner)",
        rich_help_panel="Advanced",
    ),
    stream_name: Optional[str] = typer.Option(
        None, "--stream", help="Custom log stream name (default: auto-generated)",
        rich_help_panel="Advanced",
    ),
    table: str = typer.Option(
        "logs", "--table", help="Table to read from",
        rich_help_panel="SQLite options",
    ),
    query: Optional[str] = typer.Option(
        None, "--query", help="Custom SQL query (overrides --table)",
        rich_help_panel="SQLite options",
    ),
    timestamp_col: str = typer.Option(
        "timestamp", "--ts-col", help="Column containing timestamps",
        rich_help_panel="SQLite options",
    ),
    message_col: Optional[str] = typer.Option(
        None, "--msg-col", help="Column containing the message (omit to serialize full row)",
        rich_help_panel="SQLite options",
    ),
) -> None:
    """Import a log file into CloudWatch Logs.

    Reads .log, .jsonl, or .sqlite/.db files and uploads them to the
    CloudWatch log group for the given owner. The log group must already
    exist — use [bold]create-group[/] first, or pass [bold]--create-group[/].
    """
    if not file.exists():
        console.print(f"[red]File not found:[/] {file}")
        raise typer.Exit(1)

    suffix = file.suffix.lower()
    detected = fmt or {
        ".jsonl": "jsonl",
        ".ndjson": "jsonl",
        ".json": "jsonl",
        ".sqlite": "sqlite",
        ".sqlite3": "sqlite",
        ".db": "sqlite",
    }.get(suffix, "text")

    log_group = log_group_override or f"{log_group_prefix.rstrip('/')}/{owner}"
    console.print(f"[bold]Importing[/] {file}  →  [cyan]{log_group}[/]  (format={detected})")

    if detected == "jsonl":
        reader = _read_jsonl(file)
    elif detected == "sqlite":
        reader = _read_sqlite(file, table, query, timestamp_col, message_col)
    else:
        reader = _read_text_lines(file)

    uploader: _CWUploader | None = None
    if not dry_run:
        uploader = _make_uploader(region, log_group, stream_name, create_group)

    fallback_ts = _epoch_ms_now()
    accepted = 0
    skipped = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing…", total=None)
        for ts, message in reader:
            ts = ts or fallback_ts
            if uploader:
                uploader.add(ts, message)
            accepted += 1
            progress.update(task, advance=1)

        if uploader:
            uploader.flush()

    console.print(
        f"\n[bold green]Done.[/]  accepted={accepted}  flushed={uploader.total if uploader else 0}"
    )
    if uploader:
        console.print(
            f"  log_group={uploader.log_group}  log_stream={uploader.log_stream}"
        )


_CREATE_EPILOG = (
    "[dim]Examples:\n\n"
    "  trailhead-cli create-group --owner myapp\n\n"
    "  trailhead-cli create-group --owner myapp --retention 30\n\n"
    "  trailhead-cli create-group --owner billing -r us-west-2[/]"
)


@app.command(epilog=_CREATE_EPILOG)
def create_group(
    owner: str = typer.Option(..., "--owner", "-o", help="Owner tag"),
    region: str = typer.Option("us-east-1", "--region", "-r", help="AWS region"),
    log_group_prefix: str = typer.Option(
        "/trailhead", "--prefix", help="Log group prefix [dim]\\[default: /trailhead][/]",
        show_default=False,
    ),
    retention_days: Optional[int] = typer.Option(
        None, "--retention", help="Log retention in days (omit for no expiry)"
    ),
) -> None:
    """Create a CloudWatch log group for an owner.

    The group is named {prefix}/{owner}, e.g. /trailhead/myapp.
    Run this before [bold]import-logs[/], or use [bold]import-logs --create-group[/].
    """
    name = f"{log_group_prefix.rstrip('/')}/{owner}"
    client = boto3.client("logs", region_name=region)
    try:
        client.create_log_group(logGroupName=name)
    except client.exceptions.ResourceAlreadyExistsException:
        console.print(f"[yellow]Already exists:[/] {name}")
        return
    if retention_days:
        client.put_retention_policy(logGroupName=name, retentionInDays=retention_days)
    console.print(f"[green]Created:[/] {name}")


# ---------------------------------------------------------------------------
# ship — live log shipper to the Trailhead API
# ---------------------------------------------------------------------------
class _FileTailer:
    """Tails a regular file or named pipe (FIFO), handling rotation and truncation.

    Regular files use Python buffered IO.  FIFOs use raw fd reads + select()
    to avoid the mismatch between Python's internal buffer and OS readability.
    """

    def __init__(self, path: Path, from_start: bool = False):
        self.path = path
        self._fh = None       # buffered file (regular files)
        self._fd: int = -1    # raw fd (FIFOs)
        self._ino: int = 0
        self._is_fifo: bool = False
        self._remainder: str = ""
        self.needs_reopen: bool = False
        st = os.stat(str(path))
        self._is_fifo = stat.S_ISFIFO(st.st_mode)
        self._open(from_start)

    @property
    def is_fifo(self) -> bool:
        return self._is_fifo

    def _open(self, from_start: bool = True) -> None:
        self._close_handles()
        self.needs_reopen = False
        if self._is_fifo:
            self._fd = os.open(str(self.path), os.O_RDONLY)
            self._remainder = ""
        else:
            self._fh = open(self.path, errors="replace")
            self._ino = os.fstat(self._fh.fileno()).st_ino
            if not from_start:
                self._fh.seek(0, 2)

    def _close_handles(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def reopen(self) -> None:
        """Reopen. Blocks on FIFOs until a writer connects."""
        self._open(from_start=True)

    def read_lines(self) -> list[str]:
        if self._is_fifo:
            return self._read_fifo()
        return self._read_file()

    def _read_fifo(self) -> list[str]:
        lines: list[str] = []
        while select.select([self._fd], [], [], 0)[0]:
            chunk = os.read(self._fd, 65536)
            if not chunk:
                self.needs_reopen = True
                break
            text = self._remainder + chunk.decode(errors="replace")
            parts = text.split("\n")
            self._remainder = parts[-1]
            for part in parts[:-1]:
                part = part.rstrip("\r")
                if part:
                    lines.append(part)
        return lines

    def _read_file(self) -> list[str]:
        lines: list[str] = []
        while True:
            line = self._fh.readline()
            if not line:
                break
            line = line.rstrip("\n\r")
            if line:
                lines.append(line)

        try:
            st = os.stat(self.path)
            if st.st_ino != self._ino:
                self._open(from_start=True)
                while True:
                    line = self._fh.readline()
                    if not line:
                        break
                    line = line.rstrip("\n\r")
                    if line:
                        lines.append(line)
        except FileNotFoundError:
            pass

        pos = self._fh.tell()
        size = os.fstat(self._fh.fileno()).st_size
        if pos > size:
            self._fh.seek(0)

        return lines

    def close(self) -> None:
        self._close_handles()


def _read_stdin_lines(timeout: float = 0.05) -> tuple[list[str], bool]:
    """Non-blocking read from stdin. Returns (lines, eof)."""
    lines: list[str] = []
    while select.select([sys.stdin], [], [], timeout)[0]:
        line = sys.stdin.readline()
        if not line:
            return lines, True
        line = line.rstrip("\n\r")
        if line:
            lines.append(line)
        timeout = 0
    return lines, False


def _line_to_ndjson(line: str, now_ms: int) -> str:
    try:
        obj = json.loads(line)
        if not isinstance(obj, dict):
            obj = {"message": line}
    except (json.JSONDecodeError, ValueError):
        obj = {"message": line}
    if "timestamp" not in obj:
        ts = _try_parse_ts(line)
        obj["timestamp"] = ts or now_ms
    return json.dumps(obj, separators=(",", ":"))


def _ship_batch(session, url: str, lines: list[str]) -> dict:
    now_ms = _epoch_ms_now()
    body = "\n".join(_line_to_ndjson(l, now_ms) for l in lines)
    resp = session.post(url, data=body.encode())
    resp.raise_for_status()
    return resp.json()


_SHIP_EPILOG = (
    "[dim]Examples:\n\n"
    "  trailhead-cli ship /var/log/nginx/access.pipe -o mysite_access --mkfifo\n\n"
    "  trailhead-cli ship /var/log/nginx/access.log -o mysite_access --follow\n\n"
    "  tail -f /var/log/app.log | trailhead-cli ship -o myapp\n\n"
    "  Set TRAILHEAD_API_URL and TRAILHEAD_API_KEY env vars to avoid passing them every time.[/]"
)


@app.command(epilog=_SHIP_EPILOG)
def ship(
    file: Optional[Path] = typer.Argument(
        None, help="Log file to ship (omit for stdin)"
    ),
    owner: str = typer.Option(
        ..., "--owner", "-o", help="Owner tag — log group is {prefix}/{owner}"
    ),
    api_url: str = typer.Option(
        ..., "--api-url", "-u", envvar="TRAILHEAD_API_URL",
        help="Trailhead API base URL [dim](or TRAILHEAD_API_URL env)[/]",
    ),
    api_key: str = typer.Option(
        ..., "--api-key", "-k", envvar="TRAILHEAD_API_KEY",
        help="API key [dim](or TRAILHEAD_API_KEY env)[/]",
    ),
    batch_size: int = typer.Option(
        500, "--batch-size", help="Max lines per HTTP request",
        rich_help_panel="Tuning",
    ),
    flush_interval: float = typer.Option(
        5.0, "--flush-interval", help="Max seconds between flushes",
        rich_help_panel="Tuning",
    ),
    mkfifo: bool = typer.Option(
        False, "--mkfifo", help="Create a named pipe at FILE, removed on exit (zero-disk mode)"
    ),
    follow: bool = typer.Option(
        False, "--follow", help="Tail the file continuously (ignored for stdin)"
    ),
    from_start: bool = typer.Option(
        False, "--from-start", help="Read from beginning of file (default: tail from end)"
    ),
) -> None:
    """Stream logs to the Trailhead API in real time.

    Reads from a log file, named pipe (FIFO), or stdin. Batches lines and
    POSTs them as NDJSON. JSON lines are forwarded as-is; plain text is
    wrapped as [bold]{\"message\": \"...\"}[/].

    For zero-disk streaming, pass [bold]--mkfifo[/] — the pipe is created
    automatically and removed on exit. Point your web server's access_log
    at the pipe path.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    url = f"{api_url.rstrip('/')}/ingest?owner={owner}"

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "x-api-key": api_key,
        "Content-Type": "application/x-ndjson",
    })

    is_stdin = file is None or str(file) == "-"
    created_fifo: Path | None = None

    if not is_stdin:
        if mkfifo:
            if file.exists():
                if not stat.S_ISFIFO(os.stat(str(file)).st_mode):
                    console.print(f"[red]Path exists and is not a FIFO:[/] {file}")
                    raise typer.Exit(1)
            else:
                os.mkfifo(str(file))
                created_fifo = file
                console.print(f"[green]Created FIFO:[/] {file}")
        elif not file.exists():
            console.print(f"[red]File not found:[/] {file}")
            raise typer.Exit(1)

    tailer: _FileTailer | None = None
    if not is_stdin:
        is_fifo = stat.S_ISFIFO(os.stat(str(file)).st_mode)
        if is_fifo:
            console.print(f"[bold]Shipping[/] FIFO {file} → [cyan]{url}[/]")
            console.print("[dim]  waiting for writer…[/]")
            follow = True
        else:
            mode = "tail --follow" if follow else "read"
            console.print(f"[bold]Shipping[/] {file} → [cyan]{url}[/]  ({mode})")
        tailer = _FileTailer(file, from_start=from_start)
    else:
        follow = True
        console.print(f"[bold]Shipping[/] stdin → [cyan]{url}[/]")

    buf: list[str] = []
    last_flush = time.monotonic()
    total_shipped = 0
    running = True

    def _stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        while running:
            if tailer:
                if tailer.needs_reopen:
                    if buf:
                        try:
                            result = _ship_batch(session, url, buf)
                            total_shipped += result.get("accepted", len(buf))
                            console.print(
                                f"  [dim]shipped {len(buf)} lines ({total_shipped} total)[/]"
                            )
                        except Exception as exc:
                            console.print(f"  [red]error:[/] {exc}")
                        buf.clear()
                        last_flush = time.monotonic()
                    console.print("[dim]  writer disconnected, waiting for reconnect…[/]")
                    tailer.reopen()
                    continue
                new_lines = tailer.read_lines()
            else:
                new_lines, eof = _read_stdin_lines(timeout=0.1)
                if eof and not new_lines:
                    buf.extend(new_lines)
                    break

            buf.extend(new_lines)

            now = time.monotonic()
            should_flush = buf and (
                len(buf) >= batch_size
                or (now - last_flush) >= flush_interval
            )

            if should_flush:
                try:
                    result = _ship_batch(session, url, buf)
                    n = result.get("accepted", len(buf))
                    total_shipped += n
                    console.print(
                        f"  [dim]shipped {n} lines ({total_shipped} total)[/]"
                    )
                except Exception as exc:
                    console.print(f"  [red]error:[/] {exc}")
                buf.clear()
                last_flush = now

            if not new_lines:
                if not follow:
                    break
                time.sleep(0.1)
    finally:
        if buf:
            try:
                result = _ship_batch(session, url, buf)
                total_shipped += result.get("accepted", len(buf))
            except Exception:
                console.print(f"  [red]failed to flush {len(buf)} remaining lines[/]")
        if tailer:
            tailer.close()
        session.close()
        if created_fifo:
            try:
                os.unlink(str(created_fifo))
                console.print(f"[dim]Removed FIFO: {created_fifo}[/]")
            except OSError:
                pass

    console.print(f"\n[bold green]Done.[/]  total shipped: {total_shipped}")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
