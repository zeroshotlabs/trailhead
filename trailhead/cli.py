"""trailhead-cli — bulk-import logs from files or SQLite into CloudWatch."""
from __future__ import annotations

import json
import re
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import boto3
import typer
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
    help="Import logs into AWS CloudWatch via Trailhead conventions.",
    no_args_is_help=True,
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
@app.command()
def import_logs(
    file: Path = typer.Argument(..., help="Path to .log, .jsonl, .sqlite, or .db file"),
    owner: str = typer.Option(..., "--owner", "-o", help="Owner tag (maps to log group)"),
    region: str = typer.Option("us-east-1", "--region", "-r", help="AWS region"),
    log_group_prefix: str = typer.Option(
        "/trailhead", "--prefix", help="CloudWatch log group prefix"
    ),
    log_group_override: Optional[str] = typer.Option(
        None, "--log-group", help="Full log group name (overrides prefix/owner)"
    ),
    stream_name: Optional[str] = typer.Option(
        None, "--stream", help="Custom log stream name"
    ),
    create_group: bool = typer.Option(
        False, "--create-group", help="Auto-create the log group if missing"
    ),
    table: str = typer.Option("logs", "--table", help="SQLite table (for .sqlite/.db)"),
    query: Optional[str] = typer.Option(None, "--query", help="Custom SQL query (SQLite)"),
    timestamp_col: str = typer.Option(
        "timestamp", "--ts-col", help="Timestamp column name (SQLite)"
    ),
    message_col: Optional[str] = typer.Option(
        None, "--msg-col", help="Message column name (SQLite); omit to serialize full row"
    ),
    fmt: Optional[str] = typer.Option(
        None,
        "--format",
        "-f",
        help="Force format: text, jsonl, sqlite (default: auto-detect from extension)",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Parse only, don't upload"),
) -> None:
    """Import log data from a file into CloudWatch Logs."""
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


@app.command()
def create_group(
    owner: str = typer.Option(..., "--owner", "-o", help="Owner tag"),
    region: str = typer.Option("us-east-1", "--region", "-r"),
    log_group_prefix: str = typer.Option("/trailhead", "--prefix"),
    retention_days: Optional[int] = typer.Option(
        None, "--retention", help="Log retention in days (default: never expire)"
    ),
) -> None:
    """Pre-create a CloudWatch log group for an owner."""
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


def main() -> None:
    app()


if __name__ == "__main__":
    main()
