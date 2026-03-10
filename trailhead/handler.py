"""Trailhead — Lambda handler for log ingestion to CloudWatch Logs.

Zero external dependencies; uses only stdlib + boto3 from the Lambda runtime.
"""
from __future__ import annotations

import base64
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

_logs = boto3.client("logs")
_PREFIX = os.environ.get("LOG_GROUP_PREFIX", "/trailhead")
_AUTO_CREATE = os.environ.get("AUTO_CREATE_GROUPS", "true").lower() == "true"
_OWNER_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")

# PutLogEvents hard limits
_MAX_EVENTS = 10_000
_MAX_BYTES = 1_048_576
_OVERHEAD = 26  # per-event wire overhead


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _resp(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_ts(raw) -> int | None:
    """Best-effort parse to epoch-ms."""
    if isinstance(raw, (int, float)):
        return int(raw * 1000) if raw < 1e12 else int(raw)
    if isinstance(raw, str):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


def _flush(log_group: str, log_stream: str, events: list[dict]) -> None:
    if not events:
        return
    events.sort(key=lambda e: e["timestamp"])
    _logs.put_log_events(
        logGroupName=log_group,
        logStreamName=log_stream,
        logEvents=events,
    )


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------
def lambda_handler(event, context):
    path = event.get("path", "")
    method = event.get("httpMethod", "")

    if path == "/health" and method == "GET":
        return _resp(200, {"status": "healthy"})

    if path != "/ingest" or method != "POST":
        return _resp(404, {"error": "not found"})

    return _handle_ingest(event)


# ---------------------------------------------------------------------------
# POST /ingest
# ---------------------------------------------------------------------------
def _handle_ingest(event: dict) -> dict:
    params = event.get("queryStringParameters") or {}
    owner = params.get("owner", "")
    if not _OWNER_RE.match(owner):
        return _resp(422, {"error": "owner must be 1-128 chars of [a-zA-Z0-9_\\-.]"})

    log_group = f"{_PREFIX.rstrip('/')}/{owner}"

    # --- ensure log group ---
    if _AUTO_CREATE:
        try:
            _logs.create_log_group(logGroupName=log_group)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                return _resp(500, {"error": str(exc)})
    else:
        try:
            resp = _logs.describe_log_groups(logGroupNamePrefix=log_group, limit=1)
            if not any(g["logGroupName"] == log_group for g in resp.get("logGroups", [])):
                return _resp(404, {"error": f"Log group '{log_group}' not found"})
        except ClientError as exc:
            return _resp(500, {"error": str(exc)})

    # --- create log stream ---
    day = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    log_stream = f"{day}/{uuid.uuid4().hex[:12]}"
    _logs.create_log_stream(logGroupName=log_group, logStreamName=log_stream)

    # --- parse NDJSON body ---
    body = event.get("body", "") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()

    now_ms = int(time.time() * 1000)
    accepted = 0
    rejected = 0
    buf: list[dict] = []
    buf_bytes = 0
    flushed = 0

    for line in body.split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            ts = _parse_ts(obj.pop("timestamp", None)) or now_ms
            message = json.dumps(obj, separators=(",", ":"))
            ev_bytes = len(message.encode()) + _OVERHEAD

            if buf and (len(buf) >= _MAX_EVENTS or (buf_bytes + ev_bytes) > _MAX_BYTES):
                _flush(log_group, log_stream, buf)
                flushed += len(buf)
                buf = []
                buf_bytes = 0

            buf.append({"timestamp": ts, "message": message})
            buf_bytes += ev_bytes
            accepted += 1
        except Exception:
            rejected += 1

    if buf:
        _flush(log_group, log_stream, buf)
        flushed += len(buf)

    return _resp(200, {
        "status": "ok",
        "owner": owner,
        "log_group": log_group,
        "log_stream": log_stream,
        "accepted": accepted,
        "rejected": rejected,
        "flushed": flushed,
    })
