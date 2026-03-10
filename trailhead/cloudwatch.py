"""Async CloudWatch Logs client with automatic batching for PutLogEvents limits."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

import aiobotocore.session
from botocore.exceptions import ClientError

from .config import Config

logger = logging.getLogger("trailhead.cloudwatch")

# PutLogEvents hard limits
_MAX_BATCH_EVENTS = 10_000
_MAX_BATCH_BYTES = 1_048_576
_EVENT_OVERHEAD = 26  # per-event wire overhead in bytes


class Batcher:
    """Accumulates log events and flushes to a single CloudWatch log stream.

    Automatically respects PutLogEvents size/count limits.  Call
    ``add_and_maybe_flush`` for each event; call ``flush`` when done.
    """

    def __init__(self, client, log_group: str, log_stream: str, cfg: Config):
        self._client = client
        self.log_group = log_group
        self.log_stream = log_stream
        self._max_events = min(cfg.ingest.max_batch_events, _MAX_BATCH_EVENTS)
        self._max_bytes = min(cfg.ingest.max_batch_bytes, _MAX_BATCH_BYTES)
        self._buf: list[dict] = []
        self._buf_bytes: int = 0
        self.events_flushed: int = 0

    # ------------------------------------------------------------------

    async def add_and_maybe_flush(self, timestamp_ms: int, message: str) -> None:
        event_bytes = len(message.encode()) + _EVENT_OVERHEAD

        if self._buf and (
            len(self._buf) >= self._max_events
            or (self._buf_bytes + event_bytes) > self._max_bytes
        ):
            await self.flush()

        self._buf.append({"timestamp": timestamp_ms, "message": message})
        self._buf_bytes += event_bytes

    async def flush(self) -> int:
        if not self._buf:
            return 0

        events = sorted(self._buf, key=lambda e: e["timestamp"])
        count = len(events)

        await self._client.put_log_events(
            logGroupName=self.log_group,
            logStreamName=self.log_stream,
            logEvents=events,
        )

        self.events_flushed += count
        self._buf.clear()
        self._buf_bytes = 0
        logger.debug("Flushed %d events to %s / %s", count, self.log_group, self.log_stream)
        return count


class CloudWatchManager:
    """Manages the aiobotocore CloudWatch Logs client lifecycle."""

    def __init__(self, config: Config):
        self._cfg = config
        self._session = aiobotocore.session.get_session()
        self._ctx = None
        self._client = None

    async def start(self) -> None:
        self._ctx = self._session.create_client("logs", region_name=self._cfg.aws.region)
        self._client = await self._ctx.__aenter__()
        logger.info("CloudWatch client started (region=%s)", self._cfg.aws.region)

    async def stop(self) -> None:
        if self._ctx:
            await self._ctx.__aexit__(None, None, None)
            self._client = None
            logger.info("CloudWatch client stopped")

    # ------------------------------------------------------------------

    def _group_name(self, owner: str) -> str:
        prefix = self._cfg.aws.log_group_prefix.rstrip("/")
        return f"{prefix}/{owner}"

    async def ensure_group(self, owner: str) -> str:
        name = self._group_name(owner)
        if self._cfg.aws.auto_create_groups:
            try:
                await self._client.create_log_group(logGroupName=name)
                logger.info("Created log group %s", name)
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                    raise
        else:
            try:
                resp = await self._client.describe_log_groups(
                    logGroupNamePrefix=name, limit=1
                )
                groups = resp.get("logGroups", [])
                if not any(g["logGroupName"] == name for g in groups):
                    raise ValueError(
                        f"Log group '{name}' does not exist. "
                        "Create it first or set auto_create_groups: true in config."
                    )
            except ClientError:
                raise
        return name

    async def create_batcher(self, owner: str) -> Batcher:
        log_group = await self.ensure_group(owner)
        ts = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        log_stream = f"{ts}/{uuid.uuid4().hex[:12]}"

        await self._client.create_log_stream(
            logGroupName=log_group, logStreamName=log_stream
        )
        logger.info("Created log stream %s/%s", log_group, log_stream)
        return Batcher(self._client, log_group, log_stream, self._cfg)
