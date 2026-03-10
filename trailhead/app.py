"""Trailhead — async log ingestion API.

Run directly:  python -m trailhead.app
Via entry-point: trailhead
"""
from __future__ import annotations

import asyncio
import logging
import re
import secrets
import time
from contextlib import asynccontextmanager
from typing import Annotated

import orjson
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import ORJSONResponse

from .cloudwatch import CloudWatchManager
from .config import Config, load_config

logger = logging.getLogger("trailhead.app")

_OWNER_RE = re.compile(r"^[a-zA-Z0-9_\-\.]{1,128}$")

# ---------------------------------------------------------------------------
# Globals wired up during lifespan
# ---------------------------------------------------------------------------
_cw: CloudWatchManager | None = None
_cfg: Config | None = None


def _get_cfg() -> Config:
    assert _cfg is not None
    return _cfg


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _cw, _cfg
    _cfg = load_config()
    _cw = CloudWatchManager(_cfg)
    await _cw.start()
    logger.info("Trailhead ready on %s:%s", _cfg.server.host, _cfg.server.port)
    yield
    await _cw.stop()


app = FastAPI(
    title="Trailhead",
    version="0.1.0",
    default_response_class=ORJSONResponse,
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------
async def _verify_key(x_api_key: Annotated[str, Header()]) -> str:
    cfg = _get_cfg()
    for key in cfg.auth.api_keys:
        if secrets.compare_digest(x_api_key, key):
            return x_api_key
    raise HTTPException(status_code=401, detail="Invalid API key")


# ---------------------------------------------------------------------------
# Timestamp helper
# ---------------------------------------------------------------------------
def _parse_ts(raw) -> int | None:
    """Best-effort parse to epoch-ms.  Returns None on failure."""
    if isinstance(raw, int):
        return raw if raw > 1e12 else raw * 1000
    if isinstance(raw, float):
        return int(raw * 1000) if raw < 1e12 else int(raw)
    if isinstance(raw, str):
        try:
            from datetime import datetime, timezone

            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None
    return None


# ---------------------------------------------------------------------------
# POST /ingest  —  streaming NDJSON
# ---------------------------------------------------------------------------
@app.post("/ingest")
async def ingest(
    request: Request,
    owner: Annotated[str, Query(description="Owner bucket / tag for this log data")],
    _key: str = Depends(_verify_key),
):
    if not _OWNER_RE.match(owner):
        raise HTTPException(
            status_code=422,
            detail="owner must be 1-128 chars of [a-zA-Z0-9_\\-.]",
        )

    assert _cw is not None
    try:
        batcher = await _cw.create_batcher(owner)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    now_ms = int(time.time() * 1000)
    accepted = 0
    rejected = 0
    buf = b""

    async for chunk in request.stream():
        buf += chunk
        while b"\n" in buf:
            line, buf = buf.split(b"\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                obj = orjson.loads(line)
                ts = _parse_ts(obj.pop("timestamp", None)) or now_ms
                message = orjson.dumps(obj).decode()
                await batcher.add_and_maybe_flush(ts, message)
                accepted += 1
            except Exception:
                rejected += 1

    # trailing data without final newline
    remainder = buf.strip()
    if remainder:
        try:
            obj = orjson.loads(remainder)
            ts = _parse_ts(obj.pop("timestamp", None)) or now_ms
            message = orjson.dumps(obj).decode()
            await batcher.add_and_maybe_flush(ts, message)
            accepted += 1
        except Exception:
            rejected += 1

    await batcher.flush()

    return {
        "status": "ok",
        "owner": owner,
        "log_group": batcher.log_group,
        "log_stream": batcher.log_stream,
        "accepted": accepted,
        "rejected": rejected,
        "flushed": batcher.events_flushed,
    }


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "healthy"}


# ---------------------------------------------------------------------------
# Entry-point
# ---------------------------------------------------------------------------
def run() -> None:
    from hypercorn.asyncio import serve
    from hypercorn.config import Config as HyperConfig

    cfg = load_config()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s  %(message)s",
    )

    hcfg = HyperConfig()
    hcfg.bind = [f"{cfg.server.host}:{cfg.server.port}"]
    hcfg.accesslog = "-"

    asyncio.run(serve(app, hcfg))


if __name__ == "__main__":
    run()
