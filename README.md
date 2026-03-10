# Trailhead

High-performance async log ingestion API backed by AWS CloudWatch Logs.

- **Streaming NDJSON** — clients POST newline-delimited JSON; the server processes lines as they arrive over a single connection (HTTP/1.1 chunked, HTTP/2 via Hypercorn)
- **Owner-based routing** — each `owner` maps to a dedicated CloudWatch log group (`/trailhead/{owner}`)
- **Pre-shared key auth** — simple `X-API-Key` header validated against the config
- **CLI bulk import** — ship `.log`, `.jsonl`, or SQLite databases straight to CloudWatch

---

## Quick start

```bash
cd trailhead
python -m venv .venv && source .venv/bin/activate
pip install -e .

# Create your config
cp config.yaml.example config.yaml
# Edit config.yaml — set a real API key and AWS region

# Pre-create a log group for an owner
trailhead-cli create-group --owner myapp --region us-east-1

# Start the server
trailhead
```

The API listens on `0.0.0.0:8000` by default (configurable in `config.yaml`).

---

## API

### `POST /ingest?owner={owner}`

Accepts **NDJSON** (one JSON object per line). Streams the request body so clients can keep a connection open and push lines continuously.

| Header / Param | Required | Description |
|---|---|---|
| `X-API-Key` | yes | Pre-shared API key from config |
| `owner` (query) | yes | Owner tag — routes to `/trailhead/{owner}` log group |

Each JSON line may contain an optional `timestamp` field (epoch-ms, epoch-s, or ISO-8601 string). If absent, server receipt time is used.

#### Example — curl

```bash
printf '{"level":"info","msg":"boot ok","timestamp":1710000000000}\n{"level":"warn","msg":"disk 90%%"}\n' \
  | curl -s -X POST "http://localhost:8000/ingest?owner=myapp" \
         -H "X-API-Key: YOUR_KEY" \
         -H "Content-Type: application/x-ndjson" \
         --data-binary @-
```

#### Response

```json
{
  "status": "ok",
  "owner": "myapp",
  "log_group": "/trailhead/myapp",
  "log_stream": "2026/03/10/a1b2c3d4e5f6",
  "accepted": 2,
  "rejected": 0,
  "flushed": 2
}
```

### `GET /health`

Returns `{"status": "healthy"}`.

---

## CLI — `trailhead-cli`

### `import-logs`

Import from a local file directly to CloudWatch (no running server required).

```bash
# Plain text log file
trailhead-cli import-logs access.log --owner nginx --create-group

# JSONL file
trailhead-cli import-logs events.jsonl --owner billing -r us-west-2

# SQLite database
trailhead-cli import-logs app.db --owner backend --format sqlite \
    --table events --ts-col created_at --msg-col payload

# Custom SQL query
trailhead-cli import-logs app.db --owner backend --format sqlite \
    --query "SELECT * FROM logs WHERE level='ERROR'"

# Dry run (parse & validate, no upload)
trailhead-cli import-logs huge.log --owner test --dry-run
```

### `create-group`

Pre-create a CloudWatch log group with optional retention.

```bash
trailhead-cli create-group --owner myapp --retention 30
```

---

## Configuration

`config.yaml` (or set `TRAILHEAD_CONFIG` env var to a custom path):

```yaml
server:
  host: "0.0.0.0"
  port: 8000

auth:
  api_keys:
    - "your-secret-key"

aws:
  region: "us-east-1"
  log_group_prefix: "/trailhead"
  auto_create_groups: false    # set true for dev convenience

ingest:
  max_batch_events: 10000     # per PutLogEvents call
  max_batch_bytes: 1048576    # 1 MB per batch
```

---

## Architecture notes

- **Server**: FastAPI + Hypercorn (HTTP/2 ready; QUIC/HTTP/3 possible with `aioquic`)
- **CloudWatch client**: `aiobotocore` for fully async I/O — no thread-pool bottleneck
- **Batching**: events accumulate in memory and flush automatically when CloudWatch size/count limits are reached, or when the request stream ends
- **CLI**: sync `boto3` for straightforward batch uploads with Rich progress bars
- **Timestamps**: auto-parsed from epoch-ms, epoch-s, or ISO-8601; falls back to ingest time
- **Extensibility**: the owner/log-group routing and streaming ingest provide the foundation for real-time analytics pipelines (Kinesis tap, Lambda fan-out, etc.)
