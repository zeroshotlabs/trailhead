# Trailhead

Serverless log ingestion API backed by AWS CloudWatch Logs.

- **One-command deploy** — `sam build && sam deploy --guided`, done
- **Zero external deps** — Lambda handler uses only stdlib + boto3 from the runtime
- **API-key auth at the edge** — API Gateway manages keys and usage plans; Lambda never sees unauthorized traffic
- **Owner-based routing** — each `owner` maps to a CloudWatch log group (`/trailhead/{owner}`)
- **CLI bulk import** — ship `.log`, `.jsonl`, or SQLite databases straight to CloudWatch

---

## Deploy

Prerequisites: [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) and configured AWS credentials.

```bash
sam build
sam deploy --guided   # first time — picks region, stack name, creates S3 bucket
```

After deploy, the stack outputs two values:

| Output | Description |
|---|---|
| `ApiUrl` | Base URL, e.g. `https://abc123.execute-api.us-east-1.amazonaws.com/v1` |
| `ApiKeyId` | Key ID — retrieve the secret with the command below |

```bash
# Get your API key value
aws apigateway get-api-key --api-key <ApiKeyId> --include-value --query 'value' --output text
```

---

## API

### `POST /ingest?owner={owner}`

Accepts **NDJSON** (one JSON object per line).

| Header / Param | Required | Description |
|---|---|---|
| `x-api-key` | yes | API key managed by API Gateway |
| `owner` (query) | yes | Owner tag — routes to `/trailhead/{owner}` log group |

Each JSON line may include a `timestamp` field (epoch-ms, epoch-s, or ISO-8601). If absent, server receipt time is used.

```bash
printf '{"level":"info","msg":"deployed","timestamp":1710000000000}\n{"level":"warn","msg":"disk 90%%"}\n' \
  | curl -s -X POST "https://<api-id>.execute-api.<region>.amazonaws.com/v1/ingest?owner=myapp" \
         -H "x-api-key: YOUR_KEY" \
         -H "Content-Type: application/x-ndjson" \
         --data-binary @-
```

Response:

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

Install locally:

```bash
cd trailhead
pip install -e .
```

### Quickstart: zero-disk nginx log shipping

Point nginx at a named pipe — logs never touch disk:

```bash
# 1. Create a log group
trailhead-cli create-group --owner secondpageai_access --retention 30

# 2. Export your API URL and key
export TRAILHEAD_API_URL=https://<api-id>.execute-api.<region>.amazonaws.com/v1
export TRAILHEAD_API_KEY=<your-key>

# 3. Start the shipper — it creates the pipe and removes it on exit
trailhead-cli ship /var/log/nginx/access.pipe -o secondpageai_access --mkfifo &

# 4. Point nginx at the pipe (in your nginx.conf)
#    access_log /var/log/nginx/access.pipe combined;
#    Then: nginx -s reload
```

`--mkfifo` creates the named pipe automatically using Python's `os.mkfifo()` (no system packages needed) and removes it on exit (SIGTERM, SIGINT, or normal shutdown). The shipper auto-reconnects when nginx restarts.

Log groups are named `{prefix}/{owner}` — with the default prefix that's `/trailhead/secondpageai_access`.

### `ship`

Stream logs to the Trailhead API in real time. Reads from a named pipe (FIFO), file, or stdin. Batches lines and POSTs them as NDJSON with connection reuse and automatic retries.

JSON lines are forwarded as-is. Plain text lines (standard nginx/apache format) are wrapped as `{"message": "..."}` with auto-detected timestamps.

```bash
# Zero-disk: creates the pipe, removes it on exit
trailhead-cli ship /tmp/access.pipe -o mysite_access --mkfifo

# Tail a regular log file
trailhead-cli ship /var/log/nginx/access.log -o mysite_access --follow

# Pipe from any process
my_app 2>&1 | trailhead-cli ship -o myapp

# Read a file once (no --follow), then exit
trailhead-cli ship /var/log/nginx/access.log.1 -o mysite_access

# Tune batch size and flush interval
trailhead-cli ship /var/log/app.log -o myapp --follow --batch-size 200 --flush-interval 2
```

Set `TRAILHEAD_API_URL` and `TRAILHEAD_API_KEY` env vars to avoid passing `--api-url` and `--api-key` every time. Handles SIGTERM/SIGINT gracefully (flushes remaining buffer before exit).

**Running as a systemd service:**

```ini
# /etc/systemd/system/trailhead-ship.service
[Unit]
Description=Trailhead log shipper
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/trailhead-cli ship /var/log/nginx/access.pipe --owner mysite_access --mkfifo
Environment=TRAILHEAD_API_URL=https://<api-id>.execute-api.<region>.amazonaws.com/v1
Environment=TRAILHEAD_API_KEY=<your-key>
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `create-group`

Pre-create a CloudWatch log group for an owner. Use this when you want to set retention or prepare groups before any logs arrive.

```bash
trailhead-cli create-group --owner mysite_access                  # no expiry
trailhead-cli create-group --owner mysite_access --retention 30   # expire after 30 days
trailhead-cli create-group --owner billing -r us-west-2           # different region
```

### `import-logs`

Bulk-import a local file directly into CloudWatch (via boto3, no API needed). Useful for backfilling historical logs.

The group must already exist, or pass `--create-group` to auto-create.

```bash
# Plain text log file
trailhead-cli import-logs access.log --owner nginx --create-group

# JSONL
trailhead-cli import-logs events.jsonl --owner billing -r us-west-2

# SQLite
trailhead-cli import-logs app.db --owner backend -f sqlite \
    --table events --ts-col created_at --msg-col payload

# Dry run — parse and validate without uploading
trailhead-cli import-logs huge.log --owner test --dry-run
```

File format is auto-detected from extension (`.log`/`.txt` → text, `.jsonl`/`.ndjson` → JSONL, `.sqlite`/`.db` → SQLite). Override with `--format text|jsonl|sqlite`.

---

## SAM template parameters

| Parameter | Default | Description |
|---|---|---|
| `LogGroupPrefix` | `/trailhead` | Prefix for CloudWatch log groups |
| `AutoCreateGroups` | `true` | Auto-create groups on first ingest per owner |

Override during deploy:

```bash
sam deploy --parameter-overrides LogGroupPrefix=/logs AutoCreateGroups=false
```

---

## Local development

```bash
sam build
sam local start-api          # requires Docker
# Then:
curl http://127.0.0.1:3000/health
```

---

## Architecture

```
Client ──▶ API Gateway (REST, x-api-key enforced)
               │
               ▼
           Lambda (handler.py)
               │
               ├─ parse NDJSON body
               ├─ ensure log group exists
               ├─ create log stream per request
               └─ batch PutLogEvents (10k events / 1MB per call)
               │
               ▼
         CloudWatch Logs
           /trailhead/{owner}
```

- **Payload limit**: ~6 MB per request (Lambda sync invocation limit). For larger imports, use `trailhead-cli import-logs` which streams directly via boto3 with no size cap.
- **Throttle defaults**: 100 req/s sustained, 200 burst (configurable in `template.yaml` UsagePlan).
- **Extensibility**: the owner/log-group routing sets up cleanly for CloudWatch subscription filters, Kinesis fan-out, or Metrics Filters when you add real-time analytics.
