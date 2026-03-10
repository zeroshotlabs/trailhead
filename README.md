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

The CLI uses boto3 directly (no running server required). Install it locally:

```bash
cd trailhead
pip install -e .
```

### `import-logs`

```bash
# Plain text log
trailhead-cli import-logs access.log --owner nginx --create-group

# JSONL
trailhead-cli import-logs events.jsonl --owner billing -r us-west-2

# SQLite
trailhead-cli import-logs app.db --owner backend -f sqlite \
    --table events --ts-col created_at --msg-col payload

# Custom SQL
trailhead-cli import-logs app.db --owner backend -f sqlite \
    --query "SELECT * FROM logs WHERE level='ERROR'"

# Dry run
trailhead-cli import-logs huge.log --owner test --dry-run
```

### `create-group`

Pre-create a log group with optional retention policy.

```bash
trailhead-cli create-group --owner myapp --retention 30
```

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
