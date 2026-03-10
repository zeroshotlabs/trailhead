# Trailhead

Serverless log ingestion API backed by AWS CloudWatch Logs.

- **One-command deploy** — `sam build && sam deploy --guided`, done
- **Zero external deps** — Lambda handler uses only stdlib + boto3
- **API-key auth at the edge** — API Gateway manages keys and usage plans
- **Direct mode** — pipe your server's output straight to CloudWatch, no API needed
- **CLI bulk import** — ship `.log`, `.jsonl`, or SQLite databases

---

## Deploy (API mode)

Prerequisites: [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) and configured AWS credentials.

```bash
sam build
sam deploy --guided
```

Retrieve your API key:

```bash
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

```bash
printf '{"level":"info","msg":"deployed"}\n{"level":"warn","msg":"disk 90%%"}\n' \
  | curl -s -X POST "https://<api-id>.execute-api.<region>.amazonaws.com/v1/ingest?owner=myapp" \
         -H "x-api-key: YOUR_KEY" \
         -H "Content-Type: application/x-ndjson" \
         --data-binary @-
```

### `GET /health`

Returns `{"status": "healthy"}`.

---

## Using with ruph

The recommended way to ship ruph request logs is **direct integration** — ruph
POSTs NDJSON batches to the Trailhead API with no intermediary. Add a
`[trailhead]` section to `ruph.ini`:

```ini
[trailhead]
api_url = https://<api-id>.execute-api.<region>.amazonaws.com/v1
api_key = <your-key>
default_owner = myserver

[https.example.com]
trailhead_owner = examplecom
```

When `trailhead_owner` is set for a vhost, ruph streams full request records
(IP, method, host, path, status, headers, duration, user-agent, etc.) directly
to CloudWatch. See the ruph documentation for details.

---

## CLI — `trailhead-cli`

```bash
cd trailhead
pip install -e .
```

The CLI is useful for **backfilling**, **bulk imports**, and shipping logs from
servers that don't have direct Trailhead integration.

### `ship`

Stream logs to CloudWatch in real time.

**SQLite tailing** — polls a database for new rows:

```bash
# Via the API (needs TRAILHEAD_API_URL + TRAILHEAD_API_KEY env vars)
export TRAILHEAD_API_URL=https://<api-id>.execute-api.<region>.amazonaws.com/v1
export TRAILHEAD_API_KEY=<your-key>

trailhead-cli ship /path/to/requests.db -o mysite_access

# Ship everything from the beginning (backfill)
trailhead-cli ship /path/to/requests.db -o mysite_access --from-start

# Direct to CloudWatch (no API needed)
trailhead-cli ship /path/to/requests.db -o mysite_access --direct --create-group
```

SQLite mode:
- Auto-detected by `.db`/`.sqlite` extension or file header
- Reads from the `requests` table (override with `--table`)
- Tracks last processed row ID in a state file for restart resilience
- Polls every 1s for new rows, batches and flushes per `--batch-size`/`--flush-interval`
- WAL mode allows safe concurrent reads while the writer runs

**stdin piping** — for servers that can write to stdout:

```bash
my_server 2>&1 | trailhead-cli ship -o mysite_access --direct
```

**File tailing** — for text log files:

```bash
trailhead-cli ship /var/log/access.log -o mysite_access --follow
```

Two backends:
- **API mode** (default): POSTs NDJSON batches to the Trailhead Lambda API. Needs `--api-url`/`--api-key` or env vars.
- **`--direct`**: sends straight to CloudWatch via boto3. No API deployment needed.

Handles SIGTERM/SIGINT gracefully (flushes buffer before exit).

### `create-group`

Pre-create a CloudWatch log group with optional retention.

```bash
trailhead-cli create-group --owner mysite_access                  # no expiry
trailhead-cli create-group --owner mysite_access --retention 30   # 30 days
```

### `import-logs`

Bulk-import a local file directly into CloudWatch via boto3. Useful for backfilling.

```bash
trailhead-cli import-logs access.log --owner nginx --create-group
trailhead-cli import-logs app.db --owner backend -f sqlite --table events
trailhead-cli import-logs huge.log --owner test --dry-run
```

---

## Architecture

```
                                  ┌─ API mode ─────────────────────────┐
                                  │                                    │
Client ──▶ API Gateway (x-api-key)│──▶ Lambda ──▶ CloudWatch Logs     │
                                  └────────────────────────────────────┘

                                  ┌─ Direct mode ──────────────────────┐
                                  │                                    │
my_server | trailhead-cli ship ───│──▶ boto3  ──▶ CloudWatch Logs      │
                                  └────────────────────────────────────┘

CloudWatch Logs
  /trailhead/{owner}
    └── 2026/03/10/{stream-id}
```

- **Payload limit (API)**: ~6 MB per request (Lambda limit). Use `--direct` or `import-logs` for larger payloads.
- **Throttle (API)**: 100 req/s sustained, 200 burst (configurable in `template.yaml`).
- **Extensibility**: owner/log-group routing sets up for CloudWatch subscription filters, Kinesis fan-out, or Metrics Filters for real-time analytics.
