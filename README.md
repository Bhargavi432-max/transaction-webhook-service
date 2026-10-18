# Transaction Webhook Service

A Python service that receives transaction webhooks from payment processors (e.g. Razorpay), acknowledges them immediately with `202 Accepted`, and processes them reliably in the background, exactly once per `transaction_id`.

**Live API:** https://transaction-webhook-service-0zsd.onrender.com  (API docs: https://transaction-webhook-service-0zsd.onrender.com/docs)

> Hosted on Render's free tier, so the first request after 15 minutes idle takes about 50 s while the instance wakes up. Call `GET /` once before testing.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/` | Health check → `{"status": "HEALTHY", "current_time": "..."}` |
| `POST` | `/v1/webhooks/transactions` | Receive a webhook → `202 Accepted` |
| `GET` | `/v1/transactions/{transaction_id}` | Look up status (`PROCESSING` / `PROCESSED`) |

Interactive API docs are served at `/docs`.

---

## How it works

```
 Payment processor                     This service                               Postgres (Supabase)
 ─────────────────                     ────────────                               ───────────────────
 POST /v1/webhooks/transactions ──▶ validate ──▶ INSERT ... ON CONFLICT DO NOTHING ──▶ row: PROCESSING
                 ◀── 202 Accepted (~10 ms)   └─▶ wake worker
                                                                               
                                     Background worker (same process, asyncio)
                                     loop: claim oldest PROCESSING row       ◀──── UPDATE ... FOR UPDATE SKIP LOCKED
                                           (lease it for 60 s)                     (sets locked_until)
                                           await 30 s "external API call"
                                           mark PROCESSED, set processed_at  ────▶ row: PROCESSED
```

1. **The webhook path does one thing:** a single indexed `INSERT`. All slow work happens off the request path, so the response time doesn't depend on processing load. The insert has a hard 400 ms budget, so the endpoint always answers within 500 ms.
2. **The database is the queue.** A webhook is only acknowledged after its row is committed. The background worker pulls work from the same table, so there is no separate broker to run, and nothing lives only in memory.
3. **Leases instead of "in-memory tasks".** The worker claims a row by setting `locked_until = now() + 60s`. If the process crashes, is redeployed or sleeps mid-processing, the lease expires and the transaction is claimed again. On a graceful shutdown (SIGTERM), leases are released at once so the next instance resumes immediately.
4. **Concurrency.** Processing is I/O-bound (waiting on an external API), so one instance runs up to 100 transactions at once (`MAX_CONCURRENCY`). `FOR UPDATE SKIP LOCKED` means you can run several instances against the same table and no row is ever claimed twice.

### Idempotency

Idempotency is enforced by the database, not by application code:

- `transaction_id` is the **primary key**, and the insert is `ON CONFLICT (transaction_id) DO NOTHING`. Concurrent duplicates race on the unique index; exactly one wins and the rest are no-ops. This holds across threads, processes and instances.
- Duplicates still get `202 Accepted` (with `"duplicate": true` in the body), so the processor stops retrying. They are never treated as errors.
- Only newly inserted rows enter the queue, and `mark_processed` is guarded by `WHERE status = 'PROCESSING'`. A transaction can't be finalised twice.
- The first payload for a `transaction_id` wins; later payloads with the same ID are ignored.

### Reliability and error handling

| Failure | Behaviour |
|---|---|
| DB unavailable or slow when a webhook arrives | `503` instead of `202`. We never acknowledge a webhook we haven't stored, so the processor's retry delivers it later (and the retry is idempotent). |
| Process crashes or is killed mid-processing | Row stays `PROCESSING`; lease expires after 60 s and it is reprocessed. **Tested by `docker kill` during processing.** |
| Graceful redeploy / restart | In-flight leases are released on SIGTERM; the new instance picks them up immediately. **Tested by `docker stop` during processing.** |
| Processing step throws | Error recorded in `last_error`, retried with exponential backoff (2 s, 4 s, … up to 60 s); after `MAX_ATTEMPTS` (5) it's marked `FAILED`, so a poison message can't block the queue. |
| Invalid payload (missing field, negative amount, bad currency) | `422` with validation details; nothing stored. |

Processing is **at-least-once with idempotent finalisation**. If a crash happens after the 30 s call but before the `PROCESSED` write, the simulated call runs again. In a real integration you would pass `transaction_id` as an idempotency key to the downstream API, which is the standard way payment APIs handle this.

---

## Technical choices

| Choice | Why |
|---|---|
| **FastAPI + Uvicorn** | Async-native: the 30 s waits are `await`s, not blocked threads, so one small instance handles many transactions in flight. Pydantic validation and OpenAPI docs come for free. |
| **PostgreSQL on Supabase** | Managed, free, persistent cloud Postgres. A unique primary key gives correct idempotency, and `SKIP LOCKED` makes it a solid job queue without adding Redis/SQS/Celery. |
| **asyncpg** | Fastest async Postgres driver. `statement_cache_size=0` keeps it compatible with Supabase's connection pooler. |
| **In-process worker, not Celery/RQ** | For a 2–3 hour scope, one deployable is simpler to run and reason about. Because state lives in Postgres rather than in the worker, the worker can be moved to its own process or service later without changing the design. |
| **Render** | Deploys straight from GitHub with `render.yaml`. Free tier. |

### What I'd do for production

- Run the worker as a separate service so API and processing scale independently.
- **Verify the processor's signature** (e.g. Razorpay's `X-Razorpay-Signature` HMAC) before accepting a webhook. It's omitted here because the spec defines no secret.
- Add structured logs, metrics (queue depth, processing latency, failures) and alerting on `FAILED` rows.
- Add schema migrations (Alembic) instead of `CREATE TABLE IF NOT EXISTS` on startup.
- Use a paid always-on instance (see the note on cold starts below).

---

## Run locally

**Option A: Docker (no setup)**

```bash
docker compose up --build            # API on http://localhost:8000, Postgres included
# port 8000 busy?  API_PORT=8010 docker compose up --build
```

**Option B: Python 3.12 + your own Postgres**

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export DATABASE_URL="postgresql://user:pass@host:5432/db"   # see .env.example
uvicorn app.main:app --reload
```

The table is created automatically on startup.

Configuration (environment variables):

| Variable | Default | |
|---|---|---|
| `DATABASE_URL` | local Postgres | Postgres connection string |
| `PROCESSING_DELAY_SECONDS` | `30` | Simulated external call duration |
| `MAX_CONCURRENCY` | `100` | Transactions processed concurrently per instance |
| `LEASE_SECONDS` | delay + 30 | How long a claim lasts before another worker may retry it |
| `MAX_ATTEMPTS` | `5` | Retries before a transaction is marked `FAILED` |

---

## Test it

### Automated end-to-end check

`scripts/smoke_test.py` covers all four success criteria. It uses only the standard library, and you can point it at any deployment:

```bash
python3 scripts/smoke_test.py                                  # local
python3 scripts/smoke_test.py https://transaction-webhook-service-0zsd.onrender.com
```

Output from a local run:

```
1. Single transaction
  PASS 202 Accepted in 9 ms
  PASS status is PROCESSING right away
2. Duplicate prevention (20 concurrent copies of one webhook)
  PASS every copy got 202 (no errors)
  PASS exactly one copy created the transaction (got 1)
3. Performance under load (100 distinct webhooks, 25 at a time)
  latency p50=8 ms  p95=14 ms  max=15 ms
  PASS new webhook still answered in 1 ms while 100+ are processing
  PASS single transaction PROCESSED after 30.0s
  PASS duplicate transaction stored and processed exactly once
All checks passed.
```

### Manually with curl

```bash
URL=https://transaction-webhook-service-0zsd.onrender.com      # or http://localhost:8000

curl $URL/

curl -i -X POST $URL/v1/webhooks/transactions \
  -H 'Content-Type: application/json' \
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'
# HTTP/1.1 202 Accepted

# send it again → still 202, "duplicate": true, no second transaction
curl $URL/v1/transactions/txn_abc123def456     # status: PROCESSING, processed_at: null
sleep 30
curl $URL/v1/transactions/txn_abc123def456     # status: PROCESSED, processed_at set
```

---

## Deploy (Render + Supabase)

1. **Supabase:** create a project. Go to **Connect → Session pooler** and copy the connection string (the direct connection is IPv6-only, which Render can't reach).
2. **Render:** **New → Blueprint**, pick this repo (it reads `render.yaml`), and set `DATABASE_URL` to the Supabase string. Choose the same region as the Supabase project to keep webhook latency low.
3. Open `https://<service>.onrender.com/`. It should return `HEALTHY`.

> **Current deployment and latency:** the API runs on Render (Singapore) and the database on Supabase (Seoul), the closest free regions I had available. Each DB round trip crosses regions (~150–300 ms), so webhooks take ~300–450 ms end to end. Under bursts of many simultaneous requests, some webhooks exceed the 400 ms write budget and get a `503` rather than an unpersisted `202`, and the processor's retry delivers them (idempotently). Putting the database in the same region as the API brings each query down to ~2 ms. Locally, with the DB co-located, p95 is ~20 ms under 100 concurrent webhooks.

> **Free-tier cold starts:** Render's free instances sleep after 15 minutes without traffic and take about 50 s to wake. The first request after a sleep can't meet the 500 ms target, so hit `GET /` before testing. Once the service is awake, webhooks respond in tens of milliseconds. Transactions accepted before a sleep aren't lost: they're in Postgres and are resumed when the service wakes. In production this would run on an always-on instance.
