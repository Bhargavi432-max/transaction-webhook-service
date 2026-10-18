"""End-to-end check of the four success criteria against a running service.

Usage:
    python scripts/smoke_test.py                      # http://localhost:8000
    python scripts/smoke_test.py https://your-app.onrender.com

Uses only the standard library, so no install is needed.
"""

import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000").rstrip("/")
DELAY = 30
WAIT_TIMEOUT = DELAY + 30


def request(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status, payload = resp.status, resp.read()
    except urllib.error.HTTPError as err:
        status, payload = err.code, err.read()
    elapsed_ms = (time.perf_counter() - start) * 1000
    return status, (json.loads(payload) if payload else None), elapsed_ms


def webhook(txn_id, amount=1500):
    return request(
        "POST",
        "/v1/webhooks/transactions",
        {
            "transaction_id": txn_id,
            "source_account": "acc_user_789",
            "destination_account": "acc_merchant_456",
            "amount": amount,
            "currency": "INR",
        },
    )


def wait_processed(txn_id):
    deadline = time.time() + WAIT_TIMEOUT
    while time.time() < deadline:
        status, body, _ = request("GET", f"/v1/transactions/{txn_id}")
        if status == 200 and body[0]["status"] == "PROCESSED":
            return body
        time.sleep(2)
    raise AssertionError(f"{txn_id} not processed within {WAIT_TIMEOUT}s")


def check(cond, msg):
    print(("  PASS " if cond else "  FAIL ") + msg)
    if not cond:
        sys.exit(1)


def main():
    run = uuid.uuid4().hex[:8]
    print(f"Target: {BASE}")

    print("\n0. Health check")
    status, body, ms = request("GET", "/")
    check(status == 200 and body["status"] == "HEALTHY", f"GET / -> {status} {body} ({ms:.0f} ms)")

    print("\n1. Single transaction")
    single = f"txn_single_{run}"
    status, _, ms = webhook(single)
    check(status == 202, f"202 Accepted in {ms:.0f} ms")
    check(ms < 500, "responded under 500 ms")
    status, body, _ = request("GET", f"/v1/transactions/{single}")
    check(body[0]["status"] == "PROCESSING" and body[0]["processed_at"] is None, "status is PROCESSING right away")

    print("\n2. Duplicate prevention (20 concurrent copies of one webhook)")
    dup = f"txn_dup_{run}"
    with ThreadPoolExecutor(20) as pool:
        results = list(pool.map(lambda _: webhook(dup), range(20)))
    check(all(r[0] == 202 for r in results), "every copy got 202 (no errors)")
    created = [r for r in results if r[1] and not r[1]["duplicate"]]
    check(len(created) == 1, f"exactly one copy created the transaction (got {len(created)})")

    print("\n3. Performance under load (100 distinct webhooks, 25 at a time)")
    with ThreadPoolExecutor(25) as pool:
        results = list(pool.map(lambda i: webhook(f"txn_load_{run}_{i}"), range(100)))
    latencies = sorted(r[2] for r in results)
    p95 = latencies[int(len(latencies) * 0.95) - 1]
    check(all(r[0] == 202 for r in results), "all 100 accepted")
    print(f"  latency p50={latencies[50]:.0f} ms  p95={p95:.0f} ms  max={latencies[-1]:.0f} ms")
    status, _, ms = webhook(f"txn_during_load_{run}")
    check(status == 202 and ms < 500, f"new webhook still answered in {ms:.0f} ms while 100+ are processing")

    print(f"\nWaiting ~{DELAY}s for background processing...")
    body = wait_processed(single)[0]
    created_at = body["created_at"].replace("Z", "+00:00")
    processed_at = body["processed_at"].replace("Z", "+00:00")
    from datetime import datetime

    took = (datetime.fromisoformat(processed_at) - datetime.fromisoformat(created_at)).total_seconds()
    check(DELAY <= took < DELAY + 10, f"single transaction PROCESSED after {took:.1f}s")

    body = wait_processed(dup)
    check(len(body) == 1, "duplicate transaction stored and processed exactly once")

    for i in (0, 50, 99):
        wait_processed(f"txn_load_{run}_{i}")
    check(True, "load transactions processed")

    status, _, _ = request("GET", "/v1/transactions/does_not_exist")
    check(status == 404, "unknown transaction -> 404")
    print("\nAll checks passed.")


if __name__ == "__main__":
    main()
