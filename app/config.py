import os


def _int(name: str, default: int) -> int:
    return int(os.getenv(name, default))


def _float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

# Simulated external API call duration.
PROCESSING_DELAY_SECONDS = _float("PROCESSING_DELAY_SECONDS", 30)

# How long a worker "owns" a claimed transaction. If the process dies mid-processing,
# the lease expires and the transaction is picked up again — nothing is lost.
LEASE_SECONDS = _float("LEASE_SECONDS", PROCESSING_DELAY_SECONDS + 30)

# Max transactions processed concurrently by one instance. Processing is I/O-bound
# (waiting on an external API), so this can be high.
MAX_CONCURRENCY = _int("MAX_CONCURRENCY", 100)

# Fallback poll interval; new webhooks wake the worker immediately.
POLL_INTERVAL_SECONDS = _float("POLL_INTERVAL_SECONDS", 2)

MAX_ATTEMPTS = _int("MAX_ATTEMPTS", 5)

# Hard budget for the webhook DB write so the endpoint always answers well under 500 ms.
WEBHOOK_DB_TIMEOUT_SECONDS = _float("WEBHOOK_DB_TIMEOUT_SECONDS", 0.4)

DB_POOL_MIN = _int("DB_POOL_MIN", 5)
DB_POOL_MAX = _int("DB_POOL_MAX", 10)
