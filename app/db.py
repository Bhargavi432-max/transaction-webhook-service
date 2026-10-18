import asyncpg

from app import config

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      TEXT PRIMARY KEY,
    source_account      TEXT NOT NULL,
    destination_account TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    currency            CHAR(3) NOT NULL,
    status              TEXT NOT NULL DEFAULT 'PROCESSING',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    attempts            INT NOT NULL DEFAULT 0,
    locked_until        TIMESTAMPTZ,
    last_error          TEXT
);

CREATE INDEX IF NOT EXISTS transactions_pending_idx
    ON transactions (created_at)
    WHERE status = 'PROCESSING';
"""


async def create_pool() -> asyncpg.Pool:
    pool = await asyncpg.create_pool(
        config.DATABASE_URL,
        min_size=config.DB_POOL_MIN,
        max_size=config.DB_POOL_MAX,
        # Keep warm connections around: opening a new TLS connection to a remote DB
        # costs several round trips and would blow the webhook latency budget.
        max_inactive_connection_lifetime=0,
        # Cached prepared statements make each query a single round trip. They are not
        # supported by transaction-mode poolers (Supabase port 6543), so disable there.
        statement_cache_size=0 if ":6543" in config.DATABASE_URL else 100,
        command_timeout=10,
    )
    async with pool.acquire() as conn:
        await conn.execute(SCHEMA)
    return pool


async def insert_transaction(pool: asyncpg.Pool, txn: dict) -> bool:
    """Insert a new transaction. Returns False if this transaction_id already exists."""
    row = await pool.fetchrow(
        """
        INSERT INTO transactions (transaction_id, source_account, destination_account, amount, currency)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (transaction_id) DO NOTHING
        RETURNING transaction_id
        """,
        txn["transaction_id"],
        txn["source_account"],
        txn["destination_account"],
        txn["amount"],
        txn["currency"],
    )
    return row is not None


async def get_transaction(pool: asyncpg.Pool, transaction_id: str) -> asyncpg.Record | None:
    return await pool.fetchrow("SELECT * FROM transactions WHERE transaction_id = $1", transaction_id)


async def claim_next(pool: asyncpg.Pool) -> asyncpg.Record | None:
    """Atomically lease the oldest pending transaction that no one else is working on.

    FOR UPDATE SKIP LOCKED lets several workers/instances pull from the same table
    without ever claiming the same row twice.
    """
    return await pool.fetchrow(
        """
        UPDATE transactions t
        SET locked_until = now() + make_interval(secs => $1),
            attempts = t.attempts + 1
        WHERE t.transaction_id = (
            SELECT transaction_id FROM transactions
            WHERE status = 'PROCESSING'
              AND (locked_until IS NULL OR locked_until < now())
            ORDER BY created_at
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING t.*
        """,
        config.LEASE_SECONDS,
    )


async def mark_processed(pool: asyncpg.Pool, transaction_id: str) -> None:
    # The status guard makes this a no-op if the transaction was already finalised.
    await pool.execute(
        """
        UPDATE transactions
        SET status = 'PROCESSED', processed_at = now(), locked_until = NULL, last_error = NULL
        WHERE transaction_id = $1 AND status = 'PROCESSING'
        """,
        transaction_id,
    )


async def mark_failed_attempt(pool: asyncpg.Pool, transaction_id: str, error: str, retry_in: float) -> None:
    """Record the error and schedule a retry; give up after MAX_ATTEMPTS."""
    await pool.execute(
        """
        UPDATE transactions
        SET last_error = $2,
            locked_until = now() + make_interval(secs => $3),
            status = CASE WHEN attempts >= $4 THEN 'FAILED' ELSE status END
        WHERE transaction_id = $1 AND status = 'PROCESSING'
        """,
        transaction_id,
        error[:1000],
        retry_in,
        config.MAX_ATTEMPTS,
    )


async def release_leases(pool: asyncpg.Pool, transaction_ids: list[str]) -> None:
    """On graceful shutdown, hand in-flight work back so the next instance starts it right away."""
    if transaction_ids:
        await pool.execute(
            "UPDATE transactions SET locked_until = NULL WHERE transaction_id = ANY($1::text[]) AND status = 'PROCESSING'",
            transaction_ids,
        )
