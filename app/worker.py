import asyncio
import logging

import asyncpg

from app import config, db

log = logging.getLogger("worker")


class TransactionWorker:
    """Background processor that uses the transactions table itself as a durable queue.

    The webhook only writes a PROCESSING row; this worker leases rows, runs the (slow)
    processing step and marks them PROCESSED. Because the queue lives in Postgres, a crash
    or redeploy never loses work: unfinished rows are simply leased again.
    """

    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool
        self._wake = asyncio.Event()
        self._slots = asyncio.Semaphore(config.MAX_CONCURRENCY)
        self._in_flight: dict[str, asyncio.Task] = {}
        self._loop_task: asyncio.Task | None = None

    def start(self) -> None:
        self._loop_task = asyncio.create_task(self._run(), name="transaction-worker")

    def notify(self) -> None:
        """Called after a new webhook is stored so it is picked up without waiting for a poll."""
        self._wake.set()

    async def stop(self) -> None:
        if self._loop_task:
            self._loop_task.cancel()
        # Snapshot first: done-callbacks remove entries from _in_flight as tasks finish.
        txn_ids = list(self._in_flight)
        tasks = list(self._in_flight.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await db.release_leases(self.pool, txn_ids)
        except Exception:
            log.exception("Failed to release leases on shutdown; they will expire on their own")

    async def _run(self) -> None:
        log.info("Worker started (delay=%ss, concurrency=%s)", config.PROCESSING_DELAY_SECONDS, config.MAX_CONCURRENCY)
        while True:
            await self._slots.acquire()
            try:
                self._wake.clear()
                row = await db.claim_next(self.pool)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._slots.release()
                log.exception("Failed to claim work; retrying")
                await asyncio.sleep(config.POLL_INTERVAL_SECONDS)
                continue

            if row is None:
                self._slots.release()
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=config.POLL_INTERVAL_SECONDS)
                except TimeoutError:
                    pass
                continue

            txn_id = row["transaction_id"]
            task = asyncio.create_task(self._process(row), name=f"process-{txn_id}")
            self._in_flight[txn_id] = task
            task.add_done_callback(lambda _t, txn_id=txn_id: self._done(txn_id))

    def _done(self, txn_id: str) -> None:
        self._in_flight.pop(txn_id, None)
        self._slots.release()

    async def _process(self, row: asyncpg.Record) -> None:
        txn_id = row["transaction_id"]
        log.info("Processing %s (attempt %s)", txn_id, row["attempts"])
        try:
            await self._call_external_api(row)
            await db.mark_processed(self.pool, txn_id)
            log.info("Processed %s", txn_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            retry_in = min(2 ** row["attempts"], 60)
            log.exception("Processing %s failed; retrying in %ss", txn_id, retry_in)
            try:
                await db.mark_failed_attempt(self.pool, txn_id, repr(exc), retry_in)
            except Exception:
                log.exception("Could not record failure for %s; lease expiry will retry it", txn_id)

    async def _call_external_api(self, row: asyncpg.Record) -> None:
        # Stand-in for real work (fraud checks, ledger posting, etc.).
        await asyncio.sleep(config.PROCESSING_DELAY_SECONDS)
