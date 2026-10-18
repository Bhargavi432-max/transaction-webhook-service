import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app import config, db
from app.worker import TransactionWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("api")


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class TransactionWebhook(BaseModel):
    transaction_id: str = Field(min_length=1, max_length=255)
    source_account: str = Field(min_length=1, max_length=255)
    destination_account: str = Field(min_length=1, max_length=255)
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Za-z]{3}$")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await db.create_pool()
    app.state.worker = TransactionWorker(app.state.pool)
    app.state.worker.start()
    yield
    await app.state.worker.stop()
    await app.state.pool.close()


app = FastAPI(title="Transaction Webhook Service", lifespan=lifespan)


@app.get("/")
async def health():
    return {"status": "HEALTHY", "current_time": iso(datetime.now(timezone.utc))}


@app.post("/v1/webhooks/transactions", status_code=status.HTTP_202_ACCEPTED)
async def receive_transaction_webhook(payload: TransactionWebhook, request: Request):
    txn = payload.model_dump()
    txn["currency"] = txn["currency"].upper()
    try:
        # Only a single indexed insert happens on the request path; the 30 s of
        # processing runs in the background worker.
        created = await asyncio.wait_for(
            db.insert_transaction(request.app.state.pool, txn),
            timeout=config.WEBHOOK_DB_TIMEOUT_SECONDS,
        )
    except Exception:
        # We never ACK a webhook we could not persist: a non-2xx makes the payment
        # processor retry, and the retry is safe because inserts are idempotent.
        log.exception("Failed to persist webhook %s", txn["transaction_id"])
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"detail": "Temporarily unable to accept webhook, please retry"},
        )

    if created:
        request.app.state.worker.notify()
    return {"message": "Webhook accepted", "transaction_id": txn["transaction_id"], "duplicate": not created}


@app.get("/v1/transactions/{transaction_id}")
async def get_transaction(transaction_id: str, request: Request):
    row = await db.get_transaction(request.app.state.pool, transaction_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return [
        {
            "transaction_id": row["transaction_id"],
            "source_account": row["source_account"],
            "destination_account": row["destination_account"],
            "amount": float(row["amount"]),
            "currency": row["currency"],
            "status": row["status"],
            "created_at": iso(row["created_at"]),
            "processed_at": iso(row["processed_at"]),
        }
    ]
