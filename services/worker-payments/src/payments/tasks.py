"""Task ``payments.authorize`` (contract section 3).

Idempotency: the authorization id is derived from ``order_id``, so a redelivered message
(``acks_late``) produces the same authorization; a real PSP would receive it as its
idempotency key. No other side effect happens here.
"""

import hashlib
from datetime import UTC, datetime
from decimal import Decimal

from celery.utils.log import get_task_logger

from payments.celery_app import app

logger = get_task_logger(__name__)

AMOUNT_LIMIT = Decimal("1000")
# Transient infrastructure failures (e.g. reaching the payment provider) are retried.
INFRA_ERRORS = (ConnectionError, TimeoutError)


class PaymentDeclined(Exception):
    """Business refusal: never retried."""


def authorization_id(order_id: str) -> str:
    return "auth_" + hashlib.sha256(order_id.encode()).hexdigest()[:20]


def authorize_payment(order: dict, now: datetime) -> dict:
    """Simulated, deterministic payment provider (contract business rules)."""
    if Decimal(str(order["total_amount"])) > AMOUNT_LIMIT:
        raise PaymentDeclined("amount exceeds limit")
    payment = {
        "authorization_id": authorization_id(order["order_id"]),
        "status": "authorized",
        "authorized_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return {**order, "payment": payment}


@app.task(
    name="payments.authorize",
    autoretry_for=INFRA_ERRORS,
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    throws=(PaymentDeclined,),  # expected failure: logged without traceback
)
def authorize(order: dict) -> dict:
    try:
        enriched = authorize_payment(order, datetime.now(UTC))
    except PaymentDeclined as exc:
        logger.info("payment declined order_id=%s total_amount=%s reason=%s",
                    order["order_id"], order["total_amount"], exc)
        raise
    logger.info("payment authorized order_id=%s authorization_id=%s total_amount=%s %s",
                order["order_id"], enriched["payment"]["authorization_id"],
                order["total_amount"], order["currency"])
    return enriched
