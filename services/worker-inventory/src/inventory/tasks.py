"""Task ``inventory.reserve`` (contract section 3).

Idempotency: the reservation id is derived from ``order_id``, so a redelivered message
(``acks_late``) yields the same reservation; a real stock service would use it as its
idempotency key. No other side effect happens here.
"""

import hashlib
from datetime import UTC, datetime

from celery.utils.log import get_task_logger

from inventory.celery_app import app

logger = get_task_logger(__name__)

OUT_OF_STOCK_SKU = "OUT-OF-STOCK"
# Transient infrastructure failures (e.g. reaching the stock service) are retried.
INFRA_ERRORS = (ConnectionError, TimeoutError)


class OutOfStock(Exception):
    """Business refusal: never retried."""


def reservation_id(order_id: str) -> str:
    return "res_" + hashlib.sha256(order_id.encode()).hexdigest()[:20]


def reserve_stock(order: dict, now: datetime) -> dict:
    """Simulated, deterministic stock service (contract business rules)."""
    missing = [item["sku"] for item in order["items"] if item["sku"] == OUT_OF_STOCK_SKU]
    if missing:
        raise OutOfStock(f"sku {', '.join(missing)} unavailable")
    reservation = {
        "reservation_id": reservation_id(order["order_id"]),
        "status": "reserved",
        "reserved_at": now.isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    return {**order, "reservation": reservation}


@app.task(
    name="inventory.reserve",
    autoretry_for=INFRA_ERRORS,
    retry_backoff=True,
    retry_backoff_max=60,
    retry_jitter=True,
    max_retries=5,
    throws=(OutOfStock,),  # expected failure: logged without traceback
)
def reserve(order: dict) -> dict:
    try:
        enriched = reserve_stock(order, datetime.now(UTC))
    except OutOfStock as exc:
        logger.info("stock unavailable order_id=%s reason=%s", order["order_id"], exc)
        raise
    logger.info("stock reserved order_id=%s reservation_id=%s items=%d",
                order["order_id"], enriched["reservation"]["reservation_id"], len(order["items"]))
    return enriched
