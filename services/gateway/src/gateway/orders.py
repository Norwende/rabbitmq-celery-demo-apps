"""Order creation (Celery chain by task name) and status reconstruction."""

import json
import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from celery import chain, signature
from celery.canvas import Signature
from celery.result import AsyncResult, result_from_tuple
from redis import Redis
from redis.exceptions import RedisError

from gateway.celery_app import app as celery_app
from gateway.models import OrderRequest, OrderStatus, Step

logger = logging.getLogger(__name__)

# The chain emitted for every order, in execution order (contract section 3).
CHAIN_STEPS = ("payments.authorize", "inventory.reserve", "notifications.order_confirmed")
ERRBACK_TASK = "notifications.order_failed"


def order_key(order_id: str) -> str:
    return f"order:{order_id}"


def build_order(request: OrderRequest, now: datetime) -> dict:
    """Build the contract ``Order`` payload (JSON-serializable)."""
    total = sum((item.unit_price * item.quantity for item in request.items), Decimal(0))
    return {
        "order_id": f"ord_{uuid.uuid4().hex}",
        "customer_id": request.customer_id,
        "currency": request.currency,
        "items": [
            {"sku": item.sku, "quantity": item.quantity, "unit_price": float(item.unit_price)}
            for item in request.items
        ],
        "total_amount": float(total.quantize(Decimal("0.01"))),
        "created_at": now.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }


def build_workflow(order: dict) -> Signature:
    """payments.authorize(order) | inventory.reserve | notifications.order_confirmed.

    The errback is attached to every link of the chain: whichever task fails, the failing
    worker sends ``notifications.order_failed(<failed task id>, order=order)`` by name.
    """
    workflow = chain(
        signature(CHAIN_STEPS[0], args=[order], app=celery_app),
        *(signature(name, app=celery_app) for name in CHAIN_STEPS[1:]),
    )
    workflow.link_error(signature(ERRBACK_TASK, kwargs={"order": order}, app=celery_app))
    return workflow


def submit_order(order: dict, redis: Redis, ttl_seconds: int) -> None:
    """Make the order trackable, then publish its chain.

    ``freeze()`` assigns the task ids of every link (and returns the result tree) without
    sending anything; ``apply_async()`` then publishes with exactly those ids. Storing the
    tracking key first guarantees that a published chain is always trackable: if Redis is
    down nothing is published, and if publishing fails the key is removed again (best
    effort). Either error propagates and becomes a 503.
    """
    workflow = build_workflow(order)
    key = order_key(order["order_id"])
    redis.set(key, json.dumps(workflow.freeze().as_tuple()), ex=ttl_seconds)
    try:
        workflow.apply_async()
    except Exception:
        try:
            redis.delete(key)
        except RedisError as exc:
            logger.warning("could not remove tracking key %s after a failed publish: %s", key, exc)
        raise


def load_status(order_id: str, redis: Redis) -> OrderStatus | None:
    raw = redis.get(order_key(order_id))
    if raw is None:
        return None
    last = result_from_tuple(json.loads(raw), app=celery_app)
    results: list[AsyncResult] = []
    node: AsyncResult | None = last
    while node is not None:
        results.append(node)
        node = node.parent
    results.reverse()
    steps = [Step(task=name, task_id=r.id, state=r.state) for name, r in zip(CHAIN_STEPS, results, strict=True)]
    return summarize(order_id, steps, results)


def summarize(order_id: str, steps: list[Step], results: list[AsyncResult]) -> OrderStatus:
    """Derive the global status from the per-step Celery states."""
    for step, result in zip(steps, results, strict=True):
        if step.state == "FAILURE":
            # Celery also marks the links after the failing one as FAILURE: the first one is the cause.
            return OrderStatus(
                order_id=order_id, status="FAILURE", steps=steps, error=describe_error(result.result)
            )
    if steps[-1].state == "SUCCESS":
        return OrderStatus(order_id=order_id, status="SUCCESS", steps=steps, result=results[-1].result)
    if all(step.state == "PENDING" for step in steps):
        return OrderStatus(order_id=order_id, status="PENDING", steps=steps)
    return OrderStatus(order_id=order_id, status="PROCESSING", steps=steps)


def describe_error(exc: object) -> str:
    # The exception class only exists in the worker; Celery rebuilds one with the same name.
    if isinstance(exc, BaseException):
        return f"{type(exc).__name__}: {exc}"
    return str(exc)


def utcnow() -> datetime:
    return datetime.now(UTC)
