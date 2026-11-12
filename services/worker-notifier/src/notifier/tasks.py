"""Tasks ``notifications.order_confirmed``, ``notifications.order_failed`` and
``notifications.sales_digest``.

All publish an event to the stream from inside the task, on the worker's own broker
connection pool (``app.producer_or_acquire``). Delivery is at-least-once: if the task is
redelivered after a successful publish, the same ``event_id`` is published again and
subscribers deduplicate on it.
"""

from datetime import UTC, datetime
from functools import cache

from amqp.exceptions import MessageNacked
from celery.utils.log import get_task_logger
from kombu.exceptions import OperationalError
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from notifier.celery_app import app
from notifier.config import settings
from notifier.events import (
    ORDER_CONFIRMED,
    ORDER_FAILED,
    build_digest_event,
    build_event,
    digest_period,
    parse_period,
    publish_event,
)
from notifier.redis_client import connect_redis

logger = get_task_logger(__name__)

# Maintained by subscriber-analytics; read-only here (contract section 4).
ANALYTICS_KEY = "analytics:orders"
DIGEST_MAX_RETRIES = 3
DIGEST_RETRY_COUNTDOWN_SECONDS = 5

INFRA_ERRORS = (
    OperationalError,  # kombu gave up publishing (broker unreachable)
    MessageNacked,  # broker refused the message (publisher confirm nack)
    ConnectionError,
    TimeoutError,
    RedisConnectionError,  # reading the failed task's result
    RedisTimeoutError,
)
RETRY_OPTIONS = {
    "autoretry_for": INFRA_ERRORS,
    "retry_backoff": True,
    "retry_backoff_max": 60,
    "retry_jitter": True,
    "max_retries": 10,
}


def emit(order: dict, event_type: str, reason: str | None = None) -> dict:
    event = build_event(order, event_type, datetime.now(UTC), reason)
    with app.producer_or_acquire() as producer:
        publish_event(producer, event)
    logger.info("event published order_id=%s event_type=%s event_id=%s reason=%s",
                order["order_id"], event_type, event["event_id"], reason)
    return {"order_id": order["order_id"], "event_id": event["event_id"], "event_type": event_type}


def describe_failure(exc: object, state: str) -> str:
    # The exception class only exists in the failing worker: Celery rebuilds one with the
    # same name from the JSON result ({"exc_type", "exc_message", ...}).
    if isinstance(exc, BaseException):
        return f"{type(exc).__name__}: {exc}"
    return f"task ended in state {state}"


@app.task(name="notifications.order_confirmed", **RETRY_OPTIONS)
def order_confirmed(order: dict) -> dict:
    return emit(order, ORDER_CONFIRMED)


@app.task(name="notifications.order_failed", **RETRY_OPTIONS)
def order_failed(failed_task_id: str, *, order: dict) -> dict:
    """Errback of the order chain.

    The failing worker does not have this task registered, so Celery sends it by name with
    the failed task id as the only positional argument (plus the signature's kwargs).
    The reason is read from the failed task's result in the Redis backend, which Celery
    stores before calling the errbacks.
    """
    failed = app.AsyncResult(failed_task_id)
    reason = describe_failure(failed.result, failed.state)
    return emit(order, ORDER_FAILED, reason)


@cache
def analytics_redis() -> Redis:
    # Created on first use, i.e. inside the prefork child: never shared across a fork.
    return connect_redis(settings)


@app.task(name="notifications.sales_digest", bind=True, ignore_result=True, max_retries=DIGEST_MAX_RETRIES)
def sales_digest(self, period: str | None = None) -> dict:
    """Publish an ``order.digest`` event: a snapshot of the analytics aggregates.

    Sent by the scheduler (beat) without argument; ``period`` can be passed explicitly to
    re-emit the digest of a given minute (``YYYY-MM-DDTHH:MMZ``) with the same event_id.

    Which tick is this? Beat entries have static arguments, so beat cannot pass its tick
    time. The task derives it from the current time truncated to the minute. A duplicated
    or redelivered tick therefore gets the same period, hence the same event_id, as long
    as it runs in the minute it was sent in: the scheduler sets ``expires`` < 60 s, so an
    older message is dropped by RabbitMQ (message TTL) or discarded by the worker instead
    of being run. Trade-off: if beat itself were late by a whole minute, the digest would
    carry the minute it was actually sent in; for a snapshot of cumulative counters that
    is harmless, and it avoids a custom scheduler rewriting the arguments at send time.
    """
    period = parse_period(period) if period else digest_period(datetime.now(UTC))
    try:
        totals = analytics_redis().hgetall(ANALYTICS_KEY)
        event = build_digest_event(period, totals, datetime.now(UTC))
        with app.producer_or_acquire() as producer:
            publish_event(producer, event)
    except INFRA_ERRORS as exc:
        # The retry carries the period computed by the first attempt (same event_id).
        raise self.retry(args=(period,), exc=exc, countdown=DIGEST_RETRY_COUNTDOWN_SECONDS) from exc
    logger.info("digest published period=%s event_id=%s confirmed=%s failed=%s revenue=%s",
                period, event["event_id"], event["confirmed"], event["failed"], event["revenue"])
    return {"period": period, "event_id": event["event_id"], "event_type": event["event_type"]}
