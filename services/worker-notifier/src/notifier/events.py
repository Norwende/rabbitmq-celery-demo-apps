"""Events published to the RabbitMQ stream (contract section 4): order events and digests."""

import hashlib
from datetime import UTC, datetime

from kombu import Exchange, Producer, Queue

ORDERS_EXCHANGE = Exchange("orders", type="topic", durable=True)
# Declared idempotently on every publish: same arguments as the subscribers' declaration.
EVENTS_STREAM = Queue(
    "orders.events",
    exchange=ORDERS_EXCHANGE,
    routing_key="order.#",
    durable=True,
    queue_arguments={"x-queue-type": "stream", "x-max-age": "7D"},
)

ORDER_CONFIRMED = "order.confirmed"
ORDER_FAILED = "order.failed"
ORDER_DIGEST = "order.digest"
# A digest is identified by the minute of its tick, the finest crontab granularity.
PERIOD_FORMAT = "%Y-%m-%dT%H:%MZ"

# kombu-level retries of the publish itself (reconnect, redeclare) before the task gives up
# and Celery's autoretry takes over.
PUBLISH_RETRY_POLICY = {"max_retries": 3, "interval_start": 0, "interval_step": 1, "interval_max": 3}


def event_id(subject: str, event_type: str) -> str:
    """Deterministic id so that subscribers can deduplicate redelivered events.

    ``subject`` is the ``order_id`` for order events, the ``period`` for digests.
    """
    return "evt_" + hashlib.sha256(f"{subject}:{event_type}".encode()).hexdigest()[:24]


def iso_utc(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_event(order: dict, event_type: str, occurred_at: datetime, reason: str | None = None) -> dict:
    return {
        "event_id": event_id(order["order_id"], event_type),
        "event_type": event_type,
        "occurred_at": iso_utc(occurred_at),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "total_amount": order["total_amount"],
        "currency": order["currency"],
        "reason": reason,
    }


def digest_period(moment: datetime) -> str:
    """The tick a digest belongs to: ``moment`` truncated to the minute, UTC."""
    return moment.astimezone(UTC).strftime(PERIOD_FORMAT)


def parse_period(value: str) -> str:
    """Validate and normalize an explicit ``period`` argument (``YYYY-MM-DDTHH:MMZ``)."""
    return datetime.strptime(value, PERIOD_FORMAT).strftime(PERIOD_FORMAT)


def build_digest_event(period: str, totals: dict[str, str], occurred_at: datetime) -> dict:
    """Digest of the analytics aggregates (hash ``analytics:orders``, read as strings).

    The id depends on the period only: a redelivered or duplicated tick publishes the same
    ``event_id`` (and ``message_id``) again.
    """
    return {
        "event_id": event_id(period, ORDER_DIGEST),
        "event_type": ORDER_DIGEST,
        "occurred_at": iso_utc(occurred_at),
        "period": period,
        "confirmed": int(totals.get("confirmed", 0)),
        "failed": int(totals.get("failed", 0)),
        "revenue": {
            field.removeprefix("revenue:"): round(float(value), 2)
            for field, value in sorted(totals.items())
            if field.startswith("revenue:")
        },
    }


def publish_event(producer: Producer, event: dict) -> None:
    """Publish with publisher confirms (the broker connection has ``confirm_publish``).

    ``publish`` returns once the broker confirmed the message; a nack or an exhausted
    retry raises, which fails the Celery task so it can be retried.
    """
    producer.publish(
        event,
        exchange=ORDERS_EXCHANGE,
        routing_key=event["event_type"],
        serializer="json",
        delivery_mode=2,
        message_id=event["event_id"],
        declare=[ORDERS_EXCHANGE, EVENTS_STREAM],
        retry=True,
        retry_policy=PUBLISH_RETRY_POLICY,
    )
