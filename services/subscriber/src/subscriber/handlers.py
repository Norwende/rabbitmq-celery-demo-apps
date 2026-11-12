"""What each subscriber does with an event, selected by role (``SUBSCRIBER_ROLE``, default the name).

Event types (contract section 4): ``order.confirmed`` and ``order.failed`` (``OrderEvent``),
``order.digest`` (``DigestEvent``, published by the scheduled ``notifications.sales_digest``).
A type unknown to a role is logged at DEBUG and acknowledged, never an error: the contract
can gain event types without breaking older subscribers.
"""

import json
import logging
import sys
from collections.abc import Callable
from typing import Protocol

from redis import Redis

logger = logging.getLogger(__name__)


class Handler(Protocol):
    def __call__(self, event: dict, offset: int) -> None: ...


ORDER_EVENT_TYPES = frozenset({"order.confirmed", "order.failed"})
ORDER_DIGEST = "order.digest"


def format_revenue(revenue: dict[str, float | str]) -> str:
    return ", ".join(f"{currency}: {float(amount):.2f}" for currency, amount in sorted(revenue.items()))


# Only order events reach the script: other types (digests...) are not counted, so they do
# not need deduplication and stay out of the "seen" set.
# Aggregates are deduplicated on event_id rather than relying on the checkpoint alone:
# * the notifier is at-least-once, so the same event (same event_id) can be in the stream twice;
# * the checkpoint is written after the aggregate update, so a crash in between replays one event.
# The "seen" set and the counters are updated in one atomic script, so an event is counted
# exactly once whatever the replay. (Demo trade-off: the seen set is never trimmed.)
ANALYTICS_SCRIPT = """
if redis.call('SADD', KEYS[2], ARGV[1]) == 0 then
  return 0
end
if ARGV[2] == 'order.confirmed' then
  redis.call('HINCRBY', KEYS[1], 'confirmed', 1)
  redis.call('HINCRBYFLOAT', KEYS[1], 'revenue:' .. ARGV[3], ARGV[4])
elseif ARGV[2] == 'order.failed' then
  redis.call('HINCRBY', KEYS[1], 'failed', 1)
end
return 1
"""
ANALYTICS_KEY = "analytics:orders"
ANALYTICS_SEEN_KEY = "analytics:orders:seen"


class AnalyticsHandler:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.script = redis.register_script(ANALYTICS_SCRIPT)

    def __call__(self, event: dict, offset: int) -> None:
        if event.get("event_type") not in ORDER_EVENT_TYPES:
            logger.debug("analytics offset=%s event_type=%s ignored (not an order event)",
                         offset, event.get("event_type"))
            return
        counted = self.script(
            keys=[ANALYTICS_KEY, ANALYTICS_SEEN_KEY],
            args=[event["event_id"], event["event_type"], event["currency"], event["total_amount"]],
        )
        totals = self.redis.hgetall(ANALYTICS_KEY)
        logger.info(
            "analytics offset=%s order_id=%s event_type=%s %s confirmed=%s failed=%s revenue={%s}",
            offset, event["order_id"], event["event_type"],
            "counted" if counted else "duplicate-skipped",
            totals.get("confirmed", "0"), totals.get("failed", "0"),
            format_revenue({k.removeprefix("revenue:"): v for k, v in totals.items() if k.startswith("revenue:")}),
        )


def make_audit_handler() -> Handler:
    # One raw JSON document per line on stdout, without the log prefix, for log shippers.
    # Every event is journaled, whatever its type (an audit trail does not filter).
    audit_logger = logging.getLogger("subscriber.audit.events")
    audit_logger.propagate = False
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    audit_logger.handlers[:] = [handler]
    audit_logger.setLevel(logging.INFO)

    def audit(event: dict, offset: int) -> None:
        audit_logger.info(json.dumps({"stream_offset": offset, **event}, sort_keys=True))

    return audit


def live(event: dict, offset: int) -> None:
    event_type = event.get("event_type")
    if event_type in ORDER_EVENT_TYPES:
        logger.info("LIVE %s order_id=%s customer_id=%s amount=%s %s%s",
                    event_type, event["order_id"], event["customer_id"],
                    event["total_amount"], event["currency"],
                    f" reason={event['reason']!r}" if event.get("reason") else "")
    elif event_type == ORDER_DIGEST:
        logger.info("LIVE %s period=%s confirmed=%s failed=%s revenue={%s}",
                    event_type, event["period"], event["confirmed"], event["failed"],
                    format_revenue(event["revenue"]))
    else:
        logger.debug("live offset=%s event_type=%s ignored (unknown type)", offset, event_type)


def make_handler(role: str, redis_factory: Callable[[], Redis]) -> Handler:
    match role:
        case "analytics":
            return AnalyticsHandler(redis_factory())
        case "audit":
            return make_audit_handler()
        case "live":
            return live
    raise ValueError(f"unknown subscriber role {role!r}: expected analytics, audit or live")
