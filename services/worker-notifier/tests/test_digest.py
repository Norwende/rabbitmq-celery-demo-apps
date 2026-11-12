from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from notifier import tasks
from notifier.events import build_digest_event, digest_period, event_id, parse_period

TOTALS = {"confirmed": "61", "failed": "2", "revenue:EUR": "639.79999999999999999", "revenue:USD": "10"}
NOW = datetime(2026, 11, 11, 21, 5, 2, tzinfo=UTC)


def test_period_is_the_minute_of_the_tick_in_utc():
    assert digest_period(NOW) == "2026-11-11T21:05Z"
    assert digest_period(NOW + timedelta(seconds=57)) == "2026-11-11T21:05Z"
    assert digest_period(NOW.astimezone(timezone(timedelta(hours=1)))) == "2026-11-11T21:05Z"
    assert digest_period(NOW + timedelta(minutes=1)) == "2026-11-11T21:06Z"


def test_explicit_period_is_validated():
    assert parse_period("2026-11-11T21:05Z") == "2026-11-11T21:05Z"
    with pytest.raises(ValueError):
        parse_period("2026-11-11 21:05")


def test_digest_event_matches_contract():
    event = build_digest_event("2026-11-11T21:05Z", TOTALS, NOW)
    assert event == {
        "event_id": event_id("2026-11-11T21:05Z", "order.digest"),
        "event_type": "order.digest",
        "occurred_at": "2026-11-11T21:05:02Z",
        "period": "2026-11-11T21:05Z",
        "confirmed": 61,
        "failed": 2,
        "revenue": {"EUR": 639.8, "USD": 10.0},
    }


def test_digest_id_depends_on_the_period_only():
    first = build_digest_event("2026-11-11T21:05Z", TOTALS, NOW)
    redelivered = build_digest_event("2026-11-11T21:05Z", {"confirmed": "62"}, NOW + timedelta(seconds=30))
    next_tick = build_digest_event("2026-11-11T21:06Z", TOTALS, NOW + timedelta(minutes=1))
    assert first["event_id"] == redelivered["event_id"]
    assert first["event_id"] != next_tick["event_id"]
    assert first["event_id"] != event_id("2026-11-11T21:05Z", "order.confirmed")


def test_empty_aggregates_give_zero_counters():
    event = build_digest_event("2026-11-11T21:05Z", {}, NOW)
    assert (event["confirmed"], event["failed"], event["revenue"]) == (0, 0, {})


@pytest.fixture
def published(monkeypatch):
    producer = MagicMock()

    @contextmanager
    def producer_or_acquire():
        yield producer

    monkeypatch.setattr(tasks.app, "producer_or_acquire", producer_or_acquire)
    return producer


def test_sales_digest_reads_the_aggregates_and_publishes_order_digest(monkeypatch, published):
    redis = MagicMock()
    redis.hgetall.return_value = TOTALS
    monkeypatch.setattr(tasks, "analytics_redis", lambda: redis)

    result = tasks.sales_digest("2026-11-11T21:05Z")

    redis.hgetall.assert_called_once_with("analytics:orders")
    kwargs = published.publish.call_args.kwargs
    assert kwargs["routing_key"] == "order.digest"
    assert kwargs["message_id"] == event_id("2026-11-11T21:05Z", "order.digest")
    body = published.publish.call_args.args[0]
    assert (body["period"], body["confirmed"], body["revenue"]) == ("2026-11-11T21:05Z", 61, {"EUR": 639.8, "USD": 10.0})
    assert result == {"period": "2026-11-11T21:05Z", "event_id": body["event_id"], "event_type": "order.digest"}


def test_sales_digest_without_argument_uses_the_current_minute(monkeypatch, published):
    redis = MagicMock()
    redis.hgetall.return_value = {}
    monkeypatch.setattr(tasks, "analytics_redis", lambda: redis)
    result = tasks.sales_digest()
    assert result["period"] == digest_period(datetime.now(UTC))


def test_sales_digest_retries_with_the_period_of_the_first_attempt(monkeypatch, published):
    redis = MagicMock()
    redis.hgetall.side_effect = RedisConnectionError("failover")
    monkeypatch.setattr(tasks, "analytics_redis", lambda: redis)
    retry = MagicMock(side_effect=RuntimeError("retry scheduled"))
    monkeypatch.setattr(tasks.sales_digest, "retry", retry)

    with pytest.raises(RuntimeError, match="retry scheduled"):
        tasks.sales_digest("2026-11-11T21:05Z")
    assert retry.call_args.kwargs["args"] == ("2026-11-11T21:05Z",)
    published.publish.assert_not_called()


def test_sales_digest_is_registered_by_its_contract_name():
    task = tasks.app.tasks["notifications.sales_digest"]
    assert task.ignore_result is True
    assert tasks.app.amqp.router.route({}, "notifications.sales_digest")["queue"].name == "notifications"
