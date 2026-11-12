import json
from unittest.mock import MagicMock

import pytest
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError

from subscriber.config import Settings
from subscriber.consumer import StreamSubscriber
from subscriber.handlers import make_handler
from subscriber.offsets import Checkpoint, parse_offset_spec, resolve_start_offset
from subscriber.redis_client import FailoverAwareConnection, connect_redis, redis_retry


@pytest.mark.parametrize(("value", "expected"), [("first", "first"), (" NEXT ", "next"), ("42", 42)])
def test_parse_offset_spec(value, expected):
    assert parse_offset_spec(value) == expected


@pytest.mark.parametrize("value", ["-1", "yesterday", ""])
def test_parse_offset_spec_rejects_invalid_values(value):
    with pytest.raises(ValueError):
        parse_offset_spec(value)


def test_resolve_start_offset_resumes_after_checkpoint():
    assert resolve_start_offset("first", 41) == 42
    assert resolve_start_offset("first", 0) == 1
    assert resolve_start_offset("next", None) == "next"
    assert resolve_start_offset(7, None) == 7


def test_checkpoint_key_and_roundtrip():
    redis = MagicMock()
    redis.get.return_value = "12"
    checkpoint = Checkpoint(redis, "orders.events", "analytics")
    assert checkpoint.key == "stream:orders.events:offset:analytics"
    assert checkpoint.load() == 12
    checkpoint.save(13)
    redis.set.assert_called_once_with("stream:orders.events:offset:analytics", 13)


def message(offset: int) -> MagicMock:
    msg = MagicMock()
    msg.headers = {"x-stream-offset": offset}
    return msg


def test_on_message_processes_checkpoints_then_acks_and_skips_already_processed_offsets():
    handler = MagicMock()
    checkpoint = MagicMock(spec=Checkpoint)
    checkpoint.load.return_value = 4
    subscriber = StreamSubscriber(MagicMock(), handler, "first", 10, checkpoint)

    replayed, fresh = message(4), message(5)
    subscriber.on_message({"event_id": "e4"}, replayed)  # chunk replay after re-attach
    subscriber.on_message({"event_id": "e5"}, fresh)

    handler.assert_called_once_with({"event_id": "e5"}, 5)
    checkpoint.save.assert_called_once_with(5)
    replayed.ack.assert_called_once()
    fresh.ack.assert_called_once()
    assert subscriber.last_offset == 5


def test_stream_consumer_uses_offset_argument_and_non_global_prefetch():
    subscriber = StreamSubscriber(MagicMock(), MagicMock(), "next", 25, None)
    Consumer = MagicMock()
    subscriber.get_consumers(Consumer, MagicMock())
    kwargs = Consumer.call_args.kwargs
    (queue,) = kwargs["queues"]
    assert queue.consumer_arguments == {"x-stream-offset": "next"}
    assert queue.queue_arguments == {"x-queue-type": "stream", "x-max-age": "7D"}
    assert kwargs["prefetch_count"] == 25
    assert kwargs["no_ack"] is False


def test_unknown_role_is_rejected():
    with pytest.raises(ValueError):
        make_handler("billing", MagicMock())


def test_redis_client_retries_failover_errors_with_a_bounded_total_wait():
    connection = connect_redis(Settings.from_env()).connection_pool.make_connection()
    assert isinstance(connection, FailoverAwareConnection)
    retry = connection.retry
    assert retry == redis_retry()
    assert {ConnectionError, TimeoutError, ReadOnlyError} <= set(retry._supported_errors)
    total = sum(retry._backoff.compute(failure) for failure in range(1, retry.get_retries() + 1))
    assert 4 <= total <= 8


def test_reconnection_inside_a_retry_asks_sentinel_for_the_current_master(monkeypatch):
    connection = connect_redis(Settings.from_env()).connection_pool.make_connection()
    connection.host = "demoted-master"
    monkeypatch.setattr(connection.connection_pool, "get_master_address", lambda: ("new-master", 6379))
    sent = []

    def fake_connect_to(self, address):
        self.host, self.port = address
        self._sock = object()

    monkeypatch.setattr(FailoverAwareConnection, "connect_to", fake_connect_to)
    monkeypatch.setattr("redis.connection.Connection.send_packed_command",
                        lambda self, command, check_health=True: sent.append((self.host, command)))
    connection.send_packed_command(b"SET k v")
    assert sent == [("new-master", b"SET k v")]


ORDER_EVENT = {"event_id": "evt_o", "event_type": "order.confirmed", "occurred_at": "2026-11-11T21:00:02Z",
               "order_id": "ord_1", "customer_id": "cust_42", "total_amount": 39.8, "currency": "EUR", "reason": None}
DIGEST_EVENT = {"event_id": "evt_d", "event_type": "order.digest", "occurred_at": "2026-11-11T21:05:02Z",
                "period": "2026-11-11T21:05Z", "confirmed": 61, "failed": 2, "revenue": {"EUR": 639.8}}
FUTURE_EVENT = {"event_id": "evt_f", "event_type": "order.refunded", "occurred_at": "2026-11-11T21:06:00Z"}


def analytics_handler():
    redis = MagicMock()
    redis.hgetall.return_value = {"confirmed": "1", "failed": "0", "revenue:EUR": "39.8"}
    return make_handler("analytics", lambda: redis), redis


def test_analytics_counts_order_events_through_the_dedup_script():
    handler, redis = analytics_handler()
    handler(ORDER_EVENT, 7)
    redis.register_script.return_value.assert_called_once_with(
        keys=["analytics:orders", "analytics:orders:seen"], args=["evt_o", "order.confirmed", "EUR", 39.8])


@pytest.mark.parametrize("event", [DIGEST_EVENT, FUTURE_EVENT])
def test_analytics_ignores_non_order_events_without_touching_redis(event):
    handler, redis = analytics_handler()
    handler(event, 8)
    redis.register_script.return_value.assert_not_called()
    redis.hgetall.assert_not_called()


def test_live_prints_a_readable_digest_line(caplog):
    caplog.set_level("INFO", logger="subscriber.handlers")
    make_handler("live", MagicMock())(DIGEST_EVENT, 9)
    assert caplog.messages == ["LIVE order.digest period=2026-11-11T21:05Z confirmed=61 failed=2 revenue={EUR: 639.80}"]


def test_live_logs_unknown_types_at_debug_only(caplog):
    caplog.set_level("DEBUG", logger="subscriber.handlers")
    make_handler("live", MagicMock())(FUTURE_EVENT, 10)
    assert [r.levelname for r in caplog.records] == ["DEBUG"]


@pytest.mark.parametrize("event", [ORDER_EVENT, DIGEST_EVENT, FUTURE_EVENT])
def test_audit_journals_every_event_as_json(event, capsys):
    make_handler("audit", MagicMock())(event, 11)
    assert json.loads(capsys.readouterr().out) == {"stream_offset": 11, **event}


def test_unknown_event_type_is_acked_and_checkpointed():
    checkpoint = MagicMock(spec=Checkpoint)
    checkpoint.load.return_value = None
    subscriber = StreamSubscriber(MagicMock(), make_handler("live", MagicMock()), "next", 10, checkpoint)
    msg = message(12)
    subscriber.on_message(FUTURE_EVENT, msg)
    msg.ack.assert_called_once()
    checkpoint.save.assert_called_once_with(12)
