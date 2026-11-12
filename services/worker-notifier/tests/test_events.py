from datetime import UTC, datetime
from unittest.mock import MagicMock

from notifier.events import (
    EVENTS_STREAM,
    ORDERS_EXCHANGE,
    build_event,
    event_id,
    publish_event,
)
from notifier.tasks import describe_failure

ORDER = {"order_id": "ord_1", "customer_id": "cust_42", "currency": "EUR", "total_amount": 39.8,
         "items": [], "created_at": "2026-11-11T21:00:00Z"}
NOW = datetime(2026, 11, 11, 21, 0, 2, tzinfo=UTC)


def test_event_id_is_deterministic_per_order_and_type():
    assert event_id("ord_1", "order.confirmed") == event_id("ord_1", "order.confirmed")
    assert event_id("ord_1", "order.confirmed") != event_id("ord_1", "order.failed")
    assert event_id("ord_1", "order.failed") != event_id("ord_2", "order.failed")


def test_build_event_matches_contract():
    event = build_event(ORDER, "order.failed", NOW, "OutOfStock: sku OUT-OF-STOCK unavailable")
    assert event == {
        "event_id": event_id("ord_1", "order.failed"),
        "event_type": "order.failed",
        "occurred_at": "2026-11-11T21:00:02Z",
        "order_id": "ord_1",
        "customer_id": "cust_42",
        "total_amount": 39.8,
        "currency": "EUR",
        "reason": "OutOfStock: sku OUT-OF-STOCK unavailable",
    }


def test_publish_declares_stream_and_uses_event_type_as_routing_key():
    producer = MagicMock()
    event = build_event(ORDER, "order.confirmed", NOW)
    publish_event(producer, event)
    kwargs = producer.publish.call_args.kwargs
    assert kwargs["routing_key"] == "order.confirmed"
    assert kwargs["retry"] is True
    assert kwargs["declare"] == [ORDERS_EXCHANGE, EVENTS_STREAM]
    assert EVENTS_STREAM.queue_arguments == {"x-queue-type": "stream", "x-max-age": "7D"}
    assert (EVENTS_STREAM.routing_key, ORDERS_EXCHANGE.type) == ("order.#", "topic")


def test_describe_failure_from_rebuilt_exception():
    PaymentDeclined = type("PaymentDeclined", (Exception,), {})
    assert describe_failure(PaymentDeclined("amount exceeds limit"), "FAILURE") == \
        "PaymentDeclined: amount exceeds limit"
    assert describe_failure(None, "PENDING") == "task ended in state PENDING"


def test_failed_task_result_is_rebuilt_without_importing_the_worker_module():
    from notifier.celery_app import app

    exc = app.backend.exception_to_python(
        {"exc_type": "PaymentDeclined", "exc_module": "payments.tasks", "exc_message": ["amount exceeds limit"]}
    )
    assert describe_failure(exc, "FAILURE") == "PaymentDeclined: amount exceeds limit"
