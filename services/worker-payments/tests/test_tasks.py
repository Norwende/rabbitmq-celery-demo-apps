from datetime import UTC, datetime

import pytest

from payments.celery_app import app, route_by_prefix
from payments.tasks import PaymentDeclined, authorize_payment

NOW = datetime(2026, 11, 11, 21, 0, 1, tzinfo=UTC)


def order(total: float) -> dict:
    return {"order_id": "ord_1", "customer_id": "cust_42", "currency": "EUR",
            "items": [], "total_amount": total, "created_at": "2026-11-11T21:00:00Z"}


def test_authorize_enriches_order_with_deterministic_authorization():
    first = authorize_payment(order(1000.0), NOW)
    second = authorize_payment(order(1000.0), NOW)
    assert first["payment"] == second["payment"]
    assert first["payment"]["status"] == "authorized"
    assert first["payment"]["authorization_id"].startswith("auth_")
    assert first["payment"]["authorized_at"] == "2026-11-11T21:00:01Z"
    assert first["total_amount"] == 1000.0


def test_amount_above_limit_is_declined():
    with pytest.raises(PaymentDeclined, match="amount exceeds limit"):
        authorize_payment(order(1000.01), NOW)


def test_business_error_is_not_retried():
    task = app.tasks["payments.authorize"]
    assert PaymentDeclined not in task.autoretry_for
    assert ConnectionError in task.autoretry_for


@pytest.mark.parametrize("name", ["inventory.reserve", "notifications.order_failed", "payments.authorize"])
def test_every_routed_queue_is_quorum_on_topic_exchange(name):
    queue = app.amqp.router.route({}, name)["queue"]
    assert queue.name == route_by_prefix(name, [], {}, {})["queue"]
    assert queue.queue_arguments == {"x-queue-type": "quorum"}
    assert (queue.exchange.name, queue.exchange.type, queue.routing_key) == (queue.name, "topic", queue.name)
