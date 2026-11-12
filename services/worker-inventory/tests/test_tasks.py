from datetime import UTC, datetime

import pytest

from inventory.celery_app import app
from inventory.tasks import OutOfStock, reserve_stock

NOW = datetime(2026, 11, 11, 21, 0, 2, tzinfo=UTC)


def order(*skus: str) -> dict:
    return {"order_id": "ord_1", "customer_id": "cust_42", "currency": "EUR", "total_amount": 10.0,
            "items": [{"sku": sku, "quantity": 1, "unit_price": 5.0} for sku in skus],
            "payment": {"authorization_id": "auth_x", "status": "authorized"}}


def test_reserve_keeps_payment_and_adds_deterministic_reservation():
    first = reserve_stock(order("SKU-BOOK-001"), NOW)
    assert first == reserve_stock(order("SKU-BOOK-001"), NOW)
    assert first["payment"]["authorization_id"] == "auth_x"
    assert first["reservation"]["status"] == "reserved"
    assert first["reservation"]["reservation_id"].startswith("res_")
    assert first["reservation"]["reserved_at"] == "2026-11-11T21:00:02Z"


def test_out_of_stock_sku_is_refused():
    with pytest.raises(OutOfStock, match="OUT-OF-STOCK"):
        reserve_stock(order("SKU-BOOK-001", "OUT-OF-STOCK"), NOW)


def test_business_error_is_not_retried():
    assert OutOfStock not in app.tasks["inventory.reserve"].autoretry_for


def test_published_queue_is_quorum_on_topic_exchange():
    queue = app.amqp.router.route({}, "notifications.order_confirmed")["queue"]
    assert queue.name == "notifications"
    assert queue.queue_arguments == {"x-queue-type": "quorum"}
    assert queue.exchange.type == "topic"
