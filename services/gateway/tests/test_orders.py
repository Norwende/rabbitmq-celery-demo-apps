from datetime import UTC, datetime

import pytest
from celery.result import AsyncResult
from fastapi.testclient import TestClient

from gateway.celery_app import app as celery_app
from gateway.celery_app import route_by_prefix
from gateway.config import parse_sentinels
from gateway.main import app
from gateway.models import OrderRequest, Step
from gateway.orders import (
    CHAIN_STEPS,
    ERRBACK_TASK,
    build_order,
    build_workflow,
    summarize,
)


def make_order() -> dict:
    request = OrderRequest.model_validate(
        {
            "customer_id": "cust_42",
            "items": [
                {"sku": "SKU-BOOK-001", "quantity": 2, "unit_price": "19.90"},
                {"sku": "SKU-PEN-002", "quantity": 3, "unit_price": "0.10"},
            ],
        }
    )
    return build_order(request, datetime(2026, 11, 11, 21, 0, tzinfo=UTC))


def test_build_order_computes_total_and_defaults():
    order = make_order()
    assert order["total_amount"] == 40.10
    assert order["currency"] == "EUR"
    assert order["order_id"].startswith("ord_")
    assert order["created_at"] == "2026-11-11T21:00:00Z"
    assert order["items"][0] == {"sku": "SKU-BOOK-001", "quantity": 2, "unit_price": 19.9}


@pytest.mark.parametrize(
    ("name", "queue"),
    [("payments.authorize", "payments"), ("notifications.order_failed", "notifications")],
)
def test_route_by_prefix(name, queue):
    assert route_by_prefix(name, [], {}, {}) == {"queue": queue}


def test_route_by_prefix_ignores_builtin_and_unprefixed_tasks():
    assert route_by_prefix("celery.chord_unlock", [], {}, {}) is None
    assert route_by_prefix("ping", [], {}, {}) is None


def test_routed_queue_is_quorum_on_topic_exchange():
    queue = celery_app.amqp.router.route({}, "inventory.reserve")["queue"]
    assert queue.name == "inventory"
    assert queue.queue_arguments == {"x-queue-type": "quorum"}
    assert (queue.exchange.name, queue.exchange.type) == ("inventory", "topic")


def test_workflow_is_a_chain_by_name_with_errback_on_every_link():
    order = make_order()
    workflow = build_workflow(order)
    assert [task.task for task in workflow.tasks] == list(CHAIN_STEPS)
    assert workflow.tasks[0].args == (order,)
    # The chain copies its link_error onto every task when it is applied.
    for task in workflow.unchain_tasks():
        (errback,) = task.options["link_error"]
        assert errback["task"] == ERRBACK_TASK
        assert errback["kwargs"] == {"order": order}


def make_steps(states: list[str]) -> tuple[list[Step], list[AsyncResult]]:
    steps = [Step(task=name, task_id=f"id-{i}", state=state)
             for i, (name, state) in enumerate(zip(CHAIN_STEPS, states, strict=True))]
    return steps, [AsyncResult(step.task_id, app=celery_app) for step in steps]


def test_summarize_failure_reports_first_failed_step(monkeypatch):
    class PaymentDeclined(Exception):
        pass

    monkeypatch.setattr(AsyncResult, "result", property(lambda self: PaymentDeclined("amount exceeds limit")))
    status = summarize("ord_1", *make_steps(["FAILURE", "FAILURE", "FAILURE"]))
    assert status.status == "FAILURE"
    assert status.error == "PaymentDeclined: amount exceeds limit"


@pytest.mark.parametrize(
    ("states", "expected"),
    [
        (["PENDING", "PENDING", "PENDING"], "PENDING"),
        (["SUCCESS", "STARTED", "PENDING"], "PROCESSING"),
        (["SUCCESS", "RETRY", "PENDING"], "PROCESSING"),
    ],
)
def test_summarize_in_progress(states, expected):
    assert summarize("ord_1", *make_steps(states)).status == expected


def test_parse_sentinels():
    assert parse_sentinels("a:26379, b:26380,c") == (("a", 26379), ("b", 26380), ("c", 26379))


@pytest.mark.parametrize(
    "body",
    [
        {"customer_id": "c", "items": []},
        {"customer_id": "c", "items": [{"sku": "x", "quantity": 0, "unit_price": 1}]},
        {"customer_id": "c", "currency": "euro", "items": [{"sku": "x", "quantity": 1, "unit_price": 1}]},
    ],
)
def test_post_orders_rejects_invalid_payload(body):
    assert TestClient(app).post("/orders", json=body).status_code == 422
