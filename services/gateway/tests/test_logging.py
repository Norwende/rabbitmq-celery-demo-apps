"""Structured logging: JSON shape, request context, isolation between concurrent requests."""

import asyncio
import logging
import re
import threading
from datetime import datetime

import httpx2
import pytest
import structlog
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from gateway.logs import (
    RequestContextMiddleware,
    bind_request_context,
    configure_logging,
    redact,
)
from gateway.main import app

logger = structlog.get_logger("gateway.tests")


def test_event_is_one_json_object_with_iso_utc_timestamp_level_logger_and_fields(logs):
    logger.info("order_accepted", order_id="ord_1", total_amount=12.5, currency="EUR")

    (event,) = logs.events()
    assert event["event"] == "order_accepted"
    assert event["level"] == "info"
    assert event["logger"] == "gateway.tests"
    assert (event["order_id"], event["total_amount"], event["currency"]) == ("ord_1", 12.5, "EUR")
    timestamp = datetime.fromisoformat(event["timestamp"])
    assert timestamp.utcoffset().total_seconds() == 0
    assert len(logs.text.splitlines()) == 1


def test_stdlib_loggers_come_out_in_the_same_format(logs):
    logging.getLogger("kombu.connection").warning("connection lost, retrying in %s s", 2)

    (event,) = logs.events()
    assert event["event"] == "connection lost, retrying in 2 s"
    assert (event["logger"], event["level"]) == ("kombu.connection", "warning")
    assert "timestamp" in event


def test_exceptions_are_rendered_as_structured_fields(logs):
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("unexpected_error")
        logging.getLogger("uvicorn.error").exception("Exception in ASGI application")

    for event in logs.events():
        (exc,) = event["exception"]
        assert (exc["exc_type"], exc["exc_value"]) == ("ValueError", "boom")
        assert exc["frames"]


def test_url_credentials_are_masked_everywhere(logs):
    url = "amqp://orderflow:s3cret@rabbitmq:5672//"
    logger.warning("broker_error", url=url, nested={"urls": [url]})
    logging.getLogger("celery").error("Cannot connect to %s", url)

    assert "s3cret" not in logs.text
    assert logs.events("broker_error")[0]["url"] == "amqp://orderflow:***@rabbitmq:5672//"
    assert redact("sentinel://:pw@h:26379/0;sentinel://:pw@h2:26379/0") == (
        "sentinel://:***@h:26379/0;sentinel://:***@h2:26379/0"
    )


def test_level_is_honoured(logs):
    logs.configure("WARNING")
    logger.info("hidden")
    logging.getLogger("redis").info("hidden too")
    logger.warning("shown")
    assert [e["event"] for e in logs.events()] == ["shown"]


def test_console_format_is_human_readable(logs):
    logs.configure("INFO", "console")
    logger.info("order_accepted", order_id="ord_1")
    assert "order_accepted" in logs.text and "order_id=ord_1" in logs.text
    assert not logs.text.lstrip().startswith("{")


def test_unknown_format_is_refused():
    with pytest.raises(ValueError, match="LOG_FORMAT"):
        configure_logging("INFO", "xml")


def test_request_id_is_taken_from_the_header_and_echoed(logs):
    logs.configure("DEBUG")
    response = TestClient(app).get("/healthz", headers={"X-Request-ID": "req-42"})

    assert response.headers["X-Request-ID"] == "req-42"
    (access,) = logs.events("http_request")
    assert access["request_id"] == "req-42"
    assert (access["method"], access["path"], access["status"]) == ("GET", "/healthz", 200)
    assert isinstance(access["duration_ms"], float)


def test_request_id_is_generated_when_absent_or_malformed(logs):
    client = TestClient(app)
    generated = client.get("/healthz").headers["X-Request-ID"]
    malformed = client.get("/healthz", headers={"X-Request-ID": "bad id\twith spaces"}).headers["X-Request-ID"]
    for value in (generated, malformed):
        assert re.fullmatch(r"[0-9a-f]{32}", value)
    assert generated != malformed


def test_probe_paths_are_not_logged_at_info(logs):
    client = TestClient(app)
    client.get("/healthz")
    assert logs.events("http_request") == []

    logs.configure("DEBUG")
    client.get("/healthz")
    (access,) = logs.events("http_request")
    assert (access["level"], access["path"], access["status"]) == ("debug", "/healthz", 200)


def make_concurrency_app(barrier: threading.Barrier) -> FastAPI:
    probe = FastAPI()
    probe.add_middleware(RequestContextMiddleware)

    @probe.get("/work/{order_id}")
    def work(order_id: str, request: Request) -> dict:  # sync: runs in the threadpool
        seen_before = structlog.contextvars.get_contextvars()
        bind_request_context(request, order_id=order_id)
        barrier.wait(timeout=5)  # every request is in flight, each in its own thread
        logger.info("work_done")
        return {"thread": threading.get_ident(), "before": seen_before,
                "after": structlog.contextvars.get_contextvars()}

    return probe


def test_context_reaches_the_threadpool_and_does_not_leak_between_concurrent_requests(logs):
    count = 8
    probe = make_concurrency_app(threading.Barrier(count))

    async def run() -> list[httpx2.Response]:
        transport = httpx2.ASGITransport(app=probe)
        async with httpx2.AsyncClient(transport=transport, base_url="http://test") as client:
            return await asyncio.gather(*(
                client.get(f"/work/ord_{i}", headers={"X-Request-ID": f"req-{i}"}) for i in range(count)
            ))

    responses = asyncio.run(run())

    assert len({r.json()["thread"] for r in responses}) == count  # really concurrent threads
    for i, response in enumerate(responses):
        body = response.json()
        # bound by the middleware (event loop), visible in the handler thread
        assert body["before"] == {"request_id": f"req-{i}", "method": "GET", "path": f"/work/ord_{i}"}
        assert body["after"]["order_id"] == f"ord_{i}"
        assert response.headers["X-Request-ID"] == f"req-{i}"
    for event in logs.events("work_done") + logs.events("http_request"):
        assert event["order_id"] == "ord_" + event["request_id"].removeprefix("req-")
    assert len(logs.events("http_request")) == count


def test_each_request_starts_from_a_clean_context():
    client = TestClient(make_concurrency_app(threading.Barrier(1)))
    client.get("/work/ord_a", headers={"X-Request-ID": "req-a"})
    body = client.get("/work/ord_b", headers={"X-Request-ID": "req-b"}).json()
    assert body["before"] == {"request_id": "req-b", "method": "GET", "path": "/work/ord_b"}
