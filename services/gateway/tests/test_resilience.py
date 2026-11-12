"""Failure handling around Redis and the broker (no broker or Redis needed)."""

import json

import pytest
from celery.result import result_from_tuple
from fastapi.testclient import TestClient
from kombu.exceptions import OperationalError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gateway.celery_app import app as celery_app
from gateway.config import Settings
from gateway.main import app, get_redis
from gateway.orders import CHAIN_STEPS, order_key, submit_order
from gateway.redis_client import (
    RETRY_ERRORS,
    FailoverAwareConnection,
    connect_redis,
    redis_retry,
)

ORDER = {"order_id": "ord_test", "customer_id": "c", "currency": "EUR", "items": [], "total_amount": 1.0}
BODY = {"customer_id": "c", "items": [{"sku": "SKU-1", "quantity": 1, "unit_price": "9.99"}]}


class FakeRedis:
    def __init__(self, fail_on: set[str] = frozenset(), error: Exception | None = None) -> None:
        self.data: dict[str, str] = {}
        self.fail_on = fail_on
        self.error = error or RedisConnectionError("master down")

    def _maybe_fail(self, command: str) -> None:
        if command in self.fail_on:
            raise self.error

    def set(self, key, value, ex=None):
        self._maybe_fail("set")
        self.data[key] = value

    def get(self, key):
        self._maybe_fail("get")
        return self.data.get(key)

    def delete(self, key):
        self._maybe_fail("delete")
        self.data.pop(key, None)


class PublishSpy:
    """Stands in for ``Celery.send_task`` (the chain's first link is sent by name)."""

    def __init__(self, redis: FakeRedis, error: Exception | None = None) -> None:
        self.redis, self.error, self.calls = redis, error, []

    def __call__(self, name, args=None, kwargs=None, **options):
        # Snapshot of the tracking key at publish time.
        self.calls.append({"name": name, "options": options, "stored": dict(self.redis.data)})
        if self.error:
            raise self.error


@pytest.fixture
def redis():
    return FakeRedis()


@pytest.fixture
def publish(monkeypatch, redis):
    spy = PublishSpy(redis)
    monkeypatch.setattr(celery_app, "send_task", spy)
    return spy


def stored_ids(raw: str) -> list[str]:
    node, ids = result_from_tuple(json.loads(raw), app=celery_app), []
    while node is not None:
        ids.append(node.id)
        node = node.parent
    return ids[::-1]


def test_tracking_key_is_stored_before_publishing_with_the_ids_actually_published(redis, publish):
    submit_order(ORDER, redis, ttl_seconds=60)

    (call,) = publish.calls
    key = order_key(ORDER["order_id"])
    assert key in call["stored"], "the key must exist before the chain is published"
    # Ids used by apply_async: first link's task_id, then the remaining links (the chain
    # option lists them last-first).
    published = [call["options"]["task_id"]] + [s["options"]["task_id"] for s in reversed(call["options"]["chain"])]
    assert [call["name"]] + [s["task"] for s in reversed(call["options"]["chain"])] == list(CHAIN_STEPS)
    assert stored_ids(call["stored"][key]) == published
    assert len(set(published)) == 3


@pytest.mark.parametrize("error", [RedisConnectionError("down"), RedisTimeoutError("slow")])
def test_nothing_is_published_when_the_tracking_key_cannot_be_stored(publish, error):
    redis = FakeRedis(fail_on={"set"}, error=error)
    with pytest.raises(type(error)):
        submit_order(ORDER, redis, ttl_seconds=60)
    assert publish.calls == []


def test_tracking_key_is_removed_when_publishing_fails(monkeypatch, redis):
    monkeypatch.setattr(celery_app, "send_task", PublishSpy(redis, error=OperationalError("broker down")))
    with pytest.raises(OperationalError):
        submit_order(ORDER, redis, ttl_seconds=60)
    assert redis.data == {}


def test_publish_error_is_kept_when_the_key_cleanup_also_fails(monkeypatch):
    redis = FakeRedis(fail_on={"delete"})
    monkeypatch.setattr(celery_app, "send_task", PublishSpy(redis, error=OperationalError("broker down")))
    with pytest.raises(OperationalError):
        submit_order(ORDER, redis, ttl_seconds=60)


@pytest.fixture
def client_with(monkeypatch):
    def make(redis: FakeRedis, publish_error: Exception | None = None) -> TestClient:
        monkeypatch.setattr(celery_app, "send_task", PublishSpy(redis, error=publish_error))
        app.dependency_overrides[get_redis] = lambda: redis
        return TestClient(app)

    yield make
    app.dependency_overrides.clear()


def assert_503(response, dependency: str) -> None:
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert response.json() == {"detail": f"{dependency} temporarily unavailable, retry later"}


def test_post_order_returns_202_and_the_order_is_trackable(client_with, redis, logs):
    response = client_with(redis).post("/orders", json=BODY, headers={"X-Request-ID": "req-1"})
    assert response.status_code == 202
    order_id = response.json()["order_id"]
    assert order_key(order_id) in redis.data

    (accepted,) = logs.events("order_accepted")
    assert {k: accepted[k] for k in ("request_id", "method", "path", "order_id", "customer_id",
                                     "total_amount", "currency")} == {
        "request_id": "req-1", "method": "POST", "path": "/orders", "order_id": order_id,
        "customer_id": "c", "total_amount": 9.99, "currency": "EUR",
    }
    (access,) = logs.events("http_request")
    assert (access["status"], access["order_id"], access["request_id"]) == (202, order_id, "req-1")


@pytest.mark.parametrize("error", [RedisConnectionError("down"), RedisTimeoutError("slow"), ReadOnlyError("replica")])
def test_post_order_returns_503_when_redis_is_unavailable(client_with, error, logs):
    response = client_with(FakeRedis(fail_on={"set"}, error=error)).post("/orders", json=BODY)
    assert_503(response, "redis")
    # A single warning line, without traceback; the access event follows.
    (warning,) = [e for e in logs.events() if e["level"] == "warning"]
    assert {k: warning[k] for k in ("event", "dependency", "status", "error_type", "error", "method", "path")} == {
        "event": "dependency_unavailable", "dependency": "redis", "status": 503,
        "error_type": type(error).__name__, "error": str(error), "method": "POST", "path": "/orders",
    }
    assert "exception" not in warning
    assert [e["status"] for e in logs.events("http_request")] == [503]


def test_post_order_returns_503_and_leaves_no_key_when_the_broker_is_unavailable(client_with, redis):
    response = client_with(redis, publish_error=OperationalError("broker down")).post("/orders", json=BODY)
    assert_503(response, "broker")
    assert redis.data == {}


def test_get_order_returns_503_when_redis_is_unavailable(client_with):
    assert_503(client_with(FakeRedis(fail_on={"get"})).get("/orders/ord_x"), "redis")


def test_get_unknown_order_is_still_404(client_with, redis):
    assert client_with(redis).get("/orders/ord_x").status_code == 404


def test_retry_policy_covers_failover_errors_with_a_bounded_total_wait():
    retry = redis_retry()
    assert set(RETRY_ERRORS) <= set(retry._supported_errors)
    total = sum(retry._backoff.compute(failure) for failure in range(1, retry.get_retries() + 1))
    assert 4 <= total <= 8


def test_retry_reconnects_until_the_new_master_answers(monkeypatch):
    monkeypatch.setattr("redis.retry.sleep", lambda seconds: None)
    outcomes = [ReadOnlyError("demoted master"), RedisConnectionError("refused"), "OK"]
    disconnects = []

    def attempt():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    assert redis_retry().call_with_retry(attempt, disconnects.append) == "OK"
    assert len(disconnects) == 2


def test_gateway_and_result_backend_clients_share_the_failover_configuration():
    gateway_client = connect_redis(Settings.from_env())
    backend_client = celery_app.backend.client
    for client in (gateway_client, backend_client):
        connection = client.connection_pool.make_connection()
        assert isinstance(connection, FailoverAwareConnection)
        assert connection.retry == redis_retry()
        assert connection.socket_timeout == 2.0
        # Sentinels: a single fast attempt each (discover_master() tries them in turn).
        sentinel = client.connection_pool.sentinel_manager.sentinels[0]
        assert sentinel.connection_pool.connection_kwargs["retry"].get_retries() == 0


def test_reconnection_inside_a_retry_asks_sentinel_for_the_current_master(monkeypatch):
    connection = connect_redis(Settings.from_env()).connection_pool.make_connection()
    connection.host = "demoted-master"  # previous master, connection lost
    monkeypatch.setattr(connection.connection_pool, "get_master_address", lambda: ("new-master", 6379))
    connected, sent = [], []

    def fake_connect_to(self, address):
        self.host, self.port = address
        connected.append(address)
        self._sock = object()

    monkeypatch.setattr(FailoverAwareConnection, "connect_to", fake_connect_to)
    monkeypatch.setattr("redis.connection.Connection.send_packed_command",
                        lambda self, command, check_health=True: sent.append((self.host, command)))

    connection.send_packed_command(b"SET k v")

    assert connected == [("new-master", 6379)]
    assert sent == [("new-master", b"SET k v")]


def test_result_backend_does_not_subscribe_to_results_on_publish():
    # The stock Redis backend would open a pub/sub subscription here (needs Redis).
    assert celery_app.backend.on_task_call(None, "task-id") is None
