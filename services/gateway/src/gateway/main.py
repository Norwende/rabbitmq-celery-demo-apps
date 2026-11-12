"""FastAPI application (contract section 5)."""

import logging
from functools import lru_cache
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from kombu.exceptions import OperationalError as BrokerUnavailable
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError
from redis.exceptions import TimeoutError as RedisTimeoutError

from gateway.celery_app import app as celery_app
from gateway.config import settings
from gateway.models import OrderAccepted, OrderRequest, OrderStatus
from gateway.orders import build_order, load_status, submit_order, utcnow
from gateway.redis_client import connect_redis

logger = logging.getLogger(__name__)

app = FastAPI(title="OrderFlow gateway", version="0.1.0")

RETRY_AFTER_SECONDS = 5


def unavailable(dependency: str):
    """Exception handler: an infrastructure dependency is still down after the client retries."""

    def handler(request: Request, exc: Exception) -> JSONResponse:
        logger.warning("%s %s -> 503: %s unavailable (%s: %s)",
                       request.method, request.url.path, dependency, type(exc).__name__, exc)
        return JSONResponse(
            {"detail": f"{dependency} temporarily unavailable, retry later"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": str(RETRY_AFTER_SECONDS)},
        )

    return handler


for _redis_error in (RedisConnectionError, RedisTimeoutError, ReadOnlyError):
    app.add_exception_handler(_redis_error, unavailable("redis"))
# Celery re-raises broker connection errors as kombu's OperationalError.
app.add_exception_handler(BrokerUnavailable, unavailable("broker"))


@lru_cache(maxsize=1)
def get_redis() -> Redis:
    return connect_redis(settings)


RedisDep = Annotated[Redis, Depends(get_redis)]


# Sync handlers run in FastAPI's threadpool: the Celery producer and redis-py are blocking.
@app.post("/orders", status_code=status.HTTP_202_ACCEPTED, response_model=OrderAccepted)
def create_order(body: OrderRequest, request: Request, redis: RedisDep) -> OrderAccepted:
    order = build_order(body, utcnow())
    submit_order(order, redis, settings.order_ttl_seconds)
    logger.info(
        "order accepted order_id=%s customer_id=%s total_amount=%s %s",
        order["order_id"], order["customer_id"], order["total_amount"], order["currency"],
    )
    status_url = str(request.url_for("get_order", order_id=order["order_id"]))
    return OrderAccepted(order_id=order["order_id"], status_url=status_url)


@app.get("/orders/{order_id}", response_model=OrderStatus)
def get_order(order_id: str, redis: RedisDep) -> OrderStatus:
    order_status = load_status(order_id, redis)
    if order_status is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="order not found")
    return order_status


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.get("/readyz")
def readyz(response: Response, redis: RedisDep) -> dict:
    checks = {"broker": "ok", "redis": "ok"}
    try:
        # Pooled connection: probes do not open a new AMQP connection every time.
        with celery_app.connection_or_acquire() as conn:
            conn.ensure_connection(max_retries=1)
    except Exception as exc:  # noqa: BLE001 - any failure means "not ready"
        checks["broker"] = f"error: {exc}"
    try:
        redis.ping()
    except Exception as exc:  # noqa: BLE001
        checks["redis"] = f"error: {exc}"
    if any(value != "ok" for value in checks.values()):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return checks
