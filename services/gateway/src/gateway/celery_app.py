"""Celery app used as a pure producer: no task is registered here.

Tasks are referenced only by name. Every queue this app publishes to is created on the
fly by Celery (``task_create_missing_queues``) with the same declaration as in the
workers: quorum queue, bound to a topic exchange of the same name. Declaring a queue
with different arguments than an existing one makes RabbitMQ answer PRECONDITION_FAILED,
so the queue settings block below is identical in every service.
"""

from celery import Celery
from celery.backends.redis import SentinelBackend

from gateway.config import settings
from gateway.redis_client import master_connection_kwargs, sentinel_kwargs


def route_by_prefix(name: str, args, kwargs, options, task=None, **kw) -> dict | None:
    """Route ``<queue>.<action>`` to the queue named by the prefix (contract section 3)."""
    queue, sep, _ = name.partition(".")
    if not sep or queue == "celery":
        return None  # Celery built-in tasks keep the default routing.
    return {"queue": queue}


class GatewayResultBackend(SentinelBackend):
    """Sentinel result backend for a producer that only *reads* task states.

    * ``on_task_call`` is a no-op: the stock Redis backend subscribes to the result pub/sub
      channel of every task it sends (for ``AsyncResult.get()``), which the gateway never
      calls. That subscription would add a Redis round trip, and a failure mode, to every
      publish, and give up with a ``RuntimeError`` during a Sentinel failover.
    * its Redis connections are configured like the gateway's own client (retry policy,
      Sentinel-aware reconnection, timeouts).
    """

    def on_task_call(self, producer, task_id):
        pass

    def _get_sentinel_instance(self, **params):
        return super()._get_sentinel_instance(**{**params, **master_connection_kwargs()})


app = Celery("gateway")
app.conf.update(
    broker_url=settings.broker_url,
    broker_transport_options={"confirm_publish": True},
    broker_connection_retry_on_startup=True,
    # Publishing survives the loss of the RabbitMQ node behind the pooled connection:
    # reconnect (through the Service) and retry for ~6 s (0, 0.5, 1, 1.5, 1.5, 1.5 s).
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 6, "interval_start": 0, "interval_step": 0.5, "interval_max": 1.5,
    },
    result_backend=f"{__name__}:GatewayResultBackend+{settings.result_backend_url}",
    result_backend_transport_options={
        "master_name": settings.redis_master_name,
        "sentinel_kwargs": sentinel_kwargs(settings),
    },
    result_expires=settings.order_ttl_seconds,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_accept_content=["json"],
    enable_utc=True,
    timezone="UTC",
    # Queue declarations: must stay identical across all services.
    task_default_queue_type="quorum",
    task_default_exchange_type="topic",
    task_create_missing_queue_type="quorum",
    task_create_missing_queue_exchange_type="topic",
    task_routes=(route_by_prefix,),
    task_send_sent_event=False,
)
