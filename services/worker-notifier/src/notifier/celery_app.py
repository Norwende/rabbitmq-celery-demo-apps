"""Celery app of worker-notifier (consumes queue ``notifications``).

Queue declarations: every queue is a quorum queue bound to a topic exchange of the same
name, with the queue name as routing key. This applies to the queue we consume (default
queue) and to any queue another task would be routed to (``task_create_missing_queue_type``),
so declarations are identical in every service; a mismatch would be refused by RabbitMQ
with PRECONDITION_FAILED. The order events stream is declared in ``notifier.events``.
The topic exchange is required by Celery's native delayed delivery (retry countdowns on
quorum queues), which skips direct exchanges.
"""

import logging
import sys

from celery import Celery
from celery.app.log import TaskFormatter
from celery.signals import setup_logging

from notifier.config import settings
from notifier.probes import install_probes

QUEUE = "notifications"


def route_by_prefix(name: str, args, kwargs, options, task=None, **kw) -> dict | None:
    """Route ``<queue>.<action>`` to the queue named by the prefix (contract section 3)."""
    queue, sep, _ = name.partition(".")
    if not sep or queue == "celery":
        return None  # Celery built-in tasks keep the default routing.
    return {"queue": queue}


app = Celery("worker-notifier", include=["notifier.tasks"])
app.conf.update(
    broker_url=settings.broker_url,
    broker_transport_options={"confirm_publish": True},
    broker_connection_retry_on_startup=True,
    result_backend=settings.result_backend_url,
    result_backend_transport_options=settings.result_backend_transport_options,
    result_expires=86400,
    result_extended=True,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    result_accept_content=["json"],
    enable_utc=True,
    timezone="UTC",
    # Queue declarations: must stay identical across all services.
    task_default_queue=QUEUE,
    task_default_queue_type="quorum",
    task_default_exchange_type="topic",
    task_create_missing_queue_type="quorum",
    task_create_missing_queue_exchange_type="topic",
    task_routes=(route_by_prefix,),
    # At-least-once execution: tasks must be idempotent.
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    worker_cancel_long_running_tasks_on_connection_loss=True,
    task_track_started=True,
    task_soft_time_limit=20,
    task_time_limit=30,
    worker_concurrency=settings.worker_concurrency,
    worker_send_task_events=False,
    task_send_sent_event=False,
    worker_enable_remote_control=False,
)
install_probes(app)


@setup_logging.connect
def configure_logging(**kwargs) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        TaskFormatter("%(asctime)s %(levelname)-7s %(name)s [%(task_name)s %(task_id)s] %(message)s")
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level)
