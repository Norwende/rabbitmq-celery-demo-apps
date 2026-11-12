"""Celery app of the scheduler: ``celery beat`` only, no task registered, no worker.

Beat sends the scheduled tasks **by name** (``send_task``): their code lives in the
workers. Every queue it publishes to is created on the fly by Celery with the same
declaration as in the workers (quorum queue bound to a topic exchange of the same name);
a mismatch would be refused by RabbitMQ with PRECONDITION_FAILED, so the queue settings
block below is identical in every service.

Schedule store: the default ``PersistentScheduler`` (a shelve file in ``/tmp``, an
emptyDir), wrapped to feed the liveness probe. The schedule is crontab-only and fully
defined here, so the file only remembers "last run" times:

* new pod (empty /tmp): every entry starts with ``last_run_at = now``, the first run is
  the next crontab slot, nothing is caught up;
* beat container restarted in the same pod (file kept): ``beat_cron_starting_deadline``
  drops a slot missed for more than 30 s instead of running it late.

A database-backed scheduler (django-celery-beat, RedBeat) would only be worth it for a
schedule edited at runtime, or for a lock between several beats: here there is exactly
one beat (Deployment with 1 replica, ``Recreate``).
"""

import logging
import sys

from celery import Celery
from celery.schedules import crontab
from celery.signals import setup_logging

from scheduler.config import Settings, settings

DIGEST_TASK = "notifications.sales_digest"
# The digest task identifies its tick by the current minute (contract section 3): it must
# never run in a later minute than the one it was sent in.
MAX_EXPIRES_SECONDS = 59


def route_by_prefix(name: str, args, kwargs, options, task=None, **kw) -> dict | None:
    """Route ``<queue>.<action>`` to the queue named by the prefix (contract section 3)."""
    queue, sep, _ = name.partition(".")
    if not sep or queue == "celery":
        return None  # Celery built-in tasks keep the default routing.
    return {"queue": queue}


def build_beat_schedule(config: Settings) -> dict:
    """Beat entries, by task name only.

    ``expires``: Celery sets it both as the AMQP per-message TTL (``expiration``: RabbitMQ
    drops a tick nobody consumed in time) and in the task headers (the worker discards a
    tick received, or redelivered, too late). The ticks missed while the notifier is down
    never pile up and run late in a burst; the next tick produces a fresh digest anyway.
    """
    if not 1 <= config.digest_expires_seconds <= MAX_EXPIRES_SECONDS:
        raise ValueError(
            f"DIGEST_EXPIRES_SECONDS={config.digest_expires_seconds}: expected 1..{MAX_EXPIRES_SECONDS}"
        )
    return {
        "sales-digest": {
            "task": DIGEST_TASK,
            "schedule": crontab.from_string(config.digest_cron),  # "m h dom mon dow", UTC
            "options": {"expires": config.digest_expires_seconds},
        },
    }


app = Celery("scheduler")
app.conf.update(
    broker_url=settings.broker_url,
    broker_transport_options={"confirm_publish": True},
    broker_connection_retry_on_startup=True,
    # Same publish retry policy as the gateway: reconnect (through the Service) and retry
    # for ~6 s if the RabbitMQ node behind the connection disappeared.
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 6, "interval_start": 0, "interval_step": 0.5, "interval_max": 1.5,
    },
    # No result backend: beat never reads results. result_expires=None also prevents beat
    # from adding its default "celery.backend_cleanup" entry (sent to a "celery" queue
    # nobody consumes).
    result_expires=None,
    task_ignore_result=True,
    task_serializer="json",
    accept_content=["json"],
    enable_utc=True,
    timezone="UTC",
    # Queue declarations: must stay identical across all services.
    task_default_queue_type="quorum",
    task_default_exchange_type="topic",
    task_create_missing_queue_type="quorum",
    task_create_missing_queue_exchange_type="topic",
    task_routes=(route_by_prefix,),
    task_send_sent_event=False,
    # Beat.
    beat_schedule=build_beat_schedule(settings),
    beat_scheduler="scheduler.probes:HeartbeatScheduler",
    beat_schedule_filename="/tmp/celerybeat-schedule",
    beat_max_loop_interval=10,  # tick (and heartbeat) at least every 10 s
    beat_cron_starting_deadline=30,
    beat_enable_remote_control=False,  # no pidbox queue on the broker
)


@setup_logging.connect
def configure_logging(**kwargs) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(settings.log_level)
