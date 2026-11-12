"""Redis master client resolved through Sentinel (Sentinel itself is password protected).

A Sentinel failover (planned by the redis preStop, or after a crash) makes the master
unreachable for a few seconds. Every command is therefore retried by redis-py itself on
connection/timeout errors, and on ``ReadOnlyError`` (a demoted master that we are still
connected to). All commands the gateway sends (GET, SET, DEL, PING) are idempotent.
"""

from redis import Redis
from redis.backoff import ExponentialBackoff, NoBackoff
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
from redis.retry import Retry
from redis.sentinel import Sentinel, SentinelManagedConnection

from gateway.config import Settings

SOCKET_TIMEOUT_SECONDS = 2.0
RETRY_ERRORS = (ConnectionError, TimeoutError, ReadOnlyError)
# Back-off between attempts: 0.2, 0.4, 0.8, then 1 s -> at most 6.4 s of waiting in total
# (plus the attempts themselves), enough to cover a failover (~2-5 s here).
RETRY_ATTEMPTS = 8
RETRY_BACKOFF_BASE_SECONDS = 0.1
RETRY_BACKOFF_CAP_SECONDS = 1.0


def redis_retry() -> Retry:
    """Retry policy shared by every Sentinel-managed client of the gateway."""
    return Retry(
        ExponentialBackoff(cap=RETRY_BACKOFF_CAP_SECONDS, base=RETRY_BACKOFF_BASE_SECONDS),
        RETRY_ATTEMPTS,
        supported_errors=RETRY_ERRORS,
    )


class FailoverAwareConnection(SentinelManagedConnection):
    """Asks Sentinel for the current master on every reconnection, including retries.

    redis-py 8.1 reconnects inside a command's retry loop with ``connect_check_health()``,
    which reuses the previous host instead of asking Sentinel (only ``connect()`` does):
    every retry of a command went back to the demoted master (measured: 8 retries of
    "The previous master is now a slave" while Sentinel already announced the new one).
    One Sentinel-aware attempt here; the command-level ``Retry`` does the retrying.
    """

    def send_packed_command(self, command, check_health=True):
        if not self._sock and self.connection_pool.is_master:
            self.connect_to(self.connection_pool.get_master_address())
        super().send_packed_command(command, check_health)


def sentinel_kwargs(settings: Settings) -> dict:
    """Connection options for the sentinels themselves.

    ``discover_master()`` already tries each sentinel in turn: one fast attempt per
    sentinel, so that a dead one (e.g. in the pod being deleted) is skipped at once.
    """
    return {
        "password": settings.redis_password or None,
        "socket_timeout": SOCKET_TIMEOUT_SECONDS,
        "retry": Retry(NoBackoff(), 0),
    }


def master_connection_kwargs() -> dict:
    """Connection options for the Redis master (also used by the Celery result backend)."""
    return {
        "socket_timeout": SOCKET_TIMEOUT_SECONDS,
        "retry": redis_retry(),
        "connection_class": FailoverAwareConnection,
    }


def connect_redis(settings: Settings) -> Redis:
    sentinel = Sentinel(
        list(settings.redis_sentinels),
        sentinel_kwargs=sentinel_kwargs(settings),
        password=settings.redis_password or None,
        db=settings.redis_db,
        **master_connection_kwargs(),
    )
    return sentinel.master_for(settings.redis_master_name, decode_responses=True)
