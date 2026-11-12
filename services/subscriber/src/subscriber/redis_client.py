"""Redis master client resolved through Sentinel (Sentinel itself is password protected).

A Sentinel failover makes the master unreachable for a few seconds. Without retries the
checkpoint write or the analytics script would raise out of the consume loop and crash
the pod (safe thanks to the checkpoint and the event_id deduplication, but noisy). Every
command is therefore retried by redis-py on connection/timeout errors and on
``ReadOnlyError`` (a demoted master we are still connected to). The commands sent (GET/SET
of the checkpoint, the deduplicating analytics script, HGETALL) are all idempotent.
Same configuration as the gateway.
"""

from redis import Redis
from redis.backoff import ExponentialBackoff, NoBackoff
from redis.exceptions import ConnectionError, ReadOnlyError, TimeoutError
from redis.retry import Retry
from redis.sentinel import Sentinel, SentinelManagedConnection

from subscriber.config import Settings

SOCKET_TIMEOUT_SECONDS = 2.0
RETRY_ERRORS = (ConnectionError, TimeoutError, ReadOnlyError)
# Back-off between attempts: 0.2, 0.4, 0.8, then 1 s -> at most 6.4 s of waiting in total
# (plus the attempts themselves), enough to cover a failover (~2-5 s here).
RETRY_ATTEMPTS = 8
RETRY_BACKOFF_BASE_SECONDS = 0.1
RETRY_BACKOFF_CAP_SECONDS = 1.0


def redis_retry() -> Retry:
    return Retry(
        ExponentialBackoff(cap=RETRY_BACKOFF_CAP_SECONDS, base=RETRY_BACKOFF_BASE_SECONDS),
        RETRY_ATTEMPTS,
        supported_errors=RETRY_ERRORS,
    )


class FailoverAwareConnection(SentinelManagedConnection):
    """Asks Sentinel for the current master on every reconnection, including retries.

    redis-py 8.1 reconnects inside a command's retry loop with ``connect_check_health()``,
    which reuses the previous host instead of asking Sentinel (only ``connect()`` does):
    every retry would go back to the demoted master. One Sentinel-aware attempt here; the
    command-level ``Retry`` does the retrying.
    """

    def send_packed_command(self, command, check_health=True):
        if not self._sock and self.connection_pool.is_master:
            self.connect_to(self.connection_pool.get_master_address())
        super().send_packed_command(command, check_health)


def connect_redis(settings: Settings) -> Redis:
    password = settings.redis_password or None
    sentinel = Sentinel(
        list(settings.redis_sentinels),
        # discover_master() tries each sentinel in turn: one fast attempt per sentinel.
        sentinel_kwargs={"password": password, "socket_timeout": SOCKET_TIMEOUT_SECONDS,
                         "retry": Retry(NoBackoff(), 0)},
        socket_timeout=SOCKET_TIMEOUT_SECONDS,
        password=password,
        db=settings.redis_db,
        retry=redis_retry(),
        connection_class=FailoverAwareConnection,
    )
    return sentinel.master_for(settings.redis_master_name, decode_responses=True)
