"""Settings built from the contract environment variables (docs/contracts.md sections 2 and 4)."""

import os
from dataclasses import dataclass
from urllib.parse import quote


@dataclass(frozen=True)
class Settings:
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_vhost: str
    rabbitmq_user: str
    rabbitmq_password: str
    redis_sentinels: tuple[tuple[str, int], ...]
    redis_master_name: str
    redis_password: str
    redis_db: int
    log_level: str
    subscriber_name: str
    subscriber_role: str
    stream_offset: str
    checkpoint_enabled: bool
    prefetch_count: int

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        name = env("SUBSCRIBER_NAME", "audit")
        return cls(
            rabbitmq_host=env("RABBITMQ_HOST", "localhost"),
            rabbitmq_port=int(env("RABBITMQ_PORT", "5672")),
            rabbitmq_vhost=env("RABBITMQ_VHOST", "/"),
            rabbitmq_user=env("RABBITMQ_USER", "guest"),
            rabbitmq_password=env("RABBITMQ_PASSWORD", "guest"),
            redis_sentinels=parse_sentinels(env("REDIS_SENTINELS", "localhost:26379")),
            redis_master_name=env("REDIS_MASTER_NAME", "orderflow"),
            redis_password=env("REDIS_PASSWORD", ""),
            redis_db=int(env("REDIS_DB", "0")),
            log_level=env("LOG_LEVEL", "INFO").upper(),
            subscriber_name=name,
            # The role defaults to the name; it lets e.g. "audit-replay" behave like "audit".
            subscriber_role=env("SUBSCRIBER_ROLE", name),
            stream_offset=env("STREAM_OFFSET", "next"),
            checkpoint_enabled=env("CHECKPOINT_ENABLED", "false").strip().lower() in {"1", "true", "yes"},
            prefetch_count=int(env("PREFETCH_COUNT", "50")),
        )

    @property
    def broker_url(self) -> str:
        user, password = quote(self.rabbitmq_user, safe=""), quote(self.rabbitmq_password, safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"


def parse_sentinels(value: str) -> tuple[tuple[str, int], ...]:
    """Parse ``host:port,host:port`` into ``((host, port), ...)``."""
    sentinels = []
    for item in filter(None, (part.strip() for part in value.split(","))):
        host, _, port = item.rpartition(":")
        sentinels.append((host, int(port)) if host else (item, 26379))
    return tuple(sentinels)
