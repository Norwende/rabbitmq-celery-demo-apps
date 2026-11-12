"""Settings built from the contract environment variables (docs/contracts.md section 2)."""

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
    log_format: str
    order_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
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
            log_format=env("LOG_FORMAT", "json").lower(),
            order_ttl_seconds=int(env("ORDER_TTL_SECONDS", "86400")),
        )

    @property
    def broker_url(self) -> str:
        user, password = quote(self.rabbitmq_user, safe=""), quote(self.rabbitmq_password, safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"

    @property
    def result_backend_url(self) -> str:
        auth = f":{quote(self.redis_password, safe='')}@" if self.redis_password else ""
        return ";".join(
            f"sentinel://{auth}{host}:{port}/{self.redis_db}" for host, port in self.redis_sentinels
        )


def parse_sentinels(value: str) -> tuple[tuple[str, int], ...]:
    """Parse ``host:port,host:port`` into ``((host, port), ...)``."""
    sentinels = []
    for item in filter(None, (part.strip() for part in value.split(","))):
        host, _, port = item.rpartition(":")
        sentinels.append((host, int(port)) if host else (item, 26379))
    return tuple(sentinels)


settings = Settings.from_env()
