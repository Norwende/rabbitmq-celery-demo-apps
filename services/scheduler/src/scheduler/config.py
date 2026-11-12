"""Settings built from the contract environment variables (docs/contracts.md section 2).

The scheduler only talks to the broker: no Redis variable is read here.
"""

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
    log_level: str
    digest_cron: str
    digest_expires_seconds: int

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ.get
        return cls(
            rabbitmq_host=env("RABBITMQ_HOST", "localhost"),
            rabbitmq_port=int(env("RABBITMQ_PORT", "5672")),
            rabbitmq_vhost=env("RABBITMQ_VHOST", "/"),
            rabbitmq_user=env("RABBITMQ_USER", "guest"),
            rabbitmq_password=env("RABBITMQ_PASSWORD", "guest"),
            log_level=env("LOG_LEVEL", "INFO").upper(),
            digest_cron=env("DIGEST_CRON", "* * * * *"),
            digest_expires_seconds=int(env("DIGEST_EXPIRES_SECONDS", "50")),
        )

    @property
    def broker_url(self) -> str:
        user, password = quote(self.rabbitmq_user, safe=""), quote(self.rabbitmq_password, safe="")
        vhost = quote(self.rabbitmq_vhost, safe="")
        return f"amqp://{user}:{password}@{self.rabbitmq_host}:{self.rabbitmq_port}/{vhost}"


settings = Settings.from_env()
