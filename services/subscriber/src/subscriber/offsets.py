"""Stream offset resolution and Redis checkpoint."""

from redis import Redis

NAMED_OFFSETS = {"first", "last", "next"}


def parse_offset_spec(value: str) -> str | int:
    """Validate ``STREAM_OFFSET``: ``first``, ``last``, ``next`` or a non-negative integer."""
    spec = value.strip().lower()
    if spec in NAMED_OFFSETS:
        return spec
    if spec.isdigit():
        return int(spec)
    raise ValueError(f"invalid STREAM_OFFSET {value!r}: expected first, last, next or an integer >= 0")


def resolve_start_offset(configured: str | int, last_processed: int | None) -> str | int:
    """Where to (re)attach: right after the last processed offset, else the configured spec."""
    if last_processed is not None:
        return last_processed + 1
    return configured


class Checkpoint:
    """Last processed offset of one subscriber, stored at ``stream:<stream>:offset:<name>``."""

    def __init__(self, redis: Redis, stream: str, name: str) -> None:
        self.redis = redis
        self.key = f"stream:{stream}:offset:{name}"

    def load(self) -> int | None:
        value = self.redis.get(self.key)
        return int(value) if value is not None else None

    def save(self, offset: int) -> None:
        self.redis.set(self.key, offset)
