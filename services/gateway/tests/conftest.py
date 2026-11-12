import io
import json

import pytest

from gateway.logs import configure_logging

# Configure before any logger is used: structlog caches loggers on first use.
configure_logging("INFO", "json", stream=io.StringIO())


class LogCapture:
    """The gateway's log stream, captured in memory."""

    def __init__(self) -> None:
        self.stream = io.StringIO()
        self.configure()

    def configure(self, level: str = "INFO", fmt: str = "json") -> None:
        self.stream.seek(0)
        self.stream.truncate()
        configure_logging(level, fmt, stream=self.stream)

    @property
    def text(self) -> str:
        return self.stream.getvalue()

    def events(self, event: str | None = None) -> list[dict]:
        parsed = [json.loads(line) for line in self.text.splitlines() if line.strip()]
        return [e for e in parsed if event is None or e["event"] == event]


@pytest.fixture
def logs():
    yield LogCapture()
    configure_logging("INFO", "json", stream=io.StringIO())
