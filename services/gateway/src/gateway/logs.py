"""Structured logging for the gateway (structlog).

* One event per line on stdout: JSON by default (containers), or a human-readable console
  rendering with ``LOG_FORMAT=console`` (docker compose). ``LOG_LEVEL`` sets the level.
* Standard library loggers (uvicorn.error, celery, kombu, amqp, redis…) go through the same
  processors via ``ProcessorFormatter`` (foreign pre-chain): the stream stays homogeneous.
* Request-scoped context lives in ``structlog.contextvars``: ``RequestContextMiddleware``
  binds ``request_id`` / ``method`` / ``path`` for each request, emits one access event and
  clears the context. Sync endpoints run in the threadpool, which copies the context.
* Credentials embedded in URLs (``amqp://user:password@…``) are masked in every event.
"""

import logging
import re
import sys
import time
import uuid
from typing import Any, TextIO

import structlog
from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

LOG_FORMATS = ("json", "console")
REQUEST_ID_HEADER = "x-request-id"
# Probe endpoints: their access events go to DEBUG (kubelet calls them every few seconds).
QUIET_PATHS = frozenset({"/healthz", "/readyz"})
# request.state attribute through which a sync handler hands fields to the access event
# (the handler runs in a copy of the context: what it binds does not flow back up).
STATE_KEY = "log_context"

_ACCEPTED_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_URL_CREDENTIALS = re.compile(r"(?P<scheme>[a-z][a-z0-9+.-]*://)(?P<user>[^:/@\s]*):[^@\s]+@", re.IGNORECASE)

# Access events: one per request, emitted by RequestContextMiddleware.
access_logger = structlog.get_logger("gateway.access")


def redact(value: Any) -> Any:
    """Mask the password of any ``scheme://user:password@host`` URL (recursively)."""
    if isinstance(value, str):
        return _URL_CREDENTIALS.sub(r"\g<scheme>\g<user>:***@", value)
    if isinstance(value, dict):
        return {key: redact(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return type(value)(redact(item) for item in value)
    return value


def redact_secrets(_logger: Any, _method: str, event_dict: dict) -> dict:
    return redact(event_dict)


def configure_logging(level: str = "INFO", fmt: str = "json", stream: TextIO | None = None) -> None:
    """Route structlog and every standard library logger to one handler on ``stream``."""
    if fmt not in LOG_FORMATS:
        raise ValueError(f"LOG_FORMAT={fmt!r}: expected one of {', '.join(LOG_FORMATS)}")
    shared: list = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.PositionalArgumentsFormatter(),  # stdlib "%s" calls of libraries
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    if fmt == "json":
        # exc_info -> "exception": list of {exc_type, exc_value, frames[…]} (structured).
        final = [structlog.processors.dict_tracebacks, redact_secrets, structlog.processors.JSONRenderer()]
    else:
        final = [redact_secrets, structlog.dev.ConsoleRenderer(colors=False)]
    structlog.configure(
        processors=[structlog.stdlib.filter_by_level, *shared, structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *final],
    ))
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    # uvicorn installs no handler with log_config=None; make sure its loggers propagate to
    # the root handler. Its access log is disabled (uvicorn.run(access_log=False)): the
    # middleware below emits a structured access event instead.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).handlers = []
        logging.getLogger(name).propagate = True


def bind_request_context(request: Request, **fields: Any) -> None:
    """Bind fields for the rest of the handler *and* the request's access event."""
    structlog.contextvars.bind_contextvars(**fields)
    if not hasattr(request.state, STATE_KEY):
        setattr(request.state, STATE_KEY, {})
    getattr(request.state, STATE_KEY).update(fields)


def _request_id(scope: Scope) -> str:
    for name, value in scope.get("headers", ()):
        if name == REQUEST_ID_HEADER.encode():
            candidate = value.decode("latin-1").strip()
            if _ACCEPTED_REQUEST_ID.match(candidate):
                return candidate
            break  # malformed or oversized: do not echo it, generate a fresh one
    return uuid.uuid4().hex


class RequestContextMiddleware:
    """Pure ASGI middleware: request context, ``X-Request-ID`` echo, one access event."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = _request_id(scope)
        path = scope["path"]
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id, method=scope["method"], path=path)
        status_code = 500
        started = time.perf_counter()

        async def send_with_request_id(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                message["headers"] = [*message.get("headers", []), (b"x-request-id", request_id.encode())]
            await send(message)

        # The context is cleared when the next request starts, not here: uvicorn logs an
        # unhandled exception after this middleware returns, and that line keeps the request
        # fields. Each request runs in its own asyncio task (own copy of the context), so
        # nothing leaks from one request to another either way.
        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            extra = scope.get("state", {}).get(STATE_KEY, {})
            level = logging.DEBUG if path in QUIET_PATHS and status_code < 500 else logging.INFO
            access_logger.log(level, "http_request", status=status_code,
                       duration_ms=round((time.perf_counter() - started) * 1000, 1), **extra)
