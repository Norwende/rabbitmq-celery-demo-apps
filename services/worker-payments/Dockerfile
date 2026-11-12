# syntax=docker/dockerfile:1
FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:0.10.8 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM python:3.13-slim
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --no-create-home app
COPY --from=builder /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
# Gossip/mingle/heartbeat rely on worker events and remote control, which are disabled.
CMD ["celery", "--app=payments.celery_app", "worker", "--queues=payments", "--hostname=payments@%h", \
     "--without-gossip", "--without-mingle", "--without-heartbeat"]
