# Base images and package index are build arguments, so that an internal registry/mirror
# can replace Docker Hub, ghcr.io and PyPI (scripts/build-images.ps1 -BaseRegistry,
# -PythonImage, -UvImage, -PipIndexUrl). No "# syntax=" directive: BuildKit's built-in
# Dockerfile frontend handles RUN --mount, nothing extra is pulled at build time.
ARG PYTHON_IMAGE=python:3.13-slim
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.10.8

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=0
# Package index used by every uv command of this stage. uv.lock records pypi.org: with a
# mirror, "uv sync --locked" would refuse the lock, so it is re-resolved against the mirror
# first (versions preferred from the lock) and the build fails if any pinned version moved.
ARG UV_DEFAULT_INDEX=https://pypi.org/simple
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ "$UV_DEFAULT_INDEX" != "https://pypi.org/simple" ]; then \
        grep -E '^(name|version) = ' uv.lock > /tmp/pins && uv lock \
        && grep -E '^(name|version) = ' uv.lock | diff /tmp/pins - ; \
    fi \
    && uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv uv sync --locked --no-dev --no-editable

FROM ${PYTHON_IMAGE}
RUN groupadd --system --gid 10001 app && useradd --system --uid 10001 --gid app --no-create-home app
COPY --from=builder /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 10001:10001
# Liveness: /tmp/subscriber-heartbeat is touched every few seconds by the consume loop.
CMD ["python", "-m", "subscriber"]
