# syntax=docker/dockerfile:1

FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# Matches the Rill CLI used for local development. Override at build time:
#   docker compose build --build-arg RILL_VERSION=v0.90.5
ARG RILL_VERSION=v0.90.5
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        make \
        unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://cdn.rilldata.com/rill/${RILL_VERSION}/rill_linux_${TARGETARCH}.zip" -o /tmp/rill.zip \
    && unzip -q /tmp/rill.zip -d /usr/local/bin \
    && chmod 755 /usr/local/bin/rill \
    && rm /tmp/rill.zip \
    && rill version

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}" \
    RILL_PORT=9009

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

COPY . .
RUN chmod 755 /app/docker/entrypoint.sh \
    && useradd --create-home --shell /bin/sh --uid 1000 app \
    && chown -R app:app /app

USER app

# DuckDB extensions used by Rill models (read_parquet and friends).
RUN rill runtime install-duckdb-extensions

EXPOSE 9009

HEALTHCHECK --interval=15s --timeout=5s --start-period=45s --retries=5 \
    CMD curl -fsS "http://127.0.0.1:${RILL_PORT}/" >/dev/null || exit 1

ENTRYPOINT ["/app/docker/entrypoint.sh"]
CMD ["demo"]
