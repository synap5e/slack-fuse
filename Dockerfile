# slack-fuse-server container image.
#
# Built and pushed to ghcr.io/synap5e/slack-fuse:<tag> by
# .github/workflows/docker.yml. The k8s-homelab manifest pins the tag
# explicitly; do not deploy :latest.
#
# Multi-stage build: the uv image bakes the dependency closure into a venv at
# /app/.venv, the runtime stage copies just the venv + source for a smaller
# image without uv itself. The slack-fuse CLI (FUSE mount) is NOT part of this
# image — only slack-fuse-server is shipped here.

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# pyfuse3 is a top-level project dep (used by the FUSE client, not the server)
# and may build from source on debian — needs libfuse3 headers + a C toolchain.
# The bookworm-slim base lacks both; add for the build only.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libfuse3-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/app/.venv

WORKDIR /app

# Resolve and install dependencies before copying the source so layer caching
# keys on pyproject + lockfile rather than on every code change.
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Now copy the source and install the project itself.
COPY slack_fuse ./slack_fuse
COPY slack_fuse_render ./slack_fuse_render
COPY slack_fuse_server ./slack_fuse_server
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM python:3.12-slim-bookworm

# libpq for psycopg's binary wheel (already pulled in by the wheel, but ldd
# wants it on PATH). curl is for the operator-side `curl http://.../health`
# debug path; tiny enough to keep.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libpq5 \
        libfuse3-3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user. UID/GID 10001 to avoid colliding with the postgres
# image's "postgres" UID in case k8s ever colocates them.
RUN useradd --system --uid 10001 --gid root --home-dir /app slackfuse

WORKDIR /app
COPY --from=builder --chown=slackfuse:root /app/.venv /app/.venv
COPY --from=builder --chown=slackfuse:root /app/slack_fuse /app/slack_fuse
COPY --from=builder --chown=slackfuse:root /app/slack_fuse_render /app/slack_fuse_render
COPY --from=builder --chown=slackfuse:root /app/slack_fuse_server /app/slack_fuse_server

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER slackfuse
EXPOSE 18765

ENTRYPOINT ["slack-fuse-server"]
CMD ["serve"]
