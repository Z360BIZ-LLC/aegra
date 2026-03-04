ARG PY_VERSION=3.12
FROM python:${PY_VERSION}-slim-bookworm AS base
ARG PY_VERSION

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# Create non-root user for runtime
RUN addgroup --system app && adduser --system --ingroup app app

# -----------------------------
# Builder stage
# -----------------------------
FROM base AS builder

# Retrieve the uv binary directly from the official image.
COPY --from=ghcr.io/astral-sh/uv:0.9.26 /uv /bin/uv

# Install system build dependencies required for compiling Python extensions.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy packages/common and install it first (used by our custom agents)
# Build context is agent-layer root (../../.. from aegra)
COPY packages/common/ ./packages/common/
RUN uv pip install --system --compile-bytecode ./packages/common/

# Copy aegra-api package and install with all dependencies
COPY agents/text/aegra/libs/aegra-api/ ./libs/aegra-api/
RUN uv pip install --system --compile-bytecode ./libs/aegra-api/

# -----------------------------
# Final, minimal runtime image
# -----------------------------
FROM base AS final

# Install only minimal runtime libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy installed Python packages from the builder stage.
COPY --from=builder /usr/local/lib/python${PY_VERSION}/site-packages/ /usr/local/lib/python${PY_VERSION}/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy packages/common (needed for agents/vector_store module imports)
COPY packages/common/ ./packages/common/

# Add packages/common to PYTHONPATH
ENV PYTHONPATH="/app/packages/common:${PYTHONPATH}"

# Copy runtime assets required by the app and alembic (from aegra submodule)
COPY agents/text/aegra/libs/aegra-api/alembic.ini ./alembic.ini
COPY agents/text/aegra/libs/aegra-api/alembic/ ./alembic/
COPY agents/text/aegra/aegra.json ./aegra.json

# Copy your custom graphs to src/ (contains text_agent/, askz_agent/, etc. — no conflict with aegra_api/)
COPY agents/text/src/ ./src/

# Create writable cache directories for non-root user
RUN mkdir -p /tmp/composio-cache && chown -R app:app /tmp/composio-cache

EXPOSE 8000

# Run as non-root
USER app

# Start server (aegra auto-runs migrations in app lifespan)
CMD ["uvicorn", "aegra_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
