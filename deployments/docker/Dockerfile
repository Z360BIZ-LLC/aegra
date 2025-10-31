FROM python:3.11-slim-bookworm AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

WORKDIR /app

# Install uv (available in all stages)
RUN pip install uv

# Create non-root user for runtime
RUN addgroup --system app && adduser --system --ingroup app app

# -----------------------------
# Builder stage
# -----------------------------
FROM base AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy aegra project metadata and source (from submodule)
COPY agents/text/aegra/pyproject.toml ./
COPY agents/text/aegra/README.md ./
COPY agents/text/aegra/src/ ./src/

# Copy packages/common (workspace dependency for agent-layer-common)
COPY packages/common/ ./packages/common/

# Install all dependencies using uv sync
RUN uv sync --no-dev

# -----------------------------
# Final, minimal runtime image
# -----------------------------
FROM base AS final

# Install only minimal runtime libs (psycopg[binary] used, so libpq not required at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy uv virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Copy packages/common (needed for agents/vector_store module imports)
COPY packages/common/ ./packages/common/

# Add packages/common to PYTHONPATH
ENV PYTHONPATH="/app/packages/common:${PYTHONPATH}"

# Copy runtime assets required by the app and alembic (from aegra submodule)
COPY agents/text/aegra/alembic.ini ./alembic.ini
COPY agents/text/aegra/alembic/ ./alembic/
COPY agents/text/aegra/aegra.json ./aegra.json
COPY agents/text/aegra/auth.py ./auth.py

# Copy aegra server source (contains agent_server/)
COPY agents/text/aegra/src/ ./src/

# Copy your custom graphs to src/ as well (contains text_agent/, askz_agent/, no conflict!)
COPY agents/text/src/ ./src/
COPY agents/text/aegra/deployments/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
EXPOSE 8000

# Run as non-root
USER app

# Run uvicorn using uv for AWS Fargate deployment
ENTRYPOINT ["/entrypoint.sh"]
#CMD ["uv", "run", "uvicorn", "src.agent_server.main:app", "--host", "0.0.0.0", "--port", "8000"]


