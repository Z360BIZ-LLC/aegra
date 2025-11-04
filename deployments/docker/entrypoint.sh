#!/bin/sh
set -e

echo "Running database migrations..."
uv run alembic upgrade head

echo "Starting Aegra server..."
exec uv run uvicorn src.agent_server.main:app \
  --host 0.0.0.0 \
  --port "${PORT:-8000}" \
  --reload
