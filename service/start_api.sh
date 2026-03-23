#!/bin/sh
set -eu

. venv/bin/activate

API_HOST="${HLTHPRT_API_HOST:-127.0.0.1}"
API_PORT="${HLTHPRT_API_PORT:-8081}"
API_WORKERS="${HLTHPRT_API_WORKERS:-1}"

nginx
exec python main.py server start --host "${API_HOST}" --port "${API_PORT}" --workers "${API_WORKERS}"
