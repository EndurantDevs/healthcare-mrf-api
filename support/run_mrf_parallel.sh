#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WORKERS=16
TEST_MODE=0
IMPORT_ID="${IMPORT_ID:-$(date +%Y%m%d)}"
LOG_DIR="${LOG_DIR:-/tmp/healthporta_mrf_${IMPORT_ID}}"
PYTHON_BIN="${PYTHON_BIN:-}"

usage() {
  cat <<'EOF'
Usage:
  support/run_mrf_parallel.sh [--test] [--workers N] [--import-id YYYYMMDD] [--log-dir PATH]

Behavior:
  1. Enqueue the MRF import.
  2. Start N separate process.MRF worker processes in parallel.
  3. Wait for all workers to finish.
  4. Enqueue finalize and run process.MRF_finish.

Notes:
  - This launches N actual worker processes.
  - Each worker process is forced to HLTHPRT_MAX_MRF_JOBS=1.
  - The finish worker is forced to HLTHPRT_MAX_MRF_FINISH_JOBS=1 because there
    is only one shutdown job.
  - Set PYTHON_BIN to override the Python executable. By default, the script
    prefers venv314/bin/python, then venv/bin/python, then python.
  - Prefer --workers 16 for dev-server/full-import runs. If you want one
    process with internal concurrency, use:
      HLTHPRT_MAX_MRF_JOBS=16 HLTHPRT_MRF_QUEUE_READ_LIMIT=512 python main.py worker process.MRF --burst
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --test)
      TEST_MODE=1
      shift
      ;;
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --import-id)
      IMPORT_ID="$2"
      shift 2
      ;;
    --log-dir)
      LOG_DIR="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$LOG_DIR"

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x venv314/bin/python ]]; then
    PYTHON_BIN=venv314/bin/python
  elif [[ -x venv/bin/python ]]; then
    PYTHON_BIN=venv/bin/python
  else
    PYTHON_BIN=python
  fi
fi

queue_depth() {
  "$PYTHON_BIN" - "$1" <<'PY'
import asyncio
import sys

from arq.connections import create_pool

from process.redis_config import build_redis_settings


async def main() -> None:
    redis = await create_pool(build_redis_settings())
    try:
        print(await redis.zcard(sys.argv[1]))
    finally:
        await redis.aclose()


asyncio.run(main())
PY
}

START_ARGS=()
FINISH_ARGS=(--import-id "$IMPORT_ID")
if [[ "$TEST_MODE" -eq 1 ]]; then
  START_ARGS+=(--test)
  FINISH_ARGS=(--test "${FINISH_ARGS[@]}")
fi

echo "Running MRF import"
echo "  import_id: $IMPORT_ID"
echo "  workers:   $WORKERS"
echo "  test_mode: $TEST_MODE"
echo "  log_dir:   $LOG_DIR"
echo "  python:    $PYTHON_BIN"

export HLTHPRT_IMPORT_ID_OVERRIDE="$IMPORT_ID"

"$PYTHON_BIN" main.py start mrf "${START_ARGS[@]}"

declare -a PIDS=()
(
  export HLTHPRT_IMPORT_ID_OVERRIDE="$IMPORT_ID"
  export HLTHPRT_MAX_MRF_JOBS=1
  exec "$PYTHON_BIN" main.py worker process.MRF --burst
) >"$LOG_DIR/worker_1.log" 2>&1 &
BOOTSTRAP_PID="$!"
PIDS+=("$BOOTSTRAP_PID")

for _ in $(seq 1 300); do
  DEPTH="$(queue_depth arq:MRF)"
  if [[ "$DEPTH" -gt 0 ]]; then
    break
  fi
  if ! kill -0 "$BOOTSTRAP_PID" 2>/dev/null; then
    break
  fi
  sleep 2
done

if [[ "$WORKERS" -gt 1 ]]; then
  for i in $(seq 2 "$WORKERS"); do
    (
      export HLTHPRT_IMPORT_ID_OVERRIDE="$IMPORT_ID"
      export HLTHPRT_MAX_MRF_JOBS=1
      exec "$PYTHON_BIN" main.py worker process.MRF --burst
    ) >"$LOG_DIR/worker_${i}.log" 2>&1 &
    PIDS+=("$!")
  done
fi

FAILURES=0
for pid in "${PIDS[@]}"; do
  if ! wait "$pid"; then
    FAILURES=$((FAILURES + 1))
  fi
done

if [[ "$FAILURES" -ne 0 ]]; then
  echo "MRF worker failures: $FAILURES" >&2
  exit 1
fi

if grep -E " [0-9.]+s ! .* failed," "$LOG_DIR"/worker_*.log >/dev/null 2>&1; then
  echo "MRF job failures detected in worker logs; not running finish." >&2
  exit 1
fi

REMAINING="$(queue_depth arq:MRF)"
if [[ "$REMAINING" -ne 0 ]]; then
  echo "MRF queue still has $REMAINING job(s); not running finish." >&2
  exit 1
fi

"$PYTHON_BIN" main.py finish mrf "${FINISH_ARGS[@]}"
HLTHPRT_MAX_MRF_FINISH_JOBS=1 "$PYTHON_BIN" main.py worker process.MRF_finish --burst -v | tee "$LOG_DIR/finish.log"

echo "Completed."
echo "Logs: $LOG_DIR"
