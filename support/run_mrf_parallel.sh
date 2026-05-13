#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

WORKERS=10
TEST_MODE=0
IMPORT_ID="${IMPORT_ID:-$(date +%Y%m%d)}"
LOG_DIR="${LOG_DIR:-/tmp/healthporta_mrf_${IMPORT_ID}}"

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
  - If you want one process with internal concurrency, use:
      HLTHPRT_MAX_MRF_JOBS=10 python main.py stop mrf --import-id YYYYMMDD
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

export HLTHPRT_IMPORT_ID_OVERRIDE="$IMPORT_ID"

venv/bin/python main.py start mrf "${START_ARGS[@]}"

declare -a PIDS=()
for i in $(seq 1 "$WORKERS"); do
  (
    export HLTHPRT_IMPORT_ID_OVERRIDE="$IMPORT_ID"
    export HLTHPRT_MAX_MRF_JOBS=1
    exec venv/bin/python main.py worker process.MRF --burst
  ) >"$LOG_DIR/worker_${i}.log" 2>&1 &
  PIDS+=("$!")
done

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

venv/bin/python main.py finish mrf "${FINISH_ARGS[@]}"
venv/bin/python main.py worker process.MRF_finish --burst -v | tee "$LOG_DIR/finish.log"

echo "Completed."
echo "Logs: $LOG_DIR"
