"""Signal and deadline checks for the plan-pricing census envelope."""

import signal
import json
import subprocess
import time
from pathlib import Path

import pytest

from . import test_plan_pricing_projection_v3_census_envelope as envelope


def _script_definitions(tmp_path: Path) -> Path:
    definitions = tmp_path / "envelope-definitions.sh"
    definitions.write_text(
        envelope.SCRIPT.read_text(encoding="utf-8").rsplit('main "$@"', 1)[0],
        encoding="utf-8",
    )
    return definitions


def test_zero_second_start_enforces_global_deadline(tmp_path: Path) -> None:
    """A start at Bash second zero must still activate the global deadline."""

    definitions = _script_definitions(tmp_path)
    deadline_result = subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; START_SECONDS=0; DEADLINE_SECONDS=10; '
            "SECONDS=11; operation_timeout",
            "bash",
            str(definitions),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert deadline_result.returncode == 124
    assert deadline_result.stdout == ""


def test_quota_admission_probe_uses_remaining_envelope_time(tmp_path: Path) -> None:
    """Quota admission must stop at the cleanup reserve, not a retry count."""

    definitions = _script_definitions(tmp_path)
    counter = tmp_path / "probe-count"
    limits = tmp_path / "probe-limits"
    counter.write_text("0", encoding="utf-8")
    limits.write_text("", encoding="utf-8")
    probe_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
STATE_DIR=$2
COUNTER=$3
LIMITS=$4
OWNER_TOKEN=testowner1
QUOTA_NAME=hp-pv3-census-testowner1
quota_probe_manifest() { :; }
kctl_with_limit() {
  printf '%s\n' "$1" >>"$LIMITS"
  shift
  cat >/dev/null
  count=$(<"$COUNTER")
  printf '%s\n' "$((count + 1))" >"$COUNTER"
  [ "$((count % 2))" -eq 0 ] && return 124
  return 0
}
sleep() { :; }
seconds_before_cleanup() {
  count=$(<"$COUNTER")
  [ "$count" -lt 4 ] && printf 1 || printf 0
}
prove_quota_admission
""",
            "bash",
            str(definitions),
            str(tmp_path),
            str(counter),
            str(limits),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert probe_result.returncode == 1
    assert counter.read_text(encoding="utf-8") == "4\n"
    assert limits.read_text(encoding="utf-8") == "1\n1\n1\n1\n"
    assert "did not prove" in probe_result.stderr


def test_receipt_treats_shell_values_as_data(tmp_path: Path) -> None:
    """Cluster-derived receipt values must never become Python source."""

    definitions = _script_definitions(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    receipt_write_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$0"
STATE_DIR=$1
SOURCE_SHA=$(printf 'a%.0s' {1..40})
OWNER_TOKEN=testowner1
QUOTA_UID='uid"quoted'
write_receipt 1
""",
            str(definitions),
            str(state_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert receipt_write_result.returncode == 0, receipt_write_result.stderr
    receipt = json.loads((state_dir / "envelope-receipt.json").read_text())
    assert receipt["resource_uids"]["quota"] == 'uid"quoted'


@pytest.mark.parametrize("failure_stage", ["writer", "chmod"])
def test_receipt_write_failures_never_publish_success(
    tmp_path: Path,
    failure_stage: str,
) -> None:
    """Every receipt publication step must return nonzero before final rename."""

    definitions = _script_definitions(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    failed_write_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$0"
STATE_DIR=$1
SOURCE_SHA=$(printf 'a%.0s' {1..40})
if [ "$2" = writer ]; then
  python3() { : >"${STATE_DIR}/envelope-receipt.json.tmp"; return 1; }
else
  chmod() { return 1; }
fi
if write_receipt 1; then exit 97; fi
[ ! -e "${STATE_DIR}/envelope-receipt.json" ]
""",
            str(definitions),
            str(state_dir),
            failure_stage,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert failed_write_result.returncode == 0, failed_write_result.stderr


def _run_ignoring_child_probe(
    tmp_path: Path, *, deadline: bool
) -> subprocess.CompletedProcess[str]:
    definitions = _script_definitions(tmp_path)
    ready = tmp_path / "ready"
    marker = tmp_path / "deadline"
    mode = "deadline" if deadline else "interrupt"
    return subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
CHILD_TERMINATION_GRACE_SECONDS=0
CHILD_KILL_VERIFY_SECONDS=2
python3 -c 'import os, signal, sys, time; os.setsid(); signal.signal(signal.SIGTERM, signal.SIG_IGN); open(sys.argv[1], "w").close(); time.sleep(60)' "$2" &
pid=$!
while [ ! -e "$2" ]; do /bin/sleep 0.01; done
if [ "$4" = deadline ]; then
  CHILD_DEADLINE_MARKER=$3
  capture_child_pid "${pid}"
  start_child_deadline_timer "${pid}" 0 "$3"
else
  INTERRUPT_EXIT=143
  INTERRUPT_SIGNAL=TERM
  capture_child_pid "${pid}"
fi
reap_child_group "${pid}"
child_group_absent "${pid}"
printf '%s:%s\n' "${TIMED_OUT}" "${CHILD_EXIT_CODE}"
""",
            "bash",
            str(definitions),
            str(ready),
            str(marker),
            mode,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_pending_signal_kills_child_group_before_cleanup(tmp_path: Path) -> None:
    """A pending TERM must reap an ignoring child before cleanup can start."""

    signal_result = _run_ignoring_child_probe(tmp_path, deadline=False)

    assert signal_result.returncode == 0, signal_result.stderr
    assert signal_result.stdout.startswith("false:")


def test_owned_deadline_marks_and_reaps_ignoring_child(tmp_path: Path) -> None:
    """The wrapper must own timeout attribution and reap before returning."""

    result = _run_ignoring_child_probe(tmp_path, deadline=True)

    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("true:")
    assert (tmp_path / "deadline").read_text() == "deadline\n"


def test_stopping_fired_timer_preserves_deadline_marker(tmp_path: Path) -> None:
    """A timer stopped during atomic publication must retain timeout evidence."""

    definitions = _script_definitions(tmp_path)
    marker = tmp_path / "deadline"
    timer_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
CHILD_DEADLINE_MARKER=$2
printf 'deadline\n' >"${CHILD_DEADLINE_MARKER}.tmp"
python3 -c 'import time; time.sleep(60)' &
CHILD_DEADLINE_TIMER_PID=$!
stop_child_deadline_timer
[ -e "${CHILD_DEADLINE_MARKER}" ]
[ ! -e "${CHILD_DEADLINE_MARKER}.tmp" ]
""",
            "bash",
            str(definitions),
            str(marker),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert timer_result.returncode == 0, timer_result.stderr
    assert marker.read_text() == "deadline\n"


def test_signal_handler_never_launches_a_timer(tmp_path: Path) -> None:
    """A trap must not start background work that can overwrite Bash `$!`."""

    definitions = _script_definitions(tmp_path)
    timer = tmp_path / "timer"
    handler_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
timer_path=$2
signal_child_group() { :; }
arm_child_shutdown() { : >"$timer_path"; }
CHILD_PID=123
on_signal TERM 143
[ "${CHILD_SIGNAL_FORWARDED}" = true ]
[ ! -e "${timer_path}" ]
""",
            "bash",
            str(definitions),
            str(timer),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert handler_result.returncode == 0, handler_result.stderr


@pytest.mark.parametrize("interrupt_stage", ["fence", "log"])
def test_pre_child_signal_never_starts_census(
    tmp_path: Path,
    interrupt_stage: str,
) -> None:
    """A signal at either final pre-child boundary must stop child spawn."""

    definitions = _script_definitions(tmp_path)
    marker = tmp_path / "child-started"
    state_root = tmp_path / "state"
    state_root.mkdir()
    run_result = subprocess.run(
        [
            "bash",
            "-c",
            """
export HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT=$3
source "$1"
require_command() { :; }
verify_source_and_target() { :; }
start_lock() { :; }
require_lock_held() { :; }
create_quota() { :; }
prove_quota_admission() { :; }
wait_for_arc_idle() { :; }
set_import_drain() { :; }
create_worker_fence() { :; }
verify_stable_zero_work() { :; }
INJECT_INTERRUPT=$4
verify_child_fences() {
  [ "${INJECT_INTERRUPT}" != fence ] || on_signal TERM 143
}
log() {
  if [ "${INJECT_INTERRUPT}" = log ] \
      && [[ "$*" = 'running the foreground census lifecycle' ]]; then
    on_signal TERM 143
  fi
}
cleanup_envelope() { CLEANUP_COMPLETE=true; }
write_receipt() { :; }
export HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN=run
TEST_CHILD_MARKER=$2
CHILD_COMMAND=(/usr/bin/touch "${TEST_CHILD_MARKER}")
STATE_DIR=$3/run
DEADLINE_SECONDS=900
run_envelope
""",
            "bash",
            str(definitions),
            str(marker),
            str(state_root),
            interrupt_stage,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert run_result.returncode == 143, run_result.stderr
    assert not marker.exists()


def test_signal_during_receipt_cannot_seal_success(tmp_path: Path) -> None:
    """A signal before receipt commit must remove any success artifact."""

    definitions = _script_definitions(tmp_path)
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    receipt = state_dir / "envelope-receipt.json"
    finish_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
STATE_DIR=$2
EXIT_TRAP_ACTIVE=true
cleanup_envelope() { CLEANUP_COMPLETE=true; }
write_receipt() {
  : >"${STATE_DIR}/envelope-receipt.json"
  on_signal TERM 143
}
trap 'on_signal TERM 143' TERM
trap 'finish $?' EXIT
finish 0
""",
            "bash",
            str(definitions),
            str(state_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert finish_result.returncode == 143, finish_result.stderr
    assert not receipt.exists()


def test_signal_before_receipt_boundary_changes_final_exit(tmp_path: Path) -> None:
    """The final interrupt decision must follow the receipt boundary."""

    definitions = _script_definitions(tmp_path)
    written_exit = tmp_path / "written-exit"
    finish_result = subprocess.run(
        [
            "bash",
            "-c",
            """
source "$1"
set -T
WRITTEN_EXIT=$2
EXIT_TRAP_ACTIVE=true
cleanup_envelope() { CLEANUP_COMPLETE=true; }
write_receipt() { printf '%s\n' "$1" >"${WRITTEN_EXIT}"; }
inject_before_boundary() {
  if [ "${BASH_COMMAND}" = RECEIPT_FINALIZING=true ]; then
    trap - DEBUG
    on_signal TERM 143
  fi
}
trap inject_before_boundary DEBUG
finish 0
""",
            "bash",
            str(definitions),
            str(written_exit),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert finish_result.returncode == 143, finish_result.stderr
    assert written_exit.read_text(encoding="utf-8") == "143\n"


@pytest.mark.parametrize(
    "signal_number, expected_exit",
    [(signal.SIGINT, 130), (signal.SIGTERM, 143)],
)
def test_signal_forwards_once_and_finishes_cleanup(
    tmp_path: Path, signal_number: signal.Signals, expected_exit: int
) -> None:
    """Host signals must reach the census group once before ordered teardown."""

    env_by_name, state_root, checkout = envelope._fake_environment(
        tmp_path, FAKE_CHILD_MODE="wait"
    )
    process = subprocess.Popen(
        [
            "/bin/bash",
            str(envelope.SCRIPT),
            "run",
            *envelope._arguments(state_root, checkout),
        ],
        env=env_by_name,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    events = tmp_path / "fake-state/events"
    for _ in range(500):
        if events.exists() and "child" in events.read_text():
            break
        time.sleep(0.02)
    else:
        process.kill()
        raise AssertionError("foreground census child did not start")
    process.send_signal(signal_number)
    process.send_signal(signal_number)
    _stdout, stderr = process.communicate(timeout=10)

    assert process.returncode == expected_exit, stderr
    assert (
        events.read_text().splitlines().count(f"child_signal_{signal_number.value}")
        == 1
    )
    receipt = envelope._receipt(state_root)
    assert receipt["exit_code"] == expected_exit
    assert receipt["cleanup"]["complete"] is True
