"""Host signals must stop the owned ARC helper before envelope cleanup."""

import os
from pathlib import Path
import signal
import subprocess
import time

import pytest

from . import plan_pricing_projection_v3_census_envelope_harness as envelope
from .test_plan_pricing_projection_v3_census_envelope_interrupts import _script_definitions


def _pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.mark.parametrize("phase", ["hold", "restore"])
@pytest.mark.parametrize("number", [signal.SIGTERM, signal.SIGINT])
def test_arc_helper_signal_cleanup(tmp_path: Path, phase: str, number: signal.Signals) -> None:
    env_by_name, state_root, checkout = envelope._fake_environment(tmp_path, FAKE_ARC_WAIT_PHASE=phase)
    process = subprocess.Popen(
        ["/bin/bash", str(envelope.SCRIPT), "run", *envelope._arguments(state_root, checkout)],
        env=env_by_name, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, start_new_session=True,
    )
    events = tmp_path / "fake-state/events"
    helper_pid = None
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if events.exists() and f"arc_{phase}_ready" in events.read_text().splitlines():
                helper_pid = int((tmp_path / "fake-state/arc-helper-pid").read_text())
                break
            if process.poll() is not None:
                _, stderr = process.communicate()
                pytest.fail(f"ARC {phase} exited before signal: {process.returncode}: {stderr}")
            time.sleep(0.02)
        assert helper_pid is not None, f"ARC {phase} helper did not become ready"
        process.send_signal(number)
        try:
            _, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            pytest.fail(f"parent deferred {number.name} while ARC {phase} remained active")
        assert process.returncode == 128 + number, stderr
        assert not _pid_exists(helper_pid), "ARC helper outlived the parent cleanup"
        observed = events.read_text().splitlines()
        assert observed.count(f"arc_{phase}_signal_{signal.SIGTERM.value}") == 1
        assert "lock_stop" not in observed
        if phase == "hold":
            assert "quota_create" not in observed and "child" not in observed
        receipt = envelope._receipt(state_root)
        assert receipt["exit_code"] == 128 + number
        assert receipt["cleanup"]["complete"] is False
        assert receipt["cleanup"]["lock_released"] is False
    finally:
        if helper_pid is not None and _pid_exists(helper_pid):
            os.kill(helper_pid, signal.SIGKILL)
        if process.poll() is None:
            try:
                process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.communicate(timeout=5)
        if helper_pid is not None:
            assert not _pid_exists(helper_pid), "task-created helper was not cleaned up"


def test_arc_helper_group_kills_signal_ignoring_descendant(tmp_path: Path) -> None:
    env_by_name, state_root, checkout = envelope._fake_environment(
        tmp_path, FAKE_ARC_WAIT_PHASE="hold", FAKE_ARC_IGNORE_SIGNAL="1",
    )
    process = subprocess.Popen(
        ["/bin/bash", str(envelope.SCRIPT), "run", *envelope._arguments(state_root, checkout)],
        env=env_by_name, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    events = tmp_path / "fake-state/events"
    helper_pid = None
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if events.exists() and "arc_hold_ready" in events.read_text().splitlines():
                helper_pid = int((tmp_path / "fake-state/arc-helper-pid").read_text())
                break
            if process.poll() is not None:
                _, stderr = process.communicate()
                pytest.fail(f"ARC hold exited before signal: {process.returncode}: {stderr}")
            time.sleep(0.02)
        assert helper_pid is not None
        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)
        assert process.returncode == 143, stderr
        assert not _pid_exists(helper_pid)
        receipt = envelope._receipt(state_root)
        assert receipt["cleanup"]["complete"] is False
        assert receipt["cleanup"]["lock_released"] is False
    finally:
        if helper_pid is not None and _pid_exists(helper_pid):
            os.kill(helper_pid, signal.SIGKILL)
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)


def test_sigint_before_setsid_cannot_start_arc_helper(tmp_path: Path) -> None:
    env_by_name, state_root, checkout = envelope._fake_environment(
        tmp_path, FAKE_ARC_SETSID_DELAY="0.5",
    )
    process = subprocess.Popen(
        ["/bin/bash", str(envelope.SCRIPT), "run", *envelope._arguments(state_root, checkout)],
        env=env_by_name, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        start_new_session=True,
    )
    events = tmp_path / "fake-state/events"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if events.exists() and "arc_setsid_waiting" in events.read_text().splitlines():
            break
        if process.poll() is not None:
            _, stderr = process.communicate()
            pytest.fail(f"ARC launcher exited before signal: {process.returncode}: {stderr}")
        time.sleep(0.02)
    else:
        process.kill()
        process.communicate()
        pytest.fail("ARC launcher did not reach the widened startup window")

    process.send_signal(signal.SIGINT)
    _, stderr = process.communicate(timeout=5)

    assert process.returncode == 130, stderr
    assert "arc_hold" not in events.read_text().splitlines()
    receipt = envelope._receipt(state_root)
    assert receipt["cleanup"]["complete"] is False
    assert receipt["cleanup"]["lock_released"] is False


def test_cleanup_never_overwrites_a_live_arc_group(tmp_path: Path) -> None:
    marker = tmp_path / "second-arc-helper"
    result = subprocess.run(
        ["bash", "-c", r'''
source "$1"
STATE_DIR=$2
ARC_ACQUISITION_PID=123
kill() { return 0; }
child_group_absent() { return 1; }
arc_acquisition() { : >"$3"; }
if cleanup_envelope; then exit 99; fi
''', "bash", str(_script_definitions(tmp_path)), str(tmp_path), str(marker)],
        check=False, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert not marker.exists()
    assert "ARC helper process group remains" in result.stdout


_PRE_HOLD_SIGNAL = r"""export HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT=$3
source "$1"
require_command() { :; }
verify_reviewed_hashes() { :; }; verify_source_and_target() { :; }
open_reviewed_arc_descriptor() { :; }
state_dir_is_confined() { :; }; create_state_directory() { :; }
start_lock() { :; }; require_lock_held() { :; }
cleanup_envelope() { CLEANUP_COMPLETE=true; }; write_receipt() { :; }
TEST_ARC_MARKER=$2
TEST_SIGNAL=$4
TEST_EXIT=$5
timeout() { : >"${TEST_ARC_MARKER}"; }
log() {
  if [[ "$*" = 'holding new ARC acquisition and draining native scale sets' ]]; then
    on_signal "${TEST_SIGNAL}" "${TEST_EXIT}"
  fi
}
export HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN=run
STATE_DIR=$3/run
DEADLINE_SECONDS=900
run_envelope
"""


@pytest.mark.parametrize(("name", "expected_exit"), [("TERM", 143), ("INT", 130)])
def test_arc_hold_pending_signal(tmp_path: Path, name: str, expected_exit: int) -> None:
    marker = tmp_path / "arc-started"
    result = subprocess.run(
        ["bash", "-c", _PRE_HOLD_SIGNAL, "bash", str(_script_definitions(tmp_path)),
         str(marker), str(tmp_path), name, str(expected_exit)],
        check=False, capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == expected_exit, result.stderr
    assert not marker.exists(), "ARC helper started after the pending host signal"
