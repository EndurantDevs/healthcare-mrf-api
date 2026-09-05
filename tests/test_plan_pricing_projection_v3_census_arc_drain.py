"""The envelope owns ARC pause, census fencing, and safe capacity restoration."""

import os
from pathlib import Path
import subprocess

import pytest

from scripts.research import plan_pricing_projection_v3_census_support as support

from . import plan_pricing_projection_v3_census_envelope_harness as envelope


def test_arc_pause_and_restore_bracket_quota(tmp_path: Path) -> None:
    result, state_root = envelope._run_envelope(tmp_path)

    assert result.returncode == 0, result.stderr
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert events.index("lock_create") < events.index("arc_hold") < events.index("quota_create")
    assert events.index("quota_delete") < events.index("arc_restore") < events.index("lock_stop")
    assert events.count("arc_verify") >= 3
    assert envelope._receipt(state_root)["cleanup"]["arc_capacity_restored"] is True


@pytest.mark.parametrize("phase", ["hold", "verify"])
def test_failed_acquisition_prevents_child_and_restores_capacity(tmp_path: Path, phase: str) -> None:
    result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_FAIL=phase)

    assert result.returncode != 0
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "child" not in events
    assert "arc_restore" in events and "lock_stop" in events
    if phase == "hold":
        assert "quota_create" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


@pytest.mark.parametrize("settings", [{"FAKE_ARC_FAIL": "identity"}, {"FAKE_CHILD_CLEANUP": "false"}])
def test_unproven_cleanup_keeps_acquisition_pause_and_outer_fences(tmp_path: Path, settings: dict) -> None:
    result, state_root = envelope._run_envelope(tmp_path, **settings)

    assert result.returncode != 0
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "arc_restore" not in events
    assert "quota_delete" not in events and "lock_stop" not in events
    assert (tmp_path / "fake-state/arc-held").exists()
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_failed_capacity_restoration_keeps_lock_and_reports_incomplete(tmp_path: Path) -> None:
    result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_FAIL="restore")

    assert result.returncode != 0
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "quota_delete" in events and "lock_stop" not in events
    assert (tmp_path / "fake-state/arc-held").exists()
    cleanup = envelope._receipt(state_root)["cleanup"]
    assert cleanup["quota_removed"] is True
    assert cleanup["arc_capacity_restored"] is False
    assert cleanup["lock_released"] is False
    assert cleanup["complete"] is False
    assert (tmp_path / "fake-state/lock").exists()


def test_post_child_acquisition_failure_invalidates_proof(tmp_path: Path) -> None:
    result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_FAIL_AFTER_CHILD="1")

    assert result.returncode != 0
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["arc_capacity_restored"] is True
    assert receipt["cleanup"]["complete"] is True


def test_unreviewed_arc_helper_is_rejected_before_lock(tmp_path: Path) -> None:
    result, state_root = envelope._run_envelope(
        tmp_path, FAKE_ARC_HELPER_MISMATCH="1",
    )

    assert result.returncode != 0
    events = tmp_path / "fake-state/events"
    assert not events.exists() or "lock_create" not in events.read_text().splitlines()
    cleanup = envelope._receipt(state_root)["cleanup"]
    assert cleanup["lock_released"] is False
    assert cleanup["complete"] is True


def test_arc_helper_is_in_reviewed_harness_inventory() -> None:
    assert "scripts/research/plan_pricing_projection_v3_census_arc.py" in support.HARNESS_PATHS


def _reviewed_helper_checkout(tmp_path: Path, attack: str) -> tuple[Path, str]:
    checkout = tmp_path / "repo"
    helper = checkout / "scripts/research/plan_pricing_projection_v3_census_arc.py"
    helper.parent.mkdir(parents=True)
    program = 'import os; open(os.environ["ARC_TEST_MARKER"], "a").write("x")\n'
    helper.write_text(program)
    subprocess.run(["git", "init", "-q", str(checkout)], check=True)
    subprocess.run(["git", "-C", str(checkout), "add", str(helper)], check=True)
    subprocess.run(
        ["git", "-C", str(checkout), "-c", "user.name=Test", "-c",
         "user.email=test@example.invalid", "commit", "-qm", "reviewed helper"],
        check=True,
    )
    source_sha = subprocess.run(
        ["git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if attack == "checkout":
        helper.write_text(program + "# unreviewed change\n")
    elif attack == "replace":
        helper.write_text(program + "# replacement object\n")
        subprocess.run(["git", "-C", str(checkout), "add", str(helper)], check=True)
        subprocess.run(
            ["git", "-C", str(checkout), "-c", "user.name=Test", "-c",
             "user.email=test@example.invalid", "commit", "-qm", "replacement helper"],
            check=True,
        )
        replacement_sha = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        subprocess.run(
            ["git", "-C", str(checkout), "replace", source_sha, replacement_sha],
            check=True,
        )
        subprocess.run(
            ["git", "-C", str(checkout), "update-ref", "HEAD", source_sha,
             replacement_sha],
            check=True,
        )
    return checkout, source_sha


@pytest.mark.parametrize("attack", ["none", "checkout", "descriptor", "replace"])
def test_arc_helper_executes_only_reviewed_source(tmp_path: Path, attack: str) -> None:
    """Execute only the committed helper object despite checkout or descriptor attacks."""
    definitions = tmp_path / "envelope.sh"
    definitions.write_text(envelope.SCRIPT.read_text().rsplit('main "$@"', 1)[0])
    checkout, source_sha = _reviewed_helper_checkout(tmp_path, attack)
    marker = tmp_path / "executed"
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    setsid = fake_bin / "setsid"
    setsid.write_text('#!/bin/sh\nexec "$@"\n')
    setsid.chmod(0o755)
    replacement = tmp_path / "replacement"
    if attack == "descriptor":
        replacement.write_text("raise SystemExit('unreviewed')\n")
    completed_process = subprocess.run(
        ["bash", "-c", r'''
source "$1"
REPO_DIR=$2
SOURCE_SHA=$3
OWNER_TOKEN=test-owner
STATE_DIR=$4
START_SECONDS=${SECONDS}
DEADLINE_SECONDS=900
if [ -n "${ARC_TEST_REPLACEMENT}" ]; then
  set -T
  replace_descriptor_copy() {
    if [[ "${BASH_COMMAND}" == *'exec 8<'* ]]; then
      trap - DEBUG
      mv "${ARC_TEST_REPLACEMENT}" "${STATE_DIR}/reviewed-arc-helper"
    fi
  }
  trap replace_descriptor_copy DEBUG
fi
arc_acquisition hold
arc_acquisition verify
''', "bash", str(definitions), str(checkout), source_sha, str(state)],
        env={**os.environ, "ARC_TEST_MARKER": str(marker),
             "ARC_TEST_REPLACEMENT": str(replacement) if attack == "descriptor" else "",
             "PATH": f"{fake_bin}:{os.environ['PATH']}"},
        capture_output=True, text=True, timeout=10, check=False,
    )
    is_attack = attack != "none"
    assert (completed_process.returncode != 0) is is_attack, completed_process.stderr
    assert marker.exists() is not is_attack
    if not is_attack:
        assert marker.read_text() == "xx"
