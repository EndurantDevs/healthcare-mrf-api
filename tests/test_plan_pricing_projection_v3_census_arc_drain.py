"""The envelope owns ARC pause, census fencing, and safe capacity restoration."""

from pathlib import Path

import pytest

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
    assert cleanup["arc_capacity_restored"] is False
    assert cleanup["complete"] is False


def test_post_child_acquisition_failure_invalidates_proof(tmp_path: Path) -> None:
    result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_FAIL_AFTER_CHILD="1")

    assert result.returncode != 0
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["arc_capacity_restored"] is True
    assert receipt["cleanup"]["complete"] is True
