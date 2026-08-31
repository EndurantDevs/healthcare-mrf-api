"""Admission and final cleanup checks for the census envelope."""

import hashlib
from pathlib import Path

import pytest

from . import plan_pricing_projection_v3_census_envelope_harness as envelope


def test_missing_v1_admission_api_fails_before_any_mutation(tmp_path: Path) -> None:
    """The exact admission API must exist before any outer fence is created."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_V1_ADMISSION_MISSING="1",
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_state_root_replacement_fails_before_any_outer_fence(tmp_path: Path) -> None:
    """A swapped root cannot redirect task state outside the reviewed directory."""

    outside = tmp_path / "outside-state-root"
    outside.mkdir()
    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_REPLACE_STATE_ROOT_DURING_GIT="1",
    )

    assert result.returncode == 1
    assert (tmp_path / "fake-state/events").read_text().splitlines() == [
        "state_root_replaced"
    ]
    original_root = state_root.with_name(state_root.name + ".original")
    assert original_root.is_dir()
    assert not (outside / "run").exists()
    assert not (original_root / "run").exists()


@pytest.mark.parametrize("replacement", ["symlink", "fifo"])
def test_child_copy_rejects_unbounded_source_replacement(
    tmp_path: Path,
    replacement: str,
) -> None:
    """The bounded no-follow copy rejects a replaced source before launch."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_REPLACE_CHILD_DURING_COPY=replacement,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "child_executable_replaced_during_copy" in events
    assert "child" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


@pytest.mark.parametrize("server_minor", ["29", "35+", "invalid"])
def test_unsupported_server_version_fails_before_any_mutation(
    tmp_path: Path,
    server_minor: str,
) -> None:
    """An old or malformed Kubernetes version must fail before the lock."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_SERVER_MINOR=server_minor,
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


@pytest.mark.parametrize("kind", ["job", "pod", "configmap"])
def test_preexisting_census_resource_fails_before_any_mutation(
    tmp_path: Path, kind: str
) -> None:
    """Any labeled census resource must fail before acquiring an outer fence."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_PREEXISTING_CENSUS_KIND=kind,
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


@pytest.mark.parametrize("kind", ["job", "pod", "configmap"])
def test_labeled_census_residue_retains_every_outer_fence(
    tmp_path: Path,
    kind: str,
) -> None:
    """Any differently named census residue blocks teardown of all fences."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CENSUS_RESIDUE_AFTER_CHILD_KIND=kind,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "child" in events
    assert "drain_set_false" not in events
    assert "binding_delete" not in events
    assert "policy_delete" not in events
    assert "quota_delete" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


@pytest.mark.parametrize("resource", ["quota", "policy", "binding"])
def test_delayed_create_outcome_retains_every_outer_fence(
    tmp_path: Path,
    resource: str,
) -> None:
    """An unresolved create may appear after the first absence read."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CREATE_DELAYED_ERROR=resource,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert f"{resource}_create" in events
    assert f"{resource}_delayed_appear" in events
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_unproven_child_cleanup_retains_every_outer_fence(tmp_path: Path) -> None:
    """The parent must not release fences from one absence snapshot alone."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_CLEANUP="false",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "child" in events
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    census_receipt = state_root / "run/census-receipt.json"
    assert (
        receipt["census_receipt_sha256"]
        == hashlib.sha256(census_receipt.read_bytes()).hexdigest()
    )
    assert receipt["runtime_attestation"]["pod_uid"] == "pod-uid"
    assert receipt["runtime_attestation"]["configmap_uid"] == "configmap-uid"
    assert (
        receipt["runtime_attestation"]["source_overlay_sha256"]
        == envelope.OVERLAY_SHA256
    )
    assert receipt["cleanup"]["complete"] is False


def test_seed_reappearance_retains_drain_quota_and_lock(tmp_path: Path) -> None:
    """A reseed race must stop teardown before restoring the import lane."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_SEED_REAPPEAR="cleanup",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "binding_delete" not in events and "policy_delete" not in events
    assert "drain_set_false" in events and "drain_set_true" in events
    assert (tmp_path / "fake-state/drain").read_text() == "true"
    assert "quota_delete" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["cleanup"]["drain_restored"] is False
    assert receipt["cleanup"]["complete"] is False


def test_restore_timeout_after_commit_continues_from_readback(tmp_path: Path) -> None:
    """An ambiguous restore PATCH is safe when exact readback proves commit."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_DRAIN_RESTORE_ERROR="after",
    )

    assert result.returncode == 0, result.stderr
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "drain_restore_timeout_after" in events
    assert "binding_delete" in events and "policy_delete" in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


def test_restore_timeout_before_commit_retains_the_hard_fence(tmp_path: Path) -> None:
    """An uncommitted restore PATCH must retain VAP, quota, lock, and drain."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_DRAIN_RESTORE_ERROR="before",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "drain_restore_timeout_before" in events
    assert "binding_delete" not in events and "policy_delete" not in events
    assert "quota_delete" not in events and "lock_stop" not in events
    assert (tmp_path / "fake-state/drain").read_text() == "true"
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_active_engine_work_fails_before_child_and_cleans_fences(
    tmp_path: Path,
) -> None:
    """Any active engine Job or Pod must reject admission before the census."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ACTIVE_WORK="1",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "child" not in events
    assert events[-6:] == [
        "drain_set_false",
        "drain_read",
        "binding_delete",
        "policy_delete",
        "quota_delete",
        "lock_stop",
    ]
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True
