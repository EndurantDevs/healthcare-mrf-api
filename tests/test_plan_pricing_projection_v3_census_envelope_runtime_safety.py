"""Runtime-boundary checks for the plan-pricing census envelope."""

import time
from pathlib import Path

import pytest

from . import plan_pricing_projection_v3_census_envelope_harness as envelope


@pytest.mark.parametrize(
    ("mode", "expected_exit"),
    [
        ("missing", 1),
        ("malformed", 143),
        ("bare-digest", 143),
        ("empty-image", 143),
        ("symlink", 143),
        ("job-replaced", 143),
        ("pod-replaced", 143),
        ("annotation-mismatch", 143),
        ("job-source-mismatch", 143),
        ("pod-source-mismatch", 143),
        ("configmap-mutable", 143),
        ("overlay-mismatch", 143),
        ("configmap-replaced", 143),
        ("configmap-replaced-before-first-read", 143),
    ],
)
def test_invalid_runtime_attestation_cannot_complete_envelope(
    tmp_path: Path,
    mode: str,
    expected_exit: int,
) -> None:
    """Only a regular exact Kubernetes runtime attestation is admissible."""

    envelope_result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ATTESTATION_MODE=mode,
    )

    assert envelope_result.returncode == expected_exit
    if mode != "empty-image":
        pod_reads = tmp_path / "fake-state/attestation-pod-reads"
        assert not pod_reads.exists() or int(pod_reads.read_text()) <= 2
    if mode == "missing":
        assert not (state_root / "run/child-jobs.tmp").exists()
        assert not (state_root / "run/child-deadline-fired").exists()
    receipt = envelope._receipt(state_root)
    if mode == "missing":
        assert receipt["child_exit_code"] == 1
    if mode == "empty-image":
        assert int((tmp_path / "fake-state/attestation-pod-reads").read_text()) > 2
    if expected_exit == 143:
        events = (tmp_path / "fake-state/events").read_text().splitlines()
        assert events.count("child_signal_15") == 1
    assert receipt["status"] == "failed"
    assert receipt["runtime_attestation"] is None
    assert receipt["cleanup"]["complete"] is False


def test_child_cannot_replace_parent_runtime_attestation(tmp_path: Path) -> None:
    """Final attestation bytes must equal the parent's private captured digest."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="mutate-attestation",
    )

    assert result.returncode == 1
    receipt = envelope._receipt(state_root)
    assert receipt["child_exit_code"] == 0
    assert receipt["runtime_attestation"] is None
    assert receipt["cleanup"]["complete"] is False
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events


def test_arc_zero_must_be_stable_before_import_drain(tmp_path: Path) -> None:
    """A late ARC Pod between zero samples must restart the idle proof."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ARC_LATE_CALL="2",
    )

    assert result.returncode == 0
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert events.index("arc_late_2") < events.index("drain_set_true")
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


def test_arc_reappearance_at_pre_child_fence_blocks_run(tmp_path: Path) -> None:
    """ARC work appearing after the idle proof must block the census child."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ARC_LATE_CALL="4",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "arc_late_4" in events
    assert events.index("capacity") < events.index("arc_late_4")
    assert "child" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["capacity"]["verified"] is True
    assert receipt["pre_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is True


@pytest.mark.parametrize(
    "drift",
    [
        {"FAKE_ACTIVE_WORK_AFTER_CHILD": "1"},
        {"FAKE_ARC_AFTER_CHILD": "1"},
        {"FAKE_SCHEDULER_DRIFT_AFTER_CHILD": "1"},
    ],
)
def test_post_child_work_drift_rejects_receipt(
    tmp_path: Path, drift: dict[str, str]
) -> None:
    """Scheduler, engine, or ARC drift must reject post-child acceptance."""

    result, state_root = envelope._run_envelope(tmp_path, **drift)

    assert result.returncode == 1
    assert "child" in (tmp_path / "fake-state/events").read_text().splitlines()
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is True


def test_timeout_with_census_objects_retains_every_outer_fence(
    tmp_path: Path,
) -> None:
    """An orphaned census must never outlive the admission protections."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="native124",
        FAKE_CHILD_ORPHAN="1",
    )

    assert result.returncode == 124
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_orphaned_census_pod_retains_every_outer_fence(tmp_path: Path) -> None:
    """A census Pod must keep the envelope even after Job and ConfigMap vanish."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_ORPHAN_POD="1",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_unlabeled_attested_pod_retains_every_outer_fence(tmp_path: Path) -> None:
    """The exact attested Pod must be absent even if its label disappears."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_UNLABELED_ATTESTED_POD_AFTER_CHILD="1",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


def test_nonzero_child_unlabeled_attested_pod_retains_fences(tmp_path: Path) -> None:
    """A failed child still binds cleanup to its exact attested Pod."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="exit7",
        FAKE_UNLABELED_ATTESTED_POD_AFTER_CHILD="1",
    )

    assert result.returncode == 7
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is False


@pytest.mark.parametrize("resource", ("quota", "policy", "binding"))
def test_create_then_client_failure_reconciles_exact_resource(
    tmp_path: Path,
    resource: str,
) -> None:
    """An ambiguous create result must still UID-clean its exact resource."""

    result, state_root = envelope._run_envelope(tmp_path, FAKE_CREATE_ERROR=resource)

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert f"{resource}_create" in events
    assert f"{resource}_delete" in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


@pytest.mark.parametrize("failure_mode", ("contended", "client-error"))
def test_lock_must_be_proven_before_admission(
    tmp_path: Path,
    failure_mode: str,
) -> None:
    """A transient unit without a proven flock cannot admit later fences."""

    environment = (
        {"FAKE_LOCK_CONTENDED": "1"}
        if failure_mode == "contended"
        else {"FAKE_LOCK_CLIENT_ERROR": "1"}
    )
    result, state_root = envelope._run_envelope(tmp_path, **environment)

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert events == ["lock_create", "lock_stop"]
    assert envelope._receipt(state_root)["cleanup"]["lock_released"] is True


def test_child_drain_drift_fails_receipt_before_release(tmp_path: Path) -> None:
    """The child boundary must re-attest semantic import drain continuity."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_DRAIN_DRIFT_AFTER_CHILD="1",
    )

    assert result.returncode == 1
    receipt = envelope._receipt(state_root)
    assert receipt["pre_child_fence_verified"] is True
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is True


def test_post_child_api_error_retains_every_outer_fence(tmp_path: Path) -> None:
    """An unreadable absence check must retain every outer protection."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_GET_ERROR_AFTER_CHILD="configmap",
    )

    assert result.returncode == 143
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is False


@pytest.mark.parametrize("drift", ["gone", "replaced"])
def test_post_child_lock_drift_retains_every_outer_fence(
    tmp_path: Path,
    drift: str,
) -> None:
    """A lost or replaced build flock must block every fence deletion."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_LOCK_DRIFT_AFTER_CHILD=drift,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is False


@pytest.mark.parametrize("resource", ("quota", "policy", "binding"))
def test_same_uid_spec_drift_retains_every_outer_fence(
    tmp_path: Path,
    resource: str,
) -> None:
    """A same-UID semantic change must stop before any fence is released."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_SPEC_DRIFT=resource,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert not any(event.endswith("_delete") for event in events)
    assert "drain_set_false" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["post_child_fence_verified"] is False
    assert receipt["cleanup"]["complete"] is False


@pytest.mark.parametrize("arc_kind", ("runner", "import", "mrf"))
def test_arc_workload_must_naturally_drain(
    tmp_path: Path,
    arc_kind: str,
) -> None:
    """Each ephemeral runner and workflow namespace must delay admission."""

    started = time.monotonic()
    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ARC_DRAIN=arc_kind,
    )

    assert result.returncode == 0
    assert time.monotonic() - started < 20
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert f"arc_active_{arc_kind}" in events
    assert events.index(f"arc_active_{arc_kind}") < events.index("child")
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True
