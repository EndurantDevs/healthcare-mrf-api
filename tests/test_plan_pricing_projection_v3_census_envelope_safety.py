"""Failure-safety checks for the plan-pricing census envelope."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from . import test_plan_pricing_projection_v3_census_envelope as envelope


def test_envelope_uses_ordered_fences_and_reverse_uid_cleanup(tmp_path: Path) -> None:
    """A successful foreground census must release every exact outer fence."""

    run_result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_LISTENER="1")

    assert run_result.returncode == 0, run_result.stderr
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    expected_events = [
        "lock_create",
        "quota_create",
        "quota_probe_denied",
        "drain_read",
        "drain_set_true",
        "drain_read",
        "policy_create",
        "binding_create",
        "probe_denied",
        "drain_read",
        "child",
        "drain_read",
        "binding_delete",
        "policy_delete",
        "drain_set_false",
        "quota_delete",
        "lock_stop",
    ]
    assert [event for event in events if event in expected_events] == expected_events
    assert events.count("zero_sample") == 5
    receipt = envelope._receipt(state_root)
    assert receipt["status"] == "complete"
    assert receipt["cleanup"]["complete"] is True
    assert receipt["probe_verified"] is True
    assert receipt["quota_probe_verified"] is True
    assert receipt["pre_child_fence_verified"] is True
    assert receipt["post_child_fence_verified"] is True
    assert receipt["prior_drain_mode"] is False
    receipt_path = state_root / "run/census-receipt.json"
    assert (
        receipt["census_receipt_sha256"]
        == hashlib.sha256(receipt_path.read_bytes()).hexdigest()
    )
    assert receipt["census_job"] == "plan-pricing-v3-census-test"
    assert "off-node PostgreSQL" in receipt["postgresql_boundary"]


def test_plan_renders_exact_fences_without_external_commands(tmp_path: Path) -> None:
    """The default plan must render the global hold without mutation."""

    state_root = tmp_path / "state"
    result = subprocess.run(
        [
            "/bin/bash",
            str(envelope.SCRIPT),
            "plan",
            *envelope._arguments(state_root, tmp_path),
        ],
        env={
            "PATH": "/nonexistent",
            "HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT": str(state_root),
        },
        check=True,
        capture_output=True,
        text=True,
    )

    assert "temporary global DEV build, ARC, and import hold" in result.stdout
    assert "separate direct authority is required" in result.stdout
    assert "Kubernetes QoS does not reserve or cap off-node PostgreSQL" in result.stdout
    assert 'pods: "0"' in result.stdout
    assert f"name: hp-pv3-census-quota-probe-{envelope.OWNER}" in result.stdout
    assert "failurePolicy: Fail" in result.stdout
    assert f"message: hp-pv3-census-deny-{envelope.OWNER}" in result.stdout
    assert not state_root.exists()


def test_uid_drift_retains_outer_fences(tmp_path: Path) -> None:
    """Cleanup must stop before removing outer protections after UID drift."""

    result, state_root = envelope._run_envelope(tmp_path, FAKE_DRIFT="policy")

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "binding_delete" in events
    assert "policy_delete" not in events
    assert "drain_set_false" not in events
    assert "quota_delete" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["cleanup"] == {
        "binding_removed": True,
        "complete": False,
        "drain_restored": False,
        "lock_released": False,
        "policy_removed": False,
        "quota_removed": False,
    }


def test_uid_replacement_during_delete_retains_outer_fences(tmp_path: Path) -> None:
    """A server-side UID precondition must retain a replacement resource."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_REPLACE_ON_DELETE="policy",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "binding_delete" in events
    assert "policy_replace" in events
    assert "policy_delete" not in events
    assert "drain_set_false" not in events
    assert "quota_delete" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["cleanup"]["binding_removed"] is True
    assert receipt["cleanup"]["complete"] is False


def test_uid_bound_delete_waits_for_same_uid_finalization(tmp_path: Path) -> None:
    """Foreground deletion must wait through same-UID terminating reads."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_DELETE_LINGER="policy",
    )

    assert result.returncode == 0, result.stderr
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


def test_native_124_is_not_misclassified_as_owned_deadline(tmp_path: Path) -> None:
    """A child-native 124 must stay distinct from the wrapper deadline."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="native124",
    )

    assert result.returncode == 124
    receipt = envelope._receipt(state_root)
    assert receipt["child_exit_code"] == 124
    assert receipt["timed_out"] is False
    assert receipt["cleanup"]["complete"] is True


def test_fast_nonzero_child_status_is_preserved(tmp_path: Path) -> None:
    """A child that exits immediately must retain its actual failure status."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="exit7",
    )

    assert result.returncode == 7
    receipt = envelope._receipt(state_root)
    assert receipt["child_exit_code"] == 7
    assert receipt["cleanup"]["complete"] is True


def test_lingering_child_group_is_terminated_and_fails(tmp_path: Path) -> None:
    """Background work after a zero exit must be reaped and fail the run."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_CHILD_MODE="linger",
    )

    assert result.returncode == 1
    receipt = envelope._receipt(state_root)
    assert receipt["child_exit_code"] == 1
    assert receipt["cleanup"]["complete"] is True


def test_preexisting_resource_fails_before_any_mutation(tmp_path: Path) -> None:
    """A colliding quota name must stop before the build lock is acquired."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_PREEXISTING="quota",
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_missing_v1_admission_api_fails_before_any_mutation(tmp_path: Path) -> None:
    """The exact admission API must exist before any outer fence is created."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_V1_ADMISSION_MISSING="1",
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


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


def test_preexisting_census_pod_fails_before_any_mutation(tmp_path: Path) -> None:
    """A labeled census Pod must fail before acquiring any outer fence."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_PREEXISTING_CENSUS_POD="1",
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_seed_reappearance_retains_drain_quota_and_lock(tmp_path: Path) -> None:
    """A reseed race must stop teardown before restoring the import lane."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_SEED_REAPPEAR="cleanup",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "binding_delete" in events and "policy_delete" in events
    assert "drain_set_false" not in events
    assert "quota_delete" not in events
    assert "lock_stop" not in events
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
    assert events[-5:] == [
        "binding_delete",
        "policy_delete",
        "drain_set_false",
        "quota_delete",
        "lock_stop",
    ]
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


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
    assert "child" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


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

    assert result.returncode == 1
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

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_ARC_DRAIN=arc_kind,
    )

    assert result.returncode == 0
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert f"arc_active_{arc_kind}" in events
    assert events.index(f"arc_active_{arc_kind}") < events.index("child")
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True
