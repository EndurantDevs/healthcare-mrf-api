"""Failure-safety checks for the plan-pricing census envelope."""

import hashlib
import subprocess
from pathlib import Path

import pytest

from . import plan_pricing_projection_v3_census_envelope_harness as envelope


def test_envelope_uses_ordered_fences_and_reverse_uid_cleanup(tmp_path: Path) -> None:
    """A successful foreground census must release every exact outer fence."""

    run_result, state_root = envelope._run_envelope(tmp_path, FAKE_ARC_LISTENER="1")

    assert run_result.returncode == 0, run_result.stderr
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    expected_events = (
        "lock_create quota_create quota_probe_denied drain_read drain_set_true "
        "drain_read policy_create binding_create probe_denied capacity drain_read "
        "child drain_read drain_set_false drain_read binding_delete policy_delete "
        "quota_delete lock_stop"
    ).split()
    assert [event for event in events if event in expected_events] == expected_events
    assert events.count("zero_sample") == 5
    capacity_index = events.index("capacity")
    child_index = events.index("child")
    assert events[capacity_index + 1 : child_index] == ["drain_read", "zero_sample"]
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
    assert receipt["runtime_attestation"]["pod_uid"] == "pod-uid"
    assert (
        receipt["child_executable_sha256"]
        == receipt["expected_child_executable_sha256"]
    )
    assert receipt["capacity"] == {
        "verified": True,
        "host_available_memory_bytes": 2,
        "minimum_host_available_memory_bytes": 1,
        "host_swap_free_bytes": 2,
        "minimum_host_swap_free_bytes": 1,
        "postgresql_tablespace_path": str(tmp_path / "repo"),
        "postgresql_tablespace_free_bytes": 2,
        "minimum_postgresql_tablespace_free_bytes": 1,
    }
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
    assert "drain_set_false" in events
    assert events[-2:] == ["drain_set_true", "drain_read"]
    assert (tmp_path / "fake-state/drain").read_text() == "true"
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
    assert "drain_set_false" in events
    assert events[-2:] == ["drain_set_true", "drain_read"]
    assert (tmp_path / "fake-state/drain").read_text() == "true"
    assert "quota_delete" not in events
    assert "lock_stop" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["cleanup"]["binding_removed"] is True
    assert receipt["cleanup"]["complete"] is False


@pytest.mark.parametrize(
    "field_name",
    [
        "FAKE_HOST_AVAILABLE_MEMORY_BYTES",
        "FAKE_HOST_SWAP_FREE_BYTES",
        "FAKE_POSTGRESQL_TABLESPACE_FREE_BYTES",
    ],
)
def test_capacity_below_any_reviewed_minimum_never_starts_child(
    tmp_path: Path,
    field_name: str,
) -> None:
    """Every host and PostgreSQL minimum is a fail-closed final admission gate."""

    result, state_root = envelope._run_envelope(tmp_path, **{field_name: "0"})

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "capacity" in events
    assert "child" not in events
    receipt = envelope._receipt(state_root)
    assert receipt["status"] == "failed"
    assert receipt["capacity"]["verified"] is False
    assert receipt["cleanup"]["complete"] is True


def test_child_executable_change_during_capacity_never_starts_child(
    tmp_path: Path,
) -> None:
    """The reviewed executable is rehashed after the final capacity sample."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_REPLACE_CHILD_DURING_CAPACITY="1",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    replacement_index = events.index("child_executable_replaced")
    assert events[replacement_index - 1] == "capacity"
    assert "child" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


def test_child_executable_change_during_final_fence_never_starts_child(
    tmp_path: Path,
) -> None:
    """The executable is rehashed after the final mutable fence reads."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_REPLACE_CHILD_DURING_FINAL_FENCE="1",
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert events.index("capacity") < events.index(
        "child_executable_replaced_during_final_fence"
    )
    assert "child" not in events
    assert envelope._receipt(state_root)["cleanup"]["complete"] is True


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


def test_wrong_reviewed_envelope_hash_fails_before_any_mutation(tmp_path: Path) -> None:
    """A changed envelope script must fail before acquiring any outer fence."""

    env_by_name, state_root, checkout = envelope._fake_environment(tmp_path)
    arguments = envelope._arguments(state_root, checkout)
    hash_index = arguments.index("--expected-envelope-script-sha256") + 1
    arguments[hash_index] = "0" * 64
    result = subprocess.run(
        ["/bin/bash", str(envelope.SCRIPT), "run", *arguments],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_zero_minimum_swap_fails_before_any_mutation(tmp_path: Path) -> None:
    """Packet authority cannot disable the host-swap admission gate."""

    env_by_name, state_root, checkout = envelope._fake_environment(tmp_path)
    arguments = envelope._arguments(state_root, checkout)
    swap_index = arguments.index("--minimum-host-swap-free-bytes") + 1
    arguments[swap_index] = "0"
    result = subprocess.run(
        ["/bin/bash", str(envelope.SCRIPT), "run", *arguments],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_path_resolved_child_executable_fails_before_any_mutation(
    tmp_path: Path,
) -> None:
    """The reviewed launcher must not be replaceable through PATH lookup."""

    env_by_name, state_root, checkout = envelope._fake_environment(tmp_path)
    arguments = envelope._arguments(state_root, checkout)
    separator = arguments.index("--")
    arguments[separator + 1] = "census-child"
    child_command = arguments[separator + 1 :]
    command_bytes = b"".join(value.encode() + b"\0" for value in child_command)
    command_index = arguments.index("--expected-child-command-sha256") + 1
    arguments[command_index] = hashlib.sha256(command_bytes).hexdigest()
    result = subprocess.run(
        ["/bin/bash", str(envelope.SCRIPT), "run", *arguments],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
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


@pytest.mark.parametrize("kind", ("job", "pod", "configmap"))
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
