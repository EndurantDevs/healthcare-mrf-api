"""Admission and final cleanup checks for the census envelope."""

import hashlib
from pathlib import Path
import subprocess

import pytest
import yaml

from . import plan_pricing_projection_v3_census_envelope_harness as envelope


def test_plan_renders_exact_fences_without_external_commands(tmp_path: Path) -> None:
    """The default plan must render the global hold without mutation."""

    state_root = tmp_path / "state"
    plan_result = subprocess.run(
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

    assert "temporary global DEV build, ARC, and import hold" in plan_result.stdout
    assert "separate direct authority is required" in plan_result.stdout
    assert "Kubernetes QoS does not reserve or cap off-node PostgreSQL" in plan_result.stdout
    assert 'pods: "0"' in plan_result.stdout
    assert f"name: hp-pv3-census-quota-probe-{envelope.OWNER}" in plan_result.stdout
    assert "        runAsNonRoot: true\n        runAsUser: 65532" in plan_result.stdout
    assert "failurePolicy: Fail" in plan_result.stdout
    assert f"message: hp-pv3-census-deny-{envelope.OWNER}" in plan_result.stdout
    probe_job_manifest = yaml.safe_load(
        plan_result.stdout.split("--- server-dry-run probe ---\n", 1)[1]
    )
    probe_pod_spec = probe_job_manifest["spec"]["template"]["spec"]
    assert probe_pod_spec["automountServiceAccountToken"] is False
    assert probe_pod_spec["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    probe_worker_spec = probe_pod_spec["containers"][0]
    assert probe_worker_spec["imagePullPolicy"] == "IfNotPresent"
    assert probe_worker_spec["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    assert not state_root.exists()


def test_missing_v1_admission_api_fails_before_any_mutation(tmp_path: Path) -> None:
    """The exact admission API must exist before any outer fence is created."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_V1_ADMISSION_MISSING="1",
    )

    assert result.returncode == 1
    assert not (tmp_path / "fake-state/events").exists()
    assert not (state_root / "run").exists()


def test_worker_fence_waits_for_owner_denial_propagation(tmp_path: Path) -> None:
    """A newly created policy may allow probes until its cache refreshes."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_PROBE_ALLOWED_ATTEMPTS="2",
    )

    assert result.returncode == 0, result.stderr
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert events.count("probe_allowed") == 2
    assert events.index("probe_allowed") < events.index("probe_denied") < events.index("child")
    receipt = envelope._receipt(state_root)
    assert receipt["probe_verified"] is True
    assert receipt["cleanup"]["complete"] is True


def test_worker_fence_fails_closed_when_denial_never_propagates(tmp_path: Path) -> None:
    """An admission cache that never activates must not launch the census."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_PROBE_ALLOWED_ATTEMPTS="999999",
    )

    assert result.returncode == 1
    assert "engine-worker denial policy did not propagate" in result.stderr
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
    receipt = envelope._receipt(state_root)
    assert receipt["probe_verified"] is False
    assert receipt["cleanup"]["complete"] is True


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


@pytest.mark.parametrize(
    "environment",
    [
        {"FAKE_SEED_REAPPEAR": "preexisting-pod"},
        {"FAKE_SEED_POD_LIST_ERROR": "1"},
    ],
)
def test_seed_pod_presence_or_unreadability_fails_before_mutation(
    tmp_path: Path,
    environment: dict[str, str],
) -> None:
    """Seed Pod presence or unreadability must fail before any outer fence."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        **environment,
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


@pytest.mark.parametrize("kind", ["job", "pod"])
def test_seed_reappearance_retains_drain_quota_and_lock(
    tmp_path: Path,
    kind: str,
) -> None:
    """A reseed race must stop teardown before restoring the import lane."""

    result, state_root = envelope._run_envelope(
        tmp_path,
        FAKE_SEED_REAPPEAR=kind,
    )

    assert result.returncode == 1
    events = (tmp_path / "fake-state/events").read_text().splitlines()
    assert "binding_delete" not in events and "policy_delete" not in events
    assert "drain_set_true" in events
    assert ("drain_set_false" in events) is (kind == "job")
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
