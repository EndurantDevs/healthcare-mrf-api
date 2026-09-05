# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Outer-envelope acceptance authority for the projection-v3 census."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from scripts.research import plan_pricing_projection_v3_census_contract as contract
from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from scripts.research import plan_pricing_projection_v3_census_identity as identity

CENSUS_ENVELOPE_CONTRACT = "healthporta.plan-pricing-v3-census-envelope.v1"
CENSUS_RUNTIME_ATTESTATION_CONTRACT = (
    "healthporta.plan-pricing-v3-census-runtime-attestation.v1"
)
CENSUS_ENVELOPE_SCRIPT_PATH = identity.CENSUS_ENVELOPE_SCRIPT_PATH
_EXPECTED_ENVELOPE_KEYS = frozenset(
    {
        "contract",
        "status",
        "exit_code",
        "reviewed_source_sha",
        "envelope_script_sha256",
        "expected_envelope_script_sha256",
        "owner_token",
        "resource_uids",
        "prior_drain_mode",
        "child_command_sha256",
        "expected_child_command_sha256",
        "child_executable_sha256",
        "expected_child_executable_sha256",
        "expected_source_manifest_sha256",
        "expected_harness_manifest_sha256",
        "expected_source_overlay_sha256",
        "child_exit_code",
        "census_job",
        "census_configmap",
        "census_receipt_sha256",
        "timed_out",
        "probe_verified",
        "quota_probe_verified",
        "pre_child_fence_verified",
        "post_child_fence_verified",
        "capacity",
        "runtime_attestation",
        "runtime_attestation_sha256",
        "cleanup",
        "postgresql_boundary",
    }
)
_EXPECTED_RUNTIME_ATTESTATION_KEYS = frozenset(
    {
        "contract",
        "job_name",
        "job_uid",
        "pod_name",
        "pod_uid",
        "pod_owner_job_name",
        "pod_owner_job_uid",
        "container_name",
        "image_id",
        "source_sha",
        "source_manifest_sha256",
        "harness_manifest_sha256",
        "source_overlay_sha256",
        "configmap_name",
        "configmap_uid",
        "job_source_configmap_name",
        "pod_source_configmap_name",
    }
)
_EXPECTED_AUTHORITY_KEYS = frozenset(
    {
        "expected_source_sha",
        "expected_envelope_script_sha256",
        "expected_child_command_sha256",
        "expected_child_executable_sha256",
        "expected_source_manifest_sha256",
        "expected_harness_manifest_sha256",
        "expected_source_overlay_sha256",
        "expected_census_job",
        "expected_census_configmap",
        "expected_target",
        "runtime_attestation",
        "capacity",
    }
)
_EXPECTED_CAPACITY_KEYS = frozenset(
    {
        "verified",
        "host_available_memory_bytes",
        "minimum_host_available_memory_bytes",
        "host_swap_free_bytes",
        "minimum_host_swap_free_bytes",
        "postgresql_tablespace_path",
        "postgresql_tablespace_free_bytes",
        "minimum_postgresql_tablespace_free_bytes",
    }
)
_EXPECTED_CAPACITY_AUTHORITY_KEYS = frozenset(
    {
        "minimum_host_available_memory_bytes",
        "minimum_host_swap_free_bytes",
        "postgresql_tablespace_path",
        "minimum_postgresql_tablespace_free_bytes",
    }
)
_EXPECTED_ENVELOPE_RESOURCE_KEYS = frozenset(
    {"quota", "policy", "binding", "lock_invocation"}
)
_EXPECTED_ENVELOPE_CLEANUP_KEYS = frozenset(
    {
        "binding_removed",
        "policy_removed",
        "drain_restored",
        "quota_removed",
        "arc_capacity_restored",
        "lock_released",
        "complete",
    }
)


def census_receipt_sha256(receipt_by_field: Mapping[str, Any]) -> str:
    """Hash the exact canonical bytes written by the census receipt writer."""

    serialized = json.dumps(receipt_by_field, indent=2, sort_keys=True) + "\n"
    return hashlib.sha256(serialized.encode()).hexdigest()


def _is_sha256(field_value: Any) -> bool:
    return (
        isinstance(field_value, str)
        and len(field_value) == 64
        and not (set(field_value) - set("0123456789abcdef"))
    )


def _is_git_sha(field_value: Any) -> bool:
    return (
        isinstance(field_value, str)
        and len(field_value) == 40
        and not (set(field_value) - set("0123456789abcdef"))
    )


def _is_capacity_valid(capacity_by_field: Any) -> bool:
    if (
        not isinstance(capacity_by_field, Mapping)
        or frozenset(capacity_by_field) != _EXPECTED_CAPACITY_KEYS
        or capacity_by_field.get("verified") is not True
        or not isinstance(capacity_by_field.get("postgresql_tablespace_path"), str)
        or not capacity_by_field["postgresql_tablespace_path"].startswith("/")
    ):
        return False
    integer_fields = _EXPECTED_CAPACITY_KEYS - {
        "verified",
        "postgresql_tablespace_path",
    }
    if any(
        type(capacity_by_field[field_name]) is not int
        or capacity_by_field[field_name] < 0
        for field_name in integer_fields
    ):
        return False
    return (
        capacity_by_field["minimum_host_available_memory_bytes"] > 0
        and capacity_by_field["minimum_host_swap_free_bytes"] > 0
        and capacity_by_field["minimum_postgresql_tablespace_free_bytes"] > 0
        and capacity_by_field["host_available_memory_bytes"]
        >= capacity_by_field["minimum_host_available_memory_bytes"]
        and capacity_by_field["host_swap_free_bytes"]
        >= capacity_by_field["minimum_host_swap_free_bytes"]
        and capacity_by_field["postgresql_tablespace_free_bytes"]
        >= capacity_by_field["minimum_postgresql_tablespace_free_bytes"]
    )


def _is_successful_envelope(envelope_by_field: Mapping[str, Any]) -> bool:
    cleanup_by_field = envelope_by_field.get("cleanup")
    resource_uids = envelope_by_field.get("resource_uids")
    return (
        frozenset(envelope_by_field) == _EXPECTED_ENVELOPE_KEYS
        and envelope_by_field.get("contract") == CENSUS_ENVELOPE_CONTRACT
        and envelope_by_field.get("status") == "complete"
        and type(envelope_by_field.get("exit_code")) is int
        and envelope_by_field["exit_code"] == 0
        and type(envelope_by_field.get("child_exit_code")) is int
        and envelope_by_field["child_exit_code"] == 0
        and envelope_by_field.get("timed_out") is False
        and envelope_by_field.get("probe_verified") is True
        and envelope_by_field.get("quota_probe_verified") is True
        and envelope_by_field.get("pre_child_fence_verified") is True
        and envelope_by_field.get("post_child_fence_verified") is True
        and type(envelope_by_field.get("prior_drain_mode")) is bool
        and isinstance(envelope_by_field.get("owner_token"), str)
        and bool(envelope_by_field["owner_token"])
        and all(
            _is_sha256(envelope_by_field.get(field_name))
            for field_name in (
                "envelope_script_sha256",
                "expected_envelope_script_sha256",
                "child_command_sha256",
                "expected_child_command_sha256",
                "child_executable_sha256",
                "expected_child_executable_sha256",
                "expected_source_manifest_sha256",
                "expected_harness_manifest_sha256",
                "expected_source_overlay_sha256",
                "runtime_attestation_sha256",
            )
        )
        and isinstance(envelope_by_field.get("census_job"), str)
        and bool(envelope_by_field["census_job"])
        and isinstance(envelope_by_field.get("census_configmap"), str)
        and bool(envelope_by_field["census_configmap"])
        and envelope_by_field.get("postgresql_boundary")
        == "Kubernetes QoS does not reserve or cap off-node PostgreSQL"
        and isinstance(cleanup_by_field, Mapping)
        and frozenset(cleanup_by_field) == _EXPECTED_ENVELOPE_CLEANUP_KEYS
        and all(field_value is True for field_value in cleanup_by_field.values())
        and isinstance(resource_uids, Mapping)
        and frozenset(resource_uids) == _EXPECTED_ENVELOPE_RESOURCE_KEYS
        and all(
            isinstance(field_value, str) and field_value
            for field_value in resource_uids.values()
        )
        and _is_capacity_valid(envelope_by_field.get("capacity"))
    )


def _is_capacity_authority_match(
    capacity_by_field: Any,
    expected_by_field: Any,
) -> bool:
    if (
        not isinstance(capacity_by_field, Mapping)
        or not isinstance(expected_by_field, Mapping)
        or frozenset(expected_by_field) != _EXPECTED_CAPACITY_AUTHORITY_KEYS
        or not isinstance(expected_by_field.get("postgresql_tablespace_path"), str)
        or not expected_by_field["postgresql_tablespace_path"].startswith("/")
    ):
        return False
    minimum_fields = _EXPECTED_CAPACITY_AUTHORITY_KEYS - {"postgresql_tablespace_path"}
    if any(
        type(expected_by_field[field_name]) is not int
        or expected_by_field[field_name] <= 0
        for field_name in minimum_fields
    ):
        return False
    return all(
        capacity_by_field.get(field_name) == field_value
        for field_name, field_value in expected_by_field.items()
    )


def _is_image_identity_match(image_digest: Any, image_id: str) -> bool:
    """Require one exact Kubernetes image ID for a sha256 digest."""

    return (
        isinstance(image_digest, str)
        and image_digest.startswith("sha256:")
        and len(image_digest) == 71
        and not (set(image_digest[7:]) - set("0123456789abcdef"))
        and (
            image_id == f"containerd://{image_digest}"
            or image_id.endswith(f"@{image_digest}")
        )
    )


def _is_runtime_attestation_match(
    runtime_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
    authority_by_field: Mapping[str, Any],
    attestation_by_field: Any,
) -> bool:
    """Bind the inner runtime identity to exact external Kubernetes evidence."""

    if (
        not isinstance(attestation_by_field, Mapping)
        or frozenset(attestation_by_field) != _EXPECTED_RUNTIME_ATTESTATION_KEYS
        or not all(
            isinstance(field_value, str) and bool(field_value)
            for field_value in attestation_by_field.values()
        )
    ):
        return False
    image_id = attestation_by_field["image_id"]
    image_digest = runtime_by_field.get("image_digest")
    return (
        attestation_by_field["contract"] == CENSUS_RUNTIME_ATTESTATION_CONTRACT
        and attestation_by_field["job_name"] == runtime_by_field.get("job_name")
        and attestation_by_field["job_name"] == envelope_by_field.get("census_job")
        and attestation_by_field["job_name"]
        == authority_by_field.get("expected_census_job")
        and attestation_by_field["pod_name"] == runtime_by_field.get("pod_name")
        and attestation_by_field["pod_uid"] == runtime_by_field.get("pod_uid")
        and attestation_by_field["pod_owner_job_name"]
        == attestation_by_field["job_name"]
        and attestation_by_field["pod_owner_job_uid"] == attestation_by_field["job_uid"]
        and attestation_by_field["container_name"]
        == runtime_by_field.get("container_name")
        and attestation_by_field["source_sha"]
        == envelope_by_field.get("reviewed_source_sha")
        == authority_by_field.get("expected_source_sha")
        and attestation_by_field["source_manifest_sha256"]
        == envelope_by_field.get("expected_source_manifest_sha256")
        == authority_by_field.get("expected_source_manifest_sha256")
        and attestation_by_field["harness_manifest_sha256"]
        == envelope_by_field.get("expected_harness_manifest_sha256")
        == authority_by_field.get("expected_harness_manifest_sha256")
        and attestation_by_field["source_overlay_sha256"]
        == envelope_by_field.get("expected_source_overlay_sha256")
        == authority_by_field.get("expected_source_overlay_sha256")
        and attestation_by_field["configmap_name"]
        == envelope_by_field.get("census_configmap")
        == authority_by_field.get("expected_census_configmap")
        and attestation_by_field["job_source_configmap_name"]
        == attestation_by_field["configmap_name"]
        and attestation_by_field["pod_source_configmap_name"]
        == attestation_by_field["configmap_name"]
        and _is_image_identity_match(image_digest, image_id)
        and runtime_by_field.get("identity_contract")
        == "immutable-image-plus-source-overlay-v1"
        and runtime_by_field.get("external_pod_image_id_attestation_required") is True
    )


def _external_evidence(
    receipt_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
    authority_by_field: Mapping[str, Any],
) -> tuple[Mapping[str, Any], str] | None:
    if (
        not isinstance(authority_by_field, Mapping)
        or frozenset(authority_by_field) != _EXPECTED_AUTHORITY_KEYS
        or not all(
            _is_sha256(authority_by_field.get(field_name))
            for field_name in (
                "expected_envelope_script_sha256",
                "expected_child_command_sha256",
                "expected_child_executable_sha256",
                "expected_source_manifest_sha256",
                "expected_harness_manifest_sha256",
                "expected_source_overlay_sha256",
            )
        )
        or not _is_git_sha(authority_by_field.get("expected_source_sha"))
        or not all(
            isinstance(authority_by_field.get(field_name), str)
            and bool(authority_by_field[field_name])
            for field_name in ("expected_census_job", "expected_census_configmap")
        )
    ):
        return None
    attestation_bytes = authority_by_field.get("runtime_attestation")
    if not isinstance(attestation_bytes, bytes):
        return None
    try:
        attestation_by_field = json.loads(attestation_bytes)
        receipt_sha256 = census_receipt_sha256(receipt_by_field)
        identity.validated_target(authority_by_field.get("expected_target"))
    except (TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if hashlib.sha256(attestation_bytes).hexdigest() != envelope_by_field.get(
        "runtime_attestation_sha256"
    ) or not _is_capacity_authority_match(
        envelope_by_field.get("capacity"), authority_by_field.get("capacity")
    ):
        return None
    return attestation_by_field, receipt_sha256


def _is_reviewed_identity_bound(
    receipt_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
    authority_by_field: Mapping[str, Any],
    attestation_by_field: Mapping[str, Any],
    receipt_sha256: str,
) -> bool:
    """Bind reviewed process, runtime, source, target, and resource identity."""

    runtime_by_field = receipt_by_field.get("runtime")
    return (
        isinstance(runtime_by_field, Mapping)
        and authority_by_field["expected_envelope_script_sha256"]
        == envelope_by_field.get("expected_envelope_script_sha256")
        == envelope_by_field.get("envelope_script_sha256")
        and authority_by_field["expected_child_command_sha256"]
        == envelope_by_field.get("expected_child_command_sha256")
        == envelope_by_field.get("child_command_sha256")
        and authority_by_field["expected_child_executable_sha256"]
        == envelope_by_field.get("expected_child_executable_sha256")
        == envelope_by_field.get("child_executable_sha256")
        and envelope_by_field.get("runtime_attestation") == attestation_by_field
        and _is_runtime_attestation_match(
            runtime_by_field,
            envelope_by_field,
            authority_by_field,
            attestation_by_field,
        )
        and envelope_by_field.get("census_job") == runtime_by_field.get("job_name")
        and envelope_by_field.get("census_receipt_sha256") == receipt_sha256
        and authority_by_field["expected_source_sha"]
        == envelope_by_field.get("reviewed_source_sha")
        and authority_by_field["expected_source_manifest_sha256"]
        == envelope_by_field.get("expected_source_manifest_sha256")
        and authority_by_field["expected_harness_manifest_sha256"]
        == envelope_by_field.get("expected_harness_manifest_sha256")
        and authority_by_field["expected_source_overlay_sha256"]
        == envelope_by_field.get("expected_source_overlay_sha256")
        and identity.is_source_pair_bound(
            receipt_by_field,
            envelope_by_field.get("reviewed_source_sha"),
            authority_by_field["expected_source_manifest_sha256"],
            authority_by_field["expected_harness_manifest_sha256"],
            authority_by_field["expected_envelope_script_sha256"],
        )
        and authority_by_field["expected_census_job"]
        == envelope_by_field.get("census_job")
        and authority_by_field["expected_census_configmap"]
        == envelope_by_field.get("census_configmap")
        and authority_by_field["expected_target"]
        == receipt_by_field.get("expected_target")
    )


def is_authoritative_envelope(
    receipt_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
    authority_by_field: Mapping[str, Any],
) -> bool:
    """Bind the exact inner receipt to one successful outer process envelope."""

    external_evidence = _external_evidence(
        receipt_by_field,
        envelope_by_field,
        authority_by_field,
    )
    if external_evidence is None:
        return False
    attestation_by_field, receipt_sha256 = external_evidence
    return (
        receipt_by_field.get("contract") == diagnostics.CENSUS_RECEIPT_CONTRACT
        and receipt_by_field.get("status") == "provisional"
        and receipt_by_field.get("accepted") is False
        and receipt_by_field.get("mode") == "cardinality_census"
        and receipt_by_field.get("cap_calibration_admissible") is False
        and receipt_by_field.get("resource_proof_admissible") is False
        and receipt_by_field.get("proof_scope") == "row_count_limits_only"
        and receipt_by_field.get("acceptance_authority")
        == diagnostics.CENSUS_ACCEPTANCE_AUTHORITY
        and _is_reviewed_identity_bound(
            receipt_by_field,
            envelope_by_field,
            authority_by_field,
            attestation_by_field,
            receipt_sha256,
        )
        and _is_successful_envelope(envelope_by_field)
    )


def is_accepted(
    receipt_by_field: Mapping[str, Any],
    envelope_by_field: Mapping[str, Any],
    authority_by_field: Mapping[str, Any],
) -> bool:
    """Accept a cardinality candidate only with exact external authority."""

    measured_result = receipt_by_field.get("measurement")
    source_before = receipt_by_field.get("source_before")
    source_after = receipt_by_field.get("source_after")
    return (
        isinstance(measured_result, Mapping)
        and isinstance(source_before, Mapping)
        and source_before == source_after
        and contract.is_cardinality_candidate_accepted(
            receipt_by_field,
            measured_result,
            source_before == source_after,
        )
        and is_authoritative_envelope(
            receipt_by_field,
            envelope_by_field,
            authority_by_field,
        )
    )
