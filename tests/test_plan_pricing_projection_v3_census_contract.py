# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed receipt gates for the projection-v3 census."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest

from scripts.research import plan_pricing_projection_v3_census as census
from scripts.research import plan_pricing_projection_v3_census_authority as authority
from scripts.research import plan_pricing_projection_v3_census_contract as contract
from scripts.research import (
    plan_pricing_projection_v3_census_diagnostics as diagnostics,
)
from scripts.research import plan_pricing_projection_v3_census_identity as identity
from scripts.research import plan_pricing_projection_v3_census_support as support

_RUNTIME = {
    "job_name": "census-job",
    "pod_name": "census-pod",
    "pod_uid": "pod-uid",
    "image_digest": "sha256:" + "c" * 64,
    "container_name": "census",
    "identity_contract": "immutable-image-plus-source-overlay-v1",
    "external_pod_image_id_attestation_required": True,
}
_SOURCE_SHA = "a" * 40
_ENVELOPE_SHA = "b" * 64
_COMMAND_SHA = "c" * 64
_EXECUTABLE_SHA = "d" * 64
_SOURCE_FILES = [[path, "7" * 64] for path in support.SOURCE_PATHS]
_HARNESS_FILES = [
    [path, _ENVELOPE_SHA if path == identity.CENSUS_ENVELOPE_SCRIPT_PATH else "8" * 64]
    for path in support.HARNESS_PATHS
]
_SOURCE_MANIFEST_SHA = support._canonical_sha256(_SOURCE_FILES)
_HARNESS_MANIFEST_SHA = support._canonical_sha256(_HARNESS_FILES)
_OVERLAY_SHA = "1" * 64
_CONFIGMAP = "census-source"
_PROVIDER_SIGNATURE = "2" * 64
_TARGET = {
    "healthporta_plan_id": "plan",
    "plan_release_id": "release",
    "serving_revision_id": "serving",
    "binding_set_digest": "3" * 64,
    "binding_count": 3,
    "in_network_binding_count": 3,
    "distinct_snapshot_count": 3,
    "distinct_plan_count": 1,
}


def _runtime_attestation() -> dict:
    return {
        "contract": authority.CENSUS_RUNTIME_ATTESTATION_CONTRACT,
        "job_name": _RUNTIME["job_name"],
        "job_uid": "job-uid",
        "pod_name": _RUNTIME["pod_name"],
        "pod_uid": _RUNTIME["pod_uid"],
        "pod_owner_job_name": _RUNTIME["job_name"],
        "pod_owner_job_uid": "job-uid",
        "container_name": _RUNTIME["container_name"],
        "image_id": "docker-pullable://example@" + _RUNTIME["image_digest"],
        "source_sha": _SOURCE_SHA,
        "source_manifest_sha256": _SOURCE_MANIFEST_SHA,
        "harness_manifest_sha256": _HARNESS_MANIFEST_SHA,
        "source_overlay_sha256": _OVERLAY_SHA,
        "configmap_name": _CONFIGMAP,
        "configmap_uid": "configmap-uid",
        "job_source_configmap_name": _CONFIGMAP,
        "pod_source_configmap_name": _CONFIGMAP,
    }


def _runtime_attestation_bytes() -> bytes:
    serialized = json.dumps(
        _runtime_attestation(), sort_keys=True, separators=(",", ":")
    )
    return f"{serialized}\n".encode()


def _runtime_attestation_sha256() -> str:
    return hashlib.sha256(_runtime_attestation_bytes()).hexdigest()


def _successful_envelope(receipt_by_field: dict) -> dict:
    return {
        "contract": authority.CENSUS_ENVELOPE_CONTRACT,
        "status": "complete",
        "exit_code": 0,
        "reviewed_source_sha": _SOURCE_SHA,
        "envelope_script_sha256": _ENVELOPE_SHA,
        "expected_envelope_script_sha256": _ENVELOPE_SHA,
        "owner_token": "testowner1",
        "resource_uids": {
            "quota": "quota-uid",
            "policy": "policy-uid",
            "binding": "binding-uid",
            "lock_invocation": "lock-uid",
        },
        "prior_drain_mode": False,
        "child_command_sha256": _COMMAND_SHA,
        "expected_child_command_sha256": _COMMAND_SHA,
        "child_executable_sha256": _EXECUTABLE_SHA,
        "expected_child_executable_sha256": _EXECUTABLE_SHA,
        "expected_source_manifest_sha256": _SOURCE_MANIFEST_SHA,
        "expected_harness_manifest_sha256": _HARNESS_MANIFEST_SHA,
        "expected_source_overlay_sha256": _OVERLAY_SHA,
        "child_exit_code": 0,
        "census_job": receipt_by_field["runtime"]["job_name"],
        "census_configmap": _CONFIGMAP,
        "census_receipt_sha256": authority.census_receipt_sha256(receipt_by_field),
        "timed_out": False,
        "probe_verified": True,
        "quota_probe_verified": True,
        "pre_child_fence_verified": True,
        "post_child_fence_verified": True,
        "capacity": {
            "verified": True,
            "host_available_memory_bytes": 2,
            "minimum_host_available_memory_bytes": 1,
            "host_swap_free_bytes": 2,
            "minimum_host_swap_free_bytes": 1,
            "postgresql_tablespace_path": "/data/postgresql",
            "postgresql_tablespace_free_bytes": 2,
            "minimum_postgresql_tablespace_free_bytes": 1,
        },
        "runtime_attestation": _runtime_attestation(),
        "runtime_attestation_sha256": _runtime_attestation_sha256(),
        "cleanup": {
            "binding_removed": True,
            "policy_removed": True,
            "drain_restored": True,
            "quota_removed": True,
            "lock_released": True,
            "complete": True,
        },
        "postgresql_boundary": (
            "Kubernetes QoS does not reserve or cap off-node PostgreSQL"
        ),
    }


def _authority() -> dict:
    return {
        "expected_source_sha": _SOURCE_SHA,
        "expected_envelope_script_sha256": _ENVELOPE_SHA,
        "expected_child_command_sha256": _COMMAND_SHA,
        "expected_child_executable_sha256": _EXECUTABLE_SHA,
        "expected_source_manifest_sha256": _SOURCE_MANIFEST_SHA,
        "expected_harness_manifest_sha256": _HARNESS_MANIFEST_SHA,
        "expected_source_overlay_sha256": _OVERLAY_SHA,
        "expected_census_job": _RUNTIME["job_name"],
        "expected_census_configmap": _CONFIGMAP,
        "expected_target": deepcopy(_TARGET),
        "runtime_attestation": _runtime_attestation_bytes(),
        "capacity": {
            "minimum_host_available_memory_bytes": 1,
            "minimum_host_swap_free_bytes": 1,
            "postgresql_tablespace_path": "/data/postgresql",
            "minimum_postgresql_tablespace_free_bytes": 1,
        },
    }


def _database_receipt() -> dict:
    run_token = contract.census_database_run_token(_RUNTIME)
    resources_by_stage = {}
    for stage in contract.CENSUS_DATABASE_STAGE_KEYS:
        resource_by_field = {
            "before_count": 1,
            "before_backend_memory_context_bytes_maximum": 1,
            "before_temporary_relation_bytes_maximum": 0,
        }
        if stage != "measurement_complete":
            resource_by_field.update(
                after_count=1,
                after_backend_memory_context_bytes_maximum=1,
                after_temporary_relation_bytes_maximum=0,
            )
        resources_by_stage[stage] = resource_by_field
    return {
        "runtime": _RUNTIME,
        "database_run_token": run_token,
        "database_backend_pid": 123,
        "database_session_settings": (
            contract.expected_census_database_settings(run_token)
        ),
        "database_stage": "measurement_complete",
        "database_application_name": contract.census_database_application_name(
            run_token,
            "measurement_complete",
        ),
        "database_stage_resources": resources_by_stage,
    }


def _staged_counts() -> dict[str, int]:
    return dict.fromkeys(
        (
            "provider_set_count",
            "provider_membership_count",
            "maximum_provider_set_membership_count",
            "provider_cell_count",
            "provider_fragment_byte_count",
            "provider_npi_count",
            "pending_npi_count",
            "referenced_empty_provider_set_count",
            "price_membership_cached_block_count",
            "price_membership_identity_retained_bytes",
            "price_membership_metadata_fragment_count",
            "price_membership_maximum_fragments_per_block",
            "price_membership_singleton_peak_bytes",
        ),
        0,
    )


def _source_identity() -> dict:
    return {
        "declared_git_head": _SOURCE_SHA,
        "observed_git_head": None,
        "manifest_sha256": _SOURCE_MANIFEST_SHA,
        "files": deepcopy(_SOURCE_FILES),
        "harness_manifest_sha256": _HARNESS_MANIFEST_SHA,
        "harness_files": deepcopy(_HARNESS_FILES),
    }


def _accepted_inputs() -> tuple[dict, dict]:
    """Build the smallest complete authoritative census fixture."""

    work_by_field = census._empty_metrics()
    for field_name in ("membership_probe_rows", "member_cell_rows"):
        work_by_field[field_name] = {"total": 1, "maximum_per_code": 1}
    staged_by_field = _staged_counts()
    persistent_counts = dict.fromkeys(contract.PROJECTION_RELATIONS, 0)
    measurement_by_field = {
        "release": {
            **{
                field_name: _TARGET[field_name]
                for field_name in (
                    "healthporta_plan_id",
                    "plan_release_id",
                    "serving_revision_id",
                    "binding_set_digest",
                    "binding_count",
                )
            },
            "published_at": "2026-08-30T00:00:00Z",
        },
        "serving_shape": deepcopy(_TARGET),
        "provider_signature": _PROVIDER_SIGNATURE,
        "projection_id": identity.projection_id(
            _TARGET["binding_set_digest"], _PROVIDER_SIGNATURE
        ),
        "work": work_by_field,
        "staged": staged_by_field,
        "fixed_cap_gates": contract.fixed_cap_gates(work_by_field, staged_by_field),
        "observed_work_limits": contract.observed_work_limits(work_by_field),
        "persistent_counts_before": persistent_counts,
    }
    source_by_field = _source_identity()
    receipt_by_field = {
        **_database_receipt(),
        "contract": diagnostics.CENSUS_RECEIPT_CONTRACT,
        "status": "provisional",
        "accepted": False,
        "mode": "cardinality_census",
        "cap_calibration_admissible": False,
        "resource_proof_admissible": False,
        "acceptance_authority": diagnostics.CENSUS_ACCEPTANCE_AUTHORITY,
        "proof_scope": "row_count_limits_only",
        "expected_target": deepcopy(_TARGET),
        "source_before": source_by_field,
        "source_after": source_by_field,
        "rollback_complete": True,
        "temporary_relations_after_rollback": [],
        "postflight": {
            "release_matches": True,
            "provider_signature_matches": True,
            "persistent_counts_match": True,
            "persistent_counts_after": dict(persistent_counts),
            "accepted": True,
        },
        "measurement": measurement_by_field,
    }
    return receipt_by_field, measurement_by_field


def _is_accepted(
    receipt_by_field: dict,
    _measurement_by_field: dict,
    envelope_by_field: dict | None = None,
    authority_by_field: dict | None = None,
) -> bool:
    return authority.is_accepted(
        receipt_by_field,
        (
            _successful_envelope(receipt_by_field)
            if envelope_by_field is None
            else envelope_by_field
        ),
        _authority() if authority_by_field is None else authority_by_field,
    )


def test_acceptance_admits_the_unmutated_baseline() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()

    assert _is_accepted(receipt_by_field, measurement_by_field)


def test_successful_inner_candidate_remains_provisional() -> None:
    receipt_by_field = {}

    assert contract.seal_cardinality_census(receipt_by_field, True, "finished") == 0
    assert receipt_by_field["status"] == "provisional"
    assert receipt_by_field["accepted"] is False
    assert receipt_by_field["cap_calibration_admissible"] is False


def test_acceptance_binds_the_hashed_inner_measurement() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    receipt_by_field["measurement"] = {}

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def test_acceptance_validates_the_embedded_measurement_not_an_equal_detached_copy() -> (
    None
):
    receipt_by_field, measurement_by_field = _accepted_inputs()
    detached_measurement = deepcopy(measurement_by_field)
    receipt_by_field["measurement"]["work"]["membership_probe_rows"]["total"] = True

    assert not _is_accepted(receipt_by_field, detached_measurement)


def test_acceptance_derives_source_equality_from_the_hashed_receipt() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    receipt_by_field["source_after"] = {
        **receipt_by_field["source_after"],
        "unexpected": True,
    }

    assert not _is_accepted(receipt_by_field, measurement_by_field)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda postflight: postflight.pop("release_matches"),
        lambda postflight: postflight.update(release_matches=False),
        lambda postflight: postflight.update(persistent_counts_match=False),
        lambda postflight: postflight["persistent_counts_after"].update(
            plan_pricing_card=1
        ),
    ],
)
def test_acceptance_recomputes_the_complete_postflight_contract(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(receipt_by_field["postflight"])

    assert not _is_accepted(receipt_by_field, measurement_by_field)


@pytest.mark.parametrize(
    ("collection_name", "mutation"),
    (
        ("fixed_cap_gates", lambda values: values.pop(next(iter(values)))),
        ("fixed_cap_gates", lambda values: values.update(extra_gate=True)),
        ("fixed_cap_gates", lambda values: values.update({next(iter(values)): 1})),
        ("observed_work_limits", lambda values: values.pop(next(iter(values)))),
        ("observed_work_limits", lambda values: values.update(extra_limit=1)),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): True}),
        ),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): 1.0}),
        ),
        (
            "observed_work_limits",
            lambda values: values.update({next(iter(values)): -1}),
        ),
    ),
)
def test_acceptance_rejects_malformed_gate_or_limit_contract(
    collection_name,
    mutation,
) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(measurement_by_field[collection_name])

    assert not _is_accepted(receipt_by_field, measurement_by_field)


@pytest.mark.parametrize(
    ("mutation"),
    (
        lambda measurement: measurement["work"].pop(next(iter(measurement["work"]))),
        lambda measurement: measurement["work"].update(
            extra={"total": 0, "maximum_per_code": 0}
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=True
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=0, maximum_per_code=1
        ),
        lambda measurement: measurement["work"]["membership_probe_rows"].update(
            total=1.0
        ),
        lambda measurement: measurement["staged"].pop(
            next(iter(measurement["staged"]))
        ),
        lambda measurement: measurement["staged"].update(extra=0),
        lambda measurement: measurement["staged"].update(provider_set_count=True),
        lambda measurement: measurement["staged"].update(provider_set_count=-1),
        lambda measurement: measurement["staged"].update(
            price_membership_identity_retained_bytes=1
        ),
        lambda measurement: measurement["staged"].update(
            price_membership_singleton_peak_bytes=1
        ),
        lambda measurement: measurement["staged"].update(
            price_membership_metadata_fragment_count=0,
            price_membership_maximum_fragments_per_block=1,
            price_membership_singleton_peak_bytes=(
                contract.PRICE_MEMBERSHIP_TRANSIENT_BYTES_PER_FRAGMENT
            ),
        ),
    ),
)
def test_acceptance_rejects_malformed_measurement_contract(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(measurement_by_field)

    assert not _is_accepted(receipt_by_field, measurement_by_field)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda receipt, measurement: receipt.pop("expected_target"),
        lambda receipt, measurement: receipt["expected_target"].update(
            plan_release_id="other"
        ),
        lambda receipt, measurement: measurement.pop("release"),
        lambda receipt, measurement: measurement["release"].update(binding_count=2),
        lambda receipt, measurement: measurement.update(serving_shape={}),
        lambda receipt, measurement: measurement["serving_shape"].update(
            distinct_plan_count=True
        ),
        lambda receipt, measurement: measurement.update(provider_signature="bad"),
        lambda receipt, measurement: measurement.update(projection_id="bad"),
    ],
)
def test_acceptance_binds_exact_release_and_projection_identity(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(receipt_by_field, measurement_by_field)

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def test_acceptance_recomputes_derived_gates_and_limits() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    oversized_occurrence_count = contract.MAX_CODE_OCCURRENCES * 4 // 5 + 1
    measurement_by_field["work"]["normalized_occurrence_rows"] = {
        "total": oversized_occurrence_count,
        "maximum_per_code": oversized_occurrence_count,
    }
    assert not _is_accepted(receipt_by_field, measurement_by_field)

    receipt_by_field, measurement_by_field = _accepted_inputs()
    measurement_by_field["observed_work_limits"][
        "maximum_code_membership_probe_rows"
    ] = 2
    assert not _is_accepted(receipt_by_field, measurement_by_field)
