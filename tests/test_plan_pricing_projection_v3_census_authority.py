# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""External-envelope authority checks for the projection-v3 census."""

import hashlib
import json
from copy import deepcopy

import pytest

from scripts.research import plan_pricing_projection_v3_census_support as support
from tests.test_plan_pricing_projection_v3_census_contract import (
    _TARGET,
    _accepted_inputs,
    _authority,
    _is_accepted,
    _runtime_attestation,
    _successful_envelope,
)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda envelope: envelope.update(status="failed"),
        lambda envelope: envelope.update(exit_code=True),
        lambda envelope: envelope.update(child_exit_code=143),
        lambda envelope: envelope.update(census_job="stale-job"),
        lambda envelope: envelope.update(census_receipt_sha256="d" * 64),
        lambda envelope: envelope.update(timed_out=True),
        lambda envelope: envelope.update(post_child_fence_verified=False),
        lambda envelope: envelope["cleanup"].update(complete=False),
        lambda envelope: envelope.update(reviewed_source_sha="d" * 40),
    ],
)
def test_acceptance_requires_the_successful_outer_envelope(mutation) -> None:
    """Inner evidence is provisional until the outer child exit is accepted."""

    receipt_by_field, measurement_by_field = _accepted_inputs()
    envelope_by_field = _successful_envelope(receipt_by_field)
    mutation(envelope_by_field)

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        envelope_by_field,
    )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("expected_envelope_script_sha256", "d" * 64),
        ("expected_child_command_sha256", "d" * 64),
        ("expected_child_executable_sha256", "e" * 64),
        ("expected_source_sha", "d" * 40),
        ("expected_source_manifest_sha256", "4" * 64),
        ("expected_harness_manifest_sha256", "5" * 64),
        ("expected_source_overlay_sha256", "6" * 64),
        ("expected_census_job", "other-job"),
        ("expected_census_configmap", "other-configmap"),
    ],
)
def test_acceptance_binds_reviewed_script_and_command_hashes(
    field_name,
    field_value,
) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    authority_by_field = _authority()
    authority_by_field[field_name] = field_value

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        authority_by_field=authority_by_field,
    )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("minimum_host_available_memory_bytes", True),
        ("minimum_host_available_memory_bytes", 1.0),
        ("minimum_host_available_memory_bytes", 2),
        ("minimum_host_swap_free_bytes", True),
        ("minimum_host_swap_free_bytes", 0),
        ("postgresql_tablespace_path", "relative"),
        ("minimum_postgresql_tablespace_free_bytes", 2),
    ],
)
def test_acceptance_binds_exact_capacity_authority(field_name, field_value) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    authority_by_field = _authority()
    authority_by_field["capacity"][field_name] = field_value

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        authority_by_field=authority_by_field,
    )


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("pod_uid", "other-pod-uid"),
        ("pod_owner_job_uid", "other-job-uid"),
        ("container_name", "other"),
        ("image_id", "sha256:" + "c" * 64),
        ("image_id", "containerd://sha256:" + "d" * 64),
        ("source_sha", "d" * 40),
        ("source_manifest_sha256", "4" * 64),
        ("harness_manifest_sha256", "5" * 64),
        ("source_overlay_sha256", "6" * 64),
        ("configmap_name", "other-configmap"),
        ("configmap_uid", ""),
        ("job_source_configmap_name", "other-configmap"),
        ("pod_source_configmap_name", "other-configmap"),
    ],
)
def test_acceptance_binds_exact_external_runtime_attestation(
    field_name,
    field_value,
) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    envelope_by_field = _successful_envelope(receipt_by_field)
    authority_by_field = _authority()
    attestation = _runtime_attestation()
    attestation[field_name] = field_value
    attestation_bytes = (
        json.dumps(attestation, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    authority_by_field["runtime_attestation"] = attestation_bytes
    envelope_by_field["runtime_attestation"] = attestation
    envelope_by_field["runtime_attestation_sha256"] = hashlib.sha256(
        attestation_bytes
    ).hexdigest()

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        envelope_by_field,
        authority_by_field,
    )


def test_acceptance_rejects_missing_external_authority() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        authority_by_field={},
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda receipt: receipt.pop("contract"),
        lambda receipt: receipt.update(contract="unknown"),
    ],
)
def test_acceptance_requires_the_exact_inner_contract(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    mutation(receipt_by_field)

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def test_acceptance_rejects_a_stale_same_source_inner_receipt() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    envelope_by_field = _successful_envelope(receipt_by_field)
    receipt_by_field["finished_at"] = "later"

    assert not _is_accepted(
        receipt_by_field,
        measurement_by_field,
        envelope_by_field,
    )


@pytest.mark.parametrize(
    "field_name",
    ["declared_git_head", "manifest_sha256", "harness_manifest_sha256"],
)
def test_acceptance_binds_external_source_identity(field_name: str) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    receipt_by_field["source_after"] = {
        **receipt_by_field["source_after"],
        field_name: "0" * (40 if field_name == "declared_git_head" else 64),
    }

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def _replace_first_source_path(source_by_field: dict) -> None:
    source_by_field["files"][0][0] = "api/unreviewed.py"
    source_by_field["manifest_sha256"] = support._canonical_sha256(
        source_by_field["files"]
    )


def _reorder_harness(source_by_field: dict) -> None:
    source_by_field["harness_files"].reverse()
    source_by_field["harness_manifest_sha256"] = support._canonical_sha256(
        source_by_field["harness_files"]
    )


@pytest.mark.parametrize(
    "mutation",
    [
        _replace_first_source_path,
        _reorder_harness,
        lambda source: source.update(observed_git_head="0" * 40),
        lambda source: source.update(extra_harness_key=True),
        lambda source: source["files"][0].__setitem__(1, "bad"),
        lambda source: source["files"][0].__setitem__(1, "0" * 64),
    ],
)
def test_acceptance_recomputes_the_exact_source_identity(mutation) -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    source_by_field = deepcopy(receipt_by_field["source_before"])
    mutation(source_by_field)
    receipt_by_field["source_before"] = source_by_field
    receipt_by_field["source_after"] = deepcopy(source_by_field)

    assert not _is_accepted(receipt_by_field, measurement_by_field)


def test_acceptance_binds_the_target_to_external_authority() -> None:
    receipt_by_field, measurement_by_field = _accepted_inputs()
    target_by_field = {**_TARGET, "plan_release_id": "other-release"}
    receipt_by_field["expected_target"] = target_by_field
    measurement_by_field["serving_shape"] = deepcopy(target_by_field)
    measurement_by_field["release"]["plan_release_id"] = "other-release"

    assert not _is_accepted(receipt_by_field, measurement_by_field)
