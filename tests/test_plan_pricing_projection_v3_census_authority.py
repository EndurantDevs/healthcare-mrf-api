# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""External-envelope authority checks for the projection-v3 census."""

import hashlib
import json

import pytest

from tests.test_plan_pricing_projection_v3_census_contract import (
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
