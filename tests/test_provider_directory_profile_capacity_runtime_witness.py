# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed-schema tests for signed Provider Profile runtime witnesses."""

from __future__ import annotations

import dataclasses

import pytest

from process import provider_directory_profile_capacity_attestation as lease
from process.provider_directory_profile_capacity_runtime_witness import (
    CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
    CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
)
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _verify,
)


def test_coordinator_fields_preserve_exact_typed_and_wire_schema():
    envelope = _signed_envelope()
    runtime_witness_by_field = envelope["lease"]["runtime_witness"]
    verified_witness = _verify(envelope).runtime_witness
    expected_fields = {
        CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
        CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
    }

    assert expected_fields <= set(runtime_witness_by_field)
    assert tuple(field.name for field in dataclasses.fields(verified_witness)) == tuple(
        runtime_witness_by_field
    )
    assert dataclasses.asdict(verified_witness) == runtime_witness_by_field


def test_new_v1_lease_is_rejected_at_application_boundary():
    def v1_contract(body):
        body["contract_id"] = "provider-directory-database-capacity-lease-v1"

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="unsupported_contract: contract_id",
    ):
        _verify(_signed_envelope(body_mutator=v1_contract))


def test_legacy_v1_shape_and_opaque_runtime_digest_replay_fail_closed():
    def legacy_body(body):
        body["contract_id"] = "provider-directory-database-capacity-lease-v1"
        body.pop("runtime_witness")
        body.pop("runtime_witness_sha256")
        body.pop("deployment_witness")

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_fields",
    ):
        _verify(_signed_envelope(body_mutator=legacy_body))

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="identity_mismatch: runtime_witness_sha256",
    ):
        _verify(
            _signed_envelope(
                body_mutator=lambda body: body.update(
                    {"runtime_witness_sha256": "99" * 32}
                )
            )
        )


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("runtime_witness", "healthcare_source_commit", "0" * 40),
        ("runtime_witness", "healthcare_image_digest", "latest"),
        ("runtime_witness", "profile_migration_revision", "bad/revision"),
        ("runtime_witness", "profile_schema_version", True),
        ("runtime_witness", "profile_strategy_version", "legacy"),
        ("deployment_witness", "flux_revision", "bad revision"),
        ("deployment_witness", "bootstrap_config_sha256", "short"),
        ("deployment_witness", "preflight_pod_name", "bad/pod"),
        ("deployment_witness", "preflight_transport", "public_http"),
    ],
)
def test_runtime_and_deployment_witness_scalars_are_closed(
    section,
    field,
    value,
):
    def mutate(body):
        body[section][field] = value

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="invalid_value",
    ):
        _verify(_signed_envelope(body_mutator=mutate))
