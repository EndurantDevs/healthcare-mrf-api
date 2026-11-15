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
    runtime_witness_by_field = envelope["lease"]["runtime"]
    verified_witness = _verify(envelope).runtime
    expected_fields = {
        CAPACITY_RUNTIME_CONTROL_PLANE_SOURCE_COMMIT_FIELD,
        CAPACITY_RUNTIME_CONTROL_PLANE_IMAGE_DIGEST_FIELD,
    }

    assert expected_fields <= set(runtime_witness_by_field)
    assert tuple(field.name for field in dataclasses.fields(verified_witness)) == tuple(runtime_witness_by_field)
    assert dataclasses.asdict(verified_witness) == runtime_witness_by_field


def test_new_v1_lease_is_rejected_at_application_boundary():
    def v1_contract(body):
        body["contract_id"] = "provider-directory-database-capacity-lease-v1"

    with pytest.raises(
        lease.ProviderDirectoryCapacityLeaseError,
        match="unsupported_contract: contract_id",
    ):
        _verify(_signed_envelope(body_mutator=v1_contract))


def test_new_lease_refuses_an_opaque_runtime_digest_instead_of_runtime():
    envelope = _signed_envelope()
    envelope["lease"].pop("runtime")
    envelope["lease"]["runtime_witness_sha256"] = "99" * 32
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_fields"):
        _verify(envelope)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("healthcare_source_commit", "0" * 40),
        ("healthcare_image_digest", "latest"),
        ("control_plane_source_commit", "0" * 40),
        ("control_plane_image_digest", "latest"),
        ("profile_migration_revision", "bad/revision"),
        ("profile_schema_version", True),
        ("profile_strategy_version", "legacy"),
        ("postgres_server_version_num", True),
    ],
)
def test_runtime_code_image_and_schema_scalars_are_closed(field, value):
    with pytest.raises(lease.ProviderDirectoryCapacityLeaseError, match="invalid_value"):
        _verify(_signed_envelope(body_mutator=lambda body: body["runtime"].update({field: value})))
