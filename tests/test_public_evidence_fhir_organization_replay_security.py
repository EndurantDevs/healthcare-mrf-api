# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tamper, capability, completeness, and privacy replay tests."""

from __future__ import annotations

import copy
from dataclasses import replace

import pytest

from public_evidence import source_record_replay_contract as replay_contract
from public_evidence.source_record_replay_primitives import (
    PublicEvidenceFhirOrganizationReplayError,
)
from process.public_evidence_fhir_organization_replay import (
    replay_fhir_organization_retained_rows,
    verify_fhir_organization_replay_result,
)
from tests.public_evidence_fhir_organization_replay_support import (
    default_retained_rows,
    replay_fixture,
    retained_organization_row,
)
from tests.tin_npi_connector_unit_support import (
    RecordingProjector,
    TEST_EIN,
    TOKEN_POLICY_ID,
    token_policy,
)

_ERROR = "public_evidence_fhir_organization_replay_invalid"


def _replay(fixture, **overrides):
    request_by_field = {
        "release": fixture.release,
        "inventory": fixture.inventory,
        "source_vector": fixture.source_vector,
        "retained_rows": fixture.retained_rows,
        "token_projectors": fixture.token_projectors,
        "record_identity_token_policy_id": fixture.record_policy_id,
    }
    request_by_field.update(overrides)
    return replay_fhir_organization_retained_rows(**request_by_field)


def _verify(fixture, candidate, **overrides):
    request_by_field = {
        "release": fixture.release,
        "inventory": fixture.inventory,
        "source_vector": fixture.source_vector,
        "retained_rows": fixture.retained_rows,
        "token_projectors": fixture.token_projectors,
        "record_identity_token_policy_id": fixture.record_policy_id,
    }
    request_by_field.update(overrides)
    return verify_fhir_organization_replay_result(
        candidate,
        **request_by_field,
    )


def _assert_replay_rejected(fixture, **overrides) -> None:
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _replay(fixture, **overrides)


@pytest.mark.parametrize(
    "retained_row_transform",
    (
        lambda rows: rows[:-1],
        lambda rows: (*rows, retained_organization_row("synthetic-c", matched=False)),
        lambda rows: (rows[0], rows[0]),
        lambda rows: tuple(reversed(rows)),
    ),
)
def test_replay_rejects_missing_extra_duplicate_or_unordered_rows(
    tmp_path,
    retained_row_transform,
) -> None:
    fixture = replay_fixture(tmp_path)

    _assert_replay_rejected(
        fixture,
        retained_rows=retained_row_transform(fixture.retained_rows),
    )


def test_replay_rejects_payload_and_resource_identity_mutation(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    payload_mutation = copy.deepcopy(fixture.retained_rows)
    payload_mutation[0]["payload_json"]["identifiers"][0]["value"] = "1000000004"
    identity_mutation = copy.deepcopy(fixture.retained_rows)
    identity_mutation[0]["payload_json"]["resource_id"] = "different-resource"

    _assert_replay_rejected(fixture, retained_rows=payload_mutation)
    _assert_replay_rejected(fixture, retained_rows=identity_mutation)


def test_replay_rejects_wrong_inventory_root_and_dataset_digest(tmp_path) -> None:
    wrong_root_fixture = replay_fixture(
        tmp_path / "root", inventory_root_override="0" * 64
    )
    _assert_replay_rejected(wrong_root_fixture)

    fixture = replay_fixture(tmp_path / "fence")
    wrong_dataset = replace(
        fixture.dataset,
        organization_resource_sha256="0" * 64,
    )
    wrong_vector = replace(
        fixture.source_vector,
        fhir_datasets=(wrong_dataset,),
    )
    _assert_replay_rejected(fixture, source_vector=wrong_vector)


def test_replay_rejects_zero_organization_fence(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    empty_dataset = replace(
        fixture.dataset,
        organization_resource_count=0,
        organization_resource_sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4"
        "649b934ca495991b7852b855",
    )
    empty_vector = replace(
        fixture.source_vector,
        fhir_datasets=(empty_dataset,),
    )

    _assert_replay_rejected(
        fixture,
        source_vector=empty_vector,
        retained_rows=(),
    )


def test_replay_requires_connector_owned_projector_capabilities(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    fake_projector = RecordingProjector(fixture.token_projectors[0])
    inert_copy = copy.copy(fixture.token_projectors[0])

    _assert_replay_rejected(fixture, token_projectors=(fake_projector,))
    _assert_replay_rejected(fixture, token_projectors=(inert_copy,))


def test_replay_rejects_wrong_policy_even_when_every_row_is_unmatched(tmp_path) -> None:
    unmatched_rows = tuple(
        retained_organization_row(f"synthetic-unmatched-{ordinal}", matched=False)
        for ordinal in range(2)
    )
    fixture = replay_fixture(tmp_path / "fixture", retained_rows=unmatched_rows)
    wrong_policy_id = TOKEN_POLICY_ID.removesuffix("a") + "z"
    wrong_policy_path = tmp_path / "wrong"
    wrong_policy_path.mkdir()
    wrong_projector = token_policy(wrong_policy_path, policy_id=wrong_policy_id)

    _assert_replay_rejected(fixture, token_projectors=(wrong_projector,))
    _assert_replay_rejected(
        fixture,
        record_identity_token_policy_id=wrong_policy_id,
    )


def test_replay_rejects_tampered_connector_owned_capability(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    projector = fixture.token_projectors[0]
    object.__setattr__(projector, "_token_policy_id", "invalid-policy")

    _assert_replay_rejected(fixture)


def test_full_verifier_rejects_a_structurally_valid_forged_result(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    forged_proof = replay_contract._VerifiedReplayProof(
        source_vector_sha256="1" * 64,
        dataset_fence_sha256="2" * 64,
        token_policy_id=fixture.record_policy_id,
        token_policy_descriptor_sha256=(
            fixture.source_vector.token_policies[0].token_policy_descriptor_sha256
        ),
        source_record_vector_sha256="3" * 64,
        scan_proof_sha256="4" * 64,
    )
    forged_result = replay_contract._build_fhir_organization_replay_result(
        release=fixture.release,
        inventory=fixture.inventory,
        proof=forged_proof,
        execution_seal=replay_contract._EXECUTION_SEAL,
    )

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, forged_result)


class _AlwaysEqual:
    def __eq__(self, other):
        return True


@pytest.mark.parametrize(
    "field_name",
    ("contract", "foundation_scope", "member_count"),
)
def test_full_verifier_does_not_trust_custom_field_equality(
    tmp_path,
    field_name: str,
) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    hostile_result = replay_result._replace(**{field_name: _AlwaysEqual()})

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, hostile_result)


def test_full_verifier_recursively_checks_authority_field_types(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    hostile_authority = replay_result.authority_state._replace(
        lifecycle_state=_AlwaysEqual()
    )

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, replay_result._replace(authority_state=hostile_authority))


class _StatefulRetainedRow(dict):
    def get(self, key, default=None):
        if key == "payload_json":
            return {"resource_id": "changing", "active": False, "identifiers": []}
        return super().get(key, default)


class _HostileRetainedRowKey:
    def __init__(self) -> None:
        self.hash_calls = 0
        self.equality_calls = 0

    def __hash__(self) -> int:
        self.hash_calls += 1
        return hash("payload_json")

    def __eq__(self, other: object) -> bool:
        self.equality_calls += 1
        return False


def test_replay_rejects_custom_or_stateful_row_mappings(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    stateful_row = _StatefulRetainedRow(fixture.retained_rows[0])

    _assert_replay_rejected(
        fixture,
        retained_rows=(stateful_row, fixture.retained_rows[1]),
    )


def test_replay_rejects_non_string_row_keys_without_custom_hooks(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    hostile_key = _HostileRetainedRowKey()
    hostile_row_by_field = {
        key: value
        for key, value in fixture.retained_rows[0].items()
        if key != "payload_json"
    }
    hostile_row_by_field[hostile_key] = fixture.retained_rows[0]["payload_json"]
    hostile_key.hash_calls = 0
    hostile_key.equality_calls = 0

    _assert_replay_rejected(
        fixture,
        retained_rows=(hostile_row_by_field, fixture.retained_rows[1]),
    )

    assert hostile_key.hash_calls == 0
    assert hostile_key.equality_calls == 0


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong-contract"),
        ("foundation_scope", "wrong-scope"),
        ("replay_policy_id", "wrong_policy_v1"),
        ("replay_policy_descriptor_sha256", "0" * 64),
        ("source_vector_sha256", "1" * 64),
        ("dataset_fence_sha256", "2" * 64),
        ("token_policy_id", "ptg-tin-hmac-sha256-v1:wrong"),
        ("token_policy_descriptor_sha256", "3" * 64),
        ("record_kind", "fhir_location"),
        ("record_identity_contract_id", "wrong_identity_v1"),
        ("record_identity_descriptor_sha256", "4" * 64),
        ("payload_canonicalization_contract_id", "wrong_payload_v1"),
        ("member_count", 3),
        ("member_root_sha256", "5" * 64),
        ("source_record_vector_sha256", "6" * 64),
        ("scan_contract_id", "wrong-scan"),
        ("scan_proof_sha256", "7" * 64),
        ("replay_ref", "perp1_forged"),
        ("contract_sha256", "8" * 64),
    ),
)
def test_result_verification_rejects_fixed_and_derived_tampering(
    tmp_path,
    field_name: str,
    replacement: object,
) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    hostile = replay_result._replace(**{field_name: replacement})

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, hostile)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("source_bytes_authenticated", True),
        ("source_authenticity_claimed", True),
        ("whole_source_complete", True),
        ("release_content_binding_verified", True),
        ("durable_relation_replay_verified", True),
        ("payload_derivation_verified", True),
        ("database_io_enabled", True),
        ("serving_authority", "enabled"),
        ("publication_enabled", True),
    ),
)
def test_result_verification_rejects_authority_escalation(
    tmp_path,
    field_name: str,
    replacement: object,
) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    authority = replay_result.authority_state._replace(**{field_name: replacement})

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, replay_result._replace(authority_state=authority))


def test_result_verification_rejects_nested_inventory_tampering(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    hostile_inventory = replay_result.inventory._replace(member_root_sha256="9" * 64)

    with pytest.raises(PublicEvidenceFhirOrganizationReplayError, match=f"^{_ERROR}$"):
        _verify(fixture, replay_result._replace(inventory=hostile_inventory))


class _ExplodingRows:
    def __iter__(self):
        raise RuntimeError(f"private-row {TEST_EIN} secret={bytes(range(32)).hex()}")


def test_result_and_failures_do_not_expose_rows_or_secret_material(tmp_path) -> None:
    fixture = replay_fixture(tmp_path)
    replay_result = _replay(fixture)
    forbidden_values = (
        TEST_EIN,
        "012345678",
        "synthetic-organization-a",
        bytes(range(32)).hex(),
        fixture.retained_rows[0]["payload_hash"],
    )

    public_text = f"{replay_result!r} {replay_result!s}"
    assert not any(value in public_text for value in forbidden_values)
    with pytest.raises(PublicEvidenceFhirOrganizationReplayError) as caught:
        _replay(fixture, retained_rows=_ExplodingRows())
    assert str(caught.value) == _ERROR
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    assert not any(value in str(caught.value) for value in forbidden_values)
    assert not any("row" in field for field in replay_result._fields)
    assert not any("projector" in field for field in replay_result._fields)
    assert not any("evidence" in field for field in replay_result._fields)
