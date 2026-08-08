# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict validation and redaction for prospective persistence candidates."""

from __future__ import annotations

from typing import NamedTuple

import pytest

from public_evidence import evidence_record_contract as record_contract
from public_evidence import record_persistence_candidate_contract as candidate_contract
from public_evidence import record_persistence_candidate_primitives as candidate_primitive
from tests.public_evidence_record_support import (
    name_input,
    network_input,
    relationship_input,
    source_release,
)


class StringSubclass(str):
    pass


class EqualityCompatible:
    def __eq__(self, _other: object) -> bool:
        return True


class ForeignTypedRow(NamedTuple):
    record_type: str
    row_sha256: str


class SourceLinkSubclass(candidate_primitive.PublicEvidenceRecordSourceLinkRow):
    pass


class ForeignReference:
    missing = 1


def _relationship_candidate() -> (
    candidate_primitive.PublicEvidenceRecordPersistenceCandidate
):
    source = source_release("tic")
    normalized = record_contract.build_public_evidence_record(
        source,
        relationship_input(
            source,
            "tic_provider_group_member",
            membership_state="members_present",
        ),
    )
    return candidate_contract.build_public_evidence_record_persistence_candidate(
        normalized
    )


def _network_candidate() -> candidate_primitive.PublicEvidenceRecordPersistenceCandidate:
    source = source_release("public_provider_directory_fhir")
    normalized = record_contract.build_public_evidence_record(
        source,
        network_input(source),
    )
    return candidate_contract.build_public_evidence_record_persistence_candidate(
        normalized
    )


def _assert_public_context_free(error: BaseException) -> None:
    assert type(error) is (
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    )
    assert str(error) == "public_evidence_record_persistence_candidate_invalid"
    assert error.__cause__ is None
    assert error.__context__ is None


def _assert_rejected(candidate: object) -> None:
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ) as exc_info:
        candidate_contract.validate_public_evidence_record_persistence_candidate(
            candidate
        )
    _assert_public_context_free(exc_info.value)


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("contract", "wrong-contract"),
        ("foundation_scope", "wrong-scope"),
        ("candidate_ref", "pepc1_forged"),
        ("contract_sha256", "0" * 64),
        ("contract_sha256", StringSubclass("0" * 64)),
    ),
)
def test_top_level_fixed_reference_and_digest_tampering_fails_closed(
    field_name: str,
    replacement: object,
) -> None:
    candidate = _relationship_candidate()
    _assert_rejected(candidate._replace(**{field_name: replacement}))


@pytest.mark.parametrize(
    ("field_name", "replacement"),
    (
        ("source_kind", "future_source"),
        ("source_kind", EqualityCompatible()),
        ("source_record_count", True),
        ("typed_row_sha256", "0" * 64),
        ("source_link_vector_sha256", "0" * 64),
        ("authority_state_sha256", "0" * 64),
        ("database_io_authority", "writer"),
        ("publication_enabled", True),
        ("row_sha256", "0" * 64),
    ),
)
def test_common_row_tampering_and_equality_spoofing_fails_closed(
    field_name: str,
    replacement: object,
) -> None:
    candidate = _relationship_candidate()
    hostile_common = candidate.common_row._replace(**{field_name: replacement})
    _assert_rejected(candidate._replace(common_row=hostile_common))


def test_link_vector_requires_exact_classes_order_ordinals_and_membership() -> None:
    candidate = _network_candidate()
    links = candidate.source_link_rows
    hostile_vectors = (
        tuple(reversed(links)),
        links[:-1],
        links + (links[-1],),
        (links[0]._replace(source_record_ordinal=1), *links[1:]),
        (links[0]._replace(row_sha256="0" * 64), *links[1:]),
        (SourceLinkSubclass(*links[0]), *links[1:]),
    )
    for hostile_links in hostile_vectors:
        _assert_rejected(candidate._replace(source_link_rows=hostile_links))


def test_wrong_or_tampered_typed_row_fails_closed() -> None:
    relationship = _relationship_candidate()
    network = _network_candidate()
    hostile_rows = (
        network.typed_row,
        ForeignTypedRow("tax_identity_relationship", relationship.typed_row.row_sha256),
        relationship.typed_row._replace(related_npi="1000000004"),
        relationship.typed_row._replace(row_sha256="0" * 64),
        relationship.typed_row._replace(candidate_only=True),
    )
    for hostile_row in hostile_rows:
        _assert_rejected(relationship._replace(typed_row=hostile_row))


def test_authority_escalation_and_wrong_candidate_type_fail_closed() -> None:
    candidate = _relationship_candidate()
    for field_name, replacement in (
        ("storage_schema_state", "defined"),
        ("database_write_state", "executed"),
        ("database_row_presence_verified", True),
        ("database_constraint_parity_verified", True),
        ("source_bytes_authenticated", True),
        ("legal_ownership_claimed", True),
        ("writer_authority", "enabled"),
        ("migration_authority", "enabled"),
        ("serving_authority", "enabled"),
        ("publication_enabled", True),
    ):
        authority = candidate.authority_state._replace(
            **{field_name: replacement}
        )
        _assert_rejected(candidate._replace(authority_state=authority))
    _assert_rejected(object())


def test_nested_record_tampering_is_revalidated_before_projection() -> None:
    candidate = _relationship_candidate()
    hostile_record = candidate.record._replace(contract_sha256="0" * 64)
    _assert_rejected(candidate._replace(record=hostile_record))


def test_builder_and_validator_clear_unexpected_and_nested_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _relationship_candidate()

    def explode(*_args: object) -> None:
        raise ZeroDivisionError("private-candidate-value")

    monkeypatch.setattr(candidate_contract, "_typed_row", explode)
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ) as build_exc:
        candidate_contract.build_public_evidence_record_persistence_candidate(
            candidate.record
        )
    _assert_public_context_free(build_exc.value)

    monkeypatch.setattr(candidate_contract, "_build_candidate", explode)
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ) as validate_exc:
        candidate_contract.validate_public_evidence_record_persistence_candidate(
            candidate
        )
    _assert_public_context_free(validate_exc.value)


def test_builder_clears_nested_normalized_record_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate = _relationship_candidate()

    def nested_failure(*_args: object) -> None:
        try:
            raise RuntimeError("private-record-value")
        except RuntimeError:
            raise record_contract.PublicEvidenceRecordError(
                "public_evidence_record_invalid"
            ) from None

    monkeypatch.setattr(
        candidate_contract,
        "validate_public_evidence_record",
        nested_failure,
    )
    try:
        candidate_contract.build_public_evidence_record_persistence_candidate(
            candidate.record
        )
    except candidate_primitive.PublicEvidenceRecordPersistenceCandidateError as error:
        caught_error = error
    else:
        raise AssertionError("expected normalized candidate failure")
    _assert_public_context_free(caught_error)


def test_all_representations_redact_public_identifiers_and_digest_material() -> None:
    relationship = _relationship_candidate()
    source_release_descriptor = source_release("tic")
    normalized_name = record_contract.build_public_evidence_record(
        source_release_descriptor,
        name_input(
            source_release_descriptor,
            "tic_source_reported_business_name",
            source_reported_name="Synthetic Public Display Name",
        ),
    )
    name = candidate_contract.build_public_evidence_record_persistence_candidate(
        normalized_name
    )
    rendered = " ".join(
        repr(descriptor)
        for descriptor in (
            relationship,
            relationship.common_row,
            relationship.source_link_rows[0],
            relationship.typed_row,
            relationship.authority_state,
            name,
            name.typed_row,
        )
    )
    assert str(relationship) == repr(relationship)
    assert str(relationship.common_row) == repr(relationship.common_row)
    assert str(relationship.source_link_rows[0]) == repr(
        relationship.source_link_rows[0]
    )
    assert str(relationship.typed_row) == repr(relationship.typed_row)
    assert str(relationship.authority_state) == repr(relationship.authority_state)
    assert "Synthetic Public Display Name" not in rendered
    assert relationship.record.evidence.related_npi not in rendered
    assert relationship.record.evidence.tax_identity.full_hmac_sha256 not in rendered
    assert relationship.record.source_records[0].record_hmac_sha256 not in rendered
    assert relationship.candidate_ref not in rendered


def test_private_shape_helpers_fail_closed_on_foreign_rows_and_references() -> None:
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ):
        candidate_contract._row_payload(object())
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ):
        candidate_contract._optional_reference(object(), "missing")
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ):
        candidate_contract._optional_reference(ForeignReference(), "missing")
    assert candidate_contract._is_exact_value_match("plain", "plain") is True
    assert candidate_contract._is_exact_value_match("plain", "other") is False
    assert candidate_contract._is_exact_value_match((1,), (1, 2)) is False


def test_internal_closed_variant_dispatch_and_link_bounds_fail_closed() -> None:
    relationship = _relationship_candidate().record
    network = _network_candidate().record
    mismatched_calls = (
        (candidate_contract._relationship_row, network),
        (candidate_contract._name_row, relationship),
        (candidate_contract._enumeration_row, relationship),
        (candidate_contract._entity_address_row, relationship),
        (candidate_contract._network_row, relationship),
    )
    for builder, hostile_record in mismatched_calls:
        with pytest.raises(
            candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
        ):
            builder(hostile_record)

    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ):
        candidate_contract._typed_row(relationship._replace(record_type="future"))
    for hostile_records in (
        tuple(reversed(network.source_records)),
        network.source_records + (network.source_records[-1],),
        (),
    ):
        with pytest.raises(
            candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
        ):
            candidate_contract._source_link_rows(
                network._replace(source_records=hostile_records)
            )
    with pytest.raises(
        candidate_primitive.PublicEvidenceRecordPersistenceCandidateError
    ):
        candidate_contract._common_row(
            relationship._replace(evidence=object()),
            "0" * 64,
            "0" * 64,
        )
