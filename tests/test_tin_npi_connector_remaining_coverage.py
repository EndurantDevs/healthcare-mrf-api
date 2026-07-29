# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Remaining fail-closed contract coverage for the TIN-to-NPI connector."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import tin_npi_connector_extract as extraction_module
from process.tin_npi_connector import (
    FhirOrganizationEvidenceState,
    FhirTinNpiIdentifierPolicy,
    TinNpiConnectorError,
    build_compact_tin_npi_generation,
)
from process.tin_npi_connector_extract import (
    _ExtractionContext,
    _canonical_token_projectors,
    _classify_effective_identifier,
    _extract_verified_organization_evidence,
    _identifier_cutoff,
    _project_token_rows,
    _select_effective_identifiers,
)
from process.tin_npi_connector_generation import (
    _has_valid_evidence_scope,
    _lookup_key,
    _validate_generation_order,
    is_generation_reuse_compatible,
)
from process.tin_npi_connector_support import (
    _UnresolvedFhirIdentifierPeriod,
    strict_evidence_text,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    REVIEWED_TAX_AS_EIN_POLICY,
    REVIEWED_TAX_AS_EIN_RULE,
    TEST_EIN,
    extract_evidence,
    fhir_dataset,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    token_policy,
    typed_identifier,
)


def _extraction_context(tmp_path, **changes):
    context = _ExtractionContext(
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        source_record_identity_sha256=b"\x01" * 32,
        source_record_payload_hash="c" * 64,
        token_projectors=(token_policy(tmp_path),),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
    )
    return replace(context, **changes)


def _matched_generation(tmp_path):
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )
    payload_hash = extraction.evidence[0].source_record_payload_hash
    vector = source_vector(
        fhir_datasets=(
            fhir_dataset(
                organization_identities=(("organization-1", payload_hash),),
            ),
        ),
    )
    generation = build_compact_tin_npi_generation(
        (matched_scan(extraction),),
        source_vector=vector,
    )
    return generation, extraction.evidence[0]


def test_extraction_rejects_projector_without_policy_identity():
    with pytest.raises(TinNpiConnectorError, match="projectors are invalid"):
        _canonical_token_projectors((object(),))


def test_extraction_rejects_unrepresentable_canonical_cutoff(monkeypatch):
    monkeypatch.setattr(extraction_module, "_as_utc_datetime", lambda _value: None)

    with pytest.raises(TinNpiConnectorError, match="cutoff is invalid"):
        _identifier_cutoff(EVIDENCE_AS_OF)


def test_extraction_maps_unresolved_period_to_terminal_state(monkeypatch):
    def unresolved_identifier(*_args, **_kwargs):
        raise _UnresolvedFhirIdentifierPeriod("unresolved")

    monkeypatch.setattr(
        extraction_module,
        "_is_identifier_effective",
        unresolved_identifier,
    )

    identifier_class, terminal_state = _classify_effective_identifier(
        npi_identifier("1234567893"),
        identifier_rule=REVIEWED_TAX_AS_EIN_RULE,
        evidence_cutoff=None,
    )

    assert identifier_class is None
    assert terminal_state is FhirOrganizationEvidenceState.UNRESOLVED_IDENTIFIER_PERIOD


def test_extraction_ignores_non_mapping_identifier_entries():
    selected = _select_effective_identifiers(
        (object(),),
        identifier_rule=REVIEWED_TAX_AS_EIN_RULE,
        evidence_cutoff=None,
    )

    assert selected.state is FhirOrganizationEvidenceState.MISSING_IDENTIFIERS


def test_extraction_rejects_invalid_source_record_projection(tmp_path):
    delegate = token_policy(tmp_path)

    class ShortIdentityProjector:
        token_policy_id = delegate.token_policy_id

        def tokenize_ein(self, normalized_ein):
            return delegate.tokenize_ein(normalized_ein)

        @staticmethod
        def pseudonymize_source_record(**_identity):
            return b"short"

    context = _extraction_context(
        tmp_path,
        token_projectors=(ShortIdentityProjector(),),
    )

    with pytest.raises(TinNpiConnectorError, match="source-record identity"):
        _project_token_rows(
            context,
            normalized_ein="012345678",
            resource_id="organization-1",
        )


def test_verified_extraction_rejects_non_organization(tmp_path):
    result = _extract_verified_organization_evidence(
        {"resourceType": "Patient"},
        _extraction_context(tmp_path),
    )

    assert result.state is FhirOrganizationEvidenceState.NOT_ORGANIZATION


def test_verified_extraction_rejects_invalid_record_identity(tmp_path):
    context = _extraction_context(
        tmp_path,
        source_record_identity_sha256=b"short",
    )

    with pytest.raises(TinNpiConnectorError, match="record identity is invalid"):
        _extract_verified_organization_evidence(organization(), context)


def test_verified_extraction_rejects_invalid_identifier_policy(tmp_path):
    context = _extraction_context(tmp_path, identifier_policy=object())

    with pytest.raises(TinNpiConnectorError, match="identifier policy is invalid"):
        _extract_verified_organization_evidence(organization(), context)


def test_verified_extraction_treats_scalar_identifiers_as_missing(tmp_path):
    result = _extract_verified_organization_evidence(
        {
            "resourceType": "Organization",
            "id": "organization-1",
            "identifier": "not-an-identifier-array",
        },
        _extraction_context(tmp_path),
    )

    assert result.state is FhirOrganizationEvidenceState.MISSING_IDENTIFIERS


@pytest.mark.parametrize(
    "changes",
    (
        {"rule_id": ""},
        {"npi_systems": ("contains whitespace",)},
        {"npi_type_codings": (("valid", "contains whitespace"),)},
        {"npi_systems": (), "npi_type_codings": ()},
        {"excluded_identifier_uses": ("contains whitespace",)},
    ),
)
def test_identifier_rule_rejects_noncanonical_contract_fields(changes):
    with pytest.raises(TinNpiConnectorError, match="FHIR identifier"):
        replace(REVIEWED_TAX_AS_EIN_RULE, **changes)


def test_identifier_policy_requires_at_least_one_reviewed_rule():
    with pytest.raises(TinNpiConnectorError, match="policy rules are invalid"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(),
        )


def test_evidence_text_rejects_surrounding_whitespace():
    with pytest.raises(TinNpiConnectorError, match="evidence source is invalid"):
        strict_evidence_text(" source-a", "source", limit=64)


def test_generation_exposes_canonical_scan_proof_json(tmp_path):
    generation, _evidence = _matched_generation(tmp_path)

    assert generation.scan_proof_canonical_json.startswith("{")
    assert '"source_id":"source-a"' in generation.scan_proof_canonical_json


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("source_ordinal_map_digest", b"short"),
        ("lookup_digest", b"short"),
    ),
)
def test_generation_rejects_invalid_digest_shapes(
    tmp_path,
    field_name,
    invalid_value,
):
    generation, _evidence = _matched_generation(tmp_path)

    with pytest.raises(TinNpiConnectorError, match="generation is invalid"):
        replace(generation, **{field_name: invalid_value})


def _tampered_forward_generation(generation, field_name, value):
    tampered_forward = copy.copy(generation.forward_rows[0])
    object.__setattr__(tampered_forward, field_name, value)
    tampered_generation = copy.copy(generation)
    object.__setattr__(tampered_generation, "forward_rows", (tampered_forward,))
    return tampered_generation


def test_generation_rechecks_source_count_width(tmp_path):
    generation, _evidence = _matched_generation(tmp_path)
    tampered = _tampered_forward_generation(
        generation,
        "source_evidence_counts",
        (),
    )

    with pytest.raises(TinNpiConnectorError, match="source evidence counts"):
        tampered._observed_source_policy_evidence_counts()


def test_generation_rechecks_source_count_bitmap_parity(tmp_path):
    generation, _evidence = _matched_generation(tmp_path)
    tampered = _tampered_forward_generation(
        generation,
        "source_evidence_counts",
        (0,),
    )

    with pytest.raises(TinNpiConnectorError, match="source evidence counts"):
        tampered._observed_source_policy_evidence_counts()


def test_generation_order_rejects_duplicate_evidence(tmp_path):
    generation, evidence = _matched_generation(tmp_path)
    candidate = SimpleNamespace(
        evidence_rows=(evidence, evidence),
        forward_rows=generation.forward_rows,
        reverse_rows=generation.reverse_rows,
    )

    with pytest.raises(TinNpiConnectorError, match="evidence rows are invalid"):
        _validate_generation_order(candidate)


def test_generation_order_rejects_duplicate_forward_rows(tmp_path):
    generation, _evidence = _matched_generation(tmp_path)
    candidate = SimpleNamespace(
        evidence_rows=(),
        forward_rows=(generation.forward_rows[0], generation.forward_rows[0]),
        reverse_rows=generation.reverse_rows,
    )

    with pytest.raises(TinNpiConnectorError, match="forward rows are invalid"):
        _validate_generation_order(candidate)


def test_generation_scope_rejects_evidence_without_scan_proof(tmp_path):
    _generation, evidence = _matched_generation(tmp_path)
    candidate = SimpleNamespace(scan_proofs=(), evidence_rows=(evidence,))

    assert _has_valid_evidence_scope(candidate) is False


def test_generation_reuse_rejects_invalid_input():
    with pytest.raises(TinNpiConnectorError, match="reuse input is invalid"):
        is_generation_reuse_compatible(object(), object())


def test_generation_reuse_rejects_different_source_vector():
    first = build_compact_tin_npi_generation(
        (),
        source_vector=source_vector(
            fhir_datasets=(fhir_dataset(organization_identities=()),),
        ),
    )
    second = build_compact_tin_npi_generation(
        (),
        source_vector=source_vector(
            fhir_datasets=(
                fhir_dataset(
                    dataset_hash="b" * 64,
                    organization_identities=(),
                ),
            ),
        ),
    )

    assert is_generation_reuse_compatible(first, second) is False


def test_generation_reuse_rejects_nondeterministic_content(tmp_path):
    generation, _evidence = _matched_generation(tmp_path)
    candidate = copy.copy(generation)
    object.__setattr__(candidate, "generation_id", "0" * 64)

    with pytest.raises(TinNpiConnectorError, match="produced different content"):
        is_generation_reuse_compatible(generation, candidate)


def test_lookup_key_contains_full_authoritative_identity(tmp_path):
    _generation, evidence = _matched_generation(tmp_path)

    assert _lookup_key(evidence) == (
        evidence.token.token_policy_id,
        evidence.token.tin_id_128,
        evidence.token.tin_hmac_sha256,
        evidence.relationship_class,
    )
