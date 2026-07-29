# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed boundary coverage for compact TIN-to-NPI contracts."""

from __future__ import annotations

import copy
import datetime as dt
import pickle
from dataclasses import replace

import pytest

from process import tin_npi_connector_adapters as adapters
from process import tin_npi_connector_build as build
from process import tin_npi_connector_evidence as evidence
from process import tin_npi_connector_lookup as lookup
from process import tin_npi_connector_security as security
from process.tin_npi_connector import (
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FhirOrganizationEvidenceResult,
    FhirOrganizationEvidenceState,
    NpiTinLookupReference,
    NpiTinLookupRow,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorError,
    TinNpiLookupRow,
    TinTaxIdentityToken,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    REVIEWED_TAX_AS_EIN_POLICY,
    TEST_EIN,
    TOKEN_POLICY_ID,
    extract_evidence,
    fhir_dataset,
    npi_identifier,
    organization,
    source_vector,
    token_policy,
    typed_identifier,
)


def _evidence_and_vector(tmp_path):
    resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    evidence_row = extract_evidence(resource, tmp_path).evidence[0]
    dataset = fhir_dataset(
        organization_identities=(
            ("organization-1", evidence_row.source_record_payload_hash),
        )
    )
    return evidence_row, source_vector(fhir_datasets=(dataset,))


def _lookup_fields(tmp_path):
    return {
        "token": token_policy(tmp_path).tokenize_ein(TEST_EIN),
        "relationship_class": FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        "npis": (1234567893,),
        "evidence_count": 1,
        "source_ids": ("source-a",),
        "source_bitmap": b"\x01",
        "npi_source_bitmap_matrix": b"\x01",
        "source_evidence_counts": (1,),
    }


def _reject(message, callback, *args, **kwargs):
    with pytest.raises(TinNpiConnectorError, match=message):
        callback(*args, **kwargs)


def test_security_redacts_and_rejects_corrupt_capability_state(tmp_path):
    protected = security._ProtectedSecret(bytes(range(32)))
    assert str(protected) == repr(protected) == "<redacted>"
    with pytest.raises(TypeError, match="immutable"):
        protected.value = b"replacement"
    snapshots = (
        copy.copy(protected),
        copy.deepcopy(protected),
        pickle.loads(pickle.dumps(protected)),
    )
    assert {repr(snapshot) for snapshot in snapshots} == {"<redacted-tin-token-policy>"}
    _reject("secret is invalid", security._read_protected_secret, object())
    _reject(
        "secret is invalid",
        security._read_protected_secret,
        security._ProtectedSecret(b"short"),
    )
    _reject("policy ID is invalid", security.canonical_token_policy_id, None)
    _reject(
        "secret is invalid",
        security._TinHmacTokenPolicy,
        token_policy_id=TOKEN_POLICY_ID,
        secret=bytearray(range(32)),
    )

    policy = token_policy(tmp_path)
    assert "secret" not in repr(policy)
    with pytest.raises(TypeError, match="immutable"):
        policy._token_policy_id = TOKEN_POLICY_ID
    policy_snapshots = (
        copy.copy(policy),
        copy.deepcopy(policy),
        pickle.loads(pickle.dumps(policy)),
    )
    assert all(
        repr(snapshot) == "<redacted-tin-token-policy>" for snapshot in policy_snapshots
    )
    object.__setattr__(policy, "_binding", security._ProtectedSecret(b"\0" * 32))
    _reject("policy state is invalid", policy.tokenize_ein, TEST_EIN)

    policy = token_policy(tmp_path)
    object.__setattr__(policy, "_secret", object())
    _reject("policy state is invalid", lambda: policy.token_policy_id)


def test_security_framing_and_token_shape_guards(tmp_path):
    invalid_tin_fields = (
        ("eín", "012345678"),
        ("ein", "１２３４５６７８９"),
        ("x" * 0x10000, "012345678"),
        ("ein", "1" * 0x10000),
    )
    for tin_type, normalized_tin in invalid_tin_fields:
        _reject(
            "token input is invalid",
            security._tin_hmac_message,
            tin_type=tin_type,
            normalized_tin=normalized_tin,
        )

    identity_fields_by_name = {
        "token_policy_id": TOKEN_POLICY_ID,
        "source_id": "source-a",
        "source_endpoint_id": "endpoint-a",
        "source_dataset_id": "dataset-a",
        "resource_id": "organization-1",
    }
    for field, invalid_value in (
        ("source_id", None),
        ("source_id", " source-a"),
        ("source_endpoint_id", ""),
        ("source_dataset_id", "x" * 129),
        ("resource_id", "resource\nid"),
    ):
        _reject(
            "identity input is invalid",
            security._source_record_hmac_message,
            **{**identity_fields_by_name, field: invalid_value},
        )

    digest = bytes(range(32))
    invalid_tokens = (
        dict(tin_id_128=bytearray(digest[:16]), tin_hmac_sha256=digest),
        dict(tin_id_128=digest[:15], tin_hmac_sha256=digest),
        dict(tin_id_128=digest[:16], tin_hmac_sha256=bytearray(digest)),
        dict(tin_id_128=digest[:16], tin_hmac_sha256=digest[:-1]),
        dict(tin_id_128=b"\0" * 16, tin_hmac_sha256=digest),
    )
    for token_fields in invalid_tokens:
        _reject(
            "identity token is invalid",
            TinTaxIdentityToken,
            token_policy_id=TOKEN_POLICY_ID,
            **token_fields,
        )
    token = token_policy(tmp_path).tokenize_ein(TEST_EIN)
    assert token.has_matching_full_hmac(token.tin_hmac_sha256)
    assert not token.has_matching_full_hmac(memoryview(token.tin_hmac_sha256))
    assert "digest=<redacted>" in repr(token)


def test_evidence_rows_and_results_reject_invalid_identity_states(tmp_path):
    evidence_row, _ = _evidence_and_vector(tmp_path)
    invalid_rows = (
        (dict(token=object()), "TIN token is invalid"),
        (dict(npi=True), "NPI"),
        (dict(npi=1234567890), "NPI"),
        (dict(source_record_hmac_sha256=b"short"), "identity is invalid"),
        (dict(source_record_identity_sha256=bytearray(32)), "identity is invalid"),
        (dict(relationship_class="legal_owner"), "relationship is invalid"),
    )
    for changes, message in invalid_rows:
        _reject(message, replace, evidence_row, **changes)
    object.__setattr__(evidence_row, "relationship_class", "x" * 0x10000)
    _reject("identity is invalid", lambda: evidence_row.evidence_id)

    invalid_results = (
        ("matched", (), "state is invalid"),
        (FhirOrganizationEvidenceState.MATCHED, [evidence_row], "result is invalid"),
        (FhirOrganizationEvidenceState.MATCHED, (object(),), "result is invalid"),
        (FhirOrganizationEvidenceState.MATCHED, (), "result is inconsistent"),
        (
            FhirOrganizationEvidenceState.MISSING_EIN,
            (evidence_row,),
            "result is inconsistent",
        ),
    )
    for state, evidence_rows, message in invalid_results:
        _reject(
            message,
            FhirOrganizationEvidenceResult,
            state=state,
            evidence=evidence_rows,
        )


def test_evidence_payload_and_identity_sets_are_canonical_and_bounded():
    class DisplayValue:
        def __str__(self):
            return "stable-display"

    payload_by_key = {
        "date": dt.date(2026, 7, 29),
        "datetime": dt.datetime(2026, 7, 29, tzinfo=dt.timezone.utc),
        "display": DisplayValue(),
    }
    payload_hash = evidence.canonical_provider_directory_payload_hash(payload_by_key)
    assert payload_hash == evidence.canonical_provider_directory_payload_hash(
        payload_by_key
    )
    _reject(
        "payload is invalid",
        evidence.canonical_provider_directory_payload_hash,
        [],
    )
    cyclic_values = []
    cyclic_values.append(cyclic_values)
    _reject(
        "payload is invalid",
        evidence.canonical_provider_directory_payload_hash,
        {"cycle": cyclic_values},
    )

    valid_digest = evidence.canonical_fhir_organization_identity_sha256(
        (("organization-a", payload_hash), ("organization-b", payload_hash))
    )
    assert len(valid_digest) == 64
    invalid_identity_sets = (
        "organization-a",
        (("organization-a", payload_hash), ("organization-a", payload_hash)),
        (("organization-b", payload_hash), ("organization-a", payload_hash)),
        (("organization-a",),),
    )
    for identities in invalid_identity_sets:
        _reject(
            "identit",
            evidence.canonical_fhir_organization_identity_sha256,
            identities,
        )


def test_forward_lookup_shape_and_provenance_guards(tmp_path):
    valid_fields = _lookup_fields(tmp_path)
    invalid_cases = (
        (dict(token=object()), "token is invalid"),
        (dict(relationship_class="legal_owner"), "relationship is invalid"),
        (dict(npis=[]), "NPIs are invalid"),
        (dict(npis=()), "NPIs are invalid"),
        (dict(npis=(1234567893, 1234567893)), "NPIs are invalid"),
        (dict(npis=(True,)), "NPIs are invalid"),
        (dict(evidence_count=True), "evidence count is invalid"),
        (dict(evidence_count=0), "evidence count is invalid"),
        (dict(source_ids=("source-b", "source-a")), "source IDs are invalid"),
        (dict(source_evidence_counts=[1]), "source bitmap is invalid"),
        (
            dict(
                source_evidence_counts=(),
                source_bitmap=b"",
                npi_source_bitmap_matrix=b"",
            ),
            "source bitmap is invalid",
        ),
        (dict(source_bitmap=bytearray(b"\x01")), "source bitmap is invalid"),
        (dict(source_bitmap=b"\0"), "source bitmap is invalid"),
        (
            dict(npi_source_bitmap_matrix=bytearray(b"\x01")),
            "source bitmap is invalid",
        ),
        (dict(npi_source_bitmap_matrix=b"\x01\x01"), "source bitmap is invalid"),
        (
            dict(
                source_ids=("source-a", "source-b"),
                source_evidence_counts=(-1, 2),
            ),
            "source bitmap is invalid",
        ),
        (dict(source_evidence_counts=(2,)), "source bitmap is invalid"),
        (
            dict(source_bitmap=b"\x02", npi_source_bitmap_matrix=b"\x02"),
            "source bitmap is invalid",
        ),
        (
            dict(source_bitmap=b"\x02", npi_source_bitmap_matrix=b"\x01"),
            "source bitmap is invalid",
        ),
        (
            dict(
                evidence_count=2,
                source_ids=("source-a", "source-b"),
                source_bitmap=b"\x03",
                npi_source_bitmap_matrix=b"\x03",
                source_evidence_counts=(0, 2),
            ),
            "source bitmap is invalid",
        ),
    )
    for changes, message in invalid_cases:
        _reject(message, TinNpiLookupRow, **{**valid_fields, **changes})


def test_lookup_query_reverse_and_digest_boundaries(tmp_path):
    lookup_row = TinNpiLookupRow(**_lookup_fields(tmp_path))
    assert "token=<redacted>" in repr(lookup_row)
    assert lookup_row.npis_supported_by_source_ordinal(0) == (1234567893,)
    for source_ordinal in (True, -1, 1):
        _reject(
            "ordinal is invalid",
            lookup_row.npis_supported_by_source_ordinal,
            source_ordinal,
        )
    _reject("NPI is unavailable", lookup_row.source_bitmap_for_npi, 1000000004)
    _reject(
        "token is invalid",
        NpiTinLookupReference,
        token=object(),
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    )
    _reject(
        "relationship is invalid",
        NpiTinLookupReference,
        token=lookup_row.token,
        relationship_class="legal_owner",
    )
    reference = NpiTinLookupReference(
        token=lookup_row.token,
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    )
    assert "token=<redacted>" in repr(reference)
    invalid_reverse_rows = (
        (True, (reference,), "row is invalid"),
        (1234567893, [], "row is invalid"),
        (1234567893, (), "row is invalid"),
        (1234567893, (object(),), "row is invalid"),
        (1234567893, (reference, reference), "references are invalid"),
    )
    for npi, references, message in invalid_reverse_rows:
        _reject(message, NpiTinLookupRow, npi=npi, tax_identities=references)

    for scan_digest, lookup_digest in (
        (None, b"\0" * 32),
        (b"\0" * 31, b"\0" * 32),
        (b"\0" * 32, None),
        (b"\0" * 32, b"\0" * 31),
    ):
        _reject(
            "digests are invalid",
            lookup._generation_id,
            source_vector_id="a" * 64,
            scan_proof_digest=scan_digest,
            lookup_digest=lookup_digest,
        )


def test_lookup_rejects_evidence_outside_source_ordinals(tmp_path):
    evidence_row, _ = _evidence_and_vector(tmp_path)
    _reject(
        "outside the ordinal map",
        lookup._source_support_for_evidence,
        (evidence_row,),
        source_ordinal_by_id={},
        source_count=1,
    )


def _validate_scope(evidence_row, vector):
    dataset = vector.fhir_datasets[0]
    dataset_by_key = {
        (dataset.source_id, dataset.endpoint_id, dataset.dataset_id): dataset
    }
    return build._validate_evidence_source_scope(
        evidence_row,
        source_vector=vector,
        selected_dataset_by_key=dataset_by_key,
        selected_policy_ids=set(vector.token_policy_ids),
    )


def test_build_scope_rejects_source_vector_drift(tmp_path):
    evidence_row, vector = _evidence_and_vector(tmp_path)
    _reject("evidence row is invalid", _validate_scope, object(), vector)
    outside_changes = (
        dict(source_dataset_id="other-dataset"),
        dict(
            token=TinTaxIdentityToken(
                token_policy_id=f"{TIN_TOKEN_POLICY_PREFIX}other",
                tin_id_128=bytes(range(16)),
                tin_hmac_sha256=bytes(range(32)),
            )
        ),
        dict(identifier_policy_id="other-policy"),
        dict(identifier_policy_sha256="0" * 64),
        dict(evidence_as_of="2026-07-28T00:00:00.000000Z"),
    )
    for changes in outside_changes:
        _reject(
            "outside its source vector",
            _validate_scope,
            replace(evidence_row, **changes),
            vector,
        )
    for changes in (
        dict(identifier_rule_id="other-rule"),
        dict(identifier_rule_sha256="0" * 64),
    ):
        _reject(
            "identifier rule",
            _validate_scope,
            replace(evidence_row, **changes),
            vector,
        )


def test_build_deduplication_and_source_vector_guards(tmp_path, monkeypatch):
    evidence_row, vector = _evidence_and_vector(tmp_path)
    monkeypatch.setattr(
        evidence.FhirTinNpiEvidence,
        "evidence_id",
        property(lambda self: b"\0" * 32),
    )
    _reject(
        "identity collision",
        build._deduplicate_evidence_rows,
        (evidence_row, replace(evidence_row, npi=1000000004)),
        source_vector=vector,
        scan_proofs=(),
    )
    monkeypatch.undo()
    two_policy_vector = source_vector(
        fhir_datasets=vector.fhir_datasets,
        policy_ids=(TOKEN_POLICY_ID, f"{TIN_TOKEN_POLICY_PREFIX}2026-07-b"),
    )
    for selected_vector, message in (
        (two_policy_vector, "every token policy"),
        (vector, "scan evidence identity collision"),
    ):
        _reject(
            message,
            build._deduplicate_evidence_rows,
            (evidence_row,),
            source_vector=selected_vector,
            scan_proofs=(),
        )
    _reject(
        "source vector is invalid",
        build.build_compact_tin_npi_generation,
        (),
        source_vector=object(),
    )


def _adapt(organization_row):
    return adapters.extract_normalized_organization_evidence_for_policies(
        organization_row,
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        token_projectors=(),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
    )


def test_normalized_adapter_rejects_non_organization_or_malformed_rows():
    for organization_row in ([], {"resource_type": "Location"}):
        extraction = _adapt(organization_row)
        assert extraction.state is FhirOrganizationEvidenceState.NOT_ORGANIZATION
        assert extraction.evidence == ()
    malformed_rows = (
        (
            {"resource_type": "Organization", "payload_json": []},
            "payload is invalid",
        ),
        (
            dict(
                resource_type="Organization",
                resource_id="outer",
                payload_hash="0" * 64,
                payload_json={"resource_id": "inner"},
            ),
            "resource identity mismatch",
        ),
    )
    for organization_row, message in malformed_rows:
        _reject(message, _adapt, organization_row)
