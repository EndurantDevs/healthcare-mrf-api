# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lookup factoring and source-provenance tests for connector generations."""

from __future__ import annotations

from dataclasses import replace

import pytest

from process.tin_npi_connector import (
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FhirTinNpiIdentifierPolicy,
    TinNpiConnectorError,
    TinNpiLookupRow,
    TinTaxIdentityToken,
    build_compact_tin_npi_generation,
    canonical_provider_directory_payload_hash,
    canonical_source_ordinal_map_digest,
    extract_fhir_organization_tin_npi_evidence,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    REVIEWED_TAX_AS_EIN_POLICY,
    TEST_EIN,
    TOKEN_POLICY_ID,
    extract_evidence,
    fhir_dataset,
    identifier_rule,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    token_policy,
    typed_identifier,
    unmatched_scan,
)


def test_compact_generation_factors_npis_and_reverse_lookup(tmp_path):
    generation, vector = _build_multi_source_generation(tmp_path)

    assert generation.source_vector_id == vector.source_vector_id
    assert generation.generation_id != vector.source_vector_id
    assert generation.evidence_count == 3
    assert not hasattr(generation, "evidence_digest")
    assert generation.source_ordinal_map == ("source-a", "source-b")
    assert generation.source_ordinal_map_json == (
        '[{"ordinal":0,"source_id":"source-a"},' '{"ordinal":1,"source_id":"source-b"}]'
    )
    assert len(generation.source_ordinal_map_digest) == 32
    assert len(generation.lookup_digest) == 32
    assert len(generation.forward_rows) == 1
    _assert_multi_source_lookup(generation)
    _assert_generation_mutations_rejected(generation)


def _build_multi_source_generation(tmp_path):
    multi_source_policy = FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=(
            identifier_rule(),
            identifier_rule(source_id="source-b", endpoint_id="endpoint-b"),
        ),
    )
    first_organization = organization(
        npi_identifier("1234567893"),
        typed_identifier("NPI", "1000000004"),
        typed_identifier("TAX", TEST_EIN),
    )
    first_extraction = extract_evidence(
        first_organization,
        tmp_path,
        identifier_policy_override=multi_source_policy,
    )
    second_organization = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
        resource_id="organization-b",
    )
    second_hash = canonical_provider_directory_payload_hash(second_organization)
    second_extraction = _extract_second_source(
        second_organization,
        second_hash,
        multi_source_policy,
        tmp_path,
    )
    vector = _multi_source_vector(
        first_extraction.evidence[0].source_record_payload_hash,
        second_hash,
        multi_source_policy,
    )
    scans = (
        matched_scan(first_extraction),
        matched_scan(
            second_extraction,
            source_id="source-b",
            endpoint_id="endpoint-b",
            dataset_id="dataset-b",
            resource_id="organization-b",
            payload_hash=second_hash,
        ),
    )
    return build_compact_tin_npi_generation(scans, source_vector=vector), vector


def _extract_second_source(resource, payload_hash, policy, tmp_path):
    return extract_fhir_organization_tin_npi_evidence(
        resource,
        source_id="source-b",
        source_endpoint_id="endpoint-b",
        source_dataset_id="dataset-b",
        resource_payload_hash=payload_hash,
        token_projector=token_policy(tmp_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=policy,
    )


def _multi_source_vector(first_payload_hash, second_payload_hash, policy):
    return source_vector(
        fhir_datasets=(
            fhir_dataset(
                organization_identities=(("organization-1", first_payload_hash),),
            ),
            fhir_dataset(
                source_id="source-b",
                endpoint_id="endpoint-b",
                dataset_id="dataset-b",
                dataset_hash="b" * 64,
                organization_identities=(("organization-b", second_payload_hash),),
            ),
        ),
        identifier_policy_override=policy,
    )


def _assert_multi_source_lookup(generation):
    lookup = generation.forward_rows[0]
    assert lookup.npis == (1000000004, 1234567893)
    assert lookup.evidence_count == 3
    assert lookup.source_ids == ("source-a", "source-b")
    assert lookup.source_bitmap == b"\x03"
    assert lookup.npi_source_bitmap_matrix == b"\x01\x03"
    assert lookup.source_evidence_counts == (2, 1)
    assert lookup.source_bitmap_for_npi(1000000004) == b"\x01"
    assert lookup.source_bitmap_for_npi(1234567893) == b"\x03"
    assert lookup.npis_supported_by_source_ordinal(0) == (
        1000000004,
        1234567893,
    )
    assert lookup.npis_supported_by_source_ordinal(1) == (1234567893,)
    assert [reverse.npi for reverse in generation.reverse_rows] == [
        1000000004,
        1234567893,
    ]


def _assert_generation_mutations_rejected(generation):
    lookup = generation.forward_rows[0]
    generation_changes = (
        ({"lookup_digest": b"\0" * 32}, "generation is inconsistent"),
        ({"source_ordinal_map_digest": b"\0" * 32}, "generation is inconsistent"),
        (
            {"reverse_rows": tuple(reversed(generation.reverse_rows))},
            "reverse rows are invalid",
        ),
    )
    for invalid_changes, error_marker in generation_changes:
        with pytest.raises(TinNpiConnectorError, match=error_marker):
            replace(generation, **invalid_changes)
    with pytest.raises(TinNpiConnectorError, match="source bitmap is invalid"):
        replace(
            generation,
            forward_rows=(replace(lookup, source_bitmap=b"\x01"),),
        )
    with pytest.raises(TinNpiConnectorError, match="generation is inconsistent"):
        replace(
            generation,
            forward_rows=(replace(lookup, npi_source_bitmap_matrix=b"\x03\x01"),),
        )


def test_lookup_row_requires_aligned_nonempty_per_npi_source_segments():
    token = TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=bytes(range(16)),
        tin_hmac_sha256=bytes(range(32)),
    )
    valid_lookup = {
        "token": token,
        "relationship_class": FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        "npis": (1000000004, 1234567893),
        "evidence_count": 2,
        "source_ids": ("source-a",),
        "source_bitmap": b"\x01",
        "npi_source_bitmap_matrix": b"\x01\x01",
        "source_evidence_counts": (2,),
    }
    invalid_changes = (
        {"npi_source_bitmap_matrix": b"\x01"},
        {"npi_source_bitmap_matrix": b"\x01\x00"},
        {
            "evidence_count": 3,
            "source_ids": ("source-a", "source-b"),
            "source_bitmap": b"\x03",
            "npi_source_bitmap_matrix": b"\x03\x01",
            "source_evidence_counts": (1, 2),
        },
    )
    for lookup_changes in invalid_changes:
        with pytest.raises(TinNpiConnectorError, match="source bitmap is invalid"):
            TinNpiLookupRow(**{**valid_lookup, **lookup_changes})

    lookup = TinNpiLookupRow(**valid_lookup)
    with pytest.raises(TinNpiConnectorError, match="NPI is unavailable"):
        lookup.source_bitmap_for_npi(1999999999)
    with pytest.raises(TinNpiConnectorError, match="source ordinal is invalid"):
        lookup.npis_supported_by_source_ordinal(1)


def test_source_bitmaps_follow_vector(tmp_path):
    generation = _build_nine_source_generation(tmp_path)
    expected_source_ids = tuple(f"source-{index:02d}" for index in range(9))

    assert generation.source_ordinal_map == expected_source_ids
    assert generation.source_ordinal_map_digest == (
        canonical_source_ordinal_map_digest(reversed(expected_source_ids))
    )
    lookup = generation.forward_rows[0]
    assert lookup.source_ids == ("source-00", "source-03", "source-08")
    assert lookup.source_bitmap == b"\x09\x01"
    assert lookup.npi_source_bitmap_matrix == b"\x09\x01"
    assert lookup.source_evidence_counts == (1, 0, 0, 1, 0, 0, 0, 0, 1)


def _build_nine_source_generation(tmp_path):
    organization_resource = organization(
        npi_identifier("1234567893"),
        typed_identifier("TAX", TEST_EIN),
    )
    payload_hash = canonical_provider_directory_payload_hash(organization_resource)
    fhir_datasets = tuple(
        fhir_dataset(
            source_id=f"source-{index:02d}",
            endpoint_id=f"endpoint-{index:02d}",
            dataset_id=f"dataset-{index:02d}",
            dataset_hash=f"{index + 1:x}" * 64,
            organization_identities=(("organization-1", payload_hash),),
        )
        for index in range(9)
    )
    vector = source_vector(fhir_datasets=tuple(reversed(fhir_datasets)))
    projector = token_policy(tmp_path)
    scans = tuple(
        _nine_source_scan(
            index,
            organization_resource,
            payload_hash,
            projector,
            vector.identifier_policy,
        )
        for index in range(9)
    )
    return build_compact_tin_npi_generation(scans, source_vector=vector)


def _nine_source_scan(index, organization_resource, payload_hash, projector, policy):
    scan_identity_map = {
        "source_id": f"source-{index:02d}",
        "endpoint_id": f"endpoint-{index:02d}",
        "dataset_id": f"dataset-{index:02d}",
        "resource_id": "organization-1",
    }
    if index not in {0, 3, 8}:
        return unmatched_scan(**scan_identity_map, payload_hash=payload_hash)
    extraction = extract_fhir_organization_tin_npi_evidence(
        organization_resource,
        source_id=scan_identity_map["source_id"],
        source_endpoint_id=scan_identity_map["endpoint_id"],
        source_dataset_id=scan_identity_map["dataset_id"],
        resource_payload_hash=payload_hash,
        token_projector=projector,
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=policy,
    )
    return matched_scan(extraction, **scan_identity_map)
