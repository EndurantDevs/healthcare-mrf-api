# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded, redacted admission tests for connector publications."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

import pytest

from process.tin_npi_connector import (
    build_compact_tin_npi_generation,
    canonical_evidence_as_of,
    canonical_fhir_organization_scan_proof_digest,
)
from process.tin_npi_connector_lookup import _generation_id
from process.tin_npi_connector_publication import (
    ConnectorPublicationBundle,
    ConnectorPublicationLimits,
    TIN_NPI_GENERATION_CONTRACT_ID,
    TIN_NPI_RAW_POLICY_ID,
    TinNpiConnectorPublicationError,
    admit_connector_publication_bundle,
)
from tests.tin_npi_connector_unit_support import (
    EVIDENCE_AS_OF,
    TEST_EIN,
    TEST_EIN_NORMALIZED,
    TEST_HMAC_HEX,
    extract_evidence,
    fhir_dataset,
    matched_scan,
    npi_identifier,
    organization,
    source_vector,
    typed_identifier,
)


def test_bundle_binds_frozen_contract_and_redacts_identity_material(tmp_path):
    bundle = _matched_bundle(tmp_path)

    assert bundle.generation_contract_id == TIN_NPI_GENERATION_CONTRACT_ID
    assert bundle.raw_policy_id == TIN_NPI_RAW_POLICY_ID
    rendered = repr(bundle)
    assert rendered == (
        "<tin-npi-connector-publication-bundle "
        "sources=1 policies=1 evidence=2>"
    )
    for sensitive_value in (
        TEST_EIN,
        TEST_EIN_NORMALIZED,
        TEST_HMAC_HEX,
        "1234567893",
        "1000000004",
        "source-a",
    ):
        assert sensitive_value not in rendered


def test_admission_returns_exact_non_sensitive_counts(tmp_path):
    bundle = _matched_bundle(tmp_path)
    counts = bundle.counts

    assert admit_connector_publication_bundle(
        bundle,
        limits=_limits_for(counts),
    ) == counts
    assert counts.source_count == counts.dataset_count == 1
    assert counts.token_policy_count == counts.organization_count == 1
    assert counts.metadata_byte_count > 0
    assert counts.evidence_row_count == counts.reverse_row_count == 2
    assert counts.forward_row_count == 1
    assert counts.npi_edge_count == 2


@pytest.mark.parametrize(
    "limit_name",
    (
        "max_sources",
        "max_datasets",
        "max_token_policies",
        "max_metadata_bytes",
        "max_organizations",
        "max_evidence_rows",
        "max_forward_rows",
        "max_reverse_rows",
        "max_npi_edges",
    ),
)
def test_admission_enforces_every_capacity_limit(tmp_path, limit_name):
    bundle = _matched_bundle(tmp_path)
    limits = replace(_limits_for(bundle.counts), **{limit_name: 0})

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication capacity exceeded",
    ):
        admit_connector_publication_bundle(bundle, limits=limits)


def test_complete_zero_evidence_requires_explicit_policy():
    bundle = _empty_bundle()
    limits = _limits_for(bundle.counts)

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="zero evidence requires explicit admission",
    ):
        admit_connector_publication_bundle(bundle, limits=limits)

    admitted = admit_connector_publication_bundle(
        bundle,
        limits=replace(limits, allow_complete_zero_evidence=True),
    )
    assert admitted.evidence_row_count == admitted.npi_edge_count == 0


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    (
        ("max_sources", -1),
        ("copy_batch_size", 127),
        ("build_lease_seconds", 29),
        ("lock_timeout_ms", 0),
        ("statement_timeout_ms", 0),
        ("operation_timeout_seconds", float("inf")),
        ("operation_timeout_seconds", 10**1000),
        ("allow_complete_zero_evidence", 1),
    ),
)
def test_limits_fail_closed(field_name, invalid_value):
    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication limits are invalid",
    ):
        replace(_limits_for(_empty_bundle().counts), **{field_name: invalid_value})


def test_derived_counts_fail_closed_when_forged():
    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication counts are invalid",
    ):
        replace(_empty_bundle().counts, source_count=-1)
    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication counts are invalid",
    ):
        replace(_empty_bundle().counts, token_policy_count=1 << 31)


def test_bundle_rejects_a_different_source_vector(tmp_path):
    bundle = _matched_bundle(tmp_path)
    changed_relation = replace(
        bundle.source_vector.input_relations[0],
        relation_oid=2002,
    )
    changed_vector = replace(
        bundle.source_vector,
        input_relations=(changed_relation,),
    )

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication source binding is invalid",
    ) as captured_error:
        ConnectorPublicationBundle(changed_vector, bundle.generation)
    error_text = repr(captured_error.value)
    assert TEST_EIN_NORMALIZED not in error_text
    assert TEST_HMAC_HEX not in error_text


def test_bundle_rejects_an_unsupported_projection_contract():
    bundle = _empty_bundle()
    changed_vector = replace(
        bundle.source_vector,
        projection_policy_id="healthporta.tin-npi.candidate-projection.v1",
    )
    changed_generation = build_compact_tin_npi_generation(
        (),
        source_vector=changed_vector,
    )

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication contract is unsupported",
    ):
        ConnectorPublicationBundle(changed_vector, changed_generation)


def test_bundle_rejects_scan_proof_drift():
    bundle = _empty_bundle()
    generation = bundle.generation
    changed_proofs = (
        replace(generation.scan_proofs[0], source_summary_sha256="e" * 64),
    )
    changed_scan_digest = canonical_fhir_organization_scan_proof_digest(
        changed_proofs
    )
    changed_generation = replace(
        generation,
        scan_proofs=changed_proofs,
        scan_proof_digest=changed_scan_digest,
        generation_id=_generation_id(
            source_vector_id=generation.source_vector_id,
            scan_proof_digest=changed_scan_digest,
            lookup_digest=generation.lookup_digest,
        ),
    )

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication source proof is invalid",
    ):
        ConnectorPublicationBundle(bundle.source_vector, changed_generation)


def test_bundle_rejects_evidence_cutoff_drift(tmp_path):
    bundle = _matched_bundle(tmp_path)
    changed_cutoff = canonical_evidence_as_of(
        dt.datetime(2026, 7, 28, tzinfo=dt.timezone.utc)
    )
    changed_evidence_rows = tuple(
        replace(evidence, evidence_as_of=changed_cutoff)
        for evidence in bundle.generation.evidence_rows
    )
    changed_generation = replace(
        bundle.generation,
        evidence_rows=changed_evidence_rows,
    )

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication evidence is outside its source vector",
    ):
        ConnectorPublicationBundle(bundle.source_vector, changed_generation)


def test_admission_requires_exact_contract_types():
    bundle = _empty_bundle()
    limits = _limits_for(bundle.counts)

    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication admission input is invalid",
    ):
        admit_connector_publication_bundle(object(), limits=limits)
    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication admission input is invalid",
    ):
        admit_connector_publication_bundle(bundle, limits=object())
    with pytest.raises(
        TinNpiConnectorPublicationError,
        match="connector publication bundle is invalid",
    ):
        ConnectorPublicationBundle(object(), bundle.generation)


def _matched_bundle(tmp_path) -> ConnectorPublicationBundle:
    extraction = extract_evidence(
        organization(
            npi_identifier("1234567893"),
            typed_identifier("NPI", "1000000004"),
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
    return ConnectorPublicationBundle(vector, generation)


def _empty_bundle() -> ConnectorPublicationBundle:
    vector = source_vector(
        fhir_datasets=(fhir_dataset(organization_identities=()),),
    )
    generation = build_compact_tin_npi_generation((), source_vector=vector)
    return ConnectorPublicationBundle(vector, generation)


def _limits_for(counts) -> ConnectorPublicationLimits:
    return ConnectorPublicationLimits(
        max_sources=counts.source_count,
        max_datasets=counts.dataset_count,
        max_token_policies=counts.token_policy_count,
        max_metadata_bytes=counts.metadata_byte_count,
        max_organizations=counts.organization_count,
        max_evidence_rows=counts.evidence_row_count,
        max_forward_rows=counts.forward_row_count,
        max_reverse_rows=counts.reverse_row_count,
        max_npi_edges=counts.npi_edge_count,
    )
