# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generation model and SQL loaders shared by connector PostgreSQL proofs."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from process import tin_npi_connector as connector
from process.tin_npi_connector_lookup import _generation_id, _lookup_digest
from process.tin_npi_connector_scan import (
    canonical_fhir_organization_scan_proof_digest,
    canonical_fhir_organization_scan_proof_json,
)


@dataclass(frozen=True)
class GenerationCounts:
    """Expected canonical row and edge counts for one generation."""

    organization_count: int
    matched_organization_count: int
    evidence_count: int
    forward_row_count: int
    reverse_row_count: int
    npi_edge_count: int


@dataclass(frozen=True)
class GenerationModel:
    """Canonical source, proof, lookup, and evidence payload for one build."""

    source_vector: connector.TinNpiConnectorSourceVector
    scan_proofs: tuple[connector.FhirOrganizationScanProof, ...]
    lookup_rows: tuple[connector.TinNpiLookupRow, ...]
    evidence_rows: tuple[connector.FhirTinNpiEvidence, ...]
    scan_proof_json: str
    scan_proof_digest: bytes
    lookup_digest: bytes
    generation_id: bytes
    counts: GenerationCounts


def build_generation_model(
    source_vector,
    scan_proofs,
    lookup_rows,
    evidence_rows,
    *,
    organization_count,
    matched_organization_count,
):
    scan_proof_json = canonical_fhir_organization_scan_proof_json(scan_proofs)
    scan_proof_digest = canonical_fhir_organization_scan_proof_digest(scan_proofs)
    lookup_digest = _lookup_digest(lookup_rows)
    generation_id = bytes.fromhex(
        _generation_id(
            source_vector_id=source_vector.source_vector_id,
            scan_proof_digest=scan_proof_digest,
            lookup_digest=lookup_digest,
        )
    )
    counts = GenerationCounts(
        organization_count=organization_count,
        matched_organization_count=matched_organization_count,
        evidence_count=len(evidence_rows),
        forward_row_count=len(lookup_rows),
        reverse_row_count=len(
            {npi for lookup_row in lookup_rows for npi in lookup_row.npis}
        ),
        npi_edge_count=sum(len(lookup_row.npis) for lookup_row in lookup_rows),
    )
    return GenerationModel(
        source_vector=source_vector,
        scan_proofs=scan_proofs,
        lookup_rows=lookup_rows,
        evidence_rows=evidence_rows,
        scan_proof_json=scan_proof_json,
        scan_proof_digest=scan_proof_digest,
        lookup_digest=lookup_digest,
        generation_id=generation_id,
        counts=counts,
    )


GENERATION_INSERT_SQL = """
    INSERT INTO {quoted_schema}.tin_npi_connector_generation (
        generation_id,
        source_vector_id,
        source_vector_canonical_json,
        schema_version,
        lookup_schema_version,
        lookup_contract_id,
        generation_contract,
        raw_policy,
        projection_policy_id,
        relationship_class,
        site_resolution_contract_id,
        source_record_identity_contract_id,
        identifier_policy_id,
        identifier_policy_sha256,
        evidence_as_of,
        source_ordinal_contract,
        source_ordinal_map_canonical_json,
        source_ordinal_map_digest,
        scan_contract_id,
        scan_proof_canonical_json,
        scan_proof_digest,
        source_count,
        source_dataset_count,
        source_relation_count,
        token_policy_count,
        lookup_digest,
        organization_count,
        matched_organization_count,
        evidence_count,
        forward_row_count,
        reverse_row_count,
        npi_edge_count,
        build_token_sha256,
        build_lease_expires_at,
        state
    ) VALUES (
        $1, $2, $3, 3, 2,
        'healthporta.tin-npi.compact-lookup.v2',
        'tin_npi_connector_generation_v3',
        'token_only_v1',
        $4,
        'same_organization_identifier',
        $5,
        $6,
        $7,
        $8,
        $9,
        'source_id_sorted_utf8_lsb0_bitmap_v1',
        $10,
        $11,
        $12,
        $13,
        $14,
        $15, $16, $17, $18,
        $19,
        $20, $21, $22, $23, $24, $25,
        $26,
        clock_timestamp() + make_interval(secs => $27::double precision),
        'building'
    )
    RETURNING generation_key
"""


async def insert_generation(
    connection,
    quoted_schema,
    model,
    build_token,
    lease_seconds=3600.0,
):
    vector = model.source_vector
    counts = model.counts
    generation_key = await connection.fetchval(
        GENERATION_INSERT_SQL.format(quoted_schema=quoted_schema),
        model.generation_id,
        bytes.fromhex(vector.source_vector_id),
        vector.canonical_json,
        vector.projection_policy_id,
        connector.TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
        connector.FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
        vector.identifier_policy.policy_id,
        bytes.fromhex(vector.identifier_policy.descriptor_sha256),
        vector.evidence_as_of,
        connector.canonical_source_ordinal_map_json(
            fhir_dataset.source_id for fhir_dataset in vector.fhir_datasets
        ),
        connector.canonical_source_ordinal_map_digest(
            fhir_dataset.source_id for fhir_dataset in vector.fhir_datasets
        ),
        connector.TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
        model.scan_proof_json,
        model.scan_proof_digest,
        len({fhir_dataset.source_id for fhir_dataset in vector.fhir_datasets}),
        len(vector.fhir_datasets),
        len(vector.input_relations),
        len(vector.token_policies),
        model.lookup_digest,
        counts.organization_count,
        counts.matched_organization_count,
        counts.evidence_count,
        counts.forward_row_count,
        counts.reverse_row_count,
        counts.npi_edge_count,
        hashlib.sha256(build_token.encode()).digest(),
        lease_seconds,
    )
    return int(generation_key)


async def set_build_token(connection, build_token):
    await connection.execute(
        "SELECT set_config('healthporta.tin_npi_build_token', $1, TRUE)",
        build_token,
    )


async def insert_generation_policies(
    connection,
    quoted_schema,
    generation_key,
    policy_ids,
):
    await connection.executemany(
        f"""
        INSERT INTO {quoted_schema}.tin_npi_connector_generation_policy (
            generation_key,
            token_policy_id
        ) VALUES ($1, $2)
        """,
        [(generation_key, token_policy_id) for token_policy_id in policy_ids],
    )


async def insert_lookup_rows(connection, quoted_schema, generation_key, lookup_rows):
    await connection.executemany(
        f"""
        INSERT INTO {quoted_schema}.tin_npi_connector_lookup (
            generation_key,
            token_policy_id,
            tin_id_128,
            tin_hmac_sha256,
            npis,
            evidence_count,
            source_bitmap,
            npi_source_bitmap_matrix,
            source_evidence_counts
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        [
            (
                generation_key,
                lookup_row.token.token_policy_id,
                lookup_row.token.tin_id_128,
                lookup_row.token.tin_hmac_sha256,
                list(lookup_row.npis),
                lookup_row.evidence_count,
                lookup_row.source_bitmap,
                lookup_row.npi_source_bitmap_matrix,
                list(lookup_row.source_evidence_counts),
            )
            for lookup_row in lookup_rows
        ],
    )


async def insert_evidence_rows(
    connection,
    quoted_schema,
    generation_key,
    evidence_rows,
):
    await connection.executemany(
        f"""
        INSERT INTO {quoted_schema}.tin_npi_connector_evidence (
            generation_key,
            evidence_id,
            token_policy_id,
            tin_id_128,
            tin_hmac_sha256,
            npi,
            source_ordinal,
            relationship_class,
            source_record_hmac_sha256,
            source_record_identity_sha256,
            source_record_payload_sha256,
            identifier_policy_sha256,
            identifier_rule_id,
            identifier_rule_sha256
        ) VALUES (
            $1, $2, $3, $4, $5, $6, 0, $7, $8, $9, $10, $11, $12, $13
        )
        """,
        [
            (
                generation_key,
                evidence_row.evidence_id,
                evidence_row.token.token_policy_id,
                evidence_row.token.tin_id_128,
                evidence_row.token.tin_hmac_sha256,
                evidence_row.npi,
                evidence_row.relationship_class,
                evidence_row.source_record_hmac_sha256,
                evidence_row.source_record_identity_sha256,
                bytes.fromhex(evidence_row.source_record_payload_hash),
                bytes.fromhex(evidence_row.identifier_policy_sha256),
                evidence_row.identifier_rule_id,
                bytes.fromhex(evidence_row.identifier_rule_sha256),
            )
            for evidence_row in evidence_rows
        ],
    )


async def load_generation_children(
    connection,
    quoted_schema,
    generation_key,
    model,
):
    policy_ids = tuple(
        policy.token_policy_id for policy in model.source_vector.token_policies
    )
    await insert_generation_policies(
        connection,
        quoted_schema,
        generation_key,
        policy_ids,
    )
    await insert_lookup_rows(
        connection,
        quoted_schema,
        generation_key,
        model.lookup_rows,
    )
    await insert_evidence_rows(
        connection,
        quoted_schema,
        generation_key,
        model.evidence_rows,
    )
