# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL metadata and registry operations for connector generations."""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping

from process.tin_npi_connector_generation_store_types import (
    ConnectorGenerationStoreConnection,
    TinNpiConnectorGenerationStoreError,
)
from process.tin_npi_connector_publication import (
    ConnectorPublicationBundle,
    ConnectorPublicationCounts,
    ConnectorPublicationLimits,
)
from process.tin_npi_connector_support import (
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
    TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
    TIN_NPI_GENERATION_CONTRACT_ID,
    TIN_NPI_LOOKUP_CONTRACT_ID,
    TIN_NPI_LOOKUP_SCHEMA_VERSION,
    TIN_NPI_RAW_POLICY_ID,
    TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
    TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID,
    TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
)

GENERATION_METADATA_COLUMNS = (
    "generation_id",
    "source_vector_id",
    "source_vector_canonical_json",
    "schema_version",
    "lookup_schema_version",
    "lookup_contract_id",
    "generation_contract",
    "raw_policy",
    "projection_policy_id",
    "relationship_class",
    "site_resolution_contract_id",
    "source_record_identity_contract_id",
    "identifier_policy_id",
    "identifier_policy_sha256",
    "evidence_as_of",
    "source_ordinal_contract",
    "source_ordinal_map_canonical_json",
    "source_ordinal_map_digest",
    "scan_contract_id",
    "scan_proof_canonical_json",
    "scan_proof_digest",
    "source_count",
    "source_dataset_count",
    "source_relation_count",
    "token_policy_count",
    "lookup_digest",
    "organization_count",
    "matched_organization_count",
    "evidence_count",
    "forward_row_count",
    "reverse_row_count",
    "npi_edge_count",
)


async def set_transaction_guards(
    connection: ConnectorGenerationStoreConnection,
    limits: ConnectorPublicationLimits,
) -> None:
    """Set transaction-local durability and timeout controls."""

    await connection.execute(
        "SELECT set_config('lock_timeout', $1, true)",
        f"{limits.lock_timeout_ms}ms",
    )
    await connection.execute(
        "SELECT set_config('statement_timeout', $1, true)",
        f"{limits.statement_timeout_ms}ms",
    )
    await connection.execute(
        "SELECT set_config('synchronous_commit', 'on', true)"
    )


async def register_and_verify_policies(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
) -> None:
    """Insert-or-verify every immutable policy descriptor."""

    await _register_identifier_policy(connection, schema, bundle)
    for token_policy in sorted(
        bundle.source_vector.token_policies,
        key=lambda policy: policy.token_policy_id,
    ):
        await _register_token_policy(connection, schema, token_policy)


async def _register_identifier_policy(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
) -> None:
    policy = bundle.source_vector.identifier_policy
    digest = bytes.fromhex(policy.descriptor_sha256)
    await connection.execute(
        f"""
        INSERT INTO {table(schema, 'tin_npi_connector_identifier_policy')} (
            identifier_policy_id, descriptor_canonical_json,
            identifier_policy_sha256
        ) VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        policy.policy_id,
        policy.descriptor_canonical_json,
        digest,
    )
    observed = await connection.fetchrow(
        f"""
        SELECT descriptor_canonical_json, identifier_policy_sha256
          FROM {table(schema, 'tin_npi_connector_identifier_policy')}
         WHERE identifier_policy_id = $1
        """,
        policy.policy_id,
    )
    expected_fields_by_name = {
        "descriptor_canonical_json": policy.descriptor_canonical_json,
        "identifier_policy_sha256": digest,
    }
    if not is_exact_record_match(observed, expected_fields_by_name):
        raise TinNpiConnectorGenerationStoreError(
            "connector identifier policy registry conflict"
        )


async def _register_token_policy(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    policy,
) -> None:
    digest = bytes.fromhex(policy.token_policy_descriptor_sha256)
    await connection.execute(
        f"""
        INSERT INTO {table(schema, 'tin_npi_connector_token_policy')} (
            token_policy_id, token_policy_descriptor_sha256
        ) VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        """,
        policy.token_policy_id,
        digest,
    )
    observed = await connection.fetchrow(
        f"""
        SELECT token_policy_descriptor_sha256
          FROM {table(schema, 'tin_npi_connector_token_policy')}
         WHERE token_policy_id = $1
        """,
        policy.token_policy_id,
    )
    if not is_exact_record_match(
        observed,
        {"token_policy_descriptor_sha256": digest},
    ):
        raise TinNpiConnectorGenerationStoreError(
            "connector token policy registry conflict"
        )


async def insert_generation(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    bundle: ConnectorPublicationBundle,
    *,
    counts: ConnectorPublicationCounts,
    limits: ConnectorPublicationLimits,
    build_token: str,
) -> int | None:
    """Insert one building generation, or return None on identity conflict."""

    generation_key = await connection.fetchval(
        _generation_insert_sql(schema),
        *_generation_insert_parameters(
            bundle,
            counts,
            limits=limits,
            build_token=build_token,
        ),
    )
    if generation_key is None:
        return None
    if type(generation_key) is not int or generation_key <= 0:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation key is invalid"
        )
    return generation_key


def _generation_insert_sql(schema: str) -> str:
    return f"""
        INSERT INTO {table(schema, 'tin_npi_connector_generation')} (
            generation_id, source_vector_id, source_vector_canonical_json,
            schema_version, lookup_schema_version, lookup_contract_id,
            generation_contract, raw_policy, projection_policy_id,
            relationship_class, site_resolution_contract_id,
            source_record_identity_contract_id, identifier_policy_id,
            identifier_policy_sha256, evidence_as_of,
            source_ordinal_contract, source_ordinal_map_canonical_json,
            source_ordinal_map_digest, scan_contract_id,
            scan_proof_canonical_json, scan_proof_digest, source_count,
            source_dataset_count, source_relation_count, token_policy_count,
            lookup_digest, organization_count, matched_organization_count,
            evidence_count, forward_row_count, reverse_row_count,
            npi_edge_count, build_token_sha256, build_lease_expires_at, state
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25,
            $26, $27, $28, $29, $30, $31, $32, $33,
            clock_timestamp() + make_interval(secs => $34::double precision),
            $35
        )
        ON CONFLICT DO NOTHING
        RETURNING generation_key
    """


def _generation_insert_parameters(
    bundle: ConnectorPublicationBundle,
    counts: ConnectorPublicationCounts,
    *,
    limits: ConnectorPublicationLimits,
    build_token: str,
) -> tuple[object, ...]:
    metadata = expected_generation_metadata(bundle, counts)
    return tuple(metadata[column] for column in GENERATION_METADATA_COLUMNS) + (
        hashlib.sha256(build_token.encode("ascii")).digest(),
        limits.build_lease_seconds,
        "building",
    )


async def read_generation(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    source_vector_id: bytes,
) -> Mapping[str, object] | None:
    """Read and share-lock the generation bound to one source vector."""

    selected_columns = ", ".join(GENERATION_METADATA_COLUMNS)
    return await connection.fetchrow(
        f"""
        SELECT generation_key, state, completed_at, failed_at, retired_at,
               gc_after, {selected_columns}
          FROM {table(schema, 'tin_npi_connector_generation')}
         WHERE source_vector_id = $1
         FOR SHARE
        """,
        source_vector_id,
    )


async def seal_generation(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    generation_key: int,
    source_vector_id: bytes,
) -> Mapping[str, object]:
    """Trigger database sealing and return the verified complete row."""

    status = await connection.execute(
        f"""
        UPDATE {table(schema, 'tin_npi_connector_generation')}
           SET state = 'complete'
         WHERE generation_key = $1 AND state = 'building'
        """,
        generation_key,
    )
    if status != "UPDATE 1":
        raise TinNpiConnectorGenerationStoreError(
            "connector generation seal failed"
        )
    sealed = await read_generation(connection, schema, source_vector_id)
    if sealed is None:
        raise TinNpiConnectorGenerationStoreError(
            "connector generation seal verification failed"
        )
    return sealed


def is_exact_record_match(
    record: Mapping[str, object] | None,
    expected: Mapping[str, object],
) -> bool:
    """Return whether every expected field matches with constant-time bytes."""

    if record is None:
        return False
    return all(
        _is_exact_value_match(record[field], value)
        for field, value in expected.items()
    )


def _is_exact_value_match(observed: object, expected: object) -> bool:
    if type(expected) is bytes:
        return type(observed) is bytes and hmac.compare_digest(observed, expected)
    return observed == expected


def expected_generation_metadata(
    bundle: ConnectorPublicationBundle,
    counts: ConnectorPublicationCounts,
) -> dict[str, object]:
    """Return every immutable generation field required for exact reuse."""

    vector = bundle.source_vector
    generation = bundle.generation
    policy = vector.identifier_policy
    return {
        "generation_id": bytes.fromhex(generation.generation_id),
        "source_vector_id": bytes.fromhex(vector.source_vector_id),
        "source_vector_canonical_json": vector.canonical_json,
        "schema_version": TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
        "lookup_schema_version": TIN_NPI_LOOKUP_SCHEMA_VERSION,
        "lookup_contract_id": TIN_NPI_LOOKUP_CONTRACT_ID,
        "generation_contract": TIN_NPI_GENERATION_CONTRACT_ID,
        "raw_policy": TIN_NPI_RAW_POLICY_ID,
        "projection_policy_id": vector.projection_policy_id,
        "relationship_class": FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        "site_resolution_contract_id": TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
        "source_record_identity_contract_id": (
            FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID
        ),
        "identifier_policy_id": policy.policy_id,
        "identifier_policy_sha256": bytes.fromhex(policy.descriptor_sha256),
        "evidence_as_of": vector.evidence_as_of,
        "source_ordinal_contract": TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID,
        "source_ordinal_map_canonical_json": generation.source_ordinal_map_json,
        "source_ordinal_map_digest": generation.source_ordinal_map_digest,
        "scan_contract_id": TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID,
        "scan_proof_canonical_json": generation.scan_proof_canonical_json,
        "scan_proof_digest": generation.scan_proof_digest,
        "source_count": counts.source_count,
        "source_dataset_count": counts.dataset_count,
        "source_relation_count": len(vector.input_relations),
        "token_policy_count": counts.token_policy_count,
        "lookup_digest": generation.lookup_digest,
        "organization_count": counts.organization_count,
        "matched_organization_count": generation.matched_organization_count,
        "evidence_count": counts.evidence_row_count,
        "forward_row_count": counts.forward_row_count,
        "reverse_row_count": counts.reverse_row_count,
        "npi_edge_count": counts.npi_edge_count,
    }


def table(schema: str, table_name: str) -> str:
    """Return one qualified table from prevalidated static identifiers."""

    return f'"{schema}"."{table_name}"'
