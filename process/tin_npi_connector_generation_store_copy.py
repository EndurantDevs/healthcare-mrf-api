# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded binary-COPY rows for one connector generation."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from process.tin_npi_connector_publication import ConnectorPublicationBundle
from process.tin_npi_connector_generation_store_types import (
    ConnectorGenerationStoreConnection,
    TinNpiConnectorGenerationStoreError,
)

_GENERATION_POLICY_COLUMNS = ("generation_key", "token_policy_id")
_LOOKUP_COLUMNS = (
    "generation_key",
    "token_policy_id",
    "tin_id_128",
    "tin_hmac_sha256",
    "npis",
    "evidence_count",
    "source_bitmap",
    "npi_source_bitmap_matrix",
    "source_evidence_counts",
)
_EVIDENCE_COLUMNS = (
    "generation_key",
    "evidence_id",
    "token_policy_id",
    "tin_id_128",
    "tin_hmac_sha256",
    "npi",
    "source_ordinal",
    "relationship_class",
    "source_record_hmac_sha256",
    "source_record_identity_sha256",
    "source_record_payload_sha256",
    "identifier_policy_sha256",
    "identifier_rule_id",
    "identifier_rule_sha256",
)


async def copy_generation_children(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    generation_key: int,
    bundle: ConnectorPublicationBundle,
    batch_size: int,
) -> None:
    """COPY policies, forward lookups, and evidence in foreign-key order."""

    generation = bundle.generation
    await _copy_batches(
        connection,
        schema,
        "tin_npi_connector_generation_policy",
        _GENERATION_POLICY_COLUMNS,
        tuple(sorted(bundle.source_vector.token_policy_ids)),
        lambda policy_id: (generation_key, policy_id),
        batch_size,
    )
    await _copy_batches(
        connection,
        schema,
        "tin_npi_connector_lookup",
        _LOOKUP_COLUMNS,
        generation.forward_rows,
        lambda row: _lookup_record(generation_key, row),
        batch_size,
    )
    source_ordinal_by_id = {
        source_id: ordinal
        for ordinal, source_id in enumerate(generation.source_ordinal_map)
    }
    await _copy_batches(
        connection,
        schema,
        "tin_npi_connector_evidence",
        _EVIDENCE_COLUMNS,
        generation.evidence_rows,
        lambda row: _evidence_record(
            generation_key,
            row,
            source_ordinal_by_id,
        ),
        batch_size,
    )


def _lookup_record(
    generation_key: int,
    row,
) -> tuple[object, ...]:
    return (
        generation_key,
        row.token.token_policy_id,
        row.token.tin_id_128,
        row.token.tin_hmac_sha256,
        list(row.npis),
        row.evidence_count,
        row.source_bitmap,
        row.npi_source_bitmap_matrix,
        list(row.source_evidence_counts),
    )


def _evidence_record(
    generation_key: int,
    row,
    source_ordinal_by_id: dict[str, int],
) -> tuple[object, ...]:
    return (
        generation_key,
        row.evidence_id,
        row.token.token_policy_id,
        row.token.tin_id_128,
        row.token.tin_hmac_sha256,
        row.npi,
        source_ordinal_by_id[row.source_id],
        row.relationship_class,
        row.source_record_hmac_sha256,
        row.source_record_identity_sha256,
        bytes.fromhex(row.source_record_payload_hash),
        bytes.fromhex(row.identifier_policy_sha256),
        row.identifier_rule_id,
        bytes.fromhex(row.identifier_rule_sha256),
    )


async def _copy_batches(
    connection: ConnectorGenerationStoreConnection,
    schema: str,
    table_name: str,
    columns: tuple[str, ...],
    source_rows: Sequence[object],
    record_builder: Callable[[object], tuple[object, ...]],
    batch_size: int,
) -> None:
    for batch_start in range(0, len(source_rows), batch_size):
        source_batch = source_rows[batch_start : batch_start + batch_size]
        records = tuple(record_builder(row) for row in source_batch)
        status = await connection.copy_records_to_table(
            table_name,
            schema_name=schema,
            columns=columns,
            records=records,
        )
        if status != f"COPY {len(records)}":
            raise TinNpiConnectorGenerationStoreError(
                "connector generation COPY count is invalid"
            )
