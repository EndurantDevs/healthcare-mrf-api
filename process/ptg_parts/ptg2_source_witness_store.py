# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL persistence for immutable PTG V3 source witnesses."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    SharedLayoutBuildOwnership,
    lock_shared_layout_for_dense_write,
    shared_support_digest,
)
from process.ptg_parts.ptg2_source_witness import (
    build_persisted_source_witness,
    decode_persisted_source_witness,
)
from process.ptg_parts.ptg2_source_witness_contract import (
    LoadedSourceWitness,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    SourceWitnessPublication,
)
from process.ptg_parts.ptg2_source_witness_primitives import nonnegative_int


def _database_metadata_by_field(row_mapping: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "contract": str(row_mapping.get("contract") or ""),
        "selection_method": str(row_mapping.get("selection_method") or ""),
        "source_set_digest": bytes(row_mapping.get("source_set_digest") or b"").hex(),
        "sample_digest": bytes(row_mapping.get("sample_digest") or b"").hex(),
        "queryable_occurrence_population_count": nonnegative_int(
            row_mapping,
            "queryable_occurrence_population_count",
            error_field_name="database occurrence population",
        ),
        "provider_population_count": nonnegative_int(
            row_mapping,
            "provider_population_count",
            error_field_name="database provider population",
        ),
        "occurrence_witness_count": nonnegative_int(
            row_mapping,
            "occurrence_witness_count",
            error_field_name="database occurrence witness count",
        ),
        "provider_witness_count": nonnegative_int(
            row_mapping,
            "provider_witness_count",
            error_field_name="database provider witness count",
        ),
        "payload_sha256": bytes(row_mapping.get("payload_sha256") or b"").hex(),
    }


async def load_shared_source_witness(
    *,
    schema_name: str,
    snapshot_key: int,
    expected_raw_source_sha256: Sequence[str],
    expected_metadata: Mapping[str, Any],
) -> LoadedSourceWitness:
    """Load and cross-check one immutable witness row from PostgreSQL."""

    schema = _quote_ident(schema_name)
    database_row = await db.first(
        f"""
        SELECT contract, selection_method, source_set_digest, sample_digest,
               queryable_occurrence_population_count, provider_population_count,
               occurrence_witness_count, provider_witness_count, payload_sha256,
               payload
          FROM {schema}.ptg2_v3_source_audit_witness
         WHERE snapshot_key = :snapshot_key
        """,
        snapshot_key=int(snapshot_key),
    )
    if database_row is None:
        raise RuntimeError("strict V3 layout has no persisted source witness")
    row_mapping = dict(getattr(database_row, "_mapping", database_row))
    loaded_witness = decode_persisted_source_witness(
        bytes(row_mapping.get("payload") or b""),
        expected_raw_source_sha256=expected_raw_source_sha256,
        expected_metadata=expected_metadata,
    )
    database_metadata_by_field = _database_metadata_by_field(row_mapping)
    if any(
        loaded_witness.metadata.get(field_name) != field_value
        for field_name, field_value in database_metadata_by_field.items()
    ):
        raise RuntimeError("strict V3 persisted source witness row is inconsistent")
    return loaded_witness


async def _replace_source_witness_row(
    *,
    schema: str,
    snapshot_key: int,
    witness_payload: bytes,
    witness_metadata: Mapping[str, Any],
    session: Any,
) -> None:
    await session.execute(
        db.text(
            f"DELETE FROM {schema}.ptg2_v3_source_audit_witness "
            "WHERE snapshot_key = :snapshot_key"
        ),
        {"snapshot_key": snapshot_key},
    )
    await session.execute(
        db.text(
            f"""
            INSERT INTO {schema}.ptg2_v3_source_audit_witness
                (snapshot_key, contract, selection_method, source_set_digest,
                 sample_digest, queryable_occurrence_population_count,
                 provider_population_count, occurrence_witness_count,
                 provider_witness_count, payload_sha256, payload)
            VALUES
                (:snapshot_key, :contract, :selection_method,
                 :source_set_digest, :sample_digest, :occurrence_population,
                 :provider_population, :occurrence_count,
                 :provider_count, :payload_sha256, :payload)
            """
        ),
        {
            "snapshot_key": snapshot_key,
            "contract": PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
            "selection_method": PTG2_V3_SOURCE_WITNESS_SELECTION,
            "source_set_digest": bytes.fromhex(str(witness_metadata["source_set_digest"])),
            "sample_digest": bytes.fromhex(str(witness_metadata["sample_digest"])),
            "occurrence_population": int(
                witness_metadata["queryable_occurrence_population_count"]
            ),
            "provider_population": int(witness_metadata["provider_population_count"]),
            "occurrence_count": int(witness_metadata["occurrence_witness_count"]),
            "provider_count": int(witness_metadata["provider_witness_count"]),
            "payload_sha256": bytes.fromhex(str(witness_metadata["payload_sha256"])),
            "payload": witness_payload,
        },
    )


async def publish_shared_source_witness(
    *,
    schema_name: str,
    build_ownership: SharedLayoutBuildOwnership,
    entries: Sequence[Mapping[str, Any]],
    expected_raw_source_sha256: Sequence[str],
) -> SourceWitnessPublication:
    """Publish one source witness while holding the immutable layout build lease."""

    witness_payload, witness_metadata = build_persisted_source_witness(
        entries,
        expected_raw_source_sha256=expected_raw_source_sha256,
    )
    schema = _quote_ident(schema_name)
    snapshot_key = int(build_ownership.snapshot_key)
    async with db.transaction() as session:
        await lock_shared_layout_for_dense_write(
            session,
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=str(build_ownership.build_token),
        )
        await _replace_source_witness_row(
            schema=schema,
            snapshot_key=snapshot_key,
            witness_payload=witness_payload,
            witness_metadata=witness_metadata,
            session=session,
        )
    return SourceWitnessPublication(
        metadata=witness_metadata,
        support_digest=shared_support_digest({"source_witness": witness_metadata}),
        row_count=int(witness_metadata["record_count"]),
        stored_byte_count=len(witness_payload),
    )


__all__ = ["load_shared_source_witness", "publish_shared_source_witness"]
