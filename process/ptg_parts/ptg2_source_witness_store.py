# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL persistence for immutable PTG V3 source witnesses."""

from __future__ import annotations

import hashlib
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
    PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_PART_COUNT,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    PTG2_V3_SOURCE_WITNESS_PAYLOAD_CONTRACT,
    PTG2_V3_SOURCE_WITNESS_SELECTION,
    SourceWitnessPublication,
)
from process.ptg_parts.ptg2_source_witness_primitives import nonnegative_int


def split_source_witness_payload(witness_payload: bytes) -> tuple[bytes, ...]:
    """Split one authenticated logical payload into bounded database parts."""

    payload = bytes(witness_payload)
    if not payload or len(payload) > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise RuntimeError("strict V3 source witness payload size is invalid")
    payload_parts = tuple(
        payload[offset : offset + PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES]
        for offset in range(0, len(payload), PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES)
    )
    if not payload_parts or len(payload_parts) > PTG2_V3_SOURCE_WITNESS_MAX_PART_COUNT:
        raise RuntimeError("strict V3 source witness part count is invalid")
    return payload_parts


def assemble_source_witness_payload(
    payload_part_rows: Sequence[Mapping[str, Any] | Any],
    *,
    expected_payload_sha256: bytes | str,
) -> bytes:
    """Authenticate and join one legacy or segmented database payload."""

    normalized_part_rows = _normalize_payload_part_rows(payload_part_rows)
    witness_parts = _authenticate_payload_parts(normalized_part_rows)
    witness_payload = b"".join(witness_parts)
    if len(witness_payload) > PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES:
        raise RuntimeError("strict V3 source witness payload exceeds its bound")
    expected_digest = (
        bytes.fromhex(expected_payload_sha256)
        if isinstance(expected_payload_sha256, str)
        else bytes(expected_payload_sha256)
    )
    if (
        len(expected_digest) != 32
        or hashlib.sha256(witness_payload).digest() != expected_digest
    ):
        raise RuntimeError("strict V3 source witness payload digest is invalid")
    return witness_payload


def _normalize_payload_part_rows(
    payload_part_rows: Sequence[Mapping[str, Any] | Any],
) -> tuple[dict[str, Any], ...]:
    normalized_part_rows = tuple(
        dict(getattr(payload_part_row, "_mapping", payload_part_row))
        for payload_part_row in payload_part_rows
    )
    if (
        not normalized_part_rows
        or len(normalized_part_rows) > PTG2_V3_SOURCE_WITNESS_MAX_PART_COUNT
    ):
        raise RuntimeError("strict V3 source witness part count is invalid")
    expected_part_numbers = list(range(len(normalized_part_rows)))
    observed_part_numbers = [
        int(payload_part_row.get("part_number", -1))
        for payload_part_row in normalized_part_rows
    ]
    if observed_part_numbers != expected_part_numbers:
        raise RuntimeError("strict V3 source witness part order is invalid")
    return normalized_part_rows


def _authenticate_payload_parts(
    normalized_part_rows: Sequence[Mapping[str, Any]],
) -> tuple[bytes, ...]:
    has_segmented_payload = len(normalized_part_rows) > 1
    authenticated_parts: list[bytes] = []
    for payload_part_index, payload_part_row in enumerate(normalized_part_rows):
        payload_part = bytes(payload_part_row.get("payload") or b"")
        if not payload_part:
            raise RuntimeError("strict V3 source witness part is empty")
        if (
            has_segmented_payload
            and len(payload_part) > PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES
        ):
            raise RuntimeError("strict V3 source witness part exceeds its bound")
        if (
            has_segmented_payload
            and payload_part_index < len(normalized_part_rows) - 1
            and len(payload_part) != PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES
        ):
            raise RuntimeError("strict V3 source witness part framing is invalid")
        part_sha256 = payload_part_row.get("part_sha256")
        if payload_part_index > 0 and (
            not part_sha256
            or hashlib.sha256(payload_part).digest() != bytes(part_sha256)
        ):
            raise RuntimeError("strict V3 source witness part digest is invalid")
        authenticated_parts.append(payload_part)
    return tuple(authenticated_parts)


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
    payload_part_rows = await db.all(
        f"""
        SELECT part_number, payload, part_sha256
          FROM {schema}.ptg2_v3_source_audit_witness_part
         WHERE snapshot_key = :snapshot_key
         ORDER BY part_number
        """,
        snapshot_key=int(snapshot_key),
    )
    witness_payload = assemble_source_witness_payload(
        [
            {"part_number": 0, "payload": row_mapping.get("payload")},
            *payload_part_rows,
        ],
        expected_payload_sha256=bytes(row_mapping.get("payload_sha256") or b""),
    )
    loaded_witness = decode_persisted_source_witness(
        witness_payload,
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
    """Replace one witness and all of its parts inside the caller transaction."""

    payload_parts = split_source_witness_payload(witness_payload)
    await _insert_source_witness_parent(
        schema=schema,
        snapshot_key=snapshot_key,
        witness_metadata=witness_metadata,
        first_payload_part=payload_parts[0],
        session=session,
    )
    await _insert_source_witness_tail_parts(
        schema=schema,
        snapshot_key=snapshot_key,
        tail_payload_parts=payload_parts[1:],
        session=session,
    )


async def _insert_source_witness_parent(
    *,
    schema: str,
    snapshot_key: int,
    witness_metadata: Mapping[str, Any],
    first_payload_part: bytes,
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
            "payload": first_payload_part,
        },
    )


async def _insert_source_witness_tail_parts(
    *,
    schema: str,
    snapshot_key: int,
    tail_payload_parts: Sequence[bytes],
    session: Any,
) -> None:
    for part_number, payload_part in enumerate(tail_payload_parts, start=1):
        await session.execute(
            db.text(
                f"""
                INSERT INTO {schema}.ptg2_v3_source_audit_witness_part
                    (snapshot_key, part_number, part_sha256, payload)
                VALUES
                    (:snapshot_key, :part_number, :part_sha256, :payload)
                """
            ),
            {
                "snapshot_key": snapshot_key,
                "part_number": part_number,
                "part_sha256": hashlib.sha256(payload_part).digest(),
                "payload": payload_part,
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


__all__ = [
    "assemble_source_witness_payload",
    "load_shared_source_witness",
    "publish_shared_source_witness",
    "split_source_witness_payload",
]
