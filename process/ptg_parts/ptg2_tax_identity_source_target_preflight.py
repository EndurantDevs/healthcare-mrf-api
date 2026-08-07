# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate durable target state for source-local publication."""

from __future__ import annotations

from typing import Any

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_aggregate_reuse import (
    normalize_source_ordinal_entries,
)
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    PreparedTaxIdentitySourceProjection,
    _fail,
    _strict_int,
)

_AGGREGATE_MANIFEST_CONTRACT = "ptg2_provider_group_tax_identity_v1"
_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"


def _logical_snapshot_id(value: object) -> str:
    if not isinstance(value, str):
        raise _fail()
    normalized = value.strip()
    if not normalized or len(normalized) > 96:
        raise _fail()
    return normalized


def _expected_source_values(
    prepared: PreparedTaxIdentitySourceProjection,
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            binding.source_key,
            binding.source_type,
            binding.identity_kind,
            binding.identity_sha256,
        )
        for binding in prepared.bindings
    )


async def lock_tax_identity_source_target_vector(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: object,
    prepared: PreparedTaxIdentitySourceProjection,
) -> str:
    """Short-lock the exact final source vector until caller commit."""

    normalized_snapshot_id = _logical_snapshot_id(logical_snapshot_id)
    schema = _quote_ident(schema_name)
    locked_target = (
        await session.execute(
            db.text(f"""
                SELECT scope.snapshot_id, snapshot.status
                  FROM {schema}.ptg2_v3_snapshot_scope AS scope
                  JOIN {schema}.ptg2_snapshot AS snapshot
                    ON snapshot.snapshot_id = scope.snapshot_id
                 WHERE scope.snapshot_id = :snapshot_id
                   AND snapshot.status = 'building'
                 FOR UPDATE OF snapshot, scope NOWAIT
                """),
            {"snapshot_id": normalized_snapshot_id},
        )
    ).one_or_none()
    if locked_target is None or tuple(locked_target) != (
        normalized_snapshot_id,
        "building",
    ):
        raise _fail()
    source_rows = (
        await session.execute(
            db.text(f"""
                SELECT source_key, source_type, identity_kind, identity_sha256
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY source_key
                   FOR UPDATE NOWAIT
                """),
            {"snapshot_id": normalized_snapshot_id},
        )
    ).all()
    if tuple(tuple(source_row) for source_row in source_rows) != (
        _expected_source_values(prepared)
    ):
        raise _fail()
    return normalized_snapshot_id


async def validate_tax_identity_source_target_aggregate(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    prepared: PreparedTaxIdentitySourceProjection,
    provider_group_count: int,
    lock_for_update: bool = False,
) -> None:
    """Bind source-local evidence to the exact aggregate manifest."""

    schema = _quote_ident(schema_name)
    lock_clause = "FOR UPDATE NOWAIT" if lock_for_update else ""
    aggregate_values = (
        await session.execute(
            db.text(f"""
                SELECT contract, token_policy_id,
                       token_policy_descriptor_sha256,
                       normalization_contract, hmac_contract,
                       source_ordinal_contract, source_ordinal_map,
                       source_ordinal_map_digest, source_shard_count,
                       provider_group_count, content_digest
                  FROM {schema}.ptg2_provider_tax_identity_manifest
                 WHERE snapshot_key = :snapshot_key
                   {lock_clause}
                """),
            {"snapshot_key": _strict_int(snapshot_key)},
        )
    ).one_or_none()
    if aggregate_values is None:
        raise _fail()
    expected_source_map_entries = [
        {"shard_id": binding.source_shard_id, "ordinal": binding.source_ordinal}
        for binding in sorted(
            prepared.bindings,
            key=lambda binding: binding.source_ordinal,
        )
    ]
    expected_values = (
        _AGGREGATE_MANIFEST_CONTRACT,
        prepared.token_policy_id,
        prepared.token_policy_descriptor_sha256,
        _NORMALIZATION_CONTRACT,
        _HMAC_CONTRACT,
        _SOURCE_ORDINAL_CONTRACT,
        prepared.source_ordinal_map_digest,
        prepared.source_count,
        provider_group_count,
        prepared.aggregate_tax_content_digest,
    )
    observed_values = (
        *tuple(aggregate_values[:6]),
        *tuple(aggregate_values[7:]),
    )
    if (
        observed_values != expected_values
        or normalize_source_ordinal_entries(aggregate_values[6])
        != expected_source_map_entries
    ):
        raise _fail()


async def validate_tax_identity_source_target_sources(
    session: Any,
    *,
    schema_name: str,
    logical_snapshot_id: object,
    prepared: PreparedTaxIdentitySourceProjection,
) -> None:
    """Read and compare the ordered logical source vector before writes."""

    schema = _quote_ident(schema_name)
    normalized_snapshot_id = _logical_snapshot_id(logical_snapshot_id)
    target_status = (
        await session.execute(
            db.text(f"""
                SELECT scope.snapshot_id, snapshot.status
                  FROM {schema}.ptg2_v3_snapshot_scope AS scope
                  JOIN {schema}.ptg2_snapshot AS snapshot
                    ON snapshot.snapshot_id = scope.snapshot_id
                 WHERE scope.snapshot_id = :snapshot_id
                """),
            {"snapshot_id": normalized_snapshot_id},
        )
    ).one_or_none()
    if target_status is None or tuple(target_status) != (
        normalized_snapshot_id,
        "building",
    ):
        raise _fail()
    source_rows = (
        await session.execute(
            db.text(f"""
                SELECT source_key, source_type, identity_kind, identity_sha256
                  FROM {schema}.ptg2_v3_snapshot_source
                 WHERE snapshot_id = :snapshot_id
                 ORDER BY source_key
                """),
            {"snapshot_id": normalized_snapshot_id},
        )
    ).all()
    if tuple(tuple(source_row) for source_row in source_rows) != (
        _expected_source_values(prepared)
    ):
        raise _fail()


__all__ = [
    "lock_tax_identity_source_target_vector",
    "validate_tax_identity_source_target_aggregate",
    "validate_tax_identity_source_target_sources",
]
