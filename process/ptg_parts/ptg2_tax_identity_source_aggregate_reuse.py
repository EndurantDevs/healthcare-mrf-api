# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate sealed aggregate tax-identity metadata during V4 reuse."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text

from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    _fail,
    _strict_int,
    _strict_policy,
    _strict_sha256,
)

_CONTRACT = "ptg2_provider_group_tax_identity_v1"
_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"


def normalize_source_ordinal_entries(
    raw_source_ordinal_map: object,
) -> list[object]:
    """Return one dense, sorted, schema-strict source ordinal map."""

    if isinstance(raw_source_ordinal_map, str):
        try:
            raw_source_ordinal_map = json.loads(raw_source_ordinal_map)
        except (TypeError, ValueError):
            raise _fail() from None
    if not isinstance(raw_source_ordinal_map, list):
        raise _fail()
    normalized_entries: list[object] = []
    seen_shards: set[str] = set()
    shard_ids: list[str] = []
    for expected_ordinal, entry in enumerate(raw_source_ordinal_map):
        if not isinstance(entry, Mapping) or set(entry) != {"shard_id", "ordinal"}:
            raise _fail()
        shard_id = entry.get("shard_id")
        if (
            not isinstance(shard_id, str)
            or not shard_id
            or shard_id in seen_shards
            or _strict_int(entry.get("ordinal")) != expected_ordinal
        ):
            raise _fail()
        seen_shards.add(shard_id)
        shard_ids.append(shard_id)
        normalized_entries.append({"shard_id": shard_id, "ordinal": expected_ordinal})
    if not normalized_entries or tuple(shard_ids) != tuple(sorted(shard_ids)):
        raise _fail()
    return normalized_entries


def _sealed_values(
    metadata_by_field: Mapping[str, Any],
    *,
    snapshot_key: int,
) -> tuple[object, ...]:
    if (
        _strict_int(metadata_by_field.get("snapshot_key")) != snapshot_key
        or metadata_by_field.get("contract") != _CONTRACT
        or metadata_by_field.get("normalization_contract") != _NORMALIZATION_CONTRACT
        or metadata_by_field.get("hmac_contract") != _HMAC_CONTRACT
        or metadata_by_field.get("source_ordinal_contract") != _SOURCE_ORDINAL_CONTRACT
    ):
        raise _fail()
    source_map_entries = normalize_source_ordinal_entries(
        metadata_by_field.get("source_ordinal_map")
    )
    source_shard_count = _strict_int(
        metadata_by_field.get("source_shard_count"),
        minimum=1,
    )
    if len(source_map_entries) != source_shard_count:
        raise _fail()
    return (
        _CONTRACT,
        _strict_policy(metadata_by_field.get("token_policy_id")),
        bytes.fromhex(
            _strict_sha256(metadata_by_field.get("token_policy_descriptor_sha256"))
        ),
        _NORMALIZATION_CONTRACT,
        _HMAC_CONTRACT,
        _SOURCE_ORDINAL_CONTRACT,
        source_map_entries,
        bytes.fromhex(
            _strict_sha256(metadata_by_field.get("source_ordinal_map_digest"))
        ),
        source_shard_count,
        _strict_int(metadata_by_field.get("provider_group_count")),
        _strict_int(metadata_by_field.get("tax_identity_count")),
        _strict_int(metadata_by_field.get("matched_ein_count")),
        _strict_int(metadata_by_field.get("missing_count")),
        _strict_int(metadata_by_field.get("malformed_count")),
        _strict_int(metadata_by_field.get("unsupported_type_count")),
        bytes.fromhex(_strict_sha256(metadata_by_field.get("content_digest"))),
    )


async def validate_reused_tax_identity_aggregate_manifest(
    session: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    sealed_metadata: Mapping[str, Any],
) -> None:
    """Require the complete durable aggregate manifest and source map."""

    normalized_snapshot_key = _strict_int(snapshot_key)
    schema = _quote_ident(schema_name)
    stored_values = (
        await session.execute(
            text(f"""
                SELECT contract, token_policy_id,
                       token_policy_descriptor_sha256,
                       normalization_contract, hmac_contract,
                       source_ordinal_contract, source_ordinal_map,
                       source_ordinal_map_digest, source_shard_count,
                       provider_group_count, tax_identity_count,
                       matched_ein_count, missing_count, malformed_count,
                       unsupported_type_count, content_digest
                  FROM {schema}.ptg2_provider_tax_identity_manifest
                 WHERE snapshot_key = :snapshot_key
                """),
            {"snapshot_key": normalized_snapshot_key},
        )
    ).one_or_none()
    if stored_values is None:
        raise _fail()
    normalized_stored_values = (
        *tuple(stored_values[:6]),
        normalize_source_ordinal_entries(stored_values[6]),
        *tuple(stored_values[7:]),
    )
    if normalized_stored_values != _sealed_values(
        sealed_metadata,
        snapshot_key=normalized_snapshot_key,
    ):
        raise _fail()


__all__ = [
    "normalize_source_ordinal_entries",
    "validate_reused_tax_identity_aggregate_manifest",
]
