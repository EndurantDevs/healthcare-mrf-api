# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lifecycle cleanup for immutable PTG V3 shared layouts and blocks."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_CLEANUP_GENERATIONS,
    PTG2_V3_DENSE_LAYOUT_TABLES,
    PTG2_V3_SHARED_FORMAT_VERSION,
    PTG2_V3_SHARED_GENERATION,
    shared_block_hash,
)
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_lifecycle_lock
from process.ptg_parts.ptg2_schema import resolve_ptg2_schema
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_SHARED_GENERATION,
    decode_v4_snapshot_map_pack,
    v4_layout_advisory_lock_key,
)


PTG2_V3_BUILDING_MAX_AGE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BUILDING_MAX_AGE_SECONDS"
PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_GRACE_SECONDS"
PTG2_V3_BLOCK_GC_MAX_BYTES_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_MAX_BYTES"
PTG2_V3_LAYOUT_GC_BATCH_ROWS_ENV = "HLTHPRT_PTG2_V3_LAYOUT_GC_BATCH_ROWS"
PTG2_V3_BLOCK_GC_MAX_ROWS_ENV = "HLTHPRT_PTG2_V3_BLOCK_GC_MAX_ROWS"
PTG2_V4_ABANDONMENT_BATCH_ROWS_ENV = "HLTHPRT_PTG2_V4_ABANDONMENT_BATCH_ROWS"
PTG2_V4_ABANDONMENT_TIMEOUT_SECONDS_ENV = (
    "HLTHPRT_PTG2_V4_ABANDONMENT_TIMEOUT_SECONDS"
)
PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_ENV = (
    "HLTHPRT_PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS"
)

PTG2_V3_BUILDING_MAX_AGE_SECONDS_DEFAULT = 21_600
PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT = 21_600
PTG2_V3_BLOCK_GC_MAX_BYTES_DEFAULT = 80 * 1024 * 1024 * 1024
PTG2_V3_LAYOUT_GC_BATCH_ROWS_DEFAULT = 400
PTG2_V3_BLOCK_GC_MAX_ROWS_DEFAULT = 1_000
PTG2_V4_GC_MAP_PACK_BATCH_ROWS = 128
PTG2_V4_ABANDONMENT_BATCH_ROWS_DEFAULT = 4_096
PTG2_V4_ABANDONMENT_TIMEOUT_SECONDS_DEFAULT = 30.0
PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_DEFAULT = 5.0
PTG2_V4_ABANDONMENT_MAX_BATCH_ROWS = 65_536
PTG2_V4_ABANDONMENT_TOKEN_DOMAIN = b"PTG2V4ABANDON\x01"
PTG2_V4_GC_TABLE_NAMES = (
    "ptg2_v4_snapshot_map_root",
    "ptg2_v4_snapshot_map_pack",
)
PTG2_PROVIDER_TAX_IDENTITY_BASE_TABLE_NAMES = (
    "ptg2_provider_tax_identity_legacy_layout",
    "ptg2_provider_tax_identity_manifest",
    "ptg2_provider_tax_identity",
    "ptg2_provider_group_tax_identity",
)
PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES = (
    "ptg2_provider_tax_identity_source_manifest",
    "ptg2_provider_tax_identity_source_binding",
    "ptg2_provider_group_tax_identity_source",
)
PTG2_PROVIDER_TAX_IDENTITY_TABLE_NAMES = (
    PTG2_PROVIDER_TAX_IDENTITY_BASE_TABLE_NAMES
    + PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES
)

PTG2_V3_MIGRATION_OWNED_TABLE_NAMES = (
    "ptg2_v3_snapshot_layout",
    "ptg2_v3_layout_fingerprint",
    "ptg2_layout_build_candidate",
    "ptg2_block_build_pin",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_snapshot_source",
    "ptg2_v3_block",
    "ptg2_v3_snapshot_block",
    "ptg2_v3_graph_owner",
    "ptg2_v3_code",
    "ptg2_v3_provider_group",
    "ptg2_v3_provider_set",
    "ptg2_v3_price_attr",
    "ptg2_v3_npi_scope",
    "ptg2_v3_audit_occurrence",
    "ptg2_v3_source_audit_witness_part",
    "ptg2_v3_source_audit_witness",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_v3_gc_candidate",
)

# Compatibility for cleanup callers and tests that predate explicit ownership.
_SHARED_TABLE_NAMES = PTG2_V3_MIGRATION_OWNED_TABLE_NAMES


@dataclass(frozen=True)
class PTG2SharedLayoutGCStats:
    logical_layout_count: int = 0
    candidate_hash_count: int = 0
    stored_bytes: int = 0
    tables_available: bool = True


class PTG2SharedLayoutAbandonmentDeferred(RuntimeError):
    """Bounded failed-layout cleanup stopped safely for a later retry."""


@dataclass(frozen=True)
class PTG2V4AbandonmentOptions:
    """Bound testable execution controls for failed V4 layout cleanup."""

    batch_rows: int | None = None
    timeout_seconds: float | None = None
    statement_timeout_seconds: float | None = None
    monotonic: Callable[[], float] = time.monotonic


@dataclass(frozen=True)
class _OwnedV4AbandonmentContext:
    schema_name: str
    snapshot_key: int
    build_token: str
    batch_rows: int
    deadline: float
    statement_timeout_seconds: float
    monotonic: Callable[[], float]
    grace_seconds: int


@dataclass(frozen=True)
class _OwnedV4AbandonmentInventory:
    block_hashes: tuple[bytes, ...]
    stored_bytes: int
    abandonment_token: str


@dataclass(frozen=True)
class PTG2SharedBlockSweepPlan:
    selected_hashes: tuple[bytes, ...] = ()
    stored_bytes: int = 0
    tables_available: bool = True

    @property
    def selected_hash_count(self) -> int:
        """Return the number of block hashes selected for deletion."""

        return len(self.selected_hashes)


@dataclass(frozen=True)
class PTG2SharedGCPlan:
    layouts: PTG2SharedLayoutGCStats
    sweep: PTG2SharedBlockSweepPlan

    @property
    def logical_layout_count(self) -> int:
        """Return the number of layouts released by the layout phase."""

        return self.layouts.logical_layout_count

    @property
    def candidate_hash_count(self) -> int:
        """Return the number of block hashes queued by layout release."""

        return self.layouts.candidate_hash_count

    @property
    def stored_bytes(self) -> int:
        """Return stored bytes referenced by the released layouts."""

        return self.layouts.stored_bytes

    @property
    def selected_hashes(self) -> tuple[bytes, ...]:
        """Return block hashes selected by the sweep phase."""

        return self.sweep.selected_hashes

    @property
    def selected_hash_count(self) -> int:
        """Return the number of block hashes selected by the sweep phase."""

        return self.sweep.selected_hash_count


def _row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return row
    return dict(getattr(row, "_mapping", row))


async def missing_migration_owned_tables(
    executor: Any,
    schema_name: str,
) -> tuple[str, ...]:
    """Return strict V3 tables that must have been created by Alembic."""

    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=list(PTG2_V3_MIGRATION_OWNED_TABLE_NAMES),
    )
    present_table_names = {
        str(_row_mapping(table_record).get("table_name") or "")
        for table_record in table_records
        if _row_mapping(table_record).get("table_name")
    }
    return tuple(
        sorted(set(PTG2_V3_MIGRATION_OWNED_TABLE_NAMES) - present_table_names)
    )


async def require_migration_owned_tables(
    executor: Any,
    schema_name: str,
) -> None:
    """Fail before runtime DDL or cleanup when the V3 migration is absent."""

    missing = await missing_migration_owned_tables(
        executor,
        schema_name,
    )
    if missing:
        raise RuntimeError(
            "PTG V3 migration-owned tables are missing from schema "
            f"{schema_name}: {', '.join(missing)}; run alembic upgrade head"
        )


def _env_non_negative_int(name: str, default: int) -> int:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(int(str(raw_value).strip()), 0)
    except ValueError:
        return default


def _env_positive_int(name: str, default: int) -> int:
    return max(_env_non_negative_int(name, default), 1)


def _env_positive_float(name: str, default: float) -> float:
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        return max(float(str(raw_value).strip()), 0.001)
    except ValueError:
        return default


def _building_max_age_seconds(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BUILDING_MAX_AGE_SECONDS_ENV,
        PTG2_V3_BUILDING_MAX_AGE_SECONDS_DEFAULT,
    )


def _block_gc_grace_seconds(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_ENV,
        PTG2_V3_BLOCK_GC_GRACE_SECONDS_DEFAULT,
    )


def _block_gc_max_bytes(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 0)
    return _env_non_negative_int(
        PTG2_V3_BLOCK_GC_MAX_BYTES_ENV,
        PTG2_V3_BLOCK_GC_MAX_BYTES_DEFAULT,
    )


def _layout_batch_rows(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 1)
    return _env_positive_int(
        PTG2_V3_LAYOUT_GC_BATCH_ROWS_ENV,
        PTG2_V3_LAYOUT_GC_BATCH_ROWS_DEFAULT,
    )


def _block_gc_max_rows(value: int | None = None) -> int:
    if value is not None:
        return max(int(value), 1)
    return _env_positive_int(
        PTG2_V3_BLOCK_GC_MAX_ROWS_ENV,
        PTG2_V3_BLOCK_GC_MAX_ROWS_DEFAULT,
    )


def _v4_abandonment_batch_rows(value: int | None = None) -> int:
    configured = (
        int(value)
        if value is not None
        else _env_positive_int(
            PTG2_V4_ABANDONMENT_BATCH_ROWS_ENV,
            PTG2_V4_ABANDONMENT_BATCH_ROWS_DEFAULT,
        )
    )
    return min(max(configured, 1), PTG2_V4_ABANDONMENT_MAX_BATCH_ROWS)


def _v4_abandonment_timeout_seconds(value: float | None = None) -> float:
    if value is not None:
        return max(float(value), 0.001)
    return _env_positive_float(
        PTG2_V4_ABANDONMENT_TIMEOUT_SECONDS_ENV,
        PTG2_V4_ABANDONMENT_TIMEOUT_SECONDS_DEFAULT,
    )


def _v4_abandonment_statement_timeout_seconds(
    value: float | None = None,
) -> float:
    if value is not None:
        return max(float(value), 0.001)
    return _env_positive_float(
        PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_ENV,
        PTG2_V4_ABANDONMENT_STATEMENT_TIMEOUT_SECONDS_DEFAULT,
    )


def is_shared_blocks_cleanup_manifest(serving_index: dict[str, Any] | None) -> bool:
    """Return whether a serving index names a shared-block cleanup generation."""

    if not isinstance(serving_index, dict):
        return False
    return any(
        str(serving_index.get(field_name) or "").strip().lower()
        in PTG2_V3_CLEANUP_GENERATIONS
        for field_name in ("storage_generation", "generation", "storage")
    )


def is_shared_blocks_v1_manifest(serving_index: dict[str, Any] | None) -> bool:
    """Compatibility name used only by cleanup callers; V1 is never serveable."""

    return is_shared_blocks_cleanup_manifest(serving_index)


async def _has_shared_manifest(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_snapshot
             WHERE lower(COALESCE(manifest->'serving_index'->>'storage_generation', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
                OR lower(COALESCE(manifest->'serving_index'->>'generation', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
                OR lower(COALESCE(manifest->'serving_index'->>'storage', ''))
                       = ANY(CAST(:cleanup_generations AS text[]))
        ) AS involved
        """,
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_binding(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_binding
        ) AS involved
        """
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_scope(executor: Any, schema_name: str) -> bool:
    existence_records = await executor.all(
        f"""
        SELECT EXISTS (
            SELECT 1
              FROM {_quote_ident(schema_name)}.ptg2_v3_snapshot_scope
        ) AS involved
        """
    )
    return (
        bool(_row_mapping(existence_records[0]).get("involved"))
        if existence_records
        else False
    )


async def _has_shared_tables(
    executor: Any,
    schema_name: str,
    *,
    require_shared: bool,
) -> bool:
    await _has_provider_tax_identity_tables(executor, schema_name)
    table_names = ["ptg2_snapshot", *_SHARED_TABLE_NAMES]
    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=table_names,
    )
    present_table_names = {
        str(_row_mapping(table_record).get("table_name") or "")
        for table_record in table_records
        if _row_mapping(table_record).get("table_name")
    }
    missing = sorted(set(_SHARED_TABLE_NAMES) - present_table_names)
    if not missing:
        return True

    manifest_involved = require_shared
    if not manifest_involved and "ptg2_snapshot" in present_table_names:
        manifest_involved = await _has_shared_manifest(executor, schema_name)
    has_shared_binding = False
    if "ptg2_v3_snapshot_binding" in present_table_names:
        has_shared_binding = await _has_shared_binding(executor, schema_name)
    has_shared_scope = False
    if "ptg2_v3_snapshot_scope" in present_table_names:
        has_shared_scope = await _has_shared_scope(executor, schema_name)
    if manifest_involved or has_shared_binding or has_shared_scope:
        raise RuntimeError(
            "shared-block cleanup requires the complete shared schema; "
            f"missing tables: {', '.join(missing)}"
        )
    return False


async def _has_provider_tax_identity_tables(
    executor: Any,
    schema_name: str,
) -> bool:
    """Reject partial installation of the additive shared tax sidecar."""

    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=list(PTG2_PROVIDER_TAX_IDENTITY_TABLE_NAMES),
    )
    present_names = {
        str(_row_mapping(table_record).get("table_name") or "")
        for table_record in table_records
        if _row_mapping(table_record).get("table_name")
    }
    base_names = set(PTG2_PROVIDER_TAX_IDENTITY_BASE_TABLE_NAMES)
    source_names = set(PTG2_PROVIDER_TAX_IDENTITY_SOURCE_TABLE_NAMES)
    installed_base_names = base_names & present_names
    installed_source_names = source_names & present_names
    has_incomplete_base = bool(installed_base_names) and (
        installed_base_names != base_names
    )
    has_incomplete_source = bool(installed_source_names) and (
        installed_source_names != source_names
    )
    has_source_without_base = bool(installed_source_names) and (
        installed_base_names != base_names
    )
    if has_incomplete_base or has_incomplete_source or has_source_without_base:
        expected_names = (
            base_names | source_names if installed_source_names else base_names
        )
        missing = ", ".join(sorted(expected_names - present_names))
        raise RuntimeError(
            "PTG provider tax-identity cleanup requires the complete "
            f"additive schema; missing tables: {missing}; "
            "run alembic upgrade head"
        )
    return installed_base_names == base_names


async def _has_v4_map_tables(executor: Any, schema_name: str) -> bool:
    """Return whether the additive V4 map migration is fully installed."""

    table_records = await executor.all(
        """
        SELECT table_name
          FROM information_schema.tables
         WHERE table_schema = :schema_name
           AND table_name = ANY(CAST(:table_names AS text[]))
        """,
        schema_name=schema_name,
        table_names=list(PTG2_V4_GC_TABLE_NAMES),
    )
    present_names = {
        str(_row_mapping(record).get("table_name") or "")
        for record in table_records
        if _row_mapping(record).get("table_name")
    }
    expected_names = set(PTG2_V4_GC_TABLE_NAMES)
    installed_names = expected_names & present_names
    if installed_names and installed_names != expected_names:
        missing = ", ".join(sorted(expected_names - present_names))
        raise RuntimeError(
            "PTG V4 map GC requires the complete additive schema; "
            f"missing tables: {missing}; run alembic upgrade head"
        )
    return installed_names == expected_names


_V4_REACHABLE_PACK_SQL_TEMPLATE = """
            SELECT pack.snapshot_key, pack.object_kind, pack.pack_no,
                   pack.first_block_key, pack.first_fragment_no,
                   pack.last_block_key, pack.last_fragment_no,
                   pack.coordinate_count, pack.entry_count,
                   pack.map_block_hash,
                   block.format_version, block.object_kind AS map_object_kind,
                   block.codec, block.entry_count AS map_entry_count,
                   block.raw_byte_count, block.stored_byte_count, block.payload
              FROM {schema}.ptg2_v4_snapshot_map_pack AS pack
              JOIN {schema}.ptg2_v4_snapshot_map_root AS root
                ON root.snapshot_key = pack.snapshot_key
              JOIN {schema}.ptg2_v3_snapshot_layout AS layout
                ON layout.snapshot_key = pack.snapshot_key
              JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = pack.map_block_hash
             WHERE root.state IN ('building', 'complete')
               AND layout.generation = :v4_generation
               AND (
                    NOT :restrict_snapshot_keys
                    OR pack.snapshot_key = ANY(CAST(:snapshot_keys AS bigint[]))
               )
               AND (
                    pack.snapshot_key > :last_snapshot_key
                    OR (
                        pack.snapshot_key = :last_snapshot_key
                        AND pack.object_kind > :last_object_kind
                    )
                    OR (
                        pack.snapshot_key = :last_snapshot_key
                        AND pack.object_kind = :last_object_kind
                        AND pack.pack_no > :last_pack_no
                    )
               )
             ORDER BY pack.snapshot_key, pack.object_kind, pack.pack_no
             LIMIT :batch_rows
"""


async def _v4_map_pack_batch(
    executor: Any,
    *,
    schema_name: str,
    snapshot_keys: Sequence[int] | None,
    last_coordinate: tuple[int, str, int],
) -> Sequence[Any]:
    """Read one ordered batch of persisted V4 map packs."""

    return await executor.all(
        _V4_REACHABLE_PACK_SQL_TEMPLATE.format(
            schema=_quote_ident(schema_name)
        ),
        v4_generation=PTG2_V4_SHARED_GENERATION,
        restrict_snapshot_keys=snapshot_keys is not None,
        snapshot_keys=[
            int(snapshot_key)
            for snapshot_key in (snapshot_keys or ())
        ],
        last_snapshot_key=last_coordinate[0],
        last_object_kind=last_coordinate[1],
        last_pack_no=last_coordinate[2],
        batch_rows=PTG2_V4_GC_MAP_PACK_BATCH_ROWS,
    )


def _authenticated_v4_map_payload(
    map_pack_by_field: dict[str, Any],
) -> tuple[bytes, bytes]:
    """Validate one persisted map block and return its hash and payload."""

    map_block_hash = bytes(map_pack_by_field.get("map_block_hash") or b"")
    map_payload = bytes(map_pack_by_field.get("payload") or b"")
    if (
        len(map_block_hash) != 32
        or int(map_pack_by_field.get("format_version") or -1)
        != PTG2_V3_SHARED_FORMAT_VERSION
        or map_pack_by_field.get("map_object_kind")
        != PTG2_V4_MAP_BLOCK_KIND
        or map_pack_by_field.get("codec") != "none"
        or int(map_pack_by_field.get("raw_byte_count") or -1)
        != len(map_payload)
        or int(map_pack_by_field.get("stored_byte_count") or -1)
        != len(map_payload)
        or shared_block_hash(
            format_version=PTG2_V3_SHARED_FORMAT_VERSION,
            object_kind=PTG2_V4_MAP_BLOCK_KIND,
            codec="none",
            payload=map_payload,
        )
        != map_block_hash
    ):
        raise RuntimeError("PTG V4 GC found an unauthenticated map block")
    return map_block_hash, map_payload


def _validated_v4_map_coordinates(
    map_pack_by_field: dict[str, Any],
    *,
    object_kind: str,
    map_payload: bytes,
):
    """Decode one map pack and require its relational metadata to match."""

    map_coordinates = decode_v4_snapshot_map_pack(
        map_payload,
        expected_object_kind=object_kind,
    )
    if not map_coordinates:
        raise RuntimeError("PTG V4 GC found an empty persisted map pack")
    if (
        int(map_pack_by_field.get("coordinate_count") or -1)
        != len(map_coordinates)
        or int(map_pack_by_field.get("map_entry_count") or -1)
        != len(map_coordinates)
        or int(map_pack_by_field.get("entry_count") or -1)
        != sum(
            int(map_coordinate.entry_count)
            for map_coordinate in map_coordinates
        )
        or (map_coordinates[0].block_key, map_coordinates[0].fragment_no)
        != (
            int(map_pack_by_field.get("first_block_key")),
            int(map_pack_by_field.get("first_fragment_no")),
        )
        or (map_coordinates[-1].block_key, map_coordinates[-1].fragment_no)
        != (
            int(map_pack_by_field.get("last_block_key")),
            int(map_pack_by_field.get("last_fragment_no")),
        )
    ):
        raise RuntimeError("PTG V4 GC map metadata does not match its payload")
    return map_coordinates


def _v4_map_pack_hashes(
    raw_record: Any,
    *,
    last_coordinate: tuple[int, str, int],
) -> tuple[tuple[int, str, int], set[bytes]]:
    """Authenticate one ordered map-pack record and return reachable hashes."""

    map_pack_by_field = _row_mapping(raw_record)
    coordinate = (
        int(map_pack_by_field.get("snapshot_key")),
        str(map_pack_by_field.get("object_kind") or ""),
        int(map_pack_by_field.get("pack_no")),
    )
    if coordinate <= last_coordinate:
        raise RuntimeError("PTG V4 GC map-pack ordering is not strict")
    map_block_hash, map_payload = _authenticated_v4_map_payload(
        map_pack_by_field
    )
    map_coordinates = _validated_v4_map_coordinates(
        map_pack_by_field,
        object_kind=coordinate[1],
        map_payload=map_payload,
    )
    return coordinate, {
        map_block_hash,
        *(map_coordinate.block_hash for map_coordinate in map_coordinates),
    }


async def _v4_reachable_hashes(
    executor: Any,
    *,
    schema_name: str,
    snapshot_keys: Sequence[int] | None = None,
    candidate_hashes: Iterable[bytes] | None = None,
) -> set[bytes]:
    """Decode authenticated map packs into exact V4 CAS reachability."""

    if snapshot_keys is not None and not snapshot_keys:
        return set()
    if not await _has_v4_map_tables(executor, schema_name):
        return set()
    candidate_filter = (
        {bytes(block_hash) for block_hash in candidate_hashes}
        if candidate_hashes is not None
        else None
    )
    if candidate_filter is not None and not candidate_filter:
        return set()
    reachable_hashes: set[bytes] = set()
    last_coordinate = (-1, "", -1)
    while True:
        map_pack_records = await _v4_map_pack_batch(
            executor,
            schema_name=schema_name,
            snapshot_keys=snapshot_keys,
            last_coordinate=last_coordinate,
        )
        if not map_pack_records:
            break
        for map_pack_record in map_pack_records:
            last_coordinate, pack_hashes = _v4_map_pack_hashes(
                map_pack_record,
                last_coordinate=last_coordinate,
            )
            if candidate_filter is None:
                reachable_hashes.update(pack_hashes)
            else:
                reachable_hashes.update(candidate_filter & pack_hashes)
                if reachable_hashes == candidate_filter:
                    return reachable_hashes
        if len(map_pack_records) < PTG2_V4_GC_MAP_PACK_BATCH_ROWS:
            break
    return reachable_hashes


async def _owned_v4_layout_fingerprint(
    executor: Any,
    *,
    schema_name: str,
    snapshot_key: int,
) -> bytes | None:
    """Load the unique fingerprint that serializes one V4 reservation."""

    schema = _quote_ident(schema_name)
    fingerprint_rows = await executor.all(
        f"""
        SELECT owned.semantic_fingerprint
          FROM (
                SELECT candidate.semantic_fingerprint
                  FROM {schema}.ptg2_layout_build_candidate AS candidate
                 WHERE candidate.snapshot_key = :snapshot_key
                UNION ALL
                SELECT fingerprint.semantic_fingerprint
                  FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                 WHERE fingerprint.snapshot_key = :snapshot_key
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_layout_build_candidate AS candidate
                         WHERE candidate.snapshot_key = :snapshot_key
                   )
          ) AS owned
         ORDER BY semantic_fingerprint
         LIMIT 2
        """,
        snapshot_key=int(snapshot_key),
    )
    if not fingerprint_rows:
        return None
    if len(fingerprint_rows) != 1:
        raise RuntimeError("owned PTG V4 layout has multiple fingerprints")
    fingerprint = bytes(
        _row_mapping(fingerprint_rows[0]).get("semantic_fingerprint") or b""
    )
    if len(fingerprint) != 32:
        raise RuntimeError("owned PTG V4 layout fingerprint is invalid")
    return fingerprint


def _owned_v4_layout_lock_sql(schema: str) -> str:
    return f"""
        SELECT layout.snapshot_key, layout.build_token,
               (
                   SELECT root.state
                     FROM {schema}.ptg2_v4_snapshot_map_root AS root
                    WHERE root.snapshot_key = layout.snapshot_key
               ) AS root_state,
               EXISTS (
                   SELECT 1
                     FROM {schema}.ptg2_v3_snapshot_binding AS binding
                    WHERE binding.snapshot_key = layout.snapshot_key
               ) AS is_bound
          FROM {schema}.ptg2_v3_snapshot_layout AS layout
         WHERE layout.snapshot_key = :snapshot_key
           AND layout.generation = :generation
           AND layout.state = 'building'
           AND layout.build_token = ANY(CAST(:owner_tokens AS varchar[]))
           AND COALESCE(
                (
                    SELECT candidate.semantic_fingerprint
                      FROM {schema}.ptg2_layout_build_candidate AS candidate
                     WHERE candidate.snapshot_key = layout.snapshot_key
                ),
                (
                    SELECT fingerprint.semantic_fingerprint
                      FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                     WHERE fingerprint.snapshot_key = layout.snapshot_key
                     ORDER BY fingerprint.semantic_fingerprint
                     LIMIT 1
                )
           ) = :semantic_fingerprint
           AND (
                SELECT COUNT(*)
                  FROM (
                        SELECT candidate.semantic_fingerprint
                          FROM {schema}.ptg2_layout_build_candidate AS candidate
                         WHERE candidate.snapshot_key = layout.snapshot_key
                        UNION ALL
                        SELECT fingerprint.semantic_fingerprint
                          FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                         WHERE fingerprint.snapshot_key = layout.snapshot_key
                           AND NOT EXISTS (
                                SELECT 1
                                  FROM {schema}.ptg2_layout_build_candidate AS candidate
                                 WHERE candidate.snapshot_key = layout.snapshot_key
                           )
                  ) AS owned_fingerprint
           ) = 1
         FOR UPDATE OF layout
    """


def _owned_v4_abandonment_token(build_token: str) -> str:
    """Derive the durable owner marker that prevents failed-build resumption."""

    digest = hashlib.sha256(
        PTG2_V4_ABANDONMENT_TOKEN_DOMAIN + str(build_token).encode("utf-8")
    ).hexdigest()
    return f"abandon-{digest}"


async def _claim_owned_v4_abandonment(
    executor: Any,
    *,
    schema: str,
    snapshot_key: int,
    build_token: str,
    abandonment_token: str,
    observed_token: str,
) -> None:
    """Claim an exact building layout or accept its own durable marker."""

    if observed_token == build_token:
        changed = await executor.status(
            f"""
            UPDATE {schema}.ptg2_v3_snapshot_layout
               SET build_token = :abandonment_token
             WHERE snapshot_key = :snapshot_key
               AND generation = :generation
               AND state = 'building'
               AND build_token = :build_token
            """,
            abandonment_token=abandonment_token,
            snapshot_key=snapshot_key,
            generation=PTG2_V4_SHARED_GENERATION,
            build_token=build_token,
        )
        if int(changed or 0) != 1:
            raise RuntimeError("owned PTG V4 layout changed during abandonment")
    elif observed_token != abandonment_token:
        raise RuntimeError("owned PTG V4 abandonment marker is invalid")


async def _is_owned_v4_layout_locked(
    executor: Any,
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    claim_abandonment: bool = False,
) -> bool:
    """Fence an exact unpublished V4 build against reservation and publication."""

    abandonment_token = _owned_v4_abandonment_token(build_token)
    fingerprint = await _owned_v4_layout_fingerprint(
        executor,
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
    )
    if fingerprint is None:
        return False
    await executor.status(
        "SELECT pg_advisory_xact_lock(:lock_key)",
        lock_key=v4_layout_advisory_lock_key(fingerprint),
    )
    schema = _quote_ident(schema_name)
    owner_rows = await executor.all(
        _owned_v4_layout_lock_sql(schema),
        semantic_fingerprint=fingerprint,
        snapshot_key=int(snapshot_key),
        generation=PTG2_V4_SHARED_GENERATION,
        owner_tokens=(
            [str(build_token), abandonment_token]
            if claim_abandonment
            else [str(build_token)]
        ),
    )
    if not owner_rows:
        return False
    owner = _row_mapping(owner_rows[0])
    if bool(owner.get("is_bound")):
        raise RuntimeError("refusing to abandon a bound PTG V4 layout")
    if owner.get("root_state") not in (None, "building"):
        raise RuntimeError("refusing to abandon a completed PTG V4 map root")
    if not claim_abandonment:
        return True
    await _claim_owned_v4_abandonment(
        executor,
        schema=schema,
        snapshot_key=int(snapshot_key),
        build_token=str(build_token),
        abandonment_token=abandonment_token,
        observed_token=str(owner.get("build_token") or ""),
    )
    return True


async def _additional_v4_layout_stats(
    executor: Any,
    *,
    schema_name: str,
    snapshot_keys: Sequence[int],
    v4_hashes: Iterable[bytes],
) -> PTG2SharedLayoutGCStats:
    """Count V4 hashes not already represented by selected V3 mappings."""

    normalized_hashes = sorted({bytes(block_hash) for block_hash in v4_hashes})
    if not normalized_hashes:
        return PTG2SharedLayoutGCStats()
    schema = _quote_ident(schema_name)
    stat_rows = await executor.all(
        f"""
        WITH requested AS MATERIALIZED (
            SELECT DISTINCT unnest(CAST(:block_hashes AS bytea[])) AS block_hash
        )
        SELECT COUNT(*)::bigint AS requested_count,
               COUNT(block.block_hash)::bigint AS resolved_count,
               COUNT(*) FILTER (
                   WHERE block.block_hash IS NOT NULL
                     AND NOT EXISTS (
                         SELECT 1
                           FROM {schema}.ptg2_v3_snapshot_block AS mapping
                          WHERE mapping.snapshot_key = ANY(
                                    CAST(:snapshot_keys AS bigint[])
                                )
                            AND mapping.block_hash = requested.block_hash
                     )
               )::bigint AS additional_count,
               COALESCE(SUM(block.stored_byte_count) FILTER (
                   WHERE NOT EXISTS (
                         SELECT 1
                           FROM {schema}.ptg2_v3_snapshot_block AS mapping
                          WHERE mapping.snapshot_key = ANY(
                                    CAST(:snapshot_keys AS bigint[])
                                )
                            AND mapping.block_hash = requested.block_hash
                   )
               ), 0)::bigint AS additional_stored_bytes
          FROM requested
          LEFT JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = requested.block_hash
        """,
        block_hashes=normalized_hashes,
        snapshot_keys=[int(snapshot_key) for snapshot_key in snapshot_keys],
    )
    stat_row = _row_mapping(stat_rows[0]) if stat_rows else {}
    if int(stat_row.get("requested_count") or 0) != int(
        stat_row.get("resolved_count") or 0
    ):
        raise RuntimeError("PTG V4 layout references a missing CAS block")
    return PTG2SharedLayoutGCStats(
        candidate_hash_count=int(stat_row.get("additional_count") or 0),
        stored_bytes=int(stat_row.get("additional_stored_bytes") or 0),
    )


_LAYOUT_PLAN_SQL_TEMPLATE = """
        WITH eligible_layouts AS MATERIALIZED (
            SELECT layout.snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.generation = ANY(CAST(:cleanup_generations AS text[]))
               AND (
                    (
                        layout.state = 'sealed'
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
                    OR (
                        layout.state = 'building'
                        AND layout.heartbeat_at
                            < transaction_timestamp()
                              - (:building_max_age_seconds * INTERVAL '1 second')
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
                       AND NOT (
                            binding.snapshot_id = ANY(
                                CAST(:removing_snapshot_ids AS varchar[])
                            )
                       )
               )
               AND (
                    cardinality(CAST(:removing_snapshot_ids AS varchar[])) = 0
                    OR EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_v3_snapshot_binding AS candidate_binding
                         WHERE candidate_binding.snapshot_key = layout.snapshot_key
                           AND candidate_binding.snapshot_id = ANY(
                                CAST(:removing_snapshot_ids AS varchar[])
                           )
                    )
               )
             ORDER BY layout.created_at, layout.snapshot_key
             LIMIT :layout_limit
        ),
        mapped_blocks AS MATERIALIZED (
            SELECT DISTINCT mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
              JOIN eligible_layouts AS layout
                ON layout.snapshot_key = mapping.snapshot_key
        )
        SELECT (SELECT COUNT(*) FROM eligible_layouts) AS logical_layout_count,
               ARRAY(
                   SELECT snapshot_key
                     FROM eligible_layouts
                    ORDER BY snapshot_key
               ) AS eligible_layout_keys,
               COUNT(block.block_hash) AS candidate_hash_count,
               COALESCE(SUM(block.stored_byte_count), 0) AS stored_bytes
          FROM mapped_blocks AS mapped
          JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = mapped.block_hash
"""


def _layout_plan_sql(schema_name: str) -> str:
    """Return bounded read-only SQL for eligible shared-layout accounting."""

    return _LAYOUT_PLAN_SQL_TEMPLATE.format(
        schema=_quote_ident(schema_name)
    )


async def _build_layout_release_plan_ready(
    executor: Any,
    *,
    schema_name: str,
    removing_snapshot_ids: Sequence[str],
    building_max_age_seconds: int,
    layout_limit: int | None,
) -> PTG2SharedLayoutGCStats:
    aggregate_records = await executor.all(
        _layout_plan_sql(schema_name),
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        removing_snapshot_ids=list(
            dict.fromkeys(
                str(snapshot_id) for snapshot_id in removing_snapshot_ids
            )
        ),
        building_max_age_seconds=building_max_age_seconds,
        layout_limit=layout_limit,
    )
    aggregate_record = _row_mapping(aggregate_records[0]) if aggregate_records else {}
    eligible_layout_keys = tuple(
        int(layout_key)
        for layout_key in (
            aggregate_record.get("eligible_layout_keys") or ()
        )
    )
    v4_hashes = await _v4_reachable_hashes(
        executor,
        schema_name=schema_name,
        snapshot_keys=eligible_layout_keys,
    )
    v4_stats = await _additional_v4_layout_stats(
        executor,
        schema_name=schema_name,
        snapshot_keys=eligible_layout_keys,
        v4_hashes=v4_hashes,
    )
    return PTG2SharedLayoutGCStats(
        logical_layout_count=int(aggregate_record.get("logical_layout_count") or 0),
        candidate_hash_count=(
            int(aggregate_record.get("candidate_hash_count") or 0)
            + v4_stats.candidate_hash_count
        ),
        stored_bytes=(
            int(aggregate_record.get("stored_bytes") or 0)
            + v4_stats.stored_bytes
        ),
    )


async def build_shared_layout_release_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    removing_snapshot_ids: Sequence[str] = (),
    building_max_age_seconds: int | None = None,
    max_layouts: int | None = None,
    all_eligible_layouts: bool = False,
    require_shared: bool = False,
) -> PTG2SharedLayoutGCStats:
    """Plan layout release without reading block payloads or changing state."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedLayoutGCStats(tables_available=False)
    return await _build_layout_release_plan_ready(
        executor,
        schema_name=schema_name,
        removing_snapshot_ids=removing_snapshot_ids,
        building_max_age_seconds=_building_max_age_seconds(building_max_age_seconds),
        layout_limit=None if all_eligible_layouts else _layout_batch_rows(max_layouts),
    )


def _lock_layouts_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        SELECT layout.snapshot_key
          FROM {schema}.ptg2_v3_snapshot_layout AS layout
         WHERE layout.generation = ANY(CAST(:cleanup_generations AS text[]))
           AND (
                NOT :restrict_layout_keys
                OR layout.snapshot_key = ANY(CAST(:layout_keys AS bigint[]))
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_binding AS binding
                 WHERE binding.snapshot_key = layout.snapshot_key
           )
           AND (
                (
                    layout.state = 'sealed'
                    AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                        <= transaction_timestamp()
                )
                OR (
                    layout.state = 'building'
                    AND (
                        EXISTS (
                            SELECT 1
                              FROM {schema}.ptg2_layout_build_candidate AS candidate
                             WHERE candidate.snapshot_key = layout.snapshot_key
                               AND candidate.cleanup_pending_at IS NOT NULL
                        )
                        AND NOT EXISTS (
                            SELECT 1
                              FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                             WHERE fingerprint.snapshot_key = layout.snapshot_key
                        )
                        OR (
                            layout.heartbeat_at
                                < transaction_timestamp()
                                  - (:building_max_age_seconds * INTERVAL '1 second')
                            AND COALESCE(
                                    layout.lease_until,
                                    '-infinity'::timestamptz
                                ) <= transaction_timestamp()
                        )
                    )
                )
           )
         ORDER BY layout.created_at, layout.snapshot_key
         LIMIT :layout_limit
           FOR UPDATE OF layout SKIP LOCKED
    """


def _dense_layout_delete_fragments(
    schema: str,
    *,
    selected_layout_cte: str = "layout_batch",
) -> tuple[str, str, str]:
    """Return CTEs and dependencies for every unconstrained dense layout table."""

    dense_delete_ctes: list[str] = []
    dense_delete_dependencies: list[str] = []
    dense_empty_guards: list[str] = []
    for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
        suffix = table_name.removeprefix("ptg2_v3_")
        selected_cte = f"selected_{suffix}"
        deleted_cte = f"deleted_{suffix}"
        dense_delete_ctes.append(
            f"""
        {selected_cte} AS MATERIALIZED (
            SELECT bounded.ctid
              FROM {selected_layout_cte} AS selected
              CROSS JOIN LATERAL (
                    SELECT payload.ctid
                      FROM {schema}.{_quote_ident(table_name)} AS payload
                     WHERE payload.snapshot_key = selected.snapshot_key
                     ORDER BY payload.ctid
                     LIMIT :dense_batch_rows
                       FOR UPDATE OF payload SKIP LOCKED
              ) AS bounded
        ),
        {deleted_cte} AS (
             DELETE FROM {schema}.{_quote_ident(table_name)} AS payload
             USING {selected_cte} AS bounded
             WHERE payload.ctid = bounded.ctid
            RETURNING payload.snapshot_key
        ),"""
        )
        dense_delete_dependencies.append(
            f"AND (SELECT COUNT(*) FROM {deleted_cte}) >= 0"
        )
        dense_empty_guards.append(
            f"""AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.{_quote_ident(table_name)} AS remaining
                     WHERE remaining.snapshot_key = layout.snapshot_key
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {selected_cte} AS selected_row
                             WHERE selected_row.ctid = remaining.ctid
                       )
               )"""
        )
    return (
        "\n".join(dense_delete_ctes),
        "\n               ".join(dense_delete_dependencies),
        "\n               ".join(dense_empty_guards),
    )


_RELEASE_LAYOUTS_SQL_TEMPLATE = """
        WITH layout_batch AS MATERIALIZED (
            SELECT layout.snapshot_key
              FROM {schema}.ptg2_v3_snapshot_layout AS layout
             WHERE layout.snapshot_key = ANY(CAST(:layout_keys AS bigint[]))
               AND layout.generation = ANY(CAST(:cleanup_generations AS text[]))
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
               )
               AND (
                    (
                        layout.state = 'sealed'
                        AND COALESCE(layout.lease_until, '-infinity'::timestamptz)
                            <= transaction_timestamp()
                    )
                    OR (
                        layout.state = 'building'
                        AND (
                            EXISTS (
                                SELECT 1
                                  FROM {schema}.ptg2_layout_build_candidate AS candidate
                                 WHERE candidate.snapshot_key = layout.snapshot_key
                                   AND candidate.cleanup_pending_at IS NOT NULL
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM {schema}.ptg2_v3_layout_fingerprint AS fingerprint
                                 WHERE fingerprint.snapshot_key = layout.snapshot_key
                            )
                            OR (
                                layout.heartbeat_at
                                    < transaction_timestamp()
                                      - (:building_max_age_seconds * INTERVAL '1 second')
                                AND COALESCE(
                                        layout.lease_until,
                                        '-infinity'::timestamptz
                                    ) <= transaction_timestamp()
                            )
                        )
                    )
               )
        ),
        mapped_blocks AS MATERIALIZED (
            SELECT DISTINCT mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
              JOIN layout_batch AS layout
                ON layout.snapshot_key = mapping.snapshot_key
            UNION
            SELECT DISTINCT v4_hash.block_hash
              FROM unnest(CAST(:v4_block_hashes AS bytea[]))
                       AS v4_hash(block_hash)
        ),
        queued_candidates AS (
            INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
                (block_hash, eligible_at, queued_at)
            SELECT mapped.block_hash,
                   transaction_timestamp()
                       + (:grace_seconds * INTERVAL '1 second'),
                   transaction_timestamp()
              FROM mapped_blocks AS mapped
            ON CONFLICT (block_hash) DO UPDATE
                SET eligible_at = GREATEST(
                    candidate.eligible_at,
                    EXCLUDED.eligible_at
                )
            RETURNING block_hash
        ),
        {dense_delete_sql}
        deleted_layouts AS (
            DELETE FROM {schema}.ptg2_v3_snapshot_layout AS layout
             USING layout_batch AS selected
             WHERE layout.snapshot_key = selected.snapshot_key
               AND NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_snapshot_binding AS binding
                     WHERE binding.snapshot_key = layout.snapshot_key
               )
               AND (SELECT COUNT(*) FROM queued_candidates) >= 0
               {dense_dependency_sql}
               {dense_empty_guard_sql}
            RETURNING layout.snapshot_key
        )
        SELECT (SELECT COUNT(*) FROM deleted_layouts) AS logical_layout_count,
               (SELECT COUNT(*) FROM mapped_blocks) AS candidate_hash_count,
               COALESCE((
                   SELECT SUM(block.stored_byte_count)
                     FROM mapped_blocks AS mapped
                     JOIN {schema}.ptg2_v3_block AS block
                       ON block.block_hash = mapped.block_hash
               ), 0) AS stored_bytes
"""


def _release_layouts_sql(schema_name: str) -> str:
    """Build guarded SQL that queues block GC and deletes unbound layouts."""

    schema = _quote_ident(schema_name)
    (
        dense_delete_sql,
        dense_dependency_sql,
        dense_empty_guard_sql,
    ) = _dense_layout_delete_fragments(schema)
    return _RELEASE_LAYOUTS_SQL_TEMPLATE.format(
        schema=schema,
        dense_delete_sql=dense_delete_sql,
        dense_dependency_sql=dense_dependency_sql,
        dense_empty_guard_sql=dense_empty_guard_sql,
    )


def _v4_abandonment_chunks(
    values: Sequence[bytes],
    batch_rows: int,
) -> Iterable[tuple[bytes, ...]]:
    for offset in range(0, len(values), batch_rows):
        yield tuple(values[offset : offset + batch_rows])


def _v4_abandonment_remaining_seconds(
    deadline: float,
    monotonic: Callable[[], float],
) -> float:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise PTG2SharedLayoutAbandonmentDeferred(
            "bounded PTG V4 abandonment reached its time budget"
        )
    return remaining


async def _set_v4_abandonment_statement_timeout(
    executor: Any,
    *,
    deadline: float,
    monotonic: Callable[[], float],
    statement_timeout_seconds: float,
) -> None:
    remaining = _v4_abandonment_remaining_seconds(deadline, monotonic)
    timeout_milliseconds = max(
        int(min(remaining, statement_timeout_seconds) * 1_000),
        1,
    )
    await executor.all(
        "SELECT set_config('statement_timeout', :timeout_value, true)",
        timeout_value=f"{timeout_milliseconds}ms",
    )


def _is_v4_abandonment_statement_timeout(error: BaseException) -> bool:
    error_names = {
        type(error).__name__,
        type(getattr(error, "orig", None)).__name__,
        type(getattr(getattr(error, "orig", None), "__cause__", None)).__name__,
    }
    return bool(
        error_names & {"QueryCanceledError", "QueryCanceled", "OperationalError"}
    ) and "statement timeout" in str(error).lower()


async def _owned_v4_mapping_hashes(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
) -> set[bytes]:
    """Read exact mapping hashes in deterministic bounded keyset batches."""

    schema = _quote_ident(context.schema_name)
    block_hashes: set[bytes] = set()
    last_hash = b""
    while True:
        _v4_abandonment_remaining_seconds(context.deadline, context.monotonic)
        mapping_records = await executor.all(
            f"""
            SELECT DISTINCT mapping.block_hash
              FROM {schema}.ptg2_v3_snapshot_block AS mapping
             WHERE mapping.snapshot_key = :snapshot_key
               AND mapping.block_hash > :last_hash
            ORDER BY mapping.block_hash
             LIMIT :batch_rows
            """,
            snapshot_key=context.snapshot_key,
            last_hash=last_hash,
            batch_rows=context.batch_rows,
        )
        if not mapping_records:
            break
        batch_hashes = tuple(
            bytes(_row_mapping(mapping_record).get("block_hash") or b"")
            for mapping_record in mapping_records
        )
        if (
            any(len(block_hash) != 32 for block_hash in batch_hashes)
            or tuple(sorted(set(batch_hashes))) != batch_hashes
            or batch_hashes[0] <= last_hash
        ):
            raise RuntimeError("owned PTG V4 mapping hash order is invalid")
        block_hashes.update(batch_hashes)
        last_hash = batch_hashes[-1]
        if len(mapping_records) < context.batch_rows:
            break
    return block_hashes


async def _owned_v4_stored_bytes(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    block_hashes: Sequence[bytes],
) -> int:
    """Lock every referenced CAS row and return its exact stored bytes."""

    schema = _quote_ident(context.schema_name)
    stored_bytes = 0
    for hash_batch in _v4_abandonment_chunks(
        block_hashes,
        context.batch_rows,
    ):
        _v4_abandonment_remaining_seconds(
            context.deadline,
            context.monotonic,
        )
        block_records = await executor.all(
            f"""
            SELECT block_hash, stored_byte_count
              FROM {schema}.ptg2_v3_block
             WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
             ORDER BY block_hash
             FOR KEY SHARE
            """,
            block_hashes=list(hash_batch),
        )
        if len(block_records) != len(hash_batch):
            raise RuntimeError("PTG V4 layout references a missing CAS block")
        stored_bytes += sum(
            int(
                _row_mapping(block_record).get("stored_byte_count")
                or 0
            )
            for block_record in block_records
        )
    return stored_bytes


async def _owned_v4_inventory(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
) -> _OwnedV4AbandonmentInventory | None:
    """Inventory one still-owned building layout without reading payloads."""

    await _set_v4_abandonment_statement_timeout(
        executor,
        deadline=context.deadline,
        monotonic=context.monotonic,
        statement_timeout_seconds=context.statement_timeout_seconds,
    )
    if not await _is_owned_v4_layout_locked(
        executor,
        schema_name=context.schema_name,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        claim_abandonment=True,
    ):
        return None
    block_hashes = await _owned_v4_mapping_hashes(
        executor,
        context=context,
    )
    block_hashes.update(
        await _v4_reachable_hashes(
            executor,
            schema_name=context.schema_name,
            snapshot_keys=(context.snapshot_key,),
        )
    )
    ordered_hashes = tuple(sorted(block_hashes))
    return _OwnedV4AbandonmentInventory(
        block_hashes=ordered_hashes,
        stored_bytes=await _owned_v4_stored_bytes(
            executor,
            context=context,
            block_hashes=ordered_hashes,
        ),
        abandonment_token=_owned_v4_abandonment_token(context.build_token),
    )


async def _queue_owned_v4_candidate_batch(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    block_hashes: Sequence[bytes],
) -> int:
    """Queue one locked hash batch for ordinary grace-bound CAS sweeping."""

    await _set_v4_abandonment_statement_timeout(
        executor,
        deadline=context.deadline,
        monotonic=context.monotonic,
        statement_timeout_seconds=context.statement_timeout_seconds,
    )
    if not await _is_owned_v4_layout_locked(
        executor,
        schema_name=context.schema_name,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        claim_abandonment=True,
    ):
        raise RuntimeError("owned PTG V4 layout changed during abandonment")
    schema = _quote_ident(context.schema_name)
    locked_rows = await executor.all(
        f"""
        SELECT block_hash
          FROM {schema}.ptg2_v3_block
         WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))
         ORDER BY block_hash
         FOR KEY SHARE
        """,
        block_hashes=list(block_hashes),
    )
    if len(locked_rows) != len(block_hashes):
        raise RuntimeError("PTG V4 layout references a missing CAS block")
    await executor.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_gc_candidate AS candidate
            (block_hash, eligible_at, queued_at)
        SELECT requested.block_hash,
               transaction_timestamp()
                   + (:grace_seconds * INTERVAL '1 second'),
               transaction_timestamp()
          FROM unnest(CAST(:block_hashes AS bytea[]))
                   AS requested(block_hash)
        ON CONFLICT (block_hash) DO UPDATE
            SET eligible_at = GREATEST(
                candidate.eligible_at,
                EXCLUDED.eligible_at
            )
        """,
        block_hashes=list(block_hashes),
        grace_seconds=context.grace_seconds,
    )
    return len(block_hashes)


async def _delete_owned_v4_dense_batch(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    table_name: str,
) -> int:
    """Delete one bounded dense-table batch while ownership remains exact."""

    await _set_v4_abandonment_statement_timeout(
        executor,
        deadline=context.deadline,
        monotonic=context.monotonic,
        statement_timeout_seconds=context.statement_timeout_seconds,
    )
    if not await _is_owned_v4_layout_locked(
        executor,
        schema_name=context.schema_name,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        claim_abandonment=True,
    ):
        raise RuntimeError("owned PTG V4 layout changed during abandonment")
    schema = _quote_ident(context.schema_name)
    table = _quote_ident(table_name)
    deleted_rows = await executor.all(
        f"""
        WITH selected AS MATERIALIZED (
            SELECT ctid
              FROM {schema}.{table}
             WHERE snapshot_key = :snapshot_key
             ORDER BY ctid
             LIMIT :batch_rows
             FOR UPDATE
        )
        DELETE FROM {schema}.{table} AS payload
         USING selected
         WHERE payload.ctid = selected.ctid
        RETURNING 1
        """,
        snapshot_key=context.snapshot_key,
        batch_rows=context.batch_rows,
    )
    return len(deleted_rows)


async def _current_owned_v4_hashes(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
) -> tuple[bytes, ...]:
    """Return the current exact relational and packed-map reachability."""

    current_hashes = await _owned_v4_mapping_hashes(
        executor,
        context=context,
    )
    current_hashes.update(
        await _v4_reachable_hashes(
            executor,
            schema_name=context.schema_name,
            snapshot_keys=(context.snapshot_key,),
        )
    )
    return tuple(sorted(current_hashes))


async def _assert_owned_v4_candidates_complete(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    block_hashes: Sequence[bytes],
) -> None:
    """Require every inventory hash to remain locked and queued."""

    schema = _quote_ident(context.schema_name)
    for hash_batch in _v4_abandonment_chunks(
        block_hashes,
        context.batch_rows,
    ):
        candidate_rows = await executor.all(
            f"""
            SELECT candidate.block_hash
              FROM {schema}.ptg2_v3_gc_candidate AS candidate
              JOIN {schema}.ptg2_v3_block AS block
                ON block.block_hash = candidate.block_hash
             WHERE candidate.block_hash = ANY(CAST(:block_hashes AS bytea[]))
             ORDER BY candidate.block_hash
             FOR UPDATE OF candidate
             FOR KEY SHARE OF block
            """,
            block_hashes=list(hash_batch),
        )
        if len(candidate_rows) != len(hash_batch):
            raise RuntimeError(
                "owned PTG V4 abandonment candidate queue is incomplete"
            )


async def _require_owned_v4_dense_cleanup(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
) -> None:
    """Require every snapshot-local dense table to be empty."""

    schema = _quote_ident(context.schema_name)
    dense_counts = await executor.all(
        f"""
        SELECT table_name, row_count
          FROM (
                {" UNION ALL ".join(
                    f"SELECT '{table_name}'::text AS table_name, "
                    f"COUNT(*)::bigint AS row_count "
                    f"FROM {schema}.{_quote_ident(table_name)} "
                    f"WHERE snapshot_key = :snapshot_key"
                    for table_name in PTG2_V3_DENSE_LAYOUT_TABLES
                )}
          ) AS dense_counts
         WHERE row_count <> 0
        """,
        snapshot_key=context.snapshot_key,
    )
    if dense_counts:
        raise RuntimeError("owned PTG V4 dense cleanup is incomplete")


async def _delete_owned_v4_layout(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    abandonment_token: str,
) -> None:
    """Delete the exact empty layout while its durable owner token matches."""

    schema = _quote_ident(context.schema_name)
    deleted = await executor.status(
        f"""
        DELETE FROM {schema}.ptg2_v3_snapshot_layout AS layout
         WHERE layout.snapshot_key = :snapshot_key
           AND layout.generation = :generation
           AND layout.state = 'building'
           AND layout.build_token = :abandonment_token
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_binding AS binding
                 WHERE binding.snapshot_key = layout.snapshot_key
           )
        """,
        snapshot_key=context.snapshot_key,
        generation=PTG2_V4_SHARED_GENERATION,
        abandonment_token=abandonment_token,
    )
    if int(deleted or 0) != 1:
        raise RuntimeError("owned PTG V4 layout changed during abandonment")


async def _finalize_owned_v4_abandonment(
    executor: Any,
    *,
    context: _OwnedV4AbandonmentContext,
    inventory: _OwnedV4AbandonmentInventory,
) -> PTG2SharedLayoutGCStats:
    """Fence reachability, candidates, dense rows, and exact layout deletion."""

    await _set_v4_abandonment_statement_timeout(
        executor,
        deadline=context.deadline,
        monotonic=context.monotonic,
        statement_timeout_seconds=context.statement_timeout_seconds,
    )
    if not await _is_owned_v4_layout_locked(
        executor,
        schema_name=context.schema_name,
        snapshot_key=context.snapshot_key,
        build_token=context.build_token,
        claim_abandonment=True,
    ):
        raise RuntimeError("owned PTG V4 layout changed during abandonment")
    if await _current_owned_v4_hashes(
        executor,
        context=context,
    ) != inventory.block_hashes:
        raise RuntimeError(
            "owned PTG V4 layout reachability changed during abandonment"
        )
    await _assert_owned_v4_candidates_complete(
        executor,
        context=context,
        block_hashes=inventory.block_hashes,
    )
    await _require_owned_v4_dense_cleanup(
        executor,
        context=context,
    )
    await _delete_owned_v4_layout(
        executor,
        context=context,
        abandonment_token=inventory.abandonment_token,
    )
    return PTG2SharedLayoutGCStats(
        logical_layout_count=1,
        candidate_hash_count=len(inventory.block_hashes),
        stored_bytes=inventory.stored_bytes,
    )


async def _run_owned_v4_abandonment_step(
    operation: Callable[[Any], Any],
) -> Any:
    """Run one independently committable failed-layout cleanup step."""

    async with db.acquire() as connection:
        return await operation(connection)


def _owned_v4_abandonment_context(
    *,
    schema_name: str | None,
    snapshot_key: int,
    build_token: str,
    grace_seconds: int | None,
    options: PTG2V4AbandonmentOptions | None,
) -> _OwnedV4AbandonmentContext:
    """Normalize public cleanup controls into one immutable execution context."""

    controls = options or PTG2V4AbandonmentOptions()
    monotonic = controls.monotonic
    return _OwnedV4AbandonmentContext(
        schema_name=resolve_ptg2_schema(schema_name),
        snapshot_key=int(snapshot_key),
        build_token=str(build_token),
        batch_rows=_v4_abandonment_batch_rows(controls.batch_rows),
        deadline=(
            monotonic()
            + _v4_abandonment_timeout_seconds(controls.timeout_seconds)
        ),
        statement_timeout_seconds=(
            _v4_abandonment_statement_timeout_seconds(
                controls.statement_timeout_seconds
            )
        ),
        monotonic=monotonic,
        grace_seconds=_block_gc_grace_seconds(grace_seconds),
    )


async def _checked_owned_v4_inventory(
    connection: Any,
    context: _OwnedV4AbandonmentContext,
) -> _OwnedV4AbandonmentInventory | None:
    """Validate migrations before inventorying the exact owned layout."""

    if not await _has_shared_tables(
        connection,
        context.schema_name,
        require_shared=True,
    ):
        raise RuntimeError("owned PTG V4 abandonment requires shared tables")
    if not await _has_v4_map_tables(connection, context.schema_name):
        raise RuntimeError("owned PTG V4 abandonment requires V4 map tables")
    return await _owned_v4_inventory(connection, context=context)


async def _run_owned_v4_step(
    operation: Callable[[Any], Any],
    *,
    context: _OwnedV4AbandonmentContext,
    executor: Any | None,
) -> Any:
    """Run a bounded step on the caller transaction or a fresh connection."""

    _v4_abandonment_remaining_seconds(
        context.deadline,
        context.monotonic,
    )
    if executor is not None:
        return await operation(executor)
    return await _run_owned_v4_abandonment_step(operation)


async def _queue_owned_v4_candidates(
    *,
    context: _OwnedV4AbandonmentContext,
    inventory: _OwnedV4AbandonmentInventory,
    executor: Any | None,
    progress_callback: Callable[[str, int], None] | None,
) -> None:
    """Queue every inventory hash in independently committable batches."""

    for hash_batch in _v4_abandonment_chunks(
        inventory.block_hashes,
        context.batch_rows,
    ):
        queued = await _run_owned_v4_step(
            lambda connection, hash_batch=hash_batch: (
                _queue_owned_v4_candidate_batch(
                    connection,
                    context=context,
                    block_hashes=hash_batch,
                )
            ),
            context=context,
            executor=executor,
        )
        if progress_callback is not None:
            progress_callback("candidate_hashes", int(queued))


async def _delete_owned_v4_dense_rows(
    *,
    context: _OwnedV4AbandonmentContext,
    executor: Any | None,
    progress_callback: Callable[[str, int], None] | None,
) -> None:
    """Delete every dense layout table in restartable bounded batches."""

    for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
        while True:
            deleted = await _run_owned_v4_step(
                lambda connection, table_name=table_name: (
                    _delete_owned_v4_dense_batch(
                        connection,
                        context=context,
                        table_name=table_name,
                    )
                ),
                context=context,
                executor=executor,
            )
            if progress_callback is not None and deleted:
                progress_callback("dense_rows", int(deleted))
            if deleted < context.batch_rows:
                break
        if progress_callback is not None:
            progress_callback("dense_tables", 1)


async def abandon_owned_v4_layout(
    *,
    schema_name: str | None = None,
    snapshot_key: int,
    build_token: str,
    executor: Any | None = None,
    grace_seconds: int | None = None,
    progress_callback: Callable[[str, int], None] | None = None,
    options: PTG2V4AbandonmentOptions | None = None,
) -> PTG2SharedLayoutGCStats:
    """Bound and resume exact failed V4 cleanup without deleting CAS payloads."""

    context = _owned_v4_abandonment_context(
        schema_name=schema_name,
        snapshot_key=snapshot_key,
        build_token=build_token,
        grace_seconds=grace_seconds,
        options=options,
    )
    try:
        inventory = await _run_owned_v4_step(
            lambda connection: _checked_owned_v4_inventory(
                connection,
                context,
            ),
            context=context,
            executor=executor,
        )
        if inventory is None:
            return PTG2SharedLayoutGCStats()
        await _queue_owned_v4_candidates(
            context=context,
            inventory=inventory,
            executor=executor,
            progress_callback=progress_callback,
        )
        await _delete_owned_v4_dense_rows(
            context=context,
            executor=executor,
            progress_callback=progress_callback,
        )
        stats = await _run_owned_v4_step(
            lambda connection: _finalize_owned_v4_abandonment(
                connection,
                context=context,
                inventory=inventory,
            ),
            context=context,
            executor=executor,
        )
        if progress_callback is not None:
            progress_callback("layouts", stats.logical_layout_count)
        return stats
    except Exception as error:
        if _is_v4_abandonment_statement_timeout(error):
            raise PTG2SharedLayoutAbandonmentDeferred(
                "bounded PTG V4 abandonment statement timed out"
            ) from error
        raise


async def _release_layouts_ready(
    executor: Any,
    *,
    schema_name: str,
    building_max_age_seconds: int,
    grace_seconds: int,
    max_layouts: int,
    layout_keys: Sequence[int] | None,
    dense_batch_rows: int | None = None,
) -> PTG2SharedLayoutGCStats:
    locked_rows = await executor.all(
        _lock_layouts_sql(schema_name),
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        building_max_age_seconds=building_max_age_seconds,
        layout_limit=max_layouts,
        restrict_layout_keys=layout_keys is not None,
        layout_keys=list(layout_keys or ()),
    )
    layout_keys = [
        int(_row_mapping(locked_layout_row).get("snapshot_key"))
        for locked_layout_row in locked_rows
        if _row_mapping(locked_layout_row).get("snapshot_key") is not None
    ]
    if not layout_keys:
        return PTG2SharedLayoutGCStats()
    v4_block_hashes = await _v4_reachable_hashes(
        executor,
        schema_name=schema_name,
        snapshot_keys=layout_keys,
    )
    aggregate_records = await executor.all(
        _release_layouts_sql(schema_name),
        layout_keys=layout_keys,
        cleanup_generations=list(PTG2_V3_CLEANUP_GENERATIONS),
        building_max_age_seconds=building_max_age_seconds,
        grace_seconds=grace_seconds,
        v4_block_hashes=sorted(v4_block_hashes),
        dense_batch_rows=_v4_abandonment_batch_rows(dense_batch_rows),
    )
    aggregate_record = _row_mapping(aggregate_records[0]) if aggregate_records else {}
    return PTG2SharedLayoutGCStats(
        logical_layout_count=int(aggregate_record.get("logical_layout_count") or 0),
        candidate_hash_count=int(aggregate_record.get("candidate_hash_count") or 0),
        stored_bytes=int(aggregate_record.get("stored_bytes") or 0),
    )


async def release_unbound_ptg2_shared_layouts(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    building_max_age_seconds: int | None = None,
    grace_seconds: int | None = None,
    max_layouts: int | None = None,
    require_shared: bool = False,
    layout_keys: Sequence[int] | None = None,
) -> PTG2SharedLayoutGCStats:
    """Queue hashes and delete one bounded batch of eligible unbound layouts."""

    schema_name = resolve_ptg2_schema(schema_name)
    building_age = _building_max_age_seconds(building_max_age_seconds)
    grace = _block_gc_grace_seconds(grace_seconds)
    layout_limit = _layout_batch_rows(max_layouts)

    async def _run(connection: Any) -> PTG2SharedLayoutGCStats:
        if not await _has_shared_tables(
            connection,
            schema_name,
            require_shared=require_shared,
        ):
            return PTG2SharedLayoutGCStats(tables_available=False)
        return await _release_layouts_ready(
            connection,
            schema_name=schema_name,
            building_max_age_seconds=building_age,
            grace_seconds=grace,
            max_layouts=layout_limit,
            layout_keys=layout_keys,
        )

    if executor is not None and executor is not db:
        return await _run(executor)
    async with db.acquire() as connection:
        await acquire_ptg2_lifecycle_lock(connection)
        return await _run(connection)


def _eligible_blocks_sql(schema_name: str, *, lock_rows: bool) -> str:
    schema = _quote_ident(schema_name)
    lock_clause = "FOR UPDATE OF candidate, block SKIP LOCKED" if lock_rows else ""
    return f"""
        SELECT candidate.block_hash,
               block.stored_byte_count,
               candidate.eligible_at
          FROM {schema}.ptg2_v3_gc_candidate AS candidate
          JOIN {schema}.ptg2_v3_block AS block
            ON block.block_hash = candidate.block_hash
         WHERE candidate.eligible_at <= transaction_timestamp()
           AND block.stored_byte_count <= :max_bytes
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_block AS mapping
                 WHERE mapping.block_hash = candidate.block_hash
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_block_build_pin AS pin
                 WHERE pin.block_hash = candidate.block_hash
                   AND EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_block_build_pin AS live_pin
                         WHERE live_pin.snapshot_key = pin.snapshot_key
                           AND live_pin.build_token = pin.build_token
                           AND live_pin.pin_token = pin.pin_token
                           AND live_pin.lease_until > transaction_timestamp()
                   )
           )
         ORDER BY candidate.eligible_at, candidate.block_hash
         LIMIT :max_rows
         {lock_clause}
    """


def _select_hashes_under_byte_cap(
    rows: Iterable[Any],
    *,
    max_bytes: int,
) -> PTG2SharedBlockSweepPlan:
    selected_hashes: list[bytes] = []
    selected_bytes = 0
    for raw_row in rows:
        row = _row_mapping(raw_row)
        stored_bytes = max(int(row.get("stored_byte_count") or 0), 0)
        if selected_bytes + stored_bytes > max_bytes:
            continue
        block_hash = bytes(row.get("block_hash") or b"")
        if not block_hash:
            continue
        selected_hashes.append(block_hash)
        selected_bytes += stored_bytes
    return PTG2SharedBlockSweepPlan(
        selected_hashes=tuple(selected_hashes),
        stored_bytes=selected_bytes,
    )


async def _build_sweep_plan_ready(
    executor: Any,
    *,
    schema_name: str,
    max_bytes: int,
    max_rows: int,
    lock_rows: bool,
) -> PTG2SharedBlockSweepPlan:
    candidate_rows = await executor.all(
        _eligible_blocks_sql(schema_name, lock_rows=lock_rows),
        max_bytes=max_bytes,
        max_rows=max_rows,
    )
    candidate_hashes = {
        bytes(_row_mapping(candidate_row).get("block_hash") or b"")
        for candidate_row in candidate_rows
    }
    v4_reachable = await _v4_reachable_hashes(
        executor,
        schema_name=schema_name,
        candidate_hashes=candidate_hashes,
    )
    if lock_rows and v4_reachable:
        schema = _quote_ident(schema_name)
        await executor.status(
            f"""
            DELETE FROM {schema}.ptg2_v3_gc_candidate AS candidate
             WHERE candidate.block_hash = ANY(CAST(:block_hashes AS bytea[]))
            """,
            block_hashes=sorted(v4_reachable),
        )
    filtered_rows = [
        candidate_row
        for candidate_row in candidate_rows
        if bytes(_row_mapping(candidate_row).get("block_hash") or b"")
        not in v4_reachable
    ]
    return _select_hashes_under_byte_cap(filtered_rows, max_bytes=max_bytes)


async def build_shared_block_sweep_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedBlockSweepPlan:
    """Select the exact eligible block hashes a bounded sweep would delete."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedBlockSweepPlan(tables_available=False)
    return await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=_block_gc_max_bytes(max_bytes),
        max_rows=_block_gc_max_rows(max_rows),
        lock_rows=False,
    )


def _delete_blocks_sql(
    schema_name: str,
    *,
    v4_tables_available: bool,
) -> str:
    schema = _quote_ident(schema_name)
    v4_guard = (
        f"""
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v4_snapshot_map_pack AS v4_mapping
                 WHERE v4_mapping.map_block_hash = block.block_hash
           )
        """
        if v4_tables_available
        else ""
    )
    return f"""
        DELETE FROM {schema}.ptg2_v3_block AS block
         WHERE block.block_hash = ANY(CAST(:block_hashes AS bytea[]))
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_v3_snapshot_block AS mapping
                 WHERE mapping.block_hash = block.block_hash
           )
           AND NOT EXISTS (
                SELECT 1
                  FROM {schema}.ptg2_block_build_pin AS pin
                 WHERE pin.block_hash = block.block_hash
                   AND EXISTS (
                        SELECT 1
                          FROM {schema}.ptg2_block_build_pin AS live_pin
                         WHERE live_pin.snapshot_key = pin.snapshot_key
                           AND live_pin.build_token = pin.build_token
                           AND live_pin.pin_token = pin.pin_token
                           AND live_pin.lease_until > transaction_timestamp()
                   )
           )
           {v4_guard}
        RETURNING block.block_hash, block.stored_byte_count
    """


def _remove_orphan_candidates_sql(schema_name: str) -> str:
    schema = _quote_ident(schema_name)
    return f"""
        WITH orphaned AS (
            SELECT candidate.block_hash
              FROM {schema}.ptg2_v3_gc_candidate AS candidate
             WHERE NOT EXISTS (
                    SELECT 1
                      FROM {schema}.ptg2_v3_block AS block
                     WHERE block.block_hash = candidate.block_hash
             )
             ORDER BY candidate.block_hash
             LIMIT :max_rows
        )
        DELETE FROM {schema}.ptg2_v3_gc_candidate AS candidate
         USING orphaned
         WHERE candidate.block_hash = orphaned.block_hash
    """


async def _sweep_ready(
    executor: Any,
    *,
    schema_name: str,
    max_bytes: int,
    max_rows: int,
) -> PTG2SharedBlockSweepPlan:
    selected = await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=max_bytes,
        max_rows=max_rows,
        lock_rows=True,
    )
    deleted_rows: list[Any] = []
    if selected.selected_hashes:
        v4_tables_available = await _has_v4_map_tables(executor, schema_name)
        deleted_rows = await executor.all(
            _delete_blocks_sql(
                schema_name,
                v4_tables_available=v4_tables_available,
            ),
            block_hashes=list(selected.selected_hashes),
        )
    await executor.status(
        _remove_orphan_candidates_sql(schema_name),
        max_rows=max_rows,
    )
    deleted_bytes_by_hash = {
        bytes(_row_mapping(deleted_block_row).get("block_hash") or b""): int(
            _row_mapping(deleted_block_row).get("stored_byte_count") or 0
        )
        for deleted_block_row in deleted_rows
        if _row_mapping(deleted_block_row).get("block_hash")
    }
    deleted_hashes = tuple(
        block_hash
        for block_hash in selected.selected_hashes
        if block_hash in deleted_bytes_by_hash
    )
    return PTG2SharedBlockSweepPlan(
        selected_hashes=deleted_hashes,
        stored_bytes=sum(deleted_bytes_by_hash[block_hash] for block_hash in deleted_hashes),
    )


async def sweep_ptg2_shared_blocks(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedBlockSweepPlan:
    """Delete one eligible, byte-bounded block batch after a final mapping check."""

    schema_name = resolve_ptg2_schema(schema_name)
    byte_limit = _block_gc_max_bytes(max_bytes)
    row_limit = _block_gc_max_rows(max_rows)

    async def _run(connection: Any) -> PTG2SharedBlockSweepPlan:
        if not await _has_shared_tables(
            connection,
            schema_name,
            require_shared=require_shared,
        ):
            return PTG2SharedBlockSweepPlan(tables_available=False)
        return await _sweep_ready(
            connection,
            schema_name=schema_name,
            max_bytes=byte_limit,
            max_rows=row_limit,
        )

    if executor is not None and executor is not db:
        return await _run(executor)
    async with db.acquire() as connection:
        await acquire_ptg2_lifecycle_lock(connection)
        return await _run(connection)


async def build_ptg2_shared_gc_plan(
    *,
    schema_name: str | None = None,
    executor: Any | None = None,
    removing_snapshot_ids: Sequence[str] = (),
    building_max_age_seconds: int | None = None,
    max_layouts: int | None = None,
    max_bytes: int | None = None,
    max_rows: int | None = None,
    require_shared: bool = False,
) -> PTG2SharedGCPlan:
    """Build a metadata-only dry-run plan for layout release and block sweep."""

    schema_name = resolve_ptg2_schema(schema_name)
    executor = executor or db
    if not await _has_shared_tables(
        executor,
        schema_name,
        require_shared=require_shared,
    ):
        return PTG2SharedGCPlan(
            layouts=PTG2SharedLayoutGCStats(tables_available=False),
            sweep=PTG2SharedBlockSweepPlan(tables_available=False),
        )
    layouts = await _build_layout_release_plan_ready(
        executor,
        schema_name=schema_name,
        removing_snapshot_ids=removing_snapshot_ids,
        building_max_age_seconds=_building_max_age_seconds(building_max_age_seconds),
        layout_limit=_layout_batch_rows(max_layouts),
    )
    sweep = await _build_sweep_plan_ready(
        executor,
        schema_name=schema_name,
        max_bytes=_block_gc_max_bytes(max_bytes),
        max_rows=_block_gc_max_rows(max_rows),
        lock_rows=False,
    )
    return PTG2SharedGCPlan(layouts=layouts, sweep=sweep)


missing_ptg2_v3_migration_owned_tables = missing_migration_owned_tables
require_ptg2_v3_migration_owned_tables = require_migration_owned_tables
build_ptg2_shared_layout_release_plan = build_shared_layout_release_plan
build_ptg2_shared_block_sweep_plan = build_shared_block_sweep_plan


def _print_plan(plan: PTG2SharedGCPlan) -> None:
    print(f"shared_tables_available={str(plan.layouts.tables_available).lower()}")
    print(f"logical_layouts={plan.logical_layout_count}")
    print(f"candidate_hashes={plan.candidate_hash_count}")
    print(f"candidate_stored_bytes={plan.stored_bytes}")
    print(f"selected_hashes={plan.selected_hash_count}")
    print(f"selected_stored_bytes={plan.sweep.stored_bytes}")
    for block_hash in plan.selected_hashes:
        print(f"  selected_hash={block_hash.hex()}")


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


async def _amain(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Clean PTG V3 shared layouts and blocks.")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument("--execute", action="store_true", help="Apply cleanup. Default is dry-run.")
    parser.add_argument("--max-layouts", type=_non_negative_int, default=None)
    parser.add_argument("--max-rows", type=_non_negative_int, default=None)
    parser.add_argument("--max-bytes", type=_non_negative_int, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.execute:
        plan = await build_ptg2_shared_gc_plan(
            schema_name=args.schema,
            max_layouts=args.max_layouts,
            max_rows=args.max_rows,
            max_bytes=args.max_bytes,
        )
        _print_plan(plan)
        print("cleanup_executed=false")
        return

    layouts = await release_unbound_ptg2_shared_layouts(
        schema_name=args.schema,
        max_layouts=args.max_layouts,
    )
    sweep = await sweep_ptg2_shared_blocks(
        schema_name=args.schema,
        max_rows=args.max_rows,
        max_bytes=args.max_bytes,
    )
    _print_plan(PTG2SharedGCPlan(layouts=layouts, sweep=sweep))
    print("cleanup_executed=true")


if __name__ == "__main__":
    asyncio.run(_amain())
