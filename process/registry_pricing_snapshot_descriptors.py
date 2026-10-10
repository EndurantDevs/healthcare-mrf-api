# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded native shared descriptors; release readiness remains unassessed."""

from __future__ import annotations

import json

import asyncpg

from api.ptg2_tables import (
    PTG2_SCHEMA,
    PTG2_V4_SHARED_GENERATION,
    _optional_integer,
    _serving_tables_descriptor,
    _strict_coverage_scope_id,
    _strict_v3_manifest_fields,
    _v4_provider_graph_root_sql,
    _validate_v4_provider_graph_fields,
    _validated_published_snapshot_fields,
)
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_v4_finalizer_map_sql import _ROOT_SELECTION_SQL
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_MANIFEST_KEY,
    PTG2_V4_FINALIZER_MAP_PACK_TABLE,
    PTG2_V4_FINALIZER_MAP_ROOT_TABLE,
    PTG2_V4_FINALIZER_MAP_TARGET_TABLE,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    FinalizerMapError,
    _finalizer_readiness_by_snapshot,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_GRAPH_DIAGNOSTIC_TABLE

_AVAILABILITY_SQL = """SELECT relation_name,to_regclass(relation_name) IS NOT NULL AS available
FROM unnest($1::text[]) AS names(relation_name)"""
_BOUNDED_ROOT_SQL = """WITH roots AS MATERIALIZED ({set_sql}), bounds AS (
  SELECT coalesce(sum(octet_length(to_jsonb(root)::text)+2),0)+2 AS byte_count FROM roots root
)
SELECT current_setting('transaction_isolation') AS isolation,
  current_setting('transaction_read_only') AS read_only,byte_count<=$3::bigint AS bounded,
  CASE WHEN byte_count<=$3::bigint THEN (
    SELECT coalesce(jsonb_agg(to_jsonb(root) ORDER BY root.snapshot_key),'[]'::jsonb)::text FROM roots root
  ) END AS rows_json FROM bounds"""
_STORAGE_ERRORS = (asyncpg.UndefinedTableError, asyncpg.UndefinedColumnError)
_GRAPH_DIGEST_FIELDS = ("worst_member_digest", "worst_online_member_digest")
_FINALIZER_DIGEST_FIELDS = (
    "root_map_digest",
    "root_canonical_mapping_digest",
    "root_target_identity_digest",
)


async def _available_root_tables(connection, table_names):
    relation_names = [f"{_quote_ident(PTG2_SCHEMA)}.{_quote_ident(name)}" for name in table_names]
    availability = await connection.fetch(_AVAILABILITY_SQL, relation_names)
    available_by_name = {}
    for row in availability:
        name = row["relation_name"]
        if name not in relation_names or name in available_by_name or type(row["available"]) is not bool:
            raise ValueError("registry_pricing_snapshot_root_identity_invalid")
        available_by_name[name] = row["available"]
    if set(available_by_name) != set(relation_names):
        raise ValueError("registry_pricing_snapshot_root_identity_invalid")
    return sum(available_by_name.values())


def _decode_root_digests(root_fields, digest_fields):
    for field_name in digest_fields:
        encoded_digest = root_fields.get(field_name)
        if encoded_digest is None:
            continue
        if type(encoded_digest) is not str or len(encoded_digest) != 64:
            raise PTG2ManifestArtifactError("PTG2 persisted root digest is invalid")
        try:
            digest = bytes.fromhex(encoded_digest)
        except ValueError as error:
            raise PTG2ManifestArtifactError("PTG2 persisted root digest is invalid") from error
        if digest.hex() != encoded_digest:
            raise PTG2ManifestArtifactError("PTG2 persisted root digest is invalid")
        root_fields[field_name] = digest
    return root_fields


async def _fetch_bounded_roots(connection, set_sql, arguments, max_report_bytes):
    page = await connection.fetchrow(_BOUNDED_ROOT_SQL.format(set_sql=set_sql), *arguments, max_report_bytes)
    if page["isolation"] not in {"repeatable read", "serializable"} or page["read_only"] != "on":
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    if page["bounded"] is not True:
        return None
    encoded = page["rows_json"]
    if type(encoded) is not str or len(encoded.encode()) > max_report_bytes:
        raise ValueError("registry_pricing_snapshot_roots_invalid")
    selected_roots = json.loads(encoded)
    if type(selected_roots) is not list or any(type(root) is not dict for root in selected_roots):
        raise ValueError("registry_pricing_snapshot_roots_invalid")
    return selected_roots


def _graph_root_set_sql():
    sql = _v4_provider_graph_root_sql(PTG2_SCHEMA)
    sql = sql.replace("SELECT root.representation", "SELECT root.snapshot_key, root.representation", 1)
    sql = sql.replace("root.snapshot_key = :snapshot_key", "root.snapshot_key = ANY($1::bigint[])")
    sql = sql.replace(":storage_generation", "$2::text")
    for field_name in _GRAPH_DIGEST_FIELDS:
        sql = sql.replace(
            f"diagnostic.{field_name}",
            f"encode(diagnostic.{field_name}, 'hex') AS {field_name}",
        )
    return sql


def _finalizer_root_set_sql():
    sql = _ROOT_SELECTION_SQL.format(
        schema=_quote_ident(PTG2_SCHEMA),
        root_table=_quote_ident(PTG2_V4_FINALIZER_MAP_ROOT_TABLE),
        manifest_key=PTG2_V4_FINALIZER_MAP_MANIFEST_KEY,
    )
    sql = sql.replace("CAST(:snapshot_keys AS bigint[])", "$1::bigint[]")
    sql = sql.replace("CAST(:packed_object_kinds AS text[])", "$2::text[]")
    for field_name in _FINALIZER_DIGEST_FIELDS:
        column = field_name.removeprefix("root_")
        sql = sql.replace(
            f"root.{column} AS {field_name}",
            f"encode(root.{column}, 'hex') AS {field_name}",
        )
    return sql


async def _read_graph_roots(connection, snapshot_keys, max_report_bytes):
    if await _available_root_tables(connection, ("ptg2_v4_snapshot_map_root", PTG2_V4_GRAPH_DIAGNOSTIC_TABLE)) != 2:
        return None
    graph_roots = await _fetch_bounded_roots(
        connection,
        _graph_root_set_sql(),
        (list(snapshot_keys), PTG2_V4_SHARED_GENERATION),
        max_report_bytes,
    )
    if graph_roots is None:
        return None
    graph_by_key = {}
    for root_fields in graph_roots:
        key = root_fields.get("snapshot_key")
        if type(key) is not int or key not in snapshot_keys or key in graph_by_key:
            raise ValueError("registry_pricing_snapshot_root_identity_invalid")
        graph_by_key[key] = _decode_root_digests(root_fields, _GRAPH_DIGEST_FIELDS)
    return graph_by_key


async def _read_finalizer_roots(connection, snapshot_keys, max_report_bytes):
    table_names = (
        PTG2_V4_FINALIZER_MAP_ROOT_TABLE,
        PTG2_V4_FINALIZER_MAP_PACK_TABLE,
        PTG2_V4_FINALIZER_MAP_TARGET_TABLE,
    )
    present_count = await _available_root_tables(connection, table_names)
    if present_count == 0:
        return dict.fromkeys(snapshot_keys, False)
    if present_count != len(table_names):
        raise FinalizerMapError("packed finalizer map storage extension is partial")
    finalizer_roots = await _fetch_bounded_roots(
        connection,
        _finalizer_root_set_sql(),
        (list(snapshot_keys), list(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)),
        max_report_bytes,
    )
    if finalizer_roots is None:
        return None
    selected_keys = [root.get("snapshot_key") for root in finalizer_roots]
    if any(type(key) is not int or key not in snapshot_keys for key in selected_keys) or len(set(selected_keys)) != len(
        selected_keys
    ):
        raise ValueError("registry_pricing_snapshot_root_identity_invalid")
    decoded_roots = [_decode_root_digests(root, _FINALIZER_DIGEST_FIELDS) for root in finalizer_roots]
    return _finalizer_readiness_by_snapshot(decoded_roots, snapshot_keys)


async def _optional_v4_roots(connection, snapshot_keys, max_report_bytes):
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        graph_by_key = await _read_graph_roots(connection, snapshot_keys, max_report_bytes)
        finalizer_by_key = await _read_finalizer_roots(connection, snapshot_keys, max_report_bytes)
    except BaseException as primary_error:
        try:
            await savepoint.rollback()
        except BaseException:
            primary_error.add_note("optional descriptor lookup rollback was incomplete")
            raise primary_error
        if not connection.is_in_transaction():
            raise primary_error
        if isinstance(
            primary_error,
            (*_STORAGE_ERRORS, FinalizerMapError, PTG2ManifestArtifactError),
        ):
            return None, None
        raise
    await savepoint.commit()
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    return graph_by_key, finalizer_by_key


def _published_shared_descriptor(row_fields, graph_by_key, finalizer_by_key):
    if row_fields.get("has_local_physical_binding"):
        raise PTG2ManifestArtifactError("PTG local custody is not assessed by the shared descriptor phase")
    serving_index = row_fields["layout_serving_index"]
    if isinstance(serving_index, str):
        serving_index = json.loads(serving_index)
    key, generation, cold_contract, audit_sample = _strict_v3_manifest_fields(serving_index)
    if generation == PTG2_V4_SHARED_GENERATION:
        if graph_by_key is None or finalizer_by_key is None or key not in finalizer_by_key:
            raise PTG2ManifestArtifactError("PTG2 V4 persisted descriptor is unavailable")
        _validate_v4_provider_graph_fields(
            graph_by_key.get(key, {}),
            serving_index["serving_binary"]["provider_graph_v4"],
        )
    source_count = _optional_integer(serving_index.get("source_count"))
    code_count = _optional_integer(serving_index.get("code_count"))
    coverage_scope_id = _strict_coverage_scope_id(serving_index)
    serving_rate_count = _optional_integer(serving_index.get("serving_rates"))
    if code_count == 0 and serving_rate_count is not None and serving_rate_count > 0:
        raise PTG2ManifestArtifactError("PTG2 shared layout is missing code metadata for a non-empty snapshot")
    source_set, source_key = _validated_published_snapshot_fields(
        row_fields,
        shared_snapshot_key=key,
        physical_binding=None,
        coverage_scope_id=coverage_scope_id,
        audit_sample=audit_sample,
        source_count=source_count,
        code_count=code_count,
    )
    return _serving_tables_descriptor(
        row_fields["snapshot_id"],
        row_fields,
        serving_index,
        layout_by_field={
            "shared_snapshot_key": key,
            "storage_generation": generation,
            "cold_lookup_contract": cold_contract,
        },
        source_by_field={
            "source_count": source_count,
            "code_count": code_count,
            "coverage_scope_id": coverage_scope_id,
            "source_key": source_key,
            "audit_sample": audit_sample,
            "source_witness_by_field": None,
            "source_set_by_field": source_set,
        },
    )


async def read_pricing_shared_descriptors(connection, published_rows, *, max_report_bytes):
    """Construct strict shared descriptors; no selector or full release authority."""
    if not connection.is_in_transaction():
        raise ValueError("registry_pricing_snapshot_transaction_invalid")
    v4_by_key = {}
    for row_fields in published_rows:
        serving_index = row_fields["layout_serving_index"]
        if isinstance(serving_index, str):
            serving_index = json.loads(serving_index)
        key, generation, _, _ = _strict_v3_manifest_fields(serving_index)
        if generation == PTG2_V4_SHARED_GENERATION:
            v4_by_key[key] = None
    graph_by_key, finalizer_by_key = ({}, {})
    if v4_by_key:
        graph_by_key, finalizer_by_key = await _optional_v4_roots(connection, tuple(v4_by_key), max_report_bytes)
    descriptor_by_snapshot = {}
    for row_fields in published_rows:
        try:
            descriptor_by_snapshot[row_fields["snapshot_id"]] = _published_shared_descriptor(
                row_fields, graph_by_key, finalizer_by_key
            )
        except PTG2ManifestArtifactError:
            descriptor_by_snapshot[row_fields["snapshot_id"]] = None
    return descriptor_by_snapshot
