# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate-only complete address serving index profile and source semantics."""

import hashlib
import json
import os
from uuid import UUID

from db.registry_schema import registry_schema
from process.entity_address_unified import _post_publish_index_plan
from process.network_address_projection import PinnedAddressSource, _identifier, _membership_accounting
from process.network_membership_candidate_indexes import _catalog_readiness, _projection_parity, _ready_replay
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import (
    _native_accounting,
    _scope_report,
    _validated_replay,
    _validation_report,
)


class NetworkServingIndexesError(ValueError):
    """Serving compatibility failed without committing candidate DDL or receipts."""


async def _column_metadata(connection, source_table, projection_table):
    columns = await connection.fetch(
        """
        SELECT relation.oid AS table_oid,attribute.attname,attribute.atttypid,attribute.atttypmod,
            format_type(attribute.atttypid,attribute.atttypmod) AS type_sql,
            attribute.attcollation,attribute.attndims,attribute.attnotnull,
            attribute.attidentity::text AS attidentity,attribute.attgenerated::text AS attgenerated,
            pg_get_expr(column_default.adbin,column_default.adrelid) AS default_sql,
            relation.oid=to_regclass($1) AS is_source
        FROM pg_attribute attribute JOIN pg_class relation ON relation.oid=attribute.attrelid
        LEFT JOIN pg_attrdef column_default ON column_default.adrelid=attribute.attrelid
            AND column_default.adnum=attribute.attnum
        WHERE relation.oid IN (to_regclass($1),to_regclass($2)) AND relation.relkind='r'
            AND attribute.attnum>0 AND NOT attribute.attisdropped ORDER BY attribute.attnum
    """,
        source_table,
        projection_table,
    )
    source_by_column = {column["attname"]: dict(column) for column in columns if column["is_source"]}
    candidate_by_column = {column["attname"]: dict(column) for column in columns if not column["is_source"]}
    if not source_by_column or set(candidate_by_column) != set(source_by_column) | {"canonical_network_ids"}:
        raise NetworkServingIndexesError("Candidate columns differ from the complete pinned source")
    return source_by_column, candidate_by_column


def _semantic_changes(source_by_column, candidate_by_column):
    changes = []
    for column_name, candidate_column in candidate_by_column.items():
        column_identifier = _identifier(column_name)
        if column_name == "canonical_network_ids":
            if candidate_column["default_sql"] != "'{}'::integer[]":
                changes.append(f"ALTER COLUMN {column_identifier} SET DEFAULT '{{}}'::integer[]")
            continue
        source_column = source_by_column[column_name]
        if (
            source_column["attidentity"]
            or source_column["attgenerated"]
            or any(
                candidate_column[field] != source_column[field]
                for field in ("atttypid", "atttypmod", "attcollation", "attidentity", "attgenerated")
            )
        ):
            raise NetworkServingIndexesError("Candidate column types or generated semantics differ from source")
        if candidate_column["attndims"] != source_column["attndims"]:
            if source_column["attndims"] < 1:
                raise NetworkServingIndexesError("Candidate array declaration differs from pinned source")
            type_sql = source_column["type_sql"] + "[]" * (source_column["attndims"] - 1)
            changes.append(f"ALTER COLUMN {column_identifier} TYPE {type_sql}")
        if candidate_column["attnotnull"] != source_column["attnotnull"]:
            operation = "SET" if source_column["attnotnull"] else "DROP"
            changes.append(f"ALTER COLUMN {column_identifier} {operation} NOT NULL")
        if candidate_column["default_sql"] != source_column["default_sql"]:
            default_sql = source_column["default_sql"]
            operation = "DROP DEFAULT" if default_sql is None else "SET DEFAULT " + default_sql
            changes.append(f"ALTER COLUMN {column_identifier} {operation}")
    return changes


async def _source_semantics(connection, source_table, projection_table, *, replay):
    metadata = await _column_metadata(connection, source_table, projection_table)
    changes = _semantic_changes(*metadata)
    if changes and replay:
        raise NetworkServingIndexesError("Retained serving column semantics changed")
    if changes:
        await connection.execute(f"ALTER TABLE {projection_table} " + ",".join(changes))
        if _semantic_changes(*(await _column_metadata(connection, source_table, projection_table))):
            raise NetworkServingIndexesError("Candidate source semantics could not be restored")


async def _matching_inputs(connection, candidate, copy_target, address_source, control_schema):
    scope_report = _scope_report(candidate, address_source)
    if scope_report["source_generations"].get("unified_address") != address_source.generation_id:
        raise NetworkServingIndexesError("Pinned source generation differs from candidate")
    retained_report = _validated_replay(candidate, scope_report)
    canonical_readiness = _ready_replay(candidate, retained_report, scope_report)
    namespace = _identifier(copy_target.schema_name)
    address_table = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    address_counts = await _membership_accounting(
        connection, f"{namespace}.network_membership", f"{namespace}.provider_location_binding", address_table
    )
    native_counts = await _native_accounting(
        connection, f"{namespace}.network_membership", control_schema, copy_target.candidate_id
    )
    current_report = _validation_report(scope_report, address_counts, native_counts)
    if any(
        type(retained_report.get(field)) is not type(expected) or retained_report.get(field) != expected
        for field, expected in current_report.items()
    ):
        raise NetworkServingIndexesError("Candidate validation accounting or relationships changed")
    await _catalog_readiness(connection, copy_target.schema_name)
    parity_counts = await _projection_parity(
        connection,
        f"{namespace}.entity_address_unified",
        address_table,
        f"{namespace}.network_membership",
        f"{namespace}.provider_location_binding",
    )
    if any(canonical_readiness.get(field) != expected for field, expected in parity_counts.items()):
        raise NetworkServingIndexesError("Candidate parity counts differ from canonical readiness")
    return retained_report, scope_report


def _serving_plan(schema_name):
    statements, _ = _post_publish_index_plan(_identifier(schema_name), "serving", build_concurrently=False)
    return [
        (f"entity_address_unified_idx_{name}", statement.replace("IF NOT EXISTS ", ""))
        for name, statement in statements
        if name != "canonical_network_ids"
    ]


async def _build_serving_indexes(connection, schema_name, index_plan):
    collisions = await connection.fetch(
        "SELECT relation.relname FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[])",
        schema_name,
        [index_name for index_name, _ in index_plan],
    )
    if collisions:
        raise NetworkServingIndexesError("Planned serving index name already exists in candidate schema")
    for _, statement in index_plan:
        await connection.execute(statement)


async def _index_digest(connection, schema_name, index_plan):
    index_records = await connection.fetch(
        """
        SELECT index_relation.oid AS index_oid,index_relation.relname,index_record.indrelid,
            projected.oid AS table_oid,index_record.indisvalid,index_record.indisready,index_record.indislive,
            pg_get_indexdef(index_relation.oid) AS index_definition
        FROM pg_class projected JOIN pg_namespace namespace ON namespace.oid=projected.relnamespace
        JOIN pg_index index_record ON index_record.indrelid=projected.oid
        JOIN pg_class index_relation ON index_relation.oid=index_record.indexrelid
        WHERE namespace.nspname=$1 AND projected.relname='entity_address_unified'
    """,
        schema_name,
    )
    actual_by_name = {record["relname"]: record for record in index_records}
    if not all(index_name in actual_by_name for index_name, _ in index_plan) or any(
        record["indrelid"] != record["table_oid"]
        or record["index_oid"] <= 0
        or not all(record[field] is True for field in ("indisvalid", "indisready", "indislive"))
        for record in index_records
    ):
        raise NetworkServingIndexesError("Native serving indexes are missing or invalid on the exact candidate table")
    definitions = sorted(record["index_definition"] for record in index_records)
    return hashlib.sha256(
        json.dumps(definitions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


async def _prepare_serving(connection, copy_target, address_source, control_schema):
    candidate = await _locked_candidate(connection, copy_target, _identifier(control_schema))
    if candidate["state"] not in {"ready", "published"}:
        raise NetworkServingIndexesError("Canonical candidate must be ready before full serving readiness")
    namespace = _identifier(copy_target.schema_name)
    projection_table = f"{namespace}.entity_address_unified"
    await connection.execute(
        f"LOCK TABLE {projection_table},{namespace}.network_membership,{namespace}.provider_location_binding IN SHARE MODE"
    )
    retained_report, scope_report = await _matching_inputs(
        connection, candidate, copy_target, address_source, control_schema
    )
    cached_readiness = retained_report.get("serving_readiness")
    is_replay = cached_readiness is not None
    if candidate["state"] == "published" and not is_replay:
        raise NetworkServingIndexesError("Published candidate cannot acquire a new serving receipt")
    await _source_semantics(
        connection,
        f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
        projection_table,
        replay=is_replay,
    )
    index_plan = _serving_plan(copy_target.schema_name)
    if not is_replay:
        await _build_serving_indexes(connection, copy_target.schema_name, index_plan)
    index_definition_sha256 = await _index_digest(connection, copy_target.schema_name, index_plan)
    readiness_by_field = {
        "component": "unified_address_serving",
        "readiness_revision": 1,
        "ready": True,
        "index_profile": "serving",
        "scope": scope_report,
        "index_definition_sha256": index_definition_sha256,
    }
    if is_replay:
        if json.dumps(cached_readiness, sort_keys=True) != json.dumps(readiness_by_field, sort_keys=True):
            raise NetworkServingIndexesError("Retained serving receipt or native index definitions changed")
        return cached_readiness
    await connection.execute(f"ANALYZE {projection_table}")
    await connection.execute(
        f"UPDATE {_identifier(control_schema)}.network_membership_candidate SET validation_json=$2::jsonb WHERE candidate_id=$1",
        UUID(copy_target.candidate_id),
        json.dumps(retained_report | {"serving_readiness": readiness_by_field}, sort_keys=True),
    )
    return readiness_by_field


async def prepare_network_serving_indexes(connection, copy_target, source, *, control_schema=None):
    """Build candidate serving compatibility; immutable writer closure is separate."""
    if type(copy_target) is not MembershipCopyTarget or type(source) is not PinnedAddressSource:
        raise NetworkServingIndexesError("Trusted candidate and pinned source are required")
    if not connection.is_in_transaction():
        raise NetworkServingIndexesError("Serving readiness requires a caller-owned transaction")
    if source.schema_name == copy_target.schema_name:
        raise NetworkServingIndexesError("Pinned source must be outside the candidate")
    try:
        async with connection.transaction():
            return await _prepare_with_search_path(
                connection,
                copy_target,
                source,
                control_schema if control_schema is not None else registry_schema(),
            )
    except ValueError as error:
        if isinstance(error, NetworkServingIndexesError):
            raise
        raise NetworkServingIndexesError(str(error)) from error


async def _prepare_with_search_path(connection, copy_target, address_source, control_schema):
    original_search_path = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute("SET LOCAL search_path TO pg_catalog,public")
    readiness = await _prepare_serving(connection, copy_target, address_source, control_schema)
    await connection.execute("SELECT set_config('search_path',$1,true)", original_search_path)
    return readiness
