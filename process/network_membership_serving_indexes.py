# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate-only complete address serving index profile and source semantics."""

import hashlib
import json
import re
from uuid import UUID

from db.registry_schema import registry_schema
from process.entity_address_unified import _post_publish_index_plan
from process.network_address_projection import PinnedAddressSource, _identifier, _membership_accounting
from process.network_membership_candidate_indexes import _catalog_readiness, _projection_parity, _ready_replay
from process.network_membership_candidate_lifecycle import _locked_candidate
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_validation import (
    _json_object,
    _native_accounting,
    _scope_report,
    _validated_replay,
    _validation_report,
)


class NetworkServingIndexesError(ValueError):
    """Serving compatibility failed without committing candidate DDL or receipts."""


_OPTIONAL_SOURCE_COUNT_INDEX = "entity_address_unified_idx_source_count"
_OPTIONAL_OFFICE_PAGE_INDEX = "entity_address_unified_idx_serving_office_page"
_OPTIONAL_ADDRESS_INDEXES = frozenset({_OPTIONAL_SOURCE_COUNT_INDEX, _OPTIONAL_OFFICE_PAGE_INDEX})


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
    namespace = _identifier(schema_name)
    statements, _ = _post_publish_index_plan(namespace, "serving", build_concurrently=False)
    index_statements = [
        (f"entity_address_unified_idx_{name}", statement.replace("IF NOT EXISTS ", ""))
        for name, statement in statements
        if name != "canonical_network_ids"
    ]
    # Discover state-filtered pages from narrow office records before hydrating addresses.
    index_statements.append(
        (
            _OPTIONAL_OFFICE_PAGE_INDEX,
            f"CREATE INDEX {_OPTIONAL_OFFICE_PAGE_INDEX} ON {namespace}.entity_address_unified (state_name, location_key) "
            "INCLUDE (type, npi, inferred_npi, row_origin) "
            "WHERE row_origin <> 'network_custom' AND type IN ('primary', 'secondary', 'practice', 'site')",
        )
    )
    return index_statements


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


_INDEX_SHAPE_SQL = """, jsonb_build_object(
    'method',(SELECT amname FROM pg_am WHERE oid=index_relation.relam),
    'unique',index_record.indisunique,
    'keys',ARRAY(SELECT pg_get_indexdef(index_record.indexrelid,position,true)
        FROM generate_series(1,index_record.indnkeyatts) position),
    'include',ARRAY(SELECT pg_get_indexdef(index_record.indexrelid,position,true)
        FROM generate_series(index_record.indnkeyatts+1,index_record.indnatts) position),
    'opclasses',(SELECT jsonb_agg(jsonb_build_object(
        'name',namespace.nspname||'.'||operator_class.opcname,'default',operator_class.opcdefault)
        ORDER BY key.position)
        FROM unnest(index_record.indclass) WITH ORDINALITY key(class_oid,position)
        JOIN pg_opclass operator_class ON operator_class.oid=key.class_oid
        JOIN pg_namespace namespace ON namespace.oid=operator_class.opcnamespace),
    'options',index_record.indoption::smallint[],
    'default_collations',NOT EXISTS(SELECT 1 FROM unnest(index_record.indcollation) collation_oid
        WHERE collation_oid<>0 AND collation_oid<>'pg_catalog."default"'::regcollation),
    'predicate',pg_get_expr(index_record.indpred,index_record.indrelid)
) AS index_shape"""


async def _serving_index_records(connection, schema_name, *, qualified_definitions=False, include_structure=False):
    """Deparse the same complete index catalog independently of caller lookup settings."""
    previous_path = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute(
        "SET LOCAL search_path TO pg_catalog,pg_temp"
        if qualified_definitions
        else "SET LOCAL search_path TO pg_catalog,public"
    )
    try:
        index_records = await connection.fetch(
            """
            SELECT index_relation.relname,index_record.indisvalid,index_record.indisready,index_record.indislive,
                pg_get_indexdef(index_relation.oid) AS index_definition
            """
            + (_INDEX_SHAPE_SQL if include_structure else "")
            + """
            FROM pg_class projected JOIN pg_namespace namespace ON namespace.oid=projected.relnamespace
            JOIN pg_index index_record ON index_record.indrelid=projected.oid
            JOIN pg_class index_relation ON index_relation.oid=index_record.indexrelid
            WHERE namespace.nspname=$1 AND projected.relname='entity_address_unified'
        """,
            schema_name,
        )
    except BaseException as failure:
        try:
            await connection.execute("SELECT set_config('search_path',$1,true)", previous_path)
        except BaseException:
            raise failure from None
        raise
    await connection.execute("SELECT set_config('search_path',$1,true)", previous_path)
    return index_records


def _is_index_expression_equal(actual, expected):
    """Use the existing native deparser normalization without changing literal values."""
    from process.network_geo_search import _normalized_index_definition

    actual, expected = actual or "", expected or ""
    expected = re.sub(r"\btype IN \(([^()]+)\)", r"type = ANY (ARRAY[\1])", expected)
    return re.findall(r"'(?:''|[^'])*'", actual) == re.findall(r"'(?:''|[^'])*'", expected) and (
        _normalized_index_definition(actual).lower() == _normalized_index_definition(expected).lower()
    )


def _is_index_key_equal(actual, opclass, expected):
    explicit_class = re.fullmatch(r"(.+) (public\.[A-Za-z0-9_]+_ops)", expected)
    expression = explicit_class[1] if explicit_class else expected
    return _is_index_expression_equal(actual, expression) and (
        opclass["name"] == explicit_class[2] if explicit_class else opclass["default"] is True
    )


def _is_serving_index_valid(shape, recipe):
    keys = recipe["index_elements"]
    return (
        shape["method"] == recipe.get("using", "btree")
        and shape["unique"] is bool(recipe.get("unique", False))
        and len(shape["keys"]) == len(shape["opclasses"]) == len(keys)
        and shape["options"] == [0] * len(keys)
        and shape["default_collations"] is True
        and shape["include"] == list(recipe.get("include", ()))
        and _is_index_expression_equal(shape["predicate"], recipe.get("where"))
        and all(
            _is_index_key_equal(actual, opclass, expected)
            for actual, opclass, expected in zip(shape["keys"], shape["opclasses"], keys, strict=True)
        )
    )


def _require_serving_definitions(index_records, index_plan):
    """Validate current required recipes independently of names or settings on other indexes."""
    from db.models import EntityAddressUnified

    recipes_by_name = {
        "entity_address_unified_idx_" + recipe["name"]: recipe
        for recipe in EntityAddressUnified.__my_additional_indexes__
    }
    required_names = {name for name, _ in index_plan} - _OPTIONAL_ADDRESS_INDEXES
    for index_record in index_records:
        if index_record["relname"] in required_names:
            shape = _json_object(index_record["index_shape"])
            if not _is_serving_index_valid(shape, recipes_by_name[index_record["relname"]]):
                raise NetworkServingIndexesError("Native serving index differs from its required recipe")


async def _index_digest(
    connection, schema_name, index_plan, *, qualified_definitions=False, validate_definitions=False
):
    index_records = await _serving_index_records(
        connection,
        schema_name,
        qualified_definitions=qualified_definitions,
        **({"include_structure": True} if validate_definitions else {}),
    )
    # Retained editions remain readable without optional search accelerators.
    index_records = [record for record in index_records if record["relname"] not in _OPTIONAL_ADDRESS_INDEXES]
    actual_by_name = {record["relname"]: record for record in index_records}
    if not all(
        index_name in actual_by_name for index_name, _ in index_plan if index_name not in _OPTIONAL_ADDRESS_INDEXES
    ) or any(
        not all(record[field] is True for field in ("indisvalid", "indisready", "indislive"))
        for record in index_records
    ):
        raise NetworkServingIndexesError("Native serving indexes are missing or invalid on the exact candidate table")
    if validate_definitions:
        _require_serving_definitions(index_records, index_plan)
    definitions = sorted(record["index_definition"] for record in index_records)
    return hashlib.sha256(
        json.dumps(definitions, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    ).hexdigest()


async def _require_address_search_indexes(connection, relation, *, canonical=False, compact_base=False):
    """Check the exact searched heap without building or changing retained objects."""
    from process.entity_address_search_index_sql import required_geo_taxonomy_index_sql
    from process.entity_address_unified import _sql_literal

    if not compact_base and relation.table_name != "entity_address_unified":
        raise NetworkServingIndexesError("Search address relation differs")
    if canonical or compact_base:
        await _catalog_readiness(connection, relation.schema_name, table_name=relation.table_name, canonical=canonical)
    digest = (
        None
        if compact_base
        else await _index_digest(connection, relation.schema_name, _serving_plan(relation.schema_name))
    )
    sql = required_geo_taxonomy_index_sql(
        _sql_literal(relation.schema_name), _sql_literal(relation.table_name), None, None
    )
    if await connection.fetchval(sql) is not True:
        raise NetworkServingIndexesError("Search address geo and taxonomy indexes are not ready")
    return digest


async def verify_network_search_indexes(connection, candidate, *, compact_layout=None):
    """Require native search paths before a new head; retained reads stay compatible."""
    from process.network_compact_layout import CompactAddressRelation, CompactNetworkLayout
    from process.network_compact_readiness import compact_index_digest, is_compact_candidate

    if not connection.is_in_transaction():
        raise NetworkServingIndexesError("Search readiness requires a caller-owned transaction")
    report = _json_object(candidate["validation_json"])
    if is_compact_candidate(candidate):
        if type(compact_layout) is not CompactNetworkLayout:
            raise NetworkServingIndexesError("Verified compact search layout is required")
        await _require_address_search_indexes(connection, compact_layout.base, compact_base=True)
        if compact_layout.custom is not None:
            await _require_address_search_indexes(connection, compact_layout.custom)
        from process.network_geo_search import geo_search_receipt, verify_geo_search_indexes

        search = geo_search_receipt(candidate)
        if search is not None:
            await verify_geo_search_indexes(connection, search)
        digest = await compact_index_digest(connection, candidate["schema_name"], candidate=candidate)
    else:
        relation = CompactAddressRelation(
            candidate["schema_name"],
            "entity_address_unified",
        )
        digest = await _require_address_search_indexes(connection, relation, canonical=True)
    if digest != report["serving_readiness"]["index_definition_sha256"]:
        raise NetworkServingIndexesError("Search index definitions differ from retained readiness")


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
