# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain exact finalized address stages before their short native cutover."""

import json

from sqlalchemy import text

from process.entity_address_candidate_preparation import PreparedEntityAddressGeneration
from process.entity_address_snapshot_ownership import capture_created_entity_address_archive_stage
from process.entity_address_snapshot_receipt import (
    capture_entity_address_stage_integrity_receipt as capture_entity_address_stage_integrity_receipt,
)
from process.entity_address_snapshot_source import (
    _clone_entity_address_evidence_sequence,
    entity_address_archive_relations,
    entity_address_archive_stage_schema,
    entity_address_unified,
)
from process.network_address_projection import _identifier
from process.network_bootstrap_sources import (
    _COPY_BATCH_BYTES,
    _COPY_BATCH_ROWS,
    _COPY_FRAMING_BYTES,
    _copy_native_batch,
    _CopyBatchTooLarge,
)
from process.network_cms_registry_address_capture import freeze_address_clone, native_driver
from process.network_cms_registry_address_equivalence import (
    RegistryCMSAddressEquivalenceError,
    capture_registry_cms_address_copy_source,
    portable_registry_cms_address_stage_receipt,
    validate_registry_cms_address_copy,
)
from process.provider_directory_cms_native_layout import capture_retained_native_source
from process.provider_directory_cms_preparation import RetainedNativeRelation, retained_raw_policy

_COPY_KEYS = {
    model.__tablename__: tuple(column.name for column in model.__table__.primary_key.columns)
    for model in (entity_address_unified.EntityAddressUnified, *entity_address_unified.SUPPORT_TABLE_MODELS)
}


def _prepared_copy_limits(prepared):
    rows = prepared.nonprofile_admission.plan.batch_size
    if (
        type(rows) is not int
        or rows <= 0
        or type(_COPY_BATCH_BYTES) is not int
        or not 0 < _COPY_BATCH_BYTES <= 64 * 1024**2
    ):
        raise ValueError("registry_cms_prepared_address_copy_limits_invalid")
    return min(rows, _COPY_BATCH_ROWS), _COPY_BATCH_BYTES


async def _require_prepared_copy_key(connection, source, oid, keys):
    """Require the exact native ordering index without modifying producer stages."""
    indexed = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_index i
        JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
        WHERE i.indrelid=$1::oid AND i.indrelid=to_regclass($2)
        AND i.indisprimary AND i.indisunique AND i.indisvalid AND i.indisready
        AND i.indislive AND i.indimmediate AND am.amname='btree' AND i.indnkeyatts=i.indnatts
        AND i.indexprs IS NULL AND i.indpred IS NULL
        AND ARRAY(SELECT a.attname::text FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum ORDER BY k.position)=$3::text[]
        AND NOT EXISTS(SELECT 1 FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
          JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
          JOIN pg_opclass op ON op.oid=i.indclass[k.position::int-1]
          WHERE NOT a.attnotnull OR a.attisdropped OR i.indoption[k.position::int-1]<>0
            OR i.indcollation[k.position::int-1]<>a.attcollation OR NOT op.opcdefault
            OR op.opcnamespace<>'pg_catalog'::regnamespace OR op.opcmethod<>c.relam))""",
        oid,
        source,
        list(keys),
    )
    if indexed is not True:
        raise ValueError("registry_cms_prepared_address_copy_key_invalid")


def _prepared_copy_range(keys, last_keys, upper_keys=None):
    columns = ",".join(_identifier(key) for key in keys)
    predicate, parameters = "TRUE", ()
    for boundary, comparison in ((last_keys, ">"), (upper_keys, "<=")):
        if boundary is not None:
            placeholders = ",".join("$" + str(len(parameters) + index + 1) for index in range(len(keys)))
            predicate += f" AND ROW({columns}){comparison}ROW({placeholders})"
            parameters += tuple(boundary)
    return predicate, parameters


async def _prepared_copy_bounds(connection, source_relation, keys, last_keys, row_limit):
    """Read one aggregate byte bound and terminal compound key without decoding rows."""
    columns = ",".join(_identifier(key) for key in keys)
    last_columns = ",".join(_identifier(key) + " AS copy_key_" + str(index) for index, key in enumerate(keys))
    descending = ",".join(_identifier(key) + " DESC" for key in keys)
    predicate, parameters = _prepared_copy_range(keys, last_keys)
    bounds_by_field = await connection.fetchrow(
        f"""WITH batch AS MATERIALIZED (SELECT * FROM {source_relation}
        WHERE {predicate} ORDER BY {columns} LIMIT {row_limit}),
        totals AS (SELECT count(*)::bigint AS row_count,
          COALESCE(sum(octet_length(record_send(batch))::bigint),0)::bigint AS native_bytes FROM batch),
        last_row AS (SELECT {last_columns} FROM batch ORDER BY {descending} LIMIT 1)
        SELECT totals.*,last_row.* FROM totals LEFT JOIN last_row ON TRUE""",
        *parameters,
    )
    upper_keys = tuple(bounds_by_field["copy_key_" + str(index)] for index in range(len(keys)))
    return bounds_by_field["row_count"], bounds_by_field["native_bytes"], upper_keys


def _prepared_import_admission(connection, prepared, schema, source_relation, oid):
    async def admit_import(_phase):
        """Recheck source identity and the complete signed admission at each COPY boundary."""
        actual = await connection.fetchval(
            "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass($1) AND relkind='r' AND relpersistence='p'",
            source_relation,
        )
        if actual != oid:
            raise ValueError("registry_cms_prepared_address_changed")
        await prepared.nonprofile_admission.assert_ready(prepared.fhir, schema)

    return admit_import


async def _copy_prepared_rows(connection, prepared, schema, stage_entry, expected_count, limits):
    """Copy bounded compound-key ranges, retaining the original source and admission."""
    logical, stage, oid = stage_entry
    source_relation = _identifier(prepared.address.db_schema) + "." + _identifier(stage)
    keys = _COPY_KEYS[logical]
    columns = ",".join(_identifier(key) for key in keys)
    row_limit, byte_limit = limits
    copied_rows, last_keys = 0, None
    admit_import = _prepared_import_admission(connection, prepared, schema, source_relation, oid)
    while copied_rows < expected_count:
        count, native_bytes, upper_keys = await _prepared_copy_bounds(
            connection, source_relation, keys, last_keys, row_limit
        )
        if (
            type(count) is not int
            or not 0 < count <= row_limit
            or copied_rows + count > expected_count
            or type(native_bytes) is not int
            or native_bytes < 0
            or any(key is None for key in upper_keys)
        ):
            raise ValueError("registry_cms_prepared_address_copy_accounting_invalid")
        # Native record_send includes type OIDs, so it bounds binary COPY row bytes above.
        if native_bytes + _COPY_FRAMING_BYTES > byte_limit:
            if count == 1:
                raise ValueError("registry_cms_prepared_address_copy_row_too_large")
            row_limit = max(1, count // 2)
            continue
        predicate, parameters = _prepared_copy_range(keys, last_keys, upper_keys)
        try:
            await _copy_native_batch(
                connection,
                f"SELECT * FROM {source_relation} WHERE {predicate} ORDER BY {columns}",
                parameters,
                schema,
                logical,
                count,
                byte_limit=byte_limit,
                import_admission=admit_import,
            )
        except _CopyBatchTooLarge:
            if count == 1:
                raise ValueError("registry_cms_prepared_address_copy_row_too_large") from None
            row_limit = max(1, count // 2)
            continue
        copied_rows += count
        last_keys = upper_keys


def prepared_address_stages(address):
    """Require the complete actual finalized family, without live-name fallback."""
    expected_names = {relation.table_name for relation in entity_address_archive_relations()}
    if type(address) is not PreparedEntityAddressGeneration or address.committed:
        raise ValueError("registry_cms_prepared_address_invalid")
    stages = address.stage_oids
    if (
        type(stages) is not tuple
        or len(stages) != 7
        or {entry[0] for entry in stages} != expected_names
        or len({entry[1] for entry in stages}) != 7
        or len({entry[2] for entry in stages}) != 7
    ):
        raise ValueError("registry_cms_prepared_address_invalid")
    _identifier(address.db_schema)
    for _logical, stage, oid in stages:
        _identifier(stage)
        if type(oid) is not int or not 0 < oid < 2**32:
            raise ValueError("registry_cms_prepared_address_invalid")
    return tuple(sorted(stages))


async def lock_prepared_address(session, address):
    """Pin only producer-owned prepared heaps and verify exact logged OIDs."""
    stages = prepared_address_stages(address)
    namespace = _identifier(address.db_schema)
    await session.execute(
        text(
            "LOCK TABLE "
            + ",".join(namespace + "." + _identifier(stage) for _, stage, _ in stages)
            + " IN SHARE MODE NOWAIT"
        )
    )
    for _logical, stage, oid in stages:
        actual = await session.scalar(
            text(
                "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:relation) AND relkind='r' AND relpersistence='p'"
            ),
            {"relation": namespace + "." + _identifier(stage)},
        )
        if actual != oid:
            raise ValueError("registry_cms_prepared_address_changed")
    return stages


def portable_prepared_receipt(receipt, stages):
    """Normalize physical stage names to the existing portable seven-table receipt."""
    return portable_registry_cms_address_stage_receipt(receipt, {logical: stage for logical, stage, _oid in stages})


async def _prepared_native_sources(prepared, stages):
    """Capture only the already admitted, locked source OIDs and their signed retention policy."""
    admission = prepared.nonprofile_admission
    owned_sources_by_identity = {
        (entry.relation, entry.oid): entry
        for entry in await admission.measure(prepared.fhir, prepared.address.db_schema)
    }
    if any((stage, oid) not in owned_sources_by_identity for _logical, stage, oid in stages):
        raise ValueError("registry_cms_prepared_address_changed")
    source_layout_by_logical = {
        logical: await capture_retained_native_source(
            prepared.fhir, owned_sources_by_identity[(stage, oid)], admission.plan.native_address_targets
        )
        for logical, stage, oid in stages
    }
    policy_json = json.dumps(retained_raw_policy(admission.lease), sort_keys=True, separators=(",", ":"))
    return source_layout_by_logical, policy_json


async def prepare_retained_registry_address(session, prepared, *, capture_id, owner_role, runtime_roles):
    """Copy and close the exact admitted stage family outside serving locks."""
    limits = _prepared_copy_limits(prepared)
    stages = await lock_prepared_address(session, prepared.address)
    connection = await native_driver(session)
    for logical, stage, oid in stages:
        source_relation = _identifier(prepared.address.db_schema) + "." + _identifier(stage)
        await _require_prepared_copy_key(connection, source_relation, oid, _COPY_KEYS[logical])
    source_capture = await capture_registry_cms_address_copy_source(
        session,
        source_schema=prepared.address.db_schema,
        expected_relation_oids=tuple((logical, oid) for logical, _stage, oid in stages),
        stage_table_names={logical: stage for logical, stage, _ in stages},
    )
    counts_by_logical = {entry.table_name: entry.row_count for entry in source_capture.semantic_receipt.tables}
    source_layout_by_logical, policy_json = await _prepared_native_sources(prepared, stages)
    schema = entity_address_archive_stage_schema(capture_id)
    namespace = _identifier(schema)
    await session.execute(text(f"CREATE SCHEMA {namespace}"))
    for logical, stage, oid in stages:
        source_relation = _identifier(prepared.address.db_schema) + "." + _identifier(stage)
        await session.execute(
            text(f"CREATE TABLE {namespace}.{_identifier(logical)} (LIKE {source_relation} INCLUDING ALL)")
        )
        await _preserve_prepared_storage_options(session, schema, logical, oid)
        if logical == "entity_address_evidence":
            await _clone_entity_address_evidence_sequence(session, stage_schema=schema)
        await _register_retained_relation(
            session, prepared, schema, logical, source_layout_by_logical[logical], policy_json
        )
    for logical, stage, oid in stages:
        await _copy_prepared_rows(
            connection, prepared, schema, (logical, stage, oid), counts_by_logical[logical], limits
        )
        await prepared.nonprofile_admission.assert_ready(prepared.fhir, schema)
    ownership = await capture_created_entity_address_archive_stage(session, dataset_id=capture_id)
    receipt, catalog = await freeze_address_clone(session, ownership, owner_role, runtime_roles)
    await _validate_prepared_address_copy(session, source_capture, ownership, receipt)
    return ownership, receipt, catalog, stages


async def _validate_prepared_address_copy(session, source_capture, ownership, receipt):
    """Require native content/catalog equivalence and the exact closed clone receipt."""
    try:
        witness = await validate_registry_cms_address_copy(
            session, source_capture=source_capture, clone_ownership=ownership
        )
    except RegistryCMSAddressEquivalenceError:
        raise ValueError("registry_cms_prepared_address_copy_changed") from None
    if witness.clone_receipt != receipt:
        raise ValueError("registry_cms_prepared_address_copy_changed")


async def _preserve_prepared_storage_options(session, schema, logical, source_oid):
    """LIKE omits heap and TOAST options; preserve the locked source's exact settings."""
    statement = await session.scalar(
        text(
            """SELECT CASE WHEN count(*)>0 THEN format('ALTER TABLE %I.%I SET (%s)',
              CAST(:schema AS text), CAST(:logical AS text),
              string_agg(settings.prefix||format('%I=%L', option.option_name, option.option_value),
                ',' ORDER BY settings.prefix, raw.position)) END
            FROM pg_class source LEFT JOIN pg_class toast ON toast.oid=source.reltoastrelid
            CROSS JOIN LATERAL (VALUES ('', source.reloptions), ('toast.', toast.reloptions)) settings(prefix, options)
            CROSS JOIN LATERAL unnest(settings.options) WITH ORDINALITY raw(value, position)
            CROSS JOIN LATERAL pg_options_to_table(ARRAY[raw.value]) option
            WHERE source.oid=CAST(:source_oid AS oid)"""
        ),
        {"schema": schema, "logical": logical, "source_oid": source_oid},
    )
    if statement is not None:
        await session.execute(text(statement))


async def _register_retained_relation(session, prepared, schema, logical, source_layout, policy_json):
    """Enroll each empty native heap before its bulk COPY and index growth."""
    connection = await native_driver(session)
    oid = await connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", _identifier(schema) + "." + _identifier(logical)
    )
    annotation = RetainedNativeRelation(schema, logical, oid, policy_json, source_layout)
    await prepared.nonprofile_admission.register_external_relation(
        prepared.fhir, schema, logical, oid, native_relation=annotation
    )
