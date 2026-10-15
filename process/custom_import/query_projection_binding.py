# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native catalog custody checks shared by projection writers and pinned reads."""

import hmac

from sqlalchemy import BigInteger, LargeBinary, Numeric, Text, select, text
from sqlalchemy.dialects import postgresql

from db.models.custom_import import CustomImportDefinitionRevision, CustomImportGeneration
from db.models.custom_import_storage import CustomImportSnapshotFamily, CustomImportSnapshotRelation
from process.custom_import.query_projection_sources import capability_sha256
from process.custom_import.query_projection_storage import (
    QUERY_PROJECTION_BINDING_INDEX,
    QUERY_PROJECTION_CONTRACT,
    QUERY_PROJECTION_TABLE_NAMES,
    query_projection_index_count,
    query_projection_tables,
)
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema


class QueryProjectionUnavailable(CandidateRunnerError):
    """Required projection identity, custody or completion evidence is invalid."""


_CATALOG_IDENTITY = text(
    r"""
    SELECT c.oid::bigint AS table_oid,c.relowner::bigint AS table_owner,
        n.nspowner::bigint AS schema_owner,c.relkind::text AS relkind,c.relpersistence::text AS relpersistence,
        EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AS inherited,
        EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid=c.oid AND contype='f') AS foreign_keys,
        EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=c.oid AND NOT tgisinternal) AS triggers,
        (SELECT pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.string_agg(
            ROW(a.attnum,a.attname,a.atttypid,a.atttypmod,a.attnotnull,a.attcollation,
                a.attidentity,a.attgenerated,pg_catalog.pg_get_expr(d.adbin,d.adrelid))::text,
            E'\\n' ORDER BY a.attnum),'UTF8'))
         FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d
            ON d.adrelid=a.attrelid AND d.adnum=a.attnum
         WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns_sha256,
        (SELECT pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
            a.attnum,a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod),
            a.attnotnull,a.attidentity,a.attgenerated,pg_catalog.pg_get_expr(d.adbin,d.adrelid),
            cn.nspname,coll.collname) ORDER BY a.attnum)
         FROM pg_catalog.pg_attribute a LEFT JOIN pg_catalog.pg_attrdef d
            ON d.adrelid=a.attrelid AND d.adnum=a.attnum
         LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=a.attcollation
         LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=coll.collnamespace
         WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
        (SELECT pg_catalog.array_agg(a.attname::text ORDER BY k.ordinal)
         FROM pg_catalog.pg_constraint p
         CROSS JOIN LATERAL pg_catalog.unnest(p.conkey) WITH ORDINALITY k(attnum,ordinal)
         JOIN pg_catalog.pg_attribute a ON a.attrelid=p.conrelid AND a.attnum=k.attnum
         WHERE p.conrelid=c.oid AND p.contype='p') AS primary_key
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname=:schema_name AND c.relname=:table_name
    """
)
_CATALOG_INDEXES = text(
    """
    SELECT x.relname::text AS index_name,i.indrelid::bigint AS table_oid,x.relowner::bigint AS index_owner,
        x.relkind::text AS index_kind,am.amname::text AS access_method,
        i.indisvalid AS valid,i.indisready AS ready,i.indislive AS live,
        i.indisunique AS unique_index,i.indisprimary AS primary_index,i.indisexclusion AS exclusion_index,
        i.indnkeyatts AS key_count,i.indnatts AS attribute_count,
        i.indexprs IS NULL AS plain,i.indpred IS NULL AS complete,
        ARRAY(SELECT a.attname::text FROM pg_catalog.unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
            ORDER BY k.position) AS key_names,
        ARRAY(SELECT cn.nspname::text || '.' || coll.collname::text
            FROM pg_catalog.unnest(i.indcollation::oid[]) WITH ORDINALITY k(collation_oid,position)
            LEFT JOIN pg_catalog.pg_collation coll ON coll.oid=k.collation_oid
            LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid=coll.collnamespace
            ORDER BY k.position) AS collation_names,
        NOT EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indoption::smallint[]) options(option_value)
            WHERE option_value<>0) AS default_options,
        NOT EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indcollation::oid[]) collations(collation_oid)
            WHERE collation_oid<>0) AS uncollated,
        NOT EXISTS(SELECT 1 FROM pg_catalog.unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,position)
            JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
            JOIN pg_catalog.pg_opclass op ON op.oid=i.indclass[k.position::int-1]
            JOIN pg_catalog.pg_namespace opn ON opn.oid=op.opcnamespace
            WHERE NOT op.opcdefault OR opn.nspname<>'pg_catalog' OR op.opcintype<>
                CASE WHEN a.atttypid='pg_catalog.varchar'::regtype THEN 'pg_catalog.text'::regtype
                    ELSE a.atttypid END) AS default_ops
    FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid
    JOIN pg_catalog.pg_namespace n ON n.oid=x.relnamespace
    JOIN pg_catalog.pg_am am ON am.oid=x.relam
    WHERE n.nspname=:schema_name AND x.relname=ANY(CAST(:index_names AS text[]))
    """
)


def _digest(value, label):
    if not isinstance(value, (bytes, bytearray, memoryview)) or len(value) != 32:
        raise QueryProjectionUnavailable(f"projection {label} is invalid")
    return bytes(value)


def _column_shape(column, ordinal):
    kind = column.type
    collation_parts = [None, None]
    if isinstance(kind, BigInteger):
        type_name = "bigint"
    elif isinstance(kind, Numeric):
        type_name = f"numeric({kind.precision},{kind.scale})"
    elif isinstance(kind, LargeBinary):
        type_name = "bytea"
    elif isinstance(kind, Text):
        type_name = "text"
        collation_parts = ["pg_catalog", kind.collation or "default"]
    else:
        raise QueryProjectionUnavailable("projection model column type is unsupported")
    return [ordinal, column.name, type_name, not column.nullable, "", "", None, *collation_parts]


def _expected_columns(table):
    return [_column_shape(column, ordinal) for ordinal, column in enumerate(table.c, 1)]


async def projection_table_identity(session, family_id, name, definition, *, prepare=None):
    """Reject changed native layout before retaining or trusting a table signature."""

    tables = query_projection_tables(family_id, definition)
    if type(name) is not str or name not in QUERY_PROJECTION_TABLE_NAMES:
        raise QueryProjectionUnavailable("projection relation name is invalid")
    if prepare is not None:
        await prepare(session)
    table_identity = (
        (
            await session.execute(
                _CATALOG_IDENTITY,
                {"schema_name": snapshot_schema(family_id), "table_name": name},
            )
        )
        .mappings()
        .one_or_none()
    )
    if (
        table_identity is None
        or table_identity["relkind"] != "r"
        or table_identity["relpersistence"] != "p"
        or type(table_identity["table_oid"]) is not int
        or table_identity["table_oid"] <= 0
        or type(table_identity["table_owner"]) is not int
        or table_identity["table_owner"] <= 0
        or table_identity["table_owner"] != table_identity["schema_owner"]
        or table_identity["inherited"]
        or table_identity["foreign_keys"]
        or table_identity["triggers"]
        or table_identity["columns"] != _expected_columns(tables[name])
        or table_identity["primary_key"] != list(tables[name].primary_key.columns.keys())
    ):
        raise QueryProjectionUnavailable("projection relation custody or model shape differs")
    return (
        table_identity["table_oid"],
        table_identity["table_owner"],
        _digest(table_identity["columns_sha256"], "column signature"),
    )


def _manifest_shape(manifest, require_complete):
    if type(require_complete) is not bool or manifest.projection_contract != QUERY_PROJECTION_CONTRACT:
        raise QueryProjectionUnavailable("projection contract is invalid")
    for name in (
        "family_id",
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "producing_fence",
        "root_table_oid",
        "root_table_owner",
        "child_table_oid",
        "child_table_owner",
    ):
        identifier = getattr(manifest, name)
        if type(identifier) is not int or not 0 < identifier < 2**63:
            raise QueryProjectionUnavailable("projection owner identity is invalid")
    for name in ("after_entity_binding_id", "root_count", "child_count"):
        accounting_count = getattr(manifest, name)
        if type(accounting_count) is not int or not 0 <= accounting_count < 2**63:
            raise QueryProjectionUnavailable("projection accounting is invalid")
    for name in (
        "producing_token_sha256",
        "definition_sha256",
        "capability_sha256",
        "root_columns_sha256",
        "child_columns_sha256",
    ):
        _digest(getattr(manifest, name), name)
    if manifest.root_table_oid == manifest.child_table_oid:
        raise QueryProjectionUnavailable("projection relation identities repeat")
    if type(manifest.index_count) is not int or not 0 <= manifest.index_count <= 33:
        raise QueryProjectionUnavailable("projection index accounting is invalid")
    _proof_shape(manifest)
    if manifest.phase not in {
        "root",
        "child",
        "indexes",
        "verify_root",
        "verify_child",
        "digest_root",
        "digest_child",
        "complete",
    }:
        raise QueryProjectionUnavailable("projection phase is invalid")
    if manifest.complete_at is None:
        if require_complete or manifest.phase == "complete" or manifest.projection_sha256 is not None:
            raise QueryProjectionUnavailable("projection is incomplete")
    else:
        if manifest.phase != "complete":
            raise QueryProjectionUnavailable("projection completion phase differs")
        _digest(manifest.projection_sha256, "completion signature")


def _proof_shape(manifest):
    proof_counts = []
    for scope in ("root", "child"):
        for operation in ("verified", "digested"):
            count = getattr(manifest, f"{operation}_{scope}_count")
            if type(count) is not int or not 0 <= count <= getattr(manifest, f"{scope}_count"):
                raise QueryProjectionUnavailable("projection proof accounting is invalid")
            if manifest.phase == "complete" and count != getattr(manifest, f"{scope}_count"):
                raise QueryProjectionUnavailable("projection proof coverage differs")
            proof_counts.append(count)
    if sum(proof_counts) >= 2**63 or manifest.phase in {"root", "child", "indexes"} and any(proof_counts):
        raise QueryProjectionUnavailable("projection proof progress is invalid")
    if manifest.digest_chain_sha256 is not None:
        _digest(manifest.digest_chain_sha256, "content chain")
    elif manifest.phase == "complete" or manifest.digested_root_count or manifest.digested_child_count:
        raise QueryProjectionUnavailable("projection content chain is missing")


def _definition_binding(manifest, definition):
    if not hmac.compare_digest(
        bytes(manifest.definition_sha256), bytes.fromhex(definition.digest)
    ) or not hmac.compare_digest(bytes(manifest.capability_sha256), capability_sha256(definition)):
        raise QueryProjectionUnavailable("projection definition or capability differs")
    if manifest.phase == "complete" and manifest.index_count != query_projection_index_count(definition):
        raise QueryProjectionUnavailable("projection index coverage differs")


def _index_specs(manifest, definition):
    expected_by_name = {}
    for scope, table in zip(
        ("root", "child"), query_projection_tables(manifest.family_id, definition).values(), strict=True
    ):
        keys = ["selected_value"] + (["child_key_sha256"] if scope == "child" else [])
        for column in table.c:
            if column.name.startswith("field_"):
                expected_by_name[f"{table.name}_{column.name}_idx"] = (
                    getattr(manifest, f"{scope}_table_oid"),
                    getattr(manifest, f"{scope}_table_owner"),
                    [*keys, column.name, "entity_binding_id"],
                    [None] * (len(keys) + 2),
                )
    return expected_by_name


async def _binding_index_spec(session, manifest, prepare):
    relation = CustomImportSnapshotRelation
    if prepare is not None:
        await prepare(session)
    custody = (
        await session.execute(
            select(relation.table_oid, relation.table_owner).where(
                relation.family_id == manifest.family_id, relation.relation_slot == 2
            )
        )
    ).one_or_none()
    if custody is None or any(type(value) is not int or not 0 < value < 2**63 for value in custody):
        raise QueryProjectionUnavailable("projection binding relation custody is missing or invalid")
    return (
        *custody,
        ["dataset_id", "adapter_id", "canonical_value", "entity_binding_id"],
        [None, "pg_catalog.default", "pg_catalog.C", None],
    )


async def validate_projection_indexes(session, manifest, definition, *, prepare=None):
    """Require every configured native btree before completion and pinned reads."""

    _definition_binding(manifest, definition)
    expected_by_name = _index_specs(manifest, definition)
    if type(manifest.index_count) is not int or manifest.index_count != query_projection_index_count(definition):
        raise QueryProjectionUnavailable("projection index coverage differs")
    if not expected_by_name:
        return
    expected_by_name[QUERY_PROJECTION_BINDING_INDEX] = await _binding_index_spec(session, manifest, prepare)
    if prepare is not None:
        await prepare(session)
    index_records = (
        (
            await session.execute(
                _CATALOG_INDEXES,
                {
                    "schema_name": snapshot_schema(manifest.family_id),
                    "index_names": list(expected_by_name),
                },
            )
        )
        .mappings()
        .all()
    )
    if len(index_records) != len(expected_by_name):
        raise QueryProjectionUnavailable("projection serving index is missing")
    for index_record in index_records:
        expected = expected_by_name.pop(index_record["index_name"], None)
        if expected is None or not _is_expected_index(index_record, expected):
            raise QueryProjectionUnavailable("projection serving index shape differs")


def _is_expected_index(index_record, expected):
    expected_oid, expected_owner, expected_keys, expected_collations = expected
    return not (
        index_record["table_oid"] != expected_oid
        or index_record["index_owner"] != expected_owner
        or index_record["index_kind"] != "i"
        or index_record["access_method"] != "btree"
        or index_record["key_names"] != expected_keys
        or index_record["key_count"] != len(expected_keys)
        or index_record["attribute_count"] != len(expected_keys)
        or index_record["collation_names"] != expected_collations
        or index_record["uncollated"] != (not any(expected_collations))
        or any(index_record[name] for name in ("unique_index", "primary_index", "exclusion_index"))
        or not all(
            index_record[name]
            for name in (
                "valid",
                "ready",
                "live",
                "plain",
                "complete",
                "default_options",
                "default_ops",
            )
        )
    )


async def _validate_parent(session, manifest, require_complete, prepare):
    family, generation, definition = CustomImportSnapshotFamily, CustomImportGeneration, CustomImportDefinitionRevision
    statement = (
        select(family, generation, definition)
        .join(generation, generation.generation_id == family.generation_id)
        .join(definition, definition.definition_revision_id == family.definition_revision_id)
        .where(family.family_id == manifest.family_id)
        .execution_options(populate_existing=True)
    )
    if prepare is not None:
        await prepare(session)
    parent_records = (await session.execute(statement)).one_or_none()
    if parent_records is None:
        raise QueryProjectionUnavailable("projection snapshot parent is missing")
    parent, generation_row, definition_row = parent_records
    for name in (
        "generation_id",
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "execution_id",
        "producing_fence",
    ):
        if getattr(parent, name) != getattr(manifest, name) or getattr(generation_row, name) != getattr(manifest, name):
            raise QueryProjectionUnavailable("projection producer binding differs")
    for producer_record in (parent, generation_row):
        if not hmac.compare_digest(
            _digest(producer_record.producing_token_sha256, "parent producer"), bytes(manifest.producing_token_sha256)
        ):
            raise QueryProjectionUnavailable("projection producer binding differs")
    if (
        parent.capture_bundle_id != generation_row.capture_bundle_id
        or any(
            getattr(definition_row, name) != getattr(manifest, name)
            for name in ("definition_revision_id", "dataset_id", "schema_revision_id")
        )
        or definition_row.contract_version != "custom-import/v1"
        or not hmac.compare_digest(
            _digest(definition_row.definition_sha256, "parent definition"), bytes(manifest.definition_sha256)
        )
        or (require_complete and parent.frozen_at is None)
    ):
        raise QueryProjectionUnavailable("projection snapshot or definition binding differs")


async def lock_projection_tables(session, manifest, definition, *, require_complete=True, prepare=None):
    """Pin fixed leaves, then revalidate custody without writing in read transactions."""

    if not session.in_transaction():
        raise QueryProjectionUnavailable("projection custody requires an active transaction")
    _manifest_shape(manifest, require_complete)
    _definition_binding(manifest, definition)
    await _validate_parent(session, manifest, require_complete, prepare)
    tables = query_projection_tables(manifest.family_id, definition)
    preparer = postgresql.dialect().identifier_preparer
    names = ",".join(preparer.format_table(table) for table in tables.values())
    if prepare is not None:
        await prepare(session)
    await session.execute(text(f"LOCK TABLE {names} IN ACCESS SHARE MODE"))
    await _validate_parent(session, manifest, require_complete, prepare)
    for scope, name in zip(("root", "child"), QUERY_PROJECTION_TABLE_NAMES, strict=True):
        table_oid, owner, signature = await projection_table_identity(
            session, manifest.family_id, name, definition, prepare=prepare
        )
        if (
            table_oid != getattr(manifest, f"{scope}_table_oid")
            or owner != getattr(manifest, f"{scope}_table_owner")
            or not hmac.compare_digest(signature, bytes(getattr(manifest, f"{scope}_columns_sha256")))
        ):
            raise QueryProjectionUnavailable("projection retained table identity differs")
    if require_complete or manifest.phase == "complete":
        await validate_projection_indexes(session, manifest, definition, prepare=prepare)
    return tables
