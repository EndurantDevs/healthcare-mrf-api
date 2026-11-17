# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""CMS native data layouts and the existing fixed control grant inventory."""

from sqlalchemy.dialects import postgresql

from db.plan_release_storage import storage_relation

CONTROL_ROOTS = (
    "provider_directory_source",
    "provider_directory_api_endpoint",
    "provider_directory_profile_serving_generation",
    "provider_directory_profile_initial_receipt",
    "provider_directory_profile_delta_receipt",
    "provider_directory_profile_build_checkpoint",
    "entity_address_result_generation",
    "entity_address_geo_assurance_state",
    "address_alias_artifact_state_v1",
    "reference_family_result_generation",
)
_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
_COLUMN_SQL = """SELECT a.attname AS name,format_type(a.atttypid,a.atttypmod) AS type,
 a.attnotnull AS required,a.attidentity::text AS identity,a.attgenerated::text AS generated
 FROM pg_attribute a WHERE a.attrelid=to_regclass(:relation)
 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"""
_INDEX_SQL = """SELECT c.relname AS name,m.amname AS method,i.indisunique AS "unique",
 i.indisprimary AS "primary",i.indisvalid AND i.indisready AND i.indislive AS ready,
 i.indexprs IS NULL AND i.indpred IS NULL AS plain,
 ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(number,position)
 JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.number ORDER BY position) AS columns
 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am m ON m.oid=c.relam
 WHERE i.indrelid=to_regclass(:relation) ORDER BY c.relname"""
_FOREIGN_SQL = """SELECT to_jsonb(k) AS "constraint",
 jsonb_build_array(child.oid::bigint,cn.nspname,child.relname,child.relowner::bigint) AS child,
 jsonb_build_array(parent.oid::bigint,pn.nspname,parent.relname,parent.relowner::bigint) AS parent,
 (SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM pg_trigger t WHERE t.tgconstraint=k.oid) AS hooks
 FROM pg_constraint k JOIN pg_class child ON child.oid=k.conrelid
 JOIN pg_namespace cn ON cn.oid=child.relnamespace JOIN pg_class parent ON parent.oid=k.confrelid
 JOIN pg_namespace pn ON pn.oid=parent.relnamespace WHERE k.contype='f'
 AND (k.conrelid=ANY($1::oid[]) OR k.confrelid=ANY($1::oid[])) ORDER BY k.oid"""
_TYPE_SQL = """SELECT to_jsonb(t) FROM pg_type t WHERE t.typrelid=$1::oid"""
_NATIVE_DEPENDENCIES_SQL = """WITH objects(classid,objid) AS (
 SELECT 'pg_constraint'::regclass,oid FROM pg_constraint WHERE conrelid=$1::oid
 UNION SELECT 'pg_attrdef'::regclass,oid FROM pg_attrdef WHERE adrelid=$1::oid
 UNION SELECT 'pg_class'::regclass,indexrelid FROM pg_index WHERE indrelid=$1::oid)
 SELECT NOT EXISTS(SELECT FROM objects x JOIN pg_depend d ON (d.classid,d.objid)=(x.classid,x.objid)
 LEFT JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND p.oid=d.refobjid
 LEFT JOIN pg_operator o ON d.refclassid='pg_operator'::regclass AND o.oid=d.refobjid
 WHERE (p.oid IS NOT NULL AND p.pronamespace<>'pg_catalog'::regnamespace)
 OR (o.oid IS NOT NULL AND o.oprnamespace<>'pg_catalog'::regnamespace))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_control_" + code)


def _models():
    from db.models import AddressAliasArtifactStateV1
    from db.models.system import (
        EntityAddressResultGeneration,
        ProviderDirectoryAPIEndpoint,
        ProviderDirectoryProfileBuildCheckpoint,
        ProviderDirectoryProfileDeltaReceipt,
        ProviderDirectoryProfileServingGeneration,
        ProviderDirectorySource,
    )

    return {
        model.__tablename__: model
        for model in (
            ProviderDirectorySource,
            ProviderDirectoryAPIEndpoint,
            ProviderDirectoryProfileServingGeneration,
            ProviderDirectoryProfileDeltaReceipt,
            ProviderDirectoryProfileBuildCheckpoint,
            EntityAddressResultGeneration,
            AddressAliasArtifactStateV1,
        )
    }


def _model_columns(model):
    column_spec_by_name = {}
    for column in model.__table__.columns:
        kind = str(column.type.compile(dialect=postgresql.dialect())).lower()
        kind = kind.replace("varchar", "character varying")
        if kind == "timestamp without time zone" or kind == "timestamp":
            kind = "timestamp without time zone"
        if kind == "json":
            kind = "jsonb"
        column_spec_by_name[column.name] = (kind, not column.nullable)
    return column_spec_by_name


def _require_model_columns(model, rows):
    column_spec_by_name = {row["name"]: (row["type"], row["required"]) for row in rows}
    _require(
        column_spec_by_name == _model_columns(model) and len(column_spec_by_name) == len(rows),
        "administrator_shape_required",
    )
    _require(all(row["identity"] == row["generated"] == "" for row in rows), "administrator_shape_required")


def _require_model_indexes(model, index_rows):
    from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

    index_by_name = {index_row["name"]: index_row for index_row in index_rows}
    for constraint in model.__table__.constraints:
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            keys = [column.name for column in constraint.columns]
            _require(
                any(
                    index_row["ready"]
                    and index_row["plain"]
                    and index_row["unique"]
                    and index_row["columns"] == keys
                    and (not isinstance(constraint, PrimaryKeyConstraint) or index_row["primary"])
                    for index_row in index_rows
                ),
                "administrator_index_required",
            )
    for specification in getattr(model, "__my_additional_indexes__", ()):
        name = specification.get("name") or "_".join(specification["index_elements"])
        index_row = index_by_name.get(name)
        _require(
            index_row is not None
            and index_row["ready"]
            and index_row["plain"]
            and index_row["columns"] == list(specification["index_elements"])
            and index_row["method"] == specification.get("using", "btree")
            and index_row["unique"] is bool(specification.get("unique")),
            "administrator_index_required",
        )


async def require_cms_control_model_layout(database, model, schema):
    """Runtime schema checks are read-only; mutable DML remains supported."""
    from process.provider_directory_source_acquisition_custody import (
        ACQUISITION_ROOTS,
        require_acquisition_model_layout,
    )

    if model.__tablename__ in ACQUISITION_ROOTS:
        return await require_acquisition_model_layout(database, model, schema)
    _require(model.__tablename__ in _models() and model is _models()[model.__tablename__], "model_required")
    table = storage_relation(schema, model.__tablename__)
    rows = await database.all(_COLUMN_SQL, relation=table)
    _require_model_columns(model, [dict(row._mapping if hasattr(row, "_mapping") else row) for row in rows])
    indexes = await database.all(_INDEX_SQL, relation=table)
    _require_model_indexes(model, [dict(row._mapping if hasattr(row, "_mapping") else row) for row in indexes])


# Exact post-repair source graph, plus two unchanged historical control callables.


async def _require_native_foreign_functions(connection, hook_oids):
    _require(
        await connection.fetchval(
            "SELECT count(*)=cardinality($1::oid[]) AND COALESCE(bool_and(p.pronamespace='pg_catalog'::regnamespace "
            "AND l.lanname='internal' AND NOT p.prosecdef "
            "AND p.prosrc=p.proname AND p.proname=ANY($2::text[])),true) "
            "FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang WHERE p.oid=ANY($1::oid[])",
            sorted(hook_oids),
            [
                "RI_FKey_" + name
                for name in (
                    "check_ins",
                    "check_upd",
                    "noaction_del",
                    "noaction_upd",
                    "restrict_del",
                    "restrict_upd",
                    "cascade_del",
                    "cascade_upd",
                    "setnull_del",
                    "setnull_upd",
                    "setdefault_del",
                    "setdefault_upd",
                )
            ],
        )
        is True,
        "native_foreign_function_changed",
    )
