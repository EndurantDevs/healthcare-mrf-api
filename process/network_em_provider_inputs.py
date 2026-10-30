# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain bounded native custody receipts for managed provider dependencies."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from uuid import UUID

import asyncpg
from sqlalchemy import ARRAY, BigInteger, Date, DateTime, Float, Integer, Numeric, String, Text
from sqlalchemy.sql.sqltypes import UUID as SQLUUID

from api.plan_pricing_projection_contract import PlanPricingProjectionUnavailable
from api.ptg2_geo_projection import _held_inheritance_sql, validate_projection_dependency_bindings
from db.models import (
    DoctorClinicianAddress,
    GeoZipLookup,
    MRFAddress,
    NPIAddress,
    NPIData,
    NPIDataTaxonomy,
    NUCCTaxonomy,
)
from process.network_address_projection import _identifier
from process.network_custom_address_source import _closed, _require_transaction
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_writer_closure import _protected_owner
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest

_ROLES = (
    "doctor_clinician_address",
    "geo_zip_lookup",
    "mrf_address",
    "npi",
    "npi_address",
    "npi_taxonomy",
    "nucc_taxonomy",
    "tiger.zcta5",
    "tiger.zip_state",
)
_MODELS = dict(
    zip(
        _ROLES[:7],
        (DoctorClinicianAddress, GeoZipLookup, MRFAddress, NPIData, NPIAddress, NPIDataTaxonomy, NUCCTaxonomy),
        strict=True,
    )
)
_MAX_RECEIPT_BYTES = 32768
_RELATION_FIELDS = {"role", "schema_name", "table_name", "generation_id", "relation_oid", "relfilenode"}
_CUSTODY_FIELDS = {"schema_oid", "owner_oid", "owner_role", "shape_sha256", "keys_sha256"}
_LEGACY_CONTROL_SQL = """SELECT c.oid::bigint AS table_oid,pg_catalog.pg_backend_pid() AS backend_pid,
 current_user=session_user AND pg_catalog.current_setting('role')='none' AND r.rolcanlogin
 AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) AS reader,
 NOT EXISTS(SELECT FROM pg_catalog.unnest(ARRAY[n.nspowner,c.relowner]) owner(oid)
   WHERE pg_catalog.pg_has_role(r.oid,owner.oid,'MEMBER') OR pg_catalog.pg_has_role(r.oid,owner.oid,'USAGE')
   OR pg_catalog.pg_has_role(r.oid,owner.oid,'SET')) AS owners_closed,
 NOT pg_catalog.pg_has_role(r.oid,'pg_write_all_data','MEMBER')
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_roles elevated
   WHERE (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
     OR elevated.rolreplication OR elevated.rolbypassrls)
   AND (pg_catalog.pg_has_role(r.oid,elevated.oid,'MEMBER')
     OR pg_catalog.pg_has_role(r.oid,elevated.oid,'SET'))) AS elevated_closed,
 pg_catalog.has_schema_privilege(r.oid,n.oid,'USAGE')
 AND NOT pg_catalog.has_schema_privilege(r.oid,n.oid,'CREATE') AS schema_read_only,
 pg_catalog.has_table_privilege(r.oid,c.oid,'SELECT')
 AND NOT pg_catalog.has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AS table_read_only,
 NOT pg_catalog.has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES') AS columns_read_only,
 c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition AND NOT c.relrowsecurity
 AND NOT c.relforcerowsecurity AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid) AS heap,
 (SELECT pg_catalog.bool_and(coalesce(a.atttypid=expected.type_oid AND a.atttypmod=expected.typmod
     AND NOT a.attisdropped AND a.attgenerated='' AND a.attidentity='',false))
  FROM (VALUES ('candidate_id','pg_catalog.uuid'::pg_catalog.regtype,-1),
    ('schema_name','pg_catalog.varchar'::pg_catalog.regtype,68),
    ('state','pg_catalog.varchar'::pg_catalog.regtype,20),
    ('source_generations','pg_catalog.jsonb'::pg_catalog.regtype,-1),
    ('validation_json','pg_catalog.jsonb'::pg_catalog.regtype,-1)) expected(name,type_oid,typmod)
  LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attname=expected.name AND a.attnum>0)
 AS columns_valid
 FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON c.relnamespace=n.oid
 JOIN pg_catalog.pg_roles r ON r.rolname=current_user
 WHERE n.nspname=$1 AND c.relname='network_membership_candidate'"""


def _fail():
    raise ValueError("Managed provider input custody is unavailable")


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _oid(value):
    if type(value) is not int or not 0 < value < 2**32:
        _fail()


@dataclass(frozen=True)
class EMProviderInputRelation:
    role: str
    schema_name: str
    table_name: str
    generation_id: str
    relation_oid: int
    relfilenode: int

    def __post_init__(self):
        if type(self.role) is not str or self.role not in _ROLES:
            _fail()
        _identifier(self.schema_name)
        _identifier(self.table_name)
        if (
            type(self.generation_id) is not str
            or not 1 <= len(self.generation_id) <= 128
            or any(ord(c) < 32 or ord(c) == 127 for c in self.generation_id)
        ):
            _fail()
        _oid(self.relation_oid)
        _oid(self.relfilenode)


@dataclass(frozen=True)
class EMProviderInputs:
    relations: tuple[EMProviderInputRelation, ...]

    def __post_init__(self):
        if (
            type(self.relations) is not tuple
            or len(self.relations) != len(_ROLES)
            or any(type(item) is not EMProviderInputRelation for item in self.relations)
            or tuple(item.role for item in self.relations) != _ROLES
            or len({item.relation_oid for item in self.relations}) != len(_ROLES)
            or len({(item.schema_name, item.table_name) for item in self.relations}) != len(_ROLES)
        ):
            _fail()

    def relation(self, role):
        """Render one closed role's explicitly retained relation, never a live alias."""
        if type(role) is not str or role not in _ROLES:
            _fail()
        item = self.relations[_ROLES.index(role)]
        return f"{_identifier(item.schema_name)}.{_identifier(item.table_name)}"

    def geo_bindings(self, schema_name):
        """Use the shared closed six-relation geo binding contract."""
        _identifier(schema_name)
        bindings_by_name = {}
        for item in self.relations:
            if item.role in {"npi", "npi_taxonomy", "nucc_taxonomy"}:
                continue
            canonical = item.role if item.role.startswith("tiger.") else f"{schema_name}.{item.role}"
            bindings_by_name[canonical] = {
                key: getattr(item, key) for key in ("schema_name", "table_name", "relation_oid", "relfilenode")
            }
        return validate_projection_dependency_bindings(schema_name, bindings_by_name)


def em_provider_geo_reads_sql(sql, inputs, schema_name):
    """Restrict fixed shared geo fragments to the verified native input heaps."""
    if type(sql) is not str or type(inputs) is not EMProviderInputs:
        _fail()
    inputs.__post_init__()
    for binding in inputs.geo_bindings(schema_name).values():
        relation = f"{_identifier(binding['schema_name'])}.{_identifier(binding['table_name'])}"
        # shortcut: shared emitters use uppercase FROM/JOIN and a trailing space; update when their SQL layout changes.
        sql = sql.replace(f"FROM {relation} ", f"FROM ONLY {relation} ")
        sql = sql.replace(f"JOIN {relation} ", f"JOIN ONLY {relation} ")
    return sql


def _runtime_roles(roles):
    if (
        type(roles) is not tuple
        or not 1 <= len(roles) <= 32
        or any(type(role) is not str for role in roles)
        or tuple(sorted(set(roles))) != roles
    ):
        _fail()
    for role in roles:
        _identifier(role)


def _native_type(column_type):
    if isinstance(column_type, ARRAY):
        return "_" + _native_type(column_type.item_type)
    for kind, name in (
        (BigInteger, "int8"),
        (Integer, "int4"),
        (DateTime, "timestamp"),
        (Date, "date"),
        (Float, "float8"),
        (Numeric, "numeric"),
        (Text, "text"),
        (String, "varchar"),
        (SQLUUID, "uuid"),
    ):
        if isinstance(column_type, kind):
            return name
    _fail()


def _required_columns(role):
    if role in _MODELS:
        return {column.name: _native_type(column.type) for column in _MODELS[role].__table__.columns}
    return (
        {"zcta5ce": "varchar", "the_geom": "geometry"}
        if role == "tiger.zcta5"
        else {"zip": "varchar", "stusps": "varchar"}
    )


_CATALOG_SQL = """SELECT requested.role,namespace.oid::bigint AS schema_oid,
 held.oid::bigint AS relation_oid,pg_relation_filenode(held.oid)::bigint AS relfilenode,
 owner.oid::bigint AS owner_oid,owner.rolname AS owner_role,
 owner.rolcanlogin,owner.rolsuper,owner.rolcreatedb,owner.rolcreaterole,owner.rolreplication,owner.rolbypassrls,
 namespace.nspowner=held.relowner AS same_owner,held.relkind::text AS relkind,
 held.relpersistence::text AS relpersistence,held.relispartition,held.relrowsecurity,held.relforcerowsecurity,
 EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=held.oid) AS has_triggers,
 CASE requested.role WHEN 'tiger.zcta5' THEN {zcta_inheritance}
 WHEN 'tiger.zip_state' THEN {zip_inheritance} ELSE {ordinary_inheritance} END AS inheritance_ok,
 (SELECT CASE WHEN octet_length(jsonb_agg(to_jsonb(shape) ORDER BY shape.attnum)::text)<=65536
 THEN jsonb_agg(to_jsonb(shape) ORDER BY shape.attnum)::text END FROM (
   SELECT a.attnum,a.attname,t.typname AS type_name,tn.nspname AS type_schema,
    a.atttypid::bigint AS type_oid,a.atttypmod,a.attnotnull,
    pg_get_expr(d.adbin,d.adrelid) AS default_expr,
    t.oid=to_regtype('geometry') AS canonical_geometry,
    CASE WHEN t.oid=to_regtype('geometry') THEN postgis_typmod_srid(a.atttypmod) END AS geometry_srid,
    CASE WHEN t.oid=to_regtype('geometry') THEN postgis_typmod_type(a.atttypmod) END AS geometry_type
   FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
   JOIN pg_namespace tn ON tn.oid=t.typnamespace
   LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
   WHERE a.attrelid=held.oid AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum LIMIT 257
 ) shape) AS columns_json,
 (SELECT CASE WHEN octet_length(jsonb_agg(to_jsonb(keys) ORDER BY keys.index_oid)::text)<=16384
 THEN jsonb_agg(to_jsonb(keys) ORDER BY keys.index_oid)::text END FROM (
   SELECT i.indexrelid::bigint AS index_oid,
    ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY k(num,ordinal)
     LEFT JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num
     WHERE k.ordinal<=i.indnkeyatts ORDER BY k.ordinal) AS columns
   FROM pg_index i WHERE i.indrelid=held.oid AND i.indisunique AND i.indisvalid AND i.indisready
     AND i.indislive AND i.indpred IS NULL AND i.indexprs IS NULL ORDER BY i.indexrelid LIMIT 65
 ) keys) AS keys_json
 FROM jsonb_to_recordset($1::jsonb) requested(role text,schema_name text,table_name text)
 LEFT JOIN pg_namespace namespace ON namespace.nspname=requested.schema_name
 LEFT JOIN pg_class held ON held.relnamespace=namespace.oid AND held.relname=requested.table_name
 LEFT JOIN pg_roles owner ON owner.oid=held.relowner ORDER BY requested.role COLLATE "C"
""".format(
    zcta_inheritance=_held_inheritance_sql("tiger.zcta5"),
    zip_inheritance=_held_inheritance_sql("tiger.zip_state"),
    ordinary_inheritance=_held_inheritance_sql("npi"),
)


def _shape(role, catalog_row):
    if any(
        type(catalog_row[key]) is str and len(catalog_row[key].encode()) > maximum
        for key, maximum in (("columns_json", 65536), ("keys_json", 16384))
    ):
        _fail()
    columns = json.loads(catalog_row["columns_json"]) if catalog_row["columns_json"] else []
    keys = json.loads(catalog_row["keys_json"]) if catalog_row["keys_json"] else []
    if not 1 <= len(columns) <= 256 or len(keys) > 64:
        _fail()
    if len(json.dumps(columns).encode()) > 65536 or len(json.dumps(keys).encode()) > 16384:
        _fail()
    by_name = {column["attname"]: column for column in columns}
    for name, native_type in _required_columns(role).items():
        column = by_name.get(name)
        if column is None or column["type_name"] != native_type:
            _fail()
        if native_type == "geometry":
            if (
                column["canonical_geometry"] is not True
                or column["geometry_srid"] != 4269
                or column["geometry_type"] not in {"Polygon", "MultiPolygon"}
            ):
                _fail()
        elif column["type_schema"] != "pg_catalog":
            _fail()
    required_key = {
        "npi": ["npi"],
        "npi_taxonomy": ["npi", "checksum"],
        "nucc_taxonomy": ["code"],
        "geo_zip_lookup": ["zip_code"],
    }.get(role)
    if required_key and not any(key["columns"] == required_key for key in keys):
        _fail()
    return _digest(columns), _digest(keys)


async def prepare_em_provider_inputs(connection, inputs, *, runtime_roles):
    """Capture all nine protected heaps in the caller's repeatable-read snapshot."""
    if type(inputs) is not EMProviderInputs:
        _fail()
    _runtime_roles(runtime_roles)
    await _require_transaction(connection)
    catalog_rows = await connection.fetch(
        _CATALOG_SQL, json.dumps([asdict(relation_input) for relation_input in inputs.relations])
    )
    if len(catalog_rows) != len(_ROLES) or tuple(catalog_row["role"] for catalog_row in catalog_rows) != _ROLES:
        _fail()
    relations = []
    relations_by_owner = {}
    for relation_input, catalog_row in zip(inputs.relations, catalog_rows, strict=True):
        if (
            catalog_row["relation_oid"] != relation_input.relation_oid
            or catalog_row["relfilenode"] != relation_input.relfilenode
            or not _protected_owner(catalog_row)
            or catalog_row["same_owner"] is not True
            or catalog_row["relkind"] != "r"
            or catalog_row["relpersistence"] != "p"
            or catalog_row["relispartition"] is not False
            or catalog_row["relrowsecurity"] is not False
            or catalog_row["relforcerowsecurity"] is not False
            or catalog_row["has_triggers"] is not False
            or catalog_row["inheritance_ok"] is not True
        ):
            _fail()
        _oid(catalog_row["schema_oid"])
        _oid(catalog_row["owner_oid"])
        _identifier(catalog_row["owner_role"])
        shape, keys = _shape(relation_input.role, catalog_row)
        relations.append(
            {
                **asdict(relation_input),
                "schema_oid": catalog_row["schema_oid"],
                "owner_oid": catalog_row["owner_oid"],
                "owner_role": catalog_row["owner_role"],
                "shape_sha256": shape,
                "keys_sha256": keys,
            }
        )
        relations_by_owner.setdefault((catalog_row["schema_oid"], catalog_row["owner_oid"]), []).append(
            relation_input.table_name
        )
    for (schema_oid, owner_oid), names in relations_by_owner.items():
        await _closed(connection, schema_oid, owner_oid, runtime_roles, names)
    receipt_by_field = {
        "component": "network_em_provider_inputs",
        "revision": 1,
        "runtime_roles": list(runtime_roles),
        "relations": relations,
    }
    receipt_by_field["receipt_sha256"] = _digest(receipt_by_field)
    _receipt(receipt_by_field)
    return receipt_by_field


def _receipt(receipt):
    if (
        type(receipt) is not dict
        or set(receipt) != {"component", "revision", "runtime_roles", "relations", "receipt_sha256"}
        or receipt["component"] != "network_em_provider_inputs"
        or type(receipt["revision"]) is not int
        or receipt["revision"] != 1
        or type(receipt["runtime_roles"]) is not list
        or type(receipt["relations"]) is not list
        or len(receipt["relations"]) != len(_ROLES)
    ):
        _fail()
    _runtime_roles(tuple(receipt["runtime_roles"]))
    inputs = []
    for entry in receipt["relations"]:
        if type(entry) is not dict or set(entry) != _RELATION_FIELDS | _CUSTODY_FIELDS:
            _fail()
        inputs.append(EMProviderInputRelation(**{key: entry[key] for key in _RELATION_FIELDS}))
        for key in ("schema_oid", "owner_oid"):
            _oid(entry[key])
        _identifier(entry["owner_role"])
        for key in ("shape_sha256", "keys_sha256"):
            if type(entry[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", entry[key]) is None:
                _fail()
    inputs = EMProviderInputs(tuple(inputs))
    if (
        type(receipt["receipt_sha256"]) is not str
        or receipt["receipt_sha256"]
        != _digest({key: field_value for key, field_value in receipt.items() if key != "receipt_sha256"})
        or len(json.dumps(receipt).encode()) > _MAX_RECEIPT_BYTES
    ):
        _fail()
    return inputs


def _object(value):
    if type(value) is str:
        value = json.loads(value)
    if value is None:
        return {}
    if type(value) is not dict:
        _fail()
    return value


async def record_em_provider_inputs(connection, copy_target, receipt, control_schema=None):
    """Attach only the predeclared receipt to one exact OPEN candidate under its lock."""
    _receipt(receipt)
    if type(copy_target) is not MembershipCopyTarget:
        _fail()
    await _require_transaction(connection)
    namespace = _control_namespace(control_schema)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    _require_open(candidate)
    if _object(candidate["source_generations"]).get("em_provider_inputs") != receipt["receipt_sha256"]:
        _fail()
    validation = _object(candidate["validation_json"])
    if "em_provider_inputs" in validation:
        if validation["em_provider_inputs"] != receipt:
            _fail()
        return receipt
    await connection.execute(
        f"UPDATE {namespace}.network_membership_candidate SET validation_json="
        "coalesce(validation_json,'{}'::jsonb)||jsonb_build_object('em_provider_inputs',$2::jsonb) "
        "WHERE candidate_id=$1 AND state='open'",
        UUID(copy_target.candidate_id),
        json.dumps(receipt, sort_keys=True, separators=(",", ":")),
    )
    return receipt


async def _legacy_em_control_pins(connection, control_schema, backend_pid):
    """Pin current legacy header identity after independent serving acceptance, without seal authority."""
    from process.network_address_artifact import _require_api_read_context
    from process.network_compact_layout import CompactAddressRelation

    _identifier(control_schema)
    if type(backend_pid) is not int or not 0 < backend_pid <= 2147483647:
        _fail()
    await _require_api_read_context(connection, True)
    catalog = await connection.fetchrow(_LEGACY_CONTROL_SQL, control_schema)
    if (
        catalog is None
        or type(catalog["backend_pid"]) is not int
        or catalog["backend_pid"] != backend_pid
        or any(
            catalog[field] is not True
            for field in (
                "reader",
                "owners_closed",
                "elevated_closed",
                "schema_read_only",
                "table_read_only",
                "columns_read_only",
                "heap",
                "columns_valid",
            )
        )
    ):
        _fail()
    _oid(catalog["table_oid"])
    return (CompactAddressRelation(control_schema, "network_membership_candidate", catalog["table_oid"]),)


async def _resolve_em_provider_inputs_api(connection, manifest, control_schema=None):
    """Guard the accepted sealed or legacy candidate header before reading its retained recipe."""
    from process.network_candidate_custody import read_network_control_relation_pins
    from process.network_read_identity import verify_consumed_network_relations

    try:
        if type(manifest) is not PinnedNetworkServingManifest:
            _fail()
        backend_pid = connection.get_server_pid()
        await _require_transaction(connection)
        namespace = _control_namespace(control_schema)
        retained = await resolve_network_serving_manifest(
            connection, generation_id=manifest.generation_id, control_schema=namespace[1:-1], api_read=True
        )
        if type(retained) is not PinnedNetworkServingManifest or retained != manifest:
            _fail()
        control = retained.custody_control
        if control is None:
            if retained.compact_layout is not None:
                _fail()
            pins = await _legacy_em_control_pins(connection, namespace[1:-1], backend_pid)
        elif type(control) is dict:
            pins = await read_network_control_relation_pins(
                connection,
                ("network_membership_candidate",),
                control_schema=namespace[1:-1],
                expected_candidate_table_oid=control["candidate_table_oid"],
            )
        else:
            _fail()
        candidate_header = await connection.fetchrow(
            f"SELECT source_generations,validation_json FROM ONLY {namespace}.network_membership_candidate "
            "WHERE candidate_id=$1 AND schema_name=$2 AND state='published'",
            UUID(manifest.candidate_id),
            manifest.schema_name,
        )
        await verify_consumed_network_relations(connection, pins, expected_backend_pid=backend_pid)
        if candidate_header is None or _object(candidate_header["source_generations"]) != manifest.source_generations:
            _fail()
        receipt = _object(candidate_header["validation_json"]).get("em_provider_inputs")
        inputs = _receipt(receipt)
        if manifest.source_generations.get("em_provider_inputs") != receipt["receipt_sha256"]:
            _fail()
        current = await prepare_em_provider_inputs(connection, inputs, runtime_roles=tuple(receipt["runtime_roles"]))
        if current != receipt:
            _fail()
        return inputs
    except Exception:
        raise PlanPricingProjectionUnavailable("Managed provider inputs are unavailable") from None


async def _resolve_em_provider_inputs_legacy(connection, manifest, control_schema=None):
    """Revalidate an exact retained manifest and its protected native recipe, read-only."""
    try:
        if type(manifest) is not PinnedNetworkServingManifest:
            _fail()
        await _require_transaction(connection)
        retained = await resolve_network_serving_manifest(
            connection, generation_id=manifest.generation_id, control_schema=control_schema
        )
        if retained != manifest:
            _fail()
        row = await connection.fetchrow(
            f"SELECT source_generations,validation_json FROM {_control_namespace(control_schema)}.network_membership_candidate "
            "WHERE candidate_id=$1 AND schema_name=$2 AND state='published'",
            UUID(manifest.candidate_id),
            manifest.schema_name,
        )
        if row is None or _object(row["source_generations"]) != manifest.source_generations:
            _fail()
        receipt = _object(row["validation_json"]).get("em_provider_inputs")
        inputs = _receipt(receipt)
        if manifest.source_generations.get("em_provider_inputs") != receipt["receipt_sha256"]:
            _fail()
        current = await prepare_em_provider_inputs(connection, inputs, runtime_roles=tuple(receipt["runtime_roles"]))
        if current != receipt:
            _fail()
        return inputs
    except ValueError, TypeError, KeyError, asyncpg.PostgresError:
        raise PlanPricingProjectionUnavailable("Managed provider inputs are unavailable") from None


async def resolve_em_provider_inputs(connection, manifest, control_schema=None, *, api_read=False):
    """Revalidate an exact retained manifest and its protected native recipe, read-only."""
    if type(api_read) is not bool:
        raise PlanPricingProjectionUnavailable("Managed provider inputs are unavailable")
    if api_read:
        return await _resolve_em_provider_inputs_api(connection, manifest, control_schema)
    return await _resolve_em_provider_inputs_legacy(connection, manifest, control_schema)


async def resolve_em_provider_inputs_in_session(session, manifest, control_schema=None):
    """Verify the complete retained recipe and bind its nine native inputs to this API transaction."""
    from api.network_read_execution import _original_read_driver, install_network_read_inventory
    from process.network_compact_layout import CompactAddressRelation

    try:
        transaction = session.get_transaction()
        if transaction is None:
            _fail()
        connection, backend_pid = await _original_read_driver(session)
        if session.get_transaction() is not transaction or connection.is_in_transaction() is not True:
            _fail()
        inputs = await resolve_em_provider_inputs(connection, manifest, control_schema, api_read=True)
        current_driver, current_pid = await _original_read_driver(session)
        if (
            current_driver is not connection
            or current_pid != backend_pid
            or session.get_transaction() is not transaction
            or connection.is_in_transaction() is not True
            or type(inputs) is not EMProviderInputs
        ):
            _fail()
        await install_network_read_inventory(
            session,
            tuple(
                CompactAddressRelation(relation.schema_name, relation.table_name, relation.relation_oid)
                for relation in inputs.relations
            ),
        )
        current_driver, current_pid = await _original_read_driver(session)
        if (
            current_driver is not connection
            or current_pid != backend_pid
            or session.get_transaction() is not transaction
            or connection.is_in_transaction() is not True
        ):
            _fail()
        return inputs
    except Exception:
        raise PlanPricingProjectionUnavailable("Managed provider inputs are unavailable") from None


async def verify_candidate_em_provider_inputs(connection, candidate, *, runtime_roles):
    """Verify a declared candidate recipe before any publication or retained replay."""
    from process.network_membership_validation import _em_provider_receipt

    receipt = _em_provider_receipt(candidate)
    if receipt is None:
        return None
    _runtime_roles(runtime_roles)
    if list(runtime_roles) != receipt["runtime_roles"]:
        _fail()
    try:
        current = await prepare_em_provider_inputs(connection, _receipt(receipt), runtime_roles=runtime_roles)
    except asyncpg.PostgresError as error:
        raise ValueError("Managed provider input custody is unavailable") from error
    if current != receipt:
        _fail()
    return receipt
