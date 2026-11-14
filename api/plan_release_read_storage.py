# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Locate plan release binding storage by name and hold the tables each route reads."""

import json
import re

from sqlalchemy import text

from db.plan_release_storage import (
    BINDING_COLUMNS,
    BINDING_TABLE,
    HEADER_COLUMNS,
    LEGACY_BINDING_TABLE,
    PIN_TABLE,
    REVISION_TABLE,
    storage_relation,
)
from process.plan_release_storage_catalog import CATALOG_SQL
from process.plan_release_storage_custody import _document, _require, _verify_header, _verify_payload

_ACCESS_SQL = """SELECT c.relname,c.oid::bigint,c.relowner::bigint,r.oid::bigint AS reader_oid,
 has_table_privilege(r.oid,c.oid,'SELECT') AS readable,
 current_user=session_user AND NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole
   OR r.rolreplication OR r.rolbypassrls)
   AND NOT pg_has_role(r.oid,c.relowner,'SET')
   AND NOT has_schema_privilege(r.oid,c.relnamespace,'CREATE') AS ordinary_reader,
 NOT EXISTS(SELECT FROM aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
   WHERE a.grantee<>c.relowner)
   AND NOT EXISTS(SELECT FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) grant_row
     WHERE a.attrelid=c.oid AND grant_row.grantee<>c.relowner) AS owner_only
 FROM pg_class c JOIN pg_roles r ON r.rolname=current_user
 WHERE c.oid=ANY(ARRAY(SELECT to_regclass(name)::oid FROM unnest($1::text[]) names(name)))
 ORDER BY c.relname"""

_RETAINED_READERS_SQL = """WITH retained_relations AS (
 SELECT to_regclass(name)::oid AS oid FROM unnest($1::text[]) names(name)
), readers(oid) AS (
 SELECT $3::oid
 UNION SELECT a.grantee FROM retained_relations retained JOIN pg_class c ON c.oid=retained.oid
 CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
 WHERE a.grantee<>$2::oid
 UNION SELECT grant_row.grantee FROM retained_relations retained
 JOIN pg_attribute field ON field.attrelid=retained.oid
 CROSS JOIN LATERAL aclexplode(field.attacl) grant_row WHERE grant_row.grantee<>$2::oid
)
SELECT r.oid::bigint,
 NOT (r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls)
 AND NOT pg_has_role(r.oid,$2::oid,'SET')
 AND NOT EXISTS(SELECT FROM retained_relations retained JOIN pg_class c ON c.oid=retained.oid
   WHERE NOT has_table_privilege(r.oid,c.oid,'SELECT')
   OR has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')
   OR has_schema_privilege(r.oid,c.relnamespace,'CREATE')) AS ordinary_reader
 FROM readers JOIN pg_roles r ON r.oid=readers.oid ORDER BY r.oid"""

_LEGACY_AUTHORITY_SQL = """SELECT
 (SELECT count(*)=1 AND bool_and(k.convalidated AND k.conenforced AND NOT k.condeferrable
   AND k.confrelid=header.oid AND k.confdeltype='c' AND k.confupdtype='a' AND k.confmatchtype='s'
   AND k.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=legacy.oid
     AND attname='serving_revision_id')]::smallint[]
   AND k.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=header.oid
     AND attname='serving_revision_id')]::smallint[])
  FROM pg_constraint k WHERE k.conrelid=legacy.oid AND k.contype='f') AS original_fk,
 NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=legacy.oid) AS no_rewrite,
 pin.relkind='r' AND pin.relpersistence='p' AND pin.relowner=legacy.relowner
   AND NOT pin.relrowsecurity AND NOT pin.relforcerowsecurity
   AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=pin.oid OR inhparent=pin.oid)
   AND has_table_privilege(current_user,pin.oid,'SELECT')
   AND NOT has_table_privilege(current_user,pin.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
   AND NOT has_any_column_privilege(current_user,pin.oid,'INSERT,UPDATE,REFERENCES') AS old_pin_reader
 FROM pg_class legacy,pg_class header,pg_class pin
 WHERE legacy.oid=to_regclass($1) AND header.oid=to_regclass($2) AND pin.oid=to_regclass($3)"""


def _verify_retained_family(family, access):
    """The only admitted retained state has one unchanged, bounded legacy leaf."""
    by_name = {relation["relname"]: relation for relation in family}
    _require(len(family) == len(by_name) == 4, "retained_family_changed")
    _require(set(by_name) == {BINDING_TABLE, PIN_TABLE, REVISION_TABLE, LEGACY_BINDING_TABLE}, "retained_roots_changed")
    owner = access[0]["relowner"]
    for relation in family:
        _require(
            relation["relowner"] == owner
            and relation["relpersistence"] == "p"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and relation["native_checks"]
            and relation["direct_acl_closed"],
            "retained_custody_changed",
        )
    for name in (BINDING_TABLE, PIN_TABLE):
        root = by_name[name]
        _require(
            root["relkind"] == "p"
            and not root["parents"]
            and root["inert"]
            and root["partition_key"] == "LIST (serving_revision_id)",
            "retained_parent_changed",
        )
        _verify_payload(root, pin=name == PIN_TABLE)
    header = by_name[REVISION_TABLE]
    _require(header["readers_closed"], "retained_header_reader_changed")
    _verify_header(header)
    legacy = by_name[LEGACY_BINDING_TABLE]
    _require(
        legacy["relkind"] == "r"
        and legacy["relispartition"]
        and legacy["parents"] == [by_name[BINDING_TABLE]["oid"]]
        and legacy["readers_closed"],
        "retained_attachment_changed",
    )
    constraints = _document(legacy["constraints"])
    _verify_payload(
        {**legacy, "constraints": [constraint for constraint in constraints if constraint["type"] != "f"]}, pin=False
    )
    literal = r"'hpserve_[0-9A-HJKMNP-TV-Z]{26}'"
    bound = legacy["bound"] or ""
    checks = [constraint for constraint in constraints if constraint["name"] == "plan_release_legacy_cohort_bound"]
    _require(
        re.fullmatch(rf"FOR VALUES IN \({literal}(?:, {literal})*\)", bound) is not None
        and len(checks) == 1
        and checks[0]["valid"]
        and set(re.findall(literal, checks[0]["definition"])) == set(re.findall(literal, bound)),
        "retained_bound_changed",
    )


async def is_retained_binding_read(fetch, schema):
    """Choose legacy storage only from positive catalog evidence, never a SQL error."""
    roots = [storage_relation(schema, name) for name in (BINDING_TABLE, PIN_TABLE)]
    access = await fetch(_ACCESS_SQL, roots)
    _require(len(access) == 2, "read_roots_missing")
    if all(root_access["readable"] is True for root_access in access):
        return False
    _require(
        all(
            root_access["readable"] is False and root_access["owner_only"] and root_access["ordinary_reader"]
            for root_access in access
        )
        and access[0]["relowner"] == access[1]["relowner"],
        "retained_read_not_admitted",
    )
    legacy = storage_relation(schema, LEGACY_BINDING_TABLE)
    header = storage_relation(schema, REVISION_TABLE)
    old_pin = storage_relation(schema, "ptg2_snapshot_pin")
    # Hold the original relations through encoding; administrator provisioning must wait.
    await fetch(f"SELECT 1 FROM ONLY {legacy}, ONLY {header}, ONLY {old_pin} LIMIT 0")
    access = await fetch(_ACCESS_SQL, roots)
    _require(
        all(
            root_access["owner_only"] and root_access["ordinary_reader"] and not root_access["readable"]
            for root_access in access
        ),
        "retained_read_changed",
    )
    readers = await fetch(_RETAINED_READERS_SQL, [legacy, header], access[0]["relowner"], access[0]["reader_oid"])
    _require(readers and all(reader["ordinary_reader"] for reader in readers), "retained_reader_cohort_changed")
    family = await fetch(
        CATALOG_SQL, [*roots, legacy, header], access[0]["relowner"], [reader["oid"] for reader in readers]
    )
    _verify_retained_family(family, access)
    authority = await fetch(_LEGACY_AUTHORITY_SQL, legacy, header, old_pin)
    _require(len(authority) == 1 and all(authority[0].values()), "retained_legacy_authority_changed")
    return True


_READABLE_SQL = """SELECT pg_catalog.has_table_privilege(pg_catalog.to_regclass(name),'SELECT') AS readable
 FROM unnest(CAST(:roots AS text[])) names(name)"""
# Only the documented LIST-partitioned roots may be partitioned; a read through a root applies only its own
# row security and rules, so their partitions are not checked.
_LOCKED_TABLES_SQL = """SELECT count(*)=jsonb_array_length(CAST(:tables AS jsonb)) AS admitted
 FROM jsonb_to_recordset(CAST(:tables AS jsonb)) requested(name text,parent text,columns jsonb)
 JOIN pg_catalog.pg_class c ON c.oid=pg_catalog.to_regclass(requested.name)
 WHERE c.relpersistence='p' AND NOT (c.relrowsecurity OR c.relforcerowsecurity)
 AND NOT pg_catalog.row_security_active(c.oid)
 AND NOT EXISTS(SELECT FROM pg_catalog.pg_rewrite WHERE ev_class=c.oid)
 AND (c.relkind='p' AND c.relname IN ('plan_release_snapshot_binding','plan_release_snapshot_pin','hp_plan_release_binding')
  OR c.relkind='r' AND c.relam=(SELECT oid FROM pg_catalog.pg_am WHERE amname='heap')
  AND NOT EXISTS(SELECT FROM pg_catalog.pg_inherits WHERE inhparent=c.oid))
 AND c.relispartition=(requested.parent IS NOT NULL)
 AND ARRAY(SELECT inhparent FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid)
  =ARRAY(SELECT pg_catalog.to_regclass(requested.parent)::oid WHERE requested.parent IS NOT NULL)
 AND NOT EXISTS(SELECT FROM jsonb_each_text(coalesce(requested.columns,'{}')) required WHERE NOT EXISTS(
  SELECT FROM pg_catalog.pg_attribute a WHERE a.attrelid=c.oid AND a.attname=required.key AND a.attnum>0
  AND NOT a.attisdropped AND pg_catalog.format_type(a.atttypid,a.atttypmod)=required.value))"""


async def lock_read_tables(session, tables):
    """Share-lock tables by name, then refuse one that filters rows, is not a plain heap or lacks a read column."""
    names = ", ".join("ONLY " + table["name"] for table in tables)
    # The share lock lasts until the read ends, so row level security cannot be enabled meanwhile.
    await session.execute(text(f"LOCK TABLE {names} IN ACCESS SHARE MODE"))
    # pg_class comes from the read snapshot, which can predate the lock; row_security_active sees the current state.
    admitted = await session.execute(text(_LOCKED_TABLES_SQL), {"tables": json.dumps(tables)})
    _require(admitted.scalar_one() is True, "read_table_refused")


async def is_retained_session_read(session, schema):
    """Choose storage by SELECT on both roots, then hold the tables every route reads."""
    roots = [storage_relation(schema, name) for name in (BINDING_TABLE, PIN_TABLE)]
    readable_flags = [row._mapping["readable"] for row in await session.execute(text(_READABLE_SQL), {"roots": roots})]
    _require(len(readable_flags) == 2 and None not in readable_flags, "read_roots_missing")
    _require(readable_flags[0] is readable_flags[1], "retained_read_not_admitted")
    shared = (REVISION_TABLE, "ptg2_snapshot", "ptg2_v3_snapshot_plan_scope", "ptg2_snapshot_pin")
    await lock_read_tables(session, [{"name": storage_relation(schema, name)} for name in shared])
    return readable_flags[0] is False


async def lock_binding_storage(session, schema, *, retained):
    """Hold the native partitioned roots, or the retained legacy leaf and header with the columns they read."""
    tables = [{"name": storage_relation(schema, name)} for name in (BINDING_TABLE, PIN_TABLE)]
    if retained:
        legacy = storage_relation(schema, LEGACY_BINDING_TABLE)
        tables = [
            {
                "name": legacy,
                "parent": tables[0]["name"],
                "columns": {column[0]: column[1] for column in BINDING_COLUMNS},
            },
            {
                "name": storage_relation(schema, REVISION_TABLE),
                "columns": {column[0]: column[1] for column in HEADER_COLUMNS},
            },
        ]
    await lock_read_tables(session, tables)
