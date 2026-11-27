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

_LEGACY_AUTHORITY_SQL = """SELECT
 (SELECT count(*)=1 AND bool_and(k.convalidated AND k.conenforced AND NOT k.condeferrable
   AND k.confrelid=header.oid AND k.confdeltype='c' AND k.confupdtype='a' AND k.confmatchtype='s'
   AND k.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=legacy.oid
     AND attname='serving_revision_id')]::smallint[]
   AND k.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=header.oid
     AND attname='serving_revision_id')]::smallint[])
  FROM pg_constraint k WHERE k.conrelid=legacy.oid AND k.contype='f') AS original_fk,
 NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=legacy.oid) AS no_rewrite,
 pin.relkind='r' AND pin.relpersistence='p'
   AND NOT pin.relrowsecurity AND NOT pin.relforcerowsecurity
   AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=pin.oid OR inhparent=pin.oid) AS old_pin_shape
 FROM pg_class legacy,pg_class header,pg_class pin
 WHERE legacy.oid=to_regclass($1) AND header.oid=to_regclass($2) AND pin.oid=to_regclass($3)"""


def _verify_retained_family(family):
    """The only admitted retained state has one unchanged, bounded legacy leaf."""
    by_name = {relation["relname"]: relation for relation in family}
    _require(len(family) == len(by_name) == 4, "retained_family_changed")
    _require(set(by_name) == {BINDING_TABLE, PIN_TABLE, REVISION_TABLE, LEGACY_BINDING_TABLE}, "retained_roots_changed")
    for relation in family:
        _require(
            relation["relpersistence"] == "p"
            and not relation["relrowsecurity"]
            and not relation["relforcerowsecurity"]
            and relation["native_checks"],
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
    _verify_header(by_name[REVISION_TABLE])
    legacy = by_name[LEGACY_BINDING_TABLE]
    _require(
        legacy["relkind"] == "r" and legacy["relispartition"] and legacy["parents"] == [by_name[BINDING_TABLE]["oid"]],
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
    """Choose storage by SELECT on both roots, then check the retained family's shape."""
    roots = [storage_relation(schema, name) for name in (BINDING_TABLE, PIN_TABLE)]
    readable_flags = [root["readable"] for root in await fetch(_READABLE_SQL.replace(":roots", "$1"), roots)]
    _require(len(readable_flags) == 2 and None not in readable_flags, "read_roots_missing")
    _require(readable_flags[0] is readable_flags[1], "retained_read_not_admitted")
    if readable_flags[0]:
        return False
    legacy = storage_relation(schema, LEGACY_BINDING_TABLE)
    header = storage_relation(schema, REVISION_TABLE)
    old_pin = storage_relation(schema, "ptg2_snapshot_pin")
    # Hold the original relations through encoding; administrator provisioning must wait.
    await fetch(f"SELECT 1 FROM ONLY {legacy}, ONLY {header}, ONLY {old_pin} LIMIT 0")
    # The owner and reader arguments only feed closure columns this read does not use.
    family = await fetch(CATALOG_SQL, [*roots, legacy, header], 0, [])
    _verify_retained_family(family)
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
