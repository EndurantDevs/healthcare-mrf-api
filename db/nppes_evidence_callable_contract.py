# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate existing NPPES callables from their immutable migration renderers."""

from __future__ import annotations

import importlib.util
import json
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

from db.plan_release_storage import storage_identifier, storage_relation

ARCHIVE = "validate_public_evidence_nppes_registry_admission_lifecycle_v2"
CHAIN = "validate_public_evidence_nppes_chain_admission"
IMMUTABLE = "guard_public_evidence_immutable_catalog"
_APPEND_TABLES = (
    "public_evidence_source_record",
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
    "public_evidence_nppes_registry_member",
    "public_evidence_nppes_registry_chain_archive",
)
_MIGRATIONS = {
    "foundation": "20260808090000_public_evidence_storage_foundation.py",
    "admission": "20260808220000_public_evidence_nppes_registry_admission.py",
    "lifecycle": "20260809020000_nppes_lifecycle_date_tolerance.py",
}
_FUNCTION_SQL = """SELECT p.oid::bigint AS oid,n.nspname AS schema,p.proname AS name,
 p.proowner::bigint AS owner,o.rolname AS owner_role,
 NOT(o.rolcanlogin OR o.rolsuper OR o.rolcreatedb OR o.rolcreaterole OR o.rolreplication OR o.rolbypassrls)
 AND NOT EXISTS(SELECT FROM pg_roles e WHERE
  (pg_has_role(o.oid,e.oid,'SET') OR pg_has_role(o.oid,e.oid,'USAGE'))
  AND (e.rolsuper OR e.rolcreatedb OR e.rolcreaterole OR e.rolreplication OR e.rolbypassrls)) AS owner_protected,
 l.lanname AS language,p.prokind::text,p.prorettype::bigint,p.pronargs,p.proargtypes::oid[] AS argument_types,
 p.proargnames,p.proargmodes,p.proallargtypes,p.proargdefaults,p.pronargdefaults,p.provariadic::bigint,
 p.prosecdef,p.proconfig,p.prosrc,p.proleakproof,p.proisstrict,p.proretset,p.provolatile::text,p.proparallel::text,
 p.prosupport::oid::bigint AS support,p.probin,p.prosqlbody,p.protrftypes,p.procost,p.prorows,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,a.privilege_type,a.is_grantable)
  ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
  FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a),'[]'::jsonb) AS acl,
 NOT EXISTS(SELECT FROM pg_roles r WHERE NOT r.rolsuper
  AND NOT(pg_has_role(r.oid,p.proowner,'SET') OR pg_has_role(r.oid,p.proowner,'USAGE')
   OR pg_has_role(r.oid,n.nspowner,'SET') OR pg_has_role(r.oid,n.nspowner,'USAGE'))
  AND has_function_privilege(r.oid,p.oid,'EXECUTE')) AS execute_closed
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_roles o ON o.oid=p.proowner
 JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname=$1 AND p.proname=ANY($2::text[]) ORDER BY p.oid"""
_LINK_SQL = """SELECT t.tgfoid::bigint AS function_oid,n.nspname AS schema,c.relname AS table,t.tgname AS name,
 t.tgtype::integer,t.tgenabled::text,t.tgdeferrable,t.tginitdeferred,t.tgisinternal,t.tgparentid::bigint,
 t.tgqual::text,t.tgnargs,t.tgargs,t.tgattr::smallint[] AS columns,t.tgnewtable::text,t.tgoldtable::text
 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE t.tgfoid=ANY($1::oid[]) ORDER BY t.oid"""


def _require(condition):
    if not condition:
        raise ValueError("nppes_callable_contract_changed")


def _migration(key):
    path = Path(__file__).resolve().parents[1] / "alembic" / "versions" / _MIGRATIONS[key]
    spec = importlib.util.spec_from_file_location("_nppes_callable_" + key, path)
    _require(spec is not None and spec.loader is not None)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    statements = []
    module.op = SimpleNamespace(execute=statements.append)
    return module, statements


def _body(statements, schema, name):
    prefix = 'CREATE FUNCTION "' + schema + '"."' + name + '"()'
    matches = [statement for statement in statements if statement.lstrip().startswith(prefix)]
    _require(len(matches) == 1)
    pieces = matches[0].split("$function$")
    _require(len(pieces) == 3 and pieces[2].strip() == ";")
    return pieces[1]


@lru_cache(maxsize=16)
def expected_nppes_callable_bodies(schema):
    """Record only historical helper rendering; never invoke a migration action."""
    storage_identifier(schema)
    foundation, foundation_sql = _migration("foundation")
    foundation._create_guard_function(schema)
    admission, admission_sql = _migration("admission")
    admission._create_admission_validator(schema)
    admission._create_chain_validator(schema)
    for table in _APPEND_TABLES:
        parent = admission._CHAIN if table == admission._CHAIN_ARCHIVE else admission._ADMISSION
        admission._create_append_guard(schema, table, parent, source_only=table == admission._SOURCE)
    lifecycle, _ = _migration("lifecycle")
    archive = _body(admission_sql, schema, lifecycle._OLD_VALIDATOR)
    for old, new in lifecycle._UPGRADE_REPLACEMENTS:
        _require(archive.count(old) == 1 and new not in archive)
        archive = archive.replace(old, new)
    bodies_by_name = {
        IMMUTABLE: _body(foundation_sql, schema, IMMUTABLE),
        ARCHIVE: archive,
        CHAIN: _body(admission_sql, schema, CHAIN),
    }
    bodies_by_name.update(
        {
            ("guard_" + table + "_admission_append")[:63]: _body(
                admission_sql, schema, "guard_" + table + "_admission_append"
            )
            for table in _APPEND_TABLES
        }
    )
    return bodies_by_name


def _expected_function_fields(function_name, bodies):
    return dict(
        language="plpgsql",
        prokind="f",
        prorettype=2279,
        pronargs=0,
        argument_types=[],
        proargnames=None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic=0,
        prosecdef=True,
        proconfig=["search_path=pg_catalog"],
        prosrc=bodies[function_name],
        proleakproof=False,
        proisstrict=False,
        proretset=False,
        provolatile="v",
        proparallel="u",
        support=0,
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
    )


def _function_record(function_row, schema, bodies):
    _require(
        type(function_row["oid"]) is int
        and 0 < function_row["oid"] < 2**32
        and function_row["schema"] == schema
        and function_row["name"] in bodies
    )
    expected_by_field = _expected_function_fields(function_row["name"], bodies)
    _require(all(function_row[key] == expected_value for key, expected_value in expected_by_field.items()))
    _require(
        type(function_row["owner"]) is int
        and function_row["owner"] > 0
        and type(function_row["owner_role"]) is str
        and type(function_row["owner_protected"]) is bool
        and type(function_row["execute_closed"]) is bool
    )
    acl = json.loads(function_row["acl"]) if type(function_row["acl"]) is str else function_row["acl"]
    _require(
        type(acl) is list
        and all(
            type(edge) is list
            and len(edge) == 4
            and type(edge[0]) is int
            and edge[0] >= 0
            and type(edge[1]) is int
            and edge[1] > 0
            and edge[2] == "EXECUTE"
            and type(edge[3]) is bool
            for edge in acl
        )
    )
    return dict(
        oid=function_row["oid"],
        name=function_row["name"],
        signature=storage_relation(schema, function_row["name"]) + "()",
        owner=function_row["owner"],
        owner_role=function_row["owner_role"],
        owner_protected=function_row["owner_protected"],
        execute_closed=function_row["execute_closed"],
        acl=tuple(tuple(edge) for edge in acl),
    )


def _require_links(trigger_rows, schema, mutators, archive_guard_present):
    by_name = {function_record["name"]: function_record["oid"] for function_record in mutators}
    expected_links = {
        (
            by_name[CHAIN],
            schema,
            "public_evidence_nppes_registry_chain_admission",
            "public_evidence_nppes_registry_chain_integrity_guard",
        )
    }
    if archive_guard_present:
        expected_links.add(
            (
                by_name[ARCHIVE],
                schema,
                "public_evidence_nppes_registry_admission",
                "public_evidence_nppes_registry_admission_integrity_guard",
            )
        )
    _require(
        len(trigger_rows) == len(expected_links)
        and {
            (trigger_row["function_oid"], trigger_row["schema"], trigger_row["table"], trigger_row["name"])
            for trigger_row in trigger_rows
        }
        == expected_links
    )
    for trigger_row in trigger_rows:
        _require(
            trigger_row["tgtype"] == 5
            and trigger_row["tgenabled"] == "A"
            and trigger_row["tgdeferrable"] is True
            and trigger_row["tginitdeferred"] is True
            and trigger_row["tgisinternal"] is False
            and trigger_row["tgparentid"] == 0
            and trigger_row["tgqual"] is None
            and trigger_row["tgnargs"] == 0
            and trigger_row["tgargs"] == b""
            and list(trigger_row["columns"]) == []
            and trigger_row["tgnewtable"] is None
            and trigger_row["tgoldtable"] is None
        )


async def capture_nppes_callable_contract(connection, schema, *, archive_guard_present):
    """Return body-proven readonly capabilities and the two exact adoptable mutators.

    The caller authenticates its original transaction and enrolled roles. A
    runtime caller must additionally require each mutator's protected expected
    owner and execute_closed; adoption may close the returned exact records.
    """
    _require(connection.is_in_transaction() is True and type(archive_guard_present) is bool)
    bodies = expected_nppes_callable_bodies(schema)
    rows = await connection.fetch(_FUNCTION_SQL, schema, list(bodies))
    _require(len(rows) == len(bodies) and {row["name"] for row in rows} == set(bodies))
    records = tuple(_function_record(row, schema, bodies) for row in rows)
    _require(len({record["oid"] for record in records}) == len(records))
    mutators = tuple(
        sorted((record for record in records if record["name"] in {ARCHIVE, CHAIN}), key=lambda record: record["name"])
    )
    links = await connection.fetch(_LINK_SQL, [record["oid"] for record in mutators])
    _require_links(links, schema, mutators, archive_guard_present)
    return tuple(sorted(record["oid"] for record in records if record["name"] not in {ARCHIVE, CHAIN})), mutators
