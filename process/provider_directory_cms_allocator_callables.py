# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check three fixed allocator callables; this supplies no CMS write authority.

The caller authenticates expected owners_by_name and pins the migration plan, referenced
relations and their constraints separately. PL/pgSQL dynamic SQL dependencies
are not represented by pg_depend. This read check does not fence later changes.
"""

import json

from db.migration_generic_source_candidates import (
    _cms_canonical_copy_guard_body,
    verify_generic_source_canonical_copy_guard,
)
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_identifier
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings

_SPECS = {
    "pd_dataset_candidate_tables": (
        "(kind text) RETURNS text[]\nLANGUAGE sql IMMUTABLE SET search_path=pg_catalog AS $fn$",
        ("kind",),
        1009,
        "sql",
        "i",
        False,
    ),
    "guard_pd_dataset_candidate_parent": (
        "() RETURNS trigger\nLANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$",
        (),
        2279,
        "plpgsql",
        "v",
        True,
    ),
    "validate_pd_dataset_bulk_relationships": (
        "(requested_dataset text, kind text)\nRETURNS void LANGUAGE plpgsql SET search_path=pg_catalog AS $fn$",
        ("requested_dataset", "kind"),
        2278,
        "plpgsql",
        "v",
        False,
    ),
}
_FUNCTION_SQL = """SELECT to_jsonb(p) AS definition,l.lanname AS language,
 l.lanpltrusted AND (l.laninline=0 OR (SELECT pronamespace='pg_catalog'::regnamespace
 FROM pg_catalog.pg_proc WHERE oid=l.laninline))
 AND (l.lanplcallfoid=0 OR (SELECT pronamespace='pg_catalog'::regnamespace
 FROM pg_catalog.pg_proc WHERE oid=l.lanplcallfoid))
 AND (l.lanvalidator=0 OR (SELECT pronamespace='pg_catalog'::regnamespace
 FROM pg_catalog.pg_proc WHERE oid=l.lanvalidator)) AS trusted_language,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
 FROM pg_catalog.aclexplode(COALESCE(p.proacl,pg_catalog.acldefault('f',p.proowner))) a),'[]'::jsonb) AS acl
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid=p.prolang
 WHERE p.pronamespace=pg_catalog.to_regnamespace($1) AND p.proname=ANY($2::text[]) ORDER BY p.oid"""
_DEPENDENCIES_SQL = """SELECT d.objid::bigint AS oid,d.refclassid::regclass::text AS class,
 d.refobjid::bigint AS target,d.objsubid,d.refobjsubid,d.deptype,
 d.deptype='n' AND d.objsubid=0 AND d.refobjsubid=0 AND (
   d.refclassid='pg_catalog.pg_namespace'::regclass AND d.refobjid=p.pronamespace
   OR d.refclassid='pg_catalog.pg_language'::regclass AND d.refobjid=p.prolang
   OR d.refclassid='pg_catalog.pg_type'::regclass AND d.refobjid=ANY(p.proargtypes::oid[]||p.prorettype)
      AND EXISTS(SELECT FROM pg_catalog.pg_type t WHERE t.oid=d.refobjid
                 AND t.typnamespace='pg_catalog'::regnamespace)) AS trusted
 FROM pg_catalog.pg_depend d JOIN pg_catalog.pg_proc p ON p.oid=d.objid
 WHERE d.classid='pg_catalog.pg_proc'::regclass AND d.objid=ANY($1::oid[])
 ORDER BY d.objid,d.refclassid,d.refobjid,d.refobjsubid,d.deptype"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_allocator_callable_" + code)


def _decoded(value):
    return json.loads(value) if type(value) is str else value


def _source_bodies(schema, preparation_schema):
    """Extract only exact fixed template boundaries, never parse arbitrary SQL."""
    source = candidate_functions(schema)
    bodies_by_name = {}
    for name, (suffix, *_spec) in _SPECS.items():
        prefix = "CREATE FUNCTION " + storage_identifier(schema) + "." + name
        declaration = prefix + suffix
        _require(source.count(prefix + "(") == 1 and source.count(declaration) == 1, "source_template_changed")
        body, separator, remainder = source.split(declaration, 1)[1].partition("$fn$;")
        _require(separator and "$fn$" not in body and remainder.startswith("\n"), "source_template_changed")
        bodies_by_name[name] = body
    bodies_by_name["guard_pd_dataset_candidate_parent"] = _cms_canonical_copy_guard_body(schema, preparation_schema)
    return bodies_by_name


def _expected_properties(name, namespace, owner, body):
    _suffix, arguments, return_type, language, volatility, definer = _SPECS[name]
    return dict(
        proname=name,
        pronamespace=str(namespace),
        proowner=str(owner),
        prokind="f",
        prorettype=str(return_type),
        pronargs=len(arguments),
        proargtypes=["25"] * len(arguments),
        proargnames=list(arguments) or None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=definer,
        proleakproof=False,
        proisstrict=False,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
        provolatile=volatility,
        proparallel="u",
        proconfig=["search_path=pg_catalog"],
        prosrc=body,
    ), language


def _require_definition(row, namespace, owners_by_name, bodies_by_name):
    native = _decoded(row["definition"])
    name = native["proname"]
    _require(name in _SPECS, "function_set_changed")
    expected, language = _expected_properties(name, namespace, owners_by_name[name], bodies_by_name[name])
    _require(
        set(native) == set(expected) | {"oid", "prolang", "proacl"}
        and native["proconfig"] in approved_cms_routine_settings(expected["proconfig"])
        and all(native[key] == value for key, value in expected.items() if key != "proconfig")
        and row["language"] == language
        and row["trusted_language"] is True,
        "definition_changed",
    )
    _require(_decoded(row["acl"]) == [[owners_by_name[name], owners_by_name[name], "EXECUTE", False]], "acl_changed")
    return native


async def require_cms_allocator_callables(
    connection, *, catalog_schema, expected_owner_oids, preparation_schema="hp_snapshot_retention"
):
    """Require fixed definitions and closed owner-only ACLs on an existing transaction.

    Owner OIDs must come from independently authenticated caller custody. Returned
    native identities are evidence, never an issuer record, lease or writer fence.
    Existing bounded metadata limits remain the caller's responsibility.
    """
    storage_identifier(catalog_schema)
    storage_identifier(preparation_schema)
    _require(connection.is_in_transaction() is True and not connection.is_closed(), "transaction_required")
    _require(
        type(expected_owner_oids) is dict
        and set(expected_owner_oids) == set(_SPECS)
        and all(type(oid) is int and 0 < oid < 2**32 for oid in expected_owner_oids.values()),
        "owners_required",
    )
    owners_by_name = dict(expected_owner_oids)
    bodies_by_name = _source_bodies(catalog_schema, preparation_schema)
    await connection.execute("SET LOCAL search_path = pg_catalog, pg_temp")
    _require(await connection.fetchval("SHOW search_path") == "pg_catalog, pg_temp", "search_path_changed")
    namespace = await connection.fetchval("SELECT pg_catalog.to_regnamespace($1)::oid::bigint", catalog_schema)
    function_rows = await connection.fetch(_FUNCTION_SQL, catalog_schema, list(_SPECS))
    _require(len(function_rows) == len(_SPECS), "function_set_changed")
    definitions = [
        _require_definition(function_row, namespace, owners_by_name, bodies_by_name) for function_row in function_rows
    ]
    _require({definition["proname"] for definition in definitions} == set(_SPECS), "function_set_changed")
    dependencies = await connection.fetch(_DEPENDENCIES_SQL, [int(definition["oid"]) for definition in definitions])
    _require(all(dependency["trusted"] is True for dependency in dependencies), "dependency_changed")
    await verify_generic_source_canonical_copy_guard(
        connection, catalog_schema, preparation_schema=preparation_schema, require_cms=True
    )
    return {"functions": definitions, "dependencies": [dict(dependency) for dependency in dependencies]}
