# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attest the installed retirement guards for administrator maintenance."""

import json
import re

from db import migration_provider_directory_first_acquisition_retirement as first
from db import migration_provider_directory_first_retirement_owner_lineage as owner_lineage
from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as guards
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError, schema_name
from process.provider_directory_terminal_root_retirement_native import require_native_retirement_controls


def _function_contract(sql):
    declaration = sql.split("CREATE FUNCTION ", 1)[1].split(")", 1)[0]
    name, arguments = declaration.split("(", 1)
    types = ",".join(argument.split()[-1] for argument in arguments.split(",") if argument.strip())
    body = sql.split("AS $function$\n", 1)[1].rsplit("\n    $function$;", 1)[0]
    return {
        "signature": name + "(" + types + ")",
        "body": re.sub(r"\s+", " ", body).strip(),
        "settings": ["search_path=pg_catalog", *(["TimeZone=UTC"] if "SET TimeZone = 'UTC'" in sql else [])],
        "language": re.search(r"LANGUAGE (\w+)", sql).group(1),
        "volatility": "v" if "VOLATILE" in sql else "s",
    }


def _retirement_contracts(schema, native):
    from db.migration_cms_retirement_resolution import retirement_resolution_bodies

    functions = [
        _function_contract(sql)
        for sql in (
            *(new for _, new in first.replacements(schema)),
            evidence.relation_evidence_function_sql(schema),
            guards.marker_function_sql(schema),
            guards.valid_function_sql(schema),
            guards.run_retired_function_sql(schema),
            guards.child_guard_function_sql(schema),
        )
    ]
    alternate_body_by_signature = {
        _function_contract(old)["signature"]: _function_contract(new)["body"]
        for old, new in owner_lineage.replacements(schema)
    }
    typed_by_function = {
        guards._qf(schema, name): re.sub(r"\s+", " ", bodies[1]).strip()
        for name, bodies in retirement_resolution_bodies(schema).items()
    }
    for function in functions:
        function["alternate_body"] = alternate_body_by_signature.get(function["signature"])
        function["typed_body"] = typed_by_function.get(function["signature"].split("(", 1)[0])
        function["settings_successor"] = approved_cms_routine_settings(function["settings"])[1]
    roots = (
        ("provider_directory_endpoint_dataset", "dataset", guards.PARENT_GUARD),
        ("import_run", "import_run", guards.IMPORT_RUN_GUARD),
        *((table, suffix, guards.CHILD_GUARD) for table, suffix in guards.CHILD_TRIGGER_SUFFIXES.items()),
    )
    triggers = [
        {
            "relation": guards._qf(schema, table),
            "name": f"pd_trr_{suffix}_{action}",
            "kind": kind,
            "function": guards._qf(schema, function) + "()",
        }
        for table, suffix, function in roots
        for action, kind in (("row", 31), ("truncate", 34))
        if not (
            table in native
            and (action == "row" or table == "provider_directory_uhc_flex_practitioner_dataset_resource")
        )
    ]
    return functions, triggers


async def require_retirement_controls(database):
    """Use the existing migration contracts without issuing SOURCE authority."""
    schema = schema_name()
    native = await require_native_retirement_controls(database)
    functions, triggers = _retirement_contracts(schema, native)
    valid = await database.scalar(
        r"""
        SELECT NOT EXISTS (
            SELECT FROM pg_catalog.jsonb_to_recordset(CAST(:functions AS jsonb))
                AS expected(signature text, body text, alternate_body text, typed_body text,
                            settings jsonb, settings_successor jsonb, language text, volatility text)
            LEFT JOIN pg_catalog.pg_proc p ON p.oid=pg_catalog.to_regprocedure(expected.signature)
            LEFT JOIN pg_catalog.pg_language l ON l.oid=p.prolang
            WHERE p.oid IS NULL OR NOT p.prosecdef OR p.prokind<>'f'
               OR p.proisstrict OR p.proretset
               OR (pg_catalog.to_jsonb(p.proconfig) IS DISTINCT FROM expected.settings
                   AND pg_catalog.to_jsonb(p.proconfig) IS DISTINCT FROM expected.settings_successor)
               OR l.lanname<>expected.language OR p.provolatile::text<>expected.volatility
               OR (pg_catalog.btrim(pg_catalog.regexp_replace(p.prosrc,'\s+',' ','g'))<>expected.body
                   AND (expected.alternate_body IS NULL OR
                       pg_catalog.btrim(pg_catalog.regexp_replace(p.prosrc,'\s+',' ','g'))<>expected.alternate_body)
                   AND (expected.typed_body IS NULL OR
                       pg_catalog.btrim(pg_catalog.regexp_replace(p.prosrc,'\s+',' ','g'))<>expected.typed_body))
               OR EXISTS (SELECT FROM pg_catalog.pg_proc overload
                   WHERE overload.pronamespace=p.pronamespace AND overload.proname=p.proname
                     AND overload.oid<>p.oid)
        ) AND NOT EXISTS (
            SELECT FROM pg_catalog.jsonb_to_recordset(CAST(:triggers AS jsonb))
                AS expected(relation text, name text, kind integer, function text)
            LEFT JOIN pg_catalog.pg_trigger t
              ON t.tgrelid=pg_catalog.to_regclass(expected.relation) AND t.tgname=expected.name
            WHERE t.oid IS NULL OR t.tgisinternal OR t.tgenabled<>'A' OR t.tgtype<>expected.kind
               OR t.tgfoid<>pg_catalog.to_regprocedure(expected.function)
               OR t.tgqual IS NOT NULL OR t.tgnargs<>0 OR t.tgargs<>'\x'::bytea
               OR t.tgoldtable IS NOT NULL OR t.tgnewtable IS NOT NULL
               OR t.tgattr::text<>''
               OR t.tgdeferrable OR t.tginitdeferred
        );
        """,
        functions=json.dumps(functions),
        triggers=json.dumps(triggers),
    )
    if valid is not True:
        raise TerminalRootRetirementError("state_invalid")
