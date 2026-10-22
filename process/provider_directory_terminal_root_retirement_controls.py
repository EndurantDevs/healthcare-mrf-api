# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attest the installed retirement guards for administrator maintenance."""

import json
import re

from db import migration_provider_directory_first_acquisition_retirement as first
from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as guards
from process.provider_directory_terminal_root_retirement_contract import TerminalRootRetirementError, schema_name


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


async def require_retirement_controls(database):
    """Use the existing migration contracts without issuing SOURCE authority."""
    schema = schema_name()
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
    ]
    valid = await database.scalar(
        r"""
        SELECT NOT EXISTS (
            SELECT FROM pg_catalog.jsonb_to_recordset(CAST(:functions AS jsonb))
                AS expected(signature text, body text, settings jsonb, language text, volatility text)
            LEFT JOIN pg_catalog.pg_proc p ON p.oid=pg_catalog.to_regprocedure(expected.signature)
            LEFT JOIN pg_catalog.pg_language l ON l.oid=p.prolang
            WHERE p.oid IS NULL OR NOT p.prosecdef OR p.prokind<>'f'
               OR p.proisstrict OR p.proretset
               OR pg_catalog.to_jsonb(p.proconfig) IS DISTINCT FROM expected.settings
               OR l.lanname<>expected.language OR p.provolatile::text<>expected.volatility
               OR pg_catalog.btrim(pg_catalog.regexp_replace(p.prosrc,'\s+',' ','g'))<>expected.body
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
               OR t.tgqual IS NOT NULL OR t.tgattr::text<>'' OR t.tgnargs<>0
               OR t.tgdeferrable OR t.tginitdeferred
        );
        """,
        functions=json.dumps(functions),
        triggers=json.dumps(triggers),
    )
    if valid is not True:
        raise TerminalRootRetirementError("state_invalid")
