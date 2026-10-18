# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only native catalog controls; no live trigger or guard is changed."""

import json
import os
from copy import deepcopy
from pathlib import Path

from process import network_cms_registry_address_capture as capture

_FORGED_FIELDS = (
    ("custom", "t", "tgisinternal", False),
    ("disabled", "t", "tgenabled", "D"),
    ("replica", "t", "tgenabled", "R"),
    ("always", "t", "tgenabled", "A"),
    ("event", "t", "tgtype", 5),
    ("parent", "t", "tgparentid", 1),
    ("foreign_relation", "t", "tgconstrrelid", 1),
    ("arguments", "t", "tgnargs", 1),
    ("columns", "t", "tgattr", "1"),
    ("old_table", "t", "tgoldtable", "forged"),
    ("new_table", "t", "tgnewtable", "forged"),
    ("constraint", "t", "tgconstraint", 1),
    ("trigger_index", "t", "tgconstrindid", 1),
    ("deferrability", "t", "tgdeferrable", False),
    ("initial_deferral", "t", "tginitdeferred", False),
    ("function", "t", "tgfoid", 1),
    ("constraint_type", "k", "contype", "f"),
    ("constraint_relation", "k", "conrelid", 1),
    ("constraint_namespace", "k", "connamespace", 1),
    ("constraint_index", "k", "conindid", 1),
    ("constraint_parent", "k", "conparentid", 1),
    ("constraint_validation", "k", "convalidated", False),
    ("constraint_deferrability", "k", "condeferrable", False),
    ("index_relation", "i", "indrelid", 1),
    ("nonunique", "i", "indisunique", False),
    ("immediate", "i", "indimmediate", True),
    ("invalid", "i", "indisvalid", False),
    ("unready", "i", "indisready", False),
    ("dead", "i", "indislive", False),
    ("mismatched_primary", "i", "indisprimary", True),
    ("index_owner", "idx", "relowner", 1),
    ("index_namespace", "idx", "relnamespace", 1),
    ("index_kind", "idx", "relkind", "r"),
    ("index_persistence", "idx", "relpersistence", "u"),
    ("index_method", "idx", "relam", 1),
    ("function_namespace", "p", "pronamespace", 1),
    ("function_language", "p", "prolang", 1),
    ("function_body", "p", "prosrc", "forged"),
    ("function_kind", "p", "prokind", "p"),
    ("function_volatility", "p", "provolatile", "s"),
    ("definer", "p", "prosecdef", True),
    ("set_return", "p", "proretset", True),
    ("function_arguments", "p", "pronargs", 1),
    ("function_result", "p", "prorettype", 23),
    ("function_configuration", "p", "proconfig", ["search_path=public"]),
    ("function_library", "p", "probin", "forged"),
)


def _predicate_controls(metadata):
    """Keep native identities intact except for one explicitly forged fact."""
    controls = [{"case": "native", **metadata}]
    primary = deepcopy(metadata)
    primary["k"]["contype"], primary["i"]["indisprimary"] = "p", True
    controls.append({"case": "primary", **primary})
    immediate = deepcopy(metadata)
    immediate["k"]["condeferred"], immediate["t"]["tginitdeferred"] = False, False
    controls.append({"case": "initially_immediate", **immediate})
    for name, family, field, value in _FORGED_FIELDS:
        forged = deepcopy(metadata)
        forged[family][field] = value
        controls.append({"case": name, **forged})
    return controls


def _read_only_predicate_query():
    """Evaluate the actual production predicate over typed, synthetic catalog rows."""
    query = capture._NATIVE_CONSTRAINT_TRIGGERS_SQL
    for table, alias in (
        ("pg_trigger", "t"),
        ("pg_class", "c"),
        ("pg_constraint", "k"),
        ("pg_index", "i"),
        ("pg_class", "idx"),
        ("pg_proc", "p"),
    ):
        original = f"pg_catalog.{table} {alias}\n" if alias == "t" else f"pg_catalog.{table} {alias} ON"
        replacement = f"pg_catalog.jsonb_populate_record(NULL::pg_catalog.{table},document->'{alias}') {alias}"
        query = query.replace(original, replacement + ("\n" if alias == "t" else " ON"))
    return (
        "SELECT document->>'case' AS name,("
        + query
        + ") AS admitted FROM jsonb_array_elements($2::jsonb) controls(document)"
    )


async def verify_native_unique_trigger_predicate(connection, schema_oid):
    """Attest the real native trigger and refuse forged metadata without catalog writes."""
    await capture._require_native_constraint_triggers(connection, schema_oid)
    # Canonical vector text preserves the bounds of empty native catalog vectors.
    raw = await connection.fetchval(
        "SELECT jsonb_build_object('t',to_jsonb(t)||jsonb_build_object('tgattr',t.tgattr::text),"
        "'c',to_jsonb(c),'k',to_jsonb(k),'i',to_jsonb(i),'idx',to_jsonb(idx),"
        "'p',to_jsonb(p)||jsonb_build_object('proargtypes',p.proargtypes::text))::text "
        "FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid "
        "JOIN pg_constraint k ON k.oid=t.tgconstraint JOIN pg_index i ON i.indexrelid=k.conindid "
        "JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_proc p ON p.oid=t.tgfoid "
        "WHERE c.relnamespace=$1::oid AND k.contype='u' AND k.condeferrable AND k.condeferred",
        schema_oid,
    )
    assert raw is not None
    controls = _predicate_controls(json.loads(raw))
    control_results = await connection.fetch(_read_only_predicate_query(), schema_oid, json.dumps(controls))
    expected_by_case = {
        entry["case"]: entry["case"] in {"native", "primary", "initially_immediate"} for entry in controls
    }
    actual_by_case = {control_result["name"]: control_result["admitted"] for control_result in control_results}
    assert len(control_results) == len(expected_by_case)
    assert actual_by_case == expected_by_case, {
        name: admitted for name, admitted in actual_by_case.items() if admitted != expected_by_case[name]
    }
    evidence = os.environ.get("REGISTRY_TRIGGER_PREDICATE_EVIDENCE")
    if evidence:
        Path(evidence).write_text(
            json.dumps(
                {
                    "native": True,
                    "compatible_controls": 3,
                    "forged_refused": len(control_results) - 3,
                    "read_only": True,
                }
            )
            + "\n"
        )
