# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact settings successors preserve existing source bodies and native attributes."""

import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_source_custody as reader
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_import_run_guards as run_guards
from process import provider_directory_source_callable_custody as source
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _catalog_rows, _verify
from tests.test_provider_directory_cms_transition_routines import _extra_rows, _raw
from tests.test_provider_directory_cms_transition_routines import _verify as verify_transition
from tests.test_provider_directory_import_run_guards import _migration_statements
from tests.test_provider_directory_source_callable_custody import _retirement_entry

_SCHEMA = "snapshot_schema"
_BAD_SETTINGS = (
    ["search_path=pg_temp, pg_catalog"],
    ["search_path=pg_catalog, public, pg_temp"],
    ["search_path=pg_catalog", "search_path=pg_temp"],
    ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"],
)


def _harden(row):
    definition = row["definition"]
    definition["proconfig"] = approved_cms_routine_settings(definition["proconfig"])[1]


def test_full_fixed32_uses_exact_settings_without_changing_bodies_or_frontier():
    rows = _catalog_rows(_SCHEMA)
    before = deepcopy(rows)
    for row in rows:
        _harden(row)
    assert _verify(rows) and len(rows) == len(callables._cms_callee_contracts()) == 32
    for old, new in zip(before, rows, strict=True):
        assert old == {**new, "definition": {**new["definition"], "proconfig": old["definition"]["proconfig"]}}


def test_complete_existing_frontier_and_invokers_accept_only_the_exact_successor(original_guards, monkeypatch):
    rows = [_raw(row) for row in original_guards] + _extra_rows(monkeypatch)
    for row in rows:
        _harden(row["native"])
    assert verify_transition(rows) is None
    damaged = deepcopy(rows)
    damaged[0]["native"]["definition"]["prosrc"] += "\n"
    with pytest.raises(ValueError, match="definition_changed"):
        verify_transition(damaged)


@pytest.mark.parametrize("settings", _BAD_SETTINGS)
def test_shared_source_definition_refuses_near_miss_settings(settings):
    row = _retirement_entry()
    row["definition"]["proconfig"] = settings
    with pytest.raises(ValueError, match="definition_changed"):
        source._require_definition(row, source._CONTRACTS[row["signature"]], "sample", 201, source._TYPE_OIDS, "sample")


@pytest.mark.parametrize("name", tuple(reader._REACHABLE_FUNCTIONS))
def test_retained_reader_keeps_complete_contract_with_exact_settings_successor(name):
    digest, language, volatility, definer, settings = reader._REACHABLE_FUNCTIONS[name]
    legacy = None if settings is None else list(settings)
    function_by_field = dict(
        proname=name,
        proowner=105,
        body_sha256=digest,
        lanname=language,
        provolatile=volatility,
        prosecdef=definer,
        proconfig=approved_cms_routine_settings(legacy)[1],
    )
    assert reader._require_function(function_by_field, _SCHEMA, 105) is None
    for field, value in (("proowner", 999), ("body_sha256", "f" * 64), ("proconfig", _BAD_SETTINGS[0])):
        with pytest.raises(reader.FHIRSourceCustodyError):
            reader._require_function({**function_by_field, field: value}, _SCHEMA, 105)


def _run_catalog():
    statements, _triggers = _migration_statements(_SCHEMA)
    functions = []
    for oid, (name, spec) in enumerate(run_guards._FUNCTIONS.items(), 1000):
        match = re.search(r"\bAS\s+(\$(?:[a-zA-Z_][a-zA-Z_0-9]*)?\$)", statements[name])
        body = statements[name][match.end() :].rsplit(match[1], 1)[0]
        functions.append(
            SimpleNamespace(
                _mapping={
                    **{key: expected_value for key, expected_value in spec.items() if key != "source_sha256"},
                    **run_guards._FUNCTION_DEFAULTS,
                    "function_oid": oid,
                    "function_name": name,
                    "function_schema": _SCHEMA,
                    "source": body,
                    "settings": approved_cms_routine_settings(spec["settings"])[1]
                    if name
                    in {
                        "guard_provider_directory_terminal_root_retirement_run",
                        "provider_directory_terminal_root_run_retired",
                    }
                    else deepcopy(spec["settings"]),
                }
            )
        )
    by_name = {
        function_record._mapping["function_name"]: function_record._mapping["function_oid"]
        for function_record in functions
    }
    triggers = [
        SimpleNamespace(
            _mapping={
                **run_guards._TRIGGER_DEFAULTS,
                "trigger_name": name,
                "event_mask": kind,
                "function_name": function,
                "function_schema": _SCHEMA,
                "function_oid": by_name[function],
            }
        )
        for name, (kind, function) in run_guards._TRIGGERS.items()
    ]
    return functions, triggers


@pytest.mark.asyncio
async def test_import_run_catalog_preserves_all_thirteen_functions_and_twelve_hooks():
    functions, triggers = _run_catalog()
    relation_by_field = {"schema_name": _SCHEMA, "relation_oid": 11}
    database = SimpleNamespace(all=AsyncMock(side_effect=[functions, triggers]))
    verified = await run_guards.assert_import_run_guards(database, relation_by_field)
    assert len(verified) == 25 and len(functions) == 13 and len(triggers) == 12
    functions[0]._mapping["settings"] = _BAD_SETTINGS[0]
    database.all = AsyncMock(return_value=functions)
    with pytest.raises(RuntimeError, match="guard_shape_changed"):
        await run_guards.assert_import_run_guards(database, relation_by_field)
    assert database.all.await_count == 1


@pytest.mark.asyncio
async def test_import_run_settings_outside_fixed_transition_stay_exact():
    functions, _triggers = _run_catalog()
    original = next(
        record for record in functions if record._mapping["function_name"] == "guard_npi_canonical_publication_run"
    )
    original._mapping["settings"] = ["search_path=pg_catalog, pg_temp"]
    database = SimpleNamespace(all=AsyncMock(return_value=functions))
    with pytest.raises(RuntimeError, match="guard_shape_changed"):
        await run_guards.assert_import_run_guards(database, {"schema_name": _SCHEMA, "relation_oid": 11})
