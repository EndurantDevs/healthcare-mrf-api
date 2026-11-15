# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite source settings and declaration checks without a database."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_routine_settings as settings
from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _catalog_definition, _parse_statements
from tests.test_provider_directory_cms_transition_routines import _extra_rows, _native_foreign, _raw
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _load

_SCHEMA = "snapshot_schema"
_PREPARATION = "preparation_schema"


@pytest.mark.parametrize(
    "legacy,target",
    [
        (None, None),
        (["search_path=pg_catalog"], ["search_path=pg_catalog, pg_temp"]),
        (["search_path=pg_catalog", "TimeZone=UTC"], ["search_path=pg_catalog, pg_temp", "TimeZone=UTC"]),
        (["search_path=pg_catalog, snapshot_schema"], ["search_path=pg_catalog, snapshot_schema, pg_temp"]),
        (['search_path=pg_catalog, "Mixed_Schema"'], ['search_path=pg_catalog, "Mixed_Schema", pg_temp']),
        (['search_path=pg_catalog, "select"'], ['search_path=pg_catalog, "select", pg_temp']),
    ],
)
def test_exact_source_settings_are_detached(legacy, target):
    before = deepcopy(legacy)
    old, new = settings.approved_cms_routine_settings(legacy)
    assert old == legacy and new == target and legacy == before
    if old is not None:
        old.append("changed")
        new.append("changed")
        assert legacy == before


@pytest.mark.parametrize(
    "legacy",
    [
        [],
        (),
        "search_path=pg_catalog",
        [None],
        ["search_path=pg_temp"],
        ["search_path=pg_catalog, pg_temp"],
        ["search_path=pg_catalog", "timezone=UTC"],
        ["TimeZone=UTC", "search_path=pg_catalog"],
        ["search_path=pg_catalog", "TimeZone=UTC", "jit=off"],
        ["search_path=pg_catalog, public, pg_temp"],
        ["search_path=pg_catalog, public;RESET ALL"],
        ["search_path=pg_catalog, Mixed_Schema"],
        ['search_path=pg_catalog, "bad-name"'],
    ],
)
def test_unknown_or_observed_settings_cannot_become_expectations(legacy):
    with pytest.raises(ValueError, match="source_settings_required"):
        settings.approved_cms_routine_settings(legacy)


def _ptg_rows():
    rows = []
    for oid, (name, kind) in enumerate(
        (("ptg_wave_json_ascii_text_v1", "text"), ("ptg_wave_canonical_json_ascii_v1", "jsonb")), 8000
    ):
        body = _current_functions()[name, kind][-1]
        for caller, old, new, _count in _migration()._calls(_SCHEMA):
            if caller == name:
                body = body.replace(old, new)
        entry = _entry(name + "(" + kind + ")")
        entry["definition"].update(oid=str(oid), prosrc=body, proconfig=["search_path=pg_catalog, " + _SCHEMA])
        row = _raw(entry)
        row["native"]["namespace_identifier"] = _SCHEMA
        rows.append(row)
    return rows


def _null_profile_row(monkeypatch):
    migration = _load("20260730110000_provider_directory_profile_delta.py")
    statements = []
    monkeypatch.setattr(
        migration,
        "op",
        SimpleNamespace(
            create_table=lambda *args, **kwargs: None,
            create_index=lambda *args, **kwargs: None,
            execute=statements.append,
        ),
    )
    migration._create_delta_receipt_table(_SCHEMA)
    declaration = next(statement for statement in statements if "CREATE FUNCTION" in statement)
    name = "provider_directory_profile_delta_receipt_immutable_v1"
    spec = _parse_statements([declaration], _SCHEMA)[name]
    return _raw(dict(definition=_catalog_definition(name, spec, 105, 8002), language=spec[3]))


def _declaration(row):
    native = row["native"]
    definition = native["definition"]
    lines = ["CREATE OR REPLACE FUNCTION fixed_function()\n", " RETURNS boolean\n", " LANGUAGE sql\n"]
    for item in definition["proconfig"] or []:
        if item.startswith("search_path="):
            path = item.removeprefix("search_path=").split(", ")
            lines.append(" SET search_path TO " + ", ".join("'" + part.strip('"') + "'" for part in path) + "\n")
        else:
            assert item == "TimeZone=UTC"
            lines.append(" SET \"TimeZone\" TO 'UTC'\n")
    return "".join(lines) + "AS $function$" + definition["prosrc"] + "$function$\n"


@pytest.fixture
def routines(original_guards, monkeypatch):
    entries = [_raw(row) for row in original_guards]
    entries.extend(
        [*_extra_rows(monkeypatch), *_ptg_rows(), _null_profile_row(monkeypatch), _native_foreign("RI_FKey_check_ins")]
    )
    for entry in entries:
        entry["native"].update(body=_declaration(entry), dependencies=[], acl=[[105, 105, "EXECUTE", False]])
    return sorted(entries, key=lambda entry: entry["oid"])


def _target_rows(routines):
    expected = deepcopy(routines)
    for row in expected:
        native = row["native"]
        old = native["definition"]["proconfig"]
        if native["namespace"][1] == "pg_catalog" or old is None or old[0].endswith(", pg_temp"):
            continue
        native["definition"]["proconfig"] = [old[0] + ", pg_temp", *old[1:]]
        header, delimiter, body = native["body"].partition("\nAS ")
        lines = header.splitlines()
        for index, line in enumerate(lines):
            if line.startswith(" SET search_path TO "):
                lines[index] = line + ", 'pg_temp'"
        native["body"] = "\n".join(lines) + delimiter + body
    return expected


def _terminal(routines):
    return settings.require_cms_terminal_routine_settings(
        routines, catalog_schema=_SCHEMA, catalog_schema_oid=42, preparation_schema=_PREPARATION
    )


def test_complete_source_subset_keeps_native_and_null_exceptions(routines):
    before = deepcopy(routines)
    with pytest.raises(ValueError, match="terminal_settings_required"):
        _terminal(routines)
    target = _target_rows(routines)
    assert _terminal(target) is None and routines == before
    assert [row for row in routines if row["native"]["definition"]["proconfig"] is None]
    assert [row for row in routines if row["native"]["namespace"][1] == "pg_catalog"]


@pytest.mark.parametrize("fault", ["prosrc", "proconfig", "proargtypes", "namespace", "missing_native"])
def test_target_settings_do_not_replace_source_definition_checks(routines, fault):
    rows = _target_rows(routines)
    selected = rows[0]["native"]
    if fault == "namespace":
        selected["namespace"][0] = 99
    elif fault == "missing_native":
        rows[-1]["native"]["definition"]["proconfig"] = ["search_path=pg_catalog, pg_temp"]
    else:
        selected["definition"][fault] = "changed"
    with pytest.raises(ValueError):
        _terminal(rows)


@pytest.mark.parametrize(
    "legacy,target",
    [
        (["search_path=pg_catalog"], " SET search_path TO 'pg_catalog', 'pg_temp'\n"),
        (['search_path=pg_catalog, "Mixed_Schema"'], " SET search_path TO 'pg_catalog', 'Mixed_Schema', 'pg_temp'\n"),
    ],
)
def test_pg18_declaration_changes_only_the_header(legacy, target):
    old, new = settings.approved_cms_routine_settings(legacy)
    original_line = target.replace(", 'pg_temp'", "")
    declaration = (
        "CREATE OR REPLACE FUNCTION fixed()\n" + original_line + "AS $function$\n" + original_line + "$function$\n"
    )
    assert (
        settings._declaration_after(declaration, old, new)
        == "CREATE OR REPLACE FUNCTION fixed()\n" + target + "AS $function$\n" + original_line + "$function$\n"
    )


@pytest.mark.parametrize("damage", ["missing", "duplicate", "body_only", "unknown"])
def test_pg18_declaration_unknown_header_refuses(damage):
    old, new = settings.approved_cms_routine_settings(["search_path=pg_catalog"])
    line = " SET search_path TO 'pg_catalog'\n"
    header = "CREATE OR REPLACE FUNCTION fixed()\n"
    if damage == "duplicate":
        header += line * 2
    elif damage == "unknown":
        header += " SET search_path TO 'public'\n"
    body = "AS $function$" + (line if damage == "body_only" else "") + "$function$\n"
    with pytest.raises(ValueError, match="declaration_settings_changed"):
        settings._declaration_after(header + body, old, new)
