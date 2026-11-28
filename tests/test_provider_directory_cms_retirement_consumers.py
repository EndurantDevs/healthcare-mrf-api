# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed retirement successors retain every existing identity and custody check."""

import re
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_cms_retirement_resolution import ORIGINAL_SHA256, retirement_resolution_bodies
from process import network_fhir_source_custody as reader
from process import provider_directory_import_run_guards as import_guards
from process import provider_directory_terminal_root_retirement_controls as controls

_SCHEMA = "snapshot_schema"
_PAIRS = retirement_resolution_bodies(_SCHEMA)
_EVIDENCE_NAMES = tuple(name for name in ORIGINAL_SHA256 if name.endswith("_evidence"))


@pytest.mark.parametrize("version", [0, 1])
def test_import_run_observer_recognizes_only_the_exact_reviewed_successor(version):
    name = "guard_provider_directory_terminal_root_retirement_run"
    body = _PAIRS[name][version]
    assert import_guards._is_approved_source(name, body, _SCHEMA)
    assert not import_guards._is_approved_source(name, body + "\nRETURN NULL;", _SCHEMA)
    assert not import_guards._is_approved_source("ptg_import_wave_retired_run_guard", body, _SCHEMA)


def test_import_run_successor_still_requires_the_frozen_predecessor(monkeypatch):
    name = "guard_provider_directory_terminal_root_retirement_run"
    monkeypatch.setitem(import_guards._FUNCTIONS[name], "source_sha256", "f" * 64)
    assert not import_guards._is_approved_source(name, _PAIRS[name][1], _SCHEMA)


@pytest.mark.parametrize("name", [name for name in ORIGINAL_SHA256 if name in reader._REACHABLE_FUNCTIONS])
def test_retained_reader_accepts_typed_bodies_and_keeps_settings(name):
    expected = reader._REACHABLE_FUNCTIONS[name]
    entry_by_field = dict(
        proname=name,
        proowner=105,
        prosrc=_PAIRS[name][1],
        lanname=expected[0],
        provolatile=expected[1],
        prosecdef=expected[2],
        proconfig=list(expected[3]) if expected[3] is not None else None,
    )
    reader._require_function(entry_by_field, _SCHEMA)
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function({**entry_by_field, "proconfig": ["search_path=public"]}, _SCHEMA)


@pytest.mark.asyncio
async def test_maintenance_contract_adds_exact_typed_bodies_without_dropping_old_contracts(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", _SCHEMA)
    monkeypatch.setattr(controls, "require_native_retirement_controls", AsyncMock(return_value=set()))
    database = SimpleNamespace(scalar=AsyncMock(return_value=True))
    await controls.require_retirement_controls(database)
    invocation = database.scalar.await_args
    assert "expected.typed_body" in invocation.args[0]
    functions, hooks = controls._retirement_contracts(_SCHEMA, set())
    assert hooks and all(function["body"] for function in functions)
    for function in functions:
        name = function["signature"].split("(", 1)[0].rsplit(".", 1)[-1].strip('"')
        expected = re.sub(r"\s+", " ", _PAIRS[name][1]).strip() if name in _PAIRS else None
        assert function["typed_body"] == expected
    current = next(
        function
        for function in functions
        if '"provider_directory_terminal_root_retirement_evidence"(' in function["signature"]
    )
    assert current["alternate_body"] is not None and current["alternate_body"] != current["typed_body"]
