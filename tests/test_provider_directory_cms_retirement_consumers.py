# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed retirement successors retain every existing identity and custody check."""

import hashlib
import re
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_cms_retirement_resolution import ORIGINAL_SHA256, retirement_resolution_bodies
from process import network_fhir_source_custody as reader
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_callee_inventory as inventory
from process import provider_directory_import_run_guards as import_guards
from process import provider_directory_source_callable_custody as source
from process import provider_directory_terminal_root_retirement_controls as controls
from tests.test_provider_directory_cms_allocation_guard_contracts import original_guards as original_guards
from tests.test_provider_directory_cms_callee_custody import _catalog_rows, _original_allocation_pins, _verify
from tests.test_provider_directory_cms_callee_inventory import _arguments

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


@pytest.mark.parametrize("name", ORIGINAL_SHA256)
@pytest.mark.parametrize("version", [0, 1])
def test_shared_definition_comparison_accepts_only_reviewed_body_pairs(name, version):
    definition_by_field = dict(proname=name, prosrc=_PAIRS[name][version])
    assert source._is_approved_body(definition_by_field, _SCHEMA, ORIGINAL_SHA256[name])
    definition_by_field["prosrc"] += "\nRETURN NULL;"
    assert not source._is_approved_body(definition_by_field, _SCHEMA, ORIGINAL_SHA256[name])


@pytest.mark.parametrize("name", ORIGINAL_SHA256)
def test_successor_requires_exact_predecessor_contract(name):
    definition_by_field = dict(proname=name, prosrc=_PAIRS[name][1])
    assert not source._is_approved_body(definition_by_field, _SCHEMA, "f" * 64)
    definition_by_field["proname"] = "unreviewed_helper"
    assert not source._is_approved_body(definition_by_field, _SCHEMA, ORIGINAL_SHA256[name])


def _typed_frontier():
    rows = _catalog_rows(_SCHEMA)
    for row in rows:
        name = row["definition"]["proname"]
        if name in _EVIDENCE_NAMES:
            row["definition"]["prosrc"] = _PAIRS[name][1]
    return rows


def test_fixed32_cms_checkers_share_exact_successor_recognition():
    rows = _typed_frontier()
    before = deepcopy(rows)
    assert _verify(rows) == inventory.require_cms_callee_inventory(rows, **_arguments(rows))
    assert rows == before and len(callables._cms_callee_contracts()) == 32


@pytest.mark.parametrize("name", _EVIDENCE_NAMES)
@pytest.mark.parametrize("field", ["prosrc", "proconfig", "proargtypes", "prosecdef", "provolatile", "language"])
def test_native_attributes_still_refuse_after_fingerprint_is_rebound(name, field):
    rows = _typed_frontier()
    row = next(entry for entry in rows if entry["definition"]["proname"] == name)
    if field == "language":
        row[field] = "plpgsql"
    else:
        row["definition"][field] = "unreviewed"
    with pytest.raises(ValueError, match="definition_changed"):
        inventory.require_cms_callee_inventory(rows, **_arguments(rows))


def test_original_allocation_parent_keeps_all_other_pins(original_guards):
    name = "guard_provider_directory_terminal_root_retirement_parent"
    row = next(entry for entry in original_guards if entry["definition"]["proname"] == name)
    row["definition"]["prosrc"] = _PAIRS[name][1]
    pins = _original_allocation_pins(original_guards)
    assert len(callables.require_original_cms_allocation_callees(original_guards, **pins)) == 41
    row["definition"]["proconfig"] = ["search_path=public"]
    with pytest.raises(ValueError, match="definition_changed"):
        callables.require_original_cms_allocation_callees(original_guards, **_original_allocation_pins(original_guards))


@pytest.mark.parametrize("name", [name for name in ORIGINAL_SHA256 if name in reader._REACHABLE_FUNCTIONS])
@pytest.mark.parametrize("prehashed", [False, True])
def test_retained_reader_accepts_typed_bodies_with_original_owner_and_settings(name, prehashed):
    expected = reader._REACHABLE_FUNCTIONS[name]
    entry_by_field = dict(
        proname=name,
        proowner=105,
        prosrc=_PAIRS[name][1],
        lanname=expected[1],
        provolatile=expected[2],
        prosecdef=expected[3],
        proconfig=list(expected[4]) if expected[4] is not None else None,
    )
    if prehashed:
        entry_by_field["body_sha256"] = reader._typed_retirement_digest(name, _SCHEMA)
    reader._require_function(entry_by_field, _SCHEMA, 105)
    for damaged in ({**entry_by_field, "proowner": 999}, {**entry_by_field, "proconfig": ["search_path=public"]}):
        with pytest.raises(reader.FHIRSourceCustodyError):
            reader._require_function(damaged, _SCHEMA, 105)
    entry_by_field["prosrc"] += "\nRETURN NULL;"
    if prehashed:
        entry_by_field["body_sha256"] = hashlib.sha256(
            reader._normalized(
                entry_by_field["prosrc"].replace(reader._identifier(_SCHEMA) + ".", '"__schema__".')
            ).encode()
        ).hexdigest()
    with pytest.raises(reader.FHIRSourceCustodyError):
        reader._require_function(entry_by_field, _SCHEMA, 105)


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
