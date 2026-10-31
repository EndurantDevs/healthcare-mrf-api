# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retirement shares the exact allocator/guard contract without changing writer custody."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_terminal_root_retirement_native as retirement
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_release_checks import allocator_guard_rows, legacy_function_rows


def retirement_catalog(monkeypatch, *, guard_version="v0", additional_execute=False, allocator=True, protected=True):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_schema")
    rows, _, _ = allocator_guard_rows(monkeypatch, guard_version=guard_version, additional_execute=additional_execute)
    names = {*retirement._NAMES, *([retirement._ALLOCATOR] if allocator else [])}
    rows = [row for row in rows if row["definition"]["proname"] in names]
    database = SimpleNamespace(
        all=AsyncMock(return_value=rows),
        first=AsyncMock(return_value=dict(oid=201, owner=104, protected=protected)),
    )
    return database, rows


@pytest.mark.asyncio
@pytest.mark.parametrize("guard_version", ["v0", "v1", "v2"])
@pytest.mark.parametrize("additional_execute", [False, True])
async def test_retirement_accepts_exact_allocator_guard_postimage(monkeypatch, guard_version, additional_execute):
    database, rows = retirement_catalog(monkeypatch, guard_version=guard_version, additional_execute=additional_execute)
    before = deepcopy(rows)
    actual = await retirement._native_functions(database, "source_schema", {103, 104, 105})
    expected_by_oid = {
        int(row["definition"]["oid"]): row["definition"]
        for row in rows
        if row["definition"]["proname"] in retirement._NAMES
    }
    assert actual == expected_by_oid and rows == before


@pytest.mark.asyncio
@pytest.mark.parametrize("allocator", [False, True])
async def test_retirement_owner_only_preimage_needs_no_protected_allocator(monkeypatch, allocator):
    database, _ = retirement_catalog(monkeypatch, allocator=allocator, protected=False)
    actual = await retirement._native_functions(database, "source_schema", {103, 104, 105})
    assert len(actual) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["missing_allocator", "namespace", "allocator_owner", "allocator_body", "allocator_acl", "guard_body", "deny"],
)
async def test_retirement_refuses_unapproved_allocator_guard(monkeypatch, damage):
    database, rows = retirement_catalog(
        monkeypatch,
        guard_version="v1",
        additional_execute=True,
        allocator=damage != "missing_allocator",
        protected=damage != "namespace",
    )
    by_name = {row["definition"]["proname"]: row for row in rows}
    if damage == "allocator_owner":
        by_name[retirement._ALLOCATOR]["definition"]["proowner"] = "105"
        by_name[retirement._ALLOCATOR]["acl"] = [[105, 105, "EXECUTE", False]]
    elif damage in {"allocator_body", "guard_body"}:
        name = retirement._ALLOCATOR if damage == "allocator_body" else retirement._NAMES[0]
        by_name[name]["definition"]["prosrc"] += " RETURN NULL;"
    elif damage == "allocator_acl":
        by_name[retirement._ALLOCATOR]["acl"].append([999, 104, "EXECUTE", False])
    elif damage == "deny":
        by_name[retirement._NAMES[1]]["acl"].append([104, 105, "EXECUTE", False])
    with pytest.raises((ValueError, GenericCandidateError, retirement.TerminalRootRetirementError)):
        await retirement._native_functions(database, "source_schema", {103, 104, 105})


@pytest.mark.asyncio
@pytest.mark.parametrize("field,changed", [(0, 103), (1, 104), (2, "SELECT"), (3, True)])
async def test_retirement_rejects_altered_guard_execute_edge(monkeypatch, field, changed):
    database, rows = retirement_catalog(monkeypatch, additional_execute=True)
    guard = next(row for row in rows if row["definition"]["proname"] == retirement._NAMES[0])
    guard["acl"][-1][field] = changed
    with pytest.raises(GenericCandidateError, match="callable_acl_changed"):
        await retirement._native_functions(database, "source_schema", {103, 104, 105})


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_guard", "missing_deny", "duplicate", "extra_signature"])
async def test_retirement_preserves_exact_guard_inventory(monkeypatch, damage):
    database, _ = retirement_catalog(monkeypatch)
    rows = legacy_function_rows(monkeypatch)
    rows = [row for row in rows if row["definition"]["proname"] in retirement._NAMES]
    if damage in {"missing_guard", "missing_deny"}:
        rows.pop(0 if damage == "missing_deny" else 1)
    elif damage == "duplicate":
        rows.append(deepcopy(rows[0]))
    else:
        rows.append({**rows[0], "signature": "source_schema.unapproved()"})
    database.all.return_value = rows
    with pytest.raises(retirement.TerminalRootRetirementError, match="state_invalid"):
        await retirement._native_functions(database, "source_schema", {103, 104, 105})
