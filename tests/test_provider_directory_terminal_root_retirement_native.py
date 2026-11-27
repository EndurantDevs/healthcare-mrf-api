# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retirement recognizes native roots by relation kind only."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_terminal_root_retirement_native as retirement


def _database(kinds, replication_role="origin"):
    rows = [{"root": root, "relkind": kind} for root, kind in zip(retirement._ROOTS, kinds, strict=True)]
    return SimpleNamespace(all=AsyncMock(return_value=rows), scalar=AsyncMock(return_value=replication_role))


@pytest.mark.asyncio
async def test_legacy_roots_need_no_native_checks(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_schema")
    database = _database(["r"] * len(retirement._ROOTS))
    assert await retirement.require_native_retirement_controls(database) == set()
    database.scalar.assert_not_awaited()
    roots = json.loads(database.all.await_args.kwargs["roots"])
    assert [root["root"] for root in roots] == list(retirement._ROOTS)


@pytest.mark.asyncio
async def test_native_roots_skip_owner_privilege_and_hook_audits(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_schema")
    database = _database(["p"] * len(retirement._ROOTS))
    assert await retirement.require_native_retirement_controls(database) == set(retirement._ROOTS)
    assert database.all.await_count == 1 and database.scalar.await_count == 1
    for removed in ("relowner", "pg_has_role", "has_table_privilege", "pg_trigger", "pg_inherits"):
        assert removed not in database.all.await_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["partial", "replica"])
async def test_partial_conversion_or_replica_session_is_refused(monkeypatch, damage):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_schema")
    kinds = ["p"] * len(retirement._ROOTS)
    if damage == "partial":
        kinds[0] = "r"
    database = _database(kinds, replication_role="replica" if damage == "replica" else "origin")
    with pytest.raises(retirement.TerminalRootRetirementError, match="state_invalid"):
        await retirement.require_native_retirement_controls(database)
