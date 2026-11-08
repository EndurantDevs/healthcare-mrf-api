# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""API metadata verification retains native checks without an eager table-lock pass."""

import pytest

from process import network_candidate_custody as custody
from tests.test_network_candidate_custody import TARGET, seal
from tests.test_network_candidate_custody import boundary as boundary


def _read_modes(monkeypatch, connection, modes=("repeatable read", "on")):
    original = connection.fetchrow

    async def fetchrow(statement, *arguments):
        if statement.startswith("SELECT current_setting('transaction_isolation')"):
            return modes
        return await original(statement, *arguments)

    monkeypatch.setattr(connection, "fetchrow", fetchrow)


async def test_api_metadata_reuses_full_custody_checks_without_explicit_locks(boundary, monkeypatch):
    sealed = await seal(boundary)
    _read_modes(monkeypatch, boundary)
    boundary.calls.clear()
    async with boundary.transaction():
        actual = await custody.verify_network_candidate_read_metadata(boundary, TARGET, control_schema="control")
        assert actual == sealed
    statements = [statement.upper() for statement, _arguments in boundary.calls]
    assert not any(statement.startswith("LOCK TABLE") or "PG_ADVISORY" in statement for statement in statements)
    assert not any(statement.startswith("SET LOCAL ROLE") for statement in statements)


@pytest.mark.parametrize("modes", [("repeatable read", "off"), ("read committed", "on")])
async def test_api_metadata_requires_read_only_repeatable_snapshot(boundary, monkeypatch, modes):
    await seal(boundary)
    _read_modes(monkeypatch, boundary, modes)
    async with boundary.transaction():
        with pytest.raises(custody.NetworkCandidateCustodyError):
            await custody.verify_network_candidate_read_metadata(boundary, TARGET, control_schema="control")


@pytest.mark.parametrize("changed", ["missing_seal", "heap_guards", "index", "evidence", "runtime"])
async def test_api_metadata_keeps_native_refusals(boundary, monkeypatch, changed):
    await seal(boundary)
    _read_modes(monkeypatch, boundary)
    if changed == "missing_seal":
        boundary.seal = None
    elif changed == "heap_guards":
        boundary.heap_safe = False
    elif changed == "index":
        boundary.index = "c" * 64
    elif changed == "evidence":
        boundary.candidate["validation_json"]["changed"] = True
    else:
        boundary.forbidden.add("reader")
    async with boundary.transaction():
        with pytest.raises(custody.NetworkCandidateCustodyError):
            await custody.verify_network_candidate_read_metadata(boundary, TARGET, control_schema="control")


async def test_api_metadata_does_not_accept_operator_mode(boundary):
    with pytest.raises(custody.NetworkCandidateCustodyError):
        await custody._verify(boundary, TARGET, "control", {}, {}, None, operator=True, api_read=True)


async def test_optional_legacy_seal_does_not_remove_read_snapshot_requirement(boundary, monkeypatch):
    await seal(boundary)
    boundary.seal = None
    _read_modes(monkeypatch, boundary)
    async with boundary.transaction():
        assert (
            await custody.verify_network_candidate_read_metadata(
                boundary, TARGET, control_schema="control", required=False
            )
            is None
        )
    _read_modes(monkeypatch, boundary, ("read committed", "on"))
    async with boundary.transaction():
        with pytest.raises(custody.NetworkCandidateCustodyError):
            await custody.verify_network_candidate_read_metadata(
                boundary, TARGET, control_schema="control", required=False
            )
