# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Data validation, configured PostgreSQL DCL and atomic control transition proofs."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nppes_public_evidence_catalog as catalog
from process import nppes_public_evidence_control_custody as controls
from process.nppes_public_evidence_writer_contract import NppesPublicEvidenceWriterError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _bind_data_checks(monkeypatch, state, policy, connection):
    """Keep before/after DATA validation failures observable through the DCL harness."""

    async def previous_data(actual, schema):
        assert actual is connection and schema == policy.schema and state.guard is not None
        state.phases.append("data_before")
        if state.failure == "before":
            raise RuntimeError("data_before_invalid")

    async def candidate_data(actual, schema, actual_policy):
        assert actual is connection and schema == policy.schema and actual_policy is policy and state.guard is None
        state.phases.append("data_after")
        if state.failure in {"after", "cancel"}:
            error = asyncio.CancelledError if state.failure == "cancel" else RuntimeError
            raise error("data_after_invalid")

    monkeypatch.setattr(catalog, "assert_nppes_admission_catalog", AsyncMock(side_effect=previous_data))
    monkeypatch.setattr(catalog, "assert_nppes_candidate_catalog", AsyncMock(side_effect=candidate_data))


@asynccontextmanager
async def _control_savepoint(state):
    saved = deepcopy((state.guard, state.permissions))
    state.savepoints.append("begin")
    try:
        yield
    except BaseException:
        state.guard, state.permissions = saved
        state.savepoints.append("rollback")
        raise
    else:
        state.savepoints.append("release")


@pytest.fixture
def harness(monkeypatch):
    """Model native DCL effects and savepoint rollback without connecting to PostgreSQL."""
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    state = SimpleNamespace(
        guard=dict(proname=controls.ARCHIVE_VALIDATOR, tgenabled="A", tgdeferrable=True, tginitdeferred=True),
        active=True,
        incomplete=None,
        failure=None,
        sql=[],
        phases=[],
        permissions=[],
        savepoints=[],
    )

    async def execute(statement):
        state.sql.append(statement)
        if statement.startswith(("ALTER ", "REVOKE ", "GRANT ")):
            state.permissions.append(statement)
        if statement.startswith("DROP TRIGGER"):
            state.guard = None
        if state.failure == "grant" and statement.startswith("REVOKE EXECUTE"):
            raise RuntimeError("native_grant_refused")

    async def fetch(statement, *arguments):
        if "WHERE p.oid=to_regprocedure($1)" in statement:
            assert arguments[0].startswith('"source_schema".')
            return [dict(grantee=0, rolname=None), dict(grantee=42, rolname="previous_writer")]
        return [
            dict(grantee=0, rolname=None, column_name=None),
            dict(grantee=42, rolname="previous_writer", column_name="admission_ref"),
        ]

    async def fetchval(statement, *arguments):
        if "to_regclass($1)::oid" in statement:
            return 100
        assert "WHERE NOT EXISTS" in statement
        return state.incomplete is not None and f'"{state.incomplete}"' in statement

    connection = SimpleNamespace(
        is_in_transaction=lambda: state.active,
        execute=AsyncMock(side_effect=execute),
        fetch=AsyncMock(side_effect=fetch),
        fetchrow=AsyncMock(side_effect=lambda *_: deepcopy(state.guard)),
        fetchval=AsyncMock(side_effect=fetchval),
        transaction=lambda: _control_savepoint(state),
    )

    _bind_data_checks(monkeypatch, state, policy, connection)
    return SimpleNamespace(connection=connection, state=state, policy=policy)


async def _adopt(harness):
    return await controls.adopt_nppes_control_custody(
        harness.connection,
        harness.policy.schema,
        harness.policy,
        migration_role="opaque previous migration token",
        namespace_owner_role=object(),
    )


async def test_data_validation_precedes_guard_retirement(harness):
    await _adopt(harness)
    state = harness.state
    assert state.phases == ["data_before", "data_after"]
    assert state.guard is None and state.savepoints == ["begin", "release"]
    assert state.sql[0] == (
        "LOCK TABLE "
        + ",".join(f'"source_schema"."{name}"' for name in sorted(controls.CONTROL_TABLES))
        + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    assert state.sql[-1].startswith("DROP TRIGGER")
    assert sum("WHERE NOT EXISTS" in call.args[0] for call in harness.connection.fetchval.await_args_list) == 2


async def test_native_dcl_closes_exact_configured_targets(harness):
    await _adopt(harness)
    statements = harness.state.sql
    assert {statement for statement in statements if statement.startswith("GRANT")} == {
        f'GRANT SELECT ON TABLE "source_schema"."{name}" TO "source_reader"' for name in controls.CONTROL_TABLES
    }
    assert {statement for statement in statements if statement.startswith("ALTER TABLE")} == {
        f'ALTER TABLE "source_schema"."{name}" OWNER TO "source_owner"' for name in controls.CONTROL_TABLES
    }
    signatures = {
        f'"source_schema"."{name}"()'
        for name in controls.callable_contract.expected_nppes_callable_bodies("source_schema")
        if name != controls.callable_contract.IMMUTABLE
    }
    assert len(signatures) == 8
    assert {statement for statement in statements if statement.startswith("ALTER FUNCTION")} == {
        f'ALTER FUNCTION {signature} OWNER TO "source_owner"' for signature in signatures
    }
    for signature in signatures:
        for role in ("PUBLIC", '"previous_writer"'):
            assert f"REVOKE EXECUTE ON FUNCTION {signature} FROM {role} CASCADE" in statements
    for name in controls.CONTROL_TABLES:
        assert f'REVOKE ALL PRIVILEGES ON TABLE "source_schema"."{name}" FROM PUBLIC CASCADE' in statements
        assert (
            f'REVOKE ALL PRIVILEGES ("admission_ref") ON TABLE "source_schema"."{name}" FROM "previous_writer" CASCADE'
            in statements
        )
    assert all("pg_has_role" not in call.args[0] for call in harness.connection.fetch.await_args_list)


@pytest.mark.parametrize("table", [controls.CONTROL_TABLES[1], controls.CONTROL_TABLES[3]])
async def test_missing_data_receipt_preserves_guard(harness, table):
    harness.state.incomplete = table
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _adopt(harness)
    assert harness.state.guard is not None and harness.state.permissions == []
    assert harness.state.savepoints == ["begin", "rollback"]


@pytest.mark.parametrize("failure", ["before", "grant", "after", "cancel"])
async def test_caught_failure_rolls_back_transition(harness, failure):
    harness.state.failure = failure
    error = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(error):
        await _adopt(harness)
    assert harness.state.guard is not None and harness.state.permissions == []
    assert harness.state.savepoints == ["begin", "rollback"] and harness.state.active
    assert any(statement.startswith("DROP TRIGGER") for statement in harness.state.sql) is (
        failure in {"after", "cancel"}
    )


async def test_replay_revalidates_current_data_without_dcl(harness):
    await _adopt(harness)
    harness.state.sql.clear()
    harness.state.phases.clear()
    await _adopt(harness)
    assert harness.state.phases == ["data_after"]
    assert len(harness.state.sql) == 1 and harness.state.sql[0].startswith("LOCK TABLE")


@pytest.mark.parametrize(
    "field,value",
    [("proname", "unrelated"), ("tgenabled", "D"), ("tgdeferrable", False), ("tginitdeferred", False)],
)
async def test_wrong_data_guard_refuses_transition(harness, field, value):
    harness.state.guard[field] = value
    with pytest.raises(NppesPublicEvidenceWriterError):
        await _adopt(harness)
    assert harness.state.permissions == [] and harness.state.phases == []


@pytest.mark.parametrize("change", ["schema", "transaction"])
async def test_wrong_scope_refuses_before_ddl(harness, change):
    harness.state.active = change != "transaction"
    with pytest.raises(NppesPublicEvidenceWriterError):
        await controls.adopt_nppes_control_custody(
            harness.connection,
            "other_schema" if change == "schema" else harness.policy.schema,
            harness.policy,
            migration_role=None,
            namespace_owner_role=None,
        )
    harness.connection.execute.assert_not_awaited()
