# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic PG18-shaped plans and the existing executor; no native acceptance."""

import asyncio
import copy
import json
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from process import provider_directory_profile_target_wal_witness as witness

_INSERT = (
    "INSERT INTO synthetic_target (key) SELECT key FROM synthetic_stage AS target WHERE key > :lower ORDER BY key;"
)
_DELETE = "DELETE FROM synthetic_target AS target WHERE key > :lower;"


class Stale(ValueError):
    pass


def _plan(operation="insert", affected_rows=3, **changes):
    # Document-shaped synthetic data, not a retained PostgreSQL execution receipt.
    return [
        {
            "Plan": {
                "Node Type": "ModifyTable",
                "Operation": operation.capitalize(),
                "Parallel Aware": False,
                "Async Capable": False,
                "Relation Name": "synthetic_target",
                "Alias": "synthetic_target",
                "Startup Cost": 0.0,
                "Total Cost": 1.0,
                "Plan Rows": 0,
                "Plan Width": 4,
                "Actual Rows": float(affected_rows),
                "Actual Loops": 1,
                "WAL Records": 4,
                "WAL FPI": 2,
                "WAL Bytes": 260,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Parent Relationship": "Outer",
                        "Actual Rows": 99,
                        "Actual Loops": 1,
                        "WAL Records": 2,
                        "WAL FPI": 1,
                        "WAL Bytes": 100,
                    }
                ],
                **changes,
            }
        }
    ]


class FakeDatabase:
    def __init__(self, result):
        self.result = result
        self.session = SimpleNamespace(in_transaction=lambda: True, in_nested_transaction=lambda: False)
        self.binding = SimpleNamespace(session=self.session)
        self.reader = None
        self.calls = []
        self.after_execute = None

    def _transaction_binding(self):
        return self.binding

    def _reader_binding(self):
        return self.reader

    async def scalar(self, sql, **params):
        self.calls.append((sql, params))
        if self.after_execute:
            self.after_execute()
        if isinstance(self.result, BaseException):
            raise self.result
        return self.result


def _fhir(result):
    return SimpleNamespace(db=FakeDatabase(result), ProviderDirectoryArtifactBuildStale=Stale)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation,statement", [("insert", _INSERT), ("delete", _DELETE)])
@pytest.mark.parametrize("encoded", [False, True])
async def test_executes_once_with_bound_values_and_root_only_counters(operation, statement, encoded):
    plan = _plan(operation)
    fhir = _fhir(json.dumps(plan) if encoded else plan)
    value = "quotes' and ; remain a bound value"
    result = await witness.execute_target_statement(fhir, statement, operation, 3, {"lower": value})
    assert result == witness.TargetStatementWalWitness(operation, 3, 4, 2, 260)
    assert fhir.db.calls == [(witness._EXPLAIN + statement[:-1] + " RETURNING 1;", {"lower": value})]
    assert value not in fhir.db.calls[0][0]
    assert result.wal_record_bytes == 260  # Child bytes must not be added.
    with pytest.raises(FrozenInstanceError):
        result.wal_record_bytes = 0


@pytest.mark.asyncio
async def test_zero_row_statement_has_a_closed_zero_witness():
    fhir = _fhir(_plan(affected_rows=0, **{"WAL Records": 0, "WAL FPI": 0, "WAL Bytes": 0}))
    result = await witness.execute_target_statement(fhir, _INSERT, "insert", 0, {"lower": 1})
    assert result == witness.TargetStatementWalWitness("insert", 0, 0, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["Actual Loops", "WAL Records", "WAL FPI", "WAL Bytes"])
@pytest.mark.parametrize("value", [None, -1, True, 1.0, "1", float("nan"), 9223372036854775808])
async def test_rejects_missing_or_nonclosed_native_counters(field, value):
    plan = _plan()
    if value is None:
        del plan[0]["Plan"][field]
    else:
        plan[0]["Plan"][field] = value
    fhir = _fhir(plan)
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        [],
        [{}, {}],
        [{}],
        [{"Plan": []}],
        _plan(**{"Node Type": "Seq Scan"}),
        _plan(**{"Operation": "Delete"}),
        _plan(**{"Actual Loops": 0}),
        _plan(**{"Actual Loops": 2}),
        "{broken",
        '[{"Plan":null}]',
    ],
)
async def test_rejects_unusable_plan_shapes_without_child_fallback(result):
    fhir = _fhir(copy.deepcopy(result))
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
async def test_exact_changed_row_failure_survives_missing_wal_metrics():
    plan = _plan(affected_rows=2)
    del plan[0]["Plan"]["WAL Bytes"]
    fhir = _fhir(plan)
    with pytest.raises(Stale, match="^provider_directory_profile_delta_rowcount_changed$"):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["unbound", "ended", "nested", "reader"])
async def test_rejects_nonpublication_scope_before_executing(scope):
    fhir = _fhir(_plan())
    if scope == "unbound":
        fhir.db.binding = None
    elif scope == "ended":
        fhir.db.session.in_transaction = lambda: False
    elif scope == "nested":
        fhir.db.session.in_nested_transaction = lambda: True
    else:
        fhir.db.reader = SimpleNamespace(session=object())
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert fhir.db.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "ended", "nested", "reader"])
async def test_lost_scope_after_dml_has_no_witness_and_no_second_execution(change):
    fhir = _fhir(_plan())

    def alter_scope():
        if change == "binding":
            fhir.db.binding = SimpleNamespace(session=fhir.db.session)
        elif change == "ended":
            fhir.db.session.in_transaction = lambda: False
        elif change == "nested":
            fhir.db.session.in_nested_transaction = lambda: True
        else:
            fhir.db.reader = SimpleNamespace(session=object())

    fhir.db.after_execute = alter_scope
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("driver_failed"), asyncio.CancelledError()])
async def test_executor_failure_or_cancellation_propagates_without_retry(error):
    fhir = _fhir(error)
    with pytest.raises(type(error)) as caught:
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
    assert caught.value is error
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement,operation,expected",
    [
        (_INSERT, "update", 3),
        (_INSERT, None, 3),
        (_INSERT, [], 3),
        (_INSERT, "insert", True),
        (_INSERT, "insert", -1),
        (_INSERT, "insert", 1.0),
        (_INSERT, "delete", 3),
        (_INSERT[:-1], "insert", 3),
    ],
)
async def test_rejects_out_of_scope_trusted_statement_contract_before_execution(statement, operation, expected):
    fhir = _fhir(_plan())
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, statement, operation, expected, {})
    assert fhir.db.calls == []


@pytest.mark.asyncio
async def test_existing_database_scalar_and_borrowed_sqlalchemy_session_preserve_binding():
    database = Database()
    database._database_override = "synthetic"
    fhir = SimpleNamespace(db=database, ProviderDirectoryArtifactBuildStale=Stale)
    result = SimpleNamespace(scalar=lambda: _plan())
    # A real engine supplies valid bind metadata; mocked execution never connects.
    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic")
    try:
        async with AsyncSession(bind=engine) as session, session.begin():
            session.execute = AsyncMock(return_value=result)
            async with database.bind_existing_session(session):
                binding = database._transaction_binding()
                value = await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
                assert value.wal_record_bytes == 260
                assert database._transaction_binding() is binding
                assert session.in_transaction() and not session.in_nested_transaction()
                sql, params = session.execute.call_args.args
                assert str(sql) == witness._EXPLAIN + _INSERT[:-1] + " RETURNING 1;"
                assert params == {"lower": 1}
                with pytest.raises(RuntimeError, match="child asyncio task"):
                    await asyncio.create_task(
                        witness.execute_target_statement(fhir, _INSERT, "insert", 3, {"lower": 1})
                    )
                assert session.execute.await_count == 1
    finally:
        await engine.dispose()
    assert database.engine is None and database.session_factory is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value", [None, -1, True, "1", 0.5, float("nan"), float("inf"), float("-inf"), float(2**53), 9223372036854775808]
)
async def test_rejects_nonintegral_or_inexact_native_rows(value):
    plan = _plan()
    if value is None:
        del plan[0]["Plan"]["Actual Rows"]
    else:
        plan[0]["Plan"]["Actual Rows"] = value
    fhir = _fhir(plan)
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_target_statement(fhir, _INSERT, "insert", 3, {})
    assert len(fhir.db.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [0, 1, 3, 64])
@pytest.mark.parametrize("encoded", [False, True])
async def test_actual_postgres_integral_float_row_counts(rows, encoded):
    plan = _plan(affected_rows=rows)
    fhir = _fhir(json.dumps(plan) if encoded else plan)
    result = await witness.execute_target_statement(fhir, _INSERT, "insert", rows, {})
    assert type(result.affected_rows) is int and result.affected_rows == rows
    assert len(fhir.db.calls) == 1
