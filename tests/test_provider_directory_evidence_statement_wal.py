# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Generated evidence DML and actual retained worker path; native acceptance pending."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile as profile
from process import provider_directory_profile_target_wal_witness as witness
from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir, owned_wave, run_wave
from tests.test_provider_directory_profile_target_wal_witness import FakeDatabase


def plan(rows=3, conflicts=2, **changes):
    return [
        {
            "Plan": {
                "Node Type": "ModifyTable",
                "Operation": "Insert",
                "Actual Loops": 1,
                "Actual Rows": 0.0,
                "Tuples Inserted": rows,
                "Conflicting Tuples": conflicts,
                "Conflict Resolution": "NOTHING",
                "WAL Records": 2,
                "WAL FPI": 1,
                "WAL Bytes": 96,
                "Plans": [{"Node Type": "Result", "WAL Records": 1, "WAL FPI": 0, "WAL Bytes": 24}],
                **changes,
            }
        }
    ]


def sql(fact="name", buckets=1):
    return profile.profile_evidence_insert_sql(
        target_ref='"fixture"."evidence"',
        source_ref='"fixture"."source"',
        practitioner_ref='"fixture"."practitioner"',
        role_ref='"fixture"."role"',
        organization_ref='"fixture"."organization"',
        service_ref='"fixture"."service"',
        fact_type=fact,
        role_bucket_count=buckets,
        role_bucket=0,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fact", profile.PROFILE_EVIDENCE_FACT_TYPES)
@pytest.mark.parametrize("buckets", [1, 32])
async def test_every_generated_fact_is_observed_byte_for_byte_without_returning(fact, buckets):
    statement = sql(fact, buckets)
    database = FakeDatabase(plan())
    params_by_name = {"source_ids": ["quotes' remain bound"], "dataset_ids": ["dataset"], "profile_as_of": "2026-01-01"}
    result = await witness.execute_evidence_statement(database, statement, params_by_name)
    assert result.affected_rows == 3 and result.wal_record_bytes == 96
    assert database.calls == [(witness._EXPLAIN + statement, params_by_name)]
    assert "RETURNING" not in database.calls[0][0]


@pytest.mark.asyncio
@pytest.mark.parametrize("rows,conflicts", [(0, 0), (0, 5), (1, 5), (3, 2), (64, 0)])
async def test_inserted_count_is_not_zero_actual_rows_or_conflicts(rows, conflicts):
    database = FakeDatabase(plan(rows, conflicts))
    result = await witness.execute_evidence_statement(database, sql(), {})
    assert result.affected_rows == rows


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["Tuples Inserted", "Conflicting Tuples"])
@pytest.mark.parametrize("value", [None, True, -1, "3", 0.5, float("nan"), float("inf"), 2**53])
async def test_missing_or_inexact_tuple_count_is_not_an_affected_count(field, value):
    native = plan(**{field: value})
    database = FakeDatabase(native)
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_evidence_statement(database, sql(), {})
    assert len(database.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "native",
    [
        plan(**{"Conflict Resolution": "UPDATE"}),
        plan(**{"Actual Rows": 3.0}),
        plan(**{"Actual Loops": 2}),
        plan(**{"Plans": [{"Node Type": "Result", "Plans": [{"Node Type": "ModifyTable"}]}]}),
        [{"Plan": plan()[0]["Plan"]}, {"Plan": plan()[0]["Plan"]}],
    ],
)
async def test_non_single_nonreturning_insert_plan_fails_after_one_execution(native):
    database = FakeDatabase(native)
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_evidence_statement(database, sql(), {})
    assert len(database.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO fixture WITH changed AS (DELETE FROM old RETURNING *) SELECT * FROM changed ON CONFLICT (evidence_key) DO NOTHING;",
        "INSERT INTO fixture SELECT 1; INSERT INTO fixture SELECT 2 ON CONFLICT (evidence_key) DO NOTHING;",
        "INSERT INTO fixture SELECT 1 ON CONFLICT (evidence_key) DO UPDATE SET x=2;",
        "INSERT INTO fixture SELECT 1 ON CONFLICT (evidence_key) DO NOTHING RETURNING 1;",
    ],
)
async def test_other_mutation_shapes_rejected_before_execution(statement):
    database = FakeDatabase(plan())
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_evidence_statement(database, statement, {})
    assert not database.calls


@pytest.mark.asyncio
async def test_actual_owner_consumes_separate_statement_and_unclassified_residual(owned_wave):
    state, _, admission, *_ = owned_wave
    assert await run_wave(owned_wave, 2) == [1, 1]
    for capture in admission.wal_tracker.owned_evidence_wave_outcomes[0]["workers"]:
        assert capture["statement_measurement"]["wal_record_bytes"] == 96
        assert capture["unclassified_record_residual"] == {
            "scope": "unclassified_transaction_remainder",
            "units": "native_record_bytes",
            "wal_record_bytes": 32,
            "wal_records": 1,
            "wal_fpi": 0,
        }
        assert capture["statement_reconciliation_status"] == "complete"
        assert capture["measurement"]["wal_record_bytes"] == "128"
        assert capture["measurement"]["global_physical_lsn_span"] == 256
        assert not capture["accounting_authority"] and not capture["reservation_refund"]
    assert state.max_checkedout == 1 and fhir._PROFILE_EVIDENCE_STATEMENT_WAL.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("wal_record_bytes", 129), ("wal_records", 4), ("wal_fpi", 2)])
async def test_negative_residual_retains_committed_truth_and_fails_incomplete(owned_wave, monkeypatch, field, value):
    original = witness.execute_evidence_statement

    async def bad_measurement(*args, **kwargs):
        from dataclasses import replace

        return replace(await original(*args, **kwargs), **{field: value})

    monkeypatch.setattr(witness, "execute_evidence_statement", bad_measurement)
    with pytest.raises(RuntimeError, match="owned_capture_incomplete"):
        await run_wave(owned_wave)
    state, _, admission, *_ = owned_wave
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[0]["workers"][0]
    assert capture["committed"] and capture["status"] == "committed_accounting_incomplete"
    assert capture["unclassified_record_residual"] is None
    assert "commit" in state.connections[0].events and not capture["reservation_refund"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["overrun", "postcommit_sample", "unknown_commit"])
async def test_statement_witness_survives_error_without_inventing_residual(owned_wave, mode):
    state, _, admission, *_ = owned_wave
    state.mode = mode
    with pytest.raises(custody.OwnedWalTransactionError):
        await run_wave(owned_wave)
    capture = admission.wal_tracker.owned_evidence_wave_outcomes[0]["workers"][0]
    assert (capture["statement_measurement"] is None) == (mode == "overrun")
    assert capture["unclassified_record_residual"] is None
    if mode == "overrun":
        assert capture["commit_state"] == "rolled_back" and "commit" not in state.connections[0].events
    if mode == "postcommit_sample":
        assert capture["committed"] and capture["status"] == "committed_accounting_incomplete"
    assert not capture["reservation_refund"]


@pytest.mark.asyncio
async def test_projection_overrun_precedes_missing_wal_counters():
    native = plan(rows=3)
    del native[0]["Plan"]["WAL Bytes"]
    database = FakeDatabase(native)
    with pytest.raises(RuntimeError, match="^provider_directory_profile_evidence_projection_exceeded$"):
        await witness.execute_evidence_statement(database, sql(), {}, maximum_rows=2)
    assert len(database.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "reader", "ended", "nested"])
async def test_binding_loss_after_actual_statement_has_no_witness_or_retry(change):
    database = FakeDatabase(plan())

    def alter():
        if change == "binding":
            from types import SimpleNamespace

            database.binding = SimpleNamespace(session=database.session)
        elif change == "reader":
            database.reader = object()
        elif change == "ended":
            database.session.in_transaction = lambda: False
        else:
            database.session.in_nested_transaction = lambda: True

    database.after_execute = alter
    with pytest.raises(witness.TargetStatementWalWitnessUnavailable):
        await witness.execute_evidence_statement(database, sql(), {})
    assert len(database.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [OSError("synthetic executor failure"), asyncio.CancelledError()])
async def test_existing_executor_failure_or_cancellation_propagates(error):
    database = FakeDatabase(error)
    with pytest.raises(type(error)) as failure:
        await witness.execute_evidence_statement(database, sql(), {})
    assert failure.value is error and len(database.calls) == 1


@pytest.mark.asyncio
async def test_zero_insert_actual_owner_returns_zero_and_leaves_whole_records_unclassified(owned_wave, monkeypatch):
    original = Session.execute

    async def zero_insert(self, statement, parameters):
        result = await original(self, statement, parameters)
        native = result.scalar()
        native[0]["Plan"].update(
            {"Tuples Inserted": 0, "Conflicting Tuples": 5, "WAL Records": 0, "WAL FPI": 0, "WAL Bytes": 0}
        )
        return result

    monkeypatch.setattr(Session, "execute", zero_insert)
    assert await run_wave(owned_wave) == [0]
    capture = owned_wave[2].wal_tracker.owned_evidence_wave_outcomes[0]["workers"][0]
    assert capture["statement_measurement"]["affected_rows"] == 0
    assert capture["unclassified_record_residual"]["wal_record_bytes"] == 128
    assert capture["unclassified_record_residual"]["wal_records"] == 3


@pytest.mark.asyncio
async def test_one_owner_cannot_silently_accumulate_multiple_statement_witnesses(owned_wave, monkeypatch):
    original = fhir._execute_profile_evidence_batch

    async def duplicate(*args):
        await original(*args)
        return await original(*args)

    monkeypatch.setattr(fhir, "_execute_profile_evidence_batch", duplicate)
    with pytest.raises(custody.OwnedWalTransactionError) as failure:
        await run_wave(owned_wave)
    assert isinstance(failure.value.__cause__, RuntimeError)
    assert str(failure.value.__cause__) == "provider_directory_profile_evidence_statement_owner_invalid"
    assert owned_wave[0].connections[0].events.count("write") == 1
    assert "commit" not in owned_wave[0].connections[0].events


@pytest.mark.asyncio
async def test_unowned_executor_keeps_status_and_does_not_explain(owned_wave, monkeypatch):
    _, database, _, build, batches, projection, _ = owned_wave
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(database, "status", status)
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=AssertionError("legacy path cannot explain")))
    # This test reaches the statement executor without opening or pretending custody.
    monkeypatch.setattr(fhir, "_assert_evidence_batch_storage", AsyncMock())
    assert await fhir._execute_profile_evidence_batch_statement(build, batches[0], "COPY unused", {}, projection) == 1
    assert status.await_count == 1


@pytest.mark.asyncio
async def test_actual_database_sqlalchemy_borrowed_session_executes_original_sql_once():
    from types import SimpleNamespace

    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

    from db.connection import Database

    database = Database()
    database._database_override = "synthetic"
    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic")
    try:
        async with AsyncSession(bind=engine) as session, session.begin():
            session.execute = AsyncMock(return_value=SimpleNamespace(scalar=lambda: plan(rows=3)))
            async with database.bind_existing_session(session):
                binding = database._transaction_binding()
                result = await witness.execute_evidence_statement(
                    database, sql(), {"source_ids": ["bound"]}, maximum_rows=3
                )
                assert result.affected_rows == 3 and database._transaction_binding() is binding
                assert session.in_transaction() and not session.in_nested_transaction()
                statement, params = session.execute.call_args.args
                assert str(statement) == witness._EXPLAIN + sql() and params == {"source_ids": ["bound"]}
                with pytest.raises(RuntimeError, match="child asyncio task"):
                    await asyncio.create_task(witness.execute_evidence_statement(database, sql(), {}))
                assert session.execute.await_count == 1
    finally:
        await engine.dispose()
    assert database.engine is None and database.session_factory is None


@pytest.mark.asyncio
async def test_missing_tuple_witness_drains_rolled_back_siblings(owned_wave, monkeypatch):
    original = Session.execute

    async def missing_tuple(self, statement, parameters):
        result = await original(self, statement, parameters)
        del result.scalar()[0]["Plan"]["Tuples Inserted"]
        return result

    monkeypatch.setattr(Session, "execute", missing_tuple)
    with pytest.raises(custody.OwnedWalTransactionError):
        await run_wave(owned_wave, 2)
    state, _, admission, *_ = owned_wave
    wave = admission.wal_tracker.owned_evidence_wave_outcomes[0]
    assert wave["expected_workers"] == wave["drained_workers"] == 2
    assert wave["status"] == "accounting_incomplete"
    assert all("commit" not in connection.events and connection.closed for connection in state.connections)
    assert all(capture["unclassified_record_residual"] is None for capture in wave["workers"])
    assert not wave["reservation_refund"]
