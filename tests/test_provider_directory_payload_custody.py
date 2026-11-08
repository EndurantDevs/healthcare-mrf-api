# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumed compact and affected-NPI root statements under real native owners."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_owned_compact_wave import compact_wave as compact_wave
from tests.test_provider_directory_owned_compact_wave import run_wave
from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

pytestmark = pytest.mark.asyncio


@pytest.fixture
def affected_wave(control_wave, monkeypatch):
    state, database, admission, build, *_ = control_wave
    build.affected_npi_stage = "synthetic_affected"
    reserve = AsyncMock()
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", reserve)
    monkeypatch.setattr(fhir, "_admit_affected_npi_projection", AsyncMock())
    monkeypatch.setattr(database, "first", AsyncMock(return_value={"projected_rows": 2, "projected_logical_bytes": 16}))

    @asynccontextmanager
    async def window(relation_class, relations):
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), relation_class))
        try:
            yield
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)

    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", window)

    class ReturningSession(Session):
        async def execute(self, statement, parameters):
            assert reserve.await_count == 1
            result = await super().execute(statement, parameters)
            if str(statement).endswith(" RETURNING 1;"):
                result.scalar()[0]["Plan"]["Actual Rows"] = result.rowcount
            return result

    monkeypatch.setattr(custody, "AsyncSession", ReturningSession)
    scalar = AsyncMock(wraps=database.scalar)
    monkeypatch.setattr(database, "scalar", scalar)
    return state, database, admission, build, scalar


async def affected_insert(fixture, *, plain=False, expected_rows=None):
    build = fixture[3]
    statement = (
        "INSERT INTO synthetic_affected SELECT :npi;"
        if plain
        else ("INSERT INTO synthetic_affected SELECT :npi ON CONFLICT (npi) DO NOTHING;")
    )
    return await fhir._execute_affected_npi_insert(
        build,
        projection_sql="SELECT synthetic_projection;",
        insert_sql=statement,
        params={"npi": 17},
        expected_rows=expected_rows,
    )


@pytest.mark.parametrize("plain", [False, True])
async def test_affected_original_owner_one_execution_exact_params_and_terminal_witness(affected_wave, plain):
    state, _, admission, build, scalar = affected_wave
    assert await affected_insert(affected_wave, plain=plain, expected_rows=1) == 1
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    statement_custody = group["statement_custody"]
    outcome = group["original_outcome"]
    assert group["identity"][0] == "affected_npi_payload" and group["identity"][1] is build
    assert statement_custody is group["original_statement_custody"]
    assert statement_custody["outcome"] is outcome and statement_custody["session"] is outcome.session
    assert statement_custody["witness"] is statement_custody["original_witness"]
    assert statement_custody["witness"].affected_rows == 1
    assert group["consumed"] and outcome.status == "committed_measured"
    assert not group["accounting_authority"] and not group["reservation_refund"]
    assert scalar.await_count == 1 and scalar.await_args.kwargs == {"npi": 17}
    original = statement_custody["statement"]
    expected = original.strip()[:-1] + " RETURNING 1;" if plain else original
    assert scalar.await_args.args == (fhir.profile_statement_wal._EXPLAIN + expected,)
    events = state.connections[0].events
    assert events.count("write") == events.count("begin") == events.count("commit") == 1
    assert state.checkedout == 0 and outcome.cleanup_complete


@pytest.mark.parametrize("mode", ["rollback", "cancel", "postcommit_sample", "unknown_commit", "restore_failure"])
async def test_affected_failure_preserves_native_commit_truth_and_incomplete_owner(affected_wave, mode):
    state, _, admission, *_ = affected_wave
    state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception):
        await affected_insert(affected_wave, plain=True, expected_rows=1)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete"
    assert group["failure"] is not None and outcome.cleanup_complete
    assert not group["accounting_authority"] and not group["reservation_refund"]
    if mode in ["postcommit_sample", "restore_failure"]:
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"
    assert state.checkedout == 0


@pytest.mark.parametrize("projected", [True, False])
async def test_affected_original_row_guards_keep_failure_before_commit(affected_wave, projected):
    state, _, admission, *_ = affected_wave
    state.mode = "overrun" if projected else "success"
    expected = RuntimeError if projected else fhir.ProviderDirectoryArtifactBuildStale
    message = "affected_projection_exceeded" if projected else "affected_window_changed"
    with pytest.raises(expected, match=message):
        await affected_insert(affected_wave, plain=True, expected_rows=2)
    outcome = admission.wal_tracker.owned_control_transaction_groups[0]["original_outcome"]
    assert outcome.commit_state == "rolled_back" and "commit" not in state.connections[0].events


@pytest.mark.parametrize(
    "field", ["coordinate", "statement_custody", "witness", "params", "task", "session", "outcome"]
)
async def test_compact_missing_or_misbound_statement_holds_original_committed_outcome(compact_wave, monkeypatch, field):
    _, _, admission, *_ = compact_wave
    execute = fhir._execute_provider_directory_profile_compact_batch

    async def corrupt(*args, **kwargs):
        result = await execute(*args, **kwargs)
        owner = admission.wal_tracker.owned_compact_worker_outcomes[asyncio.current_task()]
        if field == "coordinate":
            owner[field] = owner[field] + 1
        elif field == "statement_custody":
            owner[field] = None
        else:
            record = owner["statement_custody"]
            if field == "witness":
                record[field] = replace(record[field])
            elif field == "params":
                record[field] = {"profile_npi_start": 999}
            else:
                record[field] = object()
        return result

    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_compact_batch", corrupt)
    with pytest.raises(RuntimeError, match="compact_owned_capture_incomplete"):
        await run_wave(compact_wave)
    wave = admission.wal_tracker.owned_compact_wave_native_outcomes[0]
    owner = wave["workers"][0]["owner"]
    assert owner["outcome"].status == "committed_measured" and owner["outcome"].is_committed
    assert wave["status"] == "incomplete"
    assert not wave["accounting_authority"] and not wave["reservation_refund"]
    with pytest.raises(RuntimeError, match="compact_owned_capture_unconsumed"):
        await run_wave(compact_wave)


async def test_borrowed_payload_remains_incomplete_until_authentic_terminal(control_wave):
    _, database, admission, *_ = control_wave
    record = fhir.profile_payload_custody.statement(
        "INSERT INTO synthetic_affected SELECT :npi ON CONFLICT (npi) DO NOTHING;", {"npi": 17}
    )
    async with custody.registry_owned_wal_transaction(database) as outcome:
        async with fhir.profile_control_custody.transaction(
            fhir, nullcontext(), identity=("affected_npi_payload",), enabled=True, statement_custody=record
        ):
            outcome.session.bind.events.append("limits")
            assert await fhir.profile_payload_custody.execute(fhir, record) == 1
        group = admission.wal_tracker.owned_control_transaction_groups[0]
        assert not group["consumed"] and outcome.measurement is None and not outcome.is_committed
    fhir._profile_owned_transaction_capture(outcome, None)
    assert group["consumed"] and record["outcome"] is outcome


async def test_missing_borrowed_owner_executes_original_once_but_never_fabricates_terminal(control_wave):
    _, database, admission, *_ = control_wave
    record = fhir.profile_payload_custody.statement("INSERT INTO synthetic_affected SELECT :npi;", {"npi": 17})
    status = AsyncMock(wraps=database.status)
    database.status = status
    async with custody.registry_owned_wal_transaction(database) as outcome:
        token = custody._ACTIVE_OWNED_TRANSACTION.set(None)
        try:
            async with fhir.profile_control_custody.transaction(
                fhir, nullcontext(), identity=("affected_npi_payload",), enabled=True, statement_custody=record
            ):
                outcome.session.bind.events.append("limits")
                assert await fhir.profile_payload_custody.execute(fhir, record) == 1
        finally:
            custody._ACTIVE_OWNED_TRANSACTION.reset(token)
    fhir._profile_owned_transaction_capture(outcome, None)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    status.assert_awaited_once_with(record["statement"], npi=17)
    assert group["original_outcome"] is None and not group["consumed"]
    assert record["outcome"] is None and record["witness"] is None
    assert outcome.is_committed and outcome.status == "committed_measured"


@pytest.mark.parametrize("field", ["statement_custody", "witness", "params", "task", "session", "outcome"])
async def test_affected_misbound_custody_retains_authentic_committed_truth(affected_wave, monkeypatch, field):
    _, _, admission, *_ = affected_wave
    consume = fhir.profile_control_custody.consume_owned_groups

    def corrupt(module, outcome, failure, *, groups=None):
        group = groups[0]
        if field == "statement_custody":
            group[field] = None
        elif field == "witness":
            group["statement_custody"][field] = replace(group["statement_custody"][field])
        elif field == "params":
            group["statement_custody"][field] = {"npi": 999}
        else:
            group["statement_custody"][field] = object()
        consume(module, outcome, failure, groups=groups)

    monkeypatch.setattr(fhir.profile_control_custody, "consume_owned_groups", corrupt)
    with pytest.raises(RuntimeError, match="control_custody_incomplete"):
        await affected_insert(affected_wave, plain=True, expected_rows=1)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["original_outcome"].is_committed
    assert group["original_outcome"].status == "committed_measured"
    assert not group["consumed"] and group["status"] == "incomplete"


async def test_affected_original_body_failure_identity_is_preserved(affected_wave, monkeypatch):
    _, database, admission, *_ = affected_wave
    failure = OSError("synthetic original affected failure")
    monkeypatch.setattr(database, "scalar", AsyncMock(side_effect=failure))
    with pytest.raises(OSError) as caught:
        await affected_insert(affected_wave, plain=True, expected_rows=1)
    assert caught.value is failure
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["failure"] is failure and group["original_outcome"].commit_state == "rolled_back"


async def test_nested_payload_keeps_original_write_and_unproved_outer_exposure(control_wave):
    _, database, admission, *_ = control_wave
    record = fhir.profile_payload_custody.statement("INSERT INTO synthetic_affected SELECT :npi;", {"npi": 17})
    status = AsyncMock(wraps=database.status)
    database.status = status
    async with custody.registry_owned_wal_transaction(database) as outcome:
        outcome.session.has_nested_transaction = True
        try:
            async with fhir.profile_control_custody.transaction(
                fhir, nullcontext(), identity=("affected_npi_payload",), enabled=True, statement_custody=record
            ):
                outcome.session.bind.events.append("limits")
                assert await fhir.profile_payload_custody.execute(fhir, record) == 1
        finally:
            outcome.session.has_nested_transaction = False
    with pytest.raises(RuntimeError, match="control_custody_incomplete"):
        fhir._profile_owned_transaction_capture(outcome, None)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    status.assert_awaited_once_with(record["statement"], npi=17)
    assert record["outcome"] is outcome and record["witness"] is None
    assert not group["consumed"] and outcome.is_committed and outcome.status == "committed_measured"
