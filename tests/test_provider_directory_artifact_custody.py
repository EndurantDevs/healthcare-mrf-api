# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual artifact payload/control owners and authentic terminal outcomes."""

import asyncio
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_owned_evidence_wave import Session, custody, fhir
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave

pytestmark = pytest.mark.asyncio


@pytest.fixture
def artifact_wave(control_wave, monkeypatch):
    state, database, admission, *_ = control_wave
    events = []

    async def project(*args):
        events.append("project")

    async def reserve(*args, **kwargs):
        events.append("reserve")

    monkeypatch.setattr(fhir, "_project_artifact_batch_capacity", project)
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", reserve)

    @asynccontextmanager
    async def window(relation_class, relations=()):
        token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set((object(), relation_class))
        try:
            yield
        finally:
            fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)

    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", window)
    monkeypatch.setattr(
        database,
        "first",
        AsyncMock(return_value={"projected_rows": 1, "projected_logical_bytes": 128, "last_cursor": None}),
    )

    class ReturningSession(Session):
        async def execute(self, statement, parameters):
            result = await super().execute(statement, parameters)
            if str(statement).endswith(" RETURNING 1;"):
                result.scalar()[0]["Plan"]["Actual Rows"] = result.rowcount
            return result

    monkeypatch.setattr(custody, "AsyncSession", ReturningSession)
    scalar = AsyncMock(wraps=database.scalar)
    status = AsyncMock(wraps=database.status)
    monkeypatch.setattr(database, "scalar", scalar)
    monkeypatch.setattr(database, "status", status)
    return state, database, admission, events, scalar, status


def batch(kind="source", *, rows=1):
    return fhir._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=7,
        source_id="synthetic_source",
        dataset_id=None if kind == "source" else "synthetic_dataset",
        evidence_run_id=None if kind == "source" else "synthetic_run",
        resource_type=kind,
        after_resource_id=None,
        last_resource_id="r1" if kind != "source" and rows else None,
        projected_rows=rows,
        projected_logical_bytes=rows * 128,
    )


async def source_insert(fixture):
    source_batch = batch()
    projection = SimpleNamespace(batches=(source_batch,), projected_rows=1)
    statement = fhir._provider_directory_artifact_source_insert_sql(
        fhir.ProviderDirectorySource, "synthetic", "scratch_source"
    )
    result = await fhir._execute_artifact_source_batch(
        source_batch,
        projection,
        "SELECT synthetic_projection;",
        statement,
        relation_ref='"synthetic"."scratch_source"',
    )
    return result, source_batch, projection, statement


async def resource_insert(fixture, *, rows=1):
    resource_batch = batch("Practitioner", rows=rows)
    fixture[1].first.return_value = {
        "projected_rows": rows,
        "projected_logical_bytes": rows * 128,
        "last_cursor": resource_batch.last_resource_id,
    }
    statement = fhir._artifact_resource_batch_insert_sql(
        fhir.ProviderDirectoryPractitioner, "synthetic", "scratch_resource"
    )
    params_by_name = {
        "source_id": resource_batch.source_id,
        "dataset_id": resource_batch.dataset_id,
        "evidence_run_id": resource_batch.evidence_run_id,
        "resource_type": "Practitioner",
        "after_resource_id": None,
        "scope_batch_size": 2,
    }
    result = await fhir._execute_artifact_resource_batch(
        fhir.ProviderDirectoryPractitioner,
        "synthetic",
        statement,
        params_by_name,
        resource_batch,
        relation_ref='"synthetic"."scratch_resource"',
    )
    return result, resource_batch, None, statement


@pytest.mark.parametrize("kind", ["source", "resource"])
async def test_artifact_original_coordinate_statement_and_native_terminal_owner(artifact_wave, kind):
    state, _, admission, events, scalar, status = artifact_wave
    result, original_batch, projection, statement = await (
        source_insert(artifact_wave) if kind == "source" else resource_insert(artifact_wave)
    )
    assert result == 1 and events == ["project", "reserve"]
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    identity = group["original_identity"]
    assert identity[0:2] == ("artifact_scope_payload", kind)
    assert identity[3] is original_batch and identity[4] is projection
    assert identity[5] is statement
    record = group["statement_custody"]
    outcome = group["original_outcome"]
    assert record is group["original_statement_custody"]
    assert record["outcome"] is outcome and record["session"] is outcome.session
    assert record["witness"] is record["original_witness"] and record["witness"].affected_rows == 1
    assert record["statement"] is statement and record["params"] == identity[6]
    assert group["consumed"] and outcome.is_committed and outcome.cleanup_complete
    assert not group["accounting_authority"] and not group["reservation_refund"]
    scalar.assert_awaited_once_with(
        fhir.profile_statement_wal._EXPLAIN + statement.strip()[:-1] + " RETURNING 1;", **record["params"]
    )
    status.assert_not_awaited()
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1


async def test_artifact_zero_probe_has_original_reader_owner_and_no_payload(artifact_wave):
    state, _, admission, events, scalar, status = artifact_wave
    result, original_batch, _, statement = await resource_insert(artifact_wave, rows=0)
    assert result == 0 and events == []
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["identity"][0] == "artifact_scope_zero_probe" and group["identity"][3] is original_batch
    assert group["identity"][5] is statement and group["statement_custody"] is None
    assert group["consumed"] and group["original_outcome"].is_committed
    scalar.assert_not_awaited()
    status.assert_not_awaited()
    assert "write" not in state.connections[0].events


@pytest.mark.parametrize("kind", ["source", "resource"])
@pytest.mark.parametrize(
    "mode", ["rollback", "cancel", "postcommit_sample", "unknown_commit", "restore_failure", "overrun"]
)
async def test_artifact_failure_preserves_primary_commit_truth_and_held_owner(artifact_wave, kind, mode):
    state, _, admission, *_ = artifact_wave
    state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception):
        await (source_insert(artifact_wave) if kind == "source" else resource_insert(artifact_wave))
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete"
    assert group["failure"] is not None and outcome.cleanup_complete
    if mode in ["postcommit_sample", "restore_failure"]:
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)


async def test_artifact_projection_drift_rolls_back_before_insert(artifact_wave):
    _, database, admission, _, scalar, status = artifact_wave
    database.first.return_value = {"projected_rows": 2, "projected_logical_bytes": 128}
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="source_projection_changed"):
        await source_insert(artifact_wave)
    assert admission.wal_tracker.owned_control_transaction_groups[0]["original_outcome"].commit_state == "rolled_back"
    scalar.assert_not_awaited()
    status.assert_not_awaited()


@pytest.mark.parametrize("operation", ["layout", "analyze", "drop"])
async def test_artifact_ordinary_control_writers_have_exact_operation_relation_and_owner(artifact_wave, operation):
    _, _, admission, _, _, status = artifact_wave
    if operation == "layout":
        await fhir._create_provider_directory_artifact_scope_layout(
            fhir.ProviderDirectorySource, "synthetic", "scratch"
        )
    elif operation == "analyze":
        await fhir._analyze_artifact_scope_table("synthetic", "scratch")
    else:
        await fhir._drop_artifact_scope_tables("synthetic", ["scratch"])
    groups = admission.wal_tracker.owned_control_transaction_groups
    assert len(groups) == status.await_count == (3 if operation == "layout" else 1)
    for group, call in zip(groups, status.await_args_list, strict=True):
        assert group["identity"][:2] == ("artifact_scope_" + operation, '"synthetic"."scratch"')
        assert group["identity"][2] is call.args[0]
        assert group["original_outcome"].is_committed and group["consumed"]
        assert not group["accounting_authority"] and not group["reservation_refund"]


async def test_artifact_recovery_retains_one_original_atomic_owner_and_all_statements(artifact_wave, monkeypatch):
    state, _, admission, _, _, status = artifact_wave
    plan = SimpleNamespace(
        relation_by_table={fhir.ProviderDirectorySource.__tablename__: "current"},
        model_by_table_name={fhir.ProviderDirectorySource.__tablename__: fhir.ProviderDirectorySource},
        created_tables=[],
    )
    coordinate = fhir._ArtifactScopeRecoveryCoordinate(fhir.ProviderDirectorySource.__tablename__, "prior", "current")
    identities_by_relation = {"prior": (17, "r", "u")}
    monkeypatch.setattr(fhir, "_artifact_scope_relation_identities", AsyncMock(return_value=identities_by_relation))
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_unreferenced", AsyncMock())
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_layouts", AsyncMock())
    await fhir._replace_terminal_artifact_scope(
        "synthetic", plan, (coordinate,), ("prior",), identities_by_relation, admission
    )
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert group["identity"][0] == "artifact_scope_recovery"
    assert group["identity"][1][1][0] is coordinate
    statements = group["identity"][2]
    assert len(statements) == status.await_count == 5
    assert [row[0] for row in statements] == [call.args[0] for call in status.await_args_list]
    assert statements[0][0].startswith("LOCK TABLE ") and statements[-1][0].startswith("DROP TABLE ")
    assert all(row[2] is outcome.session for row in statements)
    assert group["consumed"] and outcome.is_committed and outcome.cleanup_complete
    assert state.connections[0].events.count("begin") == state.connections[0].events.count("commit") == 1
    assert plan.created_tables == ["current"]


@pytest.mark.parametrize("field", ["witness", "params", "outcome", "statement_custody"])
async def test_artifact_missing_or_replaced_root_keeps_authentic_commit_and_incomplete_exposure(
    artifact_wave, monkeypatch, field
):
    _, _, admission, *_ = artifact_wave
    consume = fhir.profile_control_custody.consume_owned_groups

    def corrupt(module, outcome, failure, *, groups=None):
        group = groups[0]
        if field == "statement_custody":
            group[field] = None
        elif field == "witness":
            group["statement_custody"][field] = replace(group["statement_custody"][field])
        else:
            group["statement_custody"][field] = object()
        consume(module, outcome, failure, groups=groups)

    monkeypatch.setattr(fhir.profile_control_custody, "consume_owned_groups", corrupt)
    with pytest.raises(RuntimeError, match="control_custody_incomplete"):
        await source_insert(artifact_wave)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    assert group["original_outcome"].is_committed and group["original_outcome"].status == "committed_measured"
    assert not group["consumed"]


async def test_artifact_unknown_borrowed_owner_executes_original_once_and_holds_uncertainty(artifact_wave, monkeypatch):
    _, database, admission, _, scalar, status = artifact_wave
    monkeypatch.setattr(database, "transaction", nullcontext)
    original_batch = batch()
    async with custody.registry_owned_wal_transaction(database) as outcome:
        token = custody._ACTIVE_OWNED_TRANSACTION.set(None)
        try:
            async with fhir.profile_artifact_custody.payload_transaction(
                fhir, "source", "scratch", original_batch, object(), "INSERT INTO scratch SELECT :npi;", {"npi": 1}
            ):
                assert (
                    await fhir.profile_artifact_custody.execute_insert(
                        fhir, "INSERT INTO scratch SELECT :npi;", {"npi": 1}
                    )
                    == 1
                )
        finally:
            custody._ACTIVE_OWNED_TRANSACTION.reset(token)
    fhir._profile_owned_transaction_capture(outcome, None)
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    status.assert_awaited_once_with("INSERT INTO scratch SELECT :npi;", npi=1)
    scalar.assert_not_awaited()
    assert group["original_outcome"] is None and not group["consumed"]
    assert group["statement_custody"]["witness"] is None and outcome.is_committed


@pytest.mark.parametrize("mode", ["body", "cancel", "postcommit_sample", "unknown_commit"])
async def test_artifact_atomic_recovery_failure_preserves_owner_and_primary(artifact_wave, monkeypatch, mode):
    state, _, admission, _, _, status = artifact_wave
    plan = SimpleNamespace(
        relation_by_table={fhir.ProviderDirectorySource.__tablename__: "current"},
        model_by_table_name={fhir.ProviderDirectorySource.__tablename__: fhir.ProviderDirectorySource},
        created_tables=[],
    )
    coordinate = fhir._ArtifactScopeRecoveryCoordinate(fhir.ProviderDirectorySource.__tablename__, "prior", "current")
    identities_by_relation = {"prior": (17, "r", "u")}
    primary = OSError("synthetic original recovery failure")
    monkeypatch.setattr(fhir, "_artifact_scope_relation_identities", AsyncMock(return_value=identities_by_relation))
    monkeypatch.setattr(
        fhir, "_assert_artifact_scope_recovery_unreferenced", AsyncMock(side_effect=primary if mode == "body" else None)
    )
    monkeypatch.setattr(fhir, "_assert_artifact_scope_recovery_layouts", AsyncMock())
    if mode != "body":
        state.mode = mode
    with pytest.raises(asyncio.CancelledError if mode == "cancel" else Exception) as caught:
        await fhir._replace_terminal_artifact_scope(
            "synthetic", plan, (coordinate,), ("prior",), identities_by_relation, admission
        )
    group = admission.wal_tracker.owned_control_transaction_groups[0]
    outcome = group["original_outcome"]
    assert not group["consumed"] and group["status"] == "incomplete" and outcome.cleanup_complete
    assert state.checkedout == 0
    if mode == "body":
        assert caught.value is primary and group["failure"] is primary and status.await_count == 1
    if mode == "postcommit_sample":
        assert outcome.is_committed and outcome.status == "committed_accounting_incomplete"
    elif mode == "unknown_commit":
        assert not outcome.is_committed and outcome.commit_state == "attempted"
    else:
        assert outcome.commit_state == "rolled_back"


async def test_artifact_failed_concurrent_batches_drain_original_tasks_and_connections(artifact_wave):
    state, _, admission, *_ = artifact_wave
    state.mode = "rollback"
    tasks = [asyncio.create_task(source_insert(artifact_wave)) for _ in range(2)]
    with pytest.raises(Exception):
        await fhir._gather_provider_directory_profile_tasks(tasks)
    assert all(task.done() for task in tasks) and state.checkedout == 0
    assert all(connection.closed for connection in state.connections)
    assert all(not group["consumed"] for group in admission.wal_tracker.owned_control_transaction_groups)
