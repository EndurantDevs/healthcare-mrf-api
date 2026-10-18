# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host SQL and caller contracts; genuine SOURCE/native execution is separate."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_scope_copy as copy_scope

fhir = importlib.import_module("process.provider_directory_fhir")


def _batch():
    return fhir._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=1,
        source_id="synthetic_source",
        dataset_id="synthetic_dataset",
        evidence_run_id="synthetic_run",
        resource_type="Practitioner",
        after_resource_id="r0001",
        last_resource_id="r1000",
        projected_rows=999,
        projected_logical_bytes=99900,
    )


def _projection(batch=None):
    batch = batch or _batch()
    table = fhir._ProviderDirectoryArtifactScopeTableProjection(
        table_name=fhir.ProviderDirectoryPractitioner.__tablename__,
        resource_type=batch.resource_type,
        projected_rows=batch.projected_rows,
        projected_logical_bytes=batch.projected_logical_bytes,
        batches=(batch,),
    )
    return fhir._ProviderDirectoryArtifactScopeExactProjection(tables=(table,))


@pytest.fixture
def synthetic_admission(monkeypatch):
    """Mock the admission boundary only; no host result authenticates native roles."""
    projection = _projection()
    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(
            geometry=SimpleNamespace(bounded_admission=True),
            run_id="run_" + "a" * 32,
        ),
    )
    monkeypatch.setattr(fhir, "_assert_artifact_scope_projection", lambda value: value is projection or _refuse())
    return projection


def _refuse():
    raise RuntimeError("synthetic_projection_changed")


def test_source_requires_original_bounded_admission(monkeypatch):
    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_admission", lambda: None)
    with pytest.raises(RuntimeError, match="scope_copy_invalid"):
        copy_scope.is_enabled(fhir)


def test_exact_projection_object_and_closed_writer_gate(synthetic_admission):
    with copy_scope.materialization_scope(fhir, synthetic_admission):
        assert copy_scope.scope_projection(fhir) is synthetic_admission
        batch = synthetic_admission.tables[0].batches[0]
        assert (
            copy_scope._table_projection(fhir, fhir.ProviderDirectoryPractitioner, batch)
            is synthetic_admission.tables[0]
        )
        with pytest.raises(RuntimeError, match="scope_copy_invalid"):
            copy_scope._table_projection(fhir, fhir.ProviderDirectoryPractitioner, replace(batch))
        with pytest.raises(RuntimeError, match="scope_copy_invalid"):
            with copy_scope.materialization_scope(fhir, synthetic_admission):
                raise AssertionError("nested scope must refuse before its body")
    with pytest.raises(RuntimeError, match="scope_copy_invalid"):
        copy_scope.scope_projection(fhir)


@pytest.mark.asyncio
async def test_closed_scope_refuses_copy_before_any_sql(synthetic_admission, monkeypatch):
    scalar = AsyncMock()
    monkeypatch.setattr(fhir.db, "scalar", scalar)
    with copy_scope.materialization_scope(fhir, synthetic_admission):
        copy_scope._SCOPE.get()["closed"] = True
        with pytest.raises(RuntimeError, match="scope_copy_invalid"):
            await copy_scope.copy_batch(fhir, fhir.ProviderDirectoryPractitioner, _batch(), "SELECT 1", {})
    scalar.assert_not_awaited()


@pytest.mark.parametrize("model", (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS))
def test_full_tuple_parity_uses_completed_primary_keys_once(model):
    statement = copy_scope._parity_sql(fhir, model, '"synthetic"."completed"', "SELECT synthetic_expected")
    assert statement.count("AS MATERIALIZED") == 1
    assert statement.count("NOT EXISTS") == 3
    assert "EXCEPT" not in statement
    assert "to_jsonb(a) IS DISTINCT FROM to_jsonb(e)" in statement
    for column in model.__table__.primary_key.columns:
        assert 'a."' + column.name + '"=e."' + column.name + '"' in statement
    columns = ",".join(fhir._q(str(column.name)) for column in model.__table__.columns)
    assert statement.startswith("WITH expected(" + columns + ") AS MATERIALIZED")
    assert statement.count(":expected_rows") == 2


def test_empty_table_still_checks_no_extra_rows():
    statement = copy_scope._parity_sql(fhir, fhir.ProviderDirectoryPractitioner, '"synthetic"."completed"', "")
    assert 'FROM "synthetic"."completed" WHERE FALSE' in statement
    assert '(SELECT count(*) FROM "synthetic"."completed")=:expected_rows' in statement


def test_resource_pages_use_frozen_range_without_repeated_materialized_batches(monkeypatch):
    captured_by_field = {}
    monkeypatch.setattr(
        fhir,
        "_provider_directory_artifact_resource_select_sql",
        lambda *_args, **_kwargs: "SELECT r.* FROM synthetic_child r",
    )

    def pages(reader, statement, params, model):
        captured_by_field.update(reader=reader, statement=statement, params=params, model=model)
        return "bounded_pages"

    monkeypatch.setattr(copy_scope, "projected_pages", pages)
    batch = _batch()
    assert copy_scope._resource_pages(fhir, fhir.ProviderDirectoryPractitioner, batch) == "bounded_pages"
    assert "MATERIALIZED" not in captured_by_field["statement"] and "LIMIT" not in captured_by_field["statement"]
    assert captured_by_field["params"]["frozen_after"] == "r0001"
    assert captured_by_field["params"]["frozen_last"] == "r1000"
    assert captured_by_field["reader"] is fhir.db


def test_resource_expected_keeps_each_physical_dataset_and_typed_cast(monkeypatch):
    datasets = [
        SimpleNamespace(source_id="s" + str(i), dataset_id="d" + str(i), evidence_run_id="e" + str(i)) for i in range(2)
    ]
    monkeypatch.setattr(fhir, "_artifact_scope_resource_datasets", lambda *_args: datasets)
    monkeypatch.setattr(
        fhir,
        "_provider_directory_artifact_resource_select_sql",
        lambda _model, _schema, *, dataset: (
            "SELECT :source_ids::varchar[], CAST(:dataset_ids AS varchar[]), '" + dataset.dataset_id + "'"
        ),
    )
    statement, params = copy_scope._resource_expected(
        fhir, fhir.ProviderDirectoryPractitioner, "synthetic", object(), frozenset()
    )
    assert statement.count("UNION ALL") == 1
    assert ":d0_source_ids::varchar[]" in statement and ":d1_source_ids::varchar[]" in statement
    assert params["d0_dataset_ids"] == ["d0"] and params["d1_dataset_ids"] == ["d1"]


@pytest.mark.asyncio
async def test_complete_snapshot_closes_writers_before_catalog_and_parity(synthetic_admission, monkeypatch):
    events = []
    table = synthetic_admission.tables[0]
    plan = SimpleNamespace(
        model_by_table_name={table.table_name: fhir.ProviderDirectoryPractitioner},
        relation_by_table={table.table_name: "synthetic_complete"},
    )
    monkeypatch.setattr(copy_scope, "_target_oid", AsyncMock(return_value=19))
    monkeypatch.setattr(copy_scope, "_resource_expected", lambda *_args: ("SELECT synthetic_expected", {}))

    @asynccontextmanager
    async def owner(**_kwargs):
        assert copy_scope._SCOPE.get()["closed"] is True
        events.append("owner")
        yield

    async def catalog(*_args, **_kwargs):
        assert copy_scope._SCOPE.get()["closed"] is True
        events.append("catalog")
        return "same_native_catalog"

    async def is_snapshot_valid(statement, **params):
        assert "AS MATERIALIZED" in statement and params["expected_rows"] == table.projected_rows
        events.append("whole_snapshot_parity")
        return True

    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", owner)
    monkeypatch.setattr(fhir.profile_owned_wal, "current_owned_wal_transaction", lambda _db: object())
    monkeypatch.setattr(copy_scope.native_copy, "completed_target_catalog", catalog)
    monkeypatch.setattr(fhir.db, "scalar", is_snapshot_valid)
    with copy_scope.materialization_scope(fhir, synthetic_admission):
        await copy_scope.validate_snapshot(fhir, "synthetic", plan, object(), object(), frozenset())
    assert events == ["owner", "catalog", "whole_snapshot_parity", "catalog"]


@pytest.mark.asyncio
async def test_legacy_worker_keeps_existing_task_creation(monkeypatch):
    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: False)

    async def original():
        return asyncio.current_task()

    task = copy_scope.create_materialization_task(fhir, original(), model=None, relation=None, table_projection=None)
    assert await task is task


@pytest.mark.asyncio
async def test_partial_worker_issuance_drains_before_caller_cleanup(monkeypatch):
    issued_tasks, events = [], []
    failure = RuntimeError("synthetic second worker issuance failed")

    async def first_worker():
        try:
            events.append("worker_started")
            await asyncio.Future()
        finally:
            events.append("worker_drained")

    async def unopened_worker():
        raise AssertionError("unissued worker must not run")

    def issue(_fhir, operation, **_kwargs):
        if issued_tasks:
            raise failure
        task = asyncio.create_task(operation)
        issued_tasks.append(task)
        return task

    monkeypatch.setattr(copy_scope, "create_materialization_task", issue)
    operations = [(first_worker(), {}), (unopened_worker(), {}), (unopened_worker(), {})]
    with pytest.raises(RuntimeError) as caught:
        await copy_scope.gather_materialization_tasks(fhir, operations)
    assert caught.value is failure and all(task.done() for task in issued_tasks)
    events.append("caller_cleanup")
    assert "worker_drained" not in events or events.index("worker_drained") < events.index("caller_cleanup")
    assert all(operation.cr_frame is None for operation, _kwargs in operations)


@pytest.mark.parametrize("model", (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS))
def test_native_not_null_constraints_match_model_required_columns(model):
    constraints = [
        dict(
            relation_oid=17,
            constraint_type="n",
            condeferrable=False,
            condeferred=False,
            convalidated=True,
            connoinherit=False,
            constraint_definition="NOT NULL " + column.name,
        )
        for column in model.__table__.columns
        if not column.nullable
    ]
    fhir._assert_artifact_scope_native_not_null(model, 17, constraints)
    if len(constraints) > 1:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="not_null_changed"):
            fhir._assert_artifact_scope_native_not_null(model, 17, constraints[1:])
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="not_null_changed"):
        fhir._assert_artifact_scope_native_not_null(model, 17, [*constraints, constraints[0]])
    constraints[0]["convalidated"] = False
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="not_null_changed"):
        fhir._assert_artifact_scope_native_not_null(model, 17, constraints)


@pytest.mark.asyncio
async def test_materialization_finishes_all_indexes_then_analyzes_and_validates(synthetic_admission, monkeypatch):
    events = []
    plan = fhir._artifact_scope_materialization_plan("run_" + "a" * 32)
    monkeypatch.setattr(fhir, "_artifact_scope_materialization_plan", lambda _run: plan)

    def record(label):
        async def operation(*_args, **_kwargs):
            events.append(label)

        return operation

    monkeypatch.setattr(fhir, "_recover_provider_directory_artifact_scope", record("recover"))
    monkeypatch.setattr(fhir, "_materialize_artifact_scope_payload", record("all_copy_payloads"))
    monkeypatch.setattr(fhir, "_finish_artifact_scope_indexes", record("all_indexes"))
    monkeypatch.setattr(fhir, "_analyze_artifact_scope_table", record("analyze"))
    monkeypatch.setattr(copy_scope, "validate_snapshot", record("full_snapshot_parity"))
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", record("expose"))
    result = await fhir._materialize_artifact_scope_tables(
        "synthetic", "run_" + "a" * 32, object(), frozenset(), projection=synthetic_admission
    )
    assert result == (plan.relation_by_table, plan.created_tables)
    assert events == [
        "recover",
        "all_copy_payloads",
        "all_indexes",
        *(["analyze"] * 9),
        "full_snapshot_parity",
        "expose",
    ]


@pytest.mark.asyncio
async def test_failed_index_build_cleans_without_validating_or_exposing(synthetic_admission, monkeypatch):
    plan = fhir._artifact_scope_materialization_plan("run_" + "a" * 32)
    failure = OSError("synthetic index build failed")
    monkeypatch.setattr(fhir, "_artifact_scope_materialization_plan", lambda _run: plan)
    monkeypatch.setattr(fhir, "_recover_provider_directory_artifact_scope", AsyncMock())
    monkeypatch.setattr(fhir, "_materialize_artifact_scope_payload", AsyncMock())
    monkeypatch.setattr(fhir, "_finish_artifact_scope_indexes", AsyncMock(side_effect=failure))
    cleanup, validate, expose = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(fhir, "_cleanup_failed_artifact_scope", cleanup)
    monkeypatch.setattr(copy_scope, "validate_snapshot", validate)
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", expose)
    with pytest.raises(OSError) as caught:
        await fhir._materialize_artifact_scope_tables(
            "synthetic", "run_" + "a" * 32, object(), frozenset(), projection=synthetic_admission
        )
    assert caught.value is failure
    cleanup.assert_awaited_once_with("synthetic", plan.created_tables, failure)
    validate.assert_not_awaited()
    expose.assert_not_awaited()


@pytest.mark.asyncio
async def test_completed_profile_read_uses_issued_family_before_owner_fallback(monkeypatch):
    from process import provider_directory_profile_scope_reads as reads

    events = []

    @asynccontextmanager
    async def issued(directory):
        assert directory is fhir
        events.append("issued_read")
        yield
        events.append("read_verified")

    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(reads, "is_active", lambda _directory: True)
    monkeypatch.setattr(reads, "read_transaction", issued)
    monkeypatch.setattr(copy_scope.source_policy, "bound_publisher", _refuse)
    async with copy_scope.prepared_read_transaction(fhir):
        events.append("read_body")
    assert events == ["issued_read", "read_body", "read_verified"]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_candidate", [False, True])
async def test_late_npi_guard_runs_inside_completed_read(monkeypatch, has_candidate):
    events = []

    @asynccontextmanager
    async def issued(_directory):
        events.append("issued_read")
        try:
            yield
        finally:
            events.append("read_finished")

    async def candidate_exists(statement, **params):
        assert events[-1] in {"issued_read", "indexed_npi_check"}
        assert params["source_ids"] == ["synthetic_source"]
        assert "npi IS NULL" in statement
        events.append("indexed_npi_check")
        return has_candidate

    monkeypatch.setattr(copy_scope, "prepared_read_transaction", issued)
    monkeypatch.setattr(fhir.db, "scalar", candidate_exists)
    if has_candidate:
        with pytest.raises(RuntimeError, match="required_before_capacity_admission:Practitioner"):
            await fhir._assert_no_resource_npi_candidates("synthetic", ["synthetic_source"])
    else:
        await fhir._assert_no_resource_npi_candidates("synthetic", ["synthetic_source"])
    assert events == ["issued_read", *(["indexed_npi_check"] * (1 if has_candidate else 2)), "read_finished"]


@pytest.mark.asyncio
@pytest.mark.parametrize("body_fails", [False, True])
async def test_prepared_family_drains_before_original_scratch_cleanup(synthetic_admission, monkeypatch, body_fails):
    from process import provider_directory_profile_scope_reads as reads

    events = []
    fence, resource = object(), SimpleNamespace(source_ids=("synthetic_source",))
    names, oids = {"synthetic_root": "synthetic_complete"}, {"synthetic_root": 17}
    failure = ValueError("synthetic read body failed")

    @asynccontextmanager
    async def prepared(directory, projection, relations, identities, full_fence, resource_fence):
        assert directory is fhir and projection is synthetic_admission
        assert (relations, identities, full_fence, resource_fence) == (names, oids, fence, resource)
        assert fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == names
        events.append("family_verified")
        try:
            yield
        finally:
            events.append("children_drained")

    async def cleanup(schema, tables):
        assert schema == "synthetic" and tables == ["synthetic_complete"]
        events.append("original_cleanup")

    monkeypatch.setattr(reads, "prepared_scope", prepared)
    monkeypatch.setattr(
        fhir, "_artifact_scope_relation_identities", AsyncMock(return_value={"synthetic_complete": (17, "r", "u")})
    )
    monkeypatch.setattr(fhir, "_drain_artifact_scope_cleanup", cleanup)

    async def run():
        async with fhir._yield_artifact_dataset_scope(
            "synthetic", fence, resource, names, ["synthetic_complete"], projection=synthetic_admission
        ) as selected:
            assert selected is fence
            events.append("reader_body")
            if body_fails:
                raise failure

    if body_fails:
        with pytest.raises(ValueError) as caught:
            await run()
        assert caught.value is failure
    else:
        await run()
    assert events == ["family_verified", "reader_body", "children_drained", "original_cleanup"]


@pytest.mark.parametrize("model", fhir.RESOURCE_MODELS)
def test_resource_copy_select_names_every_canonical_model_column(model, monkeypatch):
    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: False)
    statement = fhir._provider_directory_artifact_resource_select_sql(model, "synthetic")
    aliases = [" AS " + fhir._q(column.name) for column in model.__table__.columns]
    assert all(statement.count(alias) == 1 for alias in aliases)
    assert statement.index('AS "last_seen_run_id"') > statement.index("selected.evidence_run_id")


@pytest.mark.asyncio
@pytest.mark.parametrize("is_loading", [False, True])
async def test_source_capacity_keeps_native_storage_limits_without_growing_row_scans(
    synthetic_admission, monkeypatch, is_loading
):
    events, names = [], {"synthetic_root": "synthetic_candidate"}

    @asynccontextmanager
    async def issued(_directory):
        events.append("source_owner")
        yield
        events.append("owner_finished")

    async def count_rows(statement):
        assert events[-1] == "source_owner"
        assert statement.startswith('SELECT count(*) FROM "synthetic"."synthetic_candidate"')
        events.append("final_row_census")
        return 999

    async def storage_bytes(relations):
        assert events[0] == "source_owner" and relations == ['"synthetic"."synthetic_candidate"']
        events.append("storage_bytes")
        return 4096

    budget = AsyncMock()
    monkeypatch.setattr(copy_scope, "source_read_transaction", issued)
    monkeypatch.setattr(fhir.db, "scalar", count_rows)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_relation_bytes", storage_bytes)
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_capacity_relation_cap",
        lambda *_args: SimpleNamespace(max_scratch_bytes=8192),
    )
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_wal_budget", budget)
    if is_loading:
        await copy_scope.loading_capacity(fhir, "synthetic", names, synthetic_admission)
    else:
        await fhir._assert_provider_directory_artifact_scope_observed_capacity("synthetic", names, synthetic_admission)
    assert events == ["source_owner", *([] if is_loading else ["final_row_census"]), "storage_bytes", "owner_finished"]
    budget.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "isolation,readonly",
    [("serializable", True), ("serializable", False), ("repeatable read", True), ("read committed", False)],
)
async def test_source_preflight_dispatches_original_mode_without_late_setters(monkeypatch, isolation, readonly):
    from process import provider_directory_source_observation as observation

    events, session = [], object()

    @asynccontextmanager
    async def actor(target, *, isolation):
        assert target is (fhir if readonly else fhir.db)
        events.append(isolation)
        yield session

    setter = AsyncMock(side_effect=AssertionError("source mode must be set before authentication SQL"))
    monkeypatch.setattr(copy_scope.source_policy, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(observation, "observation_transaction", actor)
    monkeypatch.setattr(copy_scope.source_policy, "source_transaction", actor)
    monkeypatch.setattr(fhir.db, "status", setter)
    async with copy_scope.preflight_transaction(fhir, isolation=isolation, readonly=readonly) as actual:
        assert actual is session and events == [isolation]
    setter.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("readonly", [False, True])
async def test_profile_inputs_reuse_original_signing_or_observation_actor(monkeypatch, readonly):
    from process import provider_directory_profile_source_inputs as inputs
    from process import provider_directory_source_observation as observation

    session = object()
    binding = SimpleNamespace(session=session)
    actor = SimpleNamespace(connection=object())
    unexpected = AsyncMock(side_effect=AssertionError("original actor must not open a second transaction"))
    monkeypatch.setattr(observation, "current_observation", lambda _directory: actor if readonly else None)
    monkeypatch.setattr(inputs.source, "source_transaction", unexpected)
    monkeypatch.setattr(inputs.source, "bound_publisher", lambda: actor)
    monkeypatch.setattr(inputs.source, "source_copy_session", lambda: session)
    monkeypatch.setattr(fhir.db, "_transaction_binding", lambda: binding)
    token = inputs.source._PUBLISHER.set(None if readonly else actor)
    try:
        async with inputs._read_actor(fhir) as selected:
            assert selected is actor
    finally:
        inputs.source._PUBLISHER.reset(token)
    unexpected.assert_not_called()


@pytest.mark.asyncio
async def test_failed_completed_capacity_cleans_exact_candidate_before_read_exposure(monkeypatch):
    failure, projection = RuntimeError("synthetic capacity exceeded"), object()
    observed, cleanup = AsyncMock(side_effect=failure), AsyncMock()
    monkeypatch.setattr(fhir, "_assert_provider_directory_artifact_scope_observed_capacity", observed)
    monkeypatch.setattr(fhir, "_cleanup_failed_artifact_scope", cleanup)
    with pytest.raises(RuntimeError) as caught:
        await copy_scope.completed_capacity(fhir, "synthetic", {"root": "candidate"}, projection, ["candidate"])
    assert caught.value is failure
    observed.assert_awaited_once_with("synthetic", {"root": "candidate"}, projection)
    cleanup.assert_awaited_once_with("synthetic", ["candidate"], failure)


@pytest.mark.asyncio
async def test_recovery_heap_keeps_native_null_constraints_and_strips_only_verified_pk(monkeypatch):
    from process import provider_directory_cms_nonprofile_observation as observation

    relation_by_field = dict(schema_name="synthetic", relation_name="candidate")
    constraints = [
        dict(relation_oid=17, constraint_type="n", constraint_definition="NOT NULL source_id"),
        dict(relation_oid=17, constraint_type="p", constraint_definition="PRIMARY KEY (source_id)"),
    ]
    catalog = SimpleNamespace(
        _profile_capacity_relation_row=AsyncMock(return_value=(relation_by_field, None)),
        _profile_capacity_relation_catalog=AsyncMock(return_value=([], [], constraints, [])),
    )
    captured_constraints = []

    def fingerprint(_relation, _attributes, _indexes, native_constraints, _triggers, _oid):
        captured_constraints.extend(native_constraints)
        return {}, {"constraints": native_constraints}

    monkeypatch.setattr(
        fhir, "_provider_directory_profile_relation_storage_fingerprint", AsyncMock(return_value="layout")
    )
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=True))
    monkeypatch.setattr(observation, "catalog_observation", lambda *_args: catalog)
    phase = AsyncMock()
    monkeypatch.setattr(fhir, "_artifact_scope_recovery_index_phase", phase)
    monkeypatch.setattr(fhir, "_profile_capacity_fingerprint_payloads", fingerprint)
    result = await fhir._artifact_scope_recovery_heap_fingerprint("synthetic", "candidate", 17, object())
    assert result[0] == "layout" and captured_constraints == constraints[:1]
    phase.assert_awaited_once()
