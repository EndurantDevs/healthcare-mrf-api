# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recovery refusals at admitted Profile and bounded CMS publication boundaries."""

import asyncio
import copy
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_capacity_cutover as cutover
from process import provider_directory_profile_initial as initial
from process import provider_directory_profile_initial_contract as initial_contract
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_profile_bounded_capacity import (
    _bounded_cutover_receipt,
    _bounded_geometry,
    admitted_window,
    capacity,
    fhir,
)
from tests.test_provider_directory_profile_capacity_preflight import _transaction
from tests.test_provider_directory_profile_capacity_runtime import (
    _geometry_database_by_field,
    _geometry_relation_by_field,
)
from tests.test_provider_directory_profile_control_capacity import _control_wal_plan_input
from tests.test_provider_directory_profile_initial import _geometry, _target
from tests.test_provider_directory_profile_resume_lineage import _build, _dataset, _source_context


@asynccontextmanager
async def _recorded_transaction(events, *, include_begin=False):
    if include_begin:
        events.append("transaction")
    try:
        yield
    except BaseException:
        events.append("rollback")
        raise
    events.append("commit")


@pytest.mark.asyncio
async def test_bounded_affected_npis_page_each_changed_source_and_new_evidence(admitted_window, monkeypatch):
    """Removed and refreshed sources share exact distinct-NPI cursor boundaries."""
    admission, _ = admitted_window
    build = replace(_build(), source_ids=("cms-npd", "cms-npd"), removed_source_ids=("source-old",))
    npi_window_rows = [
        {"row_count": 2, "last_npi": 1000000002},
        {"row_count": 1, "last_npi": 1000000003},
        {"row_count": 0, "last_npi": None},
    ]
    query = AsyncMock(side_effect=npi_window_rows * 3)
    insert = AsyncMock(side_effect=[2, 1, 2, 1, 2, 1])
    monkeypatch.setattr(fhir.db, "first", query)
    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_affected_npi_insert", insert)

    assert await fhir._populate_affected_npi_sources(build, "affected", "evidence") == 6
    assert await fhir._populate_affected_npi_delta(build, "affected", "stage") == 3
    assert [call.kwargs["params"].get("source_id") for call in insert.await_args_list] == [
        "cms-npd",
        "cms-npd",
        "source-old",
        "source-old",
        None,
        None,
    ]
    assert [call.kwargs["params"]["after_npi"] for call in insert.await_args_list] == [None, 1000000002] * 3
    assert [call.kwargs["expected_rows"] for call in insert.await_args_list] == [2, 1] * 3
    assert all(
        call.kwargs["params"]["window_size"] == admission.geometry.artifact_scope_batch_size
        for call in insert.await_args_list
    )
    assert all(
        "NOT EXISTS" in call.kwargs["insert_sql"] and "SELECT DISTINCT" in call.kwargs["projection_sql"]
        for call in insert.await_args_list
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows,error",
    [
        ([None], "affected_projection_missing"),
        ([{"row_count": 3, "last_npi": 1000000003}], "affected_window_cursor_invalid"),
        ([{"row_count": -1, "last_npi": 1000000001}], "affected_window_cursor_invalid"),
        ([{"row_count": 1, "last_npi": None}], "affected_window_cursor_invalid"),
        ([{"row_count": 1, "last_npi": 1000000001}] * 2, "affected_window_cursor_invalid"),
    ],
)
async def test_affected_window_refuses_missing_or_nonadvancing_cursor(admitted_window, monkeypatch, rows, error):
    insert = AsyncMock(return_value=1)
    monkeypatch.setattr(fhir.db, "first", AsyncMock(side_effect=rows))
    monkeypatch.setattr(fhir, "_execute_provider_directory_profile_affected_npi_insert", insert)
    with pytest.raises(RuntimeError, match=error):
        await fhir._populate_affected_npi_windows(_build(), "affected", "evidence", predicate="TRUE", params={})
    assert insert.await_count == len(rows) - 1


@pytest.mark.parametrize(
    "inserted,projected,expected,error",
    [
        (3, 2, 2, "affected_projection_exceeded"),
        (1, 2, 2, "affected_window_changed"),
        (2, 2, 2, None),
        (1, 2, None, None),
    ],
)
def test_affected_insert_keeps_admitted_count(inserted, projected, expected, error):
    if error:
        with pytest.raises(RuntimeError, match=error):
            fhir._validated_affected_npi_insert(inserted, projected, expected)
    else:
        assert fhir._validated_affected_npi_insert(inserted, projected, expected) == inserted


@pytest.mark.asyncio
@pytest.mark.parametrize("resource_batch", [False, True])
@pytest.mark.parametrize("projection_present", [False, True])
async def test_bounded_artifact_insert_requires_projection_and_exact_count(
    admitted_window,
    monkeypatch,
    resource_batch,
    projection_present,
):
    batch = SimpleNamespace(source_id="cms-npd", projected_rows=2, projected_logical_bytes=16)
    mutation_events = []

    @asynccontextmanager
    async def window(*_args):
        mutation_events.append("begin")
        try:
            yield
        except BaseException:
            mutation_events.append("rollback")
            raise
        mutation_events.append("commit")

    project = AsyncMock()
    reserve = AsyncMock()
    insert = AsyncMock(return_value=1)
    status = AsyncMock()
    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", window)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_transaction", _transaction)
    monkeypatch.setattr(fhir, "_project_artifact_batch_capacity", project)
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", reserve)
    monkeypatch.setattr(fhir, "_insert_artifact_source_batch", insert)
    monkeypatch.setattr(fhir, "_insert_projected_artifact_resource_batch", insert)
    monkeypatch.setattr(fhir, "_provider_directory_profile_capacity_status", status)
    with pytest.raises(RuntimeError, match="projection_changed" if projection_present else "projection_required"):
        if resource_batch:
            await fhir._execute_artifact_resource_batch(
                object(), "mrf", "INSERT", {}, batch if projection_present else None, relation_ref="stage"
            )
        else:
            await fhir._execute_artifact_source_batch(
                batch, object() if projection_present else None, "SELECT", "INSERT", relation_ref="stage"
            )
    assert mutation_events == (["begin", "rollback"] if projection_present else [])
    assert insert.await_count == int(projection_present)
    assert project.await_count == reserve.await_count == int(projection_present)
    status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "relation_ref,oid,error",
    [
        (None, 1, "scratch_relation_missing"),
        ("stage", None, "scratch_oid_changed"),
        ("stage", 0, "scratch_oid_changed"),
        ("stage", 42, None),
    ],
)
async def test_artifact_projection_binds_live_scratch_oid(admitted_window, monkeypatch, relation_ref, oid, error):
    admission, _ = admitted_window
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=oid))
    project = AsyncMock()
    monkeypatch.setattr(fhir, "_project_provider_directory_profile_scratch_window", project)
    batch = SimpleNamespace(projected_rows=2, projected_logical_bytes=16)
    if error:
        with pytest.raises(RuntimeError, match=error):
            await fhir._project_artifact_batch_capacity(admission, relation_ref, batch)
        project.assert_not_awaited()
    else:
        await fhir._project_artifact_batch_capacity(admission, relation_ref, batch)
        project.assert_awaited_once_with(
            "artifact_scope", "stage", 42, inserted_rows=2, inserted_logical_bytes=16, expected_persistence="u"
        )


@pytest.mark.asyncio
async def test_artifact_scope_cap_is_shared_across_all_scratch_relations(admitted_window, monkeypatch):
    admission, state = admitted_window
    state.sizes.update(first=600, second=401)
    wal_guard = AsyncMock()
    monkeypatch.setattr(fhir, "_assert_provider_directory_profile_wal_budget", wal_guard)
    with pytest.raises(RuntimeError, match="artifact_bytes_exceeded"):
        await fhir._admit_artifact_scope_totals(admission, [("first", None), ("second", None)])
    assert admission.wal_tracker.relation_refs_by_class["artifact_scope"] == {"first", "second"}
    wal_guard.assert_not_awaited()
    with pytest.raises(RuntimeError, match="scratch_bytes_exceeded"):
        await fhir._assert_provider_directory_profile_capacity_scratch("artifact_scope", ("stage",))
    assert fhir._provider_directory_profile_capacity_relation_bytes.await_args.args[0] == {"first", "second"}


@pytest.mark.parametrize(
    "mutation,error",
    [
        (lambda f, a: f.update(admission_wal_start_lsn="invalid"), "cutover_lsn_invalid"),
        (lambda f, a: f.update(admission_wal_offset_bytes=1), "cutover_wal_origin_changed"),
        (lambda f, a: a.update(wal_ledger=None), "cutover_wal_ledger_invalid"),
        (lambda f, a: a["wal_ledger"].update(settled_relation_wal_bytes=[]), "cutover_wal_ledger_invalid"),
        (
            lambda f, a: a["wal_ledger"]["pending_relation_wal_bytes"].update(evidence_target=1),
            "cutover_window_unresolved_or_exceeded",
        ),
        (lambda f, a: a["wal_ledger"].update(pending_control_wal_bytes=1), "cutover_finish_reservation_invalid"),
        (lambda f, a: a["wal_ledger"].update(pending_metadata_wal_bytes=0), "cutover_finish_reservation_invalid"),
        (lambda f, a: a.update(target_windows=None), "cutover_target_windows_invalid"),
        (lambda f, a: a["target_windows"].update(evidence_target=None), "cutover_target_windows_invalid"),
        (
            lambda f, a: a["target_windows"]["evidence_target"].update(window_count=0),
            "cutover_window_reconciliation_changed",
        ),
        (
            lambda f, a: a["target_windows"]["evidence_target"].update(windows_hash="bad"),
            "cutover_window_reconciliation_changed",
        ),
    ],
)
def test_rehashed_bounded_receipt_still_refuses_unsettled_or_changed_evidence(mutation, error):
    """Recomputing hashes cannot legitimize a changed WAL or window proof."""
    geometry, receipt, run_id = _bounded_cutover_receipt(0)
    assert fhir._provider_directory_profile_cutover_receipt_identity(receipt, geometry=geometry, expected_run_id=run_id)
    forecast, actual = receipt["cutover_forecast_json"], receipt["cutover_actual_json"]
    mutation(forecast, actual)
    forecast_hash, _ = fhir._profile_cutover_hashes(forecast, actual)
    actual["forecast_hash"] = forecast_hash
    _, actual_hash = fhir._profile_cutover_hashes(forecast, actual)
    receipt.update(cutover_forecast_hash=forecast_hash, cutover_actual_hash=actual_hash)
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="receipt_semantics_invalid") as rejected:
        fhir._provider_directory_profile_cutover_receipt_identity(receipt, geometry=geometry, expected_run_id=run_id)
    assert error in str(rejected.value.__cause__)


def test_bounded_cutover_requires_reserves_for_remaining_control_and_relations():
    geometry, receipt, _ = _bounded_cutover_receipt(0)
    forecast, actual = receipt["cutover_forecast_json"], receipt["cutover_actual_json"]
    metadata = cutover._validated_metadata_evidence(forecast)
    actual["cutover_wal_bytes"] = geometry.reservation_bytes_by_storage_class["wal"] + 1
    observed = cutover._lsn_bytes(actual["target_wal_start_lsn"]) + actual["cutover_wal_bytes"]
    actual["wal_observed_lsn"] = f"{observed >> 32:X}/{observed & 0xFFFFFFFF:X}"
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="total_wal_projection_exceeded"):
        cutover._bounded_cutover_ledger(geometry, forecast, actual, metadata)


def test_prior_admission_offset_cannot_hide_target_wal_overrun():
    geometry, receipt, run_id = _bounded_cutover_receipt(25)
    forecast, actual = receipt["cutover_forecast_json"], receipt["cutover_actual_json"]
    ledger = actual["wal_ledger"]["settled_relation_wal_bytes"]
    ledger.update(evidence_target=101, evidence_stage=0)
    actual["wal_ledger"]["accounted_control_wal_bytes"] = 24
    actual["target_windows"]["evidence_target"]["observed_wal_bytes"] = 101
    coordinate_map = {
        name: receipt[name]
        for name in ("build_id", "evidence_inserted", "evidence_deleted", "profile_inserted", "profile_deleted")
    }
    coordinate_map.update(run_id=run_id, forecast_hash=receipt["cutover_forecast_hash"])
    targets = cutover._validated_target_evidence(geometry, forecast, coordinate_map)
    metadata = cutover._validated_metadata_evidence(forecast)
    with pytest.raises(capacity.ProviderDirectoryProfileCapacityError, match="window_reconciliation_changed"):
        cutover._assert_bounded_cutover(geometry, forecast, actual, targets, metadata)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "row,error",
    [
        (None, "target_window_missing"),
        ({"row_count": 3, "last_key": 3}, "target_window_cursor_invalid"),
        ({"row_count": 1, "last_key": None}, "target_window_cursor_invalid"),
        ({"row_count": 1, "last_key": 2}, "target_window_cursor_invalid"),
        ({"row_count": 0, "last_key": None}, None),
    ],
)
async def test_target_window_refuses_cursor_drift(admitted_window, monkeypatch, row, error):
    admission, _ = admitted_window
    plan = SimpleNamespace(admission=admission, key_sql="target.npi", cursor_sql="TRUE")
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value=row))
    if error:
        with pytest.raises(RuntimeError, match=error):
            await cutover._select_target_window(fhir, plan, "target", "TRUE", {"capacity_after_key": 2})
    else:
        assert await cutover._select_target_window(fhir, plan, "target", "TRUE", {"capacity_after_key": 2}) == (0, None)


@pytest.mark.asyncio
async def test_target_replacement_requires_existing_transaction(monkeypatch):
    scalar = AsyncMock()
    monkeypatch.setattr(fhir.db, "_transaction_binding", Mock(return_value=None))
    monkeypatch.setattr(fhir.db, "scalar", scalar)
    with pytest.raises(RuntimeError, match="target_requires_transaction"):
        await cutover.replace_target_windows(fhir, "evidence_target", "target", "stage")
    scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["projection", "mutation"])
async def test_target_window_cannot_record_changed_rows(admitted_window, monkeypatch, failure):
    admission, _ = admitted_window
    geometry, projection = _bounded_geometry(wal_cap=10_000_000)
    admission = replace(admission, geometry=geometry, control_wal_projection=projection)
    plan = SimpleNamespace(
        admission=admission,
        relation_name="evidence_target",
        target_ref="target",
        stage_ref="stage",
        target_oid=1,
        stage_oid=2,
        column_sql="npi",
        key_sql="target.npi",
        totals_by_metric={},
    )
    layout = SimpleNamespace(
        relation_oid=1, toast_oid=0, toastable_columns=(), main_index_pages=(1,), toast_index_pages=()
    )
    monkeypatch.setattr(fhir, "_profile_cutover_layout_pair", AsyncMock(return_value=(layout, layout)))
    monkeypatch.setattr(fhir, "_profile_delta_deletion_projection", AsyncMock(return_value=(1, 8)))
    monkeypatch.setattr(fhir, "_provider_directory_profile_toast_chunk_count", AsyncMock(return_value=0))
    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", lambda *_args: _transaction())
    monkeypatch.setattr(fhir, "_reserve_profile_capacity_growth", AsyncMock())
    monkeypatch.setattr(fhir, "_reserve_provider_directory_profile_wal_budget", AsyncMock())
    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value="DELETE 0"))
    expected = 2 if failure == "projection" else 1
    with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="window_changed|rowcount_changed"):
        await cutover._apply_target_window(fhir, plan, "delete", "TRUE", {}, expected)
    assert plan.totals_by_metric == {}
    assert fhir.db.status.await_count == int(failure == "mutation")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observation",
    [
        None,
        {"wal_observed_lsn": "0/2", "cutover_wal_bytes": -1},
        {"wal_observed_lsn": "0/2", "cutover_wal_bytes": 2001},
    ],
)
async def test_target_wal_observation_refuses_missing_negative_or_overrun(admitted_window, monkeypatch, observation):
    monkeypatch.setattr(fhir.db, "first", AsyncMock(return_value=observation))
    forecast = SimpleNamespace(target_projection=SimpleNamespace(wal_bytes=2000))
    with pytest.raises(RuntimeError, match="actual_wal_missing|target_wal_exceeded"):
        await cutover._observe_cutover_wal(fhir, forecast, "0/1")


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "targets", "receipt_oid", "receipt_layout"])
async def test_initial_admission_rechecks_targets_before_consuming_lease(monkeypatch, drift):
    """An issued preflight cannot authorize targets that changed while waiting."""
    geometry = _geometry()
    target_state_by_field = _target()
    initial_targets = initial_contract.InitialTargets(
        target_state_by_field["evidence_target_oid"], target_state_by_field["profile_target_oid"], target_state_by_field
    )
    layout = SimpleNamespace(
        relation_oid=geometry.initial_receipt_oid, exact_fingerprint=geometry.initial_receipt_storage_fingerprint
    )
    observed_targets = initial_targets if drift != "targets" else replace(initial_targets, profile_target_oid=999)
    if drift == "receipt_oid":
        layout.relation_oid += 1
    if drift == "receipt_layout":
        layout.exact_fingerprint = "ff" * 32
    events = []

    monkeypatch.setattr(fhir.db, "transaction", lambda: _recorded_transaction(events))
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    monkeypatch.setattr(fhir, "_provider_directory_profile_selection_catalog", Mock(return_value=object()))
    for name in (
        "_lock_profile_capacity_preflight_state",
        "_lock_provider_directory_profile_capacity_control_run",
        "_assert_profile_capacity_build_unconsumed",
        "assert_profile_selection_current_in_transaction",
        "_consume_profile_capacity_preflight_receipt",
        "_assert_admission_run_toast",
        "_consume_admission_values",
    ):
        monkeypatch.setattr(fhir, name, AsyncMock())
    monkeypatch.setattr(initial, "capture_targets", AsyncMock(return_value=observed_targets))
    monkeypatch.setattr(initial, "receipt_layout", AsyncMock(return_value=layout))
    runtime = AsyncMock(return_value=("database-identity", {}))
    monkeypatch.setattr(fhir, "_profile_admission_runtime_state", runtime)
    identity = SimpleNamespace(initial_targets=initial_targets)
    workload = SimpleNamespace(database_identity=object(), control_wal_plan_input=object())
    args = (
        "synthetic-run",
        SimpleNamespace(attestation=object()),
        identity,
        object(),
        SimpleNamespace(build_id="synthetic-build"),
        workload,
        geometry,
    )
    if drift:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="initial_target_changed"):
            await fhir._consume_admission_transaction(*args)
        runtime.assert_not_awaited()
        fhir._consume_profile_capacity_preflight_receipt.assert_not_awaited()
        fhir._consume_admission_values.assert_not_awaited()
        assert events == ["rollback"]
    else:
        assert await fhir._consume_admission_transaction(*args) == "database-identity"
        fhir._assert_profile_capacity_build_unconsumed.assert_awaited_once_with("synthetic-build")
        fhir._consume_admission_values.assert_awaited_once()
        assert events == ["commit"]


@pytest.mark.asyncio
async def test_initial_preflight_captures_target_identity_and_locks_receipt_ledger(monkeypatch):
    payload = _target()
    targets = initial_contract.InitialTargets(payload["evidence_target_oid"], payload["profile_target_oid"], payload)
    token = initial.REQUESTED.set(True)
    serving = AsyncMock(side_effect=AssertionError("legacy adoption must not run"))
    monkeypatch.setattr(initial, "capture_targets", AsyncMock(return_value=targets))
    monkeypatch.setattr(fhir, "_provider_directory_profile_serving_state", serving)
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock())
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    try:
        preflight = await fhir._profile_capacity_preflight_serving("mrf")
        assert preflight.state == targets
        assert preflight.payload_sha256 == initial_contract.target_state_sha256(payload)
        identity = SimpleNamespace(initial_targets=targets)
        receipt_map = {
            "serving_generation_preflight": payload,
            "serving_generation_preflight_sha256": preflight.payload_sha256,
        }
        fhir._assert_profile_capacity_receipt_serving(receipt_map, identity)
        changed = copy.deepcopy(receipt_map)
        changed["serving_generation_preflight"]["profile_target_oid"] += 1
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="initial_target_changed"):
            fhir._assert_profile_capacity_receipt_serving(changed, identity)
        await fhir._lock_profile_capacity_preflight_state("mrf")
        assert initial_contract.RECEIPT_TABLE in fhir.db.status.await_args.args[0]
        serving.assert_not_awaited()
    finally:
        initial.REQUESTED.reset(token)


@pytest.mark.asyncio
@pytest.mark.parametrize("verifier_failure", [False, True])
async def test_cancelled_promotion_drains_initial_acknowledgement_verifier(monkeypatch, verifier_failure):
    """Cancellation stays cancellation and leaves no detached completion task."""
    stages = (SimpleNamespace(target_relation="synthetic-target"),)
    verifier_tasks = []

    async def is_completion_resolved(*_args):
        verifier_tasks.append(asyncio.current_task())
        if verifier_failure:
            raise RuntimeError("synthetic acknowledgement unavailable")
        return False

    monkeypatch.setattr(fhir, "_capture_provider_directory_artifact_promotion_identities", AsyncMock(return_value=()))
    monkeypatch.setattr(fhir, "_provider_directory_artifact_transaction_timeout_seconds", lambda *args, **kwargs: 1)
    monkeypatch.setattr(
        fhir, "_promote_provider_directory_artifact_bundle_transaction", AsyncMock(side_effect=asyncio.CancelledError)
    )
    monkeypatch.setattr(initial, "build_from_stages", lambda *args: object())
    monkeypatch.setattr(initial, "resolve_completion", is_completion_resolved)
    monkeypatch.setattr(initial, "preparation_timeout_seconds", AsyncMock(return_value=1))
    with pytest.raises(asyncio.CancelledError):
        await fhir._promote_provider_directory_artifact_bundle(stages)
    assert len(verifier_tasks) == 1 and verifier_tasks[0].done()


@pytest.mark.asyncio
async def test_lost_acknowledgement_refuses_unverifiable_initial_completion(monkeypatch):
    monkeypatch.setattr(initial, "resolve_completion", AsyncMock(side_effect=RuntimeError("synthetic read failed")))
    assert await fhir._is_initial_cutover_resolved((), None, ()) is False
    monkeypatch.setattr(initial, "resolve_completion", AsyncMock(side_effect=asyncio.CancelledError))
    with pytest.raises(asyncio.CancelledError):
        await fhir._is_initial_cutover_resolved((), None, ())


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_initial_preparation_deadline_recovers_only_verified_commit(monkeypatch, committed):
    """Expiry before preparation still requires immutable acknowledgement proof."""
    stages = (SimpleNamespace(target_relation="synthetic-target"),)
    monkeypatch.setattr(initial, "build_from_stages", lambda *args: object())
    monkeypatch.setattr(
        initial, "preparation_timeout_seconds", AsyncMock(side_effect=RuntimeError("synthetic_expired"))
    )
    monkeypatch.setattr(initial, "resolve_completion", AsyncMock(return_value=committed))
    monkeypatch.setattr(fhir, "_capture_provider_directory_artifact_promotion_identities", AsyncMock(return_value=()))
    transaction = AsyncMock()
    monkeypatch.setattr(fhir, "_promote_provider_directory_artifact_bundle_transaction", transaction)
    if committed:
        await fhir._promote_provider_directory_artifact_bundle(stages)
    else:
        with pytest.raises(RuntimeError, match="^synthetic_expired$"):
            await fhir._promote_provider_directory_artifact_bundle(stages)
    transaction.assert_not_awaited()
    initial.resolve_completion.assert_awaited_once_with(fhir, stages, None, ())


def test_cutover_hash_and_bounded_budget_refuse_incompatible_contract_or_excess():
    with pytest.raises(RuntimeError, match="cutover_contract_invalid"):
        fhir._profile_cutover_hash_domain({"contract_id": "unknown"}, "actual")
    geometry = _geometry()
    metadata = SimpleNamespace(wal_bytes=1, commit_envelope_bytes=1)
    with pytest.raises(RuntimeError, match="pre_dml_wal_exceeded"):
        fhir._validate_profile_cutover_budget(
            geometry, object(), metadata, SimpleNamespace(wal_bytes=geometry.reservation_bytes_by_storage_class["wal"])
        )


@pytest.mark.asyncio
async def test_initial_build_uses_full_scope_and_admitted_target_lineage(monkeypatch):
    """Existing artifact hints cannot turn explicit initial publication into a delta."""
    dataset = _dataset(source_id="cms-npd")
    context = replace(_source_context(), source_id="cms-npd")
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(return_value=(["cms-npd"], ["cms-npd"], (context,))),
    )
    serving = AsyncMock(side_effect=AssertionError("initial publication cannot adopt serving"))
    monkeypatch.setattr(fhir, "_provider_directory_profile_delta_serving_state", serving)
    token = initial.REQUESTED.set(True)
    admission_token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(None)
    try:
        ordinary = await fhir._profile_build_identity_inputs("mrf", fence, has_existing_artifacts=True)
        target_state_by_field = _target()
        initial_targets = initial_contract.InitialTargets(
            target_state_by_field["evidence_target_oid"],
            target_state_by_field["profile_target_oid"],
            target_state_by_field,
        )
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            replace(_wal_tracker_admission(), admitted_identity=replace(ordinary, initial_targets=initial_targets))
        )
        bound = await fhir._profile_build_identity_inputs("mrf", fence, has_existing_artifacts=True)
        assert bound.materialization_mode == "full_swap"
        assert bound.initial_targets == initial_targets and bound.serving_state is None
        assert bound.source_ids == ["cms-npd"] and bound.desired_source_vector == (("cms-npd", "dataset-a"),)
        assert bound.desired_source_vector_hash is not None
        assert bound.resume_lineage_hash != ordinary.resume_lineage_hash
        assert await fhir._provider_directory_profile_resource_scope_fence(fence, {"profile"}) is fence
        database = SimpleNamespace(**_geometry_database_by_field(), **_geometry_relation_by_field())
        layout = SimpleNamespace(
            relation_oid=29000, exact_fingerprint="cd" * 32, effective_tablespace_oids=(database.tablespace_oid,)
        )
        workload = SimpleNamespace(
            database_identity=database,
            batch_plan=bound.batch_plan,
            control_wal_plan_input=_control_wal_plan_input(),
            initial_receipt_layout=layout,
            artifact_worker_count=1,
            database_pool_size=4,
            artifact_projection=SimpleNamespace(projected_rows=1, projected_logical_bytes=8, projection_hash="ab" * 32),
        )
        inputs = fhir._profile_admission_inputs(synthetic_profile_execution(), bound, workload)
        assert inputs.initial_receipt_oid == layout.relation_oid
        assert inputs.initial_target_state_sha256 == initial_contract.target_state_sha256(target_state_by_field)
        layout.effective_tablespace_oids = (999,)
        with pytest.raises(RuntimeError, match="initial_receipt_tablespace_unsupported"):
            fhir._profile_admission_inputs(synthetic_profile_execution(), bound, workload)
        serving.assert_not_awaited()
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(admission_token)
        initial.REQUESTED.reset(token)


@pytest.mark.parametrize("admitted_identity", [None, SimpleNamespace(initial_targets=None)])
def test_initial_build_refuses_missing_admitted_targets(monkeypatch, admitted_identity):
    token = initial.REQUESTED.set(True)
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_capacity_admission",
        lambda: SimpleNamespace(admitted_identity=admitted_identity),
    )
    try:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="initial_admission_missing"):
            fhir._bind_initial_capacity_identity(object())
    finally:
        initial.REQUESTED.reset(token)


@pytest.mark.asyncio
async def test_repeated_cancellation_cancels_and_joins_pending_verifier(monkeypatch):
    started, drained = asyncio.Event(), asyncio.Event()
    verifier_tasks = []

    async def resolve(*_args):
        verifier_tasks.append(asyncio.current_task())
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    monkeypatch.setattr(initial, "build_from_stages", lambda *args: object())
    monkeypatch.setattr(initial, "resolve_completion", resolve)
    resolver = asyncio.create_task(fhir._resolve_initial_cutover_cancellation((), None, ()))
    try:
        await asyncio.wait_for(started.wait(), 1)
        resolver.cancel()
        await asyncio.wait_for(resolver, 1)
        assert drained.is_set() and verifier_tasks[0].cancelled()
    finally:
        for task in (resolver, *verifier_tasks):
            task.cancel()
        await asyncio.gather(resolver, *verifier_tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_ordinary_cancellation_does_not_start_initial_verifier(monkeypatch):
    monkeypatch.setattr(initial, "build_from_stages", lambda *args: None)
    verifier = AsyncMock()
    monkeypatch.setattr(initial, "resolve_completion", verifier)
    await fhir._resolve_initial_cutover_cancellation((), None, ())
    verifier.assert_not_awaited()


@pytest.mark.asyncio
async def test_signed_initial_replay_routes_to_initial_receipt_in_read_only_transaction(monkeypatch):
    execution = replace(
        synthetic_profile_execution(),
        capacity_attestation={
            "lease": {
                "signing_preflight_guard": {
                    "healthcare_request": {
                        "contract_id": initial_contract.REQUEST_CONTRACT,
                        "profile_materialization": initial_contract.MATERIALIZATION,
                    },
                }
            }
        },
    )
    replay_result_map = {"status": "published", "generation_id": "pdprofile_" + "a" * 32}
    initial_replay = AsyncMock(return_value=replay_result_map)
    ordinary_replay = AsyncMock(side_effect=AssertionError("initial execution cannot use delta receipt"))
    monkeypatch.setattr(initial, "committed_replay", initial_replay)
    monkeypatch.setattr(fhir, "_committed_replay_result", ordinary_replay)
    monkeypatch.setattr(fhir.db, "transaction", _transaction)
    monkeypatch.setattr(fhir.db, "status", AsyncMock())
    run_id = "run_" + "b" * 32
    fence = fhir.ProviderDirectoryArtifactDatasetFence(())
    assert (
        await fhir._provider_directory_profile_committed_run_replay(
            run_id=run_id,
            control_run_id=run_id,
            execution=execution,
            fence=fence,
        )
        is replay_result_map
    )
    assert "REPEATABLE READ, READ ONLY" in fhir.db.status.await_args.args[0]
    assert initial_replay.await_args.args[-3:] == (run_id, execution, fence)
    ordinary_replay.assert_not_awaited()


def _mock_initial_bundle_swaps(monkeypatch, build, cutover_state, events):
    @asynccontextmanager
    async def swap_window(*_args):
        events.append("window")
        yield
        events.append("window_finished")

    async def install(stage):
        events.append("install:" + stage.stage_table)

    async def finish(stage):
        events.append("finish:" + stage.stage_table)

    monkeypatch.setattr(initial, "lock_metadata", AsyncMock())
    monkeypatch.setattr(initial, "build_from_stages", lambda *args: build)
    monkeypatch.setattr(initial, "prepare_cutover", AsyncMock(return_value=cutover_state))
    monkeypatch.setattr(initial, "arm_cutover", AsyncMock())
    monkeypatch.setattr(initial, "begin_cutover", AsyncMock())
    monkeypatch.setattr(initial, "swap_window", swap_window)
    monkeypatch.setattr(initial, "finish_cutover", AsyncMock(side_effect=lambda *args: events.append("receipt")))
    monkeypatch.setattr(fhir, "_profile_capacity_mutation_window", lambda *args: _transaction())
    for name in (
        "_verify_active_profile_selection_at_cutover",
        "_lock_artifact_cutover_fence",
        "_assert_provider_directory_artifact_build_fence",
        "_lock_provider_directory_artifact_tables",
        "_lock_provider_directory_artifact_live_tables",
    ):
        monkeypatch.setattr(fhir, name, AsyncMock())
    monkeypatch.setattr(fhir, "_tighten_provider_directory_artifact_cutover_timeout", Mock())
    monkeypatch.setattr(fhir, "_install_provider_directory_prepared_stage", install)
    monkeypatch.setattr(fhir, "_finish_provider_directory_prepared_stage", finish)
    monkeypatch.setattr(
        fhir, "_record_address_alias_artifact_generation", AsyncMock(side_effect=lambda stage: events.append("alias"))
    )
    monkeypatch.setattr(
        fhir,
        "_promote_provider_directory_artifact_datasets",
        AsyncMock(side_effect=lambda fence: events.append("datasets")),
    )


def _mock_initial_bundle_transaction(monkeypatch, events, fence, expired):
    async def apply(stages, **_options):
        return await fhir._apply_locked_provider_directory_artifact_bundle(
            stages,
            "mrf",
            tuple(stage.target_relation for stage in stages),
            None,
            fence,
        )

    async def status(statement, **_params):
        if statement == "SET CONSTRAINTS ALL IMMEDIATE":
            events.append("constraints")

    async def remaining(_admission):
        events.append("deadline")
        if expired:
            raise RuntimeError("deadline_reached")
        return 1000

    monkeypatch.setattr(fhir.db, "transaction", lambda: _recorded_transaction(events, include_begin=True))
    monkeypatch.setattr(fhir.db, "status", status)
    monkeypatch.setattr(fhir.db, "scalar", AsyncMock(return_value=False))
    monkeypatch.setattr(fhir, "_apply_prepared_artifact_bundle_in_transaction", apply)
    monkeypatch.setattr(fhir, "_profile_capacity_remaining_ms", remaining)


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_initial_bundle_finishes_swaps_and_constraints_before_commit(monkeypatch, expired):
    """The initial pair, ordinary artifact, receipt, and deadline share one transaction."""
    build, cutover_state = object(), object()
    stages = (
        fhir.ProviderDirectoryPreparedArtifactStage(
            "mrf", "evidence", fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, AsyncMock(), profile_initial_build=build
        ),
        fhir.ProviderDirectoryPreparedArtifactStage(
            "mrf", "profile", fhir.profile_artifact.PROFILE_TABLE, AsyncMock(), profile_initial_build=build
        ),
        fhir.ProviderDirectoryPreparedArtifactStage("mrf", "address", "synthetic_address", AsyncMock()),
    )
    fence = SimpleNamespace(datasets=())
    events = []
    _mock_initial_bundle_swaps(monkeypatch, build, cutover_state, events)
    _mock_initial_bundle_transaction(monkeypatch, events, fence, expired)
    if expired:
        with pytest.raises(RuntimeError, match="deadline_reached"):
            await fhir._promote_provider_directory_artifact_bundle_transaction(stages)
    else:
        await fhir._promote_provider_directory_artifact_bundle_transaction(stages)
    assert events == [
        "transaction",
        "window",
        "install:evidence",
        "install:profile",
        "finish:evidence",
        "finish:profile",
        "window_finished",
        "install:address",
        "finish:address",
        "alias",
        "datasets",
        "receipt",
        "constraints",
        "deadline",
        "rollback" if expired else "commit",
    ]
    if not expired:
        remove = AsyncMock()
        monkeypatch.setattr(fhir, "_remove_provider_directory_artifact_stage", remove)
        await fhir.ProviderDirectoryArtifactBundle(stages=list(stages), promoted=True).cleanup()
        remove.assert_awaited_once_with(stages[-1])
