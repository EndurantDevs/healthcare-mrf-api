# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real capture orchestration with existing initial and delta contracts over observed SQL."""

import importlib
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_dependency_bindings as dependencies
from process import entity_address_preparation_admission as address_admission
from process import entity_address_result_generation as address_generation
from process import entity_address_snapshot_serving as address_serving
from process import provider_directory_artifact_bundle_preparation as capture
from process import provider_directory_generic_source_policy as source
from process import provider_directory_ordinary_facility_inputs as facility
from process import provider_directory_ordinary_retention_capacity as retention
from process import provider_directory_profile_runtime_observation as runtime
from process import provider_directory_profile_selection as selection
from process import provider_directory_serving_semantic_inputs as semantics
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.provider_directory_serving_capacity_signing_test_support import serving_case
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME

original = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", raising=False)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE", raising=False)


def _serving_state(case):
    return original._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=1,
        generation_id="pdprofile_" + "a" * 32,
        selection_proof_id="aa" * 32,
        authority_revision=1,
        profile_schema_version=original.profile_artifact.PROFILE_SCHEMA_VERSION,
        profile_strategy_version=original.profile_artifact.PROFILE_BUILD_STRATEGY_VERSION,
        source_vector=tuple(
            (pin["source_id"], "previous-dataset" if ordinal == 0 else pin["dataset_id"])
            for ordinal, pin in enumerate(case.execution.attestation.pairs)
        ),
        source_vector_hash="ab" * 32,
        source_context_vector=tuple((pin["source_id"], "ac" * 32) for pin in case.execution.attestation.pairs),
        source_context_vector_hash="ad" * 32,
        executable_plan_hash="ae" * 32,
        evidence_target_oid=20001,
        profile_target_oid=20002,
        evidence_rows=0,
        profile_rows=0,
        profile_as_of=case.execution.attestation.desired_profile_as_of,
        published_at=VALIDATION_TIME.isoformat(),
    )


def _legacy_history():
    historical_result = selection.profile_selection_result(
        synthetic_profile_execution(),
        profile_generation_id="pdprofile_" + "b" * 32,
        profile_rows=1,
        profile_source_evidence_rows=2,
        profile_as_of="2026-07-01",
    )
    historical_result.pop("profile_as_of")
    return {"run_id": "run_" + "b" * 32, "finished_at": VALIDATION_TIME, "result": historical_result}


def _observed_database(state, roots_by_name):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=lambda _query: state.isolation), execute=AsyncMock())
    binding = SimpleNamespace(session=session)

    async def first(query, **values):
        if "role.rolcanlogin" in query:
            assert values["role"] == "ordinary_builder"
            return dict(owner_oid=16400, schema_oid=16401, can_create=True)
        if "pg_total_relation_size(c.oid)" in query:
            return deepcopy(roots_by_name[values["name"]])
        if "SELECT run_id, finished_at" in query:
            return deepcopy(state.history)
        raise AssertionError(query)

    async def scalar(query, **values):
        if "pg_total_relation_size" in query:
            return next((root["total_bytes"] for root in roots_by_name.values() if root["oid"] == values["oid"]), 0)
        if query.startswith("SELECT EXISTS") or "to_regclass(:ref) IS NOT NULL" in query:
            return False
        raise AssertionError(query)

    return SimpleNamespace(
        engine=SimpleNamespace(url=SimpleNamespace(username="ordinary_builder")),
        _transaction_binding=lambda: binding,
        first=AsyncMock(side_effect=first),
        scalar=AsyncMock(side_effect=scalar),
        status=AsyncMock(),
    )


def _directory(case, state, database):
    source_id = case.execution.attestation.pairs[0]["source_id"]
    return SimpleNamespace(
        db=database,
        _schema=lambda: "fixture",
        _pagination_checkpoint_row_mapping=dict,
        _PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION=ContextVar("capture_execution", default=None),
        _provider_directory_profile_serving_state=AsyncMock(side_effect=lambda _schema: state.serving),
        _provider_directory_profile_source_vector=original._provider_directory_profile_source_vector,
        _provider_directory_profile_source_context_vector=lambda _contexts: tuple(
            (pin["source_id"], "ac" * 32) for pin in case.execution.attestation.pairs
        ),
        _provider_directory_profile_scope_source_ids=AsyncMock(return_value=([source_id], [], ())),
        _provider_directory_profile_delta_sources=original._provider_directory_profile_delta_sources,
        _provider_directory_profile_selection_catalog=lambda: {},
        assert_profile_selection_current_in_transaction=AsyncMock(),
        _assert_profile_selection_matches_artifact_fence=original._assert_profile_selection_matches_artifact_fence,
        _verify_provider_directory_artifact_dataset_fence=AsyncMock(),
        _locked_profile_adoption_target_oids=AsyncMock(return_value=(20002, 20001)),
        _provider_directory_profile_relation_storage_fingerprint=AsyncMock(
            return_value=SimpleNamespace(exact_fingerprint="ab" * 32)
        ),
        _unscoped_qt=original._unscoped_qt,
        profile_artifact=original.profile_artifact,
        ImportRun=original.ImportRun,
        PROFILE_SELECTION_RESULT_METRIC=original.PROFILE_SELECTION_RESULT_METRIC,
        _profile_adoption_targets=AsyncMock(),
        _profile_adoption_attested_row_counts=original._profile_adoption_attested_row_counts,
    )


def _capture_observers(monkeypatch, state):
    state.dependency_proofs = {
        name: {"kind": "revision", "identity": {"generation": 1}} for name in semantics._FAMILIES
    }
    monkeypatch.setattr(
        semantics,
        "capture_semantic_dependencies",
        AsyncMock(side_effect=lambda *_args: deepcopy(state.dependency_proofs)),
    )
    monkeypatch.setattr(runtime, "build_baked_healthcare_source_commit", lambda: "a" * 40)
    monkeypatch.setattr(source, "is_source_storage_required", lambda: False)
    monkeypatch.setattr(
        retention, "retention_layout_sha256", AsyncMock(side_effect=lambda _directory, relation: f"{relation.oid:064x}")
    )
    state.native = {"relations": [{"relation_oid": 20004}], "alias_state": {"generation": 7}}
    monkeypatch.setattr(
        address_serving,
        "_read_observed_serving",
        AsyncMock(side_effect=lambda *_args, **_kw: SimpleNamespace(as_dict=lambda: deepcopy(state.native))),
    )
    monkeypatch.setattr(
        address_generation,
        "read_entity_address_result_generation_authority",
        AsyncMock(return_value=SimpleNamespace(as_dict=lambda: {"local_generation": 4})),
    )
    state.dependencies = {"fixture.geo_zip_lookup": {"relation_oid": 40000, "relfilenode": 40001}}
    monkeypatch.setattr(dependencies, "capture_selected_publication_bindings", AsyncMock(return_value=None))
    monkeypatch.setattr(
        dependencies,
        "capture_publication_dependency_bindings",
        AsyncMock(side_effect=lambda *_args: deepcopy(state.dependencies)),
    )
    state.facility = (
        {
            "enabled": False,
            "include_nppes": False,
            "include_other_identifier": False,
            "candidate_limit": 25,
            "options": {},
        },
        {"inference_available": False, "relations": {}, "identities": {"enrollment": {}}},
    )
    monkeypatch.setattr(
        facility, "capture_facility_inputs", AsyncMock(side_effect=lambda *_args, **_kwargs: deepcopy(state.facility))
    )
    state.nucc = {"generation": 7}
    monkeypatch.setattr(
        dependencies, "capture_inference_inputs", AsyncMock(side_effect=lambda *_args: deepcopy(state.nucc))
    )


def _fixture(monkeypatch, *, initial=False, history=False):
    case = serving_case(initial_mode=initial)
    state = SimpleNamespace(
        serving=None if initial else _serving_state(case),
        history=_legacy_history() if history else None,
        isolation="repeatable read",
    )
    oid_by_family = {family: ordinal for ordinal, family in enumerate(retention._FAMILIES, 20001)}
    oid_by_family.update(profile=20002, evidence=20001)
    roots_by_name = {
        names[0]: dict(oid=oid_by_family[family], owner_oid=25000, schema_oid=16401, total_bytes=8192, persistence="p")
        for family, names in retention._FAMILIES.items()
    }
    directory = _directory(case, state, _observed_database(state, roots_by_name))
    fence = original.ProviderDirectoryArtifactDatasetFence(
        tuple(
            original.ProviderDirectoryArtifactDataset(
                pin["source_id"],
                pin["endpoint_id"],
                pin["dataset_id"],
                pin["acquisition_root_run_id"],
                dataset_hash=pin["dataset_hash"],
                status=pin["publication_status"],
                is_current=pin["is_current"],
            )
            for pin in case.execution.attestation.pairs
        )
    )
    _capture_observers(monkeypatch, state)
    limits_by_field = dict(max_retention_data_bytes=65536, max_retention_wal_bytes=65536, max_commit_wal_bytes=65536)
    return case, directory, fence, state, roots_by_name, limits_by_field


async def test_real_initial_capture_retains_undated_history_and_exact_original_targets(monkeypatch):
    case, directory, fence, state, roots, limits = _fixture(monkeypatch, initial=True, history=True)
    before = deepcopy(state.history)
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert type(captured) is capture.CapturedServingInputs and captured.database is directory.db
    historical = captured.predecessor["profile"]["historical_publication"]
    assert historical["profile_as_of"] is None and "profile_as_of" not in historical["result"]
    assert state.history == before
    assert captured.scope_payload["owner_oid"] == 16400
    assert {root["owner_oid"] for root in captured.predecessor["roots"]} == {25000}
    assert captured.scope_payload["resident_retention_data_bytes"] == 16384
    assert [entry["retention_mode"] for entry in captured.retention_plan["families"]] == [
        "existing_initial_target",
        "existing_initial_target",
    ] + ["owned_copy"] * 8
    directory._profile_adoption_targets.assert_awaited_once()
    directory.db.status.assert_not_awaited()
    directory.db._transaction_binding().session.execute.assert_not_awaited()
    assert roots["provider_directory_profile"]["oid"] == 20002


async def test_delta_capture_derives_exact_source_dataset_and_existing_partial_recipe(monkeypatch):
    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch)
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    pin = case.execution.attestation.pairs[0]
    assert captured.address_task == {
        "refresh_mode": "provider-directory-partial",
        "serving_only_refresh": True,
        "test_mode": False,
        "provider_directory_source_ids": [pin["source_id"]],
        "provider_directory_partial_scope": "latest-run",
        "provider_directory_dataset_id": pin["dataset_id"],
        "provider_directory_run_id": pin["acquisition_root_run_id"],
    }
    assert captured.scope_payload["resident_retention_data_bytes"] == 0
    assert captured.overlay_fence.datasets == fence.datasets[:1]
    assert captured.overlay_fence != fence
    assert all(entry["retention_mode"] == "owned_copy" for entry in captured.retention_plan["families"])
    captured.address_task["provider_directory_source_ids"].clear()
    assert captured.address_task["provider_directory_source_ids"] == [pin["source_id"]]
    with pytest.raises(FrozenInstanceError):
        captured.database = object()


@pytest.mark.parametrize("changed", ["root_oid", "owner", "resident_bytes", "native", "history", "engine"])
async def test_recheck_refuses_actual_predecessor_or_custody_drift(monkeypatch, changed):
    case, directory, fence, state, roots, limits = _fixture(monkeypatch, initial=True, history=True)
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    root = roots["provider_directory_profile"]
    if changed == "root_oid":
        root["oid"] += 10
    if changed == "owner":
        root["owner_oid"] += 1
    if changed == "resident_bytes":
        root["total_bytes"] += 8192
    if changed == "native":
        state.native["alias_state"]["generation"] += 1
    if changed == "history":
        state.history["run_id"] = "run_" + "c" * 32
    if changed == "engine":
        directory.db.engine = object()
    with pytest.raises(RuntimeError, match="capture_changed"):
        await captured.recheck(directory)


@pytest.mark.parametrize("changed", ["unfunded", "source_authority", "isolation", "missing_initial_mode"])
async def test_capture_refuses_before_build_on_unfunded_or_untrusted_input(monkeypatch, changed):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch, initial=True)
    if changed == "unfunded":
        limits["max_retention_data_bytes"] = 16383
    if changed == "source_authority":
        limits["owner_oid"] = 1
    if changed == "isolation":
        state.isolation = "read committed"
    if changed == "missing_initial_mode":
        case.execution = replace(case.execution, capacity_attestation={})
    with pytest.raises((RuntimeError, ValueError)):
        await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    directory.db.status.assert_not_awaited()


async def test_unchanged_scope_never_becomes_an_empty_list_unscoped_recipe(monkeypatch):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    pin = case.execution.attestation.pairs[0]
    state.serving = replace(
        state.serving,
        source_vector=tuple(
            (selected["source_id"], selected["dataset_id"]) for selected in case.execution.attestation.pairs
        ),
    )
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert captured.is_unchanged and captured.address_task == {}
    assert captured.overlay_fence.datasets == ()
    requested = await capture.capture_serving_inputs(
        directory, case.execution, fence, retention_limits=limits, refresh_source_ids=[pin["source_id"]]
    )
    assert not requested.is_unchanged and requested.address_task["provider_directory_source_ids"] == [pin["source_id"]]
    assert requested.scope_payload["requested_source_ids"] == [pin["source_id"]]
    with pytest.raises(RuntimeError, match="capture_changed"):
        await capture.capture_serving_inputs(
            directory, case.execution, fence, retention_limits=limits, refresh_source_ids=["unknown-source"]
        )


async def test_current_dependency_change_requires_recapture_without_claiming_old_output_freshness(monkeypatch):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert address_serving._read_observed_serving.await_args.kwargs["require_current_dependencies"] is False
    state.dependencies["fixture.geo_zip_lookup"]["relfilenode"] += 1
    with pytest.raises(RuntimeError, match="capture_changed"):
        await captured.recheck(directory)
    successor = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert successor.scope_payload["native_address_input_hash"] != captured.scope_payload["native_address_input_hash"]
    assert successor.predecessor["native"]["observed"] == captured.predecessor["native"]["observed"]


async def test_requested_scope_adds_work_without_omitting_server_detected_changes(monkeypatch):
    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch)
    source_ids = [pin["source_id"] for pin in case.execution.attestation.pairs]
    captured = await capture.capture_serving_inputs(
        directory, case.execution, fence, retention_limits=limits, refresh_source_ids=[source_ids[1]]
    )
    assert captured.affected_scope["refresh_source_ids"] == source_ids
    assert captured.address_task["provider_directory_source_ids"] == source_ids
    assert captured.address_task["provider_directory_partial_scope"] == "all"
    assert "provider_directory_dataset_id" not in captured.address_task


async def test_date_and_removed_source_changes_remain_mandatory(monkeypatch):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    state.serving = replace(
        state.serving,
        source_vector=(*state.serving.source_vector, ("retired-source", "retired-dataset")),
        profile_as_of="2026-06-30",
    )
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert captured.affected_scope["removed_source_ids"] == ["retired-source"]
    assert captured.affected_scope["refresh_source_ids"] == [
        pin["source_id"] for pin in case.execution.attestation.pairs
    ]
    assert "retired-source" in captured.address_task["provider_directory_source_ids"]
    assert captured.address_task["provider_directory_partial_scope"] == "all"


async def test_source_observer_retains_the_original_builder_identity(monkeypatch):
    _case, directory, _fence, _state, _roots, _limits = _fixture(monkeypatch)
    issued_factory = SimpleNamespace(database=directory.db, _require_current_policy=Mock())
    factory_context = ContextVar("capture_factory", default=issued_factory)
    monkeypatch.setattr(source, "_FACTORY", factory_context)
    monkeypatch.setattr(source, "is_source_storage_required", lambda: True)
    observed = await capture._capture_builder(directory)
    assert observed == {"owner_oid": 16400, "schema_oid": 16401}
    issued_factory._require_current_policy.assert_called_once_with()
    issued_factory.database = object()
    with pytest.raises(RuntimeError, match="capture_changed"):
        await capture._capture_builder(directory)


async def test_inference_choice_and_used_nucc_generation_are_bound(monkeypatch):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    disabled = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    dependencies.capture_inference_inputs.assert_not_awaited()
    assert disabled.affected_scope["inference_enabled"] is False
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "true")
    with pytest.raises(RuntimeError, match="capture_changed"):
        await disabled.recheck(directory)
    enabled = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert enabled.affected_scope["inference_enabled"] is True
    assert enabled.scope_payload["input_sha256"] != disabled.scope_payload["input_sha256"]
    state.nucc["generation"] += 1
    with pytest.raises(RuntimeError, match="capture_changed"):
        await enabled.recheck(directory)


async def test_actual_inference_decision_refuses_environment_drift(monkeypatch):
    from process import provider_directory_ordinary_address_admission as ordinary

    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch)
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    scope = SimpleNamespace(admission=SimpleNamespace(captured_inputs=captured, plan=case.plan))
    monkeypatch.setattr(address_admission, "_ADMISSION", ContextVar("capture_admission", default=scope))
    monkeypatch.setattr(ordinary, "require_capture", lambda _fhir, _plan, captured: captured)
    assert address_admission.inference_enabled(False) is False
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", "true")
    with pytest.raises(RuntimeError, match="inference differs"):
        address_admission.inference_enabled(False)
    with pytest.raises(RuntimeError, match="inference differs"):
        address_admission.inference_enabled(True)


def test_existing_normal_and_test_inference_options_are_preserved(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_ENABLE_INFERENCE", " YES ")
    assert address_admission.inference_enabled(False) is True
    assert address_admission.inference_enabled(True) is False
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEST_ENABLE_INFERENCE", "on")
    assert address_admission.inference_enabled(True) is True


async def test_removed_only_scope_projects_zero_inputs_without_discovery(monkeypatch):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    desired_pairs = tuple((pin["source_id"], pin["dataset_id"]) for pin in case.execution.attestation.pairs)
    state.serving = replace(state.serving, source_vector=(*desired_pairs, ("retired-source", "retired-dataset")))
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert captured.address_task["provider_directory_source_ids"] == ["retired-source"]
    assert captured.overlay_fence.datasets == () and not captured.is_unchanged
    monkeypatch.setattr(original, "db", directory.db)
    directory.db.first.reset_mock()
    resource_types = original._provider_directory_artifact_resource_types(
        {"address_overlay"}, publish_corroboration=False
    )
    projection = await original._provider_directory_artifact_scope_exact_projection(
        "fixture", captured.overlay_fence, resource_types, source_fence=captured.overlay_fence, batch_size=10
    )
    assert projection.projected_rows == projection.projected_logical_bytes == 0
    assert all(not table.batches for table in projection.tables)
    directory.db.first.assert_not_awaited()


@pytest.mark.parametrize("changed", [None, "factory", "database", "required", "during_capture"])
async def test_capture_preserves_exact_local_factory_without_serialized_authority(monkeypatch, changed):
    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch)
    issued = SimpleNamespace(database=directory.db, _require_current_policy=Mock())
    factory_context = ContextVar("capture_original_factory", default=None if changed == "required" else issued)
    monkeypatch.setattr(source, "_FACTORY", factory_context)
    if changed == "during_capture":

        async def swap_factory(*_args):
            factory_context.set(SimpleNamespace(database=directory.db, _require_current_policy=Mock()))

        directory.assert_profile_selection_current_in_transaction.side_effect = swap_factory
        with pytest.raises(RuntimeError, match="capture_changed"):
            await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
        return
    captured = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert captured._factory is (None if changed == "required" else issued)
    assert "factory" not in captured._payload_json and "factory" not in captured._limits_json
    if changed == "factory":
        factory_context.set(SimpleNamespace(database=directory.db, _require_current_policy=Mock()))
    elif changed == "database":
        issued.database = object()
    elif changed == "required":
        monkeypatch.setattr(source, "is_source_storage_required", lambda: True)
    if changed:
        with pytest.raises(RuntimeError, match="capture_changed"):
            await captured.recheck(directory)
    else:
        await captured.recheck(directory)
        assert issued._require_current_policy.call_count >= 2
