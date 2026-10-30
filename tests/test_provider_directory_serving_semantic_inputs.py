# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Input-only equality never promotes physical or unknown legacy identity into freshness."""

import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_bundle_preparation as capture
from process import provider_directory_cms_native_inputs as existing
from process import provider_directory_profile_runtime_observation as runtime
from process import provider_directory_serving_semantic_inputs as semantics
from process import reference_family_result_generation as reference
from tests.test_provider_directory_serving_input_capture import _fixture


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


async def test_semantic_identity_excludes_outputs_actor_and_requested_scope(monkeypatch):
    case, directory, fence, state, roots, limits = _fixture(monkeypatch)
    first = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    state.serving = replace(state.serving, generation_id="pdprofile_" + "f" * 32, control_generation=9)
    roots["entity_address_unified"]["oid"] += 10
    roots["entity_address_unified"]["owner_oid"] += 1
    state.native["relations"][0]["relation_oid"] = roots["entity_address_unified"]["oid"]
    second = await capture.capture_serving_inputs(
        directory,
        case.execution,
        fence,
        retention_limits=limits,
        refresh_source_ids=[case.execution.attestation.pairs[-1]["source_id"]],
    )
    assert first.scope_payload["input_sha256"] != second.scope_payload["input_sha256"]
    assert first.semantic_inputs == second.semantic_inputs
    assert first.semantic_inputs["dependency_proofs_complete"] is True
    detached = first.semantic_inputs
    detached["healthcare_source_commit"] = "b" * 40
    assert first.semantic_inputs["healthcare_source_commit"] == "a" * 40
    assert first.initial_targets is None


@pytest.mark.parametrize("recovered", [False, True])
async def test_original_initial_baseline_accessor_never_nests_recovery(monkeypatch, recovered):
    from process import provider_directory_profile_initial_recovery as initial_recovery
    from tests.test_provider_directory_profile_initial_recovery import _witness

    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch, initial=True, history=True)
    observed = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    original = observed.initial_targets
    if recovered:
        payload = json.loads(observed._payload_json)
        payload["predecessor"]["profile"] = initial_recovery.recovered_target_state(original, _witness(original))
        observed = replace(observed, _payload_json=json.dumps(payload))
        assert observed.predecessor["profile"]["initial_commit_receipt_absent"] is False
    assert observed.initial_targets == original
    assert observed.initial_targets["initial_commit_receipt_absent"] is True
    assert "recovered_initial_publication" not in observed.initial_targets
    assert observed.initial_targets["historical_publication"]["profile_as_of"] is None
    observed.initial_targets["profile_target_bytes"] += 8192
    assert observed.initial_targets == original


@pytest.mark.parametrize("changed", ["revision", "alias", "facility_content", "facility_option", "mapper", "unknown"])
async def test_same_physical_inputs_do_not_hide_semantic_change(monkeypatch, changed):
    case, directory, fence, state, _roots, limits = _fixture(monkeypatch)
    before = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    physical = deepcopy(state.dependencies)
    if changed == "revision":
        state.dependency_proofs["mrf-address"]["identity"]["generation"] += 1
    if changed == "alias":
        state.native["alias_state"]["generation"] += 1
    if changed == "facility_content":
        state.facility[1]["identities"]["enrollment"]["provider_enrollment_hospital"] = {"row_sha256": "ab" * 32}
    if changed == "facility_option":
        state.facility[0]["candidate_limit"] += 1
    if changed == "mapper":
        monkeypatch.setattr(runtime, "build_baked_healthcare_source_commit", lambda: "b" * 40)
    if changed == "unknown":
        state.dependency_proofs["mrf-address"] = None
    after = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    assert state.dependencies == physical
    assert before.semantic_inputs != after.semantic_inputs
    assert before.scope_payload["native_address_input_hash"] != after.scope_payload["native_address_input_hash"]
    assert after.semantic_inputs["dependency_proofs_complete"] is (changed != "unknown")


@pytest.mark.parametrize("changed", ["extra", "missing", "hash", "bool", "date", "source"])
async def test_semantic_journal_codec_is_closed(monkeypatch, changed):
    case, directory, fence, _state, _roots, limits = _fixture(monkeypatch)
    observed = await capture.capture_serving_inputs(directory, case.execution, fence, retention_limits=limits)
    raw = observed.semantic_inputs
    if changed == "extra":
        raw["worker_recipe"] = {}
    if changed == "missing":
        del raw["native_dependencies_sha256"]
    if changed == "hash":
        raw["source_vector_sha256"] = "A" * 64
    if changed == "bool":
        raw["dependency_proofs_complete"] = 1
    if changed == "date":
        raw["profile_as_of"] = "20261030"
    if changed == "source":
        raw["healthcare_source_commit"] = "0" * 40
    with pytest.raises(ValueError):
        capture.validated_semantic_inputs(raw)


async def test_protected_selection_reuses_existing_authentication_without_live_fallback(monkeypatch):
    observed = AsyncMock(return_value=None)
    monkeypatch.setattr(semantics, "_canonical_dependency_proof", observed)
    selected_by_name = {
        name: {"relation_oid": index} for index, name in enumerate(("npi_address", "zip_state", "zcta5"), 1)
    }
    proofs = await semantics.capture_semantic_dependencies(object(), "fixture", selected_by_name)
    assert proofs["npi"] == {"kind": "immutable", "identity": [selected_by_name["npi_address"]]}
    assert proofs["tiger"]["kind"] == "immutable"
    assert {call.args[2] for call in observed.await_args_list} == {"mrf-address", "cms-doctors", "geo"}
    assert proofs["mrf-address"] is None
    with pytest.raises(ValueError):
        await semantics.capture_semantic_dependencies(object(), "fixture", {"zip_state": selected_by_name["zip_state"]})


@pytest.mark.parametrize("state", ["missing_ledger", "unpublished", "untracked"])
async def test_unknown_legacy_input_stays_unqualified_without_registration(monkeypatch, state):
    results_by_state = {"missing_ledger": [False], "unpublished": [True, False], "untracked": [True, True, False]}
    session = SimpleNamespace(scalar=AsyncMock(side_effect=results_by_state[state]))
    observe = AsyncMock()
    monkeypatch.setattr(reference, "capture_reference_family_serving_generation", observe)
    assert await semantics._canonical_dependency_proof(session, "fixture", "mrf-address") is None
    observe.assert_not_awaited()


async def test_guard_failure_is_not_relabelled_as_unknown(monkeypatch):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[True, True, True]))
    observer = AsyncMock(side_effect=RuntimeError("changed_existing_revision_guards"))
    monkeypatch.setattr(existing, "_npi_revision", observer)
    with pytest.raises(RuntimeError, match="changed_existing_revision_guards"):
        await semantics._canonical_dependency_proof(session, "fixture", "npi")
    observer.assert_awaited_once_with(session, "fixture")


async def test_cutover_locks_mutable_inputs_on_the_original_session():
    bindings_by_name = {}
    for index, (namespace, name) in enumerate(semantics.dependencies.projection._PROJECTION_DEPENDENCIES, 1):
        schema = namespace or "fixture"
        bindings_by_name[schema + "." + name] = dict(
            schema_name=schema, table_name=name, relation_oid=index, relfilenode=index
        )
    proof_by_family = {family: {"kind": "immutable", "identity": {}} for family in semantics._FAMILIES}
    proof_by_family["npi"]["kind"] = "revision"
    proof_by_family["mrf-address"]["kind"] = "revision"
    captured = SimpleNamespace(
        predecessor={
            "native": {"semantic_dependency_proofs": proof_by_family, "current_dependency_bindings": bindings_by_name}
        }
    )
    session = SimpleNamespace(execute=AsyncMock())
    await semantics.lock_semantic_dependencies(session, "fixture", captured)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert len(statements) == 2 and "ACCESS SHARE MODE" in statements[0]
    from process.npi_result_generation import RELATION_NAMES

    expected_names = {*RELATION_NAMES, "mrf_address", "mrf_address_evidence"}
    assert (
        statements[1]
        == "LOCK TABLE " + ",".join(f'"fixture"."{name}"' for name in sorted(expected_names)) + " IN SHARE MODE NOWAIT"
    )
