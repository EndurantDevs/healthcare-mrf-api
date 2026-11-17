# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Cleanup preserves published names and refuses changed logical candidate inputs."""

import copy
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_failed_cleanup as cleanup
from process import provider_directory_profile_initial_contract as initial_contract
from process import provider_directory_profile_selection as selection
from tests.provider_directory_profile_capacity_signing_guard_test_support import synthetic_profile_execution
from tests.test_profile_failed_cleanup_preserved_state import database_context
from tests.test_provider_directory_profile_failed_cleanup import (
    _legacy_publication_fixture,
    authorization_fixture,
    initial_authorization_fixture,
)
from tests.test_provider_directory_profile_initial_cleanup import _manifest_fixture


def _published_initial_target(body, reference):
    execution = synthetic_profile_execution()
    result = selection.profile_selection_result(
        execution,
        profile_generation_id="pdprofile_" + "b" * 32,
        profile_rows=1,
        profile_source_evidence_rows=2,
        profile_as_of="2026-08-09",
    )
    result.pop("profile_as_of")
    result["generation"] = body["checkpoint"]["control_generation"] + int(reference != "control_generation")
    target = copy.deepcopy(body["initial"]["target_state"])
    target.update(
        resolution="legacy_as_of_unknown",
        evidence_rows=2,
        profile_rows=1,
        historical_publication=dict(
            run_id="run_" + "b" * 32,
            result_sha256=initial_contract.result_sha256(result),
            result=result,
            profile_as_of=None,
            temporal_metadata="not_recorded_by_producer",
        ),
    )
    return initial_contract.validated_target_state(target)


@pytest.mark.parametrize("reference", ["run_id", "selection_proof_id", "control_generation"])
def test_initial_cleanup_cannot_dispose_a_generation_referenced_by_published_history(reference):
    envelope, _trust, _key, geometry = initial_authorization_fixture(with_geometry=True)
    body = envelope["authorization"]
    target = _published_initial_target(body, reference)
    history = target["historical_publication"]
    body["checkpoint"]["owner_run_id"] = history["run_id"] if reference == "run_id" else "run_" + "a" * 32
    if reference == "selection_proof_id":
        body["checkpoint"]["selection_proof_id"] = geometry["selection_proof_id"] = history["result"]["proof_id"]
    target_hash = initial_contract.target_state_sha256(target)
    body["initial"].update(target_state=target, initial_target_state_sha256=target_hash)
    geometry["initial_target_state_sha256"] = target_hash
    body["checkpoint"]["capacity_geometry_hash"] = capacity.capacity_geometry_hash(
        capacity.validated_capacity_geometry(geometry)
    )
    before = copy.deepcopy((body, geometry))
    with pytest.raises(RuntimeError, match="build_is_serving$"):
        cleanup._validate_initial_request(dict(body, capacity_geometry=geometry))
    assert (body, geometry) == before


@pytest.mark.asyncio
async def test_cleanup_decodes_persisted_target_names_without_rewriting_serving_metadata(monkeypatch):
    envelope, _trust, _key = authorization_fixture()
    publication = _legacy_publication_fixture()
    targets_by_role = {role: {"schema": "retained", "name": role + "_published"} for role in ("evidence", "profile")}
    row_by_field = dict(
        generation_id=publication["published_generation_id"],
        selection_proof_id=publication["published_proof_id"],
        control_generation=publication["published_control_generation"],
        targets_json=json.dumps(targets_by_role),
    )
    answers = [True] * 6 + [False, False]
    fhir = database_context(
        all=AsyncMock(return_value=[SimpleNamespace(_mapping=row_by_field)]), scalar=AsyncMock(side_effect=answers)
    )
    fhir._provider_directory_profile_serving_generation_ref = lambda schema: schema + ".serving"
    monkeypatch.setattr(cleanup, "_published_history_manifest", AsyncMock(return_value=publication))
    before = copy.deepcopy(row_by_field)
    observed, serving = await cleanup._publication_manifest(
        fhir, "catalog", {}, envelope["authorization"]["checkpoint"], "published-run", locked=True
    )
    assert serving.targets == targets_by_role and row_by_field == before
    assert observed["targets"] == [{"role": role, **targets_by_role[role]} for role in ("evidence", "profile")]
    assert all(
        "retained." + targets_by_role[role]["name"] in fhir.db.status.await_args_list[index + 1].args[0]
        for index, role in enumerate(targets_by_role)
    )
    assert all(call.args[0].startswith("LOCK TABLE ") for call in fhir.db.status.await_args_list)


def test_initial_cleanup_decodes_persisted_owner_params_without_mutating_checkpoint(monkeypatch):
    _target, geometry, checkpoint, owner, *_rest = _manifest_fixture(monkeypatch, drift=False)
    original_params = copy.deepcopy(owner["params"])
    owner["params"] = cleanup.canonical(original_params)
    before = copy.deepcopy((checkpoint, owner))
    params, binding = cleanup._initial_cleanup_binding(checkpoint, owner, geometry)
    assert params == original_params and (checkpoint, owner) == before
    assert binding["build_id"] == checkpoint["build_id"]
    assert binding["to_source_vector_hash"] == checkpoint["desired_source_vector_hash"]
    params["provider_directory_profile_selection_attestation"]["proof_id"] = "changed"
    assert json.loads(owner["params"]) == original_params


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["delta", "receipt_name", "missing_receipt_name"])
async def test_initial_cleanup_refuses_changed_history_or_receipt_before_disposal(monkeypatch, change):
    from tests.test_provider_directory_profile_initial_cleanup import fhir

    _target, geometry, checkpoint, owner, *_rest = _manifest_fixture(monkeypatch, drift=False)
    if change == "delta":
        fhir.db.scalar.side_effect = lambda statement, **_params: "SELECT EXISTS" in statement
        reason = "initial_delta_history"
    else:
        relations = copy.deepcopy(geometry.relations)
        if change == "receipt_name":
            relations["initial_receipt"]["name"] = "other_initial_receipt"
        else:
            relations.pop("initial_receipt")
        geometry = replace(geometry, relations=relations)
        reason = "initial_receipt_layout_changed"
    before = copy.deepcopy((checkpoint, owner))
    with pytest.raises(RuntimeError, match=reason + "$"):
        await cleanup._initial_manifest(fhir, "mrf", geometry, locked=True, checkpoint=checkpoint, owner=owner)
    assert (checkpoint, owner) == before
    assert all(call.args[0].startswith("LOCK TABLE ") for call in fhir.db.status.await_args_list)
    fhir.db.all.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconciliation_refuses_a_different_persisted_request_before_opening_transaction(monkeypatch):
    envelope, _trust, _key = authorization_fixture()
    different = copy.deepcopy(envelope)
    different["authorization"]["operation_id"] = "different-operation"
    transaction = AsyncMock(side_effect=AssertionError("transaction opened after request mismatch"))
    fhir = SimpleNamespace(_schema=lambda: "catalog", db=SimpleNamespace(transaction=transaction))
    monkeypatch.setattr(cleanup, "_read_claim", AsyncMock(return_value=object()))
    monkeypatch.setattr(cleanup, "_claim_envelope", lambda _claim: different)
    before = copy.deepcopy(envelope)
    with pytest.raises(RuntimeError, match="claim_identity_conflict$"):
        await cleanup._reconcile_under_guards(fhir, envelope, None)
    assert envelope == before
    transaction.assert_not_called()


@pytest.mark.asyncio
async def test_changed_stage_manifest_refuses_before_capacity_reads_or_disposal(monkeypatch):
    envelope, _trust, _key = authorization_fixture()
    body = envelope["authorization"]
    stages = copy.deepcopy(body["stages"])
    stages[0]["name"] = "different_candidate"
    monkeypatch.setattr(cleanup, "_exclude_competitors", AsyncMock())
    monkeypatch.setattr(cleanup, "_stage_manifest", AsyncMock(return_value=stages))
    capacity = AsyncMock(side_effect=AssertionError("capacity read after changed stages"))
    monkeypatch.setattr(cleanup, "_cleanup_capacity_manifest", capacity)
    fhir = database_context()
    before = copy.deepcopy(body)
    with pytest.raises(RuntimeError, match="stage_manifest_changed$"):
        await cleanup._accept_capacity_fenced_stages(
            fhir, "catalog", body, {"cursor": "retained"}, {"run_id": body["checkpoint"]["owner_run_id"]}, object()
        )
    assert body == before
    capacity.assert_not_awaited()
    fhir.db.status.assert_not_awaited()
