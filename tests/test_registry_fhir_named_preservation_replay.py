# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retained typed FHIR replay uses its original approval and named office inputs."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_serving_preservation as preservation
from tests.test_registry_fhir_serving_preservation import receipt
from tests.test_registry_typed_source_recipe import _recipe, _registered_typed_resume


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, ValueError, asyncio.CancelledError])
async def test_retained_typed_replay_keeps_original_inputs_and_cleans_after_interruption(monkeypatch, failure):
    from process import network_source_contribution_custody as cleanup
    from process import registry_source_contribution_artifacts as artifacts
    from process import registry_typed_source_stage as staging

    _, _, candidate_by_field, address = _registered_typed_resume("published")
    candidate_by_field["validation_json"] = json.dumps(candidate_by_field["validation_json"])
    prior = SimpleNamespace(candidate_id=candidate_by_field["candidate_id"], approved_custom_revision=1)
    carried = SimpleNamespace(
        predecessor=prior,
        receipt={"carried_rows": 1},
        temporary_tables=tuple("pg_temp." + name for name in preservation._IdentityTables._fields),
        network_table="pg_temp.networks",
    )
    approved, stage, runtime = object(), object(), object()
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    monkeypatch.setattr(
        artifacts, "_published_artifact_candidates", AsyncMock(return_value={prior.candidate_id: candidate_by_field})
    )
    monkeypatch.setattr(artifacts, "load_published_artifact_validation", AsyncMock(return_value={"validation": None}))
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(_recipe(),)))
    pin = AsyncMock(return_value=approved)
    monkeypatch.setattr(preservation, "pin_retained_approved_membership_source", pin)
    stage_rows = AsyncMock(return_value=stage)
    monkeypatch.setattr(staging, "stage_typed_source_recipes", stage_rows)
    error = failure("retained replay interrupted") if failure else None
    monkeypatch.setattr(preservation, "_carried_typed_mapping_parity", AsyncMock(side_effect=error))
    discard = AsyncMock()
    monkeypatch.setattr(cleanup, "_cleanup_independent_source_stage", discard)
    if error is None:
        assert await preservation.has_fresh_carried_typed_data(
            connection, carried, control_schema="registry", source_witness_runtime=runtime
        )
    else:
        with pytest.raises(failure) as stopped:
            await preservation.has_fresh_carried_typed_data(
                connection, carried, control_schema="registry", source_witness_runtime=runtime
            )
        assert stopped.value is error
    pin.assert_awaited_once_with(connection, approved_revision=1, control_schema="registry")
    context = stage_rows.await_args.kwargs["read_context"]
    assert context.verified_office_inputs == (address, ()) and context.source_witness_runtime is runtime
    discard.assert_awaited_once_with(connection, stage, error)


@pytest.mark.asyncio
async def test_rederived_carry_selection_discards_changed_workspace_before_refusing(monkeypatch):
    _, target, candidate_by_field, _ = _registered_typed_resume("validated")
    stored = receipt()
    predecessor = object()
    monkeypatch.setattr(preservation, "fhir_preservation_obligation", AsyncMock(return_value=predecessor))
    monkeypatch.setattr(preservation, "verify_fhir_serving_preservation", lambda _candidate: stored)
    recipe = _recipe()
    monkeypatch.setattr(preservation, "_candidate_recipes", AsyncMock(return_value=((recipe,), None)))
    monkeypatch.setattr(preservation, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(recipe,)))
    changed = SimpleNamespace(receipt={**stored, "membership_sha256": "0" * 64})
    monkeypatch.setattr(preservation, "prepare_fhir_serving_preservation", AsyncMock(return_value=changed))
    discard = AsyncMock()
    monkeypatch.setattr(preservation, "cleanup_fhir_serving_preservation", discard)
    with pytest.raises(preservation.FHIRServingPreservationError, match="selection_changed") as stopped:
        await preservation.rederive_fhir_serving_preservation(
            "connection", target, candidate_by_field, control_schema="registry"
        )
    discard.assert_awaited_once_with("connection", changed, primary=stopped.value)
