# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reference handoffs retain named provenance and require complete contribution scope."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import facility_address_contribution_effects as effects
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_source_copy import _nucc_completed_handoff, _nucc_stage_receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("tracked", [False, True])
async def test_nucc_attempt_binds_the_selected_named_predecessor_before_stage_creation(monkeypatch, tracked):
    stage = _nucc_stage_receipt()
    context_by_field = {
        "control_run_id": stage["run_id"],
        "context": {
            "_control_attempt_id": stage["attempt_id"],
            "_control_attempt_started_at": stage["attempt_started_at"],
        },
    }
    predecessor = generation.ReferenceFamilyResultGenerationAuthority(
        "nucc",
        "11111111-1111-4111-8111-111111111111",
        1,
        generation.validate_reference_family_serving_generation(
            {
                "origin_lineage_id": "22222222-2222-4222-8222-222222222222",
                "origin_generation": 1,
                "published_at": "2026-01-01T00:00:00Z",
            }
        )
        if tracked
        else None,
        (("mrf", "nucc_taxonomy"),),
    )
    monkeypatch.setattr(archive, "is_nucc_native_handoff_required", AsyncMock(return_value=True))
    monkeypatch.setattr(
        archive, "read_reference_family_result_generation_authority", AsyncMock(return_value=predecessor)
    )
    monkeypatch.setattr(generation, "require_nucc_native_predecessor_storage", AsyncMock(return_value=None))
    create_stage = AsyncMock(return_value=stage)
    monkeypatch.setattr(archive, "_precreate_nucc_attempt", create_stage)
    if tracked:
        assert await archive.bind_nucc_native_attempt(object(), context_by_field, schema_name="mrf") == stage
        assert context_by_field["context"]["nucc_native_predecessor"] == predecessor.as_dict()
        assert context_by_field["context"]["nucc_native_stage"] == stage
        assert create_stage.await_args.kwargs == {"incumbent": predecessor.as_dict()}
    else:
        original = copy.deepcopy(context_by_field)
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="untracked"):
            await archive.bind_nucc_native_attempt(object(), context_by_field, schema_name="mrf")
        assert context_by_field == original
        create_stage.assert_not_awaited()


@pytest.mark.parametrize("failure", ["foreign_predecessor", "bad_source_digest", "changed_digest", "bounds"])
def test_named_nucc_handoff_refuses_changed_provenance_or_unbounded_metadata(failure):
    handoff = _nucc_completed_handoff(_nucc_stage_receipt())
    if failure == "foreign_predecessor":
        handoff["incumbent"]["local_generation"] = 1
        handoff["incumbent"]["serving_generation"] = {
            "origin_lineage_id": "22222222-2222-4222-8222-222222222222",
            "origin_generation": 1,
            "published_at": "2026-01-01T00:00:00Z",
        }
        handoff["incumbent"]["relations"] = [{"schema_name": "foreign_schema", "relation_name": "nucc_taxonomy"}]
    elif failure == "bad_source_digest":
        handoff["source_contract_sha256"] = "wrong"
    elif failure == "changed_digest":
        handoff["handoff_sha256"] = "b" * 64
    else:
        handoff["stage"]["indexes"][0]["definition"] += " " * 131073
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        archive.validate_nucc_native_handoff(handoff)


@pytest.mark.parametrize("failure", ["duplicate", "unsafe", "mrf_stage"])
def test_model_family_names_preserve_the_mrf_stage_and_refuse_ambiguous_names(failure):
    names = (
        ("payload", "payload") if failure == "duplicate" else ("payload;drop",) if failure == "unsafe" else ("payload",)
    )
    ownership = SimpleNamespace(importer_id="mrf", relation_names=names)
    if failure == "mrf_stage":
        assert archive._model_family_names(ownership) == ("payload", archive.STAGE_TABLE)
    else:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="declaration differs"):
            archive._model_family_names(ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", [None, "false"])
async def test_freeze_phase_requires_a_boolean_before_any_catalog_or_permission_work(phase):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="freeze phase"):
        await archive.seal_model_family_storage(session, object(), freeze_schema=phase)
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [("unknown", ("one",)), ("npi", ["one"])])
async def test_set_comparison_refuses_foreign_columns_or_unbounded_scope_types(scope):
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock())
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="set comparison scope"):
        await archive._is_model_table_equal(
            session,
            archive.models.ProviderProfileSourceRecord,
            left_schema="left_rows",
            left_name="payload",
            right_schema="right_rows",
            right_name="payload",
            scope=scope,
        )
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "family", "stage", "destination", "relation"])
async def test_facility_contribution_is_applied_once_only_to_its_declared_family(monkeypatch, failure):
    receipt_by_field = {
        "schema": "serving_rows",
        "stage_schema": "candidate_rows",
        "contribution_relation_name": "facility_address_contribution",
    }
    ownership = SimpleNamespace(
        importer_id="other" if failure == "family" else "facility-anchors",
        schema_name="candidate_rows",
        relation_names=("facility_address_contribution",),
    )
    destination = SimpleNamespace(schema_name="serving_rows")
    if failure in {"stage", "destination", "relation"}:
        receipt_by_field[
            {"stage": "stage_schema", "destination": "schema", "relation": "contribution_relation_name"}[failure]
        ] = "other_rows"
    apply_effects = AsyncMock()
    monkeypatch.setattr(effects, "validate_effect_receipt", lambda value: value)
    monkeypatch.setattr(effects, "apply_facility_address_effects", apply_effects)
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive._apply_validated_contribution(
                object(), ownership, destination, None if failure == "missing" else receipt_by_field
            )
        apply_effects.assert_not_awaited()
    else:
        session = object()
        await archive._apply_validated_contribution(session, ownership, destination, receipt_by_field)
        apply_effects.assert_awaited_once_with(session, receipt_by_field)
