# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failed cleanup refuses serving data and changed inputs before any stage disposal."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_failed_cleanup as cleanup
from tests.test_provider_directory_profile_failed_cleanup import (
    NOW,
    _legacy_publication_fixture,
    authorization_fixture,
    sign,
)


def database_context(**methods):
    return SimpleNamespace(
        db=SimpleNamespace(status=AsyncMock(), **methods),
        _unscoped_qt=lambda schema, name: schema + "." + name,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("none", None),
        ("untracked_legacy", None),
        ("selected_proof", "build_is_serving"),
        ("selected_generation", "build_is_serving"),
        ("row_build", "build_is_serving"),
        ("row_proof", "build_is_serving"),
        ("row_generation", "build_is_serving"),
        ("different_publication", "published_history_mismatch"),
        ("delta", "historical_delta_publication"),
        ("cms", "historical_cms_publication"),
    ],
)
async def test_failed_generation_cannot_dispose_current_or_retained_publication(change, reason):
    envelope, _, _ = authorization_fixture()
    coordinates = deepcopy(envelope["authorization"]["checkpoint"])
    publication = _legacy_publication_fixture()
    serving_by_field = {
        "generation_id": publication["published_generation_id"],
        "selection_proof_id": publication["published_proof_id"],
        "control_generation": publication["published_control_generation"],
    }
    match change:
        case "untracked_legacy":
            coordinates.update(selection_proof_id=None, control_generation=None)
        case "selected_proof":
            coordinates["selection_proof_id"] = publication["published_proof_id"]
        case "selected_generation":
            coordinates["control_generation"] = publication["published_control_generation"]
        case "row_build":
            serving_by_field["generation_id"] = coordinates["build_id"]
        case "row_proof":
            serving_by_field["selection_proof_id"] = coordinates["selection_proof_id"]
        case "row_generation":
            serving_by_field["control_generation"] = coordinates["control_generation"]
        case "different_publication":
            serving_by_field["generation_id"] = "another-published-generation"
    fhir = database_context(scalar=AsyncMock(side_effect=[change == "delta", change == "cms"]))
    original = deepcopy((coordinates, publication, serving_by_field))
    operation = cleanup._exclude_legacy_history(
        fhir, "catalog", coordinates, [SimpleNamespace(_mapping=serving_by_field)], publication
    )
    if reason is None:
        await operation
    else:
        with pytest.raises(RuntimeError, match=reason):
            await operation
    assert (coordinates, publication, serving_by_field) == original
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("metadata", "installed_publication_metadata_missing"),
        ("ambiguous", "publication_ambiguous"),
        ("target", "published_target_missing"),
        ("generation", "published_target_generation_missing"),
    ],
)
async def test_named_publication_layout_must_be_complete_before_failed_cleanup(monkeypatch, change, reason):
    envelope, _, _ = authorization_fixture()
    coordinates = envelope["authorization"]["checkpoint"]
    publication = _legacy_publication_fixture()
    rows = [SimpleNamespace(_mapping={})] * (2 if change == "ambiguous" else 1)
    answers_by_case = {
        "metadata": [False],
        "ambiguous": [True] * 3,
        "target": [True] * 3 + [False],
        "generation": [True] * 5 + [False],
    }
    fhir = database_context(all=AsyncMock(return_value=rows), scalar=AsyncMock(side_effect=answers_by_case[change]))
    fhir._provider_directory_profile_serving_generation_ref = lambda schema: schema + ".serving"
    monkeypatch.setattr(cleanup, "_published_history_manifest", AsyncMock(return_value=publication))
    original = deepcopy((coordinates, publication))
    with pytest.raises(RuntimeError, match=reason):
        await cleanup._publication_manifest(fhir, "catalog", {}, coordinates, "published-run", locked=True)
    assert (coordinates, publication) == original
    assert all(call.args[0].startswith("LOCK TABLE ") for call in fhir.db.status.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["none", "missing", "invalid_result", "not_latest", "same_owner"])
async def test_cleanup_publication_history_requires_latest_successful_other_run(change):
    envelope, _, _ = authorization_fixture()
    coordinates = envelope["authorization"]["checkpoint"]
    result_by_field = {
        "status": "published",
        "operation": "publish",
        "profile_generation_id": "published-generation",
        "proof_id": "published-proof",
        "generation": 7,
        "authority_revision": 2,
        "as_of": "retained-original-value",
    }
    if change == "invalid_result":
        result_by_field["status"] = "failed"
    published_by_field = {
        "importer": "provider-directory-fhir",
        "status": "succeeded",
        "finished_at": NOW,
        "metrics": json.dumps({"profile_selection_result": result_by_field}),
    }
    fhir = database_context(
        first=AsyncMock(return_value=None if change == "missing" else SimpleNamespace(_mapping=published_by_field)),
        scalar=AsyncMock(return_value="newer-run" if change == "not_latest" else "published-run"),
    )
    run = coordinates["owner_run_id"] if change == "same_owner" else "published-run"
    original = deepcopy(published_by_field)
    if change == "none":
        manifest = await cleanup._published_history_manifest(fhir, "catalog", coordinates, run, locked=True)
        assert manifest["published_generation_id"] == "published-generation"
        assert manifest["published_control_generation"] == 7
        assert "FOR UPDATE NOWAIT" in fhir.db.first.await_args.args[0]
    else:
        reason = {
            "missing": "published_history_missing",
            "invalid_result": "published_history_invalid",
            "not_latest": "published_history_not_latest",
            "same_owner": "published_history_required",
        }[change]
        with pytest.raises(RuntimeError, match=reason):
            await cleanup._published_history_manifest(fhir, "catalog", coordinates, run, locked=True)
    assert published_by_field == original
    fhir.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change,reason",
    [
        ("none", None),
        ("database", "database_changed"),
        ("runtime", "capacity_runtime_changed"),
        ("stages", "stage_manifest_changed"),
        ("reservation", "original_reservation_unaccounted"),
        ("error", "complete_error_exceeds_budget"),
    ],
)
async def test_locked_cleanup_rechecks_limits_and_stages_without_changing_checkpoint(monkeypatch, change, reason):
    envelope, _, _ = authorization_fixture()
    body = deepcopy(envelope["authorization"])
    checkpoint_by_field = {
        "last_error": "x" * cleanup.MAX_BYTES if change == "error" else "original failure",
        "cursor": "retained-page",
    }
    owner_by_field = {"run_id": body["checkpoint"]["owner_run_id"]}
    geometry = SimpleNamespace(postgres_block_size_bytes=8192)
    fhir = database_context(
        all=AsyncMock(return_value=[("unaccounted-reservation",)] if change == "reservation" else [])
    )
    fhir._geometry_postgres_values = lambda _observed: {
        "postgres_block_size_bytes": 4096 if change == "runtime" else 8192
    }
    monkeypatch.setattr(cleanup, "_checkpoint_and_owner", AsyncMock(return_value=(checkpoint_by_field, owner_by_field)))
    monkeypatch.setattr(cleanup, "_checkpoint_coordinates", lambda *_args: (body["checkpoint"], geometry))
    monkeypatch.setattr(
        cleanup,
        "_database_binding",
        AsyncMock(return_value={"schema": "other"} if change == "database" else body["database"]),
    )
    monkeypatch.setattr(cleanup, "_exclude_publication", AsyncMock(return_value=object()))
    monkeypatch.setattr(cleanup, "_capacity_identity", AsyncMock(return_value=object()))
    monkeypatch.setattr(cleanup, "_exclude_competitors", AsyncMock())
    monkeypatch.setattr(
        cleanup, "_stage_manifest", AsyncMock(return_value=[] if change == "stages" else body["stages"])
    )
    monkeypatch.setattr(cleanup, "_metadata_layouts", AsyncMock(return_value=object()))
    monkeypatch.setattr(
        cleanup,
        "project_cleanup_budget",
        AsyncMock(return_value={key: body["limits"][key] for key in ("data_bytes", "temp_bytes", "wal_bytes")}),
    )
    original = deepcopy((body, checkpoint_by_field, owner_by_field))
    if reason is None:
        assert await cleanup._accept_fenced_request(fhir, "catalog", body) is checkpoint_by_field
    else:
        with pytest.raises(RuntimeError, match=reason):
            await cleanup._accept_fenced_request(fhir, "catalog", body)
    assert (body, checkpoint_by_field, owner_by_field) == original
    fhir.db.status.assert_not_awaited()


def test_signed_cleanup_cannot_overcommit_shared_storage_classes():
    envelope, trust, key = authorization_fixture()
    body = deepcopy(envelope["authorization"])
    for entry in body["storage_classes"]:
        entry["available_bytes"] = body["limits"]["wal_bytes"]
        entry["available_after_all_reservations_bytes"] = entry["available_bytes"] - entry["reserved_bytes"]
    with pytest.raises(RuntimeError, match="reservation_accounting_invalid"):
        cleanup.validate_authorization(sign(body, key), trust=trust, now=NOW)
