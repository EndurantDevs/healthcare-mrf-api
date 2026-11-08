# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed coordinate projections cannot widen scope or fall back to live writes."""

import importlib
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_coordinate_backfill as coordinates
from process import provider_directory_output_location_rebuild as rebuild

fhir = importlib.import_module("process.provider_directory_fhir")


def test_selector_normalizes_values_without_sql_or_target_options():
    assert rebuild._repair_selector("coordinate_backfill", {"run_id": "run-a", "source_ids": ["b", "a", "b"]}) == {
        "run_id": "run-a",
        "source_ids": ["a", "b"],
    }
    for selector in (
        {},
        {"run_id": "a", "source_ids": [], "sql": "SELECT 1"},
        {"run_id": False, "source_ids": []},
        {"run_id": "a", "source_ids": [None]},
    ):
        with pytest.raises(RuntimeError, match="repair_selector_invalid"):
            rebuild._repair_selector("coordinate_backfill", selector)
    with pytest.raises(RuntimeError, match="repair_kind_invalid"):
        rebuild._repair_selector("arbitrary", {})


def test_projection_retains_complete_shape_and_exact_run_source_filters():
    projection, count, params = coordinates.coordinate_projection(fhir, {"run_id": "run-a", "source_ids": ["source-a"]})
    assert "UPDATE " not in projection and "INSERT " not in projection
    assert "last_seen_run_id = CAST(:run_id AS varchar)" in projection
    assert "source_id = ANY(CAST(:source_ids AS varchar[]))" in projection
    assert "FILTER(WHERE repair_changed)" in count
    assert params == {"run_id": "run-a", "source_ids": ["source-a"], "after_source_id": None, "after_resource_id": None}
    for name in ("source_id", "resource_id", "fhir_meta", "telephone_number", "observed_at"):
        assert f'c."{name}" AS "{name}"' in projection


@pytest.mark.asyncio
async def test_unprotected_seen_scope_refuses_before_rebuild(monkeypatch):
    monkeypatch.setattr(rebuild, "rebuild_location_snapshot", AsyncMock(side_effect=AssertionError("mutation reached")))
    with pytest.raises(RuntimeError, match="coordinate_seen_custody_required"):
        await coordinates.backfill_location_coordinates(fhir, run_id="run-a", seen_table="legacy_seen")


@pytest.mark.asyncio
async def test_standalone_coordinate_dispatch_preserves_selector_and_summary(monkeypatch):
    staged = AsyncMock(return_value=3)
    monkeypatch.setattr(coordinates, "backfill_location_coordinates", staged)
    monkeypatch.setattr(fhir, "_is_table_present", AsyncMock(side_effect=AssertionError("ordinary database reached")))
    token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({})
    try:
        assert (
            await fhir.backfill_provider_directory_location_coordinates(
                "mrf",
                run_id="run-a",
                source_ids=["source-a"],
                batch_size=10,
            )
            == 3
        )
    finally:
        fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)
    staged.assert_awaited_once_with(fhir, "mrf", run_id="run-a", source_ids=["source-a"], seen_table=None)


@pytest.mark.asyncio
async def test_coordinate_wrapper_selects_only_fixed_location_producer(monkeypatch):
    staged = AsyncMock(return_value=7)
    monkeypatch.setattr(rebuild, "rebuild_location_snapshot", staged)
    assert await coordinates.backfill_location_coordinates(fhir, "mrf", run_id="run-a", source_ids=["source-a"]) == 7
    staged.assert_awaited_once_with(
        fhir, kind="coordinate_backfill", run_id="run-a", selector={"run_id": "run-a", "source_ids": ["source-a"]}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 4])
async def test_issued_loading_result_avoids_standalone_cut(monkeypatch, count):
    from process import provider_directory_artifact_scratch_inputs as scratch

    expected = count
    issued = AsyncMock(return_value=expected)
    fallback = AsyncMock(side_effect=AssertionError("standalone cut reached"))
    monkeypatch.setattr(scratch, "prepared_scratch_enrichment", issued)
    monkeypatch.setattr(rebuild, "rebuild_location_snapshot", fallback)
    assert await coordinates.backfill_location_coordinates(fhir, "mrf", source_ids=["source"]) == expected
    issued.assert_awaited_once_with(
        fhir, "coordinate", db_schema="mrf", run_id=None, source_ids=["source"], seen_table=None
    )
    fallback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("issued", [False, True])
async def test_scoped_coordinate_dispatch_reuses_only_authenticated_prepared_metrics(monkeypatch, issued):
    from process import provider_directory_artifact_scratch_inputs as scratch

    prepared = AsyncMock(return_value=4)
    legacy = AsyncMock(return_value=7)
    monkeypatch.setattr(scratch, "active_scratch_inputs", lambda directory: object() if issued else None)
    monkeypatch.setattr(scratch, "prepared_scratch_enrichment", prepared)
    monkeypatch.setattr(fhir, "_backfill_staged_location_coordinates", legacy)
    token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({coordinates.TARGET: "staged_location"})
    try:
        assert await fhir.backfill_provider_directory_location_coordinates(
            "mrf", source_ids=["source-a"], batch_size=10
        ) == (4 if issued else 7)
    finally:
        fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)
    if issued:
        prepared.assert_awaited_once_with(
            fhir, "coordinate", db_schema="mrf", run_id=None, source_ids=["source-a"], seen_table=None
        )
        legacy.assert_not_awaited()
    else:
        prepared.assert_not_awaited()
        legacy.assert_awaited_once_with("mrf", run_id=None, source_ids=["source-a"], seen_table=None, batch_size=10)
