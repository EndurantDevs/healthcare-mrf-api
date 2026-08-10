# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from datetime import date
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_selection_snapshot as snapshot
from process import provider_directory_profile_uhc_flex as flex_profile
from process import uhc_flex_practitioner_publication as publication
from process.uhc_flex_practitioner_contract import (
    UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY,
    UHC_FLEX_PRACTITIONER_SOURCE_ID,
)
from tests.test_provider_directory_profile_uhc_flex import (
    _artifact_dataset_row,
    _catalog,
    _dataset_rows,
    _flex_metadata,
    _readiness_record,
    OFFICIAL_SOURCE_ID,
)


def test_json_and_metadata_helpers_fail_closed() -> None:
    metadata = _flex_metadata()
    assert flex_profile._json_object(json.dumps(metadata)) == metadata
    assert flex_profile._json_object("{") == {}
    assert flex_profile._json_object([]) == {}
    assert flex_profile._clean_text(" value ") == "value"
    assert flex_profile._clean_text(" ") is None
    assert flex_profile._clean_text(1) is None

    assert flex_profile.is_uhc_flex_publication_metadata_valid(
        metadata,
        dataset_id=metadata["dataset_id"],
        endpoint_id=metadata["endpoint_id"],
        evidence_run_id=metadata["acquisition_root_run_id"],
    )
    drifted_metadata_by_field = dict(metadata)
    drifted_metadata_by_field["operation_key"] = "not-a-hash"
    assert not flex_profile.is_uhc_flex_publication_metadata_valid(
        drifted_metadata_by_field,
        dataset_id=metadata["dataset_id"],
        endpoint_id=metadata["endpoint_id"],
        evidence_run_id=metadata["acquisition_root_run_id"],
    )


@pytest.mark.asyncio
async def test_publication_lock_uses_the_shared_advisory_identity() -> None:
    database = SimpleNamespace(status=AsyncMock())
    await flex_profile.lock_uhc_flex_profile_publication(database)
    database.status.assert_awaited_once()
    assert database.status.await_args.kwargs["lock_identity"] == (
        UHC_FLEX_PRACTITIONER_PUBLICATION_LOCK_IDENTITY
    )


def test_dataset_row_readiness_accepts_date_projection_and_json_text() -> None:
    dataset_row = _dataset_rows()[0]
    dataset_row["dataset_scoped_projection_as_of"] = date(2026, 8, 9)
    dataset_row["publication_metadata_json"] = json.dumps(
        dataset_row["publication_metadata_json"]
    )
    assert flex_profile.is_uhc_flex_dataset_row_ready(dataset_row)

    dataset_row["dataset_scoped_ready"] = False
    assert not flex_profile.is_uhc_flex_dataset_row_ready(dataset_row)


@pytest.mark.asyncio
async def test_selection_dataset_loader_maps_rows_and_keeps_ready_join() -> None:
    first_row = SimpleNamespace(dataset_id="dataset-one")
    second_row = SimpleNamespace(dataset_id="dataset-two")
    database = SimpleNamespace(
        all=AsyncMock(return_value=[first_row, second_row])
    )
    loaded_rows = await flex_profile.load_profile_selection_dataset_rows(
        database=database,
        endpoint_dataset_ref='"fixture"."endpoint_dataset"',
        schema_ref='"fixture"',
        row_mapping=lambda row: {"dataset_id": row.dataset_id},
    )
    assert loaded_rows == [
        {"dataset_id": "dataset-one"},
        {"dataset_id": "dataset-two"},
    ]
    selection_sql = database.all.await_args.args[0]
    assert "LEFT JOIN" in selection_sql
    assert "dataset_scoped_ready" in selection_sql
    assert "endpoint_complete" not in selection_sql


def test_readiness_matching_uses_import_run_fallback_and_rejects_none() -> None:
    dataset_row = _artifact_dataset_row()
    dataset_row["import_run_id"] = dataset_row["acquisition_root_run_id"]
    dataset_row["acquisition_root_run_id"] = " "
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(
        _readiness_record(),
        dataset_row,
    )
    assert not flex_profile.is_uhc_flex_dataset_readiness_matching(
        None,
        dataset_row,
    )


@pytest.mark.asyncio
async def test_readiness_annotation_only_loads_exact_flex_dataset(
    monkeypatch,
) -> None:
    ordinary_row_by_field = {
        "source_id": OFFICIAL_SOURCE_ID,
        "dataset_id": "ordinary-dataset",
    }
    missing_dataset_row_by_field = {
        "source_id": UHC_FLEX_PRACTITIONER_SOURCE_ID,
        "dataset_id": " ",
    }
    ready_dataset_row = _artifact_dataset_row()
    readiness_loader = AsyncMock(return_value=_readiness_record())
    monkeypatch.setattr(
        publication,
        "load_uhc_flex_practitioner_dataset_readiness",
        readiness_loader,
    )

    annotated_rows = await flex_profile.annotate_uhc_flex_profile_dataset_readiness(
        [ordinary_row_by_field, missing_dataset_row_by_field, ready_dataset_row],
        database="database",
        row_mapping=lambda row: row,
    )
    assert annotated_rows[0] is ordinary_row_by_field
    assert annotated_rows[1]["dataset_scoped_ready"] is False
    assert annotated_rows[1]["dataset_scoped_admission_id"] is None
    assert annotated_rows[2]["dataset_scoped_ready"] is True
    assert annotated_rows[2]["dataset_scoped_projection_as_of"] == (
        "2026-08-09"
    )
    readiness_loader.assert_awaited_once_with(
        ready_dataset_row["dataset_id"],
        database="database",
    )


def test_fence_readiness_rejects_closed_completion_drift() -> None:
    readiness = _readiness_record()
    dataset = SimpleNamespace(
        dataset_scoped_ready=True,
        dataset_id=readiness.dataset_id,
        endpoint_id=readiness.endpoint_id,
        source_id=readiness.source_id,
        dataset_hash=readiness.dataset_hash,
        resource_count=readiness.resource_count,
        semantic_projection_as_of=readiness.semantic_projection_as_of,
        source_authority_id=readiness.source_authority_id,
        admission_id=readiness.admission_id,
        operation_key=readiness.operation_key,
    )
    assert flex_profile.is_uhc_flex_fence_dataset_ready(dataset, readiness)
    readiness.endpoint_complete = True
    assert not flex_profile.is_uhc_flex_fence_dataset_ready(dataset, readiness)


def test_catalog_rejects_overlap_between_ordinary_and_dataset_sources(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        snapshot.profile_artifact,
        "configured_dataset_scoped_profile_source_ids",
        lambda: (OFFICIAL_SOURCE_ID,),
    )
    with pytest.raises(
        RuntimeError,
        match="profile_selection_catalog_invalid",
    ):
        snapshot._catalog_source_groups(_catalog())
