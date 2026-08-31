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
from process import provider_directory_rooted_graph_publication as rooted_publication
from process.provider_directory_dataset_scoped_publication import (
    EXACT_DATASET_PUBLICATION_LOCK_IDENTITY,
    LEGACY_PRACTITIONER_VARIANT,
    ROOTED_COMBINED_VARIANT,
)
from process.provider_directory_fhir_root_policy import ReviewedRootPolicy
from process.uhc_flex_practitioner_contract import UHC_FLEX_PRACTITIONER_SOURCE_ID
from process.uhc_flex_practitioner_single_root_contract import (
    UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
)
from tests.test_provider_directory_profile_uhc_flex import (
    _artifact_dataset_row,
    _catalog,
    _dataset_rows,
    _flex_metadata,
    _readiness_record,
    _rooted_dataset_rows,
    _rooted_metadata,
    GRAPH_DATASET_ID,
    GRAPH_ENDPOINT_ID,
    OFFICIAL_SOURCE_ID,
)
from tests.provider_directory_profile_uhc_flex_test_support import (
    _rooted_readiness_record,
)
from process.provider_directory_rooted_graph_source_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
)
from process.provider_directory_rooted_graph_twin_contract import (
    PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID,
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


def test_flex_metadata_requires_disjoint_admission_evidence() -> None:
    twin_metadata_by_field = _flex_metadata()
    single_metadata_by_field = dict(twin_metadata_by_field)
    single_metadata_by_field["admission_contract_id"] = (
        UHC_FLEX_PRACTITIONER_SINGLE_ROOT_ADMISSION_CONTRACT_ID
    )
    single_metadata_by_field.pop("baseline_acquisition_id")
    single_metadata_by_field.pop("baseline_run_id")
    single_metadata_by_field["provider_directory_reviewed_root_policy_v1"] = (
        ReviewedRootPolicy(1).document()
    )
    for metadata_by_field in (twin_metadata_by_field, single_metadata_by_field):
        assert flex_profile.is_uhc_flex_publication_metadata_valid(
            metadata_by_field,
            dataset_id=metadata_by_field["dataset_id"],
            endpoint_id=metadata_by_field["endpoint_id"],
            evidence_run_id=metadata_by_field["acquisition_root_run_id"],
        )

    mutated_metadata_records = (
        {**twin_metadata_by_field, "provider_directory_reviewed_root_policy_v1": None},
        {
            key: field_value
            for key, field_value in twin_metadata_by_field.items()
            if key != "baseline_run_id"
        },
        {**single_metadata_by_field, "baseline_acquisition_id": "pdufpa_" + "0" * 48},
        {
            **single_metadata_by_field,
            "provider_directory_reviewed_root_policy_v1": ReviewedRootPolicy(2).document(),
        },
        {**twin_metadata_by_field, "admission_contract_id": "unknown"},
    )
    for metadata_record in mutated_metadata_records:
        assert not flex_profile.is_uhc_flex_publication_metadata_valid(
            metadata_record,
            dataset_id=metadata_record["dataset_id"],
            endpoint_id=metadata_record["endpoint_id"],
            evidence_run_id=metadata_record["acquisition_root_run_id"],
        )


@pytest.mark.asyncio
async def test_publication_lock_uses_the_shared_advisory_identity() -> None:
    database = SimpleNamespace(status=AsyncMock())
    await flex_profile.lock_uhc_flex_profile_publication(database)
    database.status.assert_awaited_once()
    assert database.status.await_args.kwargs["lock_identity"] == (
        EXACT_DATASET_PUBLICATION_LOCK_IDENTITY
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


def test_rooted_metadata_accepts_legacy_distinct_or_rooted_equal_lineage() -> None:
    metadata = _rooted_metadata()
    assert flex_profile.is_uhc_flex_publication_metadata_valid(
        metadata,
        dataset_id=GRAPH_DATASET_ID,
        endpoint_id=GRAPH_ENDPOINT_ID,
        evidence_run_id=metadata["acquisition_root_run_id"],
    )

    generation_two_by_field = dict(metadata)
    generation_two_by_field.update(
        {
            "root_variant": "rooted_combined",
            "root_source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
            "root_endpoint_id": GRAPH_ENDPOINT_ID,
        }
    )
    assert flex_profile.is_uhc_flex_publication_metadata_valid(
        generation_two_by_field,
        dataset_id=GRAPH_DATASET_ID,
        endpoint_id=GRAPH_ENDPOINT_ID,
        evidence_run_id=metadata["acquisition_root_run_id"],
    )
    generation_two_by_field["root_source_id"] = UHC_FLEX_PRACTITIONER_SOURCE_ID
    assert not flex_profile.is_uhc_flex_publication_metadata_valid(
        generation_two_by_field,
        dataset_id=GRAPH_DATASET_ID,
        endpoint_id=GRAPH_ENDPOINT_ID,
        evidence_run_id=metadata["acquisition_root_run_id"],
    )


def test_rooted_metadata_requires_disjoint_admission_evidence() -> None:
    twin_metadata_by_field = _rooted_metadata()
    for field_name in (
        "provider_directory_reviewed_root_policy_v1",
        "acquisition_operation_key",
    ):
        assert not flex_profile.is_uhc_flex_publication_metadata_valid(
            {**twin_metadata_by_field, field_name: None},
            dataset_id=GRAPH_DATASET_ID,
            endpoint_id=GRAPH_ENDPOINT_ID,
            evidence_run_id=twin_metadata_by_field["acquisition_root_run_id"],
        )

    single_metadata_by_field = {
        **twin_metadata_by_field,
        "admission_contract_id": (
            PROVIDER_DIRECTORY_ROOTED_GRAPH_SINGLE_ROOT_ADMISSION_CONTRACT_ID
        ),
        "attempt_id": None,
        "comparison_acquisition_id": None,
        "provider_directory_reviewed_root_policy_v1": (
            ReviewedRootPolicy(1).document()
        ),
        "acquisition_operation_key": "a" * 64,
    }
    for field_name in ("attempt_id", "comparison_acquisition_id"):
        incomplete_metadata_by_field = dict(single_metadata_by_field)
        incomplete_metadata_by_field.pop(field_name)
        assert not flex_profile.is_uhc_flex_publication_metadata_valid(
            incomplete_metadata_by_field,
            dataset_id=GRAPH_DATASET_ID,
            endpoint_id=GRAPH_ENDPOINT_ID,
            evidence_run_id=single_metadata_by_field["acquisition_root_run_id"],
        )


def test_rooted_metadata_roundtrip_uses_exact_unordered_family_set() -> None:
    metadata = _rooted_metadata()
    stored_metadata = json.loads(json.dumps(metadata, sort_keys=True))
    assert tuple(stored_metadata["resource_counts"]) != tuple(
        metadata["resource_counts"]
    )
    assert flex_profile.is_uhc_flex_publication_metadata_valid(
        stored_metadata,
        dataset_id=GRAPH_DATASET_ID,
        endpoint_id=GRAPH_ENDPOINT_ID,
        evidence_run_id=metadata["acquisition_root_run_id"],
    )

    for mutate_counts in (
        lambda counts: counts.pop("Location"),
        lambda counts: counts.__setitem__("Unexpected", 0),
        lambda counts: counts.__setitem__("Endpoint", True),
    ):
        drifted_by_field = json.loads(json.dumps(stored_metadata))
        mutate_counts(drifted_by_field["resource_counts"])
        assert not flex_profile.is_uhc_flex_publication_metadata_valid(
            drifted_by_field,
            dataset_id=GRAPH_DATASET_ID,
            endpoint_id=GRAPH_ENDPOINT_ID,
            evidence_run_id=metadata["acquisition_root_run_id"],
        )

    readiness = _rooted_readiness_record(
        resource_counts=stored_metadata["resource_counts"]
    )
    dataset_row_by_field = {
        **_rooted_dataset_rows()[0],
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
        "publication_metadata_json": stored_metadata,
    }
    assert flex_profile.is_uhc_flex_dataset_readiness_matching(
        readiness,
        dataset_row_by_field,
    )
    fence_dataset = SimpleNamespace(
        dataset_scoped_ready=True,
        dataset_scoped_variant=ROOTED_COMBINED_VARIANT,
        dataset_scoped_cohort_complete=readiness.cohort_complete,
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
    assert flex_profile.is_uhc_flex_fence_dataset_ready(
        fence_dataset,
        readiness,
    )


def test_rooted_metadata_requires_exact_practitioner_lineage_coordinates() -> None:
    metadata = _rooted_metadata()
    for field_name, drifted_value in (
        ("root_source_id", PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID),
        ("root_endpoint_id", GRAPH_ENDPOINT_ID),
        ("practitioner_origin_source_id", "pdfhir_" + "0" * 24),
        ("practitioner_origin_endpoint_id", "0" * 64),
    ):
        drifted_by_field = dict(metadata)
        drifted_by_field[field_name] = drifted_value
        assert not flex_profile.is_uhc_flex_publication_metadata_valid(
            drifted_by_field,
            dataset_id=GRAPH_DATASET_ID,
            endpoint_id=GRAPH_ENDPOINT_ID,
            evidence_run_id=metadata["acquisition_root_run_id"],
        )


def test_rooted_dataset_row_requires_physical_completion_gates() -> None:
    dataset_row_by_field = _rooted_dataset_rows()[0]
    assert flex_profile.is_uhc_flex_dataset_row_ready(dataset_row_by_field)
    for field_name, drifted_value in (
        ("dataset_scoped_publication_kind", "other"),
        ("dataset_scoped_cohort_complete", False),
        ("dataset_scoped_rooted_graph_complete", False),
        ("dataset_scoped_endpoint_collection_complete", True),
        ("dataset_scoped_endpoint_complete", True),
    ):
        drifted_by_field = dict(dataset_row_by_field)
        drifted_by_field[field_name] = drifted_value
        assert not flex_profile.is_uhc_flex_dataset_row_ready(drifted_by_field)


@pytest.mark.asyncio
async def test_selection_dataset_loader_maps_rows_and_keeps_ready_join() -> None:
    first_row = SimpleNamespace(dataset_id="dataset-one")
    second_row = SimpleNamespace(dataset_id="dataset-two")
    database = SimpleNamespace(all=AsyncMock(return_value=[first_row, second_row]))
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
    assert "provider_directory_rooted_graph_dataset" in selection_sql
    assert "provider_directory_rooted_graph_dataset_ready" in selection_sql
    assert "dataset_scoped_rooted_graph_complete" in selection_sql
    assert "dataset_scoped_endpoint_complete" in selection_sql


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
        "dataset_id": "ordinary-dataset",
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
    assert annotated_rows[2]["dataset_scoped_projection_as_of"] == ("2026-08-09")
    readiness_loader.assert_awaited_once_with(
        ready_dataset_row["dataset_id"],
        database="database",
    )


@pytest.mark.asyncio
async def test_readiness_annotation_loads_only_exact_rooted_header(
    monkeypatch,
) -> None:
    rooted_row_by_field = {
        **_rooted_dataset_rows()[0],
        "source_id": PROVIDER_DIRECTORY_ROOTED_GRAPH_SOURCE_ID,
    }
    readiness_loader = AsyncMock(return_value=_rooted_readiness_record())
    monkeypatch.setattr(
        rooted_publication,
        "load_provider_directory_rooted_graph_dataset_readiness",
        readiness_loader,
    )

    annotated = await flex_profile.annotate_uhc_flex_profile_dataset_readiness(
        [rooted_row_by_field],
        database="database",
        row_mapping=lambda row: row,
    )
    assert annotated[0]["dataset_scoped_ready"] is True
    assert annotated[0]["dataset_scoped_variant"] == "rooted_combined"
    readiness_loader.assert_awaited_once_with(
        GRAPH_DATASET_ID,
        database="database",
    )


def test_fence_readiness_rejects_closed_completion_drift() -> None:
    readiness = _readiness_record()
    dataset = SimpleNamespace(
        dataset_scoped_ready=True,
        dataset_scoped_variant=LEGACY_PRACTITIONER_VARIANT,
        dataset_scoped_cohort_complete=readiness.cohort_complete,
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


@pytest.mark.parametrize(
    "dataset_cohort_complete, readiness_cohort_complete, retry_exhausted_count",
    ((True, False, 1), (False, True, 0)),
)
def test_fence_readiness_rejects_cohort_completion_drift(
    dataset_cohort_complete: bool,
    readiness_cohort_complete: bool,
    retry_exhausted_count: int,
) -> None:
    readiness = _readiness_record(
        cohort_complete=readiness_cohort_complete,
        retry_exhausted_count=retry_exhausted_count,
    )
    dataset = SimpleNamespace(
        dataset_scoped_ready=True,
        dataset_scoped_variant=LEGACY_PRACTITIONER_VARIANT,
        dataset_scoped_cohort_complete=dataset_cohort_complete,
        dataset_id=readiness.dataset_id,
        endpoint_id=readiness.endpoint_id,
        source_id=readiness.source_id,
        dataset_hash=readiness.dataset_hash,
        resource_count=readiness.resource_count,
        semantic_projection_as_of=readiness.semantic_projection_as_of,
        source_authority_id=readiness.source_authority_id,
        admission_id=readiness.admission_id,
        operation_key=readiness.operation_key,
        reviewed_root_policy=ReviewedRootPolicy(1),
    )
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
