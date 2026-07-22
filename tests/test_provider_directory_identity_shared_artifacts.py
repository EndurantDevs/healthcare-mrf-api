# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


fhir = importlib.import_module("process.provider_directory_fhir")


def _checkpoint_context(*, dataset_id: str | None = "dataset-a"):
    return fhir.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope",
        source_ids=("source-a",),
        owner_run_id="run",
        acquisition_root_run_id="root",
        dataset_id=dataset_id,
        lineage_verified=True,
    )


def _partition_options(**overrides):
    fields_by_name = {
        "timeout": 1,
        "run_id": "run",
        "row_batch_handler": None,
        "row_batch_size": 10,
        "retain_rows": True,
        "deadline_at": None,
        "max_pages": 0,
    }
    fields_by_name.update(overrides)
    return fhir.PartitionFetchOptions(**fields_by_name)


def _partition_page_request(**overrides):
    state = overrides.pop(
        "state",
        fhir.PartitionFetchState(
            result_model=fhir.ProviderDirectoryOrganization,
            retained_resource_rows=[],
        ),
    )
    fields_by_name = {
        "source_record": {
            "source_id": "source-a",
            "api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
        },
        "resource_type": "Organization",
        "start_url_queue": asyncio.Queue(),
        "state": state,
        "partition_start_url": f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Organization",
        "current_url": f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Organization",
        "is_residual_only": False,
        "pending_resource_rows": [],
        "fetch_options": _partition_options(),
    }
    fields_by_name.update(overrides)
    return fhir.PartitionPageRequest(**fields_by_name)


def _alohr_options(**overrides):
    fields_by_name = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "timeout": 1,
        "run_id": "run",
        "stale_cleanup": True,
        "seen_table": None,
        "checkpoint_context": None,
        "resume_required_entries": None,
    }
    fields_by_name.update(overrides)
    return fhir.AlohrGraphQLImportOptions(**fields_by_name)


def _alohr_import_state(**overrides):
    fields_by_name = {
        "source_ids": ["source-a"],
        "compatibility_source_id": "source-a",
        "selected_resource_types": {"Organization"},
        "canonical_api_base": fhir.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "options": _alohr_options(),
        "source_counts_by_resource": {"Organization": 0},
    }
    fields_by_name.update(overrides)
    return fhir.AlohrGraphQLImportState(**fields_by_name)


def test_last_updated_page_validation_rejects_type_and_window_drift():
    window = fhir.TimeWindow(
        "root",
        datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC),
        datetime.datetime(2026, 7, 23, tzinfo=datetime.UTC),
    )
    resources, error = fhir._validated_last_updated_page_resources(
        {
            "resourceType": "Bundle",
            "entry": [
                {"resource": {"resourceType": "Practitioner", "id": "p1"}}
            ]
        },
        "Organization",
        window,
    )
    assert resources == () and error == "unexpected_resource_type"
    resources, error = fhir._validated_last_updated_page_resources(
        {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Organization",
                        "id": "o1",
                        "meta": {"lastUpdated": "2026-07-24T00:00:00Z"},
                    }
                }
            ]
        },
        "Organization",
        window,
    )
    assert resources == () and error == "resource_last_updated_outside_window"


@pytest.mark.asyncio
async def test_last_updated_fetch_pipeline_returns_each_failed_phase(monkeypatch):
    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    config = fhir.LastUpdatedPartitionConfig(
        start=now,
        end=now + datetime.timedelta(days=1),
        end_mode="fixed",
        ceiling=100,
        minimum_width=datetime.timedelta(seconds=1),
        boundary_precision=datetime.timedelta(seconds=1),
        page_count=10,
        volatile_metadata_paths=(),
    )
    options = fhir.LastUpdatedPartitionFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        timeout=1,
        run_id="run",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=10,
        retain_rows=False,
        cancel_ctx=None,
        cancel_task=None,
        deadline_seconds=0,
        pagination_checkpoint=_checkpoint_context(),
    )
    failure = fhir._last_updated_partition_failure_result(
        fhir.ProviderDirectoryOrganization,
        None,
        "injected",
    )
    monkeypatch.setattr(fhir, "_partition_preflight_failure", lambda *_args: failure)
    assert await fhir._fetch_last_updated_partition_resource_rows(
        {}, "Organization", fhir.ProviderDirectoryOrganization, config, options
    ) is failure

    monkeypatch.setattr(fhir, "_partition_preflight_failure", lambda *_args: None)
    monkeypatch.setattr(fhir, "_prepare_partition_state", AsyncMock(return_value=failure))
    assert await fhir._fetch_last_updated_partition_resource_rows(
        {}, "Organization", fhir.ProviderDirectoryOrganization, config, options
    ) is failure

    state = SimpleNamespace()
    monkeypatch.setattr(fhir, "_prepare_partition_state", AsyncMock(return_value=state))
    monkeypatch.setattr(fhir, "_observe_partition_pre_census", AsyncMock(return_value=None))
    monkeypatch.setattr(fhir, "_observe_partition_counts", AsyncMock(return_value=failure))
    assert await fhir._fetch_last_updated_partition_resource_rows(
        {}, "Organization", fhir.ProviderDirectoryOrganization, config, options
    ) is failure


def test_artifact_dependency_and_metric_contracts_fail_closed():
    dataset = fhir.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run",
        selected_resources=("Location",),
    )
    fence = fhir.ProviderDirectoryArtifactDatasetFence((dataset,))
    violations = fhir._collect_artifact_target_dependency_violations(
        fence,
        ("address_overlay",),
    )
    assert len(violations) == 1
    summary = fhir._artifact_scope_projection_summary(
        fence,
        frozenset({"Organization"}),
        {"dataset-a": 1},
        [{"dataset_id": None, "resource_type": "Organization", "resource_rows": 4}],
    )
    assert summary["dataset_rows"] == 0

    with pytest.raises(RuntimeError, match="metric_missing"):
        fhir._assert_candidate_artifact_metrics_complete({"network_catalog"}, {})
    fhir._assert_candidate_artifact_metrics_complete(
        {"profile"},
        {"profile": {"skipped": True, "reason": "no_profile_enabled_sources_in_scope"}},
    )
    with pytest.raises(RuntimeError, match="corroboration"):
        fhir._assert_candidate_artifact_metrics_complete({"corroboration"}, {})

    bundle = fhir.ProviderDirectoryArtifactBundle()
    with pytest.raises(RuntimeError, match="bundle_incomplete"):
        fhir._assert_candidate_artifact_stage_relations_complete(
            {"network_catalog"},
            {"network_catalog": {"published": True}},
            bundle,
        )


@pytest.mark.asyncio
async def test_artifact_empty_scope_and_eligible_selection_boundaries():
    assert await fhir._artifact_scope_resource_rows([]) == []
    empty_fence = fhir.ProviderDirectoryArtifactDatasetFence(())
    executor = SimpleNamespace(all=AsyncMock())
    assert await fhir._artifact_eligible_validated_ids(empty_fence, executor) == {}


def test_endpoint_and_pagination_identity_helpers_fail_closed():
    with pytest.raises(ValueError, match="multiple endpoint"):
        fhir._endpoint_id_for_source_records(
            [{"endpoint_id": "one"}, {"endpoint_id": "two"}]
        )
    context = _checkpoint_context()
    checkpoint_by_field = {
        "source_ids": list(context.source_ids),
        "owner_run_id": context.owner_run_id,
        "acquisition_root_run_id": context.acquisition_root_run_id,
        "dataset_id": context.dataset_id,
        "start_url_hash": "start",
        "state": fhir.PAGINATION_CHECKPOINT_ACTIVE,
        "next_url": None,
    }
    assert fhir._compatible_pagination_resume_state(
        checkpoint_by_field,
        context,
        "start",
    ) is None
    with pytest.raises(ValueError, match="compatibility source"):
        fhir._compatibility_storage_source_id([])


@pytest.mark.asyncio
async def test_checkpoint_clear_and_alohr_resume_boundaries(monkeypatch):
    assert await fhir._checkpoint_candidate_dataset_id(
        None,
        "endpoint-a",
        ("Organization",),
    ) is None
    await fhir._clear_checkpoint_dataset_resource_type(
        _checkpoint_context(dataset_id=None),
        "Organization",
    )
    assert await fhir._clear_pagination_checkpoints(_checkpoint_context(), []) == 0
    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value="DELETE 2"))
    assert await fhir._clear_pagination_checkpoints(
        _checkpoint_context(),
        ["Organization"],
    ) == 2

    resume_required_entries: set[str] = set()
    state = _alohr_import_state(
        options=_alohr_options(
            checkpoint_context=_checkpoint_context(),
            resume_required_entries=resume_required_entries,
        ),
        checkpoint_diagnostics_by_resource={
            "alohr:Organization": {"complete": False, "error": "offline"}
        },
    )
    await fhir._finalize_alohr_checkpoints(state)
    assert resume_required_entries == {"source-a:alohr:Organization"}


@pytest.mark.asyncio
async def test_alohr_limited_items_and_transport_failure(monkeypatch):
    stream_state = fhir.AlohrGraphQLStreamState("locator", 0, 1, [], set())
    accepted = fhir._bounded_alohr_graphql_items(
        [{"id": "a"}, {"id": "b"}],
        "next",
        stream_state,
        _alohr_options(per_resource_limit=2),
    )
    assert accepted == [{"id": "a"}]
    assert stream_state.is_row_limit_reached is True

    monkeypatch.setattr(
        fhir.asyncio,
        "to_thread",
        AsyncMock(return_value=(None, None, "offline", 1)),
    )
    assert await fhir._fetch_alohr_graphql_page(
        "query",
        "providers",
        "providerList",
        next_token=None,
        timeout=1,
    ) == ([], None, "offline")


@pytest.mark.asyncio
async def test_address_overlay_remaining_noop_and_validation_boundaries(monkeypatch):
    monkeypatch.setattr(fhir, "_is_table_present", AsyncMock(return_value=False))
    assert await fhir._backfill_address_overlay_stage_coordinates(
        "mrf",
        '"mrf"."stage"',
    ) == 0
    assert await fhir._copy_existing_address_overlay(
        '"mrf"."stage"',
        '"mrf"."target"',
        "source_id",
        [],
    ) == 0
    with pytest.raises(ValueError, match="unknown Provider Directory"):
        fhir._address_overlay_component_insert_sql(
            "mrf",
            "stage",
            component="unknown",
            run_id=None,
            source_ids=[],
        )
    assert fhir._address_overlay_query_params("run", ["source-a"]) == {
        "run_id": "run",
        "source_ids": ["source-a"],
    }


@pytest.mark.asyncio
async def test_address_overlay_publish_skips_missing_requirements(monkeypatch):
    monkeypatch.setattr(
        fhir,
        "_address_overlay_missing_requirement",
        AsyncMock(return_value="provider_directory_source_unavailable"),
    )
    assert await fhir.publish_provider_directory_address_overlay("mrf") == {
        "skipped": True,
        "reason": "provider_directory_source_unavailable",
    }


@pytest.mark.asyncio
async def test_existing_pacing_lock_and_zero_wait_preserve_source_order(monkeypatch):
    source_by_field = {
        "_provider_directory_request_pacing_lock": asyncio.Lock(),
        "_provider_directory_last_request_monotonic": 0.0,
    }
    monkeypatch.setattr(fhir, "_source_request_interval_seconds", lambda *_args: 1.0)
    sleep = AsyncMock()
    monkeypatch.setattr(fhir.asyncio, "sleep", sleep)
    await fhir._pace_source_request(source_by_field)
    sleep.assert_not_awaited()
    assert source_by_field["_provider_directory_last_request_monotonic"] > 0


def test_catalog_rejects_duplicate_and_unmatched_contra_costa_rows(tmp_path):
    link = (
        "<a href='https://example.test/provider-directory/metadata'>"
        "Provider Directory API Base URL</a>"
    )
    rows = fhir._contra_costa_seed_rows_from_developer_html(link + link)
    assert len(rows) == 1
    assert fhir._contra_costa_seed_rows_from_developer_html(
        link,
        source_query="not-this-source",
    ) == []

    empty_catalog = tmp_path / "empty.html"
    empty_catalog.write_text("<html></html>", encoding="utf-8")
    fallback_rows, fallback_metrics = fhir._seed_rows_from_contra_costa_catalog(
        catalog_path=str(empty_catalog)
    )
    assert len(fallback_rows) == 1
    assert fallback_metrics["fallback"] is True


def test_retest_query_and_metadata_merge_reject_unrelated_identity(tmp_path):
    retest_path = tmp_path / "retest.json"
    retest_path.write_text(
        json.dumps(
            {
                "results": [
                    {
                        "classification": "valid",
                        "api_base": "https://example.test/fhir",
                        "org_name": "Reviewed Payer",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    assert fhir._seed_rows_from_retest_results(
        retest_path,
        source_query="other payer",
    ) == []

    target_by_field = {"api_base": "https://example.test/fhir", "metadata_json": {}}
    fhir._merge_skipped_source_row_metadata(target_by_field, {"metadata_json": {}})
    assert "provider_directory_merged_overrides" not in target_by_field["metadata_json"]


def test_url_catalog_reader_uses_bounded_user_agent(monkeypatch):
    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b"reviewed catalog"

    urlopen = lambda request, timeout: Response()
    monkeypatch.setattr(fhir.urllib.request, "urlopen", urlopen)
    assert fhir._read_text_from_path_or_url(
        None,
        "https://example.test/catalog",
        timeout=3,
    ) == ("reviewed catalog", "https://example.test/catalog")


@pytest.mark.asyncio
async def test_artifact_candidate_selection_covers_empty_and_valid_rows():
    empty_fence = fhir.ProviderDirectoryArtifactDatasetFence(
        (),
        should_select_validated_candidates=True,
    )
    executor = SimpleNamespace(all=AsyncMock())
    assert await fhir._artifact_eligible_validated_ids(empty_fence, executor) == {}
    executor.all.assert_not_awaited()

    dataset = fhir.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run",
    )
    fence = fhir.ProviderDirectoryArtifactDatasetFence(
        (dataset,),
        should_select_validated_candidates=True,
    )
    executor.all.return_value = [
        {"endpoint_id": "endpoint-a", "dataset_id": "candidate-a"},
        {"endpoint_id": None, "dataset_id": "ignored"},
    ]
    assert await fhir._artifact_eligible_validated_ids(fence, executor) == {
        "endpoint-a": ["candidate-a"]
    }


def test_complete_artifact_stage_bundle_matches_expected_relation():
    stage = fhir.ProviderDirectoryPreparedArtifactStage(
        schema="mrf",
        stage_table="network_stage",
        target_relation=fhir.PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
        rename_stage_indexes=AsyncMock(),
    )
    fhir._assert_candidate_artifact_stage_relations_complete(
        {"network_catalog"},
        {"network_catalog": {"published": True}},
        fhir.ProviderDirectoryArtifactBundle(stages=[stage]),
    )


