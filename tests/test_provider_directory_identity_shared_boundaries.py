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


def test_source_catalog_and_publish_target_identity_boundaries(tmp_path, monkeypatch):
    manifest_path = tmp_path / "manifest.json"
    monkeypatch.setattr(
        fhir,
        "PROVIDER_DIRECTORY_ENDPOINT_ACQUISITION_MANIFEST_PATH",
        manifest_path,
    )

    manifest_path.write_text(json.dumps({"entries": {}}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="manifest_invalid"):
        fhir._configured_catalog_protected_source_ids({})
    manifest_path.write_text(json.dumps({"entries": [None]}), encoding="utf-8")
    with pytest.raises(RuntimeError, match="manifest_invalid"):
        fhir._configured_catalog_protected_source_ids({})

    assert fhir._clean_source_id_list({"source-a": True})
    assert fhir._provider_directory_publish_artifact_targets([None]) is None
    with pytest.raises(ValueError, match="Unsupported"):
        fhir._provider_directory_publish_artifact_targets(b"not-a-target")


@pytest.mark.asyncio
async def test_empty_source_catalog_cleanup_is_a_noop(monkeypatch):
    monkeypatch.setattr(
        fhir,
        "_resolve_protected_catalog_source_ids",
        AsyncMock(return_value=(set(), [])),
    )
    assert await fhir._delete_stale_provider_directory_source_catalog([]) == {
        "deleted": {},
        "protected_source_ids_missing": [],
    }


def test_seed_text_and_transient_diagnostic_identity_boundaries(tmp_path, monkeypatch):
    text_path = tmp_path / "catalog.txt"
    text_path.write_text("reviewed catalog", encoding="utf-8")
    assert fhir._read_text_from_path_or_url(
        str(text_path),
        "https://unused.example.test",
        timeout=1,
    ) == ("reviewed catalog", str(text_path))
    assert fhir._extract_urls_from_text(
        "https://example.test/fhir https://example.test/fhir"
    ) == ["https://example.test/fhir"]

    molina_source_by_field = {
        "canonical_api_base": fhir.MOLINA_PROVIDER_DIRECTORY_BASE,
    }
    quota_payload_by_field = {"message": "Out of call volume quota"}
    assert fhir._is_molina_quota_response(
        molina_source_by_field,
        201,
        quota_payload_by_field,
    ) is False

    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    assert fhir._source_retry_after_seconds(
        {fhir.SOURCE_RETRY_AFTER_FIELD: "Wed, 22 Jul 2026 01:00:00"},
        max_delay_seconds=None,
        now_utc=now,
    ) == 3600
    assert fhir._molina_quota_retry_not_before(
        molina_source_by_field,
        403,
        quota_payload_by_field,
        now_utc=now,
    ) is None

    source_by_field = {}
    monkeypatch.setattr(fhir, "_source_fetch_response_class", lambda *_args: None)
    fhir._record_terminal_source_fetch_diagnostic(
        source_by_field,
        "https://example.test/fhir",
        (503, None, None, 7),
        retry_count=2,
    )
    assert fhir.SOURCE_FETCH_DIAGNOSTIC_FIELD not in source_by_field
    source_by_field[fhir.SOURCE_FETCH_DIAGNOSTIC_FIELD] = {
        "url_hash": fhir._pagination_url_hash("https://other.example.test"),
    }
    assert (
        fhir._terminal_source_fetch_diagnostic(
            source_by_field,
            "https://example.test/fhir",
        )
        is None
    )


def test_caresource_persisted_proof_and_count_payload_boundaries():
    assert fhir._caresource_persisted_pre_count(
        {"strategy_version": "old", "pre_count": 1}
    ) == (None, "checkpoint_strategy_mismatch")
    assert fhir._caresource_persisted_pre_count(
        {
            "strategy_version": fhir.CARESOURCE_OPAQUE_CURSOR_STRATEGY_VERSION,
            "pre_count": True,
        }
    ) == (None, "checkpoint_pre_count_invalid")
    count_fetch = fhir._validate_last_updated_partition_count_payload(
        {"resourceType": "Bundle", "type": "searchset", "total": True}
    )
    assert count_fetch.error == "count_total_not_exact"


@pytest.mark.asyncio
async def test_caresource_census_transport_and_nonexact_responses(monkeypatch):
    source_by_field = {"source_id": "source-a"}
    monkeypatch.setattr(
        fhir,
        "_fetch_source_json",
        AsyncMock(return_value=(503, {}, None, 1)),
    )
    monkeypatch.setattr(
        fhir,
        "_last_updated_partition_retry_not_before",
        lambda *_args, **_kwargs: "2026-07-22T01:00:00Z",
    )
    failure = await fhir._fetch_caresource_census_count(
        source_by_field,
        "https://example.test/fhir/Organization",
        timeout=1,
    )
    assert failure.error == "http_503"
    assert failure.transient is True
    assert failure.retry_not_before == "2026-07-22T01:00:00Z"

    fhir._fetch_source_json.return_value = (
        200,
        {"resourceType": "Bundle", "type": "searchset"},
        None,
        1,
    )
    nonexact = await fhir._fetch_caresource_census_count(
        source_by_field,
        "https://example.test/fhir/Organization",
        timeout=1,
    )
    assert nonexact.count is None
    assert nonexact.error == "count_total_not_exact"


@pytest.mark.asyncio
async def test_caresource_pre_proofs_fail_closed(monkeypatch):
    context = _checkpoint_context()
    resume = fhir.PaginationResumeState(
        next_url="https://example.test/next",
        pages_processed=2,
        rows_processed=3,
        recent_url_hashes=(),
        completeness={"strategy_version": "old", "pre_count": 3},
    )
    rejected = await fhir._prepare_caresource_pre_census(
        {},
        "Organization",
        fhir.ProviderDirectoryOrganization,
        context,
        resume,
        "https://example.test/fhir/Organization",
        timeout=1,
    )
    assert isinstance(rejected, fhir.ResourceFetchResult)
    assert rejected.error.endswith("checkpoint_strategy_mismatch")

    resume = fhir.PaginationResumeState(
        next_url="https://example.test/next",
        pages_processed=2,
        rows_processed=3,
        recent_url_hashes=(),
    )
    monkeypatch.setattr(
        fhir,
        "_fetch_caresource_census_count",
        AsyncMock(
            return_value=fhir.CareSourceCensusFetch(
                None,
                "offline",
                transient=True,
                retry_not_before="later",
            )
        ),
    )
    rejected = await fhir._prepare_caresource_pre_census(
        {},
        "Organization",
        fhir.ProviderDirectoryOrganization,
        context,
        resume,
        "https://example.test/fhir/Organization",
        timeout=1,
    )
    assert rejected.error.endswith("pre_census_offline")
    assert rejected.next_url_remaining is True



@pytest.mark.asyncio
async def test_caresource_post_proofs_fail_closed(monkeypatch):
    context = _checkpoint_context()
    monkeypatch.setattr(
        fhir,
        "_fetch_caresource_census_count",
        AsyncMock(
            return_value=fhir.CareSourceCensusFetch(
                None,
                "offline",
                transient=True,
                retry_not_before="later",
            )
        ),
    )
    save = AsyncMock()
    monkeypatch.setattr(fhir, "_save_pagination_checkpoint_completeness", save)
    census_outcome = await fhir._finish_caresource_opaque_cursor_census(
        {},
        "Organization",
        context,
        "https://example.test/fhir/Organization",
        {"pre_count": 3},
        timeout=1,
        rows_processed=3,
    )
    assert census_outcome.error == "post_census_offline"
    save.assert_awaited_once()

    with pytest.raises(RuntimeError, match="dataset_id_missing"):
        await fhir._caresource_unique_candidate_count(
            _checkpoint_context(dataset_id=None),
            "Organization",
        )


@pytest.mark.asyncio
async def test_partition_page_protocol_failures_are_bounded(monkeypatch):
    request = _partition_page_request()
    bounded_outcome = fhir._partition_page_bounded_outcome
    monkeypatch.setattr(fhir, "_fetch_valid_partition_bundle", AsyncMock(return_value=None))
    page_result = await fhir._fetch_partition_page(request)
    assert page_result.outcome == "failed"

    monkeypatch.setattr(
        fhir,
        "_fetch_valid_partition_bundle",
        AsyncMock(return_value={"resourceType": "Bundle"}),
    )
    monkeypatch.setattr(
        fhir,
        "_partition_page_bounded_outcome",
        AsyncMock(return_value="predicate_failed"),
    )
    page_result = await fhir._fetch_partition_page(request)
    assert page_result.outcome == "failed"

    monkeypatch.setattr(fhir, "_uhc_exact_partition_entries", lambda *_args: None)
    page_result = await fhir._split_partition_page_result(
        request,
        {"resourceType": "Bundle"},
    )
    assert page_result.outcome == "split"

    monkeypatch.setattr(fhir, "_uhc_location_partition_predicate_error", lambda *_args: None)
    monkeypatch.setattr(fhir, "_enqueue_adaptive_partition_children", lambda *_args: 0)
    monkeypatch.setattr(fhir, "_is_uhc_partition_cap_exhausted", lambda *_args: True)
    assert await bounded_outcome(
        request.source_record,
        request.resource_type,
        request.start_url_queue,
        request.state,
        request.partition_start_url,
        request.current_url,
        {"resourceType": "Bundle"},
    ) == "cap_failed"
    assert request.state.partition_error_count == 1


@pytest.mark.asyncio
async def test_partition_chain_skips_completed_and_stops_on_split(monkeypatch):
    request = _partition_page_request()
    partition_identity = "partition-a"
    request.state.completed_partition_identities.add(partition_identity)
    monkeypatch.setattr(fhir, "_uhc_partition_identity", lambda *_args: partition_identity)
    chain = fhir.PartitionChainRequest(
        source_record=request.source_record,
        resource_type=request.resource_type,
        start_url_queue=request.start_url_queue,
        state=request.state,
        request_url=request.current_url,
        fetch_options=request.fetch_options,
    )
    await fhir._fetch_partition_url_chain(chain)

    request.state.completed_partition_identities.clear()
    monkeypatch.setattr(fhir, "_should_continue_partition_chain", AsyncMock(return_value=True))
    monkeypatch.setattr(
        fhir,
        "_fetch_partition_page",
        AsyncMock(return_value=fhir.PartitionPageFetchResult(None, "split")),
    )
    flush = AsyncMock()
    monkeypatch.setattr(fhir, "_flush_partition_resource_rows", flush)
    checkpoint = AsyncMock()
    monkeypatch.setattr(fhir, "_record_partition_checkpoint", checkpoint)
    await fhir._fetch_partition_url_chain(chain)
    flush.assert_awaited_once()
    checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_enqueue_and_plan_resume_identity_edges(monkeypatch):
    state = fhir.PartitionFetchState(
        result_model=fhir.ProviderDirectoryOrganization,
        retained_resource_rows=[],
        completed_partition_identities={"child"},
    )
    queue = asyncio.Queue()
    monkeypatch.setattr(
        fhir,
        "_uhc_adaptive_partition_child_urls",
        lambda *_args: ["https://example.test/child"],
    )
    monkeypatch.setattr(fhir, "_uhc_partition_identity", lambda *_args: "child")
    assert fhir._enqueue_adaptive_partition_children({}, "Organization", queue, state, "x", {}) == 1
    assert queue.empty()

    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    invalid_config = fhir.LastUpdatedPartitionConfig(
        start=now,
        end=now,
        end_mode="fixed",
        ceiling=100,
        minimum_width=datetime.timedelta(seconds=1),
        boundary_precision=datetime.timedelta(seconds=1),
        page_count=10,
        volatile_metadata_paths=(),
    )
    with pytest.raises(RuntimeError, match="root_cutoff_invalid"):
        fhir._new_last_updated_partition_plan(invalid_config)

    context = _checkpoint_context()
    monkeypatch.setattr(fhir, "_fetch_pagination_checkpoint", AsyncMock(return_value={"x": 1}))
    resumed = SimpleNamespace(plan="resumed")
    monkeypatch.setattr(fhir, "_resume_partition_plan", AsyncMock(return_value=resumed))
    assert await fhir._load_partition_plan(context, "Organization", invalid_config) is resumed

    fhir._fetch_pagination_checkpoint.return_value = None
    with pytest.raises(RuntimeError, match="checkpoint_missing"):
        await fhir._load_partition_plan(context, "Organization", invalid_config)


