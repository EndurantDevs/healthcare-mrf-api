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


def test_alohr_stream_bounds_and_locator_identity():
    page_limited = fhir.AlohrGraphQLStreamState("locator", 1, 0, [], set())
    assert fhir._can_fetch_alohr_graphql_stream(
        page_limited,
        _alohr_options(page_limit=1),
    ) is False
    assert page_limited.is_page_limit_reached is True

    row_limited = fhir.AlohrGraphQLStreamState("locator", 0, 2, [], set())
    assert fhir._can_fetch_alohr_graphql_stream(
        row_limited,
        _alohr_options(per_resource_limit=2),
    ) is False
    assert row_limited.is_row_limit_reached is True

    unbounded = fhir.AlohrGraphQLStreamState("locator", 0, 0, [], set())
    items = [{"id": "a"}, {"id": "b"}]
    assert fhir._bounded_alohr_graphql_items(
        items,
        None,
        unbounded,
        _alohr_options(),
    ) is items


@pytest.mark.asyncio
async def test_alohr_stream_rejects_replayed_and_failed_pages(monkeypatch):
    spec = fhir.AlohrGraphQLStreamSpec(
        query="query",
        root_key="providers",
        item_key="providerList",
        append_item=lambda *_args: None,
        resource_types=("Organization",),
        checkpoint_resource_type="alohr:Organization",
    )
    locator = fhir._alohr_graphql_checkpoint_locator(spec.root_key, None)
    locator_hash = fhir._pagination_url_hash(locator)
    replayed = fhir.AlohrGraphQLStreamState(
        locator,
        0,
        0,
        [],
        {locator_hash},
    )
    assert await fhir._can_advance_alohr_graphql_stream_page(
        _alohr_import_state(),
        spec,
        replayed,
    ) is False
    assert replayed.error == "pagination_cursor_repeated"

    fresh = fhir.AlohrGraphQLStreamState(locator, 0, 0, [], set())
    monkeypatch.setattr(
        fhir,
        "_fetch_alohr_graphql_page",
        AsyncMock(return_value=([], None, "offline")),
    )
    assert await fhir._can_advance_alohr_graphql_stream_page(
        _alohr_import_state(),
        spec,
        fresh,
    ) is False
    assert fresh.error == "offline"


@pytest.mark.asyncio
async def test_alohr_stale_cleanup_handles_every_source_state(monkeypatch):
    options = _alohr_options()
    state = _alohr_import_state(
        options=options,
        selected_resource_types={"Organization", "Practitioner", "Unknown"},
        diagnostics_by_resource={
            "Organization": {"complete": False},
            "Practitioner": {"complete": True},
            "Unknown": {"complete": True},
        },
    )
    delete = AsyncMock(return_value=2)
    monkeypatch.setattr(fhir, "_delete_stale_resource_rows", delete)
    await fhir._finalize_alohr_stale_rows(state)
    assert state.stale_counts_by_resource == {"Practitioner": 2}

    seen_state = _alohr_import_state(
        options=_alohr_options(seen_table="seen"),
        diagnostics_by_resource={"Organization": {"complete": True}},
    )
    await fhir._finalize_alohr_stale_rows(seen_state)
    assert seen_state.stale_ready_source_ids_by_resource == {
        "Organization": ["source-a"]
    }


def test_address_overlay_scope_and_retention_identity_boundaries():
    assert fhir._coerce_rowcount(None) == 0
    with pytest.raises(ValueError, match="coordinate axis"):
        fhir._coordinate_from_location_sql("lat", "long", "country", axis="z")

    current = fhir._address_overlay_stage_table_name("current")
    other = fhir._address_overlay_stage_table_name("other")
    owner_token = other.rsplit("_", 1)[-1][
        : fhir.PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_OWNER_TOKEN_LENGTH
    ]
    assert fhir._address_overlay_stage_retention_status(None, current, {}) == "rejected"
    assert fhir._address_overlay_stage_retention_status(current, current, {}) == "current_stage_retained"
    assert fhir._address_overlay_stage_retention_status(other, current, {}) == "unowned_stage_retained"
    assert fhir._address_overlay_stage_retention_status(
        other,
        current,
        {owner_token: [("running", False)]},
    ) == "active_stage_retained"
    assert fhir._address_overlay_stage_retention_status(
        other,
        current,
        {owner_token: [("failed", False)]},
    ) == "recent_stage_retained"
    assert fhir._address_overlay_stage_retention_status(
        other,
        current,
        {owner_token: [("failed", True), ("failed", True)]},
    ) == "ambiguous_owner_retained"
    assert fhir._address_overlay_stage_retention_status(
        other,
        current,
        {owner_token: [("failed", True)]},
    ) is None


@pytest.mark.asyncio
async def test_address_overlay_scope_and_cleanup_fail_closed(monkeypatch):
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=[("source-a",), (None,)]))
    assert await fhir._address_overlay_scope_sources(
        "mrf",
        run_id="run",
        source_ids=None,
    ) == ["source-a"]

    current = fhir._address_overlay_stage_table_name("current")
    with pytest.raises(ValueError, match="invalid Provider Directory"):
        await fhir._cleanup_orphan_address_overlay_stages(
            "mrf",
            current_stage_table="invalid",
        )
    monkeypatch.setattr(fhir, "_address_overlay_stage_owners", AsyncMock(return_value=None))
    unavailable = await fhir._cleanup_orphan_address_overlay_stages(
        "mrf",
        current_stage_table=current,
    )
    assert unavailable["ownership_unavailable"] == 1


@pytest.mark.asyncio
async def test_address_overlay_build_defers_and_cleans_failed_stage(monkeypatch):
    monkeypatch.setattr(
        fhir,
        "_cleanup_orphan_address_overlay_stages",
        AsyncMock(return_value=fhir._address_overlay_stage_cleanup_counts()),
    )
    monkeypatch.setattr(fhir.db, "status", AsyncMock(return_value="CREATE TABLE"))
    monkeypatch.setattr(
        fhir,
        "_populate_address_overlay_stage",
        AsyncMock(
            return_value={
                "stage_rows": 1,
                "inserted": 1,
                "inserted_by_component": {"organization": 1},
                "duplicates_removed": 0,
                "copied_existing": 0,
            }
        ),
    )
    prepared = ("prepared", "stage")
    monkeypatch.setattr(
        fhir,
        "_prepared_address_overlay_result",
        AsyncMock(return_value=prepared),
    )
    stage_result = await fhir._build_provider_directory_address_overlay_stage(
        "mrf",
        "run",
        ["source-a"],
        None,
        '"mrf"."provider_directory_address_overlay"',
        fhir.ProviderDirectoryArtifactBuildFence(target_oid=1),
        True,
    )
    assert stage_result == prepared

    fhir._populate_address_overlay_stage.side_effect = RuntimeError("failed")
    drop = AsyncMock()
    monkeypatch.setattr(fhir, "_drop_address_overlay_stage_best_effort", drop)
    with pytest.raises(RuntimeError, match="failed"):
        await fhir._build_provider_directory_address_overlay_stage(
            "mrf",
            "run",
            ["source-a"],
            None,
            '"mrf"."provider_directory_address_overlay"',
            fhir.ProviderDirectoryArtifactBuildFence(target_oid=1),
            False,
        )
    drop.assert_awaited_once()


def test_catalog_parsers_dedupe_replayed_source_identity(tmp_path):
    parser = fhir._AmeriHealthCaritasCatalogParser()
    parser.feed("<table><tr><td><a>missing href</a></td></tr></table>")
    assert parser.rows == [(["missing href"], [])]

    source_rows = [
        {
            "source_id": "catalog",
            "org_name": "Reviewed Payer",
            "plan_name": "Reviewed Plan",
            "api_base": "https://example.test/fhir",
            "metadata_json": {},
        },
        {
            "source_id": "retest",
            "org_name": "Reviewed Payer",
            "plan_name": "Reviewed Plan",
            "api_base": "https://example.test/fhir",
            "seed_source": "provider-directory-db-retest",
            "metadata_json": {"provider_directory_override": "retest"},
        },
    ]
    deduped = fhir._dedupe_source_rows(source_rows)
    assert deduped == [source_rows[0]]
    assert source_rows[0]["metadata_json"][
        "provider_directory_merged_overrides"
    ] == ["retest"]

    catalog_path = tmp_path / "contra-costa.html"
    catalog_path.write_text(
        "<a href='https://ignored.test'>Other link</a>"
        "<a href='https://example.test/provider-directory/metadata'>"
        "Provider Directory API Base URL</a>",
        encoding="utf-8",
    )
    seed_rows, metrics = fhir._seed_rows_from_contra_costa_catalog(
        catalog_path=str(catalog_path)
    )
    assert len(seed_rows) == 1
    assert metrics.get("fallback") is None


@pytest.mark.asyncio
async def test_source_request_pacing_and_candidate_exhaustion(monkeypatch):
    source_by_field = {
        "_provider_directory_last_request_monotonic": fhir.time.monotonic()
    }
    monkeypatch.setattr(fhir, "_source_request_interval_seconds", lambda *_args: 1.0)
    sleep = AsyncMock()
    monkeypatch.setattr(fhir.asyncio, "sleep", sleep)
    await fhir._pace_source_request(source_by_field)
    sleep.assert_awaited_once()
    assert 0 < sleep.await_args.args[0] <= 1
    assert "_provider_directory_request_pacing_lock" in source_by_field

    monkeypatch.setattr(fhir, "_source_fetch_retry_attempts", lambda: 1)
    monkeypatch.setattr(
        fhir,
        "_fetch_source_json_attempt",
        AsyncMock(return_value=((200, {"resourceType": "Bundle"}, None, 3), False, False, False)),
    )
    result, smaller_page, retries = await fhir._fetch_source_json_candidate(
        {},
        "https://example.test/fhir/Organization?_count=1",
        timeout=1,
        is_last_candidate=True,
    )
    assert result[0] == 200
    assert smaller_page is False and retries == 0


def test_molina_quota_duration_and_bulk_flag_defaults():
    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    source_by_field = {"canonical_api_base": fhir.MOLINA_PROVIDER_DIRECTORY_BASE}
    payload = {
        "message": "Out of call volume quota; replenished in 00:01:02",
    }
    assert fhir._molina_quota_retry_not_before(
        source_by_field,
        403,
        payload,
        now_utc=now,
    ) == "2026-07-22T00:02:02Z"
    assert fhir._is_bulk_export_enabled(None) is False


@pytest.mark.asyncio
async def test_partition_residual_and_stopped_chain_do_not_publish(monkeypatch):
    request = _partition_page_request()
    monkeypatch.setattr(fhir, "_uhc_exact_partition_entries", lambda *_args: [])
    monkeypatch.setattr(fhir, "_consume_partition_entries", AsyncMock())
    monkeypatch.setattr(
        fhir,
        "_partition_page_next_result",
        AsyncMock(return_value=fhir.PartitionPageFetchResult(None, "failed")),
    )
    page_result = await fhir._split_partition_page_result(
        request,
        {"resourceType": "Bundle"},
    )
    assert page_result.outcome == "failed"

    monkeypatch.setattr(fhir, "_uhc_partition_identity", lambda *_args: "partition")
    monkeypatch.setattr(
        fhir,
        "_should_continue_partition_chain",
        AsyncMock(return_value=False),
    )
    flush = AsyncMock()
    monkeypatch.setattr(fhir, "_flush_partition_resource_rows", flush)
    checkpoint = AsyncMock()
    monkeypatch.setattr(fhir, "_record_partition_checkpoint", checkpoint)
    chain = fhir.PartitionChainRequest(
        request.source_record,
        request.resource_type,
        request.start_url_queue,
        request.state,
        request.current_url,
        request.fetch_options,
    )
    await fhir._fetch_partition_url_chain(chain)
    flush.assert_awaited_once()
    checkpoint.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_prepare_skips_completed_identity(monkeypatch):
    monkeypatch.setattr(
        fhir,
        "_load_partition_checkpoints",
        AsyncMock(return_value={"completed"}),
    )
    monkeypatch.setattr(fhir, "_uhc_partition_identity", lambda *_args: "completed")
    queue, state = await fhir._prepare_partition_fetch(
        {"source_id": "source-a"},
        "Organization",
        fhir.ProviderDirectoryOrganization,
        ["https://example.test/fhir/Organization"],
        "run",
    )
    assert queue.empty()
    assert state.completed_partition_identities == {"completed"}


def test_partition_checkpoint_and_preflight_contracts_fail_closed():
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
    with pytest.raises(ValueError, match="identity mismatch"):
        fhir._partition_checkpoint_control("[]", config)

    options_by_field = {
        "timeout": 1,
        "run_id": "run",
        "row_batch_handler": AsyncMock(return_value=0),
        "row_batch_size": 10,
        "retain_rows": False,
        "cancel_ctx": None,
        "cancel_task": None,
        "deadline_seconds": 0,
        "pagination_checkpoint": _checkpoint_context(),
    }
    bounded = fhir.LastUpdatedPartitionFetchOptions(
        per_resource_limit=1,
        page_limit=0,
        **options_by_field,
    )
    assert fhir._partition_preflight_failure(
        fhir.ProviderDirectoryOrganization,
        bounded,
    ).error.endswith("bounded_runtime_limits_not_allowed")
    streaming = fhir.LastUpdatedPartitionFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        **{**options_by_field, "row_batch_handler": None},
    )
    assert fhir._partition_preflight_failure(
        fhir.ProviderDirectoryOrganization,
        streaming,
    ).error.endswith("streaming_output_required")
    durable = fhir.LastUpdatedPartitionFetchOptions(
        per_resource_limit=0,
        page_limit=0,
        **{**options_by_field, "pagination_checkpoint": None},
    )
    assert fhir._partition_preflight_failure(
        fhir.ProviderDirectoryOrganization,
        durable,
    ).error.endswith("durable_checkpoint_context_required")


