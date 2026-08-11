# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Serial-stop boundaries for reviewed Provider Directory traversal."""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import logging
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_fhir_subset_completion_support import (
    build_subset_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _runner(source_by_field, resource_types):
    """Return a scan runner whose fetch path can be replaced per test."""

    return importer.ResourceGroupScanRunner(
        source_record=source_by_field,
        source_ids=[source_by_field["source_id"]],
        group_key=1,
        options=importer.ResourceGroupScanOptions(
            per_resource_limit=0,
            page_limit=0,
            page_count=250,
            timeout=3,
            run_id="run-a",
            stream_batch_size=1,
            cancel_ctx=None,
            cancel_task=None,
            bulk_export=False,
            bulk_export_max_pending_seconds=0,
            resource_deadline_seconds=0,
            linked_resource_limit=0,
            stale_cleanup=False,
            seen_stage_table=None,
            scan_role_reverse_lookup_planned=False,
            source_concurrency=1,
            requested_resource_concurrency=1,
            checkpointing_enabled=True,
            deferred_materialization=True,
        ),
        progress_lock=asyncio.Lock(),
        active_group_by_key={},
        report_progress=AsyncMock(),
        persist_rows=AsyncMock(),
        partial_progress=AsyncMock(),
        mark_stale_ready=AsyncMock(),
        count_by_resource=dict.fromkeys(resource_types, 0),
        stats_by_resource={},
        resource_completion=None,
        rows_by_resource={},
        diagnostic_by_resource={},
    )


def _diagnostic(*, error=None, complete=True, retry_not_before=None):
    """Return one minimal resource diagnostic used by finalization."""

    return {
        "bounded": False,
        "complete": complete,
        "error": error,
        "fetch_mode": importer.SERVER_ISSUED_SUBSET_FETCH_MODE,
        "pages_fetched": 12,
        "rows_fetched": 2500,
        "retry_not_before": retry_not_before,
    }


def _reviewed_source():
    """Return one neutral reviewed-v3 source and its contract."""

    contract = build_subset_contract()
    source_by_field = {
        "source_id": contract.source_id,
        "api_base": "https://directory.example.test/fhir",
        "canonical_api_base": "https://directory.example.test/fhir",
        importer.CURRENT_VERSION_CENSUS_CONTRACT_FIELD: contract,
        "_pagination_checkpoint_context": importer.PaginationCheckpointContext(
            canonical_api_base="https://directory.example.test/fhir",
            source_scope_hash="1" * 64,
            source_ids=(contract.source_id,),
            owner_run_id="run-a",
            acquisition_root_run_id="run-a",
        ),
    }
    return source_by_field, contract


@pytest.mark.asyncio
async def test_reviewed_serial_scan_stops_and_preserves_resumable_diagnostic(
    monkeypatch,
    caplog,
):
    """Stop before the seventh cursor and let normal resume classification run."""

    source_by_field, contract = _reviewed_source()
    resource_types = list(contract.resources)
    runner = _runner(source_by_field, resource_types)
    scanned_resource_types = []
    retry_at = "2026-08-11T03:30:00.000000Z"

    async def import_one(resource_type, *, report_start):
        assert report_start is True
        scanned_resource_types.append(resource_type)
        runner.diagnostic_by_resource[resource_type] = _diagnostic(
            complete=resource_type != "HealthcareService",
            error=(
                f"{importer.CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:TimeoutError"
                if resource_type == "HealthcareService"
                else None
            ),
            retry_not_before=(
                retry_at if resource_type == "HealthcareService" else None
            ),
        )
        return None

    monkeypatch.setattr(runner, "import_one", import_one)
    caplog.set_level(logging.WARNING)

    await runner.scan(resource_types)
    resume_entries = set()
    await importer._finalize_source_pagination_checkpoints(
        source_by_field,
        runner.diagnostic_by_resource,
        resume_entries,
    )

    assert scanned_resource_types[-1] == "HealthcareService"
    assert "OrganizationAffiliation" not in scanned_resource_types
    assert resume_entries == {f"{contract.source_id}:HealthcareService"}
    assert runner.diagnostic_by_resource["HealthcareService"][
        "retry_not_before"
    ] == retry_at
    warning = caplog.records[-1].getMessage()
    assert "resource_type=HealthcareService" in warning
    assert "completeness_retryable:TimeoutError" in warning
    assert source_by_field["api_base"] not in warning


@pytest.mark.asyncio
async def test_reviewed_terminal_failure_keeps_full_disposition_evidence(
    monkeypatch,
):
    """Keep scanning after a blocked result so the exact seal remains possible."""

    source_by_field, contract = _reviewed_source()
    resource_types = list(contract.resources)
    runner = _runner(source_by_field, resource_types)
    scanned_resource_types = []

    async def import_one(resource_type, *, report_start):
        assert report_start is True
        scanned_resource_types.append(resource_type)
        runner.diagnostic_by_resource[resource_type] = _diagnostic(
            complete=resource_type != "HealthcareService",
            error=(
                f"{importer.CURRENT_VERSION_CENSUS_BLOCKED_ERROR}:http_410"
                if resource_type == "HealthcareService"
                else None
            ),
        )
        return None

    monkeypatch.setattr(runner, "import_one", import_one)
    await runner.scan(resource_types)

    with pytest.raises(
        importer.ProviderDirectoryPaginationTerminalFailure
    ) as observed_failure:
        await importer._finalize_source_pagination_checkpoints(
            source_by_field,
            runner.diagnostic_by_resource,
            set(),
        )

    assert set(scanned_resource_types) == set(resource_types)
    assert set(runner.diagnostic_by_resource) == set(resource_types)
    assert "HealthcareService=" in str(observed_failure.value)
    assert observed_failure.value.diagnostics_by_resource == (
        runner.diagnostic_by_resource
    )


@pytest.mark.asyncio
async def test_ordinary_serial_scan_keeps_collecting_after_incomplete_result(
    monkeypatch,
):
    """Preserve collect-all behavior for sources outside reviewed v3."""

    resource_types = ["HealthcareService", "OrganizationAffiliation"]
    source_by_field = {
        "source_id": "ordinary-source",
        "api_base": "https://ordinary.example.test/fhir",
        "canonical_api_base": "https://ordinary.example.test/fhir",
    }
    runner = _runner(source_by_field, resource_types)
    scanned_resource_types = []

    async def import_one(resource_type, *, report_start):
        assert report_start is True
        scanned_resource_types.append(resource_type)
        runner.diagnostic_by_resource[resource_type] = _diagnostic(
            complete=resource_type != "HealthcareService",
            error="http_503" if resource_type == "HealthcareService" else None,
        )
        return None

    monkeypatch.setattr(runner, "import_one", import_one)
    await runner.scan(resource_types)

    assert set(scanned_resource_types) == set(resource_types)
    assert set(runner.diagnostic_by_resource) == set(resource_types)


def _patch_outer_resume_flow(monkeypatch, fetched_resource_types, retry_at):
    """Install the normal group callbacks around one retryable sixth cursor."""

    async def prepare(source_records, *_args, **_kwargs):
        prepared_source_by_field = dict(source_records[0])
        prepared_source_by_field["_pagination_checkpoint_context"] = (
            _reviewed_source()[0]["_pagination_checkpoint_context"]
        )
        return [prepared_source_by_field], None

    async def fetch(_source, resource_type, **_kwargs):
        fetched_resource_types.append(resource_type)
        is_incomplete = resource_type == "HealthcareService"
        return importer.ResourceFetchResult(
            model=importer.RESOURCE_MODELS_BY_TYPE[resource_type],
            rows=[],
            rows_fetched=0 if is_incomplete else 1,
            rows_written=0 if is_incomplete else 1,
            pages_fetched=12,
            complete=not is_incomplete,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=is_incomplete,
            error=(
                f"{importer.CURRENT_VERSION_CENSUS_RETRYABLE_ERROR}:TimeoutError"
                if is_incomplete
                else None
            ),
            fetch_mode=importer.SERVER_ISSUED_SUBSET_FETCH_MODE,
            retry_not_before=retry_at if is_incomplete else None,
        )

    @contextlib.asynccontextmanager
    async def worker_guard(_context):
        yield None

    source_metadata_writer = AsyncMock()
    monkeypatch.setattr(importer, "_prepare_resource_import_source_group", prepare)
    monkeypatch.setattr(importer, "_fetch_resource_rows", fetch)
    monkeypatch.setattr(importer, "_pagination_checkpoint_worker_guard", worker_guard)
    monkeypatch.setattr(
        importer,
        "_resource_group_pagination_checkpoint_context",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        importer,
        "_update_source_resource_import_metadata",
        source_metadata_writer,
    )
    return source_metadata_writer


@pytest.mark.asyncio
async def test_reviewed_serial_stop_reaches_outer_resume_evidence(
    monkeypatch,
    caplog,
):
    """Retain metadata and retry timing through the normal group boundary."""

    source_by_field, contract = _reviewed_source()
    source_by_field.pop("_pagination_checkpoint_context")
    resource_types = list(contract.resources)
    fetched_resource_types = []
    retry_at = "2026-08-11T03:30:00.000000Z"
    source_metadata_writer = _patch_outer_resume_flow(
        monkeypatch,
        fetched_resource_types,
        retry_at,
    )
    resume_entries = set()
    retry_not_before_by_resource = {}
    caplog.set_level(logging.WARNING)

    resource_count_by_type = await importer._import_resources(
        [source_by_field],
        resources=resource_types,
        per_resource_limit=0,
        page_limit=0,
        page_count=contract.page_count,
        timeout=3,
        run_id="run-a",
        stream_batch_size=1,
        resource_scan_concurrency=1,
        is_pagination_checkpointing_enabled=True,
        defer_typed_materialization=True,
        pagination_resume_required=resume_entries,
        resource_retry_not_before=retry_not_before_by_resource,
    )

    assert fetched_resource_types[-1] == "HealthcareService"
    assert "OrganizationAffiliation" not in fetched_resource_types
    assert resume_entries == {f"{contract.source_id}:HealthcareService"}
    assert retry_not_before_by_resource == {"HealthcareService": retry_at}
    assert resource_count_by_type["HealthcareService"] == 0
    assert resource_count_by_type["OrganizationAffiliation"] == 0
    retained_diagnostics = source_metadata_writer.await_args.kwargs["diagnostics"]
    assert set(retained_diagnostics) == set(fetched_resource_types)
    assert retained_diagnostics["HealthcareService"]["error"].endswith(
        ":TimeoutError"
    )
    assert any(
        "reviewed serial scan stopped" in log_entry.getMessage()
        for log_entry in caplog.records
    )
