# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Concurrency boundaries for resumable Provider Directory groups."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _source_by_field(source_id: str) -> dict[str, str]:
    api_base = f"https://{source_id}.example.test/fhir"
    return {
        "source_id": source_id,
        "api_base": api_base,
        "canonical_api_base": api_base,
    }


def _checkpoint_context(source_id: str):
    return importer.PaginationCheckpointContext(
        canonical_api_base=f"https://{source_id}.example.test/fhir",
        source_scope_hash=f"scope-{source_id}",
        source_ids=(source_id,),
        owner_run_id="run-current",
        retry_of_run_id=None,
        acquisition_root_run_id="run-current",
        endpoint_id=f"endpoint-{source_id}",
        dataset_id=f"dataset-{source_id}",
        lineage_verified=True,
    )


def _fetch_result(*, complete: bool):
    return importer.ResourceFetchResult(
        model=importer.ProviderDirectoryLocation,
        rows=[],
        rows_fetched=1,
        rows_written=1,
        pages_fetched=1,
        complete=complete,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=False,
        next_url_remaining=not complete,
        error=None if complete else "http_503",
    )


def _patch_group_import(monkeypatch):
    """Patch two isolated groups and return their lifecycle observations."""
    finalized_dataset_ids: list[str] = []
    cleared_dataset_ids: list[str] = []

    async def prepare(source_records, *_args, **_kwargs):
        source_by_field = dict(source_records[0])
        source_id = source_by_field["source_id"]
        context = _checkpoint_context(source_id)
        source_by_field["_pagination_checkpoint_context"] = context
        source_by_field["_endpoint_dataset_id"] = context.dataset_id
        candidate = importer.EndpointDatasetCandidate(
            endpoint_id=context.endpoint_id,
            dataset_id=context.dataset_id,
            acquisition_root_run_id=context.acquisition_root_run_id,
            source_ids=(source_id,),
            selected_resources=("Location",),
            import_run_id=context.owner_run_id,
            previous_dataset_id=None,
            checkpoint_context=context,
        )
        return [source_by_field], candidate

    async def fetch(source_by_field, _resource_type, **_kwargs):
        return _fetch_result(complete=source_by_field["source_id"] == "source-b")

    async def finalize(candidate, _diagnostics_by_resource):
        finalized_dataset_ids.append(candidate.dataset_id)
        return {"status": importer.ENDPOINT_DATASET_VALIDATED}

    async def clear(candidate):
        cleared_dataset_ids.append(candidate.dataset_id)

    monkeypatch.setattr(importer, "_prepare_resource_import_source_group", prepare)
    monkeypatch.setattr(importer, "_fetch_resource_rows", fetch)
    monkeypatch.setattr(importer, "_finalize_endpoint_dataset_candidate", finalize)
    monkeypatch.setattr(importer, "_clear_finalized_dataset_checkpoints", clear)
    monkeypatch.setattr(
        importer,
        "_update_source_resource_import_metadata",
        AsyncMock(),
    )
    mark_failed = AsyncMock()
    monkeypatch.setattr(importer, "_mark_endpoint_dataset_candidate", mark_failed)
    return finalized_dataset_ids, cleared_dataset_ids, mark_failed


@pytest.mark.asyncio
async def test_resumable_group_does_not_suppress_complete_sibling(monkeypatch):
    """Finalize a complete group while a concurrent sibling retains a cursor."""

    finalized_dataset_ids, cleared_dataset_ids, mark_failed = (
        _patch_group_import(monkeypatch)
    )
    resume_required_entries: set[str] = set()

    counts = await importer._import_resources(
        [_source_by_field("source-a"), _source_by_field("source-b")],
        resources=["Location"],
        per_resource_limit=0,
        page_limit=0,
        page_count=100,
        timeout=3,
        run_id="run-current",
        stream_batch_size=1,
        source_concurrency=2,
        is_pagination_checkpointing_enabled=True,
        pagination_resume_required=resume_required_entries,
    )

    assert counts == {"Location": 2}
    assert resume_required_entries == {"source-a:Location"}
    assert finalized_dataset_ids == cleared_dataset_ids == ["dataset-source-b"]
    mark_failed.assert_not_awaited()
