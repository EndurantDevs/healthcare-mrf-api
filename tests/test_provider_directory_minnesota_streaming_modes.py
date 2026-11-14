# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Streaming completion uses candidate counts independently of checkpoints."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_bcbs_minnesota import carrier_source as carrier_source
from tests.test_provider_directory_bcbs_minnesota import directory
from tests.test_provider_directory_minnesota_cached_uniqueness import _CachedCandidate


def _streaming_runner(source_record, candidate, run_id, stale_cleanup):
    options = directory.ResourceGroupScanOptions(
        per_resource_limit=0,
        page_limit=0,
        page_count=500,
        timeout=3,
        run_id=run_id,
        stream_batch_size=1000,
        cancel_ctx=None,
        cancel_task=None,
        bulk_export=False,
        bulk_export_max_pending_seconds=1,
        resource_deadline_seconds=0,
        linked_resource_limit=0,
        stale_cleanup=stale_cleanup,
        seen_stage_table=None,
        scan_role_reverse_lookup_planned=False,
        source_concurrency=1,
        requested_resource_concurrency=1,
        checkpointing_enabled=False,
        deferred_materialization=False,
    )
    return directory.ResourceGroupScanRunner(
        source_record=source_record,
        source_ids=list(source_record["_partition_checkpoint_source_ids"]),
        group_key=0,
        options=options,
        progress_lock=asyncio.Lock(),
        active_group_by_key={0: {}},
        report_progress=AsyncMock(),
        persist_rows=candidate.write,
        partial_progress=AsyncMock(),
        mark_stale_ready=AsyncMock(),
        count_by_resource={"Location": 0},
        stats_by_resource={},
        resource_completion=None,
        rows_by_resource={},
        diagnostic_by_resource={},
    )


@pytest.mark.parametrize(
    ("publish_artifacts", "stale_cleanup", "run_id"),
    [(True, False, "example-retry"), (False, True, "example-retry"), (True, False, None)],
    ids=["artifact-publication", "stale-cleanup", "unrooted-candidate"],
)
@pytest.mark.parametrize("repeats_first_page", [False, True])
@pytest.mark.parametrize("advertises_total", [False, True])
@pytest.mark.asyncio
async def test_streaming_candidate_totals_do_not_require_pagination_checkpoints(
    monkeypatch, carrier_source, publish_artifacts, stale_cleanup, run_id, repeats_first_page, advertises_total
):
    assert not directory._is_pagination_checkpoint_mode_enabled(
        run_id=run_id,
        full_refresh=True,
        resource_limit=0,
        page_limit=0,
        stream_batch_size=1000,
        stale_cleanup=stale_cleanup,
        publish_artifacts=publish_artifacts,
    )
    source_ids = ("example-alias", carrier_source["source_id"])
    carrier_source.update(
        _endpoint_id="example-endpoint",
        _endpoint_dataset_id="example-candidate",
        _partition_checkpoint_source_ids=source_ids,
        _partition_checkpoint_owner_run_id="example-previous-root",
    )
    assert "_pagination_checkpoint_context" not in carrier_source
    candidate = _CachedCandidate(repeats_first_page, advertises_total=advertises_total)
    aggregate = AsyncMock(side_effect=candidate.count)
    load_checkpoint = AsyncMock(side_effect=AssertionError("disabled checkpoints must not load"))
    save_checkpoint = AsyncMock(side_effect=AssertionError("disabled checkpoints must not save"))
    monkeypatch.setattr(directory, "_fetch_source_json", candidate.fetch)
    monkeypatch.setattr(directory, "_caresource_unique_candidate_count", aggregate)
    monkeypatch.setattr(directory, "_load_or_initialize_pagination_checkpoint", load_checkpoint)
    monkeypatch.setattr(directory, "_save_pagination_checkpoint", save_checkpoint)
    runner = _streaming_runner(carrier_source, candidate, run_id, stale_cleanup)

    fetch_result = await runner._fetch_rows("Location", carrier_source)

    assert fetch_result.rows == [] and fetch_result.rows_fetched == fetch_result.rows_written == 1060
    assert fetch_result.complete is not repeats_first_page
    assert fetch_result.error == ("minnesota_cache_unique_candidate_total_mismatch" if repeats_first_page else None)
    assert candidate.aggregate_observations == [(1060 - int(repeats_first_page), 0)]
    aggregate.assert_awaited_once()
    scope, resource = aggregate.await_args.args
    assert resource == "Location"
    assert vars(scope) == {
        "endpoint_id": "example-endpoint",
        "dataset_id": "example-candidate",
        "source_ids": source_ids,
        "acquisition_root_run_id": run_id,
    }
    assert len(candidate.requested_urls) == 4
    load_checkpoint.assert_not_awaited()
    save_checkpoint.assert_not_awaited()
