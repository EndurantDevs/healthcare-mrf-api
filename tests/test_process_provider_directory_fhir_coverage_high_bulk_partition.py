# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from db.models import ProviderDirectoryPractitioner
from tests.provider_directory_fhir_bulk_test_support import (
    _fetch_options,
    _identity,
    _manifest_payload,
    _source,
    _stream_options,
)
from tests.provider_directory_fhir_coverage_high_support import (
    importer,
    last_updated_partition_config,
    last_updated_partition_fetch_options,
    last_updated_partition_plan,
    last_updated_partition_state,
    resource_fetch_result,
)


@pytest.mark.asyncio
async def test_bulk_checkpoint_defensive_error_paths(monkeypatch):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    checkpoints = [{"state": importer.BULK_EXPORT_OUTPUT_COMPLETE}]
    monkeypatch.setattr(
        importer,
        "_complete_bulk_export_checkpoint",
        AsyncMock(side_effect=RuntimeError("completion-fenced")),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_output_checkpoint_error",
        Mock(return_value=None),
    )
    bulk_fetch_result = await importer._finalize_checkpointed_bulk_outputs(
        identity,
        manifest,
        _stream_options(range_resume_enabled=False),
        importer.BulkExportStreamState(),
        checkpoints,
    )
    assert bulk_fetch_result.error == "completion-fenced"

    async def fail_task():
        raise ValueError("bad-output")

    failed_task = asyncio.create_task(fail_task())
    await asyncio.sleep(0)
    active_task_map = {failed_task: (object(), {})}
    output_error = importer._completed_bulk_output_error(
        {failed_task},
        active_task_map,
    )
    assert output_error == "bulk_export_transport_valueerror"
    assert active_task_map == {}

    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_checkpoint_primary_secret",
        Mock(side_effect=RuntimeError("secret-unavailable")),
    )
    checkpoint, status_payload, error = (
        await importer._load_or_start_checkpointed_bulk_export(
            object(),
            _source(),
            identity,
            timeout=3,
            ownership_probe=AsyncMock(),
        )
    )
    assert (checkpoint, status_payload, error) == ({}, None, "secret-unavailable")


@pytest.mark.asyncio
async def test_bulk_configuration_and_terminal_repair_fail_closed(monkeypatch):
    identity = _identity()
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bulk_stream_options",
        Mock(side_effect=ValueError("invalid-stream-options")),
    )
    monkeypatch.setattr(importer, "_record_bulk_export_checkpoint_error", record_error)
    options, configured_fetch_result = await importer._configured_bulk_stream_options(
        identity,
        ProviderDirectoryPractitioner,
        _fetch_options(),
        2,
        AsyncMock(),
        _source(),
        {"rows_written": 9},
    )
    assert options is None
    assert configured_fetch_result.error == "invalid-stream-options"
    assert configured_fetch_result.rows_fetched == 9
    record_error.assert_awaited_once_with(
        identity,
        "invalid-stream-options",
        terminal=False,
    )

    monkeypatch.setattr(
        importer,
        "_load_or_start_checkpointed_bulk_export",
        AsyncMock(
            return_value=(
                {"state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE},
                None,
                None,
            )
        ),
    )
    monkeypatch.setattr(importer, "_clean_text", Mock(return_value=None))
    with pytest.raises(RuntimeError, match="terminal_checkpoint_state_missing"):
        await importer._load_active_bulk_checkpoint(
            object(),
            _source(),
            identity,
            ProviderDirectoryPractitioner,
            _fetch_options(),
            AsyncMock(),
            AsyncMock(),
        )


@pytest.mark.asyncio
async def test_bulk_terminal_repair_requires_repaired_terminal_result(monkeypatch):
    identity = _identity()
    terminal_result = resource_fetch_result(error=None)
    monkeypatch.setattr(
        importer,
        "_load_or_start_checkpointed_bulk_export",
        AsyncMock(
            return_value=(
                {"state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE},
                None,
                None,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_terminal_bulk_checkpoint_result",
        Mock(side_effect=[terminal_result, None]),
    )
    monkeypatch.setattr(
        importer,
        "_repair_terminal_bulk_export_checkpoint",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={"state": "active"}),
    )
    with pytest.raises(RuntimeError, match="terminal_checkpoint_repair_lost"):
        await importer._load_active_bulk_checkpoint(
            object(),
            _source(),
            identity,
            ProviderDirectoryPractitioner,
            _fetch_options(),
            AsyncMock(),
            AsyncMock(),
        )


@pytest.mark.asyncio
async def test_partition_page_and_missing_start_fail_explicitly(monkeypatch):
    fetch_state = importer.PartitionFetchState(
        result_model=ProviderDirectoryPractitioner,
        retained_resource_rows=[],
    )
    monkeypatch.setattr(
        importer,
        "_resolved_fhir_next_url",
        Mock(side_effect=ValueError("untrusted-next-link")),
    )
    page_result = await importer._partition_page_next_result(
        {"source_id": "source-1"},
        "https://example.test/current",
        "https://evil.test/next",
        fetch_state,
        False,
    )
    assert page_result.outcome == "failed"
    assert fetch_state.error_message == "partition_errors_1_last_untrusted-next-link"

    monkeypatch.setattr(importer, "_resource_start_url", Mock(return_value=None))
    missing_start = await importer._prepare_partition_state(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        last_updated_partition_fetch_options(),
    )
    assert missing_start.error.endswith(":resource_start_url_missing")


@pytest.mark.asyncio
async def test_partition_prepare_rejects_failed_resume(monkeypatch):
    failed_plan = last_updated_partition_plan()
    failed_plan.observe_count("root", importer.CountObservation.error("bad-count"))
    monkeypatch.setattr(
        importer,
        "_resource_start_url",
        Mock(return_value="https://example.test/fhir/Practitioner"),
    )
    monkeypatch.setattr(
        importer,
        "_load_partition_plan",
        AsyncMock(
            return_value=importer.LastUpdatedPartitionResume(
                plan=failed_plan,
                census=importer.LastUpdatedCompletenessCensus(failure="bad-count"),
            )
        ),
    )
    failed_resume = await importer._prepare_partition_state(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        last_updated_partition_fetch_options(),
    )
    assert "bad-count" in failed_resume.error


@pytest.mark.asyncio
async def test_partition_census_root_and_leaf_mismatches(monkeypatch):
    partition_config = last_updated_partition_config()
    partition_state = last_updated_partition_state(
        census=importer.LastUpdatedCompletenessCensus(ranged_root_pre=1)
    )
    terminal_fetch_result = resource_fetch_result(error="terminal")
    save_terminal = AsyncMock(return_value=terminal_fetch_result)
    monkeypatch.setattr(importer, "_saved_partition_terminal_result", save_terminal)
    invalid_count = await importer._record_partition_census_fetch(
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        partition_state,
        importer.LastUpdatedCensusRequest(
            field_name="unfiltered_pre",
            request_url="https://example.test/count",
            expected_count=None,
        ),
        importer.LastUpdatedCountFetch(observation=None, error="missing-total"),
    )
    assert invalid_count is terminal_fetch_result

    partition_state.plan.observe_count("root", importer.CountObservation.exact(2))
    root_mismatch = await importer._seed_partition_root_count(
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        partition_state,
    )
    assert root_mismatch is terminal_fetch_result
    partition_state.census.ranged_root_pre = 3
    leaf_mismatch = await importer._reconcile_partition_leaf_counts(
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        partition_state,
    )
    assert leaf_mismatch is terminal_fetch_result


@pytest.mark.asyncio
async def test_partition_next_count_handles_retry_and_invalid_response(monkeypatch):
    retry_fetch_result = resource_fetch_result(error="retry")
    monkeypatch.setattr(
        importer,
        "_saved_partition_retry_result",
        AsyncMock(return_value=retry_fetch_result),
    )
    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_count",
        AsyncMock(
            return_value=importer.LastUpdatedCountFetch(
                observation=None,
                transient=True,
                error="busy",
                retry_not_before="later",
            )
        ),
    )
    observed_count = await importer._observe_next_partition_count(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        last_updated_partition_state(),
        last_updated_partition_fetch_options(),
    )
    assert observed_count is retry_fetch_result

    monkeypatch.setattr(
        importer,
        "_fetch_last_updated_partition_count",
        AsyncMock(return_value=importer.LastUpdatedCountFetch(observation=None)),
    )
    with pytest.raises(RuntimeError, match="count_result_invalid"):
        await importer._observe_next_partition_count(
            {"source_id": "source-1"},
            "Practitioner",
            ProviderDirectoryPractitioner,
            last_updated_partition_config(),
            last_updated_partition_state(),
            last_updated_partition_fetch_options(),
        )


@pytest.mark.asyncio
async def test_partition_count_loop_stops_for_deadline_and_failure(monkeypatch):
    retry_fetch_result = resource_fetch_result(error="deadline")
    monkeypatch.setattr(
        importer,
        "_saved_partition_retry_result",
        AsyncMock(return_value=retry_fetch_result),
    )
    monkeypatch.setattr(importer, "_last_updated_partition_monotonic", Mock(return_value=10))
    deadline_state = last_updated_partition_state(deadline_at=5)
    deadline_outcome = await importer._observe_partition_counts(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        deadline_state,
        last_updated_partition_fetch_options(),
    )
    assert deadline_outcome is retry_fetch_result

    count_failure = resource_fetch_result(error="count-failure")
    monkeypatch.setattr(importer, "_last_updated_partition_monotonic", Mock(return_value=0))
    monkeypatch.setattr(
        importer,
        "_observe_next_partition_count",
        AsyncMock(return_value=count_failure),
    )
    assert await importer._observe_partition_counts(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        last_updated_partition_state(),
        last_updated_partition_fetch_options(),
    ) is count_failure


@pytest.mark.asyncio
async def test_partition_restore_and_candidate_proof_fail_closed(monkeypatch):
    partition_config = last_updated_partition_config()
    window = importer.TimeWindow(
        "root",
        partition_config.start,
        partition_config.end,
        count=2,
    )
    monkeypatch.setattr(
        importer,
        "_load_last_updated_partition_window_fingerprints",
        AsyncMock(return_value={"one": "hash"}),
    )
    with pytest.raises(RuntimeError, match="pass_one_missing"):
        await importer._restore_pass_one_proof(
            last_updated_partition_state(),
            "Practitioner",
            window,
        )

    terminal_fetch_result = resource_fetch_result(error="terminal")
    monkeypatch.setattr(
        importer,
        "_saved_partition_terminal_result",
        AsyncMock(return_value=terminal_fetch_result),
    )
    monkeypatch.setattr(
        importer,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(side_effect=RuntimeError("proof-corrupt")),
    )
    assert await importer._verified_partition_proof_counts(
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        last_updated_partition_state(),
    ) is terminal_fetch_result


@pytest.mark.asyncio
async def test_partition_candidate_proof_counts_must_match_census(monkeypatch):
    terminal_fetch_result = resource_fetch_result(error="terminal")
    monkeypatch.setattr(
        importer,
        "_saved_partition_terminal_result",
        AsyncMock(return_value=terminal_fetch_result),
    )
    proof_counts = importer.LastUpdatedPartitionProofCounts(1, 1, 1, 1, 0, 0)
    monkeypatch.setattr(
        importer,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(return_value=proof_counts),
    )
    mismatch_state = last_updated_partition_state(
        census=importer.LastUpdatedCompletenessCensus(ranged_root_pre=2)
    )
    assert await importer._verified_partition_proof_counts(
        "Practitioner",
        ProviderDirectoryPractitioner,
        last_updated_partition_config(),
        mismatch_state,
    ) is terminal_fetch_result


@pytest.mark.asyncio
async def test_partition_finish_rejects_incomplete_plan_and_proof(monkeypatch):
    partition_config = last_updated_partition_config()
    incomplete_outcome = await importer._finish_partition_fetch(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        last_updated_partition_state(),
        last_updated_partition_fetch_options(),
    )
    assert incomplete_outcome.error.endswith(":proof_incomplete")

    succeeded_plan = last_updated_partition_plan()
    succeeded_plan.observe_count("root", importer.CountObservation.exact(0))
    succeeded_plan.record_pass("root", 1, (), complete=True)
    succeeded_plan.record_pass("root", 2, (), complete=True)
    proof_failure = resource_fetch_result(error="proof-failure")
    monkeypatch.setattr(
        importer,
        "_verified_partition_proof_counts",
        AsyncMock(return_value=proof_failure),
    )
    assert await importer._finish_partition_fetch(
        {"source_id": "source-1"},
        "Practitioner",
        ProviderDirectoryPractitioner,
        partition_config,
        last_updated_partition_state(plan=succeeded_plan),
        last_updated_partition_fetch_options(),
    ) is proof_failure
