# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Worker-boundary tests for controlled address alias operations."""

from unittest.mock import AsyncMock, Mock

import pytest

from process import address_numeric_grid_alias_worker
from process.address_numeric_grid_alias_revoke import NumericGridAliasRevokeResult
from process.address_numeric_grid_alias_support import NumericGridAliasResult
from process.address_strict_source_backfill import StrictSourceBackfillResult


@pytest.mark.asyncio
async def test_control_worker_forwards_reviewed_alias_inputs_and_progress(monkeypatch):
    alias_result = NumericGridAliasResult(
        run_id="00000000-0000-0000-0000-000000000001",
        mode="shadow",
        status="sealed",
        candidate_digest="a" * 64,
        source_count=3,
        candidate_sources=2,
        candidate_rows=2,
        no_candidate=1,
        active_skipped=0,
        eligible=1,
        ambiguous=1,
        insufficient_provenance=0,
        promoted=0,
        generation=7,
        sample_rows=[],
        alias_kind="evidence_gated_address_match_v1",
    )
    run = AsyncMock(return_value=alias_result)
    cancel = AsyncMock()
    progress = Mock()
    monkeypatch.setattr(address_numeric_grid_alias_worker, "run_numeric_grid_alias", run)
    monkeypatch.setattr(address_numeric_grid_alias_worker, "raise_if_cancelled", cancel)
    monkeypatch.setattr(address_numeric_grid_alias_worker, "enqueue_live_progress", progress)

    worker_payload = await address_numeric_grid_alias_worker.process_data(
        {"worker": "synthetic"},
        {
            "mode": "shadow",
            "state_code": "UT",
            "zip_prefix": "84",
            "sample_limit": 5,
            "timeout": "30s",
            "alias_kind": "evidence_gated_address_match_v1",
        },
    )

    assert worker_payload == alias_result.__dict__
    assert run.await_args.kwargs["state_code"] == "UT"
    assert run.await_args.kwargs["zip_prefix"] == "84"
    assert run.await_args.kwargs["sample_limit"] == 5
    assert run.await_args.kwargs["timeout"] == "30s"
    assert run.await_args.kwargs["alias_kind"] == "evidence_gated_address_match_v1"
    await run.await_args.kwargs["cancel_check"]()
    cancel.assert_awaited_once_with(
        {"worker": "synthetic"},
        {
            "mode": "shadow",
            "state_code": "UT",
            "zip_prefix": "84",
            "sample_limit": 5,
            "timeout": "30s",
            "alias_kind": "evidence_gated_address_match_v1",
        },
    )
    assert progress.call_args.kwargs["run_id"] == alias_result.run_id
    assert progress.call_args.kwargs["status"] == "succeeded"
    assert progress.call_args.kwargs["source_count"] == 3


@pytest.mark.asyncio
async def test_control_worker_forwards_strict_source_backfill_inputs(monkeypatch):
    backfill_result = StrictSourceBackfillResult(
        run_id="00000000-0000-0000-0000-000000000002",
        status="backfilled",
        reviewed_shadow_run_id="00000000-0000-0000-0000-000000000001",
        reviewed_candidate_digest="b" * 64,
        evidence_digest="c" * 64,
        target_count=2,
        evidence_target_count=1,
        evidence_pair_count=2,
        updated_target_count=1,
        source_target_counts={"nppes": 1, "provider_directory_overlay": 1},
        missing_relations=[],
        generation=4,
    )
    run = AsyncMock(return_value=backfill_result)
    progress = Mock()
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "run_strict_source_backfill",
        run,
    )
    monkeypatch.setattr(address_numeric_grid_alias_worker, "enqueue_live_progress", progress)

    worker_payload = (
        await address_numeric_grid_alias_worker.process_address_strict_source_backfill(
            {},
            {
                "alias_run_id": backfill_result.reviewed_shadow_run_id,
                "expected_candidate_sha256": backfill_result.reviewed_candidate_digest,
                "reviewed_by": "synthetic-reviewer",
                "max_targets": 12,
                "timeout": "45s",
            },
        )
    )

    assert worker_payload == backfill_result.__dict__
    assert (
        run.await_args.kwargs["alias_run_id"]
        == backfill_result.reviewed_shadow_run_id
    )
    assert run.await_args.kwargs["max_targets"] == 12
    assert run.await_args.kwargs["timeout"] == "45s"
    assert "schema" not in run.await_args.kwargs
    assert progress.call_args.kwargs["source_target_counts"] == {
        "nppes": 1,
        "provider_directory_overlay": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("max_targets", (0, 10_001))
async def test_strict_backfill_worker_rejects_out_of_range_mutation_cap(
    monkeypatch,
    max_targets,
):
    run = AsyncMock()
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "run_strict_source_backfill",
        run,
    )

    with pytest.raises(ValueError, match="max_targets"):
        await address_numeric_grid_alias_worker.process_address_strict_source_backfill(
            {},
            {
                "alias_run_id": "00000000-0000-0000-0000-000000000001",
                "expected_candidate_sha256": "a" * 64,
                "reviewed_by": "synthetic-reviewer",
                "max_targets": max_targets,
            },
        )

    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_enqueued_strict_backfill_validates_cap_before_queueing(monkeypatch):
    create_pool = AsyncMock()
    monkeypatch.setattr(address_numeric_grid_alias_worker, "create_pool", create_pool)

    with pytest.raises(ValueError, match="max_targets"):
        await address_numeric_grid_alias_worker.run_address_strict_source_backfill_command(
            alias_run_id="00000000-0000-0000-0000-000000000001",
            expected_candidate_sha256="a" * 64,
            reviewed_by="synthetic-reviewer",
            max_targets=0,
            enqueue=True,
        )

    create_pool.assert_not_awaited()


def _mock_queue(monkeypatch):
    queue = AsyncMock()
    create_pool = AsyncMock(return_value=queue)
    monkeypatch.setattr(address_numeric_grid_alias_worker, "create_pool", create_pool)
    return create_pool, queue


@pytest.mark.asyncio
async def test_alias_command_runs_inline_and_enqueues(monkeypatch):
    process_alias = AsyncMock(return_value={"status": "sealed"})
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "process_address_numeric_grid_alias",
        process_alias,
    )
    create_pool, queue = _mock_queue(monkeypatch)

    inline_result = (
        await address_numeric_grid_alias_worker.run_address_numeric_grid_alias_command(
            mode="shadow",
            sample_limit=4,
            alias_kind="evidence_gated_address_match_v1",
        )
    )
    queued_result = (
        await address_numeric_grid_alias_worker.run_address_numeric_grid_alias_command(
            mode="apply",
            alias_kind="evidence_gated_address_match_v1",
            enqueue=True,
        )
    )

    assert inline_result == {"status": "sealed"}
    assert process_alias.await_args.args[1]["sample_limit"] == 4
    assert (
        process_alias.await_args.args[1]["alias_kind"]
        == "evidence_gated_address_match_v1"
    )
    assert queued_result is None
    create_pool.assert_awaited_once()
    assert queue.enqueue_job.await_args.args[0] == "process_address_numeric_grid_alias"
    assert (
        queue.enqueue_job.await_args.args[1]["alias_kind"]
        == "evidence_gated_address_match_v1"
    )


@pytest.mark.asyncio
async def test_backfill_command_runs_inline_and_enqueues(monkeypatch):
    process_backfill = AsyncMock(return_value={"status": "backfilled"})
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "process_address_strict_source_backfill",
        process_backfill,
    )
    create_pool, queue = _mock_queue(monkeypatch)
    command_options_by_name = {
        "alias_run_id": "00000000-0000-0000-0000-000000000001",
        "expected_candidate_sha256": "a" * 64,
        "reviewed_by": "synthetic-reviewer",
        "max_targets": 2,
    }

    inline_result = (
        await address_numeric_grid_alias_worker.run_address_strict_source_backfill_command(
            **command_options_by_name
        )
    )
    queued_result = (
        await address_numeric_grid_alias_worker.run_address_strict_source_backfill_command(
            **command_options_by_name,
            enqueue=True,
        )
    )

    assert inline_result == {"status": "backfilled"}
    assert process_backfill.await_args.args[1]["max_targets"] == 2
    assert queued_result is None
    create_pool.assert_awaited_once()
    assert queue.enqueue_job.await_args.args[0] == "process_address_strict_source_backfill"


@pytest.mark.asyncio
async def test_backfill_worker_uses_default_mutation_cap(monkeypatch):
    backfill_result = StrictSourceBackfillResult(
        run_id="00000000-0000-0000-0000-000000000002",
        status="backfilled",
        reviewed_shadow_run_id="00000000-0000-0000-0000-000000000001",
        reviewed_candidate_digest="b" * 64,
        evidence_digest="c" * 64,
        target_count=0,
        evidence_target_count=0,
        evidence_pair_count=0,
        updated_target_count=0,
        source_target_counts={},
        missing_relations=[],
        generation=4,
    )
    run_backfill = AsyncMock(return_value=backfill_result)
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "run_strict_source_backfill",
        run_backfill,
    )
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "enqueue_live_progress",
        Mock(),
    )

    await address_numeric_grid_alias_worker.process_address_strict_source_backfill(
        {},
        None,
    )

    assert run_backfill.await_args.kwargs["max_targets"] == 256


@pytest.mark.asyncio
async def test_revoke_worker_and_command_paths(monkeypatch):
    """Evidence alias kind survives inline, queued, and revoke adapters."""
    revoke_result = NumericGridAliasRevokeResult(
        run_id="00000000-0000-0000-0000-000000000003",
        status="revoked",
        source_address_key="00000000-0000-0000-0000-000000000001",
        target_address_key="00000000-0000-0000-0000-000000000002",
        revoked_reason="synthetic rollback",
        revoked_by="synthetic-reviewer",
        generation=5,
        alias_kind="evidence_gated_address_match_v1",
    )
    revoke_alias = AsyncMock(return_value=revoke_result)
    progress = Mock()
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "revoke_numeric_grid_alias",
        revoke_alias,
    )
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "enqueue_live_progress",
        progress,
    )
    create_pool, queue = _mock_queue(monkeypatch)
    command_options_by_name = {
        "source_address_key": revoke_result.source_address_key,
        "expected_target_address_key": revoke_result.target_address_key,
        "reason": revoke_result.revoked_reason,
        "reviewed_by": revoke_result.revoked_by,
        "alias_kind": "evidence_gated_address_match_v1",
    }

    worker_payload = await _run_revoke_worker(revoke_result, progress)
    assert revoke_alias.await_args.kwargs["alias_kind"] == (
        "evidence_gated_address_match_v1"
    )
    process_revoke = AsyncMock(return_value=revoke_result.__dict__)
    monkeypatch.setattr(
        address_numeric_grid_alias_worker,
        "process_address_numeric_grid_alias_revoke",
        process_revoke,
    )
    inline_result = await address_numeric_grid_alias_worker.run_address_alias_revoke_command(
        **command_options_by_name
    )
    queued_result = await address_numeric_grid_alias_worker.run_address_alias_revoke_command(
        **command_options_by_name,
        enqueue=True,
    )

    assert worker_payload == revoke_result.__dict__
    assert inline_result == revoke_result.__dict__
    assert queued_result is None
    create_pool.assert_awaited_once()
    assert queue.enqueue_job.await_args.args[0] == "process_address_numeric_grid_alias_revoke"
    assert queue.enqueue_job.await_args.args[1]["alias_kind"] == (
        "evidence_gated_address_match_v1"
    )


async def _run_revoke_worker(revoke_result, progress):
    worker_payload = (
        await address_numeric_grid_alias_worker.process_address_numeric_grid_alias_revoke(
            {"worker": "synthetic"},
            {
                "source_address_key": revoke_result.source_address_key,
                "expected_target_address_key": revoke_result.target_address_key,
                "reason": revoke_result.revoked_reason,
                "reviewed_by": revoke_result.revoked_by,
                "alias_kind": "evidence_gated_address_match_v1",
            },
        )
    )
    assert progress.call_args.kwargs["generation"] == 5
    return worker_payload
