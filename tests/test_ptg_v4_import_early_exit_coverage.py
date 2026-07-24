# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Changed-code coverage for early PTG import orchestration exits."""

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

from tests.ptg_v4_import_orchestration_support import (
    RecordingSourceLock,
    import_arguments,
    install_import_boundary,
)


process_ptg = importlib.import_module("process.ptg")


@pytest.mark.asyncio
async def test_provider_reference_url_is_rejected_before_database_setup():
    """Reject the obsolete provider reference lane before acquiring resources."""

    with pytest.raises(ValueError, match="provider_ref_url is not supported"):
        await process_ptg._main_with_artifact_lease(
            **import_arguments(
                provider_ref_url="https://example.test/provider-reference.json"
            )
        )


@pytest.mark.asyncio
async def test_production_import_requires_an_explicit_source_key(monkeypatch):
    """Fail production orchestration when no durable source identity exists."""

    monkeypatch.delenv("HLTHPRT_PTG2_SOURCE_KEY", raising=False)

    with pytest.raises(ValueError, match="require --source-key"):
        await process_ptg._main_with_artifact_lease(
            import_id="orchestration-test",
            import_month="2026-07",
        )


@pytest.mark.asyncio
async def test_test_import_derives_source_key_and_ignores_invalid_byte_cap(
    monkeypatch,
    caplog,
):
    """Use the test import identity and default byte cap after invalid input."""

    boundary = install_import_boundary(monkeypatch, process_ptg)
    process_toc = AsyncMock(return_value=[{"type": "unsupported", "url": "x"}])
    monkeypatch.delenv("HLTHPRT_PTG2_SOURCE_KEY", raising=False)
    monkeypatch.setenv("HLTHPRT_PTG2_TEST_MAX_BYTES", "not-an-integer")
    monkeypatch.setattr(process_ptg, "fetch_max_bytes", lambda _default: 4321)
    monkeypatch.setattr(process_ptg, "_process_table_of_contents", process_toc)

    with pytest.raises(RuntimeError, match="no supported PTG files"):
        await process_ptg._main_with_artifact_lease(
            test_mode=True,
            import_id="derived-source",
            import_month="2026-07",
            toc_urls=["https://example.test/toc.json"],
        )

    snapshot_row = boundary.push.await_args_list[0].args[0][0]
    assert snapshot_row["status"] == process_ptg.PTG2_STATUS_BUILDING
    assert process_toc.await_args.kwargs["max_bytes"] == 4321
    assert "Ignoring invalid HLTHPRT_PTG2_TEST_MAX_BYTES" in caplog.text


@pytest.mark.asyncio
async def test_source_pointer_failure_releases_an_entered_lock(monkeypatch):
    """Release the source lock when initial pointer inspection fails."""

    boundary = install_import_boundary(monkeypatch, process_ptg)
    monkeypatch.setattr(
        process_ptg,
        "_current_source_snapshot_id",
        AsyncMock(side_effect=RuntimeError("pointer unavailable")),
    )

    with pytest.raises(RuntimeError, match="pointer unavailable"):
        await process_ptg._main_with_artifact_lease(**import_arguments())

    assert boundary.lock.enter_count == 1
    assert boundary.lock.exit_count == 1
    boundary.push.assert_not_awaited()
    assert boundary.reset_tokens == ["live-token"]


@pytest.mark.asyncio
async def test_source_lock_entry_failure_resets_progress_without_exit(monkeypatch):
    """Reset live context when lock acquisition itself never completes."""

    lock = RecordingSourceLock(enter_error=RuntimeError("lock unavailable"))
    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        source_lock=lock,
    )

    with pytest.raises(RuntimeError, match="lock unavailable"):
        await process_ptg._main_with_artifact_lease(**import_arguments())

    assert lock.enter_count == 1
    assert lock.exit_count == 0
    assert boundary.reset_tokens == ["live-token"]


@pytest.mark.asyncio
async def test_snapshot_claim_failure_releases_lock_and_progress(monkeypatch):
    """Clean both lifecycle resources when the building claim cannot persist."""

    boundary = install_import_boundary(monkeypatch, process_ptg)
    boundary.push.side_effect = RuntimeError("claim failed")

    with pytest.raises(RuntimeError, match="claim failed"):
        await process_ptg._main_with_artifact_lease(**import_arguments())

    assert boundary.lock.exit_count == 1
    assert boundary.reset_tokens == ["live-token"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "terminal_status",
    [process_ptg.PTG2_STATUS_PUBLISHED, process_ptg.PTG2_STATUS_VALIDATED],
)
async def test_full_rebuild_refuses_a_completed_snapshot(
    monkeypatch,
    terminal_status,
):
    """Require a fresh attempt for either completed snapshot state."""

    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        snapshot_state={
            "snapshot_id": "snapshot-one",
            "import_run_id": "ptg2:orchestration-test",
            "status": terminal_status,
        },
    )

    with pytest.raises(
        process_ptg.PTG2FullRebuildFreshnessError,
        match="already completed",
    ) as raised:
        await process_ptg._main_with_artifact_lease(
            **import_arguments(full_rebuild_scope_digest="a" * 64)
        )

    assert raised.value.metrics_by_name["existing_snapshot_reused"] is True
    assert boundary.lock.exit_count == 1
    assert boundary.reset_tokens == ["live-token"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("existing_status", "message"),
    [
        (process_ptg.PTG2_STATUS_BUILDING, "already being built"),
        (process_ptg.PTG2_STATUS_FAILED, "existing status is failed"),
        (None, "existing status is <unknown>"),
    ],
)
async def test_nonmatching_existing_claims_fail_closed(
    monkeypatch,
    existing_status,
    message,
):
    """Different attempts cannot reuse an existing snapshot claim."""

    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        snapshot_state={
            "snapshot_id": "snapshot-one",
            "import_run_id": "ptg2:other-run",
            "status": existing_status,
            "snapshot_claim_status": "existing",
        },
    )

    with pytest.raises(RuntimeError, match=message):
        await process_ptg._main_with_artifact_lease(**import_arguments())

    assert boundary.lock.exit_count == 1
    assert boundary.reset_tokens == ["live-token"]


@pytest.mark.asyncio
async def test_published_retry_reconciles_pointer_and_finalizes_attempt(monkeypatch):
    """Return the already-published result after repairing terminal metadata."""

    snapshot_by_field = {
        "snapshot_id": "snapshot-one",
        "import_run_id": "ptg2:orchestration-test",
        "status": process_ptg.PTG2_STATUS_PUBLISHED,
        "manifest": {"serving_rates": 3},
    }
    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        snapshot_state=snapshot_by_field,
    )
    reconcile = AsyncMock(return_value={"status": "unchanged"})
    finalize = AsyncMock()
    monkeypatch.setattr(process_ptg, "_reconcile_already_published_snapshot", reconcile)
    monkeypatch.setattr(
        process_ptg,
        "_already_published_result",
        lambda **_fields: {"status": "succeeded", "message": "already published"},
    )
    monkeypatch.setattr(process_ptg, "_finalize_resumed_terminal_attempt", finalize)

    import_response = await process_ptg._main_with_artifact_lease(**import_arguments())

    assert import_response == {"status": "succeeded", "message": "already published"}
    reconcile.assert_awaited_once()
    finalize.assert_awaited_once_with(
        snapshot_by_field,
        internal_run_id="ptg2:orchestration_test",
    )
    assert boundary.lock.exit_count == 1


@pytest.mark.asyncio
async def test_validated_retry_can_leave_activation_deferred(monkeypatch):
    """Resume a candidate without touching live pointers when activation is off."""

    snapshot_by_field = {
        "snapshot_id": "snapshot-one",
        "import_run_id": "ptg2:orchestration-test",
        "status": process_ptg.PTG2_STATUS_VALIDATED,
    }
    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        snapshot_state=snapshot_by_field,
    )
    resume = AsyncMock(return_value={"activation_status": "deferred"})
    cleanup = AsyncMock()
    refresh = AsyncMock()
    monkeypatch.setattr(process_ptg, "_should_auto_activate_ptg2_candidates", lambda: False)
    monkeypatch.setattr(process_ptg, "_resume_validated_candidate", resume)
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", cleanup)
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        refresh,
    )
    monkeypatch.setattr(process_ptg, "_finalize_resumed_terminal_attempt", AsyncMock())

    candidate_response = await process_ptg._main_with_artifact_lease(
        **import_arguments()
    )

    assert candidate_response == {"activation_status": "deferred"}
    assert resume.await_args.kwargs["auto_activate"] is False
    cleanup.assert_not_awaited()
    refresh.assert_not_awaited()
    assert boundary.progress_events[-1]["message"].startswith("PTG candidate already")


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_validated_retry_activation_survives_old_state_cleanup(
    monkeypatch,
    cleanup_fails,
):
    """Keep successful activation even if post-cutover cleanup is unavailable."""

    boundary = install_import_boundary(
        monkeypatch,
        process_ptg,
        snapshot_state={
            "snapshot_id": "snapshot-one",
            "import_run_id": "ptg2:orchestration-test",
            "status": process_ptg.PTG2_STATUS_VALIDATED,
        },
    )
    cleanup = AsyncMock(
        side_effect=RuntimeError("cleanup unavailable") if cleanup_fails else None
    )
    refresh = AsyncMock(return_value={"status": "queued"})
    monkeypatch.setattr(process_ptg, "_should_auto_activate_ptg2_candidates", lambda: True)
    monkeypatch.setattr(
        process_ptg,
        "_resume_validated_candidate",
        AsyncMock(return_value={"activation_status": "activated"}),
    )
    monkeypatch.setattr(process_ptg, "_cleanup_old_ptg2_source_tables", cleanup)
    monkeypatch.setattr(
        process_ptg,
        "_enqueue_ptg2_auto_address_refresh_after_import",
        refresh,
    )
    monkeypatch.setattr(process_ptg, "_finalize_resumed_terminal_attempt", AsyncMock())

    candidate_response = await process_ptg._main_with_artifact_lease(
        **import_arguments(test_mode=True)
    )

    assert candidate_response["activation_status"] == "activated"
    assert candidate_response["address_refresh"] == {"status": "queued"}
    cleanup.assert_awaited_once()
    refresh.assert_awaited_once()
    assert boundary.progress_events[-1]["message"] == "PTG candidate activated"


@pytest.mark.asyncio
async def test_table_of_contents_failure_records_terminal_failure(monkeypatch):
    """Reject partial coverage and persist the failed table-of-contents URL."""

    boundary = install_import_boundary(monkeypatch, process_ptg)
    monkeypatch.setattr(
        process_ptg,
        "_process_table_of_contents",
        AsyncMock(side_effect=RuntimeError("catalog unavailable")),
    )

    with pytest.raises(RuntimeError, match="strict V3 never publishes partial"):
        await process_ptg._main_with_artifact_lease(
            **import_arguments(toc_urls=["https://example.test/toc.json"])
        )

    persisted = process_ptg._mark_ptg2_import_failed.await_args.kwargs["report"]
    assert persisted["toc_failures"] == [
        {
            "url": "https://example.test/toc.json",
            "error": "catalog unavailable",
        }
    ]
    assert boundary.lock.exit_count == 1


@pytest.mark.asyncio
async def test_empty_table_of_contents_is_not_a_successful_import(monkeypatch):
    """Reject a processed catalog that discovers zero rate files."""

    install_import_boundary(monkeypatch, process_ptg)
    monkeypatch.setattr(
        process_ptg,
        "_process_table_of_contents",
        AsyncMock(return_value=[]),
    )

    with pytest.raises(RuntimeError, match="discovered zero rate files"):
        await process_ptg._main_with_artifact_lease(
            **import_arguments(toc_urls=["https://example.test/toc.json"])
        )

    persisted = process_ptg._mark_ptg2_import_failed.await_args.kwargs["report"]
    assert persisted["jobs_discovered"] == 0
    assert persisted["files_processed"] == 0


@pytest.mark.asyncio
async def test_import_without_any_supported_input_is_rejected(monkeypatch):
    """Reject an empty direct invocation instead of publishing empty coverage."""

    boundary = install_import_boundary(monkeypatch, process_ptg)

    with pytest.raises(RuntimeError, match="no supported PTG files"):
        await process_ptg._main_with_artifact_lease(**import_arguments())

    process_ptg._mark_ptg2_import_failed.assert_awaited_once()
    assert boundary.lock.exit_count == 1
    assert boundary.reset_tokens[-1] == "live-token"
