"""Focused assertions for source-snapshot removal transactions."""

from process.ptg_parts import ptg2_lifecycle_lock


def assert_source_snapshot_remove_results(
    cleanup_summary,
    transaction_statements,
    status_calls,
) -> None:
    """Verify source removal preserves the expected publication state."""
    _assert_source_removal_summary(cleanup_summary)
    _assert_source_removal_fence(transaction_statements)
    _assert_source_removal_effects(status_calls)


def _assert_source_removal_summary(cleanup_summary) -> None:
    assert cleanup_summary["executed"] is True
    assert cleanup_summary["deleted_tables"] == 0
    assert cleanup_summary["deleted_v3_snapshot_scopes"] == 0
    assert cleanup_summary["deleted_v3_snapshot_bindings"] == 0
    assert cleanup_summary["deleted_artifact_chunks"] == 1
    assert cleanup_summary["deleted_artifact_manifests"] == 1
    assert cleanup_summary["deleted_snapshots"] == 1
    assert cleanup_summary["released_shared_layouts"] == 0
    assert cleanup_summary["queued_shared_block_candidates"] == 0
    assert cleanup_summary["queued_shared_block_bytes"] == 0
    assert cleanup_summary["layout_cleanup"] == "not_applicable"
    assert cleanup_summary["physical_cleanup"] == "not_applicable"


def _assert_source_removal_fence(transaction_statements) -> None:
    assert transaction_statements == [
        (
            "SELECT set_config('lock_timeout', :lock_timeout, true), "
            "set_config('statement_timeout', :statement_timeout, true)",
            {
                "lock_timeout": ptg2_lifecycle_lock.PTG2_LIFECYCLE_LOCK_TIMEOUT,
                "statement_timeout": (
                    ptg2_lifecycle_lock.PTG2_LIFECYCLE_STATEMENT_TIMEOUT
                ),
            },
        ),
        (
            "SELECT pg_advisory_xact_lock(hashtext(:gc_lock_key))",
            {"gc_lock_key": ptg2_lifecycle_lock.PTG2_SOURCE_POINTER_GC_LOCK_KEY},
        ),
    ]


def _assert_source_removal_effects(status_calls) -> None:
    assert any("ptg2_artifact_blob_chunk" in call[0] for call in status_calls)
    assert any(
        "ptg2_artifact_manifest" in call[0]
        and call[1]["snapshot_id"] == "snap_old"
        for call in status_calls
    )
    assert any(
        "ptg2_snapshot" in call[0]
        and call[1]["snapshot_id"] == "snap_old"
        for call in status_calls
    )
    scope_index = next(
        index for index, call in enumerate(status_calls)
        if "ptg2_v3_snapshot_scope" in call[0]
    )
    binding_index = next(
        index for index, call in enumerate(status_calls)
        if "ptg2_v3_snapshot_binding" in call[0]
    )
    snapshot_index = next(
        index for index, call in enumerate(status_calls)
        if 'DELETE FROM "mrf".ptg2_snapshot WHERE' in call[0]
    )
    assert scope_index < binding_index < snapshot_index
