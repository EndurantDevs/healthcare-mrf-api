from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_pointers
from tests.ptg_v4_publish_control_support import TransactionDatabase


def _patch_pointer_transaction(monkeypatch, authoritative_snapshot):
    session = object()
    database = TransactionDatabase(session)
    monkeypatch.setattr(source_pointers, "resolve_ptg2_schema", lambda: "tenant")
    monkeypatch.setattr(source_pointers.db, "transaction", database.transaction)
    monkeypatch.setattr(
        source_pointers,
        "_acquire_source_pointer_gc_lock",
        AsyncMock(),
    )
    monkeypatch.setattr(source_pointers, "lock_writable_snapshot", AsyncMock())
    monkeypatch.setattr(
        source_pointers,
        "_locked_snapshot_publication_row",
        AsyncMock(return_value=authoritative_snapshot),
    )
    return session


def _source_pointer_arguments(**overrides):
    arguments_by_name = {
        "source_key": " Source ",
        "snapshot_id": " Snapshot ",
        "previous_snapshot_id": "previous",
        "import_month": dt.date(2026, 7, 1),
        "updated_at": dt.datetime(2026, 7, 24),
    }
    arguments_by_name.update(overrides)
    return arguments_by_name


@pytest.mark.asyncio
async def test_source_pointer_repoint_locks_and_preserves_current_state(
    monkeypatch,
) -> None:
    """Fence an already-published repoint and leave the global pointer untouched."""

    session = _patch_pointer_transaction(
        monkeypatch,
        {"status": source_pointers.PTG2_STATUS_PUBLISHED, "manifest": {}},
    )
    plan_entries = [{"plan_id": "plan", "plan_market_type": "group"}]
    monkeypatch.setattr(
        source_pointers,
        "_source_plan_rows",
        AsyncMock(return_value=plan_entries),
    )
    compare_and_swap = AsyncMock()
    publish_snapshot = AsyncMock()
    replace_plans = AsyncMock()
    monkeypatch.setattr(
        source_pointers,
        "_compare_and_swap_source_pointer",
        compare_and_swap,
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        publish_snapshot,
    )
    monkeypatch.setattr(
        source_pointers,
        "_replace_source_plan_pointers",
        replace_plans,
    )
    promotion_by_field = await source_pointers._publish_ptg2_source_pointers(
        **_source_pointer_arguments()
    )
    assert promotion_by_field["global_pointer"] == "not_requested"
    assert promotion_by_field["plan_source_count"] == 1
    source_pointers.lock_writable_snapshot.assert_awaited_once()
    compare_and_swap.assert_awaited_once()
    publish_snapshot.assert_awaited_once_with(
        session,
        schema_name="tenant",
        snapshot_attributes=None,
    )
    replace_plans.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_pointer_activation_and_shared_scope_fail_closed(
    monkeypatch,
) -> None:
    """Route audited candidates atomically and require scope proof for shared layouts."""

    _patch_pointer_transaction(
        monkeypatch,
        {"status": source_pointers.PTG2_STATUS_VALIDATED, "manifest": {}},
    )
    activation = AsyncMock(return_value={"status": "audited"})
    monkeypatch.setattr(
        source_pointers,
        "_activate_ptg2_source_candidate_in_transaction",
        activation,
    )
    assert await source_pointers._publish_ptg2_source_pointers(
        **_source_pointer_arguments(require_audit_attestation=True)
    ) == {"status": "audited"}
    activation.assert_awaited_once()

    _patch_pointer_transaction(
        monkeypatch,
        {"status": source_pointers.PTG2_STATUS_PUBLISHED, "manifest": {}},
    )
    monkeypatch.setattr(
        source_pointers,
        "bind_snapshot_to_shared_layout",
        AsyncMock(),
    )
    monkeypatch.setattr(
        source_pointers,
        "_source_plan_rows",
        AsyncMock(return_value=[]),
    )
    with pytest.raises(ValueError, match="coverage scope"):
        await source_pointers._publish_ptg2_source_pointers(
            **_source_pointer_arguments(shared_snapshot_key=7)
        )


@pytest.mark.asyncio
async def test_global_pointer_rejects_candidates_and_publishes_legacy_rows(
    monkeypatch,
) -> None:
    """Reject candidate bypass while retaining the legacy global promotion path."""

    with pytest.raises(ValueError, match="snapshot id"):
        await source_pointers._publish_ptg2_global_snapshot_pointer(
            snapshot_attributes={},
            updated_at=dt.datetime(2026, 7, 24),
        )
    _patch_pointer_transaction(
        monkeypatch,
        {
            "manifest": {
                "activation": {
                    "contract": source_pointers.PTG2_CANDIDATE_ACTIVATION_CONTRACT,
                }
            }
        },
    )
    with pytest.raises(ValueError, match="audited source-pointer"):
        await source_pointers._publish_ptg2_global_snapshot_pointer(
            snapshot_attributes={"snapshot_id": "candidate"},
            updated_at=dt.datetime(2026, 7, 24),
        )

    _patch_pointer_transaction(monkeypatch, {"manifest": {}})
    bind_layout = AsyncMock()
    publish_snapshot = AsyncMock()
    reconcile_global = AsyncMock()
    monkeypatch.setattr(
        source_pointers,
        "bind_snapshot_to_shared_layout",
        bind_layout,
    )
    monkeypatch.setattr(
        source_pointers,
        "_publish_snapshot_in_pointer_transaction",
        publish_snapshot,
    )
    monkeypatch.setattr(
        source_pointers,
        "_reconcile_global_snapshot_pointer",
        reconcile_global,
    )
    attributes_by_name = {"snapshot_id": "legacy", "status": "published"}
    promotion_by_field = await source_pointers._publish_ptg2_global_snapshot_pointer(
        snapshot_attributes=attributes_by_name,
        updated_at=dt.datetime(2026, 7, 24),
        shared_snapshot_key=7,
    )
    assert promotion_by_field == {
        "status": "promoted",
        "snapshot_id": "legacy",
        "global_pointer": "reconciled",
    }
    bind_layout.assert_awaited_once()
    publish_snapshot.assert_awaited_once()
    reconcile_global.assert_awaited_once()
