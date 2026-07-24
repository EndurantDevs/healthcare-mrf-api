# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Edge-contract coverage for targeted source snapshot control."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import source_snapshot_control


class _ExecutionResult:
    def __init__(
        self,
        rows: list[dict[str, object]] | None = None,
        *,
        rowcount: int | None = None,
    ) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def all(self) -> list[dict[str, object]]:
        return self._rows


class _Session:
    def __init__(self, *results: _ExecutionResult) -> None:
        self._results = list(results)
        self.calls: list[tuple[object, dict[str, object]]] = []

    async def execute(
        self,
        statement: object,
        params: dict[str, object],
    ) -> _ExecutionResult:
        self.calls.append((statement, params))
        if self._results:
            return self._results.pop(0)
        return _ExecutionResult()


class _Transaction:
    def __init__(self) -> None:
        self.session = _Session()

    async def __aenter__(self) -> _Session:
        return self.session

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> bool:
        return False


def _terminal_snapshot() -> dict[str, object]:
    return {
        "snapshot_id": "snapshot-a",
        "status": "failed",
        "manifest": {
            "serving_index": {
                "source_key": "source-a",
                "arch_version": "postgres_binary_v3",
                "storage_generation": "shared_blocks_v4",
            }
        },
    }


def _empty_references() -> dict[str, list[str]]:
    return {
        "global_slots": [],
        "source_keys": [],
        "plan_source_keys": [],
        "previous_global_slots": [],
        "previous_source_keys": [],
        "previous_plan_source_keys": [],
        "plan_release_pins": [],
    }


@pytest.mark.asyncio
async def test_transaction_executor_returns_rows_and_row_counts() -> None:
    session = _Session(
        _ExecutionResult([{"snapshot_id": "snapshot-a"}]),
        _ExecutionResult(rowcount=3),
        _ExecutionResult(rowcount=None),
    )
    executor = source_snapshot_control._TransactionExecutor(session)

    assert await executor.all(
        "SELECT snapshot_id",
        source_key="source-a",
    ) == [{"snapshot_id": "snapshot-a"}]
    assert await executor.status(
        "DELETE FROM snapshot",
        snapshot_id="snapshot-a",
    ) == 3
    assert await executor.status(object()) is None


def test_row_mapping_handles_missing_and_object_backed_rows() -> None:
    assert source_snapshot_control._row_mapping(None) == {}
    assert source_snapshot_control._row_mapping(
        SimpleNamespace(_mapping={"snapshot_id": "snapshot-a"})
    ) == {"snapshot_id": "snapshot-a"}


@pytest.mark.asyncio
async def test_snapshot_control_requires_nonempty_identifiers() -> None:
    with pytest.raises(
        ValueError,
        match="source_key and snapshot_id are required",
    ):
        await source_snapshot_control.promote_ptg2_source_snapshot(
            source_key=" ",
            snapshot_id="snapshot-a",
        )

    with pytest.raises(ValueError, match="snapshot_id is required"):
        await source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id=" ",
        )

    with pytest.raises(ValueError, match="snapshot_id is required"):
        await source_snapshot_control.retire_ptg2_source_snapshot(
            snapshot_id=" ",
        )


@pytest.mark.asyncio
async def test_remove_plan_treats_missing_snapshot_as_idempotently_removable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value={}),
    )

    removal_plan = (
        await source_snapshot_control.build_ptg2_source_snapshot_remove_plan(
            snapshot_id="snapshot-missing",
            source_key="source-a",
        )
    )

    assert removal_plan == {
        "snapshot_id": "snapshot-missing",
        "source_key": "source-a",
        "exists": False,
        "removable": True,
        "metadata_only": True,
        "tables": [],
        "artifact_manifest_ids": [],
        "current_references": {},
    }


@pytest.mark.asyncio
async def test_remove_rejects_a_plan_that_is_not_removable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _Transaction()
    monkeypatch.setattr(
        source_snapshot_control.db,
        "transaction",
        lambda: transaction,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "build_ptg2_source_snapshot_remove_plan",
        AsyncMock(
            return_value={
                "exists": True,
                "removable": False,
                "reason": "snapshot retains a serving pointer",
            }
        ),
    )

    with pytest.raises(
        ValueError,
        match="snapshot retains a serving pointer",
    ):
        await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="snapshot-a",
        )

    assert len(transaction.session.calls) == 1


@pytest.mark.asyncio
async def test_remove_missing_snapshot_is_an_executed_noop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _Transaction()
    monkeypatch.setattr(
        source_snapshot_control.db,
        "transaction",
        lambda: transaction,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "build_ptg2_source_snapshot_remove_plan",
        AsyncMock(
            return_value={
                "snapshot_id": "snapshot-missing",
                "source_key": "source-a",
                "exists": False,
                "removable": True,
            }
        ),
    )

    cleanup_summary = (
        await source_snapshot_control.remove_ptg2_source_snapshot(
            snapshot_id="snapshot-missing",
            source_key="source-a",
        )
    )

    assert cleanup_summary["executed"] is True
    assert cleanup_summary["deleted_snapshots"] == 0
    assert cleanup_summary["released_shared_layouts"] == 0
    assert cleanup_summary["physical_cleanup"] == "not_applicable"


@pytest.mark.asyncio
async def test_retire_rejects_previous_pointer_before_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _Transaction()
    previous_references_by_field = {
        **_empty_references(),
        "previous_source_keys": ["source-a"],
    }
    monkeypatch.setattr(
        source_snapshot_control.db,
        "transaction",
        lambda: transaction,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value=_terminal_snapshot()),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        AsyncMock(return_value=previous_references_by_field),
    )

    with pytest.raises(
        ValueError,
        match="referenced by a previous snapshot pointer",
    ):
        await source_snapshot_control.retire_ptg2_source_snapshot(
            snapshot_id="snapshot-a",
            source_key="source-a",
        )

    assert len(transaction.session.calls) == 1


@pytest.mark.asyncio
async def test_retire_without_source_filter_uses_manifest_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transaction = _Transaction()
    references = AsyncMock(
        side_effect=[
            _empty_references(),
            _empty_references(),
        ]
    )
    delete_status = AsyncMock(side_effect=[2, 1])
    clear_cache = Mock()
    monkeypatch.setattr(
        source_snapshot_control.db,
        "transaction",
        lambda: transaction,
    )
    monkeypatch.setattr(
        source_snapshot_control.db,
        "status",
        delete_status,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_snapshot_row",
        AsyncMock(return_value=_terminal_snapshot()),
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_current_references",
        references,
    )
    monkeypatch.setattr(
        source_snapshot_control,
        "_clear_ptg2_snapshot_cache",
        clear_cache,
    )

    retirement_summary = (
        await source_snapshot_control.retire_ptg2_source_snapshot(
            snapshot_id="snapshot-a",
        )
    )

    assert retirement_summary["source_key"] == "source-a"
    assert retirement_summary["deleted_plan_pointers"] == 2
    assert retirement_summary["deleted_source_pointers"] == 1
    assert all(
        status_call.kwargs == {"snapshot_id": "snapshot-a"}
        for status_call in delete_status.await_args_list
    )
    assert all(
        "source_key = :source_key" not in status_call.args[0]
        for status_call in delete_status.await_args_list
    )
    clear_cache.assert_called_once_with()


@pytest.mark.asyncio
async def test_current_source_state_handles_absent_and_nullable_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = AsyncMock(
        side_effect=[
            None,
            ("snapshot-a", None),
        ]
    )
    monkeypatch.setattr(source_snapshot_control.db, "first", first)

    assert await source_snapshot_control._current_source_snapshot_state(
        "mrf",
        "source-a",
    ) == (None, None)
    assert await source_snapshot_control._current_source_snapshot_state(
        "mrf",
        "source-a",
    ) == ("snapshot-a", None)


def test_clear_snapshot_cache_releases_cached_resolutions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from api import ptg2_snapshot

    snapshot_resolve_cache_by_key = {
        "test:edge-contract": ("snapshot-a", None),
    }
    monkeypatch.setattr(
        ptg2_snapshot,
        "_PTG2_SNAPSHOT_RESOLVE_CACHE",
        snapshot_resolve_cache_by_key,
        raising=False,
    )
    source_snapshot_control._clear_ptg2_snapshot_cache()

    assert snapshot_resolve_cache_by_key == {}
