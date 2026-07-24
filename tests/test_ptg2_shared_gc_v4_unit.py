# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_shared_gc as shared_gc


class _Executor:
    def __init__(self, *rows):
        self.rows = list(rows)
        self.all_calls: list[tuple[str, dict[str, object]]] = []
        self.status_calls: list[tuple[str, dict[str, object]]] = []

    async def all(self, statement: str, **params):
        self.all_calls.append((statement, params))
        if not self.rows:
            raise AssertionError("unexpected query")
        return self.rows.pop(0)

    async def status(self, statement: str, **params):
        self.status_calls.append((statement, params))
        return "SELECT 1"


@pytest.mark.asyncio
async def test_additional_v4_layout_stats_skip_empty_hashes():
    empty_executor = _Executor()
    assert await shared_gc._additional_v4_layout_stats(
        empty_executor,
        schema_name="mrf",
        snapshot_keys=(1,),
        v4_hashes=(),
    ) == shared_gc.PTG2SharedLayoutGCStats()
    assert empty_executor.all_calls == []


@pytest.mark.asyncio
async def test_additional_v4_layout_stats_are_deduplicated():
    first_hash = b"a" * 32
    second_hash = b"b" * 32
    executor = _Executor(
        [
            {
                "requested_count": 2,
                "resolved_count": 2,
                "additional_count": 1,
                "additional_stored_bytes": 123,
            }
        ]
    )
    layout_stats = await shared_gc._additional_v4_layout_stats(
        executor,
        schema_name="mrf",
        snapshot_keys=("2", 1),
        v4_hashes=(second_hash, first_hash, second_hash),
    )
    assert layout_stats == shared_gc.PTG2SharedLayoutGCStats(
        candidate_hash_count=1,
        stored_bytes=123,
    )
    _statement, params = executor.all_calls[0]
    assert params == {
        "block_hashes": [first_hash, second_hash],
        "snapshot_keys": [2, 1],
    }


@pytest.mark.asyncio
async def test_additional_v4_layout_stats_reject_missing_blocks():
    first_hash = b"a" * 32
    second_hash = b"b" * 32
    missing_executor = _Executor(
        [
            {
                "requested_count": 2,
                "resolved_count": 1,
                "additional_count": 1,
                "additional_stored_bytes": 123,
            }
        ]
    )
    with pytest.raises(RuntimeError, match="missing CAS block"):
        await shared_gc._additional_v4_layout_stats(
            missing_executor,
            schema_name="mrf",
            snapshot_keys=(1,),
            v4_hashes=(first_hash, second_hash),
        )


@pytest.mark.asyncio
async def test_additional_v4_layout_stats_accept_empty_query_result():
    first_hash = b"a" * 32
    no_rows_executor = _Executor([])
    assert await shared_gc._additional_v4_layout_stats(
        no_rows_executor,
        schema_name="mrf",
        snapshot_keys=(1,),
        v4_hashes=(first_hash,),
    ) == shared_gc.PTG2SharedLayoutGCStats()


@pytest.mark.asyncio
async def test_owned_v4_fingerprint_validation():
    assert await shared_gc._owned_v4_layout_fingerprint(
        _Executor([]),
        schema_name="mrf",
        snapshot_key=1,
    ) is None
    with pytest.raises(RuntimeError, match="multiple fingerprints"):
        await shared_gc._owned_v4_layout_fingerprint(
            _Executor(
                [
                    {"semantic_fingerprint": b"a" * 32},
                    {"semantic_fingerprint": b"b" * 32},
                ]
            ),
            schema_name="mrf",
            snapshot_key=1,
        )
    with pytest.raises(RuntimeError, match="fingerprint is invalid"):
        await shared_gc._owned_v4_layout_fingerprint(
            _Executor([{"semantic_fingerprint": b"short"}]),
            schema_name="mrf",
            snapshot_key=1,
        )
    assert await shared_gc._owned_v4_layout_fingerprint(
        _Executor([{"semantic_fingerprint": b"a" * 32}]),
        schema_name="mrf",
        snapshot_key=1,
    ) == b"a" * 32


@pytest.mark.asyncio
async def test_owned_v4_layout_lock_fences(monkeypatch):
    fingerprint = AsyncMock(return_value=None)
    monkeypatch.setattr(shared_gc, "_owned_v4_layout_fingerprint", fingerprint)
    assert not await shared_gc._is_owned_v4_layout_locked(
        _Executor(),
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
    )

    fingerprint.return_value = b"a" * 32
    assert not await shared_gc._is_owned_v4_layout_locked(
        _Executor([]),
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
    )
    for owner, message in (
        ({"is_bound": True, "root_state": "building"}, "bound"),
        ({"is_bound": False, "root_state": "sealed"}, "completed"),
    ):
        with pytest.raises(RuntimeError, match=message):
            await shared_gc._is_owned_v4_layout_locked(
                _Executor([owner]),
                schema_name="mrf",
                snapshot_key=1,
                build_token="token",
            )
    locked_executor = _Executor([{"is_bound": False, "root_state": None}])
    assert await shared_gc._is_owned_v4_layout_locked(
        locked_executor,
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
    )
    assert "pg_advisory_xact_lock" in locked_executor.status_calls[0][0]


@pytest.mark.asyncio
async def test_shared_gc_cli_dry_run_and_execute(monkeypatch, capsys):
    plan = shared_gc.PTG2SharedGCPlan(
        layouts=shared_gc.PTG2SharedLayoutGCStats(1, 2, 3),
        sweep=shared_gc.PTG2SharedBlockSweepPlan((b"a" * 32,), 4),
    )
    build = AsyncMock(return_value=plan)
    release = AsyncMock(return_value=plan.layouts)
    sweep = AsyncMock(return_value=plan.sweep)
    monkeypatch.setattr(shared_gc, "build_ptg2_shared_gc_plan", build)
    monkeypatch.setattr(
        shared_gc,
        "release_unbound_ptg2_shared_layouts",
        release,
    )
    monkeypatch.setattr(shared_gc, "sweep_ptg2_shared_blocks", sweep)

    await shared_gc._amain(
        ("--schema", "testing", "--max-layouts", "1", "--max-rows", "2")
    )
    assert "cleanup_executed=false" in capsys.readouterr().out
    build.assert_awaited_once_with(
        schema_name="testing",
        max_layouts=1,
        max_rows=2,
        max_bytes=None,
    )
    release.assert_not_awaited()

    await shared_gc._amain(
        (
            "--schema",
            "testing",
            "--execute",
            "--max-layouts",
            "3",
            "--max-rows",
            "4",
            "--max-bytes",
            "5",
        )
    )
    output = capsys.readouterr().out
    assert "selected_hash=" in output
    assert "cleanup_executed=true" in output
    release.assert_awaited_once_with(schema_name="testing", max_layouts=3)
    sweep.assert_awaited_once_with(
        schema_name="testing",
        max_rows=4,
        max_bytes=5,
    )

    assert shared_gc._non_negative_int("0") == 0
    with pytest.raises(Exception, match="non-negative"):
        shared_gc._non_negative_int("-1")
