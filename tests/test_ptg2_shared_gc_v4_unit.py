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
        return 1


def _abandonment_context():
    return shared_gc._OwnedV4AbandonmentContext(
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
        batch_rows=2,
        deadline=10.0,
        statement_timeout_seconds=1.0,
        monotonic=lambda: 0.0,
        grace_seconds=60,
    )


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
async def test_owned_v4_layout_lock_validation_fences(monkeypatch):
    """Validation rejects absent, bound, and completed layouts without claiming."""

    fingerprint = AsyncMock(return_value=None)
    monkeypatch.setattr(shared_gc, "_owned_v4_layout_fingerprint", fingerprint)
    assert not await shared_gc._is_owned_v4_layout_locked(
        _Executor(), schema_name="mrf", snapshot_key=1, build_token="token"
    )

    fingerprint.return_value = b"a" * 32
    assert not await shared_gc._is_owned_v4_layout_locked(
        _Executor([]), schema_name="mrf", snapshot_key=1, build_token="token"
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
    validated_executor = _Executor([
        {"build_token": "token", "is_bound": False, "root_state": None}
    ])
    assert await shared_gc._is_owned_v4_layout_locked(
        validated_executor,
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
    )
    assert len(validated_executor.status_calls) == 1


@pytest.mark.asyncio
async def test_owned_v4_layout_lock_claims_and_resumes(monkeypatch):
    """Claim an owned layout once and resume only its exact durable marker."""

    monkeypatch.setattr(
        shared_gc,
        "_owned_v4_layout_fingerprint",
        AsyncMock(return_value=b"a" * 32),
    )
    locked_executor = _Executor([
        {"build_token": "token", "is_bound": False, "root_state": None}
    ])
    assert await shared_gc._is_owned_v4_layout_locked(
        locked_executor,
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
        claim_abandonment=True,
    )
    assert "pg_advisory_xact_lock" in locked_executor.status_calls[0][0]
    assert "SET build_token = :abandonment_token" in (
        locked_executor.status_calls[1][0]
    )

    abandonment_token = shared_gc._owned_v4_abandonment_token("token")
    resumed_executor = _Executor([{
        "build_token": abandonment_token,
        "is_bound": False,
        "root_state": "building",
    }])
    assert await shared_gc._is_owned_v4_layout_locked(
        resumed_executor,
        schema_name="mrf",
        snapshot_key=1,
        build_token="token",
        claim_abandonment=True,
    )
    assert len(resumed_executor.status_calls) == 1


@pytest.mark.asyncio
async def test_owned_v4_layout_lock_rejects_unknown_owner_marker(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_owned_v4_layout_fingerprint",
        AsyncMock(return_value=b"a" * 32),
    )
    executor = _Executor(
        [
            {
                "build_token": "different-owner",
                "is_bound": False,
                "root_state": "building",
            }
        ]
    )
    with pytest.raises(RuntimeError, match="marker is invalid"):
        await shared_gc._is_owned_v4_layout_locked(
            executor,
            schema_name="mrf",
            snapshot_key=1,
            build_token="token",
            claim_abandonment=True,
        )


@pytest.mark.asyncio
async def test_owned_v4_mapping_inventory_rejects_invalid_rows():
    context = _abandonment_context()
    assert await shared_gc._owned_v4_mapping_hashes(
        _Executor([]),
        context=context,
    ) == set()

    with pytest.raises(RuntimeError, match="hash order is invalid"):
        await shared_gc._owned_v4_mapping_hashes(
            _Executor([{"block_hash": b"short"}]),
            context=context,
        )

    with pytest.raises(RuntimeError, match="missing CAS block"):
        await shared_gc._owned_v4_stored_bytes(
            _Executor([]),
            context=context,
            block_hashes=(b"a" * 32,),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("shared_tables_available", "map_tables_available", "message"),
    (
        (False, True, "requires shared tables"),
        (True, False, "requires V4 map tables"),
    ),
)
async def test_owned_v4_inventory_requires_complete_schema(
    monkeypatch,
    shared_tables_available,
    map_tables_available,
    message,
):
    monkeypatch.setattr(
        shared_gc,
        "_has_shared_tables",
        AsyncMock(return_value=shared_tables_available),
    )
    monkeypatch.setattr(
        shared_gc,
        "_has_v4_map_tables",
        AsyncMock(return_value=map_tables_available),
    )

    with pytest.raises(RuntimeError, match=message):
        await shared_gc._checked_owned_v4_inventory(
            object(),
            _abandonment_context(),
        )


@pytest.mark.asyncio
async def test_owned_v4_candidate_batch_fences_owner_and_cas(monkeypatch):
    owner_lock = AsyncMock(return_value=False)
    monkeypatch.setattr(shared_gc, "_is_owned_v4_layout_locked", owner_lock)
    with pytest.raises(RuntimeError, match="layout changed"):
        await shared_gc._queue_owned_v4_candidate_batch(
            _Executor([]),
            context=_abandonment_context(),
            block_hashes=(b"a" * 32,),
        )

    owner_lock.return_value = True
    with pytest.raises(RuntimeError, match="missing CAS block"):
        await shared_gc._queue_owned_v4_candidate_batch(
            _Executor([], []),
            context=_abandonment_context(),
            block_hashes=(b"a" * 32,),
        )


@pytest.mark.asyncio
async def test_owned_v4_dense_batch_fences_owner(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_is_owned_v4_layout_locked",
        AsyncMock(return_value=False),
    )

    with pytest.raises(RuntimeError, match="layout changed"):
        await shared_gc._delete_owned_v4_dense_batch(
            _Executor([]),
            context=_abandonment_context(),
            table_name="ptg2_v3_serving_rate",
        )


@pytest.mark.asyncio
async def test_owned_v4_finalization_rejects_incomplete_cleanup(monkeypatch):
    context = _abandonment_context()
    block_hash = b"a" * 32
    with pytest.raises(RuntimeError, match="candidate queue is incomplete"):
        await shared_gc._assert_owned_v4_candidates_complete(
            _Executor([]),
            context=context,
            block_hashes=(block_hash,),
        )
    with pytest.raises(RuntimeError, match="dense cleanup is incomplete"):
        await shared_gc._require_owned_v4_dense_cleanup(
            _Executor([{"table_name": "dense", "row_count": 1}]),
            context=context,
        )

    failed_delete = _Executor()
    failed_delete.status = AsyncMock(return_value=0)
    with pytest.raises(RuntimeError, match="layout changed"):
        await shared_gc._delete_owned_v4_layout(
            failed_delete,
            context=context,
            abandonment_token="abandon-token",
        )

    owner_lock = AsyncMock(return_value=False)
    monkeypatch.setattr(shared_gc, "_is_owned_v4_layout_locked", owner_lock)
    with pytest.raises(RuntimeError, match="layout changed"):
        await shared_gc._finalize_owned_v4_abandonment(
            _Executor([]),
            context=context,
            inventory=shared_gc._OwnedV4AbandonmentInventory(
                block_hashes=(block_hash,),
                stored_bytes=1,
                abandonment_token="abandon-token",
            ),
        )


@pytest.mark.asyncio
async def test_owned_v4_finalization_rejects_reachability_drift(monkeypatch):
    monkeypatch.setattr(
        shared_gc,
        "_is_owned_v4_layout_locked",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        shared_gc,
        "_current_owned_v4_hashes",
        AsyncMock(return_value=(b"b" * 32,)),
    )

    with pytest.raises(RuntimeError, match="reachability changed"):
        await shared_gc._finalize_owned_v4_abandonment(
            _Executor([]),
            context=_abandonment_context(),
            inventory=shared_gc._OwnedV4AbandonmentInventory(
                block_hashes=(b"a" * 32,),
                stored_bytes=1,
                abandonment_token="abandon-token",
            ),
        )


@pytest.mark.asyncio
async def test_owned_v4_statement_timeout_becomes_deferred(monkeypatch):
    class OperationalError(RuntimeError):
        pass

    monkeypatch.setattr(
        shared_gc,
        "_run_owned_v4_step",
        AsyncMock(side_effect=OperationalError("statement timeout")),
    )

    with pytest.raises(
        shared_gc.PTG2SharedLayoutAbandonmentDeferred,
        match="statement timed out",
    ):
        await shared_gc.abandon_owned_v4_layout(
            snapshot_key=1,
            build_token="token",
            executor=object(),
            options=shared_gc.PTG2V4AbandonmentOptions(
                timeout_seconds=10,
                monotonic=lambda: 0.0,
            ),
        )


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
