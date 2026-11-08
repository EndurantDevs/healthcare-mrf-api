# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real bounded receipt spools; SQL transport and transaction identities are fake."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy.sql.util import find_tables

from process.custom_import.family_block_codec import FamilyBlockResourceLimit
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_receipt_validation import compare_candidate_receipts
from tests.test_custom_import_family_shard_role_validation import (
    _LIMITS,
    _STAMP,
    _binding,
    _Expected,
    _NativeTransport,
)


class _ReceiptTransport(_NativeTransport):
    def _role(self, statement):
        names = {table.name for table in find_tables(statement, check_columns=True) if table is not None}
        assert self.binding.tables_by_role["shard_receipt"].name in names
        return "shard_receipt"


def _inputs(count):
    binding = _binding()
    receipts = [ShardOutputProof(binding.layout, shard, b"p" * 32, shard).receipt() for shard in range(count)]
    rows = [tuple(receipt.values()) + (_STAMP,) for receipt in receipts]
    expected = _Expected(reversed(receipts))
    session = _ReceiptTransport(binding, {"shard_receipt": rows}, expected)
    return binding, receipts, expected, session


async def _compare(tmp_path, binding, expected, session, **changes):
    paths = []
    result = await compare_candidate_receipts(
        session,
        binding,
        expected,
        work=PlanWork(tmp_path / "receipts", changes.pop("limits", _LIMITS), paths.append),
        **changes,
    )
    assert paths == [tmp_path / "receipts"] and not paths[0].exists()
    return result


@pytest.mark.parametrize("count", (0, 3))
async def test_every_original_commitment_and_native_eof_match(tmp_path, count):
    binding, _receipts, expected, session = _inputs(count)
    result = await _compare(tmp_path, binding, expected, session)
    assert result.counts_by_role == {"shard_receipt": count} and len(result.sha256) == 32
    assert expected.complete and expected.closed and all(cursor.closed for cursor in session.cursors)
    assert [event[0] for event in session.observations] == ["isolation", "census", "cursor", "isolation"]


@pytest.mark.parametrize("damage", ("partition", "result", "source", "role", "missing", "extra", "timestamp"))
async def test_counts_alone_cannot_authorize_changed_receipts(tmp_path, damage):
    binding, _receipts, expected, session = _inputs(3)
    rows = session.rows_by_role["shard_receipt"]
    cells = list(rows[1])
    if damage in {"partition", "result"}:
        cells[1 if damage == "partition" else 2] = b"x" * 32
    elif damage == "source":
        cells[3] += 1
    elif damage == "role":
        cells[4] = cells[4] | {"family": 1}
    elif damage == "timestamp":
        cells[5] = _STAMP.replace(tzinfo=None)
    rows[1] = tuple(cells)
    if damage == "missing":
        rows.pop()
    elif damage == "extra":
        rows.append((99, *rows[0][1:]))
    with pytest.raises(FamilyBlockValidationError):
        await _compare(tmp_path, binding, expected, session)
    assert not (tmp_path / "receipts").exists() and all(cursor.closed for cursor in session.cursors)


@pytest.mark.parametrize("damage", ("duplicate", "late_error", "role_census", "budget"))
async def test_original_refusal_closes_spools_before_any_native_read(tmp_path, damage):
    binding, receipts, expected, session = _inputs(3)
    options_by_name = {}
    if damage == "duplicate":
        expected.batches = [*receipts, receipts[0]]
    elif damage == "late_error":
        expected.failure = RuntimeError("synthetic original receipt tail")
    elif damage == "role_census":
        receipts[0]["role_counts"] = {}
    else:
        options_by_name["limits"] = replace(_LIMITS, memory_bytes=32)
    with pytest.raises((ValueError, RuntimeError, FamilyBlockResourceLimit)):
        await _compare(tmp_path, binding, expected, session, **options_by_name)
    assert expected.closed and not session.observations and not (tmp_path / "receipts").exists()


async def test_transaction_change_at_a_read_boundary_refuses(tmp_path):
    binding, _receipts, expected, session = _inputs(1)
    calls = []

    async def prepare(current):
        assert current is session
        calls.append(current)
        if len(calls) == 4:
            session.nested = SimpleNamespace(is_active=True)

    with pytest.raises(FamilyBlockValidationError, match="transaction or savepoint"):
        await _compare(tmp_path, binding, expected, session, prepare=prepare)
    assert session.cursors[0].closed and not (tmp_path / "receipts").exists()


async def test_cancellation_drains_native_cursor_and_preserves_originals(tmp_path):
    binding, receipts, expected, session = _inputs(3)
    entered, release = asyncio.Event(), asyncio.Event()
    session.read_pause = entered, release
    pending = asyncio.create_task(_compare(tmp_path, binding, expected, session))
    waiting = asyncio.create_task(entered.wait())
    try:
        completed, _unfinished = await asyncio.wait((pending, waiting), timeout=5, return_when=asyncio.FIRST_COMPLETED)
        if pending in completed:
            await pending
        assert waiting in completed
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    finally:
        pending.cancel()
        waiting.cancel()
        await asyncio.gather(pending, waiting, return_exceptions=True)
    assert len(receipts) == 3 and expected.complete and expected.closed
    assert session.cursors[0].closed and not (tmp_path / "receipts").exists()
