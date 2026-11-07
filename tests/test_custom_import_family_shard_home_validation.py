# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real bounded identity spools; SQL and transaction transport are explicit fakes."""

import asyncio
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import.family_block_validate import FamilyBlockValidationError
from process.custom_import.family_shard_home_validation import compare_candidate_homes
from process.custom_import.family_shard_plan import PlanWork
from tests.test_custom_import_family_shard_role_validation import _LIMITS, _binding, _Expected, _NativeTransport


class _RangeCursor:
    def __init__(self, session, size):
        self.session, self.size, self.closed = session, size, False

    async def partitions(self, size):
        assert size == self.size
        if self.session.pause is not None:
            self.session.pause[0].set()
            await self.session.pause[1].wait()
        for offset in range(0, len(self.session.ranges), size):
            yield self.session.ranges[offset : offset + size]
        if self.session.tail_error is not None:
            raise self.session.tail_error

    async def close(self):
        self.closed = True


class _HomeTransport(_NativeTransport):
    def __init__(self, binding, expected, ranges):
        super().__init__(binding, {}, expected)
        self.ranges, self.pause, self.tail_error = ranges, None, None
        self.count_delta = 0

    async def execute(self, statement):
        assert self.expected.complete and self.expected.closed
        sql = str(statement.compile(dialect=dialect()))
        if "transaction_isolation" in sql:
            self.observations.append("isolation")
            return SimpleNamespace(one=lambda: self.isolation)
        assert "LATERAL unnest(" in sql and "generate_series" not in sql and "ORDER BY" not in sql
        assert "family_id" in sql and "upper_inf(" in sql
        self.observations.append("census")
        census = SimpleNamespace(row_count=len(self.ranges) + self.count_delta, row_bytes=2048 if self.ranges else 0)
        return SimpleNamespace(one=lambda: census)

    async def stream(self, statement):
        sql = str(statement.compile(dialect=dialect()))
        assert "ORDER BY" in sql and "revision_kind" in sql
        assert self.binding.family_id in statement.compile().params.values()
        size = statement.get_execution_options()["yield_per"]
        assert 1 <= size <= 4096
        cursor = _RangeCursor(self, size)
        self.observations.append("cursor")
        self.cursors.append(cursor)
        return cursor


def _inputs(empty=False):
    identities = [] if empty else [(2, 5), (1, 3), (2, 1), (1, 1), (2, 2)]
    ranges = [] if empty else [(1, 1, 1), (1, 3, 3), (2, 1, 2), (2, 5, 5)]
    binding, expected = _binding(), _Expected(identities)
    return binding, expected, _HomeTransport(binding, expected, ranges)


async def _compare(tmp_path, binding, expected, session, **options):
    roots = []
    result = await compare_candidate_homes(
        session, binding, expected, work=PlanWork(tmp_path / "homes", _LIMITS, roots.append), **options
    )
    assert roots == [tmp_path / "homes"] and not roots[0].exists()
    return result


@pytest.mark.parametrize("empty", (False, True))
async def test_complete_actual_ids_match_compressed_homes_including_eliminated_rows(tmp_path, empty):
    binding, expected, session = _inputs(empty)
    result = await _compare(tmp_path, binding, expected, session)
    assert (result.root_count, result.child_count, result.range_count) == ((0, 0, 0) if empty else (2, 3, 4))
    assert len(result.sha256) == 32 and expected.complete and expected.closed
    assert session.observations == ["isolation", "census", "cursor", "isolation"]
    assert session.cursors[0].closed


@pytest.mark.parametrize("damage", ("gap", "missing", "extra", "overlap", "kind", "census", "tail"))
async def test_ranges_cannot_hide_missing_or_invented_allocations(tmp_path, damage):
    binding, expected, session = _inputs()
    match damage:
        case "gap":
            session.ranges[-1] = (2, 3, 5)
        case "missing":
            session.ranges.pop()
        case "extra":
            session.ranges.append((2, 6, 6))
        case "overlap":
            session.ranges.insert(1, (1, 1, 1))
        case "kind":
            session.ranges[-1] = (3, 5, 5)
        case "census":
            session.count_delta = 1
        case "tail":
            session.tail_error = RuntimeError("synthetic native range tail")
    with pytest.raises((FamilyBlockValidationError, RuntimeError)):
        await _compare(tmp_path, binding, expected, session)
    assert session.cursors[0].closed and not (tmp_path / "homes").exists()


@pytest.mark.parametrize("damage", ("duplicate", "reserved", "missing_eliminated", "late_error"))
async def test_original_union_must_be_unique_complete_and_accepted_only(tmp_path, damage):
    binding, expected, session = _inputs()
    if damage == "duplicate":
        expected.batches.append((2, 5))
    elif damage == "reserved":
        expected.batches.append((2, 6))
    elif damage == "missing_eliminated":
        expected.batches.remove((2, 5))
    else:
        expected.failure = RuntimeError("synthetic original identity tail")
    with pytest.raises((FamilyBlockValidationError, RuntimeError)):
        await _compare(tmp_path, binding, expected, session)
    assert expected.closed and not (tmp_path / "homes").exists()
    if damage in {"duplicate", "late_error"}:
        assert not session.observations


async def test_final_read_boundary_cannot_change_the_callers_transaction(tmp_path):
    binding, expected, session = _inputs()
    calls = []

    async def prepare(current):
        assert current is session
        calls.append(current)
        if len(calls) == 6:
            session.nested = SimpleNamespace(is_active=True)

    with pytest.raises(FamilyBlockValidationError, match="transaction or savepoint"):
        await _compare(tmp_path, binding, expected, session, prepare=prepare)
    assert session.cursors[0].closed and not (tmp_path / "homes").exists()


async def test_native_range_cancellation_drains_before_spool_removal(tmp_path):
    binding, expected, session = _inputs()
    entered, release = asyncio.Event(), asyncio.Event()
    session.pause = entered, release
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
    assert expected.complete and expected.closed and session.cursors[0].closed
    assert not (tmp_path / "homes").exists()
