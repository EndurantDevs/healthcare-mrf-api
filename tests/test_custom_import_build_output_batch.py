# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Aggregate OUTPUT writes retain bounded complete-group logical pages."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import build_output as output
from process.custom_import import bulk_page_codec
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_build_output import _generation


def _winners(count, canonical="{}"):
    return tuple(
        SimpleNamespace(profile_id="default", canonical_context_key=canonical, context_id=index + 1)
        for index in range(count)
    )


def _batch(monkeypatch, request, winners, *, failure=None):
    closed_flags = []

    def stream(*_args, **_kwargs):
        try:
            yield from winners
            if failure is not None:
                raise failure
        finally:
            closed_flags.append(True)

    def lookup(_session, _request, _build_id, selected):
        assert closed_flags == [True]
        assert 0 < 3 * len(selected) <= output.MAX_BATCH_ROWS
        assert (
            output._LOOKUP_ARRAY_HEADERS
            + sum(output._winner_buffer_bytes(winner.canonical_context_key, winner.profile_id) for winner in selected)
            <= output.MAX_BATCH_BYTES
        )
        return tuple(winner.context_id for winner in selected)

    resolve = Mock(side_effect=lookup)
    monkeypatch.setattr(output, "_ordered_winners", stream)
    monkeypatch.setattr(output, "_selected_winner_context_ids", resolve)
    batch_receipt = output._winner_batch(None, request, object(), 7, _generation(request), None)
    assert closed_flags == [True]
    identities, sizes = batch_receipt
    assert sum(sizes) == len(identities)
    assert all(0 < size <= request.page_row_limit and 32 * size <= request.page_byte_limit for size in sizes)
    return batch_receipt, resolve


def test_output_reuses_source_hard_bounds():
    assert output.MAX_BATCH_ROWS == bulk_page_codec.MAX_BATCH_ROWS == 100_000
    assert output.MAX_BATCH_BYTES == bulk_page_codec.MAX_BATCH_BYTES == 268_435_456


@pytest.mark.parametrize("rows,logical_rows", [(513, 64), (257, 1), (1025, 256)])
def test_one_batch_crosses_256_without_enlarging_logical_pages(monkeypatch, rows, logical_rows):
    request = _request(page_row_limit=logical_rows)
    (ids, sizes), resolve = _batch(monkeypatch, request, _winners(rows))
    assert ids == tuple(range(1, rows + 1))
    assert sum(sizes) == rows and max(sizes) <= logical_rows
    resolve.assert_called_once()
    assert len(resolve.call_args.args[3]) == rows


def test_logical_byte_limit_still_bounds_writer_pages(monkeypatch):
    request = _request(page_row_limit=256, page_byte_limit=64)
    (ids, sizes), _ = _batch(monkeypatch, request, _winners(7))
    assert ids == tuple(range(1, 8)) and sizes == (2, 2, 2, 1)


@pytest.mark.parametrize("short_by,expected", [(0, (1, 2)), (1, (1,))])
def test_batch_byte_boundary_counts_utf8_without_splitting_a_group(monkeypatch, short_by, expected):
    canonical = "é" * 4
    winner_bytes = output._winner_buffer_bytes(canonical, "default")
    assert winner_bytes == output._WINNER_BUFFER_BYTES + len("default") + 8
    monkeypatch.setattr(output, "MAX_BATCH_BYTES", output._LOOKUP_ARRAY_HEADERS + 2 * winner_bytes - short_by)
    request = _request(page_row_limit=4)
    (ids, sizes), _ = _batch(monkeypatch, request, _winners(3, canonical))
    assert ids == expected and sizes == (len(expected),)


def test_batch_row_boundary_keeps_the_partial_last_logical_page(monkeypatch):
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 15)
    request = _request(page_row_limit=2)
    (ids, sizes), _ = _batch(monkeypatch, request, _winners(8))
    assert ids == (1, 2, 3, 4, 5) and sizes == (2, 2, 1)


def test_context_text_is_not_charged_as_winner_model_page_bytes(monkeypatch):
    request = _request(page_row_limit=1, page_byte_limit=32)
    (ids, sizes), _ = _batch(monkeypatch, request, _winners(2, "x" * 8192))
    assert ids == (1, 2) and sizes == (1, 1)


def test_empty_batch_has_no_lookup_or_fake_page(monkeypatch):
    result, resolve = _batch(monkeypatch, _request(), ())
    assert result == ((), ())
    resolve.assert_not_called()


@pytest.mark.parametrize("failure", [CandidateRunnerError("late group failure"), asyncio.CancelledError()])
def test_late_reducer_failure_closes_before_any_identity_lookup(monkeypatch, failure):
    lookup = Mock(side_effect=AssertionError("failed reduction cannot reach a lookup"))
    closed_flags = []

    def broken(*_args, **_kwargs):
        try:
            yield from _winners(3)
            raise failure
        finally:
            closed_flags.append(True)

    monkeypatch.setattr(output, "_ordered_winners", broken)
    monkeypatch.setattr(output, "_selected_winner_context_ids", lookup)
    request = _request()
    with pytest.raises(type(failure)):
        output._winner_batch(None, request, object(), 7, _generation(request), None)
    assert closed_flags == [True]
    lookup.assert_not_called()


@pytest.mark.parametrize("page_bytes,batch_bytes", [(31, 1000), (32, 33)])
def test_one_oversize_winner_is_not_silently_dropped(monkeypatch, page_bytes, batch_bytes):
    monkeypatch.setattr(output, "MAX_BATCH_BYTES", batch_bytes)
    request = _request(page_byte_limit=page_bytes)
    with pytest.raises(CandidateRunnerError, match="one winner exceeds"):
        _batch(monkeypatch, request, _winners(1))


async def test_one_protected_call_carries_all_pages_and_fresh_durable_start(monkeypatch):
    request = _request(page_row_limit=2)
    ids, sizes = tuple(range(1, 514)), (2,) * 256 + (1,)
    events, after_seen = [], []
    build = SimpleNamespace(
        output_after_profile_slot=1, output_after_entity_binding_id=9, output_after_context_key_sha256=b"a" * 32
    )

    @asynccontextmanager
    async def read_session(*_args):
        events.append("read-open")
        yield SimpleNamespace(run_sync=AsyncMock(side_effect=lambda callback: callback(None)))
        events.append("read-close")

    @asynccontextmanager
    async def write_session(*_args):
        assert events[-1] == "read-close"
        events.append("write-open")
        yield object(), build
        events.extend(("fresh-authority", "commit"))
        build.output_after_entity_binding_id = 522

    def batch(_session, _request, _registry, _build_id, _generation, after):
        after_seen.append(after)
        return (ids, sizes) if len(after_seen) == 1 else ((), ())

    call = AsyncMock()
    monkeypatch.setattr(output, "_snapshot", AsyncMock(return_value=build))
    monkeypatch.setattr(output, "_session", read_session)
    monkeypatch.setattr(output, "_page_session", write_session)
    monkeypatch.setattr(output, "_winner_batch", batch)
    monkeypatch.setattr(output, "_typed_call", call)
    await output._write_winners(None, request, object(), 7, _generation(request))
    assert call.await_count == 1
    assert call.await_args.args[1:] == (
        "winner_batch_finalize",
        (
            ("bigint", 7),
            ("bigint[]", ids),
            ("smallint", 1),
            ("bigint", 9),
            ("bytea", b"a" * 32),
            ("integer[]", sizes),
        ),
    )
    assert after_seen == [(1, 9, b"a" * 32), (1, 522, b"a" * 32)]
    assert events == ["read-open", "read-close", "write-open", "fresh-authority", "commit", "read-open", "read-close"]


@pytest.mark.parametrize("failure", [CandidateRunnerError("stale fence"), asyncio.CancelledError()])
async def test_failed_batch_does_not_advance_or_retry_locally(monkeypatch, failure):
    request = _request(page_row_limit=2)
    build = SimpleNamespace(output_after_profile_slot=None)
    transactions = []

    @asynccontextmanager
    async def read_session(*_args):
        yield SimpleNamespace(run_sync=AsyncMock(return_value=((1, 2, 3), (2, 1))))

    @asynccontextmanager
    async def write_session(*_args):
        try:
            yield object(), build
        except BaseException:
            transactions.append("rollback")
            raise
        transactions.append("commit")

    snapshot = AsyncMock(return_value=build)
    call = AsyncMock(side_effect=failure)
    monkeypatch.setattr(output, "_snapshot", snapshot)
    monkeypatch.setattr(output, "_session", read_session)
    monkeypatch.setattr(output, "_page_session", write_session)
    monkeypatch.setattr(output, "_typed_call", call)
    with pytest.raises(type(failure)):
        await output._write_winners(None, request, object(), 7, _generation(request))
    assert transactions == ["rollback"] and snapshot.await_count == call.await_count == 1
