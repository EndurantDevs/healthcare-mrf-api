# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real protected journal/FIFO files; native transport and output body are doubled.

These tests qualify allocation custody, not native PostgreSQL or retained values.
The existing original/destination suites keep their independent value checks.
"""

import asyncio
import hashlib
import time
from contextlib import asynccontextmanager, closing, contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_allocate as source_allocation
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_events as events
from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_journal as journal_api
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_retained_reservations import RetainedControlWindow, RetainedReservations
from tests import test_custom_import_family_shard_events as event_fixture

pytestmark = pytest.mark.asyncio
_PREFIX = ((1, 1), (2, 1))


async def _produce_retained_fixture(native, context, checked_context, *, work, observe, scratch):
    assert checked_context is context and scratch.root == work.root

    async def observed(kinds, remembered, *, cursor=()):
        if native.changed_cursor and kinds:
            cursor = cursor[0], cursor[1] + 1, cursor[2]
        return await observe(_PREFIX, kinds, remembered, cursor=cursor)

    control = RetainedControlWindow(scratch, native.limits, observed)
    with allocation._Output(context, scratch, _PREFIX) as output:
        with RetainedReservations(scratch, native.limits, control) as bank:
            for kinds in (
                ("family_revision", "root_revision", "pack"),
                ("child_revision", "child_revision", "pack"),
            ):
                ids = await bank.take(kinds)
                native.published.append(ids)
                output.observed(kinds, ids, False)
                if native.producer_failure is not None:
                    raise native.producer_failure
            await bank.finish(output)
        output.files["allocation"].flush()
        with closing(scratch.rows(output.paths["allocation"])) as retained_rows:
            dispositions = tuple(retained_rows)
    return SimpleNamespace(
        sha256=hashlib.sha256(journal_api._pack(dispositions)).digest(),
        counts=tuple(output.counts.items()),
        source_pack_prefix=_PREFIX,
        destination_pack_tail=_PREFIX,
    )


def _install_retained_transport(monkeypatch, native, context):
    original_page = source_allocation._page_session

    @asynccontextmanager
    async def page(factory, request, build_id):
        async with original_page(factory, request, build_id) as (session, build):
            session.info = {_WINDOW: _PageWindow(time.monotonic() + 60, 1000)}
            yield session, build

    async def guard(session, checked_context, prefix):
        assert checked_context is context and prefix == _PREFIX
        if native.refusal == "fence":
            raise RuntimeError("fence")
        if native.refusal == "cancel":
            raise asyncio.CancelledError

    async def base(checked_context):
        assert checked_context is context
        native.base_calls.append(True)
        if native.refusal == "base" and len(native.base_calls) >= 2:
            raise RuntimeError("base")

    async def allocate(session, kinds):
        assert session.writing
        with pytest.raises(FamilyBlockUnavailable, match="drain"):
            native.journal.close_terminal()
        observed_result_items = []
        for kind in kinds:
            native.next_ids[kind] = native.next_ids.get(kind, 10000) + 17
            observed_result_items.append(native.next_ids[kind])
        native.vector_calls.append((kinds, tuple(observed_result_items)))
        return tuple(observed_result_items)

    async def produce(_factory, checked_context, *, work, observe, scratch):
        return await _produce_retained_fixture(
            native, context, checked_context, work=work, observe=observe, scratch=scratch
        )

    monkeypatch.setattr(allocation, "_scope", lambda _context, _work: ("explicit control transport fixture",))
    monkeypatch.setattr(allocation, "_guard", guard)
    monkeypatch.setattr(allocation, "_check_base", base)
    monkeypatch.setattr(allocation, "_produce_retained_destination", produce)
    monkeypatch.setattr(journal_api, "_page_session", page)
    monkeypatch.setattr(journal_api, "_allocate_record_ids", allocate)
    monkeypatch.setattr(checkpoint, "_page_session", page)


@asynccontextmanager
async def _case(tmp_path, monkeypatch):
    async with event_fixture._case(tmp_path, monkeypatch) as native:
        event_fixture._native_boundary(monkeypatch, native)
        original = await event_fixture._allocate(tmp_path, native)
        context = allocation.RetainedAllocationContext(
            SimpleNamespace(request=native.request, layout=native.layout, source=native.source),
            native.journal.build_id,
            native.admission,
            SimpleNamespace(counts_by_kind={"retained": 1}),
            None,
            None,
            SimpleNamespace(proof_inputs={"owner_created_at": original.owner_created_at}),
            original,
        )
        native.native.phase = "graph"
        native.vector_calls, native.next_ids, native.published, native.base_calls = [], {}, [], []
        native.refusal = native.producer_failure = None
        native.changed_cursor = False
        _install_retained_transport(monkeypatch, native, context)
        work = allocation.RetainedAllocationWork(tmp_path / "unused-destination", native.limits, lambda _path: None)
        try:
            yield SimpleNamespace(native=native, context=context, journal=native.journal, work=work)
        finally:
            events.cleanup_admission_events(original)


async def _allocate(case):
    return await allocation.allocate_retained_destination(None, case.context, work=case.work, journal=case.journal)


async def test_completed_refill_replays_exact_vectors_without_source_decode(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        first = await _allocate(case)
        calls, values = tuple(case.native.vector_calls), tuple(case.native.published)
        assert calls and len(calls) == 2 and not case.journal.busy

        async def forbidden(*_args, **_kwargs):
            pytest.fail("committed native IDs cannot be allocated again")

        def no_source_replay(*_args, **_kwargs):
            pytest.fail("a protected phase digest must not trigger another SOURCE value decode")

        monkeypatch.setattr(journal_api, "_allocate_record_ids", forbidden)
        monkeypatch.setattr(journal_api.metadata_journal, "read_source_allocations", no_source_replay)
        case.native.published.clear()
        second = await _allocate(case)
        assert second == first and tuple(case.native.published) == values
        assert tuple(case.native.vector_calls) == calls


@pytest.mark.parametrize("failure", ("after", "unknown", "read", "producer"))
async def test_lost_commit_read_or_output_reuses_exact_committed_vectors(tmp_path, monkeypatch, failure):
    async with _case(tmp_path, monkeypatch) as case:
        if failure in {"after", "unknown"}:
            case.native.native.failure = failure
        elif failure == "read":
            original = journal_api._Run.snapshot

            async def lose_read(run):
                if case.native.vector_calls and not getattr(case.native, "read_lost", False):
                    case.native.read_lost = True
                    raise ConnectionError("fresh read lost")
                return await original(run)

            monkeypatch.setattr(journal_api._Run, "snapshot", lose_read)
        else:
            case.native.producer_failure = RuntimeError("output lost")
        if failure == "after":
            result = await _allocate(case)
            assert result and len(case.native.vector_calls) == 2
            return
        with pytest.raises((ConnectionError, RuntimeError, FamilyBlockUnavailable)):
            await _allocate(case)
        assert len(case.native.vector_calls) == 1 and not case.journal.busy
        first = case.native.vector_calls[0]
        case.native.producer_failure = None
        result = await _allocate(case)
        assert result and case.native.vector_calls.count(first) == 1 and len(case.native.vector_calls) == 2
        assert case.native.published[-2] == first[1]


@pytest.mark.parametrize("failure", ("fence", "base", "cancel"))
async def test_original_guard_base_and_cancellation_refusals_never_publish_ids(tmp_path, monkeypatch, failure):
    async with _case(tmp_path, monkeypatch) as case:
        case.native.refusal = failure
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await _allocate(case)
        assert not case.native.published and not case.journal.busy


async def test_changed_fifo_cursor_is_refused_without_replacement_ids(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        await _allocate(case)
        calls = tuple(case.native.vector_calls)
        case.native.changed_cursor = True
        with pytest.raises(FamilyBlockUnavailable, match="cursor"):
            await _allocate(case)
        assert tuple(case.native.vector_calls) == calls


@pytest.mark.parametrize("damage", ("events", "journal", "busy", "continuation"))
async def test_missing_original_custody_refuses_before_native_allocation(tmp_path, monkeypatch, damage):
    async with _case(tmp_path, monkeypatch) as case:
        context, journal = case.context, case.journal
        if damage == "events":
            context = replace(context, events=None)
        if damage == "journal":
            journal = None
        if damage == "busy":
            journal.busy = True
        if damage == "continuation":
            context.common.request = replace(
                context.common.request,
                continuation=BuildContinuationContext(9, context.common.request.fence, b"p" * 32),
            )
        try:
            with pytest.raises((ValueError, FamilyBlockUnavailable)):
                await allocation.allocate_retained_destination(None, context, work=case.work, journal=journal)
            assert not case.native.vector_calls
        finally:
            case.journal.busy = False


async def test_tampered_committed_vector_is_not_regenerated(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        await _allocate(case)
        calls = tuple(case.native.vector_calls)
        path = next((case.journal.root / "vectors").glob("retained-*.mpk"))
        path.chmod(0o600)
        body = bytearray(path.read_bytes())
        body[-1] ^= 1
        path.write_bytes(body)
        path.chmod(0o400)
        with pytest.raises(FamilyBlockUnavailable, match="custody|SHA"):
            await _allocate(case)
        assert tuple(case.native.vector_calls) == calls


async def test_complete_output_digest_cannot_change_on_replay(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        await _allocate(case)
        calls = tuple(case.native.vector_calls)
        original = allocation._produce_retained_destination

        async def changed(*args, **kwargs):
            output = await original(*args, **kwargs)
            output.sha256 = b"x" * 32
            return output

        monkeypatch.setattr(allocation, "_produce_retained_destination", changed)
        with pytest.raises(FamilyBlockUnavailable, match="protected complete EOF"):
            await _allocate(case)
        assert tuple(case.native.vector_calls) == calls


async def test_completed_original_event_digest_is_required_before_new_ids(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        case.context = replace(case.context, events=replace(case.context.events, sha256=b"x" * 32))
        with pytest.raises(FamilyBlockUnavailable, match="admission completion"):
            await _allocate(case)
        assert not case.native.vector_calls


async def test_all_provisional_files_count_against_original_journal_budget(tmp_path, monkeypatch):
    from process.custom_import.family_shard_facts import SourceFactBudgetExceeded

    async with _case(tmp_path, monkeypatch) as case:
        filler = case.journal.root / "outputs" / "provisional-budget-fill"
        with filler.open("xb") as stream:
            stream.truncate(case.journal.work.limits.max_spool_bytes)
        with pytest.raises(SourceFactBudgetExceeded):
            await _allocate(case)
        assert not case.native.vector_calls and filler.exists() and not case.journal.busy


async def test_expired_original_deadline_refuses_before_native_reconciliation(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        case.context.common.request = replace(case.context.common.request, operation_deadline=time.monotonic() - 1)
        before_items = tuple(case.native.native)
        with pytest.raises(TimeoutError, match="operation deadline"):
            await _allocate(case)
        assert not case.native.vector_calls and tuple(case.native.native) == before_items and not case.journal.busy


async def test_uncommitted_sequence_gaps_stay_provisional_and_are_not_replayed(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        case.native.native.failure = "before"
        with pytest.raises(RuntimeError, match="before commit"):
            await _allocate(case)
        assert len(case.native.vector_calls) == 1 and not case.native.published
        rejected = case.native.vector_calls[0][1]
        case.native.native.failure = None
        await _allocate(case)
        assert len(case.native.vector_calls) == 3
        assert case.native.published[0] != rejected
        assert all(value > prior for value, prior in zip(case.native.published[0], rejected, strict=True))


async def test_cancelled_committed_reply_keeps_exact_ids_for_warm_reconciliation(tmp_path, monkeypatch):
    async with _case(tmp_path, monkeypatch) as case:
        original = journal_api._page_session
        observer = SimpleNamespace(is_cancelled=False)

        @asynccontextmanager
        async def lost_reply(*args):
            async with original(*args) as values:
                yield values
            if values[0].writing and not observer.is_cancelled:
                observer.is_cancelled = True
                raise asyncio.CancelledError

        monkeypatch.setattr(journal_api, "_page_session", lost_reply)
        with pytest.raises(asyncio.CancelledError):
            await _allocate(case)
        assert len(case.native.vector_calls) == 1 and not case.native.published and not case.journal.busy
        committed = case.native.vector_calls[0]
        await _allocate(case)
        assert len(case.native.vector_calls) == 2 and case.native.vector_calls.count(committed) == 1
        assert case.native.published[0] == committed[1]


@pytest.mark.parametrize("failure", (False, True))
async def test_borrowed_source_scratch_reserves_complete_shared_budget_until_closed(monkeypatch, tmp_path, failure):
    budget = journal_api.source_journal._JournalBudget(48 * 1024, 16 * 1024)
    work = SimpleNamespace(
        root=tmp_path, limits=SimpleNamespace(max_spool_bytes=32 * 1024), current=lambda: None, budget=budget
    )
    # PlanWork retains its genuine typed limits; only the producer boundary is doubled.
    from process.custom_import.family_shard_facts import SourceFactLimits

    work.limits = SourceFactLimits(32 * 1024, 1024, 1024)

    @contextmanager
    def borrowed(*_args, **_kwargs):
        assert budget.used == budget.maximum
        try:
            if failure:
                raise RuntimeError("borrowed original read failed")
            yield iter(())
        finally:
            assert budget.used == budget.maximum

    monkeypatch.setattr(allocation, "source_family_inputs", borrowed)
    context = SimpleNamespace(common=None, admission=None, final_plan=None)
    if failure:
        with pytest.raises(RuntimeError, match="borrowed original"):
            allocation._stage_sources(context, work, {})
    else:
        allocation._stage_sources(context, work, {})
    assert budget.used == 16 * 1024
