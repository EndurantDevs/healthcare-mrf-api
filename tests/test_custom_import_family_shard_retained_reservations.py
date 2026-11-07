# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real FIFO/ledger spools; explicitly fake owner and native cost transport."""

import asyncio
import time
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_retained_allocate as allocation
from process.custom_import import family_shard_retained_reservations as reservations
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_allocate import SourceAllocationLimits
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork, _Scratch
from process.custom_import.family_shard_retained_homes import _accepted
from process.custom_import.family_shard_retained_output import _Output
from process.custom_import.family_shard_work_cost import WorkCostPage
from tests.test_custom_import_family_shard_retained_allocate import _context, _prepared, _produce_destination
from tests.test_custom_import_family_shard_retained_scope import _case as _empty_scope_case
from tests.test_custom_import_family_shard_retained_scope import _work as _empty_scope_work

_FACTS = SourceFactLimits(4 * 1024**2, 64 * 1024, 64 * 1024)
_KINDS = ("family_revision", "root_revision", "child_revision", "pack")


class _ExactDemand(reservations.RetainedReservations):
    """Reference native-ID transport: one exact observation per logical attempt."""

    def _requests(self, kinds, held_bytes):
        return {kind: count for kind, count in Counter(kinds).items()}


def _capture_costs(monkeypatch, operations):
    costs = []
    for name, operation in operations:

        def captured(*arguments, _operation=operation):
            result = _operation(*arguments)
            costs.append(
                tuple(
                    (
                        page.page_ordinal,
                        page.work_rows,
                        page.context_count,
                        page.work_bytes,
                        page.model_bytes,
                        allocation._pack_row(tuple(allocation._model_cells(scalar) for scalar in page.scalars)),
                    )
                    for page in result
                )
            )
            return result

        monkeypatch.setattr(allocation, name, captured)
    return costs


class _Owner:
    def __init__(self, *, start=8):
        self.next = dict.fromkeys(_KINDS, start)
        self.calls, self.active, self.failure = [], False, None

    async def observe(self, kinds, remembered, *, cursor=()):
        self.active = True
        try:
            identities = []
            for kind in kinds:
                identities.append(self.next[kind])
                self.next[kind] += 1
            self.calls.append((kinds, tuple(identities)))
            if self.failure is not None:
                raise self.failure
        finally:
            self.active = False
        remembered(_PageWindow(time.monotonic() + 60, 1000))
        return tuple(identities)


@asynccontextmanager
async def _case(tmp_path, *, maximum=8, memory=None, owner=None):
    facts = _FACTS if memory is None else replace(_FACTS, memory_bytes=memory)
    limits = SourceAllocationLimits(facts, maximum, maximum * 96)
    work = _Scratch(PlanWork(tmp_path / "work", facts, lambda _path: None))
    owner = owner or _Owner()
    control = reservations.RetainedControlWindow(work, limits, owner.observe)
    try:
        with _Output(_context(), work, ((11, 0), (23, 0))) as output:
            with reservations.RetainedReservations(work, limits, control) as bank:
                yield SimpleNamespace(work=work, limits=limits, control=control, owner=owner, bank=bank, output=output)
    finally:
        work.close()
    assert not (tmp_path / "work").exists()


def _dispositions(case):
    case.output.files["allocation"].flush()
    return list(case.work.rows(case.output.paths["allocation"]))


@pytest.mark.parametrize("start", [8, 98])
async def test_shared_pack_fifo_and_all_idle_tails_cross_cohorts(tmp_path, start):
    async with _case(tmp_path, owner=_Owner(start=start)) as case:
        operations = [
            (("family_revision", "root_revision", "pack"), True),
            (("child_revision", "child_revision", "pack"), False),
            (("family_revision", "root_revision", "pack"), True),
            (("child_revision", "pack"), True),
            (("family_revision", "root_revision", "pack"), True),
        ]
        consumed = defaultdict(list)
        for kinds, accepted in operations:
            identities = await case.bank.take(kinds)
            case.output.observed(kinds, identities, accepted)
            for kind, identity in zip(kinds, identities, strict=True):
                consumed[kind].append(identity)
        assert consumed["pack"] == list(range(start, start + 5))
        assert consumed["family_revision"] == consumed["root_revision"] == list(range(start, start + 3))
        assert any(queue.available for queue in case.bank.queues.values())
        await case.bank.finish(case.output)
        actual = defaultdict(list)
        for _attempt, _ordinal, kind, identity, _accepted_value in _dispositions(case):
            actual[kind].append(identity)
        observed = defaultdict(list)
        for kinds, identities in case.owner.calls:
            for kind, identity in zip(kinds, identities, strict=True):
                observed[kind].append(identity)
        assert actual == observed
        assert Counter((allocation_row[2], allocation_row[3]) for allocation_row in _dispositions(case)) == Counter(
            (kind, identity) for kind, identities in observed.items() for identity in identities
        )


@pytest.mark.parametrize("start", [8, 98])
async def test_matched_native_streams_preserve_grouping_hashes_costs_and_rejected_gaps(tmp_path, monkeypatch, start):
    artifacts = []
    operations = tuple((name, getattr(allocation, name)) for name in ("_retained_root_costs", "_child_costs"))
    for mode in (reservations.RetainedReservations, _ExactDemand):
        failures = Counter()
        costs = _capture_costs(monkeypatch, operations)
        (tmp_path / mode.__name__).mkdir()

        def should_fail_once(kind, roots):
            failures[(kind, roots)] += 1
            return not roots and failures[(kind, roots)] == 1

        _scope, work, output, _views, _observations = await _prepared(
            tmp_path / mode.__name__,
            monkeypatch,
            kinds=("retained", "source", "retained", "retained"),
            count=5,
            failure=should_fail_once,
            start=start,
            reservation_class=mode,
        )
        try:
            emitted_by_role = {name: tuple(work.rows(output.paths[name])) for name in ("family", "record", "copy")}
            ledger_rows = tuple(work.rows(output.paths["allocation"]))
            # Raw terminal leftovers have no logical attempt in the exact-demand reference.
            artifacts.append((emitted_by_role, ledger_rows, costs))
        finally:
            work.close()
    batched, exact = artifacts
    assert batched[0] == exact[0]
    assert batched[1][: len(exact[1])] == exact[1]
    assert batched[2] == exact[2]


@pytest.mark.parametrize("maximum", [1, 4])
async def test_tiny_budget_preserves_idle_tails_and_maximum_valid_demand(tmp_path, maximum):
    async with _case(tmp_path, maximum=maximum, memory=4096) as case:
        for kinds in (("child_revision",), ("child_revision",)):
            identities = await case.bank.take(kinds)
            case.output.observed(kinds, identities, True)
        idle_before = case.bank.queues["child_revision"].available
        kinds = tuple(_KINDS[:maximum])
        identities = await case.bank.take(kinds, held_bytes=4096 - 96 * maximum)
        case.output.observed(kinds, identities, True)
        assert len(identities) == maximum
        assert case.bank.queues["child_revision"].available == idle_before - int("child_revision" in kinds)
        assert all(len(call_kinds) <= maximum for call_kinds, _ids in case.owner.calls)
        await case.bank.finish(case.output)


async def test_idle_kind_quota_does_not_reject_a_full_different_kind_demand(tmp_path):
    async with _case(tmp_path, maximum=4, memory=4096) as case:
        for _ in range(4):
            kinds = ("child_revision",)
            case.output.observed(kinds, await case.bank.take(kinds), True)
        assert case.bank.queues["child_revision"].available == 3
        kinds = ("root_revision",) * 4
        identities = await case.bank.take(kinds, held_bytes=4096 - 4 * 96)
        case.output.observed(kinds, identities, True)
        assert case.bank.queues["child_revision"].available == 3
        assert case.owner.calls[-1][0] == kinds
        await case.bank.finish(case.output)


async def test_actual_fifo_preserves_nonmonotonic_ids_without_ranges(tmp_path):
    streams_by_kind = {kind: iter((101, 7, 99, 8, 1000, 9, 98, 10)) for kind in _KINDS}
    async with _case(tmp_path) as case:

        async def observe(kinds, remembered, *, cursor=()):
            identities = tuple(next(streams_by_kind[kind]) for kind in kinds)
            remembered(_PageWindow(time.monotonic() + 60, 1000))
            return identities

        case.control.observe = observe
        packs = []
        for kinds in (("child_revision", "pack"), ("root_revision", "pack"), ("child_revision", "pack")):
            identities = await case.bank.take(kinds)
            packs.append(identities[-1])
            case.output.observed(kinds, identities, True)
        assert packs == [101, 7, 99]
        await case.bank.finish(case.output)


async def test_failed_and_unused_ids_have_no_accepted_home_membership(tmp_path):
    async with _case(tmp_path) as case:
        for accepted in (False, True, True):
            kinds = ("child_revision", "pack")
            identities = await case.bank.take(kinds)
            case.output.observed(kinds, identities, accepted)
        await case.bank.finish(case.output)
        ledger = _dispositions(case)
        kind_code_by_name = dict(family_revision=3, root_revision=1, child_revision=2, pack=4)
        path = case.work.write((kind_code_by_name[row[2]], row[3], row[4]) for row in ledger)
        wanted_ids = {(kind_code_by_name[row[2]], row[3]) for row in ledger if row[4]}
        refused_ids = {(kind_code_by_name[row[2]], row[3]) for row in ledger if not row[4]}
        assert wanted_ids and refused_ids and wanted_ids.isdisjoint(refused_ids)
        assert set(_accepted(case.work, path)) == wanted_ids


@pytest.mark.parametrize("damage", ["omitted", "duplicate", "reordered"])
async def test_complete_disposition_eof_refuses_missing_or_changed_observations(tmp_path, damage):
    async with _case(tmp_path) as case:
        kinds = ("child_revision", "child_revision")
        identities = await case.bank.take(kinds)
        if damage == "omitted":
            kinds, identities = kinds[:1], identities[:1]
        elif damage == "duplicate":
            identities = (identities[0], identities[0])
        else:
            identities = tuple(reversed(identities))
        case.output.observed(kinds, identities, True)
        with pytest.raises(FamilyBlockUnavailable, match="reservation"):
            await case.bank.finish(case.output)


async def test_source_no_id_work_uses_bounded_freshness_windows(tmp_path):
    async with _case(tmp_path, maximum=16) as case:
        for _ in range(100):
            await case.control.checkpoint(rows=1)
        assert len(case.owner.calls) == 7
        assert all(not kinds for kinds, _ids in case.owner.calls)
        case.control.refresh_at = 0
        await case.control.checkpoint()
        assert len(case.owner.calls) == 8
        await case.bank.finish(case.output)


async def test_refill_and_spooling_leave_owner_transaction_closed(tmp_path, monkeypatch):
    async with _case(tmp_path) as case:
        frame = case.work.frame

        def no_sql_during_spill(*args, **kwargs):
            assert not case.owner.active
            return frame(*args, **kwargs)

        monkeypatch.setattr(case.work, "frame", no_sql_during_spill)
        kinds = ("family_revision", "root_revision", "pack")
        identities = await case.bank.take(kinds)
        case.output.observed(kinds, identities, True)
        await case.bank.finish(case.output)


@pytest.mark.parametrize("reason", ["fence", "prefix", "base", "commit"])
async def test_owner_refusal_cannot_publish_a_window_or_reservation(tmp_path, reason):
    async with _case(tmp_path) as case:
        case.owner.failure = RuntimeError(reason)
        with pytest.raises(RuntimeError, match=reason):
            await case.bank.take(("child_revision",))
        assert case.control.saved is None
        assert not any(queue.reserved for queue in case.bank.queues.values())
        assert _dispositions(case) == []


async def test_native_observer_refuses_missing_journal_before_any_sequence(monkeypatch):
    async def forbidden(*_args):
        pytest.fail("unprotected native authority must never be entered")

    monkeypatch.setattr(allocation, "_page_session", forbidden)
    with pytest.raises(ValueError, match="original protected journal"):
        await allocation._observe_ids(
            None, _context(), (), ("child_revision",), SourceAllocationLimits(_FACTS, 8, 8 * 96)
        )


async def test_initial_cpu_pass_preserves_absolute_deadline_and_cleans_only_destination(tmp_path, monkeypatch):
    async with _empty_scope_case(tmp_path, monkeypatch) as (context, registered):
        original_entries = tuple(allocation.read_retained_input(context.retained_input))
        request = replace(context.common.request, operation_deadline=time.monotonic() - 1)
        context = replace(context, common=replace(context.common, request=request))

        def forbidden_session():
            raise AssertionError("expired provisional work must not start native authority")

        with pytest.raises(TimeoutError, match="operation deadline"):
            await _produce_destination(
                forbidden_session, context, work=_empty_scope_work(tmp_path / "expired-destination", registered)
            )
        assert not (tmp_path / "expired-destination").exists()
        assert tuple(allocation.read_retained_input(context.retained_input)) == original_entries


class _CostSession:
    def __init__(self):
        self.info, self.new, self.dirty, self.deleted = {}, set(), set(), set()
        self.active, self.readonly, self.closed = False, False, False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_error):
        self.closed = True

    def in_transaction(self):
        return self.active

    @asynccontextmanager
    async def begin(self):
        self.active = True
        try:
            yield
        finally:
            self.active = False

    async def execute(self, statement):
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        self.readonly = True


@pytest.mark.parametrize("expired", [False, True])
async def test_cost_uses_saved_readonly_deadline_without_owner_refresh(tmp_path, monkeypatch, expired):
    async with _case(tmp_path) as case:
        await case.control.checkpoint()
        saved, session, late = case.control.saved, _CostSession(), []

        async def identity(current):
            assert current is session and current.readonly
            return (session,)

        async def prepare(current):
            assert current is session and current.readonly
            assert current.info[_WINDOW] is saved

        async def readonly(current):
            assert current is session and current.readonly

        async def measure(current, pages, *, limits, prepare):
            await prepare(current)
            assert len(pages) == 1 and limits is case.limits
            case.control.remembered(_PageWindow(saved.deadline + 60, 1000))
            late.append(True)
            return ("actual-cost-result",)

        def deadline(value):
            if value == saved.deadline and expired and late:
                raise TimeoutError("saved cost deadline")

        monkeypatch.setattr(reservations, "_read_transaction_identity", identity)
        monkeypatch.setattr(reservations, "_is_same_read_transaction", lambda before, after: before == after)
        monkeypatch.setattr(reservations, "_prepare_statement", prepare)
        monkeypatch.setattr(reservations, "require_readonly_snapshot", readonly)
        monkeypatch.setattr(reservations, "measure_work_pages", measure)
        monkeypatch.setattr(reservations, "_require_deadline", deadline)
        call = case.control.cost(lambda: session, (WorkCostPage(0, 1, 1, 1, 1, ()),), case.limits)
        if expired:
            with pytest.raises(TimeoutError, match="saved cost deadline"):
                await call
        else:
            assert await call == ("actual-cost-result",)
        assert len(case.owner.calls) == 1
        assert session.closed and not session.active
        await case.bank.finish(case.output)


async def test_cost_cancellation_drains_the_owned_read_transaction(tmp_path, monkeypatch):
    async with _case(tmp_path) as case:
        session, started, finished = _CostSession(), asyncio.Event(), []

        async def prepare(_current):
            case.work.current()

        async def measure(_current, _pages, *, limits, prepare):
            started.set()
            try:
                await asyncio.Future()
            finally:
                finished.append(True)

        async def identity(_current):
            return (session,)

        monkeypatch.setattr(reservations, "_read_transaction_identity", identity)
        monkeypatch.setattr(reservations, "_is_same_read_transaction", lambda before, after: before == after)
        monkeypatch.setattr(reservations, "_prepare_statement", prepare)
        monkeypatch.setattr(reservations, "require_readonly_snapshot", prepare)
        monkeypatch.setattr(reservations, "measure_work_pages", measure)
        task = asyncio.create_task(case.control.cost(lambda: session, (), case.limits))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished == [True] and session.closed and not session.active
        await case.bank.finish(case.output)
