# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep the shared commit order, real result identity and cancellation-safe finalization."""

import asyncio
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_ordinary_serving_publication as publication


async def test_profile_finishes_before_address_and_journal_use_their_original_meter(monkeypatch):
    event_names = []
    capacity, admission = object(), SimpleNamespace(captured_inputs=SimpleNamespace(initial_targets=None))
    capacity_var = ContextVar("publication_profile_capacity", default=capacity)
    fhir = SimpleNamespace(
        db=SimpleNamespace(status=AsyncMock()),
        _schema=lambda: "fixture",
        _PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION=capacity_var,
    )
    prepared = SimpleNamespace(fhir=fhir, nonprofile_admission=admission, address=object())

    async def profile(*args):
        assert capacity_var.get() is capacity
        event_names.append("profile_finished")

    async def address(*args, **kwargs):
        assert capacity_var.get() is None
        assert publication.preparation._ACTIVE.get() is admission
        event_names.append("address")

    async def finish_profile(prepared):
        assert capacity_var.get() is capacity and event_names == ["profile_finished"]
        event_names.append("profile_sealed")

    async def append(*args):
        await address()
        event_names[-1] = "journal"
        return "event", "serving"

    monkeypatch.setattr(publication, "_lock_publication", AsyncMock(return_value=(1, 2)))
    monkeypatch.setattr(publication.journal, "capture_state", AsyncMock(return_value={"roots": {}}))
    monkeypatch.setattr(publication, "_apply_profile", profile)
    monkeypatch.setattr(publication, "finish_profile_cutover", finish_profile)
    monkeypatch.setattr(publication, "_apply_overlay", address)
    monkeypatch.setattr(publication, "publish_prepared_entity_address_generation", address)
    monkeypatch.setattr(publication, "ordinary_address_receipt_callbacks", Mock(return_value=object()))
    monkeypatch.setattr(publication, "_append_publication", append)
    admission.assert_cutover_complete = AsyncMock(side_effect=address)
    assert await publication._apply_publication(prepared, {}, "worker", "generation", None) == ("event", "serving")
    assert event_names == ["profile_finished", "profile_sealed", "address", "address", "journal", "address"]
    assert capacity_var.get() is capacity


@pytest.fixture
def owner(monkeypatch):
    event_names = []
    state = SimpleNamespace(bound=False, error=None, session=SimpleNamespace(execute=AsyncMock()))

    @asynccontextmanager
    async def transaction(database):
        state.bound = True
        event_names.append("owner_enter")
        try:
            yield state
            if state.error is not None:
                raise state.error
        finally:
            state.bound = False
            event_names.append("owner_terminal")

    @asynccontextmanager
    async def scope(*args, **kwargs):
        yield

    def consume(fhir, outcome, failure, **kwargs):
        assert outcome is state and not state.bound and failure is None
        event_names.append("consume")

    fhir = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: object() if state.bound else None),
        _schema=lambda: "fixture",
        _validated_admission_run_id=lambda *args: "run_" + "1" * 32,
        profile_control_custody=SimpleNamespace(transaction=scope, consume_owned_groups=consume),
    )
    admission = SimpleNamespace(publication=scope)
    prepared = SimpleNamespace(fhir=fhir, nonprofile_admission=admission, execution=object(), metrics={})
    monkeypatch.setattr(publication, "registry_owned_wal_transaction", transaction)
    monkeypatch.setattr(publication, "ordinary_terminal_fence", scope)
    monkeypatch.setattr(publication.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
    apply = AsyncMock(return_value=({"sha256": "ab" * 32}, "actual_serving"))
    finalize = AsyncMock(return_value=True)
    monkeypatch.setattr(publication, "_apply_publication", apply)
    monkeypatch.setattr(publication, "_has_finalized_commit", finalize)
    expected_by_family = {family: {} for family in publication.journal.documents.FAMILIES}
    return SimpleNamespace(
        state=state,
        events=event_names,
        prepared=prepared,
        expected=expected_by_family,
        apply=apply,
        finalize=finalize,
    )


async def test_commit_consumes_only_after_terminal_native_owner_and_verified_event(owner):
    result = await publication.commit_ordinary_serving(owner.prepared, owner.expected, run_id="run_" + "1" * 32)
    assert result is owner.prepared.metrics
    assert owner.events == ["owner_enter", "owner_terminal", "consume"]
    owner.finalize.assert_awaited_once()
    assert owner.apply.await_args.args[2:4] == ("11111111-1111-1111-1111-111111111111",) * 2


@pytest.mark.parametrize("failure", ["body", "commit_ack", "unproved"])
async def test_failure_or_missing_commit_proof_is_never_converted_to_success(owner, failure):
    if failure == "body":
        owner.apply.side_effect = RuntimeError("body_failed")
    elif failure == "commit_ack":
        owner.state.error = RuntimeError("commit_ack_lost")
    else:
        owner.finalize.return_value = False
    expected = {"body": "body_failed", "commit_ack": "commit_ack_lost", "unproved": "commit_unproved"}[failure]
    with pytest.raises(RuntimeError, match=expected):
        await publication.commit_ordinary_serving(owner.prepared, owner.expected, run_id="run_" + "1" * 32)
    assert owner.state.bound is False
    if failure == "body":
        owner.finalize.assert_not_awaited()
    else:
        owner.finalize.assert_awaited_once()


async def test_repeated_cancellation_drains_finalizer_before_candidate_scope_can_close(owner, monkeypatch):
    started, finish = asyncio.Event(), asyncio.Event()
    terminal_flags = []

    async def has_finalized_commit(*args):
        started.set()
        await finish.wait()
        terminal_flags.append(True)
        return True

    monkeypatch.setattr(publication, "_has_finalized_commit", has_finalized_commit)
    task = asyncio.create_task(
        publication.commit_ordinary_serving(owner.prepared, owner.expected, run_id="run_" + "1" * 32)
    )
    async with asyncio.timeout(5):
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert terminal_flags == [] and not task.done()
        finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert terminal_flags == [True]


@pytest.mark.parametrize("rows", [[], [{"result": {"generation": 2}}], [{"result": {"generation": 1}}]])
@pytest.mark.parametrize("serialized", [False, True])
async def test_historical_profile_reuse_requires_exact_terminal_result(monkeypatch, rows, serialized):
    expected_by_field = {"profile_generation_id": "original", "generation": 1}
    for row in rows:
        row["result"]["profile_generation_id"] = "original"
    matches = bool(rows) and rows[0]["result"] == expected_by_field
    if serialized:
        rows = [{"result": json.dumps(row["result"])} for row in rows]
    fhir = SimpleNamespace(
        db=SimpleNamespace(all=AsyncMock(return_value=rows)),
        _schema=lambda: "fixture",
        _unscoped_qt=lambda schema, table: schema + "." + table,
        ImportRun=SimpleNamespace(__tablename__="import_run"),
        PROFILE_SELECTION_RESULT_METRIC="profile_selection_result",
        _pagination_checkpoint_row_mapping=lambda row: row,
    )
    monkeypatch.setattr(publication.journal.documents, "_profile_result", lambda result: result)
    if matches:
        assert await publication._historical_profile_result(fhir, expected_by_field) == expected_by_field
    else:
        with pytest.raises(ValueError, match="common_profile_history_changed"):
            await publication._historical_profile_result(fhir, expected_by_field)
    assert "status='succeeded' AND finished_at IS NOT NULL" in fhir.db.all.await_args.args[0]
