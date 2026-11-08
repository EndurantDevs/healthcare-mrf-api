# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Physical projection batches retain logical bounds and exact commit positions."""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.custom_import import query_projection_build as build
from process.custom_import import query_projection_digest as digest
from process.custom_import import query_projection_worker as worker
from process.custom_import.admission_worker import AdmissionTransportError
from process.custom_import.build_source import _PageWindow
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_query_projection_evidence import _fixture_digest
from tests.test_custom_import_query_projection_storage import _definition


def _manifest(phase="root"):
    return SimpleNamespace(
        phase=phase,
        after_entity_binding_id=0,
        after_selected_value=None,
        after_child_key_sha256=None,
        index_count=0,
        root_count=0,
        child_count=0,
        verified_root_count=0,
        verified_child_count=0,
        digested_root_count=0,
        digested_child_count=0,
        digest_chain_sha256=None,
    )


@pytest.fixture
def batch_case(monkeypatch):
    request = _request(definition=_definition(), page_row_limit=256, page_byte_limit=8 * 1024**2)
    session = SimpleNamespace(info={build._WINDOW: _PageWindow(100.0, 1_000)})
    advance = AsyncMock()
    monkeypatch.setattr(build, "time", SimpleNamespace(monotonic=lambda: 10.0))
    monkeypatch.setattr(build, "_advance_projection", advance)
    return SimpleNamespace(request=request, session=session, advance=advance, manifest=_manifest())


@pytest.mark.parametrize("phase", ("root", "child", "digest_root", "digest_child"))
async def test_coalescing_retains_logical_request(batch_case, phase):
    batch_case.manifest.phase = phase
    await build._advance_projection_batch(batch_case.session, batch_case.request, object(), batch_case.manifest)
    assert batch_case.advance.await_count == 32
    assert all(call.args[1] is batch_case.request for call in batch_case.advance.await_args_list)
    assert (batch_case.request.page_row_limit, batch_case.request.page_byte_limit) == (256, 8 * 1024**2)


@pytest.mark.parametrize(
    ("limit_name", "limit", "expected"),
    (
        ("MAX_BATCH_ROWS", 768, 3),
        ("MAX_BATCH_BYTES", 16 * 1024**2, 2),
        ("_MAX_LOGICAL_PAGES", 4, 4),
    ),
)
async def test_physical_bounds_reserve_each_page(monkeypatch, batch_case, limit_name, limit, expected):
    monkeypatch.setattr(build, limit_name, limit)
    await build._advance_projection_batch(batch_case.session, batch_case.request, object(), batch_case.manifest)
    assert batch_case.advance.await_count == expected


async def test_full_byte_envelope_allows_one_page(batch_case):
    request = replace(batch_case.request, page_byte_limit=build.MAX_BATCH_BYTES)
    await build._advance_projection_batch(batch_case.session, request, object(), batch_case.manifest)
    batch_case.advance.assert_awaited_once()


async def test_empty_windows_consume_bounded_work(batch_case):
    async def skip_window(_session, request, _build, manifest):
        manifest.after_entity_binding_id += request.page_row_limit
        manifest.after_selected_value = 2**63 - 1

    batch_case.advance.side_effect = skip_window
    await build._advance_projection_batch(batch_case.session, batch_case.request, object(), batch_case.manifest)
    assert batch_case.advance.await_count == 32
    assert batch_case.manifest.after_entity_binding_id == 8192
    assert batch_case.manifest.root_count == batch_case.manifest.child_count == 0


@pytest.mark.parametrize(
    ("phase", "following"),
    (
        ("root", "child"),
        ("child", "indexes"),
        ("digest_root", "digest_child"),
        ("digest_child", "complete"),
        ("indexes", "indexes"),
        ("verify_root", "verify_root"),
        ("verify_child", "verify_child"),
        ("complete", "complete"),
    ),
)
async def test_batch_stops_at_phase_boundary(batch_case, phase, following):
    batch_case.manifest.phase = phase

    async def advance_once(_session, _request, _build, manifest):
        manifest.phase = following
        if phase == "indexes":
            manifest.index_count += 1

    batch_case.advance.side_effect = advance_once
    await build._advance_projection_batch(batch_case.session, batch_case.request, object(), batch_case.manifest)
    batch_case.advance.assert_awaited_once()
    assert batch_case.manifest.index_count == int(phase == "indexes")


@pytest.mark.parametrize(
    ("fence_deadline", "ticks"),
    (
        (100.0, (10.0, 10.5, 11.0)),
        (11.0, (10.0, 10.25, 10.5)),
        (100.0, (10.0, 12.0)),
    ),
)
async def test_next_page_respects_soft_deadline(monkeypatch, batch_case, fence_deadline, ticks):
    clock_ticks = iter(ticks)
    monkeypatch.setattr(build, "time", SimpleNamespace(monotonic=lambda: next(clock_ticks)))
    batch_case.session.info[build._WINDOW] = _PageWindow(fence_deadline, 1_000)
    await build._advance_projection_batch(batch_case.session, batch_case.request, object(), batch_case.manifest)
    assert batch_case.advance.await_count == len(ticks) - 1


@pytest.fixture
def committed_case(monkeypatch, batch_case):
    batch_case.events, batch_case.committed = [], None

    @asynccontextmanager
    async def transaction(_factory, _request, _build_id):
        batch_case.events.append("open")
        yield batch_case.session, SimpleNamespace(generation_id=20, capture_bundle_id=9)
        batch_case.events.append("final-authority")
        if getattr(batch_case, "authority_lost", False):
            raise LeaseAuthorityLost("synthetic final fence loss")
        batch_case.committed = build.ProjectionProgress.from_manifest(batch_case.manifest)
        batch_case.events.append("commit")

    async def advance(_session, request, _build, manifest):
        batch_case.events.append("page")
        manifest.root_count += request.page_row_limit
        manifest.after_entity_binding_id += request.page_row_limit
        manifest.after_selected_value = 0

    monkeypatch.setattr(build, "_page_session", transaction)
    monkeypatch.setattr(build, "initialize_projection", AsyncMock(return_value=batch_case.manifest))
    monkeypatch.setattr(build, "_flush_page", AsyncMock())
    monkeypatch.setattr(build, "_MAX_LOGICAL_PAGES", 2)
    batch_case.advance.side_effect = advance
    return batch_case


async def test_receipt_follows_one_physical_commit(committed_case):
    before = build.ProjectionProgress.from_manifest(committed_case.manifest)
    receipt = await build.projection_step(None, committed_case.request, 8, expected=before)
    assert committed_case.events == ["open", "page", "page", "final-authority", "commit"]
    assert receipt.before == before and receipt.after == committed_case.committed
    assert receipt.after.root_count == 512
    pins = worker._pins(8, committed_case.request.execution_id, committed_case.request.fence, before)
    assert (
        worker.validate_receipt(asdict(receipt), pins, generation_id=20, capture_bundle_id=9, before=before) == receipt
    )


async def test_stale_cursor_cannot_start_batch(committed_case):
    before = build.ProjectionProgress.from_manifest(committed_case.manifest)
    with pytest.raises(build.ProjectionCursorConflict):
        await build.projection_step(None, committed_case.request, 8, expected=replace(before, root_count=1))
    committed_case.advance.assert_not_awaited()
    assert committed_case.committed is None


async def test_permit_checked_before_any_page(monkeypatch, committed_case):
    verify = AsyncMock(side_effect=LeaseAuthorityLost("synthetic permit rejection"))
    monkeypatch.setattr(build, "_verify_writer_permit", verify)
    with pytest.raises(LeaseAuthorityLost):
        await build.projection_step(None, committed_case.request, 8, permit=object())
    verify.assert_awaited_once()
    committed_case.advance.assert_not_awaited()
    assert committed_case.committed is None


async def test_permit_covers_one_physical_transaction(monkeypatch, committed_case):
    async def verify(_session, request, permit, purpose):
        assert request is committed_case.request and permit == "synthetic permit"
        assert purpose == (build.ProjectionPermit, build.PROJECTION_PERMIT_CONTRACT, build.PROJECTION_PATH)
        committed_case.events.append("permit")

    check = AsyncMock(side_effect=verify)
    monkeypatch.setattr(build, "_verify_writer_permit", check)
    await build.projection_step(None, committed_case.request, 8, permit="synthetic permit")
    check.assert_awaited_once()
    assert committed_case.events == ["open", "permit", "page", "page", "final-authority", "commit"]


async def test_final_fence_loss_cannot_return_receipt(committed_case):
    committed_case.authority_lost = True
    with pytest.raises(LeaseAuthorityLost):
        await build.projection_step(None, committed_case.request, 8)
    assert committed_case.events == ["open", "page", "page", "final-authority"]
    assert committed_case.committed is None


async def test_later_page_failure_cannot_commit(committed_case):
    advance = committed_case.advance.side_effect

    async def fail_later(*arguments):
        await advance(*arguments)
        if committed_case.advance.await_count == 2:
            raise CandidateRunnerError("synthetic second-page failure")

    committed_case.advance.side_effect = fail_later
    with pytest.raises(CandidateRunnerError):
        await build.projection_step(None, committed_case.request, 8)
    assert committed_case.events == ["open", "page", "page"]
    assert committed_case.committed is None


async def test_coalesced_unknown_ack_never_resends(monkeypatch, batch_case):
    before = build.ProjectionProgress.from_manifest(batch_case.manifest)
    after = replace(before, after_entity_binding_id=512, after_selected_value=0, root_count=512)
    observations = [worker.RetainedProjectionProgress(4, 8, 1, 20, 9, progress) for progress in (before, after)]
    observe = AsyncMock(side_effect=observations)
    send = AsyncMock(side_effect=AdmissionTransportError("synthetic lost acknowledgement"))
    monkeypatch.setattr(worker, "_retained_progress", observe)
    transport = SimpleNamespace(bind_request=lambda request: request, send=send)
    with pytest.raises(worker.ProjectionReconciliationRequired) as caught:
        await worker.projection_next_batch(None, batch_case.request, 8, transport)
    assert caught.value.progress == observations[1]
    assert caught.value.pins == worker._pins(8, 4, 1, before)
    assert observe.await_count == 2
    send.assert_awaited_once()


async def _digest_batches(monkeypatch, request, session, logical_pages):
    manifest = _manifest("digest_root")
    manifest.root_count, manifest.child_count = 2, 3
    manifest.capability_sha256 = _fixture_digest(request.definition).finish().capability_sha256

    async def contexts(_session, _request, _context, retained, *, child, after):
        count = retained.digested_child_count if child else retained.digested_root_count
        if count == (3 if child else 2):
            return []
        context_dict = dict(entity_binding_id=count + 1, selected_value=0, canonical_entity=f"entity_{count}")
        if child:
            context_dict.update(
                canonical_entity="entity_a",
                canonical_child_key=f'["service_{count}"]',
                child_key_sha256=bytes([count + 1]) * 32,
                field_2=None,
                field_3=None,
            )
        else:
            context_dict["field_1"] = None
        return [context_dict]

    async def finish(_session, _request, _build, retained):
        retained.phase = "complete"

    monkeypatch.setattr(build, "_MAX_LOGICAL_PAGES", logical_pages)
    monkeypatch.setattr(digest, "_source_context", AsyncMock(return_value=object()))
    monkeypatch.setattr(digest, "_digest_page", contexts)
    monkeypatch.setattr(build, "_finish_projection", finish)
    for _ in range(8):
        await build._advance_projection_batch(session, request, object(), manifest)
        if manifest.phase == "complete":
            return deepcopy(manifest), digest.retained_projection_digest(manifest, request.definition).finish()
    pytest.fail("bounded digest did not finish")


async def test_digest_batching_preserves_exact_checkpoint(monkeypatch):
    request = _request(definition=_definition(), page_row_limit=1)
    session = SimpleNamespace(info={build._WINDOW: _PageWindow(100.0, 1_000)})
    monkeypatch.setattr(build, "time", SimpleNamespace(monotonic=lambda: 10.0))
    single_state, single_digest = await _digest_batches(monkeypatch, request, session, 1)
    batched_state, batched_digest = await _digest_batches(monkeypatch, request, session, 32)
    assert vars(single_state) == vars(batched_state)
    assert single_digest == batched_digest == _fixture_digest(request.definition).finish()
    assert (batched_digest.root_count, batched_digest.child_count) == (2, 3)
