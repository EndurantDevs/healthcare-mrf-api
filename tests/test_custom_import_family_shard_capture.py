# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real sealed-byte preparation and cancellation-safe owned file stages."""

import asyncio
import threading
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_capture as capture
from process.custom_import.continuation_runtime import BuildContinuationContext
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_input import ShardInputLimits, cleanup_spool
from process.custom_import.family_shard_source_plan import cleanup_source_plan
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_shard_checkpoint import _Native
from tests.test_custom_import_family_shard_input import _capture_input, _child, _root
from tests.test_custom_import_family_shard_source_plan import _assert_oracle


def _limits():
    return capture.SourcePreparationLimits(
        ShardInputLimits(32 * 1024**2, 4, 1024), SourceFactLimits(32 * 1024**2, 1024**2, 4096)
    )


async def _inputs(monkeypatch, records_by_stream, *, close_failure=False):
    definition, capture_input, parts = _capture_input(records_by_stream)
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)
    request = _request(
        dataset_id=capture_input.identity.dataset_id,
        definition_revision_id=capture_input.identity.definition_revision_id,
        schema_revision_id=capture_input.identity.schema_revision_id,
        definition=definition,
        page_row_limit=2,
        page_byte_limit=16384,
    )

    async def load(*_args):
        return capture_input

    @asynccontextmanager
    async def retained_parts(_factory, _request, _source, work, path):
        with path.open("xb") as output:

            async def values():
                for part in parts:
                    work.frame(
                        output, (registry.stream_slots[part.receipt.stream_id], part.ordinal, part.record_count), path
                    )
                    yield part

            yield values()
        if close_failure:
            raise RuntimeError("synthetic retained reader close failed")

    @asynccontextmanager
    async def renewal(*_args):
        yield

    monkeypatch.setattr(capture, "_load_source", load)
    monkeypatch.setattr(capture, "_capture_parts", retained_parts)
    monkeypatch.setattr(capture, "_renew_while_reading", renewal)
    return request, registry, capture_input, parts


@pytest.mark.parametrize("empty", [False, True])
async def test_preparation_matches_original_source_pages_and_retains_completed_inputs(tmp_path, monkeypatch, empty):
    rows_by_stream = (
        {"root_source": [[], []], "detail_source": [[]]}
        if empty
        else {
            "root_source": [[], [_root(), _root("1234567893"), _root(npi=None)], []],
            "detail_source": [[_child(), _child(key=None)], []],
        }
    )
    request, registry, capture_input, parts = await _inputs(monkeypatch, rows_by_stream)
    registered_paths = []
    prepared_source = await capture.prepare_captured_source(
        None, request, registry, 17, root=tmp_path, register_cleanup=registered_paths.append, limits=_limits()
    )
    try:
        part_coordinates = tuple(
            (registry.stream_slots[part.receipt.stream_id], part.ordinal, part.record_count) for part in parts
        )
        observed = _assert_oracle(
            prepared_source.plan, (prepared_source.partition, request, registry, part_coordinates, capture_input, parts)
        )
        assert prepared_source.source == capture_input
        assert prepared_source.plan.counts[0] == (0 if empty else 5)
        assert len(observed) == prepared_source.plan.counts[1]
        assert prepared_source.plan.counts[-1] == len(parts)
        assert registered_paths == [tmp_path / "part-metadata", tmp_path / "input", tmp_path / "plan"]
        assert not (tmp_path / "part-metadata").exists()
        assert (tmp_path / "input").exists() and (tmp_path / "plan").exists()
    finally:
        cleanup_source_plan(prepared_source.plan)
        cleanup_spool(prepared_source.partition)
    assert not list(tmp_path.iterdir())


async def test_reader_close_failure_discards_exact_incomplete_input(tmp_path, monkeypatch):
    request, registry, _, _ = await _inputs(
        monkeypatch, {"root_source": [[_root()]], "detail_source": [[_child()]]}, close_failure=True
    )
    with pytest.raises(RuntimeError, match="reader close failed"):
        await capture.prepare_captured_source(
            None, request, registry, 17, root=tmp_path, register_cleanup=lambda _path: None, limits=_limits()
        )
    assert not list(tmp_path.iterdir())


async def test_continuation_does_not_replay_or_reallocate_source(tmp_path):
    request = replace(_request(), continuation=BuildContinuationContext(41, 51, b"p" * 32))
    with pytest.raises(FamilyBlockUnavailable, match="retained SOURCE artifacts"):
        await capture.prepare_captured_source(
            None, request, None, 17, root=tmp_path, register_cleanup=None, limits=_limits()
        )
    assert not list(tmp_path.iterdir())


async def test_repeated_cancellation_waits_for_file_thread_before_cleanup(monkeypatch):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    cleanup_calls = []

    def build(*, check):
        entered.set()
        assert release.wait(5)
        finished.set()
        return "completed-plan"

    def cleanup(plan):
        assert finished.is_set()
        cleanup_calls.append(plan)

    monkeypatch.setattr(capture, "cleanup_source_plan", cleanup)
    pending = asyncio.create_task(capture._prepare_plan(build))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        pending.cancel()
        await asyncio.sleep(0)
        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done() and not cleanup_calls
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert cleanup_calls == ["completed-plan"]


async def test_operational_plan_failure_does_not_invent_a_cleanup_failure(monkeypatch):
    failure = OSError("synthetic source plan spool budget exhausted")

    def refuse(*, check):
        raise failure

    def cleanup(_plan):
        pytest.fail("a failed plan is already responsible for its incomplete cleanup")

    monkeypatch.setattr(capture, "cleanup_source_plan", cleanup)
    with pytest.raises(OSError) as raised:
        await capture._prepare_plan(refuse)
    assert raised.value is failure
    assert not getattr(failure, "_custom_import_retry_blocked", False)
    assert failure.__cause__ is None


async def test_cancel_signals_the_running_plan_and_keeps_original_cancellation(monkeypatch):
    entered, can_check = threading.Event(), threading.Event()

    def prepare(*, check):
        entered.set()
        assert can_check.wait(5)
        check()
        pytest.fail("canceled SOURCE planning continued")

    def cleanup(_plan):
        pytest.fail("interrupted SOURCE planning owns its incomplete cleanup")

    monkeypatch.setattr(capture, "cleanup_source_plan", cleanup)
    pending = asyncio.create_task(capture._prepare_plan(prepare))
    assert await asyncio.to_thread(entered.wait, 5)
    pending.cancel()
    await asyncio.sleep(0)
    can_check.set()
    with pytest.raises(asyncio.CancelledError) as failure:
        await pending
    assert not getattr(failure.value, "_custom_import_retry_blocked", False)


def _retained_header_boundary(monkeypatch, *, phase="closed", continued=False):
    """Keep real native-scope identity checks; double only authority and header transport."""
    native = _Native(monkeypatch, continued=continued)
    native.committed["phase"] = phase
    native.build.phase = "source" if phase == "loading" else "output"
    native.header_reads = []
    header = SimpleNamespace(capture_bundle_id=native.capture.capture_bundle_id)

    async def read(_session, request):
        assert request is native.request
        native.header_reads.append(header)
        return header

    def forbidden(*_args, **_kwargs):
        pytest.fail("immutable header reopening entered capture or SOURCE planning")

    monkeypatch.setattr(capture, "_page_session", native.page)
    monkeypatch.setattr(capture, "_read_source_header", read)
    for name in ("_capture_parts", "partition_capture", "build_source_plan"):
        monkeypatch.setattr(capture, name, forbidden)
    return native, header


@pytest.mark.parametrize("phase", ("loading", "closed", "verified", "logged"))
async def test_retained_header_read_uses_current_existing_build_authority(monkeypatch, phase):
    native, header = _retained_header_boundary(monkeypatch, phase=phase)
    observed = await capture._load_retained_source(None, native.request, 7, 17, native.layout)
    assert observed is header
    assert native.header_reads == [header]
    assert native.transactions == native.authority_calls == 1


@pytest.mark.parametrize("problem", ("authority", "fence", "token", "base", "capture", "layout", "build", "phase"))
async def test_retained_header_read_refuses_changed_authority_or_identity(monkeypatch, problem):
    native, _header = _retained_header_boundary(monkeypatch)
    match problem:
        case "authority":
            native.live = False
        case "fence":
            native.build.producing_fence += 1
        case "token":
            native.build.producing_token_sha256 = b"x" * 32
        case "base":
            native.build.base_pointer_version += 1
        case "capture":
            native.capture.capture_state = "open"
        case "layout":
            native.committed["collection_slots"] = {"unknown": 99}
        case "build":
            native.build.build_id = 8
        case "phase":
            native.committed["phase"] = "published"
    with pytest.raises((FamilyBlockUnavailable, CandidateRunnerError, LeaseAuthorityLost)):
        await capture._load_retained_source(None, native.request, 7, 17, native.layout)
    assert native.authority_calls == 1 and not native.header_reads


@pytest.mark.parametrize("phase", ("graph", "output", "verifying", "verified"))
async def test_continued_header_read_uses_current_epoch_for_completed_source(monkeypatch, phase):
    native, header = _retained_header_boundary(monkeypatch, continued=True)
    native.build.phase = phase
    native.build.source_frozen_at = object()
    assert await capture._load_retained_source(None, native.request, 7, 17, native.layout) is header
    assert native.transactions == native.authority_calls == 1
    assert native.header_reads == [header]


@pytest.mark.parametrize("problem", ("authority", "build", "source", "phase"))
async def test_continued_header_read_refuses_unfinished_or_changed_epoch(monkeypatch, problem):
    native, _header = _retained_header_boundary(monkeypatch, continued=True)
    native.build.source_frozen_at = None if problem == "source" else object()
    match problem:
        case "authority":
            native.live = False
        case "build":
            native.request = replace(native.request, continuation=BuildContinuationContext(9, 11, b"p" * 32))
        case "phase":
            native.build.phase = "source"
    with pytest.raises((FamilyBlockUnavailable, CandidateRunnerError, LeaseAuthorityLost)):
        await capture._load_retained_source(None, native.request, 7, 17, native.layout)
    assert native.authority_calls == 1 and not native.header_reads


async def test_fresh_header_loading_remains_loading_only(monkeypatch):
    request = _request()
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)

    @asynccontextmanager
    async def page(_factory, supplied):
        assert supplied is request
        yield object(), None

    async def candidate(_session, supplied, family_id, _layout, phases):
        assert supplied is request and family_id == 17 and phases == {"loading"}
        raise FamilyBlockUnavailable("closed candidate refuses fresh preparation")

    async def forbidden(*_args):
        pytest.fail("fresh preparation bypassed its loading candidate guard")

    monkeypatch.setattr(capture, "_page_session", page)
    monkeypatch.setattr(capture, "_candidate", candidate)
    monkeypatch.setattr(capture, "_read_source_header", forbidden)
    with pytest.raises(FamilyBlockUnavailable, match="closed candidate"):
        await capture._load_source(None, request, registry, 17)
