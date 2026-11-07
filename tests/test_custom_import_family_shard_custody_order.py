# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real custody producers and journal bytes; native authority uses the existing double."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import msgpack
import pytest

from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_checkpoint as checkpoint
from process.custom_import import family_shard_metadata_journal as metadata_journal
from process.custom_import import family_shard_source_checkpoint as source_checkpoint
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_capture import PreparedCaptureSource
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.family_shard_route import route_capture
from tests.test_custom_import_family_shard_metadata_journal import _metadata, _snapshot, _source, _values
from tests.test_custom_import_family_shard_source_checkpoint import _stage_context
from tests.test_custom_import_family_shard_worker import _limits as worker_limits

pytestmark = pytest.mark.asyncio


async def _ready(tmp_path, case, source, journal, limits):
    """Seal genuine original metadata/tasks and append through the real journal writer."""
    facts = worker_limits().facts
    with route_capture(
        case[0], case[1].definition, root=tmp_path / "route", register_cleanup=lambda _path: None, limits=facts
    ) as shards:
        async with _metadata(case, source, journal, limits) as metadata:
            expected = _values(metadata)
            ready = source_checkpoint.prepare_source_ready(
                PreparedCaptureSource(case[4], case[0], source.source),
                source,
                shards,
                metadata,
                _stage_context(case, build_id=journal.build_id, family_id=source.snapshot_family_id),
                PlanWork(tmp_path / "ready", facts, lambda _path: None),
                worker_limits=worker_limits(),
                metadata_work=PlanWork(tmp_path / "ready-work", facts, lambda _path: None),
            )
    snapshot = await _snapshot(case, source, journal)
    async with allocation._page_session(None, case[1], journal.build_id) as (session, _build):
        await source_checkpoint.append_source_ready_checkpoint(
            session,
            _stage_context(case, build_id=journal.build_id, family_id=source.snapshot_family_id),
            ready,
            storage=snapshot,
            expected_head=snapshot.head,
        )
    return ready, expected


async def _append(case, source, journal, record):
    snapshot = await _snapshot(case, source, journal)
    async with allocation._page_session(None, case[1], journal.build_id) as (session, _build):
        await checkpoint.append_owner_checkpoint(
            session,
            case[1],
            journal.build_id,
            source.snapshot_family_id,
            None,
            storage=snapshot,
            record=record,
            expected_head=snapshot.head,
        )


@pytest.mark.parametrize("empty", (False, True))
async def test_source_ready_preserves_exact_metadata_replay_without_new_allocations(tmp_path, monkeypatch, empty):
    async with _source(tmp_path, monkeypatch, empty=empty) as (case, source, journal, native, limits):
        ready, expected = await _ready(tmp_path, case, source, journal, limits)
        before = await _snapshot(case, source, journal)
        calls = (tuple(native.allocations), tuple(native.family_calls))
        descriptor = Path(ready.root) / "source-ready.mpk"
        original_bytes = descriptor.read_bytes()
        async with _metadata(case, source, journal, limits) as metadata:
            assert _values(metadata) == expected
        assert (await _snapshot(case, source, journal)).head == before.head
        assert (tuple(native.allocations), tuple(native.family_calls)) == calls
        assert descriptor.read_bytes() == original_bytes
        record = next(before.records())[1]
        assert source_checkpoint._checkpoint_payload(record)[1] == ready.root
        assert source_checkpoint._ready_record(before, None) == record


def _damaged_record(record, problem):
    if problem == "ordinal":
        return replace(record, ordinal=1)
    if problem == "input":
        return replace(record, input_sha256=b"x" * 32)
    if problem == "commitments":
        return replace(record, commitments=record.commitments + (("unknown", b"x" * 32),))
    cells = list(msgpack.unpackb(record.payload, raw=False, use_list=False))
    match problem:
        case "unknown":
            cells[0] = "custom-import/unsupported-custody/v1"
        case "relative_root":
            cells[1] = "relative-root"
        case "directory_type":
            cells[2] = (False, *cells[2][1:])
        case "file_stamp":
            cells[3] = (*cells[3][:-1], -1)
        case "extra_cell":
            cells.append("extra")
        case _:
            raise AssertionError("unknown damage case")
    return replace(record, payload=msgpack.packb(cells, use_bin_type=True))


@pytest.mark.parametrize(
    "problem",
    ("ordinal", "input", "commitments", "relative_root", "directory_type", "file_stamp", "extra_cell", "unknown"),
)
async def test_tampered_custody_is_not_skipped_before_metadata_exposure(tmp_path, monkeypatch, problem):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        await _ready(tmp_path, case, source, journal, limits)
        snapshot = await _snapshot(case, source, journal)
        record = _damaged_record(next(snapshot.records())[1], problem)
        await _append(case, source, journal, record)
        calls = tuple(native.family_calls)
        message = "unsupported artifact" if problem == "unknown" else "SOURCE-ready checkpoint envelope differs"
        with pytest.raises(FamilyBlockUnavailable, match=message):
            async with _metadata(case, source, journal, limits):
                pytest.fail("damaged custody was treated as ignorable metadata")
        assert tuple(native.family_calls) == calls


@pytest.mark.parametrize("problem", ("duplicate", "before_query"))
async def test_source_ready_must_be_unique_and_after_completed_metadata(tmp_path, monkeypatch, problem):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        await _ready(tmp_path, case, source, journal, limits)
        records = list((await _snapshot(case, source, journal)).records())
        await _append(case, source, journal, records[0 if problem == "duplicate" else 1][1])
        calls = tuple(native.family_calls)
        with pytest.raises(FamilyBlockUnavailable, match="SOURCE-ready checkpoint is duplicate or misplaced"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("misordered custody escaped")
        assert tuple(native.family_calls) == calls


async def test_source_ready_cannot_stand_in_for_missing_metadata_prefix(tmp_path, monkeypatch):
    """Exercise control classification with omitted records, without claiming native replay."""
    async with _source(tmp_path, monkeypatch) as (case, source, journal, _native, limits):
        await _ready(tmp_path, case, source, journal, limits)
        records = list((await _snapshot(case, source, journal)).records())
        prefix_items = [records[0], *(row for row in records if row[1].kind == "source_allocation")]
        snapshot = SimpleNamespace(records=lambda **_options: iter(prefix_items))
        run = SimpleNamespace(source=source, journal=journal, work=SimpleNamespace(current=lambda: None))
        with pytest.raises(FamilyBlockUnavailable, match="complete original SOURCE prefix"):
            list(metadata_journal._checked_records(run, snapshot))


@pytest.mark.parametrize("kind", ("family_allocation", "artifact"))
async def test_source_ready_does_not_bypass_original_metadata_file_digests(tmp_path, monkeypatch, kind):
    async with _source(tmp_path, monkeypatch) as (case, source, journal, native, limits):
        await _ready(tmp_path, case, source, journal, limits)
        records = list((await _snapshot(case, source, journal)).records())
        record = next(record for _head, record in records[1:] if record.kind == kind)
        cells = msgpack.unpackb(record.payload, raw=False, use_list=False)
        path = journal.root / "vectors" / cells[4]
        path.chmod(0o600)
        path.write_bytes(path.read_bytes() + b"x")
        path.chmod(0o400)
        calls = tuple(native.family_calls)
        with pytest.raises(FamilyBlockUnavailable, match="metadata journal file custody differs"):
            async with _metadata(case, source, journal, limits):
                pytest.fail("SOURCE-ready hid damaged original metadata")
        assert tuple(native.family_calls) == calls
