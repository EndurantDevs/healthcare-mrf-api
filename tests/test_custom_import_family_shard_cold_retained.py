# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Strict retained control suffixes and exact native-observer response shapes.

These synthetic control records are parser/transport cases, never native commit
or genuine process-recovery acceptance. Real native cases are prepared separately.
"""

import hashlib
import os
from dataclasses import replace
from types import SimpleNamespace

import pytest

from process.custom_import import family_block_store as store
from process.custom_import import family_shard_events_journal as events
from process.custom_import import family_shard_retained_checkpoint as retained
from process.custom_import import family_shard_retained_journal as vectors
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_shard_checkpoint import OwnerCheckpointHead, OwnerCheckpointRecord
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_output import _pack_row

_LIMITS = SourceFactLimits(16 * 1024**2, 1024**2, 1024**2)
_COMMITMENTS = (("admission_events", b"e" * 32), ("source_allocations", b"a" * 32), ("source_plan", b"p" * 32))


@pytest.mark.parametrize("remaining", [512 * 1024, 8 * 1024**2])
def test_new_encoded_output_keeps_only_remaining_original_allowance_before_creation(tmp_path, remaining):
    from process.custom_import import family_shard_retained_stage as stage
    from process.custom_import.family_block_codec import FamilyBlockLimits
    from process.custom_import.family_shard_facts import SourceFactBudgetExceeded
    from process.custom_import.family_shard_plan import PlanWork

    limits = FamilyBlockLimits(32 * 1024**2, 1024**2, 65536, 1024**2)
    root = tmp_path / "new-retained"
    registered_paths = []
    owner = SimpleNamespace(
        root=root,
        current=lambda: None,
        source=SimpleNamespace(limits=SimpleNamespace(worker=SimpleNamespace(output=limits))),
        work=PlanWork(root, SourceFactLimits(remaining, 4096, 4096), registered_paths.append),
    )
    if remaining < limits.max_record_bytes:
        with pytest.raises(SourceFactBudgetExceeded, match="original output allowance"):
            stage._output_work(owner)
    else:
        work = stage._output_work(owner)
        filesystem = os.statvfs(tmp_path)
        unit = max(512, filesystem.f_frsize or filesystem.f_bsize)
        assert work.limits == replace(limits, max_spool_bytes=remaining - 2 * unit)
        assert work.root == root / "encoded" and work.check is owner.current
    assert not root.exists() and registered_paths == []


def _bundle(stage):
    values = tuple(
        ((1, 2, 1, 2) if index in (7, 8) else (index,)) if index < retained._REQUIRED[stage] else None
        for index in range(10)
    )
    payload = _pack_row(
        (
            retained._CONTRACT,
            b"r" * 32,
            "/private-owner",
            ("/private-owner/retained", (1, 2, 448, 501)),
            (1, 2, 1, 2),
            values,
            stage,
        )
    )
    digest = hashlib.sha256(payload).digest()
    return OwnerCheckpointRecord(
        "artifact", stage, digest, (("retained_bundle", digest), ("source_ready", b"r" * 32)), payload
    )


def _vector(ordinal, *, complete=False):
    contract, role = (
        (retained._COMPLETE, "retained_destination") if complete else (retained._VECTOR, "retained_vectors")
    )
    payload = _pack_row((contract, *((1,) * (5 if complete else 7))))
    return OwnerCheckpointRecord(
        "retained_allocation",
        ordinal,
        b"i" * 32,
        ((role, b"v" * 32), ("source_allocations", b"a" * 32), ("source_plan", b"p" * 32)),
        payload,
    )


def _snapshot(stage, *, partial=False):
    stamp = "2026-11-06T12:34:56+00:00"
    source = OwnerCheckpointRecord("source_allocation", 0, b"s" * 32)
    start = OwnerCheckpointRecord(
        "admission_allocation", 0, b"i" * 32, _COMMITMENTS, _pack_row((events._CONTRACT, "start", stamp, ()))
    )
    complete = replace(start, ordinal=1, payload=_pack_row((events._CONTRACT, "complete", stamp, ())))
    ordered_records = [source, start, complete, _bundle(0)]
    if stage:
        ordered_records.append(_bundle(1))
    if stage >= 2 or partial:
        ordered_records.append(_vector(0))
        if not partial:
            ordered_records.append(_vector(1, complete=True))
    ordered_records.extend(_bundle(index) for index in range(2, stage + 1))
    records = tuple(
        (OwnerCheckpointHead(index + 1, bytes([index + 1]) * 32), record)
        for index, record in reversed(tuple(enumerate(ordered_records)))
    )
    return SimpleNamespace(
        head=records[0][0], limits=_LIMITS, records=lambda **_options: (row for row in records)
    ), records


@pytest.mark.parametrize("stage", range(len(retained._STAGES)))
def test_retained_suffix_preserves_whole_head_and_original_source_prefix(stage):
    snapshot, records = _snapshot(stage)
    view = retained.SourcePrefix(snapshot)
    assert view.head == snapshot.head
    assert tuple(view.records()) == records[-3:]
    assert retained.latest(snapshot, None) == retained._bundle(_bundle(stage), _LIMITS)


def test_unfinished_retained_vector_stays_visible_only_to_its_original_allocator():
    snapshot, records = _snapshot(1, partial=True)
    assert tuple(retained.SourcePrefix(snapshot).records()) == records[-3:]
    allocator_records = tuple(retained.SourcePrefix(snapshot, keep_vectors=True).records())
    assert [record.kind for _head, record in allocator_records] == [
        "retained_allocation",
        "admission_allocation",
        "admission_allocation",
        "source_allocation",
    ]
    assert allocator_records[0] == records[0]


def test_fresh_admission_metadata_reader_accepts_the_same_typed_retained_suffix():
    snapshot, records = _snapshot(10)
    first = events.AdmissionSourcePrefix(snapshot, 32)
    assert tuple(first.records()) == records[-1:]
    # This is the second fresh-read path used by source_stage._reopen_metadata.
    run = SimpleNamespace(
        journal=SimpleNamespace(control_limits=_LIMITS),
        work=SimpleNamespace(current=lambda: None),
        commitments=_COMMITMENTS,
        limits=SimpleNamespace(maximum_batch_records=32),
    )
    metadata = events._MetadataPrefix(run, vectors._Prefix(run, snapshot))
    assert metadata.head == snapshot.head and tuple(metadata.records()) == records[-1:]
    assert [row[2] for row in events._event_records(run, vectors._Prefix(run, snapshot))] == [1, 0]
    events.require_completed_admission(snapshot, 32, None)


@pytest.mark.parametrize(
    "damage",
    [
        "duplicate",
        "gap",
        "missing-start",
        "misplaced",
        "vector-gap",
        "contract",
        "scope",
        "missing-input",
        "missing-complete",
        "duplicate-complete",
        "wrong-complete",
    ],
)
def test_retained_suffix_refuses_reordered_unknown_or_rebound_custody(damage):
    snapshot, original_records = _snapshot(10)
    changed_records = list(original_records)
    match damage:
        case "duplicate":
            changed_records.insert(1, changed_records[0])
        case "gap":
            del changed_records[1]
        case "missing-start":
            changed_records = [checkpoint_row for checkpoint_row in changed_records if checkpoint_row[1] != _bundle(0)]
        case "misplaced":
            changed_records.append(changed_records.pop(0))
        case "vector-gap":
            changed_records = [
                checkpoint_row
                for checkpoint_row in changed_records
                if checkpoint_row[1].kind != "retained_allocation" or checkpoint_row[1].ordinal != 0
            ]
        case "contract":
            changed_records[0] = changed_records[0][0], replace(changed_records[0][1], payload=_pack_row(("unknown",)))
        case "missing-input":
            changed_records = [pair for pair in changed_records if pair[1] != _bundle(1)]
        case "missing-complete":
            changed_records = [pair for pair in changed_records if pair[1] != _vector(1, complete=True)]
        case "duplicate-complete":
            changed_records = [
                (head, _vector(0, complete=True) if checkpoint_record == _vector(0) else checkpoint_record)
                for head, checkpoint_record in changed_records
            ]
        case "wrong-complete":
            changed_records = [
                (head, _vector(1) if checkpoint_record == _vector(1, complete=True) else checkpoint_record)
                for head, checkpoint_record in changed_records
            ]
        case _:
            cells = list(retained._bundle(changed_records[0][1], _LIMITS))
            cells[1] = b"x" * 32
            bundle_payload = _pack_row(tuple(cells))
            digest = hashlib.sha256(bundle_payload).digest()
            changed_records[0] = (
                changed_records[0][0],
                replace(
                    changed_records[0][1],
                    input_sha256=digest,
                    payload=bundle_payload,
                    commitments=(("retained_bundle", digest), ("source_ready", cells[1])),
                ),
            )
    altered = SimpleNamespace(head=snapshot.head, limits=_LIMITS, records=lambda **_options: iter(changed_records))
    with pytest.raises(FamilyBlockUnavailable):
        retained.latest(altered, None)


@pytest.mark.parametrize("invalid", [(1, 2, 2, 0), (1, 2, True, 0), (1, 2, -1, 0)])
def test_retained_bundle_refuses_invalid_protected_home_baseline(invalid):
    record = _bundle(8)
    cells = list(retained._bundle(record, _LIMITS))
    state_cells = list(cells[5])
    state_cells[7] = invalid
    cells[5] = tuple(state_cells)
    payload = _pack_row(tuple(cells))
    digest = hashlib.sha256(payload).digest()
    damaged = replace(
        record,
        payload=payload,
        input_sha256=digest,
        commitments=(("retained_bundle", digest), ("source_ready", cells[1])),
    )
    with pytest.raises(FamilyBlockUnavailable, match="home counts"):
        retained._bundle(damaged, _LIMITS)


def test_cold_vector_recovery_refuses_a_missing_original_custody_bundle():
    snapshot, records = _snapshot(1, partial=True)
    vector_records = tuple(row for row in records if row[1].kind != "artifact")
    unsupported = SimpleNamespace(
        head=snapshot.head, limits=_LIMITS, records=lambda **_options: (row for row in vector_records)
    )
    with pytest.raises(FamilyBlockUnavailable, match="lack their protected original bundle"):
        retained.latest(unsupported, None)


@pytest.mark.parametrize("tampered", [False, True])
async def test_adopted_destination_still_checks_actual_original_vector_files(tmp_path, monkeypatch, tampered):
    from process.custom_import.family_shard_plan import PlanWork
    from tests import test_custom_import_family_shard_retained_journal as fixture

    async with fixture._case(tmp_path, monkeypatch) as case:
        destination = await fixture._allocate(case)
        snapshot = await vectors.checkpoint.read_owner_checkpoint(
            None,
            case.context.common.request,
            case.journal.build_id,
            case.native.source.snapshot_family_id,
            case.native.layout,
            root=case.journal.checkpoints,
            limits=case.journal.control_limits,
        )
        owner = SimpleNamespace(
            context=case.context,
            source_journal=case.journal,
            destination=destination,
            source=SimpleNamespace(allocated=case.native.source, limits=SimpleNamespace(allocation=case.native.limits)),
            root=tmp_path / "borrowed-retained",
            current=lambda: None,
            work=PlanWork(tmp_path / "borrowed-retained", case.native.limits.spool, lambda _path: None),
        )
        vector_path = next((case.journal.root / "vectors").glob("retained-*.mpk"))
        original_bytes = vector_path.read_bytes()
        calls = tuple(case.native.vector_calls)
        try:
            if tampered:
                vector_path.chmod(0o600)
                vector_path.write_bytes(original_bytes[:-1] + bytes([original_bytes[-1] ^ 1]))
                vector_path.chmod(0o400)
                with pytest.raises(FamilyBlockUnavailable, match="custody|SHA"):
                    retained._check_native_vectors(snapshot, owner)
            else:
                retained._check_native_vectors(snapshot, owner)
            assert tuple(case.native.vector_calls) == calls and vector_path.exists()
        finally:
            if tampered:
                vector_path.chmod(0o600)
                vector_path.write_bytes(original_bytes)
                vector_path.chmod(0o400)


@pytest.mark.parametrize(
    "observed",
    [
        [(1, 101, 1, 0), (2, 202, 1, 1)],
        [(1, 101, 2, 2), (2, 202, 1, 1)],
        [(1, 303, 1, 1), (2, 202, 1, 1)],
        [(1, 101, 1, 1)],
    ],
)
async def test_home_census_refuses_unrelated_or_incomplete_exact_id_coverage(monkeypatch, observed):
    statements = []

    async def execute(_session, statement):
        statements.append(statement)
        return SimpleNamespace(all=lambda: observed)

    monkeypatch.setattr(store, "_execute", execute)
    owner = SimpleNamespace(
        context=SimpleNamespace(common=SimpleNamespace(source=SimpleNamespace(snapshot_family_id=77)))
    )
    with pytest.raises(FamilyBlockUnavailable):
        await retained._home_batch(None, owner, ((1, 101), (2, 202)))
    assert len(statements) == 1
    parameters = statements[0].compile().params
    assert 77 in parameters.values() and 101 in parameters.values() and 202 in parameters.values()


async def test_home_census_accepts_only_exact_missing_or_same_candidate_ids(monkeypatch):
    async def execute(_session, _statement):
        return SimpleNamespace(all=lambda: [(1, 101, 0, 0), (2, 202, 1, 1)])

    monkeypatch.setattr(store, "_execute", execute)
    owner = SimpleNamespace(
        context=SimpleNamespace(common=SimpleNamespace(source=SimpleNamespace(snapshot_family_id=77)))
    )
    assert await retained._home_batch(None, owner, ((1, 101), (2, 202))) == (0, 1)


@pytest.mark.parametrize("damage", [None, "missing-bundle", "wrong-record", "wrong-previous", "extra-revision"])
def test_atomic_destination_completion_requires_its_exact_custody_successor(damage):
    previous = OwnerCheckpointHead(7, b"p" * 32)
    complete, bundle = _vector(1, complete=True), _bundle(2)
    rows = [
        (OwnerCheckpointHead(9, b"n" * 32), bundle),
        (OwnerCheckpointHead(8, b"c" * 32), complete),
        (previous, _bundle(1)),
    ]
    if damage == "missing-bundle":
        del rows[0]
    elif damage == "wrong-record":
        rows[1] = rows[1][0], replace(complete, input_sha256=b"x" * 32)
    elif damage == "wrong-previous":
        rows[2] = OwnerCheckpointHead(6, b"q" * 32), rows[2][1]
    elif damage == "extra-revision":
        rows[0] = OwnerCheckpointHead(10, b"z" * 32), rows[0][1]
    snapshot = SimpleNamespace(records=lambda **_options: (row for row in rows))
    run = object.__new__(vectors._Run)
    run.work = SimpleNamespace(current=lambda: None)
    if damage is None:
        assert run.committed_completion(snapshot, previous, complete, bundle) == complete
    else:
        with pytest.raises(FamilyBlockUnavailable, match="atomic custody"):
            run.committed_completion(snapshot, previous, complete, bundle)


@pytest.mark.parametrize("damage", [None, "bytes", "hardlink", "cancel"])
async def test_closed_plan_adoption_uses_actual_complete_original_file(tmp_path, monkeypatch, damage):
    from pathlib import Path

    from process.custom_import.family_shard_plan import read_family_plan
    from process.custom_import.runner_types import CancellationRequested
    from tests import test_custom_import_family_shard_original_validation as fixture

    async with fixture._case(tmp_path, monkeypatch) as native:
        original_root = Path(native.plan.root)
        moved_root = tmp_path / "final-plan"
        original_root.rename(moved_root)
        descriptor = replace(native.plan, root=str(moved_root))
        path = moved_root / "family-plan.mpk"
        original = path.read_bytes()
        linked = tmp_path / "unrelated-link"

        def check():
            if damage == "cancel":
                raise CancellationRequested("adoption stopped")

        try:
            if damage == "bytes":
                path.chmod(0o600)
                path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
                path.chmod(0o400)
            elif damage == "hardlink":
                linked.hardlink_to(path)
            if damage is None:
                assert (
                    retained._closed_descriptor(descriptor, path.name, read_family_plan, tmp_path, check) == descriptor
                )
            else:
                error = CancellationRequested if damage == "cancel" else (ValueError, FamilyBlockUnavailable)
                with pytest.raises(error):
                    retained._closed_descriptor(descriptor, path.name, read_family_plan, tmp_path, check)
            assert path.exists()
        finally:
            if linked.exists():
                linked.unlink()
            if path.read_bytes() != original:
                path.chmod(0o600)
                path.write_bytes(original)
                path.chmod(0o400)
            moved_root.rename(original_root)


def test_encoded_adoption_reuses_actual_file_and_refuses_post_checkpoint_change(tmp_path):
    from types import SimpleNamespace

    from tests import test_custom_import_family_shard_output as fixture

    root = tmp_path / "retained"
    root.mkdir(mode=0o700)
    layout, family, _children, keys = fixture.prepared(17)
    with fixture.build(root, layout, family, keys, name="encoded") as original:
        cells = retained._encoded(original, lambda: None)
        owner = SimpleNamespace(
            source=SimpleNamespace(
                owner_lock=SimpleNamespace(root=tmp_path),
                limits=SimpleNamespace(worker=SimpleNamespace(output=fixture.limits())),
            ),
            current=lambda: None,
        )
        reopened = retained._reopen_encoded(cells, owner)
        try:
            assert reopened.path == original.path and reopened.sha256 == original.sha256
            assert list(reopened.batches(maximum_rows=5, maximum_bytes=1024**2)) == list(
                original.batches(maximum_rows=5, maximum_bytes=1024**2)
            )
        finally:
            reopened.close()
        assert original.path.exists()
        encoded_bytes = original.path.read_bytes()
        original.path.chmod(0o600)
        original.path.write_bytes(bytes([encoded_bytes[0] ^ 1]) + encoded_bytes[1:])
        original.path.chmod(0o400)
        with pytest.raises(FamilyBlockUnavailable, match="changed after checkpointing"):
            retained._encoded(original, lambda: None)


def _move_artifacts(case, owner_root):
    from pathlib import Path
    from uuid import uuid4

    stage = owner_root / ("retained-" + uuid4().hex)
    stage.mkdir(mode=0o700)
    directories = []
    parent = owner_root
    for name in ("source", "allocation-journal", "outputs"):
        parent /= name
        if not parent.exists():
            parent.mkdir(mode=0o700)
            directories.append(parent)
    paths = (
        (Path(case.context.retained_input.root), stage / "input"),
        (Path(case.destination.root), parent / ("retained-" + uuid4().hex)),
        (case.output._work.root, stage / "encoded"),
    )
    for original, moved in paths:
        original.rename(moved)
    return stage, directories, paths


async def test_bundle_hydrates_closed_original_files_without_copying_values(tmp_path, monkeypatch):
    from dataclasses import astuple

    from process.custom_import.family_shard_homes import SourceHomeCounts
    from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
    from tests import test_custom_import_family_shard_retained_storage_validation as fixture

    async with fixture._case(tmp_path, monkeypatch) as case:
        stored = await fixture._prove(tmp_path, case)
        stage, directories, paths = _move_artifacts(case, tmp_path)
        original_output_path, original_paths = case.output.path, dict(case.output._work.paths)
        assert not (tmp_path / ".owner-job.lock").exists()
        lock = acquire_owner_job_lock(tmp_path, identity=(tmp_path.stat().st_dev, tmp_path.stat().st_ino))
        try:
            original_input = replace(case.context.retained_input, root=str(paths[0][1]))
            destination = replace(case.destination, root=str(paths[1][1]))
            case.output._work.root, case.output.path = paths[2][1], paths[2][1] / "output.mpk"
            case.output._work.paths = {case.output.path: next(iter(original_paths.values()))}
            source_owner = SimpleNamespace(
                owner_lock=lock,
                ready_record=SimpleNamespace(input_sha256=b"r" * 32),
                homes=SourceHomeCounts(0, 0, 0, 0),
                limits=SimpleNamespace(worker=SimpleNamespace(output=fixture._output_work(tmp_path).limits)),
            )
            original = SimpleNamespace(
                source=source_owner,
                root=stage,
                final_plan=case.context.final_plan,
                retained_input=original_input,
                destination=destination,
                original=case.comparison,
                encoded=case.output,
                stored=stored,
                common_receipt=None,
                homes=None,
                pack_tail=None,
                current=lambda: None,
            )
            custody_record = retained._record(original, "stored")
            reopened = SimpleNamespace(source=source_owner, final_plan=original.final_plan, current=lambda: None)
            retained._hydrate(reopened, retained._bundle(custody_record, _LIMITS))
            assert astuple(reopened.retained_input) == astuple(original_input)
            assert astuple(reopened.destination) == astuple(destination)
            assert reopened.original == case.comparison and reopened.stored == stored
            assert reopened.encoded.path == case.output.path and reopened.encoded.sha256 == case.output.sha256
            assert reopened.encoded.path.stat().st_ino == case.output.path.stat().st_ino
            reopened.encoded.close()
            assert case.output.path.exists()
        finally:
            lock.close()
            (tmp_path / ".owner-job.lock").unlink()
            case.output._work.root, case.output.path = paths[2][0], original_output_path
            case.output._work.paths = original_paths
            for original, moved in reversed(paths):
                moved.rename(original)
            stage.rmdir()
            for directory in reversed(directories):
                directory.rmdir()
