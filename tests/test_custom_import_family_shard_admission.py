# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent complete SOURCE/admission joins without native acceptance claims."""

import hashlib
import json
import pickle
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_admission as audit
from process.custom_import import family_shard_allocate as allocation
from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_route_merge as route_batches
from process.custom_import import family_shard_worker as worker
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from process.custom_import.family_shard_input_records import decode_record, encode_record
from process.custom_import.family_shard_route import prepare_routed_families, route_capture
from process.custom_import.family_shard_source_plan import cleanup_source_plan, read_source_plan
from process.custom_import.runner_codec import digest_text
from process.custom_import.runner_types import CandidateRegistry
from tests.test_custom_import_family_shard_allocate import _allocate, _native_boundary
from tests.test_custom_import_family_shard_source_plan import _build, _case, _child, _root
from tests.test_custom_import_family_shard_worker import _limits, _projected_definition


@pytest.fixture
def admission_limits():
    return SourceFactLimits(64 * 1024**2, 1024**2, 512)


def _registry(case):
    slot_by_stream = {stream.stream_id: slot for slot, stream in enumerate(case[1].definition.source_streams, 1)}
    root_slot = next(
        slot for slot, stream in enumerate(case[1].definition.source_streams, 1) if stream.child_collection is None
    )
    registry = CandidateRegistry({"details": 7}, slot_by_stream, root_slot)
    parts = tuple((slot_by_stream[part.receipt.stream_id], part.ordinal, part.record_count) for part in case[5])
    return case[:2] + (registry, parts) + case[4:]


def _metadata(allocated):
    records_by_coordinate, query_keys_by_pair = {}, {}
    for pack, pack_id, roots, revisions, _rejections in allocation.read_source_allocations(allocated):
        for index, record in enumerate(pack.records):
            if revisions[index] is not None:
                records_by_coordinate[(record.stream_slot, record.source_ordinal)] = (
                    revisions[index],
                    pack_id,
                    roots[index],
                )
                if record.child_key is not None:
                    query_keys_by_pair[(digest_text("child-key", record.child_key), record.child_key)] = None
    query_keys_by_pair = {key: (rank, 1) for rank, key in enumerate(sorted(query_keys_by_pair), 1)}
    return records_by_coordinate, query_keys_by_pair


def _worker_plan(shard, case, allocated, root):
    definition = case[1].definition
    records_by_coordinate, query_keys = _metadata(allocated)
    families_by_key = {}
    with prepare_routed_families(
        shard, definition, root=root, register_cleanup=lambda _path: None, limits=_limits().facts
    ) as families:
        for index, family in enumerate(families, 1):
            if family.typed_key is not None and family.source_root is not None:
                coordinate = family.source_root.stream_slot, family.source_root.source_ordinal
                if coordinate in records_by_coordinate:
                    families_by_key[family.typed_key] = (
                        7000 + shard.shard_id * 100 + index,
                        9000 + shard.shard_id * 100 + index,
                    )
    return worker.SourceShardPlan(
        shard,
        definition.canonical,
        case[2].child_collection_slots,
        case[1].dataset_id,
        case[1].schema_revision_id,
        allocated.snapshot_family_id,
        records_by_coordinate,
        families_by_key,
        query_keys,
        True,
    )


@asynccontextmanager
async def _inputs(
    tmp_path, monkeypatch, rows_by_stream=None, *, definition=None, complete_scope=True, source_binding=True
):
    case = _registry(
        await _case(
            tmp_path, rows_by_stream, definition=definition or _projected_definition(), source_binding=source_binding
        )
    )
    source_plan = _build(tmp_path, case)
    _native_boundary(monkeypatch, source_plan)  # Explicit native-ID boundary fake only.
    allocated = await _allocate(tmp_path, case[1], source_plan)
    outputs = []
    try:
        with route_capture(
            case[0],
            case[1].definition,
            root=tmp_path / "route",
            register_cleanup=lambda _path: None,
            limits=_limits().facts,
        ) as shards:
            for shard in shards:
                plan = replace(
                    _worker_plan(shard, case, allocated, tmp_path / f"prepare-{shard.shard_id}"),
                    complete_scope=complete_scope,
                )
                outputs.append(
                    worker.build_source_shard(
                        plan,
                        root=tmp_path / f"worker-{shard.shard_id}",
                        register_cleanup=lambda _path: None,
                        limits=_limits(),
                    )
                )
            yield case, allocated, outputs
    finally:
        for output in outputs:
            output.close()
        allocation.cleanup_source_allocations(allocated)
        cleanup_source_plan(source_plan)
        cleanup_spool(case[0])


def _build_audit(tmp_path, case, allocated, outputs, limits, *, check=None, complete_scope=True):
    registered_roots = []
    result = audit.build_admission_output(
        allocated,
        case[1].definition,
        tuple(reversed(outputs)),
        work=audit.AdmissionWork(tmp_path / "admission", limits, registered_roots.append, check),
        complete_scope=complete_scope,
    )
    assert registered_roots == [tmp_path / "admission"]
    return result


def _assert_original_allocations(joined_occurrences, allocated):
    expected_allocations = [
        (
            source_record,
            pack_id,
            roots[index],
            revisions[index],
            rejections[index],
            pack.initial_rejection_ordinals[index],
        )
        for pack, pack_id, roots, revisions, rejections in allocation.read_source_allocations(allocated)
        for index, source_record in enumerate(pack.records)
    ]
    assert len(joined_occurrences) == len(expected_allocations) == 3
    for joined, (source_record, pack, root, revision, rejection, ordinal) in zip(
        joined_occurrences, expected_allocations, strict=True
    ):
        assert joined.encoded_record == encode_record(source_record)
        assert (
            joined.pack_id,
            joined.root_record_id,
            joined.revision_id,
            joined.initial_rejection_id,
            joined.initial_rejection_ordinal,
        ) == (pack, root, revision, rejection, ordinal)
        assert joined.collection_slot == (0 if source_record.collection_slot == 0 else 7)


@pytest.mark.asyncio
async def test_reference_route_fallback(tmp_path, monkeypatch, admission_limits):
    monkeypatch.setattr(route_batches, "backend", lambda: None)
    rows_by_stream = {
        "root_source": [[_root(), _root(npi=None)]],
        "detail_source": [[_child(amount="1.2300"), _child("orphan", "b")]],
    }
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        limits = replace(admission_limits, memory_bytes=1024**2)
        baseline = _build_audit(tmp_path, case, allocated, outputs, limits)
        try:
            expected = (Path(baseline.root) / audit._FILE).read_bytes()
            counts = baseline.counts
        finally:
            audit.cleanup_admission_output(baseline)
        calls = []

        def declined_sort(mode, frames, *_bounds):
            calls.append(mode)
            assert mode in {"raw", "family"}
            for frame in frames:
                order, source_value = facts._unpack(frame)
                key = facts._raw_order(source_value) if mode == "raw" else (0, facts._family_order(source_value))
                assert order == key
            return None

        monkeypatch.setattr(route_batches, "backend", lambda: (declined_sort, lambda *_arguments: None))
        actual = _build_audit(tmp_path, case, allocated, outputs, limits)
        try:
            assert calls == ["raw", "family"]
            assert actual.counts == counts
            assert (Path(actual.root) / audit._FILE).read_bytes() == expected
            assert len(list(audit.read_admission_occurrences(actual))) == allocated.source.counts[0]
        finally:
            audit.cleanup_admission_output(actual)
        assert not (tmp_path / "admission").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("collapse", (False, True))
async def test_all_original_rows_allocations_facts_and_decisions_survive_join(
    tmp_path, monkeypatch, admission_limits, collapse
):
    rows_by_stream = {"root_source": [[_root()]], "detail_source": [[_child(amount="1.2300"), _child(amount="1.2300")]]}
    async with _inputs(tmp_path, monkeypatch, rows_by_stream, definition=_projected_definition(collapse=collapse)) as (
        case,
        allocated,
        outputs,
    ):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            assert pickle.loads(pickle.dumps(descriptor)) == descriptor
            joined_occurrences = list(audit.read_admission_occurrences(descriptor))
            assert len(joined_occurrences) == descriptor.counts_by_kind["records"] == 3
            _assert_original_allocations(joined_occurrences, allocated)
            child_occurrences = [entry for entry in joined_occurrences if entry.record.collection_slot]
            assert all(
                child.record.normalized_values(case[1].definition, case[1].definition.source_streams[1])["amount"]
                == "1.2300"
                for child in child_occurrences
            )
            families = list(audit.read_admission_families(descriptor))
            assert families[0][1]["selection"] == ("source" if collapse else "absent")
            assert bool(descriptor.counts_by_kind["admission_events"]) is not collapse
            assert descriptor.complete_scope is True
        finally:
            audit.cleanup_admission_output(descriptor)
        assert not Path(descriptor.root).exists()


@pytest.mark.asyncio
async def test_source_events_keep_ids_ordinals_evidence_and_admission_zero_is_null(
    tmp_path, monkeypatch, admission_limits
):
    rows_by_stream = {
        "root_source": [[_root(), _root(npi=None)]],
        "detail_source": [[_child(amount="1"), _child(amount="2"), _child(key=None)]],
    }
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            events = list(audit.read_admission_events(descriptor))
            assert [event.ordinal for event in events] == list(range(len(events)))
            source_events = [event for event in events if event.origin == "source"]
            admission_events = [event for event in events if event.origin == "admission"]
            assert events == source_events + admission_events
            assert len(source_events) == allocated.source.counts[4] == 2
            originals_by_coordinate = {
                joined.coordinate: joined for joined in audit.read_admission_occurrences(descriptor)
            }
            for event in source_events:
                original = originals_by_coordinate[event.coordinate]
                landing_by_name = dict(zip(audit.LandingRow._fields[1:], original.landing, strict=True))
                assert (event.rejection_id, event.ordinal, event.canonical_evidence) == (
                    original.initial_rejection_id,
                    original.initial_rejection_ordinal,
                    landing_by_name["canonical_evidence"],
                )
                assert event.source_ordinal == event.coordinate[1]
                assert event.collection_slot == (None if original.collection_slot == 0 else 7)
            assert admission_events and all(
                event.rejection_id is None and event.canonical_evidence == "{}" for event in admission_events
            )
            assert next(event for event in admission_events if event.coordinate[1] == 0).source_ordinal is None
            assert all(event.collection_slot == 7 for event in admission_events)
        finally:
            audit.cleanup_admission_output(descriptor)


@pytest.mark.asyncio
async def test_child_first_earliest_resolution_differs_from_rejection_ordinal_order(
    tmp_path, monkeypatch, admission_limits
):
    document = json.loads(_projected_definition().canonical)
    document["streams"].reverse()
    definition = CustomImportDefinition.from_mapping(document)
    rows_by_stream = {"root_source": [[_root(score=None)]], "detail_source": [[_child(amount="1"), _child(amount="2")]]}
    async with _inputs(tmp_path, monkeypatch, rows_by_stream, definition=definition) as (case, allocated, outputs):
        result = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            events = list(audit.read_admission_events(result))
            assert events[0].origin == "source" and events[0].coordinate == (2, 0)
            assert events[0].source_ordinal == 0 and events[0].collection_slot is None
            resolutions_by_target = {
                resolution.target: resolution.event for resolution in audit.read_admission_resolutions(result)
            }
            assert resolutions_by_target[(2, 0)].origin == "admission"
            assert resolutions_by_target[(2, 0)].coordinate == (1, 0)
            assert resolutions_by_target[(2, 0)].ordinal == 1
            assert resolutions_by_target[(2, 0)].rejection_id is None
            root = next(entry for entry in audit.read_admission_occurrences(result) if entry.coordinate == (2, 0))
            assert root.initial_rejection_id == events[0].rejection_id
            assert root.decision.event is None and root.decision.error_code == root.record.rejection_code
        finally:
            audit.cleanup_admission_output(result)


def _rewrite_worker(output, mutate):
    entries = list(output.audit_rows())
    mutate(entries)
    output.path.chmod(0o600)
    with output.path.open("wb") as target:
        for entry in entries:
            packed = audit._pack_row(entry)
            target.write(audit._LENGTH.pack(len(packed)))
            target.write(packed)
    output.path.chmod(0o400)
    output._identity = output.path.stat()
    output.sha256 = hashlib.sha256(output.path.read_bytes()).digest()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ("facts", "decision", "bytes", "native", "family", "missing", "extra", "eof", "scope", "complete_scope")
)
async def test_self_consistent_worker_checksum_never_substitutes_for_original_source(
    tmp_path, monkeypatch, admission_limits, damage
):
    async with _inputs(tmp_path, monkeypatch, {"root_source": [[_root()]], "detail_source": [[_child()]]}) as (
        case,
        allocated,
        outputs,
    ):
        worker_output = next(output for output in outputs if output.counts["records"])

        def mutate(entries):
            occurrence_index = next(index for index, entry in enumerate(entries) if entry[0] == "occurrence")
            cells = list(entries[occurrence_index])
            match damage:
                case "facts":
                    cells[3] = dict(cells[3], raw_root_count=cells[3]["raw_root_count"] + 1)
                case "decision":
                    cells[4] = dict(cells[4], code="duplicate_root_key")
                case "bytes":
                    source_record = decode_record(cells[2])
                    cells[2] = encode_record(replace(source_record, row_ordinal=source_record.row_ordinal + 1))
                case "native":
                    cells[5] = (cells[5][0] + 1, *cells[5][1:])
                case "family":
                    index = next(index for index, entry in enumerate(entries) if entry[0] == "family")
                    entries[index] = ("family", dict(entries[index][1], source_root=(1, 999)))
                case "missing":
                    entries.pop(occurrence_index)
                    return
                case "extra":
                    entries.insert(occurrence_index, entries[occurrence_index])
                    return
                case "eof":
                    entries.pop()
                    return
                case "scope":
                    entries[0] = ("worker", dict(entries[0][1], snapshot_family_id=999))
                case "complete_scope":
                    entries[0] = ("worker", dict(entries[0][1], complete_scope=False))
            entries[occurrence_index] = tuple(cells)

        _rewrite_worker(worker_output, mutate)
        with pytest.raises(ValueError):
            _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        assert not (tmp_path / "admission").exists()
        assert len(list(read_source_plan(allocated.source))) == allocated.source.counts[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("cancel", "budget", "missing_worker", "duplicate_worker"))
async def test_incomplete_admission_never_publishes_and_preserves_inputs(
    tmp_path, monkeypatch, admission_limits, failure
):
    async with _inputs(tmp_path, monkeypatch, {"root_source": [[_root()]], "detail_source": [[_child()] * 6]}) as (
        case,
        allocated,
        outputs,
    ):
        count_by_phase = {"check": 0}

        def check():
            count_by_phase["check"] += 1
            if failure == "cancel" and count_by_phase["check"] == 60:
                raise RuntimeError("owner cancelled")

        if failure == "missing_worker":
            outputs = outputs[:-1]
        elif failure == "duplicate_worker":
            outputs = [outputs[0], *outputs[:-1]]
        limits = SourceFactLimits(4096, 2048, 256) if failure == "budget" else admission_limits
        with pytest.raises((ValueError, RuntimeError, SourceFactBudgetExceeded)):
            _build_audit(tmp_path, case, allocated, outputs, limits, check=check)
        assert not (tmp_path / "admission").exists()
        assert len(list(allocation.read_source_allocations(allocated))) == allocated.source.counts[1]


@pytest.mark.asyncio
async def test_empty_complete_source_keeps_every_worker_and_true_eof(tmp_path, monkeypatch, admission_limits):
    async with _inputs(tmp_path, monkeypatch, {"root_source": [[]], "detail_source": [[]]}) as (
        case,
        allocated,
        outputs,
    ):
        result = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            assert result.counts_by_kind["workers"] == len(outputs) > 0
            assert result.counts_by_kind["records"] == 0
            assert list(audit.read_admission_occurrences(result)) == []
            assert list(audit.read_admission_families(result)) == []
            assert list(audit.read_admission_events(result)) == []
            assert list(audit.read_admission_resolutions(result)) == []
        finally:
            audit.cleanup_admission_output(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("refresh_mode", ("snapshot", "upsert"))
async def test_capture_eof_preserves_explicit_incomplete_scope(tmp_path, monkeypatch, admission_limits, refresh_mode):
    document = json.loads(_projected_definition().canonical)
    document["refresh_mode"] = refresh_mode
    definition = CustomImportDefinition.from_mapping(document)
    async with _inputs(tmp_path, monkeypatch, definition=definition, complete_scope=False) as (
        case,
        allocated,
        outputs,
    ):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits, complete_scope=False)
        try:
            assert descriptor.complete_scope is False
            assert descriptor.counts_by_kind["records"] == 2
            assert next(audit.read_admission_families(descriptor))[1]["selection"] == (
                "source" if refresh_mode == "upsert" else "error"
            )
        finally:
            audit.cleanup_admission_output(descriptor)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ("sha", "eof", "trailing", "inode"))
async def test_output_replay_prefix_cannot_complete_with_changed_bytes_eof_or_custody(
    tmp_path, monkeypatch, admission_limits, damage
):
    async with _inputs(tmp_path, monkeypatch) as (case, allocated, outputs):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        original = descriptor
        path = Path(descriptor.root, audit._FILE)
        saved = path.read_bytes()
        path.chmod(0o600)
        if damage == "sha":
            descriptor = replace(descriptor, sha256=b"x" * 32)
        elif damage == "inode":
            backup = tmp_path / "original-audit"
            path.rename(backup)
            path.write_bytes(saved)
        else:
            footer = audit._pack_row(("complete", descriptor.counts))
            changed = saved[: -(8 + len(footer))] if damage == "eof" else saved + audit._LENGTH.pack(1) + b"\xc0"
            path.write_bytes(changed)
            descriptor = replace(descriptor, file_bytes=len(changed), sha256=hashlib.sha256(changed).digest())
        accepted_receipts = []
        try:
            with pytest.raises((ValueError, TypeError)):
                list(audit.read_admission_occurrences(descriptor))
                accepted_receipts.append(descriptor.counts)
            assert accepted_receipts == []
        finally:
            if damage == "inode":
                path.unlink()
                backup.rename(path)
            else:
                path.write_bytes(saved)
            audit.cleanup_admission_output(original)


@pytest.mark.asyncio
async def test_initially_rejected_invalid_lane_and_eliminated_ids_remain_explicit(
    tmp_path, monkeypatch, admission_limits
):
    rows_by_stream = {
        "root_source": [[_root(npi=None), _root(), _root()]],
        "detail_source": [[_child(amount="1"), _child(amount="2"), _child(key=None)]],
    }
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            joined_occurrences = list(audit.read_admission_occurrences(descriptor))
            assert len(joined_occurrences) == 6
            invalid = joined_occurrences[0]
            assert invalid.record.typed_parent_key is None
            assert invalid.root_record_id is None and invalid.revision_id is None
            assert invalid.initial_rejection_id is not None
            rejected_families = [
                header
                for _shard, header in audit.read_admission_families(descriptor)
                if header["selection"] != "source"
            ]
            assert rejected_families
            assert sum(entry.revision_id is not None for entry in joined_occurrences) == 4
            assert descriptor.counts_by_kind["candidate_errors"] > 0
        finally:
            audit.cleanup_admission_output(descriptor)


@pytest.mark.asyncio
async def test_new_root_rejections_apply_nullif_zero_without_changing_source_coordinates(
    tmp_path, monkeypatch, admission_limits
):
    rows_by_stream = {"root_source": [[_root(), _root()]], "detail_source": [[_child()]]}
    async with _inputs(tmp_path, monkeypatch, rows_by_stream) as (case, allocated, outputs):
        descriptor = _build_audit(tmp_path, case, allocated, outputs, admission_limits)
        try:
            events = list(audit.read_admission_events(descriptor))
            assert len(events) == 2 and all(event.origin == "admission" for event in events)
            assert [event.coordinate for event in events] == [(1, 0), (1, 1)]
            assert [event.source_ordinal for event in events] == [None, 1]
            assert all(event.collection_slot is None and event.canonical_evidence == "{}" for event in events)
            assert all(event.rejection_id is None for event in events)
        finally:
            audit.cleanup_admission_output(descriptor)
