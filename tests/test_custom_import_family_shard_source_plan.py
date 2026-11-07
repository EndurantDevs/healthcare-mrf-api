# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original SOURCE pack/hash/allocation semantics through bounded owned spools."""

import hashlib
import pickle
from contextlib import closing
from dataclasses import replace
from decimal import Decimal
from itertools import count
from pathlib import Path
from uuid import UUID

import pytest

from process.custom_import import build_source as original
from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_source_plan as planner
from process.custom_import.bulk_page_codec import encode_landing_batch
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool, read_partition
from process.custom_import.family_shard_input_records import encode_record
from process.custom_import.runner_types import CandidateRegistry, CandidateRunnerError
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_family_shard_input import _capture_input, _child, _partition, _root


async def _case(
    tmp_path, rows=None, *, row_limit=2, byte_limit=16384, decimal_scores=False, definition=None, source_binding=True
):
    definition, source, parts = _capture_input(
        rows or {"root_source": [[_root()]], "detail_source": [[_child()]]},
        decimal_scores=decimal_scores,
        definition=definition,
        source_binding=source_binding,
    )
    partition = await _partition(tmp_path, definition, source, parts)
    request = _request(
        dataset_id=source.identity.dataset_id,
        definition_revision_id=source.identity.definition_revision_id,
        schema_revision_id=source.identity.schema_revision_id,
        definition=definition,
        page_row_limit=row_limit,
        page_byte_limit=byte_limit,
    )
    registry = CandidateRegistry({"details": 7}, {"root_source": 1, "detail_source": 2}, 1)
    part_entries = tuple(
        (registry.stream_slots[part.receipt.stream_id], part.ordinal, part.record_count) for part in parts
    )
    return partition, request, registry, part_entries, source, parts


def _build(tmp_path, case, *, parts=None, limits=None, register=None):
    partition, request, registry, evidence, _source, _parts = case
    return planner.build_source_plan(
        partition,
        request,
        registry,
        evidence if parts is None else parts,
        root=tmp_path / "source-plan",
        register_cleanup=register or (lambda _path: None),
        limits=limits or SourceFactLimits(16 * 1024**2, 1024**2, 1024),
    )


def _oracle(case):
    _partitioned, request, registry, _evidence, source, parts = case
    source_ordinals = dict.fromkeys(registry.stream_slots.values(), 0)
    pack_ordinals = dict.fromkeys(registry.stream_slots.values(), 0)
    streams_by_id = {stream.stream_id: stream for stream in request.definition.source_streams}
    batches = []
    for part in parts:
        stream = streams_by_id[part.receipt.stream_id]
        context = original._StreamContext(request, registry, 17, stream)
        slot = context.stream_slot
        with closing(original._iter_source_pages(context, part, source.policy, (1, 0, source_ordinals[slot]))) as pages:
            for page in pages:
                batch = encode_landing_batch(
                    context,
                    (page,),
                    batch_id=UUID(int=892),
                    first_pack_ordinal=pack_ordinals[slot],
                    row_limit=request.page_row_limit,
                    byte_limit=request.page_byte_limit,
                )
                batches.append((slot, context.collection_slot, batch))
                pack_ordinals[slot] += 1
        source_ordinals[slot] += part.record_count
    return batches


def _assert_oracle(plan, case):
    observed_packs = list(planner.read_source_plan(plan))
    expected = _oracle(case)
    assert len(observed_packs) == len(expected)
    for pack, (stream, collection, batch) in zip(observed_packs, expected, strict=True):
        assert (pack.stream_slot, pack.collection_slot) == (stream, collection)
        assert tuple(tuple(row[1:]) for row in pack.landing.records) == tuple(tuple(row[1:]) for row in batch.records)
        assert (pack.landing.byte_count, pack.landing.pack_count, pack.landing.manifest_sha256) == (
            batch.byte_count,
            batch.pack_count,
            batch.manifest_sha256,
        )
        assert all(row.batch_id == UUID(int=0) for row in pack.landing.records)
        assert all(row.batch_id != UUID(int=0) for row in batch.records)
    return observed_packs


async def test_native_plan_preserves_part_pages(tmp_path, monkeypatch):
    import inspect

    from process.custom_import import source_column_preparation as columns

    if columns.native_column_encoder() is None:
        pytest.skip("compiled source columns unavailable")
    assert inspect.isbuiltin(columns.native_column_encoder())
    bind = columns.bind_column_encoder
    compiled, called = [], []

    def observed(layout):
        compiled.append(layout)
        actual = bind(layout)

        def invoke(descriptors, limit):
            called.append(descriptors[0][3])
            return actual(descriptors, limit)

        return invoke

    monkeypatch.setattr(columns, "bind_column_encoder", observed)
    roots = [_root(score=Decimal("7.000")) for _ in range(65)]
    child_rows = [_child(key=f'Ω-{index}\n\0"\\') for index in range(65)]
    roots[11] = _root(npi=None)
    child_rows[11] = _child(npi=None)
    case = await _case(
        tmp_path,
        {"root_source": [[], roots[:33], roots[33:], []], "detail_source": [[], child_rows[:33], child_rows[33:], []]},
        decimal_scores=True,
        row_limit=17,
    )
    plan = _build(tmp_path, case, limits=SourceFactLimits(32 * 1024**2, 1024**2, 8 * 1024**2))
    try:
        _assert_oracle(plan, case)
        assert len(compiled) == 2
        assert called == [33, 32, 33, 32]
        assert list(planner.read_source_parts(plan)) == list(case[3])
    finally:
        planner.cleanup_source_plan(plan)


async def test_original_stream_part_order_exact_hashes_and_all_initial_allocations(tmp_path):
    case = await _case(
        tmp_path,
        {
            "root_source": [[], [_root(), _root(), _root(npi=None), _root("1234567893")], []],
            "detail_source": [
                [_child(key="same"), _child(key="same"), _child(key="same", amount="2.00")],
                [],
                [_child(npi=None), _child(key=None), _child("1111111111", "orphan")],
            ],
        },
    )
    registered_paths = []

    def register(path):
        assert not path.exists()
        registered_paths.append(path)

    plan = _build(tmp_path, case, register=register)
    assert pickle.loads(pickle.dumps(plan)) == plan
    packs = _assert_oracle(plan, case)
    assert registered_paths == [Path(plan.root)]
    assert list(planner.read_source_parts(plan)) == list(case[3])
    assert plan.counts == (10, 6, 3, 4, 3, 6)
    assert [pack.pack_index for pack in packs] == list(range(6))
    assert [pack.pack_ordinal for pack in packs] == [0, 1, 0, 1, 2, 3]
    assert [kind for pack in packs for kind in pack.allocation_kinds] == [1, 1, None, 1, 2, 2, 2, None, None, 2]
    assert [ordinal for pack in packs for ordinal in pack.initial_rejection_ordinals] == [
        None,
        None,
        0,
        None,
        None,
        None,
        None,
        1,
        2,
        None,
    ]
    originals = sorted(
        read_partition(case[0]), key=lambda occurrence: (occurrence.stream_slot, occurrence.source_ordinal)
    )
    assert [encode_record(occurrence) for pack in packs for occurrence in pack.records] == [
        encode_record(occurrence) for occurrence in originals
    ]
    assert {
        occurrence.collection_slot for pack in packs if pack.collection_slot == 7 for occurrence in pack.records
    } == {1}
    assert {entry.name for entry in Path(plan.root).iterdir()} == {"source-plan.mpk"}
    planner.cleanup_source_plan(plan)
    assert not Path(plan.root).exists()


@pytest.mark.parametrize("row_limit,byte_limit", [(1, 16384), (256, 1600), (3, 16384)])
async def test_original_row_byte_bounds_and_part_flushes(tmp_path, row_limit, byte_limit):
    case = await _case(
        tmp_path,
        {
            "root_source": [[_root()] * 5, [_root()] * 2],
            "detail_source": [[_child(key="x" * 130)] * 7, [_child()]],
        },
        row_limit=row_limit,
        byte_limit=byte_limit,
    )
    plan = _build(tmp_path, case)
    packs = _assert_oracle(plan, case)
    assert all(len(pack.records) <= row_limit and pack.landing.byte_count <= byte_limit for pack in packs)
    assert all(len({row.part_ordinal for row in pack.records}) == 1 for pack in packs)
    if byte_limit == 1600:
        assert all(len(pack.records) < row_limit for pack in packs)


async def test_original_decimal_integer_replay_and_lexical_payload_hashes(tmp_path):
    case = await _case(
        tmp_path,
        {
            "root_source": [[_root(score=Decimal("7")), _root("1234567893", Decimal("8"))]],
            "detail_source": [[_child(amount="-0.000"), _child(key="b", amount="1.2300")]],
        },
        decimal_scores=True,
    )
    plan = _build(tmp_path, case)
    packs = _assert_oracle(plan, case)
    assert type(dict(packs[0].records[0].values)["score"]) is Decimal
    assert dict(packs[1].records[0].values)["amount"] == "-0.000"
    assert dict(packs[1].records[1].values)["amount"] == "1.2300"


async def test_zero_row_parts_are_preserved_without_inventing_packs(tmp_path):
    case = await _case(tmp_path, {"root_source": [[], []], "detail_source": [[], []]})
    plan = _build(tmp_path, case)
    assert list(planner.read_source_plan(plan)) == []
    assert list(planner.read_source_parts(plan)) == list(case[3])
    assert plan.counts == (0, 0, 0, 0, 0, 4)


def _damage_part_evidence(parts, damage):
    if damage == "missing_empty":
        parts.pop(0)
        return
    if damage == "duplicate":
        parts.insert(0, parts[0])
        return
    if damage == "reverse":
        parts.reverse()
        return
    if damage == "gap":
        parts[1] = (1, 3, 1)
        return
    if damage == "row_count":
        parts[1] = (1, 2, 0)
        return
    parts[0] = (True, 1, 0)


@pytest.mark.parametrize("damage", ["missing_empty", "duplicate", "reverse", "gap", "row_count", "bool_slot"])
async def test_incomplete_or_different_original_part_evidence_fails_closed(tmp_path, damage):
    case = await _case(tmp_path, {"root_source": [[], [_root()]], "detail_source": [[_child()]]})
    parts = list(case[3])
    _damage_part_evidence(parts, damage)
    with pytest.raises(ValueError, match="SOURCE"):
        _build(tmp_path, case, parts=parts)
    assert not (tmp_path / "source-plan").exists()


@pytest.mark.parametrize(
    "damage", ["source_ordinal", "collection_slot", "raw_parent_key", "rejection_code", "duplicate"]
)
async def test_original_occurrences_and_prepared_facts_are_rechecked(tmp_path, monkeypatch, damage):
    case = await _case(tmp_path)
    rows = list(read_partition(case[0]))
    if damage == "duplicate":
        rows.append(rows[0])
    else:
        changes_by_kind = {
            "source_ordinal": 7,
            "collection_slot": 9,
            "raw_parent_key": None,
            "rejection_code": "field_type_invalid",
        }
        rows[0] = replace(rows[0], **{damage: changes_by_kind[damage]})
    monkeypatch.setattr(planner, "read_partition", lambda _partition, *, check=None: (row for row in rows))
    with pytest.raises(ValueError, match="SOURCE"):
        _build(tmp_path, case)
    assert not (tmp_path / "source-plan").exists()


async def test_resource_refusal_cleans_registered_work_and_preserves_input(tmp_path):
    case = await _case(tmp_path)
    with pytest.raises(SourceFactBudgetExceeded):
        _build(tmp_path, case, limits=SourceFactLimits(250, 250, 250))
    assert not (tmp_path / "source-plan").exists()
    assert len(list(read_partition(case[0]))) == 2


async def test_late_preparation_refusal_never_publishes_a_prefix(tmp_path):
    case = await _case(
        tmp_path,
        {
            "root_source": [[_root()]],
            "detail_source": [[_child(), _child(key="x" * 4000)]],
        },
        row_limit=1,
        byte_limit=1600,
    )
    with pytest.raises(CandidateRunnerError, match="byte limit"):
        _build(tmp_path, case)
    assert not (tmp_path / "source-plan").exists()


async def test_initial_rejection_only_pages_keep_the_original_empty_pack_hash(tmp_path):
    case = await _case(
        tmp_path,
        {
            "root_source": [[_root(npi=None), _root(npi=None)]],
            "detail_source": [[_child(npi=None), _child(key=None)]],
        },
    )
    plan = _build(tmp_path, case)
    packs = _assert_oracle(plan, case)
    assert plan.counts == (4, 2, 0, 0, 4, 2)
    assert [kind for pack in packs for kind in pack.allocation_kinds] == [None] * 4
    assert [ordinal for pack in packs for ordinal in pack.initial_rejection_ordinals] == list(range(4))


async def test_late_partition_failure_closes_iterator_and_removes_work(tmp_path, monkeypatch):
    case = await _case(tmp_path)
    rows = list(read_partition(case[0]))
    observations = []

    def failing(_partition, *, check=None):
        try:
            yield from rows
            raise RuntimeError("synthetic incomplete partition")
        finally:
            observations.append("closed")

    monkeypatch.setattr(planner, "read_partition", failing)
    with pytest.raises(RuntimeError, match="incomplete partition"):
        _build(tmp_path, case)
    assert observations == ["closed"]
    assert not (tmp_path / "source-plan").exists()


async def test_part_iterator_must_close_successfully_before_plan_publication(tmp_path):
    case = await _case(tmp_path)

    class Parts:
        def __init__(self):
            self.rows = iter(case[3])
            self.closed = False

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.rows)

        def close(self):
            self.closed = True
            raise RuntimeError("synthetic metadata close failure")

    parts = Parts()
    with pytest.raises(RuntimeError, match="metadata close failure"):
        _build(tmp_path, case, parts=parts)
    assert parts.closed and not (tmp_path / "source-plan").exists()


@pytest.mark.parametrize("damage", ["definition", "root_stream", "collection_slot"])
async def test_capture_and_registry_identity_cannot_change(tmp_path, damage):
    case_parts = list(await _case(tmp_path))
    if damage == "definition":
        case_parts[1] = replace(case_parts[1], definition_revision_id=999)
    elif damage == "root_stream":
        case_parts[2] = replace(case_parts[2], root_stream_slot=2)
    else:
        case_parts[2] = replace(case_parts[2], child_collection_slots={"details": 0})
    with pytest.raises(ValueError, match="SOURCE"):
        _build(tmp_path, case_parts)
    assert not (tmp_path / "source-plan").exists()


@pytest.mark.parametrize("damage", ["bytes", "descriptor", "suffix", "foreign_file", "replacement"])
async def test_replay_requires_completed_exact_spool_custody_before_first_pack(tmp_path, damage):
    case = await _case(tmp_path)
    plan = _build(tmp_path, case)
    target = Path(plan.root) / "source-plan.mpk"
    if damage == "bytes":
        target.chmod(0o600)
        payload = bytearray(target.read_bytes())
        payload[-1] ^= 1
        target.write_bytes(payload)
    elif damage == "descriptor":
        plan = replace(plan, counts=(99, *plan.counts[1:]))
    elif damage == "suffix":
        target.chmod(0o600)
        with target.open("ab") as output:
            output.write(b"trailing")
    elif damage == "foreign_file":
        (Path(plan.root) / "unowned").touch()
    else:
        payload = target.read_bytes()
        replacement = tmp_path / "replacement"
        replacement.write_bytes(payload)
        replacement.replace(target)
    with pytest.raises(ValueError, match="SOURCE"):
        list(planner.read_source_plan(plan))


async def test_missing_application_footer_cannot_become_complete_even_with_new_file_hash(tmp_path):
    case = await _case(tmp_path)
    plan = _build(tmp_path, case)
    target = Path(plan.root) / "source-plan.mpk"
    payload = target.read_bytes()
    position = 0
    while position < len(payload):
        last = position
        position += 8 + planner._LENGTH.unpack(payload[position : position + 8])[0]
    target.chmod(0o600)
    target.write_bytes(payload[:last])
    plan = replace(plan, file_bytes=last, sha256=hashlib.sha256(payload[:last]).digest())
    with pytest.raises(ValueError, match="incomplete"):
        list(planner.read_source_plan(plan))


async def test_existing_unowned_work_root_is_never_removed(tmp_path):
    case = await _case(tmp_path)
    root = tmp_path / "source-plan"
    root.mkdir()
    marker = root / "preserved"
    marker.write_bytes(b"synthetic")
    with pytest.raises(FileExistsError):
        _build(tmp_path, case)
    assert marker.read_bytes() == b"synthetic"


@pytest.mark.parametrize("stop_at", [1, 4, 10])
async def test_optional_job_check_stops_file_work_and_preserves_original_capture(tmp_path, stop_at):
    case = await _case(tmp_path, {"root_source": [[_root(), _root()]], "detail_source": [[_child(), _child()]]})
    check_calls = count(1)

    def check():
        if next(check_calls) == stop_at:
            raise InterruptedError("synthetic SOURCE job stopped")

    with pytest.raises(InterruptedError, match="job stopped"):
        planner.build_source_plan(
            case[0],
            case[1],
            case[2],
            case[3],
            root=tmp_path / "source-plan",
            register_cleanup=lambda _path: None,
            limits=SourceFactLimits(16 * 1024**2, 1024**2, 1024),
            check=check,
        )
    assert not (tmp_path / "source-plan").exists()
    plan = _build(tmp_path, case)
    try:
        _assert_oracle(plan, case)
    finally:
        planner.cleanup_source_plan(plan)


async def test_plan_cancellation_interrupts_cascading_merge(tmp_path, monkeypatch):
    case = await _case(
        tmp_path,
        {"root_source": [[_root(score=index) for index in range(16)]], "detail_source": [[_child()]]},
    )
    original_merge = facts._merged
    merge_events = []
    failure = InterruptedError("synthetic merge stopped")

    def _observed_merge(work, paths):
        with closing(original_merge(work, paths)) as entries:
            for entry in entries:
                merge_events.append(len(paths))
                yield entry

    def _check_merge():
        if merge_events:
            raise failure

    monkeypatch.setattr(facts, "_merged", _observed_merge)
    with pytest.raises(InterruptedError, match="merge stopped") as raised:
        planner.build_source_plan(
            *case[:4],
            root=tmp_path / "source-plan",
            register_cleanup=lambda _path: None,
            limits=SourceFactLimits(16 * 1024**2, 1024**2, 1024),
            check=_check_merge,
        )
    assert raised.value is failure
    assert merge_events == [8]
    assert not (tmp_path / "source-plan").exists()
    plan = _build(tmp_path, case)
    try:
        _assert_oracle(plan, case)
    finally:
        planner.cleanup_source_plan(plan)


async def test_responsive_plan_digest_keeps_exact_framed_bytes_and_eof_checks():
    from io import BytesIO

    calls = []
    payload = b"synthetic framed bytes" * 7000
    assert planner._plan_digest(BytesIO(payload), lambda: calls.append(None)) == hashlib.sha256(payload).digest()
    assert len(calls) > 2


@pytest.mark.parametrize("stop_pass", [1, 2])
async def test_original_plan_hash_passes_honor_stop_and_preserve_input(tmp_path, monkeypatch, stop_pass):
    case = await _case(tmp_path)
    plan = _build(tmp_path, case)
    expected_packs = list(planner.read_source_plan(plan))
    original, scans, seen = planner._plan_digest, [], []
    failure = InterruptedError("synthetic original plan hash stopped")

    def digest(source, check):
        scans.append(source)
        return original(source, check)

    def current():
        if len(scans) == stop_pass and scans[-1].tell() > 0:
            raise failure

    monkeypatch.setattr(planner, "_plan_digest", digest)
    try:
        with pytest.raises(InterruptedError, match="original plan hash stopped") as raised:
            for pack in planner.read_source_plan(plan, check=current):
                seen.append(pack)
        assert raised.value is failure and all(source.closed for source in scans)
        assert seen == ([] if stop_pass == 1 else expected_packs)
        assert list(planner.read_source_plan(plan)) == expected_packs
        with pytest.raises(ValueError, match="cancellation check"):
            list(planner.read_source_plan(plan, check=False))
    finally:
        planner.cleanup_source_plan(plan)
        cleanup_spool(case[0])
