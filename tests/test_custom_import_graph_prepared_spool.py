# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Attempt-local prepared frames never replace durable database progress."""

from io import BytesIO
from types import SimpleNamespace

import pytest

from process.custom_import import build_graph_prepared as prepared
from process.custom_import.runner_types import CandidateRunnerError


@pytest.fixture
def scratch():
    stream = prepared.PreparedChildren()
    try:
        yield stream
    finally:
        stream.close()


def _append_children(stream, count):
    columns = (((),) * 10, ((),) * 3, 0, 0, 0)
    for child_id in range(1, count + 1):
        child = SimpleNamespace(
            child_revision_id=child_id,
            collection_slot=1,
            child_key_sha256=child_id.to_bytes(32, "big"),
        )
        stream.append(7, "records", child, 160 + child_id, columns)
    stream.finish()


def _metadata(child_ids):
    return tuple((7, "records", child_id.to_bytes(32, "big"), child_id, 160 + child_id) for child_id in child_ids)


def _resumed_state(count, *, last_id=None):
    child_id = count if last_id is None else last_id
    return SimpleNamespace(
        current=SimpleNamespace(
            selection_kind="source",
            root_record_id=7,
            attached_child_count=count,
            last_child_collection_slot=1,
            last_child_key_sha256=child_id.to_bytes(32, "big"),
            last_input_child_revision_id=child_id,
        )
    )


def _replace_frame(stream, payload, *, count=1, ordinal=0, position=0):
    """Exercise semantic frame checks beyond an otherwise valid checksum."""
    prefix = prepared.struct.pack("!QI", ordinal, count)
    header = prepared._HEADER.pack(
        b"CGP1", ordinal, len(payload), count, prepared.hashlib.sha256(prefix + payload).digest()
    )
    stream.file.seek(position)
    stream.file.write(header + payload)
    stream.file.truncate()
    stream.byte_count = stream.file.tell()
    stream.cached_frame = None


def test_prepared_frames_bound_chunks_and_preserve_exact_rows(scratch):
    _append_children(scratch, 65)
    assert scratch.chunk_count == 2
    assert scratch.row_count == 65
    records = list(scratch.records())
    assert [record[3] for _position, _index, record in records] == list(range(1, 66))
    assert len({position for position, _index, _record in records}) == 2
    rows = scratch.rows(_metadata(range(1, 66)))
    assert [(root_id, child.child_revision_id, child.byte_count, valid) for root_id, child, valid in rows] == [
        (7, child_id, 160 + child_id, True) for child_id in range(1, 66)
    ]
    assert all(child.record()[7] is None for _root_id, child, _valid in rows)


def test_prepared_rows_rewind_after_an_uncommitted_prefix(scratch):
    _append_children(scratch, 130)
    first = scratch.rows(_metadata(range(65, 131)))
    retry = scratch.rows(_metadata(range(65, 69)))
    assert [child.child_revision_id for _root, child, _valid in first] == list(range(65, 131))
    assert [child.child_revision_id for _root, child, _valid in retry] == list(range(65, 69))
    assert scratch.last_key == (7, "records", (68).to_bytes(32, "big"), 68)


@pytest.mark.parametrize("corruption", ["checksum", "truncated", "magic"])
def test_prepared_frame_corruption_fails_closed(scratch, corruption):
    _append_children(scratch, 2)
    scratch.file.seek(0)
    encoded = bytearray(scratch.file.read())
    if corruption == "checksum":
        encoded[-1] ^= 1
    elif corruption == "truncated":
        del encoded[-3:]
    else:
        encoded[0] ^= 1
    scratch.file.seek(0)
    scratch.file.write(encoded)
    scratch.file.truncate(len(encoded))
    with pytest.raises(CandidateRunnerError, match="prepared child frame"):
        scratch.frame(0)


def test_prepared_quota_overflow_discards_the_entire_cache(monkeypatch, scratch):
    _append_children(scratch, 1)
    owned_file = scratch.file
    monkeypatch.setattr(prepared, "MAX_BATCH_BYTES", scratch.byte_count)
    _append_children(scratch, 1)
    assert not scratch.enabled
    assert scratch.file is None
    assert not scratch.pending
    assert owned_file.closed


def test_prepared_short_write_aborts_instead_of_silently_falling_back(monkeypatch, scratch):
    class ShortWrite(BytesIO):
        def write(self, value):
            return super().write(value[:-1])

    owned_file = ShortWrite()
    monkeypatch.setattr(prepared, "TemporaryFile", lambda **_kwargs: owned_file)
    with pytest.raises(OSError, match="scratch write was incomplete"):
        _append_children(scratch, 1)
    assert scratch.enabled
    scratch.close()
    assert owned_file.closed


def test_prepared_disk_failure_aborts_instead_of_silently_falling_back(monkeypatch, scratch):
    def unavailable_file(**_kwargs):
        raise OSError("synthetic disk failure")

    monkeypatch.setattr(prepared, "TemporaryFile", unavailable_file)
    with pytest.raises(OSError, match="synthetic disk failure"):
        _append_children(scratch, 1)
    assert scratch.enabled
    assert scratch.file is None


def test_prepared_wide_fallback_discards_the_entire_cache(scratch):
    _append_children(scratch, 1)
    owned_file = scratch.file
    scratch.append(7, "records", object(), prepared.MAX_INPUT_BYTES, None)
    assert not scratch.enabled
    assert scratch.file is None
    assert owned_file.closed


def test_prepared_cross_frame_duplicate_fails_closed(scratch):
    _append_children(scratch, 1)
    _append_children(scratch, 1)
    with pytest.raises(CandidateRunnerError, match="not in source order"):
        tuple(scratch.records())


def test_prepared_metadata_cannot_change_original_row_charge(scratch):
    _append_children(scratch, 1)
    changed = (*_metadata((1,))[0][:-1], 1)
    with pytest.raises(CandidateRunnerError, match="identity or byte charge"):
        scratch.rows((changed,))


@pytest.mark.parametrize("count,last_id", [(2, 1), (4, 4)])
def test_prepared_resume_requires_the_exact_committed_tip(scratch, count, last_id):
    _append_children(scratch, 3)
    scratch.verify_tips((_resumed_state(2),), {1: "records"})
    with pytest.raises(CandidateRunnerError, match="prepared child prefix"):
        scratch.verify_tips((_resumed_state(count, last_id=last_id),), {1: "records"})


def test_prepared_close_releases_the_owned_file_and_buffers(scratch):
    _append_children(scratch, 2)
    scratch.frame(0)
    owned_file = scratch.file
    scratch.close()
    scratch.close()
    assert owned_file.closed
    assert scratch.file is None
    assert scratch.cached_frame is None
    assert not scratch.pending


def test_empty_and_disabled_streams_do_not_allocate_scratch(scratch):
    assert scratch.rows(()) == ()
    scratch.finish()
    scratch.verify_tips((_resumed_state(0),), {1: "records"})
    scratch.disable()
    scratch.append(7, "records", object(), 1, ())
    scratch.finish()
    assert scratch.file is None
    assert scratch.row_count == scratch.byte_count == 0


def test_encoded_row_limit_discards_previously_prepared_rows(scratch):
    _append_children(scratch, 1)
    owned_file = scratch.file
    child = SimpleNamespace(child_revision_id=2, collection_slot=1, child_key_sha256=(2).to_bytes(32, "big"))
    scratch.append(7, "records", child, 162, (b"x" * prepared.MAX_INPUT_BYTES,))
    assert not scratch.enabled
    assert scratch.file is None and owned_file.closed
    assert not scratch.pending


def test_flush_rechecks_the_encoded_frame_limit(monkeypatch, scratch):
    child = SimpleNamespace(child_revision_id=1, collection_slot=1, child_key_sha256=(1).to_bytes(32, "big"))
    scratch.append(7, "records", child, 161, (((),) * 10, ((),) * 3, 0, 0, 0))
    monkeypatch.setattr(prepared, "MAX_INPUT_BYTES", 1)
    scratch.finish()
    assert not scratch.enabled and scratch.file is None
    assert not scratch.pending


@pytest.mark.parametrize("position", [-1, 0, 1])
def test_missing_frame_positions_fail_closed(scratch, position):
    if position != 0:
        _append_children(scratch, 1)
        position = -1 if position < 0 else scratch.byte_count
    with pytest.raises(CandidateRunnerError, match="frame is missing"):
        scratch.frame(position)


def test_truncated_frame_header_is_not_treated_as_an_empty_stream(scratch):
    _append_children(scratch, 1)
    scratch.file.truncate(prepared._HEADER.size - 1)
    with pytest.raises(CandidateRunnerError, match="frame is truncated"):
        scratch.frame(0)


@pytest.mark.parametrize("count,ordinal", [(0, 0), (prepared.MAX_ROWS + 1, 0), (1, 1)])
def test_frame_inventory_bounds_are_checked_before_decoding(scratch, count, ordinal):
    _append_children(scratch, 1)
    _replace_frame(scratch, b"\xc0", count=count, ordinal=ordinal)
    with pytest.raises(CandidateRunnerError, match="frame bound differs"):
        scratch.frame(0)


@pytest.mark.parametrize("payload,message", [(b"\xc1", "encoding"), (b"\x80", "prefix"), (b"\x90", "prefix")])
def test_checksummed_frames_still_require_valid_record_encoding(scratch, payload, message):
    _append_children(scratch, 1)
    _replace_frame(scratch, payload)
    with pytest.raises(CandidateRunnerError, match=f"frame {message} differs"):
        scratch.frame(0)
    assert scratch.cached_frame is None


def test_decode_allocation_cause_is_not_reclassified_as_corruption(monkeypatch, scratch):
    _append_children(scratch, 1)

    def refuse_decode(*_args, **_kwargs):
        raise ValueError("synthetic decode allocation") from MemoryError("synthetic allocation")

    monkeypatch.setattr(prepared, "unpackb", refuse_decode)
    with pytest.raises(ValueError, match="synthetic decode allocation") as failure:
        scratch.frame(0)
    assert isinstance(failure.value.__cause__, MemoryError)
    assert scratch.cached_frame is None


@pytest.mark.parametrize(
    "changes",
    [
        {0: True},
        {0: 0},
        {1: 1},
        {1: ""},
        {1: "x" * 64},
        {2: 0},
        {3: False},
        {4: "x" * 32},
        {4: b"x" * 31},
        {5: -1},
        {6: None},
        {7: ()},
    ],
)
def test_checksummed_records_cannot_change_their_typed_identity(scratch, changes):
    _append_children(scratch, 1)
    record_parts = list(scratch.frame(0)[0][0])
    for index, value in changes.items():
        record_parts[index] = value
    _replace_frame(scratch, prepared.packb((tuple(record_parts),), use_bin_type=True))
    with pytest.raises(CandidateRunnerError, match="prepared child record differs"):
        scratch.frame(0)
    assert scratch.cached_frame is None


@pytest.mark.parametrize("shape", ["mapping", "short"])
def test_record_shape_is_checked_after_frame_decoding(scratch, shape):
    _append_children(scratch, 1)
    record = scratch.frame(0)[0][0]
    changed = {"record": record} if shape == "mapping" else record[:-1]
    _replace_frame(scratch, prepared.packb((changed,), use_bin_type=True))
    with pytest.raises(CandidateRunnerError, match="prepared child record differs"):
        scratch.frame(0)


@pytest.mark.parametrize(
    "changes",
    [{0: True}, {1: 2}, {2: b"parent"}, {3: b"x"}, {4: b"child"}, {5: b"x" * 32}, {6: None}, {7: "digest"}],
)
def test_fallback_records_keep_the_original_child_identity_and_types(scratch, changes):
    _append_children(scratch, 1)
    record_parts = list(scratch.frame(0)[0][0])
    fallback_parts = [
        record_parts[3],
        record_parts[2],
        "parent",
        b"p" * 32,
        "child",
        record_parts[4],
        "payload",
        b"d" * 32,
    ]
    for index, value in changes.items():
        fallback_parts[index] = value
    record_parts[6], record_parts[7] = None, tuple(fallback_parts)
    _replace_frame(scratch, prepared.packb((tuple(record_parts),), use_bin_type=True))
    with pytest.raises(CandidateRunnerError, match="prepared child fallback differs"):
        scratch.frame(0)


@pytest.mark.parametrize("fallback", [{}, ()])
def test_fallback_shape_is_not_accepted_as_a_child_record(scratch, fallback):
    _append_children(scratch, 1)
    record_parts = list(scratch.frame(0)[0][0])
    record_parts[6], record_parts[7] = None, fallback
    _replace_frame(scratch, prepared.packb((tuple(record_parts),), use_bin_type=True))
    with pytest.raises(CandidateRunnerError, match="prepared child fallback differs"):
        scratch.frame(0)


def test_duplicate_rows_inside_one_frame_fail_closed(scratch):
    _append_children(scratch, 1)
    record = scratch.frame(0)[0][0]
    _replace_frame(scratch, prepared.packb((record, record), use_bin_type=True), count=2)
    with pytest.raises(CandidateRunnerError, match="not in source order"):
        tuple(scratch.records())


def test_missing_frame_ordinal_fails_even_when_rows_are_ordered(scratch):
    _append_children(scratch, prepared.MAX_ROWS + 1)
    records, next_position, _ordinal = scratch.frame(0)
    last_records, _end, _ordinal = scratch.frame(next_position)
    _replace_frame(scratch, prepared.packb(last_records, use_bin_type=True), ordinal=2, position=next_position)
    scratch.chunk_count = 3
    assert len(records) == prepared.MAX_ROWS
    with pytest.raises(CandidateRunnerError, match="frame order differs"):
        tuple(scratch.records())


def test_rows_skip_unrequested_children_but_require_the_complete_prefix(scratch):
    _append_children(scratch, 3)
    rows = scratch.rows(_metadata((2,)))
    assert [child.child_revision_id for _root, child, _valid in rows] == [2]
    with pytest.raises(CandidateRunnerError, match="source prefix is incomplete"):
        scratch.rows(_metadata((3, 4)))


def test_committed_tip_verification_skips_other_roots_and_later_children(scratch):
    _append_children(scratch, 4)
    second = _resumed_state(1)
    second.current.root_record_id = 8
    with pytest.raises(CandidateRunnerError, match="prefix is shorter than durable progress"):
        scratch.verify_tips((_resumed_state(1), second), {1: "records"})
