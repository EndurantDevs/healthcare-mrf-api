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
