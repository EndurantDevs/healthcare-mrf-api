# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Temporary sort storage preserves raw limits, ordering and Work custody."""

import hashlib
import random
from compression import zstd
from contextlib import closing
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_source_journal as journal_api
from process.custom_import.runner_types import CancellationRequested
from tests.test_custom_import_family_shard_recovery_budget import retained as retained


class _Rows:
    def __init__(self, values):
        self.values, self.closed = iter(values), False

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.values)

    def close(self):
        self.closed = True


def _work(tmp_path, name="work", *, cap=16 * 1024**2, check=None):
    return facts._Work(tmp_path / name, facts.SourceFactLimits(cap, 8192, 4096), lambda _path: None, check=check)


def _sort(tmp_path, name, values, *, cap=16 * 1024**2):
    work = _work(tmp_path, name, cap=cap)
    try:
        with closing(facts._sorted(work, iter(values), lambda row: row[0])) as rows:
            actual_rows = list(rows)
        assert not work.paths and work.used == 0
        return actual_rows, work.peak_bytes
    finally:
        work.close()


def test_multiple_merge_levels_preserve_plain_sort_ties_and_reduce_live_storage(tmp_path, monkeypatch, request):
    values = [(index % 7, index, b"repeated original bytes" * 80) for index in range(511, -1, -1)]
    with monkeypatch.context() as plain:
        plain.setattr(facts, "_write_sort_run", lambda work, rows: work.write(rows))
        plain.setattr(facts, "_sort_rows", lambda work, path: work.rows(path))
        expected, plain_peak = _sort(tmp_path, "plain", values)
    actual, compressed_peak = _sort(tmp_path, "compressed", values)
    assert actual == expected and len(actual) == len(values)
    assert compressed_peak < plain_peak
    cap = (plain_peak + compressed_peak) // 2
    request.node.user_properties.extend(
        [
            ("plain_charged_peak", plain_peak),
            ("compressed_charged_peak", compressed_peak),
            ("same_cap", cap),
            ("records", len(values)),
        ]
    )
    with monkeypatch.context() as plain:
        plain.setattr(facts, "_write_sort_run", lambda work, rows: work.write(rows))
        plain.setattr(facts, "_sort_rows", lambda work, path: work.rows(path))
        with pytest.raises(facts.SourceFactBudgetExceeded):
            _sort(tmp_path, "plain-refused", values, cap=cap)
    assert not (tmp_path / "plain-refused").exists()
    assert _sort(tmp_path, "compressed-admitted", values, cap=cap)[0] == expected


def test_legacy_plain_and_compressed_runs_merge_and_final_bytes_stay_plain(tmp_path):
    work = _work(tmp_path)
    rows = [(0, ("legacy", b"a")), (1, ("compressed", b"repeat" * 700))]
    try:
        old = work.write([rows[0]])
        new = facts._write_sort_run(work, [rows[1]])
        assert list(work.rows(old)) == [rows[0]]
        assert list(work.rows(new))[0][0] == facts._COMPRESSED_SORT
        with closing(facts._merged(work, [old, new])) as merged:
            final = work.write(value for _order, value in merged)
        expected = b"".join(
            facts._LENGTH.pack(len(packed)) + packed for _order, value in rows for packed in [facts._pack(value)]
        )
        assert final.read_bytes() == expected
        assert hashlib.sha256(final.read_bytes()).digest() == hashlib.sha256(expected).digest()
        assert list(work.rows(final)) == [value for _order, value in rows]
    finally:
        work.close()


def test_incompressible_and_not_strictly_smaller_envelopes_keep_original_rows(tmp_path, monkeypatch):
    work = _work(tmp_path)
    try:
        value = (17, random.Random(7307).randbytes(4096))
        assert facts._sort_entry(work, value) is value
        raw_size = len(facts._pack(value))
        equal_size = next(
            size
            for size in range(raw_size)
            if len(facts._pack((facts._COMPRESSED_SORT, raw_size, b"x" * size))) == raw_size
        )
        monkeypatch.setattr(facts.zstd, "compress", lambda *_args, **_kwargs: b"x" * equal_size)
        assert facts._sort_entry(work, value) is value
    finally:
        work.close()


def test_original_singleton_refuses_before_native_compression(tmp_path, monkeypatch):
    work = _work(tmp_path)
    rows = _Rows([(0, b"x" * 8192)])

    def forbidden(*_args, **_kwargs):
        pytest.fail("oversized original record entered the compressor")

    monkeypatch.setattr(facts.zstd, "compress", forbidden)
    try:
        with pytest.raises(facts.SourceFactBudgetExceeded, match="singleton"):
            facts._write_sort_run(work, rows)
        assert rows.closed and work.used == 0 and len(work.paths) == 1
        assert work._entry_bytes == 2 * work._entry_charge
    finally:
        work.close()


@pytest.mark.parametrize(
    "damage", ["truncated", "trailing", "second", "checksum", "short", "long", "over_budget", "bool", "not_bytes"]
)
def test_compressed_corruption_and_expansion_are_refused(tmp_path, damage):
    work = _work(tmp_path)
    try:
        stored = facts._sort_entry(work, (7, b"same source bytes" * 400))
        marker, size, frame = stored
        assert marker == facts._COMPRESSED_SORT
        invalid_size, invalid_frame = {
            "truncated": (size, frame[:-1]),
            "trailing": (size, frame + b"unexpected"),
            "second": (size, frame + zstd.compress(b"second stream")),
            "checksum": (size, frame[:-1] + bytes([frame[-1] ^ 1])),
            "short": (size - 1, frame),
            "long": (size + 1, frame),
            "over_budget": (work.limits.max_record_bytes + 1, frame),
            "bool": (True, frame),
            "not_bytes": (size, bytearray(frame)),
        }[damage]
        with pytest.raises(ValueError, match="source fact compressed sort"):
            facts._decoded_sort_entry(work, (marker, invalid_size, invalid_frame))
    finally:
        work.close()


def test_decoder_bounds_window_and_output_before_expanding(tmp_path, monkeypatch):
    work = _work(tmp_path)
    original, observed = facts.zstd.ZstdDecompressor, []
    entry = (9, b"original payload" * 300)
    stored = facts._sort_entry(work, entry)

    def decoder(*, options):
        actual = original(options=options)
        expected = min(31, max(10, (stored[1] - 1).bit_length()))
        assert options == {zstd.DecompressionParameter.window_log_max: expected}

        def decompress(frame, *, max_length):
            assert max_length == stored[1] + 1
            result = actual.decompress(frame, max_length=max_length)
            observed.append((len(result), actual.eof, actual.unused_data))
            return result

        return SimpleNamespace(decompress=decompress, eof=True, unused_data=b"")

    monkeypatch.setattr(facts.zstd, "ZstdDecompressor", decoder)
    try:
        assert facts._decoded_sort_entry(work, stored) == entry
        assert observed == [(stored[1], True, b"")]
    finally:
        work.close()


@pytest.mark.parametrize("entry", [("unexpected",), ("invalid", 4, b"data"), (0, 1, 2, 3)])
def test_unknown_or_non_sort_envelopes_are_refused(tmp_path, entry):
    work = _work(tmp_path)
    try:
        with pytest.raises(ValueError, match="bounds"):
            facts._decoded_sort_entry(work, entry)
        raw = facts._pack(entry)
        with pytest.raises(ValueError, match="record"):
            facts._decoded_sort_entry(work, (facts._COMPRESSED_SORT, len(raw), zstd.compress(raw)))
    finally:
        work.close()


def test_stored_byte_charge_precedes_prefix_write(tmp_path, monkeypatch):
    work = _work(tmp_path)
    original, observed = work.frame, []

    def frame(target, value, path=None):
        before = work.used
        expected = facts._LENGTH.size + len(facts._pack(value))

        def write(data):
            assert work.used == before + expected
            observed.append(len(data))
            return target.write(data)

        return original(SimpleNamespace(write=write), value, path)

    monkeypatch.setattr(work, "frame", frame)
    try:
        path = facts._write_sort_run(work, [(7, b"compressible" * 500)])
        assert len(observed) == 2 and observed[0] == facts._LENGTH.size
        assert work.used == work.paths[path] == path.stat().st_size == sum(observed)
        work.remove(path)
        assert work.used == 0 and work._entry_bytes == work._entry_charge
    finally:
        work.close()


def test_sort_run_preserves_journal_aggregate_entry_and_stored_byte_charge(retained):
    retained.journal.reconcile_budget()
    before = retained.journal.budget().used
    work = journal_api.SourceAllocationWork(
        retained.journal, retained.journal.root / "outputs" / "sort-work", retained.limits
    )
    try:
        path = facts._write_sort_run(work, [(7, b"x" * 2048)])
        assert list(facts._sort_rows(work, path)) == [(7, b"x" * 2048)]
        assert retained.journal.budget().used == before + work.used + 2 * work._entry_charge
        work.remove(path)
        assert retained.journal.budget().used == before + work._entry_charge
    finally:
        work.close()
    assert retained.journal.budget().used == before


@pytest.mark.parametrize("failure", ["compress", "cancel"])
def test_codec_failure_drains_writer_and_keeps_provisional_entry(tmp_path, monkeypatch, failure):
    stops = []

    def check():
        if stops:
            raise CancellationRequested("sort cancelled")

    work = _work(tmp_path, check=check)
    rows = _Rows([(7, b"x" * 2048), (8, b"never consumed")])
    original = facts.zstd.compress

    def compress(*args, **kwargs):
        if failure == "compress":
            raise OSError("sort compression failed")
        result = original(*args, **kwargs)
        stops.append(True)
        return result

    monkeypatch.setattr(facts.zstd, "compress", compress)
    try:
        with pytest.raises((OSError, CancellationRequested), match="sort"):
            facts._write_sort_run(work, rows)
        assert rows.closed and work.used == 0 and len(work.paths) == 1
        assert work._entry_bytes == 2 * work._entry_charge
    finally:
        work.close()
    assert not work.root.exists()


def test_initial_entry_refusal_still_closes_source_iterator(tmp_path):
    unit = facts.os.statvfs(tmp_path).f_frsize
    work = facts._Work(tmp_path / "tight", facts.SourceFactLimits(2 * unit, unit, unit), lambda _path: None)
    rows = _Rows([(0, "unread")])
    try:
        with pytest.raises(facts.SourceFactBudgetExceeded, match="entry"):
            facts._write_sort_run(work, rows)
        assert rows.closed and work.used == 0 and not work.paths
    finally:
        work.close()


def test_malformed_run_drains_every_open_merge_reader(tmp_path, monkeypatch):
    work = _work(tmp_path)
    original, closed = work.rows, []

    def rows(path):
        try:
            yield from original(path)
        finally:
            closed.append(path)

    try:
        good = facts._write_sort_run(work, [(1, b"original" * 500)])
        bad = work.write([(facts._COMPRESSED_SORT, 4096, b"invalid")])
        monkeypatch.setattr(work, "rows", rows)
        with pytest.raises(ValueError, match="compressed sort"):
            list(facts._merged(work, [good, bad]))
        assert closed == [bad, good]
    finally:
        work.close()


def test_materialization_deadline_is_checked_before_sort_compression(tmp_path, monkeypatch):
    from process.custom_import.family_block_materialization import _CheckedWork

    def deadline():
        raise CancellationRequested("materialization deadline")

    def forbidden(*_args, **_kwargs):
        pytest.fail("expired materialization entered the compressor")

    plan = SimpleNamespace(
        root=tmp_path / "materialization",
        limits=facts.SourceFactLimits(1024**2, 8192, 4096),
        register_cleanup=lambda _path: None,
    )
    work = _CheckedWork(plan, deadline)
    monkeypatch.setattr(facts.zstd, "compress", forbidden)
    try:
        with pytest.raises(CancellationRequested, match="deadline"):
            facts._write_sort_run(work, [(0, b"x" * 4096)])
        assert work.used == 0
    finally:
        work.close()


def test_cancellation_after_decompression_prevents_row_escape_and_closes_reader(tmp_path, monkeypatch):
    stops, closed = [], []

    def check():
        if stops:
            raise CancellationRequested("sort decode cancelled")

    work = _work(tmp_path, check=check)
    path = facts._write_sort_run(work, [(0, b"x" * 4096)])
    original_decoder, original_rows = facts.zstd.ZstdDecompressor, work.rows

    def decoder(*, options):
        actual = original_decoder(options=options)
        proxy = SimpleNamespace()

        def decompress(frame, *, max_length):
            result = actual.decompress(frame, max_length=max_length)
            proxy.eof, proxy.unused_data = actual.eof, actual.unused_data
            stops.append(True)
            return result

        proxy.decompress = decompress
        return proxy

    def rows(path):
        try:
            yield from original_rows(path)
        finally:
            closed.append(path)

    monkeypatch.setattr(facts.zstd, "ZstdDecompressor", decoder)
    monkeypatch.setattr(work, "rows", rows)
    try:
        with pytest.raises(CancellationRequested, match="decode cancelled"):
            next(facts._sort_rows(work, path))
        assert closed == [path]
    finally:
        work.close()


def test_stored_frame_limit_is_checked_before_any_decompression(tmp_path, monkeypatch):
    work = _work(tmp_path)
    path = work.path()
    path.write_bytes(facts._LENGTH.pack(work.limits.max_record_bytes + 1))

    def forbidden(*_args, **_kwargs):
        pytest.fail("oversized stored frame entered the decompressor")

    monkeypatch.setattr(facts.zstd, "ZstdDecompressor", forbidden)
    try:
        with pytest.raises(facts.SourceFactBudgetExceeded, match="frame"):
            list(facts._sort_rows(work, path))
    finally:
        work.close()
