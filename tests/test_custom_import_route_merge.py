# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real bytes ABI, route-only ordering, and owned-stream failure boundaries."""

import inspect
import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest

from process.custom_import import family_shard_facts as facts
from process.custom_import import family_shard_route as route
from process.custom_import import family_shard_route_merge as batches
from process.custom_import.family_shard_input_records import decode_record, encode_record
from tests.test_custom_import_family_shard_build import _records, _root
from tests.test_custom_import_snowflake_bundle import _definition


@pytest.fixture
def native():
    functions = batches.backend()
    if functions is None:
        pytest.skip("compiled route byte batches unavailable")
    assert all(inspect.isbuiltin(function) for function in functions)
    return functions


@contextmanager
def _work(root, memory=65536):
    work = facts._Work(root, facts.SourceFactLimits(32 * 1024**2, 1024**2, memory), lambda _path: None)
    try:
        yield work
    finally:
        work.close()


def _values(mode, count=40, *, tied=False):
    records = _records(_definition(), [_root(f"synthetic-{index}", score=index) for index in range(count)], [])
    values = []
    for record in records:
        if tied:
            record = replace(
                record,
                source_ordinal=0,
                row_ordinal=0,
                raw_parent_key=records[0].raw_parent_key,
                typed_parent_key=records[0].typed_parent_key,
            )
        encoded = encode_record(record)
        if mode == "raw":
            values.append((facts._raw_order(encoded), encoded))
        else:
            value = record.typed_parent_key, encoded, 1, (1, 0), False
            values.append(((route._bucket(value, 256), facts._family_order(value)), value))
    return values


def _runs(work, values, lanes=8, *, compressed=False):
    write = facts._write_sort_run if compressed else lambda work, values: work.write(values)
    return [write(work, sorted(values[lane::lanes], key=lambda value: value[0])) for lane in range(lanes)]


def _saved(paths):
    return {path: (path.stat().st_ino, path.read_bytes()) for path in paths}


@pytest.mark.parametrize("mode", ["raw", "family"])
def test_initial_bytes_match_codec(tmp_path, native, mode):
    values = list(reversed(_values(mode, 80)))
    with _work(tmp_path / "work", 1024**2) as work:
        expected = facts._write_sort_run(work, sorted(values, key=lambda value: value[0]))
        batch_rows = [(value[0], facts._pack(value)) for value in values]
        calls = []

        def sort(*arguments):
            calls.append(True)
            return native[0](*arguments)

        actual = batches.write_initial(work, batch_rows, mode, sort)
        assert calls and actual.read_bytes() == expected.read_bytes()
        assert list(facts._sort_rows(work, actual)) == sorted(values, key=lambda value: value[0])


@pytest.mark.parametrize("mode", ["raw", "family"])
def test_route_sort_preserves_path_ties(tmp_path, monkeypatch, native, mode):
    route_values = _values(mode, 511, tied=True)
    entries = [source_entry[1] for source_entry in route_values]
    key = (
        facts._raw_order
        if mode == "raw"
        else lambda source_entry: (route._bucket(source_entry, 256), facts._family_order(source_entry))
    )
    with _work(tmp_path / "legacy") as work:
        expected_records = list(facts._sorted(work, iter(entries), key))
    assert expected_records != entries, "the existing level/path order is not global ingest stability"
    count_by_operation = {"sort": 0, "merge": 0}
    written_batches = []
    original_sort_batch, original_route_run = facts._sort_batch, facts._route_run

    def sort_batch(*arguments):
        written_batches.append(arguments[1])
        return original_sort_batch(*arguments)

    def route_run(*arguments):
        assert written_batches and not written_batches[-1], "completed batch must be released before carry merge"
        return original_route_run(*arguments)

    def sort(*arguments):
        count_by_operation["sort"] += 1
        return native[0](*arguments)

    def merge(*arguments):
        count_by_operation["merge"] += 1
        return native[1](*arguments)

    monkeypatch.setattr(batches, "backend", lambda: (sort, merge))
    monkeypatch.setattr(facts, "_sort_batch", sort_batch)
    monkeypatch.setattr(facts, "_route_run", route_run)
    with _work(tmp_path / "native") as work:
        actual_records = list(facts._sorted(work, iter(entries), key, route_key=mode))
        assert actual_records == expected_records and count_by_operation["sort"]
        if batches._workspace(10) is None:
            assert count_by_operation["merge"] == 0, (
                "compressed frames decline before native decode without a workspace bound"
            )
        assert not work.paths and work.used == 0


@pytest.mark.parametrize("fallback", [False, True])
def test_refill_keeps_every_suffix(tmp_path, native, fallback):
    values = _values("raw", 384)
    expected = sorted(values, key=lambda value: value[0])
    calls, budgets = [], []

    def merge(mode, lanes, memory, maximum, output):
        admitted = len(lanes) * 1024 + sum(
            2 * len(raw) + 5 * len(stored) + 1024 for frames, _eof in lanes for raw, stored in frames
        )
        assert admitted <= memory
        calls.append(tuple((len(frames), eof) for frames, eof in lanes))
        budgets.append(admitted)
        if fallback and len(calls) == 2:
            return None
        return native[1](mode, lanes, memory, maximum, output)

    with _work(tmp_path / "work") as work:
        paths = _runs(work, values)
        before = _saved(paths)
        actual = batches.write_merged(work, paths, "raw", merge)
        assert list(facts._sort_rows(work, actual)) == expected
        assert _saved(paths) == before and len(calls) > 1
        assert max(budgets) <= work.limits.memory_bytes


def test_equal_digest_unicode_order(tmp_path, native):
    digest = b"x" * 32
    texts = ["😀", "Ω", "é", "a\u0301", "a", "á"]
    values = [((0, (0, digest, text, (1, 0))), ((text, digest), b"opaque", 1, (1, 0), False)) for text in texts]
    with _work(tmp_path / "work") as work:
        paths = _runs(work, values, 3)
        actual = batches.write_merged(work, paths, "family", native[1])
        assert list(facts._sort_rows(work, actual)) == sorted(values, key=lambda value: value[0])


def test_unknown_key_after_prefix(tmp_path, native):
    values = _values("raw", 256)
    calls = []

    def merge(*arguments):
        calls.append(True)
        if len(calls) == 2:
            lanes = arguments[1]
            raw, stored = lanes[0][0][0]
            changed = (facts._pack((7, b"unsupported")), stored)
            altered = ((changed, *lanes[0][0][1:]), lanes[0][1]), *lanes[1:]
            assert native[1](arguments[0], altered, *arguments[2:]) is None
            return None
        return native[1](*arguments)

    with _work(tmp_path / "work") as work:
        paths = _runs(work, values)
        actual = batches.write_merged(work, paths, "raw", merge)
        assert len(calls) == 2 and list(facts._sort_rows(work, actual)) == sorted(values, key=lambda value: value[0])


def _legacy_lane(work, path, ordered, change_last):
    with path.open("xb") as stream:
        for index, record in enumerate(ordered):
            payload = facts._pack(record)
            if change_last and index == len(ordered) - 1:
                payload = b"\xdc\x00\x02" + payload[1:]
            size = facts._LENGTH.size + len(payload)
            work.charge(size)
            work.paths[path] += size
            stream.write(facts._LENGTH.pack(len(payload)))
            stream.write(payload)


def test_legacy_frame_after_prefix(tmp_path, native):
    values = _values("raw", 384)
    calls = []

    def merge(*arguments):
        calls.append(True)
        return native[1](*arguments)

    with _work(tmp_path / "work") as work:
        paths = []
        for lane in range(8):
            ordered = sorted(values[lane::8], key=lambda value: value[0])
            path = work.path()
            _legacy_lane(work, path, ordered, lane == 0)
            paths.append(path)
        before = _saved(paths)
        actual = batches.write_merged(work, paths, "raw", merge)
        assert len(calls) > 1 and _saved(paths) == before
        assert list(facts._sort_rows(work, actual)) == sorted(values, key=lambda value: value[0])


def test_frozen_kernel_vectors(native):
    path = Path(__file__).parents[1] / "support/ptg2_scanner/src/custom_import_route_merge_vectors.json"
    vectors = json.loads(path.read_text())
    assert len(vectors) == 18
    for vector in vectors:
        lanes = tuple(
            (
                tuple((bytes.fromhex(frame["raw"]), bytes.fromhex(frame["stored"])) for frame in lane["frames"]),
                lane["eof"],
            )
            for lane in vector["lanes"]
        )
        arguments = vector["mode"], lanes, vector["memory_bytes"], 64 * 1024**2, vector["max_bytes"], vector["max_rows"]
        if vector["result"] == "error":
            with pytest.raises(ValueError):
                native[1](*arguments)
        else:
            actual = native[1](*arguments)
            if vector["result"] == "unsupported":
                assert actual is None, vector["name"]
            else:
                assert actual == (bytes.fromhex(vector["expected_bytes"]), tuple(vector["consumed"])), vector["name"]


def test_bytes_abi_checks_before_copy(native):
    class ChangedBytes(bytes):
        pass

    with pytest.raises(ValueError, match="immutable bytes"):
        native[0]("raw", (ChangedBytes(b"x"),), 65536, 1024)
    with pytest.raises(ValueError, match="byte bound"):
        native[0]("raw", (b"x",), True, 1024)
    assert native[0]("raw", (b"large" * 1024,), 1024, 65536) is None


@pytest.mark.parametrize("failure", ["cancel", "budget", "partial", "zero"])
def test_failed_merge_keeps_inputs(tmp_path, monkeypatch, native, failure):
    with _work(tmp_path / "work") as work:
        paths = _runs(work, _values("raw", 80))
        before, used = _saved(paths), work.used
        written_sizes = []
        real_write = batches.os.write

        def write(fd, payload):
            written_sizes.append(len(payload))
            if failure == "partial":
                real_write(fd, payload[:1])
                raise OSError("synthetic partial write")
            return real_write(fd, payload)

        def merge(*arguments):
            if failure == "cancel":
                raise InterruptedError("synthetic canceled merge")
            if failure == "zero":
                return b"", tuple(0 for _ in arguments[1])
            return native[1](*arguments)

        if failure == "budget":

            def charge(_size):
                raise facts.SourceFactBudgetExceeded("synthetic bounded disk exhaustion")

            monkeypatch.setattr(work, "charge", charge)
        monkeypatch.setattr(batches.os, "write", write)
        expected = InterruptedError if failure == "cancel" else OSError
        with pytest.raises(expected):
            batches.write_merged(work, paths, "raw", merge)
        assert _saved(paths) == before and set(work.paths) == set(paths) and work.used == used
        if failure == "budget":
            assert written_sizes == []


@pytest.mark.parametrize("damage", ["checksum", "inode", "symlink", "writable"])
def test_changed_input_never_publishes(tmp_path, monkeypatch, native, damage):
    with _work(tmp_path / "work", 8 * 1024**2) as work:
        route_records = _values("raw", 32)
        first = decode_record(route_records[0][1])
        first = replace(first, values=tuple(dict(first.values, score="synthetic" * 1024).items()))
        route_records[0] = facts._raw_order(encode_record(first)), encode_record(first)
        paths = _runs(work, route_records, compressed=True)
        path, backup = paths[0], work.root / "retained-original"
        original = path.read_bytes()

        def merge(*arguments):
            if damage == "inode":
                path.rename(backup)
                path.write_bytes(original)
            return native[1](*arguments)

        if damage == "checksum":
            changed = bytearray(original)
            changed[-1] ^= 1
            path.write_bytes(changed)
        elif damage == "symlink":
            path.rename(backup)
            path.symlink_to(backup)
        elif damage == "writable":
            path.chmod(0o666)
        try:
            with pytest.raises((ValueError, OSError)):
                batches.write_merged(work, paths, "raw", merge)
            assert set(work.paths) == set(paths)
            if backup.exists():
                assert backup.read_bytes() == original
        finally:
            if path.is_symlink() or backup.exists():
                path.unlink()
                backup.rename(path)
            path.chmod(0o600)


def test_generic_sort_never_uses_backend(tmp_path, monkeypatch):
    def refused():
        raise AssertionError("generic sort must not discover the route ABI")

    monkeypatch.setattr(batches, "backend", refused)
    with _work(tmp_path / "work") as work:
        assert list(facts._sorted(work, [3, 1, 2], lambda value: value)) == [1, 2, 3]


def _compressed_run(work, damage=None):
    record = _values("raw", 1)[0]
    raw = facts._pack(record)
    compressed = batches.zstd.compress(raw, options={batches.zstd.CompressionParameter.checksum_flag: True})
    if damage == "checksum":
        compressed = compressed[:-1] + bytes([compressed[-1] ^ 1])
    elif damage == "truncated":
        compressed = compressed[:-1]
    elif damage == "trailing":
        compressed += b"trailing"
    elif damage == "window":
        compressed = b"\x28\xb5\x2f\xfd\x00\xa0" + compressed[6:]
    size = len(raw) - 1 if damage == "oversized" else len(raw)
    return work.write([(facts._COMPRESSED_SORT, size, compressed)]), record


def test_decode_reservation_precedes_allocation(tmp_path, monkeypatch):
    with _work(tmp_path / "work", 8 * 1024**2) as work:
        path, expected = _compressed_run(work)
        budget = batches._Budget(work.limits.memory_bytes, 1)
        decode = facts._decode_sort_frame
        observed_sizes = []

        def checked(*arguments):
            size = arguments[1]
            window = min(31, max(10, (size - 1).bit_length()))
            observed_sizes.append(budget.used)
            assert budget.used >= 2 * (size + 1) + batches._workspace(window)
            return decode(*arguments)

        monkeypatch.setattr(facts, "_decode_sort_frame", checked)
        reader = batches._Reader(work, path)
        try:
            assert reader.read(budget) == "ready"
            assert facts._unpack(reader.frames[0].raw) == expected
            assert observed_sizes and budget.peak <= budget.limit
            assert budget.used == 1024 + reader.frames[0].charge
        finally:
            reader.stream.close()


@pytest.mark.parametrize("unsupported", [False, True])
def test_decode_declines_before_allocation(tmp_path, monkeypatch, unsupported):
    def denied(*_arguments):
        raise AssertionError("decoder must not be constructed")

    with _work(tmp_path / "work") as work:
        path, _expected = _compressed_run(work)
        if unsupported:
            monkeypatch.setattr(batches, "_has_pinned_decoder", lambda: False)
        monkeypatch.setattr(facts, "_decode_sort_frame", denied)
        budget = batches._Budget(work.limits.memory_bytes, 1)
        reader = batches._Reader(work, path)
        try:
            assert reader.read(budget) == ("fallback" if unsupported else "budget")
            if unsupported:
                assert reader.frames[0].raw is None
            else:
                assert reader.stream.tell() == 0 and not reader.frames and budget.used == 1024
        finally:
            reader.stream.close()


@pytest.mark.parametrize("damage", ["checksum", "truncated", "trailing", "window", "oversized"])
def test_compressed_corruption_never_falls_back(tmp_path, native, damage):
    with _work(tmp_path / "work", 8 * 1024**2) as work:
        path, _expected = _compressed_run(work, damage)
        original = _saved([path])
        with pytest.raises(ValueError, match="source fact compressed sort"):
            batches.write_merged(work, [path], "raw", native[1])
        assert _saved([path]) == original and set(work.paths) == {path}


def test_decoder_capability_is_exact(monkeypatch):
    assert batches._has_pinned_decoder()
    assert batches._workspace(10) == 1024**2 + 1024 + 3 * 1024 + 64 + 16 * 1024
    assert batches._workspace(26) == 1024**2 + 2**26 + 3 * 128 * 1024 + 64 + 16 * 1024
    monkeypatch.setattr(batches.zstd, "zstd_version_info", (1, 5, 8))
    assert batches._workspace(10) is None
    assert not batches._is_standard_frame(b"\x28\xb5\x2f\xfd\x01")
    assert not batches._is_standard_frame(b"\x50\x2a\x4d\x18\x00")
    assert not batches._is_standard_frame(b"\x28\xb5\x2f")


def test_compressed_bytes_survive_native_merge(tmp_path, native):
    with _work(tmp_path / "work", 8 * 1024**2) as work:
        values = _values("raw", 80)
        paths = _runs(work, values, compressed=True)
        calls = []

        def merge(*arguments):
            calls.append(True)
            return native[1](*arguments)

        expected = sorted(values, key=lambda record: record[0])
        actual = batches.write_merged(work, paths, "raw", merge)
        frame_by_order = {}
        for path in paths:
            with path.open("rb") as stream:
                while prefix := stream.read(facts._LENGTH.size):
                    stored = stream.read(facts._LENGTH.unpack(prefix)[0])
                    record = facts._decoded_sort_entry(work, facts._unpack(stored))
                    frame_by_order[record[0]] = prefix + stored
        assert calls and actual.read_bytes() == b"".join(frame_by_order[record[0]] for record in expected)
        assert list(facts._sort_rows(work, actual)) == expected


def test_equal_keys_preserve_lane_payloads(tmp_path, native):
    values = _values("raw", 80, tied=True)
    with _work(tmp_path / "work") as work:
        paths = _runs(work, values)
        expected_records = list(facts._merged(work, paths))
        expected_bytes = b"".join(path.read_bytes() for path in paths)
        actual = batches.write_merged(work, paths, "raw", native[1])
        assert actual.read_bytes() == expected_bytes
        assert list(facts._sort_rows(work, actual)) == expected_records


@pytest.mark.parametrize("mode", ["raw", "family"])
def test_initial_unsupported_preserves_codec(tmp_path, mode):
    values = _values(mode, 32)
    with _work(tmp_path / "work", 1024**2) as work:
        expected = facts._write_sort_run(work, sorted(values, key=lambda record: record[0]))
        batch_rows = [(record[0], facts._pack(record)) for record in reversed(values)]
        actual = batches.write_initial(work, batch_rows, mode, lambda *_arguments: None)
        assert actual.read_bytes() == expected.read_bytes()


def test_serial_decode_keeps_check_order(tmp_path, monkeypatch):
    with _work(tmp_path / "work") as work:
        raw = facts._pack((1, b"opaque"))
        frame = batches.zstd.compress(raw)
        events = []
        unpack = facts._unpack

        def checked():
            events.append("check")

        def decode(payload):
            events.append("unpack")
            return unpack(payload)

        monkeypatch.setattr(work, "_io_check", checked)
        monkeypatch.setattr(facts, "_unpack", decode)
        assert facts._decoded_sort_entry(work, (facts._COMPRESSED_SORT, len(raw), frame)) == (1, b"opaque")
        assert events == ["check", "unpack", "check"]
