# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless SOURCE plan storage keeps original hashes and resource refusals."""

import random
from compression import zstd
from contextlib import closing

import msgpack
import pytest

from process.custom_import import family_shard_source_plan as planner
from process.custom_import.family_shard_facts import SourceFactBudgetExceeded, SourceFactLimits
from process.custom_import.family_shard_input import cleanup_spool
from tests.test_custom_import_family_shard_input import _child, _root
from tests.test_custom_import_family_shard_source_plan import _assert_oracle, _build, _case


async def test_compressed_plan_and_legacy_plan_keep_exact_original_pages(tmp_path, monkeypatch):
    rows = [_child(key=f"repeated-detail-{index % 4}-Ω", amount="2.000") for index in range(513)]
    rows[17], rows[31] = _child(npi=None), _child(key=None)
    case = await _case(
        tmp_path,
        {"root_source": [[], [_root(), _root(npi=None)], []], "detail_source": [rows[:256], [], rows[256:]]},
        row_limit=256,
    )
    try:
        with monkeypatch.context() as old:
            old.setattr(planner, "_stored_entry", lambda entry, _limits: entry)
            legacy = _build(tmp_path, case)
        legacy_packs = _assert_oracle(legacy, case)
        legacy_bytes, counts = legacy.file_bytes, legacy.counts
        planner.cleanup_source_plan(legacy)

        compact = _build(tmp_path, case)
        try:
            assert _assert_oracle(compact, case) == legacy_packs
            assert compact.counts == counts
            assert list(planner.read_source_parts(compact)) == list(case[3])
            assert compact.file_bytes < legacy_bytes // 3
            with closing(planner._entries(compact)) as entries:
                assert all(entry[0] in ("pack", "part") for entry in entries)
        finally:
            planner.cleanup_source_plan(compact)
    finally:
        cleanup_spool(case[0])


def test_compression_cannot_admit_an_oversized_original_record():
    entry = ("pack", b"a" * 4096)
    with pytest.raises(SourceFactBudgetExceeded, match="singleton"):
        planner._stored_entry(entry, SourceFactLimits(16384, 1024, 1024))


def test_incompressible_and_control_entries_keep_plain_storage():
    entry = ("pack", random.Random(405).randbytes(4096))
    assert planner._stored_entry(entry, SourceFactLimits(16384, 8192, 1024)) == entry
    header = ("header", "legacy-format")
    assert planner._stored_entry(header, SourceFactLimits(16384, 8192, 1024)) is header


@pytest.mark.parametrize(
    "damage",
    ["truncated", "trailing", "second_frame", "checksum", "too_short", "too_long", "over_budget", "bool", "not_bytes"],
)
def test_compressed_frame_corruption_and_expansion_fail_closed(damage):
    entry = ("pack", (b"same data" * 1000,))
    raw = msgpack.packb(entry, use_bin_type=True)
    stored = planner._stored_entry(entry, SourceFactLimits(65536, 32768, 1024))
    marker, size, frame = stored
    assert marker == planner._COMPRESSED_PACK
    invalid_size, invalid_frame = {
        "truncated": (size, frame[:-1]),
        "trailing": (size, frame + b"unexpected"),
        "second_frame": (size, frame + zstd.compress(b"second stream")),
        "checksum": (size, frame[:-1] + bytes([frame[-1] ^ 1])),
        "too_short": (size - 1, frame),
        "too_long": (size + 1, frame),
        "over_budget": (32769, frame),
        "bool": (True, frame),
        "not_bytes": (size, bytearray(frame)),
    }[damage]
    assert len(raw) < 32768
    with pytest.raises(ValueError, match="SOURCE compressed plan"):
        planner._decoded_entry((marker, invalid_size, invalid_frame), 32768)


def test_compressed_frame_cannot_replace_control_records():
    raw = msgpack.packb(("complete", (1, 2, 3)), use_bin_type=True)
    with pytest.raises(ValueError, match="record kind"):
        planner._decoded_entry((planner._COMPRESSED_PACK, len(raw), zstd.compress(raw)), 32768)


async def test_final_compression_reduces_combined_sort_and_plan_disk_budget(tmp_path, monkeypatch):
    case = await _case(
        tmp_path,
        {"root_source": [[_root()]], "detail_source": [[_child(key="same-long-key-" * 10)] * 257]},
        row_limit=128,
    )
    peaks = []
    actual = planner._Work.charge

    def observed(work, count):
        actual(work, count)
        peaks.append(work.peak_bytes)

    monkeypatch.setattr(planner._Work, "charge", observed)
    limits = SourceFactLimits(16 * 1024**2, 1024**2, 1024)
    try:
        with monkeypatch.context() as old:
            old.setattr(planner, "_stored_entry", lambda entry, _limits: entry)
            plain = _build(tmp_path, case, limits=limits)
        plain_peak = max(peaks)
        planner.cleanup_source_plan(plain)
        peaks.clear()
        compact = _build(tmp_path, case, limits=limits)
        compact_peak = max(peaks)
        _assert_oracle(compact, case)
        planner.cleanup_source_plan(compact)
        assert compact_peak < plain_peak
        cap = (plain_peak + compact_peak) // 2
        tight = SourceFactLimits(cap, min(cap, 1024**2), 1024)
        with monkeypatch.context() as old:
            old.setattr(planner, "_stored_entry", lambda entry, _limits: entry)
            with pytest.raises(SourceFactBudgetExceeded):
                _build(tmp_path, case, limits=tight)
        assert not (tmp_path / "source-plan").exists()
        compact = _build(tmp_path, case, limits=tight)
        try:
            _assert_oracle(compact, case)
        finally:
            planner.cleanup_source_plan(compact)
    finally:
        cleanup_spool(case[0])
