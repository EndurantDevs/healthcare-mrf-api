# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded header progress grammar, immutable counters and EOF transitions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError, asdict, fields, replace

import pytest

from process.custom_import import prepared_source_progress as progress
from process.custom_import.definition import canonical_json
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from process.custom_import.prepared_source_metadata import PreparedPartMetadata
from tests.test_custom_import_object_processing_policy import _document


def _policy(**changes):
    document = _document()
    document["prepared"].update(changes)
    return ObjectProcessingPolicy.from_mapping(document)


def _part(**changes):
    return PreparedPartMetadata(
        **(
            dict(
                stream_slot=1,
                part_ordinal=1,
                first_source_ordinal=1,
                record_count=2,
                byte_count=10,
                payload_sha256=bytes(range(32)),
            )
            | changes
        )
    )


def _wire(streams):
    return canonical_json(
        {"contract": progress.PREPARED_PROGRESS_CONTRACT, "streams": [asdict(stream) for stream in streams]}
    )


def test_empty_progress_is_explicit_immutable_and_canonical():
    policy = _policy()
    initial = progress.empty_progress(2, policy)
    assert type(initial) is tuple and len(initial) == 2
    assert initial == (
        progress.PreparedStreamProgress(1, 0, 0, 0, 0, False),
        progress.PreparedStreamProgress(2, 0, 0, 0, 0, False),
    )
    assert progress.totals(initial) == (0, 0, 0, 0)
    assert tuple(field.name for field in fields(initial[0])) == (
        "stream_slot",
        "part_count",
        "record_count",
        "byte_count",
        "manifest_byte_count",
        "eof",
    )
    expected = (
        '{"contract":"custom-import/prepared-source-progress/v1","streams":['
        '{"byte_count":0,"eof":false,"manifest_byte_count":0,"part_count":0,"record_count":0,"stream_slot":1},'
        '{"byte_count":0,"eof":false,"manifest_byte_count":0,"part_count":0,"record_count":0,"stream_slot":2}]}'
    )
    assert progress.canonical_progress(initial) == expected
    assert progress.parse_progress(expected, 2, policy) == initial
    assert len(expected.encode("utf-8")) < progress.MAX_PROGRESS_BYTES
    with pytest.raises(FrozenInstanceError):
        initial[0].eof = True


def test_interleaved_appends_preserve_ordinals_counts_and_independent_eof():
    policy = _policy()
    initial = progress.empty_progress(2, policy)
    first = _part()
    after_first = progress.append_progress(initial, first, policy)
    closed_first = progress.close_stream(after_first, 1, policy)
    second = _part(stream_slot=2)
    after_second = progress.append_progress(closed_first, second, policy)
    third = _part(stream_slot=2, part_ordinal=2, first_source_ordinal=3, record_count=1, byte_count=8)
    finished = progress.close_stream(progress.append_progress(after_second, third, policy), 2, policy)
    assert progress.totals(initial) == (0, 0, 0, 0)
    assert progress.totals(finished) == (3, 5, 28, sum(len(part.canonical.encode()) for part in (first, second, third)))
    assert all(stream.eof for stream in finished)
    assert progress.close_stream(finished, 2, policy) is finished
    assert finished[0] is closed_first[0]
    assert progress.parse_progress(progress.canonical_progress(finished), 2, policy) == finished


def test_canonical_empty_part_requires_eof_and_forbids_any_later_append():
    policy = _policy()
    initial = progress.empty_progress(1, policy)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.close_stream(initial, 1, policy)
    empty = _part(record_count=0, byte_count=0, payload_sha256=hashlib.sha256(b"").digest())
    appended = progress.append_progress(initial, empty, policy)
    assert progress.totals(appended) == (1, 0, 0, len(empty.canonical.encode()))
    assert not appended[0].eof
    for part in (empty, _part(part_ordinal=2, first_source_ordinal=1)):
        with pytest.raises(progress.PreparedSourceProgressError):
            progress.append_progress(appended, part, policy)
    closed = progress.close_stream(appended, 1, policy)
    assert closed[0].eof and progress.close_stream(closed, 1, policy) is closed


@pytest.mark.parametrize("part", [_part(part_ordinal=2), _part(first_source_ordinal=2), _part(stream_slot=2), None, {}])
def test_append_refuses_wrong_part_position_slot_or_type(part):
    policy = _policy()
    initial = progress.empty_progress(1, policy)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.append_progress(initial, part, policy)
    assert progress.totals(initial) == (0, 0, 0, 0)


def test_append_after_eof_or_with_duplicate_or_gapped_metadata_is_rejected():
    policy = _policy()
    appended = progress.append_progress(progress.empty_progress(1, policy), _part(), policy)
    for part in (_part(), _part(part_ordinal=3, first_source_ordinal=3), _part(part_ordinal=2, first_source_ordinal=4)):
        with pytest.raises(progress.PreparedSourceProgressError):
            progress.append_progress(appended, part, policy)
    closed = progress.close_stream(appended, 1, policy)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.append_progress(closed, _part(part_ordinal=2, first_source_ordinal=3), policy)


@pytest.mark.parametrize("stream_count", [None, True, False, 0, -1, 10, 1.0, "1"])
def test_stream_count_is_an_exact_bounded_integer(stream_count):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.empty_progress(stream_count, _policy())
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.parse_progress(
            progress.canonical_progress(progress.empty_progress(1, _policy())), stream_count, _policy()
        )


@pytest.mark.parametrize("slot", [None, True, 0, -1, 2, 1.0, "1"])
def test_close_requires_an_exact_existing_slot(slot):
    policy = _policy()
    appended = progress.append_progress(progress.empty_progress(1, policy), _part(), policy)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.close_stream(appended, slot, policy)


@pytest.mark.parametrize("field", ["stream_slot", "part_count", "record_count", "byte_count", "manifest_byte_count"])
@pytest.mark.parametrize("invalid", [None, True, False, -1, 1.0, "1", 1 << 63])
def test_counter_fields_require_exact_nonnegative_native_integers(field, invalid):
    with pytest.raises(progress.PreparedSourceProgressError):
        replace(progress.PreparedStreamProgress(1, 0, 0, 0, 0, False), **{field: invalid})


@pytest.mark.parametrize("invalid", [None, 0, 1, 1.0, "false"])
def test_eof_requires_exact_boolean(invalid):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.PreparedStreamProgress(1, 1, 1, 1, 225, invalid)


@pytest.mark.parametrize(
    "changes",
    [
        {"record_count": 1},
        {"byte_count": 1},
        {"manifest_byte_count": 1},
        {"eof": True},
        {"stream_slot": 10},
    ],
)
def test_untouched_stream_cannot_claim_rows_bytes_manifests_or_eof(changes):
    with pytest.raises(progress.PreparedSourceProgressError):
        replace(progress.PreparedStreamProgress(1, 0, 0, 0, 0, False), **changes)


@pytest.mark.parametrize(
    "state",
    [
        (1, 2, 0, 0, 450, False),
        (1, 1, 0, 1, 225, False),
        (1, 1, 1, 0, 225, False),
        (1, 2, 1, 1, 450, False),
        (1, 1, 1, 1, 0, False),
        (1, 131_073, 131_073, 1, 300_000, False),
    ],
)
def test_populated_counter_coherence_is_required(state):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.PreparedStreamProgress(*state)


@pytest.mark.parametrize(
    "malformed",
    [
        None,
        b"{}",
        "{}",
        "[]",
        " ",
        "\ufeff{}",
        "\ud800",
        "🙂" * 5000,
        "[" * 5000 + "]" * 5000,
        '{"contract":"custom-import/prepared-source-progress/v1","streams":' + "[" * 1000 + "]" * 1000 + "}",
        "x" * (16 * 1024 + 1),
    ],
)
def test_bad_text_depth_and_utf8_budgets_fail_before_json_parse(monkeypatch, malformed):
    def unexpected_parse(*_args, **_kwargs):
        raise AssertionError("malformed or unbounded progress reached JSON parsing")

    monkeypatch.setattr(progress.json, "loads", unexpected_parse)
    with pytest.raises(progress.PreparedSourceProgressError, match="^prepared_source_progress_invalid$"):
        progress.parse_progress(malformed, 1, _policy())


@pytest.mark.parametrize(
    "damage",
    [
        "space",
        "newline",
        "extra",
        "duplicate",
        "version",
        "float",
        "leading_zero",
        "eof_type",
        "nested",
        "large_array",
        "integer_digits",
    ],
)
def test_noncanonical_or_open_grammar_is_refused_before_json_parse(monkeypatch, damage):
    canonical = progress.canonical_progress(progress.empty_progress(1, _policy()))
    document = json.loads(canonical)
    wire_by_damage = {
        "space": " " + canonical,
        "newline": canonical + "\n",
        "extra": canonical[:-1] + ',"extra":0}',
        "duplicate": canonical[:-1] + ',"streams":[]}',
        "version": canonical.replace("progress/v1", "progress/v2"),
        "float": canonical.replace('"byte_count":0', '"byte_count":0.0'),
        "leading_zero": canonical.replace('"part_count":0', '"part_count":00'),
        "eof_type": canonical.replace('"eof":false', '"eof":0'),
        "nested": canonical.replace('"byte_count":0', '"byte_count":[0]'),
        "large_array": canonical_json(document | {"streams": document["streams"] * 10}),
        "integer_digits": canonical.replace('"record_count":0', '"record_count":' + "9" * 1000),
    }
    monkeypatch.setattr(progress.json, "loads", lambda *_args, **_kwargs: pytest.fail("grammar abuse reached parser"))
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.parse_progress(wire_by_damage[damage], 1, _policy())


@pytest.mark.parametrize("slots", [[], [2], [1, 1], [1, 3], [2, 1]])
def test_only_complete_ordered_stream_slots_are_accepted(slots):
    streams = tuple(progress.PreparedStreamProgress(slot, 0, 0, 0, 0, False) for slot in slots)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.parse_progress(_wire(streams), 2, _policy())
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.totals(streams)


@pytest.mark.parametrize("defect", ["parts", "records", "payload"])
def test_parse_enforces_bundle_limits_across_streams(defect):
    policy = _policy(maximum_total_bytes=2048)
    values = {"parts": (2, 2, 10, 450), "records": (1, 6, 10, 225), "payload": (1, 1, 1500, 225)}[defect]
    streams = tuple(progress.PreparedStreamProgress(slot, *values, False) for slot in (1, 2))
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.parse_progress(_wire(streams), 2, policy)


@pytest.mark.parametrize(
    "part,policy",
    [
        (_part(), _policy(part_row_limit=1)),
        (_part(byte_count=2049), _policy()),
        (_part(record_count=11), _policy(part_row_limit=20)),
    ],
)
def test_append_enforces_part_and_source_record_limits(part, policy):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.append_progress(progress.empty_progress(1, policy), part, policy)


def test_failed_aggregate_append_leaves_old_progress_unchanged():
    policy = _policy(maximum_total_bytes=2048)
    original = progress.append_progress(progress.empty_progress(2, policy), _part(byte_count=1500), policy)
    before = progress.canonical_progress(original)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.append_progress(original, _part(stream_slot=2, byte_count=1500), policy)
    assert progress.canonical_progress(original) == before


def test_manifest_bytes_are_separate_and_basic_aggregate_bigint_overflow_is_refused():
    policy = _policy(maximum_total_bytes=2048)
    streams = tuple(progress.PreparedStreamProgress(slot, 1, 1, 1, 5000, False) for slot in (1, 2))
    assert progress.parse_progress(_wire(streams), 2, policy) == streams
    assert progress.totals(streams) == (2, 2, 2, 10_000)
    too_many_records = (
        progress.PreparedStreamProgress(1, 1, (1 << 63) - 1, 1, 225, False),
        progress.PreparedStreamProgress(2, 1, 1, 1, 225, False),
    )
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.totals(too_many_records)


@pytest.mark.parametrize("invalid", [None, [], {}, (None,)])
def test_operations_reject_untyped_progress(invalid):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.totals(invalid)
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.canonical_progress(invalid)


@pytest.mark.parametrize("policy", [None, {}, True])
def test_policy_failures_use_only_progress_error(policy):
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.empty_progress(1, policy)


def test_nine_streams_remain_bounded_and_declared_count_must_match():
    policy = _policy()
    initial = progress.empty_progress(9, policy)
    assert progress.parse_progress(progress.canonical_progress(initial), 9, policy) == initial
    with pytest.raises(progress.PreparedSourceProgressError):
        progress.parse_progress(progress.canonical_progress(initial), 8, policy)
