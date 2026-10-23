# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic canonical metadata, bounded accumulation and identity refusals."""

from __future__ import annotations

import hashlib
import json
import struct
import weakref
from dataclasses import FrozenInstanceError, replace

import pytest

from process.custom_import import prepared_source_metadata as metadata
from process.custom_import.object_processing_policy import ObjectProcessingPolicy
from tests.test_custom_import_object_processing_policy import _document

_MAX_BIGINT = (1 << 63) - 1
_IDENTITY_IDS = (
    "execution_id",
    "capture_bundle_id",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "source_binding_revision_id",
)
_IDENTITY_HASHES = (
    "source_binding_sha256",
    "request_identity_sha256",
    "raw_manifest_sha256",
    "processing_policy_sha256",
)


def _policy(**prepared_changes):
    document = _document()
    document["prepared"].update(prepared_changes)
    return ObjectProcessingPolicy.from_mapping(document)


def _identity(policy=None, **changes):
    policy = policy or _policy()
    return metadata.PreparedSourceIdentity(
        **(
            dict(zip(_IDENTITY_IDS, range(1, 7), strict=True))
            | dict(
                source_binding_sha256=b"\x01" * 32,
                request_identity_sha256=b"\x02" * 32,
                raw_manifest_sha256=b"\x03" * 32,
                processing_policy_sha256=bytes.fromhex(policy.digest),
            )
            | changes
        )
    )


def _part(**changes):
    return metadata.PreparedPartMetadata(
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


def _stream(*, stream_slot=1, policy=None, parts=None):
    return metadata.summarize_prepared_stream(
        [_part(stream_slot=stream_slot)] if parts is None else parts,
        stream_slot=stream_slot,
        policy=policy or _policy(),
    )


def _empty_part(stream_slot=1):
    return _part(stream_slot=stream_slot, record_count=0, byte_count=0, payload_sha256=hashlib.sha256(b"").digest())


def _golden_streams():
    return (
        _stream(
            parts=iter(
                [
                    _part(),
                    _part(
                        part_ordinal=2,
                        first_source_ordinal=3,
                        record_count=1,
                        byte_count=8,
                        payload_sha256=b"\xff" * 32,
                    ),
                ]
            )
        ),
        _stream(stream_slot=2, parts=iter([_empty_part(2)])),
    )


def test_part_manifest_is_closed_canonical_and_hashes_exact_utf8():
    part = _part()
    expected = (
        '{"byte_count":10,"contract":"custom-import/prepared-source-part/v1",'
        '"first_source_ordinal":1,"part_ordinal":1,"payload_sha256":'
        '"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",'
        '"record_count":2,"stream_slot":1}'
    )
    assert part.canonical == expected
    assert part.digest == hashlib.sha256(expected.encode("utf-8")).digest()
    assert part.digest.hex() == "5cd3272ddaa62d979e752ba66954ea5a26c88ca7b529fd2b9f90a61104440d26"
    assert json.loads(part.canonical) == part.to_mapping()
    assert set(part.to_mapping()) == {
        "contract",
        "stream_slot",
        "part_ordinal",
        "first_source_ordinal",
        "record_count",
        "byte_count",
        "payload_sha256",
    }


def test_stream_hash_is_ordered_uint64_big_endian_length_framing():
    parts = [
        _part(),
        _part(part_ordinal=2, first_source_ordinal=3, record_count=1, byte_count=8, payload_sha256=b"\xff" * 32),
    ]
    stream = _stream(parts=iter(parts))
    framed = b"custom-import/prepared-source-part-set/v1\0" + b"".join(
        struct.pack(">Q", len(part.canonical.encode("utf-8"))) + part.canonical.encode("utf-8") for part in parts
    )
    assert stream.part_set_sha256 == hashlib.sha256(framed).digest()
    assert stream.part_set_sha256.hex() == "0d22ccee58b9795760bdab91139948179b05b09e75def5ad98813257d9f50a00"
    assert (stream.part_count, stream.record_count, stream.byte_count) == (2, 3, 18)
    assert stream.manifest_byte_count == sum(len(part.canonical.encode("utf-8")) for part in parts)
    assert stream.part_set_sha256 != hashlib.sha256(b"".join(part.digest for part in parts)).digest()


@pytest.mark.asyncio
@pytest.mark.parametrize("page_size", (1, 2, 3))
async def test_incremental_pages_match_iterable_summary_bytes(page_size):
    policy = _policy()

    def part_at(ordinal):
        return _part(part_ordinal=ordinal, first_source_ordinal=ordinal, record_count=1)

    async def pages():
        for first in range(1, 4, page_size):
            yield tuple(part_at(ordinal) for ordinal in range(first, min(first + page_size, 4)))

    fold = metadata.PreparedStreamFold(1, policy)
    async for page in pages():
        for part in page:
            fold.add(part)
    expected = _stream(policy=policy, parts=(part_at(ordinal) for ordinal in range(1, 4)))
    actual = fold.finish()
    assert actual == expected
    assert actual.to_mapping() == expected.to_mapping()
    assert fold.finish() == actual
    assert metadata.prepared_source_summary(_identity(policy), [actual], stream_count=1, policy=policy) == (
        metadata.prepared_source_summary(_identity(policy), [expected], stream_count=1, policy=policy)
    )


def test_incremental_fold_releases_each_consumed_part():
    fold = metadata.PreparedStreamFold(1, _policy())
    for ordinal in range(1, 4):
        part = _part(part_ordinal=ordinal, first_source_ordinal=ordinal, record_count=1)
        reference = weakref.ref(part)
        fold.add(part)
        del part
        assert reference() is None
    assert fold.finish().part_count == 3


@pytest.mark.parametrize(
    "next_part",
    (
        _part(stream_slot=2, part_ordinal=2, first_source_ordinal=3),
        _part(),
        _part(part_ordinal=3, first_source_ordinal=3),
        _part(part_ordinal=2, first_source_ordinal=4),
        None,
    ),
)
def test_incremental_fold_rejects_discontinuous_later_pages(next_part):
    fold = metadata.PreparedStreamFold(1, _policy())
    fold.add(_part())
    with pytest.raises(metadata.PreparedSourceMetadataError, match="^prepared_source_metadata_invalid$"):
        fold.add(next_part)


def test_incremental_empty_stream_still_requires_one_canonical_part():
    fold = metadata.PreparedStreamFold(1, _policy())
    with pytest.raises(metadata.PreparedSourceMetadataError):
        fold.finish()
    empty = _empty_part()
    fold.add(empty)
    assert fold.finish() == _stream(parts=[empty])
    with pytest.raises(metadata.PreparedSourceMetadataError):
        fold.add(_part(part_ordinal=2, first_source_ordinal=1))


@pytest.mark.parametrize("stream_slot,policy", ((0, _policy()), (10, _policy()), (True, _policy()), (1, None)))
def test_incremental_fold_rejects_invalid_scope_before_consumption(stream_slot, policy):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.PreparedStreamFold(stream_slot, policy)


def test_header_has_exact_pins_complete_streams_and_separate_hash_domain():
    streams = _golden_streams()
    result = metadata.prepared_source_summary(_identity(), iter(streams), stream_count=2, policy=_policy())
    document = json.loads(result.canonical)
    assert set(document) == {"contract", *_IDENTITY_IDS, *_IDENTITY_HASHES, "streams"}
    assert document["contract"] == metadata.PREPARED_SOURCE_CONTRACT
    assert document["source_binding_sha256"] == (b"\x01" * 32).hex()
    assert document["processing_policy_sha256"] == _policy().digest
    assert document["streams"] == [stream.to_mapping() for stream in streams]
    assert set(document["streams"][0]) == {
        "stream_slot",
        "part_count",
        "record_count",
        "byte_count",
        "manifest_byte_count",
        "part_set_sha256",
    }
    assert result.streams == streams
    assert (
        result.digest == hashlib.sha256(b"custom-import/prepared-source-rows/v1:" + result.canonical.encode()).digest()
    )
    assert result.digest != hashlib.sha256(result.canonical.encode()).digest()
    assert result.digest.hex() == "1ad1b005ccaa33ef13885c1ba1a706eecd95e417819ec26e8d67266c6e93269e"


def test_empty_stream_requires_its_single_canonical_part_and_counts_manifest_bytes():
    part = _empty_part()
    stream = _stream(parts=iter([part]))
    assert (stream.part_count, stream.record_count, stream.byte_count) == (1, 0, 0)
    assert stream.manifest_byte_count == len(part.canonical.encode())
    assert (
        stream.part_set_sha256
        == hashlib.sha256(
            b"custom-import/prepared-source-part-set/v1\0"
            + struct.pack(">Q", len(part.canonical.encode()))
            + part.canonical.encode()
        ).digest()
    )
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _stream(parts=iter(()))
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _stream(parts=iter([part, _part(part_ordinal=2, first_source_ordinal=1)]))


@pytest.mark.parametrize(
    "changes",
    [
        {"part_ordinal": 2},
        {"first_source_ordinal": 2},
        {"byte_count": 1},
        {"payload_sha256": b"z" * 32},
    ],
)
def test_empty_part_refuses_noncanonical_positions_bytes_or_hash(changes):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        replace(_empty_part(), **changes)


@pytest.mark.parametrize("field", ["stream_slot", "part_ordinal", "first_source_ordinal", "record_count", "byte_count"])
@pytest.mark.parametrize("invalid", [None, True, False, -1, 1.0, "1"])
def test_part_integer_fields_reject_wrong_types_and_negative_values(field, invalid):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _part(**{field: invalid})


@pytest.mark.parametrize(
    "changes",
    [
        {"stream_slot": 0},
        {"stream_slot": 10},
        {"part_ordinal": 0},
        {"part_ordinal": 131_073},
        {"first_source_ordinal": 0},
        {"first_source_ordinal": _MAX_BIGINT},
        {"record_count": 1_000_001},
        {"byte_count": 0},
        {"byte_count": 64 * 1024 * 1024 + 1},
        {"payload_sha256": b"short"},
        {"payload_sha256": "0" * 64},
        {"payload_sha256": bytearray(32)},
    ],
)
def test_part_bounds_and_immutable_hash_type_are_enforced(changes):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _part(**changes)


def test_part_native_maxima_are_inclusive_before_policy_admission():
    part = _part(
        stream_slot=9,
        part_ordinal=131_072,
        first_source_ordinal=_MAX_BIGINT,
        record_count=1,
        byte_count=64 * 1024 * 1024,
    )
    assert part.first_source_ordinal == _MAX_BIGINT
    assert part.byte_count == 64 * 1024 * 1024


@pytest.mark.parametrize("damage", ["space", "extra", "duplicate", "contract", "number", "digest", "type"])
def test_retained_manifest_is_compared_to_reconstruction_without_arbitrary_json_parse(monkeypatch, damage):
    part = _part()

    def unexpected_parse(*_args, **_kwargs):
        raise AssertionError("retained manifests must not be parsed")

    monkeypatch.setattr(json, "loads", unexpected_parse)
    part.verify_manifest(part.canonical, part.digest)
    canonical_by_damage = {
        "space": " " + part.canonical,
        "extra": part.canonical[:-1] + ',"extra":1}',
        "duplicate": part.canonical[:-1] + ',"record_count":2}',
        "contract": part.canonical.replace("part/v1", "part/v2"),
        "number": part.canonical.replace('"record_count":2', '"record_count":2.0'),
        "type": part.canonical.encode(),
    }
    canonical = canonical_by_damage.get(damage, part.canonical)
    digest = b"d" * 32 if damage == "digest" else part.digest
    with pytest.raises(metadata.PreparedSourceMetadataError, match="^prepared_source_metadata_invalid$"):
        part.verify_manifest(canonical, digest)


@pytest.mark.parametrize(
    "parts",
    [
        [_part(stream_slot=2)],
        [_part(part_ordinal=2)],
        [_part(first_source_ordinal=2)],
        [_part(), _part()],
        [_part(), _part(part_ordinal=3, first_source_ordinal=3)],
        [_part(), _part(part_ordinal=2, first_source_ordinal=4)],
        [{}],
    ],
)
def test_stream_refuses_wrong_slots_duplicates_gaps_and_untyped_parts(parts):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _stream(parts=iter(parts))


@pytest.mark.parametrize("defect", ["row_limit", "part_bytes", "total_bytes", "source_records"])
def test_stream_obeys_part_and_aggregate_policy_limits(defect):
    policy = _policy()
    parts = [_part()]
    if defect == "row_limit":
        policy = _policy(part_row_limit=1)
    elif defect == "part_bytes":
        parts = [_part(byte_count=2049)]
    elif defect == "total_bytes":
        policy = _policy(maximum_total_bytes=2048)
        parts = [_part(byte_count=1500), _part(part_ordinal=2, first_source_ordinal=3, byte_count=1500)]
    else:
        policy = _policy(part_row_limit=20)
        parts = [_part(record_count=11)]
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _stream(policy=policy, parts=iter(parts))


def test_stream_stops_consuming_at_first_part_budget_overrun():
    consumed_ordinals = []

    def parts():
        for ordinal in range(1, 100):
            consumed_ordinals.append(ordinal)
            yield _part(part_ordinal=ordinal, first_source_ordinal=ordinal, record_count=1)

    with pytest.raises(metadata.PreparedSourceMetadataError):
        _stream(parts=parts())
    assert consumed_ordinals == [1, 2, 3, 4]


@pytest.mark.parametrize("field", _IDENTITY_IDS)
@pytest.mark.parametrize("invalid", [None, True, 0, -1, 1.0, "1", _MAX_BIGINT + 1])
def test_identity_requires_positive_native_bigint_ids(field, invalid):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _identity(**{field: invalid})


@pytest.mark.parametrize("field", _IDENTITY_HASHES)
@pytest.mark.parametrize("invalid", [None, b"short", "0" * 64, bytearray(32)])
def test_identity_hashes_require_immutable_bytes32(field, invalid):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        _identity(**{field: invalid})


@pytest.mark.parametrize("field", (*_IDENTITY_IDS, *_IDENTITY_HASHES))
def test_every_identity_pin_changes_header_identity(field):
    original = _identity()
    changes = {field: getattr(original, field) + 1} if field in _IDENTITY_IDS else {field: b"x" * 32}
    if field == "processing_policy_sha256":
        with pytest.raises(metadata.PreparedSourceMetadataError):
            metadata.prepared_source_summary(
                replace(original, **changes), [_stream()], stream_count=1, policy=_policy()
            )
        return
    changed = metadata.prepared_source_summary(
        replace(original, **changes), [_stream()], stream_count=1, policy=_policy()
    )
    baseline = metadata.prepared_source_summary(original, [_stream()], stream_count=1, policy=_policy())
    assert changed.digest != baseline.digest


@pytest.mark.parametrize("slots,count", [([], 1), ([1], 2), ([2, 1], 2), ([1, 1], 2), ([1, 3], 2), ([1, 2, 3], 2)])
def test_header_requires_exact_declared_stream_slots_in_order(slots, count):
    streams = (_stream(stream_slot=slot) for slot in slots)
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(_identity(), streams, stream_count=count, policy=_policy())


@pytest.mark.parametrize("defect", ["parts", "records", "payload"])
def test_header_enforces_bundle_limits_across_individually_valid_streams(defect):
    policy = _policy()
    if defect == "parts":
        streams = [
            _stream(
                stream_slot=slot,
                parts=[
                    _part(stream_slot=slot, record_count=1),
                    _part(stream_slot=slot, part_ordinal=2, first_source_ordinal=2, record_count=1),
                ],
            )
            for slot in (1, 2)
        ]
    elif defect == "records":
        streams = [_stream(stream_slot=slot, parts=[_part(stream_slot=slot, record_count=6)]) for slot in (1, 2)]
    else:
        policy = _policy(maximum_total_bytes=2048)
        streams = [
            _stream(stream_slot=slot, policy=policy, parts=[_part(stream_slot=slot, byte_count=1500)])
            for slot in (1, 2)
        ]
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(_identity(policy), iter(streams), stream_count=2, policy=policy)


def test_header_limits_stream_consumption_and_refuses_policy_drift():
    consumed_slots = []

    def streams():
        for slot in range(1, 10):
            consumed_slots.append(slot)
            yield _stream(stream_slot=slot)

    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(_identity(), streams(), stream_count=2, policy=_policy())
    assert consumed_slots == [1, 2, 3]
    consumed_slots.clear()
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(
            _identity(processing_policy_sha256=b"x" * 32), streams(), stream_count=2, policy=_policy()
        )
    assert consumed_slots == []


@pytest.mark.parametrize("invalid", [None, True, 0, -1, 10, 1.0, "1"])
def test_header_declared_stream_count_is_a_bounded_integer(invalid):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(_identity(), [_stream()], stream_count=invalid, policy=_policy())


@pytest.mark.parametrize(
    "changes",
    [
        {"stream_slot": 0},
        {"part_count": 0},
        {"part_count": 131_073},
        {"record_count": -1},
        {"record_count": True},
        {"byte_count": -1},
        {"manifest_byte_count": 0},
        {"manifest_byte_count": 2 * 1024 * 1024 + 1},
        {"part_set_sha256": b"short"},
        {"record_count": 0},
        {"byte_count": 0},
        {"part_count": 3, "record_count": 2},
    ],
)
def test_typed_stream_summaries_preserve_scalar_and_count_coherence(changes):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        replace(_stream(), **changes)


@pytest.mark.parametrize("invalid", [None, {}, "synthetic"])
def test_policy_and_header_identity_require_validated_types(invalid):
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.summarize_prepared_stream([_part()], stream_slot=1, policy=invalid)
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(invalid, [_stream()], stream_count=1, policy=_policy())
    with pytest.raises(metadata.PreparedSourceMetadataError):
        metadata.prepared_source_summary(_identity(), [invalid], stream_count=1, policy=_policy())


def test_replay_payload_bytes_are_distinct_from_original_decoded_bytes_and_retention():
    document = _document()
    document["capture_limits"].update(maximum_record_bytes=16, maximum_decoded_bytes=16)
    document["maximum_dataset_retained_bytes"] = 0
    policy = ObjectProcessingPolicy.from_mapping(document)
    stream = _stream(policy=policy, parts=[_part(record_count=1, byte_count=32)])
    result = metadata.prepared_source_summary(_identity(policy), [stream], stream_count=1, policy=policy)
    assert result.streams[0].byte_count > policy.capture_limits.maximum_decoded_bytes


def test_outputs_are_immutable_and_mapping_copies_do_not_change_retained_text():
    part, identity, stream = _part(), _identity(), _stream()
    for instance, name, value in ((part, "byte_count", 12), (identity, "execution_id", 8), (stream, "record_count", 4)):
        with pytest.raises(FrozenInstanceError):
            setattr(instance, name, value)
    manifest = part.canonical
    part.to_mapping()["byte_count"] = 999
    assert part.canonical == manifest
    result = metadata.prepared_source_summary(identity, [stream], stream_count=1, policy=_policy())
    with pytest.raises(AttributeError):
        result.streams = ()
