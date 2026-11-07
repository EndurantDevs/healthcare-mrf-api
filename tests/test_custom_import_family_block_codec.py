# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic family-block parity, exact sparse custody and fail-closed decoding."""

import hashlib
import json
import struct
from compression import zstd
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import msgpack
import pytest

from process.custom_import import runner_codec
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import RootFamily
from process.custom_import.family_block_codec import (
    FamilyBlockError,
    FamilyBlockLimits,
    FamilyBlockResourceLimit,
    FamilyRecord,
    SparseRecord,
    encode_family,
    encode_retained_family,
    verify_family,
)
from process.custom_import.materialization import TypedScalar


def _definition():
    document = json.loads((Path(__file__).parent / "fixtures/custom_import/v1_valid.json").read_text())
    document["schema"]["children"][0]["fields"].extend(
        [
            {"id": "day", "slot": 6, "type": "date", "nullable": True},
            {"id": "stamp", "slot": 7, "type": "timestamp", "nullable": True},
            {"id": "note", "slot": 8, "type": "string", "nullable": True},
            {"id": "enabled", "slot": 9, "type": "boolean", "nullable": True},
        ]
    )
    return CustomImportDefinition.from_mapping(document)


def _limits(**changes):
    return replace(FamilyBlockLimits(32 * 1024 * 1024, 12 * 1024 * 1024, 256, 1024), **changes)


def _case(count=3):
    definition = _definition()
    root = FamilyRecord(91, 17, 9, {"npi": "1234567893", "display_name": "Synthetic Clinic"})
    child_records = [
        FamilyRecord(
            1001 + ordinal * 17,
            33 + ordinal,
            ordinal * 11,
            {"rate_npi": "1234567893", "service_code": f"service-{ordinal}", "amount": Decimal("12.500")},
        )
        for ordinal in range(count)
    ]
    return definition, root, _ordered(definition, child_records)


def _ordered(definition, children):
    return tuple(
        sorted(
            children,
            key=lambda record: (
                record.sparse.key_sha256
                if record.sparse
                else runner_codec.child_key_hash(definition, "rates", record.values_by_field),
                record.sparse.key_text
                if record.sparse
                else runner_codec.child_key_document(definition, "rates", record.values_by_field),
                record.sparse.payload_text
                if record.sparse
                else runner_codec.record_payload(definition.child_fields, record.values_by_field),
            ),
        )
    )


def _family_hash(definition, root, children):
    return runner_codec.new_family_hash(
        definition,
        RootFamily(
            (root.values_by_field["npi"],),
            root.values_by_field,
            {"rates": tuple(row.values_by_field for row in children)},
        ),
    )


def _raw(frames):
    return b"".join(zstd.decompress(frame[16:]) for frame in frames)


def _objects(raw):
    position, values = 0, []
    while position < len(raw):
        size = struct.unpack("!Q", raw[position : position + 8])[0]
        position += 8
        values.append(msgpack.unpackb(raw[position : position + size], raw=False))
        position += size
    assert position == len(raw)
    return values


def _frames(values, limits):
    raw = b""
    for value in values:
        packed = msgpack.packb(value, use_bin_type=True)
        raw += struct.pack("!Q", len(packed)) + packed
    return tuple(
        struct.pack("!QQ", ordinal, len(part)) + zstd.compress(part, level=3)
        for ordinal, start in enumerate(range(0, len(raw), limits.fragment_bytes))
        for part in (raw[start : start + limits.fragment_bytes],)
    )


def _rebound(manifest, frames):
    digest = hashlib.sha256(b"custom-import/family-block/v1\x00storage\x00")
    for frame in frames:
        digest.update(struct.pack("!Q", len(frame)))
        digest.update(frame)
    return replace(
        manifest,
        storage_sha256=digest.digest(),
        fragment_count=len(frames),
        raw_bytes=len(_raw(frames)),
        stored_bytes=sum(8 + len(frame) for frame in frames),
    )


def _sparse(record, definition, collection, *, payload=None, key=None):
    fields = definition.root_fields if collection is None else definition.child_fields
    payload = payload if payload is not None else runner_codec.record_payload(fields, record.values_by_field)
    key = (
        key
        if key is not None
        else (
            runner_codec.root_key_document(definition, record.values_by_field)
            if collection is None
            else runner_codec.child_key_document(definition, collection, record.values_by_field)
        )
    )
    kind = "root" if collection is None else "child"
    original = SparseRecord(
        payload, key, runner_codec.digest_text(f"{kind}-payload", payload), runner_codec.digest_text(f"{kind}-key", key)
    )
    return replace(record, values_by_field=None, sparse=original)


def test_complete_family_matches_existing_digest_and_keeps_actual_gapped_ids():
    definition, root, children = _case()
    expected = _family_hash(definition, root, children)
    with encode_family(definition, root, {"rates": iter(children)}, expected, limits=_limits()) as encoded:
        frames = tuple(encoded.fragments())
        assert tuple(encoded.fragments()) == frames
        assert encoded.manifest.family_sha256 == expected
        with verify_family(definition, iter(frames), encoded.manifest, limits=_limits()) as verified:
            assert tuple(verified.records()) == (root,)
            decoded_items = tuple(verified.records("rates"))
            assert decoded_items == children
            assert [(row.revision_id, row.pack_id, row.source_ordinal) for row in decoded_items] == [
                (row.revision_id, row.pack_id, row.source_ordinal) for row in children
            ]
            assert next(verified.records("rates")) == children[0]
        assert verified._file.closed
    assert encoded._file.closed


def test_canonical_wire_omits_child_keys_and_hashes():
    definition, root, children = _case(1)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        values = _objects(_raw(tuple(encoded.fragments())))
        wire = values[3][1]
        assert wire[-1] is None
        assert not any(isinstance(cell, bytes) for cell in wire)
        assert runner_codec.child_key_document(definition, "rates", children[0].values_by_field) not in wire


def test_canonical_root_read_uses_family_proof_without_a_root_key_rehash(monkeypatch):
    definition, root, children = _case(1)
    expected = _family_hash(definition, root, children)
    original = runner_codec.digest_text

    def without_root_rehash(domain, document):
        assert domain != "root-key"
        return original(domain, document)

    monkeypatch.setattr(runner_codec, "digest_text", without_root_rehash)
    with (
        encode_family(definition, root, {"rates": children}, expected, limits=_limits()) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert next(verified.records()).values_by_field == root.values_by_field


def test_missing_null_dates_decimal_timestamp_and_independent_scalar_types():
    definition, root, children = _case(2)
    first = replace(
        children[0],
        values_by_field=dict(
            children[0].values_by_field,
            day=date(2024, 2, 29),
            stamp=datetime(2024, 1, 1, 3, tzinfo=timezone(timedelta(hours=3))),
            enabled=False,
        ),
        scalars={
            "amount": TypedScalar("integer", "value", integer_value=73),
            "day": TypedScalar("date", "value", date_value=date(2024, 2, 28)),
            "stamp": TypedScalar("timestamp", "value", timestamp_value=datetime(2024, 1, 2, tzinfo=UTC)),
        },
    )
    second = replace(
        children[1],
        values_by_field=dict(children[1].values_by_field, day=None, amount=None),
        scalars={"amount": TypedScalar("decimal", "null")},
    )
    children = _ordered(definition, (first, second))
    with (
        encode_family(
            definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
        ) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        decoded_by_key = {family_record.revision_id: family_record for family_record in verified.records("rates")}
        assert decoded_by_key[first.revision_id].values_by_field["stamp"] == datetime(2024, 1, 1, tzinfo=UTC)
        assert decoded_by_key[first.revision_id].scalars == first.scalars
        assert "note" not in decoded_by_key[first.revision_id].values_by_field
        assert decoded_by_key[second.revision_id].values_by_field["day"] is None
        assert decoded_by_key[second.revision_id].scalars == second.scalars


@pytest.mark.parametrize(
    "payload,key",
    [
        ("opaque prior payload", "opaque prior key"),
        (' {"original":true} ', '{"b":2,"a":1}'),
        ('{"value":"1.000"}', '{"value":"-0"}'),
        ('{"value":"2024-01-01T01:00:00+01:00"}', "original key"),
    ],
)
def test_retained_sparse_bytes_hashes_scalar_absence_and_inherited_digest_are_exact(payload, key):
    definition, root, children = _case(1)
    sparse_root = _sparse(root, definition, None, payload=payload, key=key)
    sparse_child = _sparse(children[0], definition, "rates", payload=payload, key=key)
    sparse_child = replace(sparse_child, scalars={"amount": TypedScalar("string", "value", string_value="independent")})
    inherited = b"i" * 32
    with (
        encode_retained_family(
            definition, sparse_root, {"rates": (sparse_child,)}, inherited, limits=_limits()
        ) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert verified.manifest.family_sha256 == inherited
        assert tuple(verified.records()) == (sparse_root,)
        assert tuple(verified.records("rates")) == (sparse_child,)


def test_sparse_and_canonical_records_share_one_retained_family_without_normalization():
    definition, root, children = _case(3)
    sparse = _sparse(children[1], definition, "rates", payload="exact legacy text")
    children = _ordered(definition, (children[0], sparse, children[2]))
    with (
        encode_retained_family(definition, root, {"rates": children}, b"r" * 32, limits=_limits()) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert tuple(verified.records("rates")) == children


@pytest.mark.parametrize(
    "damage", ["payload_text", "payload_hash", "key_text", "key_hash", "parent_text", "parent_hash"]
)
def test_sparse_raw_hash_checks_survive_a_rebound_storage_envelope(damage):
    definition, root, children = _case(1)
    child = _sparse(children[0], definition, "rates", payload="opaque retained payload", key="opaque retained key")
    parent = runner_codec.root_key_document(definition, root.values_by_field)
    child = replace(
        child,
        sparse=replace(
            child.sparse, parent_key_text=parent, parent_key_sha256=runner_codec.digest_text("root-key", parent)
        ),
    )
    with encode_retained_family(definition, root, {"rates": (child,)}, b"r" * 32, limits=_limits()) as encoded:
        values = _objects(_raw(tuple(encoded.fragments())))
        wire = values[3][1]
        match damage:
            case "payload_text":
                wire[3] += "changed"
            case "payload_hash":
                wire[5][1] = b"x" * 32
            case "key_text":
                wire[5][0] += "changed"
            case "key_hash":
                wire[5][2] = b"x" * 32
            case "parent_text":
                wire[5][3] += "changed"
            case _:
                wire[5][4] = b"x" * 32
        frames = _frames(values, _limits())
        with pytest.raises(FamilyBlockError, match="sparse .*digest differs"):
            verify_family(definition, frames, _rebound(encoded.manifest, frames), limits=_limits())


def test_sparse_original_hash_refusal_requires_no_json_parser():
    definition, root, children = _case(1)
    opaque = _sparse(root, definition, None, payload="original opaque text", key="original opaque key")
    invalid = replace(opaque, sparse=replace(opaque.sparse, payload_sha256=b"x" * 32))
    with pytest.raises(FamilyBlockError, match="sparse payload digest"):
        encode_retained_family(definition, invalid, {"rates": children}, b"r" * 32, limits=_limits())


def test_semantic_mode_cannot_be_changed_by_the_fragment_header():
    definition, root, children = _case(1)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        values = _objects(_raw(tuple(encoded.fragments())))
        values[0][-1] = True
        frames = _frames(values, _limits())
        with pytest.raises(FamilyBlockError, match="header differs"):
            verify_family(definition, frames, _rebound(encoded.manifest, frames), limits=_limits())


def test_root_hash_uses_encoded_snapshot_under_later_mutation():
    definition, root, children = _case(1)
    expected = _family_hash(definition, root, children)
    original_by_key = dict(root.values_by_field)

    def changing_input():
        root.values_by_field["display_name"] = "Later mutation"
        yield from children

    with (
        encode_family(definition, root, {"rates": changing_input()}, expected, limits=_limits()) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert next(verified.records()).values_by_field == original_by_key


@pytest.mark.parametrize(
    "scalar",
    [
        TypedScalar("string", "value", string_value="exact \u2603"),
        TypedScalar("integer", "value", integer_value=-(2**63)),
        TypedScalar("decimal", "value", decimal_value=Decimal("-0.0000")),
        TypedScalar("decimal", "value", decimal_value=Decimal("1E+2")),
        TypedScalar("boolean", "value", boolean_value=False),
        TypedScalar("date", "value", date_value=date(2024, 2, 29)),
        TypedScalar("timestamp", "value", timestamp_value=datetime(2024, 1, 1, 1, tzinfo=timezone(timedelta(hours=1)))),
        TypedScalar("string", "null"),
    ],
)
def test_every_independent_scalar_kind_preserves_its_actual_type_and_value(scalar):
    definition, root, children = _case(1)
    child = replace(children[0], scalars={"amount": scalar})
    with (
        encode_family(
            definition, root, {"rates": (child,)}, _family_hash(definition, root, (child,)), limits=_limits()
        ) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        actual = next(verified.records("rates")).scalars["amount"]
        assert actual == scalar
        if scalar.field_type == "decimal":
            assert actual.decimal_value.as_tuple() == scalar.decimal_value.as_tuple()
        if scalar.field_type == "timestamp":
            assert actual.timestamp_value.isoformat() == scalar.timestamp_value.isoformat()


def test_decompression_is_bounded_by_the_declared_fragment_size():
    definition, root, children = _case(1)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        oversized = struct.pack("!QQ", 0, 64) + zstd.compress(b"x" * 65536)
        with pytest.raises(FamilyBlockError, match="compressed fragment"):
            verify_family(definition, (oversized,), encoded.manifest, limits=_limits())


def test_fresh_encoding_never_falls_back_to_retained_mode():
    definition, root, children = _case(1)
    sparse = _sparse(root, definition, None, payload="opaque")
    with pytest.raises(FamilyBlockError, match="explicit retained"):
        encode_family(definition, sparse, {"rates": children}, b"r" * 32, limits=_limits())
    with pytest.raises(FamilyBlockError, match="semantic digest"):
        encode_family(definition, root, {"rates": children}, b"r" * 32, limits=_limits())


@pytest.mark.parametrize("damage", ["missing", "repeat", "reorder", "trailing", "truncate", "compressed"])
def test_no_records_are_exposed_from_an_incomplete_or_damaged_stream(damage):
    definition, root, children = _case()
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        frames = list(encoded.fragments())
        match damage:
            case "missing":
                del frames[-1]
            case "repeat":
                frames.insert(1, frames[0])
            case "reorder":
                frames[0], frames[1] = frames[1], frames[0]
            case "trailing":
                frames[-1] += b"trailing"
            case "truncate":
                frames[-1] = frames[-1][:-1]
            case _:
                frames[-1] = frames[-1][:16] + b"not zstd"
        with pytest.raises((FamilyBlockError, zstd.ZstdError)):
            verify_family(definition, frames, encoded.manifest, limits=_limits())


@pytest.mark.parametrize("column,new_value", [(0, 77), (1, 78), (2, 79), (4, [["amount", "integer", "value", 5]])])
def test_envelope_binds_ids_pack_ordinals_and_independent_scalars(column, new_value):
    definition, root, children = _case(1)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        values = _objects(_raw(tuple(encoded.fragments())))
        values[3][1][column] = new_value
        frames = _frames(values, _limits())
        census = _rebound(encoded.manifest, frames)
        manifest = replace(census, storage_sha256=encoded.manifest.storage_sha256)
        with pytest.raises(FamilyBlockError, match="storage digest"):
            verify_family(definition, frames, manifest, limits=_limits())


def test_semantic_digest_is_independent_of_a_rebound_storage_envelope():
    definition, root, children = _case(1)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        values = _objects(_raw(tuple(encoded.fragments())))
        values[3][1][3] = values[3][1][3].replace('"value":"12.5"', '"value":"13.5"')
        frames = _frames(values, _limits())
        with pytest.raises(FamilyBlockError, match="semantic digest"):
            verify_family(definition, frames, _rebound(encoded.manifest, frames), limits=_limits())


def test_digest_collisions_keep_exact_key_order_and_equal_digest_semantics(monkeypatch):
    original = runner_codec.digest_text
    monkeypatch.setattr(
        runner_codec, "digest_text", lambda domain, text: b"k" * 32 if domain == "child-key" else original(domain, text)
    )
    definition, root, children = _case(4)
    assert len({runner_codec.child_key_hash(definition, "rates", row.values_by_field) for row in children}) == 1
    with (
        encode_family(
            definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
        ) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert tuple(verified.records("rates")) == children
    with pytest.raises(FamilyBlockError, match="not ordered"):
        encode_family(
            definition, root, {"rates": reversed(children)}, _family_hash(definition, root, children), limits=_limits()
        )


def test_large_record_spills_across_fragments_without_logical_cap(tmp_path):
    definition, root, children = _case(1)
    child = replace(children[0], values_by_field=dict(children[0].values_by_field, note="a" * (2 * 1024 * 1024)))
    limits = _limits(fragment_bytes=16 * 1024, memory_bytes=128, spool_directory=str(tmp_path))
    with encode_family(
        definition, root, {"rates": (child,)}, _family_hash(definition, root, (child,)), limits=limits
    ) as encoded:
        assert encoded.manifest.fragment_count > 128
        assert encoded._file._rolled
        with verify_family(definition, encoded.fragments(), encoded.manifest, limits=limits) as verified:
            assert verified._file._rolled
            assert tuple(verified.records("rates")) == (child,)
    assert list(tmp_path.iterdir()) == []


def test_one_pass_child_input_and_full_fragment_exhaustion_precede_partial_reads():
    definition, root, children = _case(25)
    consumed, exhausted = [], []

    def inputs():
        for child in children:
            consumed.append(child.revision_id)
            yield child

    with encode_family(
        definition, root, {"rates": inputs()}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:

        def fragments():
            yield from encoded.fragments()
            exhausted.append(True)

        with verify_family(definition, fragments(), encoded.manifest, limits=_limits()) as verified:
            assert exhausted == [True]
            assert consumed == [child.revision_id for child in children]
            assert next(verified.records("rates")) == children[0]


def test_empty_collection_is_present_and_unknown_collection_refuses():
    definition, root, children = _case(0)
    with (
        encode_family(
            definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
        ) as encoded,
        verify_family(definition, encoded.fragments(), encoded.manifest, limits=_limits()) as verified,
    ):
        assert tuple(verified.records("rates")) == ()
        with pytest.raises(FamilyBlockError, match="collection"):
            tuple(verified.records("unknown"))
    with pytest.raises(FamilyBlockError, match="collections"):
        encode_family(definition, root, {}, b"a" * 32, limits=_limits())


def test_resource_refusal_and_error_paths_close_owned_spools(monkeypatch):
    from process.custom_import import family_block_codec as codec

    definition, root, children = _case(1)
    spools, original = [], codec.tempfile.SpooledTemporaryFile

    def opened(*args, **kwargs):
        result = original(*args, **kwargs)
        spools.append(result)
        return result

    monkeypatch.setattr(codec.tempfile, "SpooledTemporaryFile", opened)
    limits = FamilyBlockLimits(128, 128, 64, 64)
    with pytest.raises(FamilyBlockResourceLimit):
        encode_family(definition, root, {"rates": children}, _family_hash(definition, root, children), limits=limits)
    assert spools and all(spool.closed for spool in spools)
    with encode_family(
        definition, root, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
    ) as encoded:
        with pytest.raises(FamilyBlockError):
            verify_family(definition, (), encoded.manifest, limits=_limits())
        assert spools[-1].closed


@pytest.mark.parametrize("field,value", [("revision_id", 0), ("pack_id", True), ("source_ordinal", -1)])
def test_invalid_native_identities_are_rejected(field, value):
    definition, root, children = _case(1)
    invalid = replace(root, **{field: value})
    with pytest.raises(FamilyBlockError, match="invalid"):
        encode_family(
            definition, invalid, {"rates": children}, _family_hash(definition, root, children), limits=_limits()
        )
