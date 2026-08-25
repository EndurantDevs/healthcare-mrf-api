from __future__ import annotations

import struct
from types import SimpleNamespace

import pytest

from process.ptg_parts.ptg2_shared_blocks import (
    SharedMappingDigestSummary,
    SharedBlockReference,
    shared_block_hash,
    shared_mapping_digest,
    summarize_shared_snapshot_mappings,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _validate_authoritative_mapping_summary,
)
from process.ptg_parts.ptg2_v4_finalizer_map_digest import (
    new_v4_finalizer_kind_digest,
    update_v4_finalizer_kind_digest,
    v4_finalizer_map_root_digest,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_MAP_CONTRACT,
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_finalizer_mapping_summary import (
    summarize_native_finalizer_mapping_receipts,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    encode_v4_snapshot_map_pack,
)


_COPY_HEADER = b"PGCOPY\n\xff\r\n\0" + struct.pack(">II", 0, 0)
_PRICE_KINDS = ("price_atoms_v3", "price_set_atom_memberships_v3")


class _Rows:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class _Driver:
    def __init__(self, streams):
        self.streams = streams
        self.calls = []

    async def copy_from_query(self, query, *args, output, **copy_options):
        assert copy_options == {"format": "binary"}
        self.calls.append((query, args))
        stream = self.streams[args[1]]
        for offset in range(0, len(stream), 17):
            await output(stream[offset:offset + 17])


class _Session:
    def __init__(self, fixture):
        self.fixture = fixture
        self.driver = _Driver(fixture["streams"])
        self.calls = []

    async def connection(self):
        return SimpleNamespace(
            get_raw_connection=self._raw_connection,
        )

    async def _raw_connection(self):
        return SimpleNamespace(driver_connection=self.driver)

    async def execute(self, statement, params=None):
        sql = str(statement)
        parameters_by_name = dict(params or {})
        self.calls.append((sql, parameters_by_name))
        if "GROUP BY mapping.object_kind" in sql:
            return _Rows(self.fixture["aggregates"])
        if "ptg2_v4_finalizer_map_root AS root" in sql:
            return _Rows((self.fixture["root"],))
        if "ptg2_v4_finalizer_map_pack\" AS pack" in sql:
            if int(parameters_by_name["after_pack_no"]) >= 0:
                return _Rows()
            return _Rows(
                (
                    self.fixture["packs"][parameters_by_name["object_kind"]],
                )
            )
        if "block_hashes" in parameters_by_name:
            return _Rows(
                self.fixture["targets"][bytes(block_hash)]
                for block_hash in parameters_by_name["block_hashes"]
            )
        if "COUNT(*) AS target_count" in sql:
            target_count = len(self.fixture["targets"])
            return _Rows(
                ({"target_count": target_count, "valid_target_count": target_count},)
            )
        raise AssertionError(f"unexpected SQL: {sql}")


def _mapping_record(reference):
    kind = reference.object_kind.encode()
    return b"".join(
        (
            struct.pack(">I", len(kind)),
            kind,
            struct.pack(">qIQ", reference.block_key, reference.fragment_no, reference.entry_count),
            reference.block_hash,
        )
    )


def _copy_stream(reference):
    record = _mapping_record(reference)
    return _COPY_HEADER + struct.pack(">hi", 1, len(record)) + record + struct.pack(">h", -1)


def _packed_kind_fixture(index: int, object_kind: str):
    target_payload = f"target-{index}".encode()
    target_hash = shared_block_hash(
        format_version=2,
        object_kind=object_kind,
        codec="none",
        payload=target_payload,
    )
    reference = SharedBlockReference(
        object_kind=object_kind,
        block_key=index + 1,
        fragment_no=0,
        entry_count=index + 2,
        block_hash=target_hash,
        raw_byte_count=len(target_payload),
    )
    map_payload = encode_v4_snapshot_map_pack(object_kind, (reference,))
    map_hash = shared_block_hash(
        format_version=2,
        object_kind=PTG2_V4_MAP_BLOCK_KIND,
        codec="none",
        payload=map_payload,
    )
    pack_by_field = {
        "object_kind": object_kind, "pack_no": 0,
        "first_block_key": reference.block_key, "first_fragment_no": 0,
        "last_block_key": reference.block_key, "last_fragment_no": 0,
        "coordinate_count": 1, "entry_count": reference.entry_count,
        "logical_byte_count": reference.raw_byte_count, "map_block_hash": map_hash,
        "map_format_version": 2, "map_object_kind": PTG2_V4_MAP_BLOCK_KIND,
        "map_codec": "none", "map_entry_count": 1,
        "map_raw_byte_count": len(map_payload),
        "map_stored_byte_count": len(map_payload), "map_payload": map_payload,
    }
    target_by_field = {
        "block_hash": target_hash, "format_version": 2,
        "object_kind": object_kind, "codec": "none",
        "entry_count": reference.entry_count,
        "raw_byte_count": len(target_payload),
        "stored_byte_count": len(target_payload),
    }
    kind_digest = new_v4_finalizer_kind_digest(object_kind)
    update_v4_finalizer_kind_digest(
        kind_digest,
        SimpleNamespace(
            pack_no=0,
            first_coordinate=(reference.block_key, 0),
            last_coordinate=(reference.block_key, 0),
            coordinate_count=1,
            entry_count=reference.entry_count,
            logical_byte_count=reference.raw_byte_count,
            map_block=SimpleNamespace(block_hash=map_hash),
        ),
    )
    return reference, pack_by_field, target_by_field, kind_digest.digest()


def _packed_fixture():
    packed_references = []
    pack_by_kind = {}
    target_by_hash = {}
    kind_digest_by_kind = {}
    for index, object_kind in enumerate(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS):
        reference, pack_by_field, target_by_field, kind_digest = (
            _packed_kind_fixture(index, object_kind)
        )
        packed_references.append(reference)
        pack_by_kind[object_kind] = pack_by_field
        target_by_hash[reference.block_hash] = target_by_field
        kind_digest_by_kind[object_kind] = kind_digest
    return {
        "references": packed_references,
        "packs": pack_by_kind,
        "targets": target_by_hash,
        "kind_digests": kind_digest_by_kind,
    }


def _price_fixture():
    price_references = [
        SharedBlockReference(
            object_kind,
            100 + index,
            0,
            3 + index,
            bytes([65 + index]) * 32,
            20 + index,
        )
        for index, object_kind in enumerate(_PRICE_KINDS)
    ]
    return {
        "references": price_references,
        "aggregates": [
            {
                "object_kind": reference.object_kind,
                "mapping_count": 1,
                "unique_block_count": 1,
                "resolved_mapping_count": 1,
                "entry_count": reference.entry_count,
                "logical_byte_count": reference.raw_byte_count,
            }
            for reference in price_references
        ],
        "streams": {
            reference.object_kind: _copy_stream(reference)
            for reference in price_references
        },
    }


def _root_fixture(packed_fixture_by_field):
    packed_references = packed_fixture_by_field["references"]
    pack_by_kind = packed_fixture_by_field["packs"]
    target_by_hash = packed_fixture_by_field["targets"]
    return {
        "root_snapshot_key": 41,
        "root_state": "complete",
        "root_contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "root_map_format": PTG2_V4_MAP_FORMAT,
        "root_map_digest": v4_finalizer_map_root_digest(
            packed_fixture_by_field["kind_digests"],
            required_object_kinds=PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
        ),
        "root_canonical_mapping_digest": shared_mapping_digest(packed_references),
        "root_canonical_byte_count": sum(
            len(_mapping_record(reference)) for reference in packed_references
        ),
        "root_target_identity_digest": b"t" * 32,
        "object_kind_count": len(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS),
        "map_pack_count": len(pack_by_kind),
        "coordinate_count": len(packed_references),
        "entry_count": sum(reference.entry_count for reference in packed_references),
        "logical_byte_count": sum(
            reference.raw_byte_count for reference in packed_references
        ),
        "stored_map_byte_count": sum(
            pack_by_field["map_stored_byte_count"]
            for pack_by_field in pack_by_kind.values()
        ),
        "target_block_count": len(target_by_hash),
        "completed_at": object(),
        "layout_state": "building",
        "layout_generation": "shared_blocks_v4",
    }


def _fixture():
    packed_fixture_by_field = _packed_fixture()
    price_fixture_by_field = _price_fixture()
    return {
        "aggregates": price_fixture_by_field["aggregates"],
        "packs": packed_fixture_by_field["packs"],
        "targets": packed_fixture_by_field["targets"],
        "root": _root_fixture(packed_fixture_by_field),
        "streams": price_fixture_by_field["streams"],
        "packed_references": packed_fixture_by_field["references"],
        "price_references": price_fixture_by_field["references"],
    }


@pytest.mark.asyncio
async def test_hybrid_mapping_summary_matches_canonical_v3_digest_in_bounded_batches():
    fixture = _fixture()
    session = _Session(fixture)

    summary = await summarize_shared_snapshot_mappings(
        session,
        schema_name="mrf",
        snapshot_key=41,
    )

    all_references = fixture["packed_references"] + fixture["price_references"]
    assert summary.mapping_digest == shared_mapping_digest(all_references)
    assert summary.packed_mapping_digest == shared_mapping_digest(
        fixture["packed_references"]
    )
    assert summary.relational_mapping_digest == shared_mapping_digest(
        fixture["price_references"]
    )
    assert summary.mapping_count == len(all_references)
    assert summary.unique_block_count == len(all_references)
    assert summary.object_kinds == tuple(sorted(row.object_kind for row in all_references))
    pack_calls = [call for call in session.calls if "finalizer_map_pack\" AS pack" in call[0]]
    assert len(pack_calls) == len(PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
    assert all(call[1]["batch_rows"] == 256 for call in pack_calls)


@pytest.mark.asyncio
async def test_hybrid_mapping_summary_fails_closed_on_root_digest_or_target_identity():
    bad_digest_fixture = _fixture()
    bad_digest_fixture["root"]["root_map_digest"] = b"x" * 32
    with pytest.raises(RuntimeError, match="root digest changed"):
        await summarize_shared_snapshot_mappings(
            _Session(bad_digest_fixture), schema_name="mrf", snapshot_key=41
        )

    bad_target_fixture = _fixture()
    first_target = next(iter(bad_target_fixture["targets"].values()))
    first_target["entry_count"] += 1
    with pytest.raises(RuntimeError, match="target CAS identity differs"):
        await summarize_shared_snapshot_mappings(
            _Session(bad_target_fixture), schema_name="mrf", snapshot_key=41
        )


@pytest.mark.asyncio
async def test_native_receipt_skips_pack_decode_and_hashes_price_once():
    fixture = _fixture()
    session = _Session(fixture)
    root_by_name = dict(fixture["root"])
    aggregate_by_object_kind = {
        aggregate_row["object_kind"]: (
            aggregate_row["mapping_count"],
            aggregate_row["unique_block_count"],
            aggregate_row["entry_count"],
            aggregate_row["logical_byte_count"],
        )
        for aggregate_row in fixture["aggregates"]
    }

    summary = await summarize_native_finalizer_mapping_receipts(
        session,
        schema='"mrf"',
        snapshot_key=41,
        root_by_name=root_by_name,
        aggregate_by_object_kind=aggregate_by_object_kind,
        copy_from_query=session.driver.copy_from_query,
    )

    assert summary.packed_mapping_digest == shared_mapping_digest(
        fixture["packed_references"]
    )
    assert summary.relational_mapping_digest == shared_mapping_digest(
        fixture["price_references"]
    )
    assert summary.mapping_count == len(fixture["packed_references"]) + len(
        fixture["price_references"]
    )
    assert all("finalizer_map_pack" not in sql for sql, _params in session.calls)
    assert len(session.driver.calls) == len(_PRICE_KINDS)


def _authoritative_lanes():
    return (
        SimpleNamespace(
            object_kinds=("a_kind", "b_kind"),
            mapping_count=3,
            unique_block_count=2,
            logical_byte_count=30,
        ),
        SimpleNamespace(
            object_kinds=("c_kind",),
            mapping_count=2,
            unique_block_count=2,
            logical_byte_count=20,
        ),
    )


def test_authoritative_mapping_summary_matches_bounded_lane_metadata():
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=5,
        unique_block_count=4,
        entry_count=99,
        logical_byte_count=50,
        canonical_byte_count=400,
        object_kinds=("a_kind", "b_kind", "c_kind"),
    )

    _validate_authoritative_mapping_summary(summary, *_authoritative_lanes())


@pytest.mark.parametrize(
    ("summary_field", "summary_value"),
    (
        ("object_kinds", ("a_kind", "missing_kind")),
        ("mapping_count", 4),
        ("unique_block_count", 3),
        ("logical_byte_count", 49),
    ),
)
def test_authoritative_mapping_summary_rejects_lane_disagreement(
    summary_field,
    summary_value,
):
    summary_values_by_field = {
        "mapping_digest": b"m" * 32,
        "mapping_count": 5,
        "unique_block_count": 4,
        "entry_count": 99,
        "logical_byte_count": 50,
        "canonical_byte_count": 400,
        "object_kinds": ("a_kind", "b_kind", "c_kind"),
    }
    summary_values_by_field[summary_field] = summary_value
    with pytest.raises(RuntimeError, match=summary_field):
        _validate_authoritative_mapping_summary(
            SharedMappingDigestSummary(**summary_values_by_field),
            *_authoritative_lanes(),
        )


def test_authoritative_mapping_summary_rejects_overlapping_lane_kinds():
    summary = SharedMappingDigestSummary(
        mapping_digest=b"m" * 32,
        mapping_count=2,
        unique_block_count=2,
        entry_count=2,
        logical_byte_count=2,
        canonical_byte_count=100,
        object_kinds=("a_kind",),
    )
    lane = SimpleNamespace(
        object_kinds=("a_kind",),
        mapping_count=1,
        unique_block_count=1,
        logical_byte_count=1,
    )

    with pytest.raises(RuntimeError, match="overlap object kinds"):
        _validate_authoritative_mapping_summary(summary, lane, lane)
