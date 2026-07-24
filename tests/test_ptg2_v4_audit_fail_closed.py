# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts.ptg2_shared_audit import _ReadBudget
from process.ptg_parts.ptg2_shared_blocks import SharedLayoutBuildOwnership


class _RowsSession:
    def __init__(self, rows=()) -> None:
        self.rows = tuple(rows)

    async def execute(self, _statement, _parameters=None):
        return self.rows


def _witness_compilation(
    tmp_path: Path,
    payload: bytes,
    *,
    row_count: int = 0,
) -> SimpleNamespace:
    path = tmp_path / "provider-set-audit-npi.copy"
    path.write_bytes(payload)
    artifact = SimpleNamespace(
        name="provider_set_audit_npi",
        path=path,
        byte_count=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        row_count=row_count,
    )
    return SimpleNamespace(
        output_artifacts=(artifact,),
        provider_set_audit_npi_copy_path=path,
    )


def _replace_witness_payload(compilation: SimpleNamespace, payload: bytes) -> None:
    artifact = compilation.output_artifacts[0]
    artifact.path.write_bytes(payload)
    artifact.byte_count = len(payload)
    artifact.sha256 = hashlib.sha256(payload).hexdigest()


def _empty_copy_payload() -> bytes:
    return audit._PG_COPY_HEADER + struct.pack(">h", audit._PG_COPY_TRAILER)


def _manifest() -> audit._V4RelationManifest:
    return audit._V4RelationManifest(
        relation="set_groups_direct",
        member_object_kind="v4_set_groups_direct_members_v1",
        locator_object_kind="v4_set_groups_direct_locators_v1",
        owner_base=0,
        owner_count=2,
        logical_member_count=4,
        vector_member_count=4,
        member_width=4,
        member_page_bytes=16,
        locator_page_bytes=audit._LOCATOR.size * 2,
        locator_owner_span=2,
    )


def _reader(session=None) -> audit._V4PersistedGraphReader:
    return audit._V4PersistedGraphReader(
        session or _RowsSession(),
        schema_name="mrf",
        snapshot_key=17,
        representation="direct_v1",
        budget=_ReadBudget(),
    )


def test_heavy_fragment_and_relation_helpers_reject_invalid_values() -> None:
    owner = audit._V4HeavyOwner(
        relation="set_groups_direct",
        owner_key=7,
        object_kind="v4_set_groups_direct_heavy_bitmap_v1",
        member_count=1,
        member_base=10,
        member_span=8,
        fragment_count=1,
    )
    with pytest.raises(RuntimeError, match="fragment is truncated"):
        audit._unframe_heavy_bitmap_fragment(
            b"x",
            owner=owner,
            fragment_no=0,
            entry_count=1,
            logical_offset=audit.PTG2_V4_HEAVY_BITMAP_HEADER_BYTES,
        )

    framed = audit._HEAVY_FRAGMENT_HEADER.pack(
        audit.PTG2_V4_HEAVY_BITMAP_FRAGMENT_MAGIC,
        owner.owner_key,
        owner.member_base,
        owner.member_span,
        owner.member_count,
        0,
        1,
    ) + b"\0"
    with pytest.raises(RuntimeError, match="fragment count changed"):
        audit._unframe_heavy_bitmap_fragment(
            framed,
            owner=owner,
            fragment_no=0,
            entry_count=1,
            logical_offset=audit.PTG2_V4_HEAVY_BITMAP_HEADER_BYTES,
        )

    assert audit._row_mapping(None) == {}
    with pytest.raises(RuntimeError, match="relation is unsupported"):
        audit._relation_kinds("not-a-relation")


def test_witness_artifact_contract_rejects_bad_metadata(tmp_path: Path) -> None:
    payload = _empty_copy_payload()
    missing = SimpleNamespace(
        output_artifacts=(),
        provider_set_audit_npi_copy_path=tmp_path / "missing.copy",
    )
    with pytest.raises(RuntimeError, match="missing or duplicated"):
        audit.load_v4_audit_witnesses(missing, provider_set_keys=())

    wrong_path = _witness_compilation(tmp_path, payload)
    wrong_path.provider_set_audit_npi_copy_path = tmp_path / "other.copy"
    with pytest.raises(RuntimeError, match="path changed"):
        audit.load_v4_audit_witnesses(wrong_path, provider_set_keys=())

    bad_size = _witness_compilation(tmp_path, payload)
    bad_size.output_artifacts[0].byte_count += 1
    with pytest.raises(RuntimeError, match="authentication failed"):
        audit.load_v4_audit_witnesses(bad_size, provider_set_keys=())

    valid = _witness_compilation(tmp_path, payload)
    with pytest.raises(RuntimeError, match="outside int4 range"):
        audit.load_v4_audit_witnesses(valid, provider_set_keys=(-1,))


@pytest.mark.parametrize(
    ("payload", "row_count", "message"),
    [
        (
            b"X" + _empty_copy_payload()[1:],
            0,
            "invalid COPY header",
        ),
        (
            _empty_copy_payload() + b"x",
            0,
            "trailing COPY bytes",
        ),
        (
            audit._PG_COPY_HEADER + struct.pack(">h", 2),
            0,
            "invalid COPY row width",
        ),
        (
            audit._PG_COPY_HEADER + struct.pack(">h", 3) + b"\0\0\0",
            0,
            "truncates COPY field width",
        ),
        (
            audit._PG_COPY_HEADER + struct.pack(">h", 3) + struct.pack(">i", 3),
            0,
            "invalid COPY field width",
        ),
        (
            _empty_copy_payload(),
            1,
            "row count changed",
        ),
    ],
)
def test_witness_copy_parser_rejects_structural_corruption(
    tmp_path: Path,
    payload: bytes,
    row_count: int,
    message: str,
) -> None:
    compilation = _witness_compilation(
        tmp_path,
        payload,
        row_count=row_count,
    )
    with pytest.raises(RuntimeError, match=message):
        audit.load_v4_audit_witnesses(compilation, provider_set_keys=())


@pytest.mark.asyncio
async def test_reader_cache_and_coordinate_guards_fail_closed() -> None:
    reader = _reader()
    block = audit._V4PhysicalBlock(b"a" * 32, "kind", 1, b"x")
    reader._charge_block(block)
    reader._charge_block(block)

    with pytest.raises(RuntimeError, match="coordinate is negative"):
        await reader._map_coordinates(
            object_kind="kind",
            coordinate_pairs=((-1, 0),),
        )

    coordinate = audit.V4SnapshotMapCoordinate("kind", 1, 0, 1, b"a" * 32)
    reader._coordinate_cache[("kind", 1, 0)] = coordinate
    assert await reader._map_coordinates(
        object_kind="kind",
        coordinate_pairs=((1, 0),),
    ) == {(1, 0): coordinate}

    with pytest.raises(RuntimeError, match="missing a required coordinate"):
        await _reader()._map_coordinates(
            object_kind="kind",
            coordinate_pairs=((1, 0),),
        )


@pytest.mark.asyncio
async def test_reader_physical_cache_guards_fail_closed() -> None:
    shared_hash = b"a" * 32
    first = audit.V4SnapshotMapCoordinate("kind", 0, 0, 1, shared_hash)
    second = audit.V4SnapshotMapCoordinate("kind", 1, 0, 2, shared_hash)
    with pytest.raises(RuntimeError, match="aliased CAS count"):
        await _reader()._physical_blocks(
            object_kind="kind",
            coordinates=(first, second),
            maximum_raw_bytes=16,
        )

    stale_reader = _reader()
    stale_reader._physical_cache[shared_hash] = audit._V4PhysicalBlock(
        shared_hash,
        "other-kind",
        1,
        b"x",
    )
    with pytest.raises(RuntimeError, match="cached CAS identity"):
        await stale_reader._physical_blocks(
            object_kind="kind",
            coordinates=(first,),
            maximum_raw_bytes=16,
        )

    cached_reader = _reader()
    cached = audit._V4PhysicalBlock(shared_hash, "kind", 1, b"x")
    cached_reader._physical_cache[shared_hash] = cached
    assert await cached_reader._physical_blocks(
        object_kind="kind",
        coordinates=(first,),
        maximum_raw_bytes=16,
    ) == {shared_hash: cached}

    with pytest.raises(RuntimeError, match="missing CAS block"):
        await _reader()._physical_blocks(
            object_kind="kind",
            coordinates=(first,),
            maximum_raw_bytes=16,
        )


def _locator_reader(monkeypatch, manifest, locator_payload):
    coordinate = audit.V4SnapshotMapCoordinate(
        manifest.locator_object_kind,
        0,
        0,
        1,
        b"l" * 32,
    )

    async def locator_coordinates(**_kwargs):
        return {(0, 0): coordinate}

    async def locator_blocks(**_kwargs):
        return {
            coordinate.block_hash: audit._V4PhysicalBlock(
                coordinate.block_hash,
                manifest.locator_object_kind,
                1,
                locator_payload,
            )
        }

    reader = _reader()
    monkeypatch.setattr(
        reader,
        "_map_coordinates",
        locator_coordinates,
    )
    monkeypatch.setattr(
        reader,
        "_physical_blocks",
        locator_blocks,
    )
    return reader


@pytest.mark.asyncio
async def test_reader_locator_guards_fail_closed(monkeypatch) -> None:
    """Reject invalid owners and malformed or overflowing locator pages."""
    manifest = _manifest()
    with pytest.raises(RuntimeError, match="owner is outside"):
        await _reader()._locators(manifest, (2,))

    malformed_reader = _locator_reader(monkeypatch, manifest, b"")
    with pytest.raises(RuntimeError, match="locator page is malformed"):
        await malformed_reader._locators(manifest, (0,))

    exceeding_reader = _locator_reader(
        monkeypatch,
        manifest,
        audit._LOCATOR.pack(3, 2),
    )
    with pytest.raises(RuntimeError, match="locator exceeds"):
        await exceeding_reader._locators(manifest, (0,))


def _member_page_reader(monkeypatch, manifest):
    member_coordinate = audit.V4SnapshotMapCoordinate(
        manifest.member_object_kind,
        0,
        0,
        2,
        b"m" * 32,
    )

    async def member_coordinates(**kwargs):
        page_key, fragment_no = tuple(kwargs["coordinate_pairs"])[0]
        return {
            (page_key, fragment_no): audit.V4SnapshotMapCoordinate(
                manifest.member_object_kind,
                page_key,
                fragment_no,
                2,
                member_coordinate.block_hash,
            )
        }

    async def malformed_member_blocks(**_kwargs):
        return {
            member_coordinate.block_hash: audit._V4PhysicalBlock(
                member_coordinate.block_hash,
                manifest.member_object_kind,
                2,
                struct.pack("<I", 1),
            )
        }

    reader = _reader()
    monkeypatch.setattr(
        reader,
        "_map_coordinates",
        member_coordinates,
    )
    monkeypatch.setattr(
        reader,
        "_physical_blocks",
        malformed_member_blocks,
    )
    return reader


@pytest.mark.asyncio
async def test_reader_member_page_guards_fail_closed(monkeypatch) -> None:
    """Reject unaligned coordinates and malformed member pages."""
    manifest = _manifest()
    unaligned_reader = _member_page_reader(monkeypatch, manifest)
    with pytest.raises(RuntimeError, match="page key is unaligned"):
        await unaligned_reader._member_pages(manifest, (1,))

    malformed_member_reader = _member_page_reader(monkeypatch, manifest)
    with pytest.raises(RuntimeError, match="member page is malformed"):
        await malformed_member_reader._member_pages(manifest, (0,))


def _heavy_owner(
    manifest,
    owner_key,
    object_kind,
    *,
    member_span=8,
):
    return audit._V4HeavyOwner(
        manifest.relation,
        owner_key,
        object_kind,
        1,
        0,
        member_span,
        1,
    )


@pytest.mark.asyncio
async def test_heavy_owner_payload_guards_fail_closed(monkeypatch) -> None:
    """Reject oversized bitmaps and mixed physical object kinds."""
    manifest = _manifest()
    huge_owner = _heavy_owner(
        manifest,
        0,
        "heavy-a",
        member_span=0xFFFFFFFF,
    )
    with pytest.raises(RuntimeError, match="exceeds the byte cap"):
        await _reader()._heavy_payloads(manifest, {0: huge_owner})

    mixed_owner_by_key = {
        0: _heavy_owner(manifest, 0, "heavy-a"),
        1: _heavy_owner(manifest, 1, "heavy-b"),
    }
    mixed_reader = _reader()

    async def no_coordinates(**_kwargs):
        return {}

    monkeypatch.setattr(mixed_reader, "_map_coordinates", no_coordinates)
    with pytest.raises(RuntimeError, match="mix object kinds"):
        await mixed_reader._heavy_payloads(manifest, mixed_owner_by_key)


@pytest.mark.asyncio
async def test_scalar_cardinality_guard_fails_closed(monkeypatch) -> None:
    """Reject a scalar relation whose locator publishes multiple members."""
    manifest = _manifest()
    scalar_reader = _reader()

    async def scalar_manifest(_relation):
        return manifest

    async def no_heavy(_manifest, _owner_keys):
        return {}

    async def two_members(_manifest, _owner_keys):
        return {0: (0, 2)}

    monkeypatch.setattr(scalar_reader, "_manifest", scalar_manifest)
    monkeypatch.setattr(scalar_reader, "_heavy_owners", no_heavy)
    monkeypatch.setattr(scalar_reader, "_locators", two_members)
    with pytest.raises(RuntimeError, match="changed cardinality"):
        await scalar_reader.single_members(manifest.relation, (0,))


@pytest.mark.asyncio
async def test_build_root_and_npi_dictionary_guards_fail_closed() -> None:
    with pytest.raises(RuntimeError, match="root is missing or duplicated"):
        await audit._validate_building_v4_context(
            _RowsSession(),
            schema_name="mrf",
            build_ownership=SharedLayoutBuildOwnership(17, "token"),
            representation="pattern_v1",
        )

    assert await audit._npi_keys_for_values(
        _RowsSession(),
        schema_name="mrf",
        snapshot_key=17,
        npis=(),
    ) == {}

    duplicate_key_rows = (
        {"npi": 1_111_111_111, "npi_key": 1},
        {"npi": 1_222_222_222, "npi_key": 1},
    )
    with pytest.raises(RuntimeError, match="dictionary is ambiguous"):
        await audit._npi_keys_for_values(
            _RowsSession(duplicate_key_rows),
            schema_name="mrf",
            snapshot_key=17,
            npis=(1_111_111_111, 1_222_222_222),
        )

    with pytest.raises(RuntimeError, match="absent from its dense dictionary"):
        await audit._npi_keys_for_values(
            _RowsSession(),
            schema_name="mrf",
            snapshot_key=17,
            npis=(1_111_111_111,),
        )
