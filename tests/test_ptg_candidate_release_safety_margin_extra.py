# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Extra fail-closed coverage around candidate-audit publication boundaries."""

from __future__ import annotations

import importlib
from collections.abc import Iterable
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_audit as shared_audit
from process.ptg_parts import ptg2_v4_audit as audit
from process.ptg_parts import ptg2_v4_stale_metadata_reconcile as stale_reconcile
from process.ptg_parts.ptg2_shared_audit import AuditCandidate, _ReadBudget


ptg_candidate_audit = importlib.import_module("process.ptg_candidate_audit")


class _RowsSession:
    def __init__(self, rows: Iterable[object] = ()) -> None:
        self.rows = tuple(rows)

    async def execute(self, _statement, _parameters=None):
        return self.rows


def _reader(rows: Iterable[object] = ()) -> audit._V4PersistedGraphReader:
    return audit._V4PersistedGraphReader(
        _RowsSession(rows),
        schema_name="mrf",
        snapshot_key=19,
        representation="direct_v1",
        budget=_ReadBudget(),
    )


def _manifest() -> audit._V4RelationManifest:
    return audit._V4RelationManifest(
        relation="set_groups_direct",
        member_object_kind="v4_set_groups_direct_members_v1",
        locator_object_kind="v4_set_groups_direct_locators_v1",
        owner_base=0,
        owner_count=2,
        logical_member_count=8,
        vector_member_count=8,
        member_width=4,
        member_page_bytes=16,
        locator_page_bytes=24,
        locator_owner_span=2,
    )


def _coordinate(
    *,
    block_key: int = 0,
    fragment_no: int = 0,
    entry_count: int = 1,
    block_hash: bytes = b"m" * 32,
) -> audit.V4SnapshotMapCoordinate:
    return audit.V4SnapshotMapCoordinate(
        "kind",
        block_key,
        fragment_no,
        entry_count,
        block_hash,
    )


def _map_row(
    *,
    pack_no: int = 1,
    coordinate_count: int = 1,
    entry_count: int = 1,
) -> dict[str, object]:
    return {
        "pack_no": pack_no,
        "first_block_key": 0,
        "first_fragment_no": 0,
        "last_block_key": 0,
        "last_fragment_no": 0,
        "coordinate_count": coordinate_count,
        "pack_entry_count": entry_count,
    }


def _patch_map_decoder(
    monkeypatch, coordinates: tuple[audit.V4SnapshotMapCoordinate, ...]
) -> None:
    monkeypatch.setattr(
        audit,
        "_validated_physical_payload",
        lambda *_args, **_kwargs: audit._V4PhysicalBlock(
            b"m" * 32,
            "kind",
            len(coordinates),
            b"payload",
        ),
    )
    monkeypatch.setattr(
        audit,
        "decode_v4_snapshot_map_pack",
        lambda *_args, **_kwargs: coordinates,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("rows", "coordinates", "message"),
    [
        (
            (_map_row(), _map_row()),
            (_coordinate(),),
            "duplicated a pack",
        ),
        (
            (_map_row(coordinate_count=2),),
            (_coordinate(),),
            "metadata is inconsistent",
        ),
        (
            (_map_row(coordinate_count=2, entry_count=2),),
            (_coordinate(), _coordinate()),
            "coordinate is ambiguous",
        ),
    ],
)
async def test_packed_map_query_rejects_duplicate_and_ambiguous_coordinates(
    monkeypatch,
    rows: tuple[dict[str, object], ...],
    coordinates: tuple[audit.V4SnapshotMapCoordinate, ...],
    message: str,
) -> None:
    """A map query cannot supply multiple authorities for one coordinate."""

    _patch_map_decoder(monkeypatch, coordinates)
    with pytest.raises(RuntimeError, match=message):
        await _reader(rows)._map_coordinates(
            object_kind="kind",
            coordinate_pairs=((0, 0),),
        )


@pytest.mark.asyncio
async def test_packed_map_decoder_and_cache_conflict_fail_closed(monkeypatch) -> None:
    """Invalid map bytes and an identity conflict both stop graph traversal."""

    reader = _reader((_map_row(),))
    monkeypatch.setattr(
        audit,
        "_validated_physical_payload",
        lambda *_args, **_kwargs: audit._V4PhysicalBlock(
            b"m" * 32, "kind", 1, b"payload"
        ),
    )
    monkeypatch.setattr(
        audit,
        "decode_v4_snapshot_map_pack",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad")),
    )
    with pytest.raises(RuntimeError, match="map pack is invalid"):
        await reader._map_coordinates(
            object_kind="kind", coordinate_pairs=((0, 0),)
        )

    conflicting_reader = _reader((_map_row(),))
    conflicting_reader._coordinate_cache[("kind", 0, 0)] = _coordinate(
        entry_count=2
    )
    _patch_map_decoder(monkeypatch, (_coordinate(entry_count=1),))
    with pytest.raises(RuntimeError, match="coordinate conflicts"):
        await conflicting_reader._map_coordinates(
            object_kind="kind", coordinate_pairs=((1, 0),)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("block", "message"),
    [
        (
            audit._V4PhysicalBlock(b"z" * 32, "kind", 1, b"payload"),
            "unexpected block",
        ),
        (
            audit._V4PhysicalBlock(b"m" * 32, "kind", 2, b"payload"),
            "entry count changed",
        ),
    ],
)
async def test_physical_cas_query_rejects_identity_or_count_drift(
    monkeypatch,
    block: audit._V4PhysicalBlock,
    message: str,
) -> None:
    reader = _reader(({"block_hash": b"m" * 32},))
    monkeypatch.setattr(
        audit,
        "_validated_physical_payload",
        lambda *_args, **_kwargs: block,
    )
    with pytest.raises(RuntimeError, match=message):
        await reader._physical_blocks(
            object_kind="kind",
            coordinates=(_coordinate(),),
            maximum_raw_bytes=16,
        )


def _heavy_owner() -> audit._V4HeavyOwner:
    return audit._V4HeavyOwner(
        relation="set_groups_direct",
        owner_key=0,
        object_kind="heavy-kind",
        member_count=1,
        member_base=10,
        member_span=8,
        fragment_count=1,
    )


async def _patch_heavy_storage(monkeypatch, reader, *, entry_count: int, payload: bytes):
    coordinate = audit.V4SnapshotMapCoordinate(
        "heavy-kind", 0, 0, entry_count, b"h" * 32
    )

    async def map_coordinates(**_kwargs):
        return {(0, 0): coordinate}

    async def physical_blocks(**_kwargs):
        return {
            coordinate.block_hash: audit._V4PhysicalBlock(
                coordinate.block_hash,
                "heavy-kind",
                entry_count,
                payload,
            )
        }

    monkeypatch.setattr(reader, "_map_coordinates", map_coordinates)
    monkeypatch.setattr(reader, "_physical_blocks", physical_blocks)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("entry_count", "logical_payload", "message"),
    [
        (0, b"x", "heavy member count changed"),
        (1, b"x", "heavy bitmap size changed"),
        (
            1,
            audit._HEAVY_HEADER.pack(b"BADMAGIC", 0, 10, 8, 1) + b"\x01",
            "heavy bitmap is inconsistent",
        ),
    ],
)
async def test_heavy_payload_integrity_rejects_count_size_and_header_drift(
    monkeypatch,
    entry_count: int,
    logical_payload: bytes,
    message: str,
) -> None:
    reader = _reader()
    await _patch_heavy_storage(
        monkeypatch, reader, entry_count=entry_count, payload=b"stored"
    )
    monkeypatch.setattr(
        audit,
        "_unframe_heavy_bitmap_fragment",
        lambda *_args, **_kwargs: logical_payload,
    )
    with pytest.raises(RuntimeError, match=message):
        await reader._heavy_payloads(_manifest(), {0: _heavy_owner()})


@pytest.mark.asyncio
async def test_scalar_and_binary_search_truncation_rejects_partial_graph(
    monkeypatch,
) -> None:
    manifest = _manifest()

    scalar_reader = _reader()

    async def scalar_manifest(_relation):
        return manifest

    async def no_heavy(_manifest, _keys):
        return {}

    async def one_locator(_manifest, _keys):
        return {0: (0, 1)}

    async def empty_page(_manifest, _keys):
        return {0: ()}

    async def no_heavy_payloads(_manifest, _owners):
        return {}

    monkeypatch.setattr(scalar_reader, "_manifest", scalar_manifest)
    monkeypatch.setattr(scalar_reader, "_heavy_owners", no_heavy)
    monkeypatch.setattr(scalar_reader, "_locators", one_locator)
    monkeypatch.setattr(scalar_reader, "_member_pages", empty_page)
    with pytest.raises(RuntimeError, match="scalar locator is truncated"):
        await scalar_reader.single_members(manifest.relation, (0,))

    search_reader = _reader()
    monkeypatch.setattr(search_reader, "_manifest", scalar_manifest)
    monkeypatch.setattr(search_reader, "_heavy_owners", no_heavy)
    monkeypatch.setattr(search_reader, "_heavy_payloads", no_heavy_payloads)
    monkeypatch.setattr(search_reader, "_locators", one_locator)
    monkeypatch.setattr(search_reader, "_member_pages", empty_page)
    with pytest.raises(RuntimeError, match="member locator is truncated"):
        await search_reader.contains_edges(manifest.relation, ((0, 4),))


@pytest.mark.asyncio
async def test_binary_search_round_cap_rejects_nonconvergent_graph(monkeypatch) -> None:
    manifest = _manifest()
    reader = _reader()

    async def manifest_loader(_relation):
        return manifest

    async def no_heavy(_manifest, _keys):
        return {}

    async def one_locator(_manifest, _keys):
        return {0: (0, 8)}

    async def no_heavy_payloads(_manifest, _owners):
        return {}

    monkeypatch.setattr(reader, "_manifest", manifest_loader)
    monkeypatch.setattr(reader, "_heavy_owners", no_heavy)
    monkeypatch.setattr(reader, "_heavy_payloads", no_heavy_payloads)
    monkeypatch.setattr(reader, "_locators", one_locator)
    monkeypatch.setattr(audit, "PTG2_V4_AUDIT_MAX_BINARY_SEARCH_ROUNDS", 0)
    with pytest.raises(RuntimeError, match="exceeded its round cap"):
        await reader.contains_edges(manifest.relation, ((0, 4),))


@pytest.mark.asyncio
async def test_scalar_heavy_bitmap_cardinality_drift_fails_closed(
    monkeypatch,
) -> None:
    """A single retained witness must survive both graph directions exactly."""

    manifest = _manifest()
    scalar_reader = _reader()
    owner = _heavy_owner()

    async def manifest_loader(_relation):
        return manifest

    async def heavy_owner_loader(_manifest, _keys):
        return {0: owner}

    async def no_regular_locators(_manifest, _keys):
        return {}

    async def no_regular_pages(_manifest, _keys):
        return {}

    async def cardinality_drift(_manifest, _owners):
        return {
            0: audit._HEAVY_HEADER.pack(
                audit.PTG2_V4_HEAVY_BITMAP_MAGIC, 0, 10, 8, 1
            )
            + b"\x03"
        }

    monkeypatch.setattr(scalar_reader, "_manifest", manifest_loader)
    monkeypatch.setattr(scalar_reader, "_heavy_owners", heavy_owner_loader)
    monkeypatch.setattr(scalar_reader, "_locators", no_regular_locators)
    monkeypatch.setattr(scalar_reader, "_member_pages", no_regular_pages)
    monkeypatch.setattr(scalar_reader, "_heavy_payloads", cardinality_drift)
    with pytest.raises(RuntimeError, match="scalar bitmap changed cardinality"):
        await scalar_reader.single_members(manifest.relation, (0,))



@pytest.mark.asyncio
async def test_pattern_witness_membership_fails_closed() -> None:
    candidate = AuditCandidate(1, 7, 2, 3, 1, candidate_ordinal=4)
    witness = audit.V4ProviderSetAuditWitness(7, 9, 1_111_111_111)

    class _PatternReader:
        representation = "pattern_v1"

        async def single_members(self, _relation, _keys):
            return {9: 12}

        async def contains_edges(self, _relation, _edges):
            return {(1, 2): False}

    with pytest.raises(RuntimeError, match="absent from the pattern graph"):
        await audit._verified_provider_npis_by_candidate(
            object(),
            schema_name="mrf",
            snapshot_key=19,
            candidates=(candidate,),
            witnesses={7: witness},
            reader=_PatternReader(),
        )



@pytest.mark.asyncio
async def test_direct_witness_group_membership_fails_closed(monkeypatch) -> None:
    candidate = AuditCandidate(1, 7, 2, 3, 1, candidate_ordinal=4)
    witness = audit.V4ProviderSetAuditWitness(7, 9, 1_111_111_111)

    class _DirectReader:
        representation = "direct_v1"

        async def contains_edges(self, relation, _edges):
            return {(7, 9): True} if relation == "set_groups_direct" else {(9, 3): False}

    async def npi_dictionary(*_args, **_kwargs):
        return {1_111_111_111: 3}

    monkeypatch.setattr(audit, "_npi_keys_for_values", npi_dictionary)
    with pytest.raises(RuntimeError, match="absent from its exact group"):
        await audit._verified_provider_npis_by_candidate(
            object(),
            schema_name="mrf",
            snapshot_key=19,
            candidates=(candidate,),
            witnesses={7: witness},
            reader=_DirectReader(),
        )


@pytest.mark.asyncio
async def test_shared_manifest_generation_and_stale_review_digest_fail_closed() -> None:
    with pytest.raises(ValueError, match="unsupported shared PTG audit generation"):
        await shared_audit._sealed_layout_manifest(
            object(), schema_name="mrf", snapshot_key=1, expected_generation="other"
        )

    with pytest.raises(ValueError, match="SHA-256 hex digest"):
        stale_reconcile._normalized_plan_digest("not-a-digest")


@pytest.mark.asyncio
async def test_audit_only_redelivery_rejects_an_active_candidate() -> None:
    target = SimpleNamespace(
        activated=True,
        equivalent_current_snapshot_id=None,
    )

    with pytest.raises(ValueError, match="already active or equivalent"):
        await ptg_candidate_audit._existing_candidate_audit_result(
            target,
            candidate_audit_mode="audit_only",
            run_id=None,
        )


def test_stale_marker_refuses_digest_drift_for_completed_work() -> None:
    context = object()
    request = type("Request", (), {"target_digest": "target", "expected_plan_digest": "a" * 64})()
    reconciliation_plan_by_field = {"status": "already_reconciled"}
    original = stale_reconcile.exact_stale_marker
    try:
        stale_reconcile.exact_stale_marker = lambda *_args, **_kwargs: {
            "plan_digest": "b" * 64
        }
        with pytest.raises(
            stale_reconcile.PTG2V4StaleMetadataConflict,
            match="does not match the completed",
        ):
            stale_reconcile._reviewed_marker(
                context,
                request,
                reconciliation_plan_by_field,
            )
    finally:
        stale_reconcile.exact_stale_marker = original
