from __future__ import annotations

from collections import OrderedDict
import struct

import pytest

from api import ptg2_v4_graph as graph
from api import ptg2_v4_intersection as intersection
from api import ptg2_serving
from api.ptg2_shared_blocks import PTG2SharedBlockError
from api.ptg2_types import PTG2ServingTables


def test_v4_locator_and_member_pages_decode_fixed_width_contract() -> None:
    locator_payload = struct.pack("<QI", 4094, 5) + struct.pack("<QI", 4099, 0)
    assert graph._decode_locator(
        locator_payload,
        entry_count=2,
        local_owner_index=0,
    ) == (4094, 5)
    assert graph._decode_member_page(
        struct.pack("<III", 2, 7, 11),
        entry_count=3,
    ) == (2, 7, 11)


def test_v4_member_page_uses_native_decoder_after_framing_validation(
    monkeypatch,
) -> None:
    calls: list[bytes] = []

    def fake_native(payload: bytes) -> list[int]:
        calls.append(payload)
        return [2, 7, 11]

    payload = struct.pack("<III", 2, 7, 11)
    monkeypatch.setattr(graph, "_native_u32_decoder", fake_native)
    assert graph._decode_member_page(payload, entry_count=3) == (2, 7, 11)
    assert calls == [payload]


def test_v4_member_page_python_fallback_is_exact(monkeypatch) -> None:
    monkeypatch.setattr(graph, "_native_u32_decoder", None)
    assert graph._decode_member_page(
        struct.pack("<IIII", 0, 2, 0xFFFFFFFE, 0xFFFFFFFF),
        entry_count=4,
    ) == (0, 2, 0xFFFFFFFE, 0xFFFFFFFF)


@pytest.mark.parametrize(
    "native_result, error_match",
    [
        ([2], "changed framing"),
        ([2, 0x1_0000_0000], "changed framing"),
        ([2, "not-an-integer"], "decoder failed"),
    ],
)
def test_v4_member_page_rejects_malformed_native_results(
    monkeypatch,
    native_result,
    error_match: str,
) -> None:
    monkeypatch.setattr(graph, "_native_u32_decoder", lambda _payload: native_result)
    with pytest.raises(PTG2SharedBlockError, match=error_match):
        graph._decode_member_page(struct.pack("<II", 2, 7), entry_count=2)


def test_v4_page_decoders_reject_truncation_and_out_of_range_owner() -> None:
    with pytest.raises(PTG2SharedBlockError, match="locator page"):
        graph._decode_locator(b"\0" * 11, entry_count=1, local_owner_index=0)
    with pytest.raises(PTG2SharedBlockError, match="locator page"):
        graph._decode_locator(b"\0" * 12, entry_count=1, local_owner_index=1)
    with pytest.raises(PTG2SharedBlockError, match="member page"):
        graph._decode_member_page(b"\0" * 5, entry_count=1)


def test_v4_heavy_bitmap_decoder_is_exact_and_rejects_padding_bits() -> None:
    owner = graph.V4HeavyOwner(
        relation="npi_groups_exact",
        owner_key=7,
        object_kind="v4_npi_groups_exact_heavy_bitmap_v1",
        member_count=3,
        member_base=100,
        member_span=10,
        fragment_count=1,
    )
    header = b"PTG2V4BM" + struct.pack("<IIII", 7, 100, 10, 3)
    assert graph._decode_heavy_bitmap(
        header + bytes((0b00000101, 0b00000010)),
        heavy_owner=owner,
    ) == (100, 102, 109)
    with pytest.raises(PTG2SharedBlockError, match="padding"):
        graph._decode_heavy_bitmap(
            header + bytes((0b00000101, 0b10000010)),
            heavy_owner=owner,
        )


@pytest.mark.asyncio
async def test_v4_heavy_bitmap_fragments_use_physical_entry_counts(
    monkeypatch,
) -> None:
    """Validate physical fragment counts before decoding a heavy bitmap."""

    owner = graph.V4HeavyOwner(
        relation="npi_groups_exact",
        owner_key=7,
        object_kind="v4_npi_groups_exact_heavy_bitmap_v1",
        member_count=6,
        member_base=100,
        member_span=24,
        fragment_count=2,
    )
    header = b"PTG2V4BM" + struct.pack("<IIII", 7, 100, 24, 6)

    def frame(fragment_no, entry_count, payload):
        return struct.pack(
            "<8sIIIIII",
            b"PTG2V4BF",
            7,
            100,
            24,
            6,
            fragment_no,
            entry_count,
        ) + payload

    fragment_payloads = (
        frame(0, 3, header + bytes((0b00000111,))),
        frame(1, 3, bytes((0b00000011, 0b00000001))),
    )

    class Coordinate:
        def __init__(self, fragment_no: int, entry_count: int) -> None:
            self.block_key = 7
            self.fragment_no = fragment_no
            self.entry_count = entry_count
            self.block_hash = bytes((fragment_no + 1,)) * 32

    coordinates_by_key = {
        (7, 0): Coordinate(0, 3),
        (7, 1): Coordinate(1, 3),
    }

    async def fake_coordinates(*_args, **_kwargs):
        return coordinates_by_key

    async def fake_blocks(*_args, **_kwargs):
        return {
            coordinate.block_hash: graph._CachedPhysicalBlock(
                coordinate.block_hash,
                owner.object_kind,
                coordinate.entry_count,
                fragment_payloads[coordinate.fragment_no],
            )
            for coordinate in coordinates_by_key.values()
        }

    monkeypatch.setattr(graph, "_load_map_coordinate_pairs", fake_coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", fake_blocks)
    manifest = graph.V4RelationManifest(
        snapshot_key=17,
        relation="npi_groups_exact",
        member_object_kind="v4_npi_groups_exact_members_v1",
        locator_object_kind="v4_npi_groups_exact_locators_v1",
        owner_base=0,
        owner_count=8,
        logical_member_count=6,
        vector_member_count=0,
        member_width=4,
        member_page_bytes=32,
        locator_page_bytes=32,
        locator_owner_span=2,
    )

    with graph.v4_graph_request_scope():
        assert await graph._lookup_v4_heavy_members(
            object(),
            snapshot_key=17,
            schema_name="mrf",
            relation_manifest=manifest,
            heavy_owners={7: owner},
        ) == {7: (100, 101, 102, 108, 109, 116)}

    coordinates_by_key[(7, 0)].entry_count = 2
    with pytest.raises(PTG2SharedBlockError, match="fragment conflicts"):
        with graph.v4_graph_request_scope():
            await graph._lookup_v4_heavy_members(
                object(),
                snapshot_key=17,
                schema_name="mrf",
                relation_manifest=manifest,
                heavy_owners={7: owner},
            )


def _heavy_owner_row(
    owner_key: int,
    *,
    snapshot_key: int = 17,
    relation: str = "npi_groups_exact",
    member_base: int = 100,
) -> dict[str, object]:
    return {
        "snapshot_key": snapshot_key,
        "relation": relation,
        "owner_key": owner_key,
        "object_kind": f"v4_{relation}_heavy_bitmap_v1",
        "member_count": 3,
        "member_base": member_base,
        "member_span": 8,
        "fragment_count": 1,
    }


@pytest.mark.asyncio
async def test_heavy_owner_loader_caches_scoped_hits_and_misses(
    monkeypatch,
) -> None:
    class Session:
        def __init__(self) -> None:
            self.calls = []
            self.responses = [(_heavy_owner_row(7),), ()]

        async def execute(self, statement, parameters):
            self.calls.append((str(statement), dict(parameters)))
            return self.responses.pop(0)

    session = Session()
    monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE", OrderedDict())

    loaded = await graph.load_v4_heavy_owners(
        session,
        snapshot_key=17,
        relation="npi_groups_exact",
        owner_keys=(8, 7, 7),
        schema_name="mrf",
    )
    assert tuple(loaded) == (7,)
    sql, parameters = session.calls[0]
    assert "owner_key = ANY(CAST(:owner_keys AS bigint[]))" in sql
    assert parameters["owner_keys"] == [7, 8]

    assert await graph.load_v4_heavy_owners(
        session,
        snapshot_key=17,
        relation="npi_groups_exact",
        owner_keys=(7, 8),
        schema_name="mrf",
    ) == loaded
    assert len(session.calls) == 1

    assert await graph.load_v4_heavy_owners(
        session,
        snapshot_key=17,
        relation="npi_groups_exact",
        owner_keys=(7, 8, 9),
        schema_name="mrf",
    ) == loaded
    assert session.calls[1][1]["owner_keys"] == [9]
    assert await graph.load_v4_heavy_owners(
        session,
        snapshot_key=17,
        relation="npi_groups_exact",
        owner_keys=(9,),
        schema_name="mrf",
    ) == {}
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_heavy_owner_lrus_are_bounded_and_isolated(
    monkeypatch,
) -> None:
    """Bound positive and negative caches while isolating graph namespaces."""

    class Session:
        def __init__(self) -> None:
            self.calls = []

        async def execute(self, statement, parameters):
            sql = str(statement)
            parameter_map = dict(parameters)
            self.calls.append((sql, parameter_map))
            is_other_schema = '"other"' in sql
            owner_rows = []
            for owner_key in parameter_map["owner_keys"]:
                if owner_key < 10 or is_other_schema:
                    owner_rows.append(
                        _heavy_owner_row(
                            owner_key,
                            snapshot_key=parameter_map["snapshot_key"],
                            relation=parameter_map["relation"],
                            member_base=(200 if is_other_schema else 100),
                        )
                    )
            return tuple(owner_rows)

    session = Session()
    monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE_MAX_ENTRIES", 2)
    monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE_MAX_ENTRIES", 2)

    async def load(
        owner_key: int,
        *,
        schema_name: str = "mrf",
        snapshot_key: int = 17,
        relation: str = "npi_groups_exact",
    ):
        return await graph.load_v4_heavy_owners(
            session,
            snapshot_key=snapshot_key,
            relation=relation,
            owner_keys=(owner_key,),
            schema_name=schema_name,
        )

    await load(1)
    await load(2)
    await load(1)
    await load(3)
    calls_before_evicted_positive = len(session.calls)
    await load(2)
    assert len(session.calls) == calls_before_evicted_positive + 1
    assert len(graph._HEAVY_OWNER_CACHE) == 2

    await load(10)
    await load(11)
    await load(10)
    await load(12)
    calls_before_evicted_negative = len(session.calls)
    await load(11)
    assert len(session.calls) == calls_before_evicted_negative + 1
    assert len(graph._HEAVY_OWNER_NEGATIVE_CACHE) == 2

    assert await load(20) == {}
    other = await load(20, schema_name="other")
    assert other[20].member_base == 200
    calls_before_namespace_checks = len(session.calls)
    assert (await load(3, snapshot_key=18))[3].owner_key == 3
    assert (await load(3, relation="group_npis_exact"))[3].relation == (
        "group_npis_exact"
    )
    assert len(session.calls) == calls_before_namespace_checks + 2
    assert len(graph._HEAVY_OWNER_CACHE) <= 2
    assert len(graph._HEAVY_OWNER_NEGATIVE_CACHE) <= 2


@pytest.mark.asyncio
async def test_v4_heavy_owner_loader_rejects_invalid_requests_and_hostile_rows(
    monkeypatch,
) -> None:
    class Session:
        def __init__(self, response_rows=()) -> None:
            self.response_rows = tuple(response_rows)
            self.calls = 0

        async def execute(self, _statement, _parameters):
            self.calls += 1
            return self.response_rows

    for requested in ((-1,), (0x1_0000_0000,), (True,), ("7",)):
        session = Session()
        with pytest.raises(PTG2SharedBlockError, match="owner key"):
            await graph.load_v4_heavy_owners(
                session,
                snapshot_key=17,
                relation="npi_groups_exact",
                owner_keys=requested,
                schema_name="mrf",
            )
        assert session.calls == 0

    base = _heavy_owner_row(7)
    hostile_results = (
        (_heavy_owner_row(8),),
        (base, dict(base)),
        ({**base, "snapshot_key": 18},),
        ({**base, "object_kind": "v4_wrong_heavy_bitmap_v1"},),
        ({**base, "owner_key": 0x1_0000_0000},),
        ({**base, "member_base": 0xFFFFFFFF, "member_span": 2},),
    )
    for hostile_rows in hostile_results:
        monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE", OrderedDict())
        monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE", OrderedDict())
        session = Session(hostile_rows)
        with pytest.raises(PTG2SharedBlockError):
            await graph.load_v4_heavy_owners(
                session,
                snapshot_key=17,
                relation="npi_groups_exact",
                owner_keys=(7,),
                schema_name="mrf",
            )
        assert not graph._HEAVY_OWNER_CACHE
        assert not graph._HEAVY_OWNER_NEGATIVE_CACHE


@pytest.mark.asyncio
async def test_v4_relation_lookup_uses_manifest_owner_base_and_page_geometry(
    monkeypatch,
) -> None:
    """Use manifest owner offsets and page geometry for packed lookup."""

    class Coordinate:
        def __init__(self, key: int, block_hash: bytes, entries: int) -> None:
            self.block_key = key
            self.fragment_no = 0
            self.block_hash = block_hash
            self.entry_count = entries

    requested_pages: list[tuple[str, tuple[int, ...]]] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "direct_v1", b"r" * 32)

    async def fake_manifest(*_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="set_groups_direct",
            member_object_kind="v4_set_groups_direct_members_v1",
            locator_object_kind="v4_set_groups_direct_locators_v1",
            owner_base=1,
            owner_count=3,
            logical_member_count=3,
            vector_member_count=3,
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=24,
            locator_owner_span=2,
        )

    async def fake_heavy(*_args, **kwargs):
        assert kwargs["owner_keys"] == (2,)
        return {}

    async def fake_coordinates(*_args, **kwargs):
        keys = tuple(sorted(kwargs["block_keys"]))
        kind = kwargs["object_kind"]
        requested_pages.append((kind, keys))
        if kind.endswith("locators_v1"):
            return {1: Coordinate(1, b"l" * 32, 2)}
        return {0: Coordinate(0, b"m" * 32, 3)}

    async def fake_blocks(*_args, **kwargs):
        kind = kwargs["object_kind"]
        if kind.endswith("locators_v1"):
            payload = struct.pack("<QI", 0, 1) + struct.pack("<QI", 1, 2)
            return {
                b"l" * 32: graph._CachedPhysicalBlock(
                    b"l" * 32, kind, 2, payload
                )
            }
        return {
            b"m" * 32: graph._CachedPhysicalBlock(
                b"m" * 32, kind, 3, struct.pack("<III", 5, 7, 9)
            )
        }

    monkeypatch.setattr(graph, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", fake_heavy)
    monkeypatch.setattr(graph, "_load_map_coordinates", fake_coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", fake_blocks)

    assert await graph.lookup_v4_relation_members(
        object(),
        snapshot_key=17,
        relation="set_groups_direct",
        owner_keys=(2,),
        schema_name="mrf",
    ) == {2: (7, 9)}
    assert requested_pages == [
        ("v4_set_groups_direct_locators_v1", (1,)),
        ("v4_set_groups_direct_members_v1", (0,)),
    ]


@pytest.mark.asyncio
async def test_v4_relation_budget_rejects_huge_pattern_before_member_pages(
    monkeypatch,
) -> None:
    """Reject oversized results after locators and before member reads."""

    class Coordinate:
        block_key = 0
        fragment_no = 0
        block_hash = b"l" * 32
        entry_count = 8

    requested_kinds: list[str] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def fake_manifest(*_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="pattern_groups",
            member_object_kind="v4_pattern_groups_members_v1",
            locator_object_kind="v4_pattern_groups_locators_v1",
            owner_base=0,
            owner_count=8,
            logical_member_count=1_872_272,
            vector_member_count=1_872_272,
            member_width=4,
            member_page_bytes=16_384,
            locator_page_bytes=96,
            locator_owner_span=8,
        )

    async def fake_heavy(*_args, **_kwargs):
        return {}

    async def fake_coordinates(*_args, **kwargs):
        requested_kinds.append(kwargs["object_kind"])
        if kwargs["object_kind"].endswith("members_v1"):
            raise AssertionError("member coordinates must not be loaded")
        return {0: Coordinate()}

    async def fake_blocks(*_args, **kwargs):
        kind = kwargs["object_kind"]
        requested_kinds.append(kind)
        if kind.endswith("members_v1"):
            raise AssertionError("member blocks must not be decoded")
        payload = b"".join(
            struct.pack("<QI", 0, 1_872_272 if owner == 7 else 0)
            for owner in range(8)
        )
        return {b"l" * 32: graph._CachedPhysicalBlock(b"l" * 32, kind, 8, payload)}

    monkeypatch.setattr(graph, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(graph, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(graph, "load_v4_heavy_owners", fake_heavy)
    monkeypatch.setattr(graph, "_load_map_coordinates", fake_coordinates)
    monkeypatch.setattr(graph, "_load_physical_blocks", fake_blocks)

    with pytest.raises(PTG2SharedBlockError, match="exceeds max_members"):
        await graph.lookup_v4_relation_members(
            object(),
            snapshot_key=17,
            relation="pattern_groups",
            owner_keys=(7,),
            schema_name="mrf",
            max_members=1,
        )
    assert requested_kinds == [
        "v4_pattern_groups_locators_v1",
        "v4_pattern_groups_locators_v1",
    ]


def test_v4_sorted_intersection_python_fallback_is_exact(monkeypatch) -> None:
    monkeypatch.setattr(intersection, "_native_intersection", None)
    assert intersection.intersect_sorted_u32((1, 4, 7, 12), (2, 4, 8, 12)) == (
        4,
        12,
    )
    with pytest.raises(ValueError, match="strictly increasing"):
        intersection.intersect_sorted_u32((1, 1), (1,))


def test_v4_sorted_intersection_native_path_receives_validated_values(
    monkeypatch,
) -> None:
    calls: list[tuple[tuple[int, ...], tuple[int, ...]]] = []

    def fake_native(left, right):
        calls.append((tuple(left), tuple(right)))
        return [3]

    monkeypatch.setattr(intersection, "_native_intersection", fake_native)
    assert intersection.intersect_sorted_u32((1, 3), (2, 3)) == (3,)
    assert calls == [((1, 3), (2, 3))]


def test_v4_reverse_scope_cost_and_multi_npi_inversion_are_exact() -> None:
    assert ptg2_serving._is_v4_reverse_scope_cheaper(
        first_member_count=5,
        allowed_provider_set_count=2,
        forward_member_count=100,
        forward_owner_count=10,
        reverse_member_count=4,
        reverse_owner_count=2,
    )
    assert ptg2_serving._v4_sets_by_npi_reverse(
        normalized_npis=(1111111111, 2222222222, 3333333333),
        npi_key_by_value={1111111111: 1, 2222222222: 2},
        first_members_by_npi_key={1: (4, 7), 2: (9,)},
        first_members_by_allowed_set={10: (7,), 11: (4, 9), 12: (3,)},
        allowed_provider_sets=(10, 11, 12),
    ) == {
        1111111111: (10, 11),
        2222222222: (11,),
        3333333333: (),
    }


def test_v4_relation_selection_fails_closed_across_representations() -> None:
    pattern_root = graph.V4GraphRoot(1, "pattern_v1", b"p" * 32)
    direct_root = graph.V4GraphRoot(2, "direct_v1", b"d" * 32)
    graph._validate_relation_for_root(pattern_root, "npi_patterns")
    graph._validate_relation_for_root(direct_root, "group_sets_direct")
    graph._validate_relation_for_root(pattern_root, "npi_groups_exact")
    with pytest.raises(PTG2SharedBlockError, match="does not publish"):
        graph._validate_relation_for_root(pattern_root, "group_sets_direct")
    with pytest.raises(PTG2SharedBlockError, match="does not publish"):
        graph._validate_relation_for_root(direct_root, "npi_patterns")


def test_v4_graph_request_scope_aggregates_nested_lookups_once() -> None:
    before = graph.v4_graph_metrics_snapshot()
    with graph.v4_graph_request_scope() as request_io:
        request_io.database_bytes += 123
        request_io.database_blocks += 2
        with graph.v4_graph_request_scope() as nested:
            assert nested is request_io
            nested.logical_lookups += 2
    after = graph.v4_graph_metrics_snapshot()
    assert after["request_count"] == before["request_count"] + 1
    assert after["database_bytes"] == before["database_bytes"] + 123
    assert after["database_blocks"] == before["database_blocks"] + 2
    assert after["logical_lookups"] == before["logical_lookups"] + 2


def test_v4_graph_request_scope_records_provider_expansion_work() -> None:
    before = graph.v4_graph_metrics_snapshot()
    with graph.v4_graph_request_scope():
        graph.record_v4_provider_expansion_work(
            rate_rows=64,
            provider_sets=3,
            graph_batches=2,
            rejections=1,
        )
    after = graph.v4_graph_metrics_snapshot()

    assert (
        after["provider_expansion_rate_rows"]
        - before["provider_expansion_rate_rows"]
        == 64
    )
    assert (
        after["provider_expansion_provider_sets"]
        - before["provider_expansion_provider_sets"]
        == 3
    )
    assert (
        after["provider_expansion_graph_batches"]
        - before["provider_expansion_graph_batches"]
        == 2
    )
    assert (
        after["provider_expansion_rejections"]
        - before["provider_expansion_rejections"]
        == 1
    )


def test_hot_npi_scope_enforces_sealed_second_hop_work() -> None:
    before = graph.v4_graph_metrics_snapshot()
    with graph.v4_graph_request_scope():
        with graph.v4_graph_hot_npi_scope(
            maximum_members=5,
            maximum_locator_pages=1,
            maximum_member_pages=2,
            maximum_bytes=80,
            maximum_batches=1,
        ):
            graph._charge_v4_hot_npi_work(
                "group_npis_exact",
                member_count=3,
                locator_page_count=1,
                member_page_count=2,
                byte_count=32,
                batch_count=1,
            )
            graph._charge_v4_hot_npi_work(
                "group_npis_exact",
                member_count=2,
                byte_count=32,
                dictionary_read_count=1,
            )
            with pytest.raises(
                PTG2SharedBlockError,
                match="group-to-NPI work exceeds",
            ):
                graph._charge_v4_hot_npi_work(
                    "group_npis_exact",
                    byte_count=17,
                )
    after = graph.v4_graph_metrics_snapshot()
    assert after["hot_group_npi_members"] - before["hot_group_npi_members"] == 5
    assert (
        after["hot_group_npi_locator_pages"]
        - before["hot_group_npi_locator_pages"]
        == 1
    )
    assert (
        after["hot_group_npi_member_pages"]
        - before["hot_group_npi_member_pages"]
        == 2
    )
    assert after["hot_group_npi_bytes"] - before["hot_group_npi_bytes"] == 81
    assert after["hot_group_npi_batches"] - before["hot_group_npi_batches"] == 1
    assert (
        after["hot_npi_dictionary_reads"]
        - before["hot_npi_dictionary_reads"]
        == 1
    )


def _v4_serving_tables(*, patterns: int = 1024) -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        provider_graph_v4_hot_prefix={
            "npi_prefix_target": 201,
            "max_set_patterns_per_set": patterns,
            "max_set_components_per_fallback_set": 4096,
            "max_online_group_keys_per_set": 4096,
            "max_online_source_owners_per_set": 4096,
            "max_online_source_members_per_set": 16384,
            "max_online_source_pages_per_set": 64,
            "max_online_source_bytes_per_set": 1024 * 1024,
            "online_group_npi_batch_size": 32,
            "max_online_group_npi_members_per_set": 32768,
            "max_online_group_npi_locator_pages_per_set": 16,
            "max_online_group_npi_member_pages_per_set": 128,
            "max_online_group_npi_bytes_per_set": 4 * 1024 * 1024,
            "max_online_group_npi_batches_per_set": 4,
            "provider_expansion_rate_page_rows": 64,
            "max_online_provider_expansion_rate_rows": 256,
            "max_online_provider_expansion_provider_sets": 64,
            "max_online_provider_expansion_graph_batches": 64,
        },
    )


@pytest.mark.asyncio
async def test_v4_forward_budget_stops_before_huge_pattern_dictionary_lookup(
    monkeypatch,
) -> None:
    relations: list[tuple[str, int | None]] = []
    dictionary_calls: list[bool] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_set_keys(*_args, **_kwargs):
        return {"set-a": 5}

    async def fake_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        relations.append((relation, kwargs.get("max_members")))
        raise PTG2SharedBlockError("PTG V4 graph selection exceeds max_members")

    async def fake_prefix(*_args, **kwargs):
        relations.append((kwargs["relation"], None))
        return {5: (9,)}

    async def fail_dictionary(*_args, **_kwargs):
        dictionary_calls.append(True)
        raise AssertionError("provider-group dictionary must not be queried")

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "_provider_set_keys_for_ids", fake_set_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix,
    )
    monkeypatch.setattr(
        ptg2_serving, "_shared_provider_group_ids_for_keys", fail_dictionary
    )

    with pytest.raises(PTG2SharedBlockError, match="exceeds max_members"):
        await ptg2_serving._v4_shared_graph_members_many(
            object(),
            _v4_serving_tables(),
            "provider_forward",
            ["set-a"],
            max_members=1,
        )
    assert relations == [("set_patterns", None), ("pattern_groups", 1)]
    assert not dictionary_calls


@pytest.mark.asyncio
@pytest.mark.parametrize("member_budget", [0, 1])
async def test_v4_inverse_budget_preserves_exact_empty_orphan_groups(
    monkeypatch, member_budget: int
) -> None:
    calls: list[tuple[str, tuple[int, ...], int | None]] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_lookup(*_args, **kwargs):
        call = (
            kwargs["relation"],
            tuple(kwargs["owner_keys"]),
            kwargs.get("max_members"),
        )
        calls.append(call)
        if kwargs["relation"] == "group_patterns":
            return {3: (9,), 4: (9,)}
        # The empty-incidence pattern has a zero-count locator, so the real
        # relation reader returns here without loading a member page.
        return {9: ()}

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)

    assert await ptg2_serving._v4_members_through_projection(
        object(),
        _v4_serving_tables(),
        owner_keys=(3, 4),
        direct_relation="group_sets_direct",
        projection_relation="group_patterns",
        projected_member_relation="pattern_sets",
        max_members=member_budget,
    ) == {3: (), 4: ()}
    assert calls == [
        ("group_patterns", (3, 4), None),
        ("pattern_sets", (9,), member_budget),
    ]


@pytest.mark.asyncio
async def test_v4_projection_uncapped_preserves_exact_pattern_union(monkeypatch) -> None:
    calls: list[tuple[str, int | None]] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        calls.append((relation, kwargs.get("max_members")))
        return {7: (10, 12), 9: (11, 13)}

    async def fake_prefix(*_args, **kwargs):
        calls.append((kwargs["relation"], None))
        return {1: (7, 9), 2: (9,)}

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix,
    )

    assert await ptg2_serving._v4_members_through_projection(
        object(),
        _v4_serving_tables(),
        owner_keys=(1, 2),
        direct_relation="set_groups_direct",
        projection_relation="set_patterns",
        projected_member_relation="pattern_groups",
    ) == {1: (10, 11, 12, 13), 2: (11, 13)}
    assert calls == [("set_patterns", None), ("pattern_groups", None)]


@pytest.mark.asyncio
async def test_v4_pattern_npi_hot_path_never_enumerates_exact_groups(
    monkeypatch,
) -> None:
    relations: list[str] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_npi_keys(*_args, **_kwargs):
        return {1234567890: 4}

    async def fake_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        relations.append(relation)
        if relation == "npi_patterns":
            return {4: (2, 5)}
        if relation == "pattern_sets":
            return {2: (3, 8), 5: (8, 10)}
        raise AssertionError(f"unexpected relation {relation}")

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "v4_npi_keys_for_values", fake_npi_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    observed = await ptg2_serving._v4_sets_by_npi(
        object(),
        _v4_serving_tables(),
        (1234567890,),
    )
    assert observed == {1234567890: (3, 8, 10)}
    assert relations == ["npi_patterns", "pattern_sets"]


@pytest.mark.asyncio
async def test_v4_pattern_single_npi_uses_rate_scoped_reverse_direction(
    monkeypatch,
) -> None:
    relations: list[str] = []

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_npi_keys(*_args, **_kwargs):
        return {1234567890: 4}

    async def fake_manifest(*_args, **kwargs):
        relation = kwargs["relation"]
        return type(
            "Manifest",
            (),
            {
                "owner_count": 1 if relation == "pattern_sets" else 2,
                "logical_member_count": 100 if relation == "pattern_sets" else 3,
            },
        )()

    async def fake_npi_keys(*_args, **_kwargs):
        return {1234567890: 4}

    async def fake_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        relations.append(relation)
        if relation == "npi_patterns":
            return {4: (2,)}
        raise AssertionError(f"unexpected relation {relation}")

    async def fake_prefix(*_args, **kwargs):
        relations.append(kwargs["relation"])
        return {8: (2, 5), 12: (7,)}

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(ptg2_serving, "v4_npi_keys_for_values", fake_npi_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix,
    )
    observed = await ptg2_serving._v4_sets_by_npi(
        object(),
        _v4_serving_tables(),
        (1234567890,),
        allowed_provider_set_keys=frozenset({8, 12}),
    )
    assert observed == {1234567890: (8,)}
    assert relations == ["npi_patterns", "set_patterns"]


def _patch_pattern_overflow_walk(
    monkeypatch,
    visited_relations: list[str],
) -> None:
    async def fake_npi_keys(*_args, **_kwargs):
        return {1234567890: 4}

    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_manifest(*_args, **kwargs):
        relation = kwargs["relation"]
        return type(
            "Manifest",
            (),
            {
                "owner_count": 1 if relation == "pattern_sets" else 2,
                "logical_member_count": 100 if relation == "pattern_sets" else 3,
            },
        )()

    async def fake_lookup(*_args, **kwargs):
        relation = kwargs["relation"]
        visited_relations.append(relation)
        if relation == "npi_patterns":
            return {4: (2,)}
        if relation == "pattern_sets":
            return {2: (8,)}
        raise AssertionError(f"unexpected relation {relation}")

    async def fake_prefix(*_args, **kwargs):
        visited_relations.append(kwargs["relation"])
        return {8: (2, 5), 12: (7,)}

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(
        ptg2_serving,
        "v4_npi_keys_for_values",
        fake_npi_keys,
    )
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_v4_relation_member_prefixes",
        fake_prefix,
    )


@pytest.mark.asyncio
async def test_v4_pattern_overflow_skips_rate_scoped_reverse_direction(
    monkeypatch,
) -> None:
    """Use the bounded forward path before the scoped reverse overflow walk."""

    visited_relations: list[str] = []
    _patch_pattern_overflow_walk(monkeypatch, visited_relations)

    observed = await ptg2_serving._v4_sets_by_npi(
        object(),
        _v4_serving_tables(patterns=1),
        (1234567890,),
        allowed_provider_set_keys=frozenset({8, 12}),
    )

    assert observed == {1234567890: (8,)}
    assert visited_relations == [
        "npi_patterns",
        "set_patterns",
        "pattern_sets",
    ]


@pytest.mark.asyncio
async def test_v4_multi_npi_rate_scope_keeps_native_forward_intersection(
    monkeypatch,
) -> None:
    async def fake_root(*_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"p" * 32)

    async def fake_npi_keys(*_args, **_kwargs):
        return {1234567890: 4, 2234567890: 6}

    async def fake_manifest(*_args, **kwargs):
        relation = kwargs["relation"]
        return type(
            "Manifest",
            (),
            {
                "owner_count": 2,
                "logical_member_count": 2 if relation == "pattern_sets" else 100,
            },
        )()

    async def fake_lookup(*_args, **kwargs):
        if kwargs["relation"] == "npi_patterns":
            return {4: (2,), 6: (5,)}
        return {2: (3, 8), 5: (10, 12)}

    native_calls: list[tuple[tuple[int, ...], tuple[int, ...]]] = []

    def fake_intersection(left, right):
        native_calls.append((tuple(left), tuple(right)))
        return tuple(sorted(set(left).intersection(right)))

    monkeypatch.setattr(ptg2_serving, "load_v4_graph_root", fake_root)
    monkeypatch.setattr(ptg2_serving, "load_v4_relation_manifest", fake_manifest)
    monkeypatch.setattr(ptg2_serving, "v4_npi_keys_for_values", fake_npi_keys)
    monkeypatch.setattr(ptg2_serving, "lookup_v4_relation_members", fake_lookup)
    monkeypatch.setattr(ptg2_serving, "intersect_sorted_u32", fake_intersection)
    observed = await ptg2_serving._v4_sets_by_npi(
        object(),
        _v4_serving_tables(),
        (1234567890, 2234567890),
        allowed_provider_set_keys=frozenset({8, 12}),
    )
    assert observed == {1234567890: (8,), 2234567890: (12,)}
    assert native_calls == [((8,), (8, 12)), ((12,), (8, 12))]


@pytest.mark.asyncio
async def test_v4_explicit_npi_scopes_graph_to_requested_code_first(
    monkeypatch,
) -> None:
    rate_scope_calls: list[dict[str, object]] = []
    graph_calls: list[frozenset[int] | None] = []

    async def fake_rate_scope(*_args, **kwargs):
        rate_scope_calls.append(kwargs)
        return (3, 8)

    async def fake_graph(*_args, **kwargs):
        graph_calls.append(kwargs.get("allowed_provider_set_keys"))
        return {1234567890: (8,)}

    monkeypatch.setattr(
        ptg2_serving, "_shared_rate_provider_set_keys", fake_rate_scope
    )
    monkeypatch.setattr(
        ptg2_serving, "_v4_sets_by_npi", fake_graph
    )
    scope = await ptg2_serving._version_three_explicit_npi_graph_scope(
        object(),
        _v4_serving_tables(),
        {
            "npi": "1234567890",
            "plan_id": "plan-1",
            "code_system": "CPT",
            "code": "70553",
        },
    )
    assert scope == ptg2_serving._ExplicitNpiGraphScope(1234567890, (8,))
    assert rate_scope_calls == [
        {
            "plan_id": "plan-1",
            "plan_market_type": "",
            "reported_code": "70553",
            "code_system": "CPT",
        }
    ]
    assert graph_calls == [frozenset({3, 8})]
