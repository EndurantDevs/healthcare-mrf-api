# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import struct

from api import ptg2_v4_graph as graph


def sealed_v4_hot_prefix(**overrides: object) -> dict[str, object]:
    """Return the common sealed hot-path limits used by serving tests."""

    limits_by_name: dict[str, object] = {
        "npi_prefix_target": 201,
        "max_set_patterns_per_set": 1024,
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
    }
    limits_by_name.update(overrides)
    return limits_by_name


class Coordinate:
    def __init__(
        self,
        block_key: int,
        block_hash: bytes,
        entry_count: int,
    ) -> None:
        self.block_key = block_key
        self.fragment_no = 0
        self.block_hash = block_hash
        self.entry_count = entry_count


class DuplicateNpiPrefixHarness:
    def __init__(self) -> None:
        self.pattern_limits: list[int] = []
        self.npi_group_batches: list[tuple[int, ...]] = []

    async def prefix_lookup(self, *_args, **kwargs):
        relation = kwargs["relation"]
        owners = tuple(kwargs["owner_keys"])
        if relation == "set_patterns":
            return {1: (7,)}
        if relation == "pattern_groups":
            limit = kwargs["limit_per_owner"]
            self.pattern_limits.append(limit)
            return {7: (1, 2, 3)[:limit]}
        self.npi_group_batches.append(owners)
        npi_values_by_group = {1: (10,), 2: (10,), 3: (11,)}
        return {owner: npi_values_by_group[owner] for owner in owners}


class RegularPrefixHarness:
    def __init__(self) -> None:
        self.requested_pages: list[tuple[str, tuple[int, ...]]] = []

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def manifest(self, *_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="pattern_groups",
            member_object_kind="v4_pattern_groups_members_v1",
            locator_object_kind="v4_pattern_groups_locators_v1",
            owner_base=0,
            owner_count=1,
            logical_member_count=736_869,
            vector_member_count=736_869,
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=12,
            locator_owner_span=1,
        )

    async def heavy_owners(self, *_args, **_kwargs):
        return {}

    async def coordinates(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        block_keys = tuple(sorted(kwargs["block_keys"]))
        self.requested_pages.append((object_kind, block_keys))
        if object_kind.endswith("locators_v1"):
            return {0: Coordinate(0, b"l" * 32, 1)}
        return {
            page_key: Coordinate(
                page_key,
                bytes((page_key // 4 + 1,)) * 32,
                4,
            )
            for page_key in block_keys
        }

    async def blocks(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        selected_coordinates = tuple(kwargs["coordinates"])
        if object_kind.endswith("locators_v1"):
            return {
                b"l" * 32: graph._CachedPhysicalBlock(
                    b"l" * 32,
                    object_kind,
                    1,
                    struct.pack("<QI", 0, 736_869),
                )
            }
        return {
            coordinate.block_hash: graph._CachedPhysicalBlock(
                coordinate.block_hash,
                object_kind,
                coordinate.entry_count,
                struct.pack(
                    "<IIII",
                    coordinate.block_key + 1,
                    coordinate.block_key + 2,
                    coordinate.block_key + 3,
                    coordinate.block_key + 4,
                ),
            )
            for coordinate in selected_coordinates
        }


class HeavyPrefixHarness:
    def __init__(self) -> None:
        self.owner = graph.V4HeavyOwner(
            relation="pattern_groups",
            owner_key=0,
            object_kind="v4_pattern_groups_heavy_bitmap_v1",
            member_count=6,
            member_base=10,
            member_span=24,
            fragment_count=3,
        )
        self.coordinate_by_pair = {
            (0, 0): Coordinate(0, b"a" * 32, 2),
            (0, 1): Coordinate(0, b"b" * 32, 1),
            (0, 2): Coordinate(0, b"c" * 32, 3),
        }
        header = b"PTG2V4BM" + struct.pack("<IIII", 0, 10, 24, 6)

        def framed_fragment(
            fragment_no: int,
            entry_count: int,
            payload: bytes,
        ) -> bytes:
            return (
                struct.pack(
                    "<8sIIIIII",
                    b"PTG2V4BF",
                    0,
                    10,
                    24,
                    6,
                    fragment_no,
                    entry_count,
                )
                + payload
            )

        self.payload_by_hash = {
            b"a" * 32: framed_fragment(
                0,
                2,
                header + bytes((0b00000011,)),
            ),
            b"b" * 32: framed_fragment(1, 1, bytes((0b00000001,))),
            b"c" * 32: framed_fragment(2, 3, bytes((0b00000111,))),
        }
        self.requested_coordinate_pairs: list[tuple[int, int]] = []
        self.requested_block_hashes: list[bytes] = []

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def manifest(self, *_args, **_kwargs):
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation="pattern_groups",
            member_object_kind="v4_pattern_groups_members_v1",
            locator_object_kind="v4_pattern_groups_locators_v1",
            owner_base=0,
            owner_count=1,
            logical_member_count=6,
            vector_member_count=0,
            member_width=4,
            member_page_bytes=32,
            locator_page_bytes=12,
            locator_owner_span=1,
        )

    async def heavy_owners(self, *_args, **_kwargs):
        return {0: self.owner}

    async def coordinates(self, *_args, **kwargs):
        self.requested_coordinate_pairs.extend(kwargs["coordinate_pairs"])
        return self.coordinate_by_pair

    async def blocks(self, *_args, **kwargs):
        selected_coordinates = tuple(kwargs["coordinates"])
        self.requested_block_hashes.extend(
            coordinate.block_hash for coordinate in selected_coordinates
        )
        return {
            coordinate.block_hash: graph._CachedPhysicalBlock(
                coordinate.block_hash,
                self.owner.object_kind,
                coordinate.entry_count,
                self.payload_by_hash[coordinate.block_hash],
            )
            for coordinate in selected_coordinates
        }


class PatternServingHarness:
    def __init__(self) -> None:
        self.first_set = "01" * 16
        self.second_set = "02" * 16
        self.full_relations: list[str] = []
        self.prefix_calls: list[tuple[str, tuple[int, ...], int]] = []

    async def set_keys(self, *_args, **_kwargs):
        return {self.first_set: 1, self.second_set: 2}

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def full_lookup(self, *_args, **kwargs):
        relation = kwargs["relation"]
        self.full_relations.append(relation)
        if relation != "set_patterns":
            raise AssertionError("provider groups must use bounded prefix reads")
        return {1: (7, 9), 2: (9,)}

    async def prefix_lookup(self, *_args, **kwargs):
        relation = kwargs["relation"]
        owner_keys = tuple(kwargs["owner_keys"])
        limit = kwargs["limit_per_owner"]
        self.prefix_calls.append((relation, owner_keys, limit))
        if relation == "set_patterns":
            return {1: (7, 9), 2: (9,)}
        if relation == "pattern_groups":
            return {7: (2, 5, 8), 9: (1, 3, 4)}
        assert relation == "group_npis_exact"
        npi_keys_by_group = {
            1: (10, 11),
            2: (10, 12),
            3: (10, 13),
            4: (10, 14),
        }
        return {
            owner_key: npi_keys_by_group[owner_key]
            for owner_key in owner_keys
        }

    async def npi_values(self, *_args, **kwargs):
        return {
            npi_key: 1_000_000_000 + npi_key
            for npi_key in kwargs["npi_keys"]
        }


class PathologicalSetPatternHarness:
    """Expose a 100k-degree owner while recording bounded physical reads."""

    provider_set_key = 5
    member_count = 100_000
    component_count = 2

    def __init__(self) -> None:
        self.requested_pages: list[tuple[str, tuple[int, ...]]] = []
        self.loaded_block_count = 0
        self.loaded_raw_bytes = 0

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def manifest(self, *_args, **kwargs):
        relation = kwargs["relation"]
        count = (
            self.component_count
            if relation == "set_components"
            else self.member_count
        )
        return graph.V4RelationManifest(
            snapshot_key=17,
            relation=relation,
            member_object_kind=f"v4_{relation}_members_v1",
            locator_object_kind=f"v4_{relation}_locators_v1",
            owner_base=self.provider_set_key,
            owner_count=1,
            logical_member_count=count,
            vector_member_count=count,
            member_width=4,
            member_page_bytes=16,
            locator_page_bytes=12,
            locator_owner_span=1,
        )

    async def heavy_owners(self, *_args, **_kwargs):
        return {}

    async def coordinates(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        block_keys = tuple(sorted(kwargs["block_keys"]))
        self.requested_pages.append((object_kind, block_keys))
        if object_kind.endswith("locators_v1"):
            return {
                self.provider_set_key: Coordinate(
                    self.provider_set_key,
                    b"l" * 32,
                    1,
                )
            }
        return {
            page_key: Coordinate(
                page_key,
                (
                    b"c" * 32
                    if "set_components" in object_kind
                    else bytes((page_key // 4 + 1,)) * 32
                ),
                (
                    self.component_count
                    if "set_components" in object_kind
                    else 4
                ),
            )
            for page_key in block_keys
        }

    async def blocks(self, *_args, **kwargs):
        object_kind = kwargs["object_kind"]
        selected_coordinates = tuple(kwargs["coordinates"])
        if object_kind.endswith("locators_v1"):
            count = (
                self.component_count
                if "set_components" in object_kind
                else self.member_count
            )
            block_by_hash = {
                b"l" * 32: graph._CachedPhysicalBlock(
                    b"l" * 32,
                    object_kind,
                    1,
                    struct.pack("<QI", 0, count),
                )
            }
        else:
            block_by_hash = {
                coordinate.block_hash: graph._CachedPhysicalBlock(
                    coordinate.block_hash,
                    object_kind,
                    coordinate.entry_count,
                    (
                        struct.pack("<II", 101, 102)
                        if "set_components" in object_kind
                        else struct.pack(
                            "<IIII",
                            coordinate.block_key + 1,
                            coordinate.block_key + 2,
                            coordinate.block_key + 3,
                            coordinate.block_key + 4,
                        )
                    ),
                )
                for coordinate in selected_coordinates
            }
        self.loaded_block_count += len(block_by_hash)
        self.loaded_raw_bytes += sum(
            len(block.payload) for block in block_by_hash.values()
        )
        return block_by_hash
