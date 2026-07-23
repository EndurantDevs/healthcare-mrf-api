# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from api import ptg2_v4_graph as graph


class MixedComponentPrefixHarness:
    """Serve one ordinary pattern set and one component-fallback set."""

    normal_set_id = "06" * 16
    overflow_set_id = "07" * 16

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[int, ...], int]] = []

    async def provider_metadata(self, *_args, **_kwargs):
        return (
            {self.normal_set_id: 1, self.overflow_set_id: 2},
            {self.normal_set_id: 2, self.overflow_set_id: 2},
        )

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def prefix_lookup(self, *_args, **kwargs):
        relation = kwargs["relation"]
        owner_keys = tuple(kwargs["owner_keys"])
        limit = int(kwargs["limit_per_owner"])
        self.calls.append((relation, owner_keys, limit))
        relation_values = {
            "set_patterns": {1: (7,), 2: (8, 9, 10)},
            "set_components": {2: (20, 21)},
            "pattern_groups": {7: (1, 3)},
            "component_groups": {20: (2, 4), 21: (1, 5)},
            "group_npis_exact": {1: (10,), 2: (12,), 3: (11,)},
        }[relation]
        return {
            owner_key: relation_values[owner_key]
            for owner_key in owner_keys
        }

    async def npi_values(self, *_args, **kwargs):
        return {
            npi_key: 1_000_000_000 + npi_key
            for npi_key in kwargs["npi_keys"]
        }


class ComponentDuplicateTailHarness:
    """Expose an overflow set whose later groups repeat its only exact NPI."""

    provider_set_id = "0a" * 16

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[int, ...], int]] = []

    async def provider_metadata(self, *_args, **_kwargs):
        return ({self.provider_set_id: 1}, {self.provider_set_id: 1})

    async def root(self, *_args, **_kwargs):
        return graph.V4GraphRoot(17, "pattern_v1", b"r" * 32)

    async def prefix_lookup(self, *_args, **kwargs):
        relation = kwargs["relation"]
        owner_keys = tuple(kwargs["owner_keys"])
        limit = int(kwargs["limit_per_owner"])
        self.calls.append((relation, owner_keys, limit))
        relation_values = {
            "set_patterns": {1: (7, 8)},
            "set_components": {1: (20,)},
            "component_groups": {20: (1, 2, 3)[:limit]},
            "group_npis_exact": {1: (10,), 2: (10,), 3: (10,)},
        }[relation]
        return {
            owner_key: relation_values[owner_key]
            for owner_key in owner_keys
        }

    async def npi_values(self, *_args, **kwargs):
        return {
            npi_key: 1_000_000_000 + npi_key
            for npi_key in kwargs["npi_keys"]
        }
