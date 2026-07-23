# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deterministic serving harness for incremental V4 CPT expansion tests."""

from __future__ import annotations

from types import SimpleNamespace

from api import ptg2_serving as serving
from api.ptg2_types import PTG2ServingTables


def provider_set_id(value: int) -> str:
    return f"{value:032x}"


def rate_row(
    provider_set_value: int,
    occurrence: int,
    *,
    price_key: int,
    source_key: int = 0,
    provider_count: int | None = None,
) -> dict[str, object]:
    rate_by_field: dict[str, object] = {
        "provider_set_global_id_128": provider_set_id(provider_set_value),
        "serving_content_hash_128": f"{occurrence:032x}",
        "reported_code_system": "CPT",
        "reported_code": "70553",
        "negotiation_arrangement": "FFS",
        "source_key": source_key,
        "price_key": price_key,
        "_ptg_provider_set_key": provider_set_value,
    }
    if provider_count is not None:
        rate_by_field["provider_count"] = provider_count
    return rate_by_field


def v4_tables() -> PTG2ServingTables:
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=71,
        storage_generation="shared_blocks_v4",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="packed_snapshot_maps_v4",
        source_count=1,
        source_key="synthetic-source",
    )


class IncrementalExpansionHarness:
    def __init__(
        self,
        monkeypatch,
        rate_rows: list[dict[str, object]],
        npis_by_set: dict[str, tuple[int, ...]],
        *,
        sealed_cap_overrides: dict[str, int] | None = None,
    ) -> None:
        self.rate_rows = rate_rows
        self.npis_by_set = npis_by_set
        self.rate_reads: list[tuple[int, int, bool]] = []
        self.completion_reads: list[
            tuple[tuple[int, ...], int, int, bool]
        ] = []
        self.graph_batches: list[tuple[str, ...]] = []
        self.compact_scope_reads = 0
        self.completion_membership_calls: list[
            tuple[tuple[int, ...], frozenset[int]]
        ] = []
        self.provider_memberships: dict[int, tuple[str, ...]] = {}
        sealed_caps_by_field = {
            "target": 201,
            "provider_expansion_rate_page_rows": 64,
            "maximum_provider_expansion_rate_rows": 256,
            "maximum_provider_expansion_provider_sets": 64,
            "maximum_provider_expansion_graph_batches": 64,
            **(sealed_cap_overrides or {}),
        }
        self._patch_serving_dependencies(
            monkeypatch,
            sealed_caps_by_field,
        )

    def _patch_serving_dependencies(
        self,
        monkeypatch,
        sealed_caps_by_field: dict[str, int],
    ) -> None:
        """Install deterministic rate, graph, and completion readers."""

        monkeypatch.setattr(
            serving,
            "_v4_hot_prefix_limits",
            lambda _tables: SimpleNamespace(**sealed_caps_by_field),
        )
        monkeypatch.setattr(
            serving,
            "_merge_manifest_code_variant_rows",
            self.merge_rows,
        )
        monkeypatch.setattr(
            serving,
            "_provider_npis_for_sets",
            self.provider_npis,
        )
        monkeypatch.setattr(
            serving,
            "_selected_provider_rows_by_set",
            self.provider_rows,
        )
        monkeypatch.setattr(
            serving,
            "_shared_forward_entries_for_code_rows",
            self.forward_entries,
        )
        monkeypatch.setattr(
            serving,
            "_v4_sets_by_npi",
            self.provider_sets_by_npi,
        )
        monkeypatch.setattr(
            serving,
            "_provider_set_ids_for_keys",
            self.provider_set_ids_for_keys,
        )

    def ordered_rows(self, descending: bool) -> list[dict[str, object]]:
        return sorted(
            self.rate_rows,
            key=lambda row: (
                -int(row["price_key"])
                if descending
                else int(row["price_key"]),
                int(row["_ptg_provider_set_key"]),
                int(row["source_key"]),
            ),
        )

    async def merge_rows(
        self,
        *_args,
        limit,
        offset,
        descending,
        provider_set_keys,
        **_kwargs,
    ):
        ordered_rows = self.ordered_rows(bool(descending))
        if provider_set_keys is None:
            self.rate_reads.append(
                (int(limit), int(offset), bool(descending))
            )
            return ordered_rows[int(offset) : int(offset) + int(limit)]
        normalized_provider_set_keys = tuple(
            sorted(int(provider_set_key) for provider_set_key in provider_set_keys)
        )
        self.completion_reads.append(
            (
                normalized_provider_set_keys,
                int(limit),
                int(offset),
                bool(descending),
            )
        )
        filtered_rows = [
            rate_row_by_field
            for rate_row_by_field in ordered_rows
            if int(rate_row_by_field["_ptg_provider_set_key"])
            in normalized_provider_set_keys
        ]
        return filtered_rows[int(offset) : int(offset) + int(limit)]

    async def forward_entries(self, *_args, **_kwargs):
        self.compact_scope_reads += 1
        return [
            SimpleNamespace(
                provider_set_key=int(row["_ptg_provider_set_key"])
            )
            for row in self.rate_rows
        ]

    async def provider_sets_by_npi(
        self,
        _session,
        _tables,
        npis,
        *,
        allowed_provider_set_keys,
    ):
        normalized_npis = tuple(int(npi) for npi in npis)
        normalized_allowed_keys = frozenset(
            int(provider_set_key)
            for provider_set_key in allowed_provider_set_keys
        )
        self.completion_membership_calls.append(
            (normalized_npis, normalized_allowed_keys)
        )
        return {
            npi: tuple(
                sorted(
                    int(provider_set_id_value, 16)
                    for provider_set_id_value, provider_npis in (
                        self.npis_by_set.items()
                    )
                    if npi in provider_npis
                    and int(provider_set_id_value, 16)
                    in normalized_allowed_keys
                )
            )
            for npi in normalized_npis
        }

    async def provider_set_ids_for_keys(
        self,
        _session,
        _tables,
        provider_set_keys,
    ):
        return {
            int(provider_set_key): provider_set_id(int(provider_set_key))
            for provider_set_key in provider_set_keys
        }

    async def provider_npis(
        self,
        _session,
        _tables,
        provider_set_ids,
        *,
        limit_per_set,
    ):
        assert limit_per_set > 0
        provider_set_ids = tuple(provider_set_ids)
        self.graph_batches.append(provider_set_ids)
        return {
            provider_set_id: self.npis_by_set.get(provider_set_id, ())[
                :limit_per_set
            ]
            for provider_set_id in provider_set_ids
        }

    async def provider_rows(
        self,
        *_args,
        npis,
        provider_set_ids_by_npi,
        **_kwargs,
    ):
        self.provider_memberships = dict(provider_set_ids_by_npi)
        provider_set_ids = tuple(
            dict.fromkeys(
                provider_set_id
                for npi in npis
                for provider_set_id in provider_set_ids_by_npi[npi]
            )
        )
        return {
            provider_set_id: [
                {"npi": npi, "provider_name": f"Provider {npi}"}
                for npi in npis
                if provider_set_id in provider_set_ids_by_npi[npi]
            ]
            for provider_set_id in provider_set_ids
        }

    async def select(
        self,
        *,
        target_count: int,
        descending: bool = False,
    ):
        return await serving._strict_cost_provider_expansion_selection(
            object(),
            v4_tables(),
            code_rows=[
                {"code_key": 4, "rate_count": len(self.rate_rows)}
            ],
            args={"plan_id": "synthetic-plan"},
            snapshot_id="synthetic-snapshot",
            source_trace_set_hash=None,
            network_names=[],
            target_count=target_count,
            descending=descending,
        )
