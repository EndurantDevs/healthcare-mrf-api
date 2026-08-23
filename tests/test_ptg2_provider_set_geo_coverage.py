# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact provider-set eligibility for broad geo-filtered rate searches."""

from unittest.mock import AsyncMock
from types import SimpleNamespace

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import (
    FakeResult,
    FakeSession,
    strict_v3_tables,
)


class _G0289ServingHarness:
    provider_set_ids_by_key = {7: "07" * 16, 8: "08" * 16, 9: "09" * 16}
    price_set_ids_by_key = {7: "17" * 16, 8: "18" * 16, 9: "19" * 16}
    rates_by_key = {7: "30.00", 8: "20.00", 9: "5.00"}

    def __init__(self):
        self.merged_provider_set_keys: list[frozenset[int]] = []

    def install(self, monkeypatch) -> None:
        replacement_by_function_name = {
            "_ptg2_manifest_location_match_limit": lambda: 2,
            "_membership_location_rows": self.location_rows,
            "_membership_npi_rows": self.location_rows,
            "_shared_provider_set_keys_by_npi": self.provider_sets_by_npi,
            "_shared_rate_provider_set_keys": self.rate_provider_set_keys,
            "_provider_set_ids_for_keys": self.provider_set_ids_for_keys,
            "_provider_set_keys_for_ids": self.provider_set_keys_for_ids,
            "_merge_manifest_code_variant_rows": self.merge_rate_rows,
            "_hydrate_provider_set_network_names": self.noop,
            "_prices_for_price_sets": self.prices_for_sets,
            "_procedure_details_for_rows": self.procedure_details,
        }
        for function_name, replacement in replacement_by_function_name.items():
            monkeypatch.setattr(serving, function_name, replacement)

    async def response(self, *, limit: int, offset: int) -> dict:
        return await serving._search_manifest_serving_table(
            self.session(),
            "ptg2:209901:synthetic",
            {
                "plan_id": "TEST-PLAN-001",
                "plan_market_type": "group",
                "code_system": "HCPCS",
                "code": "G0289",
                "zip5": "48201",
                "zip_radius_miles": 30,
                "order_by": "rate",
                "include_providers": "false",
            },
            SimpleNamespace(limit=limit, offset=offset),
            strict_v3_tables(snapshot_id="ptg2:209901:synthetic"),
            "product_search",
        )

    async def location_rows(self, *_args, **_kwargs):
        return [
            _location(101, source_exhausted=True),
            _location(102, source_exhausted=True),
            _location(103, source_exhausted=True),
        ]

    async def provider_sets_by_npi(self, *_args, **_kwargs):
        return {101: {7}, 102: {8}, 103: {7}}

    async def rate_provider_set_keys(self, *_args, **_kwargs):
        return frozenset(self.provider_set_ids_by_key)

    async def provider_set_ids_for_keys(
        self,
        _session,
        _serving_tables,
        provider_set_keys,
    ):
        return {
            provider_set_key: self.provider_set_ids_by_key[provider_set_key]
            for provider_set_key in provider_set_keys
        }

    async def provider_set_keys_for_ids(
        self,
        _session,
        _serving_tables,
        provider_set_ids,
    ):
        key_by_id = {
            provider_set_id: provider_set_key
            for provider_set_key, provider_set_id in self.provider_set_ids_by_key.items()
        }
        return {
            provider_set_id: key_by_id[provider_set_id]
            for provider_set_id in provider_set_ids
        }

    async def merge_rate_rows(self, *_args, **kwargs):
        provider_set_keys = frozenset(kwargs["provider_set_keys"])
        self.merged_provider_set_keys.append(provider_set_keys)
        return [
            self._rate_row(provider_set_key)
            for provider_set_key in sorted(provider_set_keys)
        ]

    async def prices_for_sets(self, _session, _tables, price_set_ids, **_kwargs):
        rate_by_price_set_id = {
            self.price_set_ids_by_key[key]: rate
            for key, rate in self.rates_by_key.items()
        }
        return {
            price_set_id: [
                {"negotiated_rate": rate_by_price_set_id[price_set_id]}
            ]
            for price_set_id in price_set_ids
        }

    async def noop(self, *_args, **_kwargs):
        return None

    async def procedure_details(self, *_args, **_kwargs):
        return {}

    def session(self) -> FakeSession:
        return FakeSession(
            [
                FakeResult(
                    [
                        {
                            "code_key": 1,
                            "plan_id": "TEST-PLAN-001",
                            "plan_market_type": "group",
                            "reported_code_system": "HCPCS",
                            "reported_code": "G0289",
                            "negotiation_arrangement": "FFS",
                            "rate_count": 3,
                        }
                    ]
                )
            ]
        )

    def _rate_row(self, provider_set_key: int) -> dict[str, object]:
        return {
            "serving_content_hash_128": f"{provider_set_key:02d}" * 16,
            "plan_id": "TEST-PLAN-001",
            "plan_market_type": "group",
            "reported_code_system": "HCPCS",
            "reported_code": "G0289",
            "negotiation_arrangement": "FFS",
            "provider_set_global_id_128": self.provider_set_ids_by_key[
                provider_set_key
            ],
            "_ptg_provider_set_key": provider_set_key,
            "provider_count": 1,
            "price_set_global_id_128": self.price_set_ids_by_key[
                provider_set_key
            ],
            "price_key": provider_set_key,
            "source_key": 0,
            "network_names": [],
        }


def _location(npi: int, *, source_exhausted: bool) -> dict[str, object]:
    return {
        "npi": npi,
        "_ptg_source_exhausted": source_exhausted,
    }


@pytest.mark.asyncio
async def test_g0289_geo_rate_pages_filter_by_exact_provider_set_coverage(
    monkeypatch,
):
    """Return 200-equivalent ordered pages beyond the former NPI cap."""

    harness = _G0289ServingHarness()
    harness.install(monkeypatch)

    full_page = await harness.response(limit=2, offset=0)
    second_page = await harness.response(limit=1, offset=1)

    assert harness.merged_provider_set_keys == [
        frozenset({7, 8}),
        frozenset({7, 8}),
    ]
    assert [
        pricing_item["prices"][0]["negotiated_rate"]
        for pricing_item in full_page["items"]
    ] == [20, 30]
    assert second_page["items"][0]["prices"][0]["negotiated_rate"] == 30
    assert full_page["pagination"] == {
        "total": 2,
        "limit": 2,
        "offset": 0,
        "page": 1,
        "has_more": False,
        "total_is_exact": True,
        "total_lower_bound": 2,
    }
    assert second_page["pagination"] == {
        "total": 2,
        "limit": 1,
        "offset": 1,
        "page": 2,
        "has_more": False,
        "total_is_exact": True,
        "total_lower_bound": 2,
    }


@pytest.mark.asyncio
async def test_coverage_stops_after_every_rate_set_has_a_witness(monkeypatch):
    """Do not enumerate a dense network after geo eligibility is exact."""

    location_rows = AsyncMock(
        return_value=[
            _location(101, source_exhausted=False),
            _location(102, source_exhausted=False),
        ]
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location_rows)
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        AsyncMock(return_value={101: {7}, 102: {8}}),
    )
    monkeypatch.setattr(
        serving,
        "_graph_location_probe_batch_size",
        lambda *_args, **_kwargs: 2,
    )

    candidates = await _provider_set_coverage(frozenset({7, 8}))

    assert candidates == serving._GraphLocationCandidates(
        [
            _location(101, source_exhausted=False),
            _location(102, source_exhausted=False),
        ],
        {101: {7}, 102: {8}},
    )
    location_rows.assert_awaited_once()


@pytest.mark.asyncio
async def test_coverage_fails_closed_before_unproven_source_end(monkeypatch):
    """Surface a work-budget response when one rate set remains unproven."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(
            return_value=[
                _location(101, source_exhausted=False),
                _location(102, source_exhausted=False),
            ]
        ),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        AsyncMock(return_value={101: {7}, 102: {7}}),
    )
    monkeypatch.setattr(
        serving,
        "_graph_location_probe_batch_size",
        lambda *_args, **_kwargs: 2,
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_manifest_location_match_limit",
        lambda: 0,
    )

    with pytest.raises(serving.PTG2OnlineWorkBudgetExceeded) as exc_info:
        await _provider_set_coverage(frozenset({7, 8}))

    assert exc_info.value.dimension == "candidate_members"


@pytest.mark.asyncio
async def test_coverage_accepts_absent_sets_after_source_exhaustion(monkeypatch):
    """Treat an unwitnessed provider set as absent only after exact exhaustion."""

    monkeypatch.setattr(
        serving,
        "_membership_location_rows",
        AsyncMock(
            return_value=[
                _location(101, source_exhausted=True),
                _location(102, source_exhausted=True),
            ]
        ),
    )
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        AsyncMock(return_value={101: {7}, 102: {7}}),
    )

    candidates = await _provider_set_coverage(frozenset({7, 8}))

    assert candidates == serving._GraphLocationCandidates(
        [_location(101, source_exhausted=True)],
        {101: {7}},
    )


@pytest.mark.asyncio
async def test_coverage_grows_to_a_later_rate_set_witness(monkeypatch):
    """Continue past an absent prefix until later eligibility is proven."""

    location_reads = AsyncMock(
        side_effect=(
            [_location(101, source_exhausted=False)],
            [
                _location(101, source_exhausted=False),
                _location(102, source_exhausted=False),
            ],
        )
    )
    monkeypatch.setattr(serving, "_membership_location_rows", location_reads)
    monkeypatch.setattr(
        serving,
        "_shared_provider_set_keys_by_npi",
        AsyncMock(side_effect=({101: {7}}, {102: {7, 8}})),
    )
    monkeypatch.setattr(
        serving,
        "_graph_location_probe_batch_size",
        lambda *_args, **_kwargs: 1,
    )

    candidates = await _provider_set_coverage(frozenset({7, 8}))

    assert candidates == serving._GraphLocationCandidates(
        [
            _location(101, source_exhausted=False),
            _location(102, source_exhausted=False),
        ],
        {101: {7}, 102: {8}},
    )
    assert location_reads.await_count == 2


@pytest.mark.asyncio
async def test_coverage_projection_skips_provider_enrichment(monkeypatch):
    """Aggregate rate searches need set IDs, not provider/address payloads."""

    provider_set_id = "07" * 16
    monkeypatch.setattr(
        serving,
        "_provider_set_ids_for_keys",
        AsyncMock(return_value={7: provider_set_id}),
    )
    enrichment = AsyncMock()
    monkeypatch.setattr(
        serving,
        "_enriched_provider_rows_for_npis",
        enrichment,
    )

    projected = await serving._project_graph_candidates(
        object(),
        strict_v3_tables(),
        serving._GraphLocationCandidates(
            [_location(101, source_exhausted=False)],
            {101: {7}},
        ),
        plan_id="synthetic-plan",
        snapshot_id="ptg2:synthetic",
        source_key="synthetic-source",
        include_provider_rows=False,
    )

    assert projected == ({provider_set_id}, {})
    enrichment.assert_not_awaited()


async def _provider_set_coverage(
    rate_provider_set_keys: frozenset[int],
) -> serving._GraphLocationCandidates | None:
    return await serving._paged_graph_candidates(
        object(),
        strict_v3_tables(),
        {},
        rate_provider_set_keys,
        3,
        require_provider_set_coverage=True,
    )
