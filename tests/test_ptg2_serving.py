# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from api import ptg2_serving
from api.code_systems import (
    canonical_catalog_code,
    catalog_code_lookup_values,
    catalog_code_system_lookup_values,
    equivalent_external_procedure_pairs,
)
from process.ptg_parts.address_assurance import summarize_ptg_price_address_payload
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


@pytest.fixture(autouse=True)
def clear_network_serving_tables_cache():
    """Keep process-local sealed-snapshot metadata isolated between tests."""

    ptg2_serving._PTG2_NETWORK_SERVING_TABLES_CACHE.clear()
    yield
    ptg2_serving._PTG2_NETWORK_SERVING_TABLES_CACHE.clear()


class FakeResult:
    def __init__(self, scalar=None, result_rows=None):
        self._scalar = scalar
        self._rows = list(result_rows or [])

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def one_or_none(self):
        return self.first()

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results=()):
        self._results = list(results)
        self.calls = []
        self.rollback_count = 0

    async def execute(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        value = self._results.pop(0) if self._results else FakeResult()
        if isinstance(value, Exception):
            raise value
        if isinstance(value, FakeResult):
            return value
        return FakeResult(scalar=value)

    async def rollback(self):
        self.rollback_count += 1


class FakePagination:
    limit = 25
    offset = 0


class FilteredProviderExpansionHarness:
    """Provide bounded provider-membership fakes for expansion tests."""

    def __init__(self, provider_set_id, rate_rows, member_npis, matching_npis):
        self.provider_set_id = provider_set_id
        self.rate_rows = rate_rows
        self.member_npis = member_npis
        self.matching_npis = matching_npis
        self.membership_limits = []

    async def merge_rates(self, *_args, provider_set_keys, **_kwargs):
        assert provider_set_keys is None or tuple(provider_set_keys) == (1,)
        return self.rate_rows

    async def provider_npis(
        self,
        _session,
        _tables,
        provider_set_global_ids,
        *,
        limit_per_set,
    ):
        self.membership_limits.append(limit_per_set)
        return {
            provider_set_id: self.member_npis[:limit_per_set]
            for provider_set_id in provider_set_global_ids
        }

    async def filter_npis(self, _session, args, npis, *, limit):
        assert args["provider_sex_code"] == "F"
        return tuple(npi for npi in npis if npi in self.matching_npis)[:limit]

    async def reverse_sets(self, _session, _tables, npis):
        return {npi: (self.provider_set_id,) for npi in npis}

    async def provider_set_keys(self, _session, _tables, provider_set_ids):
        return {provider_set_id: 1 for provider_set_id in provider_set_ids}

    async def provider_rows(self, *_args, npis, **_kwargs):
        return {
            self.provider_set_id: [
                {"npi": npi, "provider_name": f"Provider {npi}"}
                for npi in npis
            ]
        }

    def install(self, monkeypatch):
        patch_values_by_name = {
            "_merge_manifest_code_variant_rows": self.merge_rates,
            "_provider_npis_for_sets": self.provider_npis,
            "_filter_npis_by_taxonomy": self.filter_npis,
            "_provider_set_ids_for_selected_npis": self.reverse_sets,
            "_provider_set_keys_for_ids": self.provider_set_keys,
            "_selected_provider_rows_by_set": self.provider_rows,
        }
        for function_name, replacement in patch_values_by_name.items():
            monkeypatch.setattr(ptg2_serving, function_name, replacement)


class ConcurrentSessionFactory:
    def __init__(self):
        self.active = 0
        self.maximum_active = 0
        self.sessions = []

    @asynccontextmanager
    async def session(self):
        network_session = object()
        self.sessions.append(network_session)
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            yield network_session
        finally:
            self.active -= 1


def _strict_v3_tables(**overrides):
    table_values_by_field = {
        "snapshot_id": "ptg2:209901:synthetic",
        "arch_version": "postgres_binary_v3",
        "storage": "manifest_snapshot",
        "shared_snapshot_key": 41,
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "serving_table_layout": "lean_provider_key_v1",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "network_names": ["Synthetic Network"],
        "source_key": "synthetic-source",
    }
    table_values_by_field.update(overrides)
    return ptg2_serving.PTG2ServingTables(**table_values_by_field)


def _compact_item(**overrides):
    compact_row_by_field = {
        "npi": 1234567890,
        "provider_name": "Example Clinician",
        "reported_code": "70551",
        "reported_code_system": "CPT",
        "network_names": ["Synthetic Network"],
        "location_source": "npi_address",
        "address_payload": {
            "first_line": "100 Example Street",
            "city": "Example City",
            "state": "IL",
            "postal_code": "60001",
            "address_sources": ["nppes"],
        },
        "prices": [],
    }
    compact_row_by_field.update(overrides)
    return ptg2_serving._compact_item_from_row(compact_row_by_field, {})


def test_shared_forward_window_follows_dense_cost_rank_not_provider_count():
    forward_rows = [
        SimpleNamespace(
            provider_set_key=2,
            provider_count=10_000,
            price_key=1,
            source_key=0,
        ),
        SimpleNamespace(
            provider_set_key=1,
            provider_count=1,
            price_key=0,
            source_key=0,
        ),
    ]

    ascending = ptg2_serving._shared_forward_row_window(
        forward_rows,
        {1: "01", 2: "02"},
        limit=2,
        offset=0,
        descending=False,
    )
    descending = ptg2_serving._shared_forward_row_window(
        forward_rows,
        {1: "01", 2: "02"},
        limit=2,
        offset=0,
        descending=True,
    )

    assert [forward_row.price_key for forward_row in ascending] == [0, 1]
    assert [forward_row.price_key for forward_row in descending] == [1, 0]


def test_provider_expansion_candidate_window_includes_deep_offset():
    pagination = SimpleNamespace(limit=25, offset=100)

    assert ptg2_serving._ptg2_manifest_rate_candidate_limit(
        {},
        pagination,
        expand_providers=True,
        location_filter_requested=False,
    ) == 125


def test_location_provider_window_includes_has_more_sentinel(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_FLOOR", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER",
        "1",
    )
    pagination = SimpleNamespace(limit=25, offset=100)

    assert ptg2_serving._ptg2_manifest_rate_candidate_limit(
        {},
        pagination,
        expand_providers=True,
        location_filter_requested=True,
    ) == 126


def test_location_rate_window_without_provider_expansion_includes_sentinel():
    pagination = SimpleNamespace(limit=25, offset=100)

    assert ptg2_serving._ptg2_manifest_rate_candidate_limit(
        {},
        pagination,
        expand_providers=False,
        location_filter_requested=True,
    ) == 126


@pytest.mark.asyncio
async def test_location_provider_window_fails_closed_above_exactness_bound(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", "100")

    with pytest.raises(
        PTG2ManifestArtifactError,
        match="configured exactness bound",
    ):
        await ptg2_serving._ptg2_manifest_location_provider_matches(
            object(),
            _strict_v3_tables(),
            {},
            candidate_limit=101,
        )


def test_provider_expansion_prefix_continues_past_duplicate_cheapest_rows():
    rate_rows = [
        {
            "provider_set_global_id_128": "01" * 16,
            "serving_content_hash_128": f"{index + 1:032x}",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "source_key": 0,
        }
        for index in range(20)
    ]
    rate_rows.append(
        {
            "provider_set_global_id_128": "02" * 16,
            "serving_content_hash_128": "ff" * 16,
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "source_key": 0,
        }
    )

    first_rank, _, _ = ptg2_serving._rank_provider_expansion_prefix(
        rate_rows[:20],
        {"01" * 16: (1000000001, 1000000002)},
        target_count=5,
    )
    complete_rank, selected_npis, _ = ptg2_serving._rank_provider_expansion_prefix(
        rate_rows,
        {
            "01" * 16: (1000000001, 1000000002),
            "02" * 16: (1000000003, 1000000004, 1000000005),
        },
        target_count=5,
    )

    assert len(first_rank) == 2
    assert len(complete_rank) == 5
    assert selected_npis == (
        1000000001,
        1000000002,
        1000000003,
        1000000004,
        1000000005,
    )


def test_provider_expansion_prefix_preserves_graph_member_ordinal():
    provider_row_by_field = {
        "provider_set_global_id_128": "01" * 16,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "source_key": 0,
    }

    rank_by_key, selected_npis, _ = ptg2_serving._rank_provider_expansion_prefix(
        [provider_row_by_field],
        {"01" * 16: (1000000009, 1000000001, 1000000005)},
        target_count=3,
    )

    assert selected_npis == (1000000009, 1000000001, 1000000005)
    assert list(rank_by_key.values()) == [0, 1, 2]


@pytest.mark.parametrize("target_count", range(1, 13))
def test_provider_expansion_prefix_matches_exhaustive_overlap_order(target_count):
    """Verify provider expansion matches exhaustive overlap order at each target size."""
    first_set = "01" * 16
    second_set = "02" * 16
    third_set = "03" * 16
    rate_rows = [
        {
            "provider_set_global_id_128": first_set,
            "serving_content_hash_128": "11" * 16,
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "ffs",
            "source_key": 0,
        },
        {
            "provider_set_global_id_128": second_set,
            "serving_content_hash_128": "22" * 16,
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "ffs",
            "source_key": 0,
        },
        {
            "provider_set_global_id_128": third_set,
            "serving_content_hash_128": "33" * 16,
            "reported_code_system": "CPT",
            "reported_code": "99214",
            "negotiation_arrangement": "ffs",
            "source_key": 1,
        },
    ]
    npis_by_set = {
        first_set: (1000000007, 1000000001, 1000000003),
        second_set: (1000000001, 1000000005, 1000000007),
        third_set: (1000000005, 1000000009),
    }
    exhaustive_keys = []
    exhaustive_provider_set_ids = []
    for rate_row in rate_rows:
        for npi in npis_by_set[rate_row["provider_set_global_id_128"]]:
            key = (
                "npi",
                str(npi),
                rate_row["reported_code_system"],
                rate_row["reported_code"],
                rate_row["negotiation_arrangement"],
                str(rate_row["source_key"]),
            )
            if key not in exhaustive_keys:
                exhaustive_keys.append(key)
                if (
                    rate_row["provider_set_global_id_128"]
                    not in exhaustive_provider_set_ids
                ):
                    exhaustive_provider_set_ids.append(
                        rate_row["provider_set_global_id_128"]
                    )
            if len(exhaustive_keys) >= target_count:
                break
        if len(exhaustive_keys) >= target_count:
            break

    rank_by_key, selected_npis, selected_provider_set_ids = (
        ptg2_serving._rank_provider_expansion_prefix(
            rate_rows,
            npis_by_set,
            target_count=target_count,
        )
    )

    expected_keys = exhaustive_keys[:target_count]
    assert list(rank_by_key) == expected_keys
    assert list(rank_by_key.values()) == list(range(len(expected_keys)))
    assert selected_npis == tuple(
        dict.fromkeys(int(key[1]) for key in expected_keys)
    )
    assert selected_provider_set_ids == tuple(exhaustive_provider_set_ids)


def test_descending_forward_rank_keeps_immutable_ties_ascending():
    rate_rows = [
        SimpleNamespace(
            provider_set_key=2,
            provider_count=1,
            price_key=7,
            source_key=0,
        ),
        SimpleNamespace(
            provider_set_key=1,
            provider_count=1,
            price_key=7,
            source_key=0,
        ),
    ]

    descending = ptg2_serving._shared_forward_row_window(
        rate_rows,
        {1: "01", 2: "02"},
        limit=2,
        offset=0,
        descending=True,
    )

    assert [row.provider_set_key for row in descending] == [1, 2]


@pytest.mark.asyncio
async def test_strict_cost_provider_selection_grows_until_page_is_contained(
    monkeypatch,
):
    """Verify cost selection widens until the requested provider page is complete."""
    first_set = "01" * 16
    second_set = "02" * 16
    rate_rows = [
        {
            "provider_set_global_id_128": first_set if index < 64 else second_set,
            "serving_content_hash_128": f"{index + 1:032x}",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "source_key": 0,
        }
        for index in range(130)
    ]
    prefix_limits = []
    completion_filters = []

    async def fake_merge(*_args, limit, provider_set_keys, **_kwargs):
        if provider_set_keys is None:
            prefix_limits.append(limit)
            return rate_rows[:limit]
        completion_filters.append(tuple(sorted(provider_set_keys)))
        return rate_rows

    async def fake_npis(_session, _tables, provider_set_global_ids, **_kwargs):
        return {
            provider_set_id: (
                (1000000001, 1000000002)
                if provider_set_id == first_set
                else (1000000003, 1000000004, 1000000005)
            )
            for provider_set_id in provider_set_global_ids
        }

    async def fake_reverse(_session, _tables, npis):
        return {
            npi: (first_set,) if npi < 1000000003 else (second_set,)
            for npi in npis
        }

    async def fake_set_keys(_session, _tables, provider_set_ids):
        return {
            provider_set_id: 1 if provider_set_id == first_set else 2
            for provider_set_id in provider_set_ids
        }

    async def fake_provider_rows(*_args, npis, provider_set_ids_by_npi, **_kwargs):
        provider_sets = {
            provider_set_id
            for npi in npis
            for provider_set_id in provider_set_ids_by_npi[npi]
        }
        return {
            provider_set_id: [
                {"npi": npi, "provider_name": f"Provider {npi}"}
                for npi in npis
                if provider_set_id in provider_set_ids_by_npi[npi]
            ]
            for provider_set_id in provider_sets
        }

    monkeypatch.setattr(ptg2_serving, "_merge_manifest_code_variant_rows", fake_merge)
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_npis_for_sets",
        fake_npis,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_ids_for_selected_npis",
        fake_reverse,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_keys_for_ids",
        fake_set_keys,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_selected_provider_rows_by_set",
        fake_provider_rows,
    )

    selection = await ptg2_serving._strict_cost_provider_expansion_selection(
        object(),
        SimpleNamespace(source_key="synthetic-source"),
        code_rows=[{"code_key": 7, "rate_count": 130}],
        args={"plan_id": "synthetic-plan"},
        snapshot_id="synthetic-snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=5,
        descending=False,
    )

    assert selection is not None
    assert prefix_limits == [64, 130]
    assert completion_filters == [(1, 2)]
    assert selection.total_lower_bound == 5
    assert selection.exhausted is False


@pytest.mark.asyncio
async def test_provider_npi_prefix_cache_reuses_and_grows_sealed_membership(
    monkeypatch,
):
    """Reuse bounded provider-set membership and grow only beyond cached prefixes."""

    provider_set_id = "01" * 16
    member_npis = tuple(range(1000000001, 1000000051))
    membership_calls = []

    async def load_member_ids(
        _session,
        _tables,
        provider_set_ids,
        *,
        limit_per_set,
    ):
        membership_calls.append((tuple(provider_set_ids), limit_per_set))
        return {
            selected_provider_set_id: tuple(
                ptg2_serving._ptg2_npi_member_id(npi)
                for npi in member_npis[:limit_per_set]
            )
            for selected_provider_set_id in provider_set_ids
        }

    monkeypatch.setattr(
        ptg2_serving,
        "_provider_npi_member_ids_by_set",
        load_member_ids,
    )
    ptg2_serving._PTG2_PROVIDER_NPI_PREFIX_CACHE.clear()
    serving_tables = _strict_v3_tables(shared_snapshot_key=91)
    try:
        first_prefix = (
            await ptg2_serving._provider_npis_for_sets(
                object(),
                serving_tables,
                (provider_set_id,),
                limit_per_set=32,
            )
        )[provider_set_id]
        cached_short_prefix = (
            await ptg2_serving._provider_npis_for_sets(
                object(),
                serving_tables,
                (provider_set_id,),
                limit_per_set=5,
            )
        )[provider_set_id]
        complete_prefix = (
            await ptg2_serving._provider_npis_for_sets(
                object(),
                serving_tables,
                (provider_set_id,),
                limit_per_set=64,
            )
        )[provider_set_id]
        cached_complete_prefix = (
            await ptg2_serving._provider_npis_for_sets(
                object(),
                serving_tables,
                (provider_set_id,),
                limit_per_set=128,
            )
        )[provider_set_id]
    finally:
        ptg2_serving._PTG2_PROVIDER_NPI_PREFIX_CACHE.clear()

    assert first_prefix == member_npis[:32]
    assert cached_short_prefix == member_npis[:5]
    assert complete_prefix == member_npis
    assert cached_complete_prefix == member_npis
    assert membership_calls == [
        ((provider_set_id,), 32),
        ((provider_set_id,), 64),
    ]


@pytest.mark.asyncio
async def test_filtered_provider_prefix_cache_reuses_identical_filter(
    monkeypatch,
):
    """Reuse a sealed provider-set prefix for an identical provider filter."""

    provider_set_id = "02" * 16
    member_npis = tuple(range(1000000001, 1000000033))
    membership_calls = []
    filter_calls = []

    async def provider_npis(
        _session,
        _tables,
        provider_set_ids,
        *,
        limit_per_set,
    ):
        membership_calls.append((tuple(provider_set_ids), limit_per_set))
        return {
            selected_provider_set_id: member_npis[:limit_per_set]
            for selected_provider_set_id in provider_set_ids
        }

    async def filter_npis(_session, args_by_name, npis, *, limit):
        filter_calls.append(
            (args_by_name["provider_sex_code"], tuple(npis), limit)
        )
        return tuple(npi for npi in npis if npi % 2 == 0)[:limit]

    monkeypatch.setattr(
        ptg2_serving,
        "_provider_npis_for_sets",
        provider_npis,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_filter_npis_by_taxonomy",
        filter_npis,
    )
    ptg2_serving._PTG2_FILTERED_PROVIDER_PREFIX_CACHE.clear()
    serving_tables = _strict_v3_tables(shared_snapshot_key=92)
    args_by_name = {
        "plan_id": "synthetic-plan",
        "provider_sex_code": "F",
    }
    try:
        first_prefix = await ptg2_serving._filtered_provider_npis_for_expansion_set(
            object(),
            serving_tables,
            provider_set_id,
            args_by_name,
            target_count=2,
        )
        cached_prefix = await ptg2_serving._filtered_provider_npis_for_expansion_set(
            object(),
            serving_tables,
            provider_set_id,
            args_by_name,
            target_count=2,
        )
    finally:
        ptg2_serving._PTG2_FILTERED_PROVIDER_PREFIX_CACHE.clear()

    assert first_prefix == member_npis[1:4:2]
    assert cached_prefix == first_prefix
    assert membership_calls == [((provider_set_id,), 32)]
    assert len(filter_calls) == 1


@pytest.mark.asyncio
async def test_strict_cost_provider_selection_bounds_demographic_filter_expansion(
    monkeypatch,
):
    """Verify demographic filtering grows membership without full expansion."""

    provider_set_id = "01" * 16
    rate_rows = [
        {
            "provider_set_global_id_128": provider_set_id,
            "serving_content_hash_128": "11" * 16,
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "source_key": 0,
        }
    ]
    member_npis = tuple(range(1000000001, 1000000051))
    female_npis = member_npis[-2:]
    harness = FilteredProviderExpansionHarness(
        provider_set_id,
        rate_rows,
        member_npis,
        female_npis,
    )
    harness.install(monkeypatch)
    serving_tables = _strict_v3_tables(
        shared_snapshot_key=93,
        source_key="synthetic-source",
    )
    selection_args_by_name = {
        "code_rows": [{"code_key": 7, "rate_count": 1}],
        "args": {
            "plan_id": "synthetic-plan",
            "provider_sex_code": "F",
        },
        "snapshot_id": "synthetic-snapshot",
        "source_trace_set_hash": None,
        "network_names": [],
        "target_count": 2,
        "descending": False,
    }
    ptg2_serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()
    try:
        selection = (
            await ptg2_serving._strict_cost_provider_expansion_selection(
                object(),
                serving_tables,
                **selection_args_by_name,
            )
        )
        cached_selection = (
            await ptg2_serving._strict_cost_provider_expansion_selection(
                object(),
                serving_tables,
                **selection_args_by_name,
            )
        )
    finally:
        ptg2_serving._PTG2_PROVIDER_EXPANSION_SELECTION_CACHE.clear()

    assert selection is not None
    assert cached_selection is not None
    assert harness.membership_limits == [32, 64]
    assert list(selection.rank_by_key) == [
        ("npi", str(npi), "CPT", "99213", "FFS", "0")
        for npi in female_npis
    ]
    assert cached_selection.rank_by_key == selection.rank_by_key
    assert [
        provider["npi"]
        for provider in selection.providers_by_set[provider_set_id]
    ] == list(female_npis)

# Strict shared V3 serving contract


@pytest.mark.asyncio
async def test_taxonomy_enrichment_is_optional_when_reference_tables_are_absent(
    monkeypatch,
):
    session = FakeSession()

    async def is_relation_available(_session, _table_name):
        return False

    monkeypatch.setattr(ptg2_serving, "_is_relation_available", is_relation_available)

    assert await ptg2_serving._taxonomy_rows_for_npis(
        session,
        [1234567890],
    ) == {}
    assert session.calls == []


def test_serving_tables_only_accept_complete_shared_v3_contract():
    assert _strict_v3_tables().uses_shared_blocks is True
    assert _strict_v3_tables(storage_generation="snapshot_binary_v3").uses_shared_blocks is False
    assert _strict_v3_tables(shared_snapshot_key=None).uses_shared_blocks is False


def test_strict_shared_v3_guard_rejects_legacy_serving_contracts():
    with pytest.raises(PTG2ManifestArtifactError, match="strict shared-block contract"):
        ptg2_serving._require_strict_shared_v3(
            ptg2_serving.PTG2ServingTables(
                arch_version="postgres_binary_v3",
                storage="manifest_snapshot",
            )
        )


@pytest.mark.asyncio
async def test_strict_shared_v3_search_scopes_code_lookup_to_shared_snapshot():
    session = FakeSession([FakeResult(result_rows=[])])

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "ptg2:209901:synthetic",
        {"plan_id": "SYNTHETIC-PLAN", "code": "70551", "code_system": "CPT"},
        FakePagination(),
        _strict_v3_tables(),
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert response is None
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "mrf.ptg2_v3_code" in sql
    assert "mrf.ptg2_v3_snapshot_scope" in sql
    assert "code_metadata.snapshot_key = :shared_snapshot_key" in sql
    assert params_by_name["logical_snapshot_id"] == "ptg2:209901:synthetic"
    assert params_by_name["shared_snapshot_key"] == 41


@pytest.mark.asyncio
async def test_strict_shared_v3_search_matches_persisted_code_system_aliases():
    session = FakeSession([FakeResult(result_rows=[])])

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "ptg2:209901:synthetic",
        {
            "plan_id": "SYNTHETIC-PLAN",
            "code": "0297",
            "code_system": "MS_DRG",
        },
        FakePagination(),
        _strict_v3_tables(),
        ptg2_serving.PTG2_MODE_EXACT_SOURCE,
    )

    assert response is None
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "code_metadata.reported_code_system IN" in sql
    assert {
        value
        for key, value in params_by_name.items()
        if key.startswith("reported_code_system")
    } == {"MS_DRG", "MS-DRG", "MSDRG", "DRG"}


@pytest.mark.asyncio
async def test_strict_shared_v3_reverse_lookup_matches_revenue_code_forms():
    session = FakeSession(
        [
            FakeResult(
                result_rows=[
                    {
                        "code_key": 7,
                        "plan_id": "SYNTHETIC-PLAN",
                        "plan_market_type": "group",
                        "reported_code_system": "RC",
                        "reported_code": "110",
                        "rate_count": 1,
                    }
                ]
            )
        ]
    )

    code_rows = await ptg2_serving._manifest_reverse_code_rows(
        session,
        _strict_v3_tables(),
        requested_plan="SYNTHETIC-PLAN",
        code_value="110",
        code_system="RC",
        q_text="",
        code_context=None,
        plan_market_type="group",
    )

    assert code_rows[0]["reported_code"] == "110"
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "mrf.ptg2_v3_code" in sql
    assert "mrf.ptg2_v3_snapshot_scope" in sql
    assert "code_metadata.snapshot_key = :shared_snapshot_key" in sql
    assert "reported_code = :reported_code_0" in sql
    assert "reported_code = :reported_code_1" in sql
    assert params_by_name["reported_code_0"] == "0110"
    assert params_by_name["reported_code_1"] == "110"
    assert params_by_name["shared_snapshot_key"] == 41


# Code normalization and crosswalk context


def test_revenue_code_lookup_values_include_raw_and_canonical_forms():
    assert catalog_code_lookup_values("RC", "110") == ("0110", "110")
    assert catalog_code_lookup_values("revenue_code", "0450") == ("0450", "450")
    assert catalog_code_lookup_values("RC", "020") == ("0020", "020", "20")
    assert catalog_code_lookup_values("CPT", "99213") == ("99213",)


def test_code_system_lookup_values_include_legacy_persisted_aliases():
    assert catalog_code_system_lookup_values("MS-DRG") == (
        "MS_DRG",
        "MS-DRG",
        "MSDRG",
        "DRG",
    )
    assert catalog_code_system_lookup_values("CPT") == ("CPT",)


def test_code_lookup_boundaries_cover_empty_width_and_equivalent_forms():
    assert catalog_code_system_lookup_values(None) == ()
    assert canonical_catalog_code("ICD10CM", " a.12 ") == "A12"
    assert catalog_code_lookup_values(None, None) == ()
    assert equivalent_external_procedure_pairs("UNKNOWN", "12345") == set()
    assert equivalent_external_procedure_pairs("CPT", "1234") == set()
    assert equivalent_external_procedure_pairs("CPT", "12-45") == set()
    assert equivalent_external_procedure_pairs("CPT", "12345") == {
        ("CPT", "12345"),
        ("HCPCS", "12345"),
    }
    assert equivalent_external_procedure_pairs("CDT", "D1234") == {
        ("CDT", "D1234"),
        ("HCPCS", "D1234"),
    }
    assert equivalent_external_procedure_pairs("CPT", "A1234") == {
        ("CPT", "A1234"),
    }


def test_code_metadata_rows_expose_canonical_code_systems():
    assert ptg2_serving._canonical_code_metadata_row(
        {
            "reported_code_system": "MS-DRG",
            "reported_code": "0297",
        }
    ) == {
        "reported_code_system": "MS_DRG",
        "reported_code": "0297",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "code_system", "expected_pairs", "unexpected_pair"),
    [
        (
            "70551",
            "CPT",
            {("CPT", "70551"), ("HCPCS", "70551")},
            ("CDT", "70551"),
        ),
        (
            "D0120",
            "CDT",
            {("CDT", "D0120"), ("HCPCS", "D0120")},
            ("CPT", "D0120"),
        ),
    ],
)
async def test_code_context_expands_equivalent_external_codes(
    code,
    code_system,
    expected_pairs,
    unexpected_pair,
):
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(result_rows=[])]),
        code=code,
        code_system=code_system,
    )

    resolved_pairs = {
        (item["code_system"], item["code"]) for item in context["resolved_codes"]
    }
    assert expected_pairs <= resolved_pairs
    assert unexpected_pair not in resolved_pairs


@pytest.mark.asyncio
async def test_code_context_canonicalizes_drg_code():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(result_rows=[])]),
        code="47",
        code_system="DRG",
    )

    assert context["input_code"] == {"code_system": "MS_DRG", "code": "047"}
    assert context["resolved_codes"] == [{"code_system": "MS_DRG", "code": "047"}]


@pytest.mark.asyncio
async def test_code_context_expands_internal_crosswalk():
    session = FakeSession(
        [
            FakeResult(
                result_rows=[
                    {
                        "from_system": "CPT",
                        "from_code": "70551",
                        "to_system": "HP_PROCEDURE_CODE",
                        "to_code": "123456",
                        "match_type": "exact",
                        "confidence": 1.0,
                        "source": "synthetic-crosswalk",
                    }
                ]
            ),
            FakeResult(result_rows=[]),
        ]
    )

    context = await ptg2_serving._resolve_ptg2_code_search_context(
        session,
        code="70551",
        code_system="CPT",
    )

    assert context["internal_codes"] == [123456]
    assert {
        "code_system": "HP_PROCEDURE_CODE",
        "code": "123456",
    } in context["resolved_codes"]
    assert context["matched_via"][0]["source"] == "synthetic-crosswalk"


def test_manifest_code_filter_matches_revenue_code_forms():
    filters = []
    params_by_name = {}

    ptg2_serving._append_manifest_reported_code_filter(
        filters,
        params_by_name,
        code="0110",
        code_system="RC",
    )

    assert "reported_code_system = :reported_code_system_0" in filters[0]
    assert "reported_code = :reported_code_0" in filters[0]
    assert "reported_code = :reported_code_1" in filters[0]
    assert params_by_name["reported_code_system_0"] == "RC"
    assert params_by_name["reported_code_0"] == "0110"
    assert params_by_name["reported_code_1"] == "110"


def test_manifest_code_filter_uses_resolved_external_context():
    filters = []
    params_by_name = {}

    ptg2_serving._append_manifest_reported_code_filter(
        filters,
        params_by_name,
        code="70551",
        code_system="CPT",
        code_context={
            "resolved_codes": [
                {"code_system": "CPT", "code": "70551"},
                {"code_system": "HCPCS", "code": "70551"},
                {"code_system": "HP_PROCEDURE_CODE", "code": "123456"},
            ]
        },
    )

    assert set(value for key, value in params_by_name.items() if "system" in key) == {
        "CPT",
        "HCPCS",
    }
    assert set(
        value
        for key, value in params_by_name.items()
        if key.startswith("reported_code_") and "system" not in key
    ) == {"70551"}


# Taxonomy and geo SQL used by strict shared V3 provider expansion


@pytest.mark.asyncio
async def test_taxonomy_filter_uses_primary_specialty_codes():
    session = FakeSession([FakeResult(result_rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._filter_npis_by_taxonomy(
        session,
        {"specialty": "Family Medicine"},
        [1234567890, 1234567891],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "manifest_provider_specialty_nt.npi = source_npis.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert params_by_name["manifest_provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params_by_name["manifest_provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_provider_filter_applies_provider_sex_before_limit():
    session = FakeSession([FakeResult(result_rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._filter_npis_by_taxonomy(
        session,
        {"provider_sex_code": "F"},
        [1234567890, 1234567891],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "mrf.npi manifest_provider_sex_npi" in sql
    assert "manifest_provider_sex_npi.npi = source_npis.npi" in sql
    assert params_by_name["manifest_provider_sex_provider_sex_code"] == "F"
    assert "LIMIT :limit" in sql


@pytest.mark.asyncio
async def test_inferred_taxonomy_filter_requires_individual_npi():
    session = FakeSession([FakeResult(result_rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._filter_npis_by_taxonomy(
        session,
        {"code": "29888", "code_system": "CPT"},
        [1234567890, 1234567891],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert "FROM mrf.npi_taxonomy nt WHERE nt.npi = source_npis.npi" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207X00000X" in str(params_by_name)


@pytest.mark.parametrize(
    ("code", "expected_taxonomy"),
    [
        ("00100", "207L00000X"),
        ("80053", "291U00000X"),
        ("97140", "225100000X"),
        ("66984", "207W00000X"),
        ("45378", "207RG0100X"),
        ("99285", "207P00000X"),
        ("77301", "2085R0001X"),
    ],
)
def test_inferred_taxonomy_sql_for_high_confidence_cpt_families(
    code,
    expected_taxonomy,
):
    params_by_name = {}
    sql = ptg2_serving._inferred_provider_taxonomy_code_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        schema="mrf",
        params=params_by_name,
        param_prefix="inferred",
    )

    assert expected_taxonomy in params_by_name.values()
    assert "nt.healthcare_provider_taxonomy_code IN" in sql


@pytest.mark.parametrize("code", ["99213", "99203", "93000"])
def test_inferred_taxonomy_sql_ignores_mixed_use_cpt_families(code):
    sql = ptg2_serving._inferred_provider_taxonomy_code_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        schema="mrf",
        params={},
        param_prefix="inferred",
    )

    assert sql == ""


@pytest.mark.parametrize(
    "request_scope",
    [
        {"mode": "exact_source"},
        {"npi": 1053910794},
    ],
)
def test_inferred_taxonomy_sql_does_not_override_exact_provider_evidence(
    request_scope,
):
    sql = ptg2_serving._inferred_provider_taxonomy_code_sql(
        {
            "code": "23670",
            "code_system": "CPT",
            **request_scope,
        },
        nt_alias="nt",
        schema="mrf",
        params={},
        param_prefix="inferred",
    )

    assert sql == ""


def test_exact_source_taxonomy_filters_preserve_explicit_specialty():
    params_by_name = {}

    filters = ptg2_serving._membership_taxonomy_filters(
        {
            "mode": "exact_source",
            "specialty": "Family Medicine",
            "code": "23670",
            "code_system": "CPT",
            "npi": 1053910794,
        },
        params_by_name,
    )

    assert len(filters) == 1
    assert "207Q00000X" in params_by_name.values()
    assert "207X00000X" not in params_by_name.values()


def test_membership_taxonomy_filters_use_uncorrelated_semijoins():
    params_by_name = {}

    filters = ptg2_serving._membership_taxonomy_filters(
        {
            "specialty": "Family Medicine",
            "code": "29888",
            "code_system": "CPT",
        },
        params_by_name,
    )

    sql = " ".join(filters)
    assert "addr.npi IN (SELECT" in sql
    assert "FROM mrf.npi_taxonomy" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207Q00000X" in params_by_name.values()
    assert "207X00000X" in params_by_name.values()


def test_membership_geo_sql_uses_postgis_for_unified_addresses():
    params_by_name = {}

    distance_sql, filters = ptg2_serving._membership_geo_sql(
        {"lat": "41.90", "long": "-87.65", "radius_miles": "12"},
        uses_unified_addresses=True,
        parameter_map=params_by_name,
    )

    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql
    assert any("ST_DWithin" in sql for sql in filters)
    assert "COALESCE(addr.address_precision, '') <> 'city_zip'" in filters
    assert params_by_name["geo_radius_miles"] == 12.0


def test_membership_geo_sql_uses_bounded_non_unified_predicates():
    params_by_name = {}

    distance_sql, filters = ptg2_serving._membership_geo_sql(
        {"lat": "41.90", "long": "-87.65", "radius_miles": "12"},
        uses_unified_addresses=False,
        parameter_map=params_by_name,
    )

    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql
    assert "addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in filters
    assert "addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in filters
    assert any(":geo_radius_miles" in sql for sql in filters)


def test_membership_filter_preserves_zip_or_radius_semantics():
    params_by_name = {"address_types": ["practice", "primary"]}

    filter_sql, distance_sql = ptg2_serving._membership_filter_sql(
        {
            "zip5": "60601",
            "lat": "41.90",
            "long": "-87.65",
            "radius_miles": "10",
            "npi": "1234567890",
        },
        candidate_npis=(1234567890, 1234567891),
        uses_unified_addresses=True,
        address_zip5_sql="LEFT(addr.postal_code, 5)",
        parameter_map=params_by_name,
    )

    assert "LEFT(addr.postal_code, 5) = :zip5 OR" in filter_sql
    assert "ST_DWithin" in filter_sql
    assert "addr.npi = ANY(CAST(:candidate_npis AS bigint[]))" in filter_sql
    assert "addr.npi = :provider_npi" in filter_sql
    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql


def test_knn_order_requires_unscoped_first_page_coordinate_search():
    query_by_name = {"lat": "41.90", "long": "-87.65"}

    assert ptg2_serving._membership_knn_order_sql(
        query_by_name,
        candidate_npis=None,
        uses_unified_addresses=True,
        offset=0,
    ) == ptg2_serving._ptg2_geo_knn_meters_sql("addr.lat", "addr.long")
    assert (
        ptg2_serving._membership_knn_order_sql(
            {**query_by_name, "zip5": "60601"},
            candidate_npis=None,
            uses_unified_addresses=True,
            offset=0,
        )
        is None
    )
    assert (
        ptg2_serving._membership_knn_order_sql(
            query_by_name,
            candidate_npis=(1234567890,),
            uses_unified_addresses=True,
            offset=0,
        )
        is None
    )


def test_knn_query_carries_bounded_raw_probe_exhaustion_marker():
    assert "LIMIT :raw_probe_limit" in ptg2_serving._MEMBERSHIP_LOCATION_KNN_SQL
    assert "_ptg_source_exhausted" in ptg2_serving._MEMBERSHIP_LOCATION_KNN_SQL
    assert "probe_stats.raw_probe_count < :raw_probe_limit" in (
        ptg2_serving._MEMBERSHIP_LOCATION_KNN_SQL
    )


@pytest.mark.asyncio
async def test_knn_duplicate_addresses_do_not_signal_location_exhaustion(monkeypatch):
    location_calls = []
    first_prefix_rows = [
        {"npi": 1000000001, "_ptg_source_exhausted": False},
    ]
    complete_prefix_rows = [
        {"npi": 1000000001, "_ptg_source_exhausted": True},
        {"npi": 1000000002, "_ptg_source_exhausted": True},
    ]

    async def fake_location_rows(*_args, limit, **_kwargs):
        location_calls.append(limit)
        return first_prefix_rows if len(location_calls) == 1 else complete_prefix_rows

    async def fake_append(
        _session,
        _tables,
        _rate_scope,
        candidate_rows,
        matched_rows,
        groups_by_npi,
        seen_npis,
    ):
        before = len(matched_rows)
        for candidate_row in candidate_rows:
            npi = int(candidate_row["npi"])
            if npi in seen_npis:
                continue
            seen_npis.add(npi)
            matched_rows.append(candidate_row)
            groups_by_npi[npi].add("01" * 16)
        return len(matched_rows) - before

    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", fake_location_rows)
    monkeypatch.setattr(ptg2_serving, "_append_rate_matched_locations", fake_append)

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        _strict_v3_tables(),
        {"lat": 41.9, "long": -87.65},
        ptg2_serving._ptg2_build_rate_scope(("01" * 16,)),
        2,
    )

    assert candidates is not None
    assert [candidate_row["npi"] for candidate_row in candidates.location_rows] == [
        1000000001,
        1000000002,
    ]
    assert len(location_calls) == 2


# Provider-directory and address policy


@pytest.mark.asyncio
async def test_provider_directory_overlay_prefers_corroborated_contact():
    """Verify corroborated directory contacts override weaker provider context."""
    address_key = "00000000-0000-0000-0000-000000000001"
    session = FakeSession(
        [
            "mrf.provider_directory_address_corroboration",
            FakeResult(
                result_rows=[
                    {
                        "npi": 1234567890,
                        "address_key": address_key,
                        "source_key": "synthetic-source",
                        "snapshot_id": "ptg2:209901:synthetic",
                        "plan_id": "SYNTHETIC-PLAN",
                        "ptg_plan_id": "synthetic-ptg-plan",
                        "provider_directory_source_id": "directory-fixture",
                        "provider_directory_org_name": "Synthetic Health",
                        "provider_directory_plan_name": "Synthetic Directory Plan",
                        "provider_directory_location_resource_id": "location-1",
                        "provider_directory_location_name": "Example Clinic",
                        "provider_directory_telephone_number": "312-555-0100",
                        "provider_directory_phone_number": "3125550100",
                        "provider_directory_phone_extension": "45",
                        "provider_directory_network_refs": ["Organization/network-1"],
                        "provider_directory_network_names": ["Synthetic Network"],
                        "provider_directory_network_matches": [],
                        "provider_directory_plan_context_matched": True,
                        "provider_directory_network_context_present": True,
                        "provider_directory_insurance_plan_matches": [],
                        "provider_directory_match_type": "npi_address_plan",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_plan"
                        },
                    }
                ]
            ),
        ]
    )
    provider_rows = [
        {
            "npi": 1234567890,
            "network_names": ["Synthetic Network"],
            "address_payload": {
                "address_key": address_key,
                "first_line": "100 Example Street",
                "city": "Example City",
                "state": "IL",
                "postal_code": "60001",
            },
        }
    ]

    overlaid = await ptg2_serving._overlay_provider_directory_corroboration(
        session,
        provider_rows,
        plan_id="SYNTHETIC-PLAN",
        snapshot_id="ptg2:209901:synthetic",
        source_key="synthetic-source",
    )
    provider_by_field = ptg2_serving._compact_item_from_row(
        {**overlaid[0], "prices": []},
        {},
    )

    assert provider_by_field["phone_number"] == "3125550100"
    assert provider_by_field["phone_extension"] == "45"
    assert provider_by_field["address_verification"]["address_evidence_level"] == (
        "payer_directory_network_location"
    )
    assert provider_by_field["address_verification"]["provider_directory_org_name"] == (
        "Synthetic Health"
    )


def test_provider_directory_network_name_match_uses_synthetic_context_without_mutation():
    address_by_field = {
        "first_line": "100 Example Street",
        "city": "Example City",
        "state": "IL",
        "postal_code": "60001",
        "address_sources": ["provider_directory_fhir"],
        "provider_directory_source_id": "directory-fixture",
        "provider_directory_org_name": "Synthetic Health",
        "provider_directory_plan_name": "Synthetic Directory Plan",
        "provider_directory_plan_context_matched": False,
        "provider_directory_network_context_present": True,
        "provider_directory_network_matches": [
            {
                "ref": "Organization/network-1",
                "resource_id": "network-1",
                "name": "Synthetic Network",
            }
        ],
        "address_verification_evidence": {
            "matched_on": "npi_address_key_role_location"
        },
    }
    original = copy.deepcopy(address_by_field)
    provider_by_field = {
        "network_names": ["Synthetic Network"],
        "address": {
            "first_line": "100 Example Street",
            "city": "Example City",
            "state": "IL",
            "postal_code": "60001",
        },
    }

    first = ptg2_serving._address_verification_payload(
        provider_by_field,
        {},
        address_by_field,
    )
    second = ptg2_serving._address_verification_payload(
        provider_by_field,
        {},
        address_by_field,
    )

    assert address_by_field == original
    assert first["address_network_binding"] == "payer_directory_corroborated_location"
    assert first["provider_directory_network_name_matched"] is True
    assert first["provider_directory_network_matches"] == second[
        "provider_directory_network_matches"
    ]
    match = first["provider_directory_network_matches"][0]
    assert match["provider_directory_org_name"] == "Synthetic Health"
    assert match["provider_directory_issuer_key"] == "synthetichealth"


def test_provider_directory_marker_without_plan_or_network_match_is_inferred():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "payer_directory_corroborated_location",
            "address_payload": {
                "first_line": "100 Example Street",
                "city": "Example City",
                "state": "IL",
                "postal_code": "60001",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location"
                },
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["requires_location_confirmation"] is True


def test_compact_item_normalizes_provider_directory_boolean_and_list_fields():
    provider_by_field = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "network_names": ["Synthetic Network"],
            "location_source": "provider_directory_fhir",
            "address_payload": {
                "first_line": "100 Example Street",
                "city": "Example City",
                "state": "IL",
                "postal_code": "60001",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": "false",
                "provider_directory_network_context_present": "true",
                "provider_directory_network_refs": '["Organization/network-1"]',
                "provider_directory_network_names": ["Synthetic Network"],
                "provider_directory_network_matches": [
                    {"name": "Synthetic Network", "resource_id": "network-1"}
                ],
                "provider_directory_insurance_plan_refs": "InsurancePlan/plan-1",
                "provider_directory_insurance_plan_matches": '["InsurancePlan/plan-1"]',
            },
            "prices": [],
        },
        {},
    )

    verification = provider_by_field["address_verification"]
    assert verification["provider_directory_plan_context_matched"] is False
    assert verification["provider_directory_network_context_present"] is True
    assert verification["provider_directory_network_refs"] == ["Organization/network-1"]
    assert verification["provider_directory_insurance_plan_refs"] == [
        "InsurancePlan/plan-1"
    ]


def test_exact_source_item_preserves_null_and_trimmed_empty_procedure_metadata():
    null_item = ptg2_serving._compact_item_from_row(
        {
            "prices": [],
            "billing_code_type_version": None,
            "procedure_name": None,
            "procedure_description": None,
            "network_names": [],
        },
        {"mode": "exact_source"},
    )
    assert null_item["billing_code_type_version"] is None
    assert null_item["procedure_name"] is None
    assert null_item["procedure_description"] is None
    assert null_item["network_names"] == []

    empty_item = ptg2_serving._compact_item_from_row(
        {
            "prices": [],
            "billing_code_type_version": "",
            "procedure_name": "",
            "procedure_description": "",
            "network_names": [],
        },
        {"mode": "exact_source"},
    )
    assert empty_item["billing_code_type_version"] == ""
    assert empty_item["procedure_name"] == ""
    assert empty_item["procedure_description"] == ""

    product_item = ptg2_serving._compact_item_from_row(
        {"prices": [], "network_names": []},
        {"mode": "product_search"},
    )
    assert "billing_code_type_version" not in product_item
    assert "procedure_name" not in product_item
    assert "procedure_description" not in product_item


def test_compact_item_promotes_and_canonicalizes_contact_fields():
    item = _compact_item(
        address_payload={
            "first_line": "100 Example Street",
            "city": "Example City",
            "state": "IL",
            "postal_code": "60001",
            "country_code": "US",
            "telephone_number": "312-555-0100 ext 45",
            "fax_number": "312-555-0101",
            "address_sources": ["nppes"],
        }
    )

    assert item["telephone_number"] == "312-555-0100 ext 45"
    assert item["phone_number"] == "3125550100"
    assert item["phone_extension"] == "45"
    assert item["fax_number_digits"] == "3125550101"


def test_compact_item_without_displayable_address_strips_location_details():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "telephone_number": "312-555-0100",
            },
            "distance_miles": 2.3,
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    for key in ("address", "address_key", "phone", "phone_number", "distance_miles"):
        assert key not in item
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is False


def test_compact_item_city_state_fallback_is_displayable_but_unconfirmed():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "address_payload": {
                "city": "Example City",
                "state": "IL",
                "address_precision": "city_zip",
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_evidence_level"] == "city_zip_fallback"
    assert verification["displayed_address_present"] is True
    assert verification["requires_location_confirmation"] is True


def test_plan_request_can_suppress_unverified_address_and_contact_fields():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Clinician",
            "location_source": "npi_address",
            "address_payload": {
                "first_line": "100 Example Street",
                "city": "Example City",
                "state": "IL",
                "postal_code": "60001",
                "telephone_number": "312-555-0100",
                "address_sources": ["nppes"],
            },
            "prices": [],
        },
        {"plan_id": "SYNTHETIC-PLAN", "include_unverified_addresses": "false"},
    )

    assert item["address_verification"]["displayed_address_present"] is False
    assert "address" not in item
    assert "phone" not in item


# Public item shaping, merging, and ordering


def test_manifest_response_keeps_address_verification_in_public_shape():
    shaped = ptg2_serving._shape_ptg2_manifest_response(
        {
            "items": [
                {
                    "provider_name": "Example Clinician",
                    "service_code": "70551",
                    "service_code_system": "CPT",
                    "network_names": ["Synthetic Network"],
                    "source_trace": [{"source_file_version_id": "synthetic-version"}],
                    "address_verification": {
                        "address_network_binding": "payer_directory_corroborated_location"
                    },
                }
            ],
            "query": {
                "source": "ptg2_db",
                "source_key": "synthetic-source",
                "snapshot_id": "ptg2:209901:synthetic",
                "result_granularity": "provider",
            },
        },
        {},
        database_evidence={
            "contract": "postgresql_session_v1",
            "server_version_num": 160004,
            "database_selected": True,
            "backend_session_active": True,
            "transaction_snapshot_observed": True,
        },
    )

    provider_by_field = shaped["items"][0]
    assert "service_code" not in provider_by_field
    assert "service_code_system" not in provider_by_field
    assert "network_names" not in provider_by_field
    assert "source_trace" not in provider_by_field
    assert provider_by_field["address_verification"]["address_network_binding"] == (
        "payer_directory_corroborated_location"
    )
    assert "result_granularity" not in shaped["query"]
    assert shaped["provenance"]["source_key"] == "synthetic-source"
    assert shaped["provenance"]["database_evidence"][
        "server_version_num"
    ] == 160004


def test_manifest_provider_procedure_item_shapes_address_and_prices():
    provider_by_field = ptg2_serving._ptg2_manifest_provider_procedure_item(
        npi=1234567890,
        serving_data={
            "serving_content_hash_128": "rate-pack",
            "provider_set_global_id_128": "provider-set",
            "provider_count": 3,
            "price_set_global_id_128": "price-set",
            "reported_code": "29888",
            "reported_code_system": "CPT",
            "network_names": ["Synthetic Network"],
        },
        prices=[
            {
                "negotiated_type": "negotiated",
                "negotiated_rate": "1138.57",
                "billing_class": "professional",
            }
        ],
        procedure_detail={
            "procedure_name": "Example procedure",
            "procedure_description": "Synthetic procedure description",
        },
        provider_context={
            "provider_name": "Example Clinician",
            "location_source": "npi_address",
            "address_payload": {
                "first_line": "100 Example Street",
                "city": "Example City",
                "state": "IL",
                "postal_code": "60001",
                "address_sources": ["nppes"],
            },
        },
        args={"snapshot_id": "ptg2:209901:synthetic"},
    )

    assert provider_by_field["npi"] == 1234567890
    assert provider_by_field["procedure_code"] == "29888"
    assert provider_by_field["prices"][0]["negotiated_rate"] == 1138.57
    assert provider_by_field["address"]["first_line"] == "100 Example Street"
    assert provider_by_field["address_verification"]["address_evidence_level"] == (
        "nppes_provider_address"
    )


def test_provider_rate_items_merge_duplicate_location_and_code_prices():
    base_by_field = {
        "npi": 1234567890,
        "provider_name": "Example Clinician",
        "location_hash": "synthetic-location",
        "reported_code": "70551",
        "reported_code_system": "CPT",
        "address": {"first_line": "100 Example Street"},
    }
    rate_items = [
        {
            **base_by_field,
            "provider_set_hash": "provider-set-1",
            "price_set_hash": "price-set-1",
            "rate_pack_hash": "rate-pack-1",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
        },
        {
            **base_by_field,
            "provider_set_hash": "provider-set-2",
            "price_set_hash": "price-set-2",
            "rate_pack_hash": "rate-pack-2",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 200}],
        },
    ]

    merged = ptg2_serving._merge_ptg2_provider_rate_items(rate_items)

    assert len(merged) == 1
    assert {price["negotiated_rate"] for price in merged[0]["prices"]} == {100, 200}
    assert merged[0]["provider_set_hashes"] == ["provider-set-1", "provider-set-2"]
    assert merged[0]["price_set_count"] == 2
    assert merged[0]["rate_pack_count"] == 2


def test_price_merge_preserves_occurrences():
    base_by_field = {
        "npi": 1234567890,
        "location_hash": "synthetic-location",
        "reported_code": "70551",
        "reported_code_system": "CPT",
        "address": {"first_line": "100 Example Street"},
    }
    first_prices = [
        {"negotiated_type": "negotiated", "negotiated_rate": 100},
        {"negotiated_type": "negotiated", "negotiated_rate": 100},
    ]
    second_prices = [
        {"negotiated_type": "negotiated", "negotiated_rate": 200},
    ]
    rate_items = [
        {
            **base_by_field,
            "price_set_hash": "price-set-1",
            "price_set_hashes": ["price-set-1"],
            "source_trace": [{"source_key": 0}],
            **ptg2_serving._price_response_fields(first_prices),
        },
        {
            **base_by_field,
            "price_set_hash": "price-set-2",
            **ptg2_serving._price_response_fields(second_prices),
        },
    ]
    original_price_lists = [list(rate_item["prices"]) for rate_item in rate_items]
    original_hashes = list(rate_items[0]["price_set_hashes"])
    original_source_traces = list(rate_items[0]["source_trace"])

    merged = ptg2_serving._merge_ptg2_provider_rate_items(rate_items)

    assert [price["negotiated_rate"] for price in merged[0]["prices"]] == [
        100,
        100,
        200,
    ]
    assert [rate_item["prices"] for rate_item in rate_items] == original_price_lists
    assert rate_items[0]["price_set_hashes"] == original_hashes
    assert rate_items[0]["source_trace"] == original_source_traces
    assert merged[0]["price_set_hashes"] is not rate_items[0]["price_set_hashes"]
    assert merged[0]["source_trace"] is not rate_items[0]["source_trace"]
    assert merged[0]["prices"] is not rate_items[0]["prices"]


def test_provider_rate_items_do_not_merge_different_arrangements():
    base_by_field = {
        "npi": 1234567890,
        "provider_name": "Example Clinician",
        "location_hash": "synthetic-location",
        "reported_code": "70551",
        "reported_code_system": "CPT",
        "address": {"first_line": "100 Example Street"},
        "provider_set_hash": "provider-set-1",
        "price_set_hash": "price-set-1",
        "rate_pack_hash": "rate-pack-1",
    }
    rate_items = [
        {
            **base_by_field,
            "negotiation_arrangement": "FFS",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
        },
        {
            **base_by_field,
            "negotiation_arrangement": "BUNDLE",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 200}],
        },
    ]

    merged = ptg2_serving._merge_ptg2_provider_rate_items(rate_items)

    assert len(merged) == 2
    assert {
        (
            rate_item["negotiation_arrangement"],
            rate_item["prices"][0]["negotiated_rate"],
        )
        for rate_item in merged
    } == {("FFS", 100), ("BUNDLE", 200)}


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    (
        ("billing_code_type_version", "2026"),
        ("source_procedure_name", "Updated source label"),
        ("source_procedure_description", "Updated source description"),
        ("network_names", ["Second Network"]),
    ),
)
def test_provider_rate_items_do_not_merge_distinct_source_code_variants(
    changed_field,
    changed_value,
):
    base_by_field = {
        "npi": 1234567890,
        "provider_name": "Example Clinician",
        "location_hash": "synthetic-location",
        "reported_code": "38222",
        "reported_code_system": "CPT",
        "negotiation_arrangement": "FFS",
        "billing_code_type_version": "2025",
        "source_procedure_name": "Source label",
        "source_procedure_description": "Source description",
        "network_names": ["First Network"],
        "source_artifact_key": 0,
        "address": {"first_line": "100 Example Street"},
        "provider_set_hash": "provider-set-1",
        "price_set_hash": "price-set-1",
        "rate_pack_hash": "rate-pack-1",
        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
    }
    changed_variant_by_field = {
        **base_by_field,
        changed_field: changed_value,
        "price_set_hash": "price-set-2",
        "rate_pack_hash": "rate-pack-2",
    }

    merged = ptg2_serving._merge_ptg2_provider_rate_items(
        [base_by_field, changed_variant_by_field]
    )

    assert len(merged) == 2


def test_provider_search_never_caps_rate_rows_before_merge():
    assert (
        ptg2_serving._ptg2_manifest_serving_row_limit(
            {"npi": "1234567890"},
            200,
            expand_providers=True,
        )
        is None
    )
    assert (
        ptg2_serving._ptg2_manifest_serving_row_limit(
            {"npi": "1234567890"},
            200,
            expand_providers=False,
        )
        == 200
    )
    assert (
        ptg2_serving._ptg2_manifest_serving_row_limit(
            {},
            200,
            expand_providers=True,
        )
        is None
    )


def test_default_provider_sort_prioritizes_network_bound_address():
    provider_items = [
        {
            "provider_name": "No Address",
            "prices": [],
            "address_verification": {
                "displayed_address_present": False,
                "network_bound_address": False,
            },
        },
        {
            "provider_name": "Inferred Address",
            "prices": [],
            "address_verification": {
                "displayed_address_present": True,
                "network_bound_address": False,
            },
        },
        {
            "provider_name": "Verified Address",
            "prices": [],
            "address_verification": {
                "displayed_address_present": True,
                "network_bound_address": True,
            },
        },
    ]

    sorted_items = ptg2_serving._sort_ptg2_manifest_provider_items(
        provider_items,
        {},
        location_filter_requested=False,
    )

    assert [provider_by_field["provider_name"] for provider_by_field in sorted_items] == [
        "Verified Address",
        "Inferred Address",
        "No Address",
    ]


def test_provider_sort_supports_cost_and_distance():
    provider_items = [
        {
            "provider_name": "Far Low Cost",
            "distance_miles": 8,
            "prices": [{"negotiated_rate": 100}],
        },
        {
            "provider_name": "Near High Cost",
            "distance_miles": 2,
            "prices": [{"negotiated_rate": 200}],
        },
    ]

    by_cost = ptg2_serving._sort_ptg2_manifest_provider_items(
        provider_items,
        {"order_by": "cost"},
        location_filter_requested=True,
    )
    by_distance = ptg2_serving._sort_ptg2_manifest_provider_items(
        provider_items,
        {"order_by": "distance"},
        location_filter_requested=True,
    )

    assert [provider_by_field["provider_name"] for provider_by_field in by_cost] == [
        "Far Low Cost",
        "Near High Cost",
    ]
    assert [provider_by_field["provider_name"] for provider_by_field in by_distance] == [
        "Near High Cost",
        "Far Low Cost",
    ]


def test_provider_cost_sort_preserves_exact_decimal_order():
    items = [
        {
            "provider_name": "A Higher Exact Rate",
            "prices": [{"negotiated_rate": "9007199254740992.2"}],
        },
        {
            "provider_name": "Z Lower Exact Rate",
            "prices": [{"negotiated_rate": "9007199254740992.1"}],
        },
    ]

    by_cost = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "cost"},
        location_filter_requested=False,
    )

    assert [item["provider_name"] for item in by_cost] == [
        "Z Lower Exact Rate",
        "A Higher Exact Rate",
    ]


def test_provider_cost_sort_parses_exponent_tokens_without_float_rounding():
    items = [
        {
            "provider_name": "A Higher Exponent Rate",
            "prices": [{"negotiated_rate": "1.0000000000000000001e20"}],
        },
        {
            "provider_name": "Z Lower Exponent Rate",
            "prices": [{"negotiated_rate": "1e20"}],
        },
    ]

    by_cost = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "cost"},
        location_filter_requested=False,
    )

    assert [item["provider_name"] for item in by_cost] == [
        "Z Lower Exponent Rate",
        "A Higher Exponent Rate",
    ]


def test_descending_provider_cost_sort_keeps_missing_last_and_ties_ascending():
    provider_items = [
        {
            "provider_name": "B",
            "npi": 2,
            "_ptg_price_key": 7,
            "prices": [{"negotiated_rate": "10"}],
        },
        {
            "provider_name": "A",
            "npi": 1,
            "_ptg_price_key": 7,
            "prices": [{"negotiated_rate": "10"}],
        },
        {
            "provider_name": "Missing",
            "npi": 3,
            "_ptg_price_key": None,
            "prices": [],
        },
    ]

    sorted_items = ptg2_serving._sort_ptg2_manifest_provider_items(
        provider_items,
        {"order_by": "rate", "order": "desc"},
        location_filter_requested=False,
    )

    assert [provider_by_field["provider_name"] for provider_by_field in sorted_items] == [
        "A",
        "B",
        "Missing",
    ]


def test_distance_order_uses_npi_before_price_at_equal_distance():
    items = [
        {
            "provider_name": "Lower price, later NPI",
            "npi": 1000000002,
            "distance_miles": 3.0,
            "prices": [{"negotiated_rate": "1"}],
        },
        {
            "provider_name": "Higher price, earlier NPI",
            "npi": 1000000001,
            "distance_miles": 3.0,
            "prices": [{"negotiated_rate": "100"}],
        },
    ]

    sorted_items = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "distance", "order": "asc"},
        location_filter_requested=True,
    )

    assert [item["npi"] for item in sorted_items] == [1000000001, 1000000002]


def test_price_filters_preserve_only_matching_atoms():
    prices = [
        {
            "negotiated_rate": "125.00",
            "service_code": ["11"],
            "billing_code_modifier": ["25"],
        },
        {
            "negotiated_rate": "250.00",
            "service_code": ["22"],
            "billing_code_modifier": [],
        },
    ]

    assert ptg2_serving._ptg2_manifest_filter_prices(
        prices,
        {
            "pos": "11",
            "modifier": "25",
            "negotiated_rate": "125",
        },
    ) == [prices[0]]


@pytest.mark.asyncio
async def test_reverse_price_filter_scans_past_old_candidate_guess(monkeypatch):
    """Verify reverse price filtering scans beyond the former candidate guess."""
    nonmatching_rows = [
        {
            "price_set_global_id_128": f"{price_key:032x}",
            "price_key": price_key,
        }
        for price_key in range(1, 502)
    ]
    matching_row_by_field = {
        "price_set_global_id_128": f"{502:032x}",
        "price_key": 502,
    }
    batch_offsets = []

    async def fake_reverse_scope(*_args, **_kwargs):
        return ptg2_serving._VersionThreeReverseScope(
            provider_set_id_by_key={1: "01" * 16},
            candidate_code_keys=(1, 2),
        )

    async def fake_candidate_batch(
        _session,
        _tables,
        _query,
        _scope,
        metadata_offset,
        _metadata_batch_size,
    ):
        batch_offsets.append(metadata_offset)
        if metadata_offset == 0:
            return [nonmatching_rows], 128
        if metadata_offset == 128:
            return [[matching_row_by_field]], 1
        raise AssertionError(f"unexpected metadata offset {metadata_offset}")

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        return {
            price_set_id: [
                {
                    "negotiated_rate": (
                        "999" if int(price_set_id, 16) == 502 else "1"
                    )
                }
            ]
            for price_set_id in price_set_ids
        }

    monkeypatch.setattr(ptg2_serving, "_version_three_reverse_scope", fake_reverse_scope)
    monkeypatch.setattr(ptg2_serving, "_version_three_candidate_batch", fake_candidate_batch)
    monkeypatch.setattr(ptg2_serving, "_prices_for_price_sets", fake_prices)

    selection = await ptg2_serving._version_three_filtered_reverse_selection(
        object(),
        _strict_v3_tables(),
        ptg2_serving._VersionThreeReverseQuery(
            provider_set_ids=("01" * 16,),
            requested_plan="TEST-PLAN-001",
            code_value="",
            code_system=None,
            q_text="",
            code_context=None,
            source_trace_set_hash=None,
            network_names=[],
            limit=None,
            offset=0,
            apply_window=False,
        ),
        {"negotiated_rate": "999"},
        offset=0,
        limit=2,
    )

    assert [selected_row["price_key"] for selected_row in selection.rows] == [502]
    assert selection.exhausted
    assert selection.total_row_count == 1
    assert selection.matched_rows_seen == 1
    assert batch_offsets == [0, 128]


@pytest.mark.asyncio
async def test_geo_price_filter_selects_locations_from_matching_provider_sets(monkeypatch):
    """Verify geo filtering selects locations from price-matching provider sets."""
    first_provider_set_id = "01" * 16
    matching_provider_set_id = "02" * 16
    first_price_set_id = "03" * 16
    matching_price_set_id = "04" * 16
    serving_rows = [
        {
            "serving_content_hash_128": "05" * 16,
            "plan_id": "TEST-PLAN-001",
            "plan_market_type": "group",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "provider_set_global_id_128": first_provider_set_id,
            "_ptg_provider_set_key": 3,
            "provider_count": 1,
            "price_set_global_id_128": first_price_set_id,
            "price_key": 0,
            "source_key": 0,
            "network_names": [],
        },
        {
            "serving_content_hash_128": "06" * 16,
            "plan_id": "TEST-PLAN-001",
            "plan_market_type": "group",
            "reported_code_system": "CPT",
            "reported_code": "99213",
            "negotiation_arrangement": "FFS",
            "provider_set_global_id_128": matching_provider_set_id,
            "_ptg_provider_set_key": 4,
            "provider_count": 1,
            "price_set_global_id_128": matching_price_set_id,
            "price_key": 1,
            "source_key": 0,
            "network_names": [],
        },
    ]
    location_call_by_field = {}

    async def fake_merge(*_args, **_kwargs):
        return list(serving_rows)

    async def fake_noop(*_args, **_kwargs):
        return None

    async def fake_prices(*_args, **_kwargs):
        return {
            first_price_set_id: [
                {"negotiated_rate": "10", "service_code": ["11"]}
            ],
            matching_price_set_id: [
                {"negotiated_rate": "20", "service_code": ["22"]}
            ],
        }

    async def fake_location(*_args, **kwargs):
        location_call_by_field.update(kwargs)
        return (
            {matching_provider_set_id},
            {
                matching_provider_set_id: [
                    {
                        "npi": 1234567890,
                        "provider_name": "Synthetic Clinician",
                        "distance_miles": 2.0,
                        "address_payload": {},
                    }
                ]
            },
        )

    async def fake_details(*_args, **_kwargs):
        return {}

    monkeypatch.setattr(ptg2_serving, "_merge_manifest_code_variant_rows", fake_merge)
    monkeypatch.setattr(ptg2_serving, "_hydrate_provider_set_network_names", fake_noop)
    monkeypatch.setattr(ptg2_serving, "_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        fake_location,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        fake_details,
    )
    session = FakeSession(
        [
            FakeResult(
                result_rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TEST-PLAN-001",
                        "plan_market_type": "group",
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "negotiation_arrangement": "FFS",
                        "rate_count": 2,
                    }
                ]
            )
        ]
    )

    response = await ptg2_serving._search_manifest_serving_table(
        session,
        "ptg2:209901:synthetic",
        {
            "plan_id": "TEST-PLAN-001",
            "plan_market_type": "group",
            "code_system": "CPT",
            "code": "99213",
            "state": "IL",
            "pos": "22",
            "include_providers": True,
        },
        SimpleNamespace(limit=25, offset=0),
        _strict_v3_tables(),
        "product_search",
    )

    assert location_call_by_field["provider_set_keys"] == {4}
    assert location_call_by_field["require_exhaustive"] is False
    assert [provider_by_field["npi"] for provider_by_field in response["items"]] == [
        1234567890
    ]
    assert response["items"][0]["prices"] == [
        {"negotiated_rate": 20, "service_code": ["22"]}
    ]
    assert response["pagination"]["has_more"] is False
    assert response["pagination"]["total_is_exact"] is True


@pytest.mark.asyncio
async def test_geo_cost_order_requires_exhaustive_location_selection(monkeypatch):
    location_call_by_field = {}

    async def fake_location(*_args, **kwargs):
        location_call_by_field.update(kwargs)
        return set(), {}

    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        fake_location,
    )

    response = await ptg2_serving._search_manifest_serving_table(
        object(),
        "ptg2:209901:synthetic",
        {
            "plan_id": "TEST-PLAN-001",
            "code_system": "CPT",
            "code": "99213",
            "state": "IL",
            "include_providers": True,
            "order_by": "negotiated_rate",
        },
        SimpleNamespace(limit=25, offset=0),
        _strict_v3_tables(),
        "product_search",
    )

    assert response["items"] == []
    assert location_call_by_field["require_exhaustive"] is True


@pytest.mark.asyncio
async def test_provider_reverse_response_uses_page_sentinel_and_honest_total(monkeypatch):
    """Verify reverse pagination uses a sentinel and reports a lower-bound total."""
    reverse_rows = tuple(
        {
            "reported_code_system": "CPT",
            "reported_code": f"{99210 + index}",
            "provider_set_global_id_128": "01" * 16,
            "price_set_global_id_128": f"{index + 1:032x}",
            "price_key": index,
            "source_key": 0,
        }
        for index in range(3)
    )

    async def fake_provider_sets(*_args, **_kwargs):
        return ("01" * 16,)

    async def fake_code_context(*_args, **_kwargs):
        return None

    async def fake_reverse_selection(*_args, **_kwargs):
        query = _args[-1]
        assert query.limit == 3
        assert query.offset == 0
        return ptg2_serving._VersionThreeReverseSelection(
            rows=reverse_rows,
            exhausted=False,
        )

    async def fake_noop(*_args, **_kwargs):
        return None

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        return {
            price_set_id: [{"negotiated_rate": str(index + 1)}]
            for index, price_set_id in enumerate(price_set_ids)
        }

    async def fake_details(*_args, **_kwargs):
        return {}

    async def fake_provider_context(*_args, **_kwargs):
        return []

    monkeypatch.setattr(
        ptg2_serving,
        "_provider_sets_for_npi",
        fake_provider_sets,
    )
    monkeypatch.setattr(ptg2_serving, "_resolve_ptg2_code_search_context", fake_code_context)
    monkeypatch.setattr(ptg2_serving, "_version_three_reverse_selection", fake_reverse_selection)
    monkeypatch.setattr(ptg2_serving, "_hydrate_provider_set_network_names", fake_noop)
    monkeypatch.setattr(ptg2_serving, "_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(
        ptg2_serving,
        "_procedure_details_for_rows",
        fake_details,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_enriched_provider_rows_for_npis",
        fake_provider_context,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_procedure_item",
        lambda **kwargs: {
            "reported_code": kwargs["serving_data"]["reported_code"],
            "prices": kwargs["prices"],
        },
    )

    response = await ptg2_serving._search_ptg2_manifest_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "TEST-PLAN-001"},
        SimpleNamespace(limit=2, offset=0),
        snapshot_id="ptg2:209901:synthetic",
        serving_tables=_strict_v3_tables(),
    )

    assert [
        procedure_item["reported_code"] for procedure_item in response["items"]
    ] == ["99210", "99211"]
    assert response["pagination"] == {
        "total": 3,
        "limit": 2,
        "offset": 0,
        "page": 1,
        "has_more": True,
        "total_is_exact": False,
        "total_lower_bound": 3,
    }


@pytest.mark.asyncio
async def test_network_serving_tables_cache_is_revalidated_and_reloads_on_mismatch(
    monkeypatch,
):
    load_calls = []
    validation_calls = []
    is_cached_tables_current = True

    async def fake_snapshot_tables(_session, snapshot_id):
        load_calls.append(snapshot_id)
        return _strict_v3_tables(snapshot_id=snapshot_id)

    async def is_cached_tables_valid(_session, serving_tables_by_snapshot_id):
        validation_calls.append(tuple(serving_tables_by_snapshot_id))
        return is_cached_tables_current

    monkeypatch.setattr(
        ptg2_serving,
        "snapshot_serving_tables",
        fake_snapshot_tables,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_is_cached_network_serving_tables_current",
        is_cached_tables_valid,
    )
    network_snapshots = [
        ("network-a", "snapshot-a"),
        ("network-b", "snapshot-b"),
    ]

    first = await ptg2_serving._network_tables_by_snapshot_id(
        object(),
        network_snapshots,
    )
    second = await ptg2_serving._network_tables_by_snapshot_id(
        object(),
        network_snapshots,
    )
    is_cached_tables_current = False
    third = await ptg2_serving._network_tables_by_snapshot_id(
        object(),
        network_snapshots,
    )

    assert tuple(first) == ("snapshot-a", "snapshot-b")
    assert second == first
    assert third == first
    assert load_calls == [
        "snapshot-a",
        "snapshot-b",
        "snapshot-a",
        "snapshot-b",
    ]
    assert validation_calls == [
        (),
        ("snapshot-a", "snapshot-b"),
        ("snapshot-a", "snapshot-b"),
    ]


@pytest.mark.asyncio
async def test_network_descriptor_revalidation_accepts_supported_attestations():
    class Session:
        def __init__(self):
            self.calls = []

        async def execute(self, statement, params):
            self.calls.append((str(statement), dict(params)))
            return []

    session = Session()
    is_current = await ptg2_serving._is_cached_network_serving_tables_current(
        session,
        {"snapshot-a": _strict_v3_tables(snapshot_id="snapshot-a")},
    )

    assert is_current is False
    sql, params = session.calls[0]
    assert "attestation.contract = ANY(" in sql
    assert params["attestation_contracts"] == list(
        ptg2_serving.PTG2_CANDIDATE_ATTESTATION_SUPPORTED_CONTRACTS
    )


@pytest.mark.asyncio
async def test_multi_network_forward_reads_share_request_session_sequentially(
    monkeypatch,
):
    request_session = object()
    search_sessions = []

    async def fake_snapshot_tables(_session, snapshot_id):
        assert _session is request_session
        return _strict_v3_tables(snapshot_id=snapshot_id)

    async def has_fake_plan_code(*_args, **_kwargs):
        return True

    async def fake_search(_session, snapshot_id, _args, _pagination, **_kwargs):
        search_sessions.append(_session)
        await asyncio.sleep(0.01)
        return {
            "items": [{"provider_name": snapshot_id, "prices": []}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot_tables)
    monkeypatch.setattr(ptg2_serving, "_has_snapshot_plan_code", has_fake_plan_code)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_search)

    merged_response = await ptg2_serving._search_multi_ptg2_snapshots(
        request_session,
        [("network-a", "snapshot-a"), ("network-b", "snapshot-b")],
        {"plan_id": "TEST-PLAN-001"},
        FakePagination(),
    )

    assert search_sessions == [request_session, request_session]
    assert merged_response["query"]["snapshots"] == ["snapshot-a", "snapshot-b"]
    assert {network_item["network"] for network_item in merged_response["items"]} == {
        "network-a",
        "network-b",
    }


@pytest.mark.asyncio
async def test_multi_network_forward_failure_never_returns_partial_union(monkeypatch):
    request_session = object()
    searched_snapshot_ids = []

    async def fake_snapshot_tables(_session, snapshot_id):
        assert _session is request_session
        return _strict_v3_tables(snapshot_id=snapshot_id)

    async def has_fake_plan_code(*_args, **_kwargs):
        return True

    async def fake_search(_session, snapshot_id, _args, _pagination, **_kwargs):
        assert _session is request_session
        searched_snapshot_ids.append(snapshot_id)
        await asyncio.sleep(0.01)
        if snapshot_id == "snapshot-b":
            raise RuntimeError("network read failed")
        return {"items": [], "pagination": {"total": 0}, "query": {}}

    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot_tables)
    monkeypatch.setattr(ptg2_serving, "_has_snapshot_plan_code", has_fake_plan_code)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_search)

    with pytest.raises(RuntimeError, match="network read failed"):
        await ptg2_serving._search_multi_ptg2_snapshots(
            request_session,
            [("network-a", "snapshot-a"), ("network-b", "snapshot-b")],
            {"plan_id": "TEST-PLAN-001"},
            FakePagination(),
        )

    assert searched_snapshot_ids == ["snapshot-a", "snapshot-b"]


@pytest.mark.asyncio
async def test_multi_network_reverse_reads_use_independent_concurrent_sessions(monkeypatch):
    session_factory = ConcurrentSessionFactory()

    async def fake_search(_session, _npi, _args, _pagination, *, snapshot_id):
        await asyncio.sleep(0.01)
        assert _pagination.limit == 26
        return {
            "items": [
                {
                    "reported_code_system": "CPT",
                    "reported_code": snapshot_id[-1],
                    "provider_count": 1,
                }
            ],
            "pagination": {
                "total": 1,
                "total_lower_bound": 1,
                "total_is_exact": True,
                "has_more": False,
            },
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving.sa_db, "session", session_factory.session)
    monkeypatch.setattr(
        ptg2_serving,
        "_search_ptg2_provider_procedures_snapshot",
        fake_search,
    )

    merged_response = await ptg2_serving._search_multi_ptg2_provider_procedures(
        object(),
        1234567890,
        [("network-a", "snapshot-a"), ("network-b", "snapshot-b")],
        {"plan_id": "TEST-PLAN-001"},
        FakePagination(),
    )

    assert session_factory.maximum_active == 2
    assert len({id(session) for session in session_factory.sessions}) == 2
    assert merged_response["query"]["snapshots"] == ["snapshot-a", "snapshot-b"]
    assert merged_response["pagination"] == {
        "total": 2,
        "limit": 25,
        "offset": 0,
        "page": 1,
        "has_more": False,
        "total_is_exact": True,
        "total_lower_bound": 2,
    }
    assert {network_item["network"] for network_item in merged_response["items"]} == {
        "network-a",
        "network-b",
    }
