# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from api import ptg2_serving
from api.code_systems import catalog_code_lookup_values
from process.ptg_parts.address_assurance import summarize_ptg_price_address_payload
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


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
    values = {
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
    values.update(overrides)
    return ptg2_serving.PTG2ServingTables(**values)


def _compact_item(**overrides):
    row = {
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
    row.update(overrides)
    return ptg2_serving._compact_item_from_row(row, {})


def test_shared_forward_window_follows_dense_cost_rank_not_provider_count():
    rows = [
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
        rows,
        {1: "01", 2: "02"},
        limit=2,
        offset=0,
        descending=False,
    )
    descending = ptg2_serving._shared_forward_row_window(
        rows,
        {1: "01", 2: "02"},
        limit=2,
        offset=0,
        descending=True,
    )

    assert [row.price_key for row in ascending] == [0, 1]
    assert [row.price_key for row in descending] == [1, 0]


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
    rows = [
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
    rows.append(
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
        rows[:20],
        {"01" * 16: (1000000001, 1000000002)},
        target_count=5,
    )
    complete_rank, selected_npis, _ = ptg2_serving._rank_provider_expansion_prefix(
        rows,
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
    row = {
        "provider_set_global_id_128": "01" * 16,
        "serving_content_hash_128": "02" * 16,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "FFS",
        "source_key": 0,
    }

    rank_by_key, selected_npis, _ = ptg2_serving._rank_provider_expansion_prefix(
        [row],
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
    rows = [
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
    for row in rows:
        for npi in npis_by_set[row["provider_set_global_id_128"]]:
            key = (
                "npi",
                str(npi),
                row["reported_code_system"],
                row["reported_code"],
                row["negotiation_arrangement"],
                str(row["source_key"]),
            )
            if key not in exhaustive_keys:
                exhaustive_keys.append(key)
                if row["provider_set_global_id_128"] not in exhaustive_provider_set_ids:
                    exhaustive_provider_set_ids.append(
                        row["provider_set_global_id_128"]
                    )
            if len(exhaustive_keys) >= target_count:
                break
        if len(exhaustive_keys) >= target_count:
            break

    rank_by_key, selected_npis, selected_provider_set_ids = (
        ptg2_serving._rank_provider_expansion_prefix(
            rows,
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
    rows = [
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
        rows,
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
    rows = [
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
            return rows[:limit]
        completion_filters.append(tuple(sorted(provider_set_keys)))
        return rows

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
        "_ptg2_manifest_provider_npis_for_provider_sets",
        fake_npis,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_provider_set_ids_for_selected_npis",
        fake_reverse,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_set_keys_for_ids",
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


# Strict shared V3 serving contract


@pytest.mark.asyncio
async def test_taxonomy_enrichment_is_optional_when_reference_tables_are_absent(
    monkeypatch,
):
    session = FakeSession()

    async def relation_available(_session, _table_name):
        return False

    monkeypatch.setattr(ptg2_serving, "_relation_available", relation_available)

    assert await ptg2_serving._ptg2_manifest_taxonomy_rows_for_npis(
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

    response = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:209901:synthetic",
        {"plan_id": "SYNTHETIC-PLAN", "code": "70551", "code_system": "CPT"},
        FakePagination(),
        _strict_v3_tables(),
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert response is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "mrf.ptg2_v3_code" in sql
    assert "mrf.ptg2_v3_snapshot_scope" in sql
    assert "code_metadata.snapshot_key = :shared_snapshot_key" in sql
    assert params["logical_snapshot_id"] == "ptg2:209901:synthetic"
    assert params["shared_snapshot_key"] == 41


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

    rows = await ptg2_serving._ptg2_manifest_code_rows_for_provider_reverse(
        session,
        _strict_v3_tables(),
        requested_plan="SYNTHETIC-PLAN",
        code_value="110",
        code_system="RC",
        q_text="",
        code_context=None,
        plan_market_type="group",
    )

    assert rows[0]["reported_code"] == "110"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "mrf.ptg2_v3_code" in sql
    assert "mrf.ptg2_v3_snapshot_scope" in sql
    assert "code_metadata.snapshot_key = :shared_snapshot_key" in sql
    assert "reported_code = :reported_code_0" in sql
    assert "reported_code = :reported_code_1" in sql
    assert params["reported_code_0"] == "0110"
    assert params["reported_code_1"] == "110"
    assert params["shared_snapshot_key"] == 41


# Code normalization and crosswalk context


def test_revenue_code_lookup_values_include_raw_and_canonical_forms():
    assert catalog_code_lookup_values("RC", "110") == ("0110", "110")
    assert catalog_code_lookup_values("revenue_code", "0450") == ("0450", "450")
    assert catalog_code_lookup_values("RC", "020") == ("0020", "020", "20")
    assert catalog_code_lookup_values("CPT", "99213") == ("99213",)


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
    params = {}

    ptg2_serving._append_manifest_reported_code_filter(
        filters,
        params,
        code="0110",
        code_system="RC",
    )

    assert "reported_code_system = :reported_code_system_0" in filters[0]
    assert "reported_code = :reported_code_0" in filters[0]
    assert "reported_code = :reported_code_1" in filters[0]
    assert params["reported_code_system_0"] == "RC"
    assert params["reported_code_0"] == "0110"
    assert params["reported_code_1"] == "110"


def test_manifest_code_filter_uses_resolved_external_context():
    filters = []
    params = {}

    ptg2_serving._append_manifest_reported_code_filter(
        filters,
        params,
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

    assert set(value for key, value in params.items() if "system" in key) == {
        "CPT",
        "HCPCS",
    }
    assert set(
        value
        for key, value in params.items()
        if key.startswith("reported_code_") and "system" not in key
    ) == {"70551"}


# Taxonomy and geo SQL used by strict shared V3 provider expansion


@pytest.mark.asyncio
async def test_taxonomy_filter_uses_primary_specialty_codes():
    session = FakeSession([FakeResult(result_rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"specialty": "Family Medicine"},
        [1234567890, 1234567891],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "manifest_provider_specialty_nt.npi = source_npis.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert params["manifest_provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["manifest_provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_inferred_taxonomy_filter_requires_individual_npi():
    session = FakeSession([FakeResult(result_rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"code": "29888", "code_system": "CPT"},
        [1234567890, 1234567891],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "FROM mrf.npi_taxonomy nt WHERE nt.npi = source_npis.npi" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207X00000X" in str(params)


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
    params = {}
    sql = ptg2_serving._inferred_provider_taxonomy_code_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        schema="mrf",
        params=params,
        param_prefix="inferred",
    )

    assert expected_taxonomy in params.values()
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


def test_membership_taxonomy_filters_use_uncorrelated_semijoins():
    params = {}

    filters = ptg2_serving._membership_taxonomy_filters(
        {
            "specialty": "Family Medicine",
            "code": "29888",
            "code_system": "CPT",
        },
        params,
    )

    sql = " ".join(filters)
    assert "addr.npi IN (SELECT" in sql
    assert "FROM mrf.npi_taxonomy" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207Q00000X" in params.values()
    assert "207X00000X" in params.values()


def test_membership_geo_sql_uses_postgis_for_unified_addresses():
    params = {}

    distance_sql, filters = ptg2_serving._membership_geo_sql(
        {"lat": "41.90", "long": "-87.65", "radius_miles": "12"},
        uses_unified_addresses=True,
        parameter_map=params,
    )

    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql
    assert any("ST_DWithin" in sql for sql in filters)
    assert "COALESCE(addr.address_precision, '') <> 'city_zip'" in filters
    assert params["geo_radius_miles"] == 12.0


def test_membership_geo_sql_uses_bounded_non_unified_predicates():
    params = {}

    distance_sql, filters = ptg2_serving._membership_geo_sql(
        {"lat": "41.90", "long": "-87.65", "radius_miles": "12"},
        uses_unified_addresses=False,
        parameter_map=params,
    )

    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql
    assert "addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in filters
    assert "addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in filters
    assert any(":geo_radius_miles" in sql for sql in filters)


def test_membership_filter_preserves_zip_or_radius_semantics():
    params = {"address_types": ["practice", "primary"]}

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
        parameter_map=params,
    )

    assert "LEFT(addr.postal_code, 5) = :zip5 OR" in filter_sql
    assert "ST_DWithin" in filter_sql
    assert "addr.npi = ANY(CAST(:candidate_npis AS bigint[]))" in filter_sql
    assert "addr.npi = :provider_npi" in filter_sql
    assert "3958.7613" in distance_sql
    assert "asin" in distance_sql


def test_knn_order_requires_unscoped_first_page_coordinate_search():
    args = {"lat": "41.90", "long": "-87.65"}

    assert ptg2_serving._membership_knn_order_sql(
        args,
        candidate_npis=None,
        uses_unified_addresses=True,
        offset=0,
    ) == ptg2_serving._ptg2_geo_knn_meters_sql("addr.lat", "addr.long")
    assert (
        ptg2_serving._membership_knn_order_sql(
            {**args, "zip5": "60601"},
            candidate_npis=None,
            uses_unified_addresses=True,
            offset=0,
        )
        is None
    )
    assert (
        ptg2_serving._membership_knn_order_sql(
            args,
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
    first_prefix = [
        {"npi": 1000000001, "_ptg_source_exhausted": False},
    ]
    complete_prefix = [
        {"npi": 1000000001, "_ptg_source_exhausted": True},
        {"npi": 1000000002, "_ptg_source_exhausted": True},
    ]

    async def fake_location_rows(*_args, limit, **_kwargs):
        location_calls.append(limit)
        return first_prefix if len(location_calls) == 1 else complete_prefix

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
        for row in candidate_rows:
            npi = int(row["npi"])
            if npi in seen_npis:
                continue
            seen_npis.add(npi)
            matched_rows.append(row)
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
    assert [row["npi"] for row in candidates.location_rows] == [
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
    rows = [
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
        rows,
        plan_id="SYNTHETIC-PLAN",
        snapshot_id="ptg2:209901:synthetic",
        source_key="synthetic-source",
    )
    item = ptg2_serving._compact_item_from_row({**overlaid[0], "prices": []}, {})

    assert item["phone_number"] == "3125550100"
    assert item["phone_extension"] == "45"
    assert item["address_verification"]["address_evidence_level"] == (
        "payer_directory_network_location"
    )
    assert item["address_verification"]["provider_directory_org_name"] == (
        "Synthetic Health"
    )


def test_provider_directory_network_name_match_uses_synthetic_context_without_mutation():
    address_payload = {
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
    original = copy.deepcopy(address_payload)
    item = {
        "network_names": ["Synthetic Network"],
        "address": {
            "first_line": "100 Example Street",
            "city": "Example City",
            "state": "IL",
            "postal_code": "60001",
        },
    }

    first = ptg2_serving._address_verification_payload(item, {}, address_payload)
    second = ptg2_serving._address_verification_payload(item, {}, address_payload)

    assert address_payload == original
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
    item = ptg2_serving._compact_item_from_row(
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

    verification = item["address_verification"]
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

    item = shaped["items"][0]
    assert "service_code" not in item
    assert "service_code_system" not in item
    assert "network_names" not in item
    assert "source_trace" not in item
    assert item["address_verification"]["address_network_binding"] == (
        "payer_directory_corroborated_location"
    )
    assert "result_granularity" not in shaped["query"]
    assert shaped["provenance"]["source_key"] == "synthetic-source"
    assert shaped["provenance"]["database_evidence"][
        "server_version_num"
    ] == 160004


def test_manifest_provider_procedure_item_shapes_address_and_prices():
    item = ptg2_serving._ptg2_manifest_provider_procedure_item(
        npi=1234567890,
        data={
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

    assert item["npi"] == 1234567890
    assert item["procedure_code"] == "29888"
    assert item["prices"][0]["negotiated_rate"] == 1138.57
    assert item["address"]["first_line"] == "100 Example Street"
    assert item["address_verification"]["address_evidence_level"] == (
        "nppes_provider_address"
    )


def test_provider_rate_items_merge_duplicate_location_and_code_prices():
    base = {
        "npi": 1234567890,
        "provider_name": "Example Clinician",
        "location_hash": "synthetic-location",
        "reported_code": "70551",
        "reported_code_system": "CPT",
        "address": {"first_line": "100 Example Street"},
    }
    items = [
        {
            **base,
            "provider_set_hash": "provider-set-1",
            "price_set_hash": "price-set-1",
            "rate_pack_hash": "rate-pack-1",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
        },
        {
            **base,
            "provider_set_hash": "provider-set-2",
            "price_set_hash": "price-set-2",
            "rate_pack_hash": "rate-pack-2",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 200}],
        },
    ]

    merged = ptg2_serving._merge_ptg2_provider_rate_items(items)

    assert len(merged) == 1
    assert {price["negotiated_rate"] for price in merged[0]["prices"]} == {100, 200}
    assert merged[0]["provider_set_hashes"] == ["provider-set-1", "provider-set-2"]
    assert merged[0]["price_set_count"] == 2
    assert merged[0]["rate_pack_count"] == 2


def test_provider_rate_items_do_not_merge_different_arrangements():
    base = {
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
    items = [
        {
            **base,
            "negotiation_arrangement": "FFS",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100}],
        },
        {
            **base,
            "negotiation_arrangement": "BUNDLE",
            "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 200}],
        },
    ]

    merged = ptg2_serving._merge_ptg2_provider_rate_items(items)

    assert len(merged) == 2
    assert {
        (item["negotiation_arrangement"], item["prices"][0]["negotiated_rate"])
        for item in merged
    } == {("FFS", 100), ("BUNDLE", 200)}


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
    items = [
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
        items,
        {},
        location_filter_requested=False,
    )

    assert [item["provider_name"] for item in sorted_items] == [
        "Verified Address",
        "Inferred Address",
        "No Address",
    ]


def test_provider_sort_supports_cost_and_distance():
    items = [
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
        items,
        {"order_by": "cost"},
        location_filter_requested=True,
    )
    by_distance = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "distance"},
        location_filter_requested=True,
    )

    assert [item["provider_name"] for item in by_cost] == [
        "Far Low Cost",
        "Near High Cost",
    ]
    assert [item["provider_name"] for item in by_distance] == [
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
    items = [
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
        items,
        {"order_by": "rate", "order": "desc"},
        location_filter_requested=False,
    )

    assert [item["provider_name"] for item in sorted_items] == [
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
    matching_row = {
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
            return [[matching_row]], 1
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
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)

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

    assert [row["price_key"] for row in selection.rows] == [502]
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
    location_call = {}

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
        location_call.update(kwargs)
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
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        fake_location,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_procedure_details_for_rows",
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

    response = await ptg2_serving._search_ptg2_manifest_db_serving_table(
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

    assert location_call["provider_set_keys"] == {4}
    assert location_call["require_exhaustive"] is False
    assert [item["npi"] for item in response["items"]] == [1234567890]
    assert response["items"][0]["prices"] == [
        {"negotiated_rate": 20, "service_code": ["22"]}
    ]
    assert response["pagination"]["has_more"] is False
    assert response["pagination"]["total_is_exact"] is True


@pytest.mark.asyncio
async def test_geo_cost_order_requires_exhaustive_location_selection(monkeypatch):
    location_call = {}

    async def fake_location(*_args, **kwargs):
        location_call.update(kwargs)
        return set(), {}

    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_location_provider_matches",
        fake_location,
    )

    response = await ptg2_serving._search_ptg2_manifest_db_serving_table(
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
    assert location_call["require_exhaustive"] is True


@pytest.mark.asyncio
async def test_provider_reverse_response_uses_page_sentinel_and_honest_total(monkeypatch):
    """Verify reverse pagination uses a sentinel and reports a lower-bound total."""
    rows = tuple(
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
            rows=rows,
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
        "_ptg2_manifest_provider_sets_for_npi",
        fake_provider_sets,
    )
    monkeypatch.setattr(ptg2_serving, "_resolve_ptg2_code_search_context", fake_code_context)
    monkeypatch.setattr(ptg2_serving, "_version_three_reverse_selection", fake_reverse_selection)
    monkeypatch.setattr(ptg2_serving, "_hydrate_provider_set_network_names", fake_noop)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_procedure_details_for_rows",
        fake_details,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        fake_provider_context,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_procedure_item",
        lambda **kwargs: {
            "reported_code": kwargs["data"]["reported_code"],
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

    assert [item["reported_code"] for item in response["items"]] == ["99210", "99211"]
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
async def test_multi_network_forward_reads_use_independent_concurrent_sessions(monkeypatch):
    session_factory = ConcurrentSessionFactory()

    async def fake_snapshot_tables(_session, snapshot_id):
        return _strict_v3_tables(snapshot_id=snapshot_id)

    async def has_fake_plan_code(*_args, **_kwargs):
        return True

    async def fake_search(_session, snapshot_id, _args, _pagination, **_kwargs):
        await asyncio.sleep(0.01)
        return {
            "items": [{"provider_name": snapshot_id, "prices": []}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving.sa_db, "session", session_factory.session)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot_tables)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_snapshot_has_plan_code", has_fake_plan_code)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_search)

    merged_response = await ptg2_serving._search_multi_ptg2_snapshots(
        object(),
        [("network-a", "snapshot-a"), ("network-b", "snapshot-b")],
        {"plan_id": "TEST-PLAN-001"},
        FakePagination(),
    )

    assert session_factory.maximum_active == 2
    assert len({id(session) for session in session_factory.sessions}) == 2
    assert merged_response["query"]["snapshots"] == ["snapshot-a", "snapshot-b"]
    assert {network_item["network"] for network_item in merged_response["items"]} == {
        "network-a",
        "network-b",
    }


@pytest.mark.asyncio
async def test_multi_network_forward_failure_never_returns_partial_union(monkeypatch):
    session_factory = ConcurrentSessionFactory()

    async def fake_snapshot_tables(_session, snapshot_id):
        return _strict_v3_tables(snapshot_id=snapshot_id)

    async def has_fake_plan_code(*_args, **_kwargs):
        return True

    async def fake_search(_session, snapshot_id, _args, _pagination, **_kwargs):
        await asyncio.sleep(0.01)
        if snapshot_id == "snapshot-b":
            raise RuntimeError("network read failed")
        return {"items": [], "pagination": {"total": 0}, "query": {}}

    monkeypatch.setattr(ptg2_serving.sa_db, "session", session_factory.session)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot_tables)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_snapshot_has_plan_code", has_fake_plan_code)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_search)

    with pytest.raises(RuntimeError, match="network read failed"):
        await ptg2_serving._search_multi_ptg2_snapshots(
            object(),
            [("network-a", "snapshot-a"), ("network-b", "snapshot-b")],
            {"plan_id": "TEST-PLAN-001"},
            FakePagination(),
        )

    assert session_factory.maximum_active == 2


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
