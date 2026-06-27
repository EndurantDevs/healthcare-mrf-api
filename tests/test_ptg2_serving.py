# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json

import pytest

from api import ptg2_serving
from api import ptg2_response
from api import ptg2_price_sql
from api import ptg2_code_filters
from api import ptg2_types
from api import ptg2_index_cache
from api import ptg2_tables
from api import ptg2_serving_utils
from api import ptg2_code_details
from api import ptg2_code_context
from api import ptg2_snapshot


class FakeResult:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []
        self.rollback_count = 0

    async def execute(self, *_args, **_kwargs):
        self.calls.append((_args, _kwargs))
        value = self._results.pop(0) if self._results else None
        if isinstance(value, Exception):
            raise value
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)

    async def rollback(self):
        self.rollback_count += 1


class FakePagination:
    limit = 25
    offset = 0


def test_ptg2_response_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._shape_ptg2_response is ptg2_response._shape_ptg2_response
    assert ptg2_serving._catalog_key is ptg2_response._catalog_key
    assert ptg2_serving._canonical_catalog_code is ptg2_response._canonical_catalog_code
    assert ptg2_serving._coerce_json_payload is ptg2_response._coerce_json_payload
    assert ptg2_serving._normalize_price_payload is ptg2_response._normalize_price_payload
    assert ptg2_serving._summarize_price_payload is ptg2_response._summarize_price_payload
    assert ptg2_serving._price_response_fields is ptg2_response._price_response_fields


def test_ptg2_price_sql_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._empty_price_array_sql is ptg2_price_sql._empty_price_array_sql
    assert ptg2_serving._scalar_price_json_sql is ptg2_price_sql._scalar_price_json_sql
    assert ptg2_serving._typed_price_json_sql is ptg2_price_sql._typed_price_json_sql
    assert ptg2_serving._normalized_price_json_sql is ptg2_price_sql._normalized_price_json_sql
    assert ptg2_serving._price_atom_payload_sql is ptg2_price_sql._price_atom_payload_sql
    assert ptg2_serving._normalized_price_join_sql is ptg2_price_sql._normalized_price_join_sql


def test_ptg2_code_filter_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._normalize_code is ptg2_code_filters._normalize_code
    assert ptg2_serving._normalize_code_system is ptg2_code_filters._normalize_code_system
    assert ptg2_serving._append_code_filter is ptg2_code_filters._append_code_filter
    assert ptg2_serving._append_resolved_code_filter is ptg2_code_filters._append_resolved_code_filter
    assert ptg2_serving._ptg2_code_query_fields is ptg2_code_filters._ptg2_code_query_fields
    assert ptg2_serving._qualify_compact_filters is ptg2_code_filters._qualify_compact_filters
    assert ptg2_serving._normalize_taxonomy_code is ptg2_code_filters._normalize_taxonomy_code
    assert ptg2_serving._normalize_npi is ptg2_code_filters._normalize_npi
    assert ptg2_serving._inferred_provider_taxonomy_sql is ptg2_code_filters._inferred_provider_taxonomy_sql


def test_ptg2_code_context_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._query_ptg2_code_crosswalk_edges is ptg2_code_context._query_ptg2_code_crosswalk_edges
    assert ptg2_serving._resolve_ptg2_code_search_context is ptg2_code_context._resolve_ptg2_code_search_context


def test_ptg2_type_split_keeps_serving_facade_classes_stable():
    assert ptg2_serving.PTG2ServingIndex is ptg2_types.PTG2ServingIndex
    assert ptg2_serving.PTG2ServingTables is ptg2_types.PTG2ServingTables


def test_ptg2_index_cache_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving.clear_ptg2_index_cache is ptg2_index_cache.clear_ptg2_index_cache
    assert ptg2_serving._ptg2_response_cache_key is ptg2_index_cache._ptg2_response_cache_key
    assert ptg2_serving._ptg2_response_cache_get is ptg2_index_cache._ptg2_response_cache_get
    assert ptg2_serving._ptg2_response_cache_set is ptg2_index_cache._ptg2_response_cache_set
    assert ptg2_serving._artifact_root is ptg2_index_cache._artifact_root
    assert ptg2_serving._path_from_uri is ptg2_index_cache._path_from_uri
    assert ptg2_serving.load_ptg2_index_from_path is ptg2_index_cache.load_ptg2_index_from_path


def test_ptg2_snapshot_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving.current_snapshot_id is ptg2_snapshot.current_snapshot_id
    assert ptg2_serving.current_source_snapshot_id_for_plan is ptg2_snapshot.current_source_snapshot_id_for_plan
    assert ptg2_serving.resolve_current_ptg2_snapshot_id is ptg2_snapshot.resolve_current_ptg2_snapshot_id
    assert ptg2_serving.snapshot_artifact_uri is ptg2_snapshot.snapshot_artifact_uri
    assert ptg2_serving.load_current_ptg2_index is ptg2_snapshot.load_current_ptg2_index


def test_ptg2_table_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._serving_table_available is ptg2_tables._serving_table_available
    assert ptg2_serving._index_available is ptg2_tables._index_available
    assert ptg2_serving._gin_index_available_for_column is ptg2_tables._gin_index_available_for_column
    assert ptg2_serving._serving_table_name is ptg2_tables._serving_table_name
    assert ptg2_serving._safe_table_name is ptg2_tables._safe_table_name
    assert ptg2_serving._serving_table_candidates is ptg2_tables._serving_table_candidates
    assert ptg2_serving.snapshot_serving_table is ptg2_tables.snapshot_serving_table
    assert ptg2_serving.snapshot_serving_tables is ptg2_tables.snapshot_serving_tables
    assert ptg2_serving._ordered_serving_table_candidates is ptg2_tables._ordered_serving_table_candidates
    assert ptg2_serving._is_compact_serving_table is ptg2_tables._is_compact_serving_table


def test_ptg2_serving_utils_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._normalize_zip5 is ptg2_serving_utils._normalize_zip5
    assert ptg2_serving._provider_payload is ptg2_serving_utils._provider_payload
    assert ptg2_serving._row_mapping is ptg2_serving_utils._row_mapping
    assert ptg2_serving._price_filter_clauses is ptg2_serving_utils._price_filter_clauses


@pytest.mark.asyncio
async def test_manifest_filter_npis_by_provider_taxonomy_uses_primary_code_set():
    session = FakeSession([FakeResult(rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"specialty": "Family Medicine"},
        [1234567890, 1003179466, 1003141920],
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
async def test_manifest_filter_npis_by_inferred_taxonomy_requires_individual_npi():
    session = FakeSession([FakeResult(rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"code": "29888"},
        [1234567890, 1003179466, 1003141920],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "FROM mrf.npi_taxonomy nt WHERE nt.npi = source_npis.npi" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_manifest_serving_taxonomy_expansion_uses_wider_rate_candidate_window(monkeypatch):
    provider_sets = [f"{idx:032x}" for idx in range(1, 6)]
    price_sets = [f"{idx:032x}" for idx in range(101, 106)]
    rows = [
        {
            "serving_content_hash_128": f"{idx + 201:032x}",
            "plan_id": "465722012",
            "reported_code_system": "CPT",
            "reported_code": "99214",
            "procedure_global_id_128": f"{idx + 301:032x}",
            "provider_set_global_id_128": provider_sets[idx],
            "provider_count": 100 - idx,
            "price_set_global_id_128": price_sets[idx],
            "source_trace_set_hash": None,
        }
        for idx in range(5)
    ]
    session = FakeSession([5, FakeResult(rows=rows)])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_token",
        price_atom_table="mrf.ptg2_price_atom_manifest_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_manifest_token",
    )

    class LimitOnePagination:
        limit = 1
        offset = 0

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_prices(_session, _tables, price_set_ids):
        assert tuple(price_set_ids) == tuple(price_sets)
        return {
            price_set_id: [{"negotiated_type": "fee schedule", "negotiated_rate": 114.82}]
            for price_set_id in price_set_ids
        }

    async def fake_providers(_session, _tables, provider_set_ids, *, limit_per_set, args):
        assert tuple(provider_set_ids) == tuple(provider_sets)
        assert limit_per_set == 1
        assert args["specialty"] == "Family Medicine"
        return {
            provider_sets[1]: [
                {
                    "npi": 1851399604,
                    "provider_name": "Family Medicine Provider",
                    "state": "FL",
                    "city": "Jacksonville",
                    "zip5": "32210",
                    "taxonomy_codes": ["207Q00000X"],
                    "specialties": ["Family Medicine Physician"],
                }
            ]
        }

    async def fake_procedure_details(_session, row_data):
        assert [row["provider_set_global_id_128"] for row in row_data] == provider_sets
        return {("CPT", "99214"): {"procedure_name": "Office/outpatient visit"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_rows_for_provider_sets", fake_providers)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "465722012",
            "market_type": "group",
            "code": "99214",
            "code_system": "CPT",
            "include_providers": "true",
            "specialty": "Family Medicine",
        },
        LimitOnePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert len(payload["items"]) == 1
    assert payload["items"][0]["npi"] == 1851399604
    assert payload["items"][0]["taxonomy_codes"] == ["207Q00000X"]
    assert payload["pagination"]["limit"] == 1
    assert payload["query"]["plan_market_type"] == "group"
    row_sql = str(session.calls[1][0][0])
    row_params = session.calls[1][0][1]
    assert "LIMIT :rate_candidate_limit" in row_sql
    assert row_params["limit"] == 1
    assert row_params["rate_candidate_limit"] > row_params["limit"]
    assert row_params["rate_candidate_limit"] <= ptg2_serving._PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT


@pytest.mark.asyncio
async def test_manifest_serving_geo_expansion_uses_wider_location_candidate_window(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000101"
    row = {
        "serving_content_hash_128": "00000000000000000000000000000201",
        "plan_id": "010854205",
        "reported_code_system": "CPT",
        "reported_code": "29888",
        "procedure_global_id_128": "00000000000000000000000000000301",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 335,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": None,
    }
    session = FakeSession([FakeResult(rows=[row])])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_token",
        price_atom_table="mrf.ptg2_price_atom_manifest_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_manifest_token",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )

    class LimitOnePagination:
        limit = 1
        offset = 0

    seen_candidate_limit = {}

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_location_matches(_session, _tables, args, *, candidate_limit=None):
        assert args["zip5"] == "62401"
        seen_candidate_limit["value"] = candidate_limit
        return {
            provider_set_id,
        }, {
            provider_set_id: [
                {
                    "npi": 1154321222,
                    "provider_name": "ACL Surgeon",
                    "state": "IL",
                    "city": "PANA",
                    "zip5": "62557",
                    "telephone_number": "2175551212",
                    "fax_number": "2175551213",
                    "address_payload": '{"telephone_number":"2175551212","fax_number":"2175551213"}',
                    "taxonomy_codes": ["207XS0114X"],
                    "specialties": ["Orthopaedic Surgery Physician"],
                    "classifications": ["Orthopaedic Surgery"],
                    "specializations": ["Sports Medicine"],
                    "primary_specialty": "Orthopaedic Surgery Physician",
                    "primary_specialization": "Sports Medicine",
                }
            ]
        }

    async def fake_prices(_session, _tables, price_set_ids):
        assert tuple(price_set_ids) == (price_set_id,)
        return {price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": 1074.22}]}

    async def fake_procedure_details(_session, row_data):
        assert [row["provider_set_global_id_128"] for row in row_data] == [provider_set_id]
        return {("CPT", "29888"): {"procedure_name": "ACL reconstruction"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_location_provider_matches", fake_location_matches)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "010854205",
            "market_type": "group",
            "code": "29888",
            "code_system": "CPT",
            "zip5": "62401",
            "lat": "39.11952",
            "long": "-88.56418",
            "radius_miles": "100",
            "include_providers": "true",
        },
        LimitOnePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert len(payload["items"]) == 1
    assert payload["items"][0]["npi"] == 1154321222
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    assert payload["items"][0]["phone"] == "2175551212"
    assert payload["items"][0]["phone_number"] == "2175551212"
    assert payload["items"][0]["telephone_number"] == "2175551212"
    assert payload["items"][0]["fax_number"] == "2175551213"
    assert payload["items"][0]["address"]["telephone_number"] == "2175551212"
    assert seen_candidate_limit["value"] >= 500
    row_sql = str(session.calls[0][0][0])
    row_params = session.calls[0][0][1]
    assert "LIMIT :rate_candidate_limit" in row_sql
    assert row_params["limit"] == 1
    assert row_params["rate_candidate_limit"] == seen_candidate_limit["value"]
    assert row_params["rate_candidate_limit"] > row_params["limit"]


@pytest.mark.asyncio
async def test_manifest_enriched_provider_fallback_includes_taxonomy(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(
        [
            False,
            False,
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "taxonomy_codes": ["207Q00000X"],
                        "specialties": ["Family Medicine"],
                    }
                ]
            ),
        ]
    )

    rows = await ptg2_serving._ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=[1234567890],
        limit=5,
    )

    assert rows == [
        {
            "npi": 1234567890,
            "provider_name": "TiC provider",
            "taxonomy_codes": ["207Q00000X"],
            "specialties": ["Family Medicine"],
            "classifications": [],
            "specializations": [],
            "primary_specialty": None,
            "primary_specialization": None,
        }
    ]
    assert "FROM mrf.npi_taxonomy nt" in str(session.calls[2][0][0])


@pytest.mark.asyncio
async def test_manifest_enriched_provider_unified_fallback_uses_bounded_cte(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    rows = await ptg2_serving._ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=[1234567890],
        limit=5,
    )

    assert rows == []
    sql = str(session.calls[-1][0][0])
    assert "source_npis AS MATERIALIZED" in sql
    assert "fallback_addresses AS MATERIALIZED" in sql
    assert "JOIN source_npis source_filter ON source_filter.npi = na.npi" in sql
    assert "LEFT JOIN fallback_addresses na" in sql
    assert "LEFT JOIN LATERAL (\n                SELECT na.first_line" not in sql


@pytest.mark.asyncio
async def test_ptg2_address_serving_table_prefers_unified_by_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    session = FakeSession(
        [FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)])]
    )

    table_name = await ptg2_serving._ptg2_address_serving_table(
        session,
        ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS,
    )

    assert table_name == "mrf.entity_address_unified"


@pytest.mark.asyncio
async def test_ptg2_address_serving_table_uses_legacy_when_explicit(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([])

    table_name = await ptg2_serving._ptg2_address_serving_table(
        session,
        ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS,
    )

    assert table_name == "mrf.npi_address"
    assert session.calls == []


def test_ptg2_code_details_split_keeps_serving_facade_helper_stable():
    assert ptg2_serving._enrich_ptg2_code_details is ptg2_code_details._enrich_ptg2_code_details


def _compact_tables(**overrides):
    values = {
        "serving_table": "mrf.ptg2_serving_rate_compact_token",
        "price_code_set_table": "mrf.ptg2_price_code_set_token",
        "price_atom_table": "mrf.ptg2_price_atom_token",
        "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
        "procedure_table": "mrf.ptg2_procedure_token",
        "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
        "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
        "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
        "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
    }
    values.update(overrides)
    return ptg2_serving.PTG2ServingTables(**values)


def _db_serving_session():
    return FakeSession(
        [
            None,
            "snap-db",
            {"table": "mrf.ptg2_serving_rate"},
            "mrf.ptg2_serving_rate",
            1,
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-db",
                        "plan_id": "010854205",
                        "plan_name": "Heartland",
                        "procedure_code": 123456,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "provider_set_hash": "provider-set-1",
                        "provider_set_hashes": ["provider-set-a"],
                        "provider_count": 123,
                        "provider_set_count": 1,
                        "price_set_hash": "price-set-1",
                        "rate_pack_hash": "pack-1",
                        "prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 450,
                                "service_code": ["23"],
                                "billing_code_modifier": ["TC", "ZZ"],
                            }
                        ],
                        "source_trace": [{"url": "https://example.test/rates.json.gz"}],
                        "confidence": {"network": "tic_rate_npi_tin"},
                    }
                ]
            ),
        ]
    )


def _fixture_payload():
    return {
        "version": 1,
        "snapshot_id": "snap-fixture",
        "plans": {"010854205": {"name": "Heartland Dental"}},
        "procedures": {
            "70551": {
                "code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "name": "MRI brain",
            }
        },
        "providers": {
            "1": {
                "provider_ordinal": 1,
                "npi": 1234567890,
                "provider_name": "Example Imaging",
                "city": "Peoria",
                "state": "IL",
                "zip5": "61636",
            }
        },
        "rates": {
            "010854205": {
                "70551": [
                    {
                        "provider_ordinal": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 450}],
                        "source_trace": [{"url": "https://example.test/tic.json.gz"}],
                    }
                ]
            }
        },
    }


def test_search_ptg2_index_returns_prices_and_source_trace():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    payload = ptg2_serving.search_ptg2_index(index, plan_id="010854205", code="70551", state="IL")

    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["source_trace"][0]["url"] == "https://example.test/tic.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert payload["query"]["source"] == "ptg2"


def test_db_serving_code_filter_accepts_signed_hp_procedure_codes():
    filters = []
    params = {}

    ptg2_serving._append_code_filter(
        filters,
        params,
        code="-1201887592",
        code_system="HP_PROCEDURE_CODE",
    )

    assert filters == ["procedure_code = :procedure_code"]
    assert params["procedure_code"] == -1201887592


@pytest.mark.asyncio
async def test_ptg2_code_context_bridges_cpt_and_hcpcs_same_code():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="70551",
        code_system="CPT",
    )

    assert context["input_code"] == {"code_system": "CPT", "code": "70551"}
    assert {"code_system": "CPT", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "CDT", "code": "70551"} not in context["resolved_codes"]
    assert context["internal_codes"] == []


@pytest.mark.asyncio
async def test_ptg2_code_context_bridges_cdt_and_hcpcs_dental_code():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="D0120",
        code_system="CDT",
    )

    assert context["input_code"] == {"code_system": "CDT", "code": "D0120"}
    assert {"code_system": "CDT", "code": "D0120"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "D0120"} in context["resolved_codes"]
    assert {"code_system": "CPT", "code": "D0120"} not in context["resolved_codes"]
    assert context["internal_codes"] == []


@pytest.mark.asyncio
async def test_ptg2_code_context_keeps_ms_drg_exact():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="47",
        code_system="DRG",
    )

    assert context["input_code"] == {"code_system": "MS_DRG", "code": "047"}
    assert context["resolved_codes"] == [{"code_system": "MS_DRG", "code": "047"}]
    assert context["internal_codes"] == []

    filters = []
    params = {}
    ptg2_serving._append_resolved_code_filter(
        filters,
        params,
        code="47",
        code_system="DRG",
        code_context=context,
    )

    assert "reported_code_system_0_0" in params
    assert params["reported_code_system_0_0"] == "MS_DRG"
    assert params["reported_code_0"] == "047"


@pytest.mark.asyncio
async def test_ptg2_code_context_expands_internal_code_crosswalk():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession(
            [
                FakeResult(
                    rows=[
                        {
                            "from_system": "CPT",
                            "from_code": "70551",
                            "to_system": "HP_PROCEDURE_CODE",
                            "to_code": "123456",
                            "match_type": "exact",
                            "confidence": 1.0,
                            "source": "test",
                        }
                    ]
                ),
                FakeResult(rows=[]),
            ]
        ),
        code="70551",
        code_system="CPT",
    )

    assert {"code_system": "CPT", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HP_PROCEDURE_CODE", "code": "123456"} in context["resolved_codes"]
    assert context["internal_codes"] == [123456]
    assert context["matched_via"][0]["source"] == "test"


@pytest.mark.asyncio
async def test_ptg2_code_context_keeps_non_procedure_system_exact():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([]),
        code="0450",
        code_system="RC",
    )
    filters = []
    params = {}

    ptg2_serving._append_resolved_code_filter(
        filters,
        params,
        code="0450",
        code_system="RC",
        code_context=context,
    )

    assert context is None
    assert "reported_code_system = :reported_code_system" in filters[0]
    assert params["reported_code_system"] == "RC"
    assert params["reported_code"] == "0450"


@pytest.mark.asyncio
async def test_ptg2_serving_table_uses_equivalent_cpt_hcpcs_filter_for_compact_search():
    session = FakeSession(
        [
            FakeResult(rows=[]),
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
        ]
    )

    await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "code_system": "CPT"},
        FakePagination(),
        serving_tables=_compact_tables(),
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "r.reported_code = :reported_code_0" in sql
    assert "r.reported_code_system IN (:reported_code_system_0_0, :reported_code_system_0_1)" in sql
    assert set(params[key] for key in ("reported_code_system_0_0", "reported_code_system_0_1")) == {"CPT", "HCPCS"}
    assert params["reported_code_0"] == "70551"


@pytest.mark.asyncio
async def test_ptg2_serving_table_uses_equivalent_cdt_hcpcs_filter_for_compact_search():
    session = FakeSession(
        [
            FakeResult(rows=[]),
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
        ]
    )

    await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-token",
        {"plan_id": "010854205", "code": "D0120", "code_system": "CDT"},
        FakePagination(),
        serving_tables=_compact_tables(),
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "r.reported_code = :reported_code_0" in sql
    assert "r.reported_code_system IN (:reported_code_system_0_0, :reported_code_system_0_1)" in sql
    assert set(params[key] for key in ("reported_code_system_0_0", "reported_code_system_0_1")) == {"CDT", "HCPCS"}
    assert params["reported_code_0"] == "D0120"


def test_price_summary_groups_component_rates_and_counts_raw_prices():
    prices = [
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": "83.09",
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["22", "21"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": 83.09,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": ["TC"],
            "negotiated_rate": 158.58,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": [],
            "negotiated_rate": 241.67,
            "negotiated_type": "negotiated",
        },
    ]

    normalized = ptg2_serving._normalize_price_payload(prices)
    summary = ptg2_serving._summarize_price_payload(prices)

    assert len(normalized) == 3
    assert normalized[0]["service_code"] == ["21", "22"]
    assert normalized[0]["billing_code_modifier"] == ["26"]
    assert summary == [
        {
            "component": "global",
            "modifier": [],
            "rate": 241.67,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
        {
            "component": "professional",
            "modifier": ["26"],
            "rate": 83.09,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "raw_price_count": 1,
        },
        {
            "component": "technical",
            "modifier": ["TC"],
            "rate": 158.58,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
    ]


@pytest.mark.asyncio
async def test_load_current_ptg2_index_reads_snapshot_artifact(tmp_path):
    artifact = tmp_path / "snapshot.json"
    artifact.write_text(json.dumps(_fixture_payload()), encoding="utf-8")
    ptg2_serving.clear_ptg2_index_cache()
    session = FakeSession(["snap-fixture", artifact.resolve().as_uri()])

    index = await ptg2_serving.load_current_ptg2_index(session)

    assert index is not None
    assert index.snapshot_id == "snap-fixture"
    assert "010854205" in index.rates


@pytest.mark.asyncio
async def test_search_current_ptg2_index_reads_db_serving_table():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )

    assert "source" not in payload["query"]
    assert "serving_table" not in payload["query"]
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["procedure_code"] == 123456
    assert item["service_code"] == "70551"
    assert item["reported_code_system"] == "CPT"
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["provider_count"] == 123
    assert "source_trace" not in item
    assert "confidence" not in item
    assert "price_set_hash" not in item
    assert "provider_set_hash" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_sources_without_debug_fields():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_sources": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["serving_table"] == "mrf.ptg2_serving_rate"
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["source_trace"][0]["url"] == "https://example.test/rates.json.gz"
    assert "confidence" not in item
    assert "price_set_hash" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_full_details():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_details": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["procedure_consolidation"] == "HP_PROCEDURE_CODE"
    item = payload["items"][0]
    assert item["source_trace"][0]["url"] == "https://example.test/rates.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert item["price_set_hash"] == "price-set-1"
    assert item["provider_set_hash"] == "provider-set-1"


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_code_details():
    session = _db_serving_session()
    session._results.append(
        FakeResult(
            rows=[
                {
                    "code_system": "CPT",
                    "code": "70551",
                    "display_name": "MRI brain without contrast",
                    "short_description": "MRI brain without contrast",
                },
                {
                    "code_system": "POS",
                    "code": "23",
                    "display_name": "Emergency Room - Hospital",
                    "short_description": "Emergency Room - Hospital",
                },
                {
                    "code_system": "MODIFIER",
                    "code": "TC",
                    "display_name": "Technical component",
                    "short_description": "Technical component",
                },
            ]
        )
    )

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "010854205", "code": "70551", "include_code_details": "true"},
        FakePagination(),
    )

    item = payload["items"][0]
    assert item["billing_code_detail"]["display_name"] == "MRI brain without contrast"
    assert item["tic_prices"][0]["service_code_details"][0]["code_system"] == "POS"
    assert item["tic_prices"][0]["service_code_details"][0]["display_name"] == "Emergency Room - Hospital"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["code_system"] == "MODIFIER"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["display_name"] == "Technical component"
    assert item["tic_prices"][0]["billing_code_modifier_details"][1] == {
        "code_system": "MODIFIER",
        "code": "ZZ",
        "display_name": "Modifier ZZ",
        "short_description": None,
        "catalog_status": "missing",
    }
    assert "source_trace" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_caches_shaped_positive_responses(monkeypatch):
    calls = {"snapshot": 0, "search": 0}

    async def fake_resolve(_session, _args):
        return "snap-cache"

    async def fake_snapshot(_session, _snapshot_id):
        calls["snapshot"] += 1
        return ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_rate")

    async def fake_search(_session, _snapshot_id, _args, _pagination, *, serving_tables):
        del serving_tables
        calls["search"] += 1
        return {
            "items": [
                {
                    "reported_code": "70551",
                    "source_trace": [{"url": "https://example.test/rates.json.gz"}],
                    "provider_set_hash": "provider-set-1",
                }
            ],
            "query": {"snapshot_id": _snapshot_id, "source": "ptg2_db", "serving_table": "mrf.ptg2_serving_rate"},
            "pagination": {"total": 1},
        }

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)
    monkeypatch.setattr(ptg2_serving, "search_ptg2_serving_table", fake_search)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )
    payload["items"][0]["reported_code"] = "mutated"
    cached_payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )

    assert calls == {"snapshot": 1, "search": 1}
    assert cached_payload["items"][0]["reported_code"] == "70551"
    assert "source_trace" not in cached_payload["items"][0]
    assert "provider_set_hash" not in cached_payload["items"][0]
    assert "source" not in cached_payload["query"]


@pytest.mark.asyncio
async def test_search_current_ptg2_index_ignores_non_manifest_serving_storage(monkeypatch):
    async def fake_resolve(_session, _args):
        return "legacy-snap"

    async def fake_snapshot(_session, _snapshot_id):
        return ptg2_serving.PTG2ServingTables(
            storage="db_compact_snapshot",
            serving_table="mrf.ptg2_serving_rate_compact_legacy",
        )

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
    )

    assert payload is None


@pytest.mark.asyncio
async def test_search_current_ptg2_index_does_not_negative_cache_missing_payload(monkeypatch):
    calls = {"snapshot": 0, "search": 0}

    async def fake_resolve(_session, _args):
        return "snap-cache-miss"

    async def fake_snapshot(_session, _snapshot_id):
        calls["snapshot"] += 1
        return ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_rate")

    async def fake_search(_session, _snapshot_id, _args, _pagination, *, serving_tables):
        del serving_tables
        calls["search"] += 1
        return None

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.delenv(ptg2_serving.PTG2_JSON_FALLBACK_ENV, raising=False)
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)
    monkeypatch.setattr(ptg2_serving, "search_ptg2_serving_table", fake_search)

    assert await ptg2_serving.search_current_ptg2_index(FakeSession([]), {"plan_id": "010854205"}, FakePagination()) is None
    assert await ptg2_serving.search_current_ptg2_index(FakeSession([]), {"plan_id": "010854205"}, FakePagination()) is None
    assert calls == {"snapshot": 2, "search": 2}


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_routes_by_plan_source_pointer():
    session = FakeSession(["snap-source"])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "010854205", "plan_market_type": "group", "source_key": "heartland_dental"},
    )

    assert snapshot_id == "snap-source"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "ptg2_current_plan_source" in sql
    assert "JOIN mrf.ptg2_snapshot" in sql
    assert "cps.plan_market_type = :plan_market_type" in sql
    assert "cps.source_key = :source_key" in sql
    assert "s.status = 'published'" in sql
    assert "serving_index" in sql
    assert params["source_key"] == "heartland_dental"


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_prefers_loaded_serving_table():
    # Regression: a plan with multiple networks must resolve to a snapshot whose
    # serving table is actually materialized (to_regclass), not merely manifest-claimed,
    # so an unloaded newer network does not yield snapshot_not_loaded / 0 results.
    session = FakeSession(["snap-loaded"])
    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "010854205", "plan_market_type": "group"},
    )
    assert snapshot_id == "snap-loaded"
    sql = str(session.calls[0][0][0])
    assert "to_regclass(s.manifest->'serving_index'->>'table') IS NOT NULL) DESC" in sql
    # recency stays the tiebreaker among loaded snapshots
    assert "cps.import_month DESC NULLS LAST" in sql


def test_musculoskeletal_surgery_cpt_infers_orthopedic_taxonomy():
    for code in ("29888", "27447", "20000", "29999"):
        rule = ptg2_serving._inferred_provider_taxonomy_rule({"code": code, "code_system": "cpt"})
        assert rule is not None, code
        assert "207X00000X" in rule.taxonomy_codes, code
    omitted_system_rule = ptg2_serving._inferred_provider_taxonomy_rule({"code": "29888"})
    assert omitted_system_rule is not None
    assert "207X00000X" in omitted_system_rule.taxonomy_codes
    # office-visit / non-musculoskeletal codes must NOT infer orthopedic surgery
    assert ptg2_serving._inferred_provider_taxonomy_rule({"code": "99213", "code_system": "cpt"}) is None
    # Short numeric revenue codes are not CPT by default.
    assert ptg2_serving._inferred_provider_taxonomy_rule({"code": "450"}) is None


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_rolls_back_missing_source_pointer_before_fallback():
    session = FakeSession([RuntimeError("missing source pointer"), "snap-global"])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "010854205", "plan_market_type": "group"},
    )

    assert snapshot_id == "snap-global"
    assert session.rollback_count == 1
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_uses_compact_snapshot_without_market_column():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
                "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-token",
                        "plan_id": "010854205",
                        "plan_name": None,
                        "plan_id_type": None,
                        "plan_market_type": None,
                        "issuer_name": None,
                        "plan_sponsor_name": None,
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "billing_code": "99213",
                        "billing_code_type": "CPT",
                        "procedure_name": "Office visit",
                        "procedure_description": "Established office visit",
                        "provider_set_hash": "provider-set-1",
                        "provider_count": 2,
                        "provider_set_count": None,
                        "price_set_hash": "price-set-1",
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 101.42}],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {
            "plan_id": "010854205",
            "plan_market_type": "group",
            "source_key": "heartland_dental",
            "code": "99213",
            "code_system": "CPT",
        },
        FakePagination(),
    )

    assert payload["items"][0]["npi"] == 1083311500
    assert payload["items"][0]["reported_code"] == "99213"
    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == 101.42
    row_call = next(call for call in session.calls if "provider_sets AS MATERIALIZED" in str(call[0][0]))
    row_sql = str(row_call[0][0])
    assert "r.plan_market_type" not in row_sql
    assert "mrf.ptg2_provider_set_component_token" in row_sql
    assert "mrf.ptg2_provider_entry_component_token" not in row_sql
    assert "provider_group_hashes @>" not in row_sql
    assert "NULL::varchar AS plan_market_type" in row_sql
    assert "r.provider_set_count" not in row_sql
    assert "NULL::integer AS provider_set_count" in row_sql
    assert row_call[0][1]["plan_id"] == "010854205"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_filters_prices_by_pos_modifier_and_rate():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-token",
                        "plan_id": "010854205",
                        "plan_name": None,
                        "plan_id_type": None,
                        "plan_market_type": None,
                        "issuer_name": None,
                        "plan_sponsor_name": None,
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "93458",
                        "billing_code": "93458",
                        "billing_code_type": "CPT",
                        "procedure_name": "Cath placement",
                        "procedure_description": "Cath placement",
                        "provider_set_hash": "provider-set-1",
                        "provider_count": 3,
                        "provider_set_count": None,
                        "price_set_hash": "price-set-1",
                        "prices": [
                            {
                                "negotiated_type": "fee schedule",
                                "negotiated_rate": 516.08,
                                "service_code": ["21"],
                                "billing_code_modifier": ["26"],
                            }
                        ],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1235189762,
        {
            "plan_id": "010854205",
            "code": "93458",
            "code_system": "CPT",
            "pos": "21",
            "modifier": "26",
            "rate": "516.08",
            "include_details": "true",
        },
        FakePagination(),
    )

    item = payload["items"][0]
    assert item["tic_prices"][0]["negotiated_rate"] == 516.08
    assert item["tic_prices"][0]["service_code"] == ["21"]
    assert item["tic_prices"][0]["billing_code_modifier"] == ["26"]
    assert payload["query"]["price_filter"] == {
        "service_code": ["21"],
        "pos": "21",
        "billing_code_modifier": ["26"],
        "negotiated_rate": 516.08,
        "rate_tolerance": 0.01,
    }
    row_call = next(call for call in session.calls if "provider_sets AS MATERIALIZED" in str(call[0][0]))
    row_sql = str(row_call[0][0])
    params = row_call[0][1]
    assert "price_payload.prices IS NOT NULL" in row_sql
    assert "CAST(:price_service_codes AS varchar[])" in row_sql
    assert "CAST(:price_modifier_codes AS varchar[])" in row_sql
    assert "ABS(pa.negotiated_rate::numeric - :price_negotiated_rate)" in row_sql
    assert params["price_service_codes"] == ["21"]
    assert params["price_modifier_codes"] == ["26"]
    assert str(params["price_negotiated_rate"]) == "516.08"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_returns_no_match_after_snapshot_resolves():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
                "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            0,
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {"plan_id": "010854205", "code": "99213", "code_system": "CPT", "include_details": "true"},
        FakePagination(),
    )

    assert payload["items"] == []
    assert payload["pagination"]["total"] == 0
    assert payload["query"]["snapshot_id"] == "snap-token"
    assert payload["query"]["status"] == "no_match"
    assert payload["query"]["source"] == "ptg2_db"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_requires_normalized_provider_membership():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {"plan_id": "010854205", "code": "99213", "code_system": "CPT"},
        FakePagination(),
    )

    assert payload is None


@pytest.mark.asyncio
async def test_compact_serving_uses_snapshot_price_and_procedure_tables():
    session = FakeSession([FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    assert "ptg2_price_set_token" not in sql
    assert "FROM mrf.ptg2_price_set_entry_token pse" in sql
    assert "JOIN mrf.ptg2_price_atom_token pa" in sql
    assert "LEFT JOIN mrf.ptg2_price_code_set_token service_set" in sql
    assert "LEFT JOIN mrf.ptg2_price_code_set_token modifier_set" in sql
    assert "pse.price_set_hash = r.price_set_hash" in sql
    assert "ps.canonical_payload" not in sql
    assert "FROM mrf.ptg2_procedure_token proc" in sql


@pytest.mark.asyncio
async def test_compact_serving_requires_normalized_price_tables():
    session = FakeSession([FakeResult(rows=[])])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_rate_compact_token",
        procedure_table="mrf.ptg2_procedure_token",
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "010854205", "code": "70551"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    assert session.calls == []


@pytest.mark.asyncio
async def test_compact_serving_geo_search_allows_missing_specialty(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "99213", "zip5": "60601"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "addr_filter.npi = pgm_filter.npi" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5" in sql
    assert "npi_taxonomy" not in sql
    assert "specialty_like" not in params


@pytest.mark.asyncio
async def test_compact_serving_zip_centroid_search_allows_same_zip_or_radius(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "70551",
            "zip5": "60601",
            "lat": 41.8820,
            "long": -87.6278,
            "radius_miles": 10.0,
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "(LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5 OR (" in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert "CAST(:geo_radius_miles AS double precision)" in sql
    assert params["zip5"] == "60601"
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_coordinate_search_filters_npi_addresses(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "70551",
            "lat": 29.7604,
            "long": -95.3698,
            "radius_miles": 10.0,
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert "ll_to_earth" not in sql
    assert "2 * 3958.7613 * asin" in sql
    assert "radians(CAST(:geo_lat AS double precision))" in sql
    assert ") <= CAST(:geo_radius_miles AS double precision)" in sql
    assert params["geo_lat"] == 29.7604
    assert params["geo_long"] == -95.3698
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_fallback_projects_address_distance(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(["mrf.npi", FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "29888",
            "zip5": "62401",
            "lat": 39.11952,
            "long": -88.56418,
            "radius_miles": 100.0,
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    params = session.calls[-1][0][1]
    assert "addr.distance_miles, addr.zip_match_type, addr.anchor_zip5, addr.zip_radius_miles" in sql
    assert "CASE WHEN LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5 THEN 0.0 ELSE" in sql
    assert "AS distance_miles" in sql
    assert ":zip5 AS anchor_zip5, :geo_radius_miles AS zip_radius_miles" in sql
    assert "ORDER BY distance_miles ASC NULLS LAST, r.reported_code_system, r.reported_code" in sql
    assert "ORDER BY CASE WHEN LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5 THEN 0 ELSE 1 END" in sql
    assert params["zip5"] == "62401"
    assert params["geo_radius_miles"] == 100.0


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_filters_coordinates_with_unified(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(
                rows=[
                    {
                        "provider_group_global_id_128": group_id,
                        "npi": 1234567890,
                        "location_hash": "entity_address_unified:1234567890:primary:1",
                        "state": "CA",
                        "city": "GLENDALE",
                        "zip5": "91204",
                        "distance_miles": 3.25,
                        "zip_match_type": "radius",
                        "anchor_zip5": "91204",
                        "zip_radius_miles": 10.0,
                        "telephone_number": "8185551212",
                        "fax_number": "8185551213",
                        "location_source": "entity_address_unified",
                        "location_confidence_code": "entity_address_unified",
                        "address_payload": (
                            '{"address_key":"00000000-0000-0000-0000-000000000001",'
                            '"telephone_number":"8185551212","fax_number":"8185551213",'
                            '"lat":34.14024131,"long":-118.255125}'
                        ),
                        "taxonomy_codes": ["207XS0114X"],
                        "specialties": ["Orthopaedic Surgery Physician"],
                        "classifications": ["Orthopaedic Surgery"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Orthopaedic Surgery Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "TiC provider",
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )

    def fake_members_many(_serving_tables, name, group_ids, **_kwargs):
        assert name == "provider_inverted"
        assert group_ids == (group_id,)
        return {group_id: (provider_set_id,)}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many", fake_members_many)

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
    )

    assert provider_set_ids == {provider_set_id}
    provider = providers_by_set[provider_set_id][0]
    assert provider["npi"] == 1234567890
    assert provider["zip5"] == "91204"
    assert provider["distance_miles"] == 3.25
    assert provider["zip_match_type"] == "radius"
    assert provider["anchor_zip5"] == "91204"
    assert provider["zip_radius_miles"] == 10.0
    assert provider["telephone_number"] == "8185551212"
    assert provider["fax_number"] == "8185551213"
    assert provider["taxonomy_codes"] == ["207XS0114X"]
    assert provider["classifications"] == ["Orthopaedic Surgery"]
    assert provider["specializations"] == ["Sports Medicine"]
    assert provider["primary_specialization"] == "Sports Medicine"
    address = json.loads(provider["address_payload"])
    assert address["address_key"] == "00000000-0000-0000-0000-000000000001"
    assert address["telephone_number"] == "8185551212"
    assert address["fax_number"] == "8185551213"
    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "FROM mrf.entity_address_unified addr" in sql
    assert "raw_location_npis AS" in sql
    assert "location_npis AS MATERIALIZED" in sql
    assert "ORDER BY zip_rank, distance_miles ASC NULLS LAST, npi" in sql
    assert "AS distance_miles" in sql
    assert "AS zip_match_type" in sql
    assert "CAST(:geo_radius_miles AS double precision) AS zip_radius_miles" in sql
    assert "addr.telephone_number" in sql
    assert "'telephone_number', " in sql
    assert "AS telephone_number" in sql
    assert "AS fax_number" in sql
    assert "addr.address_key" in sql
    assert "addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert "ll_to_earth" not in sql
    assert "2 * 3958.7613 * asin" in sql
    assert "radians(CAST(:geo_lat AS double precision))" in sql
    assert ") <= CAST(:geo_radius_miles AS double precision)" in sql
    assert "COALESCE(addr.address_precision, '') <> 'city_zip'" in sql
    assert "fallback_addresses AS MATERIALIZED" in sql
    assert "JOIN location_npis loc ON loc.npi = na.npi" in sql
    assert "LEFT JOIN fallback_addresses na" in sql
    assert "LEFT JOIN LATERAL (\n                SELECT na.first_line" not in sql
    assert "COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications" in sql
    assert "COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations" in sql
    assert "WHERE nt.npi = addr.npi" in sql
    assert params["geo_lat"] == 34.14024131
    assert params["geo_long"] == -118.255125
    assert params["geo_radius_miles"] == 10.0
    assert params["address_types"] == ["practice", "primary"]


def test_sort_ptg2_manifest_provider_items_supports_cost_and_distance():
    items = [
        {
            "provider_name": "Far cheap",
            "prices": [{"negotiated_rate": 100.0}],
            "distance_miles": 45.0,
        },
        {
            "provider_name": "Near expensive",
            "prices": [{"negotiated_rate": 500.0}],
            "distance_miles": 2.0,
        },
    ]

    by_cost = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "total_allowed_amount", "order": "asc"},
        location_filter_requested=True,
    )
    by_distance = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "distance", "order": "asc"},
        location_filter_requested=True,
    )

    assert [item["provider_name"] for item in by_cost] == ["Far cheap", "Near expensive"]
    assert [item["provider_name"] for item in by_distance] == ["Near expensive", "Far cheap"]


def test_compact_item_promotes_location_phone_from_address_payload():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "telephone_number": "3125551212",
                "fax_number": "3125551213",
            },
            "prices": [],
        },
        {},
    )

    assert item["phone"] == "3125551212"
    assert item["phone_number"] == "3125551212"
    assert item["telephone_number"] == "3125551212"
    assert item["fax_number"] == "3125551213"
    assert item["address"]["telephone_number"] == "3125551212"


def test_orthopedic_surgery_specialty_resolves_to_taxonomy():
    from api.provider_specialty_filters import (
        ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
        resolve_provider_specialty_filter,
    )

    for term in ("orthopedic surgery", "Orthopaedic Surgeon", "orthopedics", "ortho"):
        resolved = resolve_provider_specialty_filter({"specialty": term})
        assert resolved.active, term
        assert resolved.taxonomy_codes == ORTHOPAEDIC_SURGERY_TAXONOMY_CODES, term
        assert "207X00000X" in resolved.taxonomy_codes


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_applies_specialty_taxonomy_filter(monkeypatch):
    # Regression: a location (geo/ZIP) provider search must still scope to the
    # requested clinical specialty. Without the taxonomy predicate, a procedure+ZIP
    # lookup returns every NPI that bills at that address (e.g. an optometry practice
    # or a hospital for an orthopedic ACL repair) instead of orthopedic surgeons.
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many",
        lambda *_a, **_k: {group_id: (provider_set_id,)},
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "lat": "34.14024131",
            "long": "-118.255125",
            "radius_miles": "10",
            "limit": "5",
            "specialty": "orthopedic surgery",
        },
        candidate_limit=5,
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    # The location query is now scoped by an npi_taxonomy EXISTS predicate on addr.npi.
    assert "mrf.npi_taxonomy" in sql
    assert "addr.npi" in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_inferred_taxonomy_requires_individual_npi(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many",
        lambda *_a, **_k: {group_id: (provider_set_id,)},
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "lat": "34.14024131",
            "long": "-118.255125",
            "radius_miles": "10",
            "limit": "5",
            "code": "29888",
        },
        candidate_limit=5,
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "FROM mrf.npi_taxonomy nt WHERE nt.npi = addr.npi" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_compact_serving_include_providers_expands_without_geo_filter(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(
        [
            "mrf.npi",
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "loc-1",
                        "state": "IL",
                        "city": "Peoria",
                        "zip5": "61636",
                        "location_source": "nppes",
                        "location_confidence_code": "nppes_practice_location",
                        "address_payload": {
                            "address_key": "00000000-0000-0000-0000-000000000002",
                            "line1": "1 Main St",
                        },
                        "taxonomy_codes": [],
                        "specialties": [],
                        "provider_name": "Example Provider",
                        "procedure_code": None,
                        "reported_code_system": "RC",
                        "reported_code": "450",
                        "billing_code": "450",
                        "billing_code_type": "RC",
                        "procedure_display_name": "Emergency Room",
                        "procedure_name": "Emergency Room",
                        "procedure_description": "Emergency Room",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "percentage", "negotiated_rate": 60}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["query"]["result_granularity"] == "provider"
    assert payload["query"]["include_providers"] is True
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["provider_name"] == "Example Provider"
    assert item["state"] == "IL"
    assert item["address"]["address_key"] == "00000000-0000-0000-0000-000000000002"
    assert item["tic_prices"][0]["negotiated_rate"] == 60
    sql = str(session.calls[-1][0][0])
    assert "LEFT JOIN LATERAL (" in sql
    assert "FROM mrf.npi_address addr" in sql
    assert "addr.npi = pgm.npi" in sql
    assert "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5" not in sql


@pytest.mark.asyncio
async def test_compact_serving_geo_filter_uses_unified_address_table_when_compatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "lat": "29.7604", "long": "-95.3698", "radius_miles": "10"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[1][0][0])
    assert "JOIN mrf.entity_address_unified addr_filter" in sql
    assert "JOIN mrf.npi_address addr_filter" not in sql
    assert "addr_filter.address_precision" in sql
    assert "COALESCE(addr_filter.address_precision, '') <> 'city_zip'" in sql


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_uses_unified_address_table_when_compatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "FROM mrf.entity_address_unified addr" in sql
    assert "FROM mrf.npi_address addr" not in sql
    assert "'entity_address_unified' AS location_source" in sql
    assert "'entity_address_unified' AS location_confidence_code" in sql
    assert "(to_jsonb(addr.*) - 'premise_key') AS address_payload" in sql
    assert "addr.npi = pgm.npi" in sql
    assert "addr.npi = sp.npi" not in sql
    # Provider name is resolved from the canonical NPI table (mrf.npi), never
    # left as the NULL/"TiC provider" placeholder, and street-bearing address
    # rows are preferred so city/zip-only unified rows don't hide the street.
    assert "LEFT JOIN mrf.npi n ON n.npi = pgm.npi" in sql
    assert "NULL::varchar AS provider_name" not in sql
    assert "n.provider_organization_name" in sql
    assert "NULLIF(BTRIM(addr.first_line), '') IS NULL" in sql


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_uses_placeholder_without_npi_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            FakeResult(scalar=None),
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "LEFT JOIN mrf.npi n ON n.npi = pgm.npi" not in sql
    assert "'TiC provider' AS provider_name" in sql


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_falls_back_when_unified_incompatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[("npi",), ("type",)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "FROM mrf.npi_address addr" in sql
    assert "FROM mrf.entity_address_unified addr" not in sql


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_provider_expansion_uses_direct_component_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(["mrf.npi", FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "010854205", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "JOIN mrf.ptg2_provider_set_component_token psc" in sql
    assert "ON psc.provider_set_hash = r.provider_set_hash" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm" in sql
    assert "ON pgm.provider_group_hash = psc.provider_group_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "provider_group_hashes" not in sql


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_uses_primary_taxonomy_codes_without_geo():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "specialty": "Family Medicine"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "FROM mrf.ptg2_provider_set_component_token psc_filter" in sql
    assert "pgm_filter.provider_group_hash = psc_filter.provider_group_hash" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "LOWER(COALESCE" not in sql
    assert "nucc_filter.display_name" not in sql
    assert "mrf.npi_taxonomy provider_specialty_nt" in sql
    assert "provider_specialty_nt.npi = pgm_filter.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert params["provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_filters_minimal_provider_group_layout():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(provider_set_component_table=None, provider_group_location_table=None),
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "99214",
            "specialty": "primary care",
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    params = session.calls[-1][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "ptg2_provider_set_component_token" not in sql
    assert "FROM mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "pgm_filter.provider_group_hash = r.provider_set_hash" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm" in sql
    assert "ON pgm.provider_group_hash = r.provider_set_hash" in sql
    assert "provider_specialty_nt.npi = pgm_filter.npi" in sql
    assert "provider_expansion_specialty_nt.npi = pgm.npi" in sql
    assert "LEFT JOIN LATERAL" in sql
    assert "tax.taxonomy_codes" in sql
    assert "363A00000X" not in params.values()
    assert set(
        value for key, value in params.items()
        if key.startswith("provider_specialty_taxonomy_code_")
    ) >= {"207Q00000X", "207R00000X", "208000000X", "208D00000X"}


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_geo_taxonomy_filter_uses_direct_component_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "zip5": "60601", "specialty": "dentist"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "FROM mrf.ptg2_provider_set_component_token psc_filter" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "mrf.npi_taxonomy provider_specialty_nt" in sql
    assert "provider_group_hashes" not in sql


@pytest.mark.asyncio
async def test_compact_serving_include_providers_with_geo_uses_npi_scoped_location_lookup():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "TX",
                        "city": "HOUSTON",
                        "zip5": "77030",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {
                            "address_key": "00000000-0000-0000-0000-000000000003",
                            "city": "HOUSTON",
                            "state": "TX",
                            "postal_code": "77030",
                        },
                        "taxonomy_codes": ["207Q00000X"],
                        "specialties": ["Family Medicine Physician"],
                        "classifications": ["Family Medicine"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Family Medicine Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "Example Provider",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "billing_code": "99213",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "Office visit",
                        "procedure_name": "Office visit",
                        "procedure_description": "Office visit",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "derived", "negotiated_rate": 86.48}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "99213",
            "city": "Houston",
            "state": "TX",
            "specialty": "family",
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["query"]["result_granularity"] == "provider"
    assert payload["items"][0]["location_source"] == "npi_address"
    assert payload["items"][0]["address"]["address_key"] == "00000000-0000-0000-0000-000000000003"
    assert payload["items"][0]["specialties"] == ["Family Medicine Physician"]
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    assert payload["items"][0]["specializations"] == ["Sports Medicine"]
    assert payload["items"][0]["classifications"] == ["Family Medicine"]
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "WITH rate_candidates AS MATERIALIZED" in sql
    assert "filtered_locations AS MATERIALIZED" in sql
    assert "JOIN LATERAL (" in sql
    assert "FROM mrf.ptg2_provider_group_location_token loc" in sql
    assert "JOIN filtered_locations loc" in sql
    assert "loc.npi" in sql
    assert "AND EXISTS (" in sql
    assert "OFFSET 0" in sql
    assert "COALESCE(tax.specializations, loc.specializations" not in sql
    assert "COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations" in sql
    assert "array_remove(array_agg(NULLIF(nucc.specialization, '')" in sql
    assert "FROM mrf.npi_address addr" not in sql
    assert "JOIN mrf.npi_address addr_filter" not in sql
    assert params["city_exact"] == "HOUSTON"
    assert params["provider_match_limit"] >= 64
    assert params["location_rate_candidate_limit"] >= 4096


@pytest.mark.asyncio
async def test_compact_serving_geo_provider_filter_paginates_after_provider_match():
    class LimitOnePagination:
        limit = 1
        offset = 0

    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "IL",
                        "city": "EFFINGHAM",
                        "zip5": "62401",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {"address_key": "addr-1"},
                        "taxonomy_codes": ["207X00000X"],
                        "specialties": ["Orthopaedic Surgery Physician"],
                        "classifications": ["Orthopaedic Surgery"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Orthopaedic Surgery Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "ACL Surgeon",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "29888",
                        "billing_code": "29888",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "ACL reconstruction",
                        "procedure_name": "ACL reconstruction",
                        "procedure_description": "ACL reconstruction",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 904.61}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {
            "plan_id": "010854205",
            "code": "29888",
            "zip5": "62401",
            "lat": "39.11952",
            "long": "-88.56418",
            "radius_miles": "10",
            "include_providers": "true",
        },
        LimitOnePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 1, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["items"][0]["provider_name"] == "ACL Surgeon"
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "WITH rate_candidates AS MATERIALIZED" in sql
    assert "LIMIT :rate_candidate_limit" in sql
    assert "LIMIT :rate_candidate_limit OFFSET" not in sql
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert sql.rstrip().endswith("LIMIT :limit OFFSET :offset")
    assert "n_entity.entity_type_code" in sql
    assert params["limit"] == 1
    assert params["rate_candidate_limit"] > params["limit"]


@pytest.mark.asyncio
async def test_compact_serving_infers_radiology_taxonomy_for_radiology_cpt_geo_lookup():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "MA",
                        "city": "BOSTON",
                        "zip5": "02118",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {"city": "BOSTON", "state": "MA", "postal_code": "02118"},
                        "taxonomy_codes": ["2085R0202X"],
                        "specialties": ["Diagnostic Radiology Physician"],
                        "provider_name": "Radiology Provider",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "procedure_name": "MRI brain",
                        "procedure_description": "MRI brain",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "derived", "negotiated_rate": 86.48}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {
            "plan_id": "823166837",
            "code": "70551",
            "code_system": "CPT",
            "city": "Boston",
            "state": "MA",
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "823166837", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_EXACT_SOURCE,
    )

    assert payload["items"][0]["specialties"] == ["Diagnostic Radiology Physician"]
    sql = str(session.calls[0][0][0])
    assert "nt.healthcare_provider_taxonomy_code IN" in sql
    assert "inferred_taxonomy_code_" in sql
    params = session.calls[0][0][1]
    assert "2085R0202X" in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }
    assert "2084D0003X" in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }
    assert "JOIN mrf.nucc_taxonomy nucc\n                            ON nucc.code" not in sql
    assert "2085R0001X" not in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }


@pytest.mark.parametrize(
    ("code", "expected_code", "expected_term"),
    [
        ("00100", "207L00000X", "anesthesiology"),
        ("80053", "291U00000X", "clinical medical laboratory"),
        ("97140", "225100000X", "physical therapist"),
        ("66984", "207W00000X", "ophthalmology"),
        ("45378", "207RG0100X", "gastroenterology"),
        ("99285", "207P00000X", "emergency medicine"),
        ("77301", "2085R0001X", "radiation oncology"),
    ],
)
def test_inferred_provider_taxonomy_sql_for_high_confidence_cpt_families(
    code: str,
    expected_code: str,
    expected_term: str,
):
    sql = ptg2_serving._inferred_provider_taxonomy_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        nucc_alias="nucc",
    )

    assert expected_code in sql
    assert f"%{expected_term}%" in sql


@pytest.mark.parametrize("code", ["99213", "99203", "93000"])
def test_inferred_provider_taxonomy_sql_ignores_mixed_use_cpt_families(code: str):
    sql = ptg2_serving._inferred_provider_taxonomy_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        nucc_alias="nucc",
    )

    assert sql == ""


def test_warm_cache_benchmark_fixture_p95_gate():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    result = ptg2_serving.warm_cache_benchmark(index, request_count=100)

    assert result["request_count"] == 100
    assert result["p95_ms"] <= 50.0
    assert result["passed"] is True
