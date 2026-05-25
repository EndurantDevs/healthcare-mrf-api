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
    assert context["internal_codes"] == []


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
async def test_compact_serving_geo_search_allows_missing_specialty():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "zip5": "60601"},
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
async def test_compact_serving_coordinate_search_filters_npi_addresses():
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
    assert ") <= :geo_radius_miles" in sql
    assert params["geo_lat"] == 29.7604
    assert params["geo_long"] == -95.3698
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_include_providers_expands_without_geo_filter():
    session = FakeSession(
        [
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
                        "address_payload": {"line1": "1 Main St"},
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
    assert item["tic_prices"][0]["negotiated_rate"] == 60
    sql = str(session.calls[0][0][0])
    assert "LEFT JOIN LATERAL (" in sql
    assert "FROM mrf.npi_address addr" in sql
    assert "addr.npi = sp.npi" in sql
    assert "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5" not in sql


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_provider_expansion_uses_direct_component_table():
    session = FakeSession([FakeResult(rows=[])])
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
    sql = str(session.calls[0][0][0])
    assert "JOIN mrf.ptg2_provider_set_component_token psc" in sql
    assert "ON psc.provider_set_hash = r.provider_set_hash" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm" in sql
    assert "ON pgm.provider_group_hash = psc.provider_group_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "provider_group_hashes" not in sql


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_joins_nucc_without_geo():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "010854205", "code": "70551", "specialty": "dentist"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "010854205", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "JOIN mrf.npi_taxonomy nt_filter" in sql
    assert "JOIN mrf.nucc_taxonomy nucc_filter" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "FROM mrf.ptg2_provider_set_component_token psc_filter" in sql
    assert "pgm_filter.provider_group_hash = psc_filter.provider_group_hash" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "nt_filter.npi = pgm_filter.npi" in sql
    assert params["specialty_like"] == "%dentist%"


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_geo_taxonomy_filter_uses_direct_component_table():
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
    assert "JOIN mrf.npi_taxonomy nt_filter" in sql
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
                        "address_payload": {"city": "HOUSTON", "state": "TX", "postal_code": "77030"},
                        "taxonomy_codes": ["207Q00000X"],
                        "specialties": ["Family Medicine Physician"],
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
    assert payload["items"][0]["specialties"] == ["Family Medicine Physician"]
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "WITH rate_candidates AS MATERIALIZED" in sql
    assert "JOIN LATERAL (" in sql
    assert "JOIN mrf.ptg2_provider_group_location_token loc" in sql
    assert "loc.npi" in sql
    assert "AND EXISTS (" in sql
    assert "OFFSET 0" in sql
    assert "FROM mrf.npi_address addr" not in sql
    assert "JOIN mrf.npi_address addr_filter" not in sql
    assert params["city_exact"] == "HOUSTON"
    assert params["provider_match_limit"] >= 64
    assert params["location_rate_candidate_limit"] >= 4096


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
