# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import re
import types
import asyncio
from unittest.mock import AsyncMock

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module
from api.endpoint.npi import get_all


class RecordingConnection:
    def __init__(self):
        self.calls = 0
        self.last_sql = None
        self.last_params = None
        self.sql_calls = []

    async def all(self, sql, **params):
        sql_text = str(sql)
        self.calls += 1
        self.last_sql = sql_text
        self.last_params = params
        self.sql_calls.append((sql_text, dict(params)))
        if "SELECT COUNT(DISTINCT" in sql_text:
            return [(5,)]
        return []

    async def first(self, *_args, **_kwargs):
        return None


class CandidateLimitConnection(RecordingConnection):
    async def all(self, sql, **params):
        sql_text = str(sql)
        if "LIMIT :candidate_limit" in sql_text:
            assert "candidate_limit" in params
        self.calls += 1
        self.last_sql = sql_text
        self.last_params = params
        self.sql_calls.append((sql_text, dict(params)))
        if "SELECT COUNT(DISTINCT" in sql_text:
            return [(5,)]
        return []


class FakeAcquire:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_get_all_include_total_false_skips_count_query(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "limit": "5",
            "start": "10",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 10
    assert response_body["total_source"] == "estimated_page_floor"


@pytest.mark.asyncio
async def test_get_all_locator_defaults_to_no_total_query(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "phone": "(312) 555-1212",
            "limit": "5",
            "start": "10",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 10
    assert response_body["total_source"] == "estimated_page_floor"


@pytest.mark.asyncio
async def test_get_all_unified_phone_list_and_count_bind_candidate_limit(monkeypatch):
    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys() | {
                "address_precision",
                "location_key",
                "source_count",
                "updated_at",
                "zip5",
                "phone_number",
            }
        return set()

    conn = CandidateLimitConnection()
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    response = await get_all(
        types.SimpleNamespace(
            args={
                "phone": "8106526315",
                "limit": "5",
                "start": "0",
                "include_total": "1",
            }
        )
    )

    assert json.loads(response.body)["total"] == 5
    phone_candidate_calls = [
        params
        for sql, params in conn.sql_calls
        if "LIMIT :candidate_limit" in sql
    ]
    assert {params["candidate_limit"] for params in phone_candidate_calls} == {100, 500}



@pytest.mark.asyncio
async def test_get_all_sitemap_mode_allows_20000_limit(monkeypatch):
    class SitemapConnection:
        def __init__(self):
            self.calls = 0
            self.last_params = None
            self.last_sql = None

        async def all(self, sql, **params):
            self.calls += 1
            self.last_params = params
            self.last_sql = str(sql)
            return []

    conn = SitemapConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
    async def fake_npi_list(*_args, **_kwargs):
        return list(range(1_000_000_000, 1_000_030_000))
    monkeypatch.setattr(
        npi_module,
        "_get_classification_npi_list",
        fake_npi_list,
    )

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "view": "sitemap",
            "limit": "20000",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["limit"] == 20000
    assert conn.calls == 1
    assert len(conn.last_params["page_npis"]) == 20000
    assert "mrf.addr_formatted_address_v2(" in conn.last_sql
    assert "c.formatted_address" not in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_q_filter(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "classification": "Pharmacy",
            "state": "RI",
            "q": "CVS Pharmacy",
            "has_insurance": "1",
            "limit": "0",
            "start": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)
    assert response_body["total"] == 5  # count from first call
    assert response_body["page"] == 1
    assert response_body["limit"] == 50
    assert response_body["offset"] == 0
    data_calls = [
        (sql, params)
        for sql, params in conn.sql_calls
        if "COUNT(DISTINCT c.npi)" not in sql
    ]
    assert len(data_calls) == 1
    data_sql, data_params = data_calls[0]
    assert "b.npi" in data_sql
    assert data_params["name_like_0_0"] == "%cvs%"
    assert data_params["name_like_0_1"] == "%pharmacy%"
    assert data_params["limit"] > 0  # limit expanded from 0


@pytest.mark.asyncio
async def test_get_all_count_only_supports_response_format_alias(monkeypatch):
    class MappingConnection:
        async def all(self, *_args, **_kwargs):
            return [("taxonomy_a", 3), ("taxonomy_b", 7)]

        async def first(self, *_args, **_kwargs):
            return None

    conn = MappingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"count_only": "1", "response_format": "full_taxonomy"}
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)
    assert response_body["rows"]["taxonomy_a"] == 3
    assert response_body["rows"]["taxonomy_b"] == 7


@pytest.mark.asyncio
async def test_get_all_q_alias_matches_provider_name(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "Ryan James Pasiewicz",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)
    assert response_body["total"] == 5
    assert conn.last_params["name_like_0_0"] == "%ryan%"
    assert conn.last_params["name_like_0_1"] == "%james%"
    assert conn.last_params["name_like_0_2"] == "%pasiewicz%"
    assert "filtered_npi AS MATERIALIZED" in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_accepts_name_like_legacy_alias(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"name_like": "clinic", "limit": "5", "start": "0"})
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 0
    assert response_body["total_source"] == "estimated_timeout_floor"
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%clinic%"


@pytest.mark.asyncio
async def test_get_all_deduplicates_repeated_name_like_values_from_multidict(monkeypatch):
    class FakeArgs:
        def __init__(self):
            self._values = {
                "name_like": ["CVS", "cvs"],
                "limit": ["5"],
                "start": ["0"],
            }

        def get(self, key, default=None):
            values = self._values.get(key)
            if not values:
                return default
            return values[-1]

        def getlist(self, key):
            return list(self._values.get(key, []))

    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args=FakeArgs())
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 0
    assert response_body["total_source"] == "estimated_timeout_floor"
    assert "name_like_0" in conn.last_params
    assert "name_like_1" not in conn.last_params
    assert conn.last_params["name_like_0"] == "%cvs%"
    assert "name_like_1" not in conn.last_sql


@pytest.mark.asyncio
async def test_get_all_deduplicates_q_and_legacy_name_like(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"q": "clinic", "name_like": "CLINIC", "limit": "5", "start": "0"}
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 0
    assert response_body["total_source"] == "estimated_timeout_floor"
    assert "name_like_0" in conn.last_params
    assert "name_like_1" not in conn.last_params
    assert conn.last_params["name_like_0"] == "%clinic%"


@pytest.mark.asyncio
async def test_get_all_broad_q_explicit_include_total_still_skips_slow_count(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"q": "silver", "limit": "5", "start": "0", "include_total": "true"}
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert conn.calls == 1
    assert response_body["total"] == 0
    assert response_body["total_source"] == "estimated_timeout_floor"
    assert "name_like_0" in conn.last_params
    assert conn.last_params["name_like_0"] == "%silver%"


@pytest.mark.asyncio
async def test_get_all_without_taxonomy_filters_skips_expensive_overlap(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "clinic",
            "city": "chicago",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.sql_calls
    assert all("taxonomy_array && q.int_codes" not in sql for sql, _ in conn.sql_calls)


@pytest.mark.asyncio
async def test_get_all_zip_taxonomy_uses_location_first_probe(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "zip_code": "60601",
            "codes": "207Q00000X",
            "provider_sex_code": "f",
            "include_total": "false",
            "limit": "10",
            "start": "0",
        }
    )
    await get_all(request)

    page_sql = conn.sql_calls[-1][0]
    assert "JOIN LATERAL" in page_sql
    assert "FROM mrf.npi_taxonomy AS provider_taxonomy" in page_sql
    assert (
        "provider_taxonomy.healthcare_provider_taxonomy_code "
        "IN (:page_provider_taxonomy_code_0)"
    ) in page_sql
    assert "c.taxonomy_array && q.int_codes" not in page_sql
    assert "sex_provider.provider_sex_code = :provider_sex_code" in page_sql
    assert conn.sql_calls[-1][1]["page_provider_taxonomy_code_0"] == "207Q00000X"


@pytest.mark.asyncio
async def test_get_all_name_taxonomy_materializes_the_name_driven_taxonomy_match(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "clinic",
            "codes": "207Q00000X",
            "include_total": "false",
            "limit": "10",
            "start": "0",
        }
    )
    await get_all(request)

    page_sql = conn.sql_calls[-1][0]
    assert "filtered_npi AS MATERIALIZED" in page_sql
    assert "taxonomy_matched_npi AS MATERIALIZED" in page_sql
    assert "JOIN mrf.npi_taxonomy AS provider_taxonomy" in page_sql
    assert "FROM mrf.nucc_taxonomy" not in page_sql
    assert (
        "provider_taxonomy.healthcare_provider_taxonomy_code "
        "IN (:page_name_provider_taxonomy_code_0)"
    ) in page_sql
    assert conn.sql_calls[-1][1]["page_name_provider_taxonomy_code_0"] == "207Q00000X"
    assert "AS provider_taxonomy_match ON TRUE" not in page_sql
    assert "c.taxonomy_array && q.int_codes" not in page_sql
    assert page_sql.rstrip().endswith("ORDER BY sub_s.npi_code ASC;")


@pytest.mark.asyncio
async def test_get_all_name_taxonomy_count_closes_the_cte_list(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    await get_all(
        types.SimpleNamespace(
            args={
                "q": "clinic",
                "codes": "207Q00000X",
                "include_total": "true",
                "limit": "10",
                "start": "0",
            }
        )
    )

    count_sql = next(
        sql for sql, _params in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql
    )
    assert "taxonomy_matched_npi AS MATERIALIZED" in count_sql
    assert "FROM mrf.nucc_taxonomy" not in count_sql
    assert (
        "provider_taxonomy.healthcare_provider_taxonomy_code "
        "IN (:count_name_provider_taxonomy_code_0)"
    ) in count_sql
    assert not re.search(r"\)\s*,\s*SELECT\s+COUNT", count_sql, flags=re.IGNORECASE)


@pytest.mark.asyncio
async def test_get_all_name_taxonomy_unified_join_matches_serving_index(monkeypatch):
    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys() | {
                "address_precision",
                "location_key",
                "source_count",
                "updated_at",
                "zip5",
                "phone_number",
            }
        return set()

    conn = RecordingConnection()
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    await get_all(
        types.SimpleNamespace(
            args={
                "q": "clinic",
                "codes": "207Q00000X",
                "include_total": "true",
                "limit": "10",
                "start": "0",
            }
        )
    )

    page_sql = next(sql for sql, _params in conn.sql_calls if "page_npis AS" in sql)
    count_sql = next(
        sql for sql, _params in conn.sql_calls if "SELECT COUNT(DISTINCT" in sql
    )
    expected_join = (
        "join mrf.entity_address_unified as c "
        "on coalesce(c.npi, c.inferred_npi) = fn.npi"
    )
    for query_sql in (count_sql, page_sql):
        normalized_sql = " ".join(query_sql.lower().split())
        assert "taxonomy_matched_npi as materialized" in normalized_sql
        assert expected_join in normalized_sql
        assert "as provider_taxonomy_match on true" not in normalized_sql

    assert {
        "index_elements": ("coalesce(npi, inferred_npi)",),
        "name": "coalesced_npi",
    } in npi_module.EntityAddressUnified.__my_additional_indexes__


@pytest.mark.asyncio
async def test_get_all_zip_phone_and_name_filters_are_applied(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601-1234",
            "phone": "(312) 555-1212",
            "first_name": "john",
            "last_name": "doe",
            "organization_name": "walgreens",
            "address_key": "3A47E595-03BF-597D-8825-04B75D783FC9",
            "entity_type_code": "2",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)

    assert conn.last_params["zip_code"] == "60601"
    assert conn.last_params["phone_digits"] == "3125551212"
    assert conn.last_params["address_key"] == "3a47e595-03bf-597d-8825-04b75d783fc9"
    assert conn.last_params["first_name"] == "%john%"
    assert conn.last_params["last_name"] == "%doe%"
    assert conn.last_params["organization_name"] == "%walgreens%"
    assert conn.last_params["entity_type_code"] == 2
    assert "LEFT(c.postal_code, 5) = :zip_code" in conn.last_sql
    assert "regexp_replace(COALESCE(c.telephone_number, ''), '[^0-9]', '', 'g') = :phone_digits" in conn.last_sql
    assert "c.address_key = CAST(:address_key AS uuid)" in conn.last_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("raw_code,expected_code", [("m", "M"), ("F", "F"), ("u", "U"), ("X", "X")])
async def test_get_all_applies_provider_sex_before_pagination(
    monkeypatch,
    raw_code,
    expected_code,
):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    response = await get_all(
        types.SimpleNamespace(
            args={
                "city": "Chicago",
                "provider_sex_code": raw_code,
                "limit": "5",
                "start": "0",
                "include_total": "0",
            }
        )
    )

    assert json.loads(response.body)["total_source"] == "estimated_page_floor"
    page_sql, page_params = next(
        (sql, params)
        for sql, params in conn.sql_calls
        if "page_npis AS" in sql
    )
    sex_predicate = "sex_provider.provider_sex_code = :provider_sex_code"
    assert sex_predicate in page_sql
    assert page_sql.index(sex_predicate) < page_sql.index("LIMIT :limit")
    assert page_params["provider_sex_code"] == expected_code


@pytest.mark.asyncio
async def test_get_all_provider_sex_filter_is_used_for_count_and_rows(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    await get_all(
        types.SimpleNamespace(
            args={
                "classification": "Pharmacy",
                "provider_sex_code": "F",
                "limit": "5",
                "include_total": "1",
            }
        )
    )

    sex_predicate = "sex_provider.provider_sex_code = :provider_sex_code"
    assert len(conn.sql_calls) == 2
    assert all(sex_predicate in sql for sql, _params in conn.sql_calls)
    assert all(
        params["provider_sex_code"] == "F"
        for _sql, params in conn.sql_calls
    )


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_provider_sex_code():
    with pytest.raises(
        sanic.exceptions.InvalidUsage,
        match="provider_sex_code must be one of",
    ):
        await get_all(
            types.SimpleNamespace(
                args={"city": "Chicago", "provider_sex_code": "female"}
            )
        )


@pytest.mark.asyncio
async def test_get_all_rejects_provider_sex_for_organizations():
    with pytest.raises(
        sanic.exceptions.InvalidUsage,
        match="cannot be combined with entity_type_code=2",
    ):
        await get_all(
            types.SimpleNamespace(
                args={
                    "city": "Chicago",
                    "entity_type_code": "2",
                    "provider_sex_code": "F",
                }
            )
        )


def test_provider_list_address_type_clause_keeps_normal_lists_primary_only():
    clause = npi_module._provider_list_address_type_clause(
        "c",
        "mrf.entity_address_unified",
        include_service_locations=False,
    )

    assert clause == "c.type = 'primary'"


def test_provider_list_address_type_clause_allows_exact_service_locations():
    clause = npi_module._provider_list_address_type_clause(
        "c",
        "mrf.entity_address_unified",
        include_service_locations=True,
    )

    assert clause == "c.type IN ('primary', 'secondary', 'practice', 'site')"
    assert "mail" not in clause


@pytest.mark.asyncio
async def test_get_all_unified_pages_distinct_npis_and_uses_zip5(monkeypatch):
    conn = RecordingConnection()

    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys() | {
                "address_precision",
                "location_key",
                "source_count",
                "updated_at",
                "zip5",
                "phone_number",
            }
        return set()

    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "zip_code": "60601-1234",
            "phone": "(312) 555-1212",
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    await get_all(request)

    page_sql = next(sql for sql, _params in conn.sql_calls if "page_npis AS" in sql)
    assert "FROM mrf.entity_address_unified as c" in page_sql
    assert "page_npis AS" in page_sql
    assert "SELECT DISTINCT COALESCE(c.npi, c.inferred_npi) AS npi" in page_sql
    assert "JOIN LATERAL" in page_sql
    assert "AND COALESCE(c.npi, c.inferred_npi) = pn.npi" in page_sql
    assert "c.type IN" in page_sql
    assert all(
        f"'{location_type}'" in page_sql
        for location_type in ("primary", "secondary", "practice", "site")
    )
    assert "c.zip5 = :zip_code" in page_sql
    assert "phone_candidates AS MATERIALIZED" in page_sql
    assert "provider_directory_address_overlay AS overlay" in page_sql
    assert "provider_directory_endpoint_dataset AS dataset" in page_sql
    assert "dataset.is_current IS TRUE" in page_sql
    assert "COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)" in page_sql
    assert "current_run.run_id = overlay.last_seen_run_id" in page_sql
    assert "JOIN phone_candidates AS phone_match" in page_sql
    assert "c.type = 'primary'" not in page_sql
    assert "LEFT(c.postal_code, 5) = :zip_code" not in page_sql

    assert "fallback_npis AS" not in page_sql
    assert "SELECT c.checksum" in page_sql


@pytest.mark.asyncio
async def test_get_all_unified_phone_facet_counts_include_service_locations(monkeypatch):
    class FacetConnection:
        def __init__(self):
            self.sql_calls = []
            self.last_sql = None
            self.last_params = None

        async def all(self, sql, **params):
            self.last_sql = str(sql)
            self.last_params = params
            self.sql_calls.append((str(sql), dict(params)))
            return [("Pharmacy", 1)]

        async def first(self, *_args, **_kwargs):
            return None

    async def fake_table_columns(table_name, *, session=None):
        assert session is None
        if table_name == "entity_address_unified":
            return npi_module._public_address_serving_column_keys() | {
                "address_precision",
                "location_key",
                "source_count",
                "updated_at",
                "zip5",
                "phone_number",
            }
        return set()

    conn = FacetConnection()
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", fake_table_columns)
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "phone": "(312) 555-1212",
            "count_only": "1",
            "response_format": "all",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["rows"] == {"Pharmacy": 1}
    assert conn.last_params["phone_digits"] == "3125551212"
    assert conn.last_params["candidate_limit"] == 500
    assert "FROM mrf.entity_address_unified AS c" in conn.last_sql
    assert "c.type IN ('primary', 'secondary', 'practice', 'site')" in conn.last_sql
    assert "phone_candidates AS MATERIALIZED" in conn.last_sql
    assert "provider_directory_address_overlay AS overlay" in conn.last_sql
    assert "JOIN phone_candidates AS phone_match" in conn.last_sql


def _provider_directory_fallback_record(*, premise_key=None):
    provider_record_map = {
        "npi_code": 1000000049,
        "npi": 1000000049,
        "inferred_npi": None,
        "entity_name": "SYNTHETIC PROVIDER ORGANIZATION",
        "type": "practice",
        "first_line": "115 Example Court",
        "second_line": "Suite 100" if premise_key else None,
        "city_name": "Example City",
        "state_name": "AL",
        "postal_code": "35401",
        "country_code": "US",
        "telephone_number": "+12053663010",
        "phone_number": "2053663010",
        "address_key": "00000000-0000-0000-0000-000000000001"
        if premise_key
        else "e4cd3105-5ce1-efd3-3f31-d48bfa864a13",
        "location_key": "00000000-0000-0000-0000-000000000011",
        "address_sources": ["provider_directory_fhir"],
        "source_record_ids": [
            "provider_directory_fhir:practitioner_role:synthetic_directory:role-1:loc-1"
        ],
        "source_count": 1,
        "taxonomy_array": [],
        "plans_network_array": [],
        "procedures_array": [],
        "medications_array": [],
    }
    if premise_key:
        provider_record_map["premise_key"] = premise_key
    return provider_record_map


class _ProviderDirectoryFallbackConnection:
    def __init__(self, row):
        self.row = row
        self.sql_calls = []

    async def all(self, sql, **params):
        sql_text = str(sql)
        self.sql_calls.append((sql_text, dict(params)))
        if "page_npis AS" in sql_text:
            return [_MappedProviderDirectoryRow(self.row)]
        return [self.row] if "SELECT c.*" in sql_text else []

    async def first(self, *_args, **_kwargs):
        return None


async def _provider_directory_fallback_columns(table_name, *, session=None):
    assert session is None
    if table_name != "entity_address_unified":
        return set()
    return npi_module._public_address_serving_column_keys() | {
        "address_precision",
        "location_key",
        "premise_key",
        "source_count",
        "updated_at",
        "zip5",
        "phone_number",
        "inferred_npi",
    }


async def _empty_provider_directory_enrichment(*_args, **_kwargs):
    return {}


def _install_provider_directory_fallback(monkeypatch, *, premise_key=None):
    connection = _ProviderDirectoryFallbackConnection(
        _provider_directory_fallback_record(premise_key=premise_key)
    )
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        _provider_directory_fallback_columns,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        _empty_provider_directory_enrichment,
    )
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: FakeAcquire(connection),
    )
    return connection


@pytest.mark.asyncio
async def test_get_all_unified_phone_lookup_returns_provider_directory_only_row(monkeypatch):
    """Verify get all unified phone lookup returns provider directory only row."""
    conn = _install_provider_directory_fallback(monkeypatch)

    request = types.SimpleNamespace(
        args={
            "phone": "2053663010",
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["total"] == 1
    assert response_body["total_source"] == "estimated_page_floor"
    assert len(response_body["rows"]) == 1
    provider_record_map = response_body["rows"][0]
    assert provider_record_map["npi"] == 1000000049
    assert provider_record_map["provider_organization_name"] == "SYNTHETIC PROVIDER ORGANIZATION"
    assert provider_record_map["entity_type_code"] == 1
    assert provider_record_map["phone_number"] == "2053663010"
    assert provider_record_map["address_key"] == "e4cd3105-5ce1-efd3-3f31-d48bfa864a13"
    assert provider_record_map["address_sources"] == ["provider_directory_fhir"]
    assert provider_record_map["taxonomy_list"] == []
    assert "source_record_ids" not in provider_record_map
    page_sql = next(sql for sql, _params in conn.sql_calls if "page_npis AS" in sql)
    assert "c.type IN ('primary', 'secondary', 'practice', 'site')" in page_sql
    assert "provider_directory_address_overlay AS overlay" in page_sql
    assert "dataset.status = 'published'" in page_sql
    assert any(params.get("phone_digits") == "2053663010" for _sql, params in conn.sql_calls)
    page_params = next(params for sql, params in conn.sql_calls if "page_npis AS" in sql)
    assert page_params["candidate_limit"] == 100


def test_provider_list_phone_candidate_limit_covers_page_window_and_stays_bounded():
    assert npi_module._provider_list_phone_candidate_limit(5) == 100
    assert npi_module._provider_list_phone_candidate_limit(20, 20) == 320
    assert npi_module._provider_list_phone_candidate_limit(200, 200) == 500
    assert (
        npi_module._provider_list_phone_candidate_limit(5, count_query=True)
        == 500
    )


@pytest.mark.asyncio
async def test_get_all_unified_site_key_returns_provider_directory_row(monkeypatch):
    """Verify get all unified address site key lookup returns provider directory only row."""
    address_site_key = "00000000-0000-0000-0000-000000000002"
    conn = _install_provider_directory_fallback(
        monkeypatch,
        premise_key=address_site_key,
    )

    request = types.SimpleNamespace(
        args={
            "address_site_key": address_site_key.upper(),
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["total"] == 1
    assert response_body["total_source"] == "estimated_page_floor"
    assert len(response_body["rows"]) == 1
    provider_record_map = response_body["rows"][0]
    assert provider_record_map["npi"] == 1000000049
    assert provider_record_map["address_key"] == "00000000-0000-0000-0000-000000000001"
    assert provider_record_map["address_site_key"] == address_site_key
    assert "premise_key" not in provider_record_map
    page_sql = next(sql for sql, _params in conn.sql_calls if "page_npis AS" in sql)
    hydration_sql = next(sql for sql, _params in conn.sql_calls if "SELECT c.*" in sql)
    assert "FROM mrf.entity_address_unified as c" in page_sql
    assert "c.type IN ('primary', 'secondary', 'practice', 'site')" in page_sql
    assert "c.premise_key = CAST(:address_site_key AS uuid)" in page_sql
    assert "c.location_key = ANY(:location_keys)" in hydration_sql
    assert any(params.get("address_site_key") == address_site_key for _sql, params in conn.sql_calls)


@pytest.mark.asyncio
async def test_get_all_unified_exact_npi_lookup_returns_provider_directory_only_row(monkeypatch):
    """Verify get all unified exact npi lookup returns provider directory only row."""
    conn = _install_provider_directory_fallback(monkeypatch)

    request = types.SimpleNamespace(
        args={
            "npi": "1000000049",
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["total"] == 1
    assert len(response_body["rows"]) == 1
    provider_record_map = response_body["rows"][0]
    assert provider_record_map["npi"] == 1000000049
    assert provider_record_map["provider_organization_name"] == "SYNTHETIC PROVIDER ORGANIZATION"
    assert provider_record_map["address_sources"] == ["provider_directory_fhir"]
    assert "source_record_ids" not in provider_record_map

    page_sql = next(sql for sql, _params in conn.sql_calls if "page_npis AS" in sql)
    assert "COALESCE(c.npi, c.inferred_npi) = :npi_filter" in page_sql
    assert "c.type IN ('primary', 'secondary', 'practice', 'site')" in page_sql
    assert any(params.get("npi_filter") == 1000000049 for _sql, params in conn.sql_calls)


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_npi_filter(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"npi": "abc"})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_unified_exact_lookup_can_include_provider_directory_source_summary(monkeypatch):
    """Verify get all unified exact lookup can include provider directory source summary."""
    async def fake_source_detail_map(source_ids, **_kwargs):
        assert source_ids == ["synthetic_directory"]
        return {
            "synthetic_directory": {
                "source": "provider_directory_fhir",
                "source_id": "synthetic_directory",
                "endpoint_id": "synthetic_endpoint",
                "canonical_api_base": "https://fhir.example.invalid/provider-directory",
                "org_name": "Synthetic Health Plan",
                "plan_name": "Provider Directory",
            }
        }

    conn = _install_provider_directory_fallback(monkeypatch)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_source_detail_map", fake_source_detail_map)

    request = types.SimpleNamespace(
        args={
            "phone": "2053663010",
            "address_key": "e4cd3105-5ce1-efd3-3f31-d48bfa864a13",
            "include_sources": "true",
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    resp = await get_all(request)
    provider_record_map = json.loads(resp.body)["rows"][0]
    assert conn.sql_calls

    assert provider_record_map["provider_directory_sources"] == [
            {
                "source": "provider_directory_fhir",
                "source_ids": ["synthetic_directory"],
                "endpoint_id": "synthetic_endpoint",
                "catalog_aliases_verified": False,
            "catalog_aliases": [
                {
                    "source_id": "synthetic_directory",
                    "org_name": "Synthetic Health Plan",
                    "plan_name": "Provider Directory",
                }
            ],
        }
    ]
    assert "source_record_ids" not in provider_record_map


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_entity_type_code(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(args={"q": "walgreen", "entity_type_code": "7"})
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_rejects_conflicting_zip_aliases(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "q": "walgreen",
            "zip_code": "60601",
            "postal_code": "60602",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_applies_procedure_and_medication_all_match_filters(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        lambda: asyncio.sleep(0, result={
            "npi_procedures_array_available": True,
            "npi_medications_array_available": True,
            "pricing_provider_procedure_available": False,
            "pricing_provider_prescription_available": False,
        }),
    )

    request = types.SimpleNamespace(
        args={
            "procedure_codes": "1001,1002",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001,2002",
            "medication_code_system": "HP_RX_CODE",
            "year": "2023",
            "limit": "5",
            "start": "0",
        }
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["total"] == 5
    assert "c.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" in conn.last_sql
    assert "c.procedures_array @> ARRAY[:procedure_code_1]::INTEGER[]" in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_1]::INTEGER[]" in conn.last_sql
    assert conn.last_params["procedure_code_0"] == 1001
    assert conn.last_params["procedure_code_1"] == 1002
    assert conn.last_params["medication_code_0"] == 2001
    assert conn.last_params["medication_code_1"] == 2002
    assert conn.last_params["filter_year"] == 2023


@pytest.mark.asyncio
async def test_get_all_rejects_invalid_procedure_code_system(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "procedure_codes": "1001",
            "procedure_code_system": "LOINC",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_rejects_non_numeric_internal_medication_codes(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={
            "medication_codes": "ABC123",
            "medication_code_system": "HP_RX_CODE",
        }
    )
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await get_all(request)


@pytest.mark.asyncio
async def test_get_all_falls_back_to_exists_when_arrays_unavailable(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))
    monkeypatch.setattr(
        npi_module,
        "_resolve_npi_filter_capabilities",
        lambda: asyncio.sleep(0, result={
            "npi_procedures_array_available": False,
            "npi_medications_array_available": False,
            "pricing_provider_procedure_available": True,
            "pricing_provider_prescription_available": True,
        }),
    )

    request = types.SimpleNamespace(
        args={
            "procedure_codes": "1001",
            "procedure_code_system": "HP_PROCEDURE_CODE",
            "medication_codes": "2001",
            "medication_code_system": "HP_RX_CODE",
            "limit": "5",
            "start": "0",
        }
    )
    await get_all(request)
    assert "EXISTS (SELECT 1 FROM mrf.pricing_provider_procedure" in conn.last_sql
    assert "EXISTS (SELECT 1 FROM mrf.pricing_provider_prescription" in conn.last_sql
    assert "c.procedures_array @> ARRAY[:procedure_code_0]::INTEGER[]" not in conn.last_sql
    assert "c.medications_array @> ARRAY[:medication_code_0]::INTEGER[]" not in conn.last_sql


def test_names_like_filter_clause_default_like_only(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", False)
    clause, params = npi_module._names_like_filter_clause("d", ["cvs"])
    assert " % :" not in clause
    assert "name_like_0" in params
    assert "name_like_0_fuzzy" not in params


def test_names_like_filter_clause_tokenizes_punctuation_as_and_terms(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", False)

    clause, params = npi_module._names_like_filter_clause(
        "d",
        ["  Smith,   Adam  "],
    )

    assert params == {
        "name_like_0_0": "%smith%",
        "name_like_0_1": "%adam%",
    }
    assert " OR " not in clause
    assert " AND " in clause
    assert ":name_like_0_0" in clause
    assert ":name_like_0_1" in clause


@pytest.mark.asyncio
async def test_get_all_relevance_order_is_additive_and_stably_tied_by_npi(monkeypatch):
    conn = RecordingConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    await get_all(
        types.SimpleNamespace(
            args={
                "q": "Smith, Adam",
                "order_by": "relevance",
                "include_total": "false",
                "limit": "10",
            }
        )
    )

    page_sql = next(
        sql_text
        for sql_text, _params in conn.sql_calls
        if "page_npis AS" in sql_text
    )
    assert "similarity(" in page_sql
    assert "ORDER BY relevance_score DESC, npi ASC" in page_sql
    page_params = next(
        params
        for sql_text, params in conn.sql_calls
        if "page_npis AS" in sql_text
    )
    assert page_params["relevance_q"] == "smith adam"


def test_names_like_filter_clause_can_enable_trgm_fuzzy(monkeypatch):
    monkeypatch.setattr(npi_module, "ENABLE_TRGM_FUZZY_NAME_SEARCH", True)
    clause, params = npi_module._names_like_filter_clause("d", ["cvs"])
    assert " % :name_like_0_fuzzy" in clause
    assert params["name_like_0"] == "%cvs%"
    assert params["name_like_0_fuzzy"] == "cvs"


@pytest.mark.asyncio
async def test_get_all_handles_sparse_positional_rows_without_crashing(monkeypatch):
    class SparseResultConnection:
        def __init__(self):
            self.calls = 0

        async def all(self, sql, **_kwargs):
            self.calls += 1
            if "COUNT(DISTINCT c.npi)" in str(sql):
                return [(1,)]

            # Simulate legacy/sparse row shape where address payload has fewer
            # columns than model metadata expects (e.g. missing optional arrays).
            # The handler must not crash with tuple index errors.
            row = [None] * 66
            row[1] = 1234567890  # b.npi (after npi_code)
            row[2] = "00-0000000"  # employer_identification_number
            row[3] = 2  # entity_type_code
            row[42] = 1234567890  # address npi
            row[43] = "primary"  # address type
            row[49] = "PROVIDENCE"  # city_name
            row[50] = "RI"  # state_name
            row[51] = "02903"  # postal_code
            row[62] = "333600000X"  # healthcare_provider_taxonomy_code
            row[63] = "LIC-1"
            row[64] = "RI"
            row[65] = "Y"
            return [tuple(row)]

        async def first(self, *_args, **_kwargs):
            return None

    conn = SparseResultConnection()
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(conn))

    request = types.SimpleNamespace(
        args={"name_like": "cvs", "classification": "Pharmacy", "state": "RI", "limit": "3"}
    )
    resp = await get_all(request)
    response_body = json.loads(resp.body)

    assert response_body["total"] == 1
    assert isinstance(response_body["rows"], list)
    assert len(response_body["rows"]) == 1
    assert response_body["rows"][0]["npi"] == 1234567890
    assert isinstance(response_body["rows"][0].get("taxonomy_list"), list)
    assert "employer_identification_number" not in response_body["rows"][0]
    assert "00-0000000" not in json.dumps(response_body)


@pytest.mark.asyncio
async def test_get_all_keeps_each_positional_rows_own_address(monkeypatch):
    class PositionalResultConnection:
        async def all(self, _sql, **_kwargs):
            def provider_row(street: str, address_key: str):
                row = [None] * 73
                row[0] = 1234567890  # npi_code
                row[1] = 1234567890  # b.npi
                row[3] = 1  # entity_type_code
                row[7] = "Sample"  # provider_first_name
                row[6] = "Clinician"  # provider_last_name
                row[42] = 1234567890  # address npi
                row[43] = "practice"  # address type
                row[53] = street  # first_line
                row[55] = "Example City"
                row[56] = "UT"
                row[57] = "84001"
                row[58] = "US"
                row[66] = address_key
                row[69] = "207Q00000X"
                row[72] = "Y"
                return tuple(row)

            return [
                provider_row(
                    "100 First Avenue",
                    "00000000-0000-0000-0000-000000000101",
                ),
                provider_row(
                    "200 Second Avenue",
                    "00000000-0000-0000-0000-000000000102",
                ),
            ]

        async def first(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: FakeAcquire(PositionalResultConnection()),
    )
    response = await get_all(
        types.SimpleNamespace(
            args={
                "name_like": "sample",
                "limit": "3",
                "include_total": "0",
            }
        )
    )
    provider_match = json.loads(response.body)["rows"][0]

    assert [
        location["first_line"] for location in provider_match["address_list"]
    ] == ["100 First Avenue", "200 Second Avenue"]


class _MappedProviderDirectoryRow:
    def __init__(self, mapping):
        self._mapping = mapping


class _ProviderDirectoryMappedConnection:
    async def all(self, sql, **_params):
        if "page_npis AS" not in str(sql):
            return []
        return [_MappedProviderDirectoryRow(_provider_directory_row_mapping())]

    async def first(self, *_args, **_kwargs):
        return None


class _MultiLocationMappedConnection:
    def __init__(self, rows, *, taxonomy_rows=None):
        self.rows = rows
        self.taxonomy_rows = taxonomy_rows or []
        self.sql_calls = []

    async def all(self, sql, **params):
        sql_text = str(sql)
        self.sql_calls.append((sql_text, dict(params)))
        if "FROM mrf.npi_taxonomy AS taxonomy" in sql_text:
            return [
                _MappedProviderDirectoryRow(row)
                for row in self.taxonomy_rows
            ]
        if "page_npis AS" not in sql_text:
            return []
        return [_MappedProviderDirectoryRow(row) for row in self.rows]

    async def first(self, *_args, **_kwargs):
        return None


class _HydratingMultiLocationConnection(_MultiLocationMappedConnection):
    def __init__(self, rows, hydrated_rows, *, taxonomy_rows=None):
        super().__init__(rows, taxonomy_rows=taxonomy_rows)
        self.hydrated_rows = hydrated_rows

    async def all(self, sql, **params):
        sql_text = str(sql)
        self.sql_calls.append((sql_text, dict(params)))
        if "FROM mrf.npi_taxonomy AS taxonomy" in sql_text:
            return [
                _MappedProviderDirectoryRow(row)
                for row in self.taxonomy_rows
            ]
        if "SELECT c.*" in sql_text:
            selected_keys = {
                str(value) for value in (params.get("location_keys") or [])
            }
            return [
                row
                for row in self.hydrated_rows
                if str(row.get("location_key") or "") in selected_keys
            ]
        if "page_npis AS" in sql_text:
            return [_MappedProviderDirectoryRow(row) for row in self.rows]
        return []


class _FallbackMultiLocationConnection(_MultiLocationMappedConnection):
    async def all(self, sql, **params):
        sql_text = str(sql)
        self.sql_calls.append((sql_text, dict(params)))
        if "page_npis AS" in sql_text:
            return [
                _MappedProviderDirectoryRow(
                    {
                        **row,
                        "npi_code": row.get("npi") or row.get("inferred_npi"),
                    }
                )
                for row in self.rows
            ]
        if "SELECT c.*" in sql_text:
            return self.rows
        return []


def _provider_directory_row_mapping():
    return {
        "npi_code": 1000000049,
        "npi": 1000000049,
        "entity_type_code": 2,
        "provider_organization_name": "SYNTHETIC PROVIDER ORGANIZATION",
        "employer_identification_number": "private-ein-sentinel",
        "parent_organization_tin": "private-tin-sentinel",
        "type": "practice",
        "first_line": "115 Example Court",
        "city_name": "Example City",
        "state_name": "AL",
        "postal_code": "35401",
        "country_code": "US",
        "telephone_number": "+12053663010",
        "phone_number": "2053663010",
        "address_key": "e4cd3105-5ce1-efd3-3f31-d48bfa864a13",
        "address_sources": ["provider_directory_fhir"],
        "source_record_ids": ["provider_directory_fhir:practitioner_role:synthetic_directory:role-1:loc-1"],
        "source_count": 1,
    }


async def _provider_directory_test_columns(table_name, *, session=None):
    assert session is None
    if table_name != "entity_address_unified":
        return set()
    return npi_module._public_address_serving_column_keys() | {
        "address_precision",
        "location_key",
        "source_count",
        "updated_at",
        "zip5",
        "phone_number",
        "source_record_ids",
    }


async def _empty_provider_enrichment_summary(*_args, **_kwargs):
    return {}


async def _provider_directory_source_detail_map(source_ids, **_kwargs):
    assert source_ids == ["synthetic_directory"]
    return {
        "synthetic_directory": {
            "source": "provider_directory_fhir",
            "source_id": "synthetic_directory",
            "endpoint_id": "synthetic_endpoint",
            "canonical_api_base": "https://fhir.example.invalid/provider-directory",
            "org_name": "Synthetic Health Plan",
            "plan_name": "Provider Directory",
        }
    }


@pytest.mark.asyncio
async def test_npi_all_includes_fhir_sources(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(npi_module, "_table_columns", _provider_directory_test_columns)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", _empty_provider_enrichment_summary)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_source_detail_map", _provider_directory_source_detail_map)
    monkeypatch.setattr(
        npi_module,
        "_fetch_location_status_by_record_id",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(npi_module.db, "acquire", lambda: FakeAcquire(_ProviderDirectoryMappedConnection()))

    request = types.SimpleNamespace(
        args={
            "name_like": "synthetic",
            "include_sources": "true",
            "limit": "5",
            "start": "0",
            "include_total": "0",
        }
    )
    response = await get_all(request)
    provider_match = json.loads(response.body)["rows"][0]

    assert "employer_identification_number" not in provider_match
    assert "parent_organization_tin" not in provider_match
    assert "private-ein-sentinel" not in json.dumps(provider_match)
    assert "private-tin-sentinel" not in json.dumps(provider_match)

    assert provider_match["provider_directory_sources"] == [
            {
                "source": "provider_directory_fhir",
                "source_ids": ["synthetic_directory"],
                "endpoint_id": "synthetic_endpoint",
                "catalog_aliases_verified": False,
            "catalog_aliases": [
                {
                    "source_id": "synthetic_directory",
                    "org_name": "Synthetic Health Plan",
                    "plan_name": "Provider Directory",
                }
            ],
        }
    ]
    assert "source_record_ids" not in provider_match


def _ranked_location_maps():
    return [
        {
            "npi_code": 1000000007,
            "npi": 1000000007,
            "entity_type_code": 1,
            "provider_first_name": "Sample",
            "provider_last_name_legal_name": "Clinician",
            "type": "practice",
            "first_line": street,
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": f"8400{index}",
            "country_code": "US",
            "telephone_number": f"+1801555010{index}",
            "address_key": f"00000000-0000-0000-0000-00000000010{index}",
            "premise_key": f"00000000-0000-0000-0000-00000000020{index}",
            "address_sources": ["provider_directory_fhir"],
            "source_record_ids": [
                f"provider_directory_fhir:practitioner_role:synthetic:role-1:loc-{index}"
            ],
            "source_count": 1,
            "provider_address_total": 4,
        }
        for index, street in enumerate(
            (
                "100 Example Ave",
                "200 Example Ave",
                "300 Example Ave",
                "400 Example Ave",
            ),
            start=1,
        )
    ]


def _install_multi_location_dependencies(
    monkeypatch,
    connection,
    *,
    status_by_record_id=None,
):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(
        npi_module,
        "_address_serving_table_sql",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )
    monkeypatch.setattr(npi_module, "_table_columns", _provider_directory_test_columns)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_enrichment_summary_map",
        _empty_provider_enrichment_summary,
    )
    monkeypatch.setattr(
        npi_module,
        "_fetch_location_status_by_record_id",
        AsyncMock(return_value=status_by_record_id or {}),
    )
    monkeypatch.setattr(
        npi_module.db,
        "acquire",
        lambda: FakeAcquire(connection),
    )


async def _first_provider_match(request_args):
    operation_response = await get_all(types.SimpleNamespace(args=request_args))
    return json.loads(operation_response.body)["rows"][0]


def _provider_page_sql(connection):
    return next(
        (statement_sql, statement_params)
        for statement_sql, statement_params in connection.sql_calls
        if "page_npis AS" in statement_sql
    )


def _assert_ranked_location_query_contract(connection):
    page_sql, page_params = _provider_page_sql(connection)
    assert "COUNT(*) OVER () AS provider_address_total" in page_sql
    assert "json_agg(taxonomy ORDER BY taxonomy.checksum)" not in page_sql
    assert any(
        "FROM mrf.npi_taxonomy AS taxonomy" in statement_sql
        for statement_sql, _statement_params in connection.sql_calls
    )
    assert "select sub_s.*, t.*" not in page_sql.lower()
    assert "c.type IN" in page_sql
    assert all(
        f"'{location_type}'" in page_sql
        for location_type in ("primary", "secondary", "practice", "site")
    )
    assert "LIMIT :locations_per_provider" not in page_sql
    assert "locations_per_provider" not in page_params


@pytest.mark.asyncio
async def test_npi_all_returns_three_ranked_locations_with_honest_total(monkeypatch):
    location_maps = _ranked_location_maps()
    nested_taxonomy_map = {
        "npi": 1000000007,
        "checksum": 1,
        "healthcare_provider_taxonomy_code": "207Q00000X",
    }
    location_maps[0]["taxonomy_rows"] = [nested_taxonomy_map]
    location_maps[1]["taxonomy_rows"] = [nested_taxonomy_map]
    location_maps[2]["healthcare_provider_taxonomy_code"] = "207Q00000X"
    connection = _MultiLocationMappedConnection(
        location_maps,
        taxonomy_rows=[nested_taxonomy_map],
    )
    status_by_record_id = {
        location_maps[index]["source_record_ids"][0]: location_status
        for index, location_status in enumerate(
            ("active", "unknown", "unknown", "active")
        )
    }
    _install_multi_location_dependencies(
        monkeypatch,
        connection,
        status_by_record_id=status_by_record_id,
    )
    provider_match = await _first_provider_match(
        {"name_like": "sample", "limit": "5", "start": "0", "include_total": "0"}
    )

    assert [
        location["first_line"] for location in provider_match["address_list"]
    ] == ["100 Example Ave", "400 Example Ave", "200 Example Ave"]
    assert provider_match["address_pagination"] == {
        "limit": 3,
        "offset": 0,
        "returned": 3,
        "total": 4,
        "has_more": True,
    }
    assert provider_match["location_status"] == "active"
    assert provider_match["taxonomy_list"] == [
        {"healthcare_provider_taxonomy_code": "207Q00000X"}
    ]
    assert all(
        "source_record_ids" not in location
        for location in provider_match["address_list"]
    )
    _assert_ranked_location_query_contract(connection)


@pytest.mark.asyncio
async def test_npi_all_card_view_returns_only_compact_typeahead_fields(monkeypatch):
    location_maps = _ranked_location_maps()[:1]
    location_maps[0].update(
        {
            "provider_last_name": "Clinician",
            "provider_credential_text": "MD",
        }
    )
    taxonomy_map = {
        "npi": 1000000007,
        "checksum": 1,
        "healthcare_provider_taxonomy_code": "207Q00000X",
        "healthcare_provider_primary_taxonomy_switch": "Y",
        "taxonomy_display": "Family Medicine",
    }
    connection = _MultiLocationMappedConnection(
        location_maps,
        taxonomy_rows=[taxonomy_map],
    )
    _install_multi_location_dependencies(monkeypatch, connection)

    response = await get_all(
        types.SimpleNamespace(
            args={
                "q": "sample",
                "view": "card",
                "limit": "10",
                "include_total": "0",
            }
        )
    )
    response_document = json.loads(response.body)

    assert response_document["rows"] == [
        {
            "npi": 1000000007,
            "display_name": "Sample Clinician",
            "entity_type": "individual",
            "credential": "MD",
            "primary_specialty": {
                "taxonomy_code": "207Q00000X",
                "display": "Family Medicine",
            },
            "city": "Example City",
            "state": "UT",
            "zip5": "84001",
        }
    ]
    assert len(response.body) < 15_000


@pytest.mark.parametrize(
    ("postal_code", "expected_zip5"),
    [
        ("60601", "60601"),
        ("60601-1234", "60601"),
        ("606011234", "60601"),
        ("not-a-postal-code", None),
        ("6060A", None),
    ],
)
def test_provider_card_zip5_emits_only_schema_valid_values(
    postal_code,
    expected_zip5,
):
    assert npi_module._provider_card_zip5(postal_code) == expected_zip5


def _fallback_location_maps():
    return [
        {
            "npi": None,
            "inferred_npi": 1000000015,
            "entity_name": "Synthetic clinic",
            "type": "practice",
            "first_line": f"{index} Fallback Avenue",
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": f"8410{index}",
            "address_key": f"00000000-0000-0000-0000-00000000030{index}",
            "location_key": f"00000000-0000-0000-0000-00000000035{index}",
            "premise_key": f"00000000-0000-0000-0000-00000000040{index}",
            "source_record_ids": [
                f"provider_directory_fhir:practitioner_role:synthetic:role-2:loc-{index}"
            ],
            "provider_address_total": 4,
        }
        for index in range(1, 5)
    ]


@pytest.mark.asyncio
async def test_npi_all_profile_only_fallback_groups_three_locations(monkeypatch):
    fallback_locations = _fallback_location_maps()
    connection = _FallbackMultiLocationConnection(fallback_locations)
    status_by_record_id = {
        location["source_record_ids"][0]: "active"
        for location in fallback_locations
    }
    _install_multi_location_dependencies(
        monkeypatch,
        connection,
        status_by_record_id=status_by_record_id,
    )
    provider_match = await _first_provider_match(
        {"npi": "1000000015", "limit": "5", "start": "0", "include_total": "0"}
    )

    assert provider_match["npi"] == 1000000015
    assert all(
        location["npi"] == 1000000015
        for location in provider_match["address_list"]
    )

    assert [
        location["first_line"] for location in provider_match["address_list"]
    ] == [
        "1 Fallback Avenue",
        "2 Fallback Avenue",
        "3 Fallback Avenue",
    ]
    assert provider_match["address_pagination"] == {
        "limit": 3,
        "offset": 0,
        "returned": 3,
        "total": 4,
        "has_more": True,
    }
    page_sql, page_params = _provider_page_sql(connection)
    assert "ROW_NUMBER() OVER" not in page_sql
    assert "COUNT(*) OVER" in page_sql
    assert "COALESCE(c.npi, c.inferred_npi)" in page_sql
    assert page_params["limit"] == 5


def _exact_provider_location_maps(provider_npi):
    return [
        {
            "npi_code": provider_npi,
            "npi": None,
            "inferred_npi": None if index == 1 else provider_npi,
            "entity_name": "Synthetic provider",
            "type": "practice",
            "first_line": street,
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": f"8410{index}",
            "address_key": f"00000000-0000-0000-0000-0000000005{index:02d}",
            "provider_address_total": 3,
        }
        for index, street in enumerate(
            ("100 Example Ave", "200 Example Ave", "300 Example Ave"),
            start=1,
        )
    ]


@pytest.mark.asyncio
async def test_npi_all_exact_provider_page_collects_direct_and_inferred_locations(
    monkeypatch,
):
    provider_npi = 1000000015
    location_maps = _exact_provider_location_maps(provider_npi)
    connection = _MultiLocationMappedConnection(location_maps)
    _install_multi_location_dependencies(monkeypatch, connection)
    provider_match = await _first_provider_match(
        {"npi": str(provider_npi), "limit": "1", "start": "0", "include_total": "0"}
    )

    assert provider_match["npi"] == provider_npi
    assert [
        location["first_line"]
        for location in provider_match["address_list"]
    ] == ["100 Example Ave", "200 Example Ave", "300 Example Ave"]
    assert all(
        location["npi"] == provider_npi
        for location in provider_match["address_list"]
    )
    assert provider_match["address_pagination"] == {
        "limit": 3,
        "offset": 0,
        "returned": 3,
        "total": 3,
        "has_more": False,
    }
    page_sql, page_params = _provider_page_sql(connection)
    assert "COALESCE(c.npi, c.inferred_npi) = :npi_filter" in page_sql
    assert page_params["limit"] == 1


def _duplicate_search_location_maps(provider_npi, address_key, site_key):
    return [
        {
            "npi_code": provider_npi,
            "npi": provider_npi,
            "entity_type_code": 1,
            "provider_first_name": "Synthetic",
            "provider_last_name_legal_name": "Clinician",
            "type": "practice",
            "first_line": "100 Example Ave",
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": "84101",
            "telephone_number": "2025550101",
            "address_key": address_key,
            "premise_key": site_key,
            "location_key": "synthetic-location-a",
            "address_sources": ["synthetic_source_a"],
            "source_record_ids": ["synthetic:role:active-a"],
            "provider_address_total": 5,
        },
        {
            "npi_code": provider_npi,
            "npi": None,
            "inferred_npi": provider_npi,
            "type": "practice",
            "first_line": "100 Example Ave",
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": "84101",
            "telephone_number": "2025550199",
            "address_key": address_key,
            "premise_key": site_key,
            "location_key": "synthetic-location-b",
            "address_sources": ["synthetic_source_b"],
            "source_record_ids": ["synthetic:role:unknown-b"],
            "provider_address_total": 5,
        },
    ]


def _search_hydration_location_maps(provider_npi):
    location_maps = _duplicate_search_location_maps(
        provider_npi,
        "00000000-0000-0000-0000-000000000601",
        "00000000-0000-0000-0000-000000000701",
    )
    location_maps.extend(
        {
            "npi_code": provider_npi,
            "npi": provider_npi,
            "type": "practice",
            "first_line": f"{index}00 Example Ave",
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": f"8410{index}",
            "address_key": f"00000000-0000-0000-0000-00000000060{index}",
            "location_key": f"synthetic-location-{index}",
            "source_record_ids": [f"synthetic:role:active-{index}"],
            "provider_address_total": 5,
        }
        for index in range(2, 5)
    )
    return location_maps


def _hydrated_location_maps(location_maps):
    return [
        {
            **location_map,
            "taxonomy_array": [100 + index],
            "plans_network_array": [200 + index],
            "procedures_array": [300 + index],
            "medications_array": [400 + index],
            "aca_plan_array": [f"synthetic-aca-{index}"],
            "ptg_plan_array": [f"synthetic-ptg-{index}"],
            "group_plan_array": [f"synthetic-group-{index}"],
            "fax_number": f"20255502{index:02d}",
        }
        for index, location_map in enumerate(location_maps, start=1)
    ]


def _search_location_status_map():
    return {
        "synthetic:role:active-a": "active",
        "synthetic:role:unknown-b": "unknown",
        **{
            f"synthetic:role:active-{index}": "active"
            for index in range(2, 5)
        },
    }


def _assert_hydrated_primary_location(provider_match):
    primary_location = provider_match["address_list"][0]
    assert primary_location["telephone_number"] == "2025550101"
    assert primary_location["address_sources"] == [
        "synthetic_source_a",
        "synthetic_source_b",
    ]
    assert primary_location["aca_plan_array"] == [
        "synthetic-aca-1",
        "synthetic-aca-2",
    ]
    assert primary_location["ptg_plan_array"] == [
        "synthetic-ptg-1",
        "synthetic-ptg-2",
    ]
    assert primary_location["procedures_array"] == [301, 302]
    assert primary_location["medications_array"] == [401, 402]
    assert primary_location["location_status"] == "active"
    for internal_key in (
        "location_key",
        "inferred_npi",
        "source_record_ids",
        "_base_row_identities",
    ):
        assert internal_key not in primary_location


def _hydration_query_details(connection):
    page_sql = next(
        statement_sql
        for statement_sql, _statement_params in connection.sql_calls
        if "page_npis AS" in statement_sql
    )
    hydration_sql, hydration_params = next(
        (statement_sql, statement_params)
        for statement_sql, statement_params in connection.sql_calls
        if "SELECT c.*" in statement_sql
    )
    return page_sql, hydration_sql, hydration_params


@pytest.mark.asyncio
async def test_npi_all_search_hydrates_selected_locations_and_merges_base_evidence(
    monkeypatch,
):
    location_maps = _search_hydration_location_maps(1000000023)
    connection = _HydratingMultiLocationConnection(
        location_maps,
        _hydrated_location_maps(location_maps),
    )
    _install_multi_location_dependencies(
        monkeypatch,
        connection,
        status_by_record_id=_search_location_status_map(),
    )
    provider_match = await _first_provider_match(
        {"name_like": "synthetic", "limit": "5", "start": "0", "include_total": "0"}
    )

    assert len(provider_match["address_list"]) == 3
    assert provider_match["address_pagination"] == {
        "limit": 3,
        "offset": 0,
        "returned": 3,
        "total": 4,
        "has_more": True,
    }
    _assert_hydrated_primary_location(provider_match)
    page_sql, hydration_sql, hydration_params = _hydration_query_details(connection)
    assert "c.aca_plan_array" not in page_sql
    assert "c.procedures_array" not in page_sql
    assert "c.location_key = ANY(:location_keys)" in hydration_sql
    assert {"synthetic-location-a", "synthetic-location-b"}.issubset(
        set(hydration_params["location_keys"])
    )
    assert "synthetic-location-4" not in hydration_params["location_keys"]


@pytest.mark.asyncio
async def test_npi_all_search_degrades_when_selected_hydration_fails(monkeypatch):
    class FailingHydrationConnection(_HydratingMultiLocationConnection):
        async def all(self, sql, **params):
            if "SELECT c.*" in str(sql):
                raise RuntimeError("synthetic selected hydration failure")
            return await super().all(sql, **params)

    location_maps = _search_hydration_location_maps(1000000023)
    connection = FailingHydrationConnection(
        location_maps,
        _hydrated_location_maps(location_maps),
    )
    _install_multi_location_dependencies(
        monkeypatch,
        connection,
        status_by_record_id=_search_location_status_map(),
    )

    provider_match = await _first_provider_match(
        {"name_like": "synthetic", "limit": "5", "start": "0", "include_total": "0"}
    )

    assert len(provider_match["address_list"]) == 3
    assert provider_match["address_pagination"]["total"] == 4
    assert provider_match["address_pagination"]["has_more"] is True
    assert all(
        "procedures_array" not in location
        for location in provider_match["address_list"]
    )


def _large_system_location_maps(provider_npi):
    return [
        {
            "npi_code": provider_npi,
            "npi": provider_npi,
            "entity_type_code": 2,
            "provider_organization_name": "Synthetic medical system",
            "type": "practice",
            "first_line": f"{index:04d} Example Ave",
            "city_name": "Example City",
            "state_name": "UT",
            "postal_code": "84101",
            "address_key": (
                f"00000000-0000-0000-0001-{index:012d}"
            ),
            "location_key": f"synthetic-large-{index:04d}",
            "provider_address_total": 1000,
        }
        for index in range(1000)
    ]


def _large_system_hydrated_locations(location_maps):
    return [
        {
            **location_map,
            "procedures_array": [index],
            "medications_array": [index + 1000],
        }
        for index, location_map in enumerate(location_maps)
    ]


@pytest.mark.asyncio
async def test_npi_all_large_system_hydrates_only_three_selected_locations(
    monkeypatch,
):
    location_maps = _large_system_location_maps(1000000031)
    connection = _HydratingMultiLocationConnection(
        location_maps,
        _large_system_hydrated_locations(location_maps),
    )
    _install_multi_location_dependencies(monkeypatch, connection)
    provider_match = await _first_provider_match(
        {"name_like": "synthetic", "limit": "5", "start": "0", "include_total": "0"}
    )
    _page_sql, _hydration_sql, hydration_params = _hydration_query_details(connection)

    assert len(provider_match["address_list"]) == 3
    assert provider_match["address_pagination"]["total"] == 1000
    assert provider_match["address_pagination"]["has_more"] is True
    assert len(hydration_params["location_keys"]) == 3


def test_overlay_row_merges_by_key():
    address_key = "350a43ae-8b2b-0145-9d07-8a44ad743dcc"
    merged_addresses = npi_module._dedupe_addresses_by_key(
        [
            {
                "type": "practice",
                "address_key": address_key,
                "first_line": "510 E 8TH ST",
                "address_sources": ["mrf"],
                "source_count": 1,
            },
            {
                "type": "practice",
                "address_key": address_key,
                "first_line": "510 E 8TH ST",
                "address_sources": ["provider_directory_fhir"],
                "source_record_ids": [
                    "provider_directory_fhir:organization_address:pdfhir_source:org-1:1"
                ],
                "source_count": 1,
            },
        ]
    )

    assert merged_addresses[1:] == []
    assert merged_addresses[0]["address_sources"] == ["mrf", "provider_directory_fhir"]
    assert merged_addresses[0]["source_record_ids"] == [
        "provider_directory_fhir:organization_address:pdfhir_source:org-1:1"
    ]
    assert merged_addresses[0]["multi_source_confirmed"]
