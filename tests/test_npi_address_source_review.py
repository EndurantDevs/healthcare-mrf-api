# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address thresholds cover phone selection and provider-list shortcuts."""

import json
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from api.endpoint import npi
from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection
from tests.test_plan_pricing_projection_postgres import POSTGRES_DSN_ENV, _sqlalchemy_async_dsn


@pytest.mark.asyncio
@pytest.mark.parametrize("count_format", ["classification", "full_taxonomy"])
@pytest.mark.parametrize("minimum", [0, 3, 2147483647])
async def test_formatted_counts_apply_positive_thresholds_to_provider_addresses(monkeypatch, count_format, minimum):
    class TaxonomyConnection(RecordingConnection):
        async def all(self, sql, **params):
            await super().all(sql, **params)
            if params.get("min_address_sources") == 2147483647:
                return []
            return [("Pharmacy" if count_format == "classification" else 123, 4 if minimum else 9)]

    connection = TaxonomyConnection()
    monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.EntityAddressUnified))
    result = await npi.get_all(
        SimpleNamespace(args={"count_only": "1", "format": count_format, "min_address_sources": str(minimum)})
    )
    key = "Pharmacy" if count_format == "classification" else "123"
    assert json.loads(result.body) == {"rows": {} if minimum == 2147483647 else {key: 4 if minimum else 9}}
    statement, parameters = connection.sql_calls[-1]
    if minimum:
        assert parameters["min_address_sources"] == minimum
        assert "COUNT(DISTINCT ft.npi)" in statement
        assert statement.index(f"c.source_count >= {minimum}") < statement.rindex("GROUP BY")
        assert f"GROUP BY {'q.int_code' if count_format == 'full_taxonomy' else 'q.classification'}" in statement
    else:
        assert "entity_address_unified" not in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_phone_match_filters_the_selected_office_before_its_lateral_limit(minimum):
    params = await npi._normalize_match_candidate_params(
        SimpleNamespace(args={"phone": "3125550100", "min_address_sources": str(minimum)})
    )
    statement, arguments = npi._match_candidate_query(params, "mrf.entity_address_unified")
    lateral = str(statement).split("CROSS JOIN LATERAL (", 1)[1].split(") AS a", 1)[0]
    predicate = f"candidate_address.source_count >= {minimum}"
    assert (predicate in lateral) is bool(minimum)
    if minimum:
        assert lateral.index(predicate) < lateral.index("ORDER BY") < lateral.index("LIMIT 1")
        assert arguments["min_address_sources"] == minimum


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0", "3"])
async def test_sitemap_search_uses_filtered_selection_for_positive_thresholds(monkeypatch, minimum):
    connection = RecordingConnection()
    classification = AsyncMock(return_value=[1234567890])
    monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.EntityAddressUnified))
    monkeypatch.setattr(npi, "_get_classification_npi_list", classification)
    query_by_name = {"view": "sitemap", "classification": "Pharmacy", "include_total": "0", "limit": "1"}
    if minimum is not None:
        query_by_name["min_address_sources"] = minimum

    document = json.loads((await npi.get_all(SimpleNamespace(args=query_by_name))).body)

    assert document["rows"] == []
    if minimum == "3":
        classification.assert_not_awaited()
        page, arguments = next(call for call in connection.sql_calls if "page_npis AS" in call[0])
        assert page.index(f"c.source_count >= {minimum}") < page.index("LIMIT :limit")
        assert arguments["min_address_sources"] == 3
    else:
        classification.assert_awaited_once()
        assert any("WHERE b.npi = ANY(:page_npis)" in sql for sql, _ in connection.sql_calls)


class _ProviderConnection(RecordingConnection):
    async def all(self, sql, **params):
        ordinary = await super().all(sql, **params)
        if "page_npis AS" not in str(sql):
            return ordinary
        return [
            SimpleNamespace(
                _mapping={
                    "npi": 1234567890,
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "type": "primary",
                    "first_line": "100 Example Street",
                    "city_name": "Example City",
                    "state_name": "IL",
                    "source_count": 3,
                }
            )
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, "0", "3"])
async def test_search_summary_cannot_emit_uncounted_enrollment_postal_values(monkeypatch, minimum):
    connection = _ProviderConnection()
    summary_by_field = {
        "ffs_practice_cities": ["Uncounted City"],
        "ffs_practice_states": ["CA"],
        "ffs_practice_zip_codes": ["90001"],
        "ffs_enrollment_count": 2,
    }
    monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.EntityAddressUnified))
    monkeypatch.setattr(
        npi, "_fetch_provider_enrichment_summary_map", AsyncMock(return_value={1234567890: summary_by_field})
    )
    query_by_name = {"include_total": "0", "limit": "1"}
    if minimum is not None:
        query_by_name["min_address_sources"] = minimum

    document = json.loads((await npi.get_all(SimpleNamespace(args=query_by_name))).body)

    provider = document["rows"][0]
    actual = provider["provider_enrichment_summary"]
    for key in ("ffs_practice_cities", "ffs_practice_states", "ffs_practice_zip_codes"):
        assert actual[key] == ([] if minimum == "3" else summary_by_field[key])
    assert actual["ffs_enrollment_count"] == 2
    assert provider["city_name"] == "Example City" and provider["state_name"] == "IL"
    assert summary_by_field["ffs_practice_cities"] == ["Uncounted City"]


@pytest.mark.parametrize("minimum", [0, 3])
def test_match_evidence_filters_enrollment_postal_values_without_changing_qualified_office(minimum):
    summary_by_field = {"ffs_practice_cities": ["Uncounted City"], "has_ffs_enrollment": True}
    provider_by_field = {"npi": 1234567890, "first_line": "100 Example Street", "city_name": "Example City"}
    candidate_by_field = npi._match_candidate_output(
        provider_by_field, {"min_address_sources": minimum, "include_evidence": True}, summary_by_field
    )
    assert candidate_by_field["address"]["city_name"] == "Example City"
    assert candidate_by_field["sources"]["ffs"]["has_ffs_enrollment"] is True
    assert candidate_by_field["evidence"]["provider_enrichment_summary"]["ffs_practice_cities"] == (
        [] if minimum else ["Uncounted City"]
    )
    assert summary_by_field["ffs_practice_cities"] == ["Uncounted City"]


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum,expected", [(0, "exact-low"), (3, "qualified"), (4, None)])
async def test_native_phone_lateral_retains_a_qualified_overlay_office(minimum, expected):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text("""CREATE TEMP TABLE entity_address_unified (
                    npi bigint, inferred_npi bigint, address_key uuid, source_count integer,
                    type text, location_key text, phone_number text, telephone_number text
                ) ON COMMIT DROP""")
            )
            await connection.execute(
                text("""INSERT INTO pg_temp.entity_address_unified VALUES
                    (1234567890,NULL,'00000000-0000-0000-0000-000000000001',2,
                     'practice','exact-low','3125550100','3125550100'),
                    (1234567890,NULL,'00000000-0000-0000-0000-000000000001',3,
                     'practice','qualified','3125550101','3125550101')""")
            )
            selection = npi._address_phone_candidates_lateral_from("pg_temp.entity_address_unified", "a", minimum)
            statement = text(
                """WITH phone_candidates AS (
                    SELECT 1234567890::bigint AS provider_npi,
                           '00000000-0000-0000-0000-000000000001'::uuid AS address_key
                ), phone_provider_directory_evidence AS (
                    SELECT NULL::bigint AS provider_npi WHERE false
                ) SELECT a.location_key """
                + selection
            )
            query_result = await connection.execute(
                statement, {"phone_digits": "3125550100", "min_address_sources": minimum}
            )
            assert query_result.scalar_one_or_none() == expected
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum,address_suffix", [(0, "2"), (3, "1")])
async def test_native_phone_overlay_cannot_replace_a_qualifying_service_address(minimum, address_suffix):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    engine = create_async_engine(_sqlalchemy_async_dsn(dsn))
    try:
        async with engine.begin() as connection:
            for statement in (
                """CREATE TEMP TABLE entity_address_unified (
                    npi bigint, inferred_npi bigint, address_key uuid, source_count integer,
                    type text, phone_number text, telephone_number text) ON COMMIT DROP""",
                """INSERT INTO entity_address_unified VALUES
                    (1234567890,NULL,'00000000-0000-0000-0000-000000000001',3,'practice','3125550100',NULL),
                    (1234567890,NULL,'00000000-0000-0000-0000-000000000002',4,'mail','3125550101',NULL)""",
                """CREATE TEMP TABLE provider_directory_source AS
                    SELECT 'source'::text AS source_id, 'endpoint'::text AS endpoint_id""",
                """CREATE TEMP TABLE provider_directory_endpoint_dataset AS
                    SELECT 'dataset'::text AS dataset_id, 'endpoint'::text AS endpoint_id,
                           'run'::text AS acquisition_root_run_id, 'run'::text AS import_run_id,
                           true AS is_current, 'published'::text AS status,
                           now() AS published_at, NULL::timestamptz AS superseded_at""",
                """CREATE TEMP TABLE provider_directory_dataset_resource AS
                    SELECT 'dataset'::text AS dataset_id, 'Location'::text AS resource_type,
                           'resource'::text AS resource_id""",
                """CREATE TEMP TABLE provider_directory_address_overlay AS
                    SELECT 1234567890::bigint AS npi,
                           '00000000-0000-0000-0000-000000000002'::uuid AS address_key,
                           'source'::text AS source_id, 'run'::text AS last_seen_run_id,
                           'record'::text AS source_record_id, 'Location'::text AS resource_type,
                           'resource'::text AS resource_id, '3125550100'::text AS phone_number""",
            ):
                await connection.execute(text(statement))
            candidates = npi._address_phone_candidates_cte(
                "pg_temp.entity_address_unified", min_address_sources=minimum
            ).replace("mrf.", "pg_temp.")
            query_result = await connection.execute(
                text(f"WITH {candidates} SELECT provider_npi, address_key::text FROM phone_candidates"),
                {"phone_digits": "3125550100", "min_address_sources": minimum, "candidate_limit": 1},
            )
            assert query_result.all() == [(1234567890, f"00000000-0000-0000-0000-00000000000{address_suffix}")]
    finally:
        await engine.dispose()
