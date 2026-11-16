# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.request.parameters import RequestParameters

from api import provider_batch
from api.endpoint import npi
from api.provider_geo_search import ProviderGeoSearch
from api.provider_list_sql import _address_phone_candidates_cte
from api.provider_profile import (
    _without_uncounted_postal_addresses,
    compose_provider_profile,
    compose_source_filtered_profile,
)
from tests.test_npi_all_names_like import FakeAcquire, RecordingConnection


@pytest.mark.asyncio
@pytest.mark.parametrize("is_network_projection", [False, True])
async def test_geo_threshold_keeps_projection_and_qualifies_before_page(monkeypatch, is_network_projection):
    connection = RecordingConnection()
    search_table = '"mrf".provider_geo_search_g7'
    monkeypatch.setattr(npi, "provider_geo_read_snapshot", lambda *_args: nullcontext(None))
    monkeypatch.setattr(npi, "_provider_list_connection", lambda *_args: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_table_sql", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    monkeypatch.setattr(npi, "request_geo_search", AsyncMock(return_value=ProviderGeoSearch(search_table)))
    monkeypatch.setattr(npi, "nearest_first_pages", lambda *_args: nullcontext())
    monkeypatch.setattr(
        npi, "scoped_geo_search_relation_sql", lambda *_args: search_table if is_network_projection else None
    )
    await npi.get_near_npi(
        SimpleNamespace(args={"lat": "41", "long": "-87", "min_address_sources": "3", "include_total": "1"})
    )
    assert len(connection.sql_calls) == 2
    for sql, parameters in connection.sql_calls:
        assert f"FROM {search_table} AS a" in sql
        assert "source_address.location_key = a.location_key" in sql
        assert "source_address.source_count >= 3" in sql
        assert parameters["min_address_sources"] == 3
        if "LIMIT :limit" in sql:
            assert sql.index("source_address.source_count") < sql.index("LIMIT :limit")


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3, 2147483647])
async def test_source_threshold_only_filters_both_search_count_and_page(monkeypatch, minimum):
    connection = RecordingConnection()
    monkeypatch.setattr(npi.db, "acquire", lambda: FakeAcquire(connection))
    monkeypatch.setattr(npi, "_address_serving_model", AsyncMock(return_value=npi.EntityAddressUnified))
    monkeypatch.setattr(npi, "_fast_primary_npi_count", AsyncMock(return_value=5 if minimum == 0 else None))
    operation = await npi.get_all(SimpleNamespace(args={"min_address_sources": str(minimum), "limit": "1"}))
    assert operation.status == 200
    assert json.loads(operation.body)["total"] == 5
    for sql, parameters in connection.sql_calls:
        assert (f"c.source_count >= {minimum}" in sql) == bool(minimum)
        if minimum:
            assert parameters["min_address_sources"] == minimum
            if "LIMIT :limit" in sql:
                assert sql.index(f"c.source_count >= {minimum}") < sql.index("LIMIT :limit")


@pytest.mark.asyncio
async def test_match_candidate_threshold_precedes_candidate_limit():
    request = SimpleNamespace(
        args={"address_key": "00000000-0000-0000-0000-000000000001", "min_address_sources": "3"},
        ctx=SimpleNamespace(sa_session=None),
    )
    parameters = await npi._normalize_match_candidate_params(request)
    statement, bound = npi._match_candidate_query(parameters, "mrf.entity_address_unified")
    sql = str(statement)
    assert bound["min_address_sources"] == 3
    assert sql.index("a.source_count >= 3") < sql.index("LIMIT :candidate_limit")
    legacy, _ = npi._match_candidate_query(parameters, "mrf.npi_address")
    assert "AND FALSE" in str(legacy)


def test_phone_threshold_qualifies_exact_offices_before_bounding_providers():
    sql = _address_phone_candidates_cte("mrf.entity_address_unified", min_address_sources=3)
    assert sql.index("phone_address.source_count >= 3") < sql.index("LIMIT :candidate_limit")
    assert "qualifying_address.address_key = overlay.address_key" in sql
    assert "COALESCE(qualifying_address.npi, qualifying_address.inferred_npi) = overlay.npi" in sql
    assert "qualifying_address.type IN ('primary', 'secondary', 'practice', 'site')" in sql


def test_batch_keeps_alternate_qualifying_office_and_original_membership():
    state = provider_batch._NativeBatchState(
        {1000000004: {"npi": 1000000004}},
        {1000000004: [{"source_count": 2}, {"source_count": 3}, {"source_count": 4}, {}]},
    )
    filtered = provider_batch._filter_batch_address_state(state, RequestParameters({"min_address_sources": ["3"]}))
    assert [address["source_count"] for address in filtered.addresses[1000000004]] == [3, 4]
    assert filtered.address_members == frozenset({1000000004})
    empty = provider_batch._filter_batch_address_state(state, {"min_address_sources": "5"})
    result, found = npi._npi_batch_provider_result(
        1000000004,
        None,
        [],
        [],
        [],
        None,
        {"include_sources": False, "include_evidence": False, "address_limit": 5, "address_offset": 0},
        authorized_without_addresses=1000000004 in empty.address_members,
    )
    assert found and result["provider"]["address_list"] == []
    assert result["provider"]["address_pagination"]["total"] == 0


def test_raw_profile_addresses_are_unknown_and_filtered_before_category_pagination():
    source_projection_by_field = {
        "generation_id": "a" * 64,
        "profile": {
            "categories": {
                "locations": {
                    "availability": "available",
                    "items": [{"type": "address", "value": {"address_line_1": "100 Example St"}}],
                },
            }
        },
    }
    profile_options_by_name = {
        "state_projection": source_projection_by_field,
        "fhir_profile": None,
        "page_category": "locations",
        "page_limit": 1,
    }
    default_profile = compose_provider_profile(1000000004, **profile_options_by_name)
    filtered_profile = compose_source_filtered_profile(1000000004, **profile_options_by_name, min_address_sources=3)
    assert len(default_profile["categories"]["locations"]["items"]) == 1
    assert filtered_profile["categories"]["locations"]["items"] == []
    assert filtered_profile["category_pagination"]["total"] == 0
    assert default_profile["generation_id"] != filtered_profile["generation_id"]
    assert (
        filtered_profile["generation_id"]
        != compose_source_filtered_profile(1000000004, **profile_options_by_name, min_address_sources=4)[
            "generation_id"
        ]
    )
    assert len(source_projection_by_field["profile"]["categories"]["locations"]["items"]) == 1
    assert _without_uncounted_postal_addresses(
        {
            "endpoint": {"address": "https://example.invalid/fhir"},
            "clinic": {"practice_location": "100 Example St", "name": "Example"},
        }
    ) == {
        "endpoint": {"address": "https://example.invalid/fhir"},
        "clinic": {"name": "Example"},
    }
