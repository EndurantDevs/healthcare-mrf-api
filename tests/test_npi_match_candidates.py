# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import json
import types

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


def _request(args):
    return types.SimpleNamespace(args=args, ctx=types.SimpleNamespace(sa_session=None))


@pytest.mark.asyncio
async def test_match_candidate_params_require_locator():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module._normalize_match_candidate_params(_request({"provider_type": "hospital"}))


@pytest.mark.asyncio
async def test_match_candidate_params_reject_removed_raw_address_locators():
    with pytest.raises(sanic.exceptions.InvalidUsage) as excinfo:
        await npi_module._normalize_match_candidate_params(
            _request(
                {
                    "address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670",
                    "first_line": "326 Nichols Rd",
                    "line1_norm": "326nicholsrd",
                    "zip_code": "01420",
                    "zip5": "01420",
                }
            )
        )

    message = str(excinfo.value)
    assert "first_line" in message
    assert "line1_norm" in message
    assert "zip_code" in message
    assert "zip5" in message


@pytest.mark.asyncio
async def test_match_candidate_params_reject_entity_conflict():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module._normalize_match_candidate_params(
            _request(
                {
                    "phone": "978-343-5270",
                    "entity_kind": "individual",
                    "entity_type_code": "2",
                }
            )
        )


@pytest.mark.asyncio
async def test_match_candidate_params_accept_every_public_filter(monkeypatch):
    async def fake_ensure_specialty_resolution_cache(session):
        assert session == "test-session"

    def fake_resolve_provider_specialty_filter(args):
        assert args == {"specialty": "hospital", "include_subspecialties": False}
        return types.SimpleNamespace(
            taxonomy_codes=("282N00000X",),
            classification=None,
            unresolved_specialty=False,
            suggested_specialties=(),
        )

    monkeypatch.setattr(
        npi_module,
        "ensure_specialty_resolution_cache",
        fake_ensure_specialty_resolution_cache,
    )
    monkeypatch.setattr(
        npi_module,
        "resolve_provider_specialty_filter",
        fake_resolve_provider_specialty_filter,
    )
    request = types.SimpleNamespace(
        args={
            "address_site_key": "C4147429-6998-8EB4-D3F8-C89EA64AF4CA",
            "address_key": "CB329E77-08B7-A9C4-5A2E-34F6CA32B670",
            "lat": "42.59734038",
            "long": "-71.80791638",
            "radius_miles": "0.5",
            "phone": "(978) 343-5270",
            "entity_kind": "organization",
            "entity_type_code": "2",
            "taxonomy_scope": "261Q*, 282N00000X",
            "provider_type": "hospital",
            "specialty": "Hospital",
            "include_subspecialties": "false",
            "include_sources": "false",
            "include_evidence": "false",
            "debug": "true",
            "limit": "50",
        },
        ctx=types.SimpleNamespace(sa_session="test-session"),
    )

    params = await npi_module._normalize_match_candidate_params(request)

    assert params["address_site_key"] == "c4147429-6998-8eb4-d3f8-c89ea64af4ca"
    assert params["address_key"] == "cb329e77-08b7-a9c4-5a2e-34f6ca32b670"
    assert params["lat"] == 42.59734038
    assert params["long"] == -71.80791638
    assert params["radius_miles"] == 0.5
    assert params["phone_digits"] == "9783435270"
    assert params["entity_type_code"] == 2
    assert params["entity_kind"] == "organization"
    assert params["taxonomy_exact"] == ("282N00000X",)
    assert params["taxonomy_prefixes"] == ("261Q",)
    assert params["provider_type"] == "hospital"
    assert params["include_subspecialties"] is False
    assert params["include_sources"] is True
    assert params["include_evidence"] is True
    assert params["debug"] is True
    assert params["limit"] == 50


@pytest.mark.asyncio
async def test_match_candidate_params_geo_defaults_radius():
    params = await npi_module._normalize_match_candidate_params(
        _request({"lat": "42.59734038", "long": "-71.80791638"})
    )

    assert params["radius_miles"] == 1.0


@pytest.mark.asyncio
async def test_match_candidate_params_reject_lat_without_long():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module._normalize_match_candidate_params(_request({"lat": "42.59734038"}))


@pytest.mark.asyncio
async def test_match_candidate_params_reject_limit_above_maximum():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module._normalize_match_candidate_params(
            _request({"phone": "9783435270", "limit": "51"})
        )


def test_taxonomy_scope_tokens_accept_exact_and_prefix():
    exact, prefixes = npi_module._taxonomy_scope_tokens("282N00000X, 261Q*")

    assert exact == ("282N00000X",)
    assert prefixes == ("261Q",)


def test_match_candidate_query_uses_site_key_and_taxonomy_filter():
    params = {
        "address_site_key": "c4147429-6998-8eb4-d3f8-c89ea64af4ca",
        "address_key": None,
        "lat": None,
        "long": None,
        "radius_miles": None,
        "phone_digits": None,
        "entity_type_code": 2,
        "taxonomy_exact": ("282N00000X",),
        "taxonomy_prefixes": ("261Q",),
        "provider_type": None,
        "specialty_filter": None,
        "limit": 5,
    }

    query, query_params = npi_module._match_candidate_query(params, "mrf.entity_address_unified")
    sql = str(query)

    assert "a.premise_key = CAST(:address_site_key AS uuid)" in sql
    assert "t.healthcare_provider_taxonomy_code = ANY(:match_taxonomy_codes)" in sql
    assert "t.healthcare_provider_taxonomy_code LIKE :match_taxonomy_prefix_0" in sql
    assert query_params["entity_type_code"] == 2
    assert query_params["match_taxonomy_codes"] == ["282N00000X"]
    assert query_params["match_taxonomy_prefix_0"] == "261Q%"


def test_match_candidate_query_keeps_explicit_taxonomy_scope_restrictive():
    query, query_params = npi_module._match_candidate_query(
        {
            "address_site_key": None,
            "address_key": "d8c8e7f0-d765-4786-9349-3663085a23b3",
            "lat": None,
            "long": None,
            "radius_miles": None,
            "phone_digits": "3125551212",
            "entity_type_code": 2,
            "taxonomy_exact": ("282N00000X",),
            "taxonomy_prefixes": ("261Q",),
            "provider_type": "hospital",
            "specialty_filter": types.SimpleNamespace(
                taxonomy_codes=("273R00000X",),
                classification=None,
            ),
            "limit": 5,
        },
        "mrf.entity_address_unified",
    )
    sql = str(query)
    candidate_locations_sql = sql.split("LIMIT :candidate_limit", 1)[0]

    assert query_params["match_taxonomy_codes"] == ["282N00000X"]
    assert query_params["match_taxonomy_prefix_0"] == "261Q%"
    assert "273R00000X" not in repr(query_params)
    assert "nf.npi = COALESCE(a.npi, a.inferred_npi)" in candidate_locations_sql
    assert "t.npi = COALESCE(a.npi, a.inferred_npi)" in candidate_locations_sql


def test_match_candidate_query_uses_indexable_geo_bbox_when_geo_is_only_locator():
    params = {
        "address_site_key": None,
        "address_key": None,
        "lat": 35.1295378,
        "long": -89.86039355,
        "radius_miles": 0.5,
        "phone_digits": None,
        "entity_type_code": None,
        "taxonomy_exact": (),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
        "limit": 5,
    }

    query, query_params = npi_module._match_candidate_query(params, "mrf.entity_address_unified")
    sql = str(query)

    assert "a.lat BETWEEN CAST(:lat_min AS numeric) AND CAST(:lat_max AS numeric)" in sql
    assert "a.long BETWEEN CAST(:long_min AS numeric) AND CAST(:long_max AS numeric)" in sql
    assert query_params["lat_min"] < params["lat"] < query_params["lat_max"]
    assert query_params["long_min"] < params["long"] < query_params["long_max"]


def test_match_candidate_query_keeps_geo_as_scoring_signal_with_exact_locator():
    params = {
        "address_site_key": "c4147429-6998-8eb4-d3f8-c89ea64af4ca",
        "address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670",
        "lat": 35.1295378,
        "long": -89.86039355,
        "radius_miles": 0.5,
        "phone_digits": "9012261309",
        "entity_type_code": None,
        "taxonomy_exact": (),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
        "limit": 5,
    }

    query, query_params = npi_module._match_candidate_query(params, "mrf.entity_address_unified")
    sql = str(query)

    assert "a.premise_key = CAST(:address_site_key AS uuid)" in sql
    assert "geo_distance_miles" in sql
    assert "a.lat BETWEEN CAST(:lat_min AS numeric) AND CAST(:lat_max AS numeric)" not in sql
    assert query_params["lat"] == params["lat"]
    assert query_params["address_key"] == params["address_key"]
    assert query_params["phone_digits"] == params["phone_digits"]


def test_match_candidate_output_scores_and_hides_internal_fields():
    row = {
        "npi": 1013995133,
        "entity_type_code": 2,
        "provider_organization_name": "UMASS MEMORIAL HEALTHALLIANCE CLINTON HOSPITAL INC",
        "address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670",
        "address_site_key": "68d0c41a-8871-1b9e-0000-000000000000",
        "premise_key": "must-not-leak",
        "address_site_key_matched": True,
        "address_key_matched": False,
        "phone_matched": True,
        "geo_distance_miles": 0.14,
        "address_type": "practice",
        "first_line": "326 Nichols Rd",
        "postal_code": "01420",
        "phone_number": "9783435270",
        "address_sources": ["nppes", "provider_directory_fhir"],
        "source_count": 1,
        "taxonomy_list": [
            {
                "taxonomy_code": "282N00000X",
                "primary": True,
                "display_name": "General Acute Care Hospital",
            }
        ],
    }
    params = {
        "radius_miles": 1.0,
        "taxonomy_exact": ("282N00000X",),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
        "include_sources": False,
        "include_evidence": True,
    }
    enrichment = {
        "has_any_enrollment": True,
        "has_ffs_enrollment": True,
        "has_medicare_claims": True,
        "has_hospital_enrollment": True,
        "primary_provider_type_code": "12",
    }

    candidate = npi_module._match_candidate_output(row, params, enrichment)

    assert candidate["npi"] == 1013995133
    assert candidate["address_site_key"] == "68d0c41a-8871-1b9e-0000-000000000000"
    assert "premise_key" not in json.dumps(candidate)
    assert candidate["entity_kind"] == "organization"
    assert candidate["match_score"] >= 0.9
    assert candidate["confidence_band"] == "high"
    assert candidate["match_signals"]["address_site_key"]["matched"] is True
    assert candidate["sources"]["fhir"]["matched"] is True
    assert candidate["sources"]["ffs"]["matched"] is True
    assert candidate["facility"]["classification_confidence"] == "high"


def test_match_candidate_output_boosts_provider_type_taxonomy_match():
    candidate = npi_module._match_candidate_output(
        {
            "npi": 1730166224,
            "entity_type_code": 2,
            "provider_organization_name": "INSIGHT CHICAGO, INC.",
            "address_key": "d8c8e7f0-d765-4786-9349-3663085a23b3",
            "address_key_matched": True,
            "address_site_key_matched": False,
            "phone_matched": False,
            "geo_distance_miles": None,
            "address_sources": ["nppes"],
            "source_count": 1,
            "taxonomy_list": [
                {
                    "taxonomy_code": "282N00000X",
                    "primary": True,
                    "display_name": "General Acute Care Hospital",
                }
            ],
        },
        {
            "radius_miles": 1.0,
            "taxonomy_exact": ("282N00000X",),
            "taxonomy_prefixes": ("261Q",),
            "provider_type": "hospital",
            "specialty_filter": types.SimpleNamespace(
                taxonomy_codes=("282N00000X",),
                classification=None,
            ),
            "include_sources": False,
            "include_evidence": True,
        },
        None,
    )

    assert candidate["match_score"] == 0.64
    assert candidate["match_signals"]["taxonomy"]["contribution"] == 0.14
    assert candidate["match_signals"]["taxonomy"]["provider_type_matched"] is True


def test_match_candidate_sort_key_orders_ties():
    def top_npi(candidates):
        return sorted(candidates, key=npi_module._match_candidate_sort_key)[0]["npi"]

    assert top_npi(
        [
            {"npi": 1306486022, "match_score": 0.65, "sources": {"fhir": {"source_count": 1}}},
            {"npi": 1730166224, "match_score": 0.65, "sources": {"fhir": {"source_count": 2}}},
        ]
    ) == 1730166224
    assert top_npi(
        [
            {
                "npi": 1306486022,
                "match_score": 1.0,
                "match_signals": {"taxonomy": {"matched": True}},
                "sources": {"fhir": {"source_count": 3}},
            },
            {
                "npi": 1730166224,
                "match_score": 1.0,
                "match_signals": {"taxonomy": {"provider_type_matched": True}},
                "sources": {"fhir": {"source_count": 1}},
            },
        ]
    ) == 1730166224


@pytest.mark.asyncio
async def test_match_candidates_route_shapes_payload(monkeypatch):
    async def fake_rows(params, *, session=None):
        assert params["address_key"] == "cb329e77-08b7-a9c4-5a2e-34f6ca32b670"
        return [
            {
                "npi": 1013995133,
                "entity_type_code": 2,
                "provider_organization_name": "Hospital",
                "address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670",
                "address_site_key": "68d0c41a-8871-1b9e-0000-000000000000",
                "address_site_key_matched": False,
                "address_key_matched": True,
                "phone_matched": False,
                "geo_distance_miles": None,
                "address_sources": [],
                "source_count": 0,
                "taxonomy_list": [],
            }
        ]

    async def fake_enrichment(npis, *, include_chain=False, session=None):
        return {1013995133: {"has_any_enrollment": True, "has_ffs_enrollment": True}}

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", fake_rows)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", fake_enrichment)

    response = await npi_module.match_candidates(
        _request({"address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670", "limit": "5"})
    )
    payload = json.loads(response.body)

    assert payload["total"] == 1
    assert payload["candidates"][0]["npi"] == 1013995133
    assert payload["candidates"][0]["address_key"] == "cb329e77-08b7-a9c4-5a2e-34f6ca32b670"
    assert payload["meta"]["timeout_ms"] == 8000


@pytest.mark.asyncio
async def test_match_candidates_route_returns_503_on_timeout(monkeypatch):
    async def fake_rows(params, *, session=None):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", fake_rows)

    with pytest.raises(sanic.exceptions.ServiceUnavailable):
        await npi_module.match_candidates(
            _request({"address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670"})
        )


@pytest.mark.asyncio
async def test_fetch_match_candidate_rows_rolls_back_session_after_timeout(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.rollback_called = False

        async def rollback(self):
            self.rollback_called = True

    async def fake_address_table(required_columns, *, session=None):
        return "mrf.entity_address_unified"

    def fake_query(params, address_table_sql):
        return object(), {}

    async def fake_execute(stmt, *, session=None, params=None):
        await asyncio.sleep(1)

    session = FakeSession()
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", fake_address_table)
    monkeypatch.setattr(npi_module, "_match_candidate_query", fake_query)
    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    monkeypatch.setattr(npi_module, "_MATCH_CANDIDATES_TIMEOUT_SECONDS", 0.001)

    with pytest.raises(asyncio.TimeoutError):
        await npi_module._fetch_match_candidate_rows({"address_key": "x"}, session=session)

    assert session.rollback_called is True


@pytest.mark.asyncio
async def test_fetch_match_candidate_rows_rolls_back_session_after_cancellation(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.rollback_called = False

        async def rollback(self):
            self.rollback_called = True

    async def fake_address_table(required_columns, *, session=None):
        return "mrf.entity_address_unified"

    def fake_query(params, address_table_sql):
        return object(), {}

    async def fake_execute(stmt, *, session=None, params=None):
        raise asyncio.CancelledError()

    session = FakeSession()
    monkeypatch.setattr(npi_module, "_address_serving_table_sql", fake_address_table)
    monkeypatch.setattr(npi_module, "_match_candidate_query", fake_query)
    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)

    with pytest.raises(asyncio.CancelledError):
        await npi_module._fetch_match_candidate_rows({"address_key": "x"}, session=session)

    assert session.rollback_called is True
