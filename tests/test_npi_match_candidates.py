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
async def test_match_candidate_params_reject_zip_alias_conflict():
    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module._normalize_match_candidate_params(
            _request(
                {
                    "first_line": "326 Nichols Rd",
                    "zip": "01420",
                    "zip_code": "01421",
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
            "first_line": "326 Nichols Rd.",
            "zip": "01420-1234",
            "zip_code": "01420",
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
    assert params["first_line_norm"] == "326nicholsrd"
    assert params["zip_code"] == "01420"
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

    assert params["radius_miles"] == 5.0


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
        "first_line_norm": None,
        "zip_code": None,
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
        "raw_address_matched": False,
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
                "raw_address_matched": False,
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
    assert payload["meta"]["timeout_ms"] == 2000


@pytest.mark.asyncio
async def test_match_candidates_route_returns_503_on_timeout(monkeypatch):
    async def fake_rows(params, *, session=None):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(npi_module, "_fetch_match_candidate_rows", fake_rows)

    with pytest.raises(sanic.exceptions.ServiceUnavailable):
        await npi_module.match_candidates(
            _request({"address_key": "cb329e77-08b7-a9c4-5a2e-34f6ca32b670"})
        )
