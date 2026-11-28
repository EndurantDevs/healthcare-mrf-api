# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import InvalidUsage
from sqlalchemy.dialects.postgresql import asyncpg


@pytest.fixture
def site_intel_module():
    return importlib.import_module("api.endpoint.site_intelligence")


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    async def execute(self, _stmt):
        if not self._responses:
            raise AssertionError("Unexpected query execution")
        return _FakeResult(self._responses.pop(0))


def _site_population_responses():
    return [
        [
            SimpleNamespace(
                zip_code="60654",
                state="IL",
                latitude=41.892,
                longitude=-87.637,
            ),
            SimpleNamespace(
                zip_code="60610",
                state="IL",
                latitude=41.905,
                longitude=-87.632,
            ),
        ],
        [
            SimpleNamespace(
                zcta_code="60654",
                year=2025,
                total_beneficiaries=6200,
                part_d_beneficiaries=4500,
            ),
            SimpleNamespace(
                zcta_code="60610",
                year=2025,
                total_beneficiaries=3900,
                part_d_beneficiaries=2500,
            ),
        ],
        [
            SimpleNamespace(zcta_code="60654", total_workers=15000),
            SimpleNamespace(zcta_code="60610", total_workers=9000),
        ],
        [
            SimpleNamespace(
                zcta="60654",
                year=2025,
                measure_id="DIABETES",
                measure_name="Diabetes among adults",
                data_value=12.7,
            ),
            SimpleNamespace(
                zcta="60610",
                year=2025,
                measure_id="BPHIGH",
                measure_name="High blood pressure among adults",
                data_value=24.3,
            ),
        ],
    ]


def _site_supply_responses():
    return [
        [
            SimpleNamespace(
                zip_code="60654",
                npi=1001,
                provider_type="NURSE PRACTITIONER",
            ),
            SimpleNamespace(
                zip_code="60654",
                npi=1002,
                provider_type="PHYSICIAN ASSISTANT",
            ),
            SimpleNamespace(
                zip_code="60610",
                npi=1003,
                provider_type="INTERNAL MEDICINE",
            ),
        ],
        [
            SimpleNamespace(zip_code="60654", npi=2001, medicare_active=True),
            SimpleNamespace(zip_code="60610", npi=2002, medicare_active=True),
        ],
        [],
        [
            SimpleNamespace(
                facility_type="Hospital",
                name="Hospital A",
                latitude=41.88,
                longitude=-87.63,
            ),
            SimpleNamespace(
                facility_type="FQHC",
                name="FQHC B",
                latitude=41.90,
                longitude=-87.64,
            ),
        ],
        [
            SimpleNamespace(
                drug_name="Metformin HCL",
                sdud_volume=20000,
                estimated_gross_margin=13.4,
            ),
            SimpleNamespace(
                drug_name="Lisinopril",
                sdud_volume=12000,
                estimated_gross_margin=10.2,
            ),
        ],
    ]


async def _is_site_table_present(_session, _model):
    return True


def _install_site_score_fixture(monkeypatch, site_intel_module) -> None:
    session = _FakeSession(_site_population_responses() + _site_supply_responses())
    monkeypatch.setattr(site_intel_module, "_get_session", lambda _request: session)
    monkeypatch.setattr(
        site_intel_module,
        "_is_table_cached",
        _is_site_table_present,
    )


@pytest.mark.asyncio
async def test_site_intelligence_requires_coordinates(site_intel_module):
    request = SimpleNamespace(args={}, ctx=SimpleNamespace())
    with pytest.raises(InvalidUsage):
        await site_intel_module.get_site_score(request)


def test_site_intelligence_pharmacy_geo_stmt_uses_legacy_by_default(site_intel_module):
    stmt = site_intel_module._pharmacy_geo_stmt(
        use_unified_addresses=False,
        lat=41.892,
        lng=-87.635,
        lat_delta=0.1,
        lng_delta=0.1,
    )
    compiled = str(stmt.compile(compile_kwargs={"literal_binds": True})).lower()

    assert "npi_address" in compiled
    assert "entity_address_unified" not in compiled
    assert "address_precision" not in compiled
    assert "npi_taxonomy" in compiled
    assert "3336" in compiled


def test_site_intelligence_address_source_defaults_to_unified(site_intel_module, monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)

    assert site_intel_module._address_serving_source() == "entity_address_unified"


def test_site_intelligence_pharmacy_geo_stmt_uses_unified_addresses(site_intel_module):
    stmt = site_intel_module._pharmacy_geo_stmt(
        use_unified_addresses=True,
        lat=41.892,
        lng=-87.635,
        lat_delta=0.1,
        lng_delta=0.1,
    )
    compiled = stmt.compile(dialect=asyncpg.dialect(), compile_kwargs={"render_postcompile": True})
    sql = str(compiled).lower()
    address_table = site_intel_module.EntityAddressUnified.__table__.fullname

    assert "entity_address_unified" in sql
    assert "npi_address" not in sql
    assert "exists" in sql
    assert "join lateral" not in sql
    assert f"npi_taxonomy.npi = {address_table}.npi" in sql
    assert "npi_taxonomy.healthcare_provider_taxonomy_code like" in sql
    assert "taxonomy_array" not in sql
    assert "3336%" in compiled.params.values()
    assert sql.count("::numeric") == 4
    assert ".type = 'primary'" in sql
    assert f"coalesce({address_table}.address_precision, '') != 'city_zip'" in sql


@pytest.mark.parametrize(
    ("lat", "lng", "radius", "use_spatial"),
    [(39.0, -95.0, 5, True), (89.5, 30.0, 5, True), (89.99, 30.0, 5, False), (0, 179.99, 5, False)],
)
def test_site_intelligence_pharmacy_geo_prefilter_keeps_the_exact_coordinate_window(
    site_intel_module, lat, lng, radius, use_spatial
):
    stmt = site_intel_module._pharmacy_geo_stmt(
        use_unified_addresses=True,
        lat=lat,
        lng=lng,
        lat_delta=0.07,
        lng_delta=0.12,
        geography_radius_miles=radius,
    )
    compiled = stmt.compile(dialect=asyncpg.dialect(), compile_kwargs={"render_postcompile": True})
    sql = str(compiled).lower()

    assert sql.count("::numeric") == 4
    assert ("_st_expand" in sql) is use_spatial
    assert ("join lateral" in sql) is use_spatial
    if use_spatial:
        assert "operator(public.&&)" in sql
        assert sql.index("offset") < sql.index("between")


@pytest.mark.asyncio
async def test_site_intelligence_returns_trade_area_metrics(monkeypatch, site_intel_module):
    """Verify site intelligence returns trade area metrics."""
    _install_site_score_fixture(monkeypatch, site_intel_module)
    request = SimpleNamespace(args={"lat": "41.892", "lng": "-87.635"}, ctx=SimpleNamespace())

    resp = await site_intel_module.get_site_score(request)
    response_payload = json.loads(resp.body)

    assert response_payload["trade_areas"]["15"]["metrics"]["total_seniors"] == 10100
    assert response_payload["trade_areas"]["15"]["metrics"]["np_pa_count"] == 2
    assert response_payload["supply_metrics"]["provider_count"] == 3
    assert response_payload["supply_metrics"]["np_pa_count"] == 2
    assert response_payload["demand_metrics"]["chronic_disease_rate"].endswith("% avg")
    assert response_payload["confidence"] != "85%"
    assert response_payload["methodology"]["state_used_for_economics"] == "IL"
    assert response_payload["expected_volume"]["daily"] >= 10
    assert response_payload["score_components"]["total"] == response_payload["score_value"]
    assert response_payload["target_assessment"]["target_daily"] == 100
    assert response_payload["recommendation"]["final_decision"] in (
        "Recommend",
        "Conditional",
        "Not Recommended",
    )


@pytest.mark.asyncio
async def test_site_intelligence_target_scripts_gates_recommendation(monkeypatch, site_intel_module):
    """Verify site intelligence target scripts gates recommendation."""
    _install_site_score_fixture(monkeypatch, site_intel_module)
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635", "target_scripts_per_day": "250"},
        ctx=SimpleNamespace(),
    )
    resp = await site_intel_module.get_site_score(request)
    response_payload = json.loads(resp.body)

    assert response_payload["target_assessment"]["target_daily"] == 250
    assert response_payload["target_assessment"]["target_met"] is False
    assert response_payload["recommendation"]["base_decision"] in (
        "Recommend",
        "Conditional",
        "Not Recommended",
    )
    assert response_payload["recommendation"]["final_decision"] != "Recommend"
    if response_payload["recommendation"]["base_decision"] == "Recommend":
        assert response_payload["recommendation"]["target_adjusted"] is True


def test_radius_zip_overlap_sql_uses_wgs84(site_intel_module):
    """Verify radius ZIP overlap query transforms NAD83 ZCTA geometries and uses the GiST prefilter."""
    overlap_sql = site_intel_module._radius_zip_overlap_sql().text
    assert "ST_Transform(z.the_geom, 4326)::geography" in overlap_sql
    assert "z.the_geom::geography" not in overlap_sql
    assert "z.the_geom && ST_Transform(c.geom::geometry, 4269)" in overlap_sql
