# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Public response boundaries for site-intelligence provider supply scoring."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import site_intelligence


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, responses):
        self._responses = list(responses)

    async def execute(self, _statement):
        if not self._responses:
            raise AssertionError("unexpected site-intelligence query")
        return _Rows(self._responses.pop(0))


class _FailingSpatialSession:
    def get_bind(self):
        return object()

    async def execute(self, _statement, _params=None):
        raise RuntimeError("synthetic spatial query failure")


def _population_responses():
    return [
        [
            SimpleNamespace(
                zip_code="",
                state="IL",
                latitude="bad",
                longitude=-87.635,
            ),
            SimpleNamespace(
                zip_code="99999",
                state="IL",
                latitude=42.5,
                longitude=-87.635,
            ),
            SimpleNamespace(
                zip_code="60654",
                state="IL",
                latitude=41.892,
                longitude=-87.635,
            ),
        ],
        [
            SimpleNamespace(
                zcta_code="60654",
                year=2026,
                total_beneficiaries=20_000,
                part_d_beneficiaries=15_000,
            )
        ],
        [
            SimpleNamespace(
                zcta_code="60654",
                year=2026,
                total_workers=40_000,
            )
        ],
        [
            SimpleNamespace(
                zcta="60654",
                year=2026,
                measure_id="OTHER",
                measure_name="Unrelated measure",
                data_value=10.0,
            ),
            SimpleNamespace(
                zcta="60654",
                year=2026,
                measure_id="DIABETES",
                measure_name="Diabetes among adults",
                data_value=None,
            ),
        ],
    ]


def _provider_responses():
    providers = [
        SimpleNamespace(
            zip_code="60654",
            npi=1_000_000_000 + ordinal,
            provider_type="NURSE PRACTITIONER",
        )
        for ordinal in range(80)
    ]
    providers.append(
        SimpleNamespace(zip_code="", npi=0, provider_type="NURSE PRACTITIONER")
    )
    pharmacies = [
        SimpleNamespace(
            zip_code="60654",
            npi=2_000_000_000 + ordinal,
            medicare_active=True,
        )
        for ordinal in range(21)
    ]
    pharmacies.append(
        SimpleNamespace(zip_code="", npi=0, medicare_active=True)
    )
    return providers, pharmacies


def _pharmacy_geo_rows():
    return [
        SimpleNamespace(npi=0, lat=41.892, long=-87.635),
        SimpleNamespace(npi=3_000_000_001, lat=42.5, long=-87.635),
        SimpleNamespace(npi=3_000_000_002, lat=41.90, long=-87.635),
        SimpleNamespace(npi=3_000_000_002, lat=41.893, long=-87.635),
    ]


def _scoring_session():
    providers, pharmacies = _provider_responses()
    economics = [
        SimpleNamespace(
            drug_name="Test generic",
            sdud_volume=100,
            estimated_gross_margin=15.0,
        ),
        SimpleNamespace(
            drug_name="Ignored volume",
            sdud_volume=0,
            estimated_gross_margin=None,
        ),
    ]
    return _Session(
        [
            *_population_responses(),
            providers,
            pharmacies,
            _pharmacy_geo_rows(),
            economics,
        ]
    )


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        ({"lat": "north", "lng": "-87"}, "must be numeric"),
        ({"lat": "91", "lng": "-87"}, "lat must be between"),
        ({"lat": "41", "lng": "181"}, "lng must be between"),
    ],
)
@pytest.mark.asyncio
async def test_site_score_rejects_malformed_and_out_of_range_coordinates(
    arguments,
    message,
):
    request = SimpleNamespace(args=arguments, ctx=SimpleNamespace())
    with pytest.raises(InvalidUsage, match=message):
        await site_intelligence.get_site_score(request)


@pytest.mark.asyncio
async def test_site_score_reports_missing_zip_reference_without_querying(monkeypatch):
    async def is_table_available(_session, model):
        return model is not site_intelligence.GeoZipLookup

    monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: object())
    monkeypatch.setattr(site_intelligence, "_is_table_cached", is_table_available)
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635"},
        ctx=SimpleNamespace(),
    )

    response_payload = json.loads(
        (await site_intelligence.get_site_score(request)).body
    )

    assert response_payload["drivers"]["negative"] == [
        "ZIP reference dataset is unavailable"
    ]
    assert response_payload["supply_metrics"]["provider_count"] == 0
    assert response_payload["recommendation"]["final_decision"] == "Not Recommended"


@pytest.mark.asyncio
async def test_site_score_filters_bad_provider_rows_before_high_supply_scoring(
    monkeypatch,
):
    async def is_table_available(_session, _model):
        return True

    async def radius_weights(_session, _lat, _lng, _radius):
        return {"60654": 1.0}, "test_exact_zip"

    async def anchor_candidates(*_args, **_kwargs):
        return {
            "Hospital": [{"name": "Hospital", "miles": 5.0}],
            "FQHC": [{"name": "FQHC", "miles": 5.0}],
        }

    monkeypatch.setattr(
        site_intelligence,
        "_get_session",
        lambda _request: _scoring_session(),
    )
    monkeypatch.setattr(site_intelligence, "_is_table_cached", is_table_available)
    monkeypatch.setattr(site_intelligence, "_radius_zip_weights", radius_weights)
    monkeypatch.setattr(
        site_intelligence,
        "_load_anchor_candidates",
        anchor_candidates,
    )
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635"},
        ctx=SimpleNamespace(),
    )

    response_payload = json.loads(
        (await site_intelligence.get_site_score(request)).body
    )

    assert response_payload["demand_metrics"]["total_seniors"] == "20,000"
    assert response_payload["demand_metrics"]["daytime_workers"] == "40,000"
    assert response_payload["supply_metrics"]["provider_count"] == 80
    assert response_payload["supply_metrics"]["np_pa_count"] == 80
    assert response_payload["supply_metrics"]["active_pharmacy_count"] == 21
    assert response_payload["supply_metrics"]["pharmacy_count_radius"] == 1
    assert response_payload["score_components"] == {
        "demand": 30.0,
        "prescriber": 25.0,
        "competition": -12.0,
        "economics": 15.0,
        "anchors": 5.0,
        "total": 63.0,
    }
    assert "High active-pharmacy competition nearby" in response_payload["drivers"][
        "negative"
    ]


async def _is_geo_only_table_available(_session, model):
    return model is site_intelligence.GeoZipLookup


async def _is_table_available(_session, _model):
    return True


def _score_scenario_session(
    *,
    seniors: int,
    workers: int,
    prescribers: int,
    active_pharmacies: int,
    margin: float,
    include_inactive_pharmacy: bool,
):
    zip_rows = [
        SimpleNamespace(zip_code="60654", state="IL", latitude=41.892, longitude=-87.635),
        SimpleNamespace(zip_code="60655", state="IL", latitude=41.952, longitude=-87.635),
    ]
    medicare_rows = [
        SimpleNamespace(zcta_code="60654", year=2026, total_beneficiaries=seniors, part_d_beneficiaries=seniors),
        SimpleNamespace(zcta_code="60654", year=2025, total_beneficiaries=1, part_d_beneficiaries=1),
    ]
    workplace_rows = [
        SimpleNamespace(zcta_code="60654", year=2026, total_workers=workers),
        SimpleNamespace(zcta_code="60654", year=2025, total_workers=1),
    ]
    places_rows = [
        SimpleNamespace(zcta="60654", year=2026, measure_id="DIABETES", measure_name="Diabetes", data_value=12.0),
        SimpleNamespace(zcta="60654", year=2025, measure_id="DIABETES", measure_name="Diabetes", data_value=10.0),
        SimpleNamespace(zcta="60655", year=2026, measure_id="COPD", measure_name="COPD", data_value=8.0),
        SimpleNamespace(zcta="99999", year=2026, measure_id="OTHER", measure_name="Other", data_value=4.0),
    ]
    provider_rows = [
        SimpleNamespace(zip_code="60654", npi=1_000_000_000 + index, provider_type="NURSE PRACTITIONER")
        for index in range(prescribers)
    ]
    pharmacy_rows = [
        SimpleNamespace(zip_code="60654", npi=2_000_000_000 + index, medicare_active=True)
        for index in range(active_pharmacies)
    ]
    if include_inactive_pharmacy:
        pharmacy_rows.append(SimpleNamespace(zip_code="60654", npi=2_999_999_999, medicare_active=False))
    pharmacy_geo_rows = [
        SimpleNamespace(npi=2_000_000_000, lat=41.893, long=-87.635),
        SimpleNamespace(npi=2_000_000_000, lat=41.91, long=-87.635),
    ]
    economics_rows = [SimpleNamespace(drug_name="Synthetic generic", sdud_volume=100, estimated_gross_margin=margin)]
    return _Session([zip_rows, medicare_rows, workplace_rows, places_rows, provider_rows, pharmacy_rows, pharmacy_geo_rows, economics_rows])


@pytest.mark.asyncio
async def test_site_score_sparse_geo_only_uses_nearest_zip_fallback(monkeypatch):
    async def empty_radius_weights(*_args):
        return {}, "zip_centroid"

    async def empty_anchors(*_args, **_kwargs):
        return {"Hospital": [], "FQHC": []}

    session = _Session(
        [[SimpleNamespace(zip_code="60654", state="IL", latitude=41.902, longitude=-87.635)]]
    )
    monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: session)
    monkeypatch.setattr(site_intelligence, "_is_table_cached", _is_geo_only_table_available)
    monkeypatch.setattr(site_intelligence, "_radius_zip_weights", empty_radius_weights)
    monkeypatch.setattr(site_intelligence, "_load_anchor_candidates", empty_anchors)
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635", "radius_miles": "0.1"},
        ctx=SimpleNamespace(),
    )

    response_payload = json.loads((await site_intelligence.get_site_score(request)).body)

    assert response_payload["score_value"] == 0.0
    assert response_payload["demand_metrics"]["total_seniors"] == "0"
    assert response_payload["supply_metrics"]["provider_count"] == 0
    assert response_payload["methodology"]["demand_scope"] == "selected_radius_address_zip_fallback"
    assert response_payload["methodology"]["demand_scope_radius_miles"] == 0.1
    assert "Low Medicare-eligible population in selected local radius" in response_payload["drivers"]["negative"]


@pytest.mark.asyncio
async def test_site_score_reports_empty_zip_query_result(monkeypatch):
    monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: _Session([[]]))
    monkeypatch.setattr(site_intelligence, "_is_table_cached", _is_geo_only_table_available)
    request = SimpleNamespace(args={"lat": "41.892", "lng": "-87.635"}, ctx=SimpleNamespace())

    response_payload = json.loads((await site_intelligence.get_site_score(request)).body)

    assert response_payload["drivers"]["negative"] == [
        "No ZIP centroids found in a 15-minute trade area"
    ]
    assert response_payload["recommendation"]["final_decision"] == "Not Recommended"


@pytest.mark.asyncio
async def test_site_score_covers_upper_middle_thresholds_and_target_adjustment(monkeypatch):
    async def radius_weights(*_args):
        return {"60654": 1.0}, "zcta_polygon_overlap"

    async def nearby_anchors(*_args, **_kwargs):
        return {
            "Hospital": [{"name": "Hospital", "miles": 1.0}],
            "FQHC": [{"name": "FQHC", "miles": 1.0}],
        }

    session = _score_scenario_session(
        seniors=15_000,
        workers=30_000,
        prescribers=40,
        active_pharmacies=7,
        margin=6.0,
        include_inactive_pharmacy=True,
    )
    monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: session)
    monkeypatch.setattr(site_intelligence, "_is_table_cached", _is_table_available)
    monkeypatch.setattr(site_intelligence, "_radius_zip_weights", radius_weights)
    monkeypatch.setattr(site_intelligence, "_load_anchor_candidates", nearby_anchors)
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635", "target_scripts_per_day": "300"},
        ctx=SimpleNamespace(),
    )

    response_payload = json.loads((await site_intelligence.get_site_score(request)).body)

    assert response_payload["score_components"] == {
        "demand": 30.0,
        "prescriber": 18.0,
        "competition": 10.0,
        "economics": 4.0,
        "anchors": 10.0,
        "total": 72.0,
    }
    assert response_payload["recommendation"] == {
        "base_decision": "Recommend",
        "final_decision": "Conditional",
        "rationale": response_payload["recommendation"]["rationale"],
        "target_adjusted": True,
    }


@pytest.mark.asyncio
async def test_site_score_covers_lower_middle_supply_thresholds(monkeypatch):
    async def radius_weights(*_args):
        return {"60654": 1.0}, "zcta_polygon_overlap"

    async def empty_anchors(*_args, **_kwargs):
        return {"Hospital": [], "FQHC": []}

    session = _score_scenario_session(
        seniors=2_000,
        workers=5_000,
        prescribers=20,
        active_pharmacies=15,
        margin=6.0,
        include_inactive_pharmacy=False,
    )
    monkeypatch.setattr(site_intelligence, "_get_session", lambda _request: session)
    monkeypatch.setattr(site_intelligence, "_is_table_cached", _is_table_available)
    monkeypatch.setattr(site_intelligence, "_radius_zip_weights", radius_weights)
    monkeypatch.setattr(site_intelligence, "_load_anchor_candidates", empty_anchors)
    request = SimpleNamespace(
        args={"lat": "41.892", "lng": "-87.635", "target_scripts_per_day": "10"},
        ctx=SimpleNamespace(),
    )

    response_payload = json.loads((await site_intelligence.get_site_score(request)).body)

    assert response_payload["expected_volume"]["daily"] == 10
    assert response_payload["target_assessment"]["target_met"] is True
    assert response_payload["score_components"]["prescriber"] == 10.0
    assert response_payload["score_components"]["competition"] == 2.0
    assert response_payload["score_components"]["economics"] == 4.0


@pytest.mark.asyncio
async def test_spatial_fallbacks_do_not_require_session_rollback(monkeypatch):
    site_intelligence._TABLE_EXISTS_CACHE.clear()
    assert (
        await site_intelligence._is_zcta_overlap_available(
            _FailingSpatialSession()
        )
        is False
    )

    async def is_overlap_available(_session):
        return True

    monkeypatch.setattr(
        site_intelligence,
        "_is_zcta_overlap_available",
        is_overlap_available,
    )
    weights_by_zip, method = await site_intelligence._radius_zip_weights(
        _FailingSpatialSession(),
        41.892,
        -87.635,
        5.0,
    )
    assert weights_by_zip == {}
    assert method == "zip_centroid"


@pytest.mark.asyncio
async def test_nearest_anchor_ignores_a_later_farther_candidate():
    session = _Session(
        [
            [
                SimpleNamespace(
                    name="Near hospital",
                    latitude=41.893,
                    longitude=-87.635,
                ),
                SimpleNamespace(
                    name="Far hospital",
                    latitude=41.95,
                    longitude=-87.635,
                ),
            ]
        ]
    )

    nearest_anchor = await site_intelligence._nearest_anchor(
        session,
        41.892,
        -87.635,
        "Hospital",
        10.0,
        facility_table_available=True,
    )

    assert nearest_anchor["name"] == "Near hospital"
