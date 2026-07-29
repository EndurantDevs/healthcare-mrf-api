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
