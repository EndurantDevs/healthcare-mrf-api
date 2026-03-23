# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import json
from types import SimpleNamespace

import pytest
from sanic.exceptions import InvalidUsage


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


@pytest.mark.asyncio
async def test_site_intelligence_requires_coordinates(site_intel_module):
    request = SimpleNamespace(args={}, ctx=SimpleNamespace())
    with pytest.raises(InvalidUsage):
        await site_intel_module.get_site_score(request)


@pytest.mark.asyncio
async def test_site_intelligence_returns_trade_area_metrics(monkeypatch, site_intel_module):
    session = _FakeSession(
        [
            # zip centroid candidates
            [
                SimpleNamespace(zip_code="60654", state="IL", latitude=41.892, longitude=-87.637),
                SimpleNamespace(zip_code="60610", state="IL", latitude=41.905, longitude=-87.632),
            ],
            # medicare rows
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
            # lodes rows
            [
                SimpleNamespace(zcta_code="60654", total_workers=15000),
                SimpleNamespace(zcta_code="60610", total_workers=9000),
            ],
            # places rows
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
            # provider rows in zips
            [
                SimpleNamespace(zip_code="60654", npi=1001, provider_type="NURSE PRACTITIONER"),
                SimpleNamespace(zip_code="60654", npi=1002, provider_type="PHYSICIAN ASSISTANT"),
                SimpleNamespace(zip_code="60610", npi=1003, provider_type="INTERNAL MEDICINE"),
            ],
            # active pharmacies
            [
                SimpleNamespace(zip_code="60654", npi=2001),
                SimpleNamespace(zip_code="60610", npi=2002),
            ],
            # nearest hospital
            [SimpleNamespace(latitude=41.88, longitude=-87.63)],
            # nearest fqhc
            [SimpleNamespace(latitude=41.90, longitude=-87.64)],
            # economics
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
    )
    monkeypatch.setattr(site_intel_module, "_get_session", lambda _request: session)
    request = SimpleNamespace(args={"lat": "41.892", "lng": "-87.635"}, ctx=SimpleNamespace())

    resp = await site_intel_module.get_site_score(request)
    payload = json.loads(resp.body)

    assert payload["trade_areas"]["15"]["metrics"]["total_seniors"] == 10100
    assert payload["trade_areas"]["15"]["metrics"]["np_pa_count"] == 2
    assert payload["supply_metrics"]["provider_count"] == 3
    assert payload["supply_metrics"]["np_pa_count"] == 2
    assert payload["demand_metrics"]["chronic_disease_rate"].endswith("% avg")
    assert payload["confidence"] != "85%"
    assert payload["methodology"]["state_used_for_economics"] == "IL"
