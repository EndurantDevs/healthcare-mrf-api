# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "api" / "endpoint" / "pricing.py"
MODULE_SPEC = spec_from_file_location("pricing_endpoint_unit", MODULE_PATH)
pricing_module = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(pricing_module)

get_provider_procedure = pricing_module.get_provider_procedure
get_provider_procedure_estimated_cost_level_internal = pricing_module.get_provider_procedure_estimated_cost_level_internal
get_procedure_geo_benchmarks = pricing_module.get_procedure_geo_benchmarks
get_provider_prescription = pricing_module.get_provider_prescription
get_prescription_benchmarks = pricing_module.get_prescription_benchmarks
get_pricing_provider_score = pricing_module.get_pricing_provider_score
autocomplete_prescriptions = pricing_module.autocomplete_prescriptions
autocomplete_procedures = pricing_module.autocomplete_procedures
list_pricing_providers = pricing_module.list_pricing_providers
list_providers_by_prescription = pricing_module.list_providers_by_prescription
list_providers_by_procedure = pricing_module.list_providers_by_procedure
list_procedure_providers = pricing_module.list_procedure_providers
list_prescription_providers = pricing_module.list_prescription_providers
list_provider_procedure_locations = pricing_module.list_provider_procedure_locations
list_provider_procedures = pricing_module.list_provider_procedures
list_provider_prescriptions = pricing_module.list_provider_prescriptions


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results=None):
        self._results = list(results or [])

    async def execute(self, *_args, **_kwargs):
        if self._results:
            return self._results.pop(0)
        return FakeResult([], 0)


def make_request(results, args=None):
    session = FakeSession(results)
    return types.SimpleNamespace(
        args=args or {},
        ctx=types.SimpleNamespace(sa_session=session),
    )


@pytest.mark.asyncio
async def test_list_pricing_providers_success():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[{"npi": 1003000126, "provider_name": "Enkeshafi, Ardalan", "total_drug_cost": 1234.5}]),
        ],
        args={"year": "2023", "limit": "10", "order_by": "total_drug_cost", "order": "desc"},
    )

    response = await list_pricing_providers(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126


@pytest.mark.asyncio
async def test_list_provider_procedures_success():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "procedure_code": 123,
                        "generic_name": "Apixaban",
                        "total_claims": 45,
                        "total_drug_cost": 998.7,
                    }
                ]
            ),
        ],
        args={"year": "2023", "limit": "10", "order_by": "total_claims", "order": "desc"},
    )

    response = await list_provider_procedures(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["procedure_code"] == 123
    assert payload["items"][0]["service_code_system"] == "HP_PROCEDURE_CODE"
    assert payload["items"][0]["service_code"] == "123"
    assert payload["items"][0]["service_name"] == "Apixaban"
    assert payload["items"][0]["total_services"] == 45
    assert payload["items"][0]["total_allowed_amount"] == 998.7
    assert "total_30day_fills" not in payload["items"][0]
    assert "total_day_supply" not in payload["items"][0]
    assert "total_drug_cost" not in payload["items"][0]


@pytest.mark.asyncio
async def test_get_provider_procedure_not_found():
    request = make_request([FakeResult(rows=[])], args={"year": "2023"})

    with pytest.raises(Exception) as exc:
        await get_provider_procedure(request, "1003000126", "999")

    assert "not found" in str(exc.value).lower()


@pytest.mark.asyncio
async def test_list_provider_procedure_locations_success():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "location_key": 1,
                        "city": "Bethesda",
                        "state": "MD",
                        "procedure_code": 321,
                        "generic_name": "Example Service",
                        "total_drug_cost": 55.0,
                    }
                ]
            ),
        ],
        args={"year": "2023", "order_by": "city", "order": "asc"},
    )

    response = await list_provider_procedure_locations(request, "1003000126", "123")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["city"] == "Bethesda"
    assert payload["items"][0]["service_code"] == "321"
    assert payload["items"][0]["total_allowed_amount"] == 55.0


@pytest.mark.asyncio
async def test_list_provider_procedures_legacy_fields_opt_in():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "procedure_code": 123,
                        "generic_name": "Apixaban",
                        "brand_name": "70553",
                        "total_claims": 45,
                        "total_30day_fills": 40,
                        "total_day_supply": 50,
                        "total_drug_cost": 998.7,
                        "total_benes": 30,
                    }
                ]
            ),
        ],
        args={"year": "2023", "include_legacy_fields": "1"},
    )

    response = await list_provider_procedures(request, "1003000126")
    payload = json.loads(response.body)
    item = payload["items"][0]
    assert item["reported_code"] == "70553"
    assert item["reported_code_system"] == "CPT"
    assert item["total_30day_fills"] == 40
    assert item["total_day_supply"] == 50
    assert item["total_drug_cost"] == 998.7


@pytest.mark.asyncio
async def test_list_provider_procedures_with_code_resolution():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "from_system": "HCPCS",
                        "from_code": "A1234",
                        "to_system": "HP_PROCEDURE_CODE",
                        "to_code": "123",
                        "match_type": "exact",
                        "confidence": 1.0,
                        "source": "test",
                    }
                ]
            ),
            FakeResult(scalar=1),
            FakeResult(rows=[{"npi": 1003000126, "procedure_code": 123, "generic_name": "Apixaban"}]),
        ],
        args={"year": "2023", "code": "A1234", "code_system": "HCPCS"},
    )

    response = await list_provider_procedures(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["query"]["input_code"]["code_system"] == "HCPCS"
    assert payload["query"]["resolved_codes"][0]["code_system"] == "HCPCS"


@pytest.mark.asyncio
async def test_list_procedure_providers_success():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(rows=[{"npi": 1003000126, "provider_name": "Enkeshafi, Ardalan", "total_drug_cost": 1234.5}]),
        ],
        args={"year": "2023", "limit": "10", "order_by": "total_drug_cost", "order": "desc"},
    )

    response = await list_procedure_providers(request, "HP_PROCEDURE_CODE", "123")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["query"]["input_code"]["code_system"] == "HP_PROCEDURE_CODE"


@pytest.mark.asyncio
async def test_get_pricing_provider_score_success():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "state": "MD",
                        "total_drug_cost": 1200.0,
                        "total_claims": 200.0,
                        "total_benes": 150.0,
                    }
                ]
            ),
            FakeResult(scalar=100),
            FakeResult(scalar=65),
            FakeResult(scalar=80),
            FakeResult(scalar=70),
            FakeResult(scalar=12),
            FakeResult(scalar=4),
        ],
        args={"year": "2023"},
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["npi"] == 1003000126
    assert payload["estimated_cost_level"] in {"$", "$$", "$$$", "$$$$", "$$$$$"}
    assert payload["quality_proxy_score"] >= 0


@pytest.mark.asyncio
async def test_list_provider_prescriptions_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "rx_code_system": "HP_RX_CODE",
                        "rx_code": "12345",
                        "rx_name": "Atorvastatin",
                        "generic_name": "Atorvastatin",
                        "brand_name": "Lipitor",
                        "total_claims": 100,
                        "total_drug_cost": 1200.0,
                        "total_benes": 80,
                    }
                ]
            ),
        ],
        args={"year": "2023"},
    )

    response = await list_provider_prescriptions(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["prescription_code_system"] == "HP_RX_CODE"
    assert payload["items"][0]["prescription_code"] == "12345"
    assert payload["items"][0]["prescription_name"] == "Atorvastatin"
    assert payload["items"][0]["total_prescriptions"] == 100
    assert payload["items"][0]["total_allowed_amount"] == 1200.0
    assert payload["items"][0]["preferred_prescription_code_system"] == "HP_RX_CODE"
    assert payload["items"][0]["preferred_prescription_code"] == "12345"


@pytest.mark.asyncio
async def test_list_provider_prescriptions_prefers_ndc_then_rxnorm():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "rx_code_system": "HP_RX_CODE",
                        "rx_code": "12345",
                        "rx_name": "Atorvastatin",
                        "generic_name": "Atorvastatin",
                        "brand_name": "Lipitor",
                        "total_claims": 100,
                        "total_drug_cost": 1200.0,
                        "total_benes": 80,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "from_system": "HP_RX_CODE",
                        "from_code": "12345",
                        "to_system": "RXNORM",
                        "to_code": "83367",
                    },
                    {
                        "from_system": "HP_RX_CODE",
                        "from_code": "12345",
                        "to_system": "NDC",
                        "to_code": "00071015523",
                    },
                ]
            ),
        ],
        args={"year": "2023"},
    )

    response = await list_provider_prescriptions(request, "1003000126")
    payload = json.loads(response.body)
    item = payload["items"][0]
    assert item["ndc_code"] == "00071015523"
    assert item["rxnorm_id"] == "83367"
    assert item["preferred_prescription_code_system"] == "NDC"
    assert item["preferred_prescription_code"] == "00071015523"


@pytest.mark.asyncio
async def test_get_provider_prescription_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "rx_code_system": "HP_RX_CODE",
                        "rx_code": "12345",
                        "rx_name": "Atorvastatin",
                        "total_claims": 50,
                        "total_drug_cost": 600.0,
                    }
                ]
            )
        ],
        args={"year": "2023"},
    )

    response = await get_provider_prescription(request, "1003000126", "HP_RX_CODE", "12345")
    payload = json.loads(response.body)
    assert payload["prescription_code"] == "12345"
    assert payload["prescription_name"] == "Atorvastatin"
    assert payload["total_prescriptions"] == 50
    assert payload["total_allowed_amount"] == 600.0


@pytest.mark.asyncio
async def test_list_prescription_providers_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "total_claims": 80,
                        "total_drug_cost": 900.0,
                        "total_benes": 75,
                    }
                ]
            ),
        ],
        args={"year": "2023", "limit": "10"},
    )

    response = await list_prescription_providers(request, "HP_RX_CODE", "12345")
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["items"][0]["total_prescriptions"] == 80
    assert payload["items"][0]["total_allowed_amount"] == 900.0


@pytest.mark.asyncio
async def test_get_prescription_benchmarks_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(
                rows=[
                    {
                        "from_system": "NDC",
                        "from_code": "0001",
                        "to_system": "HP_RX_CODE",
                        "to_code": "12345",
                        "match_type": "exact",
                        "confidence": 1.0,
                        "source": "test",
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "matched_rows": 10,
                        "provider_count": 5,
                        "total_claims": 300.0,
                        "total_drug_cost": 2500.0,
                        "avg_total_drug_cost": 500.0,
                        "min_total_drug_cost": 100.0,
                        "max_total_drug_cost": 1000.0,
                    }
                ]
            ),
            FakeResult(rows=[{"p20": 200.0, "p40": 400.0, "p60": 600.0, "p80": 800.0}]),
        ],
        args={"year": "2023"},
    )

    response = await get_prescription_benchmarks(request, "NDC", "0001")
    payload = json.loads(response.body)
    assert payload["query"]["input_code"]["code_system"] == "NDC"
    assert payload["benchmark"]["provider_count"] == 5
    assert payload["benchmark"]["total_prescriptions"] == 300.0
    assert payload["benchmark"]["total_allowed_amount"] == 2500.0


@pytest.mark.asyncio
async def test_autocomplete_procedures_dedupes_terms_across_systems():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "code_system": "CPT",
                        "code": "70553",
                        "display_name": "Magnetic resonance imaging",
                        "short_description": "MRI",
                        "effective_year": 2023,
                        "source": "cms_physician_provider_service",
                    },
                    {
                        "code_system": "HCPCS",
                        "code": "70553",
                        "display_name": "Magnetic resonance imaging",
                        "short_description": "MRI",
                        "effective_year": 2023,
                        "source": "cms_physician_provider_service",
                    },
                    {
                        "code_system": "HP_PROCEDURE_CODE",
                        "code": "123456",
                        "display_name": "Magnetic resonance imaging",
                        "short_description": "MRI",
                        "effective_year": 2023,
                        "source": "cms_physician_provider_service",
                    },
                ]
            ),
        ],
        args={"q": "magnetic", "limit": "10"},
    )

    response = await autocomplete_procedures(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["term"] == "Magnetic resonance imaging"
    assert set(item["code_systems"]) == {"CPT", "HCPCS", "HP_PROCEDURE_CODE"}
    assert "123456" in item["internal_codes"]


@pytest.mark.asyncio
async def test_autocomplete_prescriptions_returns_generic_and_brand():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "rx_code_system": "HP_RX_CODE",
                        "rx_code": "ABC123",
                        "rx_name": "Lisinopril",
                        "generic_name": "Lisinopril",
                        "brand_name": "Prinivil",
                        "total_claims": 500,
                        "total_drug_cost": 1800.0,
                        "total_benes": 420,
                    }
                ]
            ),
        ],
        args={"q": "lisin", "year": "2023"},
    )

    response = await autocomplete_prescriptions(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["generic_name"] == "Lisinopril"
    assert item["brand_name"] == "Prinivil"
    assert item["display_label"] == "Lisinopril / Prinivil"
    assert item["total_prescriptions"] == 500
    assert item["prescription_code_system"] == "HP_RX_CODE"
    assert item["prescription_code"] == "ABC123"
    assert item["preferred_prescription_code_system"] == "HP_RX_CODE"
    assert item["preferred_prescription_code"] == "ABC123"


@pytest.mark.asyncio
async def test_autocomplete_prescriptions_enriches_ndc_and_rxnorm_codes():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "rx_code_system": "HP_RX_CODE",
                        "rx_code": "ABC123",
                        "rx_name": "Lisinopril",
                        "generic_name": "Lisinopril",
                        "brand_name": "Prinivil",
                        "total_claims": 500,
                        "total_drug_cost": 1800.0,
                        "total_benes": 420,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "from_system": "HP_RX_CODE",
                        "from_code": "ABC123",
                        "to_system": "RXNORM",
                        "to_code": "29046",
                    },
                    {
                        "from_system": "HP_RX_CODE",
                        "from_code": "ABC123",
                        "to_system": "NDC",
                        "to_code": "00093015001",
                    },
                ]
            ),
        ],
        args={"q": "lisin", "year": "2023"},
    )

    response = await autocomplete_prescriptions(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["ndc_code"] == "00093015001"
    assert item["rxnorm_id"] == "29046"
    assert item["preferred_prescription_code_system"] == "NDC"
    assert item["preferred_prescription_code"] == "00093015001"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_with_q():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Diagnostic Radiology",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20814",
                        "total_claims": 100.0,
                        "total_drug_cost": 25000.0,
                        "total_benes": 80.0,
                        "matched_service_codes": 2,
                    }
                ]
            ),
        ],
        args={"q": "mri", "year": "2023", "state": "MD"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["query"]["q"] == "mri"
    assert payload["query"]["state"] == "MD"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_cost_index_requires_code():
    request = make_request(
        [],
        args={"q": "mri", "order_by": "cost_index", "zip5": "20814", "year": "2023"},
    )

    with pytest.raises(pricing_module.InvalidUsage, match="requires 'code'"):
        await list_providers_by_procedure(request)


@pytest.mark.asyncio
async def test_list_providers_by_procedure_cost_index_requires_location():
    request = make_request(
        [],
        args={"code": "123", "order_by": "cost_index", "state": "MD", "year": "2023"},
    )

    with pytest.raises(
        pricing_module.InvalidUsage,
        match="requires either 'zip5' or both 'state' and 'city'",
    ):
        await list_providers_by_procedure(request)


@pytest.mark.asyncio
async def test_list_providers_by_procedure_cost_index_with_code_and_zip5():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Diagnostic Radiology",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20814",
                        "total_services": 100.0,
                        "total_submitted_charges": 30000.0,
                        "total_allowed_amount": 25000.0,
                        "total_beneficiaries": 80.0,
                        "matched_service_codes": 2,
                        "cost_index": 250.0,
                    }
                ]
            ),
        ],
        args={"code": "123", "zip5": "20814", "order_by": "cost_index", "order": "asc", "year": "2023"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["items"][0]["cost_index"] == 250.0
    assert payload["items"][0]["avg_submitted_charge"] == 300.0
    assert payload["items"][0]["avg_allowed_amount"] == 250.0
    assert "charge_per_service_avg" not in payload["items"][0]
    assert "medicare_avg_submitted_charge_per_service" not in payload["items"][0]
    assert "medicare_avg_allowed_amount_per_service" not in payload["items"][0]
    assert "medicare_average_price_per_service" not in payload["items"][0]
    assert "average_price" not in payload["items"][0]
    assert payload["query"]["order_by"] == "cost_index"
    assert payload["query"]["code"] == "123"
    assert payload["query"]["zip5"] == "20814"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_cost_index_enriched_from_peer_stats():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Diagnostic Radiology",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20814",
                        "total_services": 100.0,
                        "total_submitted_charges": 30000.0,
                        "total_allowed_amount": 25000.0,
                        "total_beneficiaries": 80.0,
                        "matched_service_codes": 2,
                        "cost_index": 250.0,
                    }
                ]
            ),
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "procedure_code": 123,
                        "geography_scope": "zip5",
                        "geography_value": "20814",
                        "specialty_key": "diagnostic radiology",
                        "setting_key": "all",
                        "claim_count": 42.0,
                        "avg_submitted_charge": 415.0,
                        "total_submitted_charge": 17430.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "procedure_code": 123,
                        "year": 2023,
                        "geography_scope": "zip5",
                        "geography_value": "20814",
                        "specialty_key": "diagnostic radiology",
                        "setting_key": "all",
                        "provider_count": 250,
                        "min_claim_count": 11.0,
                        "max_claim_count": 900.0,
                        "p10": 150.0,
                        "p20": 250.0,
                        "p40": 350.0,
                        "p50": 425.0,
                        "p60": 500.0,
                        "p80": 700.0,
                        "p90": 900.0,
                    }
                ]
            ),
        ],
        args={"code": "123", "zip5": "20814", "order_by": "cost_index", "order": "asc", "year": "2023"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    item = payload["items"][0]

    assert item["cost_index"] == "$$$"
    assert item["estimated_cost_level"] == "$$$"
    assert item["avg_allowed_amount"] == 250.0


@pytest.mark.asyncio
async def test_list_providers_by_procedure_cost_index_with_legacy_aliases_opt_in():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Diagnostic Radiology",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20814",
                        "total_services": 100.0,
                        "total_submitted_charges": 30000.0,
                        "total_allowed_amount": 25000.0,
                        "total_beneficiaries": 80.0,
                        "matched_service_codes": 2,
                        "cost_index": 250.0,
                    }
                ]
            ),
        ],
        args={
            "code": "123",
            "zip5": "20814",
            "order_by": "cost_index",
            "order": "asc",
            "year": "2023",
            "include_legacy_fields": "1",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    item = payload["items"][0]
    assert item["avg_submitted_charge"] == 300.0
    assert item["avg_allowed_amount"] == 250.0
    assert item["charge_per_service_avg"] == 300.0
    assert item["medicare_avg_submitted_charge_per_service"] == 300.0
    assert item["medicare_avg_allowed_amount_per_service"] == 250.0
    assert item["medicare_average_price_per_service"] == 250.0
    assert item["average_price"] == 250.0


@pytest.mark.asyncio
async def test_list_providers_by_prescription_with_q():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_prescription"),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Internal Medicine",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20814",
                        "total_claims": 60.0,
                        "total_drug_cost": 3400.0,
                        "total_benes": 55.0,
                        "matched_prescription_codes": 3,
                    }
                ]
            ),
        ],
        args={"q": "atorvastatin", "year": "2023", "state": "MD"},
    )

    response = await list_providers_by_prescription(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["items"][0]["total_prescriptions"] == 60.0
    assert payload["query"]["q"] == "atorvastatin"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "procedure_code": 123,
                        "geography_scope": "national",
                        "geography_value": "US",
                        "specialty_key": "diagnostic radiology",
                        "setting_key": "all",
                        "claim_count": 42,
                        "avg_submitted_charge": 415.0,
                        "total_submitted_charge": 17430.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "procedure_code": 123,
                        "year": 2023,
                        "geography_scope": "national",
                        "geography_value": "US",
                        "specialty_key": "diagnostic radiology",
                        "setting_key": "all",
                        "provider_count": 250,
                        "min_claim_count": 11.0,
                        "max_claim_count": 900.0,
                        "p10": 150.0,
                        "p20": 250.0,
                        "p40": 350.0,
                        "p50": 425.0,
                        "p60": 500.0,
                        "p80": 700.0,
                        "p90": 900.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "service_description": "Magnetic resonance imaging, brain",
                        "reported_code": "70553",
                    }
                ]
            ),
        ],
        args={"year": "2023", "state": "MD"},
    )

    response = await get_provider_procedure_estimated_cost_level_internal(request, "1003000126", "123")
    payload = json.loads(response.body)
    assert payload["npi"] == 1003000126
    assert payload["provider"]["procedure_code"] == 123
    assert payload["peer_group"]["provider_count"] == 250
    assert payload["peer_group"]["typical_range"]["p10"] == 150.0
    assert payload["peer_group"]["typical_range"]["p90"] == 900.0
    assert payload["estimated_cost_level"] == "$$$"
    assert payload["confidence"] == "medium"
    assert payload["procedure"]["code_system"] == "HP_PROCEDURE_CODE"
    assert payload["procedure"]["code"] == "123"
    assert payload["procedure"]["name"] == "Magnetic resonance imaging, brain"
    assert payload["procedure"]["reported_code"] == "70553"
    assert payload["procedure"]["reported_code_system"] == "CPT"


@pytest.mark.asyncio
async def test_get_procedure_geo_benchmarks_success():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_procedure_geo_benchmark"),
            FakeResult(
                rows=[
                    {
                        "from_system": "CPT",
                        "from_code": "70553",
                        "to_system": "HP_PROCEDURE_CODE",
                        "to_code": "123",
                        "match_type": "exact",
                        "confidence": 1.0,
                        "source": "test",
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "rows": 1,
                        "total_services": 1000.0,
                        "avg_submitted_charge": 420.0,
                        "avg_payment_amount": 220.0,
                        "avg_standardized_amount": 300.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "rows": 1,
                        "total_services": 150.0,
                        "avg_submitted_charge": 460.0,
                        "avg_payment_amount": 240.0,
                        "avg_standardized_amount": 315.0,
                    }
                ]
            ),
        ],
        args={"year": "2023", "state": "MD"},
    )

    response = await get_procedure_geo_benchmarks(request, "CPT", "70553")
    payload = json.loads(response.body)
    assert payload["query"]["input_code"]["code_system"] == "CPT"
    assert payload["benchmarks"]["national"]["total_services"] == 1000.0
    assert payload["benchmarks"]["state"]["geography_value"] == "MD"
    assert payload["benchmarks"]["state"]["avg_submitted_charge"] == 460.0
