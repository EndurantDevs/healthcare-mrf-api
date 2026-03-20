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
pricing_statistics = pricing_module.pricing_statistics


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
async def test_list_pricing_providers_tier_relevance_ordering():
    request = make_request(
        [
            FakeResult(scalar=1),
            FakeResult(scalar="mrf.pricing_provider_quality_score"),
            FakeResult(scalar="national"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "total_services": 100,
                        "total_allowed_amount": 1234.5,
                        "total_beneficiaries": 80,
                    }
                ]
            ),
        ],
        args={"year": "2023", "limit": "10", "order_by": "tier_relevance", "order": "desc"},
    )

    response = await list_pricing_providers(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["query"]["order_by"] == "tier_relevance"
    assert payload["query"]["benchmark_mode"] == "national"


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
            FakeResult(scalar="mrf.pricing_provider_quality_score"),
            FakeResult(scalar="mrf.pricing_provider_quality_domain"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "model_version": "v2",
                        "benchmark_mode": "national",
                        "tier": "high",
                        "borderline_status": False,
                        "score_0_100": 72.4,
                        "estimated_cost_level": "$$",
                        "risk_ratio_point": 0.91,
                        "ci75_low": 0.86,
                        "ci75_high": 0.97,
                        "ci90_low": 0.82,
                        "ci90_high": 1.01,
                        "low_score_threshold_failed": False,
                        "low_confidence_threshold_failed": False,
                        "high_score_threshold_passed": True,
                        "high_confidence_threshold_passed": True,
                        "selected_geography": "national",
                        "selected_cohort_level": "L3",
                        "peer_count": 512,
                        "specialty_key": "__all__",
                        "taxonomy_code": None,
                        "procedure_bucket": "__all__",
                    },
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "model_version": "v2",
                        "benchmark_mode": "state",
                        "tier": "acceptable",
                        "borderline_status": False,
                        "score_0_100": 55.0,
                        "estimated_cost_level": "$$$",
                        "risk_ratio_point": 0.99,
                        "ci75_low": 0.90,
                        "ci75_high": 1.06,
                        "ci90_low": 0.84,
                        "ci90_high": 1.10,
                        "low_score_threshold_failed": False,
                        "low_confidence_threshold_failed": False,
                        "high_score_threshold_passed": False,
                        "high_confidence_threshold_passed": False,
                    },
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "model_version": "v2",
                        "benchmark_mode": "zip",
                        "tier": "low",
                        "borderline_status": True,
                        "score_0_100": 30.0,
                        "estimated_cost_level": "$$$$",
                        "risk_ratio_point": 1.15,
                        "ci75_low": 1.05,
                        "ci75_high": 1.22,
                        "ci90_low": 1.02,
                        "ci90_high": 1.25,
                        "low_score_threshold_failed": True,
                        "low_confidence_threshold_failed": True,
                        "high_score_threshold_passed": False,
                        "high_confidence_threshold_passed": False,
                    },
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "benchmark_mode": "national",
                        "domain": "appropriateness",
                        "risk_ratio": 0.93,
                        "score_0_100": 69.5,
                        "evidence_n": 145.0,
                        "ci75_low": 0.88,
                        "ci75_high": 0.99,
                        "ci90_low": 0.84,
                        "ci90_high": 1.04,
                    },
                    {
                        "benchmark_mode": "national",
                        "domain": "effectiveness",
                        "risk_ratio": 0.89,
                        "score_0_100": 75.0,
                        "evidence_n": 132.0,
                        "ci75_low": 0.83,
                        "ci75_high": 0.95,
                        "ci90_low": 0.79,
                        "ci90_high": 1.0,
                    },
                    {
                        "benchmark_mode": "national",
                        "domain": "cost",
                        "risk_ratio": 0.95,
                        "score_0_100": 66.7,
                        "evidence_n": 201.0,
                        "ci75_low": 0.9,
                        "ci75_high": 1.0,
                        "ci90_low": 0.86,
                        "ci90_high": 1.05,
                    },
                ]
            ),
        ],
        args={"year": "2023", "benchmark_mode": "national"},
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["npi"] == 1003000126
    assert payload["model_version"] == "v2"
    assert payload["benchmark_mode"] == "national"
    assert payload["tier"] == "high"
    assert payload["borderline_status"] is False
    assert payload["score_0_100"] == 72.4
    assert payload["estimated_cost_level"] == "$$"
    assert payload["overall"]["risk_ratio_point"] == 0.91
    assert payload["overall"]["ci_75"]["low"] == 0.86
    assert payload["domains"]["appropriateness"]["risk_ratio_point"] == 0.93
    assert payload["domains"]["effectiveness"]["score_0_100"] == 75.0
    assert payload["domains"]["cost"]["evidence_n"] == 201.0
    assert payload["curation_checks"]["high_score_threshold_passed"] is True
    assert payload["cohort_context"]["computed_live"] is False
    assert payload["cohort_context"]["selected_geography"] == "national"
    assert payload["cohort_context"]["selected_cohort_level"] == "L3"
    assert payload["scores_by_benchmark_mode"]["zip"]["tier"] == "low"
    assert payload["scores_by_benchmark_mode"]["state"]["tier"] == "acceptable"
    assert payload["scores_by_benchmark_mode"]["national"]["tier"] == "high"
    assert payload["scores_by_benchmark_mode"]["national"]["cohort_context"]["computed_live"] is False
    assert payload["available_benchmark_modes"] == ["zip", "state", "national"]


@pytest.mark.asyncio
async def test_get_pricing_provider_score_defaults_to_most_local_mode():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_quality_score"),
            FakeResult(scalar="mrf.pricing_provider_quality_domain"),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "model_version": "v2",
                        "benchmark_mode": "national",
                        "tier": "acceptable",
                        "borderline_status": False,
                        "score_0_100": 50.0,
                        "estimated_cost_level": "$$$",
                        "risk_ratio_point": 1.0,
                        "ci75_low": 0.9,
                        "ci75_high": 1.1,
                        "ci90_low": 0.85,
                        "ci90_high": 1.15,
                        "low_score_threshold_failed": False,
                        "low_confidence_threshold_failed": False,
                        "high_score_threshold_passed": False,
                        "high_confidence_threshold_passed": False,
                    },
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "model_version": "v2",
                        "benchmark_mode": "zip",
                        "tier": "high",
                        "borderline_status": False,
                        "score_0_100": 70.0,
                        "estimated_cost_level": "$$",
                        "risk_ratio_point": 0.86,
                        "ci75_low": 0.75,
                        "ci75_high": 0.95,
                        "ci90_low": 0.68,
                        "ci90_high": 0.99,
                        "low_score_threshold_failed": False,
                        "low_confidence_threshold_failed": False,
                        "high_score_threshold_passed": True,
                        "high_confidence_threshold_passed": True,
                    },
                ]
            ),
            FakeResult(rows=[]),
        ],
        args={"year": "2023"},
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["benchmark_mode"] == "zip"
    assert payload["tier"] == "high"
    assert payload.get("cohort_context") is None
    assert payload["scores_by_benchmark_mode"]["state"] is None


@pytest.mark.asyncio
async def test_get_pricing_provider_score_live_override_path():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "state": "MD",
                        "zip5": "20850",
                        "total_services": 120.0,
                        "total_beneficiaries": 60.0,
                        "total_allowed_amount": 24000.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "quality_score": 80.0,
                        "cost_score": 70.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "total_rx_claims": 150.0,
                        "total_rx_beneficiaries": 75.0,
                    }
                ]
            ),
            FakeResult(scalar=0.4),
            FakeResult(
                rows=[
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "zip",
                            "geography_scope": "zip",
                            "geography_value": "20850",
                            "cohort_level": "L0",
                            "peer_count": 44,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.0408163265306123,
                            "target_rx_appropriateness": 2.0,
                            "target_effectiveness": 80.0,
                            "target_qpp_cost": 70.0,
                            "target_cost": 24489.795918367346,
                        }
                    }
                ]
            ),
        ],
        args={
            "year": "2023",
            "benchmark_mode": "zip",
            "specialty": "cardiology",
            "taxonomy_code": "207RC0000X",
            "procedure_codes": "100,999",
        },
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["benchmark_mode"] == "zip"
    assert payload["tier"] == "acceptable"
    assert payload["score_0_100"] == 50.0
    assert payload["available_benchmark_modes"] == ["zip"]
    assert payload["scores_by_benchmark_mode"]["zip"] is not None
    assert payload["scores_by_benchmark_mode"]["state"] is None
    assert payload["scores_by_benchmark_mode"]["national"] is None
    assert payload["cohort_context"]["computed_live"] is True
    assert payload["cohort_context"]["selected_geography"] == "zip:20850"
    assert payload["cohort_context"]["selected_cohort_level"] == "L0"
    assert payload["cohort_context"]["peer_count"] == 44
    assert payload["cohort_context"]["specialty_key"] == "cardiology"
    assert payload["cohort_context"]["taxonomy_code"] == "207RC0000X"
    assert payload["cohort_context"]["procedure_match_threshold"] == 0.3


@pytest.mark.asyncio
async def test_get_pricing_provider_score_live_override_all_modes_when_benchmark_not_forced():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "state": "MD",
                        "zip5": "20850",
                        "provider_type": "Cardiology",
                        "total_services": 100.0,
                        "total_beneficiaries": 50.0,
                        "total_allowed_amount": 24000.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "quality_score": 80.0,
                        "cost_score": 70.0,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "total_rx_claims": 150.0,
                        "total_rx_beneficiaries": 75.0,
                    }
                ]
            ),
            FakeResult(scalar=0.4),
            FakeResult(
                rows=[
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "zip",
                            "geography_scope": "zip",
                            "geography_value": "20850",
                            "cohort_level": "L0",
                            "peer_count": 44,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.0408163265306123,
                            "target_rx_appropriateness": 2.0,
                            "target_effectiveness": 80.0,
                            "target_qpp_cost": 70.0,
                            "target_cost": 24489.795918367346,
                        }
                    },
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "state",
                            "geography_scope": "state",
                            "geography_value": "MD",
                            "cohort_level": "L1",
                            "peer_count": 120,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.1,
                            "target_rx_appropriateness": 2.2,
                            "target_effectiveness": 81.0,
                            "target_qpp_cost": 72.0,
                            "target_cost": 25000.0,
                        }
                    },
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "national",
                            "geography_scope": "national",
                            "geography_value": "US",
                            "cohort_level": "L2",
                            "peer_count": 500,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.2,
                            "target_rx_appropriateness": 2.1,
                            "target_effectiveness": 82.0,
                            "target_qpp_cost": 71.0,
                            "target_cost": 25500.0,
                        }
                    },
                ]
            ),
        ],
        args={
            "year": "2023",
            "specialty": "cardiology",
            "taxonomy_code": "207RC0000X",
            "procedure_codes": "100,999",
        },
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["benchmark_mode"] == "zip"
    assert payload["available_benchmark_modes"] == ["zip", "state", "national"]
    assert payload["scores_by_benchmark_mode"]["zip"] is not None
    assert payload["scores_by_benchmark_mode"]["state"] is not None
    assert payload["scores_by_benchmark_mode"]["national"] is not None
    assert payload["cohort_context"]["computed_live"] is True


@pytest.mark.asyncio
async def test_get_pricing_provider_score_variants_scope_provider_returns_variant_cards():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "year": 2023,
                        "state": "MD",
                        "zip5": "20850",
                        "provider_type": "Cardiology",
                        "total_services": 100.0,
                        "total_beneficiaries": 50.0,
                        "total_allowed_amount": 24000.0,
                    }
                ]
            ),
            FakeResult(rows=[{"quality_score": 80.0, "cost_score": 70.0}]),
            FakeResult(rows=[{"total_rx_claims": 150.0, "total_rx_beneficiaries": 75.0}]),
            FakeResult(scalar=0.4),
            FakeResult(
                rows=[
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "zip",
                            "geography_scope": "zip",
                            "geography_value": "20850",
                            "cohort_level": "L0",
                            "peer_count": 44,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.0,
                            "target_rx_appropriateness": 2.0,
                            "target_effectiveness": 80.0,
                            "target_qpp_cost": 70.0,
                            "target_cost": 24500.0,
                        }
                    },
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "zip",
                            "geography_scope": "state",
                            "geography_value": "MD",
                            "cohort_level": "L1",
                            "peer_count": 90,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.1,
                            "target_rx_appropriateness": 2.1,
                            "target_effectiveness": 81.0,
                            "target_qpp_cost": 71.0,
                            "target_cost": 25000.0,
                        }
                    },
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "state",
                            "geography_scope": "state",
                            "geography_value": "MD",
                            "cohort_level": "L1",
                            "peer_count": 130,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.1,
                            "target_rx_appropriateness": 2.2,
                            "target_effectiveness": 81.0,
                            "target_qpp_cost": 72.0,
                            "target_cost": 25000.0,
                        }
                    },
                    {
                        "row_data": {
                            "year": 2023,
                            "benchmark_mode": "national",
                            "geography_scope": "national",
                            "geography_value": "US",
                            "cohort_level": "L2",
                            "peer_count": 500,
                            "specialty_key": "cardiology",
                            "taxonomy_code": "207RC0000X",
                            "procedure_bucket": "100,200",
                            "target_appropriateness": 2.2,
                            "target_rx_appropriateness": 2.1,
                            "target_effectiveness": 82.0,
                            "target_qpp_cost": 71.0,
                            "target_cost": 25500.0,
                        }
                    },
                ]
            ),
        ],
        args={
            "year": "2023",
            "variants_scope": "provider",
        },
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["variants_scope"] == "provider"
    assert payload["benchmark_mode"] == "zip"
    assert payload["available_benchmark_modes"] == ["zip", "state", "national"]
    assert len(payload["variants_by_benchmark_mode"]["zip"]) == 2
    assert len(payload["variants_by_benchmark_mode"]["state"]) == 1
    assert len(payload["variants_by_benchmark_mode"]["national"]) == 1
    assert payload["variants_by_benchmark_mode"]["zip"][0]["benchmark_mode"] == "zip"
    assert payload["variants_by_benchmark_mode"]["zip"][0]["cohort_context"]["selected_cohort_level"] == "L0"
    assert payload["variants_by_benchmark_mode"]["zip"][0]["cohort_context"]["computed_live"] is True


@pytest.mark.asyncio
async def test_get_pricing_provider_score_invalid_variants_scope():
    request = make_request([], args={"variants_scope": "matrix"})

    with pytest.raises(Exception) as exc:
        await get_pricing_provider_score(request, "1003000126")

    assert "variants_scope" in str(exc.value)


@pytest.mark.asyncio
async def test_get_pricing_provider_score_rejects_legacy_variants_param():
    request = make_request([], args={"variants": "all"})

    with pytest.raises(Exception) as exc:
        await get_pricing_provider_score(request, "1003000126")

    assert "no longer supported" in str(exc.value)


@pytest.mark.asyncio
async def test_get_pricing_provider_score_invalid_procedure_code_system():
    request = make_request([], args={"procedure_code_system": "RXNORM"})

    with pytest.raises(Exception) as exc:
        await get_pricing_provider_score(request, "1003000126")

    assert "procedure_code_system" in str(exc.value)


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
async def test_list_providers_by_procedure_zip_radius_expands_candidates():
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "zip5": "20814",
                        "state": "MD",
                        "city_lower": "bethesda",
                        "latitude": 39.0,
                        "longitude": -77.1,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {"zip5": "20814", "state": "MD", "city_lower": "bethesda", "distance_miles": 0.0},
                    {"zip5": "20816", "state": "MD", "city_lower": "bethesda", "distance_miles": 6.5},
                ]
            ),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000126,
                        "provider_name": "Enkeshafi, Ardalan",
                        "provider_type": "Diagnostic Radiology",
                        "city": "Bethesda",
                        "state": "MD",
                        "zip5": "20816",
                        "total_services": 100.0,
                        "total_submitted_charges": 30000.0,
                        "total_allowed_amount": 25000.0,
                        "total_beneficiaries": 80.0,
                        "matched_service_codes": 2,
                    }
                ]
            ),
        ],
        args={"q": "mri", "zip5": "20814", "zip_radius_miles": "30", "year": "2023"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    item = payload["items"][0]
    assert item["zip5"] == "20816"
    assert item["distance_miles"] == 6.5
    assert item["distance_bucket"] == "within_10mi"
    assert item["anchor_zip5"] == "20814"
    assert payload["query"]["zip_radius_miles"] == 30.0
    assert payload["query"]["zip_candidate_count"] == 2


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
    assert payload["peer_group"]["cohort_type"] == "national"
    assert payload["peer_group"]["specialty_scope"] == "specialty_specific"
    assert payload["estimated_cost_level"] == "$$$"
    assert payload["confidence"] == "medium"
    assert payload["procedure"]["code_system"] == "HP_PROCEDURE_CODE"
    assert payload["procedure"]["code"] == "123"
    assert payload["procedure"]["name"] == "Magnetic resonance imaging, brain"
    assert payload["procedure"]["reported_code"] == "70553"
    assert payload["procedure"]["reported_code_system"] == "CPT"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_zip_ring_fallback():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(
                rows=[
                    {
                        "zip5": "20814",
                        "state": "MD",
                        "city_lower": "bethesda",
                        "latitude": 39.0,
                        "longitude": -77.1,
                    }
                ]
            ),
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
                    {"zip5": "20814", "state": "MD", "city_lower": "bethesda", "distance_miles": 0.0},
                    {"zip5": "20816", "state": "MD", "city_lower": "bethesda", "distance_miles": 6.5},
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "procedure_code": 123,
                        "year": 2023,
                        "geography_scope": "zip5",
                        "geography_value": "20816",
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
        args={"year": "2023", "zip5": "20814", "zip_radius_miles": "30"},
    )

    response = await get_provider_procedure_estimated_cost_level_internal(request, "1003000126", "123")
    payload = json.loads(response.body)
    assert payload["peer_group"]["geography_scope"] == "zip5"
    assert payload["peer_group"]["geography_value"] == "20816"
    assert payload["peer_group"]["cohort_type"] == "zip"
    assert payload["peer_group"]["specialty_scope"] == "specialty_specific"
    assert payload["peer_group"]["distance_miles"] == 6.5
    assert payload["peer_group"]["distance_bucket"] == "within_10mi"
    assert payload["query"]["zip_radius_miles"] == 30.0


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_near_dynamic():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(
                rows=[
                    {
                        "zip5": "20814",
                        "state": "MD",
                        "city_lower": "bethesda",
                        "latitude": 39.0,
                        "longitude": -77.1,
                    }
                ]
            ),
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
                    {"zip5": "20814", "state": "MD", "city_lower": "bethesda", "distance_miles": 0.0},
                    {"zip5": "20816", "state": "MD", "city_lower": "bethesda", "distance_miles": 6.5},
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "npi": 1003000101,
                        "claim_count": 15.0,
                        "avg_submitted_charge": 200.0,
                        "geography_value": "20814",
                    },
                    {
                        "npi": 1003000102,
                        "claim_count": 16.0,
                        "avg_submitted_charge": 220.0,
                        "geography_value": "20814",
                    },
                    {
                        "npi": 1003000103,
                        "claim_count": 17.0,
                        "avg_submitted_charge": 240.0,
                        "geography_value": "20814",
                    },
                    {
                        "npi": 1003000104,
                        "claim_count": 18.0,
                        "avg_submitted_charge": 260.0,
                        "geography_value": "20814",
                    },
                    {
                        "npi": 1003000105,
                        "claim_count": 19.0,
                        "avg_submitted_charge": 280.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000106,
                        "claim_count": 20.0,
                        "avg_submitted_charge": 300.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000107,
                        "claim_count": 21.0,
                        "avg_submitted_charge": 320.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000108,
                        "claim_count": 22.0,
                        "avg_submitted_charge": 340.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000109,
                        "claim_count": 23.0,
                        "avg_submitted_charge": 360.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000110,
                        "claim_count": 24.0,
                        "avg_submitted_charge": 380.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000111,
                        "claim_count": 25.0,
                        "avg_submitted_charge": 400.0,
                        "geography_value": "20816",
                    },
                    {
                        "npi": 1003000112,
                        "claim_count": 26.0,
                        "avg_submitted_charge": 420.0,
                        "geography_value": "20816",
                    },
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
        args={
            "year": "2023",
            "zip5": "20814",
            "zip_radius_miles": "30",
            "cohort_strategy": "near_dynamic",
        },
    )

    response = await get_provider_procedure_estimated_cost_level_internal(request, "1003000126", "123")
    payload = json.loads(response.body)
    assert payload["peer_group"]["geography_scope"] == "zip_radius"
    assert payload["peer_group"]["geography_value"] == "20814|30mi"
    assert payload["peer_group"]["provider_count"] == 12
    assert payload["peer_group"]["cohort_type"] == "zip_radius"
    assert payload["peer_group"]["specialty_scope"] == "specialty_specific"
    assert payload["peer_group"]["cohort_strategy_used"] == "near_dynamic"
    assert payload["query"]["cohort_strategy"] == "near_dynamic"
    assert payload["query"]["cohort_strategy_used"] == "near_dynamic"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_near_dynamic_fallbacks_to_precomputed():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
            FakeResult(
                rows=[
                    {
                        "zip5": "20814",
                        "state": "MD",
                        "city_lower": "bethesda",
                        "latitude": 39.0,
                        "longitude": -77.1,
                    }
                ]
            ),
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
                    {"zip5": "20814", "state": "MD", "city_lower": "bethesda", "distance_miles": 0.0},
                    {"zip5": "20816", "state": "MD", "city_lower": "bethesda", "distance_miles": 6.5},
                ]
            ),
            FakeResult(rows=[]),
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "procedure_code": 123,
                        "year": 2023,
                        "geography_scope": "zip5",
                        "geography_value": "20816",
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
        args={
            "year": "2023",
            "zip5": "20814",
            "zip_radius_miles": "30",
            "cohort_strategy": "near_dynamic",
        },
    )

    response = await get_provider_procedure_estimated_cost_level_internal(request, "1003000126", "123")
    payload = json.loads(response.body)
    assert payload["peer_group"]["geography_scope"] == "zip5"
    assert payload["peer_group"]["geography_value"] == "20816"
    assert payload["peer_group"]["cohort_type"] == "zip"
    assert payload["peer_group"]["specialty_scope"] == "specialty_specific"
    assert payload["peer_group"]["cohort_strategy_used"] == "precomputed"
    assert payload["query"]["cohort_strategy"] == "near_dynamic"
    assert payload["query"]["cohort_strategy_used"] == "precomputed"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_invalid_cohort_strategy():
    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_procedure_cost_profile"),
            FakeResult(scalar="mrf.pricing_procedure_peer_stats"),
        ],
        args={"cohort_strategy": "matrix"},
    )

    with pytest.raises(Exception) as exc:
        await get_provider_procedure_estimated_cost_level_internal(request, "1003000126", "123")

    assert "cohort_strategy" in str(exc.value)


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


@pytest.mark.asyncio
async def test_pricing_statistics_success():
    request = make_request([
        FakeResult(scalar=321000),
        FakeResult(scalar=845000),
        FakeResult(scalar=6123),
        FakeResult(scalar=27890),
    ])

    response = await pricing_statistics(request)
    payload = json.loads(response.body)

    assert payload == {
        "medicare_individual_providers": 321000,
        "providers_with_procedure_history": 845000,
        "procedure_codes_tracked": 6123,
        "procedure_zip_codes": 27890,
    }
