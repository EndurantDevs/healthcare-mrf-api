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
group_plan_providers = pricing_module.group_plan_providers
autocomplete_prescriptions = pricing_module.autocomplete_prescriptions
autocomplete_procedures = pricing_module.autocomplete_procedures
list_pricing_providers = pricing_module.list_pricing_providers
list_provider_specialties = pricing_module.list_provider_specialties
list_providers_by_prescription = pricing_module.list_providers_by_prescription
list_providers_by_procedure = pricing_module.list_providers_by_procedure
list_procedure_providers = pricing_module.list_procedure_providers
list_prescription_providers = pricing_module.list_prescription_providers
list_provider_procedure_locations = pricing_module.list_provider_procedure_locations
list_provider_procedures = pricing_module.list_provider_procedures
list_provider_prescriptions = pricing_module.list_provider_prescriptions
resolve_procedure_taxonomy = pricing_module.resolve_procedure_taxonomy
pricing_statistics = pricing_module.pricing_statistics


class FakeResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = rows or []
        self._scalar = scalar

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return self._rows

    def mappings(self):
        return self

    def all(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results=None):
        self._results = list(results or [])
        self.executions = []

    async def execute(self, *_args, **_kwargs):
        self.executions.append((_args, _kwargs))
        if self._results:
            return self._results.pop(0)
        return FakeResult([], 0)


def make_request(results, args=None):
    session = FakeSession(results)
    return types.SimpleNamespace(
        args=args or {},
        ctx=types.SimpleNamespace(sa_session=session),
    )


SMILE_ALLOWED_TABLE_NAMES = (
    "mrf.ptg_allowed_item_smile_import",
    "mrf.ptg_allowed_payment_smile_import",
    "mrf.ptg_allowed_provider_payment_smile_import",
)
SMILE_ALLOWED_ROW_BY_FIELD = {
    "npi": 1427166008,
    "plan_id": "911643507",
    "billing_code_type": "CPT",
    "billing_code": "99214",
    "allowed_amount": 133.0,
    "billed_charge": 155.0,
    "tin_type": "ein",
    "tin_value": "123456789",
}
SMILE_IMPORT_ROW_BY_FIELD = {
    "source_file_import_id": "smile_import",
    "source_key": "ptg_smile",
    "snapshot_id": "ptg2:202607:smile",
    "metrics": {"plan_count": 1},
}


async def fake_plan_snapshot_pairs(_session, _plan_fields):
    return [("ptg_test", "ptg2:test")]


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
async def test_group_plan_providers_filters_to_ten_digit_npis(monkeypatch):
    async def fake_current_source_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_current_source_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_ids_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1000000000), types.SimpleNamespace(npi=1234567890)]),
            FakeResult(scalar=2),
        ],
        args={"plan_id": "465722012", "market_type": "group", "count": "true", "limit": "100"},
    )

    response = await group_plan_providers(request)
    payload = json.loads(response.body)

    assert [item["npi"] for item in payload["providers"]["items"]] == [1000000000, 1234567890]
    assert payload["providers"]["total_distinct"] == 2
    first_params = request.ctx.sa_session.executions[0][0][1]
    count_params = request.ctx.sa_session.executions[1][0][1]
    assert first_params["npi_min"] == 1000000000
    assert first_params["npi_max"] == 9999999999
    assert count_params["npi_min"] == 1000000000
    assert count_params["npi_max"] == 9999999999
    assert "gm.npi BETWEEN :npi_min AND :npi_max" in str(request.ctx.sa_session.executions[0][0][0])


@pytest.mark.asyncio
async def test_group_plan_providers_filters_by_primary_taxonomy(monkeypatch):
    async def fake_current_source_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_current_source_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_ids_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1234567890)])],
        args={
            "plan_id": "465722012",
            "market_type": "group",
            "specialty": "Family Medicine",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    payload = json.loads(response.body)

    assert [item["npi"] for item in payload["providers"]["items"]] == [1234567890]
    assert payload["taxonomy_filter"]["specialty"] == "Family Medicine"
    assert payload["taxonomy_filter"]["taxonomy_code_set"] == ["207Q00000X", "208D00000X"]
    sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "mrf.npi_taxonomy group_provider_specialty_nt" in sql
    assert "group_provider_specialty_nt.npi = gm.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert "display_name" not in sql
    assert params["group_provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["group_provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_group_plan_providers_classification_internal_medicine_uses_base_taxonomy(monkeypatch):
    async def fake_current_source_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_current_source_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_ids_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1003000530)])],
        args={
            "plan_id": "465722012",
            "market_type": "group",
            "classification": "Internal Medicine",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    payload = json.loads(response.body)

    assert payload["taxonomy_filter"]["classification"] == "Internal Medicine"
    assert payload["taxonomy_filter"]["taxonomy_code_set"] == ["207R00000X"]
    sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "nucc_taxonomy" not in sql
    assert params["group_provider_specialty_taxonomy_code_0"] == "207R00000X"


@pytest.mark.asyncio
async def test_group_plan_providers_applies_location_filter_and_returns_addresses(monkeypatch):
    """Location filters should constrain group-plan provider NPIs and return addresses."""

    async def fake_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_ids_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(scalar=1),
            FakeResult(
                rows=[
                    {
                        "npi": 1073913877,
                        "type": "primary",
                        "first_line": "1400 W GREENLEAF AVE STE 101",
                        "second_line": None,
                        "city_name": "CHICAGO",
                        "state_name": "IL",
                        "postal_code": "606262805",
                        "phone_number": "3125550100",
                    }
                ]
            ),
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "city": "Chicago",
            "state": "IL",
            "zip5": "60626",
            "zip_radius_miles": "0",
            "count": "true",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    payload = json.loads(response.body)

    assert payload["location_filter"] == {
        "requested": True,
        "city": "chicago",
        "state": "IL",
        "zip5": "60626",
        "zip_radius_miles": 0.0,
        "zips_considered": 1,
        "include_mail_addresses": False,
        "address_source": "npi",
        "address_types": ["primary", "secondary"],
        "count_requested": True,
        "count_exact": True,
    }
    assert payload["providers"]["total_distinct"] == 1
    assert payload["providers"]["items"][0]["address"] == {
        "type": "primary",
        "first_line": "1400 W GREENLEAF AVE STE 101",
        "second_line": None,
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "606262805",
        "zip5": "60626",
        "phone_number": "3125550100",
    }
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    count_sql = str(request.ctx.sa_session.executions[1][0][0])
    address_sql = str(request.ctx.sa_session.executions[2][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "mrf.npi_address addr" in provider_sql
    assert "addr.type IN ('primary', 'secondary')" in provider_sql
    assert "LEFT(COALESCE(addr.postal_code, ''), 5) = ANY(:location_zips)" in provider_sql
    assert "mrf.npi_address addr" in count_sql
    assert "type IN ('primary', 'secondary')" in address_sql
    assert params["location_city"] == "chicago"
    assert params["location_state"] == "IL"
    assert params["location_zips"] == ["60626"]


@pytest.mark.asyncio
async def test_group_plan_providers_widens_zip_filter_to_radius_by_default(monkeypatch):
    """zip5 should mean 'this ZIP plus zip_radius_miles' (default 10), not strict equality."""

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    async def fake_zip_radius_rows(_session, *, zip5, radius_miles, state_hint=None, **_kwargs):
        assert zip5 == "60601"
        assert radius_miles == 10.0
        return [
            {"zip5": "60601", "distance_miles": 0.0, "is_anchor": True},
            {"zip5": "60602", "distance_miles": 0.6},
            {"zip5": "60611", "distance_miles": 1.2},
        ]

    monkeypatch.setattr(pricing_module, "current_source_snapshot_ids_for_plan", fake_plan_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_zip_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "zip5": "60601",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    response_payload = json.loads(response.body)

    assert response_payload["location_filter"]["zip_radius_miles"] == 10.0
    assert response_payload["location_filter"]["zips_considered"] == 3
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "= ANY(:location_zips)" in provider_sql
    assert params["location_zips"] == ["60601", "60602", "60611"]


@pytest.mark.asyncio
async def test_group_plan_providers_drives_from_local_specialty_candidates(monkeypatch):
    """specialty + zip-set queries must drive from a materialized candidate CTE,
    not walk the member table probing per-NPI EXISTS."""

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    async def fake_zip_radius_rows(_session, *, zip5, radius_miles, state_hint=None, **_kwargs):
        return [{"zip5": "60601"}, {"zip5": "60602"}]

    monkeypatch.setattr(pricing_module, "current_source_snapshot_ids_for_plan", fake_plan_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_zip_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "specialty": "orthopedic surgeon",
            "zip5": "60601",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    response_payload = json.loads(response.body)

    assert response_payload["ok"] is True
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "WITH local_specialty_npis AS MATERIALIZED" in provider_sql
    assert "JOIN local_specialty_npis lsn ON lsn.npi = gm.npi" in provider_sql
    assert "= ANY(:location_zips)" in provider_sql
    # index-compatible taxonomy predicate: raw column, uppercased params
    assert "UPPER(COALESCE(cand_provider_specialty_nt.healthcare_provider_taxonomy_code" not in provider_sql
    assert "cand_provider_specialty_nt.healthcare_provider_taxonomy_code IN (" in provider_sql
    assert params["location_zips"] == ["60601", "60602"]


@pytest.mark.asyncio
async def test_group_plan_providers_unions_all_published_network_snapshots(monkeypatch):
    """A multi-network plan must enumerate every published snapshot, not just the first."""

    async def fake_network_snapshot_pairs(_session, _plan_fields):
        return [("ptg_ndc", "ptg2:test:ndc"), ("ptg_c2", "ptg2:test:c2")]

    member_table_by_snapshot = {
        "ptg2:test:ndc": "mrf.ptg2_provider_group_member_ndc",
        "ptg2:test:c2": "mrf.ptg2_provider_group_member_c2",
    }

    async def fake_snapshot_serving_tables(_session, snapshot_id):
        return types.SimpleNamespace(provider_group_member_table=member_table_by_snapshot[snapshot_id])

    monkeypatch.setattr(pricing_module, "current_source_snapshot_ids_for_plan", fake_network_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1073913877)])],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    response_payload = json.loads(response.body)

    assert response_payload["snapshot_id"] == "ptg2:test:ndc"
    assert response_payload["snapshots"] == [
        {"source_key": "ptg_ndc", "snapshot_id": "ptg2:test:ndc", "enumerated": True},
        {"source_key": "ptg_c2", "snapshot_id": "ptg2:test:c2", "enumerated": True},
    ]
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    assert "SELECT npi FROM mrf.ptg2_provider_group_member_ndc UNION ALL SELECT npi FROM mrf.ptg2_provider_group_member_c2" in provider_sql
    assert "(SELECT npi FROM" in provider_sql


@pytest.mark.asyncio
async def test_group_plan_providers_splits_multi_network_postal_scans(monkeypatch):
    """ZIP-filtered multi-network lookups should keep per-network member scans indexable."""

    async def fake_network_snapshot_pairs(_session, _plan_fields):
        return [("ptg_ndc", "ptg2:test:ndc"), ("ptg_c2", "ptg2:test:c2")]

    member_table_by_snapshot = {
        "ptg2:test:ndc": "mrf.ptg2_provider_group_member_ndc",
        "ptg2:test:c2": "mrf.ptg2_provider_group_member_c2",
    }

    async def fake_snapshot_serving_tables(_session, snapshot_id):
        return types.SimpleNamespace(provider_group_member_table=member_table_by_snapshot[snapshot_id])

    async def fake_postal_radius_rows(_session, **keyword_args):
        return [{"zip5": keyword_args["zip5"]}, {"zip5": "60602"}]

    monkeypatch.setattr(pricing_module, "current_source_snapshot_ids_for_plan", fake_network_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_postal_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877), types.SimpleNamespace(npi=1234567890)]),
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877), types.SimpleNamespace(npi=1003000126)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "zip5": "60601",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    response_payload = json.loads(response.body)

    assert [provider_item["npi"] for provider_item in response_payload["providers"]["items"]] == [
        1003000126,
        1073913877,
        1234567890,
    ]
    first_member_query_sql = str(request.ctx.sa_session.executions[0][0][0])
    second_member_query_sql = str(request.ctx.sa_session.executions[1][0][0])
    first_member_query_params = request.ctx.sa_session.executions[0][0][1]
    assert "FROM mrf.ptg2_provider_group_member_ndc gm" in first_member_query_sql
    assert "FROM mrf.ptg2_provider_group_member_c2 gm" in second_member_query_sql
    assert "UNION ALL" not in first_member_query_sql
    assert "UNION ALL" not in second_member_query_sql
    assert "EXISTS (" in first_member_query_sql
    assert first_member_query_params["limit"] == 10
    assert first_member_query_params["location_zips"] == ["60601", "60602"]


@pytest.mark.asyncio
async def test_group_plan_providers_uses_unified_service_locations_when_configured(monkeypatch):
    """Unified address serving should use service-location rows for group-plan filters."""

    async def fake_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return types.SimpleNamespace(provider_group_member_table="mrf.ptg2_provider_group_member_test")

    async def fake_group_plan_provider_address_source(_session):
        return "mrf.entity_address_unified", True, True, True

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_ids_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(
        pricing_module,
        "_group_plan_provider_address_source",
        fake_group_plan_provider_address_source,
    )
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1003362153)]),
            FakeResult(
                rows=[
                    {
                        "npi": 1003362153,
                        "type": "practice",
                        "first_line": "2335 S MICHIGAN AVE",
                        "second_line": None,
                        "city_name": "CHICAGO",
                        "state_name": "IL",
                        "postal_code": "606160000",
                        "zip5": "60616",
                        "phone_number": "3125550100",
                        "address_precision": "street",
                        "plan_coverage_match": False,
                    }
                ]
            ),
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "classification": "Family Medicine",
            "city": "Chicago",
            "state": "IL",
            "count": "true",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    payload = json.loads(response.body)

    assert payload["location_filter"]["address_source"] == "unified"
    assert payload["location_filter"]["address_types"] == ["practice", "site", "primary", "secondary"]
    assert payload["location_filter"]["count_requested"] is True
    assert payload["location_filter"]["count_exact"] is False
    assert payload["providers"]["total_distinct"] is None
    assert payload["providers"]["items"][0]["address"] == {
        "type": "practice",
        "first_line": "2335 S MICHIGAN AVE",
        "second_line": None,
        "city": "CHICAGO",
        "state": "IL",
        "postal_code": "606160000",
        "zip5": "60616",
        "phone_number": "3125550100",
        "address_precision": "street",
        "plan_coverage_match": False,
    }
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    address_sql = str(request.ctx.sa_session.executions[1][0][0])
    assert len(request.ctx.sa_session.executions) == 2
    assert "mrf.entity_address_unified addr" in provider_sql
    assert "addr.type IN ('practice', 'site', 'primary', 'secondary')" in provider_sql
    assert "UPPER(COALESCE(addr.state_code, addr.state_name, '')) = :location_state" in provider_sql
    assert "mrf.entity_address_unified addr" in address_sql
    assert "addr.group_plan_array @> ARRAY[:plan_id]::varchar[]" in address_sql
    assert "mrf.entity_address_plan_bridge eapb" in address_sql
    assert "CASE addr.type WHEN 'practice' THEN 0" in address_sql


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
                        "score_method": "direct",
                        "confidence_0_100": 91.0,
                        "confidence_band": "high",
                        "cost_source": "direct",
                        "data_coverage_0_100": 87.0,
                        "provider_class": "clinician",
                        "location_source": "doctor_clinician_address",
                        "has_claims": True,
                        "has_qpp": True,
                        "has_rx": False,
                        "has_enrollment": True,
                        "has_medicare_claims": True,
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
    assert payload["score_method"] == "direct"
    assert payload["confidence_0_100"] == 91.0
    assert payload["confidence_band"] == "high"
    assert payload["cost_source"] == "direct"
    assert payload["provider_class"] == "clinician"
    assert payload["evidence_profile"]["has_claims"] is True
    assert payload["evidence_profile"]["has_qpp"] is True
    assert payload["evidence_profile"]["location_source"] == "doctor_clinician_address"
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
    assert payload["score_method"] == "direct"
    assert payload["confidence_band"] == "high"
    assert payload["cost_source"] == "direct"
    assert payload["evidence_profile"]["has_claims"] is True


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
async def test_load_provider_quality_profile_sql_has_no_trailing_cte_comma(monkeypatch):
    async def _table_exists(_session, _table_name):
        return True

    async def _table_columns(_session, table_name):
        if table_name == pricing_module.EntityAddressUnified.__tablename__:
            return {"checksum", "multi_source_confirmed", "source_count"}
        if table_name == pricing_module.NUCCTaxonomy.__tablename__:
            return {"classification"}
        return set()

    class CapturingSession:
        async def execute(self, statement, _params=None):
            sql = str(statement)
            assert "WITH provider_choice AS" in sql
            assert "npi_address_choice AS" in sql
            npi_cte_tail = sql.split("npi_address_choice AS", 1)[1]
            assert "),\n            SELECT" not in npi_cte_tail
            return FakeResult(
                rows=[
                    {
                        "npi": 1234567893,
                        "specialty_key": None,
                        "taxonomy_code": None,
                        "taxonomy_classification": None,
                        "zip5": None,
                        "state_key": None,
                        "provider_class": "unknown",
                        "location_source": "unknown",
                        "has_enrollment": False,
                        "has_medicare_claims": False,
                    }
                ]
            )

    monkeypatch.setattr(pricing_module, "_table_exists", _table_exists)
    monkeypatch.setattr(pricing_module, "_table_columns", _table_columns)

    profile = await pricing_module._load_provider_quality_profile(
        CapturingSession(),
        npi=1234567893,
        year=2023,
    )

    assert profile == {
        "npi": 1234567893,
        "specialty_key": None,
        "taxonomy_code": None,
        "taxonomy_classification": None,
        "zip5": None,
        "state_key": None,
        "provider_class": "unknown",
        "location_source": "unknown",
        "has_enrollment": False,
        "has_medicare_claims": False,
    }


@pytest.mark.asyncio
async def test_get_pricing_provider_score_estimated_fallback(monkeypatch):
    async def _fake_profile(_session, *, npi, year):
        assert npi == 1003000126
        assert year == 2023
        return {
            "npi": npi,
            "specialty_key": "cardiology",
            "taxonomy_code": "207RC0000X",
            "zip5": "20850",
            "state_key": "MD",
            "provider_class": "clinician",
            "location_source": "doctor_clinician_address",
            "has_enrollment": True,
            "has_medicare_claims": False,
        }

    async def _fake_estimated_modes(_session, **_kwargs):
        return {
            "zip": pricing_module._build_quality_mode_payload(
                {
                    "model_version": "v2",
                    "benchmark_mode": "zip",
                    "tier": "acceptable",
                    "borderline_status": False,
                    "score_0_100": 57.4,
                    "estimated_cost_level": "$$$",
                    "score_method": "estimated",
                    "confidence_0_100": 43.0,
                    "confidence_band": "low",
                    "cost_source": "peer_estimated",
                    "data_coverage_0_100": 46.0,
                    "provider_class": "clinician",
                    "location_source": "doctor_clinician_address",
                    "has_claims": False,
                    "has_qpp": False,
                    "has_rx": False,
                    "has_enrollment": True,
                    "has_medicare_claims": False,
                    "risk_ratio_point": 0.94,
                    "ci75_low": 0.88,
                    "ci75_high": 1.01,
                    "ci90_low": 0.84,
                    "ci90_high": 1.05,
                    "low_score_threshold_failed": False,
                    "low_confidence_threshold_failed": False,
                    "high_score_threshold_passed": False,
                    "high_confidence_threshold_passed": False,
                },
                pricing_module._empty_domains_payload(),
                cohort_context={
                    "selected_geography": "zip:20850",
                    "selected_cohort_level": None,
                    "peer_count": 84,
                    "specialty_key": "cardiology",
                    "taxonomy_code": "207RC0000X",
                    "procedure_bucket": None,
                    "computed_live": False,
                    "procedure_match_threshold": None,
                },
            ),
            "state": None,
            "national": None,
        }

    monkeypatch.setattr(pricing_module, "_load_provider_quality_profile", _fake_profile)
    monkeypatch.setattr(pricing_module, "_load_estimated_quality_modes", _fake_estimated_modes)

    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_quality_score"),
            FakeResult(scalar="mrf.pricing_provider_quality_domain"),
            FakeResult(rows=[]),
        ],
        args={"year": "2023"},
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["benchmark_mode"] == "zip"
    assert payload["score_method"] == "estimated"
    assert payload["confidence_band"] == "low"
    assert payload["cost_source"] == "peer_estimated"
    assert payload["available_benchmark_modes"] == ["zip"]
    assert payload["cohort_context"]["selected_geography"] == "zip:20850"
    assert payload["evidence_profile"]["has_claims"] is False
    assert payload["evidence_profile"]["has_enrollment"] is True


@pytest.mark.asyncio
async def test_get_pricing_provider_score_unavailable_when_profile_is_insufficient(monkeypatch):
    async def _fake_profile(_session, *, npi, year):
        assert npi == 1003000126
        assert year == 2023
        return {
            "npi": npi,
            "specialty_key": None,
            "taxonomy_code": None,
            "zip5": None,
            "state_key": None,
            "provider_class": "unknown",
            "location_source": "unknown",
            "has_enrollment": False,
            "has_medicare_claims": False,
        }

    async def _unexpected_estimated_modes(_session, **_kwargs):
        raise AssertionError("estimated cohort lookup should not run for insufficient profiles")

    monkeypatch.setattr(pricing_module, "_load_provider_quality_profile", _fake_profile)
    monkeypatch.setattr(pricing_module, "_load_estimated_quality_modes", _unexpected_estimated_modes)

    request = make_request(
        [
            FakeResult(scalar="mrf.pricing_provider_quality_score"),
            FakeResult(scalar="mrf.pricing_provider_quality_domain"),
            FakeResult(rows=[]),
        ],
        args={"year": "2023"},
    )

    response = await get_pricing_provider_score(request, "1003000126")
    payload = json.loads(response.body)
    assert payload["score_method"] == "unavailable"
    assert payload["tier"] is None
    assert payload["score_0_100"] is None
    assert "missing_specialty_or_taxonomy" in payload["unavailable_reasons"]
    assert "missing_geography" in payload["unavailable_reasons"]
    assert payload["available_benchmark_modes"] == []


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
            FakeResult(scalar=None),
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
            FakeResult(scalar=None),
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
            FakeResult(scalar=None),
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
        args={
            "q": "mri",
            "year": "2023",
            "state": "MD",
            "include_sources": "true",
            "include_evidence": "true",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 1
    assert payload["items"][0]["npi"] == 1003000126
    assert payload["query"]["q"] == "mri"
    assert payload["query"]["state"] == "MD"
    assert payload["query"]["include_sources"] is True
    assert payload["query"]["include_evidence"] is True
    assert payload["sources"][0]["source_importer"] == "claims-pricing"
    assert payload["sources"][0]["serving_tables"] == ["pricing_provider", "pricing_provider_procedure"]
    assert payload["evidence"]["matched_provider_location_count"] == 1
    assert payload["evidence"]["filters"]["state"] == "MD"


@pytest.mark.asyncio
async def test_list_provider_specialties_filters_by_procedure_and_geo():
    request = make_request(
        [
            FakeResult(scalar=None),
            FakeResult(
                rows=[
                    {
                        "from_system": "CPT",
                        "from_code": "99214",
                        "to_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM,
                        "to_code": "1607056713",
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "specialty": "Family Medicine",
                        "specialty_key": "family medicine",
                        "provider_count": 12,
                        "total_services": 345.0,
                    },
                    {
                        "specialty": "Internal Medicine",
                        "specialty_key": "internal medicine",
                        "provider_count": 7,
                        "total_services": 123.0,
                    },
                ]
            ),
            FakeResult(scalar=2),
        ],
        args={
            "code": "99214",
            "code_system": "CPT",
            "zip5": "10001",
            "q": "medicine",
            "year": "2023",
            "limit": "10",
            "zip_radius_miles": "0",
        },
    )

    response = await list_provider_specialties(request)
    payload = json.loads(response.body)
    assert payload["pagination"]["total"] == 2
    assert payload["items"][0] == {
        "specialty": "Family Medicine",
        "specialty_key": "family medicine",
        "provider_count": 12,
        "total_services": 345.0,
    }
    assert payload["items"][1]["specialty"] == "Internal Medicine"
    assert payload["query"]["code"] == "99214"
    assert payload["query"]["zip5"] == "10001"
    assert payload["query"]["q"] == "medicine"
    assert {"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "1607056713"} in payload["query"][
        "resolved_codes"
    ]


@pytest.mark.asyncio
async def test_resolve_procedure_taxonomy_em_code_needs_intent(monkeypatch):
    async def fake_resolve_internal_codes(_session, _code, _args, default_system=None):
        return [1607056713], {
            "input_code": {"code_system": default_system, "code": "99214"},
            "resolved_codes": [{"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "1607056713"}],
            "internal_codes": [1607056713],
            "matched_via": [],
            "expanded": False,
        }

    async def fake_load_evidence(_session, *, year, internal_codes, limit):
        assert year == 2023
        assert internal_codes == [1607056713]
        assert limit == 10
        return [
            {
                "taxonomy_code": "207Q00000X",
                "classification": "Family Medicine",
                "specialization": None,
                "display_name": "Family Medicine",
                "distinct_npis": 80,
                "total_services": 500.0,
                "total_beneficiaries": 400.0,
                "provider_types": ["Family Practice"],
            }
        ]

    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", fake_resolve_internal_codes)
    monkeypatch.setattr(pricing_module, "_load_procedure_taxonomy_evidence", fake_load_evidence)
    request = make_request([], args={"code": "99214", "code_system": "CPT", "year": "2023"})

    response = await resolve_procedure_taxonomy(request)
    payload = json.loads(response.body)

    assert payload["resolution"]["status"] == "ambiguous"
    assert payload["resolution"]["recommended_mode"] == "ambiguous"
    assert payload["resolution"]["needs_intent"] is True
    assert payload["resolution"]["provider_filter"] is None


@pytest.mark.asyncio
async def test_resolve_procedure_taxonomy_acl_uses_soft_ortho_boost(monkeypatch):
    async def fake_resolve_internal_codes(_session, _code, _args, default_system=None):
        return [12329888], {
            "input_code": {"code_system": default_system, "code": "29888"},
            "resolved_codes": [{"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "12329888"}],
            "internal_codes": [12329888],
            "matched_via": [],
            "expanded": False,
        }

    async def fake_load_evidence(_session, *, year, internal_codes, limit):
        return []

    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", fake_resolve_internal_codes)
    monkeypatch.setattr(pricing_module, "_load_procedure_taxonomy_evidence", fake_load_evidence)
    request = make_request([], args={"code": "29888", "code_system": "CPT", "year": "2023"})

    response = await resolve_procedure_taxonomy(request)
    payload = json.loads(response.body)

    assert payload["resolution"]["status"] == "resolved"
    assert payload["resolution"]["recommended_mode"] == "soft_boost"
    assert payload["resolution"]["safe_for_hard_filter"] is False
    assert "207X00000X" in payload["resolution"]["taxonomy_codes"]
    assert payload["resolution"]["provider_boost"]["primary_only"] is False
    assert "procedure_is_known_to_skew_younger_than_medicare_ffs" in payload["evidence"]["bias_notes"]


@pytest.mark.asyncio
async def test_resolve_procedure_taxonomy_requires_explicit_hard_filter_opt_in(monkeypatch):
    async def fake_resolve_internal_codes(_session, _code, _args, default_system=None):
        return [170551], {
            "input_code": {"code_system": default_system, "code": "70551"},
            "resolved_codes": [{"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "170551"}],
            "internal_codes": [170551],
            "matched_via": [],
            "expanded": False,
        }

    async def fake_load_evidence(_session, *, year, internal_codes, limit):
        return [
            {
                "taxonomy_code": "2085R0202X",
                "classification": "Radiology",
                "specialization": "Diagnostic Radiology",
                "display_name": "Diagnostic Radiology Physician",
                "distinct_npis": 120,
                "total_services": 900.0,
                "total_beneficiaries": 500.0,
                "provider_types": ["Diagnostic Radiology"],
            }
        ]

    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", fake_resolve_internal_codes)
    monkeypatch.setattr(pricing_module, "_load_procedure_taxonomy_evidence", fake_load_evidence)

    request = make_request([], args={"code": "70551", "code_system": "CPT", "year": "2023"})
    response = await resolve_procedure_taxonomy(request)
    payload = json.loads(response.body)
    assert payload["resolution"]["safe_for_hard_filter"] is True
    assert payload["resolution"]["recommended_mode"] == "soft_boost"
    assert payload["resolution"]["provider_filter"] is None

    request = make_request(
        [],
        args={"code": "70551", "code_system": "CPT", "year": "2023", "allow_hard_filter": "true"},
    )
    response = await resolve_procedure_taxonomy(request)
    payload = json.loads(response.body)
    assert payload["resolution"]["recommended_mode"] == "hard_filter"
    assert "2085R0202X" in payload["resolution"]["provider_filter"]["taxonomy_codes"]
    assert payload["resolution"]["provider_filter"]["primary_only"] is False


@pytest.mark.asyncio
async def test_load_procedure_taxonomy_evidence_uses_cache(monkeypatch):
    call_counts_by_name = {"quality": 0}
    evidence_rows = [
        {
            "taxonomy_code": "207Q00000X",
            "classification": "Family Medicine",
            "specialization": None,
            "display_name": "Family Medicine",
            "distinct_npis": 80,
            "total_services": 500.0,
            "total_beneficiaries": 400.0,
            "provider_types": ["Family Practice"],
        }
    ]

    async def has_fake_table(_session, _table_name):
        return True

    async def fake_load_quality(_session, *, year, internal_codes, limit):
        assert year == 2023
        assert internal_codes == [1607056713]
        assert limit == 10
        call_counts_by_name["quality"] += 1
        return [dict(evidence_rows[0])]

    pricing_module._PROCEDURE_TAXONOMY_EVIDENCE_CACHE.clear()
    monkeypatch.setattr(pricing_module, "_table_exists", has_fake_table)
    monkeypatch.setattr(pricing_module, "_load_quality_procedure_taxonomy_evidence", fake_load_quality)

    first_items = await pricing_module._load_procedure_taxonomy_evidence(
        object(),
        year=2023,
        internal_codes=[1607056713],
        limit=10,
    )
    first_items[0]["taxonomy_code"] = "MUTATED"
    second_items = await pricing_module._load_procedure_taxonomy_evidence(
        object(),
        year=2023,
        internal_codes=[1607056713],
        limit=10,
    )

    assert call_counts_by_name == {"quality": 1}
    assert second_items[0]["taxonomy_code"] == "207Q00000X"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_routes_plan_filter_to_ptg2(monkeypatch):
    seen_args = {}

    async def fake_search(_session, args, pagination):
        seen_args.update(args)
        return {
            "items": [{"npi": 1234567890, "tic_prices": [{"negotiated_rate": "450.00"}]}],
            "pagination": {
                "total": 1,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2", "plan_id": args["plan_id"]},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [FakeResult(scalar=1)],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "source_key": "example_dental",
            "code": "70551",
            "limit": "10",
            "include_providers": "true",
            "include_code_details": "true",
            "include_sources": "true",
            "include_unverified_addresses": "true",
            "classification": "Internal Medicine",
            "taxonomy_codes": "207R00000X",
            "include_subspecialties": "false",
            "primary_only": "false",
            "lat": "29.7604",
            "long": "-95.3698",
            "radius_miles": "10",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == "450.00"
    assert payload["query"]["source"] == "ptg2"
    assert seen_args["plan_market_type"] == "group"
    assert seen_args["source_key"] == "example_dental"
    assert seen_args["include_providers"] == "true"
    assert seen_args["include_code_details"] == "true"
    assert seen_args["include_sources"] == "true"
    assert seen_args["include_unverified_addresses"] == "true"
    assert seen_args["include_details"] is None
    assert seen_args["classification"] == "Internal Medicine"
    assert seen_args["taxonomy_codes"] == "207R00000X"
    assert seen_args["include_subspecialties"] == "false"
    assert seen_args["primary_only"] == "false"
    assert seen_args["order_by"] == "total_allowed_amount"
    assert seen_args["order"] == "asc"
    assert seen_args["lat"] == 29.7604
    assert seen_args["long"] == -95.3698
    assert seen_args["radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_list_providers_by_procedure_infers_ptg_code_system(monkeypatch):
    observed_search_args_by_name = {}

    async def fake_search(_session, args, pagination):
        observed_search_args_by_name.update(args)
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2"},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={"plan_id": "010854205", "market_type": "group", "code": "70551"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["query"]["source"] == "ptg2"
    assert observed_search_args_by_name["code_system"] == "CPT"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_falls_back_to_allowed_amount_evidence(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    async def fake_allowed(_session, args, pagination):
        return {
            "result_state": "allowed_amounts_found",
            "pricing_scope": "plan_scoped_allowed_amounts",
            "items": [
                {
                    "npi": 1427166008,
                    "network_status": "out_of_network_or_not_confirmed_in_network",
                    "prices": [{"source": "allowed_amounts", "allowed_amount": 133.0}],
                }
            ],
            "pagination": {
                "total": 1,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg_allowed_amounts", "plan_id": args["plan_id"]},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    monkeypatch.setattr(pricing_module, "_search_ptg_allowed_amount_evidence", fake_allowed)
    request = make_request(
        [],
        args={
            "plan_id": "911643507",
            "market_type": "group",
            "code": "99203",
            "zip5": "60601",
            "year": "2023",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["result_state"] == "allowed_amounts_found"
    assert payload["pricing_scope"] == "plan_scoped_allowed_amounts"
    assert payload["items"][0]["network_status"] == "out_of_network_or_not_confirmed_in_network"
    assert payload["items"][0]["prices"][0]["source"] == "allowed_amounts"
    assert payload["query"]["year_semantics"] == "ignored_for_plan_scoped_ptg_rates"


@pytest.mark.asyncio
async def test_allowed_amount_empty_ptg_fallback(monkeypatch):
    async def fake_search(_session, _args, pagination):
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2", "status": "no_match"},
        }

    async def fake_allowed(_session, args, pagination):
        return {
            "result_state": "allowed_amounts_found",
            "pricing_scope": "plan_scoped_allowed_amounts",
            "items": [{"npi": 1427166008, "allowed_amount_min": 133.0}],
            "pagination": {
                "total": 1,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg_allowed_amounts", "plan_id": args["plan_id"]},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    monkeypatch.setattr(pricing_module, "_search_ptg_allowed_amount_evidence", fake_allowed)
    request = make_request(
        [],
        args={"plan_id": "911643507", "market_type": "group", "code": "99203", "zip5": "60601"},
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["result_state"] == "allowed_amounts_found"
    assert payload["items"][0]["allowed_amount_min"] == 133.0
    assert payload["query"]["source"] == "ptg_allowed_amounts"


@pytest.mark.asyncio
async def test_allowed_amount_search_checks_tables_and_allows_single_plan_alias(monkeypatch):
    """Allowed-amount fallback tolerates catalog-plan aliases for single-plan files."""
    checked_tables = []

    async def has_test_table(_session, table_name):
        checked_tables.append(table_name)
        return True

    async def fake_import_rows(_session, _args, *, plan_id):
        assert plan_id == "7862274fdc01bcc0"
        return [SMILE_IMPORT_ROW_BY_FIELD]

    plan_id_filters = []

    async def fake_allowed_rows(_session, *, table_names, plan_id, code, code_system, npi, limit):
        assert table_names == SMILE_ALLOWED_TABLE_NAMES
        assert code == "99214"
        assert code_system == "CPT"
        assert npi is None
        assert limit > 0
        plan_id_filters.append(plan_id)
        return [] if plan_id else [SMILE_ALLOWED_ROW_BY_FIELD]

    async def fake_provider_rows(_session, *, npis, **_kwargs):
        assert npis == (1427166008,)
        return []

    monkeypatch.setattr(pricing_module, "_table_exists", has_test_table)
    monkeypatch.setattr(pricing_module, "_allowed_amount_import_rows_for_plan", fake_import_rows)
    monkeypatch.setattr(pricing_module, "_allowed_amount_rows_from_tables", fake_allowed_rows)
    monkeypatch.setattr(pricing_module, "_ptg2_manifest_enriched_provider_rows_for_npis", fake_provider_rows)

    fallback_payload = await pricing_module._search_ptg_allowed_amount_evidence(
        FakeSession(),
        {"plan_id": "7862274fdc01bcc0", "market_type": "group", "code": "99214", "code_system": "CPT"},
        types.SimpleNamespace(limit=3, offset=0, page=1),
    )

    assert fallback_payload["result_state"] == "allowed_amounts_found"
    assert fallback_payload["pricing_scope"] == "plan_scoped_allowed_amounts"
    assert fallback_payload["items"][0]["network_status"] == "out_of_network_or_not_confirmed_in_network"
    assert fallback_payload["items"][0]["prices"][0]["allowed_amount"] == 133.0
    assert plan_id_filters == ["7862274fdc01bcc0", ""]
    assert checked_tables == ["hp_import_control.source_file_import", *SMILE_ALLOWED_TABLE_NAMES]


@pytest.mark.asyncio
async def test_allowed_amount_import_rows_include_file_plan_candidates():
    session = FakeSession(
        [
            FakeResult([]),
            FakeResult(
                [
                    {
                        "source_file_import_id": "candidate_import",
                        "snapshot_id": "ptg2:202607:candidate",
                        "source_key": "ptg_candidate",
                        "import_month": "2026-07-01",
                        "attempt_no": 1,
                        "metrics": {"plan_count": 1},
                    }
                ]
            ),
        ]
    )

    rows = await pricing_module._allowed_amount_import_rows_for_plan(
        session,
        {"market_type": "group"},
        plan_id="911643507",
    )

    assert [row["source_file_import_id"] for row in rows] == ["candidate_import"]
    assert len(session.executions) == 2


@pytest.mark.asyncio
async def test_list_providers_by_procedure_can_disable_allowed_amount_fallback(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    async def fail_allowed(*_args, **_kwargs):
        raise AssertionError("allowed amount fallback should be disabled")

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    monkeypatch.setattr(pricing_module, "_search_ptg_allowed_amount_evidence", fail_allowed)
    request = make_request(
        [],
        args={
            "plan_id": "911643507",
            "market_type": "group",
            "code": "99203",
            "include_allowed_amounts": "false",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["items"] == []
    assert payload["result_state"] == "no_snapshot_for_plan"
    assert payload["query"]["source"] == "ptg2"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_rejects_broad_group_plan_office_visit(monkeypatch):
    async def fail_search(*_args, **_kwargs):
        raise AssertionError("broad provider-directory request should fail before PTG search")

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fail_search)
    request = make_request(
        [],
        args={
            "plan_id": "010854205",
            "plan_id_type": "EIN",
            "market_type": "group",
            "code": "99213",
            "code_system": "CPT",
            "state": "IL",
            "city": "Chicago",
            "include_providers": "true",
            "limit": "50",
        },
    )

    with pytest.raises(pricing_module.InvalidUsage, match="provider-directory request"):
        await list_providers_by_procedure(request)


@pytest.mark.asyncio
async def test_list_providers_by_procedure_allows_taxonomy_scoped_group_plan_office_visit(monkeypatch):
    observed_search_args = {}

    async def fake_search(_session, args, pagination):
        observed_search_args.update(args)
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2"},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "code": "99213",
            "code_system": "CPT",
            "state": "IL",
            "city": "Chicago",
            "include_providers": "true",
            "classification": "Family Medicine",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["query"]["source"] == "ptg2"
    assert observed_search_args["classification"] == "Family Medicine"
    assert observed_search_args["include_providers"] == "true"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_routes_zip_as_default_radius_to_ptg2(monkeypatch):
    seen_args = {}

    async def fake_search(_session, args, pagination):
        seen_args.update(args)
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2", "zip_radius_miles": args["zip_radius_miles"]},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "zip5": "62401",
                        "state": "IL",
                        "city_lower": "effingham",
                        "latitude": 39.1200,
                        "longitude": -88.5434,
                    }
                ]
            )
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "code": "29888",
            "zip5": "62401",
            "include_providers": "true",
            "npi": "['']",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["query"]["zip_radius_miles"] == 10.0
    assert seen_args["zip5"] == "62401"
    assert seen_args["zip_radius_miles"] == 10.0
    assert seen_args["lat"] == 39.1200
    assert seen_args["long"] == -88.5434
    assert seen_args["radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_list_providers_by_procedure_allows_100_mile_zip_radius_to_ptg2(monkeypatch):
    seen_args = {}

    async def fake_search(_session, args, pagination):
        seen_args.update(args)
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2", "zip_radius_miles": args["zip_radius_miles"]},
        }

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [
            FakeResult(
                rows=[
                    {
                        "zip5": "62401",
                        "state": "IL",
                        "city_lower": "effingham",
                        "latitude": 39.1200,
                        "longitude": -88.5434,
                    }
                ]
            )
        ],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "code": "29888",
            "zip5": "62401",
            "zip_radius_miles": "100",
            "include_providers": "true",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["query"]["zip_radius_miles"] == 100.0
    assert seen_args["zip_radius_miles"] == 100.0
    assert seen_args["radius_miles"] == 100.0


@pytest.mark.asyncio
async def test_list_providers_by_procedure_empty_loaded_ptg_source_reports_no_match(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={
            "plan_id": "010854205",
            "market_type": "group",
            "source_key": "ptg_2627a71e793162ac",
            "snapshot_id": "ptg2:202606:7ed1678cf1a2",
            "code": "29888",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["items"] == []
    assert payload["pagination"]["total"] == 0
    assert payload["result_state"] == "no_matching_rates"
    assert payload["pricing_scope"] == "plan_scoped_ptg"
    assert payload["query"]["source"] == "ptg2"
    assert payload["query"]["status"] == "no_match"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_no_ptg_route_reports_reason(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={
            "plan_id": "010854205",
            "plan_id_type": "EIN",
            "market_type": "group",
            "code": "29888",
            "year": "2023",
        },
    )

    response = await list_providers_by_procedure(request)
    payload = json.loads(response.body)

    assert payload["resolved"] is False
    assert payload["result_state"] == "no_snapshot_for_plan"
    assert payload["pricing_scope"] == "plan_scoped_ptg"
    assert payload["reason"] == "no published serving snapshot for this plan_id + market_type"
    assert payload["pagination"]["total"] == 0
    assert payload["query"]["status"] == "no_route"
    assert payload["query"]["plan_id_type"] == "ein"
    assert payload["query"]["ignored_params"] == ["year"]
    assert payload["query"]["year_semantics"] == "ignored_for_plan_scoped_ptg_rates"


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
        args={
            "code": "123",
            "zip5": "20814",
            "zip_radius_miles": "0",
            "order_by": "cost_index",
            "order": "asc",
            "year": "2023",
        },
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
            FakeResult(scalar=None),
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
async def test_postal_radius_cache_avoids_repeated_geo_lookup(monkeypatch):
    """Repeated postal-radius expansion should reuse memory and return copies."""

    pricing_module._ZIP_RADIUS_ROWS_CACHE.clear()
    monkeypatch.setattr(pricing_module, "_ZIP_RADIUS_ROWS_CACHE_TTL_SECONDS", 300.0)
    monkeypatch.setattr(pricing_module, "_ZIP_RADIUS_ROWS_CACHE_MAX_KEYS", 16)
    geo_lookup_session = FakeSession(
        [
            FakeResult(rows=[{"zip5": "20814", "state": "MD", "latitude": 39.0, "longitude": -77.1}]),
            FakeResult(rows=[{"zip5": "20814", "state": "MD", "distance_miles": 0.0}]),
        ]
    )

    initial_radius_matches = await pricing_module._zip_radius_rows(
        geo_lookup_session,
        zip5="20814",
        radius_miles=5.0,
    )
    initial_radius_matches[0]["zip5"] = "mutated"
    cached_radius_matches = await pricing_module._zip_radius_rows(
        geo_lookup_session,
        zip5="20814",
        radius_miles=5.0,
    )

    assert len(geo_lookup_session.executions) == 2
    assert cached_radius_matches[0]["zip5"] == "20814"
    pricing_module._ZIP_RADIUS_ROWS_CACHE.clear()


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
        args={
            "code": "123",
            "zip5": "20814",
            "zip_radius_miles": "0",
            "order_by": "cost_index",
            "order": "asc",
            "year": "2023",
        },
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
            "zip_radius_miles": "0",
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
            FakeResult(scalar=None),
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
