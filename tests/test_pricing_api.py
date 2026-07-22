# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import os
import types
from datetime import datetime, timedelta, timezone
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from api import ptg2_capacity_evidence as capacity_evidence
from api import plan_release_serving
from api.ptg2_candidate_audit import (
    PTG2_CANDIDATE_AUDIT_ACCESS_ARG,
    PTG2_CANDIDATE_AUDIT_HEADER,
)

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


async def fake_plan_snapshot_pairs(_session, _plan_fields):
    return [("ptg_test", "ptg2:test")]


def strict_snapshot_tables(snapshot_id="ptg2:test"):
    snapshot_key_by_id = {
        "ptg2:test": 17,
        "ptg2:test:ndc": 41,
        "ptg2:test:c2": 42,
    }
    return types.SimpleNamespace(
        uses_shared_blocks=True,
        shared_snapshot_key=snapshot_key_by_id[snapshot_id],
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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["query"]["order_by"] == "tier_relevance"
    assert pricing_response["query"]["benchmark_mode"] == "national"


@pytest.mark.asyncio
async def test_group_plan_providers_filters_to_ten_digit_npis(monkeypatch):
    async def fake_snapshot_id(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1000000000), types.SimpleNamespace(npi=1234567890)]),
            FakeResult(scalar=2),
        ],
        args={"plan_id": "TESTPLAN001", "market_type": "group", "count": "true", "limit": "100"},
    )

    response = await group_plan_providers(request)
    pricing_response = json.loads(response.body)

    assert [pricing_item["npi"] for pricing_item in pricing_response["providers"]["items"]] == [1000000000, 1234567890]
    assert pricing_response["providers"]["total_distinct"] == 2
    first_params = request.ctx.sa_session.executions[0][0][1]
    count_params = request.ctx.sa_session.executions[1][0][1]
    assert first_params["npi_min"] == 1000000000
    assert first_params["npi_max"] == 9999999999
    assert count_params["npi_min"] == 1000000000
    assert count_params["npi_max"] == 9999999999
    assert "gm.npi BETWEEN :npi_min AND :npi_max" in str(request.ctx.sa_session.executions[0][0][0])


@pytest.mark.asyncio
async def test_group_plan_providers_pages_strict_v3_npi_scope(monkeypatch):
    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1234567890)])],
        args={"plan_id": "TESTPLAN001", "market_type": "group", "limit": "10"},
    )

    response = await group_plan_providers(request)
    pricing_response = json.loads(response.body)

    assert [pricing_item["npi"] for pricing_item in pricing_response["providers"]["items"]] == [1234567890]
    sql = str(request.ctx.sa_session.executions[0][0][0])
    assert "FROM mrf.ptg2_v3_npi_scope" in sql
    assert "snapshot_key = ANY(:snapshot_keys)" in sql


@pytest.mark.asyncio
async def test_group_plan_providers_filters_by_canonical_provider_sex(monkeypatch):
    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(
        pricing_module,
        "snapshot_serving_tables",
        fake_snapshot_serving_tables,
    )
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1234567890)])],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "provider_sex_code": "f",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    pricing_response = json.loads(response.body)

    sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert pricing_response["provider_sex_code"] == "F"
    assert "mrf.npi group_provider_sex_npi" in sql
    assert "group_provider_sex_npi.npi = gm.npi" in sql
    assert params["group_provider_sex_provider_sex_code"] == "F"


@pytest.mark.asyncio
async def test_group_plan_providers_filters_by_primary_taxonomy(monkeypatch):
    async def fake_snapshot_id(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1234567890)])],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "specialty": "Family Medicine",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    pricing_response = json.loads(response.body)

    assert [pricing_item["npi"] for pricing_item in pricing_response["providers"]["items"]] == [1234567890]
    assert pricing_response["taxonomy_filter"]["specialty"] == "Family Medicine"
    assert pricing_response["taxonomy_filter"]["taxonomy_code_set"] == ["207Q00000X", "208D00000X"]
    sql = str(request.ctx.sa_session.executions[0][0][0])
    params = request.ctx.sa_session.executions[0][0][1]
    assert "mrf.npi_taxonomy group_provider_specialty_nt" in sql
    assert "group_provider_specialty_nt.npi = gm.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert "display_name" not in sql
    assert params["group_provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["group_provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_group_plan_providers_does_not_refresh_legacy_specialty_cache(monkeypatch):
    async def fail_if_called(_session):
        raise AssertionError("PTG request must not use the process-global specialty cache")

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "ensure_specialty_resolution_cache",
        fail_if_called,
        raising=False,
    )
    monkeypatch.setattr(pricing_module, "current_network_snapshots_for_plan", fake_plan_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1234567890)])],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "specialty": "Family Medicine",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)

    assert json.loads(response.body)["providers"]["items"] == [{"npi": 1234567890}]


@pytest.mark.asyncio
async def test_plan_scoped_search_resolves_dynamic_specialty_without_legacy_cache(monkeypatch):
    seen_argument_map = {}

    async def fail_if_called(_session):
        raise AssertionError("PTG request must not use the process-global specialty cache")

    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
        return {
            "items": [],
            "pagination": {
                "total": 0,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2", "plan_id": args["plan_id"]},
        }

    monkeypatch.setattr(
        pricing_module,
        "ensure_specialty_resolution_cache",
        fail_if_called,
        raising=False,
    )
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [
            FakeResult(rows=[{
                "target_system": "NUCC",
                "target_code": "2085R0202X",
                "metadata_json": None,
            }]),
        ],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "70551",
            "specialty": "imaging specialist",
            "include_allowed_amounts": "false",
            "limit": "10",
        },
    )

    response = await list_providers_by_procedure(request)

    assert json.loads(response.body)["query"]["source"] == "ptg2"
    assert seen_argument_map["taxonomy_codes"] == ("2085R0202X",)
    assert len(request.ctx.sa_session.executions) == 1
    assert "FROM mrf.terminology_synonym" in str(request.ctx.sa_session.executions[0][0][0])


@pytest.mark.asyncio
async def test_group_plan_providers_classification_internal_medicine_uses_base_taxonomy(monkeypatch):
    async def fake_snapshot_id(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
        fake_plan_snapshot_pairs,
    )
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1003000530)])],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "classification": "Internal Medicine",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["taxonomy_filter"]["classification"] == "Internal Medicine"
    assert pricing_response["taxonomy_filter"]["taxonomy_code_set"] == ["207R00000X"]
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
        return strict_snapshot_tables(_snapshot_id)

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
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
            "plan_id": "TESTPLAN001",
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
    pricing_response = json.loads(response.body)

    assert pricing_response["location_filter"] == {
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
            "plan_ids_considered": ["TESTPLAN001"],
            "plan_market_types_considered": ["group"],
    }
    assert pricing_response["providers"]["total_distinct"] == 1
    assert pricing_response["providers"]["items"][0]["address"] == {
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
        return strict_snapshot_tables(_snapshot_id)

    async def fake_zip_radius_rows(_session, *, zip5, radius_miles, state_hint=None, **_kwargs):
        assert zip5 == "60601"
        assert radius_miles == 10.0
        return [
            {"zip5": "60601", "distance_miles": 0.0, "is_anchor": True},
            {"zip5": "60602", "distance_miles": 0.6},
            {"zip5": "60611", "distance_miles": 1.2},
        ]

    monkeypatch.setattr(pricing_module, "current_network_snapshots_for_plan", fake_plan_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_zip_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "TESTPLAN001",
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
        return strict_snapshot_tables(_snapshot_id)

    async def fake_zip_radius_rows(_session, *, zip5, radius_miles, state_hint=None, **_kwargs):
        return [{"zip5": "60601"}, {"zip5": "60602"}]

    monkeypatch.setattr(pricing_module, "current_network_snapshots_for_plan", fake_plan_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_zip_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "TESTPLAN001",
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

    async def fake_snapshot_serving_tables(_session, snapshot_id):
        return strict_snapshot_tables(snapshot_id)

    monkeypatch.setattr(pricing_module, "current_network_snapshots_for_plan", fake_network_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    request = make_request(
        [FakeResult(rows=[types.SimpleNamespace(npi=1073913877)])],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "enrich": "0",
            "limit": "10",
        },
    )

    response = await group_plan_providers(request)
    response_payload = json.loads(response.body)

    assert response_payload["snapshot_id"] == "ptg2:test:ndc"
    assert response_payload["snapshots"] == [
        {
            "source_key": "ptg_ndc",
            "snapshot_id": "ptg2:test:ndc",
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "enumerated": True,
        },
        {
            "source_key": "ptg_c2",
            "snapshot_id": "ptg2:test:c2",
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "enumerated": True,
        },
    ]
    provider_sql = str(request.ctx.sa_session.executions[0][0][0])
    assert "SELECT npi FROM mrf.ptg2_v3_npi_scope" in provider_sql
    assert "snapshot_key = ANY(:snapshot_keys)" in provider_sql
    assert "(SELECT npi FROM" in provider_sql


def _canonical_directory_selection():
    return _canonical_release_selection(
        [
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=0,
                snapshot_id="ptg2:release:a",
                source_key="aetna-a",
                plan_id="38-2418014",
                plan_market_type="group",
                role="in_network",
                required=True,
            ),
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=1,
                snapshot_id="ptg2:release:b",
                source_key="aetna-b",
                plan_id="PLAN-B",
                plan_market_type="group",
                role="in_network",
                required=True,
            ),
        ]
    )


def _canonical_directory_tables():
    return {
        "ptg2:release:a": types.SimpleNamespace(
            uses_shared_blocks=True,
            shared_snapshot_key=71,
            plan_id="382418014",
            plan_market_type="group",
            source_key="aetna-a",
        ),
        "ptg2:release:b": types.SimpleNamespace(
            uses_shared_blocks=True,
            shared_snapshot_key=72,
            plan_id="PLAN-B",
            plan_market_type="group",
            source_key="aetna-b",
        ),
    }


def _configure_canonical_directory(monkeypatch, selection, tables_by_snapshot_id):
    monkeypatch.setattr(
        pricing_module,
        "resolve_plan_release_serving",
        AsyncMock(return_value=selection),
    )
    monkeypatch.setattr(
        pricing_module,
        "snapshot_serving_tables",
        AsyncMock(
            side_effect=lambda _session, snapshot_id: (
                tables_by_snapshot_id[snapshot_id]
            )
        ),
    )
    monkeypatch.setattr(
        pricing_module,
        "_group_plan_provider_address_source",
        AsyncMock(
            return_value=("mrf.entity_address_unified", True, True, True)
        ),
    )


def _canonical_directory_request(selection):
    address_by_field = {
        "npi": 1073913877,
        "type": "practice",
        "first_line": "1 TEST WAY",
        "second_line": None,
        "city_name": "CHICAGO",
        "state_name": "IL",
        "postal_code": "606010000",
        "zip5": "60601",
        "phone_number": None,
        "address_precision": "street",
        "plan_coverage_match": True,
    }
    return make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877)]),
            FakeResult(rows=[address_by_field]),
        ],
        args={
            "plan_release_id": selection.plan_release_id,
            "city": "Chicago",
            "enrich": "0",
            "limit": "10",
        },
    )


def _assert_canonical_directory_response(response_by_field, selection, request):
    assert response_by_field["resolved"] is True
    assert response_by_field["plan_id"] is None
    assert response_by_field["plan_ids"] == ["38-2418014", "PLAN-B"]
    assert response_by_field["market_type"] == "group"
    assert response_by_field["snapshots"] == [
        {
            "source_key": "aetna-a",
            "snapshot_id": "ptg2:release:a",
            "plan_id": "38-2418014",
            "plan_market_type": "group",
            "enumerated": True,
        },
        {
            "source_key": "aetna-b",
            "snapshot_id": "ptg2:release:b",
            "plan_id": "PLAN-B",
            "plan_market_type": "group",
            "enumerated": True,
        },
    ]
    assert response_by_field["healthporta_plan_id"] == selection.healthporta_plan_id
    assert response_by_field["query"]["plan_release_id"] == selection.plan_release_id
    address_sql = str(request.ctx.sa_session.executions[2][0][0])
    address_parameters = request.ctx.sa_session.executions[2][0][1]
    assert "eapb.plan_id = ANY(CAST(:plan_ids AS text[]))" in address_sql
    assert address_parameters["plan_ids"] == ["38-2418014", "382418014", "PLAN-B"]
    assert response_by_field["location_filter"]["plan_ids_considered"] == [
        "38-2418014",
        "PLAN-B",
    ]


@pytest.mark.asyncio
async def test_canonical_group_directory_preserves_every_binding_scope(
    monkeypatch,
):
    """Canonical directory reads must preserve each frozen source scope."""

    selection = _canonical_directory_selection()
    _configure_canonical_directory(
        monkeypatch,
        selection,
        _canonical_directory_tables(),
    )
    request = _canonical_directory_request(selection)

    response = await group_plan_providers(request)
    response_by_field = json.loads(response.body)
    _assert_canonical_directory_response(response_by_field, selection, request)


@pytest.mark.asyncio
async def test_canonical_group_directory_fails_closed_on_scope_mismatch(
    monkeypatch,
):
    selection = _canonical_release_selection(
        [
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=0,
                snapshot_id="ptg2:release:a",
                source_key="aetna-a",
                plan_id="38-2418014",
                plan_market_type="group",
                role="in_network",
                required=True,
            )
        ]
    )
    monkeypatch.setattr(
        pricing_module,
        "resolve_plan_release_serving",
        AsyncMock(return_value=selection),
    )
    monkeypatch.setattr(
        pricing_module,
        "snapshot_serving_tables",
        AsyncMock(return_value=types.SimpleNamespace(
            uses_shared_blocks=True,
            shared_snapshot_key=71,
            plan_id="ANOTHER-PLAN",
            plan_market_type="group",
            source_key="aetna-a",
        )),
    )
    request = make_request(
        [],
        args={"plan_release_id": selection.plan_release_id},
    )

    with pytest.raises(
        pricing_module.PTG2ManifestArtifactError,
        match="attested plan, market, and source scope",
    ):
        await group_plan_providers(request)


@pytest.mark.asyncio
async def test_unknown_canonical_group_release_stays_unresolved(monkeypatch):
    monkeypatch.setattr(
        pricing_module,
        "resolve_plan_release_serving",
        AsyncMock(return_value=None),
    )
    release_id = "hprelease_" + "0" * 26
    response = await group_plan_providers(
        make_request([], args={"plan_release_id": release_id})
    )
    response_by_field = json.loads(response.body)

    assert response_by_field["resolved"] is False
    assert response_by_field["plan_release_id"] == release_id
    assert "healthporta_plan_id" not in response_by_field
    assert response_by_field["providers"]["items"] == []
    assert response_by_field["reason"] == (
        "no complete published serving revision for this plan_release_id"
    )


@pytest.mark.asyncio
async def test_group_plan_providers_splits_multi_network_postal_scans(monkeypatch):
    """ZIP-filtered multi-network lookups should keep per-network member scans indexable."""

    async def fake_network_snapshot_pairs(_session, _plan_fields):
        return [("ptg_ndc", "ptg2:test:ndc"), ("ptg_c2", "ptg2:test:c2")]

    async def fake_snapshot_serving_tables(_session, snapshot_id):
        return strict_snapshot_tables(snapshot_id)

    async def fake_postal_radius_rows(_session, **keyword_args):
        return [{"zip5": keyword_args["zip5"]}, {"zip5": "60602"}]

    monkeypatch.setattr(pricing_module, "current_network_snapshots_for_plan", fake_network_snapshot_pairs)
    monkeypatch.setattr(pricing_module, "snapshot_serving_tables", fake_snapshot_serving_tables)
    monkeypatch.setattr(pricing_module, "_zip_radius_rows", fake_postal_radius_rows)
    request = make_request(
        [
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877), types.SimpleNamespace(npi=1234567890)]),
            FakeResult(rows=[types.SimpleNamespace(npi=1073913877), types.SimpleNamespace(npi=1003000126)]),
            FakeResult(rows=[]),
        ],
        args={
            "plan_id": "TESTPLAN001",
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
    assert "FROM mrf.ptg2_v3_npi_scope gm" in first_member_query_sql
    assert "FROM mrf.ptg2_v3_npi_scope gm" in second_member_query_sql
    assert "UNION ALL" not in first_member_query_sql
    assert "UNION ALL" not in second_member_query_sql
    assert "EXISTS (" in first_member_query_sql
    assert first_member_query_params["split_snapshot_key"] == 41
    assert first_member_query_params["limit"] == 10
    assert first_member_query_params["location_zips"] == ["60601", "60602"]


@pytest.mark.asyncio
async def test_group_plan_providers_uses_unified_service_locations_when_configured(monkeypatch):
    """Unified address serving should use service-location rows for group-plan filters."""

    async def fake_snapshot_id_for_plan(_session, _plan_fields):
        return "ptg2:test"

    async def fake_snapshot_serving_tables(_session, _snapshot_id):
        return strict_snapshot_tables(_snapshot_id)

    async def fake_group_plan_provider_address_source(_session):
        return "mrf.entity_address_unified", True, True, True

    monkeypatch.setattr(
        pricing_module,
        "current_source_snapshot_id_for_plan",
        fake_snapshot_id_for_plan,
    )
    monkeypatch.setattr(
        pricing_module,
        "current_network_snapshots_for_plan",
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
            "plan_id": "TESTPLAN001",
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
    pricing_response = json.loads(response.body)

    assert pricing_response["location_filter"]["address_source"] == "unified"
    assert pricing_response["location_filter"]["address_types"] == ["practice", "site", "primary", "secondary"]
    assert pricing_response["location_filter"]["count_requested"] is True
    assert pricing_response["location_filter"]["count_exact"] is False
    assert pricing_response["providers"]["total_distinct"] is None
    assert pricing_response["providers"]["items"][0]["address"] == {
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
    assert "addr.group_plan_array" in address_sql
    assert "&& CAST(:plan_ids AS varchar[])" in address_sql
    assert "mrf.entity_address_plan_bridge eapb" in address_sql
    assert "eapb.plan_id = ANY(CAST(:plan_ids AS text[]))" in address_sql
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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["procedure_code"] == 123
    assert pricing_response["items"][0]["service_code_system"] == "HP_PROCEDURE_CODE"
    assert pricing_response["items"][0]["service_code"] == "123"
    assert pricing_response["items"][0]["service_name"] == "Apixaban"
    assert pricing_response["items"][0]["total_services"] == 45
    assert pricing_response["items"][0]["total_allowed_amount"] == 998.7
    assert "total_30day_fills" not in pricing_response["items"][0]
    assert "total_day_supply" not in pricing_response["items"][0]
    assert "total_drug_cost" not in pricing_response["items"][0]


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["city"] == "Bethesda"
    assert pricing_response["items"][0]["service_code"] == "321"
    assert pricing_response["items"][0]["total_allowed_amount"] == 55.0


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
    pricing_response = json.loads(response.body)
    pricing_item = pricing_response["items"][0]
    assert pricing_item["reported_code"] == "70553"
    assert pricing_item["reported_code_system"] == "CPT"
    assert pricing_item["total_30day_fills"] == 40
    assert pricing_item["total_day_supply"] == 50
    assert pricing_item["total_drug_cost"] == 998.7


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["query"]["input_code"]["code_system"] == "HCPCS"
    assert pricing_response["query"]["resolved_codes"][0]["code_system"] == "HCPCS"


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["query"]["input_code"]["code_system"] == "HP_PROCEDURE_CODE"


@pytest.mark.asyncio
async def test_get_pricing_provider_score_success():
    """Return the configured provider score and benchmark details."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["npi"] == 1003000126
    assert pricing_response["model_version"] == "v2"
    assert pricing_response["benchmark_mode"] == "national"
    assert pricing_response["tier"] == "high"
    assert pricing_response["borderline_status"] is False
    assert pricing_response["score_0_100"] == 72.4
    assert pricing_response["estimated_cost_level"] == "$$"
    assert pricing_response["score_method"] == "direct"
    assert pricing_response["confidence_0_100"] == 91.0
    assert pricing_response["confidence_band"] == "high"
    assert pricing_response["cost_source"] == "direct"
    assert pricing_response["provider_class"] == "clinician"
    assert pricing_response["evidence_profile"]["has_claims"] is True
    assert pricing_response["evidence_profile"]["has_qpp"] is True
    assert pricing_response["evidence_profile"]["location_source"] == "doctor_clinician_address"
    assert pricing_response["overall"]["risk_ratio_point"] == 0.91
    assert pricing_response["overall"]["ci_75"]["low"] == 0.86
    assert pricing_response["domains"]["appropriateness"]["risk_ratio_point"] == 0.93
    assert pricing_response["domains"]["effectiveness"]["score_0_100"] == 75.0
    assert pricing_response["domains"]["cost"]["evidence_n"] == 201.0
    assert pricing_response["curation_checks"]["high_score_threshold_passed"] is True
    assert pricing_response["cohort_context"]["computed_live"] is False
    assert pricing_response["cohort_context"]["selected_geography"] == "national"
    assert pricing_response["cohort_context"]["selected_cohort_level"] == "L3"
    assert pricing_response["scores_by_benchmark_mode"]["zip"]["tier"] == "low"
    assert pricing_response["scores_by_benchmark_mode"]["state"]["tier"] == "acceptable"
    assert pricing_response["scores_by_benchmark_mode"]["national"]["tier"] == "high"
    assert pricing_response["scores_by_benchmark_mode"]["national"]["cohort_context"]["computed_live"] is False
    assert pricing_response["available_benchmark_modes"] == ["zip", "state", "national"]


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
    pricing_response = json.loads(response.body)
    assert pricing_response["benchmark_mode"] == "zip"
    assert pricing_response["tier"] == "high"
    assert pricing_response.get("cohort_context") is None
    assert pricing_response["scores_by_benchmark_mode"]["state"] is None


@pytest.mark.asyncio
async def test_get_pricing_provider_score_live_override_path():
    """Prefer a qualifying live score over the stored score."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["benchmark_mode"] == "zip"
    assert pricing_response["tier"] == "acceptable"
    assert pricing_response["score_0_100"] == 50.0
    assert pricing_response["available_benchmark_modes"] == ["zip"]
    assert pricing_response["scores_by_benchmark_mode"]["zip"] is not None
    assert pricing_response["scores_by_benchmark_mode"]["state"] is None
    assert pricing_response["scores_by_benchmark_mode"]["national"] is None
    assert pricing_response["cohort_context"]["computed_live"] is True
    assert pricing_response["cohort_context"]["selected_geography"] == "zip:20850"
    assert pricing_response["cohort_context"]["selected_cohort_level"] == "L0"
    assert pricing_response["cohort_context"]["peer_count"] == 44
    assert pricing_response["cohort_context"]["specialty_key"] == "cardiology"
    assert pricing_response["cohort_context"]["taxonomy_code"] == "207RC0000X"
    assert pricing_response["cohort_context"]["procedure_match_threshold"] == 0.3
    assert pricing_response["score_method"] == "direct"
    assert pricing_response["confidence_band"] == "high"
    assert pricing_response["cost_source"] == "direct"
    assert pricing_response["evidence_profile"]["has_claims"] is True


@pytest.mark.asyncio
async def test_live_score_override_all_modes():
    """Apply live overrides across modes when no benchmark is forced."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["benchmark_mode"] == "zip"
    assert pricing_response["available_benchmark_modes"] == ["zip", "state", "national"]
    assert pricing_response["scores_by_benchmark_mode"]["zip"] is not None
    assert pricing_response["scores_by_benchmark_mode"]["state"] is not None
    assert pricing_response["scores_by_benchmark_mode"]["national"] is not None
    assert pricing_response["cohort_context"]["computed_live"] is True


@pytest.mark.asyncio
async def test_load_provider_quality_profile_sql_has_no_trailing_cte_comma(monkeypatch):
    async def _is_table_present(_session, _table_name):
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

    monkeypatch.setattr(pricing_module, "_table_exists", _is_table_present)
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
    """Estimate a provider score when direct score evidence is absent."""
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
                pricing_module._empty_domain_payloads_by_name(),
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
    pricing_response = json.loads(response.body)
    assert pricing_response["benchmark_mode"] == "zip"
    assert pricing_response["score_method"] == "estimated"
    assert pricing_response["confidence_band"] == "low"
    assert pricing_response["cost_source"] == "peer_estimated"
    assert pricing_response["available_benchmark_modes"] == ["zip"]
    assert pricing_response["cohort_context"]["selected_geography"] == "zip:20850"
    assert pricing_response["evidence_profile"]["has_claims"] is False
    assert pricing_response["evidence_profile"]["has_enrollment"] is True


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
    pricing_response = json.loads(response.body)
    assert pricing_response["score_method"] == "unavailable"
    assert pricing_response["tier"] is None
    assert pricing_response["score_0_100"] is None
    assert "missing_specialty_or_taxonomy" in pricing_response["unavailable_reasons"]
    assert "missing_geography" in pricing_response["unavailable_reasons"]
    assert pricing_response["available_benchmark_modes"] == []


@pytest.mark.asyncio
async def test_get_pricing_provider_score_variants_scope_provider_returns_variant_cards():
    """Return score variant cards for provider-scoped requests."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["variants_scope"] == "provider"
    assert pricing_response["benchmark_mode"] == "zip"
    assert pricing_response["available_benchmark_modes"] == ["zip", "state", "national"]
    assert len(pricing_response["variants_by_benchmark_mode"]["zip"]) == 2
    assert len(pricing_response["variants_by_benchmark_mode"]["state"]) == 1
    assert len(pricing_response["variants_by_benchmark_mode"]["national"]) == 1
    assert pricing_response["variants_by_benchmark_mode"]["zip"][0]["benchmark_mode"] == "zip"
    assert pricing_response["variants_by_benchmark_mode"]["zip"][0]["cohort_context"]["selected_cohort_level"] == "L0"
    assert pricing_response["variants_by_benchmark_mode"]["zip"][0]["cohort_context"]["computed_live"] is True


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["prescription_code_system"] == "HP_RX_CODE"
    assert pricing_response["items"][0]["prescription_code"] == "12345"
    assert pricing_response["items"][0]["prescription_name"] == "Atorvastatin"
    assert pricing_response["items"][0]["total_prescriptions"] == 100
    assert pricing_response["items"][0]["total_allowed_amount"] == 1200.0
    assert pricing_response["items"][0]["preferred_prescription_code_system"] == "HP_RX_CODE"
    assert pricing_response["items"][0]["preferred_prescription_code"] == "12345"


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
    pricing_response = json.loads(response.body)
    pricing_item = pricing_response["items"][0]
    assert pricing_item["ndc_code"] == "00071015523"
    assert pricing_item["rxnorm_id"] == "83367"
    assert pricing_item["preferred_prescription_code_system"] == "NDC"
    assert pricing_item["preferred_prescription_code"] == "00071015523"


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
    pricing_response = json.loads(response.body)
    assert pricing_response["prescription_code"] == "12345"
    assert pricing_response["prescription_name"] == "Atorvastatin"
    assert pricing_response["total_prescriptions"] == 50
    assert pricing_response["total_allowed_amount"] == 600.0


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["items"][0]["total_prescriptions"] == 80
    assert pricing_response["items"][0]["total_allowed_amount"] == 900.0


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
    pricing_response = json.loads(response.body)
    assert pricing_response["query"]["input_code"]["code_system"] == "NDC"
    assert pricing_response["benchmark"]["provider_count"] == 5
    assert pricing_response["benchmark"]["total_prescriptions"] == 300.0
    assert pricing_response["benchmark"]["total_allowed_amount"] == 2500.0


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    pricing_item = pricing_response["items"][0]
    assert pricing_item["term"] == "Magnetic resonance imaging"
    assert set(pricing_item["code_systems"]) == {"CPT", "HCPCS", "HP_PROCEDURE_CODE"}
    assert "123456" in pricing_item["internal_codes"]


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    pricing_item = pricing_response["items"][0]
    assert pricing_item["generic_name"] == "Lisinopril"
    assert pricing_item["brand_name"] == "Prinivil"
    assert pricing_item["display_label"] == "Lisinopril / Prinivil"
    assert pricing_item["total_prescriptions"] == 500
    assert pricing_item["prescription_code_system"] == "HP_RX_CODE"
    assert pricing_item["prescription_code"] == "ABC123"
    assert pricing_item["preferred_prescription_code_system"] == "HP_RX_CODE"
    assert pricing_item["preferred_prescription_code"] == "ABC123"


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    pricing_item = pricing_response["items"][0]
    assert pricing_item["ndc_code"] == "00093015001"
    assert pricing_item["rxnorm_id"] == "29046"
    assert pricing_item["preferred_prescription_code_system"] == "NDC"
    assert pricing_item["preferred_prescription_code"] == "00093015001"


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
            "provider_sex_code": "f",
            "include_sources": "true",
            "include_evidence": "true",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["query"]["q"] == "mri"
    assert pricing_response["query"]["state"] == "MD"
    assert pricing_response["query"]["provider_sex_code"] == "F"
    assert pricing_response["query"]["include_sources"] is True
    assert pricing_response["query"]["include_evidence"] is True
    assert pricing_response["sources"][0]["source_importer"] == "claims-pricing"
    assert pricing_response["sources"][0]["serving_tables"] == ["pricing_provider", "pricing_provider_procedure"]
    assert pricing_response["evidence"]["matched_provider_location_count"] == 1
    assert pricing_response["evidence"]["filters"]["state"] == "MD"
    assert "mrf.npi" in str(request.ctx.sa_session.executions[1][0][0])
    assert "provider_sex_code" in str(
        request.ctx.sa_session.executions[1][0][0]
    )


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 2
    assert pricing_response["items"][0] == {
        "specialty": "Family Medicine",
        "specialty_key": "family medicine",
        "provider_count": 12,
        "total_services": 345.0,
    }
    assert pricing_response["items"][1]["specialty"] == "Internal Medicine"
    assert pricing_response["query"]["code"] == "99214"
    assert pricing_response["query"]["zip5"] == "10001"
    assert pricing_response["query"]["q"] == "medicine"
    assert {"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "1607056713"} in pricing_response["query"][
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
    pricing_response = json.loads(response.body)

    assert pricing_response["resolution"]["status"] == "ambiguous"
    assert pricing_response["resolution"]["recommended_mode"] == "ambiguous"
    assert pricing_response["resolution"]["needs_intent"] is True
    assert pricing_response["resolution"]["provider_filter"] is None


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
    pricing_response = json.loads(response.body)

    assert pricing_response["resolution"]["status"] == "resolved"
    assert pricing_response["resolution"]["recommended_mode"] == "soft_boost"
    assert pricing_response["resolution"]["safe_for_hard_filter"] is False
    assert "207X00000X" in pricing_response["resolution"]["taxonomy_codes"]
    assert pricing_response["resolution"]["provider_boost"]["primary_only"] is False
    assert "procedure_is_known_to_skew_younger_than_medicare_ffs" in pricing_response["evidence"]["bias_notes"]


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
    pricing_response = json.loads(response.body)
    assert pricing_response["resolution"]["safe_for_hard_filter"] is True
    assert pricing_response["resolution"]["recommended_mode"] == "soft_boost"
    assert pricing_response["resolution"]["provider_filter"] is None

    request = make_request(
        [],
        args={"code": "70551", "code_system": "CPT", "year": "2023", "allow_hard_filter": "true"},
    )
    response = await resolve_procedure_taxonomy(request)
    pricing_response = json.loads(response.body)
    assert pricing_response["resolution"]["recommended_mode"] == "hard_filter"
    assert "2085R0202X" in pricing_response["resolution"]["provider_filter"]["taxonomy_codes"]
    assert pricing_response["resolution"]["provider_filter"]["primary_only"] is False


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
    """Pass plan filters through the PTG provider search path."""
    seen_argument_map = {}
    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
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
            "plan_id": "TESTPLAN001",
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
            "provider_sex_code": "x",
            "include_subspecialties": "false",
            "primary_only": "false",
            "lat": "29.7604",
            "long": "-95.3698",
            "radius_miles": "10",
        },
    )
    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["items"][0]["tic_prices"][0]["negotiated_rate"] == "450.00"
    assert pricing_response["query"]["source"] == "ptg2"
    assert seen_argument_map["plan_market_type"] == "group"
    assert seen_argument_map["source_key"] == "example_dental"
    assert seen_argument_map["include_providers"] == "true"
    assert seen_argument_map["include_code_details"] == "true"
    assert seen_argument_map["include_sources"] == "true"
    assert seen_argument_map["include_unverified_addresses"] == "true"
    assert seen_argument_map["include_details"] is None
    assert seen_argument_map["classification"] == "Internal Medicine"
    assert seen_argument_map["taxonomy_codes"] == "207R00000X"
    assert seen_argument_map["provider_sex_code"] == "X"
    assert seen_argument_map["include_subspecialties"] == "false"
    assert seen_argument_map["primary_only"] == "false"
    assert seen_argument_map["order_by"] == "total_allowed_amount"
    assert seen_argument_map["order"] == "asc"
    assert seen_argument_map["lat"] == 29.7604
    assert seen_argument_map["long"] == -95.3698
    assert seen_argument_map["radius_miles"] == 10.0


def _configure_pricing_capacity_evidence(monkeypatch):
    """Configure a fresh, isolated Ed25519 evidence signer for this test."""

    private_key = Ed25519PrivateKey.from_private_bytes(bytes(range(1, 33)))
    public_key = private_key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )
    release_digest = "11" * 32
    environment_id = "22" * 32
    key_id = "capacity-api-test-key"
    run_nonce = "a1" * 32
    contention_run_id = "b1" * 32
    process_started_at = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(
        seconds=1
    )

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setenv(capacity_evidence.CAPACITY_ISOLATED_PROCESS_ENV, "1")
    monkeypatch.setenv(
        capacity_evidence.CAPACITY_PRIVATE_KEY_ENV,
        private_key.private_bytes_raw().hex(),
    )
    monkeypatch.setenv(capacity_evidence.CAPACITY_KEY_ID_ENV, key_id)
    monkeypatch.setenv(
        capacity_evidence.CAPACITY_RELEASE_DIGEST_ENV, release_digest
    )
    monkeypatch.setenv(
        capacity_evidence.CAPACITY_ENVIRONMENT_ID_ENV, environment_id
    )
    monkeypatch.setattr(
        capacity_evidence._PROCESS_IDENTITY, "process_id", os.getpid()
    )
    monkeypatch.setattr(
        capacity_evidence._PROCESS_IDENTITY, "instance", "ab" * 32
    )
    monkeypatch.setattr(
        capacity_evidence._PROCESS_IDENTITY, "started_at", process_started_at
    )
    monkeypatch.setattr(
        capacity_evidence._PROCESS_IDENTITY,
        "challenge_state",
        capacity_evidence.CapacityEvidenceState(),
    )
    monkeypatch.setattr(
        capacity_evidence._PROCESS_IDENTITY,
        "isolated_request_instance",
        None,
    )
    return (
        public_key,
        release_digest,
        environment_id,
        key_id,
        run_nonce,
        contention_run_id,
    )


def _capacity_evidence_pricing_request(run_nonce, contention_run_id):
    """Create the exact standard PTG request used by the evidence contract."""

    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "snapshot_id": "strict-v3-snapshot",
            "mode": "exact_source",
            "code_system": "CPT",
            "code": "99213",
            "npi": "1234567890",
            "include_providers": "true",
            "include_details": "true",
            "include_sources": "true",
            "include_allowed_amounts": "false",
            "include_unverified_addresses": "true",
            "order_by": "npi",
            "order": "asc",
            "offset": "0",
            "limit": "100",
        },
    )
    challenge = "7d" * 32
    request.headers = {
        "Authorization": "Bearer operator-secret",
        capacity_evidence.CAPACITY_CHALLENGE_HEADER: challenge,
        capacity_evidence.CAPACITY_RUN_NONCE_HEADER: run_nonce,
        capacity_evidence.CAPACITY_CONTENTION_RUN_ID_HEADER: contention_run_id,
        capacity_evidence.CAPACITY_SEMANTIC_CLASS_HEADER: "matched_positive",
        capacity_evidence.CAPACITY_SELECTION_ORDINAL_HEADER: "0",
    }
    request.method = "GET"
    request.path = capacity_evidence.CAPACITY_QUERY_PATH
    request.route = types.SimpleNamespace(
        name="pricing.providers.search_by_procedure"
    )
    return request, challenge


def _assert_capacity_observation(observation_map, key_id, contention_run_id):
    """Assert the endpoint-bound, redacted V3 evidence contract."""

    assert observation_map["api_evidence_key_id"] == key_id
    assert observation_map["response_status"] == 200
    assert observation_map["observation_ordinal"] == 0
    assert observation_map["cold"] is True
    assert observation_map["first_observation"] is True
    assert observation_map["contention_run_id"] == contention_run_id
    assert observation_map["semantic_class"] == "matched_positive"
    assert observation_map["selection_method"] == "known_match_v1"
    assert observation_map["selection_ordinal"] == 0
    assert observation_map["result_count"] == 1
    assert observation_map["server_duration_ns"] >= 0
    assert observation_map["page_limit"] == 100
    serialized_observation = json.dumps(observation_map)
    assert "TESTPLAN001" not in serialized_observation
    assert "strict-v3-snapshot" not in serialized_observation


@pytest.mark.asyncio
async def test_standard_ptg_route_attaches_authenticated_capacity_evidence(
    monkeypatch,
):
    """The public PTG route exposes a collector-verifiable signed response."""

    async def fake_search(_session, _args, pagination):
        return {
            "items": [{"npi": 1234567890, "tic_prices": []}],
            "pagination": {
                "total": 1,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "page": pagination.page,
            },
            "query": {"source": "ptg2"},
        }

    (
        public_key,
        release_digest,
        environment_id,
        key_id,
        run_nonce,
        contention_run_id,
    ) = _configure_pricing_capacity_evidence(monkeypatch)
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request, challenge = _capacity_evidence_pricing_request(
        run_nonce, contention_run_id
    )

    capacity_response = await list_providers_by_procedure(request)

    observation = capacity_evidence.collect_capacity_http_observation(
        capacity_response.headers,
        challenge=challenge,
        run_nonce=run_nonce,
        query_parameters=request.args,
        response_status_code=capacity_response.status,
        response_body=capacity_response.body,
        response_redirect_count=0,
        collector_received_at=datetime.now(timezone.utc),
        expected_api_evidence_key_id=key_id,
        expected_api_evidence_public_key=public_key,
        expected_release_digest=release_digest,
        expected_environment_id=environment_id,
        expected_contention_run_id=contention_run_id,
        expected_semantic_class="matched_positive",
        expected_selection_ordinal=0,
        state=capacity_evidence.CapacityEvidenceState(),
    )

    _assert_capacity_observation(observation, key_id, contention_run_id)


@pytest.mark.asyncio
async def test_public_procedure_route_cannot_escalate_to_candidate(monkeypatch):
    seen_argument_map = {}

    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
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

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [FakeResult(scalar=1)],
        args={
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "source_key": "source_a",
            "snapshot_id": "candidate-snapshot",
            "code": "70551",
        },
    )
    request.route = types.SimpleNamespace(
        name="pricing.providers.search_by_procedure"
    )
    request.headers = {
        PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot",
        "Authorization": "Bearer operator-secret",
    }

    await list_providers_by_procedure(request)

    assert PTG2_CANDIDATE_AUDIT_ACCESS_ARG not in seen_argument_map


@pytest.mark.asyncio
async def test_audit_procedure_route_attaches_exact_candidate_capability(monkeypatch):
    seen_argument_map = {}

    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
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

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "operator-secret")
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [FakeResult(scalar=1)],
        args={
            "plan_id": "12-3456789",
            "plan_market_type": "group",
            "source_key": "source_a",
            "snapshot_id": "candidate-snapshot",
            "code": "70551",
        },
    )
    request.route = types.SimpleNamespace(
        name="pricing.providers.audit_search_by_procedure"
    )
    request.headers = {
        PTG2_CANDIDATE_AUDIT_HEADER: "candidate-snapshot",
        "Authorization": "Bearer operator-secret",
    }

    await list_providers_by_procedure(request)

    access = seen_argument_map[PTG2_CANDIDATE_AUDIT_ACCESS_ARG]
    assert access.snapshot_id == "candidate-snapshot"
    assert access.source_key == "source_a"
    assert access.plan_id == "12-3456789"
    assert access.plan_market_type == "group"


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
        args={"plan_id": "TESTPLAN001", "market_type": "group", "code": "70551"},
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["query"]["source"] == "ptg2"
    assert observed_search_args_by_name["code_system"] == "CPT"


def _build_allowed_amount_fallback_response() -> dict:
    return {
        "result_state": "allowed_amounts_found",
        "pricing_scope": "plan_scoped_allowed_amounts",
        "resolved": True,
        "items": [
            {
                "npi": 1427166008,
                "network_status": (
                    "out_of_network_or_not_confirmed_in_network"
                ),
                "prices": [
                    {
                        "source": "allowed_amounts",
                        "allowed_amount": 133.0,
                    }
                ],
            }
        ],
        "pagination": {
            "total": 1,
            "limit": 25,
            "offset": 0,
            "page": 1,
        },
        "query": {
            "source": "ptg2_allowed_amounts",
            "plan_id": "TESTPLAN001",
        },
    }


def _build_current_allowed_snapshots() -> list[dict]:
    return [
        {
            "snapshot_id": "ptg2:202607:allowed-a",
            "source_key": "allowed_source_a",
        },
        {
            "snapshot_id": "ptg2:202607:allowed-b",
            "source_key": "allowed_source_b",
        },
    ]


def _canonical_release_selection(bindings):
    return plan_release_serving.PlanReleaseServingSelection(
        serving_revision_id="hpserve_" + "3" * 26,
        plan_release_id="hprelease_" + "0" * 26,
        healthporta_plan_id="hpplan_" + "1" * 26,
        plan_version_id="hpversion_" + "2" * 26,
        release_month="2026-07",
        release_status="published",
        binding_set_digest="a" * 64,
        bindings=tuple(bindings),
    )


def _build_allowed_amount_page_row(*, total: int = 12501) -> dict:
    return {
        "total": total,
        "result_network_statuses": ["in_network"],
        "result_network_semantics_values": [
            "in_network_historical_allowed_amounts"
        ],
        "source_rows_json": json.dumps(
            [
                {
                    "source_key": "allowed_source_a",
                    "snapshot_id": "ptg2:202607:allowed-a",
                    "source_file_import_id": "allowed-import-a",
                },
                {
                    "source_key": "allowed_source_b",
                    "snapshot_id": "ptg2:202607:allowed-b",
                    "source_file_import_id": "allowed-import-b",
                },
            ]
        ),
        "npi": 1427166008,
        "provider_name": "Example Medical Group",
        "state": "IL",
        "city": "Chicago",
        "zip5": "60601",
        "distance_miles": 2.5,
        "address_payload": json.dumps({"city": "Chicago", "state": "IL"}),
        "taxonomy_codes": ["207Q00000X"],
        "specialties": ["Family Medicine"],
        "classifications": ["Family Medicine"],
        "specializations": [],
        "primary_specialty": "Family Medicine",
        "primary_specialization": None,
        "allowed_amount_min": 133.0,
        "allowed_amount_max": 175.0,
        "allowed_amount_avg": 151.25,
        "billed_charge_min": 155.0,
        "billed_charge_max": 210.0,
        "evidence_count": 15000,
        "network_statuses": ["in_network"],
        "network_semantics_values": [
            "in_network_historical_allowed_amounts"
        ],
        "source_keys": ["allowed_source_a", "allowed_source_b"],
        "snapshot_ids": [
            "ptg2:202607:allowed-a",
            "ptg2:202607:allowed-b",
        ],
        "import_run_ids": [
            "ptg2:allowed-import-a",
            "ptg2:allowed-import-b",
        ],
    }


def _build_in_network_allowed_evidence(
    *,
    source_key: str = "allowed_source_a",
) -> dict:
    return {
        "npi": 1427166008,
        "source_key": source_key,
        "snapshot_id": f"ptg2:202607:{source_key}",
        "import_run_id": f"ptg2:{source_key}",
        "plan_id": "TESTPLAN001",
        "billing_code_type": "CPT",
        "billing_code": "99214",
        "allowed_amount": 133.0,
        "billed_charge": 155.0,
        "tin_type": "ein",
        "tin_value": "123456789",
        "network_status": "in_network",
        "network_semantics": "in_network_historical_allowed_amounts",
    }


@pytest.mark.asyncio
async def test_list_providers_by_procedure_falls_back_to_allowed_amounts(
    monkeypatch,
):
    strict_search = AsyncMock(return_value=None)
    allowed_search = AsyncMock(
        return_value=_build_allowed_amount_fallback_response()
    )
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", strict_search)
    monkeypatch.setattr(
        pricing_module,
        "_search_ptg_allowed_amount_evidence",
        allowed_search,
    )
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "70551",
            "zip5": "60601",
            "provider_sex_code": "u",
            "year": "2023",
            "include_allowed_amounts": "true",
        },
    )

    response = await list_providers_by_procedure(request)
    response_by_field = json.loads(response.body)

    assert response_by_field["result_state"] == "allowed_amounts_found"
    assert response_by_field["pricing_scope"] == "plan_scoped_allowed_amounts"
    assert response_by_field["items"][0]["prices"][0]["allowed_amount"] == 133.0
    assert response_by_field["query"]["year_semantics"] == (
        "ignored_for_plan_scoped_ptg_rates"
    )
    strict_search.assert_awaited_once()
    allowed_search.assert_awaited_once()
    assert strict_search.await_args.args[1]["provider_sex_code"] == "U"
    assert allowed_search.await_args.args[1]["provider_sex_code"] == "U"


def test_allowed_amount_provider_predicate_preserves_provider_sex():
    query_parameter_map = {}

    provider_predicate_sql = pricing_module._allowed_amount_provider_filter_sql(
        {"provider_sex_code": "x"},
        query_parameter_map,
    )

    assert (
        "npi_data.provider_sex_code = :allowed_provider_sex_code"
        in provider_predicate_sql
    )
    assert query_parameter_map["allowed_provider_sex_code"] == "X"


@pytest.mark.asyncio
async def test_missing_plan_snapshots_return_empty_200_after_default_allowed_fallback(
    monkeypatch,
):
    strict_search = AsyncMock(return_value=None)
    allowed_search = AsyncMock(return_value=None)
    monkeypatch.setattr(
        pricing_module,
        "search_current_ptg2_index",
        strict_search,
    )
    monkeypatch.setattr(
        pricing_module,
        "_search_ptg_allowed_amount_evidence",
        allowed_search,
    )

    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "99203",
        },
    )
    response = await list_providers_by_procedure(request)
    response_by_field = json.loads(response.body)

    assert response.status == 200
    assert response_by_field["result_state"] == "no_snapshot_for_plan"
    assert response_by_field["pricing_scope"] == "plan_scoped_ptg"
    assert response_by_field["resolved"] is False
    assert response_by_field["reason"] == (
        "no published serving snapshot for this plan_id + market_type"
    )
    assert response_by_field["items"] == []
    strict_search.assert_awaited_once()
    allowed_search.assert_awaited_once()
    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_plan_scoped_ptg_miss_does_not_query_allowed_amounts_when_false(
    monkeypatch,
):
    strict_search = AsyncMock(return_value=None)
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", strict_search)
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "99203",
            "include_allowed_amounts": "false",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["items"] == []
    assert pricing_response["result_state"] == "no_snapshot_for_plan"
    assert pricing_response["query"]["source"] == "ptg2"
    strict_search.assert_awaited_once()
    assert request.ctx.sa_session.executions == []


@pytest.mark.asyncio
async def test_negotiated_ptg_result_does_not_query_allowed_amounts(monkeypatch):
    strict_response_by_field = {
        "items": [{"npi": 1427166008, "prices": [{"negotiated_rate": 98.0}]}],
        "pagination": {"total": 1, "limit": 25, "offset": 0, "page": 1},
        "query": {"source": "ptg2"},
    }
    strict_search = AsyncMock(return_value=strict_response_by_field)
    allowed_search = AsyncMock()
    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", strict_search)
    monkeypatch.setattr(
        pricing_module,
        "_search_ptg_allowed_amount_evidence",
        allowed_search,
    )
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "99203",
        },
    )

    response = await list_providers_by_procedure(request)
    response_by_field = json.loads(response.body)

    assert response_by_field["items"] == strict_response_by_field["items"]
    strict_search.assert_awaited_once()
    allowed_search.assert_not_awaited()


@pytest.mark.asyncio
async def test_allowed_amount_snapshot_lookup_uses_allowed_only_current_pointers(
):
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "source_key": "allowed_source_a",
                        "snapshot_id": "ptg2:202607:allowed-a",
                    },
                    {
                        "source_key": "allowed_source_b",
                        "snapshot_id": "ptg2:202607:allowed-b",
                    },
                ]
            )
        ]
    )

    current_snapshots = (
        await pricing_module._current_allowed_amount_snapshots_for_plan(
            session,
            {"market_type": "group"},
            plan_id="TESTPLAN001",
        )
    )

    assert current_snapshots == _build_current_allowed_snapshots()
    lookup_sql = str(session.executions[0][0][0])
    lookup_parameters = session.executions[0][0][1]
    assert "ptg2_current_source_snapshot allowed_pointer" in lookup_sql
    assert "ptg2_allowed_amount_plan plan_coverage" in lookup_sql
    assert "ptg2_current_plan_source" not in lookup_sql
    assert "serving_index" not in lookup_sql
    assert "json_typeof(allowed_index) = 'object'" in lookup_sql
    assert "jsonb_typeof(allowed_index)" not in lookup_sql
    assert "allowed_index->>'current_source_key'" in lookup_sql
    assert "allowed_index->>'arch_version' = 'postgres_binary_v3'" in lookup_sql
    assert "allowed_index->>'storage' = 'postgresql'" in lookup_sql
    assert "allowed_index->>'snapshot_scoped' = 'true'" in lookup_sql
    assert lookup_parameters["allowed_contract"] == (
        pricing_module.PTG2_ALLOWED_AMOUNT_CONTRACT
    )
    assert lookup_parameters["market_type"] == "group"
    assert lookup_parameters["plan_ids"] == ["TESTPLAN001"]


@pytest.mark.asyncio
async def test_allowed_amount_snapshot_lookup_uses_isolated_divergent_pointer(
):
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "source_key": "allowed_source_a",
                        "snapshot_id": "ptg2:202607:allowed-current",
                    }
                ]
            )
        ]
    )

    current_snapshots = (
        await pricing_module._current_allowed_amount_snapshots_for_plan(
            session,
            {
                "market_type": "group",
                "snapshot_id": "ptg2:202607:negotiated-current",
            },
            plan_id="TESTPLAN001",
        )
    )

    assert current_snapshots == [
        {
            "source_key": "allowed_source_a",
            "snapshot_id": "ptg2:202607:allowed-current",
        }
    ]
    lookup_parameters = session.executions[0][0][1]
    assert "snapshot_id" not in lookup_parameters


def _allowed_amount_sql_test_context():
    request_args_by_name = {
        "plan_id": "TESTPLAN001",
        "plan_market_type": "group",
        "code": "99214",
        "code_system": "CPT",
        "pos": "11",
        "modifier": "25,59",
        "taxonomy_codes": "207Q00000X",
        "primary_only": "true",
        "state": "IL",
        "city": "Chicago",
        "zip5": "60601",
        "lat": 41.88,
        "long": -87.63,
        "radius_miles": 10,
    }
    pagination = types.SimpleNamespace(limit=25, offset=50, page=3)
    parameter_map = pricing_module._allowed_amount_query_params(
        request_args_by_name,
        pagination,
        plan_id="TESTPLAN001",
        code="99214",
        code_system="CPT",
        npi=None,
        current_snapshots=_build_current_allowed_snapshots(),
    )

    sql_text = str(
        pricing_module._allowed_amount_page_sql(
            request_args_by_name,
            address_table="mrf.entity_address_unified",
            parameter_map=parameter_map,
        )
    )
    return sql_text, parameter_map


def test_allowed_amount_sql_filters_before_exact_count_and_pagination():
    sql_text, _parameter_map = _allowed_amount_sql_test_context()

    assert "ptg2_current_plan_source" not in sql_text
    assert "mrf.ptg2_current_source_snapshot allowed_pointer" in sql_text
    assert (
        "allowed_pointer.snapshot_id = snapshot.snapshot_id"
        in sql_text
    )
    assert (
        "allowed_pointer.source_key"
        in sql_text
    )
    assert (
        "->'allowed_amount_index'->>'current_source_key'"
        in sql_text
    )
    assert "unnest(" in sql_text
    assert "mrf.ptg2_allowed_amount_plan" in sql_text
    assert "mrf.ptg2_allowed_amount_item" in sql_text
    assert "mrf.ptg2_allowed_amount_payment" in sql_text
    assert "mrf.ptg2_allowed_amount_provider_payment" in sql_text
    assert "provider_payment.npi" in sql_text
    assert "@> ARRAY[CAST(:npi AS bigint)]" in sql_text
    assert "COUNT(DISTINCT filtered_provider.npi)" in sql_text
    assert "LIMIT :limit" in sql_text
    assert "OFFSET :offset" in sql_text
    assert "LIMIT 10000" not in sql_text
    assert "allowed_payment.service_code" in sql_text
    assert "allowed_payment.billing_code_modifier" in sql_text
    assert "allowed_amount_specialty_nt" in sql_text
    assert "healthcare_provider_primary_taxonomy_switch" in sql_text
    assert "mrf.entity_address_unified" in sql_text
    assert "provider_page.npi ASC" in sql_text
    assert "__LOCATION_" not in sql_text
    assert "__TAXONOMY_" not in sql_text


def test_allowed_amount_sql_uses_stable_current_source_parameters():
    _sql_text, parameter_map = _allowed_amount_sql_test_context()

    assert parameter_map["snapshot_ids"] == [
        "ptg2:202607:allowed-a",
        "ptg2:202607:allowed-b",
    ]
    assert parameter_map["service_codes"] == ["11"]
    assert parameter_map["modifier_codes"] == ["25", "59"]
    assert parameter_map["limit"] == 25
    assert parameter_map["offset"] == 50


def test_allowed_amount_data_queries_rebind_snapshots_to_live_pointer():
    page_sql, _parameter_map = _allowed_amount_sql_test_context()
    detail_sql = str(pricing_module._allowed_amount_detail_sql())

    for query_sql in (page_sql, detail_sql):
        assert "ptg2_current_source_snapshot allowed_pointer" in query_sql
        assert (
            "allowed_pointer.snapshot_id = snapshot.snapshot_id"
            in query_sql
        )
        assert (
            "snapshot.manifest"
            in query_sql
        )
        assert (
            "->'allowed_amount_index'->>'current_source_key'"
            in query_sql
        )
        assert (
            "= LOWER(current_snapshot.source_key)"
            in query_sql
        )


def test_allowed_amount_sql_uses_shared_file_plan_coverage_for_each_plan():
    request_args_by_name = {
        "plan_market_type": "group",
        "code": "99214",
        "code_system": "CPT",
    }
    pagination = types.SimpleNamespace(limit=25, offset=0, page=1)
    sql_texts = []
    plan_parameter_sets = []

    for plan_id in ("PLAN-A", "PLAN-B"):
        parameter_map = pricing_module._allowed_amount_query_params(
            request_args_by_name,
            pagination,
            plan_id=plan_id,
            code="99214",
            code_system="CPT",
            npi=None,
            current_snapshots=_build_current_allowed_snapshots(),
        )
        sql_texts.append(
            str(
                pricing_module._allowed_amount_page_sql(
                    request_args_by_name,
                    address_table=None,
                    parameter_map=parameter_map,
                )
            )
        )
        plan_parameter_sets.append(parameter_map["plan_ids"])

    assert sql_texts[0] == sql_texts[1]
    assert plan_parameter_sets == [
        ["PLAN-A", "PLAN-A"],
        ["PLAN-B", "PLAN-B"],
    ]
    assert "allowed_item.file_id = plan_coverage.file_id" in sql_texts[0]
    assert "plan_coverage.plan_id" in sql_texts[0]
    assert "allowed_item.plan_id" not in sql_texts[0]
    assert "allowed_item.plan_market_type" not in sql_texts[0]


def _canonical_allowed_binding_snapshots():
    return [
        {
            "snapshot_id": "ptg2:allowed:group",
            "source_key": "allowed-group",
            "plan_id": "38-2418014",
            "plan_market_type": "group",
        },
        {
            "snapshot_id": "ptg2:allowed:individual",
            "source_key": "allowed-individual",
            "plan_id": "PLAN-B",
            "plan_market_type": "individual",
        },
    ]


def test_canonical_allowed_amount_parameters_preserve_binding_local_tuples():
    """Frozen physical coordinates must remain aligned by tuple position."""

    parameter_map = pricing_module._allowed_amount_query_params(
        {
            "plan_release_id": "hprelease_" + "0" * 26,
            "code": "99214",
            "code_system": "CPT",
        },
        types.SimpleNamespace(limit=25, offset=0, page=1),
        plan_id="38-2418014",
        code="99214",
        code_system="CPT",
        npi=None,
        current_snapshots=_canonical_allowed_binding_snapshots(),
    )

    assert list(
        zip(
            parameter_map["snapshot_ids"],
            parameter_map["source_keys"],
            parameter_map["plan_ids"],
            parameter_map["plan_market_types"],
        )
    ) == [
        (
            "ptg2:allowed:group",
            "allowed-group",
            "38-2418014",
            "group",
        ),
        (
            "ptg2:allowed:group",
            "allowed-group",
            "382418014",
            "group",
        ),
        (
            "ptg2:allowed:individual",
            "allowed-individual",
            "PLAN-B",
            "individual",
        ),
    ]
    query_sql = str(
        pricing_module._allowed_amount_page_sql(
            {},
            address_table=None,
            parameter_map=parameter_map,
        )
    )
    assert "plan_coverage.plan_id = current_snapshot.plan_id" in query_sql
    assert (
        "plan_coverage.plan_market_type, '') "
        "= current_snapshot.plan_market_type"
    ) in " ".join(query_sql.split())


def _assert_allowed_amount_multi_source_response(response_by_field):
    assert response_by_field is not None
    assert response_by_field["result_state"] == "allowed_amounts_found"
    assert response_by_field["pricing_scope"] == "plan_scoped_allowed_amounts"
    assert response_by_field["query"]["source"] == "ptg2_allowed_amounts"
    assert response_by_field["query"]["snapshot_id"] is None
    assert response_by_field["query"]["snapshot_ids"] == [
        "ptg2:202607:allowed-a",
        "ptg2:202607:allowed-b",
    ]
    assert response_by_field["pagination"] == {
        "total": 12501,
        "limit": 25,
        "offset": 10000,
        "page": 401,
        "has_more": True,
        "total_is_exact": True,
    }
    assert {
        source_by_field["source_key"]
        for source_by_field in response_by_field["sources"]
    } == {"allowed_source_a", "allowed_source_b"}
    assert all(
        source_by_field["network_status"] == "in_network"
        for source_by_field in response_by_field["sources"]
    )
    assert response_by_field["warnings"][0]["code"] == (
        "allowed_amounts_not_negotiated_rates"
    )
    provider_by_field = response_by_field["items"][0]
    assert provider_by_field["network_status"] == "in_network"
    assert provider_by_field["prices"][0]["allowed_amount"] == 133.0
    assert provider_by_field["evidence_count"] == 15000
    assert provider_by_field["source_keys"] == [
        "allowed_source_a",
        "allowed_source_b",
    ]


def _assert_allowed_amount_sql_executions(session):
    page_sql = str(session.executions[0][0][0])
    detail_sql = str(session.executions[1][0][0])
    assert "LIMIT 10000" not in page_sql
    assert "ROW_NUMBER() OVER" in detail_sql
    assert session.executions[0][0][1]["snapshot_ids"] == [
        "ptg2:202607:allowed-a",
        "ptg2:202607:allowed-b",
    ]


def _assert_allowed_amount_location_payload_suppressed(provider_by_field):
    """Assert inferred location data and metadata are absent at every depth."""

    suppressed_field_names = {
        "address",
        "address_network_binding",
        "city",
        "distance_bucket",
        "distance_miles",
        "network_bound_address",
        "requires_location_confirmation",
        "state",
        "zip5",
    }
    pending_payloads = [provider_by_field]
    while pending_payloads:
        nested_payload = pending_payloads.pop()
        if isinstance(nested_payload, dict):
            assert suppressed_field_names.isdisjoint(nested_payload)
            confidence_payload = nested_payload.get("confidence")
            if isinstance(confidence_payload, dict):
                assert "location" not in confidence_payload
            pending_payloads.extend(nested_payload.values())
        elif isinstance(nested_payload, list):
            pending_payloads.extend(nested_payload)
    assert provider_by_field["confidence"] == {
        "network": "allowed_amounts_in_network"
    }


@pytest.mark.asyncio
async def test_allowed_amount_search_combines_sources_and_keeps_exact_large_total(
    monkeypatch,
):
    monkeypatch.setattr(
        pricing_module,
        "_current_allowed_amount_snapshots_for_plan",
        AsyncMock(return_value=_build_current_allowed_snapshots()),
    )
    monkeypatch.setattr(
        pricing_module,
        "_allowed_amount_address_table",
        AsyncMock(return_value="mrf.npi_address"),
    )
    session = FakeSession(
        [
            FakeResult([_build_allowed_amount_page_row()]),
            FakeResult(
                [
                    _build_in_network_allowed_evidence(
                        source_key="allowed_source_a"
                    ),
                    _build_in_network_allowed_evidence(
                        source_key="allowed_source_b"
                    ),
                ]
            ),
        ]
    )
    response_by_field = await pricing_module._search_ptg_allowed_amount_evidence(
        session,
        {
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "code": "99214",
            "code_system": "CPT",
            "pos": "11",
            "modifier": "25",
            "taxonomy_codes": "207Q00000X",
            "primary_only": "true",
        },
        types.SimpleNamespace(limit=25, offset=10000, page=401),
    )

    _assert_allowed_amount_multi_source_response(response_by_field)
    _assert_allowed_amount_sql_executions(session)


@pytest.mark.asyncio
async def test_allowed_amount_search_filters_with_location_but_omits_unverified_address(
    monkeypatch,
):
    """Location filters remain active while inferred location output is omitted."""

    monkeypatch.setattr(
        pricing_module,
        "_current_allowed_amount_snapshots_for_plan",
        AsyncMock(return_value=_build_current_allowed_snapshots()),
    )
    monkeypatch.setattr(
        pricing_module,
        "_allowed_amount_address_table",
        AsyncMock(return_value="mrf.npi_address"),
    )
    session = FakeSession(
        [
            FakeResult([_build_allowed_amount_page_row(total=1)]),
            FakeResult([_build_in_network_allowed_evidence()]),
        ]
    )

    response_by_field = await pricing_module._search_ptg_allowed_amount_evidence(
        session,
        {
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "code": "99214",
            "code_system": "CPT",
            "city": "Chicago",
            "state": "IL",
            "include_unverified_addresses": "false",
        },
        types.SimpleNamespace(limit=25, offset=0, page=1),
    )

    page_sql = str(session.executions[0][0][0])
    assert "allowed_location.npi IS NOT NULL" in page_sql
    assert "UPPER(COALESCE(addr.city_name, '')) = :allowed_city" in page_sql
    provider_by_field = response_by_field["items"][0]
    _assert_allowed_amount_location_payload_suppressed(provider_by_field)


@pytest.mark.asyncio
async def test_canonical_allowed_amount_zero_rows_is_resolved_no_match(
    monkeypatch,
):
    """An exact allowed-only release resolves even when it has zero rows."""

    selection = _allowed_only_release_selection()
    monkeypatch.setattr(
        pricing_module,
        "resolve_plan_release_serving",
        AsyncMock(return_value=selection),
    )
    monkeypatch.setattr(
        pricing_module,
        "_allowed_amount_address_table",
        AsyncMock(return_value="mrf.npi_address"),
    )
    session = FakeSession(
        [FakeResult(rows=[_build_allowed_amount_page_row(total=0)])]
    )

    response_by_field = (
        await pricing_module._search_ptg_allowed_amount_evidence(
            session,
            {
                "plan_release_id": selection.plan_release_id,
                "code": "99214",
                "code_system": "CPT",
            },
            types.SimpleNamespace(limit=25, offset=0, page=1),
        )
    )

    _assert_allowed_release_no_match(response_by_field, selection, session)


def _allowed_only_release_selection():
    return _canonical_release_selection(
        [
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=0,
                snapshot_id="ptg2:allowed:release",
                source_key="allowed-release",
                plan_id="38-2418014",
                plan_market_type="group",
                role="allowed_amounts",
                required=True,
            )
        ]
    )


def _assert_allowed_release_no_match(response_by_field, selection, session):
    assert response_by_field["resolved"] is True
    assert response_by_field["result_state"] == "no_matching_rates"
    assert response_by_field["items"] == []
    assert response_by_field["query"]["status"] == "no_match"
    assert response_by_field["plan_release_id"] == selection.plan_release_id
    assert response_by_field["query"]["healthporta_plan_id"] == selection.healthporta_plan_id
    assert response_by_field["query"]["bindings"] == [
        {
            "source_key": "allowed-release",
            "snapshot_id": "ptg2:allowed:release",
            "plan_id": "38-2418014",
            "plan_market_type": "group",
        }
    ]
    parameters_by_name = session.executions[0][0][1]
    assert list(
        zip(
            parameters_by_name["snapshot_ids"],
            parameters_by_name["source_keys"],
            parameters_by_name["plan_ids"],
            parameters_by_name["plan_market_types"],
        )
    ) == [
        (
            "ptg2:allowed:release",
            "allowed-release",
            "38-2418014",
            "group",
        ),
        (
            "ptg2:allowed:release",
            "allowed-release",
            "382418014",
            "group",
        ),
    ]


@pytest.mark.asyncio
async def test_allowed_amount_search_preserves_no_match_for_negotiated_rate_filter(
    monkeypatch,
):
    snapshot_lookup = AsyncMock(return_value=_build_current_allowed_snapshots())
    monkeypatch.setattr(
        pricing_module,
        "_current_allowed_amount_snapshots_for_plan",
        snapshot_lookup,
    )

    response_by_field = await pricing_module._search_ptg_allowed_amount_evidence(
        FakeSession(),
        {
            "plan_id": "TESTPLAN001",
            "code": "99214",
            "negotiated_rate": "133.00",
            "negotiated_rate_tolerance": "0.01",
        },
        types.SimpleNamespace(limit=10, offset=0, page=1),
    )

    assert response_by_field is None
    snapshot_lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_providers_by_procedure_rejects_broad_group_plan_office_visit(monkeypatch):
    async def fail_search(*_args, **_kwargs):
        raise AssertionError("broad provider-directory request should fail before PTG search")

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fail_search)
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
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
async def test_canonical_plan_uses_binding_market_for_broad_expansion_guard(
    monkeypatch,
):
    selection = _canonical_release_selection(
        [
            plan_release_serving.PlanReleaseSnapshotBinding(
                binding_ordinal=0,
                snapshot_id="ptg2:release:group",
                source_key="aetna-group",
                plan_id="38-2418014",
                plan_market_type="group",
                role="in_network",
                required=True,
            )
        ]
    )
    monkeypatch.setattr(
        pricing_module,
        "resolve_plan_release_serving",
        AsyncMock(return_value=selection),
    )

    async def fail_search(*_args, **_kwargs):
        raise AssertionError("resolved group guard must run before PTG search")

    monkeypatch.setattr(
        pricing_module,
        "search_current_ptg2_index",
        fail_search,
    )
    request = make_request(
        [],
        args={
            "plan_release_id": selection.plan_release_id,
            "market_type": "individual",
            "code": "99213",
            "code_system": "CPT",
            "state": "IL",
            "city": "Chicago",
            "include_providers": "true",
        },
    )

    with pytest.raises(
        pricing_module.InvalidUsage,
        match="provider-directory request",
    ):
        await list_providers_by_procedure(request)


@pytest.mark.asyncio
async def test_list_providers_by_procedure_allows_taxonomy_scoped_group_plan_office_visit(monkeypatch):
    observed_search_argument_map = {}

    async def fake_search(_session, args, pagination):
        observed_search_argument_map.update(args)
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
            "plan_id": "TESTPLAN001",
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
    pricing_response = json.loads(response.body)

    assert pricing_response["query"]["source"] == "ptg2"
    assert observed_search_argument_map["classification"] == "Family Medicine"
    assert observed_search_argument_map["include_providers"] == "true"


@pytest.mark.asyncio
async def test_procedure_provider_search_uses_default_zip_radius(monkeypatch):
    seen_argument_map = {}

    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
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
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "29888",
            "zip5": "62401",
            "include_providers": "true",
            "npi": "['']",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["query"]["zip_radius_miles"] == 10.0
    assert seen_argument_map["zip5"] == "62401"
    assert seen_argument_map["zip_radius_miles"] == 10.0
    assert seen_argument_map["lat"] == 39.1200
    assert seen_argument_map["long"] == -88.5434
    assert seen_argument_map["radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_procedure_provider_search_allows_100_mile_radius(monkeypatch):
    seen_argument_map = {}

    async def fake_search(_session, args, pagination):
        seen_argument_map.update(args)
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
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "29888",
            "zip5": "62401",
            "zip_radius_miles": "100",
            "include_providers": "true",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["query"]["zip_radius_miles"] == 100.0
    assert seen_argument_map["zip_radius_miles"] == 100.0
    assert seen_argument_map["radius_miles"] == 100.0


@pytest.mark.asyncio
async def test_list_providers_by_procedure_empty_loaded_ptg_source_reports_no_match(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "source_key": "example_source_a",
            "snapshot_id": "ptg2:202606:fixture_a",
            "code": "29888",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["items"] == []
    assert pricing_response["pagination"]["total"] == 0
    assert pricing_response["result_state"] == "no_matching_rates"
    assert pricing_response["pricing_scope"] == "plan_scoped_ptg"
    assert pricing_response["query"]["source"] == "ptg2"
    assert pricing_response["query"]["status"] == "no_match"


@pytest.mark.asyncio
async def test_list_providers_by_procedure_no_ptg_route_reports_reason(monkeypatch):
    async def fake_search(_session, _args, _pagination):
        return None

    monkeypatch.setattr(pricing_module, "search_current_ptg2_index", fake_search)
    request = make_request(
        [],
        args={
            "plan_id": "TESTPLAN001",
            "plan_id_type": "EIN",
            "market_type": "group",
            "code": "29888",
            "year": "2023",
        },
    )

    response = await list_providers_by_procedure(request)
    pricing_response = json.loads(response.body)

    assert pricing_response["resolved"] is False
    assert pricing_response["result_state"] == "no_snapshot_for_plan"
    assert pricing_response["pricing_scope"] == "plan_scoped_ptg"
    assert pricing_response["reason"] == "no published serving snapshot for this plan_id + market_type"
    assert pricing_response["pagination"]["total"] == 0
    assert pricing_response["query"]["status"] == "no_route"
    assert pricing_response["query"]["plan_id_type"] == "ein"
    assert pricing_response["query"]["ignored_params"] == ["year"]
    assert pricing_response["query"]["year_semantics"] == "ignored_for_plan_scoped_ptg_rates"


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["items"][0]["cost_index"] == 250.0
    assert pricing_response["items"][0]["avg_submitted_charge"] == 300.0
    assert pricing_response["items"][0]["avg_allowed_amount"] == 250.0
    assert "charge_per_service_avg" not in pricing_response["items"][0]
    assert "medicare_avg_submitted_charge_per_service" not in pricing_response["items"][0]
    assert "medicare_avg_allowed_amount_per_service" not in pricing_response["items"][0]
    assert "medicare_average_price_per_service" not in pricing_response["items"][0]
    assert "average_price" not in pricing_response["items"][0]
    assert pricing_response["query"]["order_by"] == "cost_index"
    assert pricing_response["query"]["code"] == "123"
    assert pricing_response["query"]["zip5"] == "20814"


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
    pricing_response = json.loads(response.body)
    pricing_item = pricing_response["items"][0]
    assert pricing_item["zip5"] == "20816"
    assert pricing_item["distance_miles"] == 6.5
    assert pricing_item["distance_bucket"] == "within_10mi"
    assert pricing_item["anchor_zip5"] == "20814"
    assert pricing_response["query"]["zip_radius_miles"] == 30.0
    assert pricing_response["query"]["zip_candidate_count"] == 2


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
    """Enrich procedure provider results with peer cost statistics."""
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
    pricing_response = json.loads(response.body)
    pricing_item = pricing_response["items"][0]

    assert pricing_item["cost_index"] == "$$$"
    assert pricing_item["estimated_cost_level"] == "$$$"
    assert pricing_item["avg_allowed_amount"] == 250.0


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
    pricing_response = json.loads(response.body)
    pricing_item = pricing_response["items"][0]
    assert pricing_item["avg_submitted_charge"] == 300.0
    assert pricing_item["avg_allowed_amount"] == 250.0
    assert pricing_item["charge_per_service_avg"] == 300.0
    assert pricing_item["medicare_avg_submitted_charge_per_service"] == 300.0
    assert pricing_item["medicare_avg_allowed_amount_per_service"] == 250.0
    assert pricing_item["medicare_average_price_per_service"] == 250.0
    assert pricing_item["average_price"] == 250.0


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
    pricing_response = json.loads(response.body)
    assert pricing_response["pagination"]["total"] == 1
    assert pricing_response["items"][0]["npi"] == 1003000126
    assert pricing_response["items"][0]["total_prescriptions"] == 60.0
    assert pricing_response["query"]["q"] == "atorvastatin"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_success():
    """Return an estimated cost level from available benchmark data."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["npi"] == 1003000126
    assert pricing_response["provider"]["procedure_code"] == 123
    assert pricing_response["peer_group"]["provider_count"] == 250
    assert pricing_response["peer_group"]["typical_range"]["p10"] == 150.0
    assert pricing_response["peer_group"]["typical_range"]["p90"] == 900.0
    assert pricing_response["peer_group"]["cohort_type"] == "national"
    assert pricing_response["peer_group"]["specialty_scope"] == "specialty_specific"
    assert pricing_response["estimated_cost_level"] == "$$$"
    assert pricing_response["confidence"] == "medium"
    assert pricing_response["procedure"]["code_system"] == "HP_PROCEDURE_CODE"
    assert pricing_response["procedure"]["code"] == "123"
    assert pricing_response["procedure"]["name"] == "Magnetic resonance imaging, brain"
    assert pricing_response["procedure"]["reported_code"] == "70553"
    assert pricing_response["procedure"]["reported_code_system"] == "CPT"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_zip_ring_fallback():
    """Use the ZIP-ring fallback when local benchmark data is sparse."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["peer_group"]["geography_scope"] == "zip5"
    assert pricing_response["peer_group"]["geography_value"] == "20816"
    assert pricing_response["peer_group"]["cohort_type"] == "zip"
    assert pricing_response["peer_group"]["specialty_scope"] == "specialty_specific"
    assert pricing_response["peer_group"]["distance_miles"] == 6.5
    assert pricing_response["peer_group"]["distance_bucket"] == "within_10mi"
    assert pricing_response["query"]["zip_radius_miles"] == 30.0


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_near_dynamic():
    """Use nearby dynamic peers for the estimated cost level."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["peer_group"]["geography_scope"] == "zip_radius"
    assert pricing_response["peer_group"]["geography_value"] == "20814|30mi"
    assert pricing_response["peer_group"]["provider_count"] == 12
    assert pricing_response["peer_group"]["cohort_type"] == "zip_radius"
    assert pricing_response["peer_group"]["specialty_scope"] == "specialty_specific"
    assert pricing_response["peer_group"]["cohort_strategy_used"] == "near_dynamic"
    assert pricing_response["query"]["cohort_strategy"] == "near_dynamic"
    assert pricing_response["query"]["cohort_strategy_used"] == "near_dynamic"


@pytest.mark.asyncio
async def test_get_provider_procedure_estimated_cost_level_near_dynamic_fallbacks_to_precomputed():
    """Fall back to precomputed peers when dynamic peers are insufficient."""
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
    pricing_response = json.loads(response.body)
    assert pricing_response["peer_group"]["geography_scope"] == "zip5"
    assert pricing_response["peer_group"]["geography_value"] == "20816"
    assert pricing_response["peer_group"]["cohort_type"] == "zip"
    assert pricing_response["peer_group"]["specialty_scope"] == "specialty_specific"
    assert pricing_response["peer_group"]["cohort_strategy_used"] == "precomputed"
    assert pricing_response["query"]["cohort_strategy"] == "near_dynamic"
    assert pricing_response["query"]["cohort_strategy_used"] == "precomputed"


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
    pricing_response = json.loads(response.body)
    assert pricing_response["query"]["input_code"]["code_system"] == "CPT"
    assert pricing_response["benchmarks"]["national"]["total_services"] == 1000.0
    assert pricing_response["benchmarks"]["state"]["geography_value"] == "MD"
    assert pricing_response["benchmarks"]["state"]["avg_submitted_charge"] == 460.0


@pytest.mark.asyncio
async def test_pricing_statistics_success():
    request = make_request([
        FakeResult(scalar=321000),
        FakeResult(scalar=845000),
        FakeResult(scalar=6123),
        FakeResult(scalar=27890),
    ])

    response = await pricing_statistics(request)
    pricing_response = json.loads(response.body)

    assert pricing_response == {
        "medicare_individual_providers": 321000,
        "providers_with_procedure_history": 845000,
        "procedure_codes_tracked": 6123,
        "procedure_zip_codes": 27890,
    }
