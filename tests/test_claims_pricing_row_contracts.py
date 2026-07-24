# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import csv
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

claims_pricing = importlib.import_module("process.claims_pricing")

PROVIDER_SERVICE_FIELDS = (
    "Rndrng_NPI",
    "HCPCS_Cd",
    "HCPCS_Desc",
    "Rndrng_Prvdr_City",
    "Rndrng_Prvdr_State_Abrvtn",
    "Rndrng_Prvdr_Zip5",
    "Rndrng_Prvdr_State_FIPS",
    "Rndrng_Prvdr_Cntry",
    "Place_Of_Srvc",
    "Avg_Mdcr_Alowd_Amt",
    "Avg_Sbmtd_Chrg",
    "Tot_Srvcs",
)
PROVIDER_SERVICE_BASE_BY_FIELD = {
    "Rndrng_NPI": "1000000001",
    "HCPCS_Cd": "99213",
    "HCPCS_Desc": "Office visit",
    "Rndrng_Prvdr_City": "Testville",
    "Rndrng_Prvdr_State_Abrvtn": "CA",
    "Rndrng_Prvdr_Zip5": "90210",
    "Rndrng_Prvdr_State_FIPS": "06",
    "Rndrng_Prvdr_Cntry": "US",
    "Place_Of_Srvc": "11",
    "Avg_Mdcr_Alowd_Amt": "10",
    "Avg_Sbmtd_Chrg": "20",
}
GEO_SERVICE_FIELDS = (
    "HCPCS_Cd",
    "HCPCS_Desc",
    "Rndrng_Prvdr_Geo_Lvl",
    "Rndrng_Prvdr_State_Abrvtn",
    "Tot_Srvcs",
    "Avg_Mdcr_Alowd_Amt",
)
GEO_SERVICE_ROWS_BY_FIELD = [
    {
        "HCPCS_Cd": "99213",
        "HCPCS_Desc": "National",
        "Rndrng_Prvdr_Geo_Lvl": "national",
        "Tot_Srvcs": "2",
        "Avg_Mdcr_Alowd_Amt": "10",
    },
    {
        "HCPCS_Cd": "99213",
        "HCPCS_Desc": "State low",
        "Rndrng_Prvdr_Geo_Lvl": "state",
        "Rndrng_Prvdr_State_Abrvtn": "CA",
        "Tot_Srvcs": "3",
        "Avg_Mdcr_Alowd_Amt": "11",
    },
    {
        "HCPCS_Cd": "99213",
        "HCPCS_Desc": "State high",
        "Rndrng_Prvdr_Geo_Lvl": "state",
        "Rndrng_Prvdr_State_Abrvtn": "CA",
        "Tot_Srvcs": "5",
        "Avg_Mdcr_Alowd_Amt": "12",
    },
]


def _write_csv(
    csv_path: Path,
    field_names: list[str] | tuple[str, ...],
    rows_by_field: list[dict],
) -> None:
    with csv_path.open("w", encoding="utf-8", newline="") as csv_handle:
        csv_writer = csv.DictWriter(csv_handle, fieldnames=field_names)
        csv_writer.writeheader()
        csv_writer.writerows(rows_by_field)


def test_provider_row_contract_distinguishes_invalid_reasons():
    missing_npi, has_invalid_state = claims_pricing._provider_row_from_source({}, 2023)
    assert missing_npi is None
    assert has_invalid_state is False
    invalid_state, has_invalid_state = claims_pricing._provider_row_from_source(
        {"Rndrng_NPI": "1000000001", "Rndrng_Prvdr_State_Abrvtn": "invalid"},
        2023,
    )
    assert invalid_state is None
    assert has_invalid_state is True


def test_provider_row_contract_normalizes_all_public_fields():
    source_row_by_field = {
        "Rndrng_NPI": "1000000001",
        "Rndrng_Prvdr_First_Name": " A ",
        "Rndrng_Prvdr_Last_Org_Name": " Example ",
        "Rndrng_Prvdr_Crdntls": " MD ",
        "Rndrng_Prvdr_Type": " Clinic ",
        "Rndrng_Prvdr_City": " Testville ",
        "Rndrng_Prvdr_State_Abrvtn": "ca",
        "Rndrng_Prvdr_Zip5": "90210-0001",
        "Rndrng_Prvdr_Cntry": " US ",
        "Tot_Srvcs": "3",
        "Tot_HCPCS_Cds": "2",
        "Tot_Mdcr_Alowd_Amt": "30.5",
        "Tot_Sbmtd_Chrg": "45",
        "Tot_Benes": "1",
    }
    provider_row_by_field, has_invalid_state = claims_pricing._provider_row_from_source(
        source_row_by_field,
        2023,
    )
    assert has_invalid_state is False
    assert provider_row_by_field["provider_name"] == "Example, A"
    assert provider_row_by_field["state"] == "CA"
    assert provider_row_by_field["zip5"] == "90210"
    assert provider_row_by_field["total_allowed_amount"] == 30.5


@pytest.mark.asyncio
async def test_provider_loader_batches_and_deduplicates(monkeypatch, tmp_path):
    csv_path = tmp_path / "providers.csv"
    field_names = ["Rndrng_NPI", "Rndrng_Prvdr_State_Abrvtn", "Tot_Srvcs"]
    _write_csv(
        csv_path,
        field_names,
        [
            {"Rndrng_NPI": "bad", "Rndrng_Prvdr_State_Abrvtn": "CA", "Tot_Srvcs": "1"},
            {"Rndrng_NPI": "1000000001", "Rndrng_Prvdr_State_Abrvtn": "CA", "Tot_Srvcs": "1"},
            {"Rndrng_NPI": "1000000001", "Rndrng_Prvdr_State_Abrvtn": "CA", "Tot_Srvcs": "2"},
            {"Rndrng_NPI": "1000000002", "Rndrng_Prvdr_State_Abrvtn": "invalid", "Tot_Srvcs": "1"},
        ],
    )
    pushed_batches = []

    async def capture_push(provider_rows, provider_cls, **_options):
        pushed_batches.append((list(provider_rows), provider_cls))

    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", capture_push)
    monkeypatch.setattr(claims_pricing, "IMPORT_BATCH_SIZE", 2)
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    provider_cls = SimpleNamespace(__tablename__="provider_stage")
    await claims_pricing._load_provider_rows(str(csv_path), provider_cls, 2023, test_mode=False)
    assert len(pushed_batches) == 1
    assert pushed_batches[0][0][0]["total_services"] == 2.0
    assert pushed_batches[0][1] is provider_cls


@pytest.mark.asyncio
async def test_provider_loader_warns_when_no_rows(monkeypatch, tmp_path):
    csv_path = tmp_path / "providers.csv"
    _write_csv(csv_path, ["Rndrng_NPI"], [{"Rndrng_NPI": "bad"}])
    safe_print = Mock()
    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    monkeypatch.setattr(claims_pricing, "_safe_print", safe_print)
    await claims_pricing._load_provider_rows(str(csv_path), object(), 2023, test_mode=False)
    assert "accepted 0 rows" in safe_print.call_args.args[0]


@pytest.mark.asyncio
async def test_provider_loader_test_mode_samples_and_limits(monkeypatch, tmp_path):
    csv_path = tmp_path / "providers.csv"
    _write_csv(
        csv_path,
        ["Rndrng_NPI"],
        [{"Rndrng_NPI": f"1000000{number:03d}"} for number in range(1, 4)],
    )
    push_objects = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", push_objects)
    monkeypatch.setattr(claims_pricing, "_row_allowed_for_test", lambda row_number: row_number > 1)
    monkeypatch.setattr(claims_pricing, "TEST_PROVIDER_ROW_LIMIT", 1)
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    await claims_pricing._load_provider_rows(str(csv_path), object(), 2023, test_mode=True)
    assert push_objects.await_args.args[0][0]["npi"] == 1000000002


def test_weighted_total_handles_missing_inputs():
    assert claims_pricing._weighted_total(None, 3.0) is None
    assert claims_pricing._weighted_total(4.0, None) == 4.0
    assert claims_pricing._weighted_total(4.0, 3.0) == 12.0


def test_provider_service_candidate_rejects_invalid_rows():
    missing_identity, has_invalid_state = claims_pricing._provider_service_candidate({}, 2023)
    assert missing_identity is None
    assert has_invalid_state is False
    invalid_code, _ = claims_pricing._provider_service_candidate(
        {"Rndrng_NPI": "1000000001", "HCPCS_Cd": "invalid"},
        2023,
    )
    assert invalid_code is None
    invalid_state, has_invalid_state = claims_pricing._provider_service_candidate(
        {
            "Rndrng_NPI": "1000000001",
            "HCPCS_Cd": "99213",
            "Rndrng_Prvdr_State_Abrvtn": "invalid",
        },
        2023,
    )
    assert invalid_state is None
    assert has_invalid_state is True


def test_provider_service_candidate_builds_location_contract():
    candidate, has_invalid_state = claims_pricing._provider_service_candidate(
        {
            "Rndrng_NPI": "1000000001",
            "HCPCS_Cd": "99213",
            "HCPCS_Desc": "Office visit",
            "Rndrng_Prvdr_City": "Testville",
            "Rndrng_Prvdr_State_Abrvtn": "CA",
            "Rndrng_Prvdr_Zip5": "90210",
            "Rndrng_Prvdr_State_FIPS": "06",
            "Rndrng_Prvdr_Cntry": "",
            "Place_Of_Srvc": "11",
            "Avg_Mdcr_Alowd_Amt": "10",
            "Avg_Sbmtd_Chrg": "20",
            "Tot_Srvcs": "3",
        },
        2023,
    )
    assert has_invalid_state is False
    assert candidate.total_allowed_amount == 30.0
    assert candidate.total_submitted_charges == 60.0
    assert candidate.state_fips == "6"
    assert claims_pricing._provider_location_fields(candidate)["country"] == "US"


def test_procedure_merge_sums_amounts_and_fills_labels():
    accumulated_by_field = {
        "total_services": None,
        "total_beneficiary_day_services": 1.0,
        "total_submitted_charges": None,
        "total_allowed_amount": 3.0,
        "total_beneficiaries": None,
        "service_description": None,
        "reported_code": "99213",
    }
    candidate_by_field = {
        "total_services": 2.0,
        "total_beneficiary_day_services": None,
        "total_submitted_charges": 4.0,
        "total_allowed_amount": 5.0,
        "total_beneficiaries": 1.0,
        "service_description": "Office visit",
        "reported_code": "other",
    }
    claims_pricing._merge_provider_procedure_fields(accumulated_by_field, candidate_by_field)
    assert accumulated_by_field["total_allowed_amount"] == 8.0
    assert accumulated_by_field["service_description"] == "Office visit"
    assert accumulated_by_field["reported_code"] == "99213"


@pytest.mark.asyncio
async def test_provider_service_loader_aggregates_and_stages_location(monkeypatch, tmp_path):
    """Aggregate duplicate services and stage a normalized location exactly once."""

    csv_path = tmp_path / "provider_service.csv"
    _write_csv(
        csv_path,
        PROVIDER_SERVICE_FIELDS,
        [
            {**PROVIDER_SERVICE_BASE_BY_FIELD, "Tot_Srvcs": "2"},
            {**PROVIDER_SERVICE_BASE_BY_FIELD, "Tot_Srvcs": "3"},
            {
                **PROVIDER_SERVICE_BASE_BY_FIELD,
                "Rndrng_NPI": "1000000002",
                "Rndrng_Prvdr_State_Abrvtn": "invalid",
            },
        ],
    )
    pushed_by_table = {}

    async def capture_push(procedure_rows, target_cls, **_options):
        pushed_by_table.setdefault(target_cls.__tablename__, []).extend(procedure_rows)

    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", capture_push)
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    procedure_cls = SimpleNamespace(__tablename__="procedure_stage")
    location_cls = SimpleNamespace(__tablename__="location_stage")
    await claims_pricing._load_provider_service_rows(
        str(csv_path),
        procedure_cls,
        location_cls,
        2023,
        test_mode=False,
    )
    assert pushed_by_table["procedure_stage"][0]["total_services"] == 5.0
    assert pushed_by_table["procedure_stage"][0]["total_allowed_amount"] == 50.0
    assert pushed_by_table["location_stage"][0]["state_fips"] == "6"
    assert len(pushed_by_table["location_stage"]) == 1


@pytest.mark.asyncio
async def test_provider_service_loader_test_limit(monkeypatch, tmp_path):
    csv_path = tmp_path / "provider_service.csv"
    _write_csv(
        csv_path,
        ["Rndrng_NPI", "HCPCS_Cd"],
        [
            {"Rndrng_NPI": "1000000001", "HCPCS_Cd": "99213"},
            {"Rndrng_NPI": "1000000002", "HCPCS_Cd": "99214"},
        ],
    )
    push_objects = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", push_objects)
    monkeypatch.setattr(claims_pricing, "_row_allowed_for_test", lambda _row_number: True)
    monkeypatch.setattr(claims_pricing, "TEST_PROVIDER_SERVICE_ROW_LIMIT", 1)
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    await claims_pricing._load_provider_service_rows(
        str(csv_path),
        SimpleNamespace(__tablename__="procedure"),
        SimpleNamespace(__tablename__="location"),
        2023,
        test_mode=True,
    )
    assert push_objects.await_count == 2


@pytest.mark.parametrize(
    ("geo_level", "expected_priority"),
    [("national", 3), ("state", 2), ("county", 1), ("other", 0)],
)
def test_geo_priority_contract(geo_level, expected_priority):
    source_row_by_field = {"Rndrng_Prvdr_Geo_Lvl": geo_level}
    assert claims_pricing._geo_level_priority(source_row_by_field) == expected_priority


def test_geo_scope_contract_handles_supported_levels():
    assert claims_pricing._geo_scope_value_from_row({"Rndrng_Prvdr_Geo_Lvl": "national"}) == (
        "national",
        "US",
    )
    assert claims_pricing._geo_scope_value_from_row(
        {"Rndrng_Prvdr_Geo_Lvl": "state", "Rndrng_Prvdr_State_Abrvtn": "ca"}
    ) == ("state", "CA")
    assert claims_pricing._geo_scope_value_from_row(
        {"Rndrng_Prvdr_Geo_Lvl": "state", "Rndrng_Prvdr_State_Abrvtn": "invalid"}
    ) is None
    assert claims_pricing._geo_scope_value_from_row({"Rndrng_Prvdr_Geo_Lvl": "county"}) is None


def test_geo_candidate_exposes_procedure_without_benchmark():
    assert claims_pricing._geo_service_candidate({"HCPCS_Cd": "invalid"}, 2023) is None
    candidate = claims_pricing._geo_service_candidate(
        {"HCPCS_Cd": "99213", "Tot_Srvcs": "2", "Avg_Mdcr_Alowd_Amt": "10"},
        2023,
    )
    assert candidate.procedure_row_by_field["total_allowed_amount"] == 20.0
    assert candidate.benchmark_key is None


@pytest.mark.asyncio
async def test_geo_loader_selects_priority_and_weight(monkeypatch, tmp_path):
    """Prefer national procedure labels while retaining the strongest scope benchmark."""

    csv_path = tmp_path / "geo.csv"
    _write_csv(csv_path, GEO_SERVICE_FIELDS, GEO_SERVICE_ROWS_BY_FIELD)
    pushed_by_table = {}

    async def capture_push(geo_rows, target_cls, **_options):
        pushed_by_table[target_cls.__tablename__] = list(geo_rows)

    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", capture_push)
    monkeypatch.setattr(claims_pricing, "_print_row_progress", Mock())
    await claims_pricing._load_geo_service_rows(
        str(csv_path),
        SimpleNamespace(__tablename__="procedure"),
        SimpleNamespace(__tablename__="benchmark"),
        2023,
        test_mode=False,
    )
    assert pushed_by_table["procedure"][0]["service_description"] == "National"
    state_benchmark = next(
        benchmark_row
        for benchmark_row in pushed_by_table["benchmark"]
        if benchmark_row["geography_scope"] == "state"
    )
    assert state_benchmark["total_services"] == 5.0


@pytest.mark.asyncio
async def test_push_helpers_accept_empty_collections(monkeypatch):
    push_objects = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_push_objects_with_retry", push_objects)
    await claims_pricing._flush_location_rows([], object())
    await claims_pricing._flush_provider_procedure_rows({}, object())
    await claims_pricing._push_geo_service_candidates({}, {}, object(), object())
    push_objects.assert_not_awaited()
