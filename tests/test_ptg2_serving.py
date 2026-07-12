# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving
from api import ptg2_response
from api import ptg2_price_sql
from api import ptg2_code_filters
from api import ptg2_types
from api import ptg2_index_cache
from api import ptg2_tables
from api import ptg2_serving_utils
from api import ptg2_code_details
from api import ptg2_code_context
from api import ptg2_snapshot
from api.code_systems import catalog_code_lookup_values
from process.ptg_parts.address_assurance import summarize_ptg_price_address_payload
from process.ptg_parts.ptg2_manifest_artifacts import (
    write_serving_by_code_sidecar,
    write_serving_by_provider_set_sidecar,
)


class FakeResult:
    def __init__(self, scalar=None, rows=None):
        self._scalar = scalar
        self._rows = list(rows or [])

    def scalar(self):
        return self._scalar

    def first(self):
        return self._rows[0] if self._rows else None

    def __iter__(self):
        return iter(self._rows)


class FakeSession:
    def __init__(self, results):
        self._results = list(results)
        self.calls = []
        self.rollback_count = 0

    async def execute(self, *_args, **_kwargs):
        statement_sql = str(_args[0]).strip().lower() if _args else ""
        if statement_sql == "set local jit = off":
            return FakeResult()
        if "set_config('plan_cache_mode', 'force_custom_plan', true)" in statement_sql:
            self.calls.append((_args, _kwargs))
            return FakeResult(
                rows=[
                    {
                        "plan_cache_mode": "auto",
                        "parallel_workers": "2",
                    }
                ]
            )
        if "set_config('plan_cache_mode', :plan_cache_mode, true)" in statement_sql:
            self.calls.append((_args, _kwargs))
            return FakeResult()
        self.calls.append((_args, _kwargs))
        value = self._results.pop(0) if self._results else None
        if isinstance(value, Exception):
            raise value
        if isinstance(value, FakeResult):
            return value
        return FakeResult(value)

    async def rollback(self):
        self.rollback_count += 1


class FakePagination:
    limit = 25
    offset = 0


class _Pagination:
    def __init__(self, *, limit=25, offset=0):
        self.limit = limit
        self.offset = offset


def test_revenue_code_lookup_values_include_raw_and_canonical_forms():
    assert catalog_code_lookup_values("RC", "110") == ("0110", "110")
    assert catalog_code_lookup_values("revenue_code", "0450") == ("0450", "450")
    assert catalog_code_lookup_values("RC", "020") == ("0020", "020", "20")
    assert catalog_code_lookup_values("RC", "0020") == ("0020", "020", "20")
    assert catalog_code_lookup_values("CPT", "99213") == ("99213",)


def _revenue_code_count_rows(*, include_rate_count: bool = False):
    """Build canonical and raw code-dictionary rows for synthetic tests."""

    rows = [
        {
            "code_key": code_key,
            "plan_id": "TESTPLAN001",
            "reported_code_system": "RC",
            "reported_code": reported_code,
        }
        for code_key, reported_code in ((7, "0110"), (8, "110"))
    ]
    if include_rate_count:
        for row in rows:
            row["rate_count"] = 1
    return rows


def _manifest_fixture_rate_row(
    *,
    code_key: int,
    reported_code: str,
    provider_set_id: str,
    price_set_id: str,
    provider_count: int,
):
    """Build one synthetic manifest response row."""

    return {
        "serving_content_hash_128": f"{code_key + 500:032x}",
        "plan_id": "TESTPLAN001",
        "reported_code_system": "RC",
        "reported_code": reported_code,
        "provider_set_global_id_128": provider_set_id,
        "provider_count": provider_count,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": None,
        "network_names": [],
    }


def _v3_revenue_forward_fixture(monkeypatch):
    """Configure a synthetic v3 forward lookup with two compatible code forms."""

    canonical_provider_set_id = "0000000000000000000000000000000a"
    raw_provider_set_id = "0000000000000000000000000000000b"
    canonical_price_set_id = "00000000000000000000000000000101"
    raw_price_set_id = "00000000000000000000000000000102"
    session = FakeSession([FakeResult(rows=_revenue_code_count_rows(include_rate_count=True))])
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        serving_binary_table="mrf.ptg2_serving_binary_fixture",
        code_count_table="mrf.ptg2_code_count_fixture",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dictionary_fixture",
        serving_table_layout="lean_provider_key_v1",
    )
    variant_reader = AsyncMock(
        return_value=[
            _manifest_fixture_rate_row(
                code_key=7,
                reported_code="0110",
                provider_set_id=canonical_provider_set_id,
                price_set_id=canonical_price_set_id,
                provider_count=2,
            ),
            _manifest_fixture_rate_row(
                code_key=8,
                reported_code="110",
                provider_set_id=raw_provider_set_id,
                price_set_id=raw_price_set_id,
                provider_count=3,
            ),
        ]
    )
    price_reader = AsyncMock(
        return_value={
            canonical_price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": "100.00"}],
            raw_price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": "200.00"}],
        }
    )
    procedure_reader = AsyncMock(
        return_value={
            ("RC", "0110"): {"procedure_name": "Example revenue service"},
            ("RC", "110"): {"procedure_name": "Example revenue service"},
        }
    )
    monkeypatch.setattr(ptg2_serving, "_merge_manifest_code_variant_rows", variant_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_reader)
    return SimpleNamespace(
        session=session,
        tables=tables,
        variant_reader=variant_reader,
        price_reader=price_reader,
        canonical_price_set_id=canonical_price_set_id,
        raw_price_set_id=raw_price_set_id,
    )


def _fake_call_sql(session: FakeSession, pattern: str) -> str:
    for args, _kwargs in session.calls:
        sql = str(args[0])
        if pattern in sql:
            return sql
    raise AssertionError(f"SQL call containing {pattern!r} not found")


def _async_sidecar_members_many(mapping):
    async def fake(_session, _serving_tables, _name, _owner_ids, **_kwargs):
        return mapping

    return fake


@pytest.mark.asyncio
async def test_overlay_provider_directory_corroboration_marks_address_and_prefers_directory_phone():
    session = FakeSession(
        [
            "mrf.provider_directory_address_corroboration",
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "address_key": "00000000-0000-0000-0000-000000000001",
                        "source_key": "ptg_source",
                        "snapshot_id": "ptg2:202606:snap",
                        "plan_id": "TESTPLAN001",
                        "ptg_plan_id": "ptg-plan",
                        "provider_directory_source_id": "pdfhir_1",
                        "provider_directory_org_name": "Example Payer",
                        "provider_directory_plan_name": "Example Plan",
                        "provider_directory_provider_resource_id": "prac-1",
                        "provider_directory_provider_name": "Alex Rivera",
                        "provider_directory_role_resource_id": "role-1",
                        "provider_directory_location_resource_id": "loc-1",
                        "provider_directory_location_name": "Example Clinic",
                        "provider_directory_telephone_number": "312-555-0100",
                        "provider_directory_phone_number": "3125550100",
                        "provider_directory_phone_extension": "45",
                        "provider_directory_fax_number": "312-555-0101",
                        "provider_directory_fax_number_digits": "3125550101",
                        "provider_directory_fax_extension": "9",
                        "provider_directory_plan_context_matched": True,
                        "provider_directory_network_context_present": True,
                        "provider_directory_network_refs": ["Organization/network-1"],
                        "provider_directory_insurance_plan_refs": ["InsurancePlan/plan-1"],
                        "provider_directory_insurance_plan_matches": ["InsurancePlan/plan-1"],
                        "provider_directory_match_type": "practitioner_role",
                        "provider_directory_observed_at": "2026-06-28T00:00:00",
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_verification_evidence": {
                            "source": "provider_directory_fhir",
                            "matched_on": "npi_address_key_role_location_plan",
                        },
                    }
                ]
            ),
        ]
    )

    rows = await ptg2_serving._overlay_provider_directory_corroboration(
        session,
        [
            {
                "npi": 1234567890,
                "location_source": "npi_address",
                "location_confidence_code": "npi_address",
                "telephone_number": "312-000-0000",
                "address_payload": {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "first_line": "100 Main St",
                },
            }
        ],
        plan_id="TESTPLAN001",
        snapshot_id="ptg2:202606:snap",
        source_key="ptg_source",
    )

    row = rows[0]
    assert row["location_source"] == "provider_directory_fhir"
    assert row["location_confidence_code"] == "payer_directory_corroborated_location"
    assert row["telephone_number"] == "312-555-0100"
    assert row["phone_number"] == "3125550100"
    assert row["phone_extension"] == "45"
    assert row["fax_number"] == "312-555-0101"
    assert row["fax_number_digits"] == "3125550101"
    assert row["fax_extension"] == "9"
    assert row["address_payload"]["address_sources"] == ["provider_directory_fhir"]
    assert row["address_payload"]["address_network_binding"] == "payer_directory_corroborated_location"
    assert row["address_payload"]["provider_directory_location_name"] == "Example Clinic"
    assert row["address_payload"]["phone_number"] == "3125550100"
    assert row["address_payload"]["fax_number_digits"] == "3125550101"
    item = ptg2_serving._compact_item_from_row({**row, "prices": []}, {})
    assert item["address_verification"]["address_network_binding"] == "payer_directory_corroborated_location"
    assert item["address_verification"]["requires_location_confirmation"] is False
    assert item["address_verification"]["displayed_address_present"] is True
    assert item["address_verification"]["network_bound_address"] is True
    assert item["address_verification"]["provider_directory_plan_context_matched"] is True
    assert item["address_verification"]["provider_directory_location_resource_id"] == "loc-1"
    assert item["address_verification"]["provider_directory_insurance_plan_matches"] == ["InsurancePlan/plan-1"]
    assert item["address_verification"]["address_verification_evidence"]["matched_on"].endswith("_plan")
    sql = str(session.calls[1][0][0])
    assert "address_key = ANY(CAST(:address_keys AS uuid[]))" in sql
    assert "address_key::text = ANY(CAST(:address_keys AS text[]))" not in sql


@pytest.mark.asyncio
async def test_overlay_provider_directory_address_only_keeps_network_binding_inferred():
    session = FakeSession(
        [
            "mrf.provider_directory_address_corroboration",
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "address_key": "00000000-0000-0000-0000-000000000001",
                        "source_key": "ptg_source",
                        "snapshot_id": "ptg2:202606:snap",
                        "plan_id": "TESTPLAN001",
                        "ptg_plan_id": "ptg-plan",
                        "provider_directory_source_id": "pdfhir_1",
                        "provider_directory_location_resource_id": "loc-1",
                        "provider_directory_location_name": "Example Clinic",
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_context_present": True,
                        "provider_directory_network_refs": ["Organization/network-1"],
                        "provider_directory_insurance_plan_refs": [],
                        "provider_directory_insurance_plan_matches": [],
                        "address_network_binding": "provider_directory_address",
                        "address_verification_evidence": {
                            "source": "provider_directory_fhir",
                            "matched_on": "npi_address_key_role_location",
                        },
                    }
                ]
            ),
        ]
    )

    rows = await ptg2_serving._overlay_provider_directory_corroboration(
        session,
        [
            {
                "npi": 1234567890,
                "location_source": "npi_address",
                "location_confidence_code": "npi_address",
                "address_payload": {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "first_line": "100 Main St",
                },
            }
        ],
        plan_id="TESTPLAN001",
        snapshot_id="ptg2:202606:snap",
        source_key="ptg_source",
    )

    item = ptg2_serving._compact_item_from_row({**rows[0], "prices": []}, {})

    assert rows[0]["location_confidence_code"] == "provider_directory_address"
    assert rows[0]["address_payload"]["provider_directory_network_refs"] == ["Organization/network-1"]
    assert item["address_verification"]["address_evidence_level"] == "provider_directory_address"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["displayed_address_present"] is True
    assert item["address_verification"]["network_bound_address"] is False
    assert item["address_verification"]["provider_directory_plan_context_matched"] is False
    assert item["address_verification"]["provider_directory_network_context_present"] is True
    assert item["address_verification"]["provider_directory_network_refs"] == ["Organization/network-1"]
    assert item["address_verification"]["address_verification_evidence"]["matched_on"] == "npi_address_key_role_location"


@pytest.mark.asyncio
async def test_overlay_provider_directory_without_plan_match_downgrades_network_marker():
    session = FakeSession(
        [
            "mrf.provider_directory_address_corroboration",
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "address_key": "00000000-0000-0000-0000-000000000001",
                        "source_key": "ptg_source",
                        "snapshot_id": "ptg2:202606:snap",
                        "plan_id": "TESTPLAN001",
                        "ptg_plan_id": "ptg-plan",
                        "provider_directory_source_id": "pdfhir_1",
                        "provider_directory_location_resource_id": "loc-1",
                        "provider_directory_location_name": "Example Clinic",
                        "provider_directory_plan_context_matched": False,
                        "provider_directory_network_context_present": True,
                        "provider_directory_network_refs": ["Organization/network-1"],
                        "provider_directory_insurance_plan_refs": [],
                        "provider_directory_insurance_plan_matches": [],
                        "address_network_binding": "payer_directory_corroborated_location",
                        "address_verification_evidence": {
                            "source": "provider_directory_fhir",
                            "matched_on": "npi_address_key_role_location",
                        },
                    }
                ]
            ),
        ]
    )

    rows = await ptg2_serving._overlay_provider_directory_corroboration(
        session,
        [
            {
                "npi": 1234567890,
                "location_source": "npi_address",
                "location_confidence_code": "npi_address",
                "address_payload": {
                    "address_key": "00000000-0000-0000-0000-000000000001",
                    "first_line": "100 Main St",
                },
            }
        ],
        plan_id="TESTPLAN001",
        snapshot_id="ptg2:202606:snap",
        source_key="ptg_source",
    )

    item = ptg2_serving._compact_item_from_row({**rows[0], "prices": []}, {})

    assert rows[0]["location_confidence_code"] == "provider_directory_address"
    assert rows[0]["address_payload"]["address_network_binding"] == "provider_directory_address"
    assert item["address_verification"]["address_evidence_level"] == "provider_directory_address"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["provider_directory_plan_context_matched"] is False


def test_ptg2_response_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._shape_ptg2_response is ptg2_response._shape_ptg2_response
    assert ptg2_serving._catalog_key is ptg2_response._catalog_key
    assert ptg2_serving._canonical_catalog_code is ptg2_response._canonical_catalog_code
    assert ptg2_serving._coerce_json_payload is ptg2_response._coerce_json_payload
    assert ptg2_serving._normalize_price_payload is ptg2_response._normalize_price_payload
    assert ptg2_serving._summarize_price_payload is ptg2_response._summarize_price_payload
    assert ptg2_serving._price_response_fields is ptg2_response._price_response_fields


def test_ptg2_response_shape_keeps_address_verification_without_source_opt_in():
    shaped = ptg2_serving._shape_ptg2_response(
        {
            "items": [
                {
                    "npi": 1234567890,
                    "source_key": "ptg_source",
                    "snapshot_id": "ptg2:202606:snap",
                    "network_names": ["C2"],
                    "address_verification": {
                        "address_network_binding": "payer_directory_corroborated_location",
                        "provider_directory_plan_context_matched": True,
                        "address_verification_evidence": {
                            "matched_on": "npi_address_key_role_location_plan"
                        },
                    },
                }
            ],
            "query": {"source_key": "ptg_source", "snapshot_id": "ptg2:202606:snap"},
        },
        {},
    )

    item = shaped["items"][0]
    assert "source_key" not in item
    assert "snapshot_id" not in item
    assert "network_names" not in item
    assert item["address_verification"]["address_network_binding"] == "payer_directory_corroborated_location"
    assert item["address_verification"]["provider_directory_plan_context_matched"] is True
    assert item["address_verification"]["address_verification_evidence"]["matched_on"].endswith("_plan")


def test_ptg2_price_sql_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._empty_price_array_sql is ptg2_price_sql._empty_price_array_sql
    assert ptg2_serving._scalar_price_json_sql is ptg2_price_sql._scalar_price_json_sql
    assert ptg2_serving._typed_price_json_sql is ptg2_price_sql._typed_price_json_sql
    assert ptg2_serving._normalized_price_json_sql is ptg2_price_sql._normalized_price_json_sql
    assert ptg2_serving._price_atom_payload_sql is ptg2_price_sql._price_atom_payload_sql
    assert ptg2_serving._normalized_price_join_sql is ptg2_price_sql._normalized_price_join_sql


def test_ptg2_code_filter_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._normalize_code is ptg2_code_filters._normalize_code
    assert ptg2_serving._normalize_code_system is ptg2_code_filters._normalize_code_system
    assert ptg2_serving._append_code_filter is ptg2_code_filters._append_code_filter
    assert ptg2_serving._append_resolved_code_filter is ptg2_code_filters._append_resolved_code_filter
    assert ptg2_serving._ptg2_code_query_fields is ptg2_code_filters._ptg2_code_query_fields
    assert ptg2_serving._qualify_compact_filters is ptg2_code_filters._qualify_compact_filters
    assert ptg2_serving._normalize_taxonomy_code is ptg2_code_filters._normalize_taxonomy_code
    assert ptg2_serving._normalize_npi is ptg2_code_filters._normalize_npi
    assert ptg2_serving._inferred_provider_taxonomy_sql is ptg2_code_filters._inferred_provider_taxonomy_sql


def test_ptg2_code_context_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._query_ptg2_code_crosswalk_edges is ptg2_code_context._query_ptg2_code_crosswalk_edges
    assert ptg2_serving._resolve_ptg2_code_search_context is ptg2_code_context._resolve_ptg2_code_search_context


def test_ptg2_type_split_keeps_serving_facade_classes_stable():
    assert ptg2_serving.PTG2ServingIndex is ptg2_types.PTG2ServingIndex
    assert ptg2_serving.PTG2ServingTables is ptg2_types.PTG2ServingTables


def test_ptg2_index_cache_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving.clear_ptg2_index_cache is ptg2_index_cache.clear_ptg2_index_cache
    assert ptg2_serving._ptg2_response_cache_key is ptg2_index_cache._ptg2_response_cache_key
    assert ptg2_serving._ptg2_response_cache_get is ptg2_index_cache._ptg2_response_cache_get
    assert ptg2_serving._ptg2_response_cache_set is ptg2_index_cache._ptg2_response_cache_set
    assert ptg2_serving._artifact_root is ptg2_index_cache._artifact_root
    assert ptg2_serving._path_from_uri is ptg2_index_cache._path_from_uri
    assert ptg2_serving.load_ptg2_index_from_path is ptg2_index_cache.load_ptg2_index_from_path


@pytest.mark.asyncio
async def test_postgres_binary_serving_table_does_not_response_cache_direct_lookup(monkeypatch):
    ptg2_serving.clear_ptg2_index_cache()
    call_count_by_name = {"uncached": 0}

    async def fake_manifest_db_search(_session, _snapshot_id, _args, _pagination, serving_tables, _mode_value):
        call_count_by_name["uncached"] += 1
        return {
            "items": [{"serving_binary_table": serving_tables.serving_binary_table}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": _snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving, "_search_ptg2_manifest_db_serving_table", fake_manifest_db_search)
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v1",
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_serving_rate_a",
        serving_binary_table="mrf.ptg2_serving_binary_a",
    )

    first_payload = await ptg2_serving.search_ptg2_serving_table(
        object(),
        "snapshot-a",
        {"plan_id": "plan-a", "code": "99213", "code_system": "CPT"},
        FakePagination(),
        serving_tables=tables,
    )
    second_payload = await ptg2_serving.search_ptg2_serving_table(
        object(),
        "snapshot-a",
        {"plan_id": "plan-a", "code": "99213", "code_system": "CPT"},
        FakePagination(),
        serving_tables=tables,
    )
    await ptg2_serving.search_ptg2_serving_table(
        object(),
        "snapshot-a",
        {"plan_id": "plan-a", "code": "99213", "code_system": "CPT"},
        FakePagination(),
        serving_tables=ptg2_serving.PTG2ServingTables(
            arch_version="postgres_binary_v1",
            storage="manifest_snapshot",
            serving_table="mrf.ptg2_serving_rate_b",
            serving_binary_table="mrf.ptg2_serving_binary_b",
        ),
    )

    assert first_payload == second_payload
    assert call_count_by_name["uncached"] == 3
    ptg2_serving.clear_ptg2_index_cache()


@pytest.mark.asyncio
async def test_postgres_binary_provider_procedures_does_not_response_cache_reverse_lookup(monkeypatch):
    ptg2_serving.clear_ptg2_index_cache()
    call_count_by_name = {"uncached": 0}
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v1",
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_serving_rate_a",
        serving_binary_table="mrf.ptg2_serving_binary_a",
    )

    async def fake_resolve_snapshot(_session, _args):
        return "snapshot-a"

    async def fake_snapshot_tables(_session, _snapshot_id):
        return tables

    async def fake_provider_procedures(
        _session,
        npi,
        _args,
        _pagination,
        *,
        snapshot_id,
        serving_tables,
    ):
        call_count_by_name["uncached"] += 1
        return {
            "items": [{"npi": npi, "serving_binary_table": serving_tables.serving_binary_table}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve_snapshot)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot_tables)
    monkeypatch.setattr(ptg2_serving, "_search_ptg2_manifest_provider_procedures", fake_provider_procedures)

    first_payload = await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "plan-a", "source_key": "network-a", "include_details": "true"},
        FakePagination(),
    )
    second_payload = await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "plan-a", "source_key": "network-a", "include_details": "true"},
        FakePagination(),
    )
    await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        2234567890,
        {"plan_id": "plan-a", "source_key": "network-a", "include_details": "true"},
        FakePagination(),
    )

    assert first_payload == second_payload
    assert call_count_by_name["uncached"] == 3
    ptg2_serving.clear_ptg2_index_cache()


def _patch_multi_network_provider_procedure_search(monkeypatch):
    async def fake_network_snapshots(_session, _args):
        return [("network-z", "snapshot-z"), ("network-a", "snapshot-a")]

    async def fake_snapshot_search(_session, npi, _args, _pagination, *, snapshot_id):
        provider_count = 20 if snapshot_id == "snapshot-z" else 10
        return {
            "items": [
                {
                    "npi": npi,
                    "reported_code_system": "CPT",
                    "reported_code": "99213",
                    "provider_count": provider_count,
                    "provider_set_hash": f"provider-{snapshot_id}",
                    "price_set_hash": f"price-{snapshot_id}",
                    "prices": [{"negotiated_rate": "100.00"}],
                }
            ],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(
        ptg2_serving,
        "current_source_snapshot_ids_for_plan",
        fake_network_snapshots,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_search_ptg2_provider_procedures_snapshot",
        fake_snapshot_search,
    )


@pytest.mark.asyncio
async def test_provider_procedures_combines_every_plan_network(monkeypatch):
    _patch_multi_network_provider_procedure_search(monkeypatch)
    combined_payload_by_field = await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "TESTPLAN001", "include_details": "true"},
        FakePagination(),
    )

    assert [procedure_item["provider_count"] for procedure_item in combined_payload_by_field["items"]] == [
        20,
        10,
    ]
    assert [procedure_item["network"] for procedure_item in combined_payload_by_field["items"]] == [
        "network-z",
        "network-a",
    ]
    assert combined_payload_by_field["pagination"]["total"] == 2
    assert combined_payload_by_field["query"]["combined"] is True
    assert combined_payload_by_field["query"]["snapshots"] == ["snapshot-z", "snapshot-a"]


@pytest.mark.asyncio
async def test_provider_procedure_network_pages_use_merged_order(monkeypatch):
    _patch_multi_network_provider_procedure_search(monkeypatch)
    first_page = await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "TESTPLAN001", "include_details": "true"},
        _Pagination(limit=1, offset=0),
    )
    second_page = await ptg2_serving.search_ptg2_provider_procedures(
        object(),
        1234567890,
        {"plan_id": "TESTPLAN001", "include_details": "true"},
        _Pagination(limit=1, offset=1),
    )

    assert [procedure_item["network"] for procedure_item in first_page["items"]] == ["network-z"]
    assert first_page["pagination"]["has_more"] is True
    assert [procedure_item["network"] for procedure_item in second_page["items"]] == ["network-a"]
    assert second_page["pagination"]["has_more"] is False


def test_ptg2_snapshot_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving.current_snapshot_id is ptg2_snapshot.current_snapshot_id
    assert ptg2_serving.current_source_snapshot_id_for_plan is ptg2_snapshot.current_source_snapshot_id_for_plan
    assert ptg2_serving.resolve_current_ptg2_snapshot_id is ptg2_snapshot.resolve_current_ptg2_snapshot_id
    assert ptg2_serving.snapshot_artifact_uri is ptg2_snapshot.snapshot_artifact_uri
    assert ptg2_serving.load_current_ptg2_index is ptg2_snapshot.load_current_ptg2_index


def test_ptg2_table_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._serving_table_available is ptg2_tables._serving_table_available
    assert ptg2_serving._index_available is ptg2_tables._index_available
    assert ptg2_serving._gin_index_available_for_column is ptg2_tables._gin_index_available_for_column
    assert ptg2_serving._serving_table_name is ptg2_tables._serving_table_name
    assert ptg2_serving._safe_table_name is ptg2_tables._safe_table_name
    assert ptg2_serving._serving_table_candidates is ptg2_tables._serving_table_candidates
    assert ptg2_serving.snapshot_serving_table is ptg2_tables.snapshot_serving_table
    assert ptg2_serving.snapshot_serving_tables is ptg2_tables.snapshot_serving_tables
    assert ptg2_serving._ordered_serving_table_candidates is ptg2_tables._ordered_serving_table_candidates
    assert ptg2_serving._is_compact_serving_table is ptg2_tables._is_compact_serving_table


def test_ptg2_serving_utils_split_keeps_serving_facade_helpers_stable():
    assert ptg2_serving._normalize_zip5 is ptg2_serving_utils._normalize_zip5
    assert ptg2_serving._provider_payload is ptg2_serving_utils._provider_payload
    assert ptg2_serving._row_mapping is ptg2_serving_utils._row_mapping
    assert ptg2_serving._price_filter_clauses is ptg2_serving_utils._price_filter_clauses


@pytest.mark.asyncio
async def test_manifest_filter_npis_by_provider_taxonomy_uses_primary_code_set():
    session = FakeSession([FakeResult(rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"specialty": "Family Medicine"},
        [1234567890, 1234567891, 1234567892],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "manifest_provider_specialty_nt.npi = source_npis.npi" in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert params["manifest_provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["manifest_provider_specialty_taxonomy_code_1"] == "208D00000X"


@pytest.mark.asyncio
async def test_manifest_filter_npis_by_inferred_taxonomy_requires_individual_npi():
    session = FakeSession([FakeResult(rows=[{"npi": 1234567890}])])

    filtered = await ptg2_serving._ptg2_manifest_filter_npis_by_provider_taxonomy(
        session,
        {"code": "29888"},
        [1234567890, 1234567891, 1234567892],
        limit=10,
    )

    assert filtered == (1234567890,)
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "FROM mrf.npi_taxonomy nt WHERE nt.npi = source_npis.npi" in sql
    assert "n_entity.entity_type_code" in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_manifest_prices_for_price_sets_rehydrates_lean_price_atom_dictionary(monkeypatch):
    price_set_id = "00000000000000000000000000000004"
    price_atom_id = "00000000000000000000000000000005"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "price_atom_global_id_128": price_atom_id,
                        "negotiated_type": "fee schedule",
                        "negotiated_rate": "36.23",
                        "expiration_date": "9999-12-31",
                        "service_code": ["11"],
                        "billing_class": "professional",
                        "setting": None,
                        "billing_code_modifier": [],
                        "additional_information": None,
                    }
                ]
            )
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        price_atom_table="mrf.ptg2_price_atom_manifest_snap",
        price_atom_table_layout="lean_dict_v1",
        price_atom_dictionary_table="mrf.ptg2_price_atom_dict_manifest_snap",
        id_storage="uuid",
    )

    async def fake_sidecar_members_many(_session, _serving_tables, sidecar_name, owner_ids, **_kwargs):
        assert sidecar_name == "price_forward"
        assert owner_ids == (price_set_id,)
        return {price_set_id: (price_atom_id,)}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    prices_by_set = await ptg2_serving._ptg2_manifest_prices_for_price_sets(session, tables, [price_set_id])

    sql = str(session.calls[0][0][0])
    assert prices_by_set == {
        price_set_id: [
            {
                "negotiated_type": "fee schedule",
                "negotiated_rate": "36.23",
                "expiration_date": "9999-12-31",
                "service_code": ["11"],
                "billing_class": "professional",
                "setting": None,
                "billing_code_modifier": [],
                "additional_information": None,
            }
        ]
    }
    assert "FROM mrf.ptg2_price_atom_manifest_snap price_atom" in sql
    assert "LEFT JOIN mrf.ptg2_price_atom_dict_manifest_snap negotiated_type" in sql
    assert "LEFT JOIN mrf.ptg2_price_atom_dict_manifest_snap service_code" in sql
    assert "LEFT JOIN mrf.ptg2_price_atom_dict_manifest_snap billing_code_modifier" in sql
    assert "price_atom.negotiated_type_key" in sql
    assert "price_atom_global_id_128 = ANY(CAST(:atom_ids AS uuid[]))" in sql


@pytest.mark.asyncio
async def test_manifest_prices_for_price_sets_prefers_serving_binary_atoms(monkeypatch):
    price_set_id = "00000000000000000000000000000024"
    price_atom_id = "00000000000000000000000000000025"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "price_atom_global_id_128": price_atom_id,
                        "negotiated_type": "fee schedule",
                        "negotiated_rate": "39.50",
                        "expiration_date": "9999-12-31",
                        "service_code": ["11"],
                        "billing_class": "professional",
                        "setting": None,
                        "billing_code_modifier": [],
                        "additional_information": None,
                    }
                ]
            )
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        price_atom_table="mrf.ptg2_price_atom_manifest_snap",
        serving_binary_table="mrf.ptg2_serving_binary_manifest_snap",
        id_storage="uuid",
    )

    async def fake_binary_price_members(_session, table_name, owner_ids):
        assert table_name == "mrf.ptg2_serving_binary_manifest_snap"
        assert owner_ids == (price_set_id,)
        return {price_set_id: (price_atom_id,)}

    async def fail_legacy_binary_price_members(*_args, **_kwargs):
        raise AssertionError("price dictionary fallback should not be used when direct price-id atom blocks exist")

    async def fail_sidecar_members_many(*_args, **_kwargs):
        raise AssertionError("price_forward fallback should not be used when serving binary atom blocks exist")

    monkeypatch.setattr(ptg2_serving, "lookup_atoms_by_price_id", fake_binary_price_members)
    monkeypatch.setattr(ptg2_serving, "lookup_binary_price_atoms_from_db", fail_legacy_binary_price_members)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fail_sidecar_members_many)

    prices_by_set = await ptg2_serving._ptg2_manifest_prices_for_price_sets(session, tables, [price_set_id])

    assert prices_by_set[price_set_id][0]["negotiated_rate"] == "39.50"
    assert str(session.calls[0][0][0]).count("price_atom_global_id_128 = ANY") == 1


@pytest.mark.asyncio
async def test_manifest_prices_rehydrates_lean_price_atom_constants(monkeypatch):
    price_set_id = "00000000000000000000000000000014"
    price_atom_id = "00000000000000000000000000000015"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "price_atom_global_id_128": price_atom_id,
                        "negotiated_type": "fee schedule",
                        "negotiated_rate": "41.25",
                        "expiration_date": "9999-12-31",
                        "service_code": ["11"],
                        "billing_class": "professional",
                        "setting": None,
                        "billing_code_modifier": [],
                        "additional_information": None,
                    }
                ]
            )
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        price_atom_table="mrf.ptg2_price_atom_manifest_snap",
        price_atom_table_layout="lean_dict_v2",
        price_atom_constant_values={
            "negotiated_type": "fee schedule",
            "expiration_date": "9999-12-31",
            "service_code": ["11"],
            "billing_class": "professional",
            "setting": None,
            "billing_code_modifier": [],
            "additional_information": None,
        },
        id_storage="uuid",
    )

    async def fake_sidecar_members_many(_session, _serving_tables, sidecar_name, owner_ids, **_kwargs):
        assert sidecar_name == "price_forward"
        assert owner_ids == (price_set_id,)
        return {price_set_id: (price_atom_id,)}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    prices_by_set = await ptg2_serving._ptg2_manifest_prices_for_price_sets(session, tables, [price_set_id])

    sql = str(session.calls[0][0][0])
    assert prices_by_set[price_set_id][0]["negotiated_rate"] == "41.25"
    assert "price_atom.negotiated_type_key" not in sql
    assert "price_atom.service_code_key" not in sql
    assert "LEFT JOIN" not in sql
    assert "'fee schedule'::text AS negotiated_type" in sql
    assert "ARRAY['11']::text[] AS service_code" in sql


def _lean_source_level_case(monkeypatch):
    """Return fake lean serving state with a source-level code-count row."""
    provider_hash = "00000000000000000000000000000012"
    price_hash = "00000000000000000000000000000101"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "",
                        "reported_code_system": "CPT",
                        "reported_code": "0001A",
                        "rate_count": 3,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": "00000000000000000000000000000201",
                        "plan_id": "",
                        "reported_code_system": "CPT",
                        "reported_code": "0001A",
                        "procedure_global_id_128": None,
                        "provider_set_global_id_128": provider_hash,
                        "provider_count": 53,
                        "price_set_global_id_128": price_hash,
                        "source_trace_set_hash": None,
                        "network_names": ["WPPCC000001N"],
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        source_key="ptg_providence",
        id_storage="uuid",
    )

    async def is_table_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_snap"
        return True

    async def price_rows(_session, _tables, price_hashes, **_kwargs):
        assert price_hashes == [price_hash]
        return {price_hash: [{"negotiated_type": "fee schedule", "negotiated_rate": "66.55"}]}

    async def procedure_rows(_session, row_data):
        assert row_data[0]["reported_code"] == "0001A"
        return {("CPT", "0001A"): {"procedure_name": "Immunization administration"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", is_table_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_rows)
    return session, tables


async def _search_lean_source_case(session, tables, args):
    return await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:providence",
        args,
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )


def _nonlean_source_level_case(monkeypatch):
    """Return fake non-lean manifest state with source-level plan ids."""
    provider_hash = "00000000000000000000000000000022"
    price_hash = "00000000000000000000000000000202"
    session = FakeSession(
        [
            FakeResult(scalar=2),
            FakeResult(rows=[("network_names",)]),
            FakeResult(
                rows=[
                    {
                        "serving_content_hash_128": "00000000000000000000000000000301",
                        "plan_id": "",
                        "reported_code_system": "CPT",
                        "reported_code": "49452",
                        "procedure_global_id_128": None,
                        "provider_set_global_id_128": provider_hash,
                        "provider_count": 17,
                        "price_set_global_id_128": price_hash,
                        "source_trace_set_hash": None,
                        "network_names": ["020|02IO"],
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_example_packaging_snap",
        code_count_table="mrf.ptg2_code_count_example_packaging_snap",
        source_key="ptg_example_packaging",
        id_storage="uuid",
    )

    async def is_table_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_example_packaging_snap"
        return True

    async def price_rows(_session, _tables, price_hashes, **_kwargs):
        assert price_hashes == [price_hash]
        return {price_hash: [{"negotiated_type": "fee schedule", "negotiated_rate": "101.23"}]}

    async def procedure_rows(_session, row_data):
        assert row_data[0]["reported_code"] == "49452"
        return {("CPT", "49452"): {"procedure_name": "Gastrostomy tube replacement"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", is_table_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_rows)
    return session, tables


async def _search_nonlean_source_case(session, tables, args):
    return await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:example-packaging",
        args,
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )


@pytest.mark.asyncio
async def test_lean_serving_uses_source_level_code_count(monkeypatch):
    session, tables = _lean_source_level_case(monkeypatch)
    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:providence",
        {
            "plan_id": "56707OR1390003-00",
            "market_type": "individual",
            "source_key": "ptg_providence",
            "code": "0001A",
            "code_system": "CPT",
        },
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    code_sql = str(session.calls[0][0][0])
    code_params = session.calls[0][0][1]
    assert "plan_id = :plan_id OR COALESCE(plan_id, '') = ''" in code_sql
    assert "ORDER BY CASE WHEN plan_id = :plan_id THEN 0" in code_sql
    assert code_params["plan_id"] == "56707OR1390003-00"
    assert payload["pagination"]["total"] == 3
    assert payload["items"][0]["procedure_code"] == "0001A"
    assert payload["items"][0]["procedure_name"] == "Immunization administration"
    assert payload["items"][0]["prices"][0]["negotiated_rate"] == 66.55


@pytest.mark.asyncio
async def test_lean_serving_by_code_sidecar_serves_without_serving_table(tmp_path, monkeypatch):
    provider_set_a = "0000000000000000000000000000000a"
    provider_set_b = "0000000000000000000000000000000b"
    price_a = "00000000000000000000000000000101"
    price_b = "00000000000000000000000000000102"
    sidecar = write_serving_by_code_sidecar(
        tmp_path / "serving_by_code.ptg2sbc",
        [
            (7, 1, 3, price_a),
            (7, 2, 9, price_b),
        ],
    )
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 2,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {"provider_set_key": 1, "provider_set_global_id_128": provider_set_a},
                    {"provider_set_key": 2, "provider_set_global_id_128": provider_set_b},
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        source_key="ptg_group_fixture",
        artifacts={"serving_by_code": sidecar},
    )

    async def price_rows(_session, _tables, price_hashes, **_kwargs):
        assert price_hashes == [price_b, price_a]
        return {
            price_a: [{"negotiated_type": "negotiated", "negotiated_rate": "100.00"}],
            price_b: [{"negotiated_type": "negotiated", "negotiated_rate": "200.00"}],
        }

    async def procedure_rows(_session, row_data):
        assert [row["provider_count"] for row in row_data] == [9, 3]
        return {("CPT", "70551"): {"procedure_name": "MRI brain"}}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_rows)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:group_fixture",
        {"plan_id": "TESTPLAN001", "code": "70551", "code_system": "CPT", "include_details": "true"},
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["pagination"]["total"] == 2
    prices_by_provider_set = {
        item["provider_set_hash"]: item["prices"][0]["negotiated_rate"]
        for item in payload["items"]
    }
    assert prices_by_provider_set == {provider_set_a: 100.0, provider_set_b: 200.0}


@pytest.mark.asyncio
async def test_v3_forward_combines_raw_and_canonical_revenue_codes(monkeypatch):
    """Forward lookup must preserve rows stored under both revenue-code forms."""
    fixture = _v3_revenue_forward_fixture(monkeypatch)

    response = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        fixture.session,
        "ptg2:202607:fixture",
        {"plan_id": "TESTPLAN001", "code": "110", "code_system": "RC"},
        FakePagination(),
        fixture.tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert response["pagination"]["total"] == 2
    assert {response_item["reported_code"] for response_item in response["items"]} == {"0110", "110"}
    assert [code_row["code_key"] for code_row in fixture.variant_reader.await_args.kwargs["code_rows"]] == [7, 8]
    assert fixture.price_reader.await_args.args[2] == [
        fixture.canonical_price_set_id,
        fixture.raw_price_set_id,
    ]
    code_sql = str(fixture.session.calls[0][0][0])
    code_params_by_name = fixture.session.calls[0][0][1]
    assert "reported_code IN (:reported_code, :reported_code_1)" in code_sql
    assert "LIMIT 1" not in code_sql
    assert code_params_by_name["reported_code"] == "0110"
    assert code_params_by_name["reported_code_1"] == "110"


@pytest.mark.asyncio
async def test_code_variant_rows_merge_before_pagination(monkeypatch):
    row_reader = AsyncMock(
        side_effect=[
            [
                {
                    "serving_content_hash_128": f"{7:032x}",
                    "reported_code_system": "RC",
                    "reported_code": "0110",
                    "provider_set_global_id_128": f"{17:032x}",
                    "provider_count": 2,
                    "price_set_global_id_128": f"{27:032x}",
                }
            ],
            [
                {
                    "serving_content_hash_128": f"{8:032x}",
                    "reported_code_system": "RC",
                    "reported_code": "110",
                    "provider_set_global_id_128": f"{18:032x}",
                    "provider_count": 9,
                    "price_set_global_id_128": f"{28:032x}",
                }
            ],
        ]
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_rows_from_serving_by_code_sidecar",
        row_reader,
    )

    merged_rows = await ptg2_serving._merge_manifest_code_variant_rows(
        object(),
        ptg2_serving.PTG2ServingTables(),
        code_rows=_revenue_code_count_rows(),
        provider_set_keys=None,
        source_trace_set_hash=None,
        network_names=[],
        limit=1,
        offset=1,
    )

    assert [serving_row["reported_code"] for serving_row in merged_rows] == ["0110"]
    assert [call.kwargs["limit"] for call in row_reader.await_args_list] == [2, 2]
    assert [call.kwargs["offset"] for call in row_reader.await_args_list] == [0, 0]


@pytest.mark.asyncio
async def test_lean_serving_postgres_binary_serves_without_serving_table(monkeypatch):
    provider_set_a = "0000000000000000000000000000000a"
    provider_set_b = "0000000000000000000000000000000b"
    price_a = "00000000000000000000000000000101"
    price_b = "00000000000000000000000000000102"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 2,
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {"provider_set_key": 1, "provider_set_global_id_128": provider_set_a},
                    {"provider_set_key": 2, "provider_set_global_id_128": provider_set_b},
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_binary_table="mrf.ptg2_serving_binary_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        source_key="ptg_group_fixture",
        artifacts={
            "serving_by_code": {
                "name": "serving_by_code",
                "path": "/missing/local/cache/serving_by_code.ptg2sbc",
            }
        },
    )

    async def binary_rows(_session, table_name, code_key, *, provider_set_keys=None):
        assert table_name == "mrf.ptg2_serving_binary_manifest_snap"
        assert code_key == 7
        assert provider_set_keys is None
        return (
            SimpleNamespace(code_key=7, provider_set_key=1, provider_count=3, price_set_global_id_128=price_a),
            SimpleNamespace(code_key=7, provider_set_key=2, provider_count=9, price_set_global_id_128=price_b),
        )

    async def price_rows(_session, _tables, price_hashes, **_kwargs):
        assert price_hashes == [price_b, price_a]
        return {
            price_a: [{"negotiated_type": "negotiated", "negotiated_rate": "100.00"}],
            price_b: [{"negotiated_type": "negotiated", "negotiated_rate": "200.00"}],
        }

    async def procedure_rows(_session, row_data):
        assert [row["provider_count"] for row in row_data] == [9, 3]
        return {("CPT", "70551"): {"procedure_name": "MRI brain"}}

    monkeypatch.setattr(ptg2_serving, "lookup_serving_binary_by_code_from_db", binary_rows)
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_serving_by_code_sidecar_from_db",
        AsyncMock(side_effect=AssertionError("postgres_binary_v1 must not read serving sidecar DB artifact")),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_serving_by_code_sidecar",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("postgres_binary_v1 must not read local serving sidecar")
        ),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("postgres_binary_v1 must not resolve local serving sidecar path")
        ),
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_rows)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:group_fixture",
        {"plan_id": "TESTPLAN001", "code": "70551", "code_system": "CPT", "include_details": "true"},
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["pagination"]["total"] == 2
    prices_by_provider_set = {
        item["provider_set_hash"]: item["prices"][0]["negotiated_rate"]
        for item in payload["items"]
    }
    assert prices_by_provider_set == {provider_set_a: 100.0, provider_set_b: 200.0}


@pytest.mark.asyncio
async def test_nonlean_manifest_serving_uses_source_level_plan_rows(monkeypatch):
    session, tables = _nonlean_source_level_case(monkeypatch)
    payload = await _search_nonlean_source_case(
        session,
        tables,
        {
            "plan_id": "382418014",
            "market_type": "group",
            "source_key": "ptg_example_packaging",
            "code": "49452",
            "code_system": "CPT",
        },
    )

    count_sql = str(session.calls[0][0][0])
    row_sql = _fake_call_sql(session, "serving_content_hash_128")
    assert "plan_id = :plan_id OR COALESCE(plan_id, '') = ''" in count_sql
    assert "COALESCE(SUM(rate_count), 0)" in count_sql
    assert "plan_id = :plan_id OR COALESCE(plan_id, '') = ''" in row_sql
    assert "ORDER BY CASE WHEN plan_id = :plan_id THEN 0" in row_sql
    assert payload["pagination"]["total"] == 2
    assert payload["items"][0]["procedure_code"] == "49452"
    assert payload["items"][0]["procedure_name"] == "Gastrostomy tube replacement"
    assert payload["items"][0]["prices"][0]["negotiated_rate"] == 101.23


@pytest.mark.asyncio
async def test_nonlean_manifest_serving_accepts_source_key_only(monkeypatch):
    session, tables = _nonlean_source_level_case(monkeypatch)
    payload = await _search_nonlean_source_case(
        session,
        tables,
        {"source_key": "ptg_example_packaging", "code": "49452", "code_system": "CPT"},
    )

    count_sql = str(session.calls[0][0][0])
    count_params = session.calls[0][0][1]
    assert "COALESCE(plan_id, '') = ''" in count_sql
    assert count_params["plan_id"] == ""
    assert payload["query"]["plan_id"] is None
    assert payload["items"][0]["procedure_code"] == "49452"
    assert payload["items"][0]["prices"][0]["negotiated_rate"] == 101.23


@pytest.mark.asyncio
async def test_lean_serving_accepts_source_key_only(monkeypatch):
    session, tables = _lean_source_level_case(monkeypatch)
    payload = await _search_lean_source_case(
        session,
        tables,
        {"source_key": "ptg_providence", "code": "0001A", "code_system": "CPT"},
    )

    code_sql = str(session.calls[0][0][0])
    code_params = session.calls[0][0][1]
    assert "COALESCE(plan_id, '') = ''" in code_sql
    assert code_params["plan_id"] == ""
    assert payload["query"]["plan_id"] is None
    assert payload["items"][0]["procedure_code"] == "0001A"
    assert payload["items"][0]["prices"][0]["negotiated_rate"] == 66.55


@pytest.mark.asyncio
async def test_manifest_provider_rows_keep_partial_results_when_sidecar_owner_missing(monkeypatch):
    expanded_set_id = "00000000000000000000000000000011"
    missing_set_id = "00000000000000000000000000000012"
    group_id = "00000000000000000000000000000021"
    session = FakeSession(
        [
            FakeResult(rows=[{"provider_group_global_id_128": group_id, "npi": 1234567890}]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_forward": {"name": "provider_forward", "path": "/tmp/provider_forward.ptg2sc"}},
        id_storage="uuid",
    )

    async def fake_sidecar_members_many(_session, _serving_tables, sidecar_name, owner_ids, **_kwargs):
        if sidecar_name == "provider_npi":
            return {owner_id: () for owner_id in owner_ids}
        if sidecar_name == "provider_forward":
            return {expanded_set_id: (group_id,), missing_set_id: ()}
        raise AssertionError(sidecar_name)

    async def fake_enriched_provider_rows(_session, *, npis, limit, **_kwargs):
        assert npis == (1234567890,)
        assert limit == 1
        return [{"npi": 1234567890, "provider_name": "Example Provider"}]

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        fake_enriched_provider_rows,
    )

    providers_by_set = await ptg2_serving._ptg2_manifest_provider_rows_for_provider_sets(
        session,
        tables,
        [expanded_set_id, missing_set_id],
        limit_per_set=1,
    )

    assert providers_by_set == {
        expanded_set_id: [{"npi": 1234567890, "provider_name": "Example Provider"}],
        missing_set_id: [],
    }


@pytest.mark.asyncio
async def test_manifest_provider_rows_keep_authoritative_empty_v2_set(monkeypatch):
    populated_set_id = "00000000000000000000000000000011"
    empty_set_id = "00000000000000000000000000000012"
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v2",
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        },
    )

    async def fake_provider_npis(_session, _tables, provider_set_ids, **_kwargs):
        assert provider_set_ids == (populated_set_id, empty_set_id)
        return {populated_set_id: (1234567890,), empty_set_id: ()}

    async def fake_enriched_provider_rows(_session, *, npis, **_kwargs):
        assert npis == (1234567890,)
        return [{"npi": 1234567890, "provider_name": "Example Provider"}]

    async def fail_legacy_fallback(*_args, **_kwargs):
        raise AssertionError("authoritative graph membership must not use the legacy fallback")

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_npis_for_provider_sets", fake_provider_npis)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        fake_enriched_provider_rows,
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fail_legacy_fallback)

    providers_by_set = await ptg2_serving._ptg2_manifest_provider_rows_for_provider_sets(
        object(),
        tables,
        [populated_set_id, empty_set_id],
        limit_per_set=1,
    )

    assert providers_by_set == {
        populated_set_id: [{"npi": 1234567890, "provider_name": "Example Provider"}],
        empty_set_id: [],
    }


@pytest.mark.asyncio
async def test_manifest_provider_rows_for_one_authoritative_empty_v2_set(monkeypatch):
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v2",
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        },
        provider_group_member_table="mrf.ptg2_provider_group_member_legacy",
    )

    async def fake_provider_npis(*_args, **_kwargs):
        return ()

    async def fail_enrichment(*_args, **_kwargs):
        raise AssertionError("an empty authoritative provider set must not run enrichment")

    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_npis_for_provider_set",
        fake_provider_npis,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        fail_enrichment,
    )

    providers = await ptg2_serving._ptg2_manifest_provider_rows_for_provider_set(
        object(),
        tables,
        "00000000000000000000000000000012",
        limit=10,
    )

    assert providers == []


def test_authoritative_provider_membership_requires_usable_artifacts():
    name_only_tables = ptg2_serving.PTG2ServingTables(
        artifacts={
            "provider_forward": {"name": "provider_forward"},
            "provider_group_npi": {"name": "provider_group_npi"},
        }
    )
    incomplete_tables = ptg2_serving.PTG2ServingTables(
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {"name": "provider_group_npi"},
        }
    )
    db_backed_tables = ptg2_serving.PTG2ServingTables(
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        }
    )

    assert not ptg2_serving._has_authoritative_provider_membership(name_only_tables)
    assert not ptg2_serving._has_authoritative_provider_membership(incomplete_tables)
    assert ptg2_serving._has_authoritative_provider_membership(db_backed_tables)


@pytest.mark.asyncio
async def test_manifest_provider_rows_fall_back_for_incomplete_membership(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    group_id = "00000000000000000000000000000021"
    tables = ptg2_serving.PTG2ServingTables(
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {"name": "provider_group_npi"},
        },
        provider_group_member_table="mrf.ptg2_provider_group_member_legacy",
    )

    async def fake_sidecar_members(_session, _tables, name, owner_id, **_kwargs):
        assert name == "provider_forward"
        assert owner_id == provider_set_id
        return (group_id,)

    async def fake_enriched_provider_rows(_session, *, npis, limit, **_kwargs):
        assert npis == [1234567890]
        assert limit == 10
        return [{"npi": 1234567890, "provider_name": "Example Provider"}]

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_async", fake_sidecar_members)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        fake_enriched_provider_rows,
    )

    providers = await ptg2_serving._ptg2_manifest_provider_rows_for_provider_set(
        FakeSession([FakeResult(rows=[{"npi": 1234567890}])]),
        tables,
        provider_set_id,
        limit=10,
    )

    assert providers == [{"npi": 1234567890, "provider_name": "Example Provider"}]


@pytest.mark.asyncio
async def test_manifest_provider_rows_fall_back_when_direct_membership_is_unavailable(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    group_id = "00000000000000000000000000000021"
    tables = ptg2_serving.PTG2ServingTables(
        artifacts={
            "provider_npi": {"name": "provider_npi", "path": "/missing/provider_npi.ptg2sc"},
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
        },
        provider_group_member_table="mrf.ptg2_provider_group_member_legacy",
    )

    async def fake_sidecar_members(_session, _tables, name, owner_id, **_kwargs):
        assert name == "provider_forward"
        assert owner_id == provider_set_id
        return (group_id,)

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_async", fake_sidecar_members)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        AsyncMock(return_value=[{"npi": 1234567890, "provider_name": "Example Provider"}]),
    )

    providers = await ptg2_serving._ptg2_manifest_provider_rows_for_provider_set(
        FakeSession([FakeResult(rows=[{"npi": 1234567890}])]),
        tables,
        provider_set_id,
        limit=10,
    )

    assert providers == [{"npi": 1234567890, "provider_name": "Example Provider"}]


def _tin_only_serving_tables(arch_version):
    return ptg2_serving.PTG2ServingTables(
        arch_version=arch_version,
        serving_table="mrf.ptg2_serving_manifest_token",
        price_atom_table="mrf.ptg2_price_atom_manifest_token",
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        },
    )


@pytest.mark.parametrize(
    "arch_version",
    ["postgres_binary_v1", "postgres_binary_v2", "postgres_binary_v3"],
)
@pytest.mark.asyncio
async def test_manifest_serving_preserves_tin_only_rate_during_provider_expansion(
    monkeypatch,
    arch_version,
):
    """Provider expansion preserves priced TIN-only rows across binary readers."""

    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000101"
    serving_rate_row_by_field = {
        "serving_content_hash_128": "00000000000000000000000000000201",
        "plan_id": "TESTPLAN001",
        "reported_code_system": "CPT",
        "reported_code": "99214",
        "procedure_global_id_128": "00000000000000000000000000000301",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 0,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": None,
        "network_names": ["TIN only"],
    }
    session = FakeSession(
        [
            1,
            FakeResult(rows=[{"column_name": "network_names"}]),
            FakeResult(rows=[serving_rate_row_by_field]),
        ]
    )
    tables = _tin_only_serving_tables(arch_version)

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_provider_rows_for_provider_sets",
        AsyncMock(return_value={provider_set_id: []}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_prices_for_price_sets",
        AsyncMock(return_value={price_set_id: [{"negotiated_type": "fee schedule", "negotiated_rate": 114.82}]}),
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", AsyncMock(return_value={}))

    response_payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202607:test",
        {
            "plan_id": "TESTPLAN001",
            "code": "99214",
            "code_system": "CPT",
            "include_providers": "true",
        },
        _Pagination(limit=10, offset=0),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert response_payload is not None
    assert response_payload["items"][0]["npi"] is None
    assert response_payload["items"][0]["provider_expansion_status"] == "no_npi_members"
    assert response_payload["items"][0]["prices"][0]["negotiated_rate"] == 114.82


@pytest.mark.asyncio
async def test_manifest_membership_graph_resolves_provider_sets_in_both_directions(monkeypatch):
    provider_set_id = "00000000000000000000000000000011"
    group_id = "00000000000000000000000000000021"
    npi = 1234567890
    npi_member_id = ptg2_serving._ptg2_npi_member_id(npi)
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v2",
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_inverted": {
                "name": "provider_inverted",
                "storage_uri": "db://ptg2_artifact/provider-inverted-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
            "provider_npi_group": {
                "name": "provider_npi_group",
                "storage_uri": "db://ptg2_artifact/provider-npi-group-test",
            },
        }
    )

    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **_kwargs):
        members_by_sidecar = {
            "provider_forward": {provider_set_id: (group_id,)},
            "provider_inverted": {group_id: (provider_set_id,)},
            "provider_group_npi": {group_id: (npi_member_id,)},
            "provider_npi_group": {npi_member_id: (group_id,)},
        }
        return {owner_id: members_by_sidecar[sidecar_name].get(owner_id, ()) for owner_id in owner_ids}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    npis_by_set = await ptg2_serving._ptg2_manifest_provider_npis_for_provider_sets(
        object(),
        tables,
        (provider_set_id,),
    )
    provider_sets = await ptg2_serving._ptg2_manifest_provider_sets_for_npi(
        object(),
        tables,
        npi,
    )

    assert npis_by_set == {provider_set_id: (npi,)}
    assert provider_sets == (provider_set_id,)


@pytest.mark.asyncio
async def test_current_v3_ignores_obsolete_direct_provider_membership(monkeypatch):
    provider_set_id = "00000000000000000000000000000011"
    group_id = "00000000000000000000000000000021"
    current_npi = 1234567890
    stale_npi = 1098765432
    called_artifacts = []
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        artifacts={
            "provider_npi": {
                "name": "provider_npi",
                "storage_uri": "db://ptg2_artifact/obsolete-provider-npi",
            },
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        },
    )

    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **_kwargs):
        called_artifacts.append(sidecar_name)
        members_by_artifact = {
            "provider_npi": {provider_set_id: (ptg2_serving._ptg2_npi_member_id(stale_npi),)},
            "provider_forward": {provider_set_id: (group_id,)},
            "provider_group_npi": {group_id: (ptg2_serving._ptg2_npi_member_id(current_npi),)},
        }
        return {
            owner_id: members_by_artifact[sidecar_name].get(owner_id, ())
            for owner_id in owner_ids
        }

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    npis_by_set = await ptg2_serving._ptg2_manifest_provider_npis_for_provider_sets(
        object(),
        tables,
        (provider_set_id,),
    )

    assert npis_by_set == {provider_set_id: (current_npi,)}
    assert called_artifacts == ["provider_forward", "provider_group_npi"]


@pytest.mark.asyncio
async def test_current_v3_missing_provider_graphs_fail_closed():
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v3",
        provider_group_member_table="mrf.ptg2_provider_group_member_legacy",
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="reimport the snapshot"):
        await ptg2_serving._ptg2_manifest_provider_sets_for_npi(
            object(),
            tables,
            1234567890,
        )
    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="reimport the snapshot"):
        await ptg2_serving._ptg2_manifest_provider_npis_for_provider_sets(
            object(),
            tables,
            ("00000000000000000000000000000011",),
        )


@pytest.mark.asyncio
async def test_manifest_membership_graph_bounds_provider_expansion(monkeypatch):
    provider_set_id = "00000000000000000000000000000011"
    group_ids = tuple(f"{group_index:032x}" for group_index in range(1, 601))
    group_batches = []
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v2",
        artifacts={
            "provider_forward": {
                "name": "provider_forward",
                "storage_uri": "db://ptg2_artifact/provider-forward-test",
            },
            "provider_group_npi": {
                "name": "provider_group_npi",
                "storage_uri": "db://ptg2_artifact/provider-group-npi-test",
            },
        },
    )

    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **kwargs):
        if sidecar_name == "provider_forward":
            return {provider_set_id: group_ids}
        group_batches.append((tuple(owner_ids), kwargs.get("max_members")))
        return {
            group_id: (ptg2_serving._ptg2_npi_member_id(1234567890 + int(group_id, 16) - 1),)
            for group_id in owner_ids
        }

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    npis_by_set = await ptg2_serving._ptg2_manifest_provider_npis_for_provider_sets(
        object(),
        tables,
        (provider_set_id,),
        limit_per_set=2,
    )

    assert npis_by_set == {provider_set_id: (1234567890, 1234567891)}
    assert [(len(owner_ids), max_members) for owner_ids, max_members in group_batches] == [(256, 2)]


@pytest.mark.asyncio
async def test_membership_graph_uses_address_first_for_broad_codes(monkeypatch):
    matching_group_id = "00000000000000000000000000000021"
    unrelated_group_id = "00000000000000000000000000000099"
    rate_scope = ptg2_serving._ptg2_build_rate_scope(
        tuple([matching_group_id] + [f"{group_index:032x}" for group_index in range(1000, 3000)])
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test",
        artifacts={"provider_npi_group": {"name": "provider_npi_group"}},
    )
    candidate_locations = [_sample_graph_location(1234567890), _sample_graph_location(1234567891)]

    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **_kwargs):
        assert sidecar_name == "provider_npi_group"
        return {
            owner_ids[0]: (matching_group_id,),
            owner_ids[1]: (unrelated_group_id,),
        }

    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", AsyncMock(return_value=candidate_locations))
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    candidates = await ptg2_serving._graph_location_candidates(
        object(),
        tables,
        {"zip5": "49015"},
        rate_scope,
        1,
    )

    assert [location["npi"] for location in candidates.location_rows] == [1234567890]
    assert candidates.group_ids_by_npi == {1234567890: {matching_group_id}}


@pytest.mark.asyncio
async def test_membership_graph_grows_address_probe_until_a_rate_match(monkeypatch):
    matching_group_id = "00000000000000000000000000000021"
    unrelated_group_id = "00000000000000000000000000000099"
    rate_scope = ptg2_serving._ptg2_build_rate_scope(
        tuple([matching_group_id] + [f"{group_index:032x}" for group_index in range(1000, 3000)])
    )
    first_location = _sample_graph_location(1234567890)
    second_location = _sample_graph_location(1234567891)
    location_reader = AsyncMock(
        side_effect=[
            [first_location],
            [first_location, second_location],
        ]
    )

    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **_kwargs):
        assert sidecar_name == "provider_npi_group"
        matching_owner_id = ptg2_serving._ptg2_npi_member_id(1234567891)
        return {
            owner_id: (matching_group_id if owner_id == matching_owner_id else unrelated_group_id,)
            for owner_id in owner_ids
        }

    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", location_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        ptg2_serving.PTG2ServingTables(),
        {"lat": "42.3", "long": "-85.2", "radius_miles": "10"},
        rate_scope,
        1,
    )

    assert [location["npi"] for location in candidates.location_rows] == [1234567891]
    assert [call.kwargs["limit"] for call in location_reader.await_args_list] == [64, 256]
    assert [call.kwargs["offset"] for call in location_reader.await_args_list] == [0, 0]


@pytest.mark.asyncio
async def test_membership_graph_crosses_deduplicated_probe_plateau(monkeypatch):
    matching_group_id = "00000000000000000000000000000021"
    unrelated_group_id = "00000000000000000000000000000099"
    rate_scope = ptg2_serving._ptg2_build_rate_scope(
        tuple([matching_group_id] + [f"{group_index:032x}" for group_index in range(1000, 3000)])
    )
    first_location = _sample_graph_location(1234567890)
    second_location = _sample_graph_location(1234567891)
    location_reader = AsyncMock(
        side_effect=[
            [first_location],
            [first_location],
            [first_location, second_location],
        ]
    )

    async def fake_sidecar_members_many(_session, _tables, _sidecar_name, owner_ids, **_kwargs):
        matching_owner_id = ptg2_serving._ptg2_npi_member_id(1234567891)
        return {
            owner_id: (matching_group_id if owner_id == matching_owner_id else unrelated_group_id,)
            for owner_id in owner_ids
        }

    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", location_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        ptg2_serving.PTG2ServingTables(),
        {"lat": "42.3", "long": "-85.2", "radius_miles": "10"},
        rate_scope,
        1,
    )

    assert [location["npi"] for location in candidates.location_rows] == [1234567891]
    assert [call.kwargs["limit"] for call in location_reader.await_args_list] == [64, 256, 1024]


@pytest.mark.asyncio
async def test_membership_graph_exhaustion_grows_probe_logarithmically(monkeypatch):
    location_reader = AsyncMock(return_value=[_sample_graph_location(1234567890)])
    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", location_reader)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_location_match_limit", lambda: 16)

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        ptg2_serving.PTG2ServingTables(),
        {"lat": "42.3", "long": "-85.2", "radius_miles": "10"},
        ptg2_serving._ptg2_build_rate_scope(tuple(f"{value:032x}" for value in range(2000))),
        1,
    )

    assert candidates.location_rows == []
    assert [call.kwargs["limit"] for call in location_reader.await_args_list] == [64, 256, 320]


def test_membership_graph_uses_wider_first_probe_for_taxonomy_selectivity():
    assert ptg2_serving._graph_location_probe_batch_size(
        100,
        taxonomy_filter_requested=False,
    ) == 150
    assert ptg2_serving._graph_location_probe_batch_size(
        100,
        taxonomy_filter_requested=True,
    ) == 1600
    assert ptg2_serving._graph_location_probe_batch_size(
        500,
        taxonomy_filter_requested=True,
    ) == 2000


def test_membership_graph_projects_next_probe_from_observed_density():
    assert ptg2_serving._next_graph_location_probe_limit(
        1600,
        batch_size=1600,
        max_candidates=100_000,
        observed_matches=25,
        required_matches=100,
    ) == 8000
    assert ptg2_serving._next_graph_location_probe_limit(
        1600,
        batch_size=1600,
        max_candidates=100_000,
        observed_matches=0,
        required_matches=100,
    ) == 6400
    assert ptg2_serving._next_graph_location_probe_limit(
        1600,
        batch_size=1600,
        max_candidates=100_000,
        observed_matches=1,
        required_matches=100,
    ) == 12_800


@pytest.mark.asyncio
async def test_membership_graph_uses_taxonomy_selectivity_window_on_first_probe(monkeypatch):
    matching_group_id = "00000000000000000000000000000021"
    location_rows = [_sample_graph_location(1234567000 + index) for index in range(100)]
    location_reader = AsyncMock(return_value=location_rows)

    async def fake_sidecar_members_many(_session, _tables, _sidecar_name, owner_ids, **_kwargs):
        return {owner_id: (matching_group_id,) for owner_id in owner_ids}

    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", location_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_filter_npis_by_provider_taxonomy",
        AsyncMock(return_value=tuple(location["npi"] for location in location_rows)),
    )

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        ptg2_serving.PTG2ServingTables(),
        {"lat": "42.3", "long": "-85.2", "taxonomy_codes": ["207Q00000X"]},
        ptg2_serving._ptg2_build_rate_scope((matching_group_id,)),
        100,
    )

    assert len(candidates.location_rows) == 100
    assert candidates.taxonomy_filtered is True
    assert location_reader.await_args.kwargs["limit"] == 1600
    assert location_reader.await_count == 1


@pytest.mark.asyncio
async def test_membership_graph_grows_until_taxonomy_has_enough_rate_matches(monkeypatch):
    matching_group_id = "00000000000000000000000000000021"
    first_location = _sample_graph_location(1234567890)
    rejected_location = _sample_graph_location(1234567891)
    second_location = _sample_graph_location(1234567892)
    location_reader = AsyncMock(
        side_effect=[
            [first_location, rejected_location],
            [first_location, rejected_location, second_location],
        ]
    )

    async def fake_sidecar_members_many(_session, _tables, _sidecar_name, owner_ids, **_kwargs):
        return {owner_id: (matching_group_id,) for owner_id in owner_ids}

    taxonomy_filter = AsyncMock(
        side_effect=[
            (1234567890,),
            (1234567890, 1234567892),
        ]
    )
    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", location_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_filter_npis_by_provider_taxonomy",
        taxonomy_filter,
    )

    candidates = await ptg2_serving._paged_graph_candidates(
        object(),
        ptg2_serving.PTG2ServingTables(),
        {"lat": "42.3", "long": "-85.2", "taxonomy_codes": ["207Q00000X"]},
        ptg2_serving._ptg2_build_rate_scope((matching_group_id,)),
        2,
    )

    assert [location["npi"] for location in candidates.location_rows] == [1234567890, 1234567892]
    assert candidates.taxonomy_filtered is True
    assert [call.kwargs["limit"] for call in location_reader.await_args_list] == [64, 192]
    assert taxonomy_filter.await_count == 2


@pytest.mark.asyncio
async def test_taxonomy_filtered_graph_candidates_are_not_filtered_twice(monkeypatch):
    candidates = ptg2_serving._GraphLocationCandidates(
        [_sample_graph_location(1234567890)],
        {1234567890: {"00000000000000000000000000000021"}},
        taxonomy_filtered=True,
    )
    taxonomy_filter = AsyncMock(side_effect=AssertionError("taxonomy filter repeated"))
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_filter_npis_by_provider_taxonomy",
        taxonomy_filter,
    )

    result = await ptg2_serving._taxonomy_filtered_candidates(
        object(),
        {"taxonomy_codes": ["207Q00000X"]},
        candidates,
        1,
    )

    assert result is candidates
    taxonomy_filter.assert_not_awaited()


@pytest.mark.asyncio
async def test_membership_location_query_keeps_site_addresses(monkeypatch):
    tables = ptg2_serving.PTG2ServingTables(provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test")
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )

    location_query = await ptg2_serving._membership_location_query(
        object(),
        tables,
        {"zip5": "49015"},
        candidate_npis=(1234567890,),
        limit=10,
    )
    invalid_radius_query = await ptg2_serving._membership_location_query(
        object(),
        tables,
        {"radius_miles": "10"},
        candidate_npis=(1234567890,),
        limit=10,
    )

    assert "site" in location_query.parameter_map["address_types"]
    assert location_query.knn_order_sql is None
    assert invalid_radius_query is None


@pytest.mark.asyncio
async def test_membership_location_rows_uses_postgis_nearest_neighbor_for_coordinate_probe(monkeypatch):
    session = FakeSession([FakeResult(rows=[])])
    tables = ptg2_serving.PTG2ServingTables(
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test"
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )

    location_rows = await ptg2_serving._membership_location_rows(
        session,
        tables,
        {"lat": "42.3", "long": "-85.2", "radius_miles": "10"},
        candidate_npis=None,
        limit=100,
    )

    assert location_rows == []
    planner_setup_sql = str(session.calls[0][0][0])
    location_statement_sql = str(session.calls[1][0][0])
    parameter_map = session.calls[1][0][1]
    planner_restore_sql = str(session.calls[2][0][0])
    restore_parameter_map = session.calls[2][0][1]
    assert "set_config('plan_cache_mode', 'force_custom_plan', true)" in planner_setup_sql
    assert "set_config('max_parallel_workers_per_gather', '0', true)" in planner_setup_sql
    assert "set_config('plan_cache_mode', :plan_cache_mode, true)" in planner_restore_sql
    assert restore_parameter_map == {"plan_cache_mode": "auto", "parallel_workers": "2"}
    assert "nearest_addresses AS MATERIALIZED" in location_statement_sql
    assert "entity_address_unified_idx_geo_idx" not in location_statement_sql
    assert "Geography(ST_MakePoint((addr.long)::double precision" in location_statement_sql
    assert "<-> Geography(ST_MakePoint" in location_statement_sql
    assert "ST_DWithin(" in location_statement_sql
    assert "addr.type IN ('primary', 'secondary', 'practice', 'site')" in location_statement_sql
    assert "addr.type = ANY" not in location_statement_sql
    assert "ROW_NUMBER() OVER" in location_statement_sql
    assert parameter_map["probe_limit"] == 164


@pytest.mark.asyncio
async def test_membership_location_rows_does_not_mask_query_failure_with_restore(monkeypatch):
    session = FakeSession([RuntimeError("location query failed")])
    tables = ptg2_serving.PTG2ServingTables(
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test"
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )

    with pytest.raises(RuntimeError, match="location query failed"):
        await ptg2_serving._membership_location_rows(
            session,
            tables,
            {"lat": "42.3", "long": "-85.2", "radius_miles": "10"},
            candidate_npis=None,
            limit=100,
        )

    assert len(session.calls) == 2
    assert "set_config('plan_cache_mode', :plan_cache_mode, true)" not in str(session.calls[-1][0][0])


@pytest.mark.asyncio
async def test_membership_location_knn_defers_taxonomy_until_after_indexed_probe(monkeypatch):
    tables = ptg2_serving.PTG2ServingTables(
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test"
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )

    location_query = await ptg2_serving._membership_location_query(
        object(),
        tables,
        {
            "lat": "42.3",
            "long": "-85.2",
            "radius_miles": "10",
            "taxonomy_codes": ["207Q00000X"],
            "primary_only": True,
        },
        candidate_npis=None,
        limit=200,
    )

    assert location_query.knn_order_sql is not None
    assert "membership_location_specialty" not in location_query.filter_sql
    assert "membership_location_inferred_taxonomy" not in location_query.filter_sql
    assert "npi_taxonomy" not in location_query.filter_sql
    assert not any("taxonomy" in parameter_name for parameter_name in location_query.parameter_map)


@pytest.mark.asyncio
async def test_membership_location_query_filters_taxonomy_before_candidate_limit(monkeypatch):
    tables = ptg2_serving.PTG2ServingTables(provider_npi_scope_table="mrf.ptg2_provider_npi_scope_test")
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_address_serving_table",
        AsyncMock(return_value="mrf.entity_address_unified"),
    )

    location_query = await ptg2_serving._membership_location_query(
        object(),
        tables,
        {
            "zip5": "60654",
            "taxonomy_codes": ["207Q00000X"],
            "primary_only": True,
        },
        candidate_npis=(1234567890, 1234567891),
        limit=200,
    )

    assert "addr.npi IN (SELECT" in location_query.filter_sql
    assert "membership_location_specialty_nt.npi" in location_query.filter_sql
    assert "healthcare_provider_primary_taxonomy_switch" in location_query.filter_sql
    assert location_query.parameter_map["membership_location_specialty_taxonomy_code_0"] == "207Q00000X"
    assert location_query.parameter_map["limit"] == 200


def test_graph_provider_data_preserves_street_fallback():
    enriched_address = json.dumps({"first_line": "100 Test Street", "city": "BATTLE CREEK"})
    matched_address = json.dumps({"first_line": None, "city": "BATTLE CREEK"})

    provider_data = ptg2_serving._graph_provider_data(
        {
            "npi": 1234567890,
            "state": "MI",
            "city": "BATTLE CREEK",
            "zip5": "49015",
            "distance_miles": 2.5,
            "location_hash": "unified-location",
            "address_payload": matched_address,
        },
        {
            "npi": 1234567890,
            "state": "MI",
            "city": "BATTLE CREEK",
            "zip5": "49015",
            "address_payload": enriched_address,
        },
        "entity_address_unified",
    )

    assert provider_data["address_payload"] == enriched_address
    assert provider_data["distance_miles"] == 2.5
    assert provider_data["location_hash"] == "unified-location"


def _sample_graph_location(npi):
    return {
        "npi": npi,
        "state": "MI",
        "city": "BATTLE CREEK",
        "zip5": "49015",
        "distance_miles": None,
        "location_hash": "location-1",
        "address_payload": "{}",
    }


@pytest.mark.asyncio
async def test_manifest_membership_graph_resolves_geo_provider_sets(monkeypatch):
    """Graph geo traversal preserves provider-set membership and enrichment."""
    provider_set_id = "00000000000000000000000000000011"
    group_id = "00000000000000000000000000000021"
    npi = 1234567890
    tables = ptg2_serving.PTG2ServingTables(
        arch_version="postgres_binary_v2",
        source_key="ptg_graph",
        provider_npi_scope_table="mrf.ptg2_provider_npi_scope_graph",
        artifacts={
            "provider_forward": {"name": "provider_forward"},
            "provider_inverted": {"name": "provider_inverted"},
            "provider_group_npi": {"name": "provider_group_npi"},
            "provider_npi_group": {"name": "provider_npi_group"},
        },
    )
    location_rows = [_sample_graph_location(npi)]
    async def fake_sidecar_members_many(_session, _tables, sidecar_name, owner_ids, **_kwargs):
        members_by_sidecar = {
            "provider_group_npi": {group_id: (ptg2_serving._ptg2_npi_member_id(npi),)},
            "provider_inverted": {group_id: (provider_set_id,)},
        }
        return {owner_id: members_by_sidecar[sidecar_name].get(owner_id, ()) for owner_id in owner_ids}
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_scope_from_sidecar",
        AsyncMock(return_value=ptg2_serving._ptg2_build_rate_scope((group_id,))),
    )
    monkeypatch.setattr(ptg2_serving, "_membership_location_rows", AsyncMock(return_value=location_rows))
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_members_many)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_filter_npis_by_provider_taxonomy",
        AsyncMock(return_value=(npi,)),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_enriched_provider_rows_for_npis",
        AsyncMock(return_value=[{"npi": npi, "provider_name": "Example Provider"}]),
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.entity_address_unified"))
    provider_set_ids, providers_by_set = await ptg2_serving._graph_location_matches(
        object(),
        tables,
        {"code": "S4011", "code_system": "HCPCS", "zip5": "49015"},
        candidate_limit=10,
        plan_id="382418014",
    )
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == npi
    assert providers_by_set[provider_set_id][0]["provider_name"] == "Example Provider"
    assert providers_by_set[provider_set_id][0]["zip5"] == "49015"


@pytest.mark.asyncio
async def test_manifest_serving_taxonomy_expansion_uses_wider_rate_candidate_window(monkeypatch):
    provider_sets = [f"{idx:032x}" for idx in range(1, 6)]
    price_sets = [f"{idx:032x}" for idx in range(101, 106)]
    rows = [
        {
            "serving_content_hash_128": f"{idx + 201:032x}",
            "plan_id": "TESTPLAN001",
            "reported_code_system": "CPT",
            "reported_code": "99214",
            "procedure_global_id_128": f"{idx + 301:032x}",
            "provider_set_global_id_128": provider_sets[idx],
            "provider_count": 100 - idx,
            "price_set_global_id_128": price_sets[idx],
            "source_trace_set_hash": None,
            "network_names": ["C2"],
        }
        for idx in range(5)
    ]
    session = FakeSession([5, FakeResult(rows=[{"column_name": "network_names"}]), FakeResult(rows=rows)])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_token",
        price_atom_table="mrf.ptg2_price_atom_manifest_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_manifest_token",
    )

    class LimitOnePagination:
        limit = 1
        offset = 0

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        assert tuple(price_set_ids) == tuple(price_sets)
        return {
            price_set_id: [{"negotiated_type": "fee schedule", "negotiated_rate": 114.82}]
            for price_set_id in price_set_ids
        }

    async def fake_providers(_session, _tables, provider_set_ids, *, limit_per_set, args):
        assert tuple(provider_set_ids) == tuple(provider_sets)
        assert limit_per_set == 1
        assert args["specialty"] == "Family Medicine"
        return {
            provider_sets[1]: [
                {
                    "npi": 1851399604,
                    "provider_name": "Family Medicine Provider",
                    "state": "FL",
                    "city": "Jacksonville",
                    "zip5": "32210",
                    "taxonomy_codes": ["207Q00000X"],
                    "specialties": ["Family Medicine Physician"],
                }
            ]
        }

    async def fake_procedure_details(_session, row_data):
        assert [row["provider_set_global_id_128"] for row in row_data] == provider_sets
        return {("CPT", "99214"): {"procedure_name": "Office/outpatient visit"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_rows_for_provider_sets", fake_providers)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "99214",
                "code_system": "CPT",
                "include_providers": "true",
                "include_sources": "true",
                "specialty": "Family Medicine",
            },
        LimitOnePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert len(payload["items"]) == 1
    assert payload["items"][0]["npi"] == 1851399604
    assert payload["items"][0]["network_names"] == ["C2"]
    assert payload["items"][0]["taxonomy_codes"] == ["207Q00000X"]
    assert payload["pagination"]["limit"] == 1
    assert payload["query"]["plan_market_type"] == "group"
    row_sql = str(session.calls[2][0][0])
    row_params = session.calls[2][0][1]
    assert "LIMIT :rate_candidate_limit" in row_sql
    assert row_params["limit"] == 1
    assert row_params["rate_candidate_limit"] > row_params["limit"]
    assert row_params["rate_candidate_limit"] <= ptg2_serving._PTG2_MANIFEST_TAXONOMY_RATE_CANDIDATE_LIMIT


@pytest.mark.asyncio
async def test_manifest_serving_geo_expansion_uses_wider_location_candidate_window(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000101"
    row = {
        "serving_content_hash_128": "00000000000000000000000000000201",
        "plan_id": "TESTPLAN001",
        "reported_code_system": "CPT",
        "reported_code": "29888",
        "procedure_global_id_128": "00000000000000000000000000000301",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 335,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": None,
        "network_names": ["C2"],
    }
    session = FakeSession([FakeResult(rows=[{"column_name": "network_names"}]), FakeResult(rows=[row])])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_token",
        price_atom_table="mrf.ptg2_price_atom_manifest_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_manifest_token",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )

    class LimitOnePagination:
        limit = 1
        offset = 0

    seen_candidate_limit = {}

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_location_matches(
        _session,
        _tables,
        args,
        *,
        candidate_limit=None,
        plan_id=None,
        snapshot_id=None,
        source_key=None,
    ):
        assert args["zip5"] == "62401"
        assert plan_id == "TESTPLAN001"
        assert snapshot_id == "ptg2:202606:test"
        assert source_key is None
        seen_candidate_limit["value"] = candidate_limit
        return {
            provider_set_id,
        }, {
            provider_set_id: [
                {
                    "npi": 1154321222,
                    "provider_name": "ACL Surgeon",
                    "state": "IL",
                    "city": "PANA",
                    "zip5": "62557",
                    "telephone_number": "2175551212",
                    "fax_number": "2175551213",
                    "address_payload": '{"telephone_number":"2175551212","fax_number":"2175551213"}',
                    "taxonomy_codes": ["207XS0114X"],
                    "specialties": ["Orthopaedic Surgery Physician"],
                    "classifications": ["Orthopaedic Surgery"],
                    "specializations": ["Sports Medicine"],
                    "primary_specialty": "Orthopaedic Surgery Physician",
                    "primary_specialization": "Sports Medicine",
                }
            ]
        }

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        assert tuple(price_set_ids) == (price_set_id,)
        return {price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": 1074.22}]}

    async def fake_procedure_details(_session, row_data):
        assert [row["provider_set_global_id_128"] for row in row_data] == [provider_set_id]
        return {("CPT", "29888"): {"procedure_name": "ACL reconstruction"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_location_provider_matches", fake_location_matches)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "29888",
            "code_system": "CPT",
            "zip5": "62401",
            "lat": "39.11952",
            "long": "-88.56418",
                "radius_miles": "100",
                "include_providers": "true",
                "include_sources": "true",
                "include_unverified_addresses": "true",
            },
        LimitOnePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert len(payload["items"]) == 1
    assert payload["items"][0]["npi"] == 1154321222
    assert payload["items"][0]["network_names"] == ["C2"]
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    assert payload["items"][0]["phone"] == "2175551212"
    assert payload["items"][0]["phone_number"] == "2175551212"
    assert payload["items"][0]["telephone_number"] == "2175551212"
    assert payload["items"][0]["fax_number"] == "2175551213"
    assert payload["items"][0]["address"]["telephone_number"] == "2175551212"
    assert seen_candidate_limit["value"] == 100
    row_sql = str(session.calls[1][0][0])
    row_params = session.calls[1][0][1]
    assert "LIMIT :rate_candidate_limit" in row_sql
    assert row_params["limit"] == 1
    assert row_params["rate_candidate_limit"] == seen_candidate_limit["value"]
    assert row_params["rate_candidate_limit"] > row_params["limit"]


def test_manifest_location_candidate_window_defaults_to_double_page_floor(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_OVERFETCH_CAP", raising=False)
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_FLOOR", raising=False)

    class LimitFiftyPagination:
        limit = 50
        offset = 0

    assert (
        ptg2_serving._ptg2_manifest_rate_candidate_limit(
            {},
            LimitFiftyPagination(),
            expand_providers=True,
            location_filter_requested=True,
        )
        == 100
    )

    class LimitTwoHundredPagination:
        limit = 200
        offset = 0

    assert (
        ptg2_serving._ptg2_manifest_rate_candidate_limit(
            {},
            LimitTwoHundredPagination(),
            expand_providers=True,
            location_filter_requested=True,
        )
        == 300
    )


def test_manifest_location_candidate_window_honors_env_multiplier(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_MATCH_LIMIT", raising=False)
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_MULTIPLIER", "3")
    monkeypatch.delenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_OVERFETCH_CAP", raising=False)
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_LOCATION_CANDIDATE_FLOOR", "25")

    class LimitFiftyPagination:
        limit = 50
        offset = 0

    assert (
        ptg2_serving._ptg2_manifest_rate_candidate_limit(
            {},
            LimitFiftyPagination(),
            expand_providers=True,
            location_filter_requested=True,
        )
        == 150
    )

    class LimitTwoHundredPagination:
        limit = 200
        offset = 0

    assert (
        ptg2_serving._ptg2_manifest_rate_candidate_limit(
            {},
            LimitTwoHundredPagination(),
            expand_providers=True,
            location_filter_requested=True,
        )
        == 600
    )


@pytest.mark.asyncio
async def test_manifest_enriched_provider_fallback_includes_taxonomy(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(
        [
            False,
            False,
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "taxonomy_codes": ["207Q00000X"],
                        "specialties": ["Family Medicine"],
                    }
                ]
            ),
        ]
    )

    rows = await ptg2_serving._ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=[1234567890],
        limit=5,
    )

    assert rows == [
        {
            "npi": 1234567890,
            "provider_name": "TiC provider",
            "taxonomy_codes": ["207Q00000X"],
            "specialties": ["Family Medicine"],
            "classifications": [],
            "specializations": [],
            "primary_specialty": None,
            "primary_specialization": None,
        }
    ]
    assert "FROM mrf.npi_taxonomy nt" in str(session.calls[2][0][0])


@pytest.mark.asyncio
async def test_manifest_enriched_provider_unified_fallback_uses_bounded_cte(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    rows = await ptg2_serving._ptg2_manifest_enriched_provider_rows_for_npis(
        session,
        npis=[1234567890],
        limit=5,
    )

    assert rows == []
    sql = str(session.calls[-1][0][0])
    assert "source_npis AS MATERIALIZED" in sql
    assert "fallback_addresses AS MATERIALIZED" in sql
    assert "JOIN source_npis source_filter ON source_filter.npi = na.npi" in sql
    assert "LEFT JOIN fallback_addresses na" in sql
    assert "na.phone_number" in sql
    assert "addr.phone_number" in sql
    assert "'fax_number_digits'" in sql
    assert "json_build_object(\n                (" not in sql
    assert "LEFT JOIN LATERAL (\n                SELECT na.first_line" not in sql


@pytest.mark.asyncio
async def test_ptg2_address_serving_table_prefers_unified_by_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_SERVING_SOURCE", raising=False)
    session = FakeSession(
        [FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)])]
    )

    table_name = await ptg2_serving._ptg2_address_serving_table(
        session,
        ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS,
    )

    assert table_name == "mrf.entity_address_unified"


@pytest.mark.asyncio
async def test_ptg2_address_serving_table_uses_legacy_when_explicit(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([])

    table_name = await ptg2_serving._ptg2_address_serving_table(
        session,
        ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS,
    )

    assert table_name == "mrf.npi_address"
    assert session.calls == []


def test_ptg2_code_details_split_keeps_serving_facade_helper_stable():
    assert ptg2_serving._enrich_ptg2_code_details is ptg2_code_details._enrich_ptg2_code_details


def _compact_tables(**overrides):
    values = {
        "serving_table": "mrf.ptg2_serving_rate_compact_token",
        "price_code_set_table": "mrf.ptg2_price_code_set_token",
        "price_atom_table": "mrf.ptg2_price_atom_token",
        "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
        "procedure_table": "mrf.ptg2_procedure_token",
        "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
        "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
        "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
        "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
    }
    values.update(overrides)
    return ptg2_serving.PTG2ServingTables(**values)


def _db_serving_session():
    return FakeSession(
        [
            FakeResult(rows=[("example_network", "snap-db")]),
            {"table": "mrf.ptg2_serving_rate"},
            "mrf.ptg2_serving_rate",
            1,
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-db",
                        "plan_id": "TESTPLAN001",
                        "plan_name": "Example Plan",
                        "procedure_code": 123456,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "provider_set_hash": "provider-set-1",
                        "provider_set_hashes": ["provider-set-a"],
                        "provider_count": 123,
                        "provider_set_count": 1,
                        "price_set_hash": "price-set-1",
                        "rate_pack_hash": "pack-1",
                        "prices": [
                            {
                                "negotiated_type": "negotiated",
                                "negotiated_rate": 450,
                                "service_code": ["23"],
                                "billing_code_modifier": ["TC", "ZZ"],
                            }
                        ],
                        "source_trace": [
                            {
                                "source_file_version_id": "source-version-db-row",
                                "original_url": "https://example.test/rates.json.gz",
                            }
                        ],
                        "network_names": ["C2"],
                        "confidence": {"network": "tic_rate_npi_tin"},
                    }
                ]
            ),
        ]
    )


def _fixture_payload():
    return {
        "version": 1,
        "snapshot_id": "snap-fixture",
        "plans": {"TESTPLAN001": {"name": "Example Group"}},
        "procedures": {
            "70551": {
                "code": "70551",
                "billing_code": "70551",
                "billing_code_type": "CPT",
                "name": "MRI brain",
            }
        },
        "providers": {
            "1": {
                "provider_ordinal": 1,
                "npi": 1234567890,
                "provider_name": "Example Imaging",
                "city": "Peoria",
                "state": "IL",
                "zip5": "61636",
                "location_source": "npi_address",
                "location_confidence_code": "npi_address",
                "address_payload": {
                    "first_line": "1 Main St",
                    "city_name": "Peoria",
                    "state_name": "IL",
                    "postal_code": "61636",
                    "address_sources": ["nppes"],
                    "address_source_mask": 1,
                },
            }
        },
        "rates": {
            "TESTPLAN001": {
                "70551": [
                    {
                        "provider_ordinal": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 450}],
                        "network_names": ["C2"],
                        "source_trace": [
                            {
                                "source_file_version_id": "source-version-fixture",
                                "original_url": "https://example.test/tic.json.gz",
                            }
                        ],
                    }
                ]
            }
        },
    }


def test_search_ptg2_index_returns_prices_and_source_trace():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    payload = ptg2_serving.search_ptg2_index(index, plan_id="TESTPLAN001", code="70551", state="IL")

    assert payload["pagination"]["total"] == 1
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["network_names"] == ["C2"]
    assert item["source_trace"][0]["source_file_version_id"] == "source-version-fixture"
    assert item["source_trace"][0]["original_url"] == "https://example.test/tic.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["address_evidence_level"] == "nppes_provider_address"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert payload["query"]["source"] == "ptg2"


def test_db_serving_code_filter_accepts_signed_hp_procedure_codes():
    filters = []
    params = {}

    ptg2_serving._append_code_filter(
        filters,
        params,
        code="-1201887592",
        code_system="HP_PROCEDURE_CODE",
    )

    assert filters == ["procedure_code = :procedure_code"]
    assert params["procedure_code"] == -1201887592


@pytest.mark.asyncio
async def test_ptg2_code_context_bridges_cpt_and_hcpcs_same_code():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="70551",
        code_system="CPT",
    )

    assert context["input_code"] == {"code_system": "CPT", "code": "70551"}
    assert {"code_system": "CPT", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "CDT", "code": "70551"} not in context["resolved_codes"]
    assert context["internal_codes"] == []


@pytest.mark.asyncio
async def test_ptg2_code_context_bridges_cdt_and_hcpcs_dental_code():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="D0120",
        code_system="CDT",
    )

    assert context["input_code"] == {"code_system": "CDT", "code": "D0120"}
    assert {"code_system": "CDT", "code": "D0120"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "D0120"} in context["resolved_codes"]
    assert {"code_system": "CPT", "code": "D0120"} not in context["resolved_codes"]
    assert context["internal_codes"] == []


@pytest.mark.asyncio
async def test_ptg2_code_context_keeps_ms_drg_exact():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([FakeResult(rows=[])]),
        code="47",
        code_system="DRG",
    )

    assert context["input_code"] == {"code_system": "MS_DRG", "code": "047"}
    assert context["resolved_codes"] == [{"code_system": "MS_DRG", "code": "047"}]
    assert context["internal_codes"] == []

    filters = []
    params = {}
    ptg2_serving._append_resolved_code_filter(
        filters,
        params,
        code="47",
        code_system="DRG",
        code_context=context,
    )

    assert "reported_code_system_0_0" in params
    assert params["reported_code_system_0_0"] == "MS_DRG"
    assert params["reported_code_0"] == "047"


@pytest.mark.asyncio
async def test_ptg2_code_context_expands_internal_code_crosswalk():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession(
            [
                FakeResult(
                    rows=[
                        {
                            "from_system": "CPT",
                            "from_code": "70551",
                            "to_system": "HP_PROCEDURE_CODE",
                            "to_code": "123456",
                            "match_type": "exact",
                            "confidence": 1.0,
                            "source": "test",
                        }
                    ]
                ),
                FakeResult(rows=[]),
            ]
        ),
        code="70551",
        code_system="CPT",
    )

    assert {"code_system": "CPT", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HCPCS", "code": "70551"} in context["resolved_codes"]
    assert {"code_system": "HP_PROCEDURE_CODE", "code": "123456"} in context["resolved_codes"]
    assert context["internal_codes"] == [123456]
    assert context["matched_via"][0]["source"] == "test"


@pytest.mark.asyncio
async def test_ptg2_code_context_matches_raw_and_canonical_revenue_codes():
    context = await ptg2_serving._resolve_ptg2_code_search_context(
        FakeSession([]),
        code="110",
        code_system="RC",
    )
    filters = []
    params_by_name = {}

    ptg2_serving._append_resolved_code_filter(
        filters,
        params_by_name,
        code="110",
        code_system="RC",
        code_context=context,
    )

    assert context is None
    assert "reported_code_system = :reported_code_system" in filters[0]
    assert "reported_code IN (:reported_code, :reported_code_1)" in filters[0]
    assert params_by_name["reported_code_system"] == "RC"
    assert params_by_name["reported_code"] == "0110"
    assert params_by_name["reported_code_1"] == "110"


def test_manifest_code_filter_matches_revenue_code_forms():
    filters = []
    params_by_name = {}

    ptg2_serving._append_manifest_reported_code_filter(
        filters,
        params_by_name,
        code="0110",
        code_system="RC",
    )

    assert "reported_code_system = :reported_code_system_0" in filters[0]
    assert "reported_code = :reported_code_0" in filters[0]
    assert "reported_code_system = :reported_code_system_1" in filters[0]
    assert "reported_code = :reported_code_1" in filters[0]
    assert params_by_name["reported_code_system_0"] == "RC"
    assert params_by_name["reported_code_0"] == "0110"
    assert params_by_name["reported_code_system_1"] == "RC"
    assert params_by_name["reported_code_1"] == "110"


def test_preferred_code_rows_select_plan_scope_per_code_form():
    preferred_rows = ptg2_serving._preferred_code_metadata_rows(
        [
            {"code_key": 1, "plan_id": "", "reported_code_system": "RC", "reported_code": "0110"},
            {
                "code_key": 2,
                "plan_id": "TESTPLAN001",
                "reported_code_system": "RC",
                "reported_code": "0110",
            },
            {"code_key": 3, "plan_id": "", "reported_code_system": "RC", "reported_code": "110"},
        ],
        "TESTPLAN001",
    )

    assert [row["code_key"] for row in preferred_rows] == [2, 3]


@pytest.mark.asyncio
async def test_v3_reverse_matches_revenue_code_forms():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "RC",
                        "reported_code": "110",
                        "rate_count": 1,
                    }
                ]
            )
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(code_count_table="mrf.ptg2_code_count_fixture")

    metadata_rows = await ptg2_serving._ptg2_manifest_code_rows_for_provider_reverse(
        session,
        tables,
        requested_plan="TESTPLAN001",
        code_value="110",
        code_system="RC",
        q_text="",
        code_context=None,
    )

    assert metadata_rows[0]["reported_code"] == "110"
    sql = str(session.calls[0][0][0])
    query_params_by_name = session.calls[0][0][1]
    assert "reported_code = :reported_code_0" in sql
    assert "reported_code = :reported_code_1" in sql
    assert query_params_by_name["reported_code_0"] == "0110"
    assert query_params_by_name["reported_code_1"] == "110"


@pytest.mark.asyncio
async def test_ptg2_serving_table_uses_equivalent_cpt_hcpcs_filter_for_compact_search():
    session = FakeSession(
        [
            FakeResult(rows=[]),
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
        ]
    )

    await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551", "code_system": "CPT"},
        FakePagination(),
        serving_tables=_compact_tables(),
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "r.reported_code = :reported_code_0" in sql
    assert "r.reported_code_system IN (:reported_code_system_0_0, :reported_code_system_0_1)" in sql
    assert set(params[key] for key in ("reported_code_system_0_0", "reported_code_system_0_1")) == {"CPT", "HCPCS"}
    assert params["reported_code_0"] == "70551"


@pytest.mark.asyncio
async def test_ptg2_serving_table_uses_equivalent_cdt_hcpcs_filter_for_compact_search():
    session = FakeSession(
        [
            FakeResult(rows=[]),
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
        ]
    )

    await ptg2_serving.search_ptg2_serving_table(
        session,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "D0120", "code_system": "CDT"},
        FakePagination(),
        serving_tables=_compact_tables(),
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "r.reported_code = :reported_code_0" in sql
    assert "r.reported_code_system IN (:reported_code_system_0_0, :reported_code_system_0_1)" in sql
    assert set(params[key] for key in ("reported_code_system_0_0", "reported_code_system_0_1")) == {"CDT", "HCPCS"}
    assert params["reported_code_0"] == "D0120"


def test_price_summary_groups_component_rates_and_counts_raw_prices():
    prices = [
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": "83.09",
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["22", "21"],
            "billing_code_modifier": ["26"],
            "negotiated_rate": 83.09,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": ["TC"],
            "negotiated_rate": 158.58,
            "negotiated_type": "negotiated",
        },
        {
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "billing_code_modifier": [],
            "negotiated_rate": 241.67,
            "negotiated_type": "negotiated",
        },
    ]

    normalized = ptg2_serving._normalize_price_payload(prices)
    summary = ptg2_serving._summarize_price_payload(prices)

    assert len(normalized) == 3
    assert normalized[0]["service_code"] == ["21", "22"]
    assert normalized[0]["billing_code_modifier"] == ["26"]
    assert summary == [
        {
            "component": "global",
            "modifier": [],
            "rate": 241.67,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
        {
            "component": "professional",
            "modifier": ["26"],
            "rate": 83.09,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21", "22"],
            "raw_price_count": 1,
        },
        {
            "component": "technical",
            "modifier": ["TC"],
            "rate": 158.58,
            "negotiated_type": "negotiated",
            "billing_class": "professional",
            "setting": "inpatient",
            "service_code": ["21"],
            "raw_price_count": 1,
        },
    ]


@pytest.mark.asyncio
async def test_load_current_ptg2_index_reads_snapshot_artifact(tmp_path):
    artifact = tmp_path / "snapshot.json"
    artifact.write_text(json.dumps(_fixture_payload()), encoding="utf-8")
    ptg2_serving.clear_ptg2_index_cache()
    session = FakeSession(["snap-fixture", artifact.resolve().as_uri()])

    index = await ptg2_serving.load_current_ptg2_index(session)

    assert index is not None
    assert index.snapshot_id == "snap-fixture"
    assert "TESTPLAN001" in index.rates


@pytest.mark.asyncio
async def test_search_current_ptg2_index_reads_db_serving_table():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "TESTPLAN001", "code": "70551"},
        FakePagination(),
    )

    assert "source" not in payload["query"]
    assert "serving_table" not in payload["query"]
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["procedure_code"] == 123456
    assert item["service_code"] == "70551"
    assert item["reported_code_system"] == "CPT"
    assert item["tic_prices"][0]["negotiated_rate"] == 450
    assert item["provider_count"] == 123
    assert "source_trace" not in item
    assert "snapshot_id" not in item
    assert "network_names" not in item
    assert "confidence" not in item
    assert "price_set_hash" not in item
    assert "provider_set_hash" not in item
    assert item["address_verification"]["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["displayed_address_present"] is False


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_sources_without_debug_fields():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "TESTPLAN001", "code": "70551", "include_sources": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["serving_table"] == "mrf.ptg2_serving_rate"
    assert "procedure_consolidation" not in payload["query"]
    item = payload["items"][0]
    assert item["source_trace"][0]["source_file_version_id"] == "source-version-db-row"
    assert item["source_trace"][0]["original_url"] == "https://example.test/rates.json.gz"
    assert item["snapshot_id"] == "snap-db"
    assert item["network_names"] == ["C2"]
    assert "confidence" not in item
    assert "price_set_hash" not in item
    assert item["address_verification"]["rate_network_binding"] == "tic_provider_group_npi_tin"
    row_sql = "\n".join(str(call[0][0]) for call in session.calls if call[0])
    assert "ptg2_source_trace_set" in row_sql
    assert "ptg2_source_trace" in row_sql
    assert "'source_file_version_id', st.source_file_version_id" in row_sql
    assert "sts.source_trace_set_hash = r.source_trace_set_hash" in row_sql


@pytest.mark.asyncio
async def test_manifest_db_serving_hydrates_source_trace_from_trace_set(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000101"
    rate_pack_id = "00000000000000000000000000000201"
    row = {
        "serving_content_hash_128": rate_pack_id,
        "plan_id": "TESTPLAN001",
        "reported_code_system": "CPT",
        "reported_code": "29888",
        "procedure_global_id_128": "00000000000000000000000000000301",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 2,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": "trace-set-1",
        "network_names": ["C2"],
    }
    session = FakeSession(
        [
            1,
            FakeResult(rows=[row]),
            FakeResult(
                rows=[
                    {
                        "source_trace_set_hash": "trace-set-1",
                        "source_trace": [
                            {
                                "source_file_version_id": "source-version-1",
                                "original_url": "https://payer.example.invalid/mrf/rates.json.gz",
                            }
                        ],
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_manifest_token")

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_has_columns(_session, _table_name, required_columns):
        assert required_columns == {"network_names"}
        return True

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        assert price_set_ids == [price_set_id]
        return {price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": 1138.57}]}

    async def fake_procedure_details(_session, row_data):
        assert row_data[0]["source_trace_set_hash"] == "trace-set-1"
        return {("CPT", "29888"): {"procedure_name": "ACL reconstruction"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_table_has_columns", fake_has_columns)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "29888",
            "code_system": "CPT",
            "include_sources": "true",
        },
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    item = payload["items"][0]
    assert item["source_trace"][0]["source_file_version_id"] == "source-version-1"
    assert item["source_trace"][0]["original_url"] == "https://payer.example.invalid/mrf/rates.json.gz"
    assert item["address_verification"]["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert any("ptg2_source_trace_set" in str(call[0][0]) for call in session.calls if call[0])


@pytest.mark.asyncio
async def test_manifest_db_serving_skips_source_trace_for_compact_response(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    price_set_id = "00000000000000000000000000000101"
    row = {
        "serving_content_hash_128": "00000000000000000000000000000201",
        "plan_id": "TESTPLAN001",
        "reported_code_system": "CPT",
        "reported_code": "29888",
        "procedure_global_id_128": "00000000000000000000000000000301",
        "provider_set_global_id_128": provider_set_id,
        "provider_count": 2,
        "price_set_global_id_128": price_set_id,
        "source_trace_set_hash": "trace-set-1",
        "network_names": ["C2"],
    }
    session = FakeSession([1, FakeResult(rows=[row])])
    tables = ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_manifest_token")

    async def fake_available(_session, table_name):
        assert table_name == "mrf.ptg2_serving_manifest_token"
        return True

    async def fake_has_columns(_session, _table_name, required_columns):
        assert required_columns == {"network_names"}
        return True

    async def fake_prices(_session, _tables, price_set_ids, **_kwargs):
        assert price_set_ids == [price_set_id]
        return {price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": 1138.57}]}

    async def fake_procedure_details(_session, row_data):
        assert row_data[0]["source_trace_set_hash"] == "trace-set-1"
        return {("CPT", "29888"): {"procedure_name": "ACL reconstruction"}}

    monkeypatch.setattr(ptg2_serving, "_serving_table_available", fake_available)
    monkeypatch.setattr(ptg2_serving, "_ptg2_table_has_columns", fake_has_columns)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", fake_prices)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", fake_procedure_details)
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_source_traces_for_trace_sets",
        AsyncMock(side_effect=AssertionError("compact responses should not hydrate source traces")),
    )

    payload = await ptg2_serving._search_ptg2_manifest_db_serving_table(
        session,
        "ptg2:202606:test",
        {
            "plan_id": "TESTPLAN001",
            "market_type": "group",
            "code": "29888",
            "code_system": "CPT",
        },
        FakePagination(),
        tables,
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert "source_trace" not in payload["items"][0]
    assert not any("ptg2_source_trace_set" in str(call[0][0]) for call in session.calls if call[0])


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_full_details():
    session = _db_serving_session()

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "TESTPLAN001", "code": "70551", "include_details": "true"},
        FakePagination(),
    )

    assert payload["query"]["source"] == "ptg2_db"
    assert payload["query"]["procedure_consolidation"] == "HP_PROCEDURE_CODE"
    item = payload["items"][0]
    assert item["source_trace"][0]["source_file_version_id"] == "source-version-db-row"
    assert item["source_trace"][0]["original_url"] == "https://example.test/rates.json.gz"
    assert item["confidence"]["network"] == "tic_rate_npi_tin"
    assert item["price_set_hash"] == "price-set-1"
    assert item["provider_set_hash"] == "provider-set-1"
    assert item["address_verification"]["address_evidence_level"] == "unknown"


@pytest.mark.asyncio
async def test_search_current_ptg2_index_can_include_code_details():
    session = _db_serving_session()
    session._results.append(
        FakeResult(
            rows=[
                {
                    "code_system": "CPT",
                    "code": "70551",
                    "display_name": "MRI brain without contrast",
                    "short_description": "MRI brain without contrast",
                },
                {
                    "code_system": "POS",
                    "code": "23",
                    "display_name": "Emergency Room - Hospital",
                    "short_description": "Emergency Room - Hospital",
                },
                {
                    "code_system": "MODIFIER",
                    "code": "TC",
                    "display_name": "Technical component",
                    "short_description": "Technical component",
                },
            ]
        )
    )

    payload = await ptg2_serving.search_current_ptg2_index(
        session,
        {"plan_id": "TESTPLAN001", "code": "70551", "include_code_details": "true"},
        FakePagination(),
    )

    item = payload["items"][0]
    assert item["billing_code_detail"]["display_name"] == "MRI brain without contrast"
    assert item["tic_prices"][0]["service_code_details"][0]["code_system"] == "POS"
    assert item["tic_prices"][0]["service_code_details"][0]["display_name"] == "Emergency Room - Hospital"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["code_system"] == "MODIFIER"
    assert item["tic_prices"][0]["billing_code_modifier_details"][0]["display_name"] == "Technical component"
    assert item["tic_prices"][0]["billing_code_modifier_details"][1] == {
        "code_system": "MODIFIER",
        "code": "ZZ",
        "display_name": "Modifier ZZ",
        "short_description": None,
        "catalog_status": "missing",
    }
    assert "source_trace" not in item


@pytest.mark.asyncio
async def test_search_current_ptg2_index_caches_shaped_positive_responses(monkeypatch):
    calls = {"snapshot": 0, "search": 0}

    async def fake_resolve(_session, _args):
        return "snap-cache"

    async def fake_snapshot(_session, _snapshot_id):
        calls["snapshot"] += 1
        return ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_rate")

    async def fake_search(_session, _snapshot_id, _args, _pagination, *, serving_tables):
        del serving_tables
        calls["search"] += 1
        return {
            "items": [
                {
                    "reported_code": "70551",
                    "source_trace": [
                        {
                            "source_file_version_id": "source-version-cache",
                            "original_url": "https://example.test/rates.json.gz",
                        }
                    ],
                    "provider_set_hash": "provider-set-1",
                }
            ],
            "query": {"snapshot_id": _snapshot_id, "source": "ptg2_db", "serving_table": "mrf.ptg2_serving_rate"},
            "pagination": {"total": 1},
        }

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)
    monkeypatch.setattr(ptg2_serving, "search_ptg2_serving_table", fake_search)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "source_key": "example_network", "code": "70551"},
        FakePagination(),
    )
    payload["items"][0]["reported_code"] = "mutated"
    cached_payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "source_key": "example_network", "code": "70551"},
        FakePagination(),
    )

    assert calls == {"snapshot": 1, "search": 1}
    assert cached_payload["items"][0]["reported_code"] == "70551"
    assert "source_trace" not in cached_payload["items"][0]
    assert "provider_set_hash" not in cached_payload["items"][0]
    assert "source" not in cached_payload["query"]


@pytest.mark.asyncio
async def test_search_current_ptg2_index_ignores_non_manifest_serving_storage(monkeypatch):
    async def fake_resolve(_session, _args):
        return "legacy-snap"

    async def fake_snapshot(_session, _snapshot_id):
        return ptg2_serving.PTG2ServingTables(
            storage="db_compact_snapshot",
            serving_table="mrf.ptg2_serving_rate_compact_legacy",
        )

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "source_key": "example_network", "code": "70551"},
        FakePagination(),
    )

    assert payload is None


@pytest.mark.asyncio
async def test_search_current_ptg2_index_caches_missing_payload(monkeypatch):
    calls_by_name = {"snapshot": 0, "search": 0}

    async def fake_resolve(_session, _args):
        return "snap-cache-miss"

    async def fake_snapshot(_session, _snapshot_id):
        calls_by_name["snapshot"] += 1
        return ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_rate")

    async def fake_search(_session, _snapshot_id, _args, _pagination, *, serving_tables):
        del serving_tables
        calls_by_name["search"] += 1
        return None

    ptg2_serving.clear_ptg2_index_cache()
    monkeypatch.delenv(ptg2_serving.PTG2_JSON_FALLBACK_ENV, raising=False)
    monkeypatch.setattr(ptg2_serving, "resolve_current_ptg2_snapshot_id", fake_resolve)
    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_snapshot)
    monkeypatch.setattr(ptg2_serving, "search_ptg2_serving_table", fake_search)

    search_arg_map = {"plan_id": "TESTPLAN001", "source_key": "example_network"}
    assert await ptg2_serving.search_current_ptg2_index(FakeSession([]), search_arg_map, FakePagination()) is None
    assert await ptg2_serving.search_current_ptg2_index(FakeSession([]), search_arg_map, FakePagination()) is None
    assert calls_by_name == {"snapshot": 1, "search": 1}


@pytest.mark.asyncio
async def test_search_current_ptg2_index_combines_networks_for_multi_network_plan(monkeypatch):
    async def fake_ids(_session, _args):
        # A sample plan served by two networks, each with its own snapshot.
        return [("c2", "snap-c2"), ("ppo_ndc", "snap-ppo")]

    async def fake_one(_session, snapshot_id, _args, pagination):
        # Each network prices a different surgeon; the PPO row is cheaper.
        per_network = {
            "snap-c2": {
                "items": [
                    {"npi": 111, "provider_name": "Ambrose", "prices": [{"negotiated_rate": 1138.57}]}
                ],
                "pagination": {"total": 1, "limit": pagination.limit, "offset": pagination.offset},
                "query": {"snapshot_id": snapshot_id, "code": "29888"},
            },
            "snap-ppo": {
                "items": [
                    {"npi": 222, "provider_name": "Boswell", "prices": [{"negotiated_rate": 905.0}]}
                ],
                "pagination": {"total": 1, "limit": pagination.limit, "offset": pagination.offset},
                "query": {"snapshot_id": snapshot_id, "code": "29888"},
            },
        }
        return per_network.get(snapshot_id)

    monkeypatch.setattr(ptg2_serving, "current_source_snapshot_ids_for_plan", fake_ids)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_one)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "code": "29888", "order_by": "rate", "order": "asc"},
        FakePagination(),
    )

    # Union of both networks, globally re-sorted by rate asc (cheaper PPO row first).
    assert [item["npi"] for item in payload["items"]] == [222, 111]
    assert payload["pagination"]["total"] == 2
    assert payload["pagination"]["has_more"] is False
    # Each row stays attributable to the network it came from.
    assert {item["npi"]: item["network"] for item in payload["items"]} == {111: "c2", 222: "ppo_ndc"}
    assert payload["query"]["combined"] is True
    assert payload["query"]["snapshot_id"] is None
    assert {n["source_key"] for n in payload["query"]["networks"]} == {"c2", "ppo_ndc"}


@pytest.mark.asyncio
async def test_search_current_ptg2_index_combined_pagination_reports_has_more(monkeypatch):
    async def fake_ids(_session, _args):
        return [("c2", "snap-c2"), ("ppo_ndc", "snap-ppo")]

    async def fake_one(_session, snapshot_id, _args, pagination):
        # Each network returns one page item but reports a much larger true total,
        # mirroring the route-item fast path's window count.
        payload_by_snapshot = {
            "snap-c2": {
                "items": [
                    {"npi": 111, "provider_name": "Ambrose", "prices": [{"negotiated_rate": 1138.57}]}
                ],
                "pagination": {"total": 40, "limit": pagination.limit, "offset": pagination.offset},
                "query": {"snapshot_id": snapshot_id, "code": "29888"},
            },
            "snap-ppo": {
                "items": [
                    {"npi": 222, "provider_name": "Boswell", "prices": [{"negotiated_rate": 905.0}]}
                ],
                "pagination": {"total": 37, "limit": pagination.limit, "offset": pagination.offset},
                "query": {"snapshot_id": snapshot_id, "code": "29888"},
            },
        }
        return payload_by_snapshot.get(snapshot_id)

    monkeypatch.setattr(ptg2_serving, "current_source_snapshot_ids_for_plan", fake_ids)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_one)

    combined_payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "code": "29888", "order_by": "rate", "order": "asc"},
        FakePagination(),
    )

    assert combined_payload["pagination"]["total"] == 77
    assert combined_payload["pagination"]["has_more"] is True


def test_manifest_route_item_candidates_include_raw_and_canonical_revenue_code():
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_3f764988bc31fee2",
        storage="manifest_snapshot",
        id_storage="uuid",
    )

    candidates = ptg2_serving._ptg2_route_item_table_candidates(
        tables,
        plan_id="TESTPLAN001",
        code_system="RC",
        code="110",
    )

    assert candidates == (
        "mrf.ptg2_route_item_3f764988bc31fee2_testplan001_rc_0110",
        "mrf.ptg2_route_item_3f764988bc31fee2_testplan001_rc_110",
        "mrf.ptg2_route_item_3f764_testplan001_rc_0110",
        "mrf.ptg2_route_item_3f764_testplan001_rc_110",
    )


@pytest.mark.asyncio
async def test_manifest_route_item_tables_select_each_revenue_code_form(monkeypatch):
    availability_check = AsyncMock(return_value=True)
    column_check = AsyncMock(return_value=True)
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", availability_check)
    monkeypatch.setattr(ptg2_serving, "_ptg2_table_has_columns", column_check)
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_3f764988bc31fee2",
    )

    route_tables = await ptg2_serving._ptg2_route_item_tables(
        FakeSession([]),
        tables,
        plan_id="TESTPLAN001",
        code_system="RC",
        code="110",
    )

    assert route_tables == (
        "mrf.ptg2_route_item_3f764988bc31fee2_testplan001_rc_0110",
        "mrf.ptg2_route_item_3f764988bc31fee2_testplan001_rc_110",
    )
    assert availability_check.await_count == 2
    assert column_check.await_count == 2


@pytest.mark.asyncio
async def test_manifest_route_item_fast_path_unions_revenue_code_tables(monkeypatch):
    route_tables = (
        "mrf.ptg2_route_item_fixture_testplan001_rc_0110",
        "mrf.ptg2_route_item_fixture_testplan001_rc_110",
    )
    route_table_lookup = AsyncMock(return_value=route_tables)
    monkeypatch.setattr(ptg2_serving, "_ptg2_route_item_tables", route_table_lookup)
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "item_payload": {"npi": 1234567890, "reported_code": "0110"},
                        "distance_miles": 1.0,
                        "zip_rank": 1,
                        "total_matches": 2,
                    },
                    {
                        "item_payload": {"npi": 1234567891, "reported_code": "110"},
                        "distance_miles": 2.0,
                        "zip_rank": 1,
                        "total_matches": 2,
                    },
                ]
            )
        ]
    )

    response = await ptg2_serving._search_ptg2_manifest_route_item_table(
        session,
        "ptg2:fixture",
        {
            "plan_id": "TESTPLAN001",
            "code": "110",
            "code_system": "RC",
            "include_providers": "true",
            "lat": "40.0",
            "long": "-75.0",
            "radius_miles": "10",
        },
        FakePagination(),
        ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_fixture"),
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
        requested_plan="TESTPLAN001",
        requested_system="RC",
        requested_code="0110",
    )

    query_sql = str(session.calls[0][0][0])
    assert response["pagination"]["total"] == 2
    assert response["pagination"]["has_more"] is False
    assert "UNION ALL" in query_sql
    assert all(table_name in query_sql for table_name in route_tables)
    assert "0::integer AS route_table_ordinal" in query_sql
    assert "1::integer AS route_table_ordinal" in query_sql
    assert "route_table_ordinal, location_hash NULLS LAST" in query_sql


@pytest.mark.asyncio
async def test_manifest_route_item_table_fast_path_shapes_payload():
    columns = sorted(ptg2_serving._PTG2_ROUTE_ITEM_COLUMNS)
    session = FakeSession(
        [
            FakeResult(scalar=True),
            FakeResult(rows=[(column,) for column in columns]),
            FakeResult(
                rows=[
                    {
                        "item_payload": {
                            "npi": 1234567890,
                            "provider_name": "Example Provider",
                            "reported_code": "90837",
                            "reported_code_system": "CPT",
                            "service_code": "90837",
                            "service_code_system": "CPT",
                            "price_summary": [{"rate": 101.0}],
                        },
                        "distance_miles": 4.25,
                        "zip_rank": 1,
                        "total_matches": 7,
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_fixture_table_a",
        storage="manifest_snapshot",
        id_storage="uuid",
    )

    response = await ptg2_serving._search_ptg2_manifest_route_item_table(
        session,
        "ptg2:202606:fixture_a",
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "include_providers": "true",
            "zip5": "60601",
            "lat": 41.88526,
            "long": -87.62194,
            "radius_miles": 75.0,
            "order_by": "total_allowed_amount",
            "order": "asc",
        },
        _Pagination(limit=5, offset=0),
        tables,
        "product_search",
        requested_plan="TESTPLAN001",
        requested_system="CPT",
        requested_code="90837",
    )

    assert response is not None
    assert response["items"][0]["npi"] == 1234567890
    assert response["items"][0]["distance_miles"] == 4.25
    assert response["items"][0]["zip_match_type"] == "radius"
    assert response["items"][0]["anchor_zip5"] == "60601"
    assert "service_code" not in response["items"][0]
    assert "route_item_table" not in response["query"]
    assert response["pagination"]["total"] == 7
    assert response["pagination"]["has_more"] is True
    route_sql = str(session.calls[-1][0][0])
    assert "FROM location_zip_scope zip_scope" not in route_sql
    assert "FROM mrf.ptg2_route_item_fixture_table_a_testplan001_cpt_90837" in route_sql
    assert "FROM route_items r" in route_sql
    assert "COUNT(*) OVER () AS total_matches" in route_sql
    assert "ORDER BY min_rate ASC NULLS LAST" in route_sql


@pytest.mark.asyncio
def _build_route_item_taxonomy_fake_session() -> FakeSession:
    """Build route-item rows for the lat/long taxonomy pagination regression."""
    columns = sorted(ptg2_serving._PTG2_ROUTE_ITEM_COLUMNS)
    return FakeSession(
        [
            FakeResult(scalar=True),
            FakeResult(rows=[(column,) for column in columns]),
            FakeResult(
                rows=[
                    {
                        "item_payload": {
                            "npi": 1234567890 + idx,
                            "provider_name": "Family Medicine Provider",
                            "reported_code": "99213",
                            "reported_code_system": "CPT",
                            "price_summary": [{"rate": 86.48}],
                        },
                        "distance_miles": 2.75 + idx,
                        "zip_rank": 1,
                        "total_matches": 86,
                    }
                    for idx in range(36)
                ]
            ),
        ]
    )


@pytest.mark.asyncio
async def test_manifest_route_item_table_fast_path_supports_lat_long_taxonomy_filter():
    """Route-item fast path handles dense-snapshot page-two taxonomy searches."""
    session = _build_route_item_taxonomy_fake_session()
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_7cabb84262c9",
        storage="manifest_snapshot",
        id_storage="uuid",
    )

    response = await ptg2_serving._search_ptg2_manifest_route_item_table(
        session,
        "ptg2:202607:7cabb84262c9",
        {
            "plan_id": "TESTPLAN001",
            "code": "99213",
            "code_system": "CPT",
            "include_providers": "true",
            "lat": 40.7128,
            "long": -74.0060,
            "radius_miles": 10.0,
            "taxonomy_codes": "207Q00000X",
            "primary_only": "true",
            "order_by": "total_allowed_amount",
            "order": "asc",
        },
        _Pagination(limit=50, offset=50),
        tables,
        "product_search",
        requested_plan="TESTPLAN001",
        requested_system="CPT",
        requested_code="99213",
    )

    assert response is not None
    assert response["items"][0]["npi"] == 1234567890
    assert response["items"][0]["zip_match_type"] == "radius"
    assert response["items"][0]["anchor_zip5"] is None
    assert response["pagination"]["total"] == 86
    assert response["pagination"]["has_more"] is False
    route_sql = str(session.calls[-1][0][0])
    route_params = session.calls[-1][0][1]
    assert "FROM mrf.ptg2_route_item_7cabb84262c9_testplan001_cpt_99213" in route_sql
    assert "FROM route_items r" in route_sql
    assert "r.npi IN (SELECT route_item_specialty_nt.npi" in route_sql
    assert "route_item_specialty_nt.npi = r.npi" not in route_sql
    assert "UPPER(COALESCE(route_item_specialty_nt.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'" in route_sql
    assert "route_item_specialty_nt.healthcare_provider_taxonomy_code IN (:route_item_specialty_taxonomy_code_0)" in route_sql
    assert route_params["route_item_specialty_taxonomy_code_0"] == "207Q00000X"
    assert route_params["zip5"] is None
    assert route_params["limit"] == 50
    assert route_params["offset"] == 50


@pytest.mark.asyncio
async def test_manifest_route_item_table_fast_path_rejects_explicit_specialty_filter():
    session = FakeSession([])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_fixture_table_a",
        storage="manifest_snapshot",
        id_storage="uuid",
    )

    response = await ptg2_serving._search_ptg2_manifest_route_item_table(
        session,
        "ptg2:202606:fixture_a",
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "include_providers": "true",
            "zip5": "60601",
            "lat": 41.88526,
            "long": -87.62194,
            "radius_miles": 75.0,
            "specialty": "family medicine",
        },
        _Pagination(limit=5, offset=0),
        tables,
        "product_search",
        requested_plan="TESTPLAN001",
        requested_system="CPT",
        requested_code="90837",
    )

    assert response is None
    assert session.calls == []


@pytest.mark.asyncio
async def test_manifest_route_item_table_fast_path_falls_back_when_artifact_query_fails():
    columns = sorted(ptg2_serving._PTG2_ROUTE_ITEM_COLUMNS)
    session = FakeSession(
        [
            FakeResult(scalar=True),
            FakeResult(rows=[(column,) for column in columns]),
            RuntimeError("stale cached artifact"),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_fixture_table_a",
        storage="manifest_snapshot",
        id_storage="uuid",
    )

    response = await ptg2_serving._search_ptg2_manifest_route_item_table(
        session,
        "ptg2:202606:fixture_a",
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "include_providers": "true",
            "zip5": "60601",
            "lat": 41.88526,
            "long": -87.62194,
            "radius_miles": 75.0,
            "order_by": "total_allowed_amount",
            "order": "asc",
        },
        _Pagination(limit=5, offset=0),
        tables,
        "product_search",
        requested_plan="TESTPLAN001",
        requested_system="CPT",
        requested_code="90837",
    )

    assert response is None
    assert session.rollback_count == 1


@pytest.mark.asyncio
async def test_search_multi_ptg2_snapshots_skips_network_without_plan_code(monkeypatch):
    class RealishFakeSession(FakeSession):
        sync_session = object()

    checked_snapshots: list[str] = []
    searched_snapshots: list[str] = []

    async def fake_tables(_session, _snapshot_id):
        return ptg2_serving.PTG2ServingTables(serving_table="mrf.ptg2_serving_test")

    async def fake_has_plan_code(_session, snapshot_id, _args, *, serving_tables=None):
        assert serving_tables is not None
        checked_snapshots.append(snapshot_id)
        return snapshot_id != "snap-empty"

    async def fake_one(_session, snapshot_id, _args, pagination, *, serving_tables=None):
        assert serving_tables is not None
        searched_snapshots.append(snapshot_id)
        return {
            "items": [{"npi": 111, "provider_name": "Ambrose", "prices": [{"negotiated_rate": 1138.57}]}],
            "pagination": {"total": 1, "limit": pagination.limit, "offset": pagination.offset},
            "query": {"snapshot_id": snapshot_id, "code": "90837"},
        }

    monkeypatch.setattr(ptg2_serving, "snapshot_serving_tables", fake_tables)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_snapshot_has_plan_code", fake_has_plan_code)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_one)

    payload = await ptg2_serving._search_multi_ptg2_snapshots(
        RealishFakeSession([]),
        [("empty", "snap-empty"), ("c2", "snap-c2")],
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT"},
        FakePagination(),
    )

    assert checked_snapshots == ["snap-empty", "snap-c2"]
    assert searched_snapshots == ["snap-c2"]
    assert payload["items"][0]["network"] == "c2"
    assert payload["pagination"]["total"] == 1
    assert payload["query"]["combined"] is True


@pytest.mark.asyncio
async def test_manifest_snapshot_has_plan_code_uses_code_count_for_lean_layout(monkeypatch):
    class RealishFakeSession(FakeSession):
        sync_session = object()

    session = RealishFakeSession([FakeResult(scalar=True)])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_snap",
        code_count_table="mrf.ptg2_code_count_snap",
        serving_table_layout="lean_provider_key_v1",
    )
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))

    result = await ptg2_serving._ptg2_manifest_snapshot_has_plan_code(
        session,
        "ptg2:test",
        {"plan_id": "TESTPLAN001", "code": "99213", "code_system": "CPT"},
        serving_tables=tables,
    )

    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert result is True
    assert "FROM mrf.ptg2_code_count_snap" in sql
    assert "FROM mrf.ptg2_serving_snap" not in sql
    assert params == {
        "plan_id": "TESTPLAN001",
        "reported_code": "99213",
        "reported_code_system": "CPT",
    }


@pytest.mark.asyncio
async def test_manifest_snapshot_preflight_matches_revenue_code_forms(monkeypatch):
    class RealishFakeSession(FakeSession):
        sync_session = object()

    session = RealishFakeSession([FakeResult(scalar=True)])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_fixture",
        code_count_table="mrf.ptg2_code_count_fixture",
        serving_table_layout="lean_provider_key_v1",
    )
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))

    result = await ptg2_serving._ptg2_manifest_snapshot_has_plan_code(
        session,
        "ptg2:fixture",
        {"plan_id": "TESTPLAN001", "code": "110", "code_system": "RC"},
        serving_tables=tables,
    )

    sql = str(session.calls[0][0][0])
    params_by_name = session.calls[0][0][1]
    assert result is True
    assert "reported_code IN (:reported_code, :reported_code_1)" in sql
    assert params_by_name["reported_code"] == "0110"
    assert params_by_name["reported_code_1"] == "110"


@pytest.mark.asyncio
async def test_search_current_ptg2_index_single_network_plan_uses_single_path(monkeypatch):
    seen = {}

    async def fake_ids(_session, _args):
        return [("c2", "snap-c2")]

    async def fake_one(_session, snapshot_id, _args, _pagination):
        seen["snapshot_id"] = snapshot_id
        return {
            "items": [{"npi": 111, "provider_name": "Ambrose", "prices": [{"negotiated_rate": 1138.57}]}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving, "current_source_snapshot_ids_for_plan", fake_ids)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_one)

    payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "code": "29888"},
        FakePagination(),
    )

    # A single-network plan stays on the untouched single-snapshot path: no combine
    # envelope, no per-item network tag.
    assert seen["snapshot_id"] == "snap-c2"
    assert payload["items"][0]["npi"] == 111
    assert "network" not in payload["items"][0]
    assert "combined" not in payload["query"]


@pytest.mark.asyncio
async def test_search_current_ptg2_index_pinned_snapshot_skips_network_combine(monkeypatch):
    call_count_by_name = {"ids": 0}

    async def fake_ids(_session, _args):
        call_count_by_name["ids"] += 1
        return [("c2", "snap-c2"), ("ppo_ndc", "snap-ppo")]

    async def fake_one(_session, snapshot_id, _args, _pagination):
        return {
            "items": [{"npi": 999, "prices": [{"negotiated_rate": 10.0}]}],
            "pagination": {"total": 1},
            "query": {"snapshot_id": snapshot_id},
        }

    monkeypatch.setattr(ptg2_serving, "current_source_snapshot_ids_for_plan", fake_ids)
    monkeypatch.setattr(ptg2_serving, "_search_one_ptg2_snapshot", fake_one)
    monkeypatch.setattr(
        ptg2_serving,
        "resolve_current_ptg2_snapshot_id",
        AsyncMock(return_value="snap-pinned"),
    )

    response_payload = await ptg2_serving.search_current_ptg2_index(
        FakeSession([]),
        {"plan_id": "TESTPLAN001", "code": "29888", "snapshot_id": "snap-pinned"},
        FakePagination(),
    )

    # A caller-pinned snapshot must not trigger the multi-network resolver/combine.
    assert call_count_by_name["ids"] == 0
    assert "combined" not in response_payload["query"]
    assert response_payload["query"]["snapshot_id"] == "snap-pinned"


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_routes_by_plan_source_pointer():
    session = FakeSession(["snap-source"])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group", "source_key": "example_dental"},
    )

    assert snapshot_id == "snap-source"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "ptg2_current_plan_source" in sql
    assert "JOIN mrf.ptg2_snapshot" in sql
    assert "cps.plan_id = ANY(CAST(:plan_ids AS text[]))" in sql
    assert "cps.plan_market_type = :plan_market_type" in sql
    assert "cps.source_key = :source_key" in sql
    assert "s.status = 'published'" in sql
    assert "serving_index" in sql
    assert params["plan_ids"] == ["TESTPLAN001"]
    assert params["source_key"] == "example_dental"


@pytest.mark.asyncio
async def test_requested_ptg2_snapshot_must_be_published():
    published_session = FakeSession(["snap-published"])
    missing_session = FakeSession([None])

    published = await ptg2_snapshot.current_snapshot_id(
        published_session,
        requested_snapshot_id="snap-published",
    )
    unavailable = await ptg2_snapshot.current_snapshot_id(
        missing_session,
        requested_snapshot_id="snap-building",
    )

    assert published == "snap-published"
    assert unavailable is None
    sql = str(published_session.calls[0][0][0])
    params = published_session.calls[0][0][1]
    assert "status = 'published'" in sql
    assert params == {"snapshot_id": "snap-published"}


@pytest.mark.asyncio
async def test_source_only_snapshot_resolves_published_source_pointer():
    session = FakeSession(["snap-source"])

    snapshot_id = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {"source_key": "Example_Dental"},
    )

    assert snapshot_id == "snap-source"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "ptg2_current_source_snapshot" in sql
    assert "status = 'published'" in sql
    assert params == {"source_key": "example_dental"}


@pytest.mark.asyncio
async def test_missing_source_pointer_does_not_fall_back_to_global_snapshot():
    session = FakeSession([None, "snap-global"])

    snapshot_id = await ptg2_snapshot.resolve_current_ptg2_snapshot_id(
        session,
        {"source_key": "missing_source"},
    )

    assert snapshot_id is None
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_prefers_loaded_serving_table():
    # Regression: a plan with multiple networks must resolve to a snapshot whose
    # serving table is actually materialized (to_regclass), not merely manifest-claimed,
    # so an unloaded newer network does not yield snapshot_not_loaded / 0 results.
    session = FakeSession(["snap-loaded"])
    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )
    assert snapshot_id == "snap-loaded"
    sql = str(session.calls[0][0][0])
    assert "serving_binary_table" in sql
    assert "to_regclass(COALESCE(" in sql
    assert "NULLIF(s.manifest->'serving_index'->>'table', '')" in sql
    # Recency selects among snapshots whose retained serving relation exists.
    assert "successful_files" in sql
    assert "cps.import_month DESC NULLS LAST" in sql


def test_snapshot_availability_prefers_v3_binary_relation():
    availability_sql = ptg2_snapshot._serving_relation_available_sql("snapshot")

    assert "serving_binary_table" in availability_sql
    assert availability_sql.index("serving_binary_table") < availability_sql.index("->>'table'")
    assert availability_sql.endswith("IS NOT NULL")


@pytest.mark.asyncio
async def test_current_plan_source_lookup_is_not_memory_cached():
    class RealishFakeSession(FakeSession):
        sync_session = object()

    session = RealishFakeSession(["snap-before-publish", "snap-after-publish"])

    first_snapshot = await ptg2_snapshot.current_source_snapshot_id_for_plan(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )
    second_snapshot = await ptg2_snapshot.current_source_snapshot_id_for_plan(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )

    assert first_snapshot == "snap-before-publish"
    assert second_snapshot == "snap-after-publish"
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_current_plan_source_network_list_is_not_memory_cached():
    class RealishFakeSession(FakeSession):
        sync_session = object()

    session = RealishFakeSession(
        [
            FakeResult(rows=[("ppo", "snap-ppo")]),
            FakeResult(rows=[("ppo", "snap-ppo"), ("c2", "snap-c2")]),
        ]
    )

    first_networks = await ptg2_snapshot.current_source_snapshot_ids_for_plan(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )
    second_networks = await ptg2_snapshot.current_source_snapshot_ids_for_plan(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )

    assert first_networks == [("ppo", "snap-ppo")]
    assert second_networks == [("ppo", "snap-ppo"), ("c2", "snap-c2")]
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_current_plan_source_network_list_groups_dated_files_by_manifest_network():
    session = FakeSession([FakeResult(rows=[("current-source", "current-snapshot")])])

    snapshot_pairs = await ptg2_snapshot.current_source_snapshot_ids_for_plan(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )

    assert snapshot_pairs == [("current-source", "current-snapshot")]
    query_sql = str(session.calls[0][0][0])
    assert "AS logical_network_key" in query_sql
    assert "network_names" in query_sql
    assert "successful_files" in query_sql
    assert "source_effective_month DESC NULLS LAST" in query_sql
    assert "DISTINCT ON (logical_network_key)" in query_sql
    assert "0" not in session.calls[0][0][0].compile().params


def test_musculoskeletal_surgery_cpt_infers_orthopedic_taxonomy():
    for code in ("29888", "27447", "20000", "29999"):
        rule = ptg2_serving._inferred_provider_taxonomy_rule({"code": code, "code_system": "cpt"})
        assert rule is not None, code
        assert "207X00000X" in rule.taxonomy_codes, code
    omitted_system_rule = ptg2_serving._inferred_provider_taxonomy_rule({"code": "29888"})
    assert omitted_system_rule is not None
    assert "207X00000X" in omitted_system_rule.taxonomy_codes
    # office-visit / non-musculoskeletal codes must NOT infer orthopedic surgery
    assert ptg2_serving._inferred_provider_taxonomy_rule({"code": "99213", "code_system": "cpt"}) is None
    # Short numeric revenue codes are not CPT by default.
    assert ptg2_serving._inferred_provider_taxonomy_rule({"code": "450"}) is None


@pytest.mark.asyncio
async def test_current_ptg2_snapshot_rolls_back_missing_source_pointer_before_fallback():
    session = FakeSession([RuntimeError("missing source pointer")])

    snapshot_id = await ptg2_serving.resolve_current_ptg2_snapshot_id(
        session,
        {"plan_id": "TESTPLAN001", "plan_market_type": "group"},
    )

    assert snapshot_id is None
    assert session.rollback_count == 1
    assert len(session.calls) == 1


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_uses_compact_snapshot_without_market_column():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
                "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-token",
                        "plan_id": "TESTPLAN001",
                        "plan_name": None,
                        "plan_id_type": None,
                        "plan_market_type": None,
                        "issuer_name": None,
                        "plan_sponsor_name": None,
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "billing_code": "99213",
                        "billing_code_type": "CPT",
                        "procedure_name": "Office visit",
                        "procedure_description": "Established office visit",
                        "provider_set_hash": "provider-set-1",
                        "provider_count": 2,
                        "provider_set_count": None,
                        "price_set_hash": "price-set-1",
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 101.42}],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {
            "plan_id": "TESTPLAN001",
            "plan_market_type": "group",
            "source_key": "example_dental",
            "code": "99213",
            "code_system": "CPT",
        },
        FakePagination(),
    )

    assert payload["items"][0]["npi"] == 1083311500
    assert payload["items"][0]["reported_code"] == "99213"
    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == 101.42
    row_call = next(call for call in session.calls if "provider_sets AS MATERIALIZED" in str(call[0][0]))
    row_sql = str(row_call[0][0])
    assert "r.plan_market_type" not in row_sql
    assert "mrf.ptg2_provider_set_component_token" in row_sql
    assert "mrf.ptg2_provider_entry_component_token" not in row_sql
    assert "provider_group_hashes @>" not in row_sql
    assert "NULL::varchar AS plan_market_type" in row_sql
    assert "r.provider_set_count" not in row_sql
    assert "NULL::integer AS provider_set_count" in row_sql
    assert row_call[0][1]["plan_id"] == "TESTPLAN001"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_filters_prices_by_pos_modifier_and_rate():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            FakeResult(
                rows=[
                    {
                        "serving_rate_id": "rate-1",
                        "snapshot_id": "snap-token",
                        "plan_id": "TESTPLAN001",
                        "plan_name": None,
                        "plan_id_type": None,
                        "plan_market_type": None,
                        "issuer_name": None,
                        "plan_sponsor_name": None,
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "93458",
                        "billing_code": "93458",
                        "billing_code_type": "CPT",
                        "procedure_name": "Cath placement",
                        "procedure_description": "Cath placement",
                        "provider_set_hash": "provider-set-1",
                        "provider_count": 3,
                        "provider_set_count": None,
                        "price_set_hash": "price-set-1",
                        "prices": [
                            {
                                "negotiated_type": "fee schedule",
                                "negotiated_rate": 516.08,
                                "service_code": ["21"],
                                "billing_code_modifier": ["26"],
                            }
                        ],
                    }
                ]
            ),
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1235189762,
        {
            "plan_id": "TESTPLAN001",
            "source_key": "network-a",
            "code": "93458",
            "code_system": "CPT",
            "pos": "21",
            "modifier": "26",
            "rate": "516.08",
            "include_details": "true",
        },
        FakePagination(),
    )

    item = payload["items"][0]
    assert item["tic_prices"][0]["negotiated_rate"] == 516.08
    assert item["tic_prices"][0]["service_code"] == ["21"]
    assert item["tic_prices"][0]["billing_code_modifier"] == ["26"]
    assert payload["query"]["price_filter"] == {
        "service_code": ["21"],
        "pos": "21",
        "billing_code_modifier": ["26"],
        "negotiated_rate": 516.08,
        "rate_tolerance": 0.01,
    }
    row_call = next(call for call in session.calls if "provider_sets AS MATERIALIZED" in str(call[0][0]))
    row_sql = str(row_call[0][0])
    params = row_call[0][1]
    assert "price_payload.prices IS NOT NULL" in row_sql
    assert "CAST(:price_service_codes AS varchar[])" in row_sql
    assert "CAST(:price_modifier_codes AS varchar[])" in row_sql
    assert "ABS(pa.negotiated_rate::numeric - :price_negotiated_rate)" in row_sql
    assert params["price_service_codes"] == ["21"]
    assert params["price_modifier_codes"] == ["26"]
    assert str(params["price_negotiated_rate"]) == "516.08"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_returns_no_match_after_snapshot_resolves():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_set_component_table": "mrf.ptg2_provider_set_component_token",
                "provider_set_entry_table": "mrf.ptg2_provider_set_entry_token",
                "provider_entry_component_table": "mrf.ptg2_provider_entry_component_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
            FakeResult(rows=[]),
            0,
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {
            "plan_id": "TESTPLAN001",
            "source_key": "network-a",
            "code": "99213",
            "code_system": "CPT",
            "include_details": "true",
        },
        FakePagination(),
    )

    assert payload["items"] == []
    assert payload["pagination"]["total"] == 0
    assert payload["query"]["snapshot_id"] == "snap-token"
    assert payload["query"]["status"] == "no_match"
    assert payload["query"]["source"] == "ptg2_db"


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_uses_reverse_sidecar_for_lean_snapshot(tmp_path, monkeypatch):
    provider_set_id = "0000000000000000000000000000000a"
    price_set_id = "00000000000000000000000000000101"
    sidecar = write_serving_by_provider_set_sidecar(
        tmp_path / "serving_by_provider_set.ptg2sbp",
        [
            (3, 7, 1, price_set_id),
        ],
    )
    session = FakeSession(
        [
            FakeResult(rows=[{"provider_set_key": 3, "provider_set_global_id_128": provider_set_id}]),
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 1,
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        artifacts={"serving_by_provider_set": sidecar},
    )

    async def provider_sets_for_npi(_session, _tables, npi):
        assert npi == 1234567890
        return (provider_set_id,)

    async def resolve_code_context(_session, *, code, code_system):
        assert code == "70551"
        assert code_system == "CPT"
        return {}

    async def source_traces(_session, _trace_sets):
        return {}

    async def price_rows(_session, _tables, price_hashes, **_kwargs):
        assert price_hashes == [price_set_id]
        return {price_set_id: [{"negotiated_type": "negotiated", "negotiated_rate": "451.25"}]}

    async def procedure_rows(_session, row_data):
        assert row_data[0]["provider_set_global_id_128"] == provider_set_id
        return {("CPT", "70551"): {"procedure_name": "MRI brain"}}

    async def provider_context(_session, *, npis, limit, plan_id=None, snapshot_id=None, source_key=None):
        assert npis == [1234567890]
        return [{"npi": 1234567890, "provider_name": "Lean Provider"}]

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_sets_for_npi", provider_sets_for_npi)
    monkeypatch.setattr(ptg2_serving, "_resolve_ptg2_code_search_context", resolve_code_context)
    monkeypatch.setattr(ptg2_serving, "_ptg2_source_traces_for_trace_sets", source_traces)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_prices_for_price_sets", price_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_procedure_details_for_rows", procedure_rows)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_enriched_provider_rows_for_npis", provider_context)

    payload = await ptg2_serving._search_ptg2_manifest_provider_procedures(
        session,
        1234567890,
        {"plan_id": "TESTPLAN001", "code": "70551", "code_system": "CPT", "include_details": "true"},
        FakePagination(),
        snapshot_id="ptg2:202607:group_fixture",
        serving_tables=tables,
    )

    assert payload["query"]["provider_reverse_index"] is True
    assert payload["items"][0]["npi"] == 1234567890
    assert payload["items"][0]["provider_set_hash"] == provider_set_id
    assert payload["items"][0]["reported_code"] == "70551"
    assert payload["items"][0]["tic_prices"][0]["negotiated_rate"] == 451.25


@pytest.mark.asyncio
async def test_ptg2_provider_reverse_window_prefers_code_sidecar(tmp_path):
    provider_set_a = "0000000000000000000000000000000a"
    provider_set_b = "0000000000000000000000000000000b"
    price_a = "00000000000000000000000000000101"
    price_b = "00000000000000000000000000000102"
    price_c = "00000000000000000000000000000103"
    sidecar = write_serving_by_code_sidecar(
        tmp_path / "serving_by_code.ptg2sbc",
        [
            (7, 3, 5, price_a),
            (7, 4, 9, price_b),
            (8, 3, 11, price_c),
        ],
    )
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {"provider_set_key": 3, "provider_set_global_id_128": provider_set_a},
                    {"provider_set_key": 4, "provider_set_global_id_128": provider_set_b},
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 2,
                    },
                    {
                        "code_key": 8,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70552",
                        "rate_count": 1,
                    },
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        artifacts={"serving_by_code": sidecar},
    )

    rows = await ptg2_serving._ptg2_manifest_provider_procedure_rows_from_reverse_sidecar(
        session,
        tables,
        provider_set_ids=(provider_set_a, provider_set_b),
        requested_plan="TESTPLAN001",
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        source_trace_set_hash="trace-set",
        network_names=["network"],
        limit=2,
        offset=1,
        apply_window=True,
    )

    assert [
        (row["reported_code"], row["provider_set_global_id_128"], row["provider_count"], row["price_set_global_id_128"])
        for row in rows
    ] == [
        ("70551", provider_set_a, 5, price_a),
        ("70552", provider_set_a, 11, price_c),
    ]


@pytest.mark.asyncio
async def test_broad_npi_prefers_reverse_sidecar(tmp_path, monkeypatch):
    """Broad NPI paging should use the provider-set reverse artifact."""
    provider_set = "0000000000000000000000000000000a"
    price_a = "00000000000000000000000000000101"
    price_b = "00000000000000000000000000000102"
    reverse_sidecar = write_serving_by_provider_set_sidecar(
        tmp_path / "serving_by_provider_set.ptg2sbp",
        [(3, 7, 5, price_a), (3, 8, 11, price_b)],
    )
    code_sidecar = write_serving_by_code_sidecar(tmp_path / "serving_by_code.ptg2sbc", [(7, 3, 5, price_a), (8, 3, 11, price_b)])
    session = FakeSession(
        [
            FakeResult(rows=[{"provider_set_key": 3, "provider_set_global_id_128": provider_set}]),
            FakeResult(
                rows=[
                    {"code_key": 7, "plan_id": "TESTPLAN001", "reported_code_system": "CPT", "reported_code": "70551", "rate_count": 1},
                    {"code_key": 8, "plan_id": "TESTPLAN001", "reported_code_system": "CPT", "reported_code": "70552", "rate_count": 1},
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(storage="manifest_snapshot", code_count_table="mrf.ptg2_code_count_manifest_snap", provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap", serving_table_layout="lean_provider_key_v1", artifacts={"serving_by_code": code_sidecar, "serving_by_provider_set": reverse_sidecar})
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_lookup_serving_by_code_sidecar",
        AsyncMock(side_effect=AssertionError("broad provider reverse lookup should use provider-set sidecar")),
    )

    procedure_matches = await ptg2_serving._ptg2_manifest_provider_procedure_rows_from_reverse_sidecar(
        session, tables,
        provider_set_ids=(provider_set,),
        requested_plan="TESTPLAN001", code_value="", code_system=None, q_text="", code_context=None,
        source_trace_set_hash="trace-set", network_names=["network"], limit=2, offset=0, apply_window=True,
    )

    code_count_call = next(
        call for call in session.calls if "FROM mrf.ptg2_code_count_manifest_snap" in str(call[0][0])
    )
    code_count_sql = str(code_count_call[0][0])
    code_count_params = code_count_call[0][1]
    assert "code_key = ANY(CAST(:code_keys AS integer[]))" not in code_count_sql
    assert "LIMIT :code_row_limit OFFSET :code_row_offset" in code_count_sql
    assert code_count_params["code_row_limit"] == 256
    assert code_count_params["code_row_offset"] == 0
    assert [
        (
            procedure_match["reported_code"],
            procedure_match["provider_set_global_id_128"],
            procedure_match["provider_count"],
            procedure_match["price_set_global_id_128"],
        )
        for procedure_match in procedure_matches
    ] == [
        ("70551", provider_set, 5, price_a),
        ("70552", provider_set, 11, price_b),
    ]


def _postgres_binary_reverse_fixture():
    provider_set_id = "0000000000000000000000000000000a"
    price_a_id = "00000000000000000000000000000101"
    price_b_id = "00000000000000000000000000000102"
    session = FakeSession(
        [
            FakeResult(rows=[{"provider_set_key": 3, "provider_set_global_id_128": provider_set_id}]),
            FakeResult(
                rows=[
                    {"code_key": 7, "plan_id": "TESTPLAN001", "reported_code_system": "CPT", "reported_code": "70551", "rate_count": 1},
                    {"code_key": 8, "plan_id": "TESTPLAN001", "reported_code_system": "CPT", "reported_code": "70552", "rate_count": 1},
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_binary_table="mrf.ptg2_serving_binary_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
        artifacts={
            "serving_by_code": {"path": "/missing/local/cache/serving_by_code.ptg2sbc"},
            "serving_by_provider_set": {"path": "/missing/local/cache/serving_by_provider_set.ptg2sbp"},
        },
    )
    return provider_set_id, price_a_id, price_b_id, session, tables


def _patch_binary_reverse_guards(monkeypatch, *, price_a_id: str, price_b_id: str) -> None:
    async def binary_patterns(_session, table_name, provider_set_keys, *, code_keys=None):
        assert table_name == "mrf.ptg2_serving_binary_manifest_snap"
        assert tuple(provider_set_keys) == (3,)
        assert sorted(int(code_key) for code_key in code_keys or ()) == [7, 8]
        return {
            3: (
                SimpleNamespace(code_keys=(7,), entries=((5, price_a_id),)),
                SimpleNamespace(code_keys=(8,), entries=((11, price_b_id),)),
            )
        }

    monkeypatch.setattr(
        ptg2_serving,
        "lookup_serving_binary_by_provider_sets_patterns_from_db",
        binary_patterns,
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_serving_by_provider_set_patterns_from_db",
        AsyncMock(side_effect=AssertionError("postgres_binary_v1 must not read serving sidecar DB artifact")),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "lookup_serving_by_provider_set_patterns",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("postgres_binary_v1 must not read local provider-set sidecar")
        ),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("postgres_binary_v1 must not resolve local serving sidecar path")
        ),
    )


@pytest.mark.asyncio
async def test_broad_npi_postgres_binary_reverse_avoids_local_sidecars(monkeypatch):
    """PostgreSQL binary broad NPI paging should not resolve pod-local serving files."""
    provider_set, price_a, price_b, session, tables = _postgres_binary_reverse_fixture()
    _patch_binary_reverse_guards(
        monkeypatch,
        price_a_id=price_a,
        price_b_id=price_b,
    )

    procedure_matches = await ptg2_serving._ptg2_manifest_provider_procedure_rows_from_reverse_sidecar(
        session,
        tables,
        provider_set_ids=(provider_set,),
        requested_plan="TESTPLAN001",
        code_value="",
        code_system=None,
        q_text="",
        code_context=None,
        source_trace_set_hash="trace-set",
        network_names=["network"],
        limit=2,
        offset=0,
        apply_window=True,
    )

    assert [
        (
            procedure_match["reported_code"],
            procedure_match["provider_set_global_id_128"],
            procedure_match["provider_count"],
            procedure_match["price_set_global_id_128"],
        )
        for procedure_match in procedure_matches
    ] == [
        ("70551", provider_set, 5, price_a),
        ("70552", provider_set, 11, price_b),
    ]


def _malformed_postgres_binary_tables(arch_version="postgres_binary_v1"):
    return ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        arch_version=arch_version,
        artifacts={
            "serving_by_code": {"path": "/missing/local/cache/serving_by_code.ptg2sbc"},
            "serving_by_provider_set": {"path": "/missing/local/cache/serving_by_provider_set.ptg2sbp"},
            "provider_forward": {"path": "/missing/local/cache/provider_forward.ptg2gsc"},
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("arch_version", ["postgres_binary_v1", "postgres_binary_v2"])
async def test_postgres_binary_lookup_fails_closed_without_binary_table(monkeypatch, arch_version):
    """PostgreSQL binary snapshots must not fall back to serving sidecar files."""
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not resolve local sidecar")),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="missing serving_binary_table"):
        await ptg2_serving._ptg2_manifest_lookup_serving_by_code_sidecar(
            FakeSession([]),
            _malformed_postgres_binary_tables(arch_version),
            7,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("arch_version", ["postgres_binary_v1", "postgres_binary_v2"])
async def test_postgres_binary_reverse_fails_closed_without_binary_table(monkeypatch, arch_version):
    """PostgreSQL binary reverse lookup must not fall back to provider-set files."""
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not resolve local sidecar")),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="missing serving_binary_table"):
        await ptg2_serving._ptg2_manifest_lookup_serving_by_provider_set_patterns(
            FakeSession([]),
            _malformed_postgres_binary_tables(arch_version),
            3,
        )


@pytest.mark.parametrize("arch_version", ["postgres_binary_v1", "postgres_binary_v2"])
def test_postgres_binary_sync_membership_fails_closed_without_db_storage(monkeypatch, arch_version):
    """PostgreSQL binary membership reads must not resolve local artifacts."""
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not resolve local sidecar")),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="requires PostgreSQL artifact storage"):
        ptg2_serving._ptg2_manifest_sidecar_members(
            _malformed_postgres_binary_tables(arch_version),
            "provider_forward",
            "0000000000000000000000000000000a",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("arch_version", ["postgres_binary_v1", "postgres_binary_v2"])
async def test_postgres_binary_async_membership_fails_closed_without_db_storage(monkeypatch, arch_version):
    """PostgreSQL binary async membership reads must not resolve local artifacts."""
    monkeypatch.setattr(
        ptg2_serving,
        "_resolve_ptg2_manifest_sidecar_path",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not resolve local sidecar")),
    )

    with pytest.raises(ptg2_serving.PTG2ManifestArtifactError, match="requires PostgreSQL artifact storage"):
        await ptg2_serving._ptg2_manifest_sidecar_members_many_async(
            FakeSession([]),
            _malformed_postgres_binary_tables(arch_version),
            "provider_forward",
            ("0000000000000000000000000000000a",),
        )


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_projects_lean_serving_table_rows():
    provider_set_id = "0000000000000000000000000000000a"
    price_set_id = "00000000000000000000000000000101"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "code_key": 7,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "rate_count": 1,
                    }
                ]
            ),
            FakeResult(rows=[{"provider_set_key": 3, "provider_set_global_id_128": provider_set_id}]),
            FakeResult(
                rows=[
                    {
                        "code_order": 0,
                        "code_key": 7,
                        "provider_set_key": 3,
                        "plan_id": "TESTPLAN001",
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "provider_set_global_id_128": provider_set_id,
                        "provider_count": 1,
                        "price_set_global_id_128": price_set_id,
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        storage="manifest_snapshot",
        serving_table="mrf.ptg2_serving_manifest_snap",
        code_count_table="mrf.ptg2_code_count_manifest_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_manifest_snap",
        serving_table_layout="lean_provider_key_v1",
    )

    rows = await ptg2_serving._ptg2_manifest_provider_procedure_rows_from_lean_table(
        session,
        tables,
        provider_set_ids=(provider_set_id,),
        requested_plan="TESTPLAN001",
        code_value="70551",
        code_system="CPT",
        q_text="",
        code_context={},
        source_trace_set_hash="trace-set",
        network_names=["network-a"],
        limit=25,
        offset=0,
    )

    assert rows == [
        {
            "serving_content_hash_128": ptg2_serving._ptg2_manifest_serving_content_hash(
                7,
                3,
                price_set_id,
            ),
            "plan_id": "TESTPLAN001",
            "reported_code_system": "CPT",
            "reported_code": "70551",
            "procedure_global_id_128": None,
            "provider_set_global_id_128": provider_set_id,
            "provider_count": 1,
            "price_set_global_id_128": price_set_id,
            "source_trace_set_hash": "trace-set",
            "network_names": ["network-a"],
        }
    ]
    row_sql = str(session.calls[2][0][0])
    row_params = session.calls[2][0][1]
    assert "serving_content_hash_128" not in row_sql
    assert "JOIN mrf.ptg2_code_count_manifest_snap code_count" in row_sql
    assert "JOIN mrf.ptg2_provider_set_dict_manifest_snap provider_sets" in row_sql
    assert row_params["code_keys"] == [7]
    assert row_params["provider_set_keys"] == [3]


@pytest.mark.asyncio
async def test_ptg2_provider_procedures_requires_normalized_provider_membership():
    session = FakeSession(
        [
            "snap-token",
            {
                "table": "mrf.ptg2_serving_rate_compact_token",
                "price_code_set_table": "mrf.ptg2_price_code_set_token",
                "price_atom_table": "mrf.ptg2_price_atom_token",
                "price_set_entry_table": "mrf.ptg2_price_set_entry_token",
                "procedure_table": "mrf.ptg2_procedure_token",
                "provider_set_table": "mrf.ptg2_provider_set_token",
                "provider_group_member_table": "mrf.ptg2_provider_group_member_token",
            },
            "mrf.ptg2_serving_rate_compact_token",
        ]
    )

    payload = await ptg2_serving.search_ptg2_provider_procedures(
        session,
        1083311500,
        {"plan_id": "TESTPLAN001", "code": "99213", "code_system": "CPT"},
        FakePagination(),
    )

    assert payload is None


@pytest.mark.asyncio
async def test_compact_serving_uses_snapshot_price_and_procedure_tables():
    session = FakeSession([FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    assert "ptg2_price_set_token" not in sql
    assert "FROM mrf.ptg2_price_set_entry_token pse" in sql
    assert "JOIN mrf.ptg2_price_atom_token pa" in sql
    assert "LEFT JOIN mrf.ptg2_price_code_set_token service_set" in sql
    assert "LEFT JOIN mrf.ptg2_price_code_set_token modifier_set" in sql
    assert "pse.price_set_hash = r.price_set_hash" in sql
    assert "ps.canonical_payload" not in sql
    assert "FROM mrf.ptg2_procedure_token proc" in sql


@pytest.mark.asyncio
async def test_compact_serving_requires_normalized_price_tables():
    session = FakeSession([FakeResult(rows=[])])
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_rate_compact_token",
        procedure_table="mrf.ptg2_procedure_token",
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    assert session.calls == []


@pytest.mark.asyncio
async def test_compact_serving_geo_search_allows_missing_specialty(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "99213", "zip5": "60601"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "addr_filter.npi = pgm_filter.npi" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5" in sql
    assert "npi_taxonomy" not in sql
    assert "specialty_like" not in params


@pytest.mark.asyncio
async def test_compact_serving_zip_centroid_search_allows_same_zip_or_radius(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "70551",
            "zip5": "60601",
            "lat": 41.8820,
            "long": -87.6278,
            "radius_miles": 10.0,
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "(LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5 OR (" in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert "CAST(:geo_radius_miles AS double precision)" in sql
    assert params["zip5"] == "60601"
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_coordinate_search_filters_npi_addresses(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "70551",
            "lat": 29.7604,
            "long": -95.3698,
            "radius_miles": 10.0,
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" in sql
    assert "addr_filter.long::float8 BETWEEN :geo_min_long AND :geo_max_long" in sql
    assert "ll_to_earth" not in sql
    assert "2 * 3958.7613 * asin" in sql
    assert "radians(CAST(:geo_lat AS double precision))" in sql
    assert ") <= CAST(:geo_radius_miles AS double precision)" in sql
    assert params["geo_lat"] == 29.7604
    assert params["geo_long"] == -95.3698
    assert params["geo_radius_miles"] == 10.0


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_fallback_projects_address_distance(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(["mrf.npi", FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "29888",
            "zip5": "62401",
            "lat": 39.11952,
            "long": -88.56418,
            "radius_miles": 100.0,
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    params = session.calls[-1][0][1]
    assert "addr.distance_miles, addr.zip_match_type, addr.anchor_zip5, addr.zip_radius_miles" in sql
    assert "CASE WHEN LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5 THEN 0.0 ELSE" in sql
    assert "AS distance_miles" in sql
    assert ":zip5 AS anchor_zip5, CAST(:geo_radius_miles AS double precision) AS zip_radius_miles" in sql
    assert "ORDER BY distance_miles ASC NULLS LAST, r.reported_code_system, r.reported_code" in sql
    assert "ORDER BY CASE WHEN LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5 THEN 0 ELSE 1 END" in sql
    assert params["zip5"] == "62401"
    assert params["geo_radius_miles"] == 100.0


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_filters_coordinates_with_unified(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(
                rows=[
                    {
                        "provider_group_global_id_128": group_id,
                        "npi": 1234567890,
                        "location_hash": "entity_address_unified:1234567890:primary:1",
                        "state": "CA",
                        "city": "GLENDALE",
                        "zip5": "91204",
                        "distance_miles": 3.25,
                        "zip_match_type": "radius",
                        "anchor_zip5": "91204",
                        "zip_radius_miles": 10.0,
                        "telephone_number": "8185551212",
                        "fax_number": "8185551213",
                        "location_source": "entity_address_unified",
                        "location_confidence_code": "entity_address_unified",
                        "address_payload": (
                            '{"address_key":"00000000-0000-0000-0000-000000000001",'
                            '"telephone_number":"8185551212","fax_number":"8185551213",'
                            '"lat":34.14024131,"long":-118.255125}'
                        ),
                        "taxonomy_codes": ["207XS0114X"],
                        "specialties": ["Orthopaedic Surgery Physician"],
                        "classifications": ["Orthopaedic Surgery"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Orthopaedic Surgery Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "TiC provider",
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )

    async def fake_members_many(_session, _serving_tables, name, group_ids, **_kwargs):
        assert name == "provider_inverted"
        assert group_ids == (group_id,)
        return {group_id: (provider_set_id,)}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_members_many)

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
    )

    assert provider_set_ids == {provider_set_id}
    provider = providers_by_set[provider_set_id][0]
    assert provider["npi"] == 1234567890
    assert provider["zip5"] == "91204"
    assert provider["distance_miles"] == 3.25
    assert provider["zip_match_type"] == "radius"
    assert provider["anchor_zip5"] == "91204"
    assert provider["zip_radius_miles"] == 10.0
    assert provider["telephone_number"] == "8185551212"
    assert provider["fax_number"] == "8185551213"
    assert provider["taxonomy_codes"] == ["207XS0114X"]
    assert provider["classifications"] == ["Orthopaedic Surgery"]
    assert provider["specializations"] == ["Sports Medicine"]
    assert provider["primary_specialization"] == "Sports Medicine"
    address = json.loads(provider["address_payload"])
    assert address["address_key"] == "00000000-0000-0000-0000-000000000001"
    assert address["telephone_number"] == "8185551212"
    assert address["fax_number"] == "8185551213"
    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    assert "scoped_member_npis AS MATERIALIZED" not in sql
    assert "FROM mrf.ptg2_provider_group_member_snap pgm_scope" in sql
    assert "FROM mrf.entity_address_unified addr" in sql
    assert "WHERE pgm_scope.npi = addr.npi" in sql
    assert "AND EXISTS (" in sql
    assert "raw_location_npis AS" in sql
    assert "location_npis AS MATERIALIZED" in sql
    assert "ORDER BY zip_rank, distance_miles ASC NULLS LAST, npi" in sql
    assert "AS distance_miles" in sql
    assert "AS zip_match_type" in sql
    assert "CAST(:geo_radius_miles AS double precision) AS zip_radius_miles" in sql
    assert "addr.telephone_number" in sql
    assert "'telephone_number', " in sql
    assert "AS telephone_number" in sql
    assert "AS fax_number" in sql
    assert "addr.address_key" in sql
    assert "addr.lat IS NOT NULL" in sql
    assert "addr.long IS NOT NULL" in sql
    assert "addr.type IN ('primary', 'secondary', 'practice', 'site')" in sql
    assert "ST_DWithin(" in sql
    assert "Geography(ST_MakePoint((addr.long)::double precision, (addr.lat)::double precision))" in sql
    assert "addr.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" not in sql
    assert "addr.long::float8 BETWEEN :geo_min_long AND :geo_max_long" not in sql
    assert "ll_to_earth" not in sql
    assert "2 * 3958.7613 * asin" in sql
    assert "radians(CAST(:geo_lat AS double precision))" in sql
    assert "COALESCE(addr.address_precision, '') <> 'city_zip'" in sql
    assert "fallback_addresses AS MATERIALIZED" in sql
    assert "JOIN location_npis loc ON loc.npi = na.npi" in sql
    assert "LEFT JOIN fallback_addresses na" in sql
    assert "LEFT JOIN LATERAL (\n                SELECT na.first_line" not in sql
    # Taxonomy is resolved once per NPI in the located_with_tax CTE (before the
    # provider_group_member fan-out) and surfaced via addr.* in the final SELECT.
    assert "located_with_tax AS MATERIALIZED" in sql
    assert "FROM located_with_tax addr" in sql
    assert "COALESCE(addr.classifications, ARRAY[]::varchar[]) AS classifications" in sql
    assert "COALESCE(addr.specializations, ARRAY[]::varchar[]) AS specializations" in sql
    assert "WHERE nt.npi = loc.npi" in sql
    assert params["geo_lat"] == 34.14024131
    assert params["geo_long"] == -118.255125
    assert params["geo_radius_miles"] == 10.0
    assert params["address_types"] == ["practice", "primary"]


def _expect_member_first_manifest_sql(location_sql: str) -> None:
    """Assert plan/code-constrained manifest location lookup starts from rated member NPIs."""
    assert "scoped_member_npis AS MATERIALIZED" in location_sql
    assert "FROM scoped_member_npis scope_npis" in location_sql
    assert "WHERE addr_probe.npi = scope_npis.npi" in location_sql
    assert "WHERE pgm_scope.npi = addr.npi" not in location_sql


def _fail_manifest_sidecar_usage(*_args, **_kwargs) -> None:
    """Raise when a component-table manifest test unexpectedly touches sidecars."""
    raise AssertionError("sidecar should not be used")


@pytest.mark.asyncio
async def test_manifest_location_uses_component_table(monkeypatch):
    """Component-backed manifest location lookup should prefilter by rated provider members."""
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        id_storage="uuid",
    )

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    populated_sql = str(session.calls[1][0][0])
    location_sql = str(session.calls[3][0][0])
    location_params = session.calls[3][0][1]
    assert "SELECT EXISTS" in populated_sql
    for expected_sql in (
        "rate_provider_groups AS MATERIALIZED",
        "FROM mrf.ptg2_serving_manifest_snap rate_scope",
        "JOIN mrf.ptg2_provider_set_component_snap psc",
        "rate_scope.plan_id = :location_plan_id",
        "rate_scope.reported_code = :location_reported_code",
        "JOIN rate_provider_groups rpg",
    ):
        assert expected_sql in location_sql
    _expect_member_first_manifest_sql(location_sql)
    assert location_params["location_plan_id"] == "TESTPLAN001"
    assert location_params["location_reported_code"] == "90837"
    assert location_params["location_reported_code_system"] == "CPT"
    component_sql = str(session.calls[-1][0][0])
    component_params = session.calls[-1][0][1]
    assert "FROM mrf.ptg2_provider_set_component_snap" in component_sql
    assert "CAST(:group_ids AS uuid[])" in component_sql
    assert component_params["group_ids"] == [group_id]
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_uses_component_table_with_provider_group_locations(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(side_effect=AssertionError("component-backed lookup should not use sidecar rate scope")),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = str(session.calls[3][0][0])
    assert "rate_provider_groups AS MATERIALIZED" in location_sql
    assert "FROM mrf.ptg2_provider_group_location_snap loc" in location_sql
    assert "JOIN rate_provider_groups rpg_location" in location_sql
    assert "loc.taxonomy_array && (" in location_sql
    assert "nt.healthcare_provider_taxonomy_code IN" not in location_sql
    assert "FROM mrf.npi n_entity" in location_sql
    assert "FROM mrf.entity_address_unified addr" not in location_sql
    assert "FROM mrf.ptg2_provider_group_member_snap pgm_scope" not in location_sql
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_classification_filter_scopes_via_semijoin_and_array(monkeypatch):
    """Classification filters must prefilter in-index and semi-join, not probe per row.

    Regression for plan-scoped 502s with plan_id + zip5 + classification
    (e.g. 'Orthopaedic Surgery', CPT 99204 - no inference rule): the
    classification resolves no taxonomy codes, so the code-driven
    taxonomy_array overlap was empty and the correlated EXISTS (with a nucc
    join) ran once per location row of a dense-metro zip radius.
    """
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "plan_id": "TESTPLAN001",
            "code": "99204",
            "code_system": "CPT",
            "zip5": "60601",
            "classification": "Orthopaedic Surgery",
            "limit": "20",
        },
        candidate_limit=20,
        plan_id="TESTPLAN001",
    )

    location_sql = str(session.calls[3][0][0])
    location_params = session.calls[3][0][1]
    assert "FROM mrf.ptg2_provider_group_location_snap loc" in location_sql
    # Precise classification check is an uncorrelated semi-join...
    assert "loc.npi IN (SELECT manifest_location_table_specialty_nt.npi" in location_sql
    assert "manifest_location_table_specialty_nt.npi = loc.npi" not in location_sql
    assert (
        "UPPER(COALESCE(manifest_location_table_specialty_nt.healthcare_provider_primary_taxonomy_switch, '')) = 'Y'"
        in location_sql
    )
    # ...and the classification also derives an in-index taxonomy_array
    # prefilter so the zip/GIN indexes narrow rows before any precise check.
    assert "loc.taxonomy_array && (" in location_sql
    assert "LOWER(COALESCE(classification, '')) = LOWER(:manifest_location_table_array_classification)" in location_sql
    assert location_params["manifest_location_table_array_classification"] == "Orthopaedic Surgery"
    assert location_params["manifest_location_table_specialty_classification"] == "Orthopaedic Surgery"


@pytest.mark.asyncio
async def test_manifest_location_uses_provider_group_rate_scope_table_when_declared(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        provider_group_rate_scope_table="mrf.ptg2_provider_group_rate_scope_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(side_effect=AssertionError("component-backed lookup should not use sidecar rate scope")),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    route_scope_sql = str(session.calls[2][0][0])
    route_scope_params = session.calls[2][0][1]
    assert "SELECT EXISTS" in route_scope_sql
    assert "FROM mrf.ptg2_provider_group_rate_scope_snap" in route_scope_sql
    assert route_scope_params["plan_id"] == "TESTPLAN001"
    assert route_scope_params["reported_code"] == "90837"
    assert route_scope_params["reported_code_system"] == "CPT"
    location_sql = str(session.calls[4][0][0])
    assert "rate_provider_groups AS MATERIALIZED" not in location_sql
    assert "JOIN mrf.ptg2_provider_group_rate_scope_snap rpg_location" in location_sql
    assert "rpg_location.plan_id = :location_plan_id" in location_sql
    assert "rpg_location.reported_code = :location_reported_code" in location_sql
    assert "rpg_location.reported_code_system = :location_reported_code_system" in location_sql
    assert "FROM mrf.ptg2_provider_group_member_snap pgm_scope" not in location_sql
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_uses_rate_scope_table_without_provider_group_locations(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_rate_scope_table="mrf.ptg2_provider_group_rate_scope_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(side_effect=AssertionError("rate-scope table should replace sidecar rate scope")),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = _fake_call_sql(session, "raw_location_npis AS")
    assert "rate_provider_groups AS MATERIALIZED" in location_sql
    assert "FROM mrf.ptg2_provider_group_rate_scope_snap rate_scope" in location_sql
    assert "FROM mrf.ptg2_serving_manifest_snap rate_scope" not in location_sql
    assert "JOIN mrf.ptg2_provider_set_component_snap psc" not in location_sql
    assert "FROM mrf.ptg2_provider_group_location_snap loc" not in location_sql
    assert "JOIN rate_provider_groups rpg_scope" in location_sql
    assert "FROM mrf.entity_address_unified addr" in location_sql
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_rate_provider_groups_sidecar_supports_lean_provider_key_layout(monkeypatch):
    provider_set_id = "00000000000000000000000000000012"
    group_id = "00000000000000000000000000000011"
    session = FakeSession(
        [
            FakeResult(rows=[{"provider_set_global_id_128": provider_set_id}]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        code_count_table="mrf.ptg2_code_count_snap",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dict_snap",
        serving_table_layout="lean_provider_key_v1",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )

    async def fake_sidecar_many(_session, _serving_tables, sidecar_name, owner_ids, **_kwargs):
        assert sidecar_name == "provider_forward"
        assert owner_ids == (provider_set_id,)
        return {provider_set_id: (group_id,)}

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_sidecar_many)

    result = await ptg2_serving._manifest_rate_provider_groups_from_sidecar(
        session,
        tables,
        serving_table=tables.serving_table,
        plan_id="TESTPLAN001",
        reported_code="90837",
        code_system="CPT",
    )

    sql = str(session.calls[0][0][0])
    assert result == (group_id,)
    assert "JOIN mrf.ptg2_code_count_snap code_count" in sql
    assert "JOIN mrf.ptg2_provider_set_dict_snap provider_set_dictionary" in sql
    assert "code_count.code_key = serving.code_key" in sql
    assert "provider_set_dictionary.provider_set_key = serving.provider_set_key" in sql
    assert "code_count.plan_id = :plan_id" in sql
    assert "code_count.reported_code = :reported_code" in sql


@pytest.mark.asyncio
async def test_manifest_rate_scope_combines_revenue_code_forms(monkeypatch):
    """Provider scope must include sets stored under both revenue-code forms."""
    canonical_provider_set_id = "00000000000000000000000000000012"
    raw_provider_set_id = "00000000000000000000000000000013"
    canonical_group_id = "00000000000000000000000000000021"
    raw_group_id = "00000000000000000000000000000022"
    session = FakeSession([FakeResult(rows=_revenue_code_count_rows())])
    tables = ptg2_serving.PTG2ServingTables(
        serving_binary_table="mrf.ptg2_serving_binary_fixture",
        code_count_table="mrf.ptg2_code_count_fixture",
        provider_set_dictionary_table="mrf.ptg2_provider_set_dictionary_fixture",
        serving_table_layout="lean_provider_key_v1",
        artifacts={"provider_forward": {"path": "postgres://provider-forward"}},
    )
    sidecar_reader = AsyncMock(
        side_effect=[
            (SimpleNamespace(provider_set_key=1),),
            (SimpleNamespace(provider_set_key=2),),
        ]
    )
    provider_set_reader = AsyncMock(
        return_value={1: canonical_provider_set_id, 2: raw_provider_set_id}
    )
    member_reader = AsyncMock(
        return_value={
            canonical_provider_set_id: (canonical_group_id,),
            raw_provider_set_id: (raw_group_id,),
        }
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_lookup_serving_by_code_sidecar", sidecar_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_provider_set_ids_for_keys", provider_set_reader)
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", member_reader)

    group_ids = await ptg2_serving._manifest_rate_provider_groups_from_sidecar(
        session,
        tables,
        serving_table=None,
        plan_id="TESTPLAN001",
        reported_code="0110",
        code_system="RC",
    )

    query_sql = str(session.calls[0][0][0])
    assert group_ids == (canonical_group_id, raw_group_id)
    assert provider_set_reader.await_args.args[2] == [1, 2]
    assert member_reader.await_args.args[3] == (canonical_provider_set_id, raw_provider_set_id)
    assert "LIMIT 1" not in query_sql
    assert "code_count.reported_code IN (:reported_code, :reported_code_1)" in query_sql


@pytest.mark.asyncio
async def test_manifest_location_phone_fallback_scopes_by_missing_npi(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    address_key = "00000000-0000-0000-0000-000000000001"
    premise_key = "00000000-0000-0000-0000-000000000002"
    location_by_field = {
        "provider_group_global_id_128": group_id,
        "npi": 1234567890,
        "address_key": address_key,
        "premise_key": premise_key,
        "address_payload": {"address_key": address_key},
    }
    phone_fallback = {
        "npi": 1234567890,
        "address_key": address_key,
        "premise_key": premise_key,
        "telephone_number": "3125550100",
        "phone_number": "3125550100",
        "phone_extension": None,
        "fax_number": None,
        "fax_number_digits": None,
        "fax_extension": None,
        "type": "practice",
        "checksum": "phone-row",
    }
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[phone_fallback]),
            False,
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        provider_group_rate_scope_table="mrf.ptg2_provider_group_rate_scope_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", _fail_manifest_sidecar_usage)
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(side_effect=AssertionError("component-backed lookup should not use sidecar rate scope")),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    fallback_call = next(
        (call for call in session.calls if "fallback_npis" in str(call[0][0])),
        None,
    )
    assert fallback_call is not None
    fallback_sql = str(fallback_call[0][0])
    fallback_params = fallback_call[0][1]
    assert "npi = ANY(CAST(:fallback_npis AS bigint[]))" in fallback_sql
    assert "type = ANY(CAST(:fallback_address_types AS varchar[]))" in fallback_sql
    assert "premise_key = ANY" not in fallback_sql
    assert fallback_params["fallback_npis"] == [1234567890]
    assert fallback_params["address_keys"] == [address_key]
    assert provider_set_ids == {provider_set_id}
    provider = providers_by_set[provider_set_id][0]
    assert provider["telephone_number"] == "3125550100"


@pytest.mark.asyncio
async def test_manifest_location_zip_radius_uses_literal_taxonomy_lateral_fast_path(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    ptg2_serving._PTG2_TAXONOMY_INT_CODE_CACHE.clear()
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[{"int_code": 101}, {"int_code": 202}]),
            FakeResult(scalar=True),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        provider_group_rate_scope_table="mrf.ptg2_provider_group_rate_scope_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "zip5": "60601",
            "lat": "41.88526",
            "long": "-87.62194",
            "radius_miles": "75",
            "limit": "5",
        },
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = _fake_call_sql(session, "raw_location_rows AS")
    assert "location_zip_scope AS MATERIALIZED" in location_sql
    assert "FROM mrf.geo_zip_lookup anchor" in location_sql
    assert "JOIN LATERAL" in location_sql
    assert "loc.zip5 = zip_scope.zip5" in location_sql
    assert "loc.taxonomy_array && ARRAY[101,202]::integer[]" in location_sql
    assert "OFFSET 0" in location_sql
    assert "JOIN mrf.ptg2_provider_group_rate_scope_snap rpg_location" in location_sql
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_rate_scope_table_falls_back_when_pair_uncovered(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            FakeResult(scalar=False),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_location_table="mrf.ptg2_provider_group_location_snap",
        provider_group_rate_scope_table="mrf.ptg2_provider_group_rate_scope_snap",
        artifacts={"provider_forward": {"path": "provider-forward.bin"}},
        id_storage="uuid",
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = str(session.calls[4][0][0])
    assert "rate_provider_groups AS MATERIALIZED" in location_sql
    assert "JOIN mrf.ptg2_provider_group_rate_scope_snap rpg_location" not in location_sql
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_rate_prefilter_allows_missing_code_system(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    component_by_field = {"provider_group_global_id_128": group_id, "provider_set_global_id_128": provider_set_id}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(scalar=True),
            False,
            FakeResult(rows=[location_by_field]),
            FakeResult(rows=[component_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        id_storage="uuid",
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = str(session.calls[3][0][0])
    location_params = session.calls[3][0][1]
    assert "rate_provider_groups AS MATERIALIZED" in location_sql
    assert "rate_scope.reported_code = :location_reported_code" in location_sql
    assert "rate_scope.reported_code_system = :location_reported_code_system" not in location_sql
    assert location_params["location_plan_id"] == "TESTPLAN001"
    assert location_params["location_reported_code"] == "90837"
    assert "location_reported_code_system" not in location_params
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_location_uses_sidecar_rate_groups_without_component_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    location_by_field = {"provider_group_global_id_128": group_id, "npi": 1234567890}
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[{"provider_set_global_id_128": provider_set_id}]),
            False,
            FakeResult(rows=[location_by_field]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_manifest_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={
            "provider_forward": {"name": "provider_forward", "path": "/tmp/provider_forward.ptg2sc"},
            "provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"},
        },
        id_storage="uuid",
    )

    async def fake_members_many(_session, _serving_tables, name, owner_ids, **_kwargs):
        if name == "provider_forward":
            assert owner_ids == (provider_set_id,)
            return {provider_set_id: (group_id,)}
        if name == "provider_inverted":
            assert owner_ids == (group_id,)
            return {group_id: (provider_set_id,)}
        raise AssertionError(name)

    monkeypatch.setattr(ptg2_serving, "_ptg2_manifest_sidecar_members_many_async", fake_members_many)

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "90837", "code_system": "CPT", "lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    rate_set_sql = str(session.calls[1][0][0])
    rate_set_params = session.calls[1][0][1]
    location_sql = str(session.calls[3][0][0])
    location_params = session.calls[3][0][1]
    assert "SELECT provider_set_global_id_128" in rate_set_sql
    assert "GROUP BY provider_set_global_id_128" in rate_set_sql
    assert "FROM mrf.ptg2_serving_manifest_snap" in rate_set_sql
    assert rate_set_params["plan_id"] == "TESTPLAN001"
    assert rate_set_params["reported_code"] == "90837"
    assert rate_set_params["reported_code_system"] == "CPT"
    assert "rate_provider_groups AS MATERIALIZED" not in location_sql
    _expect_member_first_manifest_sql(location_sql)
    assert "pgm_scope.provider_group_global_id_128 = ANY(CAST(:location_rate_provider_group_ids AS uuid[]))" in location_sql
    assert "pgm.provider_group_global_id_128 = ANY(CAST(:location_rate_provider_group_ids AS uuid[]))" in location_sql
    assert location_params["location_rate_provider_group_ids"] == [group_id]
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_manifest_sets_by_group_falls_back_for_empty_component_table(monkeypatch):
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession([FakeResult(rows=[])])
    tables = ptg2_serving.PTG2ServingTables(
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
        id_storage="uuid",
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )

    sets_by_group = await ptg2_serving._manifest_sets_by_group(session, tables, (group_id,))

    assert sets_by_group == {group_id: (provider_set_id,)}


@pytest.mark.asyncio
async def test_manifest_location_provider_phone_fallback_uses_address_key_npi_index(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    address_key = "00000000-0000-0000-0000-000000000001"
    premise_key = "00000000-0000-0000-0000-000000000002"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(
                rows=[
                    {
                        "provider_group_global_id_128": group_id,
                        "npi": 1234567890,
                        "address_key": address_key,
                        "premise_key": premise_key,
                        "location_hash": "entity_address_unified:1234567890:practice:1",
                        "state": "CA",
                        "city": "GLENDALE",
                        "zip5": "91204",
                        "distance_miles": 3.25,
                        "zip_match_type": "radius",
                        "anchor_zip5": "91204",
                        "zip_radius_miles": 10.0,
                        "telephone_number": None,
                        "fax_number": None,
                        "location_source": "entity_address_unified",
                        "location_confidence_code": "entity_address_unified",
                        "address_payload": (
                            '{"address_key":"00000000-0000-0000-0000-000000000001",'
                            '"premise_key":"00000000-0000-0000-0000-000000000002"}'
                        ),
                        "taxonomy_codes": [],
                        "specialties": [],
                        "classifications": [],
                        "specializations": [],
                        "primary_specialty": None,
                        "primary_specialization": None,
                        "provider_name": "TiC provider",
                    }
                ]
            ),
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "address_key": address_key,
                        "premise_key": premise_key,
                        "telephone_number": "8185551212",
                        "fax_number": "8185551213",
                        "type": "practice",
                        "checksum": 1,
                    }
                ]
            ),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
    )

    provider = providers_by_set[provider_set_id][0]
    assert provider_set_ids == {provider_set_id}
    assert provider["telephone_number"] == "8185551212"
    assert provider["fax_number"] == "8185551213"
    fallback_sql = str(session.calls[3][0][0])
    assert "address_key = ANY(CAST(:address_keys AS uuid[]))" in fallback_sql
    assert "npi = ANY(CAST(:fallback_npis AS bigint[]))" in fallback_sql
    assert "premise_key = ANY" not in fallback_sql
    assert "address_key::text = ANY(CAST(:address_keys AS text[]))" not in fallback_sql


def test_sort_ptg2_manifest_provider_items_supports_cost_and_distance():
    items = [
        {
            "provider_name": "Far cheap",
            "prices": [{"negotiated_rate": 100.0}],
            "distance_miles": 45.0,
        },
        {
            "provider_name": "Near expensive",
            "prices": [{"negotiated_rate": 500.0}],
            "distance_miles": 2.0,
        },
    ]

    by_cost = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "total_allowed_amount", "order": "asc"},
        location_filter_requested=True,
    )
    by_distance = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {"order_by": "distance", "order": "asc"},
        location_filter_requested=True,
    )

    assert [item["provider_name"] for item in by_cost] == ["Far cheap", "Near expensive"]
    assert [item["provider_name"] for item in by_distance] == ["Near expensive", "Far cheap"]


def test_merge_ptg2_provider_rate_items_preserves_same_provider_location_rates():
    items = [
        {
            "provider_name": "David Clayton Tapscott",
            "npi": "1255711370",
            "reported_code_system": "CPT",
            "reported_code": "29888",
            "location_hash": "loc-1",
            "address": {"address_key": "loc-1", "first_line": "101 E 9th St"},
            "price_set_hash": "price-a",
            "rate_pack_hash": "rate-a",
            "provider_set_hash": "provider-a",
            "prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 1074.22,
                    "billing_class": "professional",
                    "setting": "outpatient",
                }
            ],
        },
        {
            "provider_name": "David Clayton Tapscott",
            "npi": "1255711370",
            "reported_code_system": "CPT",
            "reported_code": "29888",
            "location_hash": "loc-1",
            "address": {"address_key": "loc-1", "first_line": "101 E 9th St"},
            "price_set_hash": "price-b",
            "rate_pack_hash": "rate-b",
            "provider_set_hash": "provider-b",
            "prices": [
                {
                    "negotiated_type": "negotiated",
                    "negotiated_rate": 3193.10,
                    "billing_class": "professional",
                    "setting": "outpatient",
                }
            ],
        },
    ]

    merged = ptg2_serving._merge_ptg2_provider_rate_items(items)

    assert len(merged) == 1
    assert [summary["rate"] for summary in merged[0]["price_summary"]] == [1074.22, 3193.10]
    assert merged[0]["price_set_hashes"] == ["price-a", "price-b"]
    assert merged[0]["rate_pack_hashes"] == ["rate-a", "rate-b"]
    assert merged[0]["rate_pack_count"] == 2


def test_merge_ptg2_provider_rate_items_defers_duplicate_price_summary(monkeypatch):
    calls = []
    original_price_response_fields = ptg2_serving._price_response_fields

    def count_price_response_fields(prices):
        calls.append(list(prices or []))
        return original_price_response_fields(prices)

    monkeypatch.setattr(ptg2_serving, "_price_response_fields", count_price_response_fields)
    base_item = {
        "provider_name": "Example Provider",
        "npi": "1255711370",
        "reported_code_system": "CPT",
        "reported_code": "90837",
        "location_hash": "loc-1",
        "address": {"address_key": "loc-1", "first_line": "101 E 9th St"},
        "provider_set_hash": "provider-a",
        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 100.0}],
    }
    items = []
    for idx, rate in enumerate((100.0, 150.0, 200.0), start=1):
        item = dict(base_item)
        item["price_set_hash"] = f"price-{idx}"
        item["rate_pack_hash"] = f"rate-{idx}"
        item["prices"] = [{"negotiated_type": "negotiated", "negotiated_rate": rate}]
        items.append(item)

    merged = ptg2_serving._merge_ptg2_provider_rate_items(items)

    assert len(merged) == 1
    assert len(calls) == 2
    assert [summary["rate"] for summary in merged[0]["price_summary"]] == [100.0, 150.0, 200.0]
    assert merged[0]["rate_pack_count"] == 3


def test_merge_ptg2_provider_rate_items_reuses_presummarized_unique_prices(monkeypatch):
    def fail_price_response_fields(_prices):
        raise AssertionError("unique presummarized rows should not rebuild price fields")

    monkeypatch.setattr(ptg2_serving, "_price_response_fields", fail_price_response_fields)
    item = {
        "provider_name": "Example Provider",
        "npi": "1255711370",
        "reported_code_system": "CPT",
        "reported_code": "90837",
        "location_hash": "loc-1",
        "address": {"address_key": "loc-1", "first_line": "101 E 9th St"},
        "price_set_hash": "price-a",
        "rate_pack_hash": "rate-a",
        "provider_set_hash": "provider-a",
        "prices": [{"negotiated_rate": 107.0}],
        "tic_prices": [{"negotiated_rate": 107.0}],
        "price_summary": [{"rate": 107.0}],
    }

    merged = ptg2_serving._merge_ptg2_provider_rate_items([item])

    assert len(merged) == 1
    assert merged[0]["prices"] == [{"negotiated_rate": 107.0}]
    assert merged[0]["tic_prices"] == [{"negotiated_rate": 107.0}]
    assert merged[0]["price_summary"] == [{"rate": 107.0}]


def test_compact_item_promotes_location_phone_from_address_payload():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "first_line": "100 Network Way",
                "city": "Chicago",
                "state": "IL",
                "telephone_number": "3125551212",
                "fax_number": "3125551213",
            },
            "prices": [],
        },
        {},
    )

    assert item["phone"] == "3125551212"
    assert item["phone_number"] == "3125551212"
    assert item["telephone_number"] == "3125551212"
    assert item["fax_number"] == "3125551213"
    assert item["address"]["telephone_number"] == "3125551212"


def test_compact_item_preserves_canonical_contact_fields_from_address_payload():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "first_line": "100 Network Way",
                "city": "Chicago",
                "state": "IL",
                "telephone_number": "(312) 555-1212 ext. 45",
                "phone_number": "3125551212",
                "phone_extension": "45",
                "fax_number": "(312) 555-1213",
                "fax_number_digits": "3125551213",
                "fax_extension": "9",
            },
            "prices": [],
        },
        {},
    )

    assert item["phone"] == "(312) 555-1212 ext. 45"
    assert item["telephone_number"] == "(312) 555-1212 ext. 45"
    assert item["phone_number"] == "3125551212"
    assert item["phone_extension"] == "45"
    assert item["fax_number"] == "(312) 555-1213"
    assert item["fax_number_digits"] == "3125551213"
    assert item["fax_extension"] == "9"


def test_compact_item_marks_unified_address_as_inferred_from_provider_identity():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "location_confidence_code": "entity_address_unified",
            "address_payload": {
                "first_line": "900 W Temple Ave",
                "city_name": "EFFINGHAM",
                "state_name": "IL",
                "postal_code": "62401",
                "address_precision": "street",
                "address_sources": ["nppes"],
                "source_count": 1,
                "source_mask": 65,
                "address_source_mask": 1,
                "multi_source_confirmed": False,
            },
            "network_names": ["C2"],
            "prices": [],
        },
        {"source_key": "ptg_example", "snapshot_id": "ptg2:202606:demo"},
    )

    assert item["address_precision"] == "street"
    assert item["source_key"] == "ptg_example"
    assert item["snapshot_id"] == "ptg2:202606:demo"
    assert item["network_names"] == ["C2"]
    assert item["address_verification"] == {
        "rate_network_binding": "tic_provider_group_npi_tin",
        "address_network_binding": "inferred_from_provider_identity",
        "address_evidence_level": "nppes_provider_address",
        "requires_location_confirmation": True,
        "reason": "PTG proves the NPI/TIN is in network; the displayed address comes from NPPES/provider enrichment.",
        "displayed_address_present": True,
        "network_bound_address": False,
        "location_source": "entity_address_unified",
        "location_confidence_code": "entity_address_unified",
        "address_precision": "street",
        "address_sources": ["nppes"],
        "source_count": 1,
        "multi_source_confirmed": False,
        "source_mask": 65,
        "address_source_mask": 1,
    }


def test_compact_item_with_displayable_address_passes_assurance_contract():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "location_confidence_code": "entity_address_unified",
            "address_payload": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["nppes"],
                "address_source_mask": 1,
                "source_mask": 65,
            },
            "network_names": ["C2"],
            "prices": [],
        },
        {"source_key": "ptg_example", "snapshot_id": "ptg2:202606:demo"},
    )

    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})

    assert summary["ok"] is True
    assert summary["address_verification_rows"] == 1
    assert summary["displayed_address_rows"] == 1
    assert summary["issues"] == []


def test_compact_item_can_suppress_unverified_plan_address_when_requested():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "location_confidence_code": "entity_address_unified",
            "address_payload": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["nppes"],
            },
            "network_names": ["C2"],
            "prices": [],
        },
        {"plan_id": "TESTPLAN001", "include_unverified_addresses": "false"},
    )

    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["network_bound_address"] is False
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert "address" not in item
    assert "phone" not in item


def test_ptg2_default_sort_prioritizes_network_bound_addresses():
    items = [
        {
            "provider_name": "No Address",
            "npi": 3,
            "address_verification": {
                "displayed_address_present": False,
                "network_bound_address": False,
            },
        },
        {
            "provider_name": "Inferred Address",
            "npi": 2,
            "address_verification": {
                "displayed_address_present": True,
                "network_bound_address": False,
            },
        },
        {
            "provider_name": "Verified Address",
            "npi": 1,
            "address_verification": {
                "displayed_address_present": True,
                "network_bound_address": True,
            },
        },
    ]

    sorted_items = ptg2_serving._sort_ptg2_manifest_provider_items(
        items,
        {},
        location_filter_requested=False,
    )

    assert [item["provider_name"] for item in sorted_items] == [
        "Verified Address",
        "Inferred Address",
        "No Address",
    ]


def test_manifest_provider_procedure_item_includes_address_verification():
    item = ptg2_serving._ptg2_manifest_provider_procedure_item(
        npi=1234567890,
        data={
            "serving_content_hash_128": "rate-pack",
            "provider_set_global_id_128": "provider-set",
            "provider_count": 3,
            "price_set_global_id_128": "price-set",
            "reported_code": "29888",
            "reported_code_system": "CPT",
            "network_names": ["C2"],
        },
        prices=[
            {
                "negotiated_type": "negotiated",
                "negotiated_rate": 1138.57,
                "billing_class": "professional",
            }
        ],
        procedure_detail={
            "procedure_name": "ACL reconstruction",
            "procedure_description": "Repair of anterior cruciate ligament",
        },
        provider_context={
            "provider_name": "Example Surgeon",
            "location_source": "npi_address",
            "address_payload": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["nppes"],
            },
        },
        args={"snapshot_id": "ptg2:202606:test"},
    )

    assert item["npi"] == 1234567890
    assert item["procedure_code"] == "29888"
    assert item["network_names"] == ["C2"]
    assert item["address"]["first_line"] == "900 W Temple Ave"
    assert item["address_verification"]["rate_network_binding"] == "tic_provider_group_npi_tin"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["address_evidence_level"] == "nppes_provider_address"
    assert item["address_verification"]["displayed_address_present"] is True
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is True


def test_compact_item_filters_ptg_from_inferred_address_sources():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "location_confidence_code": "entity_address_unified",
            "address_payload": {
                "first_line": "900 W Temple Ave",
                "city": "Effingham",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["nppes", "ptg"],
                "address_source_mask": 1,
                "source_mask": 65,
            },
            "network_names": ["C2"],
            "prices": [],
        },
        {"source_key": "ptg_example", "snapshot_id": "ptg2:202606:demo"},
    )

    assert "address_sources" not in item
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["address_sources"] == ["nppes"]


def test_compact_item_keeps_npi_backed_provider_group_location_as_inferred():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "npi_address",
            "location_confidence_code": "npi_address",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_verification_evidence": {
                    "source": "payer_provider_group_location",
                    "provider_group_id": 1662,
                    "json_pointer": "/provider_references/0/provider_groups/0/address",
                },
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "nppes_provider_address"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True


def test_compact_item_marks_explicit_payer_location_as_payer_confirmed():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "payer_provider_group_location",
            "location_confidence_code": "payer_confirmed_location",
            "source_trace": [
                {
                    "source_file_version_id": "source-version-1",
                    "original_url": "https://example.test/in-network-rates.json.gz",
                }
            ],
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_verification_evidence": {
                    "source": "payer_provider_group_location",
                    "provider_group_id": 1662,
                    "json_pointer": "/provider_references/0/provider_groups/0/address",
                },
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "payer_confirmed_location"
    assert item["address_verification"]["address_network_binding"] == "payer_confirmed_location"
    assert item["address_verification"]["requires_location_confirmation"] is False


def test_compact_item_does_not_mark_payer_location_without_source_trace():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "payer_provider_group_location",
            "location_confidence_code": "payer_confirmed_location",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_verification_evidence": {
                    "source": "payer_provider_group_location",
                    "provider_group_id": 1662,
                    "json_pointer": "/provider_references/0/provider_groups/0/address",
                },
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["requires_location_confirmation"] is True


def test_compact_item_does_not_mark_payer_location_without_source_record_evidence():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "payer_provider_group_location",
            "location_confidence_code": "payer_confirmed_location",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True


def test_compact_item_marks_provider_directory_network_location_as_corroborated():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "payer_directory_corroborated_location",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": True,
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location_plan",
                },
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "payer_directory_network_location"
    assert item["address_verification"]["address_network_binding"] == "payer_directory_corroborated_location"
    assert item["address_verification"]["requires_location_confirmation"] is False


def test_compact_item_promotes_provider_directory_network_name_match():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "network_names": ["C2"],
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "provider_directory_address",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_source_id": "pdfhir_aetna",
                "provider_directory_org_name": "Aetna",
                "provider_directory_plan_name": "Aetna Provider Directory",
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_context_present": True,
                "provider_directory_network_names": ["C2"],
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-1",
                        "resource_id": "network-1",
                        "name": "C2",
                        "aliases": ["C Two"],
                    }
                ],
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location",
                },
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_evidence_level"] == "payer_directory_network_location"
    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["requires_location_confirmation"] is False
    assert verification["provider_directory_plan_context_matched"] is False
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_matches"] == [
        {
            "ptg_network_name": "C2",
            "provider_directory_network_name": "C2",
            "provider_directory_network_key": "c2",
            "provider_directory_network_resource_id": "network-1",
            "provider_directory_network_ref": "Organization/network-1",
            "provider_directory_network_match_method": "canonical_network_name",
            "provider_directory_network_match_confidence": "candidate",
            "provider_directory_network_match_key": "c2",
            "provider_directory_source": "provider_directory_fhir",
            "provider_directory_source_id": "pdfhir_aetna",
            "provider_directory_org_name": "Aetna",
            "provider_directory_plan_name": "Aetna Provider Directory",
            "provider_directory_issuer_key": "aetna",
            "provider_directory_issuer_network_match_key": "aetna:c2",
        }
    ]
    assert verification["address_verification_evidence"]["matched_on"] == (
        "npi_address_key_role_location_network_name"
    )
    assert verification["address_verification_evidence"]["network_name_context_matched"] is True
    assert verification["address_verification_evidence"]["network_name_matches"] == [
        {
            "ptg_network_name": "C2",
            "provider_directory_network_name": "C2",
            "provider_directory_network_key": "c2",
            "provider_directory_network_resource_id": "network-1",
            "provider_directory_network_ref": "Organization/network-1",
            "provider_directory_network_match_method": "canonical_network_name",
            "provider_directory_network_match_confidence": "candidate",
            "provider_directory_network_match_key": "c2",
            "provider_directory_source": "provider_directory_fhir",
            "provider_directory_source_id": "pdfhir_aetna",
            "provider_directory_org_name": "Aetna",
            "provider_directory_plan_name": "Aetna Provider Directory",
            "provider_directory_issuer_key": "aetna",
            "provider_directory_issuer_network_match_key": "aetna:c2",
        }
    ]


def test_provider_directory_network_name_matching_does_not_mutate_payload():
    address_payload = {
        "first_line": "100 Network Way",
        "city": "Example",
        "state": "IL",
        "postal_code": "62401",
        "address_sources": ["provider_directory_fhir"],
        "address_network_binding": "provider_directory_address",
        "provider_directory_plan_context_matched": False,
        "provider_directory_network_context_present": True,
        "provider_directory_network_names": ["C2"],
        "provider_directory_network_matches": [
            {
                "ref": "Organization/network-1",
                "resource_id": "network-1",
                "name": "C2",
                "aliases": ["C Two"],
            }
        ],
        "address_verification_evidence": {
            "matched_on": "npi_address_key_role_location",
        },
    }
    original_payload = json.loads(json.dumps(address_payload))
    item = {
        "network_names": ["C2"],
        "address": {
            "first_line": "100 Network Way",
            "city": "Example",
            "state": "IL",
            "postal_code": "62401",
        },
    }

    first = ptg2_serving._address_verification_payload(item, {}, address_payload)
    second = ptg2_serving._address_verification_payload(item, {}, address_payload)

    assert address_payload == original_payload
    assert second["provider_directory_network_matches"] == first["provider_directory_network_matches"]
    assert len(second["provider_directory_network_matches"]) == 1


def test_compact_item_promotes_pre_shaped_provider_directory_network_match():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "network_names": ["C2"],
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "provider_directory_address",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_context_present": True,
                "provider_directory_network_matches": [
                    {
                        "ptg_network_name": "C2",
                        "provider_directory_network_name": "C2",
                        "provider_directory_network_resource_id": "network-1",
                        "provider_directory_network_ref": "Organization/network-1",
                    }
                ],
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location",
                },
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_matches"] == [
        {
            "ptg_network_name": "C2",
            "provider_directory_network_name": "C2",
            "provider_directory_network_resource_id": "network-1",
            "provider_directory_network_ref": "Organization/network-1",
        }
    ]


def test_compact_item_downgrades_pre_shaped_network_match_without_served_network_names():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "provider_directory_address",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_context_present": True,
                "provider_directory_network_matches": [
                    {
                        "ptg_network_name": "C2",
                        "provider_directory_network_name": "C2",
                    }
                ],
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location_network_name",
                    "network_name_context_matched": True,
                },
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["requires_location_confirmation"] is True
    assert "provider_directory_network_name_matched" not in verification
    assert "provider_directory_network_matches" not in verification
    assert verification["address_verification_evidence"]["matched_on"] == "npi_address_key_role_location"


def test_compact_item_normalizes_provider_directory_boolean_strings():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "network_names": ["C2"],
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "provider_directory_address",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": "false",
                "provider_directory_network_context_present": "true",
                "provider_directory_network_refs": '["Organization/network-1"]',
                "provider_directory_network_names": ["C2"],
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-1",
                        "resource_id": "network-1",
                        "name": "C2",
                    }
                ],
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location",
                },
                "provider_directory_insurance_plan_refs": "InsurancePlan/plan-1",
                "provider_directory_insurance_plan_matches": '["InsurancePlan/plan-1"]',
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["provider_directory_plan_context_matched"] is False
    assert verification["provider_directory_network_context_present"] is True
    assert verification["provider_directory_network_name_matched"] is True
    assert verification["provider_directory_network_refs"] == ["Organization/network-1"]
    assert verification["provider_directory_network_names"] == ["C2"]
    assert verification["provider_directory_insurance_plan_refs"] == ["InsurancePlan/plan-1"]
    assert verification["provider_directory_insurance_plan_matches"] == ["InsurancePlan/plan-1"]


def test_compact_item_downgrades_provider_directory_network_marker_without_plan_context():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "payer_directory_corroborated_location",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location",
                },
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "provider_directory_address"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["provider_directory_plan_context_matched"] is False


def test_compact_item_rejects_loose_provider_directory_network_name_marker():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "network_names": ["C2"],
            "location_source": "provider_directory_fhir",
            "location_confidence_code": "payer_directory_corroborated_location",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
                "provider_directory_plan_context_matched": False,
                "provider_directory_network_name_matched": True,
                "provider_directory_network_matches": [
                    {
                        "ref": "Organization/network-2",
                        "resource_id": "network-2",
                        "name": "PPO NDC",
                    }
                ],
                "address_verification_evidence": {
                    "matched_on": "npi_address_key_role_location_network_name",
                    "network_name_context_matched": True,
                    "network_name_matches": [
                        {
                            "ptg_network_name": "C2",
                            "provider_directory_network_name": "C2",
                        }
                    ],
                },
            },
            "prices": [],
        },
        {},
    )

    verification = item["address_verification"]
    assert verification["address_evidence_level"] == "provider_directory_address"
    assert verification["address_network_binding"] == "inferred_from_provider_identity"
    assert verification["requires_location_confirmation"] is True
    assert "provider_directory_network_name_matched" not in verification
    assert "provider_directory_network_matches" not in verification
    assert verification["address_verification_evidence"]["matched_on"] == "npi_address_key_role_location"
    assert "network_name_context_matched" not in verification["address_verification_evidence"]
    assert "network_name_matches" not in verification["address_verification_evidence"]


def test_compact_item_marks_provider_directory_address_without_network_as_inferred():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "location_source": "entity_address_unified",
            "address_payload": {
                "first_line": "100 Network Way",
                "city": "Example",
                "state": "IL",
                "postal_code": "62401",
                "address_sources": ["provider_directory_fhir"],
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "provider_directory_address"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True


def test_address_verification_treats_nested_address_as_displayable():
    verification = ptg2_serving._address_verification_payload(
        {
            "network_names": ["C2"],
            "address": {
                "first_line": "100 Test St",
                "city": "Chicago",
                "state": "IL",
                "postal_code": "60601",
            },
        },
        {},
        {
            "address_network_binding": "payer_directory_corroborated_location",
            "location_source": "provider_directory_fhir",
            "address_sources": ["provider_directory_fhir"],
            "provider_directory_plan_context_matched": True,
        },
    )

    assert verification["displayed_address_present"] is True
    assert verification["address_network_binding"] == "payer_directory_corroborated_location"
    assert verification["address_evidence_level"] == "payer_directory_network_location"


def test_compact_item_marks_missing_address_as_not_displayable():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["address_network_binding"] == "inferred_from_provider_identity"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["displayed_address_present"] is False
    assert "address" not in item
    assert "phone" not in item
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "displayed_address_present=false",
    } in summary["issues"]
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "address_verification is present but no usable address fields are displayed",
    } in summary["issues"]


def test_ptg2_display_policy_handles_malformed_address_verification_without_recursing():
    item = {
        "provider_name": "Example Surgeon",
        "address_verification": "not-json",
        "address": "100 Main St",
    }

    ptg2_serving._apply_address_display_policy(item, {})

    assert item["address_verification"] == "not-json"
    assert item["address"] == "100 Main St"


def test_compact_item_marks_address_key_only_as_not_displayable():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["network_bound_address"] is False
    for key in ("location_source", "address_sources", "address_precision", "source_count"):
        assert key not in item["address_verification"]
    assert "address" not in item
    assert "address_key" not in item
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "displayed_address_present=false",
    } in summary["issues"]


def test_compact_item_marks_city_only_as_not_displayable():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "city": "Effingham",
                "address_precision": "city_zip",
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["network_bound_address"] is False
    for key in ("location_source", "address_sources", "address_precision", "source_count"):
        assert key not in item["address_verification"]
    assert "address" not in item
    assert "city" not in item
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is False
    assert {
        "severity": "error",
        "item_index": 0,
        "message": "address_verification is present but no usable address fields are displayed",
    } in summary["issues"]


def test_compact_item_strips_phone_and_distance_for_no_display_address():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "address_key": "00000000-0000-0000-0000-000000000001",
                "telephone_number": "217-555-0100",
                "phone_number": "2175550100",
                "phone_extension": "101",
                "fax_number": "217-555-0101",
                "fax_number_digits": "2175550101",
                "fax_extension": "102",
            },
            "distance_miles": 2.3,
            "zip_match_type": "radius",
            "coordinates": {"lat": 39.12004, "long": -88.54338},
            "location_source": "entity_address_unified",
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["displayed_address_present"] is False
    assert item["address_verification"]["address_evidence_level"] == "unknown"
    assert item["address_verification"]["reason"] == (
        "PTG proves the provider identity is in network, but no displayable address is available."
    )
    for key in ("location_source", "address_sources", "address_precision", "source_count"):
        assert key not in item["address_verification"]
    for key in (
        "address",
        "telephone_number",
        "phone_number",
        "phone",
        "fax_number",
        "fax_number_digits",
        "phone_extension",
        "fax_extension",
        "distance_miles",
        "zip_match_type",
        "coordinates",
        "location_source",
    ):
        assert key not in item


def test_compact_item_marks_city_state_fallback_as_displayable_but_unconfirmed():
    item = ptg2_serving._compact_item_from_row(
        {
            "npi": 1234567890,
            "provider_name": "Example Surgeon",
            "address_payload": {
                "city": "Effingham",
                "state": "IL",
                "address_precision": "city_zip",
            },
            "prices": [],
        },
        {},
    )

    assert item["address_verification"]["address_evidence_level"] == "city_zip_fallback"
    assert item["address_verification"]["requires_location_confirmation"] is True
    assert item["address_verification"]["displayed_address_present"] is True
    summary = summarize_ptg_price_address_payload({"data": {"items": [item]}})
    assert summary["ok"] is True
    assert summary["displayed_address_rows"] == 1


def test_orthopedic_surgery_specialty_resolves_to_taxonomy():
    from api.provider_specialty_filters import (
        ORTHOPAEDIC_SURGERY_TAXONOMY_CODES,
        resolve_provider_specialty_filter,
    )

    for term in ("orthopedic surgery", "Orthopaedic Surgeon", "orthopedics", "ortho"):
        resolved = resolve_provider_specialty_filter({"specialty": term})
        assert resolved.active, term
        assert resolved.taxonomy_codes == ORTHOPAEDIC_SURGERY_TAXONOMY_CODES, term
        assert "207X00000X" in resolved.taxonomy_codes


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_applies_specialty_taxonomy_filter(monkeypatch):
    # Regression: a location (geo/ZIP) provider search must still scope to the
    # requested clinical specialty. Without the taxonomy predicate, a procedure+ZIP
    # lookup returns every NPI that bills at that address (e.g. an optometry practice
    # or a hospital for an orthopedic ACL repair) instead of orthopedic surgeons.
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "lat": "34.14024131",
            "long": "-118.255125",
            "radius_miles": "10",
            "limit": "5",
            "specialty": "orthopedic surgery",
        },
        candidate_limit=5,
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    # With a taxonomy filter, the member x taxonomy intersection is resolved
    # once in a MATERIALIZED CTE (uncorrelated semi-join, sargable code
    # predicate) and probed per address row. Correlated per-member probes must
    # not reappear: on whole-network member tables they run once per row and
    # time the request out.
    assert "scoped_member_npis AS MATERIALIZED" in sql
    assert "FROM mrf.ptg2_provider_group_member_snap pgm_scope" in sql
    assert "pgm_scope.npi IN (SELECT manifest_location_specialty_nt.npi" in sql
    assert "FROM scoped_member_npis scope_npis" in sql
    assert "WHERE addr_probe.npi = scope_npis.npi" in sql
    # OFFSET 0 pins the per-NPI index probe; without it the planner flattens
    # the LATERAL and bitmap-scans the whole address table.
    assert "OFFSET 0" in sql
    assert "manifest_location_specialty_nt.npi = pgm_scope.npi" not in sql
    assert "UPPER(COALESCE(manifest_location_specialty_nt.healthcare_provider_taxonomy_code" not in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_inferred_taxonomy_requires_individual_npi(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "lat": "34.14024131",
            "long": "-118.255125",
            "radius_miles": "10",
            "limit": "5",
            "code": "29888",
        },
        candidate_limit=5,
    )

    sql = str(session.calls[2][0][0])
    params = session.calls[2][0][1]
    # Inferred-taxonomy scoping is an uncorrelated semi-join in the member CTE...
    assert "scoped_taxonomy_member_npis AS MATERIALIZED" in sql
    assert "pgm_scope.npi IN (SELECT nt.npi FROM mrf.npi_taxonomy nt" in sql
    assert "nt.npi = pgm_scope.npi" not in sql
    # ...and the individual-NPI check runs as a scalar probe per candidate in a
    # second CTE stage after the intersection (an EXISTS gets pulled up into a
    # hash of the whole npi table).
    assert "scoped_member_npis AS MATERIALIZED" in sql
    assert "FROM scoped_taxonomy_member_npis scope_filter" in sql
    assert "COALESCE((SELECT COALESCE(n_entity.entity_type_code, 0) = 1" in sql
    assert "n_entity.npi = scope_filter.npi" in sql
    assert "FROM scoped_member_npis scope_npis" in sql
    assert "WHERE addr_probe.npi = scope_npis.npi" in sql
    # OFFSET 0 pins the per-NPI index probe; without it the planner flattens
    # the LATERAL and bitmap-scans the whole address table.
    assert "OFFSET 0" in sql
    assert "207X00000X" in str(params)


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_no_taxonomy_keeps_member_exists(monkeypatch):
    # Without a taxonomy filter the member check must stay a per-address-row
    # EXISTS: materializing a whole unfiltered network (1M+ NPIs) would cost
    # more than it saves.
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            False,
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"lat": "34.14024131", "long": "-118.255125", "radius_miles": "10", "limit": "5"},
        candidate_limit=5,
    )

    sql = str(session.calls[2][0][0])
    assert "pgm_scope.npi = addr.npi" in sql
    assert "scoped_member_npis" not in sql


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_small_sidecar_scope_uses_plain_any(monkeypatch):
    group_a = "00000000-0000-0000-0000-000000000011"
    group_b = "00000000-0000-0000-0000-000000000012"
    session = FakeSession([FakeResult(rows=[])])
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(return_value=(group_a, group_b)),
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_token",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
        id_storage="uuid",
    )

    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "plan_id": "TESTPLAN001",
            "code": "99203",
            "code_system": "CPT",
            "zip5": "60601",
            "npi": 1234567890,
        },
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    sql = str(session.calls[-1][0][0])
    assert "rate_provider_groups AS MATERIALIZED" not in sql
    assert "SELECT unnest(" not in sql
    assert "JOIN rate_provider_groups rpg_scope" not in sql
    assert "JOIN rate_provider_groups rpg" not in sql
    assert "pgm_scope.provider_group_global_id_128 = ANY" in sql
    assert "pgm.provider_group_global_id_128 = ANY" in sql


@pytest.mark.asyncio
async def test_large_sidecar_filters_generic_rows(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SQL_RATE_SCOPE_MAX_IDS", "0")
    group_id = "00000000000000000000000000000011"
    out_scope_group_id = "00000000000000000000000000000099"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    _fake_manifest_provider_location_row(group_id),
                    _fake_manifest_provider_location_row(out_scope_group_id),
                ]
            ),
        ]
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(return_value=(group_id,)),
    )
    sets_probe = AsyncMock(return_value={group_id: (provider_set_id,)})
    monkeypatch.setattr(ptg2_serving, "_manifest_sets_by_group", sets_probe)
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_token",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
        id_storage="uuid",
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {"plan_id": "TESTPLAN001", "code": "99203", "code_system": "CPT", "zip5": "60601"},
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )

    location_sql = str(session.calls[-1][0][0])
    assert "location_rate_provider_group_ids" not in location_sql
    sets_probe.assert_awaited_once_with(session, tables, (group_id,))
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


@pytest.mark.asyncio
async def test_rate_scope_sidecar_cache(monkeypatch):
    ptg2_serving._PTG2_RATE_SCOPE_CACHE.clear()
    ptg2_serving._PTG2_RATE_SCOPE_CACHE_BUDGET.total_ids = 0
    group_id = "00000000000000000000000000000011"
    session = FakeSession([])
    session.sync_session = object()
    sidecar_probe = AsyncMock(return_value=(group_id,))
    monkeypatch.setattr(ptg2_serving, "_manifest_rate_provider_groups_from_sidecar", sidecar_probe)
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_token",
        source_key="ptg2:test",
        artifacts={"provider_forward": {"name": "provider_forward", "path": "/tmp/provider_forward.ptg2sc", "sha256": "abc"}},
    )

    first_scope = await ptg2_serving._manifest_rate_scope_from_sidecar(
        session,
        tables,
        serving_table=tables.serving_table,
        plan_id="TESTPLAN001",
        reported_code="99203",
        code_system="CPT",
    )
    second_scope = await ptg2_serving._manifest_rate_scope_from_sidecar(
        session,
        tables,
        serving_table=tables.serving_table,
        plan_id="TESTPLAN001",
        reported_code="99203",
        code_system="CPT",
    )

    sidecar_probe.assert_awaited_once()
    assert first_scope.group_id_bytes == frozenset({bytes.fromhex(group_id)})
    assert second_scope is first_scope


def test_rate_scope_cache_evicts_by_total_ids(monkeypatch):
    ptg2_serving._PTG2_RATE_SCOPE_CACHE.clear()
    ptg2_serving._PTG2_RATE_SCOPE_CACHE_BUDGET.total_ids = 0
    monkeypatch.setattr(ptg2_serving, "_PTG2_RATE_SCOPE_CACHE_MAX_KEYS", 10)
    monkeypatch.setattr(ptg2_serving, "_PTG2_RATE_SCOPE_CACHE_MAX_IDS", 3)
    first_key = ("first",)
    second_key = ("second",)

    first_scope = ptg2_serving._ptg2_rate_scope_set(
        first_key,
        (
            "00000000000000000000000000000011",
            "00000000000000000000000000000012",
        ),
    )
    second_scope = ptg2_serving._ptg2_rate_scope_set(
        second_key,
        (
            "00000000000000000000000000000021",
            "00000000000000000000000000000022",
        ),
    )

    assert first_scope.id_count == 2
    assert second_scope.id_count == 2
    assert first_key not in ptg2_serving._PTG2_RATE_SCOPE_CACHE
    assert ptg2_serving._PTG2_RATE_SCOPE_CACHE[second_key] is second_scope
    assert ptg2_serving._PTG2_RATE_SCOPE_CACHE_BUDGET.total_ids == 2


def test_rate_scope_cache_skips_entries_larger_than_id_budget(monkeypatch):
    ptg2_serving._PTG2_RATE_SCOPE_CACHE.clear()
    ptg2_serving._PTG2_RATE_SCOPE_CACHE_BUDGET.total_ids = 0
    monkeypatch.setattr(ptg2_serving, "_PTG2_RATE_SCOPE_CACHE_MAX_KEYS", 10)
    monkeypatch.setattr(ptg2_serving, "_PTG2_RATE_SCOPE_CACHE_MAX_IDS", 1)
    cache_key = ("oversized",)

    rate_scope = ptg2_serving._ptg2_rate_scope_set(
        cache_key,
        (
            "00000000000000000000000000000011",
            "00000000000000000000000000000012",
        ),
    )

    assert rate_scope.id_count == 2
    assert cache_key not in ptg2_serving._PTG2_RATE_SCOPE_CACHE
    assert ptg2_serving._PTG2_RATE_SCOPE_CACHE_BUDGET.total_ids == 0


def test_rate_scope_omits_large_sql_tuple(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SQL_RATE_SCOPE_MAX_IDS", "1")
    first_group_id = "00000000000000000000000000000011"
    second_group_id = "00000000000000000000000000000012"

    rate_scope = ptg2_serving._ptg2_build_rate_scope((first_group_id, second_group_id))

    assert rate_scope.group_ids == ()
    assert rate_scope.id_count == 2
    assert ptg2_serving._has_rate_scope_group(rate_scope, first_group_id)
    assert ptg2_serving._has_rate_scope_group(rate_scope, second_group_id)


async def _location_first_test_context(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setenv("HLTHPRT_PTG2_MANIFEST_SQL_RATE_SCOPE_MAX_IDS", "0")
    group_id = "00000000-0000-0000-0000-000000000011"
    other_group_id = "00000000-0000-0000-0000-000000000099"
    provider_set_id = "00000000000000000000000000000012"
    in_scope_location = _fake_manifest_provider_location_row(group_id)
    out_scope_location = _fake_manifest_provider_location_row(other_group_id)
    session = FakeSession(
        [
            FakeResult(rows=[{"npi": 1234567890}]),
            FakeResult(rows=[in_scope_location, out_scope_location]),
        ]
    )
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.entity_address_unified"))
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg2_serving, "_taxonomy_int_codes_for_codes", AsyncMock(return_value=(101, 202)))
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(return_value=(group_id,)),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_sets_by_group",
        AsyncMock(return_value={group_id.replace("-", ""): (provider_set_id,)}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_overlay_provider_directory_corroboration",
        AsyncMock(side_effect=lambda _session, rows, **_kwargs: rows),
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_token",
        provider_group_member_table="mrf.ptg2_provider_group_member_token",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
        id_storage="uuid",
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "plan_id": "TESTPLAN001",
            "code": "99203",
            "code_system": "CPT",
            "zip5": "60601",
            "specialty": "orthopedic surgery",
        },
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )
    return session, provider_set_id, provider_set_ids, providers_by_set


@pytest.mark.asyncio
async def test_manifest_location_sidecar_scope_uses_location_first_python_intersection(monkeypatch):
    session, provider_set_id, provider_set_ids, providers_by_set = await _location_first_test_context(monkeypatch)
    probe_sql = _fake_call_sql(session, "located_probe AS MATERIALIZED")
    location_sql = _fake_call_sql(session, "located AS MATERIALIZED")
    location_param_map = next(
        call[0][1]
        for call in session.calls
        if "located AS MATERIALIZED" in str(call[0][0])
    )
    assert "addr.taxonomy_array && CAST(:location_first_taxonomy_int_codes AS integer[])" in probe_sql
    assert "JOIN LATERAL" in location_sql
    assert "FROM mrf.ptg2_provider_group_member_token pgm_inner" in location_sql
    assert "pgm_inner.npi = addr.npi" in location_sql
    assert "addr.npi = ANY(CAST(:location_first_probe_npis AS bigint[]))" in location_sql
    assert "scoped_member_npis AS MATERIALIZED" not in location_sql
    assert "rate_provider_groups AS MATERIALIZED" not in location_sql
    assert "location_rate_provider_group_ids" not in location_sql
    assert "location_rate_provider_group_ids" not in location_param_map
    assert location_param_map["location_first_taxonomy_int_codes"] == [101, 202]
    assert location_param_map["location_first_probe_npis"] == [1234567890]
    assert provider_set_ids == {provider_set_id}
    assert providers_by_set[provider_set_id][0]["npi"] == 1234567890


async def _run_plan_scoped_taxonomy_location_search(monkeypatch):
    """Run the group-plan TESTPLAN001 / CPT 90837 reproducer; return (sql, params).

    Mirrors the plan-scoped provider-pricing expansion that 502'd upstream:
    behavioral taxonomy_codes, primary_only=false, zip5 filter, populated
    component table.
    """
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    monkeypatch.setattr(ptg2_serving, "_PTG2_TAXONOMY_INT_CODE_CACHE", {})
    group_id = "00000000000000000000000000000011"
    provider_set_id = "00000000000000000000000000000012"
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[{"int_code": 101}, {"int_code": 202}]),
            FakeResult(scalar=True),
            FakeResult(rows=[]),
        ]
    )
    tables = ptg2_serving.PTG2ServingTables(
        serving_table="mrf.ptg2_serving_snap",
        provider_set_component_table="mrf.ptg2_provider_set_component_snap",
        provider_group_member_table="mrf.ptg2_provider_group_member_snap",
        artifacts={"provider_inverted": {"name": "provider_inverted", "path": "/tmp/provider_inverted.ptg2sc"}},
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_ptg2_manifest_sidecar_members_many_async",
        _async_sidecar_members_many({group_id: (provider_set_id,)}),
    )
    await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        tables,
        {
            "zip5": "27616",
            "limit": "5",
            "code": "90837",
            "code_system": "CPT",
            "taxonomy_codes": "101YM0800X,101YP2500X,103T00000X,103TC0700X,1041C0700X,106H00000X,2084P0800X,363LP0808X,364SP0808X",
            "primary_only": "false",
        },
        candidate_limit=5,
        plan_id="TESTPLAN001",
    )
    return str(session.calls[-1][0][0]), session.calls[-1][0][1]


@pytest.mark.asyncio
async def test_plan_scoped_taxonomy_location_search_uses_semijoin_ctes(monkeypatch):
    """Member CTE must use uncorrelated, sargable semi-joins, never per-row probes.

    Regression for plan-scoped provider-pricing 502s: correlated taxonomy
    probes ran once per member row of a whole-network member table (~32M page
    reads, 24-45s cold; the empty-first-pass secondary-address fallback
    doubled it).
    """
    sql, params_by_name = await _run_plan_scoped_taxonomy_location_search(monkeypatch)
    assert "pgm_scope.npi IN (SELECT manifest_location_specialty_nt.npi" in sql
    assert "pgm_scope.npi IN (SELECT nt.npi FROM mrf.npi_taxonomy nt" in sql
    assert "= pgm_scope.npi" not in sql
    # Sargable code predicate (bare column, uppercased parameters).
    assert "UPPER(COALESCE(manifest_location_specialty_nt.healthcare_provider_taxonomy_code" not in sql
    # primary_only=false must not add the primary-switch clause to the semi-join.
    assert "manifest_location_specialty_nt.healthcare_provider_primary_taxonomy_switch" not in sql
    assert params_by_name["manifest_location_specialty_taxonomy_code_0"] == "101YM0800X"
    assert params_by_name["manifest_location_specialty_taxonomy_code_8"] == "364SP0808X"


@pytest.mark.asyncio
async def test_plan_scoped_taxonomy_location_search_stages_entity_and_address_probes(monkeypatch):
    """Entity check and address lookup run per scoped candidate, after the intersection.

    The individual-NPI check is a scalar probe in a second CTE stage (an
    EXISTS gets pulled up and hash-joins the whole npi table), the address
    lookup drives from the scoped CTE via LATERAL + OFFSET 0 (without the
    fence the planner flattens it and bitmap-scans the whole address table),
    and component-table rate scoping still narrows the member CTE.
    """
    sql, params_by_name = await _run_plan_scoped_taxonomy_location_search(monkeypatch)
    assert "rate_provider_groups AS MATERIALIZED" in sql
    assert "rate_scope.plan_id = :location_plan_id" in sql
    assert params_by_name["location_plan_id"] == "TESTPLAN001"
    assert params_by_name["location_reported_code"] == "90837"
    assert "scoped_taxonomy_member_npis AS MATERIALIZED" in sql
    assert "FROM scoped_taxonomy_member_npis scope_filter" in sql
    assert "COALESCE((SELECT COALESCE(n_entity.entity_type_code, 0) = 1" in sql
    assert "FROM scoped_member_npis scope_npis" in sql
    assert "WHERE addr_probe.npi = scope_npis.npi" in sql
    assert "OFFSET 0" in sql


@pytest.mark.asyncio
async def test_compact_serving_include_providers_expands_without_geo_filter(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(
        [
            "mrf.npi",
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "loc-1",
                        "state": "IL",
                        "city": "Peoria",
                        "zip5": "61636",
                        "location_source": "nppes",
                        "location_confidence_code": "nppes_practice_location",
                        "address_payload": {
                            "address_key": "00000000-0000-0000-0000-000000000002",
                            "line1": "1 Main St",
                        },
                        "taxonomy_codes": [],
                        "specialties": [],
                        "provider_name": "Example Provider",
                        "procedure_code": None,
                        "reported_code_system": "RC",
                        "reported_code": "450",
                        "billing_code": "450",
                        "billing_code_type": "RC",
                        "procedure_display_name": "Emergency Room",
                        "procedure_name": "Emergency Room",
                        "procedure_description": "Emergency Room",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "percentage", "negotiated_rate": 60}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "450",
            "include_providers": "true",
            "include_unverified_addresses": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["query"]["result_granularity"] == "provider"
    assert payload["query"]["include_providers"] is True
    item = payload["items"][0]
    assert item["npi"] == 1234567890
    assert item["provider_name"] == "Example Provider"
    assert item["state"] == "IL"
    assert item["address_key"] == "00000000-0000-0000-0000-000000000002"
    assert item["address"]["address_key"] == "00000000-0000-0000-0000-000000000002"
    assert item["tic_prices"][0]["negotiated_rate"] == 60
    sql = str(session.calls[-2][0][0])
    assert "LEFT JOIN LATERAL (" in sql
    assert "FROM mrf.npi_address addr" in sql
    assert "addr.npi = pgm.npi" in sql
    assert "LEFT(COALESCE(addr.postal_code, ''), 5) = :zip5" not in sql


@pytest.mark.asyncio
async def test_compact_serving_geo_filter_uses_unified_address_table_when_compatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551", "lat": "29.7604", "long": "-95.3698", "radius_miles": "10"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[1][0][0])
    assert "JOIN mrf.entity_address_unified addr_filter" in sql
    assert "JOIN mrf.npi_address addr_filter" not in sql
    assert "addr_filter.address_precision" in sql
    assert "COALESCE(addr_filter.address_precision, '') <> 'city_zip'" in sql
    assert "addr_filter.type IN ('primary', 'secondary', 'practice', 'site')" in sql
    assert "ST_DWithin(" in sql
    assert (
        "Geography(ST_MakePoint((addr_filter.long)::double precision, "
        "(addr_filter.lat)::double precision))"
    ) in sql
    assert "addr_filter.lat::float8 BETWEEN :geo_min_lat AND :geo_max_lat" not in sql


@pytest.mark.asyncio
async def test_compact_serving_zip_geo_filter_uses_unified_zip_index_expression(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_UNIFIED_ADDRESS_COLUMNS)]),
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "70551",
            "zip5": "60601",
            "lat": "41.8820",
            "long": "-87.6278",
            "radius_miles": "10",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[1][0][0])
    assert "JOIN mrf.entity_address_unified addr_filter" in sql
    assert "COALESCE(addr_filter.zip5" in sql
    assert "LEFT(COALESCE(addr_filter.postal_code, ''), 5) = :zip5" not in sql
    assert "addr_filter.type IN ('primary', 'secondary', 'practice', 'site')" in sql
    assert sql.index("addr_filter.type IN ('primary', 'secondary', 'practice', 'site')") < sql.index(
        "(COALESCE(addr_filter.zip5"
    )


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_uses_unified_address_table_when_compatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "FROM mrf.entity_address_unified addr" in sql
    assert "FROM mrf.npi_address addr" not in sql
    assert "'entity_address_unified' AS location_source" in sql
    assert "'entity_address_unified' AS location_confidence_code" in sql
    assert "to_jsonb(addr.*)" not in sql
    assert "jsonb_build_object('npi', pgm.npi" in sql
    assert "'address_site_key', addr.premise_key::text" in sql
    assert "AS address_payload" in sql
    assert "addr.npi = pgm.npi" in sql
    assert "addr.npi = sp.npi" not in sql
    # Provider name is resolved from the canonical NPI table (mrf.npi), never
    # left as the NULL/"TiC provider" placeholder, and street-bearing address
    # rows are preferred so city/zip-only unified rows don't hide the street.
    assert "LEFT JOIN mrf.npi n ON n.npi = pgm.npi" in sql
    assert "NULL::varchar AS provider_name" not in sql
    assert "n.provider_organization_name" in sql
    assert "NULLIF(BTRIM(addr.first_line), '') IS NULL" in sql


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_uses_placeholder_without_npi_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[(column,) for column in sorted(ptg2_serving._PTG2_LEGACY_ADDRESS_COLUMNS)]),
            FakeResult(scalar=None),
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "LEFT JOIN mrf.npi n ON n.npi = pgm.npi" not in sql
    assert "'TiC provider' AS provider_name" in sql


@pytest.mark.asyncio
async def test_compact_serving_provider_expansion_falls_back_when_unified_incompatible(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "entity_address_unified")
    session = FakeSession(
        [
            FakeResult(rows=[("npi",), ("type",)]),
            "mrf.npi",
            FakeResult(rows=[]),
        ]
    )

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "FROM mrf.npi_address addr" in sql
    assert "FROM mrf.entity_address_unified addr" not in sql


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_provider_expansion_uses_direct_component_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession(["mrf.npi", FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "450", "include_providers": "true"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    assert "JOIN mrf.ptg2_provider_set_component_token psc" in sql
    assert "ON psc.provider_set_hash = r.provider_set_hash" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm" in sql
    assert "ON pgm.provider_group_hash = psc.provider_group_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "provider_group_hashes" not in sql


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_uses_primary_taxonomy_codes_without_geo():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(),
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551", "specialty": "Family Medicine"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "FROM mrf.ptg2_provider_set_component_token psc_filter" in sql
    assert "pgm_filter.provider_group_hash = psc_filter.provider_group_hash" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "LOWER(COALESCE" not in sql
    assert "nucc_filter.display_name" not in sql
    assert "mrf.npi_taxonomy provider_specialty_nt" in sql
    # Uncorrelated semi-joins, never per-member-row probes: correlated EXISTS
    # re-descends npi_taxonomy for every member of whole-network provider sets.
    # CPT 70551 also triggers the radiology inferred-taxonomy rule, so the
    # inferred clause is pinned here too.
    assert "pgm_filter.npi IN (SELECT provider_specialty_nt.npi" in sql
    assert "provider_specialty_nt.npi = pgm_filter.npi" not in sql
    assert "pgm_filter.npi IN (SELECT nt.npi FROM mrf.npi_taxonomy nt" in sql
    assert "nt.npi = pgm_filter.npi" not in sql
    assert "healthcare_provider_primary_taxonomy_switch" in sql
    assert params["provider_specialty_taxonomy_code_0"] == "207Q00000X"
    assert params["provider_specialty_taxonomy_code_1"] == "208D00000X"


def test_compact_location_filter_scopes_taxonomy_via_semijoin():
    """filtered_locations taxonomy predicates must be uncorrelated semi-joins.

    The location scan covers every row in the zip/geo set BEFORE the LIMIT; a
    correlated EXISTS re-descends npi_taxonomy once per row — the pathology
    that 502'd the manifest location matcher for sparse taxonomy codes.
    """
    params_by_name = {"limit": 25}
    sql, has_filter = ptg2_serving._compact_provider_filter_sql(
        _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token"),
        {
            "zip5": "60601",
            "code": "90837",
            "code_system": "CPT",
            "taxonomy_codes": "101YM0800X,103T00000X",
            "primary_only": "false",
        },
        params_by_name,
    )
    assert has_filter
    assert "filtered_locations AS MATERIALIZED" in sql
    assert "loc.npi IN (SELECT provider_specialty_loc_nt.npi" in sql
    assert "provider_specialty_loc_nt.npi = loc.npi" not in sql
    # The inferred CPT->behavioral taxonomy predicate is a semi-join too.
    assert "loc.npi IN (SELECT nt.npi FROM mrf.npi_taxonomy nt" in sql
    assert "nt.npi = loc.npi" not in sql
    assert params_by_name["provider_specialty_loc_taxonomy_code_0"] == "101YM0800X"
    assert params_by_name["provider_specialty_loc_taxonomy_code_1"] == "103T00000X"


@pytest.mark.asyncio
async def test_compact_serving_specialty_search_filters_minimal_provider_group_layout():
    session = FakeSession([FakeResult(rows=[])])

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        _compact_tables(provider_set_component_table=None, provider_group_location_table=None),
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "99214",
            "specialty": "primary care",
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[-1][0][0])
    params = session.calls[-1][0][1]
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "ptg2_provider_set_component_token" not in sql
    assert "FROM mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "pgm_filter.provider_group_hash = r.provider_set_hash" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm" in sql
    assert "ON pgm.provider_group_hash = r.provider_set_hash" in sql
    assert "pgm_filter.npi IN (SELECT provider_specialty_nt.npi" in sql
    assert "pgm.npi IN (SELECT provider_expansion_specialty_nt.npi" in sql
    assert "provider_specialty_nt.npi = pgm_filter.npi" not in sql
    assert "provider_expansion_specialty_nt.npi = pgm.npi" not in sql
    assert "LEFT JOIN LATERAL" in sql
    assert "tax.taxonomy_codes" in sql
    assert "363A00000X" not in params.values()
    assert set(
        value for key, value in params.items()
        if key.startswith("provider_specialty_taxonomy_code_")
    ) >= {"207Q00000X", "207R00000X", "208000000X", "208D00000X"}


@pytest.mark.asyncio
async def test_compact_serving_source_scoped_geo_taxonomy_filter_uses_direct_component_table(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ADDRESS_SERVING_SOURCE", "legacy")
    session = FakeSession([FakeResult(rows=[])])
    tables = _compact_tables()

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {"plan_id": "TESTPLAN001", "code": "70551", "zip5": "60601", "specialty": "dentist"},
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload is None
    sql = str(session.calls[0][0][0])
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert "FROM mrf.ptg2_provider_set_component_token psc_filter" in sql
    assert "JOIN mrf.ptg2_provider_group_member_token pgm_filter" in sql
    assert "psc_filter.provider_set_hash = r.provider_set_hash" in sql
    assert "ptg2_provider_entry_component_token" not in sql
    assert "JOIN mrf.npi_address addr_filter" in sql
    assert "mrf.npi_taxonomy provider_specialty_nt" in sql
    assert "provider_group_hashes" not in sql


@pytest.mark.asyncio
async def test_compact_serving_include_providers_with_geo_uses_npi_scoped_location_lookup():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "TX",
                        "city": "HOUSTON",
                        "zip5": "77030",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {
                            "address_key": "00000000-0000-0000-0000-000000000003",
                            "city": "HOUSTON",
                            "state": "TX",
                            "postal_code": "77030",
                        },
                        "taxonomy_codes": ["207Q00000X"],
                        "specialties": ["Family Medicine Physician"],
                        "classifications": ["Family Medicine"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Family Medicine Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "Example Provider",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "99213",
                        "billing_code": "99213",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "Office visit",
                        "procedure_name": "Office visit",
                        "procedure_description": "Office visit",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "derived", "negotiated_rate": 86.48}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
            {
                "plan_id": "TESTPLAN001",
                "code": "99213",
                "city": "Houston",
                "state": "TX",
                "specialty": "family",
                "include_providers": "true",
                "include_unverified_addresses": "true",
            },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["query"]["result_granularity"] == "provider"
    assert payload["items"][0]["location_source"] == "npi_address"
    assert payload["items"][0]["address_key"] == "00000000-0000-0000-0000-000000000003"
    assert payload["items"][0]["address"]["address_key"] == "00000000-0000-0000-0000-000000000003"
    assert payload["items"][0]["specialties"] == ["Family Medicine Physician"]
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    assert payload["items"][0]["specializations"] == ["Sports Medicine"]
    assert payload["items"][0]["classifications"] == ["Family Medicine"]
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "WITH rate_candidates AS MATERIALIZED" in sql
    assert "filtered_locations AS MATERIALIZED" in sql
    assert "JOIN LATERAL (" in sql
    assert "FROM mrf.ptg2_provider_group_location_token loc" in sql
    assert "JOIN filtered_locations loc" in sql
    assert "loc.npi" in sql
    assert "loc.long::float8" not in sql
    assert "loc.long" in sql
    assert "loc.city_name" in sql
    assert "loc.state_name" in sql
    assert "AND EXISTS (" in sql
    assert "OFFSET 0" in sql
    assert "COALESCE(tax.specializations, loc.specializations" not in sql
    assert "COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations" in sql
    assert "array_remove(array_agg(NULLIF(nucc.specialization, '')" in sql
    assert "'entity_address_unified'::varchar AS location_source" in sql
    assert "'address_key', loc.address_key::text" in sql
    assert "FROM mrf.npi_address addr" not in sql
    assert "JOIN mrf.npi_address addr_filter" not in sql
    assert params["city_exact"] == "HOUSTON"
    assert params["provider_match_limit"] >= 64
    assert params["location_rate_candidate_limit"] >= 4096


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_empty_sidecar_scope_skips_legacy_scan(monkeypatch):
    sidecar_probe = AsyncMock(return_value=())
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(ptg2_serving, "_manifest_rate_provider_groups_from_sidecar", sidecar_probe)

    session = FakeSession([])
    match_result = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        ptg2_types.PTG2ServingTables(
            serving_table="mrf.ptg2_serving_token",
            provider_group_member_table="mrf.ptg2_provider_group_member_token",
            provider_group_location_table="mrf.ptg2_provider_group_location_token",
            artifacts={
                "provider_forward": {"path": "provider-forward.bin"},
                "provider_inverted": {"path": "provider-inverted.bin"},
            },
            id_storage="uuid",
        ),
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "zip5": "60601",
            "lat": 41.88526,
            "long": -87.62194,
            "radius_miles": 75,
        },
        plan_id="TESTPLAN001",
    )

    assert match_result == (set(), {})
    sidecar_probe.assert_awaited_once()
    executed_sql = "\n".join(str(call[0][0]) for call in session.calls)
    assert "raw_location_npis AS" not in executed_sql
    assert "ptg2_provider_group_location_token" not in executed_sql
    assert "ptg2_provider_group_member_token" not in executed_sql


def _fake_manifest_provider_location_row(group_id: str) -> dict[str, object]:
    return {
        "provider_group_global_id_128": group_id,
        "npi": 1234567890,
        "address_key": None,
        "premise_key": None,
        "location_hash": "npi_address:1234567890:primary:addr-1",
        "state": "IL",
        "city": "CHICAGO",
        "zip5": "60601",
        "distance_miles": 0.0,
        "zip_match_type": "same_zip",
        "anchor_zip5": "60601",
        "zip_radius_miles": 75.0,
        "location_source": "npi_address",
        "location_confidence_code": "npi_address",
        "address_payload": {"first_line": "100 Main", "city": "CHICAGO"},
        "telephone_number": "312-555-0100",
        "fax_number": None,
        "phone_number": "3125550100",
        "phone_extension": None,
        "fax_number_digits": None,
        "fax_extension": None,
        "taxonomy_codes": ["103T00000X"],
        "specialties": ["Psychologist"],
        "classifications": ["Psychologist"],
        "specializations": [],
        "primary_specialty": "Psychologist",
        "primary_specialization": None,
        "provider_name": "Example Provider",
    }


@pytest.mark.asyncio
async def test_manifest_location_provider_matches_prefers_provider_group_location_table(monkeypatch):
    group_id = "00000000-0000-0000-0000-000000000001"
    session = FakeSession([
        FakeResult(rows=[]),
        FakeResult(rows=[_fake_manifest_provider_location_row(group_id)]),
    ])
    monkeypatch.setattr(ptg2_serving, "_ptg2_address_serving_table", AsyncMock(return_value="mrf.npi_address"))
    monkeypatch.setattr(ptg2_serving, "_serving_table_available", AsyncMock(return_value=True))
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_rate_provider_groups_from_sidecar",
        AsyncMock(return_value=(group_id,)),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_manifest_sets_by_group",
        AsyncMock(return_value={group_id.replace("-", ""): ("provider-set-1",)}),
    )
    monkeypatch.setattr(
        ptg2_serving,
        "_overlay_provider_directory_corroboration",
        AsyncMock(side_effect=lambda _session, rows, **_kwargs: rows),
    )

    provider_set_ids, providers_by_set = await ptg2_serving._ptg2_manifest_location_provider_matches(
        session,
        ptg2_types.PTG2ServingTables(
            serving_table="mrf.ptg2_serving_token",
            provider_group_member_table="mrf.ptg2_provider_group_member_token",
            provider_group_location_table="mrf.ptg2_provider_group_location_token",
            artifacts={"provider_inverted": {"path": "provider-inverted.bin"}},
            id_storage="uuid",
        ),
        {
            "plan_id": "TESTPLAN001",
            "code": "90837",
            "code_system": "CPT",
            "zip5": "60601",
            "lat": 41.88526,
            "long": -87.62194,
            "radius_miles": 75,
        },
        plan_id="TESTPLAN001",
    )

    assert provider_set_ids == {"provider-set-1"}
    assert providers_by_set["provider-set-1"][0]["npi"] == 1234567890
    sql = next(
        str(call[0][0])
        for call in session.calls
        if "FROM mrf.ptg2_provider_group_location_token loc" in str(call[0][0])
    )
    assert "FROM mrf.ptg2_provider_group_location_token loc" in sql
    assert "loc.taxonomy_array && (" in sql
    assert "FROM mrf.ptg2_provider_group_member_token pgm_scope" not in sql


@pytest.mark.asyncio
async def test_compact_serving_geo_provider_filter_paginates_after_provider_match():
    class LimitOnePagination:
        limit = 1
        offset = 0

    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "IL",
                        "city": "EFFINGHAM",
                        "zip5": "62401",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {"address_key": "addr-1"},
                        "taxonomy_codes": ["207X00000X"],
                        "specialties": ["Orthopaedic Surgery Physician"],
                        "classifications": ["Orthopaedic Surgery"],
                        "specializations": ["Sports Medicine"],
                        "primary_specialty": "Orthopaedic Surgery Physician",
                        "primary_specialization": "Sports Medicine",
                        "provider_name": "ACL Surgeon",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "29888",
                        "billing_code": "29888",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "ACL reconstruction",
                        "procedure_name": "ACL reconstruction",
                        "procedure_description": "ACL reconstruction",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "negotiated", "negotiated_rate": 904.61}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "29888",
            "zip5": "62401",
            "lat": "39.11952",
            "long": "-88.56418",
            "radius_miles": "10",
            "include_providers": "true",
        },
        LimitOnePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 1, "offset": 0},
        ptg2_serving.PTG2_MODE_PRODUCT_SEARCH,
    )

    assert payload["items"][0]["provider_name"] == "ACL Surgeon"
    assert payload["items"][0]["specialization"] == "Sports Medicine"
    sql = str(session.calls[0][0][0])
    params = session.calls[0][0][1]
    assert "WITH rate_candidates AS MATERIALIZED" in sql
    assert "LIMIT :rate_candidate_limit" in sql
    assert "loc.long::float8" in sql
    assert "loc.lon::float8" not in sql
    assert "LIMIT :rate_candidate_limit OFFSET" not in sql
    assert "provider_filtered_rates AS MATERIALIZED" in sql
    assert not sql.rstrip().endswith("LIMIT :limit OFFSET :offset")
    assert payload["pagination"]["limit"] == 1
    assert payload["pagination"]["total"] == 1
    assert "n_entity.entity_type_code" in sql
    # Inferred-taxonomy scoping in the provider expansion is an uncorrelated
    # semi-join. The negative assertion is scoped to the EXISTS shape because
    # the taxonomy-summary LATERAL legitimately correlates nt.npi = pgm.npi.
    assert "pgm.npi IN (SELECT nt.npi FROM mrf.npi_taxonomy nt" in sql
    assert "npi_taxonomy nt WHERE nt.npi = pgm.npi" not in sql
    assert params["limit"] == 1
    assert params["rate_candidate_limit"] > params["limit"]


@pytest.mark.asyncio
async def test_compact_serving_infers_radiology_taxonomy_for_radiology_cpt_geo_lookup():
    session = FakeSession(
        [
            FakeResult(
                rows=[
                    {
                        "npi": 1234567890,
                        "location_hash": "npi_address:1234567890:primary:addr-1",
                        "state": "MA",
                        "city": "BOSTON",
                        "zip5": "02118",
                        "location_source": "npi_address",
                        "location_confidence_code": "npi_address",
                        "address_payload": {"city": "BOSTON", "state": "MA", "postal_code": "02118"},
                        "taxonomy_codes": ["2085R0202X"],
                        "specialties": ["Diagnostic Radiology Physician"],
                        "provider_name": "Radiology Provider",
                        "procedure_code": None,
                        "reported_code_system": "CPT",
                        "reported_code": "70551",
                        "billing_code": "70551",
                        "billing_code_type": "CPT",
                        "procedure_display_name": "MRI brain",
                        "procedure_name": "MRI brain",
                        "procedure_description": "MRI brain",
                        "provider_set_hashes": ["provider-set-1"],
                        "rate_count": 1,
                        "prices": [{"negotiated_type": "derived", "negotiated_rate": 86.48}],
                        "source_trace": [],
                    }
                ]
            )
        ]
    )
    tables = _compact_tables(provider_group_location_table="mrf.ptg2_provider_group_location_token")

    payload = await ptg2_serving._search_compact_serving_table(
        session,
        "mrf.ptg2_serving_rate_compact_token",
        tables,
        "snap-token",
        {
            "plan_id": "TESTPLAN001",
            "code": "70551",
            "code_system": "CPT",
            "city": "Boston",
            "state": "MA",
            "include_providers": "true",
        },
        FakePagination(),
        ["snapshot_id = :snapshot_id", "plan_id = :plan_id"],
        {"snapshot_id": "snap-token", "plan_id": "TESTPLAN001", "limit": 25, "offset": 0},
        ptg2_serving.PTG2_MODE_EXACT_SOURCE,
    )

    assert payload["items"][0]["specialties"] == ["Diagnostic Radiology Physician"]
    sql = str(session.calls[0][0][0])
    assert "nt.healthcare_provider_taxonomy_code IN" in sql
    assert "inferred_taxonomy_code_" in sql
    params = session.calls[0][0][1]
    assert "2085R0202X" in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }
    assert "2084D0003X" in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }
    assert "JOIN mrf.nucc_taxonomy nucc\n                            ON nucc.code" not in sql
    assert "2085R0001X" not in {
        value for key, value in params.items() if key.startswith("inferred_taxonomy_code_")
    }


@pytest.mark.parametrize(
    ("code", "expected_code", "expected_term"),
    [
        ("00100", "207L00000X", "anesthesiology"),
        ("80053", "291U00000X", "clinical medical laboratory"),
        ("97140", "225100000X", "physical therapist"),
        ("66984", "207W00000X", "ophthalmology"),
        ("45378", "207RG0100X", "gastroenterology"),
        ("99285", "207P00000X", "emergency medicine"),
        ("77301", "2085R0001X", "radiation oncology"),
    ],
)
def test_inferred_provider_taxonomy_sql_for_high_confidence_cpt_families(
    code: str,
    expected_code: str,
    expected_term: str,
):
    sql = ptg2_serving._inferred_provider_taxonomy_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        nucc_alias="nucc",
    )

    assert expected_code in sql
    assert f"%{expected_term}%" in sql


@pytest.mark.parametrize("code", ["99213", "99203", "93000"])
def test_inferred_provider_taxonomy_sql_ignores_mixed_use_cpt_families(code: str):
    sql = ptg2_serving._inferred_provider_taxonomy_sql(
        {"code": code, "code_system": "CPT"},
        nt_alias="nt",
        nucc_alias="nucc",
    )

    assert sql == ""


def test_warm_cache_benchmark_fixture_p95_gate():
    index = ptg2_serving.PTG2ServingIndex.from_payload(_fixture_payload())

    result = ptg2_serving.warm_cache_benchmark(index, request_count=100)

    assert result["request_count"] == 100
    assert result["p95_ms"] <= 50.0
    assert result["passed"] is True
