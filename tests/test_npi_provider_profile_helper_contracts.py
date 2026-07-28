# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import InvalidUsage

from api.endpoint import npi as npi_module


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


def test_profile_source_arrays_and_primary_taxonomy_are_normalized():
    payload = [{"taxonomy_code": "207Q00000X"}]

    assert npi_module._json_array_value(None) == []
    assert npi_module._json_array_value(payload) is payload
    assert npi_module._json_array_value(tuple(payload)) == payload
    assert npi_module._json_array_value('[{"taxonomy_code":"207Q00000X"}]') == payload
    assert npi_module._json_array_value("{not-json") == []
    assert npi_module._json_array_value('{"taxonomy_code":"207Q00000X"}') == []
    assert npi_module._json_array_value(7) == []

    secondary_taxonomy_map = {"taxonomy_code": "207Q00000X", "primary": False}
    primary_taxonomy_map = {"taxonomy_code": "208600000X", "primary": True}
    assert npi_module._primary_taxonomy(
        ["ignored", secondary_taxonomy_map, primary_taxonomy_map]
    ) == primary_taxonomy_map
    assert npi_module._primary_taxonomy(
        ["ignored", secondary_taxonomy_map]
    ) == secondary_taxonomy_map
    assert npi_module._primary_taxonomy(["ignored"]) == {}


def test_profile_filter_tokens_are_validated_and_deduplicated():
    assert npi_module._parse_code_tokens(None, "codes") == []
    assert npi_module._parse_code_tokens(" 123,ABC-7,123, ", "codes") == [
        "123",
        "ABC-7",
    ]
    with pytest.raises(InvalidUsage, match="invalid code token"):
        npi_module._parse_code_tokens("valid,bad code", "codes")

    assert npi_module._taxonomy_scope_tokens(None) == ((), ())
    assert npi_module._taxonomy_scope_tokens(
        "207Q00000X, 261Q*,;207Q00000X;261Q*"
    ) == (("207Q00000X",), ("261Q",))
    with pytest.raises(InvalidUsage, match="NUCC codes or prefixes"):
        npi_module._taxonomy_scope_tokens("bad scope!")


def test_provider_type_matching_uses_exact_prefix_and_classification_evidence():
    specialty_filter = SimpleNamespace(
        taxonomy_codes=("208600000X",),
        classification="Surgery",
    )
    provider_row_map = {
        "taxonomy_list": [
            "ignored",
            {"taxonomy_code": "207Q00000X", "classification": "Family Medicine"},
            {"taxonomy_code": "2086A0000X", "classification": "Surgery"},
        ]
    }

    assert not npi_module._is_provider_type_filter_matched(provider_row_map, {})
    assert npi_module._is_provider_type_filter_matched(
        provider_row_map, {"taxonomy_exact": ("207Q00000X",)}
    )
    assert npi_module._is_provider_type_filter_matched(
        provider_row_map, {"taxonomy_prefixes": ("2086",)}
    )
    assert npi_module._is_provider_type_filter_matched(
        provider_row_map,
        {"provider_type": "surgeon", "specialty_filter": specialty_filter},
    )
    assert not npi_module._is_provider_type_filter_matched(
        provider_row_map,
        {
            "provider_type": "unmatched",
            "taxonomy_exact": ("999999999X",),
            "taxonomy_prefixes": ("333",),
        },
    )

    assert not npi_module._is_provider_type_taxonomy_matched(provider_row_map, {})
    assert npi_module._is_provider_type_taxonomy_matched(
        provider_row_map, {"specialty_filter": specialty_filter}
    )
    exact_specialty = SimpleNamespace(
        taxonomy_codes=("207Q00000X",),
        classification=None,
    )
    assert npi_module._is_provider_type_taxonomy_matched(
        provider_row_map, {"specialty_filter": exact_specialty}
    )
    unmatched_specialty = SimpleNamespace(
        taxonomy_codes=("999999999X",),
        classification="Unmatched",
    )
    assert not npi_module._is_provider_type_taxonomy_matched(
        provider_row_map, {"specialty_filter": unmatched_specialty}
    )


def test_ffs_enrollment_partition_keeps_chain_rows_out_of_individual_results():
    visible_enrollment_map = {
        "enrollment_id": "visible",
        "multiple_npi_flag": "N",
    }
    multiple_npi_chain_map = {
        "enrollment_id": "multi",
        "multiple_npi_flag": " y ",
    }
    provider_type_chain_map = {
        "enrollment_id": "chain-type",
        "multiple_npi_flag": None,
        "provider_type_code": next(iter(npi_module.CHAIN_PECOS_PROVIDER_TYPE_CODES)),
    }

    visible_rows, chain_rows = npi_module._partition_ffs_enrollment_payloads(
        [
            visible_enrollment_map,
            multiple_npi_chain_map,
            provider_type_chain_map,
        ]
    )

    assert visible_rows == [visible_enrollment_map]
    assert chain_rows == [multiple_npi_chain_map, provider_type_chain_map]


@pytest.mark.asyncio
async def test_filter_year_records_request_environment_and_data_provenance(monkeypatch):
    assert await npi_module._resolve_filter_year(2026, True, True) == (
        2026,
        "request",
    )

    monkeypatch.setenv("HLTHPRT_NPI_FILTER_DEFAULT_YEAR", "2025")
    assert await npi_module._resolve_filter_year(None, True, True) == (2025, "env")

    monkeypatch.delenv("HLTHPRT_NPI_FILTER_DEFAULT_YEAR")
    assert await npi_module._resolve_filter_year(None, False, False) == (
        None,
        "none",
    )

    table_available = AsyncMock(return_value=True)
    execute_stmt = AsyncMock(return_value=_Rows([(2024,)]))
    monkeypatch.setattr(npi_module, "_is_table_available", table_available)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    assert await npi_module._resolve_filter_year(None, True, True) == (2024, "data")
    assert table_available.await_count == 2
    assert "UNION ALL" in str(execute_stmt.await_args.args[0])

    execute_stmt.return_value = _Rows([])
    assert await npi_module._resolve_filter_year(None, True, False) == (
        None,
        "none",
    )


@pytest.mark.asyncio
async def test_internal_filter_codes_report_direct_crosswalk_and_missing_paths(
    monkeypatch,
):
    assert await npi_module._resolve_internal_filter_codes(
        [], "CPT", "HCPCS", "procedure_codes"
    ) == ([], "none")
    assert await npi_module._resolve_internal_filter_codes(
        ["001", "1", "002"], "CPT", "CPT", "procedure_codes"
    ) == ([1, 2], "direct")

    table_available = AsyncMock(return_value=False)
    monkeypatch.setattr(npi_module, "_is_table_available", table_available)
    assert await npi_module._resolve_internal_filter_codes(
        ["A1"], "CPT", "HCPCS", "procedure_codes"
    ) == ([], "none")

    table_available.return_value = True
    execute_stmt = AsyncMock(return_value=_Rows([("003",), (None,), ()]))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    assert await npi_module._resolve_internal_filter_codes(
        ["A1"], "CPT", "HCPCS", "procedure_codes"
    ) == ([3], "crosswalk")
    assert execute_stmt.await_args.kwargs["params"] == {
        "from_system": "CPT",
        "target_system": "HCPCS",
        "input_codes": ["A1"],
    }

    execute_stmt.return_value = _Rows([])
    assert await npi_module._resolve_internal_filter_codes(
        ["A2"], "CPT", "HCPCS", "procedure_codes"
    ) == ([], "none")


def test_provider_directory_record_keys_are_exact_deduplicated_and_bounded(
    monkeypatch,
):
    role_a = (
        "provider_directory_fhir:practitioner_role:"
        "source-a:role-a:location-a"
    )
    role_b = (
        "provider_directory_fhir:practitioner_role:"
        "source-b:role-b:location-b"
    )
    role_c = (
        "provider_directory_fhir:practitioner_role:"
        "source-c:role-c:location-c"
    )
    affiliation_a = (
        "provider_directory_fhir:organization_affiliation:"
        "source-a:affiliation-a:location-a"
    )
    affiliation_b = (
        "provider_directory_fhir:organization_affiliation:"
        "source-b:affiliation-b:location-b"
    )

    assert npi_module._directory_source_ids(None) == []
    assert npi_module._directory_source_ids(
        ["invalid", role_a, role_a, "provider_directory_fhir:x::id"]
    ) == ["source-a"]
    assert npi_module._directory_role_keys_from_records(
        [None, "invalid", role_a, role_a]
    ) == [("source-a", "role-a")]
    assert npi_module._directory_affiliation_keys_from_records(
        [role_a, affiliation_a, affiliation_a]
    ) == [("source-a", "affiliation-a")]

    addresses = [
        "ignored",
        {"source_record_ids": [role_a, affiliation_a]},
        {"phone_source_record_ids": [role_a, role_b, affiliation_b]},
        {"source_record_ids": [role_c]},
    ]
    assert npi_module._provider_directory_source_ids_from_addresses(addresses) == [
        "source-a",
        "source-b",
        "source-c",
    ]

    monkeypatch.setattr(npi_module, "MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS", 2)
    assert npi_module._provider_directory_role_keys_from_addresses(addresses) == [
        ("source-a", "role-a"),
        ("source-b", "role-b"),
    ]
    assert npi_module._provider_directory_affiliation_keys_from_addresses(
        addresses
    ) == [
        ("source-a", "affiliation-a"),
        ("source-b", "affiliation-b"),
    ]
