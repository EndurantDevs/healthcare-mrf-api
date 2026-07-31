# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi as npi_module


class _AsyncContext:
    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


@pytest.mark.asyncio
async def test_filter_capabilities_use_cache_and_schema_results(monkeypatch):
    cached_capability_map = {"npi_procedures_array_available": True}
    monkeypatch.setattr(
        npi_module,
        "_filter_cache_get",
        lambda: cached_capability_map,
    )
    assert (
        await npi_module._resolve_npi_filter_capabilities()
        is cached_capability_map
    )

    monkeypatch.setattr(npi_module, "_filter_cache_get", lambda: None)
    monkeypatch.setattr(npi_module, "_model_table_columns", lambda _model: set())
    monkeypatch.setattr(npi_module, "ENABLE_NPI_SCHEMA_CACHE", True)
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(
            return_value=SimpleNamespace(
                all=lambda: [("procedures_array",)]
            )
        ),
    )
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(side_effect=[True, False]),
    )
    monkeypatch.setattr(npi_module, "_filter_cache_set", lambda value: value)

    resolved = await npi_module._resolve_npi_filter_capabilities()

    assert resolved == {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": False,
        "pricing_provider_procedure_available": True,
        "pricing_provider_prescription_available": False,
    }


@pytest.mark.asyncio
async def test_fast_primary_count_uses_cache_and_unified_scalar(monkeypatch):
    monkeypatch.setattr(npi_module, "_primary_total_cache_get", lambda: 7)
    assert await npi_module._fast_primary_npi_count() == 7

    monkeypatch.setattr(npi_module, "_primary_total_cache_get", lambda: None)
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=9))
    monkeypatch.setattr(
        npi_module,
        "_primary_total_cache_set",
        lambda value: value,
    )
    assert await npi_module._fast_primary_npi_count() == 9


@pytest.mark.asyncio
async def test_taxonomy_codes_use_cache_and_request_session(monkeypatch):
    cache = npi_module._CLASSIFICATION_TAXONOMY_CODES_CACHE
    cache.clear()
    cache["hospital"] = (time.time(), ["282N00000X"])
    assert await npi_module._get_taxonomy_codes_for_classification(
        "Hospital"
    ) == ["282N00000X"]

    cache.clear()
    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=[
                ("261Q00000X",),
                (None,),
            ]
        )
    )
    assert await npi_module._get_taxonomy_codes_for_classification(
        "Clinic",
        session=session,
    ) == ["261Q00000X"]


@pytest.mark.asyncio
async def test_evidence_fetchers_open_a_session(monkeypatch):
    monkeypatch.setattr(
        npi_module.db,
        "session",
        lambda: _AsyncContext(object()),
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_evidence_tables",
        AsyncMock(return_value=None),
    )

    assert (
        await npi_module._fetch_provider_directory_role_evidence_map(
            [("source", "role")]
        )
        == {}
    )
    assert (
        await npi_module._fetch_provider_directory_affiliation_evidence_map(
            [("source", "affiliation")]
        )
        == {}
    )


def test_role_evidence_mapping_handles_role_without_metadata_and_network(
    monkeypatch,
):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_plan_metadata",
        lambda _mapping: None,
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_role_detail",
        lambda mapping: {"resource_id": mapping["resource_id"]},
    )
    monkeypatch.setattr(
        npi_module,
        "_append_provider_directory_network_evidence",
        lambda mapping, evidence, _keys: evidence["networks"].append(
            {"resource_id": mapping["resource_id"]}
        ),
    )
    evidence_rows = [
        {
            "source_id": "source",
            "role_id": "role",
            "resource_id": "role",
            "evidence_type": "role",
            "evidence_row_total": None,
        },
        {
            "source_id": "source",
            "role_id": "role",
            "resource_id": "network",
            "evidence_type": "network",
            "evidence_row_total": None,
        },
    ]

    evidence = npi_module._map_provider_directory_role_evidence(evidence_rows)[
        ("source", "role")
    ]

    assert evidence["practitioner_role"] == {"resource_id": "role"}
    assert evidence["networks"] == [{"resource_id": "network"}]


def test_affiliation_fields_include_plan_network_and_single_metadata(
    monkeypatch,
):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_endpoint_group_key",
        lambda _detail: ("endpoint_id", "endpoint"),
    )
    monkeypatch.setattr(
        npi_module,
        "_merge_provider_directory_role_evidence",
        lambda *_args, **_kwargs: (
            [{"resource_id": "plan"}],
            [{"resource_id": "network"}],
            [
                {
                    "returned": 1,
                    "total": 1,
                    "truncated": False,
                    "catalog_complete": True,
                }
            ],
            [],
        ),
    )

    fields = npi_module._provider_directory_affiliation_evidence_fields(
        ["source"],
        [("source", "affiliation")],
        {"source": {"endpoint_id": "endpoint"}},
        ("endpoint_id", "endpoint"),
        {("source", "affiliation"): {}},
    )

    assert fields["insurance_plans"] == [{"resource_id": "plan"}]
    assert fields["networks"] == [{"resource_id": "network"}]
    assert fields["insurance_plan_metadata"]["total"] == 1


@pytest.mark.asyncio
async def test_attach_source_details_handles_missing_nonmapping_and_success(
    monkeypatch,
):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_source_ids_from_addresses",
        lambda _addresses: ["source"],
    )
    detail_fetch = AsyncMock(side_effect=[{}, {"source": {"name": "payer"}}])
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        detail_fetch,
    )
    await npi_module._attach_provider_directory_source_details([{}])

    monkeypatch.setattr(
        npi_module,
        "_provider_directory_record_ids_from_address",
        lambda _address: ["record"],
    )
    monkeypatch.setattr(
        npi_module,
        "_directory_source_ids",
        lambda _records: ["source"],
    )
    monkeypatch.setattr(
        npi_module,
        "_directory_role_keys_from_records",
        lambda _records: [],
    )
    monkeypatch.setattr(
        npi_module,
        "_directory_affiliation_keys_from_records",
        lambda _records: [],
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_endpoint_provenance",
        lambda *_args: [{"source_id": "source"}],
    )
    address_map = {}
    await npi_module._attach_provider_directory_source_details(
        [object(), address_map]
    )

    assert address_map[npi_module.PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY] == [
        {"source_id": "source"}
    ]


def test_role_detail_aliases_accepting_patients(monkeypatch):
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_period",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_fhir_provenance",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_endpoint_details",
        lambda _value: [],
    )
    monkeypatch.setattr(
        npi_module,
        "_provider_directory_healthcare_service_details",
        lambda _value: [],
    )

    detail = npi_module._provider_directory_role_detail(
        {
            "source_id": "source",
            "resource_id": "role",
            "role_accepting_patients": True,
        }
    )

    assert detail["new_patient_acceptance"] is True
    assert detail["accepting_patients"] is True


def test_fhir_url_identity_brackets_ipv6_host():
    assert (
        npi_module._provider_directory_fhir_url_identity(
            "HTTPS://[2001:db8::1]/path?secret=value"
        )
        == "https://[2001:db8::1]/path"
    )
