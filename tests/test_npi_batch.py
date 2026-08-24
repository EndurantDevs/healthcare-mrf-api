import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml
from sanic.exceptions import InvalidUsage

from api.endpoint import npi as npi_module
from tests.npi_location_hydration_support import unified_location_mapping


class _ResultRows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


@pytest.mark.asyncio
async def test_enrichment_batch_query_uses_runtime_schema(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "provider_tenant")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    execute = AsyncMock(return_value=_ResultRows([]))
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    await npi_module._provider_enrichment_rows_for_columns(
        [1234567890],
        {"npi"},
    )

    assert "FROM provider_tenant.provider_enrichment_summary" in str(
        execute.await_args.args[0]
    )


def test_openapi_documents_npi_batch_contract():
    operation = yaml.safe_load(Path("doc/openapi.yaml").read_text())["paths"]["/npi/id/batch"]["post"]
    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]
    npi_schema = request_schema["properties"]["npis"]
    assert npi_schema["uniqueItems"] is True
    assert "normalization" in npi_schema["description"]
    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    meta_schema = response_schema["properties"]["meta"]
    assert set(meta_schema["required"]) == {"elapsed_ms", "max_batch_size", "view"}
    assert meta_schema["properties"]["view"]["enum"] == ["summary"]


def test_batch_request_requires_unique_10_digit_npis_and_caps_at_100(monkeypatch):
    normalized = npi_module._normalize_npi_batch_request(
        {"npis": [str(1_000_000_000 + index) for index in range(100)]}
    )
    assert len(normalized["npis"]) == 100
    assert normalized["address_limit"] == 5

    for invalid_request_body, message in (
        ({"npis": []}, "between 1 and 100"),
        ({"npis": [True]}, "10-digit"),
        ({"npis": [{}]}, "10-digit"),
        ({"npis": ["1234567890", 1234567890]}, "unique"),
        ({"npis": ["123"]}, "10-digit"),
        ({"npis": [str(1_000_000_000 + index) for index in range(101)]}, "between 1 and 100"),
    ):
        with pytest.raises(InvalidUsage, match=message):
            npi_module._normalize_npi_batch_request(invalid_request_body)
    monkeypatch.setattr(npi_module, "NPI_BATCH_MAX_SIZE", 2)
    with pytest.raises(InvalidUsage, match="between 1 and 2"):
        npi_module._normalize_npi_batch_request(
            {"npis": ["1234567890", "1098765432", "1987654321"]}
        )

    for invalid_request_body, message in (
        (None, "JSON object"),
        ({"npis": ["1234567890"], "unknown": True}, "unsupported batch field"),
        ({"npis": ["1234567890"], "address_limit": True}, "address_limit"),
        ({"npis": ["1234567890"], "include_sources": "true"}, "include_sources"),
    ):
        with pytest.raises(InvalidUsage, match=message):
            npi_module._normalize_npi_batch_request(invalid_request_body)


def _ranked_batch_addresses(npi):
    return [
        {
            "npi": npi,
            "type": "primary",
            "first_line": f"{index} Main Street",
            "city_name": "Chicago",
            "state_name": "IL",
            "postal_code": "60601",
            "country_code": "US",
            "_base_row_identities": [f"location:address-{index}"],
        }
        for index in range(1, 4)
    ]


@pytest.mark.asyncio
async def test_batch_uses_set_maps_once_and_preserves_partial_result_order(monkeypatch):
    found_npi = 1234567890
    missing_npi = 1098765432
    address_map = {
        **_ranked_batch_addresses(found_npi)[0],
        "state_code": "IL",
        "address_key": "00000000-0000-0000-0000-000000000001",
        "address_sources": ["nppes"],
    }
    identity_map = AsyncMock(return_value={found_npi: {"npi": found_npi}})
    candidate_map = AsyncMock(return_value={found_npi: [address_map]})
    overlay_map = AsyncMock(return_value={})
    hydration_map = AsyncMock(return_value={found_npi: [address_map]})
    names_map = AsyncMock(return_value={found_npi: []})
    enrichment_map = AsyncMock(return_value={})

    monkeypatch.setattr(npi_module, "_build_npi_identity_details_map", identity_map)
    monkeypatch.setattr(npi_module, "_fetch_npi_location_candidates_map", candidate_map)
    monkeypatch.setattr(npi_module, "_fetch_provider_directory_address_overlay_map", overlay_map)
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)
    monkeypatch.setattr(npi_module, "_fetch_other_names_map", names_map)
    monkeypatch.setattr(npi_module, "_fetch_provider_enrichment_summary_map", enrichment_map)
    monkeypatch.setattr(npi_module, "_apply_location_statuses", AsyncMock())
    monkeypatch.setattr(
        npi_module,
        "_attach_provider_directory_source_details",
        AsyncMock(),
    )
    monkeypatch.setattr(npi_module, "_request_session", lambda _request: None)

    request = SimpleNamespace(
        json={
            "npis": [str(found_npi), str(missing_npi)],
            "include_sources": True,
            "address_limit": 5,
        }
    )
    operation_response = await npi_module.get_npi_batch(request)
    response_map = json.loads(operation_response.body)

    assert [provider_result["npi"] for provider_result in response_map["items"]] == [found_npi, missing_npi]
    assert [provider_result["status"] for provider_result in response_map["items"]] == [200, 404]
    assert response_map["found"] == 1
    assert response_map["not_found"] == 1
    assert response_map["meta"]["max_batch_size"] == npi_module.NPI_BATCH_MAX_SIZE
    assert response_map["meta"]["view"] == "summary"
    assert response_map["meta"]["elapsed_ms"] >= 0
    assert response_map["items"][0]["provider"]["address_pagination"] == {
        "limit": 5,
        "offset": 0,
        "returned": 1,
        "total": 1,
        "has_more": False,
    }
    for batch_mock in (identity_map, candidate_map, overlay_map, hydration_map, names_map, enrichment_map):
        assert batch_mock.await_count == 1


@pytest.mark.asyncio
async def test_batch_address_offset_slices_and_paginates(monkeypatch):
    npi = 1234567890
    ranked_addresses = _ranked_batch_addresses(npi)
    hydration_map = AsyncMock(return_value={npi: [ranked_addresses[1]]})
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)

    selected_by_npi = await npi_module._hydrate_npi_batch_addresses(
        [npi],
        {npi: ranked_addresses},
        address_limit=1,
        address_offset=1,
        include_sources=False,
        include_evidence=False,
        session=None,
    )

    assert selected_by_npi[npi][0]["first_line"] == "2 Main Street"
    assert hydration_map.await_args.kwargs["address_row_identities"] == [
        "location:address-2"
    ]
    provider_result, was_found = npi_module._npi_batch_provider_result(
        npi,
        {"npi": npi},
        ranked_addresses,
        selected_by_npi[npi],
        [],
        None,
        {
            "address_limit": 1,
            "address_offset": 1,
            "include_sources": False,
            "include_evidence": False,
        },
    )
    assert was_found is True
    assert provider_result["provider"]["address_pagination"] == {
        "limit": 1,
        "offset": 1,
        "returned": 1,
        "total": 3,
        "has_more": True,
    }


@pytest.mark.asyncio
async def test_batch_skips_hydration_without_base_identities(monkeypatch):
    npi = 1234567890
    hydration_map = AsyncMock()
    monkeypatch.setattr(npi_module, "_fetch_npi_address_rows_map", hydration_map)
    overlay_address_map = {
        **_ranked_batch_addresses(npi)[0],
        "_base_row_identities": [],
    }
    selected_by_npi = await npi_module._hydrate_npi_batch_addresses(
        [npi],
        {npi: [overlay_address_map]},
        address_limit=1,
        address_offset=0,
        include_sources=False,
        include_evidence=False,
        session=None,
    )
    assert selected_by_npi[npi] == [overlay_address_map]
    hydration_map.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_identity_map_uses_one_sorted_set_query():
    first_npi = 1234567890
    second_npi = 1098765432

    def identity_row(npi):
        return [
            npi if column.key == "npi" else None
            for column in npi_module.NPIData.__table__.columns
        ] + [[], []]

    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=_ResultRows(
                [identity_row(first_npi), identity_row(second_npi)]
            )
        )
    )

    identity_map = await npi_module._build_npi_identity_details_map(
        [first_npi, second_npi, first_npi],
        session=session,
    )

    assert set(identity_map) == {first_npi, second_npi}
    assert identity_map[first_npi]["npi"] == first_npi
    assert identity_map[second_npi]["taxonomy_list"] == []
    statement_params = session.execute.await_args.args[0].compile().params.values()
    assert [second_npi, first_npi] in statement_params


@pytest.mark.asyncio
async def test_batch_location_maps_group_two_npis_in_one_query_each(monkeypatch):
    first_location_map = unified_location_mapping()
    second_location_map = {
        **first_location_map,
        "inferred_npi": 1098765432,
        "location_key": "synthetic-location-2",
    }
    monkeypatch.setattr(
        npi_module,
        "_address_serving_model",
        AsyncMock(return_value=npi_module.EntityAddressUnified),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value=set(first_location_map)),
    )
    execute_stmt = AsyncMock(
        side_effect=[
            _ResultRows([first_location_map, second_location_map]),
            _ResultRows([first_location_map, second_location_map]),
        ]
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)

    npis = [1234567890, 1098765432, 1234567890]
    candidate_map = await npi_module._fetch_npi_location_candidates_map(npis)
    hydration_map = await npi_module._fetch_npi_address_rows_map(
        npis,
        include_evidence=True,
        address_row_identities=[
            "location:synthetic-location-1",
            "location:synthetic-location-2",
        ],
    )

    assert set(candidate_map) == {1234567890, 1098765432}
    assert candidate_map[1098765432][0]["npi"] == 1098765432
    assert hydration_map[1234567890][0]["source_record_ids"] == (
        first_location_map["source_record_ids"]
    )
    assert hydration_map[1098765432][0]["_base_row_identities"] == [
        "location:synthetic-location-2"
    ]
    assert execute_stmt.await_count == 2
    for call in execute_stmt.await_args_list:
        assert [1098765432, 1234567890] in call.args[0].compile().params.values()


@pytest.mark.asyncio
async def test_batch_overlay_map_groups_npis_and_passes_shared_filters(monkeypatch):
    first_npi = 1234567890
    second_npi = 1098765432
    overlay_result = _ResultRows(
        [
            {"npi": first_npi, "first_line": "1 Main Street"},
            {"npi": second_npi, "first_line": "2 Main Street"},
            {"npi": None, "first_line": "Unassigned"},
        ]
    )
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        npi_module,
        "_table_columns",
        AsyncMock(return_value={"lat", "long"}),
    )
    execute_stmt = AsyncMock(return_value=overlay_result)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    session = object()

    overlay_map = await npi_module._fetch_provider_directory_address_overlay_map(
        [first_npi, second_npi, first_npi],
        address_key="synthetic-address-key",
        address_site_key="synthetic-site-key",
        session=session,
    )

    assert overlay_map == {
        first_npi: [{"npi": first_npi, "first_line": "1 Main Street"}],
        second_npi: [{"npi": second_npi, "first_line": "2 Main Street"}],
    }
    query_call = execute_stmt.await_args
    assert "overlay.npi = ANY(:npis)" in str(query_call.args[0])
    assert query_call.kwargs == {
        "session": session,
        "params": {
            "npis": [second_npi, first_npi],
            "address_key": "synthetic-address-key",
            "address_site_key": "synthetic-site-key",
        },
    }
