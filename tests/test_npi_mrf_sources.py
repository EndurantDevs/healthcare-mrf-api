# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import yaml

from api.endpoint import npi as npi_module


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return [SimpleNamespace(_mapping=row) for row in self._rows]


def _mrf_address_entries(first_address_key, second_address_key):
    return [
        {
            "npi": 1000000001,
            "address_key": first_address_key,
            "address_sources": ["nppes", "mrf"],
        },
        {
            "npi": 1000000001,
            "address_key": second_address_key,
            "address_sources": ["mrf"],
        },
        {
            "npi": 1000000001,
            "address_key": "33333333-3333-3333-3333-333333333333",
            "address_sources": ["nppes"],
        },
        {
            "npi": 1000000001,
            "address_key": first_address_key,
            "address_sources": ["mrf"],
        },
    ]


def _mrf_evidence_entries(first_address_key, second_address_key):
    return [
        {
            "npi": 1000000001,
            "address_key": first_address_key,
            "issuer_name": "Bluebird Health Plan",
            "issuer_ids": [22222],
            "source_urls": ["https://bluebird.example/providers.json"],
        },
        {
            "npi": 1000000001,
            "address_key": first_address_key,
            "issuer_name": "Northstar Health Plan",
            "issuer_ids": [11111, 11112],
            "source_urls": [
                "https://api-user:api-secret@northstar.example/a/providers.json?token=secret#part",
                "https://northstar.example/a/providers.json?token=other",
                "https://northstar.example/b/providers.json",
                "file:///private/import/providers.json",
            ],
        },
        {
            "npi": 1000000001,
            "address_key": second_address_key,
            "issuer_name": "Cedar Health Plan",
            "issuer_ids": [],
            "source_urls": [],
        },
    ]


def _assert_mrf_source_groups(addresses):
    assert addresses[0]["mrf_source_count"] == 2
    assert addresses[0]["mrf_sources"] == [
        {
            "source": "mrf",
            "issuer_name": "Bluebird Health Plan",
            "issuer_ids": [22222],
            "source_name": "Bluebird Health Plan (issuer 22222)",
            "source_urls": ["https://bluebird.example/providers.json"],
        },
        {
            "source": "mrf",
            "issuer_name": "Northstar Health Plan",
            "issuer_ids": [11111, 11112],
            "source_name": "Northstar Health Plan (issuers 11111, 11112)",
            "source_urls": [
                "https://northstar.example/a/providers.json",
                "https://northstar.example/b/providers.json",
            ],
        },
    ]
    assert addresses[1]["mrf_sources"] == [
        {
            "source": "mrf",
            "issuer_name": "Cedar Health Plan",
            "issuer_ids": [],
            "source_name": "Cedar Health Plan",
            "source_urls": [],
        }
    ]
    assert addresses[1]["mrf_source_count"] == 1
    assert "mrf_sources" not in addresses[2]
    assert addresses[3]["mrf_sources"] == addresses[0]["mrf_sources"]
    assert addresses[3]["mrf_sources"] is not addresses[0]["mrf_sources"]
    assert "secret" not in str(addresses[0]["mrf_sources"])


@pytest.mark.asyncio
async def test_mrf_sources_are_address_local_issuer_groups_with_exact_url_aliases(
    monkeypatch,
):
    """Attach normalized issuer groups only to their exact MRF-backed address."""
    first_address_key = "11111111-1111-1111-1111-111111111111"
    second_address_key = "22222222-2222-2222-2222-222222222222"
    addresses = _mrf_address_entries(first_address_key, second_address_key)
    execute_stmt = AsyncMock(
        return_value=_Rows(_mrf_evidence_entries(first_address_key, second_address_key))
    )
    table_available = AsyncMock(return_value=True)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    monkeypatch.setattr(npi_module, "_is_table_available", table_available)

    await npi_module._attach_mrf_source_details(addresses, session="session")

    execute_stmt.assert_awaited_once()
    table_available.assert_awaited_once_with(
        "mrf_address_evidence",
        session="session",
    )
    statement = str(execute_stmt.await_args.args[0])
    assert "mrf_address_evidence" in statement
    assert "mrf_address" not in statement.replace("mrf_address_evidence", "")
    assert "LOWER(BTRIM(evidence.issuer_name))" in statement
    assert "ARRAY_AGG(DISTINCT evidence.issuer_id" in statement
    assert "network_tier" not in statement
    assert "year" not in statement
    assert execute_stmt.await_args.kwargs == {
        "session": "session",
        "params": {
            "npis": [1000000001, 1000000001],
            "address_keys": [first_address_key, second_address_key],
        },
    }
    _assert_mrf_source_groups(addresses)


@pytest.mark.asyncio
async def test_mrf_sources_fail_closed_for_missing_or_stale_address_keys(monkeypatch):
    addresses = [
        {"npi": 1000000001, "address_key": None, "address_sources": ["mrf"]},
        {
            "npi": 1000000001,
            "address_key": "11111111-1111-1111-1111-111111111111",
            "address_sources": ["mrf"],
        },
    ]
    monkeypatch.setattr(
        npi_module,
        "_execute_stmt",
        AsyncMock(
            return_value=_Rows(
                [
                    {
                        "npi": 1000000001,
                        "address_key": "99999999-9999-9999-9999-999999999999",
                        "issuer_name": "Northstar Health Plan",
                        "issuer_ids": [11111],
                        "source_urls": ["https://northstar.example/providers.json"],
                    }
                ]
            )
        ),
    )
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=True),
    )

    await npi_module._attach_mrf_source_details(addresses)

    assert all("mrf_sources" not in address for address in addresses)
    assert all("mrf_source_count" not in address for address in addresses)


@pytest.mark.asyncio
async def test_mrf_sources_fail_closed_when_evidence_table_is_unavailable(
    monkeypatch,
):
    execute_stmt = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute_stmt)
    monkeypatch.setattr(
        npi_module,
        "_is_table_available",
        AsyncMock(return_value=False),
    )
    addresses = [
        {
            "npi": 1000000001,
            "address_key": "11111111-1111-1111-1111-111111111111",
            "address_sources": ["mrf"],
        }
    ]

    await npi_module._attach_mrf_source_details(addresses)

    execute_stmt.assert_not_awaited()
    assert "mrf_sources" not in addresses[0]


def test_public_mrf_source_urls_reject_local_literal_hosts():
    rejected_urls = (
        "http://localhost/providers.json",
        "http://imports.localhost/providers.json",
        "http://imports.LOCALHOST./providers.json",
        "http://127.0.0.1/providers.json",
        "http://10.0.0.1/providers.json",
        "http://169.254.169.254/providers.json",
        "http://[::1]/providers.json",
    )

    assert all(
        npi_module._public_mrf_source_url(source_url) is None
        for source_url in rejected_urls
    )
    assert (
        npi_module._public_mrf_source_url("https://8.8.8.8/providers.json")
        == "https://8.8.8.8/providers.json"
    )


@pytest.mark.asyncio
async def test_shared_source_attachment_skips_mrf_read_without_include_sources(
    monkeypatch,
):
    fhir_attachment = AsyncMock()
    mrf_attachment = AsyncMock()
    monkeypatch.setattr(
        npi_module,
        "_attach_provider_directory_source_details",
        fhir_attachment,
    )
    monkeypatch.setattr(npi_module, "_attach_mrf_source_details", mrf_attachment)
    addresses = [
        {
            "npi": 1000000001,
            "address_key": "11111111-1111-1111-1111-111111111111",
            "address_sources": ["mrf"],
        }
    ]

    await npi_module._attach_selected_address_source_details(
        addresses,
        include_sources=False,
    )

    fhir_attachment.assert_awaited_once()
    mrf_attachment.assert_not_awaited()
    assert "mrf_sources" not in addresses[0]


def test_match_candidate_returns_selected_address_mrf_sources_only_when_requested():
    provider_map = {
        "npi": 1000000001,
        "address_key": "11111111-1111-1111-1111-111111111111",
        "address_sources": ["mrf"],
        "mrf_source_count": 1,
        "mrf_sources": [
            {
                "source": "mrf",
                "issuer_name": "Northstar Health Plan",
                "issuer_ids": [11111],
                "source_name": "Northstar Health Plan (issuer 11111)",
                "source_urls": ["https://northstar.example/providers.json"],
            }
        ],
    }
    parameter_map = {
        "include_sources": True,
        "include_evidence": False,
        "taxonomy_exact": (),
        "taxonomy_prefixes": (),
        "provider_type": None,
        "specialty_filter": None,
    }

    candidate = npi_module._match_candidate_output(
        provider_map,
        parameter_map,
        enrichment=None,
    )

    assert candidate["mrf_source_count"] == 1
    assert candidate["mrf_sources"] == provider_map["mrf_sources"]
    assert candidate["sources"]["mrf"] == {"matched": True, "source_count": 1}
    hidden_candidate = npi_module._match_candidate_output(
        provider_map,
        {**parameter_map, "include_sources": False},
        enrichment=None,
    )
    assert "mrf_source_count" not in hidden_candidate
    assert "mrf_sources" not in hidden_candidate
    assert "mrf" not in hidden_candidate["sources"]


def test_openapi_documents_address_local_mrf_source_shape():
    openapi_path = Path(__file__).resolve().parents[1] / "doc" / "openapi.yaml"
    schemas = yaml.safe_load(openapi_path.read_text())["components"]["schemas"]

    address_properties = schemas["NpiAddress"]["properties"]
    assert address_properties["mrf_sources"]["items"]["$ref"].endswith(
        "/MrfAddressSource"
    )
    assert address_properties["mrf_source_count"]["minimum"] == 1
    source_schema = schemas["MrfAddressSource"]
    assert source_schema["required"] == [
        "source",
        "issuer_name",
        "source_name",
        "issuer_ids",
        "source_urls",
    ]
    assert source_schema["additionalProperties"] is False
