# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import types

import pytest

from api.endpoint import npi as npi_module


SOURCE_ID = "pdfhir_nebraska"
SOURCE_RECORD_ID = (
    "provider_directory_fhir:organization_address:"
    f"{SOURCE_ID}:organization-1:0"
)
ADDRESS_KEY = "47ab23c8-74f7-41eb-8ffb-e8b9f6214594"


def test_geo_candidate_evidence_query_is_exact_and_current():
    """Geo corroboration must use exact keys from one current published run."""
    sql = npi_module._current_provider_directory_geo_evidence_sql()

    assert "unnest(" in sql
    assert "CAST(:candidate_npis AS bigint[])" in sql
    assert "CAST(:candidate_address_keys AS uuid[])" in sql
    assert "overlay.npi = requested.npi" in sql
    assert "overlay.address_key = requested.address_key" in sql
    assert "dataset.is_current IS TRUE" in sql
    assert "dataset.status = 'published'" in sql
    assert "dataset.published_at IS NOT NULL" in sql
    assert "dataset.superseded_at IS NULL" in sql
    assert "current_run.run_id = overlay.last_seen_run_id" in sql


@pytest.fixture
def geo_evidence_fakes(monkeypatch):
    """Install deterministic query and source-detail fakes."""
    captured_by_name = {}

    async def fake_execute(statement, **keyword_args):
        captured_by_name.update(
            sql=str(statement),
            session=keyword_args.get("session"),
            params=keyword_args.get("params"),
        )
        evidence_row_by_name = {
            "npi": 1003968710,
            "address_key": ADDRESS_KEY,
            "source_record_ids": [SOURCE_RECORD_ID],
        }
        return types.SimpleNamespace(all=lambda: [evidence_row_by_name])

    async def fake_source_details(source_id_list, *, session=None):
        assert source_id_list == [SOURCE_ID]
        assert session == "session"
        return {
            SOURCE_ID: {
                "source_id": SOURCE_ID,
                "endpoint_id": "endpoint-nebraska",
                "canonical_api_base": "https://nebraska.example/fhir",
                "org_name": "State of Nebraska",
                "plan_name": "Medicaid FFS",
            }
        }

    monkeypatch.setattr(npi_module, "_execute_stmt", fake_execute)
    monkeypatch.setattr(
        npi_module,
        "_fetch_provider_directory_source_detail_map",
        fake_source_details,
    )
    return captured_by_name


@pytest.mark.asyncio
async def test_geo_candidate_attaches_current_overlay_provenance(geo_evidence_fakes):
    """A canonical base row retains exact current directory provenance."""
    candidate_row_list = [
        {
            "npi": 1003968710,
            "address_key": ADDRESS_KEY,
            "source_record_ids": ["nppes:1003968710:practice"],
        }
    ]
    candidate_params_by_name = {
        "lat": 41.09967,
        "long": -101.67646,
        "include_sources": True,
        "include_evidence": False,
    }

    await npi_module._attach_match_candidate_source_details(
        candidate_row_list,
        candidate_params_by_name,
        session="session",
    )

    assert geo_evidence_fakes["session"] == "session"
    assert geo_evidence_fakes["params"] == {
        "candidate_npis": [1003968710],
        "candidate_address_keys": [ADDRESS_KEY],
    }
    assert "current_provider_directory_runs AS MATERIALIZED" in geo_evidence_fakes[
        "sql"
    ]
    assert candidate_row_list[0]["source_record_ids"] == [
        "nppes:1003968710:practice",
        SOURCE_RECORD_ID,
    ]
    source_summary = candidate_row_list[0]["provider_directory_sources"][0]
    assert source_summary["source_ids"] == [SOURCE_ID]
