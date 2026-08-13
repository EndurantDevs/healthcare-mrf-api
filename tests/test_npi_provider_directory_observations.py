# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from api.endpoint import npi as npi_module


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


@pytest.mark.asyncio
async def test_observed_provider_directory_endpoint_returns_retained_candidate_rows(
    monkeypatch,
):
    npi = 1588616783
    query_result = _Result(
        [
            {
                "source_id": "pdfhir_source_a",
                "api_base": "https://example.test/fhir",
                "dataset_id": "dataset_a",
                "acquisition_root_run_id": "root_a",
                "dataset_status": "acquisition_abandoned",
                "dataset_created_at": "2026-08-13T08:00:00Z",
                "resource_type": "Practitioner",
                "resource_id": "practitioner_a",
                "payload_json": json.dumps(
                    {"npi": npi, "full_name": "Example Provider"}
                ),
            }
        ]
    )
    execute = AsyncMock(return_value=query_result)
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)
    request = types.SimpleNamespace(args={})

    response = await npi_module.get_provider_directory_observations(
        request,
        str(npi),
    )

    assert json.loads(response.body) == {
        "npi": npi,
        "completeness": "best_effort",
        "certified": False,
        "observations": [
            {
                "source_id": "pdfhir_source_a",
                "api_base": "https://example.test/fhir",
                "dataset_id": "dataset_a",
                "acquisition_root_run_id": "root_a",
                "dataset_status": "acquisition_abandoned",
                "dataset_created_at": "2026-08-13T08:00:00Z",
                "resource_type": "Practitioner",
                "resource_id": "practitioner_a",
                "resource": {"npi": npi, "full_name": "Example Provider"},
            }
        ],
    }
    sql = str(execute.await_args.args[0])
    assert "dataset.is_current IS FALSE" in sql
    assert "dataset.status IN" in sql
    assert "resource.payload_json::jsonb ->> 'npi' = :npi" in sql
    assert "ROW_NUMBER() OVER" in sql
    assert "WHERE recency_rank = 1" in sql


@pytest.mark.asyncio
async def test_observed_provider_directory_omits_unusable_payloads(monkeypatch):
    execute = AsyncMock(
        return_value=_Result([{"payload_json": None}, {"payload_json": "{not-json"}])
    )
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    assert await npi_module._fetch_provider_directory_observations(1588616783) == []


@pytest.mark.asyncio
async def test_observed_provider_directory_endpoint_rejects_invalid_npi(monkeypatch):
    execute = AsyncMock()
    monkeypatch.setattr(npi_module, "_execute_stmt", execute)

    response = await npi_module.get_provider_directory_observations(
        types.SimpleNamespace(args={}),
        "not-an-npi",
    )

    assert response.status == 400
    assert json.loads(response.body)["error"] == "invalid_npi"
    execute.assert_not_awaited()


def test_openapi_marks_retained_observations_as_non_certified():
    document = yaml.safe_load(Path("doc/openapi.yaml").read_text(encoding="utf-8"))
    operation = document["paths"]["/npi/id/{npi}/provider-directory-observations"]["get"]
    response = document["components"]["schemas"]["ProviderDirectoryObservationResponse"]

    assert operation["operationId"] == "getNpiIdNpiProviderDirectoryObservations"
    assert response["properties"]["completeness"]["enum"] == ["best_effort"]
    assert response["properties"]["certified"]["enum"] == [False]


def test_retained_observation_lookup_has_a_matching_partial_npi_index():
    migration = Path(
        "alembic/versions/20260813010000_provider_directory_observed_npi_index.py"
    ).read_text(encoding="utf-8")
    sql = npi_module._provider_directory_observations_sql("mrf")

    assert "CREATE INDEX CONCURRENTLY" in migration
    assert "payload_json::jsonb ->> 'npi'" in migration
    assert migration.index("payload_json::jsonb ->> 'npi'") < migration.index("dataset_id,")
    assert migration.index("dataset_id,") < migration.index("resource_type\n")
    assert "resource.payload_json::jsonb ->> 'npi' = :npi" in sql
