# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit collection probes preserve source admission and credential checks."""

import importlib
from unittest.mock import AsyncMock

import pytest

importer = importlib.import_module("process.provider_directory_fhir")
BASE = "https://directory.example/fhir"
SEARCH_BUNDLE = {
    "resourceType": "Bundle",
    "type": "searchset",
    "entry": [{"resource": {"resourceType": "Practitioner", "id": "example"}}],
}


@pytest.fixture
def source():
    return {
        "source_id": "source-example",
        "api_base": BASE,
        "canonical_api_base": BASE,
        "auth_type": "none",
        "metadata_json": {
            "provider_directory_probe_resource": "Practitioner",
            "provider_directory_supported_resources": ["Practitioner"],
        },
    }


@pytest.fixture
def probe_io(monkeypatch):
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV, raising=False)
    monkeypatch.delenv(importer.PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV, raising=False)
    fetch = AsyncMock(return_value=(200, SEARCH_BUNDLE, None, 5))
    upsert = AsyncMock(return_value=1)
    progress = AsyncMock()
    monkeypatch.setattr(importer, "_fetch_source_json", fetch)
    monkeypatch.setattr(importer, "_upsert_rows", upsert)
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", progress)
    return fetch, upsert, progress


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response", "expected", "api_base"),
    [
        pytest.param((200, SEARCH_BUNDLE, None, 5), "valid", BASE, id="search-bundle"),
        pytest.param((200, None, None, 5), "resource_error", BASE, id="html-decoded-none"),
        pytest.param((200, {"resourceType": "Bundle"}, None, 5), "resource_error", BASE, id="malformed-bundle"),
        pytest.param(
            (200, {**SEARCH_BUNDLE, "entry": [{"resource": {"resourceType": "Location", "id": "example"}}]}, None, 5),
            "resource_error",
            BASE,
            id="wrong-resource",
        ),
        pytest.param(
            (200, {"resourceType": "OperationOutcome", "issue": []}, None, 5),
            "resource_error",
            BASE,
            id="operation-outcome",
        ),
        pytest.param((401, None, None, 5), "auth_required", BASE, id="unauthorized"),
        pytest.param((403, None, None, 5), "auth_required", BASE, id="forbidden"),
        pytest.param((None, None, "SocketTimeoutError", 5), "resource_error", BASE, id="transport-error"),
        pytest.param(
            (200, {"statusCode": 403, "message": "Out of call volume quota"}, None, 5),
            "quota_exhausted",
            importer.MOLINA_PROVIDER_DIRECTORY_BASE,
            id="known-quota",
        ),
    ],
)
async def test_collection_probe_and_batch_require_valid_search(source, probe_io, response, expected, api_base):
    fetch, upsert, progress = probe_io
    source.update(api_base=api_base, canonical_api_base=api_base)
    fetch.return_value = response
    probe, capability = await importer._probe_source(source, timeout=3, run_id="run-example")
    assert probe["status"] == expected
    assert capability is None
    assert probe["resource_probe"]["resource_type"] == "Practitioner"
    result = await importer._run_source_probe_batch([source], timeout=3, concurrency=1, run_id="run-example")
    assert result == (1, int(expected == "valid"), {source["source_id"]} if expected == "valid" else set())
    assert fetch.await_count == 2
    assert all(call.args[1] == f"{api_base}/Practitioner?_count=1" for call in fetch.await_args_list)
    stored_rows_by_model = {call.args[0]: call.args[1] for call in upsert.await_args_list}
    assert stored_rows_by_model[importer.ProviderDirectorySource][0]["last_validated_status"] == expected
    capability_row = stored_rows_by_model[importer.ProviderDirectoryCapability][0]
    assert capability_row["probe_status"] == expected
    assert "resource_types" not in capability_row and "fhir_version" not in capability_row
    assert progress.await_args.kwargs["done"] == 1


@pytest.mark.asyncio
async def test_collection_probe_keeps_missing_credentials_guard(source, probe_io):
    source.update(auth_type="OAuth2/SMART", requires_registration=True)
    probe, capability = await importer._probe_source(source, timeout=3, run_id="run-example")
    assert probe["status"] == "auth_required" and capability is None
    assert "no matching credentials" in probe["error"]
    probe_io[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_ordinary_source_still_probes_capability_metadata(source, probe_io):
    source["metadata_json"].pop("provider_directory_probe_resource")
    capability_statement_map = {
        "resourceType": "CapabilityStatement",
        "fhirVersion": "4.0.1",
        "rest": [{"resource": [{"type": "Practitioner"}]}],
    }
    fetch = probe_io[0]
    fetch.side_effect = [(200, capability_statement_map, None, 5), (200, SEARCH_BUNDLE, None, 5)]
    probe, returned_capability = await importer._probe_source(source, timeout=3, run_id="run-example")
    assert probe["status"] == "valid" and returned_capability == capability_statement_map
    assert [call.args[1] for call in fetch.await_args_list] == [
        f"{BASE}/metadata?_format=json",
        f"{BASE}/Practitioner?_count=1",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "metadata",
    [
        None,
        "invalid",
        [],
        {},
        {"provider_directory_probe_resource": ["Practitioner"]},
        {"provider_directory_probe_resource": "Patient"},
        {"provider_directory_probe_resource": "Location", "provider_directory_supported_resources": ["Practitioner"]},
    ],
)
async def test_collection_probe_ignores_absent_invalid_or_unsupported_opt_in(source, probe_io, metadata):
    source["metadata_json"] = metadata
    assert (
        await importer.source_selection.probe_declared_collection(importer, source, timeout=3, run_id="run-example")
        is None
    )
    probe_io[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_probe_does_not_replace_census_contract(source, probe_io, monkeypatch):
    monkeypatch.setattr(importer, "current_version_census_contract", lambda _source: object())
    assert (
        await importer.source_selection.probe_declared_collection(importer, source, timeout=3, run_id="run-example")
        is None
    )
    probe_io[0].assert_not_awaited()


@pytest.mark.asyncio
async def test_pacificsource_seed_probes_exact_json_collection(probe_io):
    source = importer._source_row_from_seed(importer.pacificsource.seed_rows()[0])
    probe, capability = await importer._probe_source(source, timeout=3, run_id="run-example")
    assert probe["status"] == "valid" and capability is None
    assert probe_io[0].await_args.args[1] == (
        "https://api.apim.pacificsource.com/fhir/provider/R4/Practitioner?_format=json&_count=1"
    )
