# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import hashlib
import importlib
import io
import urllib.error
from unittest.mock import AsyncMock, Mock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def bulk_identity() -> importer.BulkExportCheckpointIdentity:
    """Build a deterministic checkpoint identity for isolated tests."""
    start_url = "https://bulk.example.test/fhir/$export?_type=Practitioner"
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-1",
        canonical_api_base="https://bulk.example.test/fhir",
        resource_type="Practitioner",
        source_scope_hash="scope-1",
        strategy_version="bulk-v1",
        acquisition_root_run_id="root-1",
        owner_run_id="run-1",
        retry_of_run_id=None,
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        start_url=start_url,
        start_url_hash=hashlib.sha256(start_url.encode()).hexdigest(),
    )


@contextlib.asynccontextmanager
async def artifact_guard(*_args, **_kwargs):
    """Yield a stable artifact-build fence without database access."""
    yield "build-fence"


class AsyncContext:
    """Minimal async context wrapper for mocked connections."""

    def __init__(self, value):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *_args):
        return False


class GuardConnection:
    """Mock one advisory-lock connection and its scalar results."""

    def __init__(self, *scalar_results):
        self.scalar = AsyncMock(side_effect=scalar_results)

    def execution_options(self, **_kwargs):
        return self


class GuardEngine:
    """Expose the mocked guard connection through engine.connect."""

    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return AsyncContext(self.connection)


def endpoint_candidate() -> importer.EndpointDatasetCandidate:
    """Build one immutable endpoint candidate for summary tests."""
    return importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        import_run_id="run-1",
        previous_dataset_id=None,
    )


def artifact_dataset() -> importer.ProviderDirectoryArtifactDataset:
    """Build one immutable artifact dataset for fence tests."""
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source-1",
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        evidence_run_id="root-1",
        selected_resources=("Practitioner",),
    )


def assert_bulk_decode_edges(monkeypatch) -> None:
    """Exercise corrupt checkpoint and validator decoding paths."""
    monkeypatch.setattr(
        importer,
        "_decrypt_bulk_capability",
        Mock(side_effect=RuntimeError("cipher invalid")),
    )
    checkpoint_by_field = {
        "status_url_ciphertext": "cipher",
        "status_url_hash": "hash",
    }
    assert importer._bulk_checkpoint_status_url_error(checkpoint_by_field) == "cipher invalid"
    monkeypatch.setattr(
        importer, "_decrypt_bulk_capability", Mock(return_value="not json")
    )
    with pytest.raises(RuntimeError, match="manifest_checkpoint_corrupt"):
        importer._bulk_manifest_payload_from_checkpoint(
            {"manifest_ciphertext": "cipher"}
        )
    with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
        importer._stored_bulk_output_validator(
            {
                "content_length_bytes": "bad",
                "committed_bytes": 0,
                "etag_ciphertext": "cipher",
                "etag_hash": "hash",
                "validator_checked_at": "now",
            }
        )
    assert not importer._is_complete_bulk_output_proof(
        {
            "state": importer.BULK_EXPORT_OUTPUT_COMPLETE,
            "content_length_bytes": "bad",
            "committed_bytes": 0,
        }
    )


async def run_nonvalid_probe_batch(monkeypatch) -> tuple[int, int, set[str]]:
    """Run forty local probe stubs through non-valid capability branches."""
    source_rows = [
        {
            "source_id": f"source-{source_index}",
            "api_base": "https://fixture.test/fhir",
        }
        for source_index in range(40)
    ]
    probe_by_field = {
        "status": "invalid",
        "http_status": 200,
        "url": "https://fixture.test/fhir/metadata",
        "api_base": "https://fixture.test/fhir",
        "error": "invalid fixture",
    }
    capability_by_field = {
        "resourceType": "CapabilityStatement",
        "fhirVersion": "4.0.1",
    }
    monkeypatch.setattr(
        importer,
        "_probe_source",
        AsyncMock(return_value=(probe_by_field, capability_by_field)),
    )
    monkeypatch.setattr(
        importer, "_provider_directory_api_endpoint_row", Mock(return_value=None)
    )
    monkeypatch.setattr(
        importer, "parse_capability", Mock(return_value={"source_id": "stub"})
    )
    monkeypatch.setattr(importer, "_upsert_rows", AsyncMock())
    progress = AsyncMock()
    monkeypatch.setattr(importer, "_mark_provider_directory_progress", progress)
    outcome = await importer._run_source_probe_batch(
        source_rows, timeout=1, concurrency=10, run_id="run-1"
    )
    assert progress.await_count < 42
    return outcome


async def assert_address_publication_edges(monkeypatch) -> None:
    """Cover deferred address publication and failed-stage cleanup."""
    monkeypatch.setattr(
        importer, "_address_corroboration_network_metrics", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        importer, "_provider_directory_artifact_build_guard", artifact_guard
    )
    monkeypatch.setattr(
        importer, "_stage_table_name", Mock(return_value="stage_address")
    )
    populate_stage = AsyncMock(return_value=False)
    monkeypatch.setattr(
        importer, "_populate_address_corroboration_stage", populate_stage
    )
    monkeypatch.setattr(
        importer,
        "_create_provider_directory_address_corroboration_indexes",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_prepared_address_corroboration_result",
        AsyncMock(return_value="prepared"),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=3))
    publication_outcome = (
        await importer.publish_provider_directory_address_corroboration_table(
            "public", defer_cutover=True
        )
    )
    assert publication_outcome == "prepared"
    populate_stage.side_effect = RuntimeError("stage failed")
    cleanup = AsyncMock()
    monkeypatch.setattr(
        importer, "_best_effort_drop_address_corroboration_stage", cleanup
    )
    with pytest.raises(RuntimeError, match="stage failed"):
        await importer.publish_provider_directory_address_corroboration_table("public")
    cleanup.assert_awaited_once()


async def assert_artifact_summary_edges(monkeypatch) -> None:
    """Cover invalid retained summaries and missing rebuilt lineage."""
    dataset = artifact_dataset()
    candidate = endpoint_candidate()
    monkeypatch.setattr(
        importer,
        "validate_semantic_source_summary",
        Mock(side_effect=importer.ProviderDirectorySourceSummaryError("invalid")),
    )
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        importer._validated_existing_artifact_dataset_source_summary(
            {importer.SOURCE_SUMMARY_METADATA_KEY: {}}, dataset, candidate
        )
    content_proof = importer.EndpointDatasetContentProof(
        "hash", 1, {"Practitioner": "resource-hash"}, {"Practitioner": 1}
    )
    monkeypatch.setattr(
        importer, "_existing_artifact_dataset_source_summary", Mock(return_value=None)
    )
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_proof",
        AsyncMock(return_value=content_proof),
    )
    monkeypatch.setattr(importer, "_assert_artifact_dataset_content_proof", Mock())
    monkeypatch.setattr(
        importer, "_record_current_dataset_publication_proof", AsyncMock()
    )
    monkeypatch.setattr(
        importer, "_endpoint_dataset_source_summary", AsyncMock(return_value=None)
    )
    with pytest.raises(importer.ProviderDirectoryArtifactBuildStale):
        await importer._refresh_current_artifact_dataset_source_summary(
            dataset, candidate, {}, {}
        )


async def assert_network_publication_edges(monkeypatch) -> None:
    """Cover deferred network publication with an unscoped build."""
    async_return_value_by_function_name = {
        "_network_catalog_missing_requirement": None,
        "_network_catalog_scope_sources": [],
        "_copy_existing_network_catalog": 2,
    }
    for function_name, return_value in async_return_value_by_function_name.items():
        monkeypatch.setattr(
            importer, function_name, AsyncMock(return_value=return_value)
        )
    for function_name in (
        "_ensure_provider_directory_network_catalog_table",
        "_create_provider_directory_network_catalog_indexes",
        "_prepare_provider_directory_artifact_stage",
    ):
        monkeypatch.setattr(importer, function_name, AsyncMock())
    monkeypatch.setattr(
        importer, "_provider_directory_artifact_build_guard", artifact_guard
    )
    monkeypatch.setattr(
        importer,
        "_network_catalog_stage_table_name",
        Mock(return_value="stage_network"),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value="INSERT 0 3"))
    monkeypatch.setattr(importer.db, "scalar", AsyncMock(return_value=5))
    metrics_by_field, stage = await importer.publish_provider_directory_network_catalog(
        "public", run_id=None, source_ids=None, defer_cutover=True
    )
    assert metrics_by_field["rows"] == 5
    assert stage.stage_table == "stage_network"


def assert_seed_fetch_edges(monkeypatch) -> None:
    """Cover environment seed resolution and a local HTTP error body."""
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_SEED_DB_URL", "https://fixture.test/seed.db"
    )
    monkeypatch.setattr(
        importer, "_download_seed_db", Mock(side_effect=lambda _url, path: path)
    )
    seed_path, temporary_directory = importer._resolve_seed_db(None, None)
    try:
        assert seed_path.name == "provider_directory.db"
    finally:
        temporary_directory.cleanup()
    http_error = urllib.error.HTTPError(
        "https://fixture.test/fhir", 429, "limited", {}, io.BytesIO(b"{}")
    )
    monkeypatch.setattr(
        importer.urllib.request, "urlopen", Mock(side_effect=http_error)
    )
    status_code, payload_by_field, error, _elapsed = importer._fetch_json_sync(
        "https://fixture.test/fhir", timeout=1
    )
    assert (status_code, payload_by_field, error) == (429, {}, None)
    assert (
        importer._read_response_body_with_deadline(
            io.BytesIO(b"ignored"), timeout=1, max_bytes=0
        )
        == b""
    )
