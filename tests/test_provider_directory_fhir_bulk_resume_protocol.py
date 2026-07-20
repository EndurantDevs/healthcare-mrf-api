# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import hashlib
import importlib
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")


def _aetna_source() -> dict:
    return importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows(
            source_query="Aetna"
        )[0]
    )


def _identity() -> importer.BulkExportCheckpointIdentity:
    start_url = (
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
        "/$export?_type=Practitioner"
    )
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-protocol",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-protocol",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-protocol",
        owner_run_id="run-protocol",
        retry_of_run_id=None,
        endpoint_id="endpoint-protocol",
        dataset_id="dataset-protocol",
        start_url=start_url,
        start_url_hash=hashlib.sha256(start_url.encode()).hexdigest(),
    )


def _manifest() -> importer.BulkExportManifest:
    return importer._bulk_export_manifest_from_payload(
        {
            "transactionTime": "2026-07-20T00:00:00Z",
            "request": _identity().start_url,
            "requiresAccessToken": False,
            "output": [
                {
                    "type": "Practitioner",
                    "url": (
                        "https://storage.googleapis.com/aetna/"
                        "part.ndjson?X-Goog-Signature=secret"
                    ),
                }
            ],
        },
        "Practitioner",
        expected_request_url=_identity().start_url,
    )


def _output_checkpoint(
    manifest: importer.BulkExportManifest,
) -> dict:
    output = manifest.outputs[0]
    return {
        "checkpoint_id": _identity().checkpoint_id,
        "output_id": importer._bulk_manifest_output_id(
            _identity().checkpoint_id,
            output,
        ),
        "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        "rows_written": 0,
        "committed_bytes": 0,
        "content_length_bytes": None,
        "etag_ciphertext": None,
        "etag_hash": None,
        "output_expires_at": None,
        "validator_checked_at": None,
    }


class _ProbeResponse:
    status = 206
    headers = {
        "Content-Length": "1",
        "Content-Range": "bytes 0-0/100",
        "ETag": '"output-v1"',
    }

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _ProbeSession:
    def __init__(self, failure: BaseException | None = None):
        self.failure = failure
        self.request_options = None

    def get(self, _request_url, **request_options):
        self.request_options = request_options
        if self.failure is not None:
            raise self.failure
        return _ProbeResponse()


@pytest.mark.parametrize(
    "configured_concurrency",
    [True, "not-a-number", 0, 9],
)
def test_bulk_output_concurrency_rejects_invalid_configuration(
    configured_concurrency,
):
    source_map = {
        "metadata_json": {
            "provider_directory_bulk_export_range_resume": True,
            "provider_directory_bulk_export_output_concurrency": (
                configured_concurrency
            ),
        }
    }

    with pytest.raises(
        ValueError,
        match="bulk_export_output_concurrency_invalid",
    ):
        importer._bulk_output_concurrency(source_map)


@pytest.mark.parametrize(
    ("output_url", "expected_expiration", "expected_error"),
    [
        (
            "https://storage.googleapis.com/aetna/out?Expires=1784563200",
            datetime.datetime.fromtimestamp(1784563200, datetime.UTC),
            None,
        ),
        (
            "https://storage.googleapis.com/aetna/out",
            None,
            None,
        ),
        (
            "https://storage.googleapis.com/aetna/out?Expires=invalid",
            None,
            "bulk_export_output_expiration_invalid",
        ),
        (
            "https://storage.googleapis.com/aetna/out"
            "?X-Goog-Date=invalid&X-Goog-Expires=3600",
            None,
            "bulk_export_output_expiration_invalid",
        ),
    ],
)
def test_bulk_output_expiration_parses_supported_signatures(
    output_url,
    expected_expiration,
    expected_error,
):
    if expected_error:
        with pytest.raises(ValueError, match=expected_error):
            importer._bulk_output_expiration(output_url)
        return
    assert importer._bulk_output_expiration(output_url) == expected_expiration


@pytest.mark.parametrize("content_length", [None, "invalid", "0", "-1"])
def test_bulk_content_length_rejects_nonpositive_or_missing_values(
    content_length,
):
    with pytest.raises(
        ValueError,
        match="bulk_export_output_content_length_invalid",
    ):
        importer._bulk_content_length({"content-length": content_length})


def test_bulk_output_validator_rejects_expired_url(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_bulk_export_now_utc",
        lambda: datetime.datetime(2026, 7, 20, 14, tzinfo=datetime.UTC),
    )
    with pytest.raises(ValueError, match="bulk_export_output_url_expired"):
        importer._bulk_output_validator(
            (
                "https://storage.googleapis.com/aetna/out"
                "?X-Goog-Date=20260720T120000Z&X-Goog-Expires=3600"
            ),
            {
                "accept-ranges": "bytes",
                "content-length": "100",
                "etag": '"output-v1"',
            },
        )


@pytest.mark.asyncio
async def test_bulk_probe_persists_identity_request_headers():
    session = _ProbeSession()

    status, headers_by_name, request_error = await importer._bulk_http_probe_output(
        session,
        _aetna_source(),
        _manifest().outputs[0].url,
        requires_access_token=False,
        timeout=3,
        expected_etag=None,
    )

    assert status == 206
    assert request_error is None
    assert headers_by_name == {
        "content-length": "1",
        "content-range": "bytes 0-0/100",
        "etag": '"output-v1"',
    }
    assert session.request_options["headers"]["Accept-Encoding"] == "identity"
    assert session.request_options["headers"]["Range"] == "bytes=0-0"
    assert session.request_options["allow_redirects"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "expected_error"),
    [
        (
            OSError("bulk_export_non_public_dns_address"),
            "bulk_export_non_public_dns_address",
        ),
        (OSError("network down"), "bulk_export_transport_oserror"),
        (
            RuntimeError("bulk_export_non_public_dns_address"),
            "bulk_export_non_public_dns_address",
        ),
        (RuntimeError("network down"), "bulk_export_transport_runtimeerror"),
    ],
)
async def test_bulk_probe_redacts_transport_failures(failure, expected_error):
    status, headers_by_name, request_error = await importer._bulk_http_probe_output(
        _ProbeSession(failure),
        _aetna_source(),
        _manifest().outputs[0].url,
        requires_access_token=False,
        timeout=3,
        expected_etag=None,
    )

    assert (status, headers_by_name, request_error) == (
        None,
        {},
        expected_error,
    )


@pytest.mark.asyncio
async def test_bulk_probe_rejects_untrusted_output_before_network():
    status, headers_by_name, request_error = await importer._bulk_http_probe_output(
        _ProbeSession(),
        _aetna_source(),
        "https://attacker.example/output.ndjson?signature=secret",
        requires_access_token=False,
        timeout=3,
        expected_etag=None,
    )

    assert (status, headers_by_name, request_error) == (
        None,
        {},
        "bulk_export_untrusted_output_url",
    )


@pytest.mark.asyncio
async def test_prepare_bulk_validators_persists_observed_identity(monkeypatch):
    manifest = _manifest()
    output_checkpoint_map = _output_checkpoint(manifest)
    persist_validator = AsyncMock()
    refreshed_checkpoints = [{"state": "refreshed"}]
    monkeypatch.setattr(
        importer,
        "_bulk_http_probe_output",
        AsyncMock(
            return_value=(
                206,
                {
                    "content-length": "1",
                    "content-range": "bytes 0-0/100",
                    "etag": '"output-v1"',
                },
                None,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_persist_bulk_output_validator",
        persist_validator,
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=refreshed_checkpoints),
    )

    checkpoints, validation_error = await importer._prepare_bulk_output_validators(
        object(),
        _aetna_source(),
        _identity(),
        manifest,
        [output_checkpoint_map],
        timeout=3,
    )

    assert checkpoints == refreshed_checkpoints
    assert validation_error is None
    persisted_validator = persist_validator.await_args.args[2]
    assert persisted_validator.content_length_bytes == 100
    assert persisted_validator.etag == '"output-v1"'


@pytest.mark.asyncio
async def test_prepare_bulk_validators_records_probe_failure(monkeypatch):
    manifest = _manifest()
    output_checkpoint_map = _output_checkpoint(manifest)
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bulk_http_probe_output",
        AsyncMock(return_value=(503, {}, None)),
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_output_error",
        record_error,
    )

    checkpoints, validation_error = await importer._prepare_bulk_output_validators(
        object(),
        _aetna_source(),
        _identity(),
        manifest,
        [output_checkpoint_map],
        timeout=3,
    )

    assert checkpoints == [output_checkpoint_map]
    assert validation_error == "bulk_export_output_http_503"
    record_error.assert_awaited_once_with(
        _identity(),
        output_checkpoint_map["output_id"],
        0,
        0,
        validation_error,
    )


def test_stored_bulk_validator_rejects_partial_checkpoint():
    with pytest.raises(
        ValueError,
        match="bulk_export_output_validator_checkpoint_corrupt",
    ):
        importer._stored_bulk_output_validator(
            {
                "content_length_bytes": 100,
                "etag_ciphertext": None,
                "etag_hash": None,
                "validator_checked_at": None,
            }
        )


def test_bulk_validator_change_is_terminal():
    stored = importer.BulkExportOutputValidator(
        content_length_bytes=100,
        etag='"output-v1"',
        etag_hash=hashlib.sha256(b'"output-v1"').hexdigest(),
        output_expires_at=None,
    )
    observed = importer.BulkExportOutputValidator(
        content_length_bytes=101,
        etag='"output-v2"',
        etag_hash=hashlib.sha256(b'"output-v2"').hexdigest(),
        output_expires_at=None,
    )

    with pytest.raises(ValueError, match="bulk_export_output_validator_mismatch"):
        importer._assert_bulk_output_validator_unchanged(stored, observed)


@pytest.mark.asyncio
async def test_complete_checkpoint_requires_output_proof(monkeypatch):
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value={
                "state": importer.BULK_EXPORT_CHECKPOINT_STREAMING,
            }
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="bulk_export_checkpoint_completion_lost",
    ):
        await importer._complete_bulk_export_checkpoint(
            _identity(),
            require_validators=True,
        )


@pytest.mark.asyncio
async def test_request_skips_get_at_complete_byte_checkpoint(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-resume-protocol-test-key",
    )
    etag = '"output-v1"'
    manifest = _manifest()
    output_checkpoint_map = {
        **_output_checkpoint(manifest),
        "content_length_bytes": 100,
        "etag_ciphertext": importer._encrypt_bulk_capability(etag),
        "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
        "committed_bytes": 100,
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
    }
    stream_output = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_stream_bulk_export_output_rows",
        stream_output,
    )
    options = importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-protocol",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=True,
        output_concurrency=1,
    )

    stream_result = await importer._request_checkpointed_bulk_output(
        object(),
        _aetna_source(),
        _identity(),
        manifest,
        manifest.outputs[0],
        output_checkpoint_map,
        options,
        AsyncMock(),
    )

    assert stream_result == ([], 0, 0, False, None)
    stream_output.assert_not_awaited()
