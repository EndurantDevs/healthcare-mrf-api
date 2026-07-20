# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import datetime
import hashlib
import importlib

import aiohttp
import pytest
from unittest.mock import AsyncMock


importer = importlib.import_module("process.provider_directory_fhir")


def _aetna_source() -> dict:
    return importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows(source_query="Aetna")[0]
    )


def _manifest_payload(**overrides) -> dict:
    payload = {
        "transactionTime": "2026-07-10T00:00:00Z",
        "request": "https://providerdirectory.api.aetna.com/fhir/$export?_type=Practitioner",
        "requiresAccessToken": False,
        "output": [
            {
                "type": "Practitioner",
                "url": "https://storage.googleapis.com/aetna/export.ndjson?sig=credential",
            }
        ],
    }
    payload.update(overrides)
    return payload


def test_bulk_checkpoint_encryption_fails_closed_without_any_configured_key(monkeypatch):
    for name in (
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        "HLTHPRT_CONTROL_API_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(
        RuntimeError, match="bulk_export_checkpoint_encryption_key_missing"
    ):
        importer._encrypt_bulk_capability("https://status.example/export?token=secret")


def test_bulk_checkpoint_capability_ciphertext_roundtrips_without_plaintext(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-security-test-key"
    )
    capability = "https://status.example/export?token=super-secret"

    ciphertext = importer._encrypt_bulk_capability(capability)

    assert ciphertext.startswith(importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX)
    assert capability not in ciphertext
    assert "super-secret" not in ciphertext
    assert importer._decrypt_bulk_capability(ciphertext) == capability


def test_stored_bulk_validator_roundtrips_encrypted_etag(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-resume-validator-test-key",
    )
    etag = '"output-v1"'
    checkpoint_map = {
        "content_length_bytes": 4321,
        "etag_ciphertext": importer._encrypt_bulk_capability(etag),
        "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
        "committed_bytes": 1234,
        "output_expires_at": None,
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
    }

    validator = importer._stored_bulk_output_validator(checkpoint_map)

    assert validator is not None
    assert validator.etag == etag
    assert validator.content_length_bytes == 4321
    assert etag not in checkpoint_map["etag_ciphertext"]


def test_bulk_checkpoint_decryption_survives_key_rotation(monkeypatch):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", raising=False)
    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        raising=False,
    )
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "old-control-secret")
    ciphertext = importer._encrypt_bulk_capability("https://status.example/job/1")

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "new-dedicated-secret",
    )
    assert importer._decrypt_bulk_capability(ciphertext).endswith("/job/1")

    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        "old-control-secret",
    )
    assert importer._decrypt_bulk_capability(ciphertext).endswith("/job/1")


def test_bulk_checkpoint_crypto_configuration_errors_are_retryable():
    assert not importer._is_bulk_export_error_terminal(
        "bulk_export_checkpoint_encryption_key_missing"
    )
    assert not importer._is_bulk_export_error_terminal(
        "bulk_export_checkpoint_decryption_failed"
    )
    assert importer._is_bulk_export_error_terminal(
        "bulk_export_checkpoint_ciphertext_invalid"
    )


def test_aetna_bulk_output_host_allowlist_is_exact():
    source = _aetna_source()

    assert importer._is_allowed_bulk_output_url(
        source, "https://storage.googleapis.com/aetna/export.ndjson"
    )
    assert not importer._is_allowed_bulk_output_url(
        source, "https://attacker.storage.googleapis.com/aetna/export.ndjson"
    )
    assert not importer._is_allowed_bulk_output_url(
        source, "https://storage.googleapis.com.attacker.example/export.ndjson"
    )


@pytest.mark.parametrize(
    "private_url",
    [
        "https://127.0.0.1/export",
        "https://169.254.169.254/latest/meta-data",
        "https://[::1]/export",
        "https://8.8.8.8/export",
    ],
)
def test_bulk_urls_reject_ip_literals(private_url):
    assert not importer._is_safe_bulk_https_url(private_url)


def test_bulk_request_paths_reject_ip_literals_before_network_access():
    private_source_map = {
        "api_base": "https://169.254.169.254/fhir",
        "canonical_api_base": "https://169.254.169.254/fhir",
        "metadata_json": {
            "provider_directory_bulk_export_output_hosts": ["127.0.0.1"]
        },
    }
    with pytest.raises(ValueError, match="bulk_export_untrusted_source_url"):
        importer._bulk_export_source_request_options(
            private_source_map,
            "https://169.254.169.254/fhir/$export",
        )
    assert not importer._is_allowed_bulk_output_url(
        private_source_map,
        "https://127.0.0.1/export.ndjson",
    )


@pytest.mark.parametrize(
    ("transaction_time", "error"),
    [
        (None, "bulk_export_manifest_missing_transaction_time"),
        ("not-a-timestamp", "bulk_export_manifest_invalid_transaction_time"),
        ("2026-07-10T00:00:00", "bulk_export_manifest_invalid_transaction_time"),
    ],
)
def test_bulk_manifest_rejects_missing_or_invalid_transaction_time(transaction_time, error):
    with pytest.raises(ValueError, match=error):
        importer._bulk_export_manifest_from_payload(
            _manifest_payload(transactionTime=transaction_time), "Practitioner"
        )


def test_bulk_manifest_rejects_request_mismatch_and_missing_selected_output():
    with pytest.raises(ValueError, match="bulk_export_manifest_request_mismatch"):
        importer._bulk_export_manifest_from_payload(
            _manifest_payload(),
            "Practitioner",
            expected_request_url="https://providerdirectory.api.aetna.com/fhir/$export?_type=Organization",
        )

    with pytest.raises(ValueError, match="bulk_export_manifest_missing_resource_output"):
        importer._bulk_export_manifest_from_payload(
            _manifest_payload(
                output=[
                    {
                        "type": "Organization",
                        "url": "https://storage.googleapis.com/aetna/organizations.ndjson",
                    }
                ]
            ),
            "Practitioner",
        )


def test_bulk_manifest_transaction_time_is_bounded(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_MANIFEST_MAX_AGE_SECONDS",
        "3600",
    )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_MANIFEST_FUTURE_SKEW_SECONDS",
        "300",
    )
    reference_time = datetime.datetime(2026, 7, 10, 12, tzinfo=datetime.UTC)
    importer._assert_bulk_transaction_time_window(
        "2026-07-10T11:30:00Z",
        reference_time=reference_time,
    )
    with pytest.raises(ValueError, match="transaction_time_stale"):
        importer._assert_bulk_transaction_time_window(
            "2026-07-09T00:00:00Z",
            reference_time=reference_time,
        )
    with pytest.raises(ValueError, match="transaction_time_future"):
        importer._assert_bulk_transaction_time_window(
            "2026-07-10T13:00:00Z",
            reference_time=reference_time,
        )


def test_bulk_promotion_rejects_stale_snapshot_without_prior_bulk_lineage(
    monkeypatch,
):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_MANIFEST_MAX_AGE_SECONDS",
        "86400",
    )
    stale_time = (
        datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=2)
    ).isoformat()
    with pytest.raises(RuntimeError, match="transaction_time_stale"):
        importer._assert_endpoint_dataset_bulk_freshness(
            {"Practitioner": {"fetch_mode": "checkpointed_bulk_export"}},
            {"Practitioner": stale_time},
            {},
        )


@pytest.mark.asyncio
async def test_bulk_manifest_poll_treats_redirect_as_terminal(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(302, {"location": "https://attacker.example"}, None, None)),
    )

    payload, error, polls = await importer._bulk_export_poll_manifest(
        object(),
        _aetna_source(),
        "https://providerdirectory.api.aetna.com/fhir/status/1",
        resource_type="Practitioner",
        timeout=1,
    )

    assert payload is None
    assert error == "bulk_export_status_http_302"
    assert polls == 1


@pytest.mark.parametrize(
    "redirect_error",
    ["bulk_export_status_http_302", "bulk_export_output_http_307"],
)
def test_bulk_redirect_errors_are_terminal(redirect_error):
    assert importer._is_bulk_export_error_terminal(redirect_error)


@pytest.mark.asyncio
async def test_bulk_worker_guard_conflict_prevents_fetch(monkeypatch):
    @contextlib.asynccontextmanager
    async def active_worker_guard(_identity):
        raise RuntimeError("bulk_export_checkpoint_worker_active")
        yield

    owned_fetch = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bulk_checkpoint_worker_guard",
        active_worker_guard,
    )
    monkeypatch.setattr(
        importer,
        "_fetch_owned_checkpointed_bulk_resource_rows",
        owned_fetch,
    )
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope-security",
        source_ids=("aetna",),
        owner_run_id="run-security",
        acquisition_root_run_id="run-security",
        endpoint_id="endpoint-security",
        dataset_id="dataset-security",
    )
    fetch_options = importer.BulkExportFetchOptions(
        timeout=1,
        run_id="run-security",
        row_batch_handler=AsyncMock(return_value=1),
        row_batch_size=1,
        retain_rows=False,
    )

    fetch_result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        _aetna_source(),
        "Practitioner",
        checkpoint_context,
        fetch_options,
    )

    assert fetch_result is not None
    assert fetch_result.error == "bulk_export_checkpoint_worker_active"
    owned_fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_batch_checks_ownership_before_writing():
    ordered_calls = []

    async def ownership_probe():
        ordered_calls.append("probe")

    async def write_batch(_model, resource_rows):
        ordered_calls.append("write")
        return len(resource_rows)

    options = importer.BulkExportStreamOptions(
        model=object,
        timeout=1,
        run_id="run-security",
        row_batch_handler=write_batch,
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=ownership_probe,
    )

    rows_written = await importer._write_owned_bulk_batch(
        options,
        object,
        [{"resource_id": "one"}],
    )

    assert rows_written == 1
    assert ordered_calls == ["probe", "write"]


@pytest.mark.asyncio
async def test_public_bulk_resolver_rejects_private_dns_answer(monkeypatch):
    resolver = importer._PublicBulkResolver()
    monkeypatch.setattr(
        resolver._resolver,
        "resolve",
        AsyncMock(return_value=[{"host": "10.0.0.7"}]),
    )

    with pytest.raises(OSError, match="bulk_export_non_public_dns_address"):
        await resolver.resolve("storage.googleapis.com", 443)
    await resolver.close()


@pytest.mark.asyncio
async def test_bulk_transport_errors_redact_query_credentials():
    source_map = {
        "api_base": "https://providerdirectory.api.aetna.com/fhir",
        "canonical_api_base": "https://providerdirectory.api.aetna.com/fhir",
    }
    requested_url = (
        "https://providerdirectory.api.aetna.com/fhir/status/1"
        "?access_token=super-secret&sig=another-secret"
    )

    class FailingSession:
        def get(self, *_args, **_kwargs):
            raise aiohttp.ClientConnectionError(requested_url)

    status, header_map, payload, error = await importer._bulk_http_get_json(
        FailingSession(), source_map, requested_url, timeout=1
    )

    assert (status, header_map, payload) == (None, {}, None)
    assert error == "bulk_export_transport_clientconnectionerror"
    assert "super-secret" not in error
    assert "another-secret" not in error


def test_bulk_capability_log_identity_redacts_path_and_query():
    capability_url = (
        "https://storage.googleapis.com/secret-job-id/output.ndjson"
        "?signature=super-secret"
    )

    log_identity = importer._bulk_capability_log_identity(capability_url)

    assert log_identity is not None
    assert log_identity.startswith("https://storage.googleapis.com/<redacted>")
    assert "secret-job-id" not in log_identity
    assert "super-secret" not in log_identity
