# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryLocation, ProviderDirectoryPractitioner
from tests.provider_directory_fhir_bulk_test_support import (
    _Connection,
    _Response,
    _Session,
    _acquire,
    _checkpoint,
    _checkpoint_context,
    _client_session,
    _fetch_options,
    _identity,
    _manifest_payload,
    _source,
    _stream_options,
    importer,
)

def test_bulk_host_normalization_rejects_empty_and_non_https_values():
    assert importer._pagination_host_key(None) is None
    assert importer._pagination_host_key("http://example.test") is None
    source_map = {
        **_source(),
        "metadata_json": {
            **_source()["metadata_json"],
            "provider_directory_pagination_allowed_hosts": ["extra.example"],
        },
    }
    assert "extra.example:443" in importer._pagination_allowed_hosts(
        source_map,
        _identity().start_url,
    )


@pytest.mark.asyncio
async def test_bulk_guard_awaits_autocommit_and_rejects_non_owner():
    expected_connection = object()

    class AwaitableAutocommitConnection:
        def execution_options(self, **_options):
            async def resolve_connection():
                return expected_connection

            return resolve_connection()

    assert await importer._bulk_guard_autocommit_connection(
        AwaitableAutocommitConnection()
    ) is expected_connection

    guard_connection = type(
        "GuardConnection",
        (),
        {"scalar": AsyncMock(return_value=False)},
    )()
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._assert_bulk_checkpoint_guard_owner(
            guard_connection,
            _identity(),
        )


def test_bulk_checkpoint_keyring_and_ciphertext_fail_closed(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        "[ignored]",
    )
    monkeypatch.setattr(importer.json, "loads", lambda _value: {"not": "a-list"})
    with pytest.raises(RuntimeError, match="keyring_invalid"):
        importer._bulk_checkpoint_previous_secrets()

    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", raising=False)
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    monkeypatch.delenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        raising=False,
    )
    with pytest.raises(RuntimeError, match="encryption_key_missing"):
        importer._bulk_checkpoint_decryption_secrets()
    with pytest.raises(RuntimeError, match="ciphertext_invalid"):
        importer._decrypt_bulk_capability("plaintext")

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    with pytest.raises(RuntimeError, match="decryption_failed"):
        importer._decrypt_bulk_capability(
            importer.BULK_EXPORT_CAPABILITY_CIPHER_PREFIX + "invalid-token"
        )


def test_bulk_configuration_helpers_cover_absent_values(monkeypatch):
    assert importer._bulk_export_start_url({}, "Practitioner") is None
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT", "true")
    assert importer._is_bulk_export_enabled(None) is True
    assert importer._is_bulk_domain_hostname(None) is False
    assert importer._is_safe_bulk_https_url("") is False
    assert importer._is_same_bulk_url_origin(
        "http://example.test/output",
        "https://example.test/output",
    ) is False
    assert importer._bulk_output_allowed_hosts(
        {"metadata_json": {"provider_directory_bulk_export_output_hosts": "bad"}}
    ) == set()


@pytest.mark.asyncio
async def test_stream_checks_cancellation_for_each_chunk(monkeypatch):
    line = b'{"resourceType":"Practitioner","id":"one"}\n'
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (ProviderDirectoryPractitioner, {}),
    )
    cancel_probe = AsyncMock()

    stream_result = await importer._stream_bulk_export_output_rows(
        _Session(_Response(chunks=[line])),
        _source(),
        "https://storage.googleapis.com/aetna/part.ndjson",
        model=ProviderDirectoryPractitioner,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=1),
        row_batch_size=1,
        retain_rows=False,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=None,
            resume_offset=0,
            expected_etag=None,
            expected_content_length=None,
            cancel_probe=cancel_probe,
        ),
        requires_access_token=False,
    )

    assert stream_result[-1] is None
    assert cancel_probe.await_count == 2


@pytest.mark.asyncio
async def test_stream_rejects_malformed_unterminated_final_line():
    result = await importer._stream_bulk_export_output_rows(
        _Session(_Response(chunks=[b"{malformed"])),
        _source(),
        "https://storage.googleapis.com/aetna/part.ndjson",
        model=ProviderDirectoryPractitioner,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=AsyncMock(),
            resume_offset=0,
            expected_etag=None,
            expected_content_length=None,
        ),
        requires_access_token=False,
    )

    assert result[-1] == "invalid_ndjson"


@pytest.mark.asyncio
async def test_checkpoint_adoption_reloads_live_row_before_retry(monkeypatch):
    identity = _identity(
        owner_run_id="retry-run",
        retry_of_run_id="prior-run",
    )
    checkpoint_map = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
    }
    adopted_checkpoint_map = {
        **checkpoint_map,
        "owner_run_id": identity.owner_run_id,
    }
    monkeypatch.setattr(
        importer,
        "_adopt_bulk_export_checkpoint",
        AsyncMock(
            side_effect=[
                RuntimeError("bulk_export_checkpoint_ownership_conflict"),
                adopted_checkpoint_map,
            ]
        ),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_checkpoint_lease_seconds",
        lambda: 1_000_000_000,
    )
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value=checkpoint_map),
    )

    claimed, initial_status_payload, error = (
        await importer._claim_existing_bulk_export_checkpoint(
            identity,
            checkpoint_map,
            ownership_probe=AsyncMock(),
        )
    )

    assert claimed == adopted_checkpoint_map
    assert initial_status_payload is None
    assert error is None


@pytest.mark.asyncio
async def test_legacy_bulk_poll_success_can_return_empty_complete_result(monkeypatch):
    source_map = _source()
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            return_value=(
                202,
                {"content-location": f"{source_map['api_base']}/status/one"},
                None,
                None,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_export_poll_outputs",
        AsyncMock(return_value=([], None, 1)),
    )

    fetch_result = await importer._fetch_bulk_export_resource_rows(
        source_map,
        "Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    )

    assert fetch_result is not None
    assert fetch_result.complete is True
    assert fetch_result.pages_fetched == 1
