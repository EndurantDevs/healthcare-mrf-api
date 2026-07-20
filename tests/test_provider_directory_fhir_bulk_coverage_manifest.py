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

@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (None, "bulk_export_status_non_bulk_payload"),
        (
            _manifest_payload(requiresAccessToken="false"),
            "bulk_export_manifest_invalid_requires_access_token",
        ),
        (
            _manifest_payload(request=""),
            "bulk_export_manifest_missing_request",
        ),
    ],
)
def test_manifest_parser_rejects_missing_required_identity_fields(
    payload,
    expected_error,
):
    with pytest.raises(ValueError, match=expected_error):
        importer._bulk_export_manifest_from_payload(payload, "Practitioner")


def test_bulk_request_and_checkpoint_identity_validation_edges():
    with pytest.raises(ValueError, match="manifest_invalid_request"):
        importer._normalized_bulk_request_identity("http://example.test/export")

    incomplete_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope-coverage",
        source_ids=("aetna-coverage",),
        owner_run_id="run-coverage",
        acquisition_root_run_id="root-coverage",
    )
    with pytest.raises(ValueError, match="checkpoint_context_incomplete"):
        importer._bulk_export_checkpoint_identity(
            incomplete_context,
            "Practitioner",
            _identity().start_url,
        )


def test_transaction_window_accepts_naive_reference_clock():
    importer._assert_bulk_transaction_time_window(
        "2026-07-20T12:00:00Z",
        reference_time=datetime.datetime(2026, 7, 20, 12),
    )


def test_retry_after_and_payload_fallback_cover_untyped_values():
    current = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    assert importer._bulk_export_retry_after_seconds(
        "Sun, 20 Jul 2026 12:01:00",
        now_utc=current,
    ) == 60
    assert importer._bulk_export_payload_error(
        {"error": [{"unexpected": "shape"}]}
    ) == "bulk_export_error_output"


@pytest.mark.asyncio
async def test_range_probe_sends_expected_strong_etag():
    session = _Session(_Response(status=206, headers={"ETag": '"etag"'}))

    status, headers, error = await importer._bulk_http_probe_output(
        session,
        _source(),
        "https://storage.googleapis.com/aetna/part.ndjson",
        requires_access_token=False,
        timeout=3,
        expected_etag='"etag"',
    )

    assert (status, headers, error) == (206, {"etag": '"etag"'}, None)
    assert session.calls[0][1]["headers"]["If-Match"] == '"etag"'


@pytest.mark.asyncio
async def test_successful_output_error_write_updates_parent_checkpoint(monkeypatch):
    checkpoint_error = AsyncMock()
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=1))
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        checkpoint_error,
    )

    await importer._record_bulk_export_output_error(
        _identity(),
        "output-coverage",
        3,
        100,
        "bulk_export_timeout",
    )

    checkpoint_error.assert_awaited_once_with(
        _identity(),
        "bulk_export_timeout",
        terminal=False,
    )


def test_matching_stored_validator_is_accepted():
    validator = importer.BulkExportOutputValidator(
        content_length_bytes=100,
        etag='"etag"',
        etag_hash="a" * 64,
        output_expires_at=None,
    )
    importer._assert_bulk_output_validator_unchanged(validator, validator)


@pytest.mark.asyncio
async def test_accept_checkpointed_immediate_manifest_skips_status_url(monkeypatch):
    identity = _identity()
    checkpoint = _checkpoint(identity)
    accept = AsyncMock(return_value=checkpoint)
    monkeypatch.setattr(importer, "_accept_bulk_export_checkpoint", accept)
    payload = _manifest_payload()

    result = await importer._accept_checkpointed_bulk_start(
        _source(),
        identity,
        200,
        None,
        payload,
    )

    assert result == (checkpoint, payload, None)
    accept.assert_awaited_once_with(identity, None)


@pytest.mark.asyncio
async def test_checkpointed_start_handles_unknown_and_invalid_success(monkeypatch):
    identity = _identity()
    ownership_probe = AsyncMock()
    unknown = (_checkpoint(identity), None, "bulk_export_acceptance_outcome_unknown")
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            side_effect=[
                (None, {}, None, "transport-error"),
                (200, {}, {}, None),
            ]
        ),
    )
    monkeypatch.setattr(
        importer,
        "_fail_unknown_bulk_export_acceptance",
        AsyncMock(return_value=unknown),
    )
    release = AsyncMock()
    monkeypatch.setattr(importer, "_release_bulk_export_reservation", release)

    assert await importer._start_checkpointed_bulk_export(
        object(),
        _source(),
        identity,
        timeout=3,
        ownership_probe=ownership_probe,
    ) == unknown
    assert await importer._start_checkpointed_bulk_export(
        object(),
        _source(),
        identity,
        timeout=3,
        ownership_probe=ownership_probe,
    ) == (None, None, None)
    release.assert_awaited_once_with(identity)


@pytest.mark.asyncio
async def test_checkpoint_poll_requires_acceptance_timestamp(monkeypatch):
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )

    result = await importer._poll_checkpointed_bulk_export_manifest(
        object(),
        _source(),
        _identity(),
        _checkpoint(_identity()),
        timeout=3,
        max_pending_seconds=30,
        runtime_probe=AsyncMock(),
    )

    assert result == (None, "bulk_export_checkpoint_accepted_at_invalid", 0)
    record_error.assert_awaited_once_with(
        _identity(),
        "bulk_export_checkpoint_accepted_at_invalid",
        terminal=True,
    )


@pytest.mark.asyncio
async def test_ready_manifest_persists_payload_errors_as_terminal(monkeypatch):
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )
    payload = {"error": [{"type": "OperationOutcome"}]}

    result = await importer._persist_ready_bulk_export_manifest(
        _source(),
        _identity(),
        payload,
        polls=2,
    )

    assert result == (None, "bulk_export_error_OperationOutcome", 2)
    record_error.assert_awaited_once_with(
        _identity(),
        "bulk_export_error_OperationOutcome",
        terminal=True,
    )


@pytest.mark.asyncio
async def test_initial_manifest_payload_skips_checkpoint_poll(monkeypatch):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    runtime_probe = AsyncMock()
    persist_ready = AsyncMock(return_value=(manifest, None, 0))
    monkeypatch.setattr(importer, "_bulk_manifest_from_checkpoint", lambda *_: None)
    monkeypatch.setattr(
        importer,
        "_persist_ready_bulk_export_manifest",
        persist_ready,
    )
    poll = AsyncMock()
    monkeypatch.setattr(importer, "_poll_checkpointed_bulk_export_manifest", poll)
    status_payload = _manifest_payload()

    manifest_result = await importer._checkpointed_bulk_export_manifest(
        object(),
        _source(),
        identity,
        _checkpoint(identity),
        initial_status_payload=status_payload,
        timeout=3,
        max_pending_seconds=30,
        runtime_probe=runtime_probe,
    )

    assert manifest_result == (manifest, None, 0)
    poll.assert_not_awaited()
    persist_ready.assert_awaited_once_with(
        _source(),
        identity,
        status_payload,
        polls=0,
    )
