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

@pytest.mark.asyncio
async def test_poll_boundary_detects_deadline_crossed_while_sleeping(monkeypatch):
    current = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    times = iter([current, current + datetime.timedelta(seconds=2)])
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: next(times))
    sleep = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)

    error = await importer._wait_for_bulk_export_poll_boundary(
        current + datetime.timedelta(seconds=5),
        current + datetime.timedelta(seconds=1),
    )

    assert error == importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    sleep.assert_awaited_once_with(1.0)


@pytest.mark.asyncio
async def test_pending_poll_handles_expired_and_quiet_poll_paths(monkeypatch):
    current = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: current)
    expired_context = importer.BulkExportPollContext(
        source_record=_source(),
        resource_type="Practitioner",
        status_url="https://example.test/status",
        poll_limit=3,
        poll_seconds=0,
        pending_deadline=current,
        options=importer.BulkExportPollOptions(),
    )
    assert await importer._handle_bulk_export_pending_poll(
        expired_context,
        {},
        1,
    ) == (None, importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED)

    boundary = AsyncMock(return_value=None)
    monkeypatch.setattr(importer, "_wait_for_bulk_export_poll_boundary", boundary)
    quiet_context = dataclasses.replace(expired_context, pending_deadline=None)
    next_poll_at, error = await importer._handle_bulk_export_pending_poll(
        quiet_context,
        {},
        2,
    )
    assert next_poll_at == current
    assert error is None
    boundary.assert_awaited_once_with(current, None)


def test_finished_poll_rejects_non_bulk_success_payload():
    context = importer.BulkExportPollContext(
        source_record=_source(),
        resource_type="Practitioner",
        status_url="https://example.test/status",
        poll_limit=3,
        poll_seconds=0,
        pending_deadline=None,
        options=importer.BulkExportPollOptions(),
    )
    assert importer._bulk_export_finished_poll_result(
        context,
        1,
        200,
        {},
        None,
    ) == (None, "bulk_export_status_non_bulk_payload", 1)


@pytest.mark.asyncio
async def test_legacy_poll_wait_can_skip_periodic_logging(monkeypatch):
    sleep = AsyncMock()
    log = AsyncMock()
    monkeypatch.setattr(importer.asyncio, "sleep", sleep)
    monkeypatch.setattr(importer, "_bulk_export_log", log)

    await importer._bulk_export_sleep_after_poll_wait(
        _source(),
        "Practitioner",
        2,
        3,
        {},
        0,
        "https://example.test/status",
    )

    sleep.assert_awaited_once_with(0)
    log.assert_not_awaited()


def test_status_url_and_manifest_checkpoint_fail_closed(monkeypatch):
    with pytest.raises(RuntimeError, match="status_url_hash_mismatch"):
        importer._bulk_checkpoint_status_url(
            {
                "state": importer.BULK_EXPORT_CHECKPOINT_ACCEPTED,
                "status_url_ciphertext": None,
                "status_url_hash": "orphaned-audit-hash",
            }
        )

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    ciphertext = importer._encrypt_bulk_capability(json.dumps(["not", "object"]))
    with pytest.raises(RuntimeError, match="manifest_checkpoint_corrupt"):
        importer._bulk_manifest_payload_from_checkpoint(
            {"manifest_ciphertext": ciphertext}
        )


@pytest.mark.asyncio
async def test_checkpoint_error_write_requires_current_owner(monkeypatch):
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))

    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._record_bulk_export_checkpoint_error(
            _identity(),
            "bulk_export_timeout",
            terminal=False,
        )


def test_manifest_output_identity_and_validator_changes_are_rejected():
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    assert importer._bulk_output_checkpoint_error(
        _identity().checkpoint_id,
        manifest,
        [],
    ) == "bulk_export_manifest_output_checkpoint_mismatch"

    stored = importer.BulkExportOutputValidator(
        content_length_bytes=100,
        etag='"etag-one"',
        etag_hash="a" * 64,
        output_expires_at=None,
    )
    observed = dataclasses.replace(stored, content_length_bytes=101)
    with pytest.raises(ValueError, match="output_validator_mismatch"):
        importer._assert_bulk_output_validator_unchanged(stored, observed)


@pytest.mark.asyncio
async def test_persist_validator_requires_current_output_owner(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    etag = '"etag"'
    validator = importer.BulkExportOutputValidator(
        content_length_bytes=100,
        etag=etag,
        etag_hash=hashlib.sha256(etag.encode()).hexdigest(),
        output_expires_at=None,
    )

    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._persist_bulk_output_validator(
            _identity(),
            {
                "output_id": "output-coverage",
                "state": importer.BULK_EXPORT_OUTPUT_PENDING,
            },
            validator,
        )


@pytest.mark.asyncio
async def test_probe_validator_returns_transport_error_before_parsing(monkeypatch):
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    monkeypatch.setattr(
        importer,
        "_bulk_http_probe_output",
        AsyncMock(return_value=(None, {}, "probe-failed")),
    )

    error = await importer._probe_and_persist_bulk_output_validator(
        object(),
        _source(),
        _identity(),
        manifest,
        manifest.outputs[0],
        {"output_id": "output-coverage"},
        timeout=3,
    )

    assert error == "probe-failed"


@pytest.mark.asyncio
async def test_complete_output_without_capabilities_needs_no_cleanup(monkeypatch):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    output = manifest.outputs[0]
    output_checkpoint_map = {
        "output_id": importer._bulk_manifest_output_id(
            identity.checkpoint_id,
            output,
        ),
        "state": importer.BULK_EXPORT_OUTPUT_COMPLETE,
        "content_length_bytes": 100,
        "committed_bytes": 100,
        "etag_hash": "a" * 64,
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
        "output_url_ciphertext": None,
        "etag_ciphertext": None,
    }
    clear_capability = AsyncMock()
    monkeypatch.setattr(importer, "_clear_bulk_output_capability", clear_capability)
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=[output_checkpoint_map]),
    )

    checkpoints, error = await importer._prepare_bulk_output_validators(
        object(),
        _source(),
        identity,
        manifest,
        [output_checkpoint_map],
        timeout=3,
        ownership_probe=AsyncMock(),
    )

    assert checkpoints == [output_checkpoint_map]
    assert error is None
    clear_capability.assert_not_awaited()


@pytest.mark.asyncio
async def test_output_error_write_honors_rowcount_and_parent_opt_out(monkeypatch):
    checkpoint_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        checkpoint_error,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._record_bulk_export_output_error(
            _identity(),
            "output-coverage",
            3,
            100,
            "bulk_export_timeout",
        )

    importer.db.status.return_value = 1
    await importer._record_bulk_export_output_error(
        _identity(),
        "output-coverage",
        3,
        100,
        "bulk_export_timeout",
        record_checkpoint=False,
    )
    checkpoint_error.assert_not_awaited()
