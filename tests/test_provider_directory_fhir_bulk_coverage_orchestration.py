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
async def test_output_checkpoint_validation_records_manifest_mismatch(monkeypatch):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    ownership_probe = AsyncMock()
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )

    checkpoints, error = await importer._validated_bulk_output_checkpoints(
        identity,
        manifest,
        ownership_probe,
    )

    assert checkpoints == []
    assert error == "bulk_export_manifest_output_checkpoint_mismatch"
    ownership_probe.assert_awaited_once()
    record_error.assert_awaited_once_with(identity, error, terminal=True)


@pytest.mark.asyncio
async def test_range_validator_success_continues_to_stream_resume(monkeypatch):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    checkpoints = [{"state": importer.BULK_EXPORT_OUTPUT_PENDING}]
    monkeypatch.setattr(
        importer,
        "_validated_bulk_output_checkpoints",
        AsyncMock(return_value=(checkpoints, None)),
    )
    monkeypatch.setattr(
        importer,
        "_prepare_bulk_output_validators",
        AsyncMock(return_value=(checkpoints, None)),
    )
    monkeypatch.setattr(
        importer,
        "_resume_checkpointed_bulk_outputs",
        AsyncMock(return_value=(importer.BulkExportStreamState(), "stream-error")),
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=checkpoints),
    )

    stream_result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )

    assert stream_result.error == "stream-error"


@pytest.mark.asyncio
async def test_non_owner_reservation_claims_existing_checkpoint(monkeypatch):
    identity = _identity()
    checkpoint = _checkpoint(identity)
    expected = (checkpoint, None, None)
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(importer, "_bulk_checkpoint_primary_secret", lambda: b"key")
    monkeypatch.setattr(
        importer,
        "_reserve_bulk_export_checkpoint",
        AsyncMock(return_value=(checkpoint, False)),
    )
    claim = AsyncMock(return_value=expected)
    monkeypatch.setattr(importer, "_claim_existing_bulk_export_checkpoint", claim)

    result = await importer._load_or_start_checkpointed_bulk_export(
        object(),
        _source(),
        identity,
        timeout=3,
        ownership_probe=AsyncMock(),
    )

    assert result == expected
    claim.assert_awaited_once()


def test_complete_terminal_checkpoint_returns_completed_result():
    result = importer._terminal_bulk_checkpoint_result(
        ProviderDirectoryPractitioner,
        {
            "state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE,
            "rows_written": 7,
        },
    )
    assert result is not None
    assert result.complete is True
    assert result.rows_fetched == 7

@pytest.mark.asyncio
async def test_checkpointed_fetch_rejects_unknown_resource_before_claiming():
    assert await importer._fetch_checkpointed_bulk_export_resource_rows(
        _source(),
        "UnknownResource",
        _checkpoint_context(),
        _fetch_options(),
    ) is None


@pytest.mark.asyncio
async def test_checkpoint_worker_guard_reraises_unexpected_runtime_error(monkeypatch):
    @contextlib.asynccontextmanager
    async def failing_guard(_identity):
        raise RuntimeError("unexpected-guard-error")
        yield AsyncMock()

    monkeypatch.setattr(importer, "_bulk_checkpoint_worker_guard", failing_guard)

    with pytest.raises(RuntimeError, match="unexpected-guard-error"):
        await importer._fetch_checkpointed_bulk_export_resource_rows(
            _source(),
            "Practitioner",
            _checkpoint_context(),
            _fetch_options(),
        )


@pytest.mark.asyncio
async def test_owned_fetch_honors_configuration_result_and_defensive_none(
    monkeypatch,
):
    identity = _identity()
    checkpoint = _checkpoint(identity)
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    expected = importer._checkpointed_bulk_fetch_result(
        ProviderDirectoryPractitioner,
        error="configuration-error",
    )
    cancel_probe = AsyncMock()
    runtime_probe = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bulk_fetch_runtime_probes",
        lambda *_: (cancel_probe, runtime_probe),
    )
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_load_active_bulk_checkpoint",
        AsyncMock(return_value=(checkpoint, _manifest_payload(), None)),
    )
    monkeypatch.setattr(
        importer,
        "_checkpointed_bulk_export_manifest",
        AsyncMock(return_value=(manifest, None, 0)),
    )
    monkeypatch.setattr(
        importer,
        "_configured_bulk_stream_options",
        AsyncMock(side_effect=[(None, expected), (None, None)]),
    )

    configured_fetch_result = (
        await importer._fetch_owned_checkpointed_bulk_resource_rows(
        _source(),
        identity,
        ProviderDirectoryPractitioner,
        _fetch_options(),
        AsyncMock(),
    )
    )
    assert configured_fetch_result is expected

    with pytest.raises(RuntimeError, match="stream_options_unavailable"):
        await importer._fetch_owned_checkpointed_bulk_resource_rows(
            _source(),
            identity,
            ProviderDirectoryPractitioner,
            _fetch_options(),
            AsyncMock(),
        )
