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


def _refreshable_checkpoint(identity):
    return {
        **_checkpoint(identity),
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
        "error": "bulk_export_output_http_410",
    }


def _refresh_error_result(_identity_value, error, polls, **_options):
    return None, error, polls


@pytest.mark.asyncio
async def test_refresh_orchestration_records_prepare_error(monkeypatch):
    identity = dataclasses.replace(_identity(), lineage_verified=True)
    monkeypatch.setattr(
        importer,
        "_prepare_bulk_capability_refresh",
        AsyncMock(side_effect=ValueError("prepare-error")),
    )
    record_error = AsyncMock(side_effect=_refresh_error_result)
    monkeypatch.setattr(
        importer,
        "_record_bulk_capability_refresh_error",
        record_error,
    )

    result = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), identity, _refreshable_checkpoint(identity),
        _fetch_options(), AsyncMock(),
    )

    assert result == (None, "prepare-error", 0)


@pytest.mark.asyncio
async def test_refresh_orchestration_records_request_error(monkeypatch):
    identity = dataclasses.replace(_identity(), lineage_verified=True)
    monkeypatch.setattr(
        importer,
        "_prepare_bulk_capability_refresh",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        importer,
        "_request_bulk_capability_refresh_for_options",
        AsyncMock(return_value=(None, "request-error", 2)),
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_capability_refresh_error",
        AsyncMock(side_effect=_refresh_error_result),
    )

    result = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), identity, _refreshable_checkpoint(identity),
        _fetch_options(), AsyncMock(),
    )

    assert result == (None, "request-error", 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("capability_check_outcome", ["raise", "return"])
async def test_refresh_orchestration_records_validation_error(
    monkeypatch,
    capability_check_outcome,
):
    identity = dataclasses.replace(_identity(), lineage_verified=True)
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(), "Practitioner",
    )
    monkeypatch.setattr(
        importer,
        "_prepare_bulk_capability_refresh",
        AsyncMock(return_value=[]),
    )
    monkeypatch.setattr(
        importer,
        "_request_bulk_capability_refresh_for_options",
        AsyncMock(return_value=(manifest, None, 2)),
    )
    validation_call = AsyncMock(
        side_effect=(
            ValueError("validation-error")
            if capability_check_outcome == "raise"
            else None
        ),
        return_value=(
            "validation-error"
            if capability_check_outcome == "return"
            else None
        ),
    )
    monkeypatch.setattr(
        importer,
        "_validate_and_persist_bulk_capabilities",
        validation_call,
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_capability_refresh_error",
        AsyncMock(side_effect=_refresh_error_result),
    )

    refresh_outcome = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), identity, _refreshable_checkpoint(identity),
        _fetch_options(), AsyncMock(),
    )

    assert refresh_outcome == (None, "validation-error", 2)


@pytest.mark.asyncio
async def test_refresh_cycle_rejects_lost_checkpoint(monkeypatch):
    identity = dataclasses.replace(_identity(), lineage_verified=True)
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(), "Practitioner",
    )
    monkeypatch.setattr(
        importer,
        "_next_checkpointed_bulk_manifest",
        AsyncMock(return_value=(manifest, None, 0)),
    )
    monkeypatch.setattr(
        importer,
        "_configured_bulk_stream_options",
        AsyncMock(return_value=(_stream_options(range_resume_enabled=True), None)),
    )
    monkeypatch.setattr(
        importer,
        "_stream_checkpointed_bulk_outputs",
        AsyncMock(
            return_value=importer._checkpointed_bulk_fetch_result(
                ProviderDirectoryPractitioner,
                error="bulk_export_output_http_410",
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={}),
    )

    with pytest.raises(RuntimeError, match="checkpoint_adoption_lost"):
        await importer._run_checkpointed_bulk_stream_cycles(
            object(), _source(), identity, _refreshable_checkpoint(identity),
            None, ProviderDirectoryPractitioner, _fetch_options(), AsyncMock(),
        )


@pytest.mark.asyncio
async def test_failed_refresh_checkpoint_requires_verified_lineage():
    identity = _identity()
    failed_checkpoint_by_field = {
        **_checkpoint(identity),
        "state": importer.BULK_EXPORT_CHECKPOINT_FAILED,
        "error": "bulk_export_output_http_404",
    }

    claimed, status_payload, error = (
        await importer._claim_failed_bulk_export_checkpoint(
            identity,
            failed_checkpoint_by_field,
            AsyncMock(),
        )
    )

    assert claimed == failed_checkpoint_by_field and status_payload is None
    assert error == "bulk_export_output_capability_refresh_lineage_unverified"


@pytest.mark.asyncio
@pytest.mark.parametrize("should_recover", [True, False])
async def test_failed_refresh_checkpoint_reports_recovery_outcome(
    monkeypatch,
    should_recover,
):
    identity = dataclasses.replace(_identity(), lineage_verified=True)
    failed_checkpoint_by_field = {
        **_checkpoint(identity),
        "state": importer.BULK_EXPORT_CHECKPOINT_FAILED,
        "error": "bulk_export_output_http_404",
    }
    recovered_checkpoint_by_field = {
        **failed_checkpoint_by_field,
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
    }
    recover = AsyncMock(
        side_effect=(
            None
            if should_recover
            else RuntimeError("bulk_export_checkpoint_ownership_conflict")
        ),
        return_value=recovered_checkpoint_by_field,
    )
    monkeypatch.setattr(
        importer,
        "_recover_failed_bulk_export_capability_checkpoint",
        recover,
    )
    ownership_probe = AsyncMock()

    claimed, status_payload, error = (
        await importer._claim_failed_bulk_export_checkpoint(
            identity,
            failed_checkpoint_by_field,
            ownership_probe,
        )
    )

    assert status_payload is None
    if should_recover:
        assert claimed == recovered_checkpoint_by_field and error is None
        ownership_probe.assert_awaited_once()
    else:
        assert claimed == failed_checkpoint_by_field
        assert error == "bulk_export_checkpoint_ownership_conflict"
