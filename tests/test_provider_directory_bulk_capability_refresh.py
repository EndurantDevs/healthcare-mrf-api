"""Bulk export capability-refresh safety and failure-path tests."""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
from unittest.mock import AsyncMock, Mock

import pytest

from db.models import ProviderDirectoryPractitioner
from tests.provider_directory_fhir_bulk_test_support import (
    _Connection,
    _acquire,
    _checkpoint,
    _fetch_options,
    _identity,
    _manifest_payload,
    _source,
    _stream_options,
    importer,
)


def _refresh_identity():
    return dataclasses.replace(_identity(), lineage_verified=True)


def _refresh_manifest():
    identity = _refresh_identity()
    return importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
        expected_request_url=identity.start_url,
    )


def _refresh_output_checkpoint(**overrides):
    identity = _refresh_identity()
    manifest = _refresh_manifest()
    etag = '"stable"'
    output_checkpoint_by_field = {
        "output_id": importer._bulk_manifest_output_id(
            identity.checkpoint_id,
            manifest.outputs[0],
        ),
        "output_index": 0,
        "resource_type": "Practitioner",
        "output_url_hash": manifest.outputs[0].url_hash,
        "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        "content_length_bytes": 20,
        "committed_bytes": 10,
        "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
        "validator_checked_at": importer._bulk_export_now_utc(),
        "attempt_count": 0,
    }
    output_checkpoint_by_field.update(overrides)
    return output_checkpoint_by_field


def _refresh_checkpoint(identity=None):
    identity = identity or _refresh_identity()
    return {
        **_checkpoint(identity),
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
        "error": "bulk_export_output_http_410",
        "manifest_hash": "a" * 64,
        "manifest_json": {"requiresAccessToken": False},
    }


def _refresh_error_result(_identity_value, error, polls, **_options):
    return None, error, polls


def test_capability_refresh_attempt_limit_is_bounded(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_CAPABILITY_REFRESH_ATTEMPTS",
        "0",
    )
    assert importer._bulk_capability_refresh_attempt_limit() == 1
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_CAPABILITY_REFRESH_ATTEMPTS",
        "999",
    )
    assert (
        importer._bulk_capability_refresh_attempt_limit()
        == importer.BULK_EXPORT_MAX_CAPABILITY_REFRESH_ATTEMPTS
    )


def test_manifest_output_checkpoint_mismatch_raises():
    with pytest.raises(
        ValueError,
        match="bulk_export_manifest_output_checkpoint_mismatch",
    ):
        importer._bulk_output_checkpoint_for_manifest_output(
            _refresh_identity().checkpoint_id,
            _refresh_manifest().outputs[0],
            [],
        )


@pytest.mark.parametrize(
    "overrides",
    [
        {"content_length_bytes": "invalid"},
        {"content_length_bytes": 0},
        {
            "state": importer.BULK_EXPORT_OUTPUT_COMPLETE,
            "committed_bytes": 19,
        },
    ],
)
def test_retained_validator_proof_rejects_corruption(overrides):
    with pytest.raises(
        ValueError,
        match="bulk_export_output_validator_checkpoint_corrupt",
    ):
        importer._bulk_output_validator_proof(
            _refresh_output_checkpoint(**overrides)
        )


def test_refresh_output_pair_shape_is_exact():
    manifest = _refresh_manifest()
    with pytest.raises(ValueError, match="output_checkpoint_mismatch"):
        importer._bulk_capability_refresh_output_pairs(manifest, [])
    with pytest.raises(ValueError, match="output_checkpoint_mismatch"):
        importer._bulk_capability_refresh_output_pairs(
            manifest,
            [_refresh_output_checkpoint(resource_type="Location")],
        )


def test_refresh_manifest_contract_is_retained():
    manifest = _refresh_manifest()
    with pytest.raises(ValueError, match="manifest_checkpoint_corrupt"):
        importer._assert_bulk_capability_refresh_manifest_contract({}, manifest)
    with pytest.raises(ValueError, match="manifest_mismatch"):
        importer._assert_bulk_capability_refresh_manifest_contract(
            {
                "manifest_hash": "a" * 64,
                "manifest_json": {"requiresAccessToken": True},
            },
            manifest,
        )


@pytest.mark.asyncio
async def test_begin_refresh_persists_bounded_attempt(monkeypatch):
    connection = _Connection(1)
    connection.status.side_effect = [1, 1]
    monkeypatch.setattr(importer.db, "acquire", lambda: _acquire(connection))

    await importer._begin_bulk_capability_refresh(
        _refresh_identity(),
        _refresh_checkpoint(),
        [_refresh_output_checkpoint()],
    )

    assert connection.status.await_count == 2
    assert connection.status.await_args_list[1].kwargs["attempt_limit"] > 0


@pytest.mark.asyncio
async def test_begin_refresh_rejects_ineligible_state(monkeypatch):
    with pytest.raises(ValueError, match="refresh_not_required"):
        await importer._begin_bulk_capability_refresh(
            _refresh_identity(),
            {**_refresh_checkpoint(), "error": "different"},
            [_refresh_output_checkpoint()],
        )
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_CAPABILITY_REFRESH_ATTEMPTS",
        "1",
    )
    with pytest.raises(ValueError, match="refresh_exhausted"):
        await importer._begin_bulk_capability_refresh(
            _refresh_identity(),
            _refresh_checkpoint(),
            [_refresh_output_checkpoint(attempt_count=1)],
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status_results", "expected_error"),
    [
        ([0], "bulk_export_checkpoint_ownership_lost"),
        ([1, 0], "bulk_export_output_capability_refresh_exhausted"),
    ],
)
async def test_begin_refresh_rejects_lost_updates(
    monkeypatch,
    status_results,
    expected_error,
):
    connection = _Connection(1)
    connection.status.side_effect = status_results
    monkeypatch.setattr(importer.db, "acquire", lambda: _acquire(connection))
    with pytest.raises(RuntimeError, match=expected_error):
        await importer._begin_bulk_capability_refresh(
            _refresh_identity(),
            _refresh_checkpoint(),
            [_refresh_output_checkpoint()],
        )


@pytest.mark.asyncio
async def test_start_refresh_returns_redacted_request_result(monkeypatch):
    request_result = (
        202,
        {"location": "/status/1"},
        None,
        None,
    )
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=request_result),
    )
    refresh_log = Mock()
    monkeypatch.setattr(importer, "_bulk_export_log", refresh_log)

    result = await importer._start_bulk_capability_refresh_request(
        object(),
        _source(),
        _refresh_identity(),
        timeout=3,
    )

    assert result == (202, None, "/status/1", None)
    assert refresh_log.call_args.args == ("capability_refresh_start",)


@pytest.mark.asyncio
async def test_poll_refresh_validates_status_location(monkeypatch):
    result = await importer._poll_bulk_capability_refresh_request(
        object(),
        _source(),
        _refresh_identity(),
        None,
        timeout=3,
        max_pending_seconds=10,
        runtime_probe=AsyncMock(),
    )
    assert result == (None, "bulk_export_missing_status_url", 0)


@pytest.mark.asyncio
async def test_poll_refresh_uses_bounded_poll_contract(monkeypatch):
    expected_result = (_manifest_payload(), None, 2)
    poll_manifest = AsyncMock(return_value=expected_result)
    monkeypatch.setattr(importer, "_bulk_export_poll_manifest", poll_manifest)

    result = await importer._poll_bulk_capability_refresh_request(
        object(),
        _source(),
        _refresh_identity(),
        "/status/1",
        timeout=3,
        max_pending_seconds=10,
        runtime_probe=AsyncMock(),
    )

    assert result == expected_result
    poll_options = poll_manifest.await_args.kwargs["poll_options"]
    assert poll_options.max_pending_seconds == 10
    assert poll_options.cancel_probe is not None


def test_validated_refresh_manifest_accepts_only_safe_outputs():
    manifest, error = importer._validated_bulk_capability_refresh_manifest(
        _source(),
        _refresh_identity(),
        _manifest_payload(),
    )
    assert manifest is not None and error is None

    invalid_manifest, error = (
        importer._validated_bulk_capability_refresh_manifest(
            _source(),
            _refresh_identity(),
            {"requiresAccessToken": False},
        )
    )
    assert invalid_manifest is None
    assert error == "bulk_export_manifest_missing_transaction_time"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "start_result",
        "poll_result",
        "expected_error",
    ),
    [
        ((200, None, None, "request-failed"), None, "request-failed"),
        ((None, None, None, None), None, "bulk_export_acceptance_outcome_unknown"),
        ((503, None, None, None), None, "bulk_export_status_http_503"),
        ((200, None, None, None), None, "bulk_export_status_non_bulk_payload"),
        (
            (200, {"error": [{"type": "gone"}]}, None, None),
            None,
            "bulk_export_error_gone",
        ),
        ((202, None, "/status/1", None), (None, "poll-failed", 2), "poll-failed"),
        ((200, _manifest_payload(), None, None), None, None),
        ((202, None, "/status/1", None), (_manifest_payload(), None, 2), None),
    ],
)
async def test_refresh_request_outcomes(
    monkeypatch,
    start_result,
    poll_result,
    expected_error,
):
    monkeypatch.setattr(
        importer,
        "_start_bulk_capability_refresh_request",
        AsyncMock(return_value=start_result),
    )
    if poll_result is not None:
        monkeypatch.setattr(
            importer,
            "_poll_bulk_capability_refresh_request",
            AsyncMock(return_value=poll_result),
        )

    manifest, error, polls = (
        await importer._request_bulk_capability_refresh_manifest(
            object(),
            _source(),
            _refresh_identity(),
            timeout=3,
            max_pending_seconds=10,
            runtime_probe=AsyncMock(),
        )
    )

    assert error == expected_error
    assert (manifest is not None) is (expected_error is None)
    assert polls == (2 if start_result[0] == 202 else 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("probe_result", "expected_error"),
    [
        ((None, {}, "probe-failed"), "probe-failed"),
        ((500, {}, None), "bulk_export_output_http_500"),
    ],
)
async def test_refresh_probe_reports_transport_and_http_errors(
    monkeypatch,
    probe_result,
    expected_error,
):
    monkeypatch.setattr(
        importer,
        "_bulk_http_probe_output",
        AsyncMock(return_value=probe_result),
    )
    validators, error = await importer._probe_bulk_capability_refresh_validators(
        object(),
        _source(),
        _refresh_manifest(),
        [(_refresh_manifest().outputs[0], _refresh_output_checkpoint())],
        timeout=3,
        runtime_probe=AsyncMock(),
    )
    assert validators == []
    assert error == expected_error


def test_validator_expiration_is_normalized():
    expires_at = datetime.datetime.now(datetime.UTC)
    validator = importer.BulkExportOutputValidator(
        20,
        '"stable"',
        "a" * 64,
        expires_at,
    )
    assert importer._bulk_validator_expiration(validator) == expires_at.replace(
        tzinfo=None
    )


@pytest.mark.asyncio
async def test_refreshed_output_rejects_lost_update(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    connection = _Connection(0)
    etag = '"stable"'
    validator = importer.BulkExportOutputValidator(
        20,
        etag,
        hashlib.sha256(etag.encode()).hexdigest(),
        None,
    )
    with pytest.raises(RuntimeError, match="validator_mismatch"):
        await importer._persist_refreshed_bulk_output(
            connection,
            _refresh_identity(),
            _refresh_manifest().outputs[0],
            _refresh_output_checkpoint(),
            validator,
        )


@pytest.mark.asyncio
async def test_refreshed_manifest_rejects_lost_owner(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    with pytest.raises(RuntimeError, match="ownership_lost"):
        await importer._persist_refreshed_bulk_manifest(
            _Connection(0),
            _refresh_identity(),
            _refresh_checkpoint(),
            _refresh_manifest(),
            "{}",
        )


@pytest.mark.asyncio
async def test_refresh_error_records_derived_terminal_state(monkeypatch):
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )
    result = await importer._record_bulk_capability_refresh_error(
        _refresh_identity(),
        "bulk_export_output_validator_mismatch",
        3,
    )
    assert result == (None, "bulk_export_output_validator_mismatch", 3)
    assert record_error.await_args.kwargs["terminal"] is True


@pytest.mark.asyncio
async def test_prepare_refresh_requires_verified_lineage():
    with pytest.raises(ValueError, match="lineage_unverified"):
        await importer._prepare_bulk_capability_refresh(
            _identity(),
            _refresh_checkpoint(_identity()),
        )


@pytest.mark.asyncio
async def test_validate_refresh_propagates_probe_error(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_probe_bulk_capability_refresh_validators",
        AsyncMock(return_value=([], "bulk_export_output_http_500")),
    )
    persist_refresh = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_persist_bulk_capability_refresh",
        persist_refresh,
    )

    error = await importer._validate_and_persist_bulk_capabilities(
        object(),
        _source(),
        _refresh_identity(),
        _refresh_checkpoint(),
        _refresh_manifest(),
        [_refresh_output_checkpoint()],
        _fetch_options(),
        AsyncMock(),
    )

    assert error == "bulk_export_output_http_500"
    persist_refresh.assert_not_awaited()
