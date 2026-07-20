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
async def test_bulk_stream_response_and_length_failures(monkeypatch):
    response_error = await importer._stream_bulk_export_output_rows(
        _Session(_Response(status=206)),
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
        requires_access_token=False,
    )
    assert response_error[-1] == "bulk_export_output_range_response_invalid"

    line = b'{"resourceType":"Practitioner","id":"one"}'
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (ProviderDirectoryPractitioner, {}),
    )
    length_error = await importer._stream_bulk_export_output_rows(
        _Session(
            _Response(
                headers={"etag": '"one"', "content-length": "100"},
                chunks=[line],
            )
        ),
        _source(),
        "https://storage.googleapis.com/aetna/part.ndjson",
        model=ProviderDirectoryPractitioner,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=1),
        row_batch_size=2,
        retain_rows=False,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=AsyncMock(),
            resume_offset=0,
            expected_etag='"one"',
            expected_content_length=100,
        ),
        requires_access_token=False,
    )
    assert length_error[-1] == "bulk_export_output_length_mismatch"


@pytest.mark.parametrize(
    ("headers", "offset", "expected"),
    [
        (
            {"etag": '"one"', "content-length": "99"},
            0,
            "bulk_export_output_length_mismatch",
        ),
        (
            {
                "etag": '"one"',
                "content-length": "50",
                "content-range": "invalid",
            },
            50,
            "bulk_export_output_content_range_invalid",
        ),
        (
            {"etag": '"one"', "content-length": "invalid"},
            0,
            "bulk_export_output_content_length_invalid",
        ),
    ],
)
def test_bulk_output_response_rejects_length_and_range_variants(
    headers,
    offset,
    expected,
):
    assert importer._bulk_output_response_error(
        206 if offset else 200,
        headers,
        resume_offset=offset,
        expected_etag='"one"',
        expected_content_length=100,
    ) == expected


@pytest.mark.asyncio
async def test_bulk_cancel_probe_checks_control_and_deadline(monkeypatch):
    cancel_check = AsyncMock()
    monkeypatch.setattr(importer, "raise_if_cancelled", cancel_check)
    monkeypatch.setattr(importer.time, "monotonic", lambda: 10)
    await importer._bulk_cancel_probe({}, {}, None)
    cancel_check.assert_awaited_once()
    with pytest.raises(RuntimeError, match=importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED):
        await importer._bulk_cancel_probe(None, None, 9)


def test_bulk_manifest_rejects_request_mismatch_and_missing_resource():
    with pytest.raises(ValueError, match="request_mismatch"):
        importer._bulk_export_manifest_from_payload(
            _manifest_payload(),
            "Practitioner",
            expected_request_url=(
                f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
                "/$export?_type=Location"
            ),
        )
    with pytest.raises(ValueError, match="missing_resource_output"):
        importer._bulk_export_manifest_from_payload(
            _manifest_payload(),
            "Location",
        )


@pytest.mark.asyncio
async def test_bulk_sql_mutations_fail_closed_on_each_ownership_boundary(
    monkeypatch,
):
    identity = _identity()
    monkeypatch.setattr(
        importer,
        "_refresh_bulk_export_rows_written",
        AsyncMock(),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._begin_bulk_export_output(
            identity,
            "output-coverage",
            preserve_progress=True,
        )

    importer.db.status.side_effect = [1, 0]
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._begin_bulk_export_output(
            identity,
            "output-coverage",
            preserve_progress=False,
        )

    importer.db.status.side_effect = [1, 1]
    await importer._begin_bulk_export_output(
        identity,
        "output-coverage",
        preserve_progress=True,
    )
    importer._refresh_bulk_export_rows_written.assert_awaited_once_with(identity)


@pytest.mark.asyncio
async def test_bulk_progress_and_capability_mutations_check_rowcounts(monkeypatch):
    identity = _identity()
    real_refresh_rows_written = importer._refresh_bulk_export_rows_written
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    monkeypatch.setattr(
        importer,
        "_refresh_bulk_export_rows_written",
        AsyncMock(),
    )
    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._record_bulk_export_output_progress(
            identity,
            "output-coverage",
            3,
            100,
        )
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await real_refresh_rows_written(identity)
    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._clear_bulk_output_capability(
            identity,
            "output-coverage",
        )
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._persist_bulk_export_next_poll_at(
            identity,
            datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC),
        )
    with pytest.raises(RuntimeError, match="output_ownership_lost"):
        await importer._complete_bulk_export_output(
            identity,
            "output-coverage",
            3,
            100,
            require_validator=True,
        )

    importer.db.status.return_value = 1
    await importer._record_bulk_export_output_progress(
        identity,
        "output-coverage",
        3,
        100,
    )
    await real_refresh_rows_written(identity)
    await importer._clear_bulk_output_capability(identity, "output-coverage")
    await importer._persist_bulk_export_next_poll_at(
        identity,
        datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC),
    )
    await importer._complete_bulk_export_output(
        identity,
        "output-coverage",
        3,
        100,
        require_validator=False,
    )


def test_stored_bulk_validator_rejects_bounds_and_etag_tampering(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    etag = '"etag"'
    validator_checkpoint_map = {
        "content_length_bytes": 100,
        "etag_ciphertext": importer._encrypt_bulk_capability(etag),
        "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
        "committed_bytes": 10,
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
        "output_expires_at": None,
    }
    with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
        importer._stored_bulk_output_validator(
            {**validator_checkpoint_map, "committed_bytes": 101}
        )
    with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
        importer._stored_bulk_output_validator(
            {**validator_checkpoint_map, "etag_hash": "bad"}
        )


def test_bulk_manifest_checkpoint_rejects_partial_and_tampered_state(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    payload = _manifest_payload()
    ciphertext = importer._encrypt_bulk_capability(
        json.dumps(payload, sort_keys=True)
    )
    with pytest.raises(ValueError, match="manifest_checkpoint_corrupt"):
        importer._bulk_manifest_from_checkpoint(
            {"manifest_hash": "hash", "manifest_ciphertext": None},
            "Practitioner",
            _identity().start_url,
        )
    with pytest.raises(ValueError, match="manifest_checkpoint_corrupt"):
        importer._bulk_manifest_from_checkpoint(
            {"manifest_hash": "bad", "manifest_ciphertext": ciphertext},
            "Practitioner",
            _identity().start_url,
        )


@pytest.mark.asyncio
async def test_terminal_status_hash_without_capability_is_valid():
    assert importer._bulk_checkpoint_status_url_error(
        {
            "state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE,
            "status_url_ciphertext": None,
            "status_url_hash": "audit-only",
        }
    ) is None
    assert importer._is_bulk_export_error_terminal(None) is False
    with pytest.raises(ValueError, match="terminal_checkpoint_state_invalid"):
        # Invalid states must never enter the owner-independent repair SQL.
        await importer._repair_terminal_bulk_export_checkpoint(
            _identity(),
            "streaming",
        )

@pytest.mark.asyncio
async def test_stream_checkpointed_outputs_short_circuits_validation_errors(
    monkeypatch,
):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    output_checkpoints = [{"state": importer.BULK_EXPORT_OUTPUT_PENDING}]
    monkeypatch.setattr(
        importer,
        "_validated_bulk_output_checkpoints",
        AsyncMock(return_value=(output_checkpoints, "checkpoint-invalid")),
    )
    checkpoint_error_result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )
    assert checkpoint_error_result.error == "checkpoint-invalid"

    monkeypatch.setattr(
        importer,
        "_validated_bulk_output_checkpoints",
        AsyncMock(return_value=(output_checkpoints, None)),
    )
    monkeypatch.setattr(
        importer,
        "_prepare_bulk_output_validators",
        AsyncMock(return_value=(output_checkpoints, "validator-invalid")),
    )
    validator_error_result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )
    assert validator_error_result.error == "validator-invalid"


@pytest.mark.asyncio
async def test_stream_checkpointed_outputs_skips_validator_when_disabled(
    monkeypatch,
):
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    output_checkpoints = [{"state": importer.BULK_EXPORT_OUTPUT_PENDING}]
    monkeypatch.setattr(
        importer,
        "_validated_bulk_output_checkpoints",
        AsyncMock(return_value=(output_checkpoints, None)),
    )
    prepare = AsyncMock()
    monkeypatch.setattr(importer, "_prepare_bulk_output_validators", prepare)
    monkeypatch.setattr(
        importer,
        "_resume_checkpointed_bulk_outputs",
        AsyncMock(
            return_value=(
                importer.BulkExportStreamState(),
                "output-error",
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_output_checkpoints",
        AsyncMock(return_value=output_checkpoints),
    )
    output_error_result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=False),
    )
    assert output_error_result.error == "output-error"
    prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_fetch_dispatches_durable_checkpoint_path(monkeypatch):
    source_map = {
        **_source(),
        "_pagination_checkpoint_context": _checkpoint_context(),
    }
    expected_result = object()
    checkpointed_fetch = AsyncMock(return_value=expected_result)
    monkeypatch.setattr(
        importer,
        "_fetch_checkpointed_bulk_export_resource_rows",
        checkpointed_fetch,
    )
    assert await importer._fetch_bulk_export_resource_rows(
        source_map,
        "Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
    ) is expected_result


@pytest.mark.asyncio
async def test_bulk_fetch_rejects_unknown_legacy_resource():
    assert await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Patient",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    ) is None


@pytest.mark.asyncio
async def test_bulk_fetch_returns_complete_for_empty_legacy_manifest(monkeypatch):
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            return_value=(
                200,
                {},
                _manifest_payload(
                    output=[
                        {
                            "type": "Location",
                            "url": "https://storage.googleapis.com/aetna/location.ndjson",
                        }
                    ]
                ),
                None,
            )
        ),
    )
    empty_fetch_result = await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    )
    assert empty_fetch_result is not None
    assert empty_fetch_result.complete is True
    assert empty_fetch_result.rows_fetched == 0
