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
    ("line", "parse_result", "expected_error"),
    [
        (b"not-json\n", None, "invalid_ndjson"),
        (b"[]\n", None, "invalid_ndjson"),
        (b'{"resourceType":"Location","id":"one"}\n', None, "bulk_export_output_resource_type_mismatch"),
        (b'{"resourceType":"Practitioner","id":"one"}\n', None, None),
        (
            b'{"resourceType":"Practitioner","id":"one"}\n',
            (ProviderDirectoryLocation, {}),
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_bulk_stream_rejects_or_skips_malformed_rows(
    monkeypatch,
    line,
    parse_result,
    expected_error,
):
    if parse_result is not None or line.startswith(b'{"resourceType":"Practitioner"'):
        monkeypatch.setattr(
            importer,
            "parse_fhir_resource",
            lambda *_args, **_kwargs: parse_result,
        )
    stream_result = await importer._stream_bulk_export_output_rows(
        _Session(_Response(chunks=[line])),
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
    assert stream_result[-1] == expected_error


@pytest.mark.asyncio
async def test_bulk_stream_flushes_rows_and_honors_row_limit(monkeypatch):
    line = b'{"resourceType":"Practitioner","id":"one"}\n'
    practitioner_row_map = {
        "source_id": "aetna-coverage",
        "resource_id": "one",
    }
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (
            ProviderDirectoryPractitioner,
            practitioner_row_map,
        ),
    )
    write_batch = AsyncMock(return_value=1)
    progress = AsyncMock()
    stream_result = await importer._stream_bulk_export_output_rows(
        _Session(_Response(chunks=[b"\n", line])),
        _source(),
        "https://storage.googleapis.com/aetna/part.ndjson",
        model=ProviderDirectoryPractitioner,
        resource_type="Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=write_batch,
        row_batch_size=1,
        retain_rows=True,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=progress,
            resume_offset=0,
            expected_etag=None,
            expected_content_length=None,
        ),
        requires_access_token=False,
    )
    assert stream_result == ([practitioner_row_map], 1, 1, True, None)
    write_batch.assert_awaited_once()
    progress.assert_awaited_once()


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            RuntimeError(importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED),
            importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED,
        ),
        (RuntimeError("boom"), "bulk_export_transport_runtimeerror"),
        (OSError("bulk_export_non_public_dns_address"), "bulk_export_non_public_dns_address"),
        (OSError("socket"), "bulk_export_transport_oserror"),
        (Exception("bulk_export_non_public_dns_address nested"), "bulk_export_non_public_dns_address"),
        (Exception("boom"), "bulk_export_transport_exception"),
    ],
)
@pytest.mark.asyncio
async def test_bulk_stream_sanitizes_request_failures(error, expected):
    result = await importer._stream_bulk_export_output_rows(
        _Session(error),
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
    assert result[-1] == expected

@pytest.mark.asyncio
async def test_legacy_bulk_fetch_handles_ready_outputs_and_limit(monkeypatch):
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(200, {}, _manifest_payload(), None)),
    )
    stream_output = AsyncMock(
        side_effect=[
            ([{"resource_id": "one"}], 1, 1, False, None),
            ([{"resource_id": "two"}], 1, 1, True, None),
        ]
    )
    monkeypatch.setattr(
        importer,
        "_stream_bulk_export_output_rows",
        stream_output,
    )
    manifest_payload = _manifest_payload(
        output=[
            {
                "type": "Practitioner",
                "url": f"https://storage.googleapis.com/aetna/{index}.ndjson",
            }
            for index in range(2)
        ]
    )
    importer._bulk_http_get_json.return_value = (200, {}, manifest_payload, None)
    fetch_result = await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=2,
        timeout=3,
        run_id="run-coverage",
    )
    assert fetch_result is not None
    assert fetch_result.rows_fetched == 2
    assert fetch_result.rows_written == 2
    assert fetch_result.row_limit_reached is True
    assert fetch_result.complete is False
    assert stream_output.await_count == 2


@pytest.mark.parametrize(
    ("start_response", "poll_result", "expected"),
    [
        ((500, {}, None, None), None, None),
        ((200, {}, {"resourceType": "Bundle"}, None), None, None),
        (
            (200, {}, {"error": [{"responseCode": "410"}]}, None),
            None,
            "bulk_export_error_http_410",
        ),
        ((202, {}, None, None), None, "bulk_export_missing_status_url"),
        (
            (202, {"content-location": "/status/1"}, None, None),
            (None, "bulk_export_timeout", 3),
            "bulk_export_timeout",
        ),
    ],
)
@pytest.mark.asyncio
async def test_legacy_bulk_fetch_handles_start_and_poll_failures(
    monkeypatch,
    start_response,
    poll_result,
    expected,
):
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=start_response),
    )
    if poll_result is not None:
        monkeypatch.setattr(
            importer,
            "_bulk_export_poll_outputs",
            AsyncMock(return_value=poll_result),
        )
    result = await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    )
    assert (result.error if result is not None else None) == expected


@pytest.mark.asyncio
async def test_legacy_bulk_fetch_drops_zero_progress_output_error(monkeypatch):
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(200, {}, _manifest_payload(), None)),
    )
    monkeypatch.setattr(
        importer,
        "_stream_bulk_export_output_rows",
        AsyncMock(return_value=([], 0, 0, False, "network")),
    )
    assert await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
    ) is None


@pytest.mark.parametrize(
    ("configured", "expected"),
    [
        (None, ()),
        (" old, ,new ", ("old", "new")),
        ('["old", "", "new"]', ("old", "new")),
    ],
)
def test_bulk_checkpoint_previous_keyring_formats(monkeypatch, configured, expected):
    if configured is None:
        monkeypatch.delenv(
            "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
            raising=False,
        )
    else:
        monkeypatch.setenv(
            "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
            configured,
        )
    assert importer._bulk_checkpoint_previous_secrets() == expected


def test_bulk_checkpoint_previous_keyring_rejects_invalid_json(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_PREVIOUS_KEYS",
        "[invalid",
    )
    with pytest.raises(RuntimeError, match="keyring_invalid"):
        importer._bulk_checkpoint_previous_secrets()
