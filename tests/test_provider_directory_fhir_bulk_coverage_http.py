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
    ("value", "expected"),
    [(None, None), ("", None), ("-2", 0), ("90", 60), ("invalid", None)],
)
def test_retry_after_seconds_is_bounded(value, expected):
    assert importer._retry_after_seconds(value) == expected


def test_bulk_payload_helpers_cover_malformed_and_typed_errors():
    assert importer._bulk_export_output_urls(None, "Practitioner") == []
    assert importer._bulk_export_output_urls(
        {"output": [None, {"type": "Practitioner"}]},
        "Practitioner",
    ) == []
    assert importer._bulk_export_status_payload(None) is False
    assert importer._bulk_export_payload_error(None) == (
        "bulk_export_invalid_status_payload"
    )
    assert importer._bulk_export_payload_error({"error": ["bad"]}) == (
        "bulk_export_error_output"
    )
    assert importer._bulk_export_payload_error(
        {"error": [{"type": "OperationOutcome"}]}
    ) == "bulk_export_error_OperationOutcome"
    assert importer._bulk_export_payload_error({"error": [{}]}) == (
        "bulk_export_error_output"
    )
    assert importer._bulk_json_headers(prefer_async=True)["Prefer"] == (
        "respond-async"
    )
    assert importer._bulk_export_log_url(None) is None
    assert importer._bulk_export_log_url("relative/path") == "relative/path"


def test_bulk_retry_after_supports_http_dates_and_invalid_values():
    current = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    assert importer._bulk_export_retry_after_seconds(None, now_utc=current) is None
    assert importer._bulk_export_retry_after_seconds("-5", now_utc=current) == 0
    assert importer._bulk_export_retry_after_seconds(
        "Sun, 20 Jul 2026 12:01:00 GMT",
        now_utc=current,
    ) == 60
    assert importer._bulk_export_retry_after_seconds(
        "not-a-date",
        now_utc=current,
    ) is None


@pytest.mark.parametrize(
    "headers",
    [
        {"content-length": "2", "content-range": "bytes 0-0/2"},
        {"content-length": "1", "content-range": "bytes invalid"},
        {"content-length": "1", "content-range": "bytes 0-0/0"},
    ],
)
def test_bulk_probe_validator_rejects_invalid_single_byte_proof(headers):
    headers["etag"] = '"etag"'
    with pytest.raises(ValueError, match="content_range_invalid"):
        importer._bulk_output_validator_from_probe(
            "https://storage.googleapis.com/aetna/part.ndjson",
            headers,
        )


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ValueError("bad request"), "bad request"),
        (OSError("bulk_export_non_public_dns_address"), "bulk_export_non_public_dns_address"),
        (OSError("socket failed"), "bulk_export_transport_oserror"),
        (RuntimeError("bulk_export_non_public_dns_address via connector"), "bulk_export_non_public_dns_address"),
        (RuntimeError("connector failed"), "bulk_export_transport_runtimeerror"),
    ],
)
@pytest.mark.asyncio
async def test_bulk_http_get_json_sanitizes_transport_errors(
    monkeypatch,
    error,
    expected,
):
    monkeypatch.setattr(
        importer,
        "_bulk_export_source_request_options",
        lambda *_args: {"headers": {}, "query_params": {}},
    )
    status, headers, payload, observed = await importer._bulk_http_get_json(
        _Session(error),
        _source(),
        _identity().start_url,
        timeout=3,
    )
    assert (status, headers, payload, observed) == (None, {}, None, expected)


@pytest.mark.asyncio
async def test_bulk_http_get_json_reads_success_and_async_header(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_bulk_export_source_request_options",
        lambda *_args: {"headers": {"X-Test": "yes"}, "query_params": {}},
    )
    session = _Session(
        _Response(
            status=202,
            headers={"Content-Location": "/status/1"},
            body=b'{"output": []}',
        )
    )
    result = await importer._bulk_http_get_json(
        session,
        _source(),
        _identity().start_url,
        timeout=3,
        prefer_async=True,
    )
    assert result == (
        202,
        {"content-location": "/status/1"},
        {"output": []},
        None,
    )
    assert session.calls[0][1]["headers"]["Prefer"] == "respond-async"


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ([(None, {}, None, "network")], (None, "network", 1)),
        ([(500, {}, None, None)], (None, "bulk_export_status_http_500", 1)),
        (
            [
                (202, {"retry-after": "1"}, None, None),
                (200, {}, _manifest_payload(), None),
            ],
            (
                ["https://storage.googleapis.com/aetna/part.ndjson?sig=x"],
                None,
                2,
            ),
        ),
    ],
)
@pytest.mark.asyncio
async def test_bulk_export_poll_outputs_handles_terminal_paths(
    monkeypatch,
    responses,
    expected,
):
    monkeypatch.setattr(
        importer,
        "_bulk_export_poll_settings",
        lambda: (len(responses), 0),
    )
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(side_effect=responses),
    )
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    cancel_probe = AsyncMock()
    result = await importer._bulk_export_poll_outputs(
        object(),
        _source(),
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1",
        resource_type="Practitioner",
        timeout=3,
        cancel_probe=cancel_probe,
    )
    assert result == expected
    assert cancel_probe.await_count >= 1


@pytest.mark.asyncio
async def test_bulk_export_poll_outputs_times_out_after_pending_response(
    monkeypatch,
):
    monkeypatch.setattr(importer, "_bulk_export_poll_settings", lambda: (1, 0))
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(202, {}, None, None)),
    )
    assert await importer._bulk_export_poll_outputs(
        object(),
        _source(),
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1",
        resource_type="Practitioner",
        timeout=3,
    ) == (None, "bulk_export_timeout", 1)


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        ({"resourceType": "Bundle"}, "bulk_export_status_non_bulk_payload"),
        (
            {"error": [{"responseCode": "410"}]},
            "bulk_export_error_http_410",
        ),
    ],
)
def test_bulk_export_poll_ready_result_rejects_unusable_payloads(
    payload,
    expected_error,
):
    assert importer._bulk_export_poll_ready_result(
        _source(),
        "Practitioner",
        1,
        payload,
    ) == (None, expected_error)


@pytest.mark.asyncio
async def test_bulk_manifest_poll_runs_handlers_and_returns_ready(monkeypatch):
    observed_now = datetime.datetime(2026, 7, 20, 12, tzinfo=datetime.UTC)
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: observed_now)
    monkeypatch.setattr(importer, "_bulk_export_poll_settings", lambda: (2, 0))
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            side_effect=[
                (202, {"retry-after": "0"}, None, None),
                (200, {}, _manifest_payload(), None),
            ]
        ),
    )
    lease_handler = AsyncMock()
    next_poll_handler = AsyncMock()
    cancel_probe = AsyncMock()
    status_payload, error, polls = await importer._bulk_export_poll_manifest(
        object(),
        _source(),
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1",
        resource_type="Practitioner",
        timeout=3,
        poll_options=importer.BulkExportPollOptions(
            lease_handler=lease_handler,
            accepted_at=observed_now,
            max_pending_seconds=10,
            next_poll_handler=next_poll_handler,
            cancel_probe=cancel_probe,
        ),
    )
    assert status_payload == _manifest_payload()
    assert error is None and polls == 2
    assert lease_handler.await_count == 2
    assert next_poll_handler.await_count == 1
    assert cancel_probe.await_count == 4


@pytest.mark.asyncio
async def test_bulk_manifest_poll_rejects_bad_time_and_request_error(monkeypatch):
    result = await importer._bulk_export_poll_manifest(
        object(),
        _source(),
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1",
        resource_type="Practitioner",
        timeout=3,
        poll_options=importer.BulkExportPollOptions(accepted_at="invalid"),
    )
    assert result == (None, "bulk_export_checkpoint_accepted_at_invalid", 0)

    monkeypatch.setattr(importer, "_bulk_export_poll_settings", lambda: (1, 0))
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(None, {}, None, "network")),
    )
    assert await importer._bulk_export_poll_manifest(
        object(),
        _source(),
        f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}/status/1",
        resource_type="Practitioner",
        timeout=3,
    ) == (None, "network", 1)
