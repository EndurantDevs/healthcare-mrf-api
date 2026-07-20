# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryLocation, ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")
TEST_TRANSACTION_TIME = (
    datetime.datetime.now(datetime.UTC)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)


def _source() -> dict:
    return {
        "source_id": "aetna-coverage",
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "canonical_api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "metadata_json": {
            "provider_directory_bulk_export_output_hosts": [
                "storage.googleapis.com"
            ]
        },
    }


def _identity(
    *, owner_run_id: str = "run-coverage", retry_of_run_id: str | None = None
) -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-coverage",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-coverage",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-coverage",
        owner_run_id=owner_run_id,
        retry_of_run_id=retry_of_run_id,
        endpoint_id="endpoint-coverage",
        dataset_id="dataset-coverage",
        start_url=(
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
            "/$export?_type=Practitioner"
        ),
        start_url_hash="a" * 64,
    )


def _checkpoint(identity: importer.BulkExportCheckpointIdentity) -> dict:
    return {
        "checkpoint_id": identity.checkpoint_id,
        "canonical_api_base": identity.canonical_api_base,
        "resource_type": identity.resource_type,
        "source_scope_hash": identity.source_scope_hash,
        "strategy_version": identity.strategy_version,
        "acquisition_root_run_id": identity.acquisition_root_run_id,
        "owner_run_id": identity.owner_run_id,
        "retry_of_run_id": identity.retry_of_run_id,
        "endpoint_id": identity.endpoint_id,
        "dataset_id": identity.dataset_id,
        "start_url_hash": identity.start_url_hash,
        "status_url_ciphertext": None,
        "status_url_hash": None,
        "state": importer.BULK_EXPORT_CHECKPOINT_ACCEPTED,
    }


def _manifest_payload(**overrides) -> dict:
    payload = {
        "transactionTime": TEST_TRANSACTION_TIME,
        "request": _identity().start_url,
        "requiresAccessToken": False,
        "output": [
            {
                "type": "Practitioner",
                "url": "https://storage.googleapis.com/aetna/part.ndjson?sig=x",
            }
        ],
    }
    payload.update(overrides)
    return payload


class _ChunkedContent:
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks

    async def iter_chunked(self, _size: int):
        for chunk in self.chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
        body: bytes = b"{}",
    ):
        self.status = status
        self.headers = headers or {}
        self.content = _ChunkedContent(chunks or [])
        self.body = body

    async def read(self) -> bytes:
        return self.body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _Session:
    def __init__(self, response_or_error):
        self.response_or_error = response_or_error
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **options):
        self.calls.append((url, options))
        if isinstance(self.response_or_error, BaseException):
            raise self.response_or_error
        return self.response_or_error


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
    payload, error, polls = await importer._bulk_export_poll_manifest(
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
    assert payload == _manifest_payload()
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
    result = await importer._stream_bulk_export_output_rows(
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
    assert result[-1] == expected_error


@pytest.mark.asyncio
async def test_bulk_stream_flushes_rows_and_honors_row_limit(monkeypatch):
    line = b'{"resourceType":"Practitioner","id":"one"}\n'
    row = {"source_id": "aetna-coverage", "resource_id": "one"}
    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (ProviderDirectoryPractitioner, row),
    )
    write_batch = AsyncMock(return_value=1)
    progress = AsyncMock()
    result = await importer._stream_bulk_export_output_rows(
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
    assert result == ([row], 1, 1, True, None)
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


@contextlib.asynccontextmanager
async def _client_session():
    yield object()


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
    payload = _manifest_payload(
        output=[
            {
                "type": "Practitioner",
                "url": f"https://storage.googleapis.com/aetna/{index}.ndjson",
            }
            for index in range(2)
        ]
    )
    importer._bulk_http_get_json.return_value = (200, {}, payload, None)
    result = await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=2,
        timeout=3,
        run_id="run-coverage",
    )
    assert result is not None
    assert result.rows_fetched == 2
    assert result.rows_written == 2
    assert result.row_limit_reached is True
    assert result.complete is False
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


@pytest.mark.asyncio
async def test_load_bulk_checkpoint_handles_empty_identity_and_status_errors(
    monkeypatch,
):
    identity = _identity()
    first = AsyncMock(
        side_effect=[
            None,
            {**_checkpoint(identity), "dataset_id": "wrong"},
            {**_checkpoint(identity), "status_url_hash": "orphaned"},
            _checkpoint(identity),
        ]
    )
    monkeypatch.setattr(importer.db, "first", first)

    assert await importer._load_bulk_export_checkpoint(identity) == {}
    with pytest.raises(RuntimeError, match="identity_mismatch_dataset_id"):
        await importer._load_bulk_export_checkpoint(identity)
    with pytest.raises(RuntimeError, match="status_url_hash_mismatch"):
        await importer._load_bulk_export_checkpoint(identity)
    assert (await importer._load_bulk_export_checkpoint(identity))["dataset_id"] == (
        identity.dataset_id
    )


@pytest.mark.asyncio
async def test_adopt_bulk_checkpoint_enforces_retry_lineage_and_rowcount(
    monkeypatch,
):
    with pytest.raises(RuntimeError, match="lineage_mismatch"):
        await importer._adopt_bulk_export_checkpoint(_identity())

    retry_identity = _identity(
        owner_run_id="run-coverage-retry",
        retry_of_run_id="run-coverage",
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))
    with pytest.raises(RuntimeError, match="ownership_conflict"):
        await importer._adopt_bulk_export_checkpoint(retry_identity)

    importer.db.status.return_value = 1
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(side_effect=[{}, _checkpoint(retry_identity)]),
    )
    with pytest.raises(RuntimeError, match="adoption_lost"):
        await importer._adopt_bulk_export_checkpoint(retry_identity)
    adopted = await importer._adopt_bulk_export_checkpoint(retry_identity)
    assert adopted["owner_run_id"] == retry_identity.owner_run_id


@pytest.mark.asyncio
async def test_reserve_bulk_checkpoint_reports_lost_and_owned_rows(monkeypatch):
    identity = _identity()
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(side_effect=[None, {"checkpoint_id": identity.checkpoint_id}]),
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(side_effect=[{}, _checkpoint(identity)]),
    )
    with pytest.raises(RuntimeError, match="reservation_lost"):
        await importer._reserve_bulk_export_checkpoint(identity)
    checkpoint, is_owner = await importer._reserve_bulk_export_checkpoint(identity)
    assert checkpoint["checkpoint_id"] == identity.checkpoint_id
    assert is_owner is True


@pytest.mark.asyncio
async def test_accept_bulk_checkpoint_fails_closed_on_owner_and_url_mismatch(
    monkeypatch,
):
    identity = _identity()
    status_url = f"{identity.canonical_api_base}/status/1"
    monkeypatch.setattr(
        importer,
        "_insert_accepted_bulk_checkpoint",
        AsyncMock(),
    )
    monkeypatch.setattr(importer, "_mark_bulk_checkpoint_accepted", AsyncMock())
    record_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_error,
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(
            side_effect=[
                {**_checkpoint(identity), "owner_run_id": "other"},
                _checkpoint(identity),
                _checkpoint(identity),
            ]
        ),
    )
    with pytest.raises(RuntimeError, match="ownership_lost"):
        await importer._accept_bulk_export_checkpoint(identity, status_url)
    with pytest.raises(RuntimeError, match="status_url_mismatch"):
        await importer._accept_bulk_export_checkpoint(identity, status_url)
    record_error.assert_awaited_once_with(
        identity,
        "bulk_export_status_url_mismatch",
        terminal=True,
    )
    assert await importer._accept_bulk_export_checkpoint(identity, None) == (
        _checkpoint(identity)
    )


class _Connection:
    def __init__(self, status_result: int):
        self.status = AsyncMock(return_value=status_result)


@contextlib.asynccontextmanager
async def _acquire(connection):
    yield connection


@pytest.mark.asyncio
async def test_persist_bulk_manifest_is_transactional_and_reloads_identity(
    monkeypatch,
):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
        expected_request_url=identity.start_url,
    )
    connection = _Connection(1)
    monkeypatch.setattr(importer.db, "acquire", lambda: _acquire(connection))
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(
            side_effect=[
                {},
                {**_checkpoint(identity), "manifest_hash": manifest.identity_hash},
            ]
        ),
    )
    checkpoint = await importer._persist_bulk_export_manifest(identity, manifest)
    assert checkpoint["manifest_hash"] == manifest.identity_hash
    assert connection.status.await_count == 2
    assert "manifest_audit_json" in connection.status.await_args_list[0].kwargs
    assert "output_url_ciphertext" in connection.status.await_args_list[1].kwargs


@pytest.mark.asyncio
async def test_persist_bulk_manifest_rejects_lost_owner(monkeypatch):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-coverage-key",
    )
    identity = _identity()
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(),
        "Practitioner",
    )
    connection = _Connection(0)
    monkeypatch.setattr(importer.db, "acquire", lambda: _acquire(connection))
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={}),
    )
    with pytest.raises(ValueError, match="manifest_mismatch"):
        await importer._persist_bulk_export_manifest(identity, manifest)


@pytest.mark.asyncio
async def test_claim_existing_checkpoint_covers_lineage_and_adoption_errors(
    monkeypatch,
):
    identity = _identity()
    checkpoint = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
    }
    cancel_probe = AsyncMock()
    claimed, payload, error = await importer._claim_existing_bulk_export_checkpoint(
        identity,
        checkpoint,
        ownership_probe=AsyncMock(),
        cancel_probe=cancel_probe,
    )
    assert claimed == checkpoint and payload is None
    assert error == "bulk_export_checkpoint_lineage_mismatch"
    cancel_probe.assert_awaited_once()

    retry_identity = dataclasses.replace(
        identity,
        owner_run_id="retry-run",
        retry_of_run_id="prior-run",
    )
    monkeypatch.setattr(
        importer,
        "_adopt_bulk_export_checkpoint",
        AsyncMock(side_effect=RuntimeError("bulk_export_checkpoint_adoption_lost")),
    )
    claimed, payload, error = await importer._claim_existing_bulk_export_checkpoint(
        retry_identity,
        checkpoint,
        ownership_probe=AsyncMock(),
    )
    assert claimed == checkpoint and payload is None
    assert error == "bulk_export_checkpoint_adoption_lost"


@pytest.mark.asyncio
async def test_claim_existing_checkpoint_times_out_or_observes_deleted_row(
    monkeypatch,
):
    identity = _identity(
        owner_run_id="retry-run",
        retry_of_run_id="prior-run",
    )
    checkpoint = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_STARTING,
    }
    conflict = AsyncMock(
        side_effect=RuntimeError("bulk_export_checkpoint_ownership_conflict")
    )
    monkeypatch.setattr(importer, "_adopt_bulk_export_checkpoint", conflict)
    monkeypatch.setattr(importer, "_bulk_checkpoint_lease_seconds", lambda: 0)
    claimed, payload, error = await importer._claim_existing_bulk_export_checkpoint(
        identity,
        checkpoint,
        ownership_probe=AsyncMock(),
    )
    assert claimed == checkpoint and payload is None
    assert error == "bulk_export_acceptance_outcome_unknown"

    monkeypatch.setattr(
        importer,
        "_bulk_checkpoint_lease_seconds",
        lambda: 1_000_000_000,
    )
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={}),
    )
    claimed, payload, error = await importer._claim_existing_bulk_export_checkpoint(
        identity,
        {**checkpoint, "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE},
        ownership_probe=AsyncMock(),
    )
    assert claimed == {} and payload is None
    assert error == "bulk_export_checkpoint_adoption_lost"


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
    base = {
        "content_length_bytes": 100,
        "etag_ciphertext": importer._encrypt_bulk_capability(etag),
        "etag_hash": hashlib.sha256(etag.encode()).hexdigest(),
        "committed_bytes": 10,
        "validator_checked_at": datetime.datetime(2026, 7, 20, 12),
        "output_expires_at": None,
    }
    with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
        importer._stored_bulk_output_validator(
            {**base, "committed_bytes": 101}
        )
    with pytest.raises(ValueError, match="validator_checkpoint_corrupt"):
        importer._stored_bulk_output_validator({**base, "etag_hash": "bad"})


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


def _stream_options(*, range_resume_enabled: bool) -> importer.BulkExportStreamOptions:
    return importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=range_resume_enabled,
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
    result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )
    assert result.error == "checkpoint-invalid"

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
    result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )
    assert result.error == "validator-invalid"


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
    result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=False),
    )
    assert result.error == "output-error"
    prepare.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_fetch_dispatches_checkpoint_and_empty_legacy_paths(monkeypatch):
    checkpoint_context = importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope-coverage",
        source_ids=("aetna-coverage",),
        owner_run_id="run-coverage",
        acquisition_root_run_id="root-coverage",
        endpoint_id="endpoint-coverage",
        dataset_id="dataset-coverage",
    )
    source = {**_source(), "_pagination_checkpoint_context": checkpoint_context}
    expected = object()
    checkpointed_fetch = AsyncMock(return_value=expected)
    monkeypatch.setattr(
        importer,
        "_fetch_checkpointed_bulk_export_resource_rows",
        checkpointed_fetch,
    )
    assert await importer._fetch_bulk_export_resource_rows(
        source,
        "Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
    ) is expected

    assert await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Patient",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    ) is None

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
    empty = await importer._fetch_bulk_export_resource_rows(
        _source(),
        "Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    )
    assert empty is not None and empty.complete is True
    assert empty.rows_fetched == 0


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
    checkpoint = {
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
        AsyncMock(return_value=[checkpoint]),
    )

    checkpoints, error = await importer._prepare_bulk_output_validators(
        object(),
        _source(),
        identity,
        manifest,
        [checkpoint],
        timeout=3,
        ownership_probe=AsyncMock(),
    )

    assert checkpoints == [checkpoint]
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
    payload = _manifest_payload()

    result = await importer._checkpointed_bulk_export_manifest(
        object(),
        _source(),
        identity,
        _checkpoint(identity),
        initial_status_payload=payload,
        timeout=3,
        max_pending_seconds=30,
        runtime_probe=runtime_probe,
    )

    assert result == (manifest, None, 0)
    poll.assert_not_awaited()
    persist_ready.assert_awaited_once_with(
        _source(),
        identity,
        payload,
        polls=0,
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

    result = await importer._stream_checkpointed_bulk_outputs(
        object(),
        _source(),
        identity,
        manifest,
        _stream_options(range_resume_enabled=True),
    )

    assert result.error == "stream-error"


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


def _checkpoint_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope-coverage",
        source_ids=("aetna-coverage",),
        owner_run_id="run-coverage",
        acquisition_root_run_id="root-coverage",
        endpoint_id="endpoint-coverage",
        dataset_id="dataset-coverage",
    )


def _fetch_options() -> importer.BulkExportFetchOptions:
    return importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
    )


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

    result = await importer._fetch_owned_checkpointed_bulk_resource_rows(
        _source(),
        identity,
        ProviderDirectoryPractitioner,
        _fetch_options(),
        AsyncMock(),
    )
    assert result is expected

    with pytest.raises(RuntimeError, match="stream_options_unavailable"):
        await importer._fetch_owned_checkpointed_bulk_resource_rows(
            _source(),
            identity,
            ProviderDirectoryPractitioner,
            _fetch_options(),
            AsyncMock(),
        )


def test_bulk_host_normalization_rejects_empty_and_non_https_values():
    assert importer._pagination_host_key(None) is None
    assert importer._pagination_host_key("http://example.test") is None
    source = {
        **_source(),
        "metadata_json": {
            **_source()["metadata_json"],
            "provider_directory_pagination_allowed_hosts": ["extra.example"],
        },
    }
    assert "extra.example:443" in importer._pagination_allowed_hosts(
        source,
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
    assert importer._bulk_export_enabled(None) is True
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

    result = await importer._stream_bulk_export_output_rows(
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

    assert result[-1] is None
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
    checkpoint = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
    }
    adopted = {**checkpoint, "owner_run_id": identity.owner_run_id}
    monkeypatch.setattr(
        importer,
        "_adopt_bulk_export_checkpoint",
        AsyncMock(
            side_effect=[
                RuntimeError("bulk_export_checkpoint_ownership_conflict"),
                adopted,
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
        AsyncMock(return_value=checkpoint),
    )

    claimed, payload, error = await importer._claim_existing_bulk_export_checkpoint(
        identity,
        checkpoint,
        ownership_probe=AsyncMock(),
    )

    assert claimed == adopted
    assert payload is None
    assert error is None


@pytest.mark.asyncio
async def test_legacy_bulk_poll_success_can_return_empty_complete_result(monkeypatch):
    source = _source()
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(
            return_value=(
                202,
                {"content-location": f"{source['api_base']}/status/one"},
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

    result = await importer._fetch_bulk_export_resource_rows(
        source,
        "Practitioner",
        per_resource_limit=1,
        timeout=3,
        run_id="run-coverage",
    )

    assert result is not None
    assert result.complete is True
    assert result.pages_fetched == 1
