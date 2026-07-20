# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest
from db.models import ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")


def _aetna_source() -> dict:
    return importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows(
            source_query="Aetna"
        )[0]
    )


def _checkpoint_identity() -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-resume",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-resume",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-resume",
        owner_run_id="run-resume",
        retry_of_run_id=None,
        endpoint_id="endpoint-resume",
        dataset_id="dataset-resume",
        start_url=(
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
            "/$export?_type=Practitioner"
        ),
        start_url_hash="a" * 64,
    )


def _manifest(output_count: int) -> importer.BulkExportManifest:
    return importer._bulk_export_manifest_from_payload(
        {
            "transactionTime": "2026-07-20T00:00:00Z",
            "request": (
                f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
                "/$export?_type=Practitioner"
            ),
            "requiresAccessToken": False,
            "output": [
                {
                    "type": "Practitioner",
                    "url": (
                        "https://storage.googleapis.com/aetna/"
                        f"part-{output_index}.ndjson?sig=secret"
                    ),
                }
                for output_index in range(output_count)
            ],
        },
        "Practitioner",
    )


def _output_checkpoints(
    identity: importer.BulkExportCheckpointIdentity,
    manifest: importer.BulkExportManifest,
) -> list[dict]:
    return [
        {
            "output_id": importer._bulk_manifest_output_id(
                identity.checkpoint_id,
                output,
            ),
            "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        }
        for output in manifest.outputs
    ]


def _resume_stream_options(concurrency: int) -> importer.BulkExportStreamOptions:
    return importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-resume",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=True,
        output_concurrency=concurrency,
    )


class _ChunkedContent:
    def __init__(self, chunks: list[bytes]):
        self._chunks = chunks

    async def iter_chunked(self, _chunk_size: int):
        for chunk in self._chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        status: int,
        headers: dict[str, str],
        chunks: list[bytes],
    ):
        self.status = status
        self.headers = headers
        self.content = _ChunkedContent(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _Session:
    def __init__(self, response: _Response):
        self.response = response
        self.request_url = None
        self.request_options = None

    def get(self, request_url: str, **request_options):
        self.request_url = request_url
        self.request_options = request_options
        return self.response


def test_aetna_alone_enables_eight_way_range_resume(monkeypatch):
    source = _aetna_source()
    generic_source_map = {
        "metadata_json": {
            "provider_directory_bulk_export_output_concurrency": 8,
        }
    }

    assert importer._is_bulk_range_resume_enabled(source) is True
    assert importer._bulk_output_concurrency(source) == 8
    assert importer._is_bulk_range_resume_enabled(generic_source_map) is False
    assert importer._bulk_output_concurrency(generic_source_map) == 1

    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "8")
    with pytest.raises(
        ValueError,
        match="bulk_export_output_concurrency_pool_insufficient",
    ):
        importer._assert_bulk_output_pool_capacity(8)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "9")
    importer._assert_bulk_output_pool_capacity(8)


def test_bulk_output_validator_requires_immutable_byte_identity(monkeypatch):
    observed_at = datetime.datetime(2026, 7, 20, 12, 30, tzinfo=datetime.UTC)
    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: observed_at)
    output_url = (
        "https://storage.googleapis.com/aetna/export.ndjson"
        "?X-Goog-Date=20260720T120000Z&X-Goog-Expires=3600"
    )
    response_headers_by_name = {
        "accept-ranges": "bytes",
        "content-length": "1234",
        "etag": '"strong-etag"',
    }

    validator = importer._bulk_output_validator(
        output_url,
        response_headers_by_name,
    )

    assert validator.content_length_bytes == 1234
    assert validator.etag == '"strong-etag"'
    assert validator.etag_hash == hashlib.sha256(
        b'"strong-etag"'
    ).hexdigest()
    assert validator.output_expires_at == datetime.datetime(
        2026, 7, 20, 13, tzinfo=datetime.UTC
    )

    with pytest.raises(
        ValueError,
        match="bulk_export_output_strong_etag_required",
    ):
        importer._bulk_output_validator(
            output_url,
            {**response_headers_by_name, "etag": 'W/"weak-etag"'},
        )
    with pytest.raises(
        ValueError,
        match="bulk_export_output_byte_ranges_required",
    ):
        importer._bulk_output_validator(
            output_url,
            {
                header_name: header_value
                for header_name, header_value in response_headers_by_name.items()
                if header_name != "accept-ranges"
            },
        )
    with pytest.raises(
        ValueError,
        match="bulk_export_output_content_encoding_unsupported",
    ):
        importer._bulk_output_validator(
            output_url,
            {**response_headers_by_name, "content-encoding": "gzip"},
        )


@pytest.mark.parametrize(
    ("status", "resume_offset", "headers", "expected_error"),
    [
        (
            200,
            0,
            {"etag": '"v1"', "content-length": "100"},
            None,
        ),
        (
            206,
            40,
            {
                "etag": '"v1"',
                "content-length": "60",
                "content-range": "bytes 40-99/100",
            },
            None,
        ),
        (
            200,
            40,
            {"etag": '"v1"', "content-length": "100"},
            "bulk_export_output_range_response_invalid",
        ),
        (
            206,
            40,
            {
                "etag": '"v1"',
                "content-length": "60",
                "content-range": "bytes 41-99/100",
            },
            "bulk_export_output_content_range_invalid",
        ),
        (
            206,
            40,
            {
                "etag": '"changed"',
                "content-length": "60",
                "content-range": "bytes 40-99/100",
            },
            "bulk_export_output_etag_mismatch",
        ),
        (
            412,
            40,
            {"etag": '"v1"', "content-length": "0"},
            "bulk_export_output_http_412",
        ),
    ],
)
def test_bulk_range_response_contract(
    status,
    resume_offset,
    headers,
    expected_error,
):
    assert importer._bulk_output_response_error(
        status,
        headers,
        resume_offset=resume_offset,
        expected_etag='"v1"',
        expected_content_length=100,
    ) == expected_error


@pytest.mark.asyncio
async def test_bulk_stream_resumes_at_exact_committed_ndjson_boundary():
    first_line = b'{"resourceType":"Practitioner","id":"one"}\n'
    second_line = b'{"resourceType":"Practitioner","id":"two"}\n'
    resume_offset = len(first_line)
    content_length = resume_offset + len(second_line)
    response = _Response(
        206,
        {
            "ETag": '"output-v1"',
            "Content-Length": str(len(second_line)),
            "Content-Range": (
                f"bytes {resume_offset}-{content_length - 1}/{content_length}"
            ),
        },
        [second_line[:11], second_line[11:]],
    )
    session = _Session(response)
    written_resource_ids = []
    progress = []

    async def write_batch(_model, resource_rows):
        written_resource_ids.extend(row["resource_id"] for row in resource_rows)
        return len(resource_rows)

    async def record_progress(rows_written, committed_bytes):
        progress.append((rows_written, committed_bytes))

    stream_result = await importer._stream_bulk_export_output_rows(
        session,
        _aetna_source(),
        (
            "https://storage.googleapis.com/aetna/export.ndjson"
            "?X-Goog-Signature=secret"
        ),
        model=ProviderDirectoryPractitioner,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=3,
        run_id="run-resume",
        row_batch_handler=write_batch,
        row_batch_size=1,
        retain_rows=False,
        requires_access_token=False,
        resume_options=importer.BulkOutputResumeOptions(
            row_progress_handler=record_progress,
            resume_offset=resume_offset,
            expected_etag='"output-v1"',
            expected_content_length=content_length,
        ),
    )

    assert stream_result == ([], 1, 1, False, None)
    assert written_resource_ids == ["two"]
    assert progress == [(1, content_length)]
    assert session.request_options["headers"]["Range"] == f"bytes={resume_offset}-"
    assert session.request_options["headers"]["If-Match"] == '"output-v1"'
    assert session.request_options["headers"]["Accept-Encoding"] == "identity"
    assert "secret" not in str(stream_result)


@pytest.mark.asyncio
async def test_bulk_output_concurrency_is_bounded(monkeypatch):
    identity = _checkpoint_identity()
    manifest = _manifest(6)
    concurrency_by_name = {"active": 0, "maximum": 0}

    async def stream_one(*_args, **_kwargs):
        concurrency_by_name["active"] += 1
        concurrency_by_name["maximum"] = max(
            concurrency_by_name["maximum"],
            concurrency_by_name["active"],
        )
        await asyncio.sleep(0.01)
        concurrency_by_name["active"] -= 1
        return None

    monkeypatch.setattr(
        importer,
        "_stream_one_checkpointed_bulk_output",
        stream_one,
    )
    options = _resume_stream_options(2)

    _stream_state, error = await importer._resume_checkpointed_bulk_outputs(
        object(),
        _aetna_source(),
        identity,
        manifest,
        options,
        _output_checkpoints(identity, manifest),
    )

    assert error is None
    assert concurrency_by_name["maximum"] == 2


@pytest.mark.asyncio
async def test_bulk_output_failure_cancels_and_drains_siblings(monkeypatch):
    """The first output error drains cancellation before checkpoint failure."""
    identity = _checkpoint_identity()
    manifest = _manifest(3)
    sibling_started = asyncio.Event()
    sibling_drained = asyncio.Event()
    checkpoint_errors = []

    async def stream_one(
        _session,
        _source,
        _identity,
        _manifest,
        manifest_output,
        *_args,
    ):
        if manifest_output.output_index == 0:
            await sibling_started.wait()
            raise RuntimeError("bulk_export_transport_timeout")
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_drained.set()

    async def record_checkpoint_error(_identity, error, *, terminal):
        assert sibling_drained.is_set()
        checkpoint_errors.append((error, terminal))

    monkeypatch.setattr(
        importer,
        "_stream_one_checkpointed_bulk_output",
        stream_one,
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_checkpoint_error,
    )
    options = _resume_stream_options(2)

    _stream_state, error = await importer._resume_checkpointed_bulk_outputs(
        object(),
        _aetna_source(),
        identity,
        manifest,
        options,
        _output_checkpoints(identity, manifest),
    )

    assert error == "bulk_export_transport_timeout"
    assert checkpoint_errors == [("bulk_export_transport_timeout", False)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("state", "expected_complete"),
    [
        (importer.BULK_EXPORT_OUTPUT_STREAMING, False),
        (importer.BULK_EXPORT_OUTPUT_COMPLETE, True),
    ],
)
async def test_legacy_validator_adoption_selects_safe_progress_policy(
    monkeypatch,
    state,
    expected_complete,
):
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY",
        "bulk-resume-validator-test-key",
    )
    status_calls = []

    async def record_status(sql, **parameters):
        status_calls.append((sql, parameters))
        return 1

    monkeypatch.setattr(importer.db, "status", record_status)
    output_checkpoint_map = {
        "output_id": "output-resume",
        "state": state,
        "rows_written": 17,
        "committed_bytes": 0,
        "content_length_bytes": None,
        "etag_ciphertext": None,
        "etag_hash": None,
        "validator_checked_at": None,
    }
    validator = importer.BulkExportOutputValidator(
        content_length_bytes=4321,
        etag='"output-v1"',
        etag_hash=hashlib.sha256(b'"output-v1"').hexdigest(),
        output_expires_at=None,
    )

    await importer._persist_bulk_output_validator(
        _checkpoint_identity(),
        output_checkpoint_map,
        validator,
    )

    sql, parameters = status_calls[0]
    assert "WHEN :is_complete THEN :content_length_bytes" in sql
    assert "WHEN :is_legacy AND NOT :is_complete THEN 0" in sql
    assert parameters["is_legacy"] is True
    assert parameters["is_complete"] is expected_complete
    assert parameters["content_length_bytes"] == 4321
    assert "output-v1" not in parameters["etag_ciphertext"]
    assert parameters["etag_hash"] == validator.etag_hash
