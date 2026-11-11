"""Bulk export capability-refresh safety and failure-path tests."""

from __future__ import annotations

import copy
import dataclasses
import datetime
import hashlib
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock

import pytest

from db.models import ProviderDirectoryPractitioner
from tests.provider_directory_checkpoint_unit_support import checkpoint_unit_scope as checkpoint_unit_scope
from tests.provider_directory_fhir_bulk_test_support import (
    _acquire,
    _checkpoint,
    _checkpoint_context,
    _client_session,
    _Connection,
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
    assert importer._bulk_capability_refresh_attempt_limit() == importer.BULK_EXPORT_MAX_CAPABILITY_REFRESH_ATTEMPTS


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
        importer._bulk_output_validator_proof(_refresh_output_checkpoint(**overrides))


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

    invalid_manifest, error = importer._validated_bulk_capability_refresh_manifest(
        _source(),
        _refresh_identity(),
        {"requiresAccessToken": False},
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
    monkeypatch.setattr(importer.bulk_parallel, "persist_refresh_status", AsyncMock(return_value={}))
    if poll_result is not None:
        monkeypatch.setattr(
            importer,
            "_poll_bulk_capability_refresh_request",
            AsyncMock(return_value=poll_result),
        )

    manifest, error, polls = await importer._request_bulk_capability_refresh_manifest(
        object(),
        _source(),
        _refresh_identity(),
        timeout=3,
        max_pending_seconds=10,
        runtime_probe=AsyncMock(),
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
    assert importer._bulk_validator_expiration(validator) == expires_at.replace(tzinfo=None)


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


@pytest.mark.asyncio
@pytest.mark.parametrize(("length", "etag"), [(20, '"stable"'), (21, '"stable"'), (20, '"changed"')])
async def test_refreshed_capability_requires_same_bytes_before_resume(monkeypatch, length, etag):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    identity, checkpoint = _refresh_identity(), _refresh_checkpoint()
    output = _refresh_output_checkpoint(etag_ciphertext=importer._encrypt_bulk_capability('"stable"'))
    manifest = importer._bulk_export_manifest_from_payload(
        _manifest_payload(output=[{"type": "Practitioner", "url": "https://storage.googleapis.com/example"}]),
        "Practitioner",
    )
    headers_by_name = {"content-length": "1", "content-range": f"bytes 0-0/{length}", "etag": etag}
    persist_refresh = AsyncMock()
    monkeypatch.setattr(importer, "_bulk_http_probe_output", AsyncMock(return_value=(206, headers_by_name, None)))
    monkeypatch.setattr(importer, "_persist_bulk_capability_refresh", persist_refresh)

    error = await importer._validate_and_persist_bulk_capabilities(
        object(),
        _source(),
        identity,
        checkpoint,
        manifest,
        [output],
        _fetch_options(),
        AsyncMock(),
    )

    if length != 20 or etag != '"stable"':
        assert error == "bulk_export_output_validator_mismatch"
        assert importer._is_bulk_export_error_terminal(error)
        persist_refresh.assert_not_awaited()
        return
    assert error is None
    persist_refresh.assert_awaited_once()
    assert persist_refresh.await_args.args[:3] == (identity, checkpoint, manifest)
    output_pairs, validators = persist_refresh.await_args.args[3:]
    assert output_pairs == [(manifest.outputs[0], output)]
    assert validators[0].etag_hash == output["etag_hash"]
    assert validators[0].content_length_bytes == output["content_length_bytes"]

    stream = AsyncMock(return_value=([], 0, 0, False, None))
    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream)
    await importer._request_checkpointed_bulk_output(
        object(),
        _source(),
        identity,
        manifest,
        manifest.outputs[0],
        output,
        _stream_options(range_resume_enabled=True),
        AsyncMock(),
    )

    assert stream.await_args.args[2] == manifest.outputs[0].url
    resume_options = stream.await_args.kwargs["resume_options"]
    assert resume_options.resume_offset == 10
    assert resume_options.expected_content_length == 20
    assert resume_options.expected_etag == '"stable"'


def _refresh_checkpoint_with_status(identity=None):
    identity = identity or _refresh_identity()
    status_url = f"{identity.canonical_api_base}/status/retained-export"
    checkpoint = _refresh_checkpoint(identity)
    checkpoint.update(
        status_url_ciphertext=importer._encrypt_bulk_capability(status_url),
        status_url_hash=hashlib.sha256(status_url.encode()).hexdigest(),
    )
    return checkpoint, status_url


def _retained_output_checkpoints():
    return [
        _refresh_output_checkpoint(
            output_id=f"retained-output-{index}",
            output_index=index,
            content_length_bytes=100 + index,
            committed_bytes=10 + index,
            etag_hash=hashlib.sha256(f'"stable-{index}"'.encode()).hexdigest(),
            etag_ciphertext=importer._encrypt_bulk_capability(f'"stable-{index}"'),
        )
        for index in range(31)
    ]


def _retained_probe_responses(changed_proof):
    responses = []
    for index in range(31):
        length = 100 + index + int(changed_proof == "length" and index == 30)
        etag = '"changed"' if changed_proof == "etag" and index == 30 else f'"stable-{index}"'
        responses.append(
            (
                206,
                {"content-length": "1", "content-range": f"bytes 0-0/{length}", "etag": etag},
                None,
            )
        )
    return responses


def _retained_output_refresh_mocks(monkeypatch, changed_proof):
    outputs = _retained_output_checkpoints()
    probe = AsyncMock(side_effect=_retained_probe_responses(changed_proof))
    persist = AsyncMock()
    stream = AsyncMock(return_value=([], 0, 0, False, None))
    monkeypatch.setattr(importer, "_bulk_http_probe_output", probe)
    monkeypatch.setattr(importer, "_persist_bulk_capability_refresh", persist)
    monkeypatch.setattr(importer, "_stream_bulk_export_output_rows", stream)
    return outputs, probe, persist, stream


async def _assert_retained_output_resumes(manifest, outputs, stream):
    for manifest_output, output in zip(manifest.outputs, outputs, strict=True):
        await importer._request_checkpointed_bulk_output(
            object(),
            _source(),
            _refresh_identity(),
            manifest,
            manifest_output,
            output,
            _stream_options(range_resume_enabled=True),
            AsyncMock(),
        )
        resume = stream.await_args.kwargs["resume_options"]
        assert resume.resume_offset == output["committed_bytes"]
        assert resume.expected_content_length == output["content_length_bytes"]
        assert hashlib.sha256(resume.expected_etag.encode()).hexdigest() == output["etag_hash"]
    assert stream.await_count == len(outputs)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_proof", [None, "length", "etag"])
@pytest.mark.parametrize("saved_status", [200, 404, 410])
async def test_refresh_renews_saved_status_before_resuming_all_retained_outputs(
    monkeypatch, changed_proof, saved_status
):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, status_url = _refresh_checkpoint_with_status()
    identity = _refresh_identity()
    status_payload = _manifest_payload(
        output=[
            {
                "type": "Practitioner",
                "url": f"https://storage.googleapis.com/example/part-{index}?sig=fresh",
            }
            for index in range(31)
        ]
    )
    http_get = AsyncMock(return_value=(saved_status, {}, status_payload, None))
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    start_export = AsyncMock(return_value=(200, status_payload, None, None))
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start_export)
    outputs, probe, persist, stream = _retained_output_refresh_mocks(monkeypatch, changed_proof)
    original_outputs = [dict(output) for output in outputs]
    monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock())
    monkeypatch.setattr(importer, "_prepare_bulk_capability_refresh", AsyncMock(return_value=outputs))
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", AsyncMock(side_effect=_refresh_error_result))
    manifest, error, polls = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), identity, checkpoint, _fetch_options(), AsyncMock()
    )
    assert polls == 1 and http_get.await_args.args[2] == status_url
    assert start_export.await_count == int(saved_status in {404, 410})
    assert outputs == original_outputs and probe.await_count == 31
    if changed_proof is not None:
        assert error == "bulk_export_output_validator_mismatch"
        persist.assert_not_awaited()
        stream.assert_not_awaited()
        return
    assert manifest is not None and error is None
    persist.assert_awaited_once()
    await _assert_retained_output_resumes(manifest, outputs, stream)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,error",
    [(401, None), (403, None), (404, None), (410, None), (429, None), (503, None), (None, "request-failed")],
)
async def test_saved_status_request_reports_failure_without_starting_export(monkeypatch, status, error):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(return_value=(status, {}, None, error)))
    refresh = AsyncMock(return_value=(_refresh_manifest(), None, 2))
    monkeypatch.setattr(importer, "_request_bulk_capability_refresh_manifest", refresh)
    manifest, observed_error, polls = await importer._request_bulk_capability_refresh_for_options(
        object(), _source(), _refresh_identity(), _fetch_options(), AsyncMock(), checkpoint=checkpoint
    )
    assert manifest is None and polls == 1
    assert observed_error == (error or f"bulk_export_status_http_{status}")
    refresh.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint", [None, {}], ids=["no_checkpoint", "legacy_checkpoint"])
async def test_refresh_without_saved_status_keeps_legacy_request_path(monkeypatch, checkpoint):
    expected = (_refresh_manifest(), None, 1)
    refresh = AsyncMock(return_value=expected)
    monkeypatch.setattr(importer, "_request_bulk_capability_refresh_manifest", refresh)
    outcome = await importer._request_bulk_capability_refresh_for_options(
        object(), _source(), _refresh_identity(), _fetch_options(), AsyncMock(), checkpoint=checkpoint
    )
    assert outcome == expected
    refresh.assert_awaited_once()


@pytest.mark.asyncio
async def test_refresh_refuses_an_empty_success_without_replacing_retained_capabilities(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    previous_checkpoint_by_field = dict(checkpoint)
    request = AsyncMock(return_value=(None, None, 2))
    persist = AsyncMock()
    record_error = AsyncMock(side_effect=_refresh_error_result)
    monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock())
    monkeypatch.setattr(
        importer, "_prepare_bulk_capability_refresh", AsyncMock(return_value=[_refresh_output_checkpoint()])
    )
    monkeypatch.setattr(importer, "_request_bulk_capability_refresh_for_options", request)
    monkeypatch.setattr(importer, "_validate_and_persist_bulk_capabilities", persist)
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", record_error)
    identity = _refresh_identity()
    result = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), identity, checkpoint, _fetch_options(), AsyncMock()
    )
    assert result == (None, "bulk_export_manifest_unavailable", 2)
    assert checkpoint == previous_checkpoint_by_field
    request.assert_awaited_once()
    assert request.await_args.kwargs["checkpoint"] is checkpoint
    persist.assert_not_awaited()
    record_error.assert_awaited_once_with(identity, "bulk_export_manifest_unavailable", 2)


def test_capability_persistence_swaps_status_and_retains_progress():
    parent_sql = importer._bulk_capability_parent_update_sql()
    output_sql = importer._bulk_capability_output_update_sql()
    assert "status_url_ciphertext = :status_url_ciphertext" in parent_sql
    assert "status_url_hash = :status_url_hash" in parent_sql
    assert "committed_bytes =" not in output_sql
    assert "rows_written =" not in output_sql


@pytest.mark.asyncio
async def test_refresh_rejects_corrupt_saved_status_before_any_request(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    checkpoint["status_url_hash"] = "0" * 64
    manifest, error, polls = await importer._request_bulk_capability_refresh_for_options(
        object(), _source(), _refresh_identity(), _fetch_options(), AsyncMock(), checkpoint=checkpoint
    )
    assert manifest is None and polls == 0
    assert error == "bulk_export_status_url_hash_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize("expiry", ["timestamp", 404, 410])
@pytest.mark.parametrize("changed_proof", [None, "etag", "length"])
async def test_saved_manifest_expiry_renews_once_before_exact_retained_resume(monkeypatch, expiry, changed_proof):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    fresh_payload = _manifest_payload(
        output=[
            {"type": "Practitioner", "url": f"https://storage.googleapis.com/example/part-{index}?sig=fresh"}
            for index in range(31)
        ]
    )
    retained_payload = _manifest_payload(
        output=[
            {"type": "Practitioner", "url": f"https://storage.googleapis.com/example/part-{index}?expires=1"}
            for index in range(31)
        ]
        if expiry == "timestamp"
        else fresh_payload["output"]
    )
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(return_value=(200, {}, retained_payload, None)))
    start = AsyncMock(return_value=(200, fresh_payload, None, None))
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    outputs, probe, persist, stream = _retained_output_refresh_mocks(monkeypatch, changed_proof)
    first_probe = (206, {"content-length": "1", "content-range": "bytes 0-0/100", "etag": '"stable-0"'}, None)
    probe.side_effect = [first_probe if expiry == "timestamp" else (expiry, {}, None), *list(probe.side_effect)]
    original_outputs = [dict(output) for output in outputs]
    monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock())
    monkeypatch.setattr(importer, "_prepare_bulk_capability_refresh", AsyncMock(return_value=outputs))
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", AsyncMock(side_effect=_refresh_error_result))
    manifest, error, polls = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), _refresh_identity(), checkpoint, _fetch_options(), AsyncMock()
    )
    start.assert_awaited_once()
    assert polls == 1 and probe.await_count == 32 and outputs == original_outputs
    if changed_proof:
        assert manifest is None and error == "bulk_export_output_validator_mismatch"
        persist.assert_not_awaited()
        stream.assert_not_awaited()
    else:
        assert manifest is not None and error is None
        persist.assert_awaited_once()
        await _assert_retained_output_resumes(manifest, outputs, stream)


@pytest.mark.asyncio
@pytest.mark.parametrize("probe_status", [401, 403, 410])
async def test_saved_output_auth_refuses_and_expiry_allows_one_fresh_export(monkeypatch, probe_status):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(return_value=(200, {}, _manifest_payload(), None)))
    start = AsyncMock(return_value=(200, _manifest_payload(), None, None))
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    probe = AsyncMock(return_value=(probe_status, {}, None))
    persist = AsyncMock()
    monkeypatch.setattr(importer, "_bulk_http_probe_output", probe)
    monkeypatch.setattr(importer, "_persist_bulk_capability_refresh", persist)
    monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock())
    monkeypatch.setattr(
        importer, "_prepare_bulk_capability_refresh", AsyncMock(return_value=[_refresh_output_checkpoint()])
    )
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", AsyncMock(side_effect=_refresh_error_result))
    manifest, error, polls = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), _refresh_identity(), checkpoint, _fetch_options(), AsyncMock()
    )
    assert manifest is None and error == f"bulk_export_output_http_{probe_status}" and polls == 1
    assert start.await_count == int(probe_status == 410)
    assert probe.await_count == 1 + int(probe_status == 410)
    persist.assert_not_awaited()


def _apply_refresh_checkpoint_write(checkpoint, outputs, bindings):
    if "attempt_limit" in bindings:
        incomplete_outputs = [output for output in outputs if output["state"] != bindings["complete_state"]]
        eligible_outputs = [
            output for output in incomplete_outputs if output["attempt_count"] < bindings["attempt_limit"]
        ]
        for output in eligible_outputs:
            output["attempt_count"] += 1
        return len(eligible_outputs)
    if "refreshed_output_id" in bindings:
        output = next(output for output in outputs if output["output_id"] == bindings["output_id"])
        assert output["etag_hash"] == bindings["etag_hash"]
        assert output["content_length_bytes"] == bindings["content_length_bytes"]
        output.update(
            output_id=bindings["refreshed_output_id"],
            output_url_ciphertext=bindings["output_url_ciphertext"],
            output_url_hash=bindings["output_url_hash"],
            etag_ciphertext=bindings["etag_ciphertext"],
        )
    if "manifest_audit_json" in bindings:
        checkpoint.update(
            **{
                key: bindings[key]
                for key in ("manifest_hash", "manifest_ciphertext", "status_url_ciphertext", "status_url_hash")
            },
            manifest_json=json.loads(bindings["manifest_audit_json"]),
            state=bindings["manifest_ready_state"],
            error=None,
            next_poll_at=None,
            manifest_received_at=importer._bulk_export_now_utc(),
        )
    if "next_poll_at" in bindings:
        checkpoint["next_poll_at"] = bindings["next_poll_at"]
    return 1


def _refresh_outer_checkpoint_ledger(monkeypatch, checkpoint, outputs):
    async def status(statement, **values):
        if statement == importer._bulk_checkpoint_error_update_sql():
            checkpoint.update(
                error=values["error"], state=values["failed_state"] if values["terminal"] else values["retryable_state"]
            )
            if values["terminal"]:
                checkpoint.update(status_url_ciphertext=None, manifest_ciphertext=None)
                for output in outputs:
                    output.update(output_url_ciphertext=None, etag_ciphertext=None)
            return 1
        return _apply_refresh_checkpoint_write(checkpoint, outputs, values)

    async def first(statement, **values):
        assert "AND owner_run_id = :owner_run_id" in statement and "AND error = ANY(:refresh_errors)" in statement
        assert checkpoint["owner_run_id"] == values["owner_run_id"]
        assert checkpoint["state"] == values["retryable_state"] and checkpoint["error"] in values["refresh_errors"]
        checkpoint.update(
            status_url_ciphertext=values["status_url_ciphertext"],
            status_url_hash=values["status_url_hash"],
            accepted_at=importer._bulk_export_now_utc(),
            next_poll_at=None,
        )
        return {key: checkpoint.get(key) for key in ("accepted_at", "manifest_received_at", "next_poll_at")}

    connection = _Connection(1)
    connection.status.side_effect = status
    monkeypatch.setattr(importer.db, "acquire", lambda: _acquire(connection))
    monkeypatch.setattr(importer.db, "status", AsyncMock(side_effect=status))
    monkeypatch.setattr(importer.db, "first", AsyncMock(side_effect=first))
    monkeypatch.setattr(
        importer, "_load_bulk_export_checkpoint", AsyncMock(side_effect=lambda _identity: dict(checkpoint))
    )
    monkeypatch.setattr(importer, "_load_bulk_output_checkpoints", AsyncMock(return_value=outputs))
    monkeypatch.setattr(importer, "_bulk_client_session", _client_session)
    monkeypatch.setattr(importer, "_bulk_checkpoint_worker_guard", lambda _identity: _acquire(AsyncMock()))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status_code,request_error",
    [(429, None), (503, None), (None, "bulk_export_transport_sockettimeouterror")],
)
async def test_retryable_status_keeps_refresh_intent(monkeypatch, status_code, request_error):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_CAPABILITY_REFRESH_ATTEMPTS", "1")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    source_record = _source()
    source_record["metadata_json"]["provider_directory_bulk_export_range_resume"] = True
    context = dataclasses.replace(_checkpoint_context(), lineage_verified=True)
    identity = importer._bulk_export_checkpoint_identity(
        context, "Practitioner", importer._bulk_export_start_url(source_record, "Practitioner")
    )
    checkpoint, status_url = _refresh_checkpoint_with_status(identity)
    checkpoint["manifest_ciphertext"] = importer._encrypt_bulk_capability(json.dumps(_refresh_manifest().payload))
    outputs = [
        _refresh_output_checkpoint(
            attempt_count=1,
            etag_ciphertext=importer._encrypt_bulk_capability('"stable"'),
            output_url_ciphertext=importer._encrypt_bulk_capability(_refresh_manifest().outputs[0].url),
        )
    ]
    original_checkpoint, original_outputs = dict(checkpoint), [dict(output) for output in outputs]
    _refresh_outer_checkpoint_ledger(monkeypatch, checkpoint, outputs)
    http_get = AsyncMock(side_effect=[(status_code, {}, None, request_error), (200, {}, _manifest_payload(), None)])
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)
    start, persist = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    monkeypatch.setattr(importer, "_persist_bulk_capability_refresh", persist)
    headers_by_name = {"content-length": "1", "content-range": "bytes 0-0/20", "etag": '"stable"'}
    monkeypatch.setattr(importer, "_bulk_http_probe_output", AsyncMock(return_value=(206, headers_by_name, None)))
    stream = AsyncMock(
        return_value=importer._checkpointed_bulk_fetch_result(ProviderDirectoryPractitioner, complete=True)
    )
    monkeypatch.setattr(importer, "_stream_checkpointed_bulk_outputs", stream)
    first = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert first.error == (request_error or f"bulk_export_status_http_{status_code}")
    assert not importer._is_bulk_export_error_terminal(first.error)
    assert checkpoint == original_checkpoint and outputs == original_outputs
    persist.assert_not_awaited()
    stream.assert_not_awaited()
    second = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert second.complete and second.error is None
    assert [call.args[2] for call in http_get.await_args_list] == [status_url, status_url]
    persist.assert_awaited_once()
    stream.assert_awaited_once()
    start.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("delay_seconds", [7, 120])
async def test_replacement_rate_limit_survives_handoff(monkeypatch, delay_seconds):
    source_record, context, checkpoint, outputs, _url, stream = _lifecycle_fetch_setup(monkeypatch)
    clock_values = _advance_refresh_clock(monkeypatch)
    options = dataclasses.replace(_fetch_options(), bulk_export_max_pending_seconds=20)
    original_outputs = copy.deepcopy(outputs)
    observed_poll_times = []
    status_payload = _manifest_payload(
        output=[
            {"type": "Practitioner", "url": f"https://storage.googleapis.com/example/part-{i}?sig=new"}
            for i in range(31)
        ]
    )
    responses = iter(
        [(410, {}, None, None), (429, {"retry-after": str(delay_seconds)}, None, None), (200, {}, status_payload, None)]
    )

    async def poll(*_args, **_options):
        observed_poll_times.append(clock_values[0])
        return next(responses)

    monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(side_effect=poll))
    start = AsyncMock(return_value=(202, None, "/status/replacement-export", None))
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    probe = AsyncMock(side_effect=_retained_probe_responses(None))
    monkeypatch.setattr(importer, "_bulk_http_probe_output", probe)
    first = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, options
    )
    assert first.error == "bulk_export_status_http_429" and checkpoint["error"] == "bulk_export_output_http_410"
    accepted_at = checkpoint["accepted_at"]
    expected_poll_at = accepted_at + datetime.timedelta(seconds=min(delay_seconds, 20))
    assert importer._bulk_export_utc_datetime(checkpoint["next_poll_at"], error="invalid") == expected_poll_at
    assert outputs == [{**output, "attempt_count": 1} for output in original_outputs]
    # A new invocation must honor durable pacing without the previous process's request limiter.
    fresh_source = copy.deepcopy(source_record)
    fresh_source.pop("_provider_directory_bulk_parallel", None)
    second = await importer._fetch_checkpointed_bulk_export_resource_rows(
        fresh_source, "Practitioner", context, options
    )
    assert checkpoint["accepted_at"] == accepted_at
    if delay_seconds < 20:
        assert second.complete and second.error is None
        assert observed_poll_times[-1] == expected_poll_at and probe.await_count == 31
        stream.assert_awaited_once()
    else:
        assert second.error == importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
        assert len(observed_poll_times) == 2
        probe.assert_not_awaited()
        stream.assert_not_awaited()
    start.assert_awaited_once()


@pytest.mark.asyncio
async def test_initial_status_429_without_retained_proofs_does_not_start_capability_refresh(monkeypatch):
    checkpoint_by_field = {**_checkpoint(_identity()), "error": "bulk_export_status_http_429"}
    ordinary = AsyncMock(return_value=(None, "bulk_export_status_http_429", 1))
    refresh = AsyncMock()
    monkeypatch.setattr(importer, "_checkpointed_bulk_export_manifest", ordinary)
    monkeypatch.setattr(importer, "_refresh_checkpointed_bulk_export_capabilities", refresh)
    assert await importer._next_checkpointed_bulk_manifest(
        object(), _source(), _identity(), checkpoint_by_field, _fetch_options(), None, AsyncMock()
    ) == (None, "bulk_export_status_http_429", 1)
    ordinary.assert_awaited_once()
    refresh.assert_not_awaited()


def _lifecycle_fetch_setup(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_CAPABILITY_REFRESH_ATTEMPTS", "1")
    source_record = _source()
    source_record["metadata_json"]["provider_directory_bulk_export_range_resume"] = True
    context = dataclasses.replace(_checkpoint_context(), lineage_verified=True)
    identity = importer._bulk_export_checkpoint_identity(
        context, "Practitioner", importer._bulk_export_start_url(source_record, "Practitioner")
    )
    checkpoint, status_url = _refresh_checkpoint_with_status(identity)
    old_time = importer._bulk_export_now_utc() - datetime.timedelta(days=1)
    checkpoint.update(accepted_at=old_time, manifest_received_at=old_time, next_poll_at=old_time)
    checkpoint["manifest_ciphertext"] = importer._encrypt_bulk_capability(json.dumps(_refresh_manifest().payload))
    outputs = _retained_output_checkpoints()
    _refresh_outer_checkpoint_ledger(monkeypatch, checkpoint, outputs)
    stream = AsyncMock(
        return_value=importer._checkpointed_bulk_fetch_result(ProviderDirectoryPractitioner, complete=True)
    )
    monkeypatch.setattr(importer, "_stream_checkpointed_bulk_outputs", stream)
    return source_record, context, checkpoint, outputs, status_url, stream


def _advance_refresh_clock(monkeypatch):
    clock_values = [importer._bulk_export_now_utc()]

    async def sleep(seconds):
        clock_values[0] += datetime.timedelta(seconds=seconds)

    monkeypatch.setattr(importer, "_bulk_export_now_utc", lambda: clock_values[0])
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock(side_effect=sleep))
    return clock_values


@pytest.mark.asyncio
async def test_accepted_replacement_survives_429_and_renews_again(monkeypatch):
    source_record, context, checkpoint, outputs, original_url, stream = _lifecycle_fetch_setup(monkeypatch)
    clock_values = _advance_refresh_clock(monkeypatch)
    original_checkpoint, original_outputs = dict(checkpoint), copy.deepcopy(outputs)
    replacement_url = f"{context.canonical_api_base}/status/replacement-export"
    status_payload = _manifest_payload(
        output=[
            {"type": "Practitioner", "url": f"https://storage.googleapis.com/example/part-{i}?sig=new"}
            for i in range(31)
        ]
    )
    http_get = AsyncMock(
        side_effect=[
            (410, {}, None, None),
            (202, {"retry-after": "3"}, None, None),
            (429, {}, None, None),
            (200, {}, status_payload, None),
            (200, {}, status_payload, None),
        ]
    )
    start = AsyncMock(return_value=(202, None, replacement_url, None))
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    probe = AsyncMock(side_effect=_retained_probe_responses(None) * 2)
    monkeypatch.setattr(importer, "_bulk_http_probe_output", probe)
    first = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert first.error == "bulk_export_status_http_429" and checkpoint["error"] == original_checkpoint["error"]
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_RETRYABLE
    assert checkpoint["manifest_ciphertext"] == original_checkpoint["manifest_ciphertext"]
    assert checkpoint["manifest_hash"] == original_checkpoint["manifest_hash"]
    assert importer._bulk_checkpoint_status_url(checkpoint) == replacement_url
    assert checkpoint["status_url_hash"] == hashlib.sha256(replacement_url.encode()).hexdigest()
    assert checkpoint["accepted_at"] == clock_values[0] - datetime.timedelta(seconds=3)
    next_poll_at = clock_values[0] + datetime.timedelta(seconds=importer._bulk_export_poll_settings()[1])
    assert importer._bulk_export_utc_datetime(checkpoint["next_poll_at"], error="invalid") == next_poll_at
    assert outputs == [{**output, "attempt_count": 1} for output in original_outputs]
    probe.assert_not_awaited()
    stream.assert_not_awaited()
    second = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert second.complete and second.error is None and probe.await_count == 31
    assert clock_values[0] == next_poll_at
    assert checkpoint["status_url_hash"] == hashlib.sha256(replacement_url.encode()).hexdigest()
    assert checkpoint["next_poll_at"] is None and checkpoint["accepted_at"] < checkpoint["manifest_received_at"]
    assert all(output["attempt_count"] == 1 for output in outputs)
    assert [(output["committed_bytes"], output["content_length_bytes"], output["etag_hash"]) for output in outputs] == [
        (output["committed_bytes"], output["content_length_bytes"], output["etag_hash"]) for output in original_outputs
    ]
    checkpoint.update(state=importer.BULK_EXPORT_CHECKPOINT_RETRYABLE, error=original_checkpoint["error"])
    third = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert third.complete and third.error is None and probe.await_count == 62
    assert [call.args[2] for call in http_get.await_args_list] == [original_url, *([replacement_url] * 4)]
    start.assert_awaited_once()
    assert stream.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("location", [None, "/status/replacement-export"])
async def test_completed_replacement_atomically_swaps_or_clears_status(monkeypatch, location):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _original_url = _refresh_checkpoint_with_status()
    start = AsyncMock(return_value=(200, _manifest_payload(), location, None))
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    manifest, error, polls = await importer._request_bulk_capability_refresh_manifest(
        object(), _source(), _refresh_identity(), timeout=3, max_pending_seconds=10, runtime_probe=AsyncMock()
    )
    assert error is None and polls == 0
    connection = _Connection(1)
    await importer._persist_refreshed_bulk_manifest(connection, _refresh_identity(), checkpoint, manifest, "{}")
    persisted = connection.status.await_args.kwargs
    expected_url = (
        importer._resolved_bulk_export_status_url(_source(), _refresh_identity().start_url, location)
        if location
        else None
    )
    assert manifest.status_url == expected_url
    assert "status_url" not in manifest.payload
    assert persisted["status_url_hash"] == (hashlib.sha256(expected_url.encode()).hexdigest() if expected_url else None)
    assert (
        importer._decrypt_bulk_capability(persisted["status_url_ciphertext"])
        if expected_url
        else persisted["status_url_ciphertext"]
    ) == expected_url


@pytest.mark.asyncio
@pytest.mark.parametrize("saved_time", ["stale", "future", "invalid"])
@pytest.mark.parametrize("replacement_time", ["current", "stale", "future", "invalid"])
async def test_only_stale_saved_transaction_time_allows_strict_fresh_fallback(
    monkeypatch, saved_time, replacement_time
):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, _status_url = _refresh_checkpoint_with_status()
    now = importer._bulk_export_now_utc()
    transaction_times_by_kind = {
        "current": now.isoformat(),
        "stale": (now - datetime.timedelta(days=100)).isoformat(),
        "future": (now + datetime.timedelta(days=1)).isoformat(),
        "invalid": "invalid",
    }
    retained_payload_by_field = {**_manifest_payload(), "transactionTime": transaction_times_by_kind[saved_time]}
    fresh_payload_by_field = {**_manifest_payload(), "transactionTime": transaction_times_by_kind[replacement_time]}
    monkeypatch.setattr(
        importer, "_bulk_http_get_json", AsyncMock(return_value=(200, {}, retained_payload_by_field, None))
    )
    monkeypatch.setattr(importer, "_refresh_bulk_export_rows_written", AsyncMock())
    start = AsyncMock(return_value=(200, fresh_payload_by_field, None, None))
    persist = AsyncMock()
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    monkeypatch.setattr(importer, "_persist_bulk_capability_refresh", persist)
    monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock())
    monkeypatch.setattr(
        importer, "_prepare_bulk_capability_refresh", AsyncMock(return_value=[_refresh_output_checkpoint()])
    )
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", AsyncMock(side_effect=_refresh_error_result))
    headers_by_name = {"content-length": "1", "content-range": "bytes 0-0/20", "etag": '"stable"'}
    monkeypatch.setattr(importer, "_bulk_http_probe_output", AsyncMock(return_value=(206, headers_by_name, None)))
    manifest, error, polls = await importer._refresh_checkpointed_bulk_export_capabilities(
        object(), _source(), _refresh_identity(), checkpoint, _fetch_options(), AsyncMock()
    )
    assert start.await_count == int(saved_time == "stale") and polls == 1
    is_valid = saved_time == "stale" and replacement_time == "current"
    assert (manifest is not None) is is_valid and persist.await_count == int(is_valid)
    if not is_valid:
        expected_kind = replacement_time if saved_time == "stale" else saved_time
        expected_error = (
            "bulk_export_manifest_invalid_transaction_time"
            if expected_kind == "invalid"
            else f"bulk_export_manifest_transaction_time_{expected_kind}"
        )
        assert error == expected_error


@pytest.mark.asyncio
async def test_replacement_acceptance_rejects_lost_lease_before_poll(monkeypatch):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    first = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", first)
    start = AsyncMock(return_value=(202, None, "/status/replacement-export", None))
    poll = AsyncMock()
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    monkeypatch.setattr(importer, "_poll_bulk_capability_refresh_request", poll)
    with pytest.raises(RuntimeError, match="ownership_lost"):
        await importer._request_bulk_capability_refresh_manifest(
            object(), _source(), _refresh_identity(), timeout=3, max_pending_seconds=10, runtime_probe=AsyncMock()
        )
    first.assert_awaited_once()
    poll.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("location", [None, "https://untrusted.invalid/status"])
async def test_accepted_replacement_refuses_invalid_status_before_write(monkeypatch, location):
    monkeypatch.setattr(
        importer, "_start_bulk_capability_refresh_request", AsyncMock(return_value=(202, None, location, None))
    )
    first = AsyncMock()
    monkeypatch.setattr(importer.db, "first", first)
    manifest, error, polls = await importer._request_bulk_capability_refresh_manifest(
        object(), _source(), _refresh_identity(), timeout=3, max_pending_seconds=10, runtime_probe=AsyncMock()
    )
    assert (
        manifest is None
        and error in {"bulk_export_missing_status_url", "bulk_export_untrusted_status_url"}
        and polls == 0
    )
    first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_status", [False, True])
async def test_capability_swap_rolls_back_status_and_outputs_on_parent_failure(monkeypatch, has_status):
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", "bulk-refresh-test-key")
    checkpoint, status_url = _refresh_checkpoint_with_status()
    outputs = [_refresh_output_checkpoint()]
    original_checkpoint, original_outputs = copy.deepcopy(checkpoint), copy.deepcopy(outputs)
    manifest = dataclasses.replace(_refresh_manifest(), status_url=status_url if has_status else None)
    _refresh_outer_checkpoint_ledger(monkeypatch, checkpoint, outputs)
    validator = importer.BulkExportOutputValidator(20, '"stable"', outputs[0]["etag_hash"], None)
    persist_manifest = importer._persist_refreshed_bulk_manifest

    @asynccontextmanager
    async def transaction(_database, **_coordinates):
        try:
            yield
        except RuntimeError:
            checkpoint.clear()
            checkpoint.update(original_checkpoint)
            outputs[:] = original_outputs
            raise

    async def fail_parent(connection, *args):
        connection.status.return_value = 0
        connection.status.side_effect = None
        await persist_manifest(connection, *args)

    monkeypatch.setattr(importer, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(importer, "_persist_refreshed_bulk_manifest", fail_parent)
    with pytest.raises(RuntimeError, match="ownership_lost"):
        await importer._persist_bulk_capability_refresh(
            _refresh_identity(), checkpoint, manifest, [(manifest.outputs[0], outputs[0])], [validator]
        )
    assert checkpoint == original_checkpoint and outputs == original_outputs


@pytest.mark.parametrize(
    ("accepted_at", "received_at", "is_pending"),
    [
        (None, None, False),
        ("2026-01-02", None, False),
        ("2026-01-02", "2026-01-01", True),
        ("2026-01-01", "2026-01-02", False),
    ],
)
def test_pending_refresh_reuses_only_unfinished_acceptance(accepted_at, received_at, is_pending):
    checkpoint_by_field = {
        "accepted_at": accepted_at,
        "manifest_received_at": received_at,
        "next_poll_at": "2026-01-02",
    }
    result = importer.bulk_parallel.pending_refresh_checkpoint(importer, checkpoint_by_field)
    assert (result is checkpoint_by_field) is is_pending


@pytest.mark.parametrize("invalid_field", ["accepted_at", "manifest_received_at"])
def test_pending_refresh_refuses_corrupt_timestamps(invalid_field):
    checkpoint_by_field = {"accepted_at": "2026-01-02", "manifest_received_at": "2026-01-01", invalid_field: "invalid"}
    with pytest.raises(ValueError, match=f"bulk_export_checkpoint_{invalid_field}_invalid"):
        importer.bulk_parallel.pending_refresh_checkpoint(importer, checkpoint_by_field)


@pytest.mark.asyncio
async def test_fresh_refresh_budget_refuses_before_remote_start(monkeypatch):
    source_record, context, checkpoint, outputs, _url, stream = _lifecycle_fetch_setup(monkeypatch)
    checkpoint.update(status_url_ciphertext=None, status_url_hash=None)
    for output in outputs:
        output["attempt_count"] = 1
    original_progress = [
        (output["committed_bytes"], output["content_length_bytes"], output["etag_hash"]) for output in outputs
    ]
    start, http_get = AsyncMock(), AsyncMock()
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)
    result = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert result.error == "bulk_export_output_capability_refresh_exhausted"
    assert [
        (output["committed_bytes"], output["content_length_bytes"], output["etag_hash"]) for output in outputs
    ] == original_progress
    start.assert_not_awaited()
    http_get.assert_not_awaited()
    stream.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["saved", "accepted", "reservation", "validation"])
@pytest.mark.parametrize("failure_kind", ["cancel", "control", *sorted(importer.BULK_EXPORT_FENCING_ERRORS)])
async def test_renewal_propagates_control_failures_without_clearing_capabilities(monkeypatch, stage, failure_kind):
    source_record, context, checkpoint, outputs, _url, _stream = _lifecycle_fetch_setup(monkeypatch)
    identity = importer._bulk_export_checkpoint_identity(
        context, "Practitioner", importer._bulk_export_start_url(source_record, "Practitioner")
    )
    failures_by_kind = {
        "cancel": importer.ImportCancelledError("cancelled"),
        "control": importer.ResourceImportControlPlaneFailure("control-failed"),
    }
    failure = failures_by_kind.get(failure_kind, RuntimeError(failure_kind))
    old_manifest = checkpoint["manifest_ciphertext"]
    original_outputs = copy.deepcopy(outputs)
    record_error = AsyncMock()
    monkeypatch.setattr(importer, "_record_bulk_capability_refresh_error", record_error)
    replacement_url = f"{identity.canonical_api_base}/status/replacement-export"
    if stage == "accepted":
        monkeypatch.setattr(importer, "_bulk_http_get_json", AsyncMock(side_effect=[(410, {}, None, None), failure]))
        monkeypatch.setattr(
            importer,
            "_start_bulk_capability_refresh_request",
            AsyncMock(return_value=(202, None, replacement_url, None)),
        )
    elif stage == "reservation":
        checkpoint.update(status_url_ciphertext=None, status_url_hash=None)
        monkeypatch.setattr(importer, "_begin_bulk_capability_refresh", AsyncMock(side_effect=[None, failure]))
    elif stage == "validation":
        monkeypatch.setattr(
            importer,
            "_request_bulk_capability_refresh_for_options",
            AsyncMock(return_value=(_refresh_manifest(), None, 1)),
        )
        monkeypatch.setattr(importer, "_validate_and_persist_bulk_capabilities", AsyncMock(side_effect=failure))
    else:
        monkeypatch.setattr(importer, "_poll_bulk_capability_refresh_request", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as raised:
        await importer._refresh_checkpointed_bulk_export_capabilities(
            object(), source_record, identity, checkpoint, _fetch_options(), AsyncMock()
        )
    assert raised.value is failure
    assert checkpoint["manifest_ciphertext"] == old_manifest
    assert checkpoint["error"] == "bulk_export_output_http_410"
    assert checkpoint["state"] == importer.BULK_EXPORT_CHECKPOINT_RETRYABLE
    if stage == "accepted":
        assert importer._bulk_checkpoint_status_url(checkpoint) == replacement_url
        assert outputs == [{**output, "attempt_count": 1} for output in original_outputs]
    else:
        assert outputs == original_outputs
    record_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_replacement_deadline_survives_rate_limited_fetch_handoff(monkeypatch):
    source_record, context, checkpoint, outputs, _url, stream = _lifecycle_fetch_setup(monkeypatch)
    clock_values = _advance_refresh_clock(monkeypatch)
    http_get = AsyncMock(side_effect=[(410, {}, None, None), (429, {}, None, None)])
    start = AsyncMock(return_value=(202, None, "/status/replacement-export", None))
    monkeypatch.setattr(importer, "_bulk_http_get_json", http_get)
    monkeypatch.setattr(importer, "_start_bulk_capability_refresh_request", start)
    first = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert first.error == "bulk_export_status_http_429"
    accepted_at = checkpoint["accepted_at"]
    clock_values[0] += datetime.timedelta(seconds=_fetch_options().bulk_export_max_pending_seconds)
    second = await importer._fetch_checkpointed_bulk_export_resource_rows(
        source_record, "Practitioner", context, _fetch_options()
    )
    assert second.error == importer.BULK_EXPORT_STATUS_DEADLINE_EXCEEDED
    assert checkpoint["accepted_at"] == accepted_at and all(output["attempt_count"] == 1 for output in outputs)
    assert http_get.await_count == 2
    start.assert_awaited_once()
    stream.assert_not_awaited()
