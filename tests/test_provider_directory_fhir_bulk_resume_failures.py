# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_fhir_bulk_resume import (
    _aetna_source,
    _checkpoint_identity,
    _manifest,
    _output_checkpoints,
    _resume_stream_options,
    importer,
)

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

    assert error == "bulk_export_transport_runtimeerror"
    assert checkpoint_errors == [("bulk_export_transport_runtimeerror", False)]


@pytest.mark.asyncio
async def test_bulk_output_import_cancellation_drains_siblings_and_propagates(
    monkeypatch,
):
    """A child control-plane cancellation cannot become checkpoint failure."""
    identity = _checkpoint_identity()
    manifest = _manifest(3)
    sibling_started = asyncio.Event()
    sibling_drained = asyncio.Event()

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
            raise importer.ImportCancelledError("cancelled")
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_drained.set()

    record_checkpoint_error = AsyncMock()
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

    with pytest.raises(importer.ImportCancelledError, match="cancelled"):
        await importer._resume_checkpointed_bulk_outputs(
            object(),
            _aetna_source(),
            identity,
            manifest,
            _resume_stream_options(2),
            _output_checkpoints(identity, manifest),
        )

    assert sibling_drained.is_set()
    record_checkpoint_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_bulk_output_fencing_loss_drains_without_failure_writes(monkeypatch):
    identity = _checkpoint_identity()
    manifest = _manifest(3)
    sibling_started = asyncio.Event()
    sibling_drained = asyncio.Event()

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
            raise RuntimeError("bulk_export_checkpoint_worker_guard_lost")
        sibling_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            sibling_drained.set()

    record_output_error = AsyncMock()
    record_checkpoint_error = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_stream_one_checkpointed_bulk_output",
        stream_one,
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_output_error",
        record_output_error,
    )
    monkeypatch.setattr(
        importer,
        "_record_bulk_export_checkpoint_error",
        record_checkpoint_error,
    )

    with pytest.raises(
        RuntimeError,
        match="bulk_export_checkpoint_worker_guard_lost",
    ):
        await importer._resume_checkpointed_bulk_outputs(
            object(),
            _aetna_source(),
            identity,
            manifest,
            _resume_stream_options(2),
            _output_checkpoints(identity, manifest),
        )

    assert sibling_drained.is_set()
    record_output_error.assert_not_awaited()
    record_checkpoint_error.assert_not_awaited()


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
