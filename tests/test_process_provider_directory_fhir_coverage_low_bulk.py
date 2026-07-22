# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_fhir_coverage_low_support import (
    assert_bulk_decode_edges,
    bulk_identity,
)


importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_bulk_poll_timeout(monkeypatch):
    monkeypatch.setattr(importer, "_bulk_export_poll_settings", Mock(return_value=(1, 0)))
    monkeypatch.setattr(importer, "_bulk_export_pending_poll_times", Mock(return_value=(None, None)))
    monkeypatch.setattr(
        importer,
        "_bulk_http_get_json",
        AsyncMock(return_value=(202, {}, None, None)),
    )
    monkeypatch.setattr(
        importer,
        "_handle_bulk_export_pending_poll",
        AsyncMock(return_value=(None, None)),
    )
    assert await importer._bulk_export_poll_manifest(
        object(),
        {"source_id": "source-1"},
        "https://bulk.example.test/status/1",
        resource_type="Practitioner",
        timeout=1,
    ) == (None, "bulk_export_timeout", 1)


def test_bulk_checkpoint_decode_edges(monkeypatch):
    assert_bulk_decode_edges(monkeypatch)


@pytest.mark.asyncio
async def test_bulk_checkpoint_write_helpers(monkeypatch):
    identity = bulk_identity()
    status = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer, "_encrypt_bulk_capability", Mock(return_value="cipher"))
    await importer._release_bulk_export_reservation(identity)
    await importer._insert_accepted_bulk_checkpoint(identity, identity.start_url)
    await importer._mark_bulk_checkpoint_accepted(identity, identity.start_url)
    assert status.await_count == 3
    inserted = status.await_args_list[1].kwargs
    assert inserted["status_url_hash"] == hashlib.sha256(identity.start_url.encode()).hexdigest()


@pytest.mark.asyncio
async def test_bulk_output_load_and_validation_error(monkeypatch):
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=[{"output_id": "one"}]))
    assert await importer._load_bulk_output_checkpoints("checkpoint-1") == [
        {"output_id": "one"}
    ]
    monkeypatch.setattr(
        importer,
        "_stored_bulk_output_validator",
        Mock(side_effect=ValueError("validator invalid")),
    )
    error = await importer._probe_and_persist_bulk_output_validator(
        object(),
        {},
        bulk_identity(),
        importer.BulkExportManifest("hash", {}, False, ()),
        importer.BulkExportManifestOutput(
            0, "Practitioner", "https://bulk.example.test/out", "url", "id"
        ),
        {},
        timeout=1,
    )
    assert error == "validator invalid"


@pytest.mark.asyncio
async def test_bulk_stream_value_error(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_bulk_output_stream_request",
        Mock(side_effect=ValueError("unsafe output")),
    )
    stream_outcome = await importer._stream_bulk_export_output_rows(
        object(),
        {},
        "https://bulk.example.test/out",
        model=object,
        resource_type="Practitioner",
        per_resource_limit=0,
        timeout=1,
        run_id=None,
        row_batch_handler=None,
        row_batch_size=10,
        retain_rows=False,
    )
    assert stream_outcome[-1] == "unsafe output"


@pytest.mark.asyncio
async def test_accept_checkpoint_failures(monkeypatch):
    identity = bulk_identity()
    monkeypatch.setattr(
        importer,
        "_resolved_bulk_export_status_url",
        Mock(side_effect=ValueError("unsafe status")),
    )
    monkeypatch.setattr(
        importer,
        "_accept_bulk_export_checkpoint",
        AsyncMock(return_value={"state": "accepted"}),
    )
    record_checkpoint_error = AsyncMock()
    monkeypatch.setattr(
        importer, "_record_bulk_export_checkpoint_error", record_checkpoint_error
    )
    accept_outcome = await importer._accept_checkpointed_bulk_start(
        {}, identity, 202, "bad", None
    )
    assert accept_outcome == ({"state": "accepted"}, None, "unsafe status")
    record_checkpoint_error.assert_awaited_once()
    importer._resolved_bulk_export_status_url.side_effect = None
    importer._resolved_bulk_export_status_url.return_value = identity.start_url
    importer._accept_bulk_export_checkpoint.side_effect = RuntimeError("accept lost")
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value={"state": "starting"}),
    )
    assert await importer._accept_checkpointed_bulk_start(
        {}, identity, 202, identity.start_url, None
    ) == ({"state": "starting"}, None, "accept lost")


@pytest.mark.asyncio
async def test_poll_checkpoint_status_error(monkeypatch):
    identity = bulk_identity()
    monkeypatch.setattr(
        importer,
        "_bulk_checkpoint_status_url",
        Mock(side_effect=RuntimeError("cipher invalid")),
    )
    record_checkpoint_error = AsyncMock()
    monkeypatch.setattr(
        importer, "_record_bulk_export_checkpoint_error", record_checkpoint_error
    )
    poll_outcome = await importer._poll_checkpointed_bulk_export_manifest(
        object(),
        {},
        identity,
        {"accepted_at": "2026-07-22T00:00:00Z"},
        timeout=1,
        max_pending_seconds=10,
        runtime_probe=AsyncMock(),
    )
    assert poll_outcome == (None, "cipher invalid", 0)
    record_checkpoint_error.assert_awaited_once()


@pytest.mark.asyncio
async def test_ready_manifest_and_checkpoint_errors(monkeypatch):
    identity = bulk_identity()
    monkeypatch.setattr(importer, "_bulk_export_payload_error", Mock(return_value=None))
    monkeypatch.setattr(
        importer,
        "_bulk_export_manifest_from_payload",
        Mock(side_effect=ValueError("manifest invalid")),
    )
    record_checkpoint_error = AsyncMock()
    monkeypatch.setattr(
        importer, "_record_bulk_export_checkpoint_error", record_checkpoint_error
    )
    manifest_outcome = await importer._persist_ready_bulk_export_manifest(
        {}, identity, {}, polls=2
    )
    assert manifest_outcome == (None, "manifest invalid", 2)
    runtime_probe = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_bulk_manifest_from_checkpoint",
        Mock(side_effect=RuntimeError("checkpoint invalid")),
    )
    checkpoint_outcome = await importer._checkpointed_bulk_export_manifest(
        object(),
        {},
        identity,
        {},
        initial_status_payload=None,
        timeout=1,
        max_pending_seconds=10,
        runtime_probe=runtime_probe,
    )
    assert checkpoint_outcome == (None, "checkpoint invalid", 0)
    runtime_probe.assert_awaited_once()
