# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import hashlib
import importlib
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")


def _identity() -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-atomic",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-atomic",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-atomic",
        owner_run_id="run-atomic",
        retry_of_run_id=None,
        endpoint_id="endpoint-atomic",
        dataset_id="dataset-atomic",
        start_url="https://providerdirectory.api.aetna.com/fhir/$export",
        start_url_hash="a" * 64,
    )


def _retry_identity() -> importer.BulkExportCheckpointIdentity:
    return dataclasses.replace(
        _identity(),
        owner_run_id="run-atomic-retry",
        retry_of_run_id="run-atomic",
    )


def _source() -> dict:
    return importer._source_row_from_seed(
        importer._aetna_provider_directory_data_seed_rows(source_query="Aetna")[0]
    )


def _manifest(output_count: int = 1) -> importer.BulkExportManifest:
    return importer._bulk_export_manifest_from_payload(
        {
            "transactionTime": "2026-07-20T00:00:00Z",
            "request": _identity().start_url,
            "requiresAccessToken": False,
            "output": [
                {
                    "type": "Practitioner",
                    "url": (
                        "https://storage.googleapis.com/aetna/"
                        f"part-{output_index}.ndjson?sig=x"
                    ),
                }
                for output_index in range(output_count)
            ],
        },
        "Practitioner",
        expected_request_url=_identity().start_url,
    )


def _output_checkpoint() -> dict:
    manifest_output = _manifest().outputs[0]
    return {
        "output_id": importer._bulk_manifest_output_id(
            _identity().checkpoint_id,
            manifest_output,
        ),
        "state": importer.BULK_EXPORT_OUTPUT_PENDING,
        "rows_written": 0,
        "committed_bytes": 0,
        "content_length_bytes": None,
        "etag_ciphertext": None,
        "etag_hash": None,
        "validator_checked_at": None,
    }


def _stream_options(concurrency: int = 1) -> importer.BulkExportStreamOptions:
    return importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=True,
        output_concurrency=concurrency,
    )


@pytest.mark.asyncio
async def test_terminal_transition_scrubs_capabilities_in_one_statement(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._record_bulk_export_checkpoint_error(
        _identity(),
        "bulk_export_manifest_mismatch",
        terminal=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "cleared_outputs AS" in statement
    assert "output_url_ciphertext = NULL" in statement
    assert "status_url_ciphertext = CASE" in statement
    assert "sum(output.rows_written)" in statement
    assert status.await_args.kwargs["terminal"] is True


@pytest.mark.asyncio
async def test_completion_scrubs_capabilities_in_transition_statement(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(return_value={"state": importer.BULK_EXPORT_CHECKPOINT_COMPLETE}),
    )

    await importer._complete_bulk_export_checkpoint(
        _identity(),
        require_validators=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "cleared_outputs AS" in statement
    assert "etag_ciphertext = NULL" in statement
    assert "manifest_ciphertext = NULL" in statement
    assert "sum(output.rows_written)" in statement


@pytest.mark.asyncio
async def test_output_completion_scrubs_and_refreshes_total_atomically(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._complete_bulk_export_output(
        _identity(),
        "output-atomic",
        7,
        101,
        require_validator=True,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "WITH completed_output AS" in statement
    assert "output_url_ciphertext = NULL" in statement
    assert "etag_ciphertext = NULL" in statement
    assert "sum(" in statement
    assert status.await_args.kwargs["rows_written"] == 7


@pytest.mark.asyncio
async def test_output_failure_refreshes_parent_total_atomically(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._record_bulk_export_output_error(
        _identity(),
        "output-atomic",
        7,
        101,
        "bulk_export_manifest_mismatch",
        record_checkpoint=False,
    )

    status.assert_awaited_once()
    statement = status.await_args.args[0]
    assert "WITH failed_output AS" in statement
    assert "WHEN output.output_id = :output_id" in statement
    assert "THEN :rows_written" in statement
    assert "output_url_ciphertext = CASE" in statement
    assert "etag_ciphertext = CASE" in statement
    assert status.await_args.kwargs["terminal"] is True


@pytest.mark.asyncio
async def test_stream_output_error_probes_ownership_before_failure_write(
    monkeypatch,
):
    events: list[str] = []

    async def ownership_probe():
        events.append("probe")

    async def record_error(*_args, **_kwargs):
        events.append("record")

    monkeypatch.setattr(importer, "_record_bulk_export_output_error", record_error)
    options = dataclasses.replace(
        _stream_options(),
        ownership_probe=ownership_probe,
    )

    error = await importer._finish_checkpointed_bulk_output(
        _identity(),
        "output-atomic",
        options,
        importer.BulkExportStreamState(),
        _source(),
        {"base_rows_written": 0, "committed_bytes": 0},
        ([], 0, 0, False, "bulk_export_output_http_503"),
    )

    assert error == "bulk_export_output_http_503"
    assert events == ["probe", "record"]


@pytest.mark.asyncio
async def test_stream_output_fencing_loss_skips_failure_write(monkeypatch):
    record_error = AsyncMock()
    monkeypatch.setattr(importer, "_record_bulk_export_output_error", record_error)
    options = dataclasses.replace(
        _stream_options(),
        ownership_probe=AsyncMock(
            side_effect=RuntimeError("bulk_export_checkpoint_worker_guard_lost")
        ),
    )

    with pytest.raises(
        RuntimeError,
        match="bulk_export_checkpoint_worker_guard_lost",
    ):
        await importer._finish_checkpointed_bulk_output(
            _identity(),
            "output-atomic",
            options,
            importer.BulkExportStreamState(),
            _source(),
            {"base_rows_written": 0, "committed_bytes": 0},
            ([], 0, 0, False, "bulk_export_output_http_503"),
        )

    record_error.assert_not_awaited()


@pytest.mark.asyncio
async def test_terminal_repair_is_state_scoped_not_retry_owner_scoped(monkeypatch):
    status = AsyncMock(return_value=1)
    monkeypatch.setattr(importer.db, "status", status)

    await importer._repair_terminal_bulk_export_checkpoint(
        _retry_identity(),
        importer.BULK_EXPORT_CHECKPOINT_FAILED,
    )

    statement = status.await_args.args[0]
    assert "state = :terminal_state" in statement
    assert "owner_run_id" not in statement
    assert "sum(output.rows_written)" in statement
    assert "output_url_ciphertext = NULL" in statement
    assert status.await_args.kwargs["terminal_state"] == (
        importer.BULK_EXPORT_CHECKPOINT_FAILED
    )


@pytest.mark.asyncio
async def test_terminal_repair_fails_closed_when_checkpoint_changes(monkeypatch):
    monkeypatch.setattr(importer.db, "status", AsyncMock(return_value=0))

    with pytest.raises(
        RuntimeError,
        match="bulk_export_terminal_checkpoint_repair_lost",
    ):
        await importer._repair_terminal_bulk_export_checkpoint(
            _retry_identity(),
            importer.BULK_EXPORT_CHECKPOINT_FAILED,
        )


def _atomic_fetch_options():
    return importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run-atomic",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
    )


@pytest.mark.asyncio
async def test_terminal_reload_repairs_capability_scrubbing(monkeypatch):
    """Terminal reload repairs without requiring ownership of the old run."""
    repair_terminal = AsyncMock()
    ownership_probe = AsyncMock(
        side_effect=AssertionError("terminal reload must not require retry ownership")
    )
    terminal_checkpoint_by_field = {
        "state": importer.BULK_EXPORT_CHECKPOINT_FAILED,
        "rows_written": 7,
        "error": "bulk_export_manifest_mismatch",
    }
    repaired_checkpoint_by_field = {
        **terminal_checkpoint_by_field,
        "rows_written": 11,
    }

    @contextlib.asynccontextmanager
    async def client_session():
        yield object()

    monkeypatch.setattr(importer, "_bulk_client_session", client_session)
    monkeypatch.setattr(
        importer,
        "_load_or_start_checkpointed_bulk_export",
        AsyncMock(
            return_value=(
                terminal_checkpoint_by_field,
                None,
                None,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_repair_terminal_bulk_export_checkpoint",
        repair_terminal,
    )
    monkeypatch.setattr(
        importer,
        "_load_bulk_export_checkpoint",
        AsyncMock(return_value=repaired_checkpoint_by_field),
    )
    fetch_result = await importer._fetch_owned_checkpointed_bulk_resource_rows(
        {"source_id": "aetna-provider-directory-data"},
        _retry_identity(),
        ProviderDirectoryPractitioner,
        _atomic_fetch_options(),
        ownership_probe,
    )

    assert fetch_result is not None
    assert fetch_result.error == "bulk_export_manifest_mismatch"
    assert fetch_result.rows_fetched == 11
    repair_terminal.assert_awaited_once_with(
        _retry_identity(),
        importer.BULK_EXPORT_CHECKPOINT_FAILED,
    )
    ownership_probe.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_checkpoint_is_reserved_before_ownership_probe(monkeypatch):
    """A new reservation must exist before its advisory ownership probe."""
    events: list[str] = []
    identity = _identity()
    active_checkpoint_by_field = {
        "owner_run_id": identity.owner_run_id,
        "state": importer.BULK_EXPORT_CHECKPOINT_ACCEPTED,
        "rows_written": 0,
    }

    @contextlib.asynccontextmanager
    async def client_session():
        yield object()

    async def load_checkpoint(_identity):
        events.append("load")
        return {}

    async def reserve_checkpoint(_identity):
        events.append("reserve")
        return active_checkpoint_by_field, True

    async def start_checkpoint(*_args, **_kwargs):
        events.append("start")
        return active_checkpoint_by_field, None, None

    async def ownership_probe():
        events.append("ownership")
    async def stop_after_claim(*_args, **_kwargs):
        events.append("manifest")
        return None, "bulk_export_test_stop", 0
    monkeypatch.setattr(importer, "_bulk_client_session", client_session)
    monkeypatch.setattr(importer, "_load_bulk_export_checkpoint", load_checkpoint)
    monkeypatch.setattr(importer, "_reserve_bulk_export_checkpoint", reserve_checkpoint)
    monkeypatch.setattr(importer, "_start_checkpointed_bulk_export", start_checkpoint)
    monkeypatch.setattr(importer, "_bulk_checkpoint_primary_secret", lambda: "key")
    monkeypatch.setattr(importer, "_checkpointed_bulk_export_manifest", stop_after_claim)
    fetch_result = await importer._fetch_owned_checkpointed_bulk_resource_rows(
        _source(),
        identity,
        ProviderDirectoryPractitioner,
        importer.BulkExportFetchOptions(
            timeout=3,
            run_id=identity.owner_run_id,
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=1,
            retain_rows=False,
        ),
        ownership_probe,
    )
    assert fetch_result is not None
    assert fetch_result.error == "bulk_export_test_stop"
    assert events == [
        "load",
        "reserve",
        "ownership",
        "start",
        "ownership",
        "manifest",
    ]

@pytest.mark.asyncio
async def test_retry_checkpoint_is_adopted_before_ownership_probe(monkeypatch):
    """Retry lineage adopts the checkpoint before asserting new ownership."""
    events: list[str] = []
    identity = _retry_identity()
    prior_checkpoint_by_field = {
        "owner_run_id": "run-atomic",
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
        "rows_written": 3,
    }
    adopted_checkpoint_by_field = {
        **prior_checkpoint_by_field,
        "owner_run_id": identity.owner_run_id,
    }

    @contextlib.asynccontextmanager
    async def client_session():
        yield object()
    async def load_checkpoint(_identity):
        events.append("load")
        return prior_checkpoint_by_field
    async def adopt_checkpoint(_identity):
        events.append("adopt")
        return adopted_checkpoint_by_field
    async def ownership_probe():
        events.append("ownership")
    async def cancel_probe(_ctx, _task, _deadline_at):
        events.append("cancel")
    async def stop_after_claim(*_args, **_kwargs):
        events.append("manifest")
        return None, "bulk_export_test_stop", 0
    monkeypatch.setattr(importer, "_bulk_client_session", client_session)
    monkeypatch.setattr(importer, "_load_bulk_export_checkpoint", load_checkpoint)
    monkeypatch.setattr(importer, "_adopt_bulk_export_checkpoint", adopt_checkpoint)
    monkeypatch.setattr(importer, "_bulk_cancel_probe", cancel_probe)
    monkeypatch.setattr(importer, "_checkpointed_bulk_export_manifest", stop_after_claim)
    fetch_result = await importer._fetch_owned_checkpointed_bulk_resource_rows(
        _source(),
        identity,
        ProviderDirectoryPractitioner,
        importer.BulkExportFetchOptions(
            timeout=3,
            run_id=identity.owner_run_id,
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=1,
            retain_rows=False,
        ),
        ownership_probe,
    )
    assert fetch_result is not None
    assert fetch_result.error == "bulk_export_test_stop"
    assert events == [
        "cancel",
        "load",
        "cancel",
        "adopt",
        "ownership",
        "ownership",
        "cancel",
        "manifest",
    ]
