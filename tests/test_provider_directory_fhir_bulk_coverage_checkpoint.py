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
    checkpoint_map = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
    }
    cancel_probe = AsyncMock()
    claimed, initial_status_payload, error = (
        await importer._claim_existing_bulk_export_checkpoint(
        identity,
        checkpoint_map,
        ownership_probe=AsyncMock(),
        cancel_probe=cancel_probe,
    )
    )
    assert claimed == checkpoint_map and initial_status_payload is None
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
    claimed, initial_status_payload, error = (
        await importer._claim_existing_bulk_export_checkpoint(
            retry_identity,
            checkpoint_map,
            ownership_probe=AsyncMock(),
        )
    )
    assert claimed == checkpoint_map and initial_status_payload is None
    assert error == "bulk_export_checkpoint_adoption_lost"


@pytest.mark.asyncio
async def test_claim_existing_checkpoint_times_out_or_observes_deleted_row(
    monkeypatch,
):
    identity = _identity(
        owner_run_id="retry-run",
        retry_of_run_id="prior-run",
    )
    checkpoint_map = {
        **_checkpoint(identity),
        "owner_run_id": "prior-run",
        "state": importer.BULK_EXPORT_CHECKPOINT_STARTING,
    }
    conflict = AsyncMock(
        side_effect=RuntimeError("bulk_export_checkpoint_ownership_conflict")
    )
    monkeypatch.setattr(importer, "_adopt_bulk_export_checkpoint", conflict)
    monkeypatch.setattr(importer, "_bulk_checkpoint_lease_seconds", lambda: 0)
    claimed, initial_status_payload, error = (
        await importer._claim_existing_bulk_export_checkpoint(
            identity,
            checkpoint_map,
            ownership_probe=AsyncMock(),
        )
    )
    assert claimed == checkpoint_map and initial_status_payload is None
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
    claimed, initial_status_payload, error = (
        await importer._claim_existing_bulk_export_checkpoint(
            identity,
            {
                **checkpoint_map,
                "state": importer.BULK_EXPORT_CHECKPOINT_RETRYABLE,
            },
            ownership_probe=AsyncMock(),
        )
    )
    assert claimed == {} and initial_status_payload is None
    assert error == "bulk_export_checkpoint_adoption_lost"
