# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime as dt
import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryLocation, ProviderDirectoryPractitioner
from process import provider_directory_dataset_rehydrate as rehydrate
from process import provider_directory_dataset_rehydrate_scope as rehydrate_scope
from process import provider_directory_dataset_rehydrate_types as rehydrate_types


def _mapped_payload():
    return {
        "resource_id": "location-1",
        "status": "active",
        "name": "Example Clinic",
        "city_name": "Louisville",
    }


def _digest_row():
    payload_hash = rehydrate_types._payload_hash(_mapped_payload())
    return {
        "resource_type": "Location",
        "resource_id": "location-1",
        "payload_hash": payload_hash,
    }


def _dataset_hash():
    digest = hashlib.sha256()
    rehydrate_scope._append_digest_rows(digest, [_digest_row()], 0)
    return digest.hexdigest()


def _scope_record():
    published_at = dt.datetime(2026, 7, 20, tzinfo=dt.UTC)
    return {
        "source_endpoint_id": "endpoint-1",
        "source_base": "https://example.com/fhir/",
        "endpoint_id": "endpoint-1",
        "canonical_api_base": "https://example.com/fhir",
        "acquisition_root_run_id": "root-1",
        "dataset_hash": _dataset_hash(),
        "resource_count": 1,
        "status": "published",
        "is_current": True,
        "superseded_at": None,
        "published_at": published_at,
        "publication_metadata_json": {
            "selected_resources": ["Location"],
            "source_ids": ["source-1"],
        },
        "current_count": 1,
    }


def _proof_record():
    return {
        "input_count": 1,
        "typed_count": 1,
        "typed_extra_count": 0,
        "canonical_hash_count": 1,
        "canonical_extra_count": 0,
        "source_edge_count": 1,
        "source_edge_extra_count": 0,
    }


def _checkpoint_record(state="complete"):
    return {
        "endpoint_id": "endpoint-1",
        "dataset_hash": _dataset_hash(),
        "state": state,
        "last_resource_id": "location-1",
        "expected_input_count": 1,
        "input_count": 1,
        "mapped_count": 1,
        "rejected_count": 0,
        "evidence_json": {},
        "error": None,
    }


class _GuardConnection:
    def __init__(self, lock_acquired=True):
        self.lock_acquired = lock_acquired
        self.calls = []

    async def scalar(self, statement, **parameters):
        self.calls.append((statement, parameters))
        if "pg_try_advisory_lock" in statement:
            return self.lock_acquired
        return True


class _RehydrateDatabase:
    def __init__(self, *, checkpoint_record=None, lock_acquired=True):
        self.checkpoint_record = checkpoint_record
        self.connection = _GuardConnection(lock_acquired)
        self.status_calls = []
        self.transaction_count = 0

    @asynccontextmanager
    async def acquire(self):
        yield self.connection

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        yield self

    async def first(self, statement, **_parameters):
        if "source_record.endpoint_id" in statement:
            return _scope_record()
        if rehydrate_types.CHECKPOINT_TABLE in statement:
            return self.checkpoint_record
        if "typed_extra_count" in statement:
            return _proof_record()
        raise AssertionError(statement)

    async def all(self, statement, **_parameters):
        if "SELECT resource_type, resource_id, payload_hash" in statement:
            return [_digest_row()]
        if "SELECT resource_id, payload_hash, payload_json" in statement:
            return [
                {
                    "resource_id": "location-1",
                    "payload_hash": rehydrate_types._payload_hash(_mapped_payload()),
                    "payload_json": _mapped_payload(),
                }
            ]
        raise AssertionError(statement)

    async def scalar(self, statement, **_parameters):
        if "count(*)" in statement:
            return 1
        raise AssertionError(statement)

    async def status(self, statement, **parameters):
        self.status_calls.append((statement, parameters))
        return "INSERT 0 1"


def _request(**changes):
    request_fields_by_name = {
        "source_id": "source-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "root-1",
        "owner_run_id": "owner-1",
        "resource_types": ("Location",),
        "batch_size": 10,
    }
    request_fields_by_name.update(changes)
    return rehydrate_types.RehydrationRequest(**request_fields_by_name)


def _runtime(database, *, progress_callback=None):
    async def upsert_batch(_model, typed_rows, _scope):
        return len(typed_rows)

    return rehydrate_types.RehydrationRuntime(
        database=database,
        schema="mrf",
        models_by_type={"Location": ProviderDirectoryLocation},
        upsert_batch=upsert_batch,
        cancel_check=AsyncMock(),
        progress_callback=progress_callback,
    )


def _scope(**changes):
    scope_fields_by_name = {
        "source_id": "source-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "root-1",
        "endpoint_id": "endpoint-1",
        "canonical_api_base": "https://example.com/fhir",
        "dataset_hash": _dataset_hash(),
        "resource_count": 1,
        "resource_types": ("Location",),
        "publication_metadata_hash": "a" * 64,
        "published_at": dt.datetime.now(dt.UTC),
    }
    scope_fields_by_name.update(changes)
    return rehydrate_types.DatasetScope(**scope_fields_by_name)


def _context(database, *, runtime=None, scope=None, expected_count=1):
    return rehydrate_types.ResourceContext(
        runtime or _runtime(database),
        _request(),
        scope or _scope(),
        "Location",
        ProviderDirectoryLocation,
        expected_count,
    )


@pytest.mark.asyncio
async def test_rehydrate_executes_guard_digest_batch_proof_and_progress():
    database = _RehydrateDatabase()
    progress_callback = AsyncMock()

    result = await rehydrate.rehydrate_current_dataset(
        _runtime(database, progress_callback=progress_callback),
        _request(),
    )

    assert result["dataset_id"] == "dataset-1"
    assert result["resources"]["Location"]["state"] == "complete"
    assert database.transaction_count == 2
    assert [call[1]["state"] for call in database.status_calls] == [
        "running",
        "running",
        "complete",
    ]
    progress_callback.assert_awaited_once()
    assert "pg_advisory_unlock" in database.connection.calls[-1][0]


@pytest.mark.asyncio
async def test_rehydrate_reuses_complete_checkpoint_without_rewriting():
    database = _RehydrateDatabase(checkpoint_record=_checkpoint_record())

    result = await rehydrate.rehydrate_current_dataset(
        _runtime(database),
        _request(),
    )

    assert result["resources"]["Location"]["reused_complete_checkpoint"] is True
    assert database.transaction_count == 0
    assert database.status_calls == []


@pytest.mark.asyncio
async def test_dataset_guard_rejects_busy_scope_and_still_unlocks_after_error():
    busy_database = _RehydrateDatabase(lock_acquired=False)
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="scope_busy"):
        await rehydrate.rehydrate_current_dataset(_runtime(busy_database), _request())

    database = _RehydrateDatabase()
    database.first = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        await rehydrate.rehydrate_current_dataset(_runtime(database), _request())
    assert "pg_advisory_unlock" in database.connection.calls[-1][0]


@pytest.mark.asyncio
async def test_resume_requires_exact_prefix_and_running_state():
    database = _RehydrateDatabase()
    runtime = _runtime(database)
    scope = _scope()
    context = rehydrate_types.ResourceContext(
        runtime,
        _request(),
        scope,
        "Location",
        ProviderDirectoryLocation,
        1,
    )
    checkpoint = rehydrate_types.RehydrationCheckpoint(
        "interrupted", "location-1", 1, 1, 1, 0, error="old"
    )

    resumed = await rehydrate._resumable_checkpoint(context, checkpoint)
    assert resumed.state == "running"
    assert resumed.error is None

    database.scalar = AsyncMock(return_value=0)
    assert (await rehydrate._resumable_checkpoint(context, checkpoint)).input_count == 0
    assert (await rehydrate._resumable_checkpoint(context, None)).last_resource_id is None
    wrong_state = replace(checkpoint, state="complete")
    assert (await rehydrate._resumable_checkpoint(context, wrong_state)).input_count == 0


@pytest.mark.parametrize(
    "rehydration_request",
    [
        _request(source_id=""),
        _request(batch_size=0),
        _request(batch_size=rehydrate_types.MAX_BATCH_SIZE + 1),
    ],
)
def test_request_validation_rejects_incomplete_identity_and_batch_bounds(
    rehydration_request,
):
    with pytest.raises(rehydrate_types.DatasetRehydrationError):
        rehydrate._validate_request(rehydration_request)


@pytest.mark.asyncio
async def test_batch_processing_rejects_bad_payload_and_upsert_count_mismatch(monkeypatch):
    database = _RehydrateDatabase()
    context = _context(database)
    checkpoint = rehydrate._empty_checkpoint(context)
    bad_record_fields_by_name = {
        "resource_id": "location-1",
        "payload_hash": "0" * 64,
        "payload_json": _mapped_payload(),
    }
    monkeypatch.setattr(rehydrate, "_assert_scope_unchanged", AsyncMock())
    monkeypatch.setattr(
        rehydrate,
        "_read_retained_batch",
        AsyncMock(return_value=[bad_record_fields_by_name]),
    )
    rejected = await rehydrate._process_one_batch(context, checkpoint)
    assert rejected.state == "rejected"
    assert rejected.evidence_by_name["rejected_in_batch"] == 1

    mismatch_runtime = replace(_runtime(database), upsert_batch=AsyncMock(return_value=0))
    mismatch_context = _context(database, runtime=mismatch_runtime)
    valid_record_fields_by_name = {
        "resource_id": "location-1",
        "payload_hash": rehydrate_types._payload_hash(_mapped_payload()),
        "payload_json": _mapped_payload(),
    }
    monkeypatch.setattr(
        rehydrate,
        "_read_retained_batch",
        AsyncMock(return_value=[valid_record_fields_by_name]),
    )
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="upsert_count_mismatch"):
        await rehydrate._process_one_batch(mismatch_context, checkpoint)


@pytest.mark.asyncio
async def test_rejected_batch_aborts_resource_loop_and_empty_page_fails(monkeypatch):
    database = _RehydrateDatabase()
    context = _context(database, expected_count=2)
    checkpoint = rehydrate._empty_checkpoint(context)
    rejected = replace(checkpoint, state="rejected", input_count=1, rejected_count=1)
    monkeypatch.setattr(rehydrate, "_process_one_batch", AsyncMock(return_value=rejected))
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="payload_rejected"):
        await rehydrate._process_resource_batches(context, checkpoint)

    database.all = AsyncMock(return_value=[])
    cursor_checkpoint = replace(checkpoint, last_resource_id="location-1")
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="input_ended_early"):
        await rehydrate._read_retained_batch(context, cursor_checkpoint)
    assert database.all.await_args.kwargs["cursor"] == "location-1"


@pytest.mark.asyncio
async def test_finalize_rejects_incomplete_proof_and_marks_interruptions(monkeypatch):
    database = _RehydrateDatabase()
    context = _context(database)
    checkpoint = rehydrate_types.RehydrationCheckpoint(
        "running", "location-1", 1, 1, 1, 0
    )
    incomplete_proof = rehydrate_types.ResourceProof(1, 0, 0, 0, 0, 0, 0)
    monkeypatch.setattr(rehydrate, "_assert_scope_unchanged", AsyncMock())
    monkeypatch.setattr(
        rehydrate,
        "_load_resource_proof",
        AsyncMock(return_value=incomplete_proof),
    )
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="proof_failed"):
        await rehydrate._finalize_resource(context, checkpoint)

    save_checkpoint = AsyncMock()
    monkeypatch.setattr(rehydrate, "_save_checkpoint", save_checkpoint)
    monkeypatch.setattr(rehydrate, "_load_checkpoint", AsyncMock(return_value=None))
    await rehydrate._mark_interrupted(context, RuntimeError("first"))
    monkeypatch.setattr(
        rehydrate,
        "_load_checkpoint",
        AsyncMock(return_value=replace(checkpoint, state="complete")),
    )
    await rehydrate._mark_interrupted(context, RuntimeError("second"))
    monkeypatch.setattr(rehydrate, "_load_checkpoint", AsyncMock(return_value=checkpoint))
    await rehydrate._mark_interrupted(context, RuntimeError("third"))
    assert save_checkpoint.await_args.args[1].state == "interrupted"
    monkeypatch.setattr(
        rehydrate,
        "_load_checkpoint",
        AsyncMock(side_effect=RuntimeError("checkpoint unavailable")),
    )
    await rehydrate._mark_interrupted(context, RuntimeError("fourth"))


@pytest.mark.asyncio
async def test_scope_drift_and_no_progress_callback_are_safe(monkeypatch):
    database = _RehydrateDatabase()
    context = _context(database)
    drifted_scope = replace(context.scope, dataset_hash="f" * 64)
    monkeypatch.setattr(
        rehydrate,
        "_load_dataset_scope",
        AsyncMock(return_value=drifted_scope),
    )
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="scope_changed"):
        await rehydrate._assert_scope_unchanged(
            context.runtime,
            context.request,
            context.scope,
        )
    await rehydrate._report_progress(context, rehydrate._empty_checkpoint(context))


@pytest.mark.parametrize(
    ("model", "resource_id", "payload", "expected_reason"),
    [
        (ProviderDirectoryLocation, "", _mapped_payload(), "payload_hash_mismatch"),
        (
            ProviderDirectoryLocation,
            "different-id",
            _mapped_payload(),
            "payload_provenance_invalid",
        ),
        (
            ProviderDirectoryLocation,
            "location-1",
            {**_mapped_payload(), "unknown": "value"},
            "payload_unknown_field",
        ),
        (
            ProviderDirectoryLocation,
            "location-1",
            {**_mapped_payload(), "status": 7},
            "payload_column_type_invalid",
        ),
        (
            ProviderDirectoryPractitioner,
            "practitioner-1",
            {"resource_id": "practitioner-1", "active": "yes"},
            "payload_column_type_invalid",
        ),
        (
            ProviderDirectoryPractitioner,
            "practitioner-1",
            {"resource_id": "practitioner-1", "active": True},
            None,
        ),
    ],
)
def test_payload_validation_rejects_each_unsafe_shape(
    model,
    resource_id,
    payload,
    expected_reason,
):
    assert (
        rehydrate._validate_payload(
            model,
            resource_id,
            rehydrate_types._payload_hash(payload),
            payload,
        )
        == expected_reason
    )
