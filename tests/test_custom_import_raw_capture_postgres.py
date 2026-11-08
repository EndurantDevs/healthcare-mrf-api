# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native proofs for original-byte retention and shared dataset quotas."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import uuid
from dataclasses import asdict, dataclass
from decimal import Decimal
from io import BytesIO
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from asyncpg import Connection
from sqlalchemy import func, literal_column, select, update

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPublicationEvent,
)
from db.models.custom_import_raw_capture import CustomImportCaptureRawObject
from process.custom_import import capture_retention, capture_store, cli, https_candidate, raw_capture_store, runner
from process.custom_import.capture import SealedCapture, capture_stream
from process.custom_import.capture_pending import PendingCaptureRequest, append_pending_parquet_part
from process.custom_import.capture_store import CaptureBundleConflict, CapturePayloadUnavailable
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.definition_store import register_definition
from process.custom_import.runner_types import LeaseAuthorityLost
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests import custom_import_installed_acquisition as installed_support
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_capture_pending_postgres import _start_attempt
from tests.test_custom_import_capture_store_postgres import _parquet_definition, _seed_parquet_case
from tests.test_custom_import_definition import _raw_definition
from tests.test_custom_import_https_candidate_postgres import _install_source
from tests.test_custom_import_https_candidate_postgres import forbid_https_transport as forbid_https_transport
from tests.test_custom_import_installed_acquisition_postgres import _searchable_registration
from tests.test_custom_import_installed_https_postgres import _https_command, _register_fresh_case
from tests.test_custom_import_installed_operator_postgres import installed_application as installed_application

_MAX_RETAINED_BYTES = 1024 * 1024


def _identity_kwargs(identity):
    return dict(
        dataset_id=identity.dataset_id,
        definition_revision_id=identity.definition_revision_id,
        schema_revision_id=identity.schema_revision_id,
    )


def _sealed_originals(definition, originals_by_stream, snapshot="synthetic-raw-snapshot"):
    return tuple(
        capture_stream(BytesIO(originals_by_stream[stream.stream_id]), stream, source_snapshot_token=snapshot)
        for stream in definition.source_streams
    )


async def _seed_raw_case(case):
    definition = CustomImportDefinition.from_mapping(_raw_definition())
    originals_by_stream = {
        "providers": b'"Provider ID","Provider Name"\n"1234567893","Synthetic Provider"\n',
        "rates": gzip.compress(b'{"Provider ID":"1234567893","Service Code":"A100","Amount":12}\n', mtime=0),
    }
    async with case.sessions() as session, session.begin():
        seed = await register_definition(session, f"synthetic_raw_{uuid.uuid4().hex[:16]}", definition)
    return seed, _sealed_originals(definition, originals_by_stream)


async def _register_raw(session, seed, captures, maximum_retained_bytes=_MAX_RETAINED_BYTES):
    return await raw_capture_store.register_raw_capture_bundle(
        session,
        **_identity_kwargs(seed),
        captures=captures,
        maximum_dataset_retained_bytes=maximum_retained_bytes,
    )


async def _row_counts(case):
    async with case.sessions() as session:
        return tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (CustomImportCaptureBundle, CustomImportCapture, CustomImportCaptureRawObject)
            ]
        )


async def test_translated_copy_replays_exact_gzip_and_csv_originals():
    async with isolated_publication_case() as case:
        seed, captures = await _seed_raw_case(case)
        async with case.sessions() as session, session.begin():
            registered = await _register_raw(session, seed, captures)
        async with case.sessions() as session:
            replayed = await raw_capture_store.load_replayable_raw_bundle(
                session, capture_bundle_id=registered.capture_bundle_id, **_identity_kwargs(seed)
            )
            stored_relations = await session.scalars(
                select(literal_column("tableoid::regclass::text")).select_from(CustomImportCaptureRawObject)
            )
            assert set(stored_relations) == {f"{case.schema_name}.custom_import_capture_raw_object"}
            assert replayed == captures
            assert [(capture.manifest.format, capture.manifest.compression) for capture in replayed] == [
                ("csv", "none"),
                ("ndjson", "gzip"),
            ]
        assert await _row_counts(case) == (1, 2, 2)


async def test_exact_retry_neither_copies_nor_rechecks_quota(monkeypatch):
    async with isolated_publication_case() as case:
        seed, captures = await _seed_raw_case(case)
        async with case.sessions() as session, session.begin():
            registered = await _register_raw(session, seed, captures)

        async def forbidden(*_args, **_kwargs):
            raise AssertionError("exact reuse must not copy or charge again")

        monkeypatch.setattr(Connection, "copy_records_to_table", forbidden)
        monkeypatch.setattr(capture_retention, "validate_dataset_retained_bytes", forbidden)
        async with case.sessions() as session, session.begin():
            replayed = await _register_raw(session, seed, captures, maximum_retained_bytes=0)
        assert not replayed.created and replayed.capture_bundle_id == registered.capture_bundle_id
        assert await _row_counts(case) == (1, 2, 2)


@pytest.mark.parametrize("failure_type", (RuntimeError, asyncio.CancelledError))
async def test_failure_after_copy_rolls_back_every_new_row(monkeypatch, failure_type):
    async with isolated_publication_case() as case:
        seed, captures = await _seed_raw_case(case)
        original_loader = raw_capture_store.load_replayable_raw_bundle

        async def fail_after_readback(session, **identifiers):
            assert await original_loader(session, **identifiers) == captures
            raise failure_type("synthetic interruption after native COPY")

        monkeypatch.setattr(raw_capture_store, "load_replayable_raw_bundle", fail_after_readback)
        with pytest.raises(failure_type):
            async with case.sessions() as session, session.begin():
                await _register_raw(session, seed, captures)
        assert await _row_counts(case) == (0, 0, 0)


async def test_metadata_only_bundle_cannot_be_backfilled():
    async with isolated_publication_case() as case:
        seed, captures = await _seed_raw_case(case)
        receipts = tuple(raw_capture_store._receipt(capture.manifest) for capture in captures)
        async with case.sessions() as session, session.begin():
            await capture_store.register_capture_bundle(session, **_identity_kwargs(seed), receipts=receipts)
        with pytest.raises(CapturePayloadUnavailable):
            async with case.sessions() as session, session.begin():
                await _register_raw(session, seed, captures)
        assert await _row_counts(case) == (1, 2, 0)


@dataclass(frozen=True)
class _MixedCapture:
    request: PendingCaptureRequest
    pending_bundle_id: int
    originals: tuple[SealedCapture, ...]
    parquet_part: SealedCapture
    record_count: int
    arrow_byte_count: int


def _mixed_policy(parquet_part, record_count, arrow_byte_count):
    byte_count = len(parquet_part.payload)
    manifest_bytes = len(canonical_json(asdict(parquet_part.manifest)).encode("utf-8"))
    assert byte_count > manifest_bytes
    stream_budget_by_name = dict(
        maximum_parts=1,
        maximum_compressed_bytes=byte_count,
        maximum_decoded_bytes=byte_count,
        maximum_arrow_bytes=arrow_byte_count,
        maximum_records=record_count,
        maximum_manifest_bytes=manifest_bytes,
    )
    return SegmentedCapturePolicy.from_mapping(
        dict(
            contract="custom-import/segmented-capture-policy/v1",
            part_limits=dict(
                maximum_compressed_bytes=byte_count,
                maximum_decoded_bytes=byte_count,
                maximum_record_bytes=256,
                maximum_records=record_count,
                maximum_fields_per_record=4,
                read_chunk_bytes=4096,
            ),
            stream_budget=stream_budget_by_name,
            bundle_budget={name: 2 * limit for name, limit in stream_budget_by_name.items()},
            maximum_part_arrow_bytes=arrow_byte_count,
            maximum_part_manifest_bytes=manifest_bytes,
            maximum_dataset_retained_bytes=2 * (byte_count + manifest_bytes),
            acquisition_deadline_seconds=60,
        )
    )


async def _mixed_case(case):
    definition = _parquet_definition()
    table = pa.table(
        {"ordinal": list(range(40)), "label": [hashlib.sha256(str(index).encode()).hexdigest() for index in range(40)]}
    )
    destination = BytesIO()
    pq.write_table(table, destination)
    originals_by_stream = {stream.stream_id: destination.getvalue() for stream in definition.source_streams}
    originals = _sealed_originals(definition, originals_by_stream)
    snapshot = "synthetic-pending-snapshot"
    parquet_part = _sealed_originals(definition, originals_by_stream, snapshot)[0]
    policy = _mixed_policy(parquet_part, table.num_rows, table.nbytes)
    seed = await _seed_parquet_case(case)
    request, registered = await _start_attempt(case, policy=policy, seed=seed, snapshot=snapshot)
    return _MixedCapture(request, registered.capture_bundle_id, originals, parquet_part, table.num_rows, table.nbytes)


async def _write_kind(session, mixed, kind):
    if kind == "raw":
        return await _register_raw(
            session, mixed.request, mixed.originals, mixed.request.policy.maximum_dataset_retained_bytes
        )
    return await append_pending_parquet_part(
        session,
        request=mixed.request,
        capture_bundle_id=mixed.pending_bundle_id,
        capture=mixed.parquet_part,
        ordinal=1,
        record_count=mixed.record_count,
        arrow_byte_count=mixed.arrow_byte_count,
    )


async def _contending_write(case, mixed, kind, ready):
    async with case.sessions() as session, session.begin():
        isolation, retained_bytes = (
            await session.execute(capture_retention._retained_bytes_statement(mixed.request.dataset_id, 0))
        ).one()
        assert isolation == "read committed" and retained_bytes == 0
        ready.set_result(await session.scalar(select(func.pg_backend_pid())))
        return await _write_kind(session, mixed, kind)


async def _wait_for_blocker(case, waiter_pid, owner_pid):
    async with asyncio.timeout(5), case.sessions() as session:
        while owner_pid not in await session.scalar(select(func.pg_blocking_pids(waiter_pid))):
            await asyncio.sleep(0.01)


async def _contend_mixed_writes(case, mixed, first_kind):
    waiting_task = None
    ready = asyncio.get_running_loop().create_future()
    second_kind = "parquet" if first_kind == "raw" else "raw"
    try:
        async with case.sessions() as session, session.begin():
            owner_pid = await session.scalar(select(func.pg_backend_pid()))
            await _write_kind(session, mixed, first_kind)
            waiting_task = asyncio.create_task(_contending_write(case, mixed, second_kind, ready))
            waiter_pid = await asyncio.wait_for(ready, timeout=5)
            await _wait_for_blocker(case, waiter_pid, owner_pid)
        with pytest.raises(CaptureBundleConflict, match="quota"):
            await asyncio.wait_for(waiting_task, timeout=5)
    finally:
        if waiting_task is not None:
            if not waiting_task.done():
                waiting_task.cancel()
            await asyncio.gather(waiting_task, return_exceptions=True)


@pytest.mark.parametrize("first_kind", ("raw", "parquet"))
async def test_mixed_writers_refresh_quota_after_dataset_lock(first_kind):
    async with isolated_publication_case() as case:
        mixed = await _mixed_case(case)
        await _contend_mixed_writes(case, mixed, first_kind)
        expected_raw_count = 2 if first_kind == "raw" else 0
        expected_part_count = 0 if first_kind == "raw" else 1
        async with case.sessions() as session:
            assert (
                await session.scalar(select(func.count()).select_from(CustomImportCaptureRawObject))
                == expected_raw_count
            )
            assert (
                await session.scalar(select(func.count()).select_from(CustomImportCaptureParquetPart))
                == expected_part_count
            )
            isolation, retained_bytes = (
                await session.execute(capture_retention._retained_bytes_statement(mixed.request.dataset_id, 0))
            ).one()
            expected_bytes = sum(len(capture.payload) for capture in mixed.originals)
            if first_kind == "parquet":
                expected_bytes = len(mixed.parquet_part.payload) + len(
                    canonical_json(asdict(mixed.parquet_part.manifest)).encode()
                )
            assert isolation == "read committed"
            assert retained_bytes == expected_bytes <= mixed.request.policy.maximum_dataset_retained_bytes


async def test_numeric_quota_rejects_sum_above_bigint_without_database_overflow():
    async with isolated_publication_case() as case:
        seed, captures = await _seed_raw_case(case)
        async with case.sessions() as session, session.begin():
            await _register_raw(session, seed, captures)
        original_bytes = sum(len(capture.payload) for capture in captures)
        maximum = 2**63 - 1
        async with case.sessions() as session, session.begin():
            await capture_store._lock_dataset(session, capture_store._identity(**_identity_kwargs(seed)))
            await capture_retention.validate_dataset_retained_bytes(
                session, dataset_id=seed.dataset_id, new_byte_count=0, maximum_retained_bytes=original_bytes
            )
            isolation, retained_bytes = (
                await session.execute(capture_retention._retained_bytes_statement(seed.dataset_id, maximum))
            ).one()
            assert isolation == "read committed" and retained_bytes == Decimal(maximum) + original_bytes
            with pytest.raises(CaptureBundleConflict, match="quota"):
                await capture_retention.validate_dataset_retained_bytes(
                    session, dataset_id=seed.dataset_id, new_byte_count=maximum, maximum_retained_bytes=maximum
                )
            assert await session.scalar(select(1)) == 1


def _original_cli_request():
    definition_document = _searchable_registration()["definition"]
    definition_document["streams"][0].update(format="csv", compression="gzip")
    original = ("ROOT_NPI\n" + "\n".join(installed_support.NPIS) + "\n").encode()
    payload = gzip.compress(original, mtime=0)
    request_by_key = {
        "definition": definition_document,
        "objects": [
            {
                "stream_id": "root_source",
                "url": "https://example.test/root.csv.gz",
                "expected_sha256": hashlib.sha256(payload).hexdigest(),
                "expected_bytes": len(payload),
            }
        ],
        "source_snapshot_token": "synthetic-original-snapshot",
        "idempotency_key": "synthetic-original-execution",
        "lease_token": "synthetic-original-owner",
        "limits": {},
        "timeout_seconds": 15,
        "complete_scope": True,
        "maximum_dataset_retained_bytes": _MAX_RETAINED_BYTES,
    }
    return request_by_key, {"root_source": payload}


async def _interrupt_bound_candidate(session_factory, candidate, grant, _admitted):
    async with session_factory() as session, session.begin():
        execution = await session.get(CustomImportExecution, candidate.execution_id)
        assert execution.capture_bundle_id is not None
        changed = await session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == grant.execution_id, CustomImportLease.fence == grant.fence)
            .values(expires_at=func.clock_timestamp())
        )
        assert changed.rowcount == 1
    raise LeaseAuthorityLost("synthetic interruption before claimed runner")


async def _bound_raw_state(case, registered, execution_id, payloads_by_stream):
    async with case.sessions() as session:
        execution = await session.get(CustomImportExecution, execution_id)
        lease = await session.get(CustomImportLease, execution_id)
        replayed = await raw_capture_store.load_replayable_raw_bundle(
            session,
            capture_bundle_id=execution.capture_bundle_id,
            **_identity_kwargs(SimpleNamespace(**registered)),
        )
        assert {capture.manifest.stream_id: capture.payload for capture in replayed} == payloads_by_stream
        assert [(capture.manifest.format, capture.manifest.compression) for capture in replayed] == [("csv", "gzip")]
        assert await session.scalar(select(func.count()).select_from(CustomImportCaptureRawObject)) == 1
        return execution.state, execution.capture_bundle_id, bytes(execution.request_identity_sha256), lease.fence


async def test_coordinator_bound_originals_resume_through_offline_installed_cli(installed_application, monkeypatch):
    async with isolated_publication_case() as case:
        request_by_key, payloads_by_stream = _original_cli_request()
        registered = await _register_fresh_case(installed_application, case, request_by_key["definition"])
        request = cli._source_request_from_stdin(
            SimpleNamespace(source="https", **registered), BytesIO(canonical_json(request_by_key).encode())
        )
        acquired_streams = _install_source(monkeypatch, request, payloads_by_stream)
        with monkeypatch.context() as interruption:
            interruption.setattr(runner, "run_claimed_candidate", _interrupt_bound_candidate)
            interrupted = await https_candidate.run_https_candidate(case.sessions, request)
        assert interrupted.status == "lease_lost" and acquired_streams == ["root_source"]
        before = await _bound_raw_state(case, registered, interrupted.execution_id, payloads_by_stream)
        assert before[0] == "running" and before[3] == 1
        async with case.sessions() as session:
            assert await session.get(CustomImportCurrentGeneration, registered["dataset_id"]) is None
        request_by_key["lease_token"] = "synthetic-resumed-owner"
        resumed = await _https_command(installed_application, case, registered, monkeypatch, "resume", request_by_key)
        assert resumed["status"] == "activated" and resumed["execution_id"] == interrupted.execution_id
        after = await _bound_raw_state(case, registered, resumed["execution_id"], payloads_by_stream)
        assert after[0] == "completed" and after[1:3] == before[1:3] and after[3] == 2
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, registered["dataset_id"])
            seal = await session.get(CustomImportGenerationSeal, resumed["generation_id"])
            assert pointer.generation_id == resumed["generation_id"] and pointer.pointer_version == 1
            assert seal.root_count == seal.family_count == seal.winner_count == len(installed_support.NPIS)
            assert await session.scalar(select(func.count()).select_from(CustomImportPublicationEvent)) == 1
