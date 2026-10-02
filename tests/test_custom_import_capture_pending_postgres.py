# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native application proof for committed segmented acquisition and replay."""

from __future__ import annotations

import asyncio
import hashlib
import struct
import uuid
from dataclasses import asdict, replace
from io import BytesIO

import pyarrow.parquet as pq
import pytest
from sqlalchemy import update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureUsage,
    CustomImportExecution,
    CustomImportLease,
)
from process.custom_import import execution as lifecycle
from process.custom_import.capture import capture_stream, iter_records
from process.custom_import.capture_pending import (
    PendingCaptureRequest,
    append_pending_parquet_part,
    begin_pending_parquet_bundle,
    mark_pending_parquet_eof,
    seal_pending_parquet_bundle,
)
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureReceipt,
    CaptureStoreError,
    open_replayable_parquet_parts,
    open_segmented_parquet_parts,
)
from process.custom_import.definition import canonical_json
from process.custom_import.operator import inspect_execution_evidence
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_capture_store_postgres import (
    _build_replayable_capture_set,
    _parquet_definition,
    _register_replayable_bundle,
    _schema_bearing_zero_row_parquet_part,
    _seed_parquet_case,
    _synthetic_parquet_part,
)
from tests.test_custom_import_segmented_capture_postgres import _POLICY


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode()).digest()


async def _start_attempt(case, *, policy=None, seed=None, snapshot="synthetic-pending-snapshot"):
    seed = seed or await _seed_parquet_case(case)
    token = b"synthetic-pending-owner"
    async with case.sessions() as session, session.begin():
        submission = await lifecycle.reserve_execution(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            idempotency_key=uuid.uuid4().hex,
            mechanism="local",
            request_identity_sha256=_digest("request"),
        )
        grant = await lifecycle.claim_execution(session, execution_id=submission.execution_id, token=token)
        assert grant is not None
    request = PendingCaptureRequest(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        execution_id=submission.execution_id,
        fence=grant.fence,
        token=token,
        request_identity_sha256=_digest("request"),
        source_request_sha256=_digest("source"),
        statement_sha256=_digest("statement"),
        source_snapshot_token=snapshot,
        policy=policy or SegmentedCapturePolicy.from_mapping(_POLICY),
    )
    async with case.sessions() as session, session.begin():
        registration = await begin_pending_parquet_bundle(session, request=request)
    return request, registration


def _parts(request, *, empty=False):
    result_by_stream = {}
    for stream in _parquet_definition().source_streams:
        payload = _schema_bearing_zero_row_parquet_part() if empty else _synthetic_parquet_part(stream.stream_id, 1)
        capture = capture_stream(
            BytesIO(payload),
            stream,
            source_snapshot_token=request.source_snapshot_token,
            limits=request.policy.part_limits,
        )
        table = pq.read_table(BytesIO(payload))
        result_by_stream[stream.stream_id] = ((capture, table.num_rows, table.nbytes),)
    return result_by_stream


def _receipt(request, stream_id, parts):
    payload_digest = hashlib.sha256(b"custom-import/parquet-parts/v1\0")
    manifest_digest = hashlib.sha256(b"custom-import/parquet-manifest-accounting/v1\0")
    manifest_bytes = 0
    for ordinal, (capture, record_count, arrow_byte_count) in enumerate(parts, 1):
        canonical = canonical_json(asdict(capture.manifest)).encode()
        manifest_bytes += len(canonical)
        payload_digest.update(
            struct.pack(">iq", ordinal, len(capture.payload)) + hashlib.sha256(capture.payload).digest()
        )
        manifest_digest.update(
            struct.pack(">iq", ordinal, len(canonical))
            + hashlib.sha256(canonical).digest()
            + struct.pack(">qqq", capture.manifest.decoded_bytes, arrow_byte_count, record_count)
        )
    byte_count = sum(len(capture.payload) for capture, _, _ in parts)
    canonical = canonical_json(
        dict(
            contract_version="custom-import/parquet-parts/v2",
            policy_sha256=request.policy.digest,
            source_request_sha256=request.source_request_sha256.hex(),
            statement_sha256=request.statement_sha256.hex(),
            part_count=len(parts),
            byte_count=byte_count,
            decoded_byte_count=sum(capture.manifest.decoded_bytes for capture, _, _ in parts),
            arrow_byte_count=sum(size for _, _, size in parts),
            record_count=sum(count for _, count, _ in parts),
            manifest_byte_count=manifest_bytes,
            payload_set_sha256=payload_digest.hexdigest(),
            manifest_set_sha256=manifest_digest.hexdigest(),
        )
    )
    return CaptureReceipt(
        stream_id,
        request.source_snapshot_token,
        byte_count,
        payload_digest.hexdigest(),
        canonical,
        hashlib.sha256(canonical.encode()).hexdigest(),
    )


async def _retain(case, request, registration, parts_by_stream, *, mark_eof=True):
    for stream_id, parts in parts_by_stream.items():
        for ordinal, (capture, record_count, arrow) in enumerate(parts, 1):
            async with case.sessions() as session, session.begin():
                assert (
                    await append_pending_parquet_part(
                        session,
                        request=request,
                        capture_bundle_id=registration.capture_bundle_id,
                        capture=capture,
                        ordinal=ordinal,
                        record_count=record_count,
                        arrow_byte_count=arrow,
                    )
                    == 1
                )
        if mark_eof:
            async with case.sessions() as session, session.begin():
                assert (
                    await mark_pending_parquet_eof(
                        session,
                        request=request,
                        capture_bundle_id=registration.capture_bundle_id,
                        stream_id=stream_id,
                        part_count=len(parts),
                        record_count=sum(count for _, count, _ in parts),
                    )
                    == 1
                )


def _read_ids(request, registration):
    return dict(
        capture_bundle_id=registration.capture_bundle_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )


@pytest.mark.parametrize("empty", [False, True])
async def test_incremental_commits_seal_bind_and_complete_replay(empty):
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        parts_by_stream = _parts(request, empty=empty)
        async with case.sessions() as session, session.begin():
            duplicate = await begin_pending_parquet_bundle(session, request=request)
            assert duplicate.capture_bundle_id == registration.capture_bundle_id and not duplicate.created
        await _retain(case, request, registration, parts_by_stream)
        async with case.sessions() as session, session.begin():
            bundle = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            assert bundle.capture_state == "pending" and bundle.canonical_manifest is None
            assert bundle.manifest_sha256 is None and bundle.sealed_at is None
            usage = await session.get(CustomImportCaptureUsage, request.dataset_id)
            expected_bytes = sum(
                len(capture.payload) + len(canonical_json(asdict(capture.manifest)).encode())
                for parts in parts_by_stream.values()
                for capture, _, _ in parts
            )
            assert usage.retained_bytes == expected_bytes
            with pytest.raises(CaptureBundleConflict):
                async with open_segmented_parquet_parts(session, **_read_ids(request, registration)) as parts:
                    await anext(parts)
        receipts = tuple(_receipt(request, stream_id, parts) for stream_id, parts in parts_by_stream.items())
        async with case.sessions() as session, session.begin():
            await seal_pending_parquet_bundle(
                session, request=request, capture_bundle_id=registration.capture_bundle_id, receipts=receipts
            )
        async with case.sessions() as session, session.begin():
            bundle = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            execution = await session.get(CustomImportExecution, request.execution_id)
            assert bundle.capture_state == "sealed" and execution.capture_bundle_id == bundle.capture_bundle_id
            evidence = await inspect_execution_evidence(
                session, dataset_id=request.dataset_id, execution_id=request.execution_id
            )
            assert evidence.capture_manifest_sha256 == bytes(bundle.manifest_sha256).hex()
            streams_by_id = {stream.stream_id: stream for stream in _parquet_definition().source_streams}
            received_parts = []
            async with open_segmented_parquet_parts(session, **_read_ids(request, registration)) as parts:
                async for part in parts:
                    assert (
                        len(
                            tuple(
                                iter_records(
                                    part.capture,
                                    streams_by_id[part.receipt.stream_id],
                                    limits=request.policy.part_limits,
                                )
                            )
                        )
                        == part.record_count
                    )
                    assert pq.read_table(BytesIO(part.capture.payload)).nbytes == part.arrow_byte_count
                    received_parts.append((part.receipt.stream_id, part.ordinal))
            assert received_parts == [(stream.stream_id, 1) for stream in _parquet_definition().source_streams]
            async with open_replayable_parquet_parts(session, **_read_ids(request, registration)) as parts:
                assert len([part async for part in parts]) == len(receipts)
            await seal_pending_parquet_bundle(
                session, request=request, capture_bundle_id=registration.capture_bundle_id, receipts=receipts
            )


async def test_exact_part_retry_and_eof_retry_never_double_charge():
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        parts_by_stream = _parts(request)
        stream_id, parts = next(iter(parts_by_stream.items()))
        capture, record_count, arrow = parts[0]
        async with case.sessions() as session, session.begin():
            arguments_by_field = dict(
                request=request,
                capture_bundle_id=registration.capture_bundle_id,
                capture=capture,
                ordinal=1,
                record_count=record_count,
                arrow_byte_count=arrow,
            )
            assert await append_pending_parquet_part(session, **arguments_by_field) == 1
            assert await append_pending_parquet_part(session, **arguments_by_field) == 0
            header = await session.get(CustomImportCapture, (registration.capture_bundle_id, 1), populate_existing=True)
            assert header.committed_part_count == 1
            assert (
                await mark_pending_parquet_eof(
                    session,
                    request=request,
                    capture_bundle_id=registration.capture_bundle_id,
                    stream_id=stream_id,
                    part_count=1,
                    record_count=record_count,
                )
                == 1
            )
            assert (
                await mark_pending_parquet_eof(
                    session,
                    request=request,
                    capture_bundle_id=registration.capture_bundle_id,
                    stream_id=stream_id,
                    part_count=1,
                    record_count=record_count,
                )
                == 0
            )
            with pytest.raises(CaptureBundleConflict, match="EOF"):
                await append_pending_parquet_part(session, **arguments_by_field)


@pytest.mark.parametrize("field", ["request_identity_sha256", "source_request_sha256", "statement_sha256", "token"])
async def test_attempt_drift_is_rejected_without_new_parts(field):
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        drifted = replace(request, **{field: _digest("drifted")})
        async with case.sessions() as session, session.begin():
            with pytest.raises(CaptureBundleConflict):
                await begin_pending_parquet_bundle(session, request=drifted)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            assert bundle.committed_part_count == 0


async def test_seal_requires_eof_and_failure_leaves_no_binding():
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        parts_by_stream = _parts(request)
        await _retain(case, request, registration, parts_by_stream, mark_eof=False)
        receipts = tuple(_receipt(request, stream_id, parts) for stream_id, parts in parts_by_stream.items())
        with pytest.raises(CaptureBundleConflict, match="EOF"):
            async with case.sessions() as session, session.begin():
                await seal_pending_parquet_bundle(
                    session, request=request, capture_bundle_id=registration.capture_bundle_id, receipts=receipts
                )
        async with case.sessions() as session:
            assert (await session.get(CustomImportExecution, request.execution_id)).capture_bundle_id is None
            assert (
                await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            ).capture_state == "pending"


async def test_lost_seal_ack_can_replay_only_the_exact_bound_receipt():
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        parts_by_stream = _parts(request)
        await _retain(case, request, registration, parts_by_stream)
        receipts = tuple(_receipt(request, stream_id, parts) for stream_id, parts in parts_by_stream.items())
        async with case.sessions() as session, session.begin():
            await seal_pending_parquet_bundle(
                session, request=request, capture_bundle_id=registration.capture_bundle_id, receipts=receipts
            )
        changed = replace(
            receipts[0],
            canonical_manifest=receipts[0].canonical_manifest.replace('"record_count":1', '"record_count":2'),
        )
        with pytest.raises(CaptureStoreError):
            async with case.sessions() as session, session.begin():
                await seal_pending_parquet_bundle(
                    session,
                    request=request,
                    capture_bundle_id=registration.capture_bundle_id,
                    receipts=(changed, *receipts[1:]),
                )


async def test_expired_lease_closes_mutation_without_advancing_progress():
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == request.execution_id)
                .values(expires_at=lifecycle.func.clock_timestamp())
            )
        capture, record_count, arrow = next(iter(_parts(request).values()))[0]
        async with case.sessions() as session, session.begin():
            with pytest.raises(CaptureBundleConflict, match="lease"):
                await append_pending_parquet_part(
                    session,
                    request=request,
                    capture_bundle_id=registration.capture_bundle_id,
                    capture=capture,
                    ordinal=1,
                    record_count=record_count,
                    arrow_byte_count=arrow,
                )
        async with case.sessions() as session:
            assert (
                await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            ).committed_part_count == 0


async def test_seal_binding_failure_rolls_back_every_stream_and_bundle(monkeypatch):
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        parts_by_stream = _parts(request)
        await _retain(case, request, registration, parts_by_stream)
        receipts = tuple(_receipt(request, stream_id, parts) for stream_id, parts in parts_by_stream.items())

        async def lost_authority(*_args, **_kwargs):
            return None

        monkeypatch.setattr(lifecycle, "bind_execution_capture_bundle", lost_authority)
        with pytest.raises(CaptureBundleConflict, match="before binding"):
            async with case.sessions() as session, session.begin():
                await seal_pending_parquet_bundle(
                    session, request=request, capture_bundle_id=registration.capture_bundle_id, receipts=receipts
                )
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            execution = await session.get(CustomImportExecution, request.execution_id)
            stream = await session.get(
                CustomImportCapture, (registration.capture_bundle_id, registration.stream_slots[0])
            )
            assert bundle.capture_state == stream.capture_state == "pending"
            assert bundle.canonical_manifest is None and stream.canonical_manifest is None
            assert execution.capture_bundle_id is None


async def test_dataset_retained_quota_includes_abandoned_attempts():
    document_by_field = SegmentedCapturePolicy.from_mapping(_POLICY).to_mapping()
    document_by_field["part_limits"].update(
        maximum_compressed_bytes=1024, maximum_decoded_bytes=1024, read_chunk_bytes=128
    )
    document_by_field.update(maximum_part_manifest_bytes=1024, maximum_dataset_retained_bytes=4096)
    for name, multiplier in (("stream_budget", 1), ("bundle_budget", 2)):
        document_by_field[name].update(
            maximum_compressed_bytes=1024 * multiplier,
            maximum_decoded_bytes=1024 * multiplier,
            maximum_manifest_bytes=1024 * multiplier,
        )
    policy = SegmentedCapturePolicy.from_mapping(document_by_field)
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        is_quota_exhausted = False
        for _ in range(6):
            request, registration = await _start_attempt(case, seed=seed, policy=policy)
            capture, record_count, arrow = next(iter(_parts(request).values()))[0]
            try:
                async with case.sessions() as session, session.begin():
                    await append_pending_parquet_part(
                        session,
                        request=request,
                        capture_bundle_id=registration.capture_bundle_id,
                        capture=capture,
                        ordinal=1,
                        record_count=record_count,
                        arrow_byte_count=arrow,
                    )
            except DBAPIError as exc:
                assert "retained_quota" in str(exc)
                is_quota_exhausted = True
                break
        assert is_quota_exhausted
        async with case.sessions() as session:
            usage = await session.get(CustomImportCaptureUsage, seed.dataset_id)
            assert 0 < usage.retained_bytes <= policy.maximum_dataset_retained_bytes


async def test_concurrent_exact_append_serializes_and_charges_one_part():
    async with isolated_publication_case() as case:
        request, registration = await _start_attempt(case)
        capture, record_count, arrow_byte_count = next(iter(_parts(request).values()))[0]

        async def append():
            async with case.sessions() as session, session.begin():
                return await append_pending_parquet_part(
                    session,
                    request=request,
                    capture_bundle_id=registration.capture_bundle_id,
                    capture=capture,
                    ordinal=1,
                    record_count=record_count,
                    arrow_byte_count=arrow_byte_count,
                )

        assert sorted(await asyncio.gather(append(), append())) == [0, 1]
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, registration.capture_bundle_id)
            usage = await session.get(CustomImportCaptureUsage, request.dataset_id)
            assert bundle.committed_part_count == 1
            assert usage.retained_bytes == len(capture.payload) + len(canonical_json(asdict(capture.manifest)).encode())


async def test_pending_same_snapshot_cannot_poison_legacy_reuse_or_execution_binding():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        legacy = await _register_replayable_bundle(case, seed, captures)
        request, registration = await _start_attempt(
            case, seed=seed, snapshot=captures[0].receipt.source_snapshot_token
        )
        async with case.sessions() as session, session.begin():
            with pytest.raises(lifecycle.ExecutionInvariantError, match="sealed"):
                await lifecycle.create_execution(
                    session,
                    dataset_id=request.dataset_id,
                    definition_revision_id=request.definition_revision_id,
                    schema_revision_id=request.schema_revision_id,
                    idempotency_key=uuid.uuid4().hex,
                    mechanism="local",
                    capture_bundle_id=registration.capture_bundle_id,
                )
            with pytest.raises(lifecycle.ExecutionInvariantError, match="sealed"):
                await lifecycle.bind_execution_capture_bundle(
                    session,
                    execution_id=request.execution_id,
                    dataset_id=request.dataset_id,
                    definition_revision_id=request.definition_revision_id,
                    schema_revision_id=request.schema_revision_id,
                    capture_bundle_id=registration.capture_bundle_id,
                    fence=request.fence,
                    token=request.token,
                )
        duplicate = await _register_replayable_bundle(case, seed, captures)
        assert duplicate.capture_bundle_id == legacy.capture_bundle_id and not duplicate.created
