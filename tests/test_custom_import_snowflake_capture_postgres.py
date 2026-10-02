# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native capture-only coordination from a synthetic cursor to durable replay."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict
from decimal import Decimal
from io import BytesIO

import pyarrow.parquet as pq
import pytest
from sqlalchemy import select

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
    CustomImportExecution,
    CustomImportGeneration,
)
from process.custom_import.capture import iter_records
from process.custom_import.capture_store import CaptureBundleConflict, open_segmented_parquet_parts
from process.custom_import.definition import canonical_json
from process.custom_import.definition_store import register_definition
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_capture import acquire_segmented_snowflake_capture
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_snowflake_bundle import _SNAPSHOT, _Credentials
from tests.test_custom_import_snowflake_capture import _policy
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row

_NPIS = ("1003000126", "1234567893")
_AMOUNT = Decimal("123456789012345678.123456789012")
_ROWS = tuple(_shared_row(npi, key=str(index), amount=_AMOUNT) for index, npi in enumerate(_NPIS, 1))


async def _registered_request(case, bundle):
    async with case.sessions() as session, session.begin():
        registered = await register_definition(session, "synthetic_segmented_source", bundle.definition)
    return SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key="synthetic-segmented-acquisition",
        lease_token=b"synthetic-segmented-owner",
    )


async def _assert_full_replay(session, request, bundle, policy, query_id):
    streams_by_id = {stream.stream_id: stream for stream in request.definition.source_streams}
    records_by_stream = {stream_id: [] for stream_id in streams_by_id}
    observed_parts = []
    payload_bytes = decoded_bytes = arrow_bytes = manifest_bytes = 0
    async with open_segmented_parquet_parts(
        session,
        capture_bundle_id=bundle.capture_bundle_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    ) as parts:
        async for part in parts:
            stream_id = part.receipt.stream_id
            decoded_records = tuple(iter_records(part.capture, streams_by_id[stream_id], limits=policy.part_limits))
            assert len(decoded_records) == part.record_count == 1
            assert pq.read_table(BytesIO(part.capture.payload)).nbytes == part.arrow_byte_count
            assert part.receipt.source_snapshot_token == _SNAPSHOT
            receipt = json.loads(part.receipt.canonical_manifest)
            assert receipt["source"]["query_id"] == query_id
            assert receipt["policy_sha256"] == policy.digest and receipt["part_count"] == 2
            records_by_stream[stream_id].extend(dict(decoded_record.values) for decoded_record in decoded_records)
            observed_parts.append((stream_id, part.ordinal))
            payload_bytes += len(part.capture.payload)
            decoded_bytes += part.capture.manifest.decoded_bytes
            arrow_bytes += part.arrow_byte_count
            manifest_bytes += len(canonical_json(asdict(part.capture.manifest)).encode())
    assert observed_parts == [(stream_id, ordinal) for stream_id in streams_by_id for ordinal in (1, 2)]
    assert records_by_stream == {
        "root_source": [{"npi": npi, "score": _AMOUNT, "enabled": True} for npi in _NPIS],
        "detail_source": [
            {"detail_npi": npi, "detail_id": str(index), "amount": _AMOUNT} for index, npi in enumerate(_NPIS, 1)
        ],
    }
    assert bundle.committed_part_count == bundle.committed_record_count == 4
    assert bundle.committed_byte_count == payload_bytes
    assert bundle.committed_decoded_byte_count == decoded_bytes
    assert bundle.committed_arrow_byte_count == arrow_bytes
    assert bundle.committed_manifest_byte_count == manifest_bytes
    usage = await session.get(CustomImportCaptureUsage, request.dataset_id)
    assert usage.retained_bytes == payload_bytes + manifest_bytes


async def test_shared_cursor_seals_two_streams_and_fully_replays_committed_parts(monkeypatch):
    policy = _policy()
    processing_policy = ProcessingPolicy(policy, 17, BuildPolicy(2, 4096, 1000, 60, 300))
    builder, source_request, adapter, cursor, connection = _runtime(
        monkeypatch, _ROWS, partition_rows=1, processing_policy=processing_policy
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    async with isolated_publication_case() as case:
        request = await _registered_request(case, source_request)
        capture_result = await acquire_segmented_snowflake_capture(
            case.sessions,
            request,
            statement_builder=builder,
            adapter=adapter,
            credential_provider=_Credentials(),
            policy=policy,
            driver_timeout_seconds=17,
            processing_policy=processing_policy,
        )
        assert capture_result.status == "capture_sealed"
        async with case.sessions() as session, session.begin():
            bundle = await session.get(CustomImportCaptureBundle, capture_result.capture_bundle_id)
            execution = await session.get(CustomImportExecution, capture_result.execution_id)
            assert bundle.capture_state == "sealed" and bundle.sealed_at is not None
            assert bundle.producing_execution_id == capture_result.execution_id
            assert execution.capture_bundle_id == bundle.capture_bundle_id
            assert execution.state == "running" and execution.finished_at is None
            assert bundle.acquisition_started_at == execution.started_at
            assert bundle.acquisition_deadline_at == execution.started_at + dt.timedelta(
                seconds=policy.acquisition_deadline_seconds
            )
            assert bytes(bundle.policy_sha256).hex() == policy.digest
            headers = (
                await session.scalars(
                    select(CustomImportCapture)
                    .where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
                    .order_by(CustomImportCapture.stream_slot)
                )
            ).all()
            assert len(headers) == bundle.stream_count == 2
            assert all(header.capture_state == "sealed" and header.eof_at is not None for header in headers)
            assert all(header.committed_part_count == header.payload_part_count == 2 for header in headers)
            await _assert_full_replay(session, request, bundle, policy, cursor.sfqid)
            assert await session.scalar(select(CustomImportGeneration.generation_id)) is None
        assert cursor.executed == [builder.build_statement(source_request).sql]
        assert cursor.fetchone.call_count == len(source_request.bindings) + len(_ROWS) + 1
        assert cursor.closed and connection.closed


async def _assert_unsealed_capture(session, request):
    bundle = (
        await session.scalars(
            select(CustomImportCaptureBundle).where(CustomImportCaptureBundle.dataset_id == request.dataset_id)
        )
    ).one()
    execution = await session.get(CustomImportExecution, bundle.producing_execution_id)
    assert bundle.capture_state == "pending" and bundle.sealed_at is None
    assert bundle.canonical_manifest is None and bundle.manifest_sha256 is None
    assert execution.capture_bundle_id is None
    assert execution.state == "failed" and execution.finished_at is not None
    assert execution.terminal_reason == "source_capture_failed"
    headers = (
        await session.scalars(
            select(CustomImportCapture).where(CustomImportCapture.capture_bundle_id == bundle.capture_bundle_id)
        )
    ).all()
    assert len(headers) == 2
    assert all(header.capture_state == "pending" and header.eof_at is None for header in headers)
    assert all(header.canonical_manifest is None and header.sealed_at is None for header in headers)
    retained_parts = (
        await session.scalars(
            select(CustomImportCaptureParquetPart).where(
                CustomImportCaptureParquetPart.capture_bundle_id == bundle.capture_bundle_id
            )
        )
    ).all()
    assert len(retained_parts) == bundle.committed_part_count == bundle.committed_record_count == 4
    usage = await session.get(CustomImportCaptureUsage, request.dataset_id)
    assert usage.retained_bytes == sum(
        len(part.payload) + len(part.canonical_capture_manifest.encode()) for part in retained_parts
    )
    assert usage.retained_bytes > 0
    with pytest.raises(CaptureBundleConflict):
        async with open_segmented_parquet_parts(
            session,
            capture_bundle_id=bundle.capture_bundle_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        ) as parts:
            await anext(parts)
    assert await session.scalar(select(CustomImportGeneration.generation_id)) is None


async def test_final_source_cleanup_failure_retains_only_unsealed_charged_parts(monkeypatch):
    policy = _policy()
    processing_policy = ProcessingPolicy(policy, 17, BuildPolicy(2, 4096, 1000, 60, 300))
    builder, source_request, adapter, cursor, connection = _runtime(
        monkeypatch, _ROWS, partition_rows=1, processing_policy=processing_policy
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))

    def failed_close():
        raise RuntimeError("synthetic final source close failure")

    monkeypatch.setattr(cursor, "close", failed_close)
    async with isolated_publication_case() as case:
        request = await _registered_request(case, source_request)
        with pytest.raises(SnowflakeConnectorError, match="resource cleanup failed") as caught:
            await acquire_segmented_snowflake_capture(
                case.sessions,
                request,
                statement_builder=builder,
                adapter=adapter,
                credential_provider=_Credentials(),
                policy=policy,
                driver_timeout_seconds=17,
                processing_policy=processing_policy,
            )
        assert any("cleanup is unconfirmed" in note for note in caught.value.__notes__)
        async with case.sessions() as session, session.begin():
            await _assert_unsealed_capture(session, request)
        assert cursor.executed == [builder.build_statement(source_request).sql]
        assert cursor.fetchone.call_count == len(source_request.bindings) + len(_ROWS) + 1
        assert connection.closed and not cursor.closed
