# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native retained preparation, prefix replay, quota rollback and lease fencing."""

from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import uuid
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select, text, update

from db.models.custom_import import CustomImportLease
from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from process.custom_import.capture import DecodedRecord
from process.custom_import.capture_store import CaptureBundleConflict
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import (
    bind_execution_capture_bundle,
    claim_execution,
    reserve_execution,
    resume_execution,
)
from process.custom_import.prepared_source import prepare_retained_source
from process.custom_import.prepared_source_codec import iter_prepared_source_parts, iter_prepared_source_records
from process.custom_import.prepared_source_context import load_prepared_source_context
from process.custom_import.prepared_source_metadata import (
    PreparedPartMetadata,
    prepared_source_summary,
    summarize_prepared_stream,
)
from process.custom_import.prepared_source_progress import append_progress, canonical_progress, empty_progress
from process.custom_import.prepared_source_store import (
    append_prepared_part,
    begin_prepared_source,
    prepared_source_identity,
)
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.source_binding_store import register_source_binding
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_definition import _raw_definition
from tests.test_custom_import_raw_capture_postgres import _register_raw, _sealed_originals
from tests.test_custom_import_source_batch import _permit
from tests.test_custom_import_source_binding_store import _object_binding


def _material():
    definition = CustomImportDefinition.from_mapping(_raw_definition())
    binding = _object_binding(definition)
    policy = replace(
        binding.processing_policy, prepared=dict(binding.processing_policy.prepared) | {"part_row_limit": 1}
    )
    binding = replace(binding, processing_policy=policy)
    source_records = (
        DecodedRecord(1, {"Provider ID": "1234567893", "Provider Name": "Synthetic Provider A"}),
        DecodedRecord(2, {"Provider ID": "1234567802", "Provider Name": "Synthetic Provider B"}),
    )
    originals_by_stream = {
        "providers": b'"Provider ID","Provider Name"\n"1234567893","Synthetic Provider A"\n"1234567802","Synthetic Provider B"\n',
        "rates": gzip.compress(b"", mtime=0),
    }
    parts = tuple(
        tuple(
            iter_prepared_source_parts(
                iter(stream_records),
                limits=policy.capture_limits,
                part_byte_limit=policy.prepared["part_byte_limit"],
                part_row_limit=policy.prepared["part_row_limit"],
            )
        )
        for stream_records in (source_records, ())
    )
    return SimpleNamespace(
        definition=definition,
        binding=binding,
        captures=_sealed_originals(definition, originals_by_stream),
        parts=parts,
        records=source_records,
    )


def _metadata(part, slot, ordinal):
    return PreparedPartMetadata(
        slot,
        ordinal,
        part.first_source_ordinal,
        part.record_count,
        len(part.payload),
        hashlib.sha256(part.payload).digest(),
    )


def _first_append_quota(material, adjustment):
    policy = material.binding.processing_policy
    first = _metadata(material.parts[0][0], 1, 1)
    raw_bytes = sum(len(capture.payload) for capture in material.captures)
    for _ in range(5):
        progress = append_progress(empty_progress(2, policy), first, policy)
        retained = raw_bytes + first.byte_count + len(first.canonical.encode())
        retained += len(policy.canonical.encode()) + len(canonical_progress(progress).encode())
        if policy.maximum_dataset_retained_bytes == retained + adjustment:
            material.binding = replace(material.binding, processing_policy=policy)
            return
        policy = replace(policy, maximum_dataset_retained_bytes=retained + adjustment)
    raise AssertionError("synthetic quota must converge after its decimal width stabilizes")


async def _registered_attempt(case, material):
    request_key = f"synthetic_prepared_{uuid.uuid4().hex}"
    async with case.sessions() as session, session.begin():
        receipt = await register_source_binding(
            session, dataset_key=request_key, definition=material.definition, binding=material.binding
        )
    async with case.sessions() as session, session.begin():
        capture = await _register_raw(
            session, receipt, material.captures, material.binding.processing_policy.maximum_dataset_retained_bytes
        )
    async with case.sessions() as session, session.begin():
        submission = await reserve_execution(
            session,
            dataset_id=receipt.dataset_id,
            definition_revision_id=receipt.definition_revision_id,
            schema_revision_id=receipt.schema_revision_id,
            source_binding_revision_id=receipt.source_binding_revision_id,
            idempotency_key=request_key,
            mechanism="local",
            request_identity_sha256=hashlib.sha256(request_key.encode()).digest(),
        )
    return receipt, capture, submission, request_key


async def _context(case, material):
    receipt, capture, submission, request_key = await _registered_attempt(case, material)
    token = f"synthetic-lease-{uuid.uuid4().hex}"
    async with case.sessions() as session, session.begin():
        lease = await claim_execution(session, execution_id=submission.execution_id, token=token, lease_seconds=60)
    assert lease is not None
    async with case.sessions() as session, session.begin():
        attached = await bind_execution_capture_bundle(
            session,
            execution_id=submission.execution_id,
            dataset_id=receipt.dataset_id,
            definition_revision_id=receipt.definition_revision_id,
            schema_revision_id=receipt.schema_revision_id,
            capture_bundle_id=capture.capture_bundle_id,
            fence=lease.fence,
            token=token,
        )
        assert attached is not None
    now = dt.datetime.now(dt.UTC)
    permit = _permit(
        dataset_id=receipt.dataset_id,
        definition_revision_id=receipt.definition_revision_id,
        schema_revision_id=receipt.schema_revision_id,
        source_binding_revision_id=receipt.source_binding_revision_id,
        source_binding_sha256=receipt.source_binding_sha256.hex(),
        idempotency_key=request_key,
        issued_at=now,
        expires_at=now + dt.timedelta(minutes=5),
    )
    return await load_prepared_source_context(
        case.sessions, execution_id=submission.execution_id, fence=lease.fence, token=token, source_permit=permit
    )


async def _stored(case, context):
    async with case.sessions() as session:
        header = await session.get(CustomImportPreparedSource, context.execution_id)
        parts = tuple(
            await session.scalars(
                select(CustomImportPreparedSourcePart)
                .where(CustomImportPreparedSourcePart.execution_id == context.execution_id)
                .order_by(CustomImportPreparedSourcePart.stream_slot, CustomImportPreparedSourcePart.part_ordinal)
            )
        )
    return header, parts


def _expected_summary(context, material):
    summaries = (
        summarize_prepared_stream(
            (_metadata(part, slot, ordinal) for ordinal, part in enumerate(parts, 1)),
            stream_slot=slot,
            policy=context.policy,
        )
        for slot, parts in enumerate(material.parts, 1)
    )
    return prepared_source_summary(prepared_source_identity(context), summaries, stream_count=2, policy=context.policy)


async def _run_driver(case, context):
    return await prepare_retained_source(
        case.sessions,
        execution_id=context.execution_id,
        fence=context.fence,
        token=context.lease_token,
        source_permit=context.source_permit,
    )


async def _assert_later_part_replay(case, context, record):
    async with case.sessions() as session:
        part = await session.get(CustomImportPreparedSourcePart, (context.execution_id, 1, 2))
    assert part is not None and part.first_source_ordinal == 2
    assert hashlib.sha256(part.payload).digest() == part.payload_sha256
    replayed_records = tuple(
        iter_prepared_source_records(
            BytesIO(part.payload),
            limits=context.policy.capture_limits,
            part_byte_limit=context.policy.prepared["part_byte_limit"],
            part_row_limit=context.policy.prepared["part_row_limit"],
            first_source_ordinal=part.first_source_ordinal,
            record_count=part.record_count,
        )
    )
    assert replayed_records == (record,)


async def test_driver_reuses_native_prefix_reaches_eof_and_seals_exact_retained_parts():
    material = _material()
    async with isolated_publication_case() as case:
        context = await _context(case, material)
        initial = await begin_prepared_source(case.sessions, context)
        assert (initial.state, initial.version) == ("pending", 1)
        first = material.parts[0][0]
        prefix = await append_prepared_part(case.sessions, context, stream_slot=1, part_ordinal=1, part=first)
        assert await append_prepared_part(case.sessions, context, stream_slot=1, part_ordinal=1, part=first) == prefix
        changed = first._replace(payload=first.payload.replace(b"Provider A", b"Provider Z"))
        with pytest.raises(CandidateRunnerError):
            await append_prepared_part(case.sessions, context, stream_slot=1, part_ordinal=1, part=changed)
        header, parts = await _stored(case, context)
        assert header.version == prefix.version and len(parts) == 1 and parts[0].payload == first.payload
        prepared_digest = await _run_driver(case, context)
        header, parts = await _stored(case, context)
        expected = _expected_summary(context, material)
        assert prepared_digest == header.prepared_sha256 == expected.digest
        assert header.state == "sealed" and header.sealed_at is not None
        assert header.canonical_summary == expected.canonical
        assert (header.part_count, header.record_count, header.version) == (3, 2, 7)
        assert [(part.stream_slot, part.part_ordinal, part.payload) for part in parts] == [
            (slot, ordinal, part.payload)
            for slot, stream_parts in enumerate(material.parts, 1)
            for ordinal, part in enumerate(stream_parts, 1)
        ]
        retained = header.byte_count + header.manifest_byte_count
        retained += sum(
            len(canonical.encode())
            for canonical in (header.canonical_processing_policy, header.canonical_progress, header.canonical_summary)
        )
        assert header.retained_byte_count == retained
        assert await _run_driver(case, context) == prepared_digest
        replayed_header, replayed_parts = await _stored(case, context)
        assert replayed_header.version == header.version and len(replayed_parts) == len(parts)
        await _assert_later_part_replay(case, context, material.records[1])


@pytest.mark.parametrize("adjustment", (0, -1), ids=("exact", "one-byte-short"))
async def test_native_append_accounts_original_policy_progress_payload_and_manifest(adjustment):
    material = _material()
    _first_append_quota(material, adjustment)
    async with isolated_publication_case() as case:
        context = await _context(case, material)
        initial = await begin_prepared_source(case.sessions, context)
        if adjustment:
            with pytest.raises(CaptureBundleConflict, match="retained-byte quota"):
                await append_prepared_part(
                    case.sessions, context, stream_slot=1, part_ordinal=1, part=material.parts[0][0]
                )
        else:
            await append_prepared_part(case.sessions, context, stream_slot=1, part_ordinal=1, part=material.parts[0][0])
        header, parts = await _stored(case, context)
        if adjustment:
            assert header.version == initial.version and header.canonical_progress == canonical_progress(
                initial.progress
            )
            assert (header.part_count, header.record_count, header.byte_count, header.manifest_byte_count) == (
                0,
                0,
                0,
                0,
            )
            assert parts == ()
        else:
            assert header.version == initial.version + 1 and len(parts) == 1
            assert (
                sum(len(capture.payload) for capture in material.captures) + header.retained_byte_count
                == context.policy.maximum_dataset_retained_bytes
            )


async def test_real_expired_lease_takeover_rejects_stale_append_and_adopts_exact_prefix():
    material = _material()
    async with isolated_publication_case() as case:
        context = await _context(case, material)
        await begin_prepared_source(case.sessions, context)
        prefix = await append_prepared_part(
            case.sessions, context, stream_slot=1, part_ordinal=1, part=material.parts[0][0]
        )
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == context.execution_id)
                .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
            )
        token = f"synthetic-takeover-{uuid.uuid4().hex}"
        async with case.sessions() as session, session.begin():
            lease = await resume_execution(session, execution_id=context.execution_id, token=token, lease_seconds=60)
        assert lease is not None and lease.fence > context.fence
        with pytest.raises(LeaseAuthorityLost):
            await append_prepared_part(case.sessions, context, stream_slot=1, part_ordinal=2, part=material.parts[0][1])
        header, parts = await _stored(case, context)
        assert header.version == prefix.version and len(parts) == 1
        resumed = await load_prepared_source_context(
            case.sessions,
            execution_id=context.execution_id,
            fence=lease.fence,
            token=token,
            source_permit=context.source_permit,
        )
        adopted = await begin_prepared_source(case.sessions, resumed)
        assert adopted.version == prefix.version + 1 and adopted.progress == prefix.progress
        assert (
            await append_prepared_part(case.sessions, resumed, stream_slot=1, part_ordinal=1, part=material.parts[0][0])
            == adopted
        )
        assert await _run_driver(case, resumed) == _expected_summary(resumed, material).digest
        header, parts = await _stored(case, resumed)
        assert header.producer_fence == lease.fence and header.state == "sealed" and len(parts) == 3
