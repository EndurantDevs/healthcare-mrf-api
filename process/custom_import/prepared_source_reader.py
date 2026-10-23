# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded SOURCE reads from one sealed, original-bound prepared representation."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import timedelta
from io import BytesIO

from sqlalchemy import select

from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from process.custom_import import build_source as staging
from process.custom_import.capture_store import _identity, _validated_streams
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT, iter_prepared_source_records
from process.custom_import.prepared_source_context import PreparedSourceContext, _retained_pins
from process.custom_import.prepared_source_metadata import PreparedStreamFold
from process.custom_import.prepared_source_seal import _METADATA_COLUMNS, _part_metadata, _sealed_digest
from process.custom_import.prepared_source_store import _state
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost


def _invalid():
    return CandidateRunnerError("prepared_source_replay_invalid")


async def retained_context(session, loaded, execution, bundle, *, fence, token, permit):
    """Derive value-only scope from the same immutable owner-joined binding loader."""
    request_digest, raw_digest = _retained_pins(execution, bundle, loaded)
    streams = await _validated_streams(
        session,
        _identity(
            dataset_id=loaded.dataset_id,
            definition_revision_id=loaded.definition_revision_id,
            schema_revision_id=loaded.schema_revision_id,
        ),
    )
    return PreparedSourceContext(
        loaded.dataset_id,
        loaded.definition_revision_id,
        loaded.schema_revision_id,
        execution.execution_id,
        bundle.capture_bundle_id,
        loaded.source_binding_revision_id,
        loaded.source_binding_sha256,
        request_digest,
        raw_digest,
        loaded.definition,
        loaded.binding.processing_policy,
        tuple(streams),
        fence,
        token,
        execution.started_at + timedelta(seconds=loaded.binding.processing_policy.build.build_deadline_seconds),
        permit,
    )


async def sealed_header(session, context, *, lock=False):
    """Validate the complete bounded seal, not just a non-null digest column."""
    query = select(CustomImportPreparedSource).where(CustomImportPreparedSource.execution_id == context.execution_id)
    if lock:
        query = query.with_for_update().execution_options(populate_existing=True)
    header = await session.scalar(query)
    if header is None or header.state != "sealed":
        raise _invalid()
    progress = _state(header, context).progress
    _sealed_digest(header, context, progress)
    return header, progress


@dataclass(frozen=True)
class PreparedReplayPart:
    ordinal: int
    first_source_ordinal: int
    record_count: int
    payload: bytes
    part_count: int


async def open_prepared_cursor_part(session, *, request, stream_slot, expected_cursor, deadline):
    """Fetch one indexed bounded payload; no original or earlier part reads."""
    from process.custom_import.source_batch import _read_timeout

    context = request.prepared_source
    header, progress = await sealed_header(session, context)
    if not 1 <= stream_slot <= len(progress):
        raise _invalid()
    stream = progress[stream_slot - 1]
    part_model = CustomImportPreparedSourcePart
    ordinal = expected_cursor.next_part_ordinal
    if not 1 <= ordinal <= stream.part_count:
        raise _invalid()
    # Check metadata and policy bounds before borrowing the bytea payload.
    await _read_timeout(session, request, deadline)
    metadata_row = (
        await session.execute(
            select(*_METADATA_COLUMNS).where(
                part_model.execution_id == context.execution_id,
                part_model.stream_slot == stream_slot,
                part_model.part_ordinal == ordinal,
            )
        )
    ).one_or_none()
    if metadata_row is None:
        raise _invalid()
    metadata = _part_metadata(metadata_row, context)
    if (
        metadata.byte_count > context.policy.prepared["part_byte_limit"]
        or metadata.record_count > context.policy.prepared["part_row_limit"]
        or expected_cursor.next_part_row_ordinal > metadata.record_count
        or expected_cursor.next_source_ordinal
        != metadata.first_source_ordinal - 1 + expected_cursor.next_part_row_ordinal
    ):
        raise _invalid()
    await _read_timeout(session, request, deadline)
    part_payload = await session.scalar(
        select(part_model.payload).where(
            part_model.execution_id == context.execution_id,
            part_model.stream_slot == stream_slot,
            part_model.part_ordinal == ordinal,
            part_model.dataset_id == context.dataset_id,
            part_model.definition_revision_id == context.definition_revision_id,
            part_model.schema_revision_id == context.schema_revision_id,
            part_model.payload_contract == PREPARED_SOURCE_CONTRACT,
            part_model.canonical_manifest == metadata.canonical,
            part_model.manifest_sha256 == metadata.digest,
            part_model.byte_count == metadata.byte_count,
            part_model.payload_sha256 == metadata.payload_sha256,
        )
    )
    if (
        type(part_payload) is not bytes
        or len(part_payload) != metadata.byte_count
        or hashlib.sha256(part_payload).digest() != metadata.payload_sha256
    ):
        raise _invalid()
    return PreparedReplayPart(
        ordinal, metadata.first_source_ordinal, metadata.record_count, part_payload, stream.part_count
    )


def source_pages(context, part, policy, cursor):
    """Map original labels once, preserving the existing row/family oracle."""
    if (
        cursor.next_part_row_ordinal > part.record_count
        or cursor.next_source_ordinal != part.first_source_ordinal - 1 + cursor.next_part_row_ordinal
    ):
        raise _invalid()
    field_id_by_label = {
        alias.source_label: alias.field_id
        for alias in context.request.definition.aliases
        if alias.stream_id == context.stream.stream_id
    }
    reducer = staging._SourcePageReducer(
        context, part.ordinal, cursor.next_part_row_ordinal, part.first_source_ordinal - 1
    )
    binary = BytesIO(part.payload)
    decoded = iter_prepared_source_records(
        binary,
        limits=policy.capture_limits,
        part_byte_limit=policy.prepared["part_byte_limit"],
        part_row_limit=policy.prepared["part_row_limit"],
        first_source_ordinal=part.first_source_ordinal,
        record_count=part.record_count,
    )
    primary = None
    try:
        for decoded_record in decoded:
            values_by_field = {
                field_id_by_label.get(label, label): scalar_value
                for label, scalar_value in decoded_record.values.items()
            }
            if len(values_by_field) != len(decoded_record.values):
                raise CandidateRunnerError("https_source_alias_collision")
            prepared = staging._prepare_row(context.request, context.stream, values_by_field)
            page = reducer.before_append(prepared)
            if page is not None:
                yield page
            reducer.append(prepared)
        if reducer.records:
            yield reducer.page()
    except BaseException as error:
        primary = error
    primary = staging._close_iterator(decoded, primary)
    binary.close()
    if primary is not None:
        raise primary


async def verify_stream_metadata(session, request, stream_slot, deadline):
    """Verify complete immutable metadata in bounded pages, never prior payloads."""
    context = request.prepared_source
    header, progress = await sealed_header(session, context)
    if not 1 <= stream_slot <= len(progress):
        raise _invalid()
    fold, after = PreparedStreamFold(stream_slot, context.policy), 0
    part = CustomImportPreparedSourcePart
    while True:
        remaining = int((deadline - time.monotonic()) * 1000)
        if remaining < 4:
            raise LeaseAuthorityLost("SOURCE read deadline elapsed")
        await staging._set_timeout(session, min(request.statement_timeout_ms, remaining // 2))
        metadata_rows = (
            await session.execute(
                select(*_METADATA_COLUMNS)
                .where(
                    part.execution_id == context.execution_id,
                    part.stream_slot == stream_slot,
                    part.part_ordinal > after,
                )
                .order_by(part.part_ordinal)
                .limit(256)
            )
        ).all()
        for metadata_row in metadata_rows:
            metadata = _part_metadata(metadata_row, context)
            fold.add(metadata)
            after = metadata.part_ordinal
        if len(metadata_rows) < 256:
            break
    if fold.finish().to_mapping() != json.loads(header.canonical_summary)["streams"][stream_slot - 1]:
        raise _invalid()
