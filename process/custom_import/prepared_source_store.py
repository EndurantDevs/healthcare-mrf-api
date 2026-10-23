# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Short fenced transactions for retained object-source preparation."""

from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from typing import NamedTuple

from sqlalchemy import select

from db.models.custom_import_prepared_source import CustomImportPreparedSource, CustomImportPreparedSourcePart
from process.custom_import import build_source as staging
from process.custom_import.capture_retention import validate_dataset_retained_bytes
from process.custom_import.execution import lease_token_sha256
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT, PreparedSourcePart
from process.custom_import.prepared_source_context import (
    PreparedSourceContext,
    require_prepared_source_context,
)
from process.custom_import.prepared_source_metadata import PreparedPartMetadata, PreparedSourceIdentity
from process.custom_import.prepared_source_progress import (
    append_progress,
    canonical_progress,
    close_stream,
    empty_progress,
    parse_progress,
    totals,
)
from process.custom_import.runner_types import CandidateRunnerError

_HEADER = CustomImportPreparedSource
_PART = CustomImportPreparedSourcePart
_COPY_COLUMNS = (
    "execution_id",
    "stream_slot",
    "part_ordinal",
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "first_source_ordinal",
    "record_count",
    "payload_contract",
    "byte_count",
    "payload",
    "payload_sha256",
    "canonical_manifest",
    "manifest_sha256",
)


class PreparedSourceState(NamedTuple):
    """Detached progress only; never return a live ORM authority object."""

    state: str
    version: int
    progress: tuple


def _invalid():
    return CandidateRunnerError("prepared_source_state_invalid")


def prepared_source_identity(context: PreparedSourceContext) -> PreparedSourceIdentity:
    """Pin originals, binding and policy independently of the renewable lease."""

    if not isinstance(context, PreparedSourceContext):
        raise _invalid()
    return PreparedSourceIdentity(
        **{
            name: getattr(context, name)
            for name in (
                "execution_id",
                "capture_bundle_id",
                "dataset_id",
                "definition_revision_id",
                "schema_revision_id",
                "source_binding_revision_id",
                "source_binding_sha256",
                "request_identity_sha256",
                "raw_manifest_sha256",
            )
        },
        processing_policy_sha256=bytes.fromhex(context.policy.digest),
    )


def _state(header, context):
    identity = prepared_source_identity(context)
    if any(getattr(header, name) != value for name, value in vars(identity).items()) or (
        header.canonical_processing_policy != context.policy.canonical
        or header.preparation_deadline_at != context.build_deadline_at
        or header.encoding_contract != PREPARED_SOURCE_CONTRACT
        or header.state not in ("pending", "closed", "sealed")
        or type(header.version) is not int
        or header.version < 1
    ):
        raise _invalid()
    progress = parse_progress(header.canonical_progress, len(context.stream_slots), context.policy)
    if totals(progress) != (header.part_count, header.record_count, header.byte_count, header.manifest_byte_count):
        raise _invalid()
    if (header.state == "pending") == all(stream.eof for stream in progress):
        raise _invalid()
    if header.state != "sealed" and (
        header.canonical_summary is not None or header.prepared_sha256 is not None or header.sealed_at is not None
    ):
        raise _invalid()
    return PreparedSourceState(header.state, header.version, progress)


def _require_producer(header, context):
    if header.state != "sealed" and (
        header.producer_fence != context.fence
        or header.producer_token_sha256 != lease_token_sha256(context.lease_token)
    ):
        raise _invalid()


@asynccontextmanager
async def _page(session_factory, context):
    if not isinstance(context, PreparedSourceContext):
        raise _invalid()
    async with staging._page_session(session_factory, context) as (session, _):
        await require_prepared_source_context(session, context)
        yield session


async def _header(session, context):
    await staging._prepare_statement(session)
    return await session.scalar(
        select(_HEADER)
        .where(_HEADER.execution_id == context.execution_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


async def _quota(session, context, extra):
    await staging._prepare_statement(session)
    await validate_dataset_retained_bytes(
        session,
        dataset_id=context.dataset_id,
        new_byte_count=max(0, extra),
        maximum_retained_bytes=context.policy.maximum_dataset_retained_bytes,
    )


async def begin_prepared_source(session_factory, context):
    """Create or adopt preparation under the existing dataset/execution/lease locks."""

    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None:
            await staging._prepare_statement(session)
            if await session.scalar(
                select(_PART.execution_id).where(_PART.execution_id == context.execution_id).limit(1)
            ):
                raise _invalid()
            progress = empty_progress(len(context.stream_slots), context.policy)
            canonical = canonical_progress(progress)
            await _quota(
                session, context, len(context.policy.canonical.encode("utf-8")) + len(canonical.encode("utf-8"))
            )
            header = _HEADER(
                **vars(prepared_source_identity(context)),
                canonical_processing_policy=context.policy.canonical,
                encoding_contract=PREPARED_SOURCE_CONTRACT,
                state="pending",
                version=1,
                producer_fence=context.fence,
                producer_token_sha256=lease_token_sha256(context.lease_token),
                preparation_deadline_at=context.build_deadline_at,
                canonical_progress=canonical,
                part_count=0,
                record_count=0,
                byte_count=0,
                manifest_byte_count=0,
            )
            session.add(header)
        else:
            _state(header, context)
            if header.state != "sealed":
                if header.producer_fence > context.fence:
                    raise _invalid()
                if header.producer_fence == context.fence:
                    _require_producer(header, context)
                else:
                    header.producer_fence = context.fence
                    header.producer_token_sha256 = lease_token_sha256(context.lease_token)
                    header.version += 1
        return _state(header, context)


def _part_metadata(context, stream_slot, part_ordinal, part):
    """Hash one bounded immutable part before acquiring database authority locks."""

    if not isinstance(context, PreparedSourceContext):
        raise _invalid()
    if (
        type(part) is not PreparedSourcePart
        or type(part.payload) is not bytes
        or (
            type(stream_slot) is not int
            or not 1 <= stream_slot <= len(context.stream_slots)
            or type(part_ordinal) is not int
            or not 1 <= part_ordinal <= context.policy.prepared["maximum_parts"]
            or type(part.record_count) is not int
            or not 0 <= part.record_count <= context.policy.prepared["part_row_limit"]
            or len(part.payload) > context.policy.prepared["part_byte_limit"]
        )
    ):
        raise _invalid()
    return PreparedPartMetadata(
        stream_slot,
        part_ordinal,
        part.first_source_ordinal,
        part.record_count,
        len(part.payload),
        hashlib.sha256(part.payload).digest(),
    )


async def _observe_part(session, context, metadata):
    """Resolve a previously committed prefix using metadata, without reading payloads."""

    await staging._prepare_statement(session)
    retained_metadata = await session.execute(
        select(
            _PART.dataset_id,
            _PART.definition_revision_id,
            _PART.schema_revision_id,
            _PART.first_source_ordinal,
            _PART.record_count,
            _PART.byte_count,
            _PART.payload_sha256,
            _PART.payload_contract,
            _PART.canonical_manifest,
            _PART.manifest_sha256,
        ).where(
            _PART.execution_id == context.execution_id,
            _PART.stream_slot == metadata.stream_slot,
            _PART.part_ordinal == metadata.part_ordinal,
        )
    )
    expected = (
        context.dataset_id,
        context.definition_revision_id,
        context.schema_revision_id,
        metadata.first_source_ordinal,
        metadata.record_count,
        metadata.byte_count,
        metadata.payload_sha256,
        PREPARED_SOURCE_CONTRACT,
        metadata.canonical,
        metadata.digest,
    )
    if retained_metadata.one_or_none() != expected:
        raise _invalid()


async def _copy_part(session, context, metadata, prepared_payload):
    await staging._prepare_statement(session)
    connection = await session.connection()
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(_PART.__table__.schema, _PART.__table__.schema)
    if not schema:
        raise _invalid()
    raw = await connection.get_raw_connection()
    copy_record = (
        context.execution_id,
        metadata.stream_slot,
        metadata.part_ordinal,
        context.dataset_id,
        context.definition_revision_id,
        context.schema_revision_id,
        metadata.first_source_ordinal,
        metadata.record_count,
        PREPARED_SOURCE_CONTRACT,
        metadata.byte_count,
        prepared_payload,
        metadata.payload_sha256,
        metadata.canonical,
        metadata.digest,
    )
    await raw.driver_connection.copy_records_to_table(
        _PART.__tablename__,
        schema_name=schema,
        columns=_COPY_COLUMNS,
        records=(copy_record,),
    )


def _apply_progress(header, progress):
    header.canonical_progress = canonical_progress(progress)
    header.part_count, header.record_count, header.byte_count, header.manifest_byte_count = totals(progress)
    header.version += 1


async def append_prepared_part(session_factory, context, *, stream_slot, part_ordinal, part):
    """Commit one COPY part and its counters, or observe an exact retained prefix."""

    metadata = _part_metadata(context, stream_slot, part_ordinal, part)
    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None:
            raise _invalid()
        state = _state(header, context)
        _require_producer(header, context)
        if metadata.part_ordinal <= state.progress[stream_slot - 1].part_count:
            await _observe_part(session, context, metadata)
            return state
        if state.state != "pending":
            raise _invalid()
        progress = append_progress(state.progress, metadata, context.policy)
        extra = metadata.byte_count + len(metadata.canonical.encode("utf-8"))
        extra += len(canonical_progress(progress).encode("utf-8")) - len(header.canonical_progress.encode("utf-8"))
        await _quota(session, context, extra)
        await _copy_part(session, context, metadata, part.payload)
        _apply_progress(header, progress)
        return _state(header, context)


async def mark_prepared_stream_eof(session_factory, context, *, stream_slot):
    """Call only after genuine decoder EOF and successful decoder/resource cleanup."""

    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None:
            raise _invalid()
        state = _state(header, context)
        _require_producer(header, context)
        progress = close_stream(state.progress, stream_slot, context.policy)
        if progress == state.progress:
            return state
        if state.state != "pending":
            raise _invalid()
        extra = len(canonical_progress(progress).encode("utf-8")) - len(header.canonical_progress.encode("utf-8"))
        await _quota(session, context, extra)
        _apply_progress(header, progress)
        if all(stream.eof for stream in progress):
            header.state = "closed"
        return _state(header, context)
