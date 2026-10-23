# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keyset metadata verification and a short fenced prepared-source seal."""

from __future__ import annotations

import re

from sqlalchemy import func, select

from process.custom_import import build_source as staging
from process.custom_import.definition import canonical_json
from process.custom_import.prepared_source_codec import PREPARED_SOURCE_CONTRACT
from process.custom_import.prepared_source_metadata import (
    PreparedPartMetadata,
    PreparedStreamFold,
    PreparedStreamSummary,
    prepared_source_summary,
)
from process.custom_import.prepared_source_store import (
    _PART,
    _header,
    _invalid,
    _page,
    _quota,
    _require_producer,
    _state,
    prepared_source_identity,
)
from process.custom_import.source_batch import _read_session

_PAGE_SIZE = 256
_METADATA_COLUMNS = (
    _PART.dataset_id,
    _PART.definition_revision_id,
    _PART.schema_revision_id,
    _PART.stream_slot,
    _PART.part_ordinal,
    _PART.first_source_ordinal,
    _PART.record_count,
    _PART.byte_count,
    _PART.payload_sha256,
    _PART.payload_contract,
    _PART.canonical_manifest,
    _PART.manifest_sha256,
)


def _sealed_digest(header, context, progress):
    if type(header.canonical_summary) is not str:
        raise _invalid()
    if not 2 <= len(header.canonical_summary) <= 2 * 1024 * 1024 or header.sealed_at is None:
        raise _invalid()
    identity = prepared_source_identity(context)
    prefix = canonical_json({"contract": PREPARED_SOURCE_CONTRACT, **identity.to_mapping(), "streams": []})[:-2]
    stream_patterns = []
    for stream in progress:
        canonical_stream = canonical_json(
            {
                "stream_slot": stream.stream_slot,
                "part_count": stream.part_count,
                "record_count": stream.record_count,
                "byte_count": stream.byte_count,
                "manifest_byte_count": stream.manifest_byte_count,
                "part_set_sha256": "",
            }
        )
        stream_patterns.append(
            re.escape(canonical_stream).replace(re.escape('"part_set_sha256":""'), '"part_set_sha256":"([0-9a-f]{64})"')
        )
    # A fixed identity/counter grammar rejects allocation abuse before decoding any field.
    matched = re.fullmatch(re.escape(prefix) + ",".join(stream_patterns) + re.escape("]}"), header.canonical_summary)
    if matched is None:
        raise _invalid()
    summaries = tuple(
        PreparedStreamSummary(
            stream.stream_slot,
            stream.part_count,
            stream.record_count,
            stream.byte_count,
            stream.manifest_byte_count,
            bytes.fromhex(digest),
        )
        for stream, digest in zip(progress, matched.groups(), strict=True)
    )
    summary = prepared_source_summary(identity, summaries, stream_count=len(progress), policy=context.policy)
    if (header.canonical_summary, header.prepared_sha256) != (summary.canonical, summary.digest):
        raise _invalid()
    return summary.digest


async def _closed_page(session_factory, context, expected):
    """Renew between read pages; verify metadata outside authority locks."""

    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None or _state(header, context) != expected or header.state != "closed":
            raise _invalid()
        _require_producer(header, context)


async def _metadata_page(session_factory, context, slot, after):
    async with _read_session(session_factory) as session:
        await staging._set_timeout(session, context.statement_timeout_ms)
        return tuple(
            (
                await session.execute(
                    select(*_METADATA_COLUMNS)
                    .where(
                        _PART.execution_id == context.execution_id,
                        _PART.stream_slot == slot,
                        _PART.part_ordinal > after,
                    )
                    .order_by(_PART.part_ordinal)
                    .limit(_PAGE_SIZE)
                )
            ).all()
        )


def _part_metadata(row, context):
    if tuple(row[:3]) != (context.dataset_id, context.definition_revision_id, context.schema_revision_id) or (
        row[9] != PREPARED_SOURCE_CONTRACT
    ):
        raise _invalid()
    metadata = PreparedPartMetadata(*row[3:8], bytes(row[8]))
    metadata.verify_manifest(row[10], bytes(row[11]))
    return metadata


async def _stream_summary(session_factory, context, expected, progress):
    fold = PreparedStreamFold(progress.stream_slot, context.policy)
    after = 0
    while True:
        await _closed_page(session_factory, context, expected)
        rows = await _metadata_page(session_factory, context, progress.stream_slot, after)
        for row in rows:
            metadata = _part_metadata(row, context)
            fold.add(metadata)
            after = metadata.part_ordinal
        if len(rows) < _PAGE_SIZE:
            break
    summary = fold.finish()
    if (summary.part_count, summary.record_count, summary.byte_count, summary.manifest_byte_count) != (
        progress.part_count,
        progress.record_count,
        progress.byte_count,
        progress.manifest_byte_count,
    ):
        raise _invalid()
    return summary


async def _reject_undeclared_streams(session_factory, context):
    async with _read_session(session_factory) as session:
        await staging._set_timeout(session, context.statement_timeout_ms)
        if await session.scalar(
            select(_PART.execution_id)
            .where(
                _PART.execution_id == context.execution_id,
                _PART.stream_slot.not_in(tuple(slot for _, slot in context.stream_slots)),
            )
            .limit(1)
        ):
            raise _invalid()


async def seal_prepared_source(session_factory, context):
    """Seal only complete retained metadata, without payload rereads or caller summaries."""

    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None:
            raise _invalid()
        expected = _state(header, context)
        if expected.state == "sealed":
            return _sealed_digest(header, context, expected.progress)
        _require_producer(header, context)
        if expected.state != "closed":
            raise _invalid()
    summaries = []
    for progress in expected.progress:
        summaries.append(await _stream_summary(session_factory, context, expected, progress))
    await _reject_undeclared_streams(session_factory, context)
    summary = prepared_source_summary(
        prepared_source_identity(context),
        summaries,
        stream_count=len(context.stream_slots),
        policy=context.policy,
    )
    async with _page(session_factory, context) as session:
        header = await _header(session, context)
        if header is None or _state(header, context) != expected:
            raise _invalid()
        _require_producer(header, context)
        await _quota(session, context, len(summary.canonical.encode("utf-8")))
        await staging._prepare_statement(session)
        now = await session.scalar(select(func.clock_timestamp()))
        header.canonical_summary, header.prepared_sha256 = summary.canonical, summary.digest
        header.sealed_at, header.state = now, "sealed"
        header.version += 1
        return summary.digest
