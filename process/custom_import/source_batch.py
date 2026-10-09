# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One trusted, bounded retained SOURCE step; no HTTP, publication or retry."""

from __future__ import annotations

import asyncio
import datetime as dt
import hmac
import json
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from uuid import UUID

from sqlalchemy import and_, select, text

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildStream,
    CustomImportCaptureBundle,
    CustomImportExecution,
    CustomImportSourceBindingRevision,
)
from process.custom_import import build_source as staging
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS, encode_landing_batch
from process.custom_import.capture_store import open_segmented_cursor_part, verify_segmented_stream_metadata
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _stream_fields,
    _validate_replay_partition_schema,
)
from process.custom_import.snowflake_source_binding import load_snowflake_source_binding
from process.custom_import.source_authorization import PERMIT_CONTRACT, SOURCE_PATH, SourcePermit
from process.custom_import.source_finalize_sql import finalize_source_batch
from process.custom_import.storage_layout import snapshot_models


@dataclass(frozen=True)
class SourceCursor:
    """The complete durable stream cursor, never process-local replay state."""

    next_part_ordinal: int
    next_part_row_ordinal: int
    next_source_ordinal: int
    next_pack_ordinal: int

    def __post_init__(self):
        for name, maximum in (
            ("next_part_ordinal", 2**31 - 1),
            ("next_part_row_ordinal", 2**63 - 1),
            ("next_source_ordinal", 2**63 - 1),
            ("next_pack_ordinal", 2**31 - 1),
        ):
            value = getattr(self, name)
            if type(value) is not int or not (1 if name == "next_part_ordinal" else 0) <= value <= maximum:
                raise ValueError("SOURCE cursor is malformed")

    @classmethod
    def from_model(cls, stream):
        """Freeze all four validated fields from retained stream state."""
        return cls(*(getattr(stream, name) for name in cls.__dataclass_fields__))


class SourceCursorConflict(RuntimeError):
    """No resend: reconcile the complete retained cursor and completion state."""

    def __init__(self, actual_cursor, stream_complete, phase):
        super().__init__("SOURCE cursor changed; reconcile retained progress before retry")
        self.actual_cursor, self.stream_complete, self.phase = actual_cursor, stream_complete, phase


@dataclass(frozen=True)
class SourceBatchReceipt:
    """Returned only after the owned transaction's successful commit/cleanup."""

    execution_id: int
    build_id: int
    fence: int
    stream_slot: int
    capture_bundle_id: int
    before: SourceCursor
    after: SourceCursor
    phase: str
    rows_processed: int
    stream_complete: bool


def _positive_id(value):
    if type(value) is not int or not 0 < value < 2**63:
        raise ValueError("SOURCE step identifiers must be positive bigints")


def _expiry(value):
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() != dt.timedelta(0):
        raise ValueError("SOURCE authorization expiry must be UTC")


def _same_digest(actual, expected):
    return (
        isinstance(actual, (bytes, bytearray, memoryview))
        and isinstance(expected, (bytes, bytearray, memoryview))
        and len(actual) == len(expected) == 32
        and hmac.compare_digest(bytes(actual), bytes(expected))
    )


async def _load_request(session, execution_id, build_id, fence, lease_token, source_permit):
    """Load immutable definition/binding/bounds; no caller-supplied policy."""

    build = await session.get(CustomImportBuildAttempt, build_id)
    execution = await session.get(CustomImportExecution, execution_id)
    if build is None or execution is None or build.execution_id != execution_id:
        raise CandidateRunnerError("SOURCE execution/build binding is unavailable")
    loaded = await load_snowflake_source_binding(
        session,
        definition_revision_id=build.definition_revision_id,
        source_binding_revision_id=execution.source_binding_revision_id,
    )
    bundle = await session.get(CustomImportCaptureBundle, build.capture_bundle_id)
    policy = loaded.binding.processing_policy
    if (
        policy is None
        or bundle is None
        or bundle.capture_state != "sealed"
        or bundle.payload_contract != "custom-import/parquet-parts/v2"
        or (loaded.dataset_id, loaded.schema_revision_id) != (build.dataset_id, build.schema_revision_id)
        or (bundle.dataset_id, bundle.definition_revision_id, bundle.schema_revision_id)
        != (build.dataset_id, build.definition_revision_id, build.schema_revision_id)
        or bundle.source_binding_revision_id != loaded.source_binding_revision_id
        or not _same_digest(bundle.source_binding_sha256, loaded.source_binding_sha256)
        or not _same_digest(bundle.request_identity_sha256, execution.request_identity_sha256)
        or bundle.canonical_policy != policy.capture.canonical
        or not _same_digest(bundle.policy_sha256, bytes.fromhex(policy.capture.digest))
        or any(
            getattr(build, name) != getattr(policy.build, name)
            for name in ("page_row_limit", "page_byte_limit", "statement_timeout_ms")
        )
    ):
        raise CandidateRunnerError("SOURCE retained capture/definition/policy identity differs")
    request = staging.SourceBuildRequest(
        dataset_id=build.dataset_id,
        definition_revision_id=build.definition_revision_id,
        schema_revision_id=build.schema_revision_id,
        execution_id=execution_id,
        lease_token=lease_token,
        fence=fence,
        definition=loaded.definition,
        expected_base_generation_id=build.base_generation_id,
        expected_pointer_version=build.base_pointer_version,
        complete_scope=build.complete_scope,
        page_row_limit=build.page_row_limit,
        page_byte_limit=build.page_byte_limit,
        statement_timeout_ms=build.statement_timeout_ms,
        build_deadline_at=build.build_deadline_at,
        lease_seconds=policy.build.lease_seconds,
        authorization_expires_at=source_permit.expires_at,
    )
    staging._assert_build_identity(build, request, execution)
    return request, policy.capture, build.capture_bundle_id


async def _require_owner(session):
    """Check both exact mapped entry-point owners; never grant or SET ROLE."""

    if not await staging._is_source_writer_owner(session):
        raise RuntimeError("trusted SOURCE requires the existing entry-point owners")


async def _verify_source_permit(session, request, source_permit):
    """Recheck exact signed scope under the existing execution/build locks."""
    if not isinstance(source_permit, SourcePermit) or (
        source_permit.contract != PERMIT_CONTRACT
        or source_permit.path != SOURCE_PATH
        or source_permit.method != "POST"
        or source_permit.issuer != "custom-import-execution-controller"
        or source_permit.audience != "custom-import-engine"
        or request.dataset_id != source_permit.dataset_id
        or request.definition_revision_id != source_permit.definition_revision_id
        or request.schema_revision_id != source_permit.schema_revision_id
        or request.authorization_expires_at != source_permit.expires_at
    ):
        raise RuntimeError("custom_import_source_authority_mismatch")
    await staging._prepare_statement(session)
    execution, binding = CustomImportExecution, CustomImportSourceBindingRevision
    matched = (
        await session.execute(
            select(execution.execution_id)
            .join(
                binding,
                and_(
                    binding.source_binding_revision_id == execution.source_binding_revision_id,
                    binding.dataset_id == execution.dataset_id,
                    binding.definition_revision_id == execution.definition_revision_id,
                    binding.schema_revision_id == execution.schema_revision_id,
                ),
            )
            .where(
                execution.execution_id == request.execution_id,
                execution.dataset_id == source_permit.dataset_id,
                execution.definition_revision_id == source_permit.definition_revision_id,
                execution.schema_revision_id == source_permit.schema_revision_id,
                execution.idempotency_key == source_permit.idempotency_key,
                execution.source_binding_revision_id == source_permit.source_binding_revision_id,
                binding.binding_sha256 == bytes.fromhex(source_permit.source_binding_sha256),
            )
        )
    ).scalar_one_or_none()
    if matched != request.execution_id:
        raise RuntimeError("custom_import_source_authority_mismatch")
    await _require_owner(session)


@asynccontextmanager
async def _read_session(session_factory):
    """Own only a read transaction; drain session cleanup on repeated cancel."""

    contexts, primary = [], None
    try:
        session = await staging._enter_context(contexts, session_factory())
        await staging._enter_context(contexts, session.begin())
        yield session
    except BaseException as exc:
        primary = exc
    primary = await staging._close_contexts(contexts, primary)
    if primary is not None:
        raise primary


@asynccontextmanager
async def _page(session_factory, request, build_id, source_permit):
    async with staging._page_session(session_factory, request, build_id) as (session, build):
        await session.execute(text("SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"))
        await _verify_source_permit(session, request, source_permit)
        yield session, build


async def _cursor(session, build_id, stream_slot):
    await staging._prepare_statement(session)
    return (
        await session.scalars(
            select(CustomImportBuildStream)
            .where(
                CustomImportBuildStream.build_id == build_id,
                CustomImportBuildStream.stream_slot == stream_slot,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
    ).one()


def _match_cursor(stream, expected, phase):
    actual = SourceCursor.from_model(stream)
    is_complete = stream.replay_verified_at is not None
    if actual != expected or phase != "source":
        raise SourceCursorConflict(actual, is_complete, phase)


async def _compare_prefix(session_factory, context, page, source_permit, deadline):
    """Compare one bounded contiguous range under the original request window."""
    if (
        not 0 < len(page.records) <= MAX_BATCH_ROWS
        or sum(prepared_row.byte_count for prepared_row in page.records) > MAX_BATCH_BYTES
    ):
        raise CandidateRunnerError("SOURCE prefix comparison exceeds physical batch bounds")
    async with _page(session_factory, context.request, context.build_id, source_permit) as (session, build):
        if build.phase != "source":
            raise CandidateRunnerError("committed source phase changed during prefix comparison")
        window = session.info[staging._WINDOW]
        session.info[staging._WINDOW] = replace(window, deadline=min(window.deadline, deadline))
        family_id = await staging._resolve_build_snapshot(session, context.build_id)
        await staging._prepare_statement(session)
        stored_rows = (
            await session.execute(
                staging._committed_prefix_statement(context, page, models=snapshot_models(family_id)).limit(
                    len(page.records)
                )
            )
        ).all()
        if len(stored_rows) != len(page.records) or any(
            staging._stored_fingerprint(stored_row) != staging._prepared_fingerprint(page, offset)
            for offset, stored_row in enumerate(stored_rows)
        ):
            raise CandidateRunnerError("committed source prefix differs from sealed replay")
        root_ids = sorted(
            {stored_row[0].root_revision_id for stored_row in stored_rows if stored_row[0].root_revision_id is not None}
        )
        child_ids = sorted(
            {
                stored_row[0].child_revision_id
                for stored_row in stored_rows
                if stored_row[0].child_revision_id is not None
            }
        )
        homes = (
            await staging._call(
                session,
                "lookup_custom_import_revision_home",
                (("bigint[]", root_ids), ("bigint[]", child_ids)),
            )
        ).all()
        expected_homes = {(1, identifier) for identifier in root_ids} | {(2, identifier) for identifier in child_ids}
        if (
            len(homes) != len(expected_homes)
            or {(home.revision_kind, home.revision_id) for home in homes} != expected_homes
            or any(home.family_id != family_id for home in homes)
        ):
            raise CandidateRunnerError("committed source revision homes have incomplete coverage")


async def _compare_prefix_batch(session_factory, context, pages, source_permit, deadline):
    """Combine only adjacent logical pages; discard the verified physical range."""
    first, records = pages[0], []
    for page in pages:
        if (page.part_ordinal, page.first_row, page.first_source) != (
            first.part_ordinal,
            first.first_row + len(records),
            first.first_source + len(records),
        ):
            raise CandidateRunnerError("SOURCE prefix pages are not contiguous")
        records.extend(page.records)
    await _compare_prefix(session_factory, context, replace(first, records=tuple(records)), source_permit, deadline)


async def _verify_prefix(session_factory, context, pages, cursor, source_permit, deadline):
    """Verify bounded same-part ranges, returning the first uncommitted page."""
    prefix_pages, row_count, byte_count = [], 0, 0
    while True:
        if time.monotonic() >= deadline:
            raise LeaseAuthorityLost("SOURCE read deadline elapsed")
        page = await staging._next_source_page(pages)
        page_rows = 0 if page is None else len(page.records)
        page_bytes = 0 if page is None else sum(prepared_row.byte_count for prepared_row in page.records)
        is_prefix = page is not None and page.first_row < cursor.next_part_row_ordinal
        if prefix_pages and (
            not is_prefix or row_count + page_rows > MAX_BATCH_ROWS or byte_count + page_bytes > MAX_BATCH_BYTES
        ):
            await _compare_prefix_batch(session_factory, context, prefix_pages, source_permit, deadline)
            prefix_pages.clear()
            row_count = byte_count = 0
            if time.monotonic() >= deadline:
                raise LeaseAuthorityLost("SOURCE read deadline elapsed")
        if not is_prefix:
            return page
        prefix_pages.append(page)
        row_count += page_rows
        byte_count += page_bytes
        await asyncio.sleep(0)


def _verify_part(context, part, policy, cursor):
    """Retain schema, position and actual allocation bounds before preparation."""
    fields = _stream_fields(context.request.definition, context.stream)
    _validate_replay_partition_schema(part.capture, fields=fields, limits=policy.part_limits)
    if cursor.next_part_row_ordinal > part.record_count or cursor.next_source_ordinal < cursor.next_part_row_ordinal:
        raise CandidateRunnerError("SOURCE cursor exceeds the retained part")
    arrow_limits = replace(
        policy.part_limits,
        maximum_decoded_bytes=max(policy.part_limits.maximum_decoded_bytes, policy.maximum_part_arrow_bytes),
    )
    actual_arrow = _aggregate_parquet_arrow_bytes(part.capture, limits=arrow_limits, decoded_bytes=0)
    # Landing allocation includes its original null bitmaps; replay allocation
    # can differ. Sealed accounting stays exact, and actual decode has its own cap.
    if actual_arrow > policy.maximum_part_arrow_bytes:
        raise CandidateRunnerError("decoded part Arrow bytes exceed the admitted replay limit")


async def _prepare_part(session_factory, context, part, policy, cursor, source_permit, deadline, budget=None):
    """Buffer one bounded batch; stream/compare the current part's old prefix.

    Late resume scans at most the retained part's finite row/byte limits, never
    prior payloads. Each bounded prefix range is discarded before buffering new rows.
    The optional budget bounds rows, bytes and the soft preparation deadline.
    """
    if budget is None:
        budget = MAX_BATCH_ROWS, MAX_BATCH_BYTES, deadline
    _verify_part(context, part, policy, cursor)
    pages = staging._source_pages(
        context,
        part,
        policy,
        (
            cursor.next_part_ordinal,
            cursor.next_part_row_ordinal,
            cursor.next_source_ordinal - cursor.next_part_row_ordinal,
        ),
    )
    prepared_pages, row_count, byte_count, has_eof = [], 0, 0, False
    primary = None
    try:
        first_page = await _verify_prefix(session_factory, context, pages, cursor, source_permit, deadline)
        while True:
            now = time.monotonic()
            if now >= deadline:
                raise LeaseAuthorityLost("SOURCE read deadline elapsed")
            # Reserve a full logical page before requesting the next one; the
            # decoder must actually exhaust before any part can be finished.
            if prepared_pages and (
                now >= budget[2]
                or row_count + context.request.page_row_limit > budget[0]
                or byte_count + context.request.page_byte_limit > budget[1]
            ):
                break
            page = first_page if not prepared_pages else await staging._next_source_page(pages)
            if page is None:
                has_eof = True
                break
            prepared_pages.append(page)
            row_count += len(page.records)
            byte_count += sum(prepared_row.byte_count for prepared_row in page.records)
            await asyncio.sleep(0)
    except BaseException as exc:
        primary = exc
    primary = await staging._close_source_pages(pages, primary)
    if primary is not None:
        raise primary
    return tuple(prepared_pages), has_eof


async def _copy_and_finalize(session, context, stream, pages, closed_parts):
    """Authorize, native COPY and ordinary promotion in this same transaction."""
    preview = encode_landing_batch(context, pages, batch_id=UUID(int=0), first_pack_ordinal=stream.next_pack_ordinal)
    earlier_parts = staging._source_closed_prefix(
        context, stream, pages, ((context.stream_slot, ordinal) for ordinal in closed_parts)
    )
    batch_id = (
        await staging._call(
            session,
            "source_bulk_authorize",
            (
                ("bigint", context.build_id),
                ("smallint", context.stream_slot),
                ("bigint", context.request.fence),
                ("bytea", lease_token_sha256(context.request.lease_token)),
                ("integer", len(preview.records)),
                ("bigint", preview.byte_count),
            ),
        )
    ).scalar_one()
    landing_records = tuple(
        (batch_id, ordinal, *landing_row[1:]) for ordinal, landing_row in enumerate(preview.records)
    )
    await staging._copy_source_landing(session, landing_records)
    completed = await finalize_source_batch(session, batch_id, earlier_parts)
    if type(completed) is not int or completed != len(landing_records):
        raise RuntimeError("completed SOURCE count differs from attempted rows")
    return completed


async def _freeze_if_verified(session, context, build):
    """Freeze only complete durable stream coverage via the existing authority."""
    await staging._prepare_statement(session)
    stream_rows = (
        await session.scalars(
            select(CustomImportBuildStream)
            .where(CustomImportBuildStream.build_id == context.build_id)
            .execution_options(populate_existing=True)
        )
    ).all()
    if stream_rows and all(stream.replay_verified_at is not None for stream in stream_rows):
        await staging._call(session, "freeze_custom_import_build_source", (("bigint", context.build_id),))
        await session.refresh(build)


async def _commit(session_factory, context, pages, closed_parts, expected, bundle_id, source_permit):
    """Commit progress once; return no receipt on rollback or cleanup failure."""
    async with _page(session_factory, context.request, context.build_id, source_permit) as (session, build):
        stream = await _cursor(session, context.build_id, context.stream_slot)
        _match_cursor(stream, expected, build.phase)
        is_already_complete = stream.replay_verified_at is not None
        if is_already_complete and (pages or closed_parts):
            raise SourceCursorConflict(SourceCursor.from_model(stream), True, build.phase)
        completed = await _copy_and_finalize(session, context, stream, pages, closed_parts) if pages else 0
        last_promoted_part = pages[-1].part_ordinal if pages else expected.next_part_ordinal
        for ordinal in closed_parts:
            if ordinal < last_promoted_part:
                continue  # The same finalization atomically proved these earlier parts.
            await staging._call(
                session,
                "finish_custom_import_build_source_part",
                (
                    ("bigint", context.build_id),
                    ("smallint", context.stream_slot),
                    ("integer", ordinal),
                ),
            )
        if closed_parts or is_already_complete:
            await _freeze_if_verified(session, context, build)
        stream = await _cursor(session, context.build_id, context.stream_slot)
        after = SourceCursor.from_model(stream)
        if after.next_source_ordinal - expected.next_source_ordinal != completed:
            raise RuntimeError("SOURCE completion lacks exact durable row progress")
        if after == expected and build.phase != "admission":
            raise SourceCursorConflict(after, stream.replay_verified_at is not None, build.phase)
        receipt = SourceBatchReceipt(
            context.request.execution_id,
            context.build_id,
            context.request.fence,
            context.stream_slot,
            bundle_id,
            expected,
            after,
            build.phase,
            completed,
            stream.replay_verified_at is not None,
        )
    return receipt


async def _load_context(
    session_factory, execution_id, build_id, fence, stream_slot, expected_cursor, lease_token, source_permit
):
    """Load the persisted scope, then recheck under existing authority locks."""
    async with _read_session(session_factory) as session:
        await session.execute(text("SELECT pg_catalog.set_config('statement_timeout', '1000', true)"))
        request, policy, bundle_id = await _load_request(
            session, execution_id, build_id, fence, lease_token, source_permit
        )
    async with _page(session_factory, request, build_id, source_permit) as (session, build):
        registry = await load_registry(session, request)
        stream = await _cursor(session, build_id, stream_slot)
        _match_cursor(stream, expected_cursor, build.phase)
        declared = next(
            (
                declared_stream
                for declared_stream in request.definition.source_streams
                if registry.stream_slots[declared_stream.stream_id] == stream_slot
            ),
            None,
        )
        if declared is None:
            raise CandidateRunnerError("SOURCE stream is not in the retained definition")
        deadline = session.info[staging._WINDOW].deadline
        is_complete = stream.replay_verified_at is not None
    return staging._StreamContext(request, registry, build_id, declared), policy, bundle_id, deadline, is_complete


def _has_capacity(request, rows, byte_count):
    return rows + request.page_row_limit <= MAX_BATCH_ROWS and byte_count + request.page_byte_limit <= MAX_BATCH_BYTES


async def _read_timeout(session, request, deadline):
    remaining_ms = int((deadline - time.monotonic()) * 1000)
    if remaining_ms < 4:
        raise LeaseAuthorityLost("SOURCE read deadline elapsed")
    await staging._set_timeout(session, min(request.statement_timeout_ms, remaining_ms // 2))


async def _fetch_closed_part(session_factory, identity_by_field, context, cursor, deadline):
    """Return immutable bytes only after result/read transaction cleanup succeeds."""
    async with _read_session(session_factory) as session:
        await _read_timeout(session, context.request, deadline)
        async with open_segmented_cursor_part(
            session, **identity_by_field, stream_slot=context.stream_slot, part_ordinal=cursor.next_part_ordinal
        ) as part:
            captured = part
    return captured


async def _verify_metadata(session_factory, identity_by_field, context, deadline):
    async with _read_session(session_factory) as session:
        await _read_timeout(session, context.request, deadline)
        await verify_segmented_stream_metadata(
            session, **identity_by_field, stream_slot=context.stream_slot, deadline=deadline
        )


async def _read_parts(session_factory, context, policy, bundle_id, cursor, source_permit, deadline):
    """Coalesce sequential parts with no read connection held during preparation."""
    request = context.request
    identity_by_field = dict(
        capture_bundle_id=bundle_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    pages, closed_parts, row_count, byte_count = [], [], 0, 0
    started = time.monotonic()
    # Leave half of the remaining request window for the fenced write/commit.
    soft_deadline = started + (deadline - started) / 2
    # Empty parts also have a finite per-call metadata/reader-work ceiling.
    while len(closed_parts) < MAX_BATCH_ROWS and (not pages or _has_capacity(request, row_count, byte_count)):
        now = time.monotonic()
        if now >= deadline:
            raise LeaseAuthorityLost("SOURCE read deadline elapsed")
        if (pages or closed_parts) and now >= soft_deadline:
            break
        part = await _fetch_closed_part(session_factory, identity_by_field, context, cursor, deadline)
        new_pages, has_eof = await _prepare_part(
            session_factory,
            context,
            part,
            policy,
            cursor,
            source_permit,
            deadline,
            (MAX_BATCH_ROWS - row_count, MAX_BATCH_BYTES - byte_count, soft_deadline),
        )
        is_final = part.ordinal == json.loads(part.receipt.canonical_manifest)["part_count"]
        del part
        # No EOF/earlier-part evidence until decode AND reader cleanup succeed.
        pages.extend(new_pages)
        added_rows = sum(len(page.records) for page in new_pages)
        row_count += added_rows
        byte_count += sum(prepared_row.byte_count for page in new_pages for prepared_row in page.records)
        if not has_eof:
            break
        closed_parts.append(cursor.next_part_ordinal)
        if is_final:
            await _verify_metadata(session_factory, identity_by_field, context, deadline)
            break
        cursor = SourceCursor(
            cursor.next_part_ordinal + 1,
            0,
            cursor.next_source_ordinal + added_rows,
            cursor.next_pack_ordinal + len(new_pages),
        )
    if time.monotonic() >= deadline:
        raise LeaseAuthorityLost("SOURCE read deadline elapsed")
    return tuple(pages), tuple(closed_parts)


async def serve_source_batch(
    session_factory,
    *,
    execution_id: int,
    build_id: int,
    fence: int,
    stream_slot: int,
    expected_cursor: SourceCursor,
    lease_token: str | bytes,
    source_permit: SourcePermit,
) -> SourceBatchReceipt:
    """Consume a retained part range with independently authenticated authority.

    Caller authenticates the exact operation/pins, original lease bytes and capped
    expiry, never rows, SQL or limits. Conflicts/uncertain commits grant no retry.
    """
    for identifier in (execution_id, build_id, fence, stream_slot):
        _positive_id(identifier)
    if stream_slot > 2**15 - 1:
        raise ValueError("SOURCE stream slot must be a positive smallint")
    if not isinstance(expected_cursor, SourceCursor):
        raise TypeError("SOURCE requires the complete expected cursor")
    if not isinstance(source_permit, SourcePermit):
        raise TypeError("SOURCE requires its verified typed permit")
    _expiry(source_permit.expires_at)
    lease_token_sha256(lease_token)
    context, policy, bundle_id, deadline, is_complete = await _load_context(
        session_factory,
        execution_id,
        build_id,
        fence,
        stream_slot,
        expected_cursor,
        lease_token,
        source_permit,
    )
    pages, closed_parts = (
        ((), ())
        if is_complete
        else await _read_parts(session_factory, context, policy, bundle_id, expected_cursor, source_permit, deadline)
    )
    return await _commit(session_factory, context, pages, closed_parts, expected_cursor, bundle_id, source_permit)
