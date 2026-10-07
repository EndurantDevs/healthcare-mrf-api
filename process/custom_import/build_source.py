# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded, resumable staging of sealed source bytes; no generation publication."""

from __future__ import annotations

import asyncio
import datetime as dt
import hmac
import json
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, replace
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
    CustomImportCaptureBundle,
    CustomImportChildRevision,
    CustomImportLease,
    CustomImportRejection,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportSelectionProfile,
)
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS, encode_landing_batch
from process.custom_import.capture import iter_records
from process.custom_import.capture_store import open_segmented_parquet_parts
from process.custom_import.definition import CustomImportDefinition, SourceStream
from process.custom_import.execution import MAX_LEASE_SECONDS, LeaseGrant, heartbeat_execution, lease_token_sha256
from process.custom_import.family import FamilyRejection, _canonical_child_key, _key, _parent_key, _record_error
from process.custom_import.family_raw_key import raw_family_key_evidence
from process.custom_import.materialization import DefinitionIdentity, selection_profile_models
from process.custom_import.runner_codec import (
    child_key_document,
    digest_text,
    record_payload,
    root_key_evidence_from_tuple,
)
from process.custom_import.runner_graph import rejection_model
from process.custom_import.runner_registry import (
    has_profile_mismatch,
    load_registry,
    lock_dataset,
    lock_execution,
    lock_lease,
    verify_live_attempt,
)
from process.custom_import.runner_types import (
    CandidateRegistry,
    CandidateRunnerError,
    LeaseAuthorityLost,
    SessionFactory,
)
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake_bundle_replay import (
    _aggregate_parquet_arrow_bytes,
    _normalized_integer_replay_values,
    _stream_fields,
    _validate_replay_partition_schema,
)
from process.custom_import.storage_layout import snapshot_models, snapshot_schema

_WINDOW = "custom_import_build_page_window"


@dataclass(frozen=True)
class SourceBuildRequest:
    """Explicit trusted build bounds and an already-claimed execution identity."""

    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    execution_id: int
    lease_token: str | bytes = field(repr=False)
    fence: int
    definition: CustomImportDefinition
    expected_base_generation_id: int | None
    expected_pointer_version: int
    complete_scope: bool
    page_row_limit: int
    page_byte_limit: int
    statement_timeout_ms: int
    build_deadline_at: dt.datetime
    lease_seconds: int
    # Request-local authority, never a retained build identity or bound.
    authorization_expires_at: dt.datetime | None = field(default=None, compare=False)

    def __post_init__(self) -> None:
        for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id", "fence"):
            _integer(getattr(self, name), name, 1, (1 << 63) - 1)
        _integer(self.expected_pointer_version, "expected_pointer_version", 0, (1 << 63) - 1)
        if self.expected_base_generation_id is None:
            if self.expected_pointer_version != 0:
                raise ValueError("an absent base requires pointer version zero")
        else:
            _integer(self.expected_base_generation_id, "expected_base_generation_id", 1, (1 << 63) - 1)
            if self.expected_pointer_version == 0:
                raise ValueError("a retained base requires a positive pointer version")
        _integer(self.page_row_limit, "page_row_limit", 1, 256)
        _integer(self.page_byte_limit, "page_byte_limit", 1, 268_435_456)
        _integer(self.statement_timeout_ms, "statement_timeout_ms", 1, (1 << 31) - 1)
        _integer(self.lease_seconds, "lease_seconds", 1, MAX_LEASE_SECONDS)
        if type(self.complete_scope) is not bool or type(self.lease_token) not in (str, bytes):
            raise ValueError("build scope and immutable lease token are required")
        lease_token_sha256(self.lease_token)
        if (
            not isinstance(self.build_deadline_at, dt.datetime)
            or self.build_deadline_at.tzinfo is None
            or self.build_deadline_at.utcoffset() is None
        ):
            raise ValueError("build deadline must be timezone aware")
        if not isinstance(self.definition, CustomImportDefinition):
            raise ValueError("build definition is required")
        if self.authorization_expires_at is not None and (
            type(self.authorization_expires_at) is not dt.datetime
            or self.authorization_expires_at.tzinfo is None
            or self.authorization_expires_at.utcoffset() != dt.timedelta(0)
        ):
            raise ValueError("authorization expiry must be UTC aware")
        rebuilt = CustomImportDefinition.from_json(self.definition.canonical)
        if rebuilt != self.definition:
            raise ValueError("build definition does not match its canonical document")
        object.__setattr__(self, "definition", rebuilt)


def _integer(value: object, label: str, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{label} is outside its bounded integer range")


@dataclass(frozen=True)
class SourceBuildResult:
    build_id: int
    execution_id: int
    phase: str
    source_occurrence_count: int
    candidate_error_count: int


@dataclass(frozen=True)
class _PageWindow:
    deadline: float
    statement_timeout_ms: int


async def _set_timeout(session: AsyncSession, milliseconds: int) -> None:
    with session.no_autoflush:
        await session.execute(select(func.set_config("statement_timeout", str(milliseconds), True)))


async def _prepare_statement(session: AsyncSession) -> None:
    """Keep each statement strictly inside the transaction's fenced time window."""

    window = session.info[_WINDOW]
    remaining_ms = int((window.deadline - time.monotonic()) * 1_000)
    if remaining_ms < 4:
        raise LeaseAuthorityLost("build page deadline elapsed")
    # Reserve half the remaining window for the statement's completion/check.
    await _set_timeout(session, min(window.statement_timeout_ms, remaining_ms // 2))


async def _flush_page(session: AsyncSession) -> None:
    await _prepare_statement(session)
    await session.flush()


def _assert_build_identity(build, request: SourceBuildRequest, execution) -> None:
    if (
        build.dataset_id != request.dataset_id
        or build.definition_revision_id != request.definition_revision_id
        or build.schema_revision_id != request.schema_revision_id
        or build.execution_id != request.execution_id
        or build.capture_bundle_id != execution.capture_bundle_id
        or build.request_identity_sha256 != execution.request_identity_sha256
        or build.producing_fence != request.fence
        or not hmac.compare_digest(bytes(build.producing_token_sha256), lease_token_sha256(request.lease_token))
        or build.base_generation_id != request.expected_base_generation_id
        or build.base_pointer_version != request.expected_pointer_version
        or build.complete_scope != request.complete_scope
        or build.refresh_mode != request.definition.refresh_mode
        or build.page_row_limit != request.page_row_limit
        or build.page_byte_limit != request.page_byte_limit
        or build.statement_timeout_ms != request.statement_timeout_ms
        or build.build_deadline_at != request.build_deadline_at
    ):
        raise CandidateRunnerError("build identity or bounds do not match the request")


async def _initial_page_window(session, request):
    """Bound lock acquisition before trusting any mutable ownership snapshot."""

    await _set_timeout(session, request.statement_timeout_ms)
    started = time.monotonic()
    expires_at, now, isolation = (
        await session.execute(
            select(
                CustomImportLease.expires_at, func.clock_timestamp(), func.current_setting("transaction_isolation")
            ).where(CustomImportLease.execution_id == request.execution_id)
        )
    ).one()
    if isolation != "read committed" or expires_at is None:
        raise CandidateRunnerError("build pages require a live READ COMMITTED lease")
    session.info[_WINDOW] = _PageWindow(
        started
        + (
            min(expires_at, request.build_deadline_at, request.authorization_expires_at or request.build_deadline_at)
            - now
        ).total_seconds(),
        request.statement_timeout_ms,
    )
    return expires_at


async def _lock_page(
    session: AsyncSession, request: SourceBuildRequest, build_id: int | None = None
) -> CustomImportBuildAttempt | None:
    """Bind a caller-owned transaction to one short live build window."""

    if not session.in_transaction() or session.new or session.dirty or session.deleted:
        raise CandidateRunnerError("build pages require a clean caller-owned transaction")
    expires_at = await _initial_page_window(session, request)
    await _prepare_statement(session)
    await lock_dataset(session, request.dataset_id)
    await _prepare_statement(session)
    execution = await lock_execution(session, request)
    await _prepare_statement(session)
    lease = await lock_lease(session, request.execution_id)
    await _prepare_statement(session)
    grant = LeaseGrant(request.execution_id, request.fence, expires_at, "running")
    now = await verify_live_attempt(session, request, grant, execution, lease)
    build = None
    if build_id is not None:
        await _prepare_statement(session)
        build = (
            await session.scalars(
                select(CustomImportBuildAttempt)
                .where(CustomImportBuildAttempt.build_id == build_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).one()
        _assert_build_identity(build, request, execution)
    if request.build_deadline_at <= now:
        raise LeaseAuthorityLost("build deadline elapsed")
    await _renew_page_window(session, request)
    return build


async def _renew_page_window(session, request):
    """Renew the existing lease without extending the immutable build deadline."""

    await _prepare_statement(session)
    renewed = await heartbeat_execution(
        session,
        execution_id=request.execution_id,
        fence=request.fence,
        token=request.lease_token,
        lease_seconds=request.lease_seconds,
    )
    if renewed is None or renewed.state != "running":
        raise LeaseAuthorityLost("build lease is no longer current")
    started = time.monotonic()
    await _prepare_statement(session)
    now = (await session.execute(select(func.clock_timestamp()))).scalar_one()
    session.info[_WINDOW] = _PageWindow(
        started
        + (
            min(
                renewed.expires_at,
                request.build_deadline_at,
                request.authorization_expires_at or request.build_deadline_at,
            )
            - now
        ).total_seconds(),
        request.statement_timeout_ms,
    )
    await _prepare_statement(session)


@asynccontextmanager
async def _page_session(
    session_factory: SessionFactory, request: SourceBuildRequest, build_id: int | None = None
) -> AsyncIterator[tuple[AsyncSession, CustomImportBuildAttempt | None]]:
    """Own a bounded write page, including fresh authority immediately before commit."""

    contexts = []
    primary = None
    try:
        session = await _enter_context(contexts, session_factory())
        await _enter_context(contexts, session.begin())
        build = await _lock_page(session, request, build_id)
        yield session, build
        await _flush_page(session)
        await _prepare_statement(session)
        execution = await lock_execution(session, request)
        await _prepare_statement(session)
        lease = await lock_lease(session, request.execution_id)
        await _prepare_statement(session)
        now = await verify_live_attempt(
            session,
            request,
            LeaseGrant(request.execution_id, request.fence, request.build_deadline_at, "running"),
            execution,
            lease,
        )
        if now >= request.build_deadline_at:
            raise LeaseAuthorityLost("build deadline elapsed before page commit")
        if request.authorization_expires_at is not None and now >= request.authorization_expires_at:
            raise LeaseAuthorityLost("writer authorization expired before page commit")
        await _prepare_statement(session)
    except BaseException as exc:
        primary = exc
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary


async def _call(session: AsyncSession, name: str, arguments: tuple[tuple[str, object], ...]):
    """Call an internal, explicitly typed SQL build entry point in the mapped schema."""

    await _prepare_statement(session)
    connection = await session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise CandidateRunnerError("build functions require an explicit model schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    parameter_by_name = {f"p{index}": value for index, (_, value) in enumerate(arguments)}
    casts = ",".join(f"CAST(:p{index} AS {sql_type})" for index, (sql_type, _) in enumerate(arguments))
    return await session.execute(text(f"SELECT * FROM {quoted}.{name}({casts})"), parameter_by_name)


async def _is_source_writer_owner(session: AsyncSession) -> bool:
    """Use ordinary SOURCE SQL only as the current owners of both entry points."""

    await _prepare_statement(session)
    connection = await session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.sync_connection.get_execution_options().get("schema_translate_map") or {}
    schema = schema_map.get(model_schema, model_schema)
    if not schema:
        raise CandidateRunnerError("build functions require an explicit model schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(schema)
    result = await session.execute(
        text(
            "SELECT pg_catalog.count(*)=2 AND pg_catalog.bool_and(pg_catalog.pg_get_userbyid(p.proowner)=CURRENT_USER) "
            "FROM pg_catalog.pg_proc p WHERE p.oid IN "
            "(pg_catalog.to_regprocedure(:authorize),pg_catalog.to_regprocedure(:finalize))"
        ),
        {
            "authorize": (
                f"{quoted}.source_bulk_authorize("
                "pg_catalog.int8,pg_catalog.int2,pg_catalog.int8,pg_catalog.bytea,pg_catalog.int4,pg_catalog.int8)"
            ),
            "finalize": f"{quoted}.source_set_finalize(pg_catalog.uuid,pg_catalog.int4[])",
        },
    )
    return result.scalar_one() is True


async def _begin_build(session_factory, request):
    """Begin or reload a build and bind its complete snapshot before commit."""

    async with _page_session(session_factory, request) as (session, _):
        await _prepare_statement(session)
        registry = await load_registry(session, request)
        expected = selection_profile_models(
            request.definition,
            identity=DefinitionIdentity(request.dataset_id, request.definition_revision_id, request.schema_revision_id),
            child_collection_slots=registry.child_collection_slots,
        )
        await _prepare_statement(session)
        actual = (
            await session.scalars(
                select(CustomImportSelectionProfile)
                .where(
                    CustomImportSelectionProfile.dataset_id == request.dataset_id,
                    CustomImportSelectionProfile.definition_revision_id == request.definition_revision_id,
                    CustomImportSelectionProfile.schema_revision_id == request.schema_revision_id,
                )
                .order_by(CustomImportSelectionProfile.profile_slot)
            )
        ).all()
        if not actual:
            session.add_all(expected)
            await _flush_page(session)
        elif len(actual) != len(expected) or any(
            has_profile_mismatch(stored, planned) for stored, planned in zip(actual, expected, strict=True)
        ):
            raise CandidateRunnerError("persisted selection profiles do not match the definition")
        build_id = (
            await _call(
                session,
                "begin_custom_import_build",
                (
                    ("bigint", request.execution_id),
                    ("bigint", request.fence),
                    ("bytea", lease_token_sha256(request.lease_token)),
                    ("bigint", request.expected_base_generation_id),
                    ("bigint", request.expected_pointer_version),
                    ("boolean", request.complete_scope),
                    ("integer", request.page_row_limit),
                    ("bigint", request.page_byte_limit),
                    ("integer", request.statement_timeout_ms),
                    ("timestamptz", request.build_deadline_at),
                ),
            )
        ).scalar_one()
        await _resolve_build_snapshot(session, build_id)
    return build_id, registry


async def _resolve_build_snapshot(session, build_id):
    """Resolve this transaction's real writable or frozen-finality binding."""

    family_id = (await _call(session, "resolve_custom_import_build_snapshot", (("bigint", build_id),))).scalar_one()
    if type(family_id) is not int or not 0 < family_id < 2**63:
        raise CandidateRunnerError("build snapshot binding is malformed")
    return family_id


async def _prepare_snapshot_indexes(session_factory, request, build_id, phase):
    """Prepare one isolated index per fenced transaction, renewing between them."""

    while True:
        async with _page_session(session_factory, request, build_id) as (session, build):
            if phase != "serving" and build.phase != phase:
                return
            family_id = await _resolve_build_snapshot(session, build_id)
            complete = (
                await _call(session, "prepare_custom_import_snapshot_indexes", (("bigint", family_id), ("text", phase)))
            ).scalar_one()
            if type(complete) is not bool:
                raise CandidateRunnerError("index preparation returned an invalid completion state")
        if complete:
            return


@dataclass(frozen=True)
class _PreparedRow:
    raw_key: tuple[str, bytes] | None
    typed_key: tuple[str, bytes] | None
    payload: str | None
    payload_hash: bytes | None
    child_key: str | None
    child_hash: bytes | None
    rejection: CustomImportRejection | None
    byte_count: int


def _prepare_row(request: SourceBuildRequest, stream: SourceStream, record_values: Mapping) -> _PreparedRow:
    definition = request.definition
    fields = tuple(field for field in definition.fields if field.collection == stream.child_collection)
    fields_by_id = {field.field_id: field for field in fields}
    root_key = None
    code = None
    if not isinstance(record_values, Mapping):
        code = "root_not_object" if stream.record_kind == "root" else "child_not_object"
    elif stream.record_kind == "root":
        root_key = _key(record_values, definition.root_logical_key)
        code = (
            "root_key_missing"
            if root_key is None
            else _record_error(record_values, fields_by_id, entity_field=definition.entity_field)
        )
    else:
        collection = definition.collections_by_name[stream.child_collection]
        root_key = _parent_key(record_values, collection)
        code = "orphan_child" if root_key is None else _record_error(record_values, fields_by_id)
        key = _key(record_values, collection.child_key)
        if code is None and key is None:
            code = "child_key_missing"
        if code is None and _canonical_child_key(collection, key, fields_by_id) is None:
            code = "field_type_invalid"
    raw = raw_family_key_evidence(root_key, maximum_canonical_bytes=request.page_byte_limit)
    typed = root_key_evidence_from_tuple(definition, root_key)
    if code is None and typed is None:
        code = "field_type_invalid"
    canonical_payload = payload_hash = child_key = child_hash = rejection = None
    if code is None:
        canonical_payload = record_payload(fields, record_values)
        payload_hash = digest_text(f"{stream.record_kind}-payload", canonical_payload)
        if stream.record_kind == "child":
            child_key = child_key_document(definition, stream.child_collection, record_values)
            child_hash = digest_text("child-key", child_key)
    else:
        rejection = rejection_model(
            request,
            LeaseGrant(request.execution_id, request.fence, request.build_deadline_at, "running"),
            0,
            FamilyRejection(root_key, code),
        )
    texts = [None if raw is None else raw[0], None if typed is None else typed[0], canonical_payload, child_key]
    if code is None and stream.record_kind == "child":
        texts.append(typed[0])  # canonical parent key is retained again on the child.
    if rejection is not None:
        texts.extend((rejection.canonical_root_key, rejection.canonical_evidence))
    byte_count = sum(len(document.encode("utf-8")) for document in texts if document is not None)
    if byte_count > request.page_byte_limit:
        raise CandidateRunnerError("one source occurrence exceeds the admitted page byte limit")
    return _PreparedRow(raw, typed, canonical_payload, payload_hash, child_key, child_hash, rejection, byte_count)


@dataclass(frozen=True)
class _StreamContext:
    request: SourceBuildRequest
    registry: CandidateRegistry
    build_id: int
    stream: SourceStream

    @property
    def stream_slot(self):
        """Return the immutable definition-owned stream slot."""
        return self.registry.stream_slots[self.stream.stream_id]

    @property
    def collection_slot(self):
        """Return zero for roots, otherwise the declared child collection slot."""
        return self.registry.child_collection_slots.get(self.stream.child_collection, 0)


@dataclass(frozen=True)
class _SourcePage:
    part_ordinal: int
    first_row: int
    first_source: int
    records: tuple[_PreparedRow, ...]


_SOURCE_COPY_COLUMNS = (
    "batch_id",
    "landing_ordinal",
    "pack_ordinal",
    "pack_sha256",
    "part_ordinal",
    "part_row_ordinal",
    "source_ordinal",
    "raw_key",
    "raw_hash",
    "typed_key",
    "typed_hash",
    "payload",
    "payload_hash",
    "child_key",
    "child_hash",
    "rejection_code",
    "rejection_key",
    "rejection_hash",
    "rejection_evidence",
)


async def _copy_source_landing(session, landing_rows):
    """COPY only to the native snapshot pinned by this transaction's batch."""

    if not landing_rows:
        raise CandidateRunnerError("source COPY requires an authorized nonempty batch")
    family_id = (
        await _call(session, "resolve_custom_import_source_batch_snapshot", (("uuid", landing_rows[0][0]),))
    ).scalar_one()
    try:
        schema = snapshot_schema(family_id)
    except ValueError as exc:
        raise CandidateRunnerError("source batch snapshot binding is malformed") from exc
    await _prepare_statement(session)
    connection = await session.connection()
    raw = await connection.get_raw_connection()
    await raw.driver_connection.copy_records_to_table(
        "source_bulk_landing",
        schema_name=schema,
        columns=_SOURCE_COPY_COLUMNS,
        records=landing_rows,
    )


def _source_closed_prefix(context, cursor, pages, verified_parts):
    """Bind the first durable position and exact earlier reader-close metadata."""
    first = pages[0]
    if (
        first.first_source != cursor.next_source_ordinal
        or first.part_ordinal < cursor.next_part_ordinal
        or first.first_row != (cursor.next_part_row_ordinal if first.part_ordinal == cursor.next_part_ordinal else 0)
    ):
        raise CandidateRunnerError("source cursor changed; reload before retry")
    earlier_parts = sorted(
        part
        for slot, part in verified_parts
        if slot == context.stream_slot and cursor.next_part_ordinal <= part < pages[-1].part_ordinal
    )
    if len(earlier_parts) != pages[-1].part_ordinal - cursor.next_part_ordinal or any(
        part != cursor.next_part_ordinal + offset for offset, part in enumerate(earlier_parts)
    ):
        raise CandidateRunnerError("earlier source parts lack verified reader close")
    return earlier_parts


async def _store_pages(session_factory, context, pages, *, verified_parts=()):
    """COPY native page packs and promote one bounded set in the owned transaction."""
    async with _page_session(session_factory, context.request, context.build_id) as (session, build):
        if build.phase != "source":
            raise CandidateRunnerError("source build is already frozen")
        await _prepare_statement(session)
        cursor = (
            await session.scalars(
                select(CustomImportBuildStream)
                .where(
                    CustomImportBuildStream.build_id == context.build_id,
                    CustomImportBuildStream.stream_slot == context.stream_slot,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        ).one()
        preview = encode_landing_batch(
            context, pages, batch_id=UUID(int=0), first_pack_ordinal=cursor.next_pack_ordinal
        )
        earlier_parts = _source_closed_prefix(context, cursor, pages, verified_parts)
        batch_id = (
            await _call(
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
        landing_rows = tuple((batch_id, ordinal, *landing[1:]) for ordinal, landing in enumerate(preview.records))
        await _copy_source_landing(session, landing_rows)
        if await _is_source_writer_owner(session):
            from process.custom_import.source_finalize_sql import finalize_source_batch

            completed = await finalize_source_batch(session, batch_id, earlier_parts)
        else:
            completed = (
                await _call(session, "source_set_finalize", (("uuid", batch_id), ("integer[]", earlier_parts)))
            ).scalar_one()
        if completed != len(landing_rows):
            raise CandidateRunnerError("completed source count differs from attempted rows")
    return completed


async def _store_single_page(session_factory, context, page):
    """Keep the single-page internal entry point on the same COPY/set boundary."""
    return await _store_pages(session_factory, context, (page,))


class _SourceBatch:
    """Replay-local buffering; durable cursors remain the only retry authority."""

    def __init__(self, session_factory):
        self.session_factory = session_factory
        self.pages, self.context, self.rows, self.bytes = [], None, 0, 0
        self.verified_parts = set()
        self.last_flush_at = time.monotonic()

    def clear(self):
        """Discard replay-local state, including an uncertain prior attempt."""
        self.pages.clear()
        self.context, self.rows, self.bytes = None, 0, 0
        self.verified_parts.clear()

    async def flush(self):
        """Promote buffered packs, then finish only a verified non-final reader."""
        if not self.pages:
            return
        context, last_part = self.context, self.pages[-1].part_ordinal
        await _store_pages(self.session_factory, context, tuple(self.pages), verified_parts=self.verified_parts)
        self.pages.clear()
        self.context, self.rows, self.bytes = None, 0, 0
        if (context.stream_slot, last_part) in self.verified_parts:
            await _finish_part(self.session_factory, context.request, context.build_id, context.stream_slot, last_part)
        self.verified_parts = {
            (slot, part) for slot, part in self.verified_parts if slot != context.stream_slot or part > last_part
        }
        self.last_flush_at = time.monotonic()

    async def store(self, session_factory, context, page):
        """Buffer intact packs within row, byte, stream and live-lease bounds."""
        page_bytes = sum(row.byte_count for row in page.records)
        if self.pages and (
            context != self.context
            or self.rows + len(page.records) > MAX_BATCH_ROWS
            or self.bytes + page_bytes > MAX_BATCH_BYTES
        ):
            await self.flush()
        self.context = context
        self.pages.append(page)
        self.rows += len(page.records)
        self.bytes += page_bytes
        if time.monotonic() - self.last_flush_at >= context.request.lease_seconds / 3:
            await self.flush()

    async def finish(self, request, build_id, slot, ordinal, *, final=False):
        """Record actual reader close; final calls require outer EOF and cleanup."""
        # Non-final readers have already completed decode, Arrow accounting and
        # close. Final finish is invoked only after genuine outer EOF and cleanup.
        if final:
            await self.flush()
            await _finish_part(self.session_factory, request, build_id, slot, ordinal)
            return
        self.verified_parts.add((slot, ordinal))
        if not self.pages:
            await _finish_part(self.session_factory, request, build_id, slot, ordinal)
            self.verified_parts.discard((slot, ordinal))
        elif self.context.stream_slot != slot or self.pages[-1].part_ordinal < ordinal:
            await self.flush()
            await _finish_part(self.session_factory, request, build_id, slot, ordinal)
            self.verified_parts.discard((slot, ordinal))
        elif time.monotonic() - self.last_flush_at >= request.lease_seconds / 3:
            await self.flush()
        if not self.pages:
            self.last_flush_at = time.monotonic()


def _committed_prefix_statement(context, page, *, models=None):
    """Compile exact replay joins from explicit transaction-bound hot aliases."""

    occurrence = CustomImportBuildOccurrence if models is None else models[CustomImportBuildOccurrence]
    root_record = CustomImportRootRecord if models is None else models[CustomImportRootRecord]
    root_revision = CustomImportRootRevision if models is None else models[CustomImportRootRevision]
    child_revision = CustomImportChildRevision if models is None else models[CustomImportChildRevision]
    rejection = CustomImportRejection if models is None else models[CustomImportRejection]
    return (
        select(
            occurrence,
            root_record.canonical_logical_key,
            root_revision.canonical_payload,
            child_revision.canonical_payload,
            child_revision.canonical_child_key,
            rejection.code,
            rejection.canonical_root_key,
            rejection.canonical_evidence,
        )
        .outerjoin(root_record, root_record.root_record_id == occurrence.root_record_id)
        .outerjoin(root_revision, root_revision.root_revision_id == occurrence.root_revision_id)
        .outerjoin(child_revision, child_revision.child_revision_id == occurrence.child_revision_id)
        .outerjoin(rejection, rejection.rejection_id == occurrence.rejection_id)
        .where(
            occurrence.build_id == context.build_id,
            occurrence.stream_slot == context.stream_slot,
            occurrence.origin == "source",
            occurrence.source_part_ordinal == page.part_ordinal,
            occurrence.part_row_ordinal >= page.first_row,
            occurrence.part_row_ordinal < page.first_row + len(page.records),
        )
        .order_by(occurrence.part_row_ordinal)
        .limit(context.request.page_row_limit)
    )


def _stored_fingerprint(stored_record):
    occurrence, typed, root_payload, child_payload, child_key, code, rejection_key, evidence = stored_record
    return (
        occurrence.part_row_ordinal,
        occurrence.source_ordinal,
        occurrence.raw_parent_key_canonical,
        None if occurrence.raw_parent_key_sha256 is None else bytes(occurrence.raw_parent_key_sha256),
        typed,
        root_payload or child_payload,
        child_key,
        code,
        rejection_key,
        evidence,
    )


def _prepared_fingerprint(page, offset):
    prepared = page.records[offset]
    return (
        page.first_row + offset,
        page.first_source + offset,
        None if prepared.raw_key is None else prepared.raw_key[0],
        None if prepared.raw_key is None else prepared.raw_key[1],
        None if prepared.typed_key is None else prepared.typed_key[0],
        prepared.payload,
        prepared.child_key,
        None if prepared.rejection is None else prepared.rejection.code,
        None if prepared.rejection is None else prepared.rejection.canonical_root_key,
        None if prepared.rejection is None else prepared.rejection.canonical_evidence,
    )


async def _compare_committed_page(session_factory, context, page):
    """Pin the exact snapshot anew before comparing a durable source prefix."""

    async with _page_session(session_factory, context.request, context.build_id) as (session, _):
        family_id = await _resolve_build_snapshot(session, context.build_id)
        await _prepare_statement(session)
        stored_records = (
            await session.execute(_committed_prefix_statement(context, page, models=snapshot_models(family_id)))
        ).all()
        if len(stored_records) != len(page.records):
            raise CandidateRunnerError("committed source prefix has incomplete coverage")
        if any(
            _stored_fingerprint(stored) != _prepared_fingerprint(page, offset)
            for offset, stored in enumerate(stored_records)
        ):
            raise CandidateRunnerError("committed source prefix differs from sealed replay")
        checked = (
            await _call(
                session,
                "check_custom_import_source_replay_homes",
                (
                    ("bigint", context.build_id),
                    ("smallint", context.stream_slot),
                    ("integer", page.part_ordinal),
                    ("bigint", page.first_row),
                    ("integer", len(page.records)),
                ),
            )
        ).scalar_one()
        if checked != len(page.records):
            raise CandidateRunnerError("committed source revision homes have incomplete coverage")


async def _finish_part(session_factory, request, build_id, slot, ordinal):
    async with _page_session(session_factory, request, build_id) as (session, _):
        await _call(
            session,
            "finish_custom_import_build_source_part",
            (("bigint", build_id), ("smallint", slot), ("integer", ordinal)),
        )


def _iter_source_pages(context, part, policy, cursor):
    """Decode at most one page and one lookahead record, splitting at the durable prefix."""

    request, stream = context.request, context.stream
    fields = _stream_fields(request.definition, stream)
    committed_rows = part.record_count if part.ordinal < cursor[0] else cursor[1] if part.ordinal == cursor[0] else 0
    prepared_records = []
    byte_count = first_row = count = 0
    decoded = iter_records(part.capture, stream, limits=policy.part_limits)
    primary = None
    try:
        for decoded_record in decoded:
            if tuple(decoded_record.values) != tuple(field.field_id for field in fields):
                raise CandidateRunnerError("replayed fields differ from the retained stream")
            prepared = _prepare_row(
                request, stream, _normalized_integer_replay_values(dict(decoded_record.values), fields)
            )
            if prepared_records and (
                len(prepared_records) == request.page_row_limit
                or byte_count + prepared.byte_count > request.page_byte_limit
                or count == committed_rows
            ):
                yield _SourcePage(part.ordinal, first_row, cursor[2] + first_row, tuple(prepared_records))
                prepared_records, byte_count, first_row = [], 0, count
            prepared_records.append(prepared)
            byte_count += prepared.byte_count
            count += 1
        if prepared_records:
            yield _SourcePage(part.ordinal, first_row, cursor[2] + first_row, tuple(prepared_records))
        if count != part.record_count:
            raise CandidateRunnerError("decoded part record count differs from the sealed receipt")
    except BaseException as exc:
        primary = exc
    primary = _close_iterator(decoded, primary)
    if primary is not None:
        raise primary


def _close_iterator(iterator, primary):
    try:
        iterator.close()
    except BaseException as exc:
        if primary is None:
            primary = exc
        else:
            primary.add_note(f"source decoder cleanup also failed: {type(exc).__name__}")
            primary.__cause__ = exc
    return primary


async def _replay_part(session_factory, context, part, policy, cursor, *, store_page=None):
    """Compare committed prefixes before appending, then verify genuine part EOF."""

    fields = _stream_fields(context.request.definition, context.stream)
    _validate_replay_partition_schema(part.capture, fields=fields, limits=policy.part_limits)
    committed_rows = part.record_count if part.ordinal < cursor[0] else cursor[1] if part.ordinal == cursor[0] else 0
    pages = _iter_source_pages(context, part, policy, cursor)
    primary = None
    try:
        for page in pages:
            operation = (
                _compare_committed_page if page.first_row < committed_rows else (store_page or _store_single_page)
            )
            await operation(session_factory, context, page)
    except BaseException as exc:
        primary = exc
    primary = _close_iterator(pages, primary)
    if primary is not None:
        raise primary
    arrow_limits = replace(
        policy.part_limits,
        maximum_decoded_bytes=max(
            policy.part_limits.maximum_decoded_bytes,
            policy.maximum_part_arrow_bytes,
        ),
    )
    arrow_bytes = _aggregate_parquet_arrow_bytes(part.capture, limits=arrow_limits, decoded_bytes=0)
    # Landing allocation bytes are immutable accounting, not a Parquet decode-size proof.
    if arrow_bytes > policy.maximum_part_arrow_bytes:
        raise CandidateRunnerError("decoded part Arrow bytes exceed the admitted replay limit")
    return part.record_count


async def _drain_cleanup(cleanup, primary: BaseException | None) -> BaseException | None:
    task = asyncio.create_task(cleanup)
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if primary is None:
                primary = exc
            else:
                primary._custom_import_retry_blocked = True
        except BaseException:
            break
    try:
        task.result()
    except BaseException as exc:
        if primary is None:
            primary = exc
        elif primary is not exc:
            primary._custom_import_retry_blocked = True
            primary.add_note(f"source replay cleanup also failed: {type(exc).__name__}")
            primary.__cause__ = exc
    return primary


async def _enter_context(contexts, context):
    entered = await context.__aenter__()
    contexts.append(context)
    return entered


async def _close_contexts(contexts, primary):
    """Drain the bounded reader/transaction/session stack without replacing its first error."""

    for context in reversed(contexts):
        error_type = None if primary is None else type(primary)
        traceback = None if primary is None else primary.__traceback__
        primary = await _drain_cleanup(context.__aexit__(error_type, primary, traceback), primary)
    return primary


async def _replay_source(session_factory, request, registry, build_id, bundle_id, policy, cursors):
    batch = _SourceBatch(session_factory)
    contexts = []
    primary = None
    final_part_by_slot = {}
    source_starts = dict.fromkeys(registry.stream_slots, 0)
    stream_by_id = {stream.stream_id: stream for stream in request.definition.source_streams}
    try:
        session = await _enter_context(contexts, session_factory())
        await _enter_context(contexts, session.begin())
        await _set_timeout(session, request.statement_timeout_ms)
        parts = await _enter_context(
            contexts,
            open_segmented_parquet_parts(
                session,
                capture_bundle_id=bundle_id,
                dataset_id=request.dataset_id,
                definition_revision_id=request.definition_revision_id,
                schema_revision_id=request.schema_revision_id,
            ),
        )
        async for part in parts:
            stream_id = part.receipt.stream_id
            slot = registry.stream_slots[stream_id]
            cursor = cursors[slot]
            count = await _replay_part(
                session_factory,
                _StreamContext(request, registry, build_id, stream_by_id[stream_id]),
                part,
                policy,
                (cursor[0], cursor[1], source_starts[stream_id]),
                store_page=batch.store,
            )
            source_starts[stream_id] += count
            part_count = json.loads(part.receipt.canonical_manifest)["part_count"]
            if part.ordinal == part_count:
                final_part_by_slot[slot] = part.ordinal
            elif part.ordinal >= cursor[0]:
                await batch.finish(request, build_id, slot, part.ordinal)
    except BaseException as exc:
        primary = exc
    primary = await _close_contexts(contexts, primary)
    try:
        if primary is not None:
            raise primary
        if set(final_part_by_slot) != set(registry.stream_slots.values()):
            raise CandidateRunnerError("source replay lacks complete stream EOF")
        # Only genuine outer EOF plus successful cursor/session close grants final EOF.
        for slot, ordinal in sorted(final_part_by_slot.items()):
            await batch.finish(request, build_id, slot, ordinal, final=True)
        async with _page_session(session_factory, request, build_id) as (session, _):
            await _call(session, "freeze_custom_import_build_source", (("bigint", build_id),))
    finally:
        batch.clear()


async def _source_handoff_slots(session_factory, request, build_id, registry):
    """Skip verified streams on resume; allow one interrupted global freeze."""

    async with _page_session(session_factory, request, build_id) as (session, build):
        if build.phase != "source":
            return ()
        await _prepare_statement(session)
        streams = (
            await session.scalars(
                select(CustomImportBuildStream)
                .where(CustomImportBuildStream.build_id == build_id)
                .order_by(CustomImportBuildStream.stream_slot)
                .with_for_update()
            )
        ).all()
        if {stream.stream_slot for stream in streams} != set(registry.stream_slots.values()) or not streams:
            raise CandidateRunnerError("source build streams differ from the retained definition")
        pending_slots = tuple(stream.stream_slot for stream in streams if stream.replay_verified_at is None)
        return pending_slots or (streams[0].stream_slot,)


async def _stage_source_handoff(session_factory, request, build_id, registry, transport):
    """Continue from acknowledged or independently verified durable SOURCE progress."""

    from process.custom_import import source_worker as worker

    slots = await _source_handoff_slots(session_factory, request, build_id, registry)
    for slot in slots:
        while True:
            try:
                progress = await worker.source_next_batch(session_factory, request, build_id, slot, transport)
            except worker.SourceReconciliationRequired as error:
                if error.progress is None:
                    raise
                progress = error.progress
            if progress.phase != "source":
                return
            if progress.stream_complete:
                break
    # A successful final stream receipt normally freezes SOURCE. Require that
    # durable transition before indexes/admission; never fall back to local writes.
    async with _page_session(session_factory, request, build_id) as (_, build):
        if build.phase == "source":
            raise CandidateRunnerError("source handoff ended before committed global freeze")


async def _stage_admission_handoff(session_factory, request, build_id, transport):
    """Continue after a committed receipt, advanced cursor or observed terminal phase."""

    from process.custom_import import admission_worker as worker

    while True:
        try:
            progress = await worker.admit_next_batch(session_factory, request, build_id, transport)
        except worker.AdmissionReconciliationRequired as error:
            progress = error.progress
            if progress is None or (
                progress.phase not in worker._POST_ADMISSION_PHASES
                and progress.after_occurrence_id <= error.pins.expected_after_occurrence_id
            ):
                raise
        if progress.phase in worker._POST_ADMISSION_PHASES:
            retained = await worker._retained_progress(session_factory, request, build_id)
            return SourceBuildResult(
                build_id,
                request.execution_id,
                retained.phase,
                retained.source_occurrence_count,
                retained.candidate_error_count,
            )


async def stage_segmented_source(
    session_factory: SessionFactory,
    request: SourceBuildRequest,
    *,
    writer_transports=None,
) -> SourceBuildResult:
    """Replay immutable bytes, then globally admit families; never publish or finish execution.

    On an uncertain commit, invoke again with exactly the same request. Durable
    cursors and replayed committed prefixes decide what remains, not the error's
    transport status. A new fence must start its own build.
    """

    if not isinstance(request, SourceBuildRequest):
        raise TypeError("source staging requires SourceBuildRequest")
    if writer_transports is not None:
        request = writer_transports.source.bind_request(writer_transports.admission.bind_request(request))
    build_id, registry = await _begin_build(session_factory, request)
    if writer_transports is not None:
        await _stage_source_handoff(session_factory, request, build_id, registry, writer_transports.source)
    else:
        direct_admission = await _stage_local_source(session_factory, request, build_id, registry)
    await _prepare_snapshot_indexes(session_factory, request, build_id, "admission")
    if writer_transports is not None:
        return await _stage_admission_handoff(session_factory, request, build_id, writer_transports.admission)
    return await _admit_local_pages(session_factory, request, build_id, direct_admission)


async def _stage_local_source(session_factory, request, build_id, registry):
    """Preserve standalone replay and select only the current connection's writer."""

    from process.custom_import import admission_sql as admission

    async with _page_session(session_factory, request, build_id) as (session, build):
        phase, bundle_id = build.phase, build.capture_bundle_id
        await _prepare_statement(session)
        bundle = await session.get(CustomImportCaptureBundle, bundle_id)
        policy = SegmentedCapturePolicy.from_mapping(json.loads(bundle.canonical_policy))
        if policy.canonical != bundle.canonical_policy or bytes.fromhex(policy.digest) != bytes(bundle.policy_sha256):
            raise CandidateRunnerError("sealed capture policy differs from its retained digest")
        await _prepare_statement(session)
        cursor_by_slot = {
            cursor.stream_slot: (cursor.next_part_ordinal, cursor.next_part_row_ordinal)
            for cursor in (
                await session.scalars(
                    select(CustomImportBuildStream).where(CustomImportBuildStream.build_id == build_id)
                )
            ).all()
        }
        direct_admission = await admission._has_admission_owner(session)
    if phase == "source":
        await _replay_source(session_factory, request, registry, build_id, bundle_id, policy, cursor_by_slot)
    return direct_admission


async def _admit_local_pages(session_factory, request, build_id, direct_admission):
    """Use the current owner's writer or retained dispatcher in fenced page transactions."""

    from process.custom_import import admission_sql as admission

    while True:
        try:
            async with _page_session(session_factory, request, build_id) as (session, build):
                if build.phase in ("graph", "rejected", "output", "verifying", "verified"):
                    return SourceBuildResult(
                        build_id,
                        request.execution_id,
                        build.phase,
                        build.source_occurrence_count,
                        build.candidate_error_count,
                    )
                if build.phase != "admission":
                    raise CandidateRunnerError("source build is not ready for global admission")
                if direct_admission:
                    await admission._admit_locked(session, build, build.admission_after_occurrence_id)
                else:
                    # Restricted principals retain their existing fenced dispatcher until authority is migrated.
                    await _call(
                        session,
                        "admit_custom_import_build_page",
                        (("bigint", build_id), ("bigint", build.admission_after_occurrence_id)),
                    )
        except DBAPIError as error:
            await admission._retry_admission_timeout(session_factory, request, build_id, error)
