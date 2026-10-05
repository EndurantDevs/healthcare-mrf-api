# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded graph construction from a frozen, admitted source build."""

from __future__ import annotations

import asyncio
import hmac
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from itertools import count

from sqlalchemy import LargeBinary, String, func, or_, select, text, tuple_
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportChildRevision,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportFamilyChild,
    CustomImportFamilyRevision,
    CustomImportGeneration,
    CustomImportLease,
    CustomImportPack,
    CustomImportRootRecord,
    CustomImportRootRevision,
    CustomImportSourceStream,
)
from process.custom_import import materialization as material
from process.custom_import import publication
from process.custom_import.build_graph_source_page import append_source_child_page
from process.custom_import.build_source import (
    SourceBuildRequest,
    _assert_build_identity,
    _close_contexts,
    _enter_context,
    _flush_page,
    _page_session,
    _prepare_snapshot_indexes,
    _prepare_statement,
)
from process.custom_import.build_source import (
    _call as _source_call,
)
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import (
    candidate_hash_ordered,
    digest_text,
    fields_by_collection,
    new_family_hash_ordered,
    pack_hash,
    payload_values,
    record_payload,
)
from process.custom_import.runner_graph import entity_value_digest, stored_root_values, verify_stored_child
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import (
    CancellationRequested,
    CandidateRunnerError,
    LeaseAuthorityLost,
    StoredCandidateChild,
)
from process.custom_import.storage_layout import snapshot_models


def _require_budget(deadline):
    if time.monotonic() >= deadline:
        raise LeaseAuthorityLost("build page deadline elapsed")


def _verify_request(build, request, execution):
    if build is None:
        raise CandidateRunnerError("build does not exist")
    _assert_build_identity(build, request, execution)


def _prepare_read(session, request, deadline):
    remaining_ms = int((deadline - time.monotonic()) * 1000)
    if remaining_ms < 4:
        raise LeaseAuthorityLost("build read deadline elapsed")
    session.execute(
        select(func.set_config("statement_timeout", str(min(request.statement_timeout_ms, remaining_ms // 2)), True))
    )


@contextmanager
def _read_transaction(session, request, build_id):
    """Read one frozen page without acquiring the dataset write lock."""

    with session.begin():
        session.execute(select(func.set_config("statement_timeout", str(request.statement_timeout_ms), True)))
        started = time.monotonic()
        build, execution, lease, now, isolation = session.execute(
            select(
                CustomImportBuildAttempt,
                CustomImportExecution,
                CustomImportLease,
                func.clock_timestamp(),
                func.current_setting("transaction_isolation"),
            )
            .join(CustomImportExecution, CustomImportExecution.execution_id == CustomImportBuildAttempt.execution_id)
            .join(CustomImportLease, CustomImportLease.execution_id == CustomImportExecution.execution_id)
            .where(CustomImportBuildAttempt.build_id == build_id)
            .execution_options(populate_existing=True)
        ).one()
        _verify_request(build, request, execution)
        if isolation != "read committed":
            raise CandidateRunnerError("build reads require READ COMMITTED")
        if execution.state == "canceling":
            raise CancellationRequested("candidate execution is canceling")
        if (
            execution.state != "running"
            or execution.capture_bundle_id != build.capture_bundle_id
            or lease.fence != request.fence
            or lease.token_sha256 is None
            or not hmac.compare_digest(bytes(lease.token_sha256), bytes(build.producing_token_sha256))
            or lease.expires_at is None
        ):
            raise LeaseAuthorityLost("build read requires the current running attempt")
        remaining = (min(lease.expires_at, build.build_deadline_at) - now).total_seconds() - 0.001
        if remaining <= 0:
            raise LeaseAuthorityLost("build read deadline elapsed")
        deadline = started + remaining
        session.info["custom_import_build_read_deadline"] = deadline
        _prepare_read(session, request, deadline)
        yield build, deadline
        _require_budget(deadline)
        session.expunge_all()


def _variable_bytes(*models):
    """Charge every selected text/binary column, including repeated joins."""

    return sum(
        (
            func.coalesce(func.octet_length(getattr(model, column.name)), 0)
            for model in models
            for column in model.__table__.columns
            if isinstance(column.type, (String, LargeBinary))
        ),
        0,
    )


def _admitted_keys(metadata, byte_limit):
    keys, used = [], 0
    for row in metadata:
        size = row[-1]
        if size > byte_limit and not keys:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        if used + size > byte_limit:
            break
        keys.append(tuple(row[:-1]))
        used += size
    return keys


@dataclass(frozen=True)
class _ReadPage:
    reserve_bytes: int = 0
    row_limit: int | None = None
    variable_columns: tuple | None = None
    single_page: bool = False
    physical: bool = False


def _read_rows(session, request, build_id, statement, keys, models, *, after=None, bounds=_ReadPage()):
    yield from _read_query_pages(
        session, request, build_id, lambda _session: (statement, keys, models), after=after, bounds=bounds
    )


def _build_storage_models(session, build_id):
    """Resolve both immutable bindings in the same transaction as their reads."""

    connection = session.connection()
    model_schema = CustomImportBuildAttempt.__table__.schema
    schema_map = connection.get_execution_options().get("schema_translate_map") or {}
    control_schema = schema_map.get(model_schema, model_schema)
    if not control_schema:
        raise CandidateRunnerError("build snapshot resolvers require an explicit control schema")
    quoted = connection.dialect.identifier_preparer.quote_schema(control_schema)
    candidate_id, base_id = session.execute(
        text(
            f"SELECT {quoted}.resolve_custom_import_build_snapshot(CAST(:build_id AS bigint)), "
            f"{quoted}.resolve_custom_import_build_base_snapshot(CAST(:build_id AS bigint))"
        ),
        {"build_id": build_id},
    ).one()
    if type(candidate_id) is not int or not 0 < candidate_id < 2**63:
        raise CandidateRunnerError("build candidate snapshot identity is invalid")
    if base_id is not None and (type(base_id) is not int or not 0 < base_id < 2**63):
        raise CandidateRunnerError("build base snapshot identity is invalid")
    return snapshot_models(candidate_id), None if base_id is None else snapshot_models(base_id)


def _read_snapshot_rows(session, request, build_id, query, *, after=None, bounds=_ReadPage()):
    """Bind hot tables afresh before each metadata/payload page, never by raw IDs."""

    def bound_query(current_session):
        """Verify registered relation bindings before constructing this page."""
        candidate_models, base_models = _build_storage_models(current_session, build_id)
        return query(candidate_models, base_models)

    yield from _read_query_pages(session, request, build_id, bound_query, after=after, bounds=bounds)


def _read_query_pages(session, request, build_id, query_factory, *, after=None, bounds=_ReadPage()):
    """Metadata-first keyset pages, closed before the consumer hashes a page_record."""

    if bounds.physical:
        yield from _read_physical_pages(session, request, build_id, query_factory, after=after, bounds=bounds)
        return
    limit = min(bounds.row_limit or request.page_row_limit, request.page_row_limit)
    byte_limit = request.page_byte_limit - bounds.reserve_bytes
    if byte_limit < 0:
        raise CandidateRunnerError("build page has no remaining payload budget")
    while True:
        with _read_transaction(session, request, build_id) as (_build, deadline):
            statement, keys, models = query_factory(session)
            variable_bytes = (
                _variable_bytes(*models)
                if bounds.variable_columns is None
                else sum((func.coalesce(func.octet_length(column), 0) for column in bounds.variable_columns), 0)
            )
            _prepare_read(session, request, deadline)
            page = statement.order_by(None)
            if after is not None:
                page = page.where(tuple_(*keys) > tuple(after))
            metadata = session.execute(
                page.with_only_columns(*keys, variable_bytes, maintain_column_froms=True).order_by(*keys).limit(limit)
            ).all()
            admitted = _admitted_keys(metadata, byte_limit)
            if not admitted:
                return
            _prepare_read(session, request, deadline)
            page_records = session.execute(page.where(tuple_(*keys).in_(admitted)).order_by(*keys)).all()
            if len(page_records) != len(admitted):
                raise CandidateRunnerError("frozen build page changed during its read")
            after = admitted[-1]
        for page_record in page_records:
            _require_budget(session.info["custom_import_build_read_deadline"])
            yield page_record
            # Nested bounded reads may renew the live window while reducing a
            # large family or group. Pure work must still fit the latest window.
            _require_budget(session.info["custom_import_build_read_deadline"])
        if bounds.single_page:
            return


def _physical_key_bytes(keys):
    """Bound metadata before fetching it, including maximum UTF-8 key widths."""
    size = 64 + 16 * (len(keys) + 1)
    for key in keys:
        if isinstance(key.type, (String, LargeBinary)):
            if key.type.length is None:
                raise CandidateRunnerError("physical build reads require bounded key columns")
            size += key.type.length * (4 if isinstance(key.type, String) else 1)
    return size


def _physical_keys(metadata, request, available_bytes, fixed_bytes):
    admitted, used_bytes = [], 0
    for *identity, payload_bytes in metadata:
        if type(payload_bytes) is not int or payload_bytes < 0:
            raise CandidateRunnerError("build record has invalid byte metadata")
        if payload_bytes > request.page_byte_limit:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        size = payload_bytes + fixed_bytes
        if used_bytes + size > available_bytes:
            if not admitted:
                raise CandidateRunnerError("one build record exceeds the admitted byte page")
            break
        admitted.append(tuple(identity))
        used_bytes += size
    if len(set(admitted)) != len(admitted):
        raise CandidateRunnerError("frozen build page contains duplicate keys")
    return admitted


def _read_physical_payload(session, request, deadline, page, keys, admitted):
    """Check the complete prefix count without fetching an unbudgeted extra payload."""
    prefix = page.where(tuple_(*keys) <= admitted[-1])
    prefix_count = (
        prefix.with_only_columns(func.count(), maintain_column_froms=True)
        .order_by(None)
        .correlate(None)
        .scalar_subquery()
    )
    _prepare_read(session, request, deadline)
    page_records = session.execute(prefix.add_columns(*keys, prefix_count).order_by(*keys).limit(len(admitted))).all()
    if len(page_records) != len(admitted) or any(
        page_record[-1] != len(admitted) or tuple(page_record[-len(keys) - 1 : -1]) != identity
        for page_record, identity in zip(page_records, admitted)
    ):
        raise CandidateRunnerError("frozen build page changed during its read")
    return page_records


def _read_physical_pages(session, request, build_id, query_factory, *, after, bounds):
    """Coalesce immutable verification reads; keep logical record and live lease limits."""
    available_bytes = MAX_BATCH_BYTES - bounds.reserve_bytes
    if available_bytes < 0:
        raise CandidateRunnerError("build page has no remaining payload budget")
    carry_bytes = 0
    while True:
        with _read_transaction(session, request, build_id) as (_build, deadline):
            statement, keys, models = query_factory(session)
            variable_bytes = (
                _variable_bytes(*models)
                if bounds.variable_columns is None
                else sum((func.coalesce(func.octet_length(column), 0) for column in bounds.variable_columns), 0)
            )
            metadata_bytes = _physical_key_bytes(keys)
            fixed_bytes = metadata_bytes + 64 + 16 * (len(statement.selected_columns) + 1)
            page_bytes = available_bytes - carry_bytes
            limit = max(
                1,
                min(
                    bounds.row_limit or MAX_BATCH_ROWS,
                    (MAX_BATCH_ROWS - 1) // 2,
                    page_bytes // (2 * metadata_bytes + fixed_bytes),
                ),
            )
            page = statement.order_by(None)
            if after is not None:
                page = page.where(tuple_(*keys) > tuple(after))
            _prepare_read(session, request, deadline)
            metadata = session.execute(
                page.with_only_columns(*keys, variable_bytes, maintain_column_froms=True).order_by(*keys).limit(limit)
            ).all()
            if len(metadata) > limit:
                raise CandidateRunnerError("build metadata exceeds its physical page")
            admitted = _physical_keys(metadata, request, page_bytes - 2 * len(metadata) * metadata_bytes, fixed_bytes)
            if not admitted:
                return
            # A consumer may retain the last child document to validate order.
            # Reserve that actual record, not the potentially much larger policy cap.
            carry_bytes = metadata[len(admitted) - 1][-1] + fixed_bytes
            page_records = _read_physical_payload(session, request, deadline, page, keys, admitted)
            after = admitted[-1]
            del metadata, admitted
        for page_record in page_records:
            _require_budget(session.info["custom_import_build_read_deadline"])
            yield tuple(page_record[: -len(keys) - 1])
            _require_budget(session.info["custom_import_build_read_deadline"])
        del page_records, page_record
        if bounds.single_page:
            return


def _one_row(session, request, build_id, statement, keys, models):
    return _one_query_row(session, request, build_id, lambda _session: (statement, keys, models))


def _one_query_row(session, request, build_id, query_factory):
    """Close a factory-bound keyset stream after its first row, including failures."""

    rows = _read_query_pages(session, request, build_id, query_factory, bounds=_ReadPage(row_limit=1))
    try:
        return next(rows, None)
    finally:
        rows.close()


@asynccontextmanager
async def _session(session_factory, *, transaction=False):
    """Observe each owned context exit even under repeated cancellation."""

    contexts = []
    primary = None
    try:
        session = await _enter_context(contexts, session_factory())
        if transaction:
            await _enter_context(contexts, session.begin())
        yield session
    except BaseException as exc:
        primary = exc
    primary = await _close_contexts(contexts, primary)
    if primary is not None:
        raise primary


async def _snapshot(session_factory, request, build_id):
    async with _session(session_factory) as session:

        def _read(sync_session):
            with _read_transaction(sync_session, request, build_id) as (build, _deadline):
                return build

        return await session.run_sync(_read)


async def _heartbeat(session_factory, request):
    async with _page_session(session_factory, request) as (session, _build):
        await _prepare_statement(session)


@asynccontextmanager
async def _renew_while_reading(session_factory, request):
    """Keep long immutable scans live without extending the pinned deadline."""

    async def _renew():
        while True:
            await asyncio.sleep(min(5.0, request.lease_seconds / 3))
            await _heartbeat(session_factory, request)

    try:
        async with asyncio.TaskGroup() as tasks:
            heartbeat = tasks.create_task(_renew())
            try:
                yield
            finally:
                heartbeat.cancel()
    except BaseExceptionGroup as failure:
        if len(failure.exceptions) == 1:
            raise failure.exceptions[0] from None
        raise


async def _call(session, name, values, columns=()):
    """Use the source boundary's explicitly typed internal SQL caller."""

    result = await _source_call(session, name, tuple(("bigint", value) for value in values))
    return result.one() if columns else result.scalar_one()


def _model_bytes(models):
    return sum(
        len(value.encode("utf-8")) if isinstance(value, str) else len(value)
        for model in models
        for column in model.__table__.columns
        if isinstance((value := getattr(model, column.name)), (str, bytes, bytearray, memoryview))
    )


def _page_cost(request, models, *, reserved_rows=0):
    if len(models) + reserved_rows > request.page_row_limit:
        raise CandidateRunnerError("record projection fanout exceeds the admitted row page")
    if _model_bytes(models) > request.page_byte_limit:
        raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")


def _candidate(definition, family, root_values, child=None, child_values=None):
    values_by_field = {key: root_values[key] for key in definition.query.root_fields if key in root_values}
    if child is not None:
        values_by_field.update({key: child_values[key] for key in definition.query.child_fields if key in child_values})
    return material.WinnerCandidate(
        entity_binding_id=family.entity_binding_id,
        family_revision_id=family.family_revision_id,
        family_sha256=bytes(family.family_sha256),
        context_collection_slot=0 if child is None else child.collection_slot,
        context_child_revision_id=None if child is None else child.child_revision_id,
        context_child_key_sha256=None if child is None else bytes(child.child_key_sha256),
        values_by_field=values_by_field,
    )


def _contexts(request, registry, build_id, candidate):
    scopes = material._profile_scopes(request.definition, registry.child_collection_slots)
    if not any(scope.collection_slot == candidate.context_collection_slot for scope in scopes):
        return
    contract = material._winner_candidate_contract(request.definition, scopes)
    normalized = material._normalize_winner_candidate(candidate, contract)
    for slot, (profile, scope) in enumerate(zip(request.definition.selection_profiles, scopes, strict=True), 1):
        if scope.collection_slot != candidate.context_collection_slot:
            continue
        material._validate_selection_values(profile, normalized, contract.fields_by_id)
        canonical, digest = material._context_key(profile, normalized, contract.fields_by_id)
        yield CustomImportBuildCandidateContext(
            build_id=build_id,
            profile_slot=slot,
            entity_binding_id=candidate.entity_binding_id,
            family_revision_id=candidate.family_revision_id,
            context_collection_slot=candidate.context_collection_slot,
            context_child_revision_id=candidate.context_child_revision_id,
            canonical_context_key=canonical,
            context_key_sha256=digest,
        )


def _root_models(request, registry, build_id, family, root_values):
    scalars = material.project_root_scalars(
        request.definition,
        root_target=material.RootScalarTarget(
            request.dataset_id, request.schema_revision_id, family.root_record_id, family.root_revision_id
        ),
        root_values=root_values,
    )
    return [
        *material.scalar_projection_models(request.definition, root_scalars=scalars),
        *_contexts(request, registry, build_id, _candidate(request.definition, family, root_values)),
    ]


def _child_models(request, registry, build_id, family, root_values, child, child_values, collection):
    edge = CustomImportFamilyChild(
        family_revision_id=family.family_revision_id,
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=family.root_record_id,
        collection_slot=child.collection_slot,
        child_revision_id=child.child_revision_id,
    )
    scalars = material.project_child_scalars(
        request.definition,
        collection=collection,
        child_target=material.ChildScalarTarget(
            request.dataset_id,
            request.schema_revision_id,
            family.root_record_id,
            child.collection_slot,
            child.child_revision_id,
        ),
        child_values=child_values,
        child_collection_slots=registry.child_collection_slots,
    )
    projections = [
        edge,
        *material.scalar_projection_models(
            request.definition, child_scalars=scalars, child_collection_slots=registry.child_collection_slots
        ),
    ]
    if collection == request.definition.query.child_collection:
        projections.extend(
            _contexts(
                request, registry, build_id, _candidate(request.definition, family, root_values, child, child_values)
            )
        )
    return projections


@dataclass(frozen=True)
class _FamilyInput:
    plan: CustomImportBuildFamily
    root: CustomImportRootRevision
    record: CustomImportRootRecord
    values: object
    family_sha256: bytes
    child_count: int
    entity_binding_id: int | None


def _child_statement(plan, definition, *, canonical=False, collection_slot=None, models=None):
    child = CustomImportChildRevision if models is None else models[CustomImportChildRevision]
    occurrence = CustomImportBuildOccurrence if models is None else models[CustomImportBuildOccurrence]
    edge = CustomImportFamilyChild if models is None else models[CustomImportFamilyChild]
    if plan.selection_kind == "retained" and not canonical:
        statement = (
            select(child)
            .join(edge, edge.child_revision_id == child.child_revision_id)
            .where(edge.family_revision_id == plan.base_family_revision_id)
        )
        keys = (edge.collection_slot, edge.child_revision_id)
    else:
        statement = (
            select(child)
            .join(occurrence, occurrence.child_revision_id == child.child_revision_id)
            .where(
                occurrence.build_id == plan.build_id,
                occurrence.root_record_id == plan.root_record_id,
                occurrence.origin == ("source" if plan.selection_kind == "source" else "retained"),
                occurrence.resolved_rejection_id.is_(None),
            )
        )
        keys = (child.collection_slot, child.child_key_sha256, child.child_revision_id)
        collapse_slots = tuple(
            slot
            for slot, stream in enumerate(definition.source_streams, 1)
            if stream.duplicate_policy == "collapse_identical"
        )
        if plan.selection_kind == "source" and collapse_slots:
            later = aliased(occurrence)
            statement = statement.where(
                or_(
                    occurrence.stream_slot.not_in(collapse_slots),
                    ~select(later.occurrence_id)
                    .where(
                        later.build_id == occurrence.build_id,
                        later.origin == "source",
                        later.stream_slot == occurrence.stream_slot,
                        later.root_record_id == occurrence.root_record_id,
                        later.collection_slot == occurrence.collection_slot,
                        later.raw_parent_key_sha256 == occurrence.raw_parent_key_sha256,
                        later.child_key_sha256 == occurrence.child_key_sha256,
                        later.child_revision_id.is_not(None),
                        later.source_ordinal > occurrence.source_ordinal,
                    )
                    .correlate(occurrence)
                    .exists(),
                )
            )
    if collection_slot is not None:
        statement = statement.where(child.collection_slot == collection_slot)
    return statement, keys


def _selected_revisions(session, request, build_id, plan):
    base_family = None
    if plan.selection_kind == "retained":
        base_family = _one_row(
            session,
            request,
            build_id,
            select(CustomImportFamilyRevision).where(
                CustomImportFamilyRevision.family_revision_id == plan.base_family_revision_id
            ),
            (CustomImportFamilyRevision.family_revision_id,),
            (CustomImportFamilyRevision,),
        )[0]
        root_id = base_family.root_revision_id
    else:
        with _read_transaction(session, request, build_id):
            root_id = session.scalar(
                select(CustomImportBuildOccurrence.root_revision_id).where(
                    CustomImportBuildOccurrence.occurrence_id == plan.source_root_occurrence_id,
                    CustomImportBuildOccurrence.build_id == build_id,
                )
            )
    root, root_record = _one_row(
        session,
        request,
        build_id,
        select(CustomImportRootRevision, CustomImportRootRecord)
        .join(CustomImportRootRecord, CustomImportRootRecord.root_record_id == CustomImportRootRevision.root_record_id)
        .where(CustomImportRootRevision.root_revision_id == root_id),
        (CustomImportRootRevision.root_revision_id,),
        (CustomImportRootRevision, CustomImportRootRecord),
    )
    return base_family, root, root_record


def _source_family_digest(session, request, registry, plan, root, root_record, root_values):
    child_counter = count()

    def _documents(collection):
        statement, keys = _child_statement(
            plan, request.definition, canonical=True, collection_slot=registry.child_collection_slots[collection]
        )
        for (child,) in _read_rows(
            session,
            request,
            plan.build_id,
            statement,
            keys,
            (CustomImportChildRevision,),
            bounds=_ReadPage(reserve_bytes=_model_bytes((root, root_record))),
        ):
            next(child_counter)
            yield bytes(child.child_key_sha256), child.canonical_child_key, child.canonical_payload

    digest = new_family_hash_ordered(
        request.definition,
        root_values,
        {collection.name: _documents(collection.name) for collection in request.definition.child_collections},
    )
    return digest, next(child_counter)


def _family_input(session, request, registry, build_id, root_id):
    plan = _one_row(
        session,
        request,
        build_id,
        select(CustomImportBuildFamily).where(
            CustomImportBuildFamily.build_id == build_id, CustomImportBuildFamily.root_record_id == root_id
        ),
        (CustomImportBuildFamily.root_record_id,),
        (CustomImportBuildFamily,),
    )[0]
    base_family, root, root_record = _selected_revisions(session, request, build_id, plan)
    root_values = payload_values(request.definition.root_fields, root.canonical_payload, label="build root payload")
    if record_payload(request.definition.root_fields, root_values) != root.canonical_payload or digest_text(
        "root-payload", root.canonical_payload
    ) != bytes(root.payload_sha256):
        raise CandidateRunnerError("build root payload is not canonical")
    if base_family is not None:
        binding = _one_row(
            session,
            request,
            build_id,
            select(CustomImportEntityBinding).where(
                CustomImportEntityBinding.entity_binding_id == base_family.entity_binding_id
            ),
            (CustomImportEntityBinding.entity_binding_id,),
            (CustomImportEntityBinding,),
        )[0]
        stored_root_values(request, root_record, root, binding)
        return _FamilyInput(
            plan,
            root,
            root_record,
            root_values,
            bytes(base_family.family_sha256),
            base_family.child_count,
            binding.entity_binding_id,
        )
    digest, child_count = _source_family_digest(session, request, registry, plan, root, root_record, root_values)
    return _FamilyInput(plan, root, root_record, root_values, digest, child_count, None)


def _copy_record(request, family_input, retained_revision, pack, collection):
    revision_fields_by_name = dict(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=family_input.plan.root_record_id,
        pack_id=pack.pack_id,
        source_ordinal=0,
        canonical_payload=retained_revision.canonical_payload,
        payload_sha256=retained_revision.payload_sha256,
    )
    if collection is None:
        return CustomImportRootRevision(**revision_fields_by_name)
    return CustomImportChildRevision(
        **revision_fields_by_name,
        collection_slot=retained_revision.collection_slot,
        canonical_parent_key=retained_revision.canonical_parent_key,
        parent_key_sha256=retained_revision.parent_key_sha256,
        canonical_child_key=retained_revision.canonical_child_key,
        child_key_sha256=retained_revision.child_key_sha256,
    )


def _copy_occurrence(build, plan, retained_revision, revision, pack, collection):
    return CustomImportBuildOccurrence(
        build_id=build.build_id,
        stream_slot=pack.stream_slot,
        pack_id=pack.pack_id,
        origin="retained",
        base_family_revision_id=plan.base_family_revision_id,
        base_root_revision_id=retained_revision.root_revision_id if collection is None else None,
        base_child_revision_id=retained_revision.child_revision_id if collection is not None else None,
        record_kind="root" if collection is None else "child",
        collection_slot=0 if collection is None else retained_revision.collection_slot,
        root_record_id=plan.root_record_id,
        root_revision_id=revision.root_revision_id if collection is None else None,
        child_revision_id=revision.child_revision_id if collection is not None else None,
        child_key_sha256=None if collection is None else retained_revision.child_key_sha256,
    )


def _retained_page_scope(request, registry, current, children):
    """Require one collection and a strictly increasing retained-child cursor."""

    names_by_slot = {slot: name for name, slot in registry.child_collection_slots.items()}
    previous = (current.last_child_collection_slot, current.last_input_child_revision_id)
    if (previous[0] is None) != (previous[1] is None) or current.last_child_key_sha256 is not None:
        raise CandidateRunnerError("retained child order differs")
    seen_child_ids = set()
    collection_slot = None
    for child in children:
        position = (child.collection_slot, child.child_revision_id)
        if (
            type(child.child_revision_id) is not int
            or not 1 <= child.child_revision_id < 2**63
            or child.child_revision_id in seen_child_ids
            or type(child.collection_slot) is not int
            or child.collection_slot not in names_by_slot
            or (previous[0] is not None and position <= previous)
        ):
            raise CandidateRunnerError("retained child order differs")
        previous = position
        seen_child_ids.add(child.child_revision_id)
        if collection_slot is not None and child.collection_slot != collection_slot:
            raise CandidateRunnerError("retained page crosses a collection or stream boundary")
        collection_slot = child.collection_slot
    if collection_slot is None:
        return None, None
    collection = names_by_slot[collection_slot]
    stream = next(stream for stream in request.definition.source_streams if stream.child_collection == collection)
    return collection, registry.stream_slots[stream.stream_id]


def _retained_page_models(request, registry, family_input, current, family, children):
    """Reuse canonical child validation and account for one retained pack."""

    collection, stream_slot = _retained_page_scope(request, registry, current, children)
    token_hash = lease_token_sha256(request.lease_token)
    pack = CustomImportPack(stream_slot=stream_slot, producing_token_sha256=token_hash)
    fields = fields_by_collection(request.definition).get(collection)
    child_ids, context_child_ids, profile_slots, context_keys, copied_models, payload_hashes = [], [], [], [], [], []
    for child in children:
        child_values = payload_values(fields, child.canonical_payload, label="retained child payload")
        verify_stored_child(request, family_input.record, StoredCandidateChild(collection, child, child_values))
        if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
            raise CandidateRunnerError("retained child payload digest differs")
        payload_hashes.append(bytes(child.payload_sha256))
        revision = _copy_record(request, family_input, child, pack, collection)
        occurrence = _copy_occurrence(current, family_input.plan, child, revision, pack, collection)
        projections = _child_models(
            request, registry, current.build_id, family, family_input.values, child, child_values, collection
        )
        child_ids.append(child.child_revision_id)
        for context in projections:
            if isinstance(context, CustomImportBuildCandidateContext):
                context_child_ids.append(child.child_revision_id)
                profile_slots.append(context.profile_slot)
                context_keys.append(context.canonical_context_key)
        copied_models.extend((revision, occurrence, *projections))
    if children:
        pack.pack_sha256 = pack_hash(collection, payload_hashes)
        copied_models.append(pack)
    _page_cost(request, copied_models)
    return child_ids, context_child_ids, profile_slots, context_keys


def _retained_array_arguments(request, registry, family_input, current, family, children):
    """Return the explicitly typed arguments accepted by build_source._call."""

    plan = family_input.plan
    if (
        plan.selection_kind != "retained"
        or current.complete_at is not None
        or (current.build_id, current.root_record_id, current.family_revision_id)
        != (plan.build_id, plan.root_record_id, family.family_revision_id)
        or (family.root_record_id, family.entity_binding_id) != (plan.root_record_id, family_input.entity_binding_id)
    ):
        raise CandidateRunnerError("retained page identity differs")
    if len(children) > min(256, request.page_row_limit):
        raise CandidateRunnerError("record projection fanout exceeds the admitted row page")
    child_ids, context_child_ids, profile_slots, context_keys = _retained_page_models(
        request, registry, family_input, current, family, children
    )
    return (
        ("bigint", plan.build_id),
        ("bigint", request.execution_id),
        ("bigint", request.fence),
        ("bytea", lease_token_sha256(request.lease_token)),
        ("bigint", plan.root_record_id),
        ("bigint", family.family_revision_id),
        ("bigint", current.attached_child_count),
        ("smallint", current.last_child_collection_slot),
        ("bigint", current.last_input_child_revision_id),
        ("bigint[]", tuple(child_ids)),
        ("bigint[]", tuple(context_child_ids)),
        ("smallint[]", tuple(profile_slots)),
        ("text[]", tuple(context_keys)),
    )


def _root_identity_models(request, family_input):
    """Charge exact immutable dictionary bytes without another database read."""

    value = family_input.values[request.definition.entity_field]
    return family_input.record, CustomImportEntityBinding(
        dataset_id=request.dataset_id,
        adapter_id="npi",
        canonical_value=value,
        value_sha256=entity_value_digest(value),
    )


def _retained_root_arguments(request, registry, family_input):
    """Preflight root copy cost and regenerate only current-definition contexts."""
    plan = family_input.plan
    root = family_input.root
    token_hash = lease_token_sha256(request.lease_token)
    family = CustomImportFamilyRevision(
        family_revision_id=plan.base_family_revision_id,
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=plan.root_record_id,
        root_revision_id=root.root_revision_id,
        entity_binding_id=family_input.entity_binding_id,
        family_sha256=family_input.family_sha256,
        child_count=family_input.child_count,
        producing_execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=token_hash,
    )
    pack = CustomImportPack(
        stream_slot=registry.root_stream_slot,
        producing_token_sha256=token_hash,
        pack_sha256=pack_hash("root", [bytes(root.payload_sha256)]),
    )
    revision = _copy_record(request, family_input, root, pack, None)
    occurrence = _copy_occurrence(plan, plan, root, revision, pack, None)
    projections = _root_models(request, registry, plan.build_id, family, family_input.values)
    _page_cost(
        request, [pack, revision, occurrence, family, *_root_identity_models(request, family_input), *projections]
    )
    contexts = [projection for projection in projections if isinstance(projection, CustomImportBuildCandidateContext)]
    return (
        ("bigint", plan.build_id),
        ("bigint", request.execution_id),
        ("bigint", request.fence),
        ("bytea", token_hash),
        ("bigint", plan.root_record_id),
        ("bigint", plan.base_family_revision_id),
        ("bigint", root.root_revision_id),
        ("bigint", family_input.entity_binding_id),
        ("bytea", family_input.family_sha256),
        ("bigint", family_input.child_count),
        ("smallint[]", tuple(context.profile_slot for context in contexts)),
        ("text[]", tuple(context.canonical_context_key for context in contexts)),
    )


def _source_root_models(request, registry, family_input):
    """Validate canonical SOURCE values before deriving current projections."""

    plan, root = family_input.plan, family_input.root
    if plan.selection_kind != "source" or family_input.entity_binding_id is not None:
        raise CandidateRunnerError("source root selection differs")
    root_values = payload_values(request.definition.root_fields, root.canonical_payload, label="build root payload")
    if (
        root_values != family_input.values
        or record_payload(request.definition.root_fields, root_values) != root.canonical_payload
    ):
        raise CandidateRunnerError("build root payload is not canonical")
    if digest_text("root-payload", root.canonical_payload) != bytes(root.payload_sha256):
        raise CandidateRunnerError("build root payload is not canonical")
    token_hash = lease_token_sha256(request.lease_token)
    # Provisional positive IDs validate the existing context contract. SQL derives
    # permanent entity/family IDs; neither provisional ID enters context bytes.
    family = CustomImportFamilyRevision(
        family_revision_id=1,
        entity_binding_id=1,
        dataset_id=request.dataset_id,
        schema_revision_id=request.schema_revision_id,
        root_record_id=plan.root_record_id,
        root_revision_id=root.root_revision_id,
        family_sha256=family_input.family_sha256,
        child_count=family_input.child_count,
        producing_execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=token_hash,
    )
    projections = _root_models(request, registry, plan.build_id, family, root_values)
    root_record, entity = _root_identity_models(request, family_input)
    # Reserve a possible canonical interner INSERT as well as both snapshot copies.
    _page_cost(request, [family, root_record, entity, entity, *projections], reserved_rows=2)
    return projections, root_values[request.definition.entity_field]


def _source_root_arguments(request, registry, family_input):
    """Encode native projection arrays for one protected SOURCE root call."""

    plan, root = family_input.plan, family_input.root
    projections, entity_value = _source_root_models(request, registry, family_input)
    scalars = [
        projection for projection in projections if not isinstance(projection, CustomImportBuildCandidateContext)
    ]
    contexts = [projection for projection in projections if isinstance(projection, CustomImportBuildCandidateContext)]
    columns = (
        ("smallint[]", "field_slot"),
        ("text[]", "field_type"),
        ("text[]", "value_state"),
        ("text[]", "string_value"),
        ("bigint[]", "integer_value"),
        ("numeric[]", "decimal_value"),
        ("boolean[]", "boolean_value"),
        ("date[]", "date_value"),
        ("timestamptz[]", "timestamp_value"),
    )
    return (
        ("bigint", plan.build_id),
        ("bigint", request.execution_id),
        ("bigint", request.fence),
        ("bytea", lease_token_sha256(request.lease_token)),
        ("bigint", plan.root_record_id),
        ("bigint", plan.source_root_occurrence_id),
        ("bigint", root.root_revision_id),
        ("bytea", bytes(root.payload_sha256)),
        ("bytea", family_input.family_sha256),
        ("bigint", family_input.child_count),
        ("text", entity_value),
        ("bytea", entity_value_digest(entity_value)),
        *((kind, tuple(getattr(scalar, column) for scalar in scalars)) for kind, column in columns),
        ("smallint[]", tuple(context.profile_slot for context in contexts)),
        ("text[]", tuple(context.canonical_context_key for context in contexts)),
    )


async def _start_family(session_factory, request, registry, family_input):
    plan = family_input.plan
    async with _page_session(session_factory, request, plan.build_id) as (session, _build):
        if plan.selection_kind == "retained":
            arguments = _retained_root_arguments(request, registry, family_input)
            name = "retained_root_finalize"
        elif plan.selection_kind == "source":
            arguments = _source_root_arguments(request, registry, family_input)
            name = "source_root_start"
        else:
            raise CandidateRunnerError("family selection differs")
        (await _source_call(session, name, arguments)).scalar_one()


def _child_ranges(registry, current):
    slot = current.last_child_collection_slot
    if current.selection_kind == "retained":
        yield None, None if slot is None else (slot, current.last_input_child_revision_id)
        return
    last_name = (
        None
        if slot is None
        else next(name for name, candidate_slot in registry.child_collection_slots.items() if candidate_slot == slot)
    )
    for name, candidate_slot in sorted(registry.child_collection_slots.items()):
        if last_name is not None and name < last_name:
            continue
        after = None
        if name == last_name:
            after = (slot, bytes(current.last_child_key_sha256), current.last_input_child_revision_id)
        yield candidate_slot, after


def _child_page_limit(request, registry, plan):
    """Reserve revision, occurrence and membership rows before scalar/context fanout."""

    scopes = material._profile_scopes(request.definition, registry.child_collection_slots)
    maximum_work = max(
        (
            3
            + sum(field.projection_slot is not None for field in request.definition.fields if field.collection == name)
            + sum(scope.collection_slot == slot for scope in scopes)
            for name, slot in registry.child_collection_slots.items()
        ),
        default=1,
    )
    reserved_pack = int(plan.selection_kind == "retained")
    return max(1, (request.page_row_limit - reserved_pack) // maximum_work)


def _next_children(session, request, registry, family_input, row_limit):
    plan = family_input.plan
    current = _one_row(
        session,
        request,
        plan.build_id,
        select(CustomImportBuildFamily).where(
            CustomImportBuildFamily.build_id == plan.build_id,
            CustomImportBuildFamily.root_record_id == plan.root_record_id,
        ),
        (CustomImportBuildFamily.root_record_id,),
        (CustomImportBuildFamily,),
    )[0]
    if current.complete_at is not None:
        return current, []
    for slot, after in _child_ranges(registry, current):
        statement, keys = _child_statement(current, request.definition, collection_slot=slot)
        children = _read_rows(
            session,
            request,
            plan.build_id,
            statement,
            keys,
            (CustomImportChildRevision,),
            after=after,
            bounds=_ReadPage(
                row_limit=row_limit,
                reserve_bytes=_model_bytes((family_input.root, family_input.record)),
                single_page=True,
            ),
        )
        try:
            child_revisions = []
            for (child,) in children:
                if (
                    current.selection_kind == "retained"
                    and child_revisions
                    and child.collection_slot != child_revisions[0].collection_slot
                ):
                    break
                child_revisions.append(child)
            if child_revisions:
                return current, child_revisions
        finally:
            children.close()
    return current, []


async def _child_page_models(session, request, registry, build, family_input, child, family):
    collection = next(name for name, slot in registry.child_collection_slots.items() if slot == child.collection_slot)
    child_values = payload_values(
        fields_by_collection(request.definition)[collection], child.canonical_payload, label="build child payload"
    )
    verify_stored_child(request, family_input.record, StoredCandidateChild(collection, child, child_values))
    if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
        raise CandidateRunnerError("build child payload digest differs")
    return _child_models(
        request, registry, build.build_id, family, family_input.values, child, child_values, collection
    )


async def _append_child_batch(session_factory, request, registry, family_input, current, children):
    plan = family_input.plan
    async with _page_session(session_factory, request, plan.build_id) as (session, build):
        await _prepare_statement(session)
        progress = await session.get(
            CustomImportBuildFamily, (plan.build_id, plan.root_record_id), with_for_update=True
        )
        if progress.attached_child_count != current.attached_child_count or progress.complete_at is not None:
            return
        if plan.selection_kind == "retained" and (
            progress.family_revision_id,
            progress.last_child_collection_slot,
            progress.last_input_child_revision_id,
        ) != (current.family_revision_id, current.last_child_collection_slot, current.last_input_child_revision_id):
            return
        await _prepare_statement(session)
        family = await session.get(CustomImportFamilyRevision, progress.family_revision_id)
        if plan.selection_kind == "retained":
            arguments = _retained_array_arguments(request, registry, family_input, progress, family, children)
            receipt = (await _source_call(session, "retained_array_finalize", arguments)).one()
            if not children and not receipt.complete:
                raise CandidateRunnerError("family input ended before SQL completion")
            return
        projections = []
        for child in children:
            projections.extend(await _child_page_models(session, request, registry, build, family_input, child, family))
        _page_cost(request, projections, reserved_rows=2 * len(children) if plan.selection_kind == "source" else 0)
        progress_receipt = await append_source_child_page(session, plan, progress, family, children, projections)
        if not children and not progress_receipt.complete:
            raise CandidateRunnerError("family input ended before SQL completion")


def _is_child_page_bound_error(error):
    if getattr(error, "_custom_import_retry_blocked", False):
        return False
    if isinstance(error, CandidateRunnerError):
        return str(error) in {
            "record projection fanout exceeds the admitted row page",
            "record projection fanout exceeds the admitted byte page",
        }
    original = error.orig
    message = getattr(getattr(original, "__cause__", None), "message", None)
    if message is None:
        message = getattr(getattr(original, "diag", None), "message_primary", None)
    return getattr(original, "sqlstate", None) == "P0001" and message == "custom_import_build_page_too_large"


async def _append_children(session_factory, request, registry, family_input):
    row_limit = _child_page_limit(request, registry, family_input.plan)
    while True:
        async with _session(session_factory) as session:
            current, children = await session.run_sync(
                lambda sync: _next_children(sync, request, registry, family_input, row_limit)
            )
        if current.complete_at is not None:
            return
        try:
            await _append_child_batch(session_factory, request, registry, family_input, current, children)
        except (CandidateRunnerError, DBAPIError) as error:
            if len(children) < 2 or not _is_child_page_bound_error(error):
                raise
            # SQL charges serialized scalar rows exactly. Retry only after the
            # whole oversized page has rolled back, retaining its original cursor.
            row_limit = max(1, len(children) // 2)
            continue
        await _heartbeat(session_factory, request)


def _candidate_digest(session, request, build_id, *, bounds=_ReadPage()):
    family_counter = count()

    def pending_families(candidate_models, _base_models):
        """Scan candidate plan hashes under the current registered binding."""
        plan = candidate_models[CustomImportBuildFamily]
        statement = select(plan).where(plan.build_id == build_id)
        return statement, (plan.root_key_sha256, plan.root_record_id), (plan,)

    def _hashes():
        for (family,) in _read_snapshot_rows(session, request, build_id, pending_families, bounds=bounds):
            if family.complete_at is None:
                raise CandidateRunnerError("candidate fingerprint requires completed families")
            next(family_counter)
            yield bytes(family.root_key_sha256)
            del family

    digest = candidate_hash_ordered(
        execution_id=request.execution_id,
        fence=request.fence,
        base_generation_id=request.expected_base_generation_id,
        root_key_hashes=_hashes(),
    )
    return digest, next(family_counter)


def _source_digest(session, request, build_id, capture_bundle_id):
    """Hash every declared capture in canonical stream order."""
    bundle = _one_row(
        session,
        request,
        build_id,
        select(CustomImportCaptureBundle).where(CustomImportCaptureBundle.capture_bundle_id == capture_bundle_id),
        (CustomImportCaptureBundle.capture_bundle_id,),
        (CustomImportCaptureBundle,),
    )[0]
    if (
        bundle.capture_state != "sealed"
        or bundle.dataset_id != request.dataset_id
        or bundle.definition_revision_id != request.definition_revision_id
        or bundle.schema_revision_id != request.schema_revision_id
    ):
        raise CandidateRunnerError("build capture identity is not sealed")
    digest = publication._new_digest(publication._SOURCE_BUNDLE_DOMAIN)
    publication._add_digest_record(
        digest,
        "bundle",
        {"manifest_sha256": bytes(bundle.manifest_sha256).hex(), "stream_count": bundle.stream_count},
    )
    captured_slots = []
    for (capture,) in _read_rows(
        session,
        request,
        build_id,
        select(CustomImportCapture).where(CustomImportCapture.capture_bundle_id == capture_bundle_id),
        (CustomImportCapture.stream_slot,),
        (CustomImportCapture,),
    ):
        captured_slots.append(capture.stream_slot)
        publication._add_digest_record(
            digest,
            "capture",
            {
                "byte_count": capture.byte_count,
                "content_sha256": bytes(capture.content_sha256).hex(),
                "manifest_sha256": bytes(capture.manifest_sha256).hex(),
                "stream_slot": capture.stream_slot,
            },
        )
    declared_slots = [
        stream.stream_slot
        for (stream,) in _read_rows(
            session,
            request,
            build_id,
            select(CustomImportSourceStream).where(
                CustomImportSourceStream.definition_revision_id == request.definition_revision_id
            ),
            (CustomImportSourceStream.stream_slot,),
            (CustomImportSourceStream,),
        )
    ]
    publication._require_exact_capture_coverage(bundle, declared_slots, captured_slots)
    return digest.digest()


def _next_family_inputs(session, request, registry, build_id, after_root_id):
    # Keep shared read primitives in this module; import the preparation leaf
    # only after module initialization to avoid an import cycle.
    from process.custom_import.build_graph_prepare_page import prepare_family_page

    return prepare_family_page(session, request, registry, build_id, after_root_id, physical=True)


async def _consume_family_page(session_factory, request, registry, family_inputs):
    from process.custom_import.build_graph_sets import consume_family_page

    await consume_family_page(session_factory, request, registry, family_inputs)


async def _build_graph(session_factory, request: SourceBuildRequest, build_id: int) -> int | None:
    """Finish a SQL-selected graph and atomically open its real generation."""

    build = await _snapshot(session_factory, request, build_id)
    if build.phase == "rejected":
        return None
    if build.generation_id is not None:
        return build.generation_id
    if build.phase != "graph":
        raise CandidateRunnerError("graph construction requires completed source admission")
    await _prepare_snapshot_indexes(session_factory, request, build_id, "graph")
    async with _page_session(session_factory, request, build_id) as (session, _build):
        registry = await load_registry(session, request)
    while build.plan_complete_at is None:
        async with _page_session(session_factory, request, build_id) as (session, current):
            await _call(
                session,
                "plan_custom_import_build_family_page",
                (build_id, current.plan_page_sequence),
                ("phase", "plan_stage", "page_sequence", "rows_processed", "plan_complete"),
            )
        await _heartbeat(session_factory, request)
        build = await _snapshot(session_factory, request, build_id)
    after_root_id = 0
    while True:
        async with _session(session_factory) as session:
            family_inputs = await session.run_sync(
                lambda sync: _next_family_inputs(sync, request, registry, build_id, after_root_id)
            )
        if not family_inputs:
            break
        await _consume_family_page(session_factory, request, registry, family_inputs)
        after_root_id = family_inputs[-1].plan.root_record_id
        await _heartbeat(session_factory, request)
    return await _open_output(session_factory, request, build_id, build.capture_bundle_id)


async def _open_output(session_factory, request, build_id, capture_bundle_id):
    async with _session(session_factory) as session:
        candidate_digest, count = await session.run_sync(lambda sync: _candidate_digest(sync, request, build_id))
        source_digest = await session.run_sync(lambda sync: _source_digest(sync, request, build_id, capture_bundle_id))
    async with _page_session(session_factory, request, build_id) as (session, build):
        if build.generation_id is not None:
            return build.generation_id
        generation = CustomImportGeneration(
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
            execution_id=request.execution_id,
            capture_bundle_id=build.capture_bundle_id,
            base_generation_id=build.base_generation_id,
            base_dataset_id=None if build.base_generation_id is None else request.dataset_id,
            source_bundle_sha256=source_digest,
            candidate_sha256=candidate_digest,
            root_count=count,
            family_count=count,
            producing_fence=request.fence,
            producing_token_sha256=lease_token_sha256(request.lease_token),
        )
        session.add(generation)
        await _flush_page(session)
        await _call(session, "open_custom_import_build_output", (build_id, generation.generation_id))
        return generation.generation_id


async def build_graph(session_factory, request: SourceBuildRequest, build_id: int) -> int | None:
    """Plan and build immutable families without selecting a live pointer."""

    async with _renew_while_reading(session_factory, request):
        return await _build_graph(session_factory, request, build_id)
