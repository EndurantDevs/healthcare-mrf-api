# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded graph construction from a frozen, admitted source build."""

from __future__ import annotations

import asyncio
import hmac
import time
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from itertools import count

from sqlalchemy import LargeBinary, String, func, or_, select, tuple_
from sqlalchemy.orm import aliased

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportBuildCandidateContext,
    CustomImportBuildFamily,
    CustomImportBuildOccurrence,
    CustomImportBuildStream,
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
from process.custom_import.build_source import (
    SourceBuildRequest,
    _assert_build_identity,
    _close_contexts,
    _enter_context,
    _flush_page,
    _page_session,
    _prepare_statement,
)
from process.custom_import.build_source import (
    _call as _source_call,
)
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


def _read_rows(session, request, build_id, statement, keys, models, *, after=None, bounds=_ReadPage()):
    """Metadata-first keyset pages, closed before the consumer hashes a page_record."""

    limit = min(bounds.row_limit or request.page_row_limit, request.page_row_limit)
    byte_limit = request.page_byte_limit - bounds.reserve_bytes
    if byte_limit < 0:
        raise CandidateRunnerError("build page has no remaining payload budget")
    variable_bytes = (
        _variable_bytes(*models)
        if bounds.variable_columns is None
        else sum((func.coalesce(func.octet_length(column), 0) for column in bounds.variable_columns), 0)
    )
    while True:
        with _read_transaction(session, request, build_id) as (_build, deadline):
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


def _one_row(session, request, build_id, statement, keys, models):
    rows = _read_rows(session, request, build_id, statement, keys, models, bounds=_ReadPage(row_limit=1))
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


def _child_statement(plan, definition, *, canonical=False, collection_slot=None):
    child = CustomImportChildRevision
    occurrence = CustomImportBuildOccurrence
    if plan.selection_kind == "retained" and not canonical:
        statement = (
            select(child)
            .join(CustomImportFamilyChild, CustomImportFamilyChild.child_revision_id == child.child_revision_id)
            .where(CustomImportFamilyChild.family_revision_id == plan.base_family_revision_id)
        )
        keys = (CustomImportFamilyChild.collection_slot, CustomImportFamilyChild.child_revision_id)
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
            later = aliased(CustomImportBuildOccurrence)
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


async def _copy_pack(session, request, registry, build, retained_revision, collection):
    stream_slot = (
        registry.root_stream_slot
        if collection is None
        else registry.stream_slots[
            next(
                stream.stream_id
                for stream in request.definition.source_streams
                if stream.child_collection == collection
            )
        ]
    )
    await _prepare_statement(session)
    stream = await session.get(CustomImportBuildStream, (build.build_id, stream_slot), with_for_update=True)
    pack = CustomImportPack(
        execution_id=request.execution_id,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        stream_slot=stream_slot,
        pack_ordinal=stream.next_pack_ordinal,
        capture_bundle_id=build.capture_bundle_id,
        record_count=1,
        pack_sha256=pack_hash(collection or "root", [bytes(retained_revision.payload_sha256)]),
        producing_fence=request.fence,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )
    session.add(pack)
    await _flush_page(session)
    return pack


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


async def _copy_revision(session, request, registry, build, family_input, retained_revision, collection):
    pack = await _copy_pack(session, request, registry, build, retained_revision, collection)
    revision = _copy_record(request, family_input, retained_revision, pack, collection)
    session.add(revision)
    await _flush_page(session)
    occurrence = _copy_occurrence(build, family_input.plan, retained_revision, revision, pack, collection)
    session.add(occurrence)
    return revision, [pack, revision, occurrence]


async def _commit_family(session, build_id, root_id, family_id):
    await _flush_page(session)
    return await _call(
        session,
        "commit_custom_import_build_family_page",
        (build_id, root_id, family_id),
        (
            "attached_child_count",
            "complete",
            "last_child_collection_slot",
            "last_child_key_sha256",
            "last_input_child_revision_id",
        ),
    )


async def _family_binding(session, request, family_input):
    if family_input.entity_binding_id is not None:
        return family_input.entity_binding_id, []
    entity_value = family_input.values[request.definition.entity_field]
    await _prepare_statement(session)
    binding = (
        await session.execute(
            select(CustomImportEntityBinding).where(
                CustomImportEntityBinding.dataset_id == request.dataset_id,
                CustomImportEntityBinding.adapter_id == "npi",
                CustomImportEntityBinding.canonical_value == entity_value,
            )
        )
    ).scalar_one_or_none()
    if binding is None:
        binding = CustomImportEntityBinding(
            dataset_id=request.dataset_id,
            adapter_id="npi",
            canonical_value=entity_value,
            value_sha256=entity_value_digest(entity_value),
        )
        session.add(binding)
        await _flush_page(session)
        return binding.entity_binding_id, [binding]
    if bytes(binding.value_sha256) != entity_value_digest(entity_value):
        raise CandidateRunnerError("build entity binding digest differs")
    return binding.entity_binding_id, []


async def _start_family(session_factory, request, registry, family_input):
    plan = family_input.plan
    async with _page_session(session_factory, request, plan.build_id) as (session, build):
        await _prepare_statement(session)
        current = await session.get(CustomImportBuildFamily, (plan.build_id, plan.root_record_id), with_for_update=True)
        if current.family_revision_id is not None:
            await _prepare_statement(session)
            family = await session.get(CustomImportFamilyRevision, current.family_revision_id)
            if (
                family.child_count != family_input.child_count
                or bytes(family.family_sha256) != family_input.family_sha256
            ):
                raise CandidateRunnerError("family retry differs from its selected input")
            return
        copied_models = []
        root = family_input.root
        if plan.selection_kind == "retained":
            root, copied_models = await _copy_revision(session, request, registry, build, family_input, root, None)
        entity_id, binding_models = await _family_binding(session, request, family_input)
        family = CustomImportFamilyRevision(
            dataset_id=request.dataset_id,
            schema_revision_id=request.schema_revision_id,
            root_record_id=plan.root_record_id,
            root_revision_id=root.root_revision_id,
            entity_binding_id=entity_id,
            family_sha256=family_input.family_sha256,
            child_count=family_input.child_count,
            producing_execution_id=request.execution_id,
            producing_fence=request.fence,
            producing_token_sha256=lease_token_sha256(request.lease_token),
        )
        session.add(family)
        await _flush_page(session)
        projections = _root_models(request, registry, plan.build_id, family, family_input.values)
        _page_cost(
            request,
            [*copied_models, *binding_models, family, *projections],
            reserved_rows=2 if plan.selection_kind == "source" else 0,
        )
        session.add_all(projections)
        await _commit_family(session, plan.build_id, plan.root_record_id, family.family_revision_id)


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


def _next_child(session, request, registry, family_input):
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
        return current, None
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
            bounds=_ReadPage(row_limit=1, reserve_bytes=_model_bytes((family_input.root, family_input.record))),
        )
        try:
            child = next(children, (None,))[0]
            if child is not None:
                return current, child
        finally:
            children.close()
    return current, None


async def _append_child_page(session, request, registry, build, family_input, child, family):
    collection = next(name for name, slot in registry.child_collection_slots.items() if slot == child.collection_slot)
    child_values = payload_values(
        fields_by_collection(request.definition)[collection], child.canonical_payload, label="build child payload"
    )
    verify_stored_child(request, family_input.record, StoredCandidateChild(collection, child, child_values))
    if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
        raise CandidateRunnerError("build child payload digest differs")
    projections = []
    if family_input.plan.selection_kind == "retained":
        child, projections = await _copy_revision(session, request, registry, build, family_input, child, collection)
    projections.extend(
        _child_models(request, registry, build.build_id, family, family_input.values, child, child_values, collection)
    )
    _page_cost(request, projections, reserved_rows=2 if family_input.plan.selection_kind == "source" else 0)
    session.add_all(model for model in projections if not isinstance(model, CustomImportBuildCandidateContext))
    await _flush_page(session)
    session.add_all(model for model in projections if isinstance(model, CustomImportBuildCandidateContext))


async def _append_children(session_factory, request, registry, family_input):
    plan = family_input.plan
    while True:
        async with _session(session_factory) as session:
            current, child = await session.run_sync(lambda sync: _next_child(sync, request, registry, family_input))
        if current.complete_at is not None:
            return
        async with _page_session(session_factory, request, plan.build_id) as (session, build):
            await _prepare_statement(session)
            progress = await session.get(
                CustomImportBuildFamily, (plan.build_id, plan.root_record_id), with_for_update=True
            )
            if progress.attached_child_count != current.attached_child_count or progress.complete_at is not None:
                continue
            await _prepare_statement(session)
            family = await session.get(CustomImportFamilyRevision, progress.family_revision_id)
            if child is not None:
                await _append_child_page(session, request, registry, build, family_input, child, family)
            progress_receipt = await _commit_family(
                session, plan.build_id, plan.root_record_id, family.family_revision_id
            )
            if child is None and not progress_receipt.complete:
                raise CandidateRunnerError("family input ended before SQL completion")
        await _heartbeat(session_factory, request)


def _candidate_digest(session, request, build_id):
    family_counter = count()

    def _hashes():
        statement = select(CustomImportBuildFamily).where(CustomImportBuildFamily.build_id == build_id)
        for (family,) in _read_rows(
            session,
            request,
            build_id,
            statement,
            (CustomImportBuildFamily.root_key_sha256, CustomImportBuildFamily.root_record_id),
            (CustomImportBuildFamily,),
        ):
            if family.complete_at is None:
                raise CandidateRunnerError("candidate fingerprint requires completed families")
            next(family_counter)
            yield bytes(family.root_key_sha256)

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


def _next_family_input(session, request, registry, build_id, after_root_id):
    plan = CustomImportBuildFamily
    plans = _read_rows(
        session,
        request,
        build_id,
        select(plan.root_record_id, plan.complete_at).where(plan.build_id == build_id),
        (plan.root_record_id,),
        (),
        after=(after_root_id,),
    )
    try:
        for root_id, completed_at in plans:
            if completed_at is None:
                return _family_input(session, request, registry, build_id, root_id)
    finally:
        plans.close()
    return None


async def _build_graph(session_factory, request: SourceBuildRequest, build_id: int) -> int | None:
    """Finish a SQL-selected graph and atomically open its real generation."""

    build = await _snapshot(session_factory, request, build_id)
    if build.phase == "rejected":
        return None
    if build.generation_id is not None:
        return build.generation_id
    if build.phase != "graph":
        raise CandidateRunnerError("graph construction requires completed source admission")
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
            family_input = await session.run_sync(
                lambda sync: _next_family_input(sync, request, registry, build_id, after_root_id)
            )
        if family_input is None:
            break
        await _start_family(session_factory, request, registry, family_input)
        await _append_children(session_factory, request, registry, family_input)
        after_root_id = family_input.plan.root_record_id
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
