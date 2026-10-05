# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Consume prepared families with bounded set reads and protected set writes.

Receipts carry real durable IDs and cursors. No per-family read, lock, heartbeat
or SQL finalizer is dispatched here. An uncertain transaction is resumed through
the normal graph entry point, which rechecks immutable roots and durable tips.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from sqlalchemy import BigInteger, LargeBinary, SmallInteger, and_, any_, bindparam, case, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import aliased

from db.models.custom_import import (
    CustomImportBuildAttempt as Build,
)
from db.models.custom_import import (
    CustomImportBuildCandidateContext as Context,
)
from db.models.custom_import import (
    CustomImportBuildFamily as Plan,
)
from db.models.custom_import import (
    CustomImportBuildOccurrence as Occurrence,
)
from db.models.custom_import import (
    CustomImportBuildStream as BuildStream,
)
from db.models.custom_import import (
    CustomImportChildRevision as Child,
)
from db.models.custom_import import (
    CustomImportChildScalar as Scalar,
)
from db.models.custom_import import (
    CustomImportFamilyChild as Member,
)
from db.models.custom_import import (
    CustomImportFamilyRevision as Family,
)
from db.models.custom_import import (
    CustomImportPack as Pack,
)
from db.models.custom_import import (
    CustomImportSourceStream as Stream,
)
from process.custom_import import build_graph as graph
from process.custom_import.build_graph_source_page import _SCALAR_COLUMNS
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import digest_text, fields_by_collection, payload_values
from process.custom_import.runner_graph import verify_stored_child
from process.custom_import.runner_types import CandidateRunnerError, StoredCandidateChild


@dataclass(frozen=True)
class _Started:
    source: graph._FamilyInput
    current: Plan
    family: Family
    complete: bool


@dataclass(frozen=True)
class _ChildGroup:
    state: _Started
    children: tuple[Child, ...]


def _transpose(rows, first, stop, *, arrays=False):
    """Transpose the existing typed codec without serializing native values."""
    return tuple(
        (
            rows[0][index][0] if arrays else rows[0][index][0] + "[]",
            tuple(value for row in rows for value in row[index][1]) if arrays else tuple(row[index][1] for row in rows),
        )
        for index in range(first, stop)
    )


def _root_arguments(request, registry, inputs):
    kind = inputs[0].plan.selection_kind
    codec = graph._source_root_arguments if kind == "source" else graph._retained_root_arguments
    rows = [codec(request, registry, item) for item in inputs]
    if kind == "source":
        scalar_roots = tuple(row[4][1] for row in rows for _ in row[12][1])
        context_roots = tuple(row[4][1] for row in rows for _ in row[21][1])
        return "start_custom_import_build_source_roots_page", (
            *rows[0][:4],
            *_transpose(rows, 4, 12),
            ("bigint[]", scalar_roots),
            *_transpose(rows, 12, 21, arrays=True),
            ("bigint[]", context_roots),
            *_transpose(rows, 21, 23, arrays=True),
            ("integer[]", (len(inputs),)),
        )
    context_roots = tuple(row[4][1] for row in rows for _ in row[10][1])
    return "copy_custom_import_build_retained_roots_page", (
        *rows[0][:4],
        *_transpose(rows, 4, 10),
        ("bigint[]", context_roots),
        *_transpose(rows, 10, 12, arrays=True),
        ("integer[]", (len(inputs),)),
    )


def _root_receipts(request, registry, inputs, receipts):
    """Bind returned native identities and complete durable progress before commit."""
    if [receipt.root_record_id for receipt in receipts] != [
        prepared_family.plan.root_record_id for prepared_family in inputs
    ]:
        raise CandidateRunnerError("root set returned a different family prefix")
    started_states = []
    for prepared_family, receipt in zip(inputs, receipts, strict=True):
        plan = prepared_family.plan
        is_retained = plan.selection_kind == "retained"
        cursor = (receipt.last_child_collection_slot, receipt.last_input_child_revision_id)
        digest = receipt.last_child_key_sha256
        native_ids = receipt.family_revision_id, receipt.root_revision_id, receipt.entity_binding_id
        if (
            any(type(native_id) is not int or native_id <= 0 for native_id in native_ids)
            or type(receipt.attached_child_count) is not int
            or not 0 <= receipt.attached_child_count <= prepared_family.child_count
            or type(receipt.complete) is not bool
            or receipt.complete != (receipt.attached_child_count == prepared_family.child_count)
            or (plan.family_revision_id is not None and receipt.family_revision_id != plan.family_revision_id)
            or (not is_retained and receipt.root_revision_id != prepared_family.root.root_revision_id)
            or (is_retained and receipt.root_revision_id == prepared_family.root.root_revision_id)
            or (is_retained and receipt.entity_binding_id != prepared_family.entity_binding_id)
            or (receipt.attached_child_count == 0 and (cursor != (None, None) or digest is not None))
            or (
                receipt.attached_child_count > 0
                and (
                    cursor[0] not in registry.child_collection_slots.values()
                    or type(cursor[1]) is not int
                    or cursor[1] <= 0
                    or (is_retained and digest is not None)
                    or (not is_retained and (digest is None or len(digest) != 32))
                )
            )
        ):
            raise CandidateRunnerError("root set returned inconsistent durable progress")
        current = Plan(
            build_id=plan.build_id,
            root_record_id=plan.root_record_id,
            selection_kind=plan.selection_kind,
            base_family_revision_id=plan.base_family_revision_id,
            family_revision_id=receipt.family_revision_id,
            attached_child_count=receipt.attached_child_count,
            last_child_collection_slot=cursor[0],
            last_child_key_sha256=digest,
            last_input_child_revision_id=cursor[1],
        )
        family = Family(
            family_revision_id=receipt.family_revision_id,
            root_record_id=plan.root_record_id,
            root_revision_id=receipt.root_revision_id,
            entity_binding_id=receipt.entity_binding_id,
            family_sha256=prepared_family.family_sha256,
            child_count=prepared_family.child_count,
        )
        started_states.append(_Started(prepared_family, current, family, receipt.complete))
    return started_states


def _root_fanout(request, registry):
    scopes = graph.material._profile_scopes(request.definition, registry.child_collection_slots)
    fanout = 6 + sum(field.projection_slot is not None for field in request.definition.root_fields)
    fanout += sum(scope.collection_slot == 0 for scope in scopes)
    return fanout


def _root_limit(request, registry):
    return max(1, min(256, request.page_row_limit // _root_fanout(request, registry)))


def _root_batch_arguments(pages):
    first = pages[0][1]
    return (
        *first[:4],
        *(
            (first[index][0], tuple(value for _inputs, page in pages for value in page[index][1]))
            for index in range(4, len(first) - 1)
        ),
        ("integer[]", tuple(len(inputs) for inputs, _page in pages)),
    )


def _logical_root_arguments(request, registry, inputs, held_bytes):
    fanout = _root_fanout(request, registry)
    limit = max(1, min(_root_limit(request, registry), MAX_BATCH_ROWS // fanout))
    pending = deque(inputs[offset : offset + limit] for offset in range(0, len(inputs), limit))
    while pending:
        chunk = pending.popleft()
        name, arguments = _root_arguments(request, registry, chunk)
        if held_bytes + _child_argument_bytes(arguments) > MAX_BATCH_BYTES and len(chunk) > 1:
            midpoint = len(chunk) // 2
            pending.extendleft((chunk[midpoint:], chunk[:midpoint]))
            continue
        if fanout * len(chunk) > MAX_BATCH_ROWS or held_bytes + _child_argument_bytes(arguments) > MAX_BATCH_BYTES:
            raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
        yield chunk, name, arguments


def _root_batches(request, registry, inputs):
    """Coalesce validated logical root arrays, without widening their request policy."""
    fanout = _root_fanout(request, registry)
    held_bytes = sum(graph._model_bytes((item.plan, item.root, item.record)) for item in inputs)
    for kind in ("source", "retained"):
        selected_inputs = tuple(item for item in inputs if item.plan.selection_kind == kind)
        pages, count, argument_bytes = [], 0, 0
        for chunk, name, arguments in _logical_root_arguments(request, registry, selected_inputs, held_bytes):
            size = _child_argument_bytes(arguments)
            if pages and (
                fanout * (count + len(chunk)) > MAX_BATCH_ROWS or held_bytes + argument_bytes + size > MAX_BATCH_BYTES
            ):
                yield name, tuple(item for page, _arguments in pages for item in page), _root_batch_arguments(pages)
                pages, count, argument_bytes = [], 0, 0
            pages.append((chunk, arguments))
            count += len(chunk)
            argument_bytes += size
        if pages:
            yield name, tuple(item for page, _arguments in pages for item in page), _root_batch_arguments(pages)


async def _start_roots(session_factory, request, registry, inputs):
    states = []
    for initial in _root_batches(request, registry, inputs):
        pending = deque((initial,))
        while pending:
            name, chunk, arguments = pending.popleft()
            try:
                async with graph._page_session(session_factory, request, chunk[0].plan.build_id) as (session, _build):
                    rows = (await graph._source_call(session, name, arguments)).all()
                    started = _root_receipts(request, registry, chunk, rows)
            except (CandidateRunnerError, DBAPIError) as error:
                if len(chunk) < 2 or not graph._is_child_page_bound_error(error):
                    raise
                midpoint = len(chunk) // 2
                pending.extendleft(
                    reversed(
                        tuple(_root_batches(request, registry, chunk[:midpoint]))
                        + tuple(_root_batches(request, registry, chunk[midpoint:]))
                    )
                )
                continue
            states.extend(started)
    return sorted(states, key=lambda state: state.current.root_record_id)


def _child_tips(states):
    """Bind frozen family identities and nullable cursors with six native arrays."""
    return (
        func.unnest(
            *(
                bindparam("tip_" + field, tuple(getattr(state.current, field) for state in states), type_=ARRAY(kind))
                for field, kind in (
                    ("root_record_id", BigInteger),
                    ("family_revision_id", BigInteger),
                    ("base_family_revision_id", BigInteger),
                    ("last_child_collection_slot", SmallInteger),
                    ("last_child_key_sha256", LargeBinary),
                    ("last_input_child_revision_id", BigInteger),
                )
            )
        )
        .table_valued("root_id", "family_id", "base_family_id", "after_slot", "after_hash", "after_id")
        .render_derived(name="tips")
    )


def _after_tip(registry, kind, order, tips):
    if kind == "retained":
        after = tips.c.after_slot, tips.c.after_id
    else:
        after = (
            case(
                {slot: name for name, slot in registry.child_collection_slots.items()},
                value=tips.c.after_slot,
                else_="",
            ).collate("C"),
            tips.c.after_hash,
            tips.c.after_id,
        )
    return or_(tips.c.after_slot.is_(None), tuple_(*order) > tuple_(*after))


def _build_child_scope(request, plan, child):
    return and_(
        Build.dataset_id == request.dataset_id,
        Build.definition_revision_id == request.definition_revision_id,
        Build.schema_revision_id == request.schema_revision_id,
        Build.execution_id == request.execution_id,
        Build.producing_fence == request.fence,
        Build.producing_token_sha256 == lease_token_sha256(request.lease_token),
        Build.phase == "graph",
        Build.plan_complete_at.is_not(None),
        child.child_revision_id.is_not(None),
        child.dataset_id == Build.dataset_id,
        child.schema_revision_id == Build.schema_revision_id,
        child.root_record_id == plan.root_record_id,
    )


def _source_child_scope(child, occurrence, pack):
    return and_(
        child.definition_revision_id == Build.definition_revision_id,
        occurrence.record_kind == "child",
        child.collection_slot == occurrence.collection_slot,
        child.pack_id == occurrence.pack_id,
        child.source_ordinal == occurrence.source_ordinal,
        child.child_key_sha256 == occurrence.child_key_sha256,
        pack.dataset_id == Build.dataset_id,
        pack.definition_revision_id == Build.definition_revision_id,
        pack.schema_revision_id == Build.schema_revision_id,
        pack.execution_id == Build.execution_id,
        pack.capture_bundle_id == Build.capture_bundle_id,
        pack.producing_fence == Build.producing_fence,
        pack.producing_token_sha256 == Build.producing_token_sha256,
        pack.stream_slot == occurrence.stream_slot,
        Stream.dataset_id == Build.dataset_id,
        Stream.schema_revision_id == Build.schema_revision_id,
        Stream.record_kind == "child",
        Stream.collection_slot == child.collection_slot,
        BuildStream.replay_verified_at.is_not(None),
        pack.pack_ordinal < BuildStream.next_pack_ordinal,
    )


def _last_source_occurrence(request, registry, occurrence):
    collapse_slots = tuple(
        registry.stream_slots[stream.stream_id]
        for stream in request.definition.source_streams
        if stream.duplicate_policy == "collapse_identical"
    )
    if not collapse_slots:
        return True
    later = aliased(occurrence)
    return or_(
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


def _source_child_statement(statement, request, registry, candidate_models):
    plan, child = candidate_models[Plan], candidate_models[Child]
    occurrence, pack = candidate_models[Occurrence], candidate_models[Pack]
    statement = (
        statement.join(
            occurrence, and_(occurrence.build_id == plan.build_id, occurrence.root_record_id == plan.root_record_id)
        )
        .outerjoin(child, child.child_revision_id == occurrence.child_revision_id)
        .outerjoin(pack, pack.pack_id == child.pack_id)
        .outerjoin(
            Stream,
            and_(
                Stream.definition_revision_id == Build.definition_revision_id,
                Stream.stream_slot == occurrence.stream_slot,
            ),
        )
        .outerjoin(
            BuildStream, and_(BuildStream.build_id == Build.build_id, BuildStream.stream_slot == occurrence.stream_slot)
        )
        .where(
            occurrence.origin == "source",
            occurrence.child_revision_id.is_not(None),
            occurrence.resolved_rejection_id.is_(None),
            _last_source_occurrence(request, registry, occurrence),
        )
    )
    collection = case(
        {slot: name for name, slot in registry.child_collection_slots.items()},
        value=occurrence.collection_slot,
        else_="",
    ).collate("C")
    return statement, (collection, occurrence.child_key_sha256, occurrence.child_revision_id)


def _children_statement(request, registry, states, candidate_models, base_models):
    """A global keyset query; broken payload relationships remain visible."""
    kind = states[0].current.selection_kind
    build_id = states[0].current.build_id
    plan = candidate_models[Plan]
    child = candidate_models[Child] if kind == "source" else (Child if base_models is None else base_models[Child])
    tips = _child_tips(states)
    statement = (
        select(plan.root_record_id, child)
        .select_from(plan)
        .join(Build, Build.build_id == plan.build_id)
        .join(tips, tips.c.root_id == plan.root_record_id)
    )
    valid = _build_child_scope(request, plan, child)
    if kind == "retained":
        member = Member if base_models is None else base_models[Member]
        statement = statement.join(member, member.family_revision_id == plan.base_family_revision_id).outerjoin(
            child,
            child.child_revision_id == member.child_revision_id,
        )
        valid = and_(
            valid,
            member.dataset_id == Build.dataset_id,
            member.schema_revision_id == Build.schema_revision_id,
            member.root_record_id == plan.root_record_id,
            member.collection_slot == child.collection_slot,
        )
        order = member.collection_slot, member.child_revision_id
    else:
        statement, order = _source_child_statement(statement, request, registry, candidate_models)
        valid = and_(valid, _source_child_scope(child, candidate_models[Occurrence], candidate_models[Pack]))
    identities = and_(
        plan.family_revision_id == tips.c.family_id,
        plan.base_family_revision_id.is_not_distinct_from(tips.c.base_family_id),
    )
    statement = statement.where(
        plan.build_id == build_id,
        plan.selection_kind == kind,
        _after_tip(registry, kind, order, tips),
    ).add_columns(and_(valid, identities).label("scope_valid"))
    return statement, (plan.root_record_id, *order), (child,)


def _read_child_rows(session, request, registry, states, child_limit, reserved_bytes):
    """One protected metadata/payload window; native array binding has no parameter fanout."""
    with graph._read_transaction(session, request, states[0].current.build_id) as (_build, deadline):
        candidate_models, base_models = graph._build_storage_models(session, states[0].current.build_id)
        statement, keys, models = _children_statement(request, registry, states, candidate_models, base_models)
        graph._prepare_read(session, request, deadline)
        metadata = session.execute(
            statement.with_only_columns(*keys, graph._variable_bytes(*models), maintain_column_froms=True)
            .order_by(*keys)
            .limit(child_limit)
        ).all()
        # Leave room for the native logical-page arrays as well as the fetched models.
        admitted = graph._admitted_keys(metadata, (MAX_BATCH_BYTES - reserved_bytes) // 2)
        if not admitted:
            return ()
        child_ids = tuple(key[-1] for key in admitted)
        if len(child_ids) != len(set(child_ids)):
            raise CandidateRunnerError("child page identity or provenance differs")
        graph._prepare_read(session, request, deadline)
        child_rows = session.execute(
            statement.where(keys[-1] == any_(bindparam("child_ids", child_ids, type_=ARRAY(BigInteger)))).order_by(
                *keys
            )
        ).all()
        if _child_row_keys(registry, states[0].current.selection_kind, child_rows, deadline) != admitted:
            raise CandidateRunnerError("frozen build page changed during its read")
    return child_rows


def _child_row_keys(registry, kind, child_rows, deadline):
    collection_names_by_slot = {slot: name for name, slot in registry.child_collection_slots.items()}
    identities = []
    for root_id, child, valid in child_rows:
        graph._require_budget(deadline)
        if valid is not True or child is None or child.collection_slot not in collection_names_by_slot:
            raise CandidateRunnerError("child page identity or provenance differs")
        order = (
            (child.collection_slot, child.child_revision_id)
            if kind == "retained"
            else (collection_names_by_slot[child.collection_slot], child.child_key_sha256, child.child_revision_id)
        )
        identities.append((root_id, *order))
    return identities


def _logical_child_groups(rows, states):
    """Retained packs never span a family/collection, even inside one physical batch."""
    children_by_root_id = {}
    root_ids = {state.current.root_record_id for state in states}
    last_root = None
    for root_id, child, _valid in rows:
        if root_id not in root_ids or (last_root is not None and root_id < last_root):
            raise CandidateRunnerError("child page is not in family order")
        children = children_by_root_id.setdefault(root_id, [])
        if (
            states[0].current.selection_kind == "retained"
            and children
            and (child.collection_slot != children[0].collection_slot)
        ):
            break
        last_root = root_id
        children.append(child)
    return tuple(
        _ChildGroup(state, tuple(children_by_root_id.get(state.current.root_record_id, ())))
        for state in states
        if last_root is None or state.current.root_record_id <= last_root
    )


def _source_projections(request, registry, group):
    collection_names_by_slot = {slot: name for name, slot in registry.child_collection_slots.items()}
    fields = fields_by_collection(request.definition)
    projections = []
    for child in group.children:
        collection = collection_names_by_slot[child.collection_slot]
        values = payload_values(fields[collection], child.canonical_payload, label="build child payload")
        verify_stored_child(request, group.state.source.record, StoredCandidateChild(collection, child, values))
        if digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256):
            raise CandidateRunnerError("build child payload digest differs")
        projections.extend(
            graph._child_models(
                request,
                registry,
                group.state.current.build_id,
                group.state.family,
                group.state.source.values,
                child,
                values,
                collection,
            )
        )
    return projections


def _child_arguments(request, registry, groups):
    current = groups[0].state.current
    child_roots = tuple(group.state.current.root_record_id for group in groups for _ in group.children)
    if current.selection_kind == "retained":
        encoded_rows = [
            graph._retained_array_arguments(
                request,
                registry,
                group.state.source,
                group.state.current,
                group.state.family,
                group.children,
            )
            for group in groups
        ]
        return "copy_custom_import_build_retained_families_page", (
            *encoded_rows[0][:4],
            *_transpose(encoded_rows, 4, 9),
            ("bigint[]", child_roots),
            *_transpose(encoded_rows, 9, 13, arrays=True),
            ("integer[]", (len(child_roots),)),
        )
    projections = [
        projection_model for group in groups for projection_model in _source_projections(request, registry, group)
    ]
    graph._page_cost(request, projections, reserved_rows=2 * len(child_roots))
    scalars = [projection_model for projection_model in projections if isinstance(projection_model, Scalar)]
    contexts = [projection_model for projection_model in projections if isinstance(projection_model, Context)]
    return "append_custom_import_build_source_families_page", (
        ("bigint", current.build_id),
        ("bigint", request.execution_id),
        ("bigint", request.fence),
        ("bytea", lease_token_sha256(request.lease_token)),
        *(
            (kind, tuple(getattr(group.state.current, field) for group in groups))
            for kind, field in (
                ("bigint[]", "root_record_id"),
                ("bigint[]", "family_revision_id"),
                ("bigint[]", "attached_child_count"),
                ("smallint[]", "last_child_collection_slot"),
                ("bytea[]", "last_child_key_sha256"),
                ("bigint[]", "last_input_child_revision_id"),
            )
        ),
        ("bigint[]", child_roots),
        ("bigint[]", tuple(child.child_revision_id for group in groups for child in group.children)),
        *(
            (kind, tuple(getattr(projection_model, field) for projection_model in scalars))
            for kind, field in _SCALAR_COLUMNS
        ),
        ("bigint[]", tuple(projection_model.context_child_revision_id for projection_model in contexts)),
        ("smallint[]", tuple(projection_model.profile_slot for projection_model in contexts)),
        ("text[]", tuple(projection_model.canonical_context_key for projection_model in contexts)),
        ("integer[]", (len(child_roots),)),
    )


def _child_state(state, count, cursor, complete):
    current = state.current
    progress = Plan(
        build_id=current.build_id,
        root_record_id=current.root_record_id,
        selection_kind=current.selection_kind,
        base_family_revision_id=current.base_family_revision_id,
        family_revision_id=current.family_revision_id,
        attached_child_count=count,
        last_child_collection_slot=cursor[0],
        last_child_key_sha256=cursor[1],
        last_input_child_revision_id=cursor[2],
    )
    return _Started(state.source, progress, state.family, complete)


def _child_receipts(groups, receipts):
    if [receipt.root_record_id for receipt in receipts] != [group.state.current.root_record_id for group in groups]:
        raise CandidateRunnerError("child set returned a different family prefix")
    updates = []
    for group, receipt in zip(groups, receipts, strict=True):
        state, children = group.state, group.children
        current = state.current
        cursor = current.last_child_collection_slot, current.last_child_key_sha256, current.last_input_child_revision_id
        if children:
            last = children[-1]
            cursor = (
                last.collection_slot,
                None if current.selection_kind == "retained" else last.child_key_sha256,
                last.child_revision_id,
            )
        if (
            type(receipt.attached_child_count) is not int
            or receipt.attached_child_count != current.attached_child_count + len(children)
            or (receipt.last_child_collection_slot, receipt.last_child_key_sha256, receipt.last_input_child_revision_id)
            != cursor
            or type(receipt.complete) is not bool
            or receipt.complete != (receipt.attached_child_count == state.source.child_count)
            or (not children and not receipt.complete)
        ):
            raise CandidateRunnerError("child set returned inconsistent durable progress")
        # Do not mutate earlier receipts until the protected page commits.
        updates.append(_child_state(state, receipt.attached_child_count, cursor, receipt.complete))
    return updates


def _child_fanout(request, registry):
    scopes = graph.material._profile_scopes(request.definition, registry.child_collection_slots)
    return max(
        (
            4
            + sum(field.projection_slot is not None for field in request.definition.fields if field.collection == name)
            + sum(scope.collection_slot == slot for scope in scopes)
            for name, slot in registry.child_collection_slots.items()
        ),
        default=4,
    )


def _child_limit(request, registry):
    return max(1, min(256, request.page_row_limit // _child_fanout(request, registry)))


def _child_batch_arguments(pages):
    """Keep initial family tips, logical page spans and all typed values."""
    first = pages[0][1]
    is_retained = pages[0][0][0].state.current.selection_kind == "retained"
    stop = 9 if is_retained else 10
    initial, children = {}, {}
    for groups, arguments in pages:
        for position, group in enumerate(groups):
            root_id = group.state.current.root_record_id
            initial.setdefault(root_id, (group.state, tuple(argument[1][position] for argument in arguments[4:stop])))
            children.setdefault(root_id, []).extend(group.children)
    roots = sorted(initial)
    groups = tuple(_ChildGroup(initial[root_id][0], tuple(children[root_id])) for root_id in roots)
    arguments = (
        *first[:4],
        *((first[index][0], tuple(initial[root_id][1][index - 4] for root_id in roots)) for index in range(4, stop)),
        *(
            (first[index][0], tuple(value for _groups, page in pages for value in page[index][1]))
            for index in range(stop, len(first) - 1)
        ),
        ("integer[]", tuple(page[-1][1][0] for _groups, page in pages)),
    )
    return groups, arguments


def _child_argument_bytes(arguments):
    return sum(
        len(value.encode("utf-8"))
        if isinstance(value, str)
        else len(value)
        if isinstance(value, (bytes, bytearray, memoryview))
        else 32
        for _kind, values in arguments
        for value in (values if isinstance(values, tuple) else (values,))
    )


def _preview_child_state(group):
    state = group.state
    count = state.current.attached_child_count + len(group.children)
    if not group.children or count > state.source.child_count:
        raise CandidateRunnerError("child set returned inconsistent durable progress")
    last = group.children[-1]
    return _child_state(
        state,
        count,
        (
            last.collection_slot,
            None if state.current.selection_kind == "retained" else last.child_key_sha256,
            last.child_revision_id,
        ),
        count == state.source.child_count,
    )


def _retained_logical_rows(request, registry, rows):
    """Charge distinct native packs and root tips without lowering the default cap."""
    root_ids, pack_keys = set(), set()
    fanout = _child_fanout(request, registry) - 1
    for position, (root_id, child, _valid) in enumerate(rows, 1):
        root_ids.add(root_id)
        pack_keys.add((root_id, child.collection_slot))
        if position * fanout + len(pack_keys) + len(root_ids) > request.page_row_limit:
            return rows[: position - 1]
    return rows


def _logical_child_page(
    request, registry, spans_by_root_id, reserved_bytes_by_root_id, tips_by_root_id, child_rows, limit, offset
):
    """Reserve a logical root span without repeating immutable payload serialization."""
    first_root, first_child, _valid = child_rows[offset]
    span = spans_by_root_id[first_root]
    while len(span) > 1 and (
        sum(reserved_bytes_by_root_id[root_id] for root_id in span) + graph._model_bytes((first_child,))
        > request.page_byte_limit
    ):
        midpoint = len(span) // 2
        for split in (span[:midpoint], span[midpoint:]):
            spans_by_root_id.update((root_id, split) for root_id in split)
        span = spans_by_root_id[first_root]
    remaining_rows = child_rows[offset : offset + min(limit, request.page_row_limit - len(span))]
    remaining_rows = tuple(child_row for child_row in remaining_rows if child_row[0] <= span[-1])
    logical_keys = graph._admitted_keys(
        [(index, graph._model_bytes((child,))) for index, (_root_id, child, _valid) in enumerate(remaining_rows)],
        request.page_byte_limit - sum(reserved_bytes_by_root_id[root_id] for root_id in span),
    )
    active_states = tuple(
        tips_by_root_id[root_id]
        for root_id in span
        if not tips_by_root_id[root_id].complete
        and tips_by_root_id[root_id].current.selection_kind == tips_by_root_id[first_root].current.selection_kind
    )
    groups = _logical_child_groups(remaining_rows[: len(logical_keys)], active_states)
    if tips_by_root_id[first_root].current.selection_kind == "retained":
        remaining_rows = _retained_logical_rows(
            request, registry, remaining_rows[: sum(len(group.children) for group in groups)]
        )
        return _logical_child_groups(remaining_rows, active_states)
    return groups


def _child_reservations(request, inputs, states):
    tips_by_root_id = {state.current.root_record_id: state for state in states}
    reserved_bytes_by_root_id = {
        item.plan.root_record_id: graph._model_bytes(
            (
                item.plan,
                item.root,
                item.record,
                tips_by_root_id[item.plan.root_record_id].current,
                tips_by_root_id[item.plan.root_record_id].family,
            )
        )
        for item in inputs
    }
    limit = max(1, request.page_row_limit // 2)
    spans_by_root_id = {}
    for offset in range(0, len(inputs), limit):
        root_ids = tuple(item.plan.root_record_id for item in inputs[offset : offset + limit])
        spans_by_root_id.update((root_id, root_ids) for root_id in root_ids)
    return tips_by_root_id, reserved_bytes_by_root_id, spans_by_root_id


def _advance_child_tips(groups, tips_by_root_id, reserved_bytes_by_root_id):
    for group in groups:
        root_id = group.state.current.root_record_id
        next_state = _preview_child_state(group)
        reserved_bytes_by_root_id[root_id] += len(next_state.current.last_child_key_sha256 or b"") - len(
            group.state.current.last_child_key_sha256 or b""
        )
        tips_by_root_id[root_id] = next_state


def _read_child_batch(session, request, registry, inputs, states, kind, row_limit, child_limit):
    """Preview bounded logical pages; only the returned native receipt is durable."""
    pages, child_count = [], 0
    tips_by_root_id, reserved_bytes_by_root_id, spans_by_root_id = _child_reservations(request, inputs, states)
    held_bytes = sum(reserved_bytes_by_root_id.values())
    child_rows = _read_child_rows(
        session,
        request,
        registry,
        tuple(state for state in states if not state.complete and state.current.selection_kind == kind),
        min(child_limit, (MAX_BATCH_ROWS - len(states)) // _child_fanout(request, registry)),
        held_bytes,
    )
    held_bytes += graph._model_bytes(tuple(child for _root_id, child, _valid in child_rows))
    while child_count < len(child_rows):
        groups = _logical_child_page(
            request,
            registry,
            spans_by_root_id,
            reserved_bytes_by_root_id,
            tips_by_root_id,
            child_rows,
            row_limit,
            child_count,
        )
        name, arguments = _child_arguments(request, registry, groups)
        count = sum(len(group.children) for group in groups)
        page_bytes = _child_argument_bytes(arguments) + sum(
            len(group.children[-1].child_key_sha256 or b"") - len(group.state.current.last_child_key_sha256 or b"")
            for group in groups
            if group.children and kind == "source"
        )
        if not count:
            raise CandidateRunnerError("child set returned inconsistent durable progress")
        if held_bytes + page_bytes > MAX_BATCH_BYTES:
            if not pages:
                raise CandidateRunnerError("record projection fanout exceeds the admitted byte page")
            break
        graph._require_budget(session.info["custom_import_build_read_deadline"])
        pages.append((groups, arguments))
        held_bytes += page_bytes
        child_count += count
        _advance_child_tips(groups, tips_by_root_id, reserved_bytes_by_root_id)
    if not pages:
        raise CandidateRunnerError("record projection fanout exceeds the admitted row page")
    groups, arguments = _child_batch_arguments(pages)
    return groups, name, arguments


async def _consume_child_page(session_factory, request, registry, inputs, states):
    """Consume one bounded physical root window from its committed durable tips."""
    row_limit = _child_limit(request, registry)
    child_limit = MAX_BATCH_ROWS // _child_fanout(request, registry)
    while any(not state.complete for state in states):
        for kind in ("source", "retained"):
            active_states = tuple(
                state for state in states if not state.complete and state.current.selection_kind == kind
            )
            if not active_states:
                continue
            arguments = None
            try:
                async with graph._session(session_factory) as session:
                    groups, name, arguments = await session.run_sync(
                        lambda sync: _read_child_batch(
                            sync, request, registry, inputs, states, kind, row_limit, child_limit
                        )
                    )
                async with graph._page_session(session_factory, request, inputs[0].plan.build_id) as (session, _build):
                    receipts = (await graph._source_call(session, name, arguments)).all()
                    updates = _child_receipts(groups, receipts)
            except (CandidateRunnerError, DBAPIError) as error:
                count = sum(arguments[-1][1]) if arguments is not None else row_limit
                if count < 2 or not graph._is_child_page_bound_error(error):
                    raise
                row_limit = max(1, min(row_limit, max(arguments[-1][1]) if arguments is not None else row_limit) // 2)
                child_limit = min(child_limit, max(1, count // 2))
                continue
            by_root = {state.current.root_record_id: state for state in updates}
            states[:] = [by_root.get(state.current.root_record_id, state) for state in states]
            await graph._heartbeat(session_factory, request)


async def consume_family_page(session_factory, request, registry, inputs):
    """Coalesce roots and children without widening their logical page policies."""
    if not inputs:
        return
    roots = [prepared_family.plan.root_record_id for prepared_family in inputs]
    if (
        roots != sorted(set(roots))
        or len(inputs) > MAX_BATCH_ROWS
        or len({prepared_family.plan.build_id for prepared_family in inputs}) != 1
        or any(prepared_family.plan.selection_kind not in {"source", "retained"} for prepared_family in inputs)
    ):
        raise CandidateRunnerError("family page identity or read budget differs")
    states = await _start_roots(session_factory, request, registry, inputs)
    await _consume_child_page(session_factory, request, registry, inputs, states)
