# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare a bounded page of immutable families without per-family reads.

Imported by the graph caller after build_graph initialization. Write finalizers
remain responsible for fresh authority, exact retries and durable progress.
"""

from __future__ import annotations

from collections import deque
from dataclasses import replace
from itertools import count, groupby

from sqlalchemy import BigInteger, and_, any_, bindparam, case, or_, select, tuple_
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import aliased

from db.models.custom_import import (
    CustomImportBuildAttempt as Build,
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
    CustomImportChildCollection as Collection,
)
from db.models.custom_import import (
    CustomImportChildRevision as Child,
)
from db.models.custom_import import (
    CustomImportEntityBinding as Entity,
)
from db.models.custom_import import (
    CustomImportFamilyRevision as Family,
)
from db.models.custom_import import (
    CustomImportGenerationFamily as Member,
)
from db.models.custom_import import (
    CustomImportGenerationSeal as Seal,
)
from db.models.custom_import import (
    CustomImportPack as Pack,
)
from db.models.custom_import import (
    CustomImportRootRecord as Record,
)
from db.models.custom_import import (
    CustomImportRootRevision as Root,
)
from db.models.custom_import import (
    CustomImportSourceStream as Stream,
)
from process.custom_import import build_graph as graph
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_codec import (
    digest_text,
    new_family_hash_ordered,
    payload_values,
    record_payload,
    root_key_contract_hash,
    root_key_document,
    root_key_hash,
)
from process.custom_import.runner_graph import stored_root_values
from process.custom_import.runner_types import CandidateRunnerError


def _build_scope(request):
    return and_(
        Build.dataset_id == request.dataset_id,
        Build.definition_revision_id == request.definition_revision_id,
        Build.schema_revision_id == request.schema_revision_id,
        Build.execution_id == request.execution_id,
        Build.producing_fence == request.fence,
        Build.producing_token_sha256 == lease_token_sha256(request.lease_token),
        Build.phase == "graph",
        Build.plan_complete_at.is_not(None),
    )


def _source_joins(statement, revision, record_kind, pack, occurrence):
    """A pack/stream mismatch remains visible as a failed outer join."""

    statement = (
        statement.outerjoin(
            pack,
            and_(
                pack.pack_id == revision.pack_id,
                pack.pack_id == occurrence.pack_id,
                pack.dataset_id == Build.dataset_id,
                pack.definition_revision_id == Build.definition_revision_id,
                pack.schema_revision_id == Build.schema_revision_id,
                pack.execution_id == Build.execution_id,
                pack.capture_bundle_id == Build.capture_bundle_id,
                pack.producing_fence == Build.producing_fence,
                pack.producing_token_sha256 == Build.producing_token_sha256,
                pack.stream_slot == occurrence.stream_slot,
            ),
        )
        .outerjoin(
            Stream,
            and_(
                Stream.definition_revision_id == Build.definition_revision_id,
                Stream.dataset_id == Build.dataset_id,
                Stream.schema_revision_id == Build.schema_revision_id,
                Stream.stream_slot == occurrence.stream_slot,
                Stream.record_kind == record_kind,
                Stream.collection_slot.is_(None)
                if record_kind == "root"
                else Stream.collection_slot == occurrence.collection_slot,
            ),
        )
        .outerjoin(
            BuildStream,
            and_(
                BuildStream.build_id == Build.build_id,
                BuildStream.stream_slot == occurrence.stream_slot,
                BuildStream.replay_verified_at.is_not(None),
                pack.pack_ordinal < BuildStream.next_pack_ordinal,
            ),
        )
    )
    valid = and_(pack.pack_id.is_not(None), Stream.stream_slot.is_not(None), BuildStream.build_id.is_not(None))
    return statement, valid


def _selected_source_join(statement, plan, occurrence):
    """Keep a missing or mismatched source occurrence visible to validation."""

    return statement.outerjoin(
        occurrence,
        and_(
            plan.selection_kind == "source",
            occurrence.occurrence_id == plan.source_root_occurrence_id,
            occurrence.build_id == Build.build_id,
            occurrence.origin == "source",
            occurrence.record_kind == "root",
            occurrence.collection_slot == 0,
            occurrence.root_record_id == plan.root_record_id,
            occurrence.resolved_rejection_id.is_(None),
        ),
    )


def _retained_selection_joins(statement, plan, base, member):
    """Bind retained roots to membership in the exact sealed base generation."""

    return (
        statement.outerjoin(
            member,
            and_(
                plan.selection_kind == "retained",
                member.generation_id == Build.base_generation_id,
                member.dataset_id == Build.dataset_id,
                member.schema_revision_id == Build.schema_revision_id,
                member.root_record_id == plan.root_record_id,
                member.family_revision_id == plan.base_family_revision_id,
            ),
        )
        .outerjoin(
            Seal,
            and_(
                Seal.generation_id == member.generation_id,
                Seal.dataset_id == member.dataset_id,
                Seal.definition_revision_id == member.definition_revision_id,
                Seal.schema_revision_id == member.schema_revision_id,
            ),
        )
        .outerjoin(
            base,
            and_(
                base.family_revision_id == member.family_revision_id,
                base.dataset_id == member.dataset_id,
                base.schema_revision_id == member.schema_revision_id,
                base.root_record_id == member.root_record_id,
            ),
        )
    )


def _root_identity_joins(statement, candidate_models, base, retained_root):
    """Load the selected revision and its immutable logical-key record."""

    plan, occurrence, source_root = candidate_models[Plan], candidate_models[Occurrence], candidate_models[Root]
    statement = statement.outerjoin(
        source_root,
        and_(
            plan.selection_kind == "source",
            source_root.root_revision_id == occurrence.root_revision_id,
            source_root.dataset_id == Build.dataset_id,
            source_root.schema_revision_id == Build.schema_revision_id,
            source_root.root_record_id == plan.root_record_id,
        ),
    ).outerjoin(
        retained_root,
        and_(
            plan.selection_kind == "retained",
            retained_root.root_revision_id == base.root_revision_id,
            retained_root.dataset_id == Build.dataset_id,
            retained_root.schema_revision_id == Build.schema_revision_id,
            retained_root.root_record_id == plan.root_record_id,
        ),
    )
    return statement.outerjoin(
        Record,
        and_(
            Record.root_record_id == plan.root_record_id,
            Record.dataset_id == Build.dataset_id,
            Record.logical_key_sha256 == plan.root_key_sha256,
        ),
    )


def _started_family_joins(statement, plan, base, current):
    """Include an already-started family only under this immutable producer."""

    return statement.outerjoin(
        current,
        and_(
            current.family_revision_id == plan.family_revision_id,
            current.dataset_id == Build.dataset_id,
            current.schema_revision_id == Build.schema_revision_id,
            current.root_record_id == plan.root_record_id,
            current.producing_execution_id == Build.execution_id,
            current.producing_fence == Build.producing_fence,
            current.producing_token_sha256 == Build.producing_token_sha256,
        ),
    ).outerjoin(
        Entity,
        and_(
            Entity.entity_binding_id
            == case((plan.selection_kind == "retained", base.entity_binding_id), else_=current.entity_binding_id),
            Entity.dataset_id == Build.dataset_id,
        ),
    )


def _pending_scope(request, candidate_models, base, current, retained_root, source_scope):
    """Reject missing selected joins and mismatched retry identity."""

    plan, occurrence, source_root = candidate_models[Plan], candidate_models[Occurrence], candidate_models[Root]
    return and_(
        _build_scope(request),
        Record.root_record_id.is_not(None),
        or_(
            and_(
                plan.selection_kind == "source",
                occurrence.occurrence_id.is_not(None),
                source_root.root_revision_id.is_not(None),
                source_root.definition_revision_id == Build.definition_revision_id,
                source_root.source_ordinal == occurrence.source_ordinal,
                source_scope,
            ),
            and_(
                plan.selection_kind == "retained",
                retained_root.root_revision_id.is_not(None),
                Seal.generation_id.is_not(None),
                base.family_revision_id.is_not(None),
                Entity.entity_binding_id.is_not(None),
            ),
        ),
        or_(
            plan.family_revision_id.is_(None),
            and_(
                current.family_revision_id.is_not(None),
                Entity.entity_binding_id.is_not(None),
                or_(plan.selection_kind != "source", current.root_revision_id == source_root.root_revision_id),
                or_(plan.selection_kind != "retained", current.entity_binding_id == Entity.entity_binding_id),
            ),
        ),
    )


def _pending_statement(request, build_id, candidate_models, base_models):
    """One joined pending-root page, including all selected provenance checks."""

    plan, occurrence, source_root = candidate_models[Plan], candidate_models[Occurrence], candidate_models[Root]
    base = aliased(Family if base_models is None else base_models[Family], name="base_family")
    retained_root = aliased(Root if base_models is None else base_models[Root], name="retained_root")
    member = Member if base_models is None else base_models[Member]
    current = aliased(candidate_models[Family], name="current_family")
    models = (plan, source_root, retained_root, Record, base, Entity, current)
    statement = select(*models).select_from(plan).join(Build, Build.build_id == plan.build_id)
    statement = _selected_source_join(statement, plan, occurrence)
    statement = _retained_selection_joins(statement, plan, base, member)
    statement = _root_identity_joins(statement, candidate_models, base, retained_root)
    statement = _started_family_joins(statement, plan, base, current)
    statement = statement.where(plan.build_id == build_id, plan.complete_at.is_(None))
    statement, source_scope = _source_joins(statement, source_root, "root", candidate_models[Pack], occurrence)
    scope = _pending_scope(request, candidate_models, base, current, retained_root, source_scope)
    return statement.add_columns(scope.label("scope_valid")), (plan.root_record_id,), models


def _child_identity_joins(statement, child, occurrence):
    """Expose bad revision, parent-key or collection joins instead of hiding them."""

    return statement.outerjoin(
        child,
        and_(
            child.child_revision_id == occurrence.child_revision_id,
            child.dataset_id == Build.dataset_id,
            child.definition_revision_id == Build.definition_revision_id,
            child.schema_revision_id == Build.schema_revision_id,
            child.root_record_id == occurrence.root_record_id,
            child.collection_slot == occurrence.collection_slot,
            child.pack_id == occurrence.pack_id,
            child.source_ordinal == occurrence.source_ordinal,
            child.child_key_sha256 == occurrence.child_key_sha256,
        ),
    ).outerjoin(
        Collection,
        and_(
            Collection.schema_revision_id == Build.schema_revision_id,
            Collection.dataset_id == Build.dataset_id,
            Collection.collection_slot == occurrence.collection_slot,
        ),
    )


def _collapse_identical_children(statement, definition, registry, occurrence):
    """Preserve the existing cross-pack last-occurrence policy exactly."""

    # Reuse the accepted cross-pack last-occurrence policy, including its
    # deliberate lack of a resolved_rejection_id filter on the later witness.
    collapse_slots = tuple(
        registry.stream_slots[stream.stream_id]
        for stream in definition.source_streams
        if stream.duplicate_policy == "collapse_identical"
    )
    if collapse_slots:
        later = aliased(occurrence, name="later_occurrence")
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
    return statement


def _children_statement(request, registry, build_id, root_ids, candidate_models):
    """One keyset stream, ordered exactly as the existing family digest codec."""

    plan, occurrence, child = candidate_models[Plan], candidate_models[Occurrence], candidate_models[Child]
    statement = (
        select(occurrence.root_record_id, Collection.collection_name, child)
        .select_from(occurrence)
        .join(Build, Build.build_id == occurrence.build_id)
        .outerjoin(
            plan,
            and_(
                plan.build_id == Build.build_id,
                plan.root_record_id == occurrence.root_record_id,
                plan.selection_kind == "source",
            ),
        )
    )
    statement = _child_identity_joins(statement, child, occurrence).where(
        occurrence.build_id == build_id,
        occurrence.origin == "source",
        occurrence.root_record_id.in_(root_ids),
        occurrence.child_revision_id.is_not(None),
        occurrence.resolved_rejection_id.is_(None),
    )
    statement = _collapse_identical_children(statement, request.definition, registry, occurrence)
    statement, source_scope = _source_joins(statement, child, "child", candidate_models[Pack], occurrence)
    valid = and_(
        _build_scope(request),
        plan.root_record_id.is_not(None),
        occurrence.record_kind == "child",
        child.child_revision_id.is_not(None),
        Collection.collection_name.is_not(None),
        source_scope,
    )
    # Keys use occurrence columns, so a broken revision join cannot disappear
    # through a NULL tuple IN predicate before the payload scope check.
    collection_name = case(
        {slot: name for name, slot in registry.child_collection_slots.items()},
        value=occurrence.collection_slot,
        else_="",
    ).collate("C")
    keys = (occurrence.root_record_id, collection_name, occurrence.child_key_sha256, occurrence.child_revision_id)
    return statement.add_columns(valid.label("scope_valid")), keys, (child, Collection)


def _root_input(request, row):
    plan, root, record, base, binding, current, valid = row
    if valid is not True:
        raise CandidateRunnerError("pending family identity or provenance does not match its frozen build")
    values = payload_values(request.definition.root_fields, root.canonical_payload, label="build root payload")
    if (
        record_payload(request.definition.root_fields, values) != root.canonical_payload
        or digest_text("root-payload", root.canonical_payload) != bytes(root.payload_sha256)
        or root_key_contract_hash(request.definition) != bytes(record.key_contract_sha256)
        or root_key_document(request.definition, values) != record.canonical_logical_key
        or root_key_hash(request.definition, values) != bytes(record.logical_key_sha256)
    ):
        raise CandidateRunnerError("build root payload or identity is not canonical")
    if binding is not None:
        stored_root_values(request, record, root, binding)
    if plan.selection_kind == "retained":
        return graph._FamilyInput(
            plan, root, record, values, bytes(base.family_sha256), base.child_count, binding.entity_binding_id
        )
    return graph._FamilyInput(plan, root, record, values, b"", 0, None)


def _source_child_stream(session, request, registry, build_id, root_rows, *, physical=False):
    """Stream bounded child pages while reserving all selected root bytes."""

    root_ids = tuple(root[0].root_record_id for root in root_rows if root[0].selection_kind == "source")
    if not root_ids:
        return
    if physical:
        yield from _physical_family_rows(
            session,
            request,
            build_id,
            lambda candidate_models, _base_models: _children_statement(
                request, registry, build_id, root_ids, candidate_models
            ),
            reserve_bytes=sum(
                graph._model_bytes(model for model in root[:-1] if model is not None) for root in root_rows
            ),
            row_limit=MAX_BATCH_ROWS // 2,
        )
        return
    if request.page_row_limit <= len(root_rows):
        raise CandidateRunnerError("family preparation requires row space for a root and child")
    yield from graph._read_snapshot_rows(
        session,
        request,
        build_id,
        lambda candidate_models, _base_models: _children_statement(
            request, registry, build_id, root_ids, candidate_models
        ),
        bounds=graph._ReadPage(
            reserve_bytes=sum(
                graph._model_bytes(model for model in root[:-1] if model is not None) for root in root_rows
            ),
            row_limit=request.page_row_limit - len(root_rows),
        ),
    )


def _validated_child_documents(family_input, registry, collection, child_rows, child_counter, *, request=None):
    """Keep canonical child and parent validation inside the ordered stream."""

    root_bytes = (
        graph._model_bytes((family_input.plan, family_input.root, family_input.record))
        + len(collection.encode("utf-8"))
        if request is not None
        else 0
    )
    for _root_id, actual_collection, child, valid in child_rows:
        if valid is not True or child is None or actual_collection not in registry.child_collection_slots:
            raise CandidateRunnerError("source child identity or provenance does not match its frozen build")
        if child.collection_slot != registry.child_collection_slots[collection]:
            raise CandidateRunnerError("source child collection differs from the validated registry")
        if request is not None and root_bytes + graph._model_bytes((child,)) > request.page_byte_limit:
            raise CandidateRunnerError("one build record exceeds the admitted byte page")
        if (
            child.canonical_parent_key != family_input.record.canonical_logical_key
            or bytes(child.parent_key_sha256) != bytes(family_input.record.logical_key_sha256)
            or digest_text("child-payload", child.canonical_payload) != bytes(child.payload_sha256)
        ):
            raise CandidateRunnerError("source child payload or parent digest does not match")
        next(child_counter)
        yield bytes(child.child_key_sha256), child.canonical_child_key, child.canonical_payload


def _source_input_with_digest(request, registry, family_input, child_groups, pending_groups, *, physical=False):
    """Consume only this family's canonical collection groups, without buffering."""

    child_counter = count()

    def documents(collection):
        """Consume a matching group; leave a later family or collection pending."""

        if not pending_groups:
            group = next(child_groups, None)
            if group is not None:
                pending_groups.append(group)
        if not pending_groups:
            return
        position, child_rows = pending_groups[0]
        expected = family_input.plan.root_record_id, collection
        if position < expected:
            raise CandidateRunnerError("source child stream is not in canonical family order")
        if position != expected:
            return
        pending_groups.popleft()
        yield from _validated_child_documents(
            family_input, registry, collection, child_rows, child_counter, request=request if physical else None
        )

    digest = new_family_hash_ordered(
        request.definition,
        family_input.values,
        {collection.name: documents(collection.name) for collection in request.definition.child_collections},
    )
    return replace(family_input, family_sha256=digest, child_count=next(child_counter))


def _digest_page(session, request, registry, build_id, root_rows, *, physical=False):
    """Reduce all selected SOURCE roots and verify any immutable retry families."""

    family_inputs = []
    for root_row in root_rows:
        graph._require_budget(session.info["custom_import_build_read_deadline"])
        family_inputs.append(_root_input(request, root_row))
    child_stream = _source_child_stream(session, request, registry, build_id, root_rows, physical=physical)
    child_groups = groupby(child_stream, key=lambda child: (child[0], child[1]))
    pending_groups = deque()
    try:
        for index, family_input in enumerate(family_inputs):
            graph._require_budget(session.info["custom_import_build_read_deadline"])
            if family_input.plan.selection_kind == "source":
                family_inputs[index] = family_input = _source_input_with_digest(
                    request, registry, family_input, child_groups, pending_groups, physical=physical
                )
            current_family = root_rows[index][5]
            if current_family is not None and (
                bytes(current_family.family_sha256) != family_input.family_sha256
                or current_family.child_count != family_input.child_count
                or family_input.plan.attached_child_count > family_input.child_count
            ):
                raise CandidateRunnerError("pending family retry differs from its immutable family")
            graph._require_budget(session.info["custom_import_build_read_deadline"])
        if pending_groups or next(child_groups, None) is not None:
            raise CandidateRunnerError("source child stream contains an unexpected family or collection")
        return tuple(family_inputs)
    finally:
        child_stream.close()


def _selected_root_row(row):
    """Preserve the reducer row shape after the two storage-specific root joins."""

    plan, source_root, retained_root, *identity_fields = row
    return plan, source_root if plan.selection_kind == "source" else retained_root, *identity_fields


def _physical_family_rows(session, request, build_id, query, *, row_limit, reserve_bytes=0, after=None, single=False):
    """Use the existing read authority with a bounded native-ID array payload query."""
    while True:
        with graph._read_transaction(session, request, build_id) as (_build, deadline):
            statement, keys, models = query(*graph._build_storage_models(session, build_id))
            if after is not None:
                statement = statement.where(tuple_(*keys) > tuple(after))
            graph._prepare_read(session, request, deadline)
            metadata = session.execute(
                statement.with_only_columns(*keys, graph._variable_bytes(*models), maintain_column_froms=True)
                .order_by(*keys)
                .limit(row_limit)
            ).all()
            admitted = graph._admitted_keys(metadata, MAX_BATCH_BYTES - reserve_bytes)
            if not admitted:
                return
            identities = tuple(key[-1] for key in admitted)
            if len(identities) != len(set(identities)):
                raise CandidateRunnerError("frozen family page contains duplicate identities")
            graph._prepare_read(session, request, deadline)
            payload_rows = session.execute(
                statement.add_columns(*keys)
                .where(keys[-1] == any_(bindparam("physical_ids", identities, type_=ARRAY(BigInteger))))
                .order_by(*keys)
            ).all()
            if [tuple(payload_row[-len(keys) :]) for payload_row in payload_rows] != admitted:
                raise CandidateRunnerError("frozen family page changed during its read")
            after = admitted[-1]
        for payload_row in payload_rows:
            graph._require_budget(session.info["custom_import_build_read_deadline"])
            yield tuple(payload_row[: -len(keys)])
            graph._require_budget(session.info["custom_import_build_read_deadline"])
        if single or len(admitted) == len(metadata) < row_limit:
            return


def prepare_family_page(session, request, registry, build_id, after_root_id, *, physical=False):
    """Return a strict pending-root prefix; completion stays in durable BF root_rows.

    One joined root page and one globally bounded SOURCE child stream replace
    per-root/per-collection reads. A large family spans child pages, never one
    in-memory family list. A root/child record pair must fit the byte cap.

    As in _read_rows, the row limit counts joined result tuples, not ORM models
    within a tuple. The byte reservation includes every selected model, even
    repeated hashes from the base/current family. Write fanout is a separate
    finalizer budget and cannot be inferred from this read-page tuple count.
    """

    if physical:
        from process.custom_import.build_graph_sets import _root_fanout

        stream = _physical_family_rows(
            session,
            request,
            build_id,
            lambda candidate_models, base_models: _pending_statement(request, build_id, candidate_models, base_models),
            after=(after_root_id,),
            row_limit=MAX_BATCH_ROWS // _root_fanout(request, registry),
            reserve_bytes=MAX_BATCH_BYTES // 2,
            single=True,
        )
    else:
        stream = graph._read_snapshot_rows(
            session,
            request,
            build_id,
            lambda candidate_models, base_models: _pending_statement(request, build_id, candidate_models, base_models),
            after=(after_root_id,),
            bounds=graph._ReadPage(row_limit=max(1, request.page_row_limit // 2), single_page=True),
        )
    try:
        root_rows = [_selected_root_row(root_row) for root_row in stream]
    finally:
        stream.close()
    while root_rows:
        try:
            return _digest_page(session, request, registry, build_id, root_rows, physical=physical)
        except CandidateRunnerError as exc:
            # Another root's reservation must not reject a valid oversized
            # family. Retain a strict prefix and release the suffix, which is
            # read again after that prefix commits. No durable cursor moves.
            if physical or str(exc) != "one build record exceeds the admitted byte page" or len(root_rows) == 1:
                raise
            del root_rows[max(1, len(root_rows) // 2) :]
    return ()
