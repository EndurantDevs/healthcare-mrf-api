# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Feed bounded admitted SOURCE pages into compact blocks."""

from collections import deque
from functools import partial
from itertools import count, groupby
from types import SimpleNamespace

from sqlalchemy import and_, func, select

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
    CustomImportPack as Pack,
)
from db.models.custom_import import (
    CustomImportRootRecord as RootRecord,
)
from db.models.custom_import import (
    CustomImportRootRevision as Root,
)
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import import graph_payloads, materialization, runner_codec
from process.custom_import.bulk_page_codec import MAX_BATCH_BYTES, MAX_BATCH_ROWS
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_codec import FamilyRecord
from process.custom_import.family_block_scalar_layout import pack_query_scalars, query_scalar_fields
from process.custom_import.family_block_write import (
    CONTEXT_COPY_COLUMNS,
    AllocatedBlockFamily,
    AllocatedChildCollection,
    pack_family_rows,
)
from process.custom_import.storage_layout import snapshot_models


def _root_statement(request, build_id, candidate_models, *, has_compact_base=False):
    plan, occurrence, source_root = candidate_models[Plan], candidate_models[Occurrence], candidate_models[Root]
    statement = select(plan, source_root, RootRecord).select_from(plan).join(Build, Build.build_id == plan.build_id)
    statement = prepare._selected_source_join(statement, plan, occurrence)
    statement = prepare._source_revision_join(statement, candidate_models)
    statement, source_scope = prepare._source_joins(statement, source_root, "root", candidate_models[Pack], occurrence)
    valid = and_(
        prepare._build_scope(request),
        prepare._valid_source_root(candidate_models, source_scope),
        plan.family_revision_id.is_(None),
        RootRecord.root_record_id.is_not(None),
    )
    statement = statement.where(plan.build_id == build_id, plan.complete_at.is_(None))
    if has_compact_base:
        statement = statement.where(plan.selection_kind == "source")
    return statement.add_columns(valid.label("scope_valid")), (plan.root_record_id,), (plan, source_root, RootRecord)


def read_block_roots(session, request, build_id, binding, *, after_root_id, has_compact_base=False):
    """Read the next exact SOURCE root prefix from the already scoped candidate."""
    source_rows = prepare._physical_family_rows(
        session,
        request,
        build_id,
        lambda candidate_models, _base_models: _root_statement(
            request, build_id, candidate_models, has_compact_base=has_compact_base
        ),
        after=(after_root_id,),
        bounds=graph._ReadPage(row_limit=MAX_BATCH_ROWS // 2, reserve_bytes=MAX_BATCH_BYTES // 2, single_page=True),
        candidate_models=snapshot_models(binding.family_id, schema_name=binding.tables_by_role["family"].schema),
    )
    try:
        return tuple(
            prepare._root_input(request, (plan, root, identity, None, None, None, valid))
            for plan, root, identity, valid in source_rows
        )
    finally:
        source_rows.close()


def _child_census(session, request, build_id, statement, keys, registry):
    """Fetch only exact counts and scope validity before streaming bounded payloads."""
    selected = statement.with_only_columns(
        keys[0].label("root_id"),
        keys[1].label("collection"),
        statement.selected_columns.scope_valid,
        maintain_column_froms=True,
    ).subquery()
    census = select(
        selected.c.root_id,
        selected.c.collection,
        func.count(),
        func.bool_and(func.coalesce(selected.c.scope_valid, False)),
    ).group_by(selected.c.root_id, selected.c.collection)
    counts_by_collection = {}
    with graph._read_transaction(session, request, build_id) as (_build, deadline):
        graph._prepare_read(session, request, deadline)
        for root_id, collection, population, valid in session.execute(census).all():
            if valid is not True or collection not in registry.child_collection_slots:
                raise FamilyBlockUnavailable("compact SOURCE child census differs from the frozen plan")
            counts_by_collection[root_id, collection] = population
    return counts_by_collection


def _block_record(revision, revision_id, values, fields):
    scalars_by_field = {
        field.field_id: materialization._typed_scalar(field, values[field.field_id])
        for field in fields
        if field.projection_slot is not None and field.field_id in values
    }
    return FamilyRecord(revision_id, revision.pack_id, revision.source_ordinal, values, scalars_by_field)


def _child_values(definition, collection, fields, child, proof):
    if proof is not None and proof[:5] == (
        child.canonical_parent_key,
        bytes(child.parent_key_sha256),
        child.canonical_child_key,
        bytes(child.child_key_sha256),
        bytes(child.payload_sha256),
    ):
        values, _scalars, _size = graph_payloads.projection_values(fields, proof, child.child_revision_id)
        return values
    values = runner_codec.payload_values(fields, child.canonical_payload, label="compact SOURCE child")
    key = runner_codec.child_key_document(definition, collection, values)
    if (
        runner_codec.record_payload(fields, values) != child.canonical_payload
        or key != child.canonical_child_key
        or runner_codec.digest_text("child-key", key) != bytes(child.child_key_sha256)
    ):
        raise FamilyBlockUnavailable("compact SOURCE child payload or key is not canonical")
    return values


def _collection_records(request, registry, source, collection, child_rows, observe):
    definition = request.definition
    fields = runner_codec.fields_by_collection(definition)[collection]
    selected = prepare._validated_children(source, registry, collection, child_rows, count(), request=request)
    for child, proof in graph_payloads.decode_records(
        definition,
        collection,
        ((row, row.canonical_payload) for row in selected),
    ):
        values = _child_values(definition, collection, fields, child, proof)
        record = _block_record(child, child.child_revision_id, values, fields)
        if observe is not None:
            observe(child, record)
        yield record


def _stage_contexts(stage, request, registry, source_family, family, *, child=None, child_values=None):
    candidate = graph._candidate(request.definition, family, source_family.values, child, child_values)
    for context in graph._context_values(request, registry, source_family.plan.build_id, candidate):
        stage("candidate_context", tuple(context[name] for name in CONTEXT_COPY_COLUMNS))


def _stage_child(stage, request, registry, source_family, family, query_fields, child, record):
    stage(
        "query_input",
        (
            child.child_revision_id,
            family.family_revision_id,
            child.collection_slot,
            bytes(child.child_key_sha256),
            child.canonical_child_key,
            *pack_query_scalars(query_fields, record.scalars),
        ),
    )
    values_by_field = record.values_by_field
    if values_by_field is None:
        fields = runner_codec.fields_by_collection(request.definition)[request.definition.query.child_collection]
        values_by_field = runner_codec.payload_values(
            fields, record.sparse.payload_text, label="retained compact payload"
        )
    _stage_contexts(stage, request, registry, source_family, family, child=child, child_values=values_by_field)


def _pack_source_roots(request, registry, layout, allocated, child_rows, counts, limits, stage_output):
    write, account = (None, None) if stage_output is None else stage_output
    child_groups = groupby(child_rows, key=lambda row: (row[0], row[1]))
    pending_groups = deque()
    query_collection = layout.definition.query.child_collection
    query_fields = query_scalar_fields(layout.definition, query_collection) if query_collection is not None else ()
    for source_family, (family_id, binding_id) in allocated:
        root_id = source_family.plan.root_record_id
        root = source_family.root
        root_record = _block_record(root, root.root_revision_id, source_family.values, request.definition.root_fields)
        stage_child = None
        if write is not None:
            # The canonical context key uses typed dimensions, independently of the family digest.
            identity = SimpleNamespace(
                family_revision_id=family_id, entity_binding_id=binding_id, family_sha256=bytes(32)
            )
            _stage_contexts(write, request, registry, source_family, identity)
            stage_child = partial(_stage_child, write, request, registry, source_family, identity, query_fields)

        def records(collection, *, source_family=source_family, root_id=root_id, stage_child=stage_child):
            """Consume this collection while leaving the next group's rows untouched."""
            selected = prepare._source_group(child_groups, pending_groups, root_id, collection)
            observer = stage_child if collection == query_collection else None
            yield from _collection_records(request, registry, source_family, collection, selected, observer)

        children_by_name = {
            name: AllocatedChildCollection(counts.get((root_id, name), 0), records(name))
            for name in layout.collection_slots
        }
        family = AllocatedBlockFamily(family_id, root_id, binding_id, None, root_record, children_by_name)
        yield from pack_family_rows(layout, family, limits=limits, account=account)
    if pending_groups or next(child_groups, None) is not None:
        raise FamilyBlockUnavailable("compact SOURCE stream contains an unexpected family or collection")


def pack_source_page(session, request, registry, binding, roots, identities_by_root, *, limits, stage_output=None):
    """Emit COPY rows from exact SOURCE identities, with each child payload read once."""
    if not roots:
        return
    build_id = roots[0].plan.build_id
    root_ids = tuple(source_family.plan.root_record_id for source_family in roots)
    if set(root_ids) != identities_by_root.keys() or any(root.plan.build_id != build_id for root in roots):
        raise FamilyBlockUnavailable("compact SOURCE page allocation differs from its roots")
    allocated = zip(roots, (identities_by_root[root_id] for root_id in root_ids), strict=True)
    candidate_models = snapshot_models(binding.family_id, schema_name=binding.tables_by_role["family"].schema)
    statement, keys, models = prepare._children_statement(request, registry, build_id, root_ids, candidate_models)
    counts = _child_census(session, request, build_id, statement, keys, registry)
    child_rows = prepare._physical_family_rows(
        session,
        request,
        build_id,
        lambda _candidate, _base: (statement, keys, models),
        bounds=graph._ReadPage(
            row_limit=MAX_BATCH_ROWS // 2,
            reserve_bytes=sum(
                graph._model_bytes((source_family.plan, source_family.root, source_family.record))
                for source_family in roots
            ),
        ),
        candidate_models=candidate_models,
    )
    try:
        yield from _pack_source_roots(
            request, registry, binding.layout, allocated, child_rows, counts, limits, stage_output
        )
    finally:
        child_rows.close()
