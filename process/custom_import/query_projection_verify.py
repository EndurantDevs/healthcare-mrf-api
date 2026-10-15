# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Restartable, native parity checks over bounded finalized entity windows."""

from sqlalchemy import func, literal, select, union_all

from process.custom_import.query_projection_build import _execute, _validated_source
from process.custom_import.query_projection_sources import field_slots
from process.custom_import.query_projection_storage import query_projection_tables
from process.custom_import.query_projection_values import reduction_statement
from process.custom_import.runner_types import CandidateRunnerError


def _context_columns(relation, names):
    return tuple(
        relation.c[name].collate("C").label(name) if name == "canonical_child_key" else relation.c[name]
        for name in names
    )


def _comparison_statement(expected, table, entity_ids):
    expected_contexts = expected.cte().prefix_with("MATERIALIZED")
    names = tuple(expected_contexts.c.keys())
    actual_contexts = (
        select(*_context_columns(table, names))
        .where(table.c.entity_binding_id.in_(entity_ids))
        .cte()
        .prefix_with("MATERIALIZED")
    )
    expected_rows = select(*_context_columns(expected_contexts, names))
    actual_rows = select(*_context_columns(actual_contexts, names))
    differences = union_all(expected_rows.except_all(actual_rows), actual_rows.except_all(expected_rows)).subquery()
    return select(
        select(func.count()).select_from(expected_contexts).scalar_subquery().label("expected_count"),
        select(func.count()).select_from(actual_contexts).scalar_subquery().label("actual_count"),
        select(literal(1)).select_from(differences).exists().label("differs"),
    )


async def _verify_window(session, request, context, manifest, entity_ids, fields, *, child):
    source_relation, values_by_field = await _validated_source(session, context, entity_ids, child=child)
    expected = reduction_statement(
        source_relation, fields, values_by_field=values_by_field, field_slots=field_slots(request.definition)
    )
    table = query_projection_tables(manifest.family_id, request.definition)[
        "custom_import_derived_child" if child else "custom_import_derived_root"
    ]
    comparison = (await _execute(session, _comparison_statement(expected, table, entity_ids))).one()
    if comparison.differs or comparison.expected_count != comparison.actual_count:
        raise CandidateRunnerError("query projection persisted values differ from finalized reducers")
    return comparison.expected_count, comparison.actual_count
