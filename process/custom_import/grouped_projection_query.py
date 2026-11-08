# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read admitted numeric contexts without replacing canonical family membership."""

from sqlalchemy import and_, case, inspect, select

from db.models.custom_import import CustomImportChildRevision
from process.custom_import import grouped_query as query
from process.custom_import.query_projection_sources import field_slots


def _has_required_row(selected, is_present):
    """Fail on absent advertised contexts before metric predicates can hide them."""

    identity = select(selected.c.entity_binding_id).correlate(selected)
    ambiguous = identity.union_all(identity).scalar_subquery()
    return case((is_present, selected.c.entity_binding_id), else_=ambiguous).is_not(None)


def _join_projection(context, selected, statement, fields, *, child_identity=None):
    is_child = fields[0].collection is not None
    name = "custom_import_derived_child" if is_child else "custom_import_derived_root"
    table = context.projection_tables[name]
    ownership_clauses = [
        table.c.entity_binding_id == selected.c.entity_binding_id,
        table.c.selected_value == selected.c.selected_value,
    ]
    if is_child:
        revision = inspect(context.model(CustomImportChildRevision)).selectable.alias("projected_score_child")
        statement = statement.outerjoin(
            revision,
            and_(
                revision.c.child_revision_id == child_identity,
                revision.c.dataset_id == context.target.dataset_id,
                revision.c.schema_revision_id == context.target.schema_revision_id,
                revision.c.root_record_id == selected.c.root_record_id,
                revision.c.collection_slot == context.collection_slots_by_name[fields[0].collection],
            ),
        )
        ownership_clauses.extend(
            (
                table.c.child_key_sha256 == revision.c.child_key_sha256,
                table.c.canonical_child_key.collate("C") == revision.c.canonical_child_key.collate("C"),
            )
        )
    statement = statement.outerjoin(table, and_(*ownership_clauses))
    is_present = table.c.entity_binding_id.is_not(None)
    if is_child:
        # A sibling root without this child still contributes to root projections.
        is_present = child_identity.is_(None) | is_present
    statement = statement.where(_has_required_row(selected, is_present))
    slots = field_slots(context.definition)
    return statement, {field.field_id: table.c[f"field_{slots[field.field_id]}"] for field in fields}


def _source_values(context, plan, entity_values, derived_fields):
    requested_field_ids = {predicate.field.field_id for predicate in (*plan.context_filters, *plan.filters)}
    requested_field_ids.update(term.field_id for term in plan.order_terms)
    fields = [field for field in context.definition.fields if field.field_id in requested_field_ids]
    selectors = [predicate for predicate in plan.context_filters if predicate.field.collection is not None]
    if selectors:
        selected, statement, values_by_field, states_by_field, _, child_identity = query._derived_child_values(
            context, plan, entity_values, fields, selectors, derived_fields, include_binding=True
        )
        return selected, statement, values_by_field, states_by_field, child_identity
    selected = query.selected_identities(context, plan, entity_values, include_binding=True)
    statement, values_by_field, states_by_field = query._root_values(context, selected, fields)
    return selected, statement, values_by_field, states_by_field, None


def relation_statement(context, plan, entity_values, derived_fields):
    """Join each requested wide scope once after independent family selection."""

    selected, statement, values_by_field, states_by_field, child_identity = _source_values(
        context, plan, entity_values, derived_fields
    )
    for is_child in (False, True):
        fields = [field for field in derived_fields if (field.collection is not None) == is_child]
        if fields:
            statement, projected = _join_projection(context, selected, statement, fields, child_identity=child_identity)
            values_by_field.update(projected)
            states_by_field.update(
                {
                    field_id: case((typed_value.is_not(None), "value"), else_="null")
                    for field_id, typed_value in projected.items()
                }
            )
    columns = [selected.c.entity_value]
    columns.extend(typed_value.label(f"value_{index}") for index, typed_value in enumerate(values_by_field.values()))
    columns.extend(state.label(f"state_{index}") for index, state in enumerate(states_by_field.values()))
    if child_identity is not None:
        columns.append(child_identity.label("selected_child_revision_id"))
    checked = (
        statement.with_only_columns(*columns, maintain_column_froms=True)
        .cte("projected_score_values")
        .prefix_with("MATERIALIZED")
    )
    values_by_field = {field_id: checked.c[f"value_{index}"] for index, field_id in enumerate(values_by_field)}
    states_by_field = {field_id: checked.c[f"state_{index}"] for index, field_id in enumerate(states_by_field)}
    statement = select(checked)
    if child_identity is not None:
        statement = statement.where(checked.c.selected_child_revision_id.is_not(None))
    for predicate in (*plan.context_filters, *plan.filters):
        statement = statement.where(
            query._comparison(
                predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id]
            )
        )
    return statement.with_only_columns(
        checked.c.entity_value,
        *(values_by_field[term.field_id].label(f"sort_{index}") for index, term in enumerate(plan.order_terms)),
        maintain_column_froms=True,
    ).distinct()
