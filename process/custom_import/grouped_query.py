# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Joined typed values for one selected family set and complete child key."""

from sqlalchemy import and_, case, func, inspect, literal, select

from db.models.custom_import import CustomImportChildRevision, CustomImportChildScalar, CustomImportFamilyChild
from process.custom_import import read_core as core
from process.custom_import.derived_query import reduce_expression


def selected_identities(context, plan, entity_values=None):
    """Pin all selected family identities before evaluating score predicates."""

    from process.custom_import.grouped_read import selected_family_statement

    family = context.model(core.CustomImportFamilyRevision)
    winner = context.model(core.CustomImportWinner)
    entity = context.model(core.CustomImportEntityBinding)
    statement = selected_family_statement(context, plan, materialize_default=True)
    if entity_values is not None:
        statement = statement.where(entity.canonical_value.in_(entity_values))
    return statement.with_only_columns(
        winner.family_revision_id,
        family.root_record_id,
        family.root_revision_id,
        entity.canonical_value.label("entity_value"),
        *statement.selected_columns[-2:],
        maintain_column_froms=True,
    ).cte("selected_score_families")


def _root_values(context, selected, fields):
    statement = select(selected).select_from(selected)
    values_by_field, states_by_field = {}, {}
    selection = context.definition.query.entity_selection
    selected_dimensions_by_id = {
        selection.field_id: selected.c.selected_value,
        selection.group_field_id: selected.c.group_value,
    }
    for field in fields:
        if field.field_id in selected_dimensions_by_id:
            values_by_field[field.field_id] = selected_dimensions_by_id[field.field_id]
            states_by_field[field.field_id] = literal("value")
            continue
        scalar = inspect(context.model(core.CustomImportRootScalar)).selectable.alias(f"score_root_{field.field_slot}")
        statement = statement.outerjoin(
            scalar,
            and_(
                scalar.c.root_revision_id == selected.c.root_revision_id,
                scalar.c.dataset_id == context.target.dataset_id,
                scalar.c.schema_revision_id == context.target.schema_revision_id,
                scalar.c.root_record_id == selected.c.root_record_id,
                scalar.c.field_slot == field.field_slot,
            ),
        )
        values_by_field[field.field_id] = getattr(scalar.c, core._SCALAR_COLUMNS[field.value_type])
        states_by_field[field.field_id] = scalar.c.value_state
    return statement, values_by_field, states_by_field


def _keyed_children(context, selectors):
    """Use the complete typed child key before loading sibling metric values."""

    if not selectors:
        return None
    predicate = selectors[0]
    scalar = inspect(context.model(CustomImportChildScalar)).selectable.alias("complete_child_key")
    typed_value = getattr(scalar.c, core._SCALAR_COLUMNS[predicate.field.value_type])
    return (
        select(
            scalar.c.child_revision_id,
            scalar.c.dataset_id,
            scalar.c.schema_revision_id,
            scalar.c.root_record_id,
            scalar.c.collection_slot,
            typed_value.label("selected_key_value"),
        )
        .where(
            scalar.c.dataset_id == context.target.dataset_id,
            scalar.c.schema_revision_id == context.target.schema_revision_id,
            scalar.c.collection_slot == context.collection_slots_by_name[predicate.field.collection],
            scalar.c.field_slot == predicate.field.field_slot,
            _comparison(predicate, typed_value, scalar.c.value_state),
        )
        .cte("complete_score_child_keys")
    )


def _child_statement(context, selected, keyed_children):
    member = context.model(CustomImportFamilyChild)
    revision = context.model(CustomImportChildRevision)
    statement = select(member.family_revision_id, member.root_record_id, revision.child_revision_id).select_from(member)
    if keyed_children is not None:
        statement = statement.join(
            keyed_children,
            and_(
                keyed_children.c.child_revision_id == member.child_revision_id,
                keyed_children.c.dataset_id == member.dataset_id,
                keyed_children.c.schema_revision_id == member.schema_revision_id,
                keyed_children.c.root_record_id == member.root_record_id,
                keyed_children.c.collection_slot == member.collection_slot,
            ),
        )
    statement = (
        statement.join(
            selected,
            and_(
                selected.c.family_revision_id == member.family_revision_id,
                selected.c.root_record_id == member.root_record_id,
            ),
        )
        .join(
            revision,
            and_(
                revision.child_revision_id == member.child_revision_id,
                revision.dataset_id == member.dataset_id,
                revision.schema_revision_id == member.schema_revision_id,
                revision.root_record_id == member.root_record_id,
                revision.collection_slot == member.collection_slot,
            ),
        )
        .where(
            member.dataset_id == context.target.dataset_id,
            member.schema_revision_id == context.target.schema_revision_id,
            member.collection_slot == context.collection_slots_by_name[context.definition.query.child_collection],
        )
    )
    return statement, member, revision


def _left_child_statement(context, selected, keyed_children):
    """Keep each selected root even when its complete child key is absent."""

    member = context.model(CustomImportFamilyChild)
    revision = context.model(CustomImportChildRevision)
    identities = ("child_revision_id", "dataset_id", "schema_revision_id", "root_record_id", "collection_slot")
    child_source = (
        inspect(member)
        .selectable.join(
            keyed_children,
            and_(*(getattr(keyed_children.c, name) == getattr(member, name) for name in identities)),
        )
        .join(
            inspect(revision).selectable,
            and_(*(getattr(revision, name) == getattr(member, name) for name in identities)),
        )
    )
    statement = (
        select(selected.c.family_revision_id, selected.c.root_record_id, revision.child_revision_id)
        .select_from(selected)
        .outerjoin(
            child_source,
            and_(
                selected.c.family_revision_id == member.family_revision_id,
                selected.c.root_record_id == member.root_record_id,
                member.dataset_id == context.target.dataset_id,
                member.schema_revision_id == context.target.schema_revision_id,
                member.collection_slot == context.collection_slots_by_name[context.definition.query.child_collection],
            ),
        )
    )
    return statement, member, revision


def _child_values(context, selected, fields, selectors, *, retained_columns=(), preserve_roots=False):
    keyed_children = _keyed_children(context, selectors)
    if keyed_children is not None and not preserve_roots:
        keyed_children = keyed_children.prefix_with("MATERIALIZED")
    builder = _left_child_statement if preserve_roots else _child_statement
    statement, member, revision = builder(context, selected, keyed_children)
    statement = statement.add_columns(*retained_columns)
    values_by_field, states_by_field = {}, {}
    for index, field in enumerate(fields):
        if keyed_children is not None and field.field_id == selectors[0].field.field_id:
            typed_value, state = keyed_children.c.selected_key_value, literal("value")
            if preserve_roots:
                state = case((revision.child_revision_id.is_not(None), "value"))
            values_by_field[field.field_id], states_by_field[field.field_id] = typed_value, state
            statement = statement.add_columns(typed_value.label(f"value_{index}"), state.label(f"state_{index}"))
            continue
        scalar = inspect(context.model(CustomImportChildScalar)).selectable.alias(f"score_child_{field.field_slot}")
        statement = statement.outerjoin(
            scalar,
            and_(
                scalar.c.child_revision_id == revision.child_revision_id,
                scalar.c.dataset_id == member.dataset_id,
                scalar.c.schema_revision_id == member.schema_revision_id,
                scalar.c.root_record_id == member.root_record_id,
                scalar.c.collection_slot == member.collection_slot,
                scalar.c.field_slot == field.field_slot,
            ),
        )
        typed_value = getattr(scalar.c, core._SCALAR_COLUMNS[field.value_type])
        values_by_field[field.field_id], states_by_field[field.field_id] = typed_value, scalar.c.value_state
        statement = statement.add_columns(
            typed_value.label(f"value_{index}"), scalar.c.value_state.label(f"state_{index}")
        )
    for predicate in () if preserve_roots else selectors:
        statement = statement.where(
            _comparison(predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id])
        )
    children = statement.cte("selected_score_children")
    return (
        children,
        {field.field_id: children.c[f"value_{index}"] for index, field in enumerate(fields)},
        {field.field_id: children.c[f"state_{index}"] for index, field in enumerate(fields)},
    )


def _validated_stored_children(children, *, preserve_roots=False):
    """Validate complete-key cardinality before later metric predicates can hide it."""

    count = func.count(children.c.child_revision_id) if preserve_roots else func.count()
    ranked = select(
        children,
        count.over(partition_by=(children.c.family_revision_id, children.c.root_record_id)).label("matching_count"),
    ).cte("counted_score_children")
    ambiguous = (
        select(children.c.child_revision_id)
        .where(
            children.c.family_revision_id == ranked.c.family_revision_id,
            children.c.root_record_id == ranked.c.root_record_id,
        )
        .correlate(ranked)
        .scalar_subquery()
    )
    return (
        select(
            ranked,
            case(
                (
                    ranked.c.matching_count <= 1 if preserve_roots else ranked.c.matching_count == 1,
                    ranked.c.child_revision_id,
                ),
                else_=ambiguous,
            ).label("validated_child_id"),
        )
        .cte("validated_score_children")
        .prefix_with("MATERIALIZED")
    )


def _stored_child_statement(context, plan, entity_values, fields, selectors):
    """Carry one selected root through its child join without rescanning CTE pairs."""

    selected = selected_identities(context, plan, entity_values)
    statement, root_values_by_field, root_states_by_field = _root_values(
        context, selected, [field for field in fields if field.collection is None]
    )
    selected, _, root_values_by_field, root_states_by_field = _matched_root_values(
        selected, statement, root_values_by_field, root_states_by_field, plan
    )
    retained_columns = [selected.c.entity_value, selected.c.group_value]
    retained_columns.extend(
        typed_value.label(f"root_value_{index}") for index, typed_value in enumerate(root_values_by_field.values())
    )
    retained_columns.extend(
        state.label(f"root_state_{index}") for index, state in enumerate(root_states_by_field.values())
    )
    children, child_values, child_states = _child_values(
        context,
        selected,
        [field for field in fields if field.collection is not None],
        selectors,
        retained_columns=tuple(retained_columns),
    )
    validated = _validated_stored_children(children)
    values_by_field = {
        field_id: validated.c[f"root_value_{index}"] for index, field_id in enumerate(root_values_by_field)
    }
    states_by_field = {
        field_id: validated.c[f"root_state_{index}"] for index, field_id in enumerate(root_states_by_field)
    }
    values_by_field.update({field_id: validated.c[column.name] for field_id, column in child_values.items()})
    states_by_field.update({field_id: validated.c[column.name] for field_id, column in child_states.items()})
    statement = select(validated).where(validated.c.validated_child_id.is_not(None))
    for predicate in (*plan.context_filters, *plan.filters):
        statement = statement.where(
            _comparison(predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id])
        )
    return statement.with_only_columns(
        validated.c.entity_value,
        *(values_by_field[term.field_id].label(f"sort_{index}") for index, term in enumerate(plan.order_terms)),
        maintain_column_froms=True,
    ).distinct()


def _comparison(predicate, value, state):
    if predicate.operator == "is_missing":
        return state.is_(None)
    if predicate.operator == "is_null":
        return state == "null"
    return and_(state == "value", core._has_scalar_comparison(value, predicate.operator, predicate.value))


def _stored_fields(context, plan):
    requested_field_ids = {predicate.field.field_id for predicate in (*plan.context_filters, *plan.filters)}
    requested_field_ids.update(term.field_id for term in plan.order_terms)
    derived_fields = [
        field for field in context.definition.query.derived_fields if field.field_id in requested_field_ids
    ]
    for field in derived_fields:
        requested_field_ids.add(field.source_field_id)
        if field.weight_field_id is not None:
            requested_field_ids.add(field.weight_field_id)
    fields = [field for field_id, field in context.definition.fields_by_id.items() if field_id in requested_field_ids]
    return fields, derived_fields


def _complete_child_identity(children, selected):
    """Use set-based valid identities; ambiguous complete keys retain scalar failure."""

    counts = (
        select(
            children.c.family_revision_id,
            children.c.root_record_id,
            func.count().label("matching_count"),
            func.min(children.c.child_revision_id).label("child_revision_id"),
        )
        .group_by(children.c.family_revision_id, children.c.root_record_id)
        .cte("selected_child_counts")
    )
    ambiguous = (
        select(children.c.child_revision_id)
        .where(
            children.c.family_revision_id == counts.c.family_revision_id,
            children.c.root_record_id == counts.c.root_record_id,
        )
        .correlate(counts)
        .scalar_subquery()
    )
    identities = (
        select(
            counts.c.family_revision_id,
            counts.c.root_record_id,
            case((counts.c.matching_count == 1, counts.c.child_revision_id), else_=ambiguous).label(
                "child_revision_id"
            ),
        )
        .cte("complete_score_child_identities")
        .prefix_with("MATERIALIZED")
    )
    ownership = and_(
        identities.c.family_revision_id == selected.c.family_revision_id,
        identities.c.root_record_id == selected.c.root_record_id,
    )
    return identities, ownership, identities.c.child_revision_id


def _matched_root_values(selected, statement, values_by_field, states_by_field, plan):
    """Bound stored child work to eligible roots after independent year selection."""

    for predicate in (*plan.context_filters, *plan.filters):
        if predicate.field.collection is None:
            statement = statement.where(
                _comparison(
                    predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id]
                )
            )
    columns = [selected]
    columns.extend(expression.label(f"value_{index}") for index, expression in enumerate(values_by_field.values()))
    columns.extend(expression.label(f"state_{index}") for index, expression in enumerate(states_by_field.values()))
    # Keep independent root selection out of the larger child join plan.
    matched = (
        statement.with_only_columns(*columns, maintain_column_froms=True)
        .cte("matched_root_score_values")
        .prefix_with("MATERIALIZED")
    )
    return (
        matched,
        select(matched),
        {field_id: matched.c[f"value_{index}"] for index, field_id in enumerate(values_by_field)},
        {field_id: matched.c[f"state_{index}"] for index, field_id in enumerate(states_by_field)},
    )


def _joined_values(context, plan, entity_values=None):
    fields, derived_fields = _stored_fields(context, plan)
    selectors = [predicate for predicate in plan.context_filters if predicate.field.collection is not None]
    if derived_fields and selectors:
        return _derived_child_values(context, plan, entity_values, fields, selectors, derived_fields)
    selected = selected_identities(context, plan, entity_values)
    statement, values_by_field, states_by_field = _root_values(
        context, selected, [field for field in fields if field.collection is None]
    )
    child_fields = [field for field in fields if field.collection is not None]
    if child_fields and not derived_fields:
        selected, statement, values_by_field, states_by_field = _matched_root_values(
            selected, statement, values_by_field, states_by_field, plan
        )
    child_identity = None
    if child_fields:
        children, child_values_by_field, child_states_by_field = _child_values(
            context, selected, child_fields, selectors
        )
        identity = and_(
            children.c.family_revision_id == selected.c.family_revision_id,
            children.c.root_record_id == selected.c.root_record_id,
        )
        if selectors:
            counts, ownership, child_id = _complete_child_identity(children, selected)
            statement = statement.outerjoin(counts, ownership)
            identity = and_(identity, children.c.child_revision_id == child_id)
        statement = statement.outerjoin(children, identity)
        values_by_field.update(child_values_by_field)
        states_by_field.update(child_states_by_field)
        if selectors:
            child_identity = children.c.child_revision_id
    return selected, statement, values_by_field, states_by_field, derived_fields, child_identity


def _derived_child_values(context, plan, entity_values, fields, selectors, derived_fields):
    """Reduce every configured group from one root-preserving complete-child join."""

    selected = selected_identities(context, plan, entity_values)
    statement, root_values_by_field, root_states_by_field = _root_values(
        context, selected, [field for field in fields if field.collection is None]
    )
    columns = [selected]
    columns.extend(
        typed_value.label(f"root_value_{index}") for index, typed_value in enumerate(root_values_by_field.values())
    )
    columns.extend(state.label(f"root_state_{index}") for index, state in enumerate(root_states_by_field.values()))
    roots = (
        statement.with_only_columns(*columns, maintain_column_froms=True)
        .cte("derived_root_score_values")
        .prefix_with("MATERIALIZED")
    )
    retained_columns = [roots.c.entity_value, roots.c.group_value]
    retained_columns.extend(roots.c[f"root_value_{index}"] for index in range(len(root_values_by_field)))
    retained_columns.extend(roots.c[f"root_state_{index}"] for index in range(len(root_states_by_field)))
    children, child_values_by_field, child_states_by_field = _child_values(
        context,
        roots,
        [field for field in fields if field.collection is not None],
        selectors,
        retained_columns=tuple(retained_columns),
        preserve_roots=True,
    )
    validated = _validated_stored_children(children, preserve_roots=True)
    values_by_field = {
        field_id: validated.c[f"root_value_{index}"] for index, field_id in enumerate(root_values_by_field)
    }
    states_by_field = {
        field_id: validated.c[f"root_state_{index}"] for index, field_id in enumerate(root_states_by_field)
    }
    values_by_field.update(
        {field_id: validated.c[typed_value.name] for field_id, typed_value in child_values_by_field.items()}
    )
    states_by_field.update({field_id: validated.c[state.name] for field_id, state in child_states_by_field.items()})
    return (
        validated,
        select(validated),
        values_by_field,
        states_by_field,
        derived_fields,
        validated.c.validated_child_id,
    )


def _derived_relation(selected, statement, values_by_field, derived_fields, *, context_match=None):
    columns = [selected.c.entity_value]
    for index, field in enumerate(derived_fields):
        values_by_group = {
            group: func.max(case((selected.c.group_value == group, values_by_field[field.source_field_id])))
            for group in field.group_values
        }
        weights_by_group = (
            None
            if field.weight_field_id is None
            else {
                group: func.max(case((selected.c.group_value == group, values_by_field[field.weight_field_id])))
                for group in field.group_values
            }
        )
        columns.append(reduce_expression(field, values_by_group, weights_by_group).label(f"derived_{index}"))
    if context_match is not None:
        columns.append(func.bool_or(context_match).label("context_matches"))
    reduced = (
        statement.with_only_columns(*columns, maintain_column_froms=True)
        .group_by(selected.c.entity_value)
        .cte("derived_score_values")
        .prefix_with("MATERIALIZED")
    )
    return reduced, {field.field_id: reduced.c[f"derived_{index}"] for index, field in enumerate(derived_fields)}


def _derived_only(context, plan):
    metric_ids = {predicate.field.field_id for predicate in plan.filters}
    metric_ids.update(term.field_id for term in plan.order_terms)
    return bool(metric_ids) and metric_ids <= set(context.definition.query.derived_by_id)


def _context_membership(plan, values_by_field, states_by_field, child_identity):
    """Preserve same-family selectors while reducers retain every configured source group."""

    clauses = [
        _comparison(predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id])
        for predicate in plan.context_filters
    ]
    if child_identity is not None:
        clauses.append(child_identity.is_not(None))
    return and_(*clauses) if clauses else literal(True)


def _reduced_statement(reduced, values_by_field, plan):
    statement = select(reduced.c.entity_value).where(reduced.c.context_matches.is_(True))
    for predicate in plan.filters:
        value = values_by_field[predicate.field.field_id]
        state = case((value.is_not(None), "value"), else_="null")
        statement = statement.where(_comparison(predicate, value, state))
    return statement.add_columns(
        *(values_by_field[term.field_id].label(f"sort_{index}") for index, term in enumerate(plan.order_terms))
    )


def _shared_score_values(selected, statement, values_by_field, states_by_field, child_identity):
    """Reuse sibling source values for reducers and raw predicates without repeating joins."""

    columns = [selected.c.entity_value, selected.c.group_value]
    columns.extend(expression.label(f"value_{index}") for index, expression in enumerate(values_by_field.values()))
    columns.extend(expression.label(f"state_{index}") for index, expression in enumerate(states_by_field.values()))
    if child_identity is not None:
        columns.append(child_identity.label("selected_child_revision_id"))
    shared = (
        statement.with_only_columns(*columns, maintain_column_froms=True)
        .cte("joined_score_values")
        .prefix_with("MATERIALIZED")
    )
    values_by_field = {field_id: shared.c[f"value_{index}"] for index, field_id in enumerate(values_by_field)}
    states_by_field = {field_id: shared.c[f"state_{index}"] for index, field_id in enumerate(states_by_field)}
    child_identity = shared.c.selected_child_revision_id if child_identity is not None else None
    return shared, select(shared), values_by_field, states_by_field, child_identity


def relation_statement(context, plan, entity_values=None):
    """Return deduplicated eligible providers with their exact score ordering tuple."""

    fields, derived = _stored_fields(context, plan)
    selectors = [predicate for predicate in plan.context_filters if predicate.field.collection is not None]
    if selectors and not derived:
        return _stored_child_statement(context, plan, entity_values, fields, selectors)
    selected, statement, values_by_field, states_by_field, derived_fields, child_identity = _joined_values(
        context, plan, entity_values
    )
    if derived_fields:
        selected, statement, values_by_field, states_by_field, child_identity = _shared_score_values(
            selected, statement, values_by_field, states_by_field, child_identity
        )
        only_derived = _derived_only(context, plan)
        membership = (
            _context_membership(plan, values_by_field, states_by_field, child_identity) if only_derived else None
        )
        reduced, reduced_values_by_field = _derived_relation(
            selected, statement, values_by_field, derived_fields, context_match=membership
        )
        if only_derived:
            return _reduced_statement(reduced, reduced_values_by_field, plan)
        statement = statement.join(reduced, reduced.c.entity_value == selected.c.entity_value)
        values_by_field.update(reduced_values_by_field)
        states_by_field.update(
            {
                field.field_id: case((reduced_values_by_field[field.field_id].is_not(None), "value"), else_="null")
                for field in derived_fields
            }
        )
    if child_identity is not None:
        statement = statement.where(child_identity.is_not(None))
    for predicate in (*plan.context_filters, *plan.filters):
        statement = statement.where(
            _comparison(predicate, values_by_field[predicate.field.field_id], states_by_field[predicate.field.field_id])
        )
    columns = [selected.c.entity_value]
    columns.extend(
        values_by_field[term.field_id].label(f"sort_{ordinal}") for ordinal, term in enumerate(plan.order_terms)
    )
    return statement.with_only_columns(*columns, maintain_column_froms=True).distinct()
