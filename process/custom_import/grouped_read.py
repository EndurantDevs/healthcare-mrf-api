# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Grouped selection using verified, ordinary root winner materialization."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace

from sqlalchemy import literal

from db.models.custom_import import CustomImportEntityBinding, CustomImportWinner
from process.custom_import import grouped_child_read as child
from process.custom_import import read_core as core
from process.custom_import.definition import canonical_json
from process.custom_import.materialization import _context_digest
from process.custom_import.read_contracts import (
    MAX_FAMILY_RESPONSE_BYTES as MAX_FAMILY_RESPONSE_BYTES,
    MAX_FULL_FAMILY_PAGE_SIZE,
    canonical_read_document,
)


@dataclass(frozen=True)
class GroupedReadPlan:
    """One normalized request; selection precedes every matching predicate."""

    selected_value: int | None
    context_filters: tuple
    filters: tuple
    order_terms: tuple
    fingerprint: str
    projection: str = "query_projection"


async def bind_default_profile(session, context):
    """Derive only the helper profile from an already authorized exact target."""

    selection = context.definition.query.entity_selection
    if context.target.profile_id != selection.by_value_profile or context.profile_context_slot != 0:
        raise core.CustomImportReadRequestError("grouped selection requires its declared by-value profile")
    helper_target = replace(context.target, profile_id=selection.default_profile)
    helper = await core._exact_profile(session, helper_target)
    helper_slot, helper_context_slot = core._verified_profile(
        helper, context.definition, context.collection_slots_by_name
    )
    if helper_context_slot != 0 or helper_slot == context.profile_slot:
        raise core.CustomImportReadUnavailableError("grouped selection helper profile is invalid")
    return replace(context, default_profile_slot=helper_slot)


def _verified_descriptor(context, supplied):
    selection = context.definition.query.entity_selection
    if selection is None or type(supplied) is not dict or supplied != selection.document():
        raise core.CustomImportReadRequestError("grouped selection descriptor does not match the pinned definition")
    if context.default_profile_slot is None or context.target.profile_id != selection.by_value_profile:
        raise core.CustomImportReadUnavailableError("grouped selection profiles are unavailable")
    return selection


def normalize_plan(context, query, scope, *, projection="query_projection") -> GroupedReadPlan:
    """Normalize aliases without allowing metrics to influence the selected value."""

    selection = _verified_descriptor(context, query.grouped_entity_selection)
    if type(query.require_match) is not bool or type(query.require_exact_context) is not bool:
        raise core.CustomImportReadRequestError("imported membership mode is invalid")
    if query.require_exact_context:
        raise core.CustomImportReadRequestError("grouped selection is unavailable for exact-context service reads")
    child_key = child.verified_child_key(context, query.grouped_child_query)
    child_collection = child_key.collection if child_key is not None else None
    maximum_terms = core._relation_predicate_limit(query)
    core._validate_npi_entity_relation_request(
        query.filters,
        query.order_terms,
        context_filters=query.context_filters,
        maximum_terms=maximum_terms,
    )
    selectors = core._normalized_filters(query.context_filters or (), context, query_child_collection=child_collection)
    root_selectors = tuple(predicate for predicate in selectors if predicate.field.collection is None)
    child_selectors = tuple(predicate for predicate in selectors if predicate.field.collection is not None)
    dimensions = core._verify_context_filters(root_selectors, context)
    if child_key is not None:
        child.verify_child_selectors(child_selectors, child_key)
    metrics = core._normalized_filters(query.filters, context, query_child_collection=child_collection)
    core._verify_metric_filters(metrics, dimensions + ((child_key.field_id,) if child_key is not None else ()))
    order = (
        ()
        if query.order_terms is None
        else core._normalize_query_order_terms(
            query.order_terms, context, explicit=True, query_child_collection=child_collection
        )
    )
    if (
        any(context.definition.fields_by_id[term.field_id].collection is not None for term in order)
        and not child_selectors
    ):
        raise core.CustomImportReadRequestError("child ordering requires exact complete-key context")
    selected_value = next(
        (predicate.value for predicate in selectors if predicate.field.field_id == selection.field_id), None
    )
    if len(selectors) + len(metrics) + (selected_value is None) > maximum_terms:
        raise core.CustomImportReadRequestError("grouped selection predicate count exceeds its limit")
    if (metrics or order) and not any(predicate.field.field_id == selection.group_field_id for predicate in selectors):
        raise core.CustomImportReadRequestError("grouped metrics and ordering require an explicit group selector")
    if metrics and not query.require_match:
        raise core.CustomImportReadRequestError("metric predicates require imported membership")
    projection = "full_family" if query.family_entitlement == "full_family" else projection
    fingerprint = _query_fingerprint(context, query, selectors, metrics, order, selected_value, projection, scope)
    return GroupedReadPlan(selected_value, selectors, metrics, order, fingerprint, projection)


def _query_fingerprint(context, query, selectors, metrics, order, selected_value, projection, scope):
    descriptor_map = {
        "contract": "custom-import/grouped-entity-selection/v1",
        "target": core._target_document(context.target),
        "grouped_entity_selection": context.definition.query.entity_selection.document(),
        "selection": {"mode": "max_per_entity"} if selected_value is None else {"mode": "eq", "value": selected_value},
        "context": [predicate.descriptor for predicate in selectors],
        "filters": [predicate.descriptor for predicate in metrics],
        "order": [{"field": term.field_id, "direction": term.direction, "nulls": term.nulls} for term in order],
        "projection": projection,
        "require_match": query.require_match,
        "authorization_scope_sha256": core._scope_digest(scope),
    }
    domain = b"custom-import-grouped-read/v1\0"
    if query.grouped_child_query is not None:
        descriptor_map["grouped_child_query"] = query.grouped_child_query
        domain = b"custom-import-grouped-child-read/v1\0"
    return hashlib.sha256(domain + canonical_read_document(descriptor_map)).hexdigest()


def _profile_context_digest(context, profile_id, scalar_values):
    """Use the existing root context document, canonicalizer, and digest domain."""

    profile = next(profile for profile in context.definition.selection_profiles if profile.profile_id == profile_id)
    fields = context.definition.fields_by_id
    return _context_digest(
        canonical_json(
            {
                "profile_id": profile.profile_id,
                "scope": "root",
                "dimensions": [
                    {
                        "field_slot": fields[field_id].field_slot,
                        "field_type": fields[field_id].value_type,
                        "value_state": "value",
                        "value": scalar_values[field_id],
                    }
                    for field_id in profile.context_dimensions
                ],
            }
        )
    )


def _selected_value_relation(context):
    """Address the helper's empty-context winner without panel or metric filters."""

    selection = context.definition.query.entity_selection
    helper = replace(
        context,
        target=replace(context.target, profile_id=selection.default_profile),
        profile_slot=context.default_profile_slot,
    )
    field = context.definition.fields_by_id[selection.field_id]
    return (
        core._filtered_npi_winner_statement(helper, ())
        .where(CustomImportWinner.context_key_sha256 == _profile_context_digest(context, selection.default_profile, {}))
        .with_only_columns(
            CustomImportWinner.entity_binding_id.label("entity_binding_id"),
            core._order_scalar_expression(field, helper).label("selected_value"),
            maintain_column_froms=True,
        )
        .subquery("selected_entity_value")
    )


def selected_family_statement(context, plan):
    """Retain all configured groups at one independently resolved entity value."""

    selection = context.definition.query.entity_selection
    fields = context.definition.fields_by_id
    value_expression = core._order_scalar_expression(fields[selection.field_id], context)
    group_expression = core._order_scalar_expression(fields[selection.group_field_id], context)
    statement = core._filtered_npi_winner_statement(context, ())
    selected_value = literal(plan.selected_value)
    if plan.selected_value is None:
        latest = _selected_value_relation(context)
        statement = statement.join(latest, latest.c.entity_binding_id == CustomImportWinner.entity_binding_id)
        selected_value = latest.c.selected_value
    return statement.where(
        value_expression == selected_value, group_expression.in_(selection.group_values)
    ).add_columns(
        CustomImportEntityBinding.canonical_value.label("entity_value"),
        value_expression.label("selected_value"),
        group_expression.label("group_value"),
    )


def matching_family_statement(context, plan):
    """Apply predicates to the fixed family set, never to helper selection."""

    statement = selected_family_statement(context, plan)
    for predicate in (*plan.context_filters, *plan.filters):
        if predicate.field.collection is None:
            statement = statement.where(core._predicate_condition(predicate, context))
    child_predicates = _child_predicates(plan)
    if child_predicates:
        statement = statement.where(child.matching_child_statement(context, child_predicates).exists())
    return statement


def _child_predicates(plan):
    return tuple(
        predicate for predicate in (*plan.context_filters, *plan.filters) if predicate.field.collection is not None
    )


def prepare_relation(context, query, scope):
    """Return one deduplicated NPI and one explicit-group ordering tuple."""

    plan = normalize_plan(context, query, scope)
    columns = [CustomImportEntityBinding.canonical_value.label("entity_value")]
    for ordinal, term in enumerate(plan.order_terms):
        field = context.definition.fields_by_id[term.field_id]
        expression = (
            core._order_scalar_expression(field, context)
            if field.collection is None
            else child.child_order_expression(context, _child_predicates(plan), field)
        )
        columns.append(expression.label(f"sort_{ordinal}"))
    statement = (
        matching_family_statement(context, plan).with_only_columns(*columns, maintain_column_froms=True).distinct()
    )
    return core.PreparedNpiEntityRelation(statement, plan.order_terms, plan.fingerprint, core._scope_digest(scope))


async def _selected_family_rows(session, context, plan, entity_values):
    """Batch retained families for already eligible native-page identities."""

    if not entity_values:
        return ()
    statement = selected_family_statement(context, plan).where(
        CustomImportEntityBinding.canonical_value.in_(entity_values)
    )
    selected_rows = (await session.execute(statement.limit(len(entity_values) * 2 + 1))).all()
    if len(selected_rows) > len(entity_values) * 2:
        raise core.CustomImportReadUnavailableError("grouped family count exceeds its bound")
    return tuple(selected_rows)


def _validated_family_keys(context, selected_rows):
    """Check decoded scalars and context digests before any family is emitted."""

    selection = context.definition.query.entity_selection
    keys = set()
    value_by_entity = {}
    for winner, _family, _root, entity_value, selected_value, group_value in selected_rows:
        core._normalized_integer(selected_value, selection.field_id)
        core._normalized_string(group_value, selection.group_field_id)
        key = (entity_value, group_value)
        if (
            key in keys
            or group_value not in selection.group_values
            or value_by_entity.setdefault(entity_value, selected_value) != selected_value
        ):
            raise core.CustomImportReadUnavailableError("grouped family identity is ambiguous")
        expected_digest = _profile_context_digest(
            context,
            selection.by_value_profile,
            {
                selection.field_id: selected_value,
                selection.group_field_id: group_value,
            },
        )
        if bytes(winner.context_key_sha256) != expected_digest:
            raise core.CustomImportReadUnavailableError("grouped winner context does not match its typed root")
        keys.add(key)
    return value_by_entity


def _family_sets(context, plan, selected_rows, projections, scope, *, projection):
    """Preserve configured group order without fabricated missing families."""

    selection = context.definition.query.entity_selection
    value_by_entity = _validated_family_keys(context, selected_rows)
    projected_by_entity = {}
    for selected_row, projected in zip(selected_rows, projections, strict=True):
        entity_value, selected_value, group_value = selected_row[-3:]
        fields_by_id = {field.field_id: field for field in projected.root_fields}
        for field_id, expected, kind in (
            (selection.field_id, selected_value, "integer"),
            (selection.group_field_id, group_value, "string"),
        ):
            field = fields_by_id.get(field_id)
            if field is None or field.state != "value" or field.field_type != kind or field.value != expected:
                raise core.CustomImportReadUnavailableError("projected grouped identity is invalid")
        projected_by_entity.setdefault(entity_value, {})[group_value] = projected
    return {
        entity: core.EntityFamilySet(
            context.target,
            projection,
            selection.field_id,
            value_by_entity[entity],
            tuple((group, by_group[group]) for group in selection.group_values if group in by_group),
            tuple(group for group in selection.group_values if group not in by_group),
            plan.fingerprint,
            core._scope_digest(scope),
        )
        for entity, by_group in projected_by_entity.items()
    }


async def hydrate_page(session, context, query, prepared, entity_values, scope):
    """Hydrate both selected groups after deduplicated matching and paging."""

    plan = normalize_plan(context, query, scope)
    if plan.projection == "full_family" and len(entity_values) > MAX_FULL_FAMILY_PAGE_SIZE:
        raise core.CustomImportReadRequestError("full-family provider page exceeds its bound")
    if (
        type(prepared) is not core.PreparedNpiEntityRelation
        or prepared.query_fingerprint != plan.fingerprint
        or prepared.authorization_scope_sha256 != core._scope_digest(scope)
    ):
        raise core.CustomImportReadUnavailableError("provider page query identity is unavailable")
    matching = (
        matching_family_statement(context, plan)
        .with_only_columns(CustomImportEntityBinding.canonical_value, maintain_column_froms=True)
        .where(CustomImportEntityBinding.canonical_value.in_(entity_values))
        .distinct()
    )
    eligible = tuple((await session.execute(matching)).scalars().all()) if entity_values else ()
    selected_rows = await _selected_family_rows(session, context, plan, eligible)
    _validated_family_keys(context, selected_rows)
    if plan.projection == "full_family":
        projections = await _hydrate_complete_families(session, context, selected_rows, scope, is_page=True)
    else:
        projections = await core._hydrate_search_page_items(
            session, context, tuple(core._winner_row(selected_row[:3], False) for selected_row in selected_rows)
        )
    await core.verify_published_generation(session, context.target)
    return _family_sets(context, plan, selected_rows, projections, scope, projection=plan.projection)


async def _hydrate_complete_families(session, context, selected_rows, scope, *, is_page=False):
    """Keep the grouped row cap and use the shared bounded family hydration."""

    entity_values = tuple(selected_row[-3] for selected_row in selected_rows)
    if len(set(entity_values)) > MAX_FULL_FAMILY_PAGE_SIZE:
        raise core.CustomImportReadUnavailableError("full-family provider page exceeds its bound")
    return await core._hydrate_complete_family_entities(
        session,
        context,
        tuple(core._winner_row(selected_row[:3], False) for selected_row in selected_rows),
        entity_values,
        core._scope_digest(scope),
        is_page=is_page,
    )


async def hydrate_detail(session, context, request, scope):
    """Apply entity eligibility then hydrate the bounded complete family set."""

    if request.entity.adapter_id != "npi":
        raise core.CustomImportReadRequestError("grouped entity selection requires the NPI adapter")
    core._validate_npi_page((request.entity.value,))
    query = core.NpiEntityRelationQuery(
        context_filters=request.context_filters,
        grouped_entity_selection=request.grouped_entity_selection,
        grouped_child_query=request.grouped_child_query,
    )
    plan = normalize_plan(context, query, scope, projection="full_family")
    matching = matching_family_statement(context, plan).where(
        CustomImportEntityBinding.canonical_value == request.entity.value
    )
    exists = await session.scalar(matching.with_only_columns(literal(1), maintain_column_froms=True).limit(1))
    if exists is None:
        await core.verify_published_generation(session, context.target)
        raise core.CustomImportReadEntityAbsentError("selected entity has no eligible root family")
    selected_rows = await _selected_family_rows(session, context, plan, (request.entity.value,))
    _validated_family_keys(context, selected_rows)
    if not selected_rows:
        raise core.CustomImportReadUnavailableError("selected families are unavailable")
    projections = await _hydrate_complete_families(session, context, selected_rows, scope)
    await core.verify_published_generation(session, context.target)
    return _family_sets(context, plan, selected_rows, projections, scope, projection="full_family")[
        request.entity.value
    ]
