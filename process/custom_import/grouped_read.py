# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Grouped selection using verified, ordinary root winner materialization."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace

from sqlalchemy import and_, inspect, literal, select

from db.models.custom_import import CustomImportEntityBinding, CustomImportRootScalar, CustomImportWinner
from process.custom_import import grouped_child_read as child
from process.custom_import import read_core as core
from process.custom_import.definition import canonical_json
from process.custom_import.materialization import _context_digest
from process.custom_import.read_contracts import (
    MAX_FAMILY_RESPONSE_BYTES as MAX_FAMILY_RESPONSE_BYTES,
)
from process.custom_import.read_contracts import (
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
    require_match: bool = True


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


def normalize_plan(context, query, scope, *, projection="query_projection", use_default_order=True) -> GroupedReadPlan:
    """Normalize aliases without allowing metrics to influence the selected value."""

    selection = _verified_descriptor(context, query.grouped_entity_selection)
    if query.entity_values is not None:
        core._validate_npi_page(query.entity_values)
    if type(query.require_match) is not bool or type(query.require_exact_context) is not bool:
        raise core.CustomImportReadRequestError("imported membership mode is invalid")
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
    metrics, order = _metric_terms(context, query, child_collection, dimensions, child_key, use_default_order)
    _verify_metric_scope(context, metrics, order, selectors, child_selectors)
    selected_value = next(
        (predicate.value for predicate in selectors if predicate.field.field_id == selection.field_id), None
    )
    if len(selectors) + len(metrics) + (selected_value is None) > maximum_terms:
        raise core.CustomImportReadRequestError("grouped selection predicate count exceeds its limit")
    if metrics and not query.require_match:
        raise core.CustomImportReadRequestError("metric predicates require imported membership")
    projection = "full_family" if query.family_entitlement == "full_family" else projection
    fingerprint = _query_fingerprint(context, query, selectors, metrics, order, selected_value, projection, scope)
    return GroupedReadPlan(
        selected_value, selectors, metrics, order, fingerprint, projection, query.require_match or bool(child_selectors)
    )


def _metric_terms(context, query, child_collection, dimensions, child_key, use_default_order):
    metrics = core._normalized_filters(
        query.filters,
        context,
        query_child_collection=child_collection,
        maximum_terms=core.MAX_PROVIDER_FILTER_TERMS,
        allow_derived=True,
    )
    core._verify_metric_filters(metrics, dimensions + ((child_key.field_id,) if child_key is not None else ()))
    order = (
        ()
        if query.order_terms is None and not use_default_order
        else core._normalize_query_order_terms(
            query.order_terms
            if query.order_terms is not None
            else tuple(
                core.ReadOrderTerm(term.field_id, term.direction, term.nulls)
                for term in context.definition.query.order_terms
            ),
            context,
            explicit=query.order_terms is not None,
            query_child_collection=child_collection,
            maximum_terms=core.MAX_PROVIDER_ORDER_TERMS,
            allow_derived=True,
        )
    )
    return metrics, order


def _verify_metric_scope(context, metrics, order, selectors, child_selectors):
    filterable_fields = getattr(context.definition.query, "filterable_fields", None)
    if filterable_fields is not None and any(
        predicate.field.field_id not in filterable_fields for predicate in metrics
    ):
        raise core.CustomImportReadRequestError("filter field is not opted in by the query contract")
    if (
        any(_query_field(context, term.field_id).collection is not None for term in order)
        or any(
            predicate.field.field_id in context.definition.query.derived_by_id
            and predicate.field.collection is not None
            for predicate in metrics
        )
    ) and not child_selectors:
        raise core.CustomImportReadRequestError("child ordering requires exact complete-key context")
    raw_terms = [term.field.field_id for term in metrics] + [term.field_id for term in order]
    raw_terms = [field_id for field_id in raw_terms if field_id not in context.definition.query.derived_by_id]
    selection = context.definition.query.entity_selection
    if raw_terms and not any(predicate.field.field_id == selection.group_field_id for predicate in selectors):
        raise core.CustomImportReadRequestError("grouped metrics and ordering require an explicit group selector")


def _query_field(context, field_id):
    return context.definition.fields_by_id.get(field_id) or context.definition.query.derived_by_id[field_id]


def _join_root_scalar(statement, context, field, *, prefix="root"):
    family = context.model(core.CustomImportFamilyRevision)
    scalar = inspect(context.model(CustomImportRootScalar)).selectable.alias(f"{prefix}_slot_{field.field_slot}")
    statement = statement.outerjoin(
        scalar,
        and_(
            scalar.c.root_revision_id == family.root_revision_id,
            scalar.c.dataset_id == context.target.dataset_id,
            scalar.c.schema_revision_id == context.target.schema_revision_id,
            scalar.c.root_record_id == family.root_record_id,
            scalar.c.field_slot == field.field_slot,
            scalar.c.value_state == "value",
        ),
    )
    return statement, getattr(scalar.c, core._SCALAR_COLUMNS[field.value_type])


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
    if query.entity_values is not None:
        descriptor_map["entity_values"] = sorted(query.entity_values)
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


def _selected_value_relation(context, entity_values=None):
    """Address the helper's empty-context winner without panel or metric filters."""

    winner_model = context.model(CustomImportWinner)

    selection = context.definition.query.entity_selection
    helper = replace(
        context,
        target=replace(context.target, profile_id=selection.default_profile),
        profile_slot=context.default_profile_slot,
    )
    field = context.definition.fields_by_id[selection.field_id]
    statement, value = _join_root_scalar(
        core._filtered_npi_winner_statement(helper, ()), helper, field, prefix="helper"
    )
    if entity_values is not None:
        statement = statement.where(context.model(CustomImportEntityBinding).canonical_value.in_(entity_values))
    return (
        statement.where(
            winner_model.context_key_sha256 == _profile_context_digest(context, selection.default_profile, {})
        )
        .with_only_columns(
            winner_model.entity_binding_id.label("entity_binding_id"),
            value.label("selected_value"),
            maintain_column_froms=True,
        )
        .subquery("selected_entity_value")
    )


def selected_family_statement(context, plan, *, materialize_default=False, entity_values=None):
    """Retain all configured groups at one independently resolved entity value."""

    winner_model = context.model(CustomImportWinner)
    entity_model = context.model(CustomImportEntityBinding)

    selection = context.definition.query.entity_selection
    fields = context.definition.fields_by_id
    statement = core._filtered_npi_winner_statement(context, ())
    if entity_values is not None:
        statement = statement.where(entity_model.canonical_value.in_(entity_values))
    statement, value_expression = _join_root_scalar(statement, context, fields[selection.field_id])
    statement, group_expression = _join_root_scalar(statement, context, fields[selection.group_field_id])
    selected_value = literal(plan.selected_value)
    if plan.selected_value is None:
        latest = _selected_value_relation(context, entity_values)
        if materialize_default:
            # Address the guarded helper by its indexed entity key instead of rescanning all entities.
            selected_value = (
                select(latest.c.selected_value)
                .where(latest.c.entity_binding_id == winner_model.entity_binding_id)
                .correlate(winner_model)
                .scalar_subquery()
            )
        else:
            statement = statement.join(latest, latest.c.entity_binding_id == winner_model.entity_binding_id)
            selected_value = latest.c.selected_value
    return statement.where(
        value_expression == selected_value, group_expression.in_(selection.group_values)
    ).add_columns(
        entity_model.canonical_value.label("entity_value"),
        value_expression.label("selected_value"),
        group_expression.label("group_value"),
    )


def matching_family_statement(context, plan, *, materialize_default=False, entity_values=None):
    """Apply predicates to the fixed family set, never to helper selection."""

    statement = selected_family_statement(
        context, plan, materialize_default=materialize_default, entity_values=entity_values
    )
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

    from process.custom_import.grouped_query import relation_statement

    plan = normalize_plan(context, query, scope)
    return core.PreparedNpiEntityRelation(
        relation_statement(context, plan, query.entity_values),
        plan.order_terms,
        plan.fingerprint,
        core._scope_digest(scope),
        plan.require_match,
    )


async def _selected_family_rows(session, context, plan, entity_values):
    """Batch retained families for already eligible native-page identities."""

    entity_model = context.model(CustomImportEntityBinding)

    if not entity_values:
        return ()
    statement = selected_family_statement(context, plan, entity_values=entity_values).where(
        entity_model.canonical_value.in_(entity_values)
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

    entity_model = context.model(CustomImportEntityBinding)

    plan = normalize_plan(context, query, scope)
    if plan.projection == "full_family" and len(entity_values) > MAX_FULL_FAMILY_PAGE_SIZE:
        raise core.CustomImportReadRequestError("full-family provider page exceeds its bound")
    if (
        type(prepared) is not core.PreparedNpiEntityRelation
        or prepared.query_fingerprint != plan.fingerprint
        or prepared.authorization_scope_sha256 != core._scope_digest(scope)
    ):
        raise core.CustomImportReadUnavailableError("provider page query identity is unavailable")
    from process.custom_import.grouped_query import relation_statement

    matching = relation_statement(context, plan, entity_values)
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

    entity_model = context.model(CustomImportEntityBinding)

    if request.entity.adapter_id != "npi":
        raise core.CustomImportReadRequestError("grouped entity selection requires the NPI adapter")
    core._validate_npi_page((request.entity.value,))
    query = core.NpiEntityRelationQuery(
        context_filters=request.context_filters,
        grouped_entity_selection=request.grouped_entity_selection,
        grouped_child_query=request.grouped_child_query,
    )
    plan = normalize_plan(context, query, scope, projection="full_family", use_default_order=False)
    matching = matching_family_statement(context, plan, entity_values=(request.entity.value,)).where(
        entity_model.canonical_value == request.entity.value
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
