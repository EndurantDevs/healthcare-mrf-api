# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed query views for an already verified and pinned family-block snapshot."""

from collections import OrderedDict
from types import MappingProxyType

from sqlalchemy import and_, case, cast, func, literal, literal_column, or_, select, type_coerce
from sqlalchemy.orm import aliased

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import.family_block_binding import FamilyBlockUnavailable, binding_cache_key
from process.custom_import.family_block_scalar_layout import query_scalar_fields, scalar_column

_SCALAR_MODEL_CACHE_LIMIT = 256
_SCALAR_MODEL_CACHE: OrderedDict[tuple, tuple] = OrderedDict()
_PROJECTION_VIEW_CACHE_LIMIT = 64
_PROJECTION_VIEW_CACHE: OrderedDict[tuple, tuple] = OrderedDict()

_LANES = {
    "string": "string_value",
    "integer": "integer_value",
    "decimal": "decimal_value",
    "boolean": "boolean_value",
    "date": "date_value",
    "timestamp": "timestamp_value",
}


def _checked_scope_int(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise FamilyBlockUnavailable("family block read scope is not a bounded integer")
    return value


def _scope_number(value, scalar_type):
    return cast(literal_column(str(_checked_scope_int(value))), scalar_type)


def _state_and_kind(table, index, field):
    state = (func.get_byte(table.c.raw_states, index // 4).op(">>")(2 * (index % 4))).op("&")(3)
    override = case(
        (table.c.raw_type_overrides.is_(None), 0),
        else_=(func.get_byte(table.c.raw_type_overrides, index // 2).op(">>")(4 * (index % 2))).op("&")(15),
    )
    kind = case({0: field.value_type, **dict(enumerate(_LANES, 1))}, value=override)
    return state, override, kind


def _build_scalar_model(binding, dataset_id, schema_revision_id, slot, field, field_index):
    family = binding.tables_by_role["family"]
    table = binding.tables_by_role["query_record"] if slot else family
    model = CustomImportChildScalar if slot else CustomImportRootScalar
    identifier = "child_revision_id" if slot else "root_revision_id"
    scope = model.__table__.c
    state, override, kind = _state_and_kind(table, field_index, field)
    expression_by_suffix = {
        identifier: table.c[identifier],
        "dataset_id": _scope_number(dataset_id, scope.dataset_id.type),
        "schema_revision_id": _scope_number(schema_revision_id, scope.schema_revision_id.type),
        "root_record_id": family.c.root_record_id,
        "field_slot": _scope_number(field.field_slot, scope.field_slot.type),
        "field_collection_slot": _scope_number(slot, scope.field_collection_slot.type),
        "projection_slot": _scope_number(field.projection_slot, scope.projection_slot.type),
        "field_type": type_coerce(kind, scope.field_type.type),
        "value_state": type_coerce(case({1: "null", 2: "value"}, value=state), scope.value_state.type),
    }
    if slot:
        expression_by_suffix["collection_slot"] = _scope_number(slot, scope.collection_slot.type)
    lane = _LANES[field.value_type]
    scalar_value = case((override == 0, table.c[scalar_column(field)]))
    for name in _LANES.values():
        expression_by_suffix[name] = (
            type_coerce(scalar_value, scope[name].type) if name == lane else cast(literal(None), scope[name].type)
        )
    relation = select(*(expression_by_suffix[name].label(name) for name in scope.keys()))
    if slot:
        relation = relation.select_from(table.join(family, table.c.family_revision_id == family.c.family_revision_id))
        relation = relation.where(table.c.collection_slot == slot)
    # A kind override remains a present row, so it cannot become is_missing.
    relation = relation.where(state != 0)
    return aliased(model, relation.subquery(f"family_block_scalar_{slot}_{field.field_slot}"), adapt_on_names=True)


def field_scalar_model(context, field):
    """Expose one declared raw field, preserving scalar presence and actual kind."""
    binding = context.family_block_binding
    if binding is None or binding.layout.definition.digest != context.definition.digest:
        raise FamilyBlockUnavailable("family block scalar definition differs")
    fields = query_scalar_fields(binding.layout.definition, field.collection)
    if field not in fields or field.projection_slot is None:
        raise FamilyBlockUnavailable("family block scalar field is not queryable")
    slot = 0 if field.collection is None else binding.layout.collection_slots[field.collection]
    if slot and context.collection_slots_by_name.get(field.collection) != slot:
        raise FamilyBlockUnavailable("family block scalar collection differs")
    dataset_id = _checked_scope_int(context.target.dataset_id)
    schema_revision_id = _checked_scope_int(context.target.schema_revision_id)
    cache_key = (
        binding_cache_key(binding),
        dataset_id,
        schema_revision_id,
        slot,
        field.field_slot,
        field.projection_slot,
        field.value_type,
    )
    cached_entry = _SCALAR_MODEL_CACHE.get(cache_key)
    if cached_entry is not None and cached_entry[0] is binding:
        _SCALAR_MODEL_CACHE.move_to_end(cache_key)
        return cached_entry[1]
    scalar_alias = _build_scalar_model(binding, dataset_id, schema_revision_id, slot, field, fields.index(field))
    _SCALAR_MODEL_CACHE[cache_key] = (binding, scalar_alias)
    if len(_SCALAR_MODEL_CACHE) > _SCALAR_MODEL_CACHE_LIMIT:
        _SCALAR_MODEL_CACHE.popitem(last=False)
    return scalar_alias


def query_projection_views(binding):
    """Keep derived query rows and exact dictionary keys in their existing shape."""
    cache_key = binding_cache_key(binding)
    cached_entry = _PROJECTION_VIEW_CACHE.get(cache_key)
    if cached_entry is not None and cached_entry[0] is binding:
        _PROJECTION_VIEW_CACHE.move_to_end(cache_key)
        return cached_entry[1]
    view_by_name = {}
    for role, name in (
        ("query_root", "custom_import_derived_root"),
        ("query_child", "custom_import_derived_child"),
    ):
        table = binding.tables_by_role[role]
        columns = [table.c.entity_binding_id, table.c.selected_value]
        selectable = table
        if role == "query_child":
            dictionary = binding.tables_by_role["key_dictionary"]
            selectable = table.join(
                dictionary,
                and_(
                    table.c.key_rank == dictionary.c.key_rank,
                    table.c.key_variant == dictionary.c.key_variant,
                ),
            )
            columns.extend((dictionary.c.child_key_sha256, dictionary.c.canonical_child_key))
        columns.extend(column for column in table.c if column.name.startswith("field_"))
        view_by_name[name] = select(*columns).select_from(selectable).subquery("family_block_" + name)
    frozen_views = MappingProxyType(view_by_name)
    _PROJECTION_VIEW_CACHE[cache_key] = (binding, frozen_views)
    if len(_PROJECTION_VIEW_CACHE) > _PROJECTION_VIEW_CACHE_LIMIT:
        _PROJECTION_VIEW_CACHE.popitem(last=False)
    return frozen_views


def _validated_root_field_index(context, field):
    binding = context.family_block_binding
    if binding is None or binding.layout.definition.digest != context.definition.digest:
        raise FamilyBlockUnavailable("family block scalar definition differs")
    if field.collection is not None or field.projection_slot is None:
        raise FamilyBlockUnavailable("family block root scalar field is not queryable")
    fields = query_scalar_fields(binding.layout.definition, None)
    if field not in fields:
        raise FamilyBlockUnavailable("family block root scalar field is not queryable")
    return binding.tables_by_role["family"], fields.index(field)


def _root_state_and_no_override(family_table, field_index):
    state = (func.get_byte(family_table.c.raw_states, field_index // 4).op(">>")(2 * (field_index % 4))).op("&")(3)
    no_override = or_(
        family_table.c.raw_type_overrides.is_(None),
        (
            (func.get_byte(family_table.c.raw_type_overrides, field_index // 2).op(">>")(4 * (field_index % 2))).op(
                "&"
            )(15)
        )
        == 0,
    )
    return state, no_override


def direct_root_scalar_predicate(context, predicate, comparison_fn):
    """Evaluate a root scalar filter directly on the joined compact family row."""
    family_table, field_index = _validated_root_field_index(context, predicate.field)
    state, no_override = _root_state_and_no_override(family_table, field_index)
    if predicate.operator == "is_missing":
        return state == 0
    if predicate.operator == "is_null":
        return state == 1
    return and_(
        state == 2,
        no_override,
        comparison_fn(family_table.c[scalar_column(predicate.field)], predicate.operator, predicate.value),
    )


def direct_root_scalar_order_value(context, field):
    """Project a root scalar sort expression directly from the compact family row."""
    family_table, field_index = _validated_root_field_index(context, field)
    state, no_override = _root_state_and_no_override(family_table, field_index)
    return case((and_(state == 2, no_override), family_table.c[scalar_column(field)]))


def compact_single_family_winner_base(context, winner_model, entity_model, pin_winner_fn):
    """Join winner, compact family block (once), and entity binding for root-scoped reads."""
    binding = context.family_block_binding
    if binding is None or binding.layout.definition.digest != context.definition.digest:
        raise FamilyBlockUnavailable("family block read binding differs")
    family_table = binding.tables_by_role["family"]
    statement = (
        select(winner_model)
        .select_from(winner_model)
        .join(
            family_table,
            and_(
                family_table.c.family_revision_id == winner_model.family_revision_id,
                family_table.c.entity_binding_id == winner_model.entity_binding_id,
            ),
        )
    )
    statement = pin_winner_fn(statement, context).where(
        winner_model.context_collection_slot == 0,
        winner_model.context_child_revision_id.is_(None),
    )
    return (
        statement.join(
            entity_model,
            and_(
                entity_model.entity_binding_id == winner_model.entity_binding_id,
                entity_model.dataset_id == winner_model.dataset_id,
            ),
        ),
        family_table,
    )
