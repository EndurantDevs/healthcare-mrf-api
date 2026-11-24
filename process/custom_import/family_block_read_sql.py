# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Typed query views for an already verified and pinned family-block snapshot."""

from types import MappingProxyType

from sqlalchemy import and_, case, cast, func, literal, literal_column, select, type_coerce
from sqlalchemy.orm import aliased

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_scalar_layout import query_scalar_fields, scalar_column

_LANES = {
    "string": "string_value",
    "integer": "integer_value",
    "decimal": "decimal_value",
    "boolean": "boolean_value",
    "date": "date_value",
    "timestamp": "timestamp_value",
}


def _scope_number(value, scalar_type):
    if type(value) is not int or not 0 <= value < 2**63:
        raise FamilyBlockUnavailable("family block read scope is not a bounded integer")
    return cast(literal_column(str(value)), scalar_type)


def _state_and_kind(table, index, field):
    state = (func.get_byte(table.c.raw_states, index // 4).op(">>")(2 * (index % 4))).op("&")(3)
    override = case(
        (table.c.raw_type_overrides.is_(None), 0),
        else_=(func.get_byte(table.c.raw_type_overrides, index // 2).op(">>")(4 * (index % 2))).op("&")(15),
    )
    kind = case({0: field.value_type, **dict(enumerate(_LANES, 1))}, value=override)
    return state, override, kind


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
    family = binding.tables_by_role["family"]
    table = binding.tables_by_role["query_record"] if slot else family
    model = CustomImportChildScalar if slot else CustomImportRootScalar
    identifier = "child_revision_id" if slot else "root_revision_id"
    scope = model.__table__.c
    state, override, kind = _state_and_kind(table, fields.index(field), field)
    expression_by_suffix = {
        identifier: table.c[identifier],
        "dataset_id": _scope_number(context.target.dataset_id, scope.dataset_id.type),
        "schema_revision_id": _scope_number(context.target.schema_revision_id, scope.schema_revision_id.type),
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


def query_projection_views(binding):
    """Keep derived query rows and exact dictionary keys in their existing shape."""
    view_by_name = {}
    for role, name in (
        ("query_root", "custom_import_derived_root"),
        ("query_child", "custom_import_derived_child"),
    ):
        table = binding.tables_by_role[role]
        columns = [table.c.entity_binding_id, table.c.selected_value]
        source = table
        if role == "query_child":
            dictionary = binding.tables_by_role["key_dictionary"]
            source = table.join(
                dictionary,
                and_(
                    table.c.key_rank == dictionary.c.key_rank,
                    table.c.key_variant == dictionary.c.key_variant,
                ),
            )
            columns.extend((dictionary.c.child_key_sha256, dictionary.c.canonical_child_key))
        columns.extend(column for column in table.c if column.name.startswith("field_"))
        view_by_name[name] = select(*columns).select_from(source).subquery("family_block_" + name)
    return MappingProxyType(view_by_name)
