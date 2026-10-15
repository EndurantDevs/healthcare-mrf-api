# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Reduce finalized winners; callers validate ambiguity before storing contexts.

Sources supply entity_binding_id, selected_value and group_value. Child sources
also supply canonical_child_key and child_key_sha256. Configured field IDs map
explicitly to SQL values; numeric contexts retain no per-group membership.
"""

from sqlalchemy import BigInteger, Numeric, cast, func, select

from process.custom_import.derived_query import MAX_DERIVED_QUERY_FIELDS, reduce_expression


def configured_fields(definition):
    """Retain declaration order and legacy implicit filtering permissions."""

    query = definition.query
    enabled_ids = set(query.derived_by_id if query.filterable_fields is None else query.filterable_fields)
    enabled_ids.update(query.sortable_fields)
    enabled_ids.update(term.field_id for term in query.order_terms)
    return tuple(field for field in query.derived_fields if field.field_id in enabled_ids)


def _identity_columns(source):
    columns = [source.c.entity_binding_id, source.c.selected_value]
    child_columns = ("canonical_child_key", "child_key_sha256")
    if any(name in source.c for name in child_columns):
        if not all(name in source.c for name in child_columns):
            raise ValueError("child projection source requires both complete-key columns")
        columns.extend(source.c[name] for name in child_columns)
    return columns


def ambiguity_statement(source):
    """Find repeated complete family identities before numeric contribution tests."""

    columns = [column for column in _identity_columns(source) if column.name != "child_key_sha256"]
    columns.append(source.c.group_value)
    return select(*columns, func.count().label("matching_count")).group_by(*columns).having(func.count() > 1)


def _checked_slots(fields, field_slots):
    if len(fields) > MAX_DERIVED_QUERY_FIELDS:
        raise ValueError("projection requires at most 32 numeric fields")
    slots = []
    for field in fields:
        slot = field_slots.get(field.field_id)
        if field.value_type not in {"integer", "decimal"}:
            raise ValueError("projection fields must be numeric")
        if type(slot) is not int or not 1 <= slot <= MAX_DERIVED_QUERY_FIELDS:
            raise ValueError("projection field requires its declared slot between 1 and 32")
        slots.append(slot)
    if len({field.field_id for field in fields}) != len(fields) or len(set(slots)) != len(slots):
        raise ValueError("projection fields and declaration slots must be unique")
    return slots


def _field_expression(source, field, values_by_field, slot):
    values_by_group = {
        group: func.max(values_by_field[field.source_field_id]).filter(source.c.group_value == group)
        for group in field.group_values
    }
    weights_by_group = (
        None
        if field.weight_field_id is None
        else {
            group: func.max(values_by_field[field.weight_field_id]).filter(source.c.group_value == group)
            for group in field.group_values
        }
    )
    return cast(
        reduce_expression(field, values_by_group, weights_by_group),
        BigInteger() if field.value_type == "integer" else Numeric(31, 12),
    ).label(f"field_{slot}")


def reduction_statement(source, fields, *, values_by_field, field_slots):
    """Return one context with nullable typed field_<declaration slot> columns."""

    identities = _identity_columns(source)
    is_child = "canonical_child_key" in source.c
    fields = tuple(fields)
    if any((field.collection is not None) != is_child for field in fields):
        raise ValueError("projection fields must match the source root or child scope")
    slots = _checked_slots(fields, field_slots)
    expressions = [
        _field_expression(source, field, values_by_field, slot) for field, slot in zip(fields, slots, strict=True)
    ]
    return select(*identities, *expressions).group_by(*identities)
