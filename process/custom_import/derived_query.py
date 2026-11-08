# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed, definition-owned scalar reducers across selected entity families."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from decimal import Decimal
from typing import TYPE_CHECKING
from unicodedata import category

if TYPE_CHECKING:
    from process.custom_import.definition import ChildCollection, EntitySelection, Field

MAX_DERIVED_QUERY_FIELDS = 32
_SCALE = 10**12
_NUMERIC_TYPES = {"integer", "decimal"}


@dataclass(frozen=True)
class DerivedQueryField:
    """A query-only value; it owns no source slot or persisted scalar row."""

    field_id: str
    value_type: str
    collection: str | None
    child_key: tuple[str, ...]
    kind: str
    source_field_id: str
    weight_field_id: str | None
    group_values: tuple[str, ...]
    output_path: tuple[str | tuple[str, str], ...] | None = None
    nullable: bool = dataclass_field(default=True, init=False)


def parse_derived_fields(
    raw: object,
    root_fields: tuple[Field, ...],
    child_fields: tuple[Field, ...],
    children: tuple[ChildCollection, ...],
    root_query_fields: tuple[str, ...],
    query_child_collection: str | None,
    child_query_fields: tuple[str, ...],
    selection: EntitySelection | None,
) -> tuple[DerivedQueryField, ...]:
    """Allow existing reducers only over declared projected numeric fields."""

    from process.custom_import.definition import DefinitionError, _array, _mapping

    declarations = _array(raw, "definition.query.derived_fields")
    if len(declarations) > MAX_DERIVED_QUERY_FIELDS:
        raise DefinitionError("derived query field count exceeds its limit")
    if declarations and selection is None:
        raise DefinitionError("derived query fields require grouped entity selection")
    field_by_id = {field.field_id: field for field in (*root_fields, *child_fields)}
    parsed_fields = []
    used_ids = set(field_by_id)
    for ordinal, declaration in enumerate(declarations):
        path = f"definition.query.derived_fields[{ordinal}]"
        document = _mapping(
            declaration, path, keys={"id", "expression", "collection", "complete_child_key", "output_path"}
        )
        parsed = _read_numeric_descriptor(
            document,
            path,
            field_by_id,
            root_query_fields,
            child_query_fields,
            children,
            query_child_collection,
            selection,
        )
        if parsed.field_id in used_ids:
            raise DefinitionError("derived query field ids must be unique and distinct from stored fields")
        used_ids.add(parsed.field_id)
        parsed_fields.append(parsed)
    return tuple(parsed_fields)


def _read_numeric_descriptor(
    document, path, field_by_id, root_query_fields, child_query_fields, children, query_child_collection, selection
):
    from process.custom_import.definition import _identifier, _required

    identifier = _identifier(_required(document, "id", path), f"{path}.id")
    expression, kind, scope = _derived_expression(document, path)
    collection, child_keys = _derived_scope(document, scope, children, query_child_collection, child_query_fields)
    permitted_ids = set(root_query_fields if scope == "root" else child_query_fields)
    source_id, weight_id = _derived_sources(expression, kind, field_by_id, permitted_ids, collection, path)
    return DerivedQueryField(
        identifier,
        "decimal" if weight_id is not None else field_by_id[source_id].value_type,
        collection,
        child_keys,
        kind,
        source_id,
        weight_id,
        _derived_groups(expression, selection, path),
        _derived_output_path(document, collection, path),
    )


def _derived_output_path(document, collection, path):
    from process.custom_import.definition import DefinitionError, _array, _identifier, _mapping

    if "output_path" not in document:
        return None
    steps = _array(document["output_path"], f"{path}.output_path")
    if not 1 <= len(steps) <= 16:
        raise DefinitionError("derived output path must contain one to sixteen steps")
    parsed_steps = []
    for step in steps:
        if type(step) is str:
            if len(step.encode("utf-8", errors="surrogatepass")) > 1024 or any(
                category(character) in {"Cc", "Cs"} for character in step
            ):
                raise DefinitionError("derived output keys must be bounded valid text")
            parsed_steps.append(step)
        else:
            repetition = _mapping(step, "derived output repetition", keys={"each"})
            parsed_steps.append(("each", _identifier(repetition.get("each"), "derived output collection")))
    repeated_collections = [step[1] for step in parsed_steps if type(step) is tuple]
    if repeated_collections != ([] if collection is None else [collection]):
        raise DefinitionError("derived output repetitions must match the declared child scope")
    return tuple(parsed_steps)


def _derived_expression(document, path):
    from process.custom_import.definition import DefinitionError, _mapping, _required

    expression = _mapping(
        _required(document, "expression", path),
        f"{path}.expression",
        keys={"type", "scope", "field_id", "value_field", "weight_field", "group_values"},
    )
    kind, scope = expression.get("type"), expression.get("scope")
    if type(kind) is not str or kind not in {"group_weighted_mean", "preferred_non_null"}:
        raise DefinitionError("derived query reducer is unsupported")
    if type(scope) is not str or scope not in {"root", "child"}:
        raise DefinitionError("derived query reducer scope is invalid")
    expected_keys = {"type", "scope", "group_values"}
    expected_keys |= {"value_field", "weight_field"} if kind == "group_weighted_mean" else {"field_id"}
    required_keys = expected_keys - ({"group_values"} if kind == "group_weighted_mean" else set())
    if set(expression) - expected_keys or required_keys - set(expression):
        raise DefinitionError("derived query reducer fields are invalid")
    return expression, kind, scope


def _derived_sources(expression, kind, fields, permitted, collection, path):
    from process.custom_import.definition import DefinitionError, _identifier

    source_id = _identifier(expression.get("value_field", expression.get("field_id")), f"{path}.source")
    weight_id = (
        _identifier(expression["weight_field"], f"{path}.weight_field") if kind == "group_weighted_mean" else None
    )
    for field_id in (source_id,) if weight_id is None else (source_id, weight_id):
        field = fields.get(field_id)
        if field_id not in permitted or field is None or field.collection != collection:
            raise DefinitionError("derived query sources must be permitted fields in the declared scope")
        if field.value_type not in _NUMERIC_TYPES:
            raise DefinitionError("derived query sources must be numeric")
    return source_id, weight_id


def _derived_groups(expression, selection, path):
    from process.custom_import.definition import DefinitionError, _array

    groups = tuple(_array(expression.get("group_values", list(selection.group_values)), f"{path}.group_values"))
    if (
        not groups
        or len(groups) > len(selection.group_values)
        or any(type(group) is not str or group not in selection.group_values for group in groups)
        or len(set(groups)) != len(groups)
    ):
        raise DefinitionError("derived query groups must be unique declared family values")
    return groups


def _derived_scope(document, scope, children, query_child_collection, child_query_fields):
    from process.custom_import.definition import DefinitionError, _array, _identifier

    if scope == "root":
        if "collection" in document or "complete_child_key" in document:
            raise DefinitionError("root derived query fields cannot carry a child scope")
        return None, ()
    collection = _identifier(document.get("collection"), "derived query collection")
    child_keys = tuple(
        _identifier(field_id, "derived query complete child key")
        for field_id in _array(document.get("complete_child_key"), "derived query complete child key")
    )
    declared = next((child for child in children if child.name == collection), None)
    if (
        collection != query_child_collection
        or declared is None
        or child_keys != declared.child_key
        or not set(child_keys).issubset(child_query_fields)
    ):
        raise DefinitionError("derived query child scope requires its complete projected key")
    return collection, child_keys


def reduce_expression(
    field: DerivedQueryField,
    values_by_group: Mapping[str, object],
    weights_by_group: Mapping[str, object] | None = None,
):
    """Reduce SQL scalars already bound to one entity, selected value and key."""

    from sqlalchemy import BigInteger, Numeric, and_, case, cast, func, literal

    scalar_type = BigInteger() if field.value_type == "integer" else Numeric(30, 12)
    value_by_group = {
        group: values_by_group.get(group, literal(None, type_=scalar_type)) for group in field.group_values
    }
    if field.kind == "preferred_non_null":
        return func.coalesce(*(value_by_group[group] for group in field.group_values), type_=scalar_type)
    if field.kind != "group_weighted_mean" or weights_by_group is None:
        raise ValueError("weighted derived query requires source weights")
    numerator = literal(0, type_=Numeric())
    denominator = literal(0, type_=Numeric())
    for group in field.group_values:
        value = cast(value_by_group[group], Numeric()) * _SCALE
        weight = cast(weights_by_group.get(group, literal(None, type_=Numeric())), Numeric()) * _SCALE
        eligible = and_(value.is_not(None), weight > 0)
        numerator += case((eligible, value * weight), else_=0)
        denominator += case((eligible, weight), else_=0)
    return _half_even_mean(numerator, denominator)


def _half_even_mean(numerator, denominator):
    """Match twelve-place template rounding without PostgreSQL's tie rule."""

    from sqlalchemy import Numeric, and_, case, func, literal, or_

    denominator = func.nullif(denominator, 0)
    absolute = func.abs(numerator)
    quotient = func.div(absolute, denominator)
    twice_remainder = func.mod(absolute, denominator) * 2
    increment = case(
        (
            or_(
                twice_remainder > denominator,
                and_(twice_remainder == denominator, func.mod(quotient, 2) == 1),
            ),
            1,
        ),
        else_=0,
    )
    rounded = quotient + increment
    signed = case((numerator < 0, -rounded), else_=rounded)
    # Multiplication preserves all twelve places even at the integer range boundary.
    return signed * literal(Decimal("0.000000000001"), type_=Numeric())
