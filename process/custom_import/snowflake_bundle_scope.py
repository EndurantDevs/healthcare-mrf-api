# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fixed source predicates and one family-preserving entity cohort per SELECT."""

from __future__ import annotations

MAX_ENTITY_LIMIT = 1_000_000

_COHORT = '"__ci_entity_cohort"'
_ENTITY = '"__ci_entity_id"'


def _entity_limit(value, definition=None):
    if value is not None and (type(value) is not int or not 1 <= value <= MAX_ENTITY_LIMIT):
        raise ValueError("entity limit must be an integer from 1 through 1000000")
    if value is not None and definition is not None and definition.entity_field not in definition.root_logical_key:
        raise ValueError("entity cohort requires the entity field in the root logical key")
    return value


def _quoted(identifier):
    return f'"{identifier}"'


def _resolved_row_filters(binding, columns):
    """Use physical predicates for both SQL ordering and shared-source identity."""

    columns_by_field = {column.field_id: column.column_identifier for column in columns}
    try:
        return tuple(
            sorted(
                (columns_by_field[item.field_id], item.operator, (item.value,) if item.operator == "eq" else item.value)
                for item in binding.row_filters
            )
        )
    except KeyError as exc:
        from process.custom_import.snowflake_bundle import SnowflakeBundleError

        raise SnowflakeBundleError("source filter field has no approved column") from exc


def _row_filter_sql(binding, columns, *, alias=""):
    prefix = f"{alias}." if alias else ""
    return " AND ".join(
        f"{prefix}{_quoted(column)} " + ("= %s" if operator == "eq" else f"IN ({', '.join('%s' for _ in operands)})")
        for column, operator, operands in _resolved_row_filters(binding, columns)
    )


def _row_filter_parameters(binding, columns):
    return tuple(
        value for _column, _operator, operands in _resolved_row_filters(binding, columns) for value in operands
    )


def _entity_column(request, binding, columns):
    """Derive child membership from the complete declared parent-key mapping."""

    definition = request.definition
    stream = next(stream for stream in definition.source_streams if stream.stream_id == binding.stream_id)
    field_id = definition.entity_field
    if stream.record_kind != "root":
        collection = definition.collections_by_name[stream.child_collection]
        field_id = next(part.child_field for part in collection.parent_key if part.root_field == field_id)
    return next(column.column_identifier for column in columns if column.field_id == field_id)


def _bundle_source_key(request, binding, columns):
    if request.processing_policy is None:
        return binding.stream_id
    key = binding.relation, _resolved_row_filters(binding, columns)
    return key if request.entity_limit is None else (*key, _entity_column(request, binding, columns))


def _filtered_relation_sql(binding, columns, *, request=None):
    predicate = _row_filter_sql(binding, columns)
    if request is not None and request.entity_limit is not None:
        membership = f"{_quoted(_entity_column(request, binding, columns))} IN (SELECT {_ENTITY} FROM {_COHORT})"
        predicate = f"{predicate} AND {membership}" if predicate else membership
    return binding.relation.quoted_sql + (f" WHERE {predicate}" if predicate else "")


def _cohort_source(request, selected_columns_by_stream):
    root = next(stream for stream in request.definition.source_streams if stream.record_kind == "root")
    return next(
        (binding, columns)
        for binding, columns in zip(request.bindings, selected_columns_by_stream, strict=True)
        if binding.stream_id == root.stream_id
    )


def _cohort_cte(request, selected_columns_by_stream):
    """Limit distinct entity IDs only; every matching family row remains in scope."""

    if request.entity_limit is None:
        return ""
    binding, columns = _cohort_source(request, selected_columns_by_stream)
    column = _quoted(_entity_column(request, binding, columns))
    relation = _filtered_relation_sql(binding, columns)
    conjunction = " AND " if binding.row_filters else " WHERE "
    return (
        f"{_COHORT} AS (SELECT DISTINCT {column} AS {_ENTITY} FROM {relation}"
        f"{conjunction}{column} IS NOT NULL ORDER BY {_ENTITY} ASC LIMIT {request.entity_limit})"
    )


def _cohort_parameters(request, selected_columns_by_stream):
    if request.entity_limit is None:
        return ()
    return _row_filter_parameters(*_cohort_source(request, selected_columns_by_stream))


def _bundle_parameters(request, selected_columns_by_stream):
    seen_sources = set()
    parameters = list(_cohort_parameters(request, selected_columns_by_stream))
    for binding, columns in zip(request.bindings, selected_columns_by_stream, strict=True):
        key = _bundle_source_key(request, binding, columns)
        if key not in seen_sources:
            seen_sources.add(key)
            parameters.extend(_row_filter_parameters(binding, columns))
    return tuple(parameters)
