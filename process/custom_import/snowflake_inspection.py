# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Import-safe metadata discovery and source counts for approved bindings.

Counts describe the approved source relations at one statement, not the rows
that will survive import admission. No source records are returned.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from math import ceil
from time import monotonic

from process.custom_import.definition import canonical_json
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleStatement,
    _filtered_relation_sql,
    _row_filter_parameters,
    _validated_bundle_statement,
)
from process.custom_import.snowflake_preflight import (
    SnowflakePreflightError,
    SnowflakePreflightLimits,
    _prepare_preflight,
)
from process.custom_import.snowflake_preflight_schema import (
    _is_integral_fixed,
    _source_type,
    _supports_preflight_field_type,
)
from process.custom_import.snowflake_bundle_scope import _cohort_cte, _cohort_parameters

INSPECTION_ERROR_CODES = frozenset({"byte_limit", "query_timeout", "query_unavailable", "result_invalid"})
_COUNT_COLUMNS = ("__ci_estimate_stream_ordinal", "__ci_estimate_rows")


class SnowflakeInspectionError(ValueError):
    """A closed error code without source or driver diagnostics."""

    def __init__(self, code: str) -> None:
        self.code = code if code in INSPECTION_ERROR_CODES else "query_unavailable"
        super().__init__(self.code)


@dataclass(frozen=True)
class SnowflakeInspectionStatement:
    """Only generated, approved-relation SELECTs; no raw SQL constructor."""

    bundle_statement: SnowflakeBundleStatement = field(repr=False)
    operation: str
    stream_ordinal: int | None = None
    sql: str = field(init=False, repr=False)
    parameters: tuple[str, ...] = field(init=False, repr=False)
    column_ids: tuple[str, ...] = field(init=False)

    def __post_init__(self) -> None:
        bundle = _validated_bundle_statement(self.bundle_statement)
        bindings = bundle.request.bindings
        if self.operation == "discover":
            if type(self.stream_ordinal) is not int or not 0 <= self.stream_ordinal < len(bindings):
                raise SnowflakeInspectionError("result_invalid")
            columns = bundle.selected_columns_by_stream[self.stream_ordinal]
            selected = ", ".join(f'"{column.column_identifier}" AS "{column.field_id}"' for column in columns)
            binding = bindings[self.stream_ordinal]
            sql = f"SELECT {selected} FROM {_filtered_relation_sql(binding, columns, request=bundle.request)} LIMIT 0"
            parameters = _row_filter_parameters(binding, columns)
            column_ids = tuple(column.field_id for column in columns)
        elif self.operation == "estimate" and self.stream_ordinal is None:
            sql = (
                " UNION ALL ".join(
                    f'SELECT {ordinal} AS "{_COUNT_COLUMNS[0]}", COUNT(*) AS "{_COUNT_COLUMNS[1]}" '
                    f"FROM {_filtered_relation_sql(binding, bundle.selected_columns_by_stream[ordinal], request=bundle.request)}"
                    for ordinal, binding in enumerate(bindings)
                )
                + f' ORDER BY "{_COUNT_COLUMNS[0]}"'
            )
            column_ids = _COUNT_COLUMNS
            parameters = tuple(
                filter_value
                for binding, columns in zip(bindings, bundle.selected_columns_by_stream, strict=True)
                for filter_value in _row_filter_parameters(binding, columns)
            )
        else:
            raise SnowflakeInspectionError("result_invalid")
        cohort = _cohort_cte(bundle.request, bundle.selected_columns_by_stream)
        if cohort:
            sql = f"WITH {cohort} {sql}"
            parameters = _cohort_parameters(bundle.request, bundle.selected_columns_by_stream) + parameters
        object.__setattr__(self, "sql", sql)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "column_ids", column_ids)

    def validated(self) -> SnowflakeInspectionStatement:
        """Rebuild the nested seals and generated SQL before driver execution."""

        rebuilt = SnowflakeInspectionStatement(self.bundle_statement, self.operation, self.stream_ordinal)
        if rebuilt != self:
            raise SnowflakeInspectionError("result_invalid")
        return rebuilt


def inspect_snowflake_bundle(
    definition: object,
    binding: object,
    connector: object,
    adapter: object,
    *,
    operation: str,
    limits: SnowflakePreflightLimits = SnowflakePreflightLimits(),
    clock: Callable[[], float] = monotonic,
) -> str:
    """Return bounded canonical JSON containing metadata or exact source counts.

    The injected adapter owns each cursor and enforces its statement timeout.
    A count may scan the full relation: neither elapsed time nor the small
    result bounds warehouse scan bytes or credits. Errors discard all results.
    """

    try:
        prepared = _prepare_preflight(definition, binding, connector, limits)
    except SnowflakePreflightError:
        raise SnowflakeInspectionError("result_invalid") from None
    limits = prepared.statement.limits
    bundle = prepared.statement.bundle_statement
    if operation not in {"discover", "estimate"}:
        raise SnowflakeInspectionError("result_invalid")
    deadline = clock() + limits.maximum_elapsed_seconds
    fields_by_id = {field.field_id: field for field in prepared.definition.fields}
    streams = []
    statements = (
        tuple(
            SnowflakeInspectionStatement(bundle, operation, ordinal) for ordinal in range(len(bundle.request.bindings))
        )
        if operation == "discover"
        else (SnowflakeInspectionStatement(bundle, operation),)
    )
    try:
        for statement in statements:
            streams.extend(_read_inspection_statement(statement, adapter, fields_by_id, deadline, clock))
        _remaining_seconds(deadline, clock)
    except SnowflakeInspectionError:
        raise
    except Exception:
        raise SnowflakeInspectionError("query_unavailable") from None

    return _render_inspection(prepared, operation, streams)


def _read_inspection_statement(statement, adapter, fields_by_id, deadline, clock):
    """Own one cursor and preserve its primary failure through cleanup."""

    cursor = adapter.open_inspection(statement, timeout_seconds=_remaining_seconds(deadline, clock))
    has_primary_failure = False
    try:
        _remaining_seconds(deadline, clock)
        description = cursor.description
        if (
            not isinstance(description, Sequence)
            or len(description) != len(statement.column_ids)
            or tuple(getattr(column, "name", None) for column in description) != statement.column_ids
        ):
            raise SnowflakeInspectionError("result_invalid")
        if statement.operation == "discover":
            streams = [_discover_stream(statement, cursor, fields_by_id)]
        else:
            streams = _estimate_streams(statement, cursor, deadline, clock)
        _remaining_seconds(deadline, clock)
        return streams
    except BaseException:
        has_primary_failure = True
        raise
    finally:
        try:
            cursor.close()
        except Exception:
            if not has_primary_failure:
                raise SnowflakeInspectionError("query_unavailable") from None


def _discover_stream(statement, cursor, fields_by_id):
    """Describe approved logical fields without fetching source records."""

    selected_fields = []
    field_types = getattr(cursor, "field_types", ())
    conversions = statement.bundle_statement.request.decimal_conversions or {}
    for field_id, metadata in zip(statement.column_ids, cursor.description, strict=True):
        try:
            source_type = _source_type(metadata, field_types=field_types)
        except SnowflakeConnectorError:
            source_type = "unknown"
        selected_fields.append(
            {
                "field_id": field_id,
                "source_type": source_type,
                "runtime_supported": (
                    fields_by_id[field_id].value_type == "decimal" and source_type == "REAL"
                    if field_id in conversions
                    else _supports_preflight_field_type(fields_by_id[field_id].value_type, source_type)
                ),
            }
        )
    return {
        "stream_id": statement.bundle_statement.request.bindings[statement.stream_ordinal].stream_id,
        "selected_fields": selected_fields,
    }


def _estimate_streams(statement, cursor, deadline, clock):
    """Read one exact, bounded count for every approved stream ordinal."""

    field_types = getattr(cursor, "field_types", ())
    try:
        if not all(
            _is_integral_fixed(_source_type(metadata, field_types=field_types)) for metadata in cursor.description
        ):
            raise SnowflakeInspectionError("result_invalid")
    except SnowflakeConnectorError:
        raise SnowflakeInspectionError("result_invalid") from None
    streams = []
    for ordinal, stream_binding in enumerate(statement.bundle_statement.request.bindings):
        count_row = cursor.fetchone()
        _remaining_seconds(deadline, clock)
        if (
            not isinstance(count_row, Sequence)
            or isinstance(count_row, str | bytes)
            or len(count_row) != 2
            or type(count_row[0]) is not int
            or count_row[0] != ordinal
            or type(count_row[1]) is not int
            or not 0 <= count_row[1] < 10**38
        ):
            raise SnowflakeInspectionError("result_invalid")
        streams.append({"stream_id": stream_binding.stream_id, "source_rows": count_row[1], "precision": "exact"})
    if cursor.fetchone() is not None:
        raise SnowflakeInspectionError("result_invalid")
    return streams


def _render_inspection(prepared, operation, streams):
    """Render only scoped observations within the successful receipt byte limit."""

    limits = prepared.statement.limits
    result_dict = {
        "contract": "custom-import-inspection/v1",
        "operation": operation,
        "status": "complete",
        "definition_sha256": prepared.definition.digest,
        "schema_sha256": prepared.definition.schema_digest,
        "source_binding_sha256": prepared.binding.digest,
        "limits": {
            "maximum_elapsed_seconds": limits.maximum_elapsed_seconds,
            "maximum_total_bytes": limits.maximum_total_bytes,
        },
        "scope": "selected_fields_at_each_statement"
        if operation == "discover"
        else "approved_relations_at_one_statement",
        "streams": streams,
    }
    if operation == "estimate":
        result_dict["estimates"] = {
            metric: {"precision": "unknown", "value": None}
            for metric in ("import_rows", "storage_bytes", "warehouse_credits")
        }
        result_dict["warehouse_scan_bounded"] = False
    rendered = canonical_json(result_dict)
    if len(rendered.encode("utf-8")) > limits.maximum_total_bytes:
        raise SnowflakeInspectionError("byte_limit")
    return rendered


def _remaining_seconds(deadline: float, clock: Callable[[], float]) -> int:
    remaining = deadline - clock()
    if remaining <= 0:
        raise SnowflakeInspectionError("query_timeout")
    return ceil(remaining)
