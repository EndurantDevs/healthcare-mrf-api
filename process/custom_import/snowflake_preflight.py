# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded, private in-process previews for declarative Snowflake bundles.

The module builds one preview-only SELECT from an already approved binding.  It
does not open credentials, print rows, or provide a transport for samples.
Hosts inject the cursor-owning adapter at the boundary where they can enforce a
statement timeout and dispose of the cursor.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from time import monotonic
from typing import Any, Protocol

from process.custom_import.definition import CustomImportDefinition, DefinitionError, Field
from process.custom_import.family import (
    MAX_SCALAR_INTEGER,
    MIN_SCALAR_INTEGER,
    FamilyRejection,
    RootFamily,
    SourceSnapshotError,
    assemble_root_families,
    validate_source_snapshot_tokens,
)
from process.custom_import.snowflake import (
    SnowflakeApprovedRelation,
    SnowflakeConnectorError,
    SnowflakeDeclaredColumn,
)
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleBinding,
    SnowflakeBundleError,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
    SnowflakeBundleStatementBuilder,
    _filtered_relation_sql,
    _query_identity_snapshot_token,
    _row_filter_parameters,
    _row_filter_sql,
    _snapshot_token_expression,
    _validated_bundle_statement,
)
from process.custom_import.snowflake_preflight_schema import convert_snowflake_float
from process.custom_import.snowflake_bundle_scope import _cohort_cte, _cohort_parameters

DEFAULT_MAX_ROOT_KEYS = 32
DEFAULT_MAX_CHILD_ROWS = 256
DEFAULT_MAX_TOTAL_BYTES = 1_048_576
DEFAULT_MAX_ELAPSED_SECONDS = 30
MAX_ROOT_KEYS = 1_024
MAX_CHILD_ROWS = 10_000
MAX_TOTAL_BYTES = 16 * 1_024 * 1_024
MAX_ELAPSED_SECONDS = 120

_KIND_COLUMN = "__ci_preflight_kind"
_STREAM_ORDINAL_COLUMN = "__ci_preflight_stream_ordinal"
_STREAM_ID_COLUMN = "__ci_preflight_stream_id"
_KEY_ORDINAL_COLUMN = "__ci_preflight_key_ordinal"
_TOKEN_COLUMN = "__ci_preflight_source_snapshot_token"
_MULTIPLICITY_COLUMN = "__ci_preflight_key_multiplicity"
_METADATA_KIND = 0
_KEY_KIND = 1
_DATA_KIND = 2
_BYTE_LIMIT_KIND = 3
_RAW_CTE = "__ci_preflight_raw"
_WIRE_CTE = "__ci_preflight_wire"
_WIRE_BYTES_COLUMN = "__ci_preflight_wire_bytes"
_PRECISIONS = frozenset({"exact", "lower_bound", "unknown"})


def _is_positive_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _bounded_int(value: object, label: str, *, maximum: int) -> None:
    if not _is_positive_int(value) or value > maximum:
        raise SnowflakePreflightError(f"preflight {label} is invalid")


class SnowflakePreflightError(ValueError):
    """The caller supplied an invalid bounded preview contract."""


class SnowflakePreflightCursor(Protocol):
    """One adapter-owned, single-use result cursor for a generated preview."""

    column_ids: tuple[str, ...]
    query_id: str | None

    def fetchone(self) -> Sequence[object] | None:
        """Return one result row or ``None`` after the generated SELECT ends."""

    def close(self) -> None:
        """Release the exact cursor and any connection it owns."""


class SnowflakePreflightAdapter(Protocol):
    """Execute only a generated preview statement with a bounded timeout."""

    def open_preflight(
        self,
        statement: SnowflakePreflightStatement,
        *,
        timeout_seconds: int,
    ) -> SnowflakePreflightCursor:
        """Open one cursor; no raw SQL or caller-provided parameters are accepted."""


@dataclass(frozen=True)
class SnowflakePreflightLimits:
    """Small, hard ceilings for one in-process preview response."""

    maximum_root_keys: int = DEFAULT_MAX_ROOT_KEYS
    maximum_child_rows: int = DEFAULT_MAX_CHILD_ROWS
    maximum_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES
    maximum_elapsed_seconds: int = DEFAULT_MAX_ELAPSED_SECONDS

    def __post_init__(self) -> None:
        _bounded_int(self.maximum_root_keys, "root-key limit", maximum=MAX_ROOT_KEYS)
        _bounded_int(self.maximum_child_rows, "child-row limit", maximum=MAX_CHILD_ROWS)
        _bounded_int(self.maximum_total_bytes, "byte limit", maximum=MAX_TOTAL_BYTES)
        _bounded_int(self.maximum_elapsed_seconds, "elapsed-time limit", maximum=MAX_ELAPSED_SECONDS)


@dataclass(frozen=True)
class SnowflakePreflightValidation:
    """Definition, mapping, and current capture-runtime compatibility facts."""

    definition_valid: bool
    mapping_valid: bool
    runtime_supported: bool


@dataclass(frozen=True)
class SnowflakePreflightStreamObservation:
    """A scoped query observation, never a warehouse cardinality or cost estimate."""

    stream_id: str
    observed_rows: int
    observed_bytes: int
    precision: str

    def __post_init__(self) -> None:
        if not isinstance(self.stream_id, str) or not self.stream_id:
            raise SnowflakePreflightError("preflight stream observation is invalid")
        if (
            any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in (self.observed_rows, self.observed_bytes)
            )
            or self.precision not in _PRECISIONS
        ):
            raise SnowflakePreflightError("preflight stream observation is invalid")


@dataclass(frozen=True)
class SnowflakePreflightSample:
    """A complete selected-key family sample retained only in process memory."""

    source_snapshot_token: str
    families: tuple[RootFamily, ...] = field(repr=False)


@dataclass(frozen=True)
class SnowflakePreflightRejectionDiagnostic:
    """One selected-root admission failure without exposing its source record."""

    root_key: tuple[object, ...] = field(repr=False)
    code: str

    def __post_init__(self) -> None:
        if not _has_valid_key(self.root_key) or not _is_generic_rejection_code(self.code):
            raise SnowflakePreflightError("preflight rejection diagnostic is invalid")


@dataclass(frozen=True)
class SnowflakePreflightResult:
    """Validation and bounded observation result without any logging side effect."""

    definition_sha256: str | None
    schema_sha256: str | None
    source_binding_sha256: str | None
    validation: SnowflakePreflightValidation
    observations: tuple[SnowflakePreflightStreamObservation, ...]
    observed_bytes: int
    status: str
    unavailable_reason: str | None = None
    sample: SnowflakePreflightSample | None = field(default=None, repr=False)
    rejection_diagnostics: tuple[SnowflakePreflightRejectionDiagnostic, ...] = field(default=(), repr=False)

    def __post_init__(self) -> None:
        if self.status not in {"complete", "unavailable"}:
            raise SnowflakePreflightError("preflight result status is invalid")
        if isinstance(self.observed_bytes, bool) or not isinstance(self.observed_bytes, int) or self.observed_bytes < 0:
            raise SnowflakePreflightError("preflight observed bytes are invalid")
        if self.status == "complete":
            if self.sample is None or self.unavailable_reason is not None or self.rejection_diagnostics:
                raise SnowflakePreflightError("complete preflight result is invalid")
        elif self.sample is not None or not self.unavailable_reason:
            raise SnowflakePreflightError("unavailable preflight result is invalid")
        if not isinstance(self.rejection_diagnostics, tuple) or not all(
            isinstance(item, SnowflakePreflightRejectionDiagnostic) for item in self.rejection_diagnostics
        ):
            raise SnowflakePreflightError("preflight rejection diagnostics are invalid")


@dataclass(frozen=True)
class SnowflakePreflightStatement:
    """A preview-only SELECT derived from one validated bundle statement."""

    bundle_statement: SnowflakeBundleStatement = field(repr=False)
    limits: SnowflakePreflightLimits = field(repr=False)
    sql: str = field(init=False)
    parameters: tuple[str, ...] = field(init=False, repr=False)
    column_ids: tuple[str, ...] = field(init=False)
    definition: CustomImportDefinition = field(init=False, repr=False)
    bundle_bindings: tuple[SnowflakeBundleBinding, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Derive the sole preview SQL from the sealed bundle statement inputs."""

        try:
            bundle_statement = _validated_bundle_statement(self.bundle_statement)
            limits = SnowflakePreflightLimits(
                maximum_root_keys=self.limits.maximum_root_keys,
                maximum_child_rows=self.limits.maximum_child_rows,
                maximum_total_bytes=self.limits.maximum_total_bytes,
                maximum_elapsed_seconds=self.limits.maximum_elapsed_seconds,
            )
            if limits != self.limits:
                raise ValueError
            definition = _canonical_definition(bundle_statement.request.definition)
            fields = tuple(sorted(definition.fields, key=lambda field: field.field_slot))
            root_stream = next(stream for stream in definition.source_streams if stream.record_kind == "root")
            binding_by_stream = {binding.stream_id: binding for binding in bundle_statement.request.bindings}
            columns_by_stream = {
                binding.stream_id: {column.field_id: column for column in selected_columns}
                for binding, selected_columns in zip(
                    bundle_statement.request.bindings,
                    bundle_statement.selected_columns_by_stream,
                    strict=True,
                )
            }
            root_binding = binding_by_stream[root_stream.stream_id]
            ctes = _scoped_key_ctes(bundle_statement, root_binding, columns_by_stream[root_stream.stream_id], limits)
            columns = _output_columns(fields)
            sql = _preview_sql(
                ctes,
                _preview_branches(definition, bundle_statement, binding_by_stream, columns_by_stream, fields, limits),
                columns,
                fields,
                limits,
            )
        except (
            AttributeError,
            SnowflakeBundleError,
            SnowflakePreflightError,
            StopIteration,
            TypeError,
            ValueError,
        ) as exc:
            raise SnowflakePreflightError("preflight statement is invalid") from exc
        object.__setattr__(self, "bundle_statement", bundle_statement)
        object.__setattr__(self, "limits", limits)
        object.__setattr__(self, "definition", definition)
        object.__setattr__(self, "bundle_bindings", bundle_statement.request.bindings)
        object.__setattr__(self, "column_ids", columns)
        object.__setattr__(self, "sql", sql)
        object.__setattr__(
            self,
            "parameters",
            _cohort_parameters(bundle_statement.request, bundle_statement.selected_columns_by_stream)
            + _preflight_parameters((root_binding, *bundle_statement.request.bindings), columns_by_stream),
        )


def _scoped_key_ctes(bundle, root_binding, columns_by_field, limits):
    ctes = _root_key_ctes(
        bundle.request.definition,
        _filtered_relation_sql(root_binding, tuple(columns_by_field.values()), request=bundle.request),
        columns_by_field,
        limits,
    )
    cohort = _cohort_cte(bundle.request, bundle.selected_columns_by_stream)
    return (cohort + ", ", *ctes) if cohort else ctes


def _preflight_parameters(bindings, columns_by_stream) -> tuple[str, ...]:
    """Root predicates occur once for keys and again in the root data branch."""

    return tuple(
        value
        for binding in bindings
        for value in _row_filter_parameters(binding, tuple(columns_by_stream[binding.stream_id].values()))
    )


@dataclass(frozen=True)
class _PreparedPreflight:
    definition: CustomImportDefinition
    binding: SnowflakeSourceBinding
    statement: SnowflakePreflightStatement


@dataclass
class _ObservedRows:
    metadata_tokens: dict[str, object]
    key_candidates: list[tuple[int, int, tuple[object, ...]]]
    records_by_stream: dict[str, list[dict[str, object]]]
    bytes_by_stream: dict[str, int]
    observed_bytes: int


def preflight_snowflake_bundle(
    definition: object,
    binding: object,
    connector: object,
    adapter: SnowflakePreflightAdapter,
    *,
    limits: SnowflakePreflightLimits = SnowflakePreflightLimits(),
    clock: Callable[[], float] = monotonic,
) -> SnowflakePreflightResult:
    """Validate and execute one bounded selected-key preview without source logging.

    A root-key ``R + 1`` sentinel selects a deterministic sample and only makes
    the root observation a lower bound.  A child ``C + 1`` sentinel, byte cap,
    timeout, inconsistent snapshot, or incomplete family discards the sample.
    """

    try:
        prepared = _prepare_preflight(definition, binding, connector, limits)
    except SnowflakePreflightError as exc:
        if str(exc) == "definition":
            return _invalid_result(definition, binding, "definition_invalid")
        return _invalid_result(
            definition,
            binding,
            "limits_invalid" if str(exc) == "limits" else "mapping_invalid",
            definition_valid=True,
        )

    unsupported = any(field.value_type in {"date", "timestamp"} for field in prepared.definition.fields)
    if unsupported:
        return _unavailable(
            prepared,
            observations=_unknown_observations(prepared.definition),
            observed_bytes=0,
            reason="runtime_type_unsupported",
            runtime_supported=False,
        )
    return _execute_preflight(prepared, adapter, clock)


def _prepare_preflight(
    definition_value: object,
    binding_value: object,
    builder: object,
    limits: SnowflakePreflightLimits,
) -> _PreparedPreflight:
    definition = _canonical_definition(definition_value)
    if not isinstance(limits, SnowflakePreflightLimits):
        raise SnowflakePreflightError("limits")
    if not isinstance(binding_value, SnowflakeSourceBinding):
        raise SnowflakePreflightError("mapping")
    try:
        binding = SnowflakeSourceBinding.from_json(binding_value.canonical)
        if binding != binding_value:
            raise ValueError
        approved_relations, bundle_bindings = binding.bundle_components(definition)
        if not isinstance(builder, SnowflakeBundleStatementBuilder):
            raise TypeError
        request = builder.prepare_request(
            definition,
            bindings=bundle_bindings,
            processing_policy=binding.processing_policy,
            snapshot_token_mode=binding.snapshot_token_mode,
            decimal_conversions=binding.decimal_conversions,
            entity_limit=binding.entity_limit,
        )
        if not isinstance(request, SnowflakeBundleRequest):
            raise TypeError
        expected_request = SnowflakeBundleRequest(
            definition=definition,
            bindings=bundle_bindings,
            encoding=request.encoding,
            capture_limits=request.capture_limits,
            processing_policy=binding.processing_policy,
            snapshot_token_mode=binding.snapshot_token_mode,
            decimal_conversions=binding.decimal_conversions,
            entity_limit=binding.entity_limit,
        )
        bundle_statement = builder.build_statement(request)
        if not isinstance(bundle_statement, SnowflakeBundleStatement):
            raise TypeError
        _validate_statement_mapping(bundle_statement, request, expected_request, approved_relations)
    except (
        AttributeError,
        DefinitionError,
        SnowflakeBundleError,
        SnowflakeConnectorError,
        SnowflakeSourceBindingError,
        TypeError,
        ValueError,
    ) as exc:
        raise SnowflakePreflightError("mapping") from exc
    return _PreparedPreflight(
        definition=definition,
        binding=binding,
        statement=_preflight_statement(bundle_statement, limits),
    )


def _validate_statement_mapping(
    statement: SnowflakeBundleStatement,
    actual_request: SnowflakeBundleRequest,
    expected_request: SnowflakeBundleRequest,
    approved_relations: tuple[SnowflakeApprovedRelation, ...],
) -> None:
    """Require the connector statement to retain the binding's exact columns."""

    if actual_request != expected_request or statement.request != expected_request:
        raise ValueError
    approved_by_relation = {approved.relation.parts: approved for approved in approved_relations}
    for binding, selected_columns, snapshot_column in zip(
        expected_request.bindings,
        statement.selected_columns_by_stream,
        statement.source_snapshot_token_columns_by_stream,
        strict=True,
    ):
        if selected_columns != _approved_columns(approved_by_relation, binding.relation, binding.selected_field_ids):
            raise ValueError
        if binding.source_snapshot_token_relation is None:
            if snapshot_column is not None:
                raise ValueError
            continue
        if binding.semantic_token_metadata_key is None:
            raise ValueError
        expected_snapshot = _approved_columns(
            approved_by_relation,
            binding.source_snapshot_token_relation,
            (binding.semantic_token_metadata_key,),
        )[0]
        if snapshot_column != expected_snapshot:
            raise ValueError


def _approved_columns(
    approved_by_relation: Mapping[tuple[str, str, str], SnowflakeApprovedRelation],
    relation,
    field_ids: tuple[str, ...],
) -> tuple[SnowflakeDeclaredColumn, ...]:
    approved = approved_by_relation.get(relation.parts)
    if approved is None:
        raise ValueError
    columns = tuple(approved.column_for(field_id) for field_id in field_ids)
    if any(column is None for column in columns):
        raise ValueError
    return tuple(column for column in columns if column is not None)


def _canonical_definition(value: object) -> CustomImportDefinition:
    if not isinstance(value, CustomImportDefinition):
        raise SnowflakePreflightError("definition")
    try:
        canonical = CustomImportDefinition.from_json(value.canonical)
    except (AttributeError, DefinitionError, TypeError, ValueError) as exc:
        raise SnowflakePreflightError("definition") from exc
    if canonical != value:
        raise SnowflakePreflightError("definition")
    return canonical


def _preflight_statement(
    bundle_statement: SnowflakeBundleStatement,
    limits: SnowflakePreflightLimits,
) -> SnowflakePreflightStatement:
    return SnowflakePreflightStatement(bundle_statement=bundle_statement, limits=limits)


def _root_key_ctes(
    definition: CustomImportDefinition,
    root_relation_sql: str,
    root_columns: Mapping[str, SnowflakeDeclaredColumn],
    limits: SnowflakePreflightLimits,
) -> tuple[str, str, str]:
    key_select = ", ".join(
        f"{_quoted(root_columns[field_id].column_identifier)} AS {_quoted(field_id)}"
        for field_id in definition.root_logical_key
    )
    key_group = ", ".join(_quoted(root_columns[field_id].column_identifier) for field_id in definition.root_logical_key)
    key_order = ", ".join(f"{_quoted(field_id)} ASC NULLS FIRST" for field_id in definition.root_logical_key)
    return (
        f"{_quoted('__ci_preflight_key_groups')} AS ("
        f"SELECT {key_select}, COUNT(*) AS {_quoted(_MULTIPLICITY_COLUMN)} "
        f"FROM {root_relation_sql} GROUP BY {key_group}"
        "), "
        f"{_quoted('__ci_preflight_key_candidates')} AS ("
        f"SELECT *, ROW_NUMBER() OVER (ORDER BY {key_order}) AS {_quoted(_KEY_ORDINAL_COLUMN)} "
        f"FROM (SELECT * FROM {_quoted('__ci_preflight_key_groups')} ORDER BY {key_order} "
        f"LIMIT {limits.maximum_root_keys + 1}) AS {_quoted('__ci_preflight_limited_keys')}"
        "), "
        f"{_quoted('__ci_preflight_selected_keys')} AS ("
        f"SELECT {', '.join(_quoted(field_id) for field_id in definition.root_logical_key)} "
        f"FROM {_quoted('__ci_preflight_key_candidates')} "
        f"WHERE {_quoted(_KEY_ORDINAL_COLUMN)} <= {limits.maximum_root_keys} "
        f"AND {_quoted(_MULTIPLICITY_COLUMN)} = 1"
        ")",
    )


def _preview_branches(
    definition: CustomImportDefinition,
    bundle_statement: SnowflakeBundleStatement,
    binding_by_stream: Mapping[str, SnowflakeBundleBinding],
    columns_by_stream: Mapping[str, Mapping[str, SnowflakeDeclaredColumn]],
    fields: tuple[Field, ...],
    limits: SnowflakePreflightLimits,
) -> tuple[str, ...]:
    metadata_branches = tuple(
        _metadata_branch(bundle_statement, ordinal, fields)
        for ordinal in range(1, len(bundle_statement.request.bindings) + 1)
    )
    data_branches = tuple(
        _data_branch(
            definition,
            source_stream,
            binding_by_stream[source_stream.stream_id],
            columns_by_stream[source_stream.stream_id],
            fields,
            ordinal,
            limits.maximum_child_rows,
        )
        for ordinal, source_stream in enumerate(definition.source_streams, start=1)
    )
    return (*metadata_branches, _key_branch(definition, fields), *data_branches)


def _output_columns(fields: tuple[Field, ...]) -> tuple[str, ...]:
    return (
        _KIND_COLUMN,
        _STREAM_ORDINAL_COLUMN,
        _STREAM_ID_COLUMN,
        _KEY_ORDINAL_COLUMN,
        _TOKEN_COLUMN,
        _MULTIPLICITY_COLUMN,
        *(field.field_id for field in fields),
    )


def _preview_sql(
    ctes: tuple[str, ...],
    branches: tuple[str, ...],
    columns: tuple[str, ...],
    fields: tuple[Field, ...],
    limits: SnowflakePreflightLimits,
) -> str:
    order = ", ".join(
        (
            f"{_quoted(_KIND_COLUMN)} ASC",
            f"{_quoted(_STREAM_ORDINAL_COLUMN)} ASC",
            f"{_quoted(_KEY_ORDINAL_COLUMN)} ASC NULLS FIRST",
            *(f"{_quoted(field.field_id)} ASC NULLS FIRST" for field in fields),
        )
    )
    selected_columns = ", ".join(_quoted(column) for column in columns)
    raw_cte = (
        f"{_quoted(_RAW_CTE)} AS (SELECT {selected_columns} FROM ({' UNION ALL '.join(branches)}) "
        f"AS {_quoted('__ci_preflight')})"
    )
    wire_cte = _wire_budget_cte(columns)
    wire_bytes = _quoted(_WIRE_BYTES_COLUMN)
    return (
        f"WITH {''.join(ctes)}, {raw_cte}, {wire_cte} "
        f"SELECT {selected_columns} FROM {_quoted(_RAW_CTE)} CROSS JOIN {_quoted(_WIRE_CTE)} "
        f"WHERE {wire_bytes} <= {limits.maximum_total_bytes} UNION ALL "
        f"{_byte_limit_branch(fields)} FROM {_quoted(_WIRE_CTE)} "
        f"WHERE {wire_bytes} > {limits.maximum_total_bytes} ORDER BY {order}"
    )


def _wire_budget_cte(columns: tuple[str, ...]) -> str:
    """Bound every raw result cell before the driver can materialize it."""

    row_cost = " + ".join(_wire_value_cost(column) for column in columns)
    schema_allowance = 4096 + sum(256 + len(column) for column in columns)
    return (
        f"{_quoted(_WIRE_CTE)} AS (SELECT COALESCE(SUM(2 + {row_cost}), 0) + {schema_allowance} "
        f"AS {_quoted(_WIRE_BYTES_COLUMN)} FROM {_quoted(_RAW_CTE)})"
    )


def _wire_value_cost(column: str) -> str:
    value = _quoted(column)
    return f"CASE WHEN {value} IS NULL THEN 5 ELSE 3 + 6 * OCTET_LENGTH(TO_VARCHAR({value})) END"


def _byte_limit_branch(fields: tuple[Field, ...]) -> str:
    return _select_branch(
        kind=_BYTE_LIMIT_KIND,
        stream_ordinal="NULL",
        stream_id="NULL",
        key_ordinal="NULL",
        token="NULL",
        multiplicity="NULL",
        field_values={field.field_id: "NULL" for field in fields},
        fields=fields,
    )


def _metadata_branch(
    statement: SnowflakeBundleStatement,
    ordinal: int,
    fields: tuple[Field, ...],
) -> str:
    binding = statement.request.bindings[ordinal - 1]
    snapshot_column = statement.source_snapshot_token_columns_by_stream[ordinal - 1]
    token = _snapshot_token_expression(
        binding.source_snapshot_token_relation, snapshot_column, processing_policy=statement.request.processing_policy
    )
    return _select_branch(
        kind=_METADATA_KIND,
        stream_ordinal=ordinal,
        stream_id=f"'{binding.stream_id}'",
        key_ordinal="NULL",
        token=token,
        multiplicity="NULL",
        field_values={field.field_id: "NULL" for field in fields},
        fields=fields,
    )


def _key_branch(definition: CustomImportDefinition, fields: tuple[Field, ...]) -> str:
    expression_by_key = {
        field_id: f"{_quoted('__ci_preflight_key_candidates')}.{_quoted(field_id)}"
        for field_id in definition.root_logical_key
    }
    return (
        _select_branch(
            kind=_KEY_KIND,
            stream_ordinal="0",
            stream_id="CAST(NULL AS TEXT)",
            key_ordinal=f"{_quoted('__ci_preflight_key_candidates')}.{_quoted(_KEY_ORDINAL_COLUMN)}",
            token="CAST(NULL AS TEXT)",
            multiplicity=f"{_quoted('__ci_preflight_key_candidates')}.{_quoted(_MULTIPLICITY_COLUMN)}",
            field_values={field.field_id: expression_by_key.get(field.field_id, "NULL") for field in fields},
            fields=fields,
        )
        + f" FROM {_quoted('__ci_preflight_key_candidates')}"
    )


def _data_branch(
    definition: CustomImportDefinition,
    source_stream,
    binding,
    columns_by_field: Mapping[str, SnowflakeDeclaredColumn],
    fields: tuple[Field, ...],
    ordinal: int,
    child_limit: int,
) -> str:
    alias = _quoted(f"__ci_preflight_source_{ordinal}")
    expression_by_field = {
        field_id: f"{alias}.{_quoted(column.column_identifier)}" for field_id, column in columns_by_field.items()
    }
    branch = _select_branch(
        kind=_DATA_KIND,
        stream_ordinal=ordinal,
        stream_id=f"'{source_stream.stream_id}'",
        key_ordinal="NULL",
        token="CAST(NULL AS TEXT)",
        multiplicity="NULL",
        field_values={field.field_id: expression_by_field.get(field.field_id, "NULL") for field in fields},
        fields=fields,
    )
    predicate = _row_filter_sql(binding, tuple(columns_by_field.values()), alias=alias)
    scope = f"{predicate} AND " if predicate else ""
    if source_stream.record_kind == "root":
        root_columns = columns_by_field
        conditions = " AND ".join(
            f"{alias}.{_quoted(root_columns[field_id].column_identifier)} = "
            f"{_quoted('__ci_preflight_selected_keys')}.{_quoted(field_id)}"
            for field_id in definition.root_logical_key
        )
        return (
            f"{branch} FROM {binding.relation.quoted_sql} AS {alias} WHERE {scope}EXISTS "
            f"(SELECT 1 FROM {_quoted('__ci_preflight_selected_keys')} WHERE {conditions})"
        )
    collection = definition.collections_by_name[source_stream.child_collection]
    conditions = " AND ".join(
        f"{alias}.{_quoted(columns_by_field[key_part.child_field].column_identifier)} = "
        f"{_quoted('__ci_preflight_selected_keys')}.{_quoted(key_part.root_field)}"
        for key_part in collection.parent_key
    )
    order_fields = _unique_field_ids(
        tuple(key_part.child_field for key_part in collection.parent_key),
        collection.child_key,
        tuple(column.field_id for column in columns_by_field.values()),
    )
    order = ", ".join(
        f"{alias}.{_quoted(columns_by_field[field_id].column_identifier)} ASC NULLS FIRST" for field_id in order_fields
    )
    return (
        f"SELECT * FROM ({branch} FROM {binding.relation.quoted_sql} AS {alias} WHERE {scope}EXISTS "
        f"(SELECT 1 FROM {_quoted('__ci_preflight_selected_keys')} WHERE {conditions}) "
        f"ORDER BY {order} LIMIT {child_limit + 1}) AS {_quoted(f'__ci_preflight_child_{ordinal}')}"
    )


def _select_branch(
    *,
    kind: int,
    stream_ordinal: int | str,
    stream_id: str,
    key_ordinal: str,
    token: str,
    multiplicity: str,
    field_values: Mapping[str, str],
    fields: tuple[Field, ...],
) -> str:
    values = (
        f"{kind} AS {_quoted(_KIND_COLUMN)}",
        f"{stream_ordinal} AS {_quoted(_STREAM_ORDINAL_COLUMN)}",
        f"{stream_id} AS {_quoted(_STREAM_ID_COLUMN)}",
        f"{key_ordinal} AS {_quoted(_KEY_ORDINAL_COLUMN)}",
        f"{token} AS {_quoted(_TOKEN_COLUMN)}",
        f"{multiplicity} AS {_quoted(_MULTIPLICITY_COLUMN)}",
        *(f"{field_values[field.field_id]} AS {_quoted(field.field_id)}" for field in fields),
    )
    return f"SELECT {', '.join(values)}"


def _execute_preflight(
    prepared: _PreparedPreflight,
    adapter: SnowflakePreflightAdapter,
    clock: Callable[[], float],
) -> SnowflakePreflightResult:
    started = _clock(clock)
    cursor: SnowflakePreflightCursor | None = None
    query_id: object | None = None
    failure: str | None = None
    observed: _ObservedRows | None = None
    try:
        cursor = adapter.open_preflight(
            prepared.statement,
            timeout_seconds=prepared.statement.limits.maximum_elapsed_seconds,
        )
        query_id = cursor.query_id
        if tuple(cursor.column_ids) != prepared.statement.column_ids:
            failure = "result_schema_invalid"
        elif _is_timed_out(started, prepared.statement.limits, clock):
            failure = "query_timeout"
        else:
            observed, failure = _read_rows(prepared, cursor, started, clock)
    except Exception:
        failure = "query_unavailable"
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                failure = failure or "query_unavailable"
    if observed is None:
        return _unavailable(
            prepared,
            observations=_unknown_observations(prepared.definition),
            observed_bytes=0,
            reason=failure or "query_unavailable",
        )
    observations = _observations(prepared.definition, observed, prepared.statement.limits)
    if failure is not None:
        return _unavailable(
            prepared,
            observations=_unknown_observations(prepared.definition),
            observed_bytes=observed.observed_bytes,
            reason=failure,
            runtime_supported=failure != "runtime_integer_unsupported",
        )
    return _finalize(prepared, query_id, observed, observations)


def _read_rows(
    prepared: _PreparedPreflight,
    cursor: SnowflakePreflightCursor,
    started: float,
    clock: Callable[[], float],
) -> tuple[_ObservedRows | None, str | None]:
    definition = prepared.definition
    state = _ObservedRows(
        metadata_tokens={},
        key_candidates=[],
        records_by_stream={stream.stream_id: [] for stream in definition.source_streams},
        bytes_by_stream={stream.stream_id: 0 for stream in definition.source_streams},
        observed_bytes=0,
    )
    while True:
        if _is_timed_out(started, prepared.statement.limits, clock):
            return state, "query_timeout"
        result_row = cursor.fetchone()
        if _is_timed_out(started, prepared.statement.limits, clock):
            return state, "query_timeout"
        if result_row is None:
            return state, None
        result_values = _row_values(result_row, len(prepared.statement.column_ids))
        if result_values is None:
            return state, "result_invalid"
        sentinel_reason = _byte_limit_sentinel_reason(result_values)
        if sentinel_reason is not None:
            return state, sentinel_reason
        conversions = prepared.statement.bundle_statement.request.decimal_conversions or {}
        try:
            result_values = tuple(
                convert_snowflake_float(scalar_value) if column_id in conversions else scalar_value
                for column_id, scalar_value in zip(prepared.statement.column_ids, result_values, strict=True)
            )
        except SnowflakeConnectorError:
            return state, "result_invalid"
        result_bytes = _row_bytes(result_values)
        if result_bytes is None:
            return state, "result_invalid"
        if state.observed_bytes + result_bytes > prepared.statement.limits.maximum_total_bytes:
            return state, "byte_limit"
        state.observed_bytes += result_bytes
        reason = _record_result_row(prepared.definition, state, result_values, result_bytes, prepared.statement.limits)
        if reason is not None:
            return state, reason


def _byte_limit_sentinel_reason(values: tuple[object, ...]) -> str | None:
    if values[0] != _BYTE_LIMIT_KIND:
        return None
    if type(values[0]) is int and all(value is None for value in values[1:]):
        return "byte_limit"
    return "result_invalid"


def _record_result_row(
    definition: CustomImportDefinition,
    state: _ObservedRows,
    result_values: tuple[object, ...],
    result_bytes: int,
    limits: SnowflakePreflightLimits,
) -> str | None:
    kind = result_values[0]
    if isinstance(kind, bool) or not isinstance(kind, int):
        return "result_invalid"
    fields = tuple(sorted(definition.fields, key=lambda field: field.field_slot))
    fixed_values = result_values[1:6]
    value_by_field = dict(zip((field.field_id for field in fields), result_values[6:], strict=True))
    if kind == _METADATA_KIND:
        return _record_metadata_row(definition, state, fixed_values, value_by_field)
    if kind == _KEY_KIND:
        return _record_key_metadata(definition, state, fixed_values, value_by_field, limits)
    if kind != _DATA_KIND:
        return "result_invalid"
    return _record_stream_data(definition, state, fixed_values, value_by_field, result_bytes, limits)


def _record_metadata_row(
    definition: CustomImportDefinition,
    state: _ObservedRows,
    fixed_values: tuple[object, ...],
    value_by_field: Mapping[str, object],
) -> str | None:
    stream_ordinal, stream_id, key_ordinal, token, multiplicity = fixed_values
    source_stream = _source_stream_by_ordinal(definition, stream_ordinal)
    if (
        source_stream is None
        or stream_id != source_stream.stream_id
        or key_ordinal is not None
        or multiplicity is not None
        or any(field_value is not None for field_value in value_by_field.values())
        or source_stream.stream_id in state.metadata_tokens
    ):
        return "result_invalid"
    state.metadata_tokens[source_stream.stream_id] = token
    return None


def _record_key_metadata(
    definition: CustomImportDefinition,
    state: _ObservedRows,
    fixed_values: tuple[object, ...],
    value_by_field: Mapping[str, object],
    limits: SnowflakePreflightLimits,
) -> str | None:
    stream_ordinal, stream_id, key_ordinal, token, multiplicity = fixed_values
    if (
        type(stream_ordinal) is not int
        or stream_ordinal != 0
        or stream_id is not None
        or token is not None
        or not _is_positive_int(key_ordinal)
        or not _is_positive_int(multiplicity)
        or any(
            value_by_field[field_id] is not None
            for field_id in value_by_field
            if field_id not in definition.root_logical_key
        )
        or key_ordinal > limits.maximum_root_keys + 1
        or key_ordinal != len(state.key_candidates) + 1
    ):
        return "result_invalid"
    root_key_values = tuple(value_by_field[field_id] for field_id in definition.root_logical_key)
    state.key_candidates.append((key_ordinal, multiplicity, root_key_values))
    return None


def _record_stream_data(
    definition: CustomImportDefinition,
    state: _ObservedRows,
    fixed_values: tuple[object, ...],
    value_by_field: Mapping[str, object],
    result_bytes: int,
    limits: SnowflakePreflightLimits,
) -> str | None:
    stream_ordinal, stream_id, key_ordinal, token, multiplicity = fixed_values
    source_stream = _source_stream_by_ordinal(definition, stream_ordinal)
    if (
        source_stream is None
        or stream_id != source_stream.stream_id
        or any(fixed_value is not None for fixed_value in (key_ordinal, token, multiplicity))
    ):
        return "result_invalid"
    expected_field_ids = {
        field.field_id for field in definition.fields if field.collection == source_stream.child_collection
    }
    if any(value_by_field[field_id] is not None for field_id in value_by_field if field_id not in expected_field_ids):
        return "result_invalid"
    if not _has_capture_compatible_integers(definition, source_stream.child_collection, value_by_field):
        return "runtime_integer_unsupported"
    stream_records = state.records_by_stream[source_stream.stream_id]
    stream_records.append({field_id: value_by_field[field_id] for field_id in expected_field_ids})
    state.bytes_by_stream[source_stream.stream_id] += result_bytes
    if source_stream.record_kind == "root" and len(stream_records) > limits.maximum_root_keys:
        return "root_data_incomplete"
    if source_stream.record_kind == "child" and len(stream_records) > limits.maximum_child_rows + 1:
        return "result_invalid"
    return None


def _has_capture_compatible_integers(
    definition: CustomImportDefinition,
    collection: str | None,
    value_by_field: Mapping[str, object],
) -> bool:
    """Require every declared integer to fit the downstream FIXED replay range."""

    for field in definition.fields:
        if field.collection != collection or field.value_type != "integer":
            continue
        value = value_by_field[field.field_id]
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, int)
            or not MIN_SCALAR_INTEGER <= value <= MAX_SCALAR_INTEGER
        ):
            return False
    return True


def _source_stream_by_ordinal(definition: CustomImportDefinition, stream_ordinal: object):
    if type(stream_ordinal) is not int or not 1 <= stream_ordinal <= len(definition.source_streams):
        return None
    return definition.source_streams[stream_ordinal - 1]


def _finalize(
    prepared: _PreparedPreflight,
    query_id: object | None,
    observed: _ObservedRows,
    observations: tuple[SnowflakePreflightStreamObservation, ...],
) -> SnowflakePreflightResult:
    """Admit only an all-or-nothing selected-family result."""

    definition = prepared.definition
    reason, selected_keys, snapshot_token = _finalization_inputs(prepared, query_id, observed)
    if reason is not None:
        return _unavailable(prepared, observations=observations, observed_bytes=observed.observed_bytes, reason=reason)
    if snapshot_token is None:
        return _unavailable(
            prepared, observations=observations, observed_bytes=observed.observed_bytes, reason="snapshot_invalid"
        )
    root_reason, roots = _selected_roots_complete(definition, observed, selected_keys)
    if root_reason is not None:
        return _unavailable(
            prepared, observations=observations, observed_bytes=observed.observed_bytes, reason=root_reason
        )
    if _has_selected_child_limit_reached(definition, observed, prepared.statement.limits):
        return _unavailable(
            prepared, observations=observations, observed_bytes=observed.observed_bytes, reason="child_limit_reached"
        )
    return _family_result(prepared, observed, observations, selected_keys, snapshot_token, roots)


def _finalization_inputs(
    prepared: _PreparedPreflight,
    query_id: object | None,
    observed: _ObservedRows,
) -> tuple[str | None, tuple[tuple[object, ...], ...], str | None]:
    """Validate complete metadata, selected keys, and one snapshot token."""

    definition = prepared.definition
    if set(observed.metadata_tokens) != {stream.stream_id for stream in definition.source_streams}:
        return "result_invalid", (), None
    key_reason, selected_keys = _selected_keys(definition, observed.key_candidates, prepared.statement.limits)
    if key_reason is not None:
        return key_reason, (), None
    token_reason, snapshot_token = _snapshot_token(definition, prepared.statement, query_id, observed.metadata_tokens)
    if token_reason is not None:
        return token_reason, (), None
    return None, selected_keys, snapshot_token


def _selected_roots_complete(
    definition: CustomImportDefinition,
    observed: _ObservedRows,
    selected_keys: tuple[tuple[object, ...], ...],
) -> tuple[str | None, list[dict[str, object]]]:
    """Require exactly one returned root record for every selected key."""

    root_stream = next(stream for stream in definition.source_streams if stream.record_kind == "root")
    roots = observed.records_by_stream[root_stream.stream_id]
    if {_record_key(root, definition.root_logical_key) for root in roots} != set(selected_keys) or len(roots) != len(
        selected_keys
    ):
        return "root_data_incomplete", roots
    return None, roots


def _has_selected_child_limit_reached(
    definition: CustomImportDefinition,
    observed: _ObservedRows,
    limits: SnowflakePreflightLimits,
) -> bool:
    """Return whether a selected child stream reached its non-admissible sentinel."""

    return any(
        len(observed.records_by_stream[stream.stream_id]) > limits.maximum_child_rows
        for stream in definition.source_streams
        if stream.record_kind == "child"
    )


def _family_result(
    prepared: _PreparedPreflight,
    observed: _ObservedRows,
    observations: tuple[SnowflakePreflightStreamObservation, ...],
    selected_keys: tuple[tuple[object, ...], ...],
    snapshot_token: str,
    roots: list[dict[str, object]],
) -> SnowflakePreflightResult:
    """Build selected families and retain only bounded generic rejection evidence."""

    definition = prepared.definition
    records_by_collection = {
        stream.child_collection: observed.records_by_stream[stream.stream_id]
        for stream in definition.source_streams
        if stream.record_kind == "child"
    }
    try:
        families = assemble_root_families(definition, roots, records_by_collection)
    except DefinitionError, TypeError, ValueError:
        return _unavailable(
            prepared, observations=observations, observed_bytes=observed.observed_bytes, reason="family_invalid"
        )
    if families.candidate_errors or families.rejections or len(families.families) != len(selected_keys):
        return _unavailable(
            prepared,
            observations=observations,
            observed_bytes=observed.observed_bytes,
            reason="family_invalid",
            rejection_diagnostics=_rejection_diagnostics(families.rejections, selected_keys),
        )
    return SnowflakePreflightResult(
        definition_sha256=definition.digest,
        schema_sha256=definition.schema_digest,
        source_binding_sha256=prepared.binding.digest,
        validation=SnowflakePreflightValidation(True, True, True),
        observations=observations,
        observed_bytes=observed.observed_bytes,
        status="complete",
        sample=SnowflakePreflightSample(snapshot_token, families.families),
    )


def _rejection_diagnostics(
    rejections: tuple[FamilyRejection, ...],
    selected_keys: tuple[tuple[object, ...], ...],
) -> tuple[SnowflakePreflightRejectionDiagnostic, ...]:
    """Keep one deterministic generic code per bounded selected root key."""

    selected_key_set = set(selected_keys)
    code_by_root_key: dict[tuple[object, ...], str] = {}
    for rejection in rejections:
        root_key = rejection.root_key
        if not _has_valid_key(root_key) or root_key not in selected_key_set:
            continue
        if _is_generic_rejection_code(rejection.code):
            code_by_root_key[root_key] = min(code_by_root_key.get(root_key, rejection.code), rejection.code)
    return tuple(
        SnowflakePreflightRejectionDiagnostic(root_key=root_key, code=code_by_root_key[root_key])
        for root_key in selected_keys
        if root_key in code_by_root_key
    )


def _selected_keys(
    definition: CustomImportDefinition,
    candidates: list[tuple[int, int, tuple[object, ...]]],
    limits: SnowflakePreflightLimits,
) -> tuple[str | None, tuple[tuple[object, ...], ...]]:
    if tuple(ordinal for ordinal, _multiplicity, _key in candidates) != tuple(range(1, len(candidates) + 1)):
        return "result_invalid", ()
    if len(candidates) > limits.maximum_root_keys + 1:
        return "result_invalid", ()
    selected_key_values = tuple(
        key for ordinal, _multiplicity, key in candidates if ordinal <= limits.maximum_root_keys
    )
    if any(not _has_valid_key(key) for key in selected_key_values):
        return "root_key_missing", ()
    if len(set(selected_key_values)) != len(selected_key_values):
        return "result_invalid", ()
    if any(multiplicity != 1 for ordinal, multiplicity, _key in candidates if ordinal <= limits.maximum_root_keys):
        return "duplicate_root_key", ()
    return None, selected_key_values


def _snapshot_token(
    definition: CustomImportDefinition,
    statement: SnowflakePreflightStatement,
    query_id: object | None,
    metadata_tokens: Mapping[str, object],
) -> tuple[str | None, str | None]:
    tokens_by_stream: dict[str, tuple[str | None, ...]] = {}
    for stream, bundle_binding in zip(
        definition.source_streams,
        statement.bundle_bindings,
        strict=True,
    ):
        token = metadata_tokens[stream.stream_id]
        if bundle_binding.source_snapshot_token_relation is None:
            if token is not None:
                return "snapshot_invalid", None
            try:
                token = _query_identity_snapshot_token(query_id)
            except SnowflakeBundleError:
                return "snapshot_invalid", None
        tokens_by_stream[stream.stream_id] = (token,)
    try:
        return None, validate_source_snapshot_tokens(definition, tokens_by_stream)
    except SourceSnapshotError:
        return "snapshot_invalid", None


def _observations(
    definition: CustomImportDefinition,
    observed: _ObservedRows,
    limits: SnowflakePreflightLimits,
) -> tuple[SnowflakePreflightStreamObservation, ...]:
    root_stream = next(stream for stream in definition.source_streams if stream.record_kind == "root")
    root_rows = sum(multiplicity for _ordinal, multiplicity, _key in observed.key_candidates)
    root_precision = "lower_bound" if len(observed.key_candidates) == limits.maximum_root_keys + 1 else "exact"
    observations = []
    for stream in definition.source_streams:
        if stream.stream_id == root_stream.stream_id:
            observations.append(
                SnowflakePreflightStreamObservation(
                    stream_id=stream.stream_id,
                    observed_rows=root_rows,
                    observed_bytes=observed.bytes_by_stream[stream.stream_id],
                    precision=root_precision,
                )
            )
            continue
        stream_rows = observed.records_by_stream[stream.stream_id]
        observations.append(
            SnowflakePreflightStreamObservation(
                stream_id=stream.stream_id,
                observed_rows=len(stream_rows),
                observed_bytes=observed.bytes_by_stream[stream.stream_id],
                precision="lower_bound" if len(stream_rows) > limits.maximum_child_rows else "exact",
            )
        )
    return tuple(observations)


def _unknown_observations(definition: CustomImportDefinition) -> tuple[SnowflakePreflightStreamObservation, ...]:
    return tuple(
        SnowflakePreflightStreamObservation(stream.stream_id, 0, 0, "unknown") for stream in definition.source_streams
    )


def _unavailable(
    prepared: _PreparedPreflight,
    *,
    observations: tuple[SnowflakePreflightStreamObservation, ...],
    observed_bytes: int,
    reason: str,
    runtime_supported: bool = True,
    rejection_diagnostics: tuple[SnowflakePreflightRejectionDiagnostic, ...] = (),
) -> SnowflakePreflightResult:
    return SnowflakePreflightResult(
        definition_sha256=prepared.definition.digest,
        schema_sha256=prepared.definition.schema_digest,
        source_binding_sha256=prepared.binding.digest,
        validation=SnowflakePreflightValidation(True, True, runtime_supported),
        observations=observations,
        observed_bytes=observed_bytes,
        status="unavailable",
        unavailable_reason=reason,
        rejection_diagnostics=rejection_diagnostics,
    )


def _invalid_result(
    definition: object,
    binding: object,
    reason: str,
    *,
    definition_valid: bool = False,
) -> SnowflakePreflightResult:
    return SnowflakePreflightResult(
        definition_sha256=_digest(definition, "digest"),
        schema_sha256=_digest(definition, "schema_digest"),
        source_binding_sha256=_digest(binding, "digest"),
        validation=SnowflakePreflightValidation(definition_valid, False, False),
        observations=(),
        observed_bytes=0,
        status="unavailable",
        unavailable_reason=reason,
    )


def _digest(value: object, attribute: str) -> str | None:
    candidate = getattr(value, attribute, None)
    return candidate if isinstance(candidate, str) and len(candidate) == 64 else None


def _row_values(row: object, expected_length: int) -> tuple[object, ...] | None:
    if isinstance(row, (str, bytes, bytearray, memoryview)) or not isinstance(row, Sequence):
        return None
    values = tuple(row)
    return values if len(values) == expected_length else None


def _row_bytes(row: Sequence[object]) -> int | None:
    try:
        return sum(_value_bytes(value) for value in row)
    except TypeError, UnicodeError, ValueError:
        return None


def _value_bytes(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    if isinstance(value, bool):
        return 1
    if isinstance(value, int):
        return len(str(value))
    if isinstance(value, Decimal):
        return len(str(value).encode("ascii"))
    if isinstance(value, datetime):
        return len(value.isoformat().encode("utf-8"))
    if isinstance(value, date):
        return len(value.isoformat().encode("ascii"))
    raise TypeError


def _record_key(record: Mapping[str, object], field_ids: tuple[str, ...]) -> tuple[object, ...] | None:
    values = tuple(record.get(field_id) for field_id in field_ids)
    return values if _has_valid_key(values) else None


def _has_valid_key(values: object) -> bool:
    if not isinstance(values, tuple) or not values:
        return False
    if any(value is None for value in values):
        return False
    try:
        hash(values)
    except TypeError:
        return False
    return True


def _is_generic_rejection_code(value: object) -> bool:
    if not isinstance(value, str) or not 1 <= len(value) <= 64 or value[0] not in "abcdefghijklmnopqrstuvwxyz":
        return False
    return all(character in "abcdefghijklmnopqrstuvwxyz0123456789_" for character in value)


def _quoted(identifier: str) -> str:
    return f'"{identifier}"'


def _unique_field_ids(*groups: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(field_id for group in groups for field_id in group))


def _clock(clock: Callable[[], float]) -> float:
    value = clock()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SnowflakePreflightError("preflight clock is invalid")
    return float(value)


def _is_timed_out(started: float, limits: SnowflakePreflightLimits, clock: Callable[[], float]) -> bool:
    return _clock(clock) - started > limits.maximum_elapsed_seconds
