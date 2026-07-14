# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact PostgreSQL index validation for migration and runtime adoption."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
import uuid

import sqlalchemy as sa

from db.migration_adoption import _database_connection, _normalized_expression
from db.migration_index_catalog import _index_catalog_record


@dataclass(frozen=True)
class IndexDefinition:
    """DDL inputs for an explicitly supported legacy index."""

    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...] = ()
    predicate: str = ""
    unique: bool = False
    access_method: str = "btree"


@dataclass(frozen=True)
class IndexShape:
    """Complete PostgreSQL catalog identity for a named index."""

    key_columns: tuple[str, ...]
    include_columns: tuple[str, ...]
    predicate: str
    unique: bool
    access_method: str
    operator_classes: tuple[str, ...]
    collations: tuple[str, ...]
    sort_options: tuple[int, ...]
    nulls_not_distinct: bool
    exclusion: bool
    immediate: bool
    expression_keys: bool
    relation_options: tuple[str, ...]


def _shape_from_catalog(catalog_record: dict[str, Any]) -> IndexShape:
    return IndexShape(
        key_columns=tuple(catalog_record["key_columns"] or ()),
        include_columns=tuple(catalog_record["include_columns"] or ()),
        predicate=_normalized_expression(catalog_record["predicate"]),
        unique=bool(catalog_record["indisunique"]),
        access_method=str(catalog_record["access_method"]),
        operator_classes=tuple(catalog_record["operator_classes"] or ()),
        collations=tuple(catalog_record["collations"] or ()),
        sort_options=tuple(int(value) for value in catalog_record["sort_options"] or ()),
        nulls_not_distinct=bool(catalog_record["indnullsnotdistinct"]),
        exclusion=bool(catalog_record["indisexclusion"]),
        immediate=bool(catalog_record["indimmediate"]),
        expression_keys=bool(catalog_record["expression_keys"]),
        relation_options=tuple(catalog_record["relation_options"] or ()),
    )


def _quoted_identifier(database_connection: Any, identifier: str) -> str:
    return database_connection.dialect.identifier_preparer.quote(identifier)


def _quoted_qualified_name(database_connection: Any, identifier: str) -> str:
    return ".".join(
        _quoted_identifier(database_connection, name_part)
        for name_part in identifier.split(".")
    )


@dataclass(frozen=True)
class _TemporaryIndexContext:
    table_name: str
    index_name: str
    quoted_table: str
    quoted_index: str


def _create_temporary_index_table(
    database_connection: Any,
    schema: str,
    table_name: str,
) -> _TemporaryIndexContext:
    """Clone only column metadata into an empty transaction-scoped table."""
    temporary_suffix = uuid.uuid4().hex
    context = _TemporaryIndexContext(
        table_name=f"mrf_index_table_{temporary_suffix}",
        index_name=f"mrf_index_{temporary_suffix}",
        quoted_table=_quoted_identifier(
            database_connection,
            f"mrf_index_table_{temporary_suffix}",
        ),
        quoted_index=_quoted_identifier(
            database_connection,
            f"mrf_index_{temporary_suffix}",
        ),
    )
    quoted_source = (
        f"{_quoted_identifier(database_connection, schema)}."
        f"{_quoted_identifier(database_connection, table_name)}"
    )
    database_connection.execute(
        sa.text(
            f"CREATE TEMPORARY TABLE {context.quoted_table} "
            f"(LIKE {quoted_source} INCLUDING DEFAULTS INCLUDING GENERATED) "
            "ON COMMIT DROP"
        )
    )
    return context


def _expected_index_statement(
    database_connection: Any,
    context: _TemporaryIndexContext,
    definition: IndexDefinition,
    index_options: dict[str, Any],
) -> str:
    """Render controlled expected-index DDL for the temporary table."""
    operator_classes = index_options.get("postgresql_ops") or {}
    key_definitions = []
    for column_name in definition.key_columns:
        key_definition = _quoted_identifier(database_connection, column_name)
        operator_class = operator_classes.get(column_name)
        if operator_class:
            key_definition += " " + _quoted_qualified_name(
                database_connection,
                str(operator_class),
            )
        key_definitions.append(key_definition)
    include_clause = ""
    if definition.include_columns:
        include_clause = " INCLUDE (" + ", ".join(
            _quoted_identifier(database_connection, column_name)
            for column_name in definition.include_columns
        ) + ")"
    predicate_clause = f" WHERE {definition.predicate}" if definition.predicate else ""
    nulls_clause = (
        " NULLS NOT DISTINCT"
        if index_options.get("postgresql_nulls_not_distinct")
        else ""
    )
    unique_clause = "UNIQUE " if definition.unique else ""
    access_method = _quoted_identifier(
        database_connection,
        definition.access_method,
    )
    return (
        f"CREATE {unique_clause}INDEX {context.quoted_index} "
        f"ON {context.quoted_table} USING {access_method} "
        f"({', '.join(key_definitions)})"
        f"{include_clause}{nulls_clause}{predicate_clause}"
    )


def _temporary_table_schema(
    database_connection: Any,
    temporary_table: str,
) -> str:
    """Resolve the session-specific pg_temp schema containing a table."""
    return str(
        database_connection.execute(
            sa.text(
                """
                SELECT namespace_record.nspname
                  FROM pg_class AS table_record
                  JOIN pg_namespace AS namespace_record
                    ON namespace_record.oid = table_record.relnamespace
                 WHERE table_record.oid = to_regclass(:table_name)
                """
            ),
            {"table_name": temporary_table},
        ).scalar_one()
    )


def _temporary_expected_index_shape(
    operations: Any,
    table_name: str,
    schema: str,
    definition: IndexDefinition,
    index_options: dict[str, Any],
) -> IndexShape:
    """Build an empty temporary index and return PostgreSQL's exact identity."""
    database_connection = _database_connection(operations)
    if database_connection is None:
        raise RuntimeError("index_shape_validation_connection_missing")
    temporary_context = _create_temporary_index_table(
        database_connection,
        schema,
        table_name,
    )
    try:
        database_connection.execute(
            sa.text(
                _expected_index_statement(
                    database_connection,
                    temporary_context,
                    definition,
                    index_options,
                )
            )
        )
        temporary_schema = _temporary_table_schema(
            database_connection,
            temporary_context.table_name,
        )
        catalog_record = _index_catalog_record(
            operations,
            temporary_context.index_name,
            temporary_context.table_name,
            temporary_schema,
        )
        if catalog_record is None:
            raise RuntimeError("temporary_expected_index_missing")
        return _shape_from_catalog(catalog_record)
    finally:
        database_connection.execute(
            sa.text(f"DROP TABLE IF EXISTS {temporary_context.quoted_table}")
        )


def _fallback_expected_index_shape(
    definition: IndexDefinition,
    index_options: dict[str, Any],
) -> IndexShape:
    operator_classes = index_options.get("postgresql_ops") or {}
    return IndexShape(
        key_columns=definition.key_columns,
        include_columns=definition.include_columns,
        predicate=_normalized_expression(definition.predicate),
        unique=definition.unique,
        access_method=definition.access_method,
        operator_classes=tuple(
            str(operator_classes.get(column_name) or "")
            for column_name in definition.key_columns
        ),
        collations=tuple("" for _column_name in definition.key_columns),
        sort_options=tuple(0 for _column_name in definition.key_columns),
        nulls_not_distinct=bool(
            index_options.get("postgresql_nulls_not_distinct", False)
        ),
        exclusion=False,
        immediate=True,
        expression_keys=False,
        relation_options=(),
    )


def _definition(
    columns: Sequence[str],
    index_options: dict[str, Any],
) -> IndexDefinition:
    predicate = index_options.get("postgresql_where")
    return IndexDefinition(
        key_columns=tuple(columns),
        include_columns=tuple(index_options.get("postgresql_include", ())),
        predicate="" if predicate is None else str(predicate),
        unique=bool(index_options.get("unique", False)),
        access_method=str(index_options.get("postgresql_using") or "btree"),
    )


def _expected_index_shape(
    operations: Any,
    table_name: str,
    schema: str,
    definition: IndexDefinition,
    index_options: dict[str, Any],
) -> IndexShape:
    if _database_connection(operations) is None:
        return _fallback_expected_index_shape(definition, index_options)
    return _temporary_expected_index_shape(
        operations,
        table_name,
        schema,
        definition,
        index_options,
    )


def _existing_index_shape(
    operations: Any,
    index_name: str,
    table_name: str,
    schema: str,
) -> IndexShape | None:
    catalog_record = _index_catalog_record(
        operations,
        index_name,
        table_name,
        schema,
    )
    if catalog_record is None:
        return None
    if not catalog_record["indisvalid"] or not catalog_record["indisready"]:
        raise RuntimeError(f"existing_schema_index_invalid:{schema}.{index_name}")
    return _shape_from_catalog(catalog_record)


def has_matching_index(
    operations: Any,
    index_name: str,
    table_name: str,
    columns: Sequence[str],
    *,
    schema: str,
    **index_options: Any,
) -> bool:
    """Validate a named index and return whether it already exists."""
    actual_shape = _existing_index_shape(
        operations,
        index_name,
        table_name,
        schema,
    )
    if actual_shape is None:
        return False
    expected_definition = _definition(columns, index_options)
    expected_shape = _expected_index_shape(
        operations,
        table_name,
        schema,
        expected_definition,
        index_options,
    )
    if actual_shape != expected_shape:
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{index_name}")
    return True


def create_index_if_missing(
    operations: Any,
    index_name: str,
    table_name: str,
    columns: Sequence[str],
    *,
    schema: str,
    legacy_shapes: Sequence[IndexDefinition] = (),
    **index_options: Any,
) -> None:
    """Create a named index, replacing only catalog-exact legacy definitions."""
    expected_definition = _definition(columns, index_options)
    expected_shape = _expected_index_shape(
        operations,
        table_name,
        schema,
        expected_definition,
        index_options,
    )
    actual_shape = _existing_index_shape(
        operations,
        index_name,
        table_name,
        schema,
    )
    if actual_shape == expected_shape:
        return
    if actual_shape is not None:
        recognized_legacy_shapes = {
            _expected_index_shape(
                operations,
                table_name,
                schema,
                legacy_definition,
                {
                    "unique": legacy_definition.unique,
                    "postgresql_include": legacy_definition.include_columns,
                    "postgresql_where": legacy_definition.predicate or None,
                    "postgresql_using": legacy_definition.access_method,
                },
            )
            for legacy_definition in legacy_shapes
        }
        if actual_shape not in recognized_legacy_shapes:
            raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{index_name}")
        operations.drop_index(index_name, table_name=table_name, schema=schema)
    operations.create_index(
        index_name,
        table_name,
        list(columns),
        schema=schema,
        **index_options,
    )
