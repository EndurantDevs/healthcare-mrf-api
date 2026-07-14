# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Data-preserving helpers for adopting runtime-created migration objects."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any

import sqlalchemy as sa

from db.migration_expression_adoption import (
    _canonical_column_expression,
    _database_connection,
    _normalized_expression,
)


def _schema_inspector(operations: Any) -> Any | None:
    database_connection = _database_connection(operations)
    if database_connection is None:
        return None
    try:
        return sa.inspect(database_connection)
    except sa.exc.NoInspectionAvailable:
        return None


def _constraint_column_names(constraint: Any) -> tuple[str, ...]:
    pending_columns = getattr(constraint, "_pending_colargs", ())
    return tuple(
        column if isinstance(column, str) else column.name
        for column in pending_columns
    )


def _expected_primary_keys(table_elements: Sequence[Any]) -> tuple[str, ...] | None:
    for table_element in table_elements:
        if isinstance(table_element, sa.PrimaryKeyConstraint):
            return _constraint_column_names(table_element)
    return None


def _is_type_compatible(actual_type: Any, expected_type: Any) -> bool:
    if isinstance(actual_type, sa.JSON) and isinstance(expected_type, sa.JSON):
        return True
    if not expected_type._compare_type_affinity(actual_type):
        return False
    if isinstance(expected_type, sa.Integer) and isinstance(actual_type, sa.Integer):
        expected_integer_width = (
            "big"
            if isinstance(expected_type, sa.BigInteger)
            else "small"
            if isinstance(expected_type, sa.SmallInteger)
            else "standard"
        )
        actual_integer_width = (
            "big"
            if isinstance(actual_type, sa.BigInteger)
            else "small"
            if isinstance(actual_type, sa.SmallInteger)
            else "standard"
        )
        if actual_integer_width != expected_integer_width:
            return False
    for type_attribute in ("length", "precision", "scale", "timezone"):
        if getattr(actual_type, type_attribute, None) != getattr(
            expected_type,
            type_attribute,
            None,
        ):
            return False
    return True


def _validate_computed_expression(
    operations: Any,
    column_record: dict[str, Any],
    expected_column: sa.Column,
    schema: str,
    table_name: str,
    qualified_column: str,
) -> None:
    expected_computed = expected_column.computed
    actual_computed = column_record.get("computed")
    if expected_computed is None:
        if actual_computed:
            raise RuntimeError(
                f"existing_schema_unexpected_generated_column:{qualified_column}"
            )
        return
    if not actual_computed or not actual_computed.get("persisted", False):
        raise RuntimeError(
            f"existing_schema_generated_column_missing:{qualified_column}"
        )
    expected_expression = _canonical_column_expression(
        operations,
        schema,
        table_name,
        expected_column,
    )
    if _normalized_expression(actual_computed.get("sqltext")) != _normalized_expression(
        expected_expression
    ):
        raise RuntimeError(
            f"existing_schema_generated_expression_mismatch:{qualified_column}"
        )


def _validate_or_restore_server_default(
    operations: Any,
    column_record: dict[str, Any],
    expected_column: sa.Column,
    schema: str,
    table_name: str,
    qualified_column: str,
) -> None:
    expected_default = expected_column.server_default
    if expected_default is None or expected_column.computed is not None:
        return
    actual_default = column_record.get("default")
    if actual_default is None:
        operations.alter_column(
            table_name,
            expected_column.name,
            existing_type=column_record["type"],
            server_default=expected_default.arg,
            schema=schema,
        )
        return
    expected_expression = _canonical_column_expression(
        operations,
        schema,
        table_name,
        expected_column,
    )
    if _normalized_expression(actual_default) != _normalized_expression(
        expected_expression
    ):
        raise RuntimeError(
            f"existing_schema_column_default_mismatch:{qualified_column}"
        )


def _validate_existing_column(
    operations: Any,
    column_record: dict[str, Any],
    expected_column: sa.Column,
    schema: str,
    table_name: str,
    accepted_nullabilities: Iterable[bool] | None = None,
) -> None:
    qualified_column = f"{schema}.{table_name}.{expected_column.name}"
    if not _is_type_compatible(column_record["type"], expected_column.type):
        raise RuntimeError(f"existing_schema_column_type_mismatch:{qualified_column}")
    accepted_nullable_values = set(
        accepted_nullabilities or (bool(expected_column.nullable),)
    )
    if bool(column_record["nullable"]) not in accepted_nullable_values:
        raise RuntimeError(
            f"existing_schema_column_nullability_mismatch:{qualified_column}"
        )
    _validate_computed_expression(
        operations,
        column_record,
        expected_column,
        schema,
        table_name,
        qualified_column,
    )
    _validate_or_restore_server_default(
        operations,
        column_record,
        expected_column,
        schema,
        table_name,
        qualified_column,
    )
    if (
        expected_column.server_default is None
        and expected_column.computed is None
        and column_record.get("default") is not None
    ):
        raise RuntimeError(
            f"existing_schema_unexpected_column_default:{qualified_column}"
        )


def _validate_columns(
    operations: Any,
    schema_inspector: Any,
    schema: str,
    table_name: str,
    table_elements: Sequence[Any],
) -> None:
    existing_columns_by_name = {
        column_record["name"]: column_record
        for column_record in schema_inspector.get_columns(table_name, schema=schema)
    }
    expected_columns_by_name = {
        table_element.name: table_element
        for table_element in table_elements
        if isinstance(table_element, sa.Column)
    }
    missing_columns = sorted(expected_columns_by_name.keys() - existing_columns_by_name.keys())
    if missing_columns:
        raise RuntimeError(
            f"existing_schema_missing_columns:{schema}.{table_name}:"
            + ",".join(missing_columns)
        )
    for column_name, expected_column in expected_columns_by_name.items():
        _validate_existing_column(
            operations,
            existing_columns_by_name[column_name],
            expected_column,
            schema,
            table_name,
        )


def _validate_primary_key(
    schema_inspector: Any,
    schema: str,
    table_name: str,
    table_elements: Sequence[Any],
    accepted_primary_keys: Iterable[Sequence[str]] | None,
) -> None:
    expected_primary_key = _expected_primary_keys(table_elements)
    if expected_primary_key is None:
        return
    accepted_keys = {
        tuple(primary_key_columns)
        for primary_key_columns in (accepted_primary_keys or (expected_primary_key,))
    }
    primary_key_record = schema_inspector.get_pk_constraint(table_name, schema=schema)
    actual_primary_key_columns = tuple(
        primary_key_record.get("constrained_columns") or ()
    )
    if actual_primary_key_columns not in accepted_keys:
        raise RuntimeError(
            f"existing_schema_primary_key_mismatch:{schema}.{table_name}:"
            + ",".join(actual_primary_key_columns)
        )


def _validate_unique_constraints(
    schema_inspector: Any,
    schema: str,
    table_name: str,
    table_elements: Sequence[Any],
) -> None:
    existing_constraints_by_name = {
        constraint_record["name"]: tuple(
            constraint_record.get("column_names") or ()
        )
        for constraint_record in schema_inspector.get_unique_constraints(
            table_name,
            schema=schema,
        )
    }
    for table_element in table_elements:
        if not isinstance(table_element, sa.UniqueConstraint):
            continue
        expected_columns = _constraint_column_names(table_element)
        if existing_constraints_by_name.get(table_element.name) != expected_columns:
            raise RuntimeError(
                f"existing_schema_unique_constraint_mismatch:"
                f"{schema}.{table_element.name}"
            )


def _foreign_key_identity(key_record: dict[str, Any], default_schema: str) -> tuple[Any, ...]:
    return (
        tuple(key_record.get("constrained_columns") or ()),
        key_record.get("referred_schema") or default_schema,
        key_record.get("referred_table"),
        tuple(key_record.get("referred_columns") or ()),
        (key_record.get("options") or {}).get("ondelete"),
    )


def _expected_foreign_key_identity(
    constraint: sa.ForeignKeyConstraint,
    default_schema: str,
) -> tuple[Any, ...]:
    remote_specs = [foreign_key._colspec.split(".") for foreign_key in constraint.elements]
    remote_schemas = {parts[-3] if len(parts) >= 3 else default_schema for parts in remote_specs}
    remote_tables = {parts[-2] for parts in remote_specs}
    if len(remote_schemas) != 1 or len(remote_tables) != 1:
        raise RuntimeError(f"migration_foreign_key_target_invalid:{constraint.name}")
    return (
        tuple(constraint.column_keys),
        remote_schemas.pop(),
        remote_tables.pop(),
        tuple(parts[-1] for parts in remote_specs),
        constraint.ondelete,
    )


def _validate_foreign_keys(
    schema_inspector: Any,
    schema: str,
    table_name: str,
    table_elements: Sequence[Any],
) -> None:
    existing_keys_by_name = {
        key_record["name"]: _foreign_key_identity(key_record, schema)
        for key_record in schema_inspector.get_foreign_keys(table_name, schema=schema)
    }
    for table_element in table_elements:
        if not isinstance(table_element, sa.ForeignKeyConstraint):
            continue
        expected_identity = _expected_foreign_key_identity(table_element, schema)
        if existing_keys_by_name.get(table_element.name) != expected_identity:
            raise RuntimeError(
                f"existing_schema_foreign_key_mismatch:{schema}.{table_element.name}"
            )


def create_table_or_validate(
    operations: Any,
    table_name: str,
    *table_elements: Any,
    schema: str,
    accepted_primary_keys: Iterable[Sequence[str]] | None = None,
) -> None:
    """Create a missing table or validate required columns and primary key."""
    schema_inspector = _schema_inspector(operations)
    if schema_inspector is None or not schema_inspector.has_table(
        table_name,
        schema=schema,
    ):
        operations.create_table(table_name, *table_elements, schema=schema)
        return
    _validate_columns(
        operations,
        schema_inspector,
        schema,
        table_name,
        table_elements,
    )
    _validate_primary_key(
        schema_inspector,
        schema,
        table_name,
        table_elements,
        accepted_primary_keys,
    )
    _validate_unique_constraints(
        schema_inspector,
        schema,
        table_name,
        table_elements,
    )
    _validate_foreign_keys(
        schema_inspector,
        schema,
        table_name,
        table_elements,
    )


def add_column_if_missing(
    operations: Any,
    table_name: str,
    column: sa.Column,
    *,
    schema: str,
    accepted_nullabilities: Iterable[bool] | None = None,
) -> None:
    """Add a column unless the target table already contains it."""
    schema_inspector = _schema_inspector(operations)
    if schema_inspector is not None:
        existing_columns_by_name = {
            column_record["name"]: column_record
            for column_record in schema_inspector.get_columns(table_name, schema=schema)
        }
        if column.name in existing_columns_by_name:
            _validate_existing_column(
                operations,
                existing_columns_by_name[column.name],
                column,
                schema,
                table_name,
                accepted_nullabilities,
            )
            return
    operations.add_column(table_name, column, schema=schema)


def create_foreign_key_if_missing(
    operations: Any,
    source_table: str,
    constraint: sa.ForeignKeyConstraint,
    *,
    schema: str,
) -> None:
    """Create a named foreign key unless an equivalent key already exists."""
    expected_identity = _expected_foreign_key_identity(constraint, schema)
    constraint_name = constraint.name
    schema_inspector = _schema_inspector(operations)
    if schema_inspector is not None:
        existing_key = next(
            (
                key_record
                for key_record in schema_inspector.get_foreign_keys(
                    source_table,
                    schema=schema,
                )
                if key_record["name"] == constraint_name
            ),
            None,
        )
        if existing_key is not None:
            actual_identity = _foreign_key_identity(existing_key, schema)
            if actual_identity != expected_identity:
                raise RuntimeError(
                    f"existing_schema_foreign_key_mismatch:"
                    f"{schema}.{constraint_name}"
                )
            return
    local_columns, referent_schema, referent_table, remote_columns, ondelete = (
        expected_identity
    )
    operations.create_foreign_key(
        constraint_name,
        source_table,
        referent_table,
        list(local_columns),
        list(remote_columns),
        source_schema=schema,
        referent_schema=referent_schema,
        ondelete=ondelete,
    )


def primary_key_columns(
    operations: Any,
    table_name: str,
    *,
    schema: str,
) -> tuple[str, ...] | None:
    """Return live primary-key columns, or None for operation recorders."""
    schema_inspector = _schema_inspector(operations)
    if schema_inspector is None:
        return None
    primary_key_record = schema_inspector.get_pk_constraint(table_name, schema=schema)
    return tuple(primary_key_record.get("constrained_columns") or ())


def column_is_nullable(
    operations: Any,
    table_name: str,
    column_name: str,
    *,
    schema: str,
) -> bool | None:
    """Return live nullability, or None for operation recorders."""
    schema_inspector = _schema_inspector(operations)
    if schema_inspector is None:
        return None
    for column_record in schema_inspector.get_columns(table_name, schema=schema):
        if column_record["name"] == column_name:
            return bool(column_record["nullable"])
    raise RuntimeError(
        f"existing_schema_missing_column:{schema}.{table_name}.{column_name}"
    )
