# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict catalog adoption for the PTG V4 attempt authority."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa

from db.migration_adoption import create_table_or_validate
from db.migration_expression_adoption import _normalized_expression
from db.migration_ptg2_v4_attempt_audit import (
    validate_attempt_audit_trigger,
)
from db.migration_ptg2_v4_attempt_checks import validate_exact_fence_checks
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    ATTEMPT_STAGE_TABLE,
    FENCE_AUDIT_CONSTRAINT,
    FENCE_AUDIT_SHAPE_CHECK_SQL,
    FENCE_FINAL_COLUMNS,
    FENCE_FINAL_CONSTRAINTS,
    FENCE_LEGACY_AUDIT_CONSTRAINT,
    FENCE_LEGACY_COLUMNS,
    FENCE_NONCE_CONSTRAINT,
    FENCE_STATE_CHECK_SQL,
    STAGE_FINAL_COLUMNS,
    STAGE_FINAL_CONSTRAINTS,
    fence_table_elements,
    stage_table_elements,
)


_LIFECYCLE_LOCK_KEY = "ptg2_source_pointer_gc_v1"
_STATE_CONSTRAINT = "ptg2_v4_attempt_fence_state_check"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _connection(operations: Any) -> sa.engine.Connection | None:
    get_bind = getattr(operations, "get_bind", None)
    if not callable(get_bind):
        return None
    connection = get_bind()
    if not isinstance(connection, sa.engine.Connection):
        return None
    return connection


def _has_table(
    connection: sa.engine.Connection,
    schema: str,
    table: str,
) -> bool:
    return bool(
        connection.execute(
            sa.text("SELECT to_regclass(:qualified_name) IS NOT NULL"),
            {"qualified_name": f"{schema}.{table}"},
        ).scalar_one()
    )


def _has_tables_after_catalog_lock(
    connection: sa.engine.Connection,
    schema: str,
) -> bool:
    connection.execute(
        sa.text("SELECT pg_advisory_xact_lock(hashtext(:lock_key))"),
        {"lock_key": _LIFECYCLE_LOCK_KEY},
    )
    has_fence = _has_table(connection, schema, ATTEMPT_FENCE_TABLE)
    has_stage = _has_table(connection, schema, ATTEMPT_STAGE_TABLE)
    if has_fence != has_stage:
        raise RuntimeError(f"ptg2_v4_attempt_partial_shape:{schema}")
    connection.exec_driver_sql(
        f"LOCK TABLE {_qt(schema, 'ptg2_snapshot')}, "
        f"{_qt(schema, 'ptg2_import_run')} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    if has_fence:
        connection.exec_driver_sql(
            f"LOCK TABLE {_qt(schema, ATTEMPT_FENCE_TABLE)}, "
            f"{_qt(schema, ATTEMPT_STAGE_TABLE)} "
            "IN ACCESS EXCLUSIVE MODE"
        )
    return has_fence


def _column_records(
    connection: sa.engine.Connection,
    schema: str,
    table: str,
) -> dict[str, dict[str, Any]]:
    return {
        str(record["name"]): record
        for record in sa.inspect(connection).get_columns(table, schema=schema)
    }


def _constraint_records(
    connection: sa.engine.Connection,
    schema: str,
    table: str,
) -> dict[str, tuple[str, str, bool]]:
    constraint_result = connection.execute(
        sa.text(
            """
            SELECT constraint_record.conname,
                   constraint_record.contype::text,
                   pg_get_constraintdef(constraint_record.oid, true),
                   constraint_record.convalidated
              FROM pg_constraint AS constraint_record
              JOIN pg_class AS relation_record
                ON relation_record.oid = constraint_record.conrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = relation_record.relnamespace
             WHERE namespace_record.nspname = :schema
               AND relation_record.relname = :table
               AND constraint_record.contype <> 'n'
            """
        ),
        {"schema": schema, "table": table},
    ).mappings()
    return {
        str(constraint_mapping["conname"]): (
            str(constraint_mapping["contype"]),
            str(constraint_mapping["pg_get_constraintdef"]),
            bool(constraint_mapping["convalidated"]),
        )
        for constraint_mapping in constraint_result
    }


def _assert_exact_defaults(
    column_catalog: dict[str, dict[str, Any]],
    *,
    include_nonce: bool,
) -> None:
    expected_by_column = {
        "state": {"'active'::charactervarying", "'active'::varchar"},
        "created_at": {"now()"},
    }
    if include_nonce:
        expected_by_column["fence_nonce"] = {"gen_random_uuid()"}
    for column_name, column_record in column_catalog.items():
        normalized_default = _normalized_expression(
            column_record.get("default")
        )
        if column_name in expected_by_column:
            if normalized_default not in expected_by_column[column_name]:
                raise RuntimeError(
                    "existing_schema_column_default_mismatch:"
                    f"{ATTEMPT_FENCE_TABLE}.{column_name}"
                )
        elif normalized_default:
            raise RuntimeError(
                "existing_schema_unexpected_column_default:"
                f"{ATTEMPT_FENCE_TABLE}.{column_name}"
            )


def _classified_shape(
    fence_columns: set[str],
    fence_constraints: dict[str, tuple[str, str, bool]],
    schema: str,
) -> str:
    """Classify the exact legacy or final fence catalog."""

    if (
        fence_columns == FENCE_LEGACY_COLUMNS
        and set(fence_constraints)
        == (
            FENCE_FINAL_CONSTRAINTS
            - {FENCE_AUDIT_CONSTRAINT, FENCE_NONCE_CONSTRAINT}
            | {FENCE_LEGACY_AUDIT_CONSTRAINT}
        )
    ):
        shape = "legacy"
    elif (
        fence_columns == FENCE_FINAL_COLUMNS
        and set(fence_constraints) == FENCE_FINAL_CONSTRAINTS
    ):
        shape = "final"
    else:
        raise RuntimeError(f"ptg2_v4_attempt_fence_shape_unknown:{schema}")
    return shape


def _validate_shape_contracts(
    connection: sa.engine.Connection,
    schema: str,
    shape: str,
    fence_constraints: dict[str, tuple[str, str, bool]],
    stage_constraints: dict[str, tuple[str, str, bool]],
) -> None:
    """Validate every non-name property of one allowlisted catalog."""

    stage_columns = set(
        _column_records(connection, schema, ATTEMPT_STAGE_TABLE)
    )
    if (
        stage_columns != STAGE_FINAL_COLUMNS
        or set(stage_constraints) != STAGE_FINAL_CONSTRAINTS
    ):
        raise RuntimeError(f"ptg2_v4_attempt_stage_shape_unknown:{schema}")
    is_legacy = shape == "legacy"
    fence_column_catalog = _column_records(
        connection, schema, ATTEMPT_FENCE_TABLE
    )
    _assert_exact_defaults(
        fence_column_catalog,
        include_nonce=not is_legacy,
    )
    create_table_or_validate(
        _BoundOperations(connection),
        ATTEMPT_FENCE_TABLE,
        *fence_table_elements(
            schema,
            include_nonce=not is_legacy,
            include_nonce_constraint=not is_legacy,
            legacy_audit_constraint=is_legacy,
        ),
        schema=schema,
    )
    create_table_or_validate(
        _BoundOperations(connection),
        ATTEMPT_STAGE_TABLE,
        *stage_table_elements(schema),
        schema=schema,
    )
    validate_exact_fence_checks(
        connection,
        fence_constraints,
        is_legacy=is_legacy,
    )
    validate_attempt_audit_trigger(
        connection,
        schema,
        is_legacy=is_legacy,
    )


def _shape(
    connection: sa.engine.Connection,
    schema: str,
) -> str:
    """Return the fully validated allowlisted catalog shape."""

    fence_columns = set(
        _column_records(connection, schema, ATTEMPT_FENCE_TABLE)
    )
    fence_constraints = _constraint_records(
        connection, schema, ATTEMPT_FENCE_TABLE
    )
    stage_constraints = _constraint_records(
        connection, schema, ATTEMPT_STAGE_TABLE
    )
    shape = _classified_shape(fence_columns, fence_constraints, schema)
    _validate_shape_contracts(
        connection,
        schema,
        shape,
        fence_constraints,
        stage_constraints,
    )
    return shape


class _BoundOperations:
    """Expose only the bind needed by strict validation helpers."""

    def __init__(self, connection: sa.engine.Connection) -> None:
        self._connection = connection

    def get_bind(self) -> sa.engine.Connection:
        """Return the already locked migration connection."""

        return self._connection

    def alter_column(self, *_args: Any, **_kwargs: Any) -> None:
        """Reject repair outside the explicit legacy transition."""

        raise RuntimeError("ptg2_v4_attempt_shape_default_missing")


def _assert_adoptable_rows(
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    fence = _qt(schema, ATTEMPT_FENCE_TABLE)
    connection.exec_driver_sql(
        f"""
        DO $block$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {fence}
                 WHERE (
                    state = 'active'
                    AND (
                        target_digest IS NOT NULL
                        OR plan_digest IS NOT NULL
                        OR marker_digest IS NOT NULL
                        OR marker IS NOT NULL
                        OR reconciled_at IS NOT NULL
                    )
                 ) OR (
                    state = 'reconciled'
                    AND NOT (
                        target_digest ~ '^[0-9a-f]{{64}}$'
                        AND plan_digest ~ '^[0-9a-f]{{64}}$'
                        AND marker_digest ~ '^[0-9a-f]{{64}}$'
                        AND COALESCE(jsonb_typeof(marker) = 'object', false)
                        AND reconciled_at IS NOT NULL
                    )
                 )
            ) THEN
                RAISE EXCEPTION 'ptg2_v4_attempt_legacy_rows_not_adoptable'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $block$;
        """
    )


def _upgrade_legacy_shape(
    connection: sa.engine.Connection,
    schema: str,
) -> None:
    fence = _qt(schema, ATTEMPT_FENCE_TABLE)
    _assert_adoptable_rows(connection, schema)
    connection.exec_driver_sql(
        f"ALTER TABLE {fence} ADD COLUMN fence_nonce uuid "
        "NOT NULL DEFAULT gen_random_uuid()"
    )
    connection.exec_driver_sql(
        f"ALTER TABLE {fence} ADD CONSTRAINT {_q(FENCE_NONCE_CONSTRAINT)} "
        "UNIQUE (fence_nonce)"
    )
    connection.exec_driver_sql(
        f"ALTER TABLE {fence} "
        f"DROP CONSTRAINT {_q(_STATE_CONSTRAINT)}, "
        f"DROP CONSTRAINT {_q(FENCE_LEGACY_AUDIT_CONSTRAINT)}"
    )
    connection.exec_driver_sql(
        f"ALTER TABLE {fence} "
        f"ADD CONSTRAINT {_q(_STATE_CONSTRAINT)} "
        f"CHECK ({FENCE_STATE_CHECK_SQL}) NOT VALID, "
        f"ADD CONSTRAINT {_q(FENCE_AUDIT_CONSTRAINT)} "
        f"CHECK ({FENCE_AUDIT_SHAPE_CHECK_SQL}) NOT VALID"
    )
    for constraint_name in (_STATE_CONSTRAINT, FENCE_AUDIT_CONSTRAINT):
        connection.exec_driver_sql(
            f"ALTER TABLE {fence} VALIDATE CONSTRAINT "
            f"{_q(constraint_name)}"
        )


def adopt_or_validate_attempt_tables(
    operations: Any,
    schema: str,
    *,
    require_existing: bool = False,
) -> str:
    """Adopt exactly one legacy shape or validate the current shape."""

    connection = _connection(operations)
    if connection is None:
        return "offline"
    has_tables = _has_tables_after_catalog_lock(connection, schema)
    if not has_tables:
        if require_existing:
            raise RuntimeError(f"ptg2_v4_attempt_tables_missing:{schema}")
        return "absent"
    shape = _shape(connection, schema)
    if shape == "legacy":
        _upgrade_legacy_shape(connection, schema)
        validate_attempt_audit_trigger(
            connection,
            schema,
            is_legacy=True,
        )
    final_shape = _shape_without_trigger(connection, schema)
    if final_shape != "final":
        raise RuntimeError(f"ptg2_v4_attempt_fence_shape_unknown:{schema}")
    return "present"


def _shape_without_trigger(
    connection: sa.engine.Connection,
    schema: str,
) -> str:
    fence_columns = set(
        _column_records(connection, schema, ATTEMPT_FENCE_TABLE)
    )
    stage_columns = set(
        _column_records(connection, schema, ATTEMPT_STAGE_TABLE)
    )
    fence_constraints = _constraint_records(
        connection, schema, ATTEMPT_FENCE_TABLE
    )
    stage_constraints = _constraint_records(
        connection, schema, ATTEMPT_STAGE_TABLE
    )
    if (
        fence_columns != FENCE_FINAL_COLUMNS
        or set(fence_constraints) != FENCE_FINAL_CONSTRAINTS
        or stage_columns != STAGE_FINAL_COLUMNS
        or set(stage_constraints) != STAGE_FINAL_CONSTRAINTS
    ):
        return "unknown"
    _assert_exact_defaults(
        _column_records(connection, schema, ATTEMPT_FENCE_TABLE),
        include_nonce=True,
    )
    create_table_or_validate(
        _BoundOperations(connection),
        ATTEMPT_FENCE_TABLE,
        *fence_table_elements(schema),
        schema=schema,
    )
    create_table_or_validate(
        _BoundOperations(connection),
        ATTEMPT_STAGE_TABLE,
        *stage_table_elements(schema),
        schema=schema,
    )
    validate_exact_fence_checks(
        connection,
        fence_constraints,
        is_legacy=False,
    )
    return "final"


def validate_current_attempt_authority(
    operations: Any,
    schema: str,
) -> None:
    """Validate the final tables and audit trigger under lifecycle locks."""

    connection = _connection(operations)
    if connection is None:
        return
    if not _has_tables_after_catalog_lock(connection, schema):
        raise RuntimeError(f"ptg2_v4_attempt_tables_missing:{schema}")
    if _shape_without_trigger(connection, schema) != "final":
        raise RuntimeError(f"ptg2_v4_attempt_fence_shape_unknown:{schema}")
    validate_attempt_audit_trigger(
        connection,
        schema,
        is_legacy=False,
    )


__all__ = [
    "adopt_or_validate_attempt_tables",
    "validate_current_attempt_authority",
]
