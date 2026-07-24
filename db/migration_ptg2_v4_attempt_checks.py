# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical PostgreSQL CHECK validation for the V4 attempt fence."""

from __future__ import annotations

import uuid

import sqlalchemy as sa

from db.migration_expression_adoption import _normalized_expression
from db.ptg2_v4_attempt_schema import (
    ATTEMPT_FENCE_TABLE,
    FENCE_AUDIT_CONSTRAINT,
    FENCE_AUDIT_SHAPE_CHECK_SQL,
    FENCE_LEGACY_AUDIT_CONSTRAINT,
    FENCE_LEGACY_AUDIT_SHAPE_CHECK_SQL,
    FENCE_STATE_CHECK_SQL,
)


_STATE_CONSTRAINT = "ptg2_v4_attempt_fence_state_check"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _canonical_checks(
    connection: sa.engine.Connection,
    expressions_by_name: dict[str, str],
) -> dict[str, str]:
    temporary_table = f"ptg2_v4_attempt_check_{uuid.uuid4().hex}"
    quoted_temporary = _q(temporary_table)
    connection.exec_driver_sql(
        f"CREATE TEMPORARY TABLE {quoted_temporary} ("
        "state varchar(16), target_digest varchar(64), "
        "plan_digest varchar(64), marker_digest varchar(64), "
        "marker jsonb, reconciled_at timestamptz"
        ") ON COMMIT DROP"
    )
    try:
        for constraint_name, expression in expressions_by_name.items():
            connection.exec_driver_sql(
                f"ALTER TABLE {quoted_temporary} "
                f"ADD CONSTRAINT {_q(constraint_name)} "
                f"CHECK ({expression})"
            )
        constraint_result = connection.execute(
            sa.text(
                """
                SELECT conname, pg_get_constraintdef(oid, true)
                  FROM pg_constraint
                 WHERE conrelid = to_regclass(:table_name)
                   AND contype = 'c'
                """
            ),
            {"table_name": temporary_table},
        )
        return {
            str(constraint_name): str(definition)
            for constraint_name, definition in constraint_result
        }
    finally:
        connection.exec_driver_sql(f"DROP TABLE IF EXISTS {quoted_temporary}")


def validate_exact_fence_checks(
    connection: sa.engine.Connection,
    constraint_catalog: dict[str, tuple[str, str, bool]],
    *,
    is_legacy: bool,
) -> None:
    """Require exact, validated state and audit-shape constraints."""

    audit_name = (
        FENCE_LEGACY_AUDIT_CONSTRAINT
        if is_legacy
        else FENCE_AUDIT_CONSTRAINT
    )
    audit_sql = (
        FENCE_LEGACY_AUDIT_SHAPE_CHECK_SQL
        if is_legacy
        else FENCE_AUDIT_SHAPE_CHECK_SQL
    )
    expected_by_name = _canonical_checks(
        connection,
        {
            _STATE_CONSTRAINT: FENCE_STATE_CHECK_SQL,
            audit_name: audit_sql,
        },
    )
    for constraint_name, expected_definition in expected_by_name.items():
        actual_constraint = constraint_catalog.get(constraint_name)
        if (
            actual_constraint is None
            or actual_constraint[0] != "c"
            or not actual_constraint[2]
            or _normalized_expression(actual_constraint[1])
            != _normalized_expression(expected_definition)
        ):
            raise RuntimeError(
                "existing_schema_check_constraint_mismatch:"
                f"{ATTEMPT_FENCE_TABLE}.{constraint_name}"
            )


__all__ = ["validate_exact_fence_checks"]
