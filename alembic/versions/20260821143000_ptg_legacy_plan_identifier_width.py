"""Retain canonical plan identifiers in legacy PTG projections.

Revision ID: 20260821143000_ptg_legacy_plan_identifier_width
Revises: 20260821010000_ptg_ordinary_terminal_blank_receipt
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa


revision = "20260821143000_ptg_legacy_plan_identifier_width"
down_revision = "20260821010000_ptg_ordinary_terminal_blank_receipt"
branch_labels = None
depends_on = None

_TABLES = ("ptg_file", "ptg_in_network_item", "ptg_allowed_item")


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _is_offline_mode() -> bool:
    get_context = getattr(op, "get_context", None)
    return bool(get_context and get_context().as_sql)


def _alter_plan_id_width(
    table_name: str, *, schema: str, expected: int, target: int
) -> None:
    op.alter_column(
        table_name,
        "plan_id",
        existing_type=sa.String(length=expected),
        type_=sa.String(length=target),
        schema=schema,
    )


def _set_plan_id_width(*, expected: int, target: int) -> None:
    schema = _schema()
    if _is_offline_mode():
        for table_name in _TABLES:
            _alter_plan_id_width(
                table_name,
                schema=schema,
                expected=expected,
                target=target,
            )
        return
    inspector = sa.inspect(op.get_bind())
    for table_name in _TABLES:
        if not inspector.has_table(table_name, schema=schema):
            continue
        plan_id = next(
            (
                column
                for column in inspector.get_columns(table_name, schema=schema)
                if column["name"] == "plan_id"
            ),
            None,
        )
        if plan_id is None or plan_id["type"].length not in {expected, target}:
            raise RuntimeError(f"unexpected_plan_id_column:{schema}.{table_name}")
        if plan_id["type"].length == target:
            continue
        _alter_plan_id_width(
            table_name,
            schema=schema,
            expected=expected,
            target=target,
        )


def upgrade() -> None:
    _set_plan_id_width(expected=32, target=64)


def downgrade() -> None:
    _set_plan_id_width(expected=64, target=32)
