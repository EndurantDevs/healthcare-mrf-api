"""Add snapshot-scoped strict V3 allowed-amount evidence tables.

Revision ID: 20260716120000_ptg2_allowed_amounts_v3
Revises: 20260715160000_mrf_discovery_source_checkpoints
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260716120000_ptg2_allowed_amounts_v3"
down_revision = "20260715160000_mrf_discovery_source_checkpoints"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "ptg2_allowed_amount_plan",
        sa.Column("snapshot_id", sa.String(length=96), nullable=False),
        sa.Column("plan_hash", sa.BigInteger(), nullable=False),
        sa.Column("source_file_version_id", sa.String(length=96), nullable=True),
        sa.Column("file_id", sa.BigInteger(), nullable=False),
        sa.Column("plan_name", sa.String(), nullable=True),
        sa.Column("plan_id_type", sa.String(length=16), nullable=True),
        sa.Column("plan_id", sa.String(length=64), nullable=False),
        sa.Column("plan_market_type", sa.String(length=32), nullable=True),
        sa.Column("issuer_name", sa.String(), nullable=True),
        sa.Column("plan_sponsor_name", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            [f"{schema}.ptg2_snapshot.snapshot_id"],
            name="ptg2_allowed_plan_snapshot_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "plan_hash",
            name="ptg2_allowed_amount_plan_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_plan_snapshot_lookup_idx",
        "ptg2_allowed_amount_plan",
        ["snapshot_id", "plan_id", "plan_market_type", "file_id"],
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_plan_file_idx",
        "ptg2_allowed_amount_plan",
        ["snapshot_id", "file_id"],
        schema=schema,
    )

    op.create_table(
        "ptg2_allowed_amount_item",
        sa.Column("snapshot_id", sa.String(length=96), nullable=False),
        sa.Column("allowed_item_hash", sa.BigInteger(), nullable=False),
        sa.Column("source_file_version_id", sa.String(length=96), nullable=True),
        sa.Column("file_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("billing_code_type", sa.String(length=32), nullable=True),
        sa.Column("billing_code_type_version", sa.String(length=32), nullable=True),
        sa.Column("billing_code", sa.String(length=64), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("plan_name", sa.String(), nullable=True),
        sa.Column("plan_id_type", sa.String(length=16), nullable=True),
        sa.Column("plan_id", sa.String(length=64), nullable=True),
        sa.Column("plan_market_type", sa.String(length=32), nullable=True),
        sa.Column("issuer_name", sa.String(), nullable=True),
        sa.Column("plan_sponsor_name", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id"],
            [f"{schema}.ptg2_snapshot.snapshot_id"],
            name="ptg2_allowed_item_snapshot_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "allowed_item_hash",
            name="ptg2_allowed_amount_item_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_item_snapshot_code_plan_idx",
        "ptg2_allowed_amount_item",
        [
            "snapshot_id",
            "billing_code",
            "plan_id",
            "file_id",
            "billing_code_type",
        ],
        schema=schema,
    )

    op.create_table(
        "ptg2_allowed_amount_payment",
        sa.Column("snapshot_id", sa.String(length=96), nullable=False),
        sa.Column("payment_hash", sa.BigInteger(), nullable=False),
        sa.Column("allowed_item_hash", sa.BigInteger(), nullable=False),
        sa.Column("tin_type", sa.String(length=8), nullable=True),
        sa.Column("tin_value", sa.String(length=32), nullable=True),
        sa.Column("service_code", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("billing_class", sa.String(length=32), nullable=True),
        sa.Column("setting", sa.String(length=32), nullable=True),
        sa.Column("allowed_amount", sa.Numeric(), nullable=True),
        sa.Column(
            "billing_code_modifier",
            postgresql.ARRAY(sa.String()),
            nullable=True,
        ),
        sa.Column("network_status", sa.String(length=64), nullable=True),
        sa.Column("network_semantics", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "allowed_item_hash"],
            [
                f"{schema}.ptg2_allowed_amount_item.snapshot_id",
                f"{schema}.ptg2_allowed_amount_item.allowed_item_hash",
            ],
            name="ptg2_allowed_payment_item_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "payment_hash",
            name="ptg2_allowed_amount_payment_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_payment_item_idx",
        "ptg2_allowed_amount_payment",
        ["snapshot_id", "allowed_item_hash"],
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_payment_tin_idx",
        "ptg2_allowed_amount_payment",
        ["snapshot_id", "tin_value"],
        schema=schema,
    )

    op.create_table(
        "ptg2_allowed_amount_provider_payment",
        sa.Column("snapshot_id", sa.String(length=96), nullable=False),
        sa.Column("provider_payment_hash", sa.BigInteger(), nullable=False),
        sa.Column("payment_hash", sa.BigInteger(), nullable=False),
        sa.Column("billed_charge", sa.Numeric(), nullable=True),
        sa.Column("npi", postgresql.ARRAY(sa.BigInteger()), nullable=False),
        sa.ForeignKeyConstraint(
            ["snapshot_id", "payment_hash"],
            [
                f"{schema}.ptg2_allowed_amount_payment.snapshot_id",
                f"{schema}.ptg2_allowed_amount_payment.payment_hash",
            ],
            name="ptg2_allowed_provider_payment_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "snapshot_id",
            "provider_payment_hash",
            name="ptg2_allowed_amount_provider_payment_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_provider_payment_idx",
        "ptg2_allowed_amount_provider_payment",
        ["snapshot_id", "payment_hash"],
        schema=schema,
    )
    op.create_index(
        "ptg2_allowed_provider_npi_gin_idx",
        "ptg2_allowed_amount_provider_payment",
        ["npi"],
        schema=schema,
        postgresql_using="gin",
    )


def downgrade() -> None:
    schema = _schema()
    op.drop_table("ptg2_allowed_amount_provider_payment", schema=schema)
    op.drop_table("ptg2_allowed_amount_payment", schema=schema)
    op.drop_table("ptg2_allowed_amount_item", schema=schema)
    op.drop_table("ptg2_allowed_amount_plan", schema=schema)
