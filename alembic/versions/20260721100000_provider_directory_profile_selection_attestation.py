"""Add durable Provider Directory Profile selection authority.

Revision ID: 20260721100000_provider_directory_profile_selection_attestation
Revises: 20260720130000_uhc_retained_artifact_admission
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260721100000_provider_directory_profile_selection_attestation"
down_revision = "20260720130000_uhc_retained_artifact_admission"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "provider_directory_profile_selection_authority",
        sa.Column("authority_key", sa.String(length=16), nullable=False),
        sa.Column(
            "last_revision",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "authority_key = 'global' AND last_revision >= 0",
            name="pd_profile_selection_authority_values_check",
        ),
        sa.PrimaryKeyConstraint(
            "authority_key",
            name="provider_directory_profile_selection_authority_pkey",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_profile_selection_proof",
        sa.Column("input_identity_digest", sa.String(length=64), nullable=False),
        sa.Column("proof_id", sa.String(length=64), nullable=False),
        sa.Column("identity_json", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "input_identity_digest ~ '^[0-9a-f]{64}$' "
            "AND proof_id ~ '^[0-9a-f]{64}$'",
            name="pd_profile_selection_proof_identity_check",
        ),
        sa.PrimaryKeyConstraint(
            "input_identity_digest",
            name="provider_directory_profile_selection_proof_pkey",
        ),
        sa.UniqueConstraint(
            "proof_id",
            name="provider_directory_profile_selection_proof_id_key",
        ),
        schema=schema,
    )
    op.create_table(
        "provider_directory_profile_selection_observation",
        sa.Column("authority_revision", sa.BigInteger(), nullable=False),
        sa.Column("input_identity_digest", sa.String(length=64), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "authority_revision > 0",
            name="pd_profile_selection_observation_revision_check",
        ),
        sa.ForeignKeyConstraint(
            ["input_identity_digest"],
            [
                f"{schema}.provider_directory_profile_selection_proof."
                "input_identity_digest"
            ],
            name="pd_profile_selection_observation_input_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "authority_revision",
            name="provider_directory_profile_selection_observation_pkey",
        ),
        schema=schema,
    )
    op.create_index(
        "pd_profile_selection_observation_input_idx",
        "provider_directory_profile_selection_observation",
        ["input_identity_digest"],
        schema=schema,
    )


def downgrade() -> None:
    schema = _schema()
    op.drop_table(
        "provider_directory_profile_selection_observation",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_profile_selection_proof",
        schema=schema,
    )
    op.drop_table(
        "provider_directory_profile_selection_authority",
        schema=schema,
    )
