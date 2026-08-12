# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add compact Provider Directory artifact-selection receipts.

Revision ID: 20260812010000_provider_directory_artifact_selection_receipt
Revises: 20260811140000_ptg_v12_provider_publication_merge
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import add_column_if_missing


revision = "20260812010000_provider_directory_artifact_selection_receipt"
down_revision = "20260811140000_ptg_v12_provider_publication_merge"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade() -> None:
    add_column_if_missing(
        op,
        "provider_directory_endpoint_dataset",
        sa.Column(
            "artifact_selection_receipt_json",
            postgresql.JSONB(),
            nullable=True,
        ),
        schema=_schema(),
    )


def downgrade() -> None:
    op.drop_column(
        "provider_directory_endpoint_dataset",
        "artifact_selection_receipt_json",
        schema=_schema(),
    )
