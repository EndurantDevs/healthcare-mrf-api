"""Retain privacy-safe derived Provider Directory practitioner fields.

Revision ID: 20260713220000_provider_directory_practitioner_derived_profile
Revises: 20260713213000_provider_directory_dataset_serving_relations
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import add_column_if_missing


revision = "20260713220000_provider_directory_practitioner_derived_profile"
down_revision = "20260713213000_provider_directory_dataset_serving_relations"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


DERIVED_COLUMNS = (
    sa.Column("age_years", sa.Integer(), nullable=True),
    sa.Column("age_as_of", sa.String(length=10), nullable=True),
    sa.Column("years_of_practice", sa.Integer(), nullable=True),
    sa.Column("years_of_practice_as_of", sa.String(length=10), nullable=True),
    sa.Column("years_of_practice_basis", sa.String(length=128), nullable=True),
    sa.Column(
        "years_of_practice_start_date",
        sa.String(length=10),
        nullable=True,
    ),
)


def upgrade():
    schema = _schema()
    for column in DERIVED_COLUMNS:
        add_column_if_missing(
            op,
            "provider_directory_practitioner",
            column,
            schema=schema,
        )


def downgrade():
    schema = _schema()
    for column in reversed(DERIVED_COLUMNS):
        op.drop_column(
            "provider_directory_practitioner",
            column.name,
            schema=schema,
        )
