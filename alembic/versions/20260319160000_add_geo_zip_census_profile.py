"""Add geo ZIP census profile table"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260319160000_add_geo_zip_census_profile"
down_revision = "20260305103000_add_provider_quality_cohort_tables"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "geo_zip_census_profile",
        sa.Column("zip_code", sa.String(length=5), nullable=False),
        sa.Column("total_population", sa.Integer(), nullable=True),
        sa.Column("median_household_income", sa.Integer(), nullable=True),
        sa.Column("bachelors_degree_or_higher_pct", sa.Float(), nullable=True),
        sa.Column("employment_rate_pct", sa.Float(), nullable=True),
        sa.Column("total_housing_units", sa.Integer(), nullable=True),
        sa.Column("without_health_insurance_pct", sa.Float(), nullable=True),
        sa.Column("total_employer_establishments", sa.Integer(), nullable=True),
        sa.Column("total_households", sa.Integer(), nullable=True),
        sa.Column("hispanic_or_latino", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("zip_code"),
        schema=schema,
    )


def downgrade():
    schema = _schema()
    op.drop_table("geo_zip_census_profile", schema=schema)
