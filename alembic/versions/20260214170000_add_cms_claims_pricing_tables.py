"""Add CMS claims pricing tables"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260214170000_add_cms_claims_pricing_tables"
down_revision = "20251112154500_add_do_business_as_text"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "pricing_provider",
        sa.Column("provider_key", sa.BigInteger(), nullable=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("provider_name", sa.String(), nullable=True),
        sa.Column("first_name", sa.String(), nullable=True),
        sa.Column("last_org_name", sa.String(), nullable=True),
        sa.Column("credentials", sa.String(), nullable=True),
        sa.Column("provider_type", sa.String(), nullable=True),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("zip5", sa.String(length=5), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("total_claims", sa.Float(), nullable=True),
        sa.Column("total_30day_fills", sa.Float(), nullable=True),
        sa.Column("total_drug_cost", sa.Numeric(precision=16, scale=2, asdecimal=False), nullable=True),
        sa.Column("total_day_supply", sa.Float(), nullable=True),
        sa.Column("total_benes", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("provider_key"),
        schema=schema,
    )
    op.create_index("pricing_provider_npi_idx", "pricing_provider", ["npi"], schema=schema)
    op.create_index("pricing_provider_year_idx", "pricing_provider", ["year"], schema=schema)
    op.create_index("pricing_provider_state_city_idx", "pricing_provider", ["state", "city"], schema=schema)
    op.create_index("pricing_provider_type_idx", "pricing_provider", ["provider_type"], schema=schema)

    op.create_table(
        "pricing_procedure",
        sa.Column("procedure_code", sa.BigInteger(), nullable=False),
        sa.Column("generic_name", sa.String(), nullable=True),
        sa.Column("brand_name", sa.String(), nullable=True),
        sa.Column("avg_spend_per_claim", sa.Float(), nullable=True),
        sa.Column("avg_spend_per_bene", sa.Float(), nullable=True),
        sa.Column("avg_spend_per_dose_unit", sa.Float(), nullable=True),
        sa.Column("total_spending", sa.Float(), nullable=True),
        sa.Column("total_claims", sa.Float(), nullable=True),
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("procedure_code"),
        schema=schema,
    )
    op.create_index("pricing_procedure_generic_idx", "pricing_procedure", ["generic_name"], schema=schema)
    op.create_index("pricing_procedure_brand_idx", "pricing_procedure", ["brand_name"], schema=schema)

    op.create_table(
        "pricing_provider_procedure",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("procedure_code", sa.BigInteger(), nullable=False),
        sa.Column("generic_name", sa.String(), nullable=True),
        sa.Column("brand_name", sa.String(), nullable=True),
        sa.Column("total_claims", sa.Float(), nullable=True),
        sa.Column("total_30day_fills", sa.Float(), nullable=True),
        sa.Column("total_day_supply", sa.Float(), nullable=True),
        sa.Column("total_drug_cost", sa.Numeric(precision=16, scale=2, asdecimal=False), nullable=True),
        sa.Column("total_benes", sa.Float(), nullable=True),
        sa.Column("ge65_total_claims", sa.Float(), nullable=True),
        sa.Column("ge65_total_drug_cost", sa.Float(), nullable=True),
        sa.Column("ge65_total_benes", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "procedure_code"),
        schema=schema,
    )
    op.create_index(
        "pricing_provider_proc_year_idx",
        "pricing_provider_procedure",
        ["year", "procedure_code"],
        schema=schema,
    )
    op.create_index("pricing_provider_proc_brand_idx", "pricing_provider_procedure", ["brand_name"], schema=schema)
    op.create_index("pricing_provider_proc_generic_idx", "pricing_provider_procedure", ["generic_name"], schema=schema)

    op.create_table(
        "pricing_provider_procedure_location",
        sa.Column("location_key", sa.BigInteger(), nullable=False),
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("procedure_code", sa.BigInteger(), nullable=False),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("zip5", sa.String(length=5), nullable=True),
        sa.Column("state_fips", sa.String(length=4), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("location_key"),
        schema=schema,
    )
    op.create_index(
        "pricing_provider_loc_lookup_idx",
        "pricing_provider_procedure_location",
        ["npi", "year", "procedure_code"],
        schema=schema,
    )
    op.create_index(
        "pricing_provider_loc_state_city_idx",
        "pricing_provider_procedure_location",
        ["state", "city"],
        schema=schema,
    )


def downgrade():
    schema = _schema()

    op.drop_index("pricing_provider_loc_state_city_idx", table_name="pricing_provider_procedure_location", schema=schema)
    op.drop_index("pricing_provider_loc_lookup_idx", table_name="pricing_provider_procedure_location", schema=schema)
    op.drop_table("pricing_provider_procedure_location", schema=schema)

    op.drop_index("pricing_provider_proc_generic_idx", table_name="pricing_provider_procedure", schema=schema)
    op.drop_index("pricing_provider_proc_brand_idx", table_name="pricing_provider_procedure", schema=schema)
    op.drop_index("pricing_provider_proc_year_idx", table_name="pricing_provider_procedure", schema=schema)
    op.drop_table("pricing_provider_procedure", schema=schema)

    op.drop_index("pricing_procedure_brand_idx", table_name="pricing_procedure", schema=schema)
    op.drop_index("pricing_procedure_generic_idx", table_name="pricing_procedure", schema=schema)
    op.drop_table("pricing_procedure", schema=schema)

    op.drop_index("pricing_provider_type_idx", table_name="pricing_provider", schema=schema)
    op.drop_index("pricing_provider_state_city_idx", table_name="pricing_provider", schema=schema)
    op.drop_index("pricing_provider_year_idx", table_name="pricing_provider", schema=schema)
    op.drop_index("pricing_provider_npi_idx", table_name="pricing_provider", schema=schema)
    op.drop_table("pricing_provider", schema=schema)
