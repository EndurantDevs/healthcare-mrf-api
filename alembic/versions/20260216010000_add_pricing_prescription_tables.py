"""Add pricing prescription tables"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260216010000_add_pricing_prescription_tables"
down_revision = "20260215103000_add_code_catalog_and_crosswalk"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "pricing_prescription",
        sa.Column("rx_code_system", sa.String(length=32), nullable=False),
        sa.Column("rx_code", sa.String(length=64), nullable=False),
        sa.Column("rx_name", sa.String(), nullable=True),
        sa.Column("generic_name", sa.String(), nullable=True),
        sa.Column("brand_name", sa.String(), nullable=True),
        sa.Column("total_claims", sa.Float(), nullable=True),
        sa.Column("total_30day_fills", sa.Float(), nullable=True),
        sa.Column("total_day_supply", sa.Float(), nullable=True),
        sa.Column("total_drug_cost", sa.Numeric(precision=16, scale=2, asdecimal=False), nullable=True),
        sa.Column("total_benes", sa.Float(), nullable=True),
        sa.Column("source_year", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("rx_code_system", "rx_code"),
        schema=schema,
    )
    op.create_index("pricing_prescription_name_idx", "pricing_prescription", ["rx_name"], schema=schema)
    op.create_index("pricing_prescription_generic_idx", "pricing_prescription", ["generic_name"], schema=schema)
    op.create_index("pricing_prescription_brand_idx", "pricing_prescription", ["brand_name"], schema=schema)
    op.create_index("pricing_prescription_source_year_idx", "pricing_prescription", ["source_year"], schema=schema)

    op.create_table(
        "pricing_provider_prescription",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("rx_code_system", sa.String(length=32), nullable=False),
        sa.Column("rx_code", sa.String(length=64), nullable=False),
        sa.Column("rx_name", sa.String(), nullable=True),
        sa.Column("generic_name", sa.String(), nullable=True),
        sa.Column("brand_name", sa.String(), nullable=True),
        sa.Column("provider_name", sa.String(), nullable=True),
        sa.Column("provider_type", sa.String(), nullable=True),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("zip5", sa.String(length=5), nullable=True),
        sa.Column("country", sa.String(), nullable=True),
        sa.Column("total_claims", sa.Float(), nullable=True),
        sa.Column("total_30day_fills", sa.Float(), nullable=True),
        sa.Column("total_day_supply", sa.Float(), nullable=True),
        sa.Column("total_drug_cost", sa.Numeric(precision=16, scale=2, asdecimal=False), nullable=True),
        sa.Column("total_benes", sa.Float(), nullable=True),
        sa.Column("ge65_total_claims", sa.Float(), nullable=True),
        sa.Column("ge65_total_30day_fills", sa.Float(), nullable=True),
        sa.Column("ge65_total_day_supply", sa.Float(), nullable=True),
        sa.Column("ge65_total_drug_cost", sa.Float(), nullable=True),
        sa.Column("ge65_total_benes", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "rx_code_system", "rx_code"),
        schema=schema,
    )
    op.create_index(
        "pricing_provider_rx_year_code_idx",
        "pricing_provider_prescription",
        ["year", "rx_code_system", "rx_code"],
        schema=schema,
    )
    op.create_index(
        "pricing_provider_rx_year_code_npi_idx",
        "pricing_provider_prescription",
        ["year", "rx_code_system", "rx_code", "npi"],
        schema=schema,
    )
    op.create_index(
        "pricing_provider_rx_year_state_city_idx",
        "pricing_provider_prescription",
        ["year", "state", "city"],
        schema=schema,
    )
    op.create_index("pricing_provider_rx_name_idx", "pricing_provider_prescription", ["rx_name"], schema=schema)
    op.create_index("pricing_provider_rx_generic_idx", "pricing_provider_prescription", ["generic_name"], schema=schema)
    op.create_index("pricing_provider_rx_brand_idx", "pricing_provider_prescription", ["brand_name"], schema=schema)
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_provider_rx_year_total_drug_cost_desc_idx "
        f"ON {schema}.pricing_provider_prescription (year, total_drug_cost DESC)"
    )


def downgrade():
    schema = _schema()

    op.drop_index(
        "pricing_provider_rx_year_total_drug_cost_desc_idx",
        table_name="pricing_provider_prescription",
        schema=schema,
    )
    op.drop_index("pricing_provider_rx_brand_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_index("pricing_provider_rx_generic_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_index("pricing_provider_rx_name_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_index("pricing_provider_rx_year_state_city_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_index("pricing_provider_rx_year_code_npi_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_index("pricing_provider_rx_year_code_idx", table_name="pricing_provider_prescription", schema=schema)
    op.drop_table("pricing_provider_prescription", schema=schema)

    op.drop_index("pricing_prescription_source_year_idx", table_name="pricing_prescription", schema=schema)
    op.drop_index("pricing_prescription_brand_idx", table_name="pricing_prescription", schema=schema)
    op.drop_index("pricing_prescription_generic_idx", table_name="pricing_prescription", schema=schema)
    op.drop_index("pricing_prescription_name_idx", table_name="pricing_prescription", schema=schema)
    op.drop_table("pricing_prescription", schema=schema)
