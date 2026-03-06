"""Add provider quality cohort-aware schema tables and fields"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260305103000_add_provider_quality_cohort_tables"
down_revision = "20260304220000_add_provider_quality_tables"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "pricing_provider_quality_feature",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("zip5", sa.String(length=5), nullable=True),
        sa.Column("state", sa.String(length=2), nullable=True),
        sa.Column("specialty_key", sa.String(length=128), nullable=True),
        sa.Column("taxonomy_code", sa.String(length=32), nullable=True),
        sa.Column("taxonomy_classification", sa.String(), nullable=True),
        sa.Column("procedure_count", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year"),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_idx",
        "pricing_provider_quality_feature",
        ["year"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_state_idx",
        "pricing_provider_quality_feature",
        ["year", "state"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_zip5_idx",
        "pricing_provider_quality_feature",
        ["year", "zip5"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_specialty_idx",
        "pricing_provider_quality_feature",
        ["year", "specialty_key"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_taxonomy_code_idx",
        "pricing_provider_quality_feature",
        ["year", "taxonomy_code"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_feature_year_taxonomy_class_idx",
        "pricing_provider_quality_feature",
        ["year", "taxonomy_classification"],
        schema=schema,
    )

    op.create_table(
        "pricing_provider_quality_procedure_lsh",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("band_no", sa.Integer(), nullable=False),
        sa.Column("band_hash", sa.String(length=128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "band_no"),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_proc_lsh_year_band_hash_idx",
        "pricing_provider_quality_procedure_lsh",
        ["year", "band_no", "band_hash"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_proc_lsh_year_npi_idx",
        "pricing_provider_quality_procedure_lsh",
        ["year", "npi"],
        schema=schema,
    )

    op.create_table(
        "pricing_provider_quality_peer_target",
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("benchmark_mode", sa.String(length=32), nullable=False),
        sa.Column("geography_scope", sa.String(length=16), nullable=False),
        sa.Column("geography_value", sa.String(length=128), nullable=False),
        sa.Column("cohort_level", sa.String(length=32), nullable=False),
        sa.Column("specialty_key", sa.String(length=128), nullable=False),
        sa.Column("taxonomy_code", sa.String(length=32), nullable=False),
        sa.Column("procedure_bucket", sa.String(length=64), nullable=False),
        sa.Column("peer_n", sa.Integer(), nullable=True),
        sa.Column("target_appropriateness", sa.Float(), nullable=True),
        sa.Column("target_effectiveness", sa.Float(), nullable=True),
        sa.Column("target_cost", sa.Float(), nullable=True),
        sa.Column("target_qpp_cost", sa.Float(), nullable=True),
        sa.Column("target_rx_appropriateness", sa.Float(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint(
            "year",
            "benchmark_mode",
            "geography_scope",
            "geography_value",
            "cohort_level",
            "specialty_key",
            "taxonomy_code",
            "procedure_bucket",
        ),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_peer_target_year_mode_geo_cohort_idx",
        "pricing_provider_quality_peer_target",
        ["year", "benchmark_mode", "geography_scope", "geography_value", "cohort_level"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_peer_target_year_mode_cohort_target_idx",
        "pricing_provider_quality_peer_target",
        ["year", "benchmark_mode", "cohort_level", "specialty_key", "taxonomy_code", "procedure_bucket"],
        schema=schema,
    )

    op.add_column(
        "pricing_provider_quality_measure",
        sa.Column("cohort_level", sa.String(length=32), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_measure",
        sa.Column("cohort_specialty_key", sa.String(length=128), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_measure",
        sa.Column("cohort_taxonomy_code", sa.String(length=32), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_measure",
        sa.Column("cohort_procedure_bucket", sa.String(length=64), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_measure",
        sa.Column("cohort_peer_n", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_cohort_idx",
        "pricing_provider_quality_measure",
        [
            "year",
            "benchmark_mode",
            "cohort_level",
            "cohort_specialty_key",
            "cohort_taxonomy_code",
            "cohort_procedure_bucket",
        ],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_npi_cohort_idx",
        "pricing_provider_quality_measure",
        [
            "year",
            "benchmark_mode",
            "npi",
            "cohort_level",
            "cohort_specialty_key",
            "cohort_taxonomy_code",
            "cohort_procedure_bucket",
        ],
        schema=schema,
    )

    op.add_column(
        "pricing_provider_quality_score",
        sa.Column("cohort_level", sa.String(length=32), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_score",
        sa.Column("cohort_specialty_key", sa.String(length=128), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_score",
        sa.Column("cohort_taxonomy_code", sa.String(length=32), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_score",
        sa.Column("cohort_procedure_bucket", sa.String(length=64), nullable=True),
        schema=schema,
    )
    op.add_column(
        "pricing_provider_quality_score",
        sa.Column("cohort_peer_n", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_score_year_mode_cohort_idx",
        "pricing_provider_quality_score",
        [
            "year",
            "benchmark_mode",
            "cohort_level",
            "cohort_specialty_key",
            "cohort_taxonomy_code",
            "cohort_procedure_bucket",
        ],
        schema=schema,
    )


def downgrade():
    schema = _schema()

    op.drop_index(
        "pricing_quality_score_year_mode_cohort_idx",
        table_name="pricing_provider_quality_score",
        schema=schema,
    )
    op.drop_column("pricing_provider_quality_score", "cohort_peer_n", schema=schema)
    op.drop_column("pricing_provider_quality_score", "cohort_procedure_bucket", schema=schema)
    op.drop_column("pricing_provider_quality_score", "cohort_taxonomy_code", schema=schema)
    op.drop_column("pricing_provider_quality_score", "cohort_specialty_key", schema=schema)
    op.drop_column("pricing_provider_quality_score", "cohort_level", schema=schema)

    op.drop_index(
        "pricing_quality_measure_year_mode_npi_cohort_idx",
        table_name="pricing_provider_quality_measure",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_measure_year_mode_cohort_idx",
        table_name="pricing_provider_quality_measure",
        schema=schema,
    )
    op.drop_column("pricing_provider_quality_measure", "cohort_peer_n", schema=schema)
    op.drop_column("pricing_provider_quality_measure", "cohort_procedure_bucket", schema=schema)
    op.drop_column("pricing_provider_quality_measure", "cohort_taxonomy_code", schema=schema)
    op.drop_column("pricing_provider_quality_measure", "cohort_specialty_key", schema=schema)
    op.drop_column("pricing_provider_quality_measure", "cohort_level", schema=schema)

    op.drop_index(
        "pricing_quality_peer_target_year_mode_cohort_target_idx",
        table_name="pricing_provider_quality_peer_target",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_peer_target_year_mode_geo_cohort_idx",
        table_name="pricing_provider_quality_peer_target",
        schema=schema,
    )
    op.drop_table("pricing_provider_quality_peer_target", schema=schema)

    op.drop_index(
        "pricing_quality_proc_lsh_year_npi_idx",
        table_name="pricing_provider_quality_procedure_lsh",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_proc_lsh_year_band_hash_idx",
        table_name="pricing_provider_quality_procedure_lsh",
        schema=schema,
    )
    op.drop_table("pricing_provider_quality_procedure_lsh", schema=schema)

    op.drop_index(
        "pricing_quality_feature_year_taxonomy_class_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_feature_year_taxonomy_code_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_feature_year_specialty_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_feature_year_zip5_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_feature_year_state_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_index(
        "pricing_quality_feature_year_idx",
        table_name="pricing_provider_quality_feature",
        schema=schema,
    )
    op.drop_table("pricing_provider_quality_feature", schema=schema)
