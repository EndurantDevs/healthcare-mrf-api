"""Add provider quality scoring tables"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260304220000_add_provider_quality_tables"
down_revision = "20260216010000_add_pricing_prescription_tables"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.create_table(
        "pricing_qpp_provider",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("quality_score", sa.Float(), nullable=True),
        sa.Column("cost_score", sa.Float(), nullable=True),
        sa.Column("final_score", sa.Float(), nullable=True),
        sa.Column("raw_json_text", sa.TEXT(), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year"),
        schema=schema,
    )
    op.create_index("pricing_qpp_year_npi_idx", "pricing_qpp_provider", ["year", "npi"], schema=schema)
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_qpp_year_final_score_desc_idx "
        f"ON {schema}.pricing_qpp_provider (year, final_score DESC)"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_qpp_year_quality_score_desc_idx "
        f"ON {schema}.pricing_qpp_provider (year, quality_score DESC)"
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_qpp_year_cost_score_desc_idx "
        f"ON {schema}.pricing_qpp_provider (year, cost_score DESC)"
    )

    op.create_table(
        "pricing_svi_zcta",
        sa.Column("zcta", sa.String(length=5), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("svi_overall", sa.Float(), nullable=True),
        sa.Column("svi_socioeconomic", sa.Float(), nullable=True),
        sa.Column("svi_household", sa.Float(), nullable=True),
        sa.Column("svi_minority", sa.Float(), nullable=True),
        sa.Column("svi_housing", sa.Float(), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("zcta", "year"),
        schema=schema,
    )
    op.create_index("pricing_svi_year_zcta_idx", "pricing_svi_zcta", ["year", "zcta"], schema=schema)
    op.create_index("pricing_svi_year_overall_idx", "pricing_svi_zcta", ["year", "svi_overall"], schema=schema)

    op.create_table(
        "pricing_provider_quality_measure",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("benchmark_mode", sa.String(length=32), nullable=False),
        sa.Column("measure_id", sa.String(length=128), nullable=False),
        sa.Column("domain", sa.String(length=32), nullable=False),
        sa.Column("observed", sa.Float(), nullable=True),
        sa.Column("target", sa.Float(), nullable=True),
        sa.Column("risk_ratio", sa.Float(), nullable=True),
        sa.Column("ci75_low", sa.Float(), nullable=True),
        sa.Column("ci75_high", sa.Float(), nullable=True),
        sa.Column("ci90_low", sa.Float(), nullable=True),
        sa.Column("ci90_high", sa.Float(), nullable=True),
        sa.Column("evidence_n", sa.Integer(), nullable=True),
        sa.Column("peer_group", sa.String(length=128), nullable=True),
        sa.Column("direction", sa.String(length=16), nullable=True),
        sa.Column("source", sa.String(length=128), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "benchmark_mode", "measure_id", "domain"),
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_domain_measure_idx",
        "pricing_provider_quality_measure",
        ["year", "benchmark_mode", "domain", "measure_id"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_npi_domain_idx",
        "pricing_provider_quality_measure",
        ["year", "benchmark_mode", "npi", "domain"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_peer_domain_idx",
        "pricing_provider_quality_measure",
        ["year", "benchmark_mode", "peer_group", "domain"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_measure_year_mode_domain_rr_idx",
        "pricing_provider_quality_measure",
        ["year", "benchmark_mode", "domain", "risk_ratio"],
        schema=schema,
    )

    op.create_table(
        "pricing_provider_quality_domain",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("benchmark_mode", sa.String(length=32), nullable=False),
        sa.Column("domain", sa.String(length=32), nullable=False),
        sa.Column("risk_ratio", sa.Float(), nullable=True),
        sa.Column("score_0_100", sa.Float(), nullable=True),
        sa.Column("ci75_low", sa.Float(), nullable=True),
        sa.Column("ci75_high", sa.Float(), nullable=True),
        sa.Column("ci90_low", sa.Float(), nullable=True),
        sa.Column("ci90_high", sa.Float(), nullable=True),
        sa.Column("evidence_n", sa.Integer(), nullable=True),
        sa.Column("measure_count", sa.Integer(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "benchmark_mode", "domain"),
        schema=schema,
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_quality_domain_year_mode_domain_score_desc_idx "
        f"ON {schema}.pricing_provider_quality_domain (year, benchmark_mode, domain, score_0_100 DESC)"
    )
    op.create_index(
        "pricing_quality_domain_year_mode_domain_rr_idx",
        "pricing_provider_quality_domain",
        ["year", "benchmark_mode", "domain", "risk_ratio"],
        schema=schema,
    )

    op.create_table(
        "pricing_provider_quality_score",
        sa.Column("npi", sa.BigInteger(), nullable=False),
        sa.Column("year", sa.Integer(), nullable=False),
        sa.Column("model_version", sa.String(length=32), nullable=True),
        sa.Column("benchmark_mode", sa.String(length=32), nullable=False),
        sa.Column("risk_ratio_point", sa.Float(), nullable=True),
        sa.Column("ci75_low", sa.Float(), nullable=True),
        sa.Column("ci75_high", sa.Float(), nullable=True),
        sa.Column("ci90_low", sa.Float(), nullable=True),
        sa.Column("ci90_high", sa.Float(), nullable=True),
        sa.Column("score_0_100", sa.Float(), nullable=True),
        sa.Column("tier", sa.String(length=32), nullable=True),
        sa.Column("borderline_status", sa.Boolean(), nullable=True),
        sa.Column("low_score_threshold_failed", sa.Boolean(), nullable=True),
        sa.Column("low_confidence_threshold_failed", sa.Boolean(), nullable=True),
        sa.Column("high_score_threshold_passed", sa.Boolean(), nullable=True),
        sa.Column("high_confidence_threshold_passed", sa.Boolean(), nullable=True),
        sa.Column("estimated_cost_level", sa.String(length=5), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("npi", "year", "benchmark_mode"),
        schema=schema,
    )
    op.execute(
        f"CREATE INDEX IF NOT EXISTS pricing_quality_score_year_mode_tier_score_desc_idx "
        f"ON {schema}.pricing_provider_quality_score (year, benchmark_mode, tier, score_0_100 DESC)"
    )
    op.create_index(
        "pricing_quality_score_year_mode_rr_idx",
        "pricing_provider_quality_score",
        ["year", "benchmark_mode", "risk_ratio_point"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_score_year_mode_borderline_idx",
        "pricing_provider_quality_score",
        ["year", "benchmark_mode", "borderline_status"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_score_run_id_idx",
        "pricing_provider_quality_score",
        ["run_id"],
        schema=schema,
    )
    op.create_index(
        "pricing_quality_score_year_benchmark_model_idx",
        "pricing_provider_quality_score",
        ["year", "benchmark_mode", "model_version"],
        schema=schema,
    )

    op.create_table(
        "pricing_quality_run",
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("import_id", sa.String(length=32), nullable=True),
        sa.Column("year", sa.Integer(), nullable=True),
        sa.Column("qpp_source_version", sa.String(length=128), nullable=True),
        sa.Column("svi_source_version", sa.String(length=128), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=True),
        sa.Column("qpp_rows", sa.Integer(), nullable=True),
        sa.Column("svi_rows", sa.Integer(), nullable=True),
        sa.Column("measure_rows", sa.Integer(), nullable=True),
        sa.Column("domain_rows", sa.Integer(), nullable=True),
        sa.Column("score_rows", sa.Integer(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("run_id"),
        schema=schema,
    )
    op.create_index("pricing_quality_run_year_status_idx", "pricing_quality_run", ["year", "status"], schema=schema)
    op.create_index("pricing_quality_run_created_at_idx", "pricing_quality_run", ["created_at"], schema=schema)
    op.create_index("pricing_quality_run_updated_at_idx", "pricing_quality_run", ["updated_at"], schema=schema)


def downgrade():
    schema = _schema()

    op.drop_index("pricing_quality_run_updated_at_idx", table_name="pricing_quality_run", schema=schema)
    op.drop_index("pricing_quality_run_created_at_idx", table_name="pricing_quality_run", schema=schema)
    op.drop_index("pricing_quality_run_year_status_idx", table_name="pricing_quality_run", schema=schema)
    op.drop_table("pricing_quality_run", schema=schema)

    op.drop_index("pricing_quality_score_year_benchmark_model_idx", table_name="pricing_provider_quality_score", schema=schema)
    op.drop_index("pricing_quality_score_run_id_idx", table_name="pricing_provider_quality_score", schema=schema)
    op.drop_index("pricing_quality_score_year_mode_borderline_idx", table_name="pricing_provider_quality_score", schema=schema)
    op.drop_index("pricing_quality_score_year_mode_rr_idx", table_name="pricing_provider_quality_score", schema=schema)
    op.drop_index("pricing_quality_score_year_mode_tier_score_desc_idx", table_name="pricing_provider_quality_score", schema=schema)
    op.drop_table("pricing_provider_quality_score", schema=schema)

    op.drop_index("pricing_quality_domain_year_mode_domain_rr_idx", table_name="pricing_provider_quality_domain", schema=schema)
    op.drop_index("pricing_quality_domain_year_mode_domain_score_desc_idx", table_name="pricing_provider_quality_domain", schema=schema)
    op.drop_table("pricing_provider_quality_domain", schema=schema)

    op.drop_index("pricing_quality_measure_year_mode_domain_rr_idx", table_name="pricing_provider_quality_measure", schema=schema)
    op.drop_index("pricing_quality_measure_year_mode_peer_domain_idx", table_name="pricing_provider_quality_measure", schema=schema)
    op.drop_index("pricing_quality_measure_year_mode_npi_domain_idx", table_name="pricing_provider_quality_measure", schema=schema)
    op.drop_index("pricing_quality_measure_year_mode_domain_measure_idx", table_name="pricing_provider_quality_measure", schema=schema)
    op.drop_table("pricing_provider_quality_measure", schema=schema)

    op.drop_index("pricing_svi_year_overall_idx", table_name="pricing_svi_zcta", schema=schema)
    op.drop_index("pricing_svi_year_zcta_idx", table_name="pricing_svi_zcta", schema=schema)
    op.drop_table("pricing_svi_zcta", schema=schema)

    op.drop_index("pricing_qpp_year_cost_score_desc_idx", table_name="pricing_qpp_provider", schema=schema)
    op.drop_index("pricing_qpp_year_quality_score_desc_idx", table_name="pricing_qpp_provider", schema=schema)
    op.drop_index("pricing_qpp_year_final_score_desc_idx", table_name="pricing_qpp_provider", schema=schema)
    op.drop_index("pricing_qpp_year_npi_idx", table_name="pricing_qpp_provider", schema=schema)
    op.drop_table("pricing_qpp_provider", schema=schema)
