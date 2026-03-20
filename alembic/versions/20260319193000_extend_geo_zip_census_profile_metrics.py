"""Extend geo ZIP census profile metrics"""

import os

from alembic import op
import sqlalchemy as sa


revision = "20260319193000_extend_geo_zip_census_profile_metrics"
down_revision = "20260319160000_add_geo_zip_census_profile"
branch_labels = None
depends_on = None


def _schema():
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def upgrade():
    schema = _schema()

    op.add_column("geo_zip_census_profile", sa.Column("business_employment", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("business_payroll_annual_k", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("hispanic_or_latino_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("poverty_rate_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("median_age", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("unemployment_rate_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("labor_force_participation_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("vacancy_rate_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("median_home_value", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("median_gross_rent", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("commute_mean_minutes", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("commute_mode_drove_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("commute_mode_carpool_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("commute_mode_public_transit_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("commute_mode_walked_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("commute_mode_worked_from_home_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("broadband_access_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("race_white_alone", sa.Integer(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_black_or_african_american_alone", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_american_indian_and_alaska_native_alone", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("race_asian_alone", sa.Integer(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_native_hawaiian_and_other_pacific_islander_alone", sa.Integer(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("race_some_other_race_alone", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("race_two_or_more_races", sa.Integer(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("race_white_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_black_or_african_american_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_american_indian_and_alaska_native_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("race_asian_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("race_native_hawaiian_and_other_pacific_islander_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("race_some_other_race_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("race_two_or_more_races_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("acs_white_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("acs_black_or_african_american_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("acs_american_indian_and_alaska_native_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("acs_asian_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column(
        "geo_zip_census_profile",
        sa.Column("acs_native_hawaiian_and_other_pacific_islander_alone_pct", sa.Float(), nullable=True),
        schema=schema,
    )
    op.add_column("geo_zip_census_profile", sa.Column("acs_some_other_race_alone_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("acs_two_or_more_races_pct", sa.Float(), nullable=True), schema=schema)
    op.add_column("geo_zip_census_profile", sa.Column("acs_hispanic_or_latino_pct", sa.Float(), nullable=True), schema=schema)


def downgrade():
    schema = _schema()

    op.drop_column("geo_zip_census_profile", "acs_hispanic_or_latino_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_two_or_more_races_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_some_other_race_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_native_hawaiian_and_other_pacific_islander_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_asian_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_american_indian_and_alaska_native_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_black_or_african_american_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "acs_white_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_two_or_more_races_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_some_other_race_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_native_hawaiian_and_other_pacific_islander_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_asian_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_american_indian_and_alaska_native_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_black_or_african_american_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_white_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_two_or_more_races", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_some_other_race_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_native_hawaiian_and_other_pacific_islander_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_asian_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_american_indian_and_alaska_native_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_black_or_african_american_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "race_white_alone", schema=schema)
    op.drop_column("geo_zip_census_profile", "broadband_access_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mode_worked_from_home_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mode_walked_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mode_public_transit_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mode_carpool_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mode_drove_alone_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "commute_mean_minutes", schema=schema)
    op.drop_column("geo_zip_census_profile", "median_gross_rent", schema=schema)
    op.drop_column("geo_zip_census_profile", "median_home_value", schema=schema)
    op.drop_column("geo_zip_census_profile", "vacancy_rate_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "labor_force_participation_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "unemployment_rate_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "median_age", schema=schema)
    op.drop_column("geo_zip_census_profile", "poverty_rate_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "hispanic_or_latino_pct", schema=schema)
    op.drop_column("geo_zip_census_profile", "business_payroll_annual_k", schema=schema)
    op.drop_column("geo_zip_census_profile", "business_employment", schema=schema)
