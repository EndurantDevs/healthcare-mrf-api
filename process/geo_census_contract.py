# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Static Census dataset contract for ZIP enrichment."""

from __future__ import annotations

from dataclasses import dataclass
import re


CENSUS_API_BASE = "https://api.census.gov/data"
DEFAULT_ACS5_YEAR = 2024
DEFAULT_DECENNIAL_YEAR = 2020
DEFAULT_CBP_YEAR = 2023
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 1.0
DEFAULT_TEST_ROW_LIMIT = 500

PROFILE_COLUMN_DDLS: tuple[str, ...] = (
    "business_employment INTEGER",
    "business_payroll_annual_k INTEGER",
    "hispanic_or_latino_pct DOUBLE PRECISION",
    "poverty_rate_pct DOUBLE PRECISION",
    "median_age DOUBLE PRECISION",
    "unemployment_rate_pct DOUBLE PRECISION",
    "labor_force_participation_pct DOUBLE PRECISION",
    "vacancy_rate_pct DOUBLE PRECISION",
    "median_home_value INTEGER",
    "median_gross_rent INTEGER",
    "commute_mean_minutes DOUBLE PRECISION",
    "commute_mode_drove_alone_pct DOUBLE PRECISION",
    "commute_mode_carpool_pct DOUBLE PRECISION",
    "commute_mode_public_transit_pct DOUBLE PRECISION",
    "commute_mode_walked_pct DOUBLE PRECISION",
    "commute_mode_worked_from_home_pct DOUBLE PRECISION",
    "broadband_access_pct DOUBLE PRECISION",
    "race_white_alone INTEGER",
    "race_black_or_african_american_alone INTEGER",
    "race_american_indian_and_alaska_native_alone INTEGER",
    "race_asian_alone INTEGER",
    "race_native_hawaiian_and_other_pacific_islander_alone INTEGER",
    "race_some_other_race_alone INTEGER",
    "race_two_or_more_races INTEGER",
    "race_white_alone_pct DOUBLE PRECISION",
    "race_black_or_african_american_alone_pct DOUBLE PRECISION",
    "race_american_indian_and_alaska_native_alone_pct DOUBLE PRECISION",
    "race_asian_alone_pct DOUBLE PRECISION",
    "race_native_hawaiian_and_other_pacific_islander_alone_pct DOUBLE PRECISION",
    "race_some_other_race_alone_pct DOUBLE PRECISION",
    "race_two_or_more_races_pct DOUBLE PRECISION",
    "acs_white_alone_pct DOUBLE PRECISION",
    "acs_black_or_african_american_alone_pct DOUBLE PRECISION",
    "acs_american_indian_and_alaska_native_alone_pct DOUBLE PRECISION",
    "acs_asian_alone_pct DOUBLE PRECISION",
    "acs_native_hawaiian_and_other_pacific_islander_alone_pct DOUBLE PRECISION",
    "acs_some_other_race_alone_pct DOUBLE PRECISION",
    "acs_two_or_more_races_pct DOUBLE PRECISION",
    "acs_hispanic_or_latino_pct DOUBLE PRECISION",
)

SUPPRESSED_VALUES = {
    "",
    "-",
    "null",
    "none",
    "(x)",
    "n",
    "d",
    "s",
    "x",
    "-666666666",
    "-222222222",
    "-333333333",
    "-999999999",
    ".",
    "n/a",
    "na",
}

ZIP_TOKEN = re.compile(r"(?<!\d)(\d{5})(?!\d)")

RACE_COUNT_FIELDS = (
    ("race_white_alone", "race_white_alone_pct"),
    ("race_black_or_african_american_alone", "race_black_or_african_american_alone_pct"),
    ("race_american_indian_and_alaska_native_alone", "race_american_indian_and_alaska_native_alone_pct"),
    ("race_asian_alone", "race_asian_alone_pct"),
    (
        "race_native_hawaiian_and_other_pacific_islander_alone",
        "race_native_hawaiian_and_other_pacific_islander_alone_pct",
    ),
    ("race_some_other_race_alone", "race_some_other_race_alone_pct"),
    ("race_two_or_more_races", "race_two_or_more_races_pct"),
)

ACS_SUBJECT_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("median_household_income", "S1901_C01_012E", "int"),
    ("bachelors_degree_or_higher_pct", "S1501_C02_015E", "float"),
    ("without_health_insurance_pct", "S2701_C05_001E", "float"),
    ("poverty_rate_pct", "S1701_C03_001E", "float"),
    ("commute_mode_drove_alone_pct", "S0801_C01_003E", "float"),
    ("commute_mode_carpool_pct", "S0801_C01_004E", "float"),
    ("commute_mode_public_transit_pct", "S0801_C01_009E", "float"),
    ("commute_mode_walked_pct", "S0801_C01_010E", "float"),
    ("commute_mode_worked_from_home_pct", "S0801_C01_013E", "float"),
    ("commute_mean_minutes", "S0801_C01_046E", "float"),
    ("broadband_access_pct", "S2801_C02_014E", "float"),
)

ACS_PROFILE_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("employment_rate_pct", "DP03_0004PE", "float"),
    ("total_households", "DP02_0001E", "int"),
    ("median_age", "DP05_0018E", "float"),
    ("unemployment_rate_pct", "DP03_0009PE", "float"),
    ("labor_force_participation_pct", "DP03_0002PE", "float"),
    ("vacancy_rate_pct", "DP04_0003PE", "float"),
    ("median_home_value", "DP04_0089E", "int"),
    ("median_gross_rent", "DP04_0134E", "int"),
    ("acs_white_alone_pct", "DP05_0037PE", "float"),
    ("acs_black_or_african_american_alone_pct", "DP05_0045PE", "float"),
    ("acs_american_indian_and_alaska_native_alone_pct", "DP05_0053PE", "float"),
    ("acs_asian_alone_pct", "DP05_0061PE", "float"),
    ("acs_native_hawaiian_and_other_pacific_islander_alone_pct", "DP05_0069PE", "float"),
    ("acs_some_other_race_alone_pct", "DP05_0074PE", "float"),
    ("acs_two_or_more_races_pct", "DP05_0035PE", "float"),
    ("acs_hispanic_or_latino_pct", "DP05_0090PE", "float"),
)

DECENNIAL_DHC_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("total_population", "P1_001N", "int"),
    ("hispanic_or_latino", "P9_002N", "int"),
    ("race_white_alone", "P8_003N", "int"),
    ("race_black_or_african_american_alone", "P8_004N", "int"),
    ("race_american_indian_and_alaska_native_alone", "P8_005N", "int"),
    ("race_asian_alone", "P8_006N", "int"),
    ("race_native_hawaiian_and_other_pacific_islander_alone", "P8_007N", "int"),
    ("race_some_other_race_alone", "P8_008N", "int"),
    ("race_two_or_more_races", "P8_009N", "int"),
)


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dataset: str
    geography: str
    zip_column: str
    fields: tuple[tuple[str, str, str], ...]


def _dataset_specs(acs5_year: int, decennial_year: int, cbp_year: int) -> tuple[DatasetSpec, ...]:
    """Return the supported Census dataset specifications."""
    return (
        DatasetSpec(
            name="acs_subject",
            dataset=f"{acs5_year}/acs/acs5/subject",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=ACS_SUBJECT_FIELDS,
        ),
        DatasetSpec(
            name="acs_profile",
            dataset=f"{acs5_year}/acs/acs5/profile",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=ACS_PROFILE_FIELDS,
        ),
        DatasetSpec(
            name="acs_housing",
            dataset=f"{acs5_year}/acs/acs5",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=(("total_housing_units", "B25002_001E", "int"),),
        ),
        DatasetSpec(
            name="decennial_dhc",
            dataset=f"{decennial_year}/dec/dhc",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=DECENNIAL_DHC_FIELDS,
        ),
        DatasetSpec(
            name="cbp",
            dataset=f"{cbp_year}/cbp",
            geography="zip code:*",
            zip_column="zip code",
            fields=(
                ("total_employer_establishments", "ESTAB", "int"),
                ("business_employment", "EMP", "int"),
                ("business_payroll_annual_k", "PAYANN", "int"),
            ),
        ),
    )


__all__ = (
    "ACS_PROFILE_FIELDS",
    "ACS_SUBJECT_FIELDS",
    "CENSUS_API_BASE",
    "DEFAULT_ACS5_YEAR",
    "DEFAULT_CBP_YEAR",
    "DEFAULT_DECENNIAL_YEAR",
    "DEFAULT_RETRIES",
    "DEFAULT_RETRY_DELAY_SECONDS",
    "DEFAULT_TEST_ROW_LIMIT",
    "DEFAULT_TIMEOUT_SECONDS",
    "DECENNIAL_DHC_FIELDS",
    "DatasetSpec",
    "PROFILE_COLUMN_DDLS",
    "RACE_COUNT_FIELDS",
    "SUPPRESSED_VALUES",
    "ZIP_TOKEN",
    "_dataset_specs",
)
