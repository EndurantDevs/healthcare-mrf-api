# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import logging
import os
import re
from dataclasses import dataclass
from typing import Dict

import aiohttp
import click

from db.models import GeoZipCensusProfile, db
from process.ext.utils import ensure_database, get_http_client

logger = logging.getLogger(__name__)

CENSUS_API_BASE = "https://api.census.gov/data"
DEFAULT_ACS5_YEAR = 2024
DEFAULT_DECENNIAL_YEAR = 2020
DEFAULT_CBP_YEAR = 2023
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_RETRIES = 3
DEFAULT_RETRY_DELAY_SECONDS = 1.0
DEFAULT_TEST_ROW_LIMIT = 500
MAX_INSERT_PARAMS = 30000
IMPORT_BATCH_SIZE = max(100, MAX_INSERT_PARAMS // max(1, len(GeoZipCensusProfile.__table__.columns)))

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


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    dataset: str
    geography: str
    zip_column: str
    fields: tuple[tuple[str, str, str], ...]


def _resolve_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default


def _resolve_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default


def _normalize_zip(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    token = ZIP_TOKEN.findall(text)
    if token:
        return token[-1]
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 5:
        return digits
    if len(digits) > 5:
        return digits[-5:]
    return None


def _clean_raw_value(value: object) -> str | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.lower() in SUPPRESSED_VALUES:
        return None
    return text


def _to_int(value: object) -> int | None:
    text = _clean_raw_value(value)
    if text is None:
        return None
    try:
        return int(float(text.replace(",", "")))
    except (TypeError, ValueError):
        return None


def _to_float(value: object) -> float | None:
    text = _clean_raw_value(value)
    if text is None:
        return None
    try:
        return float(text.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _to_pct(numerator: int | float | None, denominator: int | float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    try:
        return (float(numerator) / float(denominator)) * 100.0
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _finalize_profile_record(record: dict[str, object]) -> None:
    total_population = record.get("total_population")
    for count_field, pct_field in RACE_COUNT_FIELDS:
        record[pct_field] = _to_pct(record.get(count_field), total_population)
    record["hispanic_or_latino_pct"] = _to_pct(record.get("hispanic_or_latino"), total_population)


def _dataset_specs(acs5_year: int, decennial_year: int, cbp_year: int) -> tuple[DatasetSpec, ...]:
    return (
        DatasetSpec(
            name="acs_subject",
            dataset=f"{acs5_year}/acs/acs5/subject",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=(
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
            ),
        ),
        DatasetSpec(
            name="acs_profile",
            dataset=f"{acs5_year}/acs/acs5/profile",
            geography="zip code tabulation area:*",
            zip_column="zip code tabulation area",
            fields=(
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
            ),
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
            fields=(
                ("total_population", "P1_001N", "int"),
                ("hispanic_or_latino", "P9_002N", "int"),
                ("race_white_alone", "P8_003N", "int"),
                ("race_black_or_african_american_alone", "P8_004N", "int"),
                ("race_american_indian_and_alaska_native_alone", "P8_005N", "int"),
                ("race_asian_alone", "P8_006N", "int"),
                ("race_native_hawaiian_and_other_pacific_islander_alone", "P8_007N", "int"),
                ("race_some_other_race_alone", "P8_008N", "int"),
                ("race_two_or_more_races", "P8_009N", "int"),
            ),
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


async def _fetch_dataset_rows(
    client: aiohttp.ClientSession,
    spec: DatasetSpec,
    api_key: str | None,
    timeout_seconds: int,
    retries: int,
    retry_delay_seconds: float,
    test_mode: bool,
    test_row_limit: int,
) -> dict[str, dict[str, int | float | None]]:
    params: dict[str, str] = {
        "get": ",".join(["NAME", *(var for _, var, _ in spec.fields)]),
        "for": spec.geography,
    }
    if api_key:
        params["key"] = api_key

    url = f"{CENSUS_API_BASE}/{spec.dataset}"
    last_error: Exception | None = None
    payload = None
    for attempt in range(1, retries + 1):
        try:
            async with client.get(
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as response:
                body = await response.text()
                if response.status >= 400:
                    raise RuntimeError(
                        f"Census API {spec.name} failed status={response.status}: {body[:220]}"
                    )
                payload = await response.json(content_type=None)
            break
        except Exception as exc:  # pylint: disable=broad-exception-caught
            last_error = exc
            if attempt >= retries:
                break
            await asyncio.sleep(min(retry_delay_seconds * attempt, 20.0))

    if payload is None:
        raise RuntimeError(
            f"Unable to fetch Census dataset {spec.name} from {url}: {last_error!r}"
        )

    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"Unexpected Census payload for {spec.name}: {type(payload)!r}")

    header = payload[0]
    if not isinstance(header, list):
        raise RuntimeError(f"Census header malformed for {spec.name}")
    index = {str(col): idx for idx, col in enumerate(header)}
    if spec.zip_column not in index:
        raise RuntimeError(
            f"Census payload for {spec.name} missing geography column {spec.zip_column!r}"
        )

    rows = payload[1:]
    if test_mode:
        rows = rows[: max(1, test_row_limit)]

    result: dict[str, dict[str, int | float | None]] = {}
    for row in rows:
        if not isinstance(row, list):
            continue
        zip_code = _normalize_zip(row[index[spec.zip_column]])
        if not zip_code:
            continue
        values: dict[str, int | float | None] = {}
        for field_name, variable, data_type in spec.fields:
            var_index = index.get(variable)
            raw_value = row[var_index] if var_index is not None and var_index < len(row) else None
            if data_type == "float":
                values[field_name] = _to_float(raw_value)
            else:
                values[field_name] = _to_int(raw_value)
        result[zip_code] = values

    logger.info("Fetched Census %s rows=%s", spec.name, len(result))
    return result


def _build_base_record(zip_code: str) -> dict[str, object]:
    return {
        "zip_code": zip_code,
        "total_population": None,
        "median_household_income": None,
        "bachelors_degree_or_higher_pct": None,
        "employment_rate_pct": None,
        "total_housing_units": None,
        "without_health_insurance_pct": None,
        "total_employer_establishments": None,
        "business_employment": None,
        "business_payroll_annual_k": None,
        "total_households": None,
        "hispanic_or_latino": None,
        "hispanic_or_latino_pct": None,
        "poverty_rate_pct": None,
        "median_age": None,
        "unemployment_rate_pct": None,
        "labor_force_participation_pct": None,
        "vacancy_rate_pct": None,
        "median_home_value": None,
        "median_gross_rent": None,
        "commute_mean_minutes": None,
        "commute_mode_drove_alone_pct": None,
        "commute_mode_carpool_pct": None,
        "commute_mode_public_transit_pct": None,
        "commute_mode_walked_pct": None,
        "commute_mode_worked_from_home_pct": None,
        "broadband_access_pct": None,
        "race_white_alone": None,
        "race_black_or_african_american_alone": None,
        "race_american_indian_and_alaska_native_alone": None,
        "race_asian_alone": None,
        "race_native_hawaiian_and_other_pacific_islander_alone": None,
        "race_some_other_race_alone": None,
        "race_two_or_more_races": None,
        "race_white_alone_pct": None,
        "race_black_or_african_american_alone_pct": None,
        "race_american_indian_and_alaska_native_alone_pct": None,
        "race_asian_alone_pct": None,
        "race_native_hawaiian_and_other_pacific_islander_alone_pct": None,
        "race_some_other_race_alone_pct": None,
        "race_two_or_more_races_pct": None,
        "acs_white_alone_pct": None,
        "acs_black_or_african_american_alone_pct": None,
        "acs_american_indian_and_alaska_native_alone_pct": None,
        "acs_asian_alone_pct": None,
        "acs_native_hawaiian_and_other_pacific_islander_alone_pct": None,
        "acs_some_other_race_alone_pct": None,
        "acs_two_or_more_races_pct": None,
        "acs_hispanic_or_latino_pct": None,
        "updated_at": datetime.datetime.utcnow(),
    }


async def _collect_profile_map(test_mode: bool = False) -> Dict[str, Dict[str, object]]:
    api_key = (os.getenv("HLTHPRT_CENSUS_API_KEY") or "").strip() or None
    if not api_key:
        logger.warning("HLTHPRT_CENSUS_API_KEY not set; continuing with anonymous Census API access")

    acs5_year = _resolve_int_env("HLTHPRT_CENSUS_ACS5_YEAR", DEFAULT_ACS5_YEAR)
    decennial_year = _resolve_int_env("HLTHPRT_CENSUS_DECENNIAL_YEAR", DEFAULT_DECENNIAL_YEAR)
    cbp_year = _resolve_int_env("HLTHPRT_CENSUS_CBP_YEAR", DEFAULT_CBP_YEAR)
    timeout_seconds = _resolve_int_env("HLTHPRT_CENSUS_HTTP_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)
    retries = max(1, _resolve_int_env("HLTHPRT_CENSUS_DOWNLOAD_RETRIES", DEFAULT_RETRIES))
    retry_delay_seconds = max(
        0.1,
        _resolve_float_env("HLTHPRT_CENSUS_RETRY_DELAY_SECONDS", DEFAULT_RETRY_DELAY_SECONDS),
    )
    test_row_limit = max(
        1,
        _resolve_int_env("HLTHPRT_CENSUS_TEST_ROW_LIMIT", DEFAULT_TEST_ROW_LIMIT),
    )

    specs = _dataset_specs(acs5_year, decennial_year, cbp_year)
    profile_map: Dict[str, Dict[str, object]] = {}
    client = await get_http_client(use_proxy=False)
    async with client:
        for spec in specs:
            dataset_rows = await _fetch_dataset_rows(
                client,
                spec,
                api_key,
                timeout_seconds,
                retries,
                retry_delay_seconds,
                test_mode,
                test_row_limit,
            )
            if spec.zip_column == "zip code tabulation area":
                for zip_code in dataset_rows:
                    profile_map.setdefault(zip_code, _build_base_record(zip_code))

            for zip_code, row_values in dataset_rows.items():
                # CBP includes non-ZCTA ZIP entries; keep storage to ZCTA scope only.
                if zip_code not in profile_map:
                    continue
                profile_map[zip_code].update(row_values)

    for record in profile_map.values():
        _finalize_profile_record(record)

    logger.info("Prepared Census ZIP profiles: rows=%s", len(profile_map))
    return profile_map


async def _flush_rows(rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    table = GeoZipCensusProfile.__table__
    insert_stmt = db.insert(table).values(rows)
    update_cols = {
        column.name: getattr(insert_stmt.excluded, column.name)
        for column in table.c
        if not column.primary_key
    }
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=GeoZipCensusProfile.__my_index_elements__,
        set_=update_cols,
    )
    await insert_stmt.status()
    rows.clear()


async def _ensure_profile_columns(schema: str) -> None:
    for column_ddl in PROFILE_COLUMN_DDLS:
        await db.status(
            f"ALTER TABLE {schema}.{GeoZipCensusProfile.__tablename__} "
            f"ADD COLUMN IF NOT EXISTS {column_ddl};"
        )


async def load_geo_census_lookup(test_mode: bool = False) -> int:
    profiles = await _collect_profile_map(test_mode=test_mode)
    await ensure_database(test_mode)
    await db.create_table(GeoZipCensusProfile.__table__, checkfirst=True)

    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await _ensure_profile_columns(schema)
    buffered_rows: list[dict[str, object]] = []

    async with db.transaction():
        await db.status(f"TRUNCATE TABLE {schema}.{GeoZipCensusProfile.__tablename__};")
        for zip_code in sorted(profiles.keys()):
            record = profiles[zip_code]
            buffered_rows.append(record)
            if len(buffered_rows) >= IMPORT_BATCH_SIZE:
                await _flush_rows(buffered_rows)
        await _flush_rows(buffered_rows)

    logger.info("Loaded Census ZIP profile rows=%s", len(profiles))
    return len(profiles)


@click.command(help="Load Census ZIP profile metrics by ZIP/ZCTA and persist locally.")
@click.option("--test", is_flag=True, help="Load a deterministic sample of rows for quick smoke testing.")
def geo_census_lookup(test: bool) -> None:
    asyncio.run(load_geo_census_lookup(test_mode=test))
