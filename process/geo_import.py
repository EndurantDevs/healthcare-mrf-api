# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import logging
import re
from collections.abc import Iterator
from pathlib import Path

import click
from sqlalchemy import Table

from db.models import GeoZipLookup, db
from process.ext.utils import ensure_database

logger = logging.getLogger(__name__)

SUPPORT_ZIP_DIR = Path(__file__).resolve().parents[1] / "support" / "zip"
DEFAULT_SOURCE = SUPPORT_ZIP_DIR / "geo_city_public.csv"
IMPORT_BATCH_SIZE = 2000
REQUIRED_GEO_SOURCE_FIELDS = (
    "Zip Code",
    "Official USPS city name",
    "Official USPS State Code",
    "Official State Name",
    "Population",
    "Primary Official County Code",
    "Primary Official County Name",
    "Timezone",
    "Geo Point",
)
SQL_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class GeoSourceValidationError(ValueError):
    """Raised when a geo lookup source cannot be safely imported."""


def _parse_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_population(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _parse_geo_point(value):
    if not value:
        return None, None
    parts = [part.strip() for part in value.split(",")]
    if len(parts) != 2:
        return None, None
    lat = _parse_float(parts[0])
    lon = _parse_float(parts[1])
    return lat, lon


def _normalize_county_code(value):
    if value in (None, ""):
        return None
    digits = "".join(ch for ch in str(value).strip() if ch.isdigit())
    if not digits:
        return None
    return digits[-5:].rjust(5, "0")


def _geo_zip_values_from_csv_fields(
    csv_fields: dict[str, str | None],
) -> dict[str, object] | None:
    zip_code = (csv_fields.get("Zip Code") or "").strip()
    city = (csv_fields.get("Official USPS city name") or "").strip()
    if not zip_code or not city:
        return None

    latitude, longitude = _parse_geo_point(csv_fields.get("Geo Point"))
    return {
        "zip_code": zip_code.rjust(5, "0"),
        "city": city,
        "city_lower": city.lower(),
        "state": (csv_fields.get("Official USPS State Code") or "").strip().upper(),
        "state_name": (csv_fields.get("Official State Name") or "").strip(),
        "county_name": (csv_fields.get("Primary Official County Name") or "").strip(),
        "county_code": _normalize_county_code(
            csv_fields.get("Primary Official County Code")
        ),
        "latitude": latitude,
        "longitude": longitude,
        "timezone": (csv_fields.get("Timezone") or "").strip(),
        "population": _parse_population(csv_fields.get("Population")),
    }


def _validate_sql_identifier(value: object, *, label: str) -> str:
    identifier = str(value or "").strip()
    if not SQL_IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid {label}: {value!r}")
    return identifier


def _qualified_geo_zip_table(target_table: Table) -> str:
    schema_name = _validate_sql_identifier(
        target_table.schema,
        label="GeoZipLookup schema",
    )
    table_name = _validate_sql_identifier(
        target_table.name,
        label="GeoZipLookup table name",
    )
    return f'"{schema_name}"."{table_name}"'


def _require_geo_source_headers(field_names: list[str] | None) -> None:
    available_fields = set(field_names or ())
    missing_fields = [
        field_name
        for field_name in REQUIRED_GEO_SOURCE_FIELDS
        if field_name not in available_fields
    ]
    if missing_fields:
        raise GeoSourceValidationError(
            "Geo source is not a semicolon-delimited CSV with the required "
            f"headers; missing: {', '.join(missing_fields)}"
        )


def _iter_geo_source_fields(
    csv_path: Path,
) -> Iterator[dict[str, str | None]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";", strict=True)
        _require_geo_source_headers(reader.fieldnames)
        yield from reader


def _preflight_geo_source(csv_path: Path) -> int:
    try:
        if not csv_path.exists():
            raise FileNotFoundError(f"Geo source file not found: {csv_path}")
        if not csv_path.is_file():
            raise GeoSourceValidationError(
                f"Geo source is not a regular file: {csv_path}"
            )

        valid_geo_row_count = sum(
            1
            for csv_fields in _iter_geo_source_fields(csv_path)
            if _geo_zip_values_from_csv_fields(csv_fields) is not None
        )
    except FileNotFoundError:
        raise
    except GeoSourceValidationError:
        raise
    except (OSError, UnicodeError, csv.Error) as exc:
        raise GeoSourceValidationError(
            f"Geo source is not a readable UTF-8 semicolon CSV: {csv_path}"
        ) from exc

    if valid_geo_row_count == 0:
        raise GeoSourceValidationError(
            f"Geo source contains no valid ZIP and city rows: {csv_path}"
        )
    return valid_geo_row_count


async def _flush_rows(
    pending_geo_rows: list[dict[str, object]],
    *,
    target_table: Table | None = None,
) -> None:
    if not pending_geo_rows:
        return
    table = target_table if target_table is not None else GeoZipLookup.__table__
    insert_stmt = db.insert(table).values(pending_geo_rows)
    column_update_map = {
        column.name: getattr(insert_stmt.excluded, column.name)
        for column in table.c
        if not column.primary_key
    }
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=GeoZipLookup.__my_index_elements__,
        set_=column_update_map,
    )
    await insert_stmt.status()
    pending_geo_rows.clear()


async def load_geo_lookup(
    source_file: Path | None = None,
    *,
    test_mode: bool = False,
) -> None:
    """Load geographic lookup rows from the configured CSV source."""
    csv_path = Path(source_file) if source_file else DEFAULT_SOURCE
    expected_geo_row_count = _preflight_geo_source(csv_path)
    target_table = GeoZipLookup.__table__
    qualified_table_name = _qualified_geo_zip_table(target_table)

    await ensure_database(bool(test_mode))
    await db.create_table(target_table, checkfirst=True)

    pending_geo_rows: list[dict[str, object]] = []
    processed = 0
    async with db.transaction():
        await db.status(f"TRUNCATE TABLE {qualified_table_name};")
        for csv_fields in _iter_geo_source_fields(csv_path):
            geo_zip_values = _geo_zip_values_from_csv_fields(csv_fields)
            if geo_zip_values is None:
                continue
            pending_geo_rows.append(geo_zip_values)
            processed += 1
            if len(pending_geo_rows) >= IMPORT_BATCH_SIZE:
                await _flush_rows(
                    pending_geo_rows,
                    target_table=target_table,
                )

        await _flush_rows(
            pending_geo_rows,
            target_table=target_table,
        )
        if processed != expected_geo_row_count:
            raise GeoSourceValidationError(
                "Geo source changed after preflight: "
                f"expected {expected_geo_row_count} valid rows, read {processed}"
            )

    logger.info("Loaded %s geo zip rows from %s", processed, csv_path)


async def main(test_mode: bool = False, file_path: Path | str | None = None) -> dict[str, object]:
    """Run the geographic lookup import entry point."""
    source_file = Path(file_path) if file_path else None
    await load_geo_lookup(
        source_file=source_file,
        test_mode=bool(test_mode),
    )
    return {"test_mode": bool(test_mode), "source_file": str(source_file or DEFAULT_SOURCE)}


@click.command(help="Load geo zip lookup data from support/zip CSV files.")
@click.option(
    "--file",
    "file_path",
    type=click.Path(path_type=Path),
    help="Optional path to the geo CSV file (defaults to support/zip/geo_city_public.csv).",
)
def geo_lookup(file_path: Path | None = None):
    """Run the geographic lookup import synchronously."""
    asyncio.run(load_geo_lookup(file_path))
