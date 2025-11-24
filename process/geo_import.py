# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import logging
import os
from pathlib import Path
from typing import Dict, List

import click

from db.models import GeoZipLookup, db
from process.ext.utils import ensure_database

logger = logging.getLogger(__name__)

SUPPORT_ZIP_DIR = Path(__file__).resolve().parents[1] / "support" / "zip"
DEFAULT_SOURCE = SUPPORT_ZIP_DIR / "geo_city_public.csv"


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


async def _flush_rows(rows: List[Dict]):
    if not rows:
        return
    table = GeoZipLookup.__table__
    insert_stmt = db.insert(table).values(rows)
    update_cols = {
        column.name: getattr(insert_stmt.excluded, column.name)
        for column in table.c
        if not column.primary_key
    }
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=GeoZipLookup.__my_index_elements__,
        set_=update_cols,
    )
    await insert_stmt.status()
    rows.clear()


async def load_geo_lookup(source_file: Path | None = None):
    csv_path = Path(source_file) if source_file else DEFAULT_SOURCE
    if not csv_path.exists():
        raise FileNotFoundError(f"Geo source file not found: {csv_path}")

    await ensure_database(False)
    await db.create_table(GeoZipLookup.__table__, checkfirst=True)
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await db.status(f"TRUNCATE TABLE {schema}.{GeoZipLookup.__tablename__};")

    rows: List[Dict] = []
    processed = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            zip_code = (row.get("Zip Code") or "").strip()
            city = (row.get("Official USPS city name") or "").strip()
            if not zip_code or not city:
                continue
            state = (row.get("Official USPS State Code") or "").strip().upper()
            state_name = (row.get("Official State Name") or "").strip()
            county_name = (row.get("Primary Official County Name") or "").strip()
            county_code = (row.get("Primary Official County Code") or "").strip()
            timezone = (row.get("Timezone") or "").strip()
            population = _parse_population(row.get("Population"))
            lat, lon = _parse_geo_point(row.get("Geo Point"))

            record = {
                "zip_code": zip_code.rjust(5, "0"),
                "city": city,
                "city_lower": city.lower(),
                "state": state,
                "state_name": state_name,
                "county_name": county_name,
                "county_code": county_code,
                "latitude": lat,
                "longitude": lon,
                "timezone": timezone,
                "population": population,
            }
            rows.append(record)
            processed += 1
            if len(rows) >= 2000:
                await _flush_rows(rows)

    await _flush_rows(rows)
    logger.info("Loaded %s geo zip rows from %s", processed, csv_path)


@click.command(help="Load geo zip lookup data from support/zip CSV files.")
@click.option(
    "--file",
    "file_path",
    type=click.Path(path_type=Path),
    help="Optional path to the geo CSV file (defaults to support/zip/geo_city_public.csv).",
)
def geo_lookup(file_path: Path | None = None):
    asyncio.run(load_geo_lookup(file_path))
