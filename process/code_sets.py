# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import html
import os
import re
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from db.connection import init_db
from db.models import CodeCatalog, db
from process.ext.utils import ensure_database

DEFAULT_POS_URL = "https://www.cms.gov/medicare/coding-billing/place-of-service-codes/code-sets"
DEFAULT_RC_URL = "https://bluebutton.cms.gov/fhir/CodeSystem/CLM-REV-CNTR-CD/"
SOURCE_POS = "cms_place_of_service_code_set"
SOURCE_RC = "cms_bluebutton_revenue_center_code"


@dataclass(frozen=True)
class CodeSetRow:
    code_system: str
    code: str
    display_name: str
    short_description: str | None = None
    long_description: str | None = None
    source: str = ""


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "tr":
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
        elif tag == "br" and self._current_cell is not None:
            self._current_cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append(_clean_text("".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            self._current_cell = None


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _download_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "HealthPorta code-set importer",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return raw.decode(charset, errors="replace")


def _parse_tables(source_html: str) -> list[list[str]]:
    parser = _TableParser()
    parser.feed(source_html)
    return parser.rows


def _expand_code_range(raw_code: str, width: int) -> list[str]:
    text = _clean_text(raw_code)
    range_match = re.fullmatch(r"(\d{1,4})\s*-\s*(\d{1,4})", text)
    if range_match:
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        if start <= end and end - start <= 100:
            return [str(value).zfill(width) for value in range(start, end + 1)]
    digits = re.sub(r"\D", "", text)
    if not digits:
        return []
    return [digits.zfill(width)]


def parse_pos_code_rows(source_html: str) -> list[CodeSetRow]:
    rows: list[CodeSetRow] = []
    for cells in _parse_tables(source_html):
        if len(cells) < 3:
            continue
        codes = _expand_code_range(cells[0], 2)
        if not codes:
            continue
        name = _clean_text(cells[1])
        description = _clean_text(cells[2])
        if not name or name.lower() == "place of service name":
            continue
        for code in codes:
            rows.append(
                CodeSetRow(
                    code_system="POS",
                    code=code,
                    display_name=name,
                    short_description=name,
                    long_description=description or None,
                    source=SOURCE_POS,
                )
            )
    return rows


def parse_revenue_code_rows(source_html: str) -> list[CodeSetRow]:
    rows: list[CodeSetRow] = []
    for cells in _parse_tables(source_html):
        if len(cells) < 2:
            continue
        codes = _expand_code_range(cells[0], 4)
        if not codes:
            continue
        display_name = _clean_text(cells[1])
        if not display_name or "revenue" in cells[0].lower():
            continue
        for code in codes:
            rows.append(
                CodeSetRow(
                    code_system="RC",
                    code=code,
                    display_name=display_name,
                    short_description=display_name,
                    long_description=None,
                    source=SOURCE_RC,
                )
            )
    return rows


async def _ensure_code_catalog(schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.status(
        f"""
        ALTER TABLE {schema}.{CodeCatalog.__tablename__}
            ALTER COLUMN code_system TYPE VARCHAR(32),
            ALTER COLUMN code TYPE VARCHAR(128),
            ALTER COLUMN display_name TYPE TEXT,
            ALTER COLUMN short_description TYPE TEXT,
            ALTER COLUMN long_description TYPE TEXT,
            ALTER COLUMN source TYPE VARCHAR(128);
        """
    )
    await db.status(
        f"ALTER TABLE {schema}.{CodeCatalog.__tablename__} ADD COLUMN IF NOT EXISTS code_checksum INTEGER;"
    )


async def _upsert_code_rows(schema: str, rows: list[CodeSetRow]) -> int:
    seen: set[tuple[str, str]] = set()
    inserted = 0
    for row in rows:
        key = (row.code_system, row.code)
        if key in seen:
            continue
        seen.add(key)
        await db.status(
            f"""
            INSERT INTO {schema}.{CodeCatalog.__tablename__}
                (code_system, code, code_checksum, display_name, short_description, long_description, is_active, source, updated_at)
            VALUES
                (:code_system, :code, hashtext(UPPER(:checksum_system) || '|' || UPPER(:checksum_code)),
                 :display_name, :short_description, :long_description, TRUE, :source, NOW())
            ON CONFLICT (code_system, code) DO UPDATE
            SET
                code_checksum = excluded.code_checksum,
                display_name = excluded.display_name,
                short_description = excluded.short_description,
                long_description = excluded.long_description,
                is_active = excluded.is_active,
                source = excluded.source,
                updated_at = excluded.updated_at;
            """,
            code_system=row.code_system,
            code=row.code,
            checksum_system=row.code_system,
            checksum_code=row.code,
            display_name=row.display_name,
            short_description=row.short_description,
            long_description=row.long_description,
            source=row.source,
        )
        inserted += 1
    return inserted


async def import_code_sets(test_mode: bool = False) -> dict[str, Any]:
    await ensure_database(test_mode)
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await _ensure_code_catalog(schema)

    pos_url = os.getenv("HLTHPRT_CODE_SETS_POS_URL", DEFAULT_POS_URL)
    rc_url = os.getenv("HLTHPRT_CODE_SETS_RC_URL", DEFAULT_RC_URL)
    pos_html, rc_html = await asyncio.gather(
        asyncio.to_thread(_download_text, pos_url),
        asyncio.to_thread(_download_text, rc_url),
    )
    pos_rows = parse_pos_code_rows(pos_html)
    rc_rows = parse_revenue_code_rows(rc_html)
    if test_mode:
        pos_rows = [row for row in pos_rows if row.code in {"21", "22", "23"}] or pos_rows[:10]
        rc_rows = [row for row in rc_rows if row.code in {"0450", "0981"}] or rc_rows[:10]
    if not pos_rows:
        raise RuntimeError(f"CMS POS source produced no code rows: {pos_url}")
    if not rc_rows:
        raise RuntimeError(f"CMS Blue Button revenue-code source produced no code rows: {rc_url}")

    pos_count = await _upsert_code_rows(schema, pos_rows)
    rc_count = await _upsert_code_rows(schema, rc_rows)
    result = {
        "pos_rows": pos_count,
        "rc_rows": rc_count,
        "pos_url": pos_url,
        "rc_url": rc_url,
    }
    print(
        "Code set import done: "
        f"POS={pos_count:,} RC={rc_count:,} at {datetime.datetime.utcnow().isoformat()}Z"
    )
    return result


async def main(test_mode: bool = False) -> dict[str, Any]:
    await init_db(db)
    try:
        return await import_code_sets(test_mode=test_mode)
    finally:
        await db.disconnect()
