# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import html
import os
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from db.connection import init_db
from db.models import CodeCatalog, CodeRelationship, CodeSynonym, db
from process.control_cancel import ImportCancelledError
from process.ext.utils import ensure_database, make_class, push_objects
from process.redis_config import build_redis_settings
from process.url_security import fetch_max_bytes, urlopen_safe

# Tighter default cap for MS-DRG: these are CMS HTML TOC/index pages, not bulk MRF
# downloads. HLTHPRT_FETCH_MAX_BYTES overrides it per deployment.
MS_DRG_DEFAULT_MAX_BYTES = 64 * 1024 * 1024

try:
    import redis
except ImportError:  # pragma: no cover - redis is present in normal importer runtime
    redis = None


DEFAULT_CMS_MS_DRG_PAGE_URL = (
    "https://www.cms.gov/medicare/payment/prospective-payment-systems/"
    "acute-inpatient-pps/ms-drg-classifications-and-software"
)
DEFAULT_MANUAL_TOC_URL = "https://www.cms.gov/icd10m/FY2026-fr-v43.1-fullcode-cms/fullcode_cms/P0001.html"

SOURCE_MS_DRG = "cms_ms_drg_definitions_manual"
SOURCE_ICD10CM_INDEX = "cms_ms_drg_icd10cm_index"
SOURCE_ICD10PCS_INDEX = "cms_ms_drg_icd10pcs_index"
SOURCES = (SOURCE_MS_DRG, SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX)

CMS_MS_DRG_ATTRIBUTION = (
    "Centers for Medicare & Medicaid Services (CMS), MS-DRG Classifications and Software, "
    f"{DEFAULT_CMS_MS_DRG_PAGE_URL}"
)

DEFAULT_CONCURRENCY = 10
TEST_INDEX_PAGE_LIMIT = 2
BATCH_SIZE = 5000


@dataclass(frozen=True)
class MsDrgCatalogRow:
    code: str
    mdc: str | None
    designation: str | None
    title: str


class _TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.rows: list[list[str]] = []
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Begin table-row or cell capture for an opening HTML tag."""
        if tag == "tr":
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []
        elif tag == "br" and self._current_cell is not None:
            self._current_cell.append(" ")

    def handle_data(self, data: str) -> None:
        """Append text to the active HTML table cell."""
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        """Finalize captured HTML table cells and rows."""
        if tag in {"td", "th"} and self._current_cell is not None and self._current_row is not None:
            self._current_row.append(_clean_text("".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            self._current_cell = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_import_id(raw: str | None) -> str:
    cleaned = "".join(ch for ch in str(raw or os.getenv("HLTHPRT_MS_DRG_IMPORT_ID") or "") if ch.isalnum())
    if cleaned:
        return cleaned[:32]
    return _now().strftime("%Y%m%d")


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def _download_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "HealthPorta MS-DRG importer",
            "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8",
        },
    )
    raw, charset = urlopen_safe(
        request,
        timeout=120,
        max_bytes=fetch_max_bytes(MS_DRG_DEFAULT_MAX_BYTES),
    )
    return raw.decode(charset or "utf-8", errors="replace")


def _parse_tables(source_html: str) -> list[list[str]]:
    parser = _TableParser()
    parser.feed(source_html)
    return parser.rows


def _extract_links(source_html: str) -> list[tuple[str, str, int]]:
    links: list[tuple[str, str, int]] = []
    for match in re.finditer(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", source_html, re.I | re.S):
        href = html.unescape(match.group(1))
        text = _clean_text(re.sub(r"<[^>]+>", " ", match.group(2)))
        links.append((href, text, match.start()))
    return links


def _find_latest_manual_toc_url(cms_page_html: str, cms_page_url: str) -> str | None:
    candidates: list[tuple[int, str]] = []
    for href, text, position in _extract_links(cms_page_html):
        normalized_text = text.lower()
        if "definitions manual table of contents" not in normalized_text:
            continue
        prior = cms_page_html[:position]
        heading_matches = list(re.finditer(r"<h[23][^>]*>(.*?)</h[23]>", prior, re.I | re.S))
        closest_heading = ""
        if heading_matches:
            closest_heading = _clean_text(re.sub(r"<[^>]+>", " ", heading_matches[-1].group(1))).lower()
        if (
            "proposed" in normalized_text
            or "draft" in normalized_text
            or "test grouper" in closest_heading
            or "proposed rule" in closest_heading
        ):
            continue
        score = 0
        if "-fr-" in href.lower():
            score -= 20
        if "v43.1" in href.lower():
            score -= 10
        candidates.append((score + position, urllib.parse.urljoin(cms_page_url, href)))
    if not candidates:
        return None
    return sorted(candidates, key=lambda item: item[0])[0][1]


def _extract_release(source_html: str, fallback_url: str) -> str:
    text = _clean_text(re.sub(r"<[^>]+>", " ", source_html))
    match = re.search(r"\bv(?:ersion\s*)?(\d+(?:\.\d+)?)\b", text, re.I)
    if match:
        return f"v{match.group(1)}"
    url_match = re.search(r"v(\d+(?:[.-]\d+)?)", fallback_url, re.I)
    if url_match:
        return "v" + url_match.group(1).replace("-", ".")
    return "unknown"


def _find_link(source_html: str, text_pattern: str, base_url: str) -> str | None:
    pattern = re.compile(text_pattern, re.I)
    for href, text, _position in _extract_links(source_html):
        if pattern.search(text):
            return urllib.parse.urljoin(base_url, href)
    return None


def _parse_ms_drg_catalog_rows(source_html: str) -> list[MsDrgCatalogRow]:
    rows: list[MsDrgCatalogRow] = []
    for match in re.finditer(r"<tr>\s*<td>([^<]+)", source_html, re.I):
        raw = _clean_text(match.group(1))
        parts = [_clean_text(part) for part in raw.split(",", 3)]
        if len(parts) != 4:
            continue
        code, mdc, designation, title = parts
        if not re.fullmatch(r"\d{3}", code) or not title:
            continue
        rows.append(
            MsDrgCatalogRow(
                code=code,
                mdc=mdc or None,
                designation=designation or None,
                title=title,
            )
        )
    return rows


def _expand_ms_drg_values(raw_value: str) -> list[str]:
    values: list[str] = []
    for match in re.finditer(r"(\d{3})\s*-\s*(\d{3})|(\d{3})", raw_value or ""):
        if match.group(3):
            values.append(match.group(3))
            continue
        start = int(match.group(1))
        end = int(match.group(2))
        if start <= end and end - start <= 100:
            values.extend(f"{value:03d}" for value in range(start, end + 1))
    return values


def _parse_diagnosis_index_relationships(source_html: str) -> tuple[set[tuple[str, str, str, str, str]], set[str]]:
    relationships: set[tuple[str, str, str, str, str]] = set()
    diagnosis_codes: set[str] = set()
    for cells in _parse_tables(source_html):
        if len(cells) < 3 or cells[0].upper() in {"DX", "DIAGNOSIS"}:
            continue
        for offset in (0, 4, 8):
            if len(cells) <= offset + 2:
                continue
            diagnosis_code = re.sub(r"[^A-Z0-9]", "", cells[offset].upper())
            if not diagnosis_code or not re.fullmatch(r"[A-Z][A-Z0-9]{2,7}", diagnosis_code):
                continue
            diagnosis_codes.add(diagnosis_code)
            for ms_drg in _expand_ms_drg_values(cells[offset + 2]):
                relationships.add(("MS_DRG", ms_drg, "uses_icd10cm", "ICD10CM", diagnosis_code))
                relationships.add(("ICD10CM", diagnosis_code, "groups_to_ms_drg", "MS_DRG", ms_drg))
    return relationships, diagnosis_codes


def _parse_procedure_index_relationships(source_html: str) -> tuple[set[tuple[str, str, str, str, str]], dict[str, str]]:
    relationships: set[tuple[str, str, str, str, str]] = set()
    procedure_codes: dict[str, str] = {}
    current_code: str | None = None
    for cells in _parse_tables(source_html):
        if len(cells) < 4 or cells[0].upper() == "CODE":
            continue
        raw_code = re.sub(r"[^A-Z0-9]", "", cells[0].upper())
        if raw_code:
            current_code = raw_code
        if not current_code or not re.fullmatch(r"[A-Z0-9]{7}", current_code):
            continue
        category = _clean_text(cells[3])
        if category:
            procedure_codes.setdefault(current_code, category)
        for ms_drg in _expand_ms_drg_values(cells[2]):
            relationships.add(("MS_DRG", ms_drg, "uses_icd10pcs", "ICD10PCS", current_code))
            relationships.add(("ICD10PCS", current_code, "groups_to_ms_drg", "MS_DRG", ms_drg))
    return relationships, procedure_codes


def _discover_sequential_index_urls(first_page_html: str, first_page_url: str, limit: int | None = None) -> list[str]:
    heading = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", first_page_html, re.I)
    next_match = re.search(r'id=["\']next_page["\']\s+href=["\']([^"\']+)["\']', first_page_html, re.I)
    urls = [first_page_url]
    if not heading or not next_match:
        return urls[:limit] if limit else urls

    total_pages = int(heading.group(2))
    next_href = next_match.group(1)
    next_name = next_href.rsplit("/", 1)[-1]
    number_match = re.fullmatch(r"P(\d{4})\.html", next_name, re.I)
    if not number_match:
        return urls[:limit] if limit else urls

    next_number = int(number_match.group(1))
    page_count = total_pages if limit is None else min(total_pages, max(limit, 1))
    for offset in range(page_count - 1):
        urls.append(urllib.parse.urljoin(first_page_url, f"P{next_number + offset:04d}.html"))
    return urls


async def _download_many(urls: list[str], concurrency: int) -> list[tuple[str, str]]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def fetch(url: str) -> tuple[str, str]:
        """Download one MS-DRG source under the shared concurrency limit."""
        async with semaphore:
            return url, await asyncio.to_thread(_download_text, url)

    return await asyncio.gather(*(fetch(url) for url in urls))


def _cancel_requested(run_id: str | None) -> bool:
    if not run_id or redis is None:
        return False
    try:
        settings = build_redis_settings()
        dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
        if dsn:
            client = redis.Redis.from_url(dsn, socket_connect_timeout=2, socket_timeout=2)
        else:
            client = redis.Redis(
                host=settings.host,
                port=settings.port,
                password=settings.password,
                db=settings.database,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        value = client.get(f"cancel:{run_id}")
        return value in {b"1", "1", 1, True}
    except Exception:
        return False


def _raise_if_cancelled(run_id: str | None) -> None:
    if _cancel_requested(run_id):
        raise ImportCancelledError(f"import run {run_id} was cancelled")


def _catalog_row(
    *,
    code_system: str,
    code: str,
    code_type: str,
    display_name: str,
    short_description: str | None,
    long_description: str | None,
    source: str,
    source_release: str,
) -> dict[str, Any]:
    return {
        "code_system": code_system,
        "code": code,
        "code_type": code_type,
        "display_name": display_name,
        "short_description": short_description,
        "long_description": long_description,
        "is_active": True,
        "source": source,
        "source_release": source_release,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _synonym_row(code: str, synonym: str, term_type: str) -> dict[str, Any]:
    return {
        "code_system": "MS_DRG",
        "code": code,
        "synonym": synonym,
        "term_type": term_type,
        "language": "ENG",
        "source": SOURCE_MS_DRG,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _relationship_row(
    from_system: str,
    from_code: str,
    relationship: str,
    to_system: str,
    to_code: str,
    source: str,
) -> dict[str, Any]:
    return {
        "from_system": from_system,
        "from_code": from_code,
        "relationship": relationship,
        "to_system": to_system,
        "to_code": to_code,
        "source": source,
        "source_attribution": CMS_MS_DRG_ATTRIBUTION,
        "updated_at": _now(),
    }


def _build_catalog_and_synonym_rows(
    catalog_rows: list[MsDrgCatalogRow],
    procedure_codes: dict[str, str],
    release: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    catalog_payloads: list[dict[str, Any]] = []
    synonym_payloads: list[dict[str, Any]] = []
    for row in catalog_rows:
        designation = {"M": "medical", "P": "surgical"}.get(row.designation or "", row.designation or "unspecified")
        details = [f"MS-DRG {row.code}", f"{designation} designation"]
        if row.mdc:
            details.append(row.mdc)
        catalog_payloads.append(
            _catalog_row(
                code_system="MS_DRG",
                code=row.code,
                code_type="inpatient_case_group",
                display_name=row.title,
                short_description=row.title,
                long_description="; ".join(details),
                source=SOURCE_MS_DRG,
                source_release=release,
            )
        )
        synonym_payloads.append(_synonym_row(row.code, f"MS-DRG {row.code}", "alias"))
        synonym_payloads.append(_synonym_row(row.code, f"DRG {row.code}", "alias"))
        synonym_payloads.append(_synonym_row(row.code, row.title, "preferred"))

    for code, category in sorted(procedure_codes.items()):
        catalog_payloads.append(
            _catalog_row(
                code_system="ICD10PCS",
                code=code,
                code_type="procedure",
                display_name=f"ICD-10-PCS {code}",
                short_description=category or None,
                long_description=category or None,
                source=SOURCE_ICD10PCS_INDEX,
                source_release=release,
            )
        )
    return catalog_payloads, synonym_payloads


async def _ensure_tables(schema: str) -> None:
    await db.create_table(CodeCatalog.__table__, checkfirst=True)
    await db.create_table(CodeSynonym.__table__, checkfirst=True)
    await db.create_table(CodeRelationship.__table__, checkfirst=True)
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


async def _push(stage_cls, rows: list[dict[str, Any]]) -> int:
    count = 0
    for start in range(0, len(rows), BATCH_SIZE):
        chunk = rows[start : start + BATCH_SIZE]
        await push_objects(chunk, stage_cls)
        count += len(chunk)
    return count


def _source_sql_list(sources: tuple[str, ...]) -> str:
    return ", ".join(f"'{source}'" for source in sources)


async def _merge_catalog_stage(stage_cls, schema: str, sources_to_replace: tuple[str, ...]) -> None:
    sources = _source_sql_list(sources_to_replace)
    await db.status(f"DELETE FROM {schema}.{CodeCatalog.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeCatalog.__tablename__}
            (code_system, code, code_type, display_name, short_description, long_description,
             is_active, source, source_release, source_attribution, updated_at)
        SELECT code_system, code, code_type, display_name, short_description, long_description,
               is_active, source, source_release, source_attribution, updated_at
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT (code_system, code) DO UPDATE SET
            code_type = EXCLUDED.code_type,
            display_name = EXCLUDED.display_name,
            short_description = EXCLUDED.short_description,
            long_description = EXCLUDED.long_description,
            is_active = EXCLUDED.is_active,
            source = EXCLUDED.source,
            source_release = EXCLUDED.source_release,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_synonym_stage(stage_cls, schema: str, sources_to_replace: tuple[str, ...]) -> None:
    sources = _source_sql_list(sources_to_replace)
    await db.status(f"DELETE FROM {schema}.{CodeSynonym.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeSynonym.__tablename__}
            (code_system, code, synonym, term_type, language, source, source_attribution, updated_at)
        SELECT code_system, code, synonym, term_type, language, source, source_attribution, updated_at
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT (code_system, code, synonym, term_type) DO UPDATE SET
            language = EXCLUDED.language,
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def _merge_relationship_stage(stage_cls, schema: str, sources_to_replace: tuple[str, ...]) -> None:
    sources = _source_sql_list(sources_to_replace)
    await db.status(f"DELETE FROM {schema}.{CodeRelationship.__tablename__} WHERE source IN ({sources});")
    await db.status(
        f"""
        INSERT INTO {schema}.{CodeRelationship.__tablename__}
            (from_system, from_code, relationship, to_system, to_code, source, source_attribution, updated_at)
        SELECT from_system, from_code, relationship, to_system, to_code, source, source_attribution, updated_at
          FROM {schema}.{stage_cls.__tablename__}
        ON CONFLICT (from_system, from_code, relationship, to_system, to_code) DO UPDATE SET
            source = EXCLUDED.source,
            source_attribution = EXCLUDED.source_attribution,
            updated_at = EXCLUDED.updated_at;
        """
    )


async def import_ms_drg(
    *,
    test_mode: bool = False,
    include_relationships: bool = True,
    relationship_page_limit: int | None = None,
    concurrency: int | None = None,
    source_url: str | None = None,
    manual_toc_url: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Download, normalize, and persist MS-DRG reference data."""
    await ensure_database(test_mode)
    schema = _schema()
    await _ensure_tables(schema)

    cms_page_url = source_url or os.getenv("HLTHPRT_MS_DRG_CMS_PAGE_URL") or DEFAULT_CMS_MS_DRG_PAGE_URL
    if manual_toc_url:
        toc_url = manual_toc_url
        cms_page_html = ""
    else:
        cms_page_html = await asyncio.to_thread(_download_text, cms_page_url)
        toc_url = (
            os.getenv("HLTHPRT_MS_DRG_MANUAL_TOC_URL")
            or _find_latest_manual_toc_url(cms_page_html, cms_page_url)
            or DEFAULT_MANUAL_TOC_URL
        )

    _raise_if_cancelled(run_id)
    toc_html = await asyncio.to_thread(_download_text, toc_url)
    release = _extract_release(toc_html, toc_url)
    appendix_url = _find_link(toc_html, r"appendix\s+a\s+list\s+of\s+ms-drgs", toc_url)
    if not appendix_url:
        raise RuntimeError(f"Could not find MS-DRG Appendix A link in {toc_url}")
    appendix_html = await asyncio.to_thread(_download_text, appendix_url)
    list_url = _find_link(appendix_html, r"list\s+of\s+ms-drgs", appendix_url) or appendix_url
    list_html = await asyncio.to_thread(_download_text, list_url)
    catalog_rows = _parse_ms_drg_catalog_rows(list_html)
    if test_mode:
        smoke_codes = {"001", "031", "097", "371", "470", "714", "791", "820"}
        catalog_rows = [row for row in catalog_rows if row.code in smoke_codes] or catalog_rows[:20]
    if not catalog_rows:
        raise RuntimeError(f"CMS MS-DRG list produced no rows: {list_url}")

    relationship_limit = relationship_page_limit
    if test_mode and relationship_limit is None:
        relationship_limit = TEST_INDEX_PAGE_LIMIT
    effective_concurrency = max(int(concurrency or os.getenv("HLTHPRT_MS_DRG_CONCURRENCY", DEFAULT_CONCURRENCY)), 1)

    relationships: set[tuple[str, str, str, str, str]] = set()
    procedure_codes: dict[str, str] = {}
    diagnosis_codes: set[str] = set()
    diagnosis_page_count = 0
    procedure_page_count = 0

    if include_relationships:
        diagnosis_landing = _find_link(toc_html, r"diagnosis\s+code/mdc/ms-drg\s+index", toc_url)
        procedure_landing = _find_link(toc_html, r"procedure\s+code/ms-drg\s+index", toc_url)
        if not diagnosis_landing or not procedure_landing:
            missing = []
            if not diagnosis_landing:
                missing.append("diagnosis code/MDC/MS-DRG index")
            if not procedure_landing:
                missing.append("procedure code/MS-DRG index")
            raise RuntimeError(f"Could not find CMS MS-DRG relationship index link(s): {', '.join(missing)}")
        if diagnosis_landing:
            landing_html = await asyncio.to_thread(_download_text, diagnosis_landing)
            first_url = _find_link(landing_html, r"diagnosis\s+code/mdc/ms-drg\s+index", diagnosis_landing) or diagnosis_landing
            first_html = await asyncio.to_thread(_download_text, first_url)
            urls = _discover_sequential_index_urls(first_html, first_url, relationship_limit)
            diagnosis_page_count = len(urls)
            page_payloads = [(first_url, first_html)]
            page_payloads.extend(await _download_many(urls[1:], effective_concurrency))
            for _url, page_html in page_payloads:
                rels, codes = _parse_diagnosis_index_relationships(page_html)
                relationships.update(rels)
                diagnosis_codes.update(codes)
                _raise_if_cancelled(run_id)
        if procedure_landing:
            landing_html = await asyncio.to_thread(_download_text, procedure_landing)
            first_url = _find_link(landing_html, r"procedure\s+code/ms-drg\s+index", procedure_landing) or procedure_landing
            first_html = await asyncio.to_thread(_download_text, first_url)
            urls = _discover_sequential_index_urls(first_html, first_url, relationship_limit)
            procedure_page_count = len(urls)
            page_payloads = [(first_url, first_html)]
            page_payloads.extend(await _download_many(urls[1:], effective_concurrency))
            for _url, page_html in page_payloads:
                rels, codes = _parse_procedure_index_relationships(page_html)
                relationships.update(rels)
                procedure_codes.update(codes)
                _raise_if_cancelled(run_id)

    if test_mode and relationships:
        allowed_ms_drgs = {row.code for row in catalog_rows}
        relationships = {
            row
            for row in relationships
            if (row[0] == "MS_DRG" and row[1] in allowed_ms_drgs) or (row[3] == "MS_DRG" and row[4] in allowed_ms_drgs)
        }
        procedure_codes = {
            code: category
            for code, category in procedure_codes.items()
            if any((rel[3] == "ICD10PCS" and rel[4] == code) or (rel[0] == "ICD10PCS" and rel[1] == code) for rel in relationships)
        }
    if include_relationships and not relationships:
        raise RuntimeError("CMS MS-DRG relationship indexes produced no relationships")

    catalog_payloads, synonym_payloads = _build_catalog_and_synonym_rows(catalog_rows, procedure_codes, release)
    relationship_payloads = [
        _relationship_row(*row, SOURCE_ICD10PCS_INDEX if "ICD10PCS" in {row[0], row[3]} else SOURCE_ICD10CM_INDEX)
        for row in sorted(relationships)
    ]

    suffix = _normalize_import_id(import_id)
    stages = {
        CodeCatalog: make_class(CodeCatalog, suffix),
        CodeSynonym: make_class(CodeSynonym, suffix),
        CodeRelationship: make_class(CodeRelationship, suffix),
    }
    for stage_cls in stages.values():
        await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_cls.__tablename__};")
        await db.create_table(stage_cls.__table__, checkfirst=True)

    catalog_count = await _push(stages[CodeCatalog], catalog_payloads)
    synonym_count = await _push(stages[CodeSynonym], synonym_payloads)
    relationship_count = await _push(stages[CodeRelationship], relationship_payloads) if include_relationships else 0

    catalog_sources = SOURCES if include_relationships else (SOURCE_MS_DRG,)
    await _merge_catalog_stage(stages[CodeCatalog], schema, catalog_sources)
    await _merge_synonym_stage(stages[CodeSynonym], schema, (SOURCE_MS_DRG,))
    if include_relationships:
        await _merge_relationship_stage(stages[CodeRelationship], schema, (SOURCE_ICD10CM_INDEX, SOURCE_ICD10PCS_INDEX))

    result = {
        "source_url": cms_page_url,
        "manual_toc_url": toc_url,
        "ms_drg_list_url": list_url,
        "source_release": release,
        "catalog_rows": catalog_count,
        "ms_drg_rows": len(catalog_rows),
        "synonym_rows": synonym_count,
        "relationship_rows": relationship_count,
        "icd10cm_codes_observed": len(diagnosis_codes),
        "icd10pcs_rows": len(procedure_codes),
        "diagnosis_index_pages": diagnosis_page_count,
        "procedure_index_pages": procedure_page_count,
        "include_relationships": include_relationships,
    }
    print(
        "MS-DRG import done: "
        f"MS_DRG={len(catalog_rows):,} catalog={catalog_count:,} synonyms={synonym_count:,} "
        f"relationships={relationship_count:,} ICD10PCS={len(procedure_codes):,} "
        f"release={release} at {_now().isoformat()}Z"
    )
    return result


async def main(
    test_mode: bool = False,
    include_relationships: bool = True,
    relationship_page_limit: int | None = None,
    concurrency: int | None = None,
    source_url: str | None = None,
    manual_toc_url: str | None = None,
    import_id: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Run the MS-DRG import entry point."""
    await init_db(db)
    try:
        return await import_ms_drg(
            test_mode=test_mode,
            include_relationships=include_relationships,
            relationship_page_limit=relationship_page_limit,
            concurrency=concurrency,
            source_url=source_url,
            manual_toc_url=manual_toc_url,
            import_id=import_id,
            run_id=run_id,
        )
    finally:
        await db.disconnect()
