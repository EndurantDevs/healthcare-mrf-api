# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""CMS HTML discovery, parsing, download, and cancellation contracts for MS-DRG."""

from __future__ import annotations

import asyncio
import html
import os
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from process.control_cancel import ImportCancelledError
from process.redis_config import build_redis_settings
from process.url_security import fetch_max_bytes, urlopen_safe

try:
    import redis
except ImportError:  # pragma: no cover - redis is present in normal importer runtime
    redis = None

MS_DRG_DEFAULT_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_CMS_MS_DRG_PAGE_URL = (
    "https://www.cms.gov/medicare/payment/prospective-payment-systems/"
    "acute-inpatient-pps/ms-drg-classifications-and-software"
)
DEFAULT_MANUAL_TOC_URL = (
    "https://www.cms.gov/icd10m/FY2026-fr-v43.1-fullcode-cms/fullcode_cms/P0001.html"
)


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

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
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
        if (
            tag in {"td", "th"}
            and self._current_cell is not None
            and self._current_row is not None
        ):
            self._current_row.append(_clean_text("".join(self._current_cell)))
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            self._current_cell = None


def _clean_text(value: Any) -> str:
    normalized_text = html.unescape(str(value or "")).replace("\xa0", " ")
    return re.sub(r"\s+", " ", normalized_text).strip()


def _download_text(url: str) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "HealthPorta MS-DRG importer",
            "Accept": "text/html,application/xhtml+xml,text/plain;q=0.9,*/*;q=0.8",
        },
    )
    response_bytes, charset = urlopen_safe(
        request,
        timeout=120,
        max_bytes=fetch_max_bytes(MS_DRG_DEFAULT_MAX_BYTES),
    )
    return response_bytes.decode(charset or "utf-8", errors="replace")


def _parse_tables(source_html: str) -> list[list[str]]:
    table_parser = _TableParser()
    table_parser.feed(source_html)
    return table_parser.rows


def _extract_links(source_html: str) -> list[tuple[str, str, int]]:
    extracted_links: list[tuple[str, str, int]] = []
    link_pattern = r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>"
    for link_match in re.finditer(link_pattern, source_html, re.I | re.S):
        href = html.unescape(link_match.group(1))
        link_text = _clean_text(re.sub(r"<[^>]+>", " ", link_match.group(2)))
        extracted_links.append((href, link_text, link_match.start()))
    return extracted_links


def _find_latest_manual_toc_url(
    cms_page_html: str,
    cms_page_url: str,
) -> str | None:
    candidate_urls: list[tuple[int, str]] = []
    for href, link_text, position in _extract_links(cms_page_html):
        normalized_text = link_text.lower()
        if "definitions manual table of contents" not in normalized_text:
            continue
        heading_matches = list(
            re.finditer(
                r"<h[23][^>]*>(.*?)</h[23]>",
                cms_page_html[:position],
                re.I | re.S,
            )
        )
        closest_heading = ""
        if heading_matches:
            heading_html = heading_matches[-1].group(1)
            closest_heading = _clean_text(re.sub(r"<[^>]+>", " ", heading_html)).lower()
        is_draft_candidate = (
            "proposed" in normalized_text
            or "draft" in normalized_text
            or "test grouper" in closest_heading
            or "proposed rule" in closest_heading
        )
        if is_draft_candidate:
            continue
        candidate_score = position
        if "-fr-" in href.lower():
            candidate_score -= 20
        if "v43.1" in href.lower():
            candidate_score -= 10
        candidate_urls.append(
            (candidate_score, urllib.parse.urljoin(cms_page_url, href))
        )
    if not candidate_urls:
        return None
    return sorted(candidate_urls, key=lambda candidate: candidate[0])[0][1]


def _extract_release(source_html: str, fallback_url: str) -> str:
    normalized_text = _clean_text(re.sub(r"<[^>]+>", " ", source_html))
    version_match = re.search(r"\bv(?:ersion\s*)?(\d+(?:\.\d+)?)\b", normalized_text, re.I)
    if version_match:
        return f"v{version_match.group(1)}"
    url_match = re.search(r"v(\d+(?:[.-]\d+)?)", fallback_url, re.I)
    if url_match:
        return "v" + url_match.group(1).replace("-", ".")
    return "unknown"


def _find_link(source_html: str, text_pattern: str, base_url: str) -> str | None:
    compiled_pattern = re.compile(text_pattern, re.I)
    for href, link_text, _position in _extract_links(source_html):
        if compiled_pattern.search(link_text):
            return urllib.parse.urljoin(base_url, href)
    return None


def _parse_ms_drg_catalog_rows(source_html: str) -> list[MsDrgCatalogRow]:
    catalog_rows: list[MsDrgCatalogRow] = []
    for table_match in re.finditer(r"<tr>\s*<td>([^<]+)", source_html, re.I):
        raw_entry = _clean_text(table_match.group(1))
        entry_parts = [_clean_text(part) for part in raw_entry.split(",", 3)]
        if len(entry_parts) != 4:
            continue
        code, mdc, designation, title = entry_parts
        if not re.fullmatch(r"\d{3}", code) or not title:
            continue
        catalog_rows.append(
            MsDrgCatalogRow(
                code=code,
                mdc=mdc or None,
                designation=designation or None,
                title=title,
            )
        )
    return catalog_rows


def _expand_ms_drg_values(raw_value: str) -> list[str]:
    expanded_codes: list[str] = []
    for code_match in re.finditer(r"(\d{3})\s*-\s*(\d{3})|(\d{3})", raw_value or ""):
        if code_match.group(3):
            expanded_codes.append(code_match.group(3))
            continue
        range_start = int(code_match.group(1))
        range_end = int(code_match.group(2))
        if range_start <= range_end and range_end - range_start <= 100:
            expanded_codes.extend(
                f"{code_value:03d}"
                for code_value in range(range_start, range_end + 1)
            )
    return expanded_codes


def _parse_diagnosis_index_relationships(
    source_html: str,
) -> tuple[set[tuple[str, str, str, str, str]], set[str]]:
    relationships: set[tuple[str, str, str, str, str]] = set()
    diagnosis_codes: set[str] = set()
    for table_cells in _parse_tables(source_html):
        if len(table_cells) < 3 or table_cells[0].upper() in {"DX", "DIAGNOSIS"}:
            continue
        for column_offset in (0, 4, 8):
            if len(table_cells) <= column_offset + 2:
                continue
            diagnosis_code = re.sub(
                r"[^A-Z0-9]",
                "",
                table_cells[column_offset].upper(),
            )
            if not re.fullmatch(r"[A-Z][A-Z0-9]{2,7}", diagnosis_code):
                continue
            diagnosis_codes.add(diagnosis_code)
            for ms_drg_code in _expand_ms_drg_values(table_cells[column_offset + 2]):
                relationships.add(
                    ("MS_DRG", ms_drg_code, "uses_icd10cm", "ICD10CM", diagnosis_code)
                )
                relationships.add(
                    ("ICD10CM", diagnosis_code, "groups_to_ms_drg", "MS_DRG", ms_drg_code)
                )
    return relationships, diagnosis_codes


def _parse_procedure_index_relationships(
    source_html: str,
) -> tuple[set[tuple[str, str, str, str, str]], dict[str, str]]:
    relationships: set[tuple[str, str, str, str, str]] = set()
    procedure_category_by_code: dict[str, str] = {}
    current_code: str | None = None
    for table_cells in _parse_tables(source_html):
        if len(table_cells) < 4 or table_cells[0].upper() == "CODE":
            continue
        raw_code = re.sub(r"[^A-Z0-9]", "", table_cells[0].upper())
        if raw_code:
            current_code = raw_code
        if not current_code or not re.fullmatch(r"[A-Z0-9]{7}", current_code):
            continue
        procedure_category = _clean_text(table_cells[3])
        if procedure_category:
            procedure_category_by_code.setdefault(current_code, procedure_category)
        for ms_drg_code in _expand_ms_drg_values(table_cells[2]):
            relationships.add(
                ("MS_DRG", ms_drg_code, "uses_icd10pcs", "ICD10PCS", current_code)
            )
            relationships.add(
                ("ICD10PCS", current_code, "groups_to_ms_drg", "MS_DRG", ms_drg_code)
            )
    return relationships, procedure_category_by_code


def _discover_sequential_index_urls(
    first_page_html: str,
    first_page_url: str,
    limit: int | None = None,
) -> list[str]:
    page_heading = re.search(r"Page\s+(\d+)\s+of\s+(\d+)", first_page_html, re.I)
    next_link = re.search(
        r'id=["\']next_page["\']\s+href=["\']([^"\']+)["\']',
        first_page_html,
        re.I,
    )
    page_urls = [first_page_url]
    if not page_heading or not next_link:
        return page_urls[:limit] if limit else page_urls
    total_pages = int(page_heading.group(2))
    next_name = next_link.group(1).rsplit("/", 1)[-1]
    number_match = re.fullmatch(r"P(\d{4})\.html", next_name, re.I)
    if not number_match:
        return page_urls[:limit] if limit else page_urls
    next_number = int(number_match.group(1))
    page_count = total_pages if limit is None else min(total_pages, max(limit, 1))
    page_urls.extend(
        urllib.parse.urljoin(first_page_url, f"P{next_number + offset:04d}.html")
        for offset in range(page_count - 1)
    )
    return page_urls


async def _download_many(
    urls: list[str],
    concurrency: int,
) -> list[tuple[str, str]]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def fetch(url: str) -> tuple[str, str]:
        """Download one MS-DRG source under the shared concurrency limit."""
        async with semaphore:
            return url, await asyncio.to_thread(_download_text, url)

    return await asyncio.gather(*(fetch(url) for url in urls))


def _is_cancel_requested(run_id: str | None) -> bool:
    if not run_id or redis is None:
        return False
    try:
        redis_settings = build_redis_settings()
        redis_dsn = os.getenv("HLTHPRT_REDIS_ADDRESS")
        if redis_dsn:
            cancel_client = redis.Redis.from_url(
                redis_dsn,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        else:
            cancel_client = redis.Redis(
                host=redis_settings.host,
                port=redis_settings.port,
                password=redis_settings.password,
                db=redis_settings.database,
                socket_connect_timeout=2,
                socket_timeout=2,
            )
        cancel_value = cancel_client.get(f"cancel:{run_id}")
        return cancel_value in {b"1", "1", 1, True}
    except Exception:
        return False


def _raise_if_cancelled(run_id: str | None) -> None:
    if _is_cancel_requested(run_id):
        raise ImportCancelledError(f"import run {run_id} was cancelled")
