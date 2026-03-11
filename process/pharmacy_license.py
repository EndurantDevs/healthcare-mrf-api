# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import csv
import datetime
import hashlib
import html
import io
import json
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass, field
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import aiohttp
from arq import create_pool

from db.connection import db
from db.models import (PartDPharmacyActivity, NPIDataTaxonomy,
                       PharmacyLicenseImportRun, PharmacyLicenseRecord,
                       PharmacyLicenseRecordHistory, PharmacyLicenseRecordStage,
                       PharmacyLicenseSnapshot,
                       PharmacyLicenseStateCoverage)
from process.ext.utils import db_startup, download_it, ensure_database, push_objects
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

FDA_STATE_BOARD_URL = (
    "https://www.fda.gov/drugs/besaferx-your-source-online-pharmacy-information/"
    "locate-state-licensed-online-pharmacy"
)

PHARM_LICENSE_QUEUE_NAME = "arq:PharmacyLicense"
PHARM_LICENSE_FINISH_QUEUE_NAME = "arq:PharmacyLicense_finish"

PHARM_LICENSE_BATCH_SIZE = max(
    int(
        os.getenv(
            "HLTHPRT_PHARM_LICENSE_BATCH_SIZE",
            os.getenv("HLTHPRT_CLAIMS_IMPORT_BATCH_SIZE", "300000"),
        )
    ),
    1000,
)
PHARM_LICENSE_TEST_MAX_STATES = max(int(os.getenv("HLTHPRT_PHARM_LICENSE_TEST_MAX_STATES", "5")), 1)
PHARM_LICENSE_TEST_MAX_ROWS_PER_STATE = max(
    int(os.getenv("HLTHPRT_PHARM_LICENSE_TEST_MAX_ROWS_PER_STATE", "200")),
    1,
)
PHARM_LICENSE_SOURCE_TIMEOUT_SECONDS = max(
    int(os.getenv("HLTHPRT_PHARM_LICENSE_SOURCE_TIMEOUT_SECONDS", "15")),
    5,
)
PHARM_LICENSE_MAX_DOWNLOAD_BYTES = max(
    int(os.getenv("HLTHPRT_PHARM_LICENSE_MAX_DOWNLOAD_BYTES", str(20 * 1024 * 1024))),
    1024 * 1024,
)
PHARM_LICENSE_MAX_CANDIDATE_URLS = max(int(os.getenv("HLTHPRT_PHARM_LICENSE_MAX_CANDIDATE_URLS", "4")), 1)
PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES = str(
    os.getenv("HLTHPRT_PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES", "1")
).strip().lower() in {"1", "true", "yes", "on"}
PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT = str(
    os.getenv("HLTHPRT_PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT", "1")
).strip().lower() in {"1", "true", "yes", "on"}
PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES = max(
    int(os.getenv("HLTHPRT_PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES", "500")),
    1,
)
PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS = max(
    int(os.getenv("HLTHPRT_PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS", "500000")),
    100,
)
PHARM_LICENSE_HTTP_USER_AGENT = os.getenv(
    "HLTHPRT_PHARM_LICENSE_HTTP_USER_AGENT",
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")
_NON_DIGIT = re.compile(r"[^0-9]+")
_DATE_PATTERN = re.compile(r"\b(20\d{2})[-/]?(\d{2})[-/]?(\d{2})\b")
_HREF_PATTERN = re.compile(r"<a[^>]+href=\"([^\"]+)\"[^>]*>([^<]+)</a>", re.IGNORECASE)
_FILE_LINK_PATTERN = re.compile(
    r"href=[\"']([^\"']+\.(?:csv|json|zip|xml)(?:\?[^\"']*)?)[\"']",
    re.IGNORECASE,
)
_FORM_ACTION_PATTERN = re.compile(
    r"<form[^>]+action=[\"']([^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
_INPUT_NAME_PATTERN = re.compile(r"<input[^>]+name=[\"']([^\"']+)[\"'][^>]*>", re.IGNORECASE)
_INPUT_TAG_PATTERN = re.compile(r"<input[^>]*>", re.IGNORECASE)
_TAG_ATTR_PATTERN = re.compile(r"([A-Za-z0-9_:\-]+)\s*=\s*[\"']([^\"']*)[\"']")
_SELECT_NAME_PATTERN = re.compile(r"<select[^>]+name=[\"']([^\"']+)[\"'][^>]*>", re.IGNORECASE)
_OPTION_PATTERN = re.compile(
    r"<option[^>]*value=[\"']([^\"']*)[\"'][^>]*>(.*?)</option>",
    re.IGNORECASE | re.DOTALL,
)
_ASP_DATAGRID_POSTBACK_PATTERN = re.compile(
    r"<a[^>]+href=\"javascript:__doPostBack\(([^)]+)\)\"[^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
_CAPTCHA_MARKERS = (
    "please solve the captcha",
    "g-recaptcha",
    "recaptcha",
    "hcaptcha",
)
_STATE_ADAPTER_ASPNET_SEARCH = "aspnet_facility_search"
_STATE_ADAPTER_DIRECT_CSV = "direct_csv_source"
_STATE_ADAPTER_SOCRATA = "socrata_source"
_STATE_ADAPTER_MA_EXPORT_API = "ma_board_export_api"
_STATE_ADAPTER_NY_ROSA_API = "ny_rosa_api"
_STATE_ADAPTER_STATIC_UNSUPPORTED = "static_unavailable"
_STATE_ADAPTER_FALLBACK_MACHINE_READABLE = "machine_readable_discovery"
_STATE_ADAPTER_CONFIG = {
    "NJ": {
        "search_url": "https://newjersey.mylicense.com/Verification_Bulk/Search.aspx?facility=Y",
    },
}
_STATE_DIRECT_CSV_CONFIG = {
    "TX": {
        "source_url": "https://www.pharmacy.texas.gov/downloads/phydsk.csv",
    },
    "FL": {
        "source_url": os.getenv(
            "HLTHPRT_FL_MQA_EXPORT_CSV_URL",
            (
                "https://mqa-internet.doh.state.fl.us/MQASearchServices/"
                "LicenseVerificationBusiness/ExportToCsv"
                "?jsonModel=%7B%22Id%22%3A0%2C%22BusinessName%22%3Anull%2C%22Profession%22%3A%222205%22"
                "%2C%22County%22%3Anull%2C%22LicenseNumber%22%3Anull%2C%22LicenseStatus%22%3A%22ALL%22"
                "%2C%22Board%22%3A%2222%22%2C%22City%22%3Anull%2C%22ZipCode%22%3Anull%7D"
            ),
        ),
    },
}
_CO_SOC_R_SRC_URL = "https://data.colorado.gov/resource/7s5z-vewr.csv"
_CO_PHARMACY_LICENSE_TYPES = (
    "NOF",
    "OO",
    "OSP",
    "PDO",
    "SPDO",
    "TPLP",
    "TPDO",
    "WHI",
    "WHO",
    "HSP",
    "MFR",
)
_WA_SOC_R_SRC_URL = "https://data.wa.gov/resource/qxh8-f4bd.csv"
_NY_ROSA_BASE_URL = "https://api.nysed.gov/rosa/V2/findPharmacies"
_NY_ROSA_API_KEY = os.getenv(
    "HLTHPRT_NY_ROSA_API_KEY",
    "BRJF4D6U646A5PNMIB77AAW9544QFQKAYAEWI9EPU0TNP72CEEO3L4KGVN5K3R44",
)
_NY_ROSA_QUERY_TERMS = tuple(
    term.strip()
    for term in os.getenv(
        "HLTHPRT_NY_ROSA_QUERY_TERMS",
        "PHARM,DRUG,RX,APOTHECARY,HOSPITAL,MEDICAL,HEALTH,CARE,SCRIPT,CLINIC",
    ).split(",")
    if term.strip()
)
_NY_ROSA_PAGE_SIZE = max(min(int(os.getenv("HLTHPRT_NY_ROSA_PAGE_SIZE", "200")), 200), 50)
_MA_EXPORT_BASE_URL = "https://healthprofessionlicensing-api.mass.gov/api-public/export/data/board"
_MA_EXPORT_BOARD_ID = os.getenv("HLTHPRT_MA_EXPORT_BOARD_ID", "BOARD_OF_REGISTRATION_IN_PHARMACY").strip()
_STATE_SOCRATA_CONFIG = {
    "CO": {
        "source_url": _CO_SOC_R_SRC_URL,
        "columns": (
            "licensetype",
            "lastname",
            "firstname",
            "middlename",
            "licensenumber",
            "city",
            "state",
            "mailzipcode",
            "licensestatusdescription",
            "licenseexpirationdate",
            "licensefirstissuedate",
            "licenselastreneweddate",
        ),
        "where": "licensetype IN ({types})".format(
            types=", ".join(f"'{code}'" for code in _CO_PHARMACY_LICENSE_TYPES)
        ),
        "page_size": 50000,
    },
    "WA": {
        "source_url": _WA_SOC_R_SRC_URL,
        "columns": (
            "credentialnumber",
            "lastname",
            "firstname",
            "middlename",
            "credentialtype",
            "status",
            "firstissuedate",
            "lastissuedate",
            "expirationdate",
            "actiontaken",
        ),
        "where": "upper(credentialtype) like '%PHARM%'",
        "page_size": 50000,
    },
}
_STATE_STATIC_UNSUPPORTED_CONFIG = {
    "CA": "requires_dca_api_access_request_or_turnstile",
}
_CORP_SUFFIX_TOKENS = {
    "and",
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "llp",
    "ltd",
    "pc",
    "pllc",
    "the",
}
_CITY_STATE_ZIP_PATTERN = re.compile(
    r"^\s*(?P<city>[A-Za-z0-9 .'\-]+?)\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)\s*$"
)
_ADDRESS_TAIL_STATE_ZIP_PATTERN = re.compile(r"(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)\s*$")
_STREET_ADDRESS_TOKENS = {
    "aly",
    "ave",
    "blvd",
    "bnd",
    "cir",
    "court",
    "ct",
    "dr",
    "hwy",
    "lane",
    "ln",
    "pkwy",
    "pl",
    "rd",
    "road",
    "route",
    "sq",
    "st",
    "street",
    "ter",
    "trl",
    "way",
}
_TABLE_SCHEMA_CACHE: dict[str, str | None] = {}

_STATUS_PRIORITY = (
    ("revoked", ("revoked", "revoke", "rescinded")),
    ("suspended", ("suspended", "suspension")),
    ("expired", ("expired", "lapsed", "not renewed")),
    ("inactive", ("inactive", "closed", "terminated", "cancelled", "canceled", "null and void")),
    ("active", ("active", "current", "valid", "licensed", "in good standing", "renewed", "clear")),
)

_DISCIPLINARY_HINTS = (
    "disciplin",
    "sanction",
    "probation",
    "restriction",
    "board action",
    "cease",
    "consent order",
)

_STATE_NAME_TO_CODE = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "guam": "GU",
    "puerto rico": "PR",
    "virgin islands": "VI",
    "u.s. virgin islands": "VI",
}

_NPI_KEYS = (
    "npi",
    "npi number",
    "national provider identifier",
    "national provider id",
    "provider npi",
    "pharmacy npi",
)
_LICENSE_NUMBER_KEYS = (
    "license number",
    "license #",
    "lic number",
    "license no",
    "lic no",
    "permit number",
    "permit #",
    "permit no",
    "registration number",
    "license",
)
_LICENSE_TYPE_KEYS = (
    "license type",
    "license class",
    "permit type",
    "credential type",
)
_STATUS_KEYS = (
    "license status",
    "status",
    "credential status",
    "permit status",
)
_EXPIRY_KEYS = (
    "expiration date",
    "expiry date",
    "license expiration",
    "exp date",
)
_ISSUE_KEYS = ("issue date", "license issue date")
_EFFECTIVE_KEYS = ("effective date", "license effective date")
_RENEWAL_KEYS = ("last renewal date", "renewal date")
_DISCIPLINARY_FLAG_KEYS = (
    "disciplinary flag",
    "disciplinary action",
    "has discipline",
    "sanctioned",
)
_DISCIPLINARY_SUMMARY_KEYS = (
    "disciplinary summary",
    "disciplinary details",
    "board action",
    "sanction details",
)
_DISCIPLINARY_DATE_KEYS = ("disciplinary action date", "board action date")
_ENTITY_NAME_KEYS = (
    "entity name",
    "full name",
    "facility name",
    "licensee name",
    "organization name",
    "pharmacy name",
    "name",
    "business name",
)
_DBA_KEYS = ("dba", "dba name", "doing business as", "trade name")
_ADDRESS1_KEYS = (
    "address",
    "address line 1",
    "street",
    "street address",
)
_ADDRESS2_KEYS = ("address line 2", "suite", "unit")
_CITY_KEYS = ("city",)
_STATE_KEYS = ("state", "state code")
_ZIP_KEYS = ("zip", "zip code", "postal code")
_PHONE_KEYS = ("phone", "phone number", "telephone")
_SOURCE_RECORD_KEYS = ("id", "record id", "license id", "credential id")
_UPDATED_KEYS = (
    "last updated",
    "updated at",
    "status date",
    "last verified",
    "modified",
)


@dataclass(frozen=True)
class StateSource:
    state_code: str
    state_name: str
    board_url: str


@dataclass
class StateImportStats:
    supported: bool
    status: str
    source_url: str | None
    unsupported_reason: str | None
    error_text: str | None
    row_count_parsed: int
    row_count_matched: int
    row_count_dropped: int
    row_count_inserted: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class AspNetStateAdapterSpec:
    state_code: str
    search_url: str


@dataclass
class StateNpiResolver:
    state_code: str
    by_license: dict[str, int] = field(default_factory=dict)
    by_name_zip: dict[tuple[str, str], int] = field(default_factory=dict)
    by_name_city: dict[tuple[str, str], int] = field(default_factory=dict)
    by_name: dict[str, int] = field(default_factory=dict)
    stats: dict[str, int] = field(default_factory=dict)

    def _hit(self, method: str, npi: int) -> int:
        self.stats[method] = self.stats.get(method, 0) + 1
        return npi

    def resolve(
        self,
        *,
        license_number: str | None,
        entity_name: str | None,
        dba_name: str | None,
        city: str | None,
        zip_code: str | None,
    ) -> int | None:
        normalized_license = _normalize_license_for_match(license_number)
        if normalized_license:
            mapped = self.by_license.get(normalized_license)
            if mapped:
                return self._hit("license", mapped)

        normalized_city = _normalize_city_for_match(city)
        normalized_zip = _normalize_zip_for_match(zip_code)
        name_candidates = [
            _normalize_name_for_match(entity_name),
            _normalize_name_for_match(dba_name),
            _normalize_name_for_match(entity_name, loose=True),
            _normalize_name_for_match(dba_name, loose=True),
        ]
        for name_key in name_candidates:
            if not name_key:
                continue
            if normalized_zip:
                mapped = self.by_name_zip.get((name_key, normalized_zip))
                if mapped:
                    return self._hit("name_zip", mapped)
            if normalized_city:
                mapped = self.by_name_city.get((name_key, normalized_city))
                if mapped:
                    return self._hit("name_city", mapped)
            mapped = self.by_name.get(name_key)
            if mapped:
                return self._hit("name", mapped)
        return None


class _HtmlTableParser(HTMLParser):
    def __init__(self, table_id: str):
        super().__init__(convert_charrefs=True)
        self.table_id = table_id
        self._inside_target_table = False
        self._table_depth = 0
        self._inside_row = False
        self._inside_cell = False
        self._cell_depth = 0
        self._cell_chunks: list[str] = []
        self._current_row: list[str] = []
        self.rows: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = dict(attrs)
        lower_tag = tag.lower()
        if lower_tag == "table":
            if self._inside_target_table:
                self._table_depth += 1
            elif attrs_dict.get("id") == self.table_id:
                self._inside_target_table = True
                self._table_depth = 1
            return
        if not self._inside_target_table:
            return
        if lower_tag == "tr" and not self._inside_row and self._table_depth == 1:
            self._inside_row = True
            self._current_row = []
            return
        if lower_tag in {"td", "th"} and self._inside_row and (self._table_depth == 1 or self._inside_cell):
            if self._inside_cell:
                self._cell_depth += 1
                return
            self._inside_cell = True
            self._cell_depth = 1
            self._cell_chunks = []

    def handle_data(self, data: str) -> None:
        if self._inside_target_table and self._inside_cell:
            self._cell_chunks.append(data)

    def handle_endtag(self, tag: str) -> None:
        lower_tag = tag.lower()
        if lower_tag == "table" and self._inside_target_table:
            self._table_depth -= 1
            if self._table_depth <= 0:
                self._inside_target_table = False
                self._table_depth = 0
            return
        if not self._inside_target_table:
            return
        if lower_tag in {"td", "th"} and self._inside_cell:
            self._cell_depth -= 1
            if self._cell_depth <= 0:
                text = " ".join(" ".join(self._cell_chunks).split())
                self._current_row.append(text)
                self._inside_cell = False
                self._cell_chunks = []
                self._cell_depth = 0
            return
        if lower_tag == "tr" and self._inside_row and self._table_depth == 1:
            if any(cell.strip() for cell in self._current_row):
                self.rows.append(self._current_row)
            self._inside_row = False
            self._current_row = []


def _normalize_run_id(run_id: str | None) -> str:
    if run_id:
        normalized = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(run_id))
        normalized = normalized.strip("_")
        if normalized:
            return normalized[:64]
    token = hashlib.sha1(str(datetime.datetime.utcnow().timestamp()).encode("utf-8")).hexdigest()[:8]
    return f"{datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{token}"


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    return normalized[:32] or datetime.date.today().strftime("%Y%m%d")


def _normalize_key(key: str) -> str:
    return _NON_ALNUM.sub("", str(key or "").strip().lower())


def _safe_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _strip_html_tags(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", str(value or ""))
    return " ".join(html.unescape(cleaned).split())


def _normalize_license_for_match(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    normalized = re.sub(r"[^A-Za-z0-9]+", "", text).upper()
    return normalized or None


def _normalize_zip_for_match(value: Any) -> str | None:
    digits = _NON_DIGIT.sub("", str(value or ""))
    if len(digits) >= 5:
        return digits[:5]
    return None


def _normalize_city_for_match(value: Any) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    normalized = _NON_ALNUM.sub(" ", text.lower()).strip()
    return " ".join(normalized.split()) or None


def _normalize_name_for_match(value: Any, *, loose: bool = False) -> str | None:
    text = _safe_text(value)
    if not text:
        return None
    normalized = _NON_ALNUM.sub(" ", text.lower()).strip()
    tokens = [token for token in normalized.split() if token]
    if loose and tokens:
        tokens = [token for token in tokens if token not in _CORP_SUFFIX_TOKENS]
    normalized_text = "".join(tokens)
    return normalized_text or None


def _unique_mapping(raw_map: dict[Any, set[int]]) -> dict[Any, int]:
    resolved: dict[Any, int] = {}
    for key, values in raw_map.items():
        if len(values) == 1:
            resolved[key] = next(iter(values))
    return resolved


def _to_date(value: Any) -> datetime.date | None:
    text = _safe_text(value)
    if not text:
        return None
    candidate = text.split("T", 1)[0].split(" ", 1)[0]
    try:
        return datetime.date.fromisoformat(candidate)
    except ValueError:
        pass
    for date_format in ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
        try:
            return datetime.datetime.strptime(candidate, date_format).date()
        except ValueError:
            continue
    match = _DATE_PATTERN.search(text)
    if not match:
        return None
    year, month, day = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"1", "true", "yes", "y", "active", "open"}:
        return True
    if text in {"0", "false", "no", "n", "inactive", "closed"}:
        return False
    return None


def _to_npi(value: Any) -> int | None:
    digits = _NON_DIGIT.sub("", str(value or ""))
    if len(digits) == 12 and digits.startswith("1") and digits.endswith("0"):
        digits = digits[1:-1]
    elif len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    try:
        parsed = int(digits)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _normalize_license_status(raw_status: Any) -> str:
    text = str(raw_status or "").strip().lower()
    if not text:
        return "unknown"
    for normalized, markers in _STATUS_PRIORITY:
        for marker in markers:
            if marker in text:
                return normalized
    return "unknown"


def _state_code_for_name(state_name: str) -> str | None:
    key = " ".join(str(state_name or "").strip().lower().split())
    return _STATE_NAME_TO_CODE.get(key)


def _hash_snapshot_id(run_id: str, state_code: str, board_url: str) -> str:
    digest = hashlib.sha1(f"{run_id}|{state_code}|{board_url}".encode("utf-8")).hexdigest()[:12]
    return f"{run_id}:{state_code}:{digest}"[:128]


def _entry_extensions(url: str | None) -> str:
    if not url:
        return ""
    path = urlparse(url).path.lower()
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".json"):
        return "json"
    if path.endswith(".zip"):
        return "zip"
    if path.endswith(".xml"):
        return "xml"
    return ""


def _is_noise_link(url: str) -> bool:
    lower = url.lower()
    return any(
        token in lower
        for token in (
            "sitemap",
            "sample",
            "salesforce",
            "embeddedservice",
            "visitor/settings",
            "robots.txt",
            "favicon",
            "theme",
            "stylesheet",
        )
    )


def _parse_fda_state_sources(html: str) -> list[StateSource]:
    section = html
    marker = "Board of Pharmacy License Databases by State"
    if marker in html:
        section = html.split(marker, 1)[1]
    if "</ul>" in section:
        section = section.split("</ul>", 1)[0]

    states: list[StateSource] = []
    seen_state_codes: set[str] = set()
    for href, state_text in _HREF_PATTERN.findall(section):
        state_name = " ".join(state_text.replace("\xa0", " ").split()).strip()
        state_code = _state_code_for_name(state_name)
        if not state_code:
            continue
        if state_code in seen_state_codes:
            continue
        board_url = href.strip()
        if not board_url:
            continue
        seen_state_codes.add(state_code)
        states.append(StateSource(state_code=state_code, state_name=state_name, board_url=board_url))
    states.sort(key=lambda item: item.state_code)
    return states


def _extract_hidden_fields(page_html: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for input_tag in _INPUT_TAG_PATTERN.findall(page_html):
        attrs = {key.lower(): html.unescape(value) for key, value in _TAG_ATTR_PATTERN.findall(input_tag)}
        if attrs.get("type", "").lower() != "hidden":
            continue
        name = attrs.get("name")
        if not name or not name.startswith("__"):
            continue
        fields[name] = attrs.get("value", "")
    return fields


def _extract_lookup_field_names(page_html: str) -> set[str]:
    names: set[str] = set()
    for match in _INPUT_NAME_PATTERN.findall(page_html):
        if match.startswith("t_web_lookup__"):
            names.add(match)
    for name in _SELECT_NAME_PATTERN.findall(page_html):
        if name.startswith("t_web_lookup__"):
            names.add(name)
    return names


def _extract_select_options(page_html: str, field_name: str) -> list[tuple[str, str]]:
    start_pattern = re.compile(
        rf"<select[^>]+name=[\"']{re.escape(field_name)}[\"'][^>]*>",
        re.IGNORECASE,
    )
    start_match = start_pattern.search(page_html)
    if not start_match:
        return []
    lower_html = page_html.lower()
    section_start = start_match.end()
    section_end = lower_html.find("</select", section_start)
    if section_end == -1:
        markers = [
            lower_html.find("</td>", section_start),
            lower_html.find("</tr>", section_start),
            lower_html.find("<input ", section_start),
            lower_html.find("<select ", section_start),
        ]
        marker_candidates = [marker for marker in markers if marker != -1]
        section_end = min(marker_candidates) if marker_candidates else len(page_html)
    section = page_html[section_start:section_end]

    options: list[tuple[str, str]] = []
    for raw_value, raw_text in _OPTION_PATTERN.findall(section):
        value = html.unescape(raw_value or "").strip()
        text = _strip_html_tags(raw_text)
        options.append((value, text))
    return options


def _pick_pharmacy_option(options: list[tuple[str, str]], *, prefer_facility: bool = False) -> str | None:
    if not options:
        return None
    for value, text in options:
        candidate = (text or value).strip().lower()
        if candidate == "pharmacy":
            return value
    filtered: list[str] = []
    for value, text in options:
        candidate = (text or value).strip().lower()
        if "pharm" not in candidate:
            continue
        if any(token in candidate for token in ("technician", "intern", "student", "preceptor")):
            continue
        if prefer_facility and any(token in candidate for token in ("pharmacist", "graduate pharmacist")):
            continue
        filtered.append(value)
    if filtered:
        return filtered[0]
    for value, text in options:
        candidate = (text or value).strip().lower()
        if "pharm" in candidate:
            return value
    return None


def _pick_exact_option(options: list[tuple[str, str]], target: str) -> str | None:
    target_lower = (target or "").strip().lower()
    if not target_lower:
        return None
    for value, text in options:
        candidate = (text or value).strip().lower()
        if candidate == target_lower:
            return value
    return None


def _extract_form_action(page_html: str, base_url: str) -> str:
    match = _FORM_ACTION_PATTERN.search(page_html)
    if not match:
        return base_url
    action = html.unescape(match.group(1) or "").strip()
    if not action:
        return base_url
    return urljoin(base_url, action)


def _extract_postback_targets(page_html: str) -> dict[int, str]:
    decoded = html.unescape(page_html)
    targets: dict[int, str] = {}
    for raw_args, body in _ASP_DATAGRID_POSTBACK_PATTERN.findall(decoded):
        arg_match = re.search(r"'([^']+)'\s*,\s*'([^']*)'", raw_args)
        if not arg_match:
            continue
        label = _strip_html_tags(body)
        if not label.isdigit():
            continue
        targets[int(label)] = arg_match.group(1)
    return targets


def _parse_datagrid_rows(page_html: str) -> list[dict[str, str]]:
    parser = _HtmlTableParser("datagrid_results")
    parser.feed(page_html)
    rows = parser.rows
    if not rows:
        return []
    headers = [_strip_html_tags(item) for item in rows[0]]
    if not headers:
        return []
    normalized_headers = [header if header else f"col_{idx}" for idx, header in enumerate(headers)]
    normalized_count = len(normalized_headers)
    parsed_rows: list[dict[str, str]] = []
    for raw_row in rows[1:]:
        if not any(cell.strip() for cell in raw_row):
            continue
        if len(raw_row) != normalized_count:
            continue
        row: dict[str, str] = {}
        for idx, header in enumerate(normalized_headers):
            row[header] = _strip_html_tags(raw_row[idx])
        # pager rows carry only page numbers in first cell
        if normalized_count > 1 and all(not row[header].strip() for header in normalized_headers[1:]):
            if re.fullmatch(r"[0-9 ]+", row[normalized_headers[0]] or ""):
                continue
        parsed_rows.append(row)
    return parsed_rows


def _hydrate_row_with_address_parts(row: dict[str, str]) -> dict[str, str]:
    hydrated = dict(row)
    address = _safe_text(hydrated.get("Address")) or _safe_text(hydrated.get("address"))
    if not address:
        return hydrated
    if hydrated.get("City") and hydrated.get("State"):
        return hydrated
    match = _CITY_STATE_ZIP_PATTERN.match(address)
    if not match:
        return hydrated
    hydrated.setdefault("City", match.group("city").strip())
    hydrated.setdefault("State", match.group("state").strip())
    hydrated.setdefault("Zip", match.group("zip").strip())
    return hydrated


def _is_captcha_page(page_html: str) -> bool:
    lower = (page_html or "").lower()
    return any(marker in lower for marker in _CAPTCHA_MARKERS)


async def _find_table_schema(table_name: str, preferred_schema: str | None = None) -> str | None:
    cache_key = f"{preferred_schema or ''}:{table_name}"
    if cache_key in _TABLE_SCHEMA_CACHE:
        return _TABLE_SCHEMA_CACHE[cache_key]

    rows = await db.all(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_name = :table_name
        """,
        table_name=table_name,
    )
    schemas = [str(row[0]) for row in rows if row and row[0]]
    chosen = None
    if preferred_schema and preferred_schema in schemas:
        chosen = preferred_schema
    elif "mrf" in schemas:
        chosen = "mrf"
    elif "public" in schemas:
        chosen = "public"
    elif schemas:
        chosen = schemas[0]
    _TABLE_SCHEMA_CACHE[cache_key] = chosen
    return chosen


async def _build_state_npi_resolver(state_code: str) -> StateNpiResolver | None:
    normalized_state = (state_code or "").strip().upper()
    if not normalized_state:
        return None
    preferred_schema = PharmacyLicenseImportRun.__table__.schema or "mrf"
    resolver = StateNpiResolver(state_code=normalized_state)
    loaded = False

    npi_taxonomy_schema = await _find_table_schema(NPIDataTaxonomy.__tablename__, preferred_schema)
    if npi_taxonomy_schema:
        rows = await db.all(
            f"""
            SELECT npi, provider_license_number
            FROM {npi_taxonomy_schema}.{NPIDataTaxonomy.__tablename__}
            WHERE provider_license_number IS NOT NULL
              AND provider_license_number_state_code IS NOT NULL
              AND upper(provider_license_number_state_code) = :state_code
            """,
            state_code=normalized_state,
        )
        by_license: dict[str, set[int]] = {}
        for row in rows:
            if not row:
                continue
            npi = _to_npi(row[0])
            if npi is None:
                continue
            key = _normalize_license_for_match(row[1])
            if not key:
                continue
            by_license.setdefault(key, set()).add(npi)
        resolver.by_license = _unique_mapping(by_license)
        loaded = loaded or bool(resolver.by_license)

    partd_schema = await _find_table_schema(PartDPharmacyActivity.__tablename__, preferred_schema)
    if partd_schema:
        rows = await db.all(
            f"""
            SELECT DISTINCT npi, pharmacy_name, city, zip_code
            FROM {partd_schema}.{PartDPharmacyActivity.__tablename__}
            WHERE state IS NOT NULL
              AND upper(state) = :state_code
              AND medicare_active IS TRUE
            """,
            state_code=normalized_state,
        )
        by_name_zip: dict[tuple[str, str], set[int]] = {}
        by_name_city: dict[tuple[str, str], set[int]] = {}
        by_name: dict[str, set[int]] = {}
        for row in rows:
            if not row:
                continue
            npi = _to_npi(row[0])
            if npi is None:
                continue
            name_values = [
                _normalize_name_for_match(row[1]),
                _normalize_name_for_match(row[1], loose=True),
            ]
            city_key = _normalize_city_for_match(row[2])
            zip_key = _normalize_zip_for_match(row[3])
            for name_key in name_values:
                if not name_key:
                    continue
                by_name.setdefault(name_key, set()).add(npi)
                if city_key:
                    by_name_city.setdefault((name_key, city_key), set()).add(npi)
                if zip_key:
                    by_name_zip.setdefault((name_key, zip_key), set()).add(npi)
        resolver.by_name = _unique_mapping(by_name)
        resolver.by_name_city = _unique_mapping(by_name_city)
        resolver.by_name_zip = _unique_mapping(by_name_zip)
        loaded = loaded or bool(resolver.by_name or resolver.by_name_city or resolver.by_name_zip)

    if not loaded:
        return None
    return resolver


def _extract_city_state_zip_from_freeform(address: Any) -> tuple[str | None, str | None, str | None]:
    address_text = _safe_text(address)
    if not address_text:
        return None, None, None
    candidate = " ".join(address_text.replace(",", " ").split())
    match = _ADDRESS_TAIL_STATE_ZIP_PATTERN.search(candidate)
    if not match:
        return None, None, None

    state = match.group("state").strip()
    zip_code = match.group("zip").strip()
    prefix = candidate[: match.start()].strip()
    if not prefix:
        return None, state, zip_code

    raw_tokens = [token.strip(".,") for token in prefix.split() if token.strip(".,")]
    if not raw_tokens:
        return None, state, zip_code

    two_plus_candidates: list[str] = []
    one_word_candidates: list[str] = []
    max_city_words = min(5, len(raw_tokens))
    for width in range(1, max_city_words + 1):
        tokens = raw_tokens[-width:]
        if any(any(ch.isdigit() for ch in token) for token in tokens):
            continue
        if any(token.lower() in _STREET_ADDRESS_TOKENS for token in tokens):
            continue
        city = " ".join(tokens).strip()
        if not city:
            continue
        if width >= 2:
            two_plus_candidates.append(city)
        else:
            one_word_candidates.append(city)

    if two_plus_candidates:
        return two_plus_candidates[0], state, zip_code
    if one_word_candidates:
        return one_word_candidates[0], state, zip_code
    return None, state, zip_code


def _ma_license_type_is_pharmacy_facility(license_type: Any) -> bool:
    license_text = str(license_type or "").strip().lower()
    if not license_text:
        return False
    excluded_markers = (
        "pharmacist license",
        "pharmacy technician",
        "pharmacy internship",
        "technician trainee",
        "nuclear pharmacist",
    )
    if any(marker in license_text for marker in excluded_markers):
        return False
    included_markers = (
        "pharmacy",
        "controlled substance permit",
        "outsourcing facility",
        "wholesaler",
        "drug distributor",
        "drug manufacturer",
    )
    return any(marker in license_text for marker in included_markers)


def _map_tx_csv_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "License Number": row.get("LIC_NBR"),
        "Entity Name": row.get("PHARMACY_NAME"),
        "License Type": row.get("PHY TYPE") or row.get("CLASS"),
        "License Status": row.get("LIC_STATUS"),
        "Issue Date": row.get("LIC_ORIG_DATE"),
        "Expiration Date": row.get("LIC_EXPR_DATE"),
        "Disciplinary Flag": row.get("DISP ACTN"),
        "Address": row.get("ADDRESS1"),
        "Address Line 2": row.get("ADDRESS2"),
        "City": row.get("CITY"),
        "State": row.get("STATE"),
        "Zip": row.get("ZIP"),
        "Phone": row.get("PHONE"),
        "Source Record ID": row.get("ENTITY_NBR"),
    }


def _map_fl_csv_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "License Number": row.get("License Number"),
        "Entity Name": row.get(" Org Name") or row.get("Org Name"),
        "DBA": row.get(" DBA Name") or row.get("DBA Name"),
        "License Type": row.get(" Profession ") or row.get("Profession"),
        "License Status": row.get(" License Status") or row.get("License Status"),
        "Address": row.get(" Address") or row.get("Address"),
        "City": row.get(" City") or row.get("City"),
        "State": "FL",
    }


def _map_co_socrata_row(row: dict[str, Any]) -> dict[str, Any]:
    parts = [
        _safe_text(row.get("firstname")),
        _safe_text(row.get("middlename")),
        _safe_text(row.get("lastname")),
    ]
    entity_name = " ".join(part for part in parts if part)
    if not entity_name:
        entity_name = _safe_text(row.get("lastname"))

    return {
        "License Number": row.get("licensenumber"),
        "Entity Name": entity_name,
        "License Type": row.get("licensetype"),
        "License Status": row.get("licensestatusdescription"),
        "Issue Date": row.get("licensefirstissuedate"),
        "Last Renewal Date": row.get("licenselastreneweddate"),
        "Expiration Date": row.get("licenseexpirationdate"),
        "City": row.get("city"),
        "State": row.get("state"),
        "Zip": row.get("mailzipcode"),
    }


def _map_wa_socrata_row(row: dict[str, Any]) -> dict[str, Any]:
    parts = [
        _safe_text(row.get("firstname")),
        _safe_text(row.get("middlename")),
        _safe_text(row.get("lastname")),
    ]
    entity_name = " ".join(part for part in parts if part)
    if not entity_name:
        entity_name = _safe_text(row.get("lastname"))

    return {
        "License Number": row.get("credentialnumber"),
        "Entity Name": entity_name,
        "License Type": row.get("credentialtype"),
        "License Status": row.get("status"),
        "Issue Date": row.get("firstissuedate"),
        "Last Renewal Date": row.get("lastissuedate"),
        "Expiration Date": row.get("expirationdate"),
        "State": "WA",
        "Disciplinary Flag": row.get("actiontaken"),
    }


def _map_ny_rosa_row(row: dict[str, Any]) -> dict[str, Any]:
    def _nested_value(raw: Any) -> Any:
        if isinstance(raw, dict):
            value = raw.get("value")
            if value not in (None, ""):
                return value
            return raw.get("label")
        return raw

    legal_name = _safe_text(_nested_value(row.get("legalName")))
    trade_name = _safe_text(_nested_value(row.get("tradeName")))
    entity_name = legal_name or trade_name or _safe_text(_nested_value(row.get("name")))
    address_text = _safe_text(_nested_value(row.get("address")))
    city, state, zip_code = _extract_city_state_zip_from_freeform(address_text)
    enforcement_actions = row.get("enforcementActions")
    disciplinary_flag = bool(enforcement_actions) if isinstance(enforcement_actions, list) else False
    disciplinary_summary = json.dumps(enforcement_actions) if disciplinary_flag else None

    return {
        "License Number": row.get("registrationNumber"),
        "Entity Name": entity_name,
        "DBA": trade_name,
        "License Type": _nested_value(row.get("type")),
        "License Status": _nested_value(row.get("status")),
        "Issue Date": _nested_value(row.get("dateFirstRegistered")),
        "Last Renewal Date": _nested_value(row.get("dateRegistrationBegins")),
        "Expiration Date": _nested_value(row.get("dateRegisteredThrough")),
        "Address": address_text,
        "City": city,
        "State": state,
        "Zip": zip_code,
        "Disciplinary Flag": disciplinary_flag,
        "Disciplinary Summary": disciplinary_summary,
        "Source Record ID": row.get("registrationNumber"),
    }


def _map_ma_export_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "License Number": row.get("License Number"),
        "Entity Name": row.get("Organization Name"),
        "License Type": row.get("License Type"),
        "License Status": row.get("License Status"),
        "Issue Date": row.get("Issue Date"),
        "Last Renewal Date": row.get("Last Issue/Renewal Date"),
        "Expiration Date": row.get("Expiration Date"),
        "Address": row.get("Address 1"),
        "Address Line 2": row.get("Address 2"),
        "City": row.get("City"),
        "State": row.get("State"),
        "Zip": row.get("Zip Code"),
        "Disciplinary Flag": bool(
            _safe_text(row.get("Suspension Start Date"))
            or _safe_text(row.get("Suspension End Date"))
            or _safe_text(row.get("Surrendered Date"))
            or _safe_text(row.get("Revoked Date"))
        ),
        "Disciplinary Summary": " | ".join(
            part
            for part in (
                f"Suspension Start: {_safe_text(row.get('Suspension Start Date'))}"
                if _safe_text(row.get("Suspension Start Date"))
                else "",
                f"Suspension End: {_safe_text(row.get('Suspension End Date'))}"
                if _safe_text(row.get("Suspension End Date"))
                else "",
                f"Surrendered: {_safe_text(row.get('Surrendered Date'))}"
                if _safe_text(row.get("Surrendered Date"))
                else "",
                f"Revoked: {_safe_text(row.get('Revoked Date'))}"
                if _safe_text(row.get("Revoked Date"))
                else "",
            )
            if part
        )
        or None,
    }


async def _load_rows_from_direct_csv_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    source_url: str,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    metadata: dict[str, Any] = {"adapter": _STATE_ADAPTER_DIRECT_CSV, "source_url": source_url}
    try:
        final_url, _content_type, raw = await _fetch_bytes(
            session,
            source_url,
            max_bytes=PHARM_LICENSE_MAX_DOWNLOAD_BYTES,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], None, metadata, f"adapter_fetch_failed:{exc}"

    try:
        records = _parse_csv_records(raw)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], final_url, metadata, f"adapter_parse_failed:{exc}"

    mapped_rows: list[dict[str, Any]] = []
    for row in records:
        if state_source.state_code == "TX":
            mapped_rows.append(_map_tx_csv_row(row))
        elif state_source.state_code == "FL":
            mapped_rows.append(_map_fl_csv_row(row))
        else:
            mapped_rows.append(dict(row))

    metadata["rows_loaded"] = len(mapped_rows)
    return mapped_rows, final_url, metadata, None


async def _load_rows_from_socrata_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    source_url: str,
    *,
    select_columns: tuple[str, ...],
    where_clause: str,
    page_size: int,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    metadata: dict[str, Any] = {
        "adapter": _STATE_ADAPTER_SOCRATA,
        "source_url": source_url,
    }
    rows: list[dict[str, Any]] = []
    offset = 0
    pages_fetched = 0
    page_limit = PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES

    while pages_fetched < page_limit and len(rows) < PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        params = {
            "$select": ",".join(select_columns),
            "$where": where_clause,
            "$limit": str(page_size),
            "$offset": str(offset),
        }
        try:
            query_url = f"{source_url}?{urlencode(params)}"
            final_url, _content_type, raw = await _fetch_bytes(
                session,
                query_url,
                max_bytes=PHARM_LICENSE_MAX_DOWNLOAD_BYTES,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return [], None, metadata, f"adapter_fetch_failed:{exc}"

        pages_fetched += 1
        try:
            page_rows = _parse_csv_records(raw)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return [], final_url, metadata, f"adapter_parse_failed:{exc}"

        if not page_rows:
            metadata["pages_fetched"] = pages_fetched
            metadata["rows_loaded"] = len(rows)
            return rows, final_url, metadata, None

        for row in page_rows:
            if state_source.state_code == "CO":
                rows.append(_map_co_socrata_row(row))
            elif state_source.state_code == "WA":
                rows.append(_map_wa_socrata_row(row))
            else:
                rows.append(dict(row))
            if len(rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
                break

        if len(page_rows) < page_size:
            metadata["pages_fetched"] = pages_fetched
            metadata["rows_loaded"] = len(rows)
            return rows, final_url, metadata, None
        offset += page_size

    metadata["pages_fetched"] = pages_fetched
    metadata["rows_loaded"] = len(rows)
    if pages_fetched >= page_limit:
        metadata["page_limit_reached"] = True
    if len(rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        metadata["row_limit_reached"] = True
    return rows, source_url, metadata, None


async def _load_rows_from_ny_rosa_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    *,
    base_url: str,
    api_key: str,
    query_terms: tuple[str, ...],
    page_size: int,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    metadata: dict[str, Any] = {
        "adapter": _STATE_ADAPTER_NY_ROSA_API,
        "source_url": base_url,
        "query_terms": list(query_terms),
    }
    if not api_key:
        return [], None, metadata, "missing_ny_rosa_api_key"

    rows_by_registration: dict[str, dict[str, Any]] = {}
    page_limit = PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES
    pages_fetched = 0
    requests_made = 0
    final_url: str | None = None

    for term in query_terms:
        if len(rows_by_registration) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
            break
        page_number = 0
        while page_number < page_limit and len(rows_by_registration) < PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
            params = {
                "name": term,
                "pageNumber": str(page_number),
                "pageSize": str(page_size),
                "sortBy": "registrationNumber",
                "sortDirection": "asc",
            }
            query_url = f"{base_url}?{urlencode(params)}"
            try:
                final_url, _content_type, raw = await _fetch_bytes(
                    session,
                    query_url,
                    max_bytes=PHARM_LICENSE_MAX_DOWNLOAD_BYTES,
                    headers={"x-oapi-key": api_key},
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return [], final_url, metadata, f"adapter_fetch_failed:{exc}"
            requests_made += 1
            pages_fetched += 1

            try:
                payload = json.loads(_decode_text(raw))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return [], final_url, metadata, f"adapter_parse_failed:{exc}"

            page_rows = payload.get("content")
            if not isinstance(page_rows, list) or not page_rows:
                break

            for row in page_rows:
                if not isinstance(row, dict):
                    continue
                registration = _safe_text(row.get("registrationNumber"))
                if not registration:
                    continue
                if registration in rows_by_registration:
                    continue
                rows_by_registration[registration] = _map_ny_rosa_row(row)
                if len(rows_by_registration) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
                    break

            if len(page_rows) < page_size:
                break
            page_number += 1

    metadata["pages_fetched"] = pages_fetched
    metadata["requests_made"] = requests_made
    metadata["rows_loaded"] = len(rows_by_registration)
    if pages_fetched >= page_limit:
        metadata["page_limit_reached"] = True
    if len(rows_by_registration) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        metadata["row_limit_reached"] = True
    return list(rows_by_registration.values()), final_url or base_url, metadata, None


async def _load_rows_from_ma_export_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    *,
    base_url: str,
    board_id: str,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    metadata: dict[str, Any] = {
        "adapter": _STATE_ADAPTER_MA_EXPORT_API,
        "source_url": base_url,
        "board_id": board_id,
    }
    board_token = (board_id or "").strip()
    if not board_token:
        return [], None, metadata, "missing_ma_export_board_id"

    export_url = f"{base_url.rstrip('/')}/{board_token}"
    try:
        final_url, _content_type, raw = await _fetch_bytes(
            session,
            export_url,
            max_bytes=PHARM_LICENSE_MAX_DOWNLOAD_BYTES,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], None, metadata, f"adapter_fetch_failed:{exc}"

    try:
        records = _parse_zip_records(raw)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], final_url, metadata, f"adapter_parse_failed:{exc}"

    mapped_rows: list[dict[str, Any]] = []
    for row in records:
        if not isinstance(row, dict):
            continue
        mapped = _map_ma_export_row(row)
        if not _ma_license_type_is_pharmacy_facility(mapped.get("License Type")):
            continue
        mapped_rows.append(mapped)
        if len(mapped_rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
            break

    metadata["rows_loaded"] = len(mapped_rows)
    metadata["records_parsed"] = len(records)
    if len(mapped_rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        metadata["row_limit_reached"] = True
    return mapped_rows, final_url, metadata, None


async def _load_rows_from_configured_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
) -> tuple[bool, list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    static_reason = _STATE_STATIC_UNSUPPORTED_CONFIG.get(state_source.state_code)
    if static_reason:
        return (
            True,
            [],
            None,
            {
                "adapter": _STATE_ADAPTER_STATIC_UNSUPPORTED,
                "source_url": state_source.board_url,
                "terminal_error": True,
            },
            static_reason,
        )

    direct_cfg = _STATE_DIRECT_CSV_CONFIG.get(state_source.state_code)
    if direct_cfg:
        rows, source_url, metadata, error = await _load_rows_from_direct_csv_source(
            session,
            state_source,
            str(direct_cfg["source_url"]),
        )
        return True, rows, source_url, metadata, error

    socrata_cfg = _STATE_SOCRATA_CONFIG.get(state_source.state_code)
    if socrata_cfg:
        rows, source_url, metadata, error = await _load_rows_from_socrata_source(
            session,
            state_source,
            str(socrata_cfg["source_url"]),
            select_columns=tuple(socrata_cfg["columns"]),
            where_clause=str(socrata_cfg["where"]),
            page_size=int(socrata_cfg.get("page_size", 50000)),
        )
        return True, rows, source_url, metadata, error

    if state_source.state_code == "NY":
        rows, source_url, metadata, error = await _load_rows_from_ny_rosa_source(
            session,
            state_source,
            base_url=_NY_ROSA_BASE_URL,
            api_key=_NY_ROSA_API_KEY,
            query_terms=_NY_ROSA_QUERY_TERMS,
            page_size=_NY_ROSA_PAGE_SIZE,
        )
        return True, rows, source_url, metadata, error

    if state_source.state_code == "MA":
        rows, source_url, metadata, error = await _load_rows_from_ma_export_source(
            session,
            state_source,
            base_url=_MA_EXPORT_BASE_URL,
            board_id=_MA_EXPORT_BOARD_ID,
        )
        return True, rows, source_url, metadata, error

    return False, [], None, {}, None


def _create_aspnet_adapter_spec(state_source: StateSource) -> AspNetStateAdapterSpec | None:
    config = _STATE_ADAPTER_CONFIG.get(state_source.state_code)
    if not config:
        return None
    return AspNetStateAdapterSpec(
        state_code=state_source.state_code,
        search_url=str(config["search_url"]),
    )


async def _load_rows_from_aspnet_search_state(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    spec: AspNetStateAdapterSpec,
) -> tuple[list[dict[str, Any]], str | None, dict[str, Any], str | None]:
    metadata: dict[str, Any] = {"adapter": _STATE_ADAPTER_ASPNET_SEARCH}
    rows: list[dict[str, Any]] = []

    try:
        search_final_url, _content_type, search_page = await _fetch_bytes(
            session,
            spec.search_url,
            max_bytes=min(PHARM_LICENSE_MAX_DOWNLOAD_BYTES, 2_000_000),
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], None, metadata, f"adapter_fetch_failed:{exc}"

    if _is_captcha_page(_decode_text(search_page)):
        return [], search_final_url, metadata, "captcha_required"

    decoded_search_page = _decode_text(search_page)
    form_action = _extract_form_action(decoded_search_page, search_final_url)
    hidden_fields = _extract_hidden_fields(decoded_search_page)
    lookup_fields = _extract_lookup_field_names(decoded_search_page)
    form_payload = dict(hidden_fields)
    for field_name in sorted(lookup_fields):
        form_payload[field_name] = ""

    profession_options = _extract_select_options(decoded_search_page, "t_web_lookup__profession_name")
    profession_value = _pick_pharmacy_option(profession_options)
    if profession_value and "t_web_lookup__profession_name" in lookup_fields:
        form_payload["t_web_lookup__profession_name"] = profession_value

    license_type_options = _extract_select_options(decoded_search_page, "t_web_lookup__license_type_name")
    license_type_value = _pick_exact_option(license_type_options, "Pharmacy")
    if license_type_value and "t_web_lookup__license_type_name" in lookup_fields:
        form_payload["t_web_lookup__license_type_name"] = license_type_value

    if profession_value is None:
        if "t_web_lookup__full_name" in lookup_fields:
            form_payload["t_web_lookup__full_name"] = "PHARM"
        elif "t_web_lookup__doing_business_as" in lookup_fields:
            form_payload["t_web_lookup__doing_business_as"] = "PHARM"
        else:
            return [], search_final_url, metadata, "state_adapter_no_pharmacy_filter"

    metadata["selected_profession"] = profession_value
    metadata["selected_license_type"] = license_type_value

    form_payload["sch_button"] = "Search"
    try:
        async with session.post(form_action, data=form_payload, allow_redirects=True, ssl=False) as response:
            result_html = await _read_response_bytes(
                response,
                max_bytes=min(PHARM_LICENSE_MAX_DOWNLOAD_BYTES, 3_000_000),
            )
            result_url = str(response.url)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], search_final_url, metadata, f"adapter_search_failed:{exc}"

    current_html = _decode_text(result_html)
    if _is_captcha_page(current_html):
        return [], result_url, metadata, "captcha_required"
    current_url = result_url
    current_page = 1
    page_count = 0
    seen_signatures: set[tuple[str, str, str]] = set()

    while page_count < PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES and len(rows) < PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        page_count += 1
        parsed_page_rows = _parse_datagrid_rows(current_html)
        if not parsed_page_rows:
            lowered = current_html.lower()
            if any(token in lowered for token in ("no records", "no record", "no results", "not found")):
                break
            if page_count == 1:
                return [], current_url, metadata, "state_adapter_no_results_grid"
            break

        for raw_row in parsed_page_rows:
            hydrated_row = _hydrate_row_with_address_parts(raw_row)
            signature = (
                _safe_text(hydrated_row.get("Name")) or "",
                _safe_text(hydrated_row.get("License #") or hydrated_row.get("License Number")) or "",
                _safe_text(hydrated_row.get("State")) or state_source.state_code,
            )
            if signature in seen_signatures:
                continue
            seen_signatures.add(signature)
            rows.append(hydrated_row)
            if len(rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
                break

        postback_targets = _extract_postback_targets(current_html)
        next_page = min((page for page in postback_targets if page > current_page), default=None)
        if next_page is None:
            break
        target = postback_targets.get(next_page)
        if not target:
            break

        payload = _extract_hidden_fields(current_html)
        payload["__EVENTTARGET"] = target
        payload["__EVENTARGUMENT"] = ""
        try:
            async with session.post(current_url, data=payload, allow_redirects=True, ssl=False) as next_response:
                current_html = _decode_text(
                    await _read_response_bytes(
                        next_response,
                        max_bytes=min(PHARM_LICENSE_MAX_DOWNLOAD_BYTES, 3_000_000),
                    )
                )
                current_url = str(next_response.url)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            metadata["pagination_error"] = str(exc)
            break
        current_page = next_page

    metadata["pages_fetched"] = page_count
    metadata["rows_loaded"] = len(rows)
    if page_count >= PHARM_LICENSE_STATE_ADAPTER_MAX_PAGES:
        metadata["page_limit_reached"] = True
    if len(rows) >= PHARM_LICENSE_STATE_ADAPTER_MAX_ROWS:
        metadata["row_limit_reached"] = True
    return rows, current_url or search_final_url, metadata, None


def _row_index(row: dict[str, Any]) -> dict[str, Any]:
    indexed: dict[str, Any] = {}
    for key, value in row.items():
        indexed[_normalize_key(key)] = value
    return indexed


def _pick_by_aliases(indexed: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        key = _normalize_key(alias)
        if key in indexed:
            value = indexed.get(key)
            if value not in (None, ""):
                return value
    return None


def _pick_license_number(indexed: dict[str, Any]) -> str | None:
    direct = _pick_by_aliases(indexed, _LICENSE_NUMBER_KEYS)
    if direct is not None:
        text = _safe_text(direct)
        if text:
            return text[:64]

    for key, value in indexed.items():
        if "license" not in key and "permit" not in key:
            continue
        if any(token in key for token in ("status", "state", "type", "disciplin", "board", "date")):
            continue
        text = _safe_text(value)
        if text:
            return text[:64]
    return None


def _pick_npi(indexed: dict[str, Any]) -> int | None:
    direct = _pick_by_aliases(indexed, _NPI_KEYS)
    if direct is not None:
        parsed = _to_npi(direct)
        if parsed is not None:
            return parsed

    for key, value in indexed.items():
        if "npi" not in key:
            continue
        parsed = _to_npi(value)
        if parsed is not None:
            return parsed
    return None


def _extract_records_from_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("records", "results", "items", "data", "value", "licenses"):
            item = payload.get(key)
            if isinstance(item, list):
                return [row for row in item if isinstance(row, dict)]
        if all(isinstance(value, dict) for value in payload.values()):
            return [value for value in payload.values() if isinstance(value, dict)]
    return []


def _decode_text(data: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


def _extract_candidate_file_links(html: str, base_url: str) -> list[str]:
    candidates: list[str] = []
    for relative in _FILE_LINK_PATTERN.findall(html):
        absolute = urljoin(base_url, relative)
        if _is_noise_link(absolute):
            continue
        candidates.append(absolute)

    def _score(url: str) -> tuple[int, int, str]:
        lower = url.lower()
        score = 0
        if any(token in lower for token in ("pharmacy", "license", "licensure", "credential")):
            score += 5
        if any(token in lower for token in ("export", "download", "lookup", "verify", "search")):
            score += 2
        if lower.endswith(".json"):
            score += 1
        if lower.endswith(".csv"):
            score += 1
        return (-score, len(url), url)

    deduped = sorted(set(candidates), key=_score)
    return deduped[:PHARM_LICENSE_MAX_CANDIDATE_URLS]


async def _read_response_bytes(response: aiohttp.ClientResponse, *, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    async for chunk in response.content.iter_chunked(128 * 1024):
        if not chunk:
            continue
        total += len(chunk)
        if total > max_bytes:
            raise RuntimeError(f"payload_too_large:{max_bytes}")
        chunks.append(chunk)
    return b"".join(chunks)


async def _fetch_bytes(
    session: aiohttp.ClientSession,
    url: str,
    *,
    max_bytes: int,
    headers: dict[str, str] | None = None,
) -> tuple[str, str, bytes]:
    async with session.get(url, allow_redirects=True, ssl=False, headers=headers) as resp:
        content = await _read_response_bytes(resp, max_bytes=max_bytes)
        return str(resp.url), str(resp.headers.get("Content-Type") or ""), content


async def _discover_machine_readable_sources(
    session: aiohttp.ClientSession,
    source: StateSource,
) -> tuple[list[str], str | None]:
    ext = _entry_extensions(source.board_url)
    if ext in {"csv", "json", "zip"}:
        return [source.board_url], None

    try:
        final_url, content_type, raw = await _fetch_bytes(
            session,
            source.board_url,
            max_bytes=min(PHARM_LICENSE_MAX_DOWNLOAD_BYTES, 1_500_000),
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], f"board_fetch_failed:{exc}"

    ext = _entry_extensions(final_url)
    if ext in {"csv", "json", "zip"}:
        return [final_url], None

    lower_type = content_type.lower()
    if "json" in lower_type:
        return [final_url], None
    if "csv" in lower_type:
        return [final_url], None

    html = _decode_text(raw)
    candidates = _extract_candidate_file_links(html, final_url)
    if not candidates:
        return [], "no_machine_readable_link"
    return candidates, None


def _parse_csv_records(raw: bytes) -> list[dict[str, Any]]:
    text = _decode_text(raw)
    handle = io.StringIO(text)
    reader = csv.DictReader(handle)
    return [dict(row) for row in reader if isinstance(row, dict)]


def _parse_json_records(raw: bytes) -> list[dict[str, Any]]:
    payload = json.loads(_decode_text(raw))
    return _extract_records_from_json(payload)


def _parse_zip_records(raw: bytes) -> list[dict[str, Any]]:
    def _parse_archive(archive: zipfile.ZipFile, *, depth: int) -> list[dict[str, Any]]:
        if depth > 3:
            return []
        parsed_rows: list[dict[str, Any]] = []
        metadata_rows: list[dict[str, Any]] = []
        names = [name for name in archive.namelist() if not name.endswith("/")]
        names.sort()
        for name in names:
            lower = name.lower()
            with archive.open(name, "r") as fp:
                content = fp.read()
            if lower.endswith(".zip"):
                try:
                    with zipfile.ZipFile(io.BytesIO(content), "r") as nested_archive:
                        nested_rows = _parse_archive(nested_archive, depth=depth + 1)
                except zipfile.BadZipFile:
                    nested_rows = []
                if nested_rows:
                    parsed_rows.extend(nested_rows)
                continue
            if not lower.endswith((".csv", ".json")):
                continue
            if lower.endswith(".csv"):
                current_rows = _parse_csv_records(content)
            else:
                current_rows = _parse_json_records(content)
            if not current_rows:
                continue
            if "metadata" in lower:
                metadata_rows.extend(current_rows)
            else:
                parsed_rows.extend(current_rows)
        if parsed_rows:
            return parsed_rows
        return metadata_rows

    with tempfile.TemporaryDirectory(prefix="pharm_license_zip_") as tmpdir:
        zip_path = Path(tmpdir) / "source.zip"
        zip_path.write_bytes(raw)
        with zipfile.ZipFile(zip_path, "r") as archive:
            return _parse_archive(archive, depth=0)


async def _load_records_from_source(
    session: aiohttp.ClientSession,
    source_url: str,
) -> tuple[list[dict[str, Any]], str | None]:
    try:
        final_url, content_type, raw = await _fetch_bytes(
            session,
            source_url,
            max_bytes=PHARM_LICENSE_MAX_DOWNLOAD_BYTES,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], f"source_fetch_failed:{exc}"

    ext = _entry_extensions(final_url) or _entry_extensions(source_url)
    lower_content_type = content_type.lower()

    try:
        if ext == "csv" or "csv" in lower_content_type:
            return _parse_csv_records(raw), None
        if ext == "json" or "json" in lower_content_type:
            return _parse_json_records(raw), None
        if ext == "zip" or "zip" in lower_content_type:
            return _parse_zip_records(raw), None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return [], f"source_parse_failed:{exc}"

    return [], "unsupported_source_format"


def _normalize_stage_row(
    row: dict[str, Any],
    *,
    run_id: str,
    snapshot_id: str,
    state_source: StateSource,
    source_url: str,
    imported_at: datetime.datetime,
    npi_resolver: StateNpiResolver | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    indexed = _row_index(row)

    license_number = _pick_license_number(indexed)
    if not license_number:
        return None, "missing_license_number"

    entity_name = _safe_text(_pick_by_aliases(indexed, _ENTITY_NAME_KEYS))
    dba_name = _safe_text(_pick_by_aliases(indexed, _DBA_KEYS))
    city = _safe_text(_pick_by_aliases(indexed, _CITY_KEYS))
    state_from_source = _safe_text(_pick_by_aliases(indexed, _STATE_KEYS))
    zip_code = _safe_text(_pick_by_aliases(indexed, _ZIP_KEYS))

    npi = _pick_npi(indexed)
    if npi is None and npi_resolver:
        npi = npi_resolver.resolve(
            license_number=license_number,
            entity_name=entity_name,
            dba_name=dba_name,
            city=city,
            zip_code=zip_code,
        )
    if npi is None:
        return None, "missing_npi"

    source_status_raw = _safe_text(_pick_by_aliases(indexed, _STATUS_KEYS))
    license_status = _normalize_license_status(source_status_raw)

    disciplinary_summary = _safe_text(_pick_by_aliases(indexed, _DISCIPLINARY_SUMMARY_KEYS))
    disciplinary_flag = _to_bool(_pick_by_aliases(indexed, _DISCIPLINARY_FLAG_KEYS))
    if disciplinary_flag is None:
        disciplinary_flag = bool(disciplinary_summary)
        if not disciplinary_flag and source_status_raw:
            lower = source_status_raw.lower()
            disciplinary_flag = any(marker in lower for marker in _DISCIPLINARY_HINTS)

    source_last_seen_at = None
    updated_raw = _pick_by_aliases(indexed, _UPDATED_KEYS)
    updated_date = _to_date(updated_raw)
    if updated_date is not None:
        source_last_seen_at = datetime.datetime.combine(updated_date, datetime.time.min)

    state_value = (state_from_source or state_source.state_code).upper()[:2]

    payload = {
        "snapshot_id": snapshot_id,
        "run_id": run_id,
        "state_code": state_source.state_code,
        "state_name": state_source.state_name,
        "board_url": state_source.board_url,
        "source_url": source_url,
        "npi": npi,
        "license_number": license_number[:64],
        "license_type": _safe_text(_pick_by_aliases(indexed, _LICENSE_TYPE_KEYS)),
        "license_status": license_status,
        "source_status_raw": source_status_raw[:256] if source_status_raw else None,
        "license_issue_date": _to_date(_pick_by_aliases(indexed, _ISSUE_KEYS)),
        "license_effective_date": _to_date(_pick_by_aliases(indexed, _EFFECTIVE_KEYS)),
        "license_expiration_date": _to_date(_pick_by_aliases(indexed, _EXPIRY_KEYS)),
        "last_renewal_date": _to_date(_pick_by_aliases(indexed, _RENEWAL_KEYS)),
        "disciplinary_flag": bool(disciplinary_flag),
        "disciplinary_summary": disciplinary_summary,
        "disciplinary_action_date": _to_date(_pick_by_aliases(indexed, _DISCIPLINARY_DATE_KEYS)),
        "entity_name": entity_name,
        "dba_name": dba_name,
        "address_line1": _safe_text(_pick_by_aliases(indexed, _ADDRESS1_KEYS)),
        "address_line2": _safe_text(_pick_by_aliases(indexed, _ADDRESS2_KEYS)),
        "city": city,
        "state": state_value,
        "zip_code": zip_code,
        "phone_number": _safe_text(_pick_by_aliases(indexed, _PHONE_KEYS)),
        "source_record_id": _safe_text(_pick_by_aliases(indexed, _SOURCE_RECORD_KEYS)),
        "source_last_seen_at": source_last_seen_at,
        "imported_at": imported_at,
    }
    return payload, None


async def _flush_stage_batch(batch: list[dict[str, Any]]) -> None:
    if not batch:
        return
    rows = list(batch)
    batch.clear()
    await push_objects(rows, PharmacyLicenseRecordStage, rewrite=False, use_copy=True)


def _test_rows_for_state(state_source: StateSource) -> list[dict[str, Any]]:
    base_npi = 1518379600 + (int(hashlib.sha1(state_source.state_code.encode("utf-8")).hexdigest()[:2], 16) % 40)
    today = datetime.date.today()
    return [
        {
            "NPI": str(base_npi),
            "License Number": f"{state_source.state_code}-PH-{base_npi % 100000:05d}",
            "License Type": "Pharmacy",
            "License Status": "Active",
            "Expiration Date": (today + datetime.timedelta(days=365)).isoformat(),
            "Issue Date": (today - datetime.timedelta(days=1200)).isoformat(),
            "Entity Name": f"Sample {state_source.state_name} Pharmacy",
            "DBA": f"Sample {state_source.state_code} RX",
            "Address": "100 Main Street",
            "City": "Sample City",
            "State": state_source.state_code,
            "Zip": "12345",
            "Phone": "555-555-1212",
            "Last Updated": today.isoformat(),
            "Disciplinary Flag": "false",
        },
        {
            "NPI": str(base_npi + 1),
            "License Number": f"{state_source.state_code}-PH-{(base_npi + 1) % 100000:05d}",
            "License Type": "Pharmacy",
            "License Status": "Suspended",
            "Expiration Date": (today + datetime.timedelta(days=180)).isoformat(),
            "Entity Name": f"Sample {state_source.state_name} Specialty Pharmacy",
            "Address": "200 Main Street",
            "City": "Sample City",
            "State": state_source.state_code,
            "Zip": "12345",
            "Phone": "555-555-1313",
            "Disciplinary Summary": "Administrative suspension pending review",
            "Disciplinary Action Date": (today - datetime.timedelta(days=30)).isoformat(),
            "Last Updated": today.isoformat(),
        },
    ]


async def _import_state_source(
    session: aiohttp.ClientSession,
    state_source: StateSource,
    *,
    run_id: str,
    snapshot_id: str,
    test_mode: bool,
) -> StateImportStats:
    imported_at = datetime.datetime.utcnow()

    raw_rows: list[dict[str, Any]] = []
    selected_source_url: str | None = None
    metadata: dict[str, Any] = {}

    if test_mode:
        raw_rows = _test_rows_for_state(state_source)
        selected_source_url = state_source.board_url
        metadata["test_mode"] = True
    else:
        adapter_error: str | None = None
        configured_handled, cfg_rows, cfg_source_url, cfg_metadata, cfg_error = await _load_rows_from_configured_source(
            session,
            state_source,
        )
        if configured_handled:
            metadata.update(cfg_metadata)
            if cfg_error:
                adapter_error = cfg_error
                metadata["adapter_error"] = cfg_error
                if metadata.get("terminal_error"):
                    return StateImportStats(
                        supported=False,
                        status="unsupported",
                        source_url=None,
                        unsupported_reason=cfg_error,
                        error_text=None,
                        row_count_parsed=0,
                        row_count_matched=0,
                        row_count_dropped=0,
                        row_count_inserted=0,
                        metadata=metadata,
                    )
            else:
                raw_rows = cfg_rows
                selected_source_url = cfg_source_url or state_source.board_url
                metadata["source_adapter"] = metadata.get("adapter")

        adapter_spec = None if configured_handled else _create_aspnet_adapter_spec(state_source)
        if adapter_spec:
            adapter_rows, adapter_source_url, adapter_metadata, adapter_error = await _load_rows_from_aspnet_search_state(
                session,
                state_source,
                adapter_spec,
            )
            metadata.update(adapter_metadata)
            metadata["adapter_source_url"] = adapter_source_url
            if adapter_error:
                metadata["adapter_error"] = adapter_error
            else:
                raw_rows = adapter_rows
                selected_source_url = adapter_source_url or state_source.board_url
                metadata["source_adapter"] = _STATE_ADAPTER_ASPNET_SEARCH

        if not raw_rows and selected_source_url is None:
            candidate_urls, discover_error = await _discover_machine_readable_sources(session, state_source)
            metadata["candidate_urls"] = candidate_urls
            if discover_error:
                reason = adapter_error or discover_error
                return StateImportStats(
                    supported=False,
                    status="unsupported",
                    source_url=None,
                    unsupported_reason=reason,
                    error_text=None,
                    row_count_parsed=0,
                    row_count_matched=0,
                    row_count_dropped=0,
                    row_count_inserted=0,
                    metadata=metadata,
                )

            for candidate_url in candidate_urls[:PHARM_LICENSE_MAX_CANDIDATE_URLS]:
                rows, load_error = await _load_records_from_source(session, candidate_url)
                if load_error:
                    metadata.setdefault("source_errors", []).append({"url": candidate_url, "error": load_error})
                    continue
                if rows:
                    raw_rows = rows
                    selected_source_url = candidate_url
                    metadata["source_adapter"] = _STATE_ADAPTER_FALLBACK_MACHINE_READABLE
                    break

            if selected_source_url is None:
                reason = adapter_error or "no_parseable_machine_readable_source"
                if metadata.get("source_errors"):
                    reason = f"no_parseable_machine_readable_source:{metadata['source_errors'][0]['error']}"
                return StateImportStats(
                    supported=False,
                    status="unsupported",
                    source_url=None,
                    unsupported_reason=reason,
                    error_text=None,
                    row_count_parsed=0,
                    row_count_matched=0,
                    row_count_dropped=0,
                    row_count_inserted=0,
                    metadata=metadata,
                )

    npi_resolver: StateNpiResolver | None = None
    if not test_mode and raw_rows:
        npi_resolver = await _build_state_npi_resolver(state_source.state_code)
        if npi_resolver:
            metadata["npi_resolver"] = {
                "license_keys": len(npi_resolver.by_license),
                "name_zip_keys": len(npi_resolver.by_name_zip),
                "name_city_keys": len(npi_resolver.by_name_city),
                "name_keys": len(npi_resolver.by_name),
            }

    row_count_parsed = 0
    row_count_matched = 0
    row_count_dropped = 0
    inserted = 0
    stage_batch: list[dict[str, Any]] = []

    for row in raw_rows:
        row_count_parsed += 1
        normalized, drop_reason = _normalize_stage_row(
            row,
            run_id=run_id,
            snapshot_id=snapshot_id,
            state_source=state_source,
            source_url=selected_source_url or state_source.board_url,
            imported_at=imported_at,
            npi_resolver=npi_resolver,
        )
        if normalized is None:
            row_count_dropped += 1
            if drop_reason:
                metadata.setdefault("drop_reasons", {})
                metadata["drop_reasons"][drop_reason] = metadata["drop_reasons"].get(drop_reason, 0) + 1
            if test_mode and row_count_parsed >= PHARM_LICENSE_TEST_MAX_ROWS_PER_STATE:
                break
            continue

        row_count_matched += 1
        stage_batch.append(normalized)
        if len(stage_batch) >= PHARM_LICENSE_BATCH_SIZE:
            await _flush_stage_batch(stage_batch)
            inserted += PHARM_LICENSE_BATCH_SIZE

        if test_mode and row_count_parsed >= PHARM_LICENSE_TEST_MAX_ROWS_PER_STATE:
            break

    if stage_batch:
        inserted += len(stage_batch)
        await _flush_stage_batch(stage_batch)

    if npi_resolver and npi_resolver.stats:
        metadata["npi_match_stats"] = dict(sorted(npi_resolver.stats.items()))

    if row_count_matched == 0:
        return StateImportStats(
            supported=True,
            status="completed_no_match",
            source_url=selected_source_url,
            unsupported_reason="no_npi_matchable_rows",
            error_text=None,
            row_count_parsed=row_count_parsed,
            row_count_matched=0,
            row_count_dropped=row_count_dropped,
            row_count_inserted=0,
            metadata=metadata,
        )

    return StateImportStats(
        supported=True,
        status="completed",
        source_url=selected_source_url,
        unsupported_reason=None,
        error_text=None,
        row_count_parsed=row_count_parsed,
        row_count_matched=row_count_matched,
        row_count_dropped=row_count_dropped,
        row_count_inserted=inserted,
        metadata=metadata,
    )


def _iter_additional_indexes(obj: type) -> list[dict[str, Any]]:
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        return list(obj.__my_additional_indexes__)
    return []


async def _ensure_indexes(obj: type, schema: str, *, include_additional: bool = True) -> None:
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {obj.__tablename__}_idx_primary "
            f"ON {schema}.{obj.__tablename__} ({cols});"
        )

    if not include_additional:
        return

    for index_data in _iter_additional_indexes(obj):
        elements = index_data.get("index_elements")
        if not elements:
            continue
        index_name = index_data.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
        using = index_data.get("using")
        where = index_data.get("where")
        stmt = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}.{obj.__tablename__}"
        if using:
            stmt += f" USING {using}"
        stmt += f" ({', '.join(elements)})"
        if where:
            stmt += f" WHERE {where}"
        stmt += ";"
        await db.status(stmt)


async def _drop_additional_indexes(obj: type, schema: str) -> None:
    for index_data in _iter_additional_indexes(obj):
        elements = index_data.get("index_elements")
        if not elements:
            continue
        index_name = index_data.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
        await db.status(f"DROP INDEX IF EXISTS {schema}.{index_name};")


async def _ensure_tables() -> str:
    schema = PharmacyLicenseImportRun.__table__.schema or "mrf"
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    for cls in (
        PharmacyLicenseImportRun,
        PharmacyLicenseSnapshot,
        PharmacyLicenseStateCoverage,
        PharmacyLicenseRecordStage,
        PharmacyLicenseRecord,
        PharmacyLicenseRecordHistory,
    ):
        await db.create_table(cls.__table__, checkfirst=True)
        include_additional = True
        if PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES and cls in (PharmacyLicenseRecord, PharmacyLicenseRecordHistory):
            include_additional = False
        await _ensure_indexes(cls, schema, include_additional=include_additional)
    return schema


async def _drop_secondary_indexes(schema: str) -> None:
    for cls in (PharmacyLicenseRecord, PharmacyLicenseRecordHistory):
        await _drop_additional_indexes(cls, schema)


async def _ensure_secondary_indexes(schema: str) -> None:
    for cls in (PharmacyLicenseRecord, PharmacyLicenseRecordHistory):
        await _ensure_indexes(cls, schema, include_additional=True)


async def _analyze_tables(schema: str) -> None:
    await db.status(f"ANALYZE {schema}.{PharmacyLicenseRecordStage.__tablename__};")
    await db.status(f"ANALYZE {schema}.{PharmacyLicenseRecord.__tablename__};")
    await db.status(f"ANALYZE {schema}.{PharmacyLicenseRecordHistory.__tablename__};")


async def _truncate_stage_table(schema: str) -> None:
    await db.status(f"TRUNCATE TABLE {schema}.{PharmacyLicenseRecordStage.__tablename__};")


async def _materialize_snapshot(schema: str, snapshot_id: str, run_id: str) -> int:
    stage_table = f"{schema}.{PharmacyLicenseRecordStage.__tablename__}"
    canonical_table = f"{schema}.{PharmacyLicenseRecord.__tablename__}"
    history_table = f"{schema}.{PharmacyLicenseRecordHistory.__tablename__}"

    await db.status(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (npi, state_code, license_number)
                npi,
                state_code,
                state_name,
                board_url,
                source_url,
                license_number,
                license_type,
                license_status,
                source_status_raw,
                license_issue_date,
                license_effective_date,
                license_expiration_date,
                last_renewal_date,
                disciplinary_flag,
                disciplinary_summary,
                disciplinary_action_date,
                entity_name,
                dba_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                phone_number,
                source_record_id,
                source_last_seen_at,
                imported_at
            FROM {stage_table}
            WHERE snapshot_id = :snapshot_id
            ORDER BY
                npi,
                state_code,
                license_number,
                source_last_seen_at DESC NULLS LAST,
                imported_at DESC NULLS LAST,
                id DESC
        ),
        prepared AS (
            SELECT
                md5(npi::text || '|' || state_code || '|' || license_number) AS license_key,
                md5(
                    jsonb_build_array(
                        state_name,
                        board_url,
                        source_url,
                        license_number,
                        license_type,
                        license_status,
                        source_status_raw,
                        license_issue_date,
                        license_effective_date,
                        license_expiration_date,
                        last_renewal_date,
                        disciplinary_flag,
                        disciplinary_summary,
                        disciplinary_action_date,
                        entity_name,
                        dba_name,
                        address_line1,
                        address_line2,
                        city,
                        state,
                        zip_code,
                        phone_number,
                        source_record_id
                    )::text
                ) AS record_signature,
                CAST(:snapshot_id AS varchar) AS snapshot_id,
                CAST(:run_id AS varchar) AS run_id,
                npi,
                state_code,
                state_name,
                board_url,
                source_url,
                license_number,
                license_type,
                license_status,
                source_status_raw,
                license_issue_date,
                license_effective_date,
                license_expiration_date,
                last_renewal_date,
                disciplinary_flag,
                disciplinary_summary,
                disciplinary_action_date,
                entity_name,
                dba_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                phone_number,
                source_record_id,
                COALESCE(source_last_seen_at, imported_at, CURRENT_TIMESTAMP) AS source_last_seen_at,
                COALESCE(imported_at, CURRENT_TIMESTAMP) AS imported_at
            FROM latest
        )
        INSERT INTO {history_table} (
            snapshot_id,
            run_id,
            license_key,
            record_signature,
            npi,
            state_code,
            state_name,
            board_url,
            source_url,
            license_number,
            license_type,
            license_status,
            source_status_raw,
            license_issue_date,
            license_effective_date,
            license_expiration_date,
            last_renewal_date,
            disciplinary_flag,
            disciplinary_summary,
            disciplinary_action_date,
            entity_name,
            dba_name,
            address_line1,
            address_line2,
            city,
            state,
            zip_code,
            phone_number,
            source_record_id,
            source_last_seen_at,
            imported_at
        )
        SELECT
            p.snapshot_id,
            p.run_id,
            p.license_key,
            p.record_signature,
            p.npi,
            p.state_code,
            p.state_name,
            p.board_url,
            p.source_url,
            p.license_number,
            p.license_type,
            p.license_status,
            p.source_status_raw,
            p.license_issue_date,
            p.license_effective_date,
            p.license_expiration_date,
            p.last_renewal_date,
            p.disciplinary_flag,
            p.disciplinary_summary,
            p.disciplinary_action_date,
            p.entity_name,
            p.dba_name,
            p.address_line1,
            p.address_line2,
            p.city,
            p.state,
            p.zip_code,
            p.phone_number,
            p.source_record_id,
            p.source_last_seen_at,
            p.imported_at
        FROM prepared p
        LEFT JOIN {canonical_table} c ON c.license_key = p.license_key
        WHERE c.license_key IS NULL OR c.record_signature IS DISTINCT FROM p.record_signature;
        """,
        snapshot_id=snapshot_id,
        run_id=run_id,
    )

    await db.status(
        f"""
        WITH latest AS (
            SELECT DISTINCT ON (npi, state_code, license_number)
                npi,
                state_code,
                state_name,
                board_url,
                source_url,
                license_number,
                license_type,
                license_status,
                source_status_raw,
                license_issue_date,
                license_effective_date,
                license_expiration_date,
                last_renewal_date,
                disciplinary_flag,
                disciplinary_summary,
                disciplinary_action_date,
                entity_name,
                dba_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                phone_number,
                source_record_id,
                source_last_seen_at,
                imported_at
            FROM {stage_table}
            WHERE snapshot_id = :snapshot_id
            ORDER BY
                npi,
                state_code,
                license_number,
                source_last_seen_at DESC NULLS LAST,
                imported_at DESC NULLS LAST,
                id DESC
        ),
        prepared AS (
            SELECT
                md5(npi::text || '|' || state_code || '|' || license_number) AS license_key,
                md5(
                    jsonb_build_array(
                        state_name,
                        board_url,
                        source_url,
                        license_number,
                        license_type,
                        license_status,
                        source_status_raw,
                        license_issue_date,
                        license_effective_date,
                        license_expiration_date,
                        last_renewal_date,
                        disciplinary_flag,
                        disciplinary_summary,
                        disciplinary_action_date,
                        entity_name,
                        dba_name,
                        address_line1,
                        address_line2,
                        city,
                        state,
                        zip_code,
                        phone_number,
                        source_record_id
                    )::text
                ) AS record_signature,
                CAST(:snapshot_id AS varchar) AS last_snapshot_id,
                npi,
                state_code,
                state_name,
                board_url,
                source_url,
                license_number,
                license_type,
                license_status,
                source_status_raw,
                license_issue_date,
                license_effective_date,
                license_expiration_date,
                last_renewal_date,
                disciplinary_flag,
                disciplinary_summary,
                disciplinary_action_date,
                entity_name,
                dba_name,
                address_line1,
                address_line2,
                city,
                state,
                zip_code,
                phone_number,
                source_record_id,
                COALESCE(source_last_seen_at, imported_at, CURRENT_TIMESTAMP) AS source_last_seen_at,
                COALESCE(imported_at, CURRENT_TIMESTAMP) AS imported_at
            FROM latest
        )
        INSERT INTO {canonical_table} (
            license_key,
            record_signature,
            last_snapshot_id,
            npi,
            state_code,
            state_name,
            board_url,
            source_url,
            license_number,
            license_type,
            license_status,
            source_status_raw,
            license_issue_date,
            license_effective_date,
            license_expiration_date,
            last_renewal_date,
            disciplinary_flag,
            disciplinary_summary,
            disciplinary_action_date,
            entity_name,
            dba_name,
            address_line1,
            address_line2,
            city,
            state,
            zip_code,
            phone_number,
            source_record_id,
            first_seen_at,
            last_seen_at,
            last_verified_at,
            updated_at
        )
        SELECT
            p.license_key,
            p.record_signature,
            p.last_snapshot_id,
            p.npi,
            p.state_code,
            p.state_name,
            p.board_url,
            p.source_url,
            p.license_number,
            p.license_type,
            p.license_status,
            p.source_status_raw,
            p.license_issue_date,
            p.license_effective_date,
            p.license_expiration_date,
            p.last_renewal_date,
            p.disciplinary_flag,
            p.disciplinary_summary,
            p.disciplinary_action_date,
            p.entity_name,
            p.dba_name,
            p.address_line1,
            p.address_line2,
            p.city,
            p.state,
            p.zip_code,
            p.phone_number,
            p.source_record_id,
            p.source_last_seen_at,
            p.source_last_seen_at,
            p.source_last_seen_at,
            p.imported_at
        FROM prepared p
        ON CONFLICT (license_key)
        DO UPDATE SET
            record_signature = EXCLUDED.record_signature,
            last_snapshot_id = EXCLUDED.last_snapshot_id,
            npi = EXCLUDED.npi,
            state_code = EXCLUDED.state_code,
            state_name = EXCLUDED.state_name,
            board_url = EXCLUDED.board_url,
            source_url = EXCLUDED.source_url,
            license_number = EXCLUDED.license_number,
            license_type = EXCLUDED.license_type,
            license_status = EXCLUDED.license_status,
            source_status_raw = EXCLUDED.source_status_raw,
            license_issue_date = EXCLUDED.license_issue_date,
            license_effective_date = EXCLUDED.license_effective_date,
            license_expiration_date = EXCLUDED.license_expiration_date,
            last_renewal_date = EXCLUDED.last_renewal_date,
            disciplinary_flag = EXCLUDED.disciplinary_flag,
            disciplinary_summary = EXCLUDED.disciplinary_summary,
            disciplinary_action_date = EXCLUDED.disciplinary_action_date,
            entity_name = EXCLUDED.entity_name,
            dba_name = EXCLUDED.dba_name,
            address_line1 = EXCLUDED.address_line1,
            address_line2 = EXCLUDED.address_line2,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code,
            phone_number = EXCLUDED.phone_number,
            source_record_id = EXCLUDED.source_record_id,
            first_seen_at = LEAST({canonical_table}.first_seen_at, EXCLUDED.first_seen_at),
            last_seen_at = GREATEST({canonical_table}.last_seen_at, EXCLUDED.last_seen_at),
            last_verified_at = EXCLUDED.last_verified_at,
            updated_at = EXCLUDED.updated_at;
        """,
        snapshot_id=snapshot_id,
    )

    result = await db.all(
        f"SELECT COUNT(*)::int FROM {stage_table} WHERE snapshot_id = :snapshot_id",
        snapshot_id=snapshot_id,
    )
    stage_rows = int(result[0][0]) if result and result[0] and result[0][0] is not None else 0

    await db.status(
        f"DELETE FROM {stage_table} WHERE snapshot_id = :snapshot_id",
        snapshot_id=snapshot_id,
    )
    return stage_rows


async def _upsert_run(payload: dict[str, Any]) -> None:
    await push_objects([payload], PharmacyLicenseImportRun, rewrite=True, use_copy=False)


async def _upsert_snapshot(payload: dict[str, Any]) -> None:
    await push_objects([payload], PharmacyLicenseSnapshot, rewrite=True, use_copy=False)


async def _upsert_coverage(payload: dict[str, Any]) -> None:
    await push_objects([payload], PharmacyLicenseStateCoverage, rewrite=True, use_copy=False)


async def pharmacy_license_start(ctx, task=None):  # pragma: no cover
    del ctx
    task = task or {}
    run_id = _normalize_run_id(task.get("run_id"))
    import_id = _normalize_import_id(task.get("import_id"))
    test_mode = bool(task.get("test_mode"))
    await ensure_database(test_mode)

    schema = await _ensure_tables()
    await _truncate_stage_table(schema)
    if PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES and PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT:
        await _drop_secondary_indexes(schema)

    now = datetime.datetime.utcnow()
    await _upsert_run(
        {
            "run_id": run_id,
            "import_id": import_id,
            "status": "running",
            "started_at": now,
            "finished_at": None,
            "source_summary": {"test_mode": test_mode, "states": []},
            "error_text": None,
        }
    )

    try:
        fda_html = await download_it(FDA_STATE_BOARD_URL, local_timeout=120)
        state_sources = _parse_fda_state_sources(fda_html)
        if not state_sources:
            raise RuntimeError("No state board sources discovered from FDA page")
        if test_mode:
            state_sources = state_sources[:PHARM_LICENSE_TEST_MAX_STATES]

        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "running",
                "started_at": now,
                "finished_at": None,
                "source_summary": {
                    "test_mode": test_mode,
                    "states": [
                        {
                            "state_code": source.state_code,
                            "state_name": source.state_name,
                            "board_url": source.board_url,
                        }
                        for source in state_sources
                    ],
                },
                "error_text": None,
            }
        )

        summary_totals = {
            "states": len(state_sources),
            "supported_states": 0,
            "unsupported_states": 0,
            "parsed_rows": 0,
            "matched_rows": 0,
            "dropped_rows": 0,
            "inserted_rows": 0,
        }

        timeout = aiohttp.ClientTimeout(total=PHARM_LICENSE_SOURCE_TIMEOUT_SECONDS)
        connector = aiohttp.TCPConnector(limit=max(4, min(12, len(state_sources))))
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={"User-Agent": PHARM_LICENSE_HTTP_USER_AGENT},
        ) as session:
            for source in state_sources:
                snapshot_id = _hash_snapshot_id(run_id, source.state_code, source.board_url)
                await _upsert_snapshot(
                    {
                        "snapshot_id": snapshot_id,
                        "run_id": run_id,
                        "state_code": source.state_code,
                        "state_name": source.state_name,
                        "board_url": source.board_url,
                        "source_url": None,
                        "status": "running",
                        "row_count_parsed": 0,
                        "row_count_matched": 0,
                        "row_count_dropped": 0,
                        "row_count_inserted": 0,
                        "imported_at": None,
                        "error_text": None,
                        "metadata_json": {"test_mode": test_mode},
                    }
                )

                try:
                    stats = await _import_state_source(
                        session,
                        source,
                        run_id=run_id,
                        snapshot_id=snapshot_id,
                        test_mode=test_mode,
                    )
                    inserted_rows = 0
                    if stats.row_count_matched > 0:
                        inserted_rows = await _materialize_snapshot(schema, snapshot_id, run_id)

                    await _upsert_snapshot(
                        {
                            "snapshot_id": snapshot_id,
                            "run_id": run_id,
                            "state_code": source.state_code,
                            "state_name": source.state_name,
                            "board_url": source.board_url,
                            "source_url": stats.source_url,
                            "status": stats.status,
                            "row_count_parsed": stats.row_count_parsed,
                            "row_count_matched": stats.row_count_matched,
                            "row_count_dropped": stats.row_count_dropped,
                            "row_count_inserted": inserted_rows,
                            "imported_at": datetime.datetime.utcnow(),
                            "error_text": stats.error_text,
                            "metadata_json": stats.metadata,
                        }
                    )

                    await _upsert_coverage(
                        {
                            "state_code": source.state_code,
                            "state_name": source.state_name,
                            "board_url": source.board_url,
                            "source_url": stats.source_url,
                            "supported": bool(stats.supported),
                            "unsupported_reason": stats.unsupported_reason,
                            "status": stats.status,
                            "last_attempted_at": datetime.datetime.utcnow(),
                            "last_success_at": datetime.datetime.utcnow()
                            if stats.status in {"completed", "completed_no_match"}
                            else None,
                            "last_run_id": run_id,
                            "records_parsed": stats.row_count_parsed,
                            "records_matched": stats.row_count_matched,
                            "records_dropped": stats.row_count_dropped,
                            "records_inserted": inserted_rows,
                            "updated_at": datetime.datetime.utcnow(),
                        }
                    )

                    summary_totals["parsed_rows"] += stats.row_count_parsed
                    summary_totals["matched_rows"] += stats.row_count_matched
                    summary_totals["dropped_rows"] += stats.row_count_dropped
                    summary_totals["inserted_rows"] += inserted_rows
                    if stats.supported:
                        summary_totals["supported_states"] += 1
                    else:
                        summary_totals["unsupported_states"] += 1

                except Exception as exc:  # pylint: disable=broad-exception-caught
                    await _upsert_snapshot(
                        {
                            "snapshot_id": snapshot_id,
                            "run_id": run_id,
                            "state_code": source.state_code,
                            "state_name": source.state_name,
                            "board_url": source.board_url,
                            "source_url": None,
                            "status": "failed",
                            "row_count_parsed": 0,
                            "row_count_matched": 0,
                            "row_count_dropped": 0,
                            "row_count_inserted": 0,
                            "imported_at": datetime.datetime.utcnow(),
                            "error_text": str(exc),
                            "metadata_json": {},
                        }
                    )
                    await _upsert_coverage(
                        {
                            "state_code": source.state_code,
                            "state_name": source.state_name,
                            "board_url": source.board_url,
                            "source_url": None,
                            "supported": False,
                            "unsupported_reason": "state_import_failed",
                            "status": "failed",
                            "last_attempted_at": datetime.datetime.utcnow(),
                            "last_success_at": None,
                            "last_run_id": run_id,
                            "records_parsed": 0,
                            "records_matched": 0,
                            "records_dropped": 0,
                            "records_inserted": 0,
                            "updated_at": datetime.datetime.utcnow(),
                        }
                    )
                    summary_totals["unsupported_states"] += 1

        if PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES:
            await _ensure_secondary_indexes(schema)
            await _analyze_tables(schema)
        await _truncate_stage_table(schema)

        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "completed",
                "started_at": now,
                "finished_at": datetime.datetime.utcnow(),
                "source_summary": {
                    "test_mode": test_mode,
                    **summary_totals,
                },
                "error_text": None,
            }
        )
        print(
            f"pharmacy_license import completed run_id={run_id} "
            f"states={summary_totals['states']} matched={summary_totals['matched_rows']} "
            f"inserted={summary_totals['inserted_rows']}",
            flush=True,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        await _truncate_stage_table(schema)
        if PHARM_LICENSE_DEFER_ADDITIONAL_INDEXES and PHARM_LICENSE_DROP_ADDITIONAL_INDEXES_BEFORE_IMPORT:
            try:
                await _ensure_secondary_indexes(schema)
                await _analyze_tables(schema)
            except Exception:  # pylint: disable=broad-exception-caught
                pass
        await _upsert_run(
            {
                "run_id": run_id,
                "import_id": import_id,
                "status": "failed",
                "started_at": now,
                "finished_at": datetime.datetime.utcnow(),
                "source_summary": {"test_mode": test_mode},
                "error_text": str(exc),
            }
        )
        raise


async def pharmacy_license_finalize(_ctx, task=None):  # pragma: no cover
    task = task or {}
    run_id = _normalize_run_id(task.get("run_id"))
    import_id = _normalize_import_id(task.get("import_id"))
    now = datetime.datetime.utcnow()
    await _upsert_run(
        {
            "run_id": run_id,
            "import_id": import_id,
            "status": "completed",
            "started_at": now,
            "finished_at": now,
            "source_summary": {"finalized_via_queue": True},
            "error_text": None,
        }
    )


async def main(test_mode: bool = False, import_id: str | None = None):  # pragma: no cover
    run_id = _normalize_run_id(None)
    normalized_import_id = _normalize_import_id(import_id)
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job(
        "pharmacy_license_start",
        {
            "run_id": run_id,
            "import_id": normalized_import_id,
            "test_mode": bool(test_mode),
        },
        _queue_name=PHARM_LICENSE_QUEUE_NAME,
    )
    print(
        json.dumps(
            {
                "status": "queued",
                "run_id": run_id,
                "import_id": normalized_import_id,
                "queue_name": PHARM_LICENSE_QUEUE_NAME,
                "test_mode": bool(test_mode),
            },
            ensure_ascii=True,
        )
    )
    return run_id


async def finish_main(
    import_id: str,
    run_id: str,
    test_mode: bool = False,
    manifest_path: str | None = None,
):  # pragma: no cover
    del manifest_path
    normalized_run_id = _normalize_run_id(run_id)
    normalized_import_id = _normalize_import_id(import_id)
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    await redis.enqueue_job(
        "pharmacy_license_finalize",
        {
            "run_id": normalized_run_id,
            "import_id": normalized_import_id,
            "test_mode": bool(test_mode),
        },
        _queue_name=PHARM_LICENSE_FINISH_QUEUE_NAME,
    )
    print(
        json.dumps(
            {
                "status": "queued",
                "run_id": normalized_run_id,
                "import_id": normalized_import_id,
                "queue_name": PHARM_LICENSE_FINISH_QUEUE_NAME,
                "test_mode": bool(test_mode),
            },
            ensure_ascii=True,
        )
    )


async def startup(_ctx):  # pragma: no cover
    await db_startup(_ctx)


async def shutdown(_ctx):  # pragma: no cover
    return None
