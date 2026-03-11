# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import codecs
import csv
import datetime
import hashlib
import json
import os
import re
import sys
import tempfile
import zipfile
from pathlib import PurePath
from typing import Any

from aiofile import async_open
from aiocsv import AsyncDictReader
from arq import create_pool
from asyncpg import DuplicateTableError
from dateutil.parser import parse as parse_date

from db.models import (
    NPIData,
    PricingProvider,
    ProviderEnrichmentSummary,
    ProviderEnrollmentFFSAdditionalNPI,
    ProviderEnrollmentFFSAddress,
    ProviderEnrollmentFFS,
    ProviderEnrollmentFFSReassignment,
    ProviderEnrollmentFFSSecondarySpecialty,
    ProviderEnrollmentFQHC,
    ProviderEnrollmentHomeHealthAgency,
    ProviderEnrollmentHospital,
    ProviderEnrollmentHospice,
    ProviderEnrollmentRHC,
    ProviderEnrollmentSNF,
    db,
)
from process.ext.utils import (
    download_it,
    download_it_and_save,
    ensure_database,
    make_class,
    my_init_db,
    print_time_info,
    push_objects,
    return_checksum,
)
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job

TEST_PROVIDER_ENRICHMENT_ROWS = 1500
TEST_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET = 1
DEFAULT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET = 1
PROVIDER_ENRICHMENT_QUEUE_NAME = "arq:ProviderEnrichment"
POSTGRES_IDENTIFIER_MAX_LENGTH = 63

CATALOG_URL = os.getenv("HLTHPRT_PROVIDER_ENRICHMENT_CATALOG_URL", "https://data.cms.gov/data.json")
SOURCE_DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024
DEFAULT_MAX_PENDING_SAVE_TASKS = 4
DEFAULT_PROVIDER_ENRICHMENT_BATCH_SIZE = 5000
FFS_LATEST_DESCRIPTION = "latest"
CSV_PRIMARY_ENCODING = "utf-8-sig"
CSV_FALLBACK_ENCODING = "cp1252"
CSV_PROBE_CHUNK_SIZE = 1024 * 1024

STRICT_SOURCE_PRESENCE = str(
    os.getenv("HLTHPRT_PROVIDER_ENRICHMENT_STRICT_SOURCE_PRESENCE", "1")
).strip().lower() in {"1", "true", "yes", "on"}
ENABLE_NPPES_GAP_CHECK = str(
    os.getenv("HLTHPRT_PROVIDER_ENRICHMENT_ENABLE_NPPES_GAP_CHECK", "0")
).strip().lower() in {"1", "true", "yes", "on"}
INCLUDE_PROVIDER_ENRICHMENT_HISTORY = str(
    os.getenv("HLTHPRT_PROVIDER_ENRICHMENT_INCLUDE_HISTORY", "0")
).strip().lower() in {"1", "true", "yes", "on"}


def _env_positive_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _env_optional_limit(name: str, default: int | None) -> int | None:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    if value <= 0:
        return None
    return value


def _archived_identifier(name: str, suffix: str = "_old") -> str:
    candidate = f"{name}{suffix}"
    if len(candidate) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return candidate
    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(suffix) - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}{suffix}"


def _normalize_import_id(raw: str | None) -> str:
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return datetime.datetime.now().strftime("%Y%m%d")


def _normalize_title(raw: str) -> str:
    return " ".join(str(raw or "").strip().lower().split())


def _safe_int(raw: Any, default: int | None = None) -> int | None:
    if raw in (None, ""):
        return default
    text = str(raw).strip()
    if not text:
        return default
    digits = "".join(ch for ch in text if ch.isdigit())
    if not digits:
        return default
    try:
        return int(digits)
    except (TypeError, ValueError):
        return default


def _safe_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def _safe_state(raw: Any) -> str | None:
    text = _safe_text(raw)
    if not text:
        return None
    text = text.upper()
    return text[:2] if len(text) >= 2 else text


def _safe_zip(raw: Any) -> str | None:
    text = _safe_text(raw)
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 5:
        return digits[:5]
    return text[:12]


def _safe_date(raw: Any) -> datetime.date | None:
    text = _safe_text(raw)
    if not text:
        return None
    try:
        return parse_date(text, fuzzy=True).date()
    except (TypeError, ValueError, OverflowError):
        return None


def _safe_datetime(raw: Any) -> datetime.datetime | None:
    text = _safe_text(raw)
    if not text:
        return None
    try:
        parsed = parse_date(text, fuzzy=True)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return parsed
    except (TypeError, ValueError, OverflowError):
        return None


def _sql_varchar_array_literal(values: list[str]) -> str:
    normalized = [str(v) for v in values if str(v).strip()]
    if not normalized:
        return "ARRAY[]::varchar[]"
    escaped = ["'" + value.replace("'", "''") + "'" for value in normalized]
    return f"ARRAY[{', '.join(escaped)}]::varchar[]"


def _is_csv_distribution(obj: dict[str, Any]) -> bool:
    media_type = _normalize_title(obj.get("mediaType") or "")
    fmt = _normalize_title(obj.get("format") or "")
    return "csv" in media_type or "csv" in fmt


def _looks_like_csv_download(url: str) -> bool:
    normalized = str(url or "").strip().lower()
    return normalized.endswith(".csv")


def _extract_period_bounds(temporal: str | None) -> tuple[datetime.date | None, datetime.date | None]:
    text = str(temporal or "").strip()
    if not text or "/" not in text:
        return None, None
    start_raw, end_raw = text.split("/", 1)
    return _safe_date(start_raw), _safe_date(end_raw)


def _extract_year(*values: Any) -> int | None:
    year_pattern = re.compile(r"\b(20\d{2})\b")
    for value in values:
        text = str(value or "")
        match = year_pattern.search(text)
        if match:
            try:
                return int(match.group(1))
            except (TypeError, ValueError):
                continue
    return None


def _resolve_header(row: dict[str, Any], aliases: tuple[str, ...]) -> Any:
    for alias in aliases:
        if alias in row:
            value = row.get(alias)
            if value not in (None, ""):
                return value
    return None


def _normalize_nppes_header(raw: str) -> str:
    normalized = re.sub(r"\(.*\)", "", str(raw).lower()).strip().replace(" ", "_")
    normalized = re.sub(r"[^a-z0-9_]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def _make_field(name: str, aliases: tuple[str, ...], required: bool = False) -> dict[str, Any]:
    return {
        "name": name,
        "aliases": aliases,
        "required": required,
    }


COMMON_FIELDS = (
    _make_field("npi", ("NPI",), required=True),
    _make_field("enrollment_id", ("ENROLLMENT ID", "ENRLMT_ID"), required=True),
    _make_field("enrollment_state", ("ENROLLMENT STATE", "STATE_CD")),
    _make_field("provider_type_code", ("PROVIDER TYPE CODE", "PROVIDER_TYPE_CD"), required=True),
    _make_field("provider_type_text", ("PROVIDER TYPE TEXT", "PROVIDER_TYPE_DESC"), required=True),
    _make_field("multiple_npi_flag", ("MULTIPLE NPI FLAG", "MULTIPLE_NPI_FLAG")),
    _make_field("ccn", ("CCN",)),
    _make_field("associate_id", ("ASSOCIATE ID",)),
    _make_field("organization_name", ("ORGANIZATION NAME",)),
    _make_field("doing_business_as_name", ("DOING BUSINESS AS NAME",)),
    _make_field("incorporation_date", ("INCORPORATION DATE",)),
    _make_field("incorporation_state", ("INCORPORATION STATE",)),
    _make_field("organization_type_structure", ("ORGANIZATION TYPE STRUCTURE",)),
    _make_field("organization_other_type_text", ("ORGANIZATION OTHER TYPE TEXT",)),
    _make_field("proprietary_nonprofit", ("PROPRIETARY_NONPROFIT", "PROPRIETARY NONPROFIT")),
    _make_field("address_line_1", ("ADDRESS LINE 1",)),
    _make_field("address_line_2", ("ADDRESS LINE 2",)),
    _make_field("city", ("CITY",)),
    _make_field("state", ("STATE", "STATE_CD")),
    _make_field("zip_code", ("ZIP CODE",)),
)


ENROLLMENT_DATASET_SPECS: tuple[dict[str, Any], ...] = (
    {
        "key": "ffs_public",
        "label": "Medicare Fee-For-Service Public Provider Enrollment",
        "title_patterns": (
            "medicare fee-for-service public provider enrollment",
            "medicare fee-for-service  public provider enrollment",
        ),
        "model": ProviderEnrollmentFFS,
        "task_key": "ffs_rows",
        "discovery": "ffs_resource_bundle",
        "resource_name_patterns": (
            "medicare ffs public provider enrollment",
        ),
        "fields": COMMON_FIELDS
        + (
            _make_field("pecos_asct_cntl_id", ("PECOS_ASCT_CNTL_ID",)),
            _make_field("first_name", ("FIRST_NAME",)),
            _make_field("middle_name", ("MDL_NAME",)),
            _make_field("last_name", ("LAST_NAME",)),
            _make_field("org_name", ("ORG_NAME",)),
        ),
    },
    {
        "key": "ffs_additional_npi",
        "label": "Medicare FFS Additional NPIs",
        "model": ProviderEnrollmentFFSAdditionalNPI,
        "task_key": "ffs_additional_npi_rows",
        "discovery": "ffs_resource_bundle",
        "resource_name_patterns": ("additional npis sub-file",),
        "fields": (
            _make_field("enrollment_id", ("ENRLMT_ID", "ENROLLMENT ID"), required=True),
            _make_field("additional_npi", ("NPI",), required=True),
        ),
        "payload_builder": "ffs_additional_npi",
    },
    {
        "key": "ffs_reassignment",
        "label": "Medicare FFS Reassignment",
        "model": ProviderEnrollmentFFSReassignment,
        "task_key": "ffs_reassignment_rows",
        "discovery": "ffs_resource_bundle",
        "resource_name_patterns": ("reassignment sub-file",),
        "fields": (
            _make_field("reassigning_enrollment_id", ("REASGN_BNFT_ENRLMT_ID",), required=True),
            _make_field("receiving_enrollment_id", ("RCV_BNFT_ENRLMT_ID",), required=True),
        ),
        "payload_builder": "ffs_reassignment",
    },
    {
        "key": "ffs_address",
        "label": "Medicare FFS Practice Locations",
        "model": ProviderEnrollmentFFSAddress,
        "task_key": "ffs_address_rows",
        "discovery": "ffs_resource_bundle",
        "resource_name_patterns": ("address sub-file",),
        "fields": (
            _make_field("enrollment_id", ("ENRLMT_ID", "ENROLLMENT ID"), required=True),
            _make_field("city", ("CITY_NAME", "CITY")),
            _make_field("state", ("STATE_CD", "STATE")),
            _make_field("zip_code", ("ZIP_CD", "ZIP CODE"), required=True),
        ),
        "payload_builder": "ffs_address",
    },
    {
        "key": "ffs_secondary_specialty",
        "label": "Medicare FFS Secondary Specialties",
        "model": ProviderEnrollmentFFSSecondarySpecialty,
        "task_key": "ffs_secondary_specialty_rows",
        "discovery": "ffs_resource_bundle",
        "resource_name_patterns": ("secondary specialty sub-file",),
        "fields": (
            _make_field("enrollment_id", ("ENRLMT_ID", "ENROLLMENT ID"), required=True),
            _make_field("provider_type_code", ("PROVIDER_TYPE_CD",), required=True),
            _make_field("provider_type_text", ("PROVIDER_TYPE_DESC",)),
        ),
        "payload_builder": "ffs_secondary_specialty",
    },
    {
        "key": "hospital",
        "label": "Hospital Enrollments",
        "title_patterns": ("hospital enrollments",),
        "model": ProviderEnrollmentHospital,
        "task_key": "hospital_rows",
        "fields": COMMON_FIELDS
        + (
            _make_field("practice_location_type", ("PRACTICE LOCATION TYPE",)),
            _make_field("location_other_type_text", ("LOCATION OTHER TYPE TEXT",)),
            _make_field("subgroup_general", ("SUBGROUP - GENERAL",)),
            _make_field("subgroup_acute_care", ("SUBGROUP - ACUTE CARE",)),
            _make_field("subgroup_alcohol_drug", ("SUBGROUP - ALCOHOL DRUG",)),
            _make_field("subgroup_childrens", ("SUBGROUP - CHILDRENS",)),
            _make_field("subgroup_long_term", ("SUBGROUP - LONG-TERM",)),
            _make_field("subgroup_psychiatric", ("SUBGROUP - PSYCHIATRIC",)),
            _make_field("subgroup_rehabilitation", ("SUBGROUP - REHABILITATION",)),
            _make_field("subgroup_short_term", ("SUBGROUP - SHORT-TERM",)),
            _make_field("subgroup_swing_bed_approved", ("SUBGROUP - SWING-BED APPROVED",)),
            _make_field("subgroup_psychiatric_unit", ("SUBGROUP - PSYCHIATRIC UNIT",)),
            _make_field("subgroup_rehabilitation_unit", ("SUBGROUP - REHABILITATION UNIT",)),
            _make_field("subgroup_specialty_hospital", ("SUBGROUP - SPECIALTY HOSPITAL",)),
            _make_field("subgroup_other", ("SUBGROUP - OTHER",)),
            _make_field("subgroup_other_text", ("SUBGROUP - OTHER TEXT",)),
            _make_field("reh_conversion_flag", ("REH CONVERSION FLAG",)),
            _make_field("reh_conversion_date", ("REH CONVERSION DATE",)),
            _make_field("cah_or_hospital_ccn", ("CAH OR HOSPITAL CCN",)),
        ),
    },
    {
        "key": "hha",
        "label": "Home Health Agency Enrollments",
        "title_patterns": ("home health agency enrollments",),
        "model": ProviderEnrollmentHomeHealthAgency,
        "task_key": "hha_rows",
        "fields": COMMON_FIELDS
        + (
            _make_field("practice_location_type", ("PRACTICE LOCATION TYPE",)),
            _make_field("location_other_type_text", ("LOCATION OTHER TYPE TEXT",)),
        ),
    },
    {
        "key": "hospice",
        "label": "Hospice Enrollments",
        "title_patterns": ("hospice enrollments",),
        "model": ProviderEnrollmentHospice,
        "task_key": "hospice_rows",
        "fields": COMMON_FIELDS,
    },
    {
        "key": "fqhc",
        "label": "Federally Qualified Health Center Enrollments",
        "title_patterns": ("federally qualified health center enrollments",),
        "model": ProviderEnrollmentFQHC,
        "task_key": "fqhc_rows",
        "fields": COMMON_FIELDS
        + (
            _make_field("telephone_number", ("TELEPHONE NUMBER",)),
        ),
    },
    {
        "key": "rhc",
        "label": "Rural Health Clinic Enrollments",
        "title_patterns": ("rural health clinic enrollments",),
        "model": ProviderEnrollmentRHC,
        "task_key": "rhc_rows",
        "fields": COMMON_FIELDS
        + (
            _make_field("telephone_number", ("TELEPHONE NUMBER",)),
        ),
    },
    {
        "key": "snf",
        "label": "Skilled Nursing Facility Enrollments",
        "title_patterns": ("skilled nursing facility enrollments",),
        "model": ProviderEnrollmentSNF,
        "task_key": "snf_rows",
        "fields": COMMON_FIELDS
        + (
            _make_field("nursing_home_provider_name", ("NURSING HOME PROVIDER NAME",)),
            _make_field("affiliation_entity_name", ("AFFILIATION ENTITY NAME",)),
            _make_field("affiliation_entity_id", ("AFFILIATION ENTITY ID",)),
        ),
    },
)

SPEC_BY_KEY = {spec["key"]: spec for spec in ENROLLMENT_DATASET_SPECS}
TASK_KEY_TO_MODEL = {spec["task_key"]: spec["model"] for spec in ENROLLMENT_DATASET_SPECS}
CATALOG_DISCOVERY_SPECS = tuple(spec for spec in ENROLLMENT_DATASET_SPECS if spec.get("discovery") != "ffs_resource_bundle")
FFS_RESOURCE_BUNDLE_SPECS = tuple(spec for spec in ENROLLMENT_DATASET_SPECS if spec.get("discovery") == "ffs_resource_bundle")
PROCESSING_CLASSES = tuple(spec["model"] for spec in ENROLLMENT_DATASET_SPECS) + (ProviderEnrichmentSummary,)


async def _table_exists(schema: str, table: str) -> bool:
    exists = await db.scalar(f"SELECT to_regclass('{schema}.{table}');")
    return bool(exists)


def _match_spec(title: str) -> dict[str, Any] | None:
    normalized = _normalize_title(title)
    for spec in CATALOG_DISCOVERY_SPECS:
        for pattern in spec["title_patterns"]:
            if _normalize_title(pattern) == normalized:
                return spec
    return None


def _match_ffs_resource_spec(name: str) -> dict[str, Any] | None:
    normalized = _normalize_title(name)
    for spec in FFS_RESOURCE_BUNDLE_SPECS:
        for pattern in spec.get("resource_name_patterns") or ():
            if _normalize_title(pattern) in normalized:
                return spec
    return None


def _is_ffs_bundle_dataset_title(title: str) -> bool:
    normalized = _normalize_title(title)
    for pattern in SPEC_BY_KEY["ffs_public"].get("title_patterns") or ():
        if _normalize_title(pattern) == normalized:
            return True
    return False


def _looks_like_provider_enrollment_title(title: str) -> bool:
    normalized = _normalize_title(title)
    if "enrollment" not in normalized:
        return False
    if "program statistics" in normalized:
        return False
    if "monthly enrollment" in normalized:
        return False
    if "medicare advantage" in normalized:
        return False
    if "part d" in normalized:
        return False
    return True


def _collect_csv_distributions(dataset_obj: dict[str, Any]) -> list[dict[str, Any]]:
    distributions = []
    for item in dataset_obj.get("distribution") or []:
        if not isinstance(item, dict):
            continue
        if not item.get("downloadURL"):
            continue
        if not _is_csv_distribution(item):
            continue
        distributions.append(item)
    distributions.sort(
        key=lambda row: _safe_datetime(row.get("modified")) or datetime.datetime.min,
        reverse=True,
    )
    return distributions


async def _discover_sources(test_mode: bool) -> tuple[list[dict[str, Any]], list[str]]:
    payload = await download_it(CATALOG_URL)
    catalog = json.loads(payload)
    datasets = catalog.get("dataset") or []

    unmapped: set[str] = set()
    discovered: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    async def _discover_ffs_resource_bundle() -> None:
        ffs_dataset = None
        for dataset in datasets:
            if not isinstance(dataset, dict):
                continue
            if _is_ffs_bundle_dataset_title(str(dataset.get("title") or "")):
                ffs_dataset = dataset
                break

        if ffs_dataset is None:
            if STRICT_SOURCE_PRESENCE:
                raise RuntimeError("No FFS provider-enrollment dataset found in CMS catalog.")
            return

        resources_api = None
        latest_distribution = None
        for distribution in ffs_dataset.get("distribution") or []:
            if not isinstance(distribution, dict):
                continue
            if _normalize_title(distribution.get("description") or "") != FFS_LATEST_DESCRIPTION:
                continue
            resources_api = str(distribution.get("resourcesAPI") or "").strip()
            latest_distribution = distribution
            if resources_api:
                break

        if not resources_api:
            if STRICT_SOURCE_PRESENCE:
                raise RuntimeError("FFS provider-enrollment dataset is missing a latest resources API.")
            return

        resource_payload = await download_it(resources_api)
        resources = (json.loads(resource_payload).get("data") or [])
        bundle_hits: set[str] = set()
        resource_modified = _safe_datetime(latest_distribution.get("modified")) if latest_distribution else None
        resource_temporal = _safe_text(latest_distribution.get("temporal")) if latest_distribution else None
        period_start, period_end = _extract_period_bounds(resource_temporal)

        for resource in resources:
            if not isinstance(resource, dict):
                continue
            resource_name = str(resource.get("name") or "").strip()
            download_url = str(resource.get("downloadURL") or "").strip()
            if not resource_name or not download_url:
                continue
            if "historical" in _normalize_title(resource_name):
                continue
            if not _looks_like_csv_download(download_url):
                continue
            spec = _match_ffs_resource_spec(resource_name)
            if spec is None or download_url in seen_urls:
                continue

            reporting_year = _extract_year(
                resource_name,
                latest_distribution.get("title") if latest_distribution else None,
                latest_distribution.get("modified") if latest_distribution else None,
            ) or datetime.datetime.utcnow().year

            discovered.append(
                {
                    "spec_key": spec["key"],
                    "dataset_title": ffs_dataset.get("title"),
                    "distribution_title": resource_name,
                    "download_url": download_url,
                    "source_modified": resource_modified,
                    "source_temporal": resource_temporal,
                    "reporting_period_start": period_start,
                    "reporting_period_end": period_end,
                    "reporting_year": reporting_year,
                }
            )
            seen_urls.add(download_url)
            bundle_hits.add(spec["key"])

        if STRICT_SOURCE_PRESENCE:
            missing = sorted(spec["key"] for spec in FFS_RESOURCE_BUNDLE_SPECS if spec["key"] not in bundle_hits)
            if missing:
                raise RuntimeError(
                    "FFS provider-enrollment resource bundle is missing required CSV files: "
                    + ", ".join(missing)
                )

    await _discover_ffs_resource_bundle()

    for dataset in datasets:
        if not isinstance(dataset, dict):
            continue
        dataset_title = str(dataset.get("title") or "")
        if not dataset_title:
            continue
        if _is_ffs_bundle_dataset_title(dataset_title):
            continue

        spec = _match_spec(dataset_title)
        if spec is None:
            if _looks_like_provider_enrollment_title(dataset_title):
                unmapped.add(dataset_title)
            continue

        csv_distributions = _collect_csv_distributions(dataset)
        if not csv_distributions and STRICT_SOURCE_PRESENCE:
            raise RuntimeError(
                f"Registered provider-enrichment dataset has no CSV distributions: {dataset_title}"
            )

        limit: int | None = None
        if test_mode:
            limit = _env_optional_limit(
                "HLTHPRT_PROVIDER_ENRICHMENT_TEST_MAX_SOURCES_PER_DATASET",
                TEST_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET,
            )
        elif not INCLUDE_PROVIDER_ENRICHMENT_HISTORY:
            limit = _env_optional_limit(
                "HLTHPRT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET",
                DEFAULT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET,
            )

        distributions_to_use = csv_distributions if limit is None else csv_distributions[:limit]
        for dist in distributions_to_use:
            url = str(dist.get("downloadURL") or "").strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)

            period_start, period_end = _extract_period_bounds(dist.get("temporal"))
            reporting_year = (
                (period_start.year if period_start else None)
                or _extract_year(dist.get("title"), dataset_title, dist.get("modified"))
                or datetime.datetime.utcnow().year
            )

            discovered.append(
                {
                    "spec_key": spec["key"],
                    "dataset_title": dataset_title,
                    "distribution_title": dist.get("title") or dataset_title,
                    "download_url": url,
                    "source_modified": _safe_datetime(dist.get("modified")),
                    "source_temporal": _safe_text(dist.get("temporal")),
                    "reporting_period_start": period_start,
                    "reporting_period_end": period_end,
                    "reporting_year": reporting_year,
                }
            )

    if STRICT_SOURCE_PRESENCE:
        for spec in CATALOG_DISCOVERY_SPECS:
            if not any(src.get("spec_key") == spec["key"] for src in discovered):
                raise RuntimeError(
                    f"No sources discovered for registered dataset '{spec['label']}' ({spec['key']})."
                )

    discovered.sort(
        key=lambda src: (
            str(src.get("spec_key") or ""),
            src.get("reporting_year") or 0,
            src.get("source_modified") or datetime.datetime.min,
        ),
        reverse=True,
    )
    return discovered, sorted(unmapped)


def _validate_headers(headers: list[str], spec: dict[str, Any], source_name: str) -> None:
    header_set = set(headers)
    missing: list[str] = []
    for field in spec["fields"]:
        if not field.get("required"):
            continue
        aliases = field.get("aliases") or ()
        if not any(alias in header_set for alias in aliases):
            missing.append(field["name"])
    if missing:
        raise RuntimeError(
            f"Schema mismatch for {source_name}: missing required mapped fields: {', '.join(sorted(missing))}"
        )


def _is_decodable(file_path: str, encoding: str) -> bool:
    decoder_cls = codecs.getincrementaldecoder(encoding)
    decoder = decoder_cls()
    try:
        with open(file_path, "rb") as handle:
            while True:
                chunk = handle.read(CSV_PROBE_CHUNK_SIZE)
                if not chunk:
                    break
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
        return True
    except UnicodeDecodeError:
        return False


def _select_csv_encoding(file_path: str) -> str:
    if _is_decodable(file_path, CSV_PRIMARY_ENCODING):
        return CSV_PRIMARY_ENCODING
    return CSV_FALLBACK_ENCODING


def _read_csv_header(file_path: str, encoding: str) -> list[str]:
    with open(file_path, "r", encoding=encoding, newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
    return [str(value) for value in header]


def _model_columns(cls: type) -> set[str]:
    return {column.name for column in cls.__table__.columns}


def _base_source_payload(source: dict[str, Any]) -> dict[str, Any]:
    return {
        "reporting_period_start": source.get("reporting_period_start"),
        "reporting_period_end": source.get("reporting_period_end"),
        "reporting_year": source.get("reporting_year"),
        "source_dataset_title": _safe_text(source.get("dataset_title")),
        "source_distribution_title": _safe_text(source.get("distribution_title")),
        "source_url": _safe_text(source.get("download_url")),
        "source_modified": source.get("source_modified"),
        "source_temporal": _safe_text(source.get("source_temporal")),
        "imported_at": datetime.datetime.utcnow(),
    }


def _build_enrollment_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    canonical: dict[str, Any] = {}
    for field in spec["fields"]:
        canonical[field["name"]] = _resolve_header(row, tuple(field["aliases"]))

    npi = _safe_int(canonical.get("npi"))
    if not npi:
        return None, "missing_npi"

    payload: dict[str, Any] = {
        "npi": int(npi),
        **_base_source_payload(source),
        "enrollment_id": _safe_text(canonical.get("enrollment_id")),
        "enrollment_state": _safe_state(canonical.get("enrollment_state")),
        "provider_type_code": _safe_text(canonical.get("provider_type_code")),
        "provider_type_text": _safe_text(canonical.get("provider_type_text")),
        "multiple_npi_flag": _safe_text(canonical.get("multiple_npi_flag")),
        "ccn": _safe_text(canonical.get("ccn")),
        "associate_id": _safe_text(canonical.get("associate_id")),
        "organization_name": _safe_text(canonical.get("organization_name")),
        "doing_business_as_name": _safe_text(canonical.get("doing_business_as_name")),
        "incorporation_date": _safe_date(canonical.get("incorporation_date")),
        "incorporation_state": _safe_state(canonical.get("incorporation_state")),
        "organization_type_structure": _safe_text(canonical.get("organization_type_structure")),
        "organization_other_type_text": _safe_text(canonical.get("organization_other_type_text")),
        "proprietary_nonprofit": _safe_text(canonical.get("proprietary_nonprofit")),
        "address_line_1": _safe_text(canonical.get("address_line_1")),
        "address_line_2": _safe_text(canonical.get("address_line_2")),
        "city": _safe_text(canonical.get("city")),
        "state": _safe_state(canonical.get("state")),
        "zip_code": _safe_zip(canonical.get("zip_code")),
    }

    if "pecos_asct_cntl_id" in model_columns:
        payload["pecos_asct_cntl_id"] = _safe_text(canonical.get("pecos_asct_cntl_id"))
    if "first_name" in model_columns:
        payload["first_name"] = _safe_text(canonical.get("first_name"))
    if "middle_name" in model_columns:
        payload["middle_name"] = _safe_text(canonical.get("middle_name"))
    if "last_name" in model_columns:
        payload["last_name"] = _safe_text(canonical.get("last_name"))
    if "org_name" in model_columns:
        payload["org_name"] = _safe_text(canonical.get("org_name"))

    if "practice_location_type" in model_columns:
        payload["practice_location_type"] = _safe_text(canonical.get("practice_location_type"))
    if "location_other_type_text" in model_columns:
        payload["location_other_type_text"] = _safe_text(canonical.get("location_other_type_text"))

    if "subgroup_general" in model_columns:
        payload["subgroup_general"] = _safe_text(canonical.get("subgroup_general"))
        payload["subgroup_acute_care"] = _safe_text(canonical.get("subgroup_acute_care"))
        payload["subgroup_alcohol_drug"] = _safe_text(canonical.get("subgroup_alcohol_drug"))
        payload["subgroup_childrens"] = _safe_text(canonical.get("subgroup_childrens"))
        payload["subgroup_long_term"] = _safe_text(canonical.get("subgroup_long_term"))
        payload["subgroup_psychiatric"] = _safe_text(canonical.get("subgroup_psychiatric"))
        payload["subgroup_rehabilitation"] = _safe_text(canonical.get("subgroup_rehabilitation"))
        payload["subgroup_short_term"] = _safe_text(canonical.get("subgroup_short_term"))
        payload["subgroup_swing_bed_approved"] = _safe_text(canonical.get("subgroup_swing_bed_approved"))
        payload["subgroup_psychiatric_unit"] = _safe_text(canonical.get("subgroup_psychiatric_unit"))
        payload["subgroup_rehabilitation_unit"] = _safe_text(canonical.get("subgroup_rehabilitation_unit"))
        payload["subgroup_specialty_hospital"] = _safe_text(canonical.get("subgroup_specialty_hospital"))
        payload["subgroup_other"] = _safe_text(canonical.get("subgroup_other"))
        payload["subgroup_other_text"] = _safe_text(canonical.get("subgroup_other_text"))
        payload["reh_conversion_flag"] = _safe_text(canonical.get("reh_conversion_flag"))
        payload["reh_conversion_date"] = _safe_date(canonical.get("reh_conversion_date"))
        payload["cah_or_hospital_ccn"] = _safe_text(canonical.get("cah_or_hospital_ccn"))

    if "telephone_number" in model_columns:
        payload["telephone_number"] = _safe_text(canonical.get("telephone_number"))

    if "nursing_home_provider_name" in model_columns:
        payload["nursing_home_provider_name"] = _safe_text(canonical.get("nursing_home_provider_name"))
        payload["affiliation_entity_name"] = _safe_text(canonical.get("affiliation_entity_name"))
        payload["affiliation_entity_id"] = _safe_text(canonical.get("affiliation_entity_id"))

    payload = {key: value for key, value in payload.items() if key in model_columns}

    checksum_fields = [
        spec["key"],
        payload.get("npi"),
        payload.get("enrollment_id"),
        payload.get("ccn"),
        payload.get("associate_id"),
        payload.get("address_line_1"),
        payload.get("zip_code"),
        payload.get("reporting_period_start"),
        payload.get("source_distribution_title"),
    ]
    payload["record_hash"] = return_checksum(checksum_fields)

    return payload, None


def _build_ffs_additional_npi_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    enrollment_id = _safe_text(_resolve_header(row, ("ENRLMT_ID", "ENROLLMENT ID")))
    additional_npi = _safe_int(_resolve_header(row, ("NPI",)))
    if not enrollment_id:
        return None, "missing_enrollment_id"
    if not additional_npi:
        return None, "missing_npi"

    payload = {
        **_base_source_payload(source),
        "enrollment_id": enrollment_id,
        "additional_npi": int(additional_npi),
    }
    payload = {key: value for key, value in payload.items() if key in model_columns}
    payload["record_hash"] = return_checksum(
        [spec["key"], payload.get("enrollment_id"), payload.get("additional_npi"), payload.get("reporting_year")]
    )
    return payload, None


def _build_ffs_address_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    enrollment_id = _safe_text(_resolve_header(row, ("ENRLMT_ID", "ENROLLMENT ID")))
    if not enrollment_id:
        return None, "missing_enrollment_id"

    payload = {
        **_base_source_payload(source),
        "enrollment_id": enrollment_id,
        "city": _safe_text(_resolve_header(row, ("CITY_NAME", "CITY"))),
        "state": _safe_state(_resolve_header(row, ("STATE_CD", "STATE"))),
        "zip_code": _safe_zip(_resolve_header(row, ("ZIP_CD", "ZIP CODE"))),
    }
    if not payload.get("zip_code"):
        return None, "missing_zip_code"
    payload = {key: value for key, value in payload.items() if key in model_columns}
    payload["record_hash"] = return_checksum(
        [
            spec["key"],
            payload.get("enrollment_id"),
            payload.get("city"),
            payload.get("state"),
            payload.get("zip_code"),
            payload.get("reporting_year"),
        ]
    )
    return payload, None


def _build_ffs_secondary_specialty_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    enrollment_id = _safe_text(_resolve_header(row, ("ENRLMT_ID", "ENROLLMENT ID")))
    provider_type_code = _safe_text(_resolve_header(row, ("PROVIDER_TYPE_CD",)))
    if not enrollment_id:
        return None, "missing_enrollment_id"
    if not provider_type_code:
        return None, "missing_provider_type_code"

    payload = {
        **_base_source_payload(source),
        "enrollment_id": enrollment_id,
        "provider_type_code": provider_type_code,
        "provider_type_text": _safe_text(_resolve_header(row, ("PROVIDER_TYPE_DESC",))),
    }
    payload = {key: value for key, value in payload.items() if key in model_columns}
    payload["record_hash"] = return_checksum(
        [
            spec["key"],
            payload.get("enrollment_id"),
            payload.get("provider_type_code"),
            payload.get("provider_type_text"),
            payload.get("reporting_year"),
        ]
    )
    return payload, None


def _build_ffs_reassignment_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    reassigning_enrollment_id = _safe_text(_resolve_header(row, ("REASGN_BNFT_ENRLMT_ID",)))
    receiving_enrollment_id = _safe_text(_resolve_header(row, ("RCV_BNFT_ENRLMT_ID",)))
    if not reassigning_enrollment_id:
        return None, "missing_reassigning_enrollment_id"
    if not receiving_enrollment_id:
        return None, "missing_receiving_enrollment_id"

    payload = {
        **_base_source_payload(source),
        "reassigning_enrollment_id": reassigning_enrollment_id,
        "receiving_enrollment_id": receiving_enrollment_id,
    }
    payload = {key: value for key, value in payload.items() if key in model_columns}
    payload["record_hash"] = return_checksum(
        [
            spec["key"],
            payload.get("reassigning_enrollment_id"),
            payload.get("receiving_enrollment_id"),
            payload.get("reporting_year"),
        ]
    )
    return payload, None


PAYLOAD_BUILDERS = {
    "ffs_additional_npi": _build_ffs_additional_npi_row_payload,
    "ffs_address": _build_ffs_address_row_payload,
    "ffs_reassignment": _build_ffs_reassignment_row_payload,
    "ffs_secondary_specialty": _build_ffs_secondary_specialty_row_payload,
}


def _build_row_payload(
    row: dict[str, Any],
    spec: dict[str, Any],
    source: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    builder_name = _safe_text(spec.get("payload_builder"))
    if builder_name:
        builder = PAYLOAD_BUILDERS[builder_name]
        return builder(row, spec, source, model_columns)
    return _build_enrollment_row_payload(row, spec, source, model_columns)


async def _download_source(url: str, target_path: str) -> None:
    await download_it_and_save(
        url,
        target_path,
        chunk_size=SOURCE_DOWNLOAD_CHUNK_SIZE,
        cache_dir="/tmp",
    )


async def _prepare_staging_tables(import_date: str, db_schema: str) -> None:
    tables = {}

    for cls in PROCESSING_CLASSES:
        tables[cls.__main_table__] = make_class(cls, import_date)
        obj = tables[cls.__main_table__]
        try:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
            await db.create_table(obj.__table__, checkfirst=True)
            if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
                await db.status(
                    f"CREATE UNIQUE INDEX {obj.__tablename__}_idx_primary "
                    f"ON {db_schema}.{obj.__tablename__} ({', '.join(obj.__my_index_elements__)});"
                )

            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                for index in cls.__my_initial_indexes__:
                    index_name = index.get("name", "_".join(index.get("index_elements")))
                    using = f"USING {index.get('using')} " if index.get("using") else ""
                    unique = " UNIQUE " if index.get("unique") else " "
                    where = f" WHERE {index.get('where')} " if index.get("where") else ""
                    create_index_sql = (
                        f"CREATE{unique}INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                        f"ON {db_schema}.{obj.__tablename__} {using}"
                        f"({', '.join(index.get('index_elements'))}){where};"
                    )
                    print(create_index_sql)
                    await db.status(create_index_sql)
        except DuplicateTableError:
            continue

    print(f"Preparing provider-enrichment staging tables done for schema={db_schema} import_date={import_date}")


async def _run_nppes_gap_check(ctx: dict[str, Any]) -> dict[str, Any]:
    report = {
        "checked": False,
        "source_zip": None,
        "unmapped_fields": [],
        "unmapped_field_count": 0,
        "medical_school_headers": [],
        "error": None,
    }

    base_url = os.getenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR")
    listing_file = os.getenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE")
    if not base_url or not listing_file:
        report["error"] = "HLTHPRT_NPPES_DOWNLOAD_URL_DIR/FILE are not configured"
        return report

    try:
        html_source = await download_it(f"{base_url}{listing_file}")
        zip_candidates = re.findall(r"(NPPES_Data_Dissemination.*?_V2\.zip)", html_source)
        if not zip_candidates:
            report["error"] = "no NPPES dissemination zip links found"
            return report

        zip_name = zip_candidates[0]
        report["source_zip"] = zip_name

        with tempfile.TemporaryDirectory() as tmpdirname:
            zip_path = str(PurePath(tmpdirname, zip_name))
            await _download_source(f"{base_url}{zip_name}", zip_path)
            with zipfile.ZipFile(zip_path, "r") as archive:
                npi_csv_name = None
                for name in archive.namelist():
                    lower = name.lower()
                    if "npidata_pfile" in lower and lower.endswith(".csv"):
                        npi_csv_name = name
                        break
                if not npi_csv_name:
                    report["error"] = "npidata_pfile*.csv was not found in the NPPES zip"
                    return report

                with archive.open(npi_csv_name, "r") as header_file:
                    line = header_file.readline().decode("utf-8-sig", errors="ignore")
                headers = next(csv.reader([line])) if line else []

        int_key_re = re.compile(r".*_\d+$")
        mapped_headers = set()
        for header in headers:
            if int_key_re.match(header) or " Address" in header:
                continue
            mapped_headers.add(_normalize_nppes_header(header))

        mapped_to_npi = {column.name for column in NPIData.__table__.columns}
        ignored = {
            "do_business_as_text",
        }
        unmapped = sorted(
            field for field in mapped_headers if field and field not in mapped_to_npi and field not in ignored
        )
        medical_school = sorted(
            header for header in headers if "medical" in str(header).lower() and "school" in str(header).lower()
        )

        report["checked"] = True
        report["unmapped_fields"] = unmapped
        report["unmapped_field_count"] = len(unmapped)
        report["medical_school_headers"] = medical_school

        if unmapped:
            print(
                "[warn] NPPES unmapped normalized fields detected "
                f"({len(unmapped)}): {', '.join(unmapped)}"
            )
        if medical_school:
            print(
                "[warn] NPPES medical-school-like headers detected: "
                f"{', '.join(medical_school)}"
            )

        ctx.setdefault("context", {}).setdefault("audit", {})["nppes_gap_report"] = report
        return report
    except Exception as exc:  # pylint: disable=broad-exception-caught
        report["error"] = str(exc)
        print(f"[warn] NPPES gap check failed: {exc}")
        ctx.setdefault("context", {}).setdefault("audit", {})["nppes_gap_report"] = report
        return report


async def process_data(ctx, task=None):  # pragma: no cover
    task = task or {}
    ctx.setdefault("context", {})
    if "test_mode" in task:
        ctx["context"]["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(ctx["context"].get("test_mode", False))

    await ensure_database(test_mode)
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    await _prepare_staging_tables(ctx["import_date"], db_schema)

    audit = ctx["context"].setdefault(
        "audit",
        {
            "dataset_stats": {},
            "unmapped_datasets": [],
            "rows_accepted": 0,
            "rows_dropped_missing_npi": 0,
        },
    )

    if ENABLE_NPPES_GAP_CHECK:
        await _run_nppes_gap_check(ctx)
    else:
        ctx.setdefault("context", {}).setdefault("audit", {})["nppes_gap_report"] = {
            "checked": False,
            "skipped": True,
            "reason": "disabled_by_config",
            "source_zip": None,
            "unmapped_fields": [],
            "unmapped_field_count": 0,
            "medical_school_headers": [],
            "error": None,
        }

    sources, unmapped = await _discover_sources(test_mode=test_mode)
    audit["unmapped_datasets"] = unmapped

    if not sources:
        raise RuntimeError("No registered provider-enrichment sources were discovered from the CMS catalog.")

    print(f"Provider enrichment discovery: sources={len(sources)} unmapped={len(unmapped)}")

    batch_size = _env_positive_int(
        "HLTHPRT_PROVIDER_ENRICHMENT_BATCH_SIZE",
        DEFAULT_PROVIDER_ENRICHMENT_BATCH_SIZE,
    )
    max_pending_save_tasks = _env_positive_int(
        "HLTHPRT_PROVIDER_ENRICHMENT_MAX_PENDING_SAVE_TASKS",
        DEFAULT_MAX_PENDING_SAVE_TASKS,
    )

    async def enqueue_or_flush(coros: list, coro) -> None:
        coros.append(asyncio.create_task(coro))
        if len(coros) >= max_pending_save_tasks:
            await asyncio.gather(*coros)
            coros.clear()

    with tempfile.TemporaryDirectory() as tmpdirname:
        for source_idx, source in enumerate(sources):
            spec_key = source["spec_key"]
            spec = SPEC_BY_KEY[spec_key]
            model_cls = spec["model"]
            task_key = spec["task_key"]
            model_columns = _model_columns(model_cls)

            local_path = str(PurePath(tmpdirname, f"provider_enrichment_{spec_key}_{source_idx}.csv"))
            print(
                "Downloading provider-enrichment source "
                f"[{source_idx + 1}/{len(sources)}] {spec_key} {source.get('distribution_title')}"
            )
            await _download_source(str(source["download_url"]), local_path)

            csv_encoding = _select_csv_encoding(local_path)
            headers = _read_csv_header(local_path, csv_encoding)
            _validate_headers(headers, spec, str(source.get("distribution_title") or source.get("dataset_title")))
            if csv_encoding != CSV_PRIMARY_ENCODING:
                print(
                    "Provider-enrichment source using fallback CSV encoding "
                    f"'{csv_encoding}': {source.get('distribution_title')}"
                )

            rows_accepted = 0
            rows_dropped_missing_npi = 0
            processed_rows = 0
            payload_rows: list[dict[str, Any]] = []
            save_tasks: list[asyncio.Task] = []

            async with async_open(local_path, "r", encoding=csv_encoding) as handle:
                reader = AsyncDictReader(handle, delimiter=",")
                async for row in reader:
                    processed_rows += 1
                    obj, drop_reason = _build_row_payload(row, spec, source, model_columns)
                    if obj is None:
                        if drop_reason == "missing_npi":
                            rows_dropped_missing_npi += 1
                        continue

                    payload_rows.append(obj)
                    rows_accepted += 1

                    if len(payload_rows) >= batch_size:
                        await enqueue_or_flush(save_tasks, save_provider_enrichment_data(ctx, {task_key: payload_rows.copy()}))
                        payload_rows.clear()

                    if test_mode and rows_accepted >= TEST_PROVIDER_ENRICHMENT_ROWS:
                        break

            if payload_rows:
                await enqueue_or_flush(save_tasks, save_provider_enrichment_data(ctx, {task_key: payload_rows.copy()}))
                payload_rows.clear()
            if save_tasks:
                await asyncio.gather(*save_tasks)

            dataset_stats = audit["dataset_stats"].setdefault(spec_key, [])
            dataset_stats.append(
                {
                    "dataset_title": source.get("dataset_title"),
                    "distribution_title": source.get("distribution_title"),
                    "download_url": source.get("download_url"),
                    "reporting_year": source.get("reporting_year"),
                    "rows_processed": processed_rows,
                    "rows_accepted": rows_accepted,
                    "rows_dropped_missing_npi": rows_dropped_missing_npi,
                }
            )
            audit["rows_accepted"] += rows_accepted
            audit["rows_dropped_missing_npi"] += rows_dropped_missing_npi
            print(
                f"Provider-enrichment source done: spec={spec_key} processed={processed_rows:,} "
                f"accepted={rows_accepted:,} dropped_missing_npi={rows_dropped_missing_npi:,}"
            )

    ctx["context"]["run"] = ctx["context"].get("run", 0) + 1


async def startup(ctx):  # pragma: no cover
    await my_init_db(db)
    ctx["context"] = {}
    ctx["context"]["start"] = datetime.datetime.utcnow()
    ctx["context"]["run"] = 0
    ctx["context"]["test_mode"] = False
    await ensure_database(False)

    override_import_id = os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    ctx["import_date"] = _normalize_import_id(override_import_id)
    import_date = ctx["import_date"]

    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    print(f"Provider-enrichment startup ready for schema={db_schema} import_date={import_date}")


async def _materialize_summary(import_date: str, db_schema: str, nppes_report: dict[str, Any]) -> None:
    table_map = {
        cls.__main_table__: make_class(cls, import_date)
        for cls in PROCESSING_CLASSES
    }

    summary_stage = table_map[ProviderEnrichmentSummary.__main_table__]
    await db.status(f"TRUNCATE TABLE {db_schema}.{summary_stage.__tablename__};")
    ffs_table = table_map[ProviderEnrollmentFFS.__main_table__].__tablename__
    ffs_additional_npi_table = table_map[ProviderEnrollmentFFSAdditionalNPI.__main_table__].__tablename__
    ffs_address_table = table_map[ProviderEnrollmentFFSAddress.__main_table__].__tablename__
    ffs_secondary_specialty_table = table_map[ProviderEnrollmentFFSSecondarySpecialty.__main_table__].__tablename__
    ffs_reassignment_table = table_map[ProviderEnrollmentFFSReassignment.__main_table__].__tablename__

    union_parts = []
    for spec in ENROLLMENT_DATASET_SPECS:
        if spec["key"] in {"ffs_additional_npi", "ffs_reassignment", "ffs_address", "ffs_secondary_specialty"}:
            continue
        table_name = table_map[spec["model"].__main_table__].__tablename__
        union_parts.append(
            f"""
            SELECT
                npi::bigint AS npi,
                reporting_year::int AS reporting_year,
                NULLIF(state, '')::varchar AS state,
                NULLIF(provider_type_code, '')::varchar AS provider_type_code,
                NULLIF(provider_type_text, '')::varchar AS provider_type_text,
                '{spec['key']}'::varchar AS dataset_key
              FROM {db_schema}.{table_name}
             WHERE npi IS NOT NULL
            """
        )

    pricing_cte = """
        SELECT NULL::bigint AS npi,
               NULL::int AS medicare_claim_year_min,
               NULL::int AS medicare_claim_year_max,
               NULL::int AS medicare_claim_rows
         WHERE FALSE
    """
    if await _table_exists(db_schema, PricingProvider.__tablename__):
        pricing_cte = f"""
            SELECT
                npi::bigint AS npi,
                MIN(year)::int AS medicare_claim_year_min,
                MAX(year)::int AS medicare_claim_year_max,
                COUNT(*)::int AS medicare_claim_rows
              FROM {db_schema}.{PricingProvider.__tablename__}
             GROUP BY npi
        """

    nppes_unmapped_count = int(nppes_report.get("unmapped_field_count") or 0)
    nppes_medical_school_fields = [str(v) for v in (nppes_report.get("medical_school_headers") or [])]
    nppes_unmapped_count_sql = str(nppes_unmapped_count)
    nppes_medical_school_fields_sql = _sql_varchar_array_literal(nppes_medical_school_fields)

    sql = f"""
        INSERT INTO {db_schema}.{summary_stage.__tablename__} (
            npi,
            latest_reporting_year,
            has_any_enrollment,
            has_ffs_enrollment,
            has_hospital_enrollment,
            has_hha_enrollment,
            has_hospice_enrollment,
            has_fqhc_enrollment,
            has_rhc_enrollment,
            has_snf_enrollment,
            has_medicare_claims,
            medicare_claim_year_min,
            medicare_claim_year_max,
            medicare_claim_rows,
            total_enrollment_rows,
            dataset_keys,
            states,
            provider_type_codes,
            provider_type_texts,
            ffs_enrollment_ids,
            ffs_pecos_asct_cntl_ids,
            ffs_secondary_provider_type_codes,
            ffs_secondary_provider_type_texts,
            ffs_practice_zip_codes,
            ffs_practice_cities,
            ffs_practice_states,
            ffs_related_npis,
            ffs_related_npi_count,
            ffs_reassignment_in_count,
            ffs_reassignment_out_count,
            primary_state,
            primary_provider_type_code,
            primary_provider_type_text,
            status,
            nppes_unmapped_field_count,
            nppes_medical_school_fields,
            updated_at
        )
        WITH enrollment_union AS (
            {' UNION ALL '.join(union_parts)}
        ),
        agg AS (
            SELECT
                npi,
                MAX(reporting_year)::int AS latest_reporting_year,
                BOOL_OR(dataset_key = 'ffs_public') AS has_ffs_enrollment,
                BOOL_OR(dataset_key = 'hospital') AS has_hospital_enrollment,
                BOOL_OR(dataset_key = 'hha') AS has_hha_enrollment,
                BOOL_OR(dataset_key = 'hospice') AS has_hospice_enrollment,
                BOOL_OR(dataset_key = 'fqhc') AS has_fqhc_enrollment,
                BOOL_OR(dataset_key = 'rhc') AS has_rhc_enrollment,
                BOOL_OR(dataset_key = 'snf') AS has_snf_enrollment,
                COUNT(*)::int AS total_enrollment_rows,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT dataset_key), NULL)::varchar[] AS dataset_keys,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT state), NULL)::varchar[] AS states,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT provider_type_code), NULL)::varchar[] AS provider_type_codes,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT provider_type_text), NULL)::varchar[] AS provider_type_texts
              FROM enrollment_union
             GROUP BY npi
        ),
        latest_provider AS (
            SELECT DISTINCT ON (npi)
                npi,
                state AS primary_state,
                provider_type_code AS primary_provider_type_code,
                provider_type_text AS primary_provider_type_text
              FROM enrollment_union
             ORDER BY npi, reporting_year DESC NULLS LAST
        ),
        ffs_base AS (
            SELECT
                npi::bigint AS npi,
                enrollment_id::varchar AS enrollment_id,
                NULLIF(pecos_asct_cntl_id, '')::varchar AS pecos_asct_cntl_id
              FROM {db_schema}.{ffs_table}
             WHERE npi IS NOT NULL
               AND enrollment_id IS NOT NULL
        ),
        ffs_rollup AS (
            SELECT
                npi,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT enrollment_id), NULL)::varchar[] AS ffs_enrollment_ids,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT pecos_asct_cntl_id), NULL)::varchar[] AS ffs_pecos_asct_cntl_ids
              FROM ffs_base
             GROUP BY npi
        ),
        ffs_secondary AS (
            SELECT
                f.npi,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.provider_type_code), NULL)::varchar[] AS ffs_secondary_provider_type_codes,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.provider_type_text), NULL)::varchar[] AS ffs_secondary_provider_type_texts
              FROM {db_schema}.{ffs_secondary_specialty_table} AS s
              JOIN ffs_base AS f ON f.enrollment_id = s.enrollment_id
             GROUP BY f.npi
        ),
        ffs_locations AS (
            SELECT
                f.npi,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.zip_code), NULL)::varchar[] AS ffs_practice_zip_codes,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.city), NULL)::varchar[] AS ffs_practice_cities,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT a.state), NULL)::varchar[] AS ffs_practice_states
              FROM {db_schema}.{ffs_address_table} AS a
              JOIN ffs_base AS f ON f.enrollment_id = a.enrollment_id
             GROUP BY f.npi
        ),
        ffs_related AS (
            SELECT
                f.npi,
                ARRAY_REMOVE(
                    ARRAY_AGG(DISTINCT CASE WHEN a.additional_npi IS NOT NULL AND a.additional_npi <> f.npi THEN a.additional_npi END),
                    NULL
                )::bigint[] AS ffs_related_npis
              FROM {db_schema}.{ffs_additional_npi_table} AS a
              JOIN ffs_base AS f ON f.enrollment_id = a.enrollment_id
             GROUP BY f.npi
        ),
        ffs_reassignment_out AS (
            SELECT
                f.npi,
                COUNT(*)::int AS ffs_reassignment_out_count
              FROM {db_schema}.{ffs_reassignment_table} AS r
              JOIN ffs_base AS f ON f.enrollment_id = r.reassigning_enrollment_id
             GROUP BY f.npi
        ),
        ffs_reassignment_in AS (
            SELECT
                f.npi,
                COUNT(*)::int AS ffs_reassignment_in_count
              FROM {db_schema}.{ffs_reassignment_table} AS r
              JOIN ffs_base AS f ON f.enrollment_id = r.receiving_enrollment_id
             GROUP BY f.npi
        ),
        pricing AS (
            {pricing_cte}
        )
        SELECT
            a.npi,
            a.latest_reporting_year,
            TRUE AS has_any_enrollment,
            COALESCE(a.has_ffs_enrollment, FALSE) AS has_ffs_enrollment,
            COALESCE(a.has_hospital_enrollment, FALSE) AS has_hospital_enrollment,
            COALESCE(a.has_hha_enrollment, FALSE) AS has_hha_enrollment,
            COALESCE(a.has_hospice_enrollment, FALSE) AS has_hospice_enrollment,
            COALESCE(a.has_fqhc_enrollment, FALSE) AS has_fqhc_enrollment,
            COALESCE(a.has_rhc_enrollment, FALSE) AS has_rhc_enrollment,
            COALESCE(a.has_snf_enrollment, FALSE) AS has_snf_enrollment,
            COALESCE(p.medicare_claim_rows, 0) > 0 AS has_medicare_claims,
            p.medicare_claim_year_min,
            p.medicare_claim_year_max,
            COALESCE(p.medicare_claim_rows, 0) AS medicare_claim_rows,
            a.total_enrollment_rows,
            COALESCE(a.dataset_keys, ARRAY[]::varchar[]) AS dataset_keys,
            COALESCE(a.states, ARRAY[]::varchar[]) AS states,
            COALESCE(a.provider_type_codes, ARRAY[]::varchar[]) AS provider_type_codes,
            COALESCE(a.provider_type_texts, ARRAY[]::varchar[]) AS provider_type_texts,
            COALESCE(fr.ffs_enrollment_ids, ARRAY[]::varchar[]) AS ffs_enrollment_ids,
            COALESCE(fr.ffs_pecos_asct_cntl_ids, ARRAY[]::varchar[]) AS ffs_pecos_asct_cntl_ids,
            COALESCE(fs.ffs_secondary_provider_type_codes, ARRAY[]::varchar[]) AS ffs_secondary_provider_type_codes,
            COALESCE(fs.ffs_secondary_provider_type_texts, ARRAY[]::varchar[]) AS ffs_secondary_provider_type_texts,
            COALESCE(fl.ffs_practice_zip_codes, ARRAY[]::varchar[]) AS ffs_practice_zip_codes,
            COALESCE(fl.ffs_practice_cities, ARRAY[]::varchar[]) AS ffs_practice_cities,
            COALESCE(fl.ffs_practice_states, ARRAY[]::varchar[]) AS ffs_practice_states,
            COALESCE(frn.ffs_related_npis, ARRAY[]::bigint[]) AS ffs_related_npis,
            COALESCE(CARDINALITY(frn.ffs_related_npis), 0)::int AS ffs_related_npi_count,
            COALESCE(fri.ffs_reassignment_in_count, 0)::int AS ffs_reassignment_in_count,
            COALESCE(fro.ffs_reassignment_out_count, 0)::int AS ffs_reassignment_out_count,
            lp.primary_state,
            lp.primary_provider_type_code,
            lp.primary_provider_type_text,
            CASE
                WHEN COALESCE(p.medicare_claim_rows, 0) > 0 THEN 'enriched'
                ELSE 'enrollment_only'
            END::varchar AS status,
            {nppes_unmapped_count_sql}::int AS nppes_unmapped_field_count,
            {nppes_medical_school_fields_sql} AS nppes_medical_school_fields,
            now()::timestamp AS updated_at
          FROM agg AS a
          LEFT JOIN latest_provider AS lp ON lp.npi = a.npi
          LEFT JOIN ffs_rollup AS fr ON fr.npi = a.npi
          LEFT JOIN ffs_secondary AS fs ON fs.npi = a.npi
          LEFT JOIN ffs_locations AS fl ON fl.npi = a.npi
          LEFT JOIN ffs_related AS frn ON frn.npi = a.npi
          LEFT JOIN ffs_reassignment_in AS fri ON fri.npi = a.npi
          LEFT JOIN ffs_reassignment_out AS fro ON fro.npi = a.npi
          LEFT JOIN pricing AS p ON p.npi = a.npi;
    """

    await db.status(sql)


async def shutdown(ctx):  # pragma: no cover
    import_date = ctx["import_date"]
    context = ctx.get("context") or {}
    if not context.get("run"):
        print("No provider-enrichment jobs ran in this worker session; skipping shutdown validation.")
        return

    await ensure_database(bool(context.get("test_mode")))
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") if os.getenv("HLTHPRT_DB_SCHEMA") else "mrf"

    tables = {}
    processing_classes = PROCESSING_CLASSES

    async def archive_index(index_name: str) -> str:
        archived_name = _archived_identifier(index_name)
        await db.status(f"DROP INDEX IF EXISTS {db_schema}.{archived_name};")
        await db.status(f"ALTER INDEX IF EXISTS {db_schema}.{index_name} RENAME TO {archived_name};")
        return archived_name

    for cls in processing_classes:
        stage_obj = make_class(cls, import_date)
        tables[cls.__main_table__] = stage_obj
        if not await _table_exists(db_schema, stage_obj.__tablename__):
            raise RuntimeError(
                f"Staging table {db_schema}.{stage_obj.__tablename__} is missing; "
                "cannot finalize provider-enrichment publish."
            )

    required_nonempty_tables = (
        ProviderEnrollmentFFS,
        ProviderEnrollmentFFSAdditionalNPI,
        ProviderEnrollmentFFSAddress,
        ProviderEnrollmentFFSSecondarySpecialty,
        ProviderEnrollmentFFSReassignment,
    )
    for cls in required_nonempty_tables:
        stage_obj = tables[cls.__main_table__]
        row_count = await db.scalar(f"SELECT COUNT(*) FROM {db_schema}.{stage_obj.__tablename__};")
        print(f"Provider-enrichment staging rows: {stage_obj.__tablename__}={int(row_count or 0):,}")
        if not row_count:
            raise RuntimeError(
                f"Required staging table {db_schema}.{stage_obj.__tablename__} is empty; "
                "aborting provider-enrichment publish."
            )

    await _materialize_summary(
        import_date,
        db_schema,
        context.get("audit", {}).get("nppes_gap_report", {}),
    )

    async with db.transaction():
        for cls in processing_classes:
            obj = tables[cls.__main_table__]
            if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                for index in cls.__my_additional_indexes__:
                    index_name = index.get("name", "_".join(index.get("index_elements")))
                    using = f"USING {index.get('using')} " if index.get("using") else ""
                    where_clause = f" WHERE {index.get('where')}" if index.get("where") else ""
                    create_index_sql = (
                        f"CREATE INDEX IF NOT EXISTS {obj.__tablename__}_idx_{index_name} "
                        f"ON {db_schema}.{obj.__tablename__} {using}"
                        f"({', '.join(index.get('index_elements'))}){where_clause};"
                    )
                    print(create_index_sql)
                    await db.status(create_index_sql)

    async def analyze_table(obj):
        print(f"Post-Index ANALYZE {db_schema}.{obj.__tablename__};")
        await db.execute_ddl(f"ANALYZE {db_schema}.{obj.__tablename__};")

    await asyncio.gather(*(analyze_table(tables[cls.__main_table__]) for cls in processing_classes))

    async with db.transaction():
        for cls in processing_classes:
            obj = tables[cls.__main_table__]
            table = obj.__main_table__
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{table} RENAME TO {table}_old;")
            await db.status(f"ALTER TABLE IF EXISTS {db_schema}.{obj.__tablename__} RENAME TO {table};")

            await archive_index(f"{table}_idx_primary")
            await db.status(
                f"ALTER INDEX IF EXISTS {db_schema}.{obj.__tablename__}_idx_primary RENAME TO {table}_idx_primary;"
            )

            move_indexes = []
            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                move_indexes += cls.__my_initial_indexes__
            if hasattr(cls, "__my_additional_indexes__") and cls.__my_additional_indexes__:
                move_indexes += cls.__my_additional_indexes__

            for index in move_indexes:
                index_name = index.get("name", "_".join(index.get("index_elements")))
                await archive_index(f"{table}_idx_{index_name}")
                await db.status(
                    f"ALTER INDEX IF EXISTS {db_schema}.{obj.__tablename__}_idx_{index_name} "
                    f"RENAME TO {table}_idx_{index_name};"
                )

    print_time_info(context.get("start"))


async def save_provider_enrichment_data(ctx, task):
    import_date = ctx["import_date"]
    test_mode = bool(ctx.get("context", {}).get("test_mode"))
    await ensure_database(test_mode)

    operations = []
    for key, rows in task.items():
        model_cls = TASK_KEY_TO_MODEL.get(key)
        if model_cls is None:
            print(f"[warn] Unknown task key for provider enrichment save: {key}")
            continue
        target_cls = make_class(model_cls, import_date)
        # Provider-enrichment datasets contain many repeated keys; direct upsert avoids
        # COPY duplicate-key exceptions and expensive fallback loops.
        operations.append(push_objects(rows, target_cls, rewrite=True, use_copy=False))

    for op in operations:
        await op


async def main(test_mode: bool = False):  # pragma: no cover
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=PROVIDER_ENRICHMENT_QUEUE_NAME)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main(bool("--test" in sys.argv)))
