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
from dataclasses import dataclass
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
from process.ext.address_canon import resolve_into_archive, source_enabled, stamp_address_keys
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
from process.control_lifecycle import mark_control_run
from process.live_progress import enqueue_live_progress
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
    normalized_values = [str(v) for v in values if str(v).strip()]
    if not normalized_values:
        return "ARRAY[]::varchar[]"
    escaped_values = [
        "'" + value.replace("'", "''") + "'" for value in normalized_values
    ]
    return f"ARRAY[{', '.join(escaped_values)}]::varchar[]"


def _is_csv_distribution(obj: dict[str, Any]) -> bool:
    media_type = _normalize_title(obj.get("mediaType") or "")
    fmt = _normalize_title(obj.get("format") or "")
    return "csv" in media_type or "csv" in fmt


def _is_likely_csv_download(url: str) -> bool:
    normalized = str(url or "").strip().lower()
    return normalized.endswith(".csv")


_looks_like_csv_download = _is_likely_csv_download


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


async def _is_table_available(schema: str, table: str) -> bool:
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


def _is_provider_enrollment_title(title: str) -> bool:
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


@dataclass(slots=True)
class _ProviderEnrichmentDiscovery:
    datasets: list[Any]
    test_mode: bool
    discovered_sources: list[dict[str, Any]]
    unmapped_titles: set[str]
    seen_urls: set[str]


def _ffs_bundle_dataset(datasets: list[Any]) -> dict[str, Any] | None:
    return next(
        (
            dataset
            for dataset in datasets
            if isinstance(dataset, dict)
            and _is_ffs_bundle_dataset_title(
                str(dataset.get("title") or "")
            )
        ),
        None,
    )


def _latest_ffs_resource_distribution(
    ffs_dataset: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None]:
    latest_distribution = None
    for distribution in ffs_dataset.get("distribution") or []:
        if not isinstance(distribution, dict):
            continue
        if (
            _normalize_title(distribution.get("description") or "")
            != FFS_LATEST_DESCRIPTION
        ):
            continue
        latest_distribution = distribution
        resources_api = str(
            distribution.get("resourcesAPI") or ""
        ).strip()
        if resources_api:
            return resources_api, distribution
    return None, latest_distribution


def _append_ffs_bundle_resources(
    discovery: _ProviderEnrichmentDiscovery,
    ffs_dataset: dict[str, Any],
    latest_distribution: dict[str, Any],
    resources: list[Any],
) -> set[str]:
    resource_modified = _safe_datetime(latest_distribution.get("modified"))
    resource_temporal = _safe_text(latest_distribution.get("temporal"))
    period_start, period_end = _extract_period_bounds(resource_temporal)
    bundle_hits = set()
    for resource_map in resources:
        if not isinstance(resource_map, dict):
            continue
        resource_name = str(resource_map.get("name") or "").strip()
        download_url = str(
            resource_map.get("downloadURL") or ""
        ).strip()
        if not resource_name or not download_url:
            continue
        if "historical" in _normalize_title(resource_name):
            continue
        if not _looks_like_csv_download(download_url):
            continue
        spec = _match_ffs_resource_spec(resource_name)
        if spec is None or download_url in discovery.seen_urls:
            continue
        reporting_year = _extract_year(
            resource_name,
            latest_distribution.get("title"),
            latest_distribution.get("modified"),
        ) or datetime.datetime.utcnow().year
        discovery.discovered_sources.append(
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
        discovery.seen_urls.add(download_url)
        bundle_hits.add(spec["key"])
    return bundle_hits


def _assert_ffs_bundle_complete(bundle_hits: set[str]) -> None:
    if not STRICT_SOURCE_PRESENCE:
        return
    missing = sorted(
        spec["key"]
        for spec in FFS_RESOURCE_BUNDLE_SPECS
        if spec["key"] not in bundle_hits
    )
    if missing:
        raise RuntimeError(
            "FFS provider-enrollment resource bundle is missing required "
            "CSV files: " + ", ".join(missing)
        )


async def _discover_ffs_resource_bundle(
    discovery: _ProviderEnrichmentDiscovery,
) -> None:
    ffs_dataset = _ffs_bundle_dataset(discovery.datasets)
    if ffs_dataset is None:
        if STRICT_SOURCE_PRESENCE:
            raise RuntimeError(
                "No FFS provider-enrollment dataset found in CMS catalog."
            )
        return
    resources_api, latest_distribution = (
        _latest_ffs_resource_distribution(ffs_dataset)
    )
    if not resources_api or latest_distribution is None:
        if STRICT_SOURCE_PRESENCE:
            raise RuntimeError(
                "FFS provider-enrollment dataset is missing a latest "
                "resources API."
            )
        return
    resource_payload = await download_it(resources_api)
    resources = json.loads(resource_payload).get("data") or []
    bundle_hits = _append_ffs_bundle_resources(
        discovery,
        ffs_dataset,
        latest_distribution,
        resources,
    )
    _assert_ffs_bundle_complete(bundle_hits)


def _provider_enrichment_distribution_limit(test_mode: bool) -> int | None:
    if test_mode:
        return _env_optional_limit(
            "HLTHPRT_PROVIDER_ENRICHMENT_TEST_MAX_SOURCES_PER_DATASET",
            TEST_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET,
        )
    if INCLUDE_PROVIDER_ENRICHMENT_HISTORY:
        return None
    return _env_optional_limit(
        "HLTHPRT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET",
        DEFAULT_PROVIDER_ENRICHMENT_MAX_SOURCES_PER_DATASET,
    )


def _catalog_distribution_source(
    dataset_title: str,
    distribution: dict[str, Any],
    spec: dict[str, Any],
) -> dict[str, Any]:
    period_start, period_end = _extract_period_bounds(
        distribution.get("temporal")
    )
    reporting_year = (
        (period_start.year if period_start else None)
        or _extract_year(
            distribution.get("title"),
            dataset_title,
            distribution.get("modified"),
        )
        or datetime.datetime.utcnow().year
    )
    return {
        "spec_key": spec["key"],
        "dataset_title": dataset_title,
        "distribution_title": distribution.get("title") or dataset_title,
        "download_url": str(distribution.get("downloadURL") or "").strip(),
        "source_modified": _safe_datetime(distribution.get("modified")),
        "source_temporal": _safe_text(distribution.get("temporal")),
        "reporting_period_start": period_start,
        "reporting_period_end": period_end,
        "reporting_year": reporting_year,
    }


def _discover_catalog_dataset_sources(
    discovery: _ProviderEnrichmentDiscovery,
    dataset_map: dict[str, Any],
) -> None:
    dataset_title = str(dataset_map.get("title") or "")
    if not dataset_title or _is_ffs_bundle_dataset_title(dataset_title):
        return
    spec = _match_spec(dataset_title)
    if spec is None:
        if _is_provider_enrollment_title(dataset_title):
            discovery.unmapped_titles.add(dataset_title)
        return
    csv_distributions = _collect_csv_distributions(dataset_map)
    if not csv_distributions and STRICT_SOURCE_PRESENCE:
        raise RuntimeError(
            "Registered provider-enrichment dataset has no CSV "
            f"distributions: {dataset_title}"
        )
    limit = _provider_enrichment_distribution_limit(discovery.test_mode)
    selected_distributions = (
        csv_distributions
        if limit is None
        else csv_distributions[:limit]
    )
    for distribution in selected_distributions:
        source_map = _catalog_distribution_source(
            dataset_title,
            distribution,
            spec,
        )
        download_url = source_map["download_url"]
        if not download_url or download_url in discovery.seen_urls:
            continue
        discovery.seen_urls.add(download_url)
        discovery.discovered_sources.append(source_map)


def _assert_catalog_sources_present(
    discovered_sources: list[dict[str, Any]],
) -> None:
    if not STRICT_SOURCE_PRESENCE:
        return
    discovered_keys = {
        source_map.get("spec_key") for source_map in discovered_sources
    }
    for spec in CATALOG_DISCOVERY_SPECS:
        if spec["key"] not in discovered_keys:
            raise RuntimeError(
                "No sources discovered for registered dataset "
                f"'{spec['label']}' ({spec['key']})."
            )


async def _discover_sources(
    test_mode: bool,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Discover registered CMS enrollment datasets and unmapped catalog titles."""
    catalog = json.loads(await download_it(CATALOG_URL))
    discovery = _ProviderEnrichmentDiscovery(
        datasets=catalog.get("dataset") or [],
        test_mode=test_mode,
        discovered_sources=[],
        unmapped_titles=set(),
        seen_urls=set(),
    )
    await _discover_ffs_resource_bundle(discovery)
    for dataset_map in discovery.datasets:
        if isinstance(dataset_map, dict):
            _discover_catalog_dataset_sources(discovery, dataset_map)
    _assert_catalog_sources_present(discovery.discovered_sources)
    discovery.discovered_sources.sort(
        key=lambda source_map: (
            str(source_map.get("spec_key") or ""),
            source_map.get("reporting_year") or 0,
            source_map.get("source_modified") or datetime.datetime.min,
        ),
        reverse=True,
    )
    return discovery.discovered_sources, sorted(discovery.unmapped_titles)


def _validate_headers(headers: list[str], spec: dict[str, Any], source_name: str) -> None:
    header_set = set(headers)
    missing_headers: list[str] = []
    for field in spec["fields"]:
        if not field.get("required"):
            continue
        aliases = field.get("aliases") or ()
        if not any(alias in header_set for alias in aliases):
            missing_headers.append(field["name"])
    if missing_headers:
        raise RuntimeError(
            f"Schema mismatch for {source_name}: missing required mapped fields: "
            f"{', '.join(sorted(missing_headers))}"
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


_ENROLLMENT_IDENTITY_FIELDS = (
    "pecos_asct_cntl_id",
    "first_name",
    "middle_name",
    "last_name",
    "org_name",
)
_ENROLLMENT_PRACTICE_FIELDS = (
    "practice_location_type",
    "location_other_type_text",
)
_ENROLLMENT_HOSPITAL_FIELDS = (
    "subgroup_general",
    "subgroup_acute_care",
    "subgroup_alcohol_drug",
    "subgroup_childrens",
    "subgroup_long_term",
    "subgroup_psychiatric",
    "subgroup_rehabilitation",
    "subgroup_short_term",
    "subgroup_swing_bed_approved",
    "subgroup_psychiatric_unit",
    "subgroup_rehabilitation_unit",
    "subgroup_specialty_hospital",
    "subgroup_other",
    "subgroup_other_text",
    "reh_conversion_flag",
    "cah_or_hospital_ccn",
)
_ENROLLMENT_NURSING_HOME_FIELDS = (
    "nursing_home_provider_name",
    "affiliation_entity_name",
    "affiliation_entity_id",
)


def _canonical_enrollment_row(
    source_row_map: dict[str, Any],
    spec: dict[str, Any],
) -> dict[str, Any]:
    return {
        field["name"]: _resolve_header(
            source_row_map,
            tuple(field["aliases"]),
        )
        for field in spec["fields"]
    }


def _enrollment_base_row_payload(
    npi: int,
    canonical_row_map: dict[str, Any],
    source_map: dict[str, Any],
) -> dict[str, Any]:
    return {
        "npi": npi,
        **_base_source_payload(source_map),
        "enrollment_id": _safe_text(canonical_row_map.get("enrollment_id")),
        "enrollment_state": _safe_state(
            canonical_row_map.get("enrollment_state")
        ),
        "provider_type_code": _safe_text(
            canonical_row_map.get("provider_type_code")
        ),
        "provider_type_text": _safe_text(
            canonical_row_map.get("provider_type_text")
        ),
        "multiple_npi_flag": _safe_text(
            canonical_row_map.get("multiple_npi_flag")
        ),
        "ccn": _safe_text(canonical_row_map.get("ccn")),
        "associate_id": _safe_text(canonical_row_map.get("associate_id")),
        "organization_name": _safe_text(
            canonical_row_map.get("organization_name")
        ),
        "doing_business_as_name": _safe_text(
            canonical_row_map.get("doing_business_as_name")
        ),
        "incorporation_date": _safe_date(
            canonical_row_map.get("incorporation_date")
        ),
        "incorporation_state": _safe_state(
            canonical_row_map.get("incorporation_state")
        ),
        "organization_type_structure": _safe_text(
            canonical_row_map.get("organization_type_structure")
        ),
        "organization_other_type_text": _safe_text(
            canonical_row_map.get("organization_other_type_text")
        ),
        "proprietary_nonprofit": _safe_text(
            canonical_row_map.get("proprietary_nonprofit")
        ),
        "address_line_1": _safe_text(canonical_row_map.get("address_line_1")),
        "address_line_2": _safe_text(canonical_row_map.get("address_line_2")),
        "city": _safe_text(canonical_row_map.get("city")),
        "state": _safe_state(canonical_row_map.get("state")),
        "zip_code": _safe_zip(canonical_row_map.get("zip_code")),
    }


def _add_enrollment_text_fields(
    row_payload_map: dict[str, Any],
    canonical_row_map: dict[str, Any],
    model_columns: set[str],
    field_names: tuple[str, ...],
) -> None:
    for field_name in field_names:
        if field_name in model_columns:
            row_payload_map[field_name] = _safe_text(
                canonical_row_map.get(field_name)
            )


def _add_hospital_enrollment_fields(
    row_payload_map: dict[str, Any],
    canonical_row_map: dict[str, Any],
    model_columns: set[str],
) -> None:
    if "subgroup_general" not in model_columns:
        return
    for field_name in _ENROLLMENT_HOSPITAL_FIELDS:
        row_payload_map[field_name] = _safe_text(
            canonical_row_map.get(field_name)
        )
    row_payload_map["reh_conversion_date"] = _safe_date(
        canonical_row_map.get("reh_conversion_date")
    )


def _add_nursing_home_enrollment_fields(
    row_payload_map: dict[str, Any],
    canonical_row_map: dict[str, Any],
    model_columns: set[str],
) -> None:
    if "nursing_home_provider_name" not in model_columns:
        return
    for field_name in _ENROLLMENT_NURSING_HOME_FIELDS:
        row_payload_map[field_name] = _safe_text(
            canonical_row_map.get(field_name)
        )


def _enrollment_record_hash(
    spec: dict[str, Any],
    row_payload_map: dict[str, Any],
) -> int:
    checksum_fields = [
        spec["key"],
        row_payload_map.get("npi"),
        row_payload_map.get("enrollment_id"),
        row_payload_map.get("ccn"),
        row_payload_map.get("associate_id"),
        row_payload_map.get("address_line_1"),
        row_payload_map.get("zip_code"),
        row_payload_map.get("reporting_period_start"),
        row_payload_map.get("source_distribution_title"),
    ]
    return return_checksum(checksum_fields)


def _build_enrollment_row_payload(
    source_row_map: dict[str, Any],
    spec: dict[str, Any],
    source_map: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    """Normalize one enrollment source row for its destination model."""
    canonical_row_map = _canonical_enrollment_row(source_row_map, spec)
    npi = _safe_int(canonical_row_map.get("npi"))
    if not npi:
        return None, "missing_npi"
    row_payload_map = _enrollment_base_row_payload(
        int(npi), canonical_row_map, source_map
    )
    _add_enrollment_text_fields(
        row_payload_map,
        canonical_row_map,
        model_columns,
        _ENROLLMENT_IDENTITY_FIELDS,
    )
    _add_enrollment_text_fields(
        row_payload_map,
        canonical_row_map,
        model_columns,
        _ENROLLMENT_PRACTICE_FIELDS,
    )
    _add_hospital_enrollment_fields(
        row_payload_map, canonical_row_map, model_columns
    )
    _add_enrollment_text_fields(
        row_payload_map,
        canonical_row_map,
        model_columns,
        ("telephone_number",),
    )
    _add_nursing_home_enrollment_fields(
        row_payload_map, canonical_row_map, model_columns
    )
    row_payload_map = {
        key: field_value
        for key, field_value in row_payload_map.items()
        if key in model_columns
    }
    row_payload_map["record_hash"] = _enrollment_record_hash(
        spec, row_payload_map
    )
    return row_payload_map, None


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
    source_row_map: dict[str, Any],
    spec: dict[str, Any],
    source_map: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    enrollment_id = _safe_text(
        _resolve_header(source_row_map, ("ENRLMT_ID", "ENROLLMENT ID"))
    )
    if not enrollment_id:
        return None, "missing_enrollment_id"

    row_payload_map = {
        **_base_source_payload(source_map),
        "enrollment_id": enrollment_id,
        "city": _safe_text(_resolve_header(source_row_map, ("CITY_NAME", "CITY"))),
        "state": _safe_state(_resolve_header(source_row_map, ("STATE_CD", "STATE"))),
        "zip_code": _safe_zip(
            _resolve_header(source_row_map, ("ZIP_CD", "ZIP CODE"))
        ),
    }
    if not row_payload_map.get("zip_code"):
        return None, "missing_zip_code"
    row_payload_map = {
        key: field_value
        for key, field_value in row_payload_map.items()
        if key in model_columns
    }
    row_payload_map["record_hash"] = return_checksum(
        [
            spec["key"],
            row_payload_map.get("enrollment_id"),
            row_payload_map.get("city"),
            row_payload_map.get("state"),
            row_payload_map.get("zip_code"),
            row_payload_map.get("reporting_year"),
        ]
    )
    return row_payload_map, None


def _build_ffs_secondary_specialty_row_payload(
    source_row_map: dict[str, Any],
    spec: dict[str, Any],
    source_map: dict[str, Any],
    model_columns: set[str],
) -> tuple[dict[str, Any] | None, str | None]:
    enrollment_id = _safe_text(
        _resolve_header(source_row_map, ("ENRLMT_ID", "ENROLLMENT ID"))
    )
    provider_type_code = _safe_text(
        _resolve_header(source_row_map, ("PROVIDER_TYPE_CD",))
    )
    if not enrollment_id:
        return None, "missing_enrollment_id"
    if not provider_type_code:
        return None, "missing_provider_type_code"

    row_payload_map = {
        **_base_source_payload(source_map),
        "enrollment_id": enrollment_id,
        "provider_type_code": provider_type_code,
        "provider_type_text": _safe_text(
            _resolve_header(source_row_map, ("PROVIDER_TYPE_DESC",))
        ),
    }
    row_payload_map = {
        key: field_value
        for key, field_value in row_payload_map.items()
        if key in model_columns
    }
    row_payload_map["record_hash"] = return_checksum(
        [
            spec["key"],
            row_payload_map.get("enrollment_id"),
            row_payload_map.get("provider_type_code"),
            row_payload_map.get("provider_type_text"),
            row_payload_map.get("reporting_year"),
        ]
    )
    return row_payload_map, None


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
    staging_table_by_name = {}

    for cls in PROCESSING_CLASSES:
        staging_table_by_name[cls.__main_table__] = make_class(cls, import_date)
        staging_model = staging_table_by_name[cls.__main_table__]
        try:
            await db.status(
                f"DROP TABLE IF EXISTS {db_schema}.{staging_model.__tablename__};"
            )
            await db.create_table(staging_model.__table__, checkfirst=True)
            if (
                hasattr(staging_model, "__my_index_elements__")
                and staging_model.__my_index_elements__
            ):
                await db.status(
                    f"CREATE UNIQUE INDEX {staging_model.__tablename__}_idx_primary "
                    f"ON {db_schema}.{staging_model.__tablename__} "
                    f"({', '.join(staging_model.__my_index_elements__)});"
                )

            if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
                for index in cls.__my_initial_indexes__:
                    index_name = index.get("name", "_".join(index.get("index_elements")))
                    using = f"USING {index.get('using')} " if index.get("using") else ""
                    unique = " UNIQUE " if index.get("unique") else " "
                    where = f" WHERE {index.get('where')} " if index.get("where") else ""
                    create_index_sql = (
                        f"CREATE{unique}INDEX IF NOT EXISTS "
                        f"{staging_model.__tablename__}_idx_{index_name} "
                        f"ON {db_schema}.{staging_model.__tablename__} {using}"
                        f"({', '.join(index.get('index_elements'))}){where};"
                    )
                    print(create_index_sql)
                    await db.status(create_index_sql)
        except DuplicateTableError:
            continue

    print(f"Preparing provider-enrichment staging tables done for schema={db_schema} import_date={import_date}")


def _new_nppes_gap_report() -> dict[str, Any]:
    return {
        "checked": False,
        "source_zip": None,
        "unmapped_fields": [],
        "unmapped_field_count": 0,
        "medical_school_headers": [],
        "error": None,
    }


async def _nppes_archive_headers(
    base_url: str,
    zip_name: str,
    gap_report_map: dict[str, Any],
) -> list[str] | None:
    gap_report_map["source_zip"] = zip_name
    with tempfile.TemporaryDirectory() as tmpdirname:
        zip_path = str(PurePath(tmpdirname, zip_name))
        await _download_source(f"{base_url}{zip_name}", zip_path)
        with zipfile.ZipFile(zip_path, "r") as archive:
            npi_csv_name = next(
                (
                    name
                    for name in archive.namelist()
                    if "npidata_pfile" in name.lower()
                    and name.lower().endswith(".csv")
                ),
                None,
            )
            if not npi_csv_name:
                gap_report_map["error"] = (
                    "npidata_pfile*.csv was not found in the NPPES zip"
                )
                return None
            with archive.open(npi_csv_name, "r") as header_file:
                line = header_file.readline().decode(
                    "utf-8-sig", errors="ignore"
                )
    return next(csv.reader([line])) if line else []


async def _current_nppes_headers(
    base_url: str,
    listing_file: str,
    gap_report_map: dict[str, Any],
) -> list[str] | None:
    html_source = await download_it(f"{base_url}{listing_file}")
    zip_candidates = re.findall(
        r"(NPPES_Data_Dissemination.*?_V2\.zip)",
        html_source,
    )
    if not zip_candidates:
        gap_report_map["error"] = "no NPPES dissemination zip links found"
        return None
    return await _nppes_archive_headers(
        base_url,
        zip_candidates[0],
        gap_report_map,
    )


def _unmapped_nppes_fields(headers: list[str]) -> list[str]:
    indexed_key_pattern = re.compile(r".*_\d+$")
    mapped_headers = {
        _normalize_nppes_header(header)
        for header in headers
        if not indexed_key_pattern.match(header) and " Address" not in header
    }
    npi_mapped_headers = {
        column.name for column in NPIData.__table__.columns
    }
    return sorted(
        field
        for field in mapped_headers
        if field
        and field not in npi_mapped_headers
        and field != "do_business_as_text"
    )


def _medical_school_nppes_headers(headers: list[str]) -> list[str]:
    return sorted(
        header
        for header in headers
        if "medical" in str(header).lower()
        and "school" in str(header).lower()
    )


def _warn_for_nppes_gaps(gap_report_map: dict[str, Any]) -> None:
    unmapped = gap_report_map["unmapped_fields"]
    medical_school = gap_report_map["medical_school_headers"]
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


def _record_nppes_gap_report(
    ctx: dict[str, Any],
    gap_report_map: dict[str, Any],
) -> dict[str, Any]:
    ctx.setdefault("context", {}).setdefault("audit", {})[
        "nppes_gap_report"
    ] = gap_report_map
    return gap_report_map


async def _run_nppes_gap_check(ctx: dict[str, Any]) -> dict[str, Any]:
    """Audit current NPPES headers for fields missing from enrichment models."""
    gap_report_map = _new_nppes_gap_report()
    base_url = os.getenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR")
    listing_file = os.getenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE")
    if not base_url or not listing_file:
        gap_report_map["error"] = (
            "HLTHPRT_NPPES_DOWNLOAD_URL_DIR/FILE are not configured"
        )
        return gap_report_map

    try:
        headers = await _current_nppes_headers(
            base_url,
            listing_file,
            gap_report_map,
        )
        if headers is None:
            return gap_report_map
        unmapped = _unmapped_nppes_fields(headers)
        gap_report_map["checked"] = True
        gap_report_map["unmapped_fields"] = unmapped
        gap_report_map["unmapped_field_count"] = len(unmapped)
        gap_report_map["medical_school_headers"] = (
            _medical_school_nppes_headers(headers)
        )
        _warn_for_nppes_gaps(gap_report_map)
    except Exception as exc:
        gap_report_map["error"] = str(exc)
        print(f"[warn] NPPES gap check failed: {exc}")
    return _record_nppes_gap_report(ctx, gap_report_map)


@dataclass(slots=True)
class _ProviderEnrichmentImport:
    ctx: dict[str, Any]
    context: dict[str, Any]
    audit: dict[str, Any]
    run_id: str
    test_mode: bool
    db_schema: str
    sources: list[dict[str, Any]]
    batch_size: int
    max_pending_save_tasks: int


@dataclass(slots=True)
class _ProviderEnrichmentSourceStats:
    processed_rows: int = 0
    rows_accepted: int = 0
    rows_dropped_missing_npi: int = 0


def _provider_enrichment_audit(
    context: dict[str, Any],
) -> dict[str, Any]:
    return context.setdefault(
        "audit",
        {
            "dataset_stats": {},
            "unmapped_datasets": [],
            "rows_accepted": 0,
            "rows_dropped_missing_npi": 0,
        },
    )


async def _audit_nppes_headers(ctx: dict[str, Any]) -> None:
    if ENABLE_NPPES_GAP_CHECK:
        await _run_nppes_gap_check(ctx)
        return
    ctx.setdefault("context", {}).setdefault("audit", {})[
        "nppes_gap_report"
    ] = {
        "checked": False,
        "skipped": True,
        "reason": "disabled_by_config",
        "source_zip": None,
        "unmapped_fields": [],
        "unmapped_field_count": 0,
        "medical_school_headers": [],
        "error": None,
    }


def _report_provider_enrichment_discovery(
    enrichment_import: _ProviderEnrichmentImport,
    unmapped_count: int,
) -> None:
    source_count = len(enrichment_import.sources)
    print(
        "Provider enrichment discovery: "
        f"sources={source_count} unmapped={unmapped_count}"
    )
    if enrichment_import.run_id:
        enqueue_live_progress(
            run_id=enrichment_import.run_id,
            importer="provider-enrichment",
            status="running",
            phase="provider-enrichment sources discovered",
            unit="sources",
            done=0,
            total=source_count,
            message=f"{source_count} sources discovered",
        )


async def _prepare_provider_enrichment_import(
    ctx: dict[str, Any],
    task: dict[str, Any],
) -> _ProviderEnrichmentImport:
    context = ctx.setdefault("context", {})
    run_id = str(context.get("control_run_id") or ctx.get("control_run_id") or "").strip()
    if "test_mode" in task:
        context["test_mode"] = bool(task.get("test_mode"))
    test_mode = bool(context.get("test_mode", False))
    await ensure_database(test_mode)
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await _prepare_staging_tables(ctx["import_date"], db_schema)
    audit = _provider_enrichment_audit(context)
    await _audit_nppes_headers(ctx)
    discovered_sources, unmapped_datasets = await _discover_sources(
        test_mode=test_mode
    )
    audit["unmapped_datasets"] = unmapped_datasets

    if not discovered_sources:
        raise RuntimeError(
            "No registered provider-enrichment sources were discovered "
            "from the CMS catalog."
        )
    enrichment_import = _ProviderEnrichmentImport(
        ctx=ctx,
        context=context,
        audit=audit,
        run_id=run_id,
        test_mode=test_mode,
        db_schema=db_schema,
        sources=discovered_sources,
        batch_size=_env_positive_int(
            "HLTHPRT_PROVIDER_ENRICHMENT_BATCH_SIZE",
            DEFAULT_PROVIDER_ENRICHMENT_BATCH_SIZE,
        ),
        max_pending_save_tasks=_env_positive_int(
            "HLTHPRT_PROVIDER_ENRICHMENT_MAX_PENDING_SAVE_TASKS",
            DEFAULT_MAX_PENDING_SAVE_TASKS,
        ),
    )
    _report_provider_enrichment_discovery(
        enrichment_import,
        len(unmapped_datasets),
    )
    return enrichment_import


def _report_provider_enrichment_source_progress(
    enrichment_import: _ProviderEnrichmentImport,
    source_map: dict[str, Any],
    source_index: int,
    *,
    loaded: bool,
) -> None:
    if not enrichment_import.run_id:
        return
    spec_key = source_map["spec_key"]
    action = "loaded" if loaded else "loading"
    enqueue_live_progress(
        run_id=enrichment_import.run_id,
        importer="provider-enrichment",
        status="running",
        phase=f"provider-enrichment {action} {spec_key}",
        unit="sources",
        done=source_index + int(loaded),
        total=len(enrichment_import.sources),
        message=f"{action} {spec_key}",
        label=str(source_map.get("distribution_title") or spec_key),
    )


def _provider_enrichment_csv_encoding(
    local_path: str,
    spec: dict[str, Any],
    source_map: dict[str, Any],
) -> str:
    csv_encoding = _select_csv_encoding(local_path)
    _validate_headers(
        _read_csv_header(local_path, csv_encoding),
        spec,
        str(
            source_map.get("distribution_title")
            or source_map.get("dataset_title")
        ),
    )
    if csv_encoding != CSV_PRIMARY_ENCODING:
        print(
            "Provider-enrichment source using fallback CSV encoding "
            f"'{csv_encoding}': {source_map.get('distribution_title')}"
        )
    return csv_encoding


async def _queue_provider_enrichment_batch(
    enrichment_import: _ProviderEnrichmentImport,
    task_key: str,
    payload_rows: list[dict[str, Any]],
    save_tasks: list[asyncio.Task],
) -> None:
    save_tasks.append(
        asyncio.create_task(
            save_provider_enrichment_data(
                enrichment_import.ctx,
                {task_key: payload_rows.copy()},
            )
        )
    )
    payload_rows.clear()
    if len(save_tasks) >= enrichment_import.max_pending_save_tasks:
        await asyncio.gather(*save_tasks)
        save_tasks.clear()


async def _stage_provider_enrichment_rows(
    enrichment_import: _ProviderEnrichmentImport,
    source_map: dict[str, Any],
    spec: dict[str, Any],
    local_path: str,
    csv_encoding: str,
) -> _ProviderEnrichmentSourceStats:
    stats = _ProviderEnrichmentSourceStats()
    payload_rows: list[dict[str, Any]] = []
    save_tasks: list[asyncio.Task] = []
    model_columns = _model_columns(spec["model"])
    async with async_open(local_path, "r", encoding=csv_encoding) as handle:
        reader = AsyncDictReader(handle, delimiter=",")
        async for source_row_map in reader:
            stats.processed_rows += 1
            row_payload_map, drop_reason = _build_row_payload(
                source_row_map,
                spec,
                source_map,
                model_columns,
            )
            if row_payload_map is None:
                if drop_reason == "missing_npi":
                    stats.rows_dropped_missing_npi += 1
                continue
            payload_rows.append(row_payload_map)
            stats.rows_accepted += 1
            if len(payload_rows) >= enrichment_import.batch_size:
                await _queue_provider_enrichment_batch(
                    enrichment_import,
                    spec["task_key"],
                    payload_rows,
                    save_tasks,
                )
            if (
                enrichment_import.test_mode
                and stats.rows_accepted >= TEST_PROVIDER_ENRICHMENT_ROWS
            ):
                break
    if payload_rows:
        await _queue_provider_enrichment_batch(
            enrichment_import,
            spec["task_key"],
            payload_rows,
            save_tasks,
        )
    if save_tasks:
        await asyncio.gather(*save_tasks)
    return stats


def _record_provider_enrichment_source_stats(
    enrichment_import: _ProviderEnrichmentImport,
    source_map: dict[str, Any],
    stats: _ProviderEnrichmentSourceStats,
) -> None:
    spec_key = source_map["spec_key"]
    dataset_stats = enrichment_import.audit["dataset_stats"].setdefault(
        spec_key,
        [],
    )
    dataset_stats.append(
        {
            "dataset_title": source_map.get("dataset_title"),
            "distribution_title": source_map.get("distribution_title"),
            "download_url": source_map.get("download_url"),
            "reporting_year": source_map.get("reporting_year"),
            "rows_processed": stats.processed_rows,
            "rows_accepted": stats.rows_accepted,
            "rows_dropped_missing_npi": stats.rows_dropped_missing_npi,
        }
    )
    enrichment_import.audit["rows_accepted"] += stats.rows_accepted
    enrichment_import.audit["rows_dropped_missing_npi"] += (
        stats.rows_dropped_missing_npi
    )
    print(
        f"Provider-enrichment source done: spec={spec_key} "
        f"processed={stats.processed_rows:,} "
        f"accepted={stats.rows_accepted:,} "
        f"dropped_missing_npi={stats.rows_dropped_missing_npi:,}"
    )


async def _stage_provider_enrichment_source(
    enrichment_import: _ProviderEnrichmentImport,
    source_map: dict[str, Any],
    source_index: int,
    tmpdirname: str,
) -> None:
    spec_key = source_map["spec_key"]
    spec = SPEC_BY_KEY[spec_key]
    local_path = str(
        PurePath(
            tmpdirname,
            f"provider_enrichment_{spec_key}_{source_index}.csv",
        )
    )
    print(
        "Downloading provider-enrichment source "
        f"[{source_index + 1}/{len(enrichment_import.sources)}] "
        f"{spec_key} {source_map.get('distribution_title')}"
    )
    _report_provider_enrichment_source_progress(
        enrichment_import,
        source_map,
        source_index,
        loaded=False,
    )
    await _download_source(str(source_map["download_url"]), local_path)
    csv_encoding = _provider_enrichment_csv_encoding(
        local_path,
        spec,
        source_map,
    )
    stats = await _stage_provider_enrichment_rows(
        enrichment_import,
        source_map,
        spec,
        local_path,
        csv_encoding,
    )
    _record_provider_enrichment_source_stats(
        enrichment_import,
        source_map,
        stats,
    )
    _report_provider_enrichment_source_progress(
        enrichment_import,
        source_map,
        source_index,
        loaded=True,
    )


async def import_provider_enrichment_sources(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
):
    """Discover, download, normalize, and stage provider-enrichment datasets."""
    enrichment_import = await _prepare_provider_enrichment_import(
        ctx,
        task or {},
    )
    with tempfile.TemporaryDirectory() as tmpdirname:
        for source_index, source_map in enumerate(
            enrichment_import.sources
        ):
            await _stage_provider_enrichment_source(
                enrichment_import,
                source_map,
                source_index,
                tmpdirname,
            )
    enrichment_import.context["run"] = (
        enrichment_import.context.get("run", 0) + 1
    )


process_data = import_provider_enrichment_sources
process_data.__name__ = "process_data"


async def startup(ctx):  # pragma: no cover
    """Initialize worker state and provider-enrichment staging tables."""
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


_PROVIDER_ENRICHMENT_SUMMARY_SQL = """
        INSERT INTO {db_schema}.{summary_stage} (
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
            {enrollment_union_sql}
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

_PROVIDER_ENRICHMENT_UNION_EXCLUDED_KEYS = frozenset(
    {
        "ffs_additional_npi",
        "ffs_reassignment",
        "ffs_address",
        "ffs_secondary_specialty",
    }
)


def _provider_enrichment_table_names(
    import_date: str,
) -> dict[str, str]:
    return {
        model_class.__main_table__: make_class(
            model_class, import_date
        ).__tablename__
        for model_class in PROCESSING_CLASSES
    }


def _provider_enrollment_union_sql(
    table_name_by_main: dict[str, str],
    db_schema: str,
) -> str:
    union_sql_parts = []
    for spec in ENROLLMENT_DATASET_SPECS:
        if spec["key"] in _PROVIDER_ENRICHMENT_UNION_EXCLUDED_KEYS:
            continue
        table_name = table_name_by_main[spec["model"].__main_table__]
        union_sql_parts.append(
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
    return " UNION ALL ".join(union_sql_parts)


async def _provider_enrichment_pricing_cte(db_schema: str) -> str:
    if await _is_table_available(db_schema, PricingProvider.__tablename__):
        return f"""
            SELECT
                npi::bigint AS npi,
                MIN(year)::int AS medicare_claim_year_min,
                MAX(year)::int AS medicare_claim_year_max,
                COUNT(*)::int AS medicare_claim_rows
              FROM {db_schema}.{PricingProvider.__tablename__}
             GROUP BY npi
        """
    return """
        SELECT NULL::bigint AS npi,
               NULL::int AS medicare_claim_year_min,
               NULL::int AS medicare_claim_year_max,
               NULL::int AS medicare_claim_rows
         WHERE FALSE
    """


async def _provider_enrichment_summary_bindings(
    import_date: str,
    db_schema: str,
    nppes_report: dict[str, Any],
) -> dict[str, str]:
    table_name_by_main = _provider_enrichment_table_names(import_date)
    medical_school_fields = [
        str(field_name)
        for field_name in (nppes_report.get("medical_school_headers") or [])
    ]
    return {
        "db_schema": db_schema,
        "summary_stage": table_name_by_main[
            ProviderEnrichmentSummary.__main_table__
        ],
        "enrollment_union_sql": _provider_enrollment_union_sql(
            table_name_by_main, db_schema
        ),
        "ffs_table": table_name_by_main[ProviderEnrollmentFFS.__main_table__],
        "ffs_additional_npi_table": table_name_by_main[
            ProviderEnrollmentFFSAdditionalNPI.__main_table__
        ],
        "ffs_address_table": table_name_by_main[
            ProviderEnrollmentFFSAddress.__main_table__
        ],
        "ffs_secondary_specialty_table": table_name_by_main[
            ProviderEnrollmentFFSSecondarySpecialty.__main_table__
        ],
        "ffs_reassignment_table": table_name_by_main[
            ProviderEnrollmentFFSReassignment.__main_table__
        ],
        "pricing_cte": await _provider_enrichment_pricing_cte(db_schema),
        "nppes_unmapped_count_sql": str(
            int(nppes_report.get("unmapped_field_count") or 0)
        ),
        "nppes_medical_school_fields_sql": _sql_varchar_array_literal(
            medical_school_fields
        ),
    }


async def _materialize_summary(
    import_date: str,
    db_schema: str,
    nppes_report: dict[str, Any],
) -> None:
    """Build the per-NPI enrichment summary from staged source tables."""
    bindings_by_name = await _provider_enrichment_summary_bindings(
        import_date,
        db_schema,
        nppes_report,
    )
    await db.status(
        "TRUNCATE TABLE "
        f"{db_schema}.{bindings_by_name['summary_stage']};"
    )
    await db.status(
        _PROVIDER_ENRICHMENT_SUMMARY_SQL.format(**bindings_by_name)
    )


_PROVIDER_ENRICHMENT_FFS_ADDRESS_FIELDS = {
    "first_line": "address_line_1",
    "second_line": "address_line_2",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "'US'",
}
_PROVIDER_ENRICHMENT_FFS_LOCATION_FIELDS = {
    "first_line": "NULL",
    "second_line": "NULL",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "'US'",
}
_PROVIDER_ENRICHMENT_REQUIRED_STAGES = (
    ProviderEnrollmentFFS,
    ProviderEnrollmentFFSAdditionalNPI,
    ProviderEnrollmentFFSAddress,
    ProviderEnrollmentFFSSecondarySpecialty,
    ProviderEnrollmentFFSReassignment,
)


async def _provider_enrichment_staging_tables(
    import_date: str,
    db_schema: str,
) -> dict[str, Any]:
    staging_table_by_name = {}
    for model_class in PROCESSING_CLASSES:
        stage_model = make_class(model_class, import_date)
        staging_table_by_name[model_class.__main_table__] = stage_model
        if not await _is_table_available(
            db_schema,
            stage_model.__tablename__,
        ):
            raise RuntimeError(
                f"Staging table {db_schema}.{stage_model.__tablename__} "
                "is missing; cannot finalize provider-enrichment publish."
            )
    return staging_table_by_name


async def _required_provider_enrichment_stage_counts(
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> dict[str, int]:
    stage_count_by_table = {}
    for model_class in _PROVIDER_ENRICHMENT_REQUIRED_STAGES:
        stage_model = staging_table_by_name[model_class.__main_table__]
        row_count = await db.scalar(
            f"SELECT COUNT(*) FROM "
            f"{db_schema}.{stage_model.__tablename__};"
        )
        stage_count_by_table[model_class.__main_table__] = int(
            row_count or 0
        )
        print(
            "Provider-enrichment staging rows: "
            f"{stage_model.__tablename__}="
            f"{stage_count_by_table[model_class.__main_table__]:,}"
        )
        if not stage_count_by_table[model_class.__main_table__]:
            raise RuntimeError(
                f"Required staging table "
                f"{db_schema}.{stage_model.__tablename__} is empty; "
                "aborting provider-enrichment publish."
            )
    return stage_count_by_table


async def _resolve_provider_enrichment_stage_address(
    stage_model: Any,
    address_fields: dict[str, str],
    db_schema: str,
) -> dict[str, Any]:
    await stamp_address_keys(
        stage_model.__tablename__,
        address_fields,
        schema=db_schema,
    )
    result = await resolve_into_archive(
        stage_model.__tablename__,
        address_fields,
        source_bit=4,
        priority=2,
        schema=db_schema,
    )
    return result.__dict__


async def _resolve_provider_enrichment_addresses(
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> dict[str, Any]:
    if not (
        source_enabled("provider_enrichment")
        or source_enabled("provider_enrollment_ffs")
    ):
        return {}
    address_stat_by_source = {
        "provider_enrollment_ffs": (
            await _resolve_provider_enrichment_stage_address(
                staging_table_by_name[ProviderEnrollmentFFS.__main_table__],
                _PROVIDER_ENRICHMENT_FFS_ADDRESS_FIELDS,
                db_schema,
            )
        ),
        "provider_enrollment_ffs_address": (
            await _resolve_provider_enrichment_stage_address(
                staging_table_by_name[
                    ProviderEnrollmentFFSAddress.__main_table__
                ],
                _PROVIDER_ENRICHMENT_FFS_LOCATION_FIELDS,
                db_schema,
            )
        ),
    }
    print(
        "Provider-enrichment canonical address resolve complete: "
        f"{address_stat_by_source}"
    )
    return address_stat_by_source


def _provider_enrichment_index_name(index: dict[str, Any]) -> str:
    return index.get("name", "_".join(index.get("index_elements")))


def _provider_enrichment_indexes(model_class: Any) -> list[dict[str, Any]]:
    return [
        *getattr(model_class, "__my_initial_indexes__", []),
        *getattr(model_class, "__my_additional_indexes__", []),
    ]


async def _create_provider_enrichment_indexes(
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> None:
    async with db.transaction():
        for model_class in PROCESSING_CLASSES:
            stage_model = staging_table_by_name[model_class.__main_table__]
            for index in getattr(
                model_class,
                "__my_additional_indexes__",
                [],
            ):
                index_name = _provider_enrichment_index_name(index)
                using = (
                    f"USING {index.get('using')} "
                    if index.get("using")
                    else ""
                )
                where_clause = (
                    f" WHERE {index.get('where')}"
                    if index.get("where")
                    else ""
                )
                create_index_sql = (
                    f"CREATE INDEX IF NOT EXISTS "
                    f"{stage_model.__tablename__}_idx_{index_name} "
                    f"ON {db_schema}.{stage_model.__tablename__} {using}"
                    f"({', '.join(index.get('index_elements'))})"
                    f"{where_clause};"
                )
                print(create_index_sql)
                await db.status(create_index_sql)


async def _refresh_enrichment_stage_statistics(
    stage_model: Any,
    db_schema: str,
) -> None:
    print(
        f"Post-Index ANALYZE {db_schema}.{stage_model.__tablename__};"
    )
    await db.execute_ddl(
        f"ANALYZE {db_schema}.{stage_model.__tablename__};"
    )


async def _refresh_all_enrichment_statistics(
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> None:
    await asyncio.gather(
        *(
            _refresh_enrichment_stage_statistics(
                staging_table_by_name[model_class.__main_table__],
                db_schema,
            )
            for model_class in PROCESSING_CLASSES
        )
    )


async def _archive_provider_enrichment_index(
    db_schema: str,
    index_name: str,
) -> None:
    archived_name = _archived_identifier(index_name)
    await db.status(
        f"DROP INDEX IF EXISTS {db_schema}.{archived_name};"
    )
    await db.status(
        f"ALTER INDEX IF EXISTS {db_schema}.{index_name} "
        f"RENAME TO {archived_name};"
    )


async def _publish_provider_enrichment_tables(
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> None:
    async with db.transaction():
        for model_class in PROCESSING_CLASSES:
            stage_model = staging_table_by_name[model_class.__main_table__]
            table_name = stage_model.__main_table__
            await db.status(
                f"DROP TABLE IF EXISTS {db_schema}.{table_name}_old;"
            )
            await db.status(
                f"ALTER TABLE IF EXISTS {db_schema}.{table_name} "
                f"RENAME TO {table_name}_old;"
            )
            await db.status(
                f"ALTER TABLE IF EXISTS "
                f"{db_schema}.{stage_model.__tablename__} "
                f"RENAME TO {table_name};"
            )
            await _archive_provider_enrichment_index(
                db_schema,
                f"{table_name}_idx_primary",
            )
            await db.status(
                f"ALTER INDEX IF EXISTS "
                f"{db_schema}.{stage_model.__tablename__}_idx_primary "
                f"RENAME TO {table_name}_idx_primary;"
            )
            for index in _provider_enrichment_indexes(model_class):
                index_name = _provider_enrichment_index_name(index)
                await _archive_provider_enrichment_index(
                    db_schema,
                    f"{table_name}_idx_{index_name}",
                )
                await db.status(
                    f"ALTER INDEX IF EXISTS "
                    f"{db_schema}.{stage_model.__tablename__}_idx_{index_name} "
                    f"RENAME TO {table_name}_idx_{index_name};"
                )


async def _complete_provider_enrichment_run(
    run_id: str,
    context: dict[str, Any],
    stage_count_by_table: dict[str, int],
    summary_rows: int,
    address_stat_by_source: dict[str, Any],
) -> dict[str, Any]:
    audit = context.get("audit", {})
    metrics_by_name = {
        "stage_rows": stage_count_by_table,
        "summary_rows": summary_rows,
        "datasets": len(audit.get("dataset_stats", {}) or {}),
        "rows_accepted": int(audit.get("rows_accepted") or 0),
        "rows_dropped_missing_npi": int(
            audit.get("rows_dropped_missing_npi") or 0
        ),
    }
    if address_stat_by_source:
        metrics_by_name["address_resolve"] = address_stat_by_source
    progress_by_name = {
        "unit": "tables",
        "done": len(PROCESSING_CLASSES),
        "total": len(PROCESSING_CLASSES),
        "pct": 100,
        "message": "succeeded",
        "phase": "provider-enrichment published",
    }
    await mark_control_run(
        run_id,
        status="succeeded",
        phase_detail="provider-enrichment published",
        progress_message="succeeded",
        progress=progress_by_name,
        metrics=metrics_by_name,
    )
    return {**metrics_by_name, "terminal_progress": progress_by_name}


async def _materialize_provider_enrichment_summary(
    ctx: dict[str, Any],
    context: dict[str, Any],
    staging_table_by_name: dict[str, Any],
    db_schema: str,
) -> int:
    await _materialize_summary(
        ctx["import_date"],
        db_schema,
        context.get("audit", {}).get("nppes_gap_report", {}),
    )
    summary_stage = staging_table_by_name[
        ProviderEnrichmentSummary.__main_table__
    ]
    return int(
        await db.scalar(
            f"SELECT COUNT(*) FROM "
            f"{db_schema}.{summary_stage.__tablename__};"
        )
        or 0
    )


async def publish_provider_enrichment_tables(
    ctx: dict[str, Any],
):
    """Validate staged data and atomically publish enrichment tables."""
    context = ctx.get("context") or {}
    if not context.get("run"):
        print(
            "No provider-enrichment jobs ran in this worker session; "
            "skipping shutdown validation."
        )
        return
    await ensure_database(bool(context.get("test_mode")))
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    staging_table_by_name = await _provider_enrichment_staging_tables(
        ctx["import_date"],
        db_schema,
    )
    stage_count_by_table = (
        await _required_provider_enrichment_stage_counts(
            staging_table_by_name,
            db_schema,
        )
    )
    address_stat_by_source = await _resolve_provider_enrichment_addresses(
        staging_table_by_name,
        db_schema,
    )
    summary_rows = await _materialize_provider_enrichment_summary(
        ctx,
        context,
        staging_table_by_name,
        db_schema,
    )
    await _create_provider_enrichment_indexes(
        staging_table_by_name,
        db_schema,
    )
    await _refresh_all_enrichment_statistics(
        staging_table_by_name,
        db_schema,
    )
    await _publish_provider_enrichment_tables(
        staging_table_by_name,
        db_schema,
    )
    run_id = str(
        context.get("control_run_id") or ctx.get("control_run_id") or ""
    ).strip()
    terminal_result = await _complete_provider_enrichment_run(
        run_id,
        context,
        stage_count_by_table,
        summary_rows,
        address_stat_by_source,
    )
    print_time_info(context.get("start"))
    return terminal_result


shutdown = publish_provider_enrichment_tables
shutdown.__name__ = "shutdown"


async def save_provider_enrichment_data(ctx, task):
    """Persist one normalized provider-enrichment task payload."""
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
    """Queue a provider-enrichment worker job."""
    redis = await create_pool(
        build_redis_settings(),
        job_serializer=serialize_job,
        job_deserializer=deserialize_job,
    )
    payload = {"test_mode": bool(test_mode)}
    await redis.enqueue_job("process_data", payload, _queue_name=PROVIDER_ENRICHMENT_QUEUE_NAME)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main(bool("--test" in sys.argv)))
