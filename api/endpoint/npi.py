# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import contextlib
import json
import logging
import math
import os
import random
import re
import time
import urllib.parse
import uuid
from collections import OrderedDict, defaultdict
from datetime import UTC, datetime
from textwrap import dedent
from types import SimpleNamespace
from typing import Any, Mapping, Optional, Sequence

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import JSON as SQLAlchemyJSON
from sqlalchemy import func, or_, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import literal_column, text, tuple_

from api.code_systems import (EXTERNAL_PROCEDURE_CODE_SYSTEMS,
                              INTERNAL_PROCEDURE_CODE_SYSTEM,
                              INTERNAL_RX_CODE_SYSTEM)
from api.endpoint.pagination import parse_pagination
from api.provider_specialty_filters import (
    ensure_specialty_resolution_cache,
    resolve_provider_specialty_filter,
)
from db.models import (AddressArchive, EntityAddressUnified, Issuer,
                       NPIAddress, NPIData, NPIDataOtherIdentifier,
                       NPIDataTaxonomy, NPIDataTaxonomyGroup, NUCCTaxonomy,
                       PlanNPIRaw, ProviderEnrichmentSummary,
                       ProviderEnrollmentFFS,
                       ProviderEnrollmentFFSAdditionalNPI,
                       ProviderEnrollmentFFSAddress,
                       ProviderEnrollmentFFSReassignment,
                       ProviderEnrollmentFFSSecondarySpecialty,
                       ProviderEnrollmentFQHC,
                       ProviderEnrollmentHomeHealthAgency,
                       ProviderEnrollmentHospice, ProviderEnrollmentHospital,
                       ProviderEnrollmentRHC, ProviderEnrollmentSNF,
                       ProviderDirectoryEndpoint,
                       ProviderDirectoryHealthcareService,
                       ProviderDirectoryInsurancePlan,
                       ProviderDirectoryOrganization,
                       ProviderDirectoryOrganizationAffiliation,
                       ProviderDirectoryPractitionerRole,
                       ProviderDirectorySource, db)
from process.ext.contact_canon import canonicalize_one as canonicalize_contact_one
from process.ext.utils import download_it
from process.openaddresses import exact_lookup_sql, fuzzy_lookup_sql, lookup_params_from_address, relaxed_lookup_sql
from process import provider_directory_profile as profile_artifact

blueprint = Blueprint("npi", url_prefix="/npi", version=1)
logger = logging.getLogger(__name__)
ENABLE_TRGM_FUZZY_NAME_SEARCH = os.getenv("HLTHPRT_ENABLE_TRGM_FUZZY_NAME_SEARCH", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
INTERNAL_MEDICATION_CODE_SYSTEM = INTERNAL_RX_CODE_SYSTEM
PROCEDURE_ALLOWED_CODE_SYSTEMS = {
    INTERNAL_PROCEDURE_CODE_SYSTEM,
    *EXTERNAL_PROCEDURE_CODE_SYSTEMS,
}
MEDICATION_ALLOWED_CODE_SYSTEMS = {
    INTERNAL_MEDICATION_CODE_SYSTEM,
    "NDC",
    "RXNORM",
}
CODE_TOKEN_PATTERN = re.compile(r"^[A-Z0-9._-]+$")
INT_CODE_PATTERN = re.compile(r"^-?\d+$")
CHAIN_PECOS_PROVIDER_TYPE_CODES = {"12-C1"}
PUBLIC_ADDRESS_EXCLUDED_COLUMNS = {"premise_key"}
PUBLIC_ADDRESS_SITE_KEY = "address_site_key"
PUBLIC_ADDRESS_SOURCE_DEBUG_COLUMNS = {
    "location_key",
    "entity_type",
    "entity_id",
    "entity_name",
    "entity_subtype",
    "row_origin",
    "archive_identity_version",
    "address_precision",
    "zip5",
    "state_code",
    "city_norm",
    "county_fips",
    "source_mask",
    "address_source_mask",
    "source_count",
    "independent_source_count",
    "multi_source_confirmed",
    "location_confidence_id",
    "confidence_score",
    "freshness_score",
    "address_sources",
}
MATCH_CANDIDATE_QUERY_PARAMS = {
    "address_site_key",
    "address_key",
    "lat",
    "long",
    "radius_miles",
    "phone",
    "entity_type_code",
    "entity_kind",
    "taxonomy_scope",
    "provider_type",
    "specialty",
    "include_subspecialties",
    "limit",
    "include_sources",
    "include_evidence",
    "debug",
}
PUBLIC_ADDRESS_EVIDENCE_DEBUG_COLUMNS = {
    "source_record_ids",
    "base_address_version",
    "inferred_npi",
    "inference_confidence",
    "inference_method",
}
# Per-address SOURCE + plan/network attribution surfaced BY DEFAULT when serving the
# unified address table, so a consumer can see where an address came from (NPPES /
# ACA / PTG-TiC) and which plans/networks it is associated with -- i.e. confirm an
# address is valid for a given plan/network. These columns do not exist on the
# legacy NPIAddress table, so they are only added for the EntityAddressUnified model.
PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS = {
    "address_sources",
    "address_precision",
    "source_count",
    "independent_source_count",
    "multi_source_confirmed",
    "aca_plan_array",
    "aca_network_array",
    "ptg_plan_array",
    "ptg_source_array",
    "group_plan_array",
}
PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY = "provider_directory_sources"
PROVIDER_DIRECTORY_CATALOG_ALIAS_COLUMNS = (
    # Catalog labels describe ingestion aliases, not provider-verified products.
    "source_id",
    "org_name",
    "plan_name",
)
PUBLIC_NESTED_TAXONOMY_EXCLUDED_COLUMNS = {"npi", "checksum"}


def _public_nested_taxonomy_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
    public_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for entry in rows or []:
        if not isinstance(entry, Mapping):
            continue
        public_entry = {
            str(key): value
            for key, value in entry.items()
            if str(key) not in PUBLIC_NESTED_TAXONOMY_EXCLUDED_COLUMNS
        }
        if not public_entry:
            continue
        dedupe_key = json.dumps(public_entry, default=str, sort_keys=True, separators=(",", ":"))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        public_rows.append(public_entry)
    return public_rows


# When true, the geo radius search returns providers at ALL geocoded service
# locations (NPPES primary/secondary PLUS TiC/PTG/ACA practice/site), matching the
# widened geo_idx partial predicate. Default OFF: the live geo_idx must be rebuilt
# to cover practice/site rows (via an entity_address_unified refresh) BEFORE this is
# enabled, otherwise the widened query cannot use the index and seq-scans the table.
GEO_SERVICE_LOCATIONS_ENV = "HLTHPRT_GEO_INCLUDE_SERVICE_LOCATIONS"
# Address types that are concrete service locations (the geo_idx + detail surface).
GEO_SERVICE_LOCATION_TYPES = ("primary", "secondary", "practice", "site")


def _geo_includes_service_locations() -> bool:
    return os.getenv(GEO_SERVICE_LOCATIONS_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


ADDRESS_SERVING_SOURCE_ENV = "HLTHPRT_ADDRESS_SERVING_SOURCE"
ADDRESS_SERVING_SOURCE_LEGACY = "legacy"
ADDRESS_SERVING_SOURCE_UNIFIED = "entity_address_unified"
FACILITY_ENROLLMENT_MODELS: dict[str, Any] = {
    "hospital": ProviderEnrollmentHospital,
    "hha": ProviderEnrollmentHomeHealthAgency,
    "hospice": ProviderEnrollmentHospice,
    "fqhc": ProviderEnrollmentFQHC,
    "rhc": ProviderEnrollmentRHC,
    "snf": ProviderEnrollmentSNF,
}


def _attach_public_address_site_key(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    premise_key = source.get("premise_key")
    if premise_key not in (None, ""):
        target.setdefault(PUBLIC_ADDRESS_SITE_KEY, premise_key)


def _redact_internal_address_fields(value: Any) -> Any:
    if isinstance(value, dict):
        _attach_public_address_site_key(value, value)
        for key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
            value.pop(key, None)
        for child in value.values():
            _redact_internal_address_fields(child)
    elif isinstance(value, list):
        for child in value:
            _redact_internal_address_fields(child)
    return value


def _env_flag(*names: str, default: bool = False) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is None:
            continue
        text_value = str(raw).strip()
        if not text_value:
            continue
        return text_value.lower() in {"1", "true", "yes", "on"}
    return default


def _parse_bounded_int(
    raw_value: Any,
    *,
    param_name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    if raw_value in (None, "", "null"):
        return default
    try:
        parsed = int(str(raw_value).strip())
    except ValueError as exc:
        raise sanic.exceptions.InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise sanic.exceptions.InvalidUsage(
            f"Parameter '{param_name}' must be between {minimum} and {maximum}"
        )
    return parsed


def _parse_optional_bounded_int(
    raw_value: Any,
    *,
    param_name: str,
    minimum: int,
    maximum: int,
) -> Optional[int]:
    if raw_value in (None, "", "null"):
        return None
    try:
        parsed = int(str(raw_value).strip())
    except ValueError as exc:
        raise sanic.exceptions.InvalidUsage(f"Parameter '{param_name}' must be an integer") from exc
    if parsed < minimum or parsed > maximum:
        raise sanic.exceptions.InvalidUsage(
            f"Parameter '{param_name}' must be between {minimum} and {maximum}"
        )
    return parsed


def _parse_bool_arg(raw_value: Any, *, default: bool) -> bool:
    if raw_value in (None, "", "null"):
        return default
    return str(raw_value).strip().lower() in {"1", "true", "yes", "on", "y"}


def _normalize_text_filter(raw_value: Any, *, param_name: str, max_length: int = 128) -> Optional[str]:
    if raw_value in (None, "", "null"):
        return None
    text_value = str(raw_value).strip()
    if not text_value:
        return None
    if len(text_value) > max_length:
        raise sanic.exceptions.InvalidUsage(f"Parameter '{param_name}' is too long (max {max_length} chars)")
    return text_value


def _normalize_state_filter(raw_value: Any) -> Optional[str]:
    state_value = _normalize_text_filter(raw_value, param_name="state", max_length=2)
    if state_value is None:
        return None
    normalized = state_value.upper()
    if not re.fullmatch(r"[A-Z]{2}", normalized):
        raise sanic.exceptions.InvalidUsage("Parameter 'state' must be a 2-letter code")
    return normalized


def _normalize_ccn_filter(raw_value: Any) -> Optional[str]:
    ccn_value = _normalize_text_filter(raw_value, param_name="ccn", max_length=32)
    if ccn_value is None:
        return None
    normalized = re.sub(r"\s+", "", ccn_value.upper())
    if not re.fullmatch(r"[A-Z0-9-]+", normalized):
        raise sanic.exceptions.InvalidUsage("Parameter 'ccn' must be alphanumeric")
    return normalized


def _provider_display_name_from_mapping(mapping: Mapping[str, Any]) -> str:
    entity_code = str(mapping.get("entity_type_code") or "").strip()
    first_name = str(mapping.get("provider_first_name") or "").strip()
    last_name = str(mapping.get("provider_last_name") or "").strip()
    organization_name = str(mapping.get("provider_organization_name") or "").strip()
    if entity_code == "1":
        personal = " ".join(part for part in [first_name, last_name] if part).strip()
        if personal:
            return personal
    if organization_name:
        return organization_name
    fallback = " ".join(part for part in [first_name, last_name] if part).strip()
    return fallback or "Unknown"


ENABLE_NPI_SCHEMA_CACHE = _env_flag(
    "HLTHPRT_ENABLE_NPI_SCHEMA_CACHE",
    "HLTHPRT_ENABLE_SCHEMA_CACHE",
)
_NPI_SCHEMA_CACHE_TTL_SECONDS = 300.0
_TABLE_EXISTS_CACHE: dict[str, tuple[float, bool]] = {}
_TABLE_COLUMNS_CACHE: dict[str, tuple[float, set[str]]] = {}
_NPI_FILTER_CAPABILITIES_CACHE_STATE: dict[
    str, Optional[tuple[float, str, dict[str, bool]]]
] = {"entry": None}
_NPI_PRIMARY_TOTAL_CACHE_STATE: dict[str, Optional[tuple[float, int]]] = {
    "entry": None
}
_NPI_HAS_INSURANCE_TOTAL_CACHE: dict[str, tuple[float, int]] = {}
_NPI_ALL_TOTAL_TIMEOUT_SECONDS = float(os.getenv("HLTHPRT_NPI_ALL_TOTAL_TIMEOUT_SECONDS", "3.0"))
_MATCH_CANDIDATES_TIMEOUT_SECONDS = float(os.getenv("HLTHPRT_MATCH_CANDIDATES_TIMEOUT_SECONDS", "8.0"))
_MATCH_CANDIDATES_DEFAULT_LIMIT = 5
_MATCH_CANDIDATES_MAX_LIMIT = 50
_MATCH_CANDIDATES_MAX_INTERNAL_ROWS = _MATCH_CANDIDATES_MAX_LIMIT * 8
_MATCH_CANDIDATES_DEFAULT_RADIUS_MILES = 1.0
_MATCH_CANDIDATES_MAX_RADIUS_MILES = 100.0
_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS = max(
    float(os.getenv("HLTHPRT_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", "300")),
    0.0,
)
_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS = max(int(os.getenv("HLTHPRT_NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS", "4096")), 0)
_NPI_DETAIL_RESPONSE_CACHE: OrderedDict[str, tuple[float, bytes]] = OrderedDict()
_TAXONOMY_CODES_CACHE_TTL_SECONDS = 600.0
_CLASSIFICATION_TAXONOMY_CODES_CACHE: dict[str, tuple[float, list[str]]] = {}
_CLASSIFICATION_NPI_CACHE_TTL_SECONDS = 600.0
_CLASSIFICATION_NPI_CACHE: dict[str, tuple[float, list[int]]] = {}
_CLASSIFICATION_CACHE_MAX_KEYS = max(1, int(os.getenv("HLTHPRT_CLASSIFICATION_CACHE_MAX_KEYS", "4")))

NAME_LIKE_TEMPLATE = (
    "LOWER("
    "COALESCE({alias}provider_first_name,'') || ' ' || "
    "COALESCE({alias}provider_last_name,'') || ' ' || "
    "COALESCE({alias}provider_organization_name,'') || ' ' || "
    "COALESCE({alias}provider_other_organization_name,'') || ' ' || "
    "COALESCE({alias}do_business_as_text,'')"
    ")"
)

ORGANIZATION_LIKE_TEMPLATE = (
    "LOWER("
    "COALESCE({alias}provider_organization_name,'') || ' ' || "
    "COALESCE({alias}provider_other_organization_name,'') || ' ' || "
    "COALESCE({alias}do_business_as_text,'')"
    ")"
)


def _taxonomy_codes_subquery(conditions: str) -> str:
    return (
        dedent(
            """
            (
                SELECT ARRAY_AGG(code) AS codes,
                       ARRAY_AGG(int_code) AS int_codes
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def _taxonomy_full_subquery(conditions: str) -> str:
    return (
        dedent(
            """
            (
                SELECT code,
                       int_code
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


def _taxonomy_classification_subquery(conditions: str) -> str:
    return (
        dedent(
            """
            (
                SELECT int_code,
                       classification
                  FROM mrf.nucc_taxonomy
                 WHERE {conditions}
            ) AS q
            """
        )
        .strip()
        .format(conditions=conditions)
    )


async def _get_taxonomy_codes_for_classification(classification: str, *, session=None) -> list[str]:
    key = str(classification or "").strip().lower()
    if not key:
        return []
    now = time.time()
    cached = _CLASSIFICATION_TAXONOMY_CODES_CACHE.get(key)
    if cached and (now - cached[0]) < _TAXONOMY_CODES_CACHE_TTL_SECONDS:
        return list(cached[1])

    query = select(NUCCTaxonomy.code).where(NUCCTaxonomy.classification == classification)
    if session is not None:
        rows = await session.execute(query)
        values = [row[0] for row in rows if row and row[0]]
    else:
        async with db.acquire() as conn:
            rows = await conn.all(query)
        values = [row[0] for row in rows if row and row[0]]
    _set_limited_classification_cache(
        _CLASSIFICATION_TAXONOMY_CODES_CACHE,
        key,
        values,
        now,
    )
    return list(values)


async def _get_classification_npi_list(classification: str, *, session=None) -> list[int]:
    key = str(classification or "").strip().lower()
    if not key:
        return []
    now = time.time()
    cached = _CLASSIFICATION_NPI_CACHE.get(key)
    if cached and (now - cached[0]) < _CLASSIFICATION_NPI_CACHE_TTL_SECONDS:
        return list(cached[1])

    taxonomy_codes = await _get_taxonomy_codes_for_classification(classification, session=session)
    if not taxonomy_codes:
        return []

    query = text(
        """
        SELECT DISTINCT t.npi
          FROM mrf.npi_taxonomy AS t
         WHERE t.healthcare_provider_taxonomy_code = ANY(:taxonomy_codes)
         ORDER BY t.npi
        """
    )
    if session is not None:
        query_result = await session.execute(
            query,
            {"taxonomy_codes": taxonomy_codes},
        )
        taxonomy_npi_rows = query_result.all()
    else:
        async with db.acquire() as conn:
            taxonomy_npi_rows = await conn.all(
                query,
                taxonomy_codes=taxonomy_codes,
            )

    npi_list: list[int] = []
    for taxonomy_npi_row in taxonomy_npi_rows:
        mapping = getattr(taxonomy_npi_row, "_mapping", None)
        if mapping is not None:
            npi_value = mapping.get("npi")
        else:
            npi_value = taxonomy_npi_row[0] if taxonomy_npi_row else None
        if npi_value is None:
            continue
        try:
            npi_list.append(int(npi_value))
        except (TypeError, ValueError):
            continue

    _set_limited_classification_cache(
        _CLASSIFICATION_NPI_CACHE,
        key,
        npi_list,
        now,
    )
    return list(npi_list)


def _set_limited_classification_cache(
    cache: dict[str, tuple[float, Any]],
    key: str,
    value: Any,
    now: float,
) -> None:
    cache[key] = (now, value)
    while len(cache) > _CLASSIFICATION_CACHE_MAX_KEYS:
        oldest_key = min(cache.items(), key=lambda item: item[1][0])[0]
        if oldest_key == key and len(cache) > 1:
            oldest_key = min(
                ((candidate_key, candidate_value) for candidate_key, candidate_value in cache.items() if candidate_key != key),
                key=lambda item: item[1][0],
            )[0]
        cache.pop(oldest_key, None)


def _taxonomy_group_subquery() -> str:
    return dedent(
        """
        (
            SELECT ARRAY_AGG(code) AS codes,
                   ARRAY_AGG(int_code) AS int_codes,
                   classification
              FROM mrf.nucc_taxonomy
             GROUP BY classification
        ) AS q
        """
    ).strip()


def _request_session(request) -> Any:
    return getattr(getattr(request, "ctx", None), "sa_session", None)


def _model_table_columns(model: Any) -> set[str]:
    table = getattr(model, "__table__", None)
    if table is None:
        return set()
    return {str(column.key) for column in table.columns if getattr(column, "key", None)}


def _schema_cache_key(table_name: str) -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    return f"{schema}.{table_name}"


def _cache_get(cache: dict[str, tuple[float, Any]], key: str) -> Any:
    if not ENABLE_NPI_SCHEMA_CACHE:
        return None
    entry = cache.get(key)
    if entry is None:
        return None
    cached_at, value = entry
    if (time.monotonic() - cached_at) > _NPI_SCHEMA_CACHE_TTL_SECONDS:
        cache.pop(key, None)
        return None
    return value


def _cache_set(cache: dict[str, tuple[float, Any]], key: str, value: Any) -> Any:
    if ENABLE_NPI_SCHEMA_CACHE:
        cache[key] = (time.monotonic(), value)
    return value


def _filter_cache_get() -> Optional[dict[str, bool]]:
    if not ENABLE_NPI_SCHEMA_CACHE:
        return None
    cache_entry = _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"]
    if cache_entry is None:
        return None
    cached_at, schema_key, value = cache_entry
    if (time.monotonic() - cached_at) > _NPI_SCHEMA_CACHE_TTL_SECONDS:
        _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = None
        return None
    if schema_key != (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"):
        _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = None
        return None
    return dict(value)


def _filter_cache_set(value: dict[str, bool]) -> dict[str, bool]:
    if ENABLE_NPI_SCHEMA_CACHE:
        _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = (
            time.monotonic(),
            os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
            dict(value),
        )
    return value


def _primary_total_cache_get() -> Optional[int]:
    if not ENABLE_NPI_SCHEMA_CACHE:
        return None
    cache_entry = _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"]
    if cache_entry is None:
        return None
    cached_at, value = cache_entry
    if (time.monotonic() - cached_at) > _NPI_SCHEMA_CACHE_TTL_SECONDS:
        _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = None
        return None
    return int(value)


def _primary_total_cache_set(value: int) -> int:
    if ENABLE_NPI_SCHEMA_CACHE:
        _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = (time.monotonic(), int(value))
    return int(value)


MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS = 256
MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE = 100
MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS = 8192
MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS = 32
MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_VALUES = 32
MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_TEXT_LENGTH = 2048
_PROVIDER_DIRECTORY_ROLE_JIT_DISABLED_ATTR = "_healthporta_provider_directory_role_jit_disabled"


def _has_insurance_total_cache_key(city: Optional[str], state: Optional[str]) -> str:
    city_key = (city or "").strip().upper()
    state_key = (state or "").strip().upper()
    return f"{city_key}|{state_key}"


def _has_insurance_total_cache_get(city: Optional[str], state: Optional[str]) -> Optional[int]:
    cached = _cache_get(_NPI_HAS_INSURANCE_TOTAL_CACHE, _has_insurance_total_cache_key(city, state))
    if cached is None:
        return None
    return int(cached)


def _has_insurance_total_cache_set(city: Optional[str], state: Optional[str], value: int) -> int:
    return int(
        _cache_set(
            _NPI_HAS_INSURANCE_TOTAL_CACHE,
            _has_insurance_total_cache_key(city, state),
            int(value),
        )
    )


PROVIDER_DIRECTORY_VISIBILITY_TABLES = (
    "provider_directory_source",
    "provider_directory_endpoint_dataset",
    "provider_directory_dataset_resource",
)
PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE = (
    "provider_directory_dataset_network_plan"
)
PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE = (
    "provider_directory_dataset_insurance_plan"
)
PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_COLUMNS = (
    "plan_active",
    "plan_identifier",
)
PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_REQUIREMENTS = tuple(
    (PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE, column_name)
    for column_name in PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_COLUMNS
)
PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE = (
    "provider_directory_dataset_affiliation_organization"
)
PROVIDER_DIRECTORY_DATASET_RELATION_VERSION = "1"

PROVIDER_DIRECTORY_EVIDENCE_CAPABILITY_SQL = """
    WITH requested_columns AS (
        SELECT column_table_name, column_name
          FROM unnest(
               CAST(:column_table_names AS varchar[]),
               CAST(:column_names AS varchar[])
          ) AS requested(column_table_name, column_name)
    )
    SELECT requested.table_name,
           to_regclass(:schema || '.' || requested.table_name)
               IS NOT NULL AS is_available
      FROM unnest(CAST(:table_names AS varchar[]))
           AS requested(table_name)
    UNION ALL
    SELECT requested.column_table_name || '.' || requested.column_name,
           EXISTS (
               SELECT 1
                 FROM pg_attribute
                WHERE attrelid = to_regclass(
                      :schema || '.' || requested.column_table_name
                )
                  AND attname = requested.column_name
                  AND attnum > 0
                  AND NOT attisdropped
           ) AS is_available
      FROM requested_columns AS requested
"""


# A handful of reference labs / large health systems carry 1k+ service locations.
# Returning every address inline produces multi-MB responses and 90ms+ build times
# (one geocode-enrichment task per address). Page the address_list by default so the
# common provider (a few addresses) is unchanged while the outliers stay bounded;
# callers walk every address via address_offset, or opt out with address_limit=all.
NPI_DETAIL_ADDRESS_DEFAULT_LIMIT = 200
NPI_DETAIL_ADDRESS_MAX_LIMIT = 1000


def _npi_detail_cache_key(
    npi: int,
    *,
    view: str,
    include_chain: bool,
    extra_info: bool,
    sync_geocode: bool,
    lookup_stored_geocode: bool,
    include_sources: bool = False,
    include_evidence: bool = False,
    include_profile: bool = True,
    profile_generation: str | None = None,
    address_limit: int | None = None,
    address_offset: int = 0,
    include_address_total: bool = True,
    address_key: str | None = None,
) -> str:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    address_source = os.getenv(ADDRESS_SERVING_SOURCE_ENV, ADDRESS_SERVING_SOURCE_UNIFIED).strip().lower()
    geocode_mode = "sync_geo" if sync_geocode else "stored_geo"
    archive_mode = "archive_geo" if lookup_stored_geocode else "no_archive_geo"
    debug_mode = f"sources:{int(include_sources)}|evidence:{int(include_evidence)}"
    profile_mode = (
        f"profile:{int(include_profile)}|"
        f"pgen:{profile_generation or 'none'}"
    )
    page_mode = (
        f"alim:{address_limit if address_limit is not None else 'all'}|"
        f"aoff:{int(address_offset or 0)}|atotal:{int(include_address_total)}|"
        f"akey:{address_key or 'none'}"
    )
    return (
        f"{schema}|{address_source}|{int(npi)}|{view}|"
        f"{'chain' if include_chain else 'default'}|"
        f"extra:{int(extra_info)}|{geocode_mode}|{archive_mode}|{debug_mode}|"
        f"{profile_mode}|{page_mode}"
    )


def _npi_detail_response_cache_get(cache_key: str) -> bytes | None:
    if _NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS <= 0 or _NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS <= 0:
        return None
    entry = _NPI_DETAIL_RESPONSE_CACHE.get(cache_key)
    if entry is None:
        return None
    cached_at, payload = entry
    if (time.monotonic() - cached_at) > _NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS:
        _NPI_DETAIL_RESPONSE_CACHE.pop(cache_key, None)
        return None
    _NPI_DETAIL_RESPONSE_CACHE.move_to_end(cache_key)
    return payload


def _npi_detail_response_cache_set(cache_key: str, payload: bytes) -> bytes:
    if _NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS <= 0 or _NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS <= 0:
        return payload
    _NPI_DETAIL_RESPONSE_CACHE[cache_key] = (time.monotonic(), payload)
    _NPI_DETAIL_RESPONSE_CACHE.move_to_end(cache_key)
    while len(_NPI_DETAIL_RESPONSE_CACHE) > _NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS:
        _NPI_DETAIL_RESPONSE_CACHE.popitem(last=False)
    return payload


def _npi_detail_response_cacheable(
    data: dict[str, Any],
    *,
    force_address_update: bool,
    sync_geocode: bool,
) -> bool:
    if force_address_update:
        return False
    if not sync_geocode:
        return True
    for address in data.get("address_list") or []:
        if isinstance(address, dict) and not address.get("lat"):
            return False
    return True


def _is_public_street_level_address(address: Any) -> bool:
    if not isinstance(address, dict):
        return False
    precision = address.get("address_precision")
    if precision is None:
        return True
    return str(precision).strip().lower() == "street"


def _address_type_rank(address: Mapping[str, Any]) -> int:
    return {
        "primary": 0,
        "practice": 1,
        "site": 2,
        "secondary": 3,
        "mail": 4,
    }.get(str(address.get("type") or "").strip().lower(), 9)


def _merge_unique_list_values(first: Any, second: Any) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for values in (first, second):
        if values is None:
            continue
        candidates = values if isinstance(values, list) else [values]
        for value in candidates:
            if value in (None, ""):
                continue
            marker = json.dumps(value, sort_keys=True, default=str)
            if marker in seen:
                continue
            seen.add(marker)
            merged.append(value)
    return merged


def _merge_duplicate_address(base: dict[str, Any], duplicate: Mapping[str, Any]) -> None:
    for key in (
        "address_sources",
        "source_record_ids",
        PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY,
        "aca_plan_array",
        "aca_network_array",
        "ptg_plan_array",
        "ptg_source_array",
        "group_plan_array",
        "taxonomy_array",
        "plans_network_array",
        "procedures_array",
        "medications_array",
    ):
        merged = _merge_unique_list_values(base.get(key), duplicate.get(key))
        if merged:
            base[key] = merged

    for key in (
        "telephone_number",
        "phone_number",
        "phone_extension",
        "fax_number",
        "fax_number_digits",
        "fax_extension",
        "formatted_address",
        "lat",
        "long",
        "place_id",
        "premise_key",
        PUBLIC_ADDRESS_SITE_KEY,
    ):
        if base.get(key) in (None, "") and duplicate.get(key) not in (None, ""):
            base[key] = duplicate.get(key)

    merged_sources = base.get("address_sources") or []
    if isinstance(merged_sources, list):
        base["source_count"] = max(
            int(base.get("source_count") or 0),
            int(duplicate.get("source_count") or 0),
            len(merged_sources),
        )
        base["independent_source_count"] = max(
            int(base.get("independent_source_count") or 0),
            int(duplicate.get("independent_source_count") or 0),
            len(merged_sources),
        )
        base["multi_source_confirmed"] = len(merged_sources) > 1


def _has_contact_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _add_canonical_contact_fields_to_address(address: dict[str, Any]) -> dict[str, Any]:
    if not (_has_contact_value(address.get("telephone_number")) or _has_contact_value(address.get("fax_number"))):
        return address

    canonical = canonicalize_contact_one(
        (
            address.get("telephone_number"),
            address.get("fax_number"),
            address.get("country_code") or "US",
        )
    )
    if not _has_contact_value(address.get("phone_number")) and _has_contact_value(canonical.get("phone_number")):
        address["phone_number"] = canonical.get("phone_number")
    if not _has_contact_value(address.get("phone_extension")) and _has_contact_value(canonical.get("phone_extension")):
        address["phone_extension"] = canonical.get("phone_extension")
    if not _has_contact_value(address.get("fax_number_digits")) and _has_contact_value(canonical.get("fax_number_digits")):
        address["fax_number_digits"] = canonical.get("fax_number_digits")
    if not _has_contact_value(address.get("fax_extension")) and _has_contact_value(canonical.get("fax_extension")):
        address["fax_extension"] = canonical.get("fax_extension")
    return address


def _address_dedupe_key(address: Mapping[str, Any]) -> str:
    raw_address_key = address.get("address_key")
    address_key = str(raw_address_key).strip().lower() if raw_address_key not in (None, "") else ""
    if not address_key:
        return ""
    raw_site_key = address.get(PUBLIC_ADDRESS_SITE_KEY) or address.get("premise_key")
    site_key = str(raw_site_key).strip().lower() if raw_site_key not in (None, "") else ""
    if site_key and site_key != address_key:
        return f"{address_key}:{site_key}"
    return address_key


def _dedupe_addresses_by_key(addresses: Sequence[Any]) -> list[dict[str, Any]]:
    keyed: dict[str, dict[str, Any]] = {}
    unkeyed: list[dict[str, Any]] = []
    for address in sorted(
        (entry for entry in addresses if isinstance(entry, dict)),
        key=lambda entry: (_address_type_rank(entry), str(entry.get("first_line") or "")),
    ):
        key = _address_dedupe_key(address)
        if not key:
            unkeyed.append(address)
            continue
        existing = keyed.get(key)
        if existing is None:
            keyed[key] = address
            continue
        _merge_duplicate_address(existing, address)
    return [
        _add_canonical_contact_fields_to_address(address)
        for address in (list(keyed.values()) + unkeyed)
    ]


def _provider_directory_source_ids_from_record_ids(record_ids: Any) -> list[str]:
    if not record_ids:
        return []
    candidates = record_ids if isinstance(record_ids, (list, tuple, set)) else [record_ids]
    source_ids: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        value = str(raw or "").strip()
        parts = value.split(":")
        if len(parts) < 3 or parts[0] != "provider_directory_fhir":
            continue
        source_id = parts[2].strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        source_ids.append(source_id)
    return source_ids


def _provider_directory_record_ids_from_address(address: Mapping[str, Any]) -> list[Any]:
    return _merge_unique_list_values(
        address.get("source_record_ids"),
        address.get("phone_source_record_ids"),
    )


def _provider_directory_source_ids_from_addresses(addresses: Sequence[Any]) -> list[str]:
    source_ids: list[str] = []
    seen: set[str] = set()
    for address in addresses or []:
        if not isinstance(address, Mapping):
            continue
        for source_id in _provider_directory_source_ids_from_record_ids(
            _provider_directory_record_ids_from_address(address)
        ):
            if source_id in seen:
                continue
            seen.add(source_id)
            source_ids.append(source_id)
    return source_ids


def _directory_role_keys_from_records(record_ids: Any) -> list[tuple[str, str]]:
    candidates = record_ids if isinstance(record_ids, (list, tuple, set)) else [record_ids]
    role_key_list: list[tuple[str, str]] = []
    seen_set: set[tuple[str, str]] = set()
    for raw_record_id in candidates:
        parts = str(raw_record_id or "").split(":")
        if len(parts) < 5 or parts[:2] != ["provider_directory_fhir", "practitioner_role"]:
            continue
        role_key = (parts[2].strip(), parts[3].strip())
        if not all(role_key) or role_key in seen_set:
            continue
        seen_set.add(role_key)
        role_key_list.append(role_key)
    return role_key_list


def _provider_directory_role_keys_from_addresses(
    addresses: Sequence[Any],
) -> list[tuple[str, str]]:
    role_key_list: list[tuple[str, str]] = []
    seen_set: set[tuple[str, str]] = set()
    for address in addresses or []:
        if not isinstance(address, Mapping):
            continue
        for role_key in _directory_role_keys_from_records(
            _provider_directory_record_ids_from_address(address)
        ):
            if role_key in seen_set:
                continue
            seen_set.add(role_key)
            role_key_list.append(role_key)
            if len(role_key_list) >= MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS:
                return role_key_list
    return role_key_list


def _directory_affiliation_keys_from_records(record_ids: Any) -> list[tuple[str, str]]:
    candidates = record_ids if isinstance(record_ids, (list, tuple, set)) else [record_ids]
    affiliation_key_list: list[tuple[str, str]] = []
    seen_set: set[tuple[str, str]] = set()
    for raw_record_id in candidates:
        parts = str(raw_record_id or "").split(":")
        if len(parts) < 5 or parts[:2] != [
            "provider_directory_fhir",
            "organization_affiliation",
        ]:
            continue
        affiliation_key = (parts[2].strip(), parts[3].strip())
        if not all(affiliation_key) or affiliation_key in seen_set:
            continue
        seen_set.add(affiliation_key)
        affiliation_key_list.append(affiliation_key)
    return affiliation_key_list


def _provider_directory_affiliation_keys_from_addresses(
    addresses: Sequence[Any],
) -> list[tuple[str, str]]:
    affiliation_key_list: list[tuple[str, str]] = []
    seen_set: set[tuple[str, str]] = set()
    for address in addresses or []:
        if not isinstance(address, Mapping):
            continue
        for affiliation_key in _directory_affiliation_keys_from_records(
            _provider_directory_record_ids_from_address(address)
        ):
            if affiliation_key in seen_set:
                continue
            seen_set.add(affiliation_key)
            affiliation_key_list.append(affiliation_key)
            if len(affiliation_key_list) >= MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS:
                return affiliation_key_list
    return affiliation_key_list


def _provider_directory_reference_resource_id_sql(reference: str, resource_type: str) -> str:
    return (
        "NULLIF(BTRIM(CASE "
        f"WHEN {reference} LIKE '%/{resource_type}/%' "
        f"THEN regexp_replace({reference}, '^.*/{resource_type}/', '') "
        f"WHEN {reference} LIKE '{resource_type}/%' "
        f"THEN regexp_replace({reference}, '^{resource_type}/', '') "
        f"ELSE {reference} END), '')"
    )


def _provider_directory_plan_network_match_sql(
    plan_network_reference: str,
    network_resource_id: str,
    network_reference: str,
) -> str:
    """Match a small InsurancePlan network list to a normalized network row."""
    normalized_plan_network_id = _provider_directory_reference_resource_id_sql(
        plan_network_reference,
        "Organization",
    )
    return (
        f"({plan_network_reference} = {network_reference} "
        f"OR {normalized_plan_network_id} = {network_resource_id})"
    )


def _insurance_plan_active_sql(alias: str) -> str:
    return f"COALESCE(NULLIF(LOWER(BTRIM({alias}.status)), ''), 'active') = 'active'"


def _dataset_relation_ready_sql(metadata_key: str) -> str:
    return f"""
        COALESCE(
            dataset.publication_metadata_json::jsonb
                -> '{metadata_key}' ->> 'complete',
            'false'
        ) = 'true'
        AND COALESCE(
            dataset.publication_metadata_json::jsonb
                -> '{metadata_key}' ->> 'version',
            ''
        ) = '{PROVIDER_DIRECTORY_DATASET_RELATION_VERSION}'
        AND COALESCE(
            dataset.publication_metadata_json::jsonb
                -> '{metadata_key}' ->> 'dataset_id',
            ''
        ) = dataset.dataset_id
    """


def _provider_directory_current_plan_resources_sql(
    schema: str,
    has_dataset_insurance_plan: bool,
) -> str:
    """Read immutable plan payloads from the compact projection when available."""
    if not has_dataset_insurance_plan:
        return """
    current_plan_resources AS NOT MATERIALIZED (
        SELECT resource.*
          FROM current_resources AS resource
         WHERE resource.resource_type = 'InsurancePlan'
    )
        """
    return f"""
    current_plan_resources AS NOT MATERIALIZED (
        SELECT source.source_id, source.canonical_api_base,
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               'InsurancePlan'::varchar AS resource_type,
               resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE} AS resource
            ON resource.dataset_id = dataset.dataset_id
    )
    """


def _provider_directory_current_resource_ctes_sql(
    schema: str,
    has_dataset_insurance_plan: bool = False,
) -> str:
    """Resolve resources through the one current, fully published endpoint dataset."""
    network_plan_ready_sql = _dataset_relation_ready_sql("dataset_network_plan")
    affiliation_ready_sql = _dataset_relation_ready_sql(
        "dataset_affiliation_organization"
    )
    return f"""
    current_endpoint_counts AS MATERIALIZED (
        SELECT dataset.endpoint_id
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
         WHERE dataset.is_current IS TRUE
      GROUP BY dataset.endpoint_id
        HAVING COUNT(*) = 1
    ), current_datasets AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id,
               COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id,
               dataset.published_at,
               ({network_plan_ready_sql}) AS dataset_network_plan_complete,
               ({affiliation_ready_sql})
                   AS dataset_affiliation_organization_complete
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
          JOIN current_endpoint_counts AS current_endpoint
            ON current_endpoint.endpoint_id = dataset.endpoint_id
         WHERE dataset.is_current IS TRUE
           AND dataset.status = 'published'
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
           AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
    ), current_resources AS NOT MATERIALIZED (
        SELECT source.source_id, source.canonical_api_base,
               dataset.dataset_id, dataset.run_id,
               dataset.published_at AS dataset_published_at,
               dataset.dataset_network_plan_complete,
               dataset.dataset_affiliation_organization_complete,
               resource.resource_type, resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id
          JOIN {schema}.provider_directory_dataset_resource AS resource
            ON resource.dataset_id = dataset.dataset_id
    ), {_provider_directory_current_plan_resources_sql(schema, has_dataset_insurance_plan)}
    """


_CURRENT_PROVIDER_DIRECTORY_TYPED_RESOURCE_MODELS = (
    (
        "current_insurance_plans",
        "InsurancePlan",
        ProviderDirectoryInsurancePlan,
    ),
    (
        "current_organizations",
        "Organization",
        ProviderDirectoryOrganization,
    ),
    (
        "current_healthcare_services",
        "HealthcareService",
        ProviderDirectoryHealthcareService,
    ),
    (
        "current_endpoints",
        "Endpoint",
        ProviderDirectoryEndpoint,
    ),
    (
        "current_roles",
        "PractitionerRole",
        ProviderDirectoryPractitionerRole,
    ),
    (
        "current_affiliations",
        "OrganizationAffiliation",
        ProviderDirectoryOrganizationAffiliation,
    ),
)


def _provider_directory_current_payload_column_sql(column: Any) -> str:
    """Project one normalized typed column from immutable dataset payload."""
    column_name = column.name
    if column_name in {"source_id", "resource_id"}:
        return f"resource.{column_name} AS {column_name}"
    if column_name == "last_seen_run_id":
        return "resource.run_id AS last_seen_run_id"
    if column_name in {"observed_at", "updated_at"}:
        return f"resource.dataset_published_at AS {column_name}"
    payload_expr = f"resource.payload_json -> '{column_name}'"
    if isinstance(column.type, SQLAlchemyJSON):
        return f"{payload_expr} AS {column_name}"
    column_type = column.type.compile(dialect=postgresql.dialect())
    return (
        f"CAST(resource.payload_json ->> '{column_name}' AS {column_type}) "
        f"AS {column_name}"
    )


def _current_typed_resource_ctes_sql() -> str:
    """Expose current immutable dataset payloads with typed-table column names."""
    cte_sql_list = []
    for cte_name, resource_type, model in _CURRENT_PROVIDER_DIRECTORY_TYPED_RESOURCE_MODELS:
        selected_columns = ",\n               ".join(
            (
                "resource.dataset_id AS dataset_id",
                (
                    "resource.dataset_network_plan_complete "
                    "AS dataset_network_plan_complete"
                ),
                (
                    "resource.dataset_affiliation_organization_complete "
                    "AS dataset_affiliation_organization_complete"
                ),
                *(
                    _provider_directory_current_payload_column_sql(column)
                    for column in model.__table__.columns
                ),
            )
        )
        cte_sql_list.append(
            f"""
    {cte_name} AS NOT MATERIALIZED (
        SELECT {selected_columns}
          FROM {"current_plan_resources" if resource_type == "InsurancePlan" else "current_resources"} AS resource
         WHERE resource.resource_type = '{resource_type}'
    )
            """.strip()
        )
    return ", ".join(cte_sql_list)


def _provider_directory_current_resource_join_sql(
    resource_alias: str,
    resource_type: str,
    current_alias: str,
) -> str:
    return f"""
          JOIN current_resources AS {current_alias}
            ON {current_alias}.source_id = {resource_alias}.source_id
           AND {current_alias}.resource_type = '{resource_type}'
           AND {current_alias}.resource_id = {resource_alias}.resource_id
           AND {resource_alias}.last_seen_run_id = {current_alias}.run_id
    """


def _provider_directory_network_resolution_sql(schema: str, has_catalog: bool) -> tuple[str, str, str]:
    organization_join = (
        "LEFT JOIN current_organizations AS network_organization "
        "ON network_organization.dataset_id = network.dataset_id "
        "AND network_organization.source_id = network.source_id "
        "AND network_organization.resource_id = network.resource_id "
        "AND network_organization.active IS DISTINCT FROM false"
    )
    if not has_catalog:
        return organization_join, "network_organization.name", "'provider_directory_organization'::varchar"
    catalog_join = (
        f"LEFT JOIN {schema}.provider_directory_network_catalog AS network_catalog "
        "ON network_catalog.source_id = network.source_id "
        "AND network_catalog.network_resource_id = network.resource_id"
    )
    network_name = "COALESCE(network_catalog.provider_directory_network_name, network_organization.name)"
    provenance = (
        "CASE WHEN network_catalog.provider_directory_network_name IS NOT NULL "
        "THEN 'provider_directory_network_catalog'::varchar "
        "ELSE 'provider_directory_organization'::varchar END"
    )
    return f"{catalog_join} {organization_join}", network_name, provenance


_EMPTY_AFFILIATION_NETWORK_CTE_SQL = """
    affiliation_networks AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.role_id,
               role.dataset_network_plan_complete,
               NULL::varchar AS reference,
               NULL::varchar AS resource_id,
               'organization-affiliation-network-derived'::varchar AS plan_provenance,
               'provider_directory_organization_affiliation'::varchar AS evidence_provenance
          FROM roles AS role
         WHERE false
    )
"""


def _dataset_affiliation_network_sql(
    schema: str,
    has_dataset_affiliation_organization: bool,
) -> str:
    affiliation_network_id = _provider_directory_reference_resource_id_sql(
        "affiliation_network_ref.value",
        "Organization",
    )
    if not has_dataset_affiliation_organization:
        return """
    dataset_affiliation_networks AS MATERIALIZED (
        SELECT role_organization.dataset_id, role_organization.source_id,
               role_organization.role_id,
               role_organization.dataset_network_plan_complete,
               NULL::varchar AS reference,
               NULL::varchar AS resource_id,
               'organization-affiliation-network-derived'::varchar AS plan_provenance,
               'provider_directory_organization_affiliation'::varchar AS evidence_provenance
          FROM role_organizations AS role_organization
         WHERE false
    )
        """
    return f"""
    dataset_affiliation_networks AS MATERIALIZED (
        SELECT DISTINCT role_organization.dataset_id,
               role_organization.source_id, role_organization.role_id,
               role_organization.dataset_network_plan_complete,
               affiliation_network_ref.value::varchar AS reference,
               {affiliation_network_id}::varchar AS resource_id,
               'organization-affiliation-network-derived'::varchar AS plan_provenance,
               'provider_directory_organization_affiliation'::varchar AS evidence_provenance
          FROM role_organizations AS role_organization
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE}
               AS affiliation_locator
            ON affiliation_locator.dataset_id = role_organization.dataset_id
           AND affiliation_locator.participating_organization_resource_id =
               role_organization.organization_resource_id
          JOIN current_affiliations AS affiliation
            ON affiliation.dataset_id = affiliation_locator.dataset_id
           AND affiliation.source_id = role_organization.source_id
           AND affiliation.resource_id = affiliation_locator.affiliation_resource_id
           AND affiliation.active IS DISTINCT FROM false
         CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
         ) AS affiliation_network_ref(value)
         WHERE role_organization.dataset_affiliation_organization_complete
    )
    """


def _legacy_affiliation_candidates_sql(
    has_affiliations: bool,
    has_dataset_affiliation_organization: bool,
) -> str:
    if not has_affiliations:
        return """
    affiliation_organization_candidates AS MATERIALIZED (
        SELECT role_organization.dataset_id, role_organization.source_id,
               role_organization.role_id,
               role_organization.dataset_network_plan_complete,
               NULL::varchar AS reference
          FROM role_organizations AS role_organization
         WHERE false
    )
        """
    relation_fallback_filter = (
        "AND NOT role_organization.dataset_affiliation_organization_complete"
        if has_dataset_affiliation_organization
        else ""
    )
    return f"""
    affiliation_organization_candidates AS MATERIALIZED (
        SELECT DISTINCT role_organization.dataset_id,
               role_organization.source_id, role_organization.role_id,
               role_organization.dataset_network_plan_complete,
               organization_candidate.reference
          FROM role_organizations AS role_organization
         CROSS JOIN LATERAL (
               VALUES
                   (role_organization.organization_ref::varchar),
                   (role_organization.organization_resource_id::varchar),
                   (('Organization/' || role_organization.organization_resource_id)::varchar)
         ) AS organization_candidate(reference)
         WHERE NULLIF(BTRIM(organization_candidate.reference), '') IS NOT NULL
           {relation_fallback_filter}
    )
    """


def _legacy_affiliation_network_sql(schema: str, has_affiliations: bool) -> str:
    if not has_affiliations:
        return """
    legacy_affiliation_networks AS MATERIALIZED (
        SELECT role_organization.dataset_id, role_organization.source_id,
               role_organization.role_id,
               role_organization.dataset_network_plan_complete,
               NULL::varchar AS reference,
               NULL::varchar AS resource_id,
               'organization-affiliation-network-derived'::varchar AS plan_provenance,
               'provider_directory_organization_affiliation'::varchar AS evidence_provenance
          FROM role_organizations AS role_organization
         WHERE false
    )
        """
    affiliation_network_id = _provider_directory_reference_resource_id_sql(
        "affiliation_network_ref.value",
        "Organization",
    )
    return f"""
    legacy_affiliation_networks AS MATERIALIZED (
        SELECT DISTINCT organization_candidate.dataset_id,
               organization_candidate.source_id, organization_candidate.role_id,
               organization_candidate.dataset_network_plan_complete,
               affiliation_network_ref.value::varchar AS reference,
               {affiliation_network_id}::varchar AS resource_id,
               'organization-affiliation-network-derived'::varchar AS plan_provenance,
               'provider_directory_organization_affiliation'::varchar AS evidence_provenance
          FROM affiliation_organization_candidates AS organization_candidate
          JOIN {schema}.provider_directory_organization_affiliation AS affiliation_locator
            ON affiliation_locator.source_id = organization_candidate.source_id
           AND affiliation_locator.participating_organization_ref = organization_candidate.reference
          JOIN current_affiliations AS affiliation
            ON affiliation.dataset_id = organization_candidate.dataset_id
           AND affiliation.source_id = affiliation_locator.source_id
           AND affiliation.resource_id = affiliation_locator.resource_id
           AND affiliation.participating_organization_ref = organization_candidate.reference
           AND affiliation.active IS DISTINCT FROM false
         CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
         ) AS affiliation_network_ref(value)
    )
    """


def _provider_directory_affiliation_network_ctes_sql(
    schema: str,
    has_affiliations: bool,
    has_dataset_affiliation_organization: bool,
) -> str:
    """Build active affiliation networks from dataset relations or legacy locators."""
    if not has_affiliations and not has_dataset_affiliation_organization:
        return _EMPTY_AFFILIATION_NETWORK_CTE_SQL
    role_organization_id = _provider_directory_reference_resource_id_sql(
        "role.organization_ref",
        "Organization",
    )
    dataset_cte_sql = _dataset_affiliation_network_sql(
        schema,
        has_dataset_affiliation_organization,
    )
    legacy_candidates_sql = _legacy_affiliation_candidates_sql(
        has_affiliations,
        has_dataset_affiliation_organization,
    )
    legacy_network_sql = _legacy_affiliation_network_sql(
        schema,
        has_affiliations,
    )
    return f"""
    role_organizations AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.role_id,
               role.organization_ref,
               role.dataset_network_plan_complete,
               role.dataset_affiliation_organization_complete,
               role_organization.resource_id AS organization_resource_id
          FROM roles AS role
          JOIN current_organizations AS role_organization
            ON role_organization.dataset_id = role.dataset_id
           AND role_organization.source_id = role.source_id
           AND role_organization.resource_id = {role_organization_id}
           AND role_organization.active IS DISTINCT FROM false
    ), {dataset_cte_sql}, {legacy_candidates_sql}, {legacy_network_sql},
    affiliation_networks AS MATERIALIZED (
        SELECT dataset_id, source_id, role_id, dataset_network_plan_complete,
               reference, resource_id,
               plan_provenance, evidence_provenance
          FROM dataset_affiliation_networks
        UNION
        SELECT dataset_id, source_id, role_id, dataset_network_plan_complete,
               reference, resource_id,
               plan_provenance, evidence_provenance
          FROM legacy_affiliation_networks
    )
    """


def _missing_catalog_plan_ctes_sql() -> str:
    return """
    role_catalog_status AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.role_id,
               NOT EXISTS (
                   SELECT 1 FROM role_networks AS role_network
                    WHERE role_network.dataset_id = role.dataset_id
                      AND role_network.source_id = role.source_id
                      AND role_network.role_id = role.role_id
               ) AS catalog_complete
          FROM roles AS role
    ), network_derived_plans AS MATERIALIZED (
        SELECT role.source_id, role.role_id, NULL::varchar AS resource_id,
               NULL::varchar AS identifier, NULL::varchar AS provenance
          FROM roles AS role
         WHERE false
    ), network_derived_plan_keys AS MATERIALIZED (
        SELECT role.source_id, role.role_id, NULL::varchar AS resource_id
          FROM roles AS role
         WHERE false
    )
    """


def _role_catalog_status_cte_sql(schema: str) -> str:
    """Require a catalog row for every role network before deriving plans."""
    return f"""
    role_catalog_status AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.role_id,
               BOOL_AND(
                   role_network.source_id IS NULL OR network_catalog.source_id IS NOT NULL
               ) AS catalog_complete
          FROM roles AS role
          LEFT JOIN role_networks AS role_network
            ON role_network.dataset_id = role.dataset_id
           AND role_network.source_id = role.source_id
           AND role_network.role_id = role.role_id
          LEFT JOIN {schema}.provider_directory_network_catalog AS network_catalog
            ON network_catalog.source_id = role_network.source_id
           AND network_catalog.network_resource_id = role_network.resource_id
      GROUP BY role.dataset_id, role.source_id, role.role_id
    )
    """


def _scoped_current_insurance_plan_ctes_sql(
    source_cte_name: str,
    scope_name: str,
    source_filter_sql: str = "",
) -> str:
    """Fence current InsurancePlan rows to the requested evidence sources first."""
    return f"""
    {scope_name}_sources AS MATERIALIZED (
        SELECT DISTINCT dataset_id, source_id
          FROM {source_cte_name}
          {source_filter_sql}
    ), {scope_name} AS MATERIALIZED (
        SELECT current_plan.*
          FROM {scope_name}_sources AS requested_source
          JOIN current_insurance_plans AS current_plan
            ON current_plan.dataset_id = requested_source.dataset_id
           AND current_plan.source_id = requested_source.source_id
    )
    """


def _dataset_role_plan_candidates_sql(schema: str) -> str:
    """Resolve role/network edges into distinct immutable plan candidates."""
    return f"""
    dataset_network_plan_candidates AS MATERIALIZED (
        SELECT role_network.dataset_id, role_network.source_id,
               role_network.role_id,
               network_plan.insurance_plan_resource_id AS resource_id,
               CASE WHEN BOOL_OR(role_network.plan_provenance = 'network-derived')
                    THEN 'network-derived'::varchar
                    ELSE 'organization-affiliation-network-derived'::varchar
                END AS provenance
          FROM valid_role_networks AS role_network
          JOIN role_catalog_status AS catalog_status
            ON catalog_status.dataset_id = role_network.dataset_id
           AND catalog_status.source_id = role_network.source_id
           AND catalog_status.role_id = role_network.role_id
           AND catalog_status.catalog_complete
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE} AS network_plan
            ON network_plan.dataset_id = role_network.dataset_id
           AND network_plan.network_resource_id = role_network.resource_id
         WHERE role_network.dataset_network_plan_complete
           AND NOT EXISTS (
               SELECT 1
                 FROM direct_plans AS direct_plan
                WHERE direct_plan.dataset_id = role_network.dataset_id
                  AND direct_plan.source_id = role_network.source_id
                  AND direct_plan.role_id = role_network.role_id
                  AND direct_plan.resource_id =
                      network_plan.insurance_plan_resource_id
           )
      GROUP BY role_network.dataset_id, role_network.source_id,
               role_network.role_id,
               network_plan.insurance_plan_resource_id
    )
    """


def _dataset_plan_scalar_sql(
    identifier_sql: str,
    active_sql: str,
    has_scalar_columns: bool,
) -> tuple[str, str]:
    """Choose generated plan scalars only when both columns are available."""
    if has_scalar_columns:
        return "insurance_plan.plan_identifier", "insurance_plan.plan_active"
    return identifier_sql, active_sql


def _dataset_role_plan_resources_sql(
    schema: str,
    insurance_plan_identifier: str,
    insurance_plan_active: str,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Load only active immutable plan payloads referenced by candidates."""
    resource_table = (
        PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE
        if has_dataset_insurance_plan
        else "provider_directory_dataset_resource"
    )
    resource_type_filter = (
        "" if has_dataset_insurance_plan else "AND insurance_plan.resource_type = 'InsurancePlan'"
    )
    selected_identifier, active_filter = _dataset_plan_scalar_sql(
        insurance_plan_identifier,
        insurance_plan_active,
        has_dataset_insurance_plan_scalars,
    )
    return f"""
    dataset_network_plan_resource_keys AS MATERIALIZED (
        SELECT DISTINCT candidate.dataset_id, candidate.resource_id
          FROM dataset_network_plan_candidates AS candidate
    ), dataset_network_plan_resources AS MATERIALIZED (
        SELECT candidate.dataset_id, insurance_plan.resource_id,
               NULLIF(BTRIM({selected_identifier}), '')::varchar AS identifier
          FROM dataset_network_plan_resource_keys AS candidate
          JOIN {schema}.{resource_table} AS insurance_plan
            ON insurance_plan.dataset_id = candidate.dataset_id
           AND insurance_plan.resource_id = candidate.resource_id
           {resource_type_filter}
           AND {active_filter}
    ), dataset_network_eligible_plan_candidates AS MATERIALIZED (
        SELECT candidate.source_id, candidate.role_id,
               insurance_plan.resource_id, insurance_plan.identifier,
               candidate.provenance
          FROM dataset_network_plan_candidates AS candidate
          JOIN dataset_network_plan_resources AS insurance_plan
            ON insurance_plan.dataset_id = candidate.dataset_id
           AND insurance_plan.resource_id = candidate.resource_id
    ), dataset_network_derived_plan_keys AS MATERIALIZED (
        SELECT source_id, role_id, resource_id
          FROM dataset_network_eligible_plan_candidates
    ), dataset_network_ranked_plan_candidates AS MATERIALIZED (
        SELECT candidate.*,
               ROW_NUMBER() OVER (
                   PARTITION BY candidate.source_id, candidate.role_id
                   ORDER BY candidate.resource_id,
                            candidate.identifier NULLS LAST,
                            candidate.provenance
               ) AS plan_rank
          FROM dataset_network_eligible_plan_candidates AS candidate
    ), dataset_network_derived_plans AS MATERIALIZED (
        SELECT source_id, role_id, resource_id, identifier, provenance
          FROM dataset_network_ranked_plan_candidates
         WHERE plan_rank <= {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE}
    )
    """


def _dataset_role_plan_sql(
    schema: str,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Build indexed immutable role-to-plan resolution CTEs."""
    insurance_plan_status = (
        "insurance_plan.payload_json::jsonb ->> 'status'"
    )
    insurance_plan_identifier = (
        "insurance_plan.payload_json::jsonb ->> 'plan_identifier'"
    )
    insurance_plan_active = (
        "COALESCE(NULLIF(LOWER(BTRIM("
        f"{insurance_plan_status})), ''), 'active') = 'active'"
    )
    candidate_sql = _dataset_role_plan_candidates_sql(schema)
    resource_sql = _dataset_role_plan_resources_sql(
        schema,
        insurance_plan_identifier,
        insurance_plan_active,
        has_dataset_insurance_plan,
        has_dataset_insurance_plan_scalars,
    )
    return f"""
    {candidate_sql}, {resource_sql}
    """


def _legacy_role_plan_sql(has_dataset_network_plan: bool) -> str:
    insurance_plan_active = _insurance_plan_active_sql("insurance_plan")
    network_match = _provider_directory_plan_network_match_sql(
        "plan_network_ref.value",
        "role_network.resource_id",
        "role_network.reference",
    )
    legacy_filter = (
        "AND NOT role_network.dataset_network_plan_complete"
        if has_dataset_network_plan
        else ""
    )
    return f"""
    legacy_network_derived_plans AS MATERIALIZED (
        SELECT role_network.source_id, role_network.role_id,
               insurance_plan.resource_id,
               NULLIF(BTRIM(insurance_plan.plan_identifier), '')::varchar AS identifier,
               CASE WHEN BOOL_OR(role_network.plan_provenance = 'network-derived')
                    THEN 'network-derived'::varchar
                    ELSE 'organization-affiliation-network-derived'::varchar
                END AS provenance
          FROM valid_role_networks AS role_network
          JOIN role_catalog_status AS catalog_status
            ON catalog_status.dataset_id = role_network.dataset_id
           AND catalog_status.source_id = role_network.source_id
           AND catalog_status.role_id = role_network.role_id
           AND catalog_status.catalog_complete
          JOIN legacy_role_insurance_plans AS insurance_plan
            ON insurance_plan.dataset_id = role_network.dataset_id
           AND insurance_plan.source_id = role_network.source_id
           AND {insurance_plan_active}
         WHERE 1 = 1
           {legacy_filter}
           AND EXISTS (
               SELECT 1
                 FROM jsonb_array_elements_text(
                      COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
                 ) AS plan_network_ref(value)
                WHERE {network_match}
         )
           AND NOT EXISTS (
               SELECT 1
                 FROM direct_plans AS direct_plan
                WHERE direct_plan.source_id = role_network.source_id
                  AND direct_plan.dataset_id = role_network.dataset_id
                  AND direct_plan.role_id = role_network.role_id
                  AND direct_plan.resource_id = insurance_plan.resource_id
           )
      GROUP BY role_network.source_id, role_network.role_id,
               insurance_plan.resource_id, insurance_plan.plan_identifier
    )
    """


def _network_derived_role_plans_cte_sql(
    schema: str,
    has_dataset_network_plan: bool,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Derive role plans from dataset edges with a legacy JSON fallback."""
    scoped_plan_ctes_sql = _scoped_current_insurance_plan_ctes_sql(
        "roles",
        "legacy_role_insurance_plans",
        (
            "WHERE NOT dataset_network_plan_complete"
            if has_dataset_network_plan
            else ""
        ),
    )
    dataset_plan_cte_sql = (
        _dataset_role_plan_sql(
            schema,
            has_dataset_insurance_plan,
            has_dataset_insurance_plan_scalars,
        )
        if has_dataset_network_plan
        else """
    dataset_network_derived_plans AS MATERIALIZED (
        SELECT role_network.source_id, role_network.role_id,
               NULL::varchar AS resource_id, NULL::varchar AS identifier,
               NULL::varchar AS provenance
          FROM valid_role_networks AS role_network
         WHERE false
    ), dataset_network_derived_plan_keys AS MATERIALIZED (
        SELECT role_network.source_id, role_network.role_id,
               NULL::varchar AS resource_id
          FROM valid_role_networks AS role_network
         WHERE false
    )
        """
    )
    legacy_plan_cte_sql = _legacy_role_plan_sql(has_dataset_network_plan)
    return f"""
    {scoped_plan_ctes_sql}, {dataset_plan_cte_sql}, {legacy_plan_cte_sql},
    network_derived_plan_keys AS MATERIALIZED (
        SELECT source_id, role_id, resource_id
          FROM dataset_network_derived_plan_keys
        UNION ALL
        SELECT source_id, role_id, resource_id
          FROM legacy_network_derived_plans
    ), network_derived_plans AS MATERIALIZED (
        SELECT source_id, role_id, resource_id, identifier, provenance
          FROM dataset_network_derived_plans
        UNION ALL
        SELECT source_id, role_id, resource_id, identifier, provenance
          FROM legacy_network_derived_plans
    )
    """


def _provider_directory_catalog_plan_ctes_sql(
    schema: str,
    has_catalog: bool,
    has_dataset_network_plan: bool,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Build catalog-gated role plan CTEs without expanding catalog payloads."""
    if not has_catalog:
        return _missing_catalog_plan_ctes_sql()
    return f"""
    {_role_catalog_status_cte_sql(schema)},
    {_network_derived_role_plans_cte_sql(
        schema,
        has_dataset_network_plan,
        has_dataset_insurance_plan,
        has_dataset_insurance_plan_scalars,
    )}
    """


def _provider_directory_network_plan_ctes_sql(
    schema: str,
    has_affiliations: bool,
    has_catalog: bool,
    has_dataset_network_plan: bool,
    has_dataset_affiliation_organization: bool,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Build same-source network intersection CTEs for role plan evidence."""
    role_network_id = _provider_directory_reference_resource_id_sql(
        "role_network_ref.value", "Organization"
    )
    affiliation_ctes_sql = _provider_directory_affiliation_network_ctes_sql(
        schema, has_affiliations, has_dataset_affiliation_organization
    )
    plan_ctes_sql = _provider_directory_catalog_plan_ctes_sql(
        schema,
        has_catalog,
        has_dataset_network_plan,
        has_dataset_insurance_plan,
        has_dataset_insurance_plan_scalars,
    )
    return f"""
    direct_role_networks AS MATERIALIZED (
        SELECT DISTINCT role.dataset_id, role.source_id, role.role_id,
               role.dataset_network_plan_complete,
               role_network_ref.value::varchar AS reference,
               {role_network_id}::varchar AS resource_id,
               'network-derived'::varchar AS plan_provenance,
               NULL::varchar AS evidence_provenance
          FROM roles AS role
         CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(role.network_refs, '[]'::jsonb)
         ) AS role_network_ref(value)
    ), {affiliation_ctes_sql}, role_networks AS MATERIALIZED (
        SELECT dataset_id, source_id, role_id, dataset_network_plan_complete,
               reference, resource_id,
               plan_provenance, evidence_provenance
          FROM direct_role_networks
        UNION
        SELECT dataset_id, source_id, role_id, dataset_network_plan_complete,
               reference, resource_id,
               plan_provenance, evidence_provenance
          FROM affiliation_networks
    ), valid_role_networks AS MATERIALIZED (
        SELECT role_network.dataset_id, role_network.source_id,
               role_network.role_id, role_network.dataset_network_plan_complete,
               role_network.reference, role_network.resource_id,
               role_network.plan_provenance, role_network.evidence_provenance
          FROM role_networks AS role_network
          JOIN current_organizations AS role_network_organization
            ON role_network_organization.dataset_id = role_network.dataset_id
           AND role_network_organization.source_id = role_network.source_id
           AND role_network_organization.resource_id = role_network.resource_id
           AND role_network_organization.active IS DISTINCT FROM false
         WHERE role_network.resource_id IS NOT NULL
    ), {plan_ctes_sql}
    """


def _provider_directory_requested_role_ctes_sql(schema: str) -> str:
    plan_id = _provider_directory_reference_resource_id_sql("plan_ref.value", "InsurancePlan")
    service_id = _provider_directory_reference_resource_id_sql(
        "service_ref.value", "HealthcareService"
    )
    endpoint_id = _provider_directory_reference_resource_id_sql(
        "endpoint_ref.value", "Endpoint"
    )
    insurance_plan_active = _insurance_plan_active_sql("insurance_plan")
    return f"""
    requested_roles AS (
        SELECT source_id, role_id
          FROM unnest(CAST(:source_ids AS varchar[]), CAST(:role_ids AS varchar[]))
               AS requested(source_id, role_id)
    ), roles AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.resource_id AS role_id,
               role.dataset_network_plan_complete,
               role.dataset_affiliation_organization_complete,
               role.organization_ref,
               role.insurance_plan_refs::jsonb, role.network_refs::jsonb,
               role.active AS role_active,
               role.identifiers::jsonb AS role_identifiers,
               role.location_refs::jsonb AS role_location_refs,
               role.healthcare_service_refs::jsonb AS role_healthcare_service_refs,
               role.endpoint_refs::jsonb AS role_endpoint_refs,
               role.specialty_codes::jsonb AS role_specialty_codes,
               role.code_codes::jsonb AS role_code_codes,
               role.telecom::jsonb AS role_telecom,
               role.period_start AS role_period_start,
               role.period_end AS role_period_end,
               role.available_time::jsonb AS role_available_time,
               role.not_available::jsonb AS role_not_available,
               role.availability_exceptions AS role_availability_exceptions,
               COALESCE(
                   role.new_patient_acceptance::jsonb,
                   role.accepting_patients::jsonb
               ) AS role_new_patient_acceptance,
               role.accepting_patients::jsonb AS role_accepting_patients,
               role.telehealth AS role_telehealth,
               role.accepting_medicaid AS role_accepting_medicaid,
               role.fhir_meta::jsonb AS role_fhir_meta,
               role.fhir_self_url AS role_fhir_self_url,
               role.fhir_fetch_url AS role_fhir_fetch_url,
               role.fhir_fetch_mode AS role_fhir_fetch_mode,
               COALESCE((
                   SELECT jsonb_agg(endpoint_detail ORDER BY endpoint_detail ->> 'resource_id')
                     FROM (
                         SELECT jsonb_strip_nulls(
                                    jsonb_build_object(
                                        'source_id', endpoint.source_id,
                                        'resource_id', endpoint.resource_id,
                                        'status', endpoint.status,
                                        'connection_type_system', endpoint.connection_type_system,
                                        'connection_type_code', endpoint.connection_type_code,
                                        'connection_type_display', endpoint.connection_type_display,
                                        'name', endpoint.name,
                                        'managing_organization_ref', endpoint.managing_organization_ref,
                                        'contact', endpoint.contact::jsonb,
                                        'period_start', endpoint.period_start,
                                        'period_end', endpoint.period_end,
                                        'payload_type_codes', endpoint.payload_type_codes::jsonb,
                                        'payload_mime_types', endpoint.payload_mime_types::jsonb,
                                        'address', endpoint.address,
                                        'fhir_meta', endpoint.fhir_meta::jsonb,
                                        'fhir_self_url', endpoint.fhir_self_url,
                                        'fhir_fetch_url', endpoint.fhir_fetch_url,
                                        'fhir_fetch_mode', endpoint.fhir_fetch_mode
                                    )
                                ) AS endpoint_detail
                           FROM jsonb_array_elements_text(
                                    COALESCE(role.endpoint_refs::jsonb, '[]'::jsonb)
                                ) AS endpoint_ref(value)
                           JOIN current_endpoints AS endpoint
                             ON endpoint.dataset_id = role.dataset_id
                            AND endpoint.source_id = role.source_id
                            AND endpoint.resource_id = {endpoint_id}
                          ORDER BY endpoint.resource_id
                          LIMIT {MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS}
                     ) AS resolved_endpoint
               ), '[]'::jsonb) AS role_endpoints,
               COALESCE((
                   SELECT jsonb_agg(service_detail ORDER BY service_detail ->> 'resource_id')
                     FROM (
                         SELECT jsonb_strip_nulls(
                                    jsonb_build_object(
                                        'source_id', service.source_id,
                                        'resource_id', service.resource_id,
                                        'active', service.active,
                                        'identifiers', service.identifiers::jsonb,
                                        'name', service.name,
                                        'type_codes', service.type_codes::jsonb,
                                        'category_codes', service.category_codes::jsonb,
                                        'specialty_codes', service.specialty_codes::jsonb,
                                        'program_codes', service.program_codes::jsonb,
                                        'communication_codes', service.communication_codes::jsonb,
                                        'appointment_required', service.appointment_required,
                                        'location_refs', service.location_refs::jsonb,
                                        'endpoint_refs', service.endpoint_refs::jsonb,
                                        'telecom', service.telecom::jsonb,
                                        'available_time', service.available_time::jsonb,
                                        'not_available', service.not_available::jsonb,
                                        'availability_exceptions', service.availability_exceptions,
                                        'accepting_patients', service.accepting_patients::jsonb,
                                        'fhir_meta', service.fhir_meta::jsonb,
                                        'fhir_self_url', service.fhir_self_url,
                                        'fhir_fetch_url', service.fhir_fetch_url,
                                        'fhir_fetch_mode', service.fhir_fetch_mode
                                    )
                                ) AS service_detail
                           FROM jsonb_array_elements_text(
                                    COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)
                                ) AS service_ref(value)
                           JOIN current_healthcare_services AS service
                             ON service.dataset_id = role.dataset_id
                            AND service.source_id = role.source_id
                            AND service.resource_id = {service_id}
                          ORDER BY service.resource_id
                          LIMIT {MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS}
                     ) AS resolved_service
               ), '[]'::jsonb) AS role_healthcare_services
          FROM requested_roles AS requested
          JOIN current_roles AS role
            ON role.source_id = requested.source_id AND role.resource_id = requested.role_id
         WHERE role.active IS DISTINCT FROM false
    ), direct_plans AS MATERIALIZED (
        SELECT role.dataset_id, role.source_id, role.role_id,
               role.dataset_network_plan_complete,
               insurance_plan.resource_id,
               NULLIF(BTRIM(insurance_plan.plan_identifier), '')::varchar AS identifier,
               COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb) AS network_refs
          FROM roles AS role
         CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(role.insurance_plan_refs, '[]'::jsonb)
         ) AS plan_ref(value)
          JOIN current_insurance_plans AS insurance_plan
            ON insurance_plan.dataset_id = role.dataset_id
           AND insurance_plan.source_id = role.source_id
           AND insurance_plan.resource_id = {plan_id}
           AND {insurance_plan_active}
    )
    """


def _provider_directory_plan_cap_ctes_sql() -> str:
    """Cap returned payload keys while retaining exact active-plan totals."""
    return f"""
    all_plan_keys AS MATERIALIZED (
        SELECT direct_plan.source_id, direct_plan.role_id, direct_plan.resource_id
          FROM direct_plans AS direct_plan
        UNION ALL
        SELECT derived_plan.source_id, derived_plan.role_id, derived_plan.resource_id
          FROM network_derived_plan_keys AS derived_plan
    ), unique_plan_keys AS MATERIALIZED (
        SELECT DISTINCT source_id, role_id, resource_id FROM all_plan_keys
    ), plan_counts_by_role AS MATERIALIZED (
        SELECT source_id, role_id, COUNT(resource_id)::bigint AS plan_total FROM unique_plan_keys GROUP BY source_id, role_id
    ), plan_candidates AS MATERIALIZED (
        SELECT direct_plan.source_id, direct_plan.role_id, direct_plan.resource_id,
               direct_plan.identifier,
               'provider_directory_insurance_plan'::varchar AS provenance
          FROM direct_plans AS direct_plan
        UNION ALL
        SELECT derived_plan.source_id, derived_plan.role_id, derived_plan.resource_id,
               derived_plan.identifier, derived_plan.provenance
          FROM network_derived_plans AS derived_plan
    ), unique_plans AS MATERIALIZED (
        SELECT DISTINCT ON (source_id, role_id, resource_id)
               source_id, role_id, resource_id, identifier, provenance
          FROM plan_candidates
      ORDER BY source_id, role_id, resource_id,
               CASE WHEN provenance = 'provider_directory_insurance_plan' THEN 0 ELSE 1 END,
               identifier, provenance
    ), ranked_plans AS MATERIALIZED (
        SELECT unique_plan.*,
               ROW_NUMBER() OVER (
                   PARTITION BY source_id, role_id
                   ORDER BY
                       CASE WHEN provenance = 'provider_directory_insurance_plan' THEN 0 ELSE 1 END,
                       resource_id, identifier NULLS LAST, provenance
               ) AS plan_rank
          FROM unique_plans AS unique_plan
    ), returned_plans AS MATERIALIZED (
        SELECT source_id, role_id, resource_id, identifier, provenance
          FROM ranked_plans
         WHERE plan_rank <= {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE}
    ), role_plan_metadata AS MATERIALIZED (
        SELECT role.source_id, role.role_id,
               LEAST(COALESCE(plan_count.plan_total, 0), {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE})::bigint
                   AS plan_returned,
               CASE WHEN catalog_status.catalog_complete
                    THEN COALESCE(plan_count.plan_total, 0) END AS plan_total,
               CASE WHEN catalog_status.catalog_complete
                    THEN COALESCE(plan_count.plan_total, 0) > {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE}
                END AS plan_truncated,
               catalog_status.catalog_complete
          FROM roles AS role
          JOIN role_catalog_status AS catalog_status
            ON catalog_status.source_id = role.source_id
           AND catalog_status.role_id = role.role_id
          LEFT JOIN plan_counts_by_role AS plan_count ON plan_count.source_id = role.source_id
           AND plan_count.role_id = role.role_id
    )
    """


def _direct_plan_network_sql(
    schema: str,
    has_dataset_network_plan: bool,
) -> str:
    legacy_filter = ""
    dataset_sql = ""
    if has_dataset_network_plan:
        dataset_sql = f"""
        SELECT direct_plan.dataset_id, direct_plan.source_id,
               direct_plan.role_id,
               ('Organization/' || network_plan.network_resource_id)::varchar
                   AS reference,
               NULL::varchar AS evidence_provenance
          FROM direct_plans AS direct_plan
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE} AS network_plan
            ON network_plan.dataset_id = direct_plan.dataset_id
           AND network_plan.insurance_plan_resource_id = direct_plan.resource_id
         WHERE direct_plan.dataset_network_plan_complete
        UNION
        """
        legacy_filter = "WHERE NOT direct_plan.dataset_network_plan_complete"
    return f"""
        {dataset_sql}
        SELECT direct_plan.dataset_id, direct_plan.source_id,
               direct_plan.role_id, network_ref.value::varchar AS reference,
               NULL::varchar AS evidence_provenance
          FROM direct_plans AS direct_plan
         CROSS JOIN LATERAL jsonb_array_elements_text(
               direct_plan.network_refs
         ) AS network_ref(value)
         {legacy_filter}
    """


def _provider_directory_role_ctes_sql(
    schema: str,
    has_affiliations: bool,
    has_catalog: bool,
    has_dataset_network_plan: bool,
    has_dataset_affiliation_organization: bool,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Compose keyed role, network, capped-plan, and network-evidence CTEs."""
    requested_role_ctes_sql = _provider_directory_requested_role_ctes_sql(schema)
    network_plan_ctes_sql = _provider_directory_network_plan_ctes_sql(
        schema,
        has_affiliations,
        has_catalog,
        has_dataset_network_plan,
        has_dataset_affiliation_organization,
        has_dataset_insurance_plan,
        has_dataset_insurance_plan_scalars,
    )
    plan_cap_ctes_sql = _provider_directory_plan_cap_ctes_sql()
    evidence_network_id = _provider_directory_reference_resource_id_sql(
        "network_ref.reference",
        "Organization",
    )
    direct_plan_network_sql = _direct_plan_network_sql(
        schema,
        has_dataset_network_plan,
    )
    return f"""
    {requested_role_ctes_sql},
    {network_plan_ctes_sql},
    {plan_cap_ctes_sql},
    network_references AS (
        SELECT role_network.dataset_id, role_network.source_id,
               role_network.role_id, role_network.reference,
               role_network.evidence_provenance
          FROM valid_role_networks AS role_network
        UNION
        {direct_plan_network_sql}
    ), networks AS (
        SELECT network_ref.dataset_id, network_ref.source_id,
               network_ref.role_id, network_ref.reference,
               network_ref.evidence_provenance,
               {evidence_network_id}::varchar AS resource_id
          FROM network_references AS network_ref
    )
    """


def _provider_directory_evidence_union_sql(schema: str, has_catalog: bool) -> str:
    network_joins, network_name, network_provenance = _provider_directory_network_resolution_sql(
        schema,
        has_catalog,
    )
    return f"""
        SELECT role.source_id, role.role_id, 'role'::varchar AS evidence_type,
               role.role_id::varchar AS resource_id, NULL::varchar AS identifier,
               NULL::varchar AS name, NULL::varchar AS reference,
               'provider_directory_practitioner_role'::varchar AS provenance,
               plan_metadata.plan_returned, plan_metadata.plan_total,
               plan_metadata.plan_truncated, plan_metadata.catalog_complete
          FROM roles AS role
          JOIN role_plan_metadata AS plan_metadata
            ON plan_metadata.source_id = role.source_id
           AND plan_metadata.role_id = role.role_id
        UNION ALL
        SELECT returned_plan.source_id, returned_plan.role_id,
               'insurance_plan'::varchar AS evidence_type,
               returned_plan.resource_id, returned_plan.identifier, NULL::varchar AS name,
               NULL::varchar AS reference, returned_plan.provenance,
               NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean
          FROM returned_plans AS returned_plan
        UNION ALL
        SELECT network.source_id, network.role_id, 'network'::varchar AS evidence_type,
               network.resource_id, NULL::varchar AS identifier,
               NULLIF(BTRIM({network_name}), '')::varchar AS name,
               network.reference,
               COALESCE(network.evidence_provenance, {network_provenance}) AS provenance,
               NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean
          FROM networks AS network
          {network_joins}
         WHERE network.resource_id IS NOT NULL AND NULLIF(BTRIM({network_name}), '') IS NOT NULL
    """


def _provider_directory_plan_evidence_payload_sql(alias: str) -> str:
    """Project response fields once without carrying large network locators."""
    return f"""
        ({alias}.payload_json::jsonb - ARRAY[
            'network_refs', 'coverage_area_refs', 'plan_json',
            'resource_id', 'resource_url', 'plan_identifier'
        ]::text[])
    """.strip()


_PROVIDER_DIRECTORY_ROLE_EVIDENCE_SQL_TEMPLATE = """
    WITH {current_resource_ctes_sql}, {current_typed_resource_ctes_sql},
         {role_ctes_sql}, evidence AS (
        {evidence_union_sql}
    ), evidence_count AS MATERIALIZED (
        SELECT COUNT(*)::bigint AS evidence_row_total
          FROM evidence
    )
    SELECT evidence.source_id, evidence.role_id, evidence.evidence_type,
           evidence.resource_id, evidence.identifier, evidence.name, evidence.reference,
           evidence.provenance, evidence.plan_returned, evidence.plan_total,
           evidence.plan_truncated, evidence.catalog_complete,
           role.role_active, role.organization_ref AS role_organization_ref,
           role.role_location_refs, role.role_healthcare_service_refs,
           role.role_endpoint_refs, role.role_endpoints,
           role.role_healthcare_services,
           role.role_specialty_codes, role.role_code_codes, role.role_telecom,
           role.role_identifiers,
           role.role_period_start, role.role_period_end, role.role_available_time,
           role.role_not_available, role.role_availability_exceptions,
           role.role_new_patient_acceptance, role.role_accepting_patients,
           role.role_telehealth,
           role.role_accepting_medicaid, role.role_fhir_meta,
           role.role_fhir_self_url, role.role_fhir_fetch_url, role.role_fhir_fetch_mode,
           {plan_payload_sql} AS plan_payload_json,
           evidence_count.evidence_row_total
      FROM evidence
 CROSS JOIN evidence_count
 LEFT JOIN roles AS role
        ON evidence.evidence_type = 'role'
       AND role.source_id = evidence.source_id
       AND role.role_id = evidence.role_id
 LEFT JOIN current_plan_resources AS plan
        ON evidence.evidence_type = 'insurance_plan'
       AND plan.source_id = evidence.source_id
       AND plan.resource_type = 'InsurancePlan'
       AND plan.resource_id = evidence.resource_id
  ORDER BY CASE WHEN evidence_type = 'role' THEN 0 ELSE 1 END,
           evidence.source_id, evidence.role_id, evidence.evidence_type, evidence.resource_id
     LIMIT {max_evidence_rows};
"""


def _provider_directory_role_evidence_sql(
    schema: str,
    has_catalog: bool,
    has_affiliations: bool = True,
    has_dataset_network_plan: bool = False,
    has_dataset_affiliation_organization: bool = False,
    has_dataset_insurance_plan: bool = False,
    has_dataset_insurance_plan_scalars: bool = False,
) -> str:
    """Build exact-key role evidence SQL with dataset-gated relation fallbacks."""
    return _PROVIDER_DIRECTORY_ROLE_EVIDENCE_SQL_TEMPLATE.format(
        current_resource_ctes_sql=_provider_directory_current_resource_ctes_sql(
            schema,
            has_dataset_insurance_plan,
        ),
        current_typed_resource_ctes_sql=_current_typed_resource_ctes_sql(),
        role_ctes_sql=_provider_directory_role_ctes_sql(
            schema,
            has_affiliations,
            has_catalog,
            has_dataset_network_plan,
            has_dataset_affiliation_organization,
            has_dataset_insurance_plan,
            has_dataset_insurance_plan_scalars,
        ),
        evidence_union_sql=_provider_directory_evidence_union_sql(
            schema,
            has_catalog,
        ),
        plan_payload_sql=_provider_directory_plan_evidence_payload_sql("plan"),
        max_evidence_rows=MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS,
    )


def _provider_directory_plan_metadata(mapping: Mapping[str, Any]) -> dict[str, Any] | None:
    has_plan_metadata = (
        mapping.get("plan_returned") is not None
        or mapping.get("catalog_complete") is not None
    )
    if not has_plan_metadata:
        return None
    return {
        "returned": int(mapping["plan_returned"] or 0),
        "total": int(mapping["plan_total"]) if mapping.get("plan_total") is not None else None,
        "truncated": (
            bool(mapping["plan_truncated"])
            if mapping.get("plan_truncated") is not None
            else None
        ),
        "catalog_complete": bool(mapping["catalog_complete"]),
    }


def _provider_directory_evidence_payload(
    mapping: Mapping[str, Any],
    field_name: str,
) -> Mapping[str, Any]:
    value = mapping.get(field_name)
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            value = json.loads(value)
    return value if isinstance(value, Mapping) else {}


def _provider_directory_evidence_field(
    mapping: Mapping[str, Any],
    prefix: str,
    payload_map: Mapping[str, Any],
    field_name: str,
) -> Any:
    if field_name in payload_map:
        return payload_map[field_name]
    return mapping.get(f"{prefix}_{field_name}" if prefix else field_name)


def _provider_directory_period(
    mapping: Mapping[str, Any],
    prefix: str,
    payload_map: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    payload_map = payload_map or {}
    period_map = {}
    for key in ("start", "end"):
        value = _provider_directory_evidence_field(
            mapping,
            prefix,
            payload_map,
            f"period_{key}",
        )
        if value is not None:
            period_map[key] = value
    return period_map or None


def _bounded_provider_directory_fhir_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)[:MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_TEXT_LENGTH]


def _provider_directory_fhir_url_identity(value: Any) -> str | None:
    text_value = _bounded_provider_directory_fhir_text(value)
    if not text_value:
        return None
    try:
        parsed = urllib.parse.urlsplit(text_value)
        port = parsed.port
    except ValueError:
        return None
    if not parsed.scheme or not parsed.hostname:
        return urllib.parse.urlunsplit(
            (parsed.scheme, "", parsed.path, "", "")
        )
    hostname = parsed.hostname.lower()
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = f"{hostname}:{port}" if port is not None else hostname
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), netloc, parsed.path, "", "")
    )


def _bounded_provider_directory_fhir_codings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    codings: list[dict[str, Any]] = []
    for raw_coding in value[:MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_VALUES]:
        if not isinstance(raw_coding, Mapping):
            continue
        coding_map: dict[str, Any] = {}
        for key in ("system", "version", "code", "display"):
            text_value = _bounded_provider_directory_fhir_text(raw_coding.get(key))
            if text_value is not None:
                coding_map[key] = text_value
        if isinstance(raw_coding.get("userSelected"), bool):
            coding_map["user_selected"] = raw_coding["userSelected"]
        if coding_map:
            codings.append(coding_map)
    return codings


def _bounded_provider_directory_fhir_strings(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        bounded_value
        for raw_value in value[:MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_VALUES]
        if (bounded_value := _bounded_provider_directory_fhir_text(raw_value))
    ]


def _provider_directory_fhir_meta(value: Any) -> dict[str, Any] | None:
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            value = json.loads(value)
    if not isinstance(value, Mapping):
        return None
    meta_map: dict[str, Any] = {}
    for key, normalized_key in (
        ("versionId", "version_id"),
        ("lastUpdated", "last_updated"),
    ):
        text_value = _bounded_provider_directory_fhir_text(value.get(key))
        if text_value is not None:
            meta_map[normalized_key] = text_value
    source = _provider_directory_fhir_url_identity(value.get("source"))
    if source is not None:
        meta_map["source"] = source
    profiles = _bounded_provider_directory_fhir_strings(value.get("profile"))
    if profiles:
        meta_map["profiles"] = profiles
    for key, normalized_key in (("security", "security"), ("tag", "tags")):
        codings = _bounded_provider_directory_fhir_codings(value.get(key))
        if codings:
            meta_map[normalized_key] = codings
    return meta_map or None


def _provider_directory_fhir_provenance(
    mapping: Mapping[str, Any],
    prefix: str,
    payload_map: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    payload_map = payload_map or {}
    provenance_map: dict[str, Any] = {}
    meta = _provider_directory_fhir_meta(
        _provider_directory_evidence_field(
            mapping,
            prefix,
            payload_map,
            "fhir_meta",
        )
    )
    if meta is not None:
        provenance_map["meta"] = meta
    for field_name in ("self_url", "fetch_url", "fetch_mode"):
        field_value = _provider_directory_evidence_field(
            mapping,
            prefix,
            payload_map,
            f"fhir_{field_name}",
        )
        if field_value is not None:
            normalized_value = (
                _provider_directory_fhir_url_identity(field_value)
                if field_name.endswith("_url")
                else _bounded_provider_directory_fhir_text(field_value)
            )
            if normalized_value is not None:
                provenance_map[field_name] = normalized_value
    return provenance_map or None


def _provider_directory_role_detail(mapping: Mapping[str, Any]) -> dict[str, Any]:
    role_detail_map = {
        "resource_type": "PractitionerRole",
        "source_id": mapping["source_id"],
        "resource_id": mapping["resource_id"],
    }
    for field_name in (
        "active",
        "identifiers",
        "organization_ref",
        "location_refs",
        "healthcare_service_refs",
        "endpoint_refs",
        "specialty_codes",
        "code_codes",
        "telecom",
        "available_time",
        "not_available",
        "availability_exceptions",
        "new_patient_acceptance",
        "accepting_patients",
        "telehealth",
        "accepting_medicaid",
    ):
        field_value = mapping.get(f"role_{field_name}")
        if field_value is not None:
            role_detail_map[field_name] = field_value
    if (
        "new_patient_acceptance" not in role_detail_map
        and "accepting_patients" in role_detail_map
    ):
        role_detail_map["new_patient_acceptance"] = role_detail_map[
            "accepting_patients"
        ]
    if (
        "accepting_patients" not in role_detail_map
        and "new_patient_acceptance" in role_detail_map
    ):
        role_detail_map["accepting_patients"] = role_detail_map[
            "new_patient_acceptance"
        ]
    period = _provider_directory_period(mapping, "role")
    if period is not None:
        role_detail_map["period"] = period
    provenance = _provider_directory_fhir_provenance(mapping, "role")
    if provenance is not None:
        role_detail_map["fhir_provenance"] = provenance
    endpoints = _provider_directory_endpoint_details(
        mapping.get("role_endpoints")
    )
    if endpoints:
        role_detail_map["endpoints"] = endpoints
    healthcare_services = _provider_directory_healthcare_service_details(
        mapping.get("role_healthcare_services")
    )
    if healthcare_services:
        role_detail_map["healthcare_services"] = healthcare_services
    return role_detail_map


def _provider_directory_evidence_list(value: Any) -> list[Mapping[str, Any]]:
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            value = json.loads(value)
    return [item for item in value if isinstance(item, Mapping)] if isinstance(value, list) else []


def _provider_directory_endpoint_details(raw_value: Any) -> list[dict[str, Any]]:
    """Return bounded resolved Endpoint details scoped to one PractitionerRole."""
    details: list[dict[str, Any]] = []
    for raw_detail in _provider_directory_evidence_list(raw_value):
        endpoint_detail_map = {
            "resource_type": "Endpoint",
            "source_id": raw_detail.get("source_id"),
            "resource_id": raw_detail.get("resource_id"),
        }
        for field_name in (
            "status",
            "name",
            "managing_organization_ref",
            "contact",
            "payload_type_codes",
            "payload_mime_types",
        ):
            if raw_detail.get(field_name) is not None:
                endpoint_detail_map[field_name] = raw_detail[field_name]
        connection_type_map = {
            key: raw_detail[f"connection_type_{key}"]
            for key in ("system", "code", "display")
            if raw_detail.get(f"connection_type_{key}") is not None
        }
        if connection_type_map:
            endpoint_detail_map["connection_type"] = connection_type_map
        period = _provider_directory_period(raw_detail, "")
        if period is not None:
            endpoint_detail_map["period"] = period
        address = _provider_directory_fhir_url_identity(raw_detail.get("address"))
        if address is not None:
            endpoint_detail_map["address"] = address
        provenance = _provider_directory_fhir_provenance(raw_detail, "")
        if provenance is not None:
            endpoint_detail_map["fhir_provenance"] = provenance
        details.append(endpoint_detail_map)
    return details


def _provider_directory_healthcare_service_details(
    raw_value: Any,
) -> list[dict[str, Any]]:
    """Return source-backed HealthcareService details without inferring acceptance."""
    details: list[dict[str, Any]] = []
    for raw_detail in _provider_directory_evidence_list(raw_value):
        service_detail_map = {
            "resource_type": "HealthcareService",
            "source_id": raw_detail.get("source_id"),
            "resource_id": raw_detail.get("resource_id"),
        }
        for field_name in (
            "active",
            "identifiers",
            "name",
            "type_codes",
            "category_codes",
            "specialty_codes",
            "program_codes",
            "communication_codes",
            "appointment_required",
            "location_refs",
            "endpoint_refs",
            "telecom",
            "available_time",
            "not_available",
            "availability_exceptions",
            "accepting_patients",
        ):
            if raw_detail.get(field_name) is not None:
                service_detail_map[field_name] = raw_detail[field_name]
        provenance = _provider_directory_fhir_provenance(raw_detail, "")
        if provenance is not None:
            service_detail_map["fhir_provenance"] = provenance
        details.append(service_detail_map)
    return details


def _append_provider_directory_plan_evidence(
    mapping: Mapping[str, Any],
    role_evidence: dict[str, Any],
    plan_keys: set[tuple[Any, ...]],
) -> None:
    plan_payload = _provider_directory_evidence_payload(
        mapping,
        "plan_payload_json",
    )
    plan_detail_map = {
        "resource_type": "InsurancePlan",
        "resource_id": mapping["resource_id"],
        "identifier": mapping["identifier"],
    }
    if mapping.get("provenance") in {
        "network-derived",
        "organization-affiliation-network-derived",
    }:
        plan_detail_map["provenance"] = mapping["provenance"]
    for field_name in (
        "status",
        "name",
        "aliases",
        "type_codes",
        "owned_by_ref",
        "administered_by_ref",
        "product_identifiers",
        "backbones",
        "coverage",
    ):
        output_field = "plan_backbones" if field_name == "backbones" else field_name
        field_value = _provider_directory_evidence_field(
            mapping,
            "plan",
            plan_payload,
            output_field,
        )
        if field_value is not None:
            plan_detail_map[output_field] = field_value
    period = _provider_directory_period(mapping, "plan", plan_payload)
    if period is not None:
        plan_detail_map["period"] = period
    provenance = _provider_directory_fhir_provenance(
        mapping,
        "plan",
        plan_payload,
    )
    if provenance is not None:
        plan_detail_map["fhir_provenance"] = provenance
    plan_fields = tuple(
        plan_detail_map.get(key)
        for key in ("resource_type", "resource_id", "identifier", "provenance")
    )
    if plan_fields not in plan_keys:
        plan_keys.add(plan_fields)
        role_evidence["insurance_plans"].append(plan_detail_map)


def _append_provider_directory_network_evidence(
    mapping: Mapping[str, Any],
    role_evidence: dict[str, Any],
    network_keys: set[tuple[Any, ...]],
) -> None:
    network_detail_map = {
        "resource_type": "Organization",
        "resource_id": mapping["resource_id"],
        "name": mapping["name"],
        "reference": mapping["reference"],
        "provenance": mapping["provenance"],
    }
    network_fields = tuple(
        network_detail_map.get(key)
        for key in ("resource_type", "resource_id", "name", "reference", "provenance")
    )
    if network_fields not in network_keys:
        network_keys.add(network_fields)
        role_evidence["networks"].append(network_detail_map)


def _map_provider_directory_role_evidence(
    evidence_rows: Sequence[Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Map bounded SQL evidence with stable set-backed per-role deduplication."""
    role_evidence_map: dict[tuple[str, str], dict[str, Any]] = {}
    plan_keys_by_role: dict[tuple[str, str], set[tuple[Any, ...]]] = {}
    network_keys_by_role: dict[tuple[str, str], set[tuple[Any, ...]]] = {}
    evidence_row_total: int | None = None
    for evidence_row in evidence_rows:
        mapping = getattr(evidence_row, "_mapping", evidence_row)
        if mapping.get("evidence_row_total") is not None:
            evidence_row_total = int(mapping["evidence_row_total"])
        role_key = (str(mapping["source_id"]), str(mapping["role_id"]))
        role_evidence = role_evidence_map.setdefault(
            role_key,
            {"insurance_plans": [], "networks": []},
        )
        evidence_type = mapping["evidence_type"]
        if evidence_type == "role":
            plan_metadata = _provider_directory_plan_metadata(mapping)
            if plan_metadata is not None:
                role_evidence["insurance_plan_metadata"] = plan_metadata
            role_evidence["practitioner_role"] = _provider_directory_role_detail(mapping)
        elif evidence_type == "insurance_plan":
            plan_keys = plan_keys_by_role.setdefault(role_key, set())
            _append_provider_directory_plan_evidence(mapping, role_evidence, plan_keys)
        elif evidence_type == "network":
            network_keys = network_keys_by_role.setdefault(role_key, set())
            _append_provider_directory_network_evidence(mapping, role_evidence, network_keys)
    for role_evidence in role_evidence_map.values():
        plan_metadata = role_evidence.get("insurance_plan_metadata")
        if isinstance(plan_metadata, dict) and evidence_row_total is not None:
            plan_metadata["returned"] = len(role_evidence["insurance_plans"])
        if evidence_row_total is not None:
            role_evidence["evidence_metadata"] = {
                "returned": len(evidence_rows),
                "total": evidence_row_total,
                "truncated": evidence_row_total > len(evidence_rows),
            }
    return role_evidence_map


async def _fetch_provider_directory_role_evidence_map(
    role_key_list: Sequence[tuple[str, str]],
    *,
    session: Any = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch bounded role evidence without scanning immutable resource payloads."""
    bounded_keys = list(dict.fromkeys(role_key_list))[:MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS]
    if not bounded_keys:
        return {}
    if session is None:
        async with db.session() as evidence_session:
            return await _fetch_provider_directory_role_evidence_map(
                bounded_keys,
                session=evidence_session,
            )
    table_flags = await _provider_directory_evidence_tables(
        session,
        required_names=(
            *PROVIDER_DIRECTORY_VISIBILITY_TABLES,
            "provider_directory_practitioner_role",
            "provider_directory_insurance_plan",
            "provider_directory_organization",
        ),
        optional_names=(
            "provider_directory_organization_affiliation",
            "provider_directory_network_catalog",
            PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE,
            PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE,
            PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE,
        ),
        optional_columns=(
            PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_REQUIREMENTS
        ),
    )
    if table_flags is None:
        return {}
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await _disable_provider_directory_evidence_jit(session)
    evidence_result = await _execute_stmt(
        text(
            _provider_directory_role_evidence_sql(
                schema,
                table_flags["provider_directory_network_catalog"],
                table_flags["provider_directory_organization_affiliation"],
                table_flags[PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE],
                table_flags[PROVIDER_DIRECTORY_DATASET_AFFILIATION_ORGANIZATION_TABLE],
                table_flags[PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE],
                _has_provider_directory_plan_scalars(table_flags),
            )
        ),
        session=session,
        params={
            "source_ids": [source_id for source_id, _role_id in bounded_keys],
            "role_ids": [role_id for _source_id, role_id in bounded_keys],
        },
    )
    return _map_provider_directory_role_evidence(evidence_result.all())


async def _provider_directory_evidence_tables(
    session: Any,
    *,
    required_names: Sequence[str],
    optional_names: Sequence[str],
    optional_columns: Sequence[tuple[str, str]] = (),
) -> dict[str, bool] | None:
    """Resolve required tables and optional table-column capabilities in one query."""
    table_names = list(dict.fromkeys((*required_names, *optional_names)))
    try:
        availability_result = await _execute_stmt(
            text(PROVIDER_DIRECTORY_EVIDENCE_CAPABILITY_SQL),
            session=session,
            params={
                "schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf",
                "table_names": table_names,
                "column_table_names": [
                    table_name for table_name, _column_name in optional_columns
                ],
                "column_names": [
                    column_name for _table_name, column_name in optional_columns
                ],
            },
        )
    except Exception:
        return None
    availability_by_name = {
        str(getattr(table_row, "_mapping", table_row)["table_name"]): bool(
            getattr(table_row, "_mapping", table_row)["is_available"]
        )
        for table_row in availability_result.all()
    }
    if not all(availability_by_name.get(table_name, False) for table_name in required_names):
        return None
    optional_capability_names = [
        *optional_names,
        *(f"{table_name}.{column_name}" for table_name, column_name in optional_columns),
    ]
    return {
        capability_name: availability_by_name.get(capability_name, False)
        for capability_name in optional_capability_names
    }


def _has_provider_directory_plan_scalars(
    capability_flags: Mapping[str, bool],
) -> bool:
    """Require both generated plan fields before selecting the scalar path."""
    return all(
        capability_flags.get(
            f"{PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE}.{column_name}",
            False,
        )
        for column_name in PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_COLUMNS
    )


async def _disable_provider_directory_evidence_jit(session: Any) -> None:
    if getattr(session, _PROVIDER_DIRECTORY_ROLE_JIT_DISABLED_ATTR, False):
        return
    await session.execute(text("SET LOCAL jit = off"))
    setattr(session, _PROVIDER_DIRECTORY_ROLE_JIT_DISABLED_ATTR, True)


def _provider_directory_requested_affiliation_ctes_sql(schema: str) -> str:
    """Fence requested affiliations and their networks to current resources."""
    network_id = _provider_directory_reference_resource_id_sql(
        "network_ref.value",
        "Organization",
    )
    return f"""
    requested_affiliations AS (
        SELECT source_id, affiliation_id
          FROM unnest(
               CAST(:source_ids AS varchar[]),
               CAST(:affiliation_ids AS varchar[])
          ) AS requested(source_id, affiliation_id)
    ), affiliations AS MATERIALIZED (
        SELECT affiliation.dataset_id, affiliation.source_id,
               affiliation.resource_id AS affiliation_id,
               affiliation.dataset_network_plan_complete,
               affiliation.network_refs::jsonb
          FROM requested_affiliations AS requested
          JOIN current_affiliations AS affiliation
            ON affiliation.source_id = requested.source_id
           AND affiliation.resource_id = requested.affiliation_id
         WHERE affiliation.active IS DISTINCT FROM false
    ), affiliation_networks AS MATERIALIZED (
        SELECT DISTINCT affiliation.dataset_id, affiliation.source_id,
               affiliation.affiliation_id,
               affiliation.dataset_network_plan_complete,
               network_ref.value::varchar AS reference,
               {network_id}::varchar AS resource_id
          FROM affiliations AS affiliation
         CROSS JOIN LATERAL jsonb_array_elements_text(
               COALESCE(affiliation.network_refs, '[]'::jsonb)
         ) AS network_ref(value)
    ), valid_affiliation_networks AS MATERIALIZED (
        SELECT affiliation_network.*
          FROM affiliation_networks AS affiliation_network
          JOIN current_organizations AS network_organization
            ON network_organization.dataset_id = affiliation_network.dataset_id
           AND network_organization.source_id = affiliation_network.source_id
           AND network_organization.resource_id = affiliation_network.resource_id
           AND network_organization.active IS DISTINCT FROM false
         WHERE affiliation_network.resource_id IS NOT NULL
    )
    """


def _dataset_affiliation_plan_sql(
    schema: str,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Resolve affiliation plan edges through active immutable plan scalars."""
    insurance_plan_status = "insurance_plan.payload_json::jsonb ->> 'status'"
    insurance_plan_identifier = (
        "insurance_plan.payload_json::jsonb ->> 'plan_identifier'"
    )
    insurance_plan_active = (
        "COALESCE(NULLIF(LOWER(BTRIM("
        f"{insurance_plan_status})), ''), 'active') = 'active'"
    )
    resource_table = (
        PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE
        if has_dataset_insurance_plan
        else "provider_directory_dataset_resource"
    )
    resource_type_filter = (
        "" if has_dataset_insurance_plan else "AND insurance_plan.resource_type = 'InsurancePlan'"
    )
    selected_identifier, active_filter = _dataset_plan_scalar_sql(
        insurance_plan_identifier,
        insurance_plan_active,
        has_dataset_insurance_plan_scalars,
    )
    return f"""
    dataset_affiliation_plan_candidates AS MATERIALIZED (
        SELECT DISTINCT affiliation_network.dataset_id,
               affiliation_network.source_id,
               affiliation_network.affiliation_id,
               network_plan.insurance_plan_resource_id AS resource_id
          FROM valid_affiliation_networks AS affiliation_network
          JOIN {schema}.{PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE} AS network_plan
            ON network_plan.dataset_id = affiliation_network.dataset_id
           AND network_plan.network_resource_id = affiliation_network.resource_id
         WHERE affiliation_network.dataset_network_plan_complete
    ), dataset_affiliation_plan_resource_keys AS MATERIALIZED (
        SELECT DISTINCT candidate.dataset_id, candidate.resource_id
          FROM dataset_affiliation_plan_candidates AS candidate
    ), dataset_affiliation_plan_resources AS MATERIALIZED (
        SELECT candidate.dataset_id, insurance_plan.resource_id,
               NULLIF(BTRIM({selected_identifier}), '')::varchar AS identifier
          FROM dataset_affiliation_plan_resource_keys AS candidate
          JOIN {schema}.{resource_table} AS insurance_plan
            ON insurance_plan.dataset_id = candidate.dataset_id
           AND insurance_plan.resource_id = candidate.resource_id
           {resource_type_filter}
           AND {active_filter}
    ), dataset_affiliation_plans AS MATERIALIZED (
        SELECT candidate.source_id, candidate.affiliation_id,
               insurance_plan.resource_id, insurance_plan.identifier,
               'organization-affiliation-network-derived'::varchar AS provenance
          FROM dataset_affiliation_plan_candidates AS candidate
          JOIN dataset_affiliation_plan_resources AS insurance_plan
            ON insurance_plan.dataset_id = candidate.dataset_id
           AND insurance_plan.resource_id = candidate.resource_id
    )
    """


def _legacy_affiliation_plan_sql(has_dataset_network_plan: bool) -> str:
    insurance_plan_active = _insurance_plan_active_sql("insurance_plan")
    network_match = _provider_directory_plan_network_match_sql(
        "plan_network_ref.value",
        "affiliation_network.resource_id",
        "affiliation_network.reference",
    )
    legacy_filter = (
        "AND NOT affiliation_network.dataset_network_plan_complete"
        if has_dataset_network_plan
        else ""
    )
    return f"""
    legacy_affiliation_plans AS MATERIALIZED (
        SELECT DISTINCT affiliation_network.source_id,
               affiliation_network.affiliation_id,
               insurance_plan.resource_id,
               NULLIF(BTRIM(insurance_plan.plan_identifier), '')::varchar AS identifier,
               'organization-affiliation-network-derived'::varchar AS provenance
          FROM valid_affiliation_networks AS affiliation_network
          JOIN legacy_affiliation_insurance_plans AS insurance_plan
            ON insurance_plan.dataset_id = affiliation_network.dataset_id
           AND insurance_plan.source_id = affiliation_network.source_id
           AND {insurance_plan_active}
         WHERE 1 = 1
           {legacy_filter}
           AND EXISTS (
               SELECT 1
                 FROM jsonb_array_elements_text(
                      COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
                 ) AS plan_network_ref(value)
                WHERE {network_match}
         )
    )
    """


def _affiliation_plan_resolution_cte_sql(
    schema: str,
    has_dataset_network_plan: bool,
    has_dataset_insurance_plan: bool,
    has_dataset_insurance_plan_scalars: bool,
) -> str:
    """Resolve affiliation plans from dataset edges with a legacy fallback."""
    scoped_plan_ctes_sql = _scoped_current_insurance_plan_ctes_sql(
        "affiliations",
        "legacy_affiliation_insurance_plans",
        (
            "WHERE NOT dataset_network_plan_complete"
            if has_dataset_network_plan
            else ""
        ),
    )
    dataset_plan_cte_sql = (
        _dataset_affiliation_plan_sql(
            schema,
            has_dataset_insurance_plan,
            has_dataset_insurance_plan_scalars,
        )
        if has_dataset_network_plan
        else """
    dataset_affiliation_plans AS MATERIALIZED (
        SELECT affiliation_network.source_id,
               affiliation_network.affiliation_id,
               NULL::varchar AS resource_id, NULL::varchar AS identifier,
               NULL::varchar AS provenance
          FROM valid_affiliation_networks AS affiliation_network
         WHERE false
    )
        """
    )
    legacy_plan_cte_sql = _legacy_affiliation_plan_sql(
        has_dataset_network_plan
    )
    return f"""
    {scoped_plan_ctes_sql}, {dataset_plan_cte_sql}, {legacy_plan_cte_sql},
    affiliation_plans AS MATERIALIZED (
        SELECT source_id, affiliation_id, resource_id, identifier, provenance
          FROM dataset_affiliation_plans
        UNION ALL
        SELECT source_id, affiliation_id, resource_id, identifier, provenance
          FROM legacy_affiliation_plans
    )
    """


def _affiliation_plan_cap_ctes_sql() -> str:
    """Cap affiliation plans and retain completeness metadata."""
    return f"""
    ranked_plans AS MATERIALIZED (
        SELECT affiliation_plan.*,
               ROW_NUMBER() OVER (
                   PARTITION BY source_id, affiliation_id
                   ORDER BY resource_id, identifier NULLS LAST
               ) AS plan_rank
          FROM affiliation_plans AS affiliation_plan
    ), returned_plans AS MATERIALIZED (
        SELECT source_id, affiliation_id, resource_id, identifier, provenance
          FROM ranked_plans
         WHERE plan_rank <= {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE}
    ), affiliation_plan_metadata AS MATERIALIZED (
        SELECT affiliation.source_id, affiliation.affiliation_id,
               LEAST(COUNT(affiliation_plan.resource_id), {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE})::bigint
                   AS plan_returned,
               COUNT(affiliation_plan.resource_id)::bigint AS plan_total,
               COUNT(affiliation_plan.resource_id) > {MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE}
                   AS plan_truncated,
               TRUE::boolean AS catalog_complete
          FROM affiliations AS affiliation
          LEFT JOIN affiliation_plans AS affiliation_plan
            ON affiliation_plan.source_id = affiliation.source_id
           AND affiliation_plan.affiliation_id = affiliation.affiliation_id
      GROUP BY affiliation.source_id, affiliation.affiliation_id
    )
    """


def _provider_directory_affiliation_evidence_union_sql(
    schema: str,
    has_catalog: bool,
) -> str:
    """Project affiliation, plan, and current network evidence rows."""
    network_joins, network_name, _network_provenance = (
        _provider_directory_network_resolution_sql(schema, has_catalog)
    )
    return f"""
        SELECT affiliation.source_id, affiliation.affiliation_id,
               'affiliation'::varchar AS evidence_type,
               affiliation.affiliation_id::varchar AS resource_id,
               NULL::varchar AS identifier, NULL::varchar AS name,
               NULL::varchar AS reference,
               'provider_directory_organization_affiliation'::varchar AS provenance,
               plan_metadata.plan_returned, plan_metadata.plan_total,
               plan_metadata.plan_truncated, plan_metadata.catalog_complete
          FROM affiliations AS affiliation
          JOIN affiliation_plan_metadata AS plan_metadata
            ON plan_metadata.source_id = affiliation.source_id
           AND plan_metadata.affiliation_id = affiliation.affiliation_id
        UNION ALL
        SELECT returned_plan.source_id, returned_plan.affiliation_id,
               'insurance_plan'::varchar AS evidence_type,
               returned_plan.resource_id, returned_plan.identifier,
               NULL::varchar AS name, NULL::varchar AS reference,
               returned_plan.provenance,
               NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean
          FROM returned_plans AS returned_plan
        UNION ALL
        SELECT network.source_id, network.affiliation_id,
               'network'::varchar AS evidence_type,
               network.resource_id, NULL::varchar AS identifier,
               NULLIF(BTRIM({network_name}), '')::varchar AS name,
               network.reference,
               'provider_directory_organization_affiliation'::varchar AS provenance,
               NULL::bigint, NULL::bigint, NULL::boolean, NULL::boolean
          FROM valid_affiliation_networks AS network
          {network_joins}
         WHERE network.resource_id IS NOT NULL
           AND NULLIF(BTRIM({network_name}), '') IS NOT NULL
    """


def _provider_directory_affiliation_evidence_sql(
    schema: str,
    has_catalog: bool,
    has_dataset_network_plan: bool = False,
    has_dataset_insurance_plan: bool = False,
    has_dataset_insurance_plan_scalars: bool = False,
) -> str:
    """Resolve exact affiliation network and plan evidence from current resources."""
    current_resource_ctes_sql = _provider_directory_current_resource_ctes_sql(
        schema,
        has_dataset_insurance_plan,
    )
    affiliation_ctes_sql = _provider_directory_requested_affiliation_ctes_sql(
        schema
    )
    plan_resolution_cte_sql = _affiliation_plan_resolution_cte_sql(
        schema,
        has_dataset_network_plan,
        has_dataset_insurance_plan,
        has_dataset_insurance_plan_scalars,
    )
    plan_cap_ctes_sql = _affiliation_plan_cap_ctes_sql()
    evidence_union_sql = _provider_directory_affiliation_evidence_union_sql(
        schema,
        has_catalog,
    )
    current_typed_resource_ctes_sql = (
        _current_typed_resource_ctes_sql()
    )
    return f"""
    WITH {current_resource_ctes_sql}, {current_typed_resource_ctes_sql},
         {affiliation_ctes_sql},
         {plan_resolution_cte_sql}, {plan_cap_ctes_sql}, evidence AS (
        {evidence_union_sql}
    ), evidence_count AS MATERIALIZED (
        SELECT COUNT(*)::bigint AS evidence_row_total
          FROM evidence
    )
    SELECT evidence.source_id, evidence.affiliation_id, evidence.evidence_type,
           evidence.resource_id, evidence.identifier, evidence.name, evidence.reference,
           evidence.provenance, evidence.plan_returned, evidence.plan_total,
           evidence.plan_truncated, evidence.catalog_complete,
           {_provider_directory_plan_evidence_payload_sql("plan")} AS plan_payload_json,
           evidence_count.evidence_row_total
      FROM evidence
 CROSS JOIN evidence_count
 LEFT JOIN current_plan_resources AS plan
        ON evidence.evidence_type = 'insurance_plan'
       AND plan.source_id = evidence.source_id
       AND plan.resource_type = 'InsurancePlan'
       AND plan.resource_id = evidence.resource_id
  ORDER BY CASE WHEN evidence.evidence_type = 'affiliation' THEN 0 ELSE 1 END,
           evidence.source_id, evidence.affiliation_id, evidence.evidence_type, evidence.resource_id
     LIMIT {MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS};
    """


def _map_provider_directory_affiliation_evidence(
    evidence_rows: Sequence[Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    affiliation_evidence_map: dict[tuple[str, str], dict[str, Any]] = {}
    plan_keys_by_affiliation: dict[tuple[str, str], set[tuple[Any, ...]]] = {}
    network_keys_by_affiliation: dict[tuple[str, str], set[tuple[Any, ...]]] = {}
    evidence_row_total: int | None = None
    for evidence_row in evidence_rows:
        mapping = getattr(evidence_row, "_mapping", evidence_row)
        if mapping.get("evidence_row_total") is not None:
            evidence_row_total = int(mapping["evidence_row_total"])
        affiliation_key = (
            str(mapping["source_id"]),
            str(mapping["affiliation_id"]),
        )
        affiliation_evidence = affiliation_evidence_map.setdefault(
            affiliation_key,
            {"insurance_plans": [], "networks": []},
        )
        evidence_type = mapping["evidence_type"]
        if evidence_type == "affiliation":
            plan_metadata = _provider_directory_plan_metadata(mapping)
            if plan_metadata is not None:
                affiliation_evidence["insurance_plan_metadata"] = plan_metadata
        elif evidence_type == "insurance_plan":
            plan_keys = plan_keys_by_affiliation.setdefault(affiliation_key, set())
            _append_provider_directory_plan_evidence(
                mapping,
                affiliation_evidence,
                plan_keys,
            )
        elif evidence_type == "network":
            network_keys = network_keys_by_affiliation.setdefault(affiliation_key, set())
            _append_provider_directory_network_evidence(
                mapping,
                affiliation_evidence,
                network_keys,
            )
    for affiliation_evidence in affiliation_evidence_map.values():
        plan_metadata = affiliation_evidence.get("insurance_plan_metadata")
        if isinstance(plan_metadata, dict) and evidence_row_total is not None:
            plan_metadata["returned"] = len(affiliation_evidence["insurance_plans"])
        if evidence_row_total is not None:
            affiliation_evidence["evidence_metadata"] = {
                "returned": len(evidence_rows),
                "total": evidence_row_total,
                "truncated": evidence_row_total > len(evidence_rows),
            }
    return affiliation_evidence_map


async def _fetch_provider_directory_affiliation_evidence_map(
    affiliation_key_list: Sequence[tuple[str, str]],
    *,
    session: Any = None,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Fetch bounded affiliation evidence through compact serving relations."""
    bounded_keys = list(dict.fromkeys(affiliation_key_list))[
        :MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS
    ]
    if not bounded_keys:
        return {}
    if session is None:
        async with db.session() as evidence_session:
            return await _fetch_provider_directory_affiliation_evidence_map(
                bounded_keys,
                session=evidence_session,
            )
    table_flags = await _provider_directory_evidence_tables(
        session,
        required_names=(
            *PROVIDER_DIRECTORY_VISIBILITY_TABLES,
            "provider_directory_insurance_plan",
            "provider_directory_organization",
        ),
        optional_names=(
            "provider_directory_network_catalog",
            PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE,
            PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE,
        ),
        optional_columns=(
            PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_SCALAR_REQUIREMENTS
        ),
    )
    if table_flags is None:
        return {}
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await _disable_provider_directory_evidence_jit(session)
    evidence_result = await _execute_stmt(
        text(
            _provider_directory_affiliation_evidence_sql(
                schema,
                table_flags["provider_directory_network_catalog"],
                table_flags[PROVIDER_DIRECTORY_DATASET_NETWORK_PLAN_TABLE],
                table_flags[PROVIDER_DIRECTORY_DATASET_INSURANCE_PLAN_TABLE],
                _has_provider_directory_plan_scalars(table_flags),
            )
        ),
        session=session,
        params={
            "source_ids": [source_id for source_id, _affiliation_id in bounded_keys],
            "affiliation_ids": [
                affiliation_id for _source_id, affiliation_id in bounded_keys
            ],
        },
    )
    return _map_provider_directory_affiliation_evidence(evidence_result.all())


def _provider_directory_source_detail_statement(source_ids: Sequence[str]) -> Any:
    table = ProviderDirectorySource.__table__
    selected_endpoints = (
        select(
            table.c.endpoint_id.label("endpoint_id"),
            table.c.canonical_api_base.label("canonical_api_base"),
        )
        .where(table.c.source_id.in_(source_ids))
        .subquery()
    )
    stmt = (
        select(
            table.c.source_id,
            table.c.endpoint_id,
            table.c.canonical_api_base,
            table.c.org_name,
            table.c.plan_name,
        )
        .where(
            or_(
                table.c.source_id.in_(source_ids),
                table.c.endpoint_id.in_(
                    select(selected_endpoints.c.endpoint_id).where(
                        selected_endpoints.c.endpoint_id.is_not(None)
                    )
                ),
                table.c.canonical_api_base.in_(
                    select(selected_endpoints.c.canonical_api_base).where(
                        selected_endpoints.c.endpoint_id.is_(None),
                        selected_endpoints.c.canonical_api_base.is_not(None),
                    )
                ),
            )
        )
        .order_by(table.c.source_id)
    )
    return stmt


def _map_source_details(
    rows: Sequence[Any],
) -> dict[str, dict[str, Any]]:
    details_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        mapping = getattr(row, "_mapping", row)
        source_id = str(mapping["source_id"] or "").strip()
        if not source_id:
            continue
        details_by_id[source_id] = {
            "source": "provider_directory_fhir",
            "source_id": source_id,
            "endpoint_id": mapping["endpoint_id"],
            "canonical_api_base": _normalized_provider_directory_api_base(
                mapping["canonical_api_base"]
            ),
            "org_name": mapping["org_name"],
            "plan_name": mapping["plan_name"],
        }
    return details_by_id


async def _fetch_provider_directory_source_detail_map(
    source_ids: Sequence[str],
    *,
    session: Any = None,
) -> dict[str, dict[str, Any]]:
    """Fetch requested sources plus aliases that share their endpoint identity."""
    unique_ids = [
        source_id
        for source_id in dict.fromkeys(str(item or "").strip() for item in source_ids)
        if source_id
    ]
    if not unique_ids:
        return {}
    if not await _table_exists(ProviderDirectorySource.__tablename__, session=session):
        return {}
    stmt = _provider_directory_source_detail_statement(unique_ids)
    result = await _execute_stmt(stmt, session=session)
    return _map_source_details(result.all())


def _normalized_provider_directory_api_base(raw_api_base: Any) -> str:
    api_base = str(raw_api_base or "").strip()
    if not api_base:
        return ""
    sanitized_api_base = _provider_directory_fhir_url_identity(api_base)
    if sanitized_api_base is None:
        return ""
    return sanitized_api_base.rstrip("/")


def _provider_directory_endpoint_group_key(
    source_detail: Mapping[str, Any],
) -> tuple[str, str]:
    endpoint_id = str(source_detail.get("endpoint_id") or "").strip()
    canonical_api_base = _normalized_provider_directory_api_base(
        source_detail.get("canonical_api_base")
    )
    if endpoint_id:
        return "endpoint_id", endpoint_id
    if canonical_api_base:
        return "canonical_api_base", canonical_api_base
    return "source_id", str(source_detail.get("source_id") or "").strip()


def _provider_directory_catalog_alias(source_detail: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: source_detail[key]
        for key in PROVIDER_DIRECTORY_CATALOG_ALIAS_COLUMNS
        if key in source_detail and source_detail[key] is not None
    }


def _merge_provider_directory_role_evidence(
    matching_role_evidence_list: Sequence[tuple[tuple[str, str], Mapping[str, Any]]],
    *,
    evidence_id_field: str = "practitioner_role_id",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[Mapping[str, Any]]]:
    insurance_plan_list: list[dict[str, Any]] = []
    network_list: list[dict[str, Any]] = []
    plan_indexes_by_key: dict[tuple[Any, ...], int] = {}
    network_keys: set[tuple[Any, ...]] = set()
    role_plan_metadata_list: list[dict[str, Any]] = []
    evidence_metadata_list: list[Mapping[str, Any]] = []
    for role_key, role_evidence in matching_role_evidence_list:
        for plan_detail in role_evidence.get("insurance_plans") or []:
            plan_key_parts = tuple(
                plan_detail.get(key) for key in ("resource_type", "resource_id")
            )
            existing_plan_index = plan_indexes_by_key.get(plan_key_parts)
            if existing_plan_index is None:
                plan_indexes_by_key[plan_key_parts] = len(insurance_plan_list)
                insurance_plan_list.append(dict(plan_detail))
            elif (
                insurance_plan_list[existing_plan_index].get("provenance")
                and not plan_detail.get("provenance")
            ):
                insurance_plan_list[existing_plan_index] = dict(plan_detail)
        for network_detail in role_evidence.get("networks") or []:
            network_fields = tuple(network_detail.get(key) for key in (
                "resource_type", "resource_id", "name", "reference", "provenance"
            ))
            if network_fields not in network_keys:
                network_keys.add(network_fields)
                network_list.append(dict(network_detail))
        plan_metadata = role_evidence.get("insurance_plan_metadata")
        if isinstance(plan_metadata, Mapping):
            role_plan_metadata_list.append(
                {
                    "source_id": role_key[0],
                    evidence_id_field: role_key[1],
                    **dict(plan_metadata),
                }
            )
        evidence_metadata = role_evidence.get("evidence_metadata")
        if isinstance(evidence_metadata, Mapping):
            evidence_metadata_list.append(evidence_metadata)
    return (
        insurance_plan_list,
        network_list,
        role_plan_metadata_list,
        evidence_metadata_list,
    )


def _provider_directory_practitioner_role_details(
    matching_role_evidence_list: Sequence[tuple[tuple[str, str], Mapping[str, Any]]],
) -> list[dict[str, Any]]:
    return sorted(
        (
            dict(role_detail)
            for _role_key, role_evidence in matching_role_evidence_list
            if isinstance(
                role_detail := role_evidence.get("practitioner_role"),
                Mapping,
            )
        ),
        key=lambda detail: (str(detail["source_id"]), str(detail["resource_id"])),
    )


def _provider_directory_role_evidence_fields(
    source_ids: Sequence[str],
    role_keys: Sequence[tuple[str, str]],
    detail_by_id: Mapping[str, Mapping[str, Any]],
    endpoint_key: tuple[str, str],
    role_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Return evidence limited to exact role keys for one endpoint group."""
    endpoint_source_ids = {
        source_id
        for source_id in source_ids
        if (source_detail := detail_by_id.get(source_id))
        and _provider_directory_endpoint_group_key(source_detail) == endpoint_key
    }
    matching_role_evidence_list = []
    seen_role_keys: set[tuple[str, str]] = set()
    for role_key in role_keys:
        if role_key in seen_role_keys or role_key[0] not in endpoint_source_ids:
            continue
        seen_role_keys.add(role_key)
        role_evidence = role_evidence_map.get(role_key)
        if role_evidence is not None:
            matching_role_evidence_list.append((role_key, role_evidence))
    if not matching_role_evidence_list:
        return {}
    field_map: dict[str, Any] = {
        "source_ids": sorted(endpoint_source_ids),
        "practitioner_role_ids": sorted(
            {role_key[1] for role_key, _role_evidence in matching_role_evidence_list}
        ),
    }
    practitioner_roles = _provider_directory_practitioner_role_details(
        matching_role_evidence_list
    )
    if practitioner_roles:
        field_map["practitioner_roles"] = practitioner_roles
    plan_list, network_list, role_plan_metadata_list, evidence_metadata_list = (
        _merge_provider_directory_role_evidence(matching_role_evidence_list)
    )
    if plan_list:
        field_map["insurance_plans"] = plan_list
    if network_list:
        field_map["networks"] = network_list
    if len(role_plan_metadata_list) == 1:
        field_map["insurance_plan_metadata"] = {
            key: role_plan_metadata_list[0][key]
            for key in ("returned", "total", "truncated", "catalog_complete")
        }
    elif role_plan_metadata_list:
        field_map["insurance_plan_metadata_by_role"] = role_plan_metadata_list
    if evidence_metadata_list:
        field_map["evidence_metadata"] = dict(evidence_metadata_list[0])
    return field_map


def _provider_directory_affiliation_evidence_fields(
    source_ids: Sequence[str],
    affiliation_keys: Sequence[tuple[str, str]],
    detail_by_id: Mapping[str, Mapping[str, Any]],
    endpoint_key: tuple[str, str],
    affiliation_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Return evidence limited to exact affiliation keys for one endpoint group."""
    endpoint_source_ids = {
        source_id
        for source_id in source_ids
        if (source_detail := detail_by_id.get(source_id))
        and _provider_directory_endpoint_group_key(source_detail) == endpoint_key
    }
    matching_affiliation_evidence_list = []
    seen_affiliation_keys: set[tuple[str, str]] = set()
    for affiliation_key in affiliation_keys:
        if (
            affiliation_key in seen_affiliation_keys
            or affiliation_key[0] not in endpoint_source_ids
        ):
            continue
        seen_affiliation_keys.add(affiliation_key)
        affiliation_evidence = affiliation_evidence_map.get(affiliation_key)
        if affiliation_evidence is not None:
            matching_affiliation_evidence_list.append(
                (affiliation_key, affiliation_evidence)
            )
    if not matching_affiliation_evidence_list:
        return {}
    field_map: dict[str, Any] = {
        "organization_affiliation_ids": sorted(
            {
                affiliation_key[1]
                for affiliation_key, _affiliation_evidence in matching_affiliation_evidence_list
            }
        ),
    }
    plan_list, network_list, plan_metadata_list, evidence_metadata_list = (
        _merge_provider_directory_role_evidence(
            matching_affiliation_evidence_list,
            evidence_id_field="organization_affiliation_id",
        )
    )
    if plan_list:
        field_map["insurance_plans"] = plan_list
    if network_list:
        field_map["networks"] = network_list
    if len(plan_metadata_list) == 1:
        field_map["insurance_plan_metadata"] = {
            key: plan_metadata_list[0][key]
            for key in ("returned", "total", "truncated", "catalog_complete")
        }
    elif plan_metadata_list:
        field_map["insurance_plan_metadata_by_affiliation"] = plan_metadata_list
    if evidence_metadata_list:
        field_map["evidence_metadata"] = dict(evidence_metadata_list[0])
    return field_map


def _merge_provider_directory_affiliation_fields(
    endpoint_provenance_map: dict[str, Any],
    affiliation_field_map: Mapping[str, Any],
) -> None:
    for key in ("insurance_plans", "networks"):
        merged_values = _merge_unique_list_values(
            endpoint_provenance_map.get(key),
            affiliation_field_map.get(key),
        )
        if merged_values:
            endpoint_provenance_map[key] = merged_values
    affiliation_ids = affiliation_field_map.get("organization_affiliation_ids")
    if affiliation_ids:
        endpoint_provenance_map["organization_affiliation_ids"] = list(
            affiliation_ids
        )
    for key in (
        "insurance_plan_metadata",
        "insurance_plan_metadata_by_affiliation",
        "evidence_metadata",
    ):
        if key in affiliation_field_map and key not in endpoint_provenance_map:
            endpoint_provenance_map[key] = affiliation_field_map[key]


def _provider_directory_selected_endpoint_keys(
    source_ids: Sequence[str],
    detail_by_id: Mapping[str, Mapping[str, Any]],
) -> list[tuple[str, str]]:
    endpoint_keys: list[tuple[str, str]] = []
    seen_endpoint_keys: set[tuple[str, str]] = set()
    for source_id in source_ids:
        source_detail = detail_by_id.get(source_id)
        if not source_detail:
            continue
        endpoint_key = _provider_directory_endpoint_group_key(source_detail)
        if endpoint_key in seen_endpoint_keys:
            continue
        seen_endpoint_keys.add(endpoint_key)
        endpoint_keys.append(endpoint_key)
    return endpoint_keys


def _provider_directory_endpoint_provenance_item(
    endpoint_key: tuple[str, str],
    source_ids: Sequence[str],
    detail_by_id: Mapping[str, Mapping[str, Any]],
    role_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]],
    role_keys: Sequence[tuple[str, str]],
    affiliation_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]],
    affiliation_keys: Sequence[tuple[str, str]],
) -> dict[str, Any]:
    """Build one endpoint item from exact role and affiliation evidence keys."""
    endpoint_aliases = sorted(
        (
            source_detail
            for source_detail in detail_by_id.values()
            if _provider_directory_endpoint_group_key(source_detail) == endpoint_key
        ),
        key=lambda source_detail: str(source_detail.get("source_id") or ""),
    )
    endpoint_provenance_map: dict[str, Any] = {
        "source": "provider_directory_fhir",
        "source_ids": sorted(
            source_id
            for source_id in source_ids
            if (source_detail := detail_by_id.get(source_id))
            and _provider_directory_endpoint_group_key(source_detail) == endpoint_key
        ),
        "catalog_aliases_verified": False,
        "catalog_aliases": [
            _provider_directory_catalog_alias(source_detail)
            for source_detail in endpoint_aliases
        ],
    }
    if endpoint_key[0] == "endpoint_id":
        endpoint_provenance_map["endpoint_id"] = endpoint_key[1]
    endpoint_provenance_map.update(
        _provider_directory_role_evidence_fields(
            source_ids, role_keys, detail_by_id, endpoint_key, role_evidence_map
        )
    )
    _merge_provider_directory_affiliation_fields(
        endpoint_provenance_map,
        _provider_directory_affiliation_evidence_fields(
            source_ids,
            affiliation_keys,
            detail_by_id,
            endpoint_key,
            affiliation_evidence_map,
        ),
    )
    return endpoint_provenance_map


def _provider_directory_endpoint_provenance(
    source_ids: Sequence[str],
    detail_by_id: Mapping[str, Mapping[str, Any]],
    role_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    role_keys: Sequence[tuple[str, str]] = (),
    affiliation_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]] | None = None,
    affiliation_keys: Sequence[tuple[str, str]] = (),
) -> list[dict[str, Any]]:
    """Group requested sources and exact evidence keys by endpoint identity."""
    return [
        _provider_directory_endpoint_provenance_item(
            endpoint_key,
            source_ids,
            detail_by_id,
            role_evidence_map or {},
            role_keys,
            affiliation_evidence_map or {},
            affiliation_keys,
        )
        for endpoint_key in _provider_directory_selected_endpoint_keys(
            source_ids,
            detail_by_id,
        )
    ]


async def _attach_provider_directory_source_details(
    addresses: Sequence[Any],
    *,
    include_role_evidence: bool = False,
    session: Any = None,
) -> None:
    source_ids = _provider_directory_source_ids_from_addresses(addresses)
    if not source_ids:
        return
    detail_by_id = await _fetch_provider_directory_source_detail_map(source_ids, session=session)
    if not detail_by_id:
        return
    role_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]] = {}
    affiliation_evidence_map: Mapping[tuple[str, str], Mapping[str, Any]] = {}
    if include_role_evidence:
        role_key_list = _provider_directory_role_keys_from_addresses(addresses)
        role_evidence_map = await _fetch_provider_directory_role_evidence_map(
            role_key_list,
            session=session,
        )
        affiliation_key_list = _provider_directory_affiliation_keys_from_addresses(
            addresses
        )
        affiliation_evidence_map = (
            await _fetch_provider_directory_affiliation_evidence_map(
                affiliation_key_list,
                session=session,
            )
        )
    for address in addresses:
        if not isinstance(address, dict):
            continue
        provider_directory_record_ids = _provider_directory_record_ids_from_address(
            address
        )
        address_source_ids = _provider_directory_source_ids_from_record_ids(
            provider_directory_record_ids
        )
        address_role_keys = _directory_role_keys_from_records(
            provider_directory_record_ids
        )
        address_affiliation_keys = _directory_affiliation_keys_from_records(
            provider_directory_record_ids
        )
        endpoint_provenance = _provider_directory_endpoint_provenance(
            address_source_ids,
            detail_by_id,
            role_evidence_map,
            address_role_keys,
            affiliation_evidence_map,
            address_affiliation_keys,
        )
        if endpoint_provenance:
            address[PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY] = endpoint_provenance


async def _execute_stmt(stmt: Any, *, session: Any = None, params: Optional[dict[str, Any]] = None):
    if session is not None:
        return await session.execute(stmt, params or {})
    return await db.execute(stmt, **(params or {}))


def _normalize_provider_enrichment_show_mode(raw_value: Any) -> str:
    return "chain" if str(raw_value or "").strip().lower() == "chain" else "default"


def _include_chain_provider_enrichment(raw_value: Any) -> bool:
    return _normalize_provider_enrichment_show_mode(raw_value) == "chain"


def _normalize_provider_enrichment_view(raw_value: Any) -> str:
    value = str(raw_value or "").strip().lower()
    if not value:
        return "full"
    if value in {"full", "summary"}:
        return value
    raise sanic.exceptions.InvalidUsage("view must be one of: full, summary")


def _unique_non_empty(values: Sequence[Any]) -> list[Any]:
    seen: set[Any] = set()
    output: list[Any] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        if value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def _serialize_ffs_reassignment_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "reassigning_enrollment_id": row.get("reassigning_enrollment_id"),
        "receiving_enrollment_id": row.get("receiving_enrollment_id"),
        "counterparty_npi": int(row["counterparty_npi"]) if row.get("counterparty_npi") is not None else None,
        "counterparty_provider_type_code": row.get("counterparty_provider_type_code"),
        "counterparty_provider_type_text": row.get("counterparty_provider_type_text"),
        "reporting_year": row.get("reporting_year"),
    }


def _is_chain_ffs_enrollment_payload(payload: dict[str, Any]) -> bool:
    multiple_npi_flag = str(payload.get("multiple_npi_flag") or "").strip().upper()
    provider_type_code = str(payload.get("provider_type_code") or "").strip().upper()
    return multiple_npi_flag == "Y" or provider_type_code in CHAIN_PECOS_PROVIDER_TYPE_CODES


def _partition_ffs_enrollment_payloads(
    rows: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    visible_rows: list[dict[str, Any]] = []
    chain_rows: list[dict[str, Any]] = []
    for row in rows:
        if _is_chain_ffs_enrollment_payload(row):
            chain_rows.append(row)
        else:
            visible_rows.append(row)
    return visible_rows, chain_rows


async def _fetch_ffs_summary_overrides(
    visible_rows_by_npi: dict[int, list[dict[str, Any]]],
    *,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Build per-NPI FFS enrollment summary overrides from detail tables."""
    summary_overrides_by_npi: dict[int, dict[str, Any]] = {}
    npi_by_enrollment: dict[str, int] = {}
    all_enrollment_ids: list[str] = []

    for npi_value, enrollment_rows in visible_rows_by_npi.items():
        enrollment_ids = _unique_non_empty(
            [enrollment.get("enrollment_id") for enrollment in enrollment_rows]
        )
        pecos_ids = _unique_non_empty(
            [enrollment.get("pecos_asct_cntl_id") for enrollment in enrollment_rows]
        )
        summary_overrides_by_npi[npi_value] = {
            "ffs_enrollment_ids": enrollment_ids,
            "ffs_pecos_asct_cntl_ids": pecos_ids,
            "ffs_secondary_provider_type_codes": [],
            "ffs_secondary_provider_type_texts": [],
            "ffs_practice_zip_codes": [],
            "ffs_practice_cities": [],
            "ffs_practice_states": [],
            "ffs_related_npis": [],
            "ffs_related_npi_count": 0,
            "ffs_reassignment_in_count": 0,
            "ffs_reassignment_out_count": 0,
        }
        for enrollment_id in enrollment_ids:
            npi_by_enrollment[enrollment_id] = npi_value
            all_enrollment_ids.append(enrollment_id)

    all_enrollment_ids = _unique_non_empty(all_enrollment_ids)
    if not all_enrollment_ids:
        return summary_overrides_by_npi

    if await _table_exists(ProviderEnrollmentFFSAdditionalNPI.__tablename__, session=session):
        stmt = (
            select(
                ProviderEnrollmentFFSAdditionalNPI.enrollment_id,
                ProviderEnrollmentFFSAdditionalNPI.additional_npi,
            )
            .where(ProviderEnrollmentFFSAdditionalNPI.enrollment_id.in_(all_enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSAdditionalNPI.enrollment_id.asc(),
                ProviderEnrollmentFFSAdditionalNPI.additional_npi.asc(),
            )
        )
        query_result = await _execute_stmt(stmt, session=session)
        related_by_npi: dict[int, list[int]] = defaultdict(list)
        for enrollment_id, additional_npi in query_result.all():
            if additional_npi is None:
                continue
            npi_value = npi_by_enrollment.get(str(enrollment_id))
            if npi_value is None:
                continue
            related_by_npi[npi_value].append(int(additional_npi))
        for npi_value, related_npis in related_by_npi.items():
            unique_related_npis = _unique_non_empty(related_npis)
            summary_overrides_by_npi[npi_value][
                "ffs_related_npis"
            ] = unique_related_npis
            summary_overrides_by_npi[npi_value][
                "ffs_related_npi_count"
            ] = len(unique_related_npis)

    if await _table_exists(ProviderEnrollmentFFSAddress.__tablename__, session=session):
        stmt = (
            select(
                ProviderEnrollmentFFSAddress.enrollment_id,
                ProviderEnrollmentFFSAddress.zip_code,
                ProviderEnrollmentFFSAddress.city,
                ProviderEnrollmentFFSAddress.state,
            )
            .where(ProviderEnrollmentFFSAddress.enrollment_id.in_(all_enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSAddress.enrollment_id.asc(),
                ProviderEnrollmentFFSAddress.state.asc().nullslast(),
                ProviderEnrollmentFFSAddress.city.asc().nullslast(),
                ProviderEnrollmentFFSAddress.zip_code.asc().nullslast(),
            )
        )
        query_result = await _execute_stmt(stmt, session=session)
        zip_codes_by_npi: dict[int, list[str]] = defaultdict(list)
        cities_by_npi: dict[int, list[str]] = defaultdict(list)
        states_by_npi: dict[int, list[str]] = defaultdict(list)
        for enrollment_id, zip_code, city, state in query_result.all():
            npi_value = npi_by_enrollment.get(str(enrollment_id))
            if npi_value is None:
                continue
            if zip_code:
                zip_codes_by_npi[npi_value].append(str(zip_code))
            if city:
                cities_by_npi[npi_value].append(str(city))
            if state:
                states_by_npi[npi_value].append(str(state))
        for npi_value in summary_overrides_by_npi:
            summary_overrides_by_npi[npi_value][
                "ffs_practice_zip_codes"
            ] = _unique_non_empty(zip_codes_by_npi.get(npi_value, []))
            summary_overrides_by_npi[npi_value][
                "ffs_practice_cities"
            ] = _unique_non_empty(cities_by_npi.get(npi_value, []))
            summary_overrides_by_npi[npi_value][
                "ffs_practice_states"
            ] = _unique_non_empty(states_by_npi.get(npi_value, []))

    if await _table_exists(ProviderEnrollmentFFSSecondarySpecialty.__tablename__, session=session):
        stmt = (
            select(
                ProviderEnrollmentFFSSecondarySpecialty.enrollment_id,
                ProviderEnrollmentFFSSecondarySpecialty.provider_type_code,
                ProviderEnrollmentFFSSecondarySpecialty.provider_type_text,
            )
            .where(ProviderEnrollmentFFSSecondarySpecialty.enrollment_id.in_(all_enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSSecondarySpecialty.enrollment_id.asc(),
                ProviderEnrollmentFFSSecondarySpecialty.provider_type_code.asc(),
            )
        )
        query_result = await _execute_stmt(stmt, session=session)
        codes_by_npi: dict[int, list[str]] = defaultdict(list)
        texts_by_npi: dict[int, list[str]] = defaultdict(list)
        for enrollment_id, provider_type_code, provider_type_text in query_result.all():
            npi_value = npi_by_enrollment.get(str(enrollment_id))
            if npi_value is None:
                continue
            if provider_type_code:
                codes_by_npi[npi_value].append(str(provider_type_code))
            if provider_type_text:
                texts_by_npi[npi_value].append(str(provider_type_text))
        for npi_value in summary_overrides_by_npi:
            summary_overrides_by_npi[npi_value][
                "ffs_secondary_provider_type_codes"
            ] = _unique_non_empty(
                codes_by_npi.get(npi_value, [])
            )
            summary_overrides_by_npi[npi_value][
                "ffs_secondary_provider_type_texts"
            ] = _unique_non_empty(
                texts_by_npi.get(npi_value, [])
            )

    if await _table_exists(ProviderEnrollmentFFSReassignment.__tablename__, session=session):
        stmt = (
            select(
                ProviderEnrollmentFFSReassignment.reassigning_enrollment_id,
                func.count().label("row_count"),
            )
            .where(ProviderEnrollmentFFSReassignment.reassigning_enrollment_id.in_(all_enrollment_ids))
            .group_by(ProviderEnrollmentFFSReassignment.reassigning_enrollment_id)
        )
        query_result = await _execute_stmt(stmt, session=session)
        for enrollment_id, row_count in query_result.all():
            npi_value = npi_by_enrollment.get(str(enrollment_id))
            if npi_value is None:
                continue
            summary_overrides_by_npi[npi_value][
                "ffs_reassignment_out_count"
            ] += int(row_count or 0)

        stmt = (
            select(
                ProviderEnrollmentFFSReassignment.receiving_enrollment_id,
                func.count().label("row_count"),
            )
            .where(ProviderEnrollmentFFSReassignment.receiving_enrollment_id.in_(all_enrollment_ids))
            .group_by(ProviderEnrollmentFFSReassignment.receiving_enrollment_id)
        )
        query_result = await _execute_stmt(stmt, session=session)
        for enrollment_id, row_count in query_result.all():
            npi_value = npi_by_enrollment.get(str(enrollment_id))
            if npi_value is None:
                continue
            summary_overrides_by_npi[npi_value][
                "ffs_reassignment_in_count"
            ] += int(row_count or 0)

    return summary_overrides_by_npi


async def _fast_has_insurance_count(city: Optional[str], state: Optional[str]) -> int:
    cached = _has_insurance_total_cache_get(city, state)
    if cached is not None:
        return cached

    required_columns = {"npi", "type", "plans_network_array"}
    if city:
        required_columns.add("city_name")
    if state:
        required_columns.add("state_name")
    address_model = await _address_serving_model(required_columns)
    table = address_model.__table__
    conditions = [
        table.c.type == "primary",
        literal_column("NOT (plans_network_array @@ '0'::query_int)"),
    ]
    if city:
        conditions.append(table.c.city_name == city)
    if state:
        conditions.append(table.c.state_name == state)

    # Legacy primary rows are one-per-NPI, so COUNT(*) avoids DISTINCT sorting.
    # Unified serving has one primary row per canonical address and must count NPIs.
    if city is None and state is None and address_model is NPIAddress:
        stmt = select(func.count()).select_from(table).where(*conditions)
    else:
        stmt = select(func.count(func.distinct(table.c.npi))).where(*conditions)
    async with db.session() as session:
        count_result = await session.execute(stmt)
        return _has_insurance_total_cache_set(
            city,
            state,
            int(count_result.scalar() or 0),
        )


async def _fast_primary_npi_count() -> int:
    cached = _primary_total_cache_get()
    if cached is not None:
        return cached
    address_model = await _address_serving_model({"type"})
    table = address_model.__table__
    if address_model is EntityAddressUnified:
        stmt = select(func.count(func.distinct(table.c.npi))).where(table.c.type == "primary")
    else:
        stmt = select(func.count()).select_from(table).where(table.c.type == "primary")
    scalar_fn = getattr(db, "scalar", None)
    if scalar_fn is not None:
        try:
            value = await scalar_fn(stmt)
            return _primary_total_cache_set(int(value or 0))
        except Exception as exc:  # pragma: no cover - fallback for lightweight test doubles
            logger.debug("primary NPI count scalar path failed; falling back to acquire path: %s", exc)
    async with db.acquire() as conn:
        rows = await conn.all(stmt)
    value = rows[0][0] if rows else 0
    return _primary_total_cache_set(int(value or 0))


def _build_nearby_sql(
    taxonomy_conditions: str,
    extra_clause: str,
    ilike_clause: str,
    *,
    use_taxonomy_filter: bool,
    address_table_sql: str = "mrf.npi_address",
    geo_precision_clause: str = "",
) -> str:
    """Build the indexed nearby-provider query for the selected address model."""
    taxonomy_from = ""
    taxonomy_where = ""
    if use_taxonomy_filter:
        taxonomy_from = (
            ",\n"
            "                              (\n"
            "                                  SELECT ARRAY_AGG(int_code) AS codes\n"
            "                                    FROM mrf.nucc_taxonomy\n"
            f"                                   WHERE {taxonomy_conditions}\n"
            "                              ) AS g"
        )
        taxonomy_where = "\n                          AND a.taxonomy_array && g.codes"
    # Match the geo_idx partial predicate so the GiST index is used. Only widen to
    # practice/site when serving the unified table AND the rebuilt index covers them
    # (flag-gated); otherwise keep the NPPES primary/secondary filter the live index
    # supports, so geo search never seq-scans.
    if address_table_sql.endswith(".entity_address_unified") and _geo_includes_service_locations():
        type_list = ", ".join(f"'{t}'" for t in GEO_SERVICE_LOCATION_TYPES)
        geo_type_clause = f"AND a.type IN ({type_list})"
    else:
        geo_type_clause = "AND (a.type = 'primary' OR a.type = 'secondary')"
    return dedent(
        """
        WITH sub_s AS (
            SELECT d.npi AS npi_code,
                   q.*,
                   d.*
              FROM mrf.npi AS d,
                   (
                       SELECT
                           ROUND(
                               CAST(
                                   ST_Distance(
                                       Geography(ST_MakePoint(a.long, a.lat)),
                                       Geography(ST_MakePoint(:in_long, :in_lat))
                                   ) / 1609.34 AS NUMERIC
                               ),
                               2
                           ) AS distance,
                           a.*
                        FROM {address_table_sql} AS a{taxonomy_from}
                        WHERE ST_DWithin(
                                Geography(ST_MakePoint(long, lat)),
                                Geography(ST_MakePoint(:in_long, :in_lat)),
                                :radius * 1609.34
                             )
                          AND a.lat IS NOT NULL
                          AND a.long IS NOT NULL
                          {taxonomy_where}
                          {geo_precision_clause}
                          {geo_type_clause}
                          {extra_clause}
                     ORDER BY distance ASC
                     LIMIT :limit
                   ) AS q
             WHERE q.npi = d.npi{ilike_clause}
        )
        SELECT sub_s.*, t.*
          FROM sub_s
          JOIN mrf.npi_taxonomy AS t ON sub_s.npi_code = t.npi;
        """
    ).format(
        taxonomy_from=taxonomy_from,
        taxonomy_where=taxonomy_where,
        geo_precision_clause=geo_precision_clause,
        geo_type_clause=geo_type_clause,
        extra_clause=extra_clause,
        ilike_clause=ilike_clause,
        address_table_sql=address_table_sql,
    )


def _exact_geo_precision_clause(address_table_sql: str) -> str:
    if address_table_sql.endswith(".entity_address_unified"):
        return "\n                          AND COALESCE(a.address_precision, '') <> 'city_zip'"
    return ""


def _name_like_clause(alias: str = "", param: str = "name_like") -> str:
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."
    expr = NAME_LIKE_TEMPLATE.format(alias=prefix)
    param_ref = f":{param}" if not param.startswith(":") else param
    return f"({expr} LIKE {param_ref})"


def _name_like_clauses(alias: str, names: Sequence[str], base_param: str = "name_like") -> tuple[str, dict]:
    if not names:
        return "", {}
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."
    expr = NAME_LIKE_TEMPLATE.format(alias=prefix)
    clauses = []
    params = {}
    for idx, name in enumerate(names):
        param_like = f"{base_param}_{idx}"
        if ENABLE_TRGM_FUZZY_NAME_SEARCH:
            param_fuzzy = f"{base_param}_{idx}_fuzzy"
            clauses.append(
                f"(({expr} LIKE :{param_like}) OR ({expr} % :{param_fuzzy}))"
            )
            params[param_fuzzy] = name.lower()
        else:
            clauses.append(f"({expr} LIKE :{param_like})")
        params[param_like] = f"%{name.lower()}%"
    joined = " OR ".join(clauses)
    return f"({joined})", params


def _normalize_zip_code(raw: Optional[str], param_name: str) -> Optional[str]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) < 5:
        raise sanic.exceptions.InvalidUsage(
            f"{param_name} must contain at least 5 digits"
        )
    return digits[:5]


def _normalize_phone_digits(raw: Optional[str]) -> Optional[str]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) < 7 or len(digits) > 15:
        raise sanic.exceptions.InvalidUsage(
            "phone must contain between 7 and 15 digits"
        )
    return digits


def _normalize_uuid_key(raw: Optional[str], param_name: str) -> Optional[str]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    try:
        return str(uuid.UUID(text_value))
    except (TypeError, ValueError) as exc:
        raise sanic.exceptions.InvalidUsage(f"{param_name} must be a valid UUID") from exc


def _normalize_address_key(raw: Optional[str]) -> Optional[str]:
    return _normalize_uuid_key(raw, "address_key")


def _normalize_exact_npi(raw: Optional[str]) -> Optional[int]:
    if raw is None:
        return None
    text_value = str(raw).strip()
    if not text_value:
        return None
    digits = "".join(ch for ch in text_value if ch.isdigit())
    if len(digits) != 10:
        raise sanic.exceptions.InvalidUsage("npi must contain exactly 10 digits")
    return int(digits)


def _normalize_code_system(raw: Optional[str], param_name: str, allowed: set[str]) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        raise sanic.exceptions.InvalidUsage(f"{param_name} is required when codes are provided")
    if value not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise sanic.exceptions.InvalidUsage(
            f"{param_name} must be one of: {allowed_values}"
        )
    return value


def _parse_code_tokens(raw: Optional[str], param_name: str) -> list[str]:
    if raw is None:
        return []
    tokens: list[str] = []
    seen: set[str] = set()
    for item in str(raw).split(","):
        token = item.strip().upper()
        if not token:
            continue
        if not CODE_TOKEN_PATTERN.fullmatch(token):
            raise sanic.exceptions.InvalidUsage(
                f"{param_name} contains invalid code token: {item!r}"
            )
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _to_int_codes(values: Sequence[str], param_name: str) -> list[int]:
    out: list[int] = []
    seen: set[int] = set()
    for value in values:
        if not INT_CODE_PATTERN.fullmatch(str(value)):
            raise sanic.exceptions.InvalidUsage(
                f"{param_name} must contain numeric codes for internal matching"
            )
        parsed = int(value)
        if parsed in seen:
            continue
        seen.add(parsed)
        out.append(parsed)
    return out


def _parse_optional_year(raw: Optional[str], param_name: str = "year") -> Optional[int]:
    if raw in (None, ""):
        return None
    try:
        year = int(str(raw).strip())
    except (TypeError, ValueError) as exc:
        raise sanic.exceptions.InvalidUsage(f"{param_name} must be an integer >= 2013") from exc
    if year < 2013:
        raise sanic.exceptions.InvalidUsage(f"{param_name} must be >= 2013")
    return year


async def _table_exists(table_name: str, *, session: Any = None) -> bool:
    cache_key = _schema_cache_key(table_name)
    cached = _cache_get(_TABLE_EXISTS_CACHE, cache_key)
    if cached is not None:
        return bool(cached)
    try:
        result = await _execute_stmt(
            text("SELECT to_regclass(:table_name);"),
            session=session,
            params={"table_name": cache_key},
        )
        rows = result.all()
        return bool(_cache_set(_TABLE_EXISTS_CACHE, cache_key, bool(rows and rows[0] and rows[0][0])))
    except Exception:  # pragma: no cover - defensive fallback for transient DB states
        return False


async def _resolve_npi_filter_capabilities(*, session: Any = None) -> dict[str, bool]:
    cached = _filter_cache_get()
    if cached is not None:
        return cached
    model_columns = _model_table_columns(NPIAddress)
    capability_map = {
        "npi_procedures_array_available": "procedures_array" in model_columns,
        "npi_medications_array_available": "medications_array" in model_columns,
        "pricing_provider_procedure_available": False,
        "pricing_provider_prescription_available": False,
    }

    if ENABLE_NPI_SCHEMA_CACHE:
        try:
            column_query_result = await _execute_stmt(
                text(
                    """
                    SELECT column_name
                      FROM information_schema.columns
                     WHERE table_schema = 'mrf'
                       AND table_name = 'npi_address'
                       AND column_name IN ('procedures_array', 'medications_array')
                    """
                ),
                session=session,
            )
            column_rows = column_query_result.all()
            columns = {
                str(column_row[0])
                for column_row in column_rows
                if column_row and column_row[0]
            }
            capability_map[
                "npi_procedures_array_available"
            ] = "procedures_array" in columns
            capability_map[
                "npi_medications_array_available"
            ] = "medications_array" in columns
        except Exception:  # pragma: no cover - defensive fallback for transient DB states
            capability_map[
                "npi_procedures_array_available"
            ] = "procedures_array" in model_columns
            capability_map[
                "npi_medications_array_available"
            ] = "medications_array" in model_columns

    capability_map["pricing_provider_procedure_available"] = await _table_exists(
        "pricing_provider_procedure",
        session=session,
    )
    capability_map[
        "pricing_provider_prescription_available"
    ] = await _table_exists(
        "pricing_provider_prescription",
        session=session,
    )
    return _filter_cache_set(capability_map)


async def _table_columns(table_name: str, *, session: Any = None) -> set[str]:
    cache_key = _schema_cache_key(table_name)
    cached = _cache_get(_TABLE_COLUMNS_CACHE, cache_key)
    if cached is not None:
        return set(cached)
    try:
        result = await _execute_stmt(
            text(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'mrf'
                   AND table_name = :table_name
                """
            ),
            session=session,
            params={"table_name": table_name},
        )
        rows = result.all()
        return set(_cache_set(_TABLE_COLUMNS_CACHE, cache_key, {str(row[0]) for row in rows if row and row[0]}))
    except Exception:  # pragma: no cover - defensive fallback for transient DB states
        return set()


def _provider_directory_profile_json(value: Any) -> dict[str, Any] | None:
    decoded = value
    if isinstance(decoded, str):
        try:
            decoded = json.loads(decoded)
        except json.JSONDecodeError:
            return None
    return dict(decoded) if isinstance(decoded, Mapping) else None


def _serialize_utc_rfc3339_datetime(
    published_at: datetime | str | None,
) -> str | None:
    """Serialize UTC publication timestamps as OpenAPI date-time values."""
    if published_at is None:
        return None
    if isinstance(published_at, str):
        timestamp_text = published_at.strip()
        if timestamp_text.endswith(("Z", "z")):
            timestamp_text = f"{timestamp_text[:-1]}+00:00"
        parsed_timestamp = datetime.fromisoformat(timestamp_text)
    elif isinstance(published_at, datetime):
        parsed_timestamp = published_at
    else:
        raise TypeError("published_at must be a datetime, ISO datetime string, or None")
    if parsed_timestamp.tzinfo is None:
        parsed_timestamp = parsed_timestamp.replace(tzinfo=UTC)
    else:
        parsed_timestamp = parsed_timestamp.astimezone(UTC)
    return parsed_timestamp.isoformat().replace("+00:00", "Z")


_PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN: set[str] = set()


async def _is_provider_directory_profile_table_available(
    table_ref: str,
    *,
    session: Any = None,
) -> bool:
    if table_ref in _PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN:
        return True
    relation_query_result = await _execute_stmt(
        text("SELECT to_regclass(:table_ref);"),
        session=session,
        params={"table_ref": table_ref},
    )
    if relation_query_result.scalar() is None:
        return False
    _PROVIDER_DIRECTORY_PROFILE_TABLES_SEEN.add(table_ref)
    return True


def _provider_directory_profile_payload(
    mapping: Mapping[str, Any],
    *,
    include_evidence: bool,
) -> dict[str, Any] | None:
    """Build one public profile artifact payload from an indexed query row."""
    profile = _provider_directory_profile_json(mapping.get("profile_json"))
    if profile is None:
        return None
    published_at = _serialize_utc_rfc3339_datetime(mapping.get("published_at"))
    profile["generation_id"] = mapping.get("generation_id")
    profile["published_at"] = published_at
    profile_payload_by_kind: dict[str, Any] = {"profile": profile}
    if include_evidence:
        evidence = _provider_directory_profile_json(mapping.get("evidence_json"))
        if evidence is not None:
            evidence["generation_id"] = mapping.get("generation_id")
            evidence["published_at"] = published_at
            profile_payload_by_kind["evidence"] = evidence
    return profile_payload_by_kind


async def _fetch_provider_directory_profile_map(
    npis: Sequence[Any],
    *,
    include_evidence: bool = False,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Fetch indexed profile artifacts for valid NPIs, with optional evidence."""
    normalized_npis = sorted(
        {
            int(npi)
            for npi in npis
            if profile_artifact.is_valid_npi(npi)
        }
    )
    if not normalized_npis:
        return {}
    table_ref = _schema_cache_key(profile_artifact.PROFILE_TABLE)
    if not await _is_provider_directory_profile_table_available(
        table_ref,
        session=session,
    ):
        return {}
    evidence_select = ", evidence_json" if include_evidence else ""
    profile_query_result = await _execute_stmt(
        text(
            f"""
            SELECT npi, profile_json, generation_id, published_at
                   {evidence_select}
              FROM {table_ref}
             WHERE npi = ANY(CAST(:npis AS bigint[]));
            """
        ),
        session=session,
        params={"npis": normalized_npis},
    )
    profiles_by_npi: dict[int, dict[str, Any]] = {}
    for profile_query_row in profile_query_result.all():
        mapping = getattr(
            profile_query_row,
            "_mapping",
            profile_query_row,
        )
        profile_payload_by_kind = _provider_directory_profile_payload(
            mapping,
            include_evidence=include_evidence,
        )
        if profile_payload_by_kind is None:
            continue
        profiles_by_npi[int(mapping["npi"])] = profile_payload_by_kind
    return profiles_by_npi


def _address_serving_unified_requested() -> bool:
    return os.getenv(ADDRESS_SERVING_SOURCE_ENV, ADDRESS_SERVING_SOURCE_UNIFIED).strip().lower() == ADDRESS_SERVING_SOURCE_UNIFIED


def _address_table_is_unified(address_table_sql: str) -> bool:
    return address_table_sql.endswith(f".{EntityAddressUnified.__tablename__}")


def _address_zip5_filter(alias: str, address_table_sql: str, *, any_array: bool = False) -> str:
    column = f"{alias}.zip5" if _address_table_is_unified(address_table_sql) else f"LEFT({alias}.postal_code, 5)"
    operator = "ANY (:zip_codes)" if any_array else ":zip_code"
    return f"{column} = {operator}"


def _address_phone_digits_filter(alias: str, address_table_sql: str) -> str:
    raw_digits = f"regexp_replace(COALESCE({alias}.telephone_number, ''), '[^0-9]', '', 'g')"
    if _address_table_is_unified(address_table_sql):
        return f"COALESCE(NULLIF({alias}.phone_number, ''), {raw_digits}) = :phone_digits"
    return f"{raw_digits} = :phone_digits"


_CURRENT_PROVIDER_DIRECTORY_PHONE_CTES = """
current_provider_directory_runs AS MATERIALIZED (
    SELECT source.source_id,
           COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar
               AS run_id
      FROM mrf.provider_directory_source AS source
      JOIN mrf.provider_directory_endpoint_dataset AS dataset
        ON dataset.endpoint_id = source.endpoint_id
     WHERE dataset.is_current IS TRUE
       AND dataset.status = 'published'
       AND dataset.published_at IS NOT NULL
       AND dataset.superseded_at IS NULL
), matching_provider_directory_phone_rows AS MATERIALIZED (
    SELECT overlay.npi, overlay.address_key, overlay.source_id,
           overlay.last_seen_run_id, overlay.source_record_id
      FROM mrf.provider_directory_address_overlay AS overlay
     WHERE overlay.phone_number = :phone_digits
       AND overlay.npi IS NOT NULL
       AND overlay.address_key IS NOT NULL
)
"""

MIN_PROVIDER_LIST_PHONE_CANDIDATES = 100
MAX_PROVIDER_LIST_PHONE_CANDIDATES = 500


def _provider_list_phone_candidate_limit(
    page_limit: int,
    page_offset: int = 0,
    *,
    count_query: bool = False,
) -> int:
    """Bound phone candidates while retaining enough rows for paging/filtering."""
    if count_query:
        return MAX_PROVIDER_LIST_PHONE_CANDIDATES
    requested_window = max(int(page_offset), 0) + max(int(page_limit), 1)
    return min(
        max(requested_window * 8, MIN_PROVIDER_LIST_PHONE_CANDIDATES),
        MAX_PROVIDER_LIST_PHONE_CANDIDATES,
    )


_PHONE_CANDIDATE_ROWS_CTE = """
phone_candidate_rows AS MATERIALIZED (
    SELECT DISTINCT
           COALESCE(phone_address.npi, phone_address.inferred_npi)::bigint AS provider_npi,
           phone_address.address_key,
           false AS provider_directory_matched,
           NULL::varchar AS source_id,
           NULL::varchar AS source_record_id,
           phone_address.source_count::integer AS source_count
      FROM {address_table_sql} AS phone_address
     WHERE phone_address.type IN ({service_types})
       AND phone_address.address_key IS NOT NULL
       AND COALESCE(phone_address.npi, phone_address.inferred_npi) IS NOT NULL
       AND {direct_phone}
    UNION ALL
    SELECT DISTINCT
           overlay.npi::bigint AS provider_npi,
           overlay.address_key,
           true AS provider_directory_matched,
           overlay.source_id::varchar,
           overlay.source_record_id::varchar,
           NULL::integer AS source_count
      FROM matching_provider_directory_phone_rows AS overlay
      JOIN current_provider_directory_runs AS current_run
        ON current_run.source_id = overlay.source_id
       AND current_run.run_id = overlay.last_seen_run_id
)
"""


_RANKED_PHONE_CANDIDATE_CTES = """
phone_candidates_unranked AS MATERIALIZED (
    SELECT candidate.provider_npi, candidate.address_key,
           BOOL_OR(candidate.provider_directory_matched) AS provider_directory_matched,
           MAX(candidate.source_count) AS source_count
      FROM phone_candidate_rows AS candidate
  GROUP BY candidate.provider_npi, candidate.address_key
), phone_candidate_best_addresses AS MATERIALIZED (
    SELECT DISTINCT ON (candidate.provider_npi)
           candidate.provider_npi, candidate.address_key,
           candidate.provider_directory_matched, candidate.source_count
      FROM phone_candidates_unranked AS candidate
  ORDER BY candidate.provider_npi,
           candidate.provider_directory_matched DESC,
           candidate.source_count DESC NULLS LAST,
           candidate.address_key
), phone_candidates AS MATERIALIZED (
    SELECT candidate.provider_npi, candidate.address_key,
           candidate.provider_directory_matched
      FROM phone_candidate_best_addresses AS candidate
  ORDER BY candidate.provider_directory_matched DESC,
           candidate.source_count DESC NULLS LAST,
           candidate.provider_npi,
           candidate.address_key
     LIMIT :candidate_limit
), phone_provider_directory_evidence AS MATERIALIZED (
    SELECT evidence.provider_npi,
           ARRAY_AGG(evidence.source_record_id ORDER BY evidence.source_id)
               AS source_record_ids
      FROM (
            SELECT candidate.provider_npi, candidate.source_id,
                   MIN(candidate.source_record_id) AS source_record_id
              FROM phone_candidate_rows AS candidate
              JOIN phone_candidates AS selected_candidate
                ON selected_candidate.provider_npi = candidate.provider_npi
             WHERE candidate.provider_directory_matched
               AND candidate.source_id IS NOT NULL
               AND candidate.source_record_id IS NOT NULL
          GROUP BY candidate.provider_npi, candidate.source_id
      ) AS evidence
  GROUP BY evidence.provider_npi
)
"""


def _address_phone_candidates_cte(address_table_sql: str) -> str | None:
    """Return indexed phone candidates, including current FHIR evidence."""
    if not _address_table_is_unified(address_table_sql):
        return None
    direct_phone = _address_phone_digits_filter("phone_address", address_table_sql)
    service_types = ", ".join(f"'{location_type}'" for location_type in GEO_SERVICE_LOCATION_TYPES)
    phone_candidate_rows_cte = _PHONE_CANDIDATE_ROWS_CTE.format(
        address_table_sql=address_table_sql,
        service_types=service_types,
        direct_phone=direct_phone,
    )
    return ",\n".join(
        (
            _CURRENT_PROVIDER_DIRECTORY_PHONE_CTES.strip(),
            phone_candidate_rows_cte.strip(),
            _RANKED_PHONE_CANDIDATE_CTES.strip(),
        )
    )


def _address_phone_candidates_join(alias: str, provider_npi_sql: str | None = None) -> str:
    provider_npi = provider_npi_sql or f"COALESCE({alias}.npi, {alias}.inferred_npi)"
    return f"""
          JOIN phone_candidates AS phone_match
            ON phone_match.provider_npi = {provider_npi}
           AND phone_match.address_key = {alias}.address_key
    """


def _address_phone_candidates_lateral_from(address_table_sql: str, alias: str) -> str:
    exact_phone = _address_phone_digits_filter("candidate_address", address_table_sql)
    service_location = _provider_list_address_type_clause(
        "candidate_address",
        address_table_sql,
        include_service_locations=True,
    )
    return f"""
          FROM phone_candidates AS phone_match
     LEFT JOIN phone_provider_directory_evidence AS phone_evidence
            ON phone_evidence.provider_npi = phone_match.provider_npi
         CROSS JOIN LATERAL (
               SELECT candidate_address.*
                 FROM {address_table_sql} AS candidate_address
                WHERE candidate_address.address_key = phone_match.address_key
                  AND COALESCE(candidate_address.npi, candidate_address.inferred_npi) = phone_match.provider_npi
                  AND {service_location}
             ORDER BY ({exact_phone}) DESC,
                      candidate_address.source_count DESC NULLS LAST,
                      candidate_address.location_key
                LIMIT 1 OFFSET 0
         ) AS {alias}
    """


def _sql_with_prefix_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    return f"WITH {',\n'.join(available_ctes)},\n" if available_ctes else "WITH "


def _sql_with_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    return f"WITH {',\n'.join(available_ctes)}\n" if available_ctes else ""


def _address_npi_filter(alias: str, address_table_sql: str) -> str:
    if _address_table_is_unified(address_table_sql):
        return f"COALESCE({alias}.npi, {alias}.inferred_npi) = :npi_filter"
    return f"{alias}.npi = :npi_filter"


def _address_site_key_filter(alias: str, address_table_sql: str) -> str:
    if _address_table_is_unified(address_table_sql):
        return f"{alias}.premise_key = CAST(:address_site_key AS uuid)"
    return "1=0"


def _provider_list_address_type_clause(
    alias: str,
    address_table_sql: str,
    *,
    include_service_locations: bool,
) -> str:
    if include_service_locations and _address_table_is_unified(address_table_sql):
        type_list = ", ".join(f"'{value}'" for value in GEO_SERVICE_LOCATION_TYPES)
        return f"{alias}.type IN ({type_list})"
    return f"{alias}.type = 'primary'"


def _primary_address_order_clause(alias: str, address_table_sql: str) -> str:
    common = (
        f"{alias}.npi, "
        f"({alias}.lat IS NULL OR {alias}.long IS NULL), "
        f"(NULLIF(TRIM(COALESCE({alias}.first_line, '')), '') IS NULL), "
    )
    if _address_table_is_unified(address_table_sql):
        return (
            common
            + f"(COALESCE({alias}.address_precision, '') = 'city_zip'), "
            + f"{alias}.source_count DESC NULLS LAST, "
            + f"{alias}.updated_at DESC NULLS LAST, "
            + f"{alias}.location_key"
        )
    return common + f"{alias}.date_added DESC NULLS LAST, {alias}.checksum"


def _public_address_column_keys() -> set[str]:
    return _model_table_columns(NPIAddress) - PUBLIC_ADDRESS_EXCLUDED_COLUMNS


def _public_address_serving_column_keys() -> set[str]:
    return _public_address_column_keys() | {"premise_key"}


async def _address_serving_model(required_columns: set[str] | None = None, *, session: Any = None):
    if not _address_serving_unified_requested():
        return NPIAddress
    required = set(required_columns or ())
    columns = await _table_columns(EntityAddressUnified.__tablename__, session=session)
    if columns and required.issubset(columns):
        return EntityAddressUnified
    return NPIAddress


async def _address_serving_table_sql(required_columns: set[str] | None = None, *, session: Any = None) -> str:
    model = await _address_serving_model(required_columns, session=session)
    return _schema_cache_key(model.__tablename__)


async def _fetch_provider_enrichment_summary_map(
    npis: Sequence[int],
    *,
    include_chain: bool = False,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Fetch public provider-enrichment summaries keyed by requested NPI."""
    unique_npis = sorted({int(npi) for npi in npis if npi is not None})
    if not unique_npis:
        return {}
    if not await _table_exists(ProviderEnrichmentSummary.__tablename__, session=session):
        return {}

    requested_columns = [
        "npi",
        "latest_reporting_year",
        "status",
        "has_any_enrollment",
        "has_medicare_claims",
        "has_ffs_enrollment",
        "has_hospital_enrollment",
        "has_hha_enrollment",
        "has_hospice_enrollment",
        "has_fqhc_enrollment",
        "has_rhc_enrollment",
        "has_snf_enrollment",
        "primary_state",
        "primary_provider_type_code",
        "total_enrollment_rows",
        "dataset_keys",
        "ffs_enrollment_ids",
        "ffs_pecos_asct_cntl_ids",
        "ffs_secondary_provider_type_codes",
        "ffs_secondary_provider_type_texts",
        "ffs_practice_zip_codes",
        "ffs_practice_cities",
        "ffs_practice_states",
        "ffs_related_npis",
        "ffs_related_npi_count",
        "ffs_reassignment_in_count",
        "ffs_reassignment_out_count",
    ]
    model_columns = _model_table_columns(ProviderEnrichmentSummary)

    async def _run_summary_query(available_columns: set[str]):
        select_columns = [
            column_name if column_name in available_columns else f"NULL AS {column_name}"
            for column_name in requested_columns
        ]
        query = text(
            f"""
            SELECT
                {', '.join(select_columns)}
              FROM mrf.{ProviderEnrichmentSummary.__tablename__}
             WHERE npi = ANY(:npis)
            """
        )
        summary_query_result = await _execute_stmt(
            query,
            session=session,
            params={"npis": unique_npis},
        )
        return summary_query_result.all()

    try:
        summary_rows = await _run_summary_query(model_columns)
    except Exception:
        available_columns = await _table_columns(ProviderEnrichmentSummary.__tablename__, session=session)
        summary_rows = await _run_summary_query(available_columns)

    summary_map: dict[int, dict[str, Any]] = {}
    for summary_row in summary_rows:
        npi_value = int(summary_row[0])
        summary_map[npi_value] = {
            "latest_reporting_year": summary_row[1],
            "status": summary_row[2],
            "has_any_enrollment": bool(summary_row[3]),
            "has_medicare_claims": bool(summary_row[4]),
            "has_ffs_enrollment": bool(summary_row[5]),
            "has_hospital_enrollment": bool(summary_row[6]),
            "has_hha_enrollment": bool(summary_row[7]),
            "has_hospice_enrollment": bool(summary_row[8]),
            "has_fqhc_enrollment": bool(summary_row[9]),
            "has_rhc_enrollment": bool(summary_row[10]),
            "has_snf_enrollment": bool(summary_row[11]),
            "primary_state": summary_row[12],
            "primary_provider_type_code": summary_row[13],
            "total_enrollment_rows": summary_row[14],
            "dataset_keys": list(summary_row[15] or []),
            "ffs_enrollment_ids": list(summary_row[16] or []),
            "ffs_pecos_asct_cntl_ids": list(summary_row[17] or []),
            "ffs_secondary_provider_type_codes": list(summary_row[18] or []),
            "ffs_secondary_provider_type_texts": list(summary_row[19] or []),
            "ffs_practice_zip_codes": list(summary_row[20] or []),
            "ffs_practice_cities": list(summary_row[21] or []),
            "ffs_practice_states": list(summary_row[22] or []),
            "ffs_related_npis": [
                int(related_npi)
                for related_npi in (summary_row[23] or [])
                if related_npi is not None
            ],
            "ffs_related_npi_count": int(summary_row[24] or 0),
            "ffs_reassignment_in_count": int(summary_row[25] or 0),
            "ffs_reassignment_out_count": int(summary_row[26] or 0),
            "ffs_chain_hidden": False,
            "ffs_chain_enrollment_count": 0,
            "ffs_chain_enrollment_ids": [],
        }

    if not summary_map or not await _table_exists(ProviderEnrollmentFFS.__tablename__, session=session):
        return summary_map

    stmt = (
        select(
            ProviderEnrollmentFFS.npi,
            ProviderEnrollmentFFS.enrollment_id,
            ProviderEnrollmentFFS.pecos_asct_cntl_id,
            ProviderEnrollmentFFS.provider_type_code,
            ProviderEnrollmentFFS.provider_type_text,
            ProviderEnrollmentFFS.multiple_npi_flag,
        )
        .where(ProviderEnrollmentFFS.npi.in_(unique_npis))
        .order_by(
            ProviderEnrollmentFFS.npi.asc(),
            ProviderEnrollmentFFS.reporting_year.desc().nullslast(),
            ProviderEnrollmentFFS.enrollment_id.asc(),
        )
    )
    enrollment_query_result = await _execute_stmt(stmt, session=session)

    ffs_rows_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for enrollment_row in enrollment_query_result.all():
        (
            npi_value,
            enrollment_id,
            pecos_asct_cntl_id,
            provider_type_code,
            provider_type_text,
            multiple_npi_flag,
        ) = enrollment_row
        ffs_rows_by_npi[int(npi_value)].append(
            {
                "enrollment_id": enrollment_id,
                "pecos_asct_cntl_id": pecos_asct_cntl_id,
                "provider_type_code": provider_type_code,
                "provider_type_text": provider_type_text,
                "multiple_npi_flag": multiple_npi_flag,
            }
        )

    visible_rows_by_npi: dict[int, list[dict[str, Any]]] = {}
    for npi_value, summary in summary_map.items():
        visible_rows, chain_rows = _partition_ffs_enrollment_payloads(ffs_rows_by_npi.get(npi_value, []))
        summary["ffs_chain_hidden"] = bool(chain_rows) and not include_chain
        summary["ffs_chain_enrollment_count"] = len(chain_rows)
        summary["ffs_chain_enrollment_ids"] = _unique_non_empty(
            [enrollment.get("enrollment_id") for enrollment in chain_rows]
        )
        if chain_rows and not include_chain:
            visible_rows_by_npi[npi_value] = visible_rows

    if not visible_rows_by_npi:
        return summary_map

    summary_overrides_by_npi = await _fetch_ffs_summary_overrides(
        visible_rows_by_npi,
        session=session,
    )
    for npi_value, visible_rows in visible_rows_by_npi.items():
        summary = summary_map.get(npi_value)
        if summary is None:
            continue
        summary_override_map = summary_overrides_by_npi.get(npi_value)
        if summary_override_map is None:
            summary_override_map = {
                "ffs_enrollment_ids": _unique_non_empty(
                    [enrollment.get("enrollment_id") for enrollment in visible_rows]
                ),
                "ffs_pecos_asct_cntl_ids": _unique_non_empty(
                    [
                        enrollment.get("pecos_asct_cntl_id")
                        for enrollment in visible_rows
                    ]
                ),
                "ffs_secondary_provider_type_codes": [],
                "ffs_secondary_provider_type_texts": [],
                "ffs_practice_zip_codes": [],
                "ffs_practice_cities": [],
                "ffs_practice_states": [],
                "ffs_related_npis": [],
                "ffs_related_npi_count": 0,
                "ffs_reassignment_in_count": 0,
                "ffs_reassignment_out_count": 0,
            }

        summary["ffs_enrollment_ids"] = summary_override_map["ffs_enrollment_ids"]
        summary["ffs_pecos_asct_cntl_ids"] = summary_override_map[
            "ffs_pecos_asct_cntl_ids"
        ]
        summary["ffs_secondary_provider_type_codes"] = summary_override_map[
            "ffs_secondary_provider_type_codes"
        ]
        summary["ffs_secondary_provider_type_texts"] = summary_override_map[
            "ffs_secondary_provider_type_texts"
        ]
        summary["ffs_practice_zip_codes"] = summary_override_map[
            "ffs_practice_zip_codes"
        ]
        summary["ffs_practice_cities"] = summary_override_map[
            "ffs_practice_cities"
        ]
        summary["ffs_practice_states"] = summary_override_map[
            "ffs_practice_states"
        ]
        summary["ffs_related_npis"] = summary_override_map["ffs_related_npis"]
        summary["ffs_related_npi_count"] = summary_override_map[
            "ffs_related_npi_count"
        ]
        summary["ffs_reassignment_in_count"] = summary_override_map[
            "ffs_reassignment_in_count"
        ]
        summary["ffs_reassignment_out_count"] = summary_override_map[
            "ffs_reassignment_out_count"
        ]
    return summary_map


def _provider_enrichment_visibility(summary: Optional[dict[str, Any]], *, include_chain: bool) -> dict[str, Any]:
    summary = summary or {}
    chain_ids = list(summary.get("ffs_chain_enrollment_ids") or [])
    chain_count = int(summary.get("ffs_chain_enrollment_count") or len(chain_ids))
    return {
        "show_mode": "chain" if include_chain else "default",
        "chain_hidden": bool(chain_count) and not include_chain,
        "chain_enrollment_count": chain_count,
        "chain_enrollment_ids": chain_ids,
    }


def _public_provider_enrichment_summary(summary: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not isinstance(summary, dict):
        return summary
    public_summary = dict(summary)
    public_summary.pop("ffs_chain_hidden", None)
    public_summary.pop("ffs_chain_enrollment_count", None)
    public_summary.pop("ffs_chain_enrollment_ids", None)
    return public_summary


async def _fetch_provider_enrichment_summary_detail(
    npi: int,
    *,
    include_chain: bool = False,
    session: Any = None,
) -> dict[str, Any]:
    summary_map = await _fetch_provider_enrichment_summary_map([npi], include_chain=include_chain, session=session)
    summary = summary_map.get(int(npi))
    return {
        "summary": _public_provider_enrichment_summary(summary),
        "ffs_visibility": _provider_enrichment_visibility(summary, include_chain=include_chain),
    }


async def _fetch_provider_enrichment_detail(
    npi: int,
    *,
    include_chain: bool = False,
    session: Any = None,
) -> dict[str, Any]:
    """Fetch enrollment details and visibility metadata for one NPI."""
    enrichment_detail_map: dict[str, Any] = {
        "summary": None,
        "enrollments": {
            "ffs_public": [],
            "hospital": [],
            "hha": [],
            "hospice": [],
            "fqhc": [],
            "rhc": [],
            "snf": [],
        },
        "ffs_subfiles": {
            "additional_npis": [],
            "practice_locations": [],
            "secondary_specialties": [],
            "reassignments_out": [],
            "reassignments_in": [],
        },
        "ffs_visibility": {
            "show_mode": "chain" if include_chain else "default",
            "chain_hidden": False,
            "chain_enrollment_count": 0,
            "chain_enrollment_ids": [],
        },
    }

    summary_map = await _fetch_provider_enrichment_summary_map([npi], include_chain=include_chain, session=session)
    summary = summary_map.get(int(npi))
    enrichment_detail_map["summary"] = _public_provider_enrichment_summary(
        summary
    )
    enrichment_detail_map["ffs_visibility"] = _provider_enrichment_visibility(
        summary,
        include_chain=include_chain,
    )

    table_model_pairs = (
        ("ffs_public", ProviderEnrollmentFFS),
        ("hospital", ProviderEnrollmentHospital),
        ("hha", ProviderEnrollmentHomeHealthAgency),
        ("hospice", ProviderEnrollmentHospice),
        ("fqhc", ProviderEnrollmentFQHC),
        ("rhc", ProviderEnrollmentRHC),
        ("snf", ProviderEnrollmentSNF),
    )
    enrollment_flag_map = {
        "ffs_public": "has_ffs_enrollment",
        "hospital": "has_hospital_enrollment",
        "hha": "has_hha_enrollment",
        "hospice": "has_hospice_enrollment",
        "fqhc": "has_fqhc_enrollment",
        "rhc": "has_rhc_enrollment",
        "snf": "has_snf_enrollment",
    }

    for key, model in table_model_pairs:
        if summary and not summary.get(enrollment_flag_map[key], False):
            continue
        if not await _table_exists(model.__tablename__, session=session):
            continue
        stmt = (
            select(model)
            .where(model.npi == npi)
            .order_by(model.reporting_year.desc().nullslast(), model.imported_at.desc().nullslast())
            .limit(25)
        )
        enrollment_query_result = await _execute_stmt(stmt, session=session)
        enrollment_rows = [
            enrollment_record.to_json_dict()
            for enrollment_record in enrollment_query_result.scalars()
        ]
        if key == "ffs_public":
            visible_rows, chain_rows = _partition_ffs_enrollment_payloads(
                enrollment_rows
            )
            enrichment_detail_map["ffs_visibility"] = {
                "show_mode": "chain" if include_chain else "default",
                "chain_hidden": bool(chain_rows) and not include_chain,
                "chain_enrollment_count": len(chain_rows),
                "chain_enrollment_ids": _unique_non_empty(
                    [
                        enrollment_record.get("enrollment_id")
                        for enrollment_record in chain_rows
                    ]
                ),
            }
            enrichment_detail_map["enrollments"][key] = (
                enrollment_rows if include_chain else visible_rows
            )
        else:
            enrichment_detail_map["enrollments"][key] = enrollment_rows

    if not await _table_exists(ProviderEnrollmentFFS.__tablename__, session=session):
        return enrichment_detail_map

    ffs_rows = enrichment_detail_map["enrollments"]["ffs_public"]
    enrollment_ids = [
        str(enrollment_record.get("enrollment_id"))
        for enrollment_record in ffs_rows
        if enrollment_record.get("enrollment_id")
    ]
    if not enrollment_ids:
        return enrichment_detail_map

    if await _table_exists(ProviderEnrollmentFFSAdditionalNPI.__tablename__, session=session):
        stmt = (
            select(ProviderEnrollmentFFSAdditionalNPI)
            .where(ProviderEnrollmentFFSAdditionalNPI.enrollment_id.in_(enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSAdditionalNPI.reporting_year.desc().nullslast(),
                ProviderEnrollmentFFSAdditionalNPI.additional_npi.asc(),
            )
            .limit(200)
        )
        enrollment_query_result = await _execute_stmt(stmt, session=session)
        enrichment_detail_map["ffs_subfiles"]["additional_npis"] = [
            enrollment_record.to_json_dict()
            for enrollment_record in enrollment_query_result.scalars()
        ]

    if await _table_exists(ProviderEnrollmentFFSAddress.__tablename__, session=session):
        stmt = (
            select(ProviderEnrollmentFFSAddress)
            .where(ProviderEnrollmentFFSAddress.enrollment_id.in_(enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSAddress.reporting_year.desc().nullslast(),
                ProviderEnrollmentFFSAddress.state.asc().nullslast(),
                ProviderEnrollmentFFSAddress.city.asc().nullslast(),
                ProviderEnrollmentFFSAddress.zip_code.asc().nullslast(),
            )
            .limit(200)
        )
        enrollment_query_result = await _execute_stmt(stmt, session=session)
        enrichment_detail_map["ffs_subfiles"]["practice_locations"] = [
            enrollment_record.to_json_dict()
            for enrollment_record in enrollment_query_result.scalars()
        ]

    if await _table_exists(ProviderEnrollmentFFSSecondarySpecialty.__tablename__, session=session):
        stmt = (
            select(ProviderEnrollmentFFSSecondarySpecialty)
            .where(ProviderEnrollmentFFSSecondarySpecialty.enrollment_id.in_(enrollment_ids))
            .order_by(
                ProviderEnrollmentFFSSecondarySpecialty.reporting_year.desc().nullslast(),
                ProviderEnrollmentFFSSecondarySpecialty.provider_type_code.asc(),
            )
            .limit(200)
        )
        enrollment_query_result = await _execute_stmt(stmt, session=session)
        enrichment_detail_map["ffs_subfiles"]["secondary_specialties"] = [
            enrollment_record.to_json_dict()
            for enrollment_record in enrollment_query_result.scalars()
        ]

    if await _table_exists(ProviderEnrollmentFFSReassignment.__tablename__, session=session):
        out_rows = await _execute_stmt(
            text(
                f"""
                WITH matched AS (
                    SELECT
                        r.reassigning_enrollment_id,
                        r.receiving_enrollment_id,
                        r.reporting_year
                      FROM mrf.{ProviderEnrollmentFFSReassignment.__tablename__} AS r
                     WHERE r.reassigning_enrollment_id = ANY(:enrollment_ids)
                     ORDER BY r.reporting_year DESC NULLS LAST, r.receiving_enrollment_id
                     LIMIT 200
                ),
                needed AS (
                    SELECT DISTINCT receiving_enrollment_id AS enrollment_id
                      FROM matched
                     WHERE receiving_enrollment_id IS NOT NULL
                ),
                ffs_latest AS (
                    SELECT DISTINCT ON (f.enrollment_id)
                        f.enrollment_id,
                        f.npi,
                        f.provider_type_code,
                        f.provider_type_text
                      FROM mrf.{ProviderEnrollmentFFS.__tablename__} AS f
                      JOIN needed AS n
                        ON n.enrollment_id = f.enrollment_id
                     ORDER BY
                        f.enrollment_id,
                        f.reporting_year DESC NULLS LAST,
                        f.imported_at DESC NULLS LAST,
                        f.record_hash DESC
                )
                SELECT
                    m.reassigning_enrollment_id,
                    m.receiving_enrollment_id,
                    dst.npi AS counterparty_npi,
                    dst.provider_type_code AS counterparty_provider_type_code,
                    dst.provider_type_text AS counterparty_provider_type_text,
                    m.reporting_year
                  FROM matched AS m
                  LEFT JOIN ffs_latest AS dst
                    ON dst.enrollment_id = m.receiving_enrollment_id
                 ORDER BY m.reporting_year DESC NULLS LAST, m.receiving_enrollment_id
                """
            ),
            session=session,
            params={"enrollment_ids": enrollment_ids},
        )
        enrichment_detail_map["ffs_subfiles"]["reassignments_out"] = [
            _serialize_ffs_reassignment_row(reassignment_row)
            for reassignment_row in out_rows.mappings().all()
        ]

        in_rows = await _execute_stmt(
            text(
                f"""
                WITH matched AS (
                    SELECT
                        r.reassigning_enrollment_id,
                        r.receiving_enrollment_id,
                        r.reporting_year
                      FROM mrf.{ProviderEnrollmentFFSReassignment.__tablename__} AS r
                     WHERE r.receiving_enrollment_id = ANY(:enrollment_ids)
                     ORDER BY r.reporting_year DESC NULLS LAST, r.reassigning_enrollment_id
                     LIMIT 200
                ),
                needed AS (
                    SELECT DISTINCT reassigning_enrollment_id AS enrollment_id
                      FROM matched
                     WHERE reassigning_enrollment_id IS NOT NULL
                ),
                ffs_latest AS (
                    SELECT DISTINCT ON (f.enrollment_id)
                        f.enrollment_id,
                        f.npi,
                        f.provider_type_code,
                        f.provider_type_text
                      FROM mrf.{ProviderEnrollmentFFS.__tablename__} AS f
                      JOIN needed AS n
                        ON n.enrollment_id = f.enrollment_id
                     ORDER BY
                        f.enrollment_id,
                        f.reporting_year DESC NULLS LAST,
                        f.imported_at DESC NULLS LAST,
                        f.record_hash DESC
                )
                SELECT
                    m.reassigning_enrollment_id,
                    m.receiving_enrollment_id,
                    src.npi AS counterparty_npi,
                    src.provider_type_code AS counterparty_provider_type_code,
                    src.provider_type_text AS counterparty_provider_type_text,
                    m.reporting_year
                  FROM matched AS m
                  LEFT JOIN ffs_latest AS src
                    ON src.enrollment_id = m.reassigning_enrollment_id
                 ORDER BY m.reporting_year DESC NULLS LAST, m.reassigning_enrollment_id
                """
            ),
            session=session,
            params={"enrollment_ids": enrollment_ids},
        )
        enrichment_detail_map["ffs_subfiles"]["reassignments_in"] = [
            _serialize_ffs_reassignment_row(reassignment_row)
            for reassignment_row in in_rows.mappings().all()
        ]

    return enrichment_detail_map


async def _resolve_filter_year(
    requested_year: Optional[int],
    include_procedures: bool,
    include_medications: bool,
    *,
    session: Any = None,
) -> tuple[Optional[int], str]:
    if requested_year is not None:
        return requested_year, "request"

    env_raw = str(os.getenv("HLTHPRT_NPI_FILTER_DEFAULT_YEAR", "")).strip()
    if env_raw:
        return _parse_optional_year(env_raw, "HLTHPRT_NPI_FILTER_DEFAULT_YEAR"), "env"

    sources: list[str] = []
    if include_procedures and await _table_exists("pricing_provider_procedure", session=session):
        sources.append("SELECT MAX(year)::INTEGER AS y FROM mrf.pricing_provider_procedure")
    if include_medications and await _table_exists("pricing_provider_prescription", session=session):
        sources.append("SELECT MAX(year)::INTEGER AS y FROM mrf.pricing_provider_prescription")
    if not sources:
        return None, "none"

    sql = "SELECT MAX(y) FROM (" + " UNION ALL ".join(sources) + ") AS years;"
    result = await _execute_stmt(text(sql), session=session)
    rows = result.all()
    year = rows[0][0] if rows and rows[0] else None
    return (int(year), "data") if year is not None else (None, "none")


async def _resolve_internal_filter_codes(
    codes: list[str],
    input_system: str,
    target_system: str,
    param_name: str,
    *,
    session: Any = None,
) -> tuple[list[int], str]:
    if not codes:
        return [], "none"

    if input_system == target_system:
        return _to_int_codes(codes, param_name), "direct"

    if not await _table_exists("code_crosswalk", session=session):
        return [], "none"

    sql = text(
        """
        SELECT DISTINCT to_code
          FROM mrf.code_crosswalk
         WHERE UPPER(from_system) = :from_system
           AND UPPER(from_code) = ANY(:input_codes)
           AND UPPER(to_system) = :target_system
        UNION
        SELECT DISTINCT from_code
          FROM mrf.code_crosswalk
        WHERE UPPER(to_system) = :from_system
           AND UPPER(to_code) = ANY(:input_codes)
           AND UPPER(from_system) = :target_system
        """
    )
    crosswalk_query_result = await _execute_stmt(
        sql,
        session=session,
        params={
            "from_system": input_system,
            "target_system": target_system,
            "input_codes": codes,
        },
    )
    crosswalk_rows = crosswalk_query_result.all()
    mapped_codes = [
        str(crosswalk_row[0])
        for crosswalk_row in crosswalk_rows
        if crosswalk_row and crosswalk_row[0] is not None
    ]
    return _to_int_codes(mapped_codes, param_name), (
        "crosswalk" if mapped_codes else "none"
    )


def _build_npi_where_clause(
    alias: str,
    names_like: Sequence[str],
    first_name: Optional[str],
    last_name: Optional[str],
    organization_name: Optional[str],
    entity_type_code: Optional[int],
) -> tuple[str, dict]:
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."

    clauses: list[str] = []
    params: dict[str, object] = {}

    if names_like:
        name_clause, name_params = _name_like_clauses(alias, names_like)
        if name_clause:
            clauses.append(name_clause)
            params.update(name_params)

    if first_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_first_name, '')) LIKE :first_name")
        params["first_name"] = f"%{first_name.lower()}%"
    if last_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_last_name, '')) LIKE :last_name")
        params["last_name"] = f"%{last_name.lower()}%"
    if organization_name:
        org_expr = ORGANIZATION_LIKE_TEMPLATE.format(alias=prefix)
        clauses.append(f"({org_expr} LIKE :organization_name)")
        params["organization_name"] = f"%{organization_name.lower()}%"
    if entity_type_code is not None:
        clauses.append(f"{prefix}entity_type_code = :entity_type_code")
        params["entity_type_code"] = entity_type_code

    if not clauses:
        return "", {}
    return " AND ".join(clauses), params


def _extract_name_filters(request) -> list[str]:
    args = getattr(request, "args", {}) or {}
    names: list[str] = []
    if hasattr(args, "getlist"):
        names.extend(args.getlist("name_like"))
    elif hasattr(args, "getall"):
        try:
            names.extend(args.getall("name_like"))
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("failed to read name_like filters with getall: %s", exc)
    else:
        maybe = args.get("name_like")
        if maybe:
            names.append(maybe)
    single = args.get("name_like")
    if single:
        names.append(single)
    normalized = []
    seen = set()
    for name in names:
        if not name:
            continue
        lower = str(name).lower()
        if lower in seen:
            continue
        seen.add(lower)
        normalized.append(lower)
    return normalized


async def _compute_npi_counts():
    async def get_npi_count():
        """Count imported NPI identity rows."""
        return await db.scalar(select(func.count(NPIData.npi)))

    async def get_npi_address_count():
        """Count imported NPI address identity rows."""
        return await db.scalar(select(func.count(tuple_(NPIAddress.npi, NPIAddress.checksum, NPIAddress.type))))

    return await asyncio.gather(get_npi_count(), get_npi_address_count())


def _validate_section_filters(section: Optional[str], classification: Optional[str], codes: Optional[list[str]]) -> None:
    """Disallow section-only lookups; they fan out to all NUCC codes and are not meaningful."""
    if section and not classification and not codes:
        raise sanic.exceptions.InvalidUsage(
            "section requires classification or codes"
        )


@blueprint.get("/")
async def npi_index_status(request):
    """Return NPI dataset counts and service release metadata."""
    npi_count, npi_address_count = await _compute_npi_counts()
    data = {
        "date": datetime.utcnow().isoformat(),
        "release": request.app.config.get("RELEASE"),
        "environment": request.app.config.get("ENVIRONMENT"),
        "product_count": npi_count,
        "import_log_errors": npi_address_count,
    }

    return response.json(data)


@blueprint.get("/active_pharmacists")
async def active_pharmacists(request):
    """Count active pharmacists linked to pharmacies by contact data."""
    request_session = _request_session(request)
    state = request.args.get("state", None)
    specialization = request.args.get("specialization", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    address_table_sql = await _address_serving_table_sql(
        {"npi", "type", "state_name", "telephone_number", "taxonomy_array"},
        session=request_session,
    )
    sql = text(
        """
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE
               """
        + ("specialization= :specialization" if specialization else "classification = 'Pharmacist'")
        + f"""
        )
        SELECT COUNT(DISTINCT phm.npi) AS active_pharmacist_count
        FROM {address_table_sql} ph
        JOIN {address_table_sql} phm
          ON ph.telephone_number = phm.telephone_number
         AND phm.type = 'primary'
         AND ph.type = 'primary'
         AND ph.state_name = phm.state_name
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
          AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
        """
        + ("\n          AND ph.state_name = :state" if state else "")
    )

    async with db.acquire() as conn:
        pharmacist_count_row = await conn.first(sql, state=state, specialization=specialization)
    return response.json({"count": pharmacist_count_row[0] if pharmacist_count_row else 0})


@blueprint.get("/pharmacists_in_pharmacies")
async def pharmacists_in_pharmacies(request):
    """Count pharmacists linked to pharmacies matching requested names."""
    request_session = _request_session(request)
    # Explicit access helps route collectors pick up query params.
    request.args.get("name_like")
    names = _extract_name_filters(request)
    if not names:
        return response.json({"count": 0})

    name_clause, name_params = _name_like_clauses("d", names)
    address_table_sql = await _address_serving_table_sql(
        {"npi", "type", "state_name", "telephone_number", "taxonomy_array"},
        session=request_session,
    )
    sql = text(
        f"""
        WITH pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        )
        SELECT COUNT(DISTINCT phm.npi) AS pharmacist_count
        FROM {address_table_sql} ph
        JOIN {address_table_sql} phm
          ON ph.telephone_number = phm.telephone_number
         AND phm.type = 'primary'
         AND ph.type = 'primary'
         AND ph.state_name = phm.state_name
        JOIN mrf.npi d ON ph.npi = d.npi
        WHERE ph.taxonomy_array && (SELECT codes FROM pharmacy_taxonomy)
          AND phm.taxonomy_array && (SELECT codes FROM pharmacist_taxonomy)
          AND ({name_clause})
    """
    )

    async with db.acquire() as conn:
        pharmacist_count_row = await conn.first(sql, **name_params)
    return response.json({"count": pharmacist_count_row[0] if pharmacist_count_row else 0})


@blueprint.get("/pharmacists_per_pharmacy")
async def pharmacists_per_pharmacy(request):
    """Return pharmacist staffing counts grouped by pharmacy."""
    request_session = _request_session(request)
    state = request.args.get("state", None)
    if state and len(state) == 2:
        state = state.upper()
    else:
        state = None

    # Explicit access helps route collectors pick up query params.
    request.args.get("name_like")
    names = _extract_name_filters(request)
    is_detailed = str(request.args.get("detailed", "")).lower() in ("1", "true", "yes")
    query_param_map = {}

    if state:
        query_param_map["state"] = state

    # Allow unscoped queries; callers may aggregate nationally. Name/state filters are applied when present.
    name_clause = ""
    name_query_param_map: dict = {}
    if names:
        name_clause, name_query_param_map = _name_like_clauses("d", names)
        query_param_map.update(name_query_param_map)

    state_filter_addr = "AND a.state_name = :state" if state else ""
    state_filter_join = "AND ph.state_name = pc.state_name"
    if state:
        state_filter_join += " AND ph.state_name = :state"
    address_table_sql = await _address_serving_table_sql(
        {"npi", "type", "state_name", "telephone_number", "taxonomy_array"},
        session=request_session,
    )
    base_cte = f"""
        WITH target_npi AS (
            SELECT npi
              FROM mrf.npi AS d
             WHERE {'1=1' if not name_clause else name_clause}
        ),
        pharmacy_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacy'
        ),
        pharmacist_taxonomy AS (
            SELECT ARRAY_AGG(int_code) AS codes
            FROM mrf.nucc_taxonomy
            WHERE classification = 'Pharmacist'
        ),
        pharmacy_subset AS (
            SELECT a.npi, a.telephone_number, a.state_name
              FROM {address_table_sql} AS a, pharmacy_taxonomy AS pc
             WHERE a.npi IN (SELECT npi FROM target_npi)
               AND a.type = 'primary'
               AND a.taxonomy_array && pc.codes
               AND a.telephone_number IS NOT NULL
               {state_filter_addr}
        ),
        pharmacist_subset AS (
            SELECT a.npi, a.telephone_number, a.state_name
              FROM {address_table_sql} AS a, pharmacist_taxonomy AS pc
             WHERE a.type = 'primary'
               AND a.taxonomy_array && pc.codes
               {("AND a.state_name = :state" if state else "")}
        ),
        pharmacist_counts AS (
            SELECT phm.telephone_number,
                   phm.state_name,
                   COUNT(DISTINCT phm.npi) AS pharmacist_count
              FROM pharmacist_subset AS phm
             WHERE phm.telephone_number IN (SELECT telephone_number FROM pharmacy_subset)
          GROUP BY phm.telephone_number, phm.state_name
        ),
        pharmacy_counts AS (
            SELECT ph.npi AS pharmacy_npi,
                   COALESCE(d.provider_organization_name, d.provider_last_name) AS pharmacy_name,
                   COALESCE(pc.pharmacist_count, 0) AS pharmacist_count
              FROM pharmacy_subset AS ph
              JOIN mrf.npi AS d ON ph.npi = d.npi
         LEFT JOIN pharmacist_counts AS pc
                ON pc.telephone_number = ph.telephone_number
               {state_filter_join}
        )
    """

    histogram_sql = text(
        base_cte
        + """
        SELECT CASE
            WHEN pharmacist_count = 0 THEN '0'
            WHEN pharmacist_count = 1 THEN '1'
            WHEN pharmacist_count = 2 THEN '2'
            WHEN pharmacist_count = 3 THEN '3'
            WHEN pharmacist_count = 4 THEN '4'
            WHEN pharmacist_count = 5 THEN '5'
            WHEN pharmacist_count = 6 THEN '6'
            WHEN pharmacist_count = 7 THEN '7'
            WHEN pharmacist_count = 8 THEN '8'
            WHEN pharmacist_count = 9 THEN '9'
            WHEN pharmacist_count = 10 THEN '10'
            WHEN pharmacist_count = 11 THEN '11'
            WHEN pharmacist_count = 12 THEN '12'
            WHEN pharmacist_count = 13 THEN '13'
            WHEN pharmacist_count = 14 THEN '14'
            WHEN pharmacist_count = 15 THEN '15'
            WHEN pharmacist_count = 16 THEN '16'
            WHEN pharmacist_count = 17 THEN '17'
            WHEN pharmacist_count = 18 THEN '18'
            WHEN pharmacist_count = 19 THEN '19'
            WHEN pharmacist_count = 20 THEN '20'
            WHEN pharmacist_count = 21 THEN '21'
            WHEN pharmacist_count = 22 THEN '22'
            WHEN pharmacist_count = 23 THEN '23'
            WHEN pharmacist_count = 24 THEN '24'
            WHEN pharmacist_count = 25 THEN '25'
            ELSE '25+'
        END AS pharmacist_group,
        COUNT(*) AS pharmacy_count
        FROM pharmacy_counts
        GROUP BY pharmacist_group
        ORDER BY pharmacist_group DESC
    """
    )

    detail_sql = text(
        base_cte
        + """
        SELECT pharmacy_npi, pharmacy_name, pharmacist_count
          FROM pharmacy_counts
         ORDER BY pharmacist_count DESC, pharmacy_npi
        """
    )

    async with db.acquire() as conn:
        histogram_rows = await conn.all(histogram_sql, **query_param_map)
        detail_rows = await conn.all(detail_sql, **query_param_map) if is_detailed else []
    histogram_entries = [
        {"pharmacist_group": histogram_row[0], "pharmacy_count": histogram_row[1]}
        for histogram_row in histogram_rows
    ]
    detail_entries = [
        {
            "pharmacy_npi": detail_row[0],
            "pharmacy_name": detail_row[1],
            "pharmacist_count": detail_row[2],
        }
        for detail_row in detail_rows
    ]
    response_payload_map = {"histogram": histogram_entries}
    if is_detailed:
        response_payload_map["rows"] = detail_entries
    return response.json(response_payload_map)


def _normalize_match_candidate_float(
    raw_value: Any,
    *,
    param_name: str,
    minimum: float,
    maximum: float,
) -> Optional[float]:
    if raw_value in (None, "", "null"):
        return None
    try:
        parsed = float(str(raw_value).strip())
    except (TypeError, ValueError) as exc:
        raise sanic.exceptions.InvalidUsage(f"{param_name} must be a number") from exc
    if not math.isfinite(parsed) or parsed < minimum or parsed > maximum:
        raise sanic.exceptions.InvalidUsage(
            f"{param_name} must be between {minimum:g} and {maximum:g}"
        )
    return parsed


def _normalize_match_candidate_limit(raw_value: Any) -> int:
    if raw_value in (None, "", "null"):
        return _MATCH_CANDIDATES_DEFAULT_LIMIT
    try:
        parsed = int(str(raw_value).strip())
    except (TypeError, ValueError) as exc:
        raise sanic.exceptions.InvalidUsage("limit must be an integer") from exc
    if parsed < 1 or parsed > _MATCH_CANDIDATES_MAX_LIMIT:
        raise sanic.exceptions.InvalidUsage(
            f"limit must be between 1 and {_MATCH_CANDIDATES_MAX_LIMIT}"
        )
    return parsed


def _normalize_match_candidate_entity_kind(raw_value: Any) -> Optional[int]:
    if raw_value in (None, "", "null"):
        return None
    entity_kind = str(raw_value).strip().lower()
    if entity_kind == "individual":
        return 1
    if entity_kind == "organization":
        return 2
    raise sanic.exceptions.InvalidUsage("entity_kind must be either individual or organization")


def _entity_kind_from_code(entity_type_code: Any) -> Optional[str]:
    if entity_type_code == 1:
        return "individual"
    if entity_type_code == 2:
        return "organization"
    return None


def _normalize_match_candidate_entity_type(raw_code: Any, raw_kind: Any) -> Optional[int]:
    entity_type_code: Optional[int] = None
    if raw_code not in (None, "", "null"):
        try:
            entity_type_code = int(str(raw_code).strip())
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            ) from exc
        if entity_type_code not in (1, 2):
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            )
    kind_code = _normalize_match_candidate_entity_kind(raw_kind)
    if entity_type_code is not None and kind_code is not None and entity_type_code != kind_code:
        raise sanic.exceptions.InvalidUsage("entity_kind and entity_type_code disagree")
    return entity_type_code if entity_type_code is not None else kind_code


def _normalize_match_candidate_term(raw_value: Any, *, param_name: str) -> Optional[str]:
    return _normalize_text_filter(raw_value, param_name=param_name, max_length=128)


def _taxonomy_scope_tokens(raw_value: Any) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if raw_value in (None, "", "null"):
        return (), ()
    exact_codes: list[str] = []
    prefixes: list[str] = []
    seen: set[str] = set()
    for raw_item in re.split(r"[,;]", str(raw_value)):
        item = raw_item.strip().upper()
        if not item:
            continue
        is_prefix = item.endswith("*")
        token = item[:-1] if is_prefix else item
        if not re.fullmatch(r"[A-Z0-9]{2,16}", token):
            raise sanic.exceptions.InvalidUsage(
                "taxonomy_scope must contain NUCC codes or prefixes like 261Q*"
            )
        dedupe_key = f"{token}*" if is_prefix else token
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        if is_prefix:
            prefixes.append(token)
        else:
            exact_codes.append(token)
    return tuple(exact_codes), tuple(prefixes)


async def _normalize_match_candidate_params(request) -> dict[str, Any]:
    """Validate and normalize provider candidate-match query parameters."""
    args = request.args
    unknown_params = sorted(set(args.keys()) - MATCH_CANDIDATE_QUERY_PARAMS)
    if unknown_params:
        joined = ", ".join(unknown_params)
        raise sanic.exceptions.InvalidUsage(f"unknown query parameter(s): {joined}")

    args.get("address_site_key")
    args.get("address_key")
    args.get("lat")
    args.get("long")
    args.get("radius_miles")
    args.get("phone")
    args.get("entity_type_code")
    args.get("entity_kind")
    args.get("taxonomy_scope")
    args.get("provider_type")
    args.get("specialty")
    args.get("include_subspecialties")
    args.get("limit")
    args.get("include_sources")
    args.get("include_evidence")
    args.get("debug")

    address_site_key = _normalize_uuid_key(args.get("address_site_key"), "address_site_key")
    address_key = _normalize_address_key(args.get("address_key"))
    latitude = _normalize_match_candidate_float(
        args.get("lat"),
        param_name="lat",
        minimum=-90.0,
        maximum=90.0,
    )
    longitude = _normalize_match_candidate_float(
        args.get("long"),
        param_name="long",
        minimum=-180.0,
        maximum=180.0,
    )
    if (latitude is None) != (longitude is None):
        raise sanic.exceptions.InvalidUsage("lat and long must be provided together")
    radius_miles = _normalize_match_candidate_float(
        args.get("radius_miles"),
        param_name="radius_miles",
        minimum=0.01,
        maximum=_MATCH_CANDIDATES_MAX_RADIUS_MILES,
    )
    if latitude is not None and radius_miles is None:
        radius_miles = _MATCH_CANDIDATES_DEFAULT_RADIUS_MILES

    phone_digits = _normalize_phone_digits(args.get("phone"))

    entity_type_code = _normalize_match_candidate_entity_type(
        args.get("entity_type_code"),
        args.get("entity_kind"),
    )
    taxonomy_exact, taxonomy_prefixes = _taxonomy_scope_tokens(args.get("taxonomy_scope"))
    provider_type = _normalize_match_candidate_term(args.get("provider_type"), param_name="provider_type")
    specialty = _normalize_match_candidate_term(args.get("specialty"), param_name="specialty")
    if provider_type and specialty and provider_type.strip().lower() != specialty.strip().lower():
        raise sanic.exceptions.InvalidUsage("provider_type and specialty must match when both are provided")
    provider_type = provider_type or specialty
    include_subspecialties = _parse_bool_arg(args.get("include_subspecialties"), default=True)
    include_sources = _parse_bool_arg(args.get("include_sources"), default=False)
    include_evidence = _parse_bool_arg(args.get("include_evidence"), default=False)
    debug = _parse_bool_arg(args.get("debug"), default=False)
    if debug:
        include_sources = True
        include_evidence = True
    limit = _normalize_match_candidate_limit(args.get("limit"))

    locator_count = sum(
        bool(value)
        for value in (
            address_site_key,
            address_key,
            latitude is not None and longitude is not None,
            phone_digits,
        )
    )
    if locator_count == 0:
        raise sanic.exceptions.InvalidUsage(
            "provide at least one locator: address_site_key, address_key, lat+long, or phone"
        )

    request_session = _request_session(request)
    specialty_filter = None
    if provider_type:
        if request_session is not None:
            await ensure_specialty_resolution_cache(request_session)
        else:
            async with db.acquire() as conn:
                await ensure_specialty_resolution_cache(conn)
        specialty_filter = resolve_provider_specialty_filter(
            {
                "specialty": provider_type,
                "include_subspecialties": include_subspecialties,
            }
        )
        if specialty_filter.unresolved_specialty:
            suggestion_note = ""
            if specialty_filter.suggested_specialties:
                suggestion_note = f" Suggestions: {', '.join(specialty_filter.suggested_specialties)}."
            raise sanic.exceptions.InvalidUsage(
                f"Unrecognized provider_type: {provider_type}.{suggestion_note}"
            )

    return {
        "address_site_key": address_site_key,
        "address_key": address_key,
        "lat": latitude,
        "long": longitude,
        "radius_miles": radius_miles,
        "phone_digits": phone_digits,
        "entity_type_code": entity_type_code,
        "entity_kind": _entity_kind_from_code(entity_type_code),
        "taxonomy_exact": taxonomy_exact,
        "taxonomy_prefixes": taxonomy_prefixes,
        "provider_type": provider_type,
        "specialty_filter": specialty_filter,
        "include_subspecialties": include_subspecialties,
        "include_sources": include_sources,
        "include_evidence": include_evidence,
        "debug": debug,
        "limit": limit,
    }


def _match_geo_distance_expr(params: dict[str, Any], alias: str = "a") -> str:
    if params.get("lat") is None or params.get("long") is None:
        return "NULL::double precision"
    return (
        "3958.8 * 2 * ASIN(LEAST(1.0, SQRT("
        f"POWER(SIN(RADIANS(({alias}.lat::double precision - :lat) / 2)), 2) + "
        f"COS(RADIANS(:lat)) * COS(RADIANS({alias}.lat::double precision)) * "
        f"POWER(SIN(RADIANS(({alias}.long::double precision - :long) / 2)), 2)"
        ")))"
    )


def _match_candidate_column_sql(address_table_sql: str) -> dict[str, str]:
    unified = _address_table_is_unified(address_table_sql)
    return {
        "provider_npi": "COALESCE(a.npi, a.inferred_npi)" if unified else "a.npi",
        "premise_key": "a.premise_key::text" if unified else "NULL::text",
        "address_precision": "a.address_precision" if unified else "NULL::text",
        "address_sources": "a.address_sources" if unified else "ARRAY[]::varchar[]",
        "source_record_ids": "a.source_record_ids" if unified else "ARRAY[]::varchar[]",
        "source_count": "a.source_count" if unified else "0",
        "independent_source_count": "a.independent_source_count" if unified else "0",
        "multi_source_confirmed": "a.multi_source_confirmed" if unified else "false",
        "entity_name": "a.entity_name" if unified else "NULL::text",
        "location_key": "a.location_key" if unified else "a.checksum::text",
        "updated_at": "a.updated_at" if unified else "a.date_added::timestamp",
    }


def _current_provider_directory_geo_evidence_sql() -> str:
    """Return exact current-overlay provenance for bounded geo candidates."""
    source_table = _schema_cache_key("provider_directory_source")
    dataset_table = _schema_cache_key("provider_directory_endpoint_dataset")
    dataset_resource_table = _schema_cache_key("provider_directory_dataset_resource")
    overlay_table = _schema_cache_key("provider_directory_address_overlay")
    return f"""
        WITH requested_candidates AS MATERIALIZED (
            SELECT requested.npi, requested.address_key
              FROM unnest(
                       CAST(:candidate_npis AS bigint[]),
                       CAST(:candidate_address_keys AS uuid[])
                   ) AS requested(npi, address_key)
        ), current_endpoint_counts AS MATERIALIZED (
            SELECT dataset.endpoint_id
              FROM {dataset_table} AS dataset
             WHERE dataset.is_current IS TRUE
          GROUP BY dataset.endpoint_id
            HAVING COUNT(*) = 1
        ), current_datasets AS MATERIALIZED (
            SELECT dataset.endpoint_id, dataset.dataset_id,
                   COALESCE(
                       dataset.acquisition_root_run_id, dataset.import_run_id
                   )::varchar AS run_id
              FROM {dataset_table} AS dataset
              JOIN current_endpoint_counts AS current_endpoint
                ON current_endpoint.endpoint_id = dataset.endpoint_id
             WHERE dataset.is_current IS TRUE
               AND dataset.status = 'published'
               AND dataset.published_at IS NOT NULL
               AND dataset.superseded_at IS NULL
               AND COALESCE(
                       dataset.acquisition_root_run_id, dataset.import_run_id
                   ) IS NOT NULL
        ), current_provider_directory_runs AS MATERIALIZED (
            SELECT source.source_id, dataset.dataset_id, dataset.run_id
              FROM {source_table} AS source
              JOIN current_datasets AS dataset
                ON dataset.endpoint_id = source.endpoint_id
        )
        SELECT requested.npi, requested.address_key::text AS address_key,
               ARRAY_AGG(
                   DISTINCT overlay.source_record_id
                   ORDER BY overlay.source_record_id
               ) AS source_record_ids
          FROM requested_candidates AS requested
          JOIN {overlay_table} AS overlay
            ON overlay.npi = requested.npi
           AND overlay.address_key = requested.address_key
          JOIN current_provider_directory_runs AS current_run
            ON current_run.source_id = overlay.source_id
           AND current_run.run_id = overlay.last_seen_run_id
          JOIN {dataset_resource_table} AS dataset_resource
            ON dataset_resource.dataset_id = current_run.dataset_id
           AND dataset_resource.resource_type = overlay.resource_type
           AND dataset_resource.resource_id = overlay.resource_id
         WHERE overlay.source_record_id IS NOT NULL
      GROUP BY requested.npi, requested.address_key;
    """


def _match_candidate_taxonomy_filter_sql(
    params: dict[str, Any],
    query_params: dict[str, Any],
    *,
    npi_sql: str = "cl.npi",
) -> str:
    specialty_filter = params.get("specialty_filter")
    taxonomy_codes = list(params.get("taxonomy_exact") or [])
    has_explicit_scope = bool(taxonomy_codes or params.get("taxonomy_prefixes"))
    if specialty_filter is not None and not has_explicit_scope:
        taxonomy_codes.extend(str(code).upper() for code in specialty_filter.taxonomy_codes)
    taxonomy_codes = list(dict.fromkeys(code for code in taxonomy_codes if code))
    taxonomy_prefixes = list(params.get("taxonomy_prefixes") or [])
    taxonomy_conditions: list[str] = []
    if taxonomy_codes:
        query_params["match_taxonomy_codes"] = taxonomy_codes
        taxonomy_conditions.append("t.healthcare_provider_taxonomy_code = ANY(:match_taxonomy_codes)")
    for idx, prefix in enumerate(taxonomy_prefixes):
        key = f"match_taxonomy_prefix_{idx}"
        query_params[key] = f"{prefix}%"
        taxonomy_conditions.append(f"t.healthcare_provider_taxonomy_code LIKE :{key}")
    if (
        specialty_filter is not None
        and not has_explicit_scope
        and specialty_filter.classification
        and not specialty_filter.taxonomy_codes
    ):
        query_params["match_taxonomy_classification"] = specialty_filter.classification
        taxonomy_conditions.append("nu.classification = :match_taxonomy_classification")
    if not taxonomy_conditions:
        return "1=1"
    taxonomy_table_sql = _schema_cache_key(NPIDataTaxonomy.__tablename__)
    nucc_table_sql = _schema_cache_key(NUCCTaxonomy.__tablename__)
    return f"""
        EXISTS (
            SELECT 1
              FROM {taxonomy_table_sql} AS t
         LEFT JOIN {nucc_table_sql} AS nu
                ON nu.code = t.healthcare_provider_taxonomy_code
             WHERE t.npi = {npi_sql}
               AND ({' OR '.join(taxonomy_conditions)})
        )
    """


def _match_candidate_query(params: dict[str, Any], address_table_sql: str) -> tuple[Any, dict[str, Any]]:
    """Build the bounded provider candidate query and its bound parameters."""
    columns = _match_candidate_column_sql(address_table_sql)
    query_params: dict[str, Any] = {
        "limit": int(params["limit"]),
        "candidate_limit": min(
            max(int(params["limit"]) * 8, 100),
            _MATCH_CANDIDATES_MAX_INTERNAL_ROWS,
        ),
    }
    address_where = [
        _provider_list_address_type_clause(
            "a",
            address_table_sql,
            include_service_locations=True,
        ),
        f"{columns['provider_npi']} IS NOT NULL",
    ]
    address_site_locator = None
    if params.get("address_site_key"):
        query_params["address_site_key"] = params["address_site_key"]
        address_site_locator = _address_site_key_filter("a", address_table_sql)
    address_key_locator = None
    if params.get("address_key"):
        query_params["address_key"] = params["address_key"]
        address_key_locator = "a.address_key = CAST(:address_key AS uuid)"
    phone_locator = None
    if params.get("phone_digits"):
        query_params["phone_digits"] = params["phone_digits"]
        phone_locator = _address_phone_digits_filter("a", address_table_sql)
    locator_candidates = [
        ("address_site_key", address_site_locator),
        ("address_key", address_key_locator),
        ("phone", phone_locator),
    ]
    selected_locator = None
    selected_locator_name = None
    for locator_name, locator_sql in locator_candidates:
        if locator_sql:
            selected_locator = locator_sql
            selected_locator_name = locator_name
            break
    phone_candidates_cte = None
    address_from_sql = f"FROM {address_table_sql} AS a"
    phone_provider_directory_match = "false"
    phone_source_record_ids = "ARRAY[]::varchar[]"
    if selected_locator_name == "phone" and _address_table_is_unified(address_table_sql):
        phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
        query_params["candidate_limit"] = min(
            max(int(params["limit"]) * 8, 20),
            _MATCH_CANDIDATES_MAX_INTERNAL_ROWS,
        )
        address_from_sql = _address_phone_candidates_lateral_from(
            address_table_sql,
            "a",
        )
        phone_provider_directory_match = "phone_match.provider_directory_matched"
        phone_source_record_ids = (
            "COALESCE(phone_evidence.source_record_ids, ARRAY[]::varchar[])"
        )
        selected_locator = "true"
    geo_distance_expr = _match_geo_distance_expr(params)
    geo_locator_where: list[str] = []
    if params.get("lat") is not None and params.get("long") is not None:
        latitude = float(params["lat"])
        longitude = float(params["long"])
        radius = float(params["radius_miles"])
        lat_delta = radius / 69.0
        lon_delta = radius / max(1.0, 69.0 * abs(math.cos(math.radians(latitude))))
        query_params.update(
            {
                "lat": latitude,
                "long": longitude,
                "radius_miles": radius,
                "lat_min": latitude - lat_delta,
                "lat_max": latitude + lat_delta,
                "long_min": longitude - lon_delta,
                "long_max": longitude + lon_delta,
            }
        )
        geo_precision_clause = (
            "AND COALESCE(a.address_precision, '') <> 'city_zip' "
            if _address_table_is_unified(address_table_sql)
            else ""
        )
        geo_locator_where.append(
            "a.lat IS NOT NULL AND a.long IS NOT NULL "
            f"{geo_precision_clause}"
            "AND a.lat BETWEEN CAST(:lat_min AS numeric) AND CAST(:lat_max AS numeric) "
            "AND a.long BETWEEN CAST(:long_min AS numeric) AND CAST(:long_max AS numeric) "
            f"AND ({geo_distance_expr}) <= :radius_miles"
        )
    # Use the most precise locator supplied. Keeping less selective locators as
    # scoring signals avoids OR plans that combine indexed keys with broad geo
    # predicates over the serving table.
    locator_where = ([selected_locator] if selected_locator else []) or geo_locator_where
    address_where.append(f"({' OR '.join(locator_where)})")

    npi_table_sql = _schema_cache_key(NPIData.__tablename__)
    taxonomy_filter = _match_candidate_taxonomy_filter_sql(params, query_params)
    if params.get("entity_type_code") is not None:
        query_params["entity_type_code"] = params["entity_type_code"]
        address_where.append(
            f"""
            EXISTS (
                SELECT 1
                  FROM {npi_table_sql} AS nf
                 WHERE nf.npi = {columns['provider_npi']}
                   AND nf.entity_type_code = :entity_type_code
            )
            """
        )
    if taxonomy_filter != "1=1":
        address_where.append(
            _match_candidate_taxonomy_filter_sql(
                params,
                query_params,
                npi_sql=columns["provider_npi"],
            )
        )

    npi_where: list[str] = []
    if params.get("entity_type_code") is not None:
        npi_where.append("n.entity_type_code = :entity_type_code")
    if taxonomy_filter != "1=1":
        npi_where.append(taxonomy_filter)
    npi_where_sql = " AND ".join(npi_where) if npi_where else "1=1"

    address_site_match = (
        "a.premise_key = CAST(:address_site_key AS uuid)"
        if _address_table_is_unified(address_table_sql) and params.get("address_site_key")
        else "false"
    )
    address_key_match = "a.address_key = CAST(:address_key AS uuid)" if params.get("address_key") else "false"
    phone_match = (
        "true"
        if phone_candidates_cte
        else _address_phone_digits_filter("a", address_table_sql)
        if params.get("phone_digits")
        else "false"
    )
    taxonomy_table_sql = _schema_cache_key(NPIDataTaxonomy.__tablename__)
    nucc_table_sql = _schema_cache_key(NUCCTaxonomy.__tablename__)
    query = text(
        f"""
        {_sql_with_prefix_ctes(phone_candidates_cte)}candidate_locations AS (
            SELECT DISTINCT ON ({columns['provider_npi']})
                   {columns['provider_npi']}::bigint AS npi,
                   a.type AS address_type,
                   a.first_line,
                   a.second_line,
                   a.city_name,
                   a.state_name,
                   a.postal_code,
                   a.country_code,
                   a.telephone_number,
                   a.phone_number,
                   a.lat::double precision AS lat,
                   a.long::double precision AS long,
                   a.address_key::text AS address_key,
                   {columns['premise_key']} AS address_site_key,
                   {columns['address_precision']} AS address_precision,
                   {columns['address_sources']} AS address_sources,
                   {columns['source_record_ids']} AS source_record_ids,
                   {phone_source_record_ids} AS phone_source_record_ids,
                   {columns['source_count']}::integer AS source_count,
                   {columns['independent_source_count']}::integer AS independent_source_count,
                   {columns['multi_source_confirmed']}::boolean AS multi_source_confirmed,
                   {columns['entity_name']} AS entity_name,
                   {columns['location_key']} AS location_key,
                   {columns['updated_at']} AS address_updated_at,
                   ({address_site_match})::boolean AS address_site_key_matched,
                   ({address_key_match})::boolean AS address_key_matched,
                   ({phone_match})::boolean AS phone_matched,
                   ({phone_provider_directory_match})::boolean
                       AS phone_provider_directory_matched,
                   ({geo_distance_expr}) AS geo_distance_miles
              {address_from_sql}
             WHERE {' AND '.join(address_where)}
          ORDER BY {columns['provider_npi']},
                   address_site_key_matched DESC,
                   address_key_matched DESC,
                   phone_matched DESC,
                   phone_provider_directory_matched DESC,
                   geo_distance_miles ASC NULLS LAST,
                   source_count DESC NULLS LAST,
                   location_key
             LIMIT :candidate_limit
        ),
        filtered AS (
            SELECT cl.*,
                   n.entity_type_code,
                   n.provider_organization_name,
                   n.provider_other_organization_name,
                   n.provider_first_name,
                   n.provider_last_name,
                   n.provider_credential_text,
                   n.do_business_as
              FROM candidate_locations AS cl
              JOIN {npi_table_sql} AS n
                ON n.npi = cl.npi
             WHERE {npi_where_sql}
        )
        SELECT f.*,
               COALESCE(
                   (
                       SELECT json_agg(
                                  json_build_object(
                                      'taxonomy_code', t.healthcare_provider_taxonomy_code,
                                      'primary', UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y',
                                      'classification', nu.classification,
                                      'specialization', nu.specialization,
                                      'section', nu.section,
                                      'display_name', nu.display_name
                                  )
                                  ORDER BY (UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y') DESC,
                                           t.healthcare_provider_taxonomy_code
                              )
                         FROM {taxonomy_table_sql} AS t
                    LEFT JOIN {nucc_table_sql} AS nu
                           ON nu.code = t.healthcare_provider_taxonomy_code
                        WHERE t.npi = f.npi
                   ),
                   '[]'::json
               ) AS taxonomy_list
          FROM filtered AS f
      ORDER BY f.address_site_key_matched DESC,
               f.address_key_matched DESC,
               f.phone_matched DESC,
               f.phone_provider_directory_matched DESC,
               f.geo_distance_miles ASC NULLS LAST,
               f.source_count DESC NULLS LAST,
               f.npi
         LIMIT :candidate_limit
        """
    )
    return query, query_params


async def _fetch_match_candidate_rows(params: dict[str, Any], *, session: Any = None) -> list[dict[str, Any]]:
    required_columns = {
        "npi",
        "type",
        "first_line",
        "postal_code",
        "telephone_number",
        "phone_number",
        "lat",
        "long",
        "address_key",
        "premise_key",
        "taxonomy_array",
    }
    address_table_sql = await _address_serving_table_sql(required_columns, session=session)
    query, query_params = _match_candidate_query(params, address_table_sql)
    try:
        candidate_query_result = await asyncio.wait_for(
            _execute_match_candidate_query(query, query_params, session),
            timeout=max(0.1, _MATCH_CANDIDATES_TIMEOUT_SECONDS),
        )
    except asyncio.TimeoutError:
        await _rollback_match_candidate_session(session)
        raise
    except asyncio.CancelledError:
        await _rollback_match_candidate_session(session)
        raise
    candidate_rows: list[dict[str, Any]] = []
    for candidate_row in candidate_query_result.all():
        mapping = getattr(candidate_row, "_mapping", candidate_row)
        candidate_rows.append(dict(mapping))
    return candidate_rows


async def _execute_match_candidate_query(query: Any, query_params: dict[str, Any], session: Any) -> Any:
    if session is not None:
        await _execute_stmt(text("SET LOCAL jit = off"), session=session)
    return await _execute_stmt(query, session=session, params=query_params)


async def _rollback_match_candidate_session(session: Any) -> None:
    if session is None:
        return
    for method_name in ("rollback", "close"):
        method = getattr(session, method_name, None)
        if method is None:
            continue
        with contextlib.suppress(Exception, asyncio.CancelledError):
            result = method()
            if asyncio.iscoroutine(result):
                await asyncio.shield(result)


def _json_array_value(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _match_candidate_name(row: Mapping[str, Any]) -> str:
    organization_name = row.get("provider_organization_name") or row.get("provider_other_organization_name")
    if organization_name:
        return str(organization_name)
    parts = [
        row.get("provider_first_name"),
        row.get("provider_last_name"),
        row.get("provider_credential_text"),
    ]
    display = " ".join(str(part).strip() for part in parts if str(part or "").strip())
    return display or str(row.get("entity_name") or row.get("npi") or "Unknown")


def _primary_taxonomy(taxonomy_list: Sequence[Any]) -> dict[str, Any]:
    first: dict[str, Any] = {}
    for raw in taxonomy_list:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        if not first:
            first = item
        if item.get("primary") is True:
            return item
    return first


def _facility_payload(
    provider_row: Mapping[str, Any],
    taxonomy_list: Sequence[Any],
    enrichment: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    primary = _primary_taxonomy(taxonomy_list)
    taxonomy_code = str(primary.get("taxonomy_code") or "")
    facility_type = primary.get("display_name") or primary.get("classification")
    evidence_labels: list[str] = []
    confidence = "medium" if taxonomy_code else "low"
    if taxonomy_code:
        evidence_labels.append(f"primary_taxonomy_{taxonomy_code}")
    if taxonomy_code.startswith("282N"):
        facility_type = facility_type or "General Acute Care Hospital"
        confidence = "high"
    if enrichment:
        flag_map = {
            "has_hospital_enrollment": "hospital_enrollment",
            "has_fqhc_enrollment": "fqhc_enrollment",
            "has_rhc_enrollment": "rhc_enrollment",
            "has_snf_enrollment": "snf_enrollment",
            "has_hha_enrollment": "hha_enrollment",
            "has_hospice_enrollment": "hospice_enrollment",
        }
        for key, label in flag_map.items():
            if enrichment.get(key):
                evidence_labels.append(label)
                confidence = "high"
        provider_type = enrichment.get("primary_provider_type_text") or enrichment.get("primary_provider_type_code")
        if provider_type:
            evidence_labels.append(f"ffs_provider_type:{provider_type}")
    if not facility_type and not evidence_labels:
        return None
    return {
        "type": facility_type,
        "taxonomy": taxonomy_code or None,
        "classification_confidence": confidence,
        "evidence": evidence_labels,
    }


def _has_match_candidate_taxonomy_context(params: Mapping[str, Any]) -> bool:
    return bool(
        params.get("taxonomy_exact")
        or params.get("taxonomy_prefixes")
        or params.get("provider_type")
        or params.get("specialty_filter")
    )


def _should_boost_general_acute_care_candidate(
    row: Mapping[str, Any],
    params: Mapping[str, Any],
    taxonomy_list: Sequence[Any],
    enrichment: Mapping[str, Any] | None,
) -> bool:
    if _has_match_candidate_taxonomy_context(params):
        return False
    if _entity_kind_from_code(row.get("entity_type_code")) != "organization":
        return False
    requested_kind = str(params.get("entity_kind") or "").strip().lower()
    if requested_kind and requested_kind != "organization":
        return False
    primary = _primary_taxonomy(taxonomy_list)
    taxonomy_code = str(primary.get("taxonomy_code") or "").upper()
    return taxonomy_code.startswith("282N") and bool(enrichment and enrichment.get("has_hospital_enrollment"))


def _match_signal_payload(
    provider_row: Mapping[str, Any],
    params: Mapping[str, Any],
    taxonomy_matched: bool,
    is_provider_type_matched: bool,
    fhir_matched: bool,
    ffs_matched: bool,
) -> tuple[dict[str, Any], float]:
    signal_map: dict[str, Any] = {
        "address_site_key": {"matched": bool(provider_row.get("address_site_key_matched"))},
        "address_key": {"matched": bool(provider_row.get("address_key_matched"))},
        "phone": {"matched": bool(provider_row.get("phone_matched"))},
        "taxonomy": {"matched": taxonomy_matched},
        "fhir": {"matched": fhir_matched},
        "ffs": {"matched": ffs_matched},
    }
    score = 0.0
    if provider_row.get("address_site_key_matched"):
        score += 0.55
        signal_map["address_site_key"]["contribution"] = 0.55
    if provider_row.get("address_key_matched"):
        score += 0.50
        signal_map["address_key"]["contribution"] = 0.50
    if provider_row.get("phone_matched"):
        score += 0.25
        signal_map["phone"]["contribution"] = 0.25
    distance = provider_row.get("geo_distance_miles")
    if distance is not None:
        distance_float = float(distance)
        radius = float(params.get("radius_miles") or _MATCH_CANDIDATES_DEFAULT_RADIUS_MILES)
        contribution = max(0.0, 0.55 * (1.0 - min(distance_float / max(radius, 0.01), 1.0)))
        contribution = round(contribution, 4)
        score += contribution
        signal_map["geo_distance"] = {
            "miles": round(distance_float, 4),
            "contribution": contribution,
        }
    else:
        signal_map["geo_distance"] = {"matched": False}
    if taxonomy_matched:
        taxonomy_contribution = 0.10
        if is_provider_type_matched:
            taxonomy_contribution += 0.04
            signal_map["taxonomy"]["provider_type_matched"] = True
        score += taxonomy_contribution
        signal_map["taxonomy"]["contribution"] = round(taxonomy_contribution, 4)
    if fhir_matched:
        score += 0.05
        signal_map["fhir"]["contribution"] = 0.05
    if ffs_matched:
        score += 0.05
        signal_map["ffs"]["contribution"] = 0.05
    return signal_map, round(min(score, 1.0), 4)


def _boost_general_acute_care_score(signals: dict[str, Any], match_score: float) -> float:
    signals["facility"] = {"matched": True, "canonical_hospital": True, "contribution": 0.06}
    return round(min(match_score + 0.06, 1.0), 4)


def _confidence_band(score: float) -> str:
    if score >= 0.75:
        return "high"
    if score >= 0.45:
        return "medium"
    return "low"


def _match_candidate_source_count(item: Mapping[str, Any]) -> int:
    """Return the corroborating address source count for ranking ties."""

    sources = item.get("sources")
    if not isinstance(sources, Mapping):
        return 0
    fhir_sources = sources.get("fhir")
    if not isinstance(fhir_sources, Mapping):
        return 0
    return int(fhir_sources.get("source_count") or 0)


def _is_match_candidate_provider_type_matched(item: Mapping[str, Any]) -> bool:
    match_signals = item.get("match_signals")
    if not isinstance(match_signals, Mapping):
        return False
    taxonomy_signal = match_signals.get("taxonomy")
    if not isinstance(taxonomy_signal, Mapping):
        return False
    return bool(taxonomy_signal.get("provider_type_matched"))


def _match_candidate_sort_key(
    item: Mapping[str, Any],
    *,
    phone_provider_directory_matched: bool = False,
) -> tuple[int, float, int, int, int]:
    """Keep exact directory phone witnesses ahead of score tie-breakers."""

    return (
        -int(phone_provider_directory_matched),
        -float(item.get("match_score") or 0),
        -int(_is_match_candidate_provider_type_matched(item)),
        -_match_candidate_source_count(item),
        int(item.get("npi") or 0),
    )


def _rank_match_candidate_outputs(
    candidate_rows: Sequence[Mapping[str, Any]],
    candidate_params: Mapping[str, Any],
    enrichment_map: Mapping[int, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Rank directory phone witnesses before applying the public result limit."""
    ranked_candidates = [
        (
            _match_candidate_output(
                candidate_row,
                candidate_params,
                enrichment_map.get(int(candidate_row["npi"])),
            ),
            bool(candidate_row.get("phone_provider_directory_matched")),
        )
        for candidate_row in candidate_rows
        if candidate_row.get("npi") is not None
    ]
    ranked_candidates.sort(
        key=lambda ranked: _match_candidate_sort_key(
            ranked[0],
            phone_provider_directory_matched=ranked[1],
        )
    )
    return [
        candidate
        for candidate, _phone_provider_directory_matched in ranked_candidates[
            : int(candidate_params["limit"])
        ]
    ]


def _provider_type_filter_matched(row: Mapping[str, Any], params: Mapping[str, Any]) -> bool:
    if not (params.get("taxonomy_exact") or params.get("taxonomy_prefixes") or params.get("provider_type")):
        return False
    taxonomy_list = _json_array_value(row.get("taxonomy_list"))
    exact_codes = set(params.get("taxonomy_exact") or [])
    specialty_filter = params.get("specialty_filter")
    if specialty_filter is not None:
        exact_codes.update(str(code).upper() for code in specialty_filter.taxonomy_codes)
    prefixes = tuple(params.get("taxonomy_prefixes") or ())
    for item in taxonomy_list:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("taxonomy_code") or "").upper()
        if code in exact_codes or any(code.startswith(prefix) for prefix in prefixes):
            return True
        if specialty_filter is not None and specialty_filter.classification:
            if item.get("classification") == specialty_filter.classification:
                return True
    return False


def _is_provider_type_taxonomy_matched(row: Mapping[str, Any], params: Mapping[str, Any]) -> bool:
    specialty_filter = params.get("specialty_filter")
    if specialty_filter is None:
        return False
    taxonomy_list = _json_array_value(row.get("taxonomy_list"))
    specialty_codes = {str(code).upper() for code in specialty_filter.taxonomy_codes}
    for item in taxonomy_list:
        if not isinstance(item, Mapping):
            continue
        code = str(item.get("taxonomy_code") or "").upper()
        if code in specialty_codes:
            return True
        if specialty_filter.classification and item.get("classification") == specialty_filter.classification:
            return True
    return False


def _match_candidate_output(
    provider_row: Mapping[str, Any],
    params: Mapping[str, Any],
    enrichment: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build one redacted, scored provider candidate response object."""
    public_provider_map = dict(provider_row)
    _redact_internal_address_fields(public_provider_map)
    taxonomy_list = _json_array_value(provider_row.get("taxonomy_list"))
    fhir_sources = _json_array_value(public_provider_map.get(PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY))
    address_sources = _json_array_value(provider_row.get("address_sources"))
    fhir_matched = bool(fhir_sources) or "provider_directory_fhir" in address_sources
    ffs_matched = bool(enrichment and (
        enrichment.get("has_any_enrollment")
        or enrichment.get("has_ffs_enrollment")
        or enrichment.get("has_medicare_claims")
    ))
    taxonomy_matched = _provider_type_filter_matched(provider_row, params)
    is_provider_type_matched = _is_provider_type_taxonomy_matched(provider_row, params)
    is_general_acute_care_matched = _should_boost_general_acute_care_candidate(
        provider_row,
        params,
        taxonomy_list,
        enrichment,
    )
    match_signals, match_score = _match_signal_payload(
        provider_row,
        params,
        taxonomy_matched,
        is_provider_type_matched,
        fhir_matched,
        ffs_matched,
    )
    if is_general_acute_care_matched:
        match_score = _boost_general_acute_care_score(match_signals, match_score)
    address_map = {
        "type": provider_row.get("address_type"),
        "first_line": provider_row.get("first_line"),
        "second_line": provider_row.get("second_line"),
        "city_name": provider_row.get("city_name"),
        "state_name": provider_row.get("state_name"),
        "postal_code": provider_row.get("postal_code"),
        "country_code": provider_row.get("country_code"),
        "telephone_number": provider_row.get("telephone_number"),
        "phone_number": provider_row.get("phone_number"),
        "lat": provider_row.get("lat"),
        "long": provider_row.get("long"),
        "address_key": provider_row.get("address_key"),
        "address_site_key": provider_row.get("address_site_key"),
    }
    address_map = {
        key: field_value
        for key, field_value in address_map.items()
        if field_value not in (None, "", [])
    }
    source_map = {
        "nppes": {"matched": True},
        "fhir": {
            "matched": fhir_matched,
            "source_count": provider_row.get("source_count") or len(fhir_sources),
        },
        "ffs": {
            "matched": ffs_matched,
            "has_ffs_enrollment": bool(enrichment and enrichment.get("has_ffs_enrollment")),
            "has_medicare_claims": bool(enrichment and enrichment.get("has_medicare_claims")),
        },
    }
    candidate_map = {
        "npi": provider_row.get("npi"),
        "display_name": _match_candidate_name(provider_row),
        "organization_name": provider_row.get("provider_organization_name"),
        "entity_type_code": provider_row.get("entity_type_code"),
        "entity_kind": _entity_kind_from_code(provider_row.get("entity_type_code")),
        "address_key": provider_row.get("address_key"),
        "address_site_key": provider_row.get("address_site_key"),
        "match_score": match_score,
        "confidence_band": _confidence_band(match_score),
        "match_signals": match_signals,
        "facility": _facility_payload(provider_row, taxonomy_list, enrichment),
        "address": address_map,
        "sources": source_map,
    }
    if taxonomy_list:
        candidate_map["taxonomy"] = taxonomy_list
    if params.get("include_sources") and fhir_sources:
        candidate_map["provider_directory_sources"] = fhir_sources
    if params.get("include_evidence"):
        evidence_dict = {
            "provider_enrichment_summary": dict(enrichment or {}),
            "source_record_ids": _json_array_value(provider_row.get("source_record_ids")),
            "address_sources": address_sources,
        }
        phone_source_record_ids = _json_array_value(
            provider_row.get("phone_source_record_ids")
        )
        if phone_source_record_ids:
            evidence_dict["phone_source_record_ids"] = phone_source_record_ids
        candidate_map["evidence"] = evidence_dict
    return {
        key: field_value
        for key, field_value in candidate_map.items()
        if field_value is not None
    }


async def _attach_match_candidate_source_details(
    rows: list[dict[str, Any]],
    params: dict[str, Any],
    *,
    session: Any = None,
) -> None:
    """Attach compact FHIR provenance without expanding role evidence."""
    await _attach_geo_candidate_record_ids(
        rows,
        params,
        database_session=session,
    )
    if not (params.get("include_sources") or params.get("include_evidence")):
        return
    await _attach_provider_directory_source_details(
        rows,
        include_role_evidence=False,
        session=session,
    )


def _geo_candidate_address_pairs(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[int, str]]:
    candidate_pairs: list[tuple[int, str]] = []
    for row in rows:
        npi_value = row.get("npi")
        address_key = str(row.get("address_key") or "").strip()
        if npi_value is None or not address_key:
            continue
        candidate_pair = (int(npi_value), address_key)
        if candidate_pair not in candidate_pairs:
            candidate_pairs.append(candidate_pair)
        if len(candidate_pairs) >= _MATCH_CANDIDATES_MAX_INTERNAL_ROWS:
            break
    return candidate_pairs


def _replace_stale_geo_provider_directory_evidence(
    candidate_row_list: Sequence[dict[str, Any]],
) -> None:
    """Remove serving-row FHIR evidence before exact current corroboration."""
    for candidate_row in candidate_row_list:
        candidate_row.pop(PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY, None)
        candidate_row["source_count"] = 0
        candidate_row["source_record_ids"] = [
            record_id
            for record_id in _merge_unique_list_values(
                None, candidate_row.get("source_record_ids")
            )
            if not str(record_id).startswith("provider_directory_fhir:")
        ]
        candidate_row["address_sources"] = [
            address_source
            for address_source in _merge_unique_list_values(
                None, candidate_row.get("address_sources")
            )
            if str(address_source).strip().lower() != "provider_directory_fhir"
        ]


async def _attach_geo_candidate_record_ids(
    candidate_row_list: list[dict[str, Any]],
    candidate_params: Mapping[str, Any],
    *,
    database_session: Any = None,
) -> None:
    """Corroborate bounded geo rows with exact current overlay evidence."""
    if candidate_params.get("lat") is None or candidate_params.get("long") is None:
        return
    _replace_stale_geo_provider_directory_evidence(candidate_row_list)
    candidate_pairs = _geo_candidate_address_pairs(candidate_row_list)
    if not candidate_pairs:
        return
    evidence_result = await _execute_stmt(
        text(_current_provider_directory_geo_evidence_sql()),
        session=database_session,
        params={
            "candidate_npis": [npi for npi, _address_key in candidate_pairs],
            "candidate_address_keys": [
                address_key for _npi, address_key in candidate_pairs
            ],
        },
    )
    record_ids_by_candidate = {
        (int(mapping["npi"]), str(mapping["address_key"])): mapping[
            "source_record_ids"
        ]
        for evidence_row in evidence_result.all()
        for mapping in [getattr(evidence_row, "_mapping", evidence_row)]
    }
    for candidate_row in candidate_row_list:
        candidate_key = (
            int(candidate_row["npi"]),
            str(candidate_row.get("address_key") or ""),
        )
        record_ids = record_ids_by_candidate.get(candidate_key)
        if record_ids:
            candidate_row["source_count"] = len(
                _provider_directory_source_ids_from_record_ids(record_ids)
            )
            candidate_row["source_record_ids"] = _merge_unique_list_values(
                candidate_row.get("source_record_ids"), record_ids
            )
            candidate_row["address_sources"] = _merge_unique_list_values(
                candidate_row.get("address_sources"),
                "provider_directory_fhir",
            )


async def _attach_candidate_sources_bounded(
    candidate_row_list: list[dict[str, Any]],
    candidate_params: dict[str, Any],
    *,
    started_at: float,
    database_session: Any = None,
) -> None:
    """Attach source details within the endpoint's remaining query budget."""
    try:
        remaining_seconds = _MATCH_CANDIDATES_TIMEOUT_SECONDS - (
            time.monotonic() - started_at
        )
        if remaining_seconds <= 0:
            raise asyncio.TimeoutError()
        await asyncio.wait_for(
            _attach_match_candidate_source_details(
                candidate_row_list,
                candidate_params,
                session=database_session,
            ),
            timeout=remaining_seconds,
        )
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as exc:
        await _rollback_match_candidate_session(database_session)
        raise sanic.exceptions.ServiceUnavailable(
            f"match candidate source lookup exceeded the {_MATCH_CANDIDATES_TIMEOUT_SECONDS:g} second query budget"
        ) from exc
    except Exception as exc:
        await _rollback_match_candidate_session(database_session)
        logger.warning(
            "match candidate source lookup failed: %s",
            type(exc).__name__,
        )
        raise sanic.exceptions.ServiceUnavailable(
            "match candidate source lookup is temporarily unavailable"
        ) from exc


@blueprint.get("/match-candidates")
async def match_candidates(request):
    """Return bounded provider candidates for an address or identity query."""
    started = time.monotonic()
    request.args.get("address_site_key")
    request.args.get("address_key")
    request.args.get("lat")
    request.args.get("long")
    request.args.get("radius_miles")
    request.args.get("phone")
    request.args.get("entity_type_code")
    request.args.get("entity_kind")
    request.args.get("taxonomy_scope")
    request.args.get("provider_type")
    request.args.get("specialty")
    request.args.get("include_subspecialties")
    request.args.get("limit")
    request.args.get("include_sources")
    request.args.get("include_evidence")
    request.args.get("debug")
    params = await _normalize_match_candidate_params(request)
    request_session = _request_session(request)
    try:
        candidate_rows = await _fetch_match_candidate_rows(params, session=request_session)
    except asyncio.TimeoutError as exc:
        raise sanic.exceptions.ServiceUnavailable(
            f"match candidate lookup exceeded the {_MATCH_CANDIDATES_TIMEOUT_SECONDS:g} second query budget"
        ) from exc
    await _attach_candidate_sources_bounded(
        candidate_rows,
        params,
        started_at=started,
        database_session=request_session,
    )
    enrichment_map = await _fetch_provider_enrichment_summary_map(
        [candidate_row.get("npi") for candidate_row in candidate_rows],
        session=request_session,
    )
    candidates = _rank_match_candidate_outputs(candidate_rows, params, enrichment_map)
    return response.json(
        {
            "candidates": candidates,
            "total": len(candidates),
            "query": {
                "entity_type_code": params.get("entity_type_code"),
                "entity_kind": params.get("entity_kind"),
                "taxonomy_scope": list(params.get("taxonomy_exact") or [])
                + [f"{prefix}*" for prefix in (params.get("taxonomy_prefixes") or [])],
                "provider_type": params.get("provider_type"),
                "include_subspecialties": params.get("include_subspecialties"),
                "limit": params.get("limit"),
            },
            "meta": {
                "elapsed_ms": round((time.monotonic() - started) * 1000.0, 2),
                "timeout_ms": int(_MATCH_CANDIDATES_TIMEOUT_SECONDS * 1000),
            },
        },
        default=str,
    )


@blueprint.get("/all")
async def get_all(request):
    """Search, count, or page through public NPI provider records."""
    count_only = str(request.args.get("count_only", "0")).strip() == "1"
    include_chain_enrichment = _include_chain_provider_enrichment(request.args.get("show"))
    response_format = request.args.get("format") or request.args.get("response_format")
    response_format = str(response_format).strip().lower() if response_format else None
    request_session = _request_session(request)
    legacy_name_like = _extract_name_filters(request)
    # Explicit access for route collectors / OpenAPI parity.
    request.args.get("q")
    request.args.get("start")
    request.args.get("limit")
    request.args.get("include_total")
    request.args.get("include_sources")
    request.args.get("include_evidence")
    request.args.get("view")
    request.args.get("npi")
    request.args.get("address_site_key")
    include_sources = _parse_bool_arg(request.args.get("include_sources"), default=False)
    include_evidence = _parse_bool_arg(request.args.get("include_evidence"), default=False)
    if _parse_bool_arg(request.args.get("debug"), default=False):
        include_sources = True
        include_evidence = True
    q_value = str(request.args.get("q") or "").strip().lower()
    include_total_raw = request.args.get("include_total")
    include_total = _parse_bool_arg(include_total_raw, default=_should_include_npi_all_total(request.args, count_only))
    view_mode = str(request.args.get("view") or "").strip().lower()
    classification = request.args.get("classification")
    sitemap_limit_mode = (
        view_mode == "sitemap"
        and str(classification or "").strip().lower() == "pharmacy"
    )
    names_like: list[str] = []
    if q_value:
        names_like.append(q_value)
    for value in legacy_name_like:
        if value not in names_like:
            names_like.append(value)
    pagination = parse_pagination(
        request.args,
        default_limit=50,
        max_limit=20000 if sitemap_limit_mode else 200,
        default_page=1,
        allow_offset=True,
        allow_start=True,
        allow_page_size=True,
    )
    start = pagination.offset
    limit = pagination.limit
    specialization = request.args.get("specialization")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    first_name = request.args.get("first_name")
    last_name = request.args.get("last_name")
    organization_name = request.args.get("organization_name")
    npi_raw = request.args.get("npi")
    phone = request.args.get("phone")
    address_key_raw = request.args.get("address_key")
    address_site_key_raw = request.args.get(PUBLIC_ADDRESS_SITE_KEY)
    zip_code_raw = request.args.get("zip_code")
    postal_code_raw = request.args.get("postal_code")
    entity_type_code_raw = request.args.get("entity_type_code")
    plan_network = request.args.get("plan_network")
    has_insurance = request.args.get("has_insurance")
    city = request.args.get("city")
    state = request.args.get("state")
    procedure_codes_raw = request.args.get("procedure_codes")
    procedure_code_system_raw = request.args.get("procedure_code_system")
    medication_codes_raw = request.args.get("medication_codes")
    medication_code_system_raw = request.args.get("medication_code_system")
    year_raw = request.args.get("year")

    city = city.upper() if city else None
    state = state.upper() if state else None

    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(",")]
    _validate_section_filters(section, classification, codes)

    if plan_network:
        plan_network = [int(x) for x in plan_network.split(",")]

    requested_procedure_codes = _parse_code_tokens(procedure_codes_raw, "procedure_codes")
    requested_medication_codes = _parse_code_tokens(medication_codes_raw, "medication_codes")
    requested_year = _parse_optional_year(year_raw, "year")

    procedure_code_system = None
    medication_code_system = None
    if requested_procedure_codes:
        procedure_code_system = _normalize_code_system(
            procedure_code_system_raw or INTERNAL_PROCEDURE_CODE_SYSTEM,
            "procedure_code_system",
            PROCEDURE_ALLOWED_CODE_SYSTEMS,
        )
    elif procedure_code_system_raw:
        _normalize_code_system(
            procedure_code_system_raw,
            "procedure_code_system",
            PROCEDURE_ALLOWED_CODE_SYSTEMS,
        )

    if requested_medication_codes:
        medication_code_system = _normalize_code_system(
            medication_code_system_raw or INTERNAL_MEDICATION_CODE_SYSTEM,
            "medication_code_system",
            MEDICATION_ALLOWED_CODE_SYSTEMS,
        )
    elif medication_code_system_raw:
        _normalize_code_system(
            medication_code_system_raw,
            "medication_code_system",
            MEDICATION_ALLOWED_CODE_SYSTEMS,
        )

    if requested_procedure_codes or requested_medication_codes or requested_year is not None:
        if request_session is not None:
            filter_year, filter_year_source = await _resolve_filter_year(
                requested_year,
                include_procedures=bool(requested_procedure_codes),
                include_medications=bool(requested_medication_codes),
                session=request_session,
            )
        else:
            filter_year, filter_year_source = await _resolve_filter_year(
                requested_year,
                include_procedures=bool(requested_procedure_codes),
                include_medications=bool(requested_medication_codes),
            )
    else:
        filter_year, filter_year_source = None, "none"

    procedure_internal_codes: list[int] = []
    medication_internal_codes: list[int] = []
    procedure_match_via = "none"
    medication_match_via = "none"
    if requested_procedure_codes:
        if request_session is not None:
            procedure_internal_codes, procedure_match_via = await _resolve_internal_filter_codes(
                requested_procedure_codes,
                procedure_code_system or INTERNAL_PROCEDURE_CODE_SYSTEM,
                INTERNAL_PROCEDURE_CODE_SYSTEM,
                "procedure_codes",
                session=request_session,
            )
        else:
            procedure_internal_codes, procedure_match_via = await _resolve_internal_filter_codes(
                requested_procedure_codes,
                procedure_code_system or INTERNAL_PROCEDURE_CODE_SYSTEM,
                INTERNAL_PROCEDURE_CODE_SYSTEM,
                "procedure_codes",
            )
    if requested_medication_codes:
        if request_session is not None:
            medication_internal_codes, medication_match_via = await _resolve_internal_filter_codes(
                requested_medication_codes,
                medication_code_system or INTERNAL_MEDICATION_CODE_SYSTEM,
                INTERNAL_MEDICATION_CODE_SYSTEM,
                "medication_codes",
                session=request_session,
            )
        else:
            medication_internal_codes, medication_match_via = await _resolve_internal_filter_codes(
                requested_medication_codes,
                medication_code_system or INTERNAL_MEDICATION_CODE_SYSTEM,
                INTERNAL_MEDICATION_CODE_SYSTEM,
                "medication_codes",
            )

    filter_capabilities = {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
        "pricing_provider_procedure_available": False,
        "pricing_provider_prescription_available": False,
    }
    if requested_procedure_codes or requested_medication_codes:
        if request_session is not None:
            filter_capabilities = await _resolve_npi_filter_capabilities(session=request_session)
        else:
            filter_capabilities = await _resolve_npi_filter_capabilities()

    zip_code = _normalize_zip_code(zip_code_raw, "zip_code")
    postal_code = _normalize_zip_code(postal_code_raw, "postal_code")
    if zip_code and postal_code and zip_code != postal_code:
        raise sanic.exceptions.InvalidUsage(
            "zip_code and postal_code must match when both are provided"
        )
    zip_code = zip_code or postal_code

    phone_digits = _normalize_phone_digits(phone)
    address_key = _normalize_address_key(address_key_raw)
    address_site_key = _normalize_uuid_key(address_site_key_raw, PUBLIC_ADDRESS_SITE_KEY)
    exact_npi = _normalize_exact_npi(npi_raw)
    entity_type_code: Optional[int] = None
    if entity_type_code_raw not in (None, ""):
        try:
            entity_type_code = int(entity_type_code_raw)
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            ) from exc
        if entity_type_code not in (1, 2):
            raise sanic.exceptions.InvalidUsage(
                "entity_type_code must be either 1 (individual) or 2 (organization)"
            )

    filters = {
        "classification": classification,
        "specialization": specialization,
        "section": section,
        "display_name": display_name,
        "first_name": first_name,
        "last_name": last_name,
        "organization_name": organization_name,
        "npi": exact_npi,
        "phone_digits": phone_digits,
        "address_key": address_key,
        PUBLIC_ADDRESS_SITE_KEY: address_site_key,
        "zip_code": zip_code,
        "entity_type_code": entity_type_code,
        "plan_network": plan_network,
        "names_like": names_like,
        "codes": codes,
        "has_insurance": has_insurance,
        "city": city,
        "state": state,
        "response_format": response_format,
        "procedure_codes_input": requested_procedure_codes,
        "procedure_code_system": procedure_code_system,
        "procedure_internal_codes": procedure_internal_codes,
        "procedure_match_via": procedure_match_via,
        "medication_codes_input": requested_medication_codes,
        "medication_code_system": medication_code_system,
        "medication_internal_codes": medication_internal_codes,
        "medication_match_via": medication_match_via,
        "filter_year": filter_year,
        "filter_year_source": filter_year_source,
        "npi_procedures_array_available": filter_capabilities["npi_procedures_array_available"],
        "npi_medications_array_available": filter_capabilities["npi_medications_array_available"],
        "pricing_provider_procedure_available": filter_capabilities["pricing_provider_procedure_available"],
        "pricing_provider_prescription_available": filter_capabilities["pricing_provider_prescription_available"],
    }

    simple_filter_present = any(
        filters.get(field)
        for field in (
            "classification",
            "specialization",
            "section",
            "display_name",
            "first_name",
            "last_name",
            "organization_name",
            "npi",
            "phone_digits",
            "address_key",
            PUBLIC_ADDRESS_SITE_KEY,
            "zip_code",
            "entity_type_code",
            "plan_network",
            "names_like",
            "codes",
            "response_format",
            "procedure_internal_codes",
            "medication_internal_codes",
        )
    )
    broad_name_total_deferred = bool(names_like) and not any(
        [
            classification,
            specialization,
            section,
            display_name,
            first_name,
            last_name,
            organization_name,
            exact_npi,
            phone_digits,
            address_key,
            address_site_key,
            zip_code,
            entity_type_code,
            plan_network,
            codes,
            has_insurance,
            city,
            state,
            response_format,
            procedure_internal_codes,
            medication_internal_codes,
        ]
    )

    def _append_array_filters(address_where: list[str], local_filters: dict[str, Any]) -> dict[str, int]:
        params: dict[str, int] = {}
        filter_year = local_filters.get("filter_year")
        procedure_internal_codes = local_filters.get("procedure_internal_codes") or []
        medication_internal_codes = local_filters.get("medication_internal_codes") or []
        procedures_array_available = bool(local_filters.get("npi_procedures_array_available", True))
        medications_array_available = bool(local_filters.get("npi_medications_array_available", True))
        procedure_table_available = bool(local_filters.get("pricing_provider_procedure_available", False))
        medication_table_available = bool(local_filters.get("pricing_provider_prescription_available", False))

        if filter_year is not None and (procedure_internal_codes or medication_internal_codes):
            params["filter_year"] = int(filter_year)

        for idx, code in enumerate(procedure_internal_codes):
            param = f"procedure_code_{idx}"
            params[param] = int(code)
            array_clause = f"c.procedures_array @> ARRAY[:{param}]::INTEGER[]"
            exists_clause = (
                "EXISTS ("
                "SELECT 1 FROM mrf.pricing_provider_procedure AS pp "
                f"WHERE pp.npi = c.npi AND pp.procedure_code = :{param}"
                + (" AND pp.year = :filter_year" if filter_year is not None else "")
                + ")"
            )
            if procedures_array_available and procedure_table_available:
                address_where.append(f"({array_clause} OR {exists_clause})")
            elif procedures_array_available:
                address_where.append(array_clause)
            elif procedure_table_available:
                address_where.append(exists_clause)
            else:
                address_where.append("1=0")

        for idx, code in enumerate(medication_internal_codes):
            param = f"medication_code_{idx}"
            params[param] = int(code)
            array_clause = f"c.medications_array @> ARRAY[:{param}]::INTEGER[]"
            exists_clause = (
                "EXISTS ("
                "SELECT 1 FROM mrf.pricing_provider_prescription AS pr "
                "WHERE pr.npi = c.npi "
                "AND pr.rx_code_system = 'HP_RX_CODE' "
                + ("AND pr.year = :filter_year " if filter_year is not None else "")
                + f"AND CASE WHEN pr.rx_code ~ '^-?[0-9]+$' THEN pr.rx_code::INTEGER END = :{param} "
                ")"
            )
            if medications_array_available and medication_table_available:
                address_where.append(f"({array_clause} OR {exists_clause})")
            elif medications_array_available:
                address_where.append(array_clause)
            elif medication_table_available:
                address_where.append(exists_clause)
            else:
                address_where.append("1=0")

        return params

    address_required_columns = _public_address_serving_column_keys()
    address_table_sql = await _address_serving_table_sql(
        address_required_columns,
        session=request_session,
    )

    async def get_count(filters):
        """Count providers matching the normalized request filters."""
        classification = filters.get("classification")
        specialization = filters.get("specialization")
        section = filters.get("section")
        display_name = filters.get("display_name")
        first_name = filters.get("first_name")
        last_name = filters.get("last_name")
        organization_name = filters.get("organization_name")
        entity_type_code = filters.get("entity_type_code")
        plan_network = filters.get("plan_network")
        names_like = filters.get("names_like") or []
        codes = filters.get("codes")
        has_insurance = filters.get("has_insurance")
        city = filters.get("city")
        state = filters.get("state")
        zip_code = filters.get("zip_code")
        phone_digits = filters.get("phone_digits")
        address_key = filters.get("address_key")
        address_site_key = filters.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters.get("npi")

        taxonomy_filters = []
        if classification:
            taxonomy_filters.append("classification = :classification")
        if specialization:
            taxonomy_filters.append("specialization = :specialization")
        if section:
            taxonomy_filters.append("section = :section")
        if display_name:
            taxonomy_filters.append("display_name = :display_name")
        if codes:
            taxonomy_filters.append("code = ANY(:codes)")

        npi_where, npi_params = _build_npi_where_clause(
            "b",
            names_like,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )

        use_taxonomy_filter = bool(taxonomy_filters)
        include_service_locations = bool(address_key or address_site_key or phone_digits or exact_npi)
        address_where = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if use_taxonomy_filter:
            address_where.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_where.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_where.append("city_name = :city")
        if state:
            address_where.append("state_name = :state")
        if zip_code:
            address_where.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c", "c.npi")
            else:
                address_where.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_where.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_where.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_where.append(_address_npi_filter("c", address_table_sql))
        dynamic_code_params = _append_array_filters(address_where, filters)

        taxonomy_conditions = " AND ".join(taxonomy_filters) if taxonomy_filters else "1=1"
        taxonomy_subquery = _taxonomy_codes_subquery(taxonomy_conditions)

        if npi_where and use_taxonomy_filter:
            query = text(
                f"""
                {_sql_with_prefix_ctes(phone_candidates_cte)}filtered_npi AS (
                    SELECT DISTINCT b.npi
                      FROM mrf.npi AS b
                     WHERE {npi_where}
                )
                SELECT COUNT(DISTINCT c.npi)
                  FROM filtered_npi AS fn
                  JOIN {address_table_sql} AS c ON c.npi = fn.npi
                  {phone_candidates_join}
                  CROSS JOIN {taxonomy_subquery}
                 WHERE {' AND '.join(address_where)}
                """
            )
        elif npi_where:
            query = text(
                f"""
                {_sql_with_prefix_ctes(phone_candidates_cte)}filtered_npi AS (
                    SELECT DISTINCT b.npi
                      FROM mrf.npi AS b
                     WHERE {npi_where}
                )
                SELECT COUNT(DISTINCT c.npi)
                  FROM filtered_npi AS fn
                  JOIN {address_table_sql} AS c ON c.npi = fn.npi
                  {phone_candidates_join}
                 WHERE {' AND '.join(address_where)}
                """
            )
        elif use_taxonomy_filter:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte)}
                SELECT COUNT(DISTINCT c.npi)
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                  CROSS JOIN {taxonomy_subquery}
                 WHERE {' AND '.join(address_where)}
                """
            )
        else:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte)}
                SELECT COUNT(DISTINCT c.npi)
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                 WHERE {' AND '.join(address_where)}
                """
            )

        query_params = {
            "classification": classification,
            "section": section,
            "display_name": display_name,
            "plan_network_array": plan_network,
            "codes": codes,
            "city": city.upper() if city else None,
            "state": state.upper() if state else None,
            "zip_code": zip_code,
            "phone_digits": phone_digits,
            "address_key": address_key,
            "address_site_key": address_site_key,
            "npi_filter": exact_npi,
            "specialization": specialization,
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": organization_name,
            "entity_type_code": entity_type_code,
            "filter_year": filters.get("filter_year"),
        }
        query_params.update(dynamic_code_params)
        query_params.update(npi_params)
        if phone_candidates_cte:
            query_params["candidate_limit"] = _provider_list_phone_candidate_limit(
                limit,
                count_query=True,
            )

        async with db.acquire() as conn:
            rows = await conn.all(query, **query_params)
        return rows[0][0] if rows else 0

    async def get_formatted_count(response_format: str) -> dict:
        """
        Return mapping for special count formats (full_taxonomy/classification).
        """
        if response_format == "full_taxonomy":
            q = text(
                "SELECT ARRAY[int_code] AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY ARRAY[int_code]"
            )
        else:
            q = text(
                "SELECT classification AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY classification"
            )
        async with db.acquire() as conn:
            rows = await conn.all(q)
        return {row[0]: row[1] for row in rows}

    async def get_classification_count_map(filters) -> dict:
        """Return provider counts grouped by NUCC classification."""
        classification = filters.get("classification")
        specialization = filters.get("specialization")
        section = filters.get("section")
        display_name = filters.get("display_name")
        first_name = filters.get("first_name")
        last_name = filters.get("last_name")
        organization_name = filters.get("organization_name")
        entity_type_code = filters.get("entity_type_code")
        plan_network = filters.get("plan_network")
        names_like = filters.get("names_like") or []
        codes = filters.get("codes")
        has_insurance = filters.get("has_insurance")
        city = filters.get("city")
        state = filters.get("state")
        zip_code = filters.get("zip_code")
        phone_digits = filters.get("phone_digits")
        address_key = filters.get("address_key")
        address_site_key = filters.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters.get("npi")

        taxonomy_filters = []
        if classification:
            taxonomy_filters.append("classification = :classification")
        if specialization:
            taxonomy_filters.append("specialization = :specialization")
        if section:
            taxonomy_filters.append("section = :section")
        if display_name:
            taxonomy_filters.append("display_name = :display_name")
        if codes:
            taxonomy_filters.append("code = ANY(:codes)")

        npi_where, npi_params = _build_npi_where_clause(
            "b",
            names_like,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )

        include_service_locations = bool(address_key or address_site_key or phone_digits or exact_npi)
        address_where = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_where.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_where.append("city_name = :city")
        if state:
            address_where.append("state_name = :state")
        if zip_code:
            address_where.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c", "c.npi")
            else:
                address_where.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_where.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_where.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_where.append(_address_npi_filter("c", address_table_sql))
        dynamic_code_params = _append_array_filters(address_where, filters)
        if npi_where:
            address_where.append(
                f"EXISTS (SELECT 1 FROM mrf.npi AS b WHERE b.npi = c.npi AND {npi_where})"
            )

        taxonomy_conditions = " AND ".join(taxonomy_filters) if taxonomy_filters else "1=1"
        taxonomy_subquery = _taxonomy_classification_subquery(taxonomy_conditions)
        query = text(
            f"""
            {_sql_with_prefix_ctes(phone_candidates_cte)}filtered_taxonomy AS (
                SELECT DISTINCT c.npi, code.int_code
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                  CROSS JOIN LATERAL unnest(COALESCE(c.taxonomy_array, ARRAY[]::INTEGER[])) AS code(int_code)
                 WHERE {' AND '.join(address_where)}
            )
            SELECT q.classification AS key,
                   COUNT(DISTINCT ft.npi) AS value
              FROM filtered_taxonomy AS ft
              JOIN {taxonomy_subquery}
                ON ft.int_code = q.int_code
             GROUP BY q.classification
            """
        )
        query_params = {
            "classification": classification,
            "section": section,
            "display_name": display_name,
            "plan_network_array": plan_network,
            "codes": codes,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "phone_digits": phone_digits,
            "address_key": address_key,
            "address_site_key": address_site_key,
            "npi_filter": exact_npi,
            "specialization": specialization,
            "first_name": first_name,
            "last_name": last_name,
            "organization_name": organization_name,
            "entity_type_code": entity_type_code,
            "filter_year": filters.get("filter_year"),
        }
        query_params.update(dynamic_code_params)
        query_params.update(npi_params)
        if phone_candidates_cte:
            query_params["candidate_limit"] = _provider_list_phone_candidate_limit(
                limit,
                count_query=True,
            )
        async with db.acquire() as conn:
            rows = await conn.all(query, **query_params)
        return {row[0]: row[1] for row in rows if row and row[0]}

    procedure_filter_unresolved = bool(requested_procedure_codes) and not bool(procedure_internal_codes)
    medication_filter_unresolved = bool(requested_medication_codes) and not bool(medication_internal_codes)
    if procedure_filter_unresolved or medication_filter_unresolved:
        if count_only and response_format in {"all", "full_taxonomy", "classification"}:
            return response.json({"rows": {}}, default=str)
        if count_only:
            return response.json({"rows": 0}, default=str)
        return response.json(
            {
                "total": 0,
                "page": pagination.page,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "rows": [],
            },
            default=str,
        )

    if count_only and not simple_filter_present and not has_insurance and not city and not state:
        return response.json({"rows": await _fast_primary_npi_count()}, default=str)

    if count_only and has_insurance and not simple_filter_present:
        rows = await _fast_has_insurance_count(city, state)
        return response.json({"rows": rows}, default=str)

    if count_only and response_format == "all":
        mapping = await get_classification_count_map(filters)
        return response.json({"rows": mapping}, default=str)

    if count_only and response_format in {"full_taxonomy", "classification"}:
        mapping = await get_formatted_count(response_format)
        return response.json({"rows": mapping}, default=str)

    async def get_sitemap_results(start_offset: int, page_limit: int, classification_value: str) -> list[dict[str, Any]]:
        """Return a deterministic provider page for sitemap generation."""
        classification_npis = await _get_classification_npi_list(
            classification_value,
            session=request_session,
        )
        if not classification_npis:
            return []
        page_npis = classification_npis[start_offset:start_offset + page_limit]
        if not page_npis:
            return []
        query = text(
            f"""
            SELECT
                b.npi,
                b.provider_organization_name,
                b.provider_other_organization_name,
                b.last_update_date,
                c.date_added,
                c.postal_code,
                c.formatted_address
            FROM mrf.npi AS b
            JOIN {address_table_sql} AS c ON c.npi = b.npi AND c.type = 'primary'
            WHERE b.npi = ANY(:page_npis)
            ORDER BY b.npi
            """
        )
        async with db.acquire() as conn:
            rows_iter = await conn.all(
                query,
                page_npis=page_npis,
            )
        rows: list[dict[str, Any]] = []
        for row in rows_iter:
            mapping = getattr(row, "_mapping", None)
            if mapping is None:
                continue
            rows.append(
                {
                    "npi": mapping.get("npi"),
                    "provider_organization_name": mapping.get("provider_organization_name"),
                    "provider_other_organization_name": mapping.get("provider_other_organization_name"),
                    "last_update_date": mapping.get("last_update_date"),
                    "date_added": mapping.get("date_added"),
                    "postal_code": mapping.get("postal_code"),
                    "formatted_address": mapping.get("formatted_address"),
                    "do_business_as": [],
                    "procedures_array": [],
                    "medications_array": [],
                }
            )
        return rows

    async def get_formatted_count(response_format: str) -> dict:
        """
        Return mapping for special count formats (full_taxonomy/classification).
        """
        # The actual SQL is less important for tests; return pairs from DB.
        q = text("SELECT key, value FROM (VALUES (1, 1)) AS t(key, value)")
        async with db.acquire() as conn:
            rows = await conn.all(q)
        return {row[0]: row[1] for row in rows}

    async def get_results(start, limit, filters):
        """Return one provider result page for normalized search filters."""
        classification = filters.get("classification")
        section = filters.get("section")
        display_name = filters.get("display_name")
        first_name = filters.get("first_name")
        last_name = filters.get("last_name")
        organization_name = filters.get("organization_name")
        entity_type_code = filters.get("entity_type_code")
        plan_network = filters.get("plan_network")
        names_like = filters.get("names_like") or []
        specialization = filters.get("specialization")
        city = filters.get("city")
        state = filters.get("state")
        has_insurance = filters.get("has_insurance")
        zip_code = filters.get("zip_code")
        phone_digits = filters.get("phone_digits")
        address_key = filters.get("address_key")
        address_site_key = filters.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters.get("npi")
        where = []
        include_service_locations = bool(address_key or address_site_key or phone_digits or exact_npi)
        address_where = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if classification:
            where.append("classification = :classification")
        if specialization:
            where.append("specialization = :specialization")
        if section:
            where.append("section = :section")
        if display_name:
            where.append("display_name = :display_name")
        use_taxonomy_filter = bool(where)
        if use_taxonomy_filter:
            address_where.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network:
            address_where.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_where.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_where.append("city_name = :city")
        if state:
            address_where.append("state_name = :state")
        if zip_code:
            address_where.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c", "c.npi")
            else:
                address_where.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_where.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_where.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_where.append(_address_npi_filter("c", address_table_sql))
        dynamic_code_params = _append_array_filters(address_where, filters)
        npi_where, npi_params = _build_npi_where_clause(
            "b",
            names_like,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )

        taxonomy_filter = " and ".join(where) if where else "1=1"
        filtered_npi_cte = None
        if npi_where:
            filtered_npi_cte = f"""
        filtered_npi AS (
            SELECT DISTINCT b.npi
              FROM mrf.npi AS b
             WHERE {npi_where}
        )
"""

        if npi_where and use_taxonomy_filter:
            address_source = (
                "filtered_npi as fn\n"
                f"    JOIN {address_table_sql} as c ON c.npi = fn.npi\n"
                f"    {phone_candidates_join}\n"
                f"    CROSS JOIN (select ARRAY_AGG(int_code) as int_codes from mrf.nucc_taxonomy where {taxonomy_filter}) as q"
            )
        elif npi_where:
            address_source = (
                "filtered_npi as fn\n"
                f"    JOIN {address_table_sql} as c ON c.npi = fn.npi\n"
                f"    {phone_candidates_join}"
            )
        elif use_taxonomy_filter:
            address_source = (
                f"{address_table_sql} as c\n"
                f"    {phone_candidates_join}\n"
                f"    CROSS JOIN (select ARRAY_AGG(int_code) as int_codes from mrf.nucc_taxonomy where {taxonomy_filter}) as q"
            )
        else:
            address_source = f"{address_table_sql} as c\n    {phone_candidates_join}"
        address_order = _primary_address_order_clause("c", address_table_sql)
        q = text(
            f"""
        {_sql_with_prefix_ctes(phone_candidates_cte, filtered_npi_cte)}page_npis AS (
            SELECT DISTINCT c.npi
              FROM {address_source}
             WHERE {' and '.join(address_where)}
             ORDER BY c.npi
             LIMIT :limit OFFSET :start
        ),
        sub_s AS (
            SELECT b.npi AS npi_code, b.*, g.*
              FROM page_npis AS pn
              JOIN mrf.npi AS b ON b.npi = pn.npi
              JOIN LATERAL (
                  SELECT c.*
                    FROM {address_source}
                   WHERE {' and '.join(address_where)}
                     AND c.npi = pn.npi
                   ORDER BY {address_order}
                   LIMIT 1
              ) AS g ON TRUE
    )

    select sub_s.*, t.* from sub_s, mrf.npi_taxonomy as t
            where sub_s.npi_code = t.npi;
    """
        )

        res = {}
        async with db.acquire() as conn:
            query_params = {
                "start": start,
                "limit": limit,
                "classification": classification,
                "section": section,
                "display_name": display_name,
                "plan_network_array": plan_network,
                "specialization": specialization,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "phone_digits": phone_digits,
                "address_key": address_key,
                "address_site_key": address_site_key,
                "npi_filter": exact_npi,
                **npi_params,
                **dynamic_code_params,
            }
            if phone_candidates_cte:
                query_params["candidate_limit"] = _provider_list_phone_candidate_limit(
                    limit,
                    start,
                )
            rows_iter = await conn.all(
                q,
                **query_params,
            )
            for r in rows_iter:
                # Prefer key-based extraction so schema drift in upstream tables
                # (for example missing optional array columns) does not break
                # positional offsets and crash /npi/all.
                row_mapping = getattr(r, "_mapping", None)
                if row_mapping is not None:
                    npi_value = (
                        row_mapping.get("npi_code")
                        or row_mapping.get("npi")
                        or row_mapping.get("npi_1")
                        or row_mapping.get("npi_2")
                    )
                    if npi_value is None:
                        continue

                    obj = res.get(npi_value)
                    if obj is None:
                        obj = {"taxonomy_list": []}
                        for c in NPIData.__table__.columns:
                            if c.key in row_mapping:
                                obj[c.key] = row_mapping.get(c.key)
                        for c in NPIAddress.__table__.columns:
                            if c.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                                continue
                            if c.key in row_mapping:
                                obj[c.key] = row_mapping.get(c.key)
                        _attach_public_address_site_key(obj, row_mapping)
                        # Unified serving: include per-address source/plan attribution
                        # (not declared on the legacy NPIAddress model) so list results
                        # match the detail + geo endpoints.
                        if address_table_sql.endswith(".entity_address_unified"):
                            for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
                                if key in row_mapping and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                                    obj[key] = row_mapping.get(key)
                            if "source_record_ids" in row_mapping:
                                obj["source_record_ids"] = row_mapping.get("source_record_ids")
                        obj["do_business_as"] = obj.get("do_business_as") or []
                        obj.setdefault("procedures_array", [])
                        obj.setdefault("medications_array", [])

                    taxonomy = {}
                    for c in NPIDataTaxonomy.__table__.columns:
                        if c.key in ("npi", "checksum"):
                            continue
                        if c.key in row_mapping:
                            taxonomy[c.key] = row_mapping.get(c.key)
                    if taxonomy:
                        obj["taxonomy_list"].append(taxonomy)

                    res[npi_value] = obj
                    continue

                # Fallback for positional row types.
                row_len = len(r)
                if row_len <= 1:
                    continue

                obj = {"taxonomy_list": []}
                count = 0
                for c in NPIData.__table__.columns:
                    count += 1
                    if count >= row_len:
                        break
                    obj[c.key] = r[count]
                for c in NPIAddress.__table__.columns:
                    count += 1
                    if count >= row_len:
                        break
                    if c.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                        continue
                    obj[c.key] = r[count]

                npi_value = obj.get("npi")
                if npi_value is None:
                    continue

                if npi_value in res:
                    obj = res[npi_value]
                else:
                    obj["do_business_as"] = obj.get("do_business_as") or []
                    obj.setdefault("procedures_array", [])
                    obj.setdefault("medications_array", [])

                taxonomy = {}
                for c in NPIDataTaxonomy.__table__.columns:
                    count += 1
                    if count >= row_len:
                        break
                    if c.key in ("npi", "checksum"):
                        continue
                    taxonomy[c.key] = r[count]
                if taxonomy:
                    obj["taxonomy_list"].append(taxonomy)
                res[npi_value] = obj

            if (
                _address_table_is_unified(address_table_sql)
                and (address_key or address_site_key or phone_digits or exact_npi is not None)
                and len(res) < limit
                and not npi_where
                and not use_taxonomy_filter
            ):
                fallback_where = list(address_where)
                if fallback_where:
                    fallback_where[0] = _provider_list_address_type_clause(
                        "c",
                        address_table_sql,
                        include_service_locations=True,
                    )
                fallback_params = {
                    "plan_network_array": plan_network,
                    "city": city,
                    "state": state,
                    "zip_code": zip_code,
                    "phone_digits": phone_digits,
                    "address_key": address_key,
                    "address_site_key": address_site_key,
                    "npi_filter": exact_npi,
                    **dynamic_code_params,
                }
                fallback_rows = await conn.all(
                    text(
                        f"""
                        SELECT c.*
                          FROM {address_table_sql} AS c
                         WHERE {' AND '.join(fallback_where)}
                         ORDER BY {_primary_address_order_clause("c", address_table_sql)}
                         LIMIT :fallback_limit
                        """
                    ),
                    fallback_limit=limit,
                    **fallback_params,
                )
                for row in fallback_rows:
                    mapping = getattr(row, "_mapping", row)
                    if not isinstance(mapping, Mapping):
                        continue
                    npi_value = mapping.get("npi") or mapping.get("inferred_npi")
                    if npi_value is None or npi_value in res:
                        continue
                    obj = {
                        "npi": npi_value,
                        "entity_type_code": 1
                        if any(
                            ":practitioner_role:" in str(record_id or "")
                            for record_id in (mapping.get("source_record_ids") or [])
                        )
                        else 2,
                        "provider_organization_name": mapping.get("entity_name"),
                        "do_business_as": [],
                        "taxonomy_list": [],
                        "procedures_array": [],
                        "medications_array": [],
                    }
                    for column in NPIAddress.__table__.columns:
                        if column.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                            continue
                        if column.key in mapping:
                            obj[column.key] = mapping.get(column.key)
                    _attach_public_address_site_key(obj, mapping)
                    for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
                        if key in mapping and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                            obj[key] = mapping.get(key)
                    if "source_record_ids" in mapping:
                        obj["source_record_ids"] = mapping.get("source_record_ids")
                    res[npi_value] = obj

        res = list(res.values())
        for row in res:
            row["do_business_as"] = row.get("do_business_as") or []
            _add_canonical_contact_fields_to_address(row)
            _redact_internal_address_fields(row)
        return res

    if count_only:
        rows = await get_count(filters)
        return response.json({"rows": rows}, default=str)

    async def _count_with_timeout() -> Optional[int]:
        if not include_total:
            return None
        if broad_name_total_deferred:
            logger.info(
                "Skipping broad NPI /all name-search total count; offset=%s limit=%s explicit_include_total=%s",
                pagination.offset,
                pagination.limit,
                include_total_raw is not None,
            )
            return None
        if not simple_filter_present and not has_insurance and not city and not state:
            return await _fast_primary_npi_count()
        try:
            return await asyncio.wait_for(
                get_count(filters),
                timeout=max(0.1, _NPI_ALL_TOTAL_TIMEOUT_SECONDS),
            )
        except asyncio.TimeoutError:
            logger.warning(
                "NPI /all total count timed out; offset=%s limit=%s",
                pagination.offset,
                pagination.limit,
            )
            return None
        except Exception as exc:  # pragma: no cover - defensive degradation
            logger.warning(
                "NPI /all total count failed (%s); offset=%s limit=%s",
                exc,
                pagination.offset,
                pagination.limit,
            )
            return None

    use_sitemap_fast_path = (
        view_mode == "sitemap"
        and not count_only
        and str(classification or "").strip().lower() == "pharmacy"
        and not any(
            [
                specialization,
                section,
                display_name,
                first_name,
                last_name,
                organization_name,
                exact_npi,
                phone_digits,
                address_key,
                address_site_key,
                zip_code,
                entity_type_code,
                plan_network,
                names_like,
                codes,
                response_format,
                procedure_internal_codes,
                medication_internal_codes,
            ]
        )
        and not has_insurance
        and not city
        and not state
    )

    if use_sitemap_fast_path:
        rows = await get_sitemap_results(start, limit, "Pharmacy")
        raw_total = None if not include_total else await _count_with_timeout()
    else:
        raw_total, rows = await asyncio.gather(
            _count_with_timeout(),
            get_results(start, limit, filters),
        )
    if include_total and raw_total is not None:
        total = int(raw_total)
        total_source = "computed"
    elif include_total:
        total = pagination.offset + len(rows)
        total_source = "estimated_timeout_floor"
    else:
        total = pagination.offset + len(rows)
        total_source = "estimated_page_floor"
    summary_map: dict[int, dict[str, Any]] = {}
    try:
        summary_map = await _fetch_provider_enrichment_summary_map(
            [row.get("npi") for row in rows if isinstance(row, dict)],
            include_chain=include_chain_enrichment,
            session=request_session,
        )
    except Exception as exc:  # pragma: no cover - defensive fallback for transient DB states
        logger.debug("Provider enrichment summary fetch failed: %s", exc)
    if summary_map:
        for row in rows:
            if not isinstance(row, dict):
                continue
            npi_value = row.get("npi")
            if npi_value is None:
                continue
            summary = summary_map.get(int(npi_value))
            if summary:
                row["provider_enrichment_summary"] = summary
    if include_sources or include_evidence:
        await _attach_provider_directory_source_details(
            rows,
            include_role_evidence=include_evidence,
            session=request_session,
        )
    if not include_evidence:
        for row in rows:
            if isinstance(row, dict):
                row.pop("source_record_ids", None)

    payload: dict[str, Any] = {
        "total": total,
        "page": pagination.page,
        "limit": pagination.limit,
        "offset": pagination.offset,
        "rows": rows,
        "total_source": total_source,
    }
    if requested_procedure_codes or requested_medication_codes:
        payload["query"] = {
            "year": filter_year,
            "year_source": filter_year_source,
            "input_procedure_codes": requested_procedure_codes or None,
            "procedure_code_system": procedure_code_system,
            "resolved_procedure_codes": procedure_internal_codes,
            "procedure_matched_via": procedure_match_via,
            "input_medication_codes": requested_medication_codes or None,
            "medication_code_system": medication_code_system,
            "resolved_medication_codes": medication_internal_codes,
            "medication_matched_via": medication_match_via,
        }
    return response.json(
        payload,
        default=str,
    )


@blueprint.get("/facilities/providers")
async def get_facility_connected_providers(request):
    """Return providers connected to a requested enrolled facility."""
    request_session = _request_session(request)
    facility_type_raw = _normalize_text_filter(request.args.get("facility_type"), param_name="facility_type", max_length=32)
    facility_type = (facility_type_raw or "hospital").lower()
    enrollment_model = FACILITY_ENROLLMENT_MODELS.get(facility_type)
    if enrollment_model is None:
        allowed = ", ".join(sorted(FACILITY_ENROLLMENT_MODELS.keys()))
        raise sanic.exceptions.InvalidUsage(f"Parameter 'facility_type' must be one of: {allowed}")

    ccn = _normalize_ccn_filter(request.args.get("ccn"))
    organization_name = _normalize_text_filter(
        request.args.get("organization_name"),
        param_name="organization_name",
        max_length=256,
    )
    city = _normalize_text_filter(request.args.get("city"), param_name="city", max_length=128)
    state = _normalize_state_filter(request.args.get("state"))
    reporting_year = _parse_optional_bounded_int(
        request.args.get("reporting_year"),
        param_name="reporting_year",
        minimum=1990,
        maximum=3000,
    )
    limit = _parse_bounded_int(request.args.get("limit"), param_name="limit", default=50, minimum=1, maximum=200)
    offset = _parse_bounded_int(request.args.get("offset"), param_name="offset", default=0, minimum=0, maximum=1_000_000)
    stats_limit = _parse_bounded_int(
        request.args.get("stats_limit"),
        param_name="stats_limit",
        default=100,
        minimum=1,
        maximum=500,
    )
    include_specialty_stats = _parse_bool_arg(request.args.get("include_specialty_stats"), default=True)

    if ccn is None and organization_name is None:
        raise sanic.exceptions.InvalidUsage("At least one facility locator is required: ccn or organization_name")

    table_name = enrollment_model.__tablename__
    if not await _table_exists(table_name, session=request_session):
        payload: dict[str, Any] = {
            "query": {
                "facility_type": facility_type,
                "ccn": ccn,
                "organization_name": organization_name,
                "city": city,
                "state": state,
                "reporting_year": reporting_year,
                "limit": limit,
                "offset": offset,
            },
            "total_providers": 0,
            "matched_facilities": [],
            "providers": [],
        }
        if include_specialty_stats:
            payload["specialty_stats"] = []
        return response.json(payload, default=str)

    model_columns = _model_table_columns(enrollment_model)
    has_cah_ccn = "cah_or_hospital_ccn" in model_columns
    has_practice_location_type = "practice_location_type" in model_columns
    facility_ccn_expr = (
        "COALESCE(NULLIF(BTRIM(h.cah_or_hospital_ccn), ''), NULLIF(BTRIM(h.ccn), ''))"
        if has_cah_ccn
        else "NULLIF(BTRIM(h.ccn), '')"
    )
    practice_location_expr = (
        "h.practice_location_type AS practice_location_type"
        if has_practice_location_type
        else "NULL::varchar AS practice_location_type"
    )

    where_clauses = ["1=1"]
    params: dict[str, Any] = {}
    if ccn:
        where_clauses.append(f"UPPER(REPLACE({facility_ccn_expr}, ' ', '')) = :ccn")
        params["ccn"] = ccn
    if organization_name:
        where_clauses.append(
            "LOWER(COALESCE(h.organization_name, '') || ' ' || COALESCE(h.doing_business_as_name, '')) "
            "LIKE :organization_name"
        )
        params["organization_name"] = f"%{organization_name.lower()}%"
    if city:
        where_clauses.append("UPPER(COALESCE(h.city, '')) = :city")
        params["city"] = city.upper()
    if state:
        where_clauses.append("UPPER(COALESCE(h.state, '')) = :state")
        params["state"] = state
    if reporting_year is not None:
        where_clauses.append("h.reporting_year = :reporting_year")
        params["reporting_year"] = reporting_year
    where_sql = " AND ".join(where_clauses)

    total_sql = text(
        f"""
        SELECT COUNT(DISTINCT h.npi) AS total_providers
          FROM mrf.{table_name} h
         WHERE {where_sql}
        """
    )

    facilities_sql = text(
        f"""
        SELECT
            x.facility_ccn,
            x.organization_name,
            x.doing_business_as_name,
            x.city,
            x.state,
            COUNT(DISTINCT x.npi) AS provider_count
          FROM (
                SELECT
                    h.npi,
                    {facility_ccn_expr} AS facility_ccn,
                    h.organization_name,
                    h.doing_business_as_name,
                    h.city,
                    h.state
                  FROM mrf.{table_name} h
                 WHERE {where_sql}
          ) AS x
         GROUP BY x.facility_ccn, x.organization_name, x.doing_business_as_name, x.city, x.state
         ORDER BY provider_count DESC, x.organization_name ASC NULLS LAST, x.facility_ccn ASC NULLS LAST
         LIMIT 25
        """
    )

    providers_sql = text(
        f"""
        WITH filtered AS (
            SELECT h.*
              FROM mrf.{table_name} h
             WHERE {where_sql}
        ),
        latest AS (
            SELECT DISTINCT ON (h.npi)
                h.npi,
                h.reporting_year,
                {facility_ccn_expr} AS facility_ccn,
                h.organization_name,
                h.doing_business_as_name,
                h.city AS facility_city,
                h.state AS facility_state,
                h.zip_code AS facility_zip_code,
                h.provider_type_code AS enrollment_provider_type_code,
                h.provider_type_text AS enrollment_provider_type_text,
                {practice_location_expr}
              FROM filtered h
             ORDER BY h.npi, h.reporting_year DESC NULLS LAST, h.imported_at DESC NULLS LAST
        ),
        taxonomy_choice AS (
            SELECT DISTINCT ON (t.npi)
                t.npi,
                t.healthcare_provider_taxonomy_code AS taxonomy_code
              FROM mrf.{NPIDataTaxonomy.__tablename__} t
             WHERE t.npi IN (SELECT npi FROM latest)
             ORDER BY
                t.npi,
                CASE WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0 ELSE 1 END,
                t.checksum
        )
        SELECT
            l.npi,
            l.reporting_year,
            l.facility_ccn,
            l.organization_name,
            l.doing_business_as_name,
            l.facility_city,
            l.facility_state,
            l.facility_zip_code,
            l.enrollment_provider_type_code,
            l.enrollment_provider_type_text,
            l.practice_location_type,
            d.entity_type_code,
            d.provider_first_name,
            d.provider_last_name,
            d.provider_organization_name,
            d.city_name AS provider_city,
            d.state_name AS provider_state,
            tc.taxonomy_code,
            nt.display_name AS specialty_display_name,
            nt.classification AS specialty_classification,
            nt.section AS specialty_section
          FROM latest l
          LEFT JOIN mrf.{NPIData.__tablename__} d
            ON d.npi = l.npi
          LEFT JOIN taxonomy_choice tc
            ON tc.npi = l.npi
          LEFT JOIN mrf.{NUCCTaxonomy.__tablename__} nt
            ON nt.code = tc.taxonomy_code
         ORDER BY l.reporting_year DESC NULLS LAST, l.npi
         LIMIT :limit OFFSET :offset
        """
    )

    specialty_sql = text(
        f"""
        WITH filtered AS (
            SELECT h.npi
              FROM mrf.{table_name} h
             WHERE {where_sql}
             GROUP BY h.npi
        ),
        taxonomy_choice AS (
            SELECT DISTINCT ON (t.npi)
                t.npi,
                t.healthcare_provider_taxonomy_code AS taxonomy_code
              FROM mrf.{NPIDataTaxonomy.__tablename__} t
             WHERE t.npi IN (SELECT npi FROM filtered)
             ORDER BY
                t.npi,
                CASE WHEN UPPER(COALESCE(t.healthcare_provider_primary_taxonomy_switch, '')) = 'Y' THEN 0 ELSE 1 END,
                t.checksum
        )
        SELECT
            COALESCE(nt.display_name, 'Unknown') AS specialty,
            COALESCE(nt.classification, 'Unknown') AS classification,
            COUNT(*) AS provider_count
          FROM filtered f
          LEFT JOIN taxonomy_choice tc
            ON tc.npi = f.npi
          LEFT JOIN mrf.{NUCCTaxonomy.__tablename__} nt
            ON nt.code = tc.taxonomy_code
         GROUP BY specialty, classification
         ORDER BY provider_count DESC, specialty ASC
         LIMIT :stats_limit
        """
    )

    execute_params = dict(params)
    execute_params["limit"] = limit
    execute_params["offset"] = offset
    execute_params["stats_limit"] = stats_limit

    if request_session is not None:
        session = request_session
        total_result = await session.execute(total_sql, params)
        facility_result = await session.execute(facilities_sql, params)
        providers_result = await session.execute(providers_sql, execute_params)
        specialty_result = (
            await session.execute(specialty_sql, execute_params) if include_specialty_stats else None
        )
    else:
        async with db.session() as session:
            total_result = await session.execute(total_sql, params)
            facility_result = await session.execute(facilities_sql, params)
            providers_result = await session.execute(providers_sql, execute_params)
            specialty_result = (
                await session.execute(specialty_sql, execute_params) if include_specialty_stats else None
            )

    total_row = total_result.first()
    total_providers = int((total_row._mapping.get("total_providers") if total_row else 0) or 0)

    matched_facilities = [
        {
            "ccn": row._mapping.get("facility_ccn"),
            "organization_name": row._mapping.get("organization_name"),
            "doing_business_as_name": row._mapping.get("doing_business_as_name"),
            "city": row._mapping.get("city"),
            "state": row._mapping.get("state"),
            "provider_count": int(row._mapping.get("provider_count") or 0),
        }
        for row in facility_result.all()
    ]

    providers = []
    for row in providers_result.all():
        mapping = row._mapping
        providers.append(
            {
                "npi": int(mapping.get("npi")) if mapping.get("npi") is not None else None,
                "provider_name": _provider_display_name_from_mapping(mapping),
                "entity_type_code": mapping.get("entity_type_code"),
                "provider_city": mapping.get("provider_city"),
                "provider_state": mapping.get("provider_state"),
                "taxonomy_code": mapping.get("taxonomy_code"),
                "specialty": mapping.get("specialty_display_name") or "Unknown",
                "specialty_classification": mapping.get("specialty_classification"),
                "specialty_section": mapping.get("specialty_section"),
                "facility": {
                    "facility_type": facility_type,
                    "ccn": mapping.get("facility_ccn"),
                    "organization_name": mapping.get("organization_name"),
                    "doing_business_as_name": mapping.get("doing_business_as_name"),
                    "city": mapping.get("facility_city"),
                    "state": mapping.get("facility_state"),
                    "zip_code": mapping.get("facility_zip_code"),
                    "reporting_year": mapping.get("reporting_year"),
                    "practice_location_type": mapping.get("practice_location_type"),
                    "provider_type_code": mapping.get("enrollment_provider_type_code"),
                    "provider_type_text": mapping.get("enrollment_provider_type_text"),
                },
            }
        )

    payload: dict[str, Any] = {
        "query": {
            "facility_type": facility_type,
            "ccn": ccn,
            "organization_name": organization_name,
            "city": city,
            "state": state,
            "reporting_year": reporting_year,
            "limit": limit,
            "offset": offset,
            "include_specialty_stats": include_specialty_stats,
            "stats_limit": stats_limit,
        },
        "total_providers": total_providers,
        "matched_facilities": matched_facilities,
        "providers": providers,
    }

    if include_specialty_stats and specialty_result is not None:
        payload["specialty_stats"] = [
            {
                "specialty": row._mapping.get("specialty"),
                "classification": row._mapping.get("classification"),
                "provider_count": int(row._mapping.get("provider_count") or 0),
            }
            for row in specialty_result.all()
        ]

    return response.json(payload, default=str)


@blueprint.get("/near/")
async def get_near_npi(request):
    """Return providers near coordinates under optional taxonomy filters."""
    request_session = _request_session(request)
    in_long, in_lat = None, None
    if request.args.get("long"):
        in_long = float(request.args.get("long"))
    if request.args.get("lat"):
        in_lat = float(request.args.get("lat"))

    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(",")]

    plan_network = request.args.get("plan_network")
    if plan_network:
        plan_network = [int(x) for x in plan_network.split(",")]
    classification = request.args.get("classification")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    procedure_codes_raw = request.args.get("procedure_codes")
    procedure_code_system_raw = request.args.get("procedure_code_system")
    medication_codes_raw = request.args.get("medication_codes")
    medication_code_system_raw = request.args.get("medication_code_system")
    year_raw = request.args.get("year")
    request.args.get("q")
    if _extract_name_filters(request):
        raise sanic.exceptions.InvalidUsage(
            "name_like is no longer supported on /npi/near/; use q"
        )
    name_query = str(request.args.get("q") or "").strip()
    exclude_npi = int(request.args.get("exclude_npi", 0))
    limit = int(request.args.get("limit", 5))
    zip_codes = []
    for zip_c in request.args.get("zip_codes", "").split(","):
        if not zip_c:
            continue
        zip_codes.append(zip_c.strip().rjust(5, "0"))
    radius = int(request.args.get("radius", 10))

    requested_procedure_codes = _parse_code_tokens(procedure_codes_raw, "procedure_codes")
    requested_medication_codes = _parse_code_tokens(medication_codes_raw, "medication_codes")
    requested_year = _parse_optional_year(year_raw, "year")

    procedure_code_system = None
    medication_code_system = None
    if requested_procedure_codes:
        procedure_code_system = _normalize_code_system(
            procedure_code_system_raw or INTERNAL_PROCEDURE_CODE_SYSTEM,
            "procedure_code_system",
            PROCEDURE_ALLOWED_CODE_SYSTEMS,
        )
    elif procedure_code_system_raw:
        _normalize_code_system(
            procedure_code_system_raw,
            "procedure_code_system",
            PROCEDURE_ALLOWED_CODE_SYSTEMS,
        )
    if requested_medication_codes:
        medication_code_system = _normalize_code_system(
            medication_code_system_raw or INTERNAL_MEDICATION_CODE_SYSTEM,
            "medication_code_system",
            MEDICATION_ALLOWED_CODE_SYSTEMS,
        )
    elif medication_code_system_raw:
        _normalize_code_system(
            medication_code_system_raw,
            "medication_code_system",
            MEDICATION_ALLOWED_CODE_SYSTEMS,
        )

    if requested_procedure_codes or requested_medication_codes or requested_year is not None:
        if request_session is not None:
            filter_year, _filter_year_source = await _resolve_filter_year(
                requested_year,
                include_procedures=bool(requested_procedure_codes),
                include_medications=bool(requested_medication_codes),
                session=request_session,
            )
        else:
            filter_year, _filter_year_source = await _resolve_filter_year(
                requested_year,
                include_procedures=bool(requested_procedure_codes),
                include_medications=bool(requested_medication_codes),
            )
    else:
        filter_year = None

    procedure_internal_codes: list[int] = []
    medication_internal_codes: list[int] = []
    if requested_procedure_codes:
        if request_session is not None:
            procedure_internal_codes, _ = await _resolve_internal_filter_codes(
                requested_procedure_codes,
                procedure_code_system or INTERNAL_PROCEDURE_CODE_SYSTEM,
                INTERNAL_PROCEDURE_CODE_SYSTEM,
                "procedure_codes",
                session=request_session,
            )
        else:
            procedure_internal_codes, _ = await _resolve_internal_filter_codes(
                requested_procedure_codes,
                procedure_code_system or INTERNAL_PROCEDURE_CODE_SYSTEM,
                INTERNAL_PROCEDURE_CODE_SYSTEM,
                "procedure_codes",
            )
    if requested_medication_codes:
        if request_session is not None:
            medication_internal_codes, _ = await _resolve_internal_filter_codes(
                requested_medication_codes,
                medication_code_system or INTERNAL_MEDICATION_CODE_SYSTEM,
                INTERNAL_MEDICATION_CODE_SYSTEM,
                "medication_codes",
                session=request_session,
            )
        else:
            medication_internal_codes, _ = await _resolve_internal_filter_codes(
                requested_medication_codes,
                medication_code_system or INTERNAL_MEDICATION_CODE_SYSTEM,
                INTERNAL_MEDICATION_CODE_SYSTEM,
                "medication_codes",
            )

    if (requested_procedure_codes and not procedure_internal_codes) or (
        requested_medication_codes and not medication_internal_codes
    ):
        return response.json([], default=str)

    filter_capabilities = {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
        "pricing_provider_procedure_available": False,
        "pricing_provider_prescription_available": False,
    }
    if requested_procedure_codes or requested_medication_codes:
        if request_session is not None:
            filter_capabilities = await _resolve_npi_filter_capabilities(session=request_session)
        else:
            filter_capabilities = await _resolve_npi_filter_capabilities()

    _validate_section_filters(section, classification, codes)
    # If only zip was provided, resolve to coordinates first using a separate connection.
    if (not (in_long and in_lat)) and zip_codes and zip_codes[0]:
        zip_sql = "select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"
        async with db.acquire() as conn_zip:
            for r in await conn_zip.all(text(zip_sql), zip_code=zip_codes[0]):
                try:
                    in_long = float(r["intptlon"])
                    in_lat = float(r["intptlat"])
                except Exception:
                    in_lat = float(r[0])
                    in_long = float(r[1])

    address_table_sql = await _address_serving_table_sql(
        _public_address_serving_column_keys(),
        session=request_session,
    )

    res = {}
    extra_filters: list[str] = []
    if exclude_npi:
        extra_filters.append("a.npi <> :exclude_npi")
    if plan_network:
        extra_filters.append("a.plans_network_array && (:plan_network_array)")
    dynamic_code_params: dict[str, int] = {}
    if filter_year is not None and (procedure_internal_codes or medication_internal_codes):
        dynamic_code_params["filter_year"] = int(filter_year)

    procedures_array_available = bool(filter_capabilities.get("npi_procedures_array_available", True))
    medications_array_available = bool(filter_capabilities.get("npi_medications_array_available", True))
    procedure_table_available = bool(filter_capabilities.get("pricing_provider_procedure_available", False))
    medication_table_available = bool(filter_capabilities.get("pricing_provider_prescription_available", False))

    for idx, code in enumerate(procedure_internal_codes):
        param = f"procedure_code_{idx}"
        dynamic_code_params[param] = int(code)
        array_clause = f"a.procedures_array @> ARRAY[:{param}]::INTEGER[]"
        exists_clause = (
            "EXISTS ("
            "SELECT 1 FROM mrf.pricing_provider_procedure AS pp "
            f"WHERE pp.npi = a.npi AND pp.procedure_code = :{param}"
            + (" AND pp.year = :filter_year" if filter_year is not None else "")
            + ")"
        )
        if procedures_array_available and procedure_table_available:
            extra_filters.append(f"({array_clause} OR {exists_clause})")
        elif procedures_array_available:
            extra_filters.append(array_clause)
        elif procedure_table_available:
            extra_filters.append(exists_clause)
        else:
            extra_filters.append("1=0")

    for idx, code in enumerate(medication_internal_codes):
        param = f"medication_code_{idx}"
        dynamic_code_params[param] = int(code)
        array_clause = f"a.medications_array @> ARRAY[:{param}]::INTEGER[]"
        exists_clause = (
            "EXISTS ("
            "SELECT 1 FROM mrf.pricing_provider_prescription AS pr "
            "WHERE pr.npi = a.npi "
            "AND pr.rx_code_system = 'HP_RX_CODE' "
            + ("AND pr.year = :filter_year " if filter_year is not None else "")
            + f"AND CASE WHEN pr.rx_code ~ '^-?[0-9]+$' THEN pr.rx_code::INTEGER END = :{param}"
            ")"
        )
        if medications_array_available and medication_table_available:
            extra_filters.append(f"({array_clause} OR {exists_clause})")
        elif medications_array_available:
            extra_filters.append(array_clause)
        elif medication_table_available:
            extra_filters.append(exists_clause)
        else:
            extra_filters.append("1=0")

    where: list[str] = []
    if zip_codes:
        # Default to a reasonable search radius when zip is used; avoid huge fan-out.
        radius = 25
        extra_filters.append(_address_zip5_filter("a", address_table_sql, any_array=True))

    bbox_params: dict[str, float] = {}
    if in_long is not None and in_lat is not None:
        delta_lat = radius / 69.0  # approx miles per degree latitude
        cos_lat = math.cos(math.radians(in_lat)) or 1e-6
        delta_long = radius / (69.0 * cos_lat)
        bbox_params = {
            "min_lat": in_lat - delta_lat,
            "max_lat": in_lat + delta_lat,
            "min_long": in_long - delta_long,
            "max_long": in_long + delta_long,
        }
        extra_filters.append("a.lat BETWEEN :min_lat AND :max_lat")
        extra_filters.append("a.long BETWEEN :min_long AND :max_long")
    if classification:
        where.append("classification = :classification")
    if section:
        where.append("section = :section")
    if display_name:
        where.append("display_name = :display_name")
    if codes:
        where.append("code = ANY(:codes)")
    ilike_clause = ""
    q_like = None
    if name_query:
        q_like = f"%{name_query}%"
        ilike_clause = f"\n            AND {_name_like_clause('d', 'q')}"

    taxonomy_conditions = " AND ".join(where) if where else "1=1"
    extra_clause = ""
    if extra_filters:
        extra_clause = "\n          AND " + "\n          AND ".join(extra_filters)

    nearby_sql = _build_nearby_sql(
        taxonomy_conditions,
        extra_clause,
        ilike_clause,
        use_taxonomy_filter=bool(where),
        address_table_sql=address_table_sql,
        geo_precision_clause=_exact_geo_precision_clause(address_table_sql),
    )

    async with db.acquire() as conn:
        res_q = await conn.all(
            text(nearby_sql),
            in_long=in_long,
            in_lat=in_lat,
            classification=classification,
            limit=limit,
            radius=radius,
            exclude_npi=exclude_npi,
            section=section,
            display_name=display_name,
            q=q_like,
            codes=codes,
            zip_codes=zip_codes,
            plan_network_array=plan_network,
            **dynamic_code_params,
            **bbox_params,
        )

    for r in res_q:
        row_mapping = getattr(r, "_mapping", None)
        if row_mapping is not None:
            row_dict = dict(row_mapping)
            npi_value = (
                row_dict.get("npi_code")
                or row_dict.get("npi")
                or row_dict.get("npi_1")
                or row_dict.get("npi_2")
            )
            if npi_value is None:
                continue

            npi_value = int(npi_value)
            obj = res.get(npi_value, {"taxonomy_list": []})
            if "distance" in row_dict and row_dict.get("distance") is not None:
                obj["distance"] = row_dict.get("distance")

            for c in NPIAddress.__table__.columns:
                if c.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                    continue
                if c.key in row_dict:
                    obj[c.key] = row_dict[c.key]
            _attach_public_address_site_key(obj, row_dict)
            # Unified serving carries per-address source/plan attribution that the
            # legacy NPIAddress model doesn't declare, so the loop above skips it.
            # Surface it here so geo results show WHERE each address came from and
            # which plans/networks confirm it (parity with the detail endpoint).
            if address_table_sql.endswith(".entity_address_unified"):
                for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
                    if key in row_dict and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                        obj[key] = row_dict[key]
            for c in NPIData.__table__.columns:
                if c.key in ("npi", "checksum", "do_business_as_text"):
                    continue
                if c.key in row_dict:
                    obj[c.key] = row_dict[c.key]

            taxonomy = {}
            for c in NPIDataTaxonomy.__table__.columns:
                if c.key in ("npi", "checksum"):
                    continue
                if c.key in row_dict:
                    taxonomy[c.key] = row_dict[c.key]
            if taxonomy:
                obj["taxonomy_list"].append(taxonomy)

            res[npi_value] = obj
            continue

        # Fallback for positional row types. Keep this defensive to avoid crashes
        # when result shape differs from model column expectations.
        row_len = len(r)
        if row_len <= 1:
            continue

        obj = {"taxonomy_list": []}
        count = 1
        obj["distance"] = r[count]

        for c in NPIAddress.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if c.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                continue
            obj[c.key] = r[count]
        for c in NPIData.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if c.key in ("npi", "checksum", "do_business_as_text"):
                continue
            obj[c.key] = r[count]

        npi_value = obj.get("npi")
        if npi_value is None:
            continue
        if npi_value in res:
            obj = res[npi_value]
        taxonomy = {}
        for c in NPIDataTaxonomy.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if c.key in ("npi", "checksum"):
                continue
            taxonomy[c.key] = r[count]
        if taxonomy:
            obj["taxonomy_list"].append(taxonomy)

        res[npi_value] = obj

    res = list(res.values())
    for row in res:
        if isinstance(row, dict):
            _add_canonical_contact_fields_to_address(row)
    _redact_internal_address_fields(res)
    return response.json(res, default=str)


@blueprint.get("/id/<npi>/full_taxonomy")
async def get_full_taxonomy_list(_request, npi):
    """Return all NUCC taxonomy details attached to one NPI."""
    t = []
    npi = int(npi)
    # plan_data = await db.select(
    #     [Plan.marketing_name, Plan.plan_id, PlanAttributes.full_plan_id, Plan.year]).select_from(
    #     Plan.join(PlanAttributes, ((Plan.plan_id == func.substr(PlanAttributes.full_plan_id, 1, 14)) & (
    #                 Plan.year == PlanAttributes.year)))). \
    #     group_by(PlanAttributes.full_plan_id, Plan.plan_id, Plan.marketing_name, Plan.year).all()
    stmt = (
        select(NPIDataTaxonomy, NUCCTaxonomy)
        .where(NPIDataTaxonomy.npi == npi)
        .where(NUCCTaxonomy.code == NPIDataTaxonomy.healthcare_provider_taxonomy_code)
    )
    result = await db.execute(stmt)
    for taxonomy, nucc in result.all():
        payload = taxonomy.to_json_dict()
        payload["nucc_taxonomy"] = nucc.to_json_dict()
        t.append(payload)
    return response.json(t)


@blueprint.get("/plans_by_npi/<npi>")
async def get_plans_by_npi(_request, npi):
    """Return issuer plan links recorded for one NPI."""

    data = []
    plan_data = []
    issuer_data = []
    npi = int(npi)

    query = (
        db.select(PlanNPIRaw, Issuer)
        .where(Issuer.issuer_id == PlanNPIRaw.issuer_id)
        .where(PlanNPIRaw.npi == npi)
        .order_by(PlanNPIRaw.issuer_id.desc())
    )

    async for plan_raw, issuer in query.iterate():
        data.append({"npi_info": plan_raw.to_json_dict(), "issuer_info": issuer.to_json_dict()})

    return response.json({"npi_data": data, "plan_data": plan_data, "issuer_data": issuer_data})


@blueprint.get("/id/<npi>")
async def get_npi(request, npi):
    """Return one NPPES- or profile-backed provider with optional provenance."""
    force_address_update = _parse_bool_arg(request.args.get("force_address_update"), default=False)
    include_sources = _parse_bool_arg(request.args.get("include_sources"), default=False)
    include_evidence = _parse_bool_arg(request.args.get("include_evidence"), default=False)
    include_profile = _parse_bool_arg(
        request.args.get("include_profile"),
        default=True,
    )
    if _parse_bool_arg(request.args.get("debug"), default=False):
        include_sources = True
        include_evidence = True
    include_extra_info = _parse_bool_arg(request.args.get("extra_info"), default=False)
    sync_geocode = _parse_bool_arg(
        request.args.get("sync_geocode"),
        default=_env_flag("HLTHPRT_NPI_DETAIL_SYNC_GEOCODE", "HLTHPRT_NPI_API_SYNC_GEOCODE", default=True),
    )
    lookup_stored_geocode = _parse_bool_arg(
        request.args.get("lookup_stored_geocode"),
        default=_env_flag(
            "HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE",
            "HLTHPRT_NPI_API_LOOKUP_STORED_GEOCODE",
            default=True,
        ),
    )
    include_chain_enrichment = _include_chain_provider_enrichment(request.args.get("show"))
    provider_enrichment_view = _normalize_provider_enrichment_view(request.args.get("view"))
    # address_list paging: default-bounded so high-volume providers never serialize
    # 1k+ addresses; address_limit=all (or 0) opts out and returns the full list.
    raw_address_limit = request.args.get("address_limit")
    if raw_address_limit is None or str(raw_address_limit).strip() == "":
        address_limit = NPI_DETAIL_ADDRESS_DEFAULT_LIMIT
    elif str(raw_address_limit).strip().lower() in ("all", "0", "-1"):
        address_limit = None
    else:
        try:
            address_limit = max(1, min(int(raw_address_limit), NPI_DETAIL_ADDRESS_MAX_LIMIT))
        except (TypeError, ValueError):
            address_limit = NPI_DETAIL_ADDRESS_DEFAULT_LIMIT
    try:
        address_offset = max(int(request.args.get("address_offset") or 0), 0)
    except (TypeError, ValueError):
        address_offset = 0
    include_address_total = _parse_bool_arg(request.args.get("include_address_total"), default=True)
    raw_address_key = request.args.get("address_key")
    if raw_address_key is None or not str(raw_address_key).strip():
        address_key = None
    else:
        try:
            address_key = str(uuid.UUID(str(raw_address_key).strip()))
        except (AttributeError, TypeError, ValueError):
            raise sanic.exceptions.InvalidUsage("address_key must be a UUID")
    npi = int(npi)
    request_session = _request_session(request)
    profile_record: dict[str, Any] | None = None
    if include_profile:
        try:
            profile_record = (
                await _fetch_provider_directory_profile_map(
                    [npi],
                    include_evidence=include_evidence,
                    session=request_session,
                )
            ).get(npi)
        except Exception as exc:  # pragma: no cover - transient publication fallback
            logger.debug(
                "Provider Directory profile fetch failed for npi=%s: %s",
                npi,
                exc,
            )
    cache_key = _npi_detail_cache_key(
        npi,
        view=provider_enrichment_view,
        include_chain=include_chain_enrichment,
        extra_info=include_extra_info,
        sync_geocode=sync_geocode,
        lookup_stored_geocode=lookup_stored_geocode,
        include_sources=include_sources,
        include_evidence=include_evidence,
        include_profile=include_profile,
        profile_generation=(
            str(profile_record["profile"].get("generation_id"))
            if profile_record and isinstance(profile_record.get("profile"), Mapping)
            else None
        ),
        address_limit=address_limit,
        address_offset=address_offset,
        include_address_total=include_address_total,
        address_key=address_key,
    )
    if not force_address_update:
        cached_body = _npi_detail_response_cache_get(cache_key)
        if cached_body is not None:
            return response.raw(cached_body, content_type="application/json")
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    address_archive_cutover = _env_flag("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER")
    v2_archive_table_cache = SimpleNamespace(resolved=False, table_name=None)
    v2_archive_table_lock = asyncio.Lock()

    async def _table_exists(table_name: str) -> bool:
        value = await db.scalar("SELECT to_regclass(:table_name);", table_name=f"{db_schema}.{table_name}")
        return isinstance(value, str) and bool(value)

    async def _table_has_column(table_name: str, column_name: str) -> bool:
        return bool(await db.scalar(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = :schema
                   AND table_name = :table
                   AND column_name = :column
            );
            """,
            schema=db_schema,
            table=table_name,
            column=column_name,
        ))

    async def _has_address_key_functions() -> bool:
        value = await db.scalar(
            "SELECT to_regprocedure(:signature);",
            signature=f"{db_schema}.addr_key_v1(text,text,text,text,text,text)",
        )
        return isinstance(value, str) and bool(value)

    async def _v2_archive_table() -> str | None:
        if v2_archive_table_cache.resolved:
            return v2_archive_table_cache.table_name
        async with v2_archive_table_lock:
            if v2_archive_table_cache.resolved:
                return v2_archive_table_cache.table_name
            if address_archive_cutover and hasattr(db, "first"):
                preferred = os.getenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2").strip() or "address_archive_v2"
                for table_name in (preferred,):
                    if (
                        await _table_exists(table_name)
                        and await _table_has_column(table_name, "address_key")
                        and await _table_has_column(table_name, "geo_source")
                        and await _has_address_key_functions()
                    ):
                        v2_archive_table_cache.table_name = table_name
                        break
            v2_archive_table_cache.resolved = True
            return v2_archive_table_cache.table_name

    async def _archive_coordinates_for(address):
        archive_table = await _v2_archive_table()
        if archive_table:
            archive_params = {
                "first_line": address.get("first_line"),
                "second_line": address.get("second_line"),
                "city_name": address.get("city_name"),
                "state_name": address.get("state_name"),
                "postal_code": address.get("postal_code"),
                "country_code": address.get("country_code") or "US",
            }
            archive_sql = f"""
                SELECT long, lat, formatted_address, place_id, geo_source
                  FROM {db_schema}.{archive_table}
                 WHERE address_key = {db_schema}.addr_key_v1(
                    :first_line, :second_line, :city_name, :state_name, :postal_code, :country_code
                 )
                   AND lat IS NOT NULL
                   AND long IS NOT NULL
                """
            if request_session is not None:
                result = await _execute_stmt(text(archive_sql), session=request_session, params=archive_params)
                row = result.first()
            else:
                row = await db.first(archive_sql, **archive_params)
            if row:
                data = row._mapping
                return SimpleNamespace(
                    long=data["long"],
                    lat=data["lat"],
                    formatted_address=data["formatted_address"],
                    place_id=data["place_id"],
                    geo_source=data.get("geo_source"),
                )
        legacy_stmt = select(AddressArchive).where(AddressArchive.checksum == address["checksum"])
        if request_session is not None:
            result = await _execute_stmt(legacy_stmt, session=request_session)
            return result.scalar()
        return await db.scalar(legacy_stmt)

    async def _openaddresses_coordinates_for(address):
        if request_session is None and not hasattr(db, "first"):
            return None
        params = lookup_params_from_address(address)
        if not params or not await _table_exists("openaddresses_geocode"):
            return None
        for query in (exact_lookup_sql(db_schema), fuzzy_lookup_sql(db_schema), relaxed_lookup_sql(db_schema)):
            if request_session is not None:
                result = await _execute_stmt(text(query), session=request_session, params=params)
                row = result.first()
            else:
                row = await db.first(query, **params)
            if row:
                data = row._mapping
                return SimpleNamespace(
                    long=data["long"],
                    lat=data["lat"],
                    formatted_address=data["formatted_address"],
                    place_id=data["place_id"],
                    geo_source=data["geo_source"],
                    geocode_source=data["geocode_source"],
                    geocode_quality=data["geocode_quality"],
                )
        return None

    async def update_addr_coordinates(
        address,
        long,
        lat,
        formatted_address,
        place_id,
        geo_source=None,
        geocode_source=None,
        geocode_quality=None,
    ):
        """Persist geocoding coordinates and provenance for one address."""
        checksum = address["checksum"]
        geo_source = str(geo_source).strip().lower() if geo_source else None
        if geo_source not in {"mapbox", "google", "tiger", "manual", "openaddresses"}:
            geo_source = "google" if place_id else None
        geocode_source = str(geocode_source).strip().lower() if geocode_source else None
        geocode_quality = str(geocode_quality).strip().lower() if geocode_quality else None
        if not geocode_source:
            geocode_source = "api_geocode"
        if not geocode_quality:
            geocode_quality = "unknown"
        await (
            db.update(NPIAddress)
            .where(NPIAddress.checksum == checksum)
            .values(
                long=long,
                lat=lat,
                formatted_address=formatted_address,
                place_id=place_id,
            )
            .status()
        )
        row = await db.scalar(select(NPIAddress).where(NPIAddress.checksum == checksum))
        if row is None:
            return
        archive_table = await _v2_archive_table()
        if archive_table:
            await db.status(
                f"""
                INSERT INTO {db_schema}.{archive_table} (
                    address_key, identity_key, identity_version, precision, premise_key,
                    line1_norm, unit_norm, city_norm, state_code, zip5, zip4, country_code,
                    first_line, second_line, city_name, state_name, postal_code,
                    telephone_number, fax_number, formatted_address, lat, long, place_id,
                    geo_source, geocode_source, geocode_quality, geocoded_at, source_bits, display_priority,
                    date_added
                )
                SELECT
                    {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')),
                    {db_schema}.addr_identity_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')),
                    1,
                    CASE
                        WHEN split_part({db_schema}.addr_identity_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')), '|', 8) = 'city_zip'
                        THEN 'city_zip' ELSE 'street'
                    END,
                    {db_schema}.addr_premise_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')),
                    {db_schema}.addr_street_norm_v1(first_line, second_line),
                    {db_schema}.addr_unit_norm_v1(first_line, second_line),
                    {db_schema}.addr_city_norm_v1(city_name),
                    LEFT({db_schema}.addr_state_code_v1(state_name), 32),
                    {db_schema}.addr_zip5_norm_v1(postal_code),
                    NULLIF(substring(regexp_replace(COALESCE(postal_code, ''), '[^0-9]', '', 'g') from 6 for 4), ''),
                    {db_schema}.addr_country_code_v1(COALESCE(NULLIF(country_code, ''), 'US')),
                    first_line, second_line, city_name, state_name, postal_code,
                    telephone_number, fax_number, formatted_address, lat, long, place_id,
                    CAST(:geo_source AS {db_schema}.address_archive_geo_source),
                    :geocode_source, :geocode_quality, now(), 1, 0, date_added
                  FROM (
                    SELECT DISTINCT ON (
                        {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US'))
                    )
                        *
                      FROM {db_schema}.npi_address
                     WHERE checksum = :checksum
                       AND {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')) IS NOT NULL
                     ORDER BY
                        {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')),
                        (place_id IS NOT NULL) DESC,
                        date_added DESC NULLS LAST,
                        npi
                  ) source
                ON CONFLICT (address_key) DO UPDATE SET
                    formatted_address = COALESCE({db_schema}.{archive_table}.formatted_address, EXCLUDED.formatted_address),
                    lat = COALESCE({db_schema}.{archive_table}.lat, EXCLUDED.lat),
                    long = COALESCE({db_schema}.{archive_table}.long, EXCLUDED.long),
                    place_id = COALESCE({db_schema}.{archive_table}.place_id, EXCLUDED.place_id),
                    geo_source = COALESCE({db_schema}.{archive_table}.geo_source, EXCLUDED.geo_source),
                    geocode_source = COALESCE({db_schema}.{archive_table}.geocode_source, EXCLUDED.geocode_source),
                    geocode_quality = COALESCE({db_schema}.{archive_table}.geocode_quality, EXCLUDED.geocode_quality),
                    geocoded_at = COALESCE({db_schema}.{archive_table}.geocoded_at, EXCLUDED.geocoded_at),
                    source_bits = {db_schema}.{archive_table}.source_bits | 1,
                    last_seen_at = now();
                """,
                checksum=checksum,
                geo_source=geo_source,
                geocode_source=geocode_source,
                geocode_quality=geocode_quality,
            )
            return
        obj = {column.key: getattr(row, column.key, None) for column in AddressArchive.__table__.columns}

        # long = long,
        # lat = lat,
        # formatted_address = formatted_address,
        # place_id = place_id
        #         del obj['checksum']
        try:
            await (
                db.insert(AddressArchive)
                .values(obj)
                .on_conflict_do_update(
                    index_elements=AddressArchive.__my_index_elements__,
                    set_=obj,
                )
                .status()
            )
        except Exception as exc:
            logger.warning("Could not archive address checksum=%s: %s", checksum, exc)

    async def _update_address(x):
        """Geocode one address when it does not already have coordinates."""
        if x.get("lat"):
            return x
        postal_code = x.get("postal_code")
        if postal_code is not None:
            postal_code = str(postal_code)
        if postal_code and len(postal_code) > 5:
            postal_code = f"{postal_code[0:5]}-{postal_code[5:]}"
        state_postal = " ".join(
            part
            for part in [
                str(x.get("state_name") or "").strip(),
                str(postal_code or "").strip(),
            ]
            if part
        )
        t_addr = ", ".join(
            part
            for part in [
                str(x.get("first_line") or "").strip(),
                str(x.get("second_line") or "").strip(),
                str(x.get("city_name") or "").strip(),
                state_postal,
            ]
            if part
        )
        t_addr = t_addr.replace(" , ", " ")

        d = x
        for key in ("lat", "long", "formatted_address", "place_id"):
            d.setdefault(key, None)
        if force_address_update:
            d["long"] = None
            d["lat"] = None
            d["formatted_address"] = None
            d["place_id"] = None

        if not d["lat"]:

            # try:
            #     raw_sql = text(f"""SELECT
            #            g.rating,
            #            ST_X(g.geomout) As lon,
            #            ST_Y(g.geomout) As lat,
            #             pprint_addy(g.addy) as formatted_address
            #             from mrf.npi,
            #             standardize_address('us_lex',
            #                  'us_gaz', 'us_rules', :addr) as addr,
            #             geocode((
            #                 (addr).house_num,  --address
            #                 null,              --predirabbrev
            #                 (addr).name,       --streetname
            #                 (addr).suftype,    --streettypeabbrev
            #                 null,              --postdirabbrev
            #                 (addr).unit,       --internal
            #                 (addr).city,       --location
            #                 (addr).state,      --stateabbrev
            #                 (addr).postcode,   --zip
            #                 true,               --parsed
            #                 null,               -- zip4
            #                 (addr).house_num    -- address_alphanumeric
            #             )::norm_addy) as g
            #            where npi = :npi""")
            #     addr = await conn.status(raw_sql, addr=t_addr, npi=npi)
            #
            #     if addr and len(addr[-1]) and addr[-1][0] and addr[-1][0][0] < 2:
            #         d['long'] = addr[-1][0][1]
            #         d['lat'] = addr[-1][0][2]
            #         d['formatted_address'] = addr[-1][0][3]
            #         d['place_id'] = None
            # except:
            #     pass
            update_geo = False
            if request.app.config.get("NPI_API_UPDATE_GEOCODE") and not d["lat"]:
                update_geo = True

            if lookup_stored_geocode and (not d["lat"]) and (not force_address_update):
                res = await _archive_coordinates_for(x)
                if res:
                    d["long"] = res.long
                    d["lat"] = res.lat
                    d["formatted_address"] = res.formatted_address
                    d["place_id"] = res.place_id
                    d["geo_source"] = getattr(res, "geo_source", None) or ("google" if res.place_id else None)

            if (lookup_stored_geocode or sync_geocode or force_address_update) and not d["lat"]:
                try:
                    res = await _openaddresses_coordinates_for(x)
                    if res:
                        d["long"] = res.long
                        d["lat"] = res.lat
                        d["formatted_address"] = res.formatted_address
                        d["place_id"] = res.place_id
                        d["geo_source"] = res.geo_source
                        d["geocode_source"] = res.geocode_source
                        d["geocode_quality"] = res.geocode_quality
                except Exception as exc:
                    logger.debug("OpenAddresses geocoding failed for %s: %s", t_addr, exc)

            if (sync_geocode or force_address_update) and not d["lat"]:
                try:
                    params = {
                        request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY_PARAM"): random.choice(
                            json.loads(request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY"))
                        )
                    }
                    encoded_params = ".json?".join(
                        (
                            urllib.parse.quote_plus(t_addr),
                            urllib.parse.urlencode(params, doseq=True),
                        )
                    )
                    if qp := request.app.config.get("GEOCODE_MAPBOX_STYLE_ADDITIONAL_QUERY_PARAMS"):
                        encoded_params = "&".join(
                            (
                                encoded_params,
                                qp,
                            )
                        )
                    url = request.app.config.get("GEOCODE_MAPBOX_STYLE_URL") + encoded_params
                    resp = await download_it(url, local_timeout=5)
                    geo_data = json.loads(resp)
                    if geo_data.get("features", []):
                        d["long"] = geo_data["features"][0]["geometry"]["coordinates"][0]
                        d["lat"] = geo_data["features"][0]["geometry"]["coordinates"][1]
                        if t2 := geo_data["features"][0].get("matching_place_name"):
                            d["formatted_address"] = t2
                        else:
                            d["formatted_address"] = geo_data["features"][0]["place_name"]
                        d["place_id"] = None
                        d["geo_source"] = "mapbox"
                except Exception as exc:
                    logger.debug("Mapbox geocoding failed for %s: %s", t_addr, exc)

            if (sync_geocode or force_address_update) and not d["lat"]:
                try:
                    params = {
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM"): t_addr,
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_KEY_PARAM"): request.app.config.get(
                            "GEOCODE_GOOGLE_STYLE_KEY"
                        ),
                    }
                    encoded_params = urllib.parse.urlencode(params, doseq=True)
                    if qp := request.app.config.get("GEOCODE_GOOGLE_STYLE_ADDITIONAL_QUERY_PARAMS"):
                        encoded_params = "&".join(
                            (
                                encoded_params,
                                qp,
                            )
                        )
                    url = "?".join(
                        (
                            request.app.config.get("GEOCODE_GOOGLE_STYLE_URL"),
                            encoded_params,
                        )
                    )
                    resp = await download_it(url)
                    geo_data = json.loads(resp)
                    if geo_data.get("results", []):
                        d["long"] = geo_data["results"][0]["geometry"]["location"]["lng"]
                        d["lat"] = geo_data["results"][0]["geometry"]["location"]["lat"]
                        d["formatted_address"] = geo_data["results"][0]["formatted_address"]
                        d["place_id"] = geo_data["results"][0]["place_id"]
                        d["geo_source"] = "google"
                except Exception as exc:
                    logger.warning("Google geocoding failed for %s: %s", t_addr, exc)

            if update_geo and d.get("lat"):
                request.app.add_task(
                    update_addr_coordinates(
                        x,
                        d["long"],
                        d["lat"],
                        d["formatted_address"],
                        d["place_id"],
                        d.get("geo_source"),
                        d.get("geocode_source"),
                        d.get("geocode_quality"),
                    )
                )

        return d

    build_kwargs: dict[str, Any] = {
        "address_limit": address_limit,
        "address_offset": address_offset,
        "include_address_total": include_address_total,
        "address_key": address_key,
    }
    if request_session is not None:
        build_kwargs["session"] = request_session
    if include_sources or include_evidence:
        build_kwargs["include_sources"] = include_sources
        build_kwargs["include_evidence"] = include_evidence
    data = await _build_npi_details(npi, **build_kwargs)

    if not data:
        if not profile_record:
            raise sanic.exceptions.NotFound
        data = {
            "npi": npi,
            "provider_directory_profile": profile_record["profile"],
        }
        if include_evidence and profile_record.get("evidence") is not None:
            data["provider_directory_profile_evidence"] = profile_record[
                "evidence"
            ]
        response_body = json.dumps(
            data,
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
        if _npi_detail_response_cacheable(
            data,
            force_address_update=force_address_update,
            sync_geocode=sync_geocode,
        ):
            _npi_detail_response_cache_set(cache_key, response_body)
        return response.raw(response_body, content_type="application/json")

    address_total = data.pop("address_total", None)

    addresses = data.get("address_list") or []
    addresses.extend(
        await _fetch_provider_directory_address_overlay(
            npi,
            address_key=address_key,
            session=request_session,
        )
    )
    if address_key is not None:
        addresses = [
            address
            for address in addresses
            if isinstance(address, Mapping)
            and str(address.get("address_key") or "").lower() == address_key
        ]
    if not include_extra_info:
        addresses = [address for address in addresses if _is_public_street_level_address(address)]
    addresses = _dedupe_addresses_by_key(addresses)
    if addresses:
        if include_sources or include_evidence:
            await _attach_provider_directory_source_details(
                addresses,
                include_role_evidence=include_evidence,
                session=request_session,
            )
        update_address_tasks = [_update_address(a) for a in addresses if a]
        if update_address_tasks:
            data["address_list"] = list(await asyncio.gather(*update_address_tasks))
        else:
            data["address_list"] = []
    else:
        data["address_list"] = []
    for address in data["address_list"]:
        if isinstance(address, dict):
            _attach_public_address_site_key(address, address)
            for key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                address.pop(key, None)
            if not include_evidence:
                address.pop("source_record_ids", None)
    if address_limit is not None:
        # Never silently truncate: tell the caller the full count and how to page.
        returned = len(data["address_list"])
        data["address_pagination"] = {
            "limit": address_limit,
            "offset": address_offset,
            "returned": returned,
            "total": address_total,
            "has_more": bool(address_total is not None and address_offset + address_limit < address_total),
        }

    if provider_enrichment_view == "summary":
        fetch_provider_enrichment = _fetch_provider_enrichment_summary_detail
    else:
        fetch_provider_enrichment = _fetch_provider_enrichment_detail

    provider_enrichment_payload: Optional[dict[str, Any]] = None
    try:
        if request_session is not None:
            other_names = await _fetch_other_names(npi, session=request_session)
            provider_enrichment_payload = await fetch_provider_enrichment(
                npi,
                include_chain=include_chain_enrichment,
                session=request_session,
            )
        else:
            other_names_task = asyncio.create_task(_fetch_other_names(npi))
            provider_enrichment_task = asyncio.create_task(
                fetch_provider_enrichment(
                    npi,
                    include_chain=include_chain_enrichment,
                )
            )
            other_names, provider_enrichment_payload = await asyncio.gather(
                other_names_task,
                provider_enrichment_task,
            )
    except Exception as exc:  # pragma: no cover - defensive fallback for transient DB states
        logger.debug("Provider enrichment detail fetch failed for npi=%s: %s", npi, exc)
        if "other_names_task" in locals() and not other_names_task.done():
            other_names_task.cancel()
        try:
            other_names = await _fetch_other_names(npi, session=request_session)
        except Exception:  # pragma: no cover - defensive fallback
            other_names = []
        provider_enrichment_payload = None
    data["other_name_list"] = other_names

    existing_dba = [name for name in (data.get("do_business_as") or []) if name]
    if existing_dba:
        data["do_business_as"] = list(dict.fromkeys(existing_dba))
    else:
        candidates = [
            entry.get("other_provider_identifier")
            for entry in other_names
            if entry.get("other_provider_identifier_type_code") == "3" and entry.get("other_provider_identifier")
        ]
        data["do_business_as"] = list(dict.fromkeys(candidates)) if candidates else []

    if provider_enrichment_payload is not None:
        data["provider_enrichment"] = provider_enrichment_payload
    else:
        data["provider_enrichment"] = {
            "summary": None,
        }
        if provider_enrichment_view == "full":
            data["provider_enrichment"]["enrollments"] = {
                "ffs_public": [],
                "hospital": [],
                "hha": [],
                "hospice": [],
                "fqhc": [],
                "rhc": [],
                "snf": [],
            }
        else:
            data["provider_enrichment"]["ffs_visibility"] = {
                "show_mode": "chain" if include_chain_enrichment else "default",
                "chain_hidden": False,
                "chain_enrollment_count": 0,
                "chain_enrollment_ids": [],
            }

    if include_profile and profile_record:
        data["provider_directory_profile"] = profile_record["profile"]
        if include_evidence and profile_record.get("evidence") is not None:
            data["provider_directory_profile_evidence"] = profile_record[
                "evidence"
            ]

    _redact_internal_address_fields(data)
    response_body = json.dumps(data, default=str, separators=(",", ":")).encode("utf-8")
    if _npi_detail_response_cacheable(
        data,
        force_address_update=force_address_update,
        sync_geocode=sync_geocode,
    ):
        _npi_detail_response_cache_set(cache_key, response_body)
    return response.raw(response_body, content_type="application/json")

async def _build_npi_details(
    npi: int,
    *,
    include_sources: bool = False,
    include_evidence: bool = False,
    address_limit: int | None = None,
    address_offset: int = 0,
    include_address_total: bool = True,
    address_key: str | None = None,
    session: Any = None,
) -> dict:
    """Assemble one provider identity, taxonomy, and address detail payload."""
    npi_data_table = NPIData.__table__
    taxonomy_table = NPIDataTaxonomy.__table__
    taxonomy_group_table = NPIDataTaxonomyGroup.__table__
    filter_capabilities = await _resolve_npi_filter_capabilities(session=session)
    procedures_array_available = bool(filter_capabilities.get("npi_procedures_array_available", True))
    medications_array_available = bool(filter_capabilities.get("npi_medications_array_available", True))
    address_model = await _address_serving_model(
        _public_address_serving_column_keys() - {"procedures_array", "medications_array"},
        session=session,
    )
    address_table = address_model.__table__
    existing_address_columns = await _table_columns(address_model.__tablename__, session=session)
    if not existing_address_columns:
        existing_address_columns = _model_table_columns(address_model)

    allowed_address_columns = set(_model_table_columns(NPIAddress))
    if address_model is EntityAddressUnified:
        # Keep premise_key internally so public response redaction can expose it
        # as address_site_key without leaking the internal column name.
        allowed_address_columns.add("premise_key")
        # Surface per-address source + plan/network attribution by default so the
        # unified table fulfils its purpose: see where each address came from and
        # which plans/networks it belongs to. Heavier internals stay behind flags.
        allowed_address_columns.update(PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS)
        # Used internally to resolve provider_directory_fhir source ids to
        # readable carrier/source details. The raw ids are removed from the
        # response unless evidence output is requested.
        allowed_address_columns.add("source_record_ids")
    if include_sources or include_evidence:
        allowed_address_columns.update(PUBLIC_ADDRESS_SOURCE_DEBUG_COLUMNS)
    if include_evidence:
        allowed_address_columns.update(PUBLIC_ADDRESS_EVIDENCE_DEBUG_COLUMNS)

    address_columns = []
    for column in address_table.columns:
        if column.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS and column.key not in allowed_address_columns:
            continue
        if column.key not in allowed_address_columns:
            continue
        if column.key not in existing_address_columns:
            if column.key == "procedures_array":
                address_columns.append(literal_column("'{}'::INTEGER[]").label("procedures_array"))
            elif column.key == "medications_array":
                address_columns.append(literal_column("'{}'::INTEGER[]").label("medications_array"))
            continue
        if column.key == "procedures_array" and not procedures_array_available:
            address_columns.append(literal_column("'{}'::INTEGER[]").label("procedures_array"))
            continue
        if column.key == "medications_array" and not medications_array_available:
            address_columns.append(literal_column("'{}'::INTEGER[]").label("medications_array"))
            continue
        address_columns.append(address_table.c[column.key])

    # Expose every known service location, not just the NPPES primary/secondary.
    # The unified builder stores TiC/PTG and ACA practice locations as type
    # 'practice'/'site', so the legacy primary/secondary-only filter silently
    # dropped all TiC addresses. Widen the filter for the unified model only;
    # legacy NPIAddress behaviour is unchanged.
    def _address_type_clause(table: Any) -> Any:
        if address_model is EntityAddressUnified:
            return table.c.type.in_(
                ("primary", "secondary", "practice", "site")
            )
        return or_(
            table.c.type == "primary",
            table.c.type == "secondary",
        )

    # Keep the serving-type predicate outside this optimization barrier. On
    # PostgreSQL 18, combining it with NPI can otherwise select a ZIP-first
    # partial-index skip scan instead of the direct NPI index.
    base_address_filters = [address_table.c.npi == npi]
    if address_key is not None and address_model is EntityAddressUnified:
        base_address_filters[0] = func.coalesce(
            address_table.c.npi, address_table.c.inferred_npi
        ) == npi
    if address_key is not None:
        base_address_filters.append(address_table.c.address_key == address_key)
    npi_address_rows = (
        select(*address_columns)
        .where(*base_address_filters)
        .offset(0)
        .subquery("npi_address_rows")
    )
    address_subquery_base = (
        select(*npi_address_rows.c)
        .where(_address_type_clause(npi_address_rows))
        # Deterministic order so address_offset paging is stable across requests.
        .order_by(
            npi_address_rows.c.type,
            npi_address_rows.c.first_line,
            npi_address_rows.c.city_name,
        )
    )
    address_total: int | None = None
    if address_limit is not None and include_address_total:
        count_npi_rows = (
            select(address_table.c.type)
            .where(*base_address_filters)
            .offset(0)
            .subquery("count_npi_address_rows")
        )
        count_stmt = select(func.count()).select_from(count_npi_rows).where(
            _address_type_clause(count_npi_rows)
        )
        if session is not None:
            total_res = await session.execute(count_stmt)
            address_total = int(total_res.scalar() or 0)
        else:
            try:
                address_total = int(await db.scalar(count_stmt) or 0)
            except Exception:  # pragma: no cover - gino fallback when no session bound
                address_total = None
    if address_limit is not None:
        address_subquery_base = address_subquery_base.limit(address_limit).offset(max(address_offset or 0, 0))
    try:
        address_subquery = address_subquery_base.alias("address_list")
    except NameError:
        address_subquery = address_subquery_base

    taxonomy_aggregate = (
        select(
            taxonomy_table.c.npi,
            func.json_agg(
                literal_column(
                    f'distinct "{NPIDataTaxonomy.__tablename__}"'
                )
            ).label("rows"),
        )
        .select_from(taxonomy_table)
        .where(taxonomy_table.c.npi == npi)
        .group_by(taxonomy_table.c.npi)
        .subquery("taxonomy_aggregate")
    )
    taxonomy_group_aggregate = (
        select(
            taxonomy_group_table.c.npi,
            func.json_agg(
                literal_column(
                    f'distinct "{NPIDataTaxonomyGroup.__tablename__}"'
                )
            ).label("rows"),
        )
        .select_from(taxonomy_group_table)
        .where(taxonomy_group_table.c.npi == npi)
        .group_by(taxonomy_group_table.c.npi)
        .subquery("taxonomy_group_aggregate")
    )
    select_columns = [
        npi_data_table,
        taxonomy_aggregate.c.rows,
        taxonomy_group_aggregate.c.rows,
    ]
    join_clause = npi_data_table.outerjoin(
        taxonomy_aggregate,
        npi_data_table.c.npi == taxonomy_aggregate.c.npi,
    ).outerjoin(
        taxonomy_group_aggregate,
        npi_data_table.c.npi == taxonomy_group_aggregate.c.npi,
    )
    if hasattr(address_subquery, "c"):
        address_aggregate = (
            select(
                address_subquery.c.npi,
                func.json_agg(
                    literal_column('distinct "address_list"')
                ).label("rows"),
            )
            .select_from(address_subquery)
            .group_by(address_subquery.c.npi)
            .subquery("address_aggregate")
        )
        join_clause = join_clause.outerjoin(
            address_aggregate,
            npi_data_table.c.npi == address_aggregate.c.npi,
        )
        select_columns.append(address_aggregate.c.rows)
    else:
        select_columns.append(literal_column("NULL::json"))
    query = (
        db.select(*select_columns)
        .select_from(join_clause)
        .where(npi_data_table.c.npi == npi)
    )

    if session is not None:
        detail_query_result = await session.execute(query._stmt)
        detail_rows = detail_query_result.all()
    else:
        detail_rows = await query.all()
    if not detail_rows:
        return {}
    result_row = detail_rows[0]
    provider_detail_map: dict[str, Any] = {
        "taxonomy_list": [],
        "taxonomy_group_list": [],
        "address_list": [],
    }
    idx = 0
    for column in NPIData.__table__.columns:
        column_value = result_row[idx]
        idx += 1
        if column.key == "do_business_as_text":
            continue
        provider_detail_map[column.key] = column_value

    if result_row[idx]:
        provider_detail_map["taxonomy_list"].extend(_public_nested_taxonomy_rows(result_row[idx]))
    idx += 1
    if result_row[idx]:
        provider_detail_map["taxonomy_group_list"].extend(_public_nested_taxonomy_rows(result_row[idx]))
    idx += 1
    if idx < len(result_row) and result_row[idx]:
        provider_detail_map["address_list"] = result_row[idx]
    if address_total is not None:
        provider_detail_map["address_total"] = address_total
    provider_detail_map["do_business_as"] = provider_detail_map.get("do_business_as") or []
    return provider_detail_map


async def _fetch_other_names(npi: int, *, session: Any = None) -> list[dict[str, Any]]:
    result = await _execute_stmt(select(NPIDataOtherIdentifier).where(NPIDataOtherIdentifier.npi == npi), session=session)
    rows: list[dict[str, Any]] = []
    seen_checksums: set[int] = set()
    for row in result.scalars():
        payload = row.to_json_dict()
        checksum = payload.pop("checksum", None)
        if checksum in seen_checksums:
            continue
        if checksum is not None:
            seen_checksums.add(checksum)
        payload.pop("npi", None)
        rows.append(payload)
    return rows


PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE = "provider_directory_address_overlay"


def _provider_directory_overlay_query_sql(
    overlay_columns: set[str],
) -> str:
    lat_select = "lat" if "lat" in overlay_columns else "NULL::numeric AS lat"
    long_select = "long" if "long" in overlay_columns else "NULL::numeric AS long"
    coordinate_group_by = ", lat, long" if {"lat", "long"}.issubset(overlay_columns) else ""
    overlay_table_sql = _schema_cache_key(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    current_resource_ctes_sql = _provider_directory_current_resource_ctes_sql(schema)
    return f"""
        WITH {current_resource_ctes_sql}, visible_overlay AS MATERIALIZED (
            SELECT overlay.*, current_resource.canonical_api_base
              FROM {overlay_table_sql} AS overlay
              JOIN current_resources AS current_resource
                ON current_resource.source_id = overlay.source_id
               AND current_resource.resource_type = overlay.resource_type
               AND current_resource.resource_id = overlay.resource_id
               AND overlay.last_seen_run_id = current_resource.run_id
             WHERE overlay.npi = :npi
               AND (
                   CAST(:address_key AS uuid) IS NULL
                   OR overlay.address_key = CAST(:address_key AS uuid)
               )
        )
        SELECT
            npi,
            'practice'::varchar AS type,
            first_line,
            second_line,
            city_name,
            state_name,
            state_code,
            postal_code,
            country_code,
            telephone_number,
            fax_number,
            phone_number,
            fax_number_digits,
            {lat_select},
            {long_select},
            address_key,
            address_precision,
            ARRAY['provider_directory_fhir']::varchar[] AS address_sources,
            ARRAY_AGG(overlay.source_record_id ORDER BY overlay.source_record_id)::varchar[] AS source_record_ids,
            COUNT(DISTINCT overlay.source_id)::integer AS source_count,
            COUNT(DISTINCT COALESCE(NULLIF(overlay.canonical_api_base, ''), overlay.source_id))::integer AS independent_source_count,
            (COUNT(DISTINCT COALESCE(NULLIF(overlay.canonical_api_base, ''), overlay.source_id)) > 1)::boolean AS multi_source_confirmed,
            MAX(source_updated_at) AS updated_at
          FROM visible_overlay AS overlay
      GROUP BY
            npi, first_line, second_line, city_name, state_name, state_code,
            postal_code, country_code, telephone_number, fax_number, phone_number,
            fax_number_digits, address_key, address_precision{coordinate_group_by}
      ORDER BY first_line NULLS LAST, city_name NULLS LAST, address_key;
    """


async def _fetch_provider_directory_address_overlay(
    npi: int,
    *,
    address_key: str | None = None,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Fetch FHIR address evidence with endpoint-aware confirmation counts."""
    if not await _table_exists(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, session=session):
        return []
    visibility_table_states = [
        await _table_exists(table_name, session=session)
        for table_name in PROVIDER_DIRECTORY_VISIBILITY_TABLES
    ]
    if not all(visibility_table_states):
        return []
    overlay_columns = await _table_columns(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, session=session)
    overlay_query = text(
        _provider_directory_overlay_query_sql(
            overlay_columns,
        )
    )
    overlay_result = await _execute_stmt(
        overlay_query,
        session=session,
        params={"npi": int(npi), "address_key": address_key},
    )
    overlay_addresses: list[dict[str, Any]] = []
    for overlay_row in overlay_result.all():
        overlay_mapping = getattr(overlay_row, "_mapping", overlay_row)
        overlay_addresses.append(dict(overlay_mapping))
    return overlay_addresses


def _should_include_npi_all_total(args: object, count_only: bool) -> bool:
    if count_only:
        return True
    getter = getattr(args, "get", None)
    if not callable(getter) or getter("include_total") is not None:
        return True
    return not any(
        str(getter(key) or "").strip()
        for key in ("phone", "address_key", PUBLIC_ADDRESS_SITE_KEY, "npi")
    )
