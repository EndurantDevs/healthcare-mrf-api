# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import base64
import binascii
import contextlib
import hashlib
import ipaddress
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
from dataclasses import dataclass
from datetime import UTC, date, datetime
from textwrap import dedent
from types import SimpleNamespace
from typing import Any, Awaitable, Callable, Iterable, Mapping, Optional, Sequence

import sanic.exceptions
from sanic import Blueprint, response
from sqlalchemy import JSON as SQLAlchemyJSON
from sqlalchemy import false, func, or_, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import literal_column, text, tuple_

from api.code_systems import (EXTERNAL_PROCEDURE_CODE_SYSTEMS,
                              INTERNAL_PROCEDURE_CODE_SYSTEM,
                              INTERNAL_RX_CODE_SYSTEM)
from api.endpoint.pagination import parse_pagination
from api.provider_demographic_filters import normalize_provider_sex_code
from api.provider_specialty_filters import (
    ensure_specialty_resolution_cache,
    resolve_provider_specialty_filter,
)
from api.provider_profile import (
    compose_provider_profile,
    compose_provider_profile_evidence,
    fetch_state_profile_projection,
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
from process.ext.address_format import (
    ADDRESS_FORMAT_FUNCTION,
    ADDRESS_FORMAT_SOURCE,
    ADDRESS_FORMAT_VERSION,
    render_formatted_address_v2,
)
from process.ext.contact_canon import canonicalize_one as canonicalize_contact_one
from process.ext.utils import download_it
from process.openaddresses import exact_lookup_sql, fuzzy_lookup_sql, lookup_params_from_address, relaxed_lookup_sql
from process import provider_directory_profile as profile_artifact
from process.florida_mqa_profile import STANDARD_CATEGORIES
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

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
PUBLIC_ADDRESS_EXCLUDED_COLUMNS = {
    "premise_key",
    "_address_site_keys",
    "_address_site_key_status",
}
PUBLIC_NPI_EXCLUDED_COLUMNS = {
    "employer_identification_number",
    "parent_organization_tin",
    "search_taxonomy_codes",
}
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
MRF_SOURCE_DETAIL_KEY = "mrf_sources"
MRF_SOURCE_COUNT_KEY = "mrf_source_count"
_PROVIDER_DIRECTORY_OBSERVED_RESOURCE_TYPES = (
    "Practitioner",
    "PractitionerRole",
)
_PROVIDER_DIRECTORY_OBSERVED_DATASET_STATUSES = (
    "acquiring",
    "incomplete",
    "failed",
    "acquisition_abandoned",
    "validated",
)
_PROVIDER_DIRECTORY_OBSERVED_RESOURCE_LIMIT = 32
UHC_PROVIDER_FILE_ADDRESS_STATUS = "payer_directory_candidate"
PROVIDER_DIRECTORY_PROFILE_SERVING_GENERATION_TABLE = (
    "provider_directory_profile_serving_generation"
)
PROVIDER_DIRECTORY_PROFILE_SERVING_QUERY_TEMPLATE = """
    WITH serving_generation AS MATERIALIZED (
        SELECT singleton_key, generation_id, published_at, profile_as_of,
               status, operation, control_generation,
               profile_target_oid, evidence_target_oid
          FROM {serving_generation_ref}
         WHERE singleton_key = 'global'
    )
    SELECT profile.npi, profile.profile_json,
           profile.generation_id AS materialization_generation_id,
           profile.published_at AS materialized_at,
           profile.tableoid::bigint AS materialization_profile_target_oid,
           serving_generation.singleton_key AS serving_generation_key,
           serving_generation.control_generation AS serving_control_generation,
           serving_generation.profile_target_oid AS serving_profile_target_oid,
           serving_generation.evidence_target_oid AS serving_evidence_target_oid,
           COALESCE(
               serving_generation.generation_id,
               profile.generation_id
           ) AS generation_id,
           COALESCE(
               serving_generation.published_at,
               profile.published_at
           ) AS published_at,
           serving_generation.profile_as_of AS profile_as_of
           {evidence_select}
      FROM {profile_table_ref} AS profile
      LEFT JOIN serving_generation
        ON serving_generation.status = 'published'
       AND serving_generation.operation = 'publish'
       AND serving_generation.control_generation > 0
       AND serving_generation.generation_id IS NOT NULL
       AND serving_generation.published_at IS NOT NULL
       AND serving_generation.profile_as_of IS NOT NULL
       AND serving_generation.profile_target_oid =
           to_regclass(:profile_table_ref)::oid::bigint
       AND serving_generation.evidence_target_oid =
           to_regclass(:evidence_table_ref)::oid::bigint
       AND profile.tableoid::bigint =
           serving_generation.profile_target_oid
     WHERE profile.npi = ANY(CAST(:npis AS bigint[]))
       AND (
           NOT EXISTS (SELECT 1 FROM serving_generation)
           OR serving_generation.singleton_key = 'global'
       );
"""
PROVIDER_DIRECTORY_CATALOG_ALIAS_COLUMNS = (
    # Catalog labels describe ingestion aliases, not provider-verified products.
    "source_id",
    "org_name",
    "plan_name",
)
PUBLIC_NESTED_TAXONOMY_EXCLUDED_COLUMNS = {"npi", "checksum"}


def _public_nested_taxonomy_rows(rows: Sequence[Any]) -> list[dict[str, Any]]:
    public_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for entry in rows or []:
        if not isinstance(entry, Mapping):
            continue
        public_entry_map = {
            str(key): value
            for key, value in entry.items()
            if str(key) not in PUBLIC_NESTED_TAXONOMY_EXCLUDED_COLUMNS
        }
        if not public_entry_map:
            continue
        dedupe_key = json.dumps(public_entry_map, default=str, sort_keys=True, separators=(",", ":"))
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        public_rows.append(public_entry_map)
    return public_rows


# When true, the geo radius search returns providers at ALL geocoded service
# locations (NPPES primary/secondary PLUS TiC/PTG/ACA practice/site), matching the
# widened geo_idx partial predicate. Default OFF: the live geo_idx must be rebuilt
# to cover practice/site rows (via an entity_address_unified refresh) BEFORE this is
# enabled, otherwise the widened query cannot use the index and seq-scans the table.
GEO_SERVICE_LOCATIONS_ENV = "HLTHPRT_GEO_INCLUDE_SERVICE_LOCATIONS"
# Address types that are concrete service locations (the geo_idx + detail surface).
GEO_SERVICE_LOCATION_TYPES = ("primary", "secondary", "practice", "site")


def _should_include_geo_service_locations() -> bool:
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


def _render_public_formatted_address(value: dict[str, Any]) -> None:
    component_keys = (
        "first_line",
        "second_line",
        "city_name",
        "state_name",
        "postal_code",
    )
    if "formatted_address" not in value and not any(
        key in value for key in component_keys
    ):
        return
    value["formatted_address"] = render_formatted_address_v2(
        value.get("first_line"),
        value.get("second_line"),
        value.get("city_name"),
        value.get("state_name"),
        value.get("postal_code"),
        value.get("country_code"),
    )
    if "formatted_address_version" in value:
        value["formatted_address_version"] = ADDRESS_FORMAT_VERSION
    if "formatted_address_source" in value:
        value["formatted_address_source"] = ADDRESS_FORMAT_SOURCE


def _redact_internal_address_fields(value: Any) -> Any:
    if isinstance(value, dict):
        _render_public_formatted_address(value)
        _attach_public_address_site_key(value, value)
        for key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
            value.pop(key, None)
        for child in value.values():
            _redact_internal_address_fields(child)
    elif isinstance(value, list):
        for child in value:
            _redact_internal_address_fields(child)
    return value


def _is_environment_flag_enabled(*names: str, default: bool = False) -> bool:
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


def _is_truthy_arg(raw_value: Any, *, default: bool) -> bool:
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


def _provider_card_taxonomy_code(taxonomy_entry: Mapping[str, Any]) -> Any:
    return taxonomy_entry.get("taxonomy_code") or taxonomy_entry.get(
        "healthcare_provider_taxonomy_code"
    )


def _provider_card_taxonomy_display(taxonomy_entry: Mapping[str, Any]) -> Any:
    return taxonomy_entry.get("display") or taxonomy_entry.get("display_name")


def _provider_card_primary_taxonomy_display(
    taxonomy_entry: Mapping[str, Any],
) -> Any:
    nested_taxonomy = taxonomy_entry.get("nucc_taxonomy")
    nested_display = (
        nested_taxonomy.get("display_name")
        if isinstance(nested_taxonomy, Mapping)
        else None
    )
    return (
        _provider_card_taxonomy_display(taxonomy_entry)
        or nested_display
    )


def _provider_card_primary_specialty(taxonomy_list: Any) -> dict[str, Any]:
    """Return the compact primary-specialty projection for one provider."""

    taxonomy_entries = [
        dict(taxonomy_entry)
        for taxonomy_entry in (taxonomy_list or [])
        if isinstance(taxonomy_entry, Mapping)
    ]
    primary_taxonomy = next(
        (
            taxonomy_entry
            for taxonomy_entry in taxonomy_entries
            if taxonomy_entry.get("primary") is True
            or str(
                taxonomy_entry.get("healthcare_provider_primary_taxonomy_switch")
                or ""
            ).upper()
            == "Y"
        ),
        taxonomy_entries[0] if taxonomy_entries else {},
    )
    taxonomy_code = _provider_card_taxonomy_code(primary_taxonomy)
    taxonomy_display = _provider_card_primary_taxonomy_display(primary_taxonomy)
    if taxonomy_code and not taxonomy_display:
        taxonomy_display = next(
            (
                _provider_card_taxonomy_display(taxonomy_entry)
                for taxonomy_entry in taxonomy_entries
                if _provider_card_taxonomy_code(taxonomy_entry) == taxonomy_code
                and _provider_card_taxonomy_display(taxonomy_entry)
            ),
            None,
        )
    return {
        "taxonomy_code": taxonomy_code,
        "display": taxonomy_display,
    }


def _provider_card_zip5(raw_postal_code: Any) -> Optional[str]:
    """Return a schema-valid ZIP5 from a ZIP5 or ZIP+4 value."""

    if raw_postal_code in (None, ""):
        return None
    match = re.fullmatch(r"\s*(\d{5})(?:-?\d{4})?\s*", str(raw_postal_code))
    return match.group(1) if match else None


def _provider_card_from_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Project one provider result into the compact doctor-search card shape."""

    entity_type_code = mapping.get("entity_type_code")
    try:
        entity_type_code = int(entity_type_code)
    except (TypeError, ValueError):
        entity_type_code = None
    postal_code = mapping.get("zip5") or mapping.get("postal_code")
    provider_card_by_field = {
        "npi": mapping.get("npi") or mapping.get("npi_code"),
        "display_name": _provider_display_name_from_mapping(mapping),
        "entity_type": _entity_kind_from_code(entity_type_code),
        "credential": mapping.get("provider_credential_text"),
        "primary_specialty": _provider_card_primary_specialty(
            mapping.get("taxonomy_list")
        ),
        "city": mapping.get("city") or mapping.get("city_name"),
        "state": (
            mapping.get("state")
            or mapping.get("state_code")
            or mapping.get("state_name")
        ),
        "zip5": _provider_card_zip5(postal_code),
    }
    distance_miles = mapping.get("distance_miles")
    if distance_miles is None:
        distance_miles = mapping.get("distance")
    if distance_miles is not None:
        provider_card_by_field["distance_miles"] = distance_miles
    return provider_card_by_field


ENABLE_NPI_SCHEMA_CACHE = _is_environment_flag_enabled(
    "HLTHPRT_ENABLE_NPI_SCHEMA_CACHE",
    "HLTHPRT_ENABLE_SCHEMA_CACHE",
)
ENABLE_NPI_SEARCH_TAXONOMY_PROJECTION = _is_environment_flag_enabled(
    "HLTHPRT_NPI_SEARCH_TAXONOMY_PROJECTION_ENABLED"
)
_NPI_SEARCH_TAXONOMY_PROJECTION_READY_SQL = """
SELECT EXISTS (
    SELECT 1
      FROM pg_catalog.pg_index AS projection_index
      JOIN pg_catalog.pg_class AS projection_index_relation
        ON projection_index_relation.oid = projection_index.indexrelid
      JOIN pg_catalog.pg_am AS projection_index_method
        ON projection_index_method.oid = projection_index_relation.relam
      JOIN pg_catalog.pg_attribute AS projection_column
        ON projection_column.attrelid = projection_index.indrelid
       AND projection_column.attname = 'search_taxonomy_codes'
       AND NOT projection_column.attisdropped
      JOIN mrf.npi_canonical_publication_receipt AS publication_receipt
        ON publication_receipt.npi_table_oid = projection_index.indrelid
      JOIN mrf.npi_canonical_publication_receipt_seal AS publication_seal
        USING (publication_ref)
     WHERE projection_index.indrelid = 'mrf.npi'::regclass
       AND projection_index_relation.relname = 'npi_idx_search_taxonomy_codes'
       AND projection_index_method.amname = 'gin'
       AND projection_index.indisvalid
       AND projection_index.indisready
       AND projection_index.indnkeyatts = 1
       AND projection_index.indexprs IS NULL
       AND projection_index.indpred IS NULL
       AND projection_index.indkey[0] = projection_column.attnum
       AND projection_column.attnotnull
       AND pg_catalog.format_type(
               projection_column.atttypid,
               projection_column.atttypmod
           ) = 'character varying[]'
);
"""


async def _assert_npi_search_taxonomy_projection_ready() -> None:
    """Fail startup when an enabled projection is not sealed and indexed."""

    if not ENABLE_NPI_SEARCH_TAXONOMY_PROJECTION:
        return
    if await db.scalar(text(_NPI_SEARCH_TAXONOMY_PROJECTION_READY_SQL)) is not True:
        raise RuntimeError("npi_search_taxonomy_projection_not_ready")


@blueprint.listener("before_server_start")
async def _assert_npi_projection_before_start(_app, _loop):
    await _assert_npi_search_taxonomy_projection_ready()


_NPI_SCHEMA_CACHE_TTL_SECONDS = 300.0
_TABLE_EXISTS_CACHE: dict[str, tuple[float, bool]] = {}
_TABLE_COLUMNS_CACHE: dict[str, tuple[float, set[str]]] = {}
_NPI_FILTER_CAPABILITIES_CACHE_STATE: dict[
    str, Optional[tuple[float, str, dict[str, bool]]]
] = {"entry": None}
_NPI_PRIMARY_TOTAL_CACHE_STATE: dict[
    str, Optional[tuple[float, str, int]]
] = {
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


def _provider_taxonomy_lateral_join(
    address_alias: str = "c",
    taxonomy_alias: str = "q",
    code_placeholders: Sequence[str] = (),
    provider_npi_sql: str | None = None,
) -> str:
    """Return an NPI-first taxonomy probe for selective provider searches."""

    taxonomy_code_predicate = (
        "provider_taxonomy.healthcare_provider_taxonomy_code "
        f"IN ({', '.join(code_placeholders)})"
        if code_placeholders
        else "provider_taxonomy.healthcare_provider_taxonomy_code "
        f"= ANY({taxonomy_alias}.codes)"
    )
    provider_npi = provider_npi_sql or f"{address_alias}.npi"
    return f"""
    JOIN LATERAL (
        SELECT 1
          FROM mrf.npi_taxonomy AS provider_taxonomy
         WHERE provider_taxonomy.npi = {provider_npi}
           AND {taxonomy_code_predicate}
         LIMIT 1
    ) AS provider_taxonomy_match ON TRUE
    """


def _provider_taxonomy_code_parameters(
    taxonomy_codes: Sequence[str],
    parameter_prefix: str,
) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return scalar taxonomy parameters that keep composite indexes usable."""

    parameters_by_name = {
        f"{parameter_prefix}_{index}": str(taxonomy_code).upper()
        for index, taxonomy_code in enumerate(taxonomy_codes)
    }
    placeholders = tuple(
        f":{parameter_name}" for parameter_name in parameters_by_name
    )
    return parameters_by_name, placeholders


def _provider_taxonomy_matched_npi_cte(
    taxonomy_conditions: str,
    *,
    code_placeholders: Sequence[str] = (),
    npi_where: str = "",
    npi_projection: str = "b.npi",
) -> str:
    """Materialize the smaller side of a provider/taxonomy intersection."""

    if code_placeholders:
        if npi_where:
            if not ENABLE_NPI_SEARCH_TAXONOMY_PROJECTION:
                return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT DISTINCT {npi_projection}
          FROM mrf.npi_taxonomy AS provider_taxonomy
          JOIN mrf.npi AS b
            ON b.npi = provider_taxonomy.npi
         WHERE provider_taxonomy.healthcare_provider_taxonomy_code IN ({', '.join(code_placeholders)})
           AND ({npi_where})
    )
    """
            return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT {npi_projection}
          FROM mrf.npi AS b
         WHERE b.search_taxonomy_codes && ARRAY[{', '.join(code_placeholders)}]::varchar[]
           AND ({npi_where})
    )
    """
        taxonomy_match_sql = (
            "WHERE provider_taxonomy.healthcare_provider_taxonomy_code "
            f"IN ({', '.join(code_placeholders)})"
        )
    else:
        taxonomy_match_sql = f"""
          JOIN (
                SELECT code
                  FROM mrf.nucc_taxonomy
                 WHERE {taxonomy_conditions}
          ) AS matched_taxonomy
            ON matched_taxonomy.code =
               provider_taxonomy.healthcare_provider_taxonomy_code
        """

    return f"""
    taxonomy_matched_npi AS MATERIALIZED (
        SELECT DISTINCT fn.*
          FROM filtered_npi AS fn
          JOIN mrf.npi_taxonomy AS provider_taxonomy
            ON provider_taxonomy.npi = fn.npi
          {taxonomy_match_sql}
    )
    """


def _is_location_first_taxonomy_filter(
    use_taxonomy_filter: bool,
    candidate_filters: Iterable[Any],
) -> bool:
    """Return whether selective candidates should drive taxonomy probes."""

    return use_taxonomy_filter and any(
        value not in (None, "", (), [], {})
        for value in candidate_filters
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


async def _classification_npi_rows(query, taxonomy_codes, session):
    """Read taxonomy-matched NPI rows through either supported DB seam."""

    if session is not None:
        query_result = await session.execute(
            query,
            {"taxonomy_codes": taxonomy_codes},
        )
        return query_result.all()
    async with db.acquire() as connection:
        return await connection.all(
            query,
            taxonomy_codes=taxonomy_codes,
        )


def _classification_npi_values(taxonomy_npi_rows) -> list[int]:
    """Normalize mapping and positional query rows into integer NPIs."""

    npi_values: list[int] = []
    for taxonomy_npi_row in taxonomy_npi_rows:
        mapping = getattr(taxonomy_npi_row, "_mapping", None)
        npi_value = (
            mapping.get("npi")
            if mapping is not None
            else (taxonomy_npi_row[0] if taxonomy_npi_row else None)
        )
        if npi_value is None:
            continue
        try:
            npi_values.append(int(npi_value))
        except (TypeError, ValueError):
            continue
    return npi_values


async def _get_classification_npi_list(classification: str, *, session=None) -> list[int]:
    """Return the publication-scoped NPI list for one taxonomy classification."""

    classification_key = str(classification or "").strip().lower()
    if not classification_key:
        return []
    publication_identity = await _npi_canonical_publication_identity(
        session=session
    )
    cache_key = (
        f"{publication_identity}|{classification_key}"
        if publication_identity is not None
        else None
    )
    now = time.time()
    cached = _CLASSIFICATION_NPI_CACHE.get(cache_key) if cache_key else None
    if cached and (now - cached[0]) < _CLASSIFICATION_NPI_CACHE_TTL_SECONDS:
        return list(cached[1])

    taxonomy_codes = await _get_taxonomy_codes_for_classification(classification, session=session)
    if not taxonomy_codes:
        return []

    schema = _runtime_db_schema()
    query = text(
        f"""
        SELECT DISTINCT t.npi
          FROM {schema}.npi_taxonomy AS t
         WHERE t.healthcare_provider_taxonomy_code = ANY(:taxonomy_codes)
         ORDER BY t.npi
        """
    )
    taxonomy_npi_rows = await _classification_npi_rows(
        query,
        taxonomy_codes,
        session,
    )
    npi_list = _classification_npi_values(taxonomy_npi_rows)

    if cache_key is not None:
        _set_limited_classification_cache(
            _CLASSIFICATION_NPI_CACHE,
            cache_key,
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


def _npi_serving_columns() -> tuple[Any, ...]:
    """Return columns available before taxonomy-projection activation."""

    return tuple(
        column
        for column in NPIData.__table__.columns
        if column.key != "search_taxonomy_codes"
    )


_DB_SCHEMA_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)


def _runtime_db_schema() -> str:
    """Resolve the schema shared by runtime queries and Alembic."""

    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("npi_database_schema_configuration_conflicts")
    schema = runtime_schema or legacy_schema or "mrf"
    if _DB_SCHEMA_RE.fullmatch(schema) is None:
        raise RuntimeError("npi_database_schema_invalid")
    return schema


def _schema_cache_key(table_name: str) -> str:
    return f"{_runtime_db_schema()}.{table_name}"


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
    if schema_key != _runtime_db_schema():
        _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = None
        return None
    return dict(value)


def _filter_cache_set(value: dict[str, bool]) -> dict[str, bool]:
    if ENABLE_NPI_SCHEMA_CACHE:
        _NPI_FILTER_CAPABILITIES_CACHE_STATE["entry"] = (
            time.monotonic(),
            _runtime_db_schema(),
            dict(value),
        )
    return value


def _primary_total_cache_get(publication_identity: str) -> Optional[int]:
    if not ENABLE_NPI_SCHEMA_CACHE:
        return None
    cache_entry = _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"]
    if cache_entry is None:
        return None
    cached_at, cached_identity, value = cache_entry
    if (time.monotonic() - cached_at) > _NPI_SCHEMA_CACHE_TTL_SECONDS:
        _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = None
        return None
    if cached_identity != publication_identity:
        _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = None
        return None
    return int(value)


def _primary_total_cache_set(publication_identity: str, value: int) -> int:
    if ENABLE_NPI_SCHEMA_CACHE:
        _NPI_PRIMARY_TOTAL_CACHE_STATE["entry"] = (
            time.monotonic(),
            publication_identity,
            int(value),
        )
    return int(value)


MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_KEYS = 256
MAX_PROVIDER_DIRECTORY_PLANS_PER_ROLE = 100
MAX_PROVIDER_DIRECTORY_ROLE_EVIDENCE_ROWS = 8192
MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS = 32
MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_VALUES = 32
MAX_PROVIDER_DIRECTORY_FHIR_PROVENANCE_TEXT_LENGTH = 2048
_PROVIDER_DIRECTORY_ROLE_JIT_DISABLED_ATTR = "_healthporta_provider_directory_role_jit_disabled"


def _has_insurance_total_cache_key(
    publication_identity: str,
    city: Optional[str],
    state: Optional[str],
) -> str:
    city_key = (city or "").strip().upper()
    state_key = (state or "").strip().upper()
    return f"{publication_identity}|{city_key}|{state_key}"


def _has_insurance_total_cache_get(
    publication_identity: str,
    city: Optional[str],
    state: Optional[str],
) -> Optional[int]:
    cached = _cache_get(
        _NPI_HAS_INSURANCE_TOTAL_CACHE,
        _has_insurance_total_cache_key(publication_identity, city, state),
    )
    if cached is None:
        return None
    return int(cached)


def _has_insurance_total_cache_set(
    publication_identity: str,
    city: Optional[str],
    state: Optional[str],
    value: int,
) -> int:
    return int(
        _cache_set(
            _NPI_HAS_INSURANCE_TOTAL_CACHE,
            _has_insurance_total_cache_key(publication_identity, city, state),
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
NPI_BATCH_MAX_SIZE = 100
NPI_BATCH_ADDRESS_DEFAULT_LIMIT = 5
NPI_BATCH_ADDRESS_MAX_LIMIT = 20
NPI_DETAIL_ADDRESS_GROUP_DEFAULT_LIMIT = 5
NPI_DETAIL_ADDRESS_GROUP_MAX_LIMIT = 5
NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT = 5
NPI_SEARCH_ADDRESS_DEFAULT_LIMIT = 3
ADDRESS_GROUPING_FLAT = "flat"
ADDRESS_GROUPING_PREMISE = "premise"
ADDRESS_GROUPING_VALUES = {ADDRESS_GROUPING_FLAT, ADDRESS_GROUPING_PREMISE}


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
    profile_serving_identity: str | None = None,
    address_overlay_serving_identity: str | None = None,
    canonical_publication_identity: str | None = None,
    address_limit: int | None = None,
    address_offset: int = 0,
    include_address_total: bool = True,
    address_key: str | None = None,
    address_site_key: str | None = None,
    address_grouping: str = ADDRESS_GROUPING_FLAT,
) -> str:
    schema = _runtime_db_schema()
    address_source = os.getenv(ADDRESS_SERVING_SOURCE_ENV, ADDRESS_SERVING_SOURCE_UNIFIED).strip().lower()
    return (
        f"{schema}|{address_source}|{int(npi)}|{view}|"
        f"{'chain' if include_chain else 'default'}|"
        f"extra:{int(extra_info)}|"
        f"{'sync_geo' if sync_geocode else 'stored_geo'}|"
        f"{'archive_geo' if lookup_stored_geocode else 'no_archive_geo'}|"
        f"sources:{int(include_sources)}|evidence:{int(include_evidence)}|"
        f"profile:{int(include_profile)}|pgen:{profile_generation or 'none'}|"
        f"pserve:{profile_serving_identity or 'unknown'}|"
        f"pdaddr:{address_overlay_serving_identity or 'unknown'}|"
        f"npipub:{canonical_publication_identity or 'untracked'}|"
        f"alim:{address_limit if address_limit is not None else 'all'}|"
        f"aoff:{int(address_offset or 0)}|atotal:{int(include_address_total)}|"
        f"akey:{address_key or 'none'}|"
        f"askey:{address_site_key or 'none'}|agroup:{address_grouping}"
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


def _is_npi_detail_response_cacheable(
    data: dict[str, Any],
    *,
    force_address_update: bool,
    sync_geocode: bool,
) -> bool:
    if force_address_update:
        return False
    if not sync_geocode:
        return True
    address_list = list(data.get("address_list") or [])
    for group in data.get("address_groups") or []:
        if isinstance(group, Mapping):
            address_list.extend(group.get("members") or [])
    for address in address_list:
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


def _json_merge_identity_marker(value: Any) -> str:
    if type(value) is str:
        return json.encoder.encode_basestring_ascii(value)
    if type(value) is bool:
        return "true" if value else "false"
    if type(value) is int:
        return str(value)
    return json.dumps(value, sort_keys=True, default=str)


def _merge_unique_list_values(first: Any, second: Any) -> list[Any]:
    merged_values: list[Any] = []
    seen_markers: set[str] = set()
    for values in (first, second):
        if values is None:
            continue
        candidates = values if isinstance(values, list) else [values]
        for value in candidates:
            if value in (None, ""):
                continue
            marker = _json_merge_identity_marker(value)
            if marker in seen_markers:
                continue
            seen_markers.add(marker)
            merged_values.append(value)
    return merged_values


def _merge_address_lists(base: dict[str, Any], duplicate: Mapping[str, Any]) -> None:
    for key in (
        "address_sources",
        "source_record_ids",
        PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY,
        "_base_row_identities",
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


def _fill_address_scalars(base: dict[str, Any], duplicate: Mapping[str, Any]) -> None:
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


HYDRATED_IDENTITY_SCALAR_FIELDS = (
    "archive_identity_version",
    "base_address_version",
    "confidence_score",
    "entity_id",
    "entity_name",
    "entity_subtype",
    "entity_type",
    "freshness_score",
    "inference_confidence",
    "inference_method",
    "location_confidence_id",
    "location_key",
    "row_origin",
    "source_mask",
    "address_source_mask",
    "city_norm",
    "county_fips",
    "state_code",
    "zip5",
    "inferred_npi",
)


def _fill_hydrated_identity_scalars(
    location_map: dict[str, Any],
    hydrated_identity_map: Mapping[str, Any],
) -> None:
    """Fill row-specific evidence only from the selected base identity."""
    for field_name in HYDRATED_IDENTITY_SCALAR_FIELDS:
        if (
            location_map.get(field_name) in (None, "")
            and hydrated_identity_map.get(field_name) not in (None, "")
        ):
            location_map[field_name] = hydrated_identity_map.get(field_name)


def _merge_duplicate_address(base: dict[str, Any], duplicate: Mapping[str, Any]) -> None:
    """Merge corroborating rows without crossing canonical location identities."""
    _merge_address_lists(base, duplicate)
    _fill_address_scalars(base, duplicate)
    if (
        duplicate.get("address_status")
        == UHC_PROVIDER_FILE_ADDRESS_STATUS
    ):
        base["address_status"] = UHC_PROVIDER_FILE_ADDRESS_STATUS

    merged_statuses = {
        str(status).strip().lower()
        for status in (base.get("location_status"), duplicate.get("location_status"))
        if status not in (None, "")
    }
    if "active" in merged_statuses:
        base["location_status"] = "active"
    elif "unknown" in merged_statuses:
        base["location_status"] = "unknown"
    elif "inactive" in merged_statuses:
        base["location_status"] = "inactive"

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
        base["multi_source_confirmed"] = bool(
            base.get("multi_source_confirmed")
            or duplicate.get("multi_source_confirmed")
            or base["independent_source_count"] > 1
            or len(merged_sources) > 1
        )


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


def _normalized_address_identity(value: Any) -> str:
    return str(value or "").strip().lower()


def _address_key_and_site_key(address: Mapping[str, Any]) -> tuple[str, str]:
    address_key = _normalized_address_identity(address.get("address_key"))
    site_key = _normalized_address_identity(
        address.get(PUBLIC_ADDRESS_SITE_KEY) or address.get("premise_key")
    )
    if site_key == address_key:
        site_key = ""
    return address_key, site_key


def _base_address_row_identity(address: Mapping[str, Any]) -> str:
    location_key = _normalized_address_identity(address.get("location_key"))
    if location_key:
        return f"location:{location_key}"
    checksum = address.get("checksum")
    if checksum in (None, ""):
        return ""
    npi = address.get("npi")
    address_type = str(address.get("type") or "").strip().lower()
    return f"legacy:{npi}:{address_type}:{checksum}"


def _merge_address_bucket(addresses: Sequence[dict[str, Any]]) -> dict[str, Any]:
    base = addresses[0]
    for duplicate in addresses[1:]:
        _merge_duplicate_address(base, duplicate)
    return base


def _merge_address_key_group(
    address_group: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return one public member for one canonical delivery-point key."""
    address_key = ""
    site_keys: set[str] = set()
    for address in address_group:
        address_key, _site_key = _address_key_and_site_key(address)
        site_keys.update(_address_site_keys(address))
    if len(site_keys) > 1:
        logger.warning(
            "Canonical address_key %s maps to %d conflicting non-null site keys",
            address_key,
            len(site_keys),
        )
    ordered_group = sorted(
        address_group,
        key=lambda address: (
            _address_type_rank(address),
            not bool(_address_key_and_site_key(address)[1]),
            _provider_location_sort_key(address),
        ),
    )
    merged_address = _merge_address_bucket(ordered_group)
    if site_keys:
        merged_address["_address_site_keys"] = sorted(site_keys)
    merged_address["_address_site_key_status"] = (
        "available"
        if len(site_keys) == 1
        else "conflicting"
        if len(site_keys) > 1
        else "missing"
    )
    return [merged_address]


def _dedupe_addresses_by_key(addresses: Sequence[Any]) -> list[dict[str, Any]]:
    """Merge every nonempty canonical address key into one public member."""
    addresses_by_key: dict[str, list[dict[str, Any]]] = {}
    unkeyed_addresses: list[dict[str, Any]] = []
    for address in sorted(
        (entry for entry in addresses if isinstance(entry, dict)),
        key=_provider_location_sort_key,
    ):
        address_key, _site_key = _address_key_and_site_key(address)
        base_row_identities = address.get("_base_row_identities") or []
        if not address_key and base_row_identities:
            address_key = f"base:{base_row_identities[0]}"
        if not address_key:
            unkeyed_addresses.append(address)
            continue
        addresses_by_key.setdefault(address_key, []).append(address)
    deduped_addresses = [
        merged_address
        for address_group in addresses_by_key.values()
        for merged_address in _merge_address_key_group(address_group)
    ]

    return [
        _add_canonical_contact_fields_to_address(address)
        for address in (deduped_addresses + unkeyed_addresses)
    ]


def _address_number(address: Mapping[str, Any], key: str) -> float | None:
    try:
        value = float(address.get(key))
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _parse_address_datetime(raw_value: Any) -> datetime | None:
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, date):
        return datetime.combine(raw_value, datetime.min.time(), tzinfo=UTC)
    if not isinstance(raw_value, str) or not raw_value.strip():
        return None
    try:
        return datetime.fromisoformat(raw_value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None


def _address_freshness_timestamp(address: Mapping[str, Any]) -> float:
    for key in ("updated_at", "last_seen_at", "date_added"):
        parsed_value = _parse_address_datetime(address.get(key))
        if parsed_value is None:
            continue
        if parsed_value.tzinfo is None:
            parsed_value = parsed_value.replace(tzinfo=UTC)
        return parsed_value.timestamp()
    return 0.0


def _provider_location_sort_key(address: Mapping[str, Any]) -> tuple[Any, ...]:
    distance = _address_number(address, "distance_miles")
    status_rank = {"active": 0, "unknown": 1, "inactive": 2}.get(
        str(address.get("location_status") or "unknown").strip().lower(),
        1,
    )
    independent_source_count = int(address.get("independent_source_count") or 0)
    is_multi_source = bool(
        address.get("multi_source_confirmed") or independent_source_count > 1
    )
    has_complete_street = bool(
        address.get("first_line")
        and address.get("city_name")
        and (address.get("state_code") or address.get("state_name"))
    )
    has_coordinates = _address_number(address, "lat") is not None and _address_number(address, "long") is not None
    has_contact = _has_contact_value(address.get("telephone_number") or address.get("phone_number"))
    address_key, site_key = _address_key_and_site_key(address)
    return (
        distance is None,
        distance if distance is not None else 0.0,
        status_rank,
        not is_multi_source,
        not has_complete_street,
        not has_coordinates,
        not has_contact,
        _address_type_rank(address),
        -independent_source_count,
        -_address_freshness_timestamp(address),
        address_key,
        site_key,
        str(address.get("first_line") or ""),
        str(address.get("city_name") or ""),
        str(address.get("state_code") or address.get("state_name") or ""),
        str(address.get("postal_code") or ""),
        tuple(
            sorted(
                str(identity_value)
                for identity_value in (
                    address.get("_base_row_identities") or []
                )
            )
        ),
    )


def _rank_provider_locations(addresses: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(addresses, key=_provider_location_sort_key)


def _address_site_keys(address: Mapping[str, Any]) -> list[str]:
    """Return every known exact premise key for one delivery-point member."""
    raw_site_keys: list[Any] = []
    stored_site_keys = address.get("_address_site_keys")
    if isinstance(stored_site_keys, (list, tuple, set)):
        raw_site_keys.extend(stored_site_keys)
    raw_site_keys.extend(
        (address.get(PUBLIC_ADDRESS_SITE_KEY), address.get("premise_key"))
    )
    return sorted(
        {
            normalized_site_key
            for raw_site_key in raw_site_keys
            if (normalized_site_key := _normalized_address_identity(raw_site_key))
        }
    )


def _is_address_site_key_match(
    address: Mapping[str, Any],
    address_site_key: str,
) -> bool:
    return address_site_key in _address_site_keys(address)


def _premise_group_identity(
    address: Mapping[str, Any],
    singleton_index: int,
) -> tuple[tuple[str, str], dict[str, Any]]:
    """Choose a strict stored-key grouping identity without fuzzy inference."""
    site_keys = _address_site_keys(address)
    address_key = _normalized_address_identity(address.get("address_key"))
    if len(site_keys) == 1:
        site_key = site_keys[0]
        return ("site", site_key), {
            "group_key": site_key,
            "grouping_basis": "address_site_key",
            "address_site_key": site_key,
            "address_site_key_status": "available",
        }
    if address_key:
        return ("address", address_key), {
            "group_key": address_key,
            "grouping_basis": "address_key_fallback",
            "address_site_key": None,
            "address_site_key_status": (
                "conflicting" if len(site_keys) > 1 else "missing"
            ),
        }
    singleton_key = f"singleton:{singleton_index}"
    return ("singleton", singleton_key), {
        "group_key": None,
        "grouping_basis": "singleton",
        "address_site_key": None,
        "address_site_key_status": (
            "conflicting" if len(site_keys) > 1 else "missing"
        ),
    }


def _group_provider_locations_by_premise(
    addresses: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Group ranked members by exact stored premise keys, preserving all units."""
    group_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    for singleton_index, address in enumerate(addresses):
        group_identity, group_fields = _premise_group_identity(
            address,
            singleton_index,
        )
        group_map = group_by_identity.get(group_identity)
        if group_map is None:
            group_map = {**group_fields, "members": []}
            group_by_identity[group_identity] = group_map
        group_map["members"].append(address)
    return list(group_by_identity.values())


def _member_pagination(total: int, returned: int) -> dict[str, Any]:
    has_more = returned < total
    return {
        "limit": NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT,
        "offset": 0,
        "returned": returned,
        "total": total,
        "has_more": has_more,
        "next_offset": returned if has_more else None,
    }


def _address_group_pagination(
    *,
    limit: int,
    offset: int,
    returned: int,
    total: int,
) -> dict[str, Any]:
    next_offset = offset + returned
    has_more = next_offset < total
    return {
        "limit": limit,
        "offset": offset,
        "returned": returned,
        "total": total,
        "has_more": has_more,
        "next_offset": next_offset if has_more else None,
    }


def _finalize_public_provider_address(
    address: dict[str, Any],
    *,
    include_sources: bool,
    include_evidence: bool,
    suppress_conflicting_site_key: bool = False,
) -> dict[str, Any]:
    """Remove serving internals while preserving the full public member row."""
    _render_public_formatted_address(address)
    if (
        suppress_conflicting_site_key
        and address.get("_address_site_key_status") == "conflicting"
    ):
        address.pop("premise_key", None)
        address.pop(PUBLIC_ADDRESS_SITE_KEY, None)
    else:
        _attach_public_address_site_key(address, address)
    for key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
        address.pop(key, None)
    address.pop("_base_row_identities", None)
    if not (include_sources or include_evidence):
        address.pop("location_key", None)
    if not include_evidence:
        address.pop("source_record_ids", None)
    return address


def _directory_source_ids(record_ids: Any) -> list[str]:
    if not record_ids:
        return []
    candidates = record_ids if isinstance(record_ids, (list, tuple, set)) else [record_ids]
    source_ids: list[str] = []
    seen_source_ids: set[str] = set()
    for raw in candidates:
        value = str(raw or "").strip()
        parts = value.split(":")
        if len(parts) < 3 or parts[0] != "provider_directory_fhir":
            continue
        source_id = parts[2].strip()
        if not source_id or source_id in seen_source_ids:
            continue
        seen_source_ids.add(source_id)
        source_ids.append(source_id)
    return source_ids


def _provider_directory_record_ids_from_address(address: Mapping[str, Any]) -> list[Any]:
    return _merge_unique_list_values(
        address.get("source_record_ids"),
        address.get("phone_source_record_ids"),
    )


def _provider_directory_source_ids_from_addresses(addresses: Sequence[Any]) -> list[str]:
    source_ids: list[str] = []
    seen_source_ids: set[str] = set()
    for address in addresses or []:
        if not isinstance(address, Mapping):
            continue
        for source_id in _directory_source_ids(
            _provider_directory_record_ids_from_address(address)
        ):
            if source_id in seen_source_ids:
                continue
            seen_source_ids.add(source_id)
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
    return profile_artifact.fhir_reference_resource_id_sql(reference, resource_type)


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


_PROVIDER_DIRECTORY_REQUESTED_ROLE_CTES_TEMPLATE = """
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
                          LIMIT {role_reference_limit}
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
                          LIMIT {role_reference_limit}
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


def _provider_directory_requested_role_ctes_sql(_schema: str) -> str:
    """Build CTEs that constrain provider-directory roles requested by callers."""
    return _PROVIDER_DIRECTORY_REQUESTED_ROLE_CTES_TEMPLATE.format(
        plan_id=_provider_directory_reference_resource_id_sql(
            "plan_ref.value", "InsurancePlan"
        ),
        service_id=_provider_directory_reference_resource_id_sql(
            "service_ref.value", "HealthcareService"
        ),
        endpoint_id=_provider_directory_reference_resource_id_sql(
            "endpoint_ref.value", "Endpoint"
        ),
        insurance_plan_active=_insurance_plan_active_sql("insurance_plan"),
        role_reference_limit=MAX_PROVIDER_DIRECTORY_ROLE_REFERENCE_DETAILS,
    )


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
    schema = _runtime_db_schema()
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
                "schema": _runtime_db_schema(),
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
    schema = _runtime_db_schema()
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
    if not await _is_table_available(ProviderDirectorySource.__tablename__, session=session):
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
        address_source_ids = _directory_source_ids(
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


def _public_mrf_source_url(value: Any) -> str | None:
    """Return a credential-free public HTTP source identity."""
    source_url = _provider_directory_fhir_url_identity(value)
    if not source_url:
        return None
    parsed = urllib.parse.urlsplit(source_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    hostname = parsed.hostname.lower()
    normalized_hostname = hostname.rstrip(".")
    if normalized_hostname == "localhost" or normalized_hostname.endswith(
        ".localhost"
    ):
        return None
    try:
        literal_ip = ipaddress.ip_address(normalized_hostname)
    except ValueError:
        return source_url
    if not literal_ip.is_global:
        return None
    return source_url


def _mrf_source_address_pairs(
    addresses: Sequence[Any],
) -> list[tuple[int, str]]:
    """Return exact selected MRF-backed address identities."""
    pairs: set[tuple[int, str]] = set()
    for address in addresses:
        if not isinstance(address, Mapping):
            continue
        if "mrf" not in _json_array_value(address.get("address_sources")):
            continue
        address_key = _normalized_address_identity(address.get("address_key"))
        npi_value = address.get("npi") or address.get("inferred_npi")
        if not address_key or npi_value in (None, ""):
            continue
        try:
            pairs.add((int(npi_value), address_key))
        except (TypeError, ValueError):
            continue
    return sorted(pairs)


_MRF_SOURCE_DETAILS_QUERY = """
    WITH selected(npi, address_key) AS (
        SELECT selected_npi, selected_address_key
          FROM UNNEST(
                   CAST(:npis AS bigint[]),
                   CAST(:address_keys AS uuid[])
               ) AS selected_rows(selected_npi, selected_address_key)
    )
    SELECT evidence.npi,
           evidence.address_key,
           MIN(BTRIM(evidence.issuer_name)) AS issuer_name,
           COALESCE(
               ARRAY_AGG(DISTINCT evidence.issuer_id ORDER BY evidence.issuer_id)
                   FILTER (WHERE evidence.issuer_id IS NOT NULL),
               ARRAY[]::integer[]
           ) AS issuer_ids,
           COALESCE(
               ARRAY_AGG(
                   DISTINCT BTRIM(evidence.source_url)
                   ORDER BY BTRIM(evidence.source_url)
               ) FILTER (
                   WHERE NULLIF(BTRIM(evidence.source_url), '') IS NOT NULL
               ),
               ARRAY[]::varchar[]
           ) AS source_urls
      FROM selected
      JOIN {evidence_table} AS evidence
        ON evidence.npi = selected.npi
       AND evidence.address_key = selected.address_key
     WHERE NULLIF(BTRIM(evidence.issuer_name), '') IS NOT NULL
  GROUP BY evidence.npi,
           evidence.address_key,
           LOWER(BTRIM(evidence.issuer_name))
  ORDER BY evidence.npi,
           evidence.address_key,
           LOWER(BTRIM(evidence.issuer_name))
"""


def _mrf_source_pair(source_row: Mapping[str, Any]) -> tuple[int, str] | None:
    """Return one valid evidence address identity."""
    address_key = _normalized_address_identity(source_row.get("address_key"))
    if not address_key:
        return None
    npi_value = source_row.get("npi") or source_row.get("inferred_npi")
    try:
        return int(npi_value), address_key
    except (TypeError, ValueError):
        return None


def _mrf_source_detail(source_row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Build one public issuer-level MRF source detail."""
    issuer_name = str(source_row.get("issuer_name") or "").strip()
    if not issuer_name:
        return None
    issuer_ids: list[int] = []
    for raw_issuer_id in source_row.get("issuer_ids") or []:
        try:
            issuer_ids.append(int(raw_issuer_id))
        except (TypeError, ValueError):
            continue
    issuer_ids = sorted(set(issuer_ids))
    if len(issuer_ids) == 1:
        source_name = f"{issuer_name} (issuer {issuer_ids[0]})"
    elif issuer_ids:
        source_name = f"{issuer_name} (issuers {', '.join(map(str, issuer_ids))})"
    else:
        source_name = issuer_name
    return {
        "source": "mrf",
        "issuer_name": issuer_name,
        "source_name": source_name,
        "issuer_ids": issuer_ids,
        "source_urls": sorted(
            {
                public_url
                for source_url in (source_row.get("source_urls") or [])
                if (public_url := _public_mrf_source_url(source_url))
            }
        ),
    }


def _mrf_source_details_by_pair(
    source_rows: Sequence[Any],
    selected_pairs: set[tuple[int, str]],
) -> dict[tuple[int, str], list[dict[str, Any]]]:
    """Group valid source details by exact selected address."""
    details_by_pair: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for result_row in source_rows:
        source_row = getattr(result_row, "_mapping", result_row)
        source_pair = _mrf_source_pair(source_row)
        if source_pair not in selected_pairs:
            continue
        source_detail = _mrf_source_detail(source_row)
        if source_detail:
            details_by_pair[source_pair].append(source_detail)
    return details_by_pair


def _apply_mrf_source_details(
    addresses: Sequence[Any],
    details_by_pair: Mapping[tuple[int, str], list[dict[str, Any]]],
) -> None:
    """Attach grouped source details to their exact address rows."""
    for address in addresses:
        if not isinstance(address, dict):
            continue
        source_pair = _mrf_source_pair(address)
        source_details = details_by_pair.get(source_pair) if source_pair else None
        if not source_details:
            continue
        sorted_source_details = sorted(
            source_details,
            key=lambda source: (
                str(source["source_name"]).casefold(),
                tuple(source["issuer_ids"]),
            ),
        )
        address[MRF_SOURCE_DETAIL_KEY] = sorted_source_details
        address[MRF_SOURCE_COUNT_KEY] = len(sorted_source_details)


async def _attach_mrf_source_details(
    addresses: Sequence[Any],
    *,
    session: Any = None,
) -> None:
    """Attach issuer-level MRF provenance to exact selected addresses."""
    address_pairs = _mrf_source_address_pairs(addresses)
    if not address_pairs:
        return
    if not await _is_table_available("mrf_address_evidence", session=session):
        return
    evidence_table = _schema_cache_key("mrf_address_evidence")
    query_result = await _execute_stmt(
        text(_MRF_SOURCE_DETAILS_QUERY.format(evidence_table=evidence_table)),
        session=session,
        params={
            "npis": [npi for npi, _address_key in address_pairs],
            "address_keys": [address_key for _npi, address_key in address_pairs],
        },
    )
    details_by_pair = _mrf_source_details_by_pair(query_result.all(), set(address_pairs))
    _apply_mrf_source_details(addresses, details_by_pair)


async def _attach_selected_address_source_details(
    addresses: Sequence[Any],
    *,
    include_sources: bool,
    include_role_evidence: bool = False,
    session: Any = None,
) -> None:
    """Attach opt-in source details through one shared response path."""
    await _attach_provider_directory_source_details(
        addresses,
        include_role_evidence=include_role_evidence,
        session=session,
    )
    if include_sources:
        await _attach_mrf_source_details(addresses, session=session)


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
    seen_values: set[Any] = set()
    output_values: list[Any] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        if value in seen_values:
            continue
        seen_values.add(value)
        output_values.append(value)
    return output_values


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


def _initialize_ffs_summary_overrides(
    visible_rows_by_npi: Mapping[int, Sequence[Mapping[str, Any]]],
) -> tuple[dict[int, dict[str, Any]], dict[str, int], list[str]]:
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
    return (
        summary_overrides_by_npi,
        npi_by_enrollment,
        _unique_non_empty(all_enrollment_ids),
    )


async def _apply_ffs_related_npi_overrides(
    summary_overrides_by_npi: dict[int, dict[str, Any]],
    npi_by_enrollment: Mapping[str, int],
    enrollment_ids: Sequence[str],
    *,
    session: Any = None,
) -> None:
    if not await _is_table_available(
        ProviderEnrollmentFFSAdditionalNPI.__tablename__,
        session=session,
    ):
        return
    statement = (
        select(
            ProviderEnrollmentFFSAdditionalNPI.enrollment_id,
            ProviderEnrollmentFFSAdditionalNPI.additional_npi,
        )
        .where(ProviderEnrollmentFFSAdditionalNPI.enrollment_id.in_(enrollment_ids))
        .order_by(
            ProviderEnrollmentFFSAdditionalNPI.enrollment_id.asc(),
            ProviderEnrollmentFFSAdditionalNPI.additional_npi.asc(),
        )
    )
    query_result = await _execute_stmt(statement, session=session)
    related_by_npi: dict[int, list[int]] = defaultdict(list)
    for enrollment_id, additional_npi in query_result.all():
        npi_value = npi_by_enrollment.get(str(enrollment_id))
        if additional_npi is not None and npi_value is not None:
            related_by_npi[npi_value].append(int(additional_npi))
    for npi_value, related_npis in related_by_npi.items():
        unique_related_npis = _unique_non_empty(related_npis)
        summary_overrides_by_npi[npi_value]["ffs_related_npis"] = unique_related_npis
        summary_overrides_by_npi[npi_value]["ffs_related_npi_count"] = len(
            unique_related_npis
        )


async def _apply_ffs_address_overrides(
    summary_overrides_by_npi: dict[int, dict[str, Any]],
    npi_by_enrollment: Mapping[str, int],
    enrollment_ids: Sequence[str],
    *,
    session: Any = None,
) -> None:
    if not await _is_table_available(
        ProviderEnrollmentFFSAddress.__tablename__,
        session=session,
    ):
        return
    statement = (
        select(
            ProviderEnrollmentFFSAddress.enrollment_id,
            ProviderEnrollmentFFSAddress.zip_code,
            ProviderEnrollmentFFSAddress.city,
            ProviderEnrollmentFFSAddress.state,
        )
        .where(ProviderEnrollmentFFSAddress.enrollment_id.in_(enrollment_ids))
        .order_by(
            ProviderEnrollmentFFSAddress.enrollment_id.asc(),
            ProviderEnrollmentFFSAddress.state.asc().nullslast(),
            ProviderEnrollmentFFSAddress.city.asc().nullslast(),
            ProviderEnrollmentFFSAddress.zip_code.asc().nullslast(),
        )
    )
    query_result = await _execute_stmt(statement, session=session)
    values_by_field: dict[str, dict[int, list[str]]] = {
        "ffs_practice_zip_codes": defaultdict(list),
        "ffs_practice_cities": defaultdict(list),
        "ffs_practice_states": defaultdict(list),
    }
    for enrollment_id, zip_code, city, state in query_result.all():
        npi_value = npi_by_enrollment.get(str(enrollment_id))
        if npi_value is None:
            continue
        for field_name, field_value in zip(
            values_by_field,
            (zip_code, city, state),
        ):
            if field_value:
                values_by_field[field_name][npi_value].append(str(field_value))
    for npi_value, summary in summary_overrides_by_npi.items():
        for field_name, values_by_npi in values_by_field.items():
            summary[field_name] = _unique_non_empty(values_by_npi.get(npi_value, []))


async def _apply_ffs_specialty_overrides(
    summary_overrides_by_npi: dict[int, dict[str, Any]],
    npi_by_enrollment: Mapping[str, int],
    enrollment_ids: Sequence[str],
    *,
    session: Any = None,
) -> None:
    if not await _is_table_available(
        ProviderEnrollmentFFSSecondarySpecialty.__tablename__,
        session=session,
    ):
        return
    statement = (
        select(
            ProviderEnrollmentFFSSecondarySpecialty.enrollment_id,
            ProviderEnrollmentFFSSecondarySpecialty.provider_type_code,
            ProviderEnrollmentFFSSecondarySpecialty.provider_type_text,
        )
        .where(
            ProviderEnrollmentFFSSecondarySpecialty.enrollment_id.in_(enrollment_ids)
        )
        .order_by(
            ProviderEnrollmentFFSSecondarySpecialty.enrollment_id.asc(),
            ProviderEnrollmentFFSSecondarySpecialty.provider_type_code.asc(),
        )
    )
    query_result = await _execute_stmt(statement, session=session)
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
    for npi_value, summary in summary_overrides_by_npi.items():
        summary["ffs_secondary_provider_type_codes"] = _unique_non_empty(
            codes_by_npi.get(npi_value, [])
        )
        summary["ffs_secondary_provider_type_texts"] = _unique_non_empty(
            texts_by_npi.get(npi_value, [])
        )


async def _fetch_ffs_reassignment_counts(
    enrollment_column: Any,
    enrollment_ids: Sequence[str],
    npi_by_enrollment: Mapping[str, int],
    *,
    session: Any = None,
) -> dict[int, int]:
    statement = (
        select(enrollment_column, func.count().label("row_count"))
        .where(enrollment_column.in_(enrollment_ids))
        .group_by(enrollment_column)
    )
    query_result = await _execute_stmt(statement, session=session)
    counts_by_npi: dict[int, int] = defaultdict(int)
    for enrollment_id, row_count in query_result.all():
        npi_value = npi_by_enrollment.get(str(enrollment_id))
        if npi_value is not None:
            counts_by_npi[npi_value] += int(row_count or 0)
    return dict(counts_by_npi)


async def _apply_ffs_reassignment_overrides(
    summary_overrides_by_npi: dict[int, dict[str, Any]],
    npi_by_enrollment: Mapping[str, int],
    enrollment_ids: Sequence[str],
    *,
    session: Any = None,
) -> None:
    if not await _is_table_available(
        ProviderEnrollmentFFSReassignment.__tablename__,
        session=session,
    ):
        return
    out_counts_by_npi = await _fetch_ffs_reassignment_counts(
        ProviderEnrollmentFFSReassignment.reassigning_enrollment_id,
        enrollment_ids,
        npi_by_enrollment,
        session=session,
    )
    in_counts_by_npi = await _fetch_ffs_reassignment_counts(
        ProviderEnrollmentFFSReassignment.receiving_enrollment_id,
        enrollment_ids,
        npi_by_enrollment,
        session=session,
    )
    for npi_value, summary in summary_overrides_by_npi.items():
        summary["ffs_reassignment_out_count"] = out_counts_by_npi.get(npi_value, 0)
        summary["ffs_reassignment_in_count"] = in_counts_by_npi.get(npi_value, 0)


async def _fetch_ffs_summary_overrides(
    visible_rows_by_npi: dict[int, list[dict[str, Any]]],
    *,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Build per-NPI FFS enrollment summary overrides from detail tables."""
    (
        summary_overrides_by_npi,
        npi_by_enrollment,
        all_enrollment_ids,
    ) = _initialize_ffs_summary_overrides(visible_rows_by_npi)

    if not all_enrollment_ids:
        return summary_overrides_by_npi
    await _apply_ffs_related_npi_overrides(
        summary_overrides_by_npi,
        npi_by_enrollment,
        all_enrollment_ids,
        session=session,
    )
    await _apply_ffs_address_overrides(
        summary_overrides_by_npi,
        npi_by_enrollment,
        all_enrollment_ids,
        session=session,
    )
    await _apply_ffs_specialty_overrides(
        summary_overrides_by_npi,
        npi_by_enrollment,
        all_enrollment_ids,
        session=session,
    )
    await _apply_ffs_reassignment_overrides(
        summary_overrides_by_npi,
        npi_by_enrollment,
        all_enrollment_ids,
        session=session,
    )
    return summary_overrides_by_npi


async def _fast_has_insurance_count(city: Optional[str], state: Optional[str]) -> int:
    required_columns = {"npi", "type", "plans_network_array"}
    if city:
        required_columns.add("city_name")
    if state:
        required_columns.add("state_name")
    address_model = await _address_serving_model(required_columns)
    publication_identity = await _npi_count_cache_identity(address_model)
    has_cached_insurance_count = (
        _has_insurance_total_cache_get(publication_identity, city, state)
        if publication_identity is not None
        else None
    )
    if has_cached_insurance_count is not None:
        return has_cached_insurance_count
    table = address_model.__table__
    provider_npi = table.c.npi
    if address_model is EntityAddressUnified:
        provider_npi = func.coalesce(table.c.npi, table.c.inferred_npi)
        is_serving_type = table.c.type.in_(GEO_SERVICE_LOCATION_TYPES)
    else:
        is_serving_type = table.c.type == "primary"
    conditions = [
        is_serving_type,
        provider_npi.is_not(None),
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
        stmt = select(func.count(func.distinct(provider_npi))).where(*conditions)
    async with db.session() as session:
        count_result = await session.execute(stmt)
        count = int(count_result.scalar() or 0)
        if publication_identity is None:
            return count
        return _has_insurance_total_cache_set(
            publication_identity,
            city,
            state,
            count,
        )


async def _fast_primary_npi_count() -> int:
    address_model = await _address_serving_model({"type"})
    publication_identity = await _npi_count_cache_identity(address_model)
    cached = (
        _primary_total_cache_get(publication_identity)
        if publication_identity is not None
        else None
    )
    if cached is not None:
        return cached
    table = address_model.__table__
    if address_model is EntityAddressUnified:
        provider_npi = func.coalesce(table.c.npi, table.c.inferred_npi)
        stmt = select(func.count(func.distinct(provider_npi))).where(
            table.c.type.in_(GEO_SERVICE_LOCATION_TYPES),
            provider_npi.is_not(None),
        )
    else:
        stmt = select(func.count()).select_from(table).where(table.c.type == "primary")
    scalar_fn = getattr(db, "scalar", None)
    if scalar_fn is not None:
        try:
            scalar_value = await scalar_fn(stmt)
            count = int(scalar_value or 0)
            if publication_identity is None:
                return count
            return _primary_total_cache_set(publication_identity, count)
        except Exception as exc:  # pragma: no cover - fallback for lightweight test doubles
            logger.debug(
                "Primary NPI count scalar path failed; using acquire path (%s)",
                type(exc).__name__,
            )
    async with db.acquire() as conn:
        count_rows = await conn.all(stmt)
    fallback_value = count_rows[0][0] if count_rows else 0
    count = int(fallback_value or 0)
    if publication_identity is None:
        return count
    return _primary_total_cache_set(publication_identity, count)


def _nearby_geo_type_clause(address_table_sql: str) -> str:
    """Return the partial geo-index address-type predicate."""

    if address_table_sql.endswith(".entity_address_unified") and _should_include_geo_service_locations():
        type_list = ", ".join(
            f"'{address_type}'" for address_type in GEO_SERVICE_LOCATION_TYPES
        )
        return f"AND a.type IN ({type_list})"
    return "AND (a.type = 'primary' OR a.type = 'secondary')"


def _build_nearby_sql(
    taxonomy_conditions: str,
    extra_clause: str,
    ilike_clause: str,
    *,
    use_taxonomy_filter: bool,
    address_table_sql: str = "mrf.npi_address",
    geo_precision_clause: str = "",
    cursor_clause: str = "",
) -> str:
    """Build the GiST KNN nearby-provider query for the selected address model.

    Do not add a separate latitude/longitude bounding box here. It makes PostgreSQL
    prefer the B-tree geo_bbox index, which must scan and sort the entire box before
    applying LIMIT. The geography KNN order lets geo_idx stop after the first page.
    Exact total counts use a different access pattern and should remain separate.
    """
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
    geo_type_clause = _nearby_geo_type_clause(address_table_sql)
    row_tiebreaker = (
        "a.location_key ASC"
        if address_table_sql.endswith(".entity_address_unified")
        else "a.type ASC"
    )
    outer_row_tiebreaker = (
        "sub_s.location_key ASC"
        if address_table_sql.endswith(".entity_address_unified")
        else "sub_s.type ASC"
    )
    return dedent(
        """
        WITH sub_s AS (
            SELECT d.npi AS npi_code,
                   ROUND(
                       CAST(
                           ST_Distance(
                               Geography(
                                   ST_MakePoint(
                                       (a.long)::double precision,
                                       (a.lat)::double precision
                                   )
                               ),
                               Geography(
                                   ST_MakePoint(
                                       CAST(:in_long AS double precision),
                                       CAST(:in_lat AS double precision)
                                   )
                               )
                           ) / 1609.34 AS NUMERIC
                       ),
                       2
                   ) AS distance,
                   Geography(
                       ST_MakePoint(
                           (a.long)::double precision,
                           (a.lat)::double precision
                       )
                   ) <-> Geography(
                       ST_MakePoint(
                           CAST(:in_long AS double precision),
                           CAST(:in_lat AS double precision)
                       )
                   ) AS cursor_distance_meters,
                   a.*,
                   d.*
              FROM {address_table_sql} AS a
              JOIN mrf.npi AS d ON d.npi = a.npi{taxonomy_from}
             WHERE ST_DWithin(
                       Geography(
                           ST_MakePoint(
                               (a.long)::double precision,
                               (a.lat)::double precision
                           )
                       ),
                       Geography(
                           ST_MakePoint(
                               CAST(:in_long AS double precision),
                               CAST(:in_lat AS double precision)
                           )
                       ),
                       :radius * 1609.34
                   )
               AND a.lat IS NOT NULL
               AND a.long IS NOT NULL
               AND a.address_key IS NOT NULL
               {taxonomy_where}
               {geo_precision_clause}
               {geo_type_clause}
               {extra_clause}{ilike_clause}{cursor_clause}
          ORDER BY Geography(
                       ST_MakePoint(
                           (a.long)::double precision,
                           (a.lat)::double precision
                       )
                   ) <-> Geography(
                       ST_MakePoint(
                           CAST(:in_long AS double precision),
                           CAST(:in_lat AS double precision)
                       )
                   ) ASC,
                   a.npi ASC,
                   a.address_key ASC,
                   CASE a.type
                       WHEN 'primary' THEN 0
                       WHEN 'practice' THEN 1
                       WHEN 'site' THEN 2
                       WHEN 'secondary' THEN 3
                       ELSE 9
                   END ASC,
                   {row_tiebreaker}
             LIMIT :limit
        )
        SELECT sub_s.*, t.*, nucc.display_name AS taxonomy_display
          FROM sub_s
          LEFT JOIN mrf.npi_taxonomy AS t ON sub_s.npi_code = t.npi
          LEFT JOIN mrf.nucc_taxonomy AS nucc
            ON nucc.code = t.healthcare_provider_taxonomy_code
      ORDER BY sub_s.cursor_distance_meters ASC,
               sub_s.npi_code ASC,
               sub_s.address_key ASC,
               CASE sub_s.type
                   WHEN 'primary' THEN 0
                   WHEN 'practice' THEN 1
                   WHEN 'site' THEN 2
                   WHEN 'secondary' THEN 3
                   ELSE 9
               END ASC,
               {outer_row_tiebreaker};
        """
    ).format(
        taxonomy_from=taxonomy_from,
        taxonomy_where=taxonomy_where,
        geo_precision_clause=geo_precision_clause,
        geo_type_clause=geo_type_clause,
        extra_clause=extra_clause,
        ilike_clause=ilike_clause,
        cursor_clause=cursor_clause,
        row_tiebreaker=row_tiebreaker,
        outer_row_tiebreaker=outer_row_tiebreaker,
        address_table_sql=address_table_sql,
    )


_NEARBY_COUNT_SQL_TEMPLATE = dedent(
    """
    SELECT COUNT(DISTINCT (a.npi, a.address_key)) AS total_count
      FROM {address_table_sql} AS a
      JOIN mrf.npi AS d ON d.npi = a.npi{taxonomy_from}
     WHERE ST_DWithin(
               Geography(
                   ST_MakePoint(
                       (a.long)::double precision,
                       (a.lat)::double precision
                   )
               ),
               Geography(
                   ST_MakePoint(
                       CAST(:in_long AS double precision),
                       CAST(:in_lat AS double precision)
                   )
               ),
               :radius * 1609.34
           )
       AND a.lat IS NOT NULL
       AND a.long IS NOT NULL
       AND a.address_key IS NOT NULL
       {taxonomy_where}
       {geo_precision_clause}
       {geo_type_clause}
       {bbox_clause}
       {extra_clause}{ilike_clause};
    """
)


def _build_nearby_count_sql(
    taxonomy_conditions: str,
    extra_clause: str,
    ilike_clause: str,
    *,
    use_taxonomy_filter: bool,
    address_table_sql: str = "mrf.npi_address",
    geo_precision_clause: str = "",
    bbox_clause: str = "",
) -> str:
    """Build the exact provider-address count query for nearby search."""

    taxonomy_from = ""
    taxonomy_where = ""
    if use_taxonomy_filter:
        taxonomy_from = (
            ",\n"
            "       (\n"
            "           SELECT ARRAY_AGG(int_code) AS codes\n"
            "             FROM mrf.nucc_taxonomy\n"
            f"            WHERE {taxonomy_conditions}\n"
            "       ) AS g"
        )
        taxonomy_where = "\n   AND a.taxonomy_array && g.codes"
    geo_type_clause = _nearby_geo_type_clause(address_table_sql)
    return _NEARBY_COUNT_SQL_TEMPLATE.format(
        taxonomy_from=taxonomy_from,
        taxonomy_where=taxonomy_where,
        geo_precision_clause=geo_precision_clause,
        geo_type_clause=geo_type_clause,
        bbox_clause=bbox_clause,
        extra_clause=extra_clause,
        ilike_clause=ilike_clause,
        address_table_sql=address_table_sql,
    )


_NEARBY_CURSOR_VERSION = 1
_NEARBY_CURSOR_IGNORED_PARAMS = frozenset({"cursor", "include_total", "limit"})


def _nearby_cursor_scope(args: Mapping[str, Any]) -> str:
    values = []
    for key in sorted(str(value) for value in args.keys()):
        if key in _NEARBY_CURSOR_IGNORED_PARAMS:
            continue
        values.append((key, str(args.get(key) or "")))
    serialized = json.dumps(values, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _encode_nearby_cursor(
    scope: str,
    distance_meters: float,
    npi: int,
    address_key: str,
) -> str:
    payload = {
        "v": _NEARBY_CURSOR_VERSION,
        "s": scope,
        "d": float(distance_meters),
        "n": int(npi),
        "a": str(address_key).lower(),
    }
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    )
    return encoded.rstrip(b"=").decode("ascii")


def _decode_nearby_cursor(raw: str, scope: str) -> tuple[float, int, str]:
    try:
        value = str(raw or "").strip()
        if not value or len(value) > 2048:
            raise ValueError("invalid cursor length")
        padding = "=" * (-len(value) % 4)
        payload = json.loads(base64.urlsafe_b64decode(value + padding))
        distance = float(payload["d"])
        npi = int(payload["n"])
        address_key = str(uuid.UUID(str(payload["a"])))
        if payload.get("v") != _NEARBY_CURSOR_VERSION:
            raise ValueError("unsupported cursor version")
        if payload.get("s") != scope:
            raise ValueError("cursor filters do not match this request")
        if not math.isfinite(distance) or distance < 0 or npi <= 0:
            raise ValueError("invalid cursor values")
        return distance, npi, address_key
    except (binascii.Error, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise sanic.exceptions.InvalidUsage(
            "cursor is invalid or does not match the current geo filters"
        ) from exc


def _nearby_cursor_filter(
    cursor: tuple[float, int, str] | None,
) -> tuple[str, dict[str, Any]]:
    """Return the KNN keyset predicate and parameters for one geo batch."""

    if cursor is None:
        return "", {}
    cursor_clause = dedent(
        """

           AND (
               Geography(
                   ST_MakePoint(
                       (a.long)::double precision,
                       (a.lat)::double precision
                   )
               ) <-> Geography(
                   ST_MakePoint(
                       CAST(:in_long AS double precision),
                       CAST(:in_lat AS double precision)
                   )
               ),
               a.npi,
               a.address_key
           ) > (
               CAST(:cursor_distance_meters AS double precision),
               CAST(:cursor_npi AS bigint),
               CAST(:cursor_address_key AS uuid)
           )
        """
    ).rstrip()
    parameters_by_name = {
        "cursor_distance_meters": cursor[0],
        "cursor_npi": cursor[1],
        "cursor_address_key": cursor[2],
    }
    return cursor_clause, parameters_by_name


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


def _name_search_tokens(value: Any) -> tuple[str, ...]:
    """Return stable alphanumeric terms for order-insensitive name matching."""

    return tuple(
        dict.fromkeys(
            token.lower()
            for token in re.findall(r"[^\W_]+", str(value or ""), flags=re.UNICODE)
            if token
        )
    )


def _names_like_filter_clause(alias: str, names: Sequence[str], base_param: str = "name_like") -> tuple[str, dict]:
    if not names:
        return "", {}
    prefix = alias
    if prefix and not prefix.endswith("."):
        prefix = f"{prefix}."
    expr = NAME_LIKE_TEMPLATE.format(alias=prefix)
    indexed_like_expressions = (
        f"LOWER(COALESCE({prefix}provider_first_name, ''))",
        f"LOWER(COALESCE({prefix}provider_last_name, ''))",
        ORGANIZATION_LIKE_TEMPLATE.format(alias=prefix),
    )
    clauses = []
    parameter_map = {}
    for idx, name in enumerate(names):
        tokens = _name_search_tokens(name)
        if not tokens:
            continue
        token_clauses = []
        for token_index, token in enumerate(tokens):
            parameter_suffix = (
                str(idx) if len(tokens) == 1 else f"{idx}_{token_index}"
            )
            param_like = f"{base_param}_{parameter_suffix}"
            indexed_like_clause = " OR ".join(
                f"({field_expression} LIKE :{param_like})"
                for field_expression in indexed_like_expressions
            )
            if ENABLE_TRGM_FUZZY_NAME_SEARCH:
                param_fuzzy = f"{param_like}_fuzzy"
                token_clauses.append(
                    f"(({indexed_like_clause}) OR ({expr} % :{param_fuzzy}))"
                )
                parameter_map[param_fuzzy] = token
            else:
                token_clauses.append(f"({indexed_like_clause})")
            parameter_map[param_like] = f"%{token}%"
        clauses.append(f"({' AND '.join(token_clauses)})")
    if not clauses:
        return "FALSE", {}
    joined = " OR ".join(clauses)
    return f"({joined})", parameter_map


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
    if not re.fullmatch(r"[1-9][0-9]{9}", text_value):
        raise sanic.exceptions.InvalidUsage(
            "npi must be exactly 10 digits and cannot start with zero"
        )
    return int(text_value)


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
    seen_tokens: set[str] = set()
    for item in str(raw).split(","):
        token = item.strip().upper()
        if not token:
            continue
        if not CODE_TOKEN_PATTERN.fullmatch(token):
            raise sanic.exceptions.InvalidUsage(
                f"{param_name} contains invalid code token: {item!r}"
            )
        if token in seen_tokens:
            continue
        seen_tokens.add(token)
        tokens.append(token)
    return tokens


def _to_int_codes(values: Sequence[str], param_name: str) -> list[int]:
    parsed_codes: list[int] = []
    seen_codes: set[int] = set()
    for value in values:
        if not INT_CODE_PATTERN.fullmatch(str(value)):
            raise sanic.exceptions.InvalidUsage(
                f"{param_name} must contain numeric codes for internal matching"
            )
        parsed = int(value)
        if parsed in seen_codes:
            continue
        seen_codes.add(parsed)
        parsed_codes.append(parsed)
    return parsed_codes


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


async def _is_table_available(table_name: str, *, session: Any = None) -> bool:
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

    capability_map["pricing_provider_procedure_available"] = await _is_table_available(
        "pricing_provider_procedure",
        session=session,
    )
    capability_map[
        "pricing_provider_prescription_available"
    ] = await _is_table_available(
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


def _serialize_provider_directory_profile_as_of(
    profile_as_of: date | str | None,
) -> str | None:
    """Return one exact calendar date or fail closed on invalid metadata."""
    if profile_as_of is None:
        return None
    if isinstance(profile_as_of, datetime):
        raise TypeError("profile_as_of must be a date without a time")
    if isinstance(profile_as_of, date):
        return profile_as_of.isoformat()
    if not isinstance(profile_as_of, str):
        raise TypeError("profile_as_of must be an ISO date string, date, or None")
    try:
        return date.fromisoformat(profile_as_of.strip()).isoformat()
    except ValueError as exc:
        raise ValueError("profile_as_of must be an ISO calendar date") from exc


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
    profile_as_of = _serialize_provider_directory_profile_as_of(
        mapping.get("profile_as_of")
    )
    profile["generation_id"] = mapping.get("generation_id")
    profile["published_at"] = published_at
    profile["profile_as_of"] = profile_as_of
    profile_payload_by_kind: dict[str, Any] = {"profile": profile}
    if include_evidence:
        evidence = _provider_directory_profile_json(mapping.get("evidence_json"))
        if evidence is not None:
            evidence["generation_id"] = mapping.get("generation_id")
            evidence["published_at"] = published_at
            evidence["profile_as_of"] = profile_as_of
            profile_payload_by_kind["evidence"] = evidence
    return profile_payload_by_kind


def _provider_directory_profile_serving_identity(
    mapping: Mapping[str, Any],
) -> str:
    """Bind response caching to fallback or validated singleton publication."""
    generation_id = str(mapping.get("generation_id") or "none")
    published_at = (
        _serialize_utc_rfc3339_datetime(mapping.get("published_at")) or "none"
    )
    profile_as_of = (
        _serialize_provider_directory_profile_as_of(
            mapping.get("profile_as_of")
        )
        or "none"
    )
    if mapping.get("serving_generation_key") == "global":
        return (
            f"singleton:{generation_id}:{published_at}:{profile_as_of}:"
            f"{mapping.get('serving_control_generation')}:"
            f"{mapping.get('serving_profile_target_oid')}:"
            f"{mapping.get('serving_evidence_target_oid')}"
        )
    return (
        f"fallback:{generation_id}:{published_at}:{profile_as_of}:"
        f"{mapping.get('materialization_profile_target_oid')}"
    )


def _provider_directory_profiles_by_npi(
    profile_query_result: Any,
    *,
    include_evidence: bool,
) -> dict[int, dict[str, Any]]:
    """Convert one serving-generation query result into public profile payloads."""
    profiles_by_npi: dict[int, dict[str, Any]] = {}
    for profile_query_row in profile_query_result.all():
        mapping = getattr(profile_query_row, "_mapping", profile_query_row)
        profile_payload_by_kind = _provider_directory_profile_payload(
            mapping,
            include_evidence=include_evidence,
        )
        if profile_payload_by_kind is not None:
            profile_payload_by_kind["_serving_identity"] = (
                _provider_directory_profile_serving_identity(mapping)
            )
            profiles_by_npi[int(mapping["npi"])] = profile_payload_by_kind
    return profiles_by_npi


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
    evidence_table_ref = _schema_cache_key(
        profile_artifact.PROFILE_EVIDENCE_TABLE
    )
    serving_generation_ref = _schema_cache_key(
        PROVIDER_DIRECTORY_PROFILE_SERVING_GENERATION_TABLE
    )
    if not await _is_provider_directory_profile_table_available(
        table_ref,
        session=session,
    ):
        return {}
    if not await _is_provider_directory_profile_table_available(
        serving_generation_ref,
        session=session,
    ):
        return {}
    evidence_select = ", profile.evidence_json" if include_evidence else ""
    profile_query = PROVIDER_DIRECTORY_PROFILE_SERVING_QUERY_TEMPLATE.format(
        serving_generation_ref=serving_generation_ref,
        profile_table_ref=table_ref,
        evidence_select=evidence_select,
    )
    profile_query_result = await _execute_stmt(
        text(profile_query),
        session=session,
        params={
            "npis": normalized_npis,
            "profile_table_ref": table_ref,
            "evidence_table_ref": evidence_table_ref,
        },
    )
    return _provider_directory_profiles_by_npi(
        profile_query_result,
        include_evidence=include_evidence,
    )


def _provider_directory_observations_sql(schema: str) -> str:
    """Select the newest bounded, non-certified retained row per resource."""
    statuses = ", ".join(
        f"'{status}'" for status in _PROVIDER_DIRECTORY_OBSERVED_DATASET_STATUSES
    )
    resource_types = ", ".join(
        f"'{resource_type}'"
        for resource_type in _PROVIDER_DIRECTORY_OBSERVED_RESOURCE_TYPES
    )
    return f"""
        WITH observed_datasets AS MATERIALIZED (
            SELECT source.source_id,
                   COALESCE(source.canonical_api_base, source.api_base) AS api_base,
                   dataset.dataset_id,
                   dataset.acquisition_root_run_id,
                   dataset.status AS dataset_status,
                   dataset.created_at AS dataset_created_at
              FROM {schema}.provider_directory_source AS source
              JOIN {schema}.provider_directory_endpoint_dataset AS dataset
                ON dataset.endpoint_id = source.endpoint_id
             WHERE dataset.is_current IS FALSE
               AND dataset.superseded_at IS NULL
               AND dataset.status IN ({statuses})
        ), matching_observations AS MATERIALIZED (
            SELECT observed.source_id, observed.api_base, observed.dataset_id,
                   observed.acquisition_root_run_id, observed.dataset_status,
                   observed.dataset_created_at, resource.resource_type,
                   resource.resource_id, resource.payload_json::jsonb AS payload_json,
                   ROW_NUMBER() OVER (
                       PARTITION BY observed.source_id,
                                    resource.resource_type,
                                    resource.resource_id
                       ORDER BY observed.dataset_created_at DESC NULLS LAST,
                                observed.dataset_id DESC
                   ) AS recency_rank
              FROM observed_datasets AS observed
              JOIN {schema}.provider_directory_dataset_resource AS resource
                ON resource.dataset_id = observed.dataset_id
             WHERE resource.resource_type IN ({resource_types})
               AND resource.payload_json::jsonb ->> 'npi' = :npi
        )
        SELECT source_id, api_base, dataset_id, acquisition_root_run_id,
               dataset_status, dataset_created_at, resource_type, resource_id,
               payload_json
          FROM matching_observations
         WHERE recency_rank = 1
         ORDER BY dataset_created_at DESC NULLS LAST,
                  source_id, resource_type, resource_id
         LIMIT {_PROVIDER_DIRECTORY_OBSERVED_RESOURCE_LIMIT};
    """


async def _fetch_provider_directory_observations(
    npi: int,
    *,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Return retained candidate rows without treating them as published facts."""
    query_result = await _execute_stmt(
        text(_provider_directory_observations_sql(_runtime_db_schema())),
        session=session,
        params={"npi": str(npi)},
    )
    observations: list[dict[str, Any]] = []
    for observation_row in query_result.all():
        mapping = getattr(observation_row, "_mapping", observation_row)
        observation_payload = _provider_directory_profile_json(mapping.get("payload_json"))
        if observation_payload is None:
            continue
        observations.append(
            {
                "source_id": mapping.get("source_id"),
                "api_base": mapping.get("api_base"),
                "dataset_id": mapping.get("dataset_id"),
                "acquisition_root_run_id": mapping.get("acquisition_root_run_id"),
                "dataset_status": mapping.get("dataset_status"),
                "dataset_created_at": mapping.get("dataset_created_at"),
                "resource_type": mapping.get("resource_type"),
                "resource_id": mapping.get("resource_id"),
                "resource": observation_payload,
            }
        )
    return observations


def _is_unified_address_serving_requested() -> bool:
    return os.getenv(ADDRESS_SERVING_SOURCE_ENV, ADDRESS_SERVING_SOURCE_UNIFIED).strip().lower() == ADDRESS_SERVING_SOURCE_UNIFIED


def _is_unified_address_table(address_table_sql: str) -> bool:
    return address_table_sql.endswith(f".{EntityAddressUnified.__tablename__}")


def _address_zip5_filter(alias: str, address_table_sql: str, *, any_array: bool = False) -> str:
    column = f"{alias}.zip5" if _is_unified_address_table(address_table_sql) else f"LEFT({alias}.postal_code, 5)"
    operator = "ANY (:zip_codes)" if any_array else ":zip_code"
    return f"{column} = {operator}"


def _address_phone_digits_filter(alias: str, address_table_sql: str) -> str:
    raw_digits = f"regexp_replace(COALESCE({alias}.telephone_number, ''), '[^0-9]', '', 'g')"
    if _is_unified_address_table(address_table_sql):
        return f"COALESCE(NULLIF({alias}.phone_number, ''), {raw_digits}) = :phone_digits"
    return f"{raw_digits} = :phone_digits"


_CURRENT_PROVIDER_DIRECTORY_PHONE_CTES = """
current_provider_directory_runs AS MATERIALIZED (
    SELECT source.source_id, dataset.dataset_id,
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
           overlay.last_seen_run_id, overlay.source_record_id,
           overlay.resource_type, overlay.resource_id
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
      JOIN mrf.provider_directory_dataset_resource AS dataset_resource
        ON dataset_resource.dataset_id = current_run.dataset_id
       AND dataset_resource.resource_type = overlay.resource_type
       AND dataset_resource.resource_id = overlay.resource_id
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
    if not _is_unified_address_table(address_table_sql):
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
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes},\n" if available_ctes else "WITH "


def _sql_with_ctes(*ctes: str | None) -> str:
    available_ctes = [cte.strip() for cte in ctes if cte and cte.strip()]
    joined_ctes = ",\n".join(available_ctes)
    return f"WITH {joined_ctes}\n" if available_ctes else ""


def _address_npi_filter(alias: str, address_table_sql: str) -> str:
    if _is_unified_address_table(address_table_sql):
        return f"COALESCE({alias}.npi, {alias}.inferred_npi) = :npi_filter"
    return f"{alias}.npi = :npi_filter"


def _address_site_key_filter(alias: str, address_table_sql: str) -> str:
    if _is_unified_address_table(address_table_sql):
        return f"{alias}.premise_key = CAST(:address_site_key AS uuid)"
    return "1=0"


def _provider_list_address_type_clause(
    alias: str,
    address_table_sql: str,
    *,
    include_service_locations: bool,
) -> str:
    if include_service_locations and _is_unified_address_table(address_table_sql):
        type_list = ", ".join(f"'{value}'" for value in GEO_SERVICE_LOCATION_TYPES)
        return f"{alias}.type IN ({type_list})"
    return f"{alias}.type = 'primary'"


def _primary_address_order_clause(alias: str, address_table_sql: str) -> str:
    common = (
        f"{alias}.npi, "
        f"({alias}.lat IS NULL OR {alias}.long IS NULL), "
        f"(NULLIF(TRIM(COALESCE({alias}.first_line, '')), '') IS NULL), "
    )
    if _is_unified_address_table(address_table_sql):
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
    if not _is_unified_address_serving_requested():
        return NPIAddress
    required_columns_set = set(required_columns or ())
    columns = await _table_columns(EntityAddressUnified.__tablename__, session=session)
    if columns and required_columns_set.issubset(columns):
        return EntityAddressUnified
    return NPIAddress


async def _address_serving_table_sql(required_columns: set[str] | None = None, *, session: Any = None) -> str:
    model = await _address_serving_model(required_columns, session=session)
    return _schema_cache_key(model.__tablename__)


PROVIDER_ENRICHMENT_SUMMARY_COLUMNS = (
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
)


async def _provider_enrichment_rows_for_columns(
    npis: Sequence[int],
    available_columns: set[str],
    *,
    session: Any = None,
) -> list[Any]:
    summary_table = _schema_cache_key(ProviderEnrichmentSummary.__tablename__)
    select_columns = [
        column_name if column_name in available_columns else f"NULL AS {column_name}"
        for column_name in PROVIDER_ENRICHMENT_SUMMARY_COLUMNS
    ]
    query = text(
        f"""
        SELECT
            {', '.join(select_columns)}
          FROM {summary_table}
         WHERE npi = ANY(:npis)
        """
    )
    query_result = await _execute_stmt(
        query,
        session=session,
        params={"npis": list(npis)},
    )
    return query_result.all()


async def _fetch_provider_enrichment_summary_rows(
    npis: Sequence[int],
    *,
    session: Any = None,
) -> list[Any]:
    try:
        return await _provider_enrichment_rows_for_columns(
            npis,
            _model_table_columns(ProviderEnrichmentSummary),
            session=session,
        )
    except Exception:
        available_columns = await _table_columns(
            ProviderEnrichmentSummary.__tablename__,
            session=session,
        )
        return await _provider_enrichment_rows_for_columns(
            npis,
            available_columns,
            session=session,
        )


def _provider_enrichment_summary_from_row(summary_row: Sequence[Any]) -> dict[str, Any]:
    return {
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


async def _fetch_provider_enrichment_ffs_rows(
    npis: Sequence[int],
    *,
    session: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    statement = (
        select(
            ProviderEnrollmentFFS.npi,
            ProviderEnrollmentFFS.enrollment_id,
            ProviderEnrollmentFFS.pecos_asct_cntl_id,
            ProviderEnrollmentFFS.provider_type_code,
            ProviderEnrollmentFFS.provider_type_text,
            ProviderEnrollmentFFS.multiple_npi_flag,
        )
        .where(ProviderEnrollmentFFS.npi.in_(npis))
        .order_by(
            ProviderEnrollmentFFS.npi.asc(),
            ProviderEnrollmentFFS.reporting_year.desc().nullslast(),
            ProviderEnrollmentFFS.enrollment_id.asc(),
        )
    )
    query_result = await _execute_stmt(statement, session=session)
    rows_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for enrollment_row in query_result.all():
        rows_by_npi[int(enrollment_row[0])].append(
            {
                "enrollment_id": enrollment_row[1],
                "pecos_asct_cntl_id": enrollment_row[2],
                "provider_type_code": enrollment_row[3],
                "provider_type_text": enrollment_row[4],
                "multiple_npi_flag": enrollment_row[5],
            }
        )
    return dict(rows_by_npi)


def _visible_ffs_rows_by_npi(
    summary_map: Mapping[int, dict[str, Any]],
    ffs_rows_by_npi: Mapping[int, Sequence[dict[str, Any]]],
    *,
    include_chain: bool,
) -> dict[int, list[dict[str, Any]]]:
    visible_rows_by_npi: dict[int, list[dict[str, Any]]] = {}
    for npi_value, summary in summary_map.items():
        visible_rows, chain_rows = _partition_ffs_enrollment_payloads(
            ffs_rows_by_npi.get(npi_value, [])
        )
        summary["ffs_chain_hidden"] = bool(chain_rows) and not include_chain
        summary["ffs_chain_enrollment_count"] = len(chain_rows)
        summary["ffs_chain_enrollment_ids"] = _unique_non_empty(
            [enrollment.get("enrollment_id") for enrollment in chain_rows]
        )
        if chain_rows and not include_chain:
            visible_rows_by_npi[npi_value] = visible_rows
    return visible_rows_by_npi


def _default_ffs_summary_override(
    visible_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return {
        "ffs_enrollment_ids": _unique_non_empty(
            [enrollment.get("enrollment_id") for enrollment in visible_rows]
        ),
        "ffs_pecos_asct_cntl_ids": _unique_non_empty(
            [enrollment.get("pecos_asct_cntl_id") for enrollment in visible_rows]
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


def _apply_provider_enrichment_overrides(
    summary_map: Mapping[int, dict[str, Any]],
    visible_rows_by_npi: Mapping[int, Sequence[Mapping[str, Any]]],
    summary_overrides_by_npi: Mapping[int, Mapping[str, Any]],
) -> None:
    override_fields = (
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
    )
    for npi_value, visible_rows in visible_rows_by_npi.items():
        summary = summary_map.get(npi_value)
        if summary is None:
            continue
        summary_override = summary_overrides_by_npi.get(npi_value)
        if summary_override is None:
            summary_override = _default_ffs_summary_override(visible_rows)
        for field_name in override_fields:
            summary[field_name] = summary_override[field_name]


async def _fetch_provider_enrichment_summary_map(
    npis: Sequence[int],
    *,
    include_chain: bool = False,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Fetch public provider-enrichment summaries keyed by requested NPI."""
    unique_npis = sorted({int(npi) for npi in npis if npi is not None})
    if not unique_npis or not await _is_table_available(
        ProviderEnrichmentSummary.__tablename__,
        session=session,
    ):
        return {}
    summary_rows = await _fetch_provider_enrichment_summary_rows(
        unique_npis,
        session=session,
    )
    summary_map = {
        int(summary_row[0]): _provider_enrichment_summary_from_row(summary_row)
        for summary_row in summary_rows
    }
    if not summary_map or not await _is_table_available(
        ProviderEnrollmentFFS.__tablename__,
        session=session,
    ):
        return summary_map
    ffs_rows_by_npi = await _fetch_provider_enrichment_ffs_rows(
        unique_npis,
        session=session,
    )
    visible_rows_by_npi = _visible_ffs_rows_by_npi(
        summary_map,
        ffs_rows_by_npi,
        include_chain=include_chain,
    )
    if not visible_rows_by_npi:
        return summary_map
    summary_overrides_by_npi = await _fetch_ffs_summary_overrides(
        visible_rows_by_npi,
        session=session,
    )
    _apply_provider_enrichment_overrides(
        summary_map,
        visible_rows_by_npi,
        summary_overrides_by_npi,
    )
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
    public_summary_map = dict(summary)
    public_summary_map.pop("ffs_chain_hidden", None)
    public_summary_map.pop("ffs_chain_enrollment_count", None)
    public_summary_map.pop("ffs_chain_enrollment_ids", None)
    return public_summary_map


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
        if not await _is_table_available(model.__tablename__, session=session):
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

    if not await _is_table_available(ProviderEnrollmentFFS.__tablename__, session=session):
        return enrichment_detail_map

    ffs_rows = enrichment_detail_map["enrollments"]["ffs_public"]
    enrollment_ids = [
        str(enrollment_record.get("enrollment_id"))
        for enrollment_record in ffs_rows
        if enrollment_record.get("enrollment_id")
    ]
    if not enrollment_ids:
        return enrichment_detail_map

    if await _is_table_available(ProviderEnrollmentFFSAdditionalNPI.__tablename__, session=session):
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

    if await _is_table_available(ProviderEnrollmentFFSAddress.__tablename__, session=session):
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

    if await _is_table_available(ProviderEnrollmentFFSSecondarySpecialty.__tablename__, session=session):
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

    if await _is_table_available(ProviderEnrollmentFFSReassignment.__tablename__, session=session):
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
    if include_procedures and await _is_table_available("pricing_provider_procedure", session=session):
        sources.append("SELECT MAX(year)::INTEGER AS y FROM mrf.pricing_provider_procedure")
    if include_medications and await _is_table_available("pricing_provider_prescription", session=session):
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

    if not await _is_table_available("code_crosswalk", session=session):
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
    parameter_map: dict[str, object] = {}

    if names_like:
        name_clause, name_params = _names_like_filter_clause(alias, names_like)
        if name_clause:
            clauses.append(name_clause)
            parameter_map.update(name_params)

    if first_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_first_name, '')) LIKE :first_name")
        parameter_map["first_name"] = f"%{first_name.lower()}%"
    if last_name:
        clauses.append(f"LOWER(COALESCE({prefix}provider_last_name, '')) LIKE :last_name")
        parameter_map["last_name"] = f"%{last_name.lower()}%"
    if organization_name:
        org_expr = ORGANIZATION_LIKE_TEMPLATE.format(alias=prefix)
        clauses.append(f"({org_expr} LIKE :organization_name)")
        parameter_map["organization_name"] = f"%{organization_name.lower()}%"
    if entity_type_code is not None:
        clauses.append(f"{prefix}entity_type_code = :entity_type_code")
        parameter_map["entity_type_code"] = entity_type_code

    if not clauses:
        return "", {}
    return " AND ".join(clauses), parameter_map


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
    normalized_names = []
    seen_names = set()
    for name in names:
        if not name:
            continue
        lower = str(name).lower()
        if lower in seen_names:
            continue
        seen_names.add(lower)
        normalized_names.append(lower)
    return normalized_names


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

    name_clause, name_params = _names_like_filter_clause("d", names)
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
        name_clause, name_query_param_map = _names_like_filter_clause("d", names)
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
    seen_tokens: set[str] = set()
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
        if dedupe_key in seen_tokens:
            continue
        seen_tokens.add(dedupe_key)
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
        raise sanic.exceptions.InvalidUsage(
            f"unknown query parameter(s): {', '.join(unknown_params)}"
        )

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
    include_subspecialties = _is_truthy_arg(args.get("include_subspecialties"), default=True)
    include_sources = _is_truthy_arg(args.get("include_sources"), default=False)
    include_evidence = _is_truthy_arg(args.get("include_evidence"), default=False)
    is_debug_requested = _is_truthy_arg(args.get("debug"), default=False)
    if is_debug_requested:
        include_sources = True
        include_evidence = True
    limit = _normalize_match_candidate_limit(args.get("limit"))

    locator_count = sum(
        bool(locator_value)
        for locator_value in (
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
        "debug": is_debug_requested,
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
    is_unified = _is_unified_address_table(address_table_sql)
    return {
        "provider_npi": "COALESCE(a.npi, a.inferred_npi)" if is_unified else "a.npi",
        "premise_key": "a.premise_key::text" if is_unified else "NULL::text",
        "address_precision": "a.address_precision" if is_unified else "NULL::text",
        "address_sources": "a.address_sources" if is_unified else "ARRAY[]::varchar[]",
        "source_record_ids": "a.source_record_ids" if is_unified else "ARRAY[]::varchar[]",
        "source_count": "a.source_count" if is_unified else "0",
        "independent_source_count": "a.independent_source_count" if is_unified else "0",
        "multi_source_confirmed": "a.multi_source_confirmed" if is_unified else "false",
        "entity_name": "a.entity_name" if is_unified else "NULL::text",
        "location_key": "a.location_key" if is_unified else "a.checksum::text",
        "updated_at": "a.updated_at" if is_unified else "a.date_added::timestamp",
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
               ) AS source_record_ids, COUNT(DISTINCT current_run.dataset_id)::integer AS provider_directory_source_count
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
    bound_parameter_map: dict[str, Any] = {
        "limit": int(params["limit"]),
        "candidate_limit": min(
            max(int(params["limit"]) * 8, 100),
            _MATCH_CANDIDATES_MAX_INTERNAL_ROWS,
        ),
    }
    address_predicates = [
        _provider_list_address_type_clause(
            "a",
            address_table_sql,
            include_service_locations=True,
        ),
        f"{columns['provider_npi']} IS NOT NULL",
    ]
    address_site_locator = None
    if params.get("address_site_key"):
        bound_parameter_map["address_site_key"] = params["address_site_key"]
        address_site_locator = _address_site_key_filter("a", address_table_sql)
    address_key_locator = None
    if params.get("address_key"):
        bound_parameter_map["address_key"] = params["address_key"]
        address_key_locator = "a.address_key = CAST(:address_key AS uuid)"
    phone_locator = None
    if params.get("phone_digits"):
        bound_parameter_map["phone_digits"] = params["phone_digits"]
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
    if selected_locator_name == "phone" and _is_unified_address_table(address_table_sql):
        phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
        bound_parameter_map["candidate_limit"] = min(
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
    geo_locator_predicates: list[str] = []
    if params.get("lat") is not None and params.get("long") is not None:
        latitude = float(params["lat"])
        longitude = float(params["long"])
        radius = float(params["radius_miles"])
        lat_delta = radius / 69.0
        lon_delta = radius / max(1.0, 69.0 * abs(math.cos(math.radians(latitude))))
        bound_parameter_map.update(
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
            if _is_unified_address_table(address_table_sql)
            else ""
        )
        geo_locator_predicates.append(
            "a.lat IS NOT NULL AND a.long IS NOT NULL "
            f"{geo_precision_clause}"
            "AND a.lat BETWEEN CAST(:lat_min AS numeric) AND CAST(:lat_max AS numeric) "
            "AND a.long BETWEEN CAST(:long_min AS numeric) AND CAST(:long_max AS numeric) "
            f"AND ({geo_distance_expr}) <= :radius_miles"
        )
    # Use the most precise locator supplied. Keeping less selective locators as
    # scoring signals avoids OR plans that combine indexed keys with broad geo
    # predicates over the serving table.
    locator_where = ([selected_locator] if selected_locator else []) or geo_locator_predicates
    address_predicates.append(f"({' OR '.join(locator_where)})")

    npi_table_sql = _schema_cache_key(NPIData.__tablename__)
    taxonomy_filter = _match_candidate_taxonomy_filter_sql(params, bound_parameter_map)
    if params.get("entity_type_code") is not None:
        bound_parameter_map["entity_type_code"] = params["entity_type_code"]
        address_predicates.append(
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
        address_predicates.append(
            _match_candidate_taxonomy_filter_sql(
                params,
                bound_parameter_map,
                npi_sql=columns["provider_npi"],
            )
        )

    npi_predicates: list[str] = []
    if params.get("entity_type_code") is not None:
        npi_predicates.append("n.entity_type_code = :entity_type_code")
    if taxonomy_filter != "1=1":
        npi_predicates.append(taxonomy_filter)
    npi_where_sql = " AND ".join(npi_predicates) if npi_predicates else "1=1"

    address_site_match = (
        "a.premise_key = CAST(:address_site_key AS uuid)"
        if _is_unified_address_table(address_table_sql) and params.get("address_site_key")
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
    geo_source_count_order = (
        ""
        if params.get("lat") is not None and params.get("long") is not None
        else "source_count DESC NULLS LAST,"
    )
    filtered_geo_source_count_order = (
        ""
        if params.get("lat") is not None and params.get("long") is not None
        else "f.source_count DESC NULLS LAST,"
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
             WHERE {' AND '.join(address_predicates)}
          ORDER BY {columns['provider_npi']},
                   address_site_key_matched DESC,
                   address_key_matched DESC,
                   phone_matched DESC,
                   phone_provider_directory_matched DESC,
                   geo_distance_miles ASC NULLS LAST,
                   {geo_source_count_order}
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
               {filtered_geo_source_count_order}
               f.npi
         LIMIT :candidate_limit
        """
    )
    return query, bound_parameter_map


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
    first_taxonomy_map: dict[str, Any] = {}
    for raw in taxonomy_list:
        if not isinstance(raw, Mapping):
            continue
        taxonomy_map = dict(raw)
        if not first_taxonomy_map:
            first_taxonomy_map = taxonomy_map
        if taxonomy_map.get("primary") is True:
            return taxonomy_map
    return first_taxonomy_map


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


def _is_provider_type_filter_matched(row: Mapping[str, Any], params: Mapping[str, Any]) -> bool:
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


def _match_candidate_source_flags(
    provider_row: Mapping[str, Any],
    public_provider_map: Mapping[str, Any],
    enrichment: Mapping[str, Any] | None,
) -> tuple[list[Any], list[Any], bool, Any, bool]:
    fhir_sources = _json_array_value(
        public_provider_map.get(PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY)
    )
    address_sources = _json_array_value(provider_row.get("address_sources"))
    fhir_matched = bool(fhir_sources) or "provider_directory_fhir" in address_sources
    fhir_source_count = provider_row.get("provider_directory_source_count")
    if fhir_source_count is None:
        fhir_source_count = provider_row.get("source_count") or len(fhir_sources)
    ffs_matched = bool(
        enrichment
        and (
            enrichment.get("has_any_enrollment")
            or enrichment.get("has_ffs_enrollment")
            or enrichment.get("has_medicare_claims")
        )
    )
    return (
        fhir_sources,
        address_sources,
        fhir_matched,
        fhir_source_count,
        ffs_matched,
    )


def _match_candidate_address_map(provider_row: Mapping[str, Any]) -> dict[str, Any]:
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
    return {
        key: field_value
        for key, field_value in address_map.items()
        if field_value not in (None, "", [])
    }


def _match_candidate_source_map(
    enrichment: Mapping[str, Any] | None,
    *,
    fhir_matched: bool,
    fhir_source_count: Any,
    ffs_matched: bool,
) -> dict[str, Any]:
    return {
        "nppes": {"matched": True},
        "fhir": {
            "matched": fhir_matched,
            "source_count": fhir_source_count,
        },
        "ffs": {
            "matched": ffs_matched,
            "has_ffs_enrollment": bool(
                enrichment and enrichment.get("has_ffs_enrollment")
            ),
            "has_medicare_claims": bool(
                enrichment and enrichment.get("has_medicare_claims")
            ),
        },
    }


def _match_candidate_evidence_map(
    provider_row: Mapping[str, Any],
    enrichment: Mapping[str, Any] | None,
    address_sources: Sequence[Any],
) -> dict[str, Any]:
    evidence_map = {
        "provider_enrichment_summary": dict(enrichment or {}),
        "source_record_ids": _json_array_value(provider_row.get("source_record_ids")),
        "address_sources": list(address_sources),
    }
    phone_source_record_ids = _json_array_value(
        provider_row.get("phone_source_record_ids")
    )
    if phone_source_record_ids:
        evidence_map["phone_source_record_ids"] = phone_source_record_ids
    return evidence_map


def _match_candidate_response_map(
    provider_row: Mapping[str, Any],
    taxonomy_list: Sequence[Any],
    enrichment: Mapping[str, Any] | None,
    match_score: int,
    match_signals: Mapping[str, Any],
    address_map: Mapping[str, Any],
    source_map: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "npi": provider_row.get("npi"),
        "display_name": _match_candidate_name(provider_row),
        "organization_name": provider_row.get("provider_organization_name"),
        "entity_type_code": provider_row.get("entity_type_code"),
        "entity_kind": _entity_kind_from_code(provider_row.get("entity_type_code")),
        "address_key": provider_row.get("address_key"),
        "address_site_key": provider_row.get("address_site_key"),
        "match_score": match_score,
        "confidence_band": _confidence_band(match_score),
        "match_signals": dict(match_signals),
        "facility": _facility_payload(provider_row, taxonomy_list, enrichment),
        "address": dict(address_map),
        "sources": dict(source_map),
    }


def _include_match_candidate_source_details(
    candidate_map: dict[str, Any],
    public_provider_map: Mapping[str, Any],
    fhir_sources: Sequence[Any],
) -> None:
    """Attach opt-in FHIR and MRF source details to one candidate."""
    if fhir_sources:
        candidate_map[PROVIDER_DIRECTORY_SOURCE_DETAIL_KEY] = fhir_sources
    mrf_sources = _json_array_value(public_provider_map.get(MRF_SOURCE_DETAIL_KEY))
    if not mrf_sources:
        return
    mrf_source_count = int(
        public_provider_map.get(MRF_SOURCE_COUNT_KEY) or len(mrf_sources)
    )
    candidate_map[MRF_SOURCE_DETAIL_KEY] = mrf_sources
    candidate_map[MRF_SOURCE_COUNT_KEY] = mrf_source_count
    candidate_map["sources"]["mrf"] = {
        "matched": True,
        "source_count": mrf_source_count,
    }


def _match_candidate_output(
    provider_row: Mapping[str, Any],
    params: Mapping[str, Any],
    enrichment: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Build one redacted, scored provider candidate response object."""
    public_provider_map = dict(provider_row)
    _redact_internal_address_fields(public_provider_map)
    taxonomy_list = _json_array_value(provider_row.get("taxonomy_list"))
    fhir_sources, address_sources, fhir_matched, fhir_source_count, ffs_matched = (
        _match_candidate_source_flags(provider_row, public_provider_map, enrichment)
    )
    is_taxonomy_matched = _is_provider_type_filter_matched(provider_row, params)
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
        is_taxonomy_matched,
        is_provider_type_matched,
        fhir_matched,
        ffs_matched,
    )
    if is_general_acute_care_matched:
        match_score = _boost_general_acute_care_score(match_signals, match_score)
    candidate_map = _match_candidate_response_map(
        provider_row,
        taxonomy_list,
        enrichment,
        match_score,
        match_signals,
        _match_candidate_address_map(provider_row),
        _match_candidate_source_map(
            enrichment,
            fhir_matched=fhir_matched,
            fhir_source_count=fhir_source_count,
            ffs_matched=ffs_matched,
        ),
    )
    if taxonomy_list:
        candidate_map["taxonomy"] = taxonomy_list
    if params.get("include_sources"):
        _include_match_candidate_source_details(candidate_map, public_provider_map, fhir_sources)
    if params.get("include_evidence"):
        candidate_map["evidence"] = _match_candidate_evidence_map(
            provider_row,
            enrichment,
            address_sources,
        )
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
    """Attach compact address-local provenance without role expansion."""
    await _attach_geo_candidate_record_ids(
        rows,
        params,
        database_session=session,
    )
    if not (params.get("include_sources") or params.get("include_evidence")):
        return
    await _attach_selected_address_source_details(
        rows,
        include_sources=bool(params.get("include_sources")),
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
        _sync_match_candidate_source_counts(candidate_row)
        candidate_row["provider_directory_source_count"] = 0


def _sync_match_candidate_source_counts(candidate_row: dict[str, Any]) -> None:
    """Keep candidate corroboration counts aligned with filtered sources."""
    source_count = len(_json_array_value(candidate_row.get("address_sources")))
    candidate_row["source_count"] = source_count
    candidate_row["independent_source_count"] = source_count
    candidate_row["multi_source_confirmed"] = source_count > 1


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
    evidence_by_candidate = {
        (int(mapping["npi"]), str(mapping["address_key"])): (
            mapping["source_record_ids"],
            int(mapping.get("provider_directory_source_count") or 0),
        )
        for evidence_row in evidence_result.all()
        for mapping in [getattr(evidence_row, "_mapping", evidence_row)]
    }
    for candidate_row in candidate_row_list:
        candidate_key = (
            int(candidate_row["npi"]),
            str(candidate_row.get("address_key") or ""),
        )
        evidence = evidence_by_candidate.get(candidate_key)
        if evidence:
            record_ids, provider_directory_source_count = evidence
            candidate_row["source_record_ids"] = _merge_unique_list_values(
                candidate_row.get("source_record_ids"), record_ids
            )
            candidate_row["address_sources"] = _merge_unique_list_values(
                candidate_row.get("address_sources"),
                "provider_directory_fhir",
            )
            _sync_match_candidate_source_counts(candidate_row)
            candidate_row[
                "provider_directory_source_count"
            ] = provider_directory_source_count


async def _run_match_candidate_stage_bounded(
    operation: Callable[[], Awaitable[Any]],
    *,
    started_at: float,
    stage_name: str,
    database_session: Any = None,
) -> Any:
    """Run one endpoint stage within the shared remaining query budget."""
    try:
        remaining_seconds = _MATCH_CANDIDATES_TIMEOUT_SECONDS - (
            time.monotonic() - started_at
        )
        if remaining_seconds <= 0:
            raise asyncio.TimeoutError()
        return await asyncio.wait_for(operation(), timeout=remaining_seconds)
    except asyncio.CancelledError:
        raise
    except asyncio.TimeoutError as exc:
        await _rollback_match_candidate_session(database_session)
        raise sanic.exceptions.ServiceUnavailable(
            f"match candidate {stage_name} exceeded the "
            f"{_MATCH_CANDIDATES_TIMEOUT_SECONDS:g} second endpoint budget"
        ) from exc
    except Exception as exc:
        await _rollback_match_candidate_session(database_session)
        logger.warning(
            "match candidate %s failed: %s",
            stage_name,
            type(exc).__name__,
        )
        raise sanic.exceptions.ServiceUnavailable(
            f"match candidate {stage_name} is temporarily unavailable"
        ) from exc


async def _attach_candidate_sources_bounded(
    candidate_row_list: list[dict[str, Any]],
    candidate_params: dict[str, Any],
    *,
    started_at: float,
    database_session: Any = None,
) -> None:
    """Attach source details within the endpoint's remaining query budget."""
    await _run_match_candidate_stage_bounded(
        lambda: _attach_match_candidate_source_details(
            candidate_row_list,
            candidate_params,
            session=database_session,
        ),
        started_at=started_at,
        stage_name="source lookup",
        database_session=database_session,
    )


async def _fetch_enriched_match_candidate_rows_bounded(
    candidate_params: dict[str, Any],
    *,
    started_at: float,
    database_session: Any = None,
) -> tuple[list[dict[str, Any]], Mapping[int, Mapping[str, Any]]]:
    """Run candidate lookup, corroboration, and enrichment on one deadline."""
    candidate_rows = await _run_match_candidate_stage_bounded(
        lambda: _fetch_match_candidate_rows(
            candidate_params,
            session=database_session,
        ),
        started_at=started_at,
        stage_name="lookup",
        database_session=database_session,
    )
    await _attach_candidate_sources_bounded(
        candidate_rows,
        candidate_params,
        started_at=started_at,
        database_session=database_session,
    )
    enrichment_map = await _run_match_candidate_stage_bounded(
        lambda: _fetch_provider_enrichment_summary_map(
            [candidate_row.get("npi") for candidate_row in candidate_rows],
            session=database_session,
        ),
        started_at=started_at,
        stage_name="enrichment lookup",
        database_session=database_session,
    )
    return candidate_rows, enrichment_map


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
    candidate_rows, enrichment_map = await _fetch_enriched_match_candidate_rows_bounded(
        params,
        started_at=started,
        database_session=request_session,
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


def _new_provider_from_search_mapping(
    row_mapping: Mapping[str, Any],
    npi_value: Any,
    address_table_sql: str,
) -> dict[str, Any]:
    """Build one list-search provider from a keyed database row."""

    provider_by_field: dict[str, Any] = {
        "taxonomy_list": [],
        "_taxonomy_identities": set(),
        "_address_candidates_complete": True,
    }
    for column in NPIData.__table__.columns:
        if column.key not in PUBLIC_NPI_EXCLUDED_COLUMNS and column.key in row_mapping:
            provider_by_field[column.key] = row_mapping.get(column.key)
    if not provider_by_field.get("provider_organization_name"):
        provider_by_field["provider_organization_name"] = row_mapping.get("entity_name")
    if provider_by_field.get("entity_type_code") is None:
        provider_by_field["entity_type_code"] = (
            1
            if any(
                ":practitioner_role:" in str(record_id or "").lower()
                for record_id in (row_mapping.get("source_record_ids") or [])
            )
            else 2
        )
    for column in NPIAddress.__table__.columns:
        if column.key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS and column.key in row_mapping:
            provider_by_field[column.key] = row_mapping.get(column.key)
    _attach_public_address_site_key(provider_by_field, row_mapping)
    if address_table_sql.endswith(".entity_address_unified"):
        for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
            if key in row_mapping and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                provider_by_field[key] = row_mapping.get(key)
        if "source_record_ids" in row_mapping:
            provider_by_field["source_record_ids"] = row_mapping.get("source_record_ids")
    provider_by_field["npi"] = npi_value
    provider_by_field["do_business_as"] = provider_by_field.get("do_business_as") or []
    provider_by_field.setdefault("procedures_array", [])
    provider_by_field.setdefault("medications_array", [])
    return provider_by_field


def _merge_search_provider_mapping(
    provider_by_field: dict[str, Any],
    row_mapping: Mapping[str, Any],
    address_candidate: dict[str, Any],
) -> None:
    """Merge one list-search address and its unique taxonomies."""

    provider_by_field.setdefault("_address_candidates", []).append(
        address_candidate
    )
    provider_by_field["_address_total"] = max(
        int(provider_by_field.get("_address_total") or 0),
        int(row_mapping.get("provider_address_total") or 0),
    )
    taxonomy_rows = row_mapping.get("taxonomy_rows")
    if isinstance(taxonomy_rows, list):
        taxonomy_payloads = _public_nested_taxonomy_rows(taxonomy_rows)
    else:
        taxonomy_by_field = {
            column.key: row_mapping.get(column.key)
            for column in NPIDataTaxonomy.__table__.columns
            if column.key not in ("npi", "checksum")
            and column.key in row_mapping
        }
        taxonomy_payloads = [taxonomy_by_field] if taxonomy_by_field else []
    for taxonomy_by_field in taxonomy_payloads:
        _append_unique_search_taxonomy(provider_by_field, taxonomy_by_field)


def _append_unique_search_taxonomy(
    provider_by_field: dict[str, Any],
    taxonomy_by_field: Mapping[str, Any],
) -> None:
    """Append one public taxonomy after null-normalized deduplication."""

    taxonomy_by_field = {
        key: field_value
        for key, field_value in taxonomy_by_field.items()
        if field_value is not None
    }
    if not taxonomy_by_field:
        return
    taxonomy_identity_parts = tuple(
        sorted(
            (
                key,
                json.dumps(field_value, sort_keys=True, default=str),
            )
            for key, field_value in taxonomy_by_field.items()
        )
    )
    taxonomy_identities = provider_by_field.setdefault(
        "_taxonomy_identities", set()
    )
    if taxonomy_identity_parts in taxonomy_identities:
        return
    taxonomy_identities.add(taxonomy_identity_parts)
    provider_by_field["taxonomy_list"].append(taxonomy_by_field)


@blueprint.get("/all")
async def list_providers(request):
    """Search, count, or page through public NPI provider records."""
    is_count_only = str(request.args.get("count_only", "0")).strip() == "1"
    include_chain_enrichment = _include_chain_provider_enrichment(request.args.get("show"))
    response_format = request.args.get("format") or request.args.get("response_format")
    response_format = str(response_format).strip().lower() if response_format else None
    request_session = _request_session(request)
    legacy_name_like = _extract_name_filters(request)
    # Explicit access for route collectors / OpenAPI parity.
    request.args.get("q")
    request.args.get("name_like")
    request.args.get("start")
    request.args.get("limit")
    request.args.get("include_total")
    request.args.get("include_sources")
    request.args.get("include_evidence")
    request.args.get("view")
    request.args.get("order_by")
    request.args.get("npi")
    request.args.get("address_site_key")
    request.args.get("provider_sex_code")
    include_sources = _is_truthy_arg(request.args.get("include_sources"), default=False)
    include_evidence = _is_truthy_arg(request.args.get("include_evidence"), default=False)
    if _is_truthy_arg(request.args.get("debug"), default=False):
        include_sources = True
        include_evidence = True
    q_value = str(request.args.get("q") or "").strip().lower()
    order_by = str(request.args.get("order_by") or "npi").strip().lower()
    if order_by not in {"npi", "relevance"}:
        raise sanic.exceptions.InvalidUsage("order_by must be one of: npi, relevance")
    relevance_q = " ".join(_name_search_tokens(q_value))
    if order_by == "relevance" and not relevance_q:
        raise sanic.exceptions.InvalidUsage("order_by=relevance requires q")
    include_total_raw = request.args.get("include_total")
    include_total = _is_truthy_arg(
        include_total_raw,
        default=_should_include_npi_all_total(request.args, is_count_only),
    )
    view_mode = str(request.args.get("view") or "").strip().lower()
    if view_mode not in {"", "sitemap", "card"}:
        raise sanic.exceptions.InvalidUsage("view must be one of: sitemap, card")
    classification = request.args.get("classification")
    is_sitemap_limit_mode = (
        view_mode == "sitemap"
        and str(classification or "").strip().lower() == "pharmacy"
    )
    name_like_values: list[str] = []
    if q_value:
        name_like_values.append(q_value)
    for legacy_name_filter in legacy_name_like:
        if legacy_name_filter not in name_like_values:
            name_like_values.append(legacy_name_filter)
    pagination = parse_pagination(
        request.args,
        default_limit=50,
        max_limit=20000 if is_sitemap_limit_mode else 200,
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
    provider_sex_code_raw = request.args.get("provider_sex_code")
    plan_network_ids = request.args.get("plan_network")
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

    if plan_network_ids:
        plan_network_ids = [int(x) for x in plan_network_ids.split(",")]

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

    capability_by_name = {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
        "pricing_provider_procedure_available": False,
        "pricing_provider_prescription_available": False,
    }
    if requested_procedure_codes or requested_medication_codes:
        if request_session is not None:
            capability_by_name = await _resolve_npi_filter_capabilities(session=request_session)
        else:
            capability_by_name = await _resolve_npi_filter_capabilities()

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
    provider_sex_code = normalize_provider_sex_code(provider_sex_code_raw)
    if entity_type_code == 2 and provider_sex_code is not None:
        raise sanic.exceptions.InvalidUsage(
            "provider_sex_code cannot be combined with entity_type_code=2"
        )

    filters_by_name = {
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
        "provider_sex_code": provider_sex_code,
        "plan_network": plan_network_ids,
        "names_like": name_like_values,
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
        "npi_procedures_array_available": capability_by_name["npi_procedures_array_available"],
        "npi_medications_array_available": capability_by_name["npi_medications_array_available"],
        "pricing_provider_procedure_available": capability_by_name["pricing_provider_procedure_available"],
        "pricing_provider_prescription_available": capability_by_name["pricing_provider_prescription_available"],
    }

    simple_filter_present = any(
        filters_by_name.get(field)
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
            "provider_sex_code",
            "plan_network",
            "names_like",
            "codes",
            "response_format",
            "procedure_internal_codes",
            "medication_internal_codes",
        )
    )
    broad_name_total_deferred = bool(name_like_values) and not any(
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
            provider_sex_code,
            plan_network_ids,
            codes,
            has_insurance,
            city,
            state,
            response_format,
            procedure_internal_codes,
            medication_internal_codes,
        ]
    )
    inline_name_taxonomy_total = bool(
        include_total
        and order_by == "npi"
        and name_like_values
        and codes
        and not phone_digits
        and not any((classification, specialization, section, display_name))
    )

    def _append_available_filter(
        address_clauses: list[str],
        array_clause: str,
        exists_clause: str,
        *,
        is_array_available: bool,
        is_table_available: bool,
    ) -> None:
        if is_array_available and is_table_available:
            address_clauses.append(f"({array_clause} OR {exists_clause})")
        elif is_array_available:
            address_clauses.append(array_clause)
        elif is_table_available:
            address_clauses.append(exists_clause)
        else:
            address_clauses.append("1=0")

    def _append_array_filters(
        address_clauses: list[str],
        filters_by_name: dict[str, Any],
    ) -> dict[str, int]:
        """Append supported procedure and medication predicates and parameters."""
        parameters_by_name: dict[str, int] = {}
        provider_npi_sql = (
            "COALESCE(c.npi, c.inferred_npi)"
            if _is_unified_address_table(address_table_sql)
            else "c.npi"
        )
        filter_year = filters_by_name.get("filter_year")
        procedure_internal_codes = filters_by_name.get("procedure_internal_codes") or []
        medication_internal_codes = filters_by_name.get("medication_internal_codes") or []
        procedures_array_available = bool(filters_by_name.get("npi_procedures_array_available", True))
        medications_array_available = bool(filters_by_name.get("npi_medications_array_available", True))
        procedure_table_available = bool(filters_by_name.get("pricing_provider_procedure_available", False))
        medication_table_available = bool(filters_by_name.get("pricing_provider_prescription_available", False))
        if filter_year is not None and (procedure_internal_codes or medication_internal_codes):
            parameters_by_name["filter_year"] = int(filter_year)
        for idx, code in enumerate(procedure_internal_codes):
            parameter_name = f"procedure_code_{idx}"
            parameters_by_name[parameter_name] = int(code)
            array_clause = f"c.procedures_array @> ARRAY[:{parameter_name}]::INTEGER[]"
            exists_clause = (
                "EXISTS ("
                "SELECT 1 FROM mrf.pricing_provider_procedure AS pp "
                f"WHERE pp.npi = {provider_npi_sql} AND pp.procedure_code = :{parameter_name}"
                + (" AND pp.year = :filter_year" if filter_year is not None else "")
                + ")"
            )
            _append_available_filter(
                address_clauses,
                array_clause,
                exists_clause,
                is_array_available=procedures_array_available,
                is_table_available=procedure_table_available,
            )
        for idx, code in enumerate(medication_internal_codes):
            parameter_name = f"medication_code_{idx}"
            parameters_by_name[parameter_name] = int(code)
            array_clause = f"c.medications_array @> ARRAY[:{parameter_name}]::INTEGER[]"
            exists_clause = (
                "EXISTS ("
                "SELECT 1 FROM mrf.pricing_provider_prescription AS pr "
                f"WHERE pr.npi = {provider_npi_sql} "
                "AND pr.rx_code_system = 'HP_RX_CODE' "
                + ("AND pr.year = :filter_year " if filter_year is not None else "")
                + f"AND CASE WHEN pr.rx_code ~ '^-?[0-9]+$' THEN pr.rx_code::INTEGER END = :{parameter_name} "
                ")"
            )
            _append_available_filter(
                address_clauses,
                array_clause,
                exists_clause,
                is_array_available=medications_array_available,
                is_table_available=medication_table_available,
            )
        return parameters_by_name

    address_required_columns = _public_address_serving_column_keys()
    address_table_sql = await _address_serving_table_sql(
        address_required_columns,
        session=request_session,
    )

    async def get_count(filters_by_name):
        """Count providers matching the normalized request filters."""
        classification = filters_by_name.get("classification")
        specialization = filters_by_name.get("specialization")
        section = filters_by_name.get("section")
        display_name = filters_by_name.get("display_name")
        first_name = filters_by_name.get("first_name")
        last_name = filters_by_name.get("last_name")
        organization_name = filters_by_name.get("organization_name")
        entity_type_code = filters_by_name.get("entity_type_code")
        provider_sex_code = filters_by_name.get("provider_sex_code")
        plan_network_ids = filters_by_name.get("plan_network")
        name_like_values = filters_by_name.get("names_like") or []
        codes = filters_by_name.get("codes")
        has_insurance = filters_by_name.get("has_insurance")
        city = filters_by_name.get("city")
        state = filters_by_name.get("state")
        zip_code = filters_by_name.get("zip_code")
        phone_digits = filters_by_name.get("phone_digits")
        address_key = filters_by_name.get("address_key")
        address_site_key = filters_by_name.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters_by_name.get("npi")
        is_unified_search = _is_unified_address_table(address_table_sql)
        provider_npi_sql = (
            "COALESCE(c.npi, c.inferred_npi)" if is_unified_search else "c.npi"
        )

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
            name_like_values,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )
        use_taxonomy_filter = bool(taxonomy_filters)
        use_location_first_taxonomy = _is_location_first_taxonomy_filter(
            use_taxonomy_filter,
            (
                npi_where,
                city,
                state,
                zip_code,
                phone_digits,
                address_key,
                address_site_key,
                exact_npi,
            ),
        )
        # Unified rows represent independently usable service locations. Search
        # providers across that complete set, then rank at most three per NPI.
        include_service_locations = _is_unified_address_table(address_table_sql)
        address_clauses = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if use_taxonomy_filter and not use_location_first_taxonomy:
            address_clauses.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network_ids:
            address_clauses.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_clauses.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_clauses.append("city_name = :city")
        if state:
            address_clauses.append("state_name = :state")
        if zip_code:
            address_clauses.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c")
            else:
                address_clauses.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_clauses.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_clauses.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_clauses.append(_address_npi_filter("c", address_table_sql))
        if provider_sex_code is not None:
            address_clauses.append(
                "EXISTS ("
                "SELECT 1 FROM mrf.npi AS sex_provider "
                f"WHERE sex_provider.npi = {provider_npi_sql} "
                "AND sex_provider.provider_sex_code = :provider_sex_code"
                ")"
            )
        dynamic_code_parameters = _append_array_filters(address_clauses, filters_by_name)

        taxonomy_conditions = " AND ".join(taxonomy_filters) if taxonomy_filters else "1=1"
        taxonomy_subquery = _taxonomy_codes_subquery(taxonomy_conditions)
        taxonomy_join = f"CROSS JOIN {taxonomy_subquery}"
        taxonomy_parameters_by_name: dict[str, str] = {}
        taxonomy_code_placeholders: tuple[str, ...] = ()
        if use_location_first_taxonomy and not npi_where:
            if codes and len(taxonomy_filters) == 1:
                taxonomy_parameters_by_name, taxonomy_code_placeholders = (
                    _provider_taxonomy_code_parameters(
                        codes,
                        "count_provider_taxonomy_code",
                    )
                )
                taxonomy_join = _provider_taxonomy_lateral_join(
                    code_placeholders=taxonomy_code_placeholders,
                    provider_npi_sql=provider_npi_sql,
                )
            else:
                taxonomy_join += _provider_taxonomy_lateral_join(
                    provider_npi_sql=provider_npi_sql,
                )
        elif npi_where and codes and len(taxonomy_filters) == 1:
            taxonomy_parameters_by_name, taxonomy_code_placeholders = (
                _provider_taxonomy_code_parameters(
                    codes,
                    "count_name_provider_taxonomy_code",
                )
            )

        filtered_npi_cte = None
        taxonomy_matched_npi_cte = None
        if npi_where:
            direct_name_taxonomy = bool(taxonomy_code_placeholders)
            if not direct_name_taxonomy:
                filtered_npi_cte = f"""
                filtered_npi AS MATERIALIZED (
                    SELECT b.npi
                      FROM mrf.npi AS b
                     WHERE {npi_where}
                )
                """
            if use_taxonomy_filter:
                taxonomy_matched_npi_cte = (
                    _provider_taxonomy_matched_npi_cte(
                        taxonomy_conditions,
                        code_placeholders=taxonomy_code_placeholders,
                        npi_where=npi_where if direct_name_taxonomy else "",
                    )
                )

        if npi_where and use_taxonomy_filter:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte, filtered_npi_cte, taxonomy_matched_npi_cte)}
                SELECT COUNT(DISTINCT {provider_npi_sql})
                  FROM taxonomy_matched_npi AS fn
                  JOIN {address_table_sql} AS c
                    ON {provider_npi_sql} = fn.npi
                  {phone_candidates_join}
                 WHERE {' AND '.join(address_clauses)}
                """
            )
        elif npi_where:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte, filtered_npi_cte)}
                SELECT COUNT(DISTINCT {provider_npi_sql})
                  FROM filtered_npi AS fn
                  JOIN {address_table_sql} AS c
                    ON {provider_npi_sql} = fn.npi
                  {phone_candidates_join}
                 WHERE {' AND '.join(address_clauses)}
                """
            )
        elif use_taxonomy_filter:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte)}
                SELECT COUNT(DISTINCT {provider_npi_sql})
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                  {taxonomy_join}
                 WHERE {' AND '.join(address_clauses)}
                """
            )
        else:
            query = text(
                f"""
                {_sql_with_ctes(phone_candidates_cte)}
                SELECT COUNT(DISTINCT {provider_npi_sql})
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                 WHERE {' AND '.join(address_clauses)}
                """
            )

        query_parameters_by_name = {
            "classification": classification,
            "section": section,
            "display_name": display_name,
            "plan_network_array": plan_network_ids,
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
            "provider_sex_code": provider_sex_code,
            "filter_year": filters_by_name.get("filter_year"),
        }
        query_parameters_by_name.update(dynamic_code_parameters)
        query_parameters_by_name.update(npi_params)
        query_parameters_by_name.update(taxonomy_parameters_by_name)
        if phone_candidates_cte:
            query_parameters_by_name["candidate_limit"] = _provider_list_phone_candidate_limit(
                limit,
                count_query=True,
            )

        async with db.acquire() as conn:
            count_records = await conn.all(query, **query_parameters_by_name)
        return count_records[0][0] if count_records else 0

    async def get_formatted_count(response_format: str) -> dict:
        """
        Return mapping for special count formats (full_taxonomy/classification).
        """
        if response_format == "full_taxonomy":
            formatted_count_query = text(
                "SELECT ARRAY[int_code] AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY ARRAY[int_code]"
            )
        else:
            formatted_count_query = text(
                "SELECT classification AS key, COUNT(*) AS value "
                "FROM mrf.nucc_taxonomy GROUP BY classification"
            )
        async with db.acquire() as conn:
            formatted_count_records = await conn.all(formatted_count_query)
        return {
            count_record[0]: count_record[1]
            for count_record in formatted_count_records
        }

    async def get_classification_count_map(filters_by_name) -> dict:
        """Return provider counts grouped by NUCC classification."""
        classification = filters_by_name.get("classification")
        specialization = filters_by_name.get("specialization")
        section = filters_by_name.get("section")
        display_name = filters_by_name.get("display_name")
        first_name = filters_by_name.get("first_name")
        last_name = filters_by_name.get("last_name")
        organization_name = filters_by_name.get("organization_name")
        entity_type_code = filters_by_name.get("entity_type_code")
        provider_sex_code = filters_by_name.get("provider_sex_code")
        plan_network_ids = filters_by_name.get("plan_network")
        name_like_values = filters_by_name.get("names_like") or []
        codes = filters_by_name.get("codes")
        has_insurance = filters_by_name.get("has_insurance")
        city = filters_by_name.get("city")
        state = filters_by_name.get("state")
        zip_code = filters_by_name.get("zip_code")
        phone_digits = filters_by_name.get("phone_digits")
        address_key = filters_by_name.get("address_key")
        address_site_key = filters_by_name.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters_by_name.get("npi")

        provider_npi_sql = (
            "COALESCE(c.npi, c.inferred_npi)"
            if _is_unified_address_table(address_table_sql)
            else "c.npi"
        )

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
            name_like_values,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )

        include_service_locations = _is_unified_address_table(address_table_sql)
        address_clauses = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if plan_network_ids:
            address_clauses.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_clauses.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_clauses.append("city_name = :city")
        if state:
            address_clauses.append("state_name = :state")
        if zip_code:
            address_clauses.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c")
            else:
                address_clauses.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_clauses.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_clauses.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_clauses.append(_address_npi_filter("c", address_table_sql))
        if provider_sex_code is not None:
            address_clauses.append(
                "EXISTS ("
                "SELECT 1 FROM mrf.npi AS sex_provider "
                f"WHERE sex_provider.npi = {provider_npi_sql} "
                "AND sex_provider.provider_sex_code = :provider_sex_code"
                ")"
            )
        dynamic_code_parameters = _append_array_filters(address_clauses, filters_by_name)
        if npi_where:
            address_clauses.append(
                "EXISTS (SELECT 1 FROM mrf.npi AS b "
                f"WHERE b.npi = {provider_npi_sql} AND {npi_where})"
            )

        taxonomy_conditions = " AND ".join(taxonomy_filters) if taxonomy_filters else "1=1"
        taxonomy_subquery = _taxonomy_classification_subquery(taxonomy_conditions)
        query = text(
            f"""
            {_sql_with_prefix_ctes(phone_candidates_cte)}filtered_taxonomy AS (
                SELECT DISTINCT {provider_npi_sql} AS npi, code.int_code
                  FROM {address_table_sql} AS c
                  {phone_candidates_join}
                  CROSS JOIN LATERAL unnest(COALESCE(c.taxonomy_array, ARRAY[]::INTEGER[])) AS code(int_code)
                 WHERE {' AND '.join(address_clauses)}
            )
            SELECT q.classification AS key,
                   COUNT(DISTINCT ft.npi) AS value
              FROM filtered_taxonomy AS ft
              JOIN {taxonomy_subquery}
                ON ft.int_code = q.int_code
             GROUP BY q.classification
            """
        )
        query_parameters_by_name = {
            "classification": classification,
            "section": section,
            "display_name": display_name,
            "plan_network_array": plan_network_ids,
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
            "provider_sex_code": provider_sex_code,
            "filter_year": filters_by_name.get("filter_year"),
        }
        query_parameters_by_name.update(dynamic_code_parameters)
        query_parameters_by_name.update(npi_params)
        if phone_candidates_cte:
            query_parameters_by_name["candidate_limit"] = _provider_list_phone_candidate_limit(
                limit,
                count_query=True,
            )
        async with db.acquire() as conn:
            classification_count_records = await conn.all(query, **query_parameters_by_name)
        return {
            count_record[0]: count_record[1]
            for count_record in classification_count_records
            if count_record and count_record[0]
        }

    procedure_filter_unresolved = bool(requested_procedure_codes) and not bool(procedure_internal_codes)
    medication_filter_unresolved = bool(requested_medication_codes) and not bool(medication_internal_codes)
    if procedure_filter_unresolved or medication_filter_unresolved:
        if is_count_only and response_format in {"all", "full_taxonomy", "classification"}:
            return response.json({"rows": {}}, default=str)
        if is_count_only:
            return response.json({"rows": 0}, default=str)
        return response.json(
            {
                "total": 0,
                "page": pagination.page,
                "limit": pagination.limit,
                "offset": pagination.offset,
                "total_source": "computed",
                "rows": [],
            },
            default=str,
        )

    if is_count_only and not simple_filter_present and not has_insurance and not city and not state:
        return response.json({"rows": await _fast_primary_npi_count()}, default=str)

    if is_count_only and has_insurance and not simple_filter_present:
        insurance_provider_count = await _fast_has_insurance_count(city, state)
        return response.json({"rows": insurance_provider_count}, default=str)

    if is_count_only and response_format == "all":
        mapping = await get_classification_count_map(filters_by_name)
        return response.json({"rows": mapping}, default=str)

    if is_count_only and response_format in {"full_taxonomy", "classification"}:
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
                {_runtime_db_schema()}.{ADDRESS_FORMAT_FUNCTION}(
                    c.first_line,
                    c.second_line,
                    c.city_name,
                    c.state_name,
                    c.postal_code,
                    c.country_code
                ) AS formatted_address
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
        sitemap_results: list[dict[str, Any]] = []
        for sitemap_record in rows_iter:
            mapping = getattr(sitemap_record, "_mapping", None)
            if mapping is None:
                continue
            sitemap_results.append(
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
        return sitemap_results

    async def get_results(start, limit, filters_by_name):
        """Return one provider result page for normalized search filters."""
        classification = filters_by_name.get("classification")
        section = filters_by_name.get("section")
        display_name = filters_by_name.get("display_name")
        first_name = filters_by_name.get("first_name")
        last_name = filters_by_name.get("last_name")
        organization_name = filters_by_name.get("organization_name")
        entity_type_code = filters_by_name.get("entity_type_code")
        provider_sex_code = filters_by_name.get("provider_sex_code")
        plan_network_ids = filters_by_name.get("plan_network")
        name_like_values = filters_by_name.get("names_like") or []
        specialization = filters_by_name.get("specialization")
        city = filters_by_name.get("city")
        state = filters_by_name.get("state")
        has_insurance = filters_by_name.get("has_insurance")
        codes = filters_by_name.get("codes")
        zip_code = filters_by_name.get("zip_code")
        phone_digits = filters_by_name.get("phone_digits")
        address_key = filters_by_name.get("address_key")
        address_site_key = filters_by_name.get(PUBLIC_ADDRESS_SITE_KEY)
        exact_npi = filters_by_name.get("npi")
        is_unified_search = _is_unified_address_table(address_table_sql)
        provider_npi_sql = (
            "COALESCE(c.npi, c.inferred_npi)" if is_unified_search else "c.npi"
        )
        npi_where, npi_params = _build_npi_where_clause(
            "b",
            name_like_values,
            first_name,
            last_name,
            organization_name,
            entity_type_code,
        )
        taxonomy_clauses = []
        include_service_locations = _is_unified_address_table(address_table_sql)
        address_clauses = [
            _provider_list_address_type_clause(
                "c",
                address_table_sql,
                include_service_locations=include_service_locations,
            )
        ]
        phone_candidates_cte = None
        phone_candidates_join = ""
        if classification:
            taxonomy_clauses.append("classification = :classification")
        if specialization:
            taxonomy_clauses.append("specialization = :specialization")
        if section:
            taxonomy_clauses.append("section = :section")
        if display_name:
            taxonomy_clauses.append("display_name = :display_name")
        if codes:
            taxonomy_clauses.append("code = ANY(:codes)")
        use_taxonomy_filter = bool(taxonomy_clauses)
        use_location_first_taxonomy = _is_location_first_taxonomy_filter(
            use_taxonomy_filter,
            (
                npi_where,
                city,
                state,
                zip_code,
                phone_digits,
                address_key,
                address_site_key,
                exact_npi,
            ),
        )
        if use_taxonomy_filter and not use_location_first_taxonomy:
            address_clauses.insert(0, "c.taxonomy_array && q.int_codes")
        if plan_network_ids:
            address_clauses.append("plans_network_array && :plan_network_array")
        if has_insurance:
            address_clauses.append("NOT (plans_network_array @@ '0'::query_int)")
        if city:
            address_clauses.append("city_name = :city")
        if state:
            address_clauses.append("state_name = :state")
        if zip_code:
            address_clauses.append(_address_zip5_filter("c", address_table_sql))
        if phone_digits:
            phone_candidates_cte = _address_phone_candidates_cte(address_table_sql)
            if phone_candidates_cte:
                phone_candidates_join = _address_phone_candidates_join("c")
            else:
                address_clauses.append(_address_phone_digits_filter("c", address_table_sql))
        if address_key:
            address_clauses.append("c.address_key = CAST(:address_key AS uuid)")
        if address_site_key:
            address_clauses.append(_address_site_key_filter("c", address_table_sql))
        if exact_npi is not None:
            address_clauses.append(_address_npi_filter("c", address_table_sql))
        if provider_sex_code is not None:
            address_clauses.append(
                "EXISTS ("
                "SELECT 1 FROM mrf.npi AS sex_provider "
                f"WHERE sex_provider.npi = {provider_npi_sql} "
                "AND sex_provider.provider_sex_code = :provider_sex_code"
                ")"
            )
        dynamic_code_parameters = _append_array_filters(address_clauses, filters_by_name)
        lightweight_candidate_columns = tuple(
            dict.fromkeys(
                NPI_LOCATION_CANDIDATE_COLUMNS
                + (
                    "entity_name",
                    "source_record_ids",
                    "address_sources",
                )
            )
        )
        candidate_projection = (
            ", ".join(
                f"c.{column_name}"
                for column_name in lightweight_candidate_columns
                if column_name != "npi"
            )
            if is_unified_search
            else "c.*"
        )
        projected_candidate_names = (
            tuple(
                column_name
                for column_name in lightweight_candidate_columns
                if column_name != "npi"
            )
            if is_unified_search
            else tuple(column.key for column in NPIAddress.__table__.columns)
        )
        search_npi_column_names = tuple(
            column.key for column in _npi_serving_columns()
        )
        search_row_column_names = (
            ("npi_code",)
            + search_npi_column_names
            + projected_candidate_names
            + ("provider_address_total",)
            + (("_provider_total",) if inline_name_taxonomy_total else ())
        )
        search_npi_projection = ", ".join(
            f"b.{column_name}" for column_name in search_npi_column_names
        )

        taxonomy_filter = " and ".join(taxonomy_clauses) if taxonomy_clauses else "1=1"
        taxonomy_parameters_by_name: dict[str, str] = {}
        taxonomy_code_placeholders: tuple[str, ...] = ()
        if npi_where and codes and len(taxonomy_clauses) == 1:
            taxonomy_parameters_by_name, taxonomy_code_placeholders = (
                _provider_taxonomy_code_parameters(
                    codes,
                    "page_name_provider_taxonomy_code",
                )
            )
        filtered_npi_cte = None
        taxonomy_matched_npi_cte = None
        if npi_where:
            filtered_npi_projection = "b.npi"
            if order_by == "relevance":
                name_expression = NAME_LIKE_TEMPLATE.format(alias="b.")
                filtered_npi_projection += (
                    f", similarity({name_expression}, :relevance_q) "
                    "AS relevance_score"
                )
            direct_name_taxonomy = bool(taxonomy_code_placeholders)
            if not direct_name_taxonomy:
                filtered_npi_cte = f"""
        filtered_npi AS MATERIALIZED (
            SELECT {filtered_npi_projection}
              FROM mrf.npi AS b
             WHERE {npi_where}
        )
"""
            if use_taxonomy_filter:
                taxonomy_matched_npi_cte = (
                    _provider_taxonomy_matched_npi_cte(
                        taxonomy_filter,
                        code_placeholders=taxonomy_code_placeholders,
                        npi_where=npi_where if direct_name_taxonomy else "",
                        npi_projection=filtered_npi_projection,
                    )
                )

        taxonomy_source = (
            "CROSS JOIN ("
            "SELECT ARRAY_AGG(code) AS codes, "
            "ARRAY_AGG(int_code) AS int_codes "
            f"FROM mrf.nucc_taxonomy WHERE {taxonomy_filter}"
            ") AS q"
        )
        if use_location_first_taxonomy and not npi_where:
            if codes and len(taxonomy_clauses) == 1:
                taxonomy_parameters_by_name, taxonomy_code_placeholders = (
                    _provider_taxonomy_code_parameters(
                        codes,
                        "page_provider_taxonomy_code",
                    )
                )
                taxonomy_source = _provider_taxonomy_lateral_join(
                    code_placeholders=taxonomy_code_placeholders,
                    provider_npi_sql=provider_npi_sql,
                )
            else:
                taxonomy_source += _provider_taxonomy_lateral_join(
                    provider_npi_sql=provider_npi_sql,
                )

        if npi_where and use_taxonomy_filter:
            address_source = (
                "taxonomy_matched_npi as fn\n"
                f"    JOIN {address_table_sql} as c ON {provider_npi_sql} = fn.npi\n"
                f"    {phone_candidates_join}"
            )
        elif npi_where:
            address_source = (
                "filtered_npi as fn\n"
                f"    JOIN {address_table_sql} as c ON {provider_npi_sql} = fn.npi\n"
                f"    {phone_candidates_join}"
            )
        elif use_taxonomy_filter:
            address_source = (
                f"{address_table_sql} as c\n"
                f"    {phone_candidates_join}\n"
                f"    {taxonomy_source}"
            )
        else:
            address_source = f"{address_table_sql} as c\n    {phone_candidates_join}"
        address_order = _primary_address_order_clause("c", address_table_sql)
        if order_by == "relevance":
            eligible_npis_sql = f"""
            SELECT {provider_npi_sql} AS npi,
                   MAX(fn.relevance_score) AS relevance_score
              FROM {address_source}
             WHERE {' and '.join(address_clauses)}
             GROUP BY {provider_npi_sql}
            """
            page_order_sql = "ORDER BY relevance_score DESC, npi ASC"
            sub_s_relevance_projection = ", pn.relevance_score AS _search_relevance"
            result_order_sql = (
                "ORDER BY sub_s._search_relevance DESC, sub_s.npi_code ASC"
            )
        else:
            eligible_npis_sql = f"""
            SELECT DISTINCT {provider_npi_sql} AS npi
              FROM {address_source}
             WHERE {' and '.join(address_clauses)}
            """
            page_order_sql = "ORDER BY npi"
            sub_s_relevance_projection = ""
            result_order_sql = "ORDER BY sub_s.npi_code ASC"
        if inline_name_taxonomy_total:
            page_npis_sql = f"""
            SELECT eligible_npi.*,
                   COUNT(*) OVER () AS provider_total
              FROM ({eligible_npis_sql}) AS eligible_npi
             {page_order_sql}
             LIMIT :limit OFFSET :start
            """
            sub_s_total_projection = ", pn.provider_total AS _provider_total"
        else:
            page_npis_sql = f"""
            {eligible_npis_sql}
            {page_order_sql}
            LIMIT :limit OFFSET :start
            """
            sub_s_total_projection = ""
        provider_page_query = text(
            f"""
        {_sql_with_prefix_ctes(phone_candidates_cte, filtered_npi_cte, taxonomy_matched_npi_cte)}page_npis AS (
            {page_npis_sql}
        ),
        sub_s AS (
            SELECT pn.npi AS npi_code, {search_npi_projection}, g.*{sub_s_relevance_projection}{sub_s_total_projection}
              FROM page_npis AS pn
         LEFT JOIN mrf.npi AS b ON b.npi = pn.npi
              JOIN LATERAL (
                  SELECT {candidate_projection},
                         COUNT(*) OVER () AS provider_address_total
                    FROM {address_source}
                   WHERE {' and '.join(address_clauses)}
                     AND {provider_npi_sql} = pn.npi
                   ORDER BY {address_order}
              ) AS g ON TRUE
        )

    SELECT sub_s.* FROM sub_s {result_order_sql};
    """
        )

        def _search_location_from_mapping(
            row_mapping: Mapping[str, Any],
        ) -> dict[str, Any]:
            location_by_field: dict[str, Any] = {}
            for column in NPIAddress.__table__.columns:
                if column.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                    continue
                if column.key in row_mapping:
                    location_by_field[column.key] = row_mapping.get(column.key)
            if location_by_field.get("npi") is None and row_mapping.get(
                "inferred_npi"
            ) is not None:
                location_by_field["npi"] = row_mapping.get("inferred_npi")
            if location_by_field.get("npi") is None:
                location_by_field["npi"] = row_mapping.get("npi_code")
            if row_mapping.get("location_key") not in (None, ""):
                location_by_field["location_key"] = row_mapping.get(
                    "location_key"
                )
            _attach_public_address_site_key(location_by_field, row_mapping)
            if address_table_sql.endswith(".entity_address_unified"):
                for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
                    if key in row_mapping and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                        location_by_field[key] = row_mapping.get(key)
                if "source_record_ids" in row_mapping:
                    location_by_field["source_record_ids"] = row_mapping.get(
                        "source_record_ids"
                    )
            base_identity = _base_address_row_identity(location_by_field)
            if base_identity:
                location_by_field["_base_row_identities"] = [base_identity]
            location_by_field["location_status"] = "unknown"
            return _add_canonical_contact_fields_to_address(location_by_field)

        providers_by_npi = {}
        provider_total: Optional[int] = None
        async with db.acquire() as conn:
            query_parameters_by_name = {
                "start": start,
                "limit": limit,
                "classification": classification,
                "section": section,
                "display_name": display_name,
                "codes": codes,
                "plan_network_array": plan_network_ids,
                "specialization": specialization,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "phone_digits": phone_digits,
                "address_key": address_key,
                "address_site_key": address_site_key,
                "npi_filter": exact_npi,
                "provider_sex_code": provider_sex_code,
                **npi_params,
                **dynamic_code_parameters,
                **taxonomy_parameters_by_name,
            }
            if order_by == "relevance":
                query_parameters_by_name["relevance_q"] = relevance_q
            if phone_candidates_cte:
                query_parameters_by_name["candidate_limit"] = _provider_list_phone_candidate_limit(
                    limit,
                    start,
                )
            rows_iter = await conn.all(
                provider_page_query,
                **query_parameters_by_name,
            )
            for provider_record in rows_iter:
                # Prefer key-based extraction so schema drift in upstream tables
                # (for example missing optional array columns) does not break
                # positional offsets and crash /npi/all.
                row_mapping = getattr(provider_record, "_mapping", None)
                if row_mapping is None:
                    row_mapping = {
                        column_name: provider_record[index]
                        for index, column_name in enumerate(
                            search_row_column_names
                        )
                        if index < len(provider_record)
                    }
                if row_mapping is not None:
                    if (
                        provider_total is None
                        and row_mapping.get("_provider_total") is not None
                    ):
                        provider_total = int(row_mapping["_provider_total"])
                    npi_value = (
                        row_mapping.get("npi_code")
                        or row_mapping.get("npi")
                        or row_mapping.get("npi_1")
                        or row_mapping.get("npi_2")
                    )
                    if npi_value is None:
                        continue

                    provider_by_field = providers_by_npi.get(npi_value)
                    if provider_by_field is None:
                        provider_by_field = _new_provider_from_search_mapping(
                            row_mapping,
                            npi_value,
                            address_table_sql,
                        )

                    address_candidate = _search_location_from_mapping(row_mapping)
                    _merge_search_provider_mapping(
                        provider_by_field,
                        row_mapping,
                        address_candidate,
                    )
                    providers_by_npi[npi_value] = provider_by_field
                    continue

        provider_results = list(providers_by_npi.values())

        async def _fetch_search_taxonomy_records() -> list[Any]:
            if not providers_by_npi:
                return []
            async with db.acquire() as taxonomy_conn:
                return await taxonomy_conn.all(
                    text(
                        "SELECT taxonomy.*, nucc.display_name AS taxonomy_display "
                        "FROM mrf.npi_taxonomy AS taxonomy "
                        "LEFT JOIN mrf.nucc_taxonomy AS nucc ON "
                        "nucc.code = taxonomy.healthcare_provider_taxonomy_code "
                        "WHERE taxonomy.npi = ANY(:provider_npis) "
                        "ORDER BY taxonomy.npi, taxonomy.checksum"
                    ),
                    provider_npis=sorted(providers_by_npi),
                )

        async def _fetch_search_enrichment_summary() -> dict[int, dict[str, Any]]:
            if view_mode == "card":
                return {}
            try:
                return await _fetch_provider_enrichment_summary_map(
                    [provider_result.get("npi") for provider_result in provider_results],
                    include_chain=include_chain_enrichment,
                    session=None,
                )
            except Exception as exc:
                logger.debug("Provider enrichment summary fetch failed: %s", exc)
                return {}

        search_read_tasks = (
            asyncio.create_task(_fetch_search_taxonomy_records()),
            asyncio.create_task(
                _apply_location_statuses(
                    [
                        address_candidate
                        for provider_result in provider_results
                        for address_candidate in provider_result.get(
                            "_address_candidates",
                            [],
                        )
                    ],
                    session=request_session,
                )
            ),
            asyncio.create_task(_fetch_search_enrichment_summary()),
        )
        try:
            taxonomy_records, _, summary_map = await asyncio.gather(
                *search_read_tasks
            )
        except BaseException:
            for search_read_task in search_read_tasks:
                search_read_task.cancel()
            await asyncio.gather(*search_read_tasks, return_exceptions=True)
            raise
        for taxonomy_record in taxonomy_records:
            taxonomy_mapping = getattr(
                taxonomy_record,
                "_mapping",
                None,
            )
            if taxonomy_mapping is None:
                taxonomy_mapping = {
                    column.key: taxonomy_record[index]
                    for index, column in enumerate(
                        NPIDataTaxonomy.__table__.columns
                    )
                    if index < len(taxonomy_record)
                }
            taxonomy_npi = taxonomy_mapping.get("npi")
            provider_by_field = providers_by_npi.get(taxonomy_npi)
            if provider_by_field is None:
                continue
            taxonomy_by_field = {
                column.key: taxonomy_mapping.get(column.key)
                for column in NPIDataTaxonomy.__table__.columns
                if column.key not in ("npi", "checksum")
            }
            if taxonomy_mapping.get("taxonomy_display") is not None:
                taxonomy_by_field["display"] = taxonomy_mapping.get(
                    "taxonomy_display"
                )
            _append_unique_search_taxonomy(
                provider_by_field,
                taxonomy_by_field,
            )
        for provider_result in provider_results:
            provider_result["do_business_as"] = provider_result.get("do_business_as") or []
            address_candidates = provider_result.pop("_address_candidates", [])
            ranked_locations = _rank_provider_locations(
                _dedupe_addresses_by_key(address_candidates)
            )
            candidates_are_complete = bool(
                provider_result.pop("_address_candidates_complete", False)
            )
            raw_address_total = int(
                provider_result.pop("_address_total", 0) or 0
            )
            address_total = (
                len(ranked_locations)
                if candidates_are_complete
                else max(raw_address_total, len(ranked_locations))
            )
            provider_result["_selected_locations"] = ranked_locations[
                :NPI_SEARCH_ADDRESS_DEFAULT_LIMIT
            ]
            provider_result["_selected_location_total"] = address_total

        selected_location_keys = sorted(
            {
                identity.removeprefix("location:")
                for provider_result in provider_results
                for location in provider_result.get("_selected_locations", [])
                for identity in (
                    list(location.get("_base_row_identities") or [])
                    + (
                        [f"location:{location.get('location_key')}" ]
                        if location.get("location_key") not in (None, "")
                        else []
                    )
                )
                if str(identity).startswith("location:")
                and identity.removeprefix("location:")
            }
        )
        hydrated_by_location_key: dict[str, dict[str, Any]] = {}
        if is_unified_search and selected_location_keys:
            try:
                async with db.acquire() as hydration_conn:
                    hydrated_rows = await hydration_conn.all(
                        text(
                            f"SELECT c.* FROM {address_table_sql} AS c "
                            "WHERE c.location_key = ANY(:location_keys)"
                        ),
                        location_keys=selected_location_keys,
                    )
                allowed_hydrated_fields = {
                    column.key for column in NPIAddress.__table__.columns
                } | PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS | {
                    "inferred_npi",
                    "location_key",
                    "premise_key",
                    "source_record_ids",
                }
                for hydrated_row in hydrated_rows:
                    hydrated_mapping = getattr(
                        hydrated_row,
                        "_mapping",
                        hydrated_row,
                    )
                    if not isinstance(hydrated_mapping, Mapping):
                        continue
                    location_key = str(
                        hydrated_mapping.get("location_key") or ""
                    )
                    if not location_key:
                        continue
                    hydrated_address_map = {
                        field_name: hydrated_mapping.get(field_name)
                        for field_name in allowed_hydrated_fields
                        if field_name in hydrated_mapping
                    }
                    if hydrated_address_map.get("npi") is None:
                        hydrated_address_map["npi"] = hydrated_mapping.get(
                            "inferred_npi"
                        )
                    _attach_public_address_site_key(
                        hydrated_address_map,
                        hydrated_mapping,
                    )
                    hydrated_by_location_key[location_key] = (
                        _add_canonical_contact_fields_to_address(
                            hydrated_address_map
                        )
                    )
            except Exception as exc:
                logger.warning(
                    "NPI search location hydration failed; returning ranked candidates (%s)",
                    exc,
                )

        for provider_result in provider_results:
            selected_locations = provider_result.pop(
                "_selected_locations",
                [],
            )
            locations: list[dict[str, Any]] = []
            for selected_location in selected_locations:
                selected_identity_keys = {
                    str(identity).removeprefix("location:")
                    for identity in (
                        list(selected_location.get("_base_row_identities") or [])
                        + (
                            [f"location:{selected_location.get('location_key')}" ]
                            if selected_location.get("location_key") not in (None, "")
                            else []
                        )
                    )
                    if str(identity).startswith("location:")
                }
                hydrated_locations = [
                    hydrated_by_location_key[location_key]
                    for location_key in sorted(selected_identity_keys)
                    if location_key in hydrated_by_location_key
                ]
                if not hydrated_locations:
                    locations.append(selected_location)
                    continue
                merged_location_map = dict(selected_location)
                for hydrated_location in hydrated_locations:
                    _merge_duplicate_address(
                        merged_location_map,
                        hydrated_location,
                    )
                merged_location_map["location_status"] = selected_location.get(
                    "location_status",
                    "unknown",
                )
                locations.append(merged_location_map)
            address_total = int(
                provider_result.pop("_selected_location_total", len(locations))
            )
            provider_result["address_list"] = locations
            provider_result["address_pagination"] = {
                "limit": NPI_SEARCH_ADDRESS_DEFAULT_LIMIT,
                "offset": 0,
                "returned": len(locations),
                "total": address_total,
                "has_more": address_total > len(locations),
            }
            provider_result.pop("_taxonomy_identities", None)
            if locations:
                provider_result.update(locations[0])
            provider_result["location_status"] = locations[0].get(
                "location_status", "unknown"
            ) if locations else "unknown"
            _add_canonical_contact_fields_to_address(provider_result)
            _redact_internal_address_fields(provider_result)
        return provider_results, provider_total, summary_map

    if is_count_only:
        count_rows = await get_count(filters_by_name)
        return response.json({"rows": count_rows}, default=str)

    async def _count_with_timeout(*, allow_inline_total: bool = True) -> Optional[int]:
        if not include_total:
            return None
        if allow_inline_total and inline_name_taxonomy_total:
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
                get_count(filters_by_name),
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
        and not is_count_only
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
                provider_sex_code,
                plan_network_ids,
                name_like_values,
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

    summary_map: dict[int, dict[str, Any]] = {}
    if use_sitemap_fast_path:
        result_rows = await get_sitemap_results(start, limit, "Pharmacy")
        raw_total = None if not include_total else await _count_with_timeout()
    else:
        raw_total, result_payload = await asyncio.gather(
            _count_with_timeout(),
            get_results(start, limit, filters_by_name),
        )
        result_rows, inline_total, summary_map = result_payload
        if inline_total is not None:
            raw_total = inline_total
        elif inline_name_taxonomy_total:
            raw_total = await _count_with_timeout(allow_inline_total=False)
    if include_total and raw_total is not None:
        total = int(raw_total)
        total_source = "computed"
    elif include_total:
        total = pagination.offset + len(result_rows)
        total_source = "estimated_timeout_floor"
    else:
        total = pagination.offset + len(result_rows)
        total_source = "estimated_page_floor"
    if view_mode != "card" and use_sitemap_fast_path:
        try:
            summary_map = await _fetch_provider_enrichment_summary_map(
                [
                    provider_result.get("npi")
                    for provider_result in result_rows
                    if isinstance(provider_result, dict)
                ],
                include_chain=include_chain_enrichment,
                session=request_session,
            )
        except Exception as exc:
            logger.debug("Provider enrichment summary fetch failed: %s", exc)
    if summary_map:
        for provider_result in result_rows:
            if not isinstance(provider_result, dict):
                continue
            npi_value = provider_result.get("npi")
            if npi_value is None:
                continue
            summary = summary_map.get(int(npi_value))
            if summary:
                provider_result["provider_enrichment_summary"] = summary
    if view_mode != "card" and (include_sources or include_evidence):
        source_detail_targets = list(result_rows)
        source_detail_targets.extend(
            location
            for provider_result in result_rows
            if isinstance(provider_result, dict)
            for location in (provider_result.get("address_list") or [])
            if isinstance(location, dict)
        )
        await _attach_selected_address_source_details(
            source_detail_targets,
            include_sources=include_sources,
            include_role_evidence=include_evidence,
            session=request_session,
        )
    if not include_evidence:
        for provider_result in result_rows:
            if not isinstance(provider_result, dict):
                continue
            provider_result.pop("source_record_ids", None)
            for location in provider_result.get("address_list") or []:
                if isinstance(location, dict):
                    location.pop("source_record_ids", None)
    for provider_result in result_rows:
        if not isinstance(provider_result, dict):
            continue
        public_locations = [provider_result]
        public_locations.extend(
            location
            for location in (provider_result.get("address_list") or [])
            if isinstance(location, dict)
        )
        for location in public_locations:
            location.pop("_base_row_identities", None)
            if not (include_sources or include_evidence):
                location.pop("location_key", None)
            if not include_evidence:
                location.pop("inferred_npi", None)

    if view_mode == "card":
        result_rows = [
            _provider_card_from_mapping(provider_result)
            for provider_result in result_rows
            if isinstance(provider_result, Mapping)
        ]

    response_map: dict[str, Any] = {
        "total": total,
        "page": pagination.page,
        "limit": pagination.limit,
        "offset": pagination.offset,
        "rows": result_rows,
        "total_source": total_source,
    }
    if requested_procedure_codes or requested_medication_codes:
        response_map["query"] = {
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
        response_map,
        default=str,
    )


get_all = list_providers
get_all.__name__ = "get_all"


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
    include_specialty_stats = _is_truthy_arg(request.args.get("include_specialty_stats"), default=True)

    if ccn is None and organization_name is None:
        raise sanic.exceptions.InvalidUsage("At least one facility locator is required: ccn or organization_name")

    table_name = enrollment_model.__tablename__
    if not await _is_table_available(table_name, session=request_session):
        response_map: dict[str, Any] = {
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
            response_map["specialty_stats"] = []
        return response.json(response_map, default=str)

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
    query_parameter_map: dict[str, Any] = {}
    if ccn:
        where_clauses.append(f"UPPER(REPLACE({facility_ccn_expr}, ' ', '')) = :ccn")
        query_parameter_map["ccn"] = ccn
    if organization_name:
        where_clauses.append(
            "LOWER(COALESCE(h.organization_name, '') || ' ' || COALESCE(h.doing_business_as_name, '')) "
            "LIKE :organization_name"
        )
        query_parameter_map["organization_name"] = f"%{organization_name.lower()}%"
    if city:
        where_clauses.append("UPPER(COALESCE(h.city, '')) = :city")
        query_parameter_map["city"] = city.upper()
    if state:
        where_clauses.append("UPPER(COALESCE(h.state, '')) = :state")
        query_parameter_map["state"] = state
    if reporting_year is not None:
        where_clauses.append("h.reporting_year = :reporting_year")
        query_parameter_map["reporting_year"] = reporting_year
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

    execute_parameter_map = dict(query_parameter_map)
    execute_parameter_map["limit"] = limit
    execute_parameter_map["offset"] = offset
    execute_parameter_map["stats_limit"] = stats_limit

    if request_session is not None:
        session = request_session
        total_result = await session.execute(total_sql, query_parameter_map)
        facility_result = await session.execute(facilities_sql, query_parameter_map)
        providers_result = await session.execute(providers_sql, execute_parameter_map)
        specialty_result = (
            await session.execute(specialty_sql, execute_parameter_map) if include_specialty_stats else None
        )
    else:
        async with db.session() as session:
            total_result = await session.execute(total_sql, query_parameter_map)
            facility_result = await session.execute(facilities_sql, query_parameter_map)
            providers_result = await session.execute(providers_sql, execute_parameter_map)
            specialty_result = (
                await session.execute(specialty_sql, execute_parameter_map) if include_specialty_stats else None
            )

    total_row = total_result.first()
    total_providers = int((total_row._mapping.get("total_providers") if total_row else 0) or 0)

    matched_facilities = [
        {
            "ccn": facility_record._mapping.get("facility_ccn"),
            "organization_name": facility_record._mapping.get("organization_name"),
            "doing_business_as_name": facility_record._mapping.get("doing_business_as_name"),
            "city": facility_record._mapping.get("city"),
            "state": facility_record._mapping.get("state"),
            "provider_count": int(facility_record._mapping.get("provider_count") or 0),
        }
        for facility_record in facility_result.all()
    ]

    providers = []
    for provider_record in providers_result.all():
        mapping = provider_record._mapping
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

    response_map: dict[str, Any] = {
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
        response_map["specialty_stats"] = [
            {
                "specialty": specialty_record._mapping.get("specialty"),
                "classification": specialty_record._mapping.get("classification"),
                "provider_count": int(specialty_record._mapping.get("provider_count") or 0),
            }
            for specialty_record in specialty_result.all()
        ]

    return response.json(response_map, default=str)


def _populate_near_provider_mapping(
    provider_by_field: dict[str, Any],
    row_dict: Mapping[str, Any],
    npi_value: int,
    address_key_value: Any,
    address_table_sql: str,
) -> None:
    """Populate one newly seen keyed geo provider row."""

    if row_dict.get("distance") is not None:
        provider_by_field["distance"] = row_dict.get("distance")
    provider_by_field["_cursor_distance_meters"] = row_dict.get(
        "cursor_distance_meters"
    )
    provider_by_field["_cursor_npi"] = npi_value
    provider_by_field["_cursor_address_key"] = str(address_key_value)
    for column in NPIAddress.__table__.columns:
        if column.key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS and column.key in row_dict:
            provider_by_field[column.key] = row_dict[column.key]
    _attach_public_address_site_key(provider_by_field, row_dict)
    if address_table_sql.endswith(".entity_address_unified"):
        for key in PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS:
            if key in row_dict and key not in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                provider_by_field[key] = row_dict[key]
    excluded_npi_columns = {
        "npi",
        "checksum",
        "do_business_as_text",
        *PUBLIC_NPI_EXCLUDED_COLUMNS,
    }
    for column in NPIData.__table__.columns:
        if column.key not in excluded_npi_columns and column.key in row_dict:
            provider_by_field[column.key] = row_dict[column.key]
    provider_by_field["npi"] = npi_value


@blueprint.get("/near/")
async def get_near_npi(request):
    """Return providers near coordinates under optional taxonomy filters."""
    request_session = _request_session(request)
    request.args.get("view")
    view_mode = str(request.args.get("view") or "").strip().lower()
    if view_mode not in {"", "card"}:
        raise sanic.exceptions.InvalidUsage("view must be: card")
    in_long, in_lat = None, None
    if request.args.get("long"):
        in_long = float(request.args.get("long"))
    if request.args.get("lat"):
        in_lat = float(request.args.get("lat"))

    codes = request.args.get("codes")
    if codes:
        codes = [x.strip() for x in codes.split(",")]

    plan_network_ids = request.args.get("plan_network")
    if plan_network_ids:
        plan_network_ids = [int(x) for x in plan_network_ids.split(",")]
    classification = request.args.get("classification")
    specialization = request.args.get("specialization")
    section = request.args.get("section")
    display_name = request.args.get("display_name")
    procedure_codes_raw = request.args.get("procedure_codes")
    procedure_code_system_raw = request.args.get("procedure_code_system")
    medication_codes_raw = request.args.get("medication_codes")
    medication_code_system_raw = request.args.get("medication_code_system")
    year_raw = request.args.get("year")
    provider_sex_code = normalize_provider_sex_code(
        request.args.get("provider_sex_code")
    )
    request.args.get("q")
    if _extract_name_filters(request):
        raise sanic.exceptions.InvalidUsage(
            "name_like is no longer supported on /npi/near/; use q"
        )
    name_query = str(request.args.get("q") or "").strip()
    exclude_npi = int(request.args.get("exclude_npi", 0))
    limit = int(request.args.get("limit", 5))
    if limit < 1:
        raise sanic.exceptions.InvalidUsage("limit must be at least 1")
    include_total = _is_truthy_arg(request.args.get("include_total"), default=False)
    cursor_raw = str(request.args.get("cursor") or "").strip()
    pagination_requested = include_total or bool(cursor_raw)
    cursor_scope = _nearby_cursor_scope(request.args)
    initial_cursor = (
        _decode_nearby_cursor(cursor_raw, cursor_scope) if cursor_raw else None
    )
    zip_codes = []
    for zip_c in request.args.get("zip_codes", "").split(","):
        if not zip_c:
            continue
        zip_codes.append(
            _normalize_zip_code(zip_c.strip().rjust(5, "0"), "zip_codes")
        )
    has_coordinates = in_long is not None and in_lat is not None
    radius = _normalize_match_candidate_float(
        request.args.get("radius"),
        param_name="radius",
        minimum=0.0,
        maximum=_MATCH_CANDIDATES_MAX_RADIUS_MILES,
    )
    if radius is None:
        radius = 25.0 if zip_codes and not has_coordinates else 10.0

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
        if pagination_requested:
            return response.json(
                {
                    "items": [],
                    "total_count": 0,
                    "next_cursor": None,
                    "has_more": False,
                    "result_identity": ["npi", "address_key"],
                },
                default=str,
            )
        return response.json([], default=str)

    capability_by_name = {
        "npi_procedures_array_available": True,
        "npi_medications_array_available": True,
        "pricing_provider_procedure_available": False,
        "pricing_provider_prescription_available": False,
    }
    if requested_procedure_codes or requested_medication_codes:
        if request_session is not None:
            capability_by_name = await _resolve_npi_filter_capabilities(session=request_session)
        else:
            capability_by_name = await _resolve_npi_filter_capabilities()

    _validate_section_filters(section, classification, codes)
    # If only zip was provided, resolve to coordinates first using a separate connection.
    if not has_coordinates and zip_codes and zip_codes[0]:
        zip_sql = "select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"
        async with db.acquire() as conn_zip:
            for coordinate_record in await conn_zip.all(text(zip_sql), zip_code=zip_codes[0]):
                try:
                    in_long = float(coordinate_record["intptlon"])
                    in_lat = float(coordinate_record["intptlat"])
                except Exception:
                    in_lat = float(coordinate_record[0])
                    in_long = float(coordinate_record[1])

    address_table_sql = await _address_serving_table_sql(
        _public_address_serving_column_keys(),
        session=request_session,
    )

    providers_by_identity: OrderedDict[tuple[int, str], dict[str, Any]] = OrderedDict()
    extra_filters: list[str] = []
    if exclude_npi:
        extra_filters.append("a.npi <> :exclude_npi")
    if plan_network_ids:
        extra_filters.append("a.plans_network_array && (:plan_network_array)")
    if provider_sex_code is not None:
        extra_filters.append(
            "EXISTS ("
            "SELECT 1 FROM mrf.npi AS sex_provider "
            "WHERE sex_provider.npi = a.npi "
            "AND sex_provider.provider_sex_code = :provider_sex_code"
            ")"
        )
    dynamic_code_parameters_by_name: dict[str, int] = {}
    if filter_year is not None and (procedure_internal_codes or medication_internal_codes):
        dynamic_code_parameters_by_name["filter_year"] = int(filter_year)

    procedures_array_available = bool(capability_by_name.get("npi_procedures_array_available", True))
    medications_array_available = bool(capability_by_name.get("npi_medications_array_available", True))
    procedure_table_available = bool(capability_by_name.get("pricing_provider_procedure_available", False))
    medication_table_available = bool(capability_by_name.get("pricing_provider_prescription_available", False))

    for idx, code in enumerate(procedure_internal_codes):
        param = f"procedure_code_{idx}"
        dynamic_code_parameters_by_name[param] = int(code)
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
        dynamic_code_parameters_by_name[param] = int(code)
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

    taxonomy_clauses: list[str] = []
    if zip_codes:
        extra_filters.append(_address_zip5_filter("a", address_table_sql, any_array=True))

    if classification:
        taxonomy_clauses.append("classification = :classification")
    if specialization:
        taxonomy_clauses.append("specialization = :specialization")
    if section:
        taxonomy_clauses.append("section = :section")
    if display_name:
        taxonomy_clauses.append("display_name = :display_name")
    if codes:
        taxonomy_clauses.append("code = ANY(:codes)")
    ilike_clause = ""
    q_like = None
    if name_query:
        q_like = f"%{name_query}%"
        ilike_clause = f"\n            AND {_name_like_clause('d', 'q')}"

    taxonomy_conditions = " AND ".join(taxonomy_clauses) if taxonomy_clauses else "1=1"
    extra_clause = ""
    if extra_filters:
        extra_clause = "\n          AND " + "\n          AND ".join(extra_filters)

    query_parameters_by_name: dict[str, Any] = {
        "in_long": in_long,
        "in_lat": in_lat,
        "classification": classification,
        "specialization": specialization,
        "radius": radius,
        "exclude_npi": exclude_npi,
        "section": section,
        "display_name": display_name,
        "q": q_like,
        "codes": codes,
        "zip_codes": zip_codes,
        "plan_network_array": plan_network_ids,
        "provider_sex_code": provider_sex_code,
        **dynamic_code_parameters_by_name,
    }

    async def fetch_nearby_rows() -> list[Any]:
        """Fetch enough KNN rows to page unique provider-address identities."""
        target_identities = limit + 1 if pagination_requested else limit
        candidate_limit = max((target_identities + 1) * 4, 16)
        batch_cursor = initial_cursor
        collected_rows: list[Any] = []
        collected_identities: set[tuple[int, str]] = set()
        async with db.acquire() as conn:
            for _batch_number in range(100):
                cursor_clause, cursor_parameters_by_name = _nearby_cursor_filter(
                    batch_cursor
                )
                batch_parameters_by_name = {
                    **query_parameters_by_name,
                    **cursor_parameters_by_name,
                }
                nearby_sql = _build_nearby_sql(
                    taxonomy_conditions,
                    extra_clause,
                    ilike_clause,
                    use_taxonomy_filter=bool(taxonomy_clauses),
                    address_table_sql=address_table_sql,
                    geo_precision_clause=_exact_geo_precision_clause(address_table_sql),
                    cursor_clause=cursor_clause,
                )
                batch_rows = await conn.all(
                    text(nearby_sql),
                    limit=candidate_limit,
                    **batch_parameters_by_name,
                )
                if not batch_rows:
                    break
                collected_rows.extend(batch_rows)
                last_cursor = None
                for batch_row in batch_rows:
                    mapping = getattr(batch_row, "_mapping", None)
                    if mapping is None:
                        continue
                    npi_value = mapping.get("npi_code") or mapping.get("npi")
                    address_key_value = mapping.get("address_key")
                    distance_value = mapping.get("cursor_distance_meters")
                    if npi_value is None or address_key_value is None:
                        continue
                    identity = (int(npi_value), str(address_key_value).lower())
                    collected_identities.add(identity)
                    if distance_value is not None:
                        last_cursor = (
                            float(distance_value),
                            int(npi_value),
                            str(address_key_value),
                        )

                if len(collected_identities) >= target_identities:
                    break
                if last_cursor is None or last_cursor == batch_cursor:
                    break
                batch_cursor = last_cursor

        return collected_rows

    bbox_parameters_by_name: dict[str, float] = {}
    bbox_clause = ""
    if in_long is not None and in_lat is not None:
        delta_lat = radius / 69.0
        cos_lat = math.cos(math.radians(in_lat)) or 1e-6
        delta_long = radius / (69.0 * cos_lat)
        bbox_parameters_by_name = {
            "min_lat": in_lat - delta_lat,
            "max_lat": in_lat + delta_lat,
            "min_long": in_long - delta_long,
            "max_long": in_long + delta_long,
        }
        bbox_clause = dedent(
            """

               AND a.lat BETWEEN :min_lat AND :max_lat
               AND a.long BETWEEN :min_long AND :max_long
            """
        ).rstrip()

    async def fetch_exact_total() -> int:
        """Count exact provider-address matches without the page limit."""

        count_sql = _build_nearby_count_sql(
            taxonomy_conditions,
            extra_clause,
            ilike_clause,
            use_taxonomy_filter=bool(taxonomy_clauses),
            address_table_sql=address_table_sql,
            geo_precision_clause=_exact_geo_precision_clause(address_table_sql),
            bbox_clause=bbox_clause,
        )
        async with db.acquire() as conn:
            rows = await conn.all(
                text(count_sql),
                **query_parameters_by_name,
                **bbox_parameters_by_name,
            )
        if not rows:
            return 0
        mapping = getattr(rows[0], "_mapping", None)
        value = mapping.get("total_count") if mapping is not None else rows[0][0]
        return int(value or 0)

    if pagination_requested:
        res_q, total_count = await asyncio.gather(
            fetch_nearby_rows(),
            fetch_exact_total(),
        )
    else:
        res_q = await fetch_nearby_rows()
        total_count = None

    for provider_record in res_q:
        row_mapping = getattr(provider_record, "_mapping", None)
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
            address_key_value = row_dict.get("address_key")
            if address_key_value is None:
                continue
            identity = (npi_value, str(address_key_value).lower())
            is_new_identity = identity not in providers_by_identity
            provider_by_field = providers_by_identity.get(identity, {"taxonomy_list": []})
            if is_new_identity:
                _populate_near_provider_mapping(
                    provider_by_field,
                    row_dict,
                    npi_value,
                    address_key_value,
                    address_table_sql,
                )

            taxonomy_by_field = {}
            for column in NPIDataTaxonomy.__table__.columns:
                if column.key in ("npi", "checksum"):
                    continue
                if column.key in row_dict:
                    taxonomy_by_field[column.key] = row_dict[column.key]
            if taxonomy_by_field and row_dict.get("taxonomy_display") is not None:
                taxonomy_by_field["display"] = row_dict.get("taxonomy_display")
            if taxonomy_by_field and taxonomy_by_field not in provider_by_field["taxonomy_list"]:
                provider_by_field["taxonomy_list"].append(taxonomy_by_field)

            providers_by_identity[identity] = provider_by_field
            continue

        # Fallback for positional row types. Keep this defensive to avoid crashes
        # when result shape differs from model column expectations.
        row_len = len(provider_record)
        if row_len <= 1:
            continue

        provider_by_field = {"taxonomy_list": []}
        count = 1
        provider_by_field["distance"] = provider_record[count]

        for column in NPIAddress.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if column.key in PUBLIC_ADDRESS_EXCLUDED_COLUMNS:
                continue
            provider_by_field[column.key] = provider_record[count]
        for column in NPIData.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if column.key in {
                "npi",
                "checksum",
                "do_business_as_text",
                *PUBLIC_NPI_EXCLUDED_COLUMNS,
            }:
                continue
            provider_by_field[column.key] = provider_record[count]

        npi_value = provider_by_field.get("npi")
        if npi_value is None:
            continue
        address_key_value = provider_by_field.get("address_key")
        identity = (int(npi_value), str(address_key_value or "").lower())
        if identity in providers_by_identity:
            provider_by_field = providers_by_identity[identity]
        taxonomy_by_field = {}
        for column in NPIDataTaxonomy.__table__.columns:
            count += 1
            if count >= row_len:
                break
            if column.key in ("npi", "checksum"):
                continue
            taxonomy_by_field[column.key] = provider_record[count]
        if taxonomy_by_field and taxonomy_by_field not in provider_by_field["taxonomy_list"]:
            provider_by_field["taxonomy_list"].append(taxonomy_by_field)

        providers_by_identity[identity] = provider_by_field

    all_provider_results = list(providers_by_identity.values())
    has_more = pagination_requested and len(all_provider_results) > limit
    provider_results = all_provider_results[:limit]
    next_cursor = None
    if has_more and provider_results:
        final_result = provider_results[-1]
        distance_value = final_result.get("_cursor_distance_meters")
        npi_value = final_result.get("_cursor_npi")
        address_key_value = final_result.get("_cursor_address_key")
        if distance_value is not None and npi_value is not None and address_key_value:
            next_cursor = _encode_nearby_cursor(
                cursor_scope,
                float(distance_value),
                int(npi_value),
                str(address_key_value),
            )
    for provider_result in provider_results:
        if isinstance(provider_result, dict):
            provider_result.pop("_cursor_distance_meters", None)
            provider_result.pop("_cursor_npi", None)
            provider_result.pop("_cursor_address_key", None)
            _add_canonical_contact_fields_to_address(provider_result)
    _redact_internal_address_fields(provider_results)
    if view_mode == "card":
        provider_results = [
            _provider_card_from_mapping(provider_result)
            for provider_result in provider_results
        ]
    if pagination_requested:
        return response.json(
            {
                "items": provider_results,
                "total_count": int(total_count or 0),
                "next_cursor": next_cursor,
                "has_more": bool(has_more),
                "result_identity": ["npi", "address_key"],
            },
            default=str,
        )
    return response.json(provider_results, default=str)


@blueprint.get("/id/<npi>/full_taxonomy")
async def get_full_taxonomy_list(_request, npi):
    """Return all NUCC taxonomy details attached to one NPI."""
    taxonomy_rows = []
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
        taxonomy_rows.append(payload)
    return response.json(taxonomy_rows)


@dataclass(frozen=True)
class _ProviderProfileQuery:
    include_evidence: bool
    include_sensitive: bool
    page_category: str | None
    requested_generation_id: str | None
    page_limit: int
    page_offset: int
    requested_categories: tuple[str, ...]


class _ProviderProfileQueryError(ValueError):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        status: int = 400,
        **details_by_key: Any,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.response_by_key = {
            "error": error_code,
            "message": message,
            **details_by_key,
        }


def _provider_profile_error_response(
    error_code: str,
    message: str,
    *,
    status: int = 400,
    **details_by_key: Any,
) -> Any:
    return response.json(
        {"error": error_code, "message": message, **details_by_key},
        status=status,
    )


def _provider_profile_generation_id(
    request_args: Any,
) -> str | None:
    generation_id = _normalize_text_filter(
        request_args.get("generation_id"),
        param_name="generation_id",
        max_length=64,
    )
    if generation_id is None:
        return None
    generation_id = generation_id.lower()
    if re.fullmatch(r"[0-9a-f]{64}", generation_id) is None:
        raise _ProviderProfileQueryError(
            "invalid_profile_generation_id",
            "generation_id must be a 64-character hexadecimal value",
        )
    return generation_id


def _assert_provider_profile_query_scope(
    request_args: Any,
    page_category: str | None,
    requested_generation_id: str | None,
    raw_categories: Any,
) -> None:
    if page_category and raw_categories:
        raise _ProviderProfileQueryError(
            "conflicting_profile_parameters",
            "category and categories cannot be used together",
        )
    has_page_window = any(
        request_args.get(parameter_name) not in (None, "", "null")
        for parameter_name in ("limit", "offset")
    )
    if has_page_window and page_category is None:
        raise _ProviderProfileQueryError(
            "profile_category_required",
            "limit and offset require the category parameter",
        )
    if requested_generation_id and page_category is None:
        raise _ProviderProfileQueryError(
            "profile_category_required",
            "generation_id requires the category parameter",
        )


def _provider_profile_requested_categories(
    raw_categories: Any,
    page_category: str | None,
) -> tuple[str, ...]:
    requested_categories = (
        (page_category,)
        if page_category
        else tuple(
            field_value.strip()
            for field_value in str(raw_categories).split(",")
            if field_value.strip()
        )
        if raw_categories
        else tuple(STANDARD_CATEGORIES)
    )
    unknown_categories = sorted(
        set(requested_categories) - set(STANDARD_CATEGORIES)
    )
    if unknown_categories:
        raise _ProviderProfileQueryError(
            "invalid_profile_categories",
            "unknown provider profile categories",
            unknown_categories=unknown_categories,
            allowed_categories=list(STANDARD_CATEGORIES),
        )
    return requested_categories


def _provider_profile_query_from_args(
    request_args: Any,
) -> _ProviderProfileQuery:
    page_category = _normalize_text_filter(
        request_args.get("category"),
        param_name="category",
        max_length=64,
    )
    requested_generation_id = _provider_profile_generation_id(
        request_args,
    )
    raw_categories = request_args.get("categories")
    _assert_provider_profile_query_scope(
        request_args,
        page_category,
        requested_generation_id,
        raw_categories,
    )
    return _ProviderProfileQuery(
        include_evidence=_is_truthy_arg(
            request_args.get("include_evidence"),
            default=False,
        ),
        include_sensitive=_is_truthy_arg(
            request_args.get("include_sensitive"),
            default=False,
        ),
        page_category=page_category,
        requested_generation_id=requested_generation_id,
        page_limit=_parse_bounded_int(
            request_args.get("limit"),
            param_name="limit",
            default=25,
            minimum=1,
            maximum=50,
        ),
        page_offset=_parse_bounded_int(
            request_args.get("offset"),
            param_name="offset",
            default=0,
            minimum=0,
            maximum=1_000_000,
        ),
        requested_categories=_provider_profile_requested_categories(
            raw_categories,
            page_category,
        ),
    )


def _compose_requested_provider_profile(
    normalized_npi: int,
    state_projection: Mapping[str, Any] | None,
    fhir_record_by_key: Mapping[str, Any] | None,
    query: _ProviderProfileQuery,
) -> dict[str, Any] | None:
    profile_by_key = compose_provider_profile(
        normalized_npi,
        state_projection=state_projection,
        fhir_profile=(
            fhir_record_by_key.get("profile")
            if fhir_record_by_key
            else None
        ),
        requested_categories=list(query.requested_categories),
        include_sensitive=query.include_sensitive,
        page_category=query.page_category,
        page_limit=query.page_limit,
        page_offset=query.page_offset,
    )
    if profile_by_key is None:
        return None
    fhir_profile_by_key = (
        fhir_record_by_key.get("profile")
        if fhir_record_by_key
        and isinstance(fhir_record_by_key.get("profile"), Mapping)
        else None
    )
    if (
        isinstance(fhir_profile_by_key, Mapping)
        and "profile_as_of" in fhir_profile_by_key
    ):
        profile_by_key = dict(profile_by_key)
        profile_by_key["profile_as_of"] = (
            _serialize_provider_directory_profile_as_of(
                fhir_profile_by_key.get("profile_as_of")
            )
        )
    return profile_by_key


def _provider_profile_generation_error(
    profile_by_key: Mapping[str, Any],
    requested_generation_id: str | None,
) -> Any | None:
    if (
        requested_generation_id is None
        or profile_by_key["generation_id"] == requested_generation_id
    ):
        return None
    return _provider_profile_error_response(
        "provider_profile_generation_changed",
        "The provider profile changed; restart category pagination.",
        status=409,
        requested_generation_id=requested_generation_id,
        current_generation_id=profile_by_key["generation_id"],
    )


def _provider_profile_response_by_key(
    normalized_npi: int,
    state_projection: Mapping[str, Any] | None,
    fhir_record_by_key: Mapping[str, Any] | None,
    profile_by_key: dict[str, Any],
    query: _ProviderProfileQuery,
) -> dict[str, Any]:
    response_by_key: dict[str, Any] = {
        "npi": normalized_npi,
        "provider_profile": profile_by_key,
    }
    if not query.include_evidence:
        return response_by_key
    evidence_by_key = compose_provider_profile_evidence(
        state_projection=state_projection,
        fhir_evidence=(
            fhir_record_by_key.get("evidence")
            if fhir_record_by_key
            else None
        ),
        provider_profile=profile_by_key,
        page_category=query.page_category,
    )
    if evidence_by_key is None:
        return response_by_key
    if "profile_as_of" in profile_by_key:
        evidence_by_key = dict(evidence_by_key)
        evidence_by_key["profile_as_of"] = profile_by_key["profile_as_of"]
    response_by_key["provider_profile_evidence"] = evidence_by_key
    return response_by_key


@blueprint.get("/id/<npi>/profile")
async def get_provider_profile(request, npi):
    """Return one categorized profile composed across reviewed public sources."""
    if not profile_artifact.is_valid_npi(npi):
        return _provider_profile_error_response(
            "invalid_npi",
            "npi must be a valid 10-digit National Provider Identifier",
        )
    query_args_by_name = {
        "categories": request.args.get("categories"),
        "category": request.args.get("category"),
        "generation_id": request.args.get("generation_id"),
        "include_evidence": request.args.get("include_evidence"),
        "include_sensitive": request.args.get("include_sensitive"),
        "limit": request.args.get("limit"),
        "offset": request.args.get("offset"),
    }
    try:
        query = _provider_profile_query_from_args(query_args_by_name)
    except _ProviderProfileQueryError as exc:
        return response.json(exc.response_by_key, status=exc.status)
    normalized_npi = int(npi)
    state_projection, fhir_profile_map = await asyncio.gather(
        fetch_state_profile_projection(normalized_npi),
        _fetch_provider_directory_profile_map(
            [normalized_npi],
            include_evidence=query.include_evidence,
        ),
    )
    fhir_record_by_key = fhir_profile_map.get(normalized_npi)
    profile_by_key = _compose_requested_provider_profile(
        normalized_npi,
        state_projection,
        fhir_record_by_key,
        query,
    )
    if profile_by_key is None:
        return _provider_profile_error_response(
            "provider_profile_not_found",
            "No reviewed provider profile facts are available for this NPI.",
            status=404,
            npi=normalized_npi,
        )
    generation_error = _provider_profile_generation_error(
        profile_by_key,
        query.requested_generation_id,
    )
    if generation_error is not None:
        return generation_error
    return response.json(
        _provider_profile_response_by_key(
            normalized_npi,
            state_projection,
            fhir_record_by_key,
            profile_by_key,
            query,
        )
    )


@blueprint.get("/id/<npi>/provider-directory-observations")
async def get_provider_directory_observations(request, npi):
    """Return retained, non-certified Provider Directory observations for one NPI."""
    if not profile_artifact.is_valid_npi(npi):
        return response.json(
            {
                "error": "invalid_npi",
                "message": "npi must be a valid 10-digit National Provider Identifier",
            },
            status=400,
        )
    normalized_npi = int(npi)
    return response.json(
        {
            "npi": normalized_npi,
            "completeness": "best_effort",
            "certified": False,
            "observations": await _fetch_provider_directory_observations(
                normalized_npi,
                session=_request_session(request),
            ),
        },
        default=str,
    )


@blueprint.get("/plans_by_npi/<npi>")
async def get_plans_by_npi(_request, npi):
    """Return issuer plan links recorded for one NPI."""

    npi_plan_rows = []
    plan_rows = []
    issuer_rows = []
    npi = int(npi)

    query = (
        db.select(PlanNPIRaw, Issuer)
        .where(Issuer.issuer_id == PlanNPIRaw.issuer_id)
        .where(PlanNPIRaw.npi == npi)
        .order_by(PlanNPIRaw.issuer_id.desc())
    )

    async for plan_raw, issuer in query.iterate():
        npi_plan_rows.append({"npi_info": plan_raw.to_json_dict(), "issuer_info": issuer.to_json_dict()})

    return response.json({"npi_data": npi_plan_rows, "plan_data": plan_rows, "issuer_data": issuer_rows})


def _normalize_npi_batch_npis(raw_npis: Any) -> list[int]:
    """Validate and normalize the ordered NPI list."""
    if not isinstance(raw_npis, list) or not 1 <= len(raw_npis) <= NPI_BATCH_MAX_SIZE:
        raise sanic.exceptions.InvalidUsage(
            f"npis must contain between 1 and {NPI_BATCH_MAX_SIZE} values"
        )
    normalized_npis: list[int] = []
    for raw_npi in raw_npis:
        if isinstance(raw_npi, bool):
            npi_text = ""
        elif isinstance(raw_npi, (str, int)):
            npi_text = str(raw_npi).strip()
        else:
            npi_text = ""
        if len(npi_text) != 10 or not npi_text.isascii() or not npi_text.isdigit():
            raise sanic.exceptions.InvalidUsage("each npi must be a 10-digit numeric value")
        normalized_npis.append(int(npi_text))
    if len(set(normalized_npis)) != len(normalized_npis):
        raise sanic.exceptions.InvalidUsage("npis must be unique")
    return normalized_npis


def _bounded_npi_batch_integer(
    raw_body: Mapping[str, Any],
    field_name: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    """Read one bounded integer option without accepting booleans."""
    field_value = raw_body.get(field_name, default)
    if (
        isinstance(field_value, bool)
        or not isinstance(field_value, int)
        or not minimum <= field_value <= maximum
    ):
        raise sanic.exceptions.InvalidUsage(
            f"{field_name} must be an integer between {minimum} and {maximum}"
        )
    return field_value


def _npi_batch_boolean_map(raw_body: Mapping[str, Any]) -> dict[str, bool]:
    boolean_option_map: dict[str, bool] = {}
    for option_name in ("include_sources", "include_evidence"):
        option_value = raw_body.get(option_name, False)
        if not isinstance(option_value, bool):
            raise sanic.exceptions.InvalidUsage(f"{option_name} must be a boolean")
        boolean_option_map[option_name] = option_value
    return boolean_option_map


def _normalize_npi_batch_request(raw_body: Any) -> dict[str, Any]:
    """Validate the bounded, shared-option provider batch contract."""
    if not isinstance(raw_body, Mapping):
        raise sanic.exceptions.InvalidUsage("request body must be a JSON object")
    allowed_fields = {
        "npis",
        "address_limit",
        "address_offset",
        "include_sources",
        "include_evidence",
    }
    unknown_fields = sorted(set(raw_body) - allowed_fields)
    if unknown_fields:
        raise sanic.exceptions.InvalidUsage(f"unsupported batch field: {unknown_fields[0]}")
    return {
        "npis": _normalize_npi_batch_npis(raw_body.get("npis")),
        "address_limit": _bounded_npi_batch_integer(
            raw_body,
            "address_limit",
            default=NPI_BATCH_ADDRESS_DEFAULT_LIMIT,
            minimum=1,
            maximum=NPI_BATCH_ADDRESS_MAX_LIMIT,
        ),
        "address_offset": _bounded_npi_batch_integer(
            raw_body,
            "address_offset",
            default=0,
            minimum=0,
            maximum=1_000_000,
        ),
        **_npi_batch_boolean_map(raw_body),
    }


async def _rank_npi_batch_addresses(
    npis: Sequence[int],
    *,
    session: Any,
) -> dict[int, list[dict[str, Any]]]:
    """Fetch and rank summary address candidates with fixed-count reads."""
    base_addresses_by_npi = await _fetch_npi_location_candidates_map(
        npis,
        session=session,
    )
    overlay_addresses_by_npi = await _fetch_provider_directory_address_overlay_map(
        npis,
        session=session,
    )
    await _apply_location_statuses(
        [
            address
            for npi in npis
            for address in base_addresses_by_npi.get(npi, [])
        ],
        session=session,
    )
    ranked_addresses_by_npi: dict[int, list[dict[str, Any]]] = {}
    for npi in npis:
        addresses = [
            address
            for address in (
                list(base_addresses_by_npi.get(npi, []))
                + list(overlay_addresses_by_npi.get(npi, []))
            )
            if _is_public_street_level_address(address)
        ]
        ranked_addresses = _rank_provider_locations(
            _dedupe_addresses_by_key(addresses)
        )
        ranked_addresses_by_npi[npi] = ranked_addresses
    return ranked_addresses_by_npi


async def _hydrate_npi_batch_addresses(
    npis: Sequence[int],
    ranked_addresses_by_npi: Mapping[int, Sequence[Mapping[str, Any]]],
    *,
    address_limit: int,
    address_offset: int,
    include_sources: bool,
    include_evidence: bool,
    session: Any,
) -> dict[int, list[dict[str, Any]]]:
    """Hydrate only each provider's selected address page."""
    selected_addresses_by_npi = {
        npi: list(ranked_addresses_by_npi[npi])[
            address_offset : address_offset + address_limit
        ]
        for npi in npis
    }

    selected_identity_list = sorted(
        {
            identity
            for npi in npis
            for identity in _selected_base_identity_list(
                selected_addresses_by_npi[npi]
            )
        }
    )
    hydrated_by_npi: dict[int, list[dict[str, Any]]] = {}
    if selected_identity_list:
        hydrated_by_npi = await _fetch_npi_address_rows_map(
            npis,
            include_sources=include_sources,
            include_evidence=include_evidence,
            address_row_identities=selected_identity_list,
            session=session,
        )
    for npi in npis:
        selected_addresses_by_npi[npi] = _merge_hydrated_location_candidates(
            selected_addresses_by_npi[npi],
            hydrated_by_npi.get(npi, []),
        )
    selected_addresses = [
        address
        for npi in npis
        for address in selected_addresses_by_npi[npi]
    ]
    if selected_addresses and (include_sources or include_evidence):
        await _attach_selected_address_source_details(
            selected_addresses,
            include_sources=include_sources,
            include_role_evidence=include_evidence,
            session=session,
        )
    return selected_addresses_by_npi


def _npi_batch_dba_names(
    provider_detail_map: Mapping[str, Any],
    other_names: Sequence[Mapping[str, Any]],
) -> list[str]:
    existing_dba_names = [
        name for name in (provider_detail_map.get("do_business_as") or []) if name
    ]
    if existing_dba_names:
        return list(dict.fromkeys(existing_dba_names))
    return list(
        dict.fromkeys(
            entry.get("other_provider_identifier")
            for entry in other_names
            if entry.get("other_provider_identifier_type_code") == "3"
            and entry.get("other_provider_identifier")
        )
    )


def _npi_batch_provider_result(
    npi: int,
    provider_detail_map: Mapping[str, Any] | None,
    ranked_addresses: Sequence[Mapping[str, Any]],
    selected_addresses: Sequence[Mapping[str, Any]],
    other_names: Sequence[Mapping[str, Any]],
    enrichment: Mapping[str, Any] | None,
    batch_params: Mapping[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Format one success or not-found entry without extra reads."""
    address_total = len(ranked_addresses)
    if provider_detail_map is None and address_total == 0:
        return (
            {
                "npi": npi,
                "status": 404,
                "error": {
                    "code": "not_found",
                    "detail": "provider was not found",
                },
            },
            False,
        )
    public_provider_map = dict(provider_detail_map or {"npi": npi})
    finalized_addresses = [
        _finalize_public_provider_address(
            dict(address),
            include_sources=bool(batch_params["include_sources"]),
            include_evidence=bool(batch_params["include_evidence"]),
        )
        for address in selected_addresses
    ]
    address_offset = int(batch_params["address_offset"])
    public_provider_map["address_list"] = finalized_addresses
    public_provider_map["address_pagination"] = {
        "limit": int(batch_params["address_limit"]),
        "offset": address_offset,
        "returned": len(finalized_addresses),
        "total": address_total,
        "has_more": address_offset + len(finalized_addresses) < address_total,
    }
    public_provider_map["other_name_list"] = list(other_names)
    public_provider_map["do_business_as"] = _npi_batch_dba_names(
        public_provider_map,
        other_names,
    )
    public_provider_map["provider_enrichment"] = {
        "summary": _public_provider_enrichment_summary(enrichment),
        "ffs_visibility": _provider_enrichment_visibility(
            enrichment,
            include_chain=False,
        ),
    }
    _redact_internal_address_fields(public_provider_map)
    return {"npi": npi, "status": 200, "provider": public_provider_map}, True


async def _build_npi_batch_payload(
    batch_params: Mapping[str, Any],
    *,
    session: Any = None,
) -> dict[str, Any]:
    """Assemble ordered provider summaries with set-based database reads."""
    npis = list(batch_params["npis"])
    detail_by_npi = await _build_npi_identity_details_map(npis, session=session)
    ranked_addresses_by_npi = await _rank_npi_batch_addresses(npis, session=session)
    selected_addresses_by_npi = await _hydrate_npi_batch_addresses(
        npis,
        ranked_addresses_by_npi,
        address_limit=int(batch_params["address_limit"]),
        address_offset=int(batch_params["address_offset"]),
        include_sources=bool(batch_params["include_sources"]),
        include_evidence=bool(batch_params["include_evidence"]),
        session=session,
    )
    other_names_by_npi = await _fetch_other_names_map(npis, session=session)
    enrichment_by_npi = await _fetch_provider_enrichment_summary_map(npis, session=session)
    response_items: list[dict[str, Any]] = []
    found_count = 0
    for npi in npis:
        provider_result_map, was_found = _npi_batch_provider_result(
            npi,
            detail_by_npi.get(npi),
            ranked_addresses_by_npi[npi],
            selected_addresses_by_npi[npi],
            other_names_by_npi.get(npi, []),
            enrichment_by_npi.get(npi),
            batch_params,
        )
        response_items.append(provider_result_map)
        found_count += int(was_found)
    return {
        "items": response_items,
        "requested": len(npis),
        "found": found_count,
        "not_found": len(npis) - found_count,
    }


@blueprint.post("/id/batch")
async def get_npi_batch(request):
    """Return up to 100 provider summaries from one bounded database pipeline."""
    started = time.monotonic()
    batch_params = _normalize_npi_batch_request(request.json)
    payload = await _build_npi_batch_payload(
        batch_params,
        session=_request_session(request),
    )
    payload["meta"] = {
        "elapsed_ms": round((time.monotonic() - started) * 1000.0, 2),
        "max_batch_size": NPI_BATCH_MAX_SIZE,
        "view": "summary",
    }
    return response.json(payload, default=str)


@blueprint.get("/id/<npi>")
async def get_npi(request, npi):
    """Return one NPPES- or profile-backed provider with optional provenance."""
    should_force_address_update = _is_truthy_arg(request.args.get("force_address_update"), default=False)
    include_sources = _is_truthy_arg(request.args.get("include_sources"), default=False)
    include_evidence = _is_truthy_arg(request.args.get("include_evidence"), default=False)
    include_profile = _is_truthy_arg(
        request.args.get("include_profile"),
        default=True,
    )
    if _is_truthy_arg(request.args.get("debug"), default=False):
        include_sources = True
        include_evidence = True
    include_extra_info = _is_truthy_arg(request.args.get("extra_info"), default=False)
    should_sync_geocode = _is_truthy_arg(
        request.args.get("sync_geocode"),
        default=_is_environment_flag_enabled(
            "HLTHPRT_NPI_DETAIL_SYNC_GEOCODE",
            "HLTHPRT_NPI_API_SYNC_GEOCODE",
            default=False,
        ),
    )
    should_lookup_stored_geocode = _is_truthy_arg(
        request.args.get("lookup_stored_geocode"),
        default=_is_environment_flag_enabled(
            "HLTHPRT_NPI_DETAIL_LOOKUP_STORED_GEOCODE",
            "HLTHPRT_NPI_API_LOOKUP_STORED_GEOCODE",
            default=False,
        ),
    )
    include_chain_enrichment = _include_chain_provider_enrichment(request.args.get("show"))
    provider_enrichment_view = _normalize_provider_enrichment_view(request.args.get("view"))
    address_grouping = str(
        request.args.get("address_grouping") or ADDRESS_GROUPING_FLAT
    ).strip().lower()
    if address_grouping not in ADDRESS_GROUPING_VALUES:
        raise sanic.exceptions.InvalidUsage(
            "address_grouping must be one of: flat, premise"
        )
    # address_list paging: default-bounded so high-volume providers never serialize
    # 1k+ addresses; address_limit=all (or 0) opts out and returns the full list.
    raw_address_limit = request.args.get("address_limit")
    if address_grouping == ADDRESS_GROUPING_PREMISE:
        normalized_group_limit = str(raw_address_limit or "").strip().lower()
        if normalized_group_limit in ("all", "0", "-1"):
            raise sanic.exceptions.InvalidUsage(
                "address_limit must be between 1 and 5 for premise grouping"
            )
        try:
            address_limit = (
                NPI_DETAIL_ADDRESS_GROUP_DEFAULT_LIMIT
                if not normalized_group_limit
                else int(normalized_group_limit)
            )
        except (TypeError, ValueError) as exc:
            raise sanic.exceptions.InvalidUsage(
                "address_limit must be between 1 and 5 for premise grouping"
            ) from exc
        if not 1 <= address_limit <= NPI_DETAIL_ADDRESS_GROUP_MAX_LIMIT:
            raise sanic.exceptions.InvalidUsage(
                "address_limit must be between 1 and 5 for premise grouping"
            )
    elif raw_address_limit is None or str(raw_address_limit).strip() == "":
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
    include_address_total = _is_truthy_arg(request.args.get("include_address_total"), default=True)
    raw_address_key = request.args.get("address_key")
    raw_address_site_key = request.args.get("address_site_key")
    if address_grouping == ADDRESS_GROUPING_PREMISE and (
        str(raw_address_key or "").strip()
        or str(raw_address_site_key or "").strip()
    ):
        raise sanic.exceptions.InvalidUsage(
            "address_key and address_site_key are not supported with premise grouping"
        )
    address_key = _normalize_uuid_key(raw_address_key, "address_key")
    address_site_key = _normalize_uuid_key(
        raw_address_site_key,
        "address_site_key",
    )
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
    is_response_cache_enabled = bool(
        not should_force_address_update
        and _NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS > 0
        and _NPI_DETAIL_RESPONSE_CACHE_MAX_KEYS > 0
    )
    address_overlay_serving_identity: str | None = None
    canonical_publication_identity: str | None = None
    if is_response_cache_enabled:
        try:
            canonical_publication_identity = (
                await _npi_canonical_publication_identity(
                    session=request_session,
                )
            )
            if canonical_publication_identity is None:
                raise RuntimeError("npi_canonical_publication_identity_missing")
            address_overlay_serving_identity = (
                await _provider_directory_address_overlay_serving_identity(
                    session=request_session,
                )
            )
        except Exception as exc:
            is_response_cache_enabled = False
            logger.debug(
                "NPI response cache identity fetch failed "
                "for npi=%s; bypassing response cache: %s",
                npi,
                exc,
            )
    cache_key = _npi_detail_cache_key(
        npi,
        view=provider_enrichment_view,
        include_chain=include_chain_enrichment,
        extra_info=include_extra_info,
        sync_geocode=should_sync_geocode,
        lookup_stored_geocode=should_lookup_stored_geocode,
        include_sources=include_sources,
        include_evidence=include_evidence,
        include_profile=include_profile,
        profile_generation=(
            str(profile_record["profile"].get("generation_id"))
            if profile_record and isinstance(profile_record.get("profile"), Mapping)
            else None
        ),
        profile_serving_identity=(
            str(profile_record.get("_serving_identity"))
            if profile_record and profile_record.get("_serving_identity")
            else None
        ),
        address_overlay_serving_identity=(
            address_overlay_serving_identity
        ),
        canonical_publication_identity=canonical_publication_identity,
        address_limit=address_limit,
        address_offset=address_offset,
        include_address_total=include_address_total,
        address_key=address_key,
        address_site_key=address_site_key,
        address_grouping=address_grouping,
    )
    if is_response_cache_enabled:
        cached_body = _npi_detail_response_cache_get(cache_key)
        if cached_body is not None:
            return response.raw(cached_body, content_type="application/json")
    db_schema = _runtime_db_schema()
    is_address_archive_cutover = _is_environment_flag_enabled(
        "HLTHPRT_ADDRESS_ARCHIVE_CUTOVER"
    )
    v2_archive_table_cache = SimpleNamespace(resolved=False, table_name=None)
    v2_archive_table_lock = asyncio.Lock()

    async def _is_npi_detail_table_available(table_name: str) -> bool:
        if request_session is not None:
            return await _is_table_available(table_name, session=request_session)
        value = await db.scalar(
            "SELECT to_regclass(:table_name);",
            table_name=f"{db_schema}.{table_name}",
        )
        return isinstance(value, str) and bool(value)

    async def _has_table_column(table_name: str, column_name: str) -> bool:
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
            if is_address_archive_cutover and hasattr(db, "first"):
                preferred = os.getenv("HLTHPRT_ADDRESS_ARCHIVE_TABLE", "address_archive_v2").strip() or "address_archive_v2"
                for table_name in (preferred,):
                    if (
                        await _is_npi_detail_table_available(table_name)
                        and await _has_table_column(table_name, "address_key")
                        and await _has_table_column(table_name, "geo_source")
                        and await _has_address_key_functions()
                    ):
                        v2_archive_table_cache.table_name = table_name
                        break
            v2_archive_table_cache.resolved = True
            return v2_archive_table_cache.table_name

    async def _archive_coordinates_for(address):
        archive_table = await _v2_archive_table()
        if archive_table:
            archive_parameter_map = {
                "first_line": address.get("first_line"),
                "second_line": address.get("second_line"),
                "city_name": address.get("city_name"),
                "state_name": address.get("state_name"),
                "postal_code": address.get("postal_code"),
                "country_code": address.get("country_code") or "US",
            }
            archive_sql = f"""
                SELECT long, lat, place_id, geo_source
                  FROM {db_schema}.{archive_table}
                 WHERE address_key = {db_schema}.addr_key_v1(
                    :first_line, :second_line, :city_name, :state_name, :postal_code, :country_code
                 )
                   AND lat IS NOT NULL
                   AND long IS NOT NULL
            """
            if request_session is not None:
                query_result = await _execute_stmt(
                    text(archive_sql),
                    session=request_session,
                    params=archive_parameter_map,
                )
                coordinate_record = query_result.first()
            else:
                coordinate_record = await db.first(archive_sql, **archive_parameter_map)
            if coordinate_record:
                coordinate_map = coordinate_record._mapping
                return SimpleNamespace(
                    long=coordinate_map["long"],
                    lat=coordinate_map["lat"],
                    place_id=coordinate_map["place_id"],
                    geo_source=coordinate_map.get("geo_source"),
                )
        legacy_stmt = select(AddressArchive).where(AddressArchive.checksum == address["checksum"])
        if request_session is not None:
            query_result = await _execute_stmt(legacy_stmt, session=request_session)
            return query_result.scalar()
        return await db.scalar(legacy_stmt)

    async def _openaddresses_coordinates_for(address):
        if request_session is None and not hasattr(db, "first"):
            return None
        params = lookup_params_from_address(address)
        if not params or not await _is_npi_detail_table_available("openaddresses_geocode"):
            return None
        for query in (exact_lookup_sql(db_schema), fuzzy_lookup_sql(db_schema), relaxed_lookup_sql(db_schema)):
            if request_session is not None:
                result = await _execute_stmt(text(query), session=request_session, params=params)
                row = result.first()
            else:
                row = await db.first(query, **params)
            if row:
                coordinate_data = row._mapping
                return SimpleNamespace(
                    long=coordinate_data["long"],
                    lat=coordinate_data["lat"],
                    place_id=coordinate_data["place_id"],
                    geo_source=coordinate_data["geo_source"],
                    geocode_source=coordinate_data["geocode_source"],
                    geocode_quality=coordinate_data["geocode_quality"],
                )
        return None

    async def update_addr_coordinates(
        address,
        long,
        lat,
        place_id,
        geo_source=None,
        geocode_source=None,
        geocode_quality=None,
    ):
        """Persist geocoding coordinates and provenance for one address."""
        checksum = address["checksum"]
        npi_value = address.get("npi") or npi
        address_type = address.get("type")
        if npi_value is None:
            return
        geo_source = str(geo_source).strip().lower() if geo_source else None
        if geo_source not in {"mapbox", "google", "tiger", "manual", "openaddresses"}:
            geo_source = "google" if place_id else None
        geocode_source = str(geocode_source).strip().lower() if geocode_source else None
        geocode_quality = str(geocode_quality).strip().lower() if geocode_quality else None
        if not geocode_source:
            geocode_source = "api_geocode"
        if not geocode_quality:
            geocode_quality = "unknown"
        address_renderer = getattr(
            getattr(func, db_schema),
            ADDRESS_FORMAT_FUNCTION,
        )
        address_update = (
            db.update(NPIAddress)
            .where(NPIAddress.checksum == checksum)
            .where(NPIAddress.npi == npi_value)
            .values(
                long=long,
                lat=lat,
                formatted_address=address_renderer(
                    NPIAddress.first_line,
                    NPIAddress.second_line,
                    NPIAddress.city_name,
                    NPIAddress.state_name,
                    NPIAddress.postal_code,
                    NPIAddress.country_code,
                ),
                place_id=place_id,
            )
        )
        if address_type is not None:
            address_update = address_update.where(
                NPIAddress.type == address_type
            )
        await address_update.status()
        address_record_stmt = (
            select(NPIAddress)
            .where(NPIAddress.checksum == checksum)
            .where(NPIAddress.npi == npi_value)
        )
        if address_type is not None:
            address_record_stmt = address_record_stmt.where(
                NPIAddress.type == address_type
            )
        address_record = await db.scalar(address_record_stmt)
        if address_record is None:
            return
        archive_table = await _v2_archive_table()
        if archive_table:
            address_type_predicate = (
                "AND type = :address_type" if address_type is not None else ""
            )
            await db.status(
                f"""
                INSERT INTO {db_schema}.{archive_table} (
                    address_key, identity_key, identity_version, precision, premise_key,
                    line1_norm, unit_norm, city_norm, state_code, zip5, zip4, country_code,
                    first_line, second_line, city_name, state_name, postal_code,
                    telephone_number, fax_number, formatted_address, lat, long, place_id,
                    formatted_address_version, formatted_address_source,
                    geo_source, geocode_source, geocode_quality, geocoded_at,
                    source_bits, strict_source_bits, display_priority, date_added
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
                    telephone_number, fax_number,
                    {db_schema}.{ADDRESS_FORMAT_FUNCTION}(
                        first_line, second_line, city_name, state_name,
                        postal_code, COALESCE(NULLIF(country_code, ''), 'US')
                    ),
                    lat, long, place_id,
                    :formatted_address_version, :formatted_address_source,
                    CAST(:geo_source AS {db_schema}.address_archive_geo_source),
                    :geocode_source, :geocode_quality, now(), 1, 1, 0, date_added
                  FROM (
                    SELECT DISTINCT ON (
                        {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US'))
                    )
                        *
                     FROM {db_schema}.npi_address
                     WHERE checksum = :checksum
                       AND npi = :npi
                       {address_type_predicate}
                       AND {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')) IS NOT NULL
                     ORDER BY
                        {db_schema}.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, COALESCE(NULLIF(country_code, ''), 'US')),
                        (place_id IS NOT NULL) DESC,
                        date_added DESC NULLS LAST,
                        npi
                  ) source
                ON CONFLICT (address_key) DO UPDATE SET
                    formatted_address = {db_schema}.{ADDRESS_FORMAT_FUNCTION}(
                        {db_schema}.{archive_table}.first_line,
                        {db_schema}.{archive_table}.second_line,
                        {db_schema}.{archive_table}.city_name,
                        {db_schema}.{archive_table}.state_name,
                        {db_schema}.{archive_table}.postal_code,
                        {db_schema}.{archive_table}.country_code
                    ),
                    formatted_address_version = EXCLUDED.formatted_address_version,
                    formatted_address_source = EXCLUDED.formatted_address_source,
                    lat = COALESCE({db_schema}.{archive_table}.lat, EXCLUDED.lat),
                    long = COALESCE({db_schema}.{archive_table}.long, EXCLUDED.long),
                    place_id = COALESCE({db_schema}.{archive_table}.place_id, EXCLUDED.place_id),
                    geo_source = COALESCE({db_schema}.{archive_table}.geo_source, EXCLUDED.geo_source),
                    geocode_source = COALESCE({db_schema}.{archive_table}.geocode_source, EXCLUDED.geocode_source),
                    geocode_quality = COALESCE({db_schema}.{archive_table}.geocode_quality, EXCLUDED.geocode_quality),
                    geocoded_at = COALESCE({db_schema}.{archive_table}.geocoded_at, EXCLUDED.geocoded_at),
                    source_bits = {db_schema}.{archive_table}.source_bits | 1,
                    strict_source_bits = {db_schema}.{archive_table}.strict_source_bits | 1,
                    last_seen_at = now();
                """,
                checksum=checksum,
                npi=npi_value,
                address_type=address_type,
                geo_source=geo_source,
                geocode_source=geocode_source,
                geocode_quality=geocode_quality,
                formatted_address_version=ADDRESS_FORMAT_VERSION,
                formatted_address_source=ADDRESS_FORMAT_SOURCE,
            )
            return
        archive_value_map = {
            column.key: getattr(address_record, column.key, None)
            for column in AddressArchive.__table__.columns
        }
        archive_value_map["formatted_address"] = render_formatted_address_v2(
            archive_value_map.get("first_line"),
            archive_value_map.get("second_line"),
            archive_value_map.get("city_name"),
            archive_value_map.get("state_name"),
            archive_value_map.get("postal_code"),
            archive_value_map.get("country_code"),
        )
        try:
            await (
                db.insert(AddressArchive)
                .values(archive_value_map)
                .on_conflict_do_update(
                    index_elements=AddressArchive.__my_index_elements__,
                    set_=archive_value_map,
                )
                .status()
            )
        except Exception as exc:
            logger.warning("Could not archive address checksum=%s: %s", checksum, exc)

    async def _update_address(address_by_field):
        """Geocode one address when it does not already have coordinates."""
        address_by_field["formatted_address"] = render_formatted_address_v2(
            address_by_field.get("first_line"),
            address_by_field.get("second_line"),
            address_by_field.get("city_name"),
            address_by_field.get("state_name"),
            address_by_field.get("postal_code"),
            address_by_field.get("country_code"),
        )
        if address_by_field.get("lat"):
            return address_by_field
        postal_code = address_by_field.get("postal_code")
        if postal_code is not None:
            postal_code = str(postal_code)
        if postal_code and len(postal_code) > 5:
            postal_code = f"{postal_code[0:5]}-{postal_code[5:]}"
        state_postal = " ".join(
            part
            for part in [
                str(address_by_field.get("state_name") or "").strip(),
                str(postal_code or "").strip(),
            ]
            if part
        )
        t_addr = ", ".join(
            part
            for part in [
                str(address_by_field.get("first_line") or "").strip(),
                str(address_by_field.get("second_line") or "").strip(),
                str(address_by_field.get("city_name") or "").strip(),
                state_postal,
            ]
            if part
        )
        t_addr = t_addr.replace(" , ", " ")

        for key in ("lat", "long", "formatted_address", "place_id"):
            address_by_field.setdefault(key, None)
        if should_force_address_update:
            address_by_field["long"] = None
            address_by_field["lat"] = None
            address_by_field["place_id"] = None

        if not address_by_field["lat"]:

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
            should_update_geo = False
            if request.app.config.get("NPI_API_UPDATE_GEOCODE") and not address_by_field["lat"]:
                should_update_geo = True

            if should_lookup_stored_geocode and (not address_by_field["lat"]) and (not should_force_address_update):
                stored_coordinates = await _archive_coordinates_for(address_by_field)
                if stored_coordinates:
                    address_by_field["long"] = stored_coordinates.long
                    address_by_field["lat"] = stored_coordinates.lat
                    address_by_field["place_id"] = stored_coordinates.place_id
                    address_by_field["geo_source"] = getattr(stored_coordinates, "geo_source", None) or (
                        "google" if stored_coordinates.place_id else None
                    )

            if (
                should_lookup_stored_geocode
                or should_sync_geocode
                or should_force_address_update
            ) and not address_by_field["lat"]:
                try:
                    openaddresses_coordinates = await _openaddresses_coordinates_for(address_by_field)
                    if openaddresses_coordinates:
                        address_by_field["long"] = openaddresses_coordinates.long
                        address_by_field["lat"] = openaddresses_coordinates.lat
                        address_by_field["place_id"] = openaddresses_coordinates.place_id
                        address_by_field["geo_source"] = openaddresses_coordinates.geo_source
                        address_by_field["geocode_source"] = openaddresses_coordinates.geocode_source
                        address_by_field["geocode_quality"] = openaddresses_coordinates.geocode_quality
                except Exception as exc:
                    logger.debug("OpenAddresses geocoding failed for %s: %s", t_addr, exc)

            if (should_sync_geocode or should_force_address_update) and not address_by_field["lat"]:
                try:
                    geocoder_parameter_map = {
                        request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY_PARAM"): random.choice(
                            json.loads(request.app.config.get("GEOCODE_MAPBOX_STYLE_KEY"))
                        )
                    }
                    encoded_params = ".json?".join(
                        (
                            urllib.parse.quote_plus(t_addr),
                            urllib.parse.urlencode(geocoder_parameter_map, doseq=True),
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
                        address_by_field["long"] = geo_data["features"][0]["geometry"]["coordinates"][0]
                        address_by_field["lat"] = geo_data["features"][0]["geometry"]["coordinates"][1]
                        address_by_field["place_id"] = None
                        address_by_field["geo_source"] = "mapbox"
                except Exception as exc:
                    logger.debug("Mapbox geocoding failed for %s: %s", t_addr, exc)

            if (should_sync_geocode or should_force_address_update) and not address_by_field["lat"]:
                try:
                    geocoder_parameter_map = {
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_ADDRESS_PARAM"): t_addr,
                        request.app.config.get("GEOCODE_GOOGLE_STYLE_KEY_PARAM"): request.app.config.get(
                            "GEOCODE_GOOGLE_STYLE_KEY"
                        ),
                    }
                    encoded_params = urllib.parse.urlencode(geocoder_parameter_map, doseq=True)
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
                        address_by_field["long"] = geo_data["results"][0]["geometry"]["location"]["lng"]
                        address_by_field["lat"] = geo_data["results"][0]["geometry"]["location"]["lat"]
                        address_by_field["place_id"] = geo_data["results"][0]["place_id"]
                        address_by_field["geo_source"] = "google"
                except Exception as exc:
                    logger.warning("Google geocoding failed for %s: %s", t_addr, exc)

            if should_update_geo and address_by_field.get("lat"):
                request.app.add_task(
                    update_addr_coordinates(
                        address_by_field,
                        address_by_field["long"],
                        address_by_field["lat"],
                        address_by_field["place_id"],
                        address_by_field.get("geo_source"),
                        address_by_field.get("geocode_source"),
                        address_by_field.get("geocode_quality"),
                    )
                )

        return address_by_field

    detail_build_map: dict[str, Any] = {
        # Bounded requests assemble provider identity without full address rows.
        # A lightweight complete candidate query ranks the combined set below,
        # then a second bounded query hydrates only the selected base rows.
        "address_limit": 0,
        "include_address_total": False,
        "address_key": address_key,
    }
    if request_session is not None:
        detail_build_map["session"] = request_session
    if include_sources or include_evidence:
        detail_build_map["include_sources"] = include_sources
        detail_build_map["include_evidence"] = include_evidence
    provider_detail_by_field = await _build_npi_details(npi, **detail_build_map)
    has_provider_detail = bool(provider_detail_by_field)

    if not has_provider_detail:
        provider_detail_by_field = {"npi": npi}
        if profile_record:
            provider_detail_by_field["provider_directory_profile"] = (
                profile_record["profile"]
            )
        if (
            include_evidence
            and profile_record
            and profile_record.get("evidence") is not None
        ):
            provider_detail_by_field["provider_directory_profile_evidence"] = (
                profile_record["evidence"]
            )

    provider_detail_by_field.pop("address_total", None)

    overlay_addresses = await _fetch_provider_directory_address_overlay(
        npi,
        address_key=address_key,
        address_site_key=address_site_key,
        session=request_session,
    )
    initial_base_addresses = list(
        provider_detail_by_field.get("address_list") or []
    )
    base_candidates = list(
        await _fetch_npi_location_candidates(
            npi,
            address_key=address_key,
            address_site_key=address_site_key,
            session=request_session,
        )
    )
    # A compatibility builder may retain direct rows despite its empty address
    # window. Keep that evidence without replacing the complete candidate set.
    base_candidates.extend(initial_base_addresses)
    await _apply_location_statuses(
        base_candidates,
        session=request_session,
    )
    addresses = base_candidates + overlay_addresses
    if address_key is not None:
        addresses = [
            address
            for address in addresses
            if isinstance(address, Mapping)
            and str(address.get("address_key") or "").lower() == address_key
        ]
    if address_site_key is not None:
        addresses = [
            address
            for address in addresses
            if isinstance(address, Mapping)
            and _is_address_site_key_match(address, address_site_key)
        ]
    if not include_extra_info:
        addresses = [address for address in addresses if _is_public_street_level_address(address)]
    addresses = _rank_provider_locations(_dedupe_addresses_by_key(addresses))
    if not has_provider_detail and not profile_record and not addresses:
        raise sanic.exceptions.NotFound
    address_total = len(addresses)
    selected_group_specs: list[dict[str, Any]] = []
    if address_grouping == ADDRESS_GROUPING_PREMISE:
        all_group_specs = _group_provider_locations_by_premise(addresses)
        selected_group_specs = all_group_specs[
            address_offset : address_offset + address_limit
        ]
        selected_candidates = [
            member
            for group_spec in selected_group_specs
            for member in group_spec["members"][
                :NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT
            ]
        ]
    else:
        all_group_specs = []
        selected_candidates = addresses
        if address_limit is not None:
            selected_candidates = selected_candidates[
                address_offset : address_offset + address_limit
            ]
    addresses = await _hydrate_selected_provider_locations(
        npi,
        selected_candidates,
        already_hydrated=False,
        include_sources=include_sources,
        include_evidence=include_evidence,
        address_key=address_key,
        session=request_session,
    )
    if addresses and (include_sources or include_evidence):
        await _attach_selected_address_source_details(
            addresses,
            include_sources=include_sources,
            include_role_evidence=include_evidence,
            session=request_session,
        )
    update_address_tasks = [
        _update_address(address)
        for address in addresses
        if address
    ]
    updated_addresses = (
        list(await asyncio.gather(*update_address_tasks))
        if update_address_tasks
        else []
    )
    if address_grouping == ADDRESS_GROUPING_PREMISE:
        provider_detail_by_field.pop("address_list", None)
        provider_detail_by_field.pop("address_pagination", None)
        response_groups: list[dict[str, Any]] = []
        member_cursor = 0
        for group_spec in selected_group_specs:
            member_total = len(group_spec["members"])
            member_returned = min(
                member_total,
                NPI_DETAIL_ADDRESS_GROUP_MEMBER_LIMIT,
            )
            group_members = [
                _finalize_public_provider_address(
                    dict(address),
                    include_sources=include_sources,
                    include_evidence=include_evidence,
                    suppress_conflicting_site_key=True,
                )
                for address in updated_addresses[
                    member_cursor : member_cursor + member_returned
                ]
                if isinstance(address, dict)
            ]
            member_cursor += member_returned
            response_groups.append(
                {
                    "group_key": group_spec["group_key"],
                    "grouping_basis": group_spec["grouping_basis"],
                    "address_site_key": group_spec["address_site_key"],
                    "address_site_key_status": group_spec[
                        "address_site_key_status"
                    ],
                    "members": group_members,
                    "member_pagination": _member_pagination(
                        member_total,
                        len(group_members),
                    ),
                }
            )
        provider_detail_by_field["address_grouping"] = ADDRESS_GROUPING_PREMISE
        provider_detail_by_field["address_groups"] = response_groups
        provider_detail_by_field["address_group_pagination"] = (
            _address_group_pagination(
                limit=address_limit,
                offset=address_offset,
                returned=len(response_groups),
                total=len(all_group_specs),
            )
        )
    else:
        provider_detail_by_field["address_list"] = [
            _finalize_public_provider_address(
                dict(address),
                include_sources=include_sources,
                include_evidence=include_evidence,
            )
            for address in updated_addresses
            if isinstance(address, dict)
        ]
        # Never silently truncate: describe the combined, deduplicated result set.
        returned = len(provider_detail_by_field["address_list"])
        effective_offset = address_offset if address_limit is not None else 0
        provider_detail_by_field["address_pagination"] = {
            "limit": address_limit,
            "offset": effective_offset,
            "returned": returned,
            "total": address_total if include_address_total else None,
            "has_more": bool(
                address_limit is not None
                and effective_offset + returned < address_total
            ),
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
    provider_detail_by_field["other_name_list"] = other_names

    existing_dba_names = [
        name
        for name in (provider_detail_by_field.get("do_business_as") or [])
        if name
    ]
    if existing_dba_names:
        provider_detail_by_field["do_business_as"] = list(
            dict.fromkeys(existing_dba_names)
        )
    else:
        candidates = [
            entry.get("other_provider_identifier")
            for entry in other_names
            if entry.get("other_provider_identifier_type_code") == "3" and entry.get("other_provider_identifier")
        ]
        provider_detail_by_field["do_business_as"] = (
            list(dict.fromkeys(candidates)) if candidates else []
        )

    if provider_enrichment_payload is not None:
        provider_detail_by_field["provider_enrichment"] = provider_enrichment_payload
    else:
        provider_detail_by_field["provider_enrichment"] = {
            "summary": None,
        }
        if provider_enrichment_view == "full":
            provider_detail_by_field["provider_enrichment"]["enrollments"] = {
                "ffs_public": [],
                "hospital": [],
                "hha": [],
                "hospice": [],
                "fqhc": [],
                "rhc": [],
                "snf": [],
            }
        else:
            provider_detail_by_field["provider_enrichment"]["ffs_visibility"] = {
                "show_mode": "chain" if include_chain_enrichment else "default",
                "chain_hidden": False,
                "chain_enrollment_count": 0,
                "chain_enrollment_ids": [],
            }

    if include_profile and profile_record:
        provider_detail_by_field["provider_directory_profile"] = profile_record[
            "profile"
        ]
        if include_evidence and profile_record.get("evidence") is not None:
            provider_detail_by_field["provider_directory_profile_evidence"] = (
                profile_record["evidence"]
            )

    _redact_internal_address_fields(provider_detail_by_field)
    response_body = json.dumps(
        provider_detail_by_field,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    if is_response_cache_enabled and _is_npi_detail_response_cacheable(
        provider_detail_by_field,
        force_address_update=should_force_address_update,
        sync_geocode=should_sync_geocode,
    ):
        _npi_detail_response_cache_set(cache_key, response_body)
    return response.raw(response_body, content_type="application/json")

NPI_LOCATION_CANDIDATE_COLUMNS = (
    "checksum",
    "npi",
    "inferred_npi",
    "type",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "state_code",
    "postal_code",
    "country_code",
    "telephone_number",
    "phone_number",
    "formatted_address",
    "location_key",
    "address_key",
    "premise_key",
    "address_precision",
    "lat",
    "long",
    "source_count",
    "independent_source_count",
    "multi_source_confirmed",
    "address_sources",
    "source_record_ids",
    "updated_at",
    "last_seen_at",
    "date_added",
)


def _provider_detail_address_type_clause(address_model: Any, table: Any) -> Any:
    if address_model is EntityAddressUnified:
        return table.c.type.in_(("primary", "secondary", "practice", "site"))
    return or_(table.c.type == "primary", table.c.type == "secondary")


def _npi_batch_address_filters(
    address_model: Any,
    address_table: Any,
    npis: Sequence[int],
) -> list[Any]:
    if address_model is EntityAddressUnified:
        return [
            func.coalesce(address_table.c.npi, address_table.c.inferred_npi).in_(npis)
        ]
    return [address_table.c.npi.in_(npis)]


def _group_npi_location_candidates(
    query_result: Any,
    candidate_columns: Sequence[Any],
) -> dict[int, list[dict[str, Any]]]:
    candidates_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for address_record in query_result.all():
        row_mapping = getattr(address_record, "_mapping", address_record)
        candidate_map = {
            column.key: row_mapping[column.key] for column in candidate_columns
        }
        provider_npi = candidate_map.get("npi") or candidate_map.get("inferred_npi")
        if provider_npi is None:
            continue
        provider_npi = int(provider_npi)
        candidate_map["npi"] = provider_npi
        base_identity = _base_address_row_identity(candidate_map)
        if base_identity:
            candidate_map["_base_row_identities"] = [base_identity]
        candidates_by_npi[provider_npi].append(candidate_map)
    return dict(candidates_by_npi)


async def _fetch_npi_location_candidates_map(
    npis: Sequence[int],
    *,
    address_key: str | None = None,
    address_site_key: str | None = None,
    session: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    """Read ranking fields for many providers in one address query."""
    unique_npis = sorted({int(npi) for npi in npis})
    if not unique_npis:
        return {}
    address_model = await _address_serving_model(
        _public_address_serving_column_keys()
        - {"procedures_array", "medications_array"},
        session=session,
    )
    address_table = address_model.__table__
    existing_columns = await _table_columns(
        address_model.__tablename__, session=session
    )
    if not existing_columns:
        existing_columns = _model_table_columns(address_model)
    candidate_columns = [
        address_table.c[column_name]
        for column_name in NPI_LOCATION_CANDIDATE_COLUMNS
        if column_name in existing_columns
    ]
    filters = _npi_batch_address_filters(address_model, address_table, unique_npis)
    if address_key is not None:
        filters.append(address_table.c.address_key == address_key)
    if address_site_key is not None:
        if (
            address_model is not EntityAddressUnified
            or "premise_key" not in existing_columns
        ):
            return {}
        filters.append(address_table.c.premise_key == address_site_key)
    statement = (
        select(*candidate_columns)
        .where(*filters)
        .where(_provider_detail_address_type_clause(address_model, address_table))
    )
    query_result = await _execute_stmt(statement, session=session)
    return _group_npi_location_candidates(query_result, candidate_columns)


async def _fetch_npi_location_candidates(
    npi: int,
    *,
    address_key: str | None = None,
    address_site_key: str | None = None,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Read only the fields needed to rank one provider's locations."""
    return (
        await _fetch_npi_location_candidates_map(
            [npi],
            address_key=address_key,
            address_site_key=address_site_key,
            session=session,
        )
    ).get(int(npi), [])


def _address_hydration_columns(
    address_model: Any,
    address_table: Any,
    existing_columns: set[str],
    *,
    include_sources: bool,
    include_evidence: bool,
    include_location_key: bool,
) -> list[Any]:
    allowed_columns = set(_model_table_columns(NPIAddress))
    if address_model is EntityAddressUnified:
        allowed_columns.update(PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS)
        allowed_columns.update(
            {"inferred_npi", "premise_key", "source_record_ids", "location_key"}
        )
    if include_sources or include_evidence:
        allowed_columns.update(PUBLIC_ADDRESS_SOURCE_DEBUG_COLUMNS)
    if include_evidence:
        allowed_columns.update(PUBLIC_ADDRESS_EVIDENCE_DEBUG_COLUMNS)
    if include_location_key:
        allowed_columns.add("location_key")
    return [
        address_table.c[column.key]
        for column in address_table.columns
        if column.key in allowed_columns and column.key in existing_columns
    ]


def _address_identity_filter(
    address_model: Any,
    address_table: Any,
    address_row_identities: Sequence[str] | None,
) -> tuple[set[str], Any | None]:
    selected_identity_set = {
        str(identity)
        for identity in (address_row_identities or [])
        if str(identity or "").strip()
    }
    if address_row_identities is None:
        return selected_identity_set, None
    if address_model is EntityAddressUnified:
        selected_keys = sorted(
            identity.split(":", 1)[1]
            for identity in selected_identity_set
            if identity.startswith("location:")
        )
        return selected_identity_set, (
            address_table.c.location_key.in_(selected_keys)
            if selected_keys
            else false()
        )
    selected_checksums = sorted(
        int(identity.rsplit(":", 1)[1])
        for identity in selected_identity_set
        if identity.startswith("legacy:")
        and identity.rsplit(":", 1)[1].lstrip("-").isdigit()
    )
    return selected_identity_set, (
        address_table.c.checksum.in_(selected_checksums)
        if selected_checksums
        else false()
    )


def _hydrate_address_query_rows(
    query_result: Any,
    selected_columns: Sequence[Any],
    npi: int,
    selected_identity_set: set[str],
) -> list[dict[str, Any]]:
    return _hydrate_address_query_rows_map(
        query_result,
        selected_columns,
        selected_identity_set,
    ).get(int(npi), [])


def _hydrate_address_query_rows_map(
    query_result: Any,
    selected_columns: Sequence[Any],
    selected_identity_set: set[str],
) -> dict[int, list[dict[str, Any]]]:
    """Group hydrated address rows by their resolved provider NPI."""
    hydrated_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for address_record in query_result.all():
        mapping = getattr(address_record, "_mapping", address_record)
        address_by_field = {
            column.key: mapping[column.key]
            for column in selected_columns
        }
        provider_npi = address_by_field.get("npi") or address_by_field.get(
            "inferred_npi"
        )
        if provider_npi is None:
            continue
        provider_npi = int(provider_npi)
        address_by_field["npi"] = provider_npi
        identity = _base_address_row_identity(address_by_field)
        if selected_identity_set and identity not in selected_identity_set:
            continue
        if identity:
            address_by_field["_base_row_identities"] = [identity]
        _attach_public_address_site_key(address_by_field, address_by_field)
        hydrated_address = (
            _add_canonical_contact_fields_to_address(address_by_field)
        )
        hydrated_by_npi[provider_npi].append(hydrated_address)
    return dict(hydrated_by_npi)


def _selected_base_identity_list(
    selected_locations: Sequence[Mapping[str, Any]],
) -> list[str]:
    """Return stable base-row identities for a selected location page."""
    return sorted(
        {
            str(identity_value)
            for location_map in selected_locations
            for identity_value in (location_map.get("_base_row_identities") or [])
            if identity_value not in (None, "")
        }
    )


def _merge_hydrated_location_candidates(
    selected_locations: Sequence[Mapping[str, Any]],
    hydrated_locations: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Merge hydrated evidence into ranked candidate scalars deterministically."""
    hydrated_by_identity = {
        str(identity_value): hydrated_location
        for hydrated_location in hydrated_locations
        for identity_value in (
            hydrated_location.get("_base_row_identities") or []
        )
        if identity_value not in (None, "")
    }
    merged_locations: list[dict[str, Any]] = []
    for selected_location in selected_locations:
        merged_location_map = dict(selected_location)
        identity_list = sorted(
            str(identity_value)
            for identity_value in (
                selected_location.get("_base_row_identities") or []
            )
            if identity_value not in (None, "")
        )
        for identity_value in identity_list:
            hydrated_location = hydrated_by_identity.get(identity_value)
            if hydrated_location is not None:
                _merge_duplicate_address(merged_location_map, hydrated_location)
        primary_identity = _base_address_row_identity(selected_location)
        if not primary_identity and len(identity_list) == 1:
            primary_identity = identity_list[0]
        primary_hydrated_location = hydrated_by_identity.get(primary_identity)
        if primary_hydrated_location is not None:
            _fill_hydrated_identity_scalars(
                merged_location_map,
                primary_hydrated_location,
            )
        merged_locations.append(merged_location_map)
    return merged_locations


async def _hydrate_selected_provider_locations(
    npi: int,
    selected_locations: Sequence[Mapping[str, Any]],
    *,
    already_hydrated: bool,
    include_sources: bool,
    include_evidence: bool,
    address_key: str | None,
    session: Any,
) -> list[dict[str, Any]]:
    """Hydrate only the base rows represented by a bounded location page."""
    if already_hydrated:
        return [dict(location_map) for location_map in selected_locations]
    identity_list = _selected_base_identity_list(selected_locations)
    hydrated_locations: list[dict[str, Any]] = []
    if identity_list:
        hydrated_locations = await _fetch_npi_address_rows(
            npi,
            include_sources=include_sources,
            include_evidence=include_evidence,
            address_key=address_key,
            address_row_identities=identity_list,
            session=session,
        )
    return _merge_hydrated_location_candidates(
        selected_locations,
        hydrated_locations,
    )


async def _fetch_npi_address_rows_map(
    npis: Sequence[int],
    *,
    include_sources: bool = False,
    include_evidence: bool = False,
    address_key: str | None = None,
    address_row_identities: Sequence[str] | None = None,
    session: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    """Hydrate selected address rows for many NPIs in one query."""
    unique_npis = sorted({int(npi) for npi in npis})
    if not unique_npis:
        return {}
    address_model = await _address_serving_model(
        _public_address_serving_column_keys()
        - {"procedures_array", "medications_array"},
        session=session,
    )
    address_table = address_model.__table__
    existing_columns = await _table_columns(
        address_model.__tablename__,
        session=session,
    )
    if not existing_columns:
        existing_columns = _model_table_columns(address_model)
    selected_columns = _address_hydration_columns(
        address_model,
        address_table,
        existing_columns,
        include_sources=include_sources,
        include_evidence=include_evidence,
        include_location_key=address_row_identities is not None,
    )
    filters = _npi_batch_address_filters(address_model, address_table, unique_npis)
    if address_key is not None:
        filters.append(address_table.c.address_key == address_key)
    selected_identity_set, identity_filter = _address_identity_filter(
        address_model,
        address_table,
        address_row_identities,
    )
    if identity_filter is not None:
        filters.append(identity_filter)
    statement = (
        select(*selected_columns)
        .where(*filters)
        .where(_provider_detail_address_type_clause(address_model, address_table))
        .order_by(
            address_table.c.type,
            address_table.c.first_line,
            address_table.c.city_name,
        )
    )
    query_result = await _execute_stmt(statement, session=session)
    return _hydrate_address_query_rows_map(
        query_result,
        selected_columns,
        selected_identity_set,
    )


async def _fetch_npi_address_rows(
    npi: int,
    *,
    include_sources: bool = False,
    include_evidence: bool = False,
    address_key: str | None = None,
    address_row_identities: Sequence[str] | None = None,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Hydrate address rows without requiring a matching NPIData identity row."""
    return (
        await _fetch_npi_address_rows_map(
            [npi],
            include_sources=include_sources,
            include_evidence=include_evidence,
            address_key=address_key,
            address_row_identities=address_row_identities,
            session=session,
        )
    ).get(int(npi), [])


def _npi_detail_from_result_row(
    result_row: Sequence[Any],
    *,
    include_address_rows: bool,
    address_total: int | None = None,
) -> dict[str, Any]:
    """Convert the shared NPI/taxonomy projection into its public shape."""
    provider_detail_map: dict[str, Any] = {
        "taxonomy_list": [],
        "taxonomy_group_list": [],
        "address_list": [],
    }
    index = 0
    for column in _npi_serving_columns():
        column_value = result_row[index]
        index += 1
        if (
            column.key == "do_business_as_text"
            or column.key in PUBLIC_NPI_EXCLUDED_COLUMNS
        ):
            continue
        provider_detail_map[column.key] = column_value
    if result_row[index]:
        provider_detail_map["taxonomy_list"].extend(
            _public_nested_taxonomy_rows(result_row[index])
        )
    index += 1
    if result_row[index]:
        provider_detail_map["taxonomy_group_list"].extend(
            _public_nested_taxonomy_rows(result_row[index])
        )
    index += 1
    if include_address_rows and index < len(result_row) and result_row[index]:
        provider_detail_map["address_list"] = result_row[index]
    if address_total is not None:
        provider_detail_map["address_total"] = address_total
    provider_detail_map["do_business_as"] = (
        provider_detail_map.get("do_business_as") or []
    )
    return provider_detail_map


def _npi_taxonomy_batch_aggregate(
    taxonomy_model: Any,
    npis: Sequence[int],
    alias: str,
) -> Any:
    taxonomy_table = taxonomy_model.__table__
    return (
        select(
            taxonomy_table.c.npi,
            func.json_agg(
                literal_column(f'distinct "{taxonomy_model.__tablename__}"')
            ).label("rows"),
        )
        .where(taxonomy_table.c.npi.in_(npis))
        .group_by(taxonomy_table.c.npi)
        .subquery(alias)
    )


async def _build_npi_identity_details_map(
    npis: Sequence[int],
    *,
    session: Any = None,
) -> dict[int, dict[str, Any]]:
    """Fetch provider identity and taxonomy aggregates for many NPIs at once."""
    unique_npis = sorted({int(npi) for npi in npis})
    if not unique_npis:
        return {}
    npi_data_table = NPIData.__table__
    taxonomy_aggregate = _npi_taxonomy_batch_aggregate(
        NPIDataTaxonomy,
        unique_npis,
        "batch_taxonomy_aggregate",
    )
    taxonomy_group_aggregate = _npi_taxonomy_batch_aggregate(
        NPIDataTaxonomyGroup,
        unique_npis,
        "batch_taxonomy_group_aggregate",
    )
    join_clause = npi_data_table.outerjoin(
        taxonomy_aggregate,
        npi_data_table.c.npi == taxonomy_aggregate.c.npi,
    ).outerjoin(
        taxonomy_group_aggregate,
        npi_data_table.c.npi == taxonomy_group_aggregate.c.npi,
    )
    query = (
        db.select(
            *_npi_serving_columns(),
            taxonomy_aggregate.c.rows,
            taxonomy_group_aggregate.c.rows,
        )
        .select_from(join_clause)
        .where(npi_data_table.c.npi.in_(unique_npis))
        .order_by(npi_data_table.c.npi)
    )
    if session is not None:
        query_result = await session.execute(query._stmt)
        detail_rows = query_result.all()
    else:
        detail_rows = await query.all()
    return {
        int(detail_row[0]): _npi_detail_from_result_row(
            detail_row,
            include_address_rows=False,
        )
        for detail_row in detail_rows
    }


def _npi_detail_allowed_address_columns(
    address_model: Any,
    *,
    include_sources: bool,
    include_evidence: bool,
    include_location_key: bool,
) -> set[str]:
    allowed_address_columns = set(_model_table_columns(NPIAddress))
    if address_model is EntityAddressUnified:
        allowed_address_columns.add("premise_key")
        allowed_address_columns.update(PUBLIC_ADDRESS_ATTRIBUTION_COLUMNS)
        allowed_address_columns.add("source_record_ids")
    if include_sources or include_evidence:
        allowed_address_columns.update(PUBLIC_ADDRESS_SOURCE_DEBUG_COLUMNS)
    if include_evidence:
        allowed_address_columns.update(PUBLIC_ADDRESS_EVIDENCE_DEBUG_COLUMNS)
    if include_location_key and address_model is EntityAddressUnified:
        allowed_address_columns.add("location_key")
    return allowed_address_columns


def _npi_detail_address_columns(
    address_table: Any,
    existing_address_columns: set[str],
    allowed_address_columns: set[str],
    filter_capabilities: Mapping[str, Any],
) -> list[Any]:
    procedures_available = bool(
        filter_capabilities.get("npi_procedures_array_available", True)
    )
    medications_available = bool(
        filter_capabilities.get("npi_medications_array_available", True)
    )
    address_columns: list[Any] = []
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
        if column.key == "procedures_array" and not procedures_available:
            address_columns.append(literal_column("'{}'::INTEGER[]").label("procedures_array"))
            continue
        if column.key == "medications_array" and not medications_available:
            address_columns.append(literal_column("'{}'::INTEGER[]").label("medications_array"))
            continue
        address_columns.append(address_table.c[column.key])
    return address_columns


async def _npi_detail_address_context(
    *,
    include_sources: bool,
    include_evidence: bool,
    include_location_key: bool,
    session: Any = None,
) -> tuple[Any, Any, list[Any]]:
    filter_capabilities = await _resolve_npi_filter_capabilities(session=session)
    address_model = await _address_serving_model(
        _public_address_serving_column_keys()
        - {"procedures_array", "medications_array"},
        session=session,
    )
    address_table = address_model.__table__
    existing_columns = await _table_columns(
        address_model.__tablename__,
        session=session,
    )
    if not existing_columns:
        existing_columns = _model_table_columns(address_model)
    allowed_columns = _npi_detail_allowed_address_columns(
        address_model,
        include_sources=include_sources,
        include_evidence=include_evidence,
        include_location_key=include_location_key,
    )
    return (
        address_model,
        address_table,
        _npi_detail_address_columns(
            address_table,
            existing_columns,
            allowed_columns,
            filter_capabilities,
        ),
    )


def _npi_detail_address_filters(
    address_model: Any,
    address_table: Any,
    npi: int,
    address_key: str | None,
    address_row_identities: Sequence[str] | None,
) -> list[Any]:
    base_address_filters = [address_table.c.npi == npi]
    if address_model is EntityAddressUnified:
        base_address_filters[0] = func.coalesce(
            address_table.c.npi, address_table.c.inferred_npi
        ) == npi
    if address_key is not None:
        base_address_filters.append(address_table.c.address_key == address_key)
    if address_row_identities is not None:
        if address_model is EntityAddressUnified:
            selected_location_keys = sorted(
                str(identity).split(":", 1)[1]
                for identity in address_row_identities
                if str(identity).startswith("location:")
            )
            base_address_filters.append(
                address_table.c.location_key.in_(selected_location_keys)
            )
        else:
            selected_checksums = sorted(
                int(str(identity).rsplit(":", 1)[1])
                for identity in address_row_identities
                if str(identity).startswith("legacy:")
                and str(identity).rsplit(":", 1)[1].lstrip("-").isdigit()
            )
            base_address_filters.append(
                address_table.c.checksum.in_(selected_checksums)
            )
    return base_address_filters


def _npi_detail_address_subquery(
    address_model: Any,
    address_table: Any,
    address_columns: Sequence[Any],
    address_filters: Sequence[Any],
    address_limit: int | None,
) -> Any:
    npi_address_rows = (
        select(*address_columns)
        .where(*address_filters)
        .offset(0)
        .subquery("npi_address_rows")
    )
    address_subquery_base = (
        select(*npi_address_rows.c)
        .where(_provider_detail_address_type_clause(address_model, npi_address_rows))
        # Deterministic order keeps bounded identity hydration stable.
        .order_by(
            npi_address_rows.c.type,
            npi_address_rows.c.first_line,
            npi_address_rows.c.city_name,
        )
    )
    if address_limit is not None:
        address_subquery_base = address_subquery_base.limit(address_limit)
    try:
        return address_subquery_base.alias("address_list")
    except NameError:
        return address_subquery_base


async def _count_npi_detail_addresses(
    address_model: Any,
    address_table: Any,
    address_filters: Sequence[Any],
    *,
    session: Any = None,
) -> int | None:
    count_npi_rows = (
        select(address_table.c.type)
        .where(*address_filters)
        .offset(0)
        .subquery("count_npi_address_rows")
    )
    count_statement = select(func.count()).select_from(count_npi_rows).where(
        _provider_detail_address_type_clause(address_model, count_npi_rows)
    )
    if session is not None:
        query_result = await session.execute(count_statement)
        return int(query_result.scalar() or 0)
    try:
        return int(await db.scalar(count_statement) or 0)
    except Exception:
        return None


def _npi_detail_taxonomy_aggregate(
    taxonomy_model: Any,
    npi: int,
    alias: str,
) -> Any:
    taxonomy_table = taxonomy_model.__table__
    return (
        select(
            taxonomy_table.c.npi,
            func.json_agg(
                literal_column(f'distinct "{taxonomy_model.__tablename__}"')
            ).label("rows"),
        )
        .select_from(taxonomy_table)
        .where(taxonomy_table.c.npi == npi)
        .group_by(taxonomy_table.c.npi)
        .subquery(alias)
    )


def _npi_detail_query(npi: int, address_subquery: Any) -> Any:
    npi_data_table = NPIData.__table__
    taxonomy_aggregate = _npi_detail_taxonomy_aggregate(
        NPIDataTaxonomy,
        npi,
        "taxonomy_aggregate",
    )
    taxonomy_group_aggregate = _npi_detail_taxonomy_aggregate(
        NPIDataTaxonomyGroup,
        npi,
        "taxonomy_group_aggregate",
    )
    select_columns = [
        *_npi_serving_columns(),
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
    return (
        db.select(*select_columns)
        .select_from(join_clause)
        .where(npi_data_table.c.npi == npi)
    )


async def _build_npi_details(
    npi: int,
    *,
    include_sources: bool = False,
    include_evidence: bool = False,
    address_limit: int | None = None,
    include_address_total: bool = True,
    address_key: str | None = None,
    address_row_identities: Sequence[str] | None = None,
    session: Any = None,
) -> dict:
    """Assemble one provider identity, taxonomy, and address detail payload."""
    address_model, address_table, address_columns = await _npi_detail_address_context(
        include_sources=include_sources,
        include_evidence=include_evidence,
        include_location_key=address_row_identities is not None,
        session=session,
    )
    address_filters = _npi_detail_address_filters(
        address_model,
        address_table,
        npi,
        address_key,
        address_row_identities,
    )
    address_total: int | None = None
    if address_limit is not None and include_address_total:
        address_total = await _count_npi_detail_addresses(
            address_model,
            address_table,
            address_filters,
            session=session,
        )
    address_subquery = _npi_detail_address_subquery(
        address_model,
        address_table,
        address_columns,
        address_filters,
        address_limit,
    )
    query = _npi_detail_query(npi, address_subquery)

    if session is not None:
        detail_query_result = await session.execute(query._stmt)
        detail_rows = detail_query_result.all()
    else:
        detail_rows = await query.all()
    if not detail_rows:
        return {}
    return _npi_detail_from_result_row(
        detail_rows[0],
        include_address_rows=True,
        address_total=address_total,
    )


async def _fetch_other_names_map(
    npis: Sequence[int],
    *,
    session: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    """Fetch and deduplicate other-name rows for many NPIs."""
    unique_npis = sorted({int(npi) for npi in npis})
    if not unique_npis:
        return {}
    result = await _execute_stmt(
        select(NPIDataOtherIdentifier).where(
            NPIDataOtherIdentifier.npi.in_(unique_npis)
        ),
        session=session,
    )
    rows_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    seen_checksums_by_npi: dict[int, set[int]] = defaultdict(set)
    for row in result.scalars():
        payload = row.to_json_dict()
        row_npi = int(payload.pop("npi", None) or row.npi)
        checksum = payload.pop("checksum", None)
        if checksum in seen_checksums_by_npi[row_npi]:
            continue
        if checksum is not None:
            seen_checksums_by_npi[row_npi].add(checksum)
        rows_by_npi[row_npi].append(payload)
    return dict(rows_by_npi)


async def _fetch_other_names(npi: int, *, session: Any = None) -> list[dict[str, Any]]:
    return (await _fetch_other_names_map([npi], session=session)).get(int(npi), [])


PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE = "provider_directory_address_overlay"


async def _npi_canonical_publication_identity(
    *,
    session: Any = None,
) -> str | None:
    """Return the uncached receipt identity for the six current NPI tables."""

    schema = _runtime_db_schema()
    receipt_table = _schema_cache_key("npi_canonical_publication_receipt")
    seal_table = _schema_cache_key("npi_canonical_publication_receipt_seal")
    live_table_by_oid_column = (
        ("npi_table_oid", "npi"),
        ("npi_address_table_oid", "npi_address"),
        ("npi_taxonomy_table_oid", "npi_taxonomy"),
        ("npi_taxonomy_group_table_oid", "npi_taxonomy_group"),
        ("npi_other_identifier_table_oid", "npi_other_identifier"),
        ("npi_phone_staffing_table_oid", "npi_phone_staffing"),
    )
    relation_predicates = " AND ".join(
        f"receipt.{oid_column}=to_regclass(:{table_name}_ref)::oid"
        for oid_column, table_name in live_table_by_oid_column
    )
    parameters_by_name = {
        f"{table_name}_ref": f"{schema}.{table_name}"
        for _, table_name in live_table_by_oid_column
    }
    try:
        identity_result = await _execute_stmt(
            text(
                f"SELECT receipt.publication_generation, receipt.publication_ref "
                f"FROM {receipt_table} AS receipt "
                f"JOIN {seal_table} AS sealed USING (publication_ref) "
                f"WHERE {relation_predicates} "
                "ORDER BY receipt.publication_generation DESC LIMIT 2"
            ),
            session=session,
            params=parameters_by_name,
        )
        identity_rows = identity_result.all()
    except Exception as exc:
        logger.debug(
            "Canonical NPI publication identity unavailable; bypassing cache (%s)",
            type(exc).__name__,
        )
        return None
    if len(identity_rows) != 1:
        return None
    publication_generation, publication_ref = identity_rows[0]
    if (
        type(publication_generation) is not int
        or publication_generation < 1
        or type(publication_ref) is not str
        or len(publication_ref) != 50
        or not publication_ref.startswith("nppub1_")
    ):
        return None
    return f"{publication_generation}:{publication_ref}"


async def _npi_count_cache_identity(address_model: Any) -> str | None:
    """Bind count caches to both canonical NPI and address-serving generations."""

    publication_identity = await _npi_canonical_publication_identity()
    if publication_identity is None:
        return None
    if address_model is NPIAddress:
        return f"{publication_identity}|address:npi-publication"
    if address_model is not EntityAddressUnified:
        return None
    try:
        relation_result = await _execute_stmt(
            text("SELECT to_regclass(:table_ref)::oid::bigint"),
            params={"table_ref": _schema_cache_key(address_model.__tablename__)},
        )
        relation_oid = relation_result.scalar()
    except Exception:
        return None
    if type(relation_oid) is not int or relation_oid < 1:
        return None
    return f"{publication_identity}|address:oid:{relation_oid}"


async def _provider_directory_address_overlay_serving_identity(
    *,
    session: Any = None,
) -> str:
    """Return both address-serving relation identities used by response caches."""
    overlay_table_ref = _schema_cache_key(
        PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
    )
    unified_table_ref = _schema_cache_key(EntityAddressUnified.__tablename__)
    identity_result = await _execute_stmt(
        text(
            "SELECT "
            "to_regclass(:overlay_table_ref)::oid::bigint "
            "AS overlay_target_oid, "
            "to_regclass(:unified_table_ref)::oid::bigint "
            "AS unified_target_oid;"
        ),
        session=session,
        params={
            "overlay_table_ref": overlay_table_ref,
            "unified_table_ref": unified_table_ref,
        },
    )
    identity_row = identity_result.first()
    if identity_row is None:
        raise RuntimeError("address_serving_identity_missing")

    def _serialize_relation_oid(raw_oid: Any, relation_name: str) -> str:
        if raw_oid is None:
            return "absent"
        if isinstance(raw_oid, bool):
            raise RuntimeError(f"{relation_name}_identity_invalid")
        try:
            normalized_oid = int(raw_oid)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{relation_name}_identity_invalid") from exc
        if normalized_oid < 1:
            raise RuntimeError(f"{relation_name}_identity_invalid")
        return f"oid:{normalized_oid}"

    overlay_identity = _serialize_relation_oid(
        identity_row.overlay_target_oid,
        "provider_directory_address_overlay",
    )
    unified_identity = _serialize_relation_oid(
        identity_row.unified_target_oid,
        "entity_address_unified",
    )
    return f"overlay:{overlay_identity}|unified:{unified_identity}"


def _directory_current_dataset_ctes_sql(schema: str) -> str:
    return f"""
    current_endpoint_counts AS MATERIALIZED (
        SELECT dataset.endpoint_id
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
         WHERE dataset.is_current IS TRUE
      GROUP BY dataset.endpoint_id
        HAVING COUNT(*) = 1
    ), current_datasets AS MATERIALIZED (
        SELECT dataset.endpoint_id, dataset.dataset_id,
               COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id)::varchar AS run_id
          FROM {schema}.provider_directory_endpoint_dataset AS dataset
          JOIN current_endpoint_counts AS current_endpoint
            ON current_endpoint.endpoint_id = dataset.endpoint_id
         WHERE dataset.is_current IS TRUE
           AND dataset.status = 'published'
           AND dataset.published_at IS NOT NULL
           AND dataset.superseded_at IS NULL
           AND COALESCE(dataset.acquisition_root_run_id, dataset.import_run_id) IS NOT NULL
    )
    """


def _directory_overlay_resource_ctes_sql(schema: str) -> str:
    """Resolve current overlay resources without parsing unrelated graph metadata."""
    return f"""
    {_directory_current_dataset_ctes_sql(schema)}, current_resources AS NOT MATERIALIZED (
        SELECT source.source_id, source.canonical_api_base,
               dataset.dataset_id, dataset.run_id,
               resource.resource_type, resource.resource_id,
               resource.payload_json::jsonb AS payload_json
          FROM {schema}.provider_directory_source AS source
          JOIN current_datasets AS dataset
            ON dataset.endpoint_id = source.endpoint_id
          JOIN {schema}.provider_directory_dataset_resource AS resource
            ON resource.dataset_id = dataset.dataset_id
    )
    """


def _overlay_coordinate_sql(overlay_columns: set[str]) -> tuple[str, str, str]:
    lat_select = "lat" if "lat" in overlay_columns else "NULL::numeric AS lat"
    long_select = "long" if "long" in overlay_columns else "NULL::numeric AS long"
    coordinate_group_by = ", lat, long" if {"lat", "long"}.issubset(overlay_columns) else ""
    return lat_select, long_select, coordinate_group_by


def _overlay_formatted_address_sql(overlay_columns: set[str]) -> str:
    """Select a persisted overlay label without doing request-time rendering."""
    if "formatted_address" not in overlay_columns:
        return "NULL::varchar AS formatted_address"
    return (
        "MAX(NULLIF(BTRIM(formatted_address), ''))::varchar "
        "AS formatted_address"
    )


def _overlay_premise_key_sql(
    overlay_columns: set[str],
) -> tuple[str, str, str]:
    """Return select, predicate, and grouping SQL for stored premise keys."""
    if "premise_key" not in overlay_columns:
        return (
            "NULL::uuid AS premise_key",
            "AND CAST(:address_site_key AS uuid) IS NULL",
            "",
        )
    return (
        "premise_key",
        """AND (
                   CAST(:address_site_key AS uuid) IS NULL
                   OR overlay.premise_key = CAST(:address_site_key AS uuid)
               )""",
        ", premise_key",
    )


def _overlay_location_status_sql() -> str:
    """Return the current-resource location-status aggregate."""
    return _provider_directory_location_status_sql(
        "overlay.payload_json",
        resource_type_sql="overlay.resource_type",
    )


_PROVIDER_DIRECTORY_OVERLAY_QUERY_TEMPLATE = """
        WITH {current_resource_ctes_sql}, visible_overlay AS MATERIALIZED (
            SELECT overlay.*, current_resource.canonical_api_base,
                   current_resource.payload_json
              FROM {overlay_table_sql} AS overlay
              JOIN current_resources AS current_resource
                ON current_resource.source_id = overlay.source_id
               AND current_resource.resource_type = overlay.resource_type
               AND current_resource.resource_id = overlay.resource_id
               AND overlay.last_seen_run_id = current_resource.run_id
             WHERE overlay.npi = ANY(:npis)
               AND (
                   CAST(:address_key AS uuid) IS NULL
                   OR overlay.address_key = CAST(:address_key AS uuid)
               )
               {premise_filter}
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
            {formatted_address_select},
            {lat_select},
            {long_select},
            address_key,
            {premise_select},
            address_precision,
            ARRAY['provider_directory_fhir']::varchar[] AS address_sources,
            ARRAY_AGG(overlay.source_record_id ORDER BY overlay.source_record_id)::varchar[] AS source_record_ids,
            COUNT(DISTINCT overlay.source_id)::integer AS source_count,
            COUNT(DISTINCT COALESCE(NULLIF(overlay.canonical_api_base, ''), overlay.source_id))::integer AS independent_source_count,
            (COUNT(DISTINCT COALESCE(NULLIF(overlay.canonical_api_base, ''), overlay.source_id)) > 1)::boolean AS multi_source_confirmed,
            {location_status_select} AS location_status,
            MAX(source_updated_at) AS updated_at
          FROM visible_overlay AS overlay
      GROUP BY
            npi, first_line, second_line, city_name, state_name, state_code,
            postal_code, country_code, telephone_number, fax_number, phone_number,
            fax_number_digits, address_key, address_precision{coordinate_group_by}{premise_group_by}
      ORDER BY first_line NULLS LAST, city_name NULLS LAST, address_key;
"""


def _provider_directory_overlay_query_sql(
    overlay_columns: set[str],
) -> str:
    """Build the current-resource provider-directory address overlay query."""
    lat_select, long_select, coordinate_group_by = _overlay_coordinate_sql(overlay_columns)
    premise_select, premise_filter, premise_group_by = _overlay_premise_key_sql(
        overlay_columns
    )
    overlay_table_sql = _schema_cache_key(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    current_resource_ctes_sql = _directory_overlay_resource_ctes_sql(
        _runtime_db_schema()
    )
    return _PROVIDER_DIRECTORY_OVERLAY_QUERY_TEMPLATE.format(
        current_resource_ctes_sql=current_resource_ctes_sql,
        overlay_table_sql=overlay_table_sql,
        premise_filter=premise_filter,
        formatted_address_select=_overlay_formatted_address_sql(overlay_columns),
        lat_select=lat_select,
        long_select=long_select,
        premise_select=premise_select,
        location_status_select=_overlay_location_status_sql(),
        coordinate_group_by=coordinate_group_by,
        premise_group_by=premise_group_by,
    )


def _provider_directory_location_status_sql(
    payload_sql: str,
    *,
    resource_type_sql: str,
) -> str:
    role_start = f"{payload_sql} -> 'period' ->> 'start'"
    role_end = f"{payload_sql} -> 'period' ->> 'end'"
    role_start_date = f"""
        CASE
            WHEN pg_input_is_valid(LEFT(COALESCE({role_start}, ''), 10), 'date')
            THEN LEFT({role_start}, 10)::date
            ELSE NULL::date
        END
    """
    role_end_date = f"""
        CASE
            WHEN pg_input_is_valid(LEFT(COALESCE({role_end}, ''), 10), 'date')
            THEN LEFT({role_end}, 10)::date
            ELSE NULL::date
        END
    """
    role_active = f"""
        LOWER(COALESCE({payload_sql} ->> 'active', '')) = 'true'
        AND (
            COALESCE({role_start}, '') = ''
            OR ({role_start_date}) <= CURRENT_DATE
        )
        AND (
            COALESCE({role_end}, '') = ''
            OR ({role_end_date}) >= CURRENT_DATE
        )
    """
    role_inactive = f"""
        LOWER(COALESCE({payload_sql} ->> 'active', '')) = 'false'
        OR ({role_start_date}) > CURRENT_DATE
        OR ({role_end_date}) < CURRENT_DATE
    """
    return f"""
        CASE
            WHEN BOOL_OR(
                {resource_type_sql} = 'PractitionerRole'
                AND ({role_active})
            ) THEN 'active'
            WHEN COUNT(*) FILTER (
                WHERE {resource_type_sql} = 'PractitionerRole'
            ) > 0
            AND BOOL_AND(COALESCE(({role_inactive}), FALSE)) FILTER (
                WHERE {resource_type_sql} = 'PractitionerRole'
            ) THEN 'inactive'
            ELSE 'unknown'
        END::varchar
    """


async def _fetch_provider_directory_address_overlay_map(
    npis: Sequence[int],
    *,
    address_key: str | None = None,
    address_site_key: str | None = None,
    session: Any = None,
) -> dict[int, list[dict[str, Any]]]:
    """Fetch current FHIR address evidence for many NPIs in one query."""
    unique_npis = sorted({int(npi) for npi in npis})
    if not unique_npis:
        return {}
    if not await _is_table_available(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, session=session):
        return {}
    visibility_table_states = [
        await _is_table_available(table_name, session=session)
        for table_name in PROVIDER_DIRECTORY_VISIBILITY_TABLES
    ]
    if not all(visibility_table_states):
        return {}
    overlay_columns = await _table_columns(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, session=session)
    overlay_query = text(
        _provider_directory_overlay_query_sql(
            overlay_columns,
        )
    )
    overlay_result = await _execute_stmt(
        overlay_query,
        session=session,
        params={
            "npis": unique_npis,
            "address_key": address_key,
            "address_site_key": address_site_key,
        },
    )
    overlay_addresses_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for overlay_row in overlay_result.all():
        overlay_mapping = getattr(overlay_row, "_mapping", overlay_row)
        overlay_address_by_field = dict(overlay_mapping)
        if UHC_PROVIDER_FILE_SOURCE_ID in _directory_source_ids(
            overlay_address_by_field.get("source_record_ids")
        ):
            overlay_address_by_field["address_status"] = (
                UHC_PROVIDER_FILE_ADDRESS_STATUS
            )
        overlay_npi = overlay_address_by_field.get("npi")
        if overlay_npi is None and len(unique_npis) == 1:
            overlay_npi = unique_npis[0]
        if overlay_npi is not None:
            overlay_addresses_by_npi[int(overlay_npi)].append(
                overlay_address_by_field
            )
    return dict(overlay_addresses_by_npi)


async def _fetch_provider_directory_address_overlay(
    npi: int,
    *,
    address_key: str | None = None,
    address_site_key: str | None = None,
    session: Any = None,
) -> list[dict[str, Any]]:
    """Fetch FHIR address evidence with endpoint-aware confirmation counts."""
    return (
        await _fetch_provider_directory_address_overlay_map(
            [npi],
            address_key=address_key,
            address_site_key=address_site_key,
            session=session,
        )
    ).get(int(npi), [])


def _location_status_query(schema: str, overlay_table_sql: str) -> Any:
    current_dataset_ctes_sql = _directory_current_dataset_ctes_sql(schema)
    role_table_sql = f"{schema}.{ProviderDirectoryPractitionerRole.__tablename__}"
    resource_table_sql = f"{schema}.provider_directory_dataset_resource"
    location_status_sql = _provider_directory_location_status_sql(
        "visible_role.payload_json",
        resource_type_sql="'PractitionerRole'",
    )
    # Typed rows are mutable; the immutable dataset payload preserves current
    # publication semantics when a checkpoint run ID differs.
    return text(
        f"""
        WITH {current_dataset_ctes_sql}, matched_overlays AS MATERIALIZED (
            SELECT overlay.source_record_id,
                   overlay.source_id,
                   overlay.resource_id,
                   overlay.last_seen_run_id
              FROM {overlay_table_sql} AS overlay
             WHERE overlay.source_record_id = ANY(:source_record_ids)
               AND overlay.resource_type = 'PractitionerRole'
        ), visible_roles AS NOT MATERIALIZED (
            SELECT overlay.source_record_id,
                   CASE
                       WHEN role.resource_id IS NOT NULL THEN jsonb_build_object(
                           'active', role.active,
                           'period', jsonb_build_object(
                               'start', role.period_start,
                               'end', role.period_end
                           )
                       )
                       ELSE resource.payload_json::jsonb
                   END AS payload_json
              FROM matched_overlays AS overlay
              JOIN {schema}.provider_directory_source AS source
                ON source.source_id = overlay.source_id
              JOIN current_datasets AS dataset
                ON dataset.endpoint_id = source.endpoint_id
               AND dataset.run_id = overlay.last_seen_run_id
         LEFT JOIN {role_table_sql} AS role
                ON role.source_id = overlay.source_id
               AND role.resource_id = overlay.resource_id
               AND role.last_seen_run_id = dataset.run_id
         LEFT JOIN {resource_table_sql} AS resource
                ON role.resource_id IS NULL
               AND resource.dataset_id = dataset.dataset_id
               AND resource.resource_type = 'PractitionerRole'
               AND resource.resource_id = overlay.resource_id
             WHERE role.resource_id IS NOT NULL
                OR resource.resource_id IS NOT NULL
        )
        SELECT visible_role.source_record_id,
               {location_status_sql} AS location_status
          FROM visible_roles AS visible_role
      GROUP BY visible_role.source_record_id;
        """
    )


def _status_map_from_result(query_result: Any) -> dict[str, str]:
    status_by_record_id: dict[str, str] = {}
    for status_record in query_result.all():
        mapping = getattr(status_record, "_mapping", status_record)
        record_id = str(mapping["source_record_id"] or "").strip()
        if record_id:
            status_by_record_id[record_id] = str(
                mapping["location_status"] or "unknown"
            ).lower()
    return status_by_record_id


async def _fetch_location_status_by_record_id(
    source_record_ids: Sequence[Any],
    *,
    session: Any = None,
) -> dict[str, str]:
    """Resolve active/inactive/unknown for current PractitionerRole evidence."""
    normalized_record_ids = sorted(
        {
            str(record_id).strip()
            for record_id in source_record_ids
            if str(record_id or "").strip().startswith(
                "provider_directory_fhir:practitioner_role:"
            )
        }
    )
    if not normalized_record_ids:
        return {}
    try:
        overlay_table_sql = _schema_cache_key(
            PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE
        )
        status_query = _location_status_query(
            _runtime_db_schema(),
            overlay_table_sql,
        )
        async with db.session() as status_session:
            query_result = await _execute_stmt(
                status_query,
                session=status_session,
                params={"source_record_ids": normalized_record_ids},
            )
            return _status_map_from_result(query_result)
    except Exception as exc:
        logger.debug("Provider location status lookup failed: %s", exc)
        return {}


def _location_status_from_source_records(
    source_record_ids: Any,
    status_by_record_id: Mapping[str, str],
) -> str:
    candidates = (
        source_record_ids
        if isinstance(source_record_ids, (list, tuple, set))
        else [source_record_ids]
    )
    statuses = []
    for record_id in candidates:
        normalized_record_id = str(record_id or "").strip()
        if not normalized_record_id:
            continue
        status = str(
            status_by_record_id.get(normalized_record_id) or "unknown"
        ).strip().lower()
        statuses.append(
            status if status in {"active", "inactive"} else "unknown"
        )
    if "active" in statuses:
        return "active"
    if statuses and all(status == "inactive" for status in statuses):
        return "inactive"
    return "unknown"


async def _apply_location_statuses(
    addresses: Sequence[Any],
    *,
    session: Any = None,
) -> None:
    """Attach conservative per-location status without overriding unknown evidence."""
    source_record_ids = [
        record_id
        for address in addresses
        if isinstance(address, dict)
        for record_id in _merge_unique_list_values(
            address.get("source_record_ids"),
            None,
        )
        if str(record_id or "").strip()
    ]
    status_by_record_id = (
        await _fetch_location_status_by_record_id(
            source_record_ids,
            session=session,
        )
        if source_record_ids
        else {}
    )
    for address in addresses:
        if not isinstance(address, dict):
            continue
        directory_status = _location_status_from_source_records(
            address.get("source_record_ids"),
            status_by_record_id,
        )
        raw_sources = address.get("address_sources") or []
        address_sources = (
            raw_sources
            if isinstance(raw_sources, (list, tuple, set))
            else [raw_sources]
        )
        has_non_directory_source = any(
            str(source_id or "").strip().lower()
            not in {"", "provider_directory_fhir"}
            for source_id in address_sources
        )
        address["location_status"] = (
            "unknown"
            if directory_status == "inactive" and has_non_directory_source
            else directory_status
        )


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
