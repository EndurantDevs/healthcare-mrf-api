# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import base64
import datetime
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp
from sqlalchemy import case, func, or_
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import (
    ProviderDirectoryCapability,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryEndpoint,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
    ProviderDirectorySource,
    ProviderDirectorySourceResource,
    db,
)
from process.control_lifecycle import mark_control_run
from process.control_cancel import raise_if_cancelled
from process.ext.address_canon import resolve_into_archive
from process.ext.contact_canon import canonicalize_one as canonicalize_contact_one
from process.ext.utils import ensure_database


DEFAULT_SEED_DB_URL = (
    "https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/provider_directory.db"
)
LOGGER = logging.getLogger(__name__)
MAX_FHIR_JSON_BODY_BYTES = 20 * 1024 * 1024
READ_CHUNK_BYTES = 256 * 1024
DEFAULT_RESOURCES = (
    "InsurancePlan",
    "PractitionerRole",
    "Practitioner",
    "Organization",
    "Location",
    "HealthcareService",
    "OrganizationAffiliation",
    "Endpoint",
)
FHIR_RESOURCE_PATH_SEGMENTS = frozenset(
    resource_type.lower() for resource_type in DEFAULT_RESOURCES
)
FHIR_BASE_TRAILING_PATH_SEGMENTS = frozenset({"provider-directory", "providerdirectory"})
LINKED_RESOURCE_TYPES = frozenset(
    {
        "InsurancePlan",
        "Practitioner",
        "Organization",
        "Location",
        "HealthcareService",
        "Endpoint",
    }
)
LINKED_REFERENCE_FIELDS = {
    "InsurancePlan": {
        "Organization": ("owned_by_ref", "administered_by_ref", "network_refs"),
        "Location": ("coverage_area_refs",),
    },
    "PractitionerRole": {
        "Practitioner": ("practitioner_ref",),
        "Organization": ("organization_ref", "network_refs"),
        "Location": ("location_refs",),
        "HealthcareService": ("healthcare_service_refs",),
        "InsurancePlan": ("insurance_plan_refs",),
        "Endpoint": ("endpoint_refs",),
    },
    "Organization": {
        "Endpoint": ("endpoint_refs",),
    },
    "HealthcareService": {
        "Location": ("location_refs",),
        "Endpoint": ("endpoint_refs",),
    },
    "OrganizationAffiliation": {
        "Organization": ("organization_ref", "participating_organization_ref", "network_refs"),
        "Location": ("location_refs",),
        "HealthcareService": ("healthcare_service_refs",),
        "Endpoint": ("endpoint_refs",),
    },
}
RESOURCE_ENDPOINT_FIELDS = {
    "InsurancePlan": "endpoint_insurance_plan",
    "Practitioner": "endpoint_practitioner",
    "Organization": "endpoint_organization",
    "Location": "endpoint_location",
    "PractitionerRole": "endpoint_practitioner_role",
    "HealthcareService": "endpoint_healthcare_service",
    "OrganizationAffiliation": "endpoint_organization_affiliation",
    "Endpoint": "endpoint_endpoint",
}
PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW = "ptg_provider_directory_address_corroboration"
PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE = "provider_directory_import_seen"
PROVIDER_DIRECTORY_IMPORT_SEEN_STAGE_PREFIX = "provider_directory_import_seen_stage"
PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_INDEXES = (
    "pd_ptg_corrob_lookup_idx",
    "pd_ptg_corrob_active_lookup_idx",
    "pd_ptg_corrob_source_snapshot_idx",
    "pd_ptg_corrob_plan_pair_idx",
    "pd_ptg_corrob_pd_source_idx",
    "pd_ptg_corrob_network_names_gin",
)
PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT = 128
PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY = 6
PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_STAGE_PREFIX = "provider_directory_location_archive_stage"
RESOURCE_MODELS_BY_TYPE = {
    "InsurancePlan": ProviderDirectoryInsurancePlan,
    "Practitioner": ProviderDirectoryPractitioner,
    "Organization": ProviderDirectoryOrganization,
    "Location": ProviderDirectoryLocation,
    "PractitionerRole": ProviderDirectoryPractitionerRole,
    "HealthcareService": ProviderDirectoryHealthcareService,
    "OrganizationAffiliation": ProviderDirectoryOrganizationAffiliation,
    "Endpoint": ProviderDirectoryEndpoint,
}
RESOURCE_TYPES_BY_MODEL = {model: resource_type for resource_type, model in RESOURCE_MODELS_BY_TYPE.items()}
SOURCE_MODELS = (
    ProviderDirectorySource,
    ProviderDirectoryCapability,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryPractitioner,
    ProviderDirectoryOrganization,
    ProviderDirectoryLocation,
    ProviderDirectoryPractitionerRole,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryEndpoint,
)
CANONICAL_RESOURCE_MODELS = (
    ProviderDirectoryCanonicalResource,
    ProviderDirectorySourceResource,
)
USER_AGENT = os.getenv(
    "HLTHPRT_PROVIDER_DIRECTORY_USER_AGENT",
    "HealthPortaProviderDirectoryImporter/0.1 (+https://app.healthporta.com)",
)
ZIP5_RE = re.compile(r"(?<!\d)(\d{5})(?:-\d{4})?(?!\d)")
US_STATE_FIPS_TO_ABBR = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "60": "AS",
    "66": "GU",
    "69": "MP",
    "72": "PR",
    "78": "VI",
}
PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON"
PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE"
SECRET_ENV_PREFIX = "env:"
DEFAULT_FULL_REFRESH_MAX_PAGES = 10000
DEFAULT_STREAM_BATCH_SIZE = 5000
DEFAULT_BULK_EXPORT_MAX_POLLS = 120
DEFAULT_BULK_EXPORT_POLL_SECONDS = 5
OAUTH_TOKEN_EXPIRY_SKEW_SECONDS = 60
_OAUTH_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
FHIR_ONBOARDING_GATEWAY_HOSTS = {
    "apps.availity.com",
    "partners.centene.com",
}
ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE = "https://alohr.esante.us/public/providers"
ALOHR_FHIR_PROVIDER_DIRECTORY_BASE = "https://fhir.alabamaonehealthrecord.com/csp/healthshare/hsods/fhir/r4"
ALOHR_GRAPHQL_URL = "https://api.esante.us/graphql"
ALOHR_TENANT_ID = "alohr"
ALOHR_PROVIDER_QUERY = """
query ALOHR_PROVIDER_FIND($criteria: ProviderSearchCriteria, $nextToken: String) {
  providers(criteria: $criteria, nextToken: $nextToken) {
    providers {
      providerId
      firstName
      lastName
      npi
      phone
      email
      specialty
      specialtyDescription
      telehealth
      acceptingMedicaid
      languages
      address { street1 street2 city state zip county }
      addresses { type street1 street2 city state zip county }
    }
    nextToken
  }
}
"""
ALOHR_ORGANIZATION_QUERY = """
query ALOHR_ORG_FIND($criteria: ProviderOrgSearchCriteria, $nextToken: String) {
  providerOrgs(criteria: $criteria, nextToken: $nextToken) {
    providerOrganizations {
      orgId
      name
      npi
      specialty
      specialtyDescription
      organizationContact { system value use }
      addresses { type street1 street2 city state zip county }
      contacts { contacts { system value use } }
    }
    nextToken
  }
}
"""
AETNA_PROVIDER_DIRECTORY_BASE = "https://apif1.aetna.com/fhir/v1/providerdirectory"


@dataclass(frozen=True)
class ResourceFetchResult:
    model: type
    rows: list[dict[str, Any]]
    rows_fetched: int
    rows_written: int
    pages_fetched: int
    complete: bool
    row_limit_reached: bool
    page_limit_reached: bool
    hard_page_limit_reached: bool
    next_url_remaining: bool
    error: str | None = None
    fetch_mode: str = "paged"

    @property
    def bounded(self) -> bool:
        return self.row_limit_reached or self.page_limit_reached or self.hard_page_limit_reached


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_ref_matches_resource(ref_expr: str, resource_type: str, resource_id_expr: str) -> str:
    resource_type_literal = str(resource_type).replace("'", "''")
    return (
        f"({ref_expr} IN ({resource_id_expr}, '{resource_type_literal}/' || {resource_id_expr}) "
        f"OR {ref_expr} LIKE '%/{resource_type_literal}/' || {resource_id_expr})"
    )


def _now() -> datetime.datetime:
    return datetime.datetime.utcnow()


def _normalize_import_id(raw: str | None) -> str:
    raw = raw or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_IMPORT_ID") or os.getenv("HLTHPRT_IMPORT_ID_OVERRIDE")
    if raw:
        cleaned = "".join(ch for ch in str(raw) if ch.isalnum())
        if cleaned:
            return cleaned[:32]
    return _now().strftime("%Y%m%d%H%M%S")


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_state_code(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    upper = text.upper()
    if re.fullmatch(r"[A-Z]{2}", upper):
        return upper
    if re.fullmatch(r"\d{1,2}", upper):
        return US_STATE_FIPS_TO_ABBR.get(upper.zfill(2))
    return None


def _normalize_state_name(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if re.fullmatch(r"\d{1,2}", text):
        return _normalize_state_code(text)
    return text


def _normalize_country_code(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    token = re.sub(r"[^A-Za-z0-9]+", "", text).upper()
    if token in {"US", "USA", "UNITEDSTATES", "UNITEDSTATESOFAMERICA", "840", "001"}:
        return "US"
    if re.fullmatch(r"[A-Z]{2}", token):
        return token
    return text.upper()


def _state_fips_restore_sql(value_sql: str) -> str:
    cases = "\n".join(
        f"                    WHEN '{fips}' THEN '{abbr}'"
        for fips, abbr in sorted(US_STATE_FIPS_TO_ABBR.items())
    )
    cleaned = f"NULLIF(BTRIM(({value_sql})::varchar), '')"
    return f"""
        CASE
            WHEN {cleaned} ~ '^[0-9]{{1,2}}$' THEN
                CASE LPAD({cleaned}, 2, '0')
{cases}
                    ELSE NULL
                END
            ELSE {value_sql}
        END
    """


def _country_restore_sql(value_sql: str) -> str:
    cleaned = f"NULLIF(BTRIM(({value_sql})::varchar), '')"
    token = f"UPPER(REGEXP_REPLACE(COALESCE(({value_sql})::varchar, ''), '[^A-Za-z0-9]+', '', 'g'))"
    return f"""
        CASE
            WHEN {cleaned} IS NULL THEN NULL::varchar
            WHEN {token} IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001') THEN 'US'
            ELSE UPPER({cleaned})
        END
    """


def _bool_from_seed(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _canonical_base(api_base: str | None) -> str | None:
    text = _clean_text(api_base)
    if not text or text.upper() == "N/A":
        return None
    parsed = urllib.parse.urlsplit(text)
    if not parsed.scheme or not parsed.netloc:
        return text.rstrip("/")
    path = parsed.path.rstrip("/")
    return urllib.parse.urlunsplit((parsed.scheme.lower(), parsed.netloc.lower(), path, "", ""))


def _parent_base_url(api_base: str) -> str | None:
    parsed = urllib.parse.urlsplit(api_base)
    if not parsed.scheme or not parsed.netloc:
        return None
    path = parsed.path.rstrip("/")
    if not path or path == "/":
        return None
    parent = path.rsplit("/", 1)[0] or ""
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parent.rstrip("/"), "", ""))


def _append_unique(values: list[str], value: str | None) -> None:
    if value and value not in values:
        values.append(value)


def _candidate_base_urls(source: dict[str, Any]) -> list[str]:
    candidates: list[str] = []

    def add(raw_url: str | None) -> None:
        api_base = _canonical_base(raw_url)
        if not api_base:
            return
        parsed = urllib.parse.urlsplit(api_base)
        if not parsed.scheme or not parsed.netloc:
            return
        _append_unique(candidates, api_base)
        segments = [segment for segment in parsed.path.strip("/").split("/") if segment]
        last_segment = segments[-1].lower() if segments else ""
        if last_segment == "metadata":
            _append_unique(candidates, _parent_base_url(api_base))
            return
        if last_segment in FHIR_RESOURCE_PATH_SEGMENTS or last_segment in FHIR_BASE_TRAILING_PATH_SEGMENTS:
            _append_unique(candidates, _parent_base_url(api_base))

    add(source.get("api_base"))
    for field in (
        "endpoint_insurance_plan",
        "endpoint_practitioner",
        "endpoint_practitioner_role",
        "endpoint_organization",
        "endpoint_organization_affiliation",
        "endpoint_location",
        "endpoint_healthcare_service",
        "endpoint_network",
        "endpoint_endpoint",
    ):
        add(source.get(field))
    return candidates


def _candidate_metadata_urls(source: dict[str, Any]) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    seen: set[str] = set()
    for api_base in _candidate_base_urls(source):
        for url in (f"{api_base}/metadata?_format=json", f"{api_base}/metadata"):
            if url in seen:
                continue
            urls.append((api_base, url))
            seen.add(url)
    return urls


def _is_placeholder_url(value: str | None) -> bool:
    text = (value or "").strip()
    if not text or text.upper() in {"N/A", "NA", "NONE", "NULL", "UNCONFIRMED", "TBD"}:
        return True
    lowered = text.lower()
    return (
        "may need query parameter" in lowered
        or "http 400" in lowered
        or "http 404" in lowered
        or "not a direct endpoint" in lowered
        or "unreachable" in lowered
    )


def _url_with_count(url: str, page_count: int) -> str:
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "_count" for key, _value in query_items):
        query_items.append(("_count", str(max(1, min(page_count, 100)))))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _resource_start_url(source: dict[str, Any], resource_type: str, *, page_count: int) -> str | None:
    api_base = _canonical_base(source.get("api_base"))
    endpoint_field = RESOURCE_ENDPOINT_FIELDS.get(resource_type)
    endpoint = _clean_text(source.get(endpoint_field)) if endpoint_field else None
    if endpoint and not _is_placeholder_url(endpoint):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme and parsed.netloc:
            return _url_with_count(endpoint, page_count)
        if api_base:
            return _url_with_count(
                urllib.parse.urljoin(api_base.rstrip("/") + "/", endpoint),
                page_count,
            )
    if not api_base:
        return None
    return _url_with_count(f"{api_base}/{resource_type}", page_count)


def _load_credentials_config() -> dict[str, Any]:
    config: dict[str, Any] = {}
    path = _clean_text(os.getenv(PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV))
    if path:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                config.update(payload)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            pass
    raw = _clean_text(os.getenv(PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV))
    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                config.update(payload)
        except json.JSONDecodeError:
            pass
    return config


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _normalize_credential_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _resolve_secret_text(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    if text.startswith(SECRET_ENV_PREFIX):
        return _clean_text(os.getenv(text[len(SECRET_ENV_PREFIX) :]))
    return text


def _merge_credential_spec(base: dict[str, Any], overlay: dict[str, Any], *, matched_by: str) -> dict[str, Any]:
    merged = dict(base)
    merged_headers = {**_mapping(base.get("headers")), **_mapping(overlay.get("headers"))}
    merged_query = {
        **_mapping(base.get("query")),
        **_mapping(base.get("query_params")),
        **_mapping(overlay.get("query")),
        **_mapping(overlay.get("query_params")),
    }
    if merged_headers:
        merged["headers"] = merged_headers
    if merged_query:
        merged["query_params"] = merged_query
    for key in ("bearer_token", "api_key", "oauth2", "oauth", "enabled"):
        if key in overlay:
            merged[key] = overlay[key]
    matched = list(merged.get("_matched_by") or [])
    matched.append(matched_by)
    merged["_matched_by"] = matched
    return merged


def _source_hosts(source: dict[str, Any]) -> set[str]:
    hosts: set[str] = set()
    for api_base in _candidate_base_urls(source):
        parsed = urllib.parse.urlsplit(api_base)
        if parsed.netloc:
            hosts.add(parsed.netloc.lower())
    return hosts


def _credential_spec_for_source(source: dict[str, Any]) -> dict[str, Any]:
    config = _load_credentials_config()
    if not config:
        return {}
    spec: dict[str, Any] = {}
    defaults = _mapping(config.get("defaults") or config.get("default"))
    if defaults:
        spec = _merge_credential_spec(spec, defaults, matched_by="defaults")

    source_id = _clean_text(source.get("source_id"))
    canonical_api_base = _canonical_base(source.get("api_base") or source.get("canonical_api_base"))
    host = urllib.parse.urlsplit(canonical_api_base or "").netloc.lower()
    org_name = _normalize_credential_key(source.get("org_name"))

    hosts = _mapping(config.get("hosts"))
    if host and host in {str(key).lower(): key for key in hosts}:
        key = {str(key).lower(): key for key in hosts}[host]
        spec = _merge_credential_spec(spec, _mapping(hosts.get(key)), matched_by=f"hosts:{host}")

    api_bases = _mapping(config.get("api_bases") or config.get("apiBases"))
    normalized_api_bases = {
        _canonical_base(str(key)) or str(key).rstrip("/"): key
        for key in api_bases
    }
    if canonical_api_base and canonical_api_base in normalized_api_bases:
        key = normalized_api_bases[canonical_api_base]
        spec = _merge_credential_spec(spec, _mapping(api_bases.get(key)), matched_by=f"api_bases:{canonical_api_base}")

    org_names = _mapping(config.get("org_names") or config.get("orgNames"))
    normalized_orgs = {_normalize_credential_key(key): key for key in org_names}
    if org_name and org_name in normalized_orgs:
        key = normalized_orgs[org_name]
        spec = _merge_credential_spec(spec, _mapping(org_names.get(key)), matched_by=f"org_names:{org_name}")

    sources = _mapping(config.get("sources"))
    if source_id and source_id in sources:
        spec = _merge_credential_spec(spec, _mapping(sources.get(source_id)), matched_by=f"sources:{source_id}")
    if spec.get("enabled") is False:
        return {}
    return spec


def _credential_allowed_for_url(source: dict[str, Any], url: str) -> bool:
    parsed = urllib.parse.urlsplit(url)
    if not parsed.netloc:
        return True
    return parsed.netloc.lower() in _source_hosts(source)


def _api_key_header(spec: dict[str, Any]) -> tuple[str, str] | None:
    api_key = spec.get("api_key")
    if isinstance(api_key, dict):
        value = _resolve_secret_text(api_key.get("value") or api_key.get("key"))
        header = _clean_text(api_key.get("header") or api_key.get("name")) or "X-API-Key"
    else:
        value = _resolve_secret_text(api_key)
        header = "X-API-Key"
    if not value:
        return None
    return header, value


def _oauth2_spec(spec: dict[str, Any]) -> dict[str, Any]:
    raw = spec.get("oauth2") if "oauth2" in spec else spec.get("oauth")
    if isinstance(raw, dict):
        nested = _mapping(raw.get("client_credentials") or raw.get("clientCredentials"))
        return {**raw, **nested}
    return {}


def _oauth2_cache_key(oauth2: dict[str, Any]) -> str:
    parts = [
        _clean_text(oauth2.get("token_url") or oauth2.get("tokenUrl")) or "",
        _resolve_secret_text(oauth2.get("client_id") or oauth2.get("clientId")) or "",
        _clean_text(oauth2.get("scope")) or "",
        _clean_text(oauth2.get("audience")) or "",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _fetch_oauth2_client_credentials_token_sync(oauth2: dict[str, Any], *, timeout: int = 15) -> str | None:
    token_url = _clean_text(oauth2.get("token_url") or oauth2.get("tokenUrl"))
    client_id = _resolve_secret_text(oauth2.get("client_id") or oauth2.get("clientId"))
    client_secret = _resolve_secret_text(oauth2.get("client_secret") or oauth2.get("clientSecret"))
    if not token_url or not client_id or not client_secret:
        return None

    cache_key = _oauth2_cache_key(oauth2)
    cached = _OAUTH_TOKEN_CACHE.get(cache_key)
    now = time.time()
    if cached and cached[1] > now:
        return cached[0]

    form: dict[str, str] = {"grant_type": "client_credentials"}
    for field in ("scope", "audience", "resource"):
        resolved = _resolve_secret_text(oauth2.get(field))
        if resolved:
            form[field] = resolved
    for key, value in _mapping(oauth2.get("extra_params") or oauth2.get("extraParams")).items():
        resolved = _resolve_secret_text(value)
        if resolved:
            form[str(key)] = resolved

    auth_mode = (_clean_text(oauth2.get("auth") or oauth2.get("client_auth") or oauth2.get("clientAuth")) or "basic").lower()
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": USER_AGENT,
    }
    if auth_mode in {"body", "post", "client_secret_post"}:
        form["client_id"] = client_id
        form["client_secret"] = client_secret
    else:
        token = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
        headers["Authorization"] = f"Basic {token}"

    request = urllib.request.Request(
        token_url,
        data=urllib.parse.urlencode(form).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_ssl_context()) as response:
            payload = _decode_json_body(response.read(1024 * 1024))
    except Exception:  # pylint: disable=broad-exception-caught
        return None
    if not payload:
        return None
    access_token = _clean_text(payload.get("access_token"))
    if not access_token:
        return None
    try:
        expires_in = int(payload.get("expires_in") or 3600)
    except Exception:
        expires_in = 3600
    _OAUTH_TOKEN_CACHE[cache_key] = (
        access_token,
        now + max(0, expires_in - OAUTH_TOKEN_EXPIRY_SKEW_SECONDS),
    )
    return access_token


def _credential_request_options_for_source(source: dict[str, Any], url: str) -> dict[str, Any]:
    spec = _credential_spec_for_source(source)
    if not spec or not _credential_allowed_for_url(source, url):
        return {"headers": {}, "query_params": {}, "descriptor": None}
    headers: dict[str, str] = {}
    bearer = _resolve_secret_text(spec.get("bearer_token"))
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    elif _oauth2_spec(spec):
        oauth_token = _fetch_oauth2_client_credentials_token_sync(_oauth2_spec(spec))
        if oauth_token:
            headers["Authorization"] = f"Bearer {oauth_token}"
    api_key_header = _api_key_header(spec)
    if api_key_header:
        headers[api_key_header[0]] = api_key_header[1]
    for key, value in _mapping(spec.get("headers")).items():
        resolved = _resolve_secret_text(value)
        if resolved:
            headers[str(key)] = resolved
    query_params: dict[str, str] = {}
    for key, value in _mapping(spec.get("query_params")).items():
        resolved = _resolve_secret_text(value)
        if resolved:
            query_params[str(key)] = resolved
    descriptor = None
    if headers or query_params:
        descriptor = {
            "matched_by": list(spec.get("_matched_by") or []),
            "header_names": sorted(headers),
            "query_param_names": sorted(query_params),
        }
    return {"headers": headers, "query_params": query_params, "descriptor": descriptor}


def _source_declares_credentialed_access(source: dict[str, Any]) -> bool:
    if _bool_from_seed(source.get("requires_registration")) or _bool_from_seed(source.get("requires_api_key")):
        return True
    auth_type = (_clean_text(source.get("auth_type")) or "").lower()
    if not auth_type or auth_type in {"n/a", "na", "none", "open", "public"}:
        return False
    return any(marker in auth_type for marker in ("oauth", "api key", "bearer", "token", "client credential"))


def _source_uses_known_onboarding_gateway(source: dict[str, Any]) -> bool:
    return bool(_source_hosts(source) & FHIR_ONBOARDING_GATEWAY_HOSTS)


def _url_with_query_params(url: str, query_params: dict[str, str] | None) -> str:
    if not query_params:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.extend((key, value) for key, value in query_params.items())
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), parsed.fragment)
    )


def _stable_source_id(row: dict[str, Any]) -> str:
    parts = [
        _clean_text(row.get("org_tin")) or "",
        _clean_text(row.get("org_name")) or "",
        _clean_text(row.get("plan_name")) or "",
        _canonical_base(row.get("api_base")) or "",
        _clean_text(row.get("source")) or "",
    ]
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"pdfhir_{digest}"


def _aetna_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    should_override = (
        api_base == "https://fhir-ehr.cerner.com/r4/aetna"
        or api_base == AETNA_PROVIDER_DIRECTORY_BASE
        or "developerportal.aetna.com" in portal_url
        or "developerportal.aetna.com" in source_url
    )
    if not should_override:
        return None
    return {
        "api_base": AETNA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": AETNA_PROVIDER_DIRECTORY_BASE,
        "requires_registration": True,
        "auth_type": _clean_text(row.get("auth_type")) or "OAuth2/SMART",
        "metadata": {
            "provider_directory_override": "aetna_apif1_providerdirectory",
            "provider_directory_override_reason": (
                "Aetna developer portal metadata is open, but resource reads require OAuth client credentials."
            ),
            "provider_directory_confirmed_base": AETNA_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_token_url": (
                "https://apif1.aetna.com/fhir/v1/fhirserver_auth/oauth2/token"
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
        },
    }


def _alohr_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    should_override = (
        api_base in {ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE, ALOHR_FHIR_PROVIDER_DIRECTORY_BASE}
        or "alohr.esante.us" in portal_url
        or "alohr.esante.us" in source_url
        or "developers.bcbsal.org" in portal_url
        or "developers.bcbsal.org" in source_url
    )
    if not should_override:
        return None
    return {
        "api_base": ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "requires_registration": True,
        "auth_type": "OAuth2/SMART",
        "metadata": {
            "provider_directory_override": "alohr_healthshare_providerdirectory",
            "provider_directory_override_reason": (
                "ALOHR serves open CapabilityStatement metadata at its HealthShare FHIR base, "
                "FHIR resource reads require auth, and public provider search is available through "
                "the ALOHR GraphQL directory connector."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
            "provider_directory_graphql_url": ALOHR_GRAPHQL_URL,
            "provider_directory_graphql_tenant_id": ALOHR_TENANT_ID,
        },
    }


def _source_row_from_seed(row: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    source_id = _stable_source_id(row)
    api_base = _clean_text(row.get("api_base"))
    canonical_api_base = _canonical_base(api_base)
    metadata = {
        "external_seed_id": _clean_text(row.get("id")),
        "note": _clean_text(row.get("note")),
    }
    override = _aetna_provider_directory_override(row) or _alohr_provider_directory_override(row)
    if override:
        api_base = override["api_base"]
        canonical_api_base = override["canonical_api_base"]
        metadata = {**metadata, **override["metadata"]}
    return {
        "source_id": source_id,
        "org_tin": _clean_text(row.get("org_tin")),
        "org_name": _clean_text(row.get("org_name")) or "Unknown payer",
        "plan_name": _clean_text(row.get("plan_name")),
        "portal_url": _clean_text(row.get("portal_url")),
        "api_base": api_base,
        "canonical_api_base": canonical_api_base,
        "endpoint_insurance_plan": _clean_text(row.get("endpoint_insurance_plan")),
        "endpoint_practitioner": _clean_text(row.get("endpoint_practitioner")),
        "endpoint_practitioner_role": _clean_text(row.get("endpoint_practitioner_role")),
        "endpoint_organization": _clean_text(row.get("endpoint_organization")),
        "endpoint_organization_affiliation": _clean_text(row.get("endpoint_organization_affiliation")),
        "endpoint_location": _clean_text(row.get("endpoint_location")),
        "endpoint_healthcare_service": _clean_text(row.get("endpoint_healthcare_service")),
        "endpoint_network": _clean_text(row.get("endpoint_network")),
        "endpoint_endpoint": _clean_text(row.get("endpoint_endpoint")),
        "requires_registration": (
            bool(_bool_from_seed(row.get("requires_registration")))
            or bool(override.get("requires_registration") if override else False)
        ),
        "requires_api_key": bool(_bool_from_seed(row.get("requires_api_key"))),
        "auth_type": override.get("auth_type") if override else _clean_text(row.get("auth_type")),
        "last_validated": _clean_text(row.get("last_validated")),
        "last_validated_status": _clean_text(row.get("last_validated_status")),
        "fhir_version": _clean_text(row.get("fhir_version")),
        "compliance_flag": _clean_text(row.get("compliance_flag")),
        "violation_type": _clean_text(row.get("violation_type")),
        "violation_detail": _clean_text(row.get("violation_detail")),
        "data_quality_flag": _clean_text(row.get("data_quality_flag")),
        "data_quality_sample_npi": _clean_text(row.get("data_quality_sample_npi")),
        "data_quality_practitioner_count": _clean_text(row.get("data_quality_practitioner_count")),
        "data_quality_checked": _clean_text(row.get("data_quality_checked")),
        "is_medicare_advantage": _bool_from_seed(row.get("is_medicare_advantage")),
        "is_medicaid_mco": _bool_from_seed(row.get("is_medicaid_mco")),
        "is_chip": _bool_from_seed(row.get("is_chip")),
        "is_qhp": _bool_from_seed(row.get("is_qhp")),
        "seed_source": _clean_text(row.get("source")),
        "seed_source_detail": _clean_text(row.get("source_detail")),
        "seed_source_url": _clean_text(row.get("source_url")),
        "seed_source_date": _clean_text(row.get("source_date")),
        "seed_row_id": _clean_text(row.get("id")),
        "id_provider_alt": _clean_text(row.get("id_provider_alt")),
        "team_status": _clean_text(row.get("team_status")),
        "metadata_json": metadata,
        "created_at": now,
        "updated_at": now,
    }


def _resource_id(resource: dict[str, Any]) -> str:
    raw = _clean_text(resource.get("id"))
    if raw:
        return raw[:256]
    digest = hashlib.sha256(json.dumps(resource, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return f"generated-{digest[:48]}"


def _codings(values: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    if isinstance(values, dict):
        values = [values]
    if not isinstance(values, list):
        return result
    for value in values:
        if not isinstance(value, dict):
            continue
        for coding in value.get("coding") or []:
            if not isinstance(coding, dict):
                continue
            item = {
                "system": _clean_text(coding.get("system")),
                "code": _clean_text(coding.get("code")),
                "display": _clean_text(coding.get("display")),
            }
            if any(item.values()):
                result.append(item)
        text = _clean_text(value.get("text"))
        if text and not result:
            result.append({"text": text})
    return result


def _references(values: Any) -> list[str]:
    if isinstance(values, dict):
        values = [values]
    refs: list[str] = []
    if not isinstance(values, list):
        return refs
    for value in values:
        if not isinstance(value, dict):
            continue
        ref = _clean_text(value.get("reference"))
        if ref:
            refs.append(ref)
    return refs


def _first_reference(value: Any) -> str | None:
    refs = _references(value)
    return refs[0] if refs else None


def _identifier_value(resource: dict[str, Any], *tokens: str) -> str | None:
    lowered = tuple(token.lower() for token in tokens)
    for identifier in resource.get("identifier") or []:
        if not isinstance(identifier, dict):
            continue
        system = str(identifier.get("system") or "").lower()
        value = _clean_text(identifier.get("value"))
        if not value:
            continue
        if any(token in system for token in lowered):
            return value
    return None


def _npi(resource: dict[str, Any]) -> int | None:
    value = _identifier_value(resource, "us-npi", "npi")
    if not value:
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) != 10:
        return None
    return int(digits)


def _tin(resource: dict[str, Any]) -> str | None:
    value = _identifier_value(resource, "tax", "tin", "ein")
    if not value:
        return None
    return value[:64]


def _telecom(resource: dict[str, Any]) -> list[dict[str, Any]]:
    telecom = resource.get("telecom") or []
    return [item for item in telecom if isinstance(item, dict)]


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := _clean_text(item))]


def _phone(telecom: list[dict[str, Any]], *, system: str = "phone") -> str | None:
    for item in telecom:
        if _clean_text(item.get("system")) == system:
            return _clean_text(item.get("value"))
    return None


def _fax(telecom: list[dict[str, Any]]) -> str | None:
    return _phone(telecom, system="fax")


def _location_contact_fields(
    telephone_number: str | None,
    fax_number: str | None,
    country_code: str | None,
) -> dict[str, str | None]:
    canonical = canonicalize_contact_one((telephone_number, fax_number, country_code))
    phone_number = canonical.get("phone_number")
    fax_number_digits = canonical.get("fax_number")
    phone_extension = canonical.get("phone_extension")
    fax_extension = canonical.get("fax_extension")
    return {
        "phone_number": phone_number if isinstance(phone_number, str) else None,
        "fax_number_digits": fax_number_digits if isinstance(fax_number_digits, str) else None,
        "phone_extension": phone_extension if isinstance(phone_extension, str) else None,
        "fax_extension": fax_extension if isinstance(fax_extension, str) else None,
    }


def _contact_main_expr(expr: str) -> str:
    extension_pattern = (
        "'[[:space:]]*(extension|ext\\.?|;ext=|#|x)"
        "[[:space:]]*[0-9]{1,16}[[:space:]]*$'"
    )
    return f"regexp_replace(COALESCE({expr}, ''), {extension_pattern}, '', 'i')"


def _contact_digits_expr(expr: str) -> str:
    return f"regexp_replace({_contact_main_expr(expr)}, '[^0-9]', '', 'g')"


def _contact_country_key_expr(expr: str) -> str:
    return f"regexp_replace(upper(COALESCE({expr}, '')), '[^A-Z]', '', 'g')"


def _canonical_contact_number_expr(expr: str, country_expr: str = "country_code") -> str:
    digits = _contact_digits_expr(expr)
    country_key = _contact_country_key_expr(country_expr)
    default_us = (
        f"({country_key} = '' OR {country_key} IN "
        "('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA'))"
    )
    main = _contact_main_expr(expr)
    return (
        "CASE "
        f"WHEN {default_us} AND length({digits}) = 10 THEN {digits} "
        f"WHEN {default_us} AND length({digits}) = 11 AND left({digits}, 1) = '1' "
        f"THEN substring({digits} from 2) "
        f"WHEN BTRIM({main}) LIKE '+%' AND length({digits}) BETWEEN 8 AND 15 THEN {digits} "
        "ELSE NULL::varchar END"
    )


def _contact_extension_expr(expr: str) -> str:
    extension_pattern = (
        "'(extension|ext\\.?|;ext=|#|x)[[:space:]]*[0-9]{1,16}[[:space:]]*$'"
    )
    extract_pattern = (
        "'^.*(extension|ext\\.?|;ext=|#|x)[[:space:]]*([0-9]{1,16})[[:space:]]*$'"
    )
    return (
        "CASE "
        f"WHEN COALESCE({expr}, '') ~* {extension_pattern} "
        f"THEN NULLIF(regexp_replace(COALESCE({expr}, ''), {extract_pattern}, '\\2', 'i'), '')::varchar "
        "ELSE NULL::varchar END"
    )


def _name(resource: dict[str, Any]) -> tuple[str | None, list[str], str | None]:
    names = resource.get("name")
    if isinstance(names, dict):
        names = [names]
    if not isinstance(names, list) or not names:
        return None, [], _clean_text(resource.get("name"))
    first = next((item for item in names if isinstance(item, dict)), None)
    if not first:
        return None, [], None
    family = _clean_text(first.get("family"))
    given = [str(value) for value in first.get("given") or [] if _clean_text(value)]
    text = _clean_text(first.get("text"))
    if not text:
        parts = given + ([family] if family else [])
        text = " ".join(parts) if parts else None
    return family, given, text


def _period(resource: dict[str, Any]) -> tuple[str | None, str | None]:
    period = resource.get("period")
    if not isinstance(period, dict):
        return None, None
    return _clean_text(period.get("start")), _clean_text(period.get("end"))


def _address(resource: dict[str, Any]) -> dict[str, Any]:
    addresses = resource.get("address")
    if isinstance(addresses, dict):
        addresses = [addresses]
    first = next((item for item in addresses or [] if isinstance(item, dict)), {})
    lines = [str(value).strip() for value in first.get("line") or [] if _clean_text(value)]
    postal_code = _clean_text(first.get("postalCode"))
    zip5 = None
    if postal_code:
        match = ZIP5_RE.search(postal_code)
        zip5 = match.group(1) if match else None
    city = _clean_text(first.get("city"))
    raw_state = _clean_text(first.get("state"))
    state = _normalize_state_name(raw_state)
    state_code = _normalize_state_code(raw_state)
    position = resource.get("position") if isinstance(resource.get("position"), dict) else {}
    return {
        "first_line": lines[0] if lines else None,
        "second_line": lines[1] if len(lines) > 1 else None,
        "city_name": city,
        "state_name": state,
        "state_code": state_code,
        "postal_code": postal_code,
        "zip5": zip5,
        "city_norm": city.upper() if city else None,
        "country_code": _normalize_country_code(first.get("country")),
        "latitude": _clean_text(position.get("latitude")),
        "longitude": _clean_text(position.get("longitude")),
        "address_json": first if first else None,
    }


def parse_capability(source: dict[str, Any], payload: dict[str, Any], probe: dict[str, Any]) -> dict[str, Any]:
    rest_resources: list[str] = []
    search_params: dict[str, list[str]] = {}
    for rest in payload.get("rest") or []:
        if not isinstance(rest, dict):
            continue
        for resource in rest.get("resource") or []:
            if not isinstance(resource, dict):
                continue
            resource_type = _clean_text(resource.get("type"))
            if not resource_type:
                continue
            rest_resources.append(resource_type)
            names = [
                str(item.get("name"))
                for item in resource.get("searchParam") or []
                if isinstance(item, dict) and _clean_text(item.get("name"))
            ]
            search_params[resource_type] = names
    software = payload.get("software") if isinstance(payload.get("software"), dict) else {}
    implementation = payload.get("implementation") if isinstance(payload.get("implementation"), dict) else {}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()
    return {
        "source_id": source["source_id"],
        "api_base": source.get("api_base"),
        "metadata_url": probe.get("url"),
        "probe_status": probe["status"],
        "http_status": probe.get("http_status"),
        "response_time_ms": probe.get("response_time_ms"),
        "resource_type": _clean_text(payload.get("resourceType")),
        "fhir_version": _clean_text(payload.get("fhirVersion")),
        "software_name": _clean_text(software.get("name")),
        "software_version": _clean_text(software.get("version")),
        "implementation_url": _clean_text(implementation.get("url")),
        "formats": payload.get("format") or [],
        "supported_resources": sorted(set(rest_resources)),
        "search_params": search_params,
        "auth_required": probe["status"] == "auth_required",
        "error": probe.get("error"),
        "capability_hash": digest,
        "probed_at": _now(),
        "run_id": probe.get("run_id"),
        "metadata_json": {
            "publisher": _clean_text(payload.get("publisher")),
            "date": _clean_text(payload.get("date")),
            "kind": _clean_text(payload.get("kind")),
        },
    }


def parse_fhir_resource(
    source_id: str,
    resource: dict[str, Any],
    *,
    resource_url: str | None = None,
    run_id: str | None = None,
) -> tuple[type, dict[str, Any]] | None:
    resource_type = resource.get("resourceType")
    observed_at = _now()
    base = {
        "source_id": source_id,
        "resource_id": _resource_id(resource),
        "resource_url": resource_url,
        "last_seen_run_id": run_id,
        "observed_at": observed_at,
        "updated_at": observed_at,
    }
    if resource_type == "InsurancePlan":
        period_start, period_end = _period(resource)
        plan = next((item for item in resource.get("plan") or [] if isinstance(item, dict)), {})
        identifier = _identifier_value(plan, "planid", "plan-id", "hios") or _identifier_value(resource, "plan")
        row = {
            **base,
            "plan_identifier": identifier,
            "status": _clean_text(resource.get("status")),
            "name": _clean_text(resource.get("name")),
            "aliases": resource.get("alias") or [],
            "type_codes": _codings(resource.get("type")),
            "owned_by_ref": _first_reference(resource.get("ownedBy")),
            "administered_by_ref": _first_reference(resource.get("administeredBy")),
            "network_refs": _references(resource.get("network")),
            "coverage_area_refs": _references(resource.get("coverageArea")),
            "plan_json": plan or None,
            "period_start": period_start,
            "period_end": period_end,
        }
        return ProviderDirectoryInsurancePlan, row
    if resource_type == "Practitioner":
        family, given, full_name = _name(resource)
        row = {
            **base,
            "npi": _npi(resource),
            "active": resource.get("active") if isinstance(resource.get("active"), bool) else None,
            "family_name": family,
            "given_names": given,
            "full_name": full_name,
            "telecom": _telecom(resource),
            "qualification_codes": _codings([item.get("code") for item in resource.get("qualification") or [] if isinstance(item, dict)]),
            "communication_codes": _codings(resource.get("communication")),
        }
        return ProviderDirectoryPractitioner, row
    if resource_type == "Organization":
        row = {
            **base,
            "npi": _npi(resource),
            "tax_id": _tin(resource),
            "active": resource.get("active") if isinstance(resource.get("active"), bool) else None,
            "name": _clean_text(resource.get("name")),
            "aliases": resource.get("alias") or [],
            "type_codes": _codings(resource.get("type")),
            "telecom": _telecom(resource),
            "address_json": resource.get("address") or [],
            "endpoint_refs": _references(resource.get("endpoint")),
        }
        return ProviderDirectoryOrganization, row
    if resource_type == "Location":
        telecom = _telecom(resource)
        address = _address(resource)
        telephone_number = _phone(telecom)
        fax_number = _fax(telecom)
        row = {
            **base,
            "status": _clean_text(resource.get("status")),
            "name": _clean_text(resource.get("name")),
            "mode": _clean_text(resource.get("mode")),
            "type_codes": _codings(resource.get("type")),
            "telephone_number": telephone_number,
            "fax_number": fax_number,
            **_location_contact_fields(telephone_number, fax_number, address.get("country_code")),
            "telecom": telecom,
            **{key: value for key, value in address.items() if key != "address_json"},
        }
        return ProviderDirectoryLocation, row
    if resource_type == "PractitionerRole":
        period_start, period_end = _period(resource)
        row = {
            **base,
            "active": resource.get("active") if isinstance(resource.get("active"), bool) else None,
            "practitioner_ref": _first_reference(resource.get("practitioner")),
            "organization_ref": _first_reference(resource.get("organization")),
            "location_refs": _references(resource.get("location")),
            "healthcare_service_refs": _references(resource.get("healthcareService")),
            "network_refs": _references(resource.get("network")),
            "insurance_plan_refs": _references(resource.get("insurancePlan")),
            "endpoint_refs": _references(resource.get("endpoint")),
            "specialty_codes": _codings(resource.get("specialty")),
            "code_codes": _codings(resource.get("code")),
            "telecom": _telecom(resource),
            "accepting_patients": resource.get("availableTime") or resource.get("extension") or [],
            "period_start": period_start,
            "period_end": period_end,
        }
        return ProviderDirectoryPractitionerRole, row
    if resource_type == "HealthcareService":
        row = {
            **base,
            "active": resource.get("active") if isinstance(resource.get("active"), bool) else None,
            "name": _clean_text(resource.get("name")),
            "type_codes": _codings(resource.get("type")),
            "category_codes": _codings(resource.get("category")),
            "specialty_codes": _codings(resource.get("specialty")),
            "location_refs": _references(resource.get("location")),
            "endpoint_refs": _references(resource.get("endpoint")),
            "telecom": _telecom(resource),
            "coverage_area_refs": _references(resource.get("coverageArea")),
        }
        return ProviderDirectoryHealthcareService, row
    if resource_type == "OrganizationAffiliation":
        period_start, period_end = _period(resource)
        row = {
            **base,
            "active": resource.get("active") if isinstance(resource.get("active"), bool) else None,
            "organization_ref": _first_reference(resource.get("organization")),
            "participating_organization_ref": _first_reference(resource.get("participatingOrganization")),
            "network_refs": _references(resource.get("network")),
            "location_refs": _references(resource.get("location")),
            "healthcare_service_refs": _references(resource.get("healthcareService")),
            "endpoint_refs": _references(resource.get("endpoint")),
            "specialty_codes": _codings(resource.get("specialty")),
            "code_codes": _codings(resource.get("code")),
            "period_start": period_start,
            "period_end": period_end,
        }
        return ProviderDirectoryOrganizationAffiliation, row
    if resource_type == "Endpoint":
        period_start, period_end = _period(resource)
        connection_type = resource.get("connectionType") if isinstance(resource.get("connectionType"), dict) else {}
        contact = resource.get("contact") or []
        row = {
            **base,
            "status": _clean_text(resource.get("status")),
            "connection_type_system": _clean_text(connection_type.get("system")),
            "connection_type_code": _clean_text(connection_type.get("code")),
            "connection_type_display": _clean_text(connection_type.get("display")),
            "name": _clean_text(resource.get("name")),
            "managing_organization_ref": _first_reference(resource.get("managingOrganization")),
            "contact": [item for item in contact if isinstance(item, dict)] if isinstance(contact, list) else [],
            "period_start": period_start,
            "period_end": period_end,
            "payload_type_codes": _codings(resource.get("payloadType")),
            "payload_mime_types": _string_list(resource.get("payloadMimeType")),
            "address": _clean_text(resource.get("address")),
            "header": _string_list(resource.get("header")),
        }
        return ProviderDirectoryEndpoint, row
    return None


def _alohr_source_uses_graphql_connector(source: dict[str, Any]) -> bool:
    api_base = _canonical_base(source.get("api_base") or source.get("canonical_api_base"))
    if api_base in {ALOHR_PUBLIC_PROVIDER_DIRECTORY_BASE, ALOHR_FHIR_PROVIDER_DIRECTORY_BASE}:
        return True
    metadata = source.get("metadata_json") if isinstance(source.get("metadata_json"), dict) else {}
    return metadata.get("provider_directory_graphql_tenant_id") == ALOHR_TENANT_ID


def _alohr_resource_id(prefix: str, *parts: Any) -> str:
    cleaned = [_clean_text(part) or "" for part in parts]
    raw = "|".join(cleaned)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    candidate = _clean_text(parts[0]) if parts else None
    if candidate and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", candidate):
        return candidate if prefix == "" else f"{prefix}-{candidate}"
    return f"{prefix}-{digest}" if prefix else digest


def _alohr_npi(value: Any) -> int | None:
    text = _clean_text(value)
    if not text or not text.isdigit():
        return None
    try:
        return int(text)
    except Exception:
        return None


def _alohr_telecom(*items: Any) -> list[dict[str, Any]]:
    telecom: list[dict[str, Any]] = []
    for item in items:
        if not item:
            continue
        if isinstance(item, list):
            for nested in item:
                if isinstance(nested, dict):
                    system = _clean_text(nested.get("system"))
                    value = _clean_text(nested.get("value"))
                    if system and value:
                        telecom.append({"system": system, "value": value, "use": _clean_text(nested.get("use"))})
            continue
        if isinstance(item, dict):
            for nested in item.get("contacts") or []:
                if isinstance(nested, dict):
                    system = _clean_text(nested.get("system"))
                    value = _clean_text(nested.get("value"))
                    if system and value:
                        telecom.append({"system": system, "value": value, "use": _clean_text(nested.get("use"))})
            continue
        value = _clean_text(item)
        if value:
            system = "email" if "@" in value else "phone"
            telecom.append({"system": system, "value": value})
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str | None, str | None, str | None]] = set()
    for item in telecom:
        key = (item.get("system"), item.get("value"), item.get("use"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append({key: value for key, value in item.items() if value is not None})
    return deduped


def _alohr_specialty_codings(code: Any, display: Any) -> list[dict[str, Any]]:
    clean_code = _clean_text(code)
    clean_display = _clean_text(display)
    if not clean_code and not clean_display:
        return []
    coding: dict[str, Any] = {}
    if clean_code:
        coding.update({"system": "http://nucc.org/provider-taxonomy", "code": clean_code})
    if clean_display:
        coding["display"] = clean_display
    return [coding]


def _alohr_address_items(item: dict[str, Any]) -> list[dict[str, Any]]:
    raw_addresses = item.get("addresses") if isinstance(item.get("addresses"), list) else []
    if not raw_addresses and isinstance(item.get("address"), dict):
        raw_addresses = [item["address"]]
    addresses: list[dict[str, Any]] = []
    seen: set[tuple[str | None, ...]] = set()
    for raw in raw_addresses:
        if not isinstance(raw, dict):
            continue
        street1 = _clean_text(raw.get("street1"))
        street2 = _clean_text(raw.get("street2"))
        city = _clean_text(raw.get("city"))
        state = _clean_text(raw.get("state"))
        zip_code = _clean_text(raw.get("zip"))
        if not any((street1, city, state, zip_code)):
            continue
        key = (street1, street2, city, state, zip_code)
        if key in seen:
            continue
        seen.add(key)
        address: dict[str, Any] = {
            "use": _clean_text(raw.get("type")) or "work",
            "line": [value for value in (street1, street2) if value],
            "city": city,
            "state": state,
            "postalCode": zip_code,
            "country": "US",
        }
        if _clean_text(raw.get("county")):
            address["district"] = _clean_text(raw.get("county"))
        addresses.append(address)
    return addresses


def _alohr_location_resource(owner_id: str, owner_name: str | None, address: dict[str, Any], telecom: list[dict[str, Any]]) -> dict[str, Any]:
    address_key = json.dumps(address, sort_keys=True)
    resource_id = _alohr_resource_id("loc", owner_id, address_key)
    return {
        "resourceType": "Location",
        "id": resource_id,
        "status": "active",
        "name": owner_name,
        "address": address,
        "telecom": telecom,
    }


def _append_alohr_parsed_resource(
    rows_by_model: dict[type, list[dict[str, Any]]],
    source_id: str,
    resource: dict[str, Any],
    *,
    run_id: str | None,
) -> None:
    parsed = parse_fhir_resource(
        source_id,
        resource,
        resource_url=f"{ALOHR_GRAPHQL_URL}#{resource.get('resourceType')}/{resource.get('id')}",
        run_id=run_id,
    )
    if parsed:
        model, row = parsed
        rows_by_model.setdefault(model, []).append(row)


def _append_alohr_provider_rows(
    rows_by_model: dict[type, list[dict[str, Any]]],
    source_id: str,
    provider: dict[str, Any],
    *,
    run_id: str | None,
) -> None:
    provider_id = _clean_text(provider.get("providerId")) or _alohr_resource_id("prac", provider.get("npi"), provider.get("firstName"), provider.get("lastName"))
    full_name = " ".join(value for value in (_clean_text(provider.get("firstName")), _clean_text(provider.get("lastName"))) if value)
    telecom = _alohr_telecom(provider.get("phone"), provider.get("email"))
    specialty = _alohr_specialty_codings(provider.get("specialty"), provider.get("specialtyDescription"))
    identifiers = []
    if _clean_text(provider.get("npi")):
        identifiers.append({"system": "http://hl7.org/fhir/sid/us-npi", "value": _clean_text(provider.get("npi"))})
    practitioner = {
        "resourceType": "Practitioner",
        "id": provider_id,
        "active": True,
        "identifier": identifiers,
        "name": [
            {
                "family": _clean_text(provider.get("lastName")),
                "given": [_clean_text(provider.get("firstName"))] if _clean_text(provider.get("firstName")) else [],
                "text": full_name or None,
            }
        ],
        "telecom": telecom,
        "qualification": [{"code": {"coding": specialty}}] if specialty else [],
        "communication": [{"coding": [{"display": language}]} for language in provider.get("languages") or [] if _clean_text(language)],
    }
    _append_alohr_parsed_resource(rows_by_model, source_id, practitioner, run_id=run_id)
    location_refs: list[dict[str, str]] = []
    for address in _alohr_address_items(provider):
        location = _alohr_location_resource(provider_id, full_name or provider_id, address, telecom)
        _append_alohr_parsed_resource(rows_by_model, source_id, location, run_id=run_id)
        location_refs.append({"reference": f"Location/{location['id']}"})
    if location_refs:
        role = {
            "resourceType": "PractitionerRole",
            "id": _alohr_resource_id("role", provider_id, ",".join(ref["reference"] for ref in location_refs)),
            "active": True,
            "practitioner": {"reference": f"Practitioner/{provider_id}"},
            "location": location_refs,
            "specialty": [{"coding": specialty}] if specialty else [],
            "telecom": telecom,
            "extension": [
                {"url": "https://healthporta.com/fhir/provider-directory/telehealth", "valueBoolean": bool(provider.get("telehealth"))},
                {"url": "https://healthporta.com/fhir/provider-directory/accepting-medicaid", "valueBoolean": bool(provider.get("acceptingMedicaid"))},
            ],
        }
        _append_alohr_parsed_resource(rows_by_model, source_id, role, run_id=run_id)


def _append_alohr_organization_rows(
    rows_by_model: dict[type, list[dict[str, Any]]],
    source_id: str,
    organization: dict[str, Any],
    *,
    run_id: str | None,
) -> None:
    org_id = _clean_text(organization.get("orgId")) or _alohr_resource_id("org", organization.get("npi"), organization.get("name"))
    contacts = []
    for item in organization.get("contacts") or []:
        if isinstance(item, dict):
            contacts.extend(item.get("contacts") or [])
    contacts.extend(organization.get("organizationContact") or [])
    telecom = _alohr_telecom(contacts)
    specialty = _alohr_specialty_codings(organization.get("specialty"), organization.get("specialtyDescription"))
    identifiers = []
    if _clean_text(organization.get("npi")):
        identifiers.append({"system": "http://hl7.org/fhir/sid/us-npi", "value": _clean_text(organization.get("npi"))})
    org_resource = {
        "resourceType": "Organization",
        "id": org_id,
        "active": True,
        "identifier": identifiers,
        "name": _clean_text(organization.get("name")),
        "type": [{"coding": specialty}] if specialty else [],
        "telecom": telecom,
        "address": _alohr_address_items(organization),
    }
    _append_alohr_parsed_resource(rows_by_model, source_id, org_resource, run_id=run_id)
    location_refs: list[dict[str, str]] = []
    for address in _alohr_address_items(organization):
        location = _alohr_location_resource(org_id, _clean_text(organization.get("name")) or org_id, address, telecom)
        _append_alohr_parsed_resource(rows_by_model, source_id, location, run_id=run_id)
        location_refs.append({"reference": f"Location/{location['id']}"})
    if location_refs:
        affiliation = {
            "resourceType": "OrganizationAffiliation",
            "id": _alohr_resource_id("affiliation", org_id, ",".join(ref["reference"] for ref in location_refs)),
            "active": True,
            "organization": {"reference": f"Organization/{org_id}"},
            "participatingOrganization": {"reference": f"Organization/{org_id}"},
            "location": location_refs,
            "specialty": [{"coding": specialty}] if specialty else [],
        }
        _append_alohr_parsed_resource(rows_by_model, source_id, affiliation, run_id=run_id)


def provider_directory_ptg_address_corroboration_sql(
    db_schema: str | None = None,
    *,
    view_name: str = PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
) -> str:
    """Build a view that corroborates PTG addresses with payer FHIR directory data.

    A match means a payer Provider Directory source links the PTG NPI to a FHIR
    role/affiliation location whose canonical address_key equals the PTG
    address_key. This is stronger than NPPES-only inference, but remains
    distinct from a TiC rate file directly supplying the service location.
    """

    schema = db_schema or _schema()
    view_ref = _qt(schema, view_name)
    practitioner_ref_match = _sql_ref_matches_resource("role.practitioner_ref", "Practitioner", "practitioner.resource_id")
    role_location_ref_match = _sql_ref_matches_resource("role.location_ref", "Location", "loc.resource_id")
    role_plan_ref_match = _sql_ref_matches_resource("plan_ref.value", "InsurancePlan", "insurance_plan.resource_id")
    role_network_ref_match = _sql_ref_matches_resource("network_ref.value", "Organization", "network_org.resource_id")
    affiliation_org_ref_match = _sql_ref_matches_resource(
        "affiliation.organization_ref", "Organization", "organization.resource_id"
    )
    affiliation_participating_org_ref_match = _sql_ref_matches_resource(
        "affiliation.participating_organization_ref", "Organization", "organization.resource_id"
    )
    affiliation_location_ref_match = _sql_ref_matches_resource("affiliation.location_ref", "Location", "loc.resource_id")
    affiliation_plan_ref_match = _sql_ref_matches_resource(
        "plan_ref.value", "InsurancePlan", "insurance_plan.resource_id"
    )
    affiliation_network_ref_match = _sql_ref_matches_resource(
        "network_ref.value", "Organization", "network_org.resource_id"
    )
    return f"""
    CREATE OR REPLACE VIEW {view_ref} AS
    WITH practitioner_role_locations AS (
        SELECT
            role.source_id,
            role.resource_id AS role_resource_id,
            role.practitioner_ref,
            role.organization_ref,
            location_ref.value AS location_ref,
            COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS network_refs,
            COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb) AS insurance_plan_refs,
            COALESCE(role.specialty_codes::jsonb, '[]'::jsonb) AS specialty_codes,
            COALESCE(role.code_codes::jsonb, '[]'::jsonb) AS code_codes,
            role.active AS role_active,
            role.observed_at AS role_observed_at
          FROM {_qt(schema, "provider_directory_practitioner_role")} role
          CROSS JOIN LATERAL jsonb_array_elements_text(
              COALESCE(role.location_refs::jsonb, '[]'::jsonb)
          ) AS location_ref(value)
    ),
    practitioner_matches AS (
        SELECT
            p.source_key,
            p.snapshot_id,
            p.plan_id,
            p.ptg_plan_id,
            p.npi,
            p.location_key,
            p.address_key,
            p.zip5,
            p.state_code,
            p.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            practitioner.resource_id AS provider_directory_provider_resource_id,
            practitioner.full_name AS provider_directory_provider_name,
            role.role_resource_id AS provider_directory_role_resource_id,
            loc.resource_id AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            loc.telephone_number AS provider_directory_telephone_number,
            loc.phone_number AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            loc.fax_number AS provider_directory_fax_number,
            loc.fax_number_digits AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            role.network_refs AS provider_directory_network_refs,
            role.insurance_plan_refs AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            COALESCE(plan_context.provider_directory_plan_context_matched, false)
                AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            COALESCE(plan_context.provider_directory_insurance_plan_matches, '[]'::jsonb)
                AS provider_directory_insurance_plan_matches,
            role.specialty_codes AS provider_directory_specialty_codes,
            role.code_codes AS provider_directory_role_codes,
            'practitioner_role'::varchar AS provider_directory_match_type,
            (role.role_active IS DISTINCT FROM false) AS provider_directory_active_role,
            (practitioner.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(role.role_observed_at, TIMESTAMP 'epoch'),
                COALESCE(practitioner.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM {_qt(schema, "ptg_address")} p
          JOIN {_qt(schema, "provider_directory_practitioner")} practitioner
            ON practitioner.npi = p.npi
          JOIN practitioner_role_locations role
            ON role.source_id = practitioner.source_id
           AND {practitioner_ref_match}
          JOIN {_qt(schema, "provider_directory_location")} loc
            ON loc.source_id = role.source_id
           AND {role_location_ref_match}
           AND (
                CASE
                    WHEN loc.address_key ~* '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                        THEN loc.address_key::uuid
                    ELSE NULL::uuid
                END
           ) = p.address_key
          LEFT JOIN LATERAL (
            SELECT
                COALESCE(
                    bool_or(
                        insurance_plan.resource_id IS NOT NULL
                        AND NULLIF(BTRIM(insurance_plan.plan_identifier), '') IS NOT NULL
                        AND (
                            NULLIF(BTRIM(insurance_plan.plan_identifier), '') IN (p.plan_id, p.ptg_plan_id)
                         OR NULLIF(BTRIM(insurance_plan.plan_identifier), '') = ANY(
                                COALESCE(p.ptg_plan_array, ARRAY[]::varchar[])
                            )
                         OR NULLIF(BTRIM(insurance_plan.plan_identifier), '') = ANY(
                                COALESCE(p.group_plan_array, ARRAY[]::varchar[])
                            )
                        )
                    ),
                    false
                ) AS provider_directory_plan_context_matched,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', plan_ref.value,
                            'resource_id', insurance_plan.resource_id,
                            'plan_identifier', insurance_plan.plan_identifier,
                            'name', insurance_plan.name
                        )
                    ) FILTER (WHERE plan_ref.value IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_insurance_plan_matches
              FROM jsonb_array_elements_text(
                  COALESCE(role.insurance_plan_refs, '[]'::jsonb)
              ) AS plan_ref(value)
              LEFT JOIN {_qt(schema, "provider_directory_insurance_plan")} insurance_plan
                ON insurance_plan.source_id = role.source_id
               AND {role_plan_ref_match}
          ) plan_context ON TRUE
          LEFT JOIN LATERAL (
            WITH network_ref_values AS (
                SELECT network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.network_refs, '[]'::jsonb)
                  ) AS network_ref(value)
                UNION
                SELECT plan_network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.insurance_plan_refs, '[]'::jsonb)
                  ) AS plan_ref(value)
                  JOIN {_qt(schema, "provider_directory_insurance_plan")} insurance_plan
                    ON insurance_plan.source_id = role.source_id
                   AND {role_plan_ref_match}
                  CROSS JOIN LATERAL jsonb_array_elements_text(
                      COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
                  ) AS plan_network_ref(value)
            )
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_org.name)
                        FILTER (WHERE NULLIF(BTRIM(network_org.name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_org.resource_id,
                            'name', network_org.name,
                            'aliases', COALESCE(network_org.aliases::jsonb, '[]'::jsonb)
                        )
                    ) FILTER (WHERE network_org.resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM network_ref_values network_ref
              LEFT JOIN {_qt(schema, "provider_directory_organization")} network_org
                ON network_org.source_id = role.source_id
               AND {role_network_ref_match}
          ) network_context ON TRUE
          JOIN {_qt(schema, "provider_directory_source")} src
            ON src.source_id = role.source_id
         WHERE p.npi IS NOT NULL
           AND p.address_key IS NOT NULL
    ),
    organization_affiliation_locations AS (
        SELECT
            affiliation.source_id,
            affiliation.resource_id AS role_resource_id,
            affiliation.organization_ref,
            affiliation.participating_organization_ref,
            location_ref.value AS location_ref,
            COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS network_refs,
            '[]'::jsonb AS insurance_plan_refs,
            COALESCE(affiliation.specialty_codes::jsonb, '[]'::jsonb) AS specialty_codes,
            COALESCE(affiliation.code_codes::jsonb, '[]'::jsonb) AS code_codes,
            affiliation.active AS role_active,
            affiliation.observed_at AS role_observed_at
          FROM {_qt(schema, "provider_directory_organization_affiliation")} affiliation
          CROSS JOIN LATERAL jsonb_array_elements_text(
              COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)
          ) AS location_ref(value)
    ),
    organization_matches AS (
        SELECT
            p.source_key,
            p.snapshot_id,
            p.plan_id,
            p.ptg_plan_id,
            p.npi,
            p.location_key,
            p.address_key,
            p.zip5,
            p.state_code,
            p.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            organization.resource_id AS provider_directory_provider_resource_id,
            organization.name AS provider_directory_provider_name,
            affiliation.role_resource_id AS provider_directory_role_resource_id,
            loc.resource_id AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            loc.telephone_number AS provider_directory_telephone_number,
            loc.phone_number AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            loc.fax_number AS provider_directory_fax_number,
            loc.fax_number_digits AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            affiliation.network_refs AS provider_directory_network_refs,
            affiliation.insurance_plan_refs AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            COALESCE(plan_context.provider_directory_plan_context_matched, false)
                AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            COALESCE(plan_context.provider_directory_insurance_plan_matches, '[]'::jsonb)
                AS provider_directory_insurance_plan_matches,
            affiliation.specialty_codes AS provider_directory_specialty_codes,
            affiliation.code_codes AS provider_directory_role_codes,
            'organization_affiliation'::varchar AS provider_directory_match_type,
            (affiliation.role_active IS DISTINCT FROM false) AS provider_directory_active_role,
            (organization.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(affiliation.role_observed_at, TIMESTAMP 'epoch'),
                COALESCE(organization.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM {_qt(schema, "ptg_address")} p
          JOIN {_qt(schema, "provider_directory_organization")} organization
            ON organization.npi = p.npi
          JOIN organization_affiliation_locations affiliation
            ON affiliation.source_id = organization.source_id
           AND (
                {affiliation_org_ref_match}
             OR {affiliation_participating_org_ref_match}
           )
          JOIN {_qt(schema, "provider_directory_location")} loc
            ON loc.source_id = affiliation.source_id
           AND {affiliation_location_ref_match}
           AND (
                CASE
                    WHEN loc.address_key ~* '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                        THEN loc.address_key::uuid
                    ELSE NULL::uuid
                END
           ) = p.address_key
          LEFT JOIN LATERAL (
            SELECT
                COALESCE(
                    bool_or(
                        insurance_plan.resource_id IS NOT NULL
                        AND NULLIF(BTRIM(insurance_plan.plan_identifier), '') IS NOT NULL
                        AND (
                            NULLIF(BTRIM(insurance_plan.plan_identifier), '') IN (p.plan_id, p.ptg_plan_id)
                         OR NULLIF(BTRIM(insurance_plan.plan_identifier), '') = ANY(
                                COALESCE(p.ptg_plan_array, ARRAY[]::varchar[])
                            )
                         OR NULLIF(BTRIM(insurance_plan.plan_identifier), '') = ANY(
                                COALESCE(p.group_plan_array, ARRAY[]::varchar[])
                            )
                        )
                    ),
                    false
                ) AS provider_directory_plan_context_matched,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', plan_ref.value,
                            'resource_id', insurance_plan.resource_id,
                            'plan_identifier', insurance_plan.plan_identifier,
                            'name', insurance_plan.name
                        )
                    ) FILTER (WHERE plan_ref.value IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_insurance_plan_matches
              FROM jsonb_array_elements_text(
                  COALESCE(affiliation.insurance_plan_refs, '[]'::jsonb)
              ) AS plan_ref(value)
              LEFT JOIN {_qt(schema, "provider_directory_insurance_plan")} insurance_plan
                ON insurance_plan.source_id = affiliation.source_id
               AND {affiliation_plan_ref_match}
          ) plan_context ON TRUE
          LEFT JOIN LATERAL (
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_org.name)
                        FILTER (WHERE NULLIF(BTRIM(network_org.name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_org.resource_id,
                            'name', network_org.name,
                            'aliases', COALESCE(network_org.aliases::jsonb, '[]'::jsonb)
                        )
                    ) FILTER (WHERE network_org.resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM jsonb_array_elements_text(
                  COALESCE(affiliation.network_refs, '[]'::jsonb)
              ) AS network_ref(value)
              LEFT JOIN {_qt(schema, "provider_directory_organization")} network_org
                ON network_org.source_id = affiliation.source_id
               AND {affiliation_network_ref_match}
          ) network_context ON TRUE
          JOIN {_qt(schema, "provider_directory_source")} src
            ON src.source_id = affiliation.source_id
         WHERE p.npi IS NOT NULL
           AND p.address_key IS NOT NULL
    ),
    matches AS (
        SELECT * FROM practitioner_matches
        UNION ALL
        SELECT * FROM organization_matches
    )
    SELECT DISTINCT
        source_key,
        snapshot_id,
        plan_id,
        ptg_plan_id,
        npi,
        location_key,
        address_key,
        zip5,
        state_code,
        city_norm,
        provider_directory_source_id,
        provider_directory_org_name,
        provider_directory_plan_name,
        provider_directory_provider_resource_id,
        provider_directory_provider_name,
        provider_directory_role_resource_id,
        provider_directory_location_resource_id,
        provider_directory_location_name,
        provider_directory_telephone_number,
        provider_directory_phone_number,
        provider_directory_phone_extension,
        provider_directory_fax_number,
        provider_directory_fax_number_digits,
        provider_directory_fax_extension,
        provider_directory_network_refs,
        provider_directory_insurance_plan_refs,
        provider_directory_specialty_codes,
        provider_directory_role_codes,
        provider_directory_match_type,
        provider_directory_active_role,
        provider_directory_active_provider,
        provider_directory_active_location,
        provider_directory_observed_at,
        (
            provider_directory_active_role
            AND provider_directory_active_provider
            AND provider_directory_active_location
        ) AS provider_directory_active_match,
        CASE
            WHEN provider_directory_active_role
             AND provider_directory_active_provider
             AND provider_directory_active_location
             AND provider_directory_plan_context_matched
                THEN 'payer_directory_corroborated_location'
            WHEN provider_directory_active_role
             AND provider_directory_active_provider
             AND provider_directory_active_location
                THEN 'provider_directory_address'
            ELSE 'payer_directory_corroborated_location_inactive_or_unknown'
        END::varchar AS address_network_binding,
        jsonb_build_object(
            'source', 'provider_directory_fhir',
            'matched_on',
                CASE
                    WHEN provider_directory_plan_context_matched
                        THEN 'npi_address_key_role_location_plan'
                    ELSE 'npi_address_key_role_location'
                END,
            'source_id', provider_directory_source_id,
            'provider_resource_id', provider_directory_provider_resource_id,
            'role_resource_id', provider_directory_role_resource_id,
            'location_resource_id', provider_directory_location_resource_id,
            'match_type', provider_directory_match_type,
            'plan_context_matched', provider_directory_plan_context_matched,
            'network_context_present', provider_directory_network_context_present,
            'network_names', provider_directory_network_names,
            'network_matches', provider_directory_network_matches,
            'insurance_plan_matches', provider_directory_insurance_plan_matches
        ) AS address_verification_evidence,
        provider_directory_plan_context_matched,
        provider_directory_network_context_present,
        provider_directory_insurance_plan_matches,
        provider_directory_network_names,
        provider_directory_network_matches
      FROM matches;
    """


async def publish_provider_directory_ptg_address_corroboration_view(db_schema: str | None = None) -> None:
    await db.status(provider_directory_ptg_address_corroboration_sql(db_schema))


def provider_directory_ptg_address_corroboration_select_sql(db_schema: str | None = None) -> str:
    sql = provider_directory_ptg_address_corroboration_sql(db_schema)
    marker = " AS\n"
    if marker not in sql:
        raise ValueError("Provider Directory PTG corroboration SQL does not contain view AS marker")
    return sql.split(marker, 1)[1].strip().rstrip(";")


def _drop_provider_directory_ptg_address_corroboration_relation_sql(schema: str, relation: str) -> str:
    relation_ref = _qt(schema, relation)
    return f"""
    DO $$
    DECLARE relkind_value "char";
    BEGIN
        SELECT cls.relkind
          INTO relkind_value
          FROM pg_class cls
          JOIN pg_namespace ns ON ns.oid = cls.relnamespace
         WHERE ns.nspname = {_sql_string_literal(schema)}
           AND cls.relname = {_sql_string_literal(relation)};

        IF relkind_value = 'v' THEN
            EXECUTE 'DROP VIEW {relation_ref}';
        ELSIF relkind_value = 'm' THEN
            EXECUTE 'DROP MATERIALIZED VIEW {relation_ref}';
        ELSIF relkind_value IN ('r', 'p') THEN
            EXECUTE 'DROP TABLE {relation_ref}';
        END IF;
    END $$;
    """


async def _create_provider_directory_ptg_address_corroboration_indexes(
    schema: str,
    table_name: str = PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
) -> None:
    table_ref = _qt(schema, table_name)
    statements = (
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_lookup_idx")}
            ON {table_ref} (npi, address_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_active_lookup_idx")}
            ON {table_ref} (npi, address_key, provider_directory_observed_at DESC NULLS LAST)
            WHERE provider_directory_active_match IS TRUE;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_source_snapshot_idx")}
            ON {table_ref} (source_key, snapshot_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_plan_pair_idx")}
            ON {table_ref} (snapshot_id, plan_id, ptg_plan_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_pd_source_idx")}
            ON {table_ref} (provider_directory_source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_qt(schema, "pd_ptg_corrob_network_names_gin")}
            ON {table_ref} USING gin (provider_directory_network_names);
        """,
    )
    for statement in statements:
        await db.status(statement)


async def publish_provider_directory_ptg_address_corroboration_table(
    db_schema: str | None = None,
) -> dict[str, Any]:
    schema = db_schema or _schema()
    relation = PTG_PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
    stage_table = _stage_table_name()
    stage_ref = _qt(schema, stage_table)
    select_sql = provider_directory_ptg_address_corroboration_select_sql(schema)
    try:
        await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
        await db.status(f"CREATE UNLOGGED TABLE {stage_ref} AS\n{select_sql};")
        row_count = int(await db.scalar(f"SELECT COUNT(*) FROM {stage_ref};") or 0)
        await db.status(_drop_provider_directory_ptg_address_corroboration_relation_sql(schema, relation))
        await db.status(f"ALTER TABLE {stage_ref} RENAME TO {_q(relation)};")
        await _create_provider_directory_ptg_address_corroboration_indexes(schema, relation)
        await db.status(f"ANALYZE {_qt(schema, relation)};")
        return {"published": True, "relation": _qt(schema, relation), "rows": row_count, "storage": "table"}
    except Exception:
        try:
            await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
        except Exception:  # pragma: no cover - cleanup best effort
            LOGGER.warning(
                "Failed to clean provider directory PTG address stage table %s",
                stage_ref,
                exc_info=True,
            )
        raise


def provider_directory_location_address_key_sql(
    db_schema: str | None = None,
    *,
    restore_state_from_zip: bool = True,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> str:
    """Stamp imported FHIR Location rows with the shared canonical address key."""

    schema = db_schema or _schema()
    qschema = _q(schema)
    location_ref = _qt(schema, "provider_directory_location")
    source_alias = "loc_src"
    raw_state_expr = f"COALESCE(NULLIF({source_alias}.state_name, ''), {source_alias}.state_code)"
    normalized_state_expr = _state_fips_restore_sql(raw_state_expr)
    normalized_country_expr = _country_restore_sql(f"{source_alias}.country_code")
    needs_country_normalization_expr = (
        f"(NULLIF({normalized_country_expr}, '') IS NOT NULL "
        f"AND {source_alias}.country_code IS DISTINCT FROM {normalized_country_expr})"
    )
    zip_state_select = "geo.state::varchar" if restore_state_from_zip else "NULL::varchar"
    zip_state_join = (
        f"""
          LEFT JOIN {_qt(schema, "geo_zip_lookup")} AS geo
            ON geo.zip_code = normalized.source_zip5
        """
        if restore_state_from_zip
        else ""
    )
    needs_work_clauses = [
        f"{source_alias}.address_key IS NULL",
        f"{source_alias}.zip5 IS NULL",
        f"{source_alias}.state_code IS NULL",
        f"{source_alias}.city_norm IS NULL",
        f"{raw_state_expr} ~ '^[0-9]{{1,2}}$'",
        needs_country_normalization_expr,
    ]
    scope_clauses: list[str] = []
    if run_id is not None and not seen_table:
        scope_clauses.append(f"{source_alias}.last_seen_run_id = :run_id")
    if source_ids:
        scope_clauses.append(f"{source_alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    if seen_table:
        seen_ref = _qt(schema, seen_table)
        seen_run_filter = "AND seen.run_id = :run_id" if run_id is not None else ""
        scope_clauses.append(
            f"""EXISTS (
                SELECT 1
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = 'Location'
                   {seen_run_filter}
                   AND seen.source_id = {source_alias}.source_id
                   AND seen.resource_id = {source_alias}.resource_id
            )"""
        )
    where_clauses = [f"({' OR '.join(needs_work_clauses)})", *scope_clauses]
    where_sql = " AND ".join(where_clauses)
    return f"""
    WITH normalized AS (
        SELECT
            {source_alias}.source_id,
            {source_alias}.resource_id,
            {source_alias}.first_line,
            {source_alias}.second_line,
            {source_alias}.city_name,
            {source_alias}.postal_code,
            {source_alias}.country_code,
            {qschema}.addr_zip5_norm_v1({source_alias}.postal_code)::varchar AS source_zip5,
            {normalized_country_expr} AS normalized_country,
            {raw_state_expr} AS raw_state,
            {normalized_state_expr} AS normalized_state
          FROM {location_ref} AS {source_alias}
         WHERE {where_sql}
    ),
    resolved AS (
        SELECT
            normalized.*,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.normalized_state::varchar, '')), '') IS NULL
                    THEN {zip_state_select}
                ELSE NULL::varchar
            END AS zip_restored_state,
            COALESCE(NULLIF(BTRIM(normalized.normalized_state::varchar), ''), {zip_state_select}) AS resolved_state
          FROM normalized
          {zip_state_join}
    ),
    keyed AS (
        SELECT
            source_id,
            resource_id,
            {qschema}.addr_key_v1(
                first_line,
                second_line,
                city_name,
                resolved_state,
                postal_code,
                COALESCE(NULLIF(normalized_country, ''), 'US')
            ) AS computed_address_key,
            source_zip5 AS computed_zip5,
            {qschema}.addr_state_code_v1(resolved_state)::varchar AS computed_state_code,
            {qschema}.addr_city_norm_v1(city_name)::varchar AS computed_city_norm,
            CASE
                WHEN raw_state ~ '^[0-9]{{1,2}}$'
                    THEN {qschema}.addr_state_code_v1(resolved_state)::varchar
                WHEN NULLIF(BTRIM(COALESCE(raw_state::varchar, '')), '') IS NULL
                 AND zip_restored_state IS NOT NULL
                    THEN {qschema}.addr_state_code_v1(zip_restored_state)::varchar
                ELSE NULL::varchar
            END AS restored_state_name,
            normalized_country
          FROM resolved
    )
    UPDATE {location_ref} AS loc
       SET address_key = keyed.computed_address_key::text,
           zip5 = COALESCE(keyed.computed_zip5, loc.zip5),
           state_name = COALESCE(keyed.restored_state_name, loc.state_name),
           state_code = COALESCE(keyed.computed_state_code, loc.state_code),
           city_norm = COALESCE(keyed.computed_city_norm, loc.city_norm),
           country_code = COALESCE(keyed.normalized_country, loc.country_code),
           updated_at = now()
      FROM keyed
     WHERE loc.source_id = keyed.source_id
       AND loc.resource_id = keyed.resource_id
       AND keyed.computed_address_key IS NOT NULL
       AND (
            loc.address_key IS DISTINCT FROM keyed.computed_address_key::text
         OR loc.zip5 IS DISTINCT FROM COALESCE(keyed.computed_zip5, loc.zip5)
         OR loc.state_name IS DISTINCT FROM COALESCE(keyed.restored_state_name, loc.state_name)
         OR loc.state_code IS DISTINCT FROM COALESCE(keyed.computed_state_code, loc.state_code)
         OR loc.city_norm IS DISTINCT FROM COALESCE(keyed.computed_city_norm, loc.city_norm)
         OR loc.country_code IS DISTINCT FROM COALESCE(keyed.normalized_country, loc.country_code)
       );
    """


async def _address_canon_functions_available(db_schema: str) -> bool:
    value = await db.scalar(
        "SELECT to_regprocedure(:signature);",
        signature=f"{db_schema}.addr_key_v1(text,text,text,text,text,text)",
    )
    return isinstance(value, str) and bool(value)


async def _table_exists(db_schema: str, table_name: str) -> bool:
    return bool(
        await db.scalar(
            "SELECT to_regclass(:qualified_name) IS NOT NULL;",
            qualified_name=f"{db_schema}.{table_name}",
        )
    )


async def publish_provider_directory_location_address_keys(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> int:
    schema = db_schema or _schema()
    if not await _address_canon_functions_available(schema):
        return 0
    restore_state_from_zip = await _table_exists(schema, "geo_zip_lookup")
    params: dict[str, Any] = {}
    if run_id is not None:
        params["run_id"] = run_id
    if source_ids:
        params["source_ids"] = list(source_ids)
    return int(
        await db.status(
            provider_directory_location_address_key_sql(
                schema,
                restore_state_from_zip=restore_state_from_zip,
                run_id=run_id,
                source_ids=source_ids,
                seen_table=seen_table,
            ),
            **params,
        )
        or 0
    )


def _provider_directory_location_archive_stage_table_name(run_id: str | None = None) -> str:
    raw = run_id or f"{os.getpid()}_{time.time_ns()}"
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_STAGE_PREFIX}_{digest}"


def provider_directory_location_archive_stage_sql(
    db_schema: str | None = None,
    stage_table: str | None = None,
) -> str:
    schema = db_schema or _schema()
    stage = stage_table or _provider_directory_location_archive_stage_table_name()
    stage_ref = _qt(schema, stage)
    location_ref = _qt(schema, "provider_directory_location")
    uuid_re = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    return f"""
    CREATE UNLOGGED TABLE {stage_ref} AS
    WITH eligible AS (
        SELECT
            loc.address_key::uuid AS address_key,
            NULLIF(BTRIM(loc.first_line), '')::text AS first_line,
            NULLIF(BTRIM(loc.second_line), '')::text AS second_line,
            NULLIF(BTRIM(loc.city_name), '')::text AS city_name,
            NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), '')::text AS state_name,
            NULLIF(BTRIM(COALESCE(NULLIF(loc.postal_code, ''), loc.zip5)), '')::text AS postal_code,
            COALESCE(NULLIF(BTRIM(loc.country_code), ''), 'US')::text AS country_code,
            loc.updated_at,
            loc.source_id,
            loc.resource_id
          FROM {location_ref} AS loc
         WHERE loc.address_key ~* '{uuid_re}'
           AND NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), '') IS NOT NULL
           AND UPPER(NULLIF(BTRIM(COALESCE(NULLIF(loc.state_name, ''), loc.state_code)), ''))
                NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')
           AND NULLIF(BTRIM(COALESCE(NULLIF(loc.postal_code, ''), loc.zip5)), '') IS NOT NULL
           AND (
                NULLIF(BTRIM(loc.first_line), '') IS NOT NULL
             OR NULLIF(BTRIM(loc.city_name), '') IS NOT NULL
           )
           AND COALESCE(
                NULLIF(
                    UPPER(regexp_replace(COALESCE(NULLIF(loc.country_code, ''), 'US'), '[^A-Z0-9]', '', 'g')),
                    ''
                ),
                'US'
           ) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')
    )
    SELECT DISTINCT ON (address_key)
        address_key,
        first_line,
        second_line,
        city_name,
        state_name,
        postal_code,
        country_code
      FROM eligible
     ORDER BY
        address_key,
        first_line IS NULL,
        length(COALESCE(first_line, '')) DESC,
        city_name IS NULL,
        length(COALESCE(city_name, '')) DESC,
        updated_at DESC NULLS LAST,
        source_id,
        resource_id;
    """


async def publish_provider_directory_location_archive(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    stage_table: str | None = None,
) -> dict[str, Any]:
    schema = db_schema or _schema()
    if not await _address_canon_functions_available(schema):
        return {"skipped": True, "reason": "canonical_functions_unavailable"}
    if not await _table_exists(schema, "address_archive_v2"):
        return {"skipped": True, "reason": "address_archive_v2_unavailable"}
    if not await _table_exists(schema, "provider_directory_location"):
        return {"skipped": True, "reason": "provider_directory_location_unavailable"}
    stage = stage_table or _provider_directory_location_archive_stage_table_name(run_id)
    await db.status(f"DROP TABLE IF EXISTS {_qt(schema, stage)};")
    try:
        await db.status(provider_directory_location_archive_stage_sql(schema, stage))
        await db.status(f"ANALYZE {_qt(schema, stage)};")
        stats = await resolve_into_archive(
            stage,
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city_name",
                "state": "state_name",
                "zip": "postal_code",
                "country": "COALESCE(NULLIF(country_code, ''), 'US')",
            },
            source_bit=PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_SOURCE_BIT,
            priority=PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_PRIORITY,
            schema=schema,
        )
        return dict(stats.__dict__)
    finally:
        await db.status(f"DROP TABLE IF EXISTS {_qt(schema, stage)};")


async def publish_provider_directory_ptg_address_corroboration_if_available(
    db_schema: str | None = None,
) -> bool:
    schema = db_schema or _schema()
    if not await _table_exists(schema, "ptg_address"):
        return False
    await publish_provider_directory_ptg_address_corroboration_table(schema)
    return True


async def _ensure_provider_directory_tables() -> None:
    schema = _schema()
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    for model in (*SOURCE_MODELS, *CANONICAL_RESOURCE_MODELS):
        await db.create_table(model.__table__, checkfirst=True)
    for model in (*SOURCE_MODELS, *CANONICAL_RESOURCE_MODELS):
        for index in getattr(model, "__my_additional_indexes__", []) or []:
            name = index.get("name") or "_".join(index.get("index_elements", ()))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            elements = ", ".join(index.get("index_elements", ()))
            unique = "UNIQUE " if index.get("unique") else ""
            await db.status(
                f"CREATE {unique}INDEX IF NOT EXISTS {name} "
                f"ON {_qt(schema, model.__tablename__)} {using}({elements}){where};"
            )
    await _ensure_provider_directory_import_seen_table(schema)


async def _ensure_provider_directory_import_seen_table(schema: str | None = None) -> None:
    schema = schema or _schema()
    seen_ref = _qt(schema, PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
    await db.status(
        f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {seen_ref} (
            run_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            seen_at timestamp NOT NULL DEFAULT now(),
            PRIMARY KEY (run_id, resource_type, source_id, resource_id)
        );
        """
    )
    # The primary key is (run_id, resource_type, source_id, resource_id), so it
    # already supports the stale-delete prefix lookup. The old prefix index only
    # doubled index writes during large imports.
    await db.status(
        f"""
        DROP INDEX IF EXISTS {_qt(schema, "provider_directory_import_seen_source_idx")};
        """
    )


async def _ensure_provider_directory_import_seen_stage_table(
    run_id: str | None,
    *,
    schema: str | None = None,
) -> str | None:
    if not run_id:
        return None
    schema = schema or _schema()
    stage_table = _provider_directory_import_seen_stage_table_name(run_id)
    stage_ref = _qt(schema, stage_table)
    await db.status(
        f"""
        CREATE UNLOGGED TABLE IF NOT EXISTS {stage_ref} (
            run_id varchar(64) NOT NULL,
            resource_type varchar(64) NOT NULL,
            source_id varchar(64) NOT NULL,
            resource_id varchar(256) NOT NULL,
            seen_at timestamp NOT NULL DEFAULT now()
        );
        """
    )
    await db.status(f"TRUNCATE TABLE {stage_ref};")
    return stage_table


async def _drop_provider_directory_import_seen_stage_table(
    stage_table: str | None,
    *,
    schema: str | None = None,
) -> None:
    if not stage_table:
        return
    schema = schema or _schema()
    await db.status(f"DROP TABLE IF EXISTS {_qt(schema, stage_table)};")


async def _prepare_provider_directory_import_seen_stage_lookup(
    stage_table: str,
    *,
    schema: str | None = None,
) -> None:
    schema = schema or _schema()
    stage_ref = _qt(schema, stage_table)
    index_name = f"{stage_table}_lookup_idx"
    await db.status(
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name)}
            ON {stage_ref} (resource_type, source_id, resource_id);
        """
    )
    await db.status(f"ANALYZE {stage_ref};")


def _dedupe_rows_by_primary_key(primary_keys: list[str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(primary_key) for primary_key in primary_keys)
        if any(value is None for value in key):
            continue
        deduped[key] = row
    return list(deduped.values())


def _max_rows_per_statement(column_count: int) -> int:
    return max(1, min(500, 30000 // max(column_count, 1)))


def _copy_upsert_enabled() -> bool:
    return os.getenv("HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT", "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _seen_stage_enabled() -> bool:
    return os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SEEN_STAGE", "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _copy_upsert_min_rows() -> int:
    try:
        return max(int(os.getenv("HLTHPRT_PROVIDER_DIRECTORY_COPY_UPSERT_MIN_ROWS", "100")), 1)
    except ValueError:
        return 100


def _short_error(err: BaseException) -> str:
    message = str(getattr(err, "orig", err)).replace("\n", " ").strip()
    if len(message) > 240:
        return f"{message[:240]}..."
    return message


def _stage_table_name() -> str:
    return f"pd_stage_{os.getpid()}_{time.time_ns()}"


def _provider_directory_import_seen_stage_table_name(run_id: str) -> str:
    digest = hashlib.sha1(run_id.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{PROVIDER_DIRECTORY_IMPORT_SEEN_STAGE_PREFIX}_{digest}"


def _json_columns(table) -> set[str]:
    result: set[str] = set()
    for column in table.columns:
        type_name = column.type.__class__.__name__.upper()
        if "JSON" in type_name:
            result.add(column.name)
    return result


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return str(value)


def _strip_postgres_nuls(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_postgres_nuls(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_postgres_nuls(item) for item in value)
    if isinstance(value, dict):
        return {
            _strip_postgres_nuls(key): _strip_postgres_nuls(item)
            for key, item in value.items()
        }
    return value


def _copy_record(row: dict[str, Any], columns: list[str], json_columns: set[str]) -> tuple[Any, ...]:
    values: list[Any] = []
    for column in columns:
        value = _strip_postgres_nuls(row.get(column))
        if value is not None and column in json_columns:
            value = json.dumps(value, sort_keys=True, default=_json_default)
        values.append(value)
    return tuple(values)


PROVIDER_DIRECTORY_RUN_METADATA_COLUMNS = frozenset({"last_seen_run_id", "observed_at", "updated_at"})
PROVIDER_DIRECTORY_LOCATION_ADDRESS_KEY_INPUT_COLUMNS = (
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "state_code",
    "postal_code",
    "country_code",
)


def _preserve_null_location_address_key(table) -> bool:
    return table.name == ProviderDirectoryLocation.__tablename__


def _location_address_key_update_expression(table, excluded):
    raw_address_changed = or_(
        *(
            getattr(table.c, column).is_distinct_from(getattr(excluded, column))
            for column in PROVIDER_DIRECTORY_LOCATION_ADDRESS_KEY_INPUT_COLUMNS
        )
    )
    return case(
        (excluded.address_key.isnot(None), excluded.address_key),
        (raw_address_changed, None),
        else_=table.c.address_key,
    )


def _location_address_key_update_sql(
    *,
    target_prefix: str,
    incoming_prefix: str,
) -> str:
    raw_address_changed = " OR ".join(
        f"{target_prefix}.{_q(column)} IS DISTINCT FROM {incoming_prefix}.{_q(column)}"
        for column in PROVIDER_DIRECTORY_LOCATION_ADDRESS_KEY_INPUT_COLUMNS
    )
    address_key = _q("address_key")
    return (
        f"CASE "
        f"WHEN {incoming_prefix}.{address_key} IS NOT NULL THEN {incoming_prefix}.{address_key} "
        f"WHEN {raw_address_changed} THEN NULL "
        f"ELSE {target_prefix}.{address_key} "
        f"END"
    )


def _effective_update_expression(table, statement, column: str):
    excluded_value = getattr(statement.excluded, column)
    if table.name == ProviderDirectoryCanonicalResource.__tablename__ and column == "first_seen_run_id":
        return func.coalesce(table.c.first_seen_run_id, excluded_value)
    if column == "address_key" and _preserve_null_location_address_key(table):
        return _location_address_key_update_expression(table, statement.excluded)
    return excluded_value


def _effective_update_sql(table, column: str, *, target_prefix: str, incoming_prefix: str) -> str:
    if table.name == ProviderDirectoryCanonicalResource.__tablename__ and column == "first_seen_run_id":
        return f"COALESCE({target_prefix}.{_q(column)}, {incoming_prefix}.{_q(column)})"
    if column == "address_key" and _preserve_null_location_address_key(table):
        return _location_address_key_update_sql(
            target_prefix=target_prefix,
            incoming_prefix=incoming_prefix,
        )
    return f"{incoming_prefix}.{_q(column)}"


async def _mark_resource_rows_seen(
    model,
    rows: list[dict[str, Any]],
    run_id: str | None,
    *,
    seen_table: str | None = None,
) -> int:
    resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
    if not run_id or not resource_type or not rows:
        return 0
    seen: dict[tuple[str, str], tuple[str, str]] = {}
    for row in rows:
        source_id = _clean_text(row.get("source_id"))
        resource_id = _clean_text(row.get("resource_id"))
        if source_id and resource_id:
            seen[(source_id, resource_id)] = (source_id, resource_id)
    if not seen:
        return 0
    items = list(seen.values())
    if _copy_upsert_enabled() and len(items) >= _copy_upsert_min_rows():
        try:
            return await _copy_mark_resource_rows_seen(resource_type, items, run_id, seen_table=seen_table)
        except Exception as exc:  # pragma: no cover - exercised on driver-specific fallback paths
            print(f"Provider Directory COPY seen fallback for {resource_type}: {_short_error(exc)}")
    max_rows = _max_rows_per_statement(4)
    total = 0
    seen_ref = _qt(_schema(), seen_table or PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
    conflict_sql = (
        ""
        if seen_table
        else "ON CONFLICT (run_id, resource_type, source_id, resource_id) DO NOTHING"
    )
    for offset in range(0, len(items), max_rows):
        batch = items[offset : offset + max_rows]
        params: dict[str, Any] = {
            "run_id": run_id,
            "resource_type": resource_type,
        }
        values_sql: list[str] = []
        for idx, (source_id, resource_id) in enumerate(batch):
            params[f"source_id_{idx}"] = source_id
            params[f"resource_id_{idx}"] = resource_id
            values_sql.append(
                f"(:run_id, :resource_type, :source_id_{idx}, :resource_id_{idx})"
            )
        await db.status(
            f"""
            INSERT INTO {seen_ref} (run_id, resource_type, source_id, resource_id)
            VALUES {", ".join(values_sql)}
            {conflict_sql};
            """,
            **params,
        )
        total += len(batch)
    return total


async def _copy_mark_resource_rows_seen(
    resource_type: str,
    items: list[tuple[str, str]],
    run_id: str,
    *,
    seen_table: str | None = None,
) -> int:
    schema = _schema()
    if seen_table:
        columns = ["run_id", "resource_type", "source_id", "resource_id"]
        records = [(run_id, resource_type, source_id, resource_id) for source_id, resource_id in items]
        async with db.acquire() as conn:
            raw_conn = conn.raw_connection
            driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
            copy_method = getattr(driver_conn, "copy_records_to_table", None)
            if copy_method is None:
                raise NotImplementedError("active database driver lacks copy_records_to_table")
            await copy_method(seen_table, schema_name=schema, columns=columns, records=records)
        return len(items)

    seen_ref = _qt(schema, PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
    stage_table = _stage_table_name()
    columns = ["run_id", "resource_type", "source_id", "resource_id"]
    records = [(run_id, resource_type, source_id, resource_id) for source_id, resource_id in items]
    async with db.acquire() as conn:
        await conn.status(
            f"""
            CREATE TEMP TABLE {_q(stage_table)}
            (LIKE {seen_ref} INCLUDING DEFAULTS) ON COMMIT DROP;
            """
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_method = getattr(driver_conn, "copy_records_to_table", None)
        if copy_method is None:
            raise NotImplementedError("active database driver lacks copy_records_to_table")
        await copy_method(stage_table, columns=columns, records=records)
        quoted_columns = ", ".join(_q(column) for column in columns)
        await conn.status(
            f"""
            INSERT INTO {seen_ref} ({quoted_columns})
            SELECT DISTINCT {quoted_columns}
            FROM {_q(stage_table)}
            ON CONFLICT (run_id, resource_type, source_id, resource_id) DO NOTHING;
            """
        )
    return len(items)


async def _clear_resource_rows_seen(run_id: str | None) -> int:
    if not run_id:
        return 0
    return int(
        await db.status(
            f"""
            DELETE FROM {_qt(_schema(), PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)}
             WHERE run_id = :run_id;
            """,
            run_id=run_id,
        )
        or 0
    )


def _canonical_resource_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key
        not in {
            "source_id",
            "last_seen_run_id",
            "observed_at",
            "updated_at",
        }
    }


def _canonical_resource_rows(
    model,
    rows: list[dict[str, Any]],
    *,
    canonical_api_base: str | None,
    run_id: str | None,
) -> list[dict[str, Any]]:
    resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
    api_base = _canonical_base(canonical_api_base)
    if not resource_type or not api_base:
        return []
    seen: dict[str, dict[str, Any]] = {}
    for row in rows:
        resource_id = _clean_text(row.get("resource_id"))
        if not resource_id:
            continue
        payload = _canonical_resource_payload(row)
        payload_hash = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=_json_default).encode("utf-8")
        ).hexdigest()
        observed_at = row.get("observed_at") or _now()
        seen[resource_id] = {
            "canonical_api_base": api_base,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "resource_url": row.get("resource_url"),
            "payload_hash": payload_hash,
            "payload_json": payload,
            "first_seen_run_id": run_id,
            "last_seen_run_id": run_id,
            "observed_at": observed_at,
            "updated_at": row.get("updated_at") or observed_at,
        }
    return list(seen.values())


def _source_resource_edge_rows(
    model,
    rows: list[dict[str, Any]],
    *,
    canonical_api_base: str | None,
    source_ids: list[str],
    run_id: str | None,
) -> list[dict[str, Any]]:
    resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
    api_base = _canonical_base(canonical_api_base)
    if not resource_type or not api_base or not source_ids:
        return []
    resource_ids = {
        resource_id
        for row in rows
        if (resource_id := _clean_text(row.get("resource_id")))
    }
    observed_at = _now()
    return [
        {
            "source_id": source_id,
            "canonical_api_base": api_base,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "last_seen_run_id": run_id,
            "observed_at": observed_at,
            "updated_at": observed_at,
        }
        for source_id in source_ids
        for resource_id in sorted(resource_ids)
    ]


def _upsert_changed_row_predicate(table, statement, columns: list[str], primary_keys: list[str]):
    payload_columns = [
        column
        for column in columns
        if column not in primary_keys and column not in PROVIDER_DIRECTORY_RUN_METADATA_COLUMNS
    ]
    if not payload_columns:
        return None
    predicates = []
    for column in payload_columns:
        current_value = getattr(table.c, column)
        excluded_value = _effective_update_expression(table, statement, column)
        if isinstance(current_value.type, SQLAlchemyJSON):
            current_value = current_value.cast(JSONB)
            excluded_value = excluded_value.cast(JSONB)
        predicates.append(current_value.is_distinct_from(excluded_value))
    return or_(
        *predicates
    )


def _copy_upsert_changed_where_sql(table, columns: list[str], primary_keys: list[str]) -> str:
    payload_columns = [
        column
        for column in columns
        if column not in primary_keys and column not in PROVIDER_DIRECTORY_RUN_METADATA_COLUMNS
    ]
    predicates: list[str] = []
    json_columns = _json_columns(table)
    target_table = _q(table.name)
    for column in payload_columns:
        quoted = _q(column)
        excluded_sql = _effective_update_sql(
            table,
            column,
            target_prefix=target_table,
            incoming_prefix="EXCLUDED",
        )
        if column in json_columns:
            predicates.append(
                f"{target_table}.{quoted}::jsonb IS DISTINCT FROM {excluded_sql}::jsonb"
            )
        else:
            predicates.append(f"{target_table}.{quoted} IS DISTINCT FROM {excluded_sql}")
    return " OR ".join(predicates)


def _copy_stage_primary_key_join_sql(
    primary_keys: list[str],
    *,
    target_alias: str = "target_row",
    stage_alias: str = "stage_row",
) -> str:
    return " AND ".join(
        f"{target_alias}.{_q(column)} = {stage_alias}.{_q(column)}"
        for column in primary_keys
    )


def _copy_stage_changed_where_sql(
    table,
    columns: list[str],
    primary_keys: list[str],
    *,
    target_alias: str = "target_row",
    stage_alias: str = "stage_row",
) -> str:
    payload_columns = [
        column
        for column in columns
        if column not in primary_keys and column not in PROVIDER_DIRECTORY_RUN_METADATA_COLUMNS
    ]
    missing_predicate = f"{target_alias}.{_q(primary_keys[0])} IS NULL"
    predicates = [missing_predicate]
    json_columns = _json_columns(table)
    for column in payload_columns:
        quoted = _q(column)
        incoming_sql = _effective_update_sql(
            table,
            column,
            target_prefix=target_alias,
            incoming_prefix=stage_alias,
        )
        if column in json_columns:
            predicates.append(
                f"{target_alias}.{quoted}::jsonb IS DISTINCT FROM {incoming_sql}::jsonb"
            )
        else:
            predicates.append(f"{target_alias}.{quoted} IS DISTINCT FROM {incoming_sql}")
    return " OR ".join(predicates)


async def _upsert_rows_values(
    model,
    normalized: list[dict[str, Any]],
    columns: list[str],
    primary_keys: list[str],
    *,
    skip_unchanged: bool,
) -> int:
    if not normalized:
        return 0
    max_rows_per_statement = _max_rows_per_statement(len(columns))
    if len(normalized) > max_rows_per_statement:
        total = 0
        for offset in range(0, len(normalized), max_rows_per_statement):
            total += await _upsert_rows_values(
                model,
                normalized[offset : offset + max_rows_per_statement],
                columns,
                primary_keys,
                skip_unchanged=skip_unchanged,
            )
        return total
    table = model.__table__
    async with db.session() as session:
        statement = pg_insert(table).values(normalized)
        update_columns = {
            column.name: _effective_update_expression(table, statement, column.name)
            for column in table.columns
            if column.name not in primary_keys
        }
        update_where = _upsert_changed_row_predicate(table, statement, columns, primary_keys) if skip_unchanged else None
        statement = statement.on_conflict_do_update(
            index_elements=primary_keys,
            set_=update_columns,
            where=update_where,
        )
        await session.execute(statement)
    return len(normalized)


async def _copy_upsert_rows(
    model,
    normalized: list[dict[str, Any]],
    columns: list[str],
    primary_keys: list[str],
    *,
    skip_unchanged: bool,
) -> int:
    table = model.__table__
    schema = table.schema or _schema()
    stage_table = _stage_table_name()
    target_ref = _qt(schema, table.name)
    quoted_stage = _q(stage_table)
    quoted_columns = ", ".join(_q(column) for column in columns)
    quoted_conflict = ", ".join(_q(column) for column in primary_keys)
    update_columns = [column for column in columns if column not in primary_keys]
    if update_columns:
        target_table = _q(table.name)
        conflict_sql = (
            "DO UPDATE SET "
            + ", ".join(
                f"{_q(column)} = "
                f"{_effective_update_sql(table, column, target_prefix=target_table, incoming_prefix='EXCLUDED')}"
                for column in update_columns
            )
        )
        update_where = _copy_upsert_changed_where_sql(table, columns, primary_keys) if skip_unchanged else ""
        if update_where:
            conflict_sql = f"{conflict_sql} WHERE {update_where}"
    else:
        conflict_sql = "DO NOTHING"

    json_columns = _json_columns(table)
    records = [_copy_record(row, columns, json_columns) for row in normalized]
    select_sql = f"SELECT {quoted_columns}\n            FROM {quoted_stage}"
    if skip_unchanged:
        stage_alias = "stage_row"
        target_alias = "target_row"
        changed_where = _copy_stage_changed_where_sql(
            table,
            columns,
            primary_keys,
            target_alias=target_alias,
            stage_alias=stage_alias,
        )
        primary_key_join = _copy_stage_primary_key_join_sql(
            primary_keys,
            target_alias=target_alias,
            stage_alias=stage_alias,
        )
        select_columns = ", ".join(f"{stage_alias}.{_q(column)}" for column in columns)
        select_sql = f"""
            SELECT {select_columns}
            FROM {quoted_stage} AS {stage_alias}
            LEFT JOIN {target_ref} AS {target_alias}
              ON {primary_key_join}
            WHERE {changed_where}
            """
    async with db.acquire() as conn:
        await conn.status(
            f"""
            CREATE TEMP TABLE {quoted_stage}
            (LIKE {target_ref} INCLUDING DEFAULTS) ON COMMIT DROP;
            """
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_method = getattr(driver_conn, "copy_records_to_table", None)
        if copy_method is None:
            raise NotImplementedError("active database driver lacks copy_records_to_table")
        await copy_method(stage_table, columns=columns, records=records)
        if skip_unchanged:
            await conn.status(f"ANALYZE {quoted_stage};")
        await conn.status(
            f"""
            INSERT INTO {target_ref} ({quoted_columns})
            {select_sql}
            ON CONFLICT ({quoted_conflict}) {conflict_sql};
            """
        )
    return len(normalized)


async def _upsert_rows(
    model,
    rows: list[dict[str, Any]],
    *,
    skip_unchanged: bool = False,
) -> int:
    if not rows:
        return 0
    table = model.__table__
    columns = [column.name for column in table.columns]
    primary_keys = [column.name for column in table.primary_key.columns]
    rows = _dedupe_rows_by_primary_key(primary_keys, rows)
    if not rows:
        return 0
    normalized = [{key: row.get(key) for key in columns} for row in rows]
    if _copy_upsert_enabled() and len(normalized) >= _copy_upsert_min_rows():
        try:
            return await _copy_upsert_rows(
                model,
                normalized,
                columns,
                primary_keys,
                skip_unchanged=skip_unchanged,
            )
        except Exception as exc:  # pragma: no cover - exercised on driver-specific fallback paths
            print(f"Provider Directory COPY upsert fallback for {model.__tablename__}: {_short_error(exc)}")
    return await _upsert_rows_values(
        model,
        normalized,
        columns,
        primary_keys,
        skip_unchanged=skip_unchanged,
    )


def _status_row_count(status: Any) -> int:
    if isinstance(status, int):
        return status
    if not status:
        return 0
    match = re.search(r"(\d+)(?!.*\d)", str(status))
    return int(match.group(1)) if match else 0


def provider_directory_location_contact_backfill_sql(schema: str) -> str:
    table_ref = _qt(schema, ProviderDirectoryLocation.__tablename__)
    phone_number = _canonical_contact_number_expr("loc.telephone_number", "loc.country_code")
    phone_extension = _contact_extension_expr("loc.telephone_number")
    fax_number_digits = _canonical_contact_number_expr("loc.fax_number", "loc.country_code")
    fax_extension = _contact_extension_expr("loc.fax_number")
    return f"""
        WITH computed AS (
            SELECT loc.source_id,
                   loc.resource_id,
                   {phone_number}::varchar AS phone_number,
                   {phone_extension}::varchar AS phone_extension,
                   {fax_number_digits}::varchar AS fax_number_digits,
                   {fax_extension}::varchar AS fax_extension
              FROM {table_ref} AS loc
             WHERE NULLIF(BTRIM(COALESCE(loc.telephone_number, '')), '') IS NOT NULL
                OR NULLIF(BTRIM(COALESCE(loc.fax_number, '')), '') IS NOT NULL
        )
        UPDATE {table_ref} AS target
           SET phone_number = computed.phone_number,
               phone_extension = computed.phone_extension,
               fax_number_digits = computed.fax_number_digits,
               fax_extension = computed.fax_extension,
               updated_at = now()
          FROM computed
         WHERE target.source_id = computed.source_id
           AND target.resource_id = computed.resource_id
           AND (
                target.phone_number IS DISTINCT FROM computed.phone_number
             OR target.phone_extension IS DISTINCT FROM computed.phone_extension
             OR target.fax_number_digits IS DISTINCT FROM computed.fax_number_digits
             OR target.fax_extension IS DISTINCT FROM computed.fax_extension
           );
    """


async def backfill_provider_directory_location_contacts() -> dict[str, Any]:
    await _ensure_provider_directory_tables()
    updated = _status_row_count(await db.status(provider_directory_location_contact_backfill_sql(_schema())))
    summary = {"location_contact_rows_updated": updated}
    print("PROVIDER_DIRECTORY_CONTACT_BACKFILL_DONE\t" + json.dumps(summary, sort_keys=True, default=str))
    return summary


def _canonical_backfill_resource_sql(resource_type: str, table_name: str) -> tuple[str, str]:
    schema = _schema()
    source_ref = _qt(schema, "provider_directory_source")
    resource_ref = _qt(schema, table_name)
    canonical_ref = _qt(schema, ProviderDirectoryCanonicalResource.__tablename__)
    edge_ref = _qt(schema, ProviderDirectorySourceResource.__tablename__)
    canonical_target = _q(ProviderDirectoryCanonicalResource.__tablename__)
    edge_target = _q(ProviderDirectorySourceResource.__tablename__)
    resource_type_literal = _sql_string_literal(resource_type)
    payload_sql = (
        "(to_jsonb(r) - 'source_id' - 'last_seen_run_id' - 'observed_at' - 'updated_at')"
    )
    base_sql = "COALESCE(NULLIF(src.canonical_api_base, ''), NULLIF(src.api_base, ''))"
    rows_where = (
        f"{base_sql} IS NOT NULL "
        "AND NULLIF(r.source_id, '') IS NOT NULL "
        "AND NULLIF(r.resource_id, '') IS NOT NULL"
    )
    canonical_sql = f"""
        INSERT INTO {canonical_ref} (
            canonical_api_base,
            resource_type,
            resource_id,
            resource_url,
            payload_hash,
            payload_json,
            first_seen_run_id,
            last_seen_run_id,
            observed_at,
            updated_at
        )
        SELECT canonical_api_base,
               resource_type,
               resource_id,
               resource_url,
               payload_hash,
               payload_json,
               first_seen_run_id,
               last_seen_run_id,
               observed_at,
               updated_at
          FROM (
            SELECT DISTINCT ON ({base_sql}, r.resource_id)
                   {base_sql} AS canonical_api_base,
                   {resource_type_literal} AS resource_type,
                   r.resource_id,
                   r.resource_url,
                   md5(({payload_sql})::text) AS payload_hash,
                   {payload_sql} AS payload_json,
                   r.last_seen_run_id AS first_seen_run_id,
                   r.last_seen_run_id,
                   r.observed_at,
                   COALESCE(r.updated_at, r.observed_at) AS updated_at
              FROM {resource_ref} AS r
              JOIN {source_ref} AS src
                ON src.source_id = r.source_id
             WHERE {rows_where}
             ORDER BY {base_sql},
                      r.resource_id,
                      r.updated_at DESC NULLS LAST,
                      r.observed_at DESC NULLS LAST,
                      r.source_id
          ) AS ranked
        ON CONFLICT (canonical_api_base, resource_type, resource_id) DO UPDATE
            SET resource_url = EXCLUDED.resource_url,
                payload_hash = EXCLUDED.payload_hash,
                payload_json = EXCLUDED.payload_json,
                first_seen_run_id = COALESCE(
                    {canonical_target}.first_seen_run_id,
                    EXCLUDED.first_seen_run_id
                ),
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                observed_at = EXCLUDED.observed_at,
                updated_at = EXCLUDED.updated_at;
    """
    edge_sql = f"""
        INSERT INTO {edge_ref} (
            source_id,
            canonical_api_base,
            resource_type,
            resource_id,
            last_seen_run_id,
            observed_at,
            updated_at
        )
        SELECT DISTINCT r.source_id,
               {base_sql} AS canonical_api_base,
               {resource_type_literal} AS resource_type,
               r.resource_id,
               r.last_seen_run_id,
               r.observed_at,
               COALESCE(r.updated_at, r.observed_at) AS updated_at
          FROM {resource_ref} AS r
          JOIN {source_ref} AS src
            ON src.source_id = r.source_id
         WHERE {rows_where}
        ON CONFLICT (source_id, resource_type, resource_id) DO UPDATE
            SET canonical_api_base = EXCLUDED.canonical_api_base,
                last_seen_run_id = EXCLUDED.last_seen_run_id,
                observed_at = EXCLUDED.observed_at,
                updated_at = EXCLUDED.updated_at;
    """
    return canonical_sql, edge_sql


async def backfill_provider_directory_canonical_resources(
    *,
    resources: str | None = None,
) -> dict[str, Any]:
    await _ensure_provider_directory_tables()
    selected = _selected_resources(resources)
    summary: dict[str, Any] = {
        "resources": {},
        "canonical_rows": 0,
        "source_edge_rows": 0,
    }
    for resource_type in selected:
        model = RESOURCE_MODELS_BY_TYPE.get(resource_type)
        if model is None:
            continue
        canonical_sql, edge_sql = _canonical_backfill_resource_sql(resource_type, model.__tablename__)
        canonical_rows = _status_row_count(await db.status(canonical_sql))
        source_edge_rows = _status_row_count(await db.status(edge_sql))
        summary["resources"][resource_type] = {
            "canonical_rows": canonical_rows,
            "source_edge_rows": source_edge_rows,
        }
        summary["canonical_rows"] += canonical_rows
        summary["source_edge_rows"] += source_edge_rows
    print("PROVIDER_DIRECTORY_CANONICAL_BACKFILL_DONE\t" + json.dumps(summary, sort_keys=True, default=str))
    return summary


def _seed_rows_from_sqlite(path: Path, *, limit: int | None = None, source_query: str | None = None) -> list[dict[str, Any]]:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    try:
        sql = "SELECT * FROM payers"
        params: list[Any] = []
        if source_query:
            sql += " WHERE lower(org_name) LIKE ? OR lower(plan_name) LIKE ?"
            needle = f"%{source_query.lower()}%"
            params.extend([needle, needle])
        sql += " ORDER BY id"
        if limit and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        return [dict(row) for row in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _download_seed_db(seed_db_url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(seed_db_url, headers={"User-Agent": USER_AGENT})
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    tmp.replace(destination)
    return destination


def _resolve_seed_db(seed_db_path: str | None, seed_db_url: str | None) -> tuple[Path | None, tempfile.TemporaryDirectory | None]:
    if seed_db_path:
        return Path(seed_db_path), None
    if not seed_db_url:
        seed_db_url = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SEED_DB_URL", DEFAULT_SEED_DB_URL)
    tmpdir = tempfile.TemporaryDirectory()
    path = Path(tmpdir.name) / "provider_directory.db"
    return _download_seed_db(seed_db_url, path), tmpdir


def _ssl_context() -> ssl.SSLContext:
    verify = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_TLS_VERIFY", "true").lower() not in {"0", "false", "no"}
    context = ssl.create_default_context()
    if not verify:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _classify_http(status_code: int | None, error: str | None, payload: dict[str, Any] | None) -> str:
    if error:
        lower = error.lower()
        if "timeout" in lower or "timed out" in lower:
            return "timeout"
        if "ssl" in lower or "certificate" in lower:
            return "ssl_error"
        if "name or service not known" in lower or "getaddrinfo" in lower:
            return "dns_failure"
        return "unreachable"
    if status_code == 200:
        if payload and payload.get("resourceType") == "CapabilityStatement":
            return "valid"
        return "valid_non_fhir"
    if status_code in {401, 403}:
        return "auth_required"
    if status_code == 404:
        return "not_found"
    if status_code and 400 <= status_code < 500:
        return "client_error"
    if status_code and 500 <= status_code < 600:
        return "server_error"
    return f"http_{status_code}" if status_code else "unreachable"


def _decode_json_body(body: bytes) -> dict[str, Any] | None:
    try:
        payload = json.loads(body.decode("utf-8-sig", errors="replace"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _read_response_body_with_deadline(
    response: Any,
    *,
    timeout: int,
    max_bytes: int = MAX_FHIR_JSON_BODY_BYTES,
) -> bytes:
    deadline = time.monotonic() + max(1, timeout)
    chunks: list[bytes] = []
    total = 0
    while total < max_bytes:
        if time.monotonic() > deadline:
            raise TimeoutError(f"response body read exceeded {timeout}s")
        chunk = response.read(min(READ_CHUNK_BYTES, max_bytes - total))
        if not chunk:
            break
        chunks.append(chunk)
        total += len(chunk)
    return b"".join(chunks)


def _fetch_json_sync(
    url: str,
    *,
    timeout: int,
    extra_headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    headers = {
        "Accept": "application/fhir+json, application/json;q=0.9, */*;q=0.1",
        "User-Agent": USER_AGENT,
    }
    if extra_headers:
        headers.update(extra_headers)
    fetch_url = _url_with_query_params(url, query_params)
    started = time.monotonic()
    request = urllib.request.Request(fetch_url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_ssl_context()) as response:
            body = _read_response_body_with_deadline(response, timeout=timeout)
            return response.status, _decode_json_body(body), None, int((time.monotonic() - started) * 1000)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read(1024 * 1024)
        except Exception:
            body = b""
        return exc.code, _decode_json_body(body), None, int((time.monotonic() - started) * 1000)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return None, None, f"{type(exc).__name__}: {exc}", int((time.monotonic() - started) * 1000)


def _post_json_sync(
    url: str,
    payload: dict[str, Any],
    *,
    timeout: int,
    extra_headers: dict[str, str] | None = None,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    if extra_headers:
        headers.update(extra_headers)
    started = time.monotonic()
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout, context=_ssl_context()) as response:
            body = _read_response_body_with_deadline(response, timeout=timeout)
            return response.status, _decode_json_body(body), None, int((time.monotonic() - started) * 1000)
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read(1024 * 1024)
        except Exception:
            body = b""
        return exc.code, _decode_json_body(body), None, int((time.monotonic() - started) * 1000)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return None, None, f"{type(exc).__name__}: {exc}", int((time.monotonic() - started) * 1000)


async def _fetch_json(url: str, *, timeout: int) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    return await asyncio.to_thread(_fetch_json_sync, url, timeout=timeout)


async def _fetch_json_with_options(
    url: str,
    *,
    timeout: int,
    extra_headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    return await asyncio.to_thread(
        _fetch_json_sync,
        url,
        timeout=timeout,
        extra_headers=extra_headers,
        query_params=query_params,
    )


async def _fetch_source_json(
    source: dict[str, Any],
    url: str,
    *,
    timeout: int,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    options = _credential_request_options_for_source(source, url)
    if not options["headers"] and not options["query_params"]:
        return await _fetch_json(url, timeout=timeout)
    return await _fetch_json_with_options(
        url,
        timeout=timeout,
        extra_headers=options["headers"],
        query_params=options["query_params"],
    )


async def _probe_source(source: dict[str, Any], *, timeout: int, run_id: str | None) -> tuple[dict[str, Any], dict[str, Any] | None]:
    urls = _candidate_metadata_urls(source)
    if not urls:
        probe = {
            "status": "no_api",
            "http_status": None,
            "response_time_ms": 0,
            "url": None,
            "error": "missing api_base",
            "run_id": run_id,
        }
        return probe, None
    best_probe: dict[str, Any] | None = None
    best_payload: dict[str, Any] | None = None
    for candidate_base, url in urls:
        status_code, payload, error, elapsed = await _fetch_source_json(source, url, timeout=timeout)
        status = _classify_http(status_code, error, payload)
        credential = _credential_request_options_for_source(source, url)
        if (
            status == "valid_non_fhir"
            and (_source_declares_credentialed_access(source) or _source_uses_known_onboarding_gateway(source))
            and not credential["descriptor"]
        ):
            status = "auth_required"
            error = error or "source requires credentialed Provider Directory access but no matching credentials are configured"
        probe = {
            "status": status,
            "http_status": status_code,
            "response_time_ms": elapsed,
            "url": url,
            "api_base": candidate_base,
            "error": error,
            "run_id": run_id,
            "credential": credential["descriptor"],
        }
        if status == "valid":
            return probe, payload
        if best_probe is None or status_code == 200:
            best_probe = probe
            best_payload = payload
    assert best_probe is not None
    return best_probe, best_payload


async def _probe_sources(
    sources: list[dict[str, Any]],
    *,
    timeout: int,
    concurrency: int,
    run_id: str | None,
) -> tuple[int, int, set[str]]:
    semaphore = asyncio.Semaphore(max(1, min(concurrency, 10)))
    capability_rows: list[dict[str, Any]] = []
    source_updates: list[dict[str, Any]] = []
    valid_source_ids: set[str] = set()
    valid = 0
    probed = 0
    total_sources = len(sources)
    report_every = max(1, total_sources // 20)

    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory probing sources",
        done=0,
        total=total_sources,
        message=f"probing 0/{total_sources} source(s)",
    )

    async def _run_probe(source: dict[str, Any]) -> None:
        nonlocal probed, valid
        async with semaphore:
            probe, payload = await _probe_source(source, timeout=timeout, run_id=run_id)
            now = _now()
            update = {
                "source_id": source["source_id"],
                **source,
                "last_probe_status": probe["status"],
                "last_probe_status_code": probe.get("http_status"),
                "last_probe_error": probe.get("error"),
                "last_probe_run_id": run_id,
                "last_probed_at": now,
                "last_validated_status": probe["status"],
                "last_validated_at": now,
                "updated_at": now,
            }
            resolved_api_base = _canonical_base(probe.get("api_base"))
            original_api_base = _canonical_base(source.get("api_base"))
            metadata = source.get("metadata_json") if isinstance(source.get("metadata_json"), dict) else {}
            update["metadata_json"] = {
                **metadata,
                "last_metadata_url": probe.get("url"),
                **(
                    {
                        "resolved_api_base": resolved_api_base,
                        "resolved_api_base_from": original_api_base,
                    }
                    if resolved_api_base and resolved_api_base != original_api_base
                    else {}
                ),
                **({"credential": probe.get("credential")} if probe.get("credential") else {}),
            }
            if resolved_api_base and probe["status"] == "valid":
                source["api_base"] = resolved_api_base
                source["canonical_api_base"] = resolved_api_base
                update["api_base"] = resolved_api_base
                update["canonical_api_base"] = resolved_api_base
            if payload and payload.get("resourceType") == "CapabilityStatement":
                capability_rows.append(parse_capability(source, payload, probe))
                update["fhir_version"] = _clean_text(payload.get("fhirVersion")) or update.get("fhir_version")
                valid += 1
                valid_source_ids.add(source["source_id"])
            else:
                capability_rows.append(
                    {
                        "source_id": source["source_id"],
                        "api_base": source.get("api_base"),
                        "metadata_url": probe.get("url"),
                        "probe_status": probe["status"],
                        "http_status": probe.get("http_status"),
                        "response_time_ms": probe.get("response_time_ms"),
                        "auth_required": probe["status"] == "auth_required",
                        "error": probe.get("error"),
                        "probed_at": now,
                        "run_id": run_id,
                    }
                )
            source_updates.append(update)
            probed += 1
            if probed == total_sources or probed % report_every == 0:
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory probing sources",
                    done=probed,
                    total=total_sources,
                    message=f"probed {probed}/{total_sources} source(s); valid={valid}",
                )

    await asyncio.gather(*[_run_probe(source) for source in sources])
    await _upsert_rows(ProviderDirectoryCapability, capability_rows)
    await _upsert_rows(ProviderDirectorySource, source_updates)
    return len(sources), valid, valid_source_ids


def _bundle_entries(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload or payload.get("resourceType") != "Bundle":
        return []
    entries = []
    for entry in payload.get("entry") or []:
        if isinstance(entry, dict) and isinstance(entry.get("resource"), dict):
            entries.append(entry)
    return entries


def _next_link(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    for link in payload.get("link") or []:
        if isinstance(link, dict) and link.get("relation") == "next":
            return _clean_text(link.get("url"))
    return None


def _bulk_export_start_url(source: dict[str, Any], resource_type: str) -> str | None:
    api_base = _canonical_base(source.get("api_base"))
    if not api_base:
        return None
    query = urllib.parse.urlencode(
        {
            "_type": resource_type,
            "_outputFormat": "application/fhir+ndjson",
        }
    )
    return f"{api_base}/$export?{query}"


def _bulk_export_enabled(value: Any) -> bool:
    if value is None:
        value = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT")
    return _bool_or_default(value, False)


def _retry_after_seconds(value: str | None) -> int | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        return max(0, min(int(text), 60))
    except ValueError:
        return None


def _bulk_export_output_urls(payload: dict[str, Any] | None, resource_type: str) -> list[str]:
    if not isinstance(payload, dict):
        return []
    urls: list[str] = []
    for item in payload.get("output") or []:
        if not isinstance(item, dict):
            continue
        if _clean_text(item.get("type")) != resource_type:
            continue
        url = _clean_text(item.get("url"))
        if url:
            urls.append(url)
    return urls


def _bulk_export_pre_stream_should_fallback(status_code: int | None, error: str | None) -> bool:
    if error:
        return True
    return status_code not in {200, 202}


def _bulk_json_headers(*, prefer_async: bool = False) -> dict[str, str]:
    headers = {
        "Accept": "application/fhir+json, application/json;q=0.9, */*;q=0.1",
        "User-Agent": USER_AGENT,
    }
    if prefer_async:
        headers["Prefer"] = "respond-async"
    return headers


def _bulk_ndjson_headers() -> dict[str, str]:
    return {
        "Accept": "application/fhir+ndjson, application/ndjson;q=0.9, text/plain;q=0.5, */*;q=0.1",
        "User-Agent": USER_AGENT,
    }


async def _bulk_http_get_json(
    session: aiohttp.ClientSession,
    source: dict[str, Any],
    url: str,
    *,
    timeout: int,
    prefer_async: bool = False,
) -> tuple[int | None, dict[str, str], dict[str, Any] | None, str | None]:
    options = _credential_request_options_for_source(source, url)
    headers = _bulk_json_headers(prefer_async=prefer_async)
    headers.update(options["headers"])
    fetch_url = _url_with_query_params(url, options["query_params"])
    try:
        async with session.get(
            fetch_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=False,
            ssl=_ssl_context(),
        ) as response:
            body = await response.read()
            header_map = {str(key).lower(): str(value) for key, value in response.headers.items()}
            return response.status, header_map, _decode_json_body(body), None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return None, {}, None, f"{type(exc).__name__}: {exc}"


async def _bulk_export_poll_outputs(
    session: aiohttp.ClientSession,
    source: dict[str, Any],
    status_url: str,
    *,
    resource_type: str,
    timeout: int,
) -> tuple[list[str] | None, str | None, int]:
    max_polls = _env_int("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT_MAX_POLLS", DEFAULT_BULK_EXPORT_MAX_POLLS)
    poll_seconds = _env_int(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT_POLL_SECONDS",
        DEFAULT_BULK_EXPORT_POLL_SECONDS,
    )
    polls = 0
    while polls < max(1, max_polls):
        polls += 1
        status_code, headers, payload, error = await _bulk_http_get_json(
            session,
            source,
            status_url,
            timeout=timeout,
        )
        if error:
            return None, error, polls
        if status_code == 202:
            await asyncio.sleep(_retry_after_seconds(headers.get("retry-after")) or poll_seconds)
            continue
        if status_code == 200:
            return _bulk_export_output_urls(payload, resource_type), None, polls
        return None, f"bulk_export_status_http_{status_code}", polls
    return None, "bulk_export_timeout", polls


async def _stream_bulk_export_output_rows(
    session: aiohttp.ClientSession,
    source: dict[str, Any],
    url: str,
    *,
    model: type,
    resource_type: str,
    per_resource_limit: int,
    timeout: int,
    run_id: str | None,
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None,
    row_batch_size: int,
    retain_rows: bool,
) -> tuple[list[dict[str, Any]], int, int, bool, str | None]:
    options = _credential_request_options_for_source(source, url)
    headers = _bulk_ndjson_headers()
    headers.update(options["headers"])
    fetch_url = _url_with_query_params(url, options["query_params"])
    rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    rows_fetched = 0
    rows_written = 0
    row_limit_reached = False

    async def flush_pending_rows() -> None:
        nonlocal rows_written, pending_rows
        if row_batch_handler and pending_rows:
            rows_written += await row_batch_handler(model, pending_rows)
            pending_rows = []

    def handle_line(raw_line: bytes) -> str | None:
        nonlocal rows_fetched, row_limit_reached
        line = raw_line.strip()
        if not line:
            return None
        try:
            resource = json.loads(line.decode("utf-8-sig", errors="replace"))
        except json.JSONDecodeError:
            return "invalid_ndjson"
        if not isinstance(resource, dict) or resource.get("resourceType") != resource_type:
            return None
        parsed = parse_fhir_resource(
            source["source_id"],
            resource,
            resource_url=url,
            run_id=run_id,
        )
        if not parsed:
            return None
        parsed_model, row = parsed
        if parsed_model is not model:
            return None
        rows_fetched += 1
        if retain_rows:
            rows.append(row)
        if row_batch_handler:
            pending_rows.append(row)
        if not _limit_allows_more(rows_fetched, per_resource_limit):
            row_limit_reached = True
        return None

    try:
        async with session.get(
            fetch_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout),
            allow_redirects=True,
            ssl=_ssl_context(),
        ) as response:
            if response.status != 200:
                return rows, rows_fetched, rows_written, row_limit_reached, f"bulk_export_output_http_{response.status}"
            buffer = b""
            async for chunk in response.content.iter_chunked(READ_CHUNK_BYTES):
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    error = handle_line(line)
                    if error:
                        return rows, rows_fetched, rows_written, row_limit_reached, error
                    if row_batch_handler and len(pending_rows) >= max(1, row_batch_size):
                        await flush_pending_rows()
                    if row_limit_reached:
                        await flush_pending_rows()
                        return rows, rows_fetched, rows_written, True, None
            if buffer.strip():
                error = handle_line(buffer)
                if error:
                    return rows, rows_fetched, rows_written, row_limit_reached, error
            await flush_pending_rows()
            return rows, rows_fetched, rows_written, row_limit_reached, None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return rows, rows_fetched, rows_written, row_limit_reached, f"{type(exc).__name__}: {exc}"


async def _fetch_bulk_export_resource_rows(
    source: dict[str, Any],
    resource_type: str,
    *,
    per_resource_limit: int,
    timeout: int,
    run_id: str | None,
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None = None,
    row_batch_size: int = DEFAULT_STREAM_BATCH_SIZE,
    retain_rows: bool = True,
) -> ResourceFetchResult | None:
    model = RESOURCE_MODELS_BY_TYPE.get(resource_type)
    url = _bulk_export_start_url(source, resource_type)
    if model is None or not url:
        return None
    async with aiohttp.ClientSession() as session:
        status_code, headers, _payload, error = await _bulk_http_get_json(
            session,
            source,
            url,
            timeout=timeout,
            prefer_async=True,
        )
        if _bulk_export_pre_stream_should_fallback(status_code, error):
            return None

        polls = 0
        if status_code == 200:
            output_urls = _bulk_export_output_urls(_payload, resource_type)
        else:
            status_url = _clean_text(headers.get("content-location") or headers.get("location"))
            if not status_url:
                return None
            output_urls, poll_error, polls = await _bulk_export_poll_outputs(
                session,
                source,
                urllib.parse.urljoin(url, status_url),
                resource_type=resource_type,
                timeout=timeout,
            )
            if poll_error is not None:
                return None
        rows: list[dict[str, Any]] = []
        rows_fetched = 0
        rows_written = 0
        row_limit_reached = False
        output_error: str | None = None
        for output_url in output_urls or []:
            batch_rows, fetched, written, limited, error = await _stream_bulk_export_output_rows(
                session,
                source,
                output_url,
                model=model,
                resource_type=resource_type,
                per_resource_limit=max(0, per_resource_limit - rows_fetched) if per_resource_limit > 0 else 0,
                timeout=timeout,
                run_id=run_id,
                row_batch_handler=row_batch_handler,
                row_batch_size=row_batch_size,
                retain_rows=retain_rows,
            )
            rows.extend(batch_rows)
            rows_fetched += fetched
            rows_written += written
            if error:
                output_error = error
                break
            if limited:
                row_limit_reached = True
                break
        if output_error and rows_fetched == 0 and rows_written == 0:
            return None
        complete = not output_error and not row_limit_reached
        return ResourceFetchResult(
            model=model,
            rows=rows,
            rows_fetched=rows_fetched,
            rows_written=rows_written,
            pages_fetched=(output_urls and len(output_urls) or 0) + polls,
            complete=complete,
            row_limit_reached=row_limit_reached,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=row_limit_reached,
            error=output_error,
            fetch_mode="bulk_export",
        )


async def _fetch_resource_rows(
    source: dict[str, Any],
    resource_type: str,
    *,
    per_resource_limit: int,
    page_limit: int,
    page_count: int,
    timeout: int,
    run_id: str | None,
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None = None,
    row_batch_size: int = DEFAULT_STREAM_BATCH_SIZE,
    retain_rows: bool = True,
    cancel_ctx: dict[str, Any] | None = None,
    cancel_task: dict[str, Any] | None = None,
    bulk_export: bool = False,
) -> ResourceFetchResult | None:
    model = RESOURCE_MODELS_BY_TYPE.get(resource_type)
    if model is None:
        return None
    if bulk_export:
        result = await _fetch_bulk_export_resource_rows(
            source,
            resource_type,
            per_resource_limit=per_resource_limit,
            timeout=timeout,
            run_id=run_id,
            row_batch_handler=row_batch_handler,
            row_batch_size=row_batch_size,
            retain_rows=retain_rows,
        )
        if result is not None:
            return result
    url = _resource_start_url(source, resource_type, page_count=page_count)
    if not url:
        return None
    rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    rows_fetched = 0
    rows_written = 0
    pages = 0
    seen_urls: set[str] = set()
    row_limit_reached = False
    page_limit_reached = False
    hard_page_limit_reached = False
    next_url_remaining = False
    error_message: str | None = None
    max_pages = _env_int("HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES", DEFAULT_FULL_REFRESH_MAX_PAGES)

    async def flush_pending_rows() -> None:
        nonlocal rows_written, pending_rows
        if not row_batch_handler or not pending_rows:
            return
        rows_written += await row_batch_handler(model, pending_rows)
        pending_rows = []

    while url and _limit_allows_more(rows_fetched, per_resource_limit):
        if cancel_ctx is not None:
            await raise_if_cancelled(cancel_ctx, cancel_task)
        if page_limit > 0 and pages >= page_limit:
            page_limit_reached = True
            next_url_remaining = True
            break
        if max_pages > 0 and pages >= max_pages:
            hard_page_limit_reached = True
            next_url_remaining = True
            break
        if url in seen_urls:
            error_message = "pagination loop detected"
            next_url_remaining = True
            break
        seen_urls.add(url)
        status_code, payload, error, _elapsed = await _fetch_source_json(source, url, timeout=timeout)
        if status_code != 200 or error:
            error_message = error or f"http_{status_code}"
            break
        if not payload or payload.get("resourceType") != "Bundle":
            error_message = "non_bundle_payload"
            break
        pages += 1
        entries = _bundle_entries(payload)
        for entry_index, entry in enumerate(entries):
            parsed = parse_fhir_resource(
                source["source_id"],
                entry["resource"],
                resource_url=_clean_text(entry.get("fullUrl")),
                run_id=run_id,
            )
            if not parsed:
                continue
            parsed_model, row = parsed
            if parsed_model is not model:
                continue
            rows_fetched += 1
            if retain_rows:
                rows.append(row)
            if row_batch_handler:
                pending_rows.append(row)
                if len(pending_rows) >= max(1, row_batch_size):
                    await flush_pending_rows()
            if not _limit_allows_more(rows_fetched, per_resource_limit):
                row_limit_reached = entry_index < len(entries) - 1
                break
        next_url = _next_link(payload)
        url = urllib.parse.urljoin(url, next_url) if next_url else None
        if not _limit_allows_more(rows_fetched, per_resource_limit) and url:
            row_limit_reached = True
            next_url_remaining = True
            break
    await flush_pending_rows()
    complete = not error_message and not row_limit_reached and not page_limit_reached and not hard_page_limit_reached and not url
    return ResourceFetchResult(
        model=model,
        rows=rows,
        rows_fetched=rows_fetched,
        rows_written=rows_written,
        pages_fetched=pages,
        complete=complete,
        row_limit_reached=row_limit_reached,
        page_limit_reached=page_limit_reached,
        hard_page_limit_reached=hard_page_limit_reached,
        next_url_remaining=next_url_remaining or bool(url),
        error=error_message,
    )


def _reference_resource_key(reference: str | None, expected_type: str) -> tuple[str, str] | None:
    reference = _clean_text(reference)
    if not reference or expected_type not in LINKED_RESOURCE_TYPES:
        return None
    parsed = urllib.parse.urlsplit(reference)
    path = parsed.path if parsed.scheme or parsed.netloc else reference
    parts = [urllib.parse.unquote(part) for part in path.strip("/").split("/") if part]
    for idx, part in enumerate(parts):
        if part == expected_type and idx + 1 < len(parts):
            resource_id = _clean_text(parts[idx + 1])
            return (expected_type, resource_id) if resource_id else None
    return None


def _linked_resource_candidate_urls(
    source: dict[str, Any],
    resource_type: str,
    resource_id: str,
    *,
    reference: str | None = None,
    reference_field: str | None = None,
) -> list[str]:
    urls: list[str] = []
    reference = _clean_text(reference)
    if reference:
        parsed_ref = urllib.parse.urlsplit(reference)
        if parsed_ref.scheme and parsed_ref.netloc:
            urls.append(urllib.parse.urlunsplit((parsed_ref.scheme, parsed_ref.netloc, parsed_ref.path, "", "")))
    api_base = _canonical_base(source.get("api_base"))
    endpoint = None
    if resource_type == "Organization" and reference_field == "network_refs":
        endpoint = _clean_text(source.get("endpoint_network"))
    if not endpoint:
        endpoint_field = RESOURCE_ENDPOINT_FIELDS.get(resource_type)
        endpoint = _clean_text(source.get(endpoint_field)) if endpoint_field else None
    if endpoint and not _is_placeholder_url(endpoint):
        encoded_id = urllib.parse.quote(resource_id, safe="")
        parsed_endpoint = urllib.parse.urlsplit(endpoint)
        endpoint_url = endpoint if parsed_endpoint.scheme and parsed_endpoint.netloc else None
        if not endpoint_url and api_base:
            endpoint_url = urllib.parse.urljoin(api_base.rstrip("/") + "/", endpoint)
        if endpoint_url:
            endpoint_with_count = _url_with_count(endpoint_url, 1)
            parsed_endpoint = urllib.parse.urlsplit(endpoint_with_count)
            endpoint_query = urllib.parse.parse_qsl(parsed_endpoint.query, keep_blank_values=True)
            if not any(key == "_id" for key, _value in endpoint_query):
                endpoint_query.append(("_id", resource_id))
            urls.append(
                urllib.parse.urlunsplit(
                    (
                        parsed_endpoint.scheme,
                        parsed_endpoint.netloc,
                        parsed_endpoint.path,
                        urllib.parse.urlencode(endpoint_query, doseq=True),
                        parsed_endpoint.fragment,
                    )
                )
            )
            endpoint_path_base = urllib.parse.urlunsplit(
                (
                    parsed_endpoint.scheme,
                    parsed_endpoint.netloc,
                    parsed_endpoint.path.rstrip("/") + "/",
                    "",
                    "",
                )
            )
            urls.append(urllib.parse.urljoin(endpoint_path_base, encoded_id))
    if api_base:
        encoded_id = urllib.parse.quote(resource_id, safe="")
        urls.append(f"{api_base}/{resource_type}/{encoded_id}")
        urls.append(f"{api_base}/{resource_type}?_id={encoded_id}&_count=1")
    deduped: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if url not in seen:
            deduped.append(url)
            seen.add(url)
    return deduped


def _row_reference_values(row: dict[str, Any], fields: tuple[str, ...]) -> list[str]:
    values: list[str] = []
    for field in fields:
        value = row.get(field)
        if isinstance(value, str):
            cleaned = _clean_text(value)
            if cleaned:
                values.append(cleaned)
        elif isinstance(value, list):
            values.extend(str(item) for item in value if _clean_text(item))
    return values


def _linked_resource_refs(rows_by_resource: dict[str, list[dict[str, Any]]]) -> list[tuple[str, str, str, str]]:
    refs: list[tuple[str, str, str, str]] = []
    seen: set[tuple[str, str]] = set()
    for source_resource_type, fields_by_target in LINKED_REFERENCE_FIELDS.items():
        for row in rows_by_resource.get(source_resource_type, []):
            for target_resource_type, fields in fields_by_target.items():
                for field in fields:
                    references = _row_reference_values(row, (field,))
                    for reference in references:
                        key = _reference_resource_key(reference, target_resource_type)
                        if not key or key in seen:
                            continue
                        seen.add(key)
                        refs.append((target_resource_type, key[1], reference, field))
    return refs


async def _fetch_linked_resource_row(
    source: dict[str, Any],
    resource_type: str,
    resource_id: str,
    *,
    reference: str | None,
    reference_field: str | None = None,
    timeout: int,
    run_id: str | None,
) -> tuple[type, dict[str, Any]] | None:
    for url in _linked_resource_candidate_urls(
        source,
        resource_type,
        resource_id,
        reference=reference,
        reference_field=reference_field,
    ):
        status_code, payload, error, _elapsed = await _fetch_source_json(source, url, timeout=timeout)
        if status_code != 200 or error or not payload:
            continue
        resource_payload = payload
        if payload.get("resourceType") == "Bundle":
            entries = _bundle_entries(payload)
            resource_payload = entries[0]["resource"] if entries else {}
        parsed = parse_fhir_resource(
            source["source_id"],
            resource_payload,
            resource_url=url,
            run_id=run_id,
        )
        if parsed and parsed[1].get("resource_id") == resource_id:
            return parsed
    return None


async def _import_linked_resource_rows(
    source: dict[str, Any],
    rows_by_resource: dict[str, list[dict[str, Any]]],
    *,
    per_source_limit: int,
    concurrency: int = 5,
    timeout: int,
    run_id: str | None,
) -> dict[str, int]:
    if per_source_limit <= 0:
        return {}
    existing = {
        (resource_type, row.get("resource_id"))
        for resource_type, rows in rows_by_resource.items()
        for row in rows
        if row.get("resource_id")
    }
    refs_to_fetch: list[tuple[str, str, str, str]] = []
    for resource_type, resource_id, reference, reference_field in _linked_resource_refs(rows_by_resource):
        if len(refs_to_fetch) >= per_source_limit:
            break
        if (resource_type, resource_id) in existing:
            continue
        refs_to_fetch.append((resource_type, resource_id, reference, reference_field))

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def _fetch_one(ref: tuple[str, str, str, str]) -> tuple[str, type, dict[str, Any]] | None:
        resource_type, resource_id, reference, reference_field = ref
        async with semaphore:
            result = await _fetch_linked_resource_row(
                source,
                resource_type,
                resource_id,
                reference=reference,
                reference_field=reference_field,
                timeout=timeout,
                run_id=run_id,
            )
            if not result:
                return None
            model, row = result
            return resource_type, model, row

    by_model: dict[type, list[dict[str, Any]]] = {}
    for result in await asyncio.gather(*[_fetch_one(ref) for ref in refs_to_fetch]):
        if not result:
            continue
        fetched_resource_type, model, row = result
        by_model.setdefault(model, []).append(row)
        existing.add((fetched_resource_type, row.get("resource_id")))

    counts: dict[str, int] = {}
    for model, rows in by_model.items():
        imported = await _upsert_rows(model, rows)
        if imported:
            counts[model.__name__.removeprefix("ProviderDirectory")] = imported
    return counts


def _empty_resource_stats() -> dict[str, Any]:
    return {
        "sources_attempted": 0,
        "sources_completed": 0,
        "sources_bounded": 0,
        "sources_failed": 0,
        "sources_empty": 0,
        "bulk_export_sources": 0,
        "pages_fetched": 0,
        "rows_fetched": 0,
    }


def _record_resource_fetch_stats(stats: dict[str, dict[str, Any]], resource_type: str, result: ResourceFetchResult) -> None:
    entry = stats.setdefault(resource_type, _empty_resource_stats())
    entry["sources_attempted"] += 1
    entry["pages_fetched"] += result.pages_fetched
    entry["rows_fetched"] += result.rows_fetched
    if result.complete:
        entry["sources_completed"] += 1
    if result.bounded:
        entry["sources_bounded"] += 1
    if result.error:
        entry["sources_failed"] += 1
    if result.complete and result.rows_fetched == 0:
        entry["sources_empty"] += 1
    if result.fetch_mode == "bulk_export":
        entry["bulk_export_sources"] += 1


def _resource_fetch_diagnostic(result: ResourceFetchResult, *, rows_written_per_source: int) -> dict[str, Any]:
    return {
        "complete": result.complete,
        "bounded": result.bounded,
        "error": result.error,
        "fetch_mode": result.fetch_mode,
        "pages_fetched": result.pages_fetched,
        "rows_fetched": result.rows_fetched,
        "rows_written": rows_written_per_source,
        "row_limit_reached": result.row_limit_reached,
        "page_limit_reached": result.page_limit_reached,
        "hard_page_limit_reached": result.hard_page_limit_reached,
        "next_url_remaining": result.next_url_remaining,
    }


async def _update_source_resource_import_metadata(
    source_ids: list[str],
    *,
    run_id: str | None,
    diagnostics: dict[str, dict[str, Any]],
) -> None:
    if not run_id or not source_ids or not diagnostics:
        return
    payload = {
        "run_id": run_id,
        "observed_at": _now().isoformat(timespec="seconds") + "Z",
        "resources": diagnostics,
    }
    payload_json = json.dumps(payload, sort_keys=True, default=str)
    for source_id in source_ids:
        await db.status(
            f"""
            UPDATE {_qt(_schema(), "provider_directory_source")}
               SET metadata_json = COALESCE(metadata_json::jsonb, '{{}}'::jsonb)
                                   || jsonb_build_object('last_resource_import', CAST(:payload AS jsonb)),
                   updated_at = now()
             WHERE source_id = :source_id;
            """,
            payload=payload_json,
            source_id=source_id,
        )


async def _upsert_resource_rows(
    model: type,
    rows: list[dict[str, Any]],
    *,
    run_id: str | None,
    track_seen: bool,
    seen_table: str | None = None,
    canonical_api_base: str | None = None,
    source_ids: list[str] | None = None,
) -> int:
    if not rows:
        return 0
    if canonical_api_base and source_ids:
        canonical_rows = _canonical_resource_rows(
            model,
            rows,
            canonical_api_base=canonical_api_base,
            run_id=run_id,
        )
        if canonical_rows:
            await _upsert_rows(
                ProviderDirectoryCanonicalResource,
                canonical_rows,
                skip_unchanged=track_seen and bool(run_id),
            )
        edge_rows = _source_resource_edge_rows(
            model,
            rows,
            canonical_api_base=canonical_api_base,
            source_ids=source_ids,
            run_id=run_id,
        )
        if edge_rows:
            await _upsert_rows(
                ProviderDirectorySourceResource,
                edge_rows,
                skip_unchanged=track_seen and bool(run_id),
            )
    if track_seen:
        await _mark_resource_rows_seen(model, rows, run_id, seen_table=seen_table)
    return await _upsert_rows(model, rows, skip_unchanged=track_seen and bool(run_id))


async def _delete_stale_resource_rows(
    model: type,
    source_id: str,
    run_id: str | None,
    *,
    use_seen_table: bool = False,
    seen_table: str | None = None,
) -> int:
    if not run_id:
        return 0
    if use_seen_table or seen_table:
        resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
        if resource_type:
            seen_ref = _qt(_schema(), seen_table or PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
            run_filter = "AND seen.run_id = :run_id" if not seen_table else ""
            return int(
                await db.status(
                    f"""
                    DELETE FROM {_qt(_schema(), model.__tablename__)} AS resource
                     WHERE resource.source_id = :source_id
                       AND NOT EXISTS (
                            SELECT 1
                              FROM {seen_ref} AS seen
                             WHERE seen.resource_type = :resource_type
                               {run_filter}
                               AND seen.source_id = resource.source_id
                               AND seen.resource_id = resource.resource_id
                       );
                    """,
                    source_id=source_id,
                    run_id=run_id,
                    resource_type=resource_type,
                )
                or 0
            )
    return int(
        await db.status(
            f"""
            DELETE FROM {_qt(_schema(), model.__tablename__)}
             WHERE source_id = :source_id
               AND last_seen_run_id IS DISTINCT FROM :run_id;
            """,
            source_id=source_id,
            run_id=run_id,
        )
        or 0
    )


def _selected_resources(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_RESOURCES)
    selected = [item.strip() for item in raw.split(",") if item.strip()]
    allowed = set(DEFAULT_RESOURCES)
    unknown = sorted(set(selected) - allowed)
    if unknown:
        raise ValueError(f"Unsupported Provider Directory FHIR resources: {', '.join(unknown)}")
    return selected


def _bool_or_default(value: Any, default: bool) -> bool:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(value)


def _int_or_default(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


async def _mark_provider_directory_progress(
    run_id: str | None,
    *,
    phase: str,
    done: int,
    total: int,
    message: str,
    details: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
) -> None:
    if not run_id:
        return
    total = max(total, 1)
    done = max(0, min(done, total))
    progress = {
        "unit": "steps",
        "done": done,
        "total": total,
        "pct": round((done / total) * 100, 2),
        "phase": phase,
        "message": message,
    }
    if isinstance(details, dict) and details:
        progress["detail"] = details
    try:
        await mark_control_run(
            run_id,
            status="running",
            phase_detail=phase,
            progress_message=message,
            progress=progress,
            metrics=metrics,
        )
    except Exception as exc:  # pylint: disable=broad-exception-caught
        print(f"Provider Directory progress update failed: {type(exc).__name__}: {exc}")


def _limit_allows_more(current: int, limit: int) -> bool:
    return limit <= 0 or current < limit


def _resource_import_group_key(source: dict[str, Any]) -> tuple[str, str]:
    api_base = _canonical_base(source.get("api_base"))
    if not api_base:
        return source.get("source_id") or "", ""
    credential_descriptor = _credential_request_options_for_source(source, f"{api_base}/metadata")["descriptor"]
    return api_base, json.dumps(credential_descriptor or {}, sort_keys=True, default=str)


def _group_resource_import_sources(
    sources: list[dict[str, Any]],
    *,
    linked_resource_limit: int,
) -> list[list[dict[str, Any]]]:
    if linked_resource_limit > 0:
        return [[source] for source in sources]
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    ordered_groups: list[list[dict[str, Any]]] = []
    for source in sources:
        key = _resource_import_group_key(source)
        if key not in groups:
            groups[key] = []
            ordered_groups.append(groups[key])
        groups[key].append(source)
    return ordered_groups


def _copy_rows_for_source_ids(rows: list[dict[str, Any]], source_ids: list[str]) -> list[dict[str, Any]]:
    if len(source_ids) <= 1:
        return rows
    copied: list[dict[str, Any]] = []
    for row in rows:
        for source_id in source_ids:
            item = dict(row)
            item["source_id"] = source_id
            copied.append(item)
    return copied


async def _fetch_alohr_graphql_page(
    query: str,
    root_key: str,
    item_key: str,
    *,
    next_token: str | None,
    timeout: int,
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    payload = {
        "query": query,
        "variables": {"criteria": {}, "nextToken": next_token},
    }
    status_code, data, error, _elapsed = await asyncio.to_thread(
        _post_json_sync,
        ALOHR_GRAPHQL_URL,
        payload,
        timeout=timeout,
        extra_headers={"tenantId": ALOHR_TENANT_ID},
    )
    if error:
        return [], None, error
    if status_code != 200:
        return [], None, f"http_{status_code}"
    if not isinstance(data, dict):
        return [], None, "invalid_json"
    errors = data.get("errors")
    if errors:
        return [], None, f"graphql_errors:{json.dumps(errors, default=str)[:500]}"
    result = data.get("data", {}).get(root_key) if isinstance(data.get("data"), dict) else None
    if not isinstance(result, dict):
        return [], None, f"missing_graphql_result:{root_key}"
    items = [item for item in result.get(item_key) or [] if isinstance(item, dict)]
    return items, _clean_text(result.get("nextToken")), None


def _alohr_resource_diagnostic(
    *,
    complete: bool,
    error: str | None,
    pages_fetched: int,
    rows_fetched: int,
    rows_written: int,
    row_limit_reached: bool,
    page_limit_reached: bool,
) -> dict[str, Any]:
    return {
        "complete": complete,
        "bounded": row_limit_reached or page_limit_reached,
        "error": error,
        "pages_fetched": pages_fetched,
        "rows_fetched": rows_fetched,
        "rows_written": rows_written,
        "row_limit_reached": row_limit_reached,
        "page_limit_reached": page_limit_reached,
        "hard_page_limit_reached": False,
        "next_url_remaining": None,
    }


async def _import_alohr_graphql_source_group(
    source_group: list[dict[str, Any]],
    *,
    resources: list[str],
    per_resource_limit: int,
    page_limit: int,
    timeout: int,
    run_id: str | None,
    stale_cleanup: bool,
    seen_table: str | None = None,
) -> tuple[
    list[str],
    dict[str, dict[str, Any]],
    dict[str, int],
    dict[str, int],
    dict[str, dict[str, Any]],
    dict[str, int],
    dict[str, list[str]],
]:
    source_ids = [source["source_id"] for source in source_group]
    rows_by_model: dict[type, list[dict[str, Any]]] = {}
    source_counts: dict[str, int] = {resource: 0 for resource in resources}
    source_resource_stats: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    stale_counts: dict[str, int] = {}
    stale_ready_source_ids: dict[str, list[str]] = {}
    selected = set(resources)

    async def fetch_stream(
        query: str,
        root_key: str,
        item_key: str,
        append_item: Callable[[dict[type, list[dict[str, Any]]], str, dict[str, Any]], None],
        resource_types: tuple[str, ...],
    ) -> None:
        if not (selected & set(resource_types)):
            return
        next_token: str | None = None
        pages_fetched = 0
        rows_fetched = 0
        error: str | None = None
        row_limit_reached = False
        page_limit_reached = False
        while True:
            if page_limit > 0 and pages_fetched >= page_limit:
                page_limit_reached = True
                break
            items, next_token, error = await _fetch_alohr_graphql_page(
                query,
                root_key,
                item_key,
                next_token=next_token,
                timeout=timeout,
            )
            if error:
                break
            pages_fetched += 1
            for item in items:
                if per_resource_limit > 0 and rows_fetched >= per_resource_limit:
                    row_limit_reached = True
                    break
                rows_fetched += 1
                for source_id in source_ids:
                    append_item(rows_by_model, source_id, item)
            if row_limit_reached or not next_token:
                break
        complete = error is None and not row_limit_reached and not page_limit_reached and not next_token
        for resource_type in resource_types:
            source_resource_stats[resource_type] = {
                "sources_attempted": len(source_ids),
                "sources_completed": len(source_ids) if complete else 0,
                "sources_bounded": len(source_ids) if (row_limit_reached or page_limit_reached) else 0,
                "sources_failed": len(source_ids) if error else 0,
                "sources_empty": len(source_ids) if complete and rows_fetched == 0 else 0,
                "pages_fetched": pages_fetched * len(source_ids),
                "rows_fetched": rows_fetched * len(source_ids),
            }
            diagnostics[resource_type] = _alohr_resource_diagnostic(
                complete=complete,
                error=error,
                pages_fetched=pages_fetched,
                rows_fetched=rows_fetched,
                rows_written=0,
                row_limit_reached=row_limit_reached,
                page_limit_reached=page_limit_reached,
            )

    def append_provider(rows: dict[type, list[dict[str, Any]]], source_id: str, item: dict[str, Any]) -> None:
        _append_alohr_provider_rows(rows, source_id, item, run_id=run_id)

    def append_organization(rows: dict[type, list[dict[str, Any]]], source_id: str, item: dict[str, Any]) -> None:
        _append_alohr_organization_rows(rows, source_id, item, run_id=run_id)

    await fetch_stream(
        ALOHR_PROVIDER_QUERY,
        "providers",
        "providers",
        append_provider,
        ("Practitioner", "Location", "PractitionerRole"),
    )
    await fetch_stream(
        ALOHR_ORGANIZATION_QUERY,
        "providerOrgs",
        "providerOrganizations",
        append_organization,
        ("Organization", "Location", "OrganizationAffiliation"),
    )

    for model, rows in rows_by_model.items():
        resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
        if resource_type not in selected:
            continue
        written = await _upsert_resource_rows(
            model,
            rows,
            run_id=run_id,
            track_seen=stale_cleanup,
            seen_table=seen_table,
            canonical_api_base=source_group[0].get("canonical_api_base") or source_group[0].get("api_base"),
            source_ids=source_ids,
        )
        source_counts[resource_type] = source_counts.get(resource_type, 0) + written
        if resource_type in diagnostics:
            diagnostics[resource_type]["rows_written"] = written // max(1, len(source_ids))
        if stale_cleanup and diagnostics.get(resource_type, {}).get("complete"):
            if seen_table:
                stale_ready_source_ids[resource_type] = list(source_ids)
            else:
                for source_id in source_ids:
                    stale_deleted = await _delete_stale_resource_rows(
                        model,
                        source_id,
                        run_id,
                        use_seen_table=True,
                    )
                    if stale_deleted:
                        stale_counts[resource_type] = stale_counts.get(resource_type, 0) + stale_deleted

    return source_ids, diagnostics, source_counts, {}, source_resource_stats, stale_counts, stale_ready_source_ids


async def _import_resources(
    sources: list[dict[str, Any]],
    *,
    resources: list[str],
    per_resource_limit: int,
    page_limit: int,
    page_count: int,
    timeout: int,
    run_id: str | None,
    linked_resource_limit: int = 0,
    linked_resource_concurrency: int = 5,
    linked_counts: dict[str, int] | None = None,
    resource_fetch_stats: dict[str, dict[str, Any]] | None = None,
    stale_counts: dict[str, int] | None = None,
    stale_cleanup: bool = False,
    stream_batch_size: int = 0,
    source_concurrency: int = 1,
    bulk_export: bool = False,
    cancel_ctx: dict[str, Any] | None = None,
    cancel_task: dict[str, Any] | None = None,
    progress_callback: Callable[[int, int, dict[str, int], dict[str, Any] | None], Awaitable[None]] | None = None,
    preserve_seen_stage: bool = False,
) -> dict[str, int]:
    counts: dict[str, int] = {resource: 0 for resource in resources}
    semaphore = asyncio.Semaphore(max(1, source_concurrency))
    source_groups = _group_resource_import_sources(sources, linked_resource_limit=linked_resource_limit)
    total_groups = len(source_groups)
    completed_groups = 0
    report_every = max(1, total_groups // 20)
    progress_lock = asyncio.Lock()
    active_partial_counts: dict[int, dict[str, int]] = {}
    active_group_details: dict[int, dict[str, Any]] = {}
    next_partial_progress_at = 0.0

    def merge_counts(target: dict[str, int], source_counts: dict[str, int]) -> None:
        for key, value in source_counts.items():
            target[key] = target.get(key, 0) + value

    def merge_resource_stats(target: dict[str, dict[str, Any]], source_stats: dict[str, dict[str, Any]]) -> None:
        for resource_type, entry in source_stats.items():
            target_entry = target.setdefault(resource_type, _empty_resource_stats())
            for key, value in entry.items():
                target_entry[key] = target_entry.get(key, 0) + value

    def progress_counts_snapshot() -> dict[str, int]:
        snapshot = dict(counts)
        for partial_counts in active_partial_counts.values():
            merge_counts(snapshot, partial_counts)
        return snapshot

    def active_group_snapshot() -> list[dict[str, Any]]:
        now = time.monotonic()
        groups: list[dict[str, Any]] = []
        for detail in active_group_details.values():
            group = {
                key: value
                for key, value in detail.items()
                if key not in {"started_monotonic", "resource_started_monotonic"}
            }
            group["elapsed_seconds"] = int(now - detail["started_monotonic"])
            if detail.get("resource_started_monotonic") is not None:
                group["resource_elapsed_seconds"] = int(now - detail["resource_started_monotonic"])
            groups.append(group)
        return sorted(
            groups,
            key=lambda item: (
                str(item.get("sample_org_name") or ""),
                str(item.get("sample_plan_name") or ""),
                str(item.get("sample_source_id") or ""),
            ),
        )

    async def report_progress(force: bool = False) -> None:
        nonlocal next_partial_progress_at
        if not progress_callback:
            return
        snapshot: dict[str, int] | None = None
        details: dict[str, Any] | None = None
        async with progress_lock:
            now = time.monotonic()
            if force or now >= next_partial_progress_at:
                if not force:
                    next_partial_progress_at = now + 15.0
                snapshot = progress_counts_snapshot()
                details = {"active_source_groups": active_group_snapshot()}
        if snapshot is not None:
            await progress_callback(completed_groups, total_groups, snapshot, details)

    async def maybe_report_partial_progress(
        group_key: int,
        resource_type: str,
        written: int,
    ) -> None:
        if not progress_callback or written <= 0:
            return
        async with progress_lock:
            group_counts = active_partial_counts.setdefault(group_key, {})
            group_counts[resource_type] = group_counts.get(resource_type, 0) + written
        await report_progress()

    async def clear_partial_progress(group_key: int) -> None:
        async with progress_lock:
            active_partial_counts.pop(group_key, None)
            active_group_details.pop(group_key, None)

    async def import_one_group(
        source_group: list[dict[str, Any]],
    ) -> tuple[
        list[str],
        dict[str, dict[str, Any]],
        dict[str, int],
        dict[str, int],
        dict[str, dict[str, Any]],
        dict[str, int],
        dict[str, list[str]],
    ]:
        if cancel_ctx is not None:
            await raise_if_cancelled(cancel_ctx, cancel_task)
        group_key = id(source_group)
        source = source_group[0]
        source_ids = [item["source_id"] for item in source_group]
        async with progress_lock:
            active_group_details[group_key] = {
                "source_ids": source_ids[:10],
                "source_count": len(source_ids),
                "sample_source_id": source.get("source_id"),
                "sample_org_name": source.get("org_name"),
                "sample_plan_name": source.get("plan_name"),
                "api_base": source.get("canonical_api_base") or source.get("api_base"),
                "current_resource": None,
                "started_at": _now().isoformat(timespec="seconds") + "Z",
                "started_monotonic": time.monotonic(),
                "resource_started_at": None,
                "resource_started_monotonic": None,
            }
        await report_progress(force=True)
        source_counts: dict[str, int] = {resource: 0 for resource in resources}
        source_linked_counts: dict[str, int] = {}
        source_resource_stats: dict[str, dict[str, Any]] = {}
        source_resource_diagnostics: dict[str, dict[str, Any]] = {}
        source_stale_counts: dict[str, int] = {}
        source_stale_ready_source_ids: dict[str, list[str]] = {}
        rows_by_resource: dict[str, list[dict[str, Any]]] = {}
        if _alohr_source_uses_graphql_connector(source):
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = "ALOHR GraphQL"
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)
            return await _import_alohr_graphql_source_group(
                source_group,
                resources=resources,
                per_resource_limit=per_resource_limit,
                page_limit=page_limit,
                timeout=timeout,
                run_id=run_id,
                stale_cleanup=stale_cleanup,
                seen_table=seen_stage_table,
            )
        for resource_type in resources:
            if cancel_ctx is not None:
                await raise_if_cancelled(cancel_ctx, cancel_task)
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = resource_type
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)
            use_streaming = stream_batch_size > 0

            async def row_batch_handler(model: type, rows: list[dict[str, Any]]) -> int:
                written = await _upsert_resource_rows(
                    model,
                    _copy_rows_for_source_ids(rows, source_ids),
                    run_id=run_id,
                    track_seen=stale_cleanup,
                    seen_table=seen_stage_table,
                    canonical_api_base=source.get("canonical_api_base") or source.get("api_base"),
                    source_ids=source_ids,
                )
                await maybe_report_partial_progress(group_key, resource_type, written)
                return written

            result = await _fetch_resource_rows(
                source,
                resource_type,
                per_resource_limit=per_resource_limit,
                page_limit=page_limit,
                page_count=page_count,
                timeout=timeout,
                run_id=run_id,
                row_batch_handler=row_batch_handler if use_streaming else None,
                row_batch_size=stream_batch_size,
                retain_rows=linked_resource_limit > 0 or not use_streaming,
                cancel_ctx=cancel_ctx,
                cancel_task=cancel_task,
                bulk_export=bulk_export,
            )
            if not result:
                continue
            for _source_id in source_ids:
                _record_resource_fetch_stats(source_resource_stats, resource_type, result)
            rows_by_resource[resource_type] = result.rows
            written_total = (
                result.rows_written
                if use_streaming
                else await _upsert_resource_rows(
                    result.model,
                    _copy_rows_for_source_ids(result.rows, source_ids),
                    run_id=run_id,
                    track_seen=stale_cleanup,
                    seen_table=seen_stage_table,
                    canonical_api_base=source.get("canonical_api_base") or source.get("api_base"),
                    source_ids=source_ids,
                )
            )
            source_counts[resource_type] += written_total
            source_resource_diagnostics[resource_type] = _resource_fetch_diagnostic(
                result,
                rows_written_per_source=written_total // max(1, len(source_ids)),
            )
            if stale_cleanup and result.complete:
                if seen_stage_table:
                    source_stale_ready_source_ids[resource_type] = list(source_ids)
                else:
                    for source_id in source_ids:
                        stale_deleted = await _delete_stale_resource_rows(
                            result.model,
                            source_id,
                            run_id,
                            use_seen_table=True,
                        )
                        if stale_deleted:
                            source_stale_counts[resource_type] = source_stale_counts.get(resource_type, 0) + stale_deleted
        if linked_resource_limit > 0 and rows_by_resource:
            source_linked_counts = await _import_linked_resource_rows(
                source,
                rows_by_resource,
                per_source_limit=linked_resource_limit,
                concurrency=linked_resource_concurrency,
                timeout=timeout,
                run_id=run_id,
            )
        return (
            source_ids,
            source_resource_diagnostics,
            source_counts,
            source_linked_counts,
            source_resource_stats,
            source_stale_counts,
            source_stale_ready_source_ids,
        )

    seen_stage_table = (
        await _ensure_provider_directory_import_seen_stage_table(run_id)
        if stale_cleanup and run_id and _seen_stage_enabled()
        else None
    )
    stale_ready_source_ids_by_resource: dict[str, set[str]] = {}

    async def run_with_limit(
        source_group: list[dict[str, Any]],
    ) -> tuple[
        list[str],
        dict[str, dict[str, Any]],
        dict[str, int],
        dict[str, int],
        dict[str, dict[str, Any]],
        dict[str, int],
        dict[str, list[str]],
    ]:
        async with semaphore:
            try:
                return await import_one_group(source_group)
            finally:
                await clear_partial_progress(id(source_group))

    tasks: list[asyncio.Task] = []
    try:
        tasks = [asyncio.create_task(run_with_limit(source_group)) for source_group in source_groups]
        for task in asyncio.as_completed(tasks):
            (
                source_ids,
                source_resource_diagnostics,
                source_counts,
                source_linked_counts,
                source_resource_stats,
                source_stale_counts,
                source_stale_ready_source_ids,
            ) = await task
            await _update_source_resource_import_metadata(
                source_ids,
                run_id=run_id,
                diagnostics=source_resource_diagnostics,
            )
            merge_counts(counts, source_counts)
            if linked_counts is not None:
                merge_counts(linked_counts, source_linked_counts)
            if resource_fetch_stats is not None:
                merge_resource_stats(resource_fetch_stats, source_resource_stats)
            if stale_counts is not None:
                merge_counts(stale_counts, source_stale_counts)
            for resource_type, ready_source_ids in source_stale_ready_source_ids.items():
                stale_ready_source_ids_by_resource.setdefault(resource_type, set()).update(ready_source_ids)
            completed_groups += 1
            if progress_callback is not None and (
                completed_groups == total_groups or completed_groups % report_every == 0
            ):
                await report_progress(force=True)

        if seen_stage_table and stale_ready_source_ids_by_resource:
            await _prepare_provider_directory_import_seen_stage_lookup(seen_stage_table)
            for resource_type, ready_source_ids in stale_ready_source_ids_by_resource.items():
                model = RESOURCE_MODELS_BY_TYPE.get(resource_type)
                if model is None:
                    continue
                for source_id in sorted(ready_source_ids):
                    stale_deleted = await _delete_stale_resource_rows(
                        model,
                        source_id,
                        run_id,
                        seen_table=seen_stage_table,
                    )
                    if stale_deleted and stale_counts is not None:
                        stale_counts[resource_type] = stale_counts.get(resource_type, 0) + stale_deleted
        return counts
    except BaseException:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        raise
    finally:
        if seen_stage_table and not preserve_seen_stage:
            await _drop_provider_directory_import_seen_stage_table(seen_stage_table)


async def process_data(ctx: dict[str, Any], task: dict[str, Any] | None = None) -> dict[str, Any]:
    task = task or {}
    await raise_if_cancelled(ctx, task)
    ctx.setdefault("context", {})
    test_mode = bool(task.get("test") or task.get("test_mode") or ctx["context"].get("test_mode"))
    ctx["context"]["test_mode"] = test_mode

    await ensure_database(test_mode)
    await _ensure_provider_directory_tables()

    run_id = _clean_text(task.get("run_id")) or _clean_text(ctx.get("control_run_id"))
    limit = int(task.get("limit") or 0) or None
    source_query = _clean_text(task.get("source_query"))
    timeout = int(task.get("timeout") or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_TIMEOUT", "15"))
    concurrency = int(task.get("concurrency") or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_CONCURRENCY", "5"))
    seed_only = bool(task.get("seed_only", False))
    probe = bool(task.get("probe", True))
    import_resources = bool(task.get("import_resources", False))
    canonical_backfill_only = bool(task.get("canonical_backfill_only", False))
    contact_backfill_only = bool(task.get("contact_backfill_only", False))
    open_only = bool(task.get("open_only", True))
    include_auth_required = bool(task.get("include_auth_required", False))
    full_refresh = bool(task.get("full_refresh", False))
    default_resource_limit = 1 if test_mode else (0 if full_refresh else 25)
    default_page_limit = 1 if test_mode else (0 if full_refresh else 3)
    default_page_count = 25 if test_mode else (100 if full_refresh else 25)
    resource_limit = _int_or_default(task.get("resource_limit"), default_resource_limit)
    page_limit = _int_or_default(task.get("page_limit"), default_page_limit)
    page_count = _int_or_default(task.get("page_count"), default_page_count)
    linked_resource_limit = _int_or_default(
        task.get("linked_resource_limit"),
        _env_int("HLTHPRT_PROVIDER_DIRECTORY_LINKED_RESOURCE_LIMIT", resource_limit),
    )
    stream_batch_size = _int_or_default(
        task.get("stream_batch_size"),
        0 if test_mode else _env_int("HLTHPRT_PROVIDER_DIRECTORY_STREAM_BATCH_SIZE", DEFAULT_STREAM_BATCH_SIZE),
    )
    bulk_export = _bulk_export_enabled(task.get("bulk_export"))
    source_concurrency = _int_or_default(
        task.get("source_concurrency"),
        1 if test_mode else _env_int("HLTHPRT_PROVIDER_DIRECTORY_SOURCE_CONCURRENCY", 1),
    )
    stale_cleanup_raw = task.get("stale_cleanup")
    stale_cleanup = _bool_or_default(stale_cleanup_raw, not test_mode and import_resources)
    resources = _selected_resources(_clean_text(task.get("resources")))
    publish_artifacts = _bool_or_default(
        task.get("publish_artifacts"),
        set(resources) == set(DEFAULT_RESOURCES),
    )
    seen_stage_table_for_publish = (
        _provider_directory_import_seen_stage_table_name(run_id)
        if publish_artifacts and stale_cleanup and run_id and _seen_stage_enabled()
        else None
    )
    if canonical_backfill_only:
        metrics = await backfill_provider_directory_canonical_resources(
            resources=_clean_text(task.get("resources")),
        )
        ctx["context"]["audit"] = metrics
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        return metrics
    if contact_backfill_only:
        metrics = await backfill_provider_directory_location_contacts()
        ctx["context"]["audit"] = metrics
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        return metrics
    await _clear_resource_rows_seen(run_id)

    seed_rows: list[dict[str, Any]]
    tmpdir = None
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory resolving source catalog",
        done=0,
        total=1,
        message="resolving Provider Directory source catalog",
    )
    if test_mode and not task.get("seed_db_path") and not task.get("seed_db_url"):
        seed_rows = [
            {
                "id": "test-cigna",
                "org_name": "Cigna",
                "plan_name": "test open fhir",
                "api_base": "https://fhir.cigna.com/ProviderDirectory/v1",
                "auth_type": "open",
                "last_validated_status": "valid",
                "data_quality_flag": "VERIFIED_REAL",
                "source": "test_fixture",
            }
        ][: limit or 1]
    else:
        seed_path, tmpdir = _resolve_seed_db(_clean_text(task.get("seed_db_path")), _clean_text(task.get("seed_db_url")))
        if seed_path is None:
            raise RuntimeError("Provider Directory seed database could not be resolved.")
        seed_rows = _seed_rows_from_sqlite(seed_path, limit=limit, source_query=source_query)

    try:
        source_rows = [_source_row_from_seed(row) for row in seed_rows]
        await _upsert_rows(ProviderDirectorySource, source_rows)
        metrics: dict[str, Any] = {
            "sources_seeded": len(source_rows),
            "sources_probed": 0,
            "valid_capability_sources": 0,
            "resource_rows": {},
            "full_refresh": full_refresh,
            "resource_limit": resource_limit,
            "page_limit": page_limit,
            "page_count": page_count,
            "stream_batch_size": stream_batch_size,
            "bulk_export": bulk_export,
            "source_concurrency": source_concurrency,
            "stale_cleanup": stale_cleanup,
            "publish_artifacts": publish_artifacts,
        }
        await _mark_provider_directory_progress(
            run_id,
            phase="provider-directory sources seeded",
            done=1,
            total=1,
            message=f"seeded {len(source_rows)} Provider Directory source(s)",
            metrics=metrics,
        )
        valid_source_ids: set[str] | None = None
        if not seed_only and probe and source_rows:
            probed, valid, valid_source_ids = await _probe_sources(
                source_rows,
                timeout=timeout,
                concurrency=concurrency,
                run_id=run_id,
            )
            metrics["sources_probed"] = probed
            metrics["valid_capability_sources"] = valid
        if not seed_only and import_resources and source_rows:
            importable = []
            for source in source_rows:
                if not _canonical_base(source.get("api_base")):
                    continue
                if valid_source_ids is not None and source["source_id"] not in valid_source_ids:
                    continue
                auth_type = (_clean_text(source.get("auth_type")) or "").lower()
                validation = (_clean_text(source.get("last_validated_status")) or "").lower()
                live_probe_valid = valid_source_ids is not None and source["source_id"] in valid_source_ids
                if open_only and auth_type not in {"open", "none", ""} and not live_probe_valid:
                    continue
                if valid_source_ids is None and validation not in {"", "valid"}:
                    continue
                if valid_source_ids is None and not include_auth_required and validation == "auth_required":
                    continue
                importable.append(source)
            source_import_groups = _group_resource_import_sources(
                importable,
                linked_resource_limit=linked_resource_limit,
            )
            metrics["source_import_groups_attempted"] = len(source_import_groups)
            metrics["source_import_duplicate_sources_collapsed"] = len(importable) - len(source_import_groups)

            async def resource_progress(
                done: int,
                total: int,
                counts: dict[str, int],
                details: dict[str, Any] | None = None,
            ) -> None:
                metrics["resource_rows"] = counts
                active_source_groups = (
                    details.get("active_source_groups", [])
                    if isinstance(details, dict)
                    else []
                )
                metrics["active_source_groups"] = active_source_groups
                active_preview = ", ".join(
                    (
                        f"{group.get('sample_org_name') or group.get('sample_source_id')}:"
                        f"{group.get('current_resource') or 'starting'}"
                    )
                    for group in active_source_groups[:3]
                )
                active_suffix = f"; active={active_preview}" if active_preview else ""
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory importing resources",
                    done=done,
                    total=total,
                    message=(
                        f"imported resources for {done}/{total} source group(s); "
                        f"rows={sum(counts.values())}"
                        f"{active_suffix}"
                    ),
                    details={"active_source_groups": active_source_groups},
                    metrics=metrics,
                )

            await _mark_provider_directory_progress(
                run_id,
                phase="provider-directory importing resources",
                done=0,
                total=max(len(source_import_groups), 1),
                message=f"importing resources from {len(source_import_groups)} source group(s)",
                metrics=metrics,
            )
            metrics["resource_rows"] = await _import_resources(
                importable,
                resources=resources,
                per_resource_limit=resource_limit,
                page_limit=page_limit,
                page_count=page_count,
                timeout=timeout,
                run_id=run_id,
                linked_resource_limit=linked_resource_limit,
                linked_resource_concurrency=concurrency,
                linked_counts=metrics.setdefault("linked_resource_rows", {}),
                resource_fetch_stats=metrics.setdefault("resource_fetch_stats", {}),
                stale_counts=metrics.setdefault("stale_resource_rows_deleted", {}),
                stale_cleanup=stale_cleanup,
                stream_batch_size=stream_batch_size,
                source_concurrency=source_concurrency,
                bulk_export=bulk_export,
                cancel_ctx=ctx,
                cancel_task={**task, "run_id": run_id},
                progress_callback=resource_progress,
                preserve_seen_stage=bool(seen_stage_table_for_publish),
            )
            metrics["sources_import_attempted"] = len(importable)
            if publish_artifacts:
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory publishing artifacts",
                    done=0,
                    total=3,
                    message="publishing Provider Directory address artifacts",
                    metrics=metrics,
                )
                if seen_stage_table_for_publish:
                    await _prepare_provider_directory_import_seen_stage_lookup(seen_stage_table_for_publish)
                metrics["location_address_keys_stamped"] = await publish_provider_directory_location_address_keys(
                    run_id=run_id,
                    seen_table=seen_stage_table_for_publish,
                )
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory publishing artifacts",
                    done=1,
                    total=3,
                    message=(
                        "stamped Provider Directory location address keys; "
                        f"rows={metrics['location_address_keys_stamped']}"
                    ),
                    metrics=metrics,
                )
                metrics["location_archive"] = await publish_provider_directory_location_archive(run_id=run_id)
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory publishing artifacts",
                    done=2,
                    total=3,
                    message=(
                        "published Provider Directory locations to address archive; "
                        f"inserted={metrics['location_archive'].get('inserted', 0)} "
                        f"updated={metrics['location_archive'].get('provenance_updates', 0)}"
                    ),
                    metrics=metrics,
                )
                metrics["ptg_corroboration_view_published"] = (
                    await publish_provider_directory_ptg_address_corroboration_if_available()
                )
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory publishing artifacts",
                    done=3,
                    total=3,
                    message="published Provider Directory PTG corroboration artifacts",
                    metrics=metrics,
                )
            else:
                metrics["location_address_keys_stamped"] = 0
                metrics["location_archive"] = {"skipped": True, "reason": "publish_artifacts_disabled"}
                metrics["ptg_corroboration_view_published"] = False
        ctx["context"]["audit"] = metrics
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        print(
            "Provider Directory FHIR import done: "
            f"sources_seeded={metrics['sources_seeded']} "
            f"sources_probed={metrics['sources_probed']} "
            f"valid_capability_sources={metrics['valid_capability_sources']} "
            f"resource_rows={metrics['resource_rows']}"
        )
        print("PROVIDER_DIRECTORY_FHIR_IMPORT_DONE\t" + json.dumps(metrics, sort_keys=True, default=str))
        return metrics
    finally:
        if seen_stage_table_for_publish:
            await _drop_provider_directory_import_seen_stage_table(seen_stage_table_for_publish)
        await _clear_resource_rows_seen(run_id)
        if tmpdir is not None:
            tmpdir.cleanup()


async def startup(ctx: dict[str, Any]) -> None:
    ctx.setdefault("context", {})
    ctx["context"].setdefault("run", 0)
    ctx["context"].setdefault("start", _now())
    ctx["import_date"] = _normalize_import_id(ctx.get("import_date"))
    await ensure_database(bool(ctx["context"].get("test_mode", False)))
    await _ensure_provider_directory_tables()


async def shutdown(ctx: dict[str, Any]) -> None:
    ctx.setdefault("context", {})["finished_at"] = _now().isoformat()


async def main(
    *,
    test_mode: bool = False,
    seed_db_path: str | None = None,
    seed_db_url: str | None = None,
    limit: int | None = None,
    source_query: str | None = None,
    seed_only: bool = False,
    probe: bool = True,
    import_resources: bool = False,
    canonical_backfill_only: bool = False,
    contact_backfill_only: bool = False,
    full_refresh: bool = False,
    stale_cleanup: bool | None = None,
    publish_artifacts: bool | None = None,
    open_only: bool = True,
    include_auth_required: bool = False,
    resources: str | None = None,
    resource_limit: int | None = None,
    linked_resource_limit: int | None = None,
    page_limit: int | None = None,
    page_count: int | None = None,
    stream_batch_size: int | None = None,
    bulk_export: bool | None = None,
    source_concurrency: int | None = None,
    concurrency: int | None = None,
    timeout: int | None = None,
) -> dict[str, Any]:
    ctx: dict[str, Any] = {"context": {"test_mode": test_mode}}
    await startup(ctx)
    task = {
        "test_mode": test_mode,
        "seed_db_path": seed_db_path,
        "seed_db_url": seed_db_url,
        "limit": limit,
        "source_query": source_query,
        "seed_only": seed_only,
        "probe": probe,
        "import_resources": import_resources,
        "canonical_backfill_only": canonical_backfill_only,
        "contact_backfill_only": contact_backfill_only,
        "full_refresh": full_refresh,
        "stale_cleanup": stale_cleanup,
        "publish_artifacts": publish_artifacts,
        "open_only": open_only,
        "include_auth_required": include_auth_required,
        "resources": resources,
        "resource_limit": resource_limit,
        "linked_resource_limit": linked_resource_limit,
        "page_limit": page_limit,
        "page_count": page_count,
        "stream_batch_size": stream_batch_size,
        "bulk_export": bulk_export,
        "source_concurrency": source_concurrency,
        "concurrency": concurrency,
        "timeout": timeout,
    }
    result = await process_data(ctx, task)
    await shutdown(ctx)
    return result
