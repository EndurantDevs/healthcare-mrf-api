# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import base64
import contextvars
import csv
import datetime
import hashlib
import io
import json
import logging
import math
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
from dataclasses import dataclass, field, replace
from functools import partial
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, AsyncIterator, Awaitable, Callable, Iterator

import aiohttp
from sqlalchemy import case, func, or_
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.schema import CreateColumn
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import (
    ProviderDirectoryAPIEndpoint,
    ProviderDirectoryCapability,
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpoint,
    ProviderDirectoryEndpointDataset,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPaginationCheckpoint,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
    ProviderDirectoryReverseLookupCheckpoint,
    ProviderDirectorySource,
    ProviderDirectorySourceResource,
    db,
)
from process.control_lifecycle import mark_control_run
from process.control_cancel import raise_if_cancelled
from process.ext.address_canon import resolve_into_archive
from process.ext.contact_canon import canonicalize_batch as canonicalize_contact_batch
from process.ext.utils import ensure_database


DEFAULT_SEED_DB_URL = (
    "https://raw.githubusercontent.com/hltiunn/provider-directory-db/main/data/provider_directory.db"
)
LOGGER = logging.getLogger(__name__)
MAX_FHIR_JSON_BODY_BYTES = 20 * 1024 * 1024
READ_CHUNK_BYTES = 256 * 1024
DEFAULT_MAX_PAGE_COUNT = 1000
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
_PUBLISH_SCOPE_UNSET = object()
FHIR_RESOURCE_PATH_SEGMENTS = frozenset(
    resource_type.lower() for resource_type in DEFAULT_RESOURCES
)
FHIR_RESOURCE_TEMPLATE_PATH_SEGMENTS = frozenset(
    ("{resource}", "%7bresource%7d", "[parameters]", "%5bparameters%5d")
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
PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW = "provider_directory_address_corroboration"
PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE = "provider_directory_network_catalog"
PROVIDER_DIRECTORY_NETWORK_CATALOG_STAGE_PREFIX = "provider_directory_network_catalog_stage"
PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE = "provider_directory_import_seen"
PROVIDER_DIRECTORY_IMPORT_SEEN_STAGE_PREFIX = "provider_directory_import_seen_stage"
PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_INDEXES = (
    "pd_price_addr_corrob_lookup_idx",
    "pd_price_addr_corrob_active_lookup_idx",
    "pd_price_addr_corrob_source_snapshot_idx",
    "pd_price_addr_corrob_plan_pair_idx",
    "pd_price_addr_corrob_pd_source_idx",
    "pd_price_addr_corrob_network_names_gin",
)
PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES = (
    "source_network_idx",
    "source_idx",
    "network_key_idx",
    "issuer_network_key_idx",
    "name_idx",
)
PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS = (
    "location_contacts",
    "location_coordinates",
    "resource_id_npis",
    "location_address_keys",
    "location_archive",
    "address_overlay",
    "network_catalog",
    "corroboration",
)
PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGET_ALIASES = {
    "all": PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS,
    "*": PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS,
    "addresses": (
        "location_contacts",
        "location_coordinates",
        "resource_id_npis",
        "location_address_keys",
        "location_archive",
        "address_overlay",
    ),
    "address_artifacts": (
        "location_contacts",
        "location_coordinates",
        "location_address_keys",
        "location_archive",
        "address_overlay",
    ),
    "location": (
        "location_contacts",
        "location_coordinates",
        "location_address_keys",
        "location_archive",
    ),
    "locations": (
        "location_contacts",
        "location_coordinates",
        "location_address_keys",
        "location_archive",
    ),
    "network": ("network_catalog",),
    "networks": ("network_catalog",),
    "ptg_corroboration": ("corroboration",),
}
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
RESOURCE_MODELS = tuple(RESOURCE_MODELS_BY_TYPE.values())
RESOURCE_TYPES_BY_MODEL = {model: resource_type for resource_type, model in RESOURCE_MODELS_BY_TYPE.items()}
SOURCE_MODELS = (
    ProviderDirectoryAPIEndpoint,
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
    ProviderDirectoryEndpointDataset,
    ProviderDirectoryDatasetResource,
    ProviderDirectoryCanonicalResource,
    ProviderDirectorySourceResource,
    ProviderDirectoryPaginationCheckpoint,
    ProviderDirectoryReverseLookupCheckpoint,
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
US_STATE_ABBRS = tuple(US_STATE_FIPS_TO_ABBR[fips] for fips in sorted(US_STATE_FIPS_TO_ABBR))
PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON"
PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE"
_PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "provider_directory_credentials_file_override",
    default=None,
)
SECRET_ENV_PREFIX = "env:"
DEFAULT_FULL_REFRESH_MAX_PAGES = 10000
DEFAULT_STREAM_BATCH_SIZE = 5000
DEFAULT_BULK_EXPORT_MAX_POLLS = 120
DEFAULT_BULK_EXPORT_POLL_SECONDS = 5
DEFAULT_LOCATION_ADDRESS_KEY_BATCH_SIZE = 50000
DEFAULT_LOCATION_COORDINATE_BATCH_SIZE = 50000
PUBLISH_CORROBORATION_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PUBLISH_CORROBORATION"
PROVIDER_DIRECTORY_REFRESH_PRESET_MONTHLY_FULL = "monthly-full"
PROVIDER_DIRECTORY_REFRESH_PRESETS = (PROVIDER_DIRECTORY_REFRESH_PRESET_MONTHLY_FULL,)
PROVIDER_DIRECTORY_MONTHLY_FULL_DEFAULTS: dict[str, Any] = {
    "import_resources": True,
    "full_refresh": True,
    "stale_cleanup": True,
    "publish_artifacts": True,
    "publish_corroboration": True,
    "open_only": False,
    "include_auth_required": True,
    "bulk_export": True,
    "include_supplemental_catalogs": True,
}
OAUTH_TOKEN_EXPIRY_SKEW_SECONDS = 60
_OAUTH_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
FHIR_ONBOARDING_GATEWAY_HOSTS = {
    "apps.availity.com",
    "partners.centene.com",
}
CMS_NON_PUBLIC_PROVIDER_DIRECTORY_RE = re.compile(r"\bpublic\s*=\s*no\b", re.IGNORECASE)
CMS_SMA_ENDPOINT_DIRECTORY_URL = (
    "https://raw.githubusercontent.com/CMSgov/SMA-Endpoint-Directory/main/SMAEndpointDirectory.csv"
)
CMS_SMA_ENDPOINT_DIRECTORY_URL_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CMS_SMA_ENDPOINT_DIRECTORY_URL"
CMS_SMA_ENDPOINT_DIRECTORY_SOURCE = "cms-sma-endpoint-directory"
CMS_SMA_CATALOG_VALIDATION_STATUS = "catalog_public"
CMS_SMA_CATALOG_IN_DEVELOPMENT_STATUS = "catalog_in_development"
CMS_SMA_TRACKED_PROVIDER_DIRECTORY_STATUSES = {"active", "in development"}
PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS = "catalog_blocked"
PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE = "provider-directory-blocker-registry"
URL_IN_TEXT_RE = re.compile(r"https?://[^\s<>\"')]+", re.IGNORECASE)
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
AETNA_PROVIDER_DIRECTORY_DATA_BASE = "https://apif1.aetna.com/fhir/v1/providerdirectorydata"
AETNA_PROVIDER_DIRECTORY_TOKEN_URL = "https://apif1.aetna.com/fhir/v1/fhirserver_auth/oauth2/token"
CIGNA_PROVIDER_DIRECTORY_BASE = "https://fhir.cigna.com/ProviderDirectory/v1"
CIGNA_EXPECTED_NONEMPTY_RESOURCES = frozenset(
    resource_type for resource_type in DEFAULT_RESOURCES if resource_type != "Endpoint"
)
CIGNA_UNEXPECTED_EMPTY_RESOURCE_ERROR = "cigna_expected_nonempty_resource_returned_zero_rows"
CENTENE_PARTNER_PORTAL_APIS_URL = "https://partners.centene.com/apis"
CENTENE_PROVIDER_DIRECTORY_BASE = "https://iopc-pd.api.centene.com/iopc/pd/fhir/providerdirectory"
CENTENE_PROVIDER_DIRECTORY_CALIFORNIA_BASE = "https://iopc-provider.api.centene.com/iopc/provider/ca"
CENTENE_STALE_GENERIC_BASE = "https://fhir.centene.com/provider-directory"
CENTENE_PROVIDER_DIRECTORY_DOC_URL = (
    "https://external-api.my.centene.com/partner-portal/files?"
    "url=OASFiles/docs/FHIR-ProviderDirectory-GettingStartedV7-PP-2026-06-25_20:40:13.md"
)
AMERIHEALTH_CARITAS_DOC_URL = "https://developer.amerihealthcaritas.com/dvp/v1/apidocumentation"
AMERIHEALTH_CARITAS_DOC_URL_ENV = "HLTHPRT_PROVIDER_DIRECTORY_AMERIHEALTH_CARITAS_CATALOG_URL"
AMERIHEALTH_CARITAS_PROVIDER_API_PREFIX = "https://api-ext.amerihealthcaritas.com"
AMERIHEALTH_CARITAS_STALE_GENERIC_BASE = "https://fhir.amerihealthcaritas.com/provider-directory"
CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL = (
    "https://www.cchealth.org/health-insurance/about-cchp-managed-care/apis-for-developers"
)
CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CONTRA_COSTA_CATALOG_URL"
CONTRA_COSTA_PROVIDER_DIRECTORY_BASE = "https://ihyml0v6d9.execute-api.us-east-1.amazonaws.com/hxprod"
CONTRA_COSTA_PROVIDER_DIRECTORY_METADATA_URL = f"{CONTRA_COSTA_PROVIDER_DIRECTORY_BASE}/metadata"
HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE = "https://providerfhirapi.healthpartnersplans.com"
HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_METADATA_URL = f"{HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE}/metadata"
HAP_PROVIDER_DIRECTORY_DOC_URL = "https://api.hap.org/providerdirectoryapi"
HAP_PROVIDER_DIRECTORY_BASE = "https://provider-directory-r4.api.hap.org"
HAP_PROVIDER_DIRECTORY_METADATA_URL = f"{HAP_PROVIDER_DIRECTORY_BASE}/metadata"
HAP_BLOCKED_PAGINATION_HOST = "fhir-prov-dir-r4.api.hap.org"
CHORUS_PROVIDER_DIRECTORY_DOC_URL = "https://appconnect.chorushealthplans.org/developers/providerapi"
FIRST_MEDICAL_PROVIDER_DIRECTORY_DOC_URL = "https://devportal.firstmedicalpr.com/ApiLibrary"
AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS = {
    "amerihealth caritas dc": "5400",
    "amerihealth caritas district of columbia": "5400",
    "amerihealth caritas de": "7100",
    "amerihealth caritas delaware": "7100",
    "amerihealth caritas la": "2100",
    "amerihealth caritas louisiana": "2100",
    "amerihealth caritas nc": "1200",
    "amerihealth caritas north carolina": "1200",
    "amerihealth caritas nh": "0900",
    "amerihealth caritas new hampshire": "0900",
    "amerihealth caritas oh": "7700",
    "amerihealth caritas ohio": "7700",
    "amerihealth caritas pa": "0500",
    "amerihealth caritas pennsylvania": "0500",
}
UHC_INTEROPERABILITY_APIS_URL = "https://www.uhc.com/legal/interoperability-apis"
UHC_PROVIDER_DIRECTORY_BASE = "https://flex.optum.com/fhirpublic/R4"
UHC_PROVIDER_DIRECTORY_METADATA_URL = f"{UHC_PROVIDER_DIRECTORY_BASE}/metadata"
HUMANA_PROVIDER_DIRECTORY_BASE = "https://fhir.humana.com/api"
HUMANA_PROVIDER_DIRECTORY_METADATA_URL = f"{HUMANA_PROVIDER_DIRECTORY_BASE}/metadata"
IEHP_PROVIDER_DIRECTORY_BASE = "https://fhir.iehp.org/provider-directory"
IEHP_PROVIDER_DIRECTORY_METADATA_URL = f"{IEHP_PROVIDER_DIRECTORY_BASE}/metadata"
MOLINA_DEVELOPER_PORTAL_URL = "https://developer.interop.molinahealthcare.com"
MOLINA_PROVIDER_DIRECTORY_BASE = "https://api.interop.molinahealthcare.com/providerdirectory"
MOLINA_PROVIDER_DIRECTORY_METADATA_URL = f"{MOLINA_PROVIDER_DIRECTORY_BASE}/metadata"
MOLINA_PAGINATION_HOST = "molina.sapphirethreesixtyfive.com"
TMHP_PROVIDER_DIRECTORY_BASE = "https://cmsinterop.tmhp.com/tmhp/fhir/pd/R4"
TMHP_PROVIDER_DIRECTORY_METADATA_URL = f"{TMHP_PROVIDER_DIRECTORY_BASE}/metadata"
NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE = "https://dhhs-api.ne.gov/dhhs/trading-partner/api/cmsi/provider/1.0.0"
NEBRASKA_DHHS_PROVIDER_DIRECTORY_METADATA_URL = f"{NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE}/metadata"
INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE = "https://api.interopstation.com/mdhhs/fhir"
SCAN_DEVELOPER_PORTAL_URL = "https://developer.scanhealthplan.com"
SCAN_PROVIDER_DIRECTORY_BASE = "https://providerdirectory.scanhealthplan.com"
SCAN_PROVIDER_DIRECTORY_DOC_URL = (
    "https://developer.scanhealthplan.com/default/documentation/providerdirectory"
)
SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR = "scan_practitioner_role_requires_reverse_lookup"
SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_RESOURCES = ("Practitioner", "Organization", "Location")
SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_PARAMS = {
    "Practitioner": "practitioner",
    "Organization": "organization",
    "Location": "location",
}
UHC_PRACTITIONER_ROLE_REVERSE_LOOKUP_RESOURCES = ("Practitioner",)
UHC_PRACTITIONER_ROLE_REVERSE_LOOKUP_PARAMS = {
    "Practitioner": "practitioner",
}
SOURCE_RESOURCE_TIMEOUT_MIN_SECONDS = {
    (UHC_PROVIDER_DIRECTORY_BASE, resource_type): 60
    for resource_type in DEFAULT_RESOURCES
}
PROVIDER_DIRECTORY_RESOURCE_PAGE_COUNT_CAPS = {
    # This HAPI proxy returns an empty PractitionerRole Bundle for _count >= 50,
    # even though _count=25 returns rows and a next link.
    (INTEROPSTATION_MDHHS_PROVIDER_DIRECTORY_BASE, "PractitionerRole"): 25,
    # UHC/Flex InsurancePlan accepts tiny pages but returns Azure 504s for
    # _count=10/100, so full imports must walk this resource one row at a time.
    (UHC_PROVIDER_DIRECTORY_BASE, "InsurancePlan"): 1,
}
PRACTITIONER_ROLE_ZERO_RETRY_REASON = "zero_practitioner_role_with_practitioner_and_location_rows"
PRACTITIONER_ROLE_ZERO_RETRY_EMPTY_ERROR = "suspicious_zero_practitioner_role_rows_after_retry"
PAGINATION_CHECKPOINT_API_BASES = frozenset(
    {
        CIGNA_PROVIDER_DIRECTORY_BASE,
        HUMANA_PROVIDER_DIRECTORY_BASE,
        IEHP_PROVIDER_DIRECTORY_BASE,
        MOLINA_PROVIDER_DIRECTORY_BASE,
    }
)
PAGINATION_CHECKPOINT_ACTIVE = "active"
PAGINATION_CHECKPOINT_COMPLETE = "complete"
PAGINATION_CHECKPOINT_RECENT_URL_LIMIT = 64
PAGINATION_RESUME_REQUIRED_ERROR = "provider_directory_pagination_resume_required"
FHIR_CONTINUATION_QUERY_NAMES = frozenset(
    {
        "_continuationtoken",
        "_getpages",
        "_getpagesid",
        "_getpagesoffset",
        "_offset",
        "cursor",
        "cursormark",
        "nexttoken",
        "page",
        "pagetoken",
    }
)


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
    deadline_reached: bool = False

    @property
    def bounded(self) -> bool:
        return (
            self.row_limit_reached
            or self.page_limit_reached
            or self.hard_page_limit_reached
            or self.deadline_reached
        )


@dataclass(frozen=True)
class PaginationCheckpointContext:
    canonical_api_base: str
    source_scope_hash: str
    source_ids: tuple[str, ...]
    owner_run_id: str
    retry_of_run_id: str | None = None


@dataclass(frozen=True)
class PaginationResumeState:
    next_url: str | None
    pages_processed: int
    rows_processed: int
    recent_url_hashes: tuple[str, ...]
    complete: bool = False
    resumed: bool = False


@dataclass
class PartitionFetchState:
    """Shared counters for concurrent partitioned FHIR searches."""

    result_model: type
    retained_resource_rows: list[dict[str, Any]]
    fetched_count: int = 0
    written_count: int = 0
    page_count: int = 0
    partition_error_count: int = 0
    error_message: str | None = None
    hard_page_limit_reached: bool = False
    deadline_reached: bool = False
    next_url_remaining: bool = False
    seen_urls: set[str] = field(default_factory=set)
    completed_partition_prefixes: set[str] = field(default_factory=set)
    pending_partition_checkpoint_rows: list[dict[str, Any]] = field(default_factory=list)
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    writer_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    checkpoint_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True)
class PartitionFetchOptions:
    timeout: int
    run_id: str | None
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None
    row_batch_size: int
    retain_rows: bool
    deadline_at: float | None
    max_pages: int


@dataclass(frozen=True)
class PartitionPageFetchResult:
    next_url: str | None
    outcome: str


@dataclass(frozen=True)
class ScanPractitionerRoleFetchOptions:
    """Runtime controls for SCAN PractitionerRole reverse lookups."""

    per_resource_limit: int
    page_limit: int
    page_count: int
    timeout: int
    run_id: str | None
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None = None
    row_batch_size: int = DEFAULT_STREAM_BATCH_SIZE
    retain_rows: bool = True
    cancel_ctx: dict[str, Any] | None = None
    cancel_task: dict[str, Any] | None = None
    deadline_seconds: int = 0
    source: dict[str, Any] | None = None
    seed_stage_table: str | None = None
    seed_source_ids: tuple[str, ...] = ()
    existing_seed_source_ids: tuple[str, ...] = ()
    seed_page_size: int = 5000
    resume_completed_seeds: bool = False
    seen_table: str | None = None


@dataclass
class _StreamedResourceRowBuffer:
    """Buffer resource rows before sending them to the streaming writer."""

    model: type
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None
    row_batch_size: int
    pending_row_items: list[dict[str, Any]]
    rows_written: int = 0

    async def add(self, row: dict[str, Any]) -> None:
        """Add one row and flush when the configured batch size is reached."""
        if not self.row_batch_handler:
            return
        self.pending_row_items.append(row)
        if len(self.pending_row_items) >= max(1, self.row_batch_size):
            await self.flush()

    async def flush(self) -> None:
        """Flush buffered rows to the streaming writer."""
        if not self.row_batch_handler or not self.pending_row_items:
            return
        row_items = list(self.pending_row_items)
        self.pending_row_items.clear()
        self.rows_written += await self.row_batch_handler(self.model, row_items)


@dataclass(frozen=True)
class _PractitionerRoleReverseLookupRequest:
    """Immutable controls for a concurrent PractitionerRole reverse lookup."""

    source: dict[str, Any]
    rows_by_resource: dict[str, list[dict[str, Any]]]
    options: ScanPractitionerRoleFetchOptions
    resource_timeout: int
    deadline_at: float | None


@dataclass
class _PractitionerRoleReverseLookupState:
    """Shared counters and streaming output for concurrent role lookup workers."""

    result_model: type
    retained_resource_rows: list[dict[str, Any]]
    streamed_rows: _StreamedResourceRowBuffer
    retain_rows: bool
    page_limit: int
    seen_role_ids: set[str] | None = None
    seed_count: int = 0
    fetched_count: int = 0
    page_count: int = 0
    reverse_error_count: int = 0
    error_message: str | None = None
    is_hard_page_limit_reached: bool = False
    is_deadline_reached: bool = False
    has_next_url_remaining: bool = False
    completed_seed_rows: list[dict[str, Any]] = field(default_factory=list)
    state_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    writer_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    checkpoint_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def add_seed(self) -> None:
        """Count a queued reverse lookup seed."""
        async with self.state_lock:
            self.seed_count += 1

    async def mark_deadline_reached(self) -> None:
        """Record that unfinished reverse lookup work remains after deadline."""
        async with self.state_lock:
            self.is_deadline_reached = True
            self.has_next_url_remaining = True

    async def record_reverse_lookup_error(self, current_error: str) -> None:
        """Record the latest reverse lookup error with a running count."""
        async with self.state_lock:
            self.reverse_error_count += 1
            self.error_message = (
                f"reverse_lookup_errors_{self.reverse_error_count}_last_{current_error}"
            )

    async def add_completed_seed(
        self,
        request: _PractitionerRoleReverseLookupRequest,
        seed_resource_type: str,
        seed_resource_id: str,
    ) -> None:
        """Persist finished reverse lookup seeds in bounded batches for resumption."""
        if not request.options.resume_completed_seeds:
            return
        checkpoint_rows: list[dict[str, Any]] = []
        async with self.checkpoint_lock:
            self.completed_seed_rows.append(
                _reverse_lookup_checkpoint_row(
                    request.source,
                    seed_resource_type,
                    seed_resource_id,
                    request.options.run_id,
                )
            )
            if len(self.completed_seed_rows) >= _reverse_lookup_checkpoint_flush_rows():
                checkpoint_rows = list(self.completed_seed_rows)
                self.completed_seed_rows.clear()
        if checkpoint_rows:
            await _upsert_rows(ProviderDirectoryReverseLookupCheckpoint, checkpoint_rows)

    async def flush_completed_seed_rows(self) -> None:
        """Write checkpoints that did not fill a full streaming batch."""
        async with self.checkpoint_lock:
            checkpoint_rows = list(self.completed_seed_rows)
            self.completed_seed_rows.clear()
        if checkpoint_rows:
            await _upsert_rows(ProviderDirectoryReverseLookupCheckpoint, checkpoint_rows)

    async def has_reached_page_limit(self) -> bool:
        """Return whether the shared reverse lookup page limit has been reached."""
        async with self.state_lock:
            return self.is_hard_page_limit_reached

    async def add_practitioner_role_page(self, resource_rows: list[dict[str, Any]]) -> None:
        """Add parsed PractitionerRole rows and update shared page counters."""
        accepted_rows: list[dict[str, Any]] = []
        async with self.state_lock:
            self.page_count += 1
            if self.page_limit > 0 and self.page_count >= self.page_limit:
                self.is_hard_page_limit_reached = True
                self.has_next_url_remaining = True
            for row in resource_rows:
                role_id = _clean_text(row.get("resource_id"))
                if not role_id:
                    continue
                if self.seen_role_ids is not None and role_id in self.seen_role_ids:
                    continue
                if self.seen_role_ids is not None:
                    self.seen_role_ids.add(role_id)
                if self.retain_rows:
                    self.retained_resource_rows.append(row)
                self.fetched_count += 1
                accepted_rows.append(row)
        if not accepted_rows:
            return
        async with self.writer_lock:
            for row in accepted_rows:
                await self.streamed_rows.add(row)

    async def produce_seed_rows(
        self,
        request: _PractitionerRoleReverseLookupRequest,
        seed_queue: asyncio.Queue[tuple[str, str, str] | None],
        worker_count: int,
    ) -> None:
        """Queue Practitioner/Organization/Location seeds for reverse lookup workers."""
        try:
            async for seed in _iter_scan_practitioner_role_seed_rows(
                request.rows_by_resource,
                request.options,
            ):
                if request.options.cancel_ctx is not None:
                    await raise_if_cancelled(
                        request.options.cancel_ctx,
                        request.options.cancel_task,
                    )
                if _is_lookup_deadline_elapsed(request.deadline_at):
                    await self.mark_deadline_reached()
                    break
                await self.add_seed()
                await seed_queue.put(seed)
        finally:
            for _unused_worker_index in range(worker_count):
                await seed_queue.put(None)

    async def fetch_seed_rows(
        self,
        request: _PractitionerRoleReverseLookupRequest,
        seed_queue: asyncio.Queue[tuple[str, str, str] | None],
    ) -> None:
        """Consume queued seeds and fetch matching PractitionerRole pages."""
        while True:
            seed = await seed_queue.get()
            try:
                if seed is None:
                    return
                await self.fetch_seed_pages(request, seed)
            finally:
                seed_queue.task_done()

    async def fetch_seed_pages(
        self,
        request: _PractitionerRoleReverseLookupRequest,
        seed: tuple[str, str, str],
    ) -> None:
        """Fetch all PractitionerRole pages for one reverse lookup seed."""
        search_param, seed_resource_type, seed_resource_id = seed
        next_lookup_url = _scan_practitioner_role_reverse_lookup_url(
            request.source,
            search_param,
            seed_resource_type,
            seed_resource_id,
            page_count=request.options.page_count,
        )
        while next_lookup_url:
            if request.options.cancel_ctx is not None:
                await raise_if_cancelled(request.options.cancel_ctx, request.options.cancel_task)
            if _is_lookup_deadline_elapsed(request.deadline_at):
                await self.mark_deadline_reached()
                return
            if await self.has_reached_page_limit():
                return
            status_code, bundle_payload, error, _elapsed = await _fetch_source_json(
                request.source,
                next_lookup_url,
                timeout=request.resource_timeout,
            )
            if status_code != 200 or error:
                await self.record_reverse_lookup_error(error or f"http_{status_code}")
                return
            if not bundle_payload or bundle_payload.get("resourceType") != "Bundle":
                await self.record_reverse_lookup_error("non_bundle_payload")
                return
            resource_rows = _parse_practitioner_role_reverse_lookup_rows(
                request.source["source_id"],
                bundle_payload,
                request.options.run_id,
            )
            await self.add_practitioner_role_page(resource_rows)
            next_link = _next_link(bundle_payload)
            next_lookup_url = _resolved_fhir_next_url(
                request.source,
                next_lookup_url,
                next_link,
            )
            if await self.has_reached_page_limit():
                return
        await self.add_completed_seed(request, seed_resource_type, seed_resource_id)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _reverse_lookup_checkpoint_flush_rows() -> int:
    return max(1, _env_int("HLTHPRT_PROVIDER_DIRECTORY_REVERSE_LOOKUP_CHECKPOINT_FLUSH_ROWS", 250))


def _reverse_lookup_checkpoint_row(
    source: dict[str, Any],
    seed_resource_type: str,
    seed_resource_id: str,
    run_id: str | None,
) -> dict[str, Any]:
    canonical_api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if not canonical_api_base:
        raise ValueError("reverse lookup checkpoint requires a canonical API base")
    now = _now()
    return {
        "canonical_api_base": canonical_api_base,
        "seed_resource_type": seed_resource_type,
        "seed_resource_id": seed_resource_id,
        "last_completed_run_id": run_id,
        "completed_at": now,
        "updated_at": now,
    }


def _reverse_lookup_checkpoint_exclusion_sql(
    options: ScanPractitionerRoleFetchOptions,
    resource_id_expr: str,
) -> str:
    """Exclude seeds already completed for the same canonical public endpoint."""
    canonical_api_base = _canonical_base(
        (options.source or {}).get("canonical_api_base") or (options.source or {}).get("api_base")
    )
    if not options.resume_completed_seeds or not canonical_api_base:
        return ""
    checkpoint_ref = _qt(_schema(), ProviderDirectoryReverseLookupCheckpoint.__tablename__)
    return f"""
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {checkpoint_ref} AS checkpoint
                        WHERE checkpoint.canonical_api_base = :checkpoint_canonical_api_base
                          AND checkpoint.seed_resource_type = :resource_type
                          AND checkpoint.seed_resource_id = {resource_id_expr}
                   )
            """


async def _clear_reverse_lookup_checkpoints(
    source: dict[str, Any],
    *,
    seed_resource_type: str | None = None,
) -> int:
    """Start the next completed scan fresh after every seed finished once."""
    canonical_api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if not canonical_api_base:
        return 0
    seed_type_filter = (
        "AND seed_resource_type = :seed_resource_type"
        if seed_resource_type
        else ""
    )
    query_params_by_name = {"canonical_api_base": canonical_api_base}
    if seed_resource_type:
        query_params_by_name["seed_resource_type"] = seed_resource_type
    result = await db.status(
        f"""
        DELETE FROM {_qt(_schema(), ProviderDirectoryReverseLookupCheckpoint.__tablename__)}
         WHERE canonical_api_base = :canonical_api_base
           {seed_type_filter};
        """,
        **query_params_by_name,
    )
    return int(result or 0)


def _reverse_lookup_checkpoint_source_ids(
    options: ScanPractitionerRoleFetchOptions,
) -> list[str]:
    return list(options.seed_source_ids or options.existing_seed_source_ids or ())


def _reverse_lookup_checkpoint_role_match_sql(
    role_alias: str,
    checkpoint_alias: str,
) -> str:
    practitioner_match = _sql_ref_matches_resource(
        f"{role_alias}.practitioner_ref",
        "Practitioner",
        f"{checkpoint_alias}.seed_resource_id",
    )
    organization_match = _sql_ref_matches_resource(
        f"{role_alias}.organization_ref",
        "Organization",
        f"{checkpoint_alias}.seed_resource_id",
    )
    location_match = _sql_ref_matches_resource(
        "location_ref.value",
        "Location",
        f"{checkpoint_alias}.seed_resource_id",
    )
    return f"""
        (
            ({checkpoint_alias}.seed_resource_type = 'Practitioner' AND {practitioner_match})
         OR ({checkpoint_alias}.seed_resource_type = 'Organization' AND {organization_match})
         OR (
                {checkpoint_alias}.seed_resource_type = 'Location'
            AND EXISTS (
                SELECT 1
                  FROM jsonb_array_elements_text(
                           COALESCE({role_alias}.location_refs::jsonb, '[]'::jsonb)
                       ) AS location_ref(value)
                 WHERE {location_match}
            )
         )
        )
    """


async def _mark_checkpointed_reverse_lookup_roles_seen(
    options: ScanPractitionerRoleFetchOptions,
) -> int:
    """Keep earlier reverse-lookup rows visible to stale cleanup during resumption."""
    if not options.resume_completed_seeds or not options.run_id:
        return 0
    source_ids = _reverse_lookup_checkpoint_source_ids(options)
    canonical_api_base = _canonical_base(
        (options.source or {}).get("canonical_api_base") or (options.source or {}).get("api_base")
    )
    if not source_ids or not canonical_api_base:
        return 0
    role_ref = _qt(_schema(), ProviderDirectoryPractitionerRole.__tablename__)
    checkpoint_ref = _qt(_schema(), ProviderDirectoryReverseLookupCheckpoint.__tablename__)
    seen_ref = _qt(_schema(), options.seen_table or PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
    role_match_sql = _reverse_lookup_checkpoint_role_match_sql("role", "checkpoint")
    run_filter = "" if options.seen_table else "ON CONFLICT (run_id, resource_type, source_id, resource_id) DO NOTHING"
    await db.status(
        f"""
        UPDATE {role_ref} AS role
           SET last_seen_run_id = :run_id,
               updated_at = now()
          FROM {checkpoint_ref} AS checkpoint
         WHERE role.source_id = ANY(CAST(:source_ids AS varchar[]))
           AND checkpoint.canonical_api_base = :canonical_api_base
           AND {role_match_sql};
        """,
        run_id=options.run_id,
        source_ids=source_ids,
        canonical_api_base=canonical_api_base,
    )
    return int(
        await db.status(
            f"""
            INSERT INTO {seen_ref} (run_id, resource_type, source_id, resource_id)
            SELECT DISTINCT :run_id,
                            'PractitionerRole',
                            role.source_id,
                            role.resource_id
              FROM {role_ref} AS role
              JOIN {checkpoint_ref} AS checkpoint
                ON checkpoint.canonical_api_base = :canonical_api_base
               AND {role_match_sql}
             WHERE role.source_id = ANY(CAST(:source_ids AS varchar[]))
            {run_filter};
            """,
            run_id=options.run_id,
            source_ids=source_ids,
            canonical_api_base=canonical_api_base,
        )
        or 0
    )


async def _mark_postal_checkpointed_roles_seen(
    source_record: dict[str, Any],
    source_ids: list[str],
    run_id: str | None,
    seen_table: str | None,
) -> int:
    """Preserve prior ZIP-partition rows during a resumed stale-cleanup run."""
    canonical_api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    if not run_id or not source_ids or not canonical_api_base:
        return 0
    role_ref = _qt(_schema(), ProviderDirectoryPractitionerRole.__tablename__)
    checkpoint_ref = _qt(_schema(), ProviderDirectoryReverseLookupCheckpoint.__tablename__)
    seen_ref = _qt(_schema(), seen_table or PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
    checkpoint_exists_sql = f"""
        EXISTS (
            SELECT 1
              FROM {checkpoint_ref} AS checkpoint
             WHERE checkpoint.canonical_api_base = :canonical_api_base
               AND checkpoint.seed_resource_type = :checkpoint_type
        )
    """
    run_conflict_sql = (
        "" if seen_table else "ON CONFLICT (run_id, resource_type, source_id, resource_id) DO NOTHING"
    )
    query_params_by_name = {
        "run_id": run_id,
        "source_ids": source_ids,
        "canonical_api_base": canonical_api_base,
        "checkpoint_type": _uhc_role_postal_checkpoint_type(source_record, source_ids),
    }
    await db.status(
        f"""
        UPDATE {role_ref} AS role
           SET last_seen_run_id = :run_id,
               updated_at = now()
         WHERE role.source_id = ANY(CAST(:source_ids AS varchar[]))
           AND {checkpoint_exists_sql};
        """,
        **query_params_by_name,
    )
    return int(
        await db.status(
            f"""
            INSERT INTO {seen_ref} (run_id, resource_type, source_id, resource_id)
            SELECT :run_id, 'PractitionerRole', role.source_id, role.resource_id
              FROM {role_ref} AS role
             WHERE role.source_id = ANY(CAST(:source_ids AS varchar[]))
               AND {checkpoint_exists_sql}
            {run_conflict_sql};
            """,
            **query_params_by_name,
        )
        or 0
    )


def _q(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _provider_directory_index_elements_sql(model: Any, index: dict[str, Any]) -> str:
    elements: list[str] = []
    using = str(index.get("using") or "").strip().lower()
    table = getattr(model, "__table__", None)
    columns = getattr(table, "columns", {}) if table is not None else {}
    for raw_element in index.get("index_elements", ()) or ():
        element = str(raw_element)
        column = columns.get(element) if hasattr(columns, "get") else None
        column_type = getattr(column, "type", None)
        if (
            using == "gin"
            and column is not None
            and isinstance(column_type, SQLAlchemyJSON)
            and not isinstance(column_type, JSONB)
        ):
            elements.append(f"({_q(element)}::jsonb)")
        else:
            elements.append(element)
    return ", ".join(elements)


POSTGRES_IDENTIFIER_MAX_LENGTH = 63


def _bounded_identifier(name: str) -> str:
    """Return a deterministic PostgreSQL-safe identifier no longer than 63 bytes."""
    if len(name) <= POSTGRES_IDENTIFIER_MAX_LENGTH:
        return name
    digest = hashlib.sha1(name.encode("utf-8", errors="ignore")).hexdigest()[:8]
    trim_to = max(1, POSTGRES_IDENTIFIER_MAX_LENGTH - len(digest) - 1)
    return f"{name[:trim_to]}_{digest}"


def _sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_ref_matches_resource(ref_expr: str, resource_type: str, resource_id_expr: str) -> str:
    resource_type_literal = str(resource_type).replace("'", "''")
    return (
        f"({ref_expr} IN ({resource_id_expr}, '{resource_type_literal}/' || {resource_id_expr}) "
        f"OR {ref_expr} LIKE '%/{resource_type_literal}/' || {resource_id_expr})"
    )


def _sql_reference_resource_id(ref_expr: str, resource_type: str) -> str:
    resource_type_literal = str(resource_type).replace("'", "''")
    return (
        "NULLIF(BTRIM(CASE "
        f"WHEN {ref_expr} LIKE '%/{resource_type_literal}/%' "
        f"THEN regexp_replace({ref_expr}, '^.*/{resource_type_literal}/', '') "
        f"WHEN {ref_expr} LIKE '{resource_type_literal}/%' "
        f"THEN regexp_replace({ref_expr}, '^{resource_type_literal}/', '') "
        f"ELSE {ref_expr} "
        "END), '')"
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


def _format_coordinate(value: float) -> str:
    return f"{value:.8f}".rstrip("0").rstrip(".")


def _is_plausible_us_coordinate_pair(latitude: float, longitude: float) -> bool:
    return (
        (24 <= latitude <= 50 and -125 <= longitude <= -66)
        or (51 <= latitude <= 72 and -180 <= longitude <= -129)
        or (18 <= latitude <= 23 and -161 <= longitude <= -154)
        or (17 <= latitude <= 19 and -68 <= longitude <= -64)
        or (13 <= latitude <= 16 and 144 <= longitude <= 146)
        or (-15 <= latitude <= -10 and -171 <= longitude <= -168)
    )


def _is_usable_coordinate_pair(latitude: float, longitude: float, country_code: str | None) -> bool:
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        return False
    if abs(latitude) < 0.0000001 and abs(longitude) < 0.0000001:
        return False
    if not country_code or country_code == "US":
        return _is_plausible_us_coordinate_pair(latitude, longitude)
    return True


def _parse_coordinate_number(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _normalize_fhir_position(
    position: dict[str, Any],
    *,
    country_code: str | None,
) -> tuple[str | None, str | None]:
    latitude = _parse_coordinate_number(position.get("latitude"))
    longitude = _parse_coordinate_number(position.get("longitude"))
    if latitude is None or longitude is None:
        return None, None

    candidates = [(latitude, longitude)]
    if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
        candidates.extend((latitude / divisor, longitude / divisor) for divisor in (1_000_000, 10_000_000))

    for candidate_latitude, candidate_longitude in candidates:
        if _is_usable_coordinate_pair(candidate_latitude, candidate_longitude, country_code):
            return _format_coordinate(candidate_latitude), _format_coordinate(candidate_longitude)
    return None, None


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


def _country_restore_default_us_sql(value_sql: str) -> str:
    return f"COALESCE(NULLIF(({_country_restore_sql(value_sql)}), ''), 'US')"


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


def _resource_or_metadata_parent_base(api_base: str | None) -> str | None:
    canonical = _canonical_base(api_base)
    if not canonical:
        return None
    parsed = urllib.parse.urlsplit(canonical)
    if not parsed.scheme or not parsed.netloc:
        return None
    segments = [segment for segment in parsed.path.strip("/").split("/") if segment]
    last_segment = segments[-1].lower() if segments else ""
    if (
        last_segment == "metadata"
        or last_segment in FHIR_RESOURCE_PATH_SEGMENTS
        or last_segment in FHIR_RESOURCE_TEMPLATE_PATH_SEGMENTS
    ):
        return _parent_base_url(canonical)
    return None


def _rebased_derived_endpoint_fields(
    row: dict[str, Any],
    endpoint_overrides: dict[str, Any],
    previous_api_base: str,
    normalized_api_base: str,
) -> dict[str, Any]:
    rebased_fields_by_name = dict(endpoint_overrides)
    normalized_fields_by_name = _source_override_endpoint_fields(normalized_api_base)
    previous_base = _canonical_base(previous_api_base)
    for endpoint_field, normalized_endpoint in normalized_fields_by_name.items():
        current_endpoint = (
            rebased_fields_by_name.get(endpoint_field)
            if endpoint_field in rebased_fields_by_name
            else row.get(endpoint_field)
        )
        canonical_endpoint = _canonical_base(current_endpoint)
        is_derived_from_previous = bool(
            previous_base
            and canonical_endpoint
            and (
                canonical_endpoint == previous_base
                or canonical_endpoint.startswith(previous_base.rstrip("/") + "/")
            )
        )
        if not canonical_endpoint or is_derived_from_previous:
            rebased_fields_by_name[endpoint_field] = normalized_endpoint
    return rebased_fields_by_name


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
        if (
            last_segment in FHIR_RESOURCE_PATH_SEGMENTS
            or last_segment in FHIR_RESOURCE_TEMPLATE_PATH_SEGMENTS
            or last_segment in FHIR_BASE_TRAILING_PATH_SEGMENTS
        ):
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
    metadata = _source_metadata(source)
    for explicit_url in (
        metadata.get("provider_directory_confirmed_metadata_url"),
        metadata.get("metadata_url"),
    ):
        metadata_url = _clean_text(explicit_url)
        if not metadata_url or metadata_url in seen:
            continue
        metadata_base = _provider_directory_base_from_metadata_url(metadata_url)
        api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
        candidate_base = _canonical_base(metadata_base or api_base)
        if not candidate_base:
            continue
        urls.append((candidate_base, metadata_url))
        seen.add(metadata_url)
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


def _max_page_count() -> int:
    raw = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_PAGE_COUNT")
    if raw is None:
        return DEFAULT_MAX_PAGE_COUNT
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return DEFAULT_MAX_PAGE_COUNT


def _bounded_page_count(page_count: int) -> int:
    return max(1, min(int(page_count or 1), _max_page_count()))


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _metadata_resource_page_count_cap(source: dict[str, Any], resource_type: str) -> int | None:
    metadata = _source_metadata(source)
    raw_caps = (
        metadata.get("provider_directory_resource_page_count_caps")
        or metadata.get("provider_directory_page_count_caps")
    )
    if isinstance(raw_caps, dict):
        for key in (resource_type, resource_type.lower()):
            if key in raw_caps and (cap := _positive_int(raw_caps.get(key))):
                return cap
    raw_resource = metadata.get("provider_directory_resource_page_count_cap")
    if isinstance(raw_resource, dict):
        for key in (resource_type, resource_type.lower()):
            if key in raw_resource and (cap := _positive_int(raw_resource.get(key))):
                return cap
    return None


def _source_resource_page_count(source: dict[str, Any], resource_type: str, page_count: int) -> int:
    bounded_count = _bounded_page_count(page_count)
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    hard_cap = PROVIDER_DIRECTORY_RESOURCE_PAGE_COUNT_CAPS.get((api_base or "", resource_type))
    metadata_cap = _metadata_resource_page_count_cap(source, resource_type)
    caps = [cap for cap in (hard_cap, metadata_cap) if cap]
    if not caps:
        return bounded_count
    return max(1, min(bounded_count, min(caps)))


def _source_resource_timeout(source: dict[str, Any], resource_type: str, timeout: int) -> int:
    base_timeout = max(int(timeout or 0), 1)
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    min_timeout = SOURCE_RESOURCE_TIMEOUT_MIN_SECONDS.get((api_base or "", resource_type))
    if min_timeout is None:
        return base_timeout
    return max(base_timeout, min_timeout)


def _source_supported_resource_types(source: dict[str, Any]) -> set[str] | None:
    configured_resources = _source_metadata(source).get(
        "provider_directory_supported_resources"
    )
    if not isinstance(configured_resources, list):
        return None
    return {
        resource_type
        for resource_type in (_clean_text(value) for value in configured_resources)
        if resource_type in DEFAULT_RESOURCES
    }


def _url_with_count(url: str, page_count: int) -> str:
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if not any(key == "_count" for key, _value in query_items):
        query_items.append(("_count", str(_bounded_page_count(page_count))))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _resource_start_url(source: dict[str, Any], resource_type: str, *, page_count: int) -> str | None:
    supported_resource_types = _source_supported_resource_types(source)
    if (
        supported_resource_types is not None
        and resource_type not in supported_resource_types
    ):
        return None
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    resource_page_count = _source_resource_page_count(source, resource_type, page_count)
    endpoint_field = RESOURCE_ENDPOINT_FIELDS.get(resource_type)
    endpoint = _clean_text(source.get(endpoint_field)) if endpoint_field else None
    if endpoint and not _is_placeholder_url(endpoint):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.scheme and parsed.netloc:
            return _url_with_count(endpoint, resource_page_count)
        if api_base:
            return _url_with_count(
                urllib.parse.urljoin(api_base.rstrip("/") + "/", endpoint),
                resource_page_count,
            )
    if not api_base:
        return None
    return _url_with_count(f"{api_base}/{resource_type}", resource_page_count)


SCAN_SEARCH_ALPHABET = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
UHC_SEARCH_ALPHABET = tuple("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
UHC_POSTAL_SEARCH_ALPHABET = tuple("0123456789")
UHC_POSTAL_ROOT_ALPHABET = tuple("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
UHC_ADAPTIVE_PARTITION_TOTAL_CAP = 10000
UHC_ADAPTIVE_PARTITION_DEFAULT_MAX_PREFIX_LENGTH = 4
UHC_POSTAL_PARTITION_MAX_PREFIX_LENGTH = 5
UHC_ROLE_POSTAL_CHECKPOINT_TYPE = "PractitionerRolePostalPrefix"


def _letter_prefixes(length: int) -> tuple[str, ...]:
    if length <= 0:
        return ("",)
    prefixes = ("",)
    for _ in range(length):
        prefixes = tuple(prefix + letter for prefix in prefixes for letter in UHC_SEARCH_ALPHABET)
    return prefixes


def _scan_partition_values(resource_type: str) -> tuple[tuple[str, str], ...]:
    if resource_type == "Practitioner":
        return tuple(("family", first + second) for first in SCAN_SEARCH_ALPHABET for second in SCAN_SEARCH_ALPHABET)
    if resource_type == "Organization":
        return tuple(("name", first + second) for first in SCAN_SEARCH_ALPHABET for second in SCAN_SEARCH_ALPHABET)
    if resource_type == "Location":
        return tuple(("name", value) for value in SCAN_SEARCH_ALPHABET)
    return ()


def _aetna_provider_directory_data_partition_values(resource_type: str) -> tuple[tuple[str, str], ...]:
    if resource_type == "InsurancePlan":
        return (("name", "aetna"),)
    state_param_by_resource = {
        "Practitioner": "address-state:exact",
        "Organization": "address-state",
        "Location": "address-state:exact",
    }
    state_param = state_param_by_resource.get(resource_type)
    if not state_param:
        return ()
    return tuple((state_param, state) for state in US_STATE_ABBRS)


def _uhc_provider_directory_partition_values(resource_type: str) -> tuple[tuple[str, str], ...]:
    if resource_type == "Practitioner":
        return tuple(("family", prefix) for prefix in _letter_prefixes(2))
    if resource_type == "Organization":
        return tuple(("name", prefix) for prefix in _letter_prefixes(2))
    if resource_type == "Location":
        return tuple(("address-state", state) for state in US_STATE_ABBRS)
    if resource_type == "PractitionerRole":
        return tuple(
            ("location.address-postalcode", prefix)
            for prefix in UHC_POSTAL_ROOT_ALPHABET
        )
    return ()


def _url_with_query_item(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    if not any(item_key == key for item_key, _item_value in query_items):
        query_items.append((key, value))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _url_with_replaced_query_item(url: str, key: str, value: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query_items = [
        (item_key, item_value)
        for item_key, item_value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if item_key != key
    ]
    query_items.append((key, value))
    query = urllib.parse.urlencode(query_items, doseq=True)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment))


def _bundle_total(payload: dict[str, Any]) -> int | None:
    total = payload.get("total")
    try:
        return int(total)
    except (TypeError, ValueError):
        return None


def _uhc_adaptive_partition_max_prefix_length() -> int:
    return _env_int(
        "HLTHPRT_PROVIDER_DIRECTORY_UHC_MAX_PREFIX_LENGTH",
        UHC_ADAPTIVE_PARTITION_DEFAULT_MAX_PREFIX_LENGTH,
    )


def _uhc_adaptive_partition_child_urls(
    source_record: dict[str, Any],
    resource_type: str,
    url: str,
    bundle_payload: dict[str, Any],
) -> list[str]:
    api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    if api_base != UHC_PROVIDER_DIRECTORY_BASE:
        return []
    query_items = urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    query_by_name = dict(query_items)
    if "_getpages" in query_by_name:
        return []
    search_param = {
        "Practitioner": "family",
        "Organization": "name",
        "PractitionerRole": "location.address-postalcode",
    }.get(resource_type)
    if not search_param:
        return []
    total = _bundle_total(bundle_payload)
    if total is None or total < UHC_ADAPTIVE_PARTITION_TOTAL_CAP:
        return []
    prefix = _clean_text(query_by_name.get(search_param))
    if not prefix:
        return []
    max_prefix_length = (
        UHC_POSTAL_PARTITION_MAX_PREFIX_LENGTH
        if resource_type == "PractitionerRole"
        else _uhc_adaptive_partition_max_prefix_length()
    )
    if len(prefix) >= max_prefix_length:
        return []
    search_alphabet = (
        UHC_POSTAL_SEARCH_ALPHABET
        if resource_type == "PractitionerRole"
        else UHC_SEARCH_ALPHABET
    )
    return [
        _url_with_replaced_query_item(url, search_param, prefix + character)
        for character in search_alphabet
    ]


def _is_uhc_role_zip_cap_hit(
    source: dict[str, Any],
    resource_type: str,
    url: str,
    payload: dict[str, Any],
) -> bool:
    if resource_type != "PractitionerRole" or not _is_uhc_role_postal_partition_enabled(source):
        return False
    query_by_name = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    )
    if "_getpages" in query_by_name:
        return False
    prefix = _clean_text(query_by_name.get("location.address-postalcode"))
    return bool(
        prefix
        and len(prefix) >= UHC_POSTAL_PARTITION_MAX_PREFIX_LENGTH
        and (_bundle_total(payload) or 0) >= UHC_ADAPTIVE_PARTITION_TOTAL_CAP
    )


def _is_uhc_partition_cap_exhausted(
    source_record: dict[str, Any],
    url: str,
    payload: dict[str, Any],
) -> bool:
    api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    if api_base != UHC_PROVIDER_DIRECTORY_BASE:
        return False
    query_by_name = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query, keep_blank_values=True)
    )
    return (
        "_getpages" not in query_by_name
        and (_bundle_total(payload) or 0) >= UHC_ADAPTIVE_PARTITION_TOTAL_CAP
    )


def _is_uhc_role_postal_partition_enabled(source: dict[str, Any]) -> bool:
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if api_base != UHC_PROVIDER_DIRECTORY_BASE:
        return False
    return os.getenv("HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS", "1").lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _uhc_partition_residual_error(
    source_record: dict[str, Any],
    resource_type: str,
) -> str | None:
    api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    if api_base != UHC_PROVIDER_DIRECTORY_BASE:
        return None
    if resource_type == "PractitionerRole" and not _is_uhc_role_postal_partition_enabled(
        source_record
    ):
        return None
    return {
        "Practitioner": "uhc_practitioner_name_residual_unverified",
        "PractitionerRole": "uhc_practitionerrole_residual_unverified",
        "Organization": "uhc_organization_name_residual_unverified",
        "Location": "uhc_location_state_residual_unverified",
    }.get(resource_type)


def _uhc_role_postal_checkpoint_type(
    source_record: dict[str, Any],
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    scoped_source_ids = source_ids or source_record.get("_partition_checkpoint_source_ids")
    if isinstance(scoped_source_ids, str):
        scoped_source_ids = (scoped_source_ids,)
    if not scoped_source_ids:
        scoped_source_ids = (_clean_text(source_record.get("source_id")) or "canonical",)
    scope_payload = "\x1f".join(sorted(set(scoped_source_ids)))
    scope_hash = hashlib.sha256(scope_payload.encode("utf-8")).hexdigest()[:16]
    return f"{UHC_ROLE_POSTAL_CHECKPOINT_TYPE}:{scope_hash}"


def _uhc_role_postal_prefix(
    source_record: dict[str, Any],
    resource_type: str,
    request_url: str,
) -> str | None:
    if resource_type != "PractitionerRole" or not _is_uhc_role_postal_partition_enabled(
        source_record
    ):
        return None
    query_by_name = dict(
        urllib.parse.parse_qsl(urllib.parse.urlsplit(request_url).query, keep_blank_values=True)
    )
    if "_getpages" in query_by_name:
        return None
    return _clean_text(query_by_name.get("location.address-postalcode"))


def _partition_checkpoint_flush_rows() -> int:
    return max(
        1,
        _env_int("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CHECKPOINT_FLUSH_ROWS", 25),
    )


async def _load_partition_checkpoints(
    source_record: dict[str, Any],
    resource_type: str,
    run_id: str | None,
) -> set[str]:
    if not run_id or resource_type != "PractitionerRole":
        return set()
    canonical_api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    if not canonical_api_base or not _is_uhc_role_postal_partition_enabled(source_record):
        return set()
    checkpoint_rows = await db.all(
        f"""
        SELECT seed_resource_id
          FROM {_qt(_schema(), ProviderDirectoryReverseLookupCheckpoint.__tablename__)}
         WHERE canonical_api_base = :canonical_api_base
           AND seed_resource_type = :seed_resource_type;
        """,
        canonical_api_base=canonical_api_base,
        seed_resource_type=_uhc_role_postal_checkpoint_type(source_record),
    )
    return {
        prefix
        for checkpoint_row in checkpoint_rows
        if (prefix := _clean_text(checkpoint_row[0]))
    }


async def _record_partition_checkpoint(
    state: PartitionFetchState,
    source_record: dict[str, Any],
    prefix: str | None,
    run_id: str | None,
) -> None:
    if not prefix or not run_id:
        return
    checkpoint_batch_rows: list[dict[str, Any]] = []
    async with state.checkpoint_lock:
        if prefix in state.completed_partition_prefixes:
            return
        state.completed_partition_prefixes.add(prefix)
        state.pending_partition_checkpoint_rows.append(
            _reverse_lookup_checkpoint_row(
                source_record,
                _uhc_role_postal_checkpoint_type(source_record),
                prefix,
                run_id,
            )
        )
        if len(state.pending_partition_checkpoint_rows) >= _partition_checkpoint_flush_rows():
            checkpoint_batch_rows = list(state.pending_partition_checkpoint_rows)
            state.pending_partition_checkpoint_rows.clear()
    if checkpoint_batch_rows:
        await _upsert_rows(ProviderDirectoryReverseLookupCheckpoint, checkpoint_batch_rows)


async def _flush_partition_checkpoints(state: PartitionFetchState) -> None:
    async with state.checkpoint_lock:
        checkpoint_batch_rows = list(state.pending_partition_checkpoint_rows)
        state.pending_partition_checkpoint_rows.clear()
    if checkpoint_batch_rows:
        await _upsert_rows(ProviderDirectoryReverseLookupCheckpoint, checkpoint_batch_rows)


def _resource_start_urls(source: dict[str, Any], resource_type: str, *, page_count: int) -> list[str]:
    start_url = _resource_start_url(source, resource_type, page_count=page_count)
    if not start_url:
        return []
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if api_base == AETNA_PROVIDER_DIRECTORY_DATA_BASE:
        partitions = _aetna_provider_directory_data_partition_values(resource_type)
        if not partitions:
            return [start_url]
        return [_url_with_query_item(start_url, key, value) for key, value in partitions]
    if api_base == UHC_PROVIDER_DIRECTORY_BASE:
        partitions = _uhc_provider_directory_partition_values(resource_type)
        if not partitions:
            return [start_url]
        return [_url_with_query_item(start_url, key, value) for key, value in partitions]
    if api_base != SCAN_PROVIDER_DIRECTORY_BASE:
        return [start_url]
    partitions = _scan_partition_values(resource_type)
    if not partitions:
        return [start_url]
    return [_url_with_query_item(start_url, key, value) for key, value in partitions]


def _scan_practitioner_role_requires_reverse_lookup(source: dict[str, Any], resource_type: str) -> bool:
    return (
        resource_type == "PractitionerRole"
        and not _is_uhc_role_postal_partition_enabled(source)
        and bool(_practitioner_role_reverse_lookup_resources(source))
    )


def _scan_practitioner_role_reverse_lookup_planned(source: dict[str, Any], resources: list[str]) -> bool:
    reverse_lookup_resources = _practitioner_role_reverse_lookup_resources(source)
    return (
        _scan_practitioner_role_requires_reverse_lookup(source, "PractitionerRole")
        and "PractitionerRole" in resources
        and any(resource in resources for resource in reverse_lookup_resources)
    )


def _practitioner_role_reverse_lookup_resources(source: dict[str, Any] | None) -> tuple[str, ...]:
    source = source or {}
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if api_base == SCAN_PROVIDER_DIRECTORY_BASE:
        return SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_RESOURCES
    if api_base == UHC_PROVIDER_DIRECTORY_BASE:
        return UHC_PRACTITIONER_ROLE_REVERSE_LOOKUP_RESOURCES
    return ()


def _practitioner_role_reverse_lookup_params(source: dict[str, Any] | None) -> dict[str, str]:
    source = source or {}
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if api_base == SCAN_PROVIDER_DIRECTORY_BASE:
        return SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_PARAMS
    if api_base == UHC_PROVIDER_DIRECTORY_BASE:
        return UHC_PRACTITIONER_ROLE_REVERSE_LOOKUP_PARAMS
    return {}


def _scan_practitioner_role_seed_rows(
    source: dict[str, Any] | None,
    rows_by_resource: dict[str, list[dict[str, Any]]]
) -> Iterator[tuple[str, str, str]]:
    seen: set[tuple[str, str]] = set()
    search_params = _practitioner_role_reverse_lookup_params(source)
    for resource_type in _practitioner_role_reverse_lookup_resources(source):
        search_param = search_params.get(resource_type)
        if not search_param:
            continue
        for row in rows_by_resource.get(resource_type, []):
            resource_id = _clean_text(row.get("resource_id"))
            if not resource_id:
                continue
            key = (resource_type, resource_id)
            if key in seen:
                continue
            seen.add(key)
            yield (search_param, resource_type, resource_id)


async def _iter_scan_practitioner_role_seed_rows(
    rows_by_resource: dict[str, list[dict[str, Any]]],
    options: ScanPractitionerRoleFetchOptions,
) -> AsyncIterator[tuple[str, str, str]]:
    if options.seed_stage_table and options.seed_source_ids:
        async for seed in _scan_role_stage_seeds(options):
            yield seed
        return
    in_memory_seed_count = 0
    for seed in _scan_practitioner_role_seed_rows(options.source, rows_by_resource):
        in_memory_seed_count += 1
        yield seed
    if in_memory_seed_count:
        return
    if options.existing_seed_source_ids:
        async for seed in _scan_role_db_seed_rows(options):
            yield seed
        return


async def _scan_role_stage_seeds(
    options: ScanPractitionerRoleFetchOptions,
) -> AsyncIterator[tuple[str, str, str]]:
    """Page reverse lookup seed resource ids from the current import stage."""
    stage_ref = _qt(_schema(), options.seed_stage_table or "")
    source_ids = list(options.seed_source_ids or ())
    page_size = max(1, options.seed_page_size)
    search_params = _practitioner_role_reverse_lookup_params(options.source)
    checkpoint_exclusion_sql = _reverse_lookup_checkpoint_exclusion_sql(options, "seed.resource_id")
    checkpoint_canonical_api_base = _canonical_base(
        (options.source or {}).get("canonical_api_base") or (options.source or {}).get("api_base")
    )
    for resource_type in _practitioner_role_reverse_lookup_resources(options.source):
        search_param = search_params.get(resource_type)
        if not search_param:
            continue
        last_resource_id = ""
        while True:
            resource_rows = await db.all(
                f"""
                SELECT DISTINCT seed.resource_id
                  FROM {stage_ref} AS seed
                 WHERE seed.resource_type = :resource_type
                   AND seed.source_id = ANY(CAST(:source_ids AS varchar[]))
                   AND seed.resource_id > :last_resource_id
                   {checkpoint_exclusion_sql}
                 ORDER BY seed.resource_id
                 LIMIT :limit;
                """,
                resource_type=resource_type,
                source_ids=source_ids,
                last_resource_id=last_resource_id,
                limit=page_size,
                checkpoint_canonical_api_base=checkpoint_canonical_api_base,
            )
            if not resource_rows:
                break
            for resource_row in resource_rows:
                resource_id = _clean_text(resource_row[0])
                if not resource_id:
                    continue
                last_resource_id = resource_id
                yield (search_param, resource_type, resource_id)
            if len(resource_rows) < page_size:
                break


async def _scan_role_db_seed_rows(
    options: ScanPractitionerRoleFetchOptions,
) -> AsyncIterator[tuple[str, str, str]]:
    """Page reverse lookup seed resource ids from already imported source rows."""
    source_ids = list(options.existing_seed_source_ids or ())
    if not source_ids:
        return
    page_size = max(1, options.seed_page_size)
    search_params = _practitioner_role_reverse_lookup_params(options.source)
    checkpoint_exclusion_sql = _reverse_lookup_checkpoint_exclusion_sql(options, "seed.resource_id")
    checkpoint_canonical_api_base = _canonical_base(
        (options.source or {}).get("canonical_api_base") or (options.source or {}).get("api_base")
    )
    seed_table_by_resource_type = {
        "Practitioner": "provider_directory_practitioner",
        "Organization": "provider_directory_organization",
        "Location": "provider_directory_location",
    }
    for resource_type in _practitioner_role_reverse_lookup_resources(options.source):
        search_param = search_params.get(resource_type)
        table_name = seed_table_by_resource_type.get(resource_type)
        if not search_param or not table_name:
            continue
        last_resource_id = ""
        while True:
            resource_rows = await db.all(
                f"""
                SELECT DISTINCT seed.resource_id
                  FROM {_qt(_schema(), table_name)} AS seed
                 WHERE seed.source_id = ANY(CAST(:source_ids AS varchar[]))
                   AND seed.resource_id > :last_resource_id
                   {checkpoint_exclusion_sql}
                 ORDER BY seed.resource_id
                 LIMIT :limit;
                """,
                resource_type=resource_type,
                source_ids=source_ids,
                last_resource_id=last_resource_id,
                limit=page_size,
                checkpoint_canonical_api_base=checkpoint_canonical_api_base,
            )
            if not resource_rows:
                break
            for resource_row in resource_rows:
                resource_id = _clean_text(resource_row[0])
                if not resource_id:
                    continue
                last_resource_id = resource_id
                yield (search_param, resource_type, resource_id)
            if len(resource_rows) < page_size:
                break


def _scan_practitioner_role_reverse_lookup_url(
    source: dict[str, Any],
    search_param: str,
    resource_type: str,
    resource_id: str,
    *,
    page_count: int,
) -> str | None:
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if not api_base:
        return None
    reference = f"{resource_type}/{resource_id}"
    return _url_with_query_params(
        f"{api_base}/PractitionerRole",
        {
            search_param: reference,
            "_count": str(_source_resource_page_count(source, "PractitionerRole", page_count)),
        },
    )


def _minimal_resource_id_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"resource_id": resource_id}
        for row in rows
        if (resource_id := _clean_text(row.get("resource_id")))
    ]


def _compact_linked_reference_rows(resource_type: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reference_fields = tuple(
        field
        for fields_by_target in LINKED_REFERENCE_FIELDS.get(resource_type, {}).values()
        for field in fields_by_target
    )
    compact_rows: list[dict[str, Any]] = []
    for row in rows:
        resource_id = _clean_text(row.get("resource_id"))
        if not resource_id:
            continue
        compact_row_dict: dict[str, Any] = {"resource_id": resource_id}
        for field in reference_fields:
            value = row.get(field)
            if isinstance(value, list):
                compact_values = [_clean_text(item) for item in value if _clean_text(item)]
                if compact_values:
                    compact_row_dict[field] = compact_values
            elif (cleaned := _clean_text(value)):
                compact_row_dict[field] = cleaned
        compact_rows.append(compact_row_dict)
    return compact_rows


def _load_credentials_config() -> dict[str, Any]:
    config: dict[str, Any] = {}
    path = _clean_text(_PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.get()) or _clean_text(
        os.getenv(PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV)
    )
    if path:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                config.update(payload)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            payload = None
    raw = _clean_text(os.getenv(PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV))
    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                config.update(payload)
        except json.JSONDecodeError:
            payload = None
    return config


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _source_metadata(source: dict[str, Any]) -> dict[str, Any]:
    return _json_object(source.get("metadata_json"))


def _expected_nonempty_resource_types(source: dict[str, Any]) -> set[str]:
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if api_base == CIGNA_PROVIDER_DIRECTORY_BASE:
        return set(CIGNA_EXPECTED_NONEMPTY_RESOURCES)
    configured_resources = _source_metadata(source).get(
        "provider_directory_expected_nonempty_resources"
    )
    if not isinstance(configured_resources, list):
        return set()
    return {
        resource_type
        for resource_type in (_clean_text(value) for value in configured_resources)
        if resource_type
    }


def _fail_closed_on_unexpected_empty_resource(
    source: dict[str, Any],
    resource_type: str,
    result: ResourceFetchResult,
) -> ResourceFetchResult:
    if (
        resource_type not in _expected_nonempty_resource_types(source)
        or not result.complete
        or result.error
        or result.bounded
        or result.rows_fetched > 0
        or result.rows_written > 0
    ):
        return result
    return replace(
        result,
        complete=False,
        error=CIGNA_UNEXPECTED_EMPTY_RESOURCE_ERROR,
    )


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
    metadata = _source_metadata(source)

    hosts = _mapping(config.get("hosts"))
    if host and host in {str(key).lower(): key for key in hosts}:
        key = {str(key).lower(): key for key in hosts}[host]
        spec = _merge_credential_spec(spec, _mapping(hosts.get(key)), matched_by=f"hosts:{host}")

    api_bases = _mapping(config.get("api_bases") or config.get("apiBases"))
    normalized_api_bases = {
        _canonical_base(str(key)) or str(key).rstrip("/"): key
        for key in api_bases
    }
    candidate_api_bases: list[str] = []
    _append_unique(candidate_api_bases, canonical_api_base)
    for equivalent_base in metadata.get("provider_directory_equivalent_api_bases") or []:
        _append_unique(candidate_api_bases, _canonical_base(equivalent_base))
    if canonical_api_base == AETNA_PROVIDER_DIRECTORY_DATA_BASE:
        _append_unique(candidate_api_bases, AETNA_PROVIDER_DIRECTORY_BASE)
    elif canonical_api_base == AETNA_PROVIDER_DIRECTORY_BASE:
        _append_unique(candidate_api_bases, AETNA_PROVIDER_DIRECTORY_DATA_BASE)
    for candidate_api_base in candidate_api_bases:
        if candidate_api_base and candidate_api_base in normalized_api_bases:
            key = normalized_api_bases[candidate_api_base]
            spec = _merge_credential_spec(
                spec,
                _mapping(api_bases.get(key)),
                matched_by=f"api_bases:{candidate_api_base}",
            )

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
        header = _resolve_secret_text(api_key.get("header") or api_key.get("name")) or "X-API-Key"
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
        _resolve_secret_text(oauth2.get("token_url") or oauth2.get("tokenUrl")) or "",
        _resolve_secret_text(oauth2.get("client_id") or oauth2.get("clientId")) or "",
        _resolve_secret_text(oauth2.get("scope")) or "",
        _resolve_secret_text(oauth2.get("audience")) or "",
    ]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def _fetch_oauth2_client_credentials_token_sync(oauth2: dict[str, Any], *, timeout: int = 15) -> str | None:
    token_url = _resolve_secret_text(oauth2.get("token_url") or oauth2.get("tokenUrl"))
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
    except Exception:
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
    metadata = _source_metadata(source)
    note = _clean_text(metadata.get("note"))
    if note and CMS_NON_PUBLIC_PROVIDER_DIRECTORY_RE.search(note):
        return True
    auth_type = (_clean_text(source.get("auth_type")) or "").lower()
    if not auth_type or auth_type in {"n/a", "na", "none", "open", "public"}:
        return False
    return any(marker in auth_type for marker in ("oauth", "api key", "bearer", "token", "client credential"))


def _source_uses_known_onboarding_gateway(source: dict[str, Any]) -> bool:
    return urllib.parse.urlsplit(_canonical_base(source.get("canonical_api_base") or source.get("api_base")) or "").netloc.lower() in FHIR_ONBOARDING_GATEWAY_HOSTS


def _select_resource_import_sources(
    source_rows: list[dict[str, Any]],
    *,
    valid_source_ids: set[str] | None,
    open_only: bool,
    include_auth_required: bool,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    selected: list[dict[str, Any]] = []
    metrics = {
        "source_import_sources_considered": len(source_rows),
        "source_import_sources_selected": 0,
        "source_import_skipped_missing_api_base": 0,
        "source_import_skipped_probe_not_valid": 0,
        "source_import_skipped_open_only": 0,
        "source_import_skipped_validation_status": 0,
        "source_import_skipped_auth_required_policy": 0,
        "source_import_sources_selected_live_probe_valid": 0,
        "source_import_sources_selected_declared_credentialed": 0,
        "source_import_sources_selected_auth_required_seed": 0,
    }
    for source in source_rows:
        if not _canonical_base(source.get("api_base")):
            metrics["source_import_skipped_missing_api_base"] += 1
            continue
        auth_type = (_clean_text(source.get("auth_type")) or "").lower()
        validation = (_clean_text(source.get("last_validated_status")) or "").lower()
        live_probe_valid = valid_source_ids is not None and source["source_id"] in valid_source_ids
        if valid_source_ids is not None and not live_probe_valid:
            metrics["source_import_skipped_probe_not_valid"] += 1
            continue
        if open_only and auth_type not in {"open", "none", ""} and not live_probe_valid:
            metrics["source_import_skipped_open_only"] += 1
            continue
        if valid_source_ids is None:
            if not include_auth_required and validation == "auth_required":
                metrics["source_import_skipped_auth_required_policy"] += 1
                continue
            allowed_statuses = {"", "valid"}
            if include_auth_required:
                allowed_statuses.add("auth_required")
            if validation not in allowed_statuses:
                metrics["source_import_skipped_validation_status"] += 1
                continue
        selected.append(source)
        metrics["source_import_sources_selected"] += 1
        if live_probe_valid:
            metrics["source_import_sources_selected_live_probe_valid"] += 1
        if _source_declares_credentialed_access(source):
            metrics["source_import_sources_selected_declared_credentialed"] += 1
        if validation == "auth_required":
            metrics["source_import_sources_selected_auth_required_seed"] += 1
    return selected, metrics


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


def _source_override_endpoint_fields(api_base: str) -> dict[str, str]:
    return {
        endpoint_field: f"{api_base.rstrip('/')}/{resource_type}"
        for resource_type, endpoint_field in RESOURCE_ENDPOINT_FIELDS.items()
    }


def _payer_alias_key(value: Any) -> str:
    text = (_clean_text(value) or "").lower()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _aetna_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    if api_base == AETNA_PROVIDER_DIRECTORY_DATA_BASE:
        return None
    parsed_api_base = urllib.parse.urlsplit(api_base or "")
    api_host = parsed_api_base.netloc.lower()
    api_path = parsed_api_base.path.lower()
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    should_override = (
        api_base == "https://fhir-ehr.cerner.com/r4/aetna"
        or api_base == AETNA_PROVIDER_DIRECTORY_BASE
        or (
            api_host == "vteapif1.aetna.com"
            and "/fhirdirectory/" in api_path
            and "/patientaccess" in api_path
        )
        or "developerportal.aetna.com" in portal_url
        or "developerportal.aetna.com" in source_url
    )
    if not should_override:
        return None
    auth_type = _clean_text(row.get("auth_type"))
    if (auth_type or "").strip().lower() in {"", "n/a", "na", "none", "open", "public"}:
        auth_type = "OAuth2/SMART"
    return {
        "api_base": AETNA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": AETNA_PROVIDER_DIRECTORY_BASE,
        "requires_registration": True,
        "auth_type": auth_type,
        "last_validated_status": "auth_required",
        "endpoints": _source_override_endpoint_fields(AETNA_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "aetna_apif1_providerdirectory",
            "provider_directory_override_reason": (
                "Aetna developer portal metadata is open, but resource reads require OAuth client credentials."
            ),
            "provider_directory_confirmed_base": AETNA_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_token_url": (
                AETNA_PROVIDER_DIRECTORY_TOKEN_URL
            ),
            "provider_directory_equivalent_api_bases": [
                AETNA_PROVIDER_DIRECTORY_DATA_BASE,
            ],
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
        },
    }


def _aetna_provider_directory_data_seed_rows(*, source_query: str | None = None) -> list[dict[str, Any]]:
    row = {
        "id": "aetna-provider-directory-commercial-medicare-bulk",
        "org_name": "Aetna",
        "plan_name": "Aetna Commercial and Medicare Provider Directory",
        "api_base": AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "auth_type": "OAuth2/SMART",
        "last_validated_status": "auth_required",
        "requires_registration": True,
        "source": "aetna-developer-portal",
        "source_detail": "official Aetna Commercial and Medicare Provider Directory FHIR server",
        "source_url": "https://developerportal.aetna.com/apis",
        "note": (
            "Supplemental Aetna source for Commercial and Medicare Provider Directory data. "
            "This base supports Bulk Data $export with OAuth2 client credentials."
        ),
        "metadata_json": {
            "provider_directory_override": "aetna_apif1_providerdirectorydata",
            "provider_directory_confirmed_base": AETNA_PROVIDER_DIRECTORY_DATA_BASE,
            "provider_directory_confirmed_token_url": AETNA_PROVIDER_DIRECTORY_TOKEN_URL,
            "provider_directory_equivalent_api_bases": [
                AETNA_PROVIDER_DIRECTORY_BASE,
            ],
            "provider_directory_bulk_export_omit_output_format": True,
        },
    }
    return [row] if _seed_row_matches_query(row, source_query) else []


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
        "endpoints": _source_override_endpoint_fields(ALOHR_FHIR_PROVIDER_DIRECTORY_BASE),
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


def _cigna_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    org_name = (_clean_text(row.get("org_name")) or "").lower()
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    should_override = (
        api_base == CIGNA_PROVIDER_DIRECTORY_BASE
        or api_base == "https://apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4"
        or "fhir.cigna.com/providerdirectory" in (api_base or "").lower()
        or (
            "cigna" in org_name
            and (
                "apps.availity.com/availity/public-fhir/fhir/v1/cigna/r4" in (api_base or "").lower()
                or "fhir.cigna.com" in portal_url
                or "fhir.cigna.com" in source_url
            )
        )
    )
    if not should_override:
        return None
    return {
        "api_base": CIGNA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": CIGNA_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "endpoints": _source_override_endpoint_fields(CIGNA_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "cigna_public_providerdirectory",
            "provider_directory_override_reason": (
                "Cigna publishes public R4 Provider Directory metadata and resources at fhir.cigna.com; "
                "upstream seed rows can point at Availity non-FHIR/onboarding paths."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": CIGNA_PROVIDER_DIRECTORY_BASE,
            "provider_directory_expected_nonempty_resources": sorted(
                CIGNA_EXPECTED_NONEMPTY_RESOURCES
            ),
            "provider_directory_supported_resources": list(DEFAULT_RESOURCES),
        },
    }


def _centene_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    source_detail = (_clean_text(row.get("source_detail")) or "").lower()
    should_override = (
        api_base in {CENTENE_PARTNER_PORTAL_APIS_URL, CENTENE_PROVIDER_DIRECTORY_BASE, CENTENE_STALE_GENERIC_BASE}
        or "partners.centene.com/apis" in portal_url
        or "partners.centene.com/apis" in source_url
        or urllib.parse.urlsplit(api_base or "").netloc.lower() == "fhir.centene.com"
        or "partner portal" in source_detail and "centene" in source_detail
    )
    if not should_override:
        return None
    return {
        "api_base": CENTENE_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": CENTENE_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "endpoints": _source_override_endpoint_fields(CENTENE_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "centene_iopc_pd_providerdirectory",
            "provider_directory_override_reason": (
                "Centene's partner portal catalog publishes the concrete public FHIR Provider Directory "
                "production host and path; seed rows can point at the portal API catalog page instead."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": CENTENE_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_catalog_url": CENTENE_PARTNER_PORTAL_APIS_URL,
            "provider_directory_confirmed_doc_url": CENTENE_PROVIDER_DIRECTORY_DOC_URL,
            "provider_directory_replaces_stale_generic_api_bases": [CENTENE_STALE_GENERIC_BASE],
            "provider_directory_special_case_california_base": CENTENE_PROVIDER_DIRECTORY_CALIFORNIA_BASE,
            "provider_directory_resource_probe_caveat": (
                "Official Centene documentation publishes this Provider Directory base, but live "
                "metadata/resource probes can return CloudFront HTTP 403 from non-allowlisted networks; "
                "normal probe validation still gates resource import."
            ),
        },
    }


def _amerihealth_caritas_plan_code(row: dict[str, Any]) -> str | None:
    plan_name_key = _payer_alias_key(row.get("plan_name"))
    if plan_name_key in AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS:
        return AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS[plan_name_key]
    org_name_key = _payer_alias_key(row.get("org_name"))
    if org_name_key in AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS:
        return AMERIHEALTH_CARITAS_PLAN_CODES_BY_ALIAS[org_name_key]
    return None


def _amerihealth_caritas_provider_directory_base(plan_code: str) -> str:
    return f"{AMERIHEALTH_CARITAS_PROVIDER_API_PREFIX}/{plan_code}/provider-api"


def _amerihealth_caritas_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    org_name = (_clean_text(row.get("org_name")) or "").lower()
    plan_code = _amerihealth_caritas_plan_code(row)
    parsed_api_base = urllib.parse.urlsplit(api_base or "")
    api_host = parsed_api_base.netloc.lower()
    api_path = parsed_api_base.path.lower()
    if api_host == "api-ext.amerihealthcaritas.com" and "/provider-api" in api_path:
        return None
    should_override = (
        plan_code
        and (
            "amerihealth caritas" in org_name
            or api_host in {"apps.availity.com", "fhir.amerihealthcaritas.com"}
            or "amerihealthcaritas.com" in portal_url
            or "amerihealthcaritas.com" in source_url
        )
    )
    if not should_override:
        return None
    provider_base = _amerihealth_caritas_provider_directory_base(plan_code)
    return {
        "api_base": provider_base,
        "canonical_api_base": provider_base,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(provider_base),
        "metadata": {
            "provider_directory_override": "amerihealth_caritas_api_ext_provider_api",
            "provider_directory_override_reason": (
                "AmeriHealth Caritas publishes plan-specific public Provider Directory FHIR bases "
                "at api-ext.amerihealthcaritas.com; seed rows can point at stale Availity or "
                "fhir.amerihealthcaritas.com paths."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": provider_base,
            "provider_directory_confirmed_metadata_url": f"{provider_base}/metadata",
            "provider_directory_confirmed_catalog_url": AMERIHEALTH_CARITAS_DOC_URL,
            "provider_directory_plan_code": plan_code,
        },
    }


def _molina_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    org_name = (_clean_text(row.get("org_name")) or "").lower()
    parsed_api_base = urllib.parse.urlsplit(api_base or "")
    api_host = parsed_api_base.netloc.lower()
    api_path = parsed_api_base.path.lower()
    should_override = (
        api_base in {MOLINA_DEVELOPER_PORTAL_URL, MOLINA_PROVIDER_DIRECTORY_BASE}
        or (
            api_host == "fhir.molinahealthcare.com"
            and api_path.rstrip("/") == "/provider-directory"
        )
        or "developer.interop.molinahealthcare.com" in portal_url
        or "developer.interop.molinahealthcare.com" in source_url
        or ("molina" in org_name and api_base == MOLINA_DEVELOPER_PORTAL_URL)
    )
    if not should_override:
        return None
    return {
        "api_base": MOLINA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": MOLINA_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(MOLINA_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "molina_interop_providerdirectory",
            "provider_directory_override_reason": (
                "Molina's developer portal documents a public Provider Directory API, while the "
                "working FHIR metadata base is api.interop.molinahealthcare.com/providerdirectory; "
                "seed and retest rows can point at the portal or stale fhir.molinahealthcare.com host."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": MOLINA_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_catalog_url": MOLINA_DEVELOPER_PORTAL_URL,
            "provider_directory_confirmed_metadata_url": MOLINA_PROVIDER_DIRECTORY_METADATA_URL,
            "provider_directory_supported_resources": [
                "Location",
                "Organization",
                "OrganizationAffiliation",
                "Practitioner",
                "PractitionerRole",
            ],
            "provider_directory_resource_probe_caveat": (
                "Live declared resources are public; generated searches for undeclared resource "
                "types return HTTP 500 and are intentionally skipped."
            ),
        },
    }


def _uhc_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    org_name = (_clean_text(row.get("org_name")) or "").lower()
    parsed_api_base = urllib.parse.urlsplit(api_base or "")
    api_host = parsed_api_base.netloc.lower()
    api_path = parsed_api_base.path.lower()
    should_override = (
        api_base in {
            UHC_INTEROPERABILITY_APIS_URL,
            f"{UHC_INTEROPERABILITY_APIS_URL}/patient-access-api",
            UHC_PROVIDER_DIRECTORY_BASE,
        }
        or (
            api_host == "fhir.uhc.com"
            and api_path.rstrip("/") == "/v1/provider-directory"
        )
        or "uhc.com/legal/interoperability-apis" in portal_url
        or "uhc.com/legal/interoperability-apis" in source_url
        or ("unitedhealthcare" in org_name and "uhc.com/legal/interoperability-apis" in (api_base or ""))
    )
    if not should_override:
        return None
    return {
        "api_base": UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": UHC_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(UHC_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "uhc_flex_optum_fhirpublic_r4",
            "provider_directory_override_reason": (
                "UnitedHealthcare publishes Provider Directory Core metadata under the Optum FLEX "
                "public R4 FHIR base; seed rows can point at the UHC interoperability landing page."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": UHC_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_catalog_url": UHC_INTEROPERABILITY_APIS_URL,
            "provider_directory_confirmed_metadata_url": UHC_PROVIDER_DIRECTORY_METADATA_URL,
            "provider_directory_resource_page_count_caps": {
                "InsurancePlan": 1,
            },
        },
    }


def _state_public_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    if api_base == TMHP_PROVIDER_DIRECTORY_BASE:
        return {
            "api_base": TMHP_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": TMHP_PROVIDER_DIRECTORY_BASE,
            "requires_registration": False,
            "auth_type": "none",
            "endpoints": _source_override_endpoint_fields(TMHP_PROVIDER_DIRECTORY_BASE),
            "metadata": {
                "provider_directory_override": "tmhp_public_providerdirectory",
                "provider_directory_override_reason": (
                    "Texas TMHP publishes public Provider Directory FHIR metadata and resource bundles; "
                    "seed rows can retain stale OAuth/SMART labels."
                ),
                "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
                "provider_directory_confirmed_base": TMHP_PROVIDER_DIRECTORY_BASE,
                "provider_directory_confirmed_metadata_url": TMHP_PROVIDER_DIRECTORY_METADATA_URL,
            },
        }
    if api_base == NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE:
        return {
            "api_base": NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
            "canonical_api_base": NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
            "requires_registration": False,
            "auth_type": "none",
            "endpoints": _source_override_endpoint_fields(NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE),
            "metadata": {
                "provider_directory_override": "nebraska_dhhs_public_providerdirectory",
                "provider_directory_override_reason": (
                    "Nebraska DHHS publishes public Provider Directory FHIR metadata and resource bundles; "
                    "seed rows can retain stale OAuth/SMART labels."
                ),
                "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
                "provider_directory_confirmed_base": NEBRASKA_DHHS_PROVIDER_DIRECTORY_BASE,
                "provider_directory_confirmed_metadata_url": NEBRASKA_DHHS_PROVIDER_DIRECTORY_METADATA_URL,
            },
        }
    return None


def _hap_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    org_key = _payer_alias_key(row.get("org_name"))
    parsed_api_base = urllib.parse.urlsplit(api_base or "")
    api_host = parsed_api_base.netloc.lower()
    api_path = parsed_api_base.path.lower()
    should_override = (
        api_base in {HAP_PROVIDER_DIRECTORY_DOC_URL, HAP_PROVIDER_DIRECTORY_BASE}
        or (
            api_host == "api.hap.org"
            and api_path.rstrip("/") in {"/providerdirectoryapi", "/fhir/provider-directory"}
        )
        or "api.hap.org/providerdirectoryapi" in portal_url
        or "api.hap.org/providerdirectoryapi" in source_url
        or org_key in {"hap", "health alliance plan"}
    )
    if not should_override:
        return None
    return {
        "api_base": HAP_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": HAP_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(HAP_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "hap_provider_directory_r4",
            "provider_directory_override_reason": (
                "HAP's developer portal publishes a public no-auth Provider Directory production "
                "base at provider-directory-r4.api.hap.org; seed rows can point at the docs page, "
                "a stale api.hap.org/fhir/provider-directory path, or contain only the HAP payer name."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": HAP_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_catalog_url": HAP_PROVIDER_DIRECTORY_DOC_URL,
            "provider_directory_confirmed_metadata_url": HAP_PROVIDER_DIRECTORY_METADATA_URL,
        },
    }


def _humana_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    source_detail = (_clean_text(row.get("source_detail")) or "").lower()
    should_override = (
        api_base == HUMANA_PROVIDER_DIRECTORY_BASE
        or bool(api_base and api_base.startswith(f"{HUMANA_PROVIDER_DIRECTORY_BASE}/"))
        or "fhir.humana.com/api" in portal_url
        or "fhir.humana.com/api" in source_url
        or ("humana" in source_detail and "fhir.humana.com" in source_detail)
    )
    if not should_override:
        return None
    return {
        "api_base": HUMANA_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": HUMANA_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(HUMANA_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "humana_public_fhir_api",
            "provider_directory_override_reason": (
                "Humana publishes public R4 Provider Directory metadata and resource search "
                "collections at fhir.humana.com/api; seed rows can retain stale OAuth labels."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": HUMANA_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_metadata_url": HUMANA_PROVIDER_DIRECTORY_METADATA_URL,
            "provider_directory_supported_resources": [
                "InsurancePlan",
                "Location",
                "Organization",
                "Practitioner",
                "PractitionerRole",
            ],
        },
    }


def _iehp_provider_directory_override(seed_row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(seed_row.get("api_base"))
    org_name = (_clean_text(seed_row.get("org_name")) or "").lower()
    portal_url = (_clean_text(seed_row.get("portal_url")) or "").lower()
    source_url = (_clean_text(seed_row.get("source_url")) or "").lower()
    should_override = (
        bool(api_base and api_base.startswith(IEHP_PROVIDER_DIRECTORY_BASE))
        or "fhir.iehp.org/provider-directory" in portal_url
        or "fhir.iehp.org/provider-directory" in source_url
        or "inland empire health plan" in org_name
    )
    if not should_override:
        return None
    return {
        "api_base": IEHP_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": IEHP_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(IEHP_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "iehp_public_provider_directory",
            "provider_directory_override_reason": (
                "IEHP publishes one public R4 Provider Directory base; seed aliases can contain "
                "double-slash resource paths that return HTTP 400."
            ),
            "provider_directory_previous_api_base": _clean_text(seed_row.get("api_base")),
            "provider_directory_confirmed_base": IEHP_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_metadata_url": IEHP_PROVIDER_DIRECTORY_METADATA_URL,
            "provider_directory_supported_resources": [
                "HealthcareService",
                "InsurancePlan",
                "Location",
                "Organization",
                "Practitioner",
                "PractitionerRole",
            ],
        },
    }


def _scan_provider_directory_override(row: dict[str, Any]) -> dict[str, Any] | None:
    api_base = _canonical_base(row.get("api_base"))
    portal_url = (_clean_text(row.get("portal_url")) or "").lower()
    source_url = (_clean_text(row.get("source_url")) or "").lower()
    org_name = (_clean_text(row.get("org_name")) or "").lower()
    should_override = (
        api_base in {SCAN_DEVELOPER_PORTAL_URL, SCAN_PROVIDER_DIRECTORY_BASE}
        or "developer.scanhealthplan.com" in portal_url
        or "developer.scanhealthplan.com" in source_url
        or ("scan health plan" in org_name and "developer.scanhealthplan.com" in (api_base or ""))
    )
    if not should_override:
        return None
    return {
        "api_base": SCAN_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": SCAN_PROVIDER_DIRECTORY_BASE,
        "requires_registration": False,
        "auth_type": "none",
        "last_validated_status": "valid",
        "endpoints": _source_override_endpoint_fields(SCAN_PROVIDER_DIRECTORY_BASE),
        "metadata": {
            "provider_directory_override": "scan_providerdirectory_intersystems",
            "provider_directory_override_reason": (
                "SCAN's developer portal embeds the Provider Directory OpenAPI spec with "
                "providerdirectory.scanhealthplan.com as the FHIR server."
            ),
            "provider_directory_previous_api_base": _clean_text(row.get("api_base")),
            "provider_directory_confirmed_base": SCAN_PROVIDER_DIRECTORY_BASE,
            "provider_directory_confirmed_catalog_url": SCAN_DEVELOPER_PORTAL_URL,
            "provider_directory_confirmed_doc_url": SCAN_PROVIDER_DIRECTORY_DOC_URL,
        },
    }


def _source_row_from_seed(row: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    api_base = _clean_text(row.get("api_base"))
    canonical_api_base = _canonical_base(api_base)
    metadata = {
        "external_seed_id": _clean_text(row.get("id")),
        "note": _clean_text(row.get("note")),
    }
    seed_metadata = _json_object(row.get("metadata_json"))
    if seed_metadata:
        metadata.update(seed_metadata)
        metadata["external_seed_id"] = _clean_text(row.get("id"))
        metadata["note"] = _clean_text(row.get("note"))
    if _clean_text(row.get("source")) == "amerihealth-caritas-developer-portal":
        metadata["provider_directory_replaces_stale_generic_api_bases"] = [
            AMERIHEALTH_CARITAS_STALE_GENERIC_BASE
        ]
        metadata["provider_directory_confirmed_catalog_url"] = AMERIHEALTH_CARITAS_DOC_URL
    override = (
        _aetna_provider_directory_override(row)
        or _alohr_provider_directory_override(row)
        or _cigna_provider_directory_override(row)
        or _centene_provider_directory_override(row)
        or _amerihealth_caritas_provider_directory_override(row)
        or _molina_provider_directory_override(row)
        or _uhc_provider_directory_override(row)
        or _state_public_provider_directory_override(row)
        or _hap_provider_directory_override(row)
        or _humana_provider_directory_override(row)
        or _iehp_provider_directory_override(row)
        or _scan_provider_directory_override(row)
    )
    if override:
        api_base = override["api_base"]
        canonical_api_base = override["canonical_api_base"]
        metadata = {**metadata, **override["metadata"]}
    endpoint_overrides = override.get("endpoints", {}) if override else {}
    parent_api_base = _resource_or_metadata_parent_base(api_base)
    if parent_api_base and _canonical_base(parent_api_base) != canonical_api_base:
        previous_api_base = api_base
        metadata.setdefault("provider_directory_previous_api_base", api_base)
        metadata["provider_directory_base_normalization"] = "resource_or_metadata_parent_base"
        metadata["provider_directory_confirmed_base"] = parent_api_base
        endpoint_overrides = _rebased_derived_endpoint_fields(
            row,
            endpoint_overrides,
            previous_api_base,
            parent_api_base,
        )
        api_base = parent_api_base
        canonical_api_base = _canonical_base(parent_api_base)
    source_id = _stable_source_id(
        {
            **row,
            "api_base": api_base,
        }
    )
    return {
        "source_id": source_id,
        "org_tin": _clean_text(row.get("org_tin")),
        "org_name": _clean_text(row.get("org_name")) or "Unknown payer",
        "plan_name": _clean_text(row.get("plan_name")),
        "portal_url": _clean_text(row.get("portal_url")),
        "api_base": api_base,
        "canonical_api_base": canonical_api_base,
        "endpoint_insurance_plan": _clean_text(
            endpoint_overrides.get("endpoint_insurance_plan")
            if "endpoint_insurance_plan" in endpoint_overrides
            else row.get("endpoint_insurance_plan")
        ),
        "endpoint_practitioner": _clean_text(
            endpoint_overrides.get("endpoint_practitioner")
            if "endpoint_practitioner" in endpoint_overrides
            else row.get("endpoint_practitioner")
        ),
        "endpoint_practitioner_role": _clean_text(
            endpoint_overrides.get("endpoint_practitioner_role")
            if "endpoint_practitioner_role" in endpoint_overrides
            else row.get("endpoint_practitioner_role")
        ),
        "endpoint_organization": _clean_text(
            endpoint_overrides.get("endpoint_organization")
            if "endpoint_organization" in endpoint_overrides
            else row.get("endpoint_organization")
        ),
        "endpoint_organization_affiliation": _clean_text(
            endpoint_overrides.get("endpoint_organization_affiliation")
            if "endpoint_organization_affiliation" in endpoint_overrides
            else row.get("endpoint_organization_affiliation")
        ),
        "endpoint_location": _clean_text(
            endpoint_overrides.get("endpoint_location")
            if "endpoint_location" in endpoint_overrides
            else row.get("endpoint_location")
        ),
        "endpoint_healthcare_service": _clean_text(
            endpoint_overrides.get("endpoint_healthcare_service")
            if "endpoint_healthcare_service" in endpoint_overrides
            else row.get("endpoint_healthcare_service")
        ),
        "endpoint_network": _clean_text(
            endpoint_overrides.get("endpoint_network")
            if "endpoint_network" in endpoint_overrides
            else row.get("endpoint_network")
        ),
        "endpoint_endpoint": _clean_text(
            endpoint_overrides.get("endpoint_endpoint")
            if "endpoint_endpoint" in endpoint_overrides
            else row.get("endpoint_endpoint")
        ),
        "requires_registration": (
            bool(override["requires_registration"])
            if override and "requires_registration" in override
            else bool(_bool_from_seed(row.get("requires_registration")))
        ),
        "requires_api_key": bool(_bool_from_seed(row.get("requires_api_key"))),
        "auth_type": override.get("auth_type") if override else _clean_text(row.get("auth_type")),
        "last_validated": _clean_text(row.get("last_validated")),
        "last_validated_status": (
            _clean_text(override.get("last_validated_status"))
            if override and "last_validated_status" in override
            else _clean_text(row.get("last_validated_status"))
        ),
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


def _npi_from_resource_id(resource_id: str | None) -> int | None:
    text = _clean_text(resource_id)
    if not text or not re.fullmatch(r"[0-9]{10}", text):
        return None
    return int(text)


def _resource_npi(resource: dict[str, Any]) -> int | None:
    return _npi(resource) or _npi_from_resource_id(_resource_id(resource))


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
    canonical = canonicalize_contact_batch([(telephone_number, fax_number, country_code)])[0]
    phone_number = canonical.get("phone_number")
    fax_number_digits = canonical.get("fax_number_digits") or canonical.get("fax_number")
    phone_extension = canonical.get("phone_extension")
    fax_extension = canonical.get("fax_extension")
    return {
        "phone_number": phone_number if isinstance(phone_number, str) else None,
        "fax_number_digits": fax_number_digits if isinstance(fax_number_digits, str) else None,
        "phone_extension": phone_extension if isinstance(phone_extension, str) else None,
        "fax_extension": fax_extension if isinstance(fax_extension, str) else None,
    }


def _attach_location_contact_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows
    canonical_rows = canonicalize_contact_batch(
        (
            row.get("telephone_number"),
            row.get("fax_number"),
            row.get("country_code") or "US",
        )
        for row in rows
    )
    for row, canonical in zip(rows, canonical_rows):
        phone_number = canonical.get("phone_number")
        fax_number_digits = canonical.get("fax_number_digits") or canonical.get("fax_number")
        phone_extension = canonical.get("phone_extension")
        fax_extension = canonical.get("fax_extension")
        row["phone_number"] = phone_number if isinstance(phone_number, str) else None
        row["fax_number_digits"] = fax_number_digits if isinstance(fax_number_digits, str) else None
        row["phone_extension"] = phone_extension if isinstance(phone_extension, str) else None
        row["fax_extension"] = fax_extension if isinstance(fax_extension, str) else None
    return rows


def _location_contact_fields_missing(rows: list[dict[str, Any]]) -> bool:
    contact_keys = ("phone_number", "phone_extension", "fax_number_digits", "fax_extension")
    return any(
        (row.get("telephone_number") or row.get("fax_number"))
        and any(key not in row for key in contact_keys)
        for row in rows
    )


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
    country_code = _normalize_country_code(first.get("country"))
    position = resource.get("position") if isinstance(resource.get("position"), dict) else {}
    latitude, longitude = _normalize_fhir_position(position, country_code=country_code)
    return {
        "first_line": lines[0] if lines else None,
        "second_line": lines[1] if len(lines) > 1 else None,
        "city_name": city,
        "state_name": state,
        "state_code": state_code,
        "postal_code": postal_code,
        "zip5": zip5,
        "city_norm": city.upper() if city else None,
        "country_code": country_code,
        "latitude": latitude,
        "longitude": longitude,
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
    normalize_location_contacts: bool = True,
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
            "npi": _resource_npi(resource),
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
            "npi": _resource_npi(resource),
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
            "telecom": telecom,
            **{key: value for key, value in address.items() if key != "address_json"},
        }
        if normalize_location_contacts:
            row.update(_location_contact_fields(telephone_number, fax_number, address.get("country_code")))
        return ProviderDirectoryLocation, row
    if resource_type == "PractitionerRole":
        period_start, period_end = _period(resource)
        row = {
            **base,
            "npi": _npi(resource),
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
        normalize_location_contacts=resource.get("resourceType") != "Location",
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


def provider_directory_address_corroboration_sql(
    db_schema: str | None = None,
    *,
    view_name: str = PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
) -> str:
    """Build a view that corroborates role-bound Provider Directory addresses.

    The compact address overlay is the driving relation, scoped to role and
    affiliation rows that can carry network context. Direct Organization
    address evidence stays in provider_directory_address_overlay for provider
    APIs, but is intentionally excluded from this PTG corroboration artifact.
    """

    schema = db_schema or _schema()
    view_ref = _qt(schema, view_name)
    plan_ref_resource_id_expr = _sql_reference_resource_id("plan_ref.value", "InsurancePlan")
    network_ref_resource_id_expr = _sql_reference_resource_id("network_ref.value", "Organization")
    return f"""
    CREATE OR REPLACE VIEW {view_ref} AS
    WITH address_candidates AS (
        SELECT
            overlay.source_record_id::varchar AS source_record_id,
            overlay.source_id::varchar AS source_id,
            overlay.resource_type::varchar AS resource_type,
            overlay.resource_id::varchar AS resource_id,
            CASE
                WHEN overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')
                    THEN NULLIF(split_part(overlay.source_record_id, ':', 5), '')
                ELSE NULL::varchar
            END AS location_resource_id,
            overlay.npi::bigint AS npi,
            NULL::varchar AS location_key,
            overlay.address_key::uuid AS address_key,
            NULLIF(LEFT(regexp_replace(COALESCE(overlay.postal_code, ''), '\\D', '', 'g'), 5), '')::varchar AS zip5,
            overlay.state_code::varchar AS state_code,
            NULLIF(regexp_replace(upper(BTRIM(COALESCE(overlay.city_name, ''))), '[^A-Z0-9]+', '', 'g'), '')::varchar AS city_norm,
            overlay.telephone_number::varchar AS telephone_number,
            overlay.phone_number::varchar AS phone_number,
            overlay.fax_number::varchar AS fax_number,
            overlay.fax_number_digits::varchar AS fax_number_digits,
            overlay.source_updated_at AS source_updated_at
          FROM {_qt(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)} overlay
         WHERE overlay.npi IS NOT NULL
           AND overlay.address_key IS NOT NULL
           AND overlay.resource_type IN ('PractitionerRole', 'OrganizationAffiliation')
    ),
    practitioner_matches AS (
        SELECT
            NULL::varchar AS source_key,
            NULL::varchar AS snapshot_id,
            NULL::varchar AS plan_id,
            NULL::varchar AS ptg_plan_id,
            e.npi,
            e.location_key,
            e.address_key,
            e.zip5,
            e.state_code,
            e.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            practitioner.resource_id AS provider_directory_provider_resource_id,
            practitioner.full_name AS provider_directory_provider_name,
            role.resource_id AS provider_directory_role_resource_id,
            COALESCE(loc.resource_id, e.location_resource_id) AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            COALESCE(loc.telephone_number, e.telephone_number) AS provider_directory_telephone_number,
            COALESCE(loc.phone_number, e.phone_number) AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            COALESCE(loc.fax_number, e.fax_number) AS provider_directory_fax_number,
            COALESCE(loc.fax_number_digits, e.fax_number_digits) AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            COALESCE(role.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs,
            COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb) AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            false AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            COALESCE(plan_context.provider_directory_insurance_plan_matches, '[]'::jsonb)
                AS provider_directory_insurance_plan_matches,
            COALESCE(role.specialty_codes::jsonb, '[]'::jsonb) AS provider_directory_specialty_codes,
            COALESCE(role.code_codes::jsonb, '[]'::jsonb) AS provider_directory_role_codes,
            'practitioner_role'::varchar AS provider_directory_match_type,
            (role.active IS DISTINCT FROM false) AS provider_directory_active_role,
            (practitioner.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.resource_id IS NULL OR loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(e.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(role.observed_at, TIMESTAMP 'epoch'),
                COALESCE(practitioner.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM address_candidates e
          JOIN {_qt(schema, "provider_directory_practitioner_role")} role
            ON e.resource_type = 'PractitionerRole'
           AND role.source_id = e.source_id
           AND role.resource_id = e.resource_id
          JOIN {_qt(schema, "provider_directory_practitioner")} practitioner
            ON practitioner.source_id = role.source_id
           AND practitioner.resource_id = NULLIF(regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''), '')
           AND COALESCE(practitioner.npi, role.npi) = e.npi
          LEFT JOIN {_qt(schema, "provider_directory_location")} loc
            ON loc.source_id = e.source_id
           AND loc.resource_id = e.location_resource_id
          LEFT JOIN LATERAL (
            SELECT
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
                  COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb)
              ) AS plan_ref(value)
              LEFT JOIN {_qt(schema, "provider_directory_insurance_plan")} insurance_plan
                ON insurance_plan.source_id = role.source_id
               AND insurance_plan.resource_id = {plan_ref_resource_id_expr}
          ) plan_context ON TRUE
          LEFT JOIN LATERAL (
            WITH network_ref_values AS (
                SELECT network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.network_refs::jsonb, '[]'::jsonb)
                  ) AS network_ref(value)
                UNION
                SELECT plan_network_ref.value
                  FROM jsonb_array_elements_text(
                      COALESCE(role.insurance_plan_refs::jsonb, '[]'::jsonb)
                  ) AS plan_ref(value)
                  JOIN {_qt(schema, "provider_directory_insurance_plan")} insurance_plan
                    ON insurance_plan.source_id = role.source_id
                   AND insurance_plan.resource_id = {plan_ref_resource_id_expr}
                  CROSS JOIN LATERAL jsonb_array_elements_text(
                      COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
                  ) AS plan_network_ref(value)
            )
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_catalog.provider_directory_network_name)
                        FILTER (WHERE NULLIF(BTRIM(network_catalog.provider_directory_network_name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_catalog.network_resource_id,
                            'name', network_catalog.provider_directory_network_name,
                            'aliases', COALESCE(network_catalog.aliases::jsonb, '[]'::jsonb),
                            'provider_directory_network_key', network_catalog.provider_directory_network_key,
                            'provider_directory_source', 'provider_directory_fhir',
                            'provider_directory_source_id', role.source_id,
                            'provider_directory_org_name', network_catalog.source_org_name,
                            'provider_directory_plan_name', network_catalog.source_plan_name,
                            'provider_directory_issuer_key', network_catalog.provider_directory_issuer_key,
                            'provider_directory_issuer_network_match_key',
                                network_catalog.provider_directory_issuer_network_match_key
                        )
                    ) FILTER (WHERE network_catalog.network_resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM network_ref_values network_ref
              LEFT JOIN {_qt(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)} network_catalog
                ON network_catalog.source_id = role.source_id
               AND network_catalog.network_resource_id = {network_ref_resource_id_expr}
          ) network_context ON TRUE
          JOIN {_qt(schema, "provider_directory_source")} src
            ON src.source_id = role.source_id
         WHERE e.npi IS NOT NULL
           AND e.address_key IS NOT NULL
    ),
    organization_affiliation_matches AS (
        SELECT
            NULL::varchar AS source_key,
            NULL::varchar AS snapshot_id,
            NULL::varchar AS plan_id,
            NULL::varchar AS ptg_plan_id,
            e.npi,
            e.location_key,
            e.address_key,
            e.zip5,
            e.state_code,
            e.city_norm,
            src.source_id AS provider_directory_source_id,
            src.org_name AS provider_directory_org_name,
            src.plan_name AS provider_directory_plan_name,
            organization.resource_id AS provider_directory_provider_resource_id,
            organization.name AS provider_directory_provider_name,
            affiliation.resource_id AS provider_directory_role_resource_id,
            COALESCE(loc.resource_id, e.location_resource_id) AS provider_directory_location_resource_id,
            loc.name AS provider_directory_location_name,
            COALESCE(loc.telephone_number, e.telephone_number) AS provider_directory_telephone_number,
            COALESCE(loc.phone_number, e.phone_number) AS provider_directory_phone_number,
            loc.phone_extension AS provider_directory_phone_extension,
            COALESCE(loc.fax_number, e.fax_number) AS provider_directory_fax_number,
            COALESCE(loc.fax_number_digits, e.fax_number_digits) AS provider_directory_fax_number_digits,
            loc.fax_extension AS provider_directory_fax_extension,
            COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb) AS provider_directory_network_refs,
            '[]'::jsonb AS provider_directory_insurance_plan_refs,
            COALESCE(network_context.provider_directory_network_names, ARRAY[]::varchar[])
                AS provider_directory_network_names,
            COALESCE(network_context.provider_directory_network_matches, '[]'::jsonb)
                AS provider_directory_network_matches,
            false AS provider_directory_plan_context_matched,
            COALESCE(network_context.provider_directory_network_context_present, false)
                AS provider_directory_network_context_present,
            '[]'::jsonb AS provider_directory_insurance_plan_matches,
            COALESCE(affiliation.specialty_codes::jsonb, '[]'::jsonb) AS provider_directory_specialty_codes,
            COALESCE(affiliation.code_codes::jsonb, '[]'::jsonb) AS provider_directory_role_codes,
            'organization_affiliation'::varchar AS provider_directory_match_type,
            (affiliation.active IS DISTINCT FROM false) AS provider_directory_active_role,
            (organization.active IS DISTINCT FROM false) AS provider_directory_active_provider,
            (loc.resource_id IS NULL OR loc.status IS NULL OR lower(loc.status) <> 'inactive') AS provider_directory_active_location,
            GREATEST(
                COALESCE(e.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(affiliation.observed_at, TIMESTAMP 'epoch'),
                COALESCE(organization.observed_at, TIMESTAMP 'epoch'),
                COALESCE(loc.observed_at, TIMESTAMP 'epoch')
            ) AS provider_directory_observed_at
          FROM address_candidates e
          JOIN {_qt(schema, "provider_directory_organization_affiliation")} affiliation
            ON e.resource_type = 'OrganizationAffiliation'
           AND affiliation.source_id = e.source_id
           AND affiliation.resource_id = e.resource_id
          JOIN LATERAL (
              SELECT DISTINCT normalized_ref AS resource_id
                FROM (
                    VALUES
                        (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                        (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                ) AS refs(normalized_ref)
               WHERE normalized_ref IS NOT NULL
          ) AS organization_ref ON TRUE
          JOIN {_qt(schema, "provider_directory_organization")} organization
            ON organization.source_id = affiliation.source_id
           AND organization.resource_id = organization_ref.resource_id
           AND organization.npi = e.npi
          LEFT JOIN {_qt(schema, "provider_directory_location")} loc
            ON loc.source_id = e.source_id
           AND loc.resource_id = e.location_resource_id
          LEFT JOIN LATERAL (
            SELECT
                bool_or(network_ref.value IS NOT NULL) AS provider_directory_network_context_present,
                COALESCE(
                    array_agg(DISTINCT network_catalog.provider_directory_network_name)
                        FILTER (WHERE NULLIF(BTRIM(network_catalog.provider_directory_network_name), '') IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS provider_directory_network_names,
                COALESCE(
                    jsonb_agg(
                        DISTINCT jsonb_build_object(
                            'ref', network_ref.value,
                            'resource_id', network_catalog.network_resource_id,
                            'name', network_catalog.provider_directory_network_name,
                            'aliases', COALESCE(network_catalog.aliases::jsonb, '[]'::jsonb),
                            'provider_directory_network_key', network_catalog.provider_directory_network_key,
                            'provider_directory_source', 'provider_directory_fhir',
                            'provider_directory_source_id', affiliation.source_id,
                            'provider_directory_org_name', network_catalog.source_org_name,
                            'provider_directory_plan_name', network_catalog.source_plan_name,
                            'provider_directory_issuer_key', network_catalog.provider_directory_issuer_key,
                            'provider_directory_issuer_network_match_key',
                                network_catalog.provider_directory_issuer_network_match_key
                        )
                    ) FILTER (WHERE network_catalog.network_resource_id IS NOT NULL),
                    '[]'::jsonb
                ) AS provider_directory_network_matches
              FROM jsonb_array_elements_text(
                  COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
              ) AS network_ref(value)
              LEFT JOIN {_qt(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)} network_catalog
                ON network_catalog.source_id = affiliation.source_id
               AND network_catalog.network_resource_id = {network_ref_resource_id_expr}
          ) network_context ON TRUE
          JOIN {_qt(schema, "provider_directory_source")} src
            ON src.source_id = affiliation.source_id
         WHERE e.npi IS NOT NULL
           AND e.address_key IS NOT NULL
    ),
    matches AS (
        SELECT * FROM practitioner_matches
        UNION ALL
        SELECT * FROM organization_affiliation_matches
    )
    SELECT
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
            'org_name', provider_directory_org_name,
            'plan_name', provider_directory_plan_name,
            'provider_directory_source_id', provider_directory_source_id,
            'provider_directory_org_name', provider_directory_org_name,
            'provider_directory_plan_name', provider_directory_plan_name,
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


async def publish_provider_directory_address_corroboration_view(db_schema: str | None = None) -> None:
    await db.status(provider_directory_address_corroboration_sql(db_schema))


def provider_directory_address_corroboration_select_sql(db_schema: str | None = None) -> str:
    sql = provider_directory_address_corroboration_sql(db_schema)
    marker = " AS\n"
    if marker not in sql:
        raise ValueError("Provider Directory PTG corroboration SQL does not contain view AS marker")
    return sql.split(marker, 1)[1].strip().rstrip(";")


def _drop_provider_directory_address_corroboration_relation_sql(schema: str, relation: str) -> str:
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


async def _create_provider_directory_address_corroboration_indexes(
    schema: str,
    table_name: str = PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW,
) -> None:
    table_ref = _qt(schema, table_name)
    index_name = partial(_address_corroboration_index_name, table_name)
    statements = (
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_lookup_idx"))}
            ON {table_ref} (npi, address_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_active_lookup_idx"))}
            ON {table_ref} (npi, address_key, provider_directory_observed_at DESC NULLS LAST)
            WHERE provider_directory_active_match IS TRUE;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_source_snapshot_idx"))}
            ON {table_ref} (source_key, snapshot_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_plan_pair_idx"))}
            ON {table_ref} (snapshot_id, plan_id, ptg_plan_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_pd_source_idx"))}
            ON {table_ref} (provider_directory_source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(index_name("pd_price_addr_corrob_network_names_gin"))}
            ON {table_ref} USING gin (provider_directory_network_names);
        """,
    )
    for statement in statements:
        await db.status(statement)


def _address_corroboration_index_name(table_name: str, target_index_name: str) -> str:
    if table_name == PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW:
        return target_index_name
    return _bounded_identifier(f"{table_name}_{target_index_name}")


async def _rename_address_corroboration_stage_indexes(schema: str, stage_table: str) -> None:
    for target_index_name in PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_INDEXES:
        stage_index_name = _address_corroboration_index_name(stage_table, target_index_name)
        if stage_index_name == target_index_name:
            continue
        await db.status(
            f"ALTER INDEX IF EXISTS {_qt(schema, stage_index_name)} RENAME TO {_q(target_index_name)};"
        )


async def _swap_address_corroboration_stage(
    schema: str,
    stage_table: str,
    stage_ref: str,
    target_relation: str,
) -> None:
    target_ref = _qt(schema, target_relation)
    old_relation = f"{target_relation}_old"
    old_ref = _qt(schema, old_relation)
    async with db.transaction():
        await db.status(_drop_provider_directory_address_corroboration_relation_sql(schema, old_relation))
        await db.status(
            f"""
            DO $$
            DECLARE relkind_value "char";
            BEGIN
                SELECT cls.relkind
                  INTO relkind_value
                  FROM pg_class cls
                  JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                 WHERE ns.nspname = {_sql_string_literal(schema)}
                   AND cls.relname = {_sql_string_literal(target_relation)};

                IF relkind_value = 'v' THEN
                    EXECUTE 'DROP VIEW {target_ref}';
                ELSIF relkind_value = 'm' THEN
                    EXECUTE 'DROP MATERIALIZED VIEW {target_ref}';
                ELSIF relkind_value IN ('r', 'p') THEN
                    EXECUTE 'ALTER TABLE {target_ref} RENAME TO {_q(old_relation)}';
                END IF;
            END $$;
            """
        )
        await db.status(f"ALTER TABLE {stage_ref} RENAME TO {_q(target_relation)};")
    await db.status(_drop_provider_directory_address_corroboration_relation_sql(schema, old_relation))
    await _rename_address_corroboration_stage_indexes(schema, stage_table)
    await _create_provider_directory_address_corroboration_indexes(schema, target_relation)
    await db.status(f"DROP TABLE IF EXISTS {old_ref};")


async def publish_provider_directory_address_corroboration_table(
    db_schema: str | None = None,
    *,
    refresh_network_catalog: bool = True,
) -> dict[str, Any]:
    schema = db_schema or _schema()
    relation = PROVIDER_DIRECTORY_ADDRESS_CORROBORATION_VIEW
    stage_table = _stage_table_name()
    stage_ref = _qt(schema, stage_table)
    network_catalog_metrics = (
        await publish_provider_directory_network_catalog(schema)
        if refresh_network_catalog
        else await _ensure_provider_directory_network_catalog_populated(schema)
    )
    select_sql = provider_directory_address_corroboration_select_sql(schema)
    try:
        await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
        await db.status(f"CREATE UNLOGGED TABLE {stage_ref} AS\n{select_sql};")
        row_count = int(await db.scalar(f"SELECT COUNT(*) FROM {stage_ref};") or 0)
        await _create_provider_directory_address_corroboration_indexes(schema, stage_table)
        await db.status(f"ANALYZE {stage_ref};")
        await _swap_address_corroboration_stage(schema, stage_table, stage_ref, relation)
        await db.status(f"ANALYZE {_qt(schema, relation)};")
        return {
            "published": True,
            "relation": _qt(schema, relation),
            "rows": row_count,
            "storage": "table",
            "network_catalog": network_catalog_metrics,
        }
    except Exception:
        try:
            await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
        except Exception:  # pragma: no cover - cleanup best effort
            LOGGER.warning(
                "Failed to clean provider directory address stage table %s",
                stage_ref,
                exc_info=True,
            )
        raise


def _provider_directory_location_coordinate_sql_map(source_alias: str) -> dict[str, str]:
    location_country_expr = _country_restore_default_us_sql(f"{source_alias}.country_code")
    raw_lat = f"({source_alias}.latitude)::numeric"
    raw_long = f"({source_alias}.longitude)::numeric"
    numeric_re = r"^-?[0-9]+(\.[0-9]+)?$"
    numeric_guard = (
        f"{source_alias}.latitude ~ '{numeric_re}' "
        f"AND {source_alias}.longitude ~ '{numeric_re}'"
    )
    normalized_lat_expr = _coordinate_from_location_sql(
        f"{source_alias}.latitude",
        f"{source_alias}.longitude",
        location_country_expr,
        axis="lat",
    )
    normalized_long_expr = _coordinate_from_location_sql(
        f"{source_alias}.latitude",
        f"{source_alias}.longitude",
        location_country_expr,
        axis="long",
    )
    return {
        "location_country_expr": location_country_expr,
        "raw_lat": raw_lat,
        "raw_long": raw_long,
        "numeric_guard": numeric_guard,
        "normalized_lat_expr": normalized_lat_expr,
        "normalized_long_expr": normalized_long_expr,
    }


def _provider_directory_location_coordinate_scopes(
    source_alias: str,
    *,
    db_schema: str,
    run_id: str | None,
    source_ids: list[str] | tuple[str, ...] | None,
    seen_table: str | None,
) -> list[str]:
    scope_clause_list: list[str] = []
    if run_id is not None and not seen_table:
        scope_clause_list.append(f"{source_alias}.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        scope_clause_list.append(f"{source_alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    if seen_table:
        seen_ref = _qt(db_schema, seen_table)
        seen_run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if run_id is not None else ""
        scope_clause_list.append(
            f"""EXISTS (
                SELECT 1
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = 'Location'
                   {seen_run_filter}
                   AND seen.source_id = {source_alias}.source_id
                   AND seen.resource_id = {source_alias}.resource_id
            )"""
        )
    scope_clause_list.append(
        f"""(
            CAST(:after_source_id AS varchar) IS NULL
            OR ({source_alias}.source_id, {source_alias}.resource_id)
                > (CAST(:after_source_id AS varchar), CAST(:after_resource_id AS varchar))
        )"""
    )
    return scope_clause_list


def _provider_directory_location_coordinate_where_sql(
    source_alias: str,
    coordinate_expr_map: dict[str, str],
    scope_clause_list: list[str],
) -> str:
    where_clause_list = [
        f"NULLIF(BTRIM(COALESCE({source_alias}.latitude, '')), '') IS NOT NULL",
        f"NULLIF(BTRIM(COALESCE({source_alias}.longitude, '')), '') IS NOT NULL",
        coordinate_expr_map["numeric_guard"],
        "NOT ("
        + _coordinate_pair_plausible_sql(
            coordinate_expr_map["raw_lat"],
            coordinate_expr_map["raw_long"],
            coordinate_expr_map["location_country_expr"],
        )
        + ")",
        f"({coordinate_expr_map['normalized_lat_expr']}) IS NOT NULL",
        f"({coordinate_expr_map['normalized_long_expr']}) IS NOT NULL",
        *scope_clause_list,
    ]
    return " AND ".join(f"({clause})" for clause in where_clause_list)


def _provider_directory_location_coordinate_batch_body(
    source_alias: str,
    location_ref: str,
    coordinate_expr_map: dict[str, str],
    where_sql: str,
) -> str:
    normalized_latitude_sql = _coordinate_text_sql(coordinate_expr_map["normalized_lat_expr"])
    normalized_longitude_sql = _coordinate_text_sql(coordinate_expr_map["normalized_long_expr"])
    return f"""
    WITH candidates AS MATERIALIZED (
        SELECT
            {source_alias}.source_id,
            {source_alias}.resource_id,
            {normalized_latitude_sql} AS normalized_latitude,
            {normalized_longitude_sql} AS normalized_longitude
          FROM {location_ref} AS {source_alias}
         WHERE {where_sql}
         ORDER BY {source_alias}.source_id,
                  {source_alias}.resource_id
         LIMIT CAST(:batch_size AS integer)
    ),
    updated AS (
        UPDATE {location_ref} AS loc
           SET latitude = candidates.normalized_latitude,
               longitude = candidates.normalized_longitude,
               updated_at = now()
          FROM candidates
         WHERE loc.source_id = candidates.source_id
           AND loc.resource_id = candidates.resource_id
           AND candidates.normalized_latitude IS NOT NULL
           AND candidates.normalized_longitude IS NOT NULL
           AND (
                loc.latitude IS DISTINCT FROM candidates.normalized_latitude
             OR loc.longitude IS DISTINCT FROM candidates.normalized_longitude
           )
         RETURNING loc.source_id,
                   loc.resource_id
    )
    SELECT
        (SELECT COUNT(*) FROM candidates)::bigint AS candidate_rows,
        (SELECT COUNT(*) FROM updated)::bigint AS updated_rows,
        (SELECT source_id FROM candidates ORDER BY source_id DESC, resource_id DESC LIMIT 1)::varchar
            AS last_source_id,
        (SELECT resource_id FROM candidates ORDER BY source_id DESC, resource_id DESC LIMIT 1)::varchar
            AS last_resource_id;
    """


def provider_directory_location_coordinate_batch_sql(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> str:
    """Normalize stored FHIR Location coordinates in bounded keyset batches."""

    schema = db_schema or _schema()
    source_alias = "loc_src"
    location_ref = _qt(schema, "provider_directory_location")
    coordinate_expr_map = _provider_directory_location_coordinate_sql_map(source_alias)
    scope_clause_list = _provider_directory_location_coordinate_scopes(
        source_alias,
        db_schema=schema,
        run_id=run_id,
        source_ids=source_ids,
        seen_table=seen_table,
    )
    where_sql = _provider_directory_location_coordinate_where_sql(
        source_alias,
        coordinate_expr_map,
        scope_clause_list,
    )
    return _provider_directory_location_coordinate_batch_body(
        source_alias,
        location_ref,
        coordinate_expr_map,
        where_sql,
    )


async def backfill_provider_directory_location_coordinates(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
    batch_size: int | None = None,
) -> int:
    """Repair stored Provider Directory coordinates that can be normalized safely."""

    schema = db_schema or _schema()
    if not await _table_exists(schema, "provider_directory_location"):
        return 0
    query_param_dict: dict[str, Any] = {}
    if run_id is not None:
        query_param_dict["run_id"] = run_id
    if source_ids:
        query_param_dict["source_ids"] = list(source_ids)
    total_updated = 0
    after_source_id: str | None = None
    after_resource_id: str | None = None
    bounded_batch_size = max(int(batch_size or _location_coordinate_batch_size()), 1)
    coordinate_batch_sql = provider_directory_location_coordinate_batch_sql(
        schema,
        run_id=run_id,
        source_ids=source_ids,
        seen_table=seen_table,
    )
    while True:
        coordinate_batch_result = await db.first(
            coordinate_batch_sql,
            **query_param_dict,
            after_source_id=after_source_id,
            after_resource_id=after_resource_id,
            batch_size=bounded_batch_size,
        )
        batch_result_mapping = (
            coordinate_batch_result._mapping
            if hasattr(coordinate_batch_result, "_mapping")
            else dict(coordinate_batch_result or {})
        )
        candidate_rows = int(batch_result_mapping.get("candidate_rows") or 0)
        updated_rows = int(batch_result_mapping.get("updated_rows") or 0)
        total_updated += updated_rows
        next_source_id = _clean_text(batch_result_mapping.get("last_source_id"))
        next_resource_id = _clean_text(batch_result_mapping.get("last_resource_id"))
        if candidate_rows <= 0 or not next_source_id or not next_resource_id:
            break
        if next_source_id == after_source_id and next_resource_id == after_resource_id:
            break
        after_source_id = next_source_id
        after_resource_id = next_resource_id
    return total_updated


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
    zip_city_select = "geo.city::varchar" if restore_state_from_zip else "NULL::varchar"
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
        f"{source_alias}.city_name IS NULL",
        f"{source_alias}.state_code IS NULL",
        f"{source_alias}.city_norm IS NULL",
        f"{raw_state_expr} ~ '^[0-9]{{1,2}}$'",
        needs_country_normalization_expr,
    ]
    scope_clauses: list[str] = []
    from_sql = f"FROM {location_ref} AS {source_alias}"
    if run_id is not None and not seen_table:
        scope_clauses.append(f"{source_alias}.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        scope_clauses.append(f"{source_alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    if seen_table:
        seen_ref = _qt(schema, seen_table)
        seen_run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if run_id is not None else ""
        from_sql = f"""
          FROM (
                SELECT DISTINCT seen.source_id,
                       seen.resource_id
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = 'Location'
                   {seen_run_filter}
               ) AS seen_scope
          JOIN {location_ref} AS {source_alias}
            ON {source_alias}.source_id = seen_scope.source_id
           AND {source_alias}.resource_id = seen_scope.resource_id
        """
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
          {from_sql}
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
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.city_name::varchar, '')), '') IS NULL
                    THEN {zip_city_select}
                ELSE NULL::varchar
            END AS zip_restored_city,
            COALESCE(NULLIF(BTRIM(normalized.normalized_state::varchar), ''), {zip_state_select}) AS resolved_state,
            COALESCE(NULLIF(BTRIM(normalized.city_name::varchar), ''), {zip_city_select}) AS resolved_city
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
                resolved_city,
                resolved_state,
                postal_code,
                COALESCE(NULLIF(normalized_country, ''), 'US')
            ) AS computed_address_key,
            source_zip5 AS computed_zip5,
            {qschema}.addr_state_code_v1(resolved_state)::varchar AS computed_state_code,
            {qschema}.addr_city_norm_v1(resolved_city)::varchar AS computed_city_norm,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(city_name::varchar, '')), '') IS NULL
                 AND zip_restored_city IS NOT NULL
                    THEN zip_restored_city::varchar
                ELSE NULL::varchar
            END AS restored_city_name,
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
           city_name = COALESCE(keyed.restored_city_name, loc.city_name),
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
         OR loc.city_name IS DISTINCT FROM COALESCE(keyed.restored_city_name, loc.city_name)
         OR loc.state_name IS DISTINCT FROM COALESCE(keyed.restored_state_name, loc.state_name)
         OR loc.state_code IS DISTINCT FROM COALESCE(keyed.computed_state_code, loc.state_code)
         OR loc.city_norm IS DISTINCT FROM COALESCE(keyed.computed_city_norm, loc.city_norm)
         OR loc.country_code IS DISTINCT FROM COALESCE(keyed.normalized_country, loc.country_code)
       );
    """


def provider_directory_location_address_key_batch_sql(
    db_schema: str | None = None,
    *,
    restore_state_from_zip: bool = True,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> str:
    """Stamp a bounded keyset batch of FHIR Location rows with address keys."""

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
    zip_city_select = "geo.city::varchar" if restore_state_from_zip else "NULL::varchar"
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
        f"{source_alias}.city_name IS NULL",
        f"{source_alias}.state_code IS NULL",
        f"{source_alias}.city_norm IS NULL",
        f"{raw_state_expr} ~ '^[0-9]{{1,2}}$'",
        needs_country_normalization_expr,
    ]
    scope_clauses: list[str] = []
    if run_id is not None and not seen_table:
        scope_clauses.append(f"{source_alias}.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        scope_clauses.append(f"{source_alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    if seen_table:
        seen_ref = _qt(schema, seen_table)
        seen_run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if run_id is not None else ""
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
    scope_clauses.append(
        f"""(
            CAST(:after_source_id AS varchar) IS NULL
            OR ({source_alias}.source_id, {source_alias}.resource_id)
                > (CAST(:after_source_id AS varchar), CAST(:after_resource_id AS varchar))
        )"""
    )
    where_clauses = [f"({' OR '.join(needs_work_clauses)})", *scope_clauses]
    where_sql = " AND ".join(where_clauses)
    return f"""
    WITH candidates AS MATERIALIZED (
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
         ORDER BY {source_alias}.source_id,
                  {source_alias}.resource_id
         LIMIT CAST(:batch_size AS integer)
    ),
    normalized AS (
        SELECT * FROM candidates
    ),
    resolved AS (
        SELECT
            normalized.*,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.normalized_state::varchar, '')), '') IS NULL
                    THEN {zip_state_select}
                ELSE NULL::varchar
            END AS zip_restored_state,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(normalized.city_name::varchar, '')), '') IS NULL
                    THEN {zip_city_select}
                ELSE NULL::varchar
            END AS zip_restored_city,
            COALESCE(NULLIF(BTRIM(normalized.normalized_state::varchar), ''), {zip_state_select}) AS resolved_state,
            COALESCE(NULLIF(BTRIM(normalized.city_name::varchar), ''), {zip_city_select}) AS resolved_city
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
                resolved_city,
                resolved_state,
                postal_code,
                COALESCE(NULLIF(normalized_country, ''), 'US')
            ) AS computed_address_key,
            source_zip5 AS computed_zip5,
            {qschema}.addr_state_code_v1(resolved_state)::varchar AS computed_state_code,
            {qschema}.addr_city_norm_v1(resolved_city)::varchar AS computed_city_norm,
            CASE
                WHEN NULLIF(BTRIM(COALESCE(city_name::varchar, '')), '') IS NULL
                 AND zip_restored_city IS NOT NULL
                    THEN zip_restored_city::varchar
                ELSE NULL::varchar
            END AS restored_city_name,
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
    ),
    updated AS (
        UPDATE {location_ref} AS loc
           SET address_key = keyed.computed_address_key::text,
               zip5 = COALESCE(keyed.computed_zip5, loc.zip5),
               city_name = COALESCE(keyed.restored_city_name, loc.city_name),
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
             OR loc.city_name IS DISTINCT FROM COALESCE(keyed.restored_city_name, loc.city_name)
             OR loc.state_name IS DISTINCT FROM COALESCE(keyed.restored_state_name, loc.state_name)
             OR loc.state_code IS DISTINCT FROM COALESCE(keyed.computed_state_code, loc.state_code)
             OR loc.city_norm IS DISTINCT FROM COALESCE(keyed.computed_city_norm, loc.city_norm)
             OR loc.country_code IS DISTINCT FROM COALESCE(keyed.normalized_country, loc.country_code)
           )
         RETURNING loc.source_id,
                   loc.resource_id
    )
    SELECT
        (SELECT COUNT(*) FROM candidates)::bigint AS candidate_rows,
        (SELECT COUNT(*) FROM updated)::bigint AS updated_rows,
        (SELECT source_id FROM candidates ORDER BY source_id DESC, resource_id DESC LIMIT 1)::varchar
            AS last_source_id,
        (SELECT resource_id FROM candidates ORDER BY source_id DESC, resource_id DESC LIMIT 1)::varchar
            AS last_resource_id;
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
    batch_size: int | None = None,
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
    total_updated = 0
    after_source_id: str | None = None
    after_resource_id: str | None = None
    bounded_batch_size = max(int(batch_size or _location_address_key_batch_size()), 1)
    sql = provider_directory_location_address_key_batch_sql(
        schema,
        restore_state_from_zip=restore_state_from_zip,
        run_id=run_id,
        source_ids=source_ids,
        seen_table=seen_table,
    )
    while True:
        row = await db.first(
            sql,
            **params,
            after_source_id=after_source_id,
            after_resource_id=after_resource_id,
            batch_size=bounded_batch_size,
        )
        payload = row._mapping if hasattr(row, "_mapping") else dict(row or {})
        candidate_rows = int(payload.get("candidate_rows") or 0)
        updated_rows = int(payload.get("updated_rows") or 0)
        total_updated += updated_rows
        next_source_id = _clean_text(payload.get("last_source_id"))
        next_resource_id = _clean_text(payload.get("last_resource_id"))
        if candidate_rows <= 0 or not next_source_id or not next_resource_id:
            break
        if next_source_id == after_source_id and next_resource_id == after_resource_id:
            break
        after_source_id = next_source_id
        after_resource_id = next_resource_id
    return total_updated


async def backfill_provider_directory_resource_id_npis(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> dict[str, int]:
    """Backfill payer rows that use the FHIR resource id as the NPI."""
    schema = db_schema or _schema()
    cleaned_source_ids = _clean_source_id_list(source_ids)
    query_params_by_name: dict[str, Any] = {}
    scope_clauses: list[str] = []
    if cleaned_source_ids:
        scope_clauses.append("resource.source_id = ANY(CAST(:source_ids AS varchar[]))")
        query_params_by_name["source_ids"] = cleaned_source_ids
    if seen_table:
        seen_run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if run_id else ""
        if run_id:
            query_params_by_name["run_id"] = run_id
        seen_ref = _qt(schema, seen_table)
        scope_clauses.append(
            f"""EXISTS (
                SELECT 1
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = :resource_type
                   {seen_run_filter}
                   AND seen.source_id = resource.source_id
                   AND seen.resource_id = resource.resource_id
            )"""
        )
    elif run_id:
        scope_clauses.append("resource.last_seen_run_id = CAST(:run_id AS varchar)")
        query_params_by_name["run_id"] = run_id
    scope_sql = "".join(f"\n               AND {clause}" for clause in scope_clauses)
    updated_counts_by_resource: dict[str, int] = {}
    for resource_type, table_name in (
        ("Practitioner", "provider_directory_practitioner"),
        ("Organization", "provider_directory_organization"),
    ):
        updated_counts_by_resource[resource_type] = _coerce_rowcount(
            await db.status(
                f"""
                UPDATE {_qt(schema, table_name)} AS resource
                   SET npi = resource.resource_id::bigint,
                       updated_at = now()
                 WHERE resource.npi IS NULL
                   AND resource.resource_id ~ '^[0-9]{{10}}$'
                   {scope_sql};
                """,
                **query_params_by_name,
                resource_type=resource_type,
            )
        )
    return updated_counts_by_resource


async def _publish_provider_directory_artifacts(
    *,
    run_id: str | None,
    metrics: dict[str, Any],
    seen_table: str | None = None,
    address_key_run_id: str | None = None,
    publish_scope_run_id: str | None | object = _PUBLISH_SCOPE_UNSET,
    source_ids: list[str] | tuple[str, ...] | None = None,
    publish_corroboration: bool = False,
    publish_artifacts_targets: set[str] | None = None,
) -> dict[str, Any]:
    effective_publish_scope_run_id = (
        run_id if publish_scope_run_id is _PUBLISH_SCOPE_UNSET else publish_scope_run_id
    )
    metrics["publish_artifacts_targets"] = (
        sorted(publish_artifacts_targets) if publish_artifacts_targets is not None else "all"
    )
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=0,
        total=6,
        message="publishing Provider Directory address artifacts",
        metrics=metrics,
    )
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "location_contacts"):
        metrics["location_contacts_backfilled"] = await backfill_provider_directory_location_contacts()
        location_contacts_message = (
            "backfilled Provider Directory location contacts; "
            f"rows={metrics['location_contacts_backfilled'].get('location_contact_rows_updated', 0)}"
        )
    else:
        metrics["location_contacts_backfilled"] = _provider_directory_publish_target_skipped()
        location_contacts_message = "skipped Provider Directory location contact backfill"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=1,
        total=6,
        message=location_contacts_message,
        metrics=metrics,
    )
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "location_coordinates"):
        metrics["location_coordinates_backfilled"] = await backfill_provider_directory_location_coordinates(
            run_id=address_key_run_id, source_ids=source_ids, seen_table=seen_table,
        )
        location_coordinates_message = (
            "backfilled Provider Directory location coordinates; "
            f"rows={metrics['location_coordinates_backfilled']}"
        )
    else:
        metrics["location_coordinates_backfilled"] = _provider_directory_publish_target_skipped()
        location_coordinates_message = "skipped Provider Directory location coordinate backfill"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=2,
        total=6,
        message=location_coordinates_message,
        metrics=metrics,
    )
    if seen_table:
        await _prepare_provider_directory_import_seen_stage_lookup(seen_table)
    should_backfill_resource_id_npis = any(
        is_provider_directory_publish_target_enabled(publish_artifacts_targets, artifact_target_name)
        for artifact_target_name in ("resource_id_npis", "location_archive", "address_overlay", "corroboration")
    )
    if should_backfill_resource_id_npis:
        resource_id_npi_backfill_run_id = address_key_run_id if address_key_run_id is not None else None
        metrics["resource_id_npis_backfilled"] = await backfill_provider_directory_resource_id_npis(
            run_id=resource_id_npi_backfill_run_id,
            source_ids=source_ids,
            seen_table=seen_table,
        )
    else:
        metrics["resource_id_npis_backfilled"] = _provider_directory_publish_target_skipped()
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "location_address_keys"):
        metrics["location_address_keys_stamped"] = await publish_provider_directory_location_address_keys(
            run_id=address_key_run_id, source_ids=source_ids, seen_table=seen_table,
        )
        location_address_keys_message = (
            "stamped Provider Directory location address keys; "
            f"rows={metrics['location_address_keys_stamped']}"
        )
    else:
        metrics["location_address_keys_stamped"] = _provider_directory_publish_target_skipped()
        location_address_keys_message = "skipped Provider Directory location address key stamping"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=3,
        total=6,
        message=location_address_keys_message,
        metrics=metrics,
    )
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "location_archive"):
        metrics["location_archive"] = await publish_provider_directory_location_archive(
            run_id=effective_publish_scope_run_id, source_ids=source_ids, seen_table=seen_table,
        )
    else:
        metrics["location_archive"] = _provider_directory_publish_target_skipped()
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "address_overlay"):
        metrics["address_overlay"] = await publish_provider_directory_address_overlay(
            run_id=effective_publish_scope_run_id, source_ids=source_ids
        )
    else:
        metrics["address_overlay"] = _provider_directory_publish_target_skipped()
    address_artifact_message = (
        "published Provider Directory locations to address archive; "
        f"inserted={metrics['location_archive'].get('inserted', 0)} "
        f"updated={metrics['location_archive'].get('provenance_updates', 0)}"
    )
    if (
        not is_provider_directory_publish_target_enabled(publish_artifacts_targets, "location_archive")
        and not is_provider_directory_publish_target_enabled(publish_artifacts_targets, "address_overlay")
    ):
        address_artifact_message = "skipped Provider Directory location archive and address overlay publish"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=4,
        total=6,
        message=address_artifact_message,
        metrics=metrics,
    )
    if is_provider_directory_publish_target_enabled(publish_artifacts_targets, "network_catalog"):
        metrics["network_catalog"] = await publish_provider_directory_network_catalog(
            run_id=effective_publish_scope_run_id,
            source_ids=source_ids,
        )
        network_catalog_message = (
            "published Provider Directory network catalog; "
            f"rows={metrics['network_catalog'].get('rows', 0)}"
        )
    else:
        metrics["network_catalog"] = _provider_directory_publish_target_skipped()
        network_catalog_message = "skipped Provider Directory network catalog publish"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=5,
        total=6,
        message=network_catalog_message,
        metrics=metrics,
    )
    metrics["publish_corroboration"] = publish_corroboration
    if publish_corroboration and is_provider_directory_publish_target_enabled(
        publish_artifacts_targets,
        "corroboration",
    ):
        metrics["ptg_corroboration_view_published"] = (
            await publish_provider_directory_address_corroboration_if_available(
                refresh_network_catalog=False,
            )
        )
        message = "published Provider Directory PTG corroboration artifacts"
    elif publish_corroboration:
        metrics["ptg_corroboration_view_published"] = False
        metrics["ptg_corroboration_view_skipped"] = _provider_directory_publish_target_skipped()
        message = "skipped Provider Directory PTG corroboration artifacts"
    else:
        metrics["ptg_corroboration_view_published"] = False
        metrics["ptg_corroboration_view_skipped"] = {
            "reason": "publish_corroboration_disabled",
        }
        message = "skipped Provider Directory PTG corroboration artifacts"
    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory publishing artifacts",
        done=6,
        total=6,
        message=message,
        metrics=metrics,
    )
    return metrics

def _provider_directory_location_archive_stage_table_name(run_id: str | None = None) -> str:
    raw = run_id or f"{os.getpid()}_{time.time_ns()}"
    digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{PROVIDER_DIRECTORY_ADDRESS_ARCHIVE_STAGE_PREFIX}_{digest}"


def provider_directory_location_archive_stage_sql(
    db_schema: str | None = None,
    stage_table: str | None = None,
    *,
    run_id: str | None = None, source_ids: list[str] | tuple[str, ...] | None = None,
    seen_table: str | None = None,
) -> str:
    schema = db_schema or _schema()
    stage = stage_table or _provider_directory_location_archive_stage_table_name()
    stage_ref = _qt(schema, stage)
    location_ref = _qt(schema, "provider_directory_location")
    organization_ref = _qt(schema, "provider_directory_organization")
    uuid_re = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
    location_scope_clauses: list[str] = []
    organization_scope_clauses: list[str] = []
    if seen_table:
        seen_ref = _qt(schema, seen_table)
        seen_run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if run_id is not None else ""
        location_scope_clauses.append(
            f"""EXISTS (
                SELECT 1
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = 'Location'
                   {seen_run_filter}
                   AND seen.source_id = loc.source_id
                   AND seen.resource_id = loc.resource_id
            )"""
        )
        organization_scope_clauses.append(
            f"""EXISTS (
                SELECT 1
                  FROM {seen_ref} AS seen
                 WHERE seen.resource_type = 'Organization'
                   {seen_run_filter}
                   AND seen.source_id = organization.source_id
                   AND seen.resource_id = organization.resource_id
            )"""
        )
    elif run_id is not None:
        location_scope_clauses.append("loc.last_seen_run_id = CAST(:run_id AS varchar)")
        organization_scope_clauses.append("organization.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        location_scope_clauses.append("loc.source_id = ANY(CAST(:source_ids AS varchar[]))")
        organization_scope_clauses.append("organization.source_id = ANY(CAST(:source_ids AS varchar[]))")
    location_scope_sql = "".join(f"\n           AND {clause}" for clause in location_scope_clauses)
    organization_scope_sql = "".join(f"\n           AND {clause}" for clause in organization_scope_clauses)
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
           {location_scope_sql}
        UNION ALL
        SELECT
            {_q(schema)}.addr_key_v1(
                NULLIF(BTRIM(addr.value->'line'->>0), ''),
                NULLIF(BTRIM(addr.value->'line'->>1), ''),
                NULLIF(BTRIM(addr.value->>'city'), ''),
                NULLIF(BTRIM(addr.value->>'state'), ''),
                NULLIF(BTRIM(addr.value->>'postalCode'), ''),
                COALESCE(NULLIF(BTRIM(addr.value->>'country'), ''), 'US')
            ) AS address_key,
            NULLIF(BTRIM(addr.value->'line'->>0), '')::text AS first_line,
            NULLIF(BTRIM(addr.value->'line'->>1), '')::text AS second_line,
            NULLIF(BTRIM(addr.value->>'city'), '')::text AS city_name,
            NULLIF(BTRIM(addr.value->>'state'), '')::text AS state_name,
            NULLIF(BTRIM(addr.value->>'postalCode'), '')::text AS postal_code,
            COALESCE(NULLIF(BTRIM(addr.value->>'country'), ''), 'US')::text AS country_code,
            organization.updated_at,
            organization.source_id,
            organization.resource_id
          FROM {organization_ref} AS organization
          JOIN LATERAL jsonb_array_elements(
                COALESCE(organization.address_json::jsonb, '[]'::jsonb)
          ) WITH ORDINALITY AS addr(value, ordinal) ON TRUE
         WHERE organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(addr.value->'line'->>0), '') IS NOT NULL
           AND NULLIF(BTRIM(addr.value->>'postalCode'), '') IS NOT NULL
           AND (
                NULLIF(BTRIM(addr.value->'line'->>0), '') IS NOT NULL
             OR NULLIF(BTRIM(addr.value->>'city'), '') IS NOT NULL
           )
           AND NULLIF(BTRIM(addr.value->>'state'), '') IS NOT NULL
           AND UPPER(NULLIF(BTRIM(addr.value->>'state'), ''))
                NOT IN ('UN', 'XX', 'ZZ', 'NULL', 'N/A')
           AND COALESCE(
                NULLIF(
                    UPPER(regexp_replace(COALESCE(NULLIF(addr.value->>'country', ''), 'US'), '[^A-Z0-9]', '', 'g')),
                    ''
                ),
                'US'
           ) IN ('US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')
           {organization_scope_sql}
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
     WHERE address_key IS NOT NULL
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
    run_id: str | None = None, source_ids: list[str] | tuple[str, ...] | None = None,
    stage_table: str | None = None,
    seen_table: str | None = None,
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
        await db.status(
            provider_directory_location_archive_stage_sql(
                schema,
                stage,
                run_id=run_id, source_ids=source_ids,
                seen_table=seen_table,
            ),
            **(({"run_id": run_id} if run_id is not None else {}) | ({"source_ids": list(source_ids)} if source_ids else {})),
        )
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


def _network_catalog_stage_table_name(run_id: str | None = None) -> str:
    raw_identifier = run_id if run_id else f"{os.getpid()}_{time.time_ns()}"
    digest = hashlib.sha1(raw_identifier.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{PROVIDER_DIRECTORY_NETWORK_CATALOG_STAGE_PREFIX}_{digest}"


def _provider_directory_network_catalog_columns() -> tuple[str, ...]:
    return (
        "source_id",
        "network_resource_id",
        "provider_directory_network_name",
        "provider_directory_network_key",
        "provider_directory_issuer_key",
        "provider_directory_issuer_network_match_key",
        "aliases",
        "refs",
        "source_resource_counts",
        "insurance_plan_ref_count",
        "practitioner_role_ref_count",
        "organization_affiliation_ref_count",
        "distinct_ref_count",
        "source_org_name",
        "source_plan_name",
        "canonical_api_base",
        "observed_at",
        "published_at",
    )


def provider_directory_network_catalog_table_sql(
    db_schema: str | None = None,
    table_name: str | None = None,
) -> str:
    """Create the compact Provider Directory network catalog relation."""
    schema = db_schema if db_schema is not None else _schema()
    table_ref = _qt(schema, table_name or PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref} (
        source_id varchar(64) NOT NULL,
        network_resource_id varchar(256) NOT NULL,
        provider_directory_network_name varchar(512) NOT NULL,
        provider_directory_network_key varchar NOT NULL,
        provider_directory_issuer_key varchar,
        provider_directory_issuer_network_match_key varchar,
        aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
        refs jsonb NOT NULL DEFAULT '[]'::jsonb,
        source_resource_counts jsonb NOT NULL DEFAULT '{{}}'::jsonb,
        insurance_plan_ref_count bigint NOT NULL DEFAULT 0,
        practitioner_role_ref_count bigint NOT NULL DEFAULT 0,
        organization_affiliation_ref_count bigint NOT NULL DEFAULT 0,
        distinct_ref_count bigint NOT NULL DEFAULT 0,
        source_org_name varchar(256),
        source_plan_name varchar(512),
        canonical_api_base text,
        observed_at timestamp,
        published_at timestamp NOT NULL DEFAULT now(),
        PRIMARY KEY (source_id, network_resource_id)
    );
    """


async def _ensure_provider_directory_network_catalog_table(schema: str) -> None:
    await db.status(provider_directory_network_catalog_table_sql(schema))
    await _create_provider_directory_network_catalog_indexes(
        schema,
        PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE,
    )


async def _provider_directory_network_catalog_has_rows(schema: str) -> bool:
    return bool(
        await db.scalar(
            f"""
            SELECT EXISTS (
                SELECT 1
                  FROM {_qt(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)}
                 LIMIT 1
            );
            """
        )
    )


async def _ensure_provider_directory_network_catalog_populated(schema: str) -> dict[str, Any]:
    await _ensure_provider_directory_network_catalog_table(schema)
    missing_reason = await _network_catalog_missing_requirement(schema)
    relation = _qt(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)
    if missing_reason:
        return {
            "skipped": True,
            "reason": missing_reason,
            "relation": relation,
        }
    if await _provider_directory_network_catalog_has_rows(schema):
        return {
            "published": False,
            "reason": "already_populated",
            "relation": relation,
        }
    return await publish_provider_directory_network_catalog(schema)


def _network_catalog_index_name(table_name: str, suffix: str) -> str:
    return _bounded_identifier(f"{table_name}_{suffix}")


async def _create_provider_directory_network_catalog_indexes(schema: str, table_name: str) -> None:
    table_ref = _qt(schema, table_name)
    statements = (
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {_q(_network_catalog_index_name(table_name, "source_network_idx"))}
            ON {table_ref} (source_id, network_resource_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(_network_catalog_index_name(table_name, "source_idx"))}
            ON {table_ref} (source_id);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(_network_catalog_index_name(table_name, "network_key_idx"))}
            ON {table_ref} (provider_directory_network_key);
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(_network_catalog_index_name(table_name, "issuer_network_key_idx"))}
            ON {table_ref} (provider_directory_issuer_network_match_key)
         WHERE provider_directory_issuer_network_match_key IS NOT NULL;
        """,
        f"""
        CREATE INDEX IF NOT EXISTS {_q(_network_catalog_index_name(table_name, "name_idx"))}
            ON {table_ref} (provider_directory_network_name);
        """,
    )
    for statement in statements:
        await db.status(statement)


async def _rename_network_catalog_stage_indexes(schema: str, stage_table: str) -> None:
    for suffix in PROVIDER_DIRECTORY_NETWORK_CATALOG_INDEX_SUFFIXES:
        stage_index_name = _network_catalog_index_name(stage_table, suffix)
        target_index_name = _network_catalog_index_name(PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE, suffix)
        if stage_index_name == target_index_name:
            continue
        await db.status(
            f"ALTER INDEX IF EXISTS {_qt(schema, stage_index_name)} RENAME TO {_q(target_index_name)};"
        )


PROVIDER_DIRECTORY_NETWORK_CATALOG_REQUIRED_TABLES = (
    "provider_directory_source",
    "provider_directory_insurance_plan",
    "provider_directory_practitioner_role",
    "provider_directory_organization_affiliation",
    "provider_directory_organization",
)


async def _network_catalog_missing_requirement(schema: str) -> str | None:
    for table_name in PROVIDER_DIRECTORY_NETWORK_CATALOG_REQUIRED_TABLES:
        if not await _table_exists(schema, table_name):
            return f"{table_name}_unavailable"
    return None


async def _network_catalog_scope_sources(
    schema: str,
    *,
    run_id: str | None,
    source_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    cleaned = _clean_source_id_list(source_ids)
    if cleaned or not run_id:
        return cleaned
    rows = await db.all(
        f"""
        SELECT DISTINCT source_id
          FROM (
                SELECT source_id
                  FROM {_qt(schema, "provider_directory_insurance_plan")}
                 WHERE last_seen_run_id = :run_id
                   AND jsonb_array_length(COALESCE(network_refs::jsonb, '[]'::jsonb)) > 0
                UNION
                SELECT source_id
                  FROM {_qt(schema, "provider_directory_practitioner_role")}
                 WHERE last_seen_run_id = :run_id
                   AND jsonb_array_length(COALESCE(network_refs::jsonb, '[]'::jsonb)) > 0
                UNION
                SELECT source_id
                  FROM {_qt(schema, "provider_directory_organization_affiliation")}
                 WHERE last_seen_run_id = :run_id
                   AND jsonb_array_length(COALESCE(network_refs::jsonb, '[]'::jsonb)) > 0
          ) AS scoped
         WHERE source_id IS NOT NULL
         ORDER BY source_id;
        """,
        run_id=run_id,
    )
    return _clean_source_id_list([row[0] for row in rows])


def _provider_directory_network_catalog_scope_filter(
    alias: str,
    *,
    run_id: str | None,
    source_ids: list[str] | tuple[str, ...] | None,
) -> str:
    clauses: list[str] = []
    if run_id:
        clauses.append(f"{alias}.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        clauses.append(f"{alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    return "".join(f"\n             AND {clause}" for clause in clauses)


def provider_directory_network_catalog_insert_sql(
    db_schema: str | None = None,
    stage_table: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Build the scoped insert for resolved Provider Directory network Organizations."""
    schema = db_schema if db_schema is not None else _schema()
    stage_ref = _qt(schema, stage_table or _network_catalog_stage_table_name(run_id))
    ref_resource_id_expr = _sql_reference_resource_id("refs_raw.network_ref", "Organization")
    insurance_plan_scope = _provider_directory_network_catalog_scope_filter(
        "insurance_plan",
        run_id=run_id,
        source_ids=source_ids,
    )
    practitioner_role_scope = _provider_directory_network_catalog_scope_filter(
        "role",
        run_id=run_id,
        source_ids=source_ids,
    )
    affiliation_scope = _provider_directory_network_catalog_scope_filter(
        "affiliation",
        run_id=run_id,
        source_ids=source_ids,
    )
    return f"""
    INSERT INTO {stage_ref} ({", ".join(_provider_directory_network_catalog_columns())})
    WITH refs_raw AS MATERIALIZED (
        SELECT
            insurance_plan.source_id::varchar AS source_id,
            'InsurancePlan'::varchar AS source_resource_type,
            insurance_plan.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            insurance_plan.last_seen_run_id::varchar AS last_seen_run_id,
            insurance_plan.observed_at AS source_observed_at,
            insurance_plan.updated_at AS source_updated_at
          FROM {_qt(schema, "provider_directory_insurance_plan")} AS insurance_plan
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(insurance_plan.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {insurance_plan_scope}
        UNION ALL
        SELECT
            role.source_id::varchar AS source_id,
            'PractitionerRole'::varchar AS source_resource_type,
            role.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            role.last_seen_run_id::varchar AS last_seen_run_id,
            role.observed_at AS source_observed_at,
            role.updated_at AS source_updated_at
          FROM {_qt(schema, "provider_directory_practitioner_role")} AS role
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(role.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE role.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {practitioner_role_scope}
        UNION ALL
        SELECT
            affiliation.source_id::varchar AS source_id,
            'OrganizationAffiliation'::varchar AS source_resource_type,
            affiliation.resource_id::varchar AS source_resource_id,
            network_ref.value::varchar AS network_ref,
            affiliation.last_seen_run_id::varchar AS last_seen_run_id,
            affiliation.observed_at AS source_observed_at,
            affiliation.updated_at AS source_updated_at
          FROM {_qt(schema, "provider_directory_organization_affiliation")} AS affiliation
         CROSS JOIN LATERAL jsonb_array_elements_text(
                COALESCE(affiliation.network_refs::jsonb, '[]'::jsonb)
         ) AS network_ref(value)
         WHERE affiliation.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_ref.value), '') IS NOT NULL
           {affiliation_scope}
    ), refs AS MATERIALIZED (
        SELECT
            refs_raw.source_id,
            refs_raw.source_resource_type,
            refs_raw.source_resource_id,
            refs_raw.network_ref,
            {ref_resource_id_expr}::varchar AS network_resource_id,
            refs_raw.last_seen_run_id,
            refs_raw.source_observed_at,
            refs_raw.source_updated_at
          FROM refs_raw
         WHERE NULLIF(BTRIM(refs_raw.network_ref), '') IS NOT NULL
    ), joined AS MATERIALIZED (
        SELECT
            refs.source_id,
            refs.source_resource_type,
            refs.source_resource_id,
            refs.network_ref,
            refs.network_resource_id,
            refs.last_seen_run_id,
            refs.source_observed_at,
            refs.source_updated_at,
            NULLIF(BTRIM(network_org.name), '')::varchar AS provider_directory_network_name,
            COALESCE(network_org.aliases::jsonb, '[]'::jsonb) AS aliases,
            NULLIF(regexp_replace(lower(COALESCE(network_org.name, '')), '[^a-z0-9]+', '', 'g'), '')
                AS provider_directory_network_key,
            NULLIF(regexp_replace(lower(COALESCE(NULLIF(src.org_name, ''), src.plan_name, '')), '[^a-z0-9]+', '', 'g'), '')
                AS provider_directory_issuer_key,
            src.org_name::varchar AS source_org_name,
            src.plan_name::varchar AS source_plan_name,
            src.canonical_api_base::text AS canonical_api_base,
            GREATEST(
                COALESCE(refs.source_observed_at, TIMESTAMP 'epoch'),
                COALESCE(refs.source_updated_at, TIMESTAMP 'epoch'),
                COALESCE(network_org.observed_at, TIMESTAMP 'epoch'),
                COALESCE(network_org.updated_at, TIMESTAMP 'epoch'),
                COALESCE(src.updated_at, TIMESTAMP 'epoch')
            ) AS observed_at
          FROM refs
          JOIN {_qt(schema, "provider_directory_organization")} AS network_org
            ON network_org.source_id = refs.source_id
           AND network_org.resource_id = refs.network_resource_id
          JOIN {_qt(schema, "provider_directory_source")} AS src
            ON src.source_id = refs.source_id
         WHERE refs.network_resource_id IS NOT NULL
           AND network_org.active IS DISTINCT FROM false
           AND NULLIF(BTRIM(network_org.name), '') IS NOT NULL
    ), keyed AS MATERIALIZED (
        SELECT
            joined.*,
            CASE
                WHEN joined.provider_directory_issuer_key IS NOT NULL
                 AND joined.provider_directory_network_key IS NOT NULL
                    THEN joined.provider_directory_issuer_key || ':' || joined.provider_directory_network_key
                ELSE NULL
            END::varchar AS provider_directory_issuer_network_match_key
          FROM joined
         WHERE joined.provider_directory_network_key IS NOT NULL
    )
    SELECT
        keyed.source_id,
        keyed.network_resource_id,
        keyed.provider_directory_network_name,
        keyed.provider_directory_network_key,
        keyed.provider_directory_issuer_key,
        keyed.provider_directory_issuer_network_match_key,
        keyed.aliases,
        COALESCE(
            jsonb_agg(
                DISTINCT jsonb_build_object(
                    'resource_type', keyed.source_resource_type,
                    'resource_id', keyed.source_resource_id,
                    'ref', keyed.network_ref,
                    'last_seen_run_id', keyed.last_seen_run_id
                )
            ),
            '[]'::jsonb
        ) AS refs,
        jsonb_build_object(
            'InsurancePlan', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'InsurancePlan'),
            'PractitionerRole', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'PractitionerRole'),
            'OrganizationAffiliation', COUNT(DISTINCT keyed.source_resource_id)
                FILTER (WHERE keyed.source_resource_type = 'OrganizationAffiliation')
        ) AS source_resource_counts,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'InsurancePlan'))::bigint
            AS insurance_plan_ref_count,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'PractitionerRole'))::bigint
            AS practitioner_role_ref_count,
        (COUNT(DISTINCT keyed.source_resource_id)
            FILTER (WHERE keyed.source_resource_type = 'OrganizationAffiliation'))::bigint
            AS organization_affiliation_ref_count,
        COUNT(DISTINCT keyed.source_resource_type || ':' || keyed.source_resource_id || ':' || keyed.network_ref)::bigint
            AS distinct_ref_count,
        keyed.source_org_name,
        keyed.source_plan_name,
        keyed.canonical_api_base,
        MAX(keyed.observed_at) AS observed_at,
        now() AS published_at
      FROM keyed
  GROUP BY
        keyed.source_id,
        keyed.network_resource_id,
        keyed.provider_directory_network_name,
        keyed.provider_directory_network_key,
        keyed.provider_directory_issuer_key,
        keyed.provider_directory_issuer_network_match_key,
        keyed.aliases,
        keyed.source_org_name,
        keyed.source_plan_name,
        keyed.canonical_api_base;
    """


async def _copy_existing_network_catalog(
    stage_ref: str,
    target_ref: str,
    columns: str,
    source_ids: list[str],
) -> int:
    if not source_ids:
        return 0
    return _coerce_rowcount(
        await db.status(
            f"""
            INSERT INTO {stage_ref} ({columns})
            SELECT {columns}
              FROM {target_ref}
             WHERE NOT (source_id = ANY(CAST(:source_ids AS varchar[])));
            """,
            source_ids=source_ids,
        )
    )


async def _swap_network_catalog_stage(schema: str, stage_table: str, stage_ref: str, target_ref: str) -> None:
    old_table = f"{PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE}_old"
    async with db.transaction():
        await db.status(f"DROP TABLE IF EXISTS {_qt(schema, old_table)};")
        await db.status(f"ALTER TABLE {target_ref} RENAME TO {_q(old_table)};")
        await db.status(f"ALTER TABLE {stage_ref} RENAME TO {_q(PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)};")
    await db.status(f"DROP TABLE IF EXISTS {_qt(schema, old_table)};")
    await _rename_network_catalog_stage_indexes(schema, stage_table)
    await _create_provider_directory_network_catalog_indexes(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)


async def publish_provider_directory_network_catalog(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Publish resolved Provider Directory networks without locking source tables."""
    schema = db_schema if db_schema is not None else _schema()
    missing_reason = await _network_catalog_missing_requirement(schema)
    if missing_reason:
        return {"skipped": True, "reason": missing_reason}
    await _ensure_provider_directory_network_catalog_table(schema)
    effective_source_ids = await _network_catalog_scope_sources(
        schema,
        run_id=run_id,
        source_ids=source_ids,
    )
    target_ref = _qt(schema, PROVIDER_DIRECTORY_NETWORK_CATALOG_TABLE)
    if run_id is not None and not effective_source_ids:
        return {
            "skipped": True,
            "reason": "no_scoped_sources",
            "source_ids": [],
            "relation": target_ref,
        }
    stage_table = _network_catalog_stage_table_name(run_id)
    stage_ref = _qt(schema, stage_table)
    columns = ", ".join(_provider_directory_network_catalog_columns())
    query_param_dict: dict[str, Any] = {}
    if run_id is not None:
        query_param_dict["run_id"] = run_id
    if effective_source_ids:
        query_param_dict["source_ids"] = effective_source_ids

    await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
    try:
        await db.status(f"CREATE UNLOGGED TABLE {stage_ref} (LIKE {target_ref} INCLUDING DEFAULTS);")
        copied_existing = await _copy_existing_network_catalog(
            stage_ref,
            target_ref,
            columns,
            effective_source_ids,
        )
        inserted = _coerce_rowcount(
            await db.status(
                provider_directory_network_catalog_insert_sql(
                    schema,
                    stage_table,
                    run_id=run_id,
                    source_ids=effective_source_ids,
                ),
                **query_param_dict,
            )
        )
        stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {stage_ref};") or 0)
        await _create_provider_directory_network_catalog_indexes(schema, stage_table)
        await db.status(f"ANALYZE {stage_ref};")
        await _swap_network_catalog_stage(schema, stage_table, stage_ref, target_ref)
        return {
            "published": True,
            "rows": stage_rows,
            "inserted": inserted,
            "copied_existing": copied_existing,
            "source_ids": effective_source_ids,
            "relation": target_ref,
        }
    except Exception:
        try:
            await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
        except Exception:  # pragma: no cover - cleanup best effort
            LOGGER.warning("Failed to clean Provider Directory network catalog stage %s", stage_ref, exc_info=True)
        raise


async def publish_provider_directory_address_corroboration_if_available(
    db_schema: str | None = None,
    *,
    refresh_network_catalog: bool = True,
) -> bool:
    schema = db_schema or _schema()
    if not await _table_exists(schema, "entity_address_unified"):
        return False
    await publish_provider_directory_address_corroboration_table(
        schema,
        refresh_network_catalog=refresh_network_catalog,
    )
    return True


async def _ensure_provider_directory_tables() -> None:
    schema = _schema()
    await db.status(f"CREATE SCHEMA IF NOT EXISTS {_q(schema)};")
    for model in (*SOURCE_MODELS, *CANONICAL_RESOURCE_MODELS):
        await db.create_table(model.__table__, checkfirst=True)
        await _ensure_provider_directory_model_columns(model, schema)
    await _ensure_provider_directory_source_column_types(schema)
    for model in (*SOURCE_MODELS, *CANONICAL_RESOURCE_MODELS):
        for index in getattr(model, "__my_additional_indexes__", []) or []:
            name = index.get("name") or "_".join(index.get("index_elements", ()))
            using = f"USING {index.get('using')} " if index.get("using") else ""
            where = f" WHERE {index.get('where')}" if index.get("where") else ""
            elements = _provider_directory_index_elements_sql(model, index)
            unique = "UNIQUE " if index.get("unique") else ""
            await db.status(
                f"CREATE {unique}INDEX IF NOT EXISTS {name} "
                f"ON {_qt(schema, model.__tablename__)} {using}({elements}){where};"
            )
    await _ensure_provider_directory_import_seen_table(schema)


async def _ensure_provider_directory_source_column_types(schema: str) -> None:
    await db.status(
        f"""
        ALTER TABLE IF EXISTS {_qt(schema, "provider_directory_source")}
            ALTER COLUMN data_quality_checked TYPE text;
        """
    )


async def _ensure_provider_directory_model_columns(model: Any, schema: str) -> None:
    rows = await db.all(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = :table_name
        """,
        schema=schema,
        table_name=model.__tablename__,
    )
    existing = {_clean_text((row._mapping if hasattr(row, "_mapping") else row).get("column_name")) for row in rows}
    table_ref = _qt(schema, model.__tablename__)
    dialect = postgresql.dialect()
    for column in model.__table__.columns:
        if column.name in existing:
            continue
        column_ddl = str(CreateColumn(column).compile(dialect=dialect)).strip()
        if not column_ddl:
            continue
        await db.status(f"ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS {column_ddl};")


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


def _location_address_key_batch_size() -> int:
    try:
        return max(
            int(
                os.getenv(
                    "HLTHPRT_PROVIDER_DIRECTORY_ADDRESS_KEY_BATCH_SIZE",
                    str(DEFAULT_LOCATION_ADDRESS_KEY_BATCH_SIZE),
                )
            ),
            1,
        )
    except ValueError:
        return DEFAULT_LOCATION_ADDRESS_KEY_BATCH_SIZE


def _location_coordinate_batch_size() -> int:
    try:
        return max(
            int(
                os.getenv(
                    "HLTHPRT_PROVIDER_DIRECTORY_COORDINATE_BATCH_SIZE",
                    str(DEFAULT_LOCATION_COORDINATE_BATCH_SIZE),
                )
            ),
            1,
        )
    except ValueError:
        return DEFAULT_LOCATION_COORDINATE_BATCH_SIZE


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
PROVIDER_DIRECTORY_SOURCE_PROBE_STATE_COLUMNS = frozenset(
    {
        "last_probe_status",
        "last_probe_status_code",
        "last_probe_error",
        "last_probe_run_id",
        "last_probed_at",
    }
)
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
    if table.name == ProviderDirectorySource.__tablename__ and column == "metadata_json":
        return func.coalesce(table.c.metadata_json.cast(JSONB), func.jsonb_build_object()).op("||")(
            func.coalesce(excluded_value.cast(JSONB), func.jsonb_build_object())
        )
    if table.name == ProviderDirectorySource.__tablename__ and column in PROVIDER_DIRECTORY_SOURCE_PROBE_STATE_COLUMNS:
        return case(
            (statement.excluded.last_probe_status.is_(None), getattr(table.c, column)),
            else_=excluded_value,
        )
    if column == "address_key" and _preserve_null_location_address_key(table):
        return _location_address_key_update_expression(table, statement.excluded)
    return excluded_value


def _effective_update_sql(table, column: str, *, target_prefix: str, incoming_prefix: str) -> str:
    if table.name == ProviderDirectoryCanonicalResource.__tablename__ and column == "first_seen_run_id":
        return f"COALESCE({target_prefix}.{_q(column)}, {incoming_prefix}.{_q(column)})"
    if table.name == ProviderDirectorySource.__tablename__ and column == "metadata_json":
        quoted = _q(column)
        return (
            f"COALESCE({target_prefix}.{quoted}::jsonb, '{{}}'::jsonb) "
            f"|| COALESCE({incoming_prefix}.{quoted}::jsonb, '{{}}'::jsonb)"
        )
    if table.name == ProviderDirectorySource.__tablename__ and column in PROVIDER_DIRECTORY_SOURCE_PROBE_STATE_COLUMNS:
        quoted = _q(column)
        probe_status = _q("last_probe_status")
        return (
            f"CASE WHEN {incoming_prefix}.{probe_status} IS NULL "
            f"THEN {target_prefix}.{quoted} "
            f"ELSE {incoming_prefix}.{quoted} "
            f"END"
        )
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
             WHERE run_id = CAST(:run_id AS varchar);
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
    if model is ProviderDirectoryLocation and _location_contact_fields_missing(rows):
        rows = _attach_location_contact_fields(rows)
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


def _seed_row_matches_query(row: dict[str, Any], source_query: str | None) -> bool:
    query = _clean_text(source_query)
    if not query:
        return True
    needle = query.lower()
    return any(
        needle in (_clean_text(row.get(field)) or "").lower()
        for field in ("org_name", "plan_name")
    )


def _seed_row_has_importable_provider_directory_override(row: dict[str, Any]) -> bool:
    source_row = _source_row_from_seed(row)
    metadata = source_row.get("metadata_json") or {}
    if not metadata.get("provider_directory_override"):
        return False
    validation = (_clean_text(source_row.get("last_validated_status")) or "").lower()
    return validation in {"", "valid", "auth_required"}


def _seed_row_has_recoverable_provider_directory_base(row: dict[str, Any]) -> bool:
    if _seed_row_has_importable_provider_directory_override(row):
        return True
    source_row = _source_row_from_seed(row)
    metadata = source_row.get("metadata_json") or {}
    return metadata.get("provider_directory_base_normalization") == "resource_or_metadata_parent_base"


class _AmeriHealthCaritasCatalogParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._in_cell = False
        self._cell_text: list[str] = []
        self._row: list[str] = []
        self._row_links: list[str] = []
        self.rows: list[tuple[list[str], list[str]]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"td", "th"}:
            self._in_cell = True
            self._cell_text = []
        if tag == "a" and self._in_cell:
            href = dict(attrs).get("href")
            if href:
                self._row_links.append(href)

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._in_cell:
            self._row.append(" ".join(" ".join(self._cell_text).split()))
            self._cell_text = []
            self._in_cell = False
        if tag == "tr" and self._row:
            self.rows.append((self._row, self._row_links))
            self._row = []
            self._row_links = []


class _HtmlLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._href: str | None = None
        self._text: list[str] = []
        self.links: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a" or self._href is not None:
            return
        href = dict(attrs).get("href")
        if not href:
            return
        self._href = href
        self._text = []

    def handle_data(self, data: str) -> None:
        if self._href is not None:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or self._href is None:
            return
        self.links.append((" ".join(" ".join(self._text).split()), self._href))
        self._href = None
        self._text = []


def _provider_api_base_from_url(url: str | None) -> str | None:
    text = _clean_text(url)
    if not text or "/provider-api" not in text:
        return None
    prefix = text.split("/provider-api", 1)[0]
    return _canonical_base(f"{prefix}/provider-api")


def _provider_directory_base_from_metadata_url(url: str | None) -> str | None:
    text = _clean_text(url)
    if not text:
        return None
    parsed = urllib.parse.urlsplit(text)
    if not parsed.scheme or not parsed.netloc:
        return None
    path = parsed.path.rstrip("/")
    if not path.lower().endswith("/metadata"):
        return None
    base_path = path[: -len("/metadata")].rstrip("/")
    if not base_path:
        return None
    return _canonical_base(urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, base_path, "", "")))


def _extract_urls_from_text(value: str | None) -> list[str]:
    text = (_clean_text(value) or "").replace("\xa0", " ")
    urls: list[str] = []
    for match in URL_IN_TEXT_RE.finditer(text):
        url = match.group(0).strip().rstrip(".,;")
        if url and url not in urls:
            urls.append(url)
    return urls


def _resource_type_from_provider_directory_url(url: str | None) -> str | None:
    canonical = _canonical_base(url)
    if not canonical:
        return None
    parsed = urllib.parse.urlsplit(canonical)
    segments = [segment for segment in parsed.path.strip("/").split("/") if segment]
    if not segments:
        return None
    resource_type_by_key = {resource_type.lower(): resource_type for resource_type in DEFAULT_RESOURCES}
    return resource_type_by_key.get(segments[-1].lower())


def _provider_directory_base_from_catalog_url(url: str | None) -> str | None:
    canonical = _canonical_base(url)
    if not canonical:
        return None
    return _resource_or_metadata_parent_base(canonical) or canonical


def _base_path(value: str | None) -> str:
    parsed = urllib.parse.urlsplit(value or "")
    return parsed.path.rstrip("/").lower()


def _cms_sma_bases_are_related(left: str | None, right: str | None) -> bool:
    left_base = _canonical_base(left)
    right_base = _canonical_base(right)
    if not left_base or not right_base:
        return False
    left_parts = urllib.parse.urlsplit(left_base)
    right_parts = urllib.parse.urlsplit(right_base)
    if left_parts.netloc.lower() != right_parts.netloc.lower():
        return False
    left_path = _base_path(left_base)
    right_path = _base_path(right_base)
    return (
        left_path == right_path
        or not left_path
        or not right_path
        or left_path.startswith(f"{right_path}/")
        or right_path.startswith(f"{left_path}/")
    )


def _cms_sma_selected_api_base(production_base: str, capability_bases: list[str]) -> str:
    for capability_base in capability_bases:
        if _cms_sma_bases_are_related(production_base, capability_base):
            return capability_base
    return production_base


def _cms_sma_header_key(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (_clean_text(value) or "").lower()).strip()


def _cms_sma_column_index(
    headers: list[str],
    keys: set[str],
    *,
    start: int = 0,
) -> int | None:
    for index, value in enumerate(headers[start:], start=start):
        if _cms_sma_header_key(value) in keys:
            return index
    return None


def _cms_sma_cell(row: list[str], index: int | None) -> str | None:
    if index is None or index >= len(row):
        return None
    return _clean_text(row[index])


def _cms_sma_endpoint_directory_seed_rows_from_csv(
    csv_text: str,
    *,
    source_query: str | None = None,
    source_url: str = CMS_SMA_ENDPOINT_DIRECTORY_URL,
) -> list[dict[str, Any]]:
    rows = list(csv.reader(io.StringIO(csv_text)))
    if len(rows) < 2:
        return []
    section_headers = rows[0]
    field_headers = rows[1]
    provider_start = next(
        (
            index
            for index, value in enumerate(section_headers)
            if _cms_sma_header_key(value) == "provider directory endpoint information"
        ),
        0,
    )
    state_col = _cms_sma_column_index(field_headers, {"state"})
    source_date_col = _cms_sma_column_index(field_headers, {"information as of date date completing the survey"})
    prod_col = _cms_sma_column_index(field_headers, {"provider directory production base url"}, start=provider_start)
    status_col = _cms_sma_column_index(field_headers, {"status drop down list"}, start=provider_start)
    cap_col = _cms_sma_column_index(field_headers, {"fhir capability statement link"}, start=provider_start)
    public_col = _cms_sma_column_index(field_headers, {"is the api public y n drop down list"}, start=provider_start)
    refresh_col = _cms_sma_column_index(
        field_headers,
        {"data refresh frequency e g real time hourly daily weekly monthly"},
        start=provider_start,
    )
    version_col = _cms_sma_column_index(field_headers, {"fhir version drop down list"}, start=provider_start)
    if state_col is None or prod_col is None or status_col is None or public_col is None:
        return []

    seed_rows: list[dict[str, Any]] = []
    for row in rows[2:]:
        state = _cms_sma_cell(row, state_col)
        if not state:
            continue
        status = _cms_sma_cell(row, status_col)
        public_value = _cms_sma_cell(row, public_col)
        status_key = (status or "").strip().lower()
        if status_key not in CMS_SMA_TRACKED_PROVIDER_DIRECTORY_STATUSES:
            continue
        if not (public_value or "").strip().lower().startswith("y"):
            continue
        validation_status = (
            CMS_SMA_CATALOG_VALIDATION_STATUS
            if status_key == "active"
            else CMS_SMA_CATALOG_IN_DEVELOPMENT_STATUS
        )

        production_urls = _extract_urls_from_text(_cms_sma_cell(row, prod_col))
        capability_urls = _extract_urls_from_text(_cms_sma_cell(row, cap_col))
        capability_bases: list[str] = []
        capability_url_by_base: dict[str, str] = {}
        for capability_url in capability_urls:
            capability_base = _provider_directory_base_from_metadata_url(capability_url)
            if not capability_base:
                continue
            _append_unique(capability_bases, capability_base)
            capability_url_by_base.setdefault(capability_base, capability_url)

        groups: dict[str, dict[str, Any]] = {}

        def group_for(api_base: str) -> dict[str, Any]:
            group = groups.setdefault(
                api_base,
                {
                    "api_base": api_base,
                    "endpoints": {},
                    "equivalent_api_bases": [],
                    "metadata_url": None,
                },
            )
            for capability_base, capability_url in capability_url_by_base.items():
                if _cms_sma_bases_are_related(api_base, capability_base):
                    group["metadata_url"] = group["metadata_url"] or capability_url
                    _append_unique(group["equivalent_api_bases"], capability_base)
            return group

        for production_url in production_urls:
            production_base = _provider_directory_base_from_catalog_url(production_url)
            if not production_base:
                continue
            api_base = _cms_sma_selected_api_base(production_base, capability_bases)
            group = group_for(api_base)
            _append_unique(group["equivalent_api_bases"], production_base)
            _append_unique(group["equivalent_api_bases"], production_url)
            resource_type = _resource_type_from_provider_directory_url(production_url)
            endpoint_field = RESOURCE_ENDPOINT_FIELDS.get(resource_type or "")
            if endpoint_field and endpoint_field not in group["endpoints"]:
                group["endpoints"][endpoint_field] = production_url

        if not groups:
            for capability_base in capability_bases:
                group = group_for(capability_base)
                group["metadata_url"] = group["metadata_url"] or capability_url_by_base.get(capability_base)

        source_date = _cms_sma_cell(row, source_date_col)
        refresh = _cms_sma_cell(row, refresh_col)
        fhir_version = _cms_sma_cell(row, version_col)
        state_key = re.sub(r"[^a-z0-9]+", "-", state.lower()).strip("-")
        for group in groups.values():
            api_base = group["api_base"]
            digest = hashlib.sha1(f"{state}|{api_base}".encode("utf-8")).hexdigest()[:8]
            metadata = {
                "provider_directory_source_catalog": CMS_SMA_ENDPOINT_DIRECTORY_SOURCE,
                "provider_directory_confirmed_catalog_url": source_url,
                "provider_directory_catalog_status": status,
                "provider_directory_catalog_public": public_value,
                "provider_directory_equivalent_api_bases": group["equivalent_api_bases"],
            }
            if group["metadata_url"]:
                metadata["provider_directory_confirmed_metadata_url"] = group["metadata_url"]
            seed_row = {
                "id": f"cms-sma-{state_key}-{digest}",
                "org_name": f"State of {state}",
                "plan_name": f"{state} Medicaid Provider Directory",
                "api_base": api_base,
                "auth_type": "none",
                "last_validated_status": validation_status,
                "requires_registration": False,
                "fhir_version": fhir_version,
                "source": CMS_SMA_ENDPOINT_DIRECTORY_SOURCE,
                "source_detail": f"CMS SMA Endpoint Directory public Provider Directory row state={state}",
                "source_url": source_url,
                "source_date": source_date,
                "note": (
                    "Catalog-discovered public Provider Directory endpoint from the CMS State Medicaid "
                    "Agency Endpoint Directory; live probe validation controls resource import."
                ),
                "metadata_json": metadata,
                **group["endpoints"],
            }
            if refresh:
                seed_row["data_quality_checked"] = refresh
            if _seed_row_matches_query(seed_row, source_query):
                seed_rows.append(seed_row)
    return seed_rows


def _external_splash_target_url(href: str | None, *, source_url: str) -> str | None:
    text = _clean_text(href)
    if not text:
        return None
    absolute = urllib.parse.urljoin(source_url, text)
    parsed = urllib.parse.urlsplit(absolute)
    params = urllib.parse.parse_qs(parsed.query)
    for key in ("splash", "url"):
        values = params.get(key)
        if values:
            return values[0]
    return absolute


def _amerihealth_caritas_seed_rows_from_catalog_html(
    html_text: str,
    *,
    source_query: str | None = None,
    source_url: str = AMERIHEALTH_CARITAS_DOC_URL,
    source_date: str | None = None,
) -> list[dict[str, Any]]:
    parser = _AmeriHealthCaritasCatalogParser()
    parser.feed(html_text)
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for cells, links in parser.rows:
        if len(cells) < 2 or cells[0].strip().lower() == "planid":
            continue
        plan_code = _clean_text(cells[0])
        plan_name = _clean_text(cells[1])
        provider_base = next(
            (
                base
                for href in links
                if (base := _provider_api_base_from_url(href))
            ),
            None,
        )
        if not plan_code or not plan_name or not provider_base:
            continue
        key = (plan_code.lower(), _payer_alias_key(plan_name), provider_base)
        if key in seen:
            continue
        seen.add(key)
        row = {
            "id": f"amerihealth-caritas-{plan_code.lower()}-{hashlib.sha1(plan_name.encode('utf-8')).hexdigest()[:8]}",
            "org_name": "AmeriHealth Caritas",
            "plan_name": plan_name,
            "api_base": provider_base,
            "auth_type": "none",
            "last_validated_status": "valid",
            "requires_registration": False,
            "source": "amerihealth-caritas-developer-portal",
            "source_detail": f"official AmeriHealth Caritas Provider Directory API plan_code={plan_code}",
            "source_url": source_url,
            "source_date": source_date,
            "note": "Supplemental Provider Directory source from AmeriHealth Caritas developer portal.",
        }
        if _seed_row_matches_query(row, source_query):
            rows.append(row)
    return rows


def _read_text_from_path_or_url(path: str | None, url: str, *, timeout: int) -> tuple[str, str]:
    clean_path = _clean_text(path)
    if clean_path:
        return Path(clean_path).read_text(encoding="utf-8"), clean_path
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace"), url


def _seed_rows_from_amerihealth_caritas_catalog(
    *,
    source_query: str | None = None,
    timeout: int = 30,
    catalog_path: str | None = None,
    catalog_url: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = (
        _clean_text(catalog_url)
        or _clean_text(os.getenv(AMERIHEALTH_CARITAS_DOC_URL_ENV))
        or AMERIHEALTH_CARITAS_DOC_URL
    )
    text, source = _read_text_from_path_or_url(catalog_path, url, timeout=timeout)
    rows = _amerihealth_caritas_seed_rows_from_catalog_html(
        text,
        source_query=source_query,
        source_url=source,
    )
    return rows, {"source": source, "rows": len(rows)}


def _contra_costa_seed_row(
    provider_base: str,
    *,
    source_url: str,
    source_date: str | None = None,
    note_suffix: str | None = None,
) -> dict[str, Any]:
    note = "Supplemental Provider Directory source from Contra Costa Health APIs for Developers page."
    if note_suffix:
        note = f"{note} {note_suffix}"
    return {
        "id": f"contra-costa-health-plan-{hashlib.sha1(provider_base.encode('utf-8')).hexdigest()[:8]}",
        "org_name": "Contra Costa Health Plan",
        "plan_name": "Contra Costa Health Plan Provider Directory",
        "api_base": provider_base,
        "auth_type": "none",
        "last_validated_status": "valid",
        "requires_registration": False,
        "is_medicaid_mco": True,
        "source": "contra-costa-health-developer-page",
        "source_detail": "official Contra Costa Health Provider Directory API base URL",
        "source_url": source_url,
        "source_date": source_date,
        "note": note,
    }


def _contra_costa_seed_rows_from_developer_html(
    html_text: str,
    *,
    source_query: str | None = None,
    source_url: str = CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL,
    source_date: str | None = None,
) -> list[dict[str, Any]]:
    parser = _HtmlLinkParser()
    parser.feed(html_text)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for label, href in parser.links:
        label_key = re.sub(r"[^a-z0-9]+", "", (_clean_text(label) or "").lower())
        if "providerdirectoryapibaseurl" not in label_key:
            continue
        target_url = _external_splash_target_url(href, source_url=source_url)
        provider_base = _provider_directory_base_from_metadata_url(target_url)
        if not provider_base or provider_base in seen:
            continue
        seen.add(provider_base)
        row = _contra_costa_seed_row(provider_base, source_url=source_url, source_date=source_date)
        if _seed_row_matches_query(row, source_query):
            rows.append(row)
    return rows


def _contra_costa_fallback_seed_rows(
    *,
    source_query: str | None = None,
    source_url: str = CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL,
) -> list[dict[str, Any]]:
    row = _contra_costa_seed_row(
        CONTRA_COSTA_PROVIDER_DIRECTORY_BASE,
        source_url=source_url,
        note_suffix=(
            "The official page can block automated fetches, so the importer retains the "
            "confirmed public metadata base as a fallback."
        ),
    )
    return [row] if _seed_row_matches_query(row, source_query) else []


def _seed_rows_from_contra_costa_catalog(
    *,
    source_query: str | None = None,
    timeout: int = 30,
    catalog_path: str | None = None,
    catalog_url: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = (
        _clean_text(catalog_url)
        or _clean_text(os.getenv(CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL_ENV))
        or CONTRA_COSTA_PROVIDER_DIRECTORY_DOC_URL
    )
    try:
        text, source = _read_text_from_path_or_url(catalog_path, url, timeout=timeout)
    except Exception as exc:
        rows = _contra_costa_fallback_seed_rows(source_query=source_query, source_url=url)
        return rows, {
            "source": url,
            "rows": len(rows),
            "fallback": True,
            "error": _short_error(exc),
        }
    rows = _contra_costa_seed_rows_from_developer_html(
        text,
        source_query=source_query,
        source_url=source,
    )
    if rows:
        return rows, {"source": source, "rows": len(rows)}
    rows = _contra_costa_fallback_seed_rows(source_query=source_query, source_url=source)
    return rows, {"source": source, "rows": len(rows), "fallback": True}


def _seed_rows_from_cms_sma_endpoint_directory(
    *,
    source_query: str | None = None,
    timeout: int = 30,
    catalog_path: str | None = None,
    catalog_url: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = (
        _clean_text(catalog_url)
        or _clean_text(os.getenv(CMS_SMA_ENDPOINT_DIRECTORY_URL_ENV))
        or CMS_SMA_ENDPOINT_DIRECTORY_URL
    )
    text, source = _read_text_from_path_or_url(catalog_path, url, timeout=timeout)
    rows = _cms_sma_endpoint_directory_seed_rows_from_csv(
        text,
        source_query=source_query,
        source_url=source,
    )
    return rows, {"source": source, "rows": len(rows)}


def _provider_directory_blocker_seed_rows(*, source_query: str | None = None) -> list[dict[str, Any]]:
    blocker_rows = [
        {
            "id": "provider-directory-blocked-chorus-community-health-plans",
            "org_name": "Chorus Community Health Plans (fka Children's Community Health Plan)",
            "plan_name": "Medicaid MCO",
            "api_base": None,
            "auth_type": "none",
            "last_validated_status": PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS,
            "requires_registration": False,
            "source": PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE,
            "source_detail": "blocked Provider Directory endpoint discovery",
            "source_url": CHORUS_PROVIDER_DIRECTORY_DOC_URL,
            "note": (
                "Official Chorus developer page exposes Provider Directory documentation through a "
                "JavaScript app, but no importable public FHIR base has been confirmed."
            ),
            "metadata_json": {
                "provider_directory_blocked": True,
                "provider_directory_blocked_reason": "official portal present but no importable public FHIR base confirmed",
                "provider_directory_confirmed_catalog_url": CHORUS_PROVIDER_DIRECTORY_DOC_URL,
            },
        },
        {
            "id": "provider-directory-blocked-first-medical-pr",
            "org_name": "First Medical Health Plan, Inc.",
            "plan_name": "Medicaid MCO",
            "api_base": None,
            "auth_type": "user token",
            "last_validated_status": PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS,
            "requires_registration": True,
            "source": PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE,
            "source_detail": "blocked Provider Directory endpoint discovery",
            "source_url": FIRST_MEDICAL_PROVIDER_DIRECTORY_DOC_URL,
            "note": (
                "Official First Medical developer portal lists Practitioner and Location APIs under "
                "a user-token security model; no open importable Provider Directory FHIR base has "
                "been confirmed."
            ),
            "metadata_json": {
                "provider_directory_blocked": True,
                "provider_directory_blocked_reason": "official portal requires user token and no open public FHIR base is confirmed",
                "provider_directory_confirmed_catalog_url": FIRST_MEDICAL_PROVIDER_DIRECTORY_DOC_URL,
            },
        },
        {
            "id": "provider-directory-blocked-territory-of-puerto-rico",
            "org_name": "Territory of Puerto Rico",
            "plan_name": "Medicaid FFS",
            "api_base": None,
            "auth_type": "none",
            "last_validated_status": PROVIDER_DIRECTORY_CATALOG_BLOCKED_STATUS,
            "requires_registration": False,
            "source": PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE,
            "source_detail": "CMS SMA Endpoint Directory Provider Directory status not yet started",
            "source_url": CMS_SMA_ENDPOINT_DIRECTORY_URL,
            "note": (
                "CMS SMA Endpoint Directory lists Puerto Rico Provider Directory implementation as "
                "not yet started/TBD, so no importable public FHIR base is currently known."
            ),
            "metadata_json": {
                "provider_directory_blocked": True,
                "provider_directory_blocked_reason": "CMS SMA Endpoint Directory lists Provider Directory as not yet started/TBD",
                "provider_directory_confirmed_catalog_url": CMS_SMA_ENDPOINT_DIRECTORY_URL,
            },
        },
    ]
    return [row for row in blocker_rows if _seed_row_matches_query(row, source_query)]


def _health_partners_plans_seed_rows(*, source_query: str | None = None) -> list[dict[str, Any]]:
    row = {
        "id": "health-partners-plans-provider-directory",
        "org_name": "Health Partners Plans",
        "plan_name": "Health Partners Plans Provider Directory",
        "api_base": HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE,
        "auth_type": "none",
        "last_validated_status": "valid",
        "requires_registration": False,
        "source": "health-partners-plans-fhir-root",
        "source_detail": "official Health Partners Plans public Provider Directory FHIR server",
        "source_url": HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE,
        "note": "Supplemental Provider Directory source from the Health Partners Plans public FHIR server.",
    }
    return [row] if _seed_row_matches_query(row, source_query) else []


def _seed_rows_from_supplemental_catalogs(
    *,
    source_query: str | None = None,
    timeout: int = 30,
    amerihealth_caritas_catalog_path: str | None = None,
    amerihealth_caritas_catalog_url: str | None = None,
    contra_costa_catalog_path: str | None = None,
    contra_costa_catalog_url: str | None = None,
    cms_sma_endpoint_directory_path: str | None = None,
    cms_sma_endpoint_directory_url: str | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {"catalogs": {}}
    try:
        catalog_rows, catalog_metrics = _seed_rows_from_amerihealth_caritas_catalog(
            source_query=source_query,
            timeout=timeout,
            catalog_path=amerihealth_caritas_catalog_path,
            catalog_url=amerihealth_caritas_catalog_url,
        )
        rows.extend(catalog_rows)
        metrics["catalogs"]["amerihealth_caritas"] = catalog_metrics
    except Exception as exc:
        metrics["catalogs"]["amerihealth_caritas"] = {
            "rows": 0,
            "error": _short_error(exc),
        }
    try:
        catalog_rows, catalog_metrics = _seed_rows_from_contra_costa_catalog(
            source_query=source_query,
            timeout=timeout,
            catalog_path=contra_costa_catalog_path,
            catalog_url=contra_costa_catalog_url,
        )
        rows.extend(catalog_rows)
        metrics["catalogs"]["contra_costa"] = catalog_metrics
    except Exception as exc:
        metrics["catalogs"]["contra_costa"] = {
            "rows": 0,
            "error": _short_error(exc),
        }
    try:
        catalog_rows, catalog_metrics = _seed_rows_from_cms_sma_endpoint_directory(
            source_query=source_query,
            timeout=timeout,
            catalog_path=cms_sma_endpoint_directory_path,
            catalog_url=cms_sma_endpoint_directory_url,
        )
        rows.extend(catalog_rows)
        metrics["catalogs"]["cms_sma_endpoint_directory"] = catalog_metrics
    except Exception as exc:
        metrics["catalogs"]["cms_sma_endpoint_directory"] = {
            "rows": 0,
            "error": _short_error(exc),
        }
    catalog_rows = _health_partners_plans_seed_rows(source_query=source_query)
    rows.extend(catalog_rows)
    metrics["catalogs"]["health_partners_plans"] = {
        "source": HEALTH_PARTNERS_PLANS_PROVIDER_DIRECTORY_BASE,
        "rows": len(catalog_rows),
    }
    catalog_rows = _aetna_provider_directory_data_seed_rows(source_query=source_query)
    rows.extend(catalog_rows)
    metrics["catalogs"]["aetna_provider_directorydata"] = {
        "source": AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "rows": len(catalog_rows),
    }
    catalog_rows = _provider_directory_blocker_seed_rows(source_query=source_query)
    rows.extend(catalog_rows)
    metrics["catalogs"]["provider_directory_blockers"] = {
        "source": PROVIDER_DIRECTORY_BLOCKER_REGISTRY_SOURCE,
        "rows": len(catalog_rows),
    }
    metrics["rows"] = len(rows)
    return rows, metrics


def _seed_rows_from_retest_results(path: Path, *, source_query: str | None = None) -> list[dict[str, Any]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Provider Directory retest results could not be read from {path}: {exc}") from exc
    results = payload.get("results") if isinstance(payload, dict) else payload
    if not isinstance(results, list):
        raise RuntimeError(f"Provider Directory retest results at {path} must be a JSON object/list with results.")
    seed_rows: list[dict[str, Any]] = []
    allowed_classifications = {"valid", "valid_non_fhir", "auth_required"}
    for index, item in enumerate(results):
        if not isinstance(item, dict):
            continue
        classification = (_clean_text(item.get("classification")) or "").lower()
        api_base = _clean_text(item.get("api_base"))
        org_name = _clean_text(item.get("org_name"))
        if not api_base or not org_name:
            continue
        is_auth_required = classification == "auth_required"
        row = {
            "id": _clean_text(item.get("payer_id")) or f"retest-{index}",
            "org_name": org_name,
            "plan_name": _clean_text(item.get("plan_name")) or "Provider Directory Retest",
            "api_base": api_base,
            "auth_type": (
                "open"
                if classification == "valid"
                else _clean_text(item.get("auth_type")) or (
                    "OAuth2 Client Credentials" if is_auth_required else "none"
                )
            ),
            "last_validated_status": classification,
            "requires_registration": is_auth_required,
            "fhir_version": _clean_text(item.get("fhir_version")),
            "source": "provider-directory-db-retest",
            "note": "Supplemental source from provider-directory-db retest_results.json",
            "source_detail": f"retest classification={classification}; status_code={item.get('status_code')}",
            "source_url": _clean_text(item.get("url")),
            "source_date": _clean_text(payload.get("tested_at")) if isinstance(payload, dict) else None,
        }
        if classification not in allowed_classifications and not _seed_row_has_recoverable_provider_directory_base(row):
            continue
        if _seed_row_matches_query(row, source_query):
            seed_rows.append(row)
    return seed_rows


def _append_unique_metadata_value(metadata: dict[str, Any], key: str, value: str | None) -> None:
    clean_value = _clean_text(value)
    if not clean_value:
        return
    values = metadata.get(key)
    if not isinstance(values, list):
        values = []
    if clean_value not in values:
        values.append(clean_value)
    metadata[key] = values


def _merge_skipped_source_row_metadata(target: dict[str, Any], skipped: dict[str, Any]) -> None:
    target_metadata = target.setdefault("metadata_json", {})
    if not isinstance(target_metadata, dict):
        target_metadata = {}
        target["metadata_json"] = target_metadata
    skipped_metadata = skipped.get("metadata_json") if isinstance(skipped.get("metadata_json"), dict) else {}
    target_base = _canonical_base(target.get("canonical_api_base") or target.get("api_base"))
    for raw_base in (
        skipped.get("api_base"),
        skipped.get("canonical_api_base"),
        skipped_metadata.get("provider_directory_previous_api_base"),
        skipped_metadata.get("provider_directory_confirmed_base"),
    ):
        base = _canonical_base(raw_base)
        if base and base != target_base:
            _append_unique_metadata_value(target_metadata, "provider_directory_equivalent_api_bases", base)
    skipped_override = _clean_text(skipped_metadata.get("provider_directory_override"))
    if skipped_override:
        _append_unique_metadata_value(target_metadata, "provider_directory_merged_overrides", skipped_override)


def _dedupe_source_rows(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen_source_ids: set[str] = set()
    seen_source_keys: set[tuple[str, str, str]] = set()
    seen_org_base_keys: set[tuple[str, str]] = set()
    rows_by_source_id: dict[str, dict[str, Any]] = {}
    rows_by_source_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    rows_by_org_base_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in source_rows:
        source_id = _clean_text(row.get("source_id"))
        org_name_key = (_clean_text(row.get("org_name")) or "").lower()
        plan_name_key = (_clean_text(row.get("plan_name")) or "").lower()
        api_base_key = _canonical_base(row.get("canonical_api_base") or row.get("api_base")) or ""
        source_key = (org_name_key, plan_name_key, api_base_key)
        org_base_key = (org_name_key, api_base_key)
        is_retest = row.get("seed_source") == "provider-directory-db-retest"
        if source_id and source_id in seen_source_ids:
            _merge_skipped_source_row_metadata(rows_by_source_id[source_id], row)
            continue
        if is_retest and (source_key in seen_source_keys or org_base_key in seen_org_base_keys):
            retained = rows_by_source_key.get(source_key) or rows_by_org_base_key.get(org_base_key)
            if retained is not None:
                _merge_skipped_source_row_metadata(retained, row)
            continue
        if source_id:
            seen_source_ids.add(source_id)
            rows_by_source_id[source_id] = row
        if is_retest:
            seen_source_keys.add(source_key)
            rows_by_source_key[source_key] = row
        if org_name_key and api_base_key:
            seen_org_base_keys.add(org_base_key)
            rows_by_org_base_key[org_base_key] = row
        deduped.append(row)
    return deduped


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


def _download_retest_results(retest_results_url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(retest_results_url, headers={"User-Agent": USER_AGENT})
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as handle:
        shutil.copyfileobj(response, handle)
    tmp.replace(destination)
    return destination


def _resolve_retest_results(
    retest_results_path: str | None,
    retest_results_url: str | None,
) -> tuple[Path | None, tempfile.TemporaryDirectory | None]:
    if retest_results_path:
        return Path(retest_results_path), None
    if not retest_results_url:
        retest_results_url = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_RETEST_RESULTS_URL")
    if not retest_results_url:
        return None, None
    tmpdir = tempfile.TemporaryDirectory()
    path = Path(tmpdir.name) / "retest_results.json"
    return _download_retest_results(retest_results_url, path), tmpdir


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
    except Exception as exc:
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
    except Exception as exc:
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


def _source_fetch_retry_attempts() -> int:
    return max(1, _env_int("HLTHPRT_PROVIDER_DIRECTORY_FETCH_ATTEMPTS", 3))


def _is_transient_source_fetch_failure(
    status_code: int | None,
    fetch_error: str | None,
) -> bool:
    if fetch_error:
        return True
    return status_code in {429, 500, 502, 503, 504}


def _source_fetch_retry_delay_seconds(attempt_index: int) -> float:
    return min(2.0, 0.25 * (2**max(0, attempt_index)))


def _source_fetch_candidate_urls(url: str) -> list[str]:
    parsed = urllib.parse.urlsplit(url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query_by_name = dict(query_items)
    if FHIR_CONTINUATION_QUERY_NAMES.intersection(
        name.lower() for name in query_by_name
    ):
        return [url]
    try:
        requested_count = int(query_by_name.get("_count") or 0)
    except (TypeError, ValueError):
        return [url]
    if requested_count <= 1:
        return [url]
    candidate_urls = [url]
    for fallback_count in (100, 25, 10, 1):
        if fallback_count >= requested_count:
            continue
        candidate_urls.append(_url_with_replaced_query_item(url, "_count", str(fallback_count)))
    return candidate_urls


def _should_reduce_source_page_size(
    status_code: int | None,
    fetch_error: str | None,
) -> bool:
    return fetch_error is None and status_code in {400, 413}


async def _fetch_source_json_once(
    source_record: dict[str, Any],
    url: str,
    *,
    timeout: int,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    options = _credential_request_options_for_source(source_record, url)
    if not options["headers"] and not options["query_params"]:
        return await _fetch_json(url, timeout=timeout)
    return await _fetch_json_with_options(
        url,
        timeout=timeout,
        extra_headers=options["headers"],
        query_params=options["query_params"],
    )


async def _fetch_source_json(
    source_record: dict[str, Any],
    url: str,
    *,
    timeout: int,
) -> tuple[int | None, dict[str, Any] | None, str | None, int]:
    total_elapsed_ms = 0
    fetch_result: tuple[int | None, dict[str, Any] | None, str | None, int] = (
        None,
        None,
        "request_not_attempted",
        0,
    )
    candidate_urls = _source_fetch_candidate_urls(url)
    for candidate_index, candidate_url in enumerate(candidate_urls):
        is_last_candidate = candidate_index == len(candidate_urls) - 1
        should_try_smaller_page = False
        attempt_count = _source_fetch_retry_attempts()
        for attempt_index in range(attempt_count):
            fetch_result = await _fetch_source_json_once(
                source_record,
                candidate_url,
                timeout=timeout,
            )
            status_code, fhir_payload, fetch_error, elapsed_ms = fetch_result
            total_elapsed_ms += elapsed_ms
            should_try_smaller_page = _should_reduce_source_page_size(
                status_code,
                fetch_error,
            )
            if should_try_smaller_page and not is_last_candidate:
                break
            if not _is_transient_source_fetch_failure(status_code, fetch_error):
                return status_code, fhir_payload, fetch_error, total_elapsed_ms
            if attempt_index + 1 < attempt_count:
                await asyncio.sleep(_source_fetch_retry_delay_seconds(attempt_index))
        if not should_try_smaller_page:
            return fetch_result[0], fetch_result[1], fetch_result[2], total_elapsed_ms
    return fetch_result[0], fetch_result[1], fetch_result[2], total_elapsed_ms


RESOURCE_ACCESS_PROBE_ORDER = (
    "Practitioner",
    "PractitionerRole",
    "Location",
    "Organization",
    "InsurancePlan",
)


def _capability_resource_types(capability_payload: dict[str, Any]) -> set[str]:
    resource_types: set[str] = set()
    for rest_item in capability_payload.get("rest") or []:
        if not isinstance(rest_item, dict):
            continue
        for resource_item in rest_item.get("resource") or []:
            if not isinstance(resource_item, dict):
                continue
            resource_type = _clean_text(resource_item.get("type"))
            if resource_type:
                resource_types.add(resource_type)
    return resource_types


def _is_access_denied_outcome(resource_payload: dict[str, Any] | None) -> bool:
    if not resource_payload or resource_payload.get("resourceType") != "OperationOutcome":
        return False
    issue_text = " ".join(
        str(issue.get("diagnostics") or issue.get("details", {}).get("text") or "")
        for issue in resource_payload.get("issue") or []
        if isinstance(issue, dict)
    ).lower()
    return any(
        marker in issue_text
        for marker in ("access denied", "not authorized", "not authorised", "forbidden")
    )


async def _probe_resource_access(
    source_record: dict[str, Any],
    candidate_base: str,
    capability_payload: dict[str, Any],
    *,
    timeout: int,
) -> dict[str, Any] | None:
    if _alohr_source_uses_graphql_connector(source_record):
        return None
    supported_resource_types = _capability_resource_types(capability_payload)
    resource_type = next(
        (
            candidate_resource_type
            for candidate_resource_type in RESOURCE_ACCESS_PROBE_ORDER
            if candidate_resource_type in supported_resource_types
        ),
        None,
    )
    if not resource_type:
        return None
    resource_probe_source_map = {
        **source_record,
        "api_base": candidate_base,
        "canonical_api_base": candidate_base,
    }
    resource_url = _resource_start_url(resource_probe_source_map, resource_type, page_count=1)
    if not resource_url:
        return None
    status_code, resource_payload, error, elapsed = await _fetch_source_json(
        resource_probe_source_map,
        resource_url,
        timeout=timeout,
    )
    is_access_denied = status_code in {401, 403} or _is_access_denied_outcome(resource_payload)
    return {
        "status": "auth_required" if is_access_denied else "reachable",
        "http_status": status_code,
        "response_time_ms": elapsed,
        "url": resource_url,
        "resource_type": resource_type,
        "error": error or ("resource access denied" if is_access_denied else None),
    }


async def _probe_metadata_candidate(
    source_record: dict[str, Any],
    candidate_base: str,
    metadata_url: str,
    *,
    timeout: int,
    run_id: str | None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    status_code, capability_payload, error, elapsed = await _fetch_source_json(
        source_record,
        metadata_url,
        timeout=timeout,
    )
    status = _classify_http(status_code, error, capability_payload)
    credential = _credential_request_options_for_source(source_record, metadata_url)
    is_credential_missing = not credential["descriptor"] and not _alohr_source_uses_graphql_connector(
        source_record
    )
    if status == "valid" and _source_declares_credentialed_access(source_record) and is_credential_missing:
        status = "auth_required"
        error = error or "source requires credentialed Provider Directory resource access but no matching credentials are configured"
    if status == "valid_non_fhir" and is_credential_missing and (
        _source_declares_credentialed_access(source_record)
        or _source_uses_known_onboarding_gateway(source_record)
    ):
        status = "auth_required"
        error = error or "source requires credentialed Provider Directory access but no matching credentials are configured"
    resource_probe_map = None
    if status == "valid" and isinstance(capability_payload, dict):
        resource_probe_map = await _probe_resource_access(
            source_record,
            candidate_base,
            capability_payload,
            timeout=timeout,
        )
        if resource_probe_map and resource_probe_map["status"] == "auth_required":
            status = "auth_required"
            status_code = resource_probe_map.get("http_status")
            error = resource_probe_map.get("error") or "Provider Directory resource access requires authorization"
    candidate_probe_map = {
        "status": status,
        "http_status": status_code,
        "response_time_ms": elapsed,
        "url": metadata_url,
        "api_base": candidate_base,
        "error": error,
        "run_id": run_id,
        "credential": credential["descriptor"],
        "resource_probe": resource_probe_map,
    }
    return candidate_probe_map, capability_payload


async def _probe_source(source_record: dict[str, Any], *, timeout: int, run_id: str | None) -> tuple[dict[str, Any], dict[str, Any] | None]:
    metadata_urls = _candidate_metadata_urls(source_record)
    if not metadata_urls:
        source_probe_map = {
            "status": "no_api",
            "http_status": None,
            "response_time_ms": 0,
            "url": None,
            "error": "missing api_base",
            "run_id": run_id,
        }
        return source_probe_map, None
    missing_credential_blocks_probe = (
        (
            _source_declares_credentialed_access(source_record)
            or _source_uses_known_onboarding_gateway(source_record)
        )
        and not _alohr_source_uses_graphql_connector(source_record)
        and not any(
            _credential_request_options_for_source(source_record, metadata_url)["descriptor"]
            for _candidate_base, metadata_url in metadata_urls
        )
    )
    if missing_credential_blocks_probe:
        candidate_base, metadata_url = metadata_urls[0]
        return (
            {
                "status": "auth_required",
                "http_status": None,
                "response_time_ms": 0,
                "url": metadata_url,
                "api_base": candidate_base,
                "error": (
                    "source requires credentialed Provider Directory access "
                    "but no matching credentials are configured"
                ),
                "run_id": run_id,
                "credential": None,
            },
            None,
        )
    best_probe: dict[str, Any] | None = None
    best_payload: dict[str, Any] | None = None
    for candidate_base, metadata_url in metadata_urls:
        candidate_probe_map, capability_payload = await _probe_metadata_candidate(
            source_record,
            candidate_base,
            metadata_url,
            timeout=timeout,
            run_id=run_id,
        )
        if candidate_probe_map["status"] == "valid":
            return candidate_probe_map, capability_payload
        if best_probe is None or candidate_probe_map.get("http_status") == 200:
            best_probe = candidate_probe_map
            best_payload = capability_payload
    assert best_probe is not None
    return best_probe, best_payload



def _source_probe_hard_timeout_seconds(source: dict[str, Any], *, timeout: int) -> int:
    candidate_count = max(1, len(_candidate_metadata_urls(source)))
    request_count = candidate_count * 2
    env_value = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_SOURCE_PROBE_HARD_TIMEOUT")
    if env_value:
        try:
            return max(1, int(env_value))
        except ValueError:
            return max(timeout + 1, (timeout * request_count) + 2)
    return max(timeout + 1, (timeout * request_count) + 2)


def _probe_flush_every() -> int:
    try:
        return max(1, int(os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROBE_FLUSH_EVERY", "50")))
    except ValueError:
        return 50


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
    probe_counts_by_name = {"valid": 0, "probed": 0}
    total_sources = len(sources)
    report_every = max(1, total_sources // 20)
    flush_every = _probe_flush_every()
    flush_lock = asyncio.Lock()

    await _mark_provider_directory_progress(
        run_id,
        phase="provider-directory probing sources",
        done=0,
        total=total_sources,
        message=f"probing 0/{total_sources} source(s)",
    )

    async def _flush_probe_rows(*, force: bool = False) -> None:
        async with flush_lock:
            if not force and len(source_updates) < flush_every:
                return
            capability_flush_rows = list(capability_rows)
            source_update_rows = list(source_updates)
            capability_rows.clear()
            source_updates.clear()
        if capability_flush_rows:
            await _upsert_rows(ProviderDirectoryCapability, capability_flush_rows)
        if source_update_rows:
            await _upsert_rows(ProviderDirectorySource, source_update_rows)

    async def _run_probe(source: dict[str, Any]) -> None:
        async with semaphore:
            started = time.monotonic()
            hard_timeout = _source_probe_hard_timeout_seconds(source, timeout=timeout)
            try:
                probe, payload = await asyncio.wait_for(
                    _probe_source(source, timeout=timeout, run_id=run_id),
                    timeout=hard_timeout,
                )
            except TimeoutError:
                probe = {
                    "status": "timeout",
                    "http_status": None,
                    "response_time_ms": int((time.monotonic() - started) * 1000),
                    "url": None,
                    "api_base": _canonical_base(source.get("api_base")),
                    "error": f"source metadata probe exceeded {hard_timeout}s hard deadline",
                    "run_id": run_id,
                    "credential": None,
                }
                payload = None
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
                if probe["status"] == "valid":
                    probe_counts_by_name["valid"] += 1
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
            probe_counts_by_name["probed"] += 1
            await _flush_probe_rows()
            if probe_counts_by_name["probed"] == total_sources or probe_counts_by_name["probed"] % report_every == 0:
                await _mark_provider_directory_progress(
                    run_id,
                    phase="provider-directory probing sources",
                    done=probe_counts_by_name["probed"],
                    total=total_sources,
                    message=f"probed {probe_counts_by_name['probed']}/{total_sources} source(s); valid={probe_counts_by_name['valid']}",
                )

    await asyncio.gather(*[_run_probe(source) for source in sources])
    await _flush_probe_rows(force=True)
    return len(sources), probe_counts_by_name["valid"], valid_source_ids


def _is_bundle_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if payload.get("resourceType") == "Bundle":
        return True
    # Aetna's /providerdirectorydata search responses are Bundle-shaped but can
    # omit the top-level resourceType while still including entry/link/type.
    return isinstance(payload.get("entry"), list) and isinstance(payload.get("link"), list)


def _bundle_entries(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not _is_bundle_payload(payload):
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


def _resolved_fhir_next_url(
    source_record: dict[str, Any],
    current_url: str,
    next_link: str | None,
) -> str | None:
    if not next_link:
        return None
    next_url = urllib.parse.urljoin(current_url, next_link)
    api_base = _canonical_base(
        source_record.get("canonical_api_base") or source_record.get("api_base")
    )
    parsed_next = urllib.parse.urlsplit(next_url)
    if api_base == MOLINA_PROVIDER_DIRECTORY_BASE:
        return _resolved_molina_next_url(current_url, next_url)
    if api_base != HAP_PROVIDER_DIRECTORY_BASE:
        return next_url
    if parsed_next.netloc.lower() != HAP_BLOCKED_PAGINATION_HOST:
        return next_url
    public_base = urllib.parse.urlsplit(HAP_PROVIDER_DIRECTORY_BASE)
    return urllib.parse.urlunsplit(
        (
            public_base.scheme,
            public_base.netloc,
            parsed_next.path,
            parsed_next.query,
            parsed_next.fragment,
        )
    )


def _resolved_molina_next_url(current_url: str, next_url: str) -> str:
    canonical_base = urllib.parse.urlsplit(MOLINA_PROVIDER_DIRECTORY_BASE)
    parsed_next = urllib.parse.urlsplit(next_url)
    if (
        parsed_next.scheme.lower() == canonical_base.scheme
        and parsed_next.netloc.lower() == canonical_base.netloc
    ):
        return next_url

    parsed_current = urllib.parse.urlsplit(current_url)
    resource_prefix = f"{canonical_base.path.rstrip('/')}/"
    resource_type = (
        parsed_current.path[len(resource_prefix) :]
        if parsed_current.path.startswith(resource_prefix)
        else ""
    )
    cursor_values = [
        cursor_value
        for key, cursor_value in urllib.parse.parse_qsl(
            parsed_next.query,
            keep_blank_values=True,
        )
        if key == "cursorMark"
    ]
    is_allowlisted = (
        parsed_next.scheme.lower() == "https"
        and parsed_next.netloc.lower() == MOLINA_PAGINATION_HOST
        and not parsed_next.fragment
        and resource_type in DEFAULT_RESOURCES
        and parsed_next.path == f"/fhir/{resource_type}"
        and len(cursor_values) == 1
    )
    if not is_allowlisted:
        raise ValueError("untrusted_molina_pagination_link")
    return urllib.parse.urlunsplit(
        (
            canonical_base.scheme,
            canonical_base.netloc,
            f"{canonical_base.path.rstrip('/')}/{resource_type}",
            parsed_next.query,
            "",
        )
    )


def _bulk_export_start_url(source: dict[str, Any], resource_type: str) -> str | None:
    api_base = _canonical_base(source.get("api_base"))
    if not api_base:
        return None
    query_params_by_name = {"_type": resource_type}
    metadata = _source_metadata(source)
    should_omit_output_format = (
        api_base == AETNA_PROVIDER_DIRECTORY_DATA_BASE
        or metadata.get("provider_directory_bulk_export_omit_output_format") is True
    )
    if not should_omit_output_format:
        query_params_by_name["_outputFormat"] = "application/fhir+ndjson"
    query = urllib.parse.urlencode(query_params_by_name)
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


def _bulk_export_status_payload(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    return any(key in payload for key in ("transactionTime", "request", "requiresAccessToken", "output", "error"))


def _bulk_export_payload_error(payload: dict[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return "bulk_export_invalid_status_payload"
    errors = payload.get("error")
    if not isinstance(errors, list) or not errors:
        return None
    first_error = next((item for item in errors if isinstance(item, dict)), None)
    if not first_error:
        return "bulk_export_error_output"
    error_type = _clean_text(first_error.get("type"))
    response_code = _clean_text(first_error.get("responseCode") or first_error.get("response_code"))
    if response_code:
        return f"bulk_export_error_http_{response_code}"
    if error_type:
        return f"bulk_export_error_{error_type}"
    return "bulk_export_error_output"


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


def _bulk_export_log_url(url: str | None) -> str | None:
    text = _clean_text(url)
    if not text:
        return None
    parsed = urllib.parse.urlsplit(text)
    if not parsed.netloc:
        return parsed.path or text
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def _bulk_export_log(event: str, **details: Any) -> None:
    rendered = " ".join(
        f"{key}={value}"
        for key, value in details.items()
        if value is not None and value != ""
    )
    suffix = f" {rendered}" if rendered else ""
    print(f"Provider Directory bulk export {event}{suffix}", flush=True)


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
    except Exception as exc:
        return None, {}, None, f"{type(exc).__name__}: {exc}"


async def _bulk_export_poll_outputs(
    session: aiohttp.ClientSession,
    source: dict[str, Any],
    status_url: str,
    *,
    resource_type: str,
    timeout: int,
) -> tuple[list[str] | None, str | None, int]:
    """Poll a Bulk Data status URL until output URLs, an error, or timeout."""
    max_polls = _env_int("HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT_MAX_POLLS", DEFAULT_BULK_EXPORT_MAX_POLLS)
    poll_seconds = _env_int(
        "HLTHPRT_PROVIDER_DIRECTORY_BULK_EXPORT_POLL_SECONDS",
        DEFAULT_BULK_EXPORT_POLL_SECONDS,
    )
    for poll in range(1, max(1, max_polls) + 1):
        status_code, headers, payload, error = await _bulk_http_get_json(
            session,
            source,
            status_url,
            timeout=timeout,
        )
        if error:
            _bulk_export_log_poll_error(source, resource_type, poll, max_polls, error)
            return None, error, poll
        if status_code == 202:
            await _bulk_export_sleep_after_poll_wait(
                source,
                resource_type,
                poll,
                max_polls,
                headers,
                poll_seconds,
                status_url,
            )
            continue
        if status_code == 200:
            output_urls, payload_error = _bulk_export_poll_ready_result(
                source,
                resource_type,
                poll,
                payload,
            )
            return output_urls, payload_error, poll
        _bulk_export_log_poll_http_error(source, resource_type, poll, status_code)
        return None, f"bulk_export_status_http_{status_code}", poll
    _bulk_export_log(
        "poll_timeout",
        source_id=source.get("source_id"),
        resource=resource_type,
        polls=max(1, max_polls),
        max_polls=max_polls,
        status_url=_bulk_export_log_url(status_url),
    )
    return None, "bulk_export_timeout", max(1, max_polls)


def _bulk_export_log_poll_error(
    source: dict[str, Any],
    resource_type: str,
    poll: int,
    max_polls: int,
    error: str,
) -> None:
    _bulk_export_log(
        "poll_error",
        source_id=source.get("source_id"),
        resource=resource_type,
        poll=poll,
        max_polls=max_polls,
        error=error,
    )


async def _bulk_export_sleep_after_poll_wait(
    source: dict[str, Any],
    resource_type: str,
    poll: int,
    max_polls: int,
    headers: dict[str, str],
    poll_seconds: int,
    status_url: str,
) -> None:
    sleep_seconds = _retry_after_seconds(headers.get("retry-after")) or poll_seconds
    if poll == 1 or poll % 10 == 0:
        _bulk_export_log(
            "poll_wait",
            source_id=source.get("source_id"),
            resource=resource_type,
            poll=poll,
            max_polls=max_polls,
            status=202,
            sleep_seconds=sleep_seconds,
            status_url=_bulk_export_log_url(status_url),
        )
    await asyncio.sleep(sleep_seconds)


def _bulk_export_poll_ready_result(
    source: dict[str, Any],
    resource_type: str,
    poll: int,
    payload: dict[str, Any] | None,
) -> tuple[list[str] | None, str | None]:
    if not _bulk_export_status_payload(payload):
        _bulk_export_log(
            "poll_invalid_payload",
            source_id=source.get("source_id"),
            resource=resource_type,
            poll=poll,
            status=200,
        )
        return None, "bulk_export_status_non_bulk_payload"
    output_urls = _bulk_export_output_urls(payload, resource_type)
    payload_error = _bulk_export_payload_error(payload)
    _bulk_export_log(
        "poll_ready",
        source_id=source.get("source_id"),
        resource=resource_type,
        poll=poll,
        status=200,
        outputs=len(output_urls),
        payload_error=payload_error,
    )
    if payload_error and not output_urls:
        return None, payload_error
    return output_urls, None


def _bulk_export_log_poll_http_error(
    source: dict[str, Any],
    resource_type: str,
    poll: int,
    status_code: int | None,
) -> None:
    _bulk_export_log(
        "poll_http_error",
        source_id=source.get("source_id"),
        resource=resource_type,
        poll=poll,
        status=status_code,
    )


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
    stream_counts_by_name = {"rows_fetched": 0, "rows_written": 0}
    row_limit_by_name = {"reached": False}

    async def flush_pending_rows() -> None:
        if row_batch_handler and pending_rows:
            pending_rows_list = list(pending_rows)
            pending_rows.clear()
            stream_counts_by_name["rows_written"] += await row_batch_handler(model, pending_rows_list)

    def handle_line(raw_line: bytes) -> str | None:
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
            normalize_location_contacts=not (resource_type == "Location" and row_batch_handler),
        )
        if not parsed:
            return None
        parsed_model, row = parsed
        if parsed_model is not model:
            return None
        stream_counts_by_name["rows_fetched"] += 1
        if retain_rows:
            rows.append(row)
        if row_batch_handler:
            pending_rows.append(row)
        if not _limit_allows_more(stream_counts_by_name["rows_fetched"], per_resource_limit):
            row_limit_by_name["reached"] = True
        return None

    try:
        _bulk_export_log(
            "stream_start",
            source_id=source.get("source_id"),
            resource=resource_type,
            output_url=_bulk_export_log_url(url),
            row_batch_size=row_batch_size,
        )
        async with session.get(
            fetch_url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=timeout, sock_read=timeout),
            allow_redirects=True,
            ssl=_ssl_context(),
        ) as response:
            if response.status != 200:
                return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, f"bulk_export_output_http_{response.status}")
            buffer = b""
            async for chunk in response.content.iter_chunked(READ_CHUNK_BYTES):
                buffer += chunk
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    error = handle_line(line)
                    if error:
                        return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, error)
                    if row_batch_handler and len(pending_rows) >= max(1, row_batch_size):
                        await flush_pending_rows()
                    if row_limit_by_name["reached"]:
                        await flush_pending_rows()
                        return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, None)
            if buffer.strip():
                error = handle_line(buffer)
                if error:
                    return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, error)
            await flush_pending_rows()
            return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, None)
    except Exception as exc:
        return _bulk_stream_result(rows, stream_counts_by_name, row_limit_by_name, f"{type(exc).__name__}: {exc}")


def _bulk_stream_result(
    rows: list[dict[str, Any]],
    stream_counts_by_name: dict[str, int],
    row_limit_by_name: dict[str, bool],
    error: str | None,
) -> tuple[list[dict[str, Any]], int, int, bool, str | None]:
    return (
        rows,
        stream_counts_by_name["rows_fetched"],
        stream_counts_by_name["rows_written"],
        row_limit_by_name["reached"],
        error,
    )


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
        status_url = _clean_text(headers.get("content-location") or headers.get("location"))
        _bulk_export_log(
            "start",
            source_id=source.get("source_id"),
            resource=resource_type,
            status=status_code,
            error=error,
            start_url=_bulk_export_log_url(url),
            status_url=_bulk_export_log_url(status_url),
        )
        if _bulk_export_pre_stream_should_fallback(status_code, error):
            return None

        polls = 0
        if status_code == 200:
            if not _bulk_export_status_payload(_payload):
                return None
            output_urls = _bulk_export_output_urls(_payload, resource_type)
            payload_error = _bulk_export_payload_error(_payload)
            if payload_error and not output_urls:
                return ResourceFetchResult(
                    model=model,
                    rows=[],
                    rows_fetched=0,
                    rows_written=0,
                    pages_fetched=1,
                    complete=False,
                    row_limit_reached=False,
                    page_limit_reached=False,
                    hard_page_limit_reached=False,
                    next_url_remaining=False,
                    error=payload_error,
                    fetch_mode="bulk_export",
                )
        else:
            if not status_url:
                return ResourceFetchResult(
                    model=model,
                    rows=[],
                    rows_fetched=0,
                    rows_written=0,
                    pages_fetched=1,
                    complete=False,
                    row_limit_reached=False,
                    page_limit_reached=False,
                    hard_page_limit_reached=False,
                    next_url_remaining=False,
                    error="bulk_export_missing_status_url",
                    fetch_mode="bulk_export",
                )
            output_urls, poll_error, polls = await _bulk_export_poll_outputs(
                session,
                source,
                urllib.parse.urljoin(url, status_url),
                resource_type=resource_type,
                timeout=timeout,
            )
            if poll_error is not None:
                return ResourceFetchResult(
                    model=model,
                    rows=[],
                    rows_fetched=0,
                    rows_written=0,
                    pages_fetched=max(1, polls),
                    complete=False,
                    row_limit_reached=False,
                    page_limit_reached=False,
                    hard_page_limit_reached=False,
                    next_url_remaining=True,
                    error=poll_error,
                    fetch_mode="bulk_export",
                )
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
            _bulk_export_log(
                "stream_output",
                source_id=source.get("source_id"),
                resource=resource_type,
                fetched=fetched,
                written=written,
                limited=limited,
                error=error,
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
async def _flush_partition_resource_rows(
    state: PartitionFetchState,
    row_batch_handler: Callable[[type, list[dict[str, Any]]], Awaitable[int]] | None,
    pending_resource_rows: list[dict[str, Any]],
) -> None:
    if not row_batch_handler or not pending_resource_rows:
        return
    resource_batch_rows = list(pending_resource_rows)
    pending_resource_rows.clear()
    async with state.writer_lock:
        written_count = await row_batch_handler(state.result_model, resource_batch_rows)
    async with state.state_lock:
        state.written_count += written_count


async def _can_fetch_partition_url(
    state: PartitionFetchState,
    request_url: str,
    *,
    max_pages: int,
) -> bool:
    async with state.state_lock:
        if state.stop_event.is_set():
            return False
        if max_pages > 0 and state.page_count >= max_pages:
            state.hard_page_limit_reached = True
            state.next_url_remaining = True
            state.stop_event.set()
            return False
        request_identity = _pagination_url_identity(request_url)
        if request_identity in state.seen_urls:
            state.error_message = (
                "pagination_cursor_repeated"
                if _has_cursor_mark_query_parameter(request_url)
                else "pagination loop detected"
            )
            state.next_url_remaining = True
            return False
        state.seen_urls.add(request_identity)
        return True


def _has_cursor_mark_query_parameter(url: str) -> bool:
    return any(
        key == "cursorMark"
        for key, _value in urllib.parse.parse_qsl(
            urllib.parse.urlsplit(url).query,
            keep_blank_values=True,
        )
    )


def _pagination_url_identity(url: str) -> str:
    if not _has_cursor_mark_query_parameter(url):
        return url
    parsed = urllib.parse.urlsplit(url)
    normalized_query = urllib.parse.urlencode(
        sorted(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)),
        doseq=True,
    )
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path,
            normalized_query,
            "",
        )
    )


def _has_fhir_continuation_query(url: str) -> bool:
    query_names = {
        name.lower()
        for name, _value in urllib.parse.parse_qsl(
            urllib.parse.urlsplit(url).query,
            keep_blank_values=True,
        )
    }
    return bool(FHIR_CONTINUATION_QUERY_NAMES.intersection(query_names))


def _should_restart_expired_pagination_checkpoint(
    *,
    status_code: int | None,
    fetch_error: str | None,
    request_url: str,
    resume_state: PaginationResumeState | None,
    has_restart_attempted: bool,
) -> bool:
    return bool(
        resume_state
        and resume_state.resumed
        and not has_restart_attempted
        and fetch_error is None
        and status_code in {400, 404, 410}
        and _has_fhir_continuation_query(request_url)
    )


async def _mark_partition_fetch_error(
    state: PartitionFetchState,
    current_error: str,
) -> None:
    async with state.state_lock:
        state.partition_error_count += 1
        state.error_message = f"partition_errors_{state.partition_error_count}_last_{current_error}"


async def _mark_partition_page_fetched(state: PartitionFetchState) -> None:
    async with state.state_lock:
        state.page_count += 1


async def _consume_partition_entries(
    state: PartitionFetchState,
    source_record: dict[str, Any],
    resource_type: str,
    bundle_entries: list[dict[str, Any]],
    pending_resource_rows: list[dict[str, Any]],
    fetch_options: PartitionFetchOptions,
) -> None:
    for entry_record in bundle_entries:
        parsed_resource = parse_fhir_resource(
            source_record["source_id"],
            entry_record["resource"],
            resource_url=_clean_text(entry_record.get("fullUrl")),
            run_id=fetch_options.run_id,
            normalize_location_contacts=not (resource_type == "Location" and fetch_options.row_batch_handler),
        )
        if not parsed_resource:
            continue
        parsed_model, resource_row = parsed_resource
        if parsed_model is not state.result_model:
            continue
        async with state.state_lock:
            state.fetched_count += 1
            if fetch_options.retain_rows:
                state.retained_resource_rows.append(resource_row)
        if fetch_options.row_batch_handler:
            pending_resource_rows.append(resource_row)
            if len(pending_resource_rows) >= max(1, fetch_options.row_batch_size):
                await _flush_partition_resource_rows(
                    state,
                    fetch_options.row_batch_handler,
                    pending_resource_rows,
                )


def _enqueue_adaptive_partition_children(
    source_record: dict[str, Any],
    resource_type: str,
    start_url_queue: asyncio.Queue[str],
    state: PartitionFetchState,
    current_url: str,
    fhir_payload: Any,
) -> int:
    child_urls = _uhc_adaptive_partition_child_urls(
        source_record,
        resource_type,
        current_url,
        fhir_payload,
    )
    for child_url in child_urls:
        child_prefix = _uhc_role_postal_prefix(source_record, resource_type, child_url)
        if child_prefix not in state.completed_partition_prefixes:
            start_url_queue.put_nowait(child_url)
    return len(child_urls)


async def _fetch_partition_page(
    source_record: dict[str, Any],
    resource_type: str,
    start_url_queue: asyncio.Queue[str],
    state: PartitionFetchState,
    current_url: str,
    pending_resource_rows: list[dict[str, Any]],
    fetch_options: PartitionFetchOptions,
) -> PartitionPageFetchResult:
    status_code, fhir_payload, fetch_error, _elapsed = await _fetch_source_json(
        source_record,
        current_url,
        timeout=fetch_options.timeout,
    )
    if status_code != 200 or fetch_error:
        await _mark_partition_fetch_error(state, fetch_error or f"http_{status_code}")
        return PartitionPageFetchResult(next_url=None, outcome="failed")
    if not _is_bundle_payload(fhir_payload):
        await _mark_partition_fetch_error(state, "non_bundle_payload")
        return PartitionPageFetchResult(next_url=None, outcome="failed")
    await _mark_partition_page_fetched(state)
    if _enqueue_adaptive_partition_children(
        source_record,
        resource_type,
        start_url_queue,
        state,
        current_url,
        fhir_payload,
    ):
        return PartitionPageFetchResult(next_url=None, outcome="split")
    is_partition_failed_on_page = _is_uhc_partition_cap_exhausted(
        source_record,
        current_url,
        fhir_payload,
    )
    if is_partition_failed_on_page:
        await _mark_partition_fetch_error(
            state,
            f"uhc_{resource_type.lower()}_partition_cap_exhausted",
        )
    await _consume_partition_entries(
        state,
        source_record,
        resource_type,
        _bundle_entries(fhir_payload),
        pending_resource_rows,
        fetch_options,
    )
    next_url = _next_link(fhir_payload)
    return PartitionPageFetchResult(
        next_url=_resolved_fhir_next_url(source_record, current_url, next_url),
        outcome="failed" if is_partition_failed_on_page else "fetched",
    )


async def _fetch_partition_url_chain(
    source_record: dict[str, Any],
    resource_type: str,
    start_url_queue: asyncio.Queue[str],
    state: PartitionFetchState,
    request_url: str,
    fetch_options: PartitionFetchOptions,
) -> None:
    pending_resource_rows: list[dict[str, Any]] = []
    partition_prefix = _uhc_role_postal_prefix(source_record, resource_type, request_url)
    if partition_prefix in state.completed_partition_prefixes:
        return
    is_partition_complete = False
    is_partition_failed = False
    current_url: str | None = request_url
    while current_url and not state.stop_event.is_set():
        if fetch_options.deadline_at is not None and time.monotonic() >= fetch_options.deadline_at:
            state.deadline_reached = True
            state.next_url_remaining = True
            state.stop_event.set()
            break
        if not await _can_fetch_partition_url(state, current_url, max_pages=fetch_options.max_pages):
            break
        page_fetch_result = await _fetch_partition_page(
            source_record,
            resource_type,
            start_url_queue,
            state,
            current_url,
            pending_resource_rows,
            fetch_options,
        )
        current_url = page_fetch_result.next_url
        is_partition_failed = is_partition_failed or page_fetch_result.outcome == "failed"
        if page_fetch_result.outcome == "split":
            break
        if not current_url and not is_partition_failed:
            is_partition_complete = True
    await _flush_partition_resource_rows(state, fetch_options.row_batch_handler, pending_resource_rows)
    if is_partition_complete:
        await _record_partition_checkpoint(
            state,
            source_record,
            partition_prefix,
            fetch_options.run_id,
        )


async def _run_partition_fetch_worker(
    source_record: dict[str, Any],
    resource_type: str,
    start_url_queue: asyncio.Queue[str],
    state: PartitionFetchState,
    fetch_options: PartitionFetchOptions,
) -> None:
    while not state.stop_event.is_set():
        try:
            request_url = start_url_queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        try:
            await _fetch_partition_url_chain(
                source_record,
                resource_type,
                start_url_queue,
                state,
                request_url,
                fetch_options,
            )
        finally:
            start_url_queue.task_done()


async def _prepare_partition_fetch(
    source_record: dict[str, Any],
    resource_type: str,
    model: type,
    start_urls: list[str],
    run_id: str | None,
) -> tuple[asyncio.Queue[str], PartitionFetchState]:
    start_url_queue: asyncio.Queue[str] = asyncio.Queue()
    state = PartitionFetchState(result_model=model, retained_resource_rows=[])
    state.completed_partition_prefixes = await _load_partition_checkpoints(
        source_record,
        resource_type,
        run_id,
    )
    for request_url in start_urls:
        partition_prefix = _uhc_role_postal_prefix(source_record, resource_type, request_url)
        if partition_prefix not in state.completed_partition_prefixes:
            start_url_queue.put_nowait(request_url)
    return start_url_queue, state


async def _execute_partition_fetch_workers(
    source_record: dict[str, Any],
    resource_type: str,
    start_url_queue: asyncio.Queue[str],
    state: PartitionFetchState,
    fetch_options: PartitionFetchOptions,
    requested_worker_count: int,
) -> None:
    worker_count = min(_partition_fetch_concurrency(), max(1, requested_worker_count))
    worker_tasks = [
        asyncio.create_task(
            _run_partition_fetch_worker(
                source_record,
                resource_type,
                start_url_queue,
                state,
                fetch_options,
            )
        )
        for _worker_index in range(worker_count)
    ]
    try:
        await asyncio.gather(*worker_tasks)
    finally:
        for worker_task in worker_tasks:
            if not worker_task.done():
                worker_task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        await _flush_partition_checkpoints(state)


async def _fetch_partitioned_resource_rows(
    source_record: dict[str, Any],
    resource_type: str,
    model: type,
    start_urls: list[str],
    fetch_options: PartitionFetchOptions,
) -> ResourceFetchResult:
    """Fetch independent FHIR search partitions concurrently and merge counts."""
    start_url_queue, state = await _prepare_partition_fetch(
        source_record,
        resource_type,
        model,
        start_urls,
        fetch_options.run_id,
    )
    await _execute_partition_fetch_workers(
        source_record,
        resource_type,
        start_url_queue,
        state,
        fetch_options,
        len(start_urls),
    )
    is_partition_scan_complete = (
        not state.error_message
        and not state.hard_page_limit_reached
        and not state.deadline_reached
        and start_url_queue.empty()
    )
    residual_error = (
        _uhc_partition_residual_error(source_record, resource_type)
        if is_partition_scan_complete
        else None
    )
    if (
        is_partition_scan_complete
        and resource_type == "PractitionerRole"
        and _is_uhc_role_postal_partition_enabled(source_record)
    ):
        await _clear_reverse_lookup_checkpoints(
            source_record,
            seed_resource_type=_uhc_role_postal_checkpoint_type(source_record),
        )
    is_complete = is_partition_scan_complete and not residual_error
    return ResourceFetchResult(
        model=model,
        rows=state.retained_resource_rows,
        rows_fetched=state.fetched_count,
        rows_written=state.written_count,
        pages_fetched=state.page_count,
        complete=is_complete,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=state.hard_page_limit_reached,
        next_url_remaining=state.next_url_remaining or not start_url_queue.empty(),
        error=state.error_message or ("deadline_reached" if state.deadline_reached else None) or residual_error,
        fetch_mode="partitioned_paged",
        deadline_reached=state.deadline_reached,
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
    deadline_seconds: int = 0,
    pagination_checkpoint: PaginationCheckpointContext | None = None,
) -> ResourceFetchResult | None:
    model = RESOURCE_MODELS_BY_TYPE.get(resource_type)
    if model is None:
        return None
    resource_timeout = _source_resource_timeout(source, resource_type, timeout)
    if bulk_export:
        result = await _fetch_bulk_export_resource_rows(
            source,
            resource_type,
            per_resource_limit=per_resource_limit,
            timeout=resource_timeout,
            run_id=run_id,
            row_batch_handler=row_batch_handler,
            row_batch_size=row_batch_size,
            retain_rows=retain_rows,
        )
        if result is not None:
            return result
    if _scan_practitioner_role_requires_reverse_lookup(source, resource_type):
        return ResourceFetchResult(
            model=model,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=0,
            complete=False,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
            error=SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR,
            fetch_mode="source_specific_deferred",
        )
    start_urls = _resource_start_urls(source, resource_type, page_count=page_count)
    if not start_urls:
        return None
    checkpoint_context = (
        pagination_checkpoint
        if (
            pagination_checkpoint is not None
            and len(start_urls) == 1
            and row_batch_handler is not None
            and per_resource_limit <= 0
            and page_limit <= 0
        )
        else None
    )
    checkpoint_start_url = start_urls[0] if checkpoint_context else None
    resume_state = (
        await _load_or_initialize_pagination_checkpoint(
            checkpoint_context,
            resource_type,
            checkpoint_start_url,
        )
        if checkpoint_context and checkpoint_start_url
        else None
    )
    if resume_state and resume_state.complete:
        return ResourceFetchResult(
            model=model,
            rows=[],
            rows_fetched=resume_state.rows_processed,
            rows_written=0,
            pages_fetched=resume_state.pages_processed,
            complete=True,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
            fetch_mode="checkpoint_complete",
        )
    rows: list[dict[str, Any]] = []
    pending_rows: list[dict[str, Any]] = []
    rows_fetched = resume_state.rows_processed if resume_state else 0
    fetch_counts_by_name = {"rows_written": 0}
    pages = resume_state.pages_processed if resume_state else 0
    seen_urls: set[str] = set()
    recent_url_hashes = list(resume_state.recent_url_hashes if resume_state else ())
    persisted_url_hashes = set(recent_url_hashes)
    has_restart_attempted = False
    row_limit_reached = False
    page_limit_reached = False
    hard_page_limit_reached = False
    next_url_remaining = False
    error_message: str | None = None
    deadline_at = time.monotonic() + deadline_seconds if deadline_seconds > 0 else None
    is_deadline_reached = False
    max_pages = (
        0
        if checkpoint_context
        or resource_type == "PractitionerRole" and _is_uhc_role_postal_partition_enabled(source)
        else _env_int("HLTHPRT_PROVIDER_DIRECTORY_MAX_FULL_PAGES", DEFAULT_FULL_REFRESH_MAX_PAGES)
    )
    pending_start_urls = (
        [resume_state.next_url]
        if resume_state and resume_state.next_url
        else list(start_urls)
    )
    partitioned_fetch = len(pending_start_urls) > 1
    if (
        partitioned_fetch
        and _partition_fetch_concurrency() > 1
        and per_resource_limit <= 0
        and page_limit <= 0
    ):
        partition_fetch_options = PartitionFetchOptions(
            timeout=resource_timeout,
            run_id=run_id,
            row_batch_handler=row_batch_handler,
            row_batch_size=row_batch_size,
            retain_rows=retain_rows,
            deadline_at=deadline_at,
            max_pages=max_pages,
        )
        return await _fetch_partitioned_resource_rows(
            source,
            resource_type,
            model,
            pending_start_urls,
            partition_fetch_options,
        )
    partition_error_count = 0
    url: str | None = None

    async def flush_pending_rows() -> None:
        if not row_batch_handler or not pending_rows:
            return
        pending_rows_list = list(pending_rows)
        pending_rows.clear()
        fetch_counts_by_name["rows_written"] += await row_batch_handler(model, pending_rows_list)

    while pending_start_urls:
        start_url = pending_start_urls.pop(0)
        url = start_url
        while url and _limit_allows_more(rows_fetched, per_resource_limit):
            if cancel_ctx is not None:
                await raise_if_cancelled(cancel_ctx, cancel_task)
            if deadline_at is not None and time.monotonic() >= deadline_at:
                is_deadline_reached = True
                next_url_remaining = True
                break
            if page_limit > 0 and pages >= page_limit:
                page_limit_reached = True
                next_url_remaining = True
                break
            if max_pages > 0 and pages >= max_pages:
                hard_page_limit_reached = True
                next_url_remaining = True
                break
            request_identity = _pagination_url_identity(url)
            request_url_hash = _pagination_url_hash(url)
            if request_identity in seen_urls or request_url_hash in persisted_url_hashes:
                error_message = (
                    "pagination_cursor_repeated"
                    if _has_cursor_mark_query_parameter(url)
                    else "pagination loop detected"
                )
                next_url_remaining = True
                break
            seen_urls.add(request_identity)
            status_code, payload, error, _elapsed = await _fetch_source_json(
                source,
                url,
                timeout=resource_timeout,
            )
            if status_code != 200 or error:
                if (
                    checkpoint_context
                    and checkpoint_start_url
                    and _should_restart_expired_pagination_checkpoint(
                        status_code=status_code,
                        fetch_error=error,
                        request_url=url,
                        resume_state=resume_state,
                        has_restart_attempted=has_restart_attempted,
                    )
                ):
                    await _save_pagination_checkpoint(
                        checkpoint_context,
                        resource_type,
                        next_url=checkpoint_start_url,
                        pages_processed=0,
                        rows_processed=0,
                        recent_url_hashes=[],
                    )
                    rows_fetched = 0
                    pages = 0
                    seen_urls.clear()
                    recent_url_hashes.clear()
                    persisted_url_hashes.clear()
                    has_restart_attempted = True
                    url = checkpoint_start_url
                    continue
                current_error = error or f"http_{status_code}"
                if partitioned_fetch:
                    partition_error_count += 1
                    error_message = f"partition_errors_{partition_error_count}_last_{current_error}"
                    url = None
                    break
                error_message = current_error
                break
            if not _is_bundle_payload(payload):
                if partitioned_fetch:
                    partition_error_count += 1
                    error_message = f"partition_errors_{partition_error_count}_last_non_bundle_payload"
                    url = None
                    break
                error_message = "non_bundle_payload"
                break
            pages += 1
            child_urls = _uhc_adaptive_partition_child_urls(source, resource_type, url, payload)
            if child_urls:
                partitioned_fetch = True
                pending_start_urls = child_urls + pending_start_urls
                url = None
                break
            if _is_uhc_partition_cap_exhausted(source, url, payload):
                error_message = f"uhc_{resource_type.lower()}_partition_cap_exhausted"
            entries = _bundle_entries(payload)
            for entry_index, entry in enumerate(entries):
                parsed = parse_fhir_resource(
                    source["source_id"],
                    entry["resource"],
                    resource_url=_clean_text(entry.get("fullUrl")),
                    run_id=run_id,
                    normalize_location_contacts=not (resource_type == "Location" and row_batch_handler),
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
                if (
                    not checkpoint_context
                    and deadline_at is not None
                    and time.monotonic() >= deadline_at
                ):
                    is_deadline_reached = True
                    next_url_remaining = True
                    break
            next_url = _next_link(payload)
            resolved_next_url = _resolved_fhir_next_url(source, url, next_url)
            if (
                checkpoint_context
                and not entries
                and resolved_next_url
                and checkpoint_context.canonical_api_base
                == MOLINA_PROVIDER_DIRECTORY_BASE
                and _pagination_url_identity(resolved_next_url) == request_identity
            ):
                resolved_next_url = None
            if checkpoint_context:
                await flush_pending_rows()
                recent_url_hashes.append(request_url_hash)
                recent_url_hashes = recent_url_hashes[
                    -PAGINATION_CHECKPOINT_RECENT_URL_LIMIT:
                ]
                await _save_pagination_checkpoint(
                    checkpoint_context,
                    resource_type,
                    next_url=resolved_next_url,
                    pages_processed=pages,
                    rows_processed=rows_fetched,
                    recent_url_hashes=recent_url_hashes,
                )
            url = resolved_next_url
            if not _limit_allows_more(rows_fetched, per_resource_limit) and url:
                row_limit_reached = True
                next_url_remaining = True
                break
        if (
            row_limit_reached
            or page_limit_reached
            or hard_page_limit_reached
            or is_deadline_reached
            or (error_message and not partitioned_fetch)
            or not _limit_allows_more(rows_fetched, per_resource_limit)
        ):
            break
    await flush_pending_rows()
    if (
        not error_message
        and not row_limit_reached
        and not page_limit_reached
        and not hard_page_limit_reached
        and not is_deadline_reached
        and not url
        and not pending_start_urls
    ):
        error_message = _uhc_partition_residual_error(source, resource_type)
    complete = (
        not error_message
        and not row_limit_reached
        and not page_limit_reached
        and not hard_page_limit_reached
        and not is_deadline_reached
        and not url
        and not pending_start_urls
    )
    return ResourceFetchResult(
        model=model,
        rows=rows,
        rows_fetched=rows_fetched,
        rows_written=fetch_counts_by_name["rows_written"],
        pages_fetched=pages,
        complete=complete,
        row_limit_reached=row_limit_reached,
        page_limit_reached=page_limit_reached,
        hard_page_limit_reached=hard_page_limit_reached,
        next_url_remaining=next_url_remaining or bool(url) or bool(pending_start_urls),
        error=error_message or ("deadline_reached" if is_deadline_reached else None),
        fetch_mode="checkpointed_paged" if checkpoint_context else "paged",
        deadline_reached=is_deadline_reached,
    )


async def _fetch_scan_practitioner_role_rows(
    source: dict[str, Any],
    rows_by_resource: dict[str, list[dict[str, Any]]],
    options: ScanPractitionerRoleFetchOptions,
) -> ResourceFetchResult:
    if options.source is None:
        options = replace(options, source=source)
    await _mark_checkpointed_reverse_lookup_roles_seen(options)
    reverse_lookup_concurrency = _scan_practitioner_role_reverse_lookup_concurrency()
    if reverse_lookup_concurrency > 1 and options.per_resource_limit <= 0 and options.page_limit <= 0:
        return await _fetch_scan_practitioner_role_rows_concurrent(
            source,
            rows_by_resource,
            options,
            concurrency=reverse_lookup_concurrency,
        )
    model = ProviderDirectoryPractitionerRole
    rows: list[dict[str, Any]] = []
    seen_role_ids: set[str] | None = set() if options.retain_rows else None
    seed_count = 0
    rows_fetched = 0
    pages = 0
    row_limit_reached = False
    page_limit_reached = False
    hard_page_limit_reached = False
    next_url_remaining = False
    error_message: str | None = None
    reverse_error_count = 0
    stream_buffer = _StreamedResourceRowBuffer(
        model=model,
        row_batch_handler=options.row_batch_handler,
        row_batch_size=options.row_batch_size,
        pending_row_items=[],
    )
    resource_timeout = _source_resource_timeout(
        options.source or source,
        "PractitionerRole",
        options.timeout,
    )
    is_deadline_reached = False
    deadline_at = (
        time.monotonic() + options.deadline_seconds
        if options.deadline_seconds > 0
        else None
    )
    max_pages = _role_lookup_page_limit()

    async for search_param, seed_resource_type, seed_resource_id in _iter_scan_practitioner_role_seed_rows(
        rows_by_resource,
        options,
    ):
        seed_count += 1
        if options.cancel_ctx is not None:
            await raise_if_cancelled(options.cancel_ctx, options.cancel_task)
        if not _limit_allows_more(rows_fetched, options.per_resource_limit):
            break
        if deadline_at is not None and time.monotonic() >= deadline_at:
            is_deadline_reached = True
            next_url_remaining = True
            break
        url = _scan_practitioner_role_reverse_lookup_url(
            source,
            search_param,
            seed_resource_type,
            seed_resource_id,
            page_count=options.page_count,
        )
        while url and _limit_allows_more(rows_fetched, options.per_resource_limit):
            if options.cancel_ctx is not None:
                await raise_if_cancelled(options.cancel_ctx, options.cancel_task)
            if deadline_at is not None and time.monotonic() >= deadline_at:
                is_deadline_reached = True
                next_url_remaining = True
                break
            if options.page_limit > 0 and pages >= options.page_limit:
                page_limit_reached = True
                next_url_remaining = True
                break
            if max_pages > 0 and pages >= max_pages:
                hard_page_limit_reached = True
                next_url_remaining = True
                break
            status_code, payload, error, _elapsed = await _fetch_source_json(
                source,
                url,
                timeout=resource_timeout,
            )
            if status_code != 200 or error:
                current_error = error or f"http_{status_code}"
                reverse_error_count += 1
                error_message = f"reverse_lookup_errors_{reverse_error_count}_last_{current_error}"
                break
            if not payload or payload.get("resourceType") != "Bundle":
                reverse_error_count += 1
                error_message = f"reverse_lookup_errors_{reverse_error_count}_last_non_bundle_payload"
                break
            pages += 1
            entries = _bundle_entries(payload)
            for entry_index, entry in enumerate(entries):
                parsed = parse_fhir_resource(
                    source["source_id"],
                    entry["resource"],
                    resource_url=_clean_text(entry.get("fullUrl")),
                    run_id=options.run_id,
                )
                if not parsed:
                    continue
                parsed_model, row = parsed
                if parsed_model is not model:
                    continue
                role_id = _clean_text(row.get("resource_id"))
                if not role_id:
                    continue
                if seen_role_ids is not None and role_id in seen_role_ids:
                    continue
                if seen_role_ids is not None:
                    seen_role_ids.add(role_id)
                if options.retain_rows:
                    rows.append(row)
                await stream_buffer.add(row)
                rows_fetched += 1
                if not _limit_allows_more(rows_fetched, options.per_resource_limit):
                    row_limit_reached = entry_index < len(entries) - 1
                    break
            next_url = _next_link(payload)
            url = _resolved_fhir_next_url(source, url, next_url)
            if not _limit_allows_more(rows_fetched, options.per_resource_limit) and url:
                row_limit_reached = True
                next_url_remaining = True
                break
        if row_limit_reached or page_limit_reached or hard_page_limit_reached:
            break
        if is_deadline_reached:
            break

    await stream_buffer.flush()
    if seed_count == 0:
        return ResourceFetchResult(
            model=model,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=0,
            complete=False,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
            error=SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR,
            fetch_mode="source_specific_deferred",
        )
    complete = (
        not error_message
        and not row_limit_reached
        and not page_limit_reached
        and not hard_page_limit_reached
        and not is_deadline_reached
        and not next_url_remaining
    )
    reverse_lookup_result = ResourceFetchResult(
        model=model,
        rows=rows,
        rows_fetched=rows_fetched,
        rows_written=stream_buffer.rows_written,
        pages_fetched=pages,
        complete=complete,
        row_limit_reached=row_limit_reached,
        page_limit_reached=page_limit_reached,
        hard_page_limit_reached=hard_page_limit_reached,
        next_url_remaining=next_url_remaining,
        error=error_message or ("deadline_reached" if is_deadline_reached else None),
        fetch_mode="source_specific_reverse_lookup",
    )
    if reverse_lookup_result.complete and options.resume_completed_seeds:
        await _clear_reverse_lookup_checkpoints(options.source or source)
    return reverse_lookup_result


def _scan_practitioner_role_reverse_lookup_concurrency() -> int:
    return max(1, _env_int("HLTHPRT_PROVIDER_DIRECTORY_ROLE_LOOKUP_CONCURRENCY", 16))


def _role_lookup_page_limit() -> int:
    return max(0, _env_int("HLTHPRT_PROVIDER_DIRECTORY_MAX_REVERSE_LOOKUP_PAGES", 0))


def _is_lookup_deadline_elapsed(deadline_at: float | None) -> bool:
    return deadline_at is not None and time.monotonic() >= deadline_at


def _parse_practitioner_role_reverse_lookup_rows(
    source_id: str,
    payload: dict[str, Any],
    run_id: str | None,
) -> list[dict[str, Any]]:
    """Parse PractitionerRole rows from a reverse lookup Bundle payload."""
    resource_rows: list[dict[str, Any]] = []
    for entry in _bundle_entries(payload):
        parsed = parse_fhir_resource(
            source_id,
            entry["resource"],
            resource_url=_clean_text(entry.get("fullUrl")),
            run_id=run_id,
        )
        if not parsed:
            continue
        parsed_model, row = parsed
        if parsed_model is ProviderDirectoryPractitionerRole:
            resource_rows.append(row)
    return resource_rows


def _new_role_lookup_state(
    result_model: type,
    retained_resource_rows: list[dict[str, Any]],
    streamed_rows: _StreamedResourceRowBuffer,
    options: ScanPractitionerRoleFetchOptions,
) -> _PractitionerRoleReverseLookupState:
    """Create shared state for concurrent PractitionerRole reverse lookup."""
    return _PractitionerRoleReverseLookupState(
        result_model=result_model,
        retained_resource_rows=retained_resource_rows,
        streamed_rows=streamed_rows,
        retain_rows=options.retain_rows,
        page_limit=_role_lookup_page_limit(),
        seen_role_ids=set() if options.retain_rows else None,
    )


def _new_role_lookup_request(
    source_config: dict[str, Any],
    resource_rows_by_type: dict[str, list[dict[str, Any]]],
    options: ScanPractitionerRoleFetchOptions,
) -> _PractitionerRoleReverseLookupRequest:
    """Create immutable controls for concurrent PractitionerRole reverse lookup."""
    deadline_at = (
        time.monotonic() + options.deadline_seconds
        if options.deadline_seconds > 0
        else None
    )
    return _PractitionerRoleReverseLookupRequest(
        source=source_config,
        rows_by_resource=resource_rows_by_type,
        options=options,
        resource_timeout=_source_resource_timeout(
            options.source or source_config,
            "PractitionerRole",
            options.timeout,
        ),
        deadline_at=deadline_at,
    )


async def _run_role_lookup_tasks(
    fetch_state: _PractitionerRoleReverseLookupState,
    request: _PractitionerRoleReverseLookupRequest,
    concurrency: int,
) -> None:
    """Run the concurrent reverse lookup producer and workers."""
    seed_queue: asyncio.Queue[tuple[str, str, str] | None] = asyncio.Queue(
        maxsize=max(concurrency * 4, 1)
    )
    lookup_tasks = [
        asyncio.create_task(fetch_state.fetch_seed_rows(request, seed_queue))
        for _unused_worker_index in range(concurrency)
    ]
    producer_task = asyncio.create_task(
        fetch_state.produce_seed_rows(request, seed_queue, concurrency)
    )
    try:
        await asyncio.gather(producer_task, *lookup_tasks)
    finally:
        for lookup_task in lookup_tasks:
            if not lookup_task.done():
                lookup_task.cancel()
        if not producer_task.done():
            producer_task.cancel()
        await asyncio.gather(producer_task, *lookup_tasks, return_exceptions=True)
        await fetch_state.flush_completed_seed_rows()


def _build_role_lookup_result(
    result_model: type,
    retained_resource_rows: list[dict[str, Any]],
    streamed_rows: _StreamedResourceRowBuffer,
    fetch_state: _PractitionerRoleReverseLookupState,
) -> ResourceFetchResult:
    """Build the ResourceFetchResult for concurrent PractitionerRole lookup."""
    if fetch_state.seed_count == 0:
        return ResourceFetchResult(
            model=result_model,
            rows=[],
            rows_fetched=0,
            rows_written=0,
            pages_fetched=0,
            complete=False,
            row_limit_reached=False,
            page_limit_reached=False,
            hard_page_limit_reached=False,
            next_url_remaining=False,
            error=SCAN_PRACTITIONER_ROLE_REVERSE_LOOKUP_ERROR,
            fetch_mode="source_specific_deferred",
        )
    is_complete = (
        not fetch_state.error_message
        and not fetch_state.is_hard_page_limit_reached
        and not fetch_state.is_deadline_reached
        and not fetch_state.has_next_url_remaining
    )
    return ResourceFetchResult(
        model=result_model,
        rows=retained_resource_rows,
        rows_fetched=fetch_state.fetched_count,
        rows_written=streamed_rows.rows_written,
        pages_fetched=fetch_state.page_count,
        complete=is_complete,
        row_limit_reached=False,
        page_limit_reached=False,
        hard_page_limit_reached=fetch_state.is_hard_page_limit_reached,
        next_url_remaining=fetch_state.has_next_url_remaining,
        error=fetch_state.error_message
        or ("deadline_reached" if fetch_state.is_deadline_reached else None),
        deadline_reached=fetch_state.is_deadline_reached,
        fetch_mode="source_specific_reverse_lookup_concurrent",
    )


async def _fetch_scan_practitioner_role_rows_concurrent(
    source_config: dict[str, Any],
    resource_rows_by_type: dict[str, list[dict[str, Any]]],
    options: ScanPractitionerRoleFetchOptions,
    *,
    concurrency: int,
) -> ResourceFetchResult:
    """Fetch reverse PractitionerRole lookups concurrently for large payer scans."""
    result_model = ProviderDirectoryPractitionerRole
    retained_resource_rows: list[dict[str, Any]] = []
    streamed_rows = _StreamedResourceRowBuffer(
        model=result_model,
        row_batch_handler=options.row_batch_handler,
        row_batch_size=options.row_batch_size,
        pending_row_items=[],
    )
    fetch_state = _new_role_lookup_state(
        result_model,
        retained_resource_rows,
        streamed_rows,
        options,
    )
    request = _new_role_lookup_request(source_config, resource_rows_by_type, options)
    await _run_role_lookup_tasks(fetch_state, request, concurrency)
    await streamed_rows.flush()
    reverse_lookup_result = _build_role_lookup_result(
        result_model,
        retained_resource_rows,
        streamed_rows,
        fetch_state,
    )
    if reverse_lookup_result.complete and options.resume_completed_seeds:
        await _clear_reverse_lookup_checkpoints(options.source or source_config)
    return reverse_lookup_result


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
    source_ids: list[str] | None = None,
    track_seen: bool = False,
    seen_table: str | None = None,
    progress_callback: Callable[[str, int], Awaitable[None]] | None = None,
    deadline_seconds: int = 0,
    flush_rows: int | None = None,
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

    counts: dict[str, int] = {}
    edge_source_ids = source_ids or [source["source_id"]]
    canonical_api_base = source.get("canonical_api_base") or source.get("api_base")
    by_model: dict[type, list[dict[str, Any]]] = {}
    linked_counts_by_name = {"pending_rows": 0}
    flush_threshold = max(int(flush_rows or _linked_resource_flush_rows()), 1)

    async def flush_pending_rows() -> None:
        if not by_model:
            return
        pending = by_model.copy()
        by_model.clear()
        linked_counts_by_name["pending_rows"] = 0
        for model, rows in pending.items():
            if not rows:
                continue
            rows = _copy_rows_for_source_ids(rows, edge_source_ids)
            imported = await _upsert_resource_rows(
                model,
                rows,
                run_id=run_id,
                track_seen=track_seen,
                seen_table=seen_table,
                canonical_api_base=canonical_api_base,
                source_ids=edge_source_ids,
            )
            if imported:
                resource_name = model.__name__.removeprefix("ProviderDirectory")
                counts[resource_name] = counts.get(resource_name, 0) + imported
                if progress_callback is not None:
                    await progress_callback(resource_name, imported)

    tasks = [asyncio.create_task(_fetch_one(ref)) for ref in refs_to_fetch]
    deadline_at = time.monotonic() + deadline_seconds if deadline_seconds > 0 else None
    try:
        for future in asyncio.as_completed(tasks):
            try:
                if deadline_at is None:
                    result = await future
                else:
                    remaining = deadline_at - time.monotonic()
                    if remaining <= 0:
                        break
                    result = await asyncio.wait_for(future, timeout=remaining)
            except TimeoutError:
                break
            except Exception:  # pragma: no cover - defensive per-reference isolation
                continue
            if not result:
                continue
            fetched_resource_type, model, row = result
            by_model.setdefault(model, []).append(row)
            linked_counts_by_name["pending_rows"] += 1
            existing.add((fetched_resource_type, row.get("resource_id")))
            if linked_counts_by_name["pending_rows"] >= flush_threshold:
                await flush_pending_rows()
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    await flush_pending_rows()
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


def _is_resource_fetch_complete_for_publish(result: ResourceFetchResult) -> bool:
    return result.complete and not result.error and not result.bounded and not result.next_url_remaining


def _record_resource_completion(
    completion: dict[str, set[str]] | None,
    resource_type: str,
    source_ids: list[str],
    result: ResourceFetchResult,
) -> None:
    if completion is None or not _is_resource_fetch_complete_for_publish(result):
        return
    completion.setdefault(resource_type, set()).update(source_ids)


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
        "deadline_reached": result.deadline_reached,
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
    if model is ProviderDirectoryLocation and _location_contact_fields_missing(rows):
        rows = _attach_location_contact_fields(rows)
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
    resource_type = RESOURCE_TYPES_BY_MODEL.get(model)
    if use_seen_table or seen_table:
        if resource_type:
            seen_ref = _qt(_schema(), seen_table or PROVIDER_DIRECTORY_IMPORT_SEEN_TABLE)
            run_filter = "AND seen.run_id = CAST(:run_id AS varchar)" if not seen_table else ""
            await db.status(
                f"""
                DELETE FROM {_qt(_schema(), ProviderDirectorySourceResource.__tablename__)} AS edge
                 WHERE edge.source_id = :source_id
                   AND edge.resource_type = :resource_type
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {seen_ref} AS seen
                         WHERE seen.resource_type = :resource_type
                           {run_filter}
                           AND seen.source_id = edge.source_id
                           AND seen.resource_id = edge.resource_id
                   );
                """,
                source_id=source_id,
                run_id=run_id,
                resource_type=resource_type,
            )
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
    if resource_type:
        await db.status(
            f"""
            DELETE FROM {_qt(_schema(), ProviderDirectorySourceResource.__tablename__)}
             WHERE source_id = :source_id
               AND resource_type = :resource_type
               AND last_seen_run_id IS DISTINCT FROM :run_id;
            """,
            source_id=source_id,
            run_id=run_id,
            resource_type=resource_type,
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


SOURCE_CATALOG_STALE_TABLE_MODELS = (
    *RESOURCE_MODELS,
    ProviderDirectoryCapability,
    ProviderDirectorySourceResource,
)


def _source_catalog_stale_cleanup_enabled(
    *,
    stale_cleanup: bool,
    full_refresh: bool,
    source_query: str | None,
    limit: int | None,
    requested_source_ids: list[str] | tuple[str, ...] | None = None,
    retest_results_configured: bool = True,
) -> bool:
    return bool(
        stale_cleanup
        and full_refresh
        and not source_query
        and not limit
        and not requested_source_ids
        and retest_results_configured
    )


async def _delete_stale_provider_directory_source_catalog(
    current_source_ids: list[str],
) -> dict[str, int]:
    source_ids = sorted({_clean_text(source_id) for source_id in current_source_ids if _clean_text(source_id)})
    if not source_ids:
        return {}
    params = {"source_ids": source_ids}
    deleted: dict[str, int] = {}
    predicate = "NOT (source_id = ANY(CAST(:source_ids AS varchar[])))"
    for model in SOURCE_CATALOG_STALE_TABLE_MODELS:
        count = int(
            await db.status(
                f"""
                DELETE FROM {_qt(_schema(), model.__tablename__)}
                 WHERE {predicate};
                """,
                **params,
            )
            or 0
        )
        if count:
            deleted[model.__tablename__] = count
    source_count = int(
        await db.status(
            f"""
            DELETE FROM {_qt(_schema(), ProviderDirectorySource.__tablename__)}
             WHERE {predicate};
            """,
            **params,
        )
        or 0
    )
    if source_count:
        deleted[ProviderDirectorySource.__tablename__] = source_count
    return deleted


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


def _publish_corroboration_enabled(value: Any) -> bool:
    return _bool_or_default(
        value,
        _bool_or_default(os.getenv(PUBLISH_CORROBORATION_ENV), False),
    )


def _provider_directory_publish_artifact_targets(raw_targets: Any) -> set[str] | None:
    if raw_targets in (None, ""):
        return None
    if isinstance(raw_targets, str):
        target_values = raw_targets.split(",")
    elif isinstance(raw_targets, (bytes, bytearray, dict)) or not hasattr(raw_targets, "__iter__"):
        target_values = (raw_targets,)
    else:
        target_values = raw_targets

    selected_targets: set[str] = set()
    for target_candidate in target_values:
        target_text = _clean_text(target_candidate)
        if not target_text:
            continue
        normalized = (
            target_text.strip()
            .lower()
            .replace("-", "_")
            .replace(" ", "_")
        )
        expanded = PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGET_ALIASES.get(normalized)
        if expanded is not None:
            selected_targets.update(expanded)
            continue
        if normalized not in PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS:
            raise ValueError(
                "Unsupported Provider Directory publish_artifacts_targets value "
                f"{target_text!r}; expected one of "
                f"{', '.join(PROVIDER_DIRECTORY_PUBLISH_ARTIFACT_TARGETS)}"
            )
        selected_targets.add(normalized)
    return selected_targets or None


def is_provider_directory_publish_target_enabled(targets: set[str] | None, target: str) -> bool:
    """Return whether a publish artifact target should be emitted for this task."""
    return targets is None or target in targets


def _provider_directory_publish_target_skipped() -> dict[str, Any]:
    return {"skipped": True, "reason": "target_not_requested"}


def _apply_provider_directory_refresh_preset(task: dict[str, Any]) -> dict[str, Any]:
    preset = _clean_text(task.get("refresh_preset") or task.get("preset"))
    if not preset:
        return task
    normalized_preset = preset.strip().lower().replace("_", "-")
    if normalized_preset not in PROVIDER_DIRECTORY_REFRESH_PRESETS:
        raise ValueError(
            "Unsupported Provider Directory refresh_preset "
            f"{preset!r}; expected one of {', '.join(PROVIDER_DIRECTORY_REFRESH_PRESETS)}"
        )
    defaults = PROVIDER_DIRECTORY_MONTHLY_FULL_DEFAULTS
    normalized_task = dict(task)
    normalized_task["refresh_preset"] = normalized_preset
    for key, value in defaults.items():
        if normalized_task.get(key) in (None, ""):
            normalized_task[key] = value
    return normalized_task


def _int_or_default(value: Any, default: int) -> int:
    if value is None or value == "":
        return default
    return int(value)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return int(value)


def _linked_resource_flush_rows() -> int:
    return _env_int("HLTHPRT_PROVIDER_DIRECTORY_LINKED_RESOURCE_FLUSH_ROWS", 1000)


def _partition_fetch_concurrency() -> int:
    return max(1, _env_int("HLTHPRT_PROVIDER_DIRECTORY_PARTITION_CONCURRENCY", 16))


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
    except Exception as exc:
        print(f"Provider Directory progress update failed: {type(exc).__name__}: {exc}")


def _limit_allows_more(current: int, limit: int) -> bool:
    return limit <= 0 or current < limit


def _resource_endpoint_signature(source: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(
        (field, _clean_text(source.get(field)) or "")
        for field in sorted(RESOURCE_ENDPOINT_FIELDS.values())
    )


def _resource_import_group_key(source: dict[str, Any]) -> tuple[str, str, tuple[tuple[str, str], ...]]:
    api_base = _canonical_base(source.get("canonical_api_base") or source.get("api_base"))
    if not api_base:
        return source.get("source_id") or "", "", ()
    credential_descriptor = _credential_request_options_for_source(source, f"{api_base}/metadata")["descriptor"]
    return (
        api_base,
        json.dumps(credential_descriptor or {}, sort_keys=True, default=str),
        _resource_endpoint_signature(source),
    )


def _group_resource_import_sources(
    sources: list[dict[str, Any]],
    *,
    linked_resource_limit: int,
) -> list[list[dict[str, Any]]]:
    groups: dict[tuple[str, str, tuple[tuple[str, str], ...]], list[dict[str, Any]]] = {}
    ordered_groups: list[list[dict[str, Any]]] = []
    for source in sources:
        key = _resource_import_group_key(source)
        if key not in groups:
            groups[key] = []
            ordered_groups.append(groups[key])
        groups[key].append(source)
    return ordered_groups


def _scoped_partition_source_record(
    source_group: list[dict[str, Any]],
    source_ids: list[str],
) -> dict[str, Any]:
    source_by_name = dict(source_group[0])
    source_by_name["_partition_checkpoint_source_ids"] = tuple(source_ids)
    return source_by_name


def _is_pagination_checkpoint_mode_enabled(
    *,
    run_id: str | None,
    full_refresh: bool,
    resource_limit: int,
    page_limit: int,
    stream_batch_size: int,
    stale_cleanup: bool,
    publish_artifacts: bool,
) -> bool:
    return bool(
        run_id
        and full_refresh
        and resource_limit <= 0
        and page_limit <= 0
        and stream_batch_size > 0
        and not stale_cleanup
        and not publish_artifacts
    )


def _pagination_checkpoint_context(
    source: dict[str, Any],
    source_ids: list[str],
    *,
    run_id: str | None,
    retry_of_run_id: str | None,
) -> PaginationCheckpointContext | None:
    canonical_api_base = _canonical_base(
        source.get("canonical_api_base") or source.get("api_base")
    )
    if not run_id or not source_ids or canonical_api_base not in PAGINATION_CHECKPOINT_API_BASES:
        return None
    scope_payload = json.dumps(
        {
            "source_ids": sorted(set(source_ids)),
            "resource_group": _resource_import_group_key(source),
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return PaginationCheckpointContext(
        canonical_api_base=canonical_api_base,
        source_scope_hash=hashlib.sha256(scope_payload.encode("utf-8")).hexdigest(),
        source_ids=tuple(sorted(set(source_ids))),
        owner_run_id=run_id,
        retry_of_run_id=retry_of_run_id,
    )


def _pagination_url_hash(url: str) -> str:
    identity = _pagination_url_identity(url)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _json_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := _clean_text(item))]


def _pagination_checkpoint_row_mapping(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if hasattr(row, "_mapping"):
        return dict(row._mapping)
    return dict(row)


def _pagination_checkpoint_table_ref() -> str:
    return _qt(_schema(), ProviderDirectoryPaginationCheckpoint.__tablename__)


async def _fetch_pagination_checkpoint(
    context: PaginationCheckpointContext,
    resource_type: str,
) -> dict[str, Any]:
    checkpoint_row = await db.first(
        f"""
        SELECT source_ids, owner_run_id, start_url_hash, next_url, state,
               pages_processed, rows_processed, recent_cursor_hashes
          FROM {_pagination_checkpoint_table_ref()}
         WHERE canonical_api_base = :canonical_api_base
           AND resource_type = :resource_type
           AND source_scope_hash = :source_scope_hash;
        """,
        canonical_api_base=context.canonical_api_base,
        resource_type=resource_type,
        source_scope_hash=context.source_scope_hash,
    )
    return _pagination_checkpoint_row_mapping(checkpoint_row)


def _compatible_pagination_resume_state(
    checkpoint: dict[str, Any],
    context: PaginationCheckpointContext,
    start_url_hash: str,
) -> PaginationResumeState | None:
    existing_source_ids = tuple(sorted(_json_text_list(checkpoint.get("source_ids"))))
    existing_owner_run_id = _clean_text(checkpoint.get("owner_run_id"))
    has_matching_owner = existing_owner_run_id == context.owner_run_id or (
        context.retry_of_run_id is not None
        and existing_owner_run_id == context.retry_of_run_id
    )
    if (
        not checkpoint
        or existing_source_ids != context.source_ids
        or _clean_text(checkpoint.get("start_url_hash")) != start_url_hash
        or not has_matching_owner
    ):
        return None
    is_complete = checkpoint.get("state") == PAGINATION_CHECKPOINT_COMPLETE
    next_url = _clean_text(checkpoint.get("next_url"))
    if not is_complete and not next_url:
        return None
    return PaginationResumeState(
        next_url=next_url,
        pages_processed=int(checkpoint.get("pages_processed") or 0),
        rows_processed=int(checkpoint.get("rows_processed") or 0),
        recent_url_hashes=tuple(
            _json_text_list(checkpoint.get("recent_cursor_hashes"))
        ),
        complete=is_complete,
        resumed=True,
    )


async def _adopt_pagination_checkpoint_owner(
    context: PaginationCheckpointContext,
    resource_type: str,
    previous_owner_run_id: str,
) -> None:
    updated = await db.status(
        f"""
        UPDATE {_pagination_checkpoint_table_ref()}
           SET owner_run_id = :owner_run_id,
               retry_of_run_id = :previous_owner_run_id,
               updated_at = now()
         WHERE canonical_api_base = :canonical_api_base
           AND resource_type = :resource_type
           AND source_scope_hash = :source_scope_hash
           AND owner_run_id = :previous_owner_run_id;
        """,
        owner_run_id=context.owner_run_id,
        previous_owner_run_id=previous_owner_run_id,
        canonical_api_base=context.canonical_api_base,
        resource_type=resource_type,
        source_scope_hash=context.source_scope_hash,
    )
    if _coerce_rowcount(updated) <= 0:
        raise RuntimeError("provider_directory_pagination_checkpoint_ownership_lost")


def _pagination_checkpoint_reset_sql() -> str:
    return f"""
    INSERT INTO {_pagination_checkpoint_table_ref()} (
        canonical_api_base, resource_type, source_scope_hash, dataset_id,
        source_ids, owner_run_id, retry_of_run_id, start_url_hash,
        next_url, state, pages_processed, rows_processed,
        recent_cursor_hashes, created_at, updated_at, completed_at
    ) VALUES (
        :canonical_api_base, :resource_type, :source_scope_hash, NULL,
        CAST(:source_ids AS jsonb), :owner_run_id, :retry_of_run_id,
        :start_url_hash, :next_url, :state, 0, 0,
        '[]'::jsonb, now(), now(), NULL
    )
    ON CONFLICT (canonical_api_base, resource_type, source_scope_hash)
    DO UPDATE SET
        dataset_id = NULL,
        source_ids = EXCLUDED.source_ids,
        owner_run_id = EXCLUDED.owner_run_id,
        retry_of_run_id = EXCLUDED.retry_of_run_id,
        start_url_hash = EXCLUDED.start_url_hash,
        next_url = EXCLUDED.next_url,
        state = EXCLUDED.state,
        pages_processed = 0,
        rows_processed = 0,
        recent_cursor_hashes = '[]'::jsonb,
        created_at = now(),
        updated_at = now(),
        completed_at = NULL;
    """


async def _reset_pagination_checkpoint(
    context: PaginationCheckpointContext,
    resource_type: str,
    start_url: str,
    start_url_hash: str,
) -> None:
    await db.status(
        _pagination_checkpoint_reset_sql(),
        canonical_api_base=context.canonical_api_base,
        resource_type=resource_type,
        source_scope_hash=context.source_scope_hash,
        source_ids=json.dumps(context.source_ids),
        owner_run_id=context.owner_run_id,
        retry_of_run_id=context.retry_of_run_id,
        start_url_hash=start_url_hash,
        next_url=start_url,
        state=PAGINATION_CHECKPOINT_ACTIVE,
    )


async def _load_or_initialize_pagination_checkpoint(
    context: PaginationCheckpointContext,
    resource_type: str,
    start_url: str,
) -> PaginationResumeState:
    """Load only same-run or direct-retry state; otherwise reset the scan."""
    start_url_hash = hashlib.sha256(start_url.encode("utf-8")).hexdigest()
    checkpoint = await _fetch_pagination_checkpoint(context, resource_type)
    resume_state = _compatible_pagination_resume_state(
        checkpoint,
        context,
        start_url_hash,
    )
    if resume_state:
        previous_owner_run_id = _clean_text(checkpoint.get("owner_run_id"))
        if previous_owner_run_id and previous_owner_run_id != context.owner_run_id:
            await _adopt_pagination_checkpoint_owner(
                context,
                resource_type,
                previous_owner_run_id,
            )
        return resume_state
    await _reset_pagination_checkpoint(
        context,
        resource_type,
        start_url,
        start_url_hash,
    )
    return PaginationResumeState(
        next_url=start_url,
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=(),
    )


async def _save_pagination_checkpoint(
    context: PaginationCheckpointContext,
    resource_type: str,
    *,
    next_url: str | None,
    pages_processed: int,
    rows_processed: int,
    recent_url_hashes: list[str],
) -> None:
    bounded_hashes = recent_url_hashes[-PAGINATION_CHECKPOINT_RECENT_URL_LIMIT:]
    is_complete = next_url is None
    updated = await db.status(
        f"""
        UPDATE {_qt(_schema(), ProviderDirectoryPaginationCheckpoint.__tablename__)}
           SET next_url = :next_url,
               state = :state,
               pages_processed = :pages_processed,
               rows_processed = :rows_processed,
               recent_cursor_hashes = CAST(:recent_cursor_hashes AS jsonb),
               updated_at = now(),
               completed_at = CASE WHEN :is_complete THEN now() ELSE NULL END
         WHERE canonical_api_base = :canonical_api_base
           AND resource_type = :resource_type
           AND source_scope_hash = :source_scope_hash
           AND owner_run_id = :owner_run_id;
        """,
        next_url=next_url,
        state=(
            PAGINATION_CHECKPOINT_COMPLETE
            if is_complete
            else PAGINATION_CHECKPOINT_ACTIVE
        ),
        pages_processed=pages_processed,
        rows_processed=rows_processed,
        recent_cursor_hashes=json.dumps(bounded_hashes),
        is_complete=is_complete,
        canonical_api_base=context.canonical_api_base,
        resource_type=resource_type,
        source_scope_hash=context.source_scope_hash,
        owner_run_id=context.owner_run_id,
    )
    if _coerce_rowcount(updated) <= 0:
        raise RuntimeError("provider_directory_pagination_checkpoint_ownership_lost")


async def _clear_pagination_checkpoints(
    context: PaginationCheckpointContext,
    resource_types: list[str],
) -> int:
    if not resource_types:
        return 0
    deleted = await db.status(
        f"""
        DELETE FROM {_qt(_schema(), ProviderDirectoryPaginationCheckpoint.__tablename__)}
         WHERE canonical_api_base = :canonical_api_base
           AND source_scope_hash = :source_scope_hash
           AND owner_run_id = :owner_run_id
           AND resource_type = ANY(CAST(:resource_types AS varchar[]));
        """,
        canonical_api_base=context.canonical_api_base,
        source_scope_hash=context.source_scope_hash,
        owner_run_id=context.owner_run_id,
        resource_types=resource_types,
    )
    return _coerce_rowcount(deleted)


async def _finalize_source_pagination_checkpoints(
    source: dict[str, Any],
    diagnostics_by_resource: dict[str, dict[str, Any]],
    resume_required_entries: set[str] | None,
) -> None:
    context = source.get("_pagination_checkpoint_context")
    if not isinstance(context, PaginationCheckpointContext):
        return
    incomplete_resource_types = [
        resource_type
        for resource_type, diagnostic in diagnostics_by_resource.items()
        if diagnostic.get("complete") is not True
        or diagnostic.get("error")
        or diagnostic.get("bounded")
    ]
    if incomplete_resource_types:
        if resume_required_entries is not None:
            resume_required_entries.update(
                f"{source['source_id']}:{resource_type}"
                for resource_type in incomplete_resource_types
            )
        return
    await _clear_pagination_checkpoints(
        context,
        list(diagnostics_by_resource),
    )


async def _reset_unexpected_empty_pagination_checkpoint(
    source: dict[str, Any],
    resource_type: str,
    page_count: int,
    result: ResourceFetchResult,
) -> None:
    context = source.get("_pagination_checkpoint_context")
    if (
        not isinstance(context, PaginationCheckpointContext)
        or result.error != CIGNA_UNEXPECTED_EMPTY_RESOURCE_ERROR
    ):
        return
    start_urls = _resource_start_urls(
        source,
        resource_type,
        page_count=page_count,
    )
    if len(start_urls) != 1:
        return
    await _save_pagination_checkpoint(
        context,
        resource_type,
        next_url=start_urls[0],
        pages_processed=0,
        rows_processed=0,
        recent_url_hashes=[],
    )


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
    linked_resource_deadline_seconds: int = 0,
    resource_deadline_seconds: int = 0,
    linked_counts: dict[str, int] | None = None,
    resource_fetch_stats: dict[str, dict[str, Any]] | None = None,
    resource_completion: dict[str, set[str]] | None = None,
    stale_counts: dict[str, int] | None = None,
    stale_cleanup: bool = False,
    stream_batch_size: int = 0,
    source_concurrency: int = 1,
    bulk_export: bool = False,
    cancel_ctx: dict[str, Any] | None = None,
    cancel_task: dict[str, Any] | None = None,
    progress_callback: Callable[[int, int, dict[str, int], dict[str, Any] | None], Awaitable[None]] | None = None,
    preserve_seen_stage: bool = False,
    is_pagination_checkpointing_enabled: bool = False,
    retry_of_run_id: str | None = None,
    pagination_resume_required: set[str] | None = None,
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
    progress_timer_by_name = {"next_partial_progress_at": 0.0}

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
        if not progress_callback:
            return
        snapshot: dict[str, int] | None = None
        details: dict[str, Any] | None = None
        async with progress_lock:
            now = time.monotonic()
            if force or now >= progress_timer_by_name["next_partial_progress_at"]:
                if not force:
                    progress_timer_by_name["next_partial_progress_at"] = now + 15.0
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
        source_ids = [item["source_id"] for item in source_group]
        source = _scoped_partition_source_record(source_group, source_ids)
        if is_pagination_checkpointing_enabled:
            source["_pagination_checkpoint_context"] = _pagination_checkpoint_context(
                source,
                source_ids,
                run_id=run_id,
                retry_of_run_id=retry_of_run_id,
            )
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
        deferred_zero_role_cleanup: ResourceFetchResult | None = None
        scan_role_reverse_lookup_planned = _scan_practitioner_role_reverse_lookup_planned(source, resources)

        async def mark_resource_stale_cleanup_ready(resource_type: str, result: ResourceFetchResult) -> None:
            if not stale_cleanup or not result.complete:
                return
            if seen_stage_table:
                source_stale_ready_source_ids[resource_type] = list(source_ids)
                return
            for source_id in source_ids:
                stale_deleted = await _delete_stale_resource_rows(
                    result.model,
                    source_id,
                    run_id,
                    use_seen_table=True,
                )
                if stale_deleted:
                    source_stale_counts[resource_type] = source_stale_counts.get(resource_type, 0) + stale_deleted

        def should_retry_zero_practitioner_role(result: ResourceFetchResult | None) -> bool:
            return (
                result is not None
                and not scan_role_reverse_lookup_planned
                and bool(_resource_start_url(source, "PractitionerRole", page_count=page_count))
                and result.complete
                and not result.error
                and not result.bounded
                and result.rows_fetched == 0
                and result.rows_written == 0
                and source_counts.get("Practitioner", 0) > 0
                and source_counts.get("Location", 0) > 0
            )
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
            if (
                resource_type == "PractitionerRole"
                and _scan_practitioner_role_requires_reverse_lookup(source, "PractitionerRole")
            ):
                continue
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = resource_type
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)
            use_streaming = stream_batch_size > 0

            async def row_batch_handler(model: type, rows: list[dict[str, Any]]) -> int:
                if linked_resource_limit > 0 or (
                    scan_role_reverse_lookup_planned
                    and resource_type in _practitioner_role_reverse_lookup_resources(source)
                ):
                    rows_by_resource.setdefault(resource_type, []).extend(
                        _compact_linked_reference_rows(resource_type, rows)
                    )
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

            if (
                stale_cleanup
                and resource_type == "PractitionerRole"
                and _is_uhc_role_postal_partition_enabled(source)
            ):
                await _mark_postal_checkpointed_roles_seen(
                    source,
                    source_ids,
                    run_id,
                    seen_stage_table,
                )
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
                retain_rows=not use_streaming,
                cancel_ctx=cancel_ctx,
                cancel_task=cancel_task,
                bulk_export=bulk_export,
                deadline_seconds=resource_deadline_seconds,
                pagination_checkpoint=source.get("_pagination_checkpoint_context"),
            )
            if not result:
                continue
            result = _fail_closed_on_unexpected_empty_resource(
                source,
                resource_type,
                result,
            )
            await _reset_unexpected_empty_pagination_checkpoint(
                source,
                resource_type,
                page_count,
                result,
            )
            for _source_id in source_ids:
                _record_resource_fetch_stats(source_resource_stats, resource_type, result)
            _record_resource_completion(resource_completion, resource_type, source_ids, result)
            if result.rows or resource_type not in rows_by_resource:
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
            if (
                resource_type == "PractitionerRole"
                and result.complete
                and not result.error
                and not result.bounded
                and result.rows_fetched == 0
                and written_total == 0
            ):
                deferred_zero_role_cleanup = result
            else:
                await mark_resource_stale_cleanup_ready(resource_type, result)
        if should_retry_zero_practitioner_role(deferred_zero_role_cleanup):
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = "PractitionerRole retry"
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)

            use_streaming = stream_batch_size > 0

            async def retry_row_batch_handler(model: type, rows: list[dict[str, Any]]) -> int:
                written = await _upsert_resource_rows(
                    model,
                    _copy_rows_for_source_ids(rows, source_ids),
                    run_id=run_id,
                    track_seen=stale_cleanup,
                    seen_table=seen_stage_table,
                    canonical_api_base=source.get("canonical_api_base") or source.get("api_base"),
                    source_ids=source_ids,
                )
                await maybe_report_partial_progress(group_key, "PractitionerRole", written)
                return written

            retry_result = await _fetch_resource_rows(
                source,
                "PractitionerRole",
                per_resource_limit=per_resource_limit,
                page_limit=page_limit,
                page_count=page_count,
                timeout=timeout,
                run_id=run_id,
                row_batch_handler=retry_row_batch_handler if use_streaming else None,
                row_batch_size=stream_batch_size,
                retain_rows=linked_resource_limit > 0 or not use_streaming,
                cancel_ctx=cancel_ctx,
                cancel_task=cancel_task,
                bulk_export=False,
                deadline_seconds=resource_deadline_seconds,
            )
            if retry_result is None:
                retry_result = replace(
                    deferred_zero_role_cleanup,
                    complete=False,
                    error=PRACTITIONER_ROLE_ZERO_RETRY_EMPTY_ERROR,
                    fetch_mode=f"{deferred_zero_role_cleanup.fetch_mode}_retry",
                )
                retry_written_total = 0
            else:
                retry_result = replace(retry_result, fetch_mode=f"{retry_result.fetch_mode}_retry")
                for _source_id in source_ids:
                    _record_resource_fetch_stats(source_resource_stats, "PractitionerRole", retry_result)
                _record_resource_completion(
                    resource_completion,
                    "PractitionerRole",
                    source_ids,
                    retry_result,
                )
                if retry_result.rows or "PractitionerRole" not in rows_by_resource:
                    rows_by_resource["PractitionerRole"] = retry_result.rows
                retry_written_total = (
                    retry_result.rows_written
                    if use_streaming
                    else await _upsert_resource_rows(
                        retry_result.model,
                        _copy_rows_for_source_ids(retry_result.rows, source_ids),
                        run_id=run_id,
                        track_seen=stale_cleanup,
                        seen_table=seen_stage_table,
                        canonical_api_base=source.get("canonical_api_base") or source.get("api_base"),
                        source_ids=source_ids,
                    )
                )
                source_counts["PractitionerRole"] = source_counts.get("PractitionerRole", 0) + retry_written_total
                if (
                    retry_result.complete
                    and not retry_result.error
                    and not retry_result.bounded
                    and retry_result.rows_fetched == 0
                    and retry_written_total == 0
                ):
                    retry_result = replace(
                        retry_result,
                        complete=False,
                        error=PRACTITIONER_ROLE_ZERO_RETRY_EMPTY_ERROR,
                    )
            retry_diagnostic = _resource_fetch_diagnostic(
                retry_result,
                rows_written_per_source=retry_written_total // max(1, len(source_ids)),
            )
            retry_diagnostic["retry_of_zero_rows"] = True
            retry_diagnostic["retry_reason"] = PRACTITIONER_ROLE_ZERO_RETRY_REASON
            source_resource_diagnostics["PractitionerRole"] = retry_diagnostic
            if retry_result.error != PRACTITIONER_ROLE_ZERO_RETRY_EMPTY_ERROR:
                await mark_resource_stale_cleanup_ready("PractitionerRole", retry_result)
        elif deferred_zero_role_cleanup is not None:
            await mark_resource_stale_cleanup_ready("PractitionerRole", deferred_zero_role_cleanup)
        if (
            "PractitionerRole" in resources
            and _scan_practitioner_role_requires_reverse_lookup(source, "PractitionerRole")
            and "PractitionerRole" not in source_resource_diagnostics
        ):
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = "PractitionerRole reverse lookup"
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)
            use_streaming = stream_batch_size > 0
            scan_seed_stage_table = (
                seen_stage_table
                if use_streaming and seen_stage_table and scan_role_reverse_lookup_planned
                else None
            )
            if scan_seed_stage_table:
                await _prepare_provider_directory_import_seen_stage_lookup(scan_seed_stage_table)
                for seed_resource_type in _practitioner_role_reverse_lookup_resources(source):
                    rows_by_resource.pop(seed_resource_type, None)

            async def scan_role_row_batch_handler(model: type, rows: list[dict[str, Any]]) -> int:
                """Write streamed SCAN PractitionerRole rows for each mirrored source."""
                written = await _upsert_resource_rows(
                    model,
                    _copy_rows_for_source_ids(rows, source_ids),
                    run_id=run_id,
                    track_seen=stale_cleanup,
                    seen_table=seen_stage_table,
                    canonical_api_base=source.get("canonical_api_base") or source.get("api_base"),
                    source_ids=source_ids,
                )
                await maybe_report_partial_progress(group_key, "PractitionerRole", written)
                return written

            result = await _fetch_scan_practitioner_role_rows(
                source,
                rows_by_resource,
                ScanPractitionerRoleFetchOptions(
                    per_resource_limit=per_resource_limit,
                    page_limit=page_limit,
                    page_count=page_count,
                    timeout=timeout,
                    run_id=run_id,
                    row_batch_handler=scan_role_row_batch_handler if use_streaming else None,
                    row_batch_size=stream_batch_size,
                    retain_rows=not use_streaming,
                    cancel_ctx=cancel_ctx,
                    cancel_task=cancel_task,
                    deadline_seconds=linked_resource_deadline_seconds,
                    source=source,
                    seed_stage_table=scan_seed_stage_table,
                    seed_source_ids=tuple(source_ids) if scan_seed_stage_table else (),
                    existing_seed_source_ids=tuple(source_ids) if not scan_seed_stage_table else (),
                    resume_completed_seeds=per_resource_limit <= 0 and page_limit <= 0,
                    seen_table=seen_stage_table,
                ),
            )
            for _source_id in source_ids:
                _record_resource_fetch_stats(source_resource_stats, "PractitionerRole", result)
            _record_resource_completion(resource_completion, "PractitionerRole", source_ids, result)
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
            source_counts["PractitionerRole"] = source_counts.get("PractitionerRole", 0) + written_total
            rows_by_resource["PractitionerRole"] = result.rows
            source_resource_diagnostics["PractitionerRole"] = _resource_fetch_diagnostic(
                result,
                rows_written_per_source=written_total // max(1, len(source_ids)),
            )
            await mark_resource_stale_cleanup_ready("PractitionerRole", result)
        if linked_resource_limit > 0 and rows_by_resource:
            async with progress_lock:
                active_group_details[group_key]["current_resource"] = "Linked resources"
                active_group_details[group_key]["resource_started_at"] = _now().isoformat(timespec="seconds") + "Z"
                active_group_details[group_key]["resource_started_monotonic"] = time.monotonic()
            await report_progress(force=True)

            async def linked_progress(resource_type: str, written: int) -> None:
                await maybe_report_partial_progress(group_key, resource_type, written)

            source_linked_counts = await _import_linked_resource_rows(
                source,
                rows_by_resource,
                per_source_limit=linked_resource_limit,
                concurrency=linked_resource_concurrency,
                timeout=timeout,
                run_id=run_id,
                source_ids=source_ids,
                track_seen=stale_cleanup,
                seen_table=seen_stage_table,
                progress_callback=linked_progress,
                deadline_seconds=linked_resource_deadline_seconds,
            )
        await _finalize_source_pagination_checkpoints(
            source,
            source_resource_diagnostics,
            pagination_resume_required,
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
    task = _apply_provider_directory_refresh_preset(task or {})
    await raise_if_cancelled(ctx, task)
    ctx.setdefault("context", {})
    test_mode = bool(task.get("test") or task.get("test_mode") or ctx["context"].get("test_mode"))
    ctx["context"]["test_mode"] = test_mode

    await ensure_database(test_mode)
    await _ensure_provider_directory_tables()

    run_id = _clean_text(task.get("run_id")) or _clean_text(ctx.get("control_run_id"))
    retry_of_run_id = _clean_text(task.get("retry_of_run_id"))
    requested_source_ids = _clean_source_id_list(
        task.get("source_ids")
        or task.get("source_id")
        or task.get("provider_directory_source_ids")
        or task.get("provider_directory_source_id")
    )
    limit = int(task.get("limit") or 0) or None
    source_query = _clean_text(task.get("source_query"))
    timeout = int(task.get("timeout") or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_TIMEOUT", "15"))
    concurrency = int(task.get("concurrency") or os.getenv("HLTHPRT_PROVIDER_DIRECTORY_CONCURRENCY", "5"))
    seed_only = bool(task.get("seed_only", False))
    probe = bool(task.get("probe", True))
    import_resources = bool(task.get("import_resources", False))
    canonical_backfill_only = bool(task.get("canonical_backfill_only", False))
    contact_backfill_only = bool(task.get("contact_backfill_only", False))
    publish_artifacts_only = bool(task.get("publish_artifacts_only", False))
    publish_corroboration = _publish_corroboration_enabled(task.get("publish_corroboration"))
    publish_artifacts_targets = _provider_directory_publish_artifact_targets(
        task.get("publish_artifacts_targets")
        or task.get("publish_artifact_targets")
        or task.get("publish_targets")
    )
    open_only = bool(task.get("open_only", True))
    include_auth_required = bool(task.get("include_auth_required", False))
    include_supplemental_catalogs = _bool_or_default(task.get("include_supplemental_catalogs"), False)
    credential_config_file = _clean_text(task.get("credential_config_file"))
    _PROVIDER_DIRECTORY_CREDENTIALS_FILE_OVERRIDE.set(credential_config_file)
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
    linked_resource_deadline_seconds = _int_or_default(
        task.get("linked_resource_deadline_seconds"),
        _env_int("HLTHPRT_PROVIDER_DIRECTORY_LINKED_RESOURCE_DEADLINE_SECONDS", 0),
    )
    resource_deadline_seconds = _int_or_default(
        task.get("resource_deadline_seconds"),
        _env_int("HLTHPRT_PROVIDER_DIRECTORY_RESOURCE_DEADLINE_SECONDS", 0),
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
        import_resources and set(resources) == set(DEFAULT_RESOURCES),
    )
    is_pagination_checkpointing_enabled = _is_pagination_checkpoint_mode_enabled(
        run_id=run_id,
        full_refresh=full_refresh,
        resource_limit=resource_limit,
        page_limit=page_limit,
        stream_batch_size=stream_batch_size,
        stale_cleanup=stale_cleanup,
        publish_artifacts=publish_artifacts,
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
    if publish_artifacts_only:
        metrics = {
            "publish_artifacts": True,
            "publish_artifacts_only": True,
            "source_ids": requested_source_ids,
        }
        metrics = await _publish_provider_directory_artifacts(
            run_id=run_id,
            metrics=metrics,
            address_key_run_id=None,
            publish_scope_run_id=None,
            source_ids=requested_source_ids,
            publish_corroboration=publish_corroboration,
            publish_artifacts_targets=publish_artifacts_targets,
        )
        ctx["context"]["audit"] = metrics
        ctx["context"]["run"] = ctx["context"].get("run", 0) + 1
        print("PROVIDER_DIRECTORY_ARTIFACT_PUBLISH_DONE\t" + json.dumps(metrics, sort_keys=True, default=str))
        return metrics
    await _clear_resource_rows_seen(run_id)

    seed_rows: list[dict[str, Any]]
    tmpdir = None
    retest_tmpdir = None
    supplemental_retest_seed_rows: list[dict[str, Any]] = []
    supplemental_catalog_seed_rows: list[dict[str, Any]] = []
    supplemental_catalog_metrics: dict[str, Any] = {
        "enabled": include_supplemental_catalogs,
        "rows": 0,
        "catalogs": {},
    }
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
        seed_rows = _seed_rows_from_sqlite(
            seed_path,
            limit=None if (task.get("retest_results_path") or task.get("retest_results_url")) else limit,
            source_query=source_query,
        )
    retest_path, retest_tmpdir = _resolve_retest_results(
        _clean_text(task.get("retest_results_path")),
        _clean_text(task.get("retest_results_url")),
    )
    if retest_path is not None:
        supplemental_retest_seed_rows = _seed_rows_from_retest_results(retest_path, source_query=source_query)
        seed_rows.extend(supplemental_retest_seed_rows)
    if include_supplemental_catalogs:
        supplemental_catalog_seed_rows, supplemental_catalog_metrics = _seed_rows_from_supplemental_catalogs(
            source_query=source_query,
            timeout=timeout,
            amerihealth_caritas_catalog_path=_clean_text(task.get("amerihealth_caritas_catalog_path")),
            amerihealth_caritas_catalog_url=_clean_text(task.get("amerihealth_caritas_catalog_url")),
            contra_costa_catalog_path=_clean_text(task.get("contra_costa_catalog_path")),
            contra_costa_catalog_url=_clean_text(task.get("contra_costa_catalog_url")),
            cms_sma_endpoint_directory_path=_clean_text(task.get("cms_sma_endpoint_directory_path")),
            cms_sma_endpoint_directory_url=_clean_text(task.get("cms_sma_endpoint_directory_url")),
        )
        supplemental_catalog_metrics["enabled"] = True
        seed_rows.extend(supplemental_catalog_seed_rows)

    try:
        source_rows = _dedupe_source_rows([_source_row_from_seed(row) for row in seed_rows])
        source_rows = _scope_source_rows(source_rows, requested_source_ids)
        if limit and (task.get("retest_results_path") or task.get("retest_results_url")):
            source_rows = source_rows[:limit]
        await _upsert_rows(ProviderDirectorySource, source_rows)
        stale_source_rows_deleted = {}
        if _source_catalog_stale_cleanup_enabled(
            stale_cleanup=stale_cleanup,
            full_refresh=full_refresh,
            source_query=source_query,
            limit=limit,
            requested_source_ids=requested_source_ids,
            retest_results_configured=retest_path is not None,
        ):
            stale_source_rows_deleted = await _delete_stale_provider_directory_source_catalog(
                [row["source_id"] for row in source_rows]
            )
        metrics: dict[str, Any] = {
            "sources_seeded": len(source_rows),
            "source_ids": requested_source_ids,
            "supplemental_retest_sources_considered": len(supplemental_retest_seed_rows),
            "supplemental_catalog_sources_considered": len(supplemental_catalog_seed_rows),
            "supplemental_catalogs": supplemental_catalog_metrics,
            "stale_source_rows_deleted": stale_source_rows_deleted,
            "sources_probed": 0,
            "valid_capability_sources": 0,
            "resource_rows": {},
            "full_refresh": full_refresh,
            "resource_limit": resource_limit,
            "linked_resource_limit": linked_resource_limit,
            "linked_resource_deadline_seconds": linked_resource_deadline_seconds,
            "resource_deadline_seconds": resource_deadline_seconds,
            "page_limit": page_limit,
            "page_count": page_count,
            "stream_batch_size": stream_batch_size,
            "bulk_export": bulk_export,
            "source_concurrency": source_concurrency,
            "stale_cleanup": stale_cleanup,
            "publish_artifacts": publish_artifacts,
            "publish_corroboration": publish_corroboration,
            "pagination_checkpoints_enabled": is_pagination_checkpointing_enabled,
            "retry_of_run_id": retry_of_run_id,
            "credential_config_file_configured": bool(credential_config_file),
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
            importable, selection_metrics = _select_resource_import_sources(
                source_rows,
                valid_source_ids=valid_source_ids,
                open_only=open_only,
                include_auth_required=include_auth_required,
            )
            metrics.update(selection_metrics)
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
            completed_source_ids_by_resource: dict[str, set[str]] = {}
            pagination_resume_required_entries: set[str] = set()
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
                linked_resource_deadline_seconds=linked_resource_deadline_seconds,
                resource_deadline_seconds=resource_deadline_seconds,
                linked_counts=metrics.setdefault("linked_resource_rows", {}),
                resource_fetch_stats=metrics.setdefault("resource_fetch_stats", {}),
                resource_completion=completed_source_ids_by_resource,
                stale_counts=metrics.setdefault("stale_resource_rows_deleted", {}),
                stale_cleanup=stale_cleanup,
                stream_batch_size=stream_batch_size,
                source_concurrency=source_concurrency,
                bulk_export=bulk_export,
                cancel_ctx=ctx,
                cancel_task={**task, "run_id": run_id},
                progress_callback=resource_progress,
                preserve_seen_stage=bool(seen_stage_table_for_publish),
                is_pagination_checkpointing_enabled=is_pagination_checkpointing_enabled,
                retry_of_run_id=retry_of_run_id,
                pagination_resume_required=pagination_resume_required_entries,
            )
            if pagination_resume_required_entries:
                required_entries = sorted(pagination_resume_required_entries)
                metrics["pagination_resume_required"] = required_entries
                ctx["context"]["audit"] = metrics
                raise RuntimeError(
                    f"{PAGINATION_RESUME_REQUIRED_ERROR}:"
                    + ",".join(required_entries)
                )
            metrics["resource_fetch_completed_source_ids"] = {
                resource_type: sorted(source_ids)
                for resource_type, source_ids in sorted(completed_source_ids_by_resource.items())
            }
            metrics["sources_import_attempted"] = len(importable)
            if publish_artifacts:
                artifact_source_ids = requested_source_ids
                if not artifact_source_ids:
                    artifact_source_ids = [
                        importable_source["source_id"]
                        for importable_source in importable
                        if _clean_text(importable_source.get("source_id")) is not None
                    ]
                required_publish_resources = [
                    resource_type
                    for resource_type in ("PractitionerRole", "Practitioner", "Location")
                    if resource_type in resources
                ]
                resource_fetch_stats_by_resource = metrics.get("resource_fetch_stats") or {}
                completion_tracking_available = bool(completed_source_ids_by_resource) or bool(
                    resource_fetch_stats_by_resource
                )
                if required_publish_resources and completion_tracking_available:
                    publishable_artifact_source_ids = [
                        source_id
                        for source_id in artifact_source_ids
                        if all(
                            source_id in completed_source_ids_by_resource.get(resource_type, set())
                            for resource_type in required_publish_resources
                        )
                    ]
                else:
                    publishable_artifact_source_ids = list(artifact_source_ids)
                metrics["publishable_artifact_source_ids"] = publishable_artifact_source_ids
                skipped_artifact_source_ids = sorted(set(artifact_source_ids) - set(publishable_artifact_source_ids))
                if skipped_artifact_source_ids:
                    metrics["artifact_publish_skipped_source_ids"] = skipped_artifact_source_ids
                    metrics["artifact_publish_required_resources"] = required_publish_resources
                if required_publish_resources and not publishable_artifact_source_ids:
                    skip_payload_by_metric = {
                        "skipped": True,
                        "reason": "critical_resource_fetch_incomplete",
                        "required_resources": required_publish_resources,
                        "source_ids": artifact_source_ids,
                    }
                    metrics["location_contacts_backfilled"] = skip_payload_by_metric
                    metrics["location_coordinates_backfilled"] = skip_payload_by_metric
                    metrics["location_address_keys_stamped"] = skip_payload_by_metric
                    metrics["location_archive"] = skip_payload_by_metric
                    metrics["address_overlay"] = skip_payload_by_metric
                    metrics["network_catalog"] = skip_payload_by_metric
                    metrics["ptg_corroboration_view_published"] = False
                    metrics["ptg_corroboration_view_skipped"] = skip_payload_by_metric
                else:
                    metrics = await _publish_provider_directory_artifacts(
                        run_id=run_id,
                        metrics=metrics,
                        seen_table=seen_stage_table_for_publish,
                        address_key_run_id=run_id,
                        publish_scope_run_id=run_id,
                        source_ids=publishable_artifact_source_ids,
                        publish_corroboration=publish_corroboration,
                        publish_artifacts_targets=publish_artifacts_targets,
                    )
            else:
                metrics["location_contacts_backfilled"] = {
                    "skipped": True,
                    "reason": "publish_artifacts_disabled",
                }
                metrics["location_coordinates_backfilled"] = {
                    "skipped": True,
                    "reason": "publish_artifacts_disabled",
                }
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
        if retest_tmpdir is not None:
            retest_tmpdir.cleanup()


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
    retest_results_path: str | None = None,
    retest_results_url: str | None = None,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | str | None = None,
    limit: int | None = None,
    source_query: str | None = None,
    refresh_preset: str | None = None,
    include_supplemental_catalogs: bool | None = None,
    cms_sma_endpoint_directory_path: str | None = None,
    cms_sma_endpoint_directory_url: str | None = None,
    seed_only: bool = False,
    probe: bool = True,
    import_resources: bool = False,
    canonical_backfill_only: bool = False,
    contact_backfill_only: bool = False,
    publish_artifacts_only: bool = False,
    publish_corroboration: bool | None = None,
    full_refresh: bool = False,
    stale_cleanup: bool | None = None,
    publish_artifacts: bool | None = None,
    open_only: bool = True,
    include_auth_required: bool = False,
    credential_config_file: str | None = None,
    resources: str | None = None,
    resource_limit: int | None = None,
    resource_deadline_seconds: int | None = None,
    linked_resource_limit: int | None = None,
    linked_resource_deadline_seconds: int | None = None,
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
        "retest_results_path": retest_results_path,
        "retest_results_url": retest_results_url,
        "run_id": run_id,
        "source_ids": source_ids,
        "limit": limit,
        "source_query": source_query,
        "refresh_preset": refresh_preset,
        "include_supplemental_catalogs": include_supplemental_catalogs,
        "cms_sma_endpoint_directory_path": cms_sma_endpoint_directory_path,
        "cms_sma_endpoint_directory_url": cms_sma_endpoint_directory_url,
        "seed_only": seed_only,
        "probe": probe,
        "import_resources": import_resources,
        "canonical_backfill_only": canonical_backfill_only,
        "contact_backfill_only": contact_backfill_only,
        "publish_artifacts_only": publish_artifacts_only,
        "publish_corroboration": publish_corroboration,
        "full_refresh": full_refresh,
        "stale_cleanup": stale_cleanup,
        "publish_artifacts": publish_artifacts,
        "open_only": open_only,
        "include_auth_required": include_auth_required,
        "credential_config_file": credential_config_file,
        "resources": resources,
        "resource_limit": resource_limit,
        "resource_deadline_seconds": resource_deadline_seconds,
        "linked_resource_limit": linked_resource_limit,
        "linked_resource_deadline_seconds": linked_resource_deadline_seconds,
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


def _clean_source_id_list(raw_source_ids: Any) -> list[str]:
    if raw_source_ids in (None, ""):
        return []
    if isinstance(raw_source_ids, str):
        source_id_values = raw_source_ids.split(",")
    elif isinstance(raw_source_ids, (bytes, bytearray, dict)) or not hasattr(raw_source_ids, "__iter__"):
        source_id_values = (raw_source_ids,)
    else:
        source_id_values = raw_source_ids
    cleaned_source_ids: list[str] = []
    seen_source_ids: set[str] = set()
    for source_id_candidate in source_id_values:
        source_id_text = _clean_text(source_id_candidate)
        if not source_id_text or source_id_text in seen_source_ids:
            continue
        seen_source_ids.add(source_id_text)
        cleaned_source_ids.append(source_id_text)
    return cleaned_source_ids


def _scope_source_rows(
    source_rows: list[dict[str, Any]],
    requested_source_ids: list[str],
) -> list[dict[str, Any]]:
    if not requested_source_ids:
        return source_rows
    rows_by_source_id = {
        source_id: row
        for row in source_rows
        if (source_id := _clean_text(row.get("source_id")))
    }
    missing_source_ids = [source_id for source_id in requested_source_ids if source_id not in rows_by_source_id]
    if missing_source_ids:
        raise ValueError(
            "Provider Directory source_id not found in resolved source catalog: "
            + ", ".join(missing_source_ids)
        )
    return [rows_by_source_id[source_id] for source_id in requested_source_ids]


PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE = "provider_directory_address_overlay"
PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_PREFIX = "provider_directory_address_overlay_stage"
PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES = (
    "source_record_idx",
    "npi_idx",
    "address_key_idx",
    "source_idx",
    "phone_idx",
)


def _address_overlay_stage_table_name(run_id: str | None = None) -> str:
    raw_identifier = run_id if run_id else f"{os.getpid()}_{time.time_ns()}"
    digest = hashlib.sha1(raw_identifier.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"{PROVIDER_DIRECTORY_ADDRESS_OVERLAY_STAGE_PREFIX}_{digest}"


def _coerce_rowcount(value: Any) -> int:
    if value is None:
        return 0
    text_value = str(value)
    matches = re.findall(r"(\d+)", text_value)
    return int(matches[-1]) if matches else 0


def _coordinate_country_key_sql(country_expr: str) -> str:
    return f"regexp_replace(upper(COALESCE(({country_expr})::varchar, '')), '[^A-Z0-9]', '', 'g')"


def _coordinate_pair_plausible_sql(latitude_expr: str, longitude_expr: str, country_expr: str) -> str:
    country_key = _coordinate_country_key_sql(country_expr)
    default_us = f"{country_key} IN ('', 'US', 'USA', 'UNITEDSTATES', 'UNITEDSTATESOFAMERICA', '840', '001')"
    return f"""
        ({latitude_expr}) BETWEEN -90 AND 90
        AND ({longitude_expr}) BETWEEN -180 AND 180
        AND NOT (ABS({latitude_expr}) < 0.0000001 AND ABS({longitude_expr}) < 0.0000001)
        AND (
            NOT ({default_us})
            OR (({latitude_expr}) BETWEEN 24 AND 50 AND ({longitude_expr}) BETWEEN -125 AND -66)
            OR (({latitude_expr}) BETWEEN 51 AND 72 AND ({longitude_expr}) BETWEEN -180 AND -129)
            OR (({latitude_expr}) BETWEEN 18 AND 23 AND ({longitude_expr}) BETWEEN -161 AND -154)
            OR (({latitude_expr}) BETWEEN 17 AND 19 AND ({longitude_expr}) BETWEEN -68 AND -64)
            OR (({latitude_expr}) BETWEEN 13 AND 16 AND ({longitude_expr}) BETWEEN 144 AND 146)
            OR (({latitude_expr}) BETWEEN -15 AND -10 AND ({longitude_expr}) BETWEEN -171 AND -168)
        )
    """


def _coordinate_from_location_sql(
    latitude_expr: str,
    longitude_expr: str,
    country_expr: str,
    *,
    axis: str,
) -> str:
    if axis not in {"lat", "long"}:
        raise ValueError(f"unsupported coordinate axis: {axis}")
    numeric_re = r"^-?[0-9]+(\.[0-9]+)?$"
    raw_lat = f"({latitude_expr})::numeric"
    raw_long = f"({longitude_expr})::numeric"
    scaled_1m_lat = f"(({latitude_expr})::numeric / 1000000)"
    scaled_1m_long = f"(({longitude_expr})::numeric / 1000000)"
    scaled_10m_lat = f"(({latitude_expr})::numeric / 10000000)"
    scaled_10m_long = f"(({longitude_expr})::numeric / 10000000)"
    raw_value = raw_lat if axis == "lat" else raw_long
    scaled_1m_value = scaled_1m_lat if axis == "lat" else scaled_1m_long
    scaled_10m_value = scaled_10m_lat if axis == "lat" else scaled_10m_long
    numeric_guard = (
        f"({latitude_expr}) ~ '{numeric_re}' AND ({longitude_expr}) ~ '{numeric_re}'"
    )
    return f"""
        CASE
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(raw_lat, raw_long, country_expr)}
                THEN {raw_value}
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(scaled_1m_lat, scaled_1m_long, country_expr)}
                THEN {scaled_1m_value}
            WHEN {numeric_guard}
             AND {_coordinate_pair_plausible_sql(scaled_10m_lat, scaled_10m_long, country_expr)}
                THEN {scaled_10m_value}
            ELSE NULL::numeric
        END
    """


def _coordinate_text_sql(numeric_expr: str) -> str:
    return (
        "NULLIF(TRIM(TRAILING '.' FROM "
        f"TRIM(TRAILING '0' FROM ROUND(({numeric_expr})::numeric, 8)::varchar)"
        "), '')::varchar"
    )


def _provider_directory_address_overlay_columns() -> tuple[str, ...]:
    return (
        "source_record_id",
        "source_id",
        "last_seen_run_id",
        "resource_type",
        "resource_id",
        "npi",
        "address_key",
        "first_line",
        "second_line",
        "city_name",
        "state_name",
        "state_code",
        "postal_code",
        "country_code",
        "telephone_number",
        "fax_number",
        "phone_number",
        "fax_number_digits",
        "lat",
        "long",
        "address_precision",
        "source_updated_at",
        "published_at",
    )


def provider_directory_address_overlay_table_sql(db_schema: str | None = None, table_name: str | None = None) -> str:
    """Create the compact Provider Directory address overlay relation."""
    schema = db_schema if db_schema is not None else _schema()
    table_ref = _qt(schema, table_name or PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    return f"""
    CREATE TABLE IF NOT EXISTS {table_ref} (
        source_record_id varchar PRIMARY KEY,
        source_id varchar(64) NOT NULL,
        last_seen_run_id varchar(64),
        resource_type varchar(64) NOT NULL,
        resource_id varchar(256) NOT NULL,
        npi bigint NOT NULL,
        address_key uuid NOT NULL,
        first_line varchar,
        second_line varchar,
        city_name varchar,
        state_name varchar,
        state_code varchar(2),
        postal_code varchar,
        country_code varchar,
        telephone_number varchar,
        fax_number varchar,
        phone_number varchar(15),
        fax_number_digits varchar(15),
        lat numeric,
        long numeric,
        address_precision varchar(32) NOT NULL DEFAULT 'street',
        source_updated_at timestamp,
        published_at timestamp NOT NULL DEFAULT now()
    );
    """


def address_overlay_coordinate_columns_sql(
    db_schema: str | None = None,
    table_name: str | None = None,
) -> str:
    """Add optional coordinate columns to older Provider Directory overlay tables."""
    schema = db_schema if db_schema is not None else _schema()
    table_ref = _qt(schema, table_name or PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    return f"""
    ALTER TABLE {table_ref}
        ADD COLUMN IF NOT EXISTS lat numeric,
        ADD COLUMN IF NOT EXISTS long numeric;
    """


async def _ensure_provider_directory_address_overlay_table(schema: str) -> None:
    await db.status(provider_directory_address_overlay_table_sql(schema))
    await db.status(address_overlay_coordinate_columns_sql(schema))
    await _create_provider_directory_address_overlay_indexes(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)


def _address_overlay_index_name(table_name: str, suffix: str) -> str:
    return _bounded_identifier(f"{table_name}_{suffix}")


async def _create_provider_directory_address_overlay_indexes(schema: str, table_name: str) -> None:
    table_ref = _qt(schema, table_name)
    statements = (
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_q(_address_overlay_index_name(table_name, 'source_record_idx'))} "
        f"ON {table_ref} (source_record_id);",
        f"CREATE INDEX IF NOT EXISTS {_q(_address_overlay_index_name(table_name, 'npi_idx'))} ON {table_ref} (npi);",
        f"CREATE INDEX IF NOT EXISTS {_q(_address_overlay_index_name(table_name, 'address_key_idx'))} "
        f"ON {table_ref} (address_key);",
        f"CREATE INDEX IF NOT EXISTS {_q(_address_overlay_index_name(table_name, 'source_idx'))} "
        f"ON {table_ref} (source_id);",
        f"""
        CREATE INDEX IF NOT EXISTS {_q(_address_overlay_index_name(table_name, 'phone_idx'))}
            ON {table_ref} (phone_number, npi)
         WHERE phone_number IS NOT NULL AND phone_number <> '';
        """,
    )
    for statement in statements:
        await db.status(statement)


async def _rename_address_overlay_stage_indexes(schema: str, stage_table: str) -> None:
    for suffix in PROVIDER_DIRECTORY_ADDRESS_OVERLAY_INDEX_SUFFIXES:
        stage_index_name = _address_overlay_index_name(stage_table, suffix)
        target_index_name = _address_overlay_index_name(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE, suffix)
        if stage_index_name == target_index_name:
            continue
        await db.status(
            f"ALTER INDEX IF EXISTS {_qt(schema, stage_index_name)} RENAME TO {_q(target_index_name)};"
        )


async def _address_overlay_scope_sources(
    schema: str,
    *,
    run_id: str | None,
    source_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    cleaned = _clean_source_id_list(source_ids)
    if cleaned or not run_id:
        return cleaned
    rows = await db.all(
        f"""
        SELECT DISTINCT source_id
          FROM (
                SELECT source_id FROM {_qt(schema, "provider_directory_organization")} WHERE last_seen_run_id = :run_id
                UNION
                SELECT source_id FROM {_qt(schema, "provider_directory_practitioner_role")} WHERE last_seen_run_id = :run_id
                UNION
                SELECT source_id FROM {_qt(schema, "provider_directory_organization_affiliation")} WHERE last_seen_run_id = :run_id
          ) AS scoped
         WHERE source_id IS NOT NULL
         ORDER BY source_id;
        """,
        run_id=run_id,
    )
    return _clean_source_id_list([row[0] for row in rows])


def _provider_directory_overlay_scope_filter(
    alias: str,
    *,
    run_id: str | None,
    source_ids: list[str] | tuple[str, ...] | None,
) -> str:
    clauses: list[str] = []
    if run_id:
        clauses.append(f"{alias}.last_seen_run_id = CAST(:run_id AS varchar)")
    if source_ids:
        clauses.append(f"{alias}.source_id = ANY(CAST(:source_ids AS varchar[]))")
    return "".join(f"\n                   AND {clause}" for clause in clauses)


ADDRESS_OVERLAY_INSERT_SQL_TEMPLATE = """
    INSERT INTO {stage_ref} ({columns})
    WITH organization_address_rows AS (
        SELECT
            ('provider_directory_fhir:organization_address:' || organization.source_id || ':' ||
                organization.resource_id || ':' || addr.ordinal::varchar)::varchar AS source_record_id,
            organization.source_id::varchar AS source_id,
            organization.last_seen_run_id::varchar AS last_seen_run_id,
            'Organization'::varchar AS resource_type,
            organization.resource_id::varchar AS resource_id,
            organization.npi::bigint AS npi,
            {qschema}.addr_key_v1(
                NULLIF(TRIM(addr.value->'line'->>0), ''),
                NULLIF(TRIM(addr.value->'line'->>1), ''),
                NULLIF(TRIM(addr.value->>'city'), ''),
                NULLIF(TRIM(addr.value->>'state'), ''),
                NULLIF(TRIM(addr.value->>'postalCode'), ''),
                {org_address_country_expr}
            ) AS address_key,
            NULLIF(TRIM(addr.value->'line'->>0), '')::varchar AS first_line,
            NULLIF(TRIM(addr.value->'line'->>1), '')::varchar AS second_line,
            NULLIF(TRIM(addr.value->>'city'), '')::varchar AS city_name,
            NULLIF(TRIM(addr.value->>'state'), '')::varchar AS state_name,
            {qschema}.addr_state_code_v1(NULLIF(TRIM(addr.value->>'state'), ''))::varchar AS state_code,
            NULLIF(TRIM(addr.value->>'postalCode'), '')::varchar AS postal_code,
            {org_address_country_expr}::varchar AS country_code,
            org_phone.telephone_number::varchar AS telephone_number,
            org_fax.fax_number::varchar AS fax_number,
            NULL::varchar AS phone_number,
            NULL::varchar AS fax_number_digits,
            NULL::numeric AS lat,
            NULL::numeric AS long,
            'street'::varchar AS address_precision,
            organization.updated_at AS source_updated_at,
            now() AS published_at
          FROM {organization_table} AS organization
          JOIN LATERAL jsonb_array_elements(
                COALESCE(organization.address_json::jsonb, '[]'::jsonb)
          ) WITH ORDINALITY AS addr(value, ordinal) ON TRUE
          LEFT JOIN LATERAL (
              SELECT telecom.value->>'value' AS telephone_number
                FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
               WHERE telecom.value->>'system' = 'phone'
                 AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
               LIMIT 1
          ) AS org_phone ON TRUE
          LEFT JOIN LATERAL (
              SELECT telecom.value->>'value' AS fax_number
                FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
               WHERE telecom.value->>'system' = 'fax'
                 AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
               LIMIT 1
          ) AS org_fax ON TRUE
         WHERE organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND NULLIF(TRIM(addr.value->'line'->>0), '') IS NOT NULL
           AND NULLIF(TRIM(addr.value->>'city'), '') IS NOT NULL
           AND NULLIF(TRIM(addr.value->>'postalCode'), '') IS NOT NULL
           {org_scope}
    ), practitioner_location_rows AS (
        SELECT
            ('provider_directory_fhir:practitioner_role:' || role.source_id || ':' ||
                role.resource_id || ':' || loc.resource_id)::varchar AS source_record_id,
            role.source_id::varchar AS source_id,
            role.last_seen_run_id::varchar AS last_seen_run_id,
            'PractitionerRole'::varchar AS resource_type,
            role.resource_id::varchar AS resource_id,
            COALESCE(practitioner.npi, role.npi)::bigint AS npi,
            CASE
                WHEN loc.address_key ~* '{uuid_re}' THEN loc.address_key::uuid
                ELSE {qschema}.addr_key_v1(
                    loc.first_line,
                    loc.second_line,
                    loc.city_name,
                    COALESCE(NULLIF(loc.state_name, ''), loc.state_code),
                    loc.postal_code,
                    {location_country_expr}
                )
            END AS address_key,
            loc.first_line::varchar AS first_line,
            loc.second_line::varchar AS second_line,
            loc.city_name::varchar AS city_name,
            COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
            loc.state_code::varchar AS state_code,
            loc.postal_code::varchar AS postal_code,
            {location_country_expr}::varchar AS country_code,
            COALESCE(role_phone.telephone_number, loc.telephone_number)::varchar AS telephone_number,
            COALESCE(role_fax.fax_number, loc.fax_number)::varchar AS fax_number,
            COALESCE(
                {role_phone_number_expr},
                loc.phone_number
            )::varchar AS phone_number,
            COALESCE(
                {role_fax_number_expr},
                loc.fax_number_digits
            )::varchar AS fax_number_digits,
            {location_lat_expr} AS lat,
            {location_long_expr} AS long,
            'street'::varchar AS address_precision,
            GREATEST(
                COALESCE(role.updated_at, TIMESTAMP 'epoch'),
                COALESCE(practitioner.updated_at, TIMESTAMP 'epoch'),
                COALESCE(loc.updated_at, TIMESTAMP 'epoch')
            ) AS source_updated_at,
            now() AS published_at
          FROM {practitioner_role_table} AS role
          JOIN {practitioner_table} AS practitioner
            ON practitioner.source_id = role.source_id
           AND practitioner.resource_id = NULLIF(regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''), '')
          JOIN LATERAL jsonb_array_elements_text(COALESCE(role.location_refs::jsonb, '[]'::jsonb)) AS location_ref(value)
            ON TRUE
          JOIN {location_table} AS loc
            ON loc.source_id = role.source_id
           AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
          LEFT JOIN LATERAL (
              SELECT telecom.value->>'value' AS telephone_number
                FROM jsonb_array_elements(COALESCE(role.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
               WHERE telecom.value->>'system' = 'phone'
                 AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
               LIMIT 1
          ) AS role_phone ON TRUE
          LEFT JOIN LATERAL (
              SELECT telecom.value->>'value' AS fax_number
                FROM jsonb_array_elements(COALESCE(role.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
               WHERE telecom.value->>'system' = 'fax'
                 AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
               LIMIT 1
          ) AS role_fax ON TRUE
         WHERE COALESCE(practitioner.npi, role.npi) BETWEEN 1000000000 AND 9999999999
           AND practitioner.active IS DISTINCT FROM false
           AND role.active IS DISTINCT FROM false
           AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
           AND NULLIF(TRIM(loc.first_line), '') IS NOT NULL
           AND NULLIF(TRIM(loc.city_name), '') IS NOT NULL
           AND NULLIF(TRIM(loc.postal_code), '') IS NOT NULL
           {role_scope}
    ), affiliation_location_rows AS (
        SELECT
            ('provider_directory_fhir:organization_affiliation:' || affiliation.source_id || ':' ||
                affiliation.resource_id || ':' || loc.resource_id)::varchar AS source_record_id,
            affiliation.source_id::varchar AS source_id,
            affiliation.last_seen_run_id::varchar AS last_seen_run_id,
            'OrganizationAffiliation'::varchar AS resource_type,
            affiliation.resource_id::varchar AS resource_id,
            organization.npi::bigint AS npi,
            CASE
                WHEN loc.address_key ~* '{uuid_re}' THEN loc.address_key::uuid
                ELSE {qschema}.addr_key_v1(
                    loc.first_line,
                    loc.second_line,
                    loc.city_name,
                    COALESCE(NULLIF(loc.state_name, ''), loc.state_code),
                    loc.postal_code,
                    {location_country_expr}
                )
            END AS address_key,
            loc.first_line::varchar AS first_line,
            loc.second_line::varchar AS second_line,
            loc.city_name::varchar AS city_name,
            COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
            loc.state_code::varchar AS state_code,
            loc.postal_code::varchar AS postal_code,
            {location_country_expr}::varchar AS country_code,
            loc.telephone_number::varchar AS telephone_number,
            loc.fax_number::varchar AS fax_number,
            loc.phone_number::varchar AS phone_number,
            loc.fax_number_digits::varchar AS fax_number_digits,
            {location_lat_expr} AS lat,
            {location_long_expr} AS long,
            'street'::varchar AS address_precision,
            GREATEST(
                COALESCE(affiliation.updated_at, TIMESTAMP 'epoch'),
                COALESCE(organization.updated_at, TIMESTAMP 'epoch'),
                COALESCE(loc.updated_at, TIMESTAMP 'epoch')
            ) AS source_updated_at,
            now() AS published_at
          FROM {affiliation_table} AS affiliation
          JOIN LATERAL (
              SELECT DISTINCT normalized_ref AS resource_id
                FROM (
                    VALUES
                        (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                        (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                ) AS refs(normalized_ref)
               WHERE normalized_ref IS NOT NULL
          ) AS organization_ref ON TRUE
          JOIN {organization_table} AS organization
            ON organization.source_id = affiliation.source_id
           AND organization.resource_id = organization_ref.resource_id
          JOIN LATERAL jsonb_array_elements_text(COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)) AS location_ref(value)
            ON TRUE
          JOIN {location_table} AS loc
            ON loc.source_id = affiliation.source_id
           AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
         WHERE organization.npi BETWEEN 1000000000 AND 9999999999
           AND organization.active IS DISTINCT FROM false
           AND affiliation.active IS DISTINCT FROM false
           AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
           AND NULLIF(TRIM(loc.first_line), '') IS NOT NULL
           AND NULLIF(TRIM(loc.city_name), '') IS NOT NULL
           AND NULLIF(TRIM(loc.postal_code), '') IS NOT NULL
           {affiliation_scope}
    )
    SELECT DISTINCT ON (source_record_id)
        source_record_id,
        source_id,
        last_seen_run_id,
        resource_type,
        resource_id,
        npi,
        address_key,
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
        lat,
        long,
        address_precision,
        source_updated_at,
        published_at
      FROM (
            SELECT * FROM organization_address_rows
            UNION ALL
            SELECT * FROM practitioner_location_rows
            UNION ALL
            SELECT * FROM affiliation_location_rows
      ) AS overlay_rows
     WHERE address_key IS NOT NULL
     ORDER BY source_record_id, source_updated_at DESC NULLS LAST;
    """


def provider_directory_address_overlay_insert_sql(
    db_schema: str | None = None,
    stage_table: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Build the scoped insert for the compact Provider Directory address overlay."""
    schema = db_schema if db_schema is not None else _schema()
    stage_ref = _qt(schema, stage_table or _address_overlay_stage_table_name(run_id))
    location_country_expr = _country_restore_default_us_sql("loc.country_code")
    return ADDRESS_OVERLAY_INSERT_SQL_TEMPLATE.format(
        stage_ref=stage_ref,
        columns=", ".join(_provider_directory_address_overlay_columns()),
        qschema=_q(schema),
        org_address_country_expr=_country_restore_default_us_sql("addr.value->>'country'"),
        location_country_expr=location_country_expr,
        role_phone_number_expr=_canonical_contact_number_expr(
            "role_phone.telephone_number",
            location_country_expr,
        ),
        role_fax_number_expr=_canonical_contact_number_expr(
            "role_fax.fax_number",
            location_country_expr,
        ),
        location_lat_expr=_coordinate_from_location_sql(
            "loc.latitude",
            "loc.longitude",
            location_country_expr,
            axis="lat",
        ),
        location_long_expr=_coordinate_from_location_sql(
            "loc.latitude",
            "loc.longitude",
            location_country_expr,
            axis="long",
        ),
        uuid_re=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        org_scope=_provider_directory_overlay_scope_filter("organization", run_id=run_id, source_ids=source_ids),
        role_scope=_provider_directory_overlay_scope_filter("role", run_id=run_id, source_ids=source_ids),
        affiliation_scope=_provider_directory_overlay_scope_filter(
            "affiliation",
            run_id=run_id,
            source_ids=source_ids,
        ),
        organization_table=_qt(schema, "provider_directory_organization"),
        practitioner_table=_qt(schema, "provider_directory_practitioner"),
        location_table=_qt(schema, "provider_directory_location"),
        practitioner_role_table=_qt(schema, "provider_directory_practitioner_role"),
        affiliation_table=_qt(schema, "provider_directory_organization_affiliation"),
    )


ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES = {
    "organization_address": "organization",
    "practitioner_role": "role",
    "organization_affiliation": "affiliation",
}
ADDRESS_OVERLAY_COMPONENT_RESOURCE_TYPES = {
    "organization_address": "Organization",
    "practitioner_role": "PractitionerRole",
    "organization_affiliation": "OrganizationAffiliation",
}
ADDRESS_OVERLAY_COMPONENTS = tuple(ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES)


def _clean_address_overlay_components(raw_components: Any) -> tuple[str, ...]:
    if raw_components in (None, "", ()):
        return ADDRESS_OVERLAY_COMPONENTS
    if isinstance(raw_components, str):
        component_values = raw_components.split(",")
    elif isinstance(raw_components, (bytes, bytearray, dict)) or not hasattr(raw_components, "__iter__"):
        component_values = (raw_components,)
    else:
        component_values = raw_components
    cleaned_components: list[str] = []
    seen_components: set[str] = set()
    for component in component_values:
        component_text = _clean_text(component)
        if not component_text:
            continue
        if component_text not in ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES:
            raise ValueError(f"unknown Provider Directory address overlay component: {component_text}")
        if component_text in seen_components:
            continue
        seen_components.add(component_text)
        cleaned_components.append(component_text)
    return tuple(cleaned_components or ADDRESS_OVERLAY_COMPONENTS)


ADDRESS_OVERLAY_COMPONENT_INSERT_TEMPLATES = {
    "organization_address": """
        INSERT INTO {stage_ref} ({columns})
        WITH organization_rows AS MATERIALIZED (
            SELECT
                organization.source_id::varchar AS source_id,
                organization.last_seen_run_id::varchar AS last_seen_run_id,
                organization.resource_id::varchar AS resource_id,
                organization.npi::bigint AS npi,
                organization.address_json::jsonb AS address_json,
                organization.updated_at AS updated_at,
                (
                    SELECT telecom.value->>'value'
                      FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                     WHERE telecom.value->>'system' = 'phone'
                       AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                     LIMIT 1
                )::varchar AS telephone_number,
                (
                    SELECT telecom.value->>'value'
                      FROM jsonb_array_elements(COALESCE(organization.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                     WHERE telecom.value->>'system' = 'fax'
                       AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                     LIMIT 1
                )::varchar AS fax_number
              FROM {organization_table} AS organization
             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
               AND organization.active IS DISTINCT FROM false
               {component_scope}
        ), raw_org_addresses AS MATERIALIZED (
            SELECT
                ('provider_directory_fhir:organization_address:' || organization_rows.source_id || ':' ||
                    organization_rows.resource_id || ':' || addr.ordinal::varchar)::varchar AS source_record_id,
                organization_rows.source_id,
                organization_rows.last_seen_run_id,
                'Organization'::varchar AS resource_type,
                organization_rows.resource_id,
                organization_rows.npi,
                jsonb_build_array(
                    NULLIF(TRIM(addr.value->'line'->>0), ''),
                    NULLIF(TRIM(addr.value->'line'->>1), ''),
                    NULLIF(TRIM(addr.value->>'city'), ''),
                    NULLIF(TRIM(addr.value->>'state'), ''),
                    NULLIF(TRIM(addr.value->>'postalCode'), ''),
                    {org_address_country_expr}
                )::text::varchar AS address_lookup_key,
                NULLIF(TRIM(addr.value->'line'->>0), '')::varchar AS first_line,
                NULLIF(TRIM(addr.value->'line'->>1), '')::varchar AS second_line,
                NULLIF(TRIM(addr.value->>'city'), '')::varchar AS city_name,
                NULLIF(TRIM(addr.value->>'state'), '')::varchar AS state_name,
                NULLIF(TRIM(addr.value->>'postalCode'), '')::varchar AS postal_code,
                {org_address_country_expr}::varchar AS country_code,
                organization_rows.telephone_number,
                organization_rows.fax_number,
                organization_rows.updated_at AS source_updated_at
              FROM organization_rows
              JOIN LATERAL jsonb_array_elements(
                    COALESCE(organization_rows.address_json, '[]'::jsonb)
              ) WITH ORDINALITY AS addr(value, ordinal) ON TRUE
             WHERE NULLIF(TRIM(addr.value->'line'->>0), '') IS NOT NULL
               AND NULLIF(TRIM(addr.value->>'city'), '') IS NOT NULL
               AND NULLIF(TRIM(addr.value->>'postalCode'), '') IS NOT NULL
        ), org_address_keys AS MATERIALIZED (
            SELECT
                key_parts.address_lookup_key,
                key_parts.first_line,
                key_parts.second_line,
                key_parts.city_name,
                key_parts.state_name,
                key_parts.postal_code,
                key_parts.country_code,
                {qschema}.addr_key_v1(
                    key_parts.first_line,
                    key_parts.second_line,
                    key_parts.city_name,
                    key_parts.state_name,
                    key_parts.postal_code,
                    key_parts.country_code
                ) AS address_key,
                {qschema}.addr_state_code_v1(key_parts.state_name)::varchar AS state_code
              FROM (
                    SELECT DISTINCT
                        address_lookup_key,
                        first_line,
                        second_line,
                        city_name,
                        state_name,
                        postal_code,
                        country_code
                      FROM raw_org_addresses
              ) AS key_parts
        )
        SELECT
            raw.source_record_id,
            raw.source_id,
            raw.last_seen_run_id,
            raw.resource_type,
            raw.resource_id,
            raw.npi,
            keys.address_key,
            raw.first_line,
            raw.second_line,
            raw.city_name,
            raw.state_name,
            keys.state_code,
            raw.postal_code,
            raw.country_code,
            raw.telephone_number,
            raw.fax_number,
            NULL::varchar AS phone_number,
            NULL::varchar AS fax_number_digits,
            NULL::numeric AS lat,
            NULL::numeric AS long,
            'street'::varchar AS address_precision,
            raw.source_updated_at,
            now() AS published_at
          FROM raw_org_addresses AS raw
          JOIN org_address_keys AS keys
            ON keys.address_lookup_key = raw.address_lookup_key
         WHERE keys.address_key IS NOT NULL;
    """,
    "practitioner_role": """
        INSERT INTO {stage_ref} ({columns})
        SELECT {columns}
          FROM (
            SELECT
                ('provider_directory_fhir:practitioner_role:' || role.source_id || ':' ||
                    role.resource_id || ':' || loc.resource_id)::varchar AS source_record_id,
                role.source_id::varchar AS source_id,
                role.last_seen_run_id::varchar AS last_seen_run_id,
                'PractitionerRole'::varchar AS resource_type,
                role.resource_id::varchar AS resource_id,
                COALESCE(practitioner.npi, role.npi)::bigint AS npi,
                CASE
                    WHEN loc.address_key ~* '{uuid_re}' THEN loc.address_key::uuid
                    ELSE {qschema}.addr_key_v1(
                        loc.first_line,
                        loc.second_line,
                        loc.city_name,
                        COALESCE(NULLIF(loc.state_name, ''), loc.state_code),
                        loc.postal_code,
                        {location_country_expr}
                    )
                END AS address_key,
                loc.first_line::varchar AS first_line,
                loc.second_line::varchar AS second_line,
                loc.city_name::varchar AS city_name,
                COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
                loc.state_code::varchar AS state_code,
                loc.postal_code::varchar AS postal_code,
                {location_country_expr}::varchar AS country_code,
                COALESCE(role_phone.telephone_number, loc.telephone_number)::varchar AS telephone_number,
                COALESCE(role_fax.fax_number, loc.fax_number)::varchar AS fax_number,
                COALESCE({role_phone_number_expr}, loc.phone_number)::varchar AS phone_number,
                COALESCE({role_fax_number_expr}, loc.fax_number_digits)::varchar AS fax_number_digits,
                {location_lat_expr} AS lat,
                {location_long_expr} AS long,
                'street'::varchar AS address_precision,
                GREATEST(
                    COALESCE(role.updated_at, TIMESTAMP 'epoch'),
                    COALESCE(practitioner.updated_at, TIMESTAMP 'epoch'),
                    COALESCE(loc.updated_at, TIMESTAMP 'epoch')
                ) AS source_updated_at,
                now() AS published_at
              FROM {practitioner_role_table} AS role
              JOIN {practitioner_table} AS practitioner
                ON practitioner.source_id = role.source_id
               AND practitioner.resource_id = NULLIF(regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''), '')
              JOIN LATERAL jsonb_array_elements_text(COALESCE(role.location_refs::jsonb, '[]'::jsonb)) AS location_ref(value)
                ON TRUE
              JOIN {location_table} AS loc
                ON loc.source_id = role.source_id
               AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
              LEFT JOIN LATERAL (
                  SELECT telecom.value->>'value' AS telephone_number
                    FROM jsonb_array_elements(COALESCE(role.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                   WHERE telecom.value->>'system' = 'phone'
                     AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                   LIMIT 1
              ) AS role_phone ON TRUE
              LEFT JOIN LATERAL (
                  SELECT telecom.value->>'value' AS fax_number
                    FROM jsonb_array_elements(COALESCE(role.telecom::jsonb, '[]'::jsonb)) AS telecom(value)
                   WHERE telecom.value->>'system' = 'fax'
                     AND NULLIF(TRIM(telecom.value->>'value'), '') IS NOT NULL
                   LIMIT 1
              ) AS role_fax ON TRUE
             WHERE COALESCE(practitioner.npi, role.npi) BETWEEN 1000000000 AND 9999999999
               AND practitioner.active IS DISTINCT FROM false
               AND role.active IS DISTINCT FROM false
               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
               AND NULLIF(TRIM(loc.first_line), '') IS NOT NULL
               AND NULLIF(TRIM(loc.city_name), '') IS NOT NULL
               AND NULLIF(TRIM(loc.postal_code), '') IS NOT NULL
               {component_scope}
          ) AS overlay_rows
         WHERE address_key IS NOT NULL;
    """,
    "organization_affiliation": """
        INSERT INTO {stage_ref} ({columns})
        SELECT {columns}
          FROM (
            SELECT
                ('provider_directory_fhir:organization_affiliation:' || affiliation.source_id || ':' ||
                    affiliation.resource_id || ':' || loc.resource_id)::varchar AS source_record_id,
                affiliation.source_id::varchar AS source_id,
                affiliation.last_seen_run_id::varchar AS last_seen_run_id,
                'OrganizationAffiliation'::varchar AS resource_type,
                affiliation.resource_id::varchar AS resource_id,
                organization.npi::bigint AS npi,
                CASE
                    WHEN loc.address_key ~* '{uuid_re}' THEN loc.address_key::uuid
                    ELSE {qschema}.addr_key_v1(
                        loc.first_line,
                        loc.second_line,
                        loc.city_name,
                        COALESCE(NULLIF(loc.state_name, ''), loc.state_code),
                        loc.postal_code,
                        {location_country_expr}
                    )
                END AS address_key,
                loc.first_line::varchar AS first_line,
                loc.second_line::varchar AS second_line,
                loc.city_name::varchar AS city_name,
                COALESCE(NULLIF(loc.state_name, ''), loc.state_code)::varchar AS state_name,
                loc.state_code::varchar AS state_code,
                loc.postal_code::varchar AS postal_code,
                {location_country_expr}::varchar AS country_code,
                loc.telephone_number::varchar AS telephone_number,
                loc.fax_number::varchar AS fax_number,
                loc.phone_number::varchar AS phone_number,
                loc.fax_number_digits::varchar AS fax_number_digits,
                {location_lat_expr} AS lat,
                {location_long_expr} AS long,
                'street'::varchar AS address_precision,
                GREATEST(
                    COALESCE(affiliation.updated_at, TIMESTAMP 'epoch'),
                    COALESCE(organization.updated_at, TIMESTAMP 'epoch'),
                    COALESCE(loc.updated_at, TIMESTAMP 'epoch')
                ) AS source_updated_at,
                now() AS published_at
              FROM {affiliation_table} AS affiliation
              JOIN LATERAL (
                  SELECT DISTINCT normalized_ref AS resource_id
                    FROM (
                        VALUES
                            (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                            (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                    ) AS refs(normalized_ref)
                   WHERE normalized_ref IS NOT NULL
              ) AS organization_ref ON TRUE
              JOIN {organization_table} AS organization
                ON organization.source_id = affiliation.source_id
               AND organization.resource_id = organization_ref.resource_id
              JOIN LATERAL jsonb_array_elements_text(COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)) AS location_ref(value)
                ON TRUE
              JOIN {location_table} AS loc
                ON loc.source_id = affiliation.source_id
               AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
               AND organization.active IS DISTINCT FROM false
               AND affiliation.active IS DISTINCT FROM false
               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
               AND NULLIF(TRIM(loc.first_line), '') IS NOT NULL
               AND NULLIF(TRIM(loc.city_name), '') IS NOT NULL
               AND NULLIF(TRIM(loc.postal_code), '') IS NOT NULL
               {component_scope}
          ) AS overlay_rows
         WHERE address_key IS NOT NULL;
    """,
}


def _address_overlay_sql_context(schema: str, stage_table: str | None, run_id: str | None) -> dict[str, str]:
    """Return shared SQL template parameters for Provider Directory overlay inserts."""
    location_country_expr = _country_restore_default_us_sql("loc.country_code")
    return {
        "stage_ref": _qt(schema, stage_table or _address_overlay_stage_table_name(run_id)),
        "columns": ", ".join(_provider_directory_address_overlay_columns()),
        "qschema": _q(schema),
        "org_address_country_expr": _country_restore_default_us_sql("addr.value->>'country'"),
        "location_country_expr": location_country_expr,
        "role_phone_number_expr": _canonical_contact_number_expr(
            "role_phone.telephone_number",
            location_country_expr,
        ),
        "role_fax_number_expr": _canonical_contact_number_expr(
            "role_fax.fax_number",
            location_country_expr,
        ),
        "location_lat_expr": _coordinate_from_location_sql(
            "loc.latitude",
            "loc.longitude",
            location_country_expr,
            axis="lat",
        ),
        "location_long_expr": _coordinate_from_location_sql(
            "loc.latitude",
            "loc.longitude",
            location_country_expr,
            axis="long",
        ),
        "uuid_re": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        "organization_table": _qt(schema, "provider_directory_organization"),
        "practitioner_table": _qt(schema, "provider_directory_practitioner"),
        "location_table": _qt(schema, "provider_directory_location"),
        "practitioner_role_table": _qt(schema, "provider_directory_practitioner_role"),
        "affiliation_table": _qt(schema, "provider_directory_organization_affiliation"),
    }


def _address_overlay_component_insert_sql(
    db_schema: str | None = None,
    stage_table: str | None = None,
    *,
    component: str,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
) -> str:
    """Build one bounded insert for a Provider Directory overlay component."""
    template = ADDRESS_OVERLAY_COMPONENT_INSERT_TEMPLATES.get(component)
    scope_type = ADDRESS_OVERLAY_COMPONENT_SCOPE_TYPES.get(component)
    if template is None or scope_type is None:
        raise ValueError(f"unknown Provider Directory address overlay component: {component}")
    schema = db_schema if db_schema is not None else _schema()
    context = _address_overlay_sql_context(schema, stage_table, run_id)
    context["component_scope"] = _provider_directory_overlay_scope_filter(
        scope_type,
        run_id=run_id,
        source_ids=source_ids,
    )
    return template.format(**context)


PROVIDER_DIRECTORY_ADDRESS_OVERLAY_REQUIRED_TABLES = (
    "provider_directory_organization",
    "provider_directory_practitioner",
    "provider_directory_location",
    "provider_directory_practitioner_role",
    "provider_directory_organization_affiliation",
)


async def _address_overlay_missing_requirement(schema: str) -> str | None:
    if not await _address_canon_functions_available(schema):
        return "canonical_functions_unavailable"
    for table_name in PROVIDER_DIRECTORY_ADDRESS_OVERLAY_REQUIRED_TABLES:
        if not await _table_exists(schema, table_name):
            return f"{table_name}_unavailable"
    return None


async def _copy_existing_address_overlay(
    stage_ref: str,
    target_ref: str,
    columns: str,
    source_ids: list[str],
    *,
    refresh_resource_types: list[str] | tuple[str, ...] | None = None,
) -> int:
    if not source_ids:
        return 0
    query_param_dict: dict[str, Any] = {"source_ids": source_ids}
    if refresh_resource_types:
        query_param_dict["refresh_resource_types"] = list(refresh_resource_types)
        refresh_filter = (
            "source_id = ANY(CAST(:source_ids AS varchar[])) "
            "AND resource_type = ANY(CAST(:refresh_resource_types AS varchar[]))"
        )
    else:
        refresh_filter = "source_id = ANY(CAST(:source_ids AS varchar[]))"
    return _coerce_rowcount(
        await db.status(
            f"""
            INSERT INTO {stage_ref} ({columns})
            SELECT {columns}
              FROM {target_ref}
             WHERE NOT ({refresh_filter});
            """,
            **query_param_dict,
        )
    )


async def _swap_address_overlay_stage(schema: str, stage_table: str, stage_ref: str, target_ref: str) -> None:
    old_table = f"{PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE}_old"
    async with db.transaction():
        await db.status(f"DROP TABLE IF EXISTS {_qt(schema, old_table)};")
        await db.status(f"ALTER TABLE {target_ref} RENAME TO {_q(old_table)};")
        await db.status(f"ALTER TABLE {stage_ref} RENAME TO {_q(PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)};")
    await db.status(f"DROP TABLE IF EXISTS {_qt(schema, old_table)};")
    await _rename_address_overlay_stage_indexes(schema, stage_table)
    await _create_provider_directory_address_overlay_indexes(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)


async def _populate_address_overlay_stage(
    schema: str,
    stage_table: str,
    stage_ref: str,
    run_id: str | None,
    source_ids: list[str],
    query_param_dict: dict[str, Any],
    components: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    target_ref = _qt(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    columns = ", ".join(_provider_directory_address_overlay_columns())
    selected_components = _clean_address_overlay_components(components)
    refresh_resource_types = (
        None
        if set(selected_components) == set(ADDRESS_OVERLAY_COMPONENTS)
        else [ADDRESS_OVERLAY_COMPONENT_RESOURCE_TYPES[component] for component in selected_components]
    )
    copied_existing = await _copy_existing_address_overlay(
        stage_ref,
        target_ref,
        columns,
        source_ids,
        refresh_resource_types=refresh_resource_types,
    )
    inserted_by_component: dict[str, int] = {}
    for component in selected_components:
        inserted_by_component[component] = _coerce_rowcount(
            await db.status(
                _address_overlay_component_insert_sql(
                    schema,
                    stage_table,
                    component=component,
                    run_id=run_id,
                    source_ids=source_ids,
                ),
                **query_param_dict,
            )
        )
    inserted = sum(inserted_by_component.values())
    countries_normalized = await _normalize_address_overlay_stage_countries(stage_ref)
    archive_coordinate_backfill_rows = await _backfill_address_overlay_stage_coordinates(schema, stage_ref)
    duplicates_removed = await _dedupe_address_overlay_stage(
        stage_ref,
        source_ids=source_ids,
        resource_types=refresh_resource_types,
    )
    stage_rows = int(await db.scalar(f"SELECT COUNT(*) FROM {stage_ref};") or 0)
    await _create_provider_directory_address_overlay_indexes(schema, stage_table)
    await db.status(f"ANALYZE {stage_ref};")
    return {
        "copied_existing": copied_existing,
        "inserted": inserted,
        "inserted_by_component": inserted_by_component,
        "components": selected_components,
        "countries_normalized": countries_normalized,
        "archive_coordinate_backfill_rows": archive_coordinate_backfill_rows,
        "duplicates_removed": duplicates_removed,
        "stage_rows": stage_rows,
    }


async def _normalize_address_overlay_stage_countries(stage_ref: str) -> int:
    normalized_country_expr = _country_restore_sql("stage_row.country_code")
    return _coerce_rowcount(
        await db.status(
            f"""
            UPDATE {stage_ref} AS stage_row
               SET country_code = {normalized_country_expr}
             WHERE NULLIF({normalized_country_expr}, '') IS NOT NULL
               AND stage_row.country_code IS DISTINCT FROM {normalized_country_expr};
            """
        )
    )


async def _backfill_address_overlay_stage_coordinates(schema: str, stage_ref: str) -> int:
    if not await _table_exists(schema, "address_archive_v2"):
        return 0
    return _coerce_rowcount(
        await db.status(
            f"""
            UPDATE {stage_ref} AS stage_row
               SET lat = COALESCE(stage_row.lat, archive.lat),
                   long = COALESCE(stage_row.long, archive.long)
              FROM {_qt(schema, "address_archive_v2")} AS archive
             WHERE stage_row.address_key IS NOT NULL
               AND archive.address_key = stage_row.address_key
               AND archive.merged_into IS NULL
               AND archive.lat IS NOT NULL
               AND archive.long IS NOT NULL
               AND NOT (ABS(archive.lat) < 0.0000001 AND ABS(archive.long) < 0.0000001)
               AND (stage_row.lat IS NULL OR stage_row.long IS NULL);
            """
        )
    )


async def _dedupe_address_overlay_stage(
    stage_ref: str,
    *,
    source_ids: list[str] | tuple[str, ...] | None = None,
    resource_types: list[str] | tuple[str, ...] | None = None,
) -> int:
    where_clauses: list[str] = []
    query_param_dict: dict[str, Any] = {}
    cleaned_source_ids = _clean_source_id_list(source_ids)
    if cleaned_source_ids:
        where_clauses.append("source_id = ANY(CAST(:dedupe_source_ids AS varchar[]))")
        query_param_dict["dedupe_source_ids"] = cleaned_source_ids
    cleaned_resource_types = [
        resource_type
        for resource_type in (_clean_text(resource_type) for resource_type in (resource_types or ()))
        if resource_type
    ]
    if cleaned_resource_types:
        where_clauses.append("resource_type = ANY(CAST(:dedupe_resource_types AS varchar[]))")
        query_param_dict["dedupe_resource_types"] = cleaned_resource_types
    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    return _coerce_rowcount(
        await db.status(
            f"""
            DELETE FROM {stage_ref} AS stage_row
             USING (
                    SELECT
                        ctid,
                        row_number() OVER (
                            PARTITION BY source_record_id
                            ORDER BY
                                source_updated_at DESC NULLS LAST,
                                published_at DESC,
                                npi,
                                address_key
                        ) AS duplicate_rank
                      FROM {stage_ref}
                     {where_sql}
                  ) AS ranked
             WHERE stage_row.ctid = ranked.ctid
               AND ranked.duplicate_rank > 1;
            """,
            **query_param_dict,
        )
    )


def _no_scoped_address_overlay_sources_result(schema: str) -> dict[str, Any]:
    return {
        "skipped": True,
        "reason": "no_scoped_sources",
        "source_ids": [],
        "relation": _qt(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE),
    }


def _address_overlay_publish_result(
    stage_metric_dict: dict[str, Any],
    *,
    source_ids: list[str],
    target_ref: str,
) -> dict[str, Any]:
    return {
        "published": True,
        "rows": stage_metric_dict["stage_rows"],
        "inserted": stage_metric_dict["inserted"],
        "inserted_by_component": stage_metric_dict["inserted_by_component"],
        "components": list(stage_metric_dict.get("components") or ADDRESS_OVERLAY_COMPONENTS),
        "countries_normalized": stage_metric_dict.get("countries_normalized", 0),
        "archive_coordinate_backfill_rows": stage_metric_dict.get("archive_coordinate_backfill_rows", 0),
        "duplicates_removed": stage_metric_dict["duplicates_removed"],
        "copied_existing": stage_metric_dict["copied_existing"],
        "source_ids": source_ids,
        "relation": target_ref,
    }


async def _drop_address_overlay_stage_best_effort(stage_ref: str) -> None:
    try:
        await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
    except Exception:  # pragma: no cover - cleanup best effort
        LOGGER.warning(
            "Failed to clean Provider Directory address overlay stage %s",
            stage_ref,
            exc_info=True,
        )


async def publish_provider_directory_address_overlay(
    db_schema: str | None = None,
    *,
    run_id: str | None = None,
    source_ids: list[str] | tuple[str, ...] | None = None,
    components: list[str] | tuple[str, ...] | str | None = None,
) -> dict[str, Any]:
    """Publish Provider Directory address evidence without rebuilding unified addresses."""
    schema = db_schema if db_schema is not None else _schema()
    missing_reason = await _address_overlay_missing_requirement(schema)
    if missing_reason:
        return {"skipped": True, "reason": missing_reason}
    await _ensure_provider_directory_address_overlay_table(schema)
    effective_source_ids = await _address_overlay_scope_sources(
        schema,
        run_id=run_id,
        source_ids=source_ids,
    )
    if run_id is not None and not effective_source_ids:
        return _no_scoped_address_overlay_sources_result(schema)
    stage_table = _address_overlay_stage_table_name(run_id)
    stage_ref = _qt(schema, stage_table)
    target_ref = _qt(schema, PROVIDER_DIRECTORY_ADDRESS_OVERLAY_TABLE)
    query_param_dict: dict[str, Any] = {}
    if run_id is not None:
        query_param_dict["run_id"] = run_id
    if effective_source_ids:
        query_param_dict["source_ids"] = effective_source_ids

    await db.status(f"DROP TABLE IF EXISTS {stage_ref};")
    try:
        await db.status(f"CREATE UNLOGGED TABLE {stage_ref} (LIKE {target_ref} INCLUDING DEFAULTS);")
        stage_metric_dict = await _populate_address_overlay_stage(
            schema,
            stage_table,
            stage_ref,
            run_id,
            effective_source_ids,
            query_param_dict,
            components=_clean_address_overlay_components(components),
        )
        await _swap_address_overlay_stage(schema, stage_table, stage_ref, target_ref)
        return _address_overlay_publish_result(
            stage_metric_dict,
            source_ids=effective_source_ids,
            target_ref=target_ref,
        )
    except Exception:
        await _drop_address_overlay_stage_best_effort(stage_ref)
        raise
