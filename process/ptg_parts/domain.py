# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PTG2 domain constants and value objects."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

PTG2_DOMAIN_IN_NETWORK = "in_network"
PTG2_DOMAIN_ALLOWED_AMOUNT = "allowed_amounts"
PTG2_DOMAIN_DRUG = "drug"

PTG2_STATUS_PENDING = "pending"
PTG2_STATUS_RUNNING = "running"
PTG2_STATUS_BUILDING = "building"
PTG2_STATUS_VALIDATED = "validated"
PTG2_STATUS_PUBLISHED = "published"
PTG2_STATUS_FAILED = "failed"
PTG2_STATUS_DEAD_LETTER = "dead_letter"
PTG2_CANDIDATE_ACTIVATION_CONTRACT = "ptg2_candidate_activation_v1"

PTG2_ARTIFACT_RAW = "raw"
PTG2_ARTIFACT_LOGICAL_JSON = "logical_json"
PTG2_ARTIFACT_SNAPSHOT_INDEX = "snapshot_index"

PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION = "nppes_practice_location"
PTG2_CONFIDENCE_NPPES_MAILING_LOCATION = "nppes_mailing_location"
PTG2_CONFIDENCE_TIC_RATE_NPI_TIN = "tic_rate_npi_tin"
PTG2_CONFIDENCE_PAYER_DIRECTORY = "payer_directory"

PTG2_MODE_EXACT_SOURCE = "exact_source"
PTG2_MODE_PRODUCT_SEARCH = "product_search"

PTG2_STRIPPED_QUERY_PARAMS = {
    "sv",
    "ss",
    "srt",
    "sp",
    "se",
    "st",
    "spr",
    "sig",
    "signature",
    "expires",
    "key-pair-id",
    "policy",
    "awsaccesskeyid",
    "x-amz-algorithm",
    "x-amz-credential",
    "x-amz-date",
    "x-amz-expires",
    "x-amz-security-token",
    "x-amz-signature",
    "x-amz-signedheaders",
}

PTG2_SET_LIKE_KEYS = {
    "npi",
    "npis",
    "service_code",
    "service_codes",
    "billing_code_modifier",
    "billing_code_modifiers",
    "modifiers",
    "provider_references",
    "bundled_codes",
    "covered_codes",
    "related_codes",
    "price_atom_hashes",
    "source_trace_hashes",
    "rate_pack_hashes",
    "chunk_hashes",
}

PTG2_MONEY_KEYS = {
    "allowed_amount",
    "billed_charge",
    "negotiated_rate",
    "rate",
    "price",
    "amount",
}


class PTG2ConfidenceEnum(str, Enum):
    NPPES_PRACTICE_LOCATION = PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION
    NPPES_MAILING_LOCATION = PTG2_CONFIDENCE_NPPES_MAILING_LOCATION
    TIC_RATE_NPI_TIN = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN
    PAYER_DIRECTORY = PTG2_CONFIDENCE_PAYER_DIRECTORY


def normalize_ptg2_search_mode(value: str | None) -> str:
    """Validate and return a PTG2 source or product search mode."""
    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def ptg2_confidence_statement(confidence_code: str) -> str:
    """Return the user-facing evidence caveat for a PTG2 confidence code."""
    statements = {
        PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION: (
            "Provider location is based on the NPPES practice address; TiC rate files do not prove the service is offered there."
        ),
        PTG2_CONFIDENCE_NPPES_MAILING_LOCATION: (
            "Provider location is based on the NPPES mailing address because no better practice location was available."
        ),
        PTG2_CONFIDENCE_TIC_RATE_NPI_TIN: (
            "Published negotiated rate is tied to the source TiC NPI/TIN relationship; it does not prove appointment availability or billing acceptance."
        ),
        PTG2_CONFIDENCE_PAYER_DIRECTORY: (
            "Provider-network confidence can be strengthened with payer directory evidence when available."
        ),
    }
    return statements.get(str(confidence_code), "Confidence is based on the available source evidence for this PTG2 snapshot.")


@dataclass(frozen=True)
class PTG2SourceCatalogEntry:
    source_type: str
    domain: str
    original_url: str
    canonical_url: str
    from_index_url: str | None = None
    description: str | None = None
    reporting_entity_name: str | None = None
    reporting_entity_type: str | None = None
    plan_info: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class PTG2SourceVersion:
    source_identity_hash: str
    source_file_version_id: str
    original_url: str
    canonical_url: str
    raw_storage_uri: str | None = None
    raw_sha256: str | None = None
    logical_sha256: str | None = None
    logical_hash_deferred: bool = False
    content_length: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    verification_mode: str | None = None
    reused_from_source_file_version_id: str | None = None


@dataclass(frozen=True)
class PTG2ContentIdentityValue:
    domain: str
    logical_sha256: str
    semantic_hash: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class PTG2ProviderGroupEvent:
    tin_type: str | None
    tin_value: str | None
    npi: tuple[int, ...]
    provider_group_ref: int | str | None = None
    network_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class PTG2ProcedureEvent:
    billing_code_type: str | None
    billing_code_type_version: str | None
    billing_code: str | None
    name: str | None = None
    description: str | None = None
    bundled_codes: tuple[dict[str, Any], ...] = ()
    covered_codes: tuple[dict[str, Any], ...] = ()


@dataclass(frozen=True)
class PTG2PriceAtomEvent:
    negotiated_type: str | None
    negotiated_rate: Decimal | str | int | None
    expiration_date: str | datetime.date | None = None
    service_code: tuple[str, ...] = ()
    billing_class: str | None = None
    setting: str | None = None
    billing_code_modifier: tuple[str, ...] = ()
    additional_information: str | None = None


@dataclass(frozen=True)
class PTG2ContractEvent:
    domain: str
    procedure: PTG2ProcedureEvent
    provider_group: PTG2ProviderGroupEvent | None = None
    prices: tuple[PTG2PriceAtomEvent, ...] = ()
    source_trace: dict[str, Any] | None = None


@dataclass(frozen=True)
class PTG2ProviderSetValue:
    npi: tuple[int, ...]
    tin_type: str | None = None
    tin_value: str | None = None


@dataclass(frozen=True)
class PTG2PriceSetValue:
    price_atom_hashes: tuple[str, ...]


@dataclass(frozen=True)
class PTG2SourceTraceSetValue:
    source_trace_hashes: tuple[str, ...]


@dataclass(frozen=True)
class PTG2RatePackValue:
    context_hash: str
    domain: str
    procedure_hash: str
    provider_set_hash: str
    price_set_hash: str
    source_trace_set_hash: str


@dataclass(frozen=True)
class PTG2HeadMetadata:
    url: str
    status: int | None = None
    etag: str | None = None
    content_length: int | None = None
    last_modified: str | None = None
    content_encoding: str | None = None
    content_type: str | None = None
    supports_head: bool = False


@dataclass(frozen=True)
class PTG2RawArtifact:
    original_url: str
    canonical_url: str
    raw_path: str
    raw_storage_uri: str
    raw_sha256: str
    byte_count: int
    head: PTG2HeadMetadata | None = None
    reused: bool = False
    verification_mode: str = "downloaded"
    reused_from_source_file_version_id: str | None = None


@dataclass(frozen=True)
class PTG2LogicalArtifact:
    logical_path: str
    logical_sha256: str
    byte_count: int
    compression: str | None = None
    member_name: str | None = None
    logical_hash_deferred: bool = False


@dataclass(frozen=True)
class PTG2FileProcessResult:
    source_type: str
    url: str
    success: bool
    file_id: int | None = None
    error: str | None = None
    summary: dict[str, Any] | None = None
    skipped: bool = False


@dataclass(frozen=True)
class PTG2DownloadedJob:
    job: dict[str, Any]
    raw_artifact: PTG2RawArtifact | None = None
    logical_artifact: PTG2LogicalArtifact | None = None
    error: str | None = None
