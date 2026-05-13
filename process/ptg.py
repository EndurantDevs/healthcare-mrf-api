# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
# pylint: disable=broad-exception-caught,too-many-branches,too-many-locals,too-many-statements

import datetime
import gzip
import hashlib
import io
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import threading
import time
import zipfile
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import asdict, dataclass, is_dataclass
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

import aiohttp
import ijson
from ijson.common import ObjectBuilder
from dateutil.parser import parse as parse_date
import asyncio
import codecs
import concurrent.futures
import multiprocessing
import pickle
import re

try:
    import orjson
except ImportError:  # pragma: no cover - optional acceleration
    orjson = None

try:
    import isal.igzip as igzip
except ImportError:  # pragma: no cover - optional acceleration
    igzip = None

from db.connection import db
from db.models import (
    ImportLog,
    PTGAllowedItem,
    PTGAllowedPayment,
    PTGAllowedProviderPayment,
    PTGBillingCode,
    PTG2ArtifactManifest,
    PTG2Capability,
    PTG2Confidence,
    PTG2ContentIdentity,
    PTG2CurrentSnapshot,
    PTG2FactChunk,
    PTG2GCCandidate,
    PTG2ImportJob,
    PTG2ImportRun,
    PTG2LocationSet,
    PTG2LocationSetMember,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
    PTG2PlanRateSet,
    PTG2PriceAtom,
    PTG2PriceSet,
    PTG2Procedure,
    PTG2ProviderGroup,
    PTG2ProviderGroupMember,
    PTG2ProviderLocation,
    PTG2ProviderSet,
    PTG2ProviderSetComponent,
    PTG2ProviderSetMember,
    PTG2RatePack,
    PTG2RateSet,
    PTG2RateSetContext,
    PTG2RelatedCodeSet,
    PTG2ServingRate,
    PTG2ServingRateCompact,
    PTG2Snapshot,
    PTG2SourceCatalog,
    PTG2SourceFileVersion,
    PTG2SourceIdentity,
    PTG2SourceTrace,
    PTG2SourceTraceSet,
    PTGFile,
    PTGInNetworkItem,
    PTGNegotiatedPrice,
    PTGNegotiatedRate,
    PTGProviderGroup,
)
from process.ext.utils import (
    ensure_database,
    flush_error_log,
    get_import_schema,
    log_error,
    make_class,
    push_objects,
    return_checksum,
)

logger = logging.getLogger(__name__)

TEST_TOC_FILES = 2
TEST_TOC_JOBS = 2
TEST_PROVIDER_GROUPS = 50
TEST_IN_NETWORK_ITEMS = 25
TEST_NEGOTIATED_PRICES = 250
TEST_ALLOWED_ITEMS = 25

PTG2_KEEP_PARTIAL_ENV = "HLTHPRT_PTG2_KEEP_PARTIAL_ARTIFACTS"
PTG2_DEFER_LOGICAL_HASH_BYTES_ENV = "HLTHPRT_PTG2_DEFER_LOGICAL_HASH_BYTES"
PTG2_PROVIDER_REF_BATCH_ROWS_ENV = "HLTHPRT_PTG2_PROVIDER_REF_BATCH_ROWS"
PTG2_ITEM_BATCH_ROWS_ENV = "HLTHPRT_PTG2_ITEM_BATCH_ROWS"
PTG2_BILLING_BATCH_ROWS_ENV = "HLTHPRT_PTG2_BILLING_BATCH_ROWS"
PTG2_RATE_BATCH_ROWS_ENV = "HLTHPRT_PTG2_RATE_BATCH_ROWS"
PTG2_PRICE_BATCH_ROWS_ENV = "HLTHPRT_PTG2_PRICE_BATCH_ROWS"
PTG2_PROGRESS_INTERVAL_SECONDS_ENV = "HLTHPRT_PTG2_PROGRESS_INTERVAL_SECONDS"
PTG2_COMPACT_IMPORT_ENV = "HLTHPRT_PTG2_COMPACT_IMPORT"
PTG2_COMPACT_BATCH_ROWS_ENV = "HLTHPRT_PTG2_COMPACT_BATCH_ROWS"
PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV = "HLTHPRT_PTG2_PROVIDER_SET_INLINE_NPI_LIMIT"
PTG2_PROVIDER_BUCKET_COUNT_ENV = "HLTHPRT_PTG2_PROVIDER_BUCKET_COUNT"
PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV = "HLTHPRT_PTG2_PROVIDER_CACHE_MEMORY_REFS"
PTG2_RATE_GROUP_FLUSH_ITEMS_ENV = "HLTHPRT_PTG2_RATE_GROUP_FLUSH_ITEMS"
PTG2_PROVIDER_COMBO_CACHE_REFS_ENV = "HLTHPRT_PTG2_PROVIDER_COMBO_CACHE_REFS"
PTG2_SERVING_ROW_LIMIT_ENV = "HLTHPRT_PTG2_SERVING_ROW_LIMIT"
PTG2_COPY_UPSERT_ROWS_ENV = "HLTHPRT_PTG2_COPY_UPSERT_ROWS"
PTG2_DIRECT_COPY_SERVING_RATE_ENV = "HLTHPRT_PTG2_DIRECT_COPY_SERVING_RATE"
PTG2_STREAMING_DEDUPE_ENV = "HLTHPRT_PTG2_STREAMING_DEDUPE"
PTG2_STAGE_PRICE_SETS_ENV = "HLTHPRT_PTG2_STAGE_PRICE_SETS"
PTG2_SKIP_EXISTING_PRICE_SETS_ENV = "HLTHPRT_PTG2_SKIP_EXISTING_PRICE_SETS"
PTG2_KEEP_PRICE_SET_STAGE_ENV = "HLTHPRT_PTG2_KEEP_PRICE_SET_STAGE"
PTG2_STAGE_SERVING_RATES_ENV = "HLTHPRT_PTG2_STAGE_SERVING_RATES"
PTG2_KEEP_SERVING_RATE_STAGE_ENV = "HLTHPRT_PTG2_KEEP_SERVING_RATE_STAGE"
PTG2_STAGE_SERVING_AS_FINAL_ENV = "HLTHPRT_PTG2_STAGE_SERVING_AS_FINAL"
PTG2_COMPACT_SERVING_TABLE_ENV = "HLTHPRT_PTG2_COMPACT_SERVING_TABLE"
PTG2_COMPACT_BULK_DROP_INDEXES_ENV = "HLTHPRT_PTG2_COMPACT_BULK_DROP_INDEXES"
PTG2_SKIP_BULK_INDEX_ENSURE_ENV = "HLTHPRT_PTG2_SKIP_BULK_INDEX_ENSURE"
PTG2_SKIP_COMPACT_FINALIZE_ENV = "HLTHPRT_PTG2_SKIP_COMPACT_FINALIZE"
PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV = "HLTHPRT_PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE"
PTG2_FAST_FINAL_REBUILD_ENV = "HLTHPRT_PTG2_FAST_FINAL_REBUILD"
PTG2_DEFER_PROVIDER_LOCATIONS_ENV = "HLTHPRT_PTG2_DEFER_PROVIDER_LOCATIONS"
PTG2_STAGE_INDEXES_ENV = "HLTHPRT_PTG2_STAGE_INDEXES"
PTG2_UNLOGGED_STAGE_ENV = "HLTHPRT_PTG2_UNLOGGED_STAGE"
PTG2_UNLOGGED_FINAL_ENV = "HLTHPRT_PTG2_UNLOGGED_FINAL"
PTG2_DEDUPE_SERVING_STAGE_MERGE_ENV = "HLTHPRT_PTG2_DEDUPE_SERVING_STAGE_MERGE"
PTG2_PROVIDER_CACHE_BACKEND_ENV = "HLTHPRT_PTG2_PROVIDER_CACHE_BACKEND"
PTG2_STREAM_BUFFER_BYTES_ENV = "HLTHPRT_PTG2_STREAM_BUFFER_BYTES"
PTG2_ASYNC_WRITE_TASKS_ENV = "HLTHPRT_PTG2_ASYNC_WRITE_TASKS"
PTG2_FAST_PROVIDER_UNION_ENV = "HLTHPRT_PTG2_FAST_PROVIDER_UNION"
PTG2_FAST_PROVIDER_AGGREGATION_ENV = "HLTHPRT_PTG2_FAST_PROVIDER_AGGREGATION"
PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV = "HLTHPRT_PTG2_EXPECTED_IN_NETWORK_ITEMS"
PTG2_FAST_OBJECT_ITERATOR_ENV = "HLTHPRT_PTG2_FAST_OBJECT_ITERATOR"
PTG2_JSON_DECODER_ITERATOR_ENV = "HLTHPRT_PTG2_JSON_DECODER_ITERATOR"
PTG2_FAST_JSON_LOADS_ENV = "HLTHPRT_PTG2_FAST_JSON_LOADS"
PTG2_ISAL_GZIP_ENV = "HLTHPRT_PTG2_ISAL_GZIP"
PTG2_SERVING_ONLY_IMPORT_ENV = "HLTHPRT_PTG2_SERVING_ONLY_IMPORT"
PTG2_SERVING_WORKERS_ENV = "HLTHPRT_PTG2_SERVING_WORKERS"
PTG2_SLIM_SERVING_ROWS_ENV = "HLTHPRT_PTG2_SLIM_SERVING_ROWS"
PTG2_RAW_WORKER_OBJECTS_ENV = "HLTHPRT_PTG2_RAW_WORKER_OBJECTS"
PTG2_WORKER_CHUNK_ITEMS_ENV = "HLTHPRT_PTG2_WORKER_CHUNK_ITEMS"
PTG2_WORKER_CHUNK_BYTES_ENV = "HLTHPRT_PTG2_WORKER_CHUNK_BYTES"
PTG2_WORKER_MAX_PENDING_BATCHES_ENV = "HLTHPRT_PTG2_WORKER_MAX_PENDING_BATCHES"
PTG2_WORKER_MAX_PENDING_BYTES_ENV = "HLTHPRT_PTG2_WORKER_MAX_PENDING_BYTES"
PTG2_WORKER_RESULT_FILES_ENV = "HLTHPRT_PTG2_WORKER_RESULT_FILES"
PTG2_RUST_SCANNER_ENV = "HLTHPRT_PTG2_RUST_SCANNER"
PTG2_RUST_SCANNER_BIN_ENV = "HLTHPRT_PTG2_RUST_SCANNER_BIN"
PTG2_RUST_COMPACT_SERVING_ENV = "HLTHPRT_PTG2_RUST_COMPACT_SERVING"
PTG2_RUST_SPLIT_IN_NETWORK_ENV = "HLTHPRT_PTG2_RUST_SPLIT_IN_NETWORK"
PTG2_RUST_COPY_UNLOGGED_STAGE_ENV = "HLTHPRT_PTG2_RUST_COPY_UNLOGGED_STAGE"
PTG2_HASH_MODE_ENV = "HLTHPRT_PTG2_HASH_MODE"


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(str(raw).strip())
    except ValueError:
        return default


def _use_stage_serving_as_final() -> bool:
    return _env_bool(
        PTG2_STAGE_SERVING_AS_FINAL_ENV,
        _env_bool(PTG2_SERVING_ONLY_IMPORT_ENV, False),
    )


def _use_compact_serving_table() -> bool:
    return _env_bool(
        PTG2_COMPACT_SERVING_TABLE_ENV,
        _env_bool(PTG2_SERVING_ONLY_IMPORT_ENV, False),
    )


def _stream_buffer_bytes() -> int:
    return max(_env_int(PTG2_STREAM_BUFFER_BYTES_ENV, 4 * 1024 * 1024), 64 * 1024)


def _json_loads(value: str | bytes | bytearray) -> Any:
    if orjson is not None and _env_bool(PTG2_FAST_JSON_LOADS_ENV, True):
        return orjson.loads(value)
    return json.loads(value)


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

PTG2_MODEL_CLASSES = (
    PTG2ImportRun,
    PTG2Snapshot,
    PTG2CurrentSnapshot,
    PTG2SourceCatalog,
    PTG2SourceIdentity,
    PTG2SourceFileVersion,
    PTG2ContentIdentity,
    PTG2ImportJob,
    PTG2ArtifactManifest,
    PTG2Plan,
    PTG2PlanAlias,
    PTG2PlanMonth,
    PTG2PlanRateSet,
    PTG2RateSetContext,
    PTG2RateSet,
    PTG2FactChunk,
    PTG2RatePack,
    PTG2ProviderGroup,
    PTG2ProviderGroupMember,
    PTG2ProviderSet,
    PTG2ProviderSetComponent,
    PTG2ProviderSetMember,
    PTG2ProviderLocation,
    PTG2ServingRateCompact,
    PTG2ServingRate,
    PTG2LocationSet,
    PTG2LocationSetMember,
    PTG2Procedure,
    PTG2RelatedCodeSet,
    PTG2PriceAtom,
    PTG2PriceSet,
    PTG2SourceTrace,
    PTG2SourceTraceSet,
    PTG2Confidence,
    PTG2Capability,
    PTG2GCCandidate,
)


class PTG2ConfidenceEnum(str, Enum):
    NPPES_PRACTICE_LOCATION = PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION
    NPPES_MAILING_LOCATION = PTG2_CONFIDENCE_NPPES_MAILING_LOCATION
    TIC_RATE_NPI_TIN = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN
    PAYER_DIRECTORY = PTG2_CONFIDENCE_PAYER_DIRECTORY


def normalize_ptg2_search_mode(value: str | None) -> str:
    mode = str(value or PTG2_MODE_PRODUCT_SEARCH).strip().lower()
    if mode not in {PTG2_MODE_EXACT_SOURCE, PTG2_MODE_PRODUCT_SEARCH}:
        raise ValueError("mode must be exact_source or product_search")
    return mode


def ptg2_confidence_statement(confidence_code: str) -> str:
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


@dataclass(frozen=True)
class PTG2FileProcessResult:
    source_type: str
    url: str
    success: bool
    file_id: int | None = None
    error: str | None = None


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def hash_prefix(value: str, length: int = 16) -> str:
    return str(value)[:length]


def normalize_money(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, float):
        raise TypeError("Float money values are not accepted; use Decimal, int, or string")
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, int):
        decimal_value = Decimal(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            decimal_value = Decimal(text)
        except InvalidOperation as exc:
            raise ValueError(f"Invalid money value: {value!r}") from exc
    else:
        raise TypeError(f"Unsupported money value type: {type(value).__name__}")
    if not decimal_value.is_finite():
        raise ValueError("Money value must be finite")
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal(1)), "f")
    return format(normalized, "f")


def money_number(value: Any) -> int | float | None:
    try:
        normalized = normalize_money(value)
    except (TypeError, ValueError, InvalidOperation):
        return None
    if normalized is None:
        return None
    if "." in normalized:
        return float(normalized)
    return int(normalized)


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        candidate = text[:10]
        try:
            return datetime.date.fromisoformat(candidate).isoformat()
        except ValueError:
            pass
    try:
        parsed = parse_date(text)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid date value: {value!r}") from exc
    if isinstance(parsed, datetime.datetime):
        return parsed.date().isoformat()
    return parsed.isoformat()


def _canonical_key(key: Any) -> str:
    return str(key or "").strip().lower()


def _canonical_sort_key(value: Any) -> str:
    normalized = _canonicalize_for_json(value)
    return json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _canonicalize_for_json(value: Any, key: str | None = None) -> Any:
    key_name = _canonical_key(key)
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Decimal):
        return normalize_money(value)
    if isinstance(value, float) and key_name in PTG2_MONEY_KEYS:
        return normalize_money(value)
    if isinstance(value, (datetime.date, datetime.datetime)):
        return normalize_date(value)
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, list):
        normalized_list = [_canonicalize_for_json(item, key=key) for item in value]
        if key_name in PTG2_SET_LIKE_KEYS:
            return sorted(normalized_list, key=_canonical_sort_key)
        return normalized_list
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for child_key in sorted(value.keys(), key=lambda item: str(item)):
            child_value = value[child_key]
            child_key_text = str(child_key)
            child_key_name = _canonical_key(child_key_text)
            if child_key_name in PTG2_MONEY_KEYS:
                out[child_key_text] = normalize_money(child_value)
            elif child_key_name.endswith("_date") or child_key_name in {"expiration_date", "last_updated_on"}:
                out[child_key_text] = normalize_date(child_value)
            else:
                out[child_key_text] = _canonicalize_for_json(child_value, key=child_key_text)
        return out
    return value


def canonical_json_dumps(value: Any) -> str:
    """
    Stable JSON representation for PTG2 semantic hashes. None is emitted as
    explicit JSON null; dict keys are sorted; known set-like arrays are sorted.
    """
    return json.dumps(
        _canonicalize_for_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def semantic_hash(value: Any, domain: str | None = None) -> str:
    payload = {"domain": domain, "payload": _canonicalize_for_json(value)} if domain else _canonicalize_for_json(value)
    payload_bytes = canonical_json_dumps(payload).encode("utf-8")
    mode = str(os.getenv(PTG2_HASH_MODE_ENV, "checksum64")).strip().lower()
    if mode == "sha256":
        return sha256_bytes(payload_bytes)
    if mode in {"blake2", "blake2b", "blake2_128"}:
        return hashlib.blake2b(payload_bytes, digest_size=16).hexdigest()
    checksum = int.from_bytes(hashlib.sha256(payload_bytes).digest()[:8], byteorder="big", signed=False) & (
        (1 << 63) - 1
    )
    return f"{checksum:016x}"


def canonicalize_url(url: str) -> str:
    parsed = urlsplit(str(url).strip())
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    if (scheme == "https" and netloc.endswith(":443")) or (scheme == "http" and netloc.endswith(":80")):
        netloc = netloc.rsplit(":", 1)[0]
    query_pairs = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() in PTG2_STRIPPED_QUERY_PARAMS:
            continue
        query_pairs.append((key, value))
    query = urlencode(sorted(query_pairs), doseq=True)
    return urlunsplit((scheme, netloc, parsed.path or "/", query, ""))


def normalize_import_month(value: str | datetime.date | None) -> datetime.date:
    if value is None:
        today = datetime.date.today()
        return datetime.date(today.year, today.month, 1)
    if isinstance(value, datetime.datetime):
        return datetime.date(value.year, value.month, 1)
    if isinstance(value, datetime.date):
        return datetime.date(value.year, value.month, 1)
    normalized = normalize_date(value)
    if normalized is None:
        raise ValueError("import month cannot be blank")
    parsed = datetime.date.fromisoformat(normalized)
    return datetime.date(parsed.year, parsed.month, 1)


def _normalize_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(plan or {})
    if "plan_sponsor_name" not in normalized and normalized.get("plan_sponser_name"):
        normalized["plan_sponsor_name"] = normalized.get("plan_sponser_name")
    return normalized


def parse_toc_catalog_entries(
    toc_content: dict[str, Any],
    toc_url: str,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
) -> list[PTG2SourceCatalogEntry]:
    toc_meta = {
        "reporting_entity_name": toc_content.get("reporting_entity_name"),
        "reporting_entity_type": toc_content.get("reporting_entity_type"),
        "last_updated_on": toc_content.get("last_updated_on"),
        "version": toc_content.get("version"),
    }
    entries = [
        PTG2SourceCatalogEntry(
            source_type="table-of-contents",
            domain="catalog",
            original_url=toc_url,
            canonical_url=canonicalize_url(toc_url),
            description=toc_content.get("description"),
            reporting_entity_name=toc_meta["reporting_entity_name"],
            reporting_entity_type=toc_meta["reporting_entity_type"],
            plan_info=(),
        )
    ]
    for structure in toc_content.get("reporting_structure", []) or []:
        plans = [_normalize_plan_payload(plan) for plan in (structure.get("reporting_plans") or [])]
        plans = _filter_reporting_plans(
            plans,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        plan_tuple = tuple(plans)
        for file_entry in structure.get("in_network_files") or []:
            location = file_entry.get("location")
            if location:
                entries.append(
                    PTG2SourceCatalogEntry(
                        source_type="in-network",
                        domain=PTG2_DOMAIN_IN_NETWORK,
                        original_url=location,
                        canonical_url=canonicalize_url(location),
                        from_index_url=toc_url,
                        description=file_entry.get("description"),
                        reporting_entity_name=toc_meta["reporting_entity_name"],
                        reporting_entity_type=toc_meta["reporting_entity_type"],
                        plan_info=plan_tuple,
                    )
                )
        allowed_amount_file = structure.get("allowed_amount_file") or {}
        if allowed_amount_file.get("location"):
            entries.append(
                PTG2SourceCatalogEntry(
                    source_type="allowed-amounts",
                    domain=PTG2_DOMAIN_ALLOWED_AMOUNT,
                    original_url=allowed_amount_file["location"],
                    canonical_url=canonicalize_url(allowed_amount_file["location"]),
                    from_index_url=toc_url,
                    description=allowed_amount_file.get("description"),
                    reporting_entity_name=toc_meta["reporting_entity_name"],
                    reporting_entity_type=toc_meta["reporting_entity_type"],
                    plan_info=plan_tuple,
                )
            )
        for drug_key in (
            "drug_file",
            "drug_files",
            "ndc_file",
            "ndc_files",
            "prescription_drug_file",
            "prescription_drug_files",
            "payer_specific_drug_files",
        ):
            drug_entries = _as_list(structure.get(drug_key))
            for drug_entry in drug_entries:
                if not isinstance(drug_entry, dict):
                    continue
                location = drug_entry.get("location")
                if location:
                    entries.append(
                        PTG2SourceCatalogEntry(
                            source_type="payer-drug",
                            domain=PTG2_DOMAIN_DRUG,
                            original_url=location,
                            canonical_url=canonicalize_url(location),
                            from_index_url=toc_url,
                            description=drug_entry.get("description"),
                            reporting_entity_name=toc_meta["reporting_entity_name"],
                            reporting_entity_type=toc_meta["reporting_entity_type"],
                            plan_info=plan_tuple,
                        )
                    )
    deduped: dict[tuple[str, str, str], PTG2SourceCatalogEntry] = {}
    for entry in entries:
        deduped[(entry.source_type, entry.domain, entry.canonical_url)] = entry
    return list(deduped.values())


def _source_identity_hash(source_type: str, canonical_url: str) -> str:
    return semantic_hash({"source_type": source_type, "canonical_url": canonical_url}, domain="source_identity")


def _catalog_entry_id(entry: PTG2SourceCatalogEntry) -> str:
    return semantic_hash(entry, domain="source_catalog")


def build_provider_set(npi: list[int] | tuple[int, ...], tin_type: str | None = None, tin_value: str | None = None) -> dict[str, Any]:
    normalized_npi = tuple(sorted({int(value) for value in npi if value is not None}))
    payload = PTG2ProviderSetValue(npi=normalized_npi, tin_type=tin_type, tin_value=tin_value)
    provider_set_hash = semantic_hash(payload, domain="provider_set")
    return {
        "provider_set_hash": provider_set_hash,
        "hash_prefix": hash_prefix(provider_set_hash),
        "provider_count": len(normalized_npi),
        "npi": list(normalized_npi),
        "tin_type": tin_type,
        "tin_value": tin_value,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def provider_hash_bucket(provider_set_hash: str, bucket_count: int = 256) -> str:
    if bucket_count <= 0:
        raise ValueError("bucket_count must be positive")
    bucket = int(str(provider_set_hash)[:8], 16) % bucket_count
    width = max(2, len(format(bucket_count - 1, "x")))
    return format(bucket, f"0{width}x")


def ptg2_provider_bucket_count() -> int:
    return max(_env_int(PTG2_PROVIDER_BUCKET_COUNT_ENV, 64), 1)


def build_price_atom(price: PTG2PriceAtomEvent | dict[str, Any]) -> dict[str, Any]:
    payload = _canonicalize_for_json(price)
    price_atom_hash = semantic_hash(payload, domain="price_atom")
    return {
        "price_atom_hash": price_atom_hash,
        "hash_prefix": hash_prefix(price_atom_hash),
        "canonical_payload": payload,
    }


def build_price_set(price_atom_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in price_atom_hashes if value}))
    payload = PTG2PriceSetValue(price_atom_hashes=normalized_hashes)
    price_set_hash = semantic_hash(payload, domain="price_set")
    return {
        "price_set_hash": price_set_hash,
        "hash_prefix": hash_prefix(price_set_hash),
        "price_atom_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_source_trace_set(source_trace_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in source_trace_hashes if value}))
    payload = PTG2SourceTraceSetValue(source_trace_hashes=normalized_hashes)
    source_trace_set_hash = semantic_hash(payload, domain="source_trace_set")
    return {
        "source_trace_set_hash": source_trace_set_hash,
        "hash_prefix": hash_prefix(source_trace_set_hash),
        "source_trace_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_pack(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_set_hash: str,
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    payload = PTG2RatePackValue(
        context_hash=context_hash,
        domain=domain,
        procedure_hash=procedure_hash,
        provider_set_hash=provider_set_hash,
        price_set_hash=price_set_hash,
        source_trace_set_hash=source_trace_set_hash,
    )
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_set_hash,
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_provider_set_collection(provider_set_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in provider_set_hashes if value}))
    payload = {"provider_set_hashes": normalized_hashes}
    collection_hash = semantic_hash(payload, domain="provider_set_collection")
    return {
        "provider_set_collection_hash": collection_hash,
        "hash_prefix": hash_prefix(collection_hash),
        "provider_set_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_procedure_collection(procedure_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in procedure_hashes if value}))
    payload = {"procedure_hashes": normalized_hashes}
    collection_hash = semantic_hash(payload, domain="procedure_collection")
    return {
        "procedure_collection_hash": collection_hash,
        "hash_prefix": hash_prefix(collection_hash),
        "procedure_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_pack_group(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_set_hashes: list[str] | tuple[str, ...],
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    provider_collection = build_provider_set_collection(provider_set_hashes)
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "provider_set_hashes": tuple(provider_collection["provider_set_hashes"]),
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
    }
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_pack_procedure_group(
    context_hash: str,
    domain: str,
    procedure_hashes: list[str] | tuple[str, ...],
    provider_set_hashes: list[str] | tuple[str, ...],
    price_set_hash: str,
    source_trace_set_hash: str,
) -> dict[str, Any]:
    provider_collection = build_provider_set_collection(provider_set_hashes)
    procedure_collection = build_procedure_collection(procedure_hashes)
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_collection["procedure_collection_hash"],
        "procedure_hashes": tuple(procedure_collection["procedure_hashes"]),
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "provider_set_hashes": tuple(provider_collection["provider_set_hashes"]),
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
    }
    rate_pack_hash = semantic_hash(payload, domain="rate_pack")
    return {
        "rate_pack_hash": rate_pack_hash,
        "hash_prefix": hash_prefix(rate_pack_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_collection["procedure_collection_hash"],
        "provider_set_hash": provider_collection["provider_set_collection_hash"],
        "price_set_hash": price_set_hash,
        "source_trace_set_hash": source_trace_set_hash,
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_fact_chunk(
    context_hash: str,
    domain: str,
    procedure_hash: str,
    provider_bucket: str,
    rate_pack_hashes: list[str] | tuple[str, ...],
) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in rate_pack_hashes if value}))
    payload = {
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_bucket": provider_bucket,
        "rate_pack_hashes": normalized_hashes,
    }
    fact_chunk_hash = semantic_hash(payload, domain="fact_chunk")
    return {
        "fact_chunk_hash": fact_chunk_hash,
        "hash_prefix": hash_prefix(fact_chunk_hash),
        "context_hash": context_hash,
        "domain": domain,
        "procedure_hash": procedure_hash,
        "provider_bucket": provider_bucket,
        "rate_pack_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def build_rate_set(context_hash: str, chunk_hashes: list[str] | tuple[str, ...]) -> dict[str, Any]:
    normalized_hashes = tuple(sorted({str(value) for value in chunk_hashes if value}))
    payload = {"context_hash": context_hash, "chunk_hashes": normalized_hashes}
    rate_set_hash = semantic_hash(payload, domain="rate_set")
    return {
        "rate_set_hash": rate_set_hash,
        "hash_prefix": hash_prefix(rate_set_hash),
        "context_hash": context_hash,
        "chunk_hashes": list(normalized_hashes),
        "canonical_payload": _canonicalize_for_json(payload),
    }


def resolve_ptg2_artifact_dir() -> Path:
    configured = os.getenv("HLTHPRT_PTG2_ARTIFACT_DIR")
    root = Path(configured) if configured else Path(tempfile.gettempdir()) / "healthporta-ptg2-artifacts"
    root.mkdir(parents=True, exist_ok=True)
    return root


def content_addressed_path(root: str | Path, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
    digest_text = str(digest)
    clean_suffix = suffix if suffix.startswith(".") or not suffix else f".{suffix}"
    return Path(root) / kind / digest_text[:2] / digest_text[2:4] / f"{digest_text}{clean_suffix}"


def ptg2_temp_parent() -> Path:
    return PTG2ArtifactStore().tmp_dir


def _safe_url_suffix(url: str) -> str:
    path = urlsplit(url).path
    suffixes = "".join(Path(path).suffixes[-2:])
    return suffixes[:24]


def sha256_file(path: str | Path, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with open(path, "rb") as fp:
        for chunk in iter(lambda: fp.read(chunk_size), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


class PTG2ArtifactStore:
    def __init__(self, root: str | Path | None = None):
        self.root = Path(root) if root else resolve_ptg2_artifact_dir()
        self.root.mkdir(parents=True, exist_ok=True)
        self.tmp_dir = self.root / "tmp"
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.root / "manifest.jsonl"

    def artifact_path(self, digest: str, kind: str = PTG2_ARTIFACT_RAW, suffix: str = "") -> Path:
        return content_addressed_path(self.root, digest, kind=kind, suffix=suffix)

    def storage_uri(self, path: str | Path) -> str:
        return Path(path).resolve().as_uri()

    def path_from_uri(self, uri: str) -> Path:
        if uri.startswith("file://"):
            return Path(unquote(urlsplit(uri).path))
        return Path(uri)

    def find_candidates(self, canonical_url: str) -> list[dict[str, Any]]:
        if not self.manifest_path.exists():
            return []
        candidates: list[dict[str, Any]] = []
        with open(self.manifest_path, "r", encoding="utf-8") as fp:
            for line in fp:
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("canonical_url") == canonical_url and payload.get("artifact_kind") == PTG2_ARTIFACT_RAW:
                    candidates.append(payload)
        return candidates

    def record_manifest(self, payload: dict[str, Any]) -> None:
        payload = dict(payload)
        payload.setdefault("recorded_at", _utcnow().isoformat())
        with open(self.manifest_path, "a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, sort_keys=True, default=str) + "\n")


def _is_strong_etag(etag: str | None) -> bool:
    if not etag:
        return False
    return not etag.strip().lower().startswith("w/")


def choose_reusable_raw_artifact(
    candidates: list[dict[str, Any]],
    head: PTG2HeadMetadata | None,
    store: PTG2ArtifactStore | None = None,
    reuse_policy: str = "metadata_or_hash",
) -> tuple[dict[str, Any] | None, str | None]:
    if not candidates:
        return None, None
    if head is not None:
        for candidate in reversed(candidates):
            same_length = (
                head.content_length is not None
                and candidate.get("content_length") is not None
                and int(candidate["content_length"]) == int(head.content_length)
            )
            if same_length and _is_strong_etag(head.etag) and candidate.get("etag") == head.etag:
                return candidate, "strong_etag_length"
        if reuse_policy in {"metadata", "metadata_or_hash"}:
            for candidate in reversed(candidates):
                same_length = (
                    head.content_length is not None
                    and candidate.get("content_length") is not None
                    and int(candidate["content_length"]) == int(head.content_length)
                )
                same_modified = bool(head.last_modified and candidate.get("last_modified") == head.last_modified)
                if same_length and same_modified:
                    return candidate, "length_last_modified"
    if store is not None and reuse_policy in {"hash", "metadata_or_hash"}:
        for candidate in reversed(candidates):
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            expected = candidate.get("raw_sha256") or candidate.get("sha256")
            if not raw_uri or not expected:
                continue
            raw_path = store.path_from_uri(raw_uri)
            if not raw_path.exists():
                continue
            actual, _ = sha256_file(raw_path)
            if actual == expected:
                return candidate, "verified_local_sha256"
    return None, None


async def fetch_head_metadata(url: str, timeout_seconds: int = 30) -> PTG2HeadMetadata:
    if not str(url).lower().startswith(("http://", "https://")):
        path = Path(urlsplit(url).path if str(url).startswith("file://") else url)
        if path.exists():
            stat = path.stat()
            return PTG2HeadMetadata(
                url=url,
                status=200,
                content_length=stat.st_size,
                last_modified=datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                supports_head=True,
            )
        return PTG2HeadMetadata(url=url, supports_head=False)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=min(timeout_seconds, 10))
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(url, allow_redirects=True) as response:
                headers = response.headers
                length = headers.get("Content-Length")
                return PTG2HeadMetadata(
                    url=str(response.url),
                    status=response.status,
                    etag=headers.get("ETag"),
                    content_length=int(length) if length and length.isdigit() else None,
                    last_modified=headers.get("Last-Modified"),
                    content_encoding=headers.get("Content-Encoding"),
                    content_type=headers.get("Content-Type"),
                    supports_head=response.status < 400,
                )
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return PTG2HeadMetadata(url=url, supports_head=False)


async def download_raw_artifact(
    url: str,
    store: PTG2ArtifactStore | None = None,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
) -> PTG2RawArtifact:
    store = store or PTG2ArtifactStore()
    keep_partials = _env_bool(PTG2_KEEP_PARTIAL_ENV, True) if keep_partial_artifacts is None else keep_partial_artifacts
    canonical_url = canonicalize_url(url)
    head = await fetch_head_metadata(url)
    if reuse_raw_artifacts:
        candidate, mode = choose_reusable_raw_artifact(store.find_candidates(canonical_url), head, store=store)
        if candidate is not None and mode is not None:
            raw_uri = candidate.get("raw_storage_uri") or candidate.get("storage_uri")
            raw_path = store.path_from_uri(raw_uri)
            expected = candidate.get("raw_sha256") or candidate.get("sha256")
            actual, byte_count = sha256_file(raw_path)
            if expected and actual != expected:
                store.record_manifest(
                    {
                        "artifact_kind": PTG2_ARTIFACT_RAW,
                        "canonical_url": canonical_url,
                        "raw_storage_uri": raw_uri,
                        "raw_sha256": expected,
                        "status": "corrupt",
                        "actual_sha256": actual,
                    }
                )
            else:
                return PTG2RawArtifact(
                    original_url=url,
                    canonical_url=canonical_url,
                    raw_path=str(raw_path),
                    raw_storage_uri=raw_uri,
                    raw_sha256=actual,
                    byte_count=byte_count,
                    head=head,
                    reused=True,
                    verification_mode=mode,
                    reused_from_source_file_version_id=candidate.get("source_file_version_id"),
                )

    tmp_path = store.tmp_dir / f"ptg2-{os.getpid()}-{datetime.datetime.utcnow().timestamp()}.part"
    digest = hashlib.sha256()
    byte_count = 0
    try:
        if str(url).startswith("file://") or (not str(url).lower().startswith(("http://", "https://")) and Path(url).exists()):
            source_path = Path(urlsplit(url).path if str(url).startswith("file://") else url)
            with open(source_path, "rb") as src, open(tmp_path, "wb") as dst:
                for chunk in iter(lambda: src.read(1024 * 1024), b""):
                    byte_count += len(chunk)
                    if max_bytes is not None and byte_count > max_bytes:
                        raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
                    digest.update(chunk)
                    dst.write(chunk)
        else:
            timeout = aiohttp.ClientTimeout(total=None, connect=60, sock_read=600)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, allow_redirects=True) as response:
                    response.raise_for_status()
                    with open(tmp_path, "wb") as dst:
                        async for chunk in response.content.iter_chunked(1024 * 1024):
                            byte_count += len(chunk)
                            if max_bytes is not None and byte_count > max_bytes:
                                raise RuntimeError(f"PTG2 max-bytes guard exceeded for {url}")
                            digest.update(chunk)
                            dst.write(chunk)
        raw_sha = digest.hexdigest()
        final_path = store.artifact_path(raw_sha, kind=PTG2_ARTIFACT_RAW, suffix=_safe_url_suffix(url))
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if final_path.exists():
            tmp_path.unlink(missing_ok=True)
        else:
            os.replace(tmp_path, final_path)
        actual_sha, actual_size = sha256_file(final_path)
        if actual_sha != raw_sha:
            raise RuntimeError(f"Checksum verification failed for {final_path}")
        raw_uri = store.storage_uri(final_path)
        manifest_payload = {
            "artifact_kind": PTG2_ARTIFACT_RAW,
            "canonical_url": canonical_url,
            "original_url": url,
            "raw_storage_uri": raw_uri,
            "raw_sha256": actual_sha,
            "sha256": actual_sha,
            "content_length": head.content_length if head else actual_size,
            "byte_count": actual_size,
            "etag": head.etag if head else None,
            "last_modified": head.last_modified if head else None,
            "status": "available",
        }
        store.record_manifest(manifest_payload)
        return PTG2RawArtifact(
            original_url=url,
            canonical_url=canonical_url,
            raw_path=str(final_path),
            raw_storage_uri=raw_uri,
            raw_sha256=actual_sha,
            byte_count=actual_size,
            head=head,
            reused=False,
            verification_mode="downloaded",
        )
    except BaseException as exc:
        if tmp_path.exists() and keep_partials:
            partial_dir = store.root / "partial-retained"
            partial_dir.mkdir(parents=True, exist_ok=True)
            partial_path = partial_dir / f"{tmp_path.name}.partial"
            try:
                os.replace(tmp_path, partial_path)
                store.record_manifest(
                    {
                        "artifact_kind": "partial_raw",
                        "canonical_url": canonical_url,
                        "original_url": url,
                        "raw_storage_uri": store.storage_uri(partial_path),
                        "partial_sha256": digest.hexdigest(),
                        "byte_count": partial_path.stat().st_size,
                        "status": "partial",
                        "error": str(exc),
                    }
                )
            except Exception as preserve_exc:
                logger.warning("Failed to preserve partial PTG2 download %s: %s", tmp_path, preserve_exc)
        elif tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        raise


def _stream_copy_with_hash(src, dst, chunk_size: int = 1024 * 1024) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    for chunk in iter(lambda: src.read(chunk_size), b""):
        digest.update(chunk)
        total += len(chunk)
        dst.write(chunk)
    return digest.hexdigest(), total


def _raw_file_is_gzip(path: str | Path) -> bool:
    path_obj = Path(path)
    if path_obj.name.endswith(".gz"):
        return True
    try:
        with open(path_obj, "rb") as fp:
            return fp.read(2) == b"\x1f\x8b"
    except OSError:
        return False


def _first_zip_member(path: str | Path) -> str | None:
    with zipfile.ZipFile(path, "r") as zip_ref:
        for name in zip_ref.namelist():
            if not name.endswith("/"):
                return name
    return None


@contextmanager
def open_json_artifact_stream(path: str | Path):
    path_obj = Path(path)
    if _raw_file_is_gzip(path_obj):
        with open(path_obj, "rb") as raw_fp:
            gzip_cls = igzip.IGzipFile if igzip is not None and _env_bool(PTG2_ISAL_GZIP_ENV, False) else gzip.GzipFile
            with gzip_cls(fileobj=raw_fp, mode="rb") as gzip_fp:
                with io.BufferedReader(gzip_fp, buffer_size=_stream_buffer_bytes()) as buffered_fp:
                    yield buffered_fp
        return
    if zipfile.is_zipfile(path_obj):
        member_name = _first_zip_member(path_obj)
        if not member_name:
            raise RuntimeError(f"No file members found in zip artifact {path_obj}")
        with zipfile.ZipFile(path_obj, "r") as zip_ref:
            with zip_ref.open(member_name, "r") as fp:
                yield fp
        return
    with open(path_obj, "rb") as fp:
        yield fp


def _compression_for_path(raw_path: str | Path) -> tuple[str | None, str | None]:
    raw_path_obj = Path(raw_path)
    compression = "gzip" if _raw_file_is_gzip(raw_path_obj) else None
    member_name = None
    if compression is None and zipfile.is_zipfile(raw_path_obj):
        compression = "zip"
        member_name = _first_zip_member(raw_path_obj)
    return compression, member_name


def logical_artifact_identity(
    raw_path: str | Path,
    *,
    raw_sha256: str | None = None,
    raw_byte_count: int | None = None,
    allow_deferred: bool = False,
) -> PTG2LogicalArtifact:
    raw_path_obj = Path(raw_path)
    compression, member_name = _compression_for_path(raw_path_obj)
    threshold = _env_int(PTG2_DEFER_LOGICAL_HASH_BYTES_ENV, 1024 * 1024 * 1024)
    if allow_deferred and raw_sha256 and raw_byte_count and threshold > 0 and raw_byte_count >= threshold:
        return PTG2LogicalArtifact(
            str(raw_path_obj),
            raw_sha256,
            raw_byte_count,
            compression=compression,
            member_name=member_name,
        )
    digest = hashlib.sha256()
    total = 0
    with open_json_artifact_stream(raw_path_obj) as src:
        for chunk in iter(lambda: src.read(1024 * 1024), b""):
            digest.update(chunk)
            total += len(chunk)
    return PTG2LogicalArtifact(str(raw_path_obj), digest.hexdigest(), total, compression=compression, member_name=member_name)


def load_json_artifact(path: str | Path) -> Any:
    with open_json_artifact_stream(path) as fp:
        return json.load(fp)


def stream_logical_artifact(raw_path: str | Path, output_dir: str | Path | None = None) -> PTG2LogicalArtifact:
    raw_path_obj = Path(raw_path)
    output_root = Path(output_dir) if output_dir else raw_path_obj.parent
    output_root.mkdir(parents=True, exist_ok=True)
    if _raw_file_is_gzip(raw_path_obj):
        target = output_root / f"{raw_path_obj.stem}_logical.json"
        with gzip.open(raw_path_obj, "rb") as src, open(target, "wb") as dst:
            digest, total = _stream_copy_with_hash(src, dst)
        return PTG2LogicalArtifact(str(target), digest, total, compression="gzip")
    if zipfile.is_zipfile(raw_path_obj):
        with zipfile.ZipFile(raw_path_obj, "r") as zip_ref:
            for name in zip_ref.namelist():
                if name.endswith("/"):
                    continue
                target = output_root / Path(name).name
                with zip_ref.open(name, "r") as src, open(target, "wb") as dst:
                    digest, total = _stream_copy_with_hash(src, dst)
                return PTG2LogicalArtifact(str(target), digest, total, compression="zip", member_name=name)
        raise RuntimeError(f"No file members found in zip artifact {raw_path_obj}")
    digest, total = sha256_file(raw_path_obj)
    return PTG2LogicalArtifact(str(raw_path_obj), digest, total)


async def materialize_json_source(
    url: str,
    output_dir: str | Path,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    materialize_logical: bool = True,
    keep_partial_artifacts: bool | None = None,
) -> tuple[PTG2RawArtifact, PTG2LogicalArtifact]:
    raw_artifact = await download_raw_artifact(
        url,
        reuse_raw_artifacts=reuse_raw_artifacts,
        max_bytes=max_bytes,
        keep_partial_artifacts=keep_partial_artifacts,
    )
    logical_artifact = (
        stream_logical_artifact(raw_artifact.raw_path, output_dir=output_dir)
        if materialize_logical
        else logical_artifact_identity(
            raw_artifact.raw_path,
            raw_sha256=raw_artifact.raw_sha256,
            raw_byte_count=raw_artifact.byte_count,
            allow_deferred=True,
        )
    )
    return raw_artifact, logical_artifact


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _ptg2_conflict_targets(cls) -> list[str]:
    if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
        for index in cls.__my_initial_indexes__:
            elements = index.get("index_elements")
            if elements:
                return [str(element) for element in elements]
    if hasattr(cls, "__my_index_elements__") and cls.__my_index_elements__:
        return [str(element) for element in cls.__my_index_elements__]
    return [key.name for key in cls.__table__.primary_key]


def _ptg2_json_columns(cls) -> set[str]:
    result = set()
    for column in cls.__table__.c:
        type_name = column.type.__class__.__name__.upper()
        if "JSON" in type_name:
            result.add(column.name)
    return result


def _ptg2_copy_record(row: dict[str, Any], columns: list[str], json_columns: set[str]) -> tuple[Any, ...]:
    values: list[Any] = []
    for column in columns:
        value = row.get(column)
        if value is not None and column in json_columns:
            value = json.dumps(value, sort_keys=True, default=_json_default)
        values.append(value)
    return tuple(values)


async def _copy_upsert_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    columns = [column.name for column in cls.__table__.c if column.name in rows[0]]
    if not columns:
        return
    conflict_targets = _ptg2_conflict_targets(cls)
    if not conflict_targets:
        await push_objects(rows, cls, rewrite=True, use_copy=False)
        return
    deduped_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        deduped_rows[tuple(row.get(target) for target in conflict_targets)] = row
    rows = list(deduped_rows.values())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    table_name = cls.__tablename__
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    update_columns = [column for column in columns if column not in set(conflict_targets)]
    if update_columns:
        conflict_sql = (
            f"DO UPDATE SET "
            + ", ".join(f"{_quote_ident(column)} = EXCLUDED.{_quote_ident(column)}" for column in update_columns)
        )
    else:
        conflict_sql = "DO NOTHING"
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            temp_table,
            columns=columns,
            records=records,
        )
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) {conflict_sql};
            """
        )


async def _copy_insert_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            cls.__tablename__,
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_ignore_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    conflict_targets = list(getattr(cls, "__my_index_elements__", []) or [])
    if not conflict_targets:
        await _copy_insert_ptg2_objects(rows, cls)
        return
    columns = list(rows[0].keys())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    table_name = cls.__tablename__
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            temp_table,
            columns=columns,
            records=records,
        )
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) DO NOTHING;
            """
        )


async def _copy_stage_price_set_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "snapshot_id",
        "price_set_hash",
        "hash_prefix",
        "price_atom_hashes",
        "canonical_payload",
        "created_at",
    ]
    records = [
        (
            snapshot_id,
            row.get("price_set_hash"),
            row.get("hash_prefix"),
            row.get("price_atom_hashes") or [],
            json.dumps(row.get("canonical_payload"), default=_json_default),
            row.get("created_at"),
        )
        for row in rows
    ]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_price_set_stage",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_stage_serving_rate_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "plan_name",
        "plan_id_type",
        "plan_market_type",
        "issuer_name",
        "plan_sponsor_name",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "billing_code",
        "billing_code_type",
        "procedure_name",
        "procedure_description",
        "procedure_display_name",
        "rate_pack_hash",
        "provider_set_hash",
        "provider_set_hashes",
        "provider_count",
        "provider_set_count",
        "price_set_hash",
        "source_trace_set_hash",
        "confidence_code",
        "prices",
        "source_trace",
        "confidence",
        "created_at",
    ]
    records = [
        (
            row.get("serving_rate_id"),
            snapshot_id,
            row.get("plan_id"),
            row.get("plan_name"),
            row.get("plan_id_type"),
            row.get("plan_market_type"),
            row.get("issuer_name"),
            row.get("plan_sponsor_name"),
            row.get("procedure_code"),
            row.get("reported_code_system"),
            row.get("reported_code"),
            row.get("billing_code"),
            row.get("billing_code_type"),
            row.get("procedure_name"),
            row.get("procedure_description"),
            row.get("procedure_display_name"),
            row.get("rate_pack_hash"),
            row.get("provider_set_hash"),
            row.get("provider_set_hashes") or [],
            row.get("provider_count"),
            row.get("provider_set_count"),
            row.get("price_set_hash"),
            row.get("source_trace_set_hash"),
            row.get("confidence_code"),
            json.dumps(row.get("prices"), default=_json_default) if row.get("prices") is not None else None,
            json.dumps(row.get("source_trace"), default=_json_default) if row.get("source_trace") is not None else None,
            json.dumps(row.get("confidence"), default=_json_default) if row.get("confidence") is not None else None,
            row.get("created_at"),
        )
        for row in rows
    ]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_serving_rate_stage",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_compact_serving_rate_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "plan_month_id",
        "procedure_hash",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "billing_code",
        "billing_code_type",
        "rate_pack_hash",
        "provider_set_hash",
        "provider_count",
        "price_set_hash",
        "source_trace_set_hash",
        "confidence_code",
        "created_at",
    ]
    records = [
        (
            row.get("serving_rate_id"),
            snapshot_id,
            row.get("plan_id"),
            row.get("plan_month_id"),
            row.get("procedure_hash"),
            row.get("procedure_code"),
            row.get("reported_code_system"),
            row.get("reported_code"),
            row.get("billing_code"),
            row.get("billing_code_type"),
            row.get("rate_pack_hash"),
            row.get("provider_set_hash"),
            row.get("provider_count"),
            row.get("price_set_hash"),
            row.get("source_trace_set_hash"),
            row.get("confidence_code"),
            row.get("created_at"),
        )
        for row in rows
    ]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_serving_rate_compact",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_compact_serving_rate_file(copy_path: Path, *, target_table: str = "ptg2_serving_rate_compact") -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    await _copy_compact_serving_rate_source(copy_path.open("rb"), target_table=target_table)


async def _copy_compact_serving_rate_bytes(payload: bytes, *, target_table: str = "ptg2_serving_rate_compact") -> None:
    if not payload:
        return
    await _copy_compact_serving_rate_source(io.BytesIO(payload), target_table=target_table)


async def _copy_compact_serving_rate_source(source, *, target_table: str = "ptg2_serving_rate_compact") -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "plan_month_id",
        "procedure_hash",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "billing_code",
        "billing_code_type",
        "rate_pack_hash",
        "provider_set_hash",
        "provider_count",
        "price_set_hash",
        "source_trace_set_hash",
        "confidence_code",
    ]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        try:
            await copy_to_table(
                target_table,
                source=source,
                schema_name=schema_name,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )
        finally:
            close = getattr(source, "close", None)
            if close is not None:
                close()


async def _copy_ptg2_dictionary_file(copy_path: Path, kind: str, *, target_table: str | None = None) -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    specs = {
        "procedure": (
            "ptg2_procedure",
            [
                "procedure_hash",
                "hash_prefix",
                "billing_code_type",
                "billing_code_type_version",
                "billing_code",
                "name",
                "description",
                "canonical_payload",
            ],
            ["procedure_hash"],
        ),
        "price_set": (
            "ptg2_price_set",
            [
                "price_set_hash",
                "hash_prefix",
                "price_atom_hashes",
                "negotiated_type",
                "negotiated_rate",
                "expiration_date",
                "service_code",
                "billing_class",
                "setting",
                "billing_code_modifier",
                "additional_information",
                "canonical_payload",
            ],
            ["price_set_hash"],
        ),
        "provider_set": (
            "ptg2_provider_set",
            [
                "provider_set_hash",
                "hash_prefix",
                "provider_count",
                "npi",
                "provider_group_hashes",
                "tin_type",
                "tin_value",
                "canonical_payload",
            ],
            ["provider_set_hash"],
        ),
        "provider_group_member": (
            "ptg2_provider_group_member",
            ["provider_group_hash", "npi", "ordinal"],
            ["provider_group_hash", "npi"],
        ),
    }
    if kind not in specs:
        raise ValueError(f"Unsupported PTG2 dictionary copy kind: {kind}")
    table_name, columns, conflict_targets = specs[kind]
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if target_table is not None:
        async with db.acquire() as conn:
            raw_conn = conn.raw_connection
            driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
            copy_to_table = getattr(driver_conn, "copy_to_table", None)
            if copy_to_table is None:
                raise NotImplementedError("Active database driver does not expose copy_to_table")
            with copy_path.open("rb") as source:
                await copy_to_table(
                    target_table,
                    source=source,
                    schema_name=schema_name,
                    columns=columns,
                    format="text",
                    delimiter="\t",
                    null="\\N",
                )
        return
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        with copy_path.open("rb") as source:
            await copy_to_table(
                temp_table,
                source=source,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) DO NOTHING;
            """
        )


_RUST_COPY_TABLE_SPECS = {
    "serving_rate_compact": (
        "ptg2_serving_rate_compact",
        [
            "serving_rate_id",
            "snapshot_id",
            "plan_id",
            "plan_month_id",
            "procedure_hash",
            "procedure_code",
            "reported_code_system",
            "reported_code",
            "billing_code",
            "billing_code_type",
            "rate_pack_hash",
            "provider_set_hash",
            "provider_count",
            "price_set_hash",
            "source_trace_set_hash",
            "confidence_code",
        ],
        ["serving_rate_id"],
    ),
    "procedure": (
        "ptg2_procedure",
        [
            "procedure_hash",
            "hash_prefix",
            "billing_code_type",
            "billing_code_type_version",
            "billing_code",
            "name",
            "description",
            "canonical_payload",
        ],
        ["procedure_hash"],
    ),
    "price_set": (
        "ptg2_price_set",
        [
            "price_set_hash",
            "hash_prefix",
            "price_atom_hashes",
            "negotiated_type",
            "negotiated_rate",
            "expiration_date",
            "service_code",
            "billing_class",
            "setting",
            "billing_code_modifier",
            "additional_information",
            "canonical_payload",
        ],
        ["price_set_hash"],
    ),
    "provider_set": (
        "ptg2_provider_set",
        [
            "provider_set_hash",
            "hash_prefix",
            "provider_count",
            "npi",
            "provider_group_hashes",
            "tin_type",
            "tin_value",
            "canonical_payload",
        ],
        ["provider_set_hash"],
    ),
    "provider_group_member": (
        "ptg2_provider_group_member",
        ["provider_group_hash", "npi", "ordinal"],
        ["provider_group_hash", "npi"],
    ),
}


def _rust_copy_stage_table_name(kind: str, token: str) -> str:
    safe_kind = re.sub(r"[^a-z0-9_]+", "_", kind.lower()).strip("_")
    safe_token = re.sub(r"[^a-z0-9_]+", "_", token.lower()).strip("_")
    return f"ptg2_rust_stage_{safe_kind}_{safe_token}"[:63]


async def _create_rust_copy_stage_tables(token: str) -> dict[str, str]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    stage_tables: dict[str, str] = {}
    for kind, (target_table, _columns, _conflict_targets) in _RUST_COPY_TABLE_SPECS.items():
        stage_table = _rust_copy_stage_table_name(kind, token)
        stage_tables[kind] = stage_table
        await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")
        await db.status(
            f"""
            CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
            (LIKE {_quote_ident(schema_name)}.{_quote_ident(target_table)} INCLUDING DEFAULTS);
            """
        )
        if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
            try:
                await db.status(f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} SET UNLOGGED;")
            except Exception as exc:
                logger.debug("Skipping PTG2 Rust stage unlogged ensure for %s: %s", stage_table, exc)
        try:
            await db.status(
                f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)} "
                "SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
            )
        except Exception as exc:
            logger.debug("Skipping PTG2 Rust stage autovacuum disable for %s: %s", stage_table, exc)
    return stage_tables


async def _merge_rust_copy_stage_tables(stage_tables: dict[str, str], *, drop: bool = True) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    merge_order = ("procedure", "price_set", "provider_group_member", "provider_set", "serving_rate_compact")
    fast_rebuild = _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False)
    for kind in merge_order:
        stage_table = stage_tables.get(kind)
        if not stage_table:
            continue
        target_table, columns, conflict_targets = _RUST_COPY_TABLE_SPECS[kind]
        if fast_rebuild and kind != "procedure":
            await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(target_table)};")
            await db.status(
                f"""
                ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
                RENAME TO {_quote_ident(target_table)};
                """
            )
            continue
        quoted_columns = ", ".join(_quote_ident(column) for column in columns)
        quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
        conflict_sql = ""
        if not fast_rebuild and not (
            kind == "serving_rate_compact" and _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True)
        ):
            conflict_sql = f"ON CONFLICT ({quoted_conflict}) DO NOTHING"
        await db.status(
            f"""
            INSERT INTO {_quote_ident(schema_name)}.{_quote_ident(target_table)} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
            {conflict_sql};
            """
        )
        if drop:
            await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")


async def _count_compact_serving_rate_rows(snapshot_id: str, plan_id: str | None = None) -> int:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    plan_filter = "AND plan_id = :plan_id" if plan_id is not None else ""
    rows = await db.all(
        f"""
        SELECT COUNT(*) AS row_count
          FROM {schema_name}.ptg2_serving_rate_compact
         WHERE snapshot_id = :snapshot_id
         {plan_filter}
        """,
        snapshot_id=snapshot_id,
        plan_id=plan_id,
    )
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        return int(row.get("row_count") or 0)
    return int(getattr(row, "row_count", 0) or 0)


async def _copy_simple_rows(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    await _copy_ignore_ptg2_objects(rows, cls)


async def _merge_staged_price_sets(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_price_set (
            price_set_hash,
            hash_prefix,
            price_atom_hashes,
            canonical_payload,
            created_at
        )
        SELECT DISTINCT ON (price_set_hash)
            price_set_hash,
            hash_prefix,
            COALESCE(price_atom_hashes, ARRAY[]::varchar[]),
            canonical_payload,
            created_at
        FROM {schema_name}.ptg2_price_set_stage
        WHERE snapshot_id = :snapshot_id
        ORDER BY price_set_hash, created_at DESC NULLS LAST
        ON CONFLICT (price_set_hash) DO NOTHING;
        """,
        snapshot_id=snapshot_id,
    )
    if not _env_bool(PTG2_KEEP_PRICE_SET_STAGE_ENV, False):
        await db.status(
            f"DELETE FROM {schema_name}.ptg2_price_set_stage WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )


async def _merge_staged_serving_rates(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_serving_rate (
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            source_trace_set_hash,
            confidence_code,
            prices,
            source_trace,
            confidence,
            created_at
        )
        SELECT
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            COALESCE(provider_set_hashes, ARRAY[]::varchar[]) AS provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            source_trace_set_hash,
            confidence_code,
            prices,
            source_trace,
            confidence,
            created_at
        FROM (
            SELECT
                serving_rate_id,
                snapshot_id,
                plan_id,
                plan_name,
                plan_id_type,
                plan_market_type,
                issuer_name,
                plan_sponsor_name,
                procedure_code,
                reported_code_system,
                reported_code,
                billing_code,
                billing_code_type,
                procedure_name,
                procedure_description,
                procedure_display_name,
                rate_pack_hash,
                provider_set_hash,
                provider_set_hashes,
                provider_count,
                provider_set_count,
                price_set_hash,
                source_trace_set_hash,
                confidence_code,
                prices,
                source_trace,
                confidence,
                created_at
            FROM {schema_name}.ptg2_serving_rate_stage
            WHERE snapshot_id = :snapshot_id
        ) AS s
        ON CONFLICT (serving_rate_id) DO NOTHING;
        """,
        snapshot_id=snapshot_id,
    )
    if not _env_bool(PTG2_KEEP_SERVING_RATE_STAGE_ENV, False):
        await db.status(
            f"DELETE FROM {schema_name}.ptg2_serving_rate_stage WHERE snapshot_id = :snapshot_id",
            snapshot_id=snapshot_id,
        )


async def _build_ptg2_provider_locations(snapshot_id: str) -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    confidence_code = PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION
    try:
        await db.status(f"ALTER TABLE {schema_name}.ptg2_provider_location ALTER COLUMN state TYPE varchar(64);")
    except Exception as exc:
        logger.debug("Skipping ptg2_provider_location state width ensure: %s", exc)
    if _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False):
        selected_npis_sql = f"""
            SELECT DISTINCT npi
            FROM {schema_name}.ptg2_provider_group_member
        """
    else:
        selected_npis_sql = f"""
            SELECT DISTINCT pgm.npi
            FROM {schema_name}.ptg2_serving_rate_compact r
            JOIN {schema_name}.ptg2_provider_set ps
              ON ps.provider_set_hash = r.provider_set_hash
            JOIN LATERAL jsonb_array_elements_text(
                COALESCE(
                    to_jsonb(ps.provider_group_hashes),
                    ps.canonical_payload::jsonb->'provider_group_hashes',
                    '[]'::jsonb
                )
            ) AS psc(provider_group_hash) ON TRUE
            JOIN {schema_name}.ptg2_provider_group_member pgm
              ON pgm.provider_group_hash = psc.provider_group_hash::bigint
            WHERE r.snapshot_id = :snapshot_id
        """
    await db.status(
        f"""
        INSERT INTO {schema_name}.ptg2_provider_location (
            location_hash,
            npi,
            state,
            city,
            city_norm,
            zip5,
            lat,
            lon,
            location_source,
            confidence_code,
            address_payload,
            created_at
        )
        WITH selected_npis AS (
            {selected_npis_sql}
        ),
        address_candidates AS (
            SELECT
                n.npi::bigint AS npi,
                1 AS priority,
                'doctor_clinician_address'::varchar AS location_source,
                NULLIF(BTRIM(d.state), '')::varchar AS state,
                NULLIF(BTRIM(d.city), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(d.city), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                d.latitude::float AS lat,
                d.longitude::float AS lon,
                jsonb_build_object(
                    'address_line1', d.address_line1,
                    'address_line2', d.address_line2,
                    'city', d.city,
                    'state', d.state,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(d.zip_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'source', 'doctor_clinician_address'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.doctor_clinician_address d ON d.npi = n.npi
            WHERE NULLIF(BTRIM(COALESCE(d.state, d.city, d.zip_code, '')), '') IS NOT NULL

            UNION ALL

            SELECT
                n.npi::bigint AS npi,
                2 AS priority,
                'entity_address_unified'::varchar AS location_source,
                NULLIF(BTRIM(e.state_name), '')::varchar AS state,
                NULLIF(BTRIM(e.city_name), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(e.city_name), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                e.lat::float AS lat,
                e.long::float AS lon,
                jsonb_build_object(
                    'address_line1', e.first_line,
                    'address_line2', e.second_line,
                    'city', e.city_name,
                    'state', e.state_name,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(e.postal_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'formatted_address', e.formatted_address,
                    'source', 'entity_address_unified'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.entity_address_unified e ON COALESCE(e.npi, e.inferred_npi) = n.npi
            WHERE e.type IN ('practice', 'primary', 'secondary', 'site')
              AND NULLIF(BTRIM(COALESCE(e.state_name, e.city_name, e.postal_code, '')), '') IS NOT NULL

            UNION ALL

            SELECT
                n.npi::bigint AS npi,
                3 AS priority,
                'npi_address'::varchar AS location_source,
                NULLIF(BTRIM(a.state_name), '')::varchar AS state,
                NULLIF(BTRIM(a.city_name), '')::varchar AS city,
                LOWER(NULLIF(BTRIM(a.city_name), ''))::varchar AS city_norm,
                NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), '')::varchar AS zip5,
                a.lat::float AS lat,
                a.long::float AS lon,
                jsonb_build_object(
                    'address_line1', a.first_line,
                    'address_line2', a.second_line,
                    'city', a.city_name,
                    'state', a.state_name,
                    'zip5', NULLIF(LEFT(REGEXP_REPLACE(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), ''),
                    'formatted_address', a.formatted_address,
                    'source', 'npi_address'
                ) AS address_payload
            FROM selected_npis n
            JOIN {schema_name}.npi_address a ON a.npi = n.npi
            WHERE a.type IN ('practice', 'primary', 'secondary')
              AND NULLIF(BTRIM(COALESCE(a.state_name, a.city_name, a.postal_code, '')), '') IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY npi
                    ORDER BY
                        priority,
                        CASE WHEN zip5 IS NOT NULL AND state IS NOT NULL THEN 0 ELSE 1 END,
                        COALESCE(zip5, ''),
                        COALESCE(city_norm, '')
                ) AS rn
            FROM address_candidates
        )
        SELECT
            md5(concat_ws('|', npi::text, COALESCE(location_source, ''), COALESCE(zip5, ''), COALESCE(city_norm, ''), COALESCE(state, ''))) AS location_hash,
            npi,
            state,
            city,
            city_norm,
            zip5,
            lat,
            lon,
            location_source,
            :confidence_code,
            address_payload::json,
            NOW()
        FROM ranked
        WHERE rn = 1
        ON CONFLICT (location_hash) DO UPDATE
        SET
            npi = excluded.npi,
            state = excluded.state,
            city = excluded.city,
            city_norm = excluded.city_norm,
            zip5 = excluded.zip5,
            lat = excluded.lat,
            lon = excluded.lon,
            location_source = excluded.location_source,
            confidence_code = excluded.confidence_code,
            address_payload = excluded.address_payload,
            created_at = excluded.created_at;
        """,
        snapshot_id=snapshot_id,
        confidence_code=confidence_code,
    )
    try:
        await db.status(f"ANALYZE {schema_name}.ptg2_provider_location;")
    except Exception as exc:
        logger.debug("Skipping ptg2_provider_location ANALYZE: %s", exc)


async def _push_ptg2_objects(rows: list[dict[str, Any]], cls, rewrite: bool = True) -> None:
    if rows and cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        try:
            await _copy_ignore_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/ignore fallback for %s: %s", cls.__tablename__, exc)
    if rows and cls is PTG2ServingRate and _env_bool(PTG2_DIRECT_COPY_SERVING_RATE_ENV, False):
        try:
            await _copy_insert_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 direct COPY fallback for %s: %s", cls.__tablename__, exc)
    if rows and rewrite and len(rows) >= max(_env_int(PTG2_COPY_UPSERT_ROWS_ENV, 250), 1):
        try:
            await _copy_upsert_ptg2_objects(rows, cls)
            return
        except Exception as exc:
            logger.warning("PTG2 copy/upsert fallback for %s: %s", cls.__tablename__, exc)
    try:
        await push_objects(rows, cls, rewrite=rewrite, use_copy=False)
    except TypeError as exc:
        if "use_copy" not in str(exc):
            raise
        await push_objects(rows, cls, rewrite=rewrite)


async def _record_source_version(
    source_type: str,
    domain: str,
    raw_artifact: PTG2RawArtifact,
    logical_artifact: PTG2LogicalArtifact,
    import_run_id: str | None = None,
) -> PTG2SourceVersion:
    source_identity_hash = _source_identity_hash(source_type, raw_artifact.canonical_url)
    version_payload = {
        "source_identity_hash": source_identity_hash,
        "raw_sha256": raw_artifact.raw_sha256,
        "logical_sha256": logical_artifact.logical_sha256,
        "etag": raw_artifact.head.etag if raw_artifact.head else None,
        "content_length": raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
        "last_modified": raw_artifact.head.last_modified if raw_artifact.head else None,
    }
    logical_hash_deferred = (
        bool(logical_artifact.compression)
        and logical_artifact.logical_sha256 == raw_artifact.raw_sha256
        and logical_artifact.byte_count == raw_artifact.byte_count
    )
    source_file_version_id = semantic_hash(version_payload, domain="source_file_version")[:32]
    content_hash = semantic_hash(
        {"domain": domain, "logical_sha256": logical_artifact.logical_sha256},
        domain="content_identity",
    )
    now = _utcnow()
    await _push_ptg2_objects(
        [
            {
                "source_identity_hash": source_identity_hash,
                "hash_prefix": hash_prefix(source_identity_hash),
                "source_type": source_type,
                "canonical_url": raw_artifact.canonical_url,
                "original_url": raw_artifact.original_url,
                "payload": {
                    "source_type": source_type,
                    "domain": domain,
                    "canonical_url": raw_artifact.canonical_url,
                    "original_url": raw_artifact.original_url,
                },
                "created_at": now,
            }
        ],
        PTG2SourceIdentity,
        rewrite=True,
    )
    await _push_ptg2_objects(
        [
            {
                "content_hash": content_hash,
                "hash_prefix": hash_prefix(content_hash),
                "domain": domain,
                "logical_sha256": logical_artifact.logical_sha256,
                "canonical_payload": {
                    "domain": domain,
                    "logical_sha256": logical_artifact.logical_sha256,
                    "compression": logical_artifact.compression,
                    "member_name": logical_artifact.member_name,
                    "logical_hash_deferred": logical_hash_deferred,
                },
                "created_at": now,
            }
        ],
        PTG2ContentIdentity,
        rewrite=True,
    )
    await _push_ptg2_objects(
        [
            {
                "source_file_version_id": source_file_version_id,
                "source_identity_hash": source_identity_hash,
                "content_hash": content_hash,
                "raw_storage_uri": raw_artifact.raw_storage_uri,
                "raw_sha256": raw_artifact.raw_sha256,
                "logical_sha256": logical_artifact.logical_sha256,
                "content_length": raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
                "etag": raw_artifact.head.etag if raw_artifact.head else None,
                "last_modified": raw_artifact.head.last_modified if raw_artifact.head else None,
                "reuse_policy": "metadata_or_hash",
                "verification_mode": raw_artifact.verification_mode,
                "reused_from_source_file_version_id": raw_artifact.reused_from_source_file_version_id,
                "verified_at": now,
                "created_at": now,
                "payload": {
                    "import_run_id": import_run_id,
                    "reused": raw_artifact.reused,
                    "logical_byte_count": logical_artifact.byte_count,
                    "raw_byte_count": raw_artifact.byte_count,
                    "logical_hash_deferred": logical_hash_deferred,
                },
            }
        ],
        PTG2SourceFileVersion,
        rewrite=True,
    )
    await _push_ptg2_objects(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_RAW, "storage_uri": raw_artifact.raw_storage_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": None,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_RAW,
                "storage_uri": raw_artifact.raw_storage_uri,
                "sha256": raw_artifact.raw_sha256,
                "byte_count": raw_artifact.byte_count,
                "payload": {
                    "canonical_url": raw_artifact.canonical_url,
                    "verification_mode": raw_artifact.verification_mode,
                    "reused": raw_artifact.reused,
                },
                "created_at": now,
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return PTG2SourceVersion(
        source_identity_hash=source_identity_hash,
        source_file_version_id=source_file_version_id,
        original_url=raw_artifact.original_url,
        canonical_url=raw_artifact.canonical_url,
        raw_storage_uri=raw_artifact.raw_storage_uri,
        raw_sha256=raw_artifact.raw_sha256,
        logical_sha256=logical_artifact.logical_sha256,
        content_length=raw_artifact.head.content_length if raw_artifact.head else raw_artifact.byte_count,
        etag=raw_artifact.head.etag if raw_artifact.head else None,
        last_modified=raw_artifact.head.last_modified if raw_artifact.head else None,
        verification_mode=raw_artifact.verification_mode,
        reused_from_source_file_version_id=raw_artifact.reused_from_source_file_version_id,
    )


def _row_mapping(row: Any) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)
    if isinstance(row, dict):
        return dict(row)
    return dict(row)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return normalize_money(value)
    return str(value)


async def build_ptg2_snapshot_index_artifact(
    classes: dict[str, type],
    snapshot_id: str,
    import_run_id: str,
) -> dict[str, Any] | None:
    item_cls = classes["PTGInNetworkItem"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    schema = item_cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    limit_clause = ""
    raw_limit = os.getenv("HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
    if raw_limit:
        try:
            row_limit = max(int(raw_limit), 1)
            limit_clause = f" LIMIT {row_limit}"
        except ValueError:
            logger.warning("Ignoring invalid HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT=%s", raw_limit)
    sql = f"""
        SELECT
            i.plan_id,
            i.plan_name,
            i.plan_id_type,
            i.plan_market_type,
            i.issuer_name,
            i.plan_sponsor_name,
            i.billing_code,
            i.billing_code_type,
            i.name AS procedure_name,
            i.description AS procedure_description,
            pg.npi AS provider_npi,
            pg.tin_type,
            pg.tin_value,
            pg.tin_business_name,
            p.negotiated_type,
            p.negotiated_rate::text AS negotiated_rate,
            p.expiration_date::text AS expiration_date,
            p.service_code,
            p.billing_class,
            p.setting,
            p.billing_code_modifier,
            p.additional_information,
            f.url AS source_url
        FROM {schema}.{item_cls.__tablename__} i
        JOIN {schema}.{rate_cls.__tablename__} r ON r.item_hash = i.item_hash
        JOIN {schema}.{price_cls.__tablename__} p ON p.rate_hash = r.rate_hash
        JOIN {schema}.{provider_cls.__tablename__} pg ON pg.provider_group_hash = r.provider_group_hash
        LEFT JOIN {schema}.{file_cls.__tablename__} f ON f.file_id = i.file_id
        WHERE i.plan_id IS NOT NULL
          AND i.billing_code IS NOT NULL
        ORDER BY i.plan_id, i.billing_code
        {limit_clause}
    """
    rows = await db.all(sql)
    if not rows:
        return None

    plans: dict[str, Any] = {}
    procedures: dict[str, Any] = {}
    providers: dict[str, Any] = {}
    provider_ordinals: dict[int, int] = {}
    rates: dict[str, dict[str, list[dict[str, Any]]]] = {}

    def _provider_ordinal(npi: int, row: dict[str, Any]) -> int:
        if npi in provider_ordinals:
            return provider_ordinals[npi]
        ordinal = len(provider_ordinals) + 1
        provider_ordinals[npi] = ordinal
        providers[str(ordinal)] = {
            "provider_ordinal": ordinal,
            "npi": npi,
            "provider_name": row.get("tin_business_name"),
            "tin_type": row.get("tin_type"),
            "tin_value": row.get("tin_value"),
        }
        return ordinal

    for raw_row in rows:
        row = _row_mapping(raw_row)
        plan_id = str(row.get("plan_id") or "").strip()
        code = str(row.get("billing_code") or "").strip().upper()
        if not plan_id or not code:
            continue
        plans.setdefault(
            plan_id,
            {
                "plan_id": plan_id,
                "plan_name": row.get("plan_name"),
                "plan_id_type": row.get("plan_id_type"),
                "plan_market_type": row.get("plan_market_type"),
                "issuer_name": row.get("issuer_name"),
                "plan_sponsor_name": row.get("plan_sponsor_name"),
            },
        )
        procedures.setdefault(
            code,
            {
                "code": code,
                "billing_code": code,
                "billing_code_type": row.get("billing_code_type"),
                "name": row.get("procedure_name"),
                "description": row.get("procedure_description"),
            },
        )
        npi_values = _as_int_list(row.get("provider_npi"))
        for npi in npi_values:
            ordinal = _provider_ordinal(npi, row)
            rate_payload = {
                "provider_ordinal": ordinal,
                "npi": npi,
                "billing_code_type": row.get("billing_code_type"),
                "prices": [
                    {
                        "negotiated_type": row.get("negotiated_type"),
                        "negotiated_rate": money_number(row.get("negotiated_rate")),
                        "expiration_date": row.get("expiration_date"),
                        "service_code": row.get("service_code") or [],
                        "billing_class": row.get("billing_class"),
                        "setting": row.get("setting"),
                        "billing_code_modifier": row.get("billing_code_modifier") or [],
                        "additional_information": row.get("additional_information"),
                    }
                ],
                "source_trace": [
                    {
                        "url": row.get("source_url"),
                        "statement": "Published negotiated rate from Transparency in Coverage source file.",
                    }
                ],
                "confidence": {
                    "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
                    "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
                    "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
                },
            }
            rates.setdefault(plan_id, {}).setdefault(code, []).append(rate_payload)

    payload = {
        "version": 1,
        "snapshot_id": snapshot_id,
        "generated_at": _utcnow().isoformat(),
        "plans": plans,
        "procedures": procedures,
        "providers": providers,
        "rates": rates,
    }
    store = PTG2ArtifactStore()
    target = store.root / PTG2_ARTIFACT_SNAPSHOT_INDEX / f"{snapshot_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.with_suffix(".json.tmp")
    tmp_target.write_text(json.dumps(payload, sort_keys=True, default=_json_default), encoding="utf-8")
    artifact_sha, byte_count = sha256_file(tmp_target)
    os.replace(tmp_target, target)
    artifact_uri = store.storage_uri(target)
    await _push_ptg2_objects(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_SNAPSHOT_INDEX, "snapshot_id": snapshot_id, "storage_uri": artifact_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_SNAPSHOT_INDEX,
                "storage_uri": artifact_uri,
                "sha256": artifact_sha,
                "byte_count": byte_count,
                "payload": {
                    "plan_count": len(plans),
                    "procedure_count": len(procedures),
                    "provider_count": len(providers),
                },
                "created_at": _utcnow(),
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return {
        "storage_uri": artifact_uri,
        "sha256": artifact_sha,
        "byte_count": byte_count,
        "plan_count": len(plans),
        "procedure_count": len(procedures),
        "provider_count": len(providers),
    }


async def build_ptg2_compact_snapshot_index_artifact(
    snapshot_id: str,
    import_run_id: str,
) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    limit_clause = ""
    raw_limit = (
        os.getenv("HLTHPRT_PTG2_COMPACT_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
        or os.getenv("HLTHPRT_PTG2_SNAPSHOT_ARTIFACT_ROW_LIMIT", "").strip()
    )
    if raw_limit:
        try:
            row_limit = max(int(raw_limit), 1)
            limit_clause = f" LIMIT {row_limit}"
        except ValueError:
            logger.warning("Ignoring invalid PTG2 compact artifact row limit=%s", raw_limit)
    sql = f"""
        WITH selected_rates AS (
            SELECT
                p.plan_id,
                p.plan_name,
                p.plan_id_type,
                p.plan_market_type,
                p.issuer_name,
                p.plan_sponsor_name,
                proc.billing_code,
                proc.billing_code_type,
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                rp.rate_pack_hash,
                rp.provider_set_hash,
                rp.price_set_hash,
                rp.source_trace_set_hash,
                rp.canonical_payload::jsonb AS rate_payload,
                prices.prices,
                traces.source_trace,
                providers.provider_set_count,
                providers.provider_count
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan p ON p.plan_hash = pm.plan_hash
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = fc.procedure_hash
            JOIN LATERAL unnest(fc.rate_pack_hashes) AS pack_ref(rate_pack_hash) ON true
            JOIN {schema}.ptg2_rate_pack rp ON rp.rate_pack_hash = pack_ref.rate_pack_hash
            LEFT JOIN {schema}.ptg2_price_set price_set ON price_set.price_set_hash = rp.price_set_hash
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'negotiated_type', pa.negotiated_type,
                            'negotiated_rate',
                                CASE
                                    WHEN pa.negotiated_rate ~ '^-?[0-9]+(\.[0-9]+)?$'
                                        THEN (pa.negotiated_rate)::numeric
                                    ELSE NULL
                                END,
                            'expiration_date', pa.expiration_date::text,
                            'service_code', COALESCE(pa.service_code, ARRAY[]::text[]),
                            'billing_class', pa.billing_class,
                            'setting', pa.setting,
                            'billing_code_modifier', COALESCE(pa.billing_code_modifier, ARRAY[]::text[]),
                            'additional_information', pa.additional_information
                        )
                        ORDER BY pa.price_atom_hash
                    ),
                    '[]'::jsonb
                ) AS prices
                FROM jsonb_array_elements_text(COALESCE(price_set.canonical_payload::jsonb->'price_atom_hashes', '[]'::jsonb)) AS pah(price_atom_hash)
                JOIN {schema}.ptg2_price_atom pa ON pa.price_atom_hash = pah.price_atom_hash
            ) prices ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ),
                    '[]'::jsonb
                ) AS source_trace
                FROM {schema}.ptg2_source_trace_set sts
                JOIN LATERAL jsonb_array_elements_text(COALESCE(sts.canonical_payload::jsonb->'source_trace_hashes', '[]'::jsonb)) AS sth(source_trace_hash) ON true
                JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                WHERE sts.source_trace_set_hash = rp.source_trace_set_hash
            ) traces ON true
            LEFT JOIN LATERAL (
                SELECT
                    count(*)::int AS provider_set_count,
                    COALESCE(sum(ps.provider_count), 0)::int AS provider_count
                FROM jsonb_array_elements_text(COALESCE(rp.canonical_payload::jsonb->'provider_set_hashes', '[]'::jsonb)) AS psh(provider_set_hash)
                LEFT JOIN {schema}.ptg2_provider_set ps ON ps.provider_set_hash = psh.provider_set_hash
            ) providers ON true
            WHERE pm.snapshot_id = :snapshot_id
              AND p.plan_id IS NOT NULL
              AND proc.billing_code IS NOT NULL
            ORDER BY p.plan_id, proc.billing_code, rp.rate_pack_hash
            {limit_clause}
        )
        SELECT * FROM selected_rates
    """
    rows = await db.all(sql, snapshot_id=snapshot_id)
    if not rows:
        return None

    plans: dict[str, Any] = {}
    procedures: dict[str, Any] = {}
    providers: dict[str, Any] = {}
    rates: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for raw_row in rows:
        row = _row_mapping(raw_row)
        plan_id = str(row.get("plan_id") or "").strip()
        code = str(row.get("billing_code") or "").strip().upper()
        provider_key = str(row.get("provider_set_hash") or row.get("rate_pack_hash") or "").strip()
        if not plan_id or not code or not provider_key:
            continue
        provider_count = int(row.get("provider_count") or 0)
        provider_set_count = int(row.get("provider_set_count") or 0)
        plans.setdefault(
            plan_id,
            {
                "plan_id": plan_id,
                "plan_name": row.get("plan_name"),
                "plan_id_type": row.get("plan_id_type"),
                "plan_market_type": row.get("plan_market_type"),
                "issuer_name": row.get("issuer_name"),
                "plan_sponsor_name": row.get("plan_sponsor_name"),
            },
        )
        procedures.setdefault(
            code,
            {
                "code": code,
                "billing_code": code,
                "billing_code_type": row.get("billing_code_type"),
                "name": row.get("procedure_name"),
                "description": row.get("procedure_description"),
            },
        )
        provider_payload = providers.setdefault(
            provider_key,
            {
                "provider_ordinal": provider_key,
                "provider_set_hash": provider_key,
                "provider_name": "TiC provider set",
                "provider_count": provider_count,
                "provider_set_count": provider_set_count,
            },
        )
        provider_payload["provider_count"] = max(int(provider_payload.get("provider_count") or 0), provider_count)
        provider_payload["provider_set_count"] = max(
            int(provider_payload.get("provider_set_count") or 0),
            provider_set_count,
        )
        rate_payload_json = row.get("rate_payload") or {}
        if isinstance(rate_payload_json, str):
            try:
                rate_payload_json = json.loads(rate_payload_json)
            except json.JSONDecodeError:
                rate_payload_json = {}
        prices_json = row.get("prices") or []
        if isinstance(prices_json, str):
            try:
                prices_json = json.loads(prices_json)
            except json.JSONDecodeError:
                prices_json = []
        source_trace_json = row.get("source_trace") or []
        if isinstance(source_trace_json, str):
            try:
                source_trace_json = json.loads(source_trace_json)
            except json.JSONDecodeError:
                source_trace_json = []
        rates.setdefault(plan_id, {}).setdefault(code, []).append(
            {
                "provider_ordinal": provider_key,
                "provider_set_hash": provider_key,
                "provider_set_hashes": rate_payload_json.get("provider_set_hashes") or [],
                "provider_count": provider_count,
                "provider_set_count": provider_set_count,
                "billing_code_type": row.get("billing_code_type"),
                "prices": prices_json,
                "price_set_hash": row.get("price_set_hash"),
                "source_trace": source_trace_json,
                "confidence": {
                    "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
                    "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
                    "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
                },
            }
        )

    payload = {
        "version": 2,
        "snapshot_id": snapshot_id,
        "generated_at": _utcnow().isoformat(),
        "plans": plans,
        "procedures": procedures,
        "providers": providers,
        "rates": rates,
    }
    store = PTG2ArtifactStore()
    target = store.root / PTG2_ARTIFACT_SNAPSHOT_INDEX / f"{snapshot_id}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_target = target.with_suffix(".json.tmp")
    tmp_target.write_text(json.dumps(payload, sort_keys=True, default=_json_default), encoding="utf-8")
    artifact_sha, byte_count = sha256_file(tmp_target)
    os.replace(tmp_target, target)
    artifact_uri = store.storage_uri(target)
    await _push_ptg2_objects(
        [
            {
                "artifact_id": semantic_hash(
                    {"kind": PTG2_ARTIFACT_SNAPSHOT_INDEX, "snapshot_id": snapshot_id, "storage_uri": artifact_uri},
                    domain="artifact_manifest",
                )[:32],
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "artifact_kind": PTG2_ARTIFACT_SNAPSHOT_INDEX,
                "storage_uri": artifact_uri,
                "sha256": artifact_sha,
                "byte_count": byte_count,
                "payload": {
                    "plan_count": len(plans),
                    "procedure_count": len(procedures),
                    "provider_count": len(providers),
                    "rate_count": sum(len(items) for plan_rates in rates.values() for items in plan_rates.values()),
                    "provider_granularity": "provider_set",
                },
                "created_at": _utcnow(),
            }
        ],
        PTG2ArtifactManifest,
        rewrite=True,
    )
    return {
        "storage_uri": artifact_uri,
        "sha256": artifact_sha,
        "byte_count": byte_count,
        "plan_count": len(plans),
        "procedure_count": len(procedures),
        "provider_count": len(providers),
        "rate_count": sum(len(items) for plan_rates in rates.values() for items in plan_rates.values()),
        "provider_granularity": "provider_set",
    }


async def _ptg2_table_available(schema: str, table_name: str) -> bool:
    try:
        value = await db.scalar("SELECT to_regclass(:table_name)", table_name=f"{schema}.{table_name}")
    except Exception as exc:
        logger.debug("Unable to check availability of %s.%s: %s", schema, table_name, exc)
        return False
    return bool(value)


async def build_ptg2_db_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    """Materialize the compact PTG2 serving view into Postgres, without local JSON artifacts."""
    del import_run_id  # The serving table is tied to snapshot_id; import run remains on the snapshot record.
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    raw_limit = os.getenv(PTG2_SERVING_ROW_LIMIT_ENV, "").strip()
    limit_clause = ""
    if raw_limit:
        try:
            limit_clause = f" LIMIT {max(int(raw_limit), 1)}"
        except ValueError:
            logger.warning("Ignoring invalid %s=%s", PTG2_SERVING_ROW_LIMIT_ENV, raw_limit)

    has_code_crosswalk = await _ptg2_table_available(schema, "code_crosswalk")
    has_pricing_procedure = await _ptg2_table_available(schema, "pricing_procedure")
    has_code_catalog = await _ptg2_table_available(schema, "code_catalog")

    mapped_code_join = (
        f"""
            LEFT JOIN LATERAL (
                SELECT cw.to_code::bigint AS procedure_code
                FROM {schema}.code_crosswalk cw
                WHERE UPPER(BTRIM(cw.from_system)) = UPPER(BTRIM(proc.billing_code_type))
                  AND UPPER(BTRIM(cw.from_code)) = UPPER(BTRIM(proc.billing_code))
                  AND UPPER(BTRIM(cw.to_system)) = 'HP_PROCEDURE_CODE'
                  AND cw.to_code ~ '^[0-9]+$'
                ORDER BY cw.confidence DESC NULLS LAST, cw.updated_at DESC NULLS LAST
                LIMIT 1
            ) mapped_code ON true
        """
        if has_code_crosswalk
        else """
            LEFT JOIN LATERAL (
                SELECT NULL::bigint AS procedure_code
            ) mapped_code ON true
        """
    )
    pricing_procedure_join = (
        f"""
            LEFT JOIN LATERAL (
                SELECT
                    pp.procedure_code,
                    pp.service_description,
                    pp.reported_code
                FROM {schema}.pricing_procedure pp
                WHERE (
                        mapped_code.procedure_code IS NOT NULL
                    AND pp.procedure_code = mapped_code.procedure_code
                )
                   OR (
                        mapped_code.procedure_code IS NULL
                    AND UPPER(BTRIM(pp.reported_code)) = UPPER(BTRIM(proc.billing_code))
                )
                ORDER BY
                    CASE WHEN pp.procedure_code = mapped_code.procedure_code THEN 0 ELSE 1 END,
                    pp.source_year DESC NULLS LAST,
                    pp.total_allowed_amount DESC NULLS LAST,
                    pp.procedure_code
                LIMIT 1
            ) pricing_proc ON true
        """
        if has_pricing_procedure
        else """
            LEFT JOIN LATERAL (
                SELECT
                    NULL::bigint AS procedure_code,
                    NULL::text AS service_description,
                    NULL::text AS reported_code
            ) pricing_proc ON true
        """
    )
    catalog_joins = (
        f"""
            LEFT JOIN {schema}.code_catalog external_catalog
              ON UPPER(BTRIM(external_catalog.code_system)) = UPPER(BTRIM(proc.billing_code_type))
             AND UPPER(BTRIM(external_catalog.code)) = UPPER(BTRIM(proc.billing_code))
            LEFT JOIN {schema}.code_catalog internal_catalog
              ON UPPER(BTRIM(internal_catalog.code_system)) = 'HP_PROCEDURE_CODE'
             AND internal_catalog.code = COALESCE(mapped_code.procedure_code, pricing_proc.procedure_code)::text
        """
        if has_code_catalog
        else """
            LEFT JOIN LATERAL (
                SELECT NULL::text AS display_name, NULL::text AS short_description
            ) external_catalog ON true
            LEFT JOIN LATERAL (
                SELECT NULL::text AS display_name, NULL::text AS short_description
            ) internal_catalog ON true
        """
    )
    confidence_payload = {
        "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
        "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
    }

    await db.status(f"DELETE FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id", snapshot_id=snapshot_id)
    await db.status(
        f"""
        WITH raw_keys AS (
            SELECT DISTINCT
                pm.snapshot_id,
                p.plan_id,
                p.plan_name,
                p.plan_id_type,
                p.plan_market_type,
                p.issuer_name,
                p.plan_sponsor_name,
                fc.procedure_hash,
                pack_ref.rate_pack_hash
            FROM {schema}.ptg2_plan_month pm
            JOIN {schema}.ptg2_plan p ON p.plan_hash = pm.plan_hash
            JOIN {schema}.ptg2_plan_rate_set prs ON prs.plan_month_id = pm.plan_month_id
            JOIN {schema}.ptg2_rate_set rs ON rs.rate_set_hash = prs.rate_set_hash
            JOIN LATERAL unnest(rs.chunk_hashes) AS chunk_ref(fact_chunk_hash) ON true
            JOIN {schema}.ptg2_fact_chunk fc ON fc.fact_chunk_hash = chunk_ref.fact_chunk_hash
            JOIN {schema}.ptg2_procedure proc_filter ON proc_filter.procedure_hash = fc.procedure_hash
            JOIN LATERAL unnest(fc.rate_pack_hashes) AS pack_ref(rate_pack_hash) ON true
            WHERE pm.snapshot_id = :snapshot_id
              AND p.plan_id IS NOT NULL
              AND proc_filter.billing_code IS NOT NULL
        ),
        procedure_map AS (
            SELECT
                proc.procedure_hash,
                COALESCE(mapped_code.procedure_code, pricing_proc.procedure_code) AS procedure_code,
                NULLIF(UPPER(BTRIM(proc.billing_code_type)), '') AS reported_code_system,
                NULLIF(UPPER(BTRIM(proc.billing_code)), '') AS reported_code,
                proc.billing_code,
                proc.billing_code_type,
                proc.name AS procedure_name,
                proc.description AS procedure_description,
                COALESCE(
                    internal_catalog.display_name,
                    internal_catalog.short_description,
                    external_catalog.display_name,
                    external_catalog.short_description,
                    pricing_proc.service_description,
                    proc.name,
                    proc.description
                ) AS procedure_display_name
            FROM (SELECT DISTINCT procedure_hash FROM raw_keys) raw_proc
            JOIN {schema}.ptg2_procedure proc ON proc.procedure_hash = raw_proc.procedure_hash
            {mapped_code_join}
            {pricing_procedure_join}
            {catalog_joins}
        ),
        rate_payloads AS (
            SELECT
                rp.rate_pack_hash,
                rp.provider_set_hash,
                providers.provider_set_hashes,
                providers.provider_count,
                providers.provider_set_count,
                rp.price_set_hash,
                prices.prices,
                traces.source_trace
            FROM (SELECT DISTINCT rate_pack_hash FROM raw_keys) raw_pack
            JOIN {schema}.ptg2_rate_pack rp ON rp.rate_pack_hash = raw_pack.rate_pack_hash
            LEFT JOIN {schema}.ptg2_price_set price_set ON price_set.price_set_hash = rp.price_set_hash
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'negotiated_type', pa.negotiated_type,
                            'negotiated_rate',
                                CASE
                                    WHEN pa.negotiated_rate ~ '^-?[0-9]+(\.[0-9]+)?$'
                                        THEN (pa.negotiated_rate)::numeric
                                    ELSE NULL
                                END,
                            'expiration_date', pa.expiration_date::text,
                            'service_code', COALESCE(pa.service_code, ARRAY[]::text[]),
                            'billing_class', pa.billing_class,
                            'setting', pa.setting,
                            'billing_code_modifier', COALESCE(pa.billing_code_modifier, ARRAY[]::text[]),
                            'additional_information', pa.additional_information
                        )
                        ORDER BY pa.price_atom_hash
                    ),
                    '[]'::jsonb
                ) AS prices
                FROM jsonb_array_elements_text(
                    COALESCE(price_set.canonical_payload::jsonb->'price_atom_hashes', to_jsonb(price_set.price_atom_hashes), '[]'::jsonb)
                ) AS pah(price_atom_hash)
                JOIN {schema}.ptg2_price_atom pa ON pa.price_atom_hash = pah.price_atom_hash
            ) prices ON true
            LEFT JOIN LATERAL (
                SELECT COALESCE(
                    jsonb_agg(
                        jsonb_build_object(
                            'url', st.original_url,
                            'canonical_url', st.canonical_url,
                            'statement', 'Published negotiated rate from Transparency in Coverage source file.'
                        )
                        ORDER BY st.source_trace_hash
                    ),
                    '[]'::jsonb
                ) AS source_trace
                FROM {schema}.ptg2_source_trace_set sts
                JOIN LATERAL jsonb_array_elements_text(COALESCE(sts.canonical_payload::jsonb->'source_trace_hashes', '[]'::jsonb)) AS sth(source_trace_hash) ON true
                JOIN {schema}.ptg2_source_trace st ON st.source_trace_hash = sth.source_trace_hash
                WHERE sts.source_trace_set_hash = rp.source_trace_set_hash
            ) traces ON true
            LEFT JOIN LATERAL (
                WITH refs AS (
                    SELECT DISTINCT psh.provider_set_hash
                    FROM jsonb_array_elements_text(COALESCE(rp.canonical_payload::jsonb->'provider_set_hashes', '[]'::jsonb)) AS psh(provider_set_hash)
                    UNION
                    SELECT rp.provider_set_hash
                    WHERE NOT (rp.canonical_payload::jsonb ? 'provider_set_hashes')
                      AND rp.provider_set_hash IS NOT NULL
                )
                SELECT
                    COALESCE(array_agg(refs.provider_set_hash ORDER BY refs.provider_set_hash), ARRAY[]::varchar[]) AS provider_set_hashes,
                    count(refs.provider_set_hash)::int AS provider_set_count,
                    COALESCE(sum(ps.provider_count), 0)::int AS provider_count
                FROM refs
                LEFT JOIN {schema}.ptg2_provider_set ps ON ps.provider_set_hash = refs.provider_set_hash
            ) providers ON true
        ),
        selected_rates AS (
            SELECT
                raw_keys.snapshot_id,
                raw_keys.plan_id,
                raw_keys.plan_name,
                raw_keys.plan_id_type,
                raw_keys.plan_market_type,
                raw_keys.issuer_name,
                raw_keys.plan_sponsor_name,
                procedure_map.procedure_code,
                procedure_map.reported_code_system,
                procedure_map.reported_code,
                procedure_map.billing_code,
                procedure_map.billing_code_type,
                procedure_map.procedure_name,
                procedure_map.procedure_description,
                procedure_map.procedure_display_name,
                rate_payloads.rate_pack_hash,
                rate_payloads.provider_set_hash,
                rate_payloads.provider_set_hashes,
                rate_payloads.provider_count,
                rate_payloads.provider_set_count,
                rate_payloads.price_set_hash,
                rate_payloads.prices,
                rate_payloads.source_trace
            FROM raw_keys
            JOIN procedure_map ON procedure_map.procedure_hash = raw_keys.procedure_hash
            JOIN rate_payloads ON rate_payloads.rate_pack_hash = raw_keys.rate_pack_hash
            ORDER BY raw_keys.snapshot_id, raw_keys.plan_id, procedure_map.billing_code, rate_payloads.rate_pack_hash
            {limit_clause}
        )
        INSERT INTO {schema}.ptg2_serving_rate (
            serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            prices,
            source_trace,
            confidence,
            created_at
        )
        SELECT
            md5(concat_ws('|', snapshot_id, plan_id, COALESCE(billing_code, ''), rate_pack_hash)) AS serving_rate_id,
            snapshot_id,
            plan_id,
            plan_name,
            plan_id_type,
            plan_market_type,
            issuer_name,
            plan_sponsor_name,
            procedure_code,
            reported_code_system,
            reported_code,
            billing_code,
            billing_code_type,
            procedure_name,
            procedure_description,
            procedure_display_name,
            rate_pack_hash,
            provider_set_hash,
            provider_set_hashes,
            provider_count,
            provider_set_count,
            price_set_hash,
            prices::json,
            source_trace::json,
            CAST(:confidence_json AS json),
            NOW()
        FROM selected_rates
        ON CONFLICT (serving_rate_id) DO UPDATE
        SET
            plan_name = excluded.plan_name,
            plan_id_type = excluded.plan_id_type,
            plan_market_type = excluded.plan_market_type,
            issuer_name = excluded.issuer_name,
            plan_sponsor_name = excluded.plan_sponsor_name,
            procedure_code = excluded.procedure_code,
            reported_code_system = excluded.reported_code_system,
            reported_code = excluded.reported_code,
            billing_code = excluded.billing_code,
            billing_code_type = excluded.billing_code_type,
            procedure_name = excluded.procedure_name,
            procedure_description = excluded.procedure_description,
            procedure_display_name = excluded.procedure_display_name,
            provider_set_hash = excluded.provider_set_hash,
            provider_set_hashes = excluded.provider_set_hashes,
            provider_count = excluded.provider_count,
            provider_set_count = excluded.provider_set_count,
            price_set_hash = excluded.price_set_hash,
            prices = excluded.prices,
            source_trace = excluded.source_trace,
            confidence = excluded.confidence,
            created_at = excluded.created_at;
        """,
        snapshot_id=snapshot_id,
        confidence_json=json.dumps(confidence_payload),
    )
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate ANALYZE: %s", exc)

    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count == 0:
        return None
    return {
        "storage": "db",
        "table": f"{schema}.ptg2_serving_rate",
        "snapshot_id": snapshot_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"""
                SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code))
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_reference_count": int(
            await db.scalar(
                f"""
                SELECT COALESCE(SUM(provider_count), 0)
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "code_crosswalk_available": has_code_crosswalk,
            "pricing_procedure_available": has_pricing_procedure,
            "code_catalog_available": has_code_catalog,
        },
    }


async def finalize_ptg2_incremental_serving_index(snapshot_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate ANALYZE: %s", exc)
    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count == 0:
        return None
    return {
        "storage": "db",
        "table": f"{schema}.ptg2_serving_rate",
        "snapshot_id": snapshot_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"""
                SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code))
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_reference_count": int(
            await db.scalar(
                f"""
                SELECT COALESCE(SUM(provider_count), 0)
                FROM {schema}.ptg2_serving_rate
                WHERE snapshot_id = :snapshot_id
                """,
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
    }


async def build_ptg2_stage_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    try:
        await db.status(f"ANALYZE {schema}.ptg2_serving_rate_stage;")
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage ANALYZE: %s", exc)
    reltuples = await db.scalar(
        """
        SELECT reltuples::bigint
          FROM pg_class
         WHERE oid = to_regclass(:table_name)
        """,
        table_name=f"{schema}.ptg2_serving_rate_stage",
    )
    estimated_rate_count = int(reltuples or 0)
    if estimated_rate_count <= 0:
        return None
    stats_rows = await db.all(
        """
        SELECT attname, n_distinct
          FROM pg_stats
         WHERE schemaname = :schema
           AND tablename = 'ptg2_serving_rate_stage'
           AND attname IN ('plan_id', 'billing_code', 'procedure_code')
        """,
        schema=schema,
    )
    stats = {str(row[0]): float(row[1] or 0) for row in stats_rows}

    def estimated_distinct(column: str) -> int | None:
        value = stats.get(column)
        if value is None:
            return None
        if value < 0:
            return max(int(abs(value) * estimated_rate_count), 1)
        return max(int(value), 1)

    return {
        "storage": "db_stage",
        "table": f"{schema}.ptg2_serving_rate_stage",
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "estimated_rate_count": estimated_rate_count,
        "estimated_plan_count": estimated_distinct("plan_id"),
        "estimated_procedure_count": estimated_distinct("procedure_code") or estimated_distinct("billing_code"),
        "provider_granularity": "provider_set",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
    }


async def build_ptg2_compact_serving_index(snapshot_id: str, import_run_id: str) -> dict[str, Any] | None:
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    compact_table = f"{schema}.ptg2_serving_rate_compact"
    if _env_bool(PTG2_SKIP_COMPACT_FINALIZE_ENV, False):
        logger.info("Skipping PTG2 compact serving finalize/index build for %s", snapshot_id)
        return {
            "storage": "db_compact",
            "table": compact_table,
            "snapshot_id": snapshot_id,
            "import_run_id": import_run_id,
            "finalize_skipped": True,
        }
    compact_indexes = [
        (
            "ptg2_serving_rate_compact_billing_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_billing_idx ON {compact_table} (snapshot_id, plan_id, billing_code)",
        ),
        (
            "ptg2_serving_rate_compact_reported_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_reported_idx ON {compact_table} (snapshot_id, plan_id, reported_code)",
        ),
        (
            "ptg2_serving_rate_compact_hp_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_hp_idx ON {compact_table} (snapshot_id, plan_id, procedure_code)",
        ),
        (
            "ptg2_serving_rate_compact_billing_order_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_billing_order_idx ON {compact_table} (snapshot_id, plan_id, billing_code, provider_count DESC, serving_rate_id)",
        ),
        (
            "ptg2_serving_rate_compact_reported_order_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_reported_order_idx ON {compact_table} (snapshot_id, plan_id, reported_code, provider_count DESC, serving_rate_id)",
        ),
        (
            "ptg2_serving_rate_compact_provider_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_provider_idx ON {compact_table} (provider_set_hash)",
        ),
        (
            "ptg2_serving_rate_compact_price_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_serving_rate_compact_price_idx ON {compact_table} (price_set_hash)",
        ),
        (
            "ptg2_provider_set_component_group_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_provider_set_component_group_idx ON {schema}.ptg2_provider_set_component (provider_group_hash, provider_set_hash)",
        ),
        (
            "ptg2_provider_set_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_set_pkey ON {schema}.ptg2_provider_set (provider_set_hash)",
        ),
        (
            "ptg2_provider_set_component_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_set_component_pkey ON {schema}.ptg2_provider_set_component (provider_set_hash, provider_group_hash)",
        ),
        (
            "ptg2_provider_group_member_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_provider_group_member_pkey ON {schema}.ptg2_provider_group_member (provider_group_hash, npi)",
        ),
        (
            "ptg2_provider_group_member_npi_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_provider_group_member_npi_idx ON {schema}.ptg2_provider_group_member (npi, provider_group_hash)",
        ),
        (
            "ptg2_price_set_prefix_idx",
            f"CREATE INDEX IF NOT EXISTS ptg2_price_set_prefix_idx ON {schema}.ptg2_price_set (hash_prefix)",
        ),
        (
            "ptg2_price_set_pkey",
            f"CREATE UNIQUE INDEX IF NOT EXISTS ptg2_price_set_pkey ON {schema}.ptg2_price_set (price_set_hash)",
        ),
    ]
    for index_name, statement in compact_indexes:
        try:
            await db.status(statement)
        except Exception as exc:
            logger.warning("Failed to ensure compact serving index %s: %s", index_name, exc)
    if _env_bool(PTG2_DEFER_PROVIDER_LOCATIONS_ENV, True):
        await _build_ptg2_provider_locations(snapshot_id)
    try:
        await db.status(f"ALTER TABLE {compact_table} RESET (autovacuum_enabled, toast.autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_provider_set_component RESET (autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_provider_group_member RESET (autovacuum_enabled);")
        await db.status(f"ALTER TABLE {schema}.ptg2_price_set RESET (autovacuum_enabled, toast.autovacuum_enabled);")
    except Exception as exc:
        logger.debug("Skipping PTG2 compact autovacuum reset: %s", exc)
    try:
        await db.status(f"ANALYZE {compact_table};")
        await db.status(f"ANALYZE {schema}.ptg2_provider_set_component;")
        await db.status(f"ANALYZE {schema}.ptg2_provider_group_member;")
        await db.status(f"ANALYZE {schema}.ptg2_price_set;")
    except Exception as exc:
        logger.debug("Skipping PTG2 compact ANALYZE: %s", exc)
    count_params = {"snapshot_id": snapshot_id}
    rate_count = int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
            **count_params,
        )
        or 0
    )
    if rate_count <= 0:
        return None
    return {
        "storage": "db_compact",
        "table": f"{schema}.ptg2_serving_rate_compact",
        "snapshot_id": snapshot_id,
        "import_run_id": import_run_id,
        "rate_count": rate_count,
        "plan_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT plan_id) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "procedure_count": int(
            await db.scalar(
                f"SELECT COUNT(DISTINCT COALESCE(procedure_code::text, reported_code, billing_code)) FROM {schema}.ptg2_serving_rate_compact WHERE snapshot_id = :snapshot_id",
                **count_params,
            )
            or 0
        ),
        "provider_granularity": "provider",
        "procedure_consolidation": {
            "system": "HP_PROCEDURE_CODE",
            "source": "streaming_import",
        },
        "json_serving_artifact": False,
    }


async def prepare_ptg2_compact_bulk_load() -> None:
    if not _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True):
        return
    schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    fast_rebuild = _env_bool(PTG2_FAST_FINAL_REBUILD_ENV, False)
    unlogged_final = _env_bool(PTG2_UNLOGGED_FINAL_ENV, fast_rebuild)
    statements = [
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_set_component SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_provider_group_member SET (autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_price_set SET (autovacuum_enabled = false, toast.autovacuum_enabled = false)",
        f"ALTER TABLE {schema}.ptg2_serving_rate_compact DROP CONSTRAINT IF EXISTS ptg2_serving_rate_compact_pkey",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_billing_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_reported_order_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_hp_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_provider_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_price_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_group_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_npi_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_idx_primary",
        f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_prefix_idx",
        f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_idx_primary",
    ]
    if fast_rebuild:
        statements = [
            f"TRUNCATE {schema}.ptg2_serving_rate_compact",
            f"TRUNCATE {schema}.ptg2_provider_set_component",
            f"TRUNCATE {schema}.ptg2_provider_group_member",
            f"TRUNCATE {schema}.ptg2_provider_set",
            f"TRUNCATE {schema}.ptg2_price_set",
        ] + statements + [
            f"DROP INDEX IF EXISTS {schema}.ptg2_serving_rate_compact_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_component_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_group_member_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_provider_set_pkey",
            f"DROP INDEX IF EXISTS {schema}.ptg2_price_set_pkey",
        ]
    if unlogged_final:
        statements = [
            f"ALTER TABLE {schema}.ptg2_serving_rate_compact SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set_component SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_group_member SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_provider_set SET UNLOGGED",
            f"ALTER TABLE {schema}.ptg2_price_set SET UNLOGGED",
        ] + statements
    for statement in statements:
        try:
            await db.status(statement)
        except Exception as exc:
            logger.debug("Skipping compact bulk-load prep statement %s: %s", statement, exc)


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]


def _normalize_import_id(import_id: str | None) -> str:
    if not import_id:
        return datetime.date.today().strftime("%Y%m%d")
    normalized = "".join(ch if ch.isalnum() else "_" for ch in str(import_id))
    if not normalized:
        return datetime.date.today().strftime("%Y%m%d")
    if len(normalized) > 34:
        suffix = hash_prefix(semantic_hash(normalized, domain="import_id"), 8)
        normalized = f"{normalized[:25]}_{suffix}"
    return normalized


def _make_checksum(*values: Any) -> int:
    digest = hashlib.sha256(canonical_json_dumps(list(values)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) & ((1 << 63) - 1)


def _coerce_date(value: Any) -> datetime.date | None:
    if value is None:
        return None
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        try:
            return datetime.date.fromisoformat(text[:10])
        except ValueError:
            pass
    try:
        parsed = parse_date(text)
    except (ValueError, TypeError):
        return None
    if isinstance(parsed, datetime.datetime):
        return parsed.date()
    return parsed


def _as_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _as_int_list(value: Any) -> list[int]:
    result: list[int] = []
    for item in _as_list(value):
        try:
            result.append(int(str(item).strip()))
        except (TypeError, ValueError):
            continue
    return result


def _normalized_npi_list(value: Any) -> list[int]:
    return sorted(set(_as_int_list(value)))


def _normalize_tin_type(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_tin_value(value: Any) -> str:
    text = str(value or "").strip().upper()
    return "".join(ch for ch in text if ch.isalnum())


def _provider_group_identity_hash(tin_info: dict[str, Any] | None, npi_list: Any) -> int:
    tin_info = tin_info or {}
    return _make_checksum(
        "provider_group",
        _normalize_tin_type(tin_info.get("type")),
        _normalize_tin_value(tin_info.get("value")),
        _normalized_npi_list(npi_list),
    )


def _provider_group_hash_prefix(provider_group_hash: int) -> str:
    return f"{int(provider_group_hash):016x}"[:16]


def _ptg2_provider_group_rows(
    *,
    provider_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group in provider_groups:
        tin_info = group.get("tin") or {}
        normalized_npi = _normalized_npi_list(group.get("npi"))
        provider_group_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        tin_type = _normalize_tin_type(tin_info.get("type"))
        tin_value = _normalize_tin_value(tin_info.get("value"))
        payload = {
            "tin_type": tin_type,
            "tin_value": tin_value,
            "npi": normalized_npi,
        }
        rows.append(
            {
                "provider_group_hash": provider_group_hash,
                "hash_prefix": _provider_group_hash_prefix(provider_group_hash),
                "provider_count": len(normalized_npi),
                "npi": normalized_npi,
                "tin_type": tin_type or None,
                "tin_value": tin_value or None,
                "tin_business_name": tin_info.get("business_name"),
                "canonical_payload": _canonicalize_for_json(payload),
                "created_at": _utcnow(),
            }
        )
    return rows


def _build_provider_set_entry(
    *,
    file_id: int,
    provider_group_ref: Any,
    provider_groups: list[dict[str, Any]],
    network_names: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    group_payloads: list[dict[str, Any]] = []
    union_npis: set[int] = set()
    tin_values: set[tuple[str, str]] = set()
    business_names: list[str] = []
    for group in provider_groups:
        tin_info = group.get("tin") or {}
        normalized_npi = _normalized_npi_list(group.get("npi"))
        provider_group_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        tin_type = _normalize_tin_type(tin_info.get("type"))
        tin_value = _normalize_tin_value(tin_info.get("value"))
        if tin_type or tin_value:
            tin_values.add((tin_type, tin_value))
        if tin_info.get("business_name"):
            business_names.append(str(tin_info.get("business_name")))
        union_npis.update(normalized_npi)
        group_payloads.append(
            {
                "provider_group_hash": provider_group_hash,
                "tin_type": tin_type,
                "tin_value": tin_value,
                "npi": normalized_npi,
            }
        )
    if not group_payloads:
        return None, None
    group_payloads = sorted(group_payloads, key=_canonical_sort_key)
    provider_hash = (
        group_payloads[0]["provider_group_hash"]
        if len(group_payloads) == 1
        else _make_checksum("provider_set", group_payloads)
    )
    sorted_tins = sorted(tin_values)
    single_tin = sorted_tins[0] if len(sorted_tins) == 1 else None
    tin_type = single_tin[0] if single_tin else ("set" if sorted_tins else None)
    tin_value = single_tin[1] if single_tin else None
    tin_business_name = business_names[0] if len(set(business_names)) == 1 else None
    npi_values = sorted(union_npis)
    provider_entry = {
        "provider_group_id": provider_group_ref,
        "network_name": network_names or [],
        "__hash__": provider_hash,
        "npi": npi_values,
        "provider_count": len(npi_values),
        "tin": {"type": tin_type, "value": tin_value, "business_name": tin_business_name},
        "provider_group_hashes": [payload["provider_group_hash"] for payload in group_payloads],
        "provider_group_count": len(group_payloads),
    }
    row = {
        "provider_group_hash": provider_hash,
        "provider_group_ref": provider_group_ref,
        "file_id": file_id,
        "network_names": network_names or [],
        "tin_type": tin_type,
        "tin_value": tin_value,
        "tin_business_name": tin_business_name,
        "npi": npi_values,
    }
    return provider_entry, row


def _combine_provider_set_entries(
    *,
    file_id: int,
    entries: list[dict[str, Any]],
    network_names: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]] | tuple[None, None]:
    clean_entries = [entry for entry in entries if entry and entry.get("__hash__")]
    if not clean_entries:
        return None, None
    if len(clean_entries) == 1:
        entry = dict(clean_entries[0])
        entry["provider_count"] = int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
        row = {
            "provider_group_hash": entry["__hash__"],
            "provider_group_ref": entry.get("provider_group_id"),
            "file_id": file_id,
            "network_names": entry.get("network_name") or network_names or [],
            "tin_type": (entry.get("tin") or {}).get("type"),
            "tin_value": (entry.get("tin") or {}).get("value"),
            "tin_business_name": (entry.get("tin") or {}).get("business_name"),
            "npi": _normalized_npi_list(entry.get("npi")),
        }
        return entry, row
    entry_hashes = sorted({int(entry["__hash__"]) for entry in clean_entries})
    provider_hash = _make_checksum("provider_rate_provider_set", entry_hashes)
    fast_provider_union = _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False)
    npi_values: set[int] = set()
    provider_count = 0
    provider_group_hashes: set[int] = set()
    merged_network_names: set[str] = set(network_names or [])
    for entry in clean_entries:
        if fast_provider_union:
            provider_count += int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
        else:
            npi_values.update(_as_int_list(entry.get("npi")))
        provider_group_hashes.update(int(value) for value in entry.get("provider_group_hashes") or [entry["__hash__"]])
        merged_network_names.update(str(value) for value in _as_list(entry.get("network_name")) if value)
    sorted_npis = [] if fast_provider_union else sorted(npi_values)
    if not fast_provider_union:
        provider_count = len(sorted_npis)
    provider_entry = {
        "provider_group_id": None,
        "network_name": sorted(merged_network_names),
        "__hash__": provider_hash,
        "npi": sorted_npis,
        "provider_count": provider_count,
        "provider_count_mode": "summed_provider_groups" if fast_provider_union else "exact_npi_union",
        "tin": {"type": "set", "value": None, "business_name": None},
        "provider_group_hashes": sorted(provider_group_hashes),
        "provider_group_count": len(provider_group_hashes),
    }
    row = {
        "provider_group_hash": provider_hash,
        "provider_group_ref": None,
        "file_id": file_id,
        "network_names": sorted(merged_network_names),
        "tin_type": "set",
        "tin_value": None,
        "tin_business_name": None,
        "npi": sorted_npis,
    }
    return provider_entry, row


def _fast_provider_entry_from_parts(
    *,
    entry_hashes: set[int],
    provider_group_hashes: set[int],
    provider_count: int,
    network_names: set[str] | None = None,
) -> dict[str, Any] | None:
    if not entry_hashes:
        return None
    sorted_entry_hashes = sorted(entry_hashes)
    provider_hash = (
        sorted_entry_hashes[0]
        if len(sorted_entry_hashes) == 1
        else _make_checksum("provider_rate_provider_set", sorted_entry_hashes)
    )
    return {
        "provider_group_id": None,
        "network_name": sorted(network_names or []),
        "__hash__": provider_hash,
        "npi": [],
        "provider_count": int(provider_count or 0),
        "provider_count_mode": "summed_provider_groups",
        "tin": {"type": "set", "value": None, "business_name": None},
        "provider_group_hashes": sorted(provider_group_hashes),
        "provider_group_count": len(provider_group_hashes),
    }


def _fast_provider_entry_from_provider_refs(
    provider_map,
    provider_refs: list[Any],
) -> tuple[dict[str, Any] | None, list[Any]]:
    entry_hashes: set[int] = set()
    provider_group_hashes: set[int] = set()
    network_names: set[str] = set()
    provider_count = 0
    missing_refs: list[Any] = []
    for provider_ref in provider_refs:
        provider_key = _normalize_provider_ref(provider_ref)
        groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
        if not groups:
            missing_refs.append(provider_ref)
            continue
        for entry in groups:
            if not entry or "__hash__" not in entry:
                continue
            entry_hash = int(entry["__hash__"])
            if entry_hash in entry_hashes:
                continue
            entry_hashes.add(entry_hash)
            provider_group_hashes.update(int(value) for value in entry.get("provider_group_hashes") or [entry_hash])
            provider_count += int(entry.get("provider_count") or len(_as_int_list(entry.get("npi"))))
            network_names.update(str(value) for value in _as_list(entry.get("network_name")) if value)
    return (
        _fast_provider_entry_from_parts(
            entry_hashes=entry_hashes,
            provider_group_hashes=provider_group_hashes,
            provider_count=provider_count,
            network_names=network_names,
        ),
        missing_refs,
    )


def _ptg2_provider_set_row(provider_entry: dict[str, Any]) -> dict[str, Any]:
    tin = provider_entry.get("tin") or {}
    npi_values = _normalized_npi_list(provider_entry.get("npi"))
    provider_count = int(provider_entry.get("provider_count") or len(npi_values))
    provider_group_hashes = sorted({int(value) for value in provider_entry.get("provider_group_hashes") or []})
    provider_group_count = provider_entry.get("provider_group_count") or len(provider_group_hashes)
    identity_payload = {
        "tin_type": _normalize_tin_type(tin.get("type")),
        "tin_value": _normalize_tin_value(tin.get("value")),
        "provider_group_hashes": provider_group_hashes,
        "provider_group_count": provider_group_count,
    }
    if not provider_group_hashes:
        identity_payload["npi"] = npi_values
    provider_set_hash = semantic_hash(identity_payload, domain="provider_set")
    inline_limit = max(_env_int(PTG2_PROVIDER_SET_INLINE_NPI_LIMIT_ENV, 0), 0)
    inline_npi = not provider_group_hashes or (bool(npi_values) and len(npi_values) <= inline_limit)
    canonical_payload = {
        **identity_payload,
        "provider_count": provider_count,
        "provider_count_mode": provider_entry.get("provider_count_mode") or "exact_npi_union",
        "npi_inline": inline_npi,
    }
    if inline_npi:
        canonical_payload["npi"] = npi_values
    return {
        "provider_set_hash": provider_set_hash,
        "hash_prefix": hash_prefix(provider_set_hash),
        "provider_count": provider_count,
        "npi": npi_values if inline_npi else None,
        "tin_type": identity_payload["tin_type"] or None,
        "tin_value": identity_payload["tin_value"] or None,
        "canonical_payload": _canonicalize_for_json(canonical_payload),
        "created_at": _utcnow(),
    }


def _normalize_code_component(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text.upper() if text else None


def _ptg2_procedure_row(in_item: dict[str, Any]) -> dict[str, Any]:
    identity_payload = {
        "billing_code_type": _normalize_code_component(in_item.get("billing_code_type")),
        "billing_code_type_version": _normalize_code_component(in_item.get("billing_code_type_version")),
        "billing_code": _normalize_code_component(in_item.get("billing_code")),
        "negotiation_arrangement": _normalize_code_component(in_item.get("negotiation_arrangement")),
    }
    source_payload = {
        "name": in_item.get("name"),
        "description": in_item.get("description"),
        "bundled_codes": in_item.get("bundled_codes") or [],
        "covered_services": in_item.get("covered_services") or [],
    }
    procedure_hash = semantic_hash(identity_payload, domain="procedure")
    return {
        "procedure_hash": procedure_hash,
        "hash_prefix": hash_prefix(procedure_hash),
        "billing_code_type": in_item.get("billing_code_type"),
        "billing_code_type_version": in_item.get("billing_code_type_version"),
        "billing_code": in_item.get("billing_code"),
        "name": in_item.get("name"),
        "description": in_item.get("description"),
        "canonical_payload": _canonicalize_for_json(
            {
                "identity": identity_payload,
                "source": source_payload,
            }
        ),
        "created_at": _utcnow(),
    }


def _ptg2_price_atom_row(negotiated_price: dict[str, Any]) -> dict[str, Any]:
    raw_rate = negotiated_price.get("negotiated_rate")
    rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
    payload = PTG2PriceAtomEvent(
        negotiated_type=negotiated_price.get("negotiated_type"),
        negotiated_rate=rate_value,
        expiration_date=_coerce_date(negotiated_price.get("expiration_date")),
        service_code=tuple(_as_list(negotiated_price.get("service_code"))),
        billing_class=negotiated_price.get("billing_class"),
        setting=negotiated_price.get("setting"),
        billing_code_modifier=tuple(_as_list(negotiated_price.get("billing_code_modifier"))),
        additional_information=negotiated_price.get("additional_information"),
    )
    built = build_price_atom(payload)
    canonical = built["canonical_payload"]
    return {
        "price_atom_hash": built["price_atom_hash"],
        "hash_prefix": built["hash_prefix"],
        "negotiated_type": negotiated_price.get("negotiated_type"),
        "negotiated_rate": normalize_money(rate_value) if rate_value is not None else None,
        "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
        "service_code": _as_list(negotiated_price.get("service_code")),
        "billing_class": negotiated_price.get("billing_class"),
        "setting": negotiated_price.get("setting"),
        "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
        "additional_information": negotiated_price.get("additional_information"),
        "canonical_payload": canonical,
        "created_at": _utcnow(),
    }


def _ptg2_source_trace_rows(source_version: PTG2SourceVersion | None, source_url: str) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = {
        "source_file_version_id": source_version.source_file_version_id if source_version else None,
        "original_url": source_version.original_url if source_version else source_url,
        "canonical_url": source_version.canonical_url if source_version else canonicalize_url(source_url),
        "json_pointer": None,
        "statement": "Published negotiated rate from Transparency in Coverage source file.",
    }
    source_trace_hash = semantic_hash(payload, domain="source_trace")
    source_trace_row = {
        "source_trace_hash": source_trace_hash,
        "hash_prefix": hash_prefix(source_trace_hash),
        "source_file_version_id": payload["source_file_version_id"],
        "original_url": payload["original_url"],
        "canonical_url": payload["canonical_url"],
        "json_pointer": payload["json_pointer"],
        "line_number": None,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }
    source_trace_set = build_source_trace_set([source_trace_hash])
    source_trace_set_row = {
        **source_trace_set,
        "created_at": _utcnow(),
    }
    return source_trace_row, source_trace_set_row


def _ptg2_context_row(
    plan_fields: dict[str, Any],
    import_month: datetime.date,
    source_version: PTG2SourceVersion | None,
) -> dict[str, Any]:
    payload = {
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "plan": plan_fields,
        "import_month": import_month.isoformat(),
        "source_file_version_id": source_version.source_file_version_id if source_version else None,
    }
    context_hash = semantic_hash(payload, domain="rate_set_context")
    return {
        "context_hash": context_hash,
        "hash_prefix": hash_prefix(context_hash),
        "domain": PTG2_DOMAIN_IN_NETWORK,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }


def _ptg2_plan_rows(
    plan_fields: dict[str, Any],
    snapshot_id: str,
    import_month: datetime.date,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    payload = {
        "plan_id": plan_fields.get("plan_id"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
    }
    plan_hash = semantic_hash(payload, domain="plan")
    plan_row = {
        "plan_hash": plan_hash,
        "hash_prefix": hash_prefix(plan_hash),
        **payload,
        "canonical_payload": _canonicalize_for_json(payload),
        "created_at": _utcnow(),
    }
    alias_rows: list[dict[str, Any]] = []
    for alias_type, alias_value in (("plan_id", payload.get("plan_id")), ("plan_name", payload.get("plan_name"))):
        if not alias_value:
            continue
        alias_payload = {"plan_hash": plan_hash, "alias_type": alias_type, "alias_value": str(alias_value)}
        alias_hash = semantic_hash(alias_payload, domain="plan_alias")
        alias_rows.append(
            {
                "alias_hash": alias_hash,
                "plan_hash": plan_hash,
                "alias_type": alias_type,
                "alias_value": str(alias_value),
                "created_at": _utcnow(),
            }
        )
    plan_month_payload = {"snapshot_id": snapshot_id, "plan_hash": plan_hash, "import_month": import_month.isoformat()}
    plan_month_id = semantic_hash(plan_month_payload, domain="plan_month")[:32]
    plan_month_row = {
        "plan_month_id": plan_month_id,
        "snapshot_id": snapshot_id,
        "plan_hash": plan_hash,
        "import_month": import_month,
        "created_at": _utcnow(),
    }
    return plan_row, alias_rows, plan_month_row


def _artifact_progress_position(stream: Any) -> int | None:
    raw_stream = getattr(getattr(stream, "raw", None), "fileobj", None)
    if raw_stream is None:
        raw_stream = getattr(stream, "fileobj", None)
    if raw_stream is not None and hasattr(raw_stream, "tell"):
        try:
            return int(raw_stream.tell())
        except (OSError, TypeError, ValueError):
            pass
    if hasattr(stream, "tell"):
        try:
            return int(stream.tell())
        except (OSError, TypeError, ValueError):
            return None
    return None


def _format_duration(seconds: float | None) -> str:
    if seconds is None or seconds < 0:
        return "unknown"
    seconds_int = int(seconds)
    hours, remainder = divmod(seconds_int, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def _maybe_log_artifact_progress(
    path: str | Path,
    stream: Any,
    state: dict[str, Any],
    label: str,
    *,
    ref_count: int = 0,
    item_count: int = 0,
) -> None:
    interval = max(_env_int(PTG2_PROGRESS_INTERVAL_SECONDS_ENV, 30), 1)
    now = time.monotonic()
    if state.get("last_log") and now - state["last_log"] < interval:
        return
    position = _artifact_progress_position(stream)
    if position is None:
        return
    try:
        total = Path(path).stat().st_size
    except OSError:
        total = 0
    if total <= 0:
        return
    position = min(position, total)
    elapsed = now - state.setdefault("started_at", now)
    rate = position / elapsed if elapsed > 0 else None
    compressed_eta = (total - position) / rate if rate and rate > 0 else None
    compressed_pct = (position / total) * 100
    expected_items = max(_env_int(PTG2_EXPECTED_IN_NETWORK_ITEMS_ENV, 0), 0)
    item_parts = ""
    if item_count > 0:
        item_started_at = state.setdefault("first_item_at", state.get("started_at", now))
        item_elapsed = max(now - item_started_at, 1e-6)
        item_rate = item_count / item_elapsed
        item_parts = f", item_rate={item_rate:.2f}/s"
        if expected_items > 0:
            remaining_items = max(expected_items - item_count, 0)
            item_eta = remaining_items / item_rate if item_rate > 0 else None
            item_pct = min((item_count / expected_items) * 100, 100)
            item_parts += (
                f", item_progress={item_pct:.2f}% "
                f"({item_count}/{expected_items}), item_eta={_format_duration(item_eta)}"
            )
    message = (
        f"PTG2 progress {label}: compressed_read={compressed_pct:.2f}% "
        f"({position / (1024 ** 3):.2f}/{total / (1024 ** 3):.2f} GiB), "
        f"provider_refs={ref_count}, in_network_items={item_count}, "
        f"elapsed={_format_duration(elapsed)}, compressed_eta={_format_duration(compressed_eta)}"
        f"{item_parts}"
    )
    print(message, flush=True)
    logger.info(message)
    state["last_log"] = now


def _normalize_provider_ref(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _dedupe_preserve(seq: list[str]) -> list[str]:
    seen = set()
    out = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _dedupe_rows_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return rows
    seen: dict[Any, dict[str, Any]] = {}
    missing: list[dict[str, Any]] = []
    for row in rows:
        value = row.get(key)
        if value is None:
            missing.append(row)
        else:
            seen[value] = row
    return list(seen.values()) + missing


def _plan_matches_filters(
    plan: dict[str, Any],
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
) -> bool:
    normalized_ids = _normalize_filter_values(plan_ids)
    normalized_name_terms = _normalize_filter_values(plan_name_contains)
    normalized_market_types = _normalize_filter_values(plan_market_types)

    if normalized_ids and str(plan.get("plan_id") or "").strip().lower() not in normalized_ids:
        return False

    if normalized_market_types and str(plan.get("plan_market_type") or "").strip().lower() not in normalized_market_types:
        return False

    if normalized_name_terms:
        searchable = " ".join(
            str(plan.get(key) or "")
            for key in ("plan_name", "plan_sponsor_name", "plan_sponser_name", "issuer_name", "reporting_entity_name")
        ).lower()
        if not any(term in searchable for term in normalized_name_terms):
            return False

    return True


def _filter_reporting_plans(
    plans: list[dict[str, Any]],
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    if not any((plan_ids, plan_name_contains, plan_market_types)):
        return plans
    return [
        plan
        for plan in plans
        if _plan_matches_filters(plan, plan_ids, plan_name_contains, plan_market_types)
    ]


def _load_toc_urls_from_file(path: str) -> list[str]:
    urls: list[str] = []
    try:
        text = Path(path).read_text(encoding="utf-8")
    except OSError:
        return urls
    text_strip = text.strip()
    if not text_strip:
        return urls
    if text_strip.startswith("["):
        try:
            data = json.loads(text_strip)
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
            elif isinstance(data, dict):
                for entry in data.values():
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
                    elif isinstance(entry, list):
                        urls.extend([str(v).strip() for v in entry if str(v).strip()])
        except json.JSONDecodeError:
            pass
    else:
        for line in text.splitlines():
            line = line.strip()
            if line:
                urls.append(line)
    return _dedupe_preserve(urls)


def _filter_jobs_by_url_contains(jobs: list[dict[str, Any]], filters: list[str] | None) -> list[dict[str, Any]]:
    needles = [str(value).strip().lower() for value in filters or [] if str(value).strip()]
    if not needles:
        return jobs
    filtered: list[dict[str, Any]] = []
    for job in jobs:
        haystack = " ".join(
            str(value or "")
            for value in (
                job.get("url"),
                job.get("description"),
                job.get("from_index_url"),
            )
        ).lower()
        if any(needle in haystack for needle in needles):
            filtered.append(job)
    return filtered


async def _ensure_indexes(obj, db_schema: str) -> None:
    if _env_bool(
        PTG2_SKIP_BULK_INDEX_ENSURE_ENV,
        _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, False),
    ) and obj in {
        PTG2PriceSet,
        PTG2ProviderSet,
        PTG2Procedure,
        PTG2ServingRateCompact,
    }:
        logger.info("Skipping PTG2 bulk index ensure for %s before bulk load", obj.__tablename__)
        return
    if (
        obj is PTG2ServingRateCompact
        and _env_bool(
            PTG2_SKIP_COMPACT_SERVING_INDEX_ENSURE_ENV,
            _env_bool(PTG2_COMPACT_BULK_DROP_INDEXES_ENV, True),
        )
    ):
        logger.info("Skipping PTG2 compact serving index ensure before bulk load")
        return
    if hasattr(obj, "__my_index_elements__") and obj.__my_index_elements__:
        cols = ", ".join(obj.__my_index_elements__)
        await db.status(
            "CREATE UNIQUE INDEX IF NOT EXISTS "
            + f"{obj.__tablename__}_idx_primary ON {db_schema}.{obj.__tablename__} ({cols});"
        )
    if hasattr(obj, "__my_additional_indexes__") and obj.__my_additional_indexes__:
        for idx in obj.__my_additional_indexes__:
            elements = idx.get("index_elements")
            if not elements:
                continue
            name = idx.get("name") or f"{obj.__tablename__}_{'_'.join(elements)}_idx"
            using = idx.get("using")
            where = idx.get("where")
            cols = ", ".join(elements)
            statement = f"CREATE INDEX IF NOT EXISTS {name} ON {db_schema}.{obj.__tablename__}"
            if using:
                statement += f" USING {using}"
            statement += f" ({cols})"
            if where:
                statement += f" WHERE {where}"
            statement += ";"
            await db.status(statement)


async def _ensure_ptg2_serving_rate_columns(db_schema: str) -> None:
    column_specs = {
        "procedure_code": "bigint",
        "reported_code_system": "varchar(64)",
        "reported_code": "varchar(64)",
        "procedure_display_name": "varchar",
        "source_trace_set_hash": "varchar(64)",
        "confidence_code": "varchar(64)",
    }
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate column %s ensure: %s", column_name, exc)


async def _ensure_ptg2_provider_set_columns(db_schema: str) -> None:
    try:
        await db.status(
            f"ALTER TABLE {db_schema}.ptg2_provider_set "
            "ADD COLUMN IF NOT EXISTS provider_group_hashes bigint[];"
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_provider_set provider_group_hashes ensure: %s", exc)


async def _ensure_ptg2_price_set_columns(db_schema: str) -> None:
    column_specs = {
        "negotiated_type": "varchar(64)",
        "negotiated_rate": "numeric",
        "expiration_date": "date",
        "service_code": "varchar[]",
        "billing_class": "varchar(32)",
        "setting": "varchar(32)",
        "billing_code_modifier": "varchar[]",
        "additional_information": "varchar",
    }
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_price_set "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_price_set column %s ensure: %s", column_name, exc)


async def _ensure_ptg2_price_set_stage_table(db_schema: str) -> None:
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    await db.status(
        f"""
        CREATE {storage_mode}TABLE IF NOT EXISTS {db_schema}.ptg2_price_set_stage (
            snapshot_id varchar(96) NOT NULL,
            price_set_hash varchar(64) NOT NULL,
            hash_prefix varchar(16),
            price_atom_hashes varchar[],
            canonical_payload json,
            created_at timestamp
        );
        """
    )
    if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
        try:
            await db.status(f"ALTER TABLE {db_schema}.ptg2_price_set_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_price_set_stage unlogged ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        await db.status(
            f"""
            CREATE INDEX IF NOT EXISTS ptg2_price_set_stage_snapshot_idx
            ON {db_schema}.ptg2_price_set_stage (snapshot_id, price_set_hash);
            """
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_price_set_stage index ensure: %s", exc)


async def _ensure_ptg2_serving_rate_stage_table(db_schema: str) -> None:
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    await db.status(
        f"""
        CREATE {storage_mode}TABLE IF NOT EXISTS {db_schema}.ptg2_serving_rate_stage (
            snapshot_id varchar(96) NOT NULL,
            serving_rate_id varchar(64) NOT NULL,
            canonical_payload json,
            plan_id varchar(64),
            plan_name varchar,
            plan_id_type varchar(32),
            plan_market_type varchar(32),
            issuer_name varchar,
            plan_sponsor_name varchar,
            procedure_code bigint,
            reported_code_system varchar(64),
            reported_code varchar(64),
            billing_code varchar(64),
            billing_code_type varchar(64),
            procedure_name varchar,
            procedure_description varchar,
            procedure_display_name varchar,
            rate_pack_hash varchar(64),
            provider_set_hash varchar(64),
            provider_set_hashes varchar[],
            provider_count integer,
            provider_set_count integer,
            price_set_hash varchar(64),
            source_trace_set_hash varchar(64),
            confidence_code varchar(64),
            prices json,
            source_trace json,
            confidence json,
            created_at timestamp
        );
        """
    )
    if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True):
        try:
            await db.status(f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage SET UNLOGGED;")
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage unlogged ensure: %s", exc)
    column_specs = {
        "canonical_payload": "json",
        "plan_id": "varchar(64)",
        "plan_name": "varchar",
        "plan_id_type": "varchar(32)",
        "plan_market_type": "varchar(32)",
        "issuer_name": "varchar",
        "plan_sponsor_name": "varchar",
        "procedure_code": "bigint",
        "reported_code_system": "varchar(64)",
        "reported_code": "varchar(64)",
        "billing_code": "varchar(64)",
        "billing_code_type": "varchar(64)",
        "procedure_name": "varchar",
        "procedure_description": "varchar",
        "procedure_display_name": "varchar",
        "rate_pack_hash": "varchar(64)",
        "provider_set_hash": "varchar(64)",
        "provider_set_hashes": "varchar[]",
        "provider_count": "integer",
        "provider_set_count": "integer",
        "price_set_hash": "varchar(64)",
        "source_trace_set_hash": "varchar(64)",
        "confidence_code": "varchar(64)",
        "prices": "json",
        "source_trace": "json",
        "confidence": "json",
    }
    for column_name, column_type in column_specs.items():
        try:
            await db.status(
                f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
                f"ADD COLUMN IF NOT EXISTS {column_name} {column_type};"
            )
        except Exception as exc:
            logger.debug("Skipping ptg2_serving_rate_stage column %s ensure: %s", column_name, exc)
    try:
        await db.status(
            f"ALTER TABLE {db_schema}.ptg2_serving_rate_stage "
            "ALTER COLUMN canonical_payload DROP NOT NULL;"
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage canonical_payload nullable ensure: %s", exc)
    if not _env_bool(PTG2_STAGE_INDEXES_ENV, False):
        return
    try:
        await db.status(
            f"""
            CREATE INDEX IF NOT EXISTS ptg2_serving_rate_stage_snapshot_idx
            ON {db_schema}.ptg2_serving_rate_stage (snapshot_id, serving_rate_id);
            """
        )
    except Exception as exc:
        logger.debug("Skipping ptg2_serving_rate_stage index ensure: %s", exc)


async def ensure_ptg2_tables() -> None:
    db_schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        raise RuntimeError(f"Failed to ensure PTG2 schema {db_schema}: {exc}") from exc
    for cls in PTG2_MODEL_CLASSES:
        try:
            await db.create_table(cls.__table__, checkfirst=True)
        except Exception as exc:
            raise RuntimeError(f"PTG2 create table {db_schema}.{cls.__tablename__} failed: {exc}") from exc
        if cls is PTG2ServingRate:
            await _ensure_ptg2_serving_rate_columns(db_schema)
        if cls is PTG2PriceSet:
            await _ensure_ptg2_price_set_columns(db_schema)
        if cls is PTG2ProviderSet:
            await _ensure_ptg2_provider_set_columns(db_schema)
        await _ensure_indexes(cls, db_schema)
    await _ensure_ptg2_price_set_stage_table(db_schema)
    await _ensure_ptg2_serving_rate_stage_table(db_schema)


async def _prepare_ptg_tables(import_id: str, test_mode: bool) -> dict[str, type]:
    db_schema = get_import_schema("HLTHPRT_DB_SCHEMA", "mrf", test_mode)
    try:
        await db.status(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    except Exception as exc:
        logger.warning("Failed to ensure schema %s exists (%s); falling back to public schema", db_schema, exc)
        db_schema = "public"
    dynamic: dict[str, type] = {}
    for cls in (
        PTGFile,
        PTGProviderGroup,
        PTGInNetworkItem,
        PTGBillingCode,
        PTGNegotiatedRate,
        PTGNegotiatedPrice,
        PTGAllowedItem,
        PTGAllowedPayment,
        PTGAllowedProviderPayment,
        ImportLog,
    ):
        obj = make_class(cls, import_id, schema_override=db_schema)
        dynamic[cls.__name__] = obj
        try:
            await db.status(f"DROP TABLE IF EXISTS {db_schema}.{obj.__tablename__};")
        except Exception as exc:
            logger.debug("PTG drop table %s failed: %s", obj.__tablename__, exc)
        try:
            await db.create_table(obj.__table__, checkfirst=True)
        except Exception as exc:
            logger.warning("PTG create table %s failed: %s", obj.__tablename__, exc)
        await _ensure_indexes(obj, db_schema)
    return dynamic


def _maybe_unzip(path: str) -> str:
    logical = stream_logical_artifact(path, output_dir=os.path.dirname(path))
    if logical.logical_path != path:
        return logical.logical_path
    return path


async def _extract_metadata_fields(file_path: str) -> dict[str, Any]:
    """
    Extract top-level metadata without loading the full file.
    """
    fields = {
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id_type",
        "plan_id",
        "plan_market_type",
        "issuer_name",
        "plan_sponsor_name",
        "last_updated_on",
        "version",
    }
    meta: dict[str, Any] = {}
    with open_json_artifact_stream(file_path) as afp:
        for prefix, event, value in ijson.parse(afp):
            if event in ("string", "number") and prefix in fields:
                meta[prefix] = value
                if len(meta) == len(fields):
                    break
    return meta


def _derive_plan_fields(meta: dict[str, Any], plan_info: list[dict[str, Any]] | None) -> dict[str, Any]:
    if any(meta.get(k) for k in ("plan_name", "plan_id", "plan_market_type", "plan_sponsor_name", "issuer_name")):
        return {
            "plan_name": meta.get("plan_name"),
            "plan_id_type": meta.get("plan_id_type"),
            "plan_id": meta.get("plan_id"),
            "plan_market_type": meta.get("plan_market_type"),
            "issuer_name": meta.get("issuer_name"),
            "plan_sponsor_name": meta.get("plan_sponsor_name"),
        }
    if plan_info:
        if len(plan_info) == 1:
            single = _normalize_plan_payload(plan_info[0])
            return {
                "plan_name": single.get("plan_name"),
                "plan_id_type": single.get("plan_id_type"),
                "plan_id": single.get("plan_id"),
                "plan_market_type": single.get("plan_market_type"),
                "issuer_name": single.get("issuer_name"),
                "plan_sponsor_name": single.get("plan_sponsor_name"),
            }
    return {}


def _build_file_row(
    url: str,
    file_type: str,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    description: str | None,
    from_index_url: str | None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    file_id = _make_checksum(url, file_type, plan_fields.get("plan_id") or plan_fields.get("plan_name") or "")
    return {
        "file_id": file_id,
        "file_type": file_type,
        "url": url,
        "description": description,
        "reporting_entity_name": meta.get("reporting_entity_name"),
        "reporting_entity_type": meta.get("reporting_entity_type"),
        "last_updated_on": _coerce_date(meta.get("last_updated_on")),
        "version": meta.get("version"),
        "plan_name": plan_fields.get("plan_name"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_id": plan_fields.get("plan_id"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
        "from_index_url": from_index_url,
    }


class PTG2ProviderReferenceCache:
    def __init__(self, path: str | Path, initial: dict[Any, list[dict[str, Any]]] | None = None):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=OFF")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS provider_ref (provider_group_ref TEXT PRIMARY KEY, groups_json TEXT NOT NULL)"
        )
        self.provider_hashes: set[int] = set()
        self.memory_limit = max(_env_int(PTG2_PROVIDER_CACHE_MEMORY_REFS_ENV, 65536), 0)
        self.memory_cache: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
        self.get_count = 0
        self.sqlite_hit_count = 0
        self.memory_hit_count = 0
        self.miss_count = 0
        if initial:
            for ref, groups in initial.items():
                self.put(ref, groups)

    @staticmethod
    def key(ref: Any) -> str:
        return str(_normalize_provider_ref(ref))

    def put(self, ref: Any, groups: list[dict[str, Any]]) -> None:
        if ref is None or not groups:
            return
        cache_key = self.key(ref)
        for group in groups:
            if isinstance(group, dict) and "__hash__" in group:
                self.provider_hashes.add(group["__hash__"])
        self._remember(cache_key, groups)
        payload = json.dumps(groups, sort_keys=True, default=_json_default)
        self.conn.execute(
            "INSERT OR REPLACE INTO provider_ref (provider_group_ref, groups_json) VALUES (?, ?)",
            (cache_key, payload),
        )

    def get(self, ref: Any) -> list[dict[str, Any]]:
        if ref is None:
            return []
        self.get_count += 1
        cache_key = self.key(ref)
        cached = self.memory_cache.get(cache_key)
        if cached is not None:
            self.memory_hit_count += 1
            self.memory_cache.move_to_end(cache_key)
            return cached
        row = self.conn.execute(
            "SELECT groups_json FROM provider_ref WHERE provider_group_ref = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            self.miss_count += 1
            return []
        groups = json.loads(row[0])
        self.sqlite_hit_count += 1
        self._remember(cache_key, groups)
        return groups

    def _remember(self, cache_key: str, groups: list[dict[str, Any]]) -> None:
        if self.memory_limit <= 0:
            return
        self.memory_cache[cache_key] = groups
        self.memory_cache.move_to_end(cache_key)
        while len(self.memory_cache) > self.memory_limit:
            self.memory_cache.popitem(last=False)

    def stats(self) -> dict[str, int]:
        return {
            "provider_cache_gets": self.get_count,
            "provider_cache_memory_hits": self.memory_hit_count,
            "provider_cache_sqlite_hits": self.sqlite_hit_count,
            "provider_cache_misses": self.miss_count,
            "provider_cache_memory_size": len(self.memory_cache),
            "provider_cache_memory_limit": self.memory_limit,
        }

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


class PTG2InMemoryProviderReferenceCache:
    def __init__(self, initial: dict[Any, list[dict[str, Any]]] | None = None):
        self.refs: dict[str, list[dict[str, Any]]] = {}
        self.provider_hashes: set[int] = set()
        self.get_count = 0
        self.hit_count = 0
        self.miss_count = 0
        if initial:
            for ref, groups in initial.items():
                self.put(ref, groups)

    @staticmethod
    def key(ref: Any) -> str:
        return str(_normalize_provider_ref(ref))

    def put(self, ref: Any, groups: list[dict[str, Any]]) -> None:
        if ref is None or not groups:
            return
        cache_key = self.key(ref)
        for group in groups:
            if isinstance(group, dict) and "__hash__" in group:
                self.provider_hashes.add(group["__hash__"])
        self.refs[cache_key] = groups

    def get(self, ref: Any) -> list[dict[str, Any]]:
        if ref is None:
            return []
        self.get_count += 1
        groups = self.refs.get(self.key(ref))
        if groups is None:
            self.miss_count += 1
            return []
        self.hit_count += 1
        return groups

    def stats(self) -> dict[str, int]:
        return {
            "provider_cache_gets": self.get_count,
            "provider_cache_memory_hits": self.hit_count,
            "provider_cache_sqlite_hits": 0,
            "provider_cache_misses": self.miss_count,
            "provider_cache_memory_size": len(self.refs),
            "provider_cache_memory_limit": len(self.refs),
        }

    def commit(self) -> None:
        return None

    def close(self) -> None:
        return None


def _provider_cache_get(provider_cache: Any, ref: Any) -> list[dict[str, Any]]:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        return provider_cache.get(ref)
    return provider_cache.get(ref) or provider_cache.get(_normalize_provider_ref(ref)) or []


def _provider_cache_put(provider_cache: Any, ref: Any, groups: list[dict[str, Any]]) -> None:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        provider_cache.put(ref, groups)
    else:
        provider_cache.setdefault(ref, []).extend(groups)


def _provider_combo_cache_key(provider_refs: list[Any] | tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(sorted({str(_normalize_provider_ref(ref)) for ref in provider_refs if ref is not None}))


def _provider_combo_cache_get(
    cache: OrderedDict[tuple[str, ...], dict[str, Any]],
    key: tuple[str, ...],
    stats: dict[str, int],
) -> dict[str, Any] | None:
    stats["provider_combo_cache_gets"] += 1
    cached = cache.get(key)
    if cached is None:
        stats["provider_combo_cache_misses"] += 1
        return None
    stats["provider_combo_cache_hits"] += 1
    cache.move_to_end(key)
    return cached


def _provider_combo_cache_put(
    cache: OrderedDict[tuple[str, ...], dict[str, Any]],
    key: tuple[str, ...],
    value: dict[str, Any],
    stats: dict[str, int],
    limit: int,
) -> None:
    if limit <= 0 or not key:
        return
    cache[key] = value
    cache.move_to_end(key)
    while len(cache) > limit:
        cache.popitem(last=False)
    stats["provider_combo_cache_size"] = len(cache)
    stats["provider_combo_cache_limit"] = limit


def _provider_cache_hashes(provider_cache: Any) -> set[int]:
    if isinstance(provider_cache, (PTG2ProviderReferenceCache, PTG2InMemoryProviderReferenceCache)):
        return set(provider_cache.provider_hashes)
    return {
        entry["__hash__"]
        for groups in provider_cache.values()
        for entry in groups
        if isinstance(entry, dict) and "__hash__" in entry
    }


def _iter_top_level_objects(
    file_obj,
    item_prefixes: dict[str, str],
    use_float: bool = True,
    progress_callback=None,
):
    active_name = None
    active_prefix = None
    builder = None
    event_count = 0
    for prefix, event, value in ijson.parse(file_obj, use_float=use_float):
        event_count += 1
        if progress_callback is not None and event_count % 100000 == 0:
            progress_callback()
        if builder is not None:
            builder.event(event, value)
            if prefix == active_prefix and event in {"end_map", "end_array"}:
                yield active_name, builder.value
                active_name = None
                active_prefix = None
                builder = None
            continue
        if event not in {"start_map", "start_array"}:
            continue
        for name, item_prefix in item_prefixes.items():
            if prefix == item_prefix:
                active_name = name
                active_prefix = item_prefix
                builder = ObjectBuilder()
                builder.event(event, value)
                break


def _iter_top_level_object_bytes(
    file_obj,
    array_names: set[str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
):
    """
    Yield raw JSON object bytes from selected top-level arrays.

    This avoids ijson's Python ObjectBuilder for very large TiC objects while
    preserving streaming gzip reads. The scanner only recognizes top-level
    object keys and then captures complete object values inside selected arrays.
    """
    chunk_size = chunk_size or _stream_buffer_bytes()
    targets = {name.encode("utf-8"): name for name in array_names}
    depth = 0
    active_name: str | None = None
    active_array_depth = 0
    capture = bytearray()
    capture_depth = 0
    in_string = False
    escape = False
    string_buffer: bytearray | None = None
    candidate_key: bytes | None = None
    pending_key: bytes | None = None
    bytes_since_progress = 0

    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        bytes_since_progress += len(chunk)
        if progress_callback is not None and bytes_since_progress >= 64 * 1024 * 1024:
            progress_callback()
            bytes_since_progress = 0
        for byte in chunk:
            char = byte
            if capture_depth:
                capture.append(char)
            if in_string:
                if string_buffer is not None:
                    string_buffer.append(char)
                if escape:
                    escape = False
                elif char == 0x5C:  # backslash
                    escape = True
                elif char == 0x22:  # quote
                    in_string = False
                    if string_buffer is not None:
                        candidate_key = bytes(string_buffer[:-1])
                        string_buffer = None
                continue

            if char == 0x22:  # quote
                in_string = True
                escape = False
                if depth == 1 and not active_name and not capture_depth:
                    string_buffer = bytearray()
                else:
                    string_buffer = None
                continue

            if candidate_key is not None:
                if char in (0x20, 0x09, 0x0A, 0x0D):
                    continue
                if char == 0x3A:  # colon
                    pending_key = candidate_key
                candidate_key = None
                if char == 0x3A:
                    continue

            if pending_key is not None:
                if char in (0x20, 0x09, 0x0A, 0x0D):
                    continue
                if char == 0x5B and pending_key in targets and depth == 1:  # [
                    depth += 1
                    active_name = targets[pending_key]
                    active_array_depth = depth
                    pending_key = None
                    continue
                pending_key = None

            if active_name and not capture_depth and char == 0x7B and depth == active_array_depth:  # {
                capture = bytearray(b"{")
                capture_depth = 1
                depth += 1
                continue

            if char in (0x7B, 0x5B):  # { [
                if capture_depth:
                    capture_depth += 1
                    depth += 1
                    continue
                depth += 1
                continue
            if char in (0x7D, 0x5D):  # } ]
                if capture_depth:
                    capture_depth -= 1
                    if capture_depth == 0:
                        yield active_name, bytes(capture)
                        capture = bytearray()
                        depth -= 1
                        continue
                if active_name and char == 0x5D and depth == active_array_depth:
                    active_name = None
                    active_array_depth = 0
                depth = max(depth - 1, 0)


def _ptg2_rust_scanner_binary() -> Path | None:
    configured = os.getenv(PTG2_RUST_SCANNER_BIN_ENV)
    candidates = []
    if configured:
        candidates.append(Path(configured))
    root = Path(__file__).resolve().parents[1]
    candidates.extend(
        [
            root / "support" / "ptg2_scanner" / "target" / "release" / "ptg2_scanner",
            root / "support" / "ptg2_scanner" / "target" / "debug" / "ptg2_scanner",
        ]
    )
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _iter_top_level_object_bytes_rust(
    path: str | Path,
    array_names: set[str],
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust scanner is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    command = [str(binary)]
    if _env_bool(PTG2_RUST_SPLIT_IN_NETWORK_ENV, False) and "in_network" in array_names:
        command.append("--split-in-network")
    command.extend([str(path), *sorted(array_names)])
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    stderr_tail: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:
        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_tail.append(line)
                if len(stderr_tail) > 20:
                    del stderr_tail[:-20]
                if line.startswith("PTG2_SCANNER_PROGRESS\t"):
                    print(line, flush=True)
                    logger.info(line)
                else:
                    logger.warning("PTG2 Rust scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-scanner-stderr",
            daemon=True,
        )
        stderr_thread.start()
    terminated_by_consumer = False
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust scanner frame header: {header!r}") from exc
            payload = process.stdout.read(payload_len)
            if len(payload) != payload_len:
                raise RuntimeError("PTG2 Rust scanner ended mid-frame")
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust scanner frame trailer")
            yield name_bytes.decode("utf-8"), payload
    finally:
        if process.poll() is None:
            terminated_by_consumer = True
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join(timeout=2)
        if return_code != 0 and not terminated_by_consumer:
            raise RuntimeError(
                f"PTG2 Rust scanner failed with exit code {return_code}: "
                f"{chr(10).join(stderr_tail)[-1000:]}"
            )


def _iter_compact_serving_records_rust(
    path: str | Path,
    *,
    snapshot_id: str,
    plan_id: str,
    plan_month_id: str,
    source_trace_set_hash: str,
    confidence_code: str = PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
    compact_copy_path: str | Path | None = None,
    compact_copy_stdout: bool = False,
    procedure_copy_path: str | Path | None = None,
    price_set_copy_path: str | Path | None = None,
    provider_set_copy_path: str | Path | None = None,
    provider_group_member_copy_path: str | Path | None = None,
):
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError(
            "PTG2 Rust compact serving is enabled but no scanner binary was found; "
            "build it with `cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml`"
        )
    env = {
        **os.environ,
        "HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID": snapshot_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_ID": plan_id,
        "HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID": plan_month_id,
        "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH": source_trace_set_hash,
        "HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE": confidence_code,
    }
    if compact_copy_stdout:
        env["HLTHPRT_PTG2_COMPACT_SERVING_COPY_STDOUT"] = "true"
    if compact_copy_path is not None:
        env["HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH"] = str(compact_copy_path)
    if procedure_copy_path is not None:
        env["HLTHPRT_PTG2_PROCEDURE_COPY_PATH"] = str(procedure_copy_path)
    if price_set_copy_path is not None:
        env["HLTHPRT_PTG2_PRICE_SET_COPY_PATH"] = str(price_set_copy_path)
    if provider_set_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_SET_COPY_PATH"] = str(provider_set_copy_path)
    if provider_group_member_copy_path is not None:
        env["HLTHPRT_PTG2_PROVIDER_GROUP_MEMBER_COPY_PATH"] = str(provider_group_member_copy_path)
    process = subprocess.Popen(
        [str(binary), "--compact-serving", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    assert process.stdout is not None
    stderr_tail: list[str] = []
    stderr_thread: threading.Thread | None = None
    if process.stderr is not None:
        def _read_scanner_stderr() -> None:
            assert process.stderr is not None
            for raw_line in iter(process.stderr.readline, b""):
                line = raw_line.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                stderr_tail.append(line)
                if len(stderr_tail) > 20:
                    del stderr_tail[:-20]
                if line.startswith("PTG2_SCANNER_PROGRESS\t"):
                    print(line, flush=True)
                    logger.info(line)
                else:
                    logger.warning("PTG2 Rust compact scanner stderr: %s", line)

        stderr_thread = threading.Thread(
            target=_read_scanner_stderr,
            name="ptg2-rust-compact-stderr",
            daemon=True,
        )
        stderr_thread.start()
    terminated_by_consumer = False
    try:
        while True:
            header = process.stdout.readline()
            if not header:
                break
            try:
                name_bytes, length_bytes = header.rstrip(b"\n").split(b"\t", 1)
                payload_len = int(length_bytes)
            except Exception as exc:
                raise RuntimeError(f"Invalid PTG2 Rust compact frame header: {header!r}") from exc
            payload = process.stdout.read(payload_len)
            if len(payload) != payload_len:
                raise RuntimeError("PTG2 Rust compact scanner ended mid-frame")
            trailer = process.stdout.read(1)
            if trailer not in {b"", b"\n"}:
                raise RuntimeError("Invalid PTG2 Rust compact scanner frame trailer")
            record_kind = name_bytes.decode("utf-8")
            if record_kind == "compact_copy_data":
                yield record_kind, payload
            else:
                yield record_kind, _json_loads(payload)
    finally:
        if process.poll() is None:
            terminated_by_consumer = True
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
        return_code = process.wait()
        if stderr_thread is not None:
            stderr_thread.join(timeout=2)
        if return_code != 0 and not terminated_by_consumer:
            raise RuntimeError(
                f"PTG2 Rust compact scanner failed with exit code {return_code}: "
                f"{chr(10).join(stderr_tail)[-1000:]}"
            )


def _skip_json_ws(buffer: str, pos: int) -> int:
    while pos < len(buffer) and buffer[pos] in " \t\r\n":
        pos += 1
    return pos


def _iter_top_level_objects_jsondecoder(
    file_obj,
    item_prefixes: dict[str, str],
    *,
    progress_callback=None,
    chunk_size: int | None = None,
    raw_object_names: set[str] | None = None,
):
    """
    Stream selected top-level array objects with JSONDecoder.raw_decode.

    The older byte scanner touched every decompressed byte in Python. This
    iterator still keeps only a bounded buffer, but lets CPython's JSON decoder
    find each object boundary while parsing the object payload.
    """
    chunk_size = chunk_size or _stream_buffer_bytes()
    array_to_name = {
        item_prefix.removesuffix(".item"): name
        for name, item_prefix in item_prefixes.items()
        if item_prefix.endswith(".item")
    }
    if not array_to_name:
        return
    key_tokens = {array_name: f'"{array_name}"' for array_name in array_to_name}
    max_key_len = max(len(token) for token in key_tokens.values())
    utf8_decoder = codecs.getincrementaldecoder("utf-8")()
    json_decoder = json.JSONDecoder()
    buffer = ""
    pos = 0
    eof = False
    active_array: str | None = None
    bytes_since_progress = 0

    def read_more() -> bool:
        nonlocal buffer, eof, bytes_since_progress
        if eof:
            return False
        chunk = file_obj.read(chunk_size)
        if not chunk:
            tail = utf8_decoder.decode(b"", final=True)
            if tail:
                buffer += tail
            eof = True
            return False
        bytes_since_progress += len(chunk)
        if progress_callback is not None and bytes_since_progress >= 64 * 1024 * 1024:
            progress_callback()
            bytes_since_progress = 0
        buffer += utf8_decoder.decode(chunk, final=False)
        return True

    def compact_buffer(force: bool = False) -> None:
        nonlocal buffer, pos
        if pos <= 0:
            return
        if force or pos > chunk_size:
            buffer = buffer[pos:]
            pos = 0

    while True:
        if active_array is None:
            while True:
                matches = [
                    (found_at, array_name, token)
                    for array_name, token in key_tokens.items()
                    for found_at in [buffer.find(token, pos)]
                    if found_at >= 0
                ]
                if matches:
                    found_at, array_name, token = min(matches, key=lambda item: item[0])
                    scan = _skip_json_ws(buffer, found_at + len(token))
                    while True:
                        if scan >= len(buffer):
                            if not read_more():
                                return
                            continue
                        if buffer[scan] != ":":
                            pos = found_at + len(token)
                            break
                        scan = _skip_json_ws(buffer, scan + 1)
                        while scan >= len(buffer):
                            if not read_more():
                                return
                            scan = _skip_json_ws(buffer, scan)
                        if buffer[scan] != "[":
                            pos = scan + 1
                            break
                        active_array = array_name
                        pos = scan + 1
                        compact_buffer(force=True)
                        break
                    if active_array is not None:
                        break
                    continue
                if eof:
                    return
                keep_from = max(len(buffer) - (max_key_len + 32), 0)
                if keep_from:
                    buffer = buffer[keep_from:]
                    pos = 0
                if not read_more() and eof:
                    return

        pos = _skip_json_ws(buffer, pos)
        while pos >= len(buffer):
            if not read_more():
                return
            pos = _skip_json_ws(buffer, pos)

        if buffer[pos] == ",":
            pos += 1
            continue
        if buffer[pos] == "]":
            pos += 1
            active_array = None
            compact_buffer(force=True)
            continue

        start_pos = pos
        try:
            payload, end_pos = json_decoder.raw_decode(buffer, pos)
        except json.JSONDecodeError:
            if eof:
                raise
            read_more()
            continue
        object_name = array_to_name[active_array]
        if raw_object_names and object_name in raw_object_names:
            yield object_name, buffer[start_pos:end_pos].encode("utf-8")
            del payload
        else:
            yield object_name, payload
        pos = end_pos
        compact_buffer()


def _iter_top_level_objects_fast(
    file_obj,
    item_prefixes: dict[str, str],
    *,
    use_float: bool = True,
    progress_callback=None,
):
    if _env_bool(PTG2_JSON_DECODER_ITERATOR_ENV, True):
        yield from _iter_top_level_objects_jsondecoder(
            file_obj,
            item_prefixes,
            progress_callback=progress_callback,
        )
        return
    array_names = {
        item_prefix.removesuffix(".item")
        for item_prefix in item_prefixes.values()
        if item_prefix.endswith(".item")
    }
    prefix_to_name = {item_prefix.removesuffix(".item"): name for name, item_prefix in item_prefixes.items()}
    for array_name, raw_object in _iter_top_level_object_bytes(
        file_obj,
        array_names,
        progress_callback=progress_callback,
    ):
        yield prefix_to_name[array_name], _json_loads(raw_object)


async def _flush_in_network_rows(
    item_rows: list[dict[str, Any]],
    billing_rows: list[dict[str, Any]],
    rate_rows: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    item_cls,
    billing_cls,
    rate_cls,
    price_cls,
    *,
    force: bool = False,
    item_batch_rows: int = 1000,
    billing_batch_rows: int = 5000,
    rate_batch_rows: int = 5000,
    price_batch_rows: int = 10000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if item_rows and (force or len(item_rows) >= item_batch_rows):
        await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
        item_rows = []
    if billing_rows and (force or len(billing_rows) >= billing_batch_rows):
        await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
        billing_rows = []
    if rate_rows and (force or len(rate_rows) >= rate_batch_rows):
        await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
        rate_rows = []
    if price_rows and (force or len(price_rows) >= price_batch_rows):
        await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
        price_rows = []
    return item_rows, billing_rows, rate_rows, price_rows


def _compact_state(batch_rows: int | None = None) -> dict[str, Any]:
    return {
        "batch_rows": batch_rows or max(_env_int(PTG2_COMPACT_BATCH_ROWS_ENV, 5000), 1),
        "seen": {
            "provider_group": set(),
            "provider_group_member": set(),
            "provider_set": set(),
            "provider_set_component": set(),
            "provider_location": set(),
            "procedure": set(),
            "price_atom": set(),
            "price_set": set(),
            "source_trace": set(),
            "source_trace_set": set(),
            "rate_pack": set(),
            "serving_rate_compact": set(),
            "serving_rate": set(),
        },
        "rows": {
            "provider_group": [],
            "provider_group_member": [],
            "provider_set": [],
            "provider_set_component": [],
            "provider_location": [],
            "procedure": [],
            "price_atom": [],
            "price_set": [],
            "source_trace": [],
            "source_trace_set": [],
            "rate_pack": [],
            "serving_rate_compact": [],
            "serving_rate": [],
        },
        "rate_pack_groups": {},
        "chunk_rate_packs": {},
        "procedure_payloads": {},
        "price_payloads": {},
        "provider_set_counts": {},
        "existing_price_set_hashes": None,
        "counts": {
            "provider_groups": 0,
            "provider_group_members": 0,
            "provider_sets": 0,
            "provider_set_components": 0,
            "provider_locations": 0,
            "procedures": 0,
            "price_atoms": 0,
            "price_sets": 0,
            "rate_packs": 0,
            "serving_rates": 0,
        },
        "pending_writes": [],
    }


def _compact_streaming_dedupe_tables() -> set[str]:
    if not _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        return set()
    return {"price_set", "serving_rate"}


async def _existing_price_set_hashes() -> set[str]:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rows = await db.all(f"SELECT price_set_hash FROM {schema_name}.ptg2_price_set")
    return {str(row[0]) for row in rows if row and row[0]}


def _compact_add_unique(state: dict[str, Any], table_key: str, hash_key: str | tuple[str, ...], row: dict[str, Any]) -> bool:
    if table_key in _compact_streaming_dedupe_tables():
        state["rows"][table_key].append(row)
        return True
    if isinstance(hash_key, tuple):
        value = tuple(row.get(key) for key in hash_key)
    else:
        value = row.get(hash_key)
    if not value or value in state["seen"][table_key]:
        return False
    state["seen"][table_key].add(value)
    state["rows"][table_key].append(row)
    return True


def _ptg2_hp_procedure_code(code_system: Any, code: Any) -> int | None:
    normalized_system = _normalize_code_component(code_system)
    normalized_code = _normalize_code_component(code)
    if not normalized_system or not normalized_code:
        return None
    return return_checksum([normalized_system, normalized_code])


def _ptg2_price_atom_payload(price_atom_row: dict[str, Any]) -> dict[str, Any]:
    expiration_date = price_atom_row.get("expiration_date")
    return {
        "negotiated_type": price_atom_row.get("negotiated_type"),
        "negotiated_rate": money_number(price_atom_row.get("negotiated_rate")),
        "expiration_date": expiration_date.isoformat() if isinstance(expiration_date, datetime.date) else expiration_date,
        "service_code": price_atom_row.get("service_code") or [],
        "billing_class": price_atom_row.get("billing_class"),
        "setting": price_atom_row.get("setting"),
        "billing_code_modifier": price_atom_row.get("billing_code_modifier") or [],
        "additional_information": price_atom_row.get("additional_information"),
    }


def _ptg2_source_trace_payload(source_trace_row: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "url": source_trace_row.get("original_url"),
            "canonical_url": source_trace_row.get("canonical_url"),
            "statement": "Published negotiated rate from Transparency in Coverage source file.",
        }
    ]


def _ptg2_serving_rate_row(
    *,
    snapshot_id: str,
    plan_fields: dict[str, Any],
    procedure_payload: dict[str, Any],
    rate_pack_row: dict[str, Any],
    provider_set_hashes: list[str],
    provider_count: int,
    provider_set_count: int,
    prices: list[dict[str, Any]] | None,
    source_trace: list[dict[str, Any]] | None,
    source_trace_set_hash: str | None = None,
    confidence: dict[str, Any] | None = None,
    confidence_code: str | None = None,
) -> dict[str, Any]:
    plan_id = str(plan_fields.get("plan_id") or "")
    billing_code = str(procedure_payload.get("billing_code") or "")
    serving_rate_id = hashlib.md5(
        "|".join(
            [
                snapshot_id,
                plan_id,
                billing_code,
                str(rate_pack_row.get("rate_pack_hash") or ""),
            ]
        ).encode("utf-8")
    ).hexdigest()
    billing_code_type = procedure_payload.get("billing_code_type")
    return {
        "serving_rate_id": serving_rate_id,
        "snapshot_id": snapshot_id,
        "plan_id": plan_id,
        "plan_name": plan_fields.get("plan_name"),
        "plan_id_type": plan_fields.get("plan_id_type"),
        "plan_market_type": plan_fields.get("plan_market_type"),
        "issuer_name": plan_fields.get("issuer_name"),
        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
        "procedure_code": _ptg2_hp_procedure_code(billing_code_type, billing_code),
        "reported_code_system": _normalize_code_component(billing_code_type),
        "reported_code": _normalize_code_component(billing_code),
        "billing_code": billing_code,
        "billing_code_type": billing_code_type,
        "procedure_name": procedure_payload.get("name"),
        "procedure_description": procedure_payload.get("description"),
        "procedure_display_name": procedure_payload.get("name") or procedure_payload.get("description"),
        "rate_pack_hash": rate_pack_row.get("rate_pack_hash"),
        "provider_set_hash": rate_pack_row.get("provider_set_hash"),
        "provider_set_hashes": provider_set_hashes,
        "provider_count": provider_count,
        "provider_set_count": provider_set_count,
        "price_set_hash": rate_pack_row.get("price_set_hash"),
        "source_trace_set_hash": source_trace_set_hash,
        "confidence_code": confidence_code or PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "prices": prices,
        "source_trace": source_trace,
        "confidence": confidence if confidence is not None else {
            "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
            "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
            "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
        },
        "created_at": _utcnow(),
    }


def _ptg2_compact_serving_rate_row(serving_row: dict[str, Any], *, plan_month_id: str, procedure_hash: str) -> dict[str, Any]:
    return {
        "serving_rate_id": serving_row.get("serving_rate_id"),
        "snapshot_id": serving_row.get("snapshot_id"),
        "plan_id": serving_row.get("plan_id"),
        "plan_month_id": plan_month_id,
        "procedure_hash": procedure_hash,
        "procedure_code": serving_row.get("procedure_code"),
        "reported_code_system": serving_row.get("reported_code_system"),
        "reported_code": serving_row.get("reported_code"),
        "billing_code": serving_row.get("billing_code"),
        "billing_code_type": serving_row.get("billing_code_type"),
        "rate_pack_hash": serving_row.get("rate_pack_hash"),
        "provider_set_hash": serving_row.get("provider_set_hash"),
        "provider_count": serving_row.get("provider_count"),
        "price_set_hash": serving_row.get("price_set_hash"),
        "source_trace_set_hash": serving_row.get("source_trace_set_hash"),
        "confidence_code": serving_row.get("confidence_code"),
        "created_at": serving_row.get("created_at") or _utcnow(),
    }


def _provider_group_member_rows(provider_entry: dict[str, Any]) -> list[dict[str, Any]]:
    provider_group_hash = provider_entry.get("__hash__")
    if provider_group_hash is None:
        return []
    rows = []
    for ordinal, npi in enumerate(_normalized_npi_list(provider_entry.get("npi")), start=1):
        rows.append({"provider_group_hash": int(provider_group_hash), "npi": int(npi), "ordinal": ordinal})
    return rows


def _provider_set_component_rows(provider_set_hash: str, provider_group_hashes: list[int] | set[int]) -> list[dict[str, Any]]:
    return [
        {"provider_set_hash": provider_set_hash, "provider_group_hash": int(group_hash), "ordinal": ordinal}
        for ordinal, group_hash in enumerate(sorted({int(value) for value in provider_group_hashes}), start=1)
    ]


async def _flush_compact_rows(state: dict[str, Any], *, force: bool = False) -> None:
    specs = (
        ("provider_group", PTG2ProviderGroup),
        ("provider_group_member", PTG2ProviderGroupMember),
        ("provider_set", PTG2ProviderSet),
        ("provider_set_component", PTG2ProviderSetComponent),
        ("provider_location", PTG2ProviderLocation),
        ("procedure", PTG2Procedure),
        ("price_atom", PTG2PriceAtom),
        ("price_set", PTG2PriceSet),
        ("source_trace", PTG2SourceTrace),
        ("source_trace_set", PTG2SourceTraceSet),
        ("rate_pack", PTG2RatePack),
        ("serving_rate_compact", PTG2ServingRateCompact),
        ("serving_rate", PTG2ServingRate),
    )
    streaming_dedupe_tables = _compact_streaming_dedupe_tables()
    conflict_keys = {
        "provider_group": "provider_group_hash",
        "provider_group_member": ("provider_group_hash", "npi"),
        "provider_set": "provider_set_hash",
        "provider_set_component": ("provider_set_hash", "provider_group_hash"),
        "provider_location": "location_hash",
        "procedure": "procedure_hash",
        "price_atom": "price_atom_hash",
        "price_set": "price_set_hash",
        "source_trace": "source_trace_hash",
        "source_trace_set": "source_trace_set_hash",
        "rate_pack": "rate_pack_hash",
        "serving_rate_compact": "serving_rate_id",
        "serving_rate": "serving_rate_id",
    }
    for table_key, cls in specs:
        rows = state["rows"][table_key]
        if rows and (force or len(rows) >= state["batch_rows"]):
            state["rows"][table_key] = []
            if table_key in streaming_dedupe_tables:
                rows = _dedupe_rows_by(rows, conflict_keys[table_key])
            if (
                table_key == "serving_rate_compact"
                and _use_compact_serving_table()
                and state.get("snapshot_id")
            ):
                await _copy_compact_serving_rate_rows(rows, state["snapshot_id"])
                continue
            if (
                table_key == "price_set"
                and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True)
                and state.get("snapshot_id")
            ):
                await _copy_stage_price_set_rows(rows, state["snapshot_id"])
                continue
            if (
                table_key == "serving_rate"
                and _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True)
                and state.get("snapshot_id")
            ):
                try:
                    await _copy_stage_serving_rate_rows(rows, state["snapshot_id"])
                    continue
                except Exception as exc:
                    logger.warning("PTG2 serving_rate stage fallback to direct write: %s", exc)
            await _schedule_compact_write(state, rows, cls)


async def _schedule_compact_write(state: dict[str, Any], rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    if cls is PTG2PriceSet and _env_bool(PTG2_STREAMING_DEDUPE_ENV, False):
        await _push_ptg2_objects(rows, cls, rewrite=True)
        return
    max_pending = max(_env_int(PTG2_ASYNC_WRITE_TASKS_ENV, 1), 1)
    if max_pending <= 1:
        await _push_ptg2_objects(rows, cls, rewrite=True)
        return
    task = asyncio.create_task(_push_ptg2_objects(rows, cls, rewrite=True))
    pending = state.setdefault("pending_writes", [])
    pending.append(task)
    if len(pending) < max_pending:
        return
    done, waiting = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
    state["pending_writes"] = list(waiting)
    for completed in done:
        await completed


async def _drain_compact_writes(state: dict[str, Any]) -> None:
    pending = state.get("pending_writes") or []
    if not pending:
        return
    state["pending_writes"] = []
    await asyncio.gather(*pending)


async def _flush_compact_rate_pack_groups(state: dict[str, Any], context_hash: str) -> None:
    if not state["rate_pack_groups"]:
        return
    procedure_rate_pack_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for (procedure_hash, price_set_hash, grouped_source_trace_set_hash), provider_set_hashes in state[
        "rate_pack_groups"
    ].items():
        provider_collection = build_provider_set_collection(list(provider_set_hashes))
        procedure_group_key = (
            provider_collection["provider_set_collection_hash"],
            price_set_hash,
            grouped_source_trace_set_hash,
        )
        grouped = procedure_rate_pack_groups.setdefault(
            procedure_group_key,
            {
                "procedure_hashes": set(),
                "provider_set_hashes": set(provider_collection["provider_set_hashes"]),
                "price_set_hash": price_set_hash,
                "source_trace_set_hash": grouped_source_trace_set_hash,
            },
        )
        grouped["procedure_hashes"].add(procedure_hash)

    for grouped in procedure_rate_pack_groups.values():
        procedure_hashes = sorted(grouped["procedure_hashes"])
        rate_pack = build_rate_pack_procedure_group(
            context_hash,
            PTG2_DOMAIN_IN_NETWORK,
            procedure_hashes,
            list(grouped["provider_set_hashes"]),
            grouped["price_set_hash"],
            grouped["source_trace_set_hash"],
        )
        rate_pack_row = {
            **rate_pack,
            "created_at": _utcnow(),
        }
        if _compact_add_unique(state, "rate_pack", "rate_pack_hash", rate_pack_row):
            state["counts"]["rate_packs"] += 1
            provider_bucket = provider_hash_bucket(
                rate_pack_row["provider_set_hash"],
                bucket_count=ptg2_provider_bucket_count(),
            )
            provider_set_hashes = list(grouped["provider_set_hashes"])
            provider_count = sum(int(state["provider_set_counts"].get(provider_hash, 0) or 0) for provider_hash in provider_set_hashes)
            price_payload = _normalize_serving_price_payload(state["price_payloads"].get(grouped["price_set_hash"], []))
            source_trace_payload = state.get("source_trace_payload") or []
            for procedure_hash in procedure_hashes:
                state["chunk_rate_packs"].setdefault((procedure_hash, provider_bucket), set()).add(
                    rate_pack_row["rate_pack_hash"]
                )
                procedure_payload = state["procedure_payloads"].get(procedure_hash)
                if procedure_payload and state.get("snapshot_id") and state.get("plan_fields"):
                    serving_row = _ptg2_serving_rate_row(
                        snapshot_id=state["snapshot_id"],
                        plan_fields=state["plan_fields"],
                        procedure_payload=procedure_payload,
                        rate_pack_row=rate_pack_row,
                        provider_set_hashes=provider_set_hashes,
                        provider_count=provider_count,
                        provider_set_count=len(provider_set_hashes),
                        prices=price_payload,
                        source_trace=source_trace_payload,
                    )
                    if _compact_add_unique(state, "serving_rate", "serving_rate_id", serving_row):
                        state["counts"]["serving_rates"] += 1
        await _flush_compact_rows(state)
    state["rate_pack_groups"] = {}
    await _flush_compact_rows(state, force=True)


def _serving_only_price_payload(negotiated_prices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    payload, _key = _serving_only_price_payload_and_key(negotiated_prices)
    return payload


def _normalize_serving_price_payload(prices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_prices: list[dict[str, Any]] = []
    for negotiated_price in prices:
        raw_rate = negotiated_price.get("negotiated_rate")
        rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
        normalized_rate = normalize_money(rate_value)
        normalized_prices.append(
            {
                **negotiated_price,
                "negotiated_rate": money_number(normalized_rate),
            }
        )
    return normalized_prices


def _serving_only_key_list(value: Any) -> tuple[str, ...]:
    return tuple(sorted(str(item) for item in _as_list(value) if item is not None))


def _serving_only_key_value(value: Any) -> Any:
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return str(value)
    return canonical_json_dumps(value)


def _serving_only_hash_text(domain: str, *parts: Any) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    for part in parts:
        text = "" if part is None else str(part)
        encoded = text.encode("utf-8")
        hasher.update(b"\x1f")
        hasher.update(str(len(encoded)).encode("ascii"))
        hasher.update(b":")
        hasher.update(encoded)
    return hasher.hexdigest()


def _serving_only_hash_int_sets(domain: str, *sets: set[int]) -> str:
    hasher = hashlib.sha256()
    hasher.update(domain.encode("utf-8"))
    for values in sets:
        sorted_values = sorted(values)
        hasher.update(b"\x1e")
        hasher.update(str(len(sorted_values)).encode("ascii"))
        for value in sorted_values:
            hasher.update(b",")
            hasher.update(str(int(value)).encode("ascii"))
    return hasher.hexdigest()


def _serving_only_hash_price_key(price_key: tuple[tuple[Any, ...], ...]) -> str:
    hasher = hashlib.sha256()
    hasher.update(b"serving_price_set")
    for price_part in price_key:
        hasher.update(b"\x1e")
        hasher.update(str(len(price_part)).encode("ascii"))
        for value in price_part:
            if isinstance(value, tuple):
                encoded = "\x1d".join(str(item) for item in value).encode("utf-8")
            else:
                encoded = ("" if value is None else str(value)).encode("utf-8")
            hasher.update(b"\x1f")
            hasher.update(str(len(encoded)).encode("ascii"))
            hasher.update(b":")
            hasher.update(encoded)
    return hasher.hexdigest()


def _serving_only_price_payload_and_key(
    negotiated_prices: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], tuple[tuple[Any, ...], ...]]:
    payload: list[dict[str, Any]] = []
    key_parts: list[tuple[Any, ...]] = []
    for negotiated_price in negotiated_prices:
        expiration_date = _coerce_date(negotiated_price.get("expiration_date"))
        expiration_text = expiration_date.isoformat() if expiration_date else None
        raw_rate = negotiated_price.get("negotiated_rate")
        rate_value = str(raw_rate) if isinstance(raw_rate, float) else raw_rate
        normalized_rate = normalize_money(rate_value)
        service_codes = _as_list(negotiated_price.get("service_code"))
        billing_code_modifiers = _as_list(negotiated_price.get("billing_code_modifier"))
        payload.append(
            {
                "negotiated_type": negotiated_price.get("negotiated_type"),
                "negotiated_rate": money_number(normalized_rate),
                "expiration_date": expiration_text,
                "service_code": service_codes,
                "billing_class": negotiated_price.get("billing_class"),
                "setting": negotiated_price.get("setting"),
                "billing_code_modifier": billing_code_modifiers,
                "additional_information": negotiated_price.get("additional_information"),
            }
        )
        key_parts.append(
            (
                _serving_only_key_value(negotiated_price.get("negotiated_type")),
                normalized_rate,
                expiration_text,
                _serving_only_key_list(service_codes),
                _serving_only_key_value(negotiated_price.get("billing_class")),
                _serving_only_key_value(negotiated_price.get("setting")),
                _serving_only_key_list(billing_code_modifiers),
                _serving_only_key_value(negotiated_price.get("additional_information")),
            )
        )
    return payload, tuple(sorted(key_parts))


_PTG2_WORKER_PROVIDER_MAP = None
_PTG2_WORKER_PLAN_FIELDS: dict[str, Any] | None = None
_PTG2_WORKER_SNAPSHOT_ID: str | None = None
_PTG2_WORKER_PLAN_MONTH_ID: str | None = None
_PTG2_WORKER_SOURCE_TRACE: list[dict[str, Any]] | None = None
_PTG2_WORKER_SOURCE_TRACE_SET_HASH: str | None = None
_PTG2_WORKER_SLIM_SERVING_ROWS = False
_PTG2_WORKER_COMPACT_SERVING = False


def _serving_only_rows_for_payload(
    payload: dict[str, Any],
    *,
    provider_map,
    plan_fields: dict[str, Any],
    snapshot_id: str,
    plan_month_id: str | None = None,
    source_trace_payload: list[dict[str, Any]],
    slim_serving_rows: bool = False,
    compact_serving: bool = False,
    source_trace_set_hash: str | None = None,
    include_price_set_rows: bool = False,
) -> list[dict[str, Any]] | dict[str, list[dict[str, Any]]]:
    procedure_payload = {
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
    }
    procedure_hash = semantic_hash(
        {
            "billing_code_type": payload.get("billing_code_type"),
            "billing_code_type_version": payload.get("billing_code_type_version"),
            "billing_code": payload.get("billing_code"),
            "name": payload.get("name"),
            "description": payload.get("description"),
        },
        domain="procedure",
    )
    procedure_row = {
        "procedure_hash": procedure_hash,
        "hash_prefix": hash_prefix(procedure_hash),
        "billing_code_type": payload.get("billing_code_type"),
        "billing_code_type_version": payload.get("billing_code_type_version"),
        "billing_code": payload.get("billing_code"),
        "name": payload.get("name"),
        "description": payload.get("description"),
        "canonical_payload": _canonicalize_for_json(
            {
                "billing_code_type": payload.get("billing_code_type"),
                "billing_code_type_version": payload.get("billing_code_type_version"),
                "billing_code": payload.get("billing_code"),
                "name": payload.get("name"),
                "description": payload.get("description"),
            }
        ),
        "created_at": _utcnow(),
    }
    item_price_groups: dict[str, dict[str, Any]] = {}
    for negotiated_rate in payload.get("negotiated_rates", []):
        provider_groups_inline = negotiated_rate.get("provider_groups") or []
        provider_refs = negotiated_rate.get("provider_references") or []
        combined_entry = None
        if provider_refs and not provider_groups_inline:
            combined_entry, _missing_refs = _fast_provider_entry_from_provider_refs(provider_map, provider_refs)
        else:
            groups_to_use: list[dict[str, Any]] = []
            for provider_ref in provider_refs:
                groups_to_use.extend(
                    _provider_cache_get(provider_map, _normalize_provider_ref(provider_ref))
                    or _provider_cache_get(provider_map, provider_ref)
                )
            if provider_groups_inline:
                inline_entry, _inline_row = _build_provider_set_entry(
                    file_id=0,
                    provider_group_ref=None,
                    provider_groups=provider_groups_inline,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if inline_entry is not None:
                    groups_to_use.append(inline_entry)
            combined_entry, _combined_row = _combine_provider_set_entries(
                file_id=0,
                entries=groups_to_use,
                network_names=negotiated_rate.get("network_name") or [],
            )
        if combined_entry is None:
            continue
        price_payload, price_key = _serving_only_price_payload_and_key(negotiated_rate.get("negotiated_prices", []))
        if not price_payload:
            continue
        item_group = item_price_groups.setdefault(
            price_key,
            {
                "prices": price_payload,
                "provider_entry_hashes": set(),
                "provider_group_hashes": set(),
                "provider_group_member_rows": [],
                "provider_count": 0,
            },
        )
        entry_hash = int(combined_entry["__hash__"])
        if entry_hash in item_group["provider_entry_hashes"]:
            continue
        item_group["provider_entry_hashes"].add(entry_hash)
        item_group["provider_group_hashes"].update(
            int(value) for value in combined_entry.get("provider_group_hashes") or [entry_hash]
        )
        item_group["provider_group_member_rows"].extend(_provider_group_member_rows(combined_entry))
        item_group["provider_count"] += int(
            combined_entry.get("provider_count") or len(_as_int_list(combined_entry.get("npi")))
        )

    rows: list[dict[str, Any]] = []
    compact_rows: list[dict[str, Any]] = []
    provider_set_rows: list[dict[str, Any]] = []
    provider_set_component_rows: list[dict[str, Any]] = []
    provider_group_member_rows: list[dict[str, Any]] = []
    price_set_rows: list[dict[str, Any]] = []
    confidence_payload = {
        "network": PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
        "location": PTG2_CONFIDENCE_NPPES_PRACTICE_LOCATION,
        "acceptance_statement": ptg2_confidence_statement(PTG2_CONFIDENCE_TIC_RATE_NPI_TIN),
    }
    for price_key, item_group in item_price_groups.items():
        provider_set_hash = _serving_only_hash_int_sets(
            "serving_provider_set",
            item_group["provider_entry_hashes"],
        )
        provider_group_hashes = sorted({int(value) for value in item_group["provider_entry_hashes"]})
        provider_count = int(item_group["provider_count"] or 0)
        provider_set_rows.append(
            {
                "provider_set_hash": provider_set_hash,
                "hash_prefix": hash_prefix(provider_set_hash),
                "provider_count": provider_count,
                "npi": None,
                "tin_type": None,
                "tin_value": None,
                "canonical_payload": _canonicalize_for_json(
                    {
                        "provider_group_hashes": provider_group_hashes,
                        "provider_group_count": len(provider_group_hashes),
                        "provider_count": provider_count,
                        "npi_inline": False,
                    }
                ),
                "created_at": _utcnow(),
            }
        )
        price_set_hash = _serving_only_hash_price_key(price_key)
        if slim_serving_rows:
            price_set_rows.append(
                {
                    "price_set_hash": price_set_hash,
                    "hash_prefix": hash_prefix(price_set_hash),
                    "price_atom_hashes": [],
                    "canonical_payload": item_group["prices"],
                    "created_at": _utcnow(),
                }
            )
        rate_pack_hash = _serving_only_hash_text(
            "serving_rate_pack",
            snapshot_id,
            procedure_hash,
            provider_set_hash,
            price_set_hash,
        )
        rows.append(
            _ptg2_serving_rate_row(
                snapshot_id=snapshot_id,
                plan_fields=plan_fields,
                procedure_payload=procedure_payload,
                rate_pack_row={
                    "rate_pack_hash": rate_pack_hash,
                    "provider_set_hash": provider_set_hash,
                    "price_set_hash": price_set_hash,
                },
                provider_set_hashes=[provider_set_hash],
                provider_count=provider_count,
                provider_set_count=1,
                prices=None if slim_serving_rows else item_group["prices"],
                source_trace=None if slim_serving_rows else source_trace_payload,
                source_trace_set_hash=source_trace_set_hash if slim_serving_rows else None,
                confidence={} if slim_serving_rows else confidence_payload,
                confidence_code=PTG2_CONFIDENCE_TIC_RATE_NPI_TIN,
            )
        )
        if not compact_serving:
            provider_set_component_rows.extend(
                _provider_set_component_rows(provider_set_hash, item_group["provider_entry_hashes"])
            )
        provider_group_member_rows.extend(item_group["provider_group_member_rows"])
        if compact_serving and plan_month_id:
            compact_rows.append(
                _ptg2_compact_serving_rate_row(rows[-1], plan_month_id=plan_month_id, procedure_hash=procedure_hash)
            )
    if include_price_set_rows:
        return {
            "serving_rows": rows,
            "serving_rate_compact_rows": compact_rows,
            "provider_set_rows": provider_set_rows,
            "price_set_rows": price_set_rows,
            "provider_set_component_rows": provider_set_component_rows,
            "provider_group_member_rows": provider_group_member_rows,
            "procedure_rows": [procedure_row],
        }
    return rows


def _serving_only_worker_process(payload_or_raw: dict[str, Any] | bytes) -> dict[str, list[dict[str, Any]]]:
    if (
        _PTG2_WORKER_PROVIDER_MAP is None
        or _PTG2_WORKER_PLAN_FIELDS is None
        or _PTG2_WORKER_SNAPSHOT_ID is None
        or _PTG2_WORKER_PLAN_MONTH_ID is None
        or _PTG2_WORKER_SOURCE_TRACE is None
    ):
        raise RuntimeError("PTG2 serving worker was not initialized")
    payload = _json_loads(payload_or_raw) if isinstance(payload_or_raw, (bytes, bytearray)) else payload_or_raw
    return _serving_only_rows_for_payload(
        payload,
        provider_map=_PTG2_WORKER_PROVIDER_MAP,
        plan_fields=_PTG2_WORKER_PLAN_FIELDS,
        snapshot_id=_PTG2_WORKER_SNAPSHOT_ID,
        plan_month_id=_PTG2_WORKER_PLAN_MONTH_ID,
        source_trace_payload=_PTG2_WORKER_SOURCE_TRACE,
        slim_serving_rows=_PTG2_WORKER_SLIM_SERVING_ROWS,
        compact_serving=_PTG2_WORKER_COMPACT_SERVING,
        source_trace_set_hash=_PTG2_WORKER_SOURCE_TRACE_SET_HASH,
        include_price_set_rows=True,
    )


def _serving_only_merge_worker_result(
    dest: dict[str, list[dict[str, Any]]],
    src: dict[str, list[dict[str, Any]]] | None,
) -> None:
    if not src:
        return
    key_fields = {
        "serving_rows": "serving_rate_id",
        "serving_rate_compact_rows": "serving_rate_id",
        "provider_set_rows": "provider_set_hash",
        "price_set_rows": "price_set_hash",
        "provider_set_component_rows": ("provider_set_hash", "provider_group_hash"),
        "provider_group_member_rows": ("provider_group_hash", "npi"),
        "procedure_rows": "procedure_hash",
    }
    seen: dict[str, set[Any]] = dest.setdefault("__seen__", {})  # type: ignore[assignment]
    for key, id_field in key_fields.items():
        rows = src.get(key) or []
        if not rows:
            continue
        key_seen = seen.setdefault(key, set())
        out = dest.setdefault(key, [])
        for row in rows:
            if isinstance(id_field, tuple):
                dedupe_id = tuple(row.get(part) for part in id_field)
            else:
                dedupe_id = row.get(id_field)
            if dedupe_id in key_seen:
                continue
            key_seen.add(dedupe_id)
            out.append(row)


def _serving_only_worker_process_chunk(
    payloads_or_raw: list[dict[str, Any] | bytes | bytearray],
) -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = {}
    for payload_or_raw in payloads_or_raw:
        result = _serving_only_worker_process(payload_or_raw)
        _serving_only_merge_worker_result(merged, result)
    merged.pop("__seen__", None)
    return merged


def _worker_payload_size(payload_or_raw: dict[str, Any] | bytes | bytearray) -> int:
    if isinstance(payload_or_raw, (bytes, bytearray)):
        return len(payload_or_raw)
    try:
        return len(canonical_json_dumps(payload_or_raw).encode("utf-8"))
    except Exception:
        return 1024 * 1024


def _serving_only_worker_process_chunk_to_files(
    payloads_or_raw: list[dict[str, Any] | bytes | bytearray],
) -> dict[str, Any]:
    key_fields = {
        "serving_rows": "serving_rate_id",
        "serving_rate_compact_rows": "serving_rate_id",
        "provider_set_rows": "provider_set_hash",
        "price_set_rows": "price_set_hash",
        "provider_set_component_rows": ("provider_set_hash", "provider_group_hash"),
        "provider_group_member_rows": ("provider_group_hash", "npi"),
        "procedure_rows": "procedure_hash",
    }
    temp_dir = Path(tempfile.mkdtemp(prefix="ptg2_worker_result_"))
    handles: dict[str, Any] = {}
    paths: dict[str, str] = {}
    counts: dict[str, int] = {}
    seen: dict[str, set[Any]] = {key: set() for key in key_fields}
    try:
        for payload_or_raw in payloads_or_raw:
            result = _serving_only_worker_process(payload_or_raw)
            for key, id_field in key_fields.items():
                rows = result.get(key) or []
                if not rows:
                    continue
                handle = handles.get(key)
                if handle is None:
                    path = temp_dir / f"{key}.pickle"
                    handle = path.open("wb")
                    handles[key] = handle
                    paths[key] = str(path)
                key_seen = seen[key]
                for row in rows:
                    if isinstance(id_field, tuple):
                        dedupe_id = tuple(row.get(part) for part in id_field)
                    else:
                        dedupe_id = row.get(id_field)
                    if dedupe_id in key_seen:
                        continue
                    key_seen.add(dedupe_id)
                    pickle.dump(row, handle, protocol=pickle.HIGHEST_PROTOCOL)
                    counts[key] = counts.get(key, 0) + 1
    finally:
        for handle in handles.values():
            handle.close()
    return {
        "__worker_result_files__": True,
        "temp_dir": str(temp_dir),
        "paths": paths,
        "counts": counts,
    }


def _iter_worker_result_rows(path: str | Path):
    with Path(path).open("rb") as fp:
        while True:
            try:
                yield pickle.load(fp)
            except EOFError:
                break


def _ptg2_worker_capacity_wait_needed(
    *,
    pending_count: int,
    pending_input_bytes: int,
    next_batch_bytes: int,
    max_pending_batches: int,
    max_pending_bytes: int,
) -> bool:
    if pending_count >= max_pending_batches:
        return True
    if pending_count > 0 and pending_input_bytes + next_batch_bytes > max_pending_bytes:
        return True
    return False


async def _parse_in_network_file_serving_only(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    source_version: PTG2SourceVersion | None,
    snapshot_id: str,
    import_month: datetime.date,
    max_items: int | None = None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    context_row = _ptg2_context_row(plan_fields, import_month, source_version)
    source_trace_row, _source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
    compact_serving = _use_compact_serving_table()
    slim_serving_rows = compact_serving or _env_bool(PTG2_SLIM_SERVING_ROWS_ENV, False)
    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)
    await _push_ptg2_objects([context_row], PTG2RateSetContext, rewrite=True)
    if slim_serving_rows or compact_serving:
        await _push_ptg2_objects([source_trace_row], PTG2SourceTrace, rewrite=True)
        await _push_ptg2_objects([_source_trace_set_row], PTG2SourceTraceSet, rewrite=True)

    state = _compact_state()
    state["snapshot_id"] = snapshot_id
    state["plan_fields"] = plan_fields
    source_trace_payload = _ptg2_source_trace_payload(source_trace_row)
    source_trace_set_hash = _source_trace_set_row["source_trace_set_hash"]
    provider_ref_count = 0
    item_count = 0
    serving_rows = 0
    progress_state: dict[str, Any] = {}
    provider_combo_cache_limit = max(_env_int(PTG2_PROVIDER_COMBO_CACHE_REFS_ENV, 32768), 0)
    provider_combo_cache: OrderedDict[tuple[str, ...], dict[str, Any]] = OrderedDict()
    provider_combo_stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": provider_combo_cache_limit,
    }
    serving_workers = max(_env_int(PTG2_SERVING_WORKERS_ENV, 1), 1)
    worker_chunk_items = max(_env_int(PTG2_WORKER_CHUNK_ITEMS_ENV, 128), 1)
    worker_chunk_bytes = max(_env_int(PTG2_WORKER_CHUNK_BYTES_ENV, 16 * 1024 * 1024), 1024 * 1024)
    worker_max_pending_batches = max(
        _env_int(PTG2_WORKER_MAX_PENDING_BATCHES_ENV, min(serving_workers, 2)),
        1,
    )
    worker_max_pending_bytes = max(
        _env_int(PTG2_WORKER_MAX_PENDING_BYTES_ENV, worker_chunk_bytes * worker_max_pending_batches),
        worker_chunk_bytes,
    )
    worker_result_files = _env_bool(PTG2_WORKER_RESULT_FILES_ENV, True)

    async def add_serving_result_parts(
        *,
        price_set_rows=(),
        result_rows=(),
        compact_rows=(),
        provider_set_rows=(),
        procedure_rows=(),
        component_rows=(),
        member_rows=(),
    ) -> None:
        nonlocal serving_rows
        if price_set_rows and _env_bool(PTG2_SKIP_EXISTING_PRICE_SETS_ENV, compact_serving):
            if state.get("existing_price_set_hashes") is None:
                state["existing_price_set_hashes"] = await _existing_price_set_hashes()
            existing_price_set_hashes = state["existing_price_set_hashes"]
        else:
            existing_price_set_hashes = None
        for price_set_row in price_set_rows:
            price_set_hash = str(price_set_row.get("price_set_hash") or "")
            if existing_price_set_hashes is not None and price_set_hash in existing_price_set_hashes:
                continue
            if _compact_add_unique(state, "price_set", "price_set_hash", price_set_row):
                if existing_price_set_hashes is not None and price_set_hash:
                    existing_price_set_hashes.add(price_set_hash)
        for provider_set_row in provider_set_rows:
            _compact_add_unique(state, "provider_set", "provider_set_hash", provider_set_row)
        for procedure_row in procedure_rows:
            _compact_add_unique(state, "procedure", "procedure_hash", procedure_row)
        for component_row in component_rows:
            _compact_add_unique(state, "provider_set_component", ("provider_set_hash", "provider_group_hash"), component_row)
        for member_row in member_rows:
            _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
        for compact_row in compact_rows:
            if _compact_add_unique(state, "serving_rate_compact", "serving_rate_id", compact_row):
                serving_rows += 1
        if compact_serving:
            await _flush_compact_rows(state)
            return
        for serving_row in result_rows:
            if _compact_add_unique(state, "serving_rate", "serving_rate_id", serving_row):
                serving_rows += 1
        await _flush_compact_rows(state)

    async def add_serving_result(result: list[dict[str, Any]] | dict[str, Any]) -> None:
        if isinstance(result, dict) and result.get("__worker_result_files__"):
            temp_dir = Path(result["temp_dir"])
            paths = result.get("paths") or {}
            try:
                await add_serving_result_parts(
                    price_set_rows=_iter_worker_result_rows(paths["price_set_rows"]) if "price_set_rows" in paths else (),
                    result_rows=_iter_worker_result_rows(paths["serving_rows"]) if "serving_rows" in paths else (),
                    compact_rows=_iter_worker_result_rows(paths["serving_rate_compact_rows"]) if "serving_rate_compact_rows" in paths else (),
                    provider_set_rows=_iter_worker_result_rows(paths["provider_set_rows"]) if "provider_set_rows" in paths else (),
                    procedure_rows=_iter_worker_result_rows(paths["procedure_rows"]) if "procedure_rows" in paths else (),
                    component_rows=_iter_worker_result_rows(paths["provider_set_component_rows"]) if "provider_set_component_rows" in paths else (),
                    member_rows=_iter_worker_result_rows(paths["provider_group_member_rows"]) if "provider_group_member_rows" in paths else (),
                )
            finally:
                for path in paths.values():
                    try:
                        Path(path).unlink(missing_ok=True)
                    except Exception:
                        logger.debug("Failed to remove PTG2 worker result file %s", path, exc_info=True)
                try:
                    temp_dir.rmdir()
                except Exception:
                    logger.debug("Failed to remove PTG2 worker result dir %s", temp_dir, exc_info=True)
            return
        if isinstance(result, dict):
            await add_serving_result_parts(
                price_set_rows=result.get("price_set_rows") or [],
                result_rows=result.get("serving_rows") or [],
                compact_rows=result.get("serving_rate_compact_rows") or [],
                provider_set_rows=result.get("provider_set_rows") or [],
                procedure_rows=result.get("procedure_rows") or [],
                component_rows=result.get("provider_set_component_rows") or [],
                member_rows=result.get("provider_group_member_rows") or [],
            )
            return
        await add_serving_result_parts(result_rows=result)

    if (
        compact_serving
        and _env_bool(PTG2_RUST_COMPACT_SERVING_ENV, False)
        and max_items is None
        and not test_mode
    ):
        rust_batches = {
            "price_set_rows": [],
            "serving_rate_compact_rows": [],
            "provider_set_rows": [],
            "procedure_rows": [],
        }
        rust_batch_limit = max(_env_int(PTG2_COMPACT_BATCH_ROWS_ENV, 5000), 1)
        rust_records = 0
        rust_item_count: set[str] = set()
        compact_copy_stdout = _env_bool("HLTHPRT_PTG2_COMPACT_COPY_STDOUT", False)
        compact_copy_path: Path | None = None
        if not compact_copy_stdout:
            compact_copy_fd, compact_copy_name = tempfile.mkstemp(prefix="ptg2_compact_serving_", suffix=".copy")
            os.close(compact_copy_fd)
            compact_copy_path = Path(compact_copy_name)
        dictionary_copy_paths: dict[str, Path] = {}
        if not compact_copy_stdout:
            for dictionary_kind in ("procedure", "price_set", "provider_set", "provider_group_member"):
                fd, name = tempfile.mkstemp(prefix=f"ptg2_{dictionary_kind}_", suffix=".copy")
                os.close(fd)
                dictionary_copy_paths[dictionary_kind] = Path(name)
        compact_copy_rows = 0
        compact_copy_completed = False
        compact_copy_tasks: set[asyncio.Task] = set()
        compact_copy_task_limit = max(_env_int("HLTHPRT_PTG2_COMPACT_COPY_TASKS", 1), 1)
        compact_copy_semaphore = asyncio.Semaphore(compact_copy_task_limit)
        stage_tables: dict[str, str] = {}
        stage_completed = False
        if _env_bool(PTG2_RUST_COPY_UNLOGGED_STAGE_ENV, True) and not compact_copy_stdout:
            stage_token = hashlib.md5(f"{snapshot_id}:{os.getpid()}:{time.time_ns()}".encode("utf-8")).hexdigest()[:12]
            stage_tables = await _create_rust_copy_stage_tables(stage_token)

        async def copy_ready_compact_file(copy_row: dict[str, Any]) -> None:
            raw_copy_path = str(copy_row.get("path") or "").strip()
            if not raw_copy_path:
                return
            copy_file = Path(raw_copy_path)
            async with compact_copy_semaphore:
                await _copy_compact_serving_rate_file(
                    copy_file,
                    target_table=stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact"),
                )
            try:
                copy_file.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 compact copy chunk %s", copy_file, exc_info=True)

        async def copy_ready_compact_bytes(payload: bytes) -> None:
            async with compact_copy_semaphore:
                await _copy_compact_serving_rate_bytes(
                    payload,
                    target_table=stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact"),
                )

        async def copy_ready_dictionary_file(kind: str, copy_row: dict[str, Any]) -> None:
            raw_copy_path = str(copy_row.get("path") or "").strip()
            if not raw_copy_path:
                return
            copy_file = Path(raw_copy_path)
            async with compact_copy_semaphore:
                await _copy_ptg2_dictionary_file(copy_file, kind, target_table=stage_tables.get(kind))
            try:
                copy_file.unlink(missing_ok=True)
            except Exception:
                logger.debug("Failed to remove PTG2 %s copy chunk %s", kind, copy_file, exc_info=True)

        async def wait_for_some_copy_tasks(force: bool = False) -> None:
            nonlocal compact_copy_tasks
            if not compact_copy_tasks:
                return
            if force:
                done, compact_copy_tasks = await asyncio.wait(
                    compact_copy_tasks,
                    return_when=asyncio.ALL_COMPLETED,
                )
                compact_copy_tasks = set()
            elif len(compact_copy_tasks) < compact_copy_task_limit * 2:
                return
            else:
                done, compact_copy_tasks = await asyncio.wait(
                    compact_copy_tasks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            for task in done:
                task.result()
        try:
            for record_kind, record_row in _iter_compact_serving_records_rust(
                file_path,
                snapshot_id=snapshot_id,
                plan_id=str(plan_fields.get("plan_id") or ""),
                plan_month_id=str(plan_month_row["plan_month_id"]),
                source_trace_set_hash=source_trace_set_hash,
                compact_copy_path=compact_copy_path,
                compact_copy_stdout=compact_copy_stdout,
                procedure_copy_path=dictionary_copy_paths.get("procedure"),
                price_set_copy_path=dictionary_copy_paths.get("price_set"),
                provider_set_copy_path=dictionary_copy_paths.get("provider_set"),
                provider_group_member_copy_path=dictionary_copy_paths.get("provider_group_member"),
            ):
                rust_records += 1
                if record_kind == "price_set":
                    rust_batches["price_set_rows"].append(record_row)
                elif record_kind == "price_set_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("price_set", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "compact_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_compact_file(record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "compact_copy_data":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_compact_bytes(record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "serving_rate_compact":
                    rust_batches["serving_rate_compact_rows"].append(record_row)
                    if record_row.get("procedure_hash"):
                        rust_item_count.add(str(record_row.get("procedure_hash")))
                elif record_kind == "provider_set":
                    rust_batches["provider_set_rows"].append(record_row)
                elif record_kind == "provider_set_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_set", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "provider_group_member_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("provider_group_member", record_row)))
                    await wait_for_some_copy_tasks()
                elif record_kind == "procedure":
                    rust_batches["procedure_rows"].append(record_row)
                    if record_row.get("procedure_hash"):
                        rust_item_count.add(str(record_row.get("procedure_hash")))
                elif record_kind == "procedure_copy_file":
                    compact_copy_tasks.add(asyncio.create_task(copy_ready_dictionary_file("procedure", record_row)))
                    await wait_for_some_copy_tasks()
                if sum(len(rows) for rows in rust_batches.values()) >= rust_batch_limit:
                    await add_serving_result_parts(
                        price_set_rows=rust_batches["price_set_rows"],
                        compact_rows=rust_batches["serving_rate_compact_rows"],
                        provider_set_rows=rust_batches["provider_set_rows"],
                        procedure_rows=rust_batches["procedure_rows"],
                    )
                    for rows in rust_batches.values():
                        rows.clear()
            if any(rust_batches.values()):
                await add_serving_result_parts(
                    price_set_rows=rust_batches["price_set_rows"],
                    compact_rows=rust_batches["serving_rate_compact_rows"],
                    provider_set_rows=rust_batches["provider_set_rows"],
                    procedure_rows=rust_batches["procedure_rows"],
                )
            await _flush_compact_rows(state, force=True)
            await _drain_compact_writes(state)
            await wait_for_some_copy_tasks(force=True)
            if compact_copy_path is not None and compact_copy_path.exists() and compact_copy_path.stat().st_size > 0:
                await _copy_compact_serving_rate_file(
                    compact_copy_path,
                    target_table=stage_tables.get("serving_rate_compact", "ptg2_serving_rate_compact"),
                )
                compact_copy_completed = True
            for dictionary_kind, dictionary_path in dictionary_copy_paths.items():
                if dictionary_path.exists() and dictionary_path.stat().st_size > 0:
                    await _copy_ptg2_dictionary_file(
                        dictionary_path,
                        dictionary_kind,
                        target_table=stage_tables.get(dictionary_kind),
                    )
            if stage_tables:
                await _merge_rust_copy_stage_tables(stage_tables, drop=True)
                stage_completed = True
            serving_rows = await _count_compact_serving_rate_rows(
                snapshot_id,
                str(plan_fields.get("plan_id") or ""),
            )
            compact_copy_completed = True
        finally:
            for task in compact_copy_tasks:
                task.cancel()
            if compact_copy_completed or not _env_bool(PTG2_KEEP_PARTIAL_ENV, False):
                try:
                    if compact_copy_path is not None:
                        compact_copy_path.unlink(missing_ok=True)
                except Exception:
                    logger.debug("Failed to remove PTG2 compact copy file %s", compact_copy_path, exc_info=True)
                for dictionary_path in dictionary_copy_paths.values():
                    try:
                        dictionary_path.unlink(missing_ok=True)
                    except Exception:
                        logger.debug("Failed to remove PTG2 dictionary copy file %s", dictionary_path, exc_info=True)
                if stage_tables and not stage_completed:
                    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
                    for stage_table in stage_tables.values():
                        try:
                            await db.status(
                                f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};"
                            )
                        except Exception:
                            logger.debug("Failed to drop PTG2 Rust stage table %s", stage_table, exc_info=True)
        item_count = len(rust_item_count)
        logger.info(
            "PTG2 Rust compact serving emitted %s dictionary records, %s copy rows for %s procedure hashes",
            rust_records,
            compact_copy_rows,
            item_count,
        )
        if slim_serving_rows and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True):
            await _merge_staged_price_sets(snapshot_id)
        if not _env_bool(PTG2_DEFER_PROVIDER_LOCATIONS_ENV, True):
            await _build_ptg2_provider_locations(snapshot_id)
        await flush_error_log(import_log_cls)
        summary = {
            "provider_refs": provider_ref_count,
            "in_network_items": item_count,
            "serving_rates": serving_rows,
            "serving_only": True,
            "serving_workers": 0,
            "worker_chunk_items": 0,
            "rust_compact_serving": True,
            "rust_records": rust_records,
        }
        print(f"PTG2 serving-only import summary: {summary}", flush=True)
        logger.info("PTG2 serving-only import summary: %s", summary)
        return summary

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "serving-only in-network import",
                ref_count=provider_ref_count,
                item_count=item_count,
            )

        if serving_workers > 1 and _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True):
            global _PTG2_WORKER_PROVIDER_MAP, _PTG2_WORKER_PLAN_FIELDS, _PTG2_WORKER_SNAPSHOT_ID, _PTG2_WORKER_PLAN_MONTH_ID, _PTG2_WORKER_SOURCE_TRACE
            global _PTG2_WORKER_SOURCE_TRACE_SET_HASH, _PTG2_WORKER_SLIM_SERVING_ROWS, _PTG2_WORKER_COMPACT_SERVING
            pool: concurrent.futures.ProcessPoolExecutor | None = None
            pending: set[concurrent.futures.Future] = set()
            pending_input_bytes_by_future: dict[concurrent.futures.Future, int] = {}
            pending_input_bytes = 0
            pending_batches: list[dict[str, Any] | bytes | bytearray] = []
            pending_batch_bytes = 0
            mp_context = multiprocessing.get_context("fork")

            async def _drain_completed_worker_results(done: set[concurrent.futures.Future]) -> None:
                nonlocal pending_input_bytes
                for future in done:
                    pending_input_bytes -= pending_input_bytes_by_future.pop(future, 0)
                    await add_serving_result(future.result())

            async def _wait_for_worker_capacity(next_batch_bytes: int) -> None:
                nonlocal pending
                while pending and _ptg2_worker_capacity_wait_needed(
                    pending_count=len(pending),
                    pending_input_bytes=pending_input_bytes,
                    next_batch_bytes=next_batch_bytes,
                    max_pending_batches=worker_max_pending_batches,
                    max_pending_bytes=worker_max_pending_bytes,
                ):
                    done, pending = concurrent.futures.wait(
                        pending,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    await _drain_completed_worker_results(done)

            async def _flush_pending_batches(force: bool = False) -> None:
                nonlocal pending_batches, pending_batch_bytes, pending_input_bytes
                if not pending_batches:
                    return
                if not force and len(pending_batches) < worker_chunk_items and pending_batch_bytes < worker_chunk_bytes:
                    return
                assert pool is not None
                batch = pending_batches
                batch_bytes = pending_batch_bytes
                pending_batches = []
                pending_batch_bytes = 0
                await _wait_for_worker_capacity(batch_bytes)
                worker_fn = (
                    _serving_only_worker_process_chunk_to_files
                    if worker_result_files
                    else _serving_only_worker_process_chunk
                )
                future = pool.submit(worker_fn, batch)
                pending.add(future)
                pending_input_bytes_by_future[future] = batch_bytes
                pending_input_bytes += batch_bytes

            try:
                item_prefixes = {
                    "provider_reference": "provider_references.item",
                    "in_network": "in_network.item",
                }
                if _env_bool(PTG2_RUST_SCANNER_ENV, False):
                    object_iterator = (
                        (
                            "provider_reference" if array_name == "provider_references" else "in_network",
                            _json_loads(raw_object) if array_name == "provider_references" else raw_object,
                        )
                        for array_name, raw_object in _iter_top_level_object_bytes_rust(
                            file_path,
                            {"provider_references", "in_network"},
                        )
                    )
                elif _env_bool(PTG2_JSON_DECODER_ITERATOR_ENV, True):
                    raw_object_names = {"in_network"} if _env_bool(PTG2_RAW_WORKER_OBJECTS_ENV, True) else set()
                    object_iterator = _iter_top_level_objects_jsondecoder(
                        afp,
                        item_prefixes,
                        progress_callback=progress_callback,
                        raw_object_names=raw_object_names,
                    )
                else:
                    object_iterator = (
                        (
                            "provider_reference" if array_name == "provider_references" else "in_network",
                            _json_loads(raw_object) if array_name == "provider_references" else raw_object,
                        )
                        for array_name, raw_object in _iter_top_level_object_bytes(
                            afp,
                            {"provider_references", "in_network"},
                            progress_callback=progress_callback,
                        )
                    )
                for object_name, payload in object_iterator:
                    if object_name == "provider_reference":
                        provider_ref_count += 1
                        if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                            continue
                        provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                        provider_entry, _provider_row = _build_provider_set_entry(
                            file_id=file_id,
                            provider_group_ref=provider_group_id,
                            provider_groups=payload.get("provider_groups", []),
                            network_names=payload.get("network_name") or payload.get("network_names") or [],
                        )
                        if provider_entry is not None:
                            _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                            for member_row in _provider_group_member_rows(provider_entry):
                                _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
                            await _flush_compact_rows(state)
                        continue
                    if object_name != "in_network":
                        continue
                    if pool is None:
                        _PTG2_WORKER_PROVIDER_MAP = provider_map
                        _PTG2_WORKER_PLAN_FIELDS = plan_fields
                        _PTG2_WORKER_SNAPSHOT_ID = snapshot_id
                        _PTG2_WORKER_PLAN_MONTH_ID = plan_month_row["plan_month_id"]
                        _PTG2_WORKER_SOURCE_TRACE = source_trace_payload
                        _PTG2_WORKER_SOURCE_TRACE_SET_HASH = source_trace_set_hash
                        _PTG2_WORKER_SLIM_SERVING_ROWS = slim_serving_rows
                        _PTG2_WORKER_COMPACT_SERVING = compact_serving
                        pool = concurrent.futures.ProcessPoolExecutor(
                            max_workers=serving_workers,
                            mp_context=mp_context,
                        )
                    item_count += 1
                    payload_size = _worker_payload_size(payload)
                    if pending_batches and pending_batch_bytes + payload_size > worker_chunk_bytes:
                        await _flush_pending_batches(force=True)
                    pending_batches.append(payload)
                    pending_batch_bytes += payload_size
                    await _flush_pending_batches()
                    if max_items is not None and item_count >= max_items:
                        break
                    if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                        break
                await _flush_pending_batches(force=True)
                while pending:
                    done, pending = concurrent.futures.wait(
                        pending,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    await _drain_completed_worker_results(done)
            finally:
                if pool is not None:
                    pool.shutdown(wait=True, cancel_futures=True)
        else:
            if _env_bool(PTG2_RUST_SCANNER_ENV, False):
                object_iterator = (
                    (
                        "provider_reference" if array_name == "provider_references" else "in_network",
                        _json_loads(raw_object),
                    )
                    for array_name, raw_object in _iter_top_level_object_bytes_rust(
                        file_path,
                        {"provider_references", "in_network"},
                    )
                )
            else:
                iterator_fn = (
                    _iter_top_level_objects_fast
                    if _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True)
                    else _iter_top_level_objects
                )
                object_iterator = iterator_fn(
                    afp,
                    {
                        "provider_reference": "provider_references.item",
                        "in_network": "in_network.item",
                    },
                    use_float=True,
                    progress_callback=progress_callback,
                )
            for object_name, payload in object_iterator:
                if object_name == "provider_reference":
                    provider_ref_count += 1
                    if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                        continue
                    provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                    provider_entry, _provider_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=provider_group_id,
                        provider_groups=payload.get("provider_groups", []),
                        network_names=payload.get("network_name") or payload.get("network_names") or [],
                    )
                    if provider_entry is not None:
                        _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                        for member_row in _provider_group_member_rows(provider_entry):
                            _compact_add_unique(state, "provider_group_member", ("provider_group_hash", "npi"), member_row)
                        await _flush_compact_rows(state)
                    continue

                if object_name != "in_network":
                    continue
                item_count += 1
                result = _serving_only_rows_for_payload(
                    payload,
                    provider_map=provider_map,
                    plan_fields=plan_fields,
                    snapshot_id=snapshot_id,
                    plan_month_id=plan_month_row["plan_month_id"],
                    source_trace_payload=source_trace_payload,
                    slim_serving_rows=slim_serving_rows,
                    compact_serving=compact_serving,
                    source_trace_set_hash=source_trace_set_hash,
                    include_price_set_rows=True,
                )
                await add_serving_result(result)
                if max_items is not None and item_count >= max_items:
                    break
                if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                    break

    await _flush_compact_rows(state, force=True)
    await _drain_compact_writes(state)
    if _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True) and not _use_stage_serving_as_final() and not compact_serving:
        await _merge_staged_serving_rates(snapshot_id)
    if slim_serving_rows and _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True):
        await _merge_staged_price_sets(snapshot_id)
    if compact_serving:
        await _build_ptg2_provider_locations(snapshot_id)
    await flush_error_log(import_log_cls)
    summary = {
        "provider_refs": provider_ref_count,
        "in_network_items": item_count,
        "serving_rates": serving_rows,
        "serving_only": True,
        "serving_workers": serving_workers,
        "worker_chunk_items": worker_chunk_items,
    }
    if hasattr(provider_map, "stats"):
        summary.update(provider_map.stats())
    summary.update(provider_combo_stats)
    print(f"PTG2 serving-only import summary: {summary}", flush=True)
    logger.info("PTG2 serving-only import summary: %s", summary)
    return summary


async def _load_provider_references_from_file(
    file_path: str,
    file_id: int,
    provider_cls,
    provider_map,
    test_mode: bool,
    log_cls,
    source_url: str,
) -> None:
    rows: list[dict[str, Any]] = []
    seen_hashes: set[int] = set()
    provider_ref_batch_rows = max(_env_int(PTG2_PROVIDER_REF_BATCH_ROWS_ENV, 5000), 1)
    with open_json_artifact_stream(file_path) as afp:
        ref_count = 0
        for ref in ijson.items(afp, "provider_references.item"):
            ref_count += 1
            provider_group_id = _normalize_provider_ref(ref.get("provider_group_id"))
            network_names = ref.get("network_name") or ref.get("network_names") or []
            provider_entry, provider_row = _build_provider_set_entry(
                file_id=file_id,
                provider_group_ref=provider_group_id,
                provider_groups=ref.get("provider_groups", []),
                network_names=network_names,
            )
            if provider_entry is None or provider_row is None:
                continue
            provider_hash = provider_entry["__hash__"]
            if provider_hash not in seen_hashes:
                seen_hashes.add(provider_hash)
                rows.append(provider_row)
            if len(rows) >= provider_ref_batch_rows:
                await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
                rows = []
            _provider_cache_put(provider_map, provider_group_id, [provider_entry])
            if test_mode and ref_count >= TEST_PROVIDER_GROUPS:
                break
    if isinstance(provider_map, PTG2ProviderReferenceCache):
        provider_map.commit()
    if rows:
        await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await flush_error_log(log_cls)
    return None


async def _parse_in_network_items(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    provider_cls = classes["PTGProviderGroup"]
    item_cls = classes["PTGInNetworkItem"]
    billing_cls = classes["PTGBillingCode"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]

    item_rows: list[dict[str, Any]] = []
    billing_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    provider_rows: list[dict[str, Any]] = []
    provider_hash_seen: set[int] = _provider_cache_hashes(provider_map)
    rate_hash_seen: set[int] = set()
    price_hash_seen: set[int] = set()
    item_batch_rows = max(_env_int(PTG2_ITEM_BATCH_ROWS_ENV, 1000), 1)
    billing_batch_rows = max(_env_int(PTG2_BILLING_BATCH_ROWS_ENV, 5000), 1)
    rate_batch_rows = max(_env_int(PTG2_RATE_BATCH_ROWS_ENV, 5000), 1)
    price_batch_rows = max(_env_int(PTG2_PRICE_BATCH_ROWS_ENV, 10000), 1)

    plan_fields = _derive_plan_fields(meta, plan_info)

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for in_item in ijson.items(afp, "in_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                in_item.get("billing_code_type"),
                in_item.get("billing_code"),
                in_item.get("negotiation_arrangement"),
                in_item.get("name"),
            )
            item_rows.append(
                {
                    "item_hash": item_hash,
                    "file_id": file_id,
                    "negotiation_arrangement": in_item.get("negotiation_arrangement"),
                    "name": in_item.get("name"),
                    "billing_code_type": in_item.get("billing_code_type"),
                    "billing_code_type_version": in_item.get("billing_code_type_version"),
                    "billing_code": in_item.get("billing_code"),
                    "description": in_item.get("description"),
                    "severity_of_illness": in_item.get("severity_of_illness"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for code in in_item.get("bundled_codes", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "bundle", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "bundle",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )
            for code in in_item.get("covered_services", []):
                billing_rows.append(
                    {
                        "code_hash": _make_checksum(item_hash, "covered", code.get("billing_code")),
                        "item_hash": item_hash,
                        "code_role": "covered",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )

            for negotiated_rate in in_item.get("negotiated_rates", []):
                provider_refs = negotiated_rate.get("provider_references") or []
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                groups_to_use: list[dict[str, Any]] = []
                for provider_ref in provider_refs:
                    provider_key = _normalize_provider_ref(provider_ref)
                    groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                    if not groups:
                        await log_error(
                            "err",
                            f"Provider reference {provider_ref} not found for file {source_url}",
                            [0],
                            source_url,
                            "ptg-in-network",
                            "json",
                            import_log_cls,
                        )
                        continue
                    groups_to_use.extend(groups)
                if provider_groups_inline:
                    inline_ref = (
                        _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                        if len(provider_groups_inline) == 1
                        else None
                    )
                    inline_entry, inline_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=inline_ref,
                        provider_groups=provider_groups_inline,
                        network_names=negotiated_rate.get("network_name") or [],
                    )
                    if inline_entry is not None and inline_row is not None:
                        inline_hash = inline_entry["__hash__"]
                        if inline_hash not in provider_hash_seen:
                            provider_hash_seen.add(inline_hash)
                            provider_rows.append(inline_row)
                        groups_to_use.append(inline_entry)
                combined_entry, combined_row = _combine_provider_set_entries(
                    file_id=file_id,
                    entries=groups_to_use,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if combined_entry is not None and combined_row is not None:
                    combined_hash = combined_entry["__hash__"]
                    if combined_hash not in provider_hash_seen:
                        provider_hash_seen.add(combined_hash)
                        provider_rows.append(combined_row)
                    groups_to_use = [combined_entry]
                else:
                    groups_to_use = []
                for group in groups_to_use:
                    rate_hash = _make_checksum(item_hash, group.get("__hash__") or "")
                    if rate_hash not in rate_hash_seen:
                        rate_hash_seen.add(rate_hash)
                        rate_rows.append(
                            {
                                "rate_hash": rate_hash,
                                "item_hash": item_hash,
                                "provider_group_ref": group.get("provider_group_id"),
                                "provider_group_hash": group.get("__hash__"),
                            }
                        )
                    for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                        price_hash = _make_checksum(
                            rate_hash,
                            negotiated_price.get("negotiated_type"),
                            negotiated_price.get("negotiated_rate"),
                            negotiated_price.get("expiration_date") or "",
                            negotiated_price.get("billing_class"),
                            negotiated_price.get("setting"),
                            "|".join(_as_list(negotiated_price.get("service_code"))),
                            "|".join(_as_list(negotiated_price.get("billing_code_modifier"))),
                        )
                        if price_hash in price_hash_seen:
                            continue
                        price_hash_seen.add(price_hash)
                        price_rows.append(
                            {
                                "price_hash": price_hash,
                                "rate_hash": rate_hash,
                                "negotiated_type": negotiated_price.get("negotiated_type"),
                                "negotiated_rate": negotiated_price.get("negotiated_rate"),
                                "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
                                "service_code": _as_list(negotiated_price.get("service_code")),
                                "billing_class": negotiated_price.get("billing_class"),
                                "setting": negotiated_price.get("setting"),
                                "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
                                "additional_information": negotiated_price.get("additional_information"),
                            }
                        )
                        if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                            break
                if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                    break

            if len(item_rows) >= item_batch_rows:
                await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
                item_rows = []
            if len(billing_rows) >= billing_batch_rows:
                await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
                billing_rows = []
            if len(rate_rows) >= rate_batch_rows:
                await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
                rate_rows = []
            if len(price_rows) >= price_batch_rows:
                await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
                price_rows = []

            if max_items is not None and count >= max_items:
                break
            if test_mode and count >= TEST_IN_NETWORK_ITEMS:
                break

    if item_rows:
        await push_objects(_dedupe_rows_by(item_rows, "item_hash"), item_cls)
    if billing_rows:
        await push_objects(_dedupe_rows_by(billing_rows, "code_hash"), billing_cls)
    if rate_rows:
        await push_objects(_dedupe_rows_by(rate_rows, "rate_hash"), rate_cls)
    if price_rows:
        await push_objects(_dedupe_rows_by(price_rows, "price_hash"), price_cls)
    if provider_rows:
        await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
    await flush_error_log(import_log_cls)


async def _parse_in_network_file_single_pass(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    """
    Parse provider_references and in_network arrays in one physical pass.

    Large UHC files are gzip streams, so a second parser pass means repeating
    all decompression. The spec places provider_references before in_network;
    when that is true this keeps provider reference storage bounded without
    replaying the gzip.
    """
    provider_cls = classes["PTGProviderGroup"]
    item_cls = classes["PTGInNetworkItem"]
    billing_cls = classes["PTGBillingCode"]
    rate_cls = classes["PTGNegotiatedRate"]
    price_cls = classes["PTGNegotiatedPrice"]

    provider_ref_batch_rows = max(_env_int(PTG2_PROVIDER_REF_BATCH_ROWS_ENV, 5000), 1)
    item_batch_rows = max(_env_int(PTG2_ITEM_BATCH_ROWS_ENV, 1000), 1)
    billing_batch_rows = max(_env_int(PTG2_BILLING_BATCH_ROWS_ENV, 5000), 1)
    rate_batch_rows = max(_env_int(PTG2_RATE_BATCH_ROWS_ENV, 5000), 1)
    price_batch_rows = max(_env_int(PTG2_PRICE_BATCH_ROWS_ENV, 10000), 1)

    plan_fields = _derive_plan_fields(meta, plan_info)
    provider_rows: list[dict[str, Any]] = []
    item_rows: list[dict[str, Any]] = []
    billing_rows: list[dict[str, Any]] = []
    rate_rows: list[dict[str, Any]] = []
    price_rows: list[dict[str, Any]] = []
    provider_ref_hash_seen: set[int] = set()
    provider_hash_seen: set[int] = _provider_cache_hashes(provider_map)
    item_hash_seen: set[int] = set()
    billing_hash_seen: set[int] = set()
    rate_hash_seen: set[int] = set()
    price_hash_seen: set[int] = set()
    ref_count = 0
    item_count = 0
    progress_state: dict[str, Any] = {}

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "in-network import",
                ref_count=ref_count,
                item_count=item_count,
            )

        iterator_fn = (
            _iter_top_level_objects_fast
            if _env_bool(PTG2_FAST_OBJECT_ITERATOR_ENV, True)
            else _iter_top_level_objects
        )
        for object_name, payload in iterator_fn(
            afp,
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
            use_float=True,
            progress_callback=progress_callback,
        ):
            if object_name == "provider_reference":
                ref_count += 1
                if test_mode and ref_count > TEST_PROVIDER_GROUPS:
                    continue
                provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                network_names = payload.get("network_name") or payload.get("network_names") or []
                provider_entry, provider_row = _build_provider_set_entry(
                    file_id=file_id,
                    provider_group_ref=provider_group_id,
                    provider_groups=payload.get("provider_groups", []),
                    network_names=network_names,
                )
                if provider_entry is None or provider_row is None:
                    continue
                provider_hash = provider_entry["__hash__"]
                if provider_hash not in provider_ref_hash_seen:
                    provider_ref_hash_seen.add(provider_hash)
                    provider_hash_seen.add(provider_hash)
                    provider_rows.append(provider_row)
                _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                if len(provider_rows) >= provider_ref_batch_rows:
                    await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
                    provider_rows = []
                    if isinstance(provider_map, PTG2ProviderReferenceCache):
                        provider_map.commit()
                continue

            if object_name != "in_network":
                continue
            item_count += 1
            item_hash = _make_checksum(
                file_id,
                payload.get("billing_code_type"),
                payload.get("billing_code"),
                payload.get("negotiation_arrangement"),
                payload.get("name"),
            )
            if item_hash not in item_hash_seen:
                item_hash_seen.add(item_hash)
                item_rows.append(
                    {
                        "item_hash": item_hash,
                        "file_id": file_id,
                        "negotiation_arrangement": payload.get("negotiation_arrangement"),
                        "name": payload.get("name"),
                        "billing_code_type": payload.get("billing_code_type"),
                        "billing_code_type_version": payload.get("billing_code_type_version"),
                        "billing_code": payload.get("billing_code"),
                        "description": payload.get("description"),
                        "severity_of_illness": payload.get("severity_of_illness"),
                        "plan_name": plan_fields.get("plan_name"),
                        "plan_id_type": plan_fields.get("plan_id_type"),
                        "plan_id": plan_fields.get("plan_id"),
                        "plan_market_type": plan_fields.get("plan_market_type"),
                        "issuer_name": plan_fields.get("issuer_name"),
                        "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                    }
            )
            for code in payload.get("bundled_codes", []):
                code_hash = _make_checksum(item_hash, "bundle", code.get("billing_code"))
                if code_hash in billing_hash_seen:
                    continue
                billing_hash_seen.add(code_hash)
                billing_rows.append(
                    {
                        "code_hash": code_hash,
                        "item_hash": item_hash,
                        "code_role": "bundle",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )
            for code in payload.get("covered_services", []):
                code_hash = _make_checksum(item_hash, "covered", code.get("billing_code"))
                if code_hash in billing_hash_seen:
                    continue
                billing_hash_seen.add(code_hash)
                billing_rows.append(
                    {
                        "code_hash": code_hash,
                        "item_hash": item_hash,
                        "code_role": "covered",
                        "billing_code_type": code.get("billing_code_type"),
                        "billing_code_type_version": code.get("billing_code_type_version"),
                        "billing_code": code.get("billing_code"),
                        "description": code.get("description"),
                    }
                )

            for negotiated_rate in payload.get("negotiated_rates", []):
                provider_refs = negotiated_rate.get("provider_references") or []
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                groups_to_use: list[dict[str, Any]] = []
                for provider_ref in provider_refs:
                    provider_key = _normalize_provider_ref(provider_ref)
                    groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                    if not groups:
                        await log_error(
                            "err",
                            f"Provider reference {provider_ref} not found for file {source_url}",
                            [0],
                            source_url,
                            "ptg-in-network",
                            "json",
                            import_log_cls,
                        )
                        continue
                    groups_to_use.extend(groups)
                if provider_groups_inline:
                    inline_ref = (
                        _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                        if len(provider_groups_inline) == 1
                        else None
                    )
                    inline_entry, inline_row = _build_provider_set_entry(
                        file_id=file_id,
                        provider_group_ref=inline_ref,
                        provider_groups=provider_groups_inline,
                        network_names=negotiated_rate.get("network_name") or [],
                    )
                    if inline_entry is not None and inline_row is not None:
                        inline_hash = inline_entry["__hash__"]
                        if inline_hash not in provider_hash_seen:
                            provider_hash_seen.add(inline_hash)
                            provider_rows.append(inline_row)
                        groups_to_use.append(inline_entry)
                combined_entry, combined_row = _combine_provider_set_entries(
                    file_id=file_id,
                    entries=groups_to_use,
                    network_names=negotiated_rate.get("network_name") or [],
                )
                if combined_entry is not None and combined_row is not None:
                    combined_hash = combined_entry["__hash__"]
                    if combined_hash not in provider_hash_seen:
                        provider_hash_seen.add(combined_hash)
                        provider_rows.append(combined_row)
                    groups_to_use = [combined_entry]
                else:
                    groups_to_use = []
                for group in groups_to_use:
                    rate_hash = _make_checksum(item_hash, group.get("__hash__") or "")
                    if rate_hash not in rate_hash_seen:
                        rate_hash_seen.add(rate_hash)
                        rate_rows.append(
                            {
                                "rate_hash": rate_hash,
                                "item_hash": item_hash,
                                "provider_group_ref": group.get("provider_group_id"),
                                "provider_group_hash": group.get("__hash__"),
                            }
                        )
                    for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                        price_hash = _make_checksum(
                            rate_hash,
                            negotiated_price.get("negotiated_type"),
                            negotiated_price.get("negotiated_rate"),
                            negotiated_price.get("expiration_date") or "",
                            negotiated_price.get("billing_class"),
                            negotiated_price.get("setting"),
                            "|".join(_as_list(negotiated_price.get("service_code"))),
                            "|".join(_as_list(negotiated_price.get("billing_code_modifier"))),
                        )
                        if price_hash in price_hash_seen:
                            continue
                        price_hash_seen.add(price_hash)
                        price_rows.append(
                            {
                                "price_hash": price_hash,
                                "rate_hash": rate_hash,
                                "negotiated_type": negotiated_price.get("negotiated_type"),
                                "negotiated_rate": negotiated_price.get("negotiated_rate"),
                                "expiration_date": _coerce_date(negotiated_price.get("expiration_date")),
                                "service_code": _as_list(negotiated_price.get("service_code")),
                                "billing_class": negotiated_price.get("billing_class"),
                                "setting": negotiated_price.get("setting"),
                                "billing_code_modifier": _as_list(negotiated_price.get("billing_code_modifier")),
                                "additional_information": negotiated_price.get("additional_information"),
                            }
                        )
                        if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                            break
                if test_mode and len(price_rows) >= TEST_NEGOTIATED_PRICES:
                    break

            if provider_rows and len(provider_rows) >= provider_ref_batch_rows:
                await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
                provider_rows = []
            item_rows, billing_rows, rate_rows, price_rows = await _flush_in_network_rows(
                item_rows,
                billing_rows,
                rate_rows,
                price_rows,
                item_cls,
                billing_cls,
                rate_cls,
                price_cls,
                item_batch_rows=item_batch_rows,
                billing_batch_rows=billing_batch_rows,
                rate_batch_rows=rate_batch_rows,
                price_batch_rows=price_batch_rows,
            )
            if isinstance(provider_map, PTG2ProviderReferenceCache):
                provider_map.commit()

            if max_items is not None and item_count >= max_items:
                break
            if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                break

    if progress_state:
        progress_state["last_log"] = 0
        _maybe_log_artifact_progress(
            file_path,
            afp if "afp" in locals() else None,
            progress_state,
            "in-network import",
            ref_count=ref_count,
            item_count=item_count,
        )
    if isinstance(provider_map, PTG2ProviderReferenceCache):
        provider_map.commit()
    if provider_rows:
        await push_objects(_dedupe_rows_by(provider_rows, "provider_group_hash"), provider_cls)
    await _flush_in_network_rows(
        item_rows,
        billing_rows,
        rate_rows,
        price_rows,
        item_cls,
        billing_cls,
        rate_cls,
        price_cls,
        force=True,
        item_batch_rows=item_batch_rows,
        billing_batch_rows=billing_batch_rows,
        rate_batch_rows=rate_batch_rows,
        price_batch_rows=price_batch_rows,
    )
    await flush_error_log(import_log_cls)


async def _parse_in_network_file_compact(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    provider_map,
    test_mode: bool,
    import_log_cls,
    source_url: str,
    source_version: PTG2SourceVersion | None,
    snapshot_id: str,
    import_month: datetime.date,
    max_items: int | None = None,
) -> dict[str, Any]:
    plan_fields = _derive_plan_fields(meta, plan_info)
    plan_row, alias_rows, plan_month_row = _ptg2_plan_rows(plan_fields, snapshot_id, import_month)
    context_row = _ptg2_context_row(plan_fields, import_month, source_version)
    source_trace_row, source_trace_set_row = _ptg2_source_trace_rows(source_version, source_url)
    await _push_ptg2_objects([plan_row], PTG2Plan, rewrite=True)
    if alias_rows:
        await _push_ptg2_objects(alias_rows, PTG2PlanAlias, rewrite=True)
    await _push_ptg2_objects([plan_month_row], PTG2PlanMonth, rewrite=True)
    await _push_ptg2_objects([context_row], PTG2RateSetContext, rewrite=True)

    state = _compact_state()
    state["snapshot_id"] = snapshot_id
    state["plan_fields"] = plan_fields
    state["source_trace_payload"] = _ptg2_source_trace_payload(source_trace_row)
    _compact_add_unique(state, "source_trace", "source_trace_hash", source_trace_row)
    _compact_add_unique(state, "source_trace_set", "source_trace_set_hash", source_trace_set_row)
    source_trace_set_hash = source_trace_set_row["source_trace_set_hash"]
    context_hash = context_row["context_hash"]
    provider_ref_count = 0
    item_count = 0
    progress_state: dict[str, Any] = {}
    rate_group_flush_items = max(_env_int(PTG2_RATE_GROUP_FLUSH_ITEMS_ENV, 1000), 0)
    provider_combo_cache_limit = max(_env_int(PTG2_PROVIDER_COMBO_CACHE_REFS_ENV, 32768), 0)
    fast_provider_aggregation = _env_bool(
        PTG2_FAST_PROVIDER_AGGREGATION_ENV,
        _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False),
    )
    provider_combo_cache: OrderedDict[tuple[str, ...], dict[str, Any]] = OrderedDict()
    provider_combo_stats = {
        "provider_combo_cache_gets": 0,
        "provider_combo_cache_hits": 0,
        "provider_combo_cache_misses": 0,
        "provider_combo_cache_size": 0,
        "provider_combo_cache_limit": provider_combo_cache_limit,
    }

    with open_json_artifact_stream(file_path) as afp:
        def progress_callback() -> None:
            _maybe_log_artifact_progress(
                file_path,
                afp,
                progress_state,
                "compact in-network import",
                ref_count=provider_ref_count,
                item_count=item_count,
            )

        for object_name, payload in _iter_top_level_objects(
            afp,
            {
                "provider_reference": "provider_references.item",
                "in_network": "in_network.item",
            },
            use_float=True,
            progress_callback=progress_callback,
        ):
            if object_name == "provider_reference":
                provider_ref_count += 1
                if test_mode and provider_ref_count > TEST_PROVIDER_GROUPS:
                    continue
                provider_group_id = _normalize_provider_ref(payload.get("provider_group_id"))
                network_names = payload.get("network_name") or payload.get("network_names") or []
                for provider_group_row in _ptg2_provider_group_rows(
                    provider_groups=payload.get("provider_groups", []),
                ):
                    if _compact_add_unique(state, "provider_group", "provider_group_hash", provider_group_row):
                        state["counts"]["provider_groups"] += 1
                provider_entry, _provider_row = _build_provider_set_entry(
                    file_id=file_id,
                    provider_group_ref=provider_group_id,
                    provider_groups=payload.get("provider_groups", []),
                    network_names=network_names,
                )
                if provider_entry is None:
                    continue
                _provider_cache_put(provider_map, provider_group_id, [provider_entry])
                continue

            if object_name != "in_network":
                continue
            item_count += 1
            procedure_row = _ptg2_procedure_row(payload)
            procedure_hash = procedure_row["procedure_hash"]
            state["procedure_payloads"][procedure_hash] = {
                "billing_code_type": procedure_row.get("billing_code_type"),
                "billing_code": procedure_row.get("billing_code"),
                "name": procedure_row.get("name"),
                "description": procedure_row.get("description"),
            }
            if _compact_add_unique(state, "procedure", "procedure_hash", procedure_row):
                state["counts"]["procedures"] += 1

            item_price_groups: dict[str, dict[str, Any]] = {}
            for negotiated_rate in payload.get("negotiated_rates", []):
                provider_groups_inline = negotiated_rate.get("provider_groups") or []
                provider_refs = negotiated_rate.get("provider_references") or []
                combined_entry = None
                if provider_refs and not provider_groups_inline:
                    combo_key = _provider_combo_cache_key(provider_refs)
                    combined_entry = _provider_combo_cache_get(provider_combo_cache, combo_key, provider_combo_stats)
                else:
                    combo_key = ()
                if combined_entry is None:
                    groups_to_use: list[dict[str, Any]] = []
                    if fast_provider_aggregation and provider_refs and not provider_groups_inline:
                        combined_entry, missing_refs = _fast_provider_entry_from_provider_refs(provider_map, provider_refs)
                        for provider_ref in missing_refs:
                            await log_error(
                                "err",
                                f"Provider reference {provider_ref} not found for file {source_url}",
                                [0],
                                source_url,
                                "ptg-in-network-compact",
                                "json",
                                import_log_cls,
                            )
                    else:
                        for provider_ref in provider_refs:
                            provider_key = _normalize_provider_ref(provider_ref)
                            groups = _provider_cache_get(provider_map, provider_key) or _provider_cache_get(provider_map, provider_ref)
                            if not groups:
                                await log_error(
                                    "err",
                                    f"Provider reference {provider_ref} not found for file {source_url}",
                                    [0],
                                    source_url,
                                    "ptg-in-network-compact",
                                    "json",
                                    import_log_cls,
                                )
                                continue
                            groups_to_use.extend(groups)
                    if provider_groups_inline:
                        for provider_group_row in _ptg2_provider_group_rows(provider_groups=provider_groups_inline):
                            if _compact_add_unique(state, "provider_group", "provider_group_hash", provider_group_row):
                                state["counts"]["provider_groups"] += 1
                        inline_ref = (
                            _normalize_provider_ref(provider_groups_inline[0].get("provider_group_id"))
                            if len(provider_groups_inline) == 1
                            else None
                        )
                        inline_entry, _inline_row = _build_provider_set_entry(
                            file_id=file_id,
                            provider_group_ref=inline_ref,
                            provider_groups=provider_groups_inline,
                            network_names=negotiated_rate.get("network_name") or [],
                        )
                        if inline_entry is not None:
                            groups_to_use.append(inline_entry)
                    if combined_entry is None:
                        combined_entry, _combined_row = _combine_provider_set_entries(
                            file_id=file_id,
                            entries=groups_to_use,
                            network_names=negotiated_rate.get("network_name") or [],
                        )
                    if combined_entry is not None and combo_key:
                        _provider_combo_cache_put(
                            provider_combo_cache,
                            combo_key,
                            combined_entry,
                            provider_combo_stats,
                            provider_combo_cache_limit,
                        )
                if combined_entry is None:
                    continue
                price_atom_hashes: list[str] = []
                price_payload: list[dict[str, Any]] = []
                for negotiated_price in negotiated_rate.get("negotiated_prices", []):
                    price_atom_row = _ptg2_price_atom_row(negotiated_price)
                    if _compact_add_unique(state, "price_atom", "price_atom_hash", price_atom_row):
                        state["counts"]["price_atoms"] += 1
                    price_atom_hashes.append(price_atom_row["price_atom_hash"])
                    price_payload.append(_ptg2_price_atom_payload(price_atom_row))
                if not price_atom_hashes:
                    continue
                price_set = build_price_set(price_atom_hashes)
                price_set_row = {
                    **price_set,
                    "created_at": _utcnow(),
                }
                if _compact_add_unique(state, "price_set", "price_set_hash", price_set_row):
                    state["counts"]["price_sets"] += 1
                state["price_payloads"][price_set_row["price_set_hash"]] = price_payload

                item_group = item_price_groups.setdefault(
                    price_set_row["price_set_hash"],
                    {
                        "price_set_hash": price_set_row["price_set_hash"],
                        "provider_entries": [],
                        "provider_entry_hashes": set(),
                        "provider_group_hashes": set(),
                        "provider_count": 0,
                        "network_names": set(),
                    },
                )
                if fast_provider_aggregation:
                    entry_hash = int(combined_entry["__hash__"])
                    if entry_hash not in item_group["provider_entry_hashes"]:
                        item_group["provider_entry_hashes"].add(entry_hash)
                        item_group["provider_group_hashes"].update(
                            int(value) for value in combined_entry.get("provider_group_hashes") or [entry_hash]
                        )
                        item_group["provider_count"] += int(
                            combined_entry.get("provider_count") or len(_as_int_list(combined_entry.get("npi")))
                        )
                        item_group["network_names"].update(
                            str(value) for value in _as_list(combined_entry.get("network_name")) if value
                        )
                else:
                    item_group["provider_entries"].append(combined_entry)

            for item_group in item_price_groups.values():
                if fast_provider_aggregation:
                    super_entry = _fast_provider_entry_from_parts(
                        entry_hashes=item_group["provider_entry_hashes"],
                        provider_group_hashes=item_group["provider_group_hashes"],
                        provider_count=item_group["provider_count"],
                        network_names=item_group["network_names"],
                    )
                else:
                    super_entry, _super_row = _combine_provider_set_entries(
                        file_id=file_id,
                        entries=item_group["provider_entries"],
                        network_names=[],
                    )
                if super_entry is None:
                    continue
                provider_set_row = _ptg2_provider_set_row(super_entry)
                state["provider_set_counts"][provider_set_row["provider_set_hash"]] = provider_set_row["provider_count"]
                if _compact_add_unique(state, "provider_set", "provider_set_hash", provider_set_row):
                    state["counts"]["provider_sets"] += 1
                provider_set_hash = provider_set_row["provider_set_hash"]
                group_key = (procedure_hash, item_group["price_set_hash"], source_trace_set_hash)
                state["rate_pack_groups"].setdefault(group_key, set()).add(provider_set_hash)
            await _flush_compact_rows(state)
            if rate_group_flush_items and item_count % rate_group_flush_items == 0:
                await _flush_compact_rate_pack_groups(state, context_hash)

            if max_items is not None and item_count >= max_items:
                break
            if test_mode and item_count >= TEST_IN_NETWORK_ITEMS:
                break

    await _flush_compact_rows(state, force=True)
    await _flush_compact_rate_pack_groups(state, context_hash)
    await _drain_compact_writes(state)
    if _env_bool(PTG2_STAGE_SERVING_RATES_ENV, True) and not _use_stage_serving_as_final():
        await _merge_staged_serving_rates(snapshot_id)
    if _env_bool(PTG2_STAGE_PRICE_SETS_ENV, True):
        await _merge_staged_price_sets(snapshot_id)
    chunk_rows: list[dict[str, Any]] = []
    chunk_hashes: list[str] = []
    for (procedure_hash, provider_bucket), rate_pack_hashes in state["chunk_rate_packs"].items():
        chunk = build_fact_chunk(
            context_hash,
            PTG2_DOMAIN_IN_NETWORK,
            procedure_hash,
            provider_bucket,
            list(rate_pack_hashes),
        )
        chunk_rows.append({**chunk, "created_at": _utcnow()})
        chunk_hashes.append(chunk["fact_chunk_hash"])
    if chunk_rows:
        await _push_ptg2_objects(chunk_rows, PTG2FactChunk, rewrite=True)
    rate_set = build_rate_set(context_hash, chunk_hashes)
    rate_set_row = {**rate_set, "created_at": _utcnow()}
    await _push_ptg2_objects([rate_set_row], PTG2RateSet, rewrite=True)
    await _push_ptg2_objects(
        [
            {
                "plan_rate_set_id": semantic_hash(
                    {
                        "plan_month_id": plan_month_row["plan_month_id"],
                        "rate_set_hash": rate_set_row["rate_set_hash"],
                        "domain": PTG2_DOMAIN_IN_NETWORK,
                    },
                    domain="plan_rate_set",
                )[:32],
                "plan_month_id": plan_month_row["plan_month_id"],
                "rate_set_hash": rate_set_row["rate_set_hash"],
                "domain": PTG2_DOMAIN_IN_NETWORK,
                "created_at": _utcnow(),
            }
        ],
        PTG2PlanRateSet,
        rewrite=True,
    )
    await flush_error_log(import_log_cls)
    summary = {
        **state["counts"],
        "provider_refs": provider_ref_count,
        "in_network_items": item_count,
        "fact_chunks": len(chunk_rows),
        "rate_set_hash": rate_set_row["rate_set_hash"],
    }
    if hasattr(provider_map, "stats"):
        summary.update(provider_map.stats())
    summary.update(provider_combo_stats)
    print(f"PTG2 compact import summary: {summary}", flush=True)
    logger.info("PTG2 compact import summary: %s", summary)
    return summary


async def _parse_allowed_amounts(
    file_path: str,
    file_id: int,
    meta: dict[str, Any],
    plan_info: list[dict[str, Any]] | None,
    classes: dict[str, type],
    test_mode: bool,
    import_log_cls,
    source_url: str,
    max_items: int | None = None,
) -> None:
    item_cls = classes["PTGAllowedItem"]
    payment_cls = classes["PTGAllowedPayment"]
    provider_payment_cls = classes["PTGAllowedProviderPayment"]

    plan_fields = _derive_plan_fields(meta, plan_info)

    item_rows: list[dict[str, Any]] = []
    payment_rows: list[dict[str, Any]] = []
    provider_payment_rows: list[dict[str, Any]] = []

    with open_json_artifact_stream(file_path) as afp:
        count = 0
        for out_item in ijson.items(afp, "out_of_network.item", use_float=True):
            count += 1
            item_hash = _make_checksum(
                file_id,
                out_item.get("billing_code_type"),
                out_item.get("billing_code"),
                out_item.get("name"),
            )
            item_rows.append(
                {
                    "allowed_item_hash": item_hash,
                    "file_id": file_id,
                    "name": out_item.get("name"),
                    "billing_code_type": out_item.get("billing_code_type"),
                    "billing_code_type_version": out_item.get("billing_code_type_version"),
                    "billing_code": out_item.get("billing_code"),
                    "description": out_item.get("description"),
                    "plan_name": plan_fields.get("plan_name"),
                    "plan_id_type": plan_fields.get("plan_id_type"),
                    "plan_id": plan_fields.get("plan_id"),
                    "plan_market_type": plan_fields.get("plan_market_type"),
                    "issuer_name": plan_fields.get("issuer_name"),
                    "plan_sponsor_name": plan_fields.get("plan_sponsor_name"),
                }
            )
            for allowed_amount in out_item.get("allowed_amounts", []):
                tin_info = allowed_amount.get("tin") or {}
                for payment in allowed_amount.get("payments", []):
                    payment_hash = _make_checksum(
                        item_hash,
                        tin_info.get("value") or "",
                        "|".join(_as_list(allowed_amount.get("service_code"))),
                        allowed_amount.get("billing_class") or "",
                        allowed_amount.get("setting") or "",
                        payment.get("allowed_amount"),
                        "|".join(_as_list(payment.get("billing_code_modifier"))),
                    )
                    payment_rows.append(
                        {
                            "payment_hash": payment_hash,
                            "allowed_item_hash": item_hash,
                            "tin_type": tin_info.get("type"),
                            "tin_value": tin_info.get("value"),
                            "service_code": _as_list(allowed_amount.get("service_code")),
                            "billing_class": allowed_amount.get("billing_class"),
                            "setting": allowed_amount.get("setting"),
                            "allowed_amount": payment.get("allowed_amount"),
                            "billing_code_modifier": _as_list(payment.get("billing_code_modifier")),
                        }
                    )
                    for provider in payment.get("providers", []):
                        provider_payment_rows.append(
                            {
                                "provider_payment_hash": _make_checksum(
                                    payment_hash,
                                    provider.get("billed_charge"),
                                    "|".join(str(n) for n in _as_int_list(provider.get("npi"))),
                                ),
                                "payment_hash": payment_hash,
                                "billed_charge": provider.get("billed_charge"),
                                "npi": _as_int_list(provider.get("npi")),
                            }
                        )
            if len(item_rows) >= 100:
                await push_objects(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
                item_rows = []
            if len(payment_rows) >= 200:
                await push_objects(payment_rows, payment_cls)
                payment_rows = []
            if len(provider_payment_rows) >= 200:
                await push_objects(provider_payment_rows, provider_payment_cls)
                provider_payment_rows = []
            if max_items is not None and count >= max_items:
                break
            if test_mode and count >= TEST_ALLOWED_ITEMS:
                break

    if item_rows:
        await push_objects(_dedupe_rows_by(item_rows, "allowed_item_hash"), item_cls)
    if payment_rows:
        await push_objects(payment_rows, payment_cls)
    if provider_payment_rows:
        await push_objects(provider_payment_rows, provider_payment_cls)
    await flush_error_log(import_log_cls)


async def _process_table_of_contents(
    toc_url: str,
    classes: dict[str, type],
    test_mode: bool,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    import_run_id: str | None = None,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    keep_partial_artifacts: bool | None = None,
) -> list[dict[str, Any]]:
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    jobs: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    seen_files: set[int] = set()

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                toc_url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download table-of-contents from %s: %s", toc_url, exc)
            return []
        toc_content = load_json_artifact(logical_artifact.logical_path)
        if import_run_id:
            await _record_source_version(
                source_type="table-of-contents",
                domain="catalog",
                raw_artifact=raw_artifact,
                logical_artifact=logical_artifact,
                import_run_id=import_run_id,
            )

    if import_run_id:
        catalog_rows = []
        for entry in parse_toc_catalog_entries(
            toc_content,
            toc_url,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        ):
            first_plan = entry.plan_info[0] if len(entry.plan_info) == 1 else {}
            catalog_rows.append(
                {
                    "source_catalog_id": _catalog_entry_id(entry),
                    "import_run_id": import_run_id,
                    "source_type": entry.source_type,
                    "domain": entry.domain,
                    "original_url": entry.original_url,
                    "canonical_url": entry.canonical_url,
                    "from_index_url": entry.from_index_url,
                    "description": entry.description,
                    "reporting_entity_name": entry.reporting_entity_name,
                    "reporting_entity_type": entry.reporting_entity_type,
                    "plan_name": first_plan.get("plan_name"),
                    "plan_id_type": first_plan.get("plan_id_type"),
                    "plan_id": first_plan.get("plan_id"),
                    "plan_market_type": first_plan.get("plan_market_type"),
                    "issuer_name": first_plan.get("issuer_name"),
                    "plan_sponsor_name": first_plan.get("plan_sponsor_name") or first_plan.get("plan_sponser_name"),
                    "payload": _canonicalize_for_json(entry),
                    "created_at": _utcnow(),
                }
            )
        if catalog_rows:
            await _push_ptg2_objects(catalog_rows, PTG2SourceCatalog, rewrite=True)

    toc_meta = {
        "reporting_entity_name": toc_content.get("reporting_entity_name"),
        "reporting_entity_type": toc_content.get("reporting_entity_type"),
        "last_updated_on": toc_content.get("last_updated_on"),
        "version": toc_content.get("version"),
    }
    file_rows.append(
        _build_file_row(
            toc_url,
            "table-of-contents",
            toc_meta,
            None,
            toc_content.get("description"),
            None,
        )
    )

    for structure in toc_content.get("reporting_structure", []):
        plans = _filter_reporting_plans(
            [_normalize_plan_payload(plan) for plan in (structure.get("reporting_plans") or [])],
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        in_network_files = structure.get("in_network_files") or []
        allowed_amount_file = structure.get("allowed_amount_file")

        for entry in in_network_files:
            location = entry.get("location")
            if not location:
                continue
            meta = dict(toc_meta)
            file_row = _build_file_row(location, "in-network", meta, plans, entry.get("description"), toc_url)
            if file_row["file_id"] not in seen_files:
                file_rows.append(file_row)
                seen_files.add(file_row["file_id"])
            jobs.append(
                {
                    "type": "in_network",
                    "url": location,
                    "description": entry.get("description"),
                    "plan_info": plans,
                    "from_index_url": toc_url,
                    "meta": meta,
                }
            )
            if test_mode and len(jobs) >= TEST_TOC_JOBS:
                break

        if allowed_amount_file:
            location = allowed_amount_file.get("location")
            if location:
                meta = dict(toc_meta)
                file_row = _build_file_row(
                    location, "allowed-amounts", meta, plans, allowed_amount_file.get("description"), toc_url
                )
                if file_row["file_id"] not in seen_files:
                    file_rows.append(file_row)
                    seen_files.add(file_row["file_id"])
                jobs.append(
                    {
                        "type": "allowed_amounts",
                        "url": location,
                        "description": allowed_amount_file.get("description"),
                        "plan_info": plans,
                        "from_index_url": toc_url,
                        "meta": meta,
                    }
                )
        if test_mode and len(jobs) >= TEST_TOC_JOBS:
            break

    if file_rows:
        await push_objects(file_rows, file_cls, rewrite=True)
    await flush_error_log(import_log_cls)
    return jobs


async def _process_provider_reference_file(
    url: str,
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
) -> dict[int, list[dict[str, Any]]]:
    provider_cls = classes["PTGProviderGroup"]
    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]
    provider_map: dict[int, list[dict[str, Any]]] = {}

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download provider-reference from %s: %s", url, exc)
            return provider_map
        provider_content = load_json_artifact(logical_artifact.logical_path)
        await _record_source_version(
            source_type="provider-reference",
            domain="provider_reference",
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )

    meta = {
        "version": provider_content.get("version"),
    }
    file_row = _build_file_row(url, "provider-reference", meta, None, None, None)
    await push_objects([file_row], file_cls, rewrite=True)

    provider_groups = provider_content.get("provider_groups") or []
    rows: list[dict[str, Any]] = []
    for idx, group in enumerate(provider_groups):
        tin_info = group.get("tin") or {}
        npi_list = group.get("npi") or []
        normalized_npi = _normalized_npi_list(npi_list)
        provider_group_ref = _normalize_provider_ref(
            group.get("provider_group_id") or group.get("provider_group_ref") or (idx + 1)
        )
        provider_hash = _provider_group_identity_hash(tin_info, normalized_npi)
        rows.append(
            {
                "provider_group_hash": provider_hash,
                "provider_group_ref": provider_group_ref,
                "file_id": file_row["file_id"],
                "network_names": group.get("network_name") or group.get("network_names") or [],
                "tin_type": tin_info.get("type"),
                "tin_value": tin_info.get("value"),
                "tin_business_name": tin_info.get("business_name"),
                "npi": normalized_npi,
            }
        )
        provider_map.setdefault(provider_group_ref, []).append(
            {
                **group,
                "network_name": group.get("network_name") or [],
                "__hash__": provider_hash,
                "provider_group_id": provider_group_ref,
            }
        )
        if test_mode and len(rows) >= TEST_PROVIDER_GROUPS:
            break

    if rows:
        await push_objects(_dedupe_rows_by(rows, "provider_group_hash"), provider_cls)
    await flush_error_log(import_log_cls)
    return provider_map


async def _process_in_network_file(
    job: dict[str, Any],
    classes: dict[str, type],
    provider_ref_cache: dict[int, list[dict[str, Any]]],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
    compact_import: bool = False,
    snapshot_id: str | None = None,
    import_month: datetime.date | None = None,
) -> PTG2FileProcessResult:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                materialize_logical=False,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download in-network file from %s: %s", url, exc)
            return PTG2FileProcessResult("in_network", url, False, error=str(exc))
        extracted = logical_artifact.logical_path
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "in-network", meta, plan_info, description, from_index_url)
        await push_objects([file_row], file_cls, rewrite=True)
        source_version = await _record_source_version(
            source_type="in-network",
            domain=PTG2_DOMAIN_IN_NETWORK,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )
        provider_cache_backend = os.getenv(PTG2_PROVIDER_CACHE_BACKEND_ENV, "sqlite").strip().lower()
        if provider_cache_backend in {"memory", "in-memory", "ram"}:
            provider_cache = PTG2InMemoryProviderReferenceCache(provider_ref_cache)
        else:
            provider_cache = PTG2ProviderReferenceCache(Path(tmpdir) / "provider_refs.sqlite", provider_ref_cache)
        try:
            if compact_import and _env_bool(PTG2_SERVING_ONLY_IMPORT_ENV, False):
                await _parse_in_network_file_serving_only(
                    extracted,
                    file_row["file_id"],
                    meta,
                    plan_info,
                    provider_cache,
                    test_mode,
                    import_log_cls,
                    url,
                    source_version,
                    snapshot_id or "ptg2:unknown",
                    import_month or normalize_import_month(None),
                    max_items=max_items,
                )
            elif compact_import:
                await _parse_in_network_file_compact(
                    extracted,
                    file_row["file_id"],
                    meta,
                    plan_info,
                    provider_cache,
                    test_mode,
                    import_log_cls,
                    url,
                    source_version,
                    snapshot_id or "ptg2:unknown",
                    import_month or normalize_import_month(None),
                    max_items=max_items,
                )
            else:
                await _parse_in_network_file_single_pass(
                    extracted, file_row["file_id"], meta, plan_info, provider_cache, classes, test_mode, import_log_cls, url,
                    max_items=max_items,
                )
        finally:
            provider_cache.close()
    return PTG2FileProcessResult("in_network", url, True, file_id=file_row["file_id"])


async def _process_allowed_amounts_file(
    job: dict[str, Any],
    classes: dict[str, type],
    test_mode: bool,
    reuse_raw_artifacts: bool = True,
    max_bytes: int | None = None,
    max_items: int | None = None,
    import_run_id: str | None = None,
    keep_partial_artifacts: bool | None = None,
) -> PTG2FileProcessResult:
    url = job["url"]
    description = job.get("description")
    plan_info = job.get("plan_info")
    from_index_url = job.get("from_index_url")
    provided_meta = job.get("meta") or {}

    file_cls = classes["PTGFile"]
    import_log_cls = classes["ImportLog"]

    with tempfile.TemporaryDirectory(dir=ptg2_temp_parent()) as tmpdir:
        try:
            raw_artifact, logical_artifact = await materialize_json_source(
                url,
                tmpdir,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                materialize_logical=False,
                keep_partial_artifacts=keep_partial_artifacts,
            )
        except Exception as exc:
            logger.warning("Failed to download allowed-amounts file from %s: %s", url, exc)
            return PTG2FileProcessResult("allowed_amounts", url, False, error=str(exc))
        extracted = logical_artifact.logical_path
        meta = provided_meta or await _extract_metadata_fields(extracted)
        file_row = _build_file_row(url, "allowed-amounts", meta, plan_info, description, from_index_url)
        await push_objects([file_row], file_cls, rewrite=True)
        await _record_source_version(
            source_type="allowed-amounts",
            domain=PTG2_DOMAIN_ALLOWED_AMOUNT,
            raw_artifact=raw_artifact,
            logical_artifact=logical_artifact,
            import_run_id=import_run_id,
        )
        await _parse_allowed_amounts(
            extracted, file_row["file_id"], meta, plan_info, classes, test_mode, import_log_cls, url,
            max_items=max_items,
        )
    return PTG2FileProcessResult("allowed_amounts", url, True, file_id=file_row["file_id"])


async def _mark_ptg2_import_failed(
    import_run_id: str,
    snapshot_id: str,
    import_month: datetime.date,
    started_at: datetime.datetime,
    error: BaseException | str,
    report: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
) -> None:
    finished = _utcnow()
    error_text = str(error)
    report_payload = dict(report or {})
    report_payload.setdefault("snapshot_id", snapshot_id)
    try:
        await _push_ptg2_objects(
            [
                {
                    "snapshot_id": snapshot_id,
                    "import_run_id": import_run_id,
                    "import_month": import_month,
                    "status": PTG2_STATUS_FAILED,
                    "created_at": started_at,
                    "validated_at": None,
                    "published_at": None,
                    "previous_snapshot_id": None,
                    "manifest": {**report_payload, "error": error_text},
                }
            ],
            PTG2Snapshot,
            rewrite=True,
        )
        await _push_ptg2_objects(
            [
                {
                    "import_run_id": import_run_id,
                    "import_month": import_month,
                    "status": PTG2_STATUS_FAILED,
                    "started_at": started_at,
                    "finished_at": finished,
                    "heartbeat_at": finished,
                    "options": dict(options or {}),
                    "report": report_payload,
                    "error": error_text,
                }
            ],
            PTG2ImportRun,
            rewrite=True,
        )
    except Exception as mark_exc:
        logger.error("Failed to mark PTG2 import %s as failed: %s", import_run_id, mark_exc)


async def main(
    test_mode: bool = False,
    toc_urls: list[str] | None = None,
    toc_list: str | None = None,
    in_network_url: str | None = None,
    allowed_url: str | None = None,
    provider_ref_url: str | None = None,
    import_id: str | None = None,
    import_month: str | datetime.date | None = None,
    max_files: int | None = None,
    max_items: int | None = None,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    file_url_contains: list[str] | None = None,
    reuse_raw_artifacts: bool = True,
    keep_partial_artifacts: bool | None = None,
) -> None:
    """
    PTG2 entry point for the Transparency in Coverage importer.
    """
    import_month_value = normalize_import_month(import_month)
    import_id_val = _normalize_import_id(import_id or import_month_value.strftime("%Y%m%d"))
    import_run_id = f"ptg2:{import_id_val}"
    snapshot_id = f"ptg2:{import_month_value.strftime('%Y%m')}:{hash_prefix(semantic_hash(import_run_id), 12)}"
    max_bytes = None
    if test_mode:
        raw_max_bytes = os.getenv("HLTHPRT_PTG2_TEST_MAX_BYTES")
        if raw_max_bytes:
            try:
                max_bytes = int(raw_max_bytes)
            except ValueError:
                logger.warning("Ignoring invalid HLTHPRT_PTG2_TEST_MAX_BYTES=%s", raw_max_bytes)
    await ensure_database(test_mode)
    await ensure_ptg2_tables()
    compact_import = _env_bool(PTG2_COMPACT_IMPORT_ENV, not test_mode)
    if compact_import and _use_compact_serving_table():
        await prepare_ptg2_compact_bulk_load()
    now = _utcnow()
    options_payload = {
        "toc_urls": toc_urls or [],
        "toc_list": toc_list,
        "in_network_url": in_network_url,
        "allowed_url": allowed_url,
        "provider_ref_url": provider_ref_url,
        "plan_ids": plan_ids or [],
        "plan_name_contains": plan_name_contains or [],
        "plan_market_types": plan_market_types or [],
        "file_url_contains": file_url_contains or [],
        "max_files": max_files,
        "max_items": max_items,
        "reuse_raw_artifacts": reuse_raw_artifacts,
        "keep_partial_artifacts": _env_bool(PTG2_KEEP_PARTIAL_ENV, True)
        if keep_partial_artifacts is None else keep_partial_artifacts,
        "compact_import": compact_import,
        "async_write_tasks": max(_env_int(PTG2_ASYNC_WRITE_TASKS_ENV, 1), 1),
        "fast_provider_union": _env_bool(PTG2_FAST_PROVIDER_UNION_ENV, False),
        "serving_only_import": _env_bool(PTG2_SERVING_ONLY_IMPORT_ENV, False),
        "stage_serving_as_final": _use_stage_serving_as_final(),
        "test_mode": test_mode,
    }
    await _push_ptg2_objects(
        [
            {
                "import_run_id": import_run_id,
                "import_month": import_month_value,
                "status": PTG2_STATUS_RUNNING,
                "started_at": now,
                "finished_at": None,
                "heartbeat_at": now,
                "options": options_payload,
                "report": {},
                "error": None,
            }
        ],
        PTG2ImportRun,
        rewrite=True,
    )
    await _push_ptg2_objects(
        [
            {
                "snapshot_id": snapshot_id,
                "import_run_id": import_run_id,
                "import_month": import_month_value,
                "status": PTG2_STATUS_BUILDING,
                "created_at": now,
                "validated_at": None,
                "published_at": None,
                "previous_snapshot_id": None,
                "manifest": {},
            }
        ],
        PTG2Snapshot,
        rewrite=True,
    )
    failure_report: dict[str, Any] = {"snapshot_id": snapshot_id, "legacy_table_suffix": import_id_val}
    try:
        if compact_import:
            schema = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
            await db.status(f"DELETE FROM {schema}.ptg2_serving_rate WHERE snapshot_id = :snapshot_id", snapshot_id=snapshot_id)
        classes = await _prepare_ptg_tables(import_id_val, test_mode)

        provider_ref_cache: dict[int, list[dict[str, Any]]] = {}
        jobs: list[dict[str, Any]] = []

        toc_candidates: list[str] = []
        if toc_urls:
            toc_candidates.extend([u for u in toc_urls if u])
        if toc_list:
            toc_candidates.extend(_load_toc_urls_from_file(toc_list))
        toc_candidates = _dedupe_preserve([u.strip() for u in toc_candidates if u.strip()])

        for idx, toc_url in enumerate(toc_candidates):
            if test_mode and idx >= TEST_TOC_FILES:
                break
            toc_jobs = await _process_table_of_contents(
                toc_url,
                classes,
                test_mode,
                plan_ids=plan_ids,
                plan_name_contains=plan_name_contains,
                plan_market_types=plan_market_types,
                import_run_id=import_run_id,
                reuse_raw_artifacts=reuse_raw_artifacts,
                max_bytes=max_bytes,
                keep_partial_artifacts=keep_partial_artifacts,
            )
            jobs.extend(toc_jobs)

        if provider_ref_url:
            provider_ref_cache.update(
                await _process_provider_reference_file(
                    provider_ref_url,
                    classes,
                    test_mode,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    import_run_id=import_run_id,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            )

        if in_network_url:
            jobs.append({"type": "in_network", "url": in_network_url})
        if allowed_url:
            jobs.append({"type": "allowed_amounts", "url": allowed_url})
        jobs = _filter_jobs_by_url_contains(jobs, file_url_contains)

        seen_jobs: set[tuple[str, str]] = set()
        processed_files = 0
        attempted_files = 0
        failed_files: list[dict[str, Any]] = []
        for job in jobs:
            job_key = (job.get("type"), job.get("url"))
            if job_key in seen_jobs:
                continue
            seen_jobs.add(job_key)
            if max_files is not None and attempted_files >= max_files:
                break
            result: PTG2FileProcessResult | None = None
            if job.get("type") == "in_network":
                attempted_files += 1
                result = await _process_in_network_file(
                    job,
                    classes,
                    provider_ref_cache,
                    test_mode,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    max_items=max_items,
                    import_run_id=import_run_id,
                    keep_partial_artifacts=keep_partial_artifacts,
                    compact_import=compact_import,
                    snapshot_id=snapshot_id,
                    import_month=import_month_value,
                )
            elif job.get("type") == "allowed_amounts":
                attempted_files += 1
                result = await _process_allowed_amounts_file(
                    job,
                    classes,
                    test_mode,
                    reuse_raw_artifacts=reuse_raw_artifacts,
                    max_bytes=max_bytes,
                    max_items=max_items,
                    import_run_id=import_run_id,
                    keep_partial_artifacts=keep_partial_artifacts,
                )
            if result is None:
                continue
            if result.success:
                processed_files += 1
            else:
                failed_files.append(asdict(result))

        failure_report = {
            "jobs_discovered": len(jobs),
            "files_attempted": attempted_files,
            "files_processed": processed_files,
            "files_failed": len(failed_files),
            "failed_files": failed_files,
            "snapshot_id": snapshot_id,
            "legacy_table_suffix": import_id_val,
        }
        if jobs and processed_files == 0:
            raise RuntimeError(
                f"PTG2 import discovered {len(jobs)} job(s) but processed zero files successfully"
            )

        await flush_error_log(classes["ImportLog"])
        if compact_import and _use_compact_serving_table():
            serving_index = await build_ptg2_compact_serving_index(snapshot_id, import_run_id)
        elif compact_import and _use_stage_serving_as_final():
            serving_index = await build_ptg2_stage_serving_index(snapshot_id, import_run_id)
        else:
            serving_index = (
                await finalize_ptg2_incremental_serving_index(snapshot_id)
                if compact_import
                else await build_ptg2_snapshot_index_artifact(classes, snapshot_id, import_run_id)
            )
        if compact_import and serving_index is None:
            serving_index = await build_ptg2_db_serving_index(snapshot_id, import_run_id)
        finished = _utcnow()
        previous_snapshot_id = None
        try:
            row = await (
                db.select(PTG2CurrentSnapshot.__table__.c.snapshot_id)
                .where(PTG2CurrentSnapshot.__table__.c.slot == "current")
                .first()
            )
            if row is not None:
                previous_snapshot_id = row[0]
        except Exception as exc:
            logger.debug("No PTG2 current snapshot found before publish: %s", exc)
        report_payload = {**failure_report, "serving_index": serving_index}
        await _push_ptg2_objects(
            [
                {
                    "snapshot_id": snapshot_id,
                    "import_run_id": import_run_id,
                    "import_month": import_month_value,
                    "status": PTG2_STATUS_PUBLISHED,
                    "created_at": now,
                    "validated_at": finished,
                    "published_at": finished,
                    "previous_snapshot_id": previous_snapshot_id,
                    "manifest": report_payload,
                }
            ],
            PTG2Snapshot,
            rewrite=True,
        )
        await _push_ptg2_objects(
            [
                {
                    "slot": "current",
                    "snapshot_id": snapshot_id,
                    "previous_snapshot_id": previous_snapshot_id,
                    "updated_at": finished,
                }
            ],
            PTG2CurrentSnapshot,
            rewrite=True,
        )
        await _push_ptg2_objects(
            [
                {
                    "import_run_id": import_run_id,
                    "import_month": import_month_value,
                    "status": PTG2_STATUS_VALIDATED,
                    "started_at": now,
                    "finished_at": finished,
                    "heartbeat_at": finished,
                    "options": options_payload,
                    "report": report_payload,
                    "error": None,
                }
            ],
            PTG2ImportRun,
            rewrite=True,
        )
    except Exception as exc:
        await _mark_ptg2_import_failed(
            import_run_id,
            snapshot_id,
            import_month_value,
            now,
            exc,
            report=failure_report,
            options=options_payload,
        )
        raise


__all__ = [
    "PTG2ArtifactStore",
    "PTG2ContentIdentityValue",
    "PTG2ContractEvent",
    "PTG2FileProcessResult",
    "PTG2HeadMetadata",
    "PTG2LogicalArtifact",
    "PTG2PriceAtomEvent",
    "PTG2PriceSetValue",
    "PTG2ProcedureEvent",
    "PTG2ProviderGroupEvent",
    "PTG2InMemoryProviderReferenceCache",
    "PTG2ProviderReferenceCache",
    "PTG2ProviderSetValue",
    "PTG2RawArtifact",
    "PTG2RatePackValue",
    "PTG2SourceCatalogEntry",
    "PTG2SourceTraceSetValue",
    "PTG2SourceVersion",
    "build_fact_chunk",
    "build_price_atom",
    "build_price_set",
    "build_procedure_collection",
    "build_provider_set",
    "build_provider_set_collection",
    "build_ptg2_db_serving_index",
    "build_ptg2_compact_snapshot_index_artifact",
    "build_ptg2_compact_serving_index",
    "build_ptg2_stage_serving_index",
    "build_rate_pack",
    "build_rate_pack_group",
    "build_rate_pack_procedure_group",
    "build_rate_set",
    "build_ptg2_snapshot_index_artifact",
    "build_source_trace_set",
    "canonical_json_dumps",
    "canonicalize_url",
    "choose_reusable_raw_artifact",
    "content_addressed_path",
    "download_raw_artifact",
    "ensure_ptg2_tables",
    "fetch_head_metadata",
    "finalize_ptg2_incremental_serving_index",
    "hash_prefix",
    "logical_artifact_identity",
    "main",
    "materialize_json_source",
    "normalize_date",
    "normalize_import_month",
    "normalize_money",
    "normalize_ptg2_search_mode",
    "parse_toc_catalog_entries",
    "provider_hash_bucket",
    "ptg2_provider_bucket_count",
    "ptg2_confidence_statement",
    "open_json_artifact_stream",
    "semantic_hash",
    "sha256_bytes",
    "sha256_file",
    "stream_logical_artifact",
]
