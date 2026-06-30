# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lightweight payer MRF source discovery and freshness catalog import."""

from __future__ import annotations

import asyncio
import csv
import datetime as dt
import gzip
import html
import io
import ipaddress
import json
import logging
import os
import re
import socket
import ssl
import zipfile
from dataclasses import dataclass, field, replace
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import (
    parse_qs,
    parse_qsl,
    quote,
    unquote,
    urlencode,
    urljoin,
    urlsplit,
)
from xml.etree import ElementTree

import aiohttp
from sqlalchemy import func, or_, select, update

from db.connection import init_db
from db.models import (
    MRFCrawlRun,
    MRFFile,
    MRFPayer,
    MRFPayerScorecard,
    MRFPlan,
    MRFSource,
    MRFUrlObservation,
    db,
)
from process.ext.utils import ensure_database, push_objects
from process.import_status_events import enqueue_status_event, flush_status_events
from process.live_progress import enqueue_live_progress
from process.ptg_parts.canonical import canonicalize_url, semantic_hash
from process.ptg_parts.source_jobs import parse_toc_catalog_entries

SOURCE_CONFIG_ENV = "HLTHPRT_MRF_DISCOVERY_SOURCE_CONFIG"
DEFAULT_SOURCE_CONFIG = Path("specs/mrf_source_discovery_sources.json")
DISCOVERY_TABLES = (
    MRFPayer,
    MRFSource,
    MRFPlan,
    MRFFile,
    MRFCrawlRun,
    MRFPayerScorecard,
    MRFUrlObservation,
)
MAX_TOC_BYTES_DEFAULT = int(
    os.getenv("HLTHPRT_MRF_DISCOVERY_MAX_TOC_BYTES", str(25 * 1024 * 1024))
)
DEFAULT_CONCURRENCY = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_CONCURRENCY", "10")), 1)
WRITE_BATCH_SIZE = max(
    int(os.getenv("HLTHPRT_MRF_DISCOVERY_WRITE_BATCH_SIZE", "2000")), 1
)
HTTP_TOTAL_TIMEOUT = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_HTTP_TIMEOUT", "300")), 1)
HTTP_READ_TIMEOUT = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_READ_TIMEOUT", "120")), 1)
DEFAULT_FILE_PROBE_TYPES = ("in-network", "allowed-amounts")
USER_AGENT = "HealthPorta mrf-source-discovery/1.0"
BROWSER_FALLBACK_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
_BROWSER_FALLBACK_HTTP_STATUSES = {403, 406}
MRF_URL_OBSERVATION_NULLABLE_KEYS = (
    "canonical_url",
    "url_type",
    "http_status",
    "etag",
    "last_modified",
    "content_length",
    "content_type",
    "final_url",
    "checked_at",
    "error",
    "metadata_json",
)
_SOURCE_CONFIG_CACHE: dict[str, Any] | None = None
_SSL_CONTEXT: ssl.SSLContext | None = None
INCOMPLETE_TLS_CHAIN_HOSTS_ENV = "HLTHPRT_INCOMPLETE_TLS_CHAIN_HOSTS"
DEFAULT_INCOMPLETE_TLS_CHAIN_HOSTS = frozenset({"api.midlandschoice.com"})
DEFAULT_SOURCE_QUERY_EXPANSION_PLATFORMS = (
    "sapphire",
    "aetna_health1",
    "uhc_public_blobs",
    "mymedicalshopper_talon",
)
LEGAL_ENTITY_QUERY_STOPWORDS = {
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "llc",
    "ltd",
    "limited",
}


@dataclass(frozen=True)
class SourceCandidate:
    payer_name: str
    provider: str
    source_url: str | None = None
    index_url: str | None = None
    human_url: str | None = None
    status: str = "needs_review"
    source_type: str = "community_index"
    access_model: str = "free"
    hosting_platform: str | None = None
    parent_group: str | None = None
    entity_type: str | None = None
    aliases: tuple[str, ...] = ()
    benefit_lines: tuple[str, ...] = ("medical",)
    source_tier: str = "mrf_importable"
    states: tuple[str, ...] = ()
    eins: tuple[str, ...] = ()
    source_coverage: tuple[str, ...] = ()
    vendor_names: tuple[str, ...] = ()
    network_names: tuple[str, ...] = ()
    plan_names: tuple[str, ...] = ()
    confidence: int | None = None
    license_status: str = "public_directory"
    review_status: str = "pending"
    num_plans: int | None = None
    num_files: int | None = None
    num_indices: int | None = None
    latest_index_date: str | None = None
    total_compressed_size: int | None = None
    raw_payload: dict[str, Any] = field(default_factory=dict)


@dataclass
class DiscoveryResult:
    providers: list[str]
    candidates: int = 0
    payers: int = 0
    sources: int = 0
    urls_checked: int = 0
    plans: int = 0
    files: int = 0
    files_probed: int = 0
    file_probe_ok: int = 0
    crawl_run_id: str | None = None
    import_control_synced: int = 0
    import_control_sources_synced: int = 0
    import_control_plans_synced: int = 0
    errors: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "providers": self.providers,
            "candidates": self.candidates,
            "payers": self.payers,
            "sources": self.sources,
            "urls_checked": self.urls_checked,
            "plans": self.plans,
            "files": self.files,
            "files_probed": self.files_probed,
            "file_probe_ok": self.file_probe_ok,
            "crawl_run_id": self.crawl_run_id,
            "import_control_synced": self.import_control_synced,
            "import_control_sources_synced": self.import_control_sources_synced,
            "import_control_plans_synced": self.import_control_plans_synced,
            "errors": self.errors,
        }


@dataclass(frozen=True)
class CrawlTarget:
    source: dict[str, Any]
    url: str
    label: str | None = None
    resolved_from_url: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC).replace(tzinfo=None)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _source_config_path() -> Path:
    configured = os.getenv(SOURCE_CONFIG_ENV)
    path = Path(configured) if configured else DEFAULT_SOURCE_CONFIG
    return path if path.is_absolute() else _repo_root() / path


def _default_ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT  # pylint: disable=global-statement
    if _SSL_CONTEXT is not None:
        return _SSL_CONTEXT
    try:
        import certifi  # pylint: disable=import-outside-toplevel

        _SSL_CONTEXT = ssl.create_default_context(cafile=certifi.where())
    except Exception:  # pylint: disable=broad-exception-caught
        _SSL_CONTEXT = ssl.create_default_context()
    return _SSL_CONTEXT


def _tcp_connector(limit: int) -> aiohttp.TCPConnector:
    return aiohttp.TCPConnector(
        limit=limit,
        family=socket.AF_INET,
        ttl_dns_cache=300,
        ssl=_default_ssl_context(),
    )


def _incomplete_tls_chain_hosts() -> set[str]:
    raw = os.getenv(INCOMPLETE_TLS_CHAIN_HOSTS_ENV)
    if raw is None:
        return set(DEFAULT_INCOMPLETE_TLS_CHAIN_HOSTS)
    return {
        value.strip().lower()
        for value in re.split(r"[, ]+", raw)
        if value.strip()
    }


def _request_ssl_kwargs(url: str | None) -> dict[str, Any]:
    parsed = urlsplit(str(url or "").strip())
    host = (parsed.hostname or "").lower()
    if parsed.scheme == "https" and host in _incomplete_tls_chain_hosts():
        return {"ssl": False}
    return {}


def _source_config() -> dict[str, Any]:
    global _SOURCE_CONFIG_CACHE  # pylint: disable=global-statement
    if _SOURCE_CONFIG_CACHE is None:
        _SOURCE_CONFIG_CACHE = json.loads(
            _source_config_path().read_text(encoding="utf-8")
        )
    return _SOURCE_CONFIG_CACHE


def _provider_config(provider: str) -> dict[str, Any]:
    providers = _source_config().get("providers") or {}
    config = providers.get(provider)
    if not isinstance(config, dict):
        raise ValueError(f"unsupported provider: {provider}")
    return config


def _platform_resolver_config(platform: str | None) -> dict[str, Any]:
    if not platform:
        return {}
    resolvers = _source_config().get("platform_resolvers") or {}
    config = resolvers.get(str(platform))
    return dict(config) if isinstance(config, dict) else {}


def _source_query_expansion_platforms() -> set[str]:
    values = _source_config().get("source_query_expansion_platforms")
    if not isinstance(values, list):
        values = list(DEFAULT_SOURCE_QUERY_EXPANSION_PLATFORMS)
    return {
        str(value or "").strip()
        for value in values
        if str(value or "").strip()
    }


def _seed_list_config(name: str | None) -> dict[str, Any]:
    seed_list_name = str(name or "").strip()
    if not seed_list_name:
        raise ValueError("seed list name is required")
    seed_lists = _source_config().get("seed_lists") or {}
    config = seed_lists.get(seed_list_name)
    if not isinstance(config, dict):
        raise ValueError(f"unsupported seed list: {seed_list_name}")
    return dict(config)


def _load_seed_list_rows(name: str | None) -> list[dict[str, str]]:
    config = _seed_list_config(name)
    schema = str(config.get("schema") or "").strip()
    if schema != "group_number_seed_v1":
        raise ValueError(f"unsupported seed list schema: {schema or 'missing'}")
    path = _resolve_config_path(str(config.get("path") or ""))
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = set(reader.fieldnames or ())
        required = {"group_number", "status"}
        missing = sorted(required - fieldnames)
        if missing:
            raise ValueError(
                f"seed list {name} missing required column(s): {', '.join(missing)}"
            )
        return [
            {key: str(value or "").strip() for key, value in row.items()}
            for row in reader
        ]


def _configured_provider_names(key: str) -> list[str]:
    values = _source_config().get(key) or []
    return [str(item).strip().lower() for item in values if str(item).strip()]


def _resolve_config_path(value: str | None) -> Path:
    if not value:
        raise ValueError("provider path is required")
    path = Path(value)
    return path if path.is_absolute() else _repo_root() / path


def _required_url(config: dict[str, Any]) -> str:
    url = str(config.get("url") or "").strip()
    if not url:
        raise ValueError("provider URL is required")
    return url


def _id(prefix: str, payload: Any) -> str:
    return f"{prefix}_{semantic_hash(payload, domain=prefix)}"


def _clean_text(value: Any) -> str:
    text = html.unescape(str(value or "")).replace("\xa0", " ")
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("**", "").replace("*", "").replace("`", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip()


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return text[:80] or "mrf-source"


def _http_urls(value: Any) -> tuple[str, ...]:
    urls: list[str] = []
    for match in re.finditer(r"https?://[^\s|)<>]+", str(value or "")):
        url = html.unescape(match.group(0)).rstrip(".,;…")
        if url and url not in urls:
            urls.append(url)
    return tuple(urls)


def _first_http_url(value: Any) -> str | None:
    urls = _http_urls(value)
    return urls[0] if urls else None


def _is_placeholder_source_url(url: str | None) -> bool:
    raw = str(url or "")
    return "{" in raw or "}" in raw or "`" in raw


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    return urlsplit(str(url)).netloc.lower() or None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _truncate_text(value: Any, limit: int) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return text[:limit]


def _parse_size_bytes(value: Any) -> int | None:
    parsed = _as_int(value)
    if parsed is not None:
        return parsed
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    match = re.match(
        r"^(?P<number>\d+(?:\.\d+)?)\s*(?P<unit>[kmgtp]?i?b|bytes?)?$", text, flags=re.I
    )
    if not match:
        return None
    number = float(match.group("number"))
    unit = (match.group("unit") or "b").lower()
    multipliers = {
        "b": 1,
        "byte": 1,
        "bytes": 1,
        "kb": 1000,
        "mb": 1000**2,
        "gb": 1000**3,
        "tb": 1000**4,
        "pb": 1000**5,
        "kib": 1024,
        "mib": 1024**2,
        "gib": 1024**3,
        "tib": 1024**4,
        "pib": 1024**5,
    }
    multiplier = multipliers.get(unit)
    return int(number * multiplier) if multiplier else None


def _query_mrf_file_name(url: str | None) -> str | None:
    parsed = urlsplit(str(url or ""))
    for key, values in parse_qs(parsed.query).items():
        if key.lower() not in {"file", "filename", "file_name", "name"}:
            continue
        for value in values:
            file_name = _clean_text(value)
            lower = file_name.lower()
            if not lower.endswith((".json", ".json.gz", ".zip", ".7z", ".csv")):
                continue
            if any(
                token in lower.replace("_", "-")
                for token in (
                    "allowed",
                    "in-network",
                    "out-of-network",
                    "negotiated",
                    "rate",
                    "rates",
                    "index",
                    "table-of-contents",
                )
            ):
                return file_name
    return None


def _container_format(url: str | None) -> str | None:
    path = (_query_mrf_file_name(url) or urlsplit(str(url or "")).path).lower()
    if path.endswith(".zip"):
        return "zip"
    if path.endswith(".7z"):
        return "7z"
    if path.endswith((".json.gz", ".gz")):
        return "gzip"
    return None


def _mrf_file_type_from_text(url: str | None, label: str | None = None) -> str | None:
    text = f"{url or ''} {label or ''}".lower().replace("_", "-")
    compact = re.sub(r"[^a-z0-9]+", "", text)
    if "table-of-content" in text:
        return "table-of-contents"
    if (
        "allowed-amount" in text
        or "allowed amount" in text
        or "allowedamount" in compact
        or "out-of-network" in text
        or "out-network" in text
        or "out of network" in text
        or "out network" in text
        or "outofnetwork" in compact
        or "outnetwork" in compact
        or re.search(r"(^|[-/_.])oon([-/_.]|$)", text)
    ):
        return "allowed-amounts"
    if (
        "in-network" in text
        or "in network" in text
        or "innetwork" in compact
        or "negotiated-rate" in text
    ):
        return "in-network"
    if "payer-drug" in text or "prescription-drug" in text or "drug-file" in text:
        return "payer-drug"
    if (
        "table-of-content" in text
        or "toc" in text
        or re.search(r"(^|[/_-])index(?:\.json|[/?#]|$)", text)
    ):
        return "table-of-contents"
    file_name = Path(urlsplit(str(url or "")).path.lower()).name.replace("_", "-")
    if re.search(r"(^|-)index(?:-[a-z0-9]+)?\.zip$", file_name) and any(
        token in text
        for token in ("mrf", "machine-readable", "price-transparency", "transparency")
    ):
        return "table-of-contents"
    return None


_MONTH_NAME_TO_NUMBER = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _looks_non_tic_mrf_reference(url: str | None, label: str | None = None) -> bool:
    parsed = urlsplit(str(url or ""))
    path = parsed.path.lower().replace("_", "-")
    file_name = Path(path).name
    text = f"{path} {parsed.query.lower()} {label or ''}".lower().replace("_", "-")
    if (
        file_name.endswith((".css", ".css.gz", ".js", ".js.gz", ".map", ".map.gz"))
        or ".min.js" in file_name
        or ".bundle.js" in file_name
    ):
        return True
    if any(token in text for token in ("balance-billing", "out-of-network-liability")):
        return True
    if "cms-data-index" in path:
        return True
    if "hospital-price-transparency" in text or "standardcharges" in file_name:
        return True
    if any(
        token in text
        for token in (
            "formulary",
            "provider-data",
            "provider-directory",
            "provider-network",
            "provider-search",
        )
    ):
        return True
    if re.match(r"^\d{4}-\d{2}-\d{2}[-_].*[-_]index\.json$", file_name) and re.search(
        r"/(?:mrf|mrfs)/", path
    ):
        return False
    tic_index_file = bool(
        re.match(r"^\d{4}-\d{2}-\d{2}[-_].*[-_]index\.json(?:\.gz)?$", file_name)
        and any(token in text for token in ("mrf", "price-transparency", "transparency"))
    )
    if (
        _mrf_file_type_from_text(url, label) == "table-of-contents" or tic_index_file
    ) and any(
        token in text
        for token in (
            "mrf",
            "machine-readable",
            "price-transparency",
            "transparency",
            "table-of-content",
            "table of content",
        )
    ):
        return False
    if re.search(
        r"(^|[-/_.])(?:providers?|plans?|drugs?|rx-plan)(?:[-/_.]|\d|$)",
        file_name,
        flags=re.I,
    ) and not any(
        token in text
        for token in (
            "allowed",
            "in-network",
            "out-of-network",
            "negotiated",
            "rate",
            "rates",
            "price-transparency",
            "transparency",
            "table-of-contents",
            "toc",
        )
    ):
        return True
    return False


def _mrf_body_file_type_from_text(
    url: str | None, label: str | None = None
) -> str | None:
    if _looks_non_tic_mrf_reference(url, label):
        return None
    inferred = _mrf_file_type_from_text(url, label)
    if inferred:
        return inferred
    if not _looks_direct_mrf_body_url(url):
        return None
    parsed = urlsplit(str(url or ""))
    path = parsed.path.lower().replace("_", "-")
    file_name = Path(path).name
    if "mrf" in file_name or re.search(r"/(?:mrf|mrfs)(?:/|$)", path):
        return "in-network"
    if any(token in path for token in ("/machine-readable", "/transparency")):
        return "in-network"
    return None


def _mrf_file_type_from_html_section(html_fragment: str | None) -> str | None:
    text = _strip_html_tags(html_fragment or "").lower().replace("_", "-")
    if not text:
        return None
    matches: list[tuple[int, str]] = []
    for file_type, patterns in (
        (
            "allowed-amounts",
            (
                r"\bout[-\s]+of[-\s]+network\b",
                r"\bout[-\s]+of[-\s]+network\s+allowed(?:\s+amounts?)?",
                r"\ballowed\s+amounts?(?:\s+and\s+billed\s+charges)?",
                r"\ballowed[-\s]+amt(?:[-\s]+details)?",
            ),
        ),
        (
            "in-network",
            (
                r"\bin[-\s]+network\b",
                r"\bin[-\s]+network\s+(?:provider\s+)?rates?\b",
                r"\bin[-\s]+network\s+negotiated\b",
                r"\bnegotiated\s+rates?\b",
            ),
        ),
    ):
        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.I):
                matches.append((match.start(), file_type))
    if not matches:
        return None
    return max(matches, key=lambda item: item[0])[1]


def _canonical_or_none(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return canonicalize_url(url)
    except Exception:
        return str(url).strip() or None


def classify_hosting_platform(url: str | None) -> str | None:
    host = _domain(url) or ""
    raw = str(url or "").lower()
    path = urlsplit(str(url or "")).path.lower()
    if host == "mrfsearch.meritain.com":
        return "meritain_mrf_search"
    if host == "mrf.healthcarebluebook.com":
        return "healthcarebluebook_mrf"
    if host == "clm.magnacare.com" and path.startswith("/transparency"):
        return "magnacare_transparency_mrf"
    if host == "caa.ebms.com":
        return "ebms_caa_directory"
    if host == "data.sccgov.org" and path == "/data.json":
        return "socrata_data_json_mrf_catalog"
    if host.endswith("sapphiremrfhub.com"):
        return "sapphire"
    if (
        _looks_direct_toc_url(raw)
        and _mrf_file_type_from_text(raw) == "table-of-contents"
    ):
        return "direct_toc"
    if _looks_direct_mrf_body_url(raw) and _mrf_body_file_type_from_text(raw):
        return "direct_mrf_body"
    if host == "mrf.healthgram.com":
        return "healthgram"
    if host == "github.com" and len([part for part in path.split("/") if part]) >= 2:
        return "github_repo_mrf"
    if host == "developers.humana.com" and (
        "cost-transparency" in path
        or "healthplan-price-transparency" in path
        or path.endswith("/resource/pctfileslist")
        or path.endswith("/resource/getdata")
    ):
        return "humana_pct_file_list"
    if host in {"www.fchn.com", "fchn.com"} and (
        path.startswith("/machine-readable-files")
        or path.startswith("/payorsearch")
    ):
        return "fchn_payor_search"
    if host in {"ehptransparency.org", "www.ehptransparency.org"}:
        return "html_mrf_links"
    if host in {"insightba.net", "www.insightba.net"} and "transparency-in-coverage" in path:
        return "insightba_html_mrf_links"
    if host == "api.midlandschoice.com" and path.startswith("/mrf"):
        return "midlandschoice_mrf"
    if host in {"www.vivahealth.com", "vivahealth.com"} and (
        path.startswith("/mrf")
        or path.startswith("/files/mrf/viva-health-commercial-")
    ):
        return "viva_health_mrf"
    if (
        host
        in {
            "www.anthem.com",
            "anthem.com",
            "www.empireblue.com",
            "empireblue.com",
            "www.healthlink.com",
            "healthlink.com",
            "www.unicaremass.com",
            "unicaremass.com",
        }
        and "/machine-readable-file/search" in path
    ):
        return "anthem_s3_mrf"
    if host in {"www.hcsc.com", "hcsc.com"} and "transparency-in-coverage" in path:
        return "hcsc_asomrf_landing"
    if (
        host in {"bcbsglobalsolutions.com", "www.bcbsglobalsolutions.com"}
        and "transparency-in-coverage" in path
    ) or (
        host == "groupadmin.bcbsglobalsolutions.com"
        and path.startswith("/transparency-in-coverage")
    ):
        return "bcbs_global_solutions_mrf"
    if (
        (
            host in {"www.harvardpilgrim.org", "harvardpilgrim.org"}
            and "machine-readable-files" in path
        )
        or (
            host in {"tuftshealthplan.com", "www.tuftshealthplan.com"}
            and "machine-readable-files" in path
        )
        or (
            host
            in {
                "massgeneralbrighamhealthplan.org",
                "www.massgeneralbrighamhealthplan.org",
            }
            and "transparency" in path
        )
    ):
        return "point32_azure_mrf_directory"
    if (
        (host in {"www.pbaclaims.com", "pbaclaims.com"} and path.startswith("/mrfs"))
        or (
            host in {"healthy.kaiserpermanente.org", "www.kaiserpermanente.org"}
            and "machine-readable" in path
        )
        or (
            host in {"www.alliedbenefit.com", "alliedbenefit.com"}
            and "transparency" in path
        )
        or (host in {"www.webtpa.com", "webtpa.com"} and "rights-protections" in path)
        or (
            host in {"www.healthnet.com", "healthnet.com"}
            and "transparency-files" in path
        )
        or (
            host in {"www.bcbsnd.com", "bcbsnd.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"www.lacare.org", "lacare.org"}
            and "transparency-coverage-machine-readable-files" in path
        )
        or (
            host in {"www.gravie.com", "gravie.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"www.priorityhealth.com", "priorityhealth.com"}
            and path.startswith("/landing/transparency")
        )
        or (
            host in {"www.mvphealthcare.com", "mvphealthcare.com"}
            and "machine-readable-files" in path
        )
        or (
            host in {"www.novahealthcare.com", "novahealthcare.com"}
            and path.startswith("/resources/mrf")
        )
        or (host in {"www.hnas.com", "hnas.com"} and "machine-readable-files" in path)
        or (
            host in {"www.anglehealth.com", "anglehealth.com"}
            and "machine-readable-files" in path
        )
        or (
            host in {"apatpa.com", "www.apatpa.com"}
            and "disclosures" in path
            and "american-plan-administrators" in path
        )
        or (
            host in {"www.simplepayhealth.com", "simplepayhealth.com"}
            and path in {"", "/"}
        )
        or host == "transparency.abadmin.com"
    ):
        return "html_delegated_mrf_links"
    if host.endswith(".mrf.payercompass.com") or host == "mrf.payercompass.com":
        return "payercompass_mrf"
    if (
        host in {"www.molinamarketplace.com", "molinamarketplace.com"}
        and "pricingtransparency" in path
    ):
        return "html_mrf_links"
    if (
        host in {"thealliance.health", "www.thealliance.health"}
        and "transparency-in-coverage-cms-9915-machine-readable-files" in path
    ):
        return "html_mrf_links"
    if (
        host in {"alamedaalliance.org", "www.alamedaalliance.org"}
        and "pricing-transparency" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.arkansasbluecross.com", "arkansasbluecross.com"}
        and "machine-readable-files" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.healthadvantage-hmo.com", "healthadvantage-hmo.com"}
        and "machine-readable-files" in path
    ):
        return "html_mrf_links"
    if host == "sisconosurprise.com" and path.startswith("/ppo/"):
        return "html_mrf_links"
    if host == "caa.imagine360.com" and path.endswith("/index.html"):
        return "html_mrf_links"
    if host == "portal.90degreebenefits.com" and path.startswith(
        "/memberportal/machinereadablefiles"
    ):
        return "healthspace_machine_readable_files"
    if host == "transparency-in-coverage.collectivehealth.com":
        return "html_mrf_links"
    if (
        host in {"www.modahealth.com", "modahealth.com"}
        and "machine-readable-files" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.selecthealth.org", "selecthealth.org"}
        and "machine-readable-data" in path
    ):
        return "html_mrf_links"
    if host in {"www.emihealth.com", "emihealth.com"} and path.startswith(
        "/machinereadables"
    ):
        return "html_mrf_links"
    if host in {"peakhealth.org", "www.peakhealth.org"}:
        if path.endswith(".zip") and re.search(r"[_-]index\.zip$", path):
            return "direct_toc"
        if path.startswith("/transparency"):
            return "html_mrf_links"
    if host == "eldoradocomputing.hosted-by-files.com" and path.startswith(
        "/centivopublic"
    ):
        return "html_mrf_links"
    if host == "boonchapman-mrf.zakipointhealth.com":
        return "html_mrf_links_mixed_directories"
    if (
        host in {"www.boonchapman.com", "boonchapman.com"}
        and "machine-readable-files" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.talltreeadmin.com", "talltreeadmin.com"}
        and "machine-readable-files" in path
    ):
        return "html_mrf_links"
    if host in {"www.ebam.com", "ebam.com"} and "machine-readable-files" in path:
        return "html_mrf_links"
    if (
        host in {"www.motivhealth.com", "motivhealth.com"}
        and "machinereadablefiles" in path
    ):
        return "html_mrf_links"
    if host in {"www.cbabluevt.com", "cbabluevt.com"} and "employer-resources" in path:
        return "html_mrf_links"
    if (
        host in {"tuition.ebpabenefits.com", "www.ebpabenefits.com", "ebpabenefits.com"}
        and "machine-readable-file" in path
    ):
        return "html_mrf_links"
    if host in {"healthezbenefits.com", "www.healthezbenefits.com"} and path.startswith(
        "/plandocuments"
    ):
        return "healthez_benefits_mrf"
    if host == "mrf.pacificsource.com" and path.startswith("/file/visit"):
        return "pacificsource_azure_mrf_listing"
    if host in {
        "sawus2prdticmrfhma.z5.web.core.windows.net",
        "sawus2prdticmrfhma.blob.core.windows.net",
    }:
        return "html_mrf_links"
    if (
        host in {"www.sanfordhealthplan.com", "sanfordhealthplan.com"}
        and "transparency-in-coverage-rule" in path
    ):
        return "html_mrf_links"
    if (
        (
            host in {"www.optimahealth.com", "optimahealth.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"www.healthpartners.com", "healthpartners.com"}
            and "transparency" in path
        )
    ):
        return "html_mrf_links"
    if (
        host in {"www.scrippshealthplan.com", "scrippshealthplan.com"}
        and "transparency-in-coverage" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.sutterhealthplan.org", "sutterhealthplan.org"}
        and (
            "healthcare-cost-transparency" in path
            or "technical-information" in path
        )
    ):
        return "sutter_health_plan_sitecore"
    if (
        host in {"www.sharphealthplan.com", "sharphealthplan.com"}
        and "api-access-for-developers" in path
    ):
        return "html_mrf_links"
    if (
        host in {"group-health.com", "www.group-health.com"}
        and "price-transparency" in path
    ):
        return "html_mrf_links"
    if (
        host in {"chorushealthplans.org", "www.chorushealthplans.org"}
        and "transparency-in-coverage" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.mclarenhealthplan.org", "mclarenhealthplan.org"}
        and "transparency-in-coverage" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.wpshealth.com", "wpshealth.com"}
        and "price-transparency" in path
    ):
        return "html_mrf_links"
    if (
        host in {"www.ucare.org", "ucare.org"}
        and "transparency-in-coverage" in path
    ):
        return "html_mrf_links"
    if host == "stmercycaremrf.z14.web.core.windows.net":
        return "html_mrf_links"
    if host == "files.myplancentral.com" and path.startswith("/tic/toc"):
        return "html_mrf_links"
    if host in {"www.healthplan.org", "healthplan.org"} and (
        path.startswith("/machine_readable_files")
        or path.startswith("/multiplan_mrfs")
        or path.endswith("_mrfs")
    ):
        return "healthplan_html_mrf_links"
    if (
        host in {"www.westernhealth.com", "westernhealth.com"}
        and "price-transparency" in path
    ):
        return "html_mrf_links"
    if host in {"www.firstchoicenext.com", "firstchoicenext.com"} and path.startswith(
        "/json"
    ):
        return "html_mrf_links"
    if (
        host in {"www.amerihealthcaritasnext.com", "amerihealthcaritasnext.com"}
        and path.startswith("/json")
    ):
        return "html_mrf_links"
    if host in {"www.avmed.org", "avmed.org"} and path.startswith("/en/for-developers"):
        return "avmed_html_mrf_links"
    if (
        host in {"www.centene.com", "centene.com"}
        and "price-transparency-files" in path
    ):
        return "html_mrf_links"
    if host in {"www.bcbsal.org", "bcbsal.org"} and path.startswith("/web/tcr"):
        return "bcbsal_html_mrf_links"
    if (
        (host in {"www.bcbsks.com", "bcbsks.com"} and path.startswith("/mrf"))
        or (host in {"www.bcbsm.com", "bcbsm.com"} and path.startswith("/mrf/index"))
        or (
            host in {"www.bcbsms.com", "bcbsms.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host
            in {
                "www.bcbsmt.com",
                "bcbsmt.com",
                "www.bcbsnm.com",
                "bcbsnm.com",
                "www.bcbsok.com",
                "bcbsok.com",
                "www.bcbstx.com",
                "bcbstx.com",
            }
            and "machine-readable-file" in path
        )
        or (
            host in {"www.bluecrossnc.com", "bluecrossnc.com"}
            and (
                "transparency-coverage-mrf" in path
                or "machine-readable-files" in path
            )
        )
        or (
            host in {"www.southcarolinablues.com", "southcarolinablues.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"www.bluecrossvt.org", "bluecrossvt.org"}
            and "machine-readable-files" in path
        )
        or (host in {"individual.carefirst.com"} and "machine-readable-file" in path)
        or (
            host in {"www.floridablue.com", "floridablue.com"}
            and "machine-readable-files" in path
        )
        or (
            host in {"www.asuris.com", "asuris.com", "www.regence.com", "regence.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"www.cdphp.com", "cdphp.com"}
            and "machine-readable-pricing-data-files" in path
        )
        or (
            host in {"www.healthoptions.org", "healthoptions.org"}
            and "policies-notices" in path
        )
        or (
            host in {"www.bswhealthplan.com", "bswhealthplan.com"}
            and path.startswith("/transparency")
        )
        or (
            host in {"capitalhealth.com", "www.capitalhealth.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"chppayment.christushealth.org"}
            and path.startswith("/documents/atex")
        )
        or (
            host in {"ebu.intermountainhealthcare.org"}
            and path.startswith("/selecthealth/transparencyincoverage")
        )
        or (
            host in {"www.wellsense.org", "wellsense.org"}
            and path.startswith("/about-us/interoperability")
        )
        or (
            host in {"www.securityhealth.org", "securityhealth.org"}
            and path.startswith("/insurance-resources/json")
        )
        or (
            host in {"www.nhpri.org", "nhpri.org"}
            and "price-transparency-machine-readable-files" in path
        )
        or (host in {"transparency.connecticare.com", "transparency.emblemhealth.com"})
        or (
            host in {"www.sidecarhealth.com", "sidecarhealth.com"}
            and "transparency-in-coverage" in path
        )
        or (
            host in {"metroplus.org", "www.metroplus.org"}
            and "machine-readable-files" in path
        )
        or (host in {"www.pehp.org", "pehp.org"} and "machinereadablefiles" in path)
        or (
            host in {"curative.com", "www.curative.com"}
            and "transparency-in-coverage-rates" in path
        )
        or (
            host in {"www.groupadministrators.com", "groupadministrators.com"}
            and "machinereadablefiles" in path
        )
        or (host == "mrf.mmsanalytics.com" and (path == "/" or path.endswith("/")))
    ):
        return "html_mrf_links"
    if host in {"www.hmsa.com", "hmsa.com"} and (
        "transparency-in-coverage-machine-readable-files" in path
    ):
        return "hmsa_monthly_toc"
    if (
        host in {"salud.grupotriples.com", "www.salud.grupotriples.com"}
        and (
            "transparency-in-coverage-machine-readable-files" in path
            or path.startswith("/en/wp-json/app/v1/mtt")
            or path.startswith("/wp-json/app/v1/mtt")
        )
    ):
        return "triples_mtt_api"
    if (
        host in {"www.uhahealth.com", "uhahealth.com"}
        and "transparency-in-coverage" in path
    ) or (host == "app.uhahealth.com" and path.startswith("/mrf")):
        return "uha_monthly_toc"
    if host == "mrfhub.providencehealthplan.com":
        return "providence_mrf_api"
    if host == "transparency.lacare.org":
        return "lacare_s3_listing"
    if host in {"www.bcbsri.com", "bcbsri.com"} and path.startswith("/developers"):
        return "bcbsri_azure_mrf_listing"
    if (
        host
        in {
            "hostedjson.z5.web.core.windows.net",
            "hostedjson.blob.core.windows.net",
        }
        or (
            host in {"www.cchealth.org", "cchealth.org"}
            and "transparency-in-coverage" in path
        )
    ):
        return "hostedjson_azure_mrf_listing"
    if host in {"alliantplans.com", "www.alliantplans.com"} and path.startswith(
        "/json/pt/"
    ):
        return "html_mrf_links"
    if host in {"www.bcbst.com", "bcbst.com"} and path.startswith("/tcr"):
        return "json_mrf_directory_links"
    if host == "price-transparency.webtpa.com":
        return "webtpa_mrf_api"
    if host in {
        "www.ibx.com",
        "ibx.com",
        "www.amerihealth.com",
        "amerihealth.com",
        "www.amerihealthnj.com",
        "amerihealthnj.com",
    } and path.startswith(("/cmstic", "/cmsticsvc/", "/developer-resources")):
        return "cmstic_file_info"
    if _looks_cmstic_keyed_toc_url(raw):
        return "cmstic_keyed_toc_redirect"
    if host in {"www.reliancematrix.com", "reliancematrix.com"} and (
        "transparency-in-coverage" in path
    ):
        return "html_delegated_mrf_links"
    if host == "www.myhealthbenefits.com" and path.startswith(
        "/myhealthbenefits/home/mrfs"
    ):
        return "html_mrf_with_healthcarebluebook"
    if host.endswith("lucenthealth.com") and path.startswith(
        "/transparency-in-coverage"
    ):
        return "html_mrf_with_healthcarebluebook"
    if (
        host in {"hpitpa.com", "www.hpitpa.com"}
        and "transparency-in-coverage-machine-readable-files" in path
    ):
        return "html_mrf_with_healthcarebluebook"
    if host == "transparency.auxiant.com" and not path.startswith(
        ("/wp-admin/", "/wp-content/", "/wp-includes/")
    ):
        return "auxiant_wordpress"
    if host in {"www.mymedicalshopper.com", "mymedicalshopper.com"} and path.startswith(
        "/mrf-search/diversified-group"
    ):
        return "mymedicalshopper_talon_bounded"
    if host in {"www.mymedicalshopper.com", "mymedicalshopper.com"} and path.startswith(
        ("/mrf-search/", "/mrf/")
    ):
        return "mymedicalshopper_talon"
    if host == "www.asrhealthbenefits.com" and path.startswith(
        ("/mrf", "/umbraco/surface/mrfdownload", "/home/umbraco/surface/mrfdownload")
    ):
        return "asr_health_benefits"
    if host in {"www.paisc.com", "paisc.com"} and "machine-readable-files" in path:
        return "html_mrf_links"
    if host in {"www.redirecthealth.com", "redirecthealth.com"} and (
        "machine-readable-data" in path
    ):
        return "html_mrf_links"
    if host in {"www.blueadvantagearkansas.com", "blueadvantagearkansas.com"} and (
        "machine-readable-files" in path
    ):
        return "blueadvantage_html_mrf_links"
    if host in {"www.geha.com", "geha.com"} and path.startswith(
        "/transparency-in-coverage"
    ):
        return "html_delegated_mrf_links"
    if host in {
        "transparency-in-coverage.uhc.com",
        "transparency-in-coverage.optum.com",
    }:
        return "uhc_public_blobs"
    if host == "providermrf.uhc.com" and path.startswith(
        ("/ifp", "/cs", "/api/files/ui/", "/api/stream/ui/")
    ):
        return "uhc_provider_mrf_files"
    if host == "transparency-in-coverage.bluecrossma.com":
        return "bcbsma_monthly_tocs"
    if (
        host in {"www.bcbswy.com", "bcbswy.com"}
        and "machine-readable-files" in path
    ):
        return "bcbswy_hmhs_monthly_toc"
    if (
        host in {"www.upmchealthplan.com", "upmchealthplan.com"}
        and "transparency-in-coverage/mrf" in path
    ):
        return "upmc_monthly_toc"
    if host.endswith("cigna.com") and (
        "machine-readable" in path or "/static/mrf/" in path
    ):
        return "cigna_static_mrf_lookup"
    if "healthsparq.com" in host:
        return "healthsparq"
    if "sapphiremrfhub.com" in host:
        return "sapphire"
    if (
        "health1.aetna.com" in host
        or "health1.firsthealth.com" in host
        or "health1.meritain.com" in host
    ):
        return "aetna_health1"
    if "mrfdata.hmhs.com" in host:
        return "highmark_hmhs"
    if "asomrf" in raw:
        return "bcbs_asomrf"
    if "changehealthcare.com" in host:
        return "change_healthcare"
    if "amazonaws.com" in host or ".s3." in host:
        return "s3"
    return "custom" if host else None


def _master_list_url_cell(
    cells: list[str], type_value: str | None
) -> tuple[str | None, str, str]:
    if len(cells) == 3:
        middle, tail = cells[1], cells[2]
        if (
            _http_urls(middle)
            or _is_placeholder_source_url(middle)
            or middle.strip() in {"", "-", "—"}
        ):
            return type_value, middle, tail
        return type_value, tail, ""
    second, third, fourth = cells[1], cells[2], cells[3]
    normalized_type = _normalize_entity_type(second) or type_value
    if (
        _http_urls(third)
        or _is_placeholder_source_url(third)
        or third.strip() in {"", "-", "—"}
    ):
        return normalized_type, third, fourth
    return normalized_type, fourth, ""


def _master_list_status(url: str | None, notes: str) -> str:
    if not url:
        return "needs_review"
    normalized = notes.lower()
    if "observed unsupported" in normalized:
        return "unsupported"
    if "observed archived" in normalized:
        return "archived"
    if "observed stale" in normalized:
        return "stale"
    return "active"


def _master_list_aliases(notes: str) -> tuple[str, ...]:
    return _master_list_note_values(notes, r"aliases?")


def _master_list_note_values(notes: str, label_pattern: str) -> tuple[str, ...]:
    match = re.search(
        rf"\b{label_pattern}\s*:\s*(?P<values>[^;]+)",
        notes or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return ()
    raw_values = match.group("values")
    try:
        parsed_values = next(csv.reader([raw_values], skipinitialspace=True))
    except (csv.Error, StopIteration):
        parsed_values = raw_values.split(",")
    values: list[str] = []
    seen: set[str] = set()
    for raw_value in parsed_values:
        value = _clean_text(raw_value)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(value)
    return tuple(values)


_SOURCE_TIER_ALIASES = {
    "": "mrf_importable",
    "mrf": "mrf_importable",
    "mrf_importable": "mrf_importable",
    "importable": "mrf_importable",
    "tic": "mrf_importable",
    "coverage": "coverage_evidence",
    "coverage_evidence": "coverage_evidence",
    "aca": "coverage_evidence",
    "employer": "coverage_evidence",
    "sbc": "coverage_evidence",
    "benefit": "coverage_evidence",
    "benefits": "coverage_evidence",
    "directory": "directory_evidence",
    "directory_evidence": "directory_evidence",
    "provider_directory": "directory_evidence",
    "vendor_directory": "directory_evidence",
}


def _normalize_source_tier(value: Any) -> str:
    text = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return _SOURCE_TIER_ALIASES.get(text, "mrf_importable")


def _master_list_source_tier(notes: str) -> str:
    values = _master_list_note_values(notes, r"source[-_\s]*tier")
    return _normalize_source_tier(values[0] if values else None)


_BENEFIT_LINE_ALIASES = {
    "medical": "medical",
    "health": "medical",
    "healthcare": "medical",
    "dental": "dental",
    "vision": "vision",
    "optical": "vision",
    "mixed": "mixed",
    "unknown": "unknown",
}


def _normalize_benefit_lines(values: Iterable[Any]) -> tuple[str, ...]:
    lines: list[str] = []
    seen: set[str] = set()
    for value in values:
        for raw_part in re.split(r"[,/]+|\band\b", str(value or ""), flags=re.I):
            key = _clean_text(raw_part).lower().replace(" ", "_")
            normalized = _BENEFIT_LINE_ALIASES.get(key)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            lines.append(normalized)
    return tuple(lines)


def _benefit_line_values(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _master_list_benefit_lines(notes: str) -> tuple[str, ...]:
    match = re.search(
        r"\bbenefit[-_\s]*lines?\s*:\s*(?P<benefit_lines>[^;]+)",
        notes or "",
        flags=re.IGNORECASE,
    )
    if not match:
        return ("medical",)
    return _normalize_benefit_lines([match.group("benefit_lines")]) or ("medical",)


def _infer_benefit_lines_from_text(*values: Any) -> tuple[str, ...]:
    text = " ".join(str(value or "") for value in values).lower().replace("_", "-")
    lines: list[str] = []
    if re.search(r"\b(dental|dentist|orthodont|connection-dental)\b", text):
        lines.append("dental")
    if re.search(
        r"\b(vision|optical|eyemed|vsp|spectera|davis-vision|superior-vision)\b",
        text,
    ):
        lines.append("vision")
    return tuple(dict.fromkeys(lines))


def _source_benefit_lines(source: dict[str, Any]) -> tuple[str, ...]:
    metadata = dict((source or {}).get("metadata_json") or {})
    return _normalize_benefit_lines(
        _benefit_line_values(metadata.get("benefit_lines"))
        + _benefit_line_values(metadata.get("benefit_line"))
    )


def _file_benefit_lines(source: dict[str, Any], *values: Any) -> tuple[str, ...]:
    return _infer_benefit_lines_from_text(*values) or _source_benefit_lines(source)


def _file_benefit_metadata(source: dict[str, Any], *values: Any) -> dict[str, Any]:
    lines = _file_benefit_lines(source, *values)
    if not lines:
        return {}
    metadata: dict[str, Any] = {"benefit_lines": list(lines)}
    if len(lines) == 1:
        metadata["benefit_line"] = lines[0]
    return metadata


def parse_master_list(markdown_text: str) -> list[SourceCandidate]:
    candidates: list[SourceCandidate] = []
    current_section = ""
    for raw_line in (markdown_text or "").splitlines():
        line = raw_line.strip()
        if line.startswith("## "):
            current_section = _clean_text(line.lstrip("# "))
            continue
        if not line.startswith("|") or "---" in line:
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 3 or cells[0].lower().startswith(("payer", "blue plan")):
            continue
        raw_payer_name = _clean_text(cells[0])
        payer_name = re.sub(r"^↳\s*", "", raw_payer_name).strip()
        payer_name = re.sub(r"\s+\([^)]*\)\s*$", "", payer_name).strip() or payer_name
        if not payer_name:
            continue
        type_value = _infer_entity_type(current_section, raw_payer_name)
        type_value, url_cell, notes_text = _master_list_url_cell(cells, type_value)
        urls = tuple(
            url for url in _http_urls(url_cell) if not _is_placeholder_source_url(url)
        ) or (None,)
        parent_group = _infer_parent_group(current_section, payer_name)
        for url in urls:
            candidates.append(
                SourceCandidate(
                    payer_name=payer_name,
                    provider="master-list",
                    source_url=str(_repo_root() / "specs" / "mrf_payer_master_list.md"),
                    index_url=url,
                    human_url=url,
                    status=_master_list_status(url, notes_text),
                    source_type="curated_registry",
                    access_model="free" if url else "unknown",
                    hosting_platform=classify_hosting_platform(url),
                    parent_group=parent_group,
                    entity_type=type_value,
                    aliases=_master_list_aliases(notes_text),
                    benefit_lines=_master_list_benefit_lines(notes_text),
                    source_tier=_master_list_source_tier(notes_text),
                    source_coverage=_master_list_note_values(
                        notes_text, r"source[-_\s]*coverage"
                    ),
                    vendor_names=_master_list_note_values(
                        notes_text, r"vendor[-_\s]*names?"
                    ),
                    network_names=_master_list_note_values(
                        notes_text, r"network[-_\s]*names?"
                    ),
                    plan_names=_master_list_note_values(
                        notes_text, r"plan[-_\s]*names?"
                    ),
                    confidence=85 if url else 45,
                    license_status="curated_public_research",
                    review_status="pending",
                    raw_payload={
                        "section": current_section,
                        "raw_payer_name": raw_payer_name,
                        "url_cell": url_cell,
                        "notes": notes_text,
                    },
                )
            )
    return _dedupe_candidates(candidates)


def _normalize_entity_type(value: Any) -> str | None:
    text = _clean_text(value).lower().replace(" ", "_")
    return text or None


def _infer_entity_type(section: str, payer_name: str) -> str | None:
    text = f"{section} {payer_name}".lower()
    if "bcbs" in text or "blue cross" in text or "blue shield" in text:
        return "blue"
    if "medicaid" in text or "mco" in text:
        return "medicaid_mco"
    if "tpa" in text:
        return "tpa"
    if "provider" in text:
        return "provider_sponsored"
    if "dtc" in text:
        return "dtc"
    if "national" in text:
        return "national"
    if "regional" in text:
        return "regional"
    return None


def _infer_parent_group(section: str, payer_name: str) -> str | None:
    text = f"{section} {payer_name}".lower()
    groups = {
        "united": "UHG",
        "elevance": "Elevance",
        "anthem": "Elevance",
        "aetna": "CVS/Aetna",
        "cigna": "Cigna",
        "humana": "Humana",
        "centene": "Centene",
        "hcsc": "HCSC",
        "highmark": "Highmark",
        "kaiser": "Kaiser",
        "molina": "Molina",
    }
    for needle, group in groups.items():
        if needle in text:
            return group
    if "bcbs" in text or "blue cross" in text or "blue shield" in text:
        return "BCBSA-independent"
    return None


def _dedupe_candidates(candidates: list[SourceCandidate]) -> list[SourceCandidate]:
    by_key: dict[tuple[str, str | None], SourceCandidate] = {}

    def rank(item: SourceCandidate) -> tuple[int, int, int]:
        status_rank = {
            "active": 5,
            "stale": 4,
            "needs_review": 3,
            "unsupported": 1,
            "archived": 0,
        }.get(str(item.status or "").lower(), 2)
        item_url = item.index_url or item.human_url or ""
        return (status_rank, 1 if item_url else 0, len(item_url))

    for item in candidates:
        key = (
            _clean_text(item.payer_name).lower(),
            _canonical_or_none(item.index_url or item.human_url),
        )
        previous = by_key.get(key)
        if previous is None or rank(item) > rank(previous):
            by_key[key] = item
    return list(by_key.values())


def _parse_provider_list(provider: str | None, *, test_mode: bool) -> list[str]:
    if not provider or provider == "all":
        return _configured_provider_names(
            "test_providers" if test_mode else "default_providers"
        )
    parts = [
        item.strip().lower() for item in re.split(r"[, ]+", provider) if item.strip()
    ]
    return parts or _configured_provider_names(
        "test_providers" if test_mode else "default_providers"
    )


async def _fetch_text(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
    expect_json: bool = False,
) -> str:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
        )
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as owned_session:
            return await _fetch_text(
                url, max_bytes=max_bytes, session=owned_session, expect_json=expect_json
            )
    class BrowserFallbackRequired(Exception):
        """Retry this public JSON URL with browser-compatible request headers."""

    async def read_response(resp: aiohttp.ClientResponse) -> tuple[bytes, str]:
        await _assert_fetch_url_allowed(str(resp.url))
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if expect_json and resp.status in _BROWSER_FALLBACK_HTTP_STATUSES:
            raise BrowserFallbackRequired()
        if expect_json and any(
            marker in content_type
            for marker in ("text/html", "application/xhtml", "application/pdf", "xml")
        ):
            raise ValueError(
                f"response content-type is not JSON: {content_type or 'unknown'}"
            )
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte discovery limit")
            if expect_json and not chunks:
                prefix = chunk.lstrip()[:64].lower()
                if prefix.startswith((b"<!doctype", b"<html", b"<?xml")):
                    raise ValueError("response body is not JSON")
            chunks.append(chunk)
        return b"".join(chunks), resp.charset or "utf-8"

    try:
        async with session.get(
            url, allow_redirects=True, **_request_ssl_kwargs(url)
        ) as resp:
            body, charset = await read_response(resp)
    except (aiohttp.ServerDisconnectedError, BrowserFallbackRequired):
        retry_headers = {
            "User-Agent": BROWSER_FALLBACK_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Connection": "close",
        }
        retry_timeout = getattr(session, "timeout", None) or aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
        )
        retry_connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(
            headers=retry_headers,
            timeout=retry_timeout,
            connector=retry_connector,
            trust_env=False,
        ) as retry_session:
            async with retry_session.get(
                url, allow_redirects=True, **_request_ssl_kwargs(url)
            ) as resp:
                body, charset = await read_response(resp)
    return _decode_response_body(body, charset=charset)


async def _fetch_bytes(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> bytes:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
        )
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as owned_session:
            return await _fetch_bytes(
                url,
                max_bytes=max_bytes,
                session=owned_session,
            )
    async with session.get(
        url, allow_redirects=True, **_request_ssl_kwargs(url)
    ) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte discovery limit")
            chunks.append(chunk)
    return b"".join(chunks)


def _decode_response_body(body: bytes, *, charset: str = "utf-8") -> str:
    if body.startswith(b"\x1f\x8b"):
        body = gzip.decompress(body)
    return body.decode(charset or "utf-8", errors="replace")


async def _fetch_json(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    data = await _fetch_json_value(url, max_bytes=max_bytes, session=session)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


async def _fetch_json_value(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> Any:
    text = await _fetch_text(
        url, max_bytes=max_bytes, session=session, expect_json=True
    )
    return _loads_mrf_json_value(text)


def _looks_tic_toc_json_text(text: str) -> bool:
    normalized = str(text or "").lower()
    return (
        '"reporting_structure"' in normalized
        and '"reporting_plans"' in normalized
        and (
            '"in_network_files"' in normalized
            or '"allowed_amount_file"' in normalized
            or '"allowed_amount_files"' in normalized
        )
    )


def _repair_missing_array_object_commas(text: str) -> str:
    # Some payer TOCs are published with adjacent objects in an array but no comma.
    # Keep this intentionally narrow and only apply it to TiC TOC-looking payloads.
    repaired: list[str] = []
    in_string = False
    escaped = False
    length = len(text)
    for idx, char in enumerate(text):
        repaired.append(char)
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char != "}":
            continue
        lookahead = idx + 1
        while lookahead < length and text[lookahead].isspace():
            lookahead += 1
        if lookahead < length and text[lookahead] == "{":
            repaired.append(",")
    return "".join(repaired)


def _loads_mrf_json_value(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        if not _looks_tic_toc_json_text(text):
            raise
        repaired = _repair_missing_array_object_commas(text)
        if repaired == text:
            raise
        return json.loads(repaired)


def _json_values_from_zip_bytes(
    body: bytes,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
) -> list[tuple[str, Any]]:
    values: list[tuple[str, Any]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(body)) as archive:
            infos = sorted(
                (info for info in archive.infolist() if not info.is_dir()),
                key=lambda info: (
                    0
                    if _mrf_file_type_from_text(info.filename, info.filename)
                    == "table-of-contents"
                    else 1,
                    info.filename,
                ),
            )
            for info in infos:
                name = info.filename
                lower_name = name.lower()
                if not lower_name.endswith((".json", ".json.gz")):
                    continue
                if info.file_size > max_bytes:
                    raise ValueError(
                        f"zip member {name} exceeds {max_bytes} byte discovery limit"
                    )
                payload = archive.read(info)
                text = _decode_response_body(payload)
                values.append((name, _loads_mrf_json_value(text)))
    except zipfile.BadZipFile as exc:
        raise ValueError("response body is not a readable ZIP archive") from exc
    if not values:
        raise ValueError("ZIP archive does not contain JSON TOC members")
    return values


async def _fetch_zip_json_values(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> list[tuple[str, Any]]:
    body = await _fetch_bytes(url, max_bytes=max_bytes, session=session)
    return _json_values_from_zip_bytes(body, max_bytes=max_bytes)


async def _post_json(
    url: str,
    payload: dict[str, Any],
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    data = await _post_json_value(url, payload, max_bytes=max_bytes, session=session)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


async def _post_json_value(
    url: str,
    payload: dict[str, Any],
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> Any:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
        )
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as owned_session:
            return await _post_json_value(
                url, payload, max_bytes=max_bytes, session=owned_session
            )
    async with session.post(
        url, json=payload, allow_redirects=True, **_request_ssl_kwargs(url)
    ) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if any(
            marker in content_type
            for marker in ("text/html", "application/xhtml", "application/pdf", "xml")
        ):
            raise ValueError(
                f"response content-type is not JSON: {content_type or 'unknown'}"
            )
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte discovery limit")
            if not chunks:
                prefix = chunk.lstrip()[:64].lower()
                if prefix.startswith((b"<!doctype", b"<html", b"<?xml")):
                    raise ValueError("response body is not JSON")
            chunks.append(chunk)
        charset = resp.charset or "utf-8"
    return json.loads(b"".join(chunks).decode(charset, errors="replace"))


async def _post_text(
    url: str,
    payload: str,
    *,
    headers: dict[str, str] | None = None,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> str:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(
            total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
        )
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(
            headers={"User-Agent": USER_AGENT},
            timeout=timeout,
            connector=connector,
            trust_env=False,
        ) as owned_session:
            return await _post_text(
                url,
                payload,
                headers=headers,
                max_bytes=max_bytes,
                session=owned_session,
            )
    async with session.post(
        url,
        data=payload,
        headers=headers or {},
        allow_redirects=True,
        **_request_ssl_kwargs(url),
    ) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte discovery limit")
            chunks.append(chunk)
        charset = resp.charset or "utf-8"
    return _decode_response_body(b"".join(chunks), charset=charset)


async def _load_candidates(
    provider: str, *, test_mode: bool, limit: int | None
) -> list[SourceCandidate]:
    config = _provider_config(provider)
    parser = str(config.get("parser") or provider).strip().lower()
    if parser == "master-list":
        path = _resolve_config_path(str(config.get("path") or ""))
        return parse_master_list(path.read_text(encoding="utf-8"))[:limit]
    if test_mode:
        return []
    raise ValueError(f"unsupported provider: {provider}")


async def _assert_fetch_url_allowed(url: str) -> None:
    parsed = urlsplit(str(url or "").strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("only http(s) URLs are allowed")
    if not parsed.hostname:
        raise ValueError("URL host is required")
    hostname = parsed.hostname.strip().lower()
    if hostname in {"localhost", "0.0.0.0"} or hostname.endswith(".local"):
        raise ValueError("local hosts are not allowed")
    try:
        ip = ipaddress.ip_address(hostname)
        _assert_public_ip(ip)
        return
    except ValueError:
        pass
    infos = await asyncio.to_thread(
        socket.getaddrinfo,
        hostname,
        parsed.port or (443 if parsed.scheme == "https" else 80),
        type=socket.SOCK_STREAM,
    )
    for info in infos:
        address = info[4][0]
        try:
            _assert_public_ip(ipaddress.ip_address(address))
        except ValueError as exc:
            raise ValueError(
                f"URL resolves to a non-public address: {address}"
            ) from exc


def _assert_public_ip(ip: ipaddress._BaseAddress) -> None:
    # not is_global also catches ranges the flag checks miss, e.g. CGNAT 100.64.0.0/10
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or not ip.is_global
    ):
        raise ValueError(f"non-public IP address is not allowed: {ip}")


async def _head_url(
    url: str, session: aiohttp.ClientSession | None = None
) -> dict[str, Any]:
    checked_at = _utc_now()
    try:
        await _assert_fetch_url_allowed(url)
        if session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=15)
            async with aiohttp.ClientSession(
                headers={"User-Agent": USER_AGENT}, timeout=timeout, trust_env=False
            ) as owned_session:
                return await _head_url(url, owned_session)
        async with session.head(
            url, allow_redirects=True, **_request_ssl_kwargs(url)
        ) as resp:
            await _assert_fetch_url_allowed(str(resp.url))
            length = resp.headers.get("Content-Length")
            return {
                "status": "ok" if resp.status < 400 else "http_error",
                "http_status": resp.status,
                "etag": resp.headers.get("ETag"),
                "last_modified": resp.headers.get("Last-Modified"),
                "content_length": int(length) if length and length.isdigit() else None,
                "content_type": resp.headers.get("Content-Type"),
                "final_url": str(resp.url),
                "checked_at": checked_at,
            }
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return {"status": "failed", "error": str(exc), "checked_at": checked_at}


async def _ensure_catalog_tables() -> None:
    for model in DISCOVERY_TABLES:
        await db.create_table(model.__table__, checkfirst=True)


def _candidate_to_rows(
    candidate: SourceCandidate, now: dt.datetime
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    payer_id = _id("mrfpayer", _clean_text(candidate.payer_name).lower())
    source_url = candidate.index_url or candidate.human_url
    aliases = sorted(
        {
            _clean_text(value)
            for value in (candidate.payer_name, *candidate.aliases)
            if _clean_text(value)
        },
        key=str.lower,
    )
    candidate_metadata = _candidate_metadata(candidate, aliases)
    source_id = _id(
        "mrfsource",
        {
            "payer": payer_id,
            "url": _canonical_or_none(source_url),
            "provider": candidate.provider,
        },
    )
    payer_row = {
        "payer_id": payer_id,
        "canonical_name": _clean_text(candidate.payer_name),
        "aliases": aliases,
        "parent_group": candidate.parent_group,
        "entity_type": candidate.entity_type,
        "states": list(candidate.states),
        "eins": list(candidate.eins),
        "lifecycle": "active",
        "source_coverage": list(candidate.source_coverage),
        "metadata_json": {
            "providers": [candidate.provider],
            **candidate_metadata,
        },
        "created_at": now,
        "updated_at": now,
    }
    if not source_url:
        return payer_row, None
    source_key_base = _slug(
        f"{candidate.provider}-{candidate.payer_name}-{_domain(source_url) or ''}"
    )
    source_key = f"{source_key_base[:80]}-{source_id[-8:]}"
    source_row = {
        "source_id": source_id,
        "payer_id": payer_id,
        "source_key": source_key,
        "display_name": _clean_text(candidate.payer_name),
        "source_type": candidate.source_type,
        "hosting_platform": candidate.hosting_platform
        or classify_hosting_platform(source_url),
        "access_model": candidate.access_model,
        "index_url": candidate.index_url,
        "human_url": candidate.human_url,
        "canonical_url": _canonical_or_none(source_url),
        "domain": _domain(source_url),
        "status": candidate.status or "needs_review",
        "schema_version": None,
        "latest_index_date": candidate.latest_index_date,
        "num_plans": candidate.num_plans,
        "num_files": candidate.num_files,
        "num_indices": candidate.num_indices,
        "total_compressed_size": candidate.total_compressed_size,
        "provenance_url": candidate.source_url,
        "seed_provider": candidate.provider,
        "confidence": candidate.confidence,
        "license_status": candidate.license_status,
        "review_status": candidate.review_status,
        "metadata_json": candidate_metadata,
        "created_at": now,
        "updated_at": now,
    }
    return payer_row, source_row


def _candidate_metadata(
    candidate: SourceCandidate, aliases: list[str] | tuple[str, ...]
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "aliases": list(aliases),
        "benefit_lines": list(candidate.benefit_lines),
        "source_tier": _normalize_source_tier(candidate.source_tier),
        "raw": candidate.raw_payload,
    }
    optional_lists = {
        "source_coverage": candidate.source_coverage,
        "vendor_names": candidate.vendor_names,
        "network_names": candidate.network_names,
        "plan_names": candidate.plan_names,
    }
    for key, values in optional_lists.items():
        cleaned = [_clean_text(value) for value in values if _clean_text(value)]
        if cleaned:
            metadata[key] = list(dict.fromkeys(cleaned))
    return metadata


async def _store_candidates(
    candidates: list[SourceCandidate],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = _utc_now()
    payer_rows_by_id: dict[str, dict[str, Any]] = {}
    source_rows_by_id: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        payer_row, source_row = _candidate_to_rows(candidate, now)
        existing = payer_rows_by_id.get(payer_row["payer_id"])
        if existing:
            existing["aliases"] = sorted(
                set((existing.get("aliases") or []) + (payer_row.get("aliases") or []))
            )
            existing["source_coverage"] = sorted(
                set(
                    (existing.get("source_coverage") or [])
                    + (payer_row.get("source_coverage") or [])
                )
            )
            existing_metadata = dict(existing.get("metadata_json") or {})
            existing_metadata["benefit_lines"] = sorted(
                set(
                    (existing_metadata.get("benefit_lines") or [])
                    + list(candidate.benefit_lines)
                )
            )
            for key in (
                "aliases",
                "source_coverage",
                "vendor_names",
                "network_names",
                "plan_names",
            ):
                existing_metadata[key] = sorted(
                    set(
                        (existing_metadata.get(key) or [])
                        + ((payer_row.get("metadata_json") or {}).get(key) or [])
                    )
                )
            existing_metadata["source_tier"] = _normalize_source_tier(
                existing_metadata.get("source_tier")
                or (payer_row.get("metadata_json") or {}).get("source_tier")
            )
            existing["metadata_json"] = {
                **existing_metadata,
                "providers": sorted(
                    set(
                        (existing.get("metadata_json") or {}).get("providers", [])
                        + [candidate.provider]
                    )
                ),
            }
        else:
            payer_rows_by_id[payer_row["payer_id"]] = payer_row
        if source_row:
            source_rows_by_id[source_row["source_id"]] = source_row
    payer_rows = list(payer_rows_by_id.values())
    source_rows = list(source_rows_by_id.values())
    await push_objects(payer_rows, MRFPayer, rewrite=True, use_copy=False)
    await push_objects(source_rows, MRFSource, rewrite=True, use_copy=False)
    return payer_rows, source_rows


async def _store_observations(
    source_rows: list[dict[str, Any]],
    *,
    test_mode: bool,
    run_id: str | None,
    progress_run_id: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    items = [
        (source, source.get("index_url") or source.get("human_url"))
        for source in source_rows
        if source.get("index_url") or source.get("human_url")
    ]
    total = len(items)
    semaphore = asyncio.Semaphore(max(1, int(concurrency or DEFAULT_CONCURRENCY)))
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=15)
    connector = _tcp_connector(
        limit=max(1, int(concurrency or DEFAULT_CONCURRENCY)) * 2
    )

    async def check_one(
        source: dict[str, Any], url: Any, session: aiohttp.ClientSession
    ) -> dict[str, Any]:
        async with semaphore:
            if test_mode:
                head = {"status": "skipped_test_mode", "checked_at": _utc_now()}
            else:
                head = await _head_url(str(url), session=session)
            return {
                "observation_id": _id(
                    "mrfurlobs",
                    {
                        "source_id": source["source_id"],
                        "url": url,
                        "checked_at": head["checked_at"].isoformat(),
                    },
                ),
                "source_id": source["source_id"],
                "url": str(url),
                "canonical_url": _canonical_or_none(str(url)),
                "url_type": "index_or_landing",
                "status": str(head.get("status") or "unknown"),
                "http_status": head.get("http_status"),
                "etag": head.get("etag"),
                "last_modified": head.get("last_modified"),
                "content_length": head.get("content_length"),
                "content_type": head.get("content_type"),
                "final_url": head.get("final_url"),
                "checked_at": head["checked_at"],
                "error": head.get("error"),
                "metadata_json": {"run_id": run_id},
            }

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
        connector=connector,
        trust_env=False,
    ) as session:
        tasks = [
            asyncio.create_task(check_one(source, url, session))
            for source, url in items
        ]
        for done, task in enumerate(asyncio.as_completed(tasks), start=1):
            observation = await task
            observations.append(observation)
            url = observation["url"]
            if progress_run_id:
                enqueue_live_progress(
                    run_id=progress_run_id,
                    importer="mrf-source-discovery",
                    status="running",
                    phase="checking source URLs",
                    unit="urls",
                    done=done,
                    total=total,
                    message=f"checked {done}/{total} URLs",
                    label=str(url),
                )
    await push_objects(observations, MRFUrlObservation, rewrite=True, use_copy=False)
    return observations


def _healthsparq_file_type(file_schema: Any) -> str:
    normalized = str(file_schema or "").strip().lower().replace("-", "_")
    if normalized == "in_network_rates":
        return "in-network"
    if normalized in {"allowed_amounts", "allowed_amount"}:
        return "allowed-amounts"
    if normalized == "table_of_contents":
        return "table-of-contents"
    if "drug" in normalized:
        return "payer-drug"
    return normalized.replace("_", "-") or "unknown"


def _healthsparq_file_url(metadata_url: str, file_path: Any) -> str:
    path = str(file_path or "").strip()
    if not path:
        return ""
    if path.startswith(("http://", "https://")):
        return path
    return urljoin(metadata_url.rsplit("/", 1)[0] + "/", path)


def _healthsparq_rows_from_metadata(
    source: dict[str, Any], metadata_url: str, payload: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list):
        return [], []
    now = _utc_now()
    plan_rows_by_id: dict[str, dict[str, Any]] = {}
    file_rows_by_id: dict[str, dict[str, Any]] = {}
    for file_item in files:
        if not isinstance(file_item, dict):
            continue
        file_url = _healthsparq_file_url(metadata_url, file_item.get("filePath"))
        if not file_url:
            continue
        reporting_plans = [
            plan
            for plan in (file_item.get("reportingPlans") or [])
            if isinstance(plan, dict)
        ]
        for plan in reporting_plans:
            plan_id = str(plan.get("planId") or "").strip()
            plan_name = plan.get("planName")
            market_type = plan.get("planMarketType")
            plan_row_id = _id(
                "mrfplan",
                {
                    "source": source["source_id"],
                    "plan_id": plan_id,
                    "plan_name": plan_name,
                    "market_type": market_type,
                },
            )
            plan_rows_by_id[plan_row_id] = {
                "mrf_plan_id": plan_row_id,
                "payer_id": source.get("payer_id"),
                "source_id": source["source_id"],
                "plan_id": plan_id or None,
                "plan_id_type": plan.get("planIdType"),
                "plan_name": plan_name,
                "market_type": market_type,
                "reporting_entity_name": file_item.get("reportingEntityName"),
                "reporting_entity_type": file_item.get("reportingEntityType"),
                "metadata_json": {
                    "raw_plan": plan,
                    "resolver": "healthsparq_public_mrf",
                },
                "first_seen_at": now,
                "last_seen_at": now,
            }
        file_type = _healthsparq_file_type(file_item.get("fileSchema"))
        canonical_url = _canonical_or_none(file_url) or file_url
        file_row_id = _id(
            "mrffile",
            {"source": source["source_id"], "type": file_type, "url": canonical_url},
        )
        file_rows_by_id[file_row_id] = {
            "mrf_file_id": file_row_id,
            "payer_id": source.get("payer_id"),
            "source_id": source["source_id"],
            "file_type": file_type,
            "url": file_url,
            "canonical_url": canonical_url,
            "from_index_url": metadata_url,
            "description": file_item.get("fileName"),
            "network_name": file_item.get("fileName"),
            "plan_ids": [
                plan.get("planId") for plan in reporting_plans if plan.get("planId")
            ],
            "plan_names": [
                plan.get("planName") for plan in reporting_plans if plan.get("planName")
            ],
            "market_types": sorted(
                {
                    plan.get("planMarketType")
                    for plan in reporting_plans
                    if plan.get("planMarketType")
                }
            ),
            "is_signed_url": _looks_signed(file_url),
            "size_bytes": None,
            "schema_version": None,
            "metadata_json": {
                "resolver": "healthsparq_public_mrf",
                "container_format": _container_format(file_url),
                **_file_benefit_metadata(
                    source,
                    file_url,
                    file_item.get("fileName"),
                    file_item.get("reportingEntityName"),
                ),
                "file_path": file_item.get("filePath"),
                "file_schema": file_item.get("fileSchema"),
                "last_updated_on": file_item.get("lastUpdatedOn"),
                "reporting_entity_name": file_item.get("reportingEntityName"),
                # Normalized to the import-control preview plan shape (snake_case keys).
                "plan_info": _healthsparq_plan_info(file_item),
            },
            "first_seen_at": now,
            "last_seen_at": now,
        }
    return list(plan_rows_by_id.values()), list(file_rows_by_id.values())


def _toc_rows_from_content(
    source: dict[str, Any], url: str, toc: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(toc.get("files"), list):
        return _healthsparq_rows_from_metadata(source, url, toc)
    plan_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    entries = parse_toc_catalog_entries(toc, str(url))
    schema_version = _truncate_text(toc.get("version"), 32)
    for entry in entries:
        plan_info = list(entry.plan_info or ())
        for plan in plan_info:
            plan_id = str(plan.get("plan_id") or "").strip()
            plan_name = plan.get("plan_name")
            market_type = plan.get("plan_market_type")
            plan_row_id = _id(
                "mrfplan",
                {
                    "source": source["source_id"],
                    "plan_id": plan_id,
                    "plan_name": plan_name,
                    "market_type": market_type,
                },
            )
            plan_rows.append(
                {
                    "mrf_plan_id": plan_row_id,
                    "payer_id": source.get("payer_id"),
                    "source_id": source["source_id"],
                    "plan_id": plan_id or None,
                    "plan_id_type": plan.get("plan_id_type"),
                    "plan_name": plan_name,
                    "market_type": market_type,
                    "reporting_entity_name": entry.reporting_entity_name,
                    "reporting_entity_type": entry.reporting_entity_type,
                    "metadata_json": {"raw_plan": plan},
                    "first_seen_at": _utc_now(),
                    "last_seen_at": _utc_now(),
                }
            )
        if entry.source_type == "table-of-contents":
            continue
        if not str(entry.original_url or "").startswith(("http://", "https://")):
            continue
        file_row_id = _id(
            "mrffile",
            {
                "source": source["source_id"],
                "type": entry.source_type,
                "url": entry.canonical_url,
            },
        )
        file_rows.append(
            {
                "mrf_file_id": file_row_id,
                "payer_id": source.get("payer_id"),
                "source_id": source["source_id"],
                "file_type": entry.source_type,
                "url": entry.original_url,
                "canonical_url": entry.canonical_url,
                "from_index_url": entry.from_index_url,
                "description": entry.description,
                "network_name": entry.description,
                "plan_ids": [
                    plan.get("plan_id") for plan in plan_info if plan.get("plan_id")
                ],
                "plan_names": [
                    plan.get("plan_name") for plan in plan_info if plan.get("plan_name")
                ],
                "market_types": sorted(
                    {
                        plan.get("plan_market_type")
                        for plan in plan_info
                        if plan.get("plan_market_type")
                    }
                ),
                "is_signed_url": _looks_signed(entry.original_url),
                "size_bytes": None,
                "schema_version": schema_version,
                "metadata_json": {
                    "container_format": _container_format(entry.original_url),
                    **_file_benefit_metadata(
                        source,
                        entry.original_url,
                        entry.description,
                        entry.reporting_entity_name,
                    ),
                    "domain": entry.domain,
                    "reporting_entity_name": entry.reporting_entity_name,
                    # Preserve the exact per-file plan list (with plan_id_type) so the
                    # import-control snapshot can be rebuilt from stored rows.
                    "plan_info": plan_info,
                },
                "first_seen_at": _utc_now(),
                "last_seen_at": _utc_now(),
            }
        )
    return plan_rows, file_rows


def _metadata_text_file_type(value: Any) -> str:
    text = str(value or "").strip().lower().replace("_", "-")
    if "allowed" in text:
        return "allowed-amounts"
    if "in-network" in text or "in network" in text:
        return "in-network"
    if "toc" in text or "table" in text:
        return "table-of-contents"
    return text or "unknown"


def _looks_direct_mrf_body_url(url: str | None) -> bool:
    path = urlsplit(str(url or "")).path.lower()
    if not path.endswith((".json", ".json.gz", ".gz", ".zip", ".7z", ".csv")):
        return False
    if re.search(r"(^|[_/-])index\.json(?:\.gz)?$", path):
        return False
    return True


def _metadata_text_fields(line: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for cell in line.split("|"):
        if ":" not in cell:
            continue
        key, value = cell.split(":", 1)
        key = _clean_text(key).lower()
        if key:
            fields[key] = _clean_text(value)
    return fields


def _metadata_text_rows_from_content(
    source: dict[str, Any], url: str, text: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = _utc_now()
    plan_rows_by_id: dict[str, dict[str, Any]] = {}
    file_rows_by_id: dict[str, dict[str, Any]] = {}
    for line in (text or "").splitlines():
        file_url = _first_http_url(line)
        if not file_url or not _looks_direct_mrf_body_url(file_url):
            continue
        fields = _metadata_text_fields(line)
        file_type = _metadata_text_file_type(
            fields.get("file scope") or fields.get("scope")
        )
        plan_name = fields.get("plan name")
        sponsor_ein = fields.get("sponsor ein") or fields.get("ein")
        plan_row_id = _id(
            "mrfplan",
            {
                "source": source["source_id"],
                "plan_id": sponsor_ein,
                "plan_name": plan_name,
                "metadata_url": url,
            },
        )
        if sponsor_ein or plan_name:
            plan_rows_by_id[plan_row_id] = {
                "mrf_plan_id": plan_row_id,
                "payer_id": source.get("payer_id"),
                "source_id": source["source_id"],
                "plan_id": sponsor_ein or None,
                "plan_id_type": "ein" if sponsor_ein else None,
                "plan_name": plan_name,
                "market_type": None,
                "reporting_entity_name": source.get("display_name"),
                "reporting_entity_type": "third_party_administrator",
                "metadata_json": {"raw_line": line, "metadata_url": url},
                "first_seen_at": now,
                "last_seen_at": now,
            }
        canonical_url = _canonical_or_none(file_url) or file_url
        file_row_id = _id(
            "mrffile",
            {"source": source["source_id"], "type": file_type, "url": canonical_url},
        )
        file_rows_by_id[file_row_id] = {
            "mrf_file_id": file_row_id,
            "payer_id": source.get("payer_id"),
            "source_id": source["source_id"],
            "file_type": file_type,
            "url": file_url,
            "canonical_url": canonical_url,
            "from_index_url": url,
            "description": plan_name,
            "network_name": plan_name,
            "plan_ids": [sponsor_ein] if sponsor_ein else [],
            "plan_names": [plan_name] if plan_name else [],
            "market_types": [],
            "is_signed_url": _looks_signed(file_url),
            "size_bytes": None,
            "schema_version": None,
            "metadata_json": {
                "resolver": "html_metadata_text",
                "container_format": _container_format(file_url),
                **_file_benefit_metadata(source, file_url, plan_name, line),
                "metadata_fields": fields,
                "metadata_url": url,
            },
            "first_seen_at": now,
            "last_seen_at": now,
        }
    return list(plan_rows_by_id.values()), list(file_rows_by_id.values())


def _current_month_start() -> str:
    return _utc_now().date().replace(day=1).isoformat()


def _render_highmark_hmhs_path(path: str, *, month_start: str | None = None) -> str:
    rendered = str(path or "")
    date_value = month_start or _current_month_start()
    rendered = rendered.replace("?FIRST_DAY_CUR_MONTH", date_value)
    return rendered.replace("FIRST_DAY_CUR_MONTH", date_value)


def _parse_highmark_hmhs_script(
    script_text: str, *, base_url: str, month_start: str | None = None
) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    for match in re.finditer(
        r"\{\s*regName:\s*[\"'](?P<region>[^\"']*)[\"']\s*,\s*dl:\s*[\"'](?P<path>[^\"']+)[\"']\s*,\s*dt:\s*[\"'](?P<label>[^\"']*)[\"']\s*\}",
        script_text or "",
        flags=re.S,
    ):
        raw_path = html.unescape(match.group("path")).strip()
        if not raw_path:
            continue
        rendered_path = _render_highmark_hmhs_path(raw_path, month_start=month_start)
        targets.append(
            {
                "url": urljoin(base_url, rendered_path),
                "label": _clean_text(match.group("label")),
                "region": _clean_text(match.group("region")),
                "raw_path": raw_path,
                "rendered_path": rendered_path,
            }
        )
    return targets


def _label_from_index_name(name: str) -> str:
    label = re.sub(r"^\d{4}-\d{2}-\d{2}_", "", str(name or ""))
    label = re.sub(r"_index\.json(?:\?.*)?$", "", label, flags=re.I)
    label = label.replace("_", " ").replace("-", " ")
    return _clean_text(label).title()


def _parse_uhc_blob_listing(payload: dict[str, Any]) -> list[dict[str, Any]]:
    blobs = payload.get("blobs") if isinstance(payload, dict) else None
    if not isinstance(blobs, list):
        return []
    targets: list[dict[str, Any]] = []
    for blob in blobs:
        if not isinstance(blob, dict):
            continue
        name = str(blob.get("name") or "").strip()
        url = str(blob.get("downloadUrl") or "").strip()
        if not name or not url or "_index.json" not in name.lower():
            continue
        targets.append(
            {
                "url": url,
                "label": _label_from_index_name(name),
                "name": name,
                "size": _as_int(blob.get("size")),
            }
        )
    return targets


def _uhc_provider_mrf_collection_from_url(url: str | None) -> str:
    parts = [
        part.lower()
        for part in urlsplit(str(url or "")).path.strip("/").split("/")
        if part
    ]
    if parts:
        if parts[0] in {"ifp", "cs"}:
            return parts[0]
        if len(parts) >= 4 and parts[:3] in (
            ["api", "files", "ui"],
            ["api", "stream", "ui"],
        ):
            collection = parts[3]
            if collection in {"ifp", "cs"}:
                return collection
    return "ifp"


def _uhc_provider_mrf_api_url(url: str | None) -> str:
    collection = _uhc_provider_mrf_collection_from_url(url)
    return f"https://providermrf.uhc.com/api/files/ui/{collection}/"


def _uhc_provider_mrf_stream_url(blob_path: Any) -> str | None:
    value = str(blob_path or "").strip().lstrip("/")
    if not value:
        return None
    return urljoin("https://providermrf.uhc.com/api/stream/", value)


def _uhc_provider_mrf_file_type(section: str) -> str | None:
    normalized = str(section or "").strip().lower()
    if normalized == "providers":
        return "provider-network"
    if normalized == "drugs":
        return "payer-drug"
    if normalized == "plans":
        return "plan-reference"
    return None


def _uhc_provider_mrf_label(name: str) -> str:
    label = re.sub(r"\.json(?:\.gz)?$", "", str(name or ""), flags=re.I)
    label = re.sub(r"^json[_-]", "", label, flags=re.I)
    label = label.replace("_", " ").replace("-", " ")
    return _clean_text(label).title()


def _uhc_provider_mrf_targets_from_payload(
    source: dict[str, Any],
    payload: dict[str, Any],
    *,
    listing_url: str,
    resolver_type: str = "uhc_provider_mrf_files",
    max_targets: int | None = None,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    for section in ("providers", "drugs", "plans"):
        file_type = _uhc_provider_mrf_file_type(section)
        entries = payload.get(section) if isinstance(payload, dict) else None
        if not isinstance(entries, list) or not file_type:
            continue
        sorted_entries = sorted(
            (entry for entry in entries if isinstance(entry, dict)),
            key=lambda entry: str(entry.get("date") or ""),
            reverse=True,
        )
        for entry in sorted_entries:
            name = str(entry.get("name") or "").strip()
            if not name or name.lower() in {section, "filename"}:
                continue
            if name.lower().endswith(".trig"):
                continue
            target_url = str(entry.get("url") or "").strip()
            if not target_url:
                target_url = _uhc_provider_mrf_stream_url(entry.get("blobPath")) or ""
            if not target_url:
                continue
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target_url,
                    label=_uhc_provider_mrf_label(name),
                    resolved_from_url=listing_url,
                    metadata={
                        "resolver": resolver_type,
                        "target_kind": "file_reference",
                        "target_file_type": file_type,
                        "container_format": _container_format(target_url),
                        "source_format": _container_format(target_url) or "json",
                        "uhc_provider_section": section,
                        "uhc_provider_file_name": name,
                        "uhc_provider_blob_path": entry.get("blobPath"),
                        "uhc_provider_file_date": entry.get("date"),
                        "uhc_provider_external": bool(entry.get("isExternal")),
                    },
                )
            )
            if max_targets and len(targets) >= max_targets:
                return targets
    return targets


def _mymedicalshopper_entity_slug_from_url(url: str | None) -> str | None:
    parts = [
        part for part in urlsplit(str(url or "")).path.strip("/").split("/") if part
    ]
    if len(parts) >= 2 and parts[0].lower() == "mrf-search":
        return parts[1]
    return None


def _mymedicalshopper_employer_slug_from_url(url: str | None) -> str | None:
    parts = [
        part for part in urlsplit(str(url or "")).path.strip("/").split("/") if part
    ]
    if len(parts) >= 2 and parts[0].lower() == "mrf":
        return parts[1]
    return None


def _mymedicalshopper_group_id_from_employer_slug(slug: str | None) -> str | None:
    parts = [part for part in re.split(r"[-_]+", str(slug or "").strip()) if part]
    if parts:
        suffix = parts[-1].lower()
        if re.fullmatch(r"\d+", suffix):
            return suffix
        if re.fullmatch(r"[a-z]{1,8}\d{3,}", suffix):
            return suffix
    return None


def _mymedicalshopper_tpa_slug_from_employer_slug(slug: str | None) -> str | None:
    parts = [part for part in re.split(r"[-_]+", str(slug or "").strip()) if part]
    if len(parts) >= 2 and re.fullmatch(r"\d+", parts[-1]):
        return parts[-2]
    return None


def _slug_label(value: str | None) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    return re.sub(r"\s+", " ", text.replace("_", " ").replace("-", " ")).strip().title()


def _mymedicalshopper_employer_selector(
    entity_slug: str, *, all_employers_searchable: bool
) -> dict[str, Any]:
    selector: dict[str, Any] = {"tpaSlug": entity_slug, "status": "Enabled"}
    if not all_employers_searchable:
        selector["machineReadableFiles.makeMRFsSearchable"] = True
    return selector


def _mymedicalshopper_employer_search_pattern(query: str | None) -> str | None:
    clean_query = _clean_text(query).lower()
    if len(clean_query) < 3:
        return None
    tokens = _query_match_tokens(clean_query)
    if not tokens:
        return None
    return ".*".join(re.escape(token) for token in tokens)


def _mymedicalshopper_employer_search_selector(
    base_selector: dict[str, Any], query: str | None
) -> dict[str, Any] | None:
    pattern = _mymedicalshopper_employer_search_pattern(query)
    if not pattern:
        return None
    return {
        "$and": [
            dict(base_selector),
            {
                "$or": [
                    {"name": {"$regex": pattern, "$options": "i"}},
                    {"slug": {"$regex": pattern, "$options": "i"}},
                ]
            },
        ]
    }


def _mymedicalshopper_entity_employer_selectors(
    entity_slug: str,
    *,
    all_employers_searchable: bool,
    source: dict[str, Any] | None,
    resolver: dict[str, Any],
) -> list[dict[str, Any]]:
    base_selector = _mymedicalshopper_employer_selector(
        entity_slug, all_employers_searchable=all_employers_searchable
    )
    search_selector = _mymedicalshopper_employer_search_selector(
        base_selector, _source_target_payer_query(source or {})
    )
    if not search_selector:
        return [base_selector]
    selectors = [search_selector]
    if resolver.get("query_search_include_full_table"):
        selectors.append(base_selector)
    return selectors


def _mymedicalshopper_sockjs_ws_url(base_url: str) -> str:
    parsed = urlsplit(str(base_url or ""))
    scheme = "wss" if parsed.scheme == "https" else "ws"
    server_id = str(abs(hash((parsed.netloc, os.getpid()))) % 1000).zfill(3)
    try:
        task_id = id(asyncio.current_task())
    except RuntimeError:
        task_id = 0
    session_id = semantic_hash(
        {
            "url": base_url,
            "pid": os.getpid(),
            "task": task_id,
            "ts": _utc_now().isoformat(),
        },
        domain="mms_sockjs",
    )[:8]
    return f"{scheme}://{parsed.netloc}/sockjs/{server_id}/{session_id}/websocket"


def _mymedicalshopper_sockjs_messages(frame: str) -> list[dict[str, Any]]:
    text = str(frame or "")
    if text in {"o", "h"}:
        return []
    if not text.startswith("a"):
        return []
    try:
        payload = json.loads(text[1:])
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    messages: list[dict[str, Any]] = []
    for item in payload:
        try:
            message = json.loads(str(item))
        except json.JSONDecodeError:
            continue
        if isinstance(message, dict):
            messages.append(message)
    return messages


async def _mymedicalshopper_ddp_send(
    ws: aiohttp.ClientWebSocketResponse, payload: dict[str, Any]
) -> None:
    encoded = json.dumps(
        [json.dumps(payload, separators=(",", ":"))], separators=(",", ":")
    )
    await ws.send_str(encoded)


async def _mymedicalshopper_ddp_recv(
    ws: aiohttp.ClientWebSocketResponse,
    *,
    timeout_seconds: float,
    deadline: float | None = None,
    operation: str = "response",
) -> list[dict[str, Any]]:
    while True:
        frame_timeout = timeout_seconds
        if deadline is not None:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError(
                    f"MyMedicalShopper DDP {operation} timed out after {timeout_seconds:g}s"
                )
            frame_timeout = min(timeout_seconds, remaining)
        try:
            frame = await asyncio.wait_for(ws.receive(), timeout=frame_timeout)
        except TimeoutError as exc:
            raise TimeoutError(
                f"MyMedicalShopper DDP {operation} timed out after {timeout_seconds:g}s"
            ) from exc
        if frame.type == aiohttp.WSMsgType.TEXT:
            messages = _mymedicalshopper_sockjs_messages(str(frame.data))
            returned: list[dict[str, Any]] = []
            for message in messages:
                if message.get("msg") == "ping":
                    pong: dict[str, Any] = {"msg": "pong"}
                    if message.get("id") is not None:
                        pong["id"] = message.get("id")
                    await _mymedicalshopper_ddp_send(ws, pong)
                    continue
                returned.append(message)
            if returned:
                return returned
            continue
        if frame.type in {
            aiohttp.WSMsgType.CLOSE,
            aiohttp.WSMsgType.CLOSED,
            aiohttp.WSMsgType.CLOSING,
        }:
            raise ValueError(
                "MyMedicalShopper DDP websocket closed before a response was received"
            )
        if frame.type == aiohttp.WSMsgType.ERROR:
            raise ValueError(f"MyMedicalShopper DDP websocket error: {ws.exception()}")


async def _mymedicalshopper_ddp_connect(
    session: aiohttp.ClientSession,
    base_url: str,
    *,
    timeout_seconds: float,
) -> aiohttp.ClientWebSocketResponse:
    await _assert_fetch_url_allowed(base_url)
    parsed = urlsplit(str(base_url or ""))
    origin = f"{parsed.scheme}://{parsed.netloc}"
    ws = await session.ws_connect(
        _mymedicalshopper_sockjs_ws_url(base_url),
        headers={"User-Agent": USER_AGENT, "Origin": origin},
        timeout=timeout_seconds,
    )
    await asyncio.wait_for(ws.receive(), timeout=timeout_seconds)
    await _mymedicalshopper_ddp_send(
        ws, {"msg": "connect", "version": "1", "support": ["1", "pre2", "pre1"]}
    )
    while True:
        for message in await _mymedicalshopper_ddp_recv(
            ws, timeout_seconds=timeout_seconds
        ):
            if message.get("msg") == "connected":
                return ws
            if message.get("msg") == "failed":
                raise ValueError(f"MyMedicalShopper DDP connection failed: {message}")


async def _mymedicalshopper_ddp_call(
    ws: aiohttp.ClientWebSocketResponse,
    *,
    method: str,
    params: list[Any],
    request_id: str,
    timeout_seconds: float,
) -> Any:
    await _mymedicalshopper_ddp_send(
        ws, {"msg": "method", "id": request_id, "method": method, "params": params}
    )
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        for message in await _mymedicalshopper_ddp_recv(
            ws,
            timeout_seconds=timeout_seconds,
            deadline=deadline,
            operation=f"method {method}",
        ):
            if message.get("msg") != "result" or message.get("id") != request_id:
                continue
            if message.get("error"):
                raise ValueError(
                    f"MyMedicalShopper DDP method {method} failed: {message['error']}"
                )
            return message.get("result")


async def _mymedicalshopper_ddp_subscribe_collect(
    ws: aiohttp.ClientWebSocketResponse,
    *,
    name: str,
    params: list[Any],
    sub_id: str,
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    await _mymedicalshopper_ddp_send(
        ws, {"msg": "sub", "id": sub_id, "name": name, "params": params}
    )
    collected: list[dict[str, Any]] = []
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    while True:
        for message in await _mymedicalshopper_ddp_recv(
            ws,
            timeout_seconds=timeout_seconds,
            deadline=deadline,
            operation=f"subscription {name}",
        ):
            if message.get("msg") in {"added", "changed", "removed"}:
                collected.append(message)
                continue
            if message.get("msg") == "ready" and sub_id in (message.get("subs") or []):
                return collected
            if message.get("msg") == "nosub" and message.get("id") == sub_id:
                raise ValueError(
                    f"MyMedicalShopper DDP subscription {name} failed: {message.get('error') or message}"
                )


def _mymedicalshopper_entity_config_from_messages(
    messages: list[dict[str, Any]], entity_slug: str
) -> dict[str, Any]:
    for message in messages:
        if message.get("collection") != "thirdPartyAdministrators":
            continue
        fields = (
            message.get("fields") if isinstance(message.get("fields"), dict) else {}
        )
        if str(fields.get("slug") or "").strip() == entity_slug:
            return dict(fields)
    return {}


def _mymedicalshopper_tpa_name_from_config(
    config: dict[str, Any], entity_slug: str | None
) -> str | None:
    for key in (
        "name",
        "displayName",
        "display_name",
        "companyName",
        "company_name",
        "title",
    ):
        value = _clean_text(config.get(key))
        if value:
            return value
    return _slug_label(entity_slug)


def _mymedicalshopper_oid_value(value: Any) -> str:
    if isinstance(value, dict) and value.get("$type") == "oid":
        return str(value.get("$value") or "").strip()
    return str(value or "").strip()


def _mymedicalshopper_tabular_info_from_messages(
    messages: list[dict[str, Any]], table_id: str = "EntityMRFEmployers"
) -> dict[str, Any]:
    info: dict[str, Any] = {"ids": [], "records_total": 0, "records_filtered": 0}
    for message in messages:
        if (
            message.get("collection") != "tabular_records"
            or message.get("id") != table_id
        ):
            continue
        fields = (
            message.get("fields") if isinstance(message.get("fields"), dict) else {}
        )
        ids = fields.get("ids") if isinstance(fields.get("ids"), list) else []
        info["ids"] = ids
        info["records_total"] = _as_int(fields.get("recordsTotal")) or len(ids)
        info["records_filtered"] = (
            _as_int(fields.get("recordsFiltered")) or info["records_total"]
        )
    return info


def _mymedicalshopper_employer_docs_from_messages(
    messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    employers: dict[str, dict[str, Any]] = {}
    for message in messages:
        if message.get("collection") != "employers" or message.get("msg") not in {
            "added",
            "changed",
        }:
            continue
        fields = (
            message.get("fields") if isinstance(message.get("fields"), dict) else {}
        )
        slug = str(fields.get("slug") or "").strip()
        if not slug:
            continue
        employers[slug] = {
            "_id": _mymedicalshopper_oid_value(message.get("id")),
            **fields,
        }
    return list(employers.values())


async def _mymedicalshopper_entity_employers(
    ws: aiohttp.ClientWebSocketResponse,
    *,
    source: dict[str, Any] | None = None,
    entity_slug: str,
    resolver: dict[str, Any],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    config_messages = await _mymedicalshopper_ddp_subscribe_collect(
        ws,
        name="entityMRFsConfig",
        params=[entity_slug],
        sub_id=f"mms-config-{entity_slug}",
        timeout_seconds=timeout_seconds,
    )
    config = _mymedicalshopper_entity_config_from_messages(config_messages, entity_slug)
    tpa_name = _mymedicalshopper_tpa_name_from_config(config, entity_slug)
    machine_readable_files = (
        config.get("machineReadableFiles")
        if isinstance(config.get("machineReadableFiles"), dict)
        else {}
    )
    selectors = _mymedicalshopper_entity_employer_selectors(
        entity_slug,
        all_employers_searchable=bool(
            machine_readable_files.get("allEmployersSearchable")
        ),
        source=source,
        resolver=resolver,
    )
    page_size = max(_as_int(resolver.get("page_size")) or 100, 1)
    max_employers = max(_as_int(resolver.get("max_employers")) or 10000, 1)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        max_employers = min(max_employers, max_targets)
    fields = {
        "name": 1,
        "slug": 1,
        "tpaSlug": 1,
        "status": 1,
        "machineReadableFiles": 1,
        "groupId": 1,
        "ein": 1,
    }
    employers: dict[str, dict[str, Any]] = {}
    for selector_index, selector in enumerate(selectors):
        offset = 0
        while offset < max_employers and len(employers) < max_employers:
            batch_size = min(page_size, max_employers - len(employers))
            info_messages = await _mymedicalshopper_ddp_subscribe_collect(
                ws,
                name="tabular_getInfo",
                params=[
                    "EntityMRFEmployers",
                    selector,
                    [["name", "asc"]],
                    offset,
                    batch_size,
                ],
                sub_id=f"mms-info-{entity_slug}-{selector_index}-{offset}",
                timeout_seconds=timeout_seconds,
            )
            info = _mymedicalshopper_tabular_info_from_messages(info_messages)
            ids = [item for item in info.get("ids") or [] if item]
            if not ids:
                break
            doc_messages = await _mymedicalshopper_ddp_subscribe_collect(
                ws,
                name="entityMRFEmployers",
                params=["EntityMRFEmployers", ids, fields],
                sub_id=f"mms-employers-{entity_slug}-{selector_index}-{offset}",
                timeout_seconds=timeout_seconds,
            )
            for employer in _mymedicalshopper_employer_docs_from_messages(doc_messages):
                slug = str(employer.get("slug") or "").strip()
                if slug:
                    employer = dict(employer)
                    employer.setdefault("tpaSlug", entity_slug)
                    if tpa_name:
                        employer.setdefault("tpaName", tpa_name)
                    employers[slug] = employer
            offset += len(ids)
            if offset >= (_as_int(info.get("records_filtered")) or len(ids)):
                break
    return list(employers.values())


async def _mymedicalshopper_generated_for_employer(
    ws: aiohttp.ClientWebSocketResponse,
    *,
    employer_slug: str,
    timeout_seconds: float,
    max_plans: int | None = None,
) -> Any:
    plans = await _mymedicalshopper_ddp_call(
        ws,
        method="getBenefitPlans",
        params=[{"employerSlug": employer_slug, "skipPlanDesign": True}],
        request_id=f"mms-plans-{employer_slug}",
        timeout_seconds=timeout_seconds,
    )
    if not isinstance(plans, list) or not plans:
        return []
    if max_plans and max_plans > 0:
        plans = plans[:max_plans]
    return await _mymedicalshopper_ddp_call(
        ws,
        method="getGeneratedMRFInfoByPlan",
        params=[plans],
        request_id=f"mms-generated-{employer_slug}",
        timeout_seconds=timeout_seconds,
    )


def _mymedicalshopper_generated_entries(generated: Any) -> list[dict[str, Any]]:
    if isinstance(generated, list):
        return [item for item in generated if isinstance(item, dict)]
    if not isinstance(generated, dict):
        return []
    for key in ("mrfGeneratedPlans", "generatedPlans", "plans", "result"):
        value = generated.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return [generated]


def _mymedicalshopper_entry_plan_value(
    entry: dict[str, Any], keys: tuple[str, ...]
) -> Any:
    for key in keys:
        value = entry.get(key)
        if value not in (None, ""):
            return value
    plan = entry.get("plan") if isinstance(entry.get("plan"), dict) else {}
    for key in keys:
        value = plan.get(key)
        if value not in (None, ""):
            return value
    return None


def _mymedicalshopper_entry_history(entry: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("mrfGeneratedInfo", "generatedMRFInfo", "mrfInfo", "history"):
        value = entry.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _mymedicalshopper_targets_from_generated(
    source: dict[str, Any],
    *,
    entity_slug: str | None,
    employer: dict[str, Any],
    generated: Any,
    resolver_type: str,
    resolved_from_url: str,
) -> list[CrawlTarget]:
    employer_slug = str(employer.get("slug") or "").strip()
    employer_name = _clean_text(employer.get("name") or employer_slug)
    employer_id = (
        _mymedicalshopper_oid_value(employer.get("_id") or employer.get("id")) or None
    )
    group_id = str(
        employer.get("groupId") or employer.get("group_id") or ""
    ).strip() or _mymedicalshopper_group_id_from_employer_slug(employer_slug)
    tpa_slug = str(
        employer.get("tpaSlug") or employer.get("tpa_slug") or entity_slug or ""
    ).strip() or _mymedicalshopper_tpa_slug_from_employer_slug(employer_slug)
    tpa_name = _clean_text(
        employer.get("tpaName") or employer.get("tpa_name")
    ) or _slug_label(tpa_slug)
    ein = str(employer.get("ein") or employer.get("EIN") or "").strip() or None
    targets_by_url: dict[str, CrawlTarget] = {}
    for entry in _mymedicalshopper_generated_entries(generated):
        plan_id = _mymedicalshopper_entry_plan_value(
            entry, ("planId", "plan_id", "id", "_id")
        )
        plan_name = _clean_text(
            _mymedicalshopper_entry_plan_value(entry, ("planName", "plan_name", "name"))
            or ""
        )
        history = _mymedicalshopper_entry_history(entry)
        latest = sorted(
            (
                item
                for item in history
                if item.get("mrfGenerated") is True
                and str(item.get("link") or "")
                .strip()
                .startswith(("http://", "https://"))
            ),
            key=lambda item: str(item.get("month") or ""),
            reverse=True,
        )
        if not latest:
            continue
        item = latest[0]
        url = str(item.get("link") or "").strip()
        canonical = _canonical_or_none(url) or url
        month = str(item.get("month") or "").strip() or None
        label_parts = [part for part in (employer_name, plan_name, month) if part]
        targets_by_url[canonical] = CrawlTarget(
            source=source,
            url=url,
            label=" - ".join(label_parts),
            resolved_from_url=resolved_from_url,
            metadata={
                "resolver": resolver_type,
                "target_file_type": "table-of-contents",
                "entity_slug": entity_slug or tpa_slug,
                "tpa_slug": tpa_slug,
                "tpa_name": tpa_name,
                "client_id": employer_id,
                "client_name": employer_name or None,
                "employer_id": employer_id or employer_slug or None,
                "employer_slug": employer_slug or None,
                "employer_name": employer_name or None,
                "group_id": group_id,
                "group_number": group_id,
                "ein": ein,
                "plan_id": str(plan_id) if plan_id not in (None, "") else None,
                "plan_name": plan_name or None,
                "month": month,
                "history_month_count": len(history),
            },
        )
    return list(targets_by_url.values())


async def _resolve_mymedicalshopper_talon_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    entity_slug = _mymedicalshopper_entity_slug_from_url(url)
    employer_slug = _mymedicalshopper_employer_slug_from_url(url)
    if not entity_slug and not employer_slug:
        raise ValueError(f"unsupported MyMedicalShopper MRF URL: {url}")
    timeout_seconds = float(_as_int(resolver.get("ddp_timeout_seconds")) or 30)
    max_plans = _as_int(resolver.get("max_plans_per_employer"))
    max_targets = _as_int(resolver.get("max_targets"))
    ws = await _mymedicalshopper_ddp_connect(
        session, url, timeout_seconds=timeout_seconds
    )
    try:
        if entity_slug:
            employers = await _mymedicalshopper_entity_employers(
                ws,
                source=source,
                entity_slug=entity_slug,
                resolver=resolver,
                timeout_seconds=timeout_seconds,
            )
        else:
            employer_name = await _mymedicalshopper_ddp_call(
                ws,
                method="getEmployerName",
                params=[employer_slug],
                request_id=f"mms-employer-name-{employer_slug}",
                timeout_seconds=timeout_seconds,
            )
            direct_tpa_slug = _mymedicalshopper_tpa_slug_from_employer_slug(
                employer_slug
            )
            employers = [
                {
                    "slug": employer_slug,
                    "name": employer_name,
                    "tpaSlug": direct_tpa_slug,
                    "tpaName": _slug_label(direct_tpa_slug),
                    "groupId": _mymedicalshopper_group_id_from_employer_slug(
                        employer_slug
                    ),
                }
            ]
        targets: list[CrawlTarget] = []
        for employer in employers:
            slug = str(employer.get("slug") or "").strip()
            if not slug:
                continue
            generated = await _mymedicalshopper_generated_for_employer(
                ws,
                employer_slug=slug,
                timeout_seconds=timeout_seconds,
                max_plans=max_plans,
            )
            targets.extend(
                _mymedicalshopper_targets_from_generated(
                    source,
                    entity_slug=entity_slug
                    or str(employer.get("tpaSlug") or "").strip()
                    or None,
                    employer=employer,
                    generated=generated,
                    resolver_type=str(
                        resolver.get("type") or "mymedicalshopper_talon_mrf"
                    ),
                    resolved_from_url=url,
                )
            )
            if max_targets and max_targets > 0 and len(targets) >= max_targets:
                targets = targets[:max_targets]
                break
        if not targets:
            raise ValueError(f"no generated MyMedicalShopper MRF links found for {url}")
        return targets
    finally:
        await ws.close()


_VIVA_HEALTH_COMMERCIAL_MRF_FILES: tuple[dict[str, str], ...] = (
    {
        "url": "https://www.vivahealth.com/files/mrf/viva-health-commercial-in-network-rates",
        "label": "VIVA Health Commercial in-network rates",
        "target_file_type": "in-network",
        "content_disposition_filename": "2026-06-01_vivahealthcommercial_in-network_rates.zip",
    },
    {
        "url": "https://www.vivahealth.com/files/mrf/viva-health-commercial-out-of-network-rates",
        "label": "VIVA Health Commercial allowed amounts",
        "target_file_type": "allowed-amounts",
        "content_disposition_filename": "2026-06-01_vivahealth_commercial_allowed-amounts.zip",
    },
)


def _viva_health_commercial_target(
    source: dict[str, Any], file_info: dict[str, str], *, resolved_from_url: str
) -> CrawlTarget:
    return CrawlTarget(
        source=source,
        url=file_info["url"],
        label=file_info["label"],
        resolved_from_url=resolved_from_url,
        metadata={
            "resolver": "viva_health_mrf",
            "target_kind": "file_reference",
            "target_file_type": file_info["target_file_type"],
            "container_format": "zip",
            "plan_market_type": "group",
            "network_name": "VIVA Health Commercial",
            "content_disposition_filename": file_info["content_disposition_filename"],
        },
    )


def _viva_health_commercial_targets(
    source: dict[str, Any], url: str
) -> list[CrawlTarget]:
    canonical_url = _canonical_or_none(url) or url
    targets: list[CrawlTarget] = []
    for file_info in _VIVA_HEALTH_COMMERCIAL_MRF_FILES:
        file_url = file_info["url"]
        if urlsplit(url).path.startswith("/files/mrf/") and (
            _canonical_or_none(file_url) or file_url
        ) != canonical_url:
            continue
        targets.append(
            _viva_health_commercial_target(source, file_info, resolved_from_url=url)
        )
    return targets


def _viva_health_employer_landing_target(
    source: dict[str, Any],
    *,
    employer_url: str,
    employer_page_url: str,
) -> CrawlTarget:
    employer_slug = _mymedicalshopper_employer_slug_from_url(employer_url)
    group_id = _mymedicalshopper_group_id_from_employer_slug(employer_slug)
    tpa_slug = _mymedicalshopper_tpa_slug_from_employer_slug(employer_slug) or (
        "viva-health" if group_id else None
    )
    employer_name = _slug_label(employer_slug)
    plan_info = []
    if employer_name or group_id:
        plan_info.append(
            {
                "plan_id": group_id,
                "plan_id_type": "group_number" if group_id else None,
                "plan_name": employer_name,
                "plan_market_type": "group",
            }
        )
    return CrawlTarget(
        source=source,
        url=employer_url,
        label=employer_name or "VIVA Health employer MRF page",
        resolved_from_url=employer_page_url,
        metadata={
            "resolver": "viva_health_mrf",
            "target_kind": "source_landing_page",
            "target_file_type": "source-landing-page",
            "external_source_url": employer_url,
            "external_hosting_platform": classify_hosting_platform(employer_url),
            "viva_employer_page_url": employer_page_url,
            "employer_slug": employer_slug,
            "employer_name": employer_name,
            "group_id": group_id,
            "group_number": group_id,
            "tpa_slug": tpa_slug,
            "tpa_name": _slug_label(tpa_slug),
            "landing_reason": "public_employer_mrf_page",
            "plan_info": plan_info,
        },
    )


async def _viva_health_employer_landing_targets(
    source: dict[str, Any],
    *,
    employer_page_url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        employer_page_url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    urls = _delegated_mrf_source_urls_from_html(html_text, base_url=employer_page_url)
    max_links = _as_int(resolver.get("max_employer_links")) or 40
    return [
        _viva_health_employer_landing_target(
            source, employer_url=employer_url, employer_page_url=employer_page_url
        )
        for employer_url in urls[:max_links]
        if classify_hosting_platform(employer_url) == "mymedicalshopper_talon"
    ]


async def _resolve_viva_health_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    path = urlsplit(url).path.rstrip("/").lower()
    targets: list[CrawlTarget] = []
    if path.startswith("/files/mrf/"):
        targets.extend(_viva_health_commercial_targets(source, url))
        if not targets:
            raise ValueError(f"no VIVA Health commercial MRF target found for {url}")
        return targets
    if resolver.get("include_commercial_static", True):
        targets.extend(_viva_health_commercial_targets(source, url))
    if resolver.get("include_employer_mrf_links", True):
        employer_url = urljoin(
            url, str(resolver.get("employer_path") or "/mrf/employers/")
        )
        try:
            employer_targets = await _viva_health_employer_landing_targets(
                source,
                employer_page_url=employer_url,
                resolver=resolver,
                session=session,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            employer_targets = []
        targets.extend(employer_targets)
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no VIVA Health MRF targets found for {url}")
    return targets


def _magnacare_results_url(base_url: str, search_term: str) -> str:
    parsed = urlsplit(str(base_url or ""))
    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    return (
        f"{origin}/Transparency/magnacare/results?"
        + urlencode(
            {
                "filters": f"search-by:{search_term}",
                "orderBy": "",
                "pageNumber": "",
                "perPageCount": "0",
            }
        )
    )


def _magnacare_download_url(base_url: str, run_history_id: str, ip_address: str) -> str:
    parsed = urlsplit(str(base_url or ""))
    origin = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    return (
        f"{origin}/Transparency/magnacare/download-file?"
        + urlencode({"runHistoryID": run_history_id, "ipAddress": ip_address})
    )


def _magnacare_html_attrs(fragment: str) -> dict[str, str]:
    attrs: dict[str, str] = {}
    for match in re.finditer(
        r"""\b(?P<name>[a-zA-Z0-9_-]+)\s*=\s*(?P<quote>["'])(?P<value>.*?)(?P=quote)""",
        fragment or "",
        flags=re.S,
    ):
        attrs[match.group("name").lower()] = html.unescape(match.group("value"))
    return attrs


def _magnacare_file_type(
    *values: Any,
) -> str | None:
    text = " ".join(_clean_text(value) for value in values if value not in (None, ""))
    normalized = text.lower().replace("_", "-")
    if re.search(r"\bout[-\s]*network\b", normalized) or "out-of-network" in normalized:
        return "allowed-amounts"
    return _mrf_body_file_type_from_text(text, text)


def _magnacare_row_plan_info(row: dict[str, Any]) -> list[dict[str, Any]]:
    plan_id = _clean_text(row.get("plan_id"))
    plan_name = _clean_text(row.get("plan_name"))
    market_type = _clean_text(row.get("plan_market_type")).lower() or "group"
    if not plan_id and not plan_name:
        return []
    return [
        {
            "plan_id": plan_id or semantic_hash(
                {"plan_name": plan_name, "market_type": market_type},
                domain="magnacare_plan",
            )[:32],
            "plan_id_type": _clean_text(row.get("plan_id_type")) or None,
            "plan_market_type": market_type,
            "plan_name": plan_name or None,
        }
    ]


def _magnacare_result_rows(html_text: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row_match in re.finditer(
        r"<tr\b(?P<attrs>[^>]*)>(?P<body>.*?)</tr>",
        html_text or "",
        flags=re.I | re.S,
    ):
        attrs = row_match.group("attrs") or ""
        if "default" not in attrs.lower():
            continue
        body = row_match.group("body") or ""
        columns = [
            _strip_html_tags(match.group("body") or "")
            for match in re.finditer(
                r"<td\b[^>]*>(?P<body>.*?)</td>", body, flags=re.I | re.S
            )
        ]
        link_match = re.search(r"<a\b(?P<attrs>[^>]*)>", body, flags=re.I | re.S)
        if len(columns) < 7 or not link_match:
            continue
        link_attrs = _magnacare_html_attrs(link_match.group("attrs") or "")
        run_history_id = _clean_text(link_attrs.get("data-rhid"))
        rows.append(
            {
                "plan_id_type": columns[0],
                "plan_market_type": columns[1],
                "plan_id": columns[2],
                "plan_name": columns[3],
                "network_name": columns[4] or link_attrs.get("data-network"),
                "file_type_label": columns[5],
                "file_size": columns[6] or link_attrs.get("data-fsize"),
                "run_history_id": run_history_id,
                "file_name": _clean_text(link_attrs.get("data-fname")),
                "file_version": _clean_text(link_attrs.get("data-fversion")),
                "external_url": _clean_text(link_attrs.get("data-href")),
                "raw_ere": _clean_text(link_attrs.get("data-ere")),
            }
        )
    return rows


def _magnacare_target_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        _clean_text(row.get("run_history_id")),
        _clean_text(row.get("file_name")).lower(),
        _clean_text(row.get("network_name")).lower(),
        _clean_text(row.get("file_type_label")).lower(),
    )


async def _resolve_magnacare_transparency_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "magnacare_transparency_mrf")
    search_terms = [
        str(item).strip()
        for item in (resolver.get("search_terms") or ["magna", "employee", "benefit"])
        if str(item).strip()
    ]
    max_bytes = int(resolver.get("max_bytes") or 5 * 1024 * 1024)
    ip_address = str(resolver.get("download_ip_address") or "127.0.0.1")
    grouped: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    max_targets = _as_int(resolver.get("max_targets"))
    for search_term in search_terms:
        results_url = _magnacare_results_url(url, search_term)
        html_text = await _fetch_text(results_url, max_bytes=max_bytes, session=session)
        for row in _magnacare_result_rows(html_text):
            run_history_id = _clean_text(row.get("run_history_id"))
            if not run_history_id or run_history_id == "0":
                continue
            file_name = _clean_text(row.get("file_name"))
            file_type = _magnacare_file_type(
                row.get("file_type_label"),
                row.get("network_name"),
                file_name,
            )
            if not file_name or not file_type:
                continue
            key = _magnacare_target_key(row)
            item = grouped.setdefault(
                key,
                {
                    "row": row,
                    "search_terms": set(),
                    "plan_info": [],
                    "plan_keys": set(),
                    "results_urls": set(),
                    "file_type": file_type,
                },
            )
            item["search_terms"].add(search_term)
            item["results_urls"].add(results_url)
            for plan in _magnacare_row_plan_info(row):
                plan_key = (
                    plan.get("plan_id"),
                    plan.get("plan_id_type"),
                    plan.get("plan_market_type"),
                    plan.get("plan_name"),
                )
                if plan_key in item["plan_keys"]:
                    continue
                item["plan_keys"].add(plan_key)
                item["plan_info"].append(plan)
        if max_targets and len(grouped) >= max_targets:
            break
    if not grouped:
        raise ValueError(f"no MagnaCare transparency MRF rows found for {url}")

    targets: list[CrawlTarget] = []
    for item in list(grouped.values())[: max_targets or len(grouped)]:
        row = item["row"]
        run_history_id = _clean_text(row.get("run_history_id"))
        dynamic_download_url = _magnacare_download_url(url, run_history_id, ip_address)
        payload = await _fetch_json(dynamic_download_url, max_bytes=max_bytes, session=session)
        file_url = _clean_text(payload.get("Data") if isinstance(payload, dict) else "")
        if not file_url.startswith(("http://", "https://")):
            continue
        file_name = _clean_text(row.get("file_name")) or Path(urlsplit(file_url).path).name
        network_name = _clean_text(row.get("network_name"))
        file_type = item["file_type"]
        label = " - ".join(
            part
            for part in (
                network_name,
                _clean_text(row.get("file_type_label")) or file_type,
                file_name,
            )
            if part
        )
        targets.append(
            CrawlTarget(
                source=source,
                url=file_url,
                label=label,
                resolved_from_url=url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": "file_reference",
                    "target_file_type": file_type,
                    "container_format": _container_format(file_url),
                    "plan_info": item["plan_info"],
                    "network_name": network_name or None,
                    "file_name": file_name,
                    "file_size": _clean_text(row.get("file_size")) or None,
                    "size_bytes": _parse_size_bytes(row.get("file_size")),
                    "schema_version": _clean_text(row.get("file_version")) or None,
                    "run_history_id": run_history_id,
                    "dynamic_download_url": dynamic_download_url,
                    "search_terms": sorted(item["search_terms"]),
                    "results_urls": sorted(item["results_urls"]),
                    "reporting_entity_name": "MagnaCare and Brighton administered plans",
                    "reporting_entity_type": "third_party_administrator",
                },
            )
        )
    if not targets:
        raise ValueError(f"no downloadable MagnaCare transparency MRF files found for {url}")
    return targets


def _asr_group_number_from_url(url: str | None) -> str | None:
    query = {
        key.lower(): value
        for key, value in parse_qsl(
            urlsplit(str(url or "")).query, keep_blank_values=True
        )
    }
    group_number = str(query.get("groupnumber") or query.get("g") or "").strip()
    return group_number or None


def _crawl_target_context_metadata(target: CrawlTarget) -> dict[str, Any]:
    metadata = dict(target.metadata or {})
    source_metadata = dict((target.source or {}).get("metadata_json") or {})
    group_number = (
        str(metadata.get("group_number") or metadata.get("group_id") or "").strip()
        or _asr_group_number_from_url(target.url)
        or _asr_group_number_from_url(target.resolved_from_url)
    )
    context: dict[str, Any] = {}
    benefit_lines = _normalize_benefit_lines(
        _benefit_line_values(metadata.get("benefit_lines"))
        + _benefit_line_values(metadata.get("benefit_line"))
        + _benefit_line_values(source_metadata.get("benefit_lines"))
        + _benefit_line_values(source_metadata.get("benefit_line"))
    )
    if benefit_lines:
        context["benefit_lines"] = list(benefit_lines)
        if len(benefit_lines) == 1:
            context["benefit_line"] = benefit_lines[0]
    if group_number:
        context["group_id"] = group_number
        context["group_number"] = group_number
    if target.label:
        context["target_label"] = str(target.label)
    for key in (
        "client_id",
        "client_name",
        "company_name",
        "employer_id",
        "employer_name",
        "employer_slug",
        "entity_slug",
        "tpa_slug",
        "tpa_name",
        "ein",
        "plan_id",
        "plan_name",
    ):
        value = str(metadata.get(key) or "").strip()
        if value:
            context[key] = value
    if not context.get("company_name"):
        source_display_name = str((target.source or {}).get("display_name") or "").strip()
        if source_display_name:
            context["company_name"] = source_display_name
    return context


def _apply_crawl_target_context_to_file_rows(
    file_rows: list[dict[str, Any]], target: CrawlTarget
) -> list[dict[str, Any]]:
    context = _crawl_target_context_metadata(target)
    target_plan_info = _metadata_plan_info(dict(target.metadata or {}))
    if not context and not target_plan_info:
        return file_rows
    annotated: list[dict[str, Any]] = []
    for row in file_rows:
        metadata = dict(row.get("metadata_json") or {})
        for key, value in context.items():
            metadata.setdefault(key, value)
        if target_plan_info:
            metadata = _merge_crawl_target_plan_info(metadata, target_plan_info)
        annotated.append({**row, "metadata_json": metadata})
    return annotated


def _plan_info_match_key(plan: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(plan.get("plan_id") or "").strip().lower(),
        _clean_text(plan.get("plan_name")).lower(),
        str(plan.get("plan_market_type") or plan.get("market_type") or "").strip().lower(),
    )


def _merge_crawl_target_plan_info(
    metadata: dict[str, Any], target_plan_info: list[dict[str, Any]]
) -> dict[str, Any]:
    plan_info = metadata.get("plan_info")
    if not isinstance(plan_info, list):
        return metadata
    target_by_key = {
        _plan_info_match_key(plan): plan
        for plan in target_plan_info
        if isinstance(plan, dict)
    }
    if not target_by_key:
        return metadata
    merged_plans: list[Any] = []
    changed = False
    for plan in plan_info:
        if not isinstance(plan, dict):
            merged_plans.append(plan)
            continue
        next_plan = dict(plan)
        target_plan = target_by_key.get(_plan_info_match_key(next_plan)) or {}
        for key in ("engine_plan_hash", "issuer_name", "plan_sponsor_name"):
            if not next_plan.get(key) and target_plan.get(key):
                next_plan[key] = target_plan[key]
                changed = True
        merged_plans.append(next_plan)
    if not changed:
        return metadata
    return {**metadata, "plan_info": merged_plans}


def _asr_group_numbers_from_seed_list(seed_list_name: str | None) -> list[str]:
    return [
        row["group_number"]
        for row in _asr_seed_rows_from_seed_list(seed_list_name)
        if row.get("group_number")
    ]


def _asr_seed_rows_from_seed_list(seed_list_name: str | None) -> list[dict[str, str]]:
    if not seed_list_name:
        return []
    seed_rows: list[dict[str, str]] = []
    for row in _load_seed_list_rows(seed_list_name):
        if str(row.get("status") or "").strip().lower() != "active":
            continue
        group_number = str(row.get("group_number") or "").strip()
        if not re.fullmatch(r"\d{4}", group_number):
            raise ValueError(
                f"ASR seed list {seed_list_name} contains a non-4-digit group number"
            )
        seed_rows.append({**row, "group_number": group_number})
    return seed_rows


_ASR_SEED_CONTEXT_KEYS = (
    "company_name",
    "client_name",
    "employer_id",
    "employer_name",
    "employer_slug",
    "entity_slug",
    "ein",
    "plan_id",
    "plan_name",
    "plan_sponsor_name",
    "tpa_name",
    "tpa_slug",
    "evidence_url",
)


def _asr_seed_context_metadata(row: dict[str, Any]) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for key in _ASR_SEED_CONTEXT_KEYS:
        value = str(row.get(key) or "").strip()
        if value:
            metadata[key] = value
    return metadata


def _asr_configured_group_numbers(resolver: dict[str, Any]) -> list[str]:
    group_numbers: list[str] = []
    group_numbers.extend(
        _asr_group_numbers_from_seed_list(
            str(resolver.get("seed_list") or "").strip() or None
        )
    )
    group_numbers.extend(
        str(value).strip() for value in (resolver.get("group_numbers") or ())
    )
    for group_number in group_numbers:
        if group_number and not re.fullmatch(r"\d{4}", group_number):
            raise ValueError("ASR configured group numbers must be 4 digits")
    return group_numbers


def _asr_group_targets_for_source(url: str, resolver: dict[str, Any]) -> list[dict[str, Any]]:
    group_targets: list[dict[str, Any]] = []
    seen: set[str] = set()
    by_group: dict[str, dict[str, Any]] = {}

    def add_group(value: Any, metadata: dict[str, Any] | None = None) -> None:
        group_number = str(value or "").strip()
        if not group_number or group_number in seen:
            if group_number and metadata:
                by_group.get(group_number, {}).update(
                    {key: item for key, item in metadata.items() if item not in (None, "")}
                )
            return
        if not re.fullmatch(r"\d{4}", group_number):
            raise ValueError("ASR configured group numbers must be 4 digits")
        seen.add(group_number)
        target = {
            "group_number": group_number,
            **{key: item for key, item in (metadata or {}).items() if item not in (None, "")},
        }
        by_group[group_number] = target
        group_targets.append(target)

    add_group(_asr_group_number_from_url(url))
    for row in _asr_seed_rows_from_seed_list(
        str(resolver.get("seed_list") or "").strip() or None
    ):
        add_group(row.get("group_number"), _asr_seed_context_metadata(row))
    for group_number in resolver.get("group_numbers") or ():
        if group_number and not re.fullmatch(r"\d{4}", str(group_number).strip()):
            raise ValueError("ASR configured group numbers must be 4 digits")
        add_group(group_number)
    return group_targets


def _asr_group_numbers_for_source(url: str, resolver: dict[str, Any]) -> list[str]:
    return [target["group_number"] for target in _asr_group_targets_for_source(url, resolver)]


def _asr_toc_url(base_url: str, resolver: dict[str, Any], group_number: str) -> str:
    toc_path = str(resolver.get("toc_path") or "/umbraco/surface/mrfdownload").strip()
    return (
        urljoin(_url_origin(base_url) + "/", toc_path.lstrip("/"))
        + "?"
        + urlencode({"fileType": "TableOfContents", "groupNumber": group_number})
    )


def _resolve_asr_health_benefits_mrf(
    source: dict[str, Any], url: str, resolver: dict[str, Any]
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "asr_health_benefits_mrf")
    targets: list[CrawlTarget] = []
    for group_target in _asr_group_targets_for_source(url, resolver):
        group_number = str(group_target["group_number"])
        context_metadata = {
            key: value
            for key, value in group_target.items()
            if key != "group_number" and value not in (None, "")
        }
        label = f"{source.get('display_name') or 'ASR Health Benefits'} group {group_number}"
        employer_label = (
            context_metadata.get("company_name")
            or context_metadata.get("employer_name")
            or context_metadata.get("client_name")
        )
        if employer_label:
            label = f"{label} - {employer_label}"
        targets.append(
            CrawlTarget(
                source=source,
                url=_asr_toc_url(url, resolver, group_number),
                label=label,
                resolved_from_url=url,
                metadata={
                    "resolver": resolver_type,
                    "target_file_type": "table-of-contents",
                    "group_number": group_number,
                    **context_metadata,
                },
            )
        )
    if not targets:
        raise ValueError("no ASR Health Benefits group numbers configured")
    return targets


def _wordpress_entry_content(html_text: str) -> str:
    match = re.search(
        r"""<div\b[^>]*class=(?P<quote>["'])(?=[^"']*\bentry-content\b)[^"']*(?P=quote)[^>]*>(?P<content>.*?)</div>\s*<!--\s*\.entry-content\s*-->""",
        html_text or "",
        flags=re.I | re.S,
    )
    return match.group("content") if match else (html_text or "")


def _auxiant_directory_url(url: str, resolver: dict[str, Any]) -> str:
    path = (
        str(resolver.get("directory_path") or "/directory-of-data-sources/").strip()
        or "/directory-of-data-sources/"
    )
    return urljoin(_url_origin(url) + "/", path.lstrip("/"))


def _auxiant_network_label(label: Any) -> str:
    return _clean_text(str(label or "").lstrip("*"))


def _auxiant_anchor_links(html_text: str, *, base_url: str) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    for match in re.finditer(
        r"<a\b(?P<attrs>[^>]*)>(?P<label>.*?)</a\s*>",
        html_text or "",
        flags=re.I | re.S,
    ):
        href_match = re.search(
            r"""href\s*=\s*(?P<quote>["'])(?P<value>.*?)(?P=quote)""",
            match.group("attrs") or "",
            flags=re.I | re.S,
        )
        if not href_match:
            continue
        raw = html.unescape(str(href_match.group("value") or "")).strip()
        if not raw or raw.lower().startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        label = html.unescape(
            re.sub(r"<[^>]+>", " ", match.group("label") or "")
        ).replace("\xa0", " ")
        label = re.sub(r"\s+", " ", label).strip()
        links.append({"url": urljoin(base_url, raw), "label": label})
    return links


def _parse_auxiant_directory_networks(
    html_text: str,
    *,
    base_url: str,
    data_available_only: bool = True,
) -> list[dict[str, Any]]:
    content = _wordpress_entry_content(html_text)
    base_host = _domain(base_url)
    networks: dict[str, dict[str, Any]] = {}
    for candidate in _auxiant_anchor_links(content, base_url=base_url):
        raw_label = str(candidate.get("label") or "").strip()
        if not raw_label:
            continue
        data_available = raw_label.startswith("*")
        if data_available_only and not data_available:
            continue
        url = str(candidate.get("url") or "").strip()
        if _domain(url) != base_host:
            continue
        path = urlsplit(url).path.rstrip("/") + "/"
        if path in {"/", "/directory-of-data-sources/", "/implementation-in-process/"}:
            continue
        if path.startswith(("/wp-", "/index/")):
            continue
        label = _auxiant_network_label(raw_label)
        if not label:
            continue
        key = _canonical_or_none(url) or url
        previous = networks.get(key)
        if previous and previous.get("data_available"):
            continue
        networks[key] = {"url": url, "label": label, "data_available": data_available}
    return list(networks.values())


def _auxiant_download_extension(
    url: str | None, label: str | None = None
) -> str | None:
    parsed = urlsplit(str(url or ""))
    text = f"{parsed.path} {parsed.query} {label or ''}".lower()
    for extension in (".json.gz", ".json.zip", ".zip", ".7z", ".json", ".csv", ".gz"):
        if extension in text:
            return extension
    return None


def _auxiant_container_format(url: str | None, label: str | None = None) -> str | None:
    detected = _container_format(url)
    if detected:
        return detected
    extension = _auxiant_download_extension(url, label)
    if extension in {".zip", ".json.zip"}:
        return "zip"
    if extension == ".7z":
        return "7z"
    if extension in {".json.gz", ".gz"}:
        return "gzip"
    return None


def _auxiant_is_download_link(url: str | None, label: str | None = None) -> bool:
    if not _auxiant_download_extension(url, label):
        return False
    parsed = urlsplit(str(url or ""))
    path = parsed.path.lower()
    if path.startswith(("/wp-content/", "/wp-includes/")):
        return False
    if path.startswith("/wp-admin/admin-ajax.php"):
        query = {
            key.lower(): value
            for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        }
        return query.get("cmd") == "file"
    return True


def _auxiant_file_type(url: str | None, label: str | None = None) -> str:
    text = f"{url or ''} {label or ''}".lower().replace("_", "-")
    if any(
        token in text
        for token in (
            "allowed-amount",
            "allowed amount",
            "out-of-network",
            "out of network",
            "/oon/",
            " oon ",
            "oon-mrf",
        )
    ):
        return "allowed-amounts"
    if any(
        token in text
        for token in ("payer-drug", "prescription-drug", "pharmacy", " rx ", "/rx/")
    ):
        return "payer-drug"
    return _mrf_file_type_from_text(url, label) or "in-network"


def _parse_auxiant_page_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    content = _wordpress_entry_content(html_text)
    base_host = _domain(base_url)
    links: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in _html_link_candidates(content, base_url=base_url):
        if candidate.get("attr") != "href":
            continue
        url = str(candidate.get("url") or "").strip()
        if not url:
            continue
        label = (
            _clean_text(candidate.get("label"))
            or Path(urlsplit(url).path).name
            or "MRF file"
        )
        if "return to list" in label.lower():
            continue
        parsed = urlsplit(url)
        if _auxiant_is_download_link(url, label):
            key = ("file_reference", _canonical_or_none(url) or url)
            links[key] = {
                "url": url,
                "label": label,
                "target_kind": "file_reference",
                "target_file_type": _auxiant_file_type(url, label),
                "container_format": _auxiant_container_format(url, label),
            }
            continue
        if parsed.scheme in {"http", "https"} and _domain(url) != base_host:
            key = ("external_landing", _canonical_or_none(url) or url)
            links[key] = {
                "url": url,
                "label": label,
                "target_kind": "external_landing",
                "hosting_platform": classify_hosting_platform(url),
            }
    return list(links.values())


def _auxiant_display_label(network_name: str) -> str:
    return f"Auxiant - {network_name}" if network_name else "Auxiant"


def _auxiant_direct_target(
    source: dict[str, Any],
    link: dict[str, Any],
    *,
    network_name: str,
    page_url: str,
    directory_url: str,
    resolver_type: str,
) -> CrawlTarget:
    return CrawlTarget(
        source=source,
        url=str(link["url"]),
        label=_auxiant_display_label(network_name),
        resolved_from_url=page_url,
        metadata={
            "resolver": resolver_type,
            "target_kind": "file_reference",
            "target_file_type": link.get("target_file_type"),
            "container_format": link.get("container_format"),
            "auxiant_network_name": network_name,
            "auxiant_network_url": page_url,
            "auxiant_directory_url": directory_url,
            "file_label": link.get("label"),
        },
    )


def _auxiant_landing_target(
    source: dict[str, Any],
    *,
    network_name: str,
    page_url: str,
    directory_url: str,
    landing_url: str,
    resolver_type: str,
    reason: str,
    landing_label: str | None = None,
    nested_error: str | None = None,
) -> CrawlTarget:
    return CrawlTarget(
        source=source,
        url=landing_url,
        label=_auxiant_display_label(network_name),
        resolved_from_url=page_url,
        metadata={
            "resolver": resolver_type,
            "target_kind": "source_landing_page",
            "target_file_type": "source-landing-page",
            "auxiant_network_name": network_name,
            "auxiant_network_url": page_url,
            "auxiant_directory_url": directory_url,
            "external_source_url": landing_url if landing_url != page_url else None,
            "external_hosting_platform": classify_hosting_platform(landing_url),
            "landing_label": landing_label,
            "landing_reason": reason,
            "nested_error": _truncate_text(nested_error, 500),
        },
    )


def _auxiant_annotated_target(
    target: CrawlTarget,
    *,
    source: dict[str, Any],
    network_name: str,
    page_url: str,
    directory_url: str,
    external_url: str,
    resolver_type: str,
) -> CrawlTarget:
    metadata = dict(target.metadata or {})
    nested_resolver = metadata.get("resolver")
    metadata.update(
        {
            "resolver": resolver_type,
            "nested_resolver": nested_resolver,
            "auxiant_network_name": network_name,
            "auxiant_network_url": page_url,
            "auxiant_directory_url": directory_url,
            "external_source_url": external_url,
            "external_hosting_platform": classify_hosting_platform(external_url),
            "nested_target_label": target.label,
        }
    )
    return CrawlTarget(
        source=source,
        url=target.url,
        label=_auxiant_display_label(network_name),
        resolved_from_url=target.resolved_from_url or external_url,
        metadata=metadata,
    )


async def _resolve_auxiant_wordpress_directory(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "auxiant_wordpress_directory")
    directory_url = _auxiant_directory_url(url, resolver)
    directory_html = await _fetch_text(
        directory_url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    networks = _parse_auxiant_directory_networks(
        directory_html,
        base_url=directory_url,
        data_available_only=bool(resolver.get("data_available_only", True)),
    )
    max_networks = _as_int(resolver.get("max_networks"))
    if max_networks:
        networks = networks[:max_networks]

    targets: list[CrawlTarget] = []
    for link in _parse_auxiant_page_links(directory_html, base_url=directory_url):
        if link.get("target_kind") == "file_reference":
            targets.append(
                _auxiant_direct_target(
                    source,
                    link,
                    network_name="Historical Out of Network Allowed Amounts",
                    page_url=directory_url,
                    directory_url=directory_url,
                    resolver_type=resolver_type,
                )
            )

    for network in networks:
        page_url = str(network["url"])
        network_name = str(network["label"])
        page_html = await _fetch_text(
            page_url,
            max_bytes=int(resolver.get("page_max_bytes") or 10 * 1024 * 1024),
            session=session,
        )
        page_links = _parse_auxiant_page_links(page_html, base_url=page_url)
        for link in page_links:
            if link.get("target_kind") == "file_reference":
                targets.append(
                    _auxiant_direct_target(
                        source,
                        link,
                        network_name=network_name,
                        page_url=page_url,
                        directory_url=directory_url,
                        resolver_type=resolver_type,
                    )
                )
                continue
            external_url = str(link.get("url") or "").strip()
            if not external_url:
                continue
            nested_platform = classify_hosting_platform(external_url)
            nested_source = {**source, "hosting_platform": nested_platform}
            nested_error: str | None = None
            try:
                nested_targets = await _crawl_targets_for_source(
                    nested_source, external_url, session
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                nested_targets = []
                nested_error = f"{type(exc).__name__}: {exc}"
            for nested_target in nested_targets:
                targets.append(
                    _auxiant_annotated_target(
                        nested_target,
                        source=source,
                        network_name=network_name,
                        page_url=page_url,
                        directory_url=directory_url,
                        external_url=external_url,
                        resolver_type=resolver_type,
                    )
                )
            if not nested_targets:
                targets.append(
                    _auxiant_landing_target(
                        source,
                        network_name=network_name,
                        page_url=page_url,
                        directory_url=directory_url,
                        landing_url=external_url,
                        resolver_type=resolver_type,
                        reason="external_landing_no_concrete_targets",
                        landing_label=str(link.get("label") or ""),
                        nested_error=nested_error,
                    )
                )
        if not page_links:
            targets.append(
                _auxiant_landing_target(
                    source,
                    network_name=network_name,
                    page_url=page_url,
                    directory_url=directory_url,
                    landing_url=page_url,
                    resolver_type=resolver_type,
                    reason="auxiant_page_without_file_links",
                    landing_label=network_name,
                )
            )
    if not targets:
        raise ValueError(f"no Auxiant network MRF links found for {directory_url}")
    return targets


def _sapphire_toc_target(
    raw_url: Any,
    *,
    base_url: str | None,
    label: Any = None,
    file_name: Any = None,
    payer_name: Any = None,
) -> dict[str, Any] | None:
    value = html.unescape(str(raw_url or "")).strip()
    if not value:
        return None
    url = urljoin(base_url or "", value)
    path = urlsplit(url).path
    path_lower = path.lower()
    if "/tocs/" not in path_lower:
        return None
    inferred_file_name = str(file_name or Path(path).name or "").strip()
    inferred_payer_name = _clean_text(payer_name)
    clean_label = _clean_text(label)
    generic_labels = {
        "",
        "copy",
        "download",
        "duplicate",
        "file",
        "open",
        "view",
        inferred_file_name.lower(),
    }
    if clean_label.lower() in generic_labels:
        clean_label = ""
    inferred_label = (
        clean_label
        or inferred_payer_name
        or _label_from_index_name(inferred_file_name or Path(path).name)
    )
    if not _looks_html_mrf_toc_url(url, inferred_file_name or inferred_label):
        return None
    target: dict[str, Any] = {
        "url": url,
        "label": inferred_label,
    }
    if inferred_file_name:
        target["file_name"] = inferred_file_name
    if inferred_payer_name:
        target["payer_name"] = inferred_payer_name
    return target


def _parse_sapphire_toc_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    urls: dict[str, dict[str, Any]] = {}
    for candidate in _html_link_candidates(html_text or "", base_url=base_url):
        target = _sapphire_toc_target(
            candidate.get("url"),
            base_url=base_url,
            label=candidate.get("label"),
        )
        if not target:
            continue
        urls[_canonical_or_none(str(target["url"])) or str(target["url"])] = target
    decoded_text = (html_text or "").replace("\\/", "/")
    for match in re.finditer(
        r"""(?P<quote>["'])(?P<url>(?:https?://[^"']+)?/tocs/[^"']+)(?P=quote)""",
        decoded_text,
        flags=re.I,
    ):
        raw = match.group("url")
        target = _sapphire_toc_target(raw, base_url=base_url)
        if not target:
            continue
        urls.setdefault(
            _canonical_or_none(str(target["url"])) or str(target["url"]),
            target,
        )
    return list(urls.values())


def _sapphire_origin_url(url: str) -> str:
    parsed = urlsplit(str(url or ""))
    if not parsed.scheme or not parsed.netloc:
        return url
    return f"{parsed.scheme}://{parsed.netloc}/"


def _sapphire_query_slug_variants(query: str | None) -> list[str]:
    raw = _clean_text(query)
    if not raw:
        return []
    normalized = raw.lower()
    replacements = {
        " incorporated": " inc",
        " limited liability company": " llc",
        " corporation": " corp",
        " company": " co",
    }
    for old, new in replacements.items():
        normalized = normalized.replace(old, new)
    tokens = re.findall(r"[a-z0-9]+", normalized)
    if not tokens:
        return []
    variants: list[list[str]] = [tokens]
    corporate_suffixes = {"inc", "llc", "ltd", "co", "corp", "corporation", "company"}
    if len(tokens) > 1 and tokens[-1] in corporate_suffixes:
        variants.append(tokens[:-1])
    slugs: list[str] = []
    for variant in variants:
        for separator in ("-", "_"):
            slug = separator.join(variant).strip(separator)
            if slug and slug not in slugs:
                slugs.append(slug)
    return slugs[:8]


async def _sapphire_query_probe_targets(
    source: dict[str, Any],
    url: str,
    query: str | None,
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    if classify_hosting_platform(url) != "sapphire":
        return []
    origin = _sapphire_origin_url(url)
    targets: list[CrawlTarget] = []
    for slug in _sapphire_query_slug_variants(query):
        probe_url = urljoin(origin, f"tocs/current/{slug}")
        head = await _head_url(probe_url, session=session)
        if str(head.get("status") or "") != "ok":
            continue
        targets.append(
            CrawlTarget(
                source=source,
                url=probe_url,
                label=_clean_text(query) or _label_from_index_name(slug),
                resolved_from_url=url,
                metadata={
                    "resolver": "sapphire_html_tocs",
                    "file_name": slug,
                    "payer_name": _clean_text(query) or None,
                    "company_name": _clean_text(query) or None,
                    "query_probe_slug": slug,
                },
            )
        )
    return targets


def _sapphire_page_data_url(url: str) -> str:
    return urljoin(_sapphire_origin_url(url), "page-data/index/page-data.json")


def _sapphire_static_query_url(url: str, query_hash: str) -> str:
    return urljoin(
        _sapphire_origin_url(url),
        f"page-data/sq/d/{quote(str(query_hash).strip(), safe='')}.json",
    )


def _sapphire_static_query_hashes(page_data_text: str | None) -> list[str]:
    hashes: list[str] = []
    try:
        payload = json.loads(page_data_text or "{}")
    except json.JSONDecodeError:
        payload = {}
    if isinstance(payload, dict):
        raw_hashes = payload.get("staticQueryHashes")
        if not isinstance(raw_hashes, list):
            result = payload.get("result")
            if isinstance(result, dict):
                raw_hashes = result.get("staticQueryHashes")
        if isinstance(raw_hashes, list):
            for item in raw_hashes:
                value = str(item or "").strip()
                if value:
                    hashes.append(value)
    if hashes:
        return list(dict.fromkeys(hashes))
    for match in re.finditer(
        r'"staticQueryHashes"\s*:\s*\[(?P<values>[^\]]*)\]',
        page_data_text or "",
        flags=re.I | re.S,
    ):
        for value in re.findall(r'"([^"]+)"', match.group("values")):
            value = value.strip()
            if value:
                hashes.append(value)
    return list(dict.fromkeys(hashes))


def _parse_sapphire_static_query_toc_links(
    query_text: str | None,
    *,
    base_url: str | None = None,
) -> list[dict[str, Any]]:
    try:
        payload = json.loads(query_text or "{}")
    except json.JSONDecodeError:
        return []
    urls: dict[str, dict[str, Any]] = {}

    def visit(value: Any, context: dict[str, Any] | None = None) -> None:
        if isinstance(value, dict):
            next_context = dict(context or {})
            for key in ("payer_name", "name", "label", "title", "file_name"):
                if value.get(key):
                    next_context.setdefault(key, value.get(key))
            for key in ("url", "href", "publicURL", "downloadUrl", "path"):
                if not value.get(key):
                    continue
                target = _sapphire_toc_target(
                    value.get(key),
                    base_url=base_url,
                    label=next_context.get("label")
                    or next_context.get("title")
                    or next_context.get("name"),
                    file_name=next_context.get("file_name"),
                    payer_name=next_context.get("payer_name"),
                )
                if not target:
                    continue
                urls.setdefault(
                    _canonical_or_none(str(target["url"])) or str(target["url"]),
                    target
                )
            for child in value.values():
                visit(child, next_context)
            return
        if isinstance(value, list):
            for child in value:
                visit(child, context)
            return
        if isinstance(value, str) and "/tocs/" in value:
            target = _sapphire_toc_target(
                value,
                base_url=base_url,
                label=(
                    context.get("label")
                    or context.get("title")
                    or context.get("name")
                    if context
                    else None
                ),
                file_name=context.get("file_name") if context else None,
                payer_name=context.get("payer_name") if context else None,
            )
            if target:
                urls.setdefault(
                    _canonical_or_none(str(target["url"])) or str(target["url"]),
                    target
                )

    visit(payload)
    return list(urls.values())


async def _resolve_sapphire_static_query_toc_links(
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[dict[str, Any]]:
    page_data_url = _sapphire_page_data_url(url)
    page_data_text = await _fetch_text(
        page_data_url,
        max_bytes=int(resolver.get("page_data_max_bytes") or 1024 * 1024),
        session=session,
        expect_json=True,
    )
    max_queries = _as_int(resolver.get("max_static_queries")) or 8
    max_targets = _as_int(resolver.get("max_targets"))
    targets: dict[str, dict[str, Any]] = {}
    for target in _parse_sapphire_static_query_toc_links(page_data_text, base_url=url):
        key = _canonical_or_none(str(target.get("url") or "")) or str(
            target.get("url") or ""
        )
        if key:
            targets[key] = target
        if max_targets and len(targets) >= max_targets:
            return list(targets.values())
    if targets:
        return list(targets.values())
    for query_hash in _sapphire_static_query_hashes(page_data_text)[:max_queries]:
        query_text = await _fetch_text(
            _sapphire_static_query_url(url, query_hash),
            max_bytes=int(resolver.get("static_query_max_bytes") or 25 * 1024 * 1024),
            session=session,
            expect_json=True,
        )
        for target in _parse_sapphire_static_query_toc_links(query_text, base_url=url):
            key = _canonical_or_none(str(target.get("url") or "")) or str(
                target.get("url") or ""
            )
            if key:
                targets[key] = target
            if max_targets and len(targets) >= max_targets:
                return list(targets.values())
    return list(targets.values())


def _parse_healthgram_network_pages(
    html_text: str, *, base_url: str
) -> list[dict[str, Any]]:
    pages: dict[str, dict[str, Any]] = {}
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "").strip()
        parsed = urlsplit(url)
        if parsed.netloc.lower() != "mrf.healthgram.com" or not re.match(
            r"^/network/[^/?#]+\.cfm$", parsed.path, flags=re.I
        ):
            continue
        label = _clean_text(candidate.get("label")) or Path(parsed.path).stem
        pages[_canonical_or_none(url) or url] = {"url": url, "label": label}
    return list(pages.values())


async def _resolve_healthgram_network_index(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "healthgram_network_index")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    network_pages = _parse_healthgram_network_pages(html_text, base_url=url)
    if not network_pages:
        network_pages = [
            {"url": url, "label": source.get("display_name") or "Healthgram"}
        ]
    max_pages = _as_int(resolver.get("max_network_pages")) or 50
    targets_by_url: dict[str, CrawlTarget] = {}
    for network_page in network_pages[:max_pages]:
        page_url = str(network_page.get("url") or "").strip()
        page_label = (
            _clean_text(network_page.get("label"))
            or source.get("display_name")
            or "Healthgram"
        )
        page_html = (
            html_text
            if page_url == url
            else await _fetch_text(
                page_url,
                max_bytes=int(
                    resolver.get("network_page_max_bytes")
                    or resolver.get("max_bytes")
                    or 5 * 1024 * 1024
                ),
                session=session,
            )
        )
        for target in _parse_html_mrf_links(page_html, base_url=page_url):
            if target.get("target_file_type") != "table-of-contents":
                continue
            target_url = str(target.get("url") or "").strip()
            if not target_url:
                continue
            index_label = _clean_text(target.get("label"))
            key = _canonical_or_none(target_url) or target_url
            targets_by_url[key] = CrawlTarget(
                source=source,
                url=target_url,
                label=page_label,
                resolved_from_url=page_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": target.get("target_kind"),
                    "target_file_type": "table-of-contents",
                    "container_format": target.get("container_format"),
                    "html_attr": target.get("html_attr"),
                    "healthgram_index_label": index_label,
                    "healthgram_landing_url": url,
                    "healthgram_network_page_url": page_url,
                    "healthgram_network_name": page_label,
                },
            )
    targets = list(targets_by_url.values())
    if not targets:
        raise ValueError(f"no Healthgram network index links found for {url}")
    return targets


async def _resolve_anthem_s3_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url, max_bytes=int(resolver.get("max_bytes") or 1024 * 1024), session=session
    )
    script_urls = [
        str(candidate.get("url") or "")
        for candidate in _html_link_candidates(html_text, base_url=url)
        if urlsplit(str(candidate.get("url") or "")).path.endswith(".js")
        and "script"
        in Path(urlsplit(str(candidate.get("url") or "")).path).name.lower()
    ]
    configured_script = str(resolver.get("script_path") or "").strip()
    if configured_script:
        script_urls.append(urljoin(url, configured_script))
    script_text = ""
    for script_url in dict.fromkeys(script_urls):
        try:
            script_text = await _fetch_text(
                script_url,
                max_bytes=int(resolver.get("max_bytes") or 1024 * 1024),
                session=session,
            )
            if script_text:
                break
        except Exception:  # pylint: disable=broad-exception-caught
            continue
    bases = _anthem_s3_bases_from_script(script_text)
    for status_url in _anthem_s3_status_urls_from_script(script_text):
        head = await _head_url(status_url, session)
        if str(head.get("status") or "") == "ok":
            bases.insert(0, status_url.rsplit("/", 1)[0] + "/")
            break
    if not bases:
        bases = ["https://antm-pt-prod-dataz-nogbd-nophi-us-east1.s3.amazonaws.com/"]
    targets = _anthem_s3_toc_targets(
        source,
        bases[0],
        _anthem_s3_toc_patterns_from_script(script_text, source_url=url),
        resolver,
        source_url=url,
    )
    if not targets:
        raise ValueError(f"no Anthem S3 TOC targets resolved for {url}")
    return targets


async def _resolve_hcsc_asomrf_landing(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    state_urls = _hcsc_asomrf_urls_from_html(html_text, base_url=url)
    max_state_pages = _as_int(resolver.get("max_state_pages")) or 10
    nested_resolver = dict(resolver)
    nested_resolver["type"] = "bcbs_asomrf_filelist"
    nested_resolver.pop("max_targets", None)
    targets: list[CrawlTarget] = []
    for state_url in state_urls[:max_state_pages]:
        try:
            for target in await _resolve_bcbs_asomrf_filelist(
                source, state_url, nested_resolver, session
            ):
                metadata = {
                    **dict(target.metadata or {}),
                    "resolver": "hcsc_asomrf_landing",
                    "delegated_resolver": (target.metadata or {}).get("resolver"),
                    "delegated_source_url": state_url,
                    "hcsc_landing_url": url,
                }
                targets.append(
                    CrawlTarget(
                        source=source,
                        url=target.url,
                        label=target.label,
                        resolved_from_url=target.resolved_from_url,
                        metadata=metadata,
                    )
                )
        except Exception:  # pylint: disable=broad-exception-caught
            continue
    targets = _dedupe_crawl_targets_by_url(targets)
    targets = _limit_crawl_targets_round_robin(
        targets, "state", _as_int(resolver.get("max_targets"))
    )
    if not targets:
        raise ValueError(f"no HCSC ASO MRF targets found for {url}")
    return targets


async def _resolve_point32_azure_mrf_directory(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    if urlsplit(url).netloc.lower().endswith(".web.core.windows.net"):
        directory_urls = [url]
    else:
        html_text = await _fetch_text(
            url,
            max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
            session=session,
        )
        for target in _crawl_targets_from_html_mrf_links(
            source,
            html_text,
            base_url=url,
            resolver="point32_azure_mrf_directory",
        ):
            metadata = {
                **dict(target.metadata or {}),
                "resolver": "point32_azure_mrf_directory",
                "point32_landing_url": url,
            }
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target.url,
                    label=target.label,
                    resolved_from_url=target.resolved_from_url,
                    metadata=metadata,
                )
            )
        directory_urls = _point32_directory_urls_from_html(html_text, base_url=url)
        if not directory_urls:
            directory_urls = _html_mrf_directory_urls(html_text, base_url=url)
    max_directories = _as_int(resolver.get("max_directories")) or 5
    for directory_url in directory_urls[:max_directories]:
        directory_html = await _fetch_text(
            directory_url,
            max_bytes=int(resolver.get("directory_max_bytes") or 50 * 1024 * 1024),
            session=session,
        )
        for target in _crawl_targets_from_html_mrf_links(
            source,
            directory_html,
            base_url=directory_url,
            resolver="point32_azure_mrf_directory",
        ):
            metadata = {
                **dict(target.metadata or {}),
                "resolver": "point32_azure_mrf_directory",
                "point32_landing_url": url,
                "point32_directory_url": directory_url,
            }
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target.url,
                    label=target.label,
                    resolved_from_url=directory_url,
                    metadata=metadata,
                )
            )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Point32 Azure MRF directory targets found for {url}")
    return targets


async def _resolve_html_delegated_mrf_links(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    targets = _crawl_targets_from_html_mrf_links(
        source, html_text, base_url=url, resolver="html_delegated_mrf_links"
    )
    max_links = _as_int(resolver.get("max_links")) or 40
    for delegated_url in _delegated_mrf_source_urls_from_html(html_text, base_url=url)[
        :max_links
    ]:
        platform = classify_hosting_platform(delegated_url)
        nested_source = {
            **source,
            "hosting_platform": platform,
            "index_url": delegated_url,
            "human_url": delegated_url,
        }
        try:
            delegated_targets = await _crawl_targets_for_source(
                nested_source,
                delegated_url,
                session,
                target_limit=_as_int(resolver.get("max_targets")),
            )
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        for target in delegated_targets:
            metadata = {
                **dict(target.metadata or {}),
                "delegated_source_url": delegated_url,
                "delegated_source_platform": platform,
                "delegated_landing_url": url,
            }
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target.url,
                    label=target.label,
                    resolved_from_url=target.resolved_from_url or delegated_url,
                    metadata=metadata,
                )
            )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no delegated MRF links found for {url}")
    return targets


def _target_url_matches_any_pattern(target: CrawlTarget, patterns: Iterable[Any]) -> bool:
    url = str(target.url or "")
    return any(
        str(pattern or "").strip()
        and re.search(str(pattern), url, flags=re.IGNORECASE)
        for pattern in patterns
    )


def _filter_crawl_targets_by_resolver_patterns(
    targets: list[CrawlTarget], resolver: dict[str, Any]
) -> list[CrawlTarget]:
    include_patterns = list(resolver.get("include_url_patterns") or [])
    exclude_patterns = list(resolver.get("exclude_url_patterns") or [])
    if include_patterns:
        targets = [
            target
            for target in targets
            if _target_url_matches_any_pattern(target, include_patterns)
        ]
    if exclude_patterns:
        targets = [
            target
            for target in targets
            if not _target_url_matches_any_pattern(target, exclude_patterns)
        ]
    return targets


async def _resolve_html_mrf_links(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    target_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes"))
    targets = _crawl_targets_from_html_mrf_links(
        source,
        html_text,
        base_url=url,
        resolver="html_mrf_links",
        target_max_bytes=target_max_bytes,
    )
    follow_directory_links = resolver.get("follow_directory_links", True) and (
        not targets or bool(resolver.get("follow_directory_links_when_targets"))
    )
    if follow_directory_links:
        directory_urls = _html_mrf_directory_urls(html_text, base_url=url)
        max_directories = _as_int(resolver.get("max_directories")) or 10
        directory_max_bytes = int(
            resolver.get("directory_max_bytes")
            or resolver.get("max_bytes")
            or 5 * 1024 * 1024
        )
        for directory_url in directory_urls[:max_directories]:
            try:
                directory_html = await _fetch_text(
                    directory_url,
                    max_bytes=directory_max_bytes,
                    session=session,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                continue
            directory_targets = _crawl_targets_from_html_mrf_links(
                source,
                directory_html,
                base_url=directory_url,
                resolver="html_mrf_directory_link",
                target_max_bytes=target_max_bytes,
            )
            if not directory_targets:
                nested_directory_urls = _html_mrf_directory_urls(
                    directory_html, base_url=directory_url
                )
                max_nested_directories = (
                    _as_int(resolver.get("max_nested_directories_per_directory")) or 5
                )
                for nested_directory_url in nested_directory_urls[
                    :max_nested_directories
                ]:
                    try:
                        nested_directory_html = await _fetch_text(
                            nested_directory_url,
                            max_bytes=directory_max_bytes,
                            session=session,
                        )
                    except Exception:  # pylint: disable=broad-exception-caught
                        continue
                    for target in _crawl_targets_from_html_mrf_links(
                        source,
                        nested_directory_html,
                        base_url=nested_directory_url,
                        resolver="html_mrf_nested_directory_link",
                        target_max_bytes=target_max_bytes,
                    ):
                        metadata = {
                            **dict(target.metadata or {}),
                            "directory_url": directory_url,
                            "nested_directory_url": nested_directory_url,
                        }
                        targets.append(
                            CrawlTarget(
                                source=source,
                                url=target.url,
                                label=target.label,
                                resolved_from_url=target.resolved_from_url,
                                metadata=metadata,
                            )
                        )
            for target in directory_targets:
                metadata = {
                    **dict(target.metadata or {}),
                    "directory_url": directory_url,
                }
                targets.append(
                    CrawlTarget(
                        source=source,
                        url=target.url,
                        label=target.label,
                        resolved_from_url=target.resolved_from_url,
                        metadata=metadata,
                    )
                )
        targets = _dedupe_crawl_targets_by_url(targets)
    if not targets and resolver.get("follow_iframe_links", True):
        frame_urls = _html_mrf_frame_urls(html_text, base_url=url)
        max_frames = _as_int(resolver.get("max_frames")) or 5
        frame_max_bytes = int(
            resolver.get("frame_max_bytes")
            or resolver.get("directory_max_bytes")
            or resolver.get("max_bytes")
            or 5 * 1024 * 1024
        )
        for frame_url in frame_urls[:max_frames]:
            try:
                frame_html = await _fetch_text(
                    frame_url,
                    max_bytes=frame_max_bytes,
                    session=session,
                )
            except Exception:  # pylint: disable=broad-exception-caught
                continue
            frame_targets = _crawl_targets_from_html_mrf_links(
                source,
                frame_html,
                base_url=frame_url,
                resolver="html_mrf_frame_link",
                target_max_bytes=target_max_bytes,
            )
            for target in frame_targets:
                metadata = {
                    **dict(target.metadata or {}),
                    "frame_url": frame_url,
                }
                targets.append(
                    CrawlTarget(
                        source=source,
                        url=target.url,
                        label=target.label,
                        resolved_from_url=target.resolved_from_url,
                        metadata=metadata,
                    )
                )
        targets = _dedupe_crawl_targets_by_url(targets)
    targets = _filter_crawl_targets_by_resolver_patterns(targets, resolver)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no direct HTML MRF links found for {url}")
    return targets


def _ebms_index_page_urls(html_text: str, *, base_url: str) -> list[tuple[str, str]]:
    urls: list[tuple[str, str]] = []
    seen: set[str] = set()
    base_host = urlsplit(base_url).netloc.lower()
    base_key = _canonical_or_none(base_url) or base_url
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "")
        parsed = urlsplit(url)
        if parsed.netloc.lower() != base_host:
            continue
        if not parsed.path.lower().endswith("/index.html"):
            continue
        key = _canonical_or_none(url) or url
        if key == base_key or key in seen:
            continue
        seen.add(key)
        urls.append((url, _clean_text(candidate.get("label"))))
    return urls


async def _resolve_ebms_caa_directory(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    root_html = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    targets: list[CrawlTarget] = []
    target_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes"))
    max_targets = _as_int(resolver.get("max_targets")) or 5000
    max_clients = _as_int(resolver.get("max_clients")) or 500
    max_nested = _as_int(resolver.get("max_nested_pages_per_client")) or 20
    client_page_max_bytes = int(
        resolver.get("client_page_max_bytes")
        or resolver.get("max_bytes")
        or 5 * 1024 * 1024
    )
    nested_page_max_bytes = int(
        resolver.get("nested_page_max_bytes")
        or resolver.get("client_page_max_bytes")
        or resolver.get("max_bytes")
        or 5 * 1024 * 1024
    )

    async def page_targets(
        page_url: str, *, client_url: str, client_label: str, nested_url: str | None
    ) -> list[CrawlTarget]:
        page_html = await _fetch_text(
            page_url,
            max_bytes=nested_page_max_bytes if nested_url else client_page_max_bytes,
            session=session,
        )
        page_targets = _crawl_targets_from_html_mrf_links(
            source,
            page_html,
            base_url=page_url,
            resolver="ebms_caa_directory",
            target_max_bytes=target_max_bytes,
        )
        enriched: list[CrawlTarget] = []
        for target in page_targets:
            metadata = {
                **dict(target.metadata or {}),
                "resolver": "ebms_caa_directory",
                "ebms_client_url": client_url,
            }
            if client_label:
                metadata["ebms_client_label"] = client_label
            if nested_url:
                metadata["ebms_nested_url"] = nested_url
            enriched.append(
                CrawlTarget(
                    source=source,
                    url=target.url,
                    label=target.label,
                    resolved_from_url=page_url,
                    metadata=metadata,
                )
            )
        return enriched

    for client_url, client_label in _ebms_index_page_urls(root_html, base_url=url)[
        :max_clients
    ]:
        try:
            client_targets = await page_targets(
                client_url,
                client_url=client_url,
                client_label=client_label,
                nested_url=None,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        targets.extend(client_targets)
        if not client_targets:
            try:
                client_html = await _fetch_text(
                    client_url, max_bytes=client_page_max_bytes, session=session
                )
            except Exception:  # pylint: disable=broad-exception-caught
                client_html = ""
            for nested_url, _nested_label in _ebms_index_page_urls(
                client_html, base_url=client_url
            )[:max_nested]:
                try:
                    targets.extend(
                        await page_targets(
                            nested_url,
                            client_url=client_url,
                            client_label=client_label,
                            nested_url=nested_url,
                        )
                    )
                except Exception:  # pylint: disable=broad-exception-caught
                    continue
                if len(targets) >= max_targets:
                    break
        if len(targets) >= max_targets:
            break
    targets = _dedupe_crawl_targets_by_url(targets)
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no EBMS CAA directory MRF links found for {url}")
    return targets


def _html_mrf_directory_urls(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    base_key = _canonical_or_none(base_url) or base_url
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "")
        parsed = urlsplit(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        file_name = Path(path).name
        label = _clean_text(candidate.get("label"))
        text = f"{path} {label}".lower().replace("_", "-")
        if host == "github.com" and path.startswith("/cmsgov/price-transparency-guide"):
            continue
        if host.endswith("cms.gov") and "price-transparency" in path:
            continue
        azure_html_mrf_listing = (
            host.endswith(".blob.core.windows.net")
            and "mrf-output/" in path
            and file_name.endswith((".html", ".htm"))
            and "mrf" in text
        )
        autoindex_month_directory = (
            host in {"ehptransparency.org", "www.ehptransparency.org"}
            and path.endswith("/")
            and re.search(r"/[a-z]+-20\d{2}/?$", path.replace("_", "-"))
        )
        if not (
            path.endswith("/")
            or "." not in file_name
            or azure_html_mrf_listing
            or autoindex_month_directory
        ):
            continue
        if not path.endswith("/") and "." not in file_name:
            base_host = urlsplit(base_url).netloc.lower()
            host = parsed.netloc.lower()
            if host != base_host and "mrf" not in f"{host} {path} {text}":
                continue
        if not autoindex_month_directory and not any(
            token in text
            for token in (
                "/mrf/",
                "mrf",
                "machine-readable",
                "machine readable",
                "transparency",
                "in-network",
                "out-of-network",
                "allowed",
                "toc",
                "table-of-contents",
            )
        ):
            continue
        key = _canonical_or_none(url) or url
        if key == base_key or key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _json_mrf_directory_links_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    candidates = [
        str(item.get("url") or "")
        for item in _html_link_candidates(html_text, base_url=base_url)
    ]
    candidates.extend(_embedded_http_urls(html_text))
    for url in candidates:
        parsed = urlsplit(url)
        path = parsed.path.lower()
        label = Path(path).name.lower()
        text = f"{path} {label}".replace("_", "-")
        if not path.endswith(".json"):
            continue
        if not any(
            token in text
            for token in (
                "directory",
                "mrf",
                "machine-readable",
                "transparency",
                "toc",
                "tcr",
            )
        ):
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _embedded_mrf_urls(value: str, *, base_url: str | None = None) -> list[str]:
    urls = list(_embedded_http_urls(value))
    if base_url:
        relative_text = re.sub(
            r"https?://[^\s|)<>\"']+",
            " ",
            _decode_embedded_url_text(value),
        )
        for match in re.finditer(
            r"""(?<![A-Za-z0-9:/])(?P<url>/(?!/)[A-Za-z0-9._~!$&'()*+,;=:@%/-]+(?:\.json(?:\.gz)?|\.zip|\.7z|\.csv)(?:\?[A-Za-z0-9._~!$&'()*+,;=:@%/?-]+)?)""",
            relative_text,
        ):
            relative = match.group("url").strip()
            if not relative or relative.lower().startswith(("http://", "https://")):
                continue
            url = urljoin(base_url, relative)
            if url not in urls:
                urls.append(url)
    return urls


def _json_mrf_directory_targets_from_payload(
    source: dict[str, Any],
    payload: Any,
    *,
    directory_url: str,
    resolver_type: str,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    seen: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                visit(nested)
            return
        if isinstance(value, list):
            for nested in value:
                visit(nested)
            return
        if not isinstance(value, str):
            return
        for file_url in _embedded_mrf_urls(value, base_url=directory_url):
            label = Path(urlsplit(file_url).path).name
            target_kind: str | None = None
            target_file_type: str | None = None
            if _looks_html_mrf_toc_url(file_url, label):
                target_kind = "toc_json"
                target_file_type = "table-of-contents"
            elif _looks_html_mrf_body_reference(file_url, label):
                target_kind = "file_reference"
                target_file_type = _mrf_file_type_from_text(file_url, label)
            if not target_kind or not target_file_type:
                continue
            key = _canonical_or_none(file_url) or file_url
            if key in seen:
                continue
            seen.add(key)
            plan_info = (
                _plan_info_from_label(label) if target_kind == "file_reference" else []
            )
            metadata = {
                "resolver": resolver_type,
                "target_kind": target_kind,
                "target_file_type": target_file_type,
                "container_format": _container_format(file_url),
                "directory_url": directory_url,
                "plan_info": plan_info,
            }
            targets.append(
                CrawlTarget(
                    source=source,
                    url=file_url,
                    label=_mrf_file_plan_label(label) or label,
                    resolved_from_url=directory_url,
                    metadata={
                        key: value
                        for key, value in metadata.items()
                        if value not in (None, "", [])
                    },
                )
            )

    visit(payload)
    return targets


async def _resolve_json_mrf_directory_links(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    if urlsplit(url).path.lower().endswith(".json"):
        directory_urls = [url]
    else:
        html_text = await _fetch_text(
            url,
            max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
            session=session,
        )
        directory_urls = _json_mrf_directory_links_from_html(html_text, base_url=url)
    max_directories = _as_int(resolver.get("max_directories")) or 20
    targets: list[CrawlTarget] = []
    for directory_url in directory_urls[:max_directories]:
        payload = await _fetch_json(
            directory_url,
            max_bytes=int(resolver.get("directory_max_bytes") or 20 * 1024 * 1024),
            session=session,
        )
        targets.extend(
            _json_mrf_directory_targets_from_payload(
                source,
                payload,
                directory_url=directory_url,
                resolver_type="json_mrf_directory_links",
            )
        )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no JSON directory MRF targets found for {url}")
    return targets


def _humana_pct_payload_rows(payload: dict[str, Any]) -> list[Any]:
    for key in ("aaData", "data", "rows"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    return []


def _humana_pct_total_records(payload: dict[str, Any]) -> int | None:
    for key in ("iTotalDisplayRecords", "iTotalRecords", "recordsFiltered", "recordsTotal"):
        parsed = _as_int(payload.get(key))
        if parsed is not None:
            return parsed
    return None


def _humana_pct_row_text(row: Any) -> str:
    if isinstance(row, dict):
        return " ".join(str(value or "") for value in row.values())
    if isinstance(row, (list, tuple)):
        return " ".join(str(value or "") for value in row)
    return str(row or "")


def _humana_pct_file_name_from_row(row: Any) -> str | None:
    text = html.unescape(_humana_pct_row_text(row))
    for url in _embedded_http_urls(text):
        parsed = urlsplit(url)
        values = parse_qs(parsed.query).get("fileName") or parse_qs(parsed.query).get(
            "filename"
        )
        if values:
            return _clean_text(values[0])
        file_name = Path(parsed.path).name
        if file_name:
            return file_name
    for match in re.finditer(
        r"""(?P<name>[A-Za-z0-9._~%+ -]+(?:\.json(?:\.gz)?|\.zip|\.7z|\.csv(?:\.gz)?))""",
        text,
        flags=re.I,
    ):
        candidate = _clean_text(match.group("name"))
        if candidate:
            return candidate
    return None


def _humana_pct_download_url(base_url: str, resolver: dict[str, Any], file_name: str) -> str:
    download_path = str(
        resolver.get("download_path") or "/syntheticdata/Resource/DownloadTOCFile"
    )
    return urljoin(base_url, download_path) + "?" + urlencode({"fileName": file_name})


def _humana_pct_targets_from_payload(
    source: dict[str, Any],
    payload: dict[str, Any],
    *,
    api_url: str,
    resolver: dict[str, Any],
    resolver_type: str,
    include_body_files: bool = False,
    max_targets: int | None = None,
) -> list[CrawlTarget]:
    base_url = f"{urlsplit(api_url).scheme}://{urlsplit(api_url).netloc}/"
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for row in _humana_pct_payload_rows(payload):
        file_name = _humana_pct_file_name_from_row(row)
        if not file_name:
            continue
        file_type = _mrf_file_type_from_text(file_name, _humana_pct_row_text(row))
        target_kind: str | None = None
        if file_type == "table-of-contents":
            target_kind = "toc_json"
        elif include_body_files:
            file_type = _mrf_body_file_type_from_text(file_name, _humana_pct_row_text(row))
            target_kind = "file_reference" if file_type else None
        if not target_kind or not file_type:
            continue
        url = _humana_pct_download_url(base_url, resolver, file_name)
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        metadata = {
            "resolver": resolver_type,
            "target_kind": target_kind,
            "target_file_type": file_type,
            "container_format": _container_format(file_name),
            "humana_file_name": file_name,
            "humana_api_url": api_url,
            "plan_info": (
                _plan_info_from_label(file_name) if target_kind == "file_reference" else []
            ),
        }
        targets.append(
            CrawlTarget(
                source=source,
                url=url,
                label=_mrf_file_plan_label(file_name) or file_name,
                resolved_from_url=api_url,
                metadata={
                    key: value
                    for key, value in metadata.items()
                    if value not in (None, "", [])
                },
            )
        )
        if max_targets and len(targets) >= max_targets:
            break
    return targets


async def _resolve_humana_pct_file_list(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "humana_pct_file_list")
    api_url = str(
        resolver.get("api_url")
        or urljoin(url, "/syntheticdata/Resource/GetData")
    )
    file_types = [
        str(item).strip()
        for item in (resolver.get("file_types") or ["innetwork"])
        if str(item).strip()
    ]
    page_size = _as_int(resolver.get("page_size")) or 100
    max_pages = _as_int(resolver.get("max_pages")) or 25
    max_targets = _as_int(resolver.get("max_targets")) or 1000
    include_body_files = bool(resolver.get("include_body_files"))
    targets: list[CrawlTarget] = []
    for file_type in file_types:
        for page in range(max_pages):
            start = page * page_size
            page_url = api_url + "?" + urlencode(
                {
                    "fileType": file_type,
                    "iDisplayStart": start,
                    "iDisplayLength": page_size,
                    "sEcho": page + 1,
                }
            )
            payload = await _fetch_json(
                page_url,
                max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
                session=session,
            )
            rows = _humana_pct_payload_rows(payload)
            if not rows:
                break
            remaining = max_targets - len(targets)
            if remaining <= 0:
                break
            targets.extend(
                _humana_pct_targets_from_payload(
                    source,
                    payload,
                    api_url=page_url,
                    resolver=resolver,
                    resolver_type=resolver_type,
                    include_body_files=include_body_files,
                    max_targets=remaining,
                )
            )
            total = _humana_pct_total_records(payload)
            if total is not None and start + len(rows) >= total:
                break
            if len(rows) < page_size:
                break
        if len(targets) >= max_targets:
            break
    targets = _dedupe_crawl_targets_by_url(targets)
    if not targets:
        raise ValueError(f"no Humana PCT MRF targets found for {url}")
    return targets[:max_targets]


def _html_looks_cloudflare_challenge(html_text: str) -> bool:
    lowered = str(html_text or "").lower()
    return (
        "cf-mitigated" in lowered
        or "__cf_chl" in lowered
        or "challenge-platform" in lowered
        or "<title>just a moment" in lowered
    )


def _fchn_payor_detail_urls_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "")
        path = urlsplit(url).path.lower()
        if "/payorsearch/home/payordetail/" not in path:
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _fchn_targets_from_detail_html(
    source: dict[str, Any],
    html_text: str,
    *,
    detail_url: str,
    resolver_type: str,
) -> list[CrawlTarget]:
    targets = _crawl_targets_from_html_mrf_links(
        source,
        html_text,
        base_url=detail_url,
        resolver=resolver_type,
    )
    enriched: list[CrawlTarget] = []
    detail_id = next(
        (
            part
            for part in reversed([part for part in urlsplit(detail_url).path.split("/") if part])
            if part.isdigit()
        ),
        None,
    )
    for target in targets:
        metadata = {
            **dict(target.metadata or {}),
            "resolver": resolver_type,
            "fchn_detail_url": detail_url,
            "fchn_payor_detail_id": detail_id,
        }
        enriched.append(
            CrawlTarget(
                source=source,
                url=target.url,
                label=target.label,
                resolved_from_url=target.resolved_from_url or detail_url,
                metadata={key: value for key, value in metadata.items() if value},
            )
        )
    return enriched


async def _resolve_fchn_payor_search(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "fchn_payor_search")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    if _html_looks_cloudflare_challenge(html_text):
        raise ValueError(f"cloudflare_challenge blocks FCHN MRF discovery for {url}")
    targets = _fchn_targets_from_detail_html(
        source, html_text, detail_url=url, resolver_type=resolver_type
    )
    max_details = _as_int(resolver.get("max_details")) or 100
    max_targets = _as_int(resolver.get("max_targets")) or 1000
    detail_urls = _fchn_payor_detail_urls_from_html(html_text, base_url=url)
    detail_max_bytes = int(
        resolver.get("detail_max_bytes")
        or resolver.get("max_bytes")
        or 5 * 1024 * 1024
    )
    for detail_url in detail_urls[:max_details]:
        if len(targets) >= max_targets:
            break
        try:
            detail_html = await _fetch_text(
                detail_url,
                max_bytes=detail_max_bytes,
                session=session,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        if _html_looks_cloudflare_challenge(detail_html):
            continue
        targets.extend(
            _fchn_targets_from_detail_html(
                source,
                detail_html,
                detail_url=detail_url,
                resolver_type=resolver_type,
            )
        )
        targets = _dedupe_crawl_targets_by_url(targets)
    targets = _dedupe_crawl_targets_by_url(targets)
    if not targets:
        raise ValueError(f"no FCHN MRF links found for {url}")
    return targets[:max_targets]


def _payercompass_file_type(frame: dict[str, Any], label: str | None = None) -> str:
    file_type = _as_int(frame.get("fileType"))
    if file_type == 1:
        return "in-network"
    if file_type == 2:
        return "allowed-amounts"
    inferred = _mrf_file_type_from_text(label or "", label or "")
    return inferred or "in-network"


def _payercompass_download_url(
    base_url: str, resolver: dict[str, Any], file_id: str
) -> str:
    download_path = str(resolver.get("download_path") or "/api/File/Download")
    download_url = urljoin(base_url, download_path)
    return f"{download_url}?{urlencode({'Id': file_id})}"


def _payercompass_target_for_file(
    source: dict[str, Any],
    *,
    base_url: str,
    resolver: dict[str, Any],
    frame: dict[str, Any],
    file_item: dict[str, Any],
) -> CrawlTarget | None:
    file_id = str(file_item.get("id") or frame.get("id") or "").strip()
    if not file_id:
        return None
    file_name = str(file_item.get("name") or frame.get("name") or file_id).strip()
    file_type = _payercompass_file_type(frame, file_name)
    size_bytes = _parse_size_bytes(file_item.get("size"))
    metadata = {
        "resolver": "payercompass_mrf",
        "target_kind": "file_reference",
        "target_file_type": file_type,
        "container_format": _container_format(file_name),
        "payercompass_file_id": file_id,
        "payercompass_file_name": file_name,
        "payercompass_timeframe_id": frame.get("id"),
        "payercompass_timeframe_name": frame.get("name"),
        "size_bytes": size_bytes,
    }
    return CrawlTarget(
        source=source,
        url=_payercompass_download_url(base_url, resolver, file_id),
        label=_mrf_file_plan_label(file_name) or file_name,
        resolved_from_url=base_url,
        metadata={
            key: value for key, value in metadata.items() if value not in (None, "", [])
        },
    )


def _payercompass_as_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, dict):
        return [value]
    return []


def _payercompass_file_name_from_value(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urlsplit(text)
    if parsed.query:
        query = parse_qs(parsed.query)
        for key, values in query.items():
            if key.lower() not in {"id", "name", "file", "filename"}:
                continue
            for raw in values:
                candidate = unquote(str(raw or "")).strip()
                if candidate:
                    return candidate
    path_name = unquote(Path(parsed.path or text).name).strip()
    return path_name or text


def _payercompass_file_match_keys(*values: Any) -> set[str]:
    keys: set[str] = set()
    for value in values:
        for candidate in (value, _payercompass_file_name_from_value(value)):
            text = unquote(str(candidate or "")).strip()
            if not text:
                continue
            lower = text.lower()
            keys.add(lower)
            path_name = unquote(Path(urlsplit(text).path or text).name).strip().lower()
            if path_name:
                keys.add(path_name)
            for suffix in (".zip", ".gz"):
                if lower.endswith(suffix):
                    keys.add(lower[: -len(suffix)])
                if path_name.endswith(suffix):
                    keys.add(path_name[: -len(suffix)])
            for uuid_match in re.findall(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                lower,
                flags=re.I,
            ):
                keys.add(uuid_match.lower())
    return {key for key in keys if key}


def _payercompass_normalized_plan_info(plan: dict[str, Any]) -> dict[str, Any] | None:
    plan_id = _clean_text(plan.get("plan_id") or plan.get("planId"))
    plan_name = _clean_text(plan.get("plan_name") or plan.get("planName"))
    sponsor_name = _clean_text(
        plan.get("plan_sponsor_name")
        or plan.get("plan_sponser_name")
        or plan.get("sponsor_name")
        or plan.get("company_name")
        or plan.get("planSponsorName")
    )
    issuer_name = _clean_text(plan.get("issuer_name") or plan.get("issuerName"))
    if not any((plan_id, plan_name, sponsor_name, issuer_name)):
        return None
    normalized = {
        "plan_id": plan_id or None,
        "plan_id_type": _clean_text(plan.get("plan_id_type") or plan.get("planIdType"))
        or None,
        "plan_market_type": _clean_text(
            plan.get("plan_market_type")
            or plan.get("planMarketType")
            or plan.get("market_type")
        )
        or "group",
        "plan_name": plan_name or None,
    }
    if sponsor_name:
        normalized["plan_sponsor_name"] = sponsor_name
    if issuer_name:
        normalized["issuer_name"] = issuer_name
    return normalized


def _payercompass_plan_key(plan: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(plan.get("plan_id") or ""),
        str(plan.get("plan_id_type") or ""),
        str(plan.get("plan_market_type") or ""),
        str(plan.get("plan_name") or ""),
        str(plan.get("plan_sponsor_name") or plan.get("issuer_name") or ""),
    )


def _payercompass_add_plan_info(
    index: dict[str, list[dict[str, Any]]],
    key: str,
    plans: list[dict[str, Any]],
) -> None:
    if not key:
        return
    existing = index.setdefault(key, [])
    seen = {_payercompass_plan_key(plan) for plan in existing}
    for plan in plans:
        plan_key = _payercompass_plan_key(plan)
        if plan_key in seen:
            continue
        existing.append(plan)
        seen.add(plan_key)


def _payercompass_plan_info_by_file_key(
    toc: dict[str, Any],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, str | None]]:
    plan_info_by_key: dict[str, list[dict[str, Any]]] = {}
    toc_metadata = {
        "reporting_entity_name": _clean_text(toc.get("reporting_entity_name")),
        "reporting_entity_type": _clean_text(toc.get("reporting_entity_type")),
    }
    for structure in _payercompass_as_list(toc.get("reporting_structure")):
        if not isinstance(structure, dict):
            continue
        plans = [
            normalized
            for raw_plan in _payercompass_as_list(structure.get("reporting_plans"))
            if isinstance(raw_plan, dict)
            for normalized in [_payercompass_normalized_plan_info(raw_plan)]
            if normalized
        ]
        if not plans:
            continue
        for file_entry in _payercompass_as_list(structure.get("in_network_files")):
            if not isinstance(file_entry, dict):
                continue
            keys = _payercompass_file_match_keys(
                file_entry.get("location"), file_entry.get("description")
            )
            for key in keys:
                _payercompass_add_plan_info(plan_info_by_key, key, plans)
    return plan_info_by_key, toc_metadata


def _payercompass_is_index_target(target: CrawlTarget) -> bool:
    metadata = target.metadata or {}
    file_name = str(metadata.get("payercompass_file_name") or target.label or "")
    normalized = file_name.lower()
    return (
        "index" in normalized
        and ".json" in normalized
        and str(metadata.get("container_format") or "").lower() == "zip"
    )


def _payercompass_target_match_keys(target: CrawlTarget) -> set[str]:
    metadata = target.metadata or {}
    return _payercompass_file_match_keys(
        target.url,
        target.label,
        metadata.get("payercompass_file_id"),
        metadata.get("payercompass_file_name"),
    )


async def _enrich_payercompass_targets_with_index_plan_info(
    targets: list[CrawlTarget],
    *,
    resolver: dict[str, Any],
    max_bytes: int,
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    index_fetch_max_bytes = (
        _as_int(resolver.get("index_max_bytes"))
        or _as_int(resolver.get("max_index_bytes"))
        or max_bytes
    )
    plan_info_by_key: dict[str, list[dict[str, Any]]] = {}
    toc_metadata: dict[str, str | None] = {}
    for target in targets:
        if not _payercompass_is_index_target(target):
            continue
        size_bytes = _parse_size_bytes((target.metadata or {}).get("size_bytes"))
        if size_bytes and size_bytes > index_fetch_max_bytes:
            continue
        try:
            toc_values = await _fetch_zip_json_values(
                target.url,
                max_bytes=index_fetch_max_bytes,
                session=session,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logging.getLogger(__name__).debug(
                "failed to fetch PayerCompass index %s: %s", target.url, exc
            )
            continue
        for _member_name, toc in toc_values:
            if not isinstance(toc, dict):
                continue
            member_index, member_metadata = _payercompass_plan_info_by_file_key(toc)
            for key, plans in member_index.items():
                _payercompass_add_plan_info(plan_info_by_key, key, plans)
            for metadata_key, metadata_value in member_metadata.items():
                if metadata_value and not toc_metadata.get(metadata_key):
                    toc_metadata[metadata_key] = metadata_value
    if not plan_info_by_key:
        return targets
    for target in targets:
        metadata = target.metadata or {}
        if metadata.get("target_file_type") != "in-network":
            continue
        if metadata.get("plan_info"):
            continue
        plan_info: list[dict[str, Any]] = []
        seen_plan_keys: set[tuple[str, str, str, str, str]] = set()
        for key in _payercompass_target_match_keys(target):
            for plan in plan_info_by_key.get(key) or []:
                plan_key = _payercompass_plan_key(plan)
                if plan_key in seen_plan_keys:
                    continue
                plan_info.append(plan)
                seen_plan_keys.add(plan_key)
        if not plan_info:
            continue
        metadata["plan_info"] = plan_info
        metadata["payercompass_plan_info_source"] = "index_toc"
        for metadata_key, metadata_value in toc_metadata.items():
            if metadata_value and not metadata.get(metadata_key):
                metadata[metadata_key] = metadata_value
    return targets


def _payercompass_targets_from_structure(
    source: dict[str, Any],
    *,
    base_url: str,
    resolver: dict[str, Any],
    structure: dict[str, Any],
    file_lists: dict[str, list[dict[str, Any]]],
) -> list[CrawlTarget]:
    mrf_config = structure.get("mrfConfig") if isinstance(structure, dict) else None
    frames = (
        (mrf_config or {}).get("timeFrames") if isinstance(mrf_config, dict) else []
    )
    if not isinstance(frames, list):
        return []
    targets: list[CrawlTarget] = []
    max_timeframes = _as_int(resolver.get("max_timeframes")) or len(frames)
    for frame in frames[:max_timeframes]:
        if not isinstance(frame, dict):
            continue
        timeframe_id = str(frame.get("id") or "").strip()
        if not timeframe_id:
            continue
        listing = file_lists.get(timeframe_id) or []
        if not listing and (_as_int(frame.get("fileCount")) or 0) == 1:
            listing = [
                {"id": timeframe_id, "name": str(frame.get("name") or timeframe_id)}
            ]
        for file_item in listing:
            if not isinstance(file_item, dict):
                continue
            target = _payercompass_target_for_file(
                source,
                base_url=base_url,
                resolver=resolver,
                frame=frame,
                file_item=file_item,
            )
            if target:
                targets.append(target)
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    return targets


async def _resolve_payercompass_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    structure_path = str(resolver.get("structure_path") or "/api/Home/GetStructureInfo")
    file_list_path = str(resolver.get("file_list_path") or "/api/File/List")
    structure_url = urljoin(url, structure_path)
    file_list_url = urljoin(url, file_list_path)
    max_bytes = int(resolver.get("max_bytes") or 10 * 1024 * 1024)
    structure = await _post_json(
        structure_url, {}, max_bytes=max_bytes, session=session
    )
    mrf_config = structure.get("mrfConfig") if isinstance(structure, dict) else None
    frames = (
        (mrf_config or {}).get("timeFrames") if isinstance(mrf_config, dict) else []
    )
    if not isinstance(frames, list):
        frames = []
    max_timeframes = _as_int(resolver.get("max_timeframes")) or len(frames)
    file_lists: dict[str, list[dict[str, Any]]] = {}
    for frame in frames[:max_timeframes]:
        if not isinstance(frame, dict):
            continue
        timeframe_id = str(frame.get("id") or "").strip()
        if not timeframe_id:
            continue
        payload = await _post_json_value(
            file_list_url,
            {"timeFrameId": timeframe_id},
            max_bytes=max_bytes,
            session=session,
        )
        file_lists[timeframe_id] = payload if isinstance(payload, list) else []
    targets = _payercompass_targets_from_structure(
        source,
        base_url=url,
        resolver=resolver,
        structure=structure,
        file_lists=file_lists,
    )
    targets = await _enrich_payercompass_targets_with_index_plan_info(
        targets,
        resolver=resolver,
        max_bytes=max_bytes,
        session=session,
    )
    if not targets:
        raise ValueError(f"no PayerCompass MRF files found for {url}")
    return targets


def _webtpa_api_base_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"unsupported WebTPA MRF URL: {url}")
    return f"{parsed.scheme}://{parsed.netloc}/"


def _webtpa_plan_id(plan: dict[str, Any]) -> str | None:
    for key in ("mrfBenefitplanId", "benefitplanId", "planId", "id"):
        value = plan.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _webtpa_plan_name(plan: dict[str, Any]) -> str | None:
    for key in ("benefitplanNm", "benefitPlanName", "planName", "name"):
        value = _clean_text(plan.get(key))
        if value:
            return value
    return None


def _webtpa_file_id(record: dict[str, Any], file_type: str) -> str | None:
    keys = (
        ("mrfInNetworkRatesId", "inNetworkRatesId", "id")
        if file_type == "in-network"
        else ("mrfAllowedAmountsId", "allowedAmountsId", "id")
    )
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _webtpa_record_location(record: dict[str, Any]) -> str | None:
    location = str(record.get("location") or record.get("url") or "").strip()
    return location if location.startswith(("http://", "https://")) else None


def _webtpa_record_target(
    source: dict[str, Any],
    *,
    plan: dict[str, Any],
    record: dict[str, Any],
    file_type: str,
    file_url: str,
    resolved_from_url: str,
) -> CrawlTarget | None:
    plan_id = _webtpa_plan_id(plan)
    plan_name = _webtpa_plan_name(plan)
    file_id = _webtpa_file_id(record, file_type)
    file_name = _clean_text(record.get("fileName") or record.get("name"))
    label = " - ".join(part for part in (plan_name, file_name) if part)
    plan_info: list[dict[str, Any]] = []
    if plan_id or plan_name:
        plan_info.append(
            {
                "plan_id": plan_id,
                "plan_id_type": "webtpa_mrf_benefitplan_id" if plan_id else None,
                "plan_name": plan_name,
            }
        )
    return CrawlTarget(
        source=source,
        url=file_url,
        label=label or file_name or plan_name or Path(urlsplit(file_url).path).name,
        resolved_from_url=resolved_from_url,
        metadata={
            "resolver": "webtpa_mrf_api",
            "target_kind": "file_reference",
            "target_file_type": file_type,
            "container_format": _container_format(file_url),
            "plan_info": plan_info,
            "webtpa_plan_id": plan_id,
            "webtpa_file_id": file_id,
            "webtpa_file_name": file_name,
            "webtpa_record_type": record.get("type"),
        },
    )


async def _resolve_webtpa_mrf_api(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    base_url = _webtpa_api_base_url(url)
    plans_url = urljoin(base_url, str(resolver.get("plans_path") or ""))
    plans = await _fetch_json_value(
        plans_url,
        max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    if not isinstance(plans, list):
        raise ValueError(f"WebTPA plans endpoint did not return a list: {plans_url}")
    max_plans = _as_int(resolver.get("max_plans")) or len(plans)
    targets: list[CrawlTarget] = []
    endpoint_specs = (
        (
            "in-network",
            str(
                resolver.get("in_network_path_template")
                or "/machinereadablefile/plans/{plan_id}/in-network-rates"
            ),
            str(
                resolver.get("in_network_location_path_template")
                or "/machinereadablefile/in-network-rates/{file_id}/location"
            ),
        ),
        (
            "allowed-amounts",
            str(
                resolver.get("allowed_amounts_path_template")
                or "/machinereadablefile/plans/{plan_id}/allowed-amounts"
            ),
            "",
        ),
    )
    for plan in [item for item in plans if isinstance(item, dict)][:max_plans]:
        plan_id = _webtpa_plan_id(plan)
        if not plan_id:
            continue
        for file_type, list_template, location_template in endpoint_specs:
            list_url = urljoin(base_url, list_template.format(plan_id=quote(plan_id)))
            records = await _fetch_json_value(
                list_url,
                max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
                session=session,
            )
            if not isinstance(records, list):
                continue
            for record in [item for item in records if isinstance(item, dict)]:
                file_url = _webtpa_record_location(record)
                file_id = _webtpa_file_id(record, file_type)
                resolved_from_url = list_url
                if not file_url and file_type == "in-network" and file_id:
                    location_url = urljoin(
                        base_url, location_template.format(file_id=quote(file_id))
                    )
                    try:
                        location_payload = await _fetch_json(
                            location_url,
                            max_bytes=1024 * 1024,
                            session=session,
                        )
                    except Exception:  # pylint: disable=broad-exception-caught
                        location_payload = {}
                    if isinstance(location_payload, dict):
                        file_url = _webtpa_record_location(location_payload)
                        resolved_from_url = location_url
                if not file_url:
                    continue
                target = _webtpa_record_target(
                    source,
                    plan=plan,
                    record=record,
                    file_type=file_type,
                    file_url=file_url,
                    resolved_from_url=resolved_from_url,
                )
                if target:
                    targets.append(target)
                max_targets = _as_int(resolver.get("max_targets"))
                if max_targets and len(targets) >= max_targets:
                    return _dedupe_crawl_targets_by_url(targets)[:max_targets]
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no WebTPA MRF file targets found for {url}")
    return targets


def _cmstic_brand_from_url(url: str) -> str | None:
    parsed = urlsplit(str(url or ""))
    params = parse_qs(parsed.query)
    for key in ("brand", "brandCode"):
        values = params.get(key) or []
        for value in values:
            value = str(value or "").strip()
            if value:
                return value
    return None


def _cmstic_brands_from_url(url: str, resolver: dict[str, Any]) -> list[str]:
    parsed = urlsplit(str(url or ""))
    host = parsed.netloc.lower()
    brand = _cmstic_brand_from_url(url)
    if brand:
        return [brand]
    configured = resolver.get("default_brands_by_host")
    configured = configured if isinstance(configured, dict) else {}
    brands = configured.get(host) or configured.get(host.lstrip("www."))
    if isinstance(brands, str):
        brands = [brands]
    if isinstance(brands, list):
        deduped: list[str] = []
        seen: set[str] = set()
        for item in brands:
            item = str(item or "").strip()
            if item and item not in seen:
                seen.add(item)
                deduped.append(item)
        if deduped:
            return deduped
    if "amerihealthnj.com" in host:
        return ["ahnj", "ahnjhmo"]
    if "amerihealth.com" in host:
        return ["ahpa"]
    if "ibx.com" in host:
        return ["qcc"]
    return []


def _cmstic_api_url(url: str, brand: str | None = None) -> str:
    parsed = urlsplit(str(url or ""))
    host = parsed.netloc.lower()
    brand = str(brand or _cmstic_brand_from_url(url) or "").strip()
    if not brand:
        raise ValueError(f"CMSTIC URL is missing a brand parameter: {url}")
    if brand.lower() in {"ahnj", "ahnjhmo"}:
        base = "https://www.amerihealthnj.com/cmsticsvc/api/fi"
    elif brand.lower() == "ahpa":
        base = "https://www.amerihealth.com/cmsticsvc/api/fi"
    elif brand.lower() in {"qcc", "khpe", "bc", "iac"}:
        base = "https://www.ibx.com/cmsticsvc/api/fi"
    else:
        raise ValueError(f"unsupported CMSTIC host: {url}")
    return f"{base}?{urlencode({'brand': brand})}"


def _looks_cmstic_keyed_toc_url(url: str | None) -> bool:
    parsed = urlsplit(str(url or "").strip())
    host = parsed.netloc.lower()
    if host not in {"www.ibx.com", "ibx.com"}:
        return False
    if not re.match(r"^/transparency-in-coverage/[A-Za-z0-9_-]+/?$", parsed.path):
        return False
    return any(str(value or "").strip() for value in parse_qs(parsed.query).get("key", ()))


def _cmstic_keyed_toc_crawl_target(
    source: dict[str, Any],
    url: str,
    *,
    final_url: str,
    resolver: dict[str, Any],
    resolver_type: str,
) -> CrawlTarget | None:
    if not _looks_cmstic_keyed_toc_url(url):
        return None
    if not _looks_direct_toc_url(final_url):
        return None
    source_id = Path(urlsplit(str(url or "")).path.rstrip("/")).name
    toc_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes"))
    metadata = {
        "resolver": resolver_type,
        "target_kind": "toc_json",
        "target_file_type": "table-of-contents",
        "cmstic_source_id": source_id,
        "file_name": f"{source_id}_index.json" if source_id else None,
        "payer_name": source.get("display_name"),
    }
    if toc_max_bytes:
        metadata["target_max_bytes"] = toc_max_bytes
    return CrawlTarget(
        source=source,
        url=final_url,
        label=str(source.get("display_name") or source_id or ""),
        resolved_from_url=url,
        metadata=metadata,
    )


async def _resolve_cmstic_keyed_toc_redirect(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    if not _looks_cmstic_keyed_toc_url(url):
        raise ValueError(f"unsupported CMSTIC keyed TOC URL: {url}")
    await _assert_fetch_url_allowed(url)
    async with session.head(
        url, allow_redirects=True, **_request_ssl_kwargs(url)
    ) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        if resp.status >= 400:
            raise ValueError(f"CMSTIC keyed TOC redirect returned HTTP {resp.status}")
        final_url = str(resp.url)
        metadata = {
            "etag": resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "content_length": resp.headers.get("Content-Length"),
            "content_type": resp.headers.get("Content-Type"),
        }
    target = _cmstic_keyed_toc_crawl_target(
        source,
        url,
        final_url=final_url,
        resolver=resolver,
        resolver_type=str(resolver.get("type") or "cmstic_keyed_toc_redirect"),
    )
    if not target:
        raise ValueError(f"CMSTIC keyed TOC redirect did not resolve to a TOC: {url}")
    return [
        CrawlTarget(
            source=target.source,
            url=target.url,
            label=target.label,
            resolved_from_url=target.resolved_from_url,
            metadata={
                **target.metadata,
                **{key: value for key, value in metadata.items() if value},
            },
        )
    ]


def _cmstic_target_from_payload(
    source: dict[str, Any],
    payload: dict[str, Any],
    *,
    api_url: str,
    resolver_type: str,
) -> CrawlTarget | None:
    file_url = str(payload.get("url") or payload.get("href") or "").strip()
    if not file_url.startswith(("http://", "https://")):
        return None
    label = _clean_text(payload.get("name")) or Path(urlsplit(file_url).path).name
    if not _looks_html_mrf_toc_url(file_url, label):
        return None
    return CrawlTarget(
        source=source,
        url=file_url,
        label=label,
        resolved_from_url=api_url,
        metadata={
            "resolver": resolver_type,
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "container_format": _container_format(file_url),
            "cmstic_name": payload.get("name"),
            "cmstic_brand": _cmstic_brand_from_url(api_url),
        },
    )


async def _resolve_cmstic_file_info(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    for brand in _cmstic_brands_from_url(url, resolver):
        api_url = _cmstic_api_url(url, brand)
        payload = await _fetch_json(
            api_url,
            max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
            session=session,
        )
        target = _cmstic_target_from_payload(
            source, payload, api_url=api_url, resolver_type="cmstic_file_info"
        )
        if target:
            targets.append(target)
    targets = _dedupe_crawl_targets_by_url(targets)
    if not targets:
        raise ValueError(f"CMSTIC file-info response did not include a TOC: {url}")
    return targets


def _github_repo_parts_from_url(
    url: str | None,
) -> tuple[str, str, str | None, str | None]:
    parsed = urlsplit(str(url or "").strip())
    if parsed.netloc.lower() != "github.com":
        raise ValueError(f"unsupported GitHub MRF URL: {url}")
    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        raise ValueError(f"GitHub MRF URL must include owner and repo: {url}")
    owner, repo = parts[0], parts[1]
    branch: str | None = None
    path_prefix: str | None = None
    if len(parts) >= 4 and parts[2] in {"tree", "blob"}:
        branch = parts[3]
        if len(parts) > 4:
            path_prefix = "/".join(parts[4:]).strip("/") or None
    return owner, repo, branch, path_prefix


def _github_raw_url(owner: str, repo: str, branch: str, path: str) -> str:
    escaped_path = quote(path.strip("/"), safe="/")
    return f"https://raw.githubusercontent.com/{owner}/{repo}/{quote(branch, safe='')}/{escaped_path}"


def _mrf_file_plan_label(path: str) -> str | None:
    name = Path(path).name
    name = re.sub(r"\.(?:json(?:\.gz)?|zip|7z|csv)$", "", name, flags=re.I)
    name = re.sub(r"^\d{4}-\d{2}-\d{2}[_-]", "", name)
    name = re.sub(
        r"[_-](?:in[_-]?network|in[_-]?network[_-]?rates|allowed[_-]?amounts?|rates?)$",
        "",
        name,
        flags=re.I,
    )
    name = re.sub(
        r"^(?:mrf|tic|toc|index|table[_-]?of[_-]?contents|[a-z0-9]+)[_-]+",
        "",
        name,
        flags=re.I,
    )
    return _slug_label(name)


def _github_mrf_plan_label(path: str) -> str | None:
    return _mrf_file_plan_label(path)


def _github_tree_mrf_target(
    source: dict[str, Any],
    *,
    owner: str,
    repo: str,
    branch: str,
    item: dict[str, Any],
    tree_url: str,
    resolver_type: str,
) -> CrawlTarget | None:
    if str(item.get("type") or "").lower() != "blob":
        return None
    path = str(item.get("path") or "").strip()
    if not path:
        return None
    raw_url = _github_raw_url(owner, repo, branch, path)
    label = _github_mrf_plan_label(path) or Path(path).name
    file_type: str | None = None
    target_kind: str | None = None
    if _looks_html_mrf_toc_url(raw_url, label):
        file_type = "table-of-contents"
        target_kind = "toc_json"
    elif _looks_direct_mrf_body_url(raw_url):
        file_type = _mrf_file_type_from_text(raw_url, label)
        target_kind = "file_reference"
    if not file_type or not target_kind:
        return None
    plan_info = _plan_info_from_label(label)
    return CrawlTarget(
        source=source,
        url=raw_url,
        label=label,
        resolved_from_url=tree_url,
        metadata={
            "resolver": resolver_type,
            "target_kind": target_kind,
            "target_file_type": file_type,
            "container_format": _container_format(raw_url),
            "github_owner": owner,
            "github_repo": repo,
            "github_branch": branch,
            "github_path": path,
            "github_sha": item.get("sha"),
            "blob_size": item.get("size"),
            "plan_info": plan_info,
        },
    )


async def _resolve_github_repo_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "github_repo_mrf_tree")
    owner, repo, branch, path_prefix = _github_repo_parts_from_url(url)
    repo_url = f"https://api.github.com/repos/{owner}/{repo}"
    if not branch:
        repo_payload = await _fetch_json(
            repo_url,
            max_bytes=int(resolver.get("repo_max_bytes") or 1024 * 1024),
            session=session,
        )
        branch = str(repo_payload.get("default_branch") or "main").strip() or "main"
    tree_url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{quote(branch, safe='')}?recursive=1"
    tree_payload = await _fetch_json(
        tree_url,
        max_bytes=int(resolver.get("tree_max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    tree = tree_payload.get("tree")
    if not isinstance(tree, list):
        raise ValueError(
            f"GitHub MRF tree response did not include file list for {owner}/{repo}"
        )
    targets: list[CrawlTarget] = []
    prefix = (path_prefix or "").strip("/")
    for item in tree:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "")
        if prefix and path != prefix and not path.startswith(f"{prefix}/"):
            continue
        target = _github_tree_mrf_target(
            source,
            owner=owner,
            repo=repo,
            branch=branch,
            item=item,
            tree_url=tree_url,
            resolver_type=resolver_type,
        )
        if target:
            targets.append(target)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no GitHub MRF files found for {owner}/{repo}")
    return targets


def _strip_html_tags(value: str) -> str:
    return _clean_text(re.sub(r"<[^>]+>", " ", value or ""))


_HTML_ATTR_MRF_REFERENCE_RE = re.compile(
    r"^data-(?:key|[a-z0-9_-]*(?:url|href|file|path|lookup|key)[a-z0-9_-]*)$",
    flags=re.I,
)


class _MRFHtmlLinkParser(HTMLParser):
    def __init__(self, html_text: str):
        super().__init__(convert_charrefs=True)
        self.html_text = html_text or ""
        self.line_offsets = self._compute_line_offsets(self.html_text)
        self.candidates: list[dict[str, Any]] = []
        self._anchor_stack: list[dict[str, Any]] = []
        self.text_parts: list[str] = []

    @staticmethod
    def _compute_line_offsets(text: str) -> list[int]:
        offsets: list[int] = []
        offset = 0
        for line in text.splitlines(keepends=True):
            offsets.append(offset)
            offset += len(line)
        offsets.append(offset)
        return offsets

    def _offset(self) -> int:
        line, column = self.getpos()
        if line <= 0:
            return 0
        if line > len(self.line_offsets):
            return len(self.html_text)
        return min(self.line_offsets[line - 1] + column, len(self.html_text))

    def _tag_end(self, start: int) -> int:
        end = self.html_text.find(">", start)
        if end < 0:
            return start
        return end + 1

    @staticmethod
    def _is_reference_attr(attr: str) -> bool:
        attr = attr.lower()
        return attr == "src" or bool(_HTML_ATTR_MRF_REFERENCE_RE.match(attr))

    def _append_attr_candidates(
        self,
        attrs: list[tuple[str, str | None]],
        *,
        label: str,
        html_start: int,
        html_end: int,
        include_href: bool,
    ) -> None:
        for attr, value in attrs:
            attr = (attr or "").lower()
            if value is None:
                continue
            if attr == "href":
                if not include_href:
                    continue
            elif not self._is_reference_attr(attr):
                continue
            self.candidates.append(
                {
                    "attr": attr,
                    "value": value,
                    "label": label,
                    "html_start": html_start,
                    "html_end": html_end,
                }
            )

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        start = self._offset()
        end = self._tag_end(start)
        if tag == "a":
            self._anchor_stack.append(
                {"attrs": attrs, "label_parts": [], "html_start": start}
            )
            return
        self._append_attr_candidates(
            attrs, label="", html_start=start, html_end=end, include_href=False
        )

    def handle_startendtag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        start = self._offset()
        end = self._tag_end(start)
        self._append_attr_candidates(
            attrs,
            label="",
            html_start=start,
            html_end=end,
            include_href=tag.lower() == "a",
        )

    def handle_data(self, data: str) -> None:
        if data:
            self.text_parts.append(data)
        if self._anchor_stack:
            self._anchor_stack[-1]["label_parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._anchor_stack:
            return
        start = self._offset()
        end = self._tag_end(start)
        anchor = self._anchor_stack.pop()
        label = _clean_text(" ".join(anchor.get("label_parts") or []))
        self._append_attr_candidates(
            anchor.get("attrs") or [],
            label=label,
            html_start=int(anchor.get("html_start") or 0),
            html_end=end,
            include_href=True,
        )


def _html_link_candidates(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    parser = _MRFHtmlLinkParser(html_text or "")
    embedded_source = html_text or ""
    try:
        parser.feed(html_text or "")
        parser.close()
        candidates.extend(parser.candidates)
        embedded_source = "\n".join(parser.text_parts)
    except Exception:
        LOGGER.debug("failed to parse HTML links with HTMLParser", exc_info=True)
    for url in _embedded_mrf_urls(embedded_source, base_url=base_url):
        candidates.append(
            {"attr": "text", "value": url, "label": Path(urlsplit(url).path).name}
        )
    if embedded_source != (html_text or ""):
        for url in _embedded_mrf_urls(html_text or "", base_url=base_url):
            label = Path(urlsplit(url).path).name
            if not (
                _looks_html_mrf_toc_url(url, label)
                or _looks_html_mrf_body_reference(url, label)
            ):
                continue
            candidates.append({"attr": "text", "value": url, "label": label})
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        raw = html.unescape(str(item.get("value") or "")).strip()
        if not raw or raw.lower().startswith(("javascript:", "mailto:", "tel:", "#")):
            continue
        url = urljoin(base_url, raw)
        key = (str(item.get("attr") or "").lower(), _canonical_or_none(url) or url)
        if key in seen:
            continue
        seen.add(key)
        label = item.get("label") or Path(urlsplit(url).path).name
        if key[0] != "href" and _clean_text(label).lower() in {
            "download",
            "view",
            "open",
            "file",
            "here",
        }:
            label = Path(urlsplit(url).path).name
        row = {
            "attr": key[0],
            "url": url,
            "label": label,
        }
        if isinstance(item.get("html_start"), int):
            row["html_start"] = item.get("html_start")
        if isinstance(item.get("html_end"), int):
            row["html_end"] = item.get("html_end")
        normalized.append(row)
    return normalized


class _MidlandsChoiceMrfParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.rows: list[list[dict[str, str]]] = []
        self._current_row: list[dict[str, str]] | None = None
        self._current_cell: dict[str, Any] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "tr":
            self._current_row = []
            return
        if tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = {"text_parts": [], "href": ""}
            return
        if tag == "a" and self._current_cell is not None:
            attrs_by_name = {name.lower(): value for name, value in attrs if name}
            href = str(attrs_by_name.get("href") or "").strip()
            if href:
                self._current_cell["href"] = href

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None and data:
            self._current_cell["text_parts"].append(data)

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if tag in {"td", "th"} and self._current_row is not None and self._current_cell:
            self._current_row.append(
                {
                    "text": _clean_text(" ".join(self._current_cell["text_parts"])),
                    "href": str(self._current_cell.get("href") or "").strip(),
                }
            )
            self._current_cell = None
            return
        if tag == "tr" and self._current_row is not None:
            if self._current_row:
                self.rows.append(self._current_row)
            self._current_row = None
            self._current_cell = None


def _parse_midlandschoice_mrf_rows(
    html_text: str, *, base_url: str
) -> list[dict[str, str]]:
    parser = _MidlandsChoiceMrfParser()
    parser.feed(html_text or "")
    parser.close()
    rows: list[dict[str, str]] = []
    for cells in parser.rows:
        if len(cells) < 5:
            continue
        network_code = _clean_text(cells[0].get("text"))
        network_name = _clean_text(cells[1].get("text"))
        file_name = _clean_text(cells[2].get("text"))
        file_type = _clean_text(cells[3].get("text"))
        href = _clean_text(cells[4].get("href"))
        if not href:
            href = next((_clean_text(cell.get("href")) for cell in cells if cell.get("href")), "")
        if not href or network_code.lower() == "network":
            continue
        file_url = urljoin(base_url, href)
        target_file_type = (
            _mrf_body_file_type_from_text(file_url, file_type or file_name)
            or _metadata_text_file_type(file_type)
        )
        if target_file_type not in {"in-network", "allowed-amounts"}:
            continue
        rows.append(
            {
                "network_code": network_code,
                "network_name": network_name,
                "file_name": file_name,
                "file_type": file_type,
                "target_file_type": target_file_type,
                "url": file_url,
            }
        )
    return rows


async def _resolve_midlandschoice_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    targets: list[CrawlTarget] = []
    resolver_type = str(resolver.get("type") or "midlandschoice_mrf")
    for row in _parse_midlandschoice_mrf_rows(html_text, base_url=url):
        label = row.get("network_name") or row.get("file_name") or row["network_code"]
        targets.append(
            CrawlTarget(
                source=source,
                url=row["url"],
                label=label,
                resolved_from_url=url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": "file_reference",
                    "target_file_type": row["target_file_type"],
                    "container_format": _container_format(row["url"]),
                    "network_code": row["network_code"],
                    "network_name": row.get("network_name"),
                    "file_name": row.get("file_name"),
                    "midlandschoice_file_type": row.get("file_type"),
                    "reporting_entity_name": source.get("display_name")
                    or "Midlands Choice",
                    "reporting_entity_type": "third_party_administrator",
                    "plan_info": _plan_info_from_label(
                        label,
                        plan_id=row["network_code"],
                        plan_id_type="network_code",
                    ),
                },
            )
        )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Midlands Choice MRF download rows found for {url}")
    return targets


def _embedded_http_urls(value: str | None) -> list[str]:
    """Extract bare URLs from HTML, including JSON/JS escaped URL strings."""

    text = _decode_embedded_url_text(value)
    urls: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"https?://[^\s\"'<>]+", text, flags=re.I):
        url = match.group(0).rstrip("\\,.;)")
        while url.endswith(("]", "}")) and (
            url.count("[") < url.count("]") or url.count("{") < url.count("}")
        ):
            url = url[:-1]
        if not url:
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _decode_embedded_url_text(value: str | None) -> str:
    text = html.unescape(str(value or ""))
    replacements = {
        "\\/": "/",
        "\\u002f": "/",
        "\\u002F": "/",
        "\\x2f": "/",
        "\\x2F": "/",
        "\\u003a": ":",
        "\\u003A": ":",
        "\\x3a": ":",
        "\\x3A": ":",
        "\\u0026": "&",
        "\\u0026amp;": "&",
        "\\u003c": "<",
        "\\u003C": "<",
        "\\x3c": "<",
        "\\x3C": "<",
        "\\u003e": ">",
        "\\u003E": ">",
        "\\x3e": ">",
        "\\x3E": ">",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _looks_html_mrf_toc_url(url: str | None, label: str | None = None) -> bool:
    parsed = urlsplit(str(url or ""))
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    text = f"{path} {query} {label or ''}".lower().replace("_", "-")
    if _looks_non_tic_mrf_reference(url, label):
        return False
    if any(
        token in text for token in ("formulary", "provider-data", "provider ")
    ) and not any(
        token in text
        for token in (
            "allowed",
            "in-network",
            "in network",
            "rate",
            "rates",
            "toc",
            "table-of-contents",
            "table of contents",
        )
    ):
        return False
    if host == "d3oz7y1cwsecds.cloudfront.net" and path == "/member-prod/bcbsal":
        return True
    if path.endswith((".json", ".json.gz")):
        file_name = Path(path).name.replace("_", "-")
        if re.search(r"(^|-)index(?:-[a-z0-9]+)?\.json(?:\.gz)?$", file_name) and any(
            token in text
            for token in (
                "mrf",
                "machine-readable",
                "price-transparency",
                "transparency",
                "table-of-content",
                "table of content",
                "table-of-contents",
                "table of contents",
            )
        ):
            return True
        return any(
            token in text
            for token in (
                "index.json",
                "index.json.gz",
                "toc",
                "table-of-content",
                "table of content",
                "table-of-contents",
                "table of contents",
            )
        )
    if "sapphiremrfhub.com" in host and "/tocs/" in path:
        return True
    if "name=table-of-contents" in query and "ext=json" in query:
        return True
    if any(token in text for token in ("/toc-json", "table-of-contents.json")):
        return True
    if (
        "mrf" in path
        and "toc" in text
        and any(token in text for token in ("index", "table-of-contents"))
    ):
        return True
    file_name = Path(path).name.replace("_", "-")
    if "." in file_name:
        return False
    if not file_name.endswith(("-index", "-toc")):
        return False
    return any(
        token in text
        for token in (
            "/mrf/",
            "mrf",
            "machine-readable",
            "transparency",
            "table-of-contents",
            "table of contents",
        )
    )


def _looks_html_mrf_body_reference(url: str | None, label: str | None = None) -> bool:
    query_file_name = _query_mrf_file_name(url)
    direct_body = _looks_direct_mrf_body_url(url) or bool(query_file_name)
    parsed = urlsplit(str(url or ""))
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if host == "github.com" and path.startswith("/cmsgov/price-transparency-guide"):
        return False
    if _looks_non_tic_mrf_reference(url, label):
        return False
    if path.endswith("/") and not query_file_name:
        return False
    text = f"{path} {label or ''} {query_file_name or ''}".lower().replace("_", "-")
    inferred_file_type = _mrf_file_type_from_text(url, label)
    if not direct_body:
        file_name = Path(path).name
        if "." in file_name:
            return False
        extensionless_mrf_body = inferred_file_type in {
            "in-network",
            "allowed-amounts",
        } and (
            re.search(r"(?:^|[-/])20\d{2}[-/]\d{2}[-/]\d{2}(?:[-/]|$)", text)
            or any(
                token in text
                for token in (
                    "large-group",
                    "small-group",
                    "individual-and-family",
                )
            )
        )
        if not extensionless_mrf_body and not any(
            token in text
            for token in (
                "getmachinereadablefile",
                "machine-readable",
                "mrf",
                "transparency",
            )
        ):
            return False
        if not any(
            token in text
            for token in (
                "allowed",
                "in-network",
                "in network",
                "negotiated",
                "out-of-network",
                "out of network",
                "rate",
                "rates",
            )
        ):
            return False
    if any(token in text for token in ("formulary", "provider-data", "provider ")):
        return False
    if direct_body and _mrf_file_type_from_text(url, label) == "table-of-contents":
        return True
    return any(
        token in text
        for token in (
            "allowed",
            "in-network",
            "in network",
            "innetwork",
            "negotiated",
            "rate",
            "rates",
            "table-of-contents",
            "table of contents",
            "mrf",
            "machine-readable",
            "transparency",
        )
    )


def _html_mrf_frame_urls(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    base_key = _canonical_or_none(base_url) or base_url
    for match in re.finditer(
        r"<iframe\b(?P<attrs>[^>]*)>", html_text or "", flags=re.I | re.S
    ):
        attrs = match.group("attrs") or ""
        src_match = re.search(
            r"""src\s*=\s*(?P<quote>["'])(?P<value>.*?)(?P=quote)""",
            attrs,
            flags=re.I | re.S,
        )
        if not src_match:
            continue
        raw_src = html.unescape(src_match.group("value") or "").strip()
        if not raw_src or raw_src.lower().startswith(("javascript:", "mailto:", "#")):
            continue
        url = urljoin(base_url, raw_src)
        parsed = urlsplit(url)
        path = parsed.path.lower()
        text = f"{path} {parsed.query}".replace("_", "-")
        if not any(
            token in text
            for token in ("mrf", "machine-readable", "transparency", "tic")
        ):
            continue
        key = _canonical_or_none(url) or url
        if key == base_key or key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _html_label_looks_fileish(label: str | None, path: str) -> bool:
    text = _clean_text(label)
    if not text:
        return True
    if text.lower() in {"here", "download", "file", "mrf", "mrf file", "json"}:
        return True
    if "..>" in text or "..." in text:
        return True
    if re.search(r"\.(?:json|gz|zip|7z|csv)\b", text, flags=re.I):
        return True
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}[_-]", text))


def _html_label_looks_planish(label: str | None) -> bool:
    text = _clean_text(label)
    if not text:
        return False
    normalized = re.sub(r"[^a-z0-9]+", " ", text.lower()).strip()
    normalized = re.sub(r"\s+(json|zip|file)$", "", normalized).strip()
    if normalized in {
        "allowed amount",
        "allowed amounts",
        "download",
        "file",
        "in network",
        "in network rates",
        "mrf",
        "mrf file",
        "table of contents",
        "toc",
    }:
        return False
    return bool(re.search(r"[a-z]", normalized))


def _mrf_file_type_from_html_link_context(
    html_text: str, html_start: int | None, html_end: int | None
) -> str | None:
    if not isinstance(html_start, int) or not isinstance(html_end, int):
        return None
    after = html_text[html_end : html_end + 700]
    after = re.split(
        r"<a\b|</(?:tr|li|div|section|article)\b",
        after,
        maxsplit=1,
        flags=re.I,
    )[0]
    return _mrf_file_type_from_html_section(after)


def _parse_html_mrf_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    urls: dict[tuple[str, str], dict[str, Any]] = {}
    section_file_type: str | None = None
    last_html_position = 0
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        html_start = candidate.get("html_start")
        if isinstance(html_start, int):
            section = _mrf_file_type_from_html_section(
                html_text[last_html_position:html_start]
            )
            if section:
                section_file_type = section
            html_end = candidate.get("html_end")
            if isinstance(html_end, int):
                last_html_position = max(last_html_position, html_end)
        url = str(candidate["url"])
        label = (
            _clean_text(candidate.get("label"))
            or Path(urlsplit(url).path).name
            or "MRF file"
        )
        attr = str(candidate.get("attr") or "")
        path = urlsplit(url).path.lower()
        label_or_path = f"{path} {label}".lower()
        target_kind: str | None = None
        target_file_type: str | None = None
        resolver = "html_mrf_link"
        query_file_name = _query_mrf_file_name(url)
        direct_body = _looks_direct_mrf_body_url(url) or bool(query_file_name)
        link_file_type = _mrf_file_type_from_html_link_context(
            html_text,
            html_start if isinstance(html_start, int) else None,
            candidate.get("html_end")
            if isinstance(candidate.get("html_end"), int)
            else None,
        )
        if path.endswith(".txt") and any(
            token in label_or_path
            for token in ("meta", "mrf", "machine-readable", "transparency")
        ):
            target_kind = "metadata_text"
            target_file_type = "metadata-index"
            resolver = "html_metadata_text"
        elif _looks_html_mrf_toc_url(url, label):
            target_kind = "toc_json"
            target_file_type = "table-of-contents"
        elif _looks_html_mrf_body_reference(url, label) or (
            direct_body
            and (link_file_type or section_file_type)
            and not _looks_non_tic_mrf_reference(url, label)
        ):
            target_kind = "file_reference"
            target_file_type = _mrf_body_file_type_from_text(url, label) or (
                (link_file_type or section_file_type) if direct_body else None
            )
            if not target_file_type:
                continue
            resolver = "html_file_reference"
            inferred_label = _mrf_file_plan_label(path)
            if (
                _looks_direct_mrf_body_url(url)
                and inferred_label
                and _html_label_looks_fileish(label, path)
            ):
                label = inferred_label
        if not target_kind or not target_file_type:
            continue
        plan_info = []
        if target_kind == "file_reference":
            if _html_label_looks_fileish(candidate.get("label"), path) or (
                target_file_type == "in-network" and _html_label_looks_planish(label)
            ):
                plan_info = _plan_info_from_label(label)
        key = (target_kind, _canonical_or_none(url) or url)
        row = {
            "url": url,
            "label": label,
            "resolver": resolver,
            "target_kind": target_kind,
            "target_file_type": target_file_type,
            "container_format": _container_format(url),
            "html_attr": attr,
        }
        if plan_info:
            row["plan_info"] = plan_info
        existing = urls.get(key)
        if existing is None or (
            existing.get("html_attr") == "text" and row.get("html_attr") != "text"
        ):
            urls[key] = row
    return list(urls.values())


def _dedupe_crawl_targets_by_url(targets: list[CrawlTarget]) -> list[CrawlTarget]:
    deduped: dict[str, CrawlTarget] = {}
    for target in targets:
        key = _canonical_or_none(target.url) or target.url
        if key not in deduped:
            deduped[key] = target
    return list(deduped.values())


def _crawl_targets_from_html_mrf_links(
    source: dict[str, Any],
    html_text: str,
    *,
    base_url: str,
    resolver: str,
    target_max_bytes: int | None = None,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    for target in _parse_html_mrf_links(html_text, base_url=base_url):
        metadata = {
            "resolver": target.get("resolver") or resolver,
            "target_kind": target.get("target_kind"),
            "target_file_type": target.get("target_file_type"),
            "container_format": target.get("container_format"),
            "html_attr": target.get("html_attr"),
            "plan_info": target.get("plan_info"),
            "target_max_bytes": (
                target_max_bytes if target.get("target_kind") == "toc_json" else None
            ),
        }
        targets.append(
            CrawlTarget(
                source=source,
                url=str(target["url"]),
                label=str(target.get("label") or source.get("display_name") or ""),
                resolved_from_url=base_url,
                metadata={
                    key: value
                    for key, value in metadata.items()
                    if value not in (None, "", [])
                },
            )
        )
    return targets


def _embedded_mrf_host_urls(value: str | None) -> list[str]:
    text = html.unescape(str(value or ""))
    host_patterns = (
        r"(?P<host>[a-z0-9.-]+\.sapphiremrfhub\.com)(?P<path>/[^\s\"'<>)]*)?",
        r"(?P<host>mrf\.healthcarebluebook\.com)(?P<path>/[A-Za-z0-9_.~%/-]+)?",
        r"(?P<host>mrf\.healthgram\.com)(?P<path>/[A-Za-z0-9_.~%/-]+)?",
        r"(?P<host>mrfsearch\.meritain\.com)(?P<path>/[A-Za-z0-9_.~%/-]+)?",
        r"(?P<host>transparency-in-coverage\.uhc\.com)(?P<path>/[A-Za-z0-9_.~%/-]+)?",
        r"(?P<host>(?:www\.)?mymedicalshopper\.com)(?P<path>/(?:mrf-search|mrf)/[A-Za-z0-9_.~%/-]+)",
        r"(?P<host>www\.asrhealthbenefits\.com)(?P<path>/(?:mrf|MRF|umbraco/surface/mrfdownload|home/umbraco/surface/mrfdownload)[A-Za-z0-9_.~%/?=&-]*)",
    )
    urls: list[str] = []
    seen: set[str] = set()
    for pattern in host_patterns:
        for match in re.finditer(pattern, text, flags=re.I):
            host = str(match.group("host") or "").rstrip(".").lower()
            path = str(match.groupdict().get("path") or "/").rstrip(".,;")
            url = f"https://{host}{path or '/'}"
            key = _canonical_or_none(url) or url
            if key in seen:
                continue
            seen.add(key)
            urls.append(url)
    return urls


def _delegated_mrf_source_urls_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    candidates = [
        str(item.get("url") or "")
        for item in _html_link_candidates(html_text, base_url=base_url)
    ]
    candidates.extend(_embedded_mrf_host_urls(html_text))
    source_key = _canonical_or_none(base_url) or base_url
    for url in candidates:
        key = _canonical_or_none(url) or url
        if not url or key == source_key or key in seen:
            continue
        platform = classify_hosting_platform(url)
        if not platform or platform in {
            "custom",
            "s3",
            "change_healthcare",
            "github_repo_mrf",
            "html_delegated_mrf_links",
        }:
            continue
        if not _platform_resolver_config(platform):
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _hcsc_asomrf_urls_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "")
        if "/asomrf" not in urlsplit(url).path.lower():
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _point32_directory_urls_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    candidates = [
        str(item.get("url") or "")
        for item in _html_link_candidates(html_text, base_url=base_url)
    ]
    candidates.extend(_embedded_http_urls(html_text))
    for url in candidates:
        parsed = urlsplit(url)
        host = parsed.netloc.lower()
        if not host.endswith(".web.core.windows.net"):
            continue
        if "mrf" not in host and "mrf" not in parsed.path.lower():
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _anthem_s3_status_urls_from_script(script_text: str) -> list[str]:
    return [
        url
        for url in _embedded_http_urls(script_text)
        if urlsplit(url).netloc.endswith("amazonaws.com")
        and urlsplit(url).path.endswith("/status.json")
    ]


def _anthem_s3_bases_from_script(script_text: str) -> list[str]:
    bases: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(
        r"""s3url\s*=\s*(?P<quote>["'])(?P<url>https?://.*?/)(?P=quote)""",
        script_text or "",
        flags=re.I,
    ):
        url = html.unescape(match.group("url"))
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        bases.append(url)
    return bases


def _anthem_s3_toc_patterns_from_script(
    script_text: str, *, source_url: str
) -> list[tuple[str, str, str]]:
    patterns: list[tuple[str, str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    regex = re.compile(
        r"""s3url\s*\+\s*["'](?P<prefix>[^"']+/)["']\s*\+\s*year\s*\+\s*["']-["']\s*\+\s*month\s*\+\s*["']-01_(?P<slug>[^"']+)_index\.json(?P<gzip>\.gz)?["']""",
        flags=re.I,
    )
    for match in regex.finditer(script_text or ""):
        pattern = (
            match.group("prefix").strip("/"),
            match.group("slug"),
            ".json.gz" if match.group("gzip") else ".json",
        )
        if pattern not in seen:
            seen.add(pattern)
            patterns.append(pattern)
    if patterns:
        return patterns
    host = _domain(source_url) or ""
    if "healthlink" in host:
        return [("healthlink", "healthlink", ".json")]
    return [("anthem", "anthem", ".json.gz")]


def _anthem_s3_toc_targets(
    source: dict[str, Any],
    base_url: str,
    patterns: list[tuple[str, str, str]],
    resolver: dict[str, Any],
    *,
    source_url: str,
    now: dt.datetime | None = None,
) -> list[CrawlTarget]:
    month_offsets = resolver.get("month_offsets")
    if not isinstance(month_offsets, list) or not month_offsets:
        month_offsets = [0]
    targets: list[CrawlTarget] = []
    current = now or _utc_now()
    for raw_offset in month_offsets:
        offset = _as_int(raw_offset)
        if offset is None:
            continue
        month_date = _add_months(current, offset)
        month_start = month_date.strftime("%Y-%m-01")
        for prefix, slug, extension in patterns:
            file_name = f"{month_start}_{slug}_index{extension}"
            target_url = urljoin(
                base_url.rstrip("/") + "/", f"{prefix.strip('/')}/{file_name}"
            )
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target_url,
                    label=file_name,
                    resolved_from_url=source_url,
                    metadata={
                        "resolver": "anthem_s3_mrf",
                        "target_kind": "toc_json",
                        "target_file_type": "table-of-contents",
                        "container_format": _container_format(target_url),
                        "month_start": month_start,
                        "s3_prefix": prefix,
                    },
                )
            )
    return _dedupe_crawl_targets_by_url(targets)


def _parse_html_mrf_metadata_links(
    html_text: str, *, base_url: str
) -> list[dict[str, Any]]:
    return [
        {
            "url": target["url"],
            "label": Path(urlsplit(str(target["url"])).path).name or target["label"],
        }
        for target in _parse_html_mrf_links(html_text, base_url=base_url)
        if target.get("target_kind") == "metadata_text"
    ]


def _plan_info_from_label(
    label: str,
    *,
    plan_id: str | None = None,
    plan_id_type: str | None = None,
    market_type: str | None = "group",
) -> list[dict[str, Any]]:
    clean_label = _clean_text(label)
    resolved_plan_id = str(plan_id or "").strip()
    resolved_plan_id_type = plan_id_type
    if not resolved_plan_id:
        match = re.search(r"\b(?P<ein>\d{9})\b", clean_label)
        if match:
            resolved_plan_id = match.group("ein")
            resolved_plan_id_type = "ein"
    if not clean_label and not resolved_plan_id:
        return []
    return [
        {
            "plan_id": resolved_plan_id or None,
            "plan_id_type": resolved_plan_id_type,
            "plan_market_type": market_type,
            "plan_name": clean_label or resolved_plan_id,
        }
    ]


def _metadata_plan_info(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    raw_plan_info = metadata.get("plan_info")
    if not isinstance(raw_plan_info, list):
        return []
    plan_info: list[dict[str, Any]] = []
    for item in raw_plan_info:
        if not isinstance(item, dict):
            continue
        plan = dict(item)
        if "plan_market_type" not in plan and plan.get("market_type"):
            plan["plan_market_type"] = plan.get("market_type")
        if not any(
            plan.get(key)
            for key in ("plan_id", "plan_name", "plan_sponsor_name", "issuer_name")
        ):
            continue
        plan_info.append(plan)
    return plan_info


def _plan_rows_from_target_metadata(target: CrawlTarget) -> list[dict[str, Any]]:
    source = target.source
    metadata = dict(target.metadata or {})
    plan_rows_by_id: dict[str, dict[str, Any]] = {}
    now = _utc_now()
    for plan in _metadata_plan_info(metadata):
        plan_id = str(plan.get("plan_id") or "").strip()
        plan_name = (
            plan.get("plan_name")
            or plan.get("plan_sponsor_name")
            or plan.get("issuer_name")
            or target.label
        )
        market_type = plan.get("plan_market_type")
        row_id = _id(
            "mrfplan",
            {
                "source": source["source_id"],
                "plan_id": plan_id,
                "plan_name": plan_name,
                "market_type": market_type,
                "target_url": target.url,
            },
        )
        plan_rows_by_id[row_id] = {
            "mrf_plan_id": row_id,
            "payer_id": source.get("payer_id"),
            "source_id": source["source_id"],
            "plan_id": plan_id or None,
            "plan_id_type": plan.get("plan_id_type"),
            "plan_name": plan_name,
            "market_type": market_type,
            "reporting_entity_name": metadata.get("reporting_entity_name")
            or source.get("display_name"),
            "reporting_entity_type": metadata.get("reporting_entity_type")
            or "third_party_administrator",
            "metadata_json": {
                "raw_plan": plan,
                "resolver": metadata.get("resolver"),
                "target_url": target.url,
                "resolved_from_url": target.resolved_from_url,
            },
            "first_seen_at": now,
            "last_seen_at": now,
        }
    return list(plan_rows_by_id.values())


def _meritain_group_id_from_healthsparq_url(url: str | None) -> str | None:
    try:
        params = _healthsparq_public_params(str(url or ""))
    except ValueError:
        return None
    reporting_entity_type = str(params.get("reportingEntityType") or "").strip()
    match = re.search(r"\bTPA[_-](?P<group>\d+)\b", reporting_entity_type, flags=re.I)
    return match.group("group") if match else None


def _parse_meritain_mrf_search_targets(
    html_text: str,
    *,
    base_url: str,
    source: dict[str, Any],
    resolver_type: str,
) -> list[CrawlTarget]:
    targets_by_url: dict[str, CrawlTarget] = {}
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "").strip()
        host = _domain(url) or ""
        if host not in {"health1.meritain.com", "health1.aetna.com"}:
            continue
        try:
            params = _healthsparq_public_params(url)
        except ValueError:
            continue
        if str(params.get("insurerCode") or "").upper() != "MERITAIN_I":
            continue
        group_id = _meritain_group_id_from_healthsparq_url(url)
        label = _clean_text(candidate.get("label")) or (
            f"Meritain group {group_id}" if group_id else "Meritain Health"
        )
        plan_info = _plan_info_from_label(
            f"Meritain group {group_id}" if group_id else label,
            plan_id=group_id,
            plan_id_type="group_id" if group_id else None,
        )
        key = _canonical_or_none(url) or url
        targets_by_url[key] = CrawlTarget(
            source=source,
            url=url,
            label=label,
            resolved_from_url=base_url,
            metadata={
                "resolver": resolver_type,
                "target_kind": "file_reference",
                "target_file_type": "table-of-contents",
                "source_format": "healthsparq_app",
                "insurer_code": params.get("insurerCode"),
                "brand_code": params.get("brandCode"),
                "reporting_entity_type": params.get("reportingEntityType"),
                "group_id": group_id,
                "plan_info": plan_info,
            },
        )
    return list(targets_by_url.values())


async def _resolve_meritain_mrf_search(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "meritain_mrf_search")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 20 * 1024 * 1024),
        session=session,
    )
    targets = _parse_meritain_mrf_search_targets(
        html_text, base_url=url, source=source, resolver_type=resolver_type
    )
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Meritain Health group MRF links found for {url}")
    return targets


def _healthcarebluebook_file_type(type_text: Any, label: str, url: str) -> str | None:
    normalized = _clean_text(type_text).lower().replace("-", " ")
    if "table of contents" in normalized:
        return "table-of-contents"
    if "out of network" in normalized or normalized == "oon":
        return "allowed-amounts"
    if "in network" in normalized:
        return "in-network"
    return _mrf_file_type_from_text(url, label)


def _healthcarebluebook_source_format(url: str) -> str | None:
    path = urlsplit(url).path.lower()
    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".7z"):
        return "7z"
    if path.endswith(".zip") or _domain(url) == "mrf.healthcarebluebook.com":
        return "zip"
    if path.endswith((".json.gz", ".gz")):
        return "gzip"
    if path.endswith(".json"):
        return "json"
    return None


def _html_enclosing_fragment(html_text: str, start: int, end: int) -> str:
    best = ""
    best_size: int | None = None
    for tag in ("tr", "li", "article", "section", "div"):
        pattern = re.compile(
            rf"<{tag}\b[^>]*>.*?</{tag}>",
            flags=re.I | re.S,
        )
        for match in pattern.finditer(html_text or ""):
            if match.start() > start or match.end() < end:
                continue
            size = match.end() - match.start()
            if best_size is None or size < best_size:
                best = match.group(0)
                best_size = size
    return best


def _healthcarebluebook_link_items_from_context(
    html_text: str, *, base_url: str
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in _html_link_candidates(html_text or "", base_url=base_url):
        link_url = str(candidate.get("url") or "").strip()
        if not link_url:
            continue
        if not (
            _healthcarebluebook_relative_file_url(link_url)
            or _looks_direct_mrf_body_url(link_url)
            or classify_hosting_platform(link_url)
        ):
            continue
        key = _canonical_or_none(link_url) or link_url
        if key in seen:
            continue
        seen.add(key)
        context_html = ""
        start = candidate.get("html_start")
        end = candidate.get("html_end")
        if isinstance(start, int) and isinstance(end, int):
            context_html = _html_enclosing_fragment(html_text or "", start, end)
        context_text = _strip_html_tags(context_html or html_text or "")
        label = _clean_text(candidate.get("label"))
        path_label = Path(urlsplit(link_url).path).name
        if context_text and (
            not label
            or label.lower() in {"download", "view", "open", "file"}
            or label == path_label
        ):
            label = context_text or Path(urlsplit(link_url).path).name
        items.append({"url": link_url, "label": label, "text": label})
        items.append({"url": None, "label": context_text, "text": context_text})
    return items


def _healthcarebluebook_grid_items(
    html_text: str, *, base_url: str
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    pattern = re.compile(
        r"""<div\b[^>]*class=(?P<quote>["'])(?=[^"']*\bgrid-item\b)[^"']*(?P=quote)[^>]*>(?P<content>.*?)</div>""",
        flags=re.I | re.S,
    )
    for match in pattern.finditer(html_text or ""):
        content = match.group("content") or ""
        links = _html_link_candidates(content, base_url=base_url)
        link = next((item for item in links if item.get("url")), None)
        label = _strip_html_tags(content)
        items.append(
            {
                "url": str(link.get("url")) if link else None,
                "label": _clean_text(link.get("label")) if link else label,
                "text": label,
            }
        )
    if not items:
        items = _healthcarebluebook_link_items_from_context(html_text, base_url=base_url)
    return items


def _healthcarebluebook_relative_file_url(url: str) -> bool:
    parsed = urlsplit(url)
    return parsed.netloc.lower() == "mrf.healthcarebluebook.com" and bool(
        re.match(r"^/[^/]+/\d+/?$", parsed.path)
    )


def _healthcarebluebook_nested_target(
    target: CrawlTarget,
    *,
    source: dict[str, Any],
    listing_url: str,
    link_url: str,
    label: str,
    file_type: str | None,
    resolver_type: str,
) -> CrawlTarget:
    metadata = dict(target.metadata or {})
    nested_resolver = metadata.get("resolver")
    metadata.update(
        {
            "resolver": resolver_type,
            "nested_resolver": nested_resolver,
            "healthcarebluebook_listing_url": listing_url,
            "healthcarebluebook_link_url": link_url,
            "healthcarebluebook_link_label": label,
            "healthcarebluebook_file_type": file_type,
            "nested_target_label": target.label,
        }
    )
    return CrawlTarget(
        source=source,
        url=target.url,
        label=target.label or label,
        resolved_from_url=target.resolved_from_url or link_url,
        metadata=metadata,
    )


async def _resolve_healthcarebluebook_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "healthcarebluebook_mrf")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
        session=session,
    )
    items = _healthcarebluebook_grid_items(html_text, base_url=url)
    targets_by_url: dict[str, CrawlTarget] = {}
    max_targets = _as_int(resolver.get("max_targets"))
    for index, item in enumerate(items):
        if max_targets and len(targets_by_url) >= max_targets:
            break
        link_url = str(item.get("url") or "").strip()
        if not link_url:
            continue
        label = (
            _clean_text(item.get("label"))
            or Path(urlsplit(link_url).path).name
            or "MRF file"
        )
        type_text = items[index + 1].get("text") if index + 1 < len(items) else ""
        file_type = _healthcarebluebook_file_type(type_text, label, link_url)
        if not file_type:
            continue
        if _healthcarebluebook_relative_file_url(
            link_url
        ) or _looks_direct_mrf_body_url(link_url):
            plan_info = _plan_info_from_label(label)
            metadata = {
                "resolver": resolver_type,
                "target_kind": "file_reference",
                "target_file_type": file_type,
                "container_format": _container_format(link_url)
                or ("zip" if _healthcarebluebook_relative_file_url(link_url) else None),
                "source_format": _healthcarebluebook_source_format(link_url),
                "healthcarebluebook_listing_url": url,
                "healthcarebluebook_file_type": _clean_text(type_text),
                "plan_info": plan_info,
            }
            key = _canonical_or_none(link_url) or link_url
            targets_by_url[key] = CrawlTarget(
                source=source,
                url=link_url,
                label=label,
                resolved_from_url=url,
                metadata={
                    key: value
                    for key, value in metadata.items()
                    if value not in (None, "", [])
                },
            )
            continue
        nested_platform = classify_hosting_platform(link_url)
        if not nested_platform:
            continue
        nested_source = {**source, "hosting_platform": nested_platform}
        try:
            nested_targets = await _crawl_targets_for_source(
                nested_source, link_url, session
            )
        except Exception:  # pylint: disable=broad-exception-caught
            nested_targets = []
        for nested_target in nested_targets:
            annotated = _healthcarebluebook_nested_target(
                nested_target,
                source=source,
                listing_url=url,
                link_url=link_url,
                label=label,
                file_type=file_type,
                resolver_type=resolver_type,
            )
            key = _canonical_or_none(annotated.url) or annotated.url
            targets_by_url[key] = annotated
            if max_targets and len(targets_by_url) >= max_targets:
                break
    targets = list(targets_by_url.values())
    if max_targets:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Healthcare Bluebook MRF links found for {url}")
    return targets


async def _resolve_html_mrf_with_healthcarebluebook(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "html_mrf_with_healthcarebluebook")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
        session=session,
    )
    targets_by_url: dict[str, CrawlTarget] = {}
    for target in _parse_html_mrf_links(html_text, base_url=url):
        target_url = str(target["url"])
        source_format = (
            "csv" if urlsplit(target_url).path.lower().endswith(".csv") else None
        )
        metadata = {
            "resolver": target.get("resolver") or resolver_type,
            "target_kind": target.get("target_kind"),
            "target_file_type": target.get("target_file_type"),
            "container_format": target.get("container_format"),
            "source_format": source_format,
            "html_attr": target.get("html_attr"),
            "landing_url": url,
            "plan_info": target.get("plan_info"),
        }
        key = _canonical_or_none(target_url) or target_url
        targets_by_url[key] = CrawlTarget(
            source=source,
            url=target_url,
            label=str(target.get("label") or source.get("display_name") or ""),
            resolved_from_url=url,
            metadata={
                key: value for key, value in metadata.items() if value not in (None, "")
            },
        )
    for candidate in _html_link_candidates(html_text, base_url=url):
        link_url = str(candidate.get("url") or "").strip()
        if _domain(link_url) != "mrf.healthcarebluebook.com":
            continue
        nested_source = {**source, "hosting_platform": "healthcarebluebook_mrf"}
        nested_resolver = _platform_resolver_config("healthcarebluebook_mrf") or {
            "type": "healthcarebluebook_mrf"
        }
        max_targets = _as_int(resolver.get("max_targets"))
        if max_targets:
            existing_nested_max = _as_int(nested_resolver.get("max_targets"))
            nested_resolver["max_targets"] = (
                min(existing_nested_max, max_targets)
                if existing_nested_max
                else max_targets
            )
        nested_targets = await _resolve_healthcarebluebook_mrf(
            nested_source, link_url, nested_resolver, session
        )
        for nested_target in nested_targets:
            metadata = dict(nested_target.metadata or {})
            metadata.update(
                {
                    "resolver": resolver_type,
                    "nested_resolver": metadata.get("resolver"),
                    "landing_url": url,
                    "healthcarebluebook_url": link_url,
                }
            )
            annotated = CrawlTarget(
                source=source,
                url=nested_target.url,
                label=nested_target.label,
                resolved_from_url=nested_target.resolved_from_url or link_url,
                metadata=metadata,
            )
            key = _canonical_or_none(annotated.url) or annotated.url
            targets_by_url[key] = annotated
    targets = list(targets_by_url.values())
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no HTML or Healthcare Bluebook MRF links found for {url}")
    return targets


def _regex_matches_config(value: Any, pattern: Any) -> bool:
    pattern_text = str(pattern or "").strip()
    if not pattern_text:
        return True
    try:
        return bool(re.search(pattern_text, str(value or "")))
    except re.error:
        return False


def _socrata_dataset_download(dataset: dict[str, Any]) -> tuple[str | None, str | None]:
    distributions = dataset.get("distribution") or []
    if not isinstance(distributions, list):
        return None, None
    candidates: list[tuple[str, str | None]] = []
    for distribution in distributions:
        if not isinstance(distribution, dict):
            continue
        download_url = str(distribution.get("downloadURL") or "").strip()
        if not download_url:
            continue
        media_type = str(distribution.get("mediaType") or "").strip() or None
        candidates.append((download_url, media_type))
    if not candidates:
        return None, None
    for download_url, media_type in candidates:
        media_text = str(media_type or "").lower()
        if "json" in media_text or "geo+json" in media_text:
            return download_url, media_type
    return candidates[0]


def _socrata_dataset_id(
    dataset: dict[str, Any], download_url: str | None
) -> str | None:
    for value in (dataset.get("identifier"), dataset.get("landingPage"), download_url):
        parsed = urlsplit(str(value or ""))
        parts = [part for part in parsed.path.split("/") if part]
        for marker in ("views", "download", "d"):
            if marker in parts:
                index = parts.index(marker)
                if index + 1 < len(parts):
                    return parts[index + 1]
        if parts and re.fullmatch(r"[a-z0-9]{4}-[a-z0-9]{4}", parts[-1], flags=re.I):
            return parts[-1]
    return None


def _socrata_coverage_month(title: str) -> tuple[int, int] | None:
    for match in re.finditer(
        r"\b(?P<month>" + "|".join(_MONTH_NAME_TO_NUMBER) + r")\s+(?P<year>20\d{2})\b",
        str(title or ""),
        flags=re.I,
    ):
        return (
            int(match.group("year")),
            _MONTH_NAME_TO_NUMBER[match.group("month").lower()],
        )
    return None


def _socrata_file_type(title: str, description: str) -> str | None:
    text = f"{title} {description}".lower().replace("_", "-")
    if re.search(r"\bin[-\s]+network\s+rates?\b", text):
        return "in-network"
    if (
        re.search(r"\bout[-\s]+of[-\s]+network\s+allowed\s+amounts?\b", text)
        or re.search(r"\ballowed\s+amounts?\b", text)
        or "outside of the vhp network" in text
    ):
        return "allowed-amounts"
    return None


def _socrata_market_type(title: str) -> str | None:
    text = str(title or "").lower()
    if "employer group" in text or "commercial" in text or "ihss" in text:
        return "group"
    if "covered california" in text or "individual and family" in text:
        return "individual"
    return None


def _socrata_benefit_metadata(
    title: str, description: str, download_url: str
) -> dict[str, Any]:
    lines = _infer_benefit_lines_from_text(title, description, download_url) or (
        "medical",
    )
    metadata: dict[str, Any] = {"benefit_lines": list(lines)}
    if len(lines) == 1:
        metadata["benefit_line"] = lines[0]
    return metadata


def _socrata_dataset_contact_text(dataset: dict[str, Any]) -> str:
    contact = dataset.get("contactPoint") or {}
    publisher = dataset.get("publisher") or {}
    values = [
        contact.get("fn") if isinstance(contact, dict) else None,
        contact.get("hasEmail") if isinstance(contact, dict) else None,
        dataset.get("attribution"),
        publisher.get("name") if isinstance(publisher, dict) else None,
    ]
    return " ".join(str(value or "") for value in values)


def _socrata_dataset_matches(dataset: dict[str, Any], resolver: dict[str, Any]) -> bool:
    title = _clean_text(dataset.get("title"))
    description = _clean_text(dataset.get("description"))
    if not title:
        return False
    if str(dataset.get("accessLevel") or "").lower() not in {"", "public"}:
        return False
    if not _socrata_file_type(title, description):
        return False
    if not _regex_matches_config(title, resolver.get("title_regex")):
        return False
    if not _regex_matches_config(
        _socrata_dataset_contact_text(dataset), resolver.get("contact_regex")
    ):
        return False
    keyword_any = [
        str(item or "").strip().lower()
        for item in (resolver.get("keyword_any") or [])
        if str(item or "").strip()
    ]
    if keyword_any:
        keyword_text = " ".join(
            str(item or "").lower() for item in (dataset.get("keyword") or [])
        )
        if not any(keyword in keyword_text for keyword in keyword_any):
            return False
    download_url, _media_type = _socrata_dataset_download(dataset)
    return bool(download_url)


def _socrata_target_from_dataset(
    source: dict[str, Any],
    dataset: dict[str, Any],
    *,
    catalog_url: str,
    resolver_type: str,
) -> CrawlTarget | None:
    title = _clean_text(dataset.get("title"))
    description = _clean_text(dataset.get("description"))
    download_url, media_type = _socrata_dataset_download(dataset)
    if not download_url:
        return None
    file_type = _socrata_file_type(title, description)
    if not file_type:
        return None
    dataset_id = _socrata_dataset_id(dataset, download_url)
    market_type = _socrata_market_type(title)
    plan_info = _plan_info_from_label(
        title,
        plan_id=dataset_id,
        plan_id_type="socrata_view_id" if dataset_id else None,
        market_type=market_type,
    )
    coverage_month = _socrata_coverage_month(title)
    benefit_metadata = _socrata_benefit_metadata(title, description, download_url)
    metadata = {
        "resolver": resolver_type,
        "target_kind": "file_reference",
        "target_file_type": file_type,
        "source_format": "json",
        "socrata_dataset_id": dataset_id,
        "socrata_landing_page": dataset.get("landingPage"),
        "socrata_media_type": media_type,
        "socrata_issued": dataset.get("issued"),
        "socrata_modified": dataset.get("modified"),
        "socrata_coverage_month": (
            f"{coverage_month[0]:04d}-{coverage_month[1]:02d}"
            if coverage_month
            else None
        ),
        "reporting_entity_name": source.get("display_name"),
        "reporting_entity_type": "health_insurance_issuer",
        "plan_info": plan_info,
        **benefit_metadata,
    }
    return CrawlTarget(
        source=source,
        url=download_url,
        label=title,
        resolved_from_url=catalog_url,
        metadata={
            key: value for key, value in metadata.items() if value not in (None, "", [])
        },
    )


async def _resolve_socrata_data_json_mrf_catalog(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "socrata_data_json_mrf_catalog")
    payload = await _fetch_json(
        url,
        max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    datasets = [
        item
        for item in (payload.get("dataset") or [])
        if isinstance(item, dict) and _socrata_dataset_matches(item, resolver)
    ]
    if resolver.get("latest_coverage_month_only"):
        coverage_months = [
            month
            for month in (
                _socrata_coverage_month(_clean_text(item.get("title")))
                for item in datasets
            )
            if month
        ]
        if coverage_months:
            latest_month = max(coverage_months)
            datasets = [
                item
                for item in datasets
                if _socrata_coverage_month(_clean_text(item.get("title")))
                == latest_month
            ]
    targets: list[CrawlTarget] = []
    for dataset in sorted(datasets, key=lambda item: _clean_text(item.get("title"))):
        target = _socrata_target_from_dataset(
            source,
            dataset,
            catalog_url=url,
            resolver_type=resolver_type,
        )
        if target:
            targets.append(target)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Socrata MRF catalog files found for {url}")
    return targets


def _cigna_lookup_urls_from_html(
    html_text: str, *, base_url: str, resolver: dict[str, Any]
) -> list[str]:
    urls: list[str] = []
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        attr = str(candidate.get("attr") or "").lower()
        url = str(candidate.get("url") or "").strip()
        path = urlsplit(url).path.lower()
        if "lookup" in attr or "/static/mrf/" in path:
            if path.endswith(".json") and url not in urls:
                urls.append(url)
    for path in resolver.get("lookup_paths") or ():
        url = urljoin(base_url, str(path))
        if url not in urls:
            urls.append(url)
    return urls


def _first_dict_value(item: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = item.get(key)
        if value not in (None, ""):
            return value
    return None


def _cigna_file_url(item: dict[str, Any]) -> str | None:
    value = _first_dict_value(
        item, ("url", "download_url", "downloadUrl", "location", "file_url", "fileUrl")
    )
    return str(value).strip() if value else None


def _parse_cigna_lookup_targets(
    payload: dict[str, Any],
    *,
    lookup_url: str,
    source: dict[str, Any],
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    toc_max_bytes = (
        _parse_size_bytes(resolver.get("toc_max_bytes")) or 100 * 1024 * 1024
    )
    groups = payload.get("mrfs") if isinstance(payload.get("mrfs"), list) else None
    if groups is None:
        groups = payload.get("mrf") if isinstance(payload.get("mrf"), list) else None
    if groups is None:
        groups = [payload]
    targets_by_url: dict[str, CrawlTarget] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_files = (
            group.get("files") if isinstance(group.get("files"), list) else None
        )
        files = group_files or [group]
        for file_item in files:
            if not isinstance(file_item, dict):
                continue
            file_url = _cigna_file_url(file_item)
            if not file_url:
                continue
            file_url = urljoin(lookup_url, file_url)
            file_name = _clean_text(
                _first_dict_value(file_item, ("file_name", "fileName", "name", "label"))
                or _first_dict_value(group, ("file_name", "fileName", "name", "label"))
                or Path(urlsplit(file_url).path).name
            )
            if not (
                _looks_direct_toc_url(file_url)
                or _mrf_file_type_from_text(file_url, file_name) == "table-of-contents"
            ):
                continue
            size_bytes = _parse_size_bytes(
                _first_dict_value(
                    file_item,
                    (
                        "file_size",
                        "fileSize",
                        "size",
                        "content_length",
                        "contentLength",
                    ),
                )
                or _first_dict_value(
                    group,
                    (
                        "file_size",
                        "fileSize",
                        "size",
                        "content_length",
                        "contentLength",
                    ),
                )
            )
            reporting_entity_name = _first_dict_value(
                file_item,
                (
                    "reporting_entity_name",
                    "reportingEntityName",
                    "entity_name",
                    "entityName",
                ),
            ) or _first_dict_value(
                group,
                (
                    "reporting_entity_name",
                    "reportingEntityName",
                    "entity_name",
                    "entityName",
                ),
            )
            reporting_entity_type = _first_dict_value(
                file_item,
                (
                    "reporting_entity_type",
                    "reportingEntityType",
                    "entity_type",
                    "entityType",
                ),
            ) or _first_dict_value(
                group,
                (
                    "reporting_entity_type",
                    "reportingEntityType",
                    "entity_type",
                    "entityType",
                ),
            )
            metadata = {
                "resolver": "cigna_static_mrf_lookup",
                "target_kind": "toc_json",
                "target_file_type": "table-of-contents",
                "target_max_bytes": toc_max_bytes,
                "lookup_url": lookup_url,
                "file_name": file_name,
                "blob_size": size_bytes,
                "reporting_entity_name": reporting_entity_name,
                "reporting_entity_type": reporting_entity_type,
                "last_updated_on": _first_dict_value(
                    file_item, ("last_updated_on", "lastUpdatedOn")
                )
                or _first_dict_value(group, ("last_updated_on", "lastUpdatedOn")),
                "reporting_month": _first_dict_value(
                    file_item, ("reporting_month", "reportingMonth")
                )
                or _first_dict_value(group, ("reporting_month", "reportingMonth")),
                "product_type": _first_dict_value(
                    file_item, ("product_type", "productType")
                )
                or _first_dict_value(group, ("product_type", "productType")),
            }
            key = _canonical_or_none(file_url) or file_url
            targets_by_url[key] = CrawlTarget(
                source=source,
                url=file_url,
                label=file_name or str(source.get("display_name") or "Cigna MRF index"),
                resolved_from_url=lookup_url,
                metadata={
                    key: value
                    for key, value in metadata.items()
                    if value not in (None, "")
                },
            )
    return list(targets_by_url.values())


def _parse_bcbs_asomrf_filelist_targets(
    payload: Any,
    *,
    filelist_url: str,
    source: dict[str, Any],
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    if not isinstance(payload, list):
        raise ValueError("expected BCBS ASO filelist JSON array")
    toc_max_bytes = (
        _parse_size_bytes(resolver.get("toc_max_bytes")) or 100 * 1024 * 1024
    )
    targets_by_url: dict[str, CrawlTarget] = {}
    for item in payload:
        if not isinstance(item, dict):
            continue
        file_url = str(item.get("url") or "").strip()
        file_name = _clean_text(item.get("name")) or Path(urlsplit(file_url).path).name
        if (
            not file_url
            or _mrf_file_type_from_text(file_url, file_name) != "table-of-contents"
        ):
            continue
        metadata = {
            "resolver": "bcbs_asomrf_filelist",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "target_max_bytes": toc_max_bytes,
            "filelist_url": filelist_url,
            "file_name": file_name,
            "state": _clean_text(item.get("state")),
            "ein": _clean_text(item.get("ein")),
            "last_update_date": _clean_text(item.get("last_update_date")),
        }
        key = _canonical_or_none(file_url) or file_url
        targets_by_url[key] = CrawlTarget(
            source=source,
            url=file_url,
            label=file_name or str(source.get("display_name") or "BCBS ASO MRF index"),
            resolved_from_url=filelist_url,
            metadata={
                key: value for key, value in metadata.items() if value not in (None, "")
            },
        )
    targets = list(targets_by_url.values())
    return _limit_crawl_targets_round_robin(
        targets, "state", _as_int(resolver.get("max_targets"))
    )


def _limit_crawl_targets_round_robin(
    targets: list[CrawlTarget], metadata_key: str, limit: int | None
) -> list[CrawlTarget]:
    if not limit or limit <= 0 or len(targets) <= limit:
        return targets
    buckets: dict[str, list[CrawlTarget]] = {}
    for target in targets:
        bucket_key = str((target.metadata or {}).get(metadata_key) or "")
        buckets.setdefault(bucket_key, []).append(target)
    limited: list[CrawlTarget] = []
    max_bucket_size = max((len(bucket) for bucket in buckets.values()), default=0)
    for index in range(max_bucket_size):
        for bucket in buckets.values():
            if index < len(bucket):
                limited.append(bucket[index])
                if len(limited) >= limit:
                    return limited
    return limited


def _bcbs_global_solutions_toc_links_from_html(
    html_text: str, *, base_url: str
) -> list[dict[str, str]]:
    links: dict[str, dict[str, str]] = {}
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "").strip()
        parsed = urlsplit(url)
        if parsed.netloc.lower() != "groupadmin.bcbsglobalsolutions.com":
            continue
        if parsed.path.lower() != "/transparency-in-coverage-toc-json.cfm":
            continue
        plan_type = (parse_qs(parsed.query).get("planType") or [""])[0].strip()
        label = _clean_text(candidate.get("label")) or plan_type or "BCBS Global TOC"
        key = _canonical_or_none(url) or url
        links[key] = {"url": url, "plan_type": plan_type, "label": label}
    return list(links.values())


def _bcbs_global_solutions_landing_links_from_html(
    html_text: str, *, base_url: str
) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "").strip()
        parsed = urlsplit(url)
        if parsed.netloc.lower() != "groupadmin.bcbsglobalsolutions.com":
            continue
        if parsed.path.lower() != "/transparency-in-coverage.cfm":
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        urls.append(url)
    return urls


def _bcbs_global_solutions_toc_has_in_network(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    reporting_structure = payload.get("reporting_structure")
    if not isinstance(reporting_structure, list):
        return False
    return any(
        isinstance(item, dict)
        and isinstance(item.get("in_network_files"), list)
        and bool(item.get("in_network_files"))
        for item in reporting_structure
    )


def _bcbs_global_solutions_first_plan_name(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    for structure in payload.get("reporting_structure") or []:
        if not isinstance(structure, dict):
            continue
        for plan in structure.get("reporting_plans") or []:
            if not isinstance(plan, dict):
                continue
            name = _clean_text(plan.get("reporting_entity_name"))
            if name:
                return name
    return None


async def _resolve_bcbs_global_solutions_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "bcbs_global_solutions_mrf")
    max_pages = _as_int(resolver.get("max_pages")) or 5
    max_targets = _as_int(resolver.get("max_targets")) or 20
    toc_max_bytes = (
        _parse_size_bytes(resolver.get("toc_max_bytes")) or MAX_TOC_BYTES_DEFAULT
    )
    page_urls = [url]
    seen_pages: set[str] = set()
    toc_links: dict[str, dict[str, str]] = {}
    while page_urls and len(seen_pages) < max_pages:
        page_url = page_urls.pop(0)
        page_key = _canonical_or_none(page_url) or page_url
        if page_key in seen_pages:
            continue
        seen_pages.add(page_key)
        html_text = await _fetch_text(
            page_url,
            max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
            session=session,
        )
        for link in _bcbs_global_solutions_toc_links_from_html(
            html_text, base_url=page_url
        ):
            link_url = str(link.get("url") or "")
            link_key = _canonical_or_none(link_url) or link_url
            if link_key:
                toc_links[link_key] = link
        for landing_url in _bcbs_global_solutions_landing_links_from_html(
            html_text, base_url=page_url
        ):
            landing_key = _canonical_or_none(landing_url) or landing_url
            if landing_key not in seen_pages and landing_url not in page_urls:
                page_urls.append(landing_url)

    targets: list[CrawlTarget] = []
    for link in toc_links.values():
        toc_url = str(link.get("url") or "").strip()
        if not toc_url:
            continue
        try:
            payload = await _fetch_json_value(
                toc_url,
                max_bytes=toc_max_bytes,
                session=session,
            )
        except Exception:  # pylint: disable=broad-exception-caught
            continue
        if not _bcbs_global_solutions_toc_has_in_network(payload):
            continue
        plan_type = _clean_text(link.get("plan_type"))
        plan_name = _bcbs_global_solutions_first_plan_name(payload)
        label = _clean_text(link.get("label")) or plan_name or plan_type
        targets.append(
            CrawlTarget(
                source=source,
                url=toc_url,
                label=label or str(source.get("display_name") or "BCBS Global TOC"),
                resolved_from_url=url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": "toc_json",
                    "target_file_type": "table-of-contents",
                    "target_max_bytes": toc_max_bytes,
                    "plan_type": plan_type,
                    "reporting_entity_name": _clean_text(
                        payload.get("reporting_entity_name")
                    ),
                    "reporting_entity_type": _clean_text(
                        payload.get("reporting_entity_type")
                    ),
                    "reporting_plan_name": plan_name,
                },
            )
        )
        if max_targets and len(targets) >= max_targets:
            break
    if not targets:
        raise ValueError(f"no BCBS Global Solutions TOCs found for {url}")
    return targets


def _bcbs_asomrf_filelist_urls_from_html(html_text: str, *, base_url: str) -> list[str]:
    urls: list[str] = []
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate.get("url") or "").strip()
        path = urlsplit(url).path.lower()
        if path.endswith("/content/dam/bcbs/mrf/si-filelist.json") and url not in urls:
            urls.append(url)
    for match in re.finditer(
        r"""(?P<quote>["'])(?P<path>/content/dam/bcbs/mrf/si-filelist\.json)(?P=quote)""",
        html_text or "",
        flags=re.I,
    ):
        url = urljoin(base_url, html.unescape(match.group("path")))
        if url not in urls:
            urls.append(url)
    return urls


async def _resolve_bcbs_asomrf_filelist(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    filelist_urls = _bcbs_asomrf_filelist_urls_from_html(html_text, base_url=url)
    if not filelist_urls:
        raise ValueError(f"no BCBS ASO filelist URL found in {url}")
    targets: list[CrawlTarget] = []
    for filelist_url in filelist_urls:
        payload = await _fetch_json_value(
            filelist_url,
            max_bytes=int(resolver.get("filelist_max_bytes") or 20 * 1024 * 1024),
            session=session,
        )
        targets.extend(
            _parse_bcbs_asomrf_filelist_targets(
                payload, filelist_url=filelist_url, source=source, resolver=resolver
            )
        )
    if not targets:
        raise ValueError(f"no BCBS ASO index URLs found from {url}")
    return targets


def _add_months(value: dt.datetime, offset: int) -> dt.datetime:
    month_index = value.year * 12 + (value.month - 1) + offset
    year = month_index // 12
    month = month_index % 12 + 1
    return value.replace(year=year, month=month, day=1)


def _bcbsma_monthly_toc_targets(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> list[CrawlTarget]:
    base_url = str(resolver.get("base_url") or url or "").strip()
    if not base_url:
        raise ValueError("BCBSMA resolver requires base_url or source URL")
    suffixes = [
        str(item).strip()
        for item in (resolver.get("toc_suffixes") or ())
        if str(item).strip()
    ]
    if not suffixes:
        raise ValueError("BCBSMA resolver requires toc_suffixes")
    month_offsets = resolver.get("month_offsets")
    if not isinstance(month_offsets, list) or not month_offsets:
        month_offsets = [0]
    current = now or _utc_now()
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for raw_offset in month_offsets:
        offset = _as_int(raw_offset)
        if offset is None:
            continue
        month_date = _add_months(current, offset)
        month_prefix = month_date.strftime("%Y-%m")
        month_start = month_date.strftime("%Y-%m-01")
        for suffix in suffixes:
            file_name = f"{month_prefix}{suffix}"
            target_url = urljoin(base_url.rstrip("/") + "/", file_name)
            key = _canonical_or_none(target_url) or target_url
            if key in seen:
                continue
            seen.add(key)
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target_url,
                    label=file_name,
                    resolved_from_url=url,
                    metadata={
                        "resolver": "bcbsma_monthly_tocs",
                        "target_kind": "toc_json",
                        "target_file_type": "table-of-contents",
                        "month_start": month_start,
                        "file_name": file_name,
                        "issuer_slug": suffix.removeprefix("-01_").removesuffix(
                            "_index.json"
                        ),
                    },
                )
            )
    return targets


def _monthly_toc_targets(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> list[CrawlTarget]:
    base_url = str(resolver.get("base_url") or url or "").strip()
    if not base_url:
        raise ValueError("monthly TOC resolver requires base_url or source URL")
    templates = [
        str(item).strip()
        for item in (resolver.get("file_templates") or ())
        if str(item).strip()
    ]
    if not templates:
        raise ValueError("monthly TOC resolver requires file_templates")
    month_offsets = resolver.get("month_offsets")
    if not isinstance(month_offsets, list) or not month_offsets:
        month_offsets = [0]
    target_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes"))
    current = now or _utc_now()
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for raw_offset in month_offsets:
        offset = _as_int(raw_offset)
        if offset is None:
            continue
        month_date = _add_months(current, offset)
        values = {
            "year": month_date.strftime("%Y"),
            "month": month_date.strftime("%m"),
            "month_start": month_date.strftime("%Y-%m-01"),
            "month_prefix": month_date.strftime("%Y-%m"),
            "yyyymm": month_date.strftime("%Y%m"),
        }
        for template in templates:
            file_path = template.format(**values)
            file_name = Path(urlsplit(file_path).path).name or file_path
            target_url = (
                file_path
                if file_path.startswith(("http://", "https://"))
                else urljoin(base_url.rstrip("/") + "/", file_path.lstrip("/"))
            )
            key = _canonical_or_none(target_url) or target_url
            if key in seen:
                continue
            seen.add(key)
            targets.append(
                CrawlTarget(
                    source=source,
                    url=target_url,
                    label=file_name,
                    resolved_from_url=url,
                    metadata={
                        "resolver": str(resolver.get("type") or "monthly_toc"),
                        "target_kind": "toc_json",
                        "target_file_type": "table-of-contents",
                        "month_start": values["month_start"],
                        "file_name": file_name,
                        "target_max_bytes": target_max_bytes,
                    },
                )
            )
    return targets


def _azure_mrf_listing_targets_from_xml(
    source: dict[str, Any],
    xml_text: str,
    *,
    listing_url: str,
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    root = ElementTree.fromstring((xml_text or "").lstrip("\ufeff"))
    container_url = _clean_text(root.attrib.get("ContainerName"))
    target_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes"))
    resolver_type = str(resolver.get("type") or "azure_mrf_listing")
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for blob in root.findall(".//Blob"):
        name = _clean_text(blob.findtext("Name"))
        file_url = _clean_text(blob.findtext("Url"))
        if not file_url and container_url and name:
            file_url = urljoin(container_url.rstrip("/") + "/", name.lstrip("/"))
        label = Path(urlsplit(file_url).path).name or name
        target_kind: str | None = None
        target_file_type: str | None = None
        if _looks_html_mrf_toc_url(file_url, label):
            target_kind = "toc_json"
            target_file_type = "table-of-contents"
        elif _looks_html_mrf_body_reference(file_url, label):
            target_kind = "file_reference"
            target_file_type = _mrf_file_type_from_text(file_url, label)
        if not file_url or not target_kind or not target_file_type:
            continue
        if resolver.get("skip_toc_targets") and target_kind == "toc_json":
            continue
        key = _canonical_or_none(file_url) or file_url
        if key in seen:
            continue
        seen.add(key)
        targets.append(
            CrawlTarget(
                source=source,
                url=file_url,
                label=label,
                resolved_from_url=listing_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": target_kind,
                    "target_file_type": target_file_type,
                    "container_format": _container_format(file_url),
                    "blob_name": name,
                    "content_length": blob.findtext("Properties/Content-Length"),
                    "content_type": blob.findtext("Properties/Content-Type"),
                    "etag": blob.findtext("Properties/Etag"),
                    "last_modified": blob.findtext("Properties/Last-Modified"),
                    "target_max_bytes": (
                        target_max_bytes if target_kind == "toc_json" else None
                    ),
                },
            )
        )
    targets.sort(key=_crawl_target_priority_key)
    return targets


def _crawl_target_priority_key(target: CrawlTarget) -> tuple[int, str]:
    metadata = target.metadata or {}
    file_type = str(metadata.get("target_file_type") or "")
    kind = str(metadata.get("target_kind") or "")
    priority = {
        "table-of-contents": 0,
        "in-network": 1,
        "allowed-amounts": 2,
        "payer-drug": 3,
    }.get(file_type, 9)
    if kind == "toc_json":
        priority = min(priority, 0)
    return priority, target.url


async def _resolve_azure_mrf_listing(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    listing_urls = [
        str(item).strip()
        for item in (resolver.get("listing_urls") or [url])
        if str(item).strip()
    ]
    targets: list[CrawlTarget] = []
    for listing_url in listing_urls:
        xml_text = await _fetch_text(
            listing_url,
            max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
            session=session,
        )
        targets.extend(
            _azure_mrf_listing_targets_from_xml(
                source, xml_text, listing_url=listing_url, resolver=resolver
            )
        )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Azure MRF listing targets found for {url}")
    return targets


def _url_with_query_params(url: str, params: dict[str, Any]) -> str:
    parsed = urlsplit(str(url or ""))
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in params.items():
        query[str(key)] = "" if value is None else str(value)
    return parsed._replace(query=urlencode(query)).geturl()


def _triples_mtt_latest_year_month(payload: Any) -> tuple[str | None, str | None]:
    if not isinstance(payload, dict):
        return None, None
    selects = payload.get("selects")
    years: list[int] = []
    months: list[int] = []
    if isinstance(selects, dict):
        for item in selects.get("year") or ():
            try:
                years.append(int(str(item.get("year") or "").strip()))
            except (AttributeError, TypeError, ValueError):
                continue
        for item in selects.get("month") or ():
            try:
                months.append(int(str(item.get("month") or "").strip()))
            except (AttributeError, TypeError, ValueError):
                continue
    for item in payload.get("list") or ():
        if not isinstance(item, dict):
            continue
        try:
            years.append(int(str(item.get("year") or "").strip()))
            months.append(int(str(item.get("month") or "").strip()))
        except (TypeError, ValueError):
            continue
    if not years or not months:
        return None, None
    return str(max(years)), f"{max(months):02d}"


def _triples_mtt_targets_from_payload(
    source: dict[str, Any],
    payload: Any,
    *,
    resolved_from_url: str,
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    if not isinstance(payload, dict):
        return []
    resolver_type = str(resolver.get("type") or "triples_mtt_api")
    items = [item for item in payload.get("list") or () if isinstance(item, dict)]
    if resolver.get("latest_month_only", True):
        latest = max(
            (
                (str(item.get("year") or ""), str(item.get("month") or ""))
                for item in items
                if item.get("url")
            ),
            default=None,
        )
        if latest:
            items = [
                item
                for item in items
                if (str(item.get("year") or ""), str(item.get("month") or ""))
                == latest
            ]
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for item in items:
        file_url = _clean_text(item.get("url"))
        label = _clean_text(item.get("marketing")) or Path(urlsplit(file_url).path).name
        file_type = _mrf_file_type_from_text(file_url, label)
        if not file_url or not file_type:
            continue
        key = _canonical_or_none(file_url) or file_url
        if key in seen:
            continue
        seen.add(key)
        plan_name = " - ".join(
            part
            for part in (
                _clean_text(item.get("plan")),
                _clean_text(item.get("marketing")),
            )
            if part
        )
        targets.append(
            CrawlTarget(
                source=source,
                url=file_url,
                label=label,
                resolved_from_url=resolved_from_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": (
                        "toc_json"
                        if file_type == "table-of-contents"
                        else "file_reference"
                    ),
                    "target_file_type": file_type,
                    "container_format": _container_format(file_url),
                    "triples_id": item.get("id"),
                    "network": item.get("network"),
                    "plan": item.get("plan"),
                    "year": item.get("year"),
                    "month": item.get("month"),
                    "marketing": item.get("marketing"),
                    "plan_info": [
                        {
                            "plan_id": None,
                            "plan_id_type": None,
                            "plan_market_type": "group",
                            "plan_name": plan_name or None,
                        }
                    ],
                },
            )
        )
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    return targets


async def _resolve_triples_mtt_api(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    api_url = str(resolver.get("api_url") or url).strip()
    params = {
        "network": resolver.get("network") or "",
        "month": "",
        "year": "",
        "plan": resolver.get("plan") or "",
    }
    initial_url = _url_with_query_params(api_url, params)
    payload = await _fetch_json_value(
        initial_url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    resolved_from_url = initial_url
    if resolver.get("latest_month_only", True):
        year, month = _triples_mtt_latest_year_month(payload)
        if year and month:
            latest_url = _url_with_query_params(
                api_url,
                {
                    "network": resolver.get("network") or "",
                    "month": month,
                    "year": year,
                    "plan": resolver.get("plan") or "",
                },
            )
            payload = await _fetch_json_value(
                latest_url,
                max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
                session=session,
            )
            resolved_from_url = latest_url
    targets = _triples_mtt_targets_from_payload(
        source, payload, resolved_from_url=resolved_from_url, resolver=resolver
    )
    if not targets:
        raise ValueError(f"no Triple-S MTT targets found for {url}")
    return targets


def _xml_child_text(element: ElementTree.Element, child_name: str) -> str:
    for child in list(element):
        if child.tag.rsplit("}", 1)[-1] == child_name:
            return _clean_text(child.text)
    return ""


def _healthspace_session_id_from_html(html_text: str | None) -> str | None:
    for pattern in (
        r"HealthspaceSessionId['\"]\s*,\s*['\"](?P<session>[^'\"]+)['\"]",
        r"HealthspaceSessionId['\"]\]\s*=\s*['\"](?P<session>[^'\"]+)['\"]",
    ):
        match = re.search(pattern, html_text or "", flags=re.I)
        if match:
            return _clean_text(match.group("session")) or None
    return None


def _healthspace_execute_soap_envelope(
    *,
    session_id: str,
    operation_id: str,
    parameters_xml: str = "<hslist />",
) -> str:
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<soap:Envelope xmlns:ns0="https://www.p2phealthcare.com" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xmlns:xsd="http://www.w3.org/2001/XMLSchema" '
        'xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
        "<soap:Body><ns0:Execute>"
        f"<ns0:sessionId>{html.escape(session_id)}</ns0:sessionId>"
        f"<ns0:operationId>{html.escape(operation_id)}</ns0:operationId>"
        f"<ns0:parameters>{parameters_xml}</ns0:parameters>"
        "</ns0:Execute></soap:Body></soap:Envelope>"
    )


def _healthspace_mrf_targets_from_soap(
    source: dict[str, Any],
    soap_text: str,
    *,
    resolved_from_url: str,
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    root = ElementTree.fromstring((soap_text or "").lstrip("\ufeff"))
    resolver_type = str(resolver.get("type") or "healthspace_machine_readable_files")
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for item in root.iter():
        if item.tag.rsplit("}", 1)[-1] != "MachineReadableFile":
            continue
        url = _clean_text(item.attrib.get("FilePathURL"))
        file_name = (
            _clean_text(item.attrib.get("FileName")) or Path(urlsplit(url).path).name
        )
        if not url:
            continue
        file_type = _mrf_file_type_from_text(url, file_name)
        if not file_type and _looks_non_tic_mrf_reference(url, file_name):
            continue
        if not file_type:
            continue
        target_kind = (
            "toc_json"
            if file_type == "table-of-contents"
            and urlsplit(url).path.lower().endswith((".json", ".json.gz"))
            else "file_reference"
        )
        company_id = _clean_text(item.attrib.get("CompanyId"))
        company_name = _clean_text(item.attrib.get("CompanyName"))
        plan_info = []
        if company_name:
            plan_info.append(
                {
                    "plan_id": company_name,
                    "plan_id_type": "healthspace_company_name",
                    "plan_market_type": "group",
                    "plan_name": company_name,
                }
            )
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        targets.append(
            CrawlTarget(
                source=source,
                url=url,
                label=company_name or _mrf_file_plan_label(file_name) or file_name,
                resolved_from_url=resolved_from_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": target_kind,
                    "target_file_type": file_type,
                    "container_format": _container_format(url),
                    "company_id": company_id,
                    "company_name": company_name,
                    "file_name": file_name,
                    "plan_info": plan_info,
                },
            )
        )
    return targets


async def _resolve_healthspace_machine_readable_files(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    page_html = await _fetch_text(
        url,
        max_bytes=int(resolver.get("page_max_bytes") or 1024 * 1024),
        session=session,
    )
    session_id = _healthspace_session_id_from_html(page_html)
    if not session_id:
        raise ValueError("Healthspace public page did not expose a session id")
    service_url = urljoin(
        url, str(resolver.get("service_path") or "/Healthspace/Healthspace.svc")
    )
    operation_id = str(
        resolver.get("operation_id") or "P2PHC.Document.GetMachineReadableFiles"
    )
    soap_text = await _post_text(
        service_url,
        _healthspace_execute_soap_envelope(
            session_id=session_id,
            operation_id=operation_id,
            parameters_xml=str(resolver.get("parameters_xml") or "<hslist />"),
        ),
        headers={
            "Content-Type": "text/xml",
            "SOAPAction": "https://www.p2phealthcare.com/IHealthspace/Execute",
        },
        max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
        session=session,
    )
    targets = _healthspace_mrf_targets_from_soap(
        source, soap_text, resolved_from_url=service_url, resolver=resolver
    )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Healthspace MRF targets found for {url}")
    return targets


def _healthez_outbound_file_type(url: str | None, label: str | None = None) -> str | None:
    parsed = urlsplit(str(url or ""))
    query = {key.lower(): value for key, value in parse_qsl(parsed.query)}
    file_type = str(query.get("filetype") or "").strip().lower()
    if file_type == "innetwork":
        return "in-network"
    if file_type == "outofnetwork":
        return "allowed-amounts"
    return _mrf_file_type_from_text(url, label)


def _healthez_normalized_outbound_url(url: str | None) -> str | None:
    parsed = urlsplit(str(url or ""))
    if not parsed.scheme or not parsed.netloc or not parsed.path.endswith(
        "/api/outbound/latest"
    ):
        return None
    pairs = parse_qsl(parsed.query, keep_blank_values=True)
    normalized: list[tuple[str, str]] = []
    has_network = any(key.lower() == "network" for key, _value in pairs)
    pending_network: str | None = None
    for key, value in pairs:
        if key.lower() == "groupname" and "=" in value:
            group_name, network = value.rsplit("=", 1)
            if network.upper() in {"AP", "AE"}:
                value = group_name
                if not has_network:
                    pending_network = network.upper()
        normalized.append((key, value))
    if pending_network:
        normalized.append(("network", pending_network))
    return parsed._replace(query=urlencode(normalized)).geturl()


def _healthez_plan_label(url: str, label: str | None) -> str:
    query = {key.lower(): value for key, value in parse_qsl(urlsplit(url).query)}
    network = str(query.get("network") or "").strip().upper()
    if network in {"AP", "AE"}:
        return f"HealthEZ {network}"
    cleaned = _clean_text(label)
    if cleaned and "machine readable" not in cleaned.lower():
        return cleaned
    return "HealthEZ"


def _healthez_targets_from_html(
    source: dict[str, Any],
    html_text: str,
    *,
    base_url: str,
    resolver_type: str,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = _healthez_normalized_outbound_url(str(candidate.get("url") or ""))
        if not url:
            continue
        file_type = _healthez_outbound_file_type(url, str(candidate.get("label") or ""))
        if file_type not in {"in-network", "allowed-amounts"}:
            continue
        key = _canonical_or_none(url) or url
        if key in seen:
            continue
        seen.add(key)
        label = _healthez_plan_label(url, str(candidate.get("label") or ""))
        targets.append(
            CrawlTarget(
                source=source,
                url=url,
                label=label,
                resolved_from_url=base_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": "file_reference",
                    "target_file_type": file_type,
                    "container_format": "zip",
                    "plan_info": _plan_info_from_label(label),
                },
            )
        )
    return targets


async def _resolve_healthez_benefits_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "healthez_benefits_mrf")
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    targets = _healthez_targets_from_html(
        source, html_text, base_url=url, resolver_type=resolver_type
    )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no HealthEZ MRF targets found for {url}")
    return targets


def _s3_xml_listing_targets_from_xml(
    source: dict[str, Any],
    xml_text: str,
    *,
    listing_url: str,
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    root = ElementTree.fromstring((xml_text or "").lstrip("\ufeff"))
    resolver_type = str(resolver.get("type") or "s3_xml_listing")
    public_base_url = str(resolver.get("public_base_url") or listing_url).rstrip("/")
    include_prefixes = tuple(
        str(item).strip()
        for item in (resolver.get("include_prefixes") or ())
        if str(item).strip()
    )
    exclude_prefixes = tuple(
        str(item).strip()
        for item in (resolver.get("exclude_prefixes") or ())
        if str(item).strip()
    )
    targets: list[CrawlTarget] = []
    seen: set[str] = set()
    for item in root.iter():
        if item.tag.rsplit("}", 1)[-1] != "Contents":
            continue
        object_key = _xml_child_text(item, "Key")
        if not object_key or object_key.endswith("/"):
            continue
        if include_prefixes and not object_key.startswith(include_prefixes):
            continue
        if exclude_prefixes and object_key.startswith(exclude_prefixes):
            continue
        file_url = f"{public_base_url}/{quote(object_key, safe='/')}"
        label = Path(object_key).name
        file_type = _mrf_file_type_from_text(file_url, label)
        if not file_type:
            continue
        key = _canonical_or_none(file_url) or file_url
        if key in seen:
            continue
        seen.add(key)
        targets.append(
            CrawlTarget(
                source=source,
                url=file_url,
                label=_mrf_file_plan_label(label) or label,
                resolved_from_url=listing_url,
                metadata={
                    "resolver": resolver_type,
                    "target_kind": "file_reference",
                    "target_file_type": file_type,
                    "container_format": _container_format(file_url),
                    "object_key": object_key,
                    "content_length": _xml_child_text(item, "Size"),
                    "etag": _xml_child_text(item, "ETag").strip('"'),
                    "last_modified": _xml_child_text(item, "LastModified"),
                },
            )
        )
    return targets


async def _resolve_s3_xml_listing(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    listing_urls = [
        str(item).strip()
        for item in (resolver.get("listing_urls") or [url])
        if str(item).strip()
    ]
    targets: list[CrawlTarget] = []
    for listing_url in listing_urls:
        xml_text = await _fetch_text(
            listing_url,
            max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
            session=session,
        )
        targets.extend(
            _s3_xml_listing_targets_from_xml(
                source, xml_text, listing_url=listing_url, resolver=resolver
            )
        )
    targets = _dedupe_crawl_targets_by_url(targets)
    targets.sort(
        key=lambda target: str((target.metadata or {}).get("last_modified") or ""),
        reverse=True,
    )
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no S3 XML MRF listing targets found for {url}")
    return targets


async def _resolve_cigna_static_mrf_lookup(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(
        url,
        max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
        session=session,
    )
    lookup_urls = _cigna_lookup_urls_from_html(
        html_text, base_url=url, resolver=resolver
    )
    if not lookup_urls:
        raise ValueError(f"no Cigna MRF lookup URLs found in {url}")
    lookup_max_bytes = int(resolver.get("lookup_max_bytes") or 2 * 1024 * 1024)
    targets: list[CrawlTarget] = []
    for lookup_url in lookup_urls:
        payload = await _fetch_json(
            lookup_url, max_bytes=lookup_max_bytes, session=session
        )
        targets.extend(
            _parse_cigna_lookup_targets(
                payload, lookup_url=lookup_url, source=source, resolver=resolver
            )
        )
    if not targets:
        raise ValueError(f"no Cigna MRF index URLs found from {url}")
    return targets


def _healthsparq_public_params(url: str) -> dict[str, str]:
    raw = html.unescape(str(url or ""))
    parsed = urlsplit(raw)
    params: dict[str, str] = {}
    fragment = parsed.fragment
    match = re.search(r"^/?(?:one|public)/(?P<params>[^/?#]+)(?P<tail>.*)$", fragment)
    if match:
        route_params = match.group("params").split("/", 1)[0]
        params.update(
            {
                key: value
                for key, value in parse_qsl(route_params, keep_blank_values=False)
                if key and value
            }
        )
        tail = match.group("tail") or ""
        if "?" in tail:
            query_text = tail.split("?", 1)[1]
            params.update(
                {
                    key: value
                    for key, value in parse_qsl(query_text, keep_blank_values=False)
                    if key and value
                }
            )
    if not params:
        params.update(
            {
                key: value
                for key, value in parse_qsl(parsed.query, keep_blank_values=False)
                if key and value
            }
        )
    if "insurerCode" not in params or "brandCode" not in params:
        raise ValueError(
            "HealthSparq public URL must include insurerCode and brandCode"
        )
    return params


def _url_origin(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("URL origin is required")
    return f"{parsed.scheme}://{parsed.netloc}"


def _healthsparq_service_url(
    source_url: str, resolver: dict[str, Any], path_key: str
) -> str:
    service_path = str(
        resolver.get("public_service_path") or "/healthsparq/public/service"
    ).strip()
    path = str(resolver.get(path_key) or "").strip()
    if not path:
        raise ValueError(f"HealthSparq resolver missing {path_key}")
    return urljoin(
        _url_origin(source_url) + "/", f"{service_path.strip('/')}/{path.lstrip('/')}"
    )


def _healthsparq_tenant(
    params: dict[str, str], resolver: dict[str, Any] | None = None
) -> str:
    resolver = resolver or {}
    overrides = (
        resolver.get("tenant_overrides")
        if isinstance(resolver.get("tenant_overrides"), dict)
        else {}
    )
    insurer_code = str(params.get("insurerCode") or "").strip()
    for key in (insurer_code, insurer_code.upper(), insurer_code.lower()):
        override = str(overrides.get(key) or "").strip()
        if override:
            return override
    tenant = str(params.get("insurerCode") or "").strip().lower()
    tenant = re.sub(r"_i$", "", tenant)
    return re.sub(r"[^a-z0-9]+", "-", tenant).strip("-")


def _healthsparq_direct_metadata_url(
    resolver: dict[str, Any], params: dict[str, str]
) -> str | None:
    if params.get("reportingEntityType"):
        return None
    template = str(resolver.get("metadata_url_template") or "").strip()
    if not template:
        return None
    tenant = _healthsparq_tenant(params, resolver)
    if not tenant:
        return None
    return template.format(
        tenant=tenant, insurerCode=params["insurerCode"], brandCode=params["brandCode"]
    )


def _healthsparq_target(
    source: dict[str, Any],
    metadata_url: str,
    resolved_from_url: str,
    params: dict[str, str],
) -> CrawlTarget:
    return CrawlTarget(
        source=source,
        url=metadata_url,
        label=str(source.get("display_name") or params["brandCode"]),
        resolved_from_url=resolved_from_url,
        metadata={
            "resolver": "healthsparq_public_mrf",
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "insurer_code": params["insurerCode"],
            "brand_code": params["brandCode"],
            "reporting_entity_type": params.get("reportingEntityType"),
            "search_term": params.get("searchTerm"),
        },
    )


def _healthsparq_plan_engine_hash(
    file_item: dict[str, Any], plan: dict[str, Any]
) -> str | None:
    plan_name = _clean_text(plan.get("planName"))
    plan_id = str(plan.get("planId") or "").strip()
    market_type = str(plan.get("planMarketType") or "").strip()
    if not plan_name or not plan_id or not market_type:
        return None
    return semantic_hash(
        {
            "reporting_entity_name": file_item.get("reportingEntityName"),
            "reporting_entity_type": file_item.get("reportingEntityType"),
            "plan_id": plan_id,
            "plan_id_type": plan.get("planIdType"),
            "market_type": market_type,
            "plan_name": plan_name,
        },
        domain="healthsparq_plan",
    )


def _healthsparq_plan_info(file_item: dict[str, Any]) -> list[dict[str, Any]]:
    reporting_plans = [
        plan
        for plan in (file_item.get("reportingPlans") or [])
        if isinstance(plan, dict)
    ]
    return [
        {
            "plan_id": plan.get("planId"),
            "plan_id_type": plan.get("planIdType"),
            "plan_market_type": plan.get("planMarketType"),
            "plan_name": plan.get("planName"),
            "engine_plan_hash": _healthsparq_plan_engine_hash(file_item, plan),
        }
        for plan in reporting_plans
    ]


def _healthsparq_file_matches_query(
    file_item: dict[str, Any], query: str | None
) -> bool:
    if not query:
        return True
    return _search_values_match_query(
        [
            file_item.get("fileName"),
            file_item.get("filePath"),
            file_item.get("reportingEntityName"),
            file_item.get("reportingEntityType"),
            *(
                value
                for plan in (file_item.get("reportingPlans") or [])
                if isinstance(plan, dict)
                for value in (
                    plan.get("planName"),
                    plan.get("planId"),
                    plan.get("planIdType"),
                    plan.get("planMarketType"),
                )
            ),
        ],
        query,
    )


def _healthsparq_targets_from_metadata(
    source: dict[str, Any],
    metadata_url: str,
    payload: dict[str, Any],
    *,
    resolved_from_url: str,
    params: dict[str, str],
) -> list[CrawlTarget]:
    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list):
        return []
    target_query = _source_target_payer_query(source)
    targets_by_key: dict[tuple[str, str], CrawlTarget] = {}
    for file_item in files:
        if not isinstance(file_item, dict):
            continue
        if not _healthsparq_file_matches_query(file_item, target_query):
            continue
        file_url = _healthsparq_file_url(metadata_url, file_item.get("filePath"))
        if not file_url:
            continue
        file_type = _healthsparq_file_type(file_item.get("fileSchema"))
        if not file_type or file_type == "unknown":
            continue
        target_kind = "file_reference"
        file_name = _clean_text(file_item.get("fileName"))
        label = (
            file_name
            or _clean_text(file_item.get("reportingEntityName"))
            or str(source.get("display_name") or params["brandCode"])
        )
        metadata = {
            "resolver": "healthsparq_public_mrf",
            "target_kind": target_kind,
            "target_file_type": file_type,
            "container_format": _container_format(file_url),
            "insurer_code": params["insurerCode"],
            "brand_code": params["brandCode"],
            "reporting_entity_type": file_item.get("reportingEntityType")
            or params.get("reportingEntityType"),
            "reporting_entity_name": file_item.get("reportingEntityName"),
            "search_term": params.get("searchTerm"),
            "source_format": "healthsparq_metadata",
            "metadata_url": metadata_url,
            "healthsparq_public_url": resolved_from_url,
            "file_path": file_item.get("filePath"),
            "file_schema": file_item.get("fileSchema"),
            "last_updated_on": file_item.get("lastUpdatedOn"),
            "plan_info": _healthsparq_plan_info(file_item),
        }
        key = (target_kind, _canonical_or_none(file_url) or file_url)
        targets_by_key[key] = CrawlTarget(
            source=source,
            url=file_url,
            label=label,
            resolved_from_url=metadata_url,
            metadata=metadata,
        )
    return list(targets_by_key.values())


async def _resolve_healthsparq_public_mrf(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    params = _healthsparq_public_params(url)
    metadata_url = _healthsparq_direct_metadata_url(resolver, params)
    if metadata_url:
        await _assert_fetch_url_allowed(metadata_url)
        try:
            payload = await _fetch_json(
                metadata_url,
                max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
                session=session,
            )
        except Exception:
            return [_healthsparq_target(source, metadata_url, url, params)]
        targets = _healthsparq_targets_from_metadata(
            source,
            metadata_url,
            payload,
            resolved_from_url=url,
            params=params,
        )
        if targets:
            max_targets = _as_int(resolver.get("max_targets"))
            return targets[:max_targets] if max_targets else targets
        return [_healthsparq_target(source, metadata_url, url, params)]
    login_url = _healthsparq_service_url(url, resolver, "login_path")
    login_query = {"_": str(int(_utc_now().timestamp() * 1000)), **params}
    await _fetch_json(
        f"{login_url}?{urlencode(login_query)}",
        max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    mrf_all_url = _healthsparq_service_url(url, resolver, "mrf_all_path")
    payload = {
        key: value
        for key, value in params.items()
        if key
        in {
            "brandCode",
            "insurerCode",
            "reportingEntityType",
            "searchTerm",
            "productCode",
        }
        and value
    }
    payload = await _post_json(
        mrf_all_url,
        payload,
        max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    metadata_url = str(payload.get("url") or "").strip()
    if not metadata_url:
        raise ValueError("HealthSparq public MRF API did not return a metadata URL")
    try:
        metadata_payload = await _fetch_json(
            metadata_url,
            max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
            session=session,
        )
    except Exception:
        return [_healthsparq_target(source, metadata_url, mrf_all_url, params)]
    targets = _healthsparq_targets_from_metadata(
        source,
        metadata_url,
        metadata_payload,
        resolved_from_url=mrf_all_url,
        params=params,
    )
    if targets:
        max_targets = _as_int(resolver.get("max_targets"))
        return targets[:max_targets] if max_targets else targets
    return [_healthsparq_target(source, metadata_url, mrf_all_url, params)]


async def _fetch_json_with_headers(
    url: str,
    *,
    headers: dict[str, str],
    max_bytes: int,
    session: aiohttp.ClientSession,
) -> dict[str, Any]:
    await _assert_fetch_url_allowed(url)
    async with session.get(
        url, headers=headers, allow_redirects=True, **_request_ssl_kwargs(url)
    ) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        chunks: list[bytes] = []
        total = 0
        async for chunk in resp.content.iter_chunked(64 * 1024):
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"response exceeds {max_bytes} byte discovery limit")
            chunks.append(chunk)
        payload = json.loads(
            _decode_response_body(b"".join(chunks), charset=resp.charset or "utf-8")
        )
    if not isinstance(payload, dict):
        raise ValueError("expected JSON object")
    return payload


def _providence_toc_targets_from_payload(
    source: dict[str, Any],
    payload: dict[str, Any],
    *,
    api_url: str,
    resolver_type: str,
) -> list[CrawlTarget]:
    targets: list[CrawlTarget] = []
    for group in payload.get("groups") or []:
        if not isinstance(group, dict):
            continue
        group_id = _clean_text(group.get("group-id"))
        group_name = _clean_text(group.get("group-name"))
        for item in group.get("tocs") or []:
            if not isinstance(item, dict):
                continue
            toc_url = _clean_text(item.get("TOC_URL"))
            if not _looks_html_mrf_toc_url(toc_url, Path(urlsplit(toc_url).path).name):
                continue
            metadata = {
                "resolver": resolver_type,
                "target_kind": "toc_json",
                "target_file_type": "table-of-contents",
                "group_id": group_id,
                "group_name": group_name,
                "providence_api_url": api_url,
                "plan_info": [
                    {
                        "plan_id": group_id or None,
                        "plan_id_type": "providence_group_id" if group_id else None,
                        "plan_market_type": "group",
                        "plan_name": group_name or None,
                    }
                ],
            }
            targets.append(
                CrawlTarget(
                    source=source,
                    url=toc_url,
                    label=Path(urlsplit(toc_url).path).name or group_name,
                    resolved_from_url=api_url,
                    metadata={
                        key: value
                        for key, value in metadata.items()
                        if value not in (None, "", [])
                    },
                )
            )
    return targets


async def _resolve_providence_mrf_api(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    resolver_type = str(resolver.get("type") or "providence_mrf_api")
    config_url = urljoin(url, str(resolver.get("config_path") or "/config.json"))
    config = await _fetch_json(
        config_url,
        max_bytes=int(resolver.get("config_max_bytes") or 1024 * 1024),
        session=session,
    )
    api_endpoint = str(config.get("API_ENDPOINT") or "").strip()
    api_key = str(config.get("X_API_KEY") or "").strip()
    if not api_endpoint or not api_key:
        raise ValueError("Providence MRF config did not include API endpoint/key")
    max_bytes = int(resolver.get("max_bytes") or 10 * 1024 * 1024)
    headers = {"X-API-Key": api_key}
    group_queries = [
        str(item).strip()
        for item in (resolver.get("group_queries") or ["Individual and Family Plans"])
        if str(item).strip()
    ]
    groups_by_id: dict[str, dict[str, Any]] = {}
    for query in group_queries:
        group_url = urljoin(api_endpoint.rstrip("/") + "/", "group/")
        group_url = f"{group_url}?{urlencode({'groupname': query})}"
        group_payload = await _fetch_json_with_headers(
            group_url, headers=headers, max_bytes=max_bytes, session=session
        )
        for group in group_payload.get("groups") or []:
            if not isinstance(group, dict):
                continue
            group_id = _clean_text(group.get("group-id"))
            if group_id:
                groups_by_id.setdefault(group_id, group)
    max_groups = _as_int(resolver.get("max_groups")) or 25
    targets: list[CrawlTarget] = []
    for group_id in list(groups_by_id)[:max_groups]:
        toc_url = urljoin(api_endpoint.rstrip("/") + "/", "toc/")
        toc_url = f"{toc_url}?{urlencode({'groupid': group_id})}"
        toc_payload = await _fetch_json_with_headers(
            toc_url, headers=headers, max_bytes=max_bytes, session=session
        )
        targets.extend(
            _providence_toc_targets_from_payload(
                source,
                toc_payload,
                api_url=toc_url,
                resolver_type=resolver_type,
            )
        )
    targets = _dedupe_crawl_targets_by_url(targets)
    max_targets = _as_int(resolver.get("max_targets"))
    if max_targets and max_targets > 0:
        targets = targets[:max_targets]
    if not targets:
        raise ValueError(f"no Providence MRF API targets found for {url}")
    return targets


def _looks_direct_toc_url(url: str | None) -> bool:
    parsed = urlsplit(str(url or ""))
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    if host == "d3oz7y1cwsecds.cloudfront.net" and path == "/member-prod/bcbsal":
        return True
    if path.endswith((".json", ".json.gz")) and (
        _mrf_file_type_from_text(url) == "table-of-contents"
    ):
        return True
    if "sapphiremrfhub.com" in host and "/tocs/" in path:
        return True
    if "name=table-of-contents" in query and "ext=json" in query:
        return True
    if "/toc-json" in path:
        return True
    file_name = Path(path).name.replace("_", "-")
    return (
        "." not in file_name
        and file_name.endswith(("-index", "-toc"))
        and "/mrf/" in path
    )


async def _crawl_targets_for_source(
    source: dict[str, Any],
    url: str,
    session: aiohttp.ClientSession,
    *,
    target_limit: int | None = None,
) -> list[CrawlTarget]:
    platform = source.get("hosting_platform") or classify_hosting_platform(url)
    resolver = _platform_resolver_config(str(platform) if platform else None)
    if resolver and target_limit and target_limit > 0:
        resolver = dict(resolver)
        existing_max_targets = _as_int(resolver.get("max_targets"))
        resolver["max_targets"] = (
            min(existing_max_targets, target_limit)
            if existing_max_targets
            else target_limit
        )
    resolver_type = str(resolver.get("type") or "").strip()
    if resolver_type == "bcbsma_monthly_tocs":
        return _bcbsma_monthly_toc_targets(source, url, resolver)
    if resolver_type == "monthly_toc_templates":
        return _monthly_toc_targets(source, url, resolver)
    if resolver_type == "azure_mrf_listing":
        return await _resolve_azure_mrf_listing(source, url, resolver, session)
    if resolver_type == "triples_mtt_api":
        return await _resolve_triples_mtt_api(source, url, resolver, session)
    if resolver_type == "s3_xml_listing":
        return await _resolve_s3_xml_listing(source, url, resolver, session)
    if resolver_type == "cigna_static_mrf_lookup":
        return await _resolve_cigna_static_mrf_lookup(source, url, resolver, session)
    if resolver_type == "bcbs_global_solutions_mrf":
        return await _resolve_bcbs_global_solutions_mrf(source, url, resolver, session)
    if resolver_type == "bcbs_asomrf_filelist":
        return await _resolve_bcbs_asomrf_filelist(source, url, resolver, session)
    if resolver_type == "meritain_mrf_search":
        return await _resolve_meritain_mrf_search(source, url, resolver, session)
    if resolver_type == "healthcarebluebook_mrf":
        return await _resolve_healthcarebluebook_mrf(source, url, resolver, session)
    if resolver_type == "ebms_caa_directory":
        return await _resolve_ebms_caa_directory(source, url, resolver, session)
    if resolver_type == "html_mrf_with_healthcarebluebook":
        return await _resolve_html_mrf_with_healthcarebluebook(
            source, url, resolver, session
        )
    if resolver_type == "healthgram_network_index":
        return await _resolve_healthgram_network_index(source, url, resolver, session)
    if resolver_type == "anthem_s3_mrf":
        return await _resolve_anthem_s3_mrf(source, url, resolver, session)
    if resolver_type == "hcsc_asomrf_landing":
        return await _resolve_hcsc_asomrf_landing(source, url, resolver, session)
    if resolver_type == "point32_azure_mrf_directory":
        return await _resolve_point32_azure_mrf_directory(
            source, url, resolver, session
        )
    if resolver_type == "html_delegated_mrf_links":
        return await _resolve_html_delegated_mrf_links(source, url, resolver, session)
    if resolver_type == "midlandschoice_mrf":
        return await _resolve_midlandschoice_mrf(source, url, resolver, session)
    if resolver_type == "html_mrf_links":
        return await _resolve_html_mrf_links(source, url, resolver, session)
    if resolver_type == "socrata_data_json_mrf_catalog":
        return await _resolve_socrata_data_json_mrf_catalog(
            source, url, resolver, session
        )
    if resolver_type == "json_mrf_directory_links":
        return await _resolve_json_mrf_directory_links(source, url, resolver, session)
    if resolver_type == "healthspace_machine_readable_files":
        return await _resolve_healthspace_machine_readable_files(
            source, url, resolver, session
        )
    if resolver_type == "humana_pct_file_list":
        return await _resolve_humana_pct_file_list(source, url, resolver, session)
    if resolver_type == "fchn_payor_search":
        return await _resolve_fchn_payor_search(source, url, resolver, session)
    if resolver_type == "viva_health_mrf":
        return await _resolve_viva_health_mrf(source, url, resolver, session)
    if resolver_type == "healthez_benefits_mrf":
        return await _resolve_healthez_benefits_mrf(source, url, resolver, session)
    if resolver_type == "payercompass_mrf":
        return await _resolve_payercompass_mrf(source, url, resolver, session)
    if resolver_type == "webtpa_mrf_api":
        return await _resolve_webtpa_mrf_api(source, url, resolver, session)
    if resolver_type == "cmstic_file_info":
        return await _resolve_cmstic_file_info(source, url, resolver, session)
    if resolver_type == "cmstic_keyed_toc_redirect":
        return await _resolve_cmstic_keyed_toc_redirect(
            source, url, resolver, session
        )
    if resolver_type == "direct_toc":
        target = _direct_toc_crawl_target(source, url, resolver=resolver_type)
        if target:
            return [target]
        raise ValueError(f"no direct MRF TOC target found for {url}")
    if resolver_type == "direct_mrf_body":
        target = _direct_mrf_body_crawl_target(source, url, resolver=resolver_type)
        if target:
            return [target]
        raise ValueError(f"no direct MRF body target found for {url}")
    if resolver_type == "github_repo_mrf_tree":
        return await _resolve_github_repo_mrf(source, url, resolver, session)
    if resolver_type == "auxiant_wordpress_directory":
        return await _resolve_auxiant_wordpress_directory(
            source, url, resolver, session
        )
    if resolver_type == "healthsparq_public_mrf":
        return await _resolve_healthsparq_public_mrf(source, url, resolver, session)
    if resolver_type == "providence_mrf_api":
        return await _resolve_providence_mrf_api(source, url, resolver, session)
    if resolver_type == "magnacare_transparency_mrf":
        return await _resolve_magnacare_transparency_mrf(
            source, url, resolver, session
        )
    if resolver_type == "mymedicalshopper_talon_mrf":
        return await _resolve_mymedicalshopper_talon_mrf(source, url, resolver, session)
    if resolver_type == "asr_health_benefits_mrf":
        return _resolve_asr_health_benefits_mrf(source, url, resolver)
    if resolver_type == "highmark_hmhs_script":
        script_path = str(resolver.get("script_path") or "/js/script.js")
        script_url = urljoin(url, script_path)
        script_text = await _fetch_text(
            script_url,
            max_bytes=int(resolver.get("max_bytes") or 1024 * 1024),
            session=session,
        )
        targets = _parse_highmark_hmhs_script(script_text, base_url=url)
        if not targets:
            raise ValueError(f"no Highmark HMHS index links found in {script_url}")
        return [
            CrawlTarget(
                source=source,
                url=str(target["url"]),
                label=str(target.get("label") or source.get("display_name") or ""),
                resolved_from_url=script_url,
                metadata={
                    "resolver": resolver_type,
                    "region": target.get("region"),
                    "raw_path": target.get("raw_path"),
                    "rendered_path": target.get("rendered_path"),
                },
            )
            for target in targets
        ]
    if resolver_type == "uhc_provider_mrf_files":
        listing_url = _uhc_provider_mrf_api_url(url)
        listing = await _fetch_json(
            listing_url,
            max_bytes=int(resolver.get("max_bytes") or 10 * 1024 * 1024),
            session=session,
        )
        targets = _uhc_provider_mrf_targets_from_payload(
            source,
            listing,
            listing_url=listing_url,
            resolver_type=resolver_type,
            max_targets=_as_int(resolver.get("max_targets")),
        )
        if not targets:
            raise ValueError(f"no UHC provider MRF files found for {url}")
        return targets
    if resolver_type == "uhc_blob_listing":
        host = _domain(url) or ""
        configured_paths = (
            resolver.get("optum_path_templates")
            if "optum.com" in host
            else resolver.get("path_templates")
        )
        paths = [str(item) for item in (configured_paths or ()) if str(item).strip()]
        targets: list[CrawlTarget] = []
        for path in paths:
            listing_url = urljoin(url, path)
            listing = await _fetch_json(
                listing_url,
                max_bytes=int(resolver.get("max_bytes") or 64 * 1024 * 1024),
                session=session,
            )
            for target in _parse_uhc_blob_listing(listing):
                targets.append(
                    CrawlTarget(
                        source=source,
                        url=str(target["url"]),
                        label=str(
                            target.get("label") or source.get("display_name") or ""
                        ),
                        resolved_from_url=listing_url,
                        metadata={
                            "resolver": resolver_type,
                            "blob_name": target.get("name"),
                            "blob_size": target.get("size"),
                        },
                    )
                )
        if not targets:
            raise ValueError(f"no UHC blob index links found for {url}")
        return targets
    if resolver_type == "sapphire_html_tocs":
        if _looks_direct_toc_url(url):
            return [
                CrawlTarget(
                    source=source,
                    url=url,
                    label=str(source.get("display_name") or ""),
                    resolved_from_url=url,
                    metadata={
                        "resolver": resolver_type,
                        "file_name": Path(urlsplit(url).path).name,
                        "payer_name": source.get("display_name"),
                    },
                )
            ]
        html_text = await _fetch_text(
            url,
            max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024),
            session=session,
        )
        targets = _parse_sapphire_toc_links(html_text, base_url=url)
        if not targets:
            targets = await _resolve_sapphire_static_query_toc_links(
                url, resolver, session
            )
        if not targets:
            raise ValueError(f"no Sapphire TOC links found for {url}")
        return [
            CrawlTarget(
                source=source,
                url=str(target["url"]),
                label=str(target.get("label") or source.get("display_name") or ""),
                resolved_from_url=url,
                metadata={
                    "resolver": resolver_type,
                    "file_name": target.get("file_name"),
                    "payer_name": target.get("payer_name"),
                },
            )
            for target in targets
        ]
    if resolver_type == "anthem_s3_mrf":
        return await _resolve_anthem_s3_mrf(source, url, resolver, session)
    if resolver_type == "hcsc_asomrf_landing":
        return await _resolve_hcsc_asomrf_landing(source, url, resolver, session)
    if resolver_type == "point32_azure_mrf_directory":
        return await _resolve_point32_azure_mrf_directory(
            source, url, resolver, session
        )
    if resolver_type == "html_delegated_mrf_links":
        return await _resolve_html_delegated_mrf_links(source, url, resolver, session)
    if resolver_type == "html_mrf_links":
        return await _resolve_html_mrf_links(source, url, resolver, session)
    if resolver_type == "socrata_data_json_mrf_catalog":
        return await _resolve_socrata_data_json_mrf_catalog(
            source, url, resolver, session
        )
    if resolver_type == "json_mrf_directory_links":
        return await _resolve_json_mrf_directory_links(source, url, resolver, session)
    if resolver_type == "healthspace_machine_readable_files":
        return await _resolve_healthspace_machine_readable_files(
            source, url, resolver, session
        )
    if resolver_type == "humana_pct_file_list":
        return await _resolve_humana_pct_file_list(source, url, resolver, session)
    if resolver_type == "fchn_payor_search":
        return await _resolve_fchn_payor_search(source, url, resolver, session)
    if resolver_type == "payercompass_mrf":
        return await _resolve_payercompass_mrf(source, url, resolver, session)
    if resolver_type == "webtpa_mrf_api":
        return await _resolve_webtpa_mrf_api(source, url, resolver, session)
    if resolver_type == "cmstic_file_info":
        return await _resolve_cmstic_file_info(source, url, resolver, session)
    if resolver_type == "cmstic_keyed_toc_redirect":
        return await _resolve_cmstic_keyed_toc_redirect(
            source, url, resolver, session
        )
    if resolver_type == "direct_toc":
        target = _direct_toc_crawl_target(source, url, resolver=resolver_type)
        if target:
            return [target]
        raise ValueError(f"no direct MRF TOC target found for {url}")
    if resolver_type == "direct_mrf_body":
        target = _direct_mrf_body_crawl_target(source, url, resolver=resolver_type)
        if target:
            return [target]
        raise ValueError(f"no direct MRF body target found for {url}")
    direct_toc_target = _direct_toc_crawl_target(source, url)
    if direct_toc_target:
        return [direct_toc_target]
    direct_body_target = _direct_mrf_body_crawl_target(source, url)
    if direct_body_target:
        return [direct_body_target]
    if not _looks_direct_toc_url(url):
        html_text = await _fetch_text(url, max_bytes=5 * 1024 * 1024, session=session)
        html_targets = _parse_html_mrf_links(html_text, base_url=url)
        if html_targets:
            return [
                CrawlTarget(
                    source=source,
                    url=str(target["url"]),
                    label=str(target.get("label") or source.get("display_name") or ""),
                    resolved_from_url=url,
                    metadata={
                        "resolver": target.get("resolver"),
                        "target_kind": target.get("target_kind"),
                        "target_file_type": target.get("target_file_type"),
                        "container_format": target.get("container_format"),
                        "html_attr": target.get("html_attr"),
                        "plan_info": target.get("plan_info"),
                    },
                )
                for target in html_targets
            ]
        delegated_targets: list[CrawlTarget] = []
        for delegated_url in _delegated_mrf_source_urls_from_html(
            html_text, base_url=url
        ):
            delegated_platform = classify_hosting_platform(delegated_url)
            if not delegated_platform:
                continue
            delegated_source = {**source, "hosting_platform": delegated_platform}
            nested_targets = await _crawl_targets_for_source(
                delegated_source,
                delegated_url,
                session,
                target_limit=target_limit,
            )
            for target in nested_targets:
                metadata = dict(target.metadata or {})
                metadata.setdefault("delegated_source_url", delegated_url)
                metadata.setdefault("delegated_source_platform", delegated_platform)
                delegated_targets.append(
                    CrawlTarget(
                        source=source,
                        url=target.url,
                        label=target.label,
                        resolved_from_url=url,
                        metadata=metadata,
                    )
                )
                if target_limit and target_limit > 0 and len(delegated_targets) >= target_limit:
                    return delegated_targets[:target_limit]
        if delegated_targets:
            return _dedupe_crawl_targets_by_url(delegated_targets)
        return []
    return [
        CrawlTarget(
            source=source,
            url=url,
            label=str(source.get("display_name") or ""),
            metadata={"resolver": None},
        )
    ]


def _crawl_skipped_observation(
    source: dict[str, Any], url: str, reason: str, run_id: str | None
) -> dict[str, Any]:
    checked_at = _utc_now()
    return {
        "observation_id": _id(
            "mrfurlobs",
            {
                "source_id": source["source_id"],
                "url": url,
                "crawl_skipped": reason,
                "checked_at": checked_at.isoformat(),
            },
        ),
        "source_id": source["source_id"],
        "url": str(url),
        "canonical_url": _canonical_or_none(str(url)),
        "url_type": "toc",
        "status": "crawl_skipped",
        "checked_at": checked_at,
        "error": reason,
        "metadata_json": {"run_id": run_id},
    }


def _crawl_failed_observation(
    source: dict[str, Any], url: str, exc: Exception, run_id: str | None
) -> dict[str, Any]:
    checked_at = _utc_now()
    return {
        "observation_id": _id(
            "mrfurlobs",
            {
                "source_id": source["source_id"],
                "url": url,
                "crawl_error": str(exc),
                "checked_at": checked_at.isoformat(),
            },
        ),
        "source_id": source["source_id"],
        "url": str(url),
        "canonical_url": _canonical_or_none(str(url)),
        "url_type": "toc",
        "status": "crawl_failed",
        "checked_at": checked_at,
        "error": str(exc),
        "metadata_json": {"run_id": run_id},
    }


def _crawl_ok_observation(
    target: CrawlTarget, *, run_id: str | None, plans: int, files: int
) -> dict[str, Any]:
    checked_at = _utc_now()
    return {
        "observation_id": _id(
            "mrfurlobs",
            {
                "source_id": target.source["source_id"],
                "url": target.url,
                "crawl_ok": checked_at.isoformat(),
            },
        ),
        "source_id": target.source["source_id"],
        "url": target.url,
        "canonical_url": _canonical_or_none(target.url),
        "url_type": "toc",
        "status": "ok",
        "checked_at": checked_at,
        "final_url": target.url,
        "metadata_json": {
            "run_id": run_id,
            "resolved_from_url": target.resolved_from_url,
            "target_label": target.label,
            "plans_discovered": plans,
            "files_discovered": files,
            **dict(target.metadata or {}),
        },
    }


def _source_target_payer_query(source: dict[str, Any]) -> str | None:
    metadata = dict((source or {}).get("metadata_json") or {})
    raw = metadata.get("raw")
    if isinstance(raw, dict):
        query = _clean_text(raw.get("target_payer_query"))
        if query:
            return query
    return None


def _crawl_target_search_values(target: CrawlTarget) -> list[Any]:
    metadata = dict(target.metadata or {})
    values: list[Any] = [
        target.label,
        target.url,
        target.resolved_from_url,
        metadata.get("file_name"),
        metadata.get("payer_name"),
        metadata.get("blob_name"),
        metadata.get("company_name"),
        metadata.get("employer_name"),
        metadata.get("employer_slug"),
        metadata.get("entity_slug"),
        metadata.get("target_label"),
    ]
    for plan in _metadata_plan_info(metadata):
        values.extend(
            [
                plan.get("plan_name"),
                plan.get("plan_sponsor_name"),
                plan.get("sponsor_name"),
                plan.get("issuer_name"),
                plan.get("company_name"),
            ]
        )
    return values


def _matched_query_expansion_target(
    target: CrawlTarget, query: str | None
) -> CrawlTarget | None:
    if not query:
        return target
    if not _search_values_match_query(_crawl_target_search_values(target), query):
        return None
    metadata = dict(target.metadata or {})
    target_label = _clean_text(
        metadata.get("company_name")
        or metadata.get("payer_name")
        or metadata.get("employer_name")
        or target.label
    )
    if target_label:
        metadata.setdefault("company_name", target_label)
        metadata.setdefault("employer_name", target_label)
    metadata["target_payer_query"] = query
    metadata["query_expansion_match"] = True
    return CrawlTarget(
        source=target.source,
        url=target.url,
        label=target.label,
        resolved_from_url=target.resolved_from_url,
        metadata=metadata,
    )


async def _resolve_crawl_targets(
    source_rows: list[dict[str, Any]],
    *,
    session: aiohttp.ClientSession,
    run_id: str | None,
    progress_run_id: str | None = None,
    concurrency: int,
    crawl_target_limit: int | None = None,
) -> tuple[list[CrawlTarget], list[dict[str, Any]]]:
    items = [
        (source, source.get("index_url") or source.get("human_url"))
        for source in source_rows
        if source.get("index_url") or source.get("human_url")
    ]
    total = len(items)
    targets: list[CrawlTarget] = []
    observations: list[dict[str, Any]] = []
    semaphore = asyncio.Semaphore(max(1, int(concurrency or DEFAULT_CONCURRENCY)))

    async def resolve_one(
        source: dict[str, Any], url: Any
    ) -> tuple[list[CrawlTarget], list[dict[str, Any]]]:
        url_text = str(url)
        async with semaphore:
            try:
                resolved_targets = await _crawl_targets_for_source(
                    source,
                    url_text,
                    session,
                    target_limit=crawl_target_limit,
                )
                target_query = _source_target_payer_query(source)
                if target_query:
                    resolved_targets.extend(
                        await _sapphire_query_probe_targets(
                            source, url_text, target_query, session
                        )
                    )
                    resolved_targets = [
                        matched
                        for target in resolved_targets
                        if (
                            matched := _matched_query_expansion_target(
                                target, target_query
                            )
                        )
                        is not None
                    ]
                if not resolved_targets:
                    return [], [
                        _crawl_skipped_observation(
                            source,
                            url_text,
                            "no configured resolver and URL is not a direct JSON TOC",
                            run_id,
                        )
                    ]
                return resolved_targets, []
            except Exception as exc:  # pylint: disable=broad-exception-caught
                return [], [_crawl_failed_observation(source, url_text, exc, run_id)]

    tasks = [asyncio.create_task(resolve_one(source, url)) for source, url in items]
    for done, task in enumerate(asyncio.as_completed(tasks), start=1):
        resolved_targets, resolved_observations = await task
        targets.extend(resolved_targets)
        observations.extend(resolved_observations)
        if progress_run_id:
            enqueue_live_progress(
                run_id=progress_run_id,
                importer="mrf-source-discovery",
                status="running",
                phase="resolving source pages",
                unit="sources",
                done=done,
                total=total,
                message=f"resolved {done}/{total} source pages",
            )
    return targets, observations


def _crawl_target_rank(target: CrawlTarget) -> tuple[int, str]:
    url = str(target.url or "").lower()
    if (target.metadata or {}).get("target_kind") == "file_reference":
        return 20, url
    if (target.metadata or {}).get("target_kind") == "source_landing_page":
        return 30, url
    if target.resolved_from_url:
        return 0, url
    if urlsplit(url).path.endswith(".json"):
        return 1, url
    return 10, url


def _direct_mrf_body_crawl_target(
    source: dict[str, Any], url: str, *, resolver: str = "direct_mrf_body"
) -> CrawlTarget | None:
    if not _looks_direct_mrf_body_url(url):
        return None
    file_type = _mrf_body_file_type_from_text(
        url, str(source.get("display_name") or "")
    )
    if file_type == "table-of-contents":
        return None
    if not file_type:
        return None
    label = (
        _mrf_file_plan_label(urlsplit(str(url or "")).path)
        or str(source.get("display_name") or "").strip()
        or Path(urlsplit(str(url or "")).path).name
        or "MRF file"
    )
    return CrawlTarget(
        source=source,
        url=url,
        label=label,
        metadata={
            "resolver": resolver,
            "target_kind": "file_reference",
            "target_file_type": file_type,
            "container_format": _container_format(url),
            "plan_info": _plan_info_from_label(label),
        },
    )


def _direct_toc_crawl_target(
    source: dict[str, Any], url: str, *, resolver: str = "direct_toc"
) -> CrawlTarget | None:
    if not _looks_direct_toc_url(url):
        return None
    return CrawlTarget(
        source=source,
        url=url,
        label=str(source.get("display_name") or ""),
        metadata={
            "resolver": resolver,
            "target_kind": "toc_json",
            "target_file_type": "table-of-contents",
            "file_name": Path(urlsplit(str(url or "")).path).name,
            "payer_name": source.get("display_name"),
        },
    )


def _target_fetch_max_bytes(target: CrawlTarget, default: int) -> int:
    metadata = target.metadata or {}
    for key in ("target_max_bytes", "max_bytes", "toc_max_bytes"):
        parsed = _parse_size_bytes(metadata.get(key))
        if parsed:
            return parsed
    return int(default or MAX_TOC_BYTES_DEFAULT)


def _toc_target_file_row(target: CrawlTarget) -> dict[str, Any]:
    now = _utc_now()
    source = target.source
    target_metadata = {
        key: value
        for key, value in dict(target.metadata or {}).items()
        if value not in (None, "")
    }
    context_metadata = _crawl_target_context_metadata(target)
    file_type = str(target_metadata.get("target_file_type") or "table-of-contents")
    plan_info = _metadata_plan_info(target_metadata)
    size_bytes = (
        _parse_size_bytes(target_metadata.get("blob_size"))
        or _parse_size_bytes(target_metadata.get("size_bytes"))
        or _parse_size_bytes(target_metadata.get("content_length"))
    )
    return {
        "mrf_file_id": _id(
            "mrffile",
            {
                "source": source["source_id"],
                "type": file_type,
                "url": _canonical_or_none(target.url),
            },
        ),
        "payer_id": source.get("payer_id"),
        "source_id": source["source_id"],
        "file_type": file_type,
        "url": target.url,
        "canonical_url": _canonical_or_none(target.url),
        "from_index_url": target.resolved_from_url,
        "description": target.label,
        "network_name": target.label,
        "plan_ids": [plan.get("plan_id") for plan in plan_info if plan.get("plan_id")],
        "plan_names": [
            plan.get("plan_name") for plan in plan_info if plan.get("plan_name")
        ],
        "market_types": sorted(
            {
                plan.get("plan_market_type")
                for plan in plan_info
                if plan.get("plan_market_type")
            }
        ),
        "is_signed_url": _looks_signed(target.url),
        "size_bytes": size_bytes,
        "etag": target_metadata.get("etag"),
        "last_modified": target_metadata.get("last_modified"),
        "schema_version": None,
        "metadata_json": {
            "container_format": target_metadata.get("container_format")
            or _container_format(target.url),
            "resolved_from_url": target.resolved_from_url,
            "target_label": target.label,
            **target_metadata,
            **context_metadata,
            **_file_benefit_metadata(
                source,
                target.url,
                target.label,
                target.resolved_from_url,
            ),
        },
        "first_seen_at": now,
        "last_seen_at": now,
    }


def _crawl_source_score(source: dict[str, Any]) -> tuple[int, int]:
    source_type = str(source.get("source_type") or "")
    seed_provider = str(source.get("seed_provider") or "")
    curation_score = 0
    if source_type == "curated_registry":
        curation_score += 100
    if seed_provider == "master-list":
        curation_score += 50
    return curation_score, _as_int(source.get("confidence")) or 0


def _crawl_source_identity_key(source: dict[str, Any]) -> str:
    url = str(source.get("index_url") or source.get("human_url") or "").strip()
    if not url:
        return str(source.get("source_id") or source.get("source_key") or "")
    resolver = _platform_resolver_config(source.get("hosting_platform"))
    if str(resolver.get("type") or "").strip() == "healthsparq_public_mrf":
        try:
            params = _healthsparq_public_params(url)
            metadata_url = _healthsparq_direct_metadata_url(resolver, params)
        except (TypeError, ValueError):
            metadata_url = None
        if metadata_url:
            return _canonical_or_none(metadata_url) or metadata_url
        return url
    return _canonical_or_none(url) or url


def _dedupe_source_rows_for_crawl(
    source_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    no_url: list[dict[str, Any]] = []
    for source in source_rows:
        url = source.get("index_url") or source.get("human_url")
        if not url:
            no_url.append(source)
            continue
        key = _crawl_source_identity_key(source)
        previous = by_url.get(key)
        if previous is None or _crawl_source_score(source) > _crawl_source_score(
            previous
        ):
            by_url[key] = source
    return list(by_url.values()) + no_url


async def _push_crawl_row_batches(
    plan_rows: list[dict[str, Any]],
    file_rows: list[dict[str, Any]],
    observation_rows: list[dict[str, Any]],
    *,
    batch_size: int | None = None,
) -> None:
    async def push_chunked(rows: list[dict[str, Any]], model: type[Any]) -> None:
        size = max(1, int(batch_size or len(rows) or 1))
        while rows:
            chunk = rows[:size]
            await push_objects(chunk, model, rewrite=True, use_copy=False)
            del rows[: len(chunk)]

    if plan_rows:
        await push_chunked(plan_rows, MRFPlan)
    if file_rows:
        await push_chunked(file_rows, MRFFile)
    if observation_rows:
        _normalize_url_observation_rows(observation_rows)
        await push_chunked(observation_rows, MRFUrlObservation)


def _normalize_url_observation_rows(rows: list[dict[str, Any]]) -> None:
    for row in rows:
        for key in MRF_URL_OBSERVATION_NULLABLE_KEYS:
            row.setdefault(key, None)


def _parse_file_probe_types(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return DEFAULT_FILE_PROBE_TYPES
    if isinstance(value, str):
        raw_items = re.split(r"[, ]+", value)
    else:
        raw_items = []
        for item in value:
            raw_items.extend(re.split(r"[, ]+", str(item)))
    items = []
    for item in raw_items:
        text = str(item or "").strip().lower()
        if text and text not in items:
            items.append(text)
    return tuple(items or DEFAULT_FILE_PROBE_TYPES)


def _parse_text_filter_values(value: Any) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        raw_items = re.split(r"[, ]+", value)
    else:
        raw_items = []
        for item in value:
            raw_items.extend(re.split(r"[, ]+", str(item)))
    items = []
    for item in raw_items:
        text = str(item or "").strip().lower()
        if text and text not in items:
            items.append(text)
    return tuple(items)


def _normalize_text_query(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _candidate_matches_text_filters(
    candidate: SourceCandidate,
    *,
    entity_types: tuple[str, ...],
    payer_query: str | None,
) -> bool:
    if entity_types:
        entity_type = str(candidate.entity_type or "").lower()
        if not any(item in entity_type for item in entity_types):
            return False
    if payer_query:
        query = _clean_text(payer_query).lower()
        searchable_names = (candidate.payer_name, *candidate.aliases)
        if not any(
            _candidate_search_name_matches_query(name, query)
            for name in searchable_names
        ):
            return False
    return True


def _candidate_search_name_matches_query(name: str, query: str) -> bool:
    text = _clean_text(name).lower()
    if not text:
        return False
    if query in text:
        return True
    tokens = re.findall(r"[a-z0-9]+", text)
    if len(tokens) >= 2 and len(text) >= 8 and text in query:
        return True
    return False


def _query_tokens(value: Any) -> tuple[str, ...]:
    return tuple(
        token.lower()
        for token in re.findall(r"[a-z0-9]+", _clean_text(value).lower())
        if token
    )


def _query_match_tokens(value: Any) -> tuple[str, ...]:
    tokens = _query_tokens(value)
    substantive = tuple(
        token for token in tokens if token not in LEGAL_ENTITY_QUERY_STOPWORDS
    )
    return substantive or tokens


def _search_values_match_query(values: Iterable[Any], query: str | None) -> bool:
    clean_query = _clean_text(query).lower()
    if not clean_query:
        return True
    query_tokens = _query_match_tokens(clean_query)
    for value in values:
        text = _search_match_text(value)
        if not text:
            continue
        if clean_query in text:
            return True
        if query_tokens:
            value_tokens = set(_query_tokens(text))
            if all(token in value_tokens for token in query_tokens):
                return True
            if all(token in text for token in query_tokens if len(token) >= 3):
                return True
    return False


def _search_match_text(value: Any) -> str:
    text = _clean_text(value).lower()
    parsed = urlsplit(str(value or "").strip())
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        text = _clean_text(
            " ".join(part for part in (unquote(parsed.path), unquote(parsed.query)) if part)
        ).lower()
    return text


def _candidate_supports_source_query_expansion(candidate: SourceCandidate) -> bool:
    source_url = candidate.index_url or candidate.human_url
    platform = candidate.hosting_platform or classify_hosting_platform(source_url)
    if platform not in _source_query_expansion_platforms():
        return False
    if not _platform_resolver_config(platform):
        return False
    return bool(source_url)


def _candidate_with_target_payer_query(
    candidate: SourceCandidate, query: str
) -> SourceCandidate:
    return replace(
        candidate,
        raw_payload={
            **dict(candidate.raw_payload or {}),
            "target_payer_query": query,
            "query_expansion_source": True,
        },
    )


def _candidate_is_importable_source(candidate: SourceCandidate) -> bool:
    if not (candidate.index_url or candidate.human_url):
        return False
    if _normalize_source_tier(candidate.source_tier) != "mrf_importable":
        return False
    return str(candidate.status or "").lower() not in {
        "archived",
        "needs_review",
        "unsupported",
    }


def _candidate_has_catalog_source(candidate: SourceCandidate) -> bool:
    if _candidate_is_importable_source(candidate):
        return True
    if not (candidate.index_url or candidate.human_url):
        return False
    if _normalize_source_tier(candidate.source_tier) == "mrf_importable":
        return False
    return str(candidate.status or "").lower() not in {"archived", "unsupported"}


def _discovery_run_mode(*, crawl: bool, check_urls: bool, probe_files: bool) -> str:
    modes = []
    if crawl:
        modes.append("crawl")
    if check_urls:
        modes.append("check_urls")
    if probe_files:
        modes.append("probe_files")
    return "+".join(modes) if modes else "seed"


async def _load_file_probe_targets(
    file_types: tuple[str, ...],
    limit: int | None,
    *,
    entity_types: tuple[str, ...] = (),
    payer_query: str | None = None,
) -> list[dict[str, Any]]:
    stmt = (
        select(
            MRFFile.mrf_file_id,
            MRFFile.url,
            MRFFile.file_type,
            MRFFile.payer_id,
            MRFPayer.canonical_name,
            MRFPayer.entity_type,
        )
        .select_from(MRFFile)
        .join(MRFPayer, MRFFile.payer_id == MRFPayer.payer_id, isouter=True)
        .where(MRFFile.file_type.in_(list(file_types)))
        .where(MRFFile.url.is_not(None))
        .where(or_(MRFFile.url.like("http://%"), MRFFile.url.like("https://%")))
        .order_by(MRFFile.last_seen_at.desc(), MRFFile.mrf_file_id.asc())
    )
    if entity_types:
        entity_value = func.lower(func.coalesce(MRFPayer.entity_type, ""))
        stmt = stmt.where(
            or_(
                *[entity_value.like(f"%{entity_type}%") for entity_type in entity_types]
            )
        )
    if payer_query:
        payer_value = func.lower(func.coalesce(MRFPayer.canonical_name, ""))
        stmt = stmt.where(payer_value.like(f"%{payer_query.lower()}%"))
    rows = await db.all(stmt)
    targets = [
        {
            "mrf_file_id": row[0],
            "url": row[1],
            "file_type": row[2],
            "payer_id": row[3],
            "payer_name": row[4],
            "entity_type": row[5],
        }
        for row in rows
    ]
    targets = _interleave_file_probe_targets_by_host(targets)
    return targets[: max(1, int(limit))] if limit else targets


def _file_probe_target_host(target: dict[str, Any]) -> str:
    return (urlsplit(str(target.get("url") or "")).netloc or "unknown").lower()


def _interleave_file_probe_targets_by_host(
    targets: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_host: dict[str, list[dict[str, Any]]] = {}
    for target in targets:
        by_host.setdefault(_file_probe_target_host(target), []).append(target)
    ordered_hosts = sorted(by_host, key=lambda host: (-len(by_host[host]), host))
    interleaved: list[dict[str, Any]] = []
    while ordered_hosts:
        next_hosts = []
        for host in ordered_hosts:
            bucket = by_host[host]
            if bucket:
                interleaved.append(bucket.pop(0))
            if bucket:
                next_hosts.append(host)
        ordered_hosts = next_hosts
    return interleaved


def _file_probe_observation(
    target: dict[str, Any], head: dict[str, Any], run_id: str | None
) -> dict[str, Any]:
    checked_at = head["checked_at"]
    return {
        "observation_id": _id(
            "mrfurlobs",
            {
                "file_id": target["mrf_file_id"],
                "url": target["url"],
                "body_file_head": checked_at.isoformat(),
            },
        ),
        "source_id": None,
        "url": str(target["url"]),
        "canonical_url": _canonical_or_none(str(target["url"])),
        "url_type": "body_file_head",
        "status": str(head.get("status") or "unknown"),
        "http_status": head.get("http_status"),
        "etag": head.get("etag"),
        "last_modified": head.get("last_modified"),
        "content_length": head.get("content_length"),
        "content_type": head.get("content_type"),
        "final_url": head.get("final_url"),
        "checked_at": checked_at,
        "error": head.get("error"),
        "metadata_json": {
            "run_id": run_id,
            "mrf_file_id": target["mrf_file_id"],
            "file_type": target.get("file_type"),
            "payer_id": target.get("payer_id"),
            "payer_name": target.get("payer_name"),
            "entity_type": target.get("entity_type"),
        },
    }


def _file_probe_update_values(
    target: dict[str, Any], head: dict[str, Any]
) -> dict[str, Any]:
    if str(head.get("status") or "") != "ok":
        return {}
    values: dict[str, Any] = {}
    if head.get("content_length") is not None:
        values["size_bytes"] = head.get("content_length")
    if head.get("etag") is not None:
        values["etag"] = head.get("etag")
    if head.get("last_modified") is not None:
        values["last_modified"] = head.get("last_modified")
    return (
        {
            "mrf_file_id": target["mrf_file_id"],
            **values,
        }
        if values
        else {}
    )


async def _update_mrf_file_probe_metadata(updates: list[dict[str, Any]]) -> None:
    if not updates:
        return
    async with db.session() as session:
        for item in updates:
            file_id = item.get("mrf_file_id")
            values = {key: value for key, value in item.items() if key != "mrf_file_id"}
            if not file_id or not values:
                continue
            await session.execute(
                update(MRFFile).where(MRFFile.mrf_file_id == file_id).values(**values)
            )


async def _probe_mrf_file_heads(
    *,
    file_types: tuple[str, ...],
    limit: int | None,
    entity_types: tuple[str, ...] = (),
    payer_query: str | None = None,
    run_id: str | None,
    progress_run_id: str | None = None,
    concurrency: int,
) -> tuple[list[dict[str, Any]], int]:
    targets = await _load_file_probe_targets(
        file_types, limit, entity_types=entity_types, payer_query=payer_query
    )
    if not targets:
        return [], 0
    worker_count = max(1, int(concurrency or DEFAULT_CONCURRENCY))
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    connector = _tcp_connector(limit=worker_count * 2)
    target_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    result_queue: asyncio.Queue[tuple[dict[str, Any], dict[str, Any]] | None] = (
        asyncio.Queue(maxsize=max(worker_count * 4, 1))
    )
    for target in targets:
        target_queue.put_nowait(target)
    for _ in range(worker_count):
        target_queue.put_nowait(None)

    async def worker(session: aiohttp.ClientSession) -> None:
        while True:
            target = await target_queue.get()
            try:
                if target is None:
                    await result_queue.put(None)
                    return
                head = await _head_url(str(target["url"]), session=session)
                await result_queue.put((target, head))
            finally:
                target_queue.task_done()

    async def writer() -> tuple[list[dict[str, Any]], int]:
        done = 0
        ok_count = 0
        finished_workers = 0
        all_observations: list[dict[str, Any]] = []
        observation_batch: list[dict[str, Any]] = []
        update_batch: list[dict[str, Any]] = []
        total = len(targets)
        while finished_workers < worker_count:
            item = await result_queue.get()
            if item is None:
                finished_workers += 1
                continue
            target, head = item
            done += 1
            if str(head.get("status") or "") == "ok":
                ok_count += 1
            observation = _file_probe_observation(target, head, run_id)
            all_observations.append(observation)
            observation_batch.append(observation)
            update_values = _file_probe_update_values(target, head)
            if update_values:
                update_batch.append(update_values)
            if len(observation_batch) >= WRITE_BATCH_SIZE:
                await push_objects(
                    observation_batch, MRFUrlObservation, rewrite=True, use_copy=False
                )
                observation_batch.clear()
            if len(update_batch) >= WRITE_BATCH_SIZE:
                await _update_mrf_file_probe_metadata(update_batch)
                update_batch.clear()
            if progress_run_id:
                enqueue_live_progress(
                    run_id=progress_run_id,
                    importer="mrf-source-discovery",
                    status="running",
                    phase="probing MRF file headers",
                    unit="files",
                    done=done,
                    total=total,
                    message=f"probed {done}/{total} file headers",
                    label=str(target.get("url") or ""),
                )
        if observation_batch:
            await push_objects(
                observation_batch, MRFUrlObservation, rewrite=True, use_copy=False
            )
        await _update_mrf_file_probe_metadata(update_batch)
        return all_observations, ok_count

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
        connector=connector,
        trust_env=False,
    ) as session:
        workers = [asyncio.create_task(worker(session)) for _ in range(worker_count)]
        writer_task = asyncio.create_task(writer())
        await target_queue.join()
        await asyncio.gather(*workers)
        return await writer_task


async def _crawl_toc_metadata(
    source_rows: list[dict[str, Any]],
    *,
    test_mode: bool,
    run_id: str | None,
    progress_run_id: str | None = None,
    max_toc_bytes: int,
    concurrency: int = DEFAULT_CONCURRENCY,
    crawl_target_limit: int | None = None,
) -> tuple[int, int, list[dict[str, Any]]]:
    if test_mode:
        return 0, 0, []
    plans_discovered = 0
    files_discovered = 0
    observation_ids: set[str] = set()
    worker_count = max(1, int(concurrency or DEFAULT_CONCURRENCY))
    write_batch_size = WRITE_BATCH_SIZE
    timeout = aiohttp.ClientTimeout(
        total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT
    )
    connector = _tcp_connector(limit=worker_count * 2)
    crawl_source_rows = _dedupe_source_rows_for_crawl(source_rows)

    async def crawl_one(
        target: CrawlTarget, session: aiohttp.ClientSession
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str]:
        try:
            target_kind = (target.metadata or {}).get("target_kind")
            target_file_type = (target.metadata or {}).get("target_file_type")
            target_max_bytes = _target_fetch_max_bytes(target, max_toc_bytes)
            if (
                target_kind == "file_reference"
                and target_file_type == "table-of-contents"
            ):
                if (target.metadata or {}).get("container_format") == "zip":
                    toc_values = await _fetch_zip_json_values(
                        target.url,
                        max_bytes=target_max_bytes,
                        session=session,
                    )
                else:
                    toc_values = [
                        (
                            Path(urlsplit(target.url).path).name,
                            await _fetch_json(
                                target.url,
                                max_bytes=target_max_bytes,
                                session=session,
                            ),
                        )
                    ]
                plan_rows: list[dict[str, Any]] = []
                file_rows: list[dict[str, Any]] = []
                for _member_name, toc in toc_values:
                    if not isinstance(toc, dict):
                        raise ValueError("expected JSON object")
                    member_plan_rows, member_file_rows = _toc_rows_from_content(
                        target.source, target.url, toc
                    )
                    plan_rows.extend(member_plan_rows)
                    file_rows.extend(
                        _apply_crawl_target_context_to_file_rows(
                            member_file_rows, target
                        )
                    )
                return (
                    plan_rows,
                    file_rows,
                    [
                        _crawl_ok_observation(
                            target,
                            run_id=run_id,
                            plans=len(plan_rows),
                            files=len(file_rows),
                        )
                    ],
                    target.url,
                )
            if target_kind in {"file_reference", "source_landing_page"}:
                plan_rows = _plan_rows_from_target_metadata(target)
                return (
                    plan_rows,
                    [],
                    [
                        _crawl_ok_observation(
                            target, run_id=run_id, plans=len(plan_rows), files=1
                        )
                    ],
                    target.url,
                )
            if target_kind == "metadata_text":
                text = await _fetch_text(
                    target.url, max_bytes=target_max_bytes, session=session
                )
                plan_rows, file_rows = _metadata_text_rows_from_content(
                    target.source, target.url, text
                )
            else:
                toc = await _fetch_json(
                    target.url, max_bytes=target_max_bytes, session=session
                )
                plan_rows, file_rows = _toc_rows_from_content(
                    target.source, target.url, toc
                )
                file_rows = _apply_crawl_target_context_to_file_rows(file_rows, target)
            return (
                plan_rows,
                file_rows,
                [
                    _crawl_ok_observation(
                        target,
                        run_id=run_id,
                        plans=len(plan_rows),
                        files=len(file_rows),
                    )
                ],
                target.url,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return (
                [],
                [],
                [_crawl_failed_observation(target.source, target.url, exc, run_id)],
                target.url,
            )

    async with aiohttp.ClientSession(
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
        connector=connector,
        trust_env=False,
    ) as session:
        targets, resolver_observations = await _resolve_crawl_targets(
            crawl_source_rows,
            session=session,
            run_id=run_id,
            progress_run_id=progress_run_id,
            concurrency=concurrency,
            crawl_target_limit=crawl_target_limit,
        )
        targets = sorted(targets, key=_crawl_target_rank)
        expanded_target_count = len(targets)
        if crawl_target_limit:
            targets = targets[: max(1, int(crawl_target_limit))]
        target_rows: list[dict[str, Any]] = []
        for target in targets:
            row = _toc_target_file_row(target)
            target_rows.append(row)
        files_discovered += len(target_rows)
        for observation in resolver_observations:
            observation_ids.add(observation["observation_id"])
        await _push_crawl_row_batches(
            [], target_rows, resolver_observations, batch_size=write_batch_size
        )
        total = len(targets)
        if progress_run_id:
            message = f"resolved {expanded_target_count} TOC targets from {len(crawl_source_rows)} source pages"
            if total != expanded_target_count:
                message = f"{message}; crawling first {total}"
            enqueue_live_progress(
                run_id=progress_run_id,
                importer="mrf-source-discovery",
                status="running",
                phase="resolved source TOCs",
                unit="targets",
                done=expanded_target_count,
                total=expanded_target_count,
                message=message,
            )
        target_queue: asyncio.Queue[CrawlTarget | None] = asyncio.Queue()
        result_queue: asyncio.Queue[
            tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str]
            | None
        ] = asyncio.Queue(maxsize=max(worker_count * 4, 1))
        for target in targets:
            target_queue.put_nowait(target)
        for _ in range(min(worker_count, max(total, 1))):
            target_queue.put_nowait(None)

        async def worker() -> None:
            while True:
                target = await target_queue.get()
                try:
                    if target is None:
                        await result_queue.put(None)
                        return
                    await result_queue.put(await crawl_one(target, session))
                finally:
                    target_queue.task_done()

        async def writer(active_workers: int) -> None:
            nonlocal plans_discovered, files_discovered
            done = 0
            finished_workers = 0
            plan_batch: list[dict[str, Any]] = []
            file_batch: list[dict[str, Any]] = []
            observation_batch: list[dict[str, Any]] = []
            while finished_workers < active_workers:
                item = await result_queue.get()
                if item is None:
                    finished_workers += 1
                    continue
                plan_rows, file_rows, crawl_observations, url = item
                done += 1
                plans_discovered += len(plan_rows)
                files_discovered += len(file_rows)
                for row in crawl_observations:
                    observation_ids.add(row["observation_id"])
                plan_batch.extend(plan_rows)
                file_batch.extend(file_rows)
                observation_batch.extend(crawl_observations)
                if (
                    len(plan_batch) >= write_batch_size
                    or len(file_batch) >= write_batch_size
                    or len(observation_batch) >= write_batch_size
                ):
                    if progress_run_id:
                        enqueue_live_progress(
                            run_id=progress_run_id,
                            importer="mrf-source-discovery",
                            status="running",
                            phase="writing TOC metadata rows",
                            unit="targets",
                            done=done,
                            total=total,
                            message=f"writing rows for TOC target {done}/{total}",
                            label=str(url),
                        )
                    await _push_crawl_row_batches(
                        plan_batch,
                        file_batch,
                        observation_batch,
                        batch_size=write_batch_size,
                    )
                if progress_run_id:
                    enqueue_live_progress(
                        run_id=progress_run_id,
                        importer="mrf-source-discovery",
                        status="running",
                        phase="crawling TOC metadata",
                        unit="targets",
                        done=done,
                        total=total,
                        message=f"crawled {done}/{total} TOC targets",
                        label=str(url),
                    )
            await _push_crawl_row_batches(
                plan_batch, file_batch, observation_batch, batch_size=write_batch_size
            )

        active_workers = min(worker_count, max(total, 1))
        workers = [asyncio.create_task(worker()) for _ in range(active_workers)]
        writer_task = asyncio.create_task(writer(active_workers))
        worker_group = asyncio.gather(*workers)
        try:
            done, _ = await asyncio.wait(
                {worker_group, writer_task}, return_when=asyncio.FIRST_EXCEPTION
            )
            for task in done:
                task.result()
            await worker_group
            await writer_task
        except Exception:
            worker_group.cancel()
            writer_task.cancel()
            await asyncio.gather(worker_group, writer_task, return_exceptions=True)
            raise
    return (
        plans_discovered,
        files_discovered,
        [{"observation_id": value} for value in observation_ids],
    )


def _looks_signed(url: str | None) -> bool:
    raw = str(url or "").lower()
    return any(
        token in raw
        for token in (
            "signature=",
            "x-amz-signature=",
            "sig=",
            "expires=",
            "x-goog-signature=",
        )
    )


def _import_control_seed_item(
    row: dict[str, Any],
    *,
    review_status: str | None = None,
    promoted_source_id: str | None = None,
) -> dict[str, Any] | None:
    url = row.get("index_url") or row.get("human_url")
    if not url:
        return None
    item = {
        "seed_url": url,
        "payer_name": row.get("display_name"),
        "source_key": row.get("source_key"),
        "provider": row.get("seed_provider"),
        "license_status": row.get("license_status"),
        "confidence": row.get("confidence"),
        "review_status": review_status or row.get("review_status") or "pending",
        "metadata": {
            "hosting_platform": row.get("hosting_platform"),
            "source_type": row.get("source_type"),
            "source_tier": _source_row_source_tier(row),
            "domain": row.get("domain"),
            "healthcare_source_id": row.get("source_id"),
            "benefit_lines": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("benefit_lines") or []
                )
            ),
            "aliases": list(
                dict.fromkeys((row.get("metadata_json") or {}).get("aliases") or [])
            ),
            "source_coverage": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("source_coverage") or []
                )
            ),
            "vendor_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("vendor_names") or []
                )
            ),
            "network_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("network_names") or []
                )
            ),
            "plan_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("plan_names") or []
                )
            ),
        },
    }
    if promoted_source_id:
        item["promoted_source_id"] = promoted_source_id
    if item["review_status"] == "promoted":
        item["reviewed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return item


async def _sync_import_control_seeds(
    source_rows: list[dict[str, Any]], *, limit: int | None = None
) -> int:
    base_url = str(
        os.getenv("HLTHPRT_IMPORT_CONTROL_URL")
        or os.getenv("HP_IMPORT_CONTROL_BASE_URL")
        or ""
    ).strip()
    token = str(
        os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN")
        or os.getenv("HLTHPRT_CONTROL_API_TOKEN")
        or ""
    ).strip()
    if not base_url or not token:
        logging.getLogger(__name__).warning(
            "import-control seed sync skipped: HLTHPRT_IMPORT_CONTROL_URL and/or HLTHPRT_IMPORT_CONTROL_TOKEN not configured"
        )
        return 0
    items = []
    for row in source_rows[: limit or len(source_rows)]:
        item = _import_control_seed_item(row)
        if item:
            items.append(item)
    if not items:
        return 0
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    async with aiohttp.ClientSession(
        headers=headers, timeout=timeout, trust_env=False
    ) as session:
        async with session.post(
            f"{base_url.rstrip('/')}/v1/catalog/seeds/import",
            json={"seed_provider": "healthcare-mrf-api", "items": items},
        ) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(
                    f"import-control seed sync failed: {resp.status} {text[:200]}"
                )
            payload = await resp.json()
    return int(payload.get("count") or len(payload.get("items") or []))


def _env_flag(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _coerce_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _source_row_source_tier(row: dict[str, Any]) -> str:
    metadata = row.get("metadata_json") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    return _normalize_source_tier(row.get("source_tier") or metadata.get("source_tier"))


def _source_row_is_importable(row: dict[str, Any]) -> bool:
    return _source_row_source_tier(row) == "mrf_importable"


def _eligible_for_public_promotion(row: dict[str, Any]) -> bool:
    """Only free, direct MRF sources become public import-control sources. Paid/vendor
    aggregator provenance rows stay in the seed review queue."""
    access_model = str(row.get("access_model") or "").strip().lower()
    source_type = str(row.get("source_type") or "").strip().lower()
    return access_model == "free" and source_type != "vendor_aggregator"


def _import_control_source_urls(row: dict[str, Any]) -> tuple[str | None, str | None]:
    index_url = row.get("index_url") or row.get("human_url")
    official_url = row.get("human_url") or row.get("index_url")
    platform = str(row.get("hosting_platform") or "").strip()
    resolver = _platform_resolver_config(platform)
    if (
        index_url
        and str(resolver.get("type") or "").strip() == "healthsparq_public_mrf"
        and resolver.get("metadata_url_template")
    ):
        try:
            params = _healthsparq_public_params(str(index_url))
            metadata_url = _healthsparq_direct_metadata_url(resolver, params)
        except (TypeError, ValueError):
            metadata_url = None
        if metadata_url:
            return metadata_url, str(official_url or index_url)
    return (
        str(index_url) if index_url else None,
        str(official_url) if official_url else None,
    )


def _chunked(items: list[Any], size: int):
    for start in range(0, len(items), max(1, size)):
        yield items[start : start + size]


def _split_preview_items(
    items: list[dict[str, Any]], *, max_plan_info: int = 200
) -> list[dict[str, Any]]:
    """Split large per-file plan arrays into smaller preview items.

    Some payers list thousands of plans against every file reference. import-control
    upserts by stable source_file_id + discovered_plan_id, so sending the same file
    URL with smaller plan_info slices is equivalent and avoids huge HTTP payloads.
    """
    split_items: list[dict[str, Any]] = []
    for item in items:
        plan_info = item.get("plan_info") or []
        if not isinstance(plan_info, list) or len(plan_info) <= max_plan_info:
            split_items.append(item)
            continue
        for plan_batch in _chunked(plan_info, max_plan_info):
            next_item = dict(item)
            next_item["plan_info"] = plan_batch
            split_items.append(next_item)
    return split_items


def _as_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item or "").strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item or "").strip()]
    return []


def _file_column_plan_info(
    *,
    source_id: str,
    plan_ids: Any,
    plan_names: Any,
    market_types: Any,
    plan_lookup: dict[tuple[str, str, str | None, str | None], dict[str, Any]],
) -> list[dict[str, Any]]:
    ids = _as_text_list(plan_ids)
    names = _as_text_list(plan_names)
    markets = _as_text_list(market_types)
    if not ids:
        return []
    items: list[dict[str, Any]] = []
    for index, plan_id in enumerate(ids):
        plan_name = names[index] if index < len(names) else None
        market_type = (
            markets[index]
            if index < len(markets)
            else (markets[0] if len(markets) == 1 else None)
        )
        lookup = (
            plan_lookup.get((source_id, plan_id, plan_name, market_type))
            or plan_lookup.get((source_id, plan_id, plan_name, None))
            or plan_lookup.get((source_id, plan_id, None, market_type))
            or plan_lookup.get((source_id, plan_id, None, None))
            or {}
        )
        items.append(
            {
                "plan_id": plan_id,
                "plan_id_type": lookup.get("plan_id_type"),
                "plan_market_type": market_type or lookup.get("market_type"),
                "plan_name": plan_name or lookup.get("plan_name"),
                "issuer_name": lookup.get("issuer_name"),
                "plan_sponsor_name": lookup.get("plan_sponsor_name"),
            }
        )
    return items


def _import_control_plan_info_with_context_ids(
    *,
    source_id: str,
    plan_info: list[dict[str, Any]],
    from_index_url: Any,
    canonical_url: Any,
) -> list[dict[str, Any]]:
    next_info: list[dict[str, Any]] = []
    source_index_url = (
        str(from_index_url or "").strip() or str(canonical_url or "").strip()
    )
    for plan in plan_info:
        next_plan = dict(plan)
        plan_id = str(next_plan.get("plan_id") or "").strip()
        plan_name = str(
            next_plan.get("plan_name")
            or next_plan.get("plan_sponsor_name")
            or next_plan.get("issuer_name")
            or ""
        ).strip()
        market_type = str(next_plan.get("plan_market_type") or "").strip()
        if not plan_id and plan_name and market_type:
            next_plan["plan_id"] = semantic_hash(
                {
                    "source_id": source_id,
                    "source_index_url": source_index_url,
                    "plan_name": plan_name,
                    "market_type": market_type,
                },
                domain="mrf_source_context_plan",
            )[:32]
            next_plan["plan_id_type"] = (
                next_plan.get("plan_id_type") or "source_context_hash"
            )
        next_info.append(next_plan)
    return next_info


_GENERIC_IMPORT_CONTROL_PLAN_LABELS = {
    "allowed amount",
    "allowed amounts",
    "allowed amount file",
    "allowed amounts file",
    "in network",
    "in-network",
    "in network file",
    "in-network file",
    "in network rates",
    "in-network rates",
    "local in-network negotiated rates file",
    "local network",
    "machine readable",
    "machine readable file",
    "machine readable files",
    "mrf",
    "mrfs",
    "machinereadables",
    "pricing transparency",
    "transparency",
    "transparency in coverage",
}


def _import_control_label_is_generic(value: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    return not normalized or normalized in _GENERIC_IMPORT_CONTROL_PLAN_LABELS


def _meaningful_import_control_label(*values: Any) -> str | None:
    for value in values:
        plan_name = _clean_text(value)
        if not plan_name or _import_control_label_is_generic(plan_name):
            continue
        return plan_name
    return None


def _import_control_file_context_plan_info(
    *,
    source_id: str,
    description: Any,
    network_name: Any,
    company_name: Any,
    from_index_url: Any,
    canonical_url: Any,
    target_label: Any = None,
) -> list[dict[str, Any]]:
    for candidate in (description, network_name, target_label, company_name):
        plan_name = _meaningful_import_control_label(candidate)
        if not plan_name:
            continue
        plan_id = semantic_hash(
            {
                "source_id": source_id,
                "source_index_url": str(from_index_url or "").strip(),
                "canonical_url": str(canonical_url or "").strip(),
                "plan_name": plan_name,
                "market_type": "group",
            },
            domain="mrf_source_file_context_plan",
        )[:32]
        return [
            {
                "plan_id": plan_id,
                "plan_id_type": "source_file_context_hash",
                "plan_market_type": "group",
                "plan_name": plan_name,
            }
        ]
    return []


def _plan_lookup_from_rows(
    rows: list[Any],
) -> dict[tuple[str, str, str | None, str | None], dict[str, Any]]:
    lookup: dict[tuple[str, str, str | None, str | None], dict[str, Any]] = {}
    for row in rows:
        source_id = str(row[0] or "")
        plan_id = str(row[1] or "").strip()
        if not source_id or not plan_id:
            continue
        metadata = _coerce_metadata(row[6] if len(row) > 6 else None)
        raw_plan = (
            metadata.get("raw_plan")
            if isinstance(metadata.get("raw_plan"), dict)
            else {}
        )
        item = {
            "plan_id_type": row[2],
            "market_type": row[3],
            "plan_name": row[4],
            "reporting_entity_name": row[5],
            "issuer_name": raw_plan.get("issuer_name") or raw_plan.get("issuerName"),
            "plan_sponsor_name": (
                raw_plan.get("plan_sponsor_name")
                or raw_plan.get("plan_sponser_name")
                or raw_plan.get("sponsor_name")
                or raw_plan.get("company_name")
            ),
        }
        plan_name = str(row[4] or "").strip() or None
        market_type = str(row[3] or "").strip() or None
        lookup.setdefault((source_id, plan_id, plan_name, market_type), item)
        lookup.setdefault((source_id, plan_id, plan_name, None), item)
        lookup.setdefault((source_id, plan_id, None, market_type), item)
        lookup.setdefault((source_id, plan_id, None, None), item)
    return lookup


def _enrich_plan_info_from_lookup(
    source_id: str,
    plan_info: list[dict[str, Any]],
    plan_lookup: dict[tuple[str, str, str | None, str | None], dict[str, Any]],
) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for plan in plan_info:
        if not isinstance(plan, dict):
            continue
        next_plan = dict(plan)
        plan_id = str(next_plan.get("plan_id") or "").strip()
        plan_name = str(next_plan.get("plan_name") or "").strip() or None
        market_type = str(next_plan.get("plan_market_type") or "").strip() or None
        lookup = (
            plan_lookup.get((source_id, plan_id, plan_name, market_type))
            or plan_lookup.get((source_id, plan_id, plan_name, None))
            or plan_lookup.get((source_id, plan_id, None, market_type))
            or plan_lookup.get((source_id, plan_id, None, None))
            or {}
        )
        if not next_plan.get("issuer_name") and lookup.get("issuer_name"):
            next_plan["issuer_name"] = lookup["issuer_name"]
        if not (
            next_plan.get("plan_sponsor_name") or next_plan.get("plan_sponser_name")
        ) and lookup.get("plan_sponsor_name"):
            next_plan["plan_sponsor_name"] = lookup["plan_sponsor_name"]
        enriched.append(next_plan)
    return enriched


def _reporting_entity_from_plan_info(
    source_id: str,
    plan_info: list[dict[str, Any]],
    plan_lookup: dict[tuple[str, str, str | None, str | None], dict[str, Any]],
) -> str | None:
    for plan in plan_info:
        plan_id = str(plan.get("plan_id") or "").strip()
        plan_name = str(plan.get("plan_name") or "").strip() or None
        market_type = str(plan.get("plan_market_type") or "").strip() or None
        lookup = (
            plan_lookup.get((source_id, plan_id, plan_name, market_type))
            or plan_lookup.get((source_id, plan_id, plan_name, None))
            or plan_lookup.get((source_id, plan_id, None, market_type))
            or plan_lookup.get((source_id, plan_id, None, None))
            or {}
        )
        reporting_entity = str(lookup.get("reporting_entity_name") or "").strip()
        if reporting_entity:
            return reporting_entity
    return None


def _company_name_from_index_url(value: Any) -> str | None:
    path = urlsplit(str(value or "")).path
    name = Path(path).name
    if not name:
        return None
    name = re.sub(r"\.(json|zip|gz)$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"_index$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"^\d{4}-\d{2}-\d{2}[_-]+", "", name)
    name = name.replace("_", " ").replace("-", " ").strip()
    if not name or name.lower() in {"index", "toc", "table of contents", "mrfdownload"}:
        return None
    name = re.sub(r"\s+", " ", name)
    if _import_control_label_is_generic(name):
        return None
    return name


def _import_control_company_name(
    metadata: dict[str, Any], from_index_url: Any
) -> str | None:
    return _meaningful_import_control_label(
        metadata.get("company_name"),
        _company_name_from_index_url(from_index_url),
    )


def _apply_company_fallback(
    plan_info: list[dict[str, Any]], company_name: str | None
) -> list[dict[str, Any]]:
    if not company_name:
        return plan_info
    next_info: list[dict[str, Any]] = []
    for plan in plan_info:
        next_plan = dict(plan)
        if not (
            next_plan.get("plan_sponsor_name")
            or next_plan.get("plan_sponser_name")
            or next_plan.get("sponsor_name")
        ):
            next_plan["plan_sponsor_name"] = company_name
        next_info.append(next_plan)
    return next_info


def _import_control_supported_rate_domain(value: Any) -> bool:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return normalized in {"in_network", "in_network_rates"}


def _import_control_snapshot_file_is_supported(
    file_type: Any, metadata: dict[str, Any], from_index_url: Any
) -> bool:
    if str(metadata.get("source_format") or "").strip().lower() == "csv":
        return False
    return _import_control_supported_rate_domain(metadata.get("domain") or file_type)


def _import_control_preview_context(
    metadata: dict[str, Any], from_index_url: Any, canonical_url: Any
) -> dict[str, str]:
    context: dict[str, str] = {}
    for key in (
        "group_id",
        "group_number",
        "client_id",
        "client_name",
        "company_name",
        "employer_id",
        "employer_name",
        "employer_slug",
        "entity_slug",
        "tpa_slug",
        "tpa_name",
        "ein",
        "plan_id",
        "plan_name",
        "target_label",
    ):
        value = str(metadata.get(key) or "").strip()
        if value and not (
            key == "company_name" and _import_control_label_is_generic(value)
        ):
            context[key] = value
    group_number = (
        context.get("group_id")
        or context.get("group_number")
        or _asr_group_number_from_url(str(from_index_url or ""))
        or _asr_group_number_from_url(str(canonical_url or ""))
    )
    if group_number:
        context["group_id"] = group_number
        context["group_number"] = group_number
    return context


async def _import_control_snapshot_items(
    source_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Read the currently-stored MRF file snapshot for the given sources and build
    import-control preview items grouped by healthcare source_id. Prefer the exact
    per-file plan list captured on MRFFile.metadata_json; for older resolver rows,
    synthesize it from MRFFile plan columns and MRFPlan plan_id_type metadata."""
    unique_ids = [sid for sid in dict.fromkeys(source_ids) if sid]
    if not unique_ids:
        return {}
    plan_rows = await db.all(
        select(
            MRFPlan.source_id,
            MRFPlan.plan_id,
            MRFPlan.plan_id_type,
            MRFPlan.market_type,
            MRFPlan.plan_name,
            MRFPlan.reporting_entity_name,
            MRFPlan.metadata_json,
        ).where(MRFPlan.source_id.in_(unique_ids))
    )
    plan_lookup = _plan_lookup_from_rows(plan_rows)
    stmt = (
        select(
            MRFFile.source_id,
            MRFFile.url,
            MRFFile.canonical_url,
            MRFFile.file_type,
            MRFFile.size_bytes,
            MRFFile.metadata_json,
            MRFFile.plan_ids,
            MRFFile.plan_names,
            MRFFile.market_types,
            MRFFile.network_name,
            MRFFile.description,
            MRFFile.from_index_url,
        )
        .where(MRFFile.source_id.in_(unique_ids))
        .where(MRFFile.url.is_not(None))
    )
    rows = await db.all(stmt)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        metadata = _coerce_metadata(row[5])
        file_domain = metadata.get("domain") or row[3]
        if not _import_control_snapshot_file_is_supported(
            file_domain, metadata, row[11]
        ):
            continue
        plan_info = metadata.get("plan_info") or []
        if not plan_info:
            plan_info = _file_column_plan_info(
                source_id=row[0],
                plan_ids=row[6],
                plan_names=row[7],
                market_types=row[8],
                plan_lookup=plan_lookup,
            )
        else:
            plan_info = _enrich_plan_info_from_lookup(row[0], plan_info, plan_lookup)
        company_name = _import_control_company_name(metadata, row[11])
        plan_info = _apply_company_fallback(plan_info, company_name)
        original_url = row[1] or row[2]
        if not original_url:
            continue
        if not plan_info:
            plan_info = _import_control_file_context_plan_info(
                source_id=row[0],
                description=row[10] or metadata.get("description"),
                network_name=row[9],
                company_name=company_name,
                from_index_url=row[11],
                canonical_url=row[2] or original_url,
                target_label=metadata.get("target_label"),
            )
        if not plan_info:
            continue
        plan_info = _import_control_plan_info_with_context_ids(
            source_id=row[0],
            plan_info=plan_info,
            from_index_url=row[11],
            canonical_url=row[2] or original_url,
        )
        context = _import_control_preview_context(
            metadata, row[11], row[2] or original_url
        )
        grouped.setdefault(row[0], []).append(
            {
                "original_url": original_url,
                "canonical_url": row[2] or original_url,
                "domain": file_domain,
                "source_type": row[3],
                "network_name": row[9],
                "description": row[10] or metadata.get("description") or company_name,
                "from_index_url": row[11],
                "company_name": company_name,
                "reporting_entity_name": metadata.get("reporting_entity_name")
                or _reporting_entity_from_plan_info(row[0], plan_info, plan_lookup),
                "content_length": row[4],
                "plan_info": plan_info,
                **context,
            }
        )
    return grouped


_IMPORT_CONTROL_STAGED_VISIBILITY = "internal"
_IMPORT_CONTROL_STAGED_STATUS = "needs_review"


def _import_control_source_identity_key(row: dict[str, Any]) -> str:
    index_url, official_url = _import_control_source_urls(row)
    return (
        _canonical_or_none(index_url or official_url)
        or str(row.get("source_id") or row.get("source_key") or "")
    )


def _import_control_source_promotion_rank(
    row: dict[str, Any],
    snapshot: dict[str, list[dict[str, Any]]],
) -> tuple[int, int, int, int]:
    status_rank = {
        "active": 5,
        "stale": 4,
        "needs_review": 3,
        "unsupported": 1,
        "archived": 0,
    }.get(str(row.get("status") or "").strip().lower(), 2)
    metadata = row.get("metadata_json") or {}
    aliases = metadata.get("aliases") or []
    if not isinstance(aliases, list):
        aliases = [aliases]
    source_id = str(row.get("source_id") or "")
    return (
        1 if snapshot.get(source_id) else 0,
        status_rank,
        len([alias for alias in aliases if str(alias or "").strip()]),
        1 if (row.get("index_url") or row.get("human_url")) else 0,
    )


def _dedupe_import_control_source_rows(
    source_rows: list[dict[str, Any]],
    snapshot: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    for row in source_rows:
        key = _import_control_source_identity_key(row)
        previous = by_key.get(key)
        if previous is None or _import_control_source_promotion_rank(
            row, snapshot
        ) > _import_control_source_promotion_rank(previous, snapshot):
            by_key[key] = row
    return list(by_key.values())


async def _promote_import_control_source(
    session: aiohttp.ClientSession,
    base: str,
    row: dict[str, Any],
    *,
    visibility: str = "public",
    status: str = "active",
    preserve_operator_state: bool = True,
) -> str | None:
    """Upsert a discovered source into import-control and return the import-control-derived
    source_id (it derives ids from the canonical URL, ignoring ours). The catalog sync stages
    sources as internal/needs_review and only flips them public after a full plan sync.
    """
    index_url, official_url = _import_control_source_urls(row)
    payload = {
        "index_url": index_url,
        "official_url": official_url,
        "source_key": row.get("source_key"),
        "display_name": row.get("display_name"),
        "payer_name": row.get("display_name"),
        "source_type": row.get("source_type"),
        "source_tier": _source_row_source_tier(row),
        "domain": row.get("domain"),
        "visibility": visibility,
        "status": status,
        "preserve_operator_state": preserve_operator_state,
        "metadata": {
            "hosting_platform": row.get("hosting_platform"),
            "healthcare_source_id": row.get("source_id"),
            "seed_provider": row.get("seed_provider"),
            "access_model": row.get("access_model"),
            "source_tier": _source_row_source_tier(row),
            "aliases": list(
                dict.fromkeys((row.get("metadata_json") or {}).get("aliases") or [])
            ),
            "benefit_lines": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("benefit_lines") or []
                )
            ),
            "source_coverage": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("source_coverage") or []
                )
            ),
            "vendor_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("vendor_names") or []
                )
            ),
            "network_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("network_names") or []
                )
            ),
            "plan_names": list(
                dict.fromkeys(
                    (row.get("metadata_json") or {}).get("plan_names") or []
                )
            ),
            "raw": (row.get("metadata_json") or {}).get("raw") or {},
        },
    }
    async with session.post(f"{base}/v1/catalog/sources", json=payload) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"import-control source promote failed: {resp.status} {text[:200]}"
            )
        data = await resp.json()
    return str(data.get("source_id") or "") or None


async def _fetch_import_control_source(
    session: aiohttp.ClientSession, base: str, ic_source_id: str
) -> dict[str, Any] | None:
    """Read the stored import-control source (the upsert response echoes the request payload,
    not the persisted operator-controlled visibility/status)."""
    async with session.get(f"{base}/v1/catalog/sources/{ic_source_id}") as resp:
        if resp.status == 404:
            return None
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"import-control source fetch failed: {resp.status} {text[:200]}"
            )
        data = await resp.json()
    return data if isinstance(data, dict) else None


async def _ingest_import_control_preview(
    session: aiohttp.ClientSession,
    base: str,
    ic_source_id: str,
    items: list[dict[str, Any]],
) -> int:
    preview = {"source_id": ic_source_id, "items": items}
    async with session.post(
        f"{base}/v1/ptg/discover/ingest-preview", json=preview
    ) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"import-control plan ingest failed: {resp.status} {text[:200]}"
            )
        data = await resp.json()
    counts = data.get("counts") or {}
    return int(counts.get("plans") or 0)


async def _mark_import_control_seed_promoted(
    session: aiohttp.ClientSession,
    base: str,
    row: dict[str, Any],
    promoted_source_id: str,
) -> bool:
    item = _import_control_seed_item(
        row,
        review_status="promoted",
        promoted_source_id=promoted_source_id,
    )
    if not item:
        return False
    async with session.post(
        f"{base}/v1/catalog/seeds/import",
        json={"seed_provider": "healthcare-mrf-api", "items": [item]},
    ) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(
                f"import-control promoted seed sync failed: {resp.status} {text[:200]}"
            )
        await resp.json()
    return True


async def _push_import_control_catalog(
    source_rows: list[dict[str, Any]], *, limit: int | None = None
) -> tuple[int, int, list[dict[str, Any]]]:
    """Promote eligible discovered sources into import-control and push their discovered
    plans (built from the stored MRF file snapshot). Returns
    (sources_synced, plans_synced, per-source errors).

    Publication is staged: each source is upserted as internal/needs_review first, its plans
    are ingested and the seed marked promoted, and only then is the source flipped public.
    A partial failure leaves the source non-public (needs_review) instead of exposing an
    incomplete plan catalog, and the source does not count as synced.

    Reuses import-control's non-destructive upsert (keyed on the stable discovered_plan_id),
    so manual fetch-preview plans and bulk-discovered plans converge instead of duplicating.
    """
    base_url = str(
        os.getenv("HLTHPRT_IMPORT_CONTROL_URL")
        or os.getenv("HP_IMPORT_CONTROL_BASE_URL")
        or ""
    ).strip()
    token = str(
        os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN")
        or os.getenv("HLTHPRT_CONTROL_API_TOKEN")
        or ""
    ).strip()
    if not base_url or not token:
        return (0, 0, [])
    eligible = [
        row
        for row in source_rows
        if _eligible_for_public_promotion(row)
        and (row.get("index_url") or row.get("human_url"))
    ]
    eligible = eligible[: limit or len(eligible)]
    if not eligible:
        return (0, 0, [])
    snapshot = await _import_control_snapshot_items(
        [str(row.get("source_id") or "") for row in eligible]
    )
    eligible = _dedupe_import_control_source_rows(eligible, snapshot)
    if not snapshot and all(
        str(row.get("status") or "active").strip().lower() == "active"
        and _source_row_is_importable(row)
        for row in eligible
    ):
        return (0, 0, [])
    base = base_url.rstrip("/")
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "User-Agent": USER_AGENT,
    }
    sources_synced = 0
    plans_synced = 0
    errors: list[dict[str, Any]] = []
    async with aiohttp.ClientSession(
        headers=headers, timeout=timeout, trust_env=False
    ) as session:
        for row in eligible:
            source_id = str(row.get("source_id") or "")
            items = snapshot.get(source_id) or []
            source_status = str(row.get("status") or "active").strip() or "active"
            evidence_only = not _source_row_is_importable(row)
            if not items and source_status.lower() == "active" and not evidence_only:
                continue
            ic_source_id: str | None = None
            try:
                # Stage the source non-public first so a mid-sync failure never leaves a
                # public source with an incomplete plan catalog.
                ic_source_id = await _promote_import_control_source(
                    session,
                    base,
                    row,
                    visibility=_IMPORT_CONTROL_STAGED_VISIBILITY,
                    status=_IMPORT_CONTROL_STAGED_STATUS,
                )
                if not ic_source_id:
                    continue
                stored = await _fetch_import_control_source(session, base, ic_source_id)
                # Flip staged rows public after ingest. Also let an active registry row with
                # crawled files clear a stale state left by an older duplicate URL row.
                staged = stored is None or (
                    str(stored.get("visibility") or "")
                    == _IMPORT_CONTROL_STAGED_VISIBILITY
                    and str(stored.get("status") or "") == _IMPORT_CONTROL_STAGED_STATUS
                )
                stored_status = str((stored or {}).get("status") or "").lower()
                source_status_lower = source_status.lower()
                source_plans = 0
                if items:
                    for batch in _chunked(_split_preview_items(items), 100):
                        source_plans += await _ingest_import_control_preview(
                            session, base, ic_source_id, batch
                        )
                    await _mark_import_control_seed_promoted(
                        session, base, row, ic_source_id
                    )
                elif evidence_only:
                    await _mark_import_control_seed_promoted(
                        session, base, row, ic_source_id
                    )
                if (
                    staged
                    or source_status_lower != "active"
                    or (items and stored_status == "stale")
                ):
                    await _promote_import_control_source(
                        session,
                        base,
                        row,
                        visibility="public",
                        status=source_status,
                        preserve_operator_state=False,
                    )
                sources_synced += 1
                plans_synced += source_plans
            except Exception as exc:  # pylint: disable=broad-exception-caught
                errors.append(
                    {
                        "source_id": source_id or None,
                        "import_control_source_id": ic_source_id,
                        "message": str(exc),
                    }
                )
                continue
    if errors and not sources_synced:
        raise RuntimeError(
            f"import-control catalog sync failed for all {len(errors)} source(s): "
            + "; ".join(str(item.get("message") or "") for item in errors)[:500]
        )
    return (sources_synced, plans_synced, errors)


def _discovery_run_params(
    *,
    test_mode: bool,
    provider: str | None,
    limit: int | None,
    source_entity_types: tuple[str, ...],
    source_payer_query: str | None,
    check_urls: bool,
    crawl: bool,
    probe_files: bool,
    file_probe_limit: int | None,
    file_probe_types: tuple[str, ...],
    file_probe_entity_types: tuple[str, ...],
    file_probe_payer_query: str | None,
    sync_import_control: bool,
    max_toc_bytes: int,
    concurrency: int,
    crawl_target_limit: int | None,
) -> dict[str, Any]:
    return {
        "test_mode": test_mode,
        "provider": provider,
        "limit": limit,
        "source_entity_types": list(source_entity_types),
        "source_payer_query": source_payer_query,
        "check_urls": check_urls,
        "crawl": crawl,
        "probe_files": probe_files,
        "file_probe_limit": file_probe_limit,
        "file_probe_types": list(file_probe_types),
        "file_probe_entity_types": list(file_probe_entity_types),
        "file_probe_payer_query": file_probe_payer_query,
        "sync_import_control": sync_import_control,
        "max_toc_bytes": max_toc_bytes,
        "concurrency": concurrency,
        "crawl_target_limit": crawl_target_limit,
    }


def _control_status_from_crawl_status(crawl_status: str) -> str:
    return "succeeded" if crawl_status == "succeeded_with_errors" else crawl_status


def _discovery_control_metrics(
    result: DiscoveryResult,
    *,
    crawl_status: str,
    crawl_run_id: str,
    run_mode: str,
    bytes_streamed: int = 0,
) -> dict[str, Any]:
    return {
        "crawl_run_id": crawl_run_id,
        "crawl_status": crawl_status,
        "run_mode": run_mode,
        "candidates": result.candidates,
        "payers": result.payers,
        "sources": result.sources,
        "urls_checked": result.urls_checked,
        "plans_discovered": result.plans,
        "files_discovered": result.files,
        "files_probed": result.files_probed,
        "file_probe_ok": result.file_probe_ok,
        "bytes_streamed": bytes_streamed,
        "import_control_synced": result.import_control_synced,
        "import_control_sources_synced": result.import_control_sources_synced,
        "import_control_plans_synced": result.import_control_plans_synced,
        "error_count": len(result.errors),
    }


def _emit_discovery_control_event(
    *,
    control_run_id: str,
    crawl_run_id: str,
    status: str,
    phase_detail: str,
    progress: dict[str, Any],
    params: dict[str, Any],
    started_at: dt.datetime,
    finished_at: dt.datetime | None = None,
    triggered_by: str | None = None,
    metrics: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
) -> None:
    if not control_run_id:
        return
    heartbeat_at = finished_at or _utc_now()
    payload = {
        "run_id": control_run_id,
        "importer": "mrf-source-discovery",
        "status": status,
        "phase_detail": phase_detail,
        "params": params,
        "progress": progress,
        "metrics": {"crawl_run_id": crawl_run_id, **(metrics or {})},
        "error": error,
        "created_at": started_at,
        "started_at": started_at,
        "heartbeat_at": heartbeat_at,
    }
    if finished_at is not None:
        payload["finished_at"] = finished_at
    if triggered_by:
        payload["triggered_by"] = triggered_by
    enqueue_status_event(payload)


async def _flush_discovery_control_events() -> None:
    timeout = float(
        os.getenv("HLTHPRT_MRF_DISCOVERY_CONTROL_EVENT_FLUSH_SECONDS", "1.0")
    )
    if timeout <= 0:
        return
    await flush_status_events(timeout_seconds=timeout)


async def main(
    test_mode: bool = False,
    provider: str | None = None,
    limit: int | None = None,
    source_entity_types: Any = None,
    source_payer_query: str | None = None,
    dry_run: bool = False,
    check_urls: bool = False,
    crawl: bool = False,
    probe_files: bool = False,
    file_probe_limit: int | None = None,
    file_probe_types: Any = None,
    file_probe_entity_types: Any = None,
    file_probe_payer_query: str | None = None,
    sync_import_control: bool = False,
    max_toc_bytes: int = MAX_TOC_BYTES_DEFAULT,
    concurrency: int = DEFAULT_CONCURRENCY,
    crawl_target_limit: int | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    max_toc_bytes = max_toc_bytes or MAX_TOC_BYTES_DEFAULT
    concurrency = max(1, int(concurrency or DEFAULT_CONCURRENCY))
    crawl_target_limit = max(1, int(crawl_target_limit)) if crawl_target_limit else None
    parsed_source_entity_types = _parse_text_filter_values(source_entity_types)
    parsed_source_payer_query = _normalize_text_query(source_payer_query)
    file_probe_limit = max(1, int(file_probe_limit)) if file_probe_limit else None
    parsed_file_probe_types = _parse_file_probe_types(file_probe_types)
    parsed_file_probe_entity_types = _parse_text_filter_values(file_probe_entity_types)
    parsed_file_probe_payer_query = _normalize_text_query(file_probe_payer_query)
    providers = _parse_provider_list(provider, test_mode=test_mode)
    bounded_limit = max(1, int(limit)) if limit else (25 if test_mode else None)
    result = DiscoveryResult(providers=providers)
    started_at = _utc_now()
    crawl_run_id = _id(
        "mrfcrawl",
        {
            "started_at": started_at.isoformat(),
            "providers": providers,
            "run_id": run_id,
        },
    )
    run_mode = _discovery_run_mode(
        crawl=crawl, check_urls=check_urls, probe_files=probe_files
    )
    result.crawl_run_id = crawl_run_id
    control_run_id = run_id or crawl_run_id
    run_params = _discovery_run_params(
        test_mode=test_mode,
        provider=provider,
        limit=limit,
        source_entity_types=parsed_source_entity_types,
        source_payer_query=parsed_source_payer_query,
        check_urls=check_urls,
        crawl=crawl,
        probe_files=probe_files,
        file_probe_limit=file_probe_limit,
        file_probe_types=parsed_file_probe_types,
        file_probe_entity_types=parsed_file_probe_entity_types,
        file_probe_payer_query=parsed_file_probe_payer_query,
        sync_import_control=sync_import_control,
        max_toc_bytes=max_toc_bytes,
        concurrency=concurrency,
        crawl_target_limit=crawl_target_limit,
    )
    emit_standalone_control_events = bool(not dry_run and not run_id)
    if emit_standalone_control_events:
        _emit_discovery_control_event(
            control_run_id=control_run_id,
            crawl_run_id=crawl_run_id,
            status="running",
            phase_detail="loading source providers",
            progress={
                "unit": "providers",
                "done": 0,
                "total": len(providers),
                "pct": 0,
                "message": "loading source providers",
                "phase": "loading source providers",
            },
            params=run_params,
            started_at=started_at,
            triggered_by="direct_cli" if not run_id else None,
        )
    if control_run_id and not dry_run:
        enqueue_live_progress(
            run_id=control_run_id,
            importer="mrf-source-discovery",
            status="running",
            phase="loading source providers",
            unit="providers",
            done=0,
            total=len(providers),
            message="loading source providers",
        )

    needs_source_load = bool(
        dry_run or check_urls or crawl or sync_import_control or not probe_files
    )
    candidates: list[SourceCandidate] = []
    if needs_source_load:
        provider_load_limit = (
            None
            if parsed_source_entity_types or parsed_source_payer_query
            else bounded_limit
        )
        for index, provider_name in enumerate(providers):
            try:
                candidates.extend(
                    await _load_candidates(
                        provider_name, test_mode=test_mode, limit=provider_load_limit
                    )
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                result.errors.append({"provider": provider_name, "message": str(exc)})
            if control_run_id and not dry_run:
                enqueue_live_progress(
                    run_id=control_run_id,
                    importer="mrf-source-discovery",
                    status="running",
                    phase="loading source providers",
                    unit="providers",
                    done=index + 1,
                    total=len(providers),
                    message=f"loaded {index + 1}/{len(providers)} providers",
                )
    candidates = _dedupe_candidates(candidates)
    if parsed_source_entity_types or parsed_source_payer_query:
        filtered_candidates: list[SourceCandidate] = []
        query_expansion_candidates: list[SourceCandidate] = []
        query_can_expand = bool(
            parsed_source_payer_query
            and sum(len(token) for token in _query_tokens(parsed_source_payer_query)) >= 3
        )
        for candidate in candidates:
            if _candidate_matches_text_filters(
                candidate,
                entity_types=parsed_source_entity_types,
                payer_query=parsed_source_payer_query,
            ):
                filtered_candidates.append(candidate)
                continue
            if query_can_expand and _candidate_supports_source_query_expansion(candidate):
                query_expansion_candidates.append(
                    _candidate_with_target_payer_query(
                        candidate, parsed_source_payer_query or ""
                    )
                )
        candidates = _dedupe_candidates(filtered_candidates + query_expansion_candidates)
    candidates = [
        candidate
        for candidate in candidates
        if _candidate_has_catalog_source(candidate)
    ]
    if bounded_limit:
        candidates = candidates[:bounded_limit]
    result.candidates = len(candidates)

    if dry_run:
        result.payers = len(
            {_clean_text(candidate.payer_name).lower() for candidate in candidates}
        )
        result.sources = len(
            [
                candidate
                for candidate in candidates
                if candidate.index_url or candidate.human_url
            ]
        )
        return result.as_dict()

    await init_db(db, asyncio.get_event_loop())
    await ensure_database(test_mode)
    await _ensure_catalog_tables()
    await push_objects(
        [
            {
                "crawl_run_id": crawl_run_id,
                "run_id": control_run_id,
                "provider": ",".join(providers),
                "mode": run_mode,
                "status": "running",
                "started_at": started_at,
                "params": run_params,
                "sources_discovered": 0,
                "urls_checked": 0,
                "etag_skipped": 0,
                "plans_discovered": 0,
                "files_discovered": 0,
                "bytes_streamed": 0,
                "errors": [],
            }
        ],
        MRFCrawlRun,
        rewrite=True,
        use_copy=False,
    )

    payer_rows, source_rows = await _store_candidates(candidates)
    result.payers = len(payer_rows)
    result.sources = len(source_rows)
    observations: list[dict[str, Any]] = []
    observation_run_id = control_run_id
    progress_run_id = control_run_id
    if check_urls:
        observations = await _store_observations(
            source_rows,
            test_mode=test_mode,
            run_id=observation_run_id,
            progress_run_id=progress_run_id,
            concurrency=concurrency,
        )
        result.urls_checked = len(observations)
    if crawl:
        plans_discovered, files_discovered, crawl_observations = (
            await _crawl_toc_metadata(
                [row for row in source_rows if _source_row_is_importable(row)],
                test_mode=test_mode,
                run_id=observation_run_id,
                progress_run_id=progress_run_id,
                max_toc_bytes=max_toc_bytes,
                concurrency=concurrency,
                crawl_target_limit=crawl_target_limit,
            )
        )
        observations.extend(crawl_observations)
        result.plans = plans_discovered
        result.files = files_discovered
    if probe_files:
        probe_observations, ok_count = await _probe_mrf_file_heads(
            file_types=parsed_file_probe_types,
            limit=file_probe_limit,
            entity_types=parsed_file_probe_entity_types,
            payer_query=parsed_file_probe_payer_query,
            run_id=observation_run_id,
            progress_run_id=progress_run_id,
            concurrency=concurrency,
        )
        observations.extend(probe_observations)
        result.files_probed = len(probe_observations)
        result.file_probe_ok = ok_count
        result.urls_checked += len(probe_observations)
    if sync_import_control:
        try:
            result.import_control_synced = await _sync_import_control_seeds(
                source_rows, limit=bounded_limit
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result.errors.append({"provider": "import-control", "message": str(exc)})
        try:
            sources_synced, plans_synced, catalog_errors = (
                await _push_import_control_catalog(source_rows, limit=bounded_limit)
            )
            result.import_control_sources_synced = sources_synced
            result.import_control_plans_synced = plans_synced
            for item in catalog_errors:
                result.errors.append({"provider": "import-control-catalog", **item})
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result.errors.append(
                {"provider": "import-control-catalog", "message": str(exc)}
            )

    crawl_status = "succeeded" if not result.errors else "succeeded_with_errors"
    finished_at = _utc_now()
    bytes_streamed = sum(int(item.get("content_length") or 0) for item in observations)
    await push_objects(
        [
            {
                "crawl_run_id": crawl_run_id,
                "run_id": control_run_id,
                "provider": ",".join(providers),
                "mode": run_mode,
                "status": crawl_status,
                "started_at": started_at,
                "finished_at": finished_at,
                "params": run_params,
                "sources_discovered": result.sources,
                "urls_checked": result.urls_checked,
                "etag_skipped": 0,
                "plans_discovered": result.plans,
                "files_discovered": result.files,
                "bytes_streamed": bytes_streamed,
                "errors": result.errors,
            }
        ],
        MRFCrawlRun,
        rewrite=True,
        use_copy=False,
    )
    if control_run_id:
        enqueue_live_progress(
            run_id=control_run_id,
            importer="mrf-source-discovery",
            status="succeeded",
            phase="mrf source discovery complete",
            unit="sources",
            done=result.sources,
            total=result.sources,
            pct=100,
            message="mrf source discovery complete",
        )
        if emit_standalone_control_events:
            _emit_discovery_control_event(
                control_run_id=control_run_id,
                crawl_run_id=crawl_run_id,
                status=_control_status_from_crawl_status(crawl_status),
                phase_detail="mrf source discovery complete",
                progress={
                    "unit": "sources",
                    "done": result.sources,
                    "total": result.sources,
                    "pct": 100,
                    "message": "mrf source discovery complete",
                    "phase": "mrf source discovery complete",
                },
                params=run_params,
                metrics=_discovery_control_metrics(
                    result,
                    crawl_status=crawl_status,
                    crawl_run_id=crawl_run_id,
                    run_mode=run_mode,
                    bytes_streamed=bytes_streamed,
                ),
                started_at=started_at,
                finished_at=finished_at,
            )
            await _flush_discovery_control_events()
    return result.as_dict()


async def process_data(
    ctx: dict[str, Any] | None = None, task: dict[str, Any] | None = None
) -> dict[str, Any]:
    task = task or {}
    run_id = (
        str(
            task.get("run_id")
            or (ctx or {}).get("control_run_id")
            or (ctx or {}).get("context", {}).get("control_run_id")
            or ""
        ).strip()
        or None
    )
    return await main(
        test_mode=bool(task.get("test_mode", task.get("test", False))),
        provider=task.get("provider"),
        limit=_as_int(task.get("limit")),
        source_entity_types=task.get("source_entity_types"),
        source_payer_query=task.get("source_payer_query"),
        dry_run=bool(task.get("dry_run", False)),
        check_urls=bool(task.get("check_urls", False)),
        crawl=bool(task.get("crawl", False)),
        probe_files=bool(task.get("probe_files", False)),
        file_probe_limit=_as_int(task.get("file_probe_limit")),
        file_probe_types=task.get("file_probe_types"),
        file_probe_entity_types=task.get("file_probe_entity_types"),
        file_probe_payer_query=task.get("file_probe_payer_query"),
        sync_import_control=bool(
            task.get(
                "sync_import_control",
                _env_flag("HLTHPRT_MRF_DISCOVERY_SYNC_IMPORT_CONTROL_DEFAULT"),
            )
        ),
        max_toc_bytes=int(task.get("max_toc_bytes") or MAX_TOC_BYTES_DEFAULT),
        concurrency=int(task.get("concurrency") or DEFAULT_CONCURRENCY),
        crawl_target_limit=_as_int(task.get("crawl_target_limit")),
        run_id=run_id,
    )


async def startup(ctx: dict[str, Any]) -> None:
    loop = asyncio.get_event_loop()
    ctx.setdefault("context", {})
    await init_db(db, loop)


async def shutdown(_ctx: dict[str, Any]) -> None:
    return None
