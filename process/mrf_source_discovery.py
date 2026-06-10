# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Lightweight payer MRF source discovery and freshness catalog import."""

from __future__ import annotations

import asyncio
import datetime as dt
import html
import ipaddress
import json
import os
import re
import socket
import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit

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
MAX_TOC_BYTES_DEFAULT = int(os.getenv("HLTHPRT_MRF_DISCOVERY_MAX_TOC_BYTES", str(25 * 1024 * 1024)))
DEFAULT_CONCURRENCY = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_CONCURRENCY", "10")), 1)
WRITE_BATCH_SIZE = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_WRITE_BATCH_SIZE", "2000")), 1)
HTTP_TOTAL_TIMEOUT = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_HTTP_TIMEOUT", "300")), 1)
HTTP_READ_TIMEOUT = max(int(os.getenv("HLTHPRT_MRF_DISCOVERY_READ_TIMEOUT", "120")), 1)
DEFAULT_FILE_PROBE_TYPES = ("in-network", "allowed-amounts")
USER_AGENT = "HealthPorta mrf-source-discovery/1.0"
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
    states: tuple[str, ...] = ()
    eins: tuple[str, ...] = ()
    source_coverage: tuple[str, ...] = ()
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
    return aiohttp.TCPConnector(limit=limit, family=socket.AF_INET, ttl_dns_cache=300, ssl=_default_ssl_context())


def _source_config() -> dict[str, Any]:
    global _SOURCE_CONFIG_CACHE  # pylint: disable=global-statement
    if _SOURCE_CONFIG_CACHE is None:
        _SOURCE_CONFIG_CACHE = json.loads(_source_config_path().read_text(encoding="utf-8"))
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


def _parse_size_bytes(value: Any) -> int | None:
    parsed = _as_int(value)
    if parsed is not None:
        return parsed
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    match = re.match(r"^(?P<number>\d+(?:\.\d+)?)\s*(?P<unit>[kmgtp]?i?b|bytes?)?$", text, flags=re.I)
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


def _container_format(url: str | None) -> str | None:
    path = urlsplit(str(url or "")).path.lower()
    if path.endswith(".zip"):
        return "zip"
    if path.endswith((".json.gz", ".gz")):
        return "gzip"
    return None


def _mrf_file_type_from_text(url: str | None, label: str | None = None) -> str | None:
    text = f"{url or ''} {label or ''}".lower().replace("_", "-")
    if "allowed-amount" in text or "allowed amount" in text:
        return "allowed-amounts"
    if "in-network" in text or "in network" in text or "negotiated-rate" in text:
        return "in-network"
    if "payer-drug" in text or "prescription-drug" in text or "drug-file" in text:
        return "payer-drug"
    if "table-of-content" in text or "toc" in text or re.search(r"(^|[/_-])index(?:\.json|[/?#]|$)", text):
        return "table-of-contents"
    return None


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
    if host in {"transparency-in-coverage.uhc.com", "transparency-in-coverage.optum.com"}:
        return "uhc_public_blobs"
    if host == "transparency-in-coverage.bluecrossma.com":
        return "bcbsma_monthly_tocs"
    if host.endswith("cigna.com") and ("machine-readable" in path or "/static/mrf/" in path):
        return "cigna_static_mrf_lookup"
    if "healthsparq.com" in host:
        return "healthsparq"
    if "sapphiremrfhub.com" in host:
        return "sapphire"
    if "health1.aetna.com" in host or "health1.firsthealth.com" in host:
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


def _master_list_url_cell(cells: list[str], type_value: str | None) -> tuple[str | None, str, str]:
    if len(cells) == 3:
        middle, tail = cells[1], cells[2]
        if _http_urls(middle) or _is_placeholder_source_url(middle) or middle.strip() in {"", "-", "—"}:
            return type_value, middle, tail
        return type_value, tail, ""
    second, third, fourth = cells[1], cells[2], cells[3]
    normalized_type = _normalize_entity_type(second) or type_value
    if _http_urls(third) or _is_placeholder_source_url(third) or third.strip() in {"", "-", "—"}:
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
        urls = tuple(url for url in _http_urls(url_cell) if not _is_placeholder_source_url(url)) or (None,)
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
    for item in candidates:
        key = (_clean_text(item.payer_name).lower(), _canonical_or_none(item.index_url or item.human_url))
        previous = by_key.get(key)
        previous_url = previous.index_url or previous.human_url if previous else None
        item_url = item.index_url or item.human_url
        if (
            previous is None
            or (not previous_url and item_url)
            or (previous_url and item_url and len(item_url) > len(previous_url))
        ):
            by_key[key] = item
    return list(by_key.values())


def _parse_provider_list(provider: str | None, *, test_mode: bool) -> list[str]:
    if not provider or provider == "all":
        return _configured_provider_names("test_providers" if test_mode else "default_providers")
    parts = [item.strip().lower() for item in re.split(r"[, ]+", provider) if item.strip()]
    return parts or _configured_provider_names("test_providers" if test_mode else "default_providers")


async def _fetch_text(
    url: str,
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
    expect_json: bool = False,
) -> str:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT)
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, connector=connector, trust_env=False) as owned_session:
            return await _fetch_text(url, max_bytes=max_bytes, session=owned_session, expect_json=expect_json)
    async with session.get(url, allow_redirects=True) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if expect_json and any(marker in content_type for marker in ("text/html", "application/xhtml", "application/pdf", "xml")):
            raise ValueError(f"response content-type is not JSON: {content_type or 'unknown'}")
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
        charset = resp.charset or "utf-8"
    return b"".join(chunks).decode(charset, errors="replace")


async def _fetch_json(url: str, *, max_bytes: int = MAX_TOC_BYTES_DEFAULT, session: aiohttp.ClientSession | None = None) -> dict[str, Any]:
    text = await _fetch_text(url, max_bytes=max_bytes, session=session, expect_json=True)
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


async def _post_json(
    url: str,
    payload: dict[str, Any],
    *,
    max_bytes: int = MAX_TOC_BYTES_DEFAULT,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, Any]:
    await _assert_fetch_url_allowed(url)
    if session is None:
        timeout = aiohttp.ClientTimeout(total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT)
        connector = _tcp_connector(limit=0)
        async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, connector=connector, trust_env=False) as owned_session:
            return await _post_json(url, payload, max_bytes=max_bytes, session=owned_session)
    async with session.post(url, json=payload, allow_redirects=True) as resp:
        await _assert_fetch_url_allowed(str(resp.url))
        content_type = str(resp.headers.get("Content-Type") or "").lower()
        if any(marker in content_type for marker in ("text/html", "application/xhtml", "application/pdf", "xml")):
            raise ValueError(f"response content-type is not JSON: {content_type or 'unknown'}")
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
    data = json.loads(b"".join(chunks).decode(charset, errors="replace"))
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


async def _load_candidates(provider: str, *, test_mode: bool, limit: int | None) -> list[SourceCandidate]:
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
    infos = await asyncio.to_thread(socket.getaddrinfo, hostname, parsed.port or (443 if parsed.scheme == "https" else 80), type=socket.SOCK_STREAM)
    for info in infos:
        address = info[4][0]
        try:
            _assert_public_ip(ipaddress.ip_address(address))
        except ValueError as exc:
            raise ValueError(f"URL resolves to a non-public address: {address}") from exc


def _assert_public_ip(ip: ipaddress._BaseAddress) -> None:
    if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
        raise ValueError(f"non-public IP address is not allowed: {ip}")


async def _head_url(url: str, session: aiohttp.ClientSession | None = None) -> dict[str, Any]:
    checked_at = _utc_now()
    try:
        await _assert_fetch_url_allowed(url)
        if session is None:
            timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=15)
            async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, trust_env=False) as owned_session:
                return await _head_url(url, owned_session)
        async with session.head(url, allow_redirects=True) as resp:
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


def _candidate_to_rows(candidate: SourceCandidate, now: dt.datetime) -> tuple[dict[str, Any], dict[str, Any] | None]:
    payer_id = _id("mrfpayer", _clean_text(candidate.payer_name).lower())
    source_url = candidate.index_url or candidate.human_url
    source_id = _id("mrfsource", {"payer": payer_id, "url": _canonical_or_none(source_url), "provider": candidate.provider})
    payer_row = {
        "payer_id": payer_id,
        "canonical_name": _clean_text(candidate.payer_name),
        "aliases": [candidate.payer_name],
        "parent_group": candidate.parent_group,
        "entity_type": candidate.entity_type,
        "states": list(candidate.states),
        "eins": list(candidate.eins),
        "lifecycle": "active",
        "source_coverage": list(candidate.source_coverage),
        "metadata_json": {"providers": [candidate.provider], "raw": candidate.raw_payload},
        "created_at": now,
        "updated_at": now,
    }
    if not source_url:
        return payer_row, None
    source_key_base = _slug(f"{candidate.provider}-{candidate.payer_name}-{_domain(source_url) or ''}")
    source_key = f"{source_key_base[:80]}-{source_id[-8:]}"
    source_row = {
        "source_id": source_id,
        "payer_id": payer_id,
        "source_key": source_key,
        "display_name": _clean_text(candidate.payer_name),
        "source_type": candidate.source_type,
        "hosting_platform": candidate.hosting_platform or classify_hosting_platform(source_url),
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
        "metadata_json": {"source_coverage": list(candidate.source_coverage), "raw": candidate.raw_payload},
        "created_at": now,
        "updated_at": now,
    }
    return payer_row, source_row


async def _store_candidates(candidates: list[SourceCandidate]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = _utc_now()
    payer_rows_by_id: dict[str, dict[str, Any]] = {}
    source_rows_by_id: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        payer_row, source_row = _candidate_to_rows(candidate, now)
        existing = payer_rows_by_id.get(payer_row["payer_id"])
        if existing:
            existing["aliases"] = sorted(set((existing.get("aliases") or []) + (payer_row.get("aliases") or [])))
            existing["source_coverage"] = sorted(set((existing.get("source_coverage") or []) + (payer_row.get("source_coverage") or [])))
            existing["metadata_json"] = {
                **dict(existing.get("metadata_json") or {}),
                "providers": sorted(set((existing.get("metadata_json") or {}).get("providers", []) + [candidate.provider])),
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
    items = [(source, source.get("index_url") or source.get("human_url")) for source in source_rows if source.get("index_url") or source.get("human_url")]
    total = len(items)
    semaphore = asyncio.Semaphore(max(1, int(concurrency or DEFAULT_CONCURRENCY)))
    timeout = aiohttp.ClientTimeout(total=30, connect=10, sock_read=15)
    connector = _tcp_connector(limit=max(1, int(concurrency or DEFAULT_CONCURRENCY)) * 2)

    async def check_one(source: dict[str, Any], url: Any, session: aiohttp.ClientSession) -> dict[str, Any]:
        async with semaphore:
            if test_mode:
                head = {"status": "skipped_test_mode", "checked_at": _utc_now()}
            else:
                head = await _head_url(str(url), session=session)
            return {
                "observation_id": _id("mrfurlobs", {"source_id": source["source_id"], "url": url, "checked_at": head["checked_at"].isoformat()}),
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

    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, connector=connector, trust_env=False) as session:
        tasks = [asyncio.create_task(check_one(source, url, session)) for source, url in items]
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


def _healthsparq_rows_from_metadata(source: dict[str, Any], metadata_url: str, payload: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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
        reporting_plans = [plan for plan in (file_item.get("reportingPlans") or []) if isinstance(plan, dict)]
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
                "metadata_json": {"raw_plan": plan, "resolver": "healthsparq_public_mrf"},
                "first_seen_at": now,
                "last_seen_at": now,
            }
        file_type = _healthsparq_file_type(file_item.get("fileSchema"))
        canonical_url = _canonical_or_none(file_url) or file_url
        file_row_id = _id("mrffile", {"source": source["source_id"], "type": file_type, "url": canonical_url})
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
            "plan_ids": [plan.get("planId") for plan in reporting_plans if plan.get("planId")],
            "plan_names": [plan.get("planName") for plan in reporting_plans if plan.get("planName")],
            "market_types": sorted({plan.get("planMarketType") for plan in reporting_plans if plan.get("planMarketType")}),
            "is_signed_url": _looks_signed(file_url),
            "size_bytes": None,
            "schema_version": None,
            "metadata_json": {
                "resolver": "healthsparq_public_mrf",
                "container_format": _container_format(file_url),
                "file_path": file_item.get("filePath"),
                "file_schema": file_item.get("fileSchema"),
                "last_updated_on": file_item.get("lastUpdatedOn"),
                "reporting_entity_name": file_item.get("reportingEntityName"),
                # Normalized to the import-control preview plan shape (snake_case keys).
                "plan_info": [
                    {
                        "plan_id": plan.get("planId"),
                        "plan_id_type": plan.get("planIdType"),
                        "plan_market_type": plan.get("planMarketType"),
                        "plan_name": plan.get("planName"),
                    }
                    for plan in reporting_plans
                ],
            },
            "first_seen_at": now,
            "last_seen_at": now,
        }
    return list(plan_rows_by_id.values()), list(file_rows_by_id.values())


def _toc_rows_from_content(source: dict[str, Any], url: str, toc: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(toc.get("files"), list):
        return _healthsparq_rows_from_metadata(source, url, toc)
    plan_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    entries = parse_toc_catalog_entries(toc, str(url))
    schema_version = str(toc.get("version") or "")
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
        file_row_id = _id("mrffile", {"source": source["source_id"], "type": entry.source_type, "url": entry.canonical_url})
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
                "plan_ids": [plan.get("plan_id") for plan in plan_info if plan.get("plan_id")],
                "plan_names": [plan.get("plan_name") for plan in plan_info if plan.get("plan_name")],
                "market_types": sorted({plan.get("plan_market_type") for plan in plan_info if plan.get("plan_market_type")}),
                "is_signed_url": _looks_signed(entry.original_url),
                "size_bytes": None,
                "schema_version": schema_version or None,
                "metadata_json": {
                    "container_format": _container_format(entry.original_url),
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
    if not path.endswith((".json", ".json.gz", ".zip")):
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


def _metadata_text_rows_from_content(source: dict[str, Any], url: str, text: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    now = _utc_now()
    plan_rows_by_id: dict[str, dict[str, Any]] = {}
    file_rows_by_id: dict[str, dict[str, Any]] = {}
    for line in (text or "").splitlines():
        file_url = _first_http_url(line)
        if not file_url or not _looks_direct_mrf_body_url(file_url):
            continue
        fields = _metadata_text_fields(line)
        file_type = _metadata_text_file_type(fields.get("file scope") or fields.get("scope"))
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
        file_row_id = _id("mrffile", {"source": source["source_id"], "type": file_type, "url": canonical_url})
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


def _parse_highmark_hmhs_script(script_text: str, *, base_url: str, month_start: str | None = None) -> list[dict[str, Any]]:
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


def _parse_sapphire_toc_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    urls: dict[str, dict[str, Any]] = {}
    for href in re.findall(r"""href=["']([^"']+)["']""", html_text or "", flags=re.I):
        href = html.unescape(href).strip()
        if not href:
            continue
        url = urljoin(base_url, href)
        path = urlsplit(url).path.lower()
        if "/tocs/" not in path or not path.endswith(".json"):
            continue
        urls[_canonical_or_none(url) or url] = {
            "url": url,
            "label": _label_from_index_name(Path(path).name),
        }
    return list(urls.values())


def _strip_html_tags(value: str) -> str:
    return _clean_text(re.sub(r"<[^>]+>", " ", value or ""))


def _html_link_candidates(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in re.finditer(r"<a\b(?P<attrs>[^>]*)>(?P<label>.*?)</a>", html_text or "", flags=re.I | re.S):
        attrs = match.group("attrs") or ""
        label = _strip_html_tags(match.group("label") or "")
        href_match = re.search(r"""href\s*=\s*(?P<quote>["'])(?P<value>.*?)(?P=quote)""", attrs, flags=re.I | re.S)
        if href_match:
            candidates.append({"attr": "href", "value": href_match.group("value"), "label": label})
    attr_pattern = re.compile(
        r"""\b(?P<attr>src|data-[a-z0-9_-]*(?:url|href|file|path|lookup)[a-z0-9_-]*)\s*=\s*(?P<quote>["'])(?P<value>.*?)(?P=quote)""",
        flags=re.I | re.S,
    )
    for match in attr_pattern.finditer(html_text or ""):
        candidates.append({"attr": match.group("attr"), "value": match.group("value"), "label": ""})
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
        normalized.append({"attr": key[0], "url": url, "label": item.get("label") or Path(urlsplit(url).path).name})
    return normalized


def _looks_html_mrf_toc_url(url: str | None, label: str | None = None) -> bool:
    path = urlsplit(str(url or "")).path.lower()
    if not path.endswith(".json"):
        return False
    text = f"{path} {label or ''}".lower().replace("_", "-")
    return any(token in text for token in ("index.json", "toc", "table-of-contents", "table of contents"))


def _looks_html_mrf_body_reference(url: str | None, label: str | None = None) -> bool:
    if not _looks_direct_mrf_body_url(url):
        return False
    path = urlsplit(str(url or "")).path.lower()
    text = f"{path} {label or ''}".lower().replace("_", "-")
    return any(
        token in text
        for token in (
            "allowed",
            "in-network",
            "in network",
            "negotiated",
            "rate",
            "rates",
            "mrf",
            "machine-readable",
            "transparency",
        )
    )


def _parse_html_mrf_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    urls: dict[tuple[str, str], dict[str, Any]] = {}
    for candidate in _html_link_candidates(html_text, base_url=base_url):
        url = str(candidate["url"])
        label = _clean_text(candidate.get("label")) or Path(urlsplit(url).path).name or "MRF file"
        attr = str(candidate.get("attr") or "")
        path = urlsplit(url).path.lower()
        label_or_path = f"{path} {label}".lower()
        target_kind: str | None = None
        target_file_type: str | None = None
        resolver = "html_mrf_link"
        if path.endswith(".txt") and any(token in label_or_path for token in ("meta", "mrf", "machine-readable", "transparency")):
            target_kind = "metadata_text"
            target_file_type = "metadata-index"
            resolver = "html_metadata_text"
        elif _looks_html_mrf_toc_url(url, label):
            target_kind = "toc_json"
            target_file_type = "table-of-contents"
        elif _looks_html_mrf_body_reference(url, label):
            target_kind = "file_reference"
            target_file_type = _mrf_file_type_from_text(url, label)
            if not target_file_type:
                continue
            resolver = "html_file_reference"
        if not target_kind or not target_file_type:
            continue
        key = (target_kind, _canonical_or_none(url) or url)
        urls[key] = {
            "url": url,
            "label": label,
            "resolver": resolver,
            "target_kind": target_kind,
            "target_file_type": target_file_type,
            "container_format": _container_format(url),
            "html_attr": attr,
        }
    return list(urls.values())


def _parse_html_mrf_metadata_links(html_text: str, *, base_url: str) -> list[dict[str, Any]]:
    return [
        {"url": target["url"], "label": Path(urlsplit(str(target["url"])).path).name or target["label"]}
        for target in _parse_html_mrf_links(html_text, base_url=base_url)
        if target.get("target_kind") == "metadata_text"
    ]


def _cigna_lookup_urls_from_html(html_text: str, *, base_url: str, resolver: dict[str, Any]) -> list[str]:
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
    value = _first_dict_value(item, ("url", "download_url", "downloadUrl", "location", "file_url", "fileUrl"))
    return str(value).strip() if value else None


def _parse_cigna_lookup_targets(
    payload: dict[str, Any],
    *,
    lookup_url: str,
    source: dict[str, Any],
    resolver: dict[str, Any],
) -> list[CrawlTarget]:
    toc_max_bytes = _parse_size_bytes(resolver.get("toc_max_bytes")) or 100 * 1024 * 1024
    groups = payload.get("mrfs") if isinstance(payload.get("mrfs"), list) else None
    if groups is None:
        groups = payload.get("mrf") if isinstance(payload.get("mrf"), list) else None
    if groups is None:
        groups = [payload]
    targets_by_url: dict[str, CrawlTarget] = {}
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_files = group.get("files") if isinstance(group.get("files"), list) else None
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
            if not (_looks_direct_toc_url(file_url) or _mrf_file_type_from_text(file_url, file_name) == "table-of-contents"):
                continue
            size_bytes = _parse_size_bytes(
                _first_dict_value(file_item, ("file_size", "fileSize", "size", "content_length", "contentLength"))
                or _first_dict_value(group, ("file_size", "fileSize", "size", "content_length", "contentLength"))
            )
            reporting_entity_name = _first_dict_value(
                file_item,
                ("reporting_entity_name", "reportingEntityName", "entity_name", "entityName"),
            ) or _first_dict_value(group, ("reporting_entity_name", "reportingEntityName", "entity_name", "entityName"))
            reporting_entity_type = _first_dict_value(
                file_item,
                ("reporting_entity_type", "reportingEntityType", "entity_type", "entityType"),
            ) or _first_dict_value(group, ("reporting_entity_type", "reportingEntityType", "entity_type", "entityType"))
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
                "last_updated_on": _first_dict_value(file_item, ("last_updated_on", "lastUpdatedOn"))
                or _first_dict_value(group, ("last_updated_on", "lastUpdatedOn")),
                "reporting_month": _first_dict_value(file_item, ("reporting_month", "reportingMonth"))
                or _first_dict_value(group, ("reporting_month", "reportingMonth")),
                "product_type": _first_dict_value(file_item, ("product_type", "productType"))
                or _first_dict_value(group, ("product_type", "productType")),
            }
            key = _canonical_or_none(file_url) or file_url
            targets_by_url[key] = CrawlTarget(
                source=source,
                url=file_url,
                label=file_name or str(source.get("display_name") or "Cigna MRF index"),
                resolved_from_url=lookup_url,
                metadata={key: value for key, value in metadata.items() if value not in (None, "")},
            )
    return list(targets_by_url.values())


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
    suffixes = [str(item).strip() for item in (resolver.get("toc_suffixes") or ()) if str(item).strip()]
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
                        "issuer_slug": suffix.removeprefix("-01_").removesuffix("_index.json"),
                    },
                )
            )
    return targets


async def _resolve_cigna_static_mrf_lookup(
    source: dict[str, Any],
    url: str,
    resolver: dict[str, Any],
    session: aiohttp.ClientSession,
) -> list[CrawlTarget]:
    html_text = await _fetch_text(url, max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024), session=session)
    lookup_urls = _cigna_lookup_urls_from_html(html_text, base_url=url, resolver=resolver)
    if not lookup_urls:
        raise ValueError(f"no Cigna MRF lookup URLs found in {url}")
    lookup_max_bytes = int(resolver.get("lookup_max_bytes") or 2 * 1024 * 1024)
    targets: list[CrawlTarget] = []
    for lookup_url in lookup_urls:
        payload = await _fetch_json(lookup_url, max_bytes=lookup_max_bytes, session=session)
        targets.extend(_parse_cigna_lookup_targets(payload, lookup_url=lookup_url, source=source, resolver=resolver))
    if not targets:
        raise ValueError(f"no Cigna MRF index URLs found from {url}")
    return targets


def _healthsparq_public_params(url: str) -> dict[str, str]:
    raw = html.unescape(str(url or ""))
    match = re.search(r"#/(?:one|public)/([^/?#]+)", raw)
    param_text = match.group(1) if match else urlsplit(raw).query
    param_text = param_text.split("/", 1)[0]
    params = {key: value for key, value in parse_qsl(param_text, keep_blank_values=False) if key and value}
    if "insurerCode" not in params or "brandCode" not in params:
        raise ValueError("HealthSparq public URL must include insurerCode and brandCode")
    return params


def _url_origin(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("URL origin is required")
    return f"{parsed.scheme}://{parsed.netloc}"


def _healthsparq_service_url(source_url: str, resolver: dict[str, Any], path_key: str) -> str:
    service_path = str(resolver.get("public_service_path") or "/healthsparq/public/service").strip()
    path = str(resolver.get(path_key) or "").strip()
    if not path:
        raise ValueError(f"HealthSparq resolver missing {path_key}")
    return urljoin(_url_origin(source_url) + "/", f"{service_path.strip('/')}/{path.lstrip('/')}")


def _healthsparq_tenant(params: dict[str, str], resolver: dict[str, Any] | None = None) -> str:
    resolver = resolver or {}
    overrides = resolver.get("tenant_overrides") if isinstance(resolver.get("tenant_overrides"), dict) else {}
    insurer_code = str(params.get("insurerCode") or "").strip()
    for key in (insurer_code, insurer_code.upper(), insurer_code.lower()):
        override = str(overrides.get(key) or "").strip()
        if override:
            return override
    tenant = str(params.get("insurerCode") or "").strip().lower()
    tenant = re.sub(r"_i$", "", tenant)
    return re.sub(r"[^a-z0-9]+", "-", tenant).strip("-")


def _healthsparq_direct_metadata_url(resolver: dict[str, Any], params: dict[str, str]) -> str | None:
    template = str(resolver.get("metadata_url_template") or "").strip()
    if not template:
        return None
    tenant = _healthsparq_tenant(params, resolver)
    if not tenant:
        return None
    return template.format(tenant=tenant, insurerCode=params["insurerCode"], brandCode=params["brandCode"])


def _healthsparq_target(source: dict[str, Any], metadata_url: str, resolved_from_url: str, params: dict[str, str]) -> CrawlTarget:
    return CrawlTarget(
        source=source,
        url=metadata_url,
        label=str(source.get("display_name") or params["brandCode"]),
        resolved_from_url=resolved_from_url,
        metadata={
            "resolver": "healthsparq_public_mrf",
            "insurer_code": params["insurerCode"],
            "brand_code": params["brandCode"],
        },
    )


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
        return [_healthsparq_target(source, metadata_url, url, params)]
    login_url = _healthsparq_service_url(url, resolver, "login_path")
    login_query = {"_": str(int(_utc_now().timestamp() * 1000)), **params}
    await _fetch_json(f"{login_url}?{urlencode(login_query)}", max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024), session=session)
    mrf_all_url = _healthsparq_service_url(url, resolver, "mrf_all_path")
    payload = await _post_json(
        mrf_all_url,
        {"brandCode": params["brandCode"], "insurerCode": params["insurerCode"]},
        max_bytes=int(resolver.get("max_bytes") or 50 * 1024 * 1024),
        session=session,
    )
    metadata_url = str(payload.get("url") or "").strip()
    if not metadata_url:
        raise ValueError("HealthSparq public MRF API did not return a metadata URL")
    return [_healthsparq_target(source, metadata_url, mrf_all_url, params)]


def _looks_direct_toc_url(url: str | None) -> bool:
    path = urlsplit(str(url or "")).path.lower()
    return path.endswith(".json")


async def _crawl_targets_for_source(source: dict[str, Any], url: str, session: aiohttp.ClientSession) -> list[CrawlTarget]:
    platform = source.get("hosting_platform") or classify_hosting_platform(url)
    resolver = _platform_resolver_config(str(platform) if platform else None)
    resolver_type = str(resolver.get("type") or "").strip()
    if resolver_type == "bcbsma_monthly_tocs":
        return _bcbsma_monthly_toc_targets(source, url, resolver)
    if resolver_type == "cigna_static_mrf_lookup":
        return await _resolve_cigna_static_mrf_lookup(source, url, resolver, session)
    if resolver_type == "healthsparq_public_mrf":
        return await _resolve_healthsparq_public_mrf(source, url, resolver, session)
    if resolver_type == "highmark_hmhs_script":
        script_path = str(resolver.get("script_path") or "/js/script.js")
        script_url = urljoin(url, script_path)
        script_text = await _fetch_text(script_url, max_bytes=int(resolver.get("max_bytes") or 1024 * 1024), session=session)
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
    if resolver_type == "uhc_blob_listing":
        host = _domain(url) or ""
        configured_paths = resolver.get("optum_path_templates") if "optum.com" in host else resolver.get("path_templates")
        paths = [str(item) for item in (configured_paths or ()) if str(item).strip()]
        targets: list[CrawlTarget] = []
        for path in paths:
            listing_url = urljoin(url, path)
            listing = await _fetch_json(listing_url, max_bytes=int(resolver.get("max_bytes") or 64 * 1024 * 1024), session=session)
            for target in _parse_uhc_blob_listing(listing):
                targets.append(
                    CrawlTarget(
                        source=source,
                        url=str(target["url"]),
                        label=str(target.get("label") or source.get("display_name") or ""),
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
        html_text = await _fetch_text(url, max_bytes=int(resolver.get("max_bytes") or 5 * 1024 * 1024), session=session)
        targets = _parse_sapphire_toc_links(html_text, base_url=url)
        if not targets:
            raise ValueError(f"no Sapphire TOC links found for {url}")
        return [
            CrawlTarget(
                source=source,
                url=str(target["url"]),
                label=str(target.get("label") or source.get("display_name") or ""),
                resolved_from_url=url,
                metadata={"resolver": resolver_type},
            )
            for target in targets
        ]
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
                    },
                )
                for target in html_targets
            ]
        return []
    return [CrawlTarget(source=source, url=url, label=str(source.get("display_name") or ""), metadata={"resolver": None})]


def _crawl_skipped_observation(source: dict[str, Any], url: str, reason: str, run_id: str | None) -> dict[str, Any]:
    checked_at = _utc_now()
    return {
        "observation_id": _id("mrfurlobs", {"source_id": source["source_id"], "url": url, "crawl_skipped": reason, "checked_at": checked_at.isoformat()}),
        "source_id": source["source_id"],
        "url": str(url),
        "canonical_url": _canonical_or_none(str(url)),
        "url_type": "toc",
        "status": "crawl_skipped",
        "checked_at": checked_at,
        "error": reason,
        "metadata_json": {"run_id": run_id},
    }


def _crawl_failed_observation(source: dict[str, Any], url: str, exc: Exception, run_id: str | None) -> dict[str, Any]:
    checked_at = _utc_now()
    return {
        "observation_id": _id("mrfurlobs", {"source_id": source["source_id"], "url": url, "crawl_error": str(exc), "checked_at": checked_at.isoformat()}),
        "source_id": source["source_id"],
        "url": str(url),
        "canonical_url": _canonical_or_none(str(url)),
        "url_type": "toc",
        "status": "crawl_failed",
        "checked_at": checked_at,
        "error": str(exc),
        "metadata_json": {"run_id": run_id},
    }


def _crawl_ok_observation(target: CrawlTarget, *, run_id: str | None, plans: int, files: int) -> dict[str, Any]:
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


async def _resolve_crawl_targets(
    source_rows: list[dict[str, Any]],
    *,
    session: aiohttp.ClientSession,
    run_id: str | None,
    progress_run_id: str | None = None,
    concurrency: int,
) -> tuple[list[CrawlTarget], list[dict[str, Any]]]:
    items = [(source, source.get("index_url") or source.get("human_url")) for source in source_rows if source.get("index_url") or source.get("human_url")]
    total = len(items)
    targets: list[CrawlTarget] = []
    observations: list[dict[str, Any]] = []
    semaphore = asyncio.Semaphore(max(1, int(concurrency or DEFAULT_CONCURRENCY)))

    async def resolve_one(source: dict[str, Any], url: Any) -> tuple[list[CrawlTarget], list[dict[str, Any]]]:
        url_text = str(url)
        async with semaphore:
            try:
                resolved_targets = await _crawl_targets_for_source(source, url_text, session)
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
                phase="resolving source TOCs",
                unit="sources",
                done=done,
                total=total,
                message=f"resolved {done}/{total} source TOC targets",
            )
    return targets, observations


def _crawl_target_rank(target: CrawlTarget) -> tuple[int, str]:
    url = str(target.url or "").lower()
    if (target.metadata or {}).get("target_kind") == "file_reference":
        return 20, url
    if target.resolved_from_url:
        return 0, url
    if urlsplit(url).path.endswith(".json"):
        return 1, url
    return 10, url


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
    target_metadata = {key: value for key, value in dict(target.metadata or {}).items() if value not in (None, "")}
    file_type = str(target_metadata.get("target_file_type") or "table-of-contents")
    size_bytes = (
        _parse_size_bytes(target_metadata.get("blob_size"))
        or _parse_size_bytes(target_metadata.get("size_bytes"))
        or _parse_size_bytes(target_metadata.get("content_length"))
    )
    return {
        "mrf_file_id": _id("mrffile", {"source": source["source_id"], "type": file_type, "url": _canonical_or_none(target.url)}),
        "payer_id": source.get("payer_id"),
        "source_id": source["source_id"],
        "file_type": file_type,
        "url": target.url,
        "canonical_url": _canonical_or_none(target.url),
        "from_index_url": target.resolved_from_url,
        "description": target.label,
        "network_name": target.label,
        "plan_ids": [],
        "plan_names": [],
        "market_types": [],
        "is_signed_url": _looks_signed(target.url),
        "size_bytes": size_bytes,
        "etag": target_metadata.get("etag"),
        "last_modified": target_metadata.get("last_modified"),
        "schema_version": None,
        "metadata_json": {
            "container_format": target_metadata.get("container_format") or _container_format(target.url),
            "resolved_from_url": target.resolved_from_url,
            "target_label": target.label,
            **target_metadata,
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


def _dedupe_source_rows_for_crawl(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    no_url: list[dict[str, Any]] = []
    for source in source_rows:
        url = source.get("index_url") or source.get("human_url")
        if not url:
            no_url.append(source)
            continue
        key = _canonical_or_none(str(url)) or str(url)
        previous = by_url.get(key)
        if previous is None or _crawl_source_score(source) > _crawl_source_score(previous):
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
    text = str(value or "").strip()
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
    if payer_query and payer_query.lower() not in _clean_text(candidate.payer_name).lower():
        return False
    return True


def _candidate_is_importable_source(candidate: SourceCandidate) -> bool:
    if not (candidate.index_url or candidate.human_url):
        return False
    return str(candidate.status or "").lower() not in {"unsupported", "archived"}


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
        stmt = stmt.where(or_(*[entity_value.like(f"%{entity_type}%") for entity_type in entity_types]))
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


def _interleave_file_probe_targets_by_host(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _file_probe_observation(target: dict[str, Any], head: dict[str, Any], run_id: str | None) -> dict[str, Any]:
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


def _file_probe_update_values(target: dict[str, Any], head: dict[str, Any]) -> dict[str, Any]:
    if str(head.get("status") or "") != "ok":
        return {}
    values: dict[str, Any] = {}
    if head.get("content_length") is not None:
        values["size_bytes"] = head.get("content_length")
    if head.get("etag") is not None:
        values["etag"] = head.get("etag")
    if head.get("last_modified") is not None:
        values["last_modified"] = head.get("last_modified")
    return {
        "mrf_file_id": target["mrf_file_id"],
        **values,
    } if values else {}


async def _update_mrf_file_probe_metadata(updates: list[dict[str, Any]]) -> None:
    if not updates:
        return
    async with db.session() as session:
        for item in updates:
            file_id = item.get("mrf_file_id")
            values = {key: value for key, value in item.items() if key != "mrf_file_id"}
            if not file_id or not values:
                continue
            await session.execute(update(MRFFile).where(MRFFile.mrf_file_id == file_id).values(**values))


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
    targets = await _load_file_probe_targets(file_types, limit, entity_types=entity_types, payer_query=payer_query)
    if not targets:
        return [], 0
    worker_count = max(1, int(concurrency or DEFAULT_CONCURRENCY))
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    connector = _tcp_connector(limit=worker_count * 2)
    target_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    result_queue: asyncio.Queue[tuple[dict[str, Any], dict[str, Any]] | None] = asyncio.Queue(maxsize=max(worker_count * 4, 1))
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
                await push_objects(observation_batch, MRFUrlObservation, rewrite=True, use_copy=False)
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
            await push_objects(observation_batch, MRFUrlObservation, rewrite=True, use_copy=False)
        await _update_mrf_file_probe_metadata(update_batch)
        return all_observations, ok_count

    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, connector=connector, trust_env=False) as session:
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
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    if test_mode:
        return [], [], []
    plan_ids: set[str] = set()
    file_ids: set[str] = set()
    observation_ids: set[str] = set()
    worker_count = max(1, int(concurrency or DEFAULT_CONCURRENCY))
    write_batch_size = WRITE_BATCH_SIZE
    timeout = aiohttp.ClientTimeout(total=HTTP_TOTAL_TIMEOUT, connect=15, sock_read=HTTP_READ_TIMEOUT)
    connector = _tcp_connector(limit=worker_count * 2)
    crawl_source_rows = _dedupe_source_rows_for_crawl(source_rows)

    async def crawl_one(target: CrawlTarget, session: aiohttp.ClientSession) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str]:
        try:
            target_kind = (target.metadata or {}).get("target_kind")
            target_max_bytes = _target_fetch_max_bytes(target, max_toc_bytes)
            if target_kind == "file_reference":
                return [], [], [_crawl_ok_observation(target, run_id=run_id, plans=0, files=1)], target.url
            if target_kind == "metadata_text":
                text = await _fetch_text(target.url, max_bytes=target_max_bytes, session=session)
                plan_rows, file_rows = _metadata_text_rows_from_content(target.source, target.url, text)
            else:
                toc = await _fetch_json(target.url, max_bytes=target_max_bytes, session=session)
                plan_rows, file_rows = _toc_rows_from_content(target.source, target.url, toc)
            return plan_rows, file_rows, [_crawl_ok_observation(target, run_id=run_id, plans=len(plan_rows), files=len(file_rows))], target.url
        except Exception as exc:  # pylint: disable=broad-exception-caught
            return [], [], [_crawl_failed_observation(target.source, target.url, exc, run_id)], target.url

    async with aiohttp.ClientSession(headers={"User-Agent": USER_AGENT}, timeout=timeout, connector=connector, trust_env=False) as session:
        targets, resolver_observations = await _resolve_crawl_targets(
            crawl_source_rows,
            session=session,
            run_id=run_id,
            progress_run_id=progress_run_id,
            concurrency=concurrency,
        )
        targets = sorted(targets, key=_crawl_target_rank)
        target_rows: list[dict[str, Any]] = []
        for target in targets:
            row = _toc_target_file_row(target)
            file_ids.add(row["mrf_file_id"])
            target_rows.append(row)
        for observation in resolver_observations:
            observation_ids.add(observation["observation_id"])
        await _push_crawl_row_batches([], target_rows, resolver_observations, batch_size=write_batch_size)
        if crawl_target_limit:
            targets = targets[: max(1, int(crawl_target_limit))]
        total = len(targets)
        target_queue: asyncio.Queue[CrawlTarget | None] = asyncio.Queue()
        result_queue: asyncio.Queue[tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str] | None] = asyncio.Queue(
            maxsize=max(worker_count * 4, 1)
        )
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
                for row in plan_rows:
                    plan_ids.add(row["mrf_plan_id"])
                for row in file_rows:
                    file_ids.add(row["mrf_file_id"])
                for row in crawl_observations:
                    observation_ids.add(row["observation_id"])
                plan_batch.extend(plan_rows)
                file_batch.extend(file_rows)
                observation_batch.extend(crawl_observations)
                if len(plan_batch) >= write_batch_size or len(file_batch) >= write_batch_size or len(observation_batch) >= write_batch_size:
                    if progress_run_id:
                        enqueue_live_progress(
                            run_id=progress_run_id,
                            importer="mrf-source-discovery",
                            status="running",
                            phase="writing TOC metadata rows",
                            unit="sources",
                            done=done,
                            total=total,
                            message=f"writing rows for TOC target {done}/{total}",
                            label=str(url),
                        )
                    await _push_crawl_row_batches(plan_batch, file_batch, observation_batch, batch_size=write_batch_size)
                if progress_run_id:
                    enqueue_live_progress(
                        run_id=progress_run_id,
                        importer="mrf-source-discovery",
                        status="running",
                        phase="crawling TOC metadata",
                        unit="sources",
                        done=done,
                        total=total,
                        message=f"crawled {done}/{total} TOC targets",
                        label=str(url),
                    )
            await _push_crawl_row_batches(plan_batch, file_batch, observation_batch, batch_size=write_batch_size)

        active_workers = min(worker_count, max(total, 1))
        workers = [asyncio.create_task(worker()) for _ in range(active_workers)]
        writer_task = asyncio.create_task(writer(active_workers))
        worker_group = asyncio.gather(*workers)
        try:
            done, _ = await asyncio.wait({worker_group, writer_task}, return_when=asyncio.FIRST_EXCEPTION)
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
        [{"mrf_plan_id": value} for value in plan_ids],
        [{"mrf_file_id": value} for value in file_ids],
        [{"observation_id": value} for value in observation_ids],
    )


def _looks_signed(url: str | None) -> bool:
    raw = str(url or "").lower()
    return any(token in raw for token in ("signature=", "x-amz-signature=", "sig=", "expires=", "x-goog-signature="))


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
            "domain": row.get("domain"),
            "healthcare_source_id": row.get("source_id"),
        },
    }
    if promoted_source_id:
        item["promoted_source_id"] = promoted_source_id
    if item["review_status"] == "promoted":
        item["reviewed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return item


async def _sync_import_control_seeds(source_rows: list[dict[str, Any]], *, limit: int | None = None) -> int:
    base_url = str(os.getenv("HLTHPRT_IMPORT_CONTROL_URL") or os.getenv("HP_IMPORT_CONTROL_BASE_URL") or "").strip()
    token = str(os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN") or os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not base_url or not token:
        return 0
    items = []
    for row in source_rows[: limit or len(source_rows)]:
        item = _import_control_seed_item(row)
        if item:
            items.append(item)
    if not items:
        return 0
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Agent": USER_AGENT}
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, trust_env=False) as session:
        async with session.post(f"{base_url.rstrip('/')}/v1/catalog/seeds/import", json={"seed_provider": "healthcare-mrf-api", "items": items}) as resp:
            if resp.status >= 400:
                text = await resp.text()
                raise RuntimeError(f"import-control seed sync failed: {resp.status} {text[:200]}")
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


def _eligible_for_public_promotion(row: dict[str, Any]) -> bool:
    """Only free, direct MRF sources become public import-control sources. Paid/vendor
    aggregator provenance rows stay in the seed review queue."""
    access_model = str(row.get("access_model") or "").strip().lower()
    source_type = str(row.get("source_type") or "").strip().lower()
    return access_model == "free" and source_type != "vendor_aggregator"


def _chunked(items: list[Any], size: int):
    for start in range(0, len(items), max(1, size)):
        yield items[start : start + size]


def _split_preview_items(items: list[dict[str, Any]], *, max_plan_info: int = 200) -> list[dict[str, Any]]:
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
        market_type = markets[index] if index < len(markets) else (markets[0] if len(markets) == 1 else None)
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


def _plan_lookup_from_rows(rows: list[Any]) -> dict[tuple[str, str, str | None, str | None], dict[str, Any]]:
    lookup: dict[tuple[str, str, str | None, str | None], dict[str, Any]] = {}
    for row in rows:
        source_id = str(row[0] or "")
        plan_id = str(row[1] or "").strip()
        if not source_id or not plan_id:
            continue
        metadata = _coerce_metadata(row[6] if len(row) > 6 else None)
        raw_plan = metadata.get("raw_plan") if isinstance(metadata.get("raw_plan"), dict) else {}
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
        if not (next_plan.get("plan_sponsor_name") or next_plan.get("plan_sponser_name")) and lookup.get("plan_sponsor_name"):
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
    if not name or name.lower() in {"index", "toc", "table of contents"}:
        return None
    return re.sub(r"\s+", " ", name)


def _apply_company_fallback(plan_info: list[dict[str, Any]], company_name: str | None) -> list[dict[str, Any]]:
    if not company_name:
        return plan_info
    next_info: list[dict[str, Any]] = []
    for plan in plan_info:
        next_plan = dict(plan)
        if not (next_plan.get("plan_sponsor_name") or next_plan.get("plan_sponser_name") or next_plan.get("sponsor_name")):
            next_plan["plan_sponsor_name"] = company_name
        next_info.append(next_plan)
    return next_info


async def _import_control_snapshot_items(source_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
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
        company_name = _company_name_from_index_url(row[11])
        plan_info = _apply_company_fallback(plan_info, company_name)
        if not plan_info:
            continue
        original_url = row[1] or row[2]
        if not original_url:
            continue
        grouped.setdefault(row[0], []).append(
            {
                "original_url": original_url,
                "canonical_url": row[2] or original_url,
                "domain": metadata.get("domain") or row[3],
                "source_type": row[3],
                "network_name": row[9],
                "description": row[10] or metadata.get("description") or company_name,
                "from_index_url": row[11],
                "company_name": company_name,
                "reporting_entity_name": metadata.get("reporting_entity_name")
                or _reporting_entity_from_plan_info(row[0], plan_info, plan_lookup),
                "content_length": row[4],
                "plan_info": plan_info,
            }
        )
    return grouped


async def _promote_import_control_source(
    session: aiohttp.ClientSession, base: str, row: dict[str, Any]
) -> str | None:
    """Upsert a discovered source into import-control as a public catalog source and return
    the import-control-derived source_id (it derives ids from the canonical URL, ignoring ours)."""
    payload = {
        "index_url": row.get("index_url") or row.get("human_url"),
        "official_url": row.get("human_url"),
        "source_key": row.get("source_key"),
        "display_name": row.get("display_name"),
        "payer_name": row.get("display_name"),
        "source_type": row.get("source_type"),
        "domain": row.get("domain"),
        "visibility": "public",
        "status": "active",
        "preserve_operator_state": True,
        "metadata": {
            "hosting_platform": row.get("hosting_platform"),
            "healthcare_source_id": row.get("source_id"),
            "seed_provider": row.get("seed_provider"),
            "access_model": row.get("access_model"),
        },
    }
    async with session.post(f"{base}/v1/catalog/sources", json=payload) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(f"import-control source promote failed: {resp.status} {text[:200]}")
        data = await resp.json()
    return str(data.get("source_id") or "") or None


async def _ingest_import_control_preview(
    session: aiohttp.ClientSession, base: str, ic_source_id: str, items: list[dict[str, Any]]
) -> int:
    preview = {"source_id": ic_source_id, "items": items}
    async with session.post(f"{base}/v1/ptg/discover/ingest-preview", json=preview) as resp:
        if resp.status >= 400:
            text = await resp.text()
            raise RuntimeError(f"import-control plan ingest failed: {resp.status} {text[:200]}")
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
            raise RuntimeError(f"import-control promoted seed sync failed: {resp.status} {text[:200]}")
        await resp.json()
    return True


async def _push_import_control_catalog(
    source_rows: list[dict[str, Any]], *, limit: int | None = None
) -> tuple[int, int]:
    """Promote eligible discovered sources into import-control and push their discovered
    plans (built from the stored MRF file snapshot). Returns (sources_synced, plans_synced).

    Reuses import-control's non-destructive upsert (keyed on the stable discovered_plan_id),
    so manual fetch-preview plans and bulk-discovered plans converge instead of duplicating."""
    base_url = str(os.getenv("HLTHPRT_IMPORT_CONTROL_URL") or os.getenv("HP_IMPORT_CONTROL_BASE_URL") or "").strip()
    token = str(os.getenv("HLTHPRT_IMPORT_CONTROL_TOKEN") or os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not base_url or not token:
        return (0, 0)
    eligible = [
        row
        for row in source_rows
        if _eligible_for_public_promotion(row) and (row.get("index_url") or row.get("human_url"))
    ]
    eligible = eligible[: limit or len(eligible)]
    if not eligible:
        return (0, 0)
    snapshot = await _import_control_snapshot_items([str(row.get("source_id") or "") for row in eligible])
    if not snapshot:
        return (0, 0)
    base = base_url.rstrip("/")
    timeout = aiohttp.ClientTimeout(total=60, connect=10, sock_read=30)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "User-Agent": USER_AGENT}
    sources_synced = 0
    plans_synced = 0
    failures = 0
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, trust_env=False) as session:
        for row in eligible:
            items = snapshot.get(str(row.get("source_id") or "")) or []
            if not items:
                continue
            try:
                ic_source_id = await _promote_import_control_source(session, base, row)
                if not ic_source_id:
                    continue
                sources_synced += 1
                for batch in _chunked(_split_preview_items(items), 100):
                    plans_synced += await _ingest_import_control_preview(session, base, ic_source_id, batch)
                await _mark_import_control_seed_promoted(session, base, row, ic_source_id)
            except Exception:  # pylint: disable=broad-exception-caught
                failures += 1
                continue
    if failures and not sources_synced:
        raise RuntimeError(f"import-control catalog sync failed for all {failures} source(s)")
    return (sources_synced, plans_synced)


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
    crawl_run_id = _id("mrfcrawl", {"started_at": started_at.isoformat(), "providers": providers, "run_id": run_id})
    run_mode = _discovery_run_mode(crawl=crawl, check_urls=check_urls, probe_files=probe_files)
    result.crawl_run_id = crawl_run_id
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="mrf-source-discovery",
            status="running",
            phase="loading source providers",
            unit="providers",
            done=0,
            total=len(providers),
            message="loading source providers",
        )

    needs_source_load = bool(dry_run or check_urls or crawl or sync_import_control or not probe_files)
    candidates: list[SourceCandidate] = []
    if needs_source_load:
        for index, provider_name in enumerate(providers):
            try:
                candidates.extend(await _load_candidates(provider_name, test_mode=test_mode, limit=bounded_limit))
            except Exception as exc:  # pylint: disable=broad-exception-caught
                result.errors.append({"provider": provider_name, "message": str(exc)})
            if run_id:
                enqueue_live_progress(
                    run_id=run_id,
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
        candidates = [
            candidate
            for candidate in candidates
            if _candidate_matches_text_filters(
                candidate,
                entity_types=parsed_source_entity_types,
                payer_query=parsed_source_payer_query,
            )
        ]
    candidates = [candidate for candidate in candidates if _candidate_is_importable_source(candidate)]
    if bounded_limit:
        candidates = candidates[:bounded_limit]
    result.candidates = len(candidates)

    if dry_run:
        result.payers = len({_clean_text(candidate.payer_name).lower() for candidate in candidates})
        result.sources = len([candidate for candidate in candidates if candidate.index_url or candidate.human_url])
        return result.as_dict()

    await init_db(db, asyncio.get_event_loop())
    await ensure_database(test_mode)
    await _ensure_catalog_tables()
    await push_objects(
        [
            {
                "crawl_run_id": crawl_run_id,
                "run_id": run_id,
                "provider": ",".join(providers),
                "mode": run_mode,
                "status": "running",
                "started_at": started_at,
                "params": {
                    "test_mode": test_mode,
                    "provider": provider,
                    "limit": limit,
                    "source_entity_types": list(parsed_source_entity_types),
                    "source_payer_query": parsed_source_payer_query,
                    "check_urls": check_urls,
                    "crawl": crawl,
                    "probe_files": probe_files,
                    "file_probe_limit": file_probe_limit,
                    "file_probe_types": list(parsed_file_probe_types),
                    "file_probe_entity_types": list(parsed_file_probe_entity_types),
                    "file_probe_payer_query": parsed_file_probe_payer_query,
                    "sync_import_control": sync_import_control,
                    "max_toc_bytes": max_toc_bytes,
                    "concurrency": concurrency,
                    "crawl_target_limit": crawl_target_limit,
                },
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
    observation_run_id = run_id or crawl_run_id
    progress_run_id = run_id
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
        plan_rows, file_rows, crawl_observations = await _crawl_toc_metadata(
            source_rows,
            test_mode=test_mode,
            run_id=observation_run_id,
            progress_run_id=progress_run_id,
            max_toc_bytes=max_toc_bytes,
            concurrency=concurrency,
            crawl_target_limit=crawl_target_limit,
        )
        observations.extend(crawl_observations)
        result.plans = len(plan_rows)
        result.files = len(file_rows)
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
            result.import_control_synced = await _sync_import_control_seeds(source_rows, limit=bounded_limit)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result.errors.append({"provider": "import-control", "message": str(exc)})
        try:
            sources_synced, plans_synced = await _push_import_control_catalog(source_rows, limit=bounded_limit)
            result.import_control_sources_synced = sources_synced
            result.import_control_plans_synced = plans_synced
        except Exception as exc:  # pylint: disable=broad-exception-caught
            result.errors.append({"provider": "import-control-catalog", "message": str(exc)})

    await push_objects(
        [
            {
                "crawl_run_id": crawl_run_id,
                "run_id": run_id,
                "provider": ",".join(providers),
                "mode": run_mode,
                "status": "succeeded" if not result.errors else "succeeded_with_errors",
                "started_at": started_at,
                "finished_at": _utc_now(),
                "params": {
                    "test_mode": test_mode,
                    "provider": provider,
                    "limit": limit,
                    "source_entity_types": list(parsed_source_entity_types),
                    "source_payer_query": parsed_source_payer_query,
                    "check_urls": check_urls,
                    "crawl": crawl,
                    "probe_files": probe_files,
                    "file_probe_limit": file_probe_limit,
                    "file_probe_types": list(parsed_file_probe_types),
                    "file_probe_entity_types": list(parsed_file_probe_entity_types),
                    "file_probe_payer_query": parsed_file_probe_payer_query,
                    "sync_import_control": sync_import_control,
                    "max_toc_bytes": max_toc_bytes,
                    "concurrency": concurrency,
                    "crawl_target_limit": crawl_target_limit,
                },
                "sources_discovered": result.sources,
                "urls_checked": result.urls_checked,
                "etag_skipped": 0,
                "plans_discovered": result.plans,
                "files_discovered": result.files,
                "bytes_streamed": sum(int(item.get("content_length") or 0) for item in observations),
                "errors": result.errors,
            }
        ],
        MRFCrawlRun,
        rewrite=True,
        use_copy=False,
    )
    if run_id:
        enqueue_live_progress(
            run_id=run_id,
            importer="mrf-source-discovery",
            status="succeeded",
            phase="mrf source discovery complete",
            unit="sources",
            done=result.sources,
            total=result.sources,
            pct=100,
            message="mrf source discovery complete",
        )
    return result.as_dict()


async def process_data(ctx: dict[str, Any] | None = None, task: dict[str, Any] | None = None) -> dict[str, Any]:
    task = task or {}
    run_id = str(task.get("run_id") or (ctx or {}).get("control_run_id") or (ctx or {}).get("context", {}).get("control_run_id") or "").strip() or None
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
        sync_import_control=bool(task.get("sync_import_control", _env_flag("HLTHPRT_MRF_DISCOVERY_SYNC_IMPORT_CONTROL_DEFAULT"))),
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
