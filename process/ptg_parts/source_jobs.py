# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source TOC filtering and PTG job dedupe helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from process.ptg_parts.canonical import (
    _canonicalize_for_json,
    canonical_json_dumps,
    canonicalize_url,
    normalize_tic_source_url,
)
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
    PTG2SourceCatalogEntry,
)
from process.ptg_parts.row_helpers import _as_list


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]


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

    if (
        normalized_ids
        and str(plan.get("plan_id") or "").strip().lower() not in normalized_ids
    ):
        return False

    if (
        normalized_market_types
        and str(plan.get("plan_market_type") or "").strip().lower()
        not in normalized_market_types
    ):
        return False

    if normalized_name_terms:
        searchable = " ".join(
            str(plan.get(key) or "")
            for key in (
                "plan_name",
                "plan_sponsor_name",
                "plan_sponser_name",
            )
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


def _normalize_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(plan or {})
    if "plan_sponsor_name" not in normalized and normalized.get("plan_sponser_name"):
        normalized["plan_sponsor_name"] = normalized.get("plan_sponser_name")
    return normalized


_TOC_BODY_FILE_PLACEHOLDERS = {
    "missing file",
    "n/a",
    "na",
    "none",
    "not available",
    "null",
}
_TOC_BODY_FILE_SUFFIXES = (
    ".json",
    ".json.gz",
    ".gz",
    ".zip",
    ".csv",
    ".txt",
)
_TOC_BODY_FILE_TOKENS = (
    "allowed",
    "download",
    "filetype",
    "innetwork",
    "machine-readable",
    "machine readable",
    "mrf",
    "ndc",
    "outnetwork",
    "outofnetwork",
    "rate",
    "rates",
    "rx",
    "transparency",
)
_TOC_BODY_ALLOWED_TOKENS = (
    "allowed-amount",
    "allowed amount",
    "allowedamount",
    "out-of-network",
    "out of network",
    "outnetwork",
    "outofnetwork",
    "oon",
)
_TOC_BODY_IN_NETWORK_TOKENS = (
    "in-network",
    "in network",
    "innetwork",
)
_TOC_BODY_DRUG_TOKENS = (
    "drug",
    "ndc",
    "pharmacy",
    "rx",
)


def _looks_like_toc_body_file_location(value: Any) -> bool:
    location = str(value or "").strip()
    if not location:
        return False
    if location.lower() in _TOC_BODY_FILE_PLACEHOLDERS:
        return False

    parsed = urlsplit(location)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    if parsed.path in {"", "/"} and not parsed.query:
        return False

    path = parsed.path.lower()
    query = parsed.query.lower()
    if any(path.endswith(suffix) for suffix in _TOC_BODY_FILE_SUFFIXES):
        return True
    if any(suffix in query for suffix in _TOC_BODY_FILE_SUFFIXES):
        return True

    searchable = f"{path} {query}".replace("_", "-").replace("%20", " ")
    compact = "".join(ch for ch in searchable if ch.isalnum())
    return any(
        token in searchable or token in compact for token in _TOC_BODY_FILE_TOKENS
    )


def _toc_body_search_text(location: Any, description: Any = None) -> tuple[str, str]:
    parsed = urlsplit(str(location or ""))
    searchable = (
        f"{parsed.path} {parsed.query} {description or ''}".lower()
        .replace("_", "-")
        .replace("%20", " ")
    )
    compact = "".join(ch for ch in searchable if ch.isalnum())
    return searchable, compact


def _toc_body_source_type(
    default_source_type: str, location: Any, description: Any = None
) -> tuple[str, str]:
    searchable, compact = _toc_body_search_text(location, description)
    if any(token in searchable or token in compact for token in _TOC_BODY_DRUG_TOKENS):
        return "payer-drug", PTG2_DOMAIN_DRUG
    if any(
        token in searchable or token in compact for token in _TOC_BODY_ALLOWED_TOKENS
    ):
        return "allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT
    if any(
        token in searchable or token in compact for token in _TOC_BODY_IN_NETWORK_TOKENS
    ):
        return "in-network", PTG2_DOMAIN_IN_NETWORK
    if default_source_type == "allowed-amounts":
        return default_source_type, PTG2_DOMAIN_ALLOWED_AMOUNT
    if default_source_type == "payer-drug":
        return default_source_type, PTG2_DOMAIN_DRUG
    return default_source_type, PTG2_DOMAIN_IN_NETWORK


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
        plans = [
            _normalize_plan_payload(plan)
            for plan in (structure.get("reporting_plans") or [])
        ]
        plans = _filter_reporting_plans(
            plans,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if not plans:
            continue
        plan_tuple = tuple(plans)
        for file_entry in _as_list(structure.get("in_network_files")):
            if not isinstance(file_entry, dict):
                continue
            location = file_entry.get("location")
            if _looks_like_toc_body_file_location(location):
                source_type, domain = _toc_body_source_type(
                    "in-network", location, file_entry.get("description")
                )
                location = normalize_tic_source_url(location)
                entries.append(
                    PTG2SourceCatalogEntry(
                        source_type=source_type,
                        domain=domain,
                        original_url=location,
                        canonical_url=canonicalize_url(location),
                        from_index_url=toc_url,
                        description=file_entry.get("description"),
                        reporting_entity_name=toc_meta["reporting_entity_name"],
                        reporting_entity_type=toc_meta["reporting_entity_type"],
                        plan_info=plan_tuple,
                    )
                )
        allowed_amount_files = _as_list(structure.get("allowed_amount_file")) + _as_list(
            structure.get("allowed_amount_files")
        )
        for allowed_amount_file in allowed_amount_files:
            if not isinstance(allowed_amount_file, dict):
                continue
            if not _looks_like_toc_body_file_location(
                allowed_amount_file.get("location")
            ):
                continue
            source_type, domain = _toc_body_source_type(
                "allowed-amounts",
                allowed_amount_file["location"],
                allowed_amount_file.get("description"),
            )
            location = normalize_tic_source_url(allowed_amount_file["location"])
            entries.append(
                PTG2SourceCatalogEntry(
                    source_type=source_type,
                    domain=domain,
                    original_url=location,
                    canonical_url=canonicalize_url(location),
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
                if _looks_like_toc_body_file_location(location):
                    source_type, domain = _toc_body_source_type(
                        "payer-drug", location, drug_entry.get("description")
                    )
                    location = normalize_tic_source_url(location)
                    entries.append(
                        PTG2SourceCatalogEntry(
                            source_type=source_type,
                            domain=domain,
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
            return urls
    else:
        for line in text.splitlines():
            line = line.strip()
            if line:
                urls.append(line)
    return _dedupe_preserve(urls)


def _filter_jobs_by_url_contains(
    jobs: list[dict[str, Any]], filters: list[str] | None
) -> list[dict[str, Any]]:
    needles = [
        str(value).strip().lower() for value in filters or [] if str(value).strip()
    ]
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


def _ptg_job_identity(job: dict[str, Any]) -> tuple[str, str]:
    job_type = str(job.get("type") or "").strip()
    url = normalize_tic_source_url(str(job.get("url") or ""))
    return job_type, canonicalize_url(url)


def _plan_identity(plan: dict[str, Any]) -> str:
    return canonical_json_dumps(_canonicalize_for_json(plan))


def _merge_ptg_job(existing: dict[str, Any], incoming: dict[str, Any]) -> None:
    existing_plans = list(existing.get("plan_info") or [])
    seen_plans = {
        _plan_identity(plan) for plan in existing_plans if isinstance(plan, dict)
    }
    for plan in incoming.get("plan_info") or []:
        if not isinstance(plan, dict):
            continue
        plan_key = _plan_identity(plan)
        if plan_key in seen_plans:
            continue
        seen_plans.add(plan_key)
        existing_plans.append(plan)
    if existing_plans:
        existing["plan_info"] = existing_plans
    if not existing.get("description") and incoming.get("description"):
        existing["description"] = incoming.get("description")
    if not existing.get("from_index_url") and incoming.get("from_index_url"):
        existing["from_index_url"] = incoming.get("from_index_url")
    if not existing.get("meta") and incoming.get("meta"):
        existing["meta"] = incoming.get("meta")


def _dedupe_ptg_jobs(jobs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_count = 0
    for job in jobs:
        identity = _ptg_job_identity(job)
        normalized_job = dict(job)
        normalized_job["url"] = normalize_tic_source_url(str(job.get("url") or ""))
        if identity in deduped:
            duplicate_count += 1
            _merge_ptg_job(deduped[identity], normalized_job)
            continue
        deduped[identity] = normalized_job
    return list(deduped.values()), duplicate_count
