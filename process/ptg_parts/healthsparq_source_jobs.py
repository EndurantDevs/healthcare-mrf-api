# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""HealthSparq metadata adapters for PTG source-job parsing."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlsplit

from process.ptg_parts.canonical import canonicalize_url, normalize_tic_source_url
from process.ptg_parts.domain import (
    PTG2_DOMAIN_ALLOWED_AMOUNT,
    PTG2_DOMAIN_DRUG,
    PTG2_DOMAIN_IN_NETWORK,
    PTG2SourceCatalogEntry,
)


def healthsparq_catalog_entries(
    toc_content: dict[str, Any],
    toc_url: str,
    toc_meta: dict[str, Any],
    *,
    plan_ids: list[str] | None,
    plan_name_contains: list[str] | None,
    plan_market_types: list[str] | None,
) -> list[PTG2SourceCatalogEntry]:
    """Convert HealthSparq files metadata into the same entries as a TiC TOC."""
    entries: list[PTG2SourceCatalogEntry] = []
    for file_item in _metadata_files(toc_content):
        entry = _catalog_entry_from_file(
            file_item,
            toc_url,
            toc_meta,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _catalog_entry_from_file(
    file_item: dict[str, Any],
    toc_url: str,
    toc_meta: dict[str, Any],
    *,
    plan_ids: list[str] | None,
    plan_name_contains: list[str] | None,
    plan_market_types: list[str] | None,
) -> PTG2SourceCatalogEntry | None:
    source_type_and_domain = _source_type_and_domain(
        _first_nonempty(file_item, "fileSchema", "file_schema", "schema", "fileType", "file_type")
    )
    if source_type_and_domain is None:
        return None
    source_type, domain = source_type_and_domain
    file_url = normalize_tic_source_url(_file_url(toc_url, file_item))
    if not _is_file_location_like_download(file_url):
        return None
    plans = _filter_plans(
        [_plan_payload(plan) for plan in _reporting_plans(file_item)],
        plan_ids=plan_ids,
        plan_name_contains=plan_name_contains,
        plan_market_types=plan_market_types,
    )
    if not plans:
        return None
    return PTG2SourceCatalogEntry(
        source_type=source_type,
        domain=domain,
        original_url=file_url,
        canonical_url=canonicalize_url(file_url),
        from_index_url=toc_url,
        description=_first_nonempty(file_item, "fileName", "file_name", "filename", "name"),
        reporting_entity_name=_first_nonempty(
            file_item,
            "reportingEntityName",
            "reporting_entity_name",
            "reportingEntity",
        )
        or toc_meta["reporting_entity_name"],
        reporting_entity_type=_first_nonempty(
            file_item,
            "reportingEntityType",
            "reporting_entity_type",
        )
        or toc_meta["reporting_entity_type"],
        plan_info=tuple(plans),
    )


def _metadata_files(payload: dict[str, Any]) -> list[dict[str, Any]]:
    containers: list[Any] = [payload]
    data = payload.get("data")
    if isinstance(data, dict):
        containers.append(data)
    elif isinstance(data, list):
        containers.append({"items": data})
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("files", "items", "results"):
            files = container.get(key)
            if isinstance(files, list):
                return [item for item in files if isinstance(item, dict)]
    return []


def _file_url(toc_url: str, file_item: dict[str, Any]) -> str:
    path = str(
        _first_nonempty(
            file_item,
            "filePath",
            "file_path",
            "path",
            "location",
            "url",
            "downloadUrl",
            "download_url",
            "href",
        )
        or ""
    ).strip()
    if not path:
        return ""
    if path.startswith(("http://", "https://")):
        return path
    return urljoin(toc_url.rsplit("/", 1)[0] + "/", path)


def _first_nonempty(source: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in (None, ""):
            return value
    return None


def _source_type_and_domain(file_schema: Any) -> tuple[str, str] | None:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(file_schema or "").strip().lower()).strip("_")
    if normalized in {"in_network_rates", "in_network"}:
        return "in-network", PTG2_DOMAIN_IN_NETWORK
    if normalized in {"allowed_amounts", "allowed_amount"}:
        return "allowed-amounts", PTG2_DOMAIN_ALLOWED_AMOUNT
    if normalized in {"table_of_contents", "toc"}:
        return "table-of-contents", "catalog"
    if "drug" in normalized:
        return "payer-drug", PTG2_DOMAIN_DRUG
    return None


def _is_file_location_like_download(value: Any) -> bool:
    location = str(value or "").strip()
    if not location:
        return False
    parsed = urlsplit(location)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return False
    path = parsed.path.lower()
    query = parsed.query.lower()
    if any(path.endswith(suffix) for suffix in (".json", ".json.gz", ".gz", ".zip", ".csv", ".txt")):
        return True
    return any(suffix in query for suffix in (".json", ".json.gz", ".gz", ".zip"))


def _reporting_plans(file_item: dict[str, Any]) -> list[dict[str, Any]]:
    plans = _first_nonempty(file_item, "reportingPlans", "reporting_plans", "plans", "planInfo", "plan_info")
    return [plan for plan in (plans or []) if isinstance(plan, dict)]


def _plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "plan_name": _first_nonempty(plan, "planName", "plan_name", "name"),
        "plan_id_type": _first_nonempty(plan, "planIdType", "plan_id_type"),
        "plan_id": _first_nonempty(plan, "planId", "plan_id"),
        "plan_market_type": _first_nonempty(plan, "planMarketType", "plan_market_type", "marketType"),
        "issuer_name": _first_nonempty(plan, "issuerName", "issuer_name", "issuer"),
        "plan_sponsor_name": _first_nonempty(
            plan,
            "planSponsorName",
            "plan_sponsor_name",
            "planSponserName",
            "plan_sponser_name",
            "sponsor_name",
        ),
    }


def _filter_plans(
    plans: list[dict[str, Any]],
    *,
    plan_ids: list[str] | None,
    plan_name_contains: list[str] | None,
    plan_market_types: list[str] | None,
) -> list[dict[str, Any]]:
    if not any((plan_ids, plan_name_contains, plan_market_types)):
        return plans
    return [
        plan
        for plan in plans
        if _is_plan_matching_filters(
            plan,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
    ]


def _is_plan_matching_filters(
    plan: dict[str, Any],
    *,
    plan_ids: list[str] | None,
    plan_name_contains: list[str] | None,
    plan_market_types: list[str] | None,
) -> bool:
    normalized_ids = _normalize_filter_values(plan_ids)
    normalized_name_terms = _normalize_filter_values(plan_name_contains)
    normalized_market_types = _normalize_filter_values(plan_market_types)
    if normalized_ids and str(plan.get("plan_id") or "").strip().lower() not in normalized_ids:
        return False
    if normalized_market_types and str(plan.get("plan_market_type") or "").strip().lower() not in normalized_market_types:
        return False
    searchable = f"{plan.get('plan_name') or ''} {plan.get('plan_sponsor_name') or ''}".lower()
    return not normalized_name_terms or any(term in searchable for term in normalized_name_terms)


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]
