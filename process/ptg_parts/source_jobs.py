# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source TOC filtering and PTG job dedupe helpers."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from process.ptg_parts.canonical import (
    _canonicalize_for_json,
    canonical_json_dumps,
    canonicalize_url,
    normalize_tic_source_url,
)
from process.ptg_parts.domain import (
    PTG2_DOMAIN_IN_NETWORK,
    PTG2SourceCatalogEntry,
)
from process.ptg_parts.healthsparq_source_jobs import healthsparq_catalog_entries
from process.ptg_parts.row_helpers import _as_list
from process.ptg_parts.toc_entries import (
    _is_toc_body_file_location,
    _toc_body_source_type,
    flat_toc_catalog_entries,
)


def _normalize_filter_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]


def _dedupe_preserve(seq: list[str]) -> list[str]:
    seen_values_set = set()
    deduped_values = []
    for item in seq:
        if item in seen_values_set:
            continue
        seen_values_set.add(item)
        deduped_values.append(item)
    return deduped_values


def _dedupe_rows_by(rows: list[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    if len(rows) < 2:
        return rows
    rows_by_value: dict[Any, dict[str, Any]] = {}
    rows_without_keys: list[dict[str, Any]] = []
    for row in rows:
        value = row.get(key)
        if value is None:
            rows_without_keys.append(row)
        else:
            rows_by_value[value] = row
    return list(rows_by_value.values()) + rows_without_keys


def _is_plan_matching_filters(
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


_plan_matches_filters = _is_plan_matching_filters


def _filter_reporting_plans(
    plans: list[dict[str, Any]],
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    plan_predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[dict[str, Any]]:
    if not any((plan_ids, plan_name_contains, plan_market_types, plan_predicate)):
        return plans
    return [
        plan
        for plan in plans
        if _is_plan_matching_filters(
            plan,
            plan_ids,
            plan_name_contains,
            plan_market_types,
        )
        and (plan_predicate is None or plan_predicate(plan))
    ]


def _normalize_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    normalized_plan_by_field = dict(plan or {})
    if (
        "plan_sponsor_name" not in normalized_plan_by_field
        and normalized_plan_by_field.get("plan_sponser_name")
    ):
        normalized_plan_by_field["plan_sponsor_name"] = normalized_plan_by_field.get(
            "plan_sponser_name"
        )
    return normalized_plan_by_field


def _is_provider_directory_index_payload(payload: dict[str, Any]) -> bool:
    if payload.get("reporting_structure"):
        return False
    return any(key in payload for key in ("provider_urls", "plan_urls"))


def _merge_duplicate_catalog_entries(entries: list[PTG2SourceCatalogEntry]) -> list[PTG2SourceCatalogEntry]:
    """Merge plan metadata for duplicate file locations in linear work."""
    entries_by_key: dict[tuple[str, str, str], PTG2SourceCatalogEntry] = {}
    plans_by_key: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    plan_identities_by_key: dict[tuple[str, str, str], set[str]] = {}
    for entry in entries:
        entry_key = (entry.source_type, entry.domain, entry.canonical_url)
        entries_by_key[entry_key] = entry
        merged_plans = plans_by_key.setdefault(entry_key, [])
        seen_plan_identities = plan_identities_by_key.setdefault(entry_key, set())
        for plan in entry.plan_info:
            plan_identity = canonical_json_dumps(plan)
            if plan_identity in seen_plan_identities:
                continue
            seen_plan_identities.add(plan_identity)
            merged_plans.append(plan)
    return [replace(entry, plan_info=tuple(plans_by_key[entry_key])) for entry_key, entry in entries_by_key.items()]


def parse_toc_catalog_entries(
    toc_content: dict[str, Any],
    toc_url: str,
    plan_ids: list[str] | None = None,
    plan_name_contains: list[str] | None = None,
    plan_market_types: list[str] | None = None,
    plan_predicate: Callable[[dict[str, Any]], bool] | None = None,
) -> list[PTG2SourceCatalogEntry]:
    """Return filtered PTG source catalog entries parsed from a TOC payload."""
    if _is_provider_directory_index_payload(toc_content):
        return []
    toc_metadata_by_field = {
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
            reporting_entity_name=toc_metadata_by_field["reporting_entity_name"],
            reporting_entity_type=toc_metadata_by_field["reporting_entity_type"],
            plan_info=(),
        )
    ]
    entries.extend(
        healthsparq_catalog_entries(
            toc_content,
            toc_url,
            toc_metadata_by_field,
            plan_ids=plan_ids,
            plan_name_contains=plan_name_contains,
            plan_market_types=plan_market_types,
        )
    )
    entries.extend(
        flat_toc_catalog_entries(
            toc_content,
            toc_url,
            toc_metadata_by_field,
        )
    )
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
            plan_predicate=plan_predicate,
        )
        if not plans:
            continue
        reporting_plans = tuple(plans)
        for file_entry in _as_list(structure.get("in_network_files")):
            if not isinstance(file_entry, dict):
                continue
            location = file_entry.get("location")
            if _is_toc_body_file_location(location):
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
                        reporting_entity_name=toc_metadata_by_field[
                            "reporting_entity_name"
                        ],
                        reporting_entity_type=toc_metadata_by_field[
                            "reporting_entity_type"
                        ],
                        plan_info=reporting_plans,
                    )
                )
        allowed_amount_files = _as_list(structure.get("allowed_amount_file")) + _as_list(
            structure.get("allowed_amount_files")
        )
        for allowed_amount_file in allowed_amount_files:
            if not isinstance(allowed_amount_file, dict):
                continue
            if not _is_toc_body_file_location(
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
                    reporting_entity_name=toc_metadata_by_field[
                        "reporting_entity_name"
                    ],
                    reporting_entity_type=toc_metadata_by_field[
                        "reporting_entity_type"
                    ],
                    plan_info=reporting_plans,
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
                if _is_toc_body_file_location(location):
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
                            reporting_entity_name=toc_metadata_by_field[
                                "reporting_entity_name"
                            ],
                            reporting_entity_type=toc_metadata_by_field[
                                "reporting_entity_type"
                            ],
                            plan_info=reporting_plans,
                        )
                    )
    return _merge_duplicate_catalog_entries(entries)


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
            parsed_content = json.loads(text_strip)
            if isinstance(parsed_content, list):
                for entry in parsed_content:
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
            elif isinstance(parsed_content, dict):
                for entry in parsed_content.values():
                    if isinstance(entry, str) and entry.strip():
                        urls.append(entry.strip())
                    elif isinstance(entry, list):
                        urls.extend(
                            [
                                str(toc_url_value).strip()
                                for toc_url_value in entry
                                if str(toc_url_value).strip()
                            ]
                        )
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
    filtered_jobs: list[dict[str, Any]] = []
    for job in jobs:
        haystack = " ".join(
            str(value or "")
            for value in (
                job.get("url"),
                job.get("description"),
            )
        ).lower()
        if any(needle in haystack for needle in needles):
            filtered_jobs.append(job)
    return filtered_jobs


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
    jobs_by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_count = 0
    for job in jobs:
        identity = _ptg_job_identity(job)
        normalized_job_by_field = dict(job)
        normalized_job_by_field["url"] = normalize_tic_source_url(
            str(job.get("url") or "")
        )
        if identity in jobs_by_identity:
            duplicate_count += 1
            _merge_ptg_job(
                jobs_by_identity[identity],
                normalized_job_by_field,
            )
            continue
        jobs_by_identity[identity] = normalized_job_by_field
    return list(jobs_by_identity.values()), duplicate_count
