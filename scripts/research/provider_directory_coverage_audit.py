#!/usr/bin/env python
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Audit Provider Directory FHIR import coverage.

The report answers the operational questions that matter after each recurring
Provider Directory import:

- how many payer sources are known, probeable, credential-gated, or failing;
- which resource tables actually received provider/location/network data;
- how much data is usable for address search and PTG corroboration; and
- which network references remain unresolved to FHIR network Organizations.

Example:

  ./venv314/bin/python scripts/research/provider_directory_coverage_audit.py \
    --host 127.0.0.1 --port 5440 --database healthporta --schema mrf --format markdown
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import os
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import asyncpg


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PROVIDER_DIRECTORY_RESOURCE_TABLES = (
    "provider_directory_insurance_plan",
    "provider_directory_practitioner",
    "provider_directory_organization",
    "provider_directory_location",
    "provider_directory_practitioner_role",
    "provider_directory_healthcare_service",
    "provider_directory_organization_affiliation",
    "provider_directory_endpoint",
)
PROVIDER_DIRECTORY_RESOURCE_TABLE_BY_TYPE = {
    "InsurancePlan": "provider_directory_insurance_plan",
    "Practitioner": "provider_directory_practitioner",
    "Organization": "provider_directory_organization",
    "Location": "provider_directory_location",
    "PractitionerRole": "provider_directory_practitioner_role",
    "HealthcareService": "provider_directory_healthcare_service",
    "OrganizationAffiliation": "provider_directory_organization_affiliation",
    "Endpoint": "provider_directory_endpoint",
}
FHIR_ONBOARDING_GATEWAY_HOSTS = frozenset(
    {
        "apps.availity.com",
        "partners.centene.com",
    }
)
FHIR_CREDENTIAL_AUTH_MARKERS = ("oauth", "api key", "bearer", "token", "client credential")
PROVIDER_DIRECTORY_CREDENTIALS_JSON_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON"
PROVIDER_DIRECTORY_CREDENTIALS_FILE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_FILE"
PROVIDER_DIRECTORY_MARKET_COUNT_FIELDS = (
    "regulated_market_source_count",
    "medicare_advantage_source_count",
    "medicaid_mco_source_count",
    "chip_source_count",
    "qhp_source_count",
)
CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT = 50
PROVIDER_DIRECTORY_PORTAL_URL_TOKENS = (
    "developer",
    "legal",
    "portal",
    "interoperability",
    "apis",
    "docs",
    "documentation",
)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value not in (None, "") else default


def _validate_identifier(value: str, *, label: str) -> str:
    cleaned = str(value or "").strip()
    if not IDENTIFIER_RE.fullmatch(cleaned):
        raise ValueError(f"{label} must be a PostgreSQL identifier, got {value!r}")
    return cleaned


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _sql_string_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_string_array(values: list[str] | tuple[str, ...] | frozenset[str]) -> str:
    return "ARRAY[" + ", ".join(_sql_string_literal(value) for value in sorted(values)) + "]::varchar[]"


def _sql_ref_matches_resource(ref_expr: str, resource_type: str, resource_id_expr: str) -> str:
    resource_type_literal = str(resource_type).replace("'", "''")
    return (
        f"({ref_expr} IN ({resource_id_expr}, '{resource_type_literal}/' || {resource_id_expr}) "
        f"OR {ref_expr} LIKE '%/{resource_type_literal}/' || {resource_id_expr})"
    )


def _sql_fhir_reference_resource_id(ref_expr: str, resource_type: str) -> str:
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


def _pct(numerator: int, denominator: int) -> float:
    return round((float(numerator) / float(denominator) * 100.0), 2) if denominator else 0.0


def _int(value: Any) -> int:
    return int(value or 0)


def _clean_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _resource_import_diagnostics(value: Any) -> dict[str, Any]:
    payload = _json_object(value)
    # Older rows stored the JSON payload as a JSON string inside json/jsonb.
    if len(payload) == 1 and next(iter(payload), None) == "":
        return {}
    resources = payload.get("resources")
    return resources if isinstance(resources, dict) else {}


def _resource_diagnostic_error(resources: dict[str, Any], resource_type: str) -> str | None:
    diagnostic = resources.get(resource_type)
    if not isinstance(diagnostic, dict):
        return None
    error = diagnostic.get("error")
    return str(error) if error else None


def _is_resource_auth_error(error: str | None) -> bool:
    return str(error or "").lower() in {"http_401", "http_403"}


def _resource_error_counts(resources: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for diagnostic in resources.values():
        if not isinstance(diagnostic, dict) or not diagnostic.get("error"):
            continue
        error = str(diagnostic["error"])
        counts[error] = counts.get(error, 0) + 1
    return counts


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _markdown_cell(value: Any) -> str:
    return str(value if value is not None else "").replace("\n", " ").replace("|", "\\|")


def _credential_market_counts(item: dict[str, Any]) -> dict[str, int]:
    return {
        field: _int(item.get(field))
        for field in PROVIDER_DIRECTORY_MARKET_COUNT_FIELDS
    }


def _credential_source_sample(item: dict[str, Any], *, payer_label: str, api_base: str | None) -> dict[str, Any]:
    sample = {
        "source_id": item.get("source_id"),
        "org_name": item.get("org_name"),
        "plan_name": item.get("plan_name"),
        "payer": payer_label or item.get("source_id"),
        "api_base": api_base,
        "probe_status": item.get("probe_status"),
        "auth_type": item.get("auth_type"),
        "reason": item.get("reason"),
        "markets": {
            "medicare_advantage": item.get("is_medicare_advantage") is True,
            "medicaid_mco": item.get("is_medicaid_mco") is True,
            "chip": item.get("is_chip") is True,
            "qhp": item.get("is_qhp") is True,
        },
    }
    portal_url = _clean_text(item.get("portal_url"))
    if portal_url and portal_url != api_base:
        sample["portal_url"] = portal_url
    return sample


def _append_unique_source_sample(target: list[dict[str, Any]], sample: dict[str, Any], *, limit: int) -> None:
    if len(target) >= limit:
        return
    key = (sample.get("source_id"), sample.get("api_base"))
    if any((item.get("source_id"), item.get("api_base")) == key for item in target):
        return
    target.append(sample)


def _credential_market_cell(item: dict[str, Any]) -> str:
    counts = _credential_market_counts(item)
    parts = [
        ("MA", counts["medicare_advantage_source_count"]),
        ("Medicaid", counts["medicaid_mco_source_count"]),
        ("CHIP", counts["chip_source_count"]),
        ("QHP", counts["qhp_source_count"]),
    ]
    nonzero = [(label, count) for label, count in parts if count]
    if not nonzero:
        nonzero = parts
    return ", ".join(f"{label}={count}" for label, count in nonzero)


def _looks_like_provider_directory_portal_target(*, source_host: str | None, api_base: str | None) -> bool:
    host = (_clean_text(source_host) or "").lower()
    parsed = urllib.parse.urlsplit(_clean_text(api_base) or "")
    path = parsed.path.lower()
    host_or_path = f"{host} {parsed.netloc.lower()} {path}"
    if "fhir" in path or "fhir" in parsed.netloc.lower():
        return False
    return any(token in host_or_path for token in PROVIDER_DIRECTORY_PORTAL_URL_TOKENS)


def _credential_backlog_endpoint_discovery_needed(item: dict[str, Any], *, api_base: str | None) -> bool:
    target_url = api_base or _clean_text(item.get("portal_url"))
    return _looks_like_provider_directory_portal_target(
        source_host=item.get("source_host"),
        api_base=target_url,
    )


def _provider_directory_projection_gap_reason(item: dict[str, Any]) -> str:
    if _int(item.get("valid_npi_organization_address_rows")):
        return "valid_npi_organization_address_projection_pending"
    if _int(item.get("role_projectable_location_refs")) or _int(item.get("affiliation_projectable_location_refs")):
        return "linked_location_projection_pending"
    if (
        _int(item.get("role_healthcare_service_projectable_location_refs"))
        or _int(item.get("affiliation_healthcare_service_projectable_location_refs"))
    ):
        return "linked_healthcare_service_location_projection_pending"
    direct_location_refs = _int(item.get("role_location_refs")) + _int(item.get("affiliation_location_refs"))
    service_location_refs = _int(item.get("role_healthcare_service_location_refs")) + _int(
        item.get("affiliation_healthcare_service_location_refs")
    )
    if not direct_location_refs and not service_location_refs:
        return "no_role_or_affiliation_location_refs"
    direct_matching_refs = _int(item.get("role_matching_location_refs")) + _int(
        item.get("affiliation_matching_location_refs")
    )
    service_matching_refs = _int(item.get("role_healthcare_service_matching_location_refs")) + _int(
        item.get("affiliation_healthcare_service_matching_location_refs")
    )
    if not direct_matching_refs and not service_matching_refs:
        if service_location_refs and not direct_location_refs:
            return "healthcare_service_location_refs_do_not_match_imported_locations"
        return "location_refs_do_not_match_imported_locations"
    valid_npi_refs = (
        _int(item.get("role_valid_npi_refs"))
        + _int(item.get("affiliation_valid_npi_refs"))
        + _int(item.get("role_healthcare_service_valid_npi_refs"))
        + _int(item.get("affiliation_healthcare_service_valid_npi_refs"))
    )
    if not valid_npi_refs:
        return "linked_providers_lack_valid_npi"
    return "linked_rows_filtered_inactive_or_unkeyable"


def _network_name_key_sql(expr: str) -> str:
    return f"regexp_replace(lower(coalesce({expr}, '')), '[^a-z0-9]+', '', 'g')"


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _canonical_base(api_base: Any) -> str | None:
    text = _clean_text(api_base)
    if not text or text.upper() == "N/A":
        return None
    parsed = urllib.parse.urlsplit(text)
    if not parsed.scheme or not parsed.netloc:
        return text.rstrip("/")
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), "", "")
    )


def _host_from_base(api_base: Any) -> str:
    base = _canonical_base(api_base)
    host = urllib.parse.urlsplit(base or "").netloc.lower()
    return host or "(missing host)"


def _name_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", (_clean_text(value) or "").lower())


def _source_row_metadata(row: dict[str, Any]) -> dict[str, Any]:
    return _json_object(row.get("metadata_json"))


def _source_catalog_retest_coverage(
    retest_payload: Any,
    source_rows: list[dict[str, Any]],
    *,
    sample_limit: int,
) -> dict[str, Any]:
    results = retest_payload.get("results") if isinstance(retest_payload, dict) else retest_payload
    if not isinstance(results, list):
        return {"available": False, "reason": "invalid retest results payload", "missing_result_count": 0}

    recoverable_source_statuses = {
        "",
        "auth_required",
        "catalog_in_development",
        "catalog_public",
        "valid",
        "valid_non_fhir",
    }
    current_bases: dict[str, list[dict[str, Any]]] = {}
    redirected_bases: dict[str, list[dict[str, Any]]] = {}
    current_source_org_keys: dict[str, list[dict[str, Any]]] = {}
    blocked_source_org_keys: dict[str, list[dict[str, Any]]] = {}
    for row in source_rows:
        metadata = _source_row_metadata(row)
        source_base = _canonical_base(row.get("canonical_api_base") or row.get("api_base"))
        validation_status = (_clean_text(row.get("last_validated_status")) or "").lower()
        org_key = _name_key(row.get("org_name"))
        if org_key and metadata.get("provider_directory_blocked"):
            blocked_source_org_keys.setdefault(org_key, []).append(row)
        if source_base and validation_status in recoverable_source_statuses:
            if org_key:
                current_source_org_keys.setdefault(org_key, []).append(row)
        for raw_base in (row.get("canonical_api_base"), row.get("api_base"), metadata.get("resolved_api_base")):
            base = _canonical_base(raw_base)
            if base:
                current_bases.setdefault(base, []).append(row)
        previous_base = _canonical_base(
            metadata.get("provider_directory_previous_api_base") or metadata.get("resolved_api_base_from")
        )
        confirmed_base = _canonical_base(metadata.get("provider_directory_confirmed_base") or source_base)
        if previous_base and confirmed_base and previous_base != confirmed_base:
            redirected_bases.setdefault(previous_base, []).append(row)
        for equivalent_base_raw in _list(metadata.get("provider_directory_equivalent_api_bases")):
            equivalent_base = _canonical_base(equivalent_base_raw)
            if equivalent_base and confirmed_base and equivalent_base != confirmed_base:
                redirected_bases.setdefault(equivalent_base, []).append(row)
        for replaced_base_raw in _list(metadata.get("provider_directory_replaces_stale_generic_api_bases")):
            replaced_base = _canonical_base(replaced_base_raw)
            if replaced_base and confirmed_base and replaced_base != confirmed_base:
                redirected_bases.setdefault(replaced_base, []).append(row)

    importable_classifications = {"valid", "valid_non_fhir"}
    credential_gated_classifications = {"auth_required"}
    checked_classifications = importable_classifications | credential_gated_classifications
    classification_counts: dict[str, int] = {}
    coverage_counts = {
        "covered_current_base": 0,
        "covered_by_redirect": 0,
        "missing": 0,
    }
    coverage_category_counts = {
        "importable_checked_result_count": 0,
        "importable_covered_count": 0,
        "importable_missing_result_count": 0,
        "credential_gated_checked_result_count": 0,
        "credential_gated_covered_count": 0,
        "credential_gated_missing_result_count": 0,
    }
    missing_samples: list[dict[str, Any]] = []
    redirected_samples: list[dict[str, Any]] = []
    unchecked_classification_counts: dict[str, int] = {}
    uncovered_unchecked_clusters_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    recovered_unchecked_result_count = 0
    uncovered_unchecked_result_count = 0
    checked_result_count = 0
    unique_bases: set[str] = set()

    for result in results:
        if not isinstance(result, dict):
            continue
        classification = _clean_text(result.get("classification")) or "unknown"
        base = _canonical_base(result.get("api_base"))
        if base:
            unique_bases.add(base)
        if classification not in checked_classifications:
            unchecked_classification_counts[classification] = unchecked_classification_counts.get(classification, 0) + 1
            if base and (base in current_bases or base in redirected_bases):
                recovered_unchecked_result_count += 1
                continue
            org_key = _name_key(result.get("org_name"))
            if org_key in current_source_org_keys:
                recovered_unchecked_result_count += 1
                continue
            if not base and classification == "no_api" and org_key in blocked_source_org_keys:
                recovered_unchecked_result_count += 1
                continue
            uncovered_unchecked_result_count += 1
            host = _host_from_base(base or result.get("api_base"))
            key = (host, classification)
            cluster = uncovered_unchecked_clusters_by_key.setdefault(
                key,
                {
                    "source_host": host,
                    "classification": classification,
                    "result_count": 0,
                    "sample_payers": [],
                    "sample_api_bases": [],
                    "sample_status_codes": [],
                },
            )
            cluster["result_count"] += 1
            payer_label = " / ".join(
                part
                for part in (
                    _clean_text(result.get("org_name")),
                    _clean_text(result.get("plan_name")),
                )
                if part
            )
            if payer_label and len(cluster["sample_payers"]) < sample_limit:
                cluster["sample_payers"].append(payer_label)
            sample_api_base = base or result.get("api_base")
            if sample_api_base and len(cluster["sample_api_bases"]) < sample_limit:
                if sample_api_base not in cluster["sample_api_bases"]:
                    cluster["sample_api_bases"].append(sample_api_base)
            status_code = result.get("status_code")
            if status_code is not None and len(cluster["sample_status_codes"]) < sample_limit:
                if status_code not in cluster["sample_status_codes"]:
                    cluster["sample_status_codes"].append(status_code)
            continue
        checked_result_count += 1
        classification_counts[classification] = classification_counts.get(classification, 0) + 1
        category = "credential_gated" if classification in credential_gated_classifications else "importable"
        coverage_category_counts[f"{category}_checked_result_count"] += 1
        if base and base in current_bases:
            coverage_counts["covered_current_base"] += 1
            coverage_category_counts[f"{category}_covered_count"] += 1
            continue
        if base and base in redirected_bases:
            coverage_counts["covered_by_redirect"] += 1
            coverage_category_counts[f"{category}_covered_count"] += 1
            if len(redirected_samples) < sample_limit:
                redirected_source = redirected_bases[base][0]
                redirected_samples.append(
                    {
                        "classification": classification,
                        "org_name": result.get("org_name"),
                        "api_base": base,
                        "covered_by_source_id": redirected_source.get("source_id"),
                        "covered_by_api_base": _canonical_base(
                            redirected_source.get("canonical_api_base") or redirected_source.get("api_base")
                        ),
                    }
                )
            continue
        coverage_counts["missing"] += 1
        coverage_category_counts[f"{category}_missing_result_count"] += 1
        if len(missing_samples) < sample_limit:
            missing_samples.append(
                {
                    "classification": classification,
                    "org_name": result.get("org_name"),
                    "api_base": base or result.get("api_base"),
                    "status_code": result.get("status_code"),
                    "payer_id": result.get("payer_id"),
                }
            )

    return {
        "available": True,
        "tested_at": retest_payload.get("tested_at") if isinstance(retest_payload, dict) else None,
        "checked_classifications": sorted(checked_classifications),
        "checked_result_count": checked_result_count,
        "checked_unique_api_base_count": len(unique_bases),
        "classification_counts": classification_counts,
        **coverage_counts,
        **coverage_category_counts,
        "covered_count": coverage_counts["covered_current_base"] + coverage_counts["covered_by_redirect"],
        "missing_result_count": coverage_counts["missing"],
        "missing_samples": missing_samples,
        "redirected_samples": redirected_samples,
        "unchecked_classification_counts": unchecked_classification_counts,
        "unchecked_result_count": sum(unchecked_classification_counts.values()),
        "recovered_unchecked_result_count": recovered_unchecked_result_count,
        "uncovered_unchecked_result_count": uncovered_unchecked_result_count,
        "uncovered_unchecked_clusters": sorted(
            uncovered_unchecked_clusters_by_key.values(),
            key=lambda item: (-_int(item.get("result_count")), item.get("source_host") or ""),
        )[:sample_limit],
    }


async def _source_catalog_retest_coverage_from_path(
    conn: asyncpg.Connection,
    schema: str,
    *,
    retest_results_path: str | None,
    sample_limit: int,
) -> dict[str, Any]:
    path = _clean_text(retest_results_path)
    if not path:
        return {"available": False, "skipped": True, "reason": "no retest results path provided"}
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "reason": "provider_directory_source unavailable", "missing_result_count": 0}
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"available": False, "reason": f"could not read retest results: {exc}", "missing_result_count": 0}
    rows = await conn.fetch(
        f"""
        SELECT source_id,
               org_name,
               api_base,
               canonical_api_base,
               last_validated_status,
               metadata_json
          FROM {_qt(schema, "provider_directory_source")}
        """
    )
    return _source_catalog_retest_coverage(
        payload,
        [dict(row) for row in rows],
        sample_limit=sample_limit,
    )


def _load_credentials_config(*, credential_config_file: str | None = None) -> tuple[dict[str, Any], str | None]:
    config: dict[str, Any] = {}
    override_path = _clean_text(credential_config_file)
    if override_path:
        try:
            payload = json.loads(Path(override_path).read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return payload, "argument_file"
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return {}, "argument_file_invalid"
        return {}, "argument_file_invalid"
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
    if raw:
        return config, "environment_json" if config else "environment_json_invalid"
    if path:
        return config, "environment_file" if config else "environment_file_invalid"
    return config, None


def _normalize_credential_key(value: Any) -> str:
    return str(value or "").strip().lower()


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


def _credential_spec_for_source(source: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
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
    normalized_hosts = {str(key).lower(): key for key in hosts}
    if host and host in normalized_hosts:
        key = normalized_hosts[host]
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


def _credential_spec_has_material(spec: dict[str, Any]) -> bool:
    if not spec:
        return False
    if _mapping(spec.get("headers")) or _mapping(spec.get("query_params")):
        return True
    if spec.get("bearer_token") or spec.get("api_key"):
        return True
    if _mapping(spec.get("oauth2") or spec.get("oauth")):
        return True
    return False


def _credential_env_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    if isinstance(value, str):
        text = _clean_text(value)
        if text and text.startswith("env:"):
            env_name = _clean_text(text[4:])
            if env_name:
                refs.add(env_name)
    elif isinstance(value, dict):
        for nested in value.values():
            refs.update(_credential_env_refs(nested))
    elif isinstance(value, list):
        for nested in value:
            refs.update(_credential_env_refs(nested))
    return refs


def _credential_secret_status(spec: dict[str, Any]) -> dict[str, Any]:
    env_refs = sorted(_credential_env_refs(spec))
    missing_env_refs = [name for name in env_refs if not _clean_text(os.getenv(name))]
    has_material = _credential_spec_has_material(spec)
    return {
        "has_material": has_material,
        "env_ref_count": len(env_refs),
        "missing_env_refs": missing_env_refs,
        "ready": bool(has_material and not missing_env_refs),
    }


async def _connect(args: argparse.Namespace) -> asyncpg.Connection:
    conn = await asyncpg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )
    if args.statement_timeout_ms:
        await conn.execute("SELECT set_config('statement_timeout', $1, false)", str(args.statement_timeout_ms))
    return conn


async def _relation_exists(conn: asyncpg.Connection, schema: str, name: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.tables
                 WHERE table_schema = $1
                   AND table_name = $2
                UNION ALL
                SELECT 1
                  FROM information_schema.views
                 WHERE table_schema = $1
                   AND table_name = $2
            )
            """,
            schema,
            name,
        )
    )


async def _relation_kind(conn: asyncpg.Connection, schema: str, name: str) -> str | None:
    value = await conn.fetchval(
        """
        SELECT CASE cls.relkind
                   WHEN 'r' THEN 'table'
                   WHEN 'p' THEN 'partitioned_table'
                   WHEN 'v' THEN 'view'
                   WHEN 'm' THEN 'materialized_view'
                   ELSE cls.relkind::text
               END AS relation_kind
          FROM pg_class cls
          JOIN pg_namespace ns ON ns.oid = cls.relnamespace
         WHERE ns.nspname = $1
           AND cls.relname = $2
        """,
        schema,
        name,
    )
    return str(value) if value else None


async def _column_exists(conn: asyncpg.Connection, schema: str, table: str, column: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_schema = $1
                   AND table_name = $2
                   AND column_name = $3
            )
            """,
            schema,
            table,
            column,
        )
    )


async def _fetch_mapping(conn: asyncpg.Connection, sql: str, *args: Any) -> dict[str, Any]:
    row = await conn.fetchrow(sql, *args)
    return dict(row) if row else {}


async def _table_row_estimate(conn: asyncpg.Connection, schema: str, table: str) -> dict[str, Any]:
    row = await _fetch_mapping(
        conn,
        """
        SELECT n_live_tup::bigint AS row_count,
               last_analyze,
               last_autoanalyze
          FROM pg_stat_user_tables
         WHERE schemaname = $1
           AND relname = $2
        """,
        schema,
        table,
    )
    row["row_count"] = _int(row.get("row_count"))
    return row


async def _column_distinct_estimate(
    conn: asyncpg.Connection,
    schema: str,
    table: str,
    column: str,
    *,
    row_count: int,
) -> int | None:
    value = await conn.fetchval(
        """
        SELECT n_distinct
          FROM pg_stats
         WHERE schemaname = $1
           AND tablename = $2
           AND attname = $3
        """,
        schema,
        table,
        column,
    )
    if value is None:
        return None
    distinct = float(value)
    if distinct < 0:
        return max(0, int(round(abs(distinct) * row_count)))
    return max(0, int(round(distinct)))


async def _source_summary(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False}
    gateway_hosts = _sql_string_array(FHIR_ONBOARDING_GATEWAY_HOSTS)
    credential_markers = _sql_string_array(FHIR_CREDENTIAL_AUTH_MARKERS)
    row = await _fetch_mapping(
        conn,
        f"""
        WITH src AS (
            SELECT *,
                   split_part(
                       regexp_replace(coalesce(canonical_api_base, api_base, portal_url, ''), '^https?://', ''),
                       '/',
                       1
                   ) AS source_host,
                   lower(coalesce(auth_type, '')) AS auth_type_norm
              FROM {_qt(schema, "provider_directory_source")}
        )
        SELECT
            count(*)::bigint AS source_count,
            count(*) FILTER (WHERE canonical_api_base IS NOT NULL)::bigint AS api_base_count,
            count(*) FILTER (WHERE last_probe_status = 'valid')::bigint AS live_valid_count,
            count(*) FILTER (WHERE last_probe_status = 'auth_required')::bigint AS live_auth_required_count,
            count(*) FILTER (WHERE last_probe_status = 'timeout')::bigint AS live_timeout_count,
            count(*) FILTER (WHERE last_probe_status = 'valid_non_fhir')::bigint AS live_valid_non_fhir_count,
            count(*) FILTER (
                WHERE last_probe_status = 'valid_non_fhir'
                  AND (
                      source_host = ANY({gateway_hosts})
                      OR EXISTS (
                          SELECT 1
                            FROM unnest({credential_markers}) AS marker(value)
                           WHERE auth_type_norm LIKE '%' || marker.value || '%'
                      )
                  )
            )::bigint AS live_credential_or_gateway_non_fhir_count,
            count(*) FILTER (WHERE last_probe_status IS NULL)::bigint AS never_probed_count,
            count(*) FILTER (WHERE auth_type IN ('open', 'none', '') OR auth_type IS NULL)::bigint AS open_or_none_auth_count,
            count(*) FILTER (WHERE is_medicare_advantage IS TRUE)::bigint AS medicare_advantage_count,
            count(*) FILTER (WHERE is_medicaid_mco IS TRUE)::bigint AS medicaid_mco_count,
            count(*) FILTER (WHERE is_qhp IS TRUE)::bigint AS qhp_count,
            max(last_probed_at) AS last_probed_at
          FROM src
        """,
    )
    source_count = _int(row.get("source_count"))
    row["available"] = True
    row["api_base_pct"] = _pct(_int(row.get("api_base_count")), source_count)
    row["live_valid_pct"] = _pct(_int(row.get("live_valid_count")), source_count)
    row["auth_required_pct"] = _pct(_int(row.get("live_auth_required_count")), source_count)
    row["timeout_pct"] = _pct(_int(row.get("live_timeout_count")), source_count)
    row["valid_non_fhir_pct"] = _pct(_int(row.get("live_valid_non_fhir_count")), source_count)
    return row


async def _probe_timeout_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "timeout_source_count": 0, "groups": []}
    limit = max(1, sample_limit)
    rows = await conn.fetch(
        f"""
        WITH timeout_sources AS (
            SELECT source_id,
                   org_name,
                   plan_name,
                   canonical_api_base,
                   api_base,
                   coalesce(NULLIF(auth_type, ''), '') AS auth_type,
                   last_probe_error,
                   is_medicare_advantage,
                   is_medicaid_mco,
                   is_chip,
                   is_qhp,
                   coalesce(
                       NULLIF(
                           split_part(
                               regexp_replace(coalesce(canonical_api_base, api_base, portal_url, ''), '^https?://', ''),
                               '/',
                               1
                           ),
                           ''
                       ),
                       '(missing host)'
                   ) AS source_host
              FROM {_qt(schema, "provider_directory_source")}
             WHERE last_probe_status = 'timeout'
        ),
        ranked AS (
            SELECT *,
                   row_number() OVER (
                       PARTITION BY source_host, auth_type
                       ORDER BY lower(org_name), lower(coalesce(plan_name, '')), source_id
                   ) AS sample_rank
              FROM timeout_sources
        )
        SELECT source_host,
               auth_type,
               count(*)::bigint AS source_count,
               count(*) FILTER (WHERE is_medicare_advantage IS TRUE)::bigint AS medicare_advantage_source_count,
               count(*) FILTER (WHERE is_medicaid_mco IS TRUE)::bigint AS medicaid_mco_source_count,
               count(*) FILTER (WHERE is_chip IS TRUE)::bigint AS chip_source_count,
               count(*) FILTER (WHERE is_qhp IS TRUE)::bigint AS qhp_source_count,
               array_remove(array_agg(source_id ORDER BY sample_rank) FILTER (WHERE sample_rank <= {limit}), NULL) AS sample_source_ids,
               array_remove(array_agg(coalesce(canonical_api_base, api_base) ORDER BY sample_rank) FILTER (WHERE sample_rank <= {limit}), NULL) AS sample_api_bases,
               array_remove(
                   array_agg(
                       concat_ws(' / ', org_name, NULLIF(plan_name, ''))
                       ORDER BY sample_rank
                   ) FILTER (WHERE sample_rank <= {limit}),
                   NULL
               ) AS sample_payers,
               array_remove(array_agg(last_probe_error ORDER BY sample_rank) FILTER (WHERE sample_rank <= {limit}), NULL) AS sample_errors
          FROM ranked
         GROUP BY source_host, auth_type
         ORDER BY source_count DESC, source_host, auth_type
        """
    )
    groups = []
    for row in rows:
        item = dict(row)
        item["source_count"] = _int(item.get("source_count"))
        item["regulated_market_source_count"] = sum(
            _int(item.get(field))
            for field in (
                "medicare_advantage_source_count",
                "medicaid_mco_source_count",
                "chip_source_count",
                "qhp_source_count",
            )
        )
        groups.append(item)
    return {
        "available": True,
        "timeout_source_count": sum(_int(item.get("source_count")) for item in groups),
        "host_count": len({item.get("source_host") for item in groups}),
        "group_count": len(groups),
        "groups": groups,
    }


async def _credential_onboarding_backlog(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
    credential_config_file: str | None = None,
) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "blocked_source_count": 0, "groups": []}
    gateway_hosts = _sql_string_array(FHIR_ONBOARDING_GATEWAY_HOSTS)
    credential_markers = _sql_string_array(FHIR_CREDENTIAL_AUTH_MARKERS)
    rows = await conn.fetch(
        f"""
        WITH src AS (
            SELECT source_id,
                   org_name,
                   plan_name,
                   canonical_api_base,
                   api_base,
                   portal_url,
                   last_probe_status,
                   last_validated_status,
                   COALESCE(NULLIF(last_probe_status, ''), NULLIF(last_validated_status, '')) AS effective_probe_status,
                   coalesce(NULLIF(auth_type, ''), '') AS auth_type,
                   lower(coalesce(auth_type, '')) AS auth_type_norm,
                   is_medicare_advantage,
                   is_medicaid_mco,
                   is_chip,
                   is_qhp,
                   split_part(
                       regexp_replace(coalesce(canonical_api_base, api_base, portal_url, ''), '^https?://', ''),
                       '/',
                       1
                   ) AS source_host
              FROM {_qt(schema, "provider_directory_source")}
        ),
        blocked AS (
            SELECT *,
                   CASE
                       WHEN effective_probe_status = 'auth_required' THEN 'auth_required'
                       WHEN effective_probe_status = 'valid_non_fhir'
                            AND source_host = ANY({gateway_hosts})
                           THEN 'onboarding_gateway'
                       WHEN effective_probe_status = 'valid_non_fhir'
                            AND EXISTS (
                                SELECT 1
                                  FROM unnest({credential_markers}) AS marker(value)
                                 WHERE auth_type_norm LIKE '%' || marker.value || '%'
                            )
                           THEN 'credentialed_non_fhir'
                       ELSE 'unknown'
                   END AS reason
              FROM src
             WHERE effective_probe_status = 'auth_required'
                OR (
                    effective_probe_status = 'valid_non_fhir'
                    AND (
                        source_host = ANY({gateway_hosts})
                        OR EXISTS (
                            SELECT 1
                              FROM unnest({credential_markers}) AS marker(value)
                             WHERE auth_type_norm LIKE '%' || marker.value || '%'
                        )
                    )
                )
        )
        SELECT source_id,
               org_name,
               plan_name,
               canonical_api_base,
               api_base,
               portal_url,
               coalesce(NULLIF(source_host, ''), '(missing host)') AS source_host,
               effective_probe_status AS probe_status,
               auth_type,
               reason,
               is_medicare_advantage,
               is_medicaid_mco,
               is_chip,
               is_qhp
          FROM blocked
         ORDER BY lower(org_name), lower(coalesce(plan_name, '')), source_id
        """
    )
    config, config_source = _load_credentials_config(credential_config_file=credential_config_file)
    groups_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    configured_source_count = 0
    missing_source_count = 0
    secret_ready_source_count = 0
    secret_missing_source_count = 0
    missing_secret_env_vars: set[str] = set()
    for row in rows:
        item = dict(row)
        key = (
            str(item.get("source_host") or "(missing host)"),
            str(item.get("probe_status") or ""),
            str(item.get("auth_type") or ""),
            str(item.get("reason") or ""),
        )
        group = groups_by_key.setdefault(
            key,
            {
                "source_host": key[0],
                "probe_status": key[1],
                "auth_type": key[2],
                "reason": key[3],
                "source_count": 0,
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 0,
                "credential_secret_ready_source_count": 0,
                "credential_secret_missing_source_count": 0,
                "credential_rule_candidate_source_count": 0,
                "endpoint_discovery_needed_source_count": 0,
                "regulated_market_source_count": 0,
                "medicare_advantage_source_count": 0,
                "medicaid_mco_source_count": 0,
                "chip_source_count": 0,
                "qhp_source_count": 0,
                "sample_payers": [],
                "sample_missing_credential_payers": [],
                "sample_missing_secret_payers": [],
                "sample_missing_secret_env_vars": [],
                "sample_source_ids": [],
                "sample_api_bases": [],
                "sample_sources": [],
                "sample_missing_credential_sources": [],
                "sample_missing_secret_sources": [],
                "sample_endpoint_discovery_sources": [],
                "_api_bases_seen": set(),
                "_api_base_targets": {},
            },
        )
        payer_label = " / ".join(
            part
            for part in (
                _clean_text(item.get("org_name")),
                _clean_text(item.get("plan_name")),
            )
            if part
        )
        group["source_count"] += 1
        if any(
            item.get(field) is True
            for field in ("is_medicare_advantage", "is_medicaid_mco", "is_chip", "is_qhp")
        ):
            group["regulated_market_source_count"] += 1
        if item.get("is_medicare_advantage") is True:
            group["medicare_advantage_source_count"] += 1
        if item.get("is_medicaid_mco") is True:
            group["medicaid_mco_source_count"] += 1
        if item.get("is_chip") is True:
            group["chip_source_count"] += 1
        if item.get("is_qhp") is True:
            group["qhp_source_count"] += 1
        if len(group["sample_payers"]) < sample_limit:
            group["sample_payers"].append(payer_label or item.get("source_id"))
        if len(group["sample_source_ids"]) < sample_limit and item.get("source_id"):
            group["sample_source_ids"].append(item.get("source_id"))
        api_base = _canonical_base(item.get("canonical_api_base") or item.get("api_base"))
        endpoint_discovery_needed = _credential_backlog_endpoint_discovery_needed(item, api_base=api_base)
        if endpoint_discovery_needed:
            group["endpoint_discovery_needed_source_count"] += 1
        else:
            group["credential_rule_candidate_source_count"] += 1
        api_base_target: dict[str, Any] | None = None
        if api_base:
            group["_api_bases_seen"].add(api_base)
            api_base_target = group["_api_base_targets"].setdefault(
                api_base,
                {
                    "api_base": api_base,
                    "source_count": 0,
                    "credential_configured_source_count": 0,
                    "credential_config_missing_source_count": 0,
                    "credential_secret_ready_source_count": 0,
                    "credential_secret_missing_source_count": 0,
                    "credential_rule_candidate_source_count": 0,
                    "endpoint_discovery_needed_source_count": 0,
                    "regulated_market_source_count": 0,
                    "medicare_advantage_source_count": 0,
                    "medicaid_mco_source_count": 0,
                    "chip_source_count": 0,
                    "qhp_source_count": 0,
                    "sample_payers": [],
                    "sample_source_ids": [],
                    "sample_missing_credential_payers": [],
                    "sample_missing_secret_payers": [],
                    "sample_missing_secret_env_vars": [],
                    "sample_sources": [],
                    "sample_missing_credential_sources": [],
                    "sample_missing_secret_sources": [],
                    "sample_endpoint_discovery_sources": [],
                },
            )
            api_base_target["source_count"] += 1
            if endpoint_discovery_needed:
                api_base_target["endpoint_discovery_needed_source_count"] += 1
            else:
                api_base_target["credential_rule_candidate_source_count"] += 1
            if any(
                item.get(field) is True
                for field in ("is_medicare_advantage", "is_medicaid_mco", "is_chip", "is_qhp")
            ):
                api_base_target["regulated_market_source_count"] += 1
            if item.get("is_medicare_advantage") is True:
                api_base_target["medicare_advantage_source_count"] += 1
            if item.get("is_medicaid_mco") is True:
                api_base_target["medicaid_mco_source_count"] += 1
            if item.get("is_chip") is True:
                api_base_target["chip_source_count"] += 1
            if item.get("is_qhp") is True:
                api_base_target["qhp_source_count"] += 1
            if len(api_base_target["sample_payers"]) < sample_limit:
                api_base_target["sample_payers"].append(payer_label or item.get("source_id"))
            if item.get("source_id") and len(api_base_target["sample_source_ids"]) < sample_limit:
                api_base_target["sample_source_ids"].append(item.get("source_id"))
        if api_base and len(group["sample_api_bases"]) < sample_limit and api_base not in group["sample_api_bases"]:
            group["sample_api_bases"].append(api_base)
        source_sample = _credential_source_sample(item, payer_label=payer_label, api_base=api_base)
        _append_unique_source_sample(group["sample_sources"], source_sample, limit=sample_limit)
        if api_base_target is not None:
            _append_unique_source_sample(api_base_target["sample_sources"], source_sample, limit=sample_limit)
        if endpoint_discovery_needed:
            _append_unique_source_sample(group["sample_endpoint_discovery_sources"], source_sample, limit=sample_limit)
            if api_base_target is not None:
                _append_unique_source_sample(
                    api_base_target["sample_endpoint_discovery_sources"],
                    source_sample,
                    limit=sample_limit,
                )

        spec = _credential_spec_for_source(item, config)
        if _credential_spec_has_material(spec):
            configured_source_count += 1
            group["credential_configured_source_count"] += 1
            if api_base_target is not None:
                api_base_target["credential_configured_source_count"] += 1
            secret_status = _credential_secret_status(spec)
            if secret_status["ready"]:
                secret_ready_source_count += 1
                group["credential_secret_ready_source_count"] += 1
                if api_base_target is not None:
                    api_base_target["credential_secret_ready_source_count"] += 1
            else:
                secret_missing_source_count += 1
                group["credential_secret_missing_source_count"] += 1
                if api_base_target is not None:
                    api_base_target["credential_secret_missing_source_count"] += 1
                missing_secret_env_vars.update(secret_status["missing_env_refs"])
                for env_name in secret_status["missing_env_refs"]:
                    if len(group["sample_missing_secret_env_vars"]) < sample_limit and env_name not in group["sample_missing_secret_env_vars"]:
                        group["sample_missing_secret_env_vars"].append(env_name)
                    if (
                        api_base_target is not None
                        and len(api_base_target["sample_missing_secret_env_vars"]) < sample_limit
                        and env_name not in api_base_target["sample_missing_secret_env_vars"]
                    ):
                        api_base_target["sample_missing_secret_env_vars"].append(env_name)
                if len(group["sample_missing_secret_payers"]) < sample_limit:
                    group["sample_missing_secret_payers"].append(payer_label or item.get("source_id"))
                if api_base_target is not None and len(api_base_target["sample_missing_secret_payers"]) < sample_limit:
                    api_base_target["sample_missing_secret_payers"].append(payer_label or item.get("source_id"))
                _append_unique_source_sample(group["sample_missing_secret_sources"], source_sample, limit=sample_limit)
                if api_base_target is not None:
                    _append_unique_source_sample(
                        api_base_target["sample_missing_secret_sources"],
                        source_sample,
                        limit=sample_limit,
                    )
        else:
            missing_source_count += 1
            group["credential_config_missing_source_count"] += 1
            if api_base_target is not None:
                api_base_target["credential_config_missing_source_count"] += 1
            if len(group["sample_missing_credential_payers"]) < sample_limit:
                group["sample_missing_credential_payers"].append(payer_label or item.get("source_id"))
            if api_base_target is not None and len(api_base_target["sample_missing_credential_payers"]) < sample_limit:
                api_base_target["sample_missing_credential_payers"].append(payer_label or item.get("source_id"))
            _append_unique_source_sample(group["sample_missing_credential_sources"], source_sample, limit=sample_limit)
            if api_base_target is not None:
                _append_unique_source_sample(
                    api_base_target["sample_missing_credential_sources"],
                    source_sample,
                    limit=sample_limit,
                )

    prepared_groups = []
    for group in groups_by_key.values():
        api_base_count = len(group.pop("_api_bases_seen", set()))
        sample_api_base_count = len(group.get("sample_api_bases") or [])
        api_base_targets = sorted(
            group.pop("_api_base_targets", {}).values(),
            key=lambda item: (-_int(item.get("source_count")), str(item.get("api_base"))),
        )
        group["api_base_count"] = api_base_count
        group["sample_api_base_count"] = sample_api_base_count
        group["api_base_sample_complete"] = sample_api_base_count >= api_base_count
        group["api_base_targets"] = api_base_targets
        prepared_groups.append(group)

    groups = sorted(
        prepared_groups,
        key=lambda group: (
            -_int(group.get("source_count")),
            str(group.get("source_host")),
            str(group.get("probe_status")),
            str(group.get("auth_type")),
            str(group.get("reason")),
        ),
    )
    return {
        "available": True,
        "blocked_source_count": len(rows),
        "credential_config_available": bool(config),
        "credential_config_source": config_source,
        "credential_configured_source_count": configured_source_count,
        "credential_config_missing_source_count": missing_source_count,
        "credential_secret_ready_source_count": secret_ready_source_count,
        "credential_secret_missing_source_count": secret_missing_source_count,
        "credential_rule_candidate_source_count": sum(
            _int(group.get("credential_rule_candidate_source_count")) for group in groups
        ),
        "endpoint_discovery_needed_source_count": sum(
            _int(group.get("endpoint_discovery_needed_source_count")) for group in groups
        ),
        "credential_missing_secret_env_vars": sorted(missing_secret_env_vars),
        "group_count": len(groups),
        "groups": groups,
    }


def _credential_backlog_export(report: dict[str, Any]) -> dict[str, Any]:
    backlog = report.get("credential_onboarding_backlog") or {}
    groups = []
    for group in backlog.get("groups") or []:
        source_host = _clean_text(group.get("source_host"))
        suggested_rule = None
        if source_host and source_host != "(missing host)":
            suggested_rule = {
                "section": "hosts",
                "key": source_host,
                "template": {
                    "oauth2": {
                        "token_url": "env:PROVIDER_DIRECTORY_TOKEN_URL",
                        "client_id": "env:PROVIDER_DIRECTORY_CLIENT_ID",
                        "client_secret": "env:PROVIDER_DIRECTORY_CLIENT_SECRET",
                        "scope": "system/*.read",
                        "auth": "basic",
                    }
                },
            }
        groups.append(
            {
                "host": source_host,
                "source_host": source_host,
                "probe_status": group.get("probe_status"),
                "auth_type": group.get("auth_type"),
                "reason": group.get("reason"),
                "source_count": group.get("source_count"),
                "credential_configured_source_count": group.get("credential_configured_source_count"),
                "credential_config_missing_source_count": group.get("credential_config_missing_source_count"),
                "credential_secret_ready_source_count": group.get("credential_secret_ready_source_count"),
                "credential_secret_missing_source_count": group.get("credential_secret_missing_source_count"),
                "credential_rule_candidate_source_count": group.get("credential_rule_candidate_source_count"),
                "endpoint_discovery_needed_source_count": group.get("endpoint_discovery_needed_source_count"),
                **_credential_market_counts(group),
                "sample_payers": _list(group.get("sample_payers")),
                "sample_missing_credential_payers": _list(group.get("sample_missing_credential_payers")),
                "sample_missing_secret_payers": _list(group.get("sample_missing_secret_payers")),
                "sample_missing_secret_env_vars": _list(group.get("sample_missing_secret_env_vars")),
                "sample_source_ids": _list(group.get("sample_source_ids")),
                "sample_api_bases": _list(group.get("sample_api_bases")),
                "sample_sources": _list(group.get("sample_sources")),
                "sample_missing_credential_sources": _list(group.get("sample_missing_credential_sources")),
                "sample_missing_secret_sources": _list(group.get("sample_missing_secret_sources")),
                "sample_endpoint_discovery_sources": _list(group.get("sample_endpoint_discovery_sources")),
                "api_base_count": group.get("api_base_count"),
                "sample_api_base_count": group.get("sample_api_base_count"),
                "api_base_sample_complete": group.get("api_base_sample_complete"),
                "suggested_credential_rule": suggested_rule,
            }
        )
    return {
        "generated_at": report.get("generated_at"),
        "schema": report.get("schema"),
        "credential_config_available": backlog.get("credential_config_available"),
        "credential_config_source": backlog.get("credential_config_source"),
        "blocked_source_count": backlog.get("blocked_source_count"),
        "credential_configured_source_count": backlog.get("credential_configured_source_count"),
        "credential_config_missing_source_count": backlog.get("credential_config_missing_source_count"),
        "credential_secret_ready_source_count": backlog.get("credential_secret_ready_source_count"),
        "credential_secret_missing_source_count": backlog.get("credential_secret_missing_source_count"),
        "credential_rule_candidate_source_count": backlog.get("credential_rule_candidate_source_count"),
        "endpoint_discovery_needed_source_count": backlog.get("endpoint_discovery_needed_source_count"),
        "credential_missing_secret_env_vars": _list(backlog.get("credential_missing_secret_env_vars")),
        "group_count": backlog.get("group_count"),
        "groups": groups,
    }


def _credential_env_prefix(source_host: str) -> str:
    key = re.sub(r"[^A-Za-z0-9]+", "_", source_host).strip("_").upper()
    return f"PROVIDER_DIRECTORY_{key}" if key else "PROVIDER_DIRECTORY"


def _credential_api_base_env_prefix(api_base: str) -> str:
    canonical = _canonical_base(api_base) or str(api_base).rstrip("/")
    parsed = urllib.parse.urlsplit(canonical)
    raw = f"{parsed.netloc}_{parsed.path.strip('/')}" if parsed.netloc else canonical
    key = re.sub(r"[^A-Za-z0-9]+", "_", raw).strip("_").upper()
    digest = hashlib.sha1(canonical.encode("utf-8", errors="ignore")).hexdigest()[:8].upper()
    if key:
        key = key[:72].rstrip("_")
        return f"PROVIDER_DIRECTORY_{key}_{digest}"
    return f"PROVIDER_DIRECTORY_API_BASE_{digest}"


def _credential_template_needs_api_key(group: dict[str, Any]) -> bool:
    text = f"{group.get('auth_type') or ''} {group.get('reason') or ''}".lower()
    return "api key" in text or "apikey" in text


def _credential_template_needs_oauth2(group: dict[str, Any]) -> bool:
    text = f"{group.get('auth_type') or ''} {group.get('reason') or ''} {group.get('probe_status') or ''}".lower()
    if any(marker in text for marker in ("oauth", "client credential", "bearer", "token")):
        return True
    return group.get("probe_status") == "auth_required" and not _credential_template_needs_api_key(group)


def _credential_template_review_reasons(group: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    source_host = (_clean_text(group.get("source_host")) or "").lower()
    api_bases = [_clean_text(value) or "" for value in _list(group.get("sample_api_bases"))]
    api_base_count = _int(group.get("api_base_count"))
    path_tokens = ("developer", "legal", "portal", "interoperability", "apis", "docs", "documentation")
    portal_like = []
    if source_host and "fhir" not in source_host and any(token in source_host for token in path_tokens):
        portal_like.append(source_host)
    for api_base in api_bases:
        parsed = urllib.parse.urlsplit(api_base)
        path = parsed.path.lower()
        if path and "fhir" not in path and any(token in path for token in path_tokens):
            portal_like.append(api_base)
    if portal_like:
        reasons.append(
            "Sample API bases or source hosts look like portal/documentation URLs; confirm the real FHIR base before enabling a host-level rule."
        )
    if api_base_count > 1 or len({urllib.parse.urlsplit(api_base).path.rstrip("/") for api_base in api_bases if api_base}) > 3:
        reasons.append(
            "This host has several known or sampled FHIR path variants; verify one host-level credential is valid for every path before enabling."
        )
    return reasons


def _credential_template_host_review_reasons(groups: list[dict[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if any(_credential_template_needs_api_key(group) for group in groups) and any(
        _credential_template_needs_oauth2(group) for group in groups
    ):
        reasons.append(
            "This host has both OAuth2 and API-key credential groups; avoid a single host-level rule unless the payer portal confirms both credentials are required together for every path. Prefer api_bases or sources rules for payer-specific auth."
        )
    return reasons


def _credential_template_api_base_candidates(group: dict[str, Any]) -> list[str]:
    candidates: list[str] = []
    for raw_api_base in _list(group.get("sample_api_bases")):
        api_base = _canonical_base(raw_api_base)
        if not api_base:
            continue
        parsed = urllib.parse.urlsplit(api_base)
        if not parsed.scheme or not parsed.netloc:
            continue
        if api_base not in candidates:
            candidates.append(api_base)
    return candidates


def _credential_template_rule_for_group(group: dict[str, Any], env_prefix: str) -> dict[str, Any]:
    rule: dict[str, Any] = {}
    if _credential_template_needs_oauth2(group):
        rule["oauth2"] = {
            "token_url": f"env:{env_prefix}_TOKEN_URL",
            "client_id": f"env:{env_prefix}_CLIENT_ID",
            "client_secret": f"env:{env_prefix}_CLIENT_SECRET",
            "scope": "system/*.read",
            "auth": "basic",
        }
    if _credential_template_needs_api_key(group):
        rule["api_key"] = {
            "header": f"env:{env_prefix}_API_KEY_HEADER",
            "value": f"env:{env_prefix}_API_KEY",
        }
    return rule


def _credential_config_template_export(report: dict[str, Any]) -> dict[str, Any]:
    backlog = _credential_backlog_export(report)
    backlog_groups = backlog.get("groups") or []
    groups_by_host: dict[str, list[dict[str, Any]]] = {}
    for group in backlog_groups:
        source_host = _clean_text(group.get("source_host"))
        if source_host and source_host != "(missing host)":
            groups_by_host.setdefault(source_host, []).append(group)
    hosts: dict[str, dict[str, Any]] = {}
    api_bases: dict[str, dict[str, Any]] = {}
    rule_summaries: list[dict[str, Any]] = []
    for group in backlog_groups:
        source_host = _clean_text(group.get("source_host"))
        if not source_host or source_host == "(missing host)" or not _int(group.get("credential_config_missing_source_count")):
            continue
        env_prefix = _credential_env_prefix(source_host)
        rule = hosts.setdefault(
            source_host,
            {
                "_notes": [
                    "Review payer developer portal requirements before enabling.",
                    "Replace env placeholders with host-specific secret names in the deployment environment.",
                ],
            },
        )
        host_groups = groups_by_host.get(source_host, [])
        review_reasons = [
            *_credential_template_review_reasons(group),
            *_credential_template_host_review_reasons(host_groups),
        ]
        if review_reasons:
            existing = list(rule.get("_review") or [])
            for reason in review_reasons:
                if reason not in existing:
                    existing.append(reason)
            rule["_review"] = existing
        for key, value in _credential_template_rule_for_group(group, env_prefix).items():
            rule.setdefault(key, value)
        api_base_rule_samples: dict[str, dict[str, Any]] = {}
        if review_reasons:
            for api_base in _credential_template_api_base_candidates(group):
                api_base_env_prefix = _credential_api_base_env_prefix(api_base)
                api_base_rule = api_bases.setdefault(
                    api_base,
                    {
                        "_notes": [
                            "Path-scoped candidate generated because the host-level rule needs review.",
                            "Confirm this exact FHIR base and credential scheme before enabling.",
                        ],
                    },
                )
                for key, value in _credential_template_rule_for_group(group, api_base_env_prefix).items():
                    api_base_rule.setdefault(key, value)
                api_base_rule_samples[api_base] = api_base_rule
        rule_summaries.append(
            {
                "source_host": source_host,
                "auth_type": group.get("auth_type"),
                "reason": group.get("reason"),
                "missing_source_count": group.get("credential_config_missing_source_count"),
                "secret_ready_source_count": group.get("credential_secret_ready_source_count"),
                "secret_missing_source_count": group.get("credential_secret_missing_source_count"),
                "credential_rule_candidate_source_count": group.get("credential_rule_candidate_source_count"),
                "endpoint_discovery_needed_source_count": group.get("endpoint_discovery_needed_source_count"),
                **_credential_market_counts(group),
                "sample_missing_secret_env_vars": group.get("sample_missing_secret_env_vars") or [],
                "sample_payers": group.get("sample_missing_credential_payers") or group.get("sample_payers") or [],
                "sample_source_ids": group.get("sample_source_ids") or [],
                "sample_api_bases": group.get("sample_api_bases") or [],
                "sample_sources": group.get("sample_missing_credential_sources") or group.get("sample_sources") or [],
                "sample_endpoint_discovery_sources": group.get("sample_endpoint_discovery_sources") or [],
                "api_base_count": group.get("api_base_count"),
                "sample_api_base_count": group.get("sample_api_base_count"),
                "api_base_sample_complete": group.get("api_base_sample_complete"),
                "api_base_rule_template_count": len(api_base_rule_samples),
                "api_base_rule_templates": api_base_rule_samples,
                "review_reasons": review_reasons,
            }
        )
    return {
        "generated_at": backlog.get("generated_at"),
        "schema": backlog.get("schema"),
        "credential_config_available": backlog.get("credential_config_available"),
        "credential_config_source": backlog.get("credential_config_source"),
        "credential_config_missing_source_count": backlog.get("credential_config_missing_source_count"),
        "credential_secret_ready_source_count": backlog.get("credential_secret_ready_source_count"),
        "credential_secret_missing_source_count": backlog.get("credential_secret_missing_source_count"),
        "credential_rule_candidate_source_count": backlog.get("credential_rule_candidate_source_count"),
        "endpoint_discovery_needed_source_count": backlog.get("endpoint_discovery_needed_source_count"),
        "credential_missing_secret_env_vars": _list(backlog.get("credential_missing_secret_env_vars")),
        "credential_config_template": {"hosts": hosts, "api_bases": api_bases},
        "rule_summaries": rule_summaries,
    }


def _credential_api_base_targets_export(report: dict[str, Any]) -> dict[str, Any]:
    backlog = report.get("credential_onboarding_backlog") or {}
    targets: list[dict[str, Any]] = []
    for group in backlog.get("groups") or []:
        for target in group.get("api_base_targets") or []:
            api_base = _clean_text(target.get("api_base"))
            if not api_base:
                continue
            env_prefix = _credential_api_base_env_prefix(api_base)
            targets.append(
                {
                    "host": group.get("source_host"),
                    "source_host": group.get("source_host"),
                    "probe_status": group.get("probe_status"),
                    "auth_type": group.get("auth_type"),
                    "reason": group.get("reason"),
                    "api_base": api_base,
                    "source_count": target.get("source_count"),
                    "credential_configured_source_count": target.get("credential_configured_source_count"),
                    "credential_config_missing_source_count": target.get("credential_config_missing_source_count"),
                    "credential_secret_ready_source_count": target.get("credential_secret_ready_source_count"),
                    "credential_secret_missing_source_count": target.get("credential_secret_missing_source_count"),
                    "credential_rule_candidate_source_count": target.get("credential_rule_candidate_source_count"),
                    "endpoint_discovery_needed_source_count": target.get("endpoint_discovery_needed_source_count"),
                    **_credential_market_counts(target),
                    "sample_payers": _list(target.get("sample_payers")),
                    "sample_source_ids": _list(target.get("sample_source_ids")),
                    "sample_missing_credential_payers": _list(target.get("sample_missing_credential_payers")),
                    "sample_missing_secret_payers": _list(target.get("sample_missing_secret_payers")),
                    "sample_missing_secret_env_vars": _list(target.get("sample_missing_secret_env_vars")),
                    "sample_sources": _list(target.get("sample_sources")),
                    "sample_missing_credential_sources": _list(target.get("sample_missing_credential_sources")),
                    "sample_missing_secret_sources": _list(target.get("sample_missing_secret_sources")),
                    "sample_endpoint_discovery_sources": _list(target.get("sample_endpoint_discovery_sources")),
                    "credential_rule_template": _credential_template_rule_for_group(group, env_prefix),
                }
            )
    targets.sort(
        key=lambda item: (
            str(item.get("source_host")),
            -_int(item.get("credential_config_missing_source_count")),
            -_int(item.get("source_count")),
            str(item.get("auth_type")),
            str(item.get("api_base")),
        )
    )
    return {
        "generated_at": report.get("generated_at"),
        "schema": report.get("schema"),
        "credential_config_available": backlog.get("credential_config_available"),
        "credential_config_source": backlog.get("credential_config_source"),
        "blocked_source_count": backlog.get("blocked_source_count"),
        "credential_configured_source_count": backlog.get("credential_configured_source_count"),
        "credential_config_missing_source_count": backlog.get("credential_config_missing_source_count"),
        "credential_secret_ready_source_count": backlog.get("credential_secret_ready_source_count"),
        "credential_secret_missing_source_count": backlog.get("credential_secret_missing_source_count"),
        "credential_rule_candidate_source_count": backlog.get("credential_rule_candidate_source_count"),
        "endpoint_discovery_needed_source_count": backlog.get("endpoint_discovery_needed_source_count"),
        "credential_missing_secret_env_vars": _list(backlog.get("credential_missing_secret_env_vars")),
        "api_base_target_count": len(targets),
        "targets": targets,
    }


def _extend_unique_samples(target: list[Any], values: list[Any], *, limit: int = 10) -> None:
    for value in values:
        if len(target) >= limit:
            break
        if value in target:
            continue
        target.append(value)


def _credential_priority_export(report: dict[str, Any]) -> dict[str, Any]:
    backlog = _credential_backlog_export(report)
    template = _credential_config_template_export(report)
    templates_by_host = (template.get("credential_config_template") or {}).get("hosts") or {}
    hosts_by_name: dict[str, dict[str, Any]] = {}

    for group in backlog.get("groups") or []:
        source_host = _clean_text(group.get("source_host")) or "(missing host)"
        host = hosts_by_name.setdefault(
            source_host,
            {
                "host": source_host,
                "source_host": source_host,
                "source_count": 0,
                "credential_configured_source_count": 0,
                "credential_config_missing_source_count": 0,
                "credential_secret_ready_source_count": 0,
                "credential_secret_missing_source_count": 0,
                "credential_rule_candidate_source_count": 0,
                "endpoint_discovery_needed_source_count": 0,
                "probe_statuses": set(),
                "auth_types": set(),
                "reasons": set(),
                "sample_payers": [],
                "sample_missing_credential_payers": [],
                "sample_missing_secret_payers": [],
                "sample_missing_secret_env_vars": [],
                "sample_source_ids": [],
                "sample_api_bases": [],
                "sample_sources": [],
                "sample_missing_credential_sources": [],
                "sample_missing_secret_sources": [],
                "sample_endpoint_discovery_sources": [],
                "api_base_group_count": 0,
                "groups": [],
                **{field: 0 for field in PROVIDER_DIRECTORY_MARKET_COUNT_FIELDS},
            },
        )
        for field in (
            "source_count",
            "credential_configured_source_count",
            "credential_config_missing_source_count",
            "credential_secret_ready_source_count",
            "credential_secret_missing_source_count",
            "credential_rule_candidate_source_count",
            "endpoint_discovery_needed_source_count",
            *PROVIDER_DIRECTORY_MARKET_COUNT_FIELDS,
        ):
            host[field] += _int(group.get(field))
        host["api_base_group_count"] += _int(group.get("api_base_count"))
        for field, target in (
            ("probe_status", "probe_statuses"),
            ("auth_type", "auth_types"),
            ("reason", "reasons"),
        ):
            value = _clean_text(group.get(field))
            if value:
                host[target].add(value)
        for field in (
            "sample_payers",
            "sample_missing_credential_payers",
            "sample_missing_secret_payers",
            "sample_missing_secret_env_vars",
            "sample_source_ids",
            "sample_api_bases",
            "sample_sources",
            "sample_missing_credential_sources",
            "sample_missing_secret_sources",
            "sample_endpoint_discovery_sources",
        ):
            _extend_unique_samples(host[field], _list(group.get(field)))
        host["groups"].append(
            {
                "probe_status": group.get("probe_status"),
                "auth_type": group.get("auth_type"),
                "reason": group.get("reason"),
                "source_count": group.get("source_count"),
                "credential_configured_source_count": group.get("credential_configured_source_count"),
                "credential_config_missing_source_count": group.get("credential_config_missing_source_count"),
                "credential_secret_ready_source_count": group.get("credential_secret_ready_source_count"),
                "credential_secret_missing_source_count": group.get("credential_secret_missing_source_count"),
                "credential_rule_candidate_source_count": group.get("credential_rule_candidate_source_count"),
                "endpoint_discovery_needed_source_count": group.get("endpoint_discovery_needed_source_count"),
                "api_base_count": group.get("api_base_count"),
                "sample_api_base_count": group.get("sample_api_base_count"),
                "api_base_sample_complete": group.get("api_base_sample_complete"),
                "sample_payers": _list(group.get("sample_payers")),
                "sample_source_ids": _list(group.get("sample_source_ids")),
                "sample_api_bases": _list(group.get("sample_api_bases")),
                "sample_sources": _list(group.get("sample_sources")),
                "sample_missing_credential_sources": _list(group.get("sample_missing_credential_sources")),
                "sample_missing_secret_sources": _list(group.get("sample_missing_secret_sources")),
                "sample_endpoint_discovery_sources": _list(group.get("sample_endpoint_discovery_sources")),
                **_credential_market_counts(group),
            }
        )

    hosts = []
    for host in hosts_by_name.values():
        host["probe_statuses"] = sorted(host["probe_statuses"])
        host["auth_types"] = sorted(host["auth_types"])
        host["reasons"] = sorted(host["reasons"])
        host["market_flag_source_mentions"] = sum(
            _int(host.get(field))
            for field in (
                "medicare_advantage_source_count",
                "medicaid_mco_source_count",
                "chip_source_count",
                "qhp_source_count",
            )
        )
        host["sample_api_base_count"] = len(host.get("sample_api_bases") or [])
        if host["source_host"] in templates_by_host:
            host["credential_rule_template"] = templates_by_host[host["source_host"]]
        hosts.append(host)

    hosts.sort(
        key=lambda item: (
            -_int(item.get("credential_config_missing_source_count")),
            -_int(item.get("regulated_market_source_count")),
            -_int(item.get("source_count")),
            str(item.get("source_host")),
        )
    )
    total_missing_sources = _int(backlog.get("credential_config_missing_source_count"))
    total_candidate_sources = _int(backlog.get("credential_rule_candidate_source_count"))
    total_endpoint_discovery_sources = _int(backlog.get("endpoint_discovery_needed_source_count"))
    cumulative_missing_sources = 0
    cumulative_candidate_sources = 0
    cumulative_endpoint_discovery_sources = 0
    cumulative_regulated_market_sources = 0
    for index, host in enumerate(hosts, start=1):
        host["priority_rank"] = index
        cumulative_missing_sources += _int(host.get("credential_config_missing_source_count"))
        cumulative_candidate_sources += _int(host.get("credential_rule_candidate_source_count"))
        cumulative_endpoint_discovery_sources += _int(host.get("endpoint_discovery_needed_source_count"))
        cumulative_regulated_market_sources += _int(host.get("regulated_market_source_count"))
        host["cumulative_credential_config_missing_source_count"] = cumulative_missing_sources
        host["cumulative_credential_rule_candidate_source_count"] = cumulative_candidate_sources
        host["cumulative_endpoint_discovery_needed_source_count"] = cumulative_endpoint_discovery_sources
        host["cumulative_regulated_market_source_count"] = cumulative_regulated_market_sources
        host["cumulative_missing_source_pct"] = _pct(cumulative_missing_sources, total_missing_sources)
        host["cumulative_candidate_source_pct"] = _pct(cumulative_candidate_sources, total_candidate_sources)
        host["cumulative_endpoint_discovery_source_pct"] = _pct(
            cumulative_endpoint_discovery_sources,
            total_endpoint_discovery_sources,
        )

    top_host_coverage = []
    top_n_values = sorted({value for value in (1, 3, 5, 10, 25, len(hosts)) if 0 < value <= len(hosts)})
    for top_n in top_n_values:
        if not hosts:
            break
        selected = hosts[:top_n]
        top_host_coverage.append(
            {
                "top_n": top_n,
                "host_count": len(selected),
                "credential_config_missing_source_count": sum(
                    _int(host.get("credential_config_missing_source_count")) for host in selected
                ),
                "credential_rule_candidate_source_count": sum(
                    _int(host.get("credential_rule_candidate_source_count")) for host in selected
                ),
                "endpoint_discovery_needed_source_count": sum(
                    _int(host.get("endpoint_discovery_needed_source_count")) for host in selected
                ),
                "regulated_market_source_count": sum(
                    _int(host.get("regulated_market_source_count")) for host in selected
                ),
                "missing_source_pct": _pct(
                    sum(_int(host.get("credential_config_missing_source_count")) for host in selected),
                    total_missing_sources,
                ),
                "candidate_source_pct": _pct(
                    sum(_int(host.get("credential_rule_candidate_source_count")) for host in selected),
                    total_candidate_sources,
                ),
                "endpoint_discovery_source_pct": _pct(
                    sum(_int(host.get("endpoint_discovery_needed_source_count")) for host in selected),
                    total_endpoint_discovery_sources,
                ),
                "hosts": [host.get("source_host") for host in selected],
            }
        )

    return {
        "generated_at": backlog.get("generated_at"),
        "schema": backlog.get("schema"),
        "credential_config_available": backlog.get("credential_config_available"),
        "credential_config_source": backlog.get("credential_config_source"),
        "blocked_source_count": backlog.get("blocked_source_count"),
        "credential_configured_source_count": backlog.get("credential_configured_source_count"),
        "credential_config_missing_source_count": backlog.get("credential_config_missing_source_count"),
        "credential_secret_ready_source_count": backlog.get("credential_secret_ready_source_count"),
        "credential_secret_missing_source_count": backlog.get("credential_secret_missing_source_count"),
        "credential_rule_candidate_source_count": backlog.get("credential_rule_candidate_source_count"),
        "endpoint_discovery_needed_source_count": backlog.get("endpoint_discovery_needed_source_count"),
        "credential_missing_secret_env_vars": _list(backlog.get("credential_missing_secret_env_vars")),
        "host_count": len(hosts),
        "top_host_coverage": top_host_coverage,
        "hosts": hosts,
    }


async def _capability_status_counts(conn: asyncpg.Connection, schema: str) -> list[dict[str, Any]]:
    if not await _relation_exists(conn, schema, "provider_directory_capability"):
        return []
    rows = await conn.fetch(
        f"""
        SELECT probe_status, count(*)::bigint AS count
          FROM {_qt(schema, "provider_directory_capability")}
         GROUP BY probe_status
         ORDER BY count(*) DESC, probe_status
        """
    )
    return [dict(row) for row in rows]


async def _estimated_resource_summary_row(
    conn: asyncpg.Connection,
    schema: str,
    table: str,
    *,
    columns: dict[str, bool],
) -> dict[str, Any]:
    estimate = await _table_row_estimate(conn, schema, table)
    row_count = _int(estimate.get("row_count"))
    return {
        "available": True,
        "estimated": True,
        "estimate_source": "pg_stat_user_tables/pg_stats",
        "row_count": row_count,
        "source_count": await _column_distinct_estimate(
            conn,
            schema,
            table,
            "source_id",
            row_count=row_count,
        ),
        "last_analyze": estimate.get("last_analyze"),
        "last_autoanalyze": estimate.get("last_autoanalyze"),
        "columns": columns,
    }


async def _resource_import_metadata_summary(conn: asyncpg.Connection, schema: str) -> dict[str, dict[str, Any]]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {}
    rows = await conn.fetch(
        f"""
        WITH resource_diagnostics AS (
            SELECT src.source_id,
                   resource.key AS resource_type,
                   COALESCE(NULLIF(resource.value->>'rows_written', ''), '0')::bigint AS rows_written,
                   COALESCE(NULLIF(resource.value->>'rows_fetched', ''), '0')::bigint AS rows_fetched,
                   COALESCE((resource.value->>'complete')::boolean, false) AS complete,
                   COALESCE((resource.value->>'bounded')::boolean, false) AS bounded,
                   NULLIF(resource.value->>'error', '') AS error
              FROM {_qt(schema, "provider_directory_source")} AS src
              CROSS JOIN LATERAL jsonb_each(
                    COALESCE(
                        src.metadata_json::jsonb #> '{{last_resource_import,resources}}',
                        '{{}}'::jsonb
                    )
              ) AS resource(key, value)
        )
        SELECT resource_type,
               sum(rows_written)::bigint AS row_count,
               sum(rows_fetched)::bigint AS fetched_row_count,
               count(DISTINCT source_id) FILTER (WHERE rows_written > 0)::bigint AS source_count,
               count(DISTINCT source_id) FILTER (WHERE rows_fetched > 0)::bigint AS fetched_source_count,
               count(DISTINCT source_id) FILTER (WHERE complete IS TRUE)::bigint AS complete_source_count,
               count(DISTINCT source_id) FILTER (WHERE bounded IS TRUE)::bigint AS bounded_source_count,
               count(DISTINCT source_id) FILTER (WHERE error IS NOT NULL)::bigint AS error_source_count
          FROM resource_diagnostics
         GROUP BY resource_type
        """
    )
    return {str(row["resource_type"]): dict(row) for row in rows}


async def _resource_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    use_estimates: bool = False,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    metadata_summary = await _resource_import_metadata_summary(conn, schema)
    resource_type_by_table = {
        table: resource_type for resource_type, table in PROVIDER_DIRECTORY_RESOURCE_TABLE_BY_TYPE.items()
    }
    for table in PROVIDER_DIRECTORY_RESOURCE_TABLES:
        if not await _relation_exists(conn, schema, table):
            summary[table] = {"available": False}
            continue
        columns = {
            "npi": await _column_exists(conn, schema, table, "npi"),
            "address_key": await _column_exists(conn, schema, table, "address_key"),
            "telephone_number": await _column_exists(conn, schema, table, "telephone_number"),
            "network_refs": await _column_exists(conn, schema, table, "network_refs"),
        }
        resource_type = resource_type_by_table.get(table)
        if use_estimates:
            summary[table] = await _estimated_resource_summary_row(
                conn,
                schema,
                table,
                columns=columns,
            )
            continue
        if metadata_summary:
            row = dict(metadata_summary.get(resource_type or "", {}))
            row.setdefault("resource_type", resource_type)
            row.setdefault("row_count", 0)
            row.setdefault("source_count", 0)
            row.setdefault("fetched_row_count", 0)
            row.setdefault("fetched_source_count", 0)
            row.setdefault("complete_source_count", 0)
            row.setdefault("bounded_source_count", 0)
            row.setdefault("error_source_count", 0)
            row["available"] = True
            row["columns"] = columns
            row["summary_source"] = "provider_directory_source.metadata_json.last_resource_import"
            summary[table] = row
            continue
        try:
            row = await _fetch_mapping(
                conn,
                f"""
                SELECT
                    count(*)::bigint AS row_count,
                    count(DISTINCT source_id)::bigint AS source_count
                    {", count(*) FILTER (WHERE npi IS NOT NULL)::bigint AS npi_count" if columns["npi"] else ""}
                    {", count(*) FILTER (WHERE address_key IS NOT NULL)::bigint AS address_key_count" if columns["address_key"] else ""}
                    {", count(*) FILTER (WHERE telephone_number IS NOT NULL AND BTRIM(telephone_number) <> '')::bigint AS phone_count" if columns["telephone_number"] else ""}
                    {", count(*) FILTER (WHERE jsonb_array_length(COALESCE(network_refs::jsonb, '[]'::jsonb)) > 0)::bigint AS network_ref_row_count" if columns["network_refs"] else ""}
                  FROM {_qt(schema, table)}
                """,
            )
        except asyncpg.exceptions.QueryCanceledError as exc:
            row = await _estimated_resource_summary_row(
                conn,
                schema,
                table,
                columns=columns,
            )
            row["exact_error"] = str(exc)
        row["available"] = True
        row["columns"] = columns
        row_count = _int(row.get("row_count"))
        if columns["address_key"]:
            row["address_key_pct"] = _pct(_int(row.get("address_key_count")), row_count)
        summary[table] = row
    return summary


async def _unified_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    fast_probe: bool = False,
) -> dict[str, Any]:
    if await _relation_exists(conn, schema, "provider_directory_address_overlay"):
        if fast_probe:
            row = await _fetch_mapping(
                conn,
                f"""
                SELECT
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         LIMIT 1
                    ) AS has_provider_directory_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE address_key IS NOT NULL
                         LIMIT 1
                    ) AS has_provider_directory_keyed_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE NULLIF(BTRIM(COALESCE(phone_number, telephone_number)), '') IS NOT NULL
                         LIMIT 1
                    ) AS has_provider_directory_phone_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE address_key IS NULL
                         LIMIT 1
                    ) AS has_provider_directory_null_key_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE NULLIF(BTRIM(source_record_id), '') IS NOT NULL
                         LIMIT 1
                    ) AS has_provider_directory_source_record_id_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE country_code = '001'
                         LIMIT 1
                    ) AS has_provider_directory_country_001_rows,
                    EXISTS (
                        SELECT 1
                          FROM {_qt(schema, "provider_directory_address_overlay")}
                         WHERE country_code = 'US'
                         LIMIT 1
                    ) AS has_provider_directory_country_us_rows
                """,
            )
            has_rows = bool(row.get("has_provider_directory_rows"))
            has_keyed_rows = bool(row.get("has_provider_directory_keyed_rows"))
            has_source_record_ids = bool(row.get("has_provider_directory_source_record_id_rows"))
            return {
                "available": True,
                "summary_source": "provider_directory_address_overlay_fast_probe",
                "fast_probe": True,
                "counts_are_lower_bounds": True,
                "provider_directory_rows": 1 if has_rows else 0,
                "provider_directory_keyed_rows": 1 if has_keyed_rows else 0,
                "provider_directory_phone_rows": 1 if row.get("has_provider_directory_phone_rows") else 0,
                "provider_directory_null_key_rows": 1 if row.get("has_provider_directory_null_key_rows") else 0,
                "provider_directory_source_record_id_rows": 1 if has_source_record_ids else 0,
                "provider_directory_country_001_rows": 1 if row.get("has_provider_directory_country_001_rows") else 0,
                "provider_directory_country_us_rows": 1 if row.get("has_provider_directory_country_us_rows") else 0,
                "provider_directory_keyed_pct": None,
                "provider_directory_phone_pct": None,
                "provider_directory_source_record_id_pct": 100.0 if has_rows and has_source_record_ids else None,
            }
        row = await _fetch_mapping(
            conn,
            f"""
            SELECT
                count(*)::bigint AS provider_directory_rows,
                count(*) FILTER (WHERE address_key IS NOT NULL)::bigint AS provider_directory_keyed_rows,
                count(*) FILTER (
                    WHERE NULLIF(BTRIM(COALESCE(phone_number, telephone_number)), '') IS NOT NULL
                )::bigint AS provider_directory_phone_rows,
                count(*) FILTER (WHERE address_key IS NULL)::bigint AS provider_directory_null_key_rows,
                count(*) FILTER (WHERE NULLIF(BTRIM(source_record_id), '') IS NOT NULL)::bigint
                    AS provider_directory_source_record_id_rows,
                count(*) FILTER (WHERE country_code = '001')::bigint AS provider_directory_country_001_rows,
                count(*) FILTER (WHERE country_code = 'US')::bigint AS provider_directory_country_us_rows
              FROM {_qt(schema, "provider_directory_address_overlay")}
            """,
        )
        total = _int(row.get("provider_directory_rows"))
        row["available"] = True
        row["summary_source"] = "provider_directory_address_overlay"
        row["provider_directory_keyed_pct"] = _pct(_int(row.get("provider_directory_keyed_rows")), total)
        row["provider_directory_phone_pct"] = _pct(_int(row.get("provider_directory_phone_rows")), total)
        row["provider_directory_source_record_id_pct"] = _pct(
            _int(row.get("provider_directory_source_record_id_rows")),
            total,
        )
        return row
    if not await _relation_exists(conn, schema, "entity_address_unified"):
        return {"available": False}
    if fast_probe:
        row = await _fetch_mapping(
            conn,
            f"""
            SELECT
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                     LIMIT 1
                ) AS has_provider_directory_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND address_key IS NOT NULL
                     LIMIT 1
                ) AS has_provider_directory_keyed_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND telephone_number IS NOT NULL
                     LIMIT 1
                ) AS has_provider_directory_phone_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND address_key IS NULL
                     LIMIT 1
                ) AS has_provider_directory_null_key_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND cardinality(COALESCE(source_record_ids, ARRAY[]::varchar[])) > 0
                     LIMIT 1
                ) AS has_provider_directory_source_record_id_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND country_code = '001'
                     LIMIT 1
                ) AS has_provider_directory_country_001_rows,
                EXISTS (
                    SELECT 1
                      FROM {_qt(schema, "entity_address_unified")}
                     WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                       AND country_code = 'US'
                     LIMIT 1
                ) AS has_provider_directory_country_us_rows
            """,
        )
        has_rows = bool(row.get("has_provider_directory_rows"))
        has_keyed_rows = bool(row.get("has_provider_directory_keyed_rows"))
        has_source_record_ids = bool(row.get("has_provider_directory_source_record_id_rows"))
        return {
            "available": True,
            "summary_source": "entity_address_unified_fast_probe",
            "fast_probe": True,
            "counts_are_lower_bounds": True,
            "provider_directory_rows": 1 if has_rows else 0,
            "provider_directory_keyed_rows": 1 if has_keyed_rows else 0,
            "provider_directory_phone_rows": 1 if row.get("has_provider_directory_phone_rows") else 0,
            "provider_directory_null_key_rows": 1 if row.get("has_provider_directory_null_key_rows") else 0,
            "provider_directory_source_record_id_rows": 1 if has_source_record_ids else 0,
            "provider_directory_country_001_rows": 1 if row.get("has_provider_directory_country_001_rows") else 0,
            "provider_directory_country_us_rows": 1 if row.get("has_provider_directory_country_us_rows") else 0,
            "provider_directory_keyed_pct": None,
            "provider_directory_phone_pct": None,
            "provider_directory_source_record_id_pct": 100.0 if has_rows and has_source_record_ids else None,
        }
    row = await _fetch_mapping(
        conn,
        f"""
        SELECT
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
            )::bigint AS provider_directory_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND address_key IS NOT NULL
            )::bigint AS provider_directory_keyed_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND telephone_number IS NOT NULL
            )::bigint AS provider_directory_phone_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND address_key IS NULL
            )::bigint AS provider_directory_null_key_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND cardinality(COALESCE(source_record_ids, ARRAY[]::varchar[])) > 0
            )::bigint AS provider_directory_source_record_id_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND country_code = '001'
            )::bigint AS provider_directory_country_001_rows,
            count(*) FILTER (
                WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]
                  AND country_code = 'US'
            )::bigint AS provider_directory_country_us_rows
          FROM {_qt(schema, "entity_address_unified")}
        """,
    )
    total = _int(row.get("provider_directory_rows"))
    row["available"] = True
    row["provider_directory_keyed_pct"] = _pct(_int(row.get("provider_directory_keyed_rows")), total)
    row["provider_directory_phone_pct"] = _pct(_int(row.get("provider_directory_phone_rows")), total)
    row["provider_directory_source_record_id_pct"] = _pct(
        _int(row.get("provider_directory_source_record_id_rows")),
        total,
    )
    return row


async def _source_resource_coverage_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
    include_unified: bool,
) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "samples": []}

    resource_tables = [
        table
        for table in PROVIDER_DIRECTORY_RESOURCE_TABLES
        if await _relation_exists(conn, schema, table)
    ]
    metadata_summary = await _resource_import_metadata_summary(conn, schema)
    if metadata_summary:
        resource_source_union = f"""
            SELECT DISTINCT src.source_id::varchar AS source_id
              FROM {_qt(schema, "provider_directory_source")} AS src
              CROSS JOIN LATERAL jsonb_each(
                    COALESCE(
                        src.metadata_json::jsonb #> '{{last_resource_import,resources}}',
                        '{{}}'::jsonb
                    )
              ) AS resource(key, value)
             WHERE COALESCE(NULLIF(resource.value->>'rows_written', ''), '0')::bigint > 0
        """
    else:
        resource_source_union = (
            " UNION ".join(
                f"SELECT DISTINCT source_id::varchar AS source_id FROM {_qt(schema, table)}"
                for table in resource_tables
            )
            if resource_tables
            else "SELECT NULL::varchar AS source_id WHERE false"
        )

    has_location = await _relation_exists(conn, schema, "provider_directory_location")
    has_location_address_key = has_location and await _column_exists(
        conn,
        schema,
        "provider_directory_location",
        "address_key",
    )
    has_organization = await _relation_exists(conn, schema, "provider_directory_organization")
    has_organization_address_json = has_organization and await _column_exists(
        conn,
        schema,
        "provider_directory_organization",
        "address_json",
    )
    has_healthcare_service = await _relation_exists(
        conn,
        schema,
        "provider_directory_healthcare_service",
    )
    has_unified = (
        include_unified
        and await _relation_exists(conn, schema, "entity_address_unified")
        and await _column_exists(conn, schema, "entity_address_unified", "address_sources")
        and await _column_exists(conn, schema, "entity_address_unified", "source_record_ids")
    )
    has_unified_address_key = has_unified and await _column_exists(
        conn,
        schema,
        "entity_address_unified",
        "address_key",
    )
    has_unified_phone = has_unified and await _column_exists(
        conn,
        schema,
        "entity_address_unified",
        "telephone_number",
    )
    if metadata_summary:
        row = await _fetch_mapping(
            conn,
            f"""
            WITH resource_diagnostics AS (
                SELECT src.source_id,
                       resource.key AS resource_type,
                       COALESCE(NULLIF(resource.value->>'rows_written', ''), '0')::bigint AS rows_written
                  FROM {_qt(schema, "provider_directory_source")} AS src
                  CROSS JOIN LATERAL jsonb_each(
                        COALESCE(
                            src.metadata_json::jsonb #> '{{last_resource_import,resources}}',
                            '{{}}'::jsonb
                        )
                  ) AS resource(key, value)
            ),
            source_context AS (
                SELECT src.source_id,
                       bool_or(resource_diagnostics.rows_written > 0) AS has_resource_rows,
                       sum(resource_diagnostics.rows_written) FILTER (
                           WHERE resource_diagnostics.resource_type = 'Location'
                       ) AS location_rows
                  FROM {_qt(schema, "provider_directory_source")} AS src
                  LEFT JOIN resource_diagnostics
                    ON resource_diagnostics.source_id = src.source_id
                 GROUP BY src.source_id
            )
            SELECT count(*)::bigint AS source_count,
                   count(*) FILTER (WHERE COALESCE(has_resource_rows, false))::bigint
                       AS sources_with_resource_rows,
                   count(*) FILTER (WHERE NOT COALESCE(has_resource_rows, false))::bigint
                       AS catalog_only_source_count,
                   count(*) FILTER (WHERE COALESCE(location_rows, 0) > 0)::bigint
                       AS sources_with_location_rows,
                   sum(COALESCE(location_rows, 0))::bigint AS location_rows
              FROM source_context
            """,
        )
        sample_catalog_only = await conn.fetch(
            f"""
            WITH resource_diagnostics AS (
                SELECT src.source_id,
                       COALESCE(NULLIF(resource.value->>'rows_written', ''), '0')::bigint AS rows_written
                  FROM {_qt(schema, "provider_directory_source")} AS src
                  CROSS JOIN LATERAL jsonb_each(
                        COALESCE(
                            src.metadata_json::jsonb #> '{{last_resource_import,resources}}',
                            '{{}}'::jsonb
                        )
                  ) AS resource(key, value)
            ),
            resource_sources AS (
                SELECT source_id
                  FROM resource_diagnostics
                 GROUP BY source_id
                HAVING bool_or(rows_written > 0)
            )
            SELECT src.source_id,
                   src.org_name,
                   src.plan_name,
                   src.canonical_api_base,
                   src.last_probe_status,
                   src.last_validated_status,
                   src.auth_type
              FROM {_qt(schema, "provider_directory_source")} AS src
              LEFT JOIN resource_sources
                ON resource_sources.source_id = src.source_id
             WHERE resource_sources.source_id IS NULL
             ORDER BY lower(src.org_name), lower(coalesce(src.plan_name, '')), src.source_id
             LIMIT $1
            """,
            sample_limit,
        )
        row["available"] = True
        row["summary_source"] = "provider_directory_source.metadata_json.last_resource_import"
        row["projection_counts_exact"] = False
        source_count = _int(row.get("source_count"))
        row["resource_source_pct"] = _pct(_int(row.get("sources_with_resource_rows")), source_count)
        row["location_source_pct"] = _pct(_int(row.get("sources_with_location_rows")), source_count)
        row["unified_available"] = bool(has_unified)
        row["sources_with_keyed_location_rows"] = None
        row["sources_with_location_rows_without_keys"] = None
        row["keyed_location_rows"] = None
        row["sources_with_organization_address_rows"] = None
        row["sources_with_valid_npi_organization_address_rows"] = None
        row["sources_with_valid_npi_organization_address_rows_without_unified_rows"] = None
        row["organization_address_rows"] = None
        row["valid_npi_organization_address_rows"] = None
        row["sources_with_unified_rows"] = None
        row["sources_with_keyed_unified_rows"] = None
        row["sources_with_phone_unified_rows"] = None
        row["sources_with_location_rows_without_unified_rows"] = None
        row["unified_rows"] = None
        row["keyed_unified_rows"] = None
        row["phone_unified_rows"] = None
        row["keyed_location_source_pct"] = None
        row["catalog_only_samples"] = [dict(item) for item in sample_catalog_only]
        row["location_without_unified_samples"] = []
        row["organization_address_without_unified_samples"] = []
        return row

    location_source_rows = (
        f"""
        SELECT source_id::varchar AS source_id,
               count(*)::bigint AS location_rows,
               count(*) FILTER (
                   WHERE {"address_key IS NOT NULL" if has_location_address_key else "false"}
               )::bigint AS keyed_location_rows
          FROM {_qt(schema, "provider_directory_location")}
         GROUP BY source_id
        """
        if has_location
        else "SELECT NULL::varchar AS source_id, 0::bigint AS location_rows, 0::bigint AS keyed_location_rows WHERE false"
    )
    organization_address_source_rows = (
        f"""
        SELECT organization.source_id::varchar AS source_id,
               sum(jsonb_array_length(COALESCE(organization.address_json::jsonb, '[]'::jsonb)))::bigint
                   AS organization_address_rows,
               sum(jsonb_array_length(COALESCE(organization.address_json::jsonb, '[]'::jsonb))) FILTER (
                   WHERE organization.npi BETWEEN 1000000000 AND 9999999999
                     AND organization.active IS DISTINCT FROM false
               )::bigint AS valid_npi_organization_address_rows
          FROM {_qt(schema, "provider_directory_organization")} AS organization
         WHERE jsonb_array_length(COALESCE(organization.address_json::jsonb, '[]'::jsonb)) > 0
      GROUP BY organization.source_id
        """
        if has_organization_address_json
        else "SELECT NULL::varchar AS source_id, 0::bigint AS organization_address_rows, 0::bigint AS valid_npi_organization_address_rows WHERE false"
    )
    unified_source_rows = (
        f"""
        SELECT split_part(pd_rid.rid, ':', 3)::varchar AS source_id,
               count(*)::bigint AS unified_rows,
               count(*) FILTER (
                   WHERE {"unified.address_key IS NOT NULL" if has_unified_address_key else "false"}
               )::bigint AS keyed_unified_rows,
               count(*) FILTER (
                   WHERE {"unified.telephone_number IS NOT NULL AND BTRIM(unified.telephone_number) <> ''" if has_unified_phone else "false"}
               )::bigint AS phone_unified_rows
          FROM {_qt(schema, "entity_address_unified")} AS unified
          CROSS JOIN LATERAL unnest(COALESCE(unified.source_record_ids, ARRAY[]::varchar[])) AS pd_rid(rid)
         WHERE unified.address_sources @> ARRAY['provider_directory_fhir']::varchar[]
           AND pd_rid.rid LIKE 'provider_directory_fhir:%'
           AND NULLIF(split_part(pd_rid.rid, ':', 3), '') IS NOT NULL
         GROUP BY split_part(pd_rid.rid, ':', 3)
        """
        if has_unified
        else "SELECT NULL::varchar AS source_id, 0::bigint AS unified_rows, 0::bigint AS keyed_unified_rows, 0::bigint AS phone_unified_rows WHERE false"
    )
    cte_sql = f"""
        WITH resource_sources AS ({resource_source_union}),
        location_source_rows AS ({location_source_rows}),
        organization_address_source_rows AS ({organization_address_source_rows}),
        unified_source_rows AS ({unified_source_rows}),
        source_context AS (
            SELECT src.source_id,
                   src.org_name,
                   src.plan_name,
                   src.canonical_api_base,
                   src.last_probe_status,
                   src.last_validated_status,
                   src.auth_type,
                   resource_sources.source_id IS NOT NULL AS has_resource_rows,
                   COALESCE(location_source_rows.location_rows, 0)::bigint AS location_rows,
                   COALESCE(location_source_rows.keyed_location_rows, 0)::bigint AS keyed_location_rows,
                   COALESCE(organization_address_source_rows.organization_address_rows, 0)::bigint
                       AS organization_address_rows,
                   COALESCE(organization_address_source_rows.valid_npi_organization_address_rows, 0)::bigint
                       AS valid_npi_organization_address_rows,
                   COALESCE(unified_source_rows.unified_rows, 0)::bigint AS unified_rows,
                   COALESCE(unified_source_rows.keyed_unified_rows, 0)::bigint AS keyed_unified_rows,
                   COALESCE(unified_source_rows.phone_unified_rows, 0)::bigint AS phone_unified_rows
              FROM {_qt(schema, "provider_directory_source")} AS src
              LEFT JOIN resource_sources
                ON resource_sources.source_id = src.source_id
              LEFT JOIN location_source_rows
                ON location_source_rows.source_id = src.source_id
              LEFT JOIN organization_address_source_rows
                ON organization_address_source_rows.source_id = src.source_id
              LEFT JOIN unified_source_rows
                ON unified_source_rows.source_id = src.source_id
        )
    """
    row = await _fetch_mapping(
        conn,
        f"""
        {cte_sql}
        SELECT count(*)::bigint AS source_count,
               count(*) FILTER (WHERE has_resource_rows)::bigint AS sources_with_resource_rows,
               count(*) FILTER (WHERE NOT has_resource_rows)::bigint AS catalog_only_source_count,
               count(*) FILTER (WHERE location_rows > 0)::bigint AS sources_with_location_rows,
               count(*) FILTER (WHERE keyed_location_rows > 0)::bigint AS sources_with_keyed_location_rows,
               count(*) FILTER (WHERE location_rows > 0 AND keyed_location_rows = 0)::bigint
                   AS sources_with_location_rows_without_keys,
               sum(location_rows)::bigint AS location_rows,
               sum(keyed_location_rows)::bigint AS keyed_location_rows,
               count(*) FILTER (WHERE organization_address_rows > 0)::bigint
                   AS sources_with_organization_address_rows,
               count(*) FILTER (WHERE valid_npi_organization_address_rows > 0)::bigint
                   AS sources_with_valid_npi_organization_address_rows,
               count(*) FILTER (WHERE valid_npi_organization_address_rows > 0 AND unified_rows = 0)::bigint
                   AS sources_with_valid_npi_organization_address_rows_without_unified_rows,
               sum(organization_address_rows)::bigint AS organization_address_rows,
               sum(valid_npi_organization_address_rows)::bigint AS valid_npi_organization_address_rows,
               count(*) FILTER (WHERE unified_rows > 0)::bigint AS sources_with_unified_rows,
               count(*) FILTER (WHERE keyed_unified_rows > 0)::bigint AS sources_with_keyed_unified_rows,
               count(*) FILTER (WHERE phone_unified_rows > 0)::bigint AS sources_with_phone_unified_rows,
               count(*) FILTER (WHERE location_rows > 0 AND unified_rows = 0)::bigint
                   AS sources_with_location_rows_without_unified_rows,
               sum(unified_rows)::bigint AS unified_rows,
               sum(keyed_unified_rows)::bigint AS keyed_unified_rows,
               sum(phone_unified_rows)::bigint AS phone_unified_rows
          FROM source_context
        """,
    )
    sample_catalog_only = await conn.fetch(
        f"""
        {cte_sql}
        SELECT source_id,
               org_name,
               plan_name,
               canonical_api_base,
               last_probe_status,
               last_validated_status,
               auth_type
          FROM source_context
         WHERE NOT has_resource_rows
         ORDER BY lower(org_name), lower(coalesce(plan_name, '')), source_id
         LIMIT $1
        """,
        sample_limit,
    )
    healthcare_service_projection_columns = (
        """
                   COALESCE(role_healthcare_service_links.role_healthcare_service_location_refs, 0)::bigint
                       AS role_healthcare_service_location_refs,
                   COALESCE(role_healthcare_service_links.role_healthcare_service_valid_npi_refs, 0)::bigint
                       AS role_healthcare_service_valid_npi_refs,
                   COALESCE(role_healthcare_service_links.role_healthcare_service_matching_location_refs, 0)::bigint
                       AS role_healthcare_service_matching_location_refs,
                   COALESCE(role_healthcare_service_links.role_healthcare_service_projectable_location_refs, 0)::bigint
                       AS role_healthcare_service_projectable_location_refs,
                   COALESCE(affiliation_healthcare_service_links.affiliation_healthcare_service_location_refs, 0)::bigint
                       AS affiliation_healthcare_service_location_refs,
                   COALESCE(affiliation_healthcare_service_links.affiliation_healthcare_service_valid_npi_refs, 0)::bigint
                       AS affiliation_healthcare_service_valid_npi_refs,
                   COALESCE(affiliation_healthcare_service_links.affiliation_healthcare_service_matching_location_refs, 0)::bigint
                       AS affiliation_healthcare_service_matching_location_refs,
                   COALESCE(affiliation_healthcare_service_links.affiliation_healthcare_service_projectable_location_refs, 0)::bigint
                       AS affiliation_healthcare_service_projectable_location_refs"""
        if has_healthcare_service
        else """
                   0::bigint AS role_healthcare_service_location_refs,
                   0::bigint AS role_healthcare_service_valid_npi_refs,
                   0::bigint AS role_healthcare_service_matching_location_refs,
                   0::bigint AS role_healthcare_service_projectable_location_refs,
                   0::bigint AS affiliation_healthcare_service_location_refs,
                   0::bigint AS affiliation_healthcare_service_valid_npi_refs,
                   0::bigint AS affiliation_healthcare_service_matching_location_refs,
                   0::bigint AS affiliation_healthcare_service_projectable_location_refs"""
    )
    healthcare_service_projection_joins = (
        f"""
              LEFT JOIN LATERAL (
                  SELECT count(*)::bigint AS role_healthcare_service_location_refs,
                         count(*) FILTER (
                             WHERE practitioner.npi BETWEEN 1000000000 AND 9999999999
                         )::bigint AS role_healthcare_service_valid_npi_refs,
                         count(*) FILTER (
                             WHERE loc.resource_id IS NOT NULL
                         )::bigint AS role_healthcare_service_matching_location_refs,
                         count(*) FILTER (
                             WHERE practitioner.npi BETWEEN 1000000000 AND 9999999999
                               AND practitioner.active IS DISTINCT FROM false
                               AND role.active IS DISTINCT FROM false
                               AND healthcare_service.active IS DISTINCT FROM false
                               AND loc.resource_id IS NOT NULL
                               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                         )::bigint AS role_healthcare_service_projectable_location_refs
                    FROM {_qt(schema, "provider_directory_practitioner_role")} AS role
                    LEFT JOIN {_qt(schema, "provider_directory_practitioner")} AS practitioner
                      ON practitioner.source_id = role.source_id
                     AND practitioner.resource_id = NULLIF(
                            regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''),
                            ''
                         )
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(role.healthcare_service_refs::jsonb, '[]'::jsonb)
                    ) AS service_ref(value) ON TRUE
                    JOIN {_qt(schema, "provider_directory_healthcare_service")} AS healthcare_service
                      ON healthcare_service.source_id = role.source_id
                     AND healthcare_service.resource_id = NULLIF(regexp_replace(service_ref.value, '^.*/', ''), '')
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)
                    ) AS location_ref(value) ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_location")} AS loc
                      ON loc.source_id = role.source_id
                     AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
                   WHERE role.source_id = sample_sources.source_id
              ) AS role_healthcare_service_links ON TRUE
              LEFT JOIN LATERAL (
                  SELECT count(*)::bigint AS affiliation_healthcare_service_location_refs,
                         count(*) FILTER (
                             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
                         )::bigint AS affiliation_healthcare_service_valid_npi_refs,
                         count(*) FILTER (
                             WHERE loc.resource_id IS NOT NULL
                         )::bigint AS affiliation_healthcare_service_matching_location_refs,
                         count(*) FILTER (
                             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
                               AND organization.active IS DISTINCT FROM false
                               AND affiliation.active IS DISTINCT FROM false
                               AND healthcare_service.active IS DISTINCT FROM false
                               AND loc.resource_id IS NOT NULL
                               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                         )::bigint AS affiliation_healthcare_service_projectable_location_refs
                    FROM {_qt(schema, "provider_directory_organization_affiliation")} AS affiliation
                    JOIN LATERAL (
                        SELECT DISTINCT normalized_ref AS resource_id
                          FROM (
                              VALUES
                                  (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                                  (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                          ) AS refs(normalized_ref)
                         WHERE normalized_ref IS NOT NULL
                    ) AS organization_ref ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_organization")} AS organization
                      ON organization.source_id = affiliation.source_id
                     AND organization.resource_id = organization_ref.resource_id
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(affiliation.healthcare_service_refs::jsonb, '[]'::jsonb)
                    ) AS service_ref(value) ON TRUE
                    JOIN {_qt(schema, "provider_directory_healthcare_service")} AS healthcare_service
                      ON healthcare_service.source_id = affiliation.source_id
                     AND healthcare_service.resource_id = NULLIF(regexp_replace(service_ref.value, '^.*/', ''), '')
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(healthcare_service.location_refs::jsonb, '[]'::jsonb)
                    ) AS location_ref(value) ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_location")} AS loc
                      ON loc.source_id = affiliation.source_id
                     AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
                   WHERE affiliation.source_id = sample_sources.source_id
              ) AS affiliation_healthcare_service_links ON TRUE"""
        if has_healthcare_service
        else ""
    )
    sample_location_without_unified = (
        await conn.fetch(
            f"""
            {cte_sql}
            , sample_sources AS (
                SELECT source_id,
                       org_name,
                       plan_name,
                       canonical_api_base,
                       last_probe_status,
                       auth_type,
                       location_rows,
                       keyed_location_rows,
                       organization_address_rows,
                       valid_npi_organization_address_rows
                  FROM source_context
                 WHERE location_rows > 0
                   AND unified_rows = 0
                 ORDER BY location_rows DESC, lower(org_name), lower(coalesce(plan_name, '')), source_id
                 LIMIT $1
            )
            SELECT sample_sources.*,
                   COALESCE(role_links.role_location_refs, 0)::bigint AS role_location_refs,
                   COALESCE(role_links.role_valid_npi_refs, 0)::bigint AS role_valid_npi_refs,
                   COALESCE(role_links.role_matching_location_refs, 0)::bigint AS role_matching_location_refs,
                   COALESCE(role_links.role_projectable_location_refs, 0)::bigint AS role_projectable_location_refs,
                   COALESCE(affiliation_links.affiliation_location_refs, 0)::bigint AS affiliation_location_refs,
                   COALESCE(affiliation_links.affiliation_valid_npi_refs, 0)::bigint AS affiliation_valid_npi_refs,
                   COALESCE(affiliation_links.affiliation_matching_location_refs, 0)::bigint
                       AS affiliation_matching_location_refs,
                   COALESCE(affiliation_links.affiliation_projectable_location_refs, 0)::bigint
                       AS affiliation_projectable_location_refs,
                   {healthcare_service_projection_columns}
              FROM sample_sources
              LEFT JOIN LATERAL (
                  SELECT count(*)::bigint AS role_location_refs,
                         count(*) FILTER (
                             WHERE practitioner.npi BETWEEN 1000000000 AND 9999999999
                         )::bigint AS role_valid_npi_refs,
                         count(*) FILTER (
                             WHERE loc.resource_id IS NOT NULL
                         )::bigint AS role_matching_location_refs,
                         count(*) FILTER (
                             WHERE practitioner.npi BETWEEN 1000000000 AND 9999999999
                               AND practitioner.active IS DISTINCT FROM false
                               AND role.active IS DISTINCT FROM false
                               AND loc.resource_id IS NOT NULL
                               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                         )::bigint AS role_projectable_location_refs
                    FROM {_qt(schema, "provider_directory_practitioner_role")} AS role
                    LEFT JOIN {_qt(schema, "provider_directory_practitioner")} AS practitioner
                      ON practitioner.source_id = role.source_id
                     AND practitioner.resource_id = NULLIF(
                            regexp_replace(COALESCE(role.practitioner_ref, ''), '^.*/', ''),
                            ''
                         )
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(role.location_refs::jsonb, '[]'::jsonb)
                    ) AS location_ref(value) ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_location")} AS loc
                      ON loc.source_id = role.source_id
                     AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
                   WHERE role.source_id = sample_sources.source_id
              ) AS role_links ON TRUE
              LEFT JOIN LATERAL (
                  SELECT count(*)::bigint AS affiliation_location_refs,
                         count(*) FILTER (
                             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
                         )::bigint AS affiliation_valid_npi_refs,
                         count(*) FILTER (
                             WHERE loc.resource_id IS NOT NULL
                         )::bigint AS affiliation_matching_location_refs,
                         count(*) FILTER (
                             WHERE organization.npi BETWEEN 1000000000 AND 9999999999
                               AND organization.active IS DISTINCT FROM false
                               AND affiliation.active IS DISTINCT FROM false
                               AND loc.resource_id IS NOT NULL
                               AND (loc.status IS NULL OR lower(loc.status) <> 'inactive')
                         )::bigint AS affiliation_projectable_location_refs
                    FROM {_qt(schema, "provider_directory_organization_affiliation")} AS affiliation
                    JOIN LATERAL (
                        SELECT DISTINCT normalized_ref AS resource_id
                          FROM (
                              VALUES
                                  (NULLIF(regexp_replace(COALESCE(affiliation.organization_ref, ''), '^.*/', ''), '')),
                                  (NULLIF(regexp_replace(COALESCE(affiliation.participating_organization_ref, ''), '^.*/', ''), ''))
                          ) AS refs(normalized_ref)
                         WHERE normalized_ref IS NOT NULL
                    ) AS organization_ref ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_organization")} AS organization
                      ON organization.source_id = affiliation.source_id
                     AND organization.resource_id = organization_ref.resource_id
                    JOIN LATERAL jsonb_array_elements_text(
                         COALESCE(affiliation.location_refs::jsonb, '[]'::jsonb)
                    ) AS location_ref(value) ON TRUE
                    LEFT JOIN {_qt(schema, "provider_directory_location")} AS loc
                      ON loc.source_id = affiliation.source_id
                     AND loc.resource_id = NULLIF(regexp_replace(location_ref.value, '^.*/', ''), '')
                   WHERE affiliation.source_id = sample_sources.source_id
              ) AS affiliation_links ON TRUE
              {healthcare_service_projection_joins}
            """,
            sample_limit,
        )
        if has_unified
        else []
    )
    sample_organization_address_without_unified = (
        await conn.fetch(
            f"""
            {cte_sql}
            SELECT source_id,
                   org_name,
                   plan_name,
                   canonical_api_base,
                   last_probe_status,
                   auth_type,
                   organization_address_rows,
                   valid_npi_organization_address_rows
              FROM source_context
             WHERE valid_npi_organization_address_rows > 0
               AND unified_rows = 0
             ORDER BY valid_npi_organization_address_rows DESC,
                      lower(org_name),
                      lower(coalesce(plan_name, '')),
                      source_id
             LIMIT $1
            """,
            sample_limit,
        )
        if has_unified and has_organization_address_json
        else []
    )
    source_count = _int(row.get("source_count"))
    row["available"] = True
    row["unified_available"] = has_unified
    row["resource_source_pct"] = _pct(_int(row.get("sources_with_resource_rows")), source_count)
    row["location_source_pct"] = _pct(_int(row.get("sources_with_location_rows")), source_count)
    row["keyed_location_source_pct"] = _pct(
        _int(row.get("sources_with_keyed_location_rows")),
        _int(row.get("sources_with_location_rows")),
    )
    row["unified_source_pct"] = _pct(_int(row.get("sources_with_unified_rows")), source_count)
    row["keyed_unified_source_pct"] = _pct(
        _int(row.get("sources_with_keyed_unified_rows")),
        _int(row.get("sources_with_unified_rows")),
    )
    row["phone_unified_source_pct"] = _pct(
        _int(row.get("sources_with_phone_unified_rows")),
        _int(row.get("sources_with_unified_rows")),
    )
    row["catalog_only_samples"] = [dict(item) for item in sample_catalog_only]
    row["location_without_unified_samples"] = [dict(item) for item in sample_location_without_unified]
    for item in row["location_without_unified_samples"]:
        item["projection_gap_reason"] = _provider_directory_projection_gap_reason(item)
    row["organization_address_without_unified_samples"] = [
        dict(item) for item in sample_organization_address_without_unified
    ]
    return row


async def _practitioner_role_reimport_gap_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    required_tables = (
        "provider_directory_source",
        "provider_directory_practitioner",
        "provider_directory_location",
        "provider_directory_practitioner_role",
    )
    for table_name in required_tables:
        if not await _relation_exists(conn, schema, table_name):
            return {"available": False, "missing_table": table_name, "samples": []}

    overlay_source_rows = (
        f"""
        SELECT source_id::varchar AS source_id,
               count(*)::bigint AS overlay_rows
          FROM {_qt(schema, "provider_directory_address_overlay")}
         GROUP BY source_id
        """
        if await _relation_exists(conn, schema, "provider_directory_address_overlay")
        else "SELECT NULL::varchar AS source_id, 0::bigint AS overlay_rows WHERE false"
    )
    cte_sql = f"""
        WITH practitioner_counts AS (
            SELECT source_id::varchar AS source_id,
                   count(*)::bigint AS practitioner_rows,
                   count(*) FILTER (WHERE npi BETWEEN 1000000000 AND 9999999999)::bigint
                       AS valid_npi_practitioner_rows
              FROM {_qt(schema, "provider_directory_practitioner")}
             GROUP BY source_id
        ),
        location_counts AS (
            SELECT source_id::varchar AS source_id,
                   count(*)::bigint AS location_rows,
                   count(*) FILTER (
                       WHERE NULLIF(BTRIM(first_line), '') IS NOT NULL
                         AND NULLIF(BTRIM(postal_code), '') IS NOT NULL
                   )::bigint AS street_zip_location_rows
              FROM {_qt(schema, "provider_directory_location")}
             GROUP BY source_id
        ),
        role_counts AS (
            SELECT source_id::varchar AS source_id,
                   count(*)::bigint AS practitioner_role_rows,
                   count(*) FILTER (
                       WHERE jsonb_array_length(COALESCE(location_refs::jsonb, '[]'::jsonb)) > 0
                   )::bigint AS practitioner_role_location_ref_rows
              FROM {_qt(schema, "provider_directory_practitioner_role")}
             GROUP BY source_id
        ),
        overlay_source_rows AS ({overlay_source_rows}),
        source_context AS (
            SELECT src.source_id,
                   src.org_name,
                   src.plan_name,
                   src.canonical_api_base,
                   src.endpoint_practitioner_role,
                   src.last_probe_status,
                   src.auth_type,
                   COALESCE(practitioner_counts.practitioner_rows, 0)::bigint AS practitioner_rows,
                   COALESCE(practitioner_counts.valid_npi_practitioner_rows, 0)::bigint
                       AS valid_npi_practitioner_rows,
                   COALESCE(location_counts.location_rows, 0)::bigint AS location_rows,
                   COALESCE(location_counts.street_zip_location_rows, 0)::bigint AS street_zip_location_rows,
                   COALESCE(role_counts.practitioner_role_rows, 0)::bigint AS practitioner_role_rows,
                   COALESCE(role_counts.practitioner_role_location_ref_rows, 0)::bigint
                       AS practitioner_role_location_ref_rows,
                   COALESCE(overlay_source_rows.overlay_rows, 0)::bigint AS overlay_rows
              FROM {_qt(schema, "provider_directory_source")} AS src
              LEFT JOIN practitioner_counts
                ON practitioner_counts.source_id = src.source_id
              LEFT JOIN location_counts
                ON location_counts.source_id = src.source_id
              LEFT JOIN role_counts
                ON role_counts.source_id = src.source_id
              LEFT JOIN overlay_source_rows
                ON overlay_source_rows.source_id = src.source_id
        )
    """
    row = await _fetch_mapping(
        conn,
        f"""
        {cte_sql}
        SELECT count(*)::bigint AS source_count,
               count(*) FILTER (
                   WHERE NULLIF(BTRIM(endpoint_practitioner_role), '') IS NOT NULL
               )::bigint AS sources_with_practitioner_role_endpoint,
               count(*) FILTER (
                   WHERE NULLIF(BTRIM(endpoint_practitioner_role), '') IS NOT NULL
                     AND valid_npi_practitioner_rows > 0
                     AND street_zip_location_rows > 0
                     AND practitioner_role_rows = 0
               )::bigint AS practitioner_role_reimport_gap_source_count,
               count(*) FILTER (
                   WHERE practitioner_role_rows > 0
                     AND practitioner_role_location_ref_rows = 0
               )::bigint AS practitioner_role_without_location_ref_source_count,
               count(*) FILTER (
                   WHERE practitioner_role_location_ref_rows > 0
                     AND overlay_rows = 0
               )::bigint AS practitioner_role_projection_gap_source_count
          FROM source_context
        """,
    )
    samples = await conn.fetch(
        f"""
        {cte_sql}
        SELECT source_id,
               org_name,
               plan_name,
               canonical_api_base,
               endpoint_practitioner_role,
               last_probe_status,
               auth_type,
               practitioner_rows,
               valid_npi_practitioner_rows,
               location_rows,
               street_zip_location_rows,
               practitioner_role_rows,
               practitioner_role_location_ref_rows,
               overlay_rows
          FROM source_context
         WHERE NULLIF(BTRIM(endpoint_practitioner_role), '') IS NOT NULL
           AND valid_npi_practitioner_rows > 0
           AND street_zip_location_rows > 0
           AND practitioner_role_rows = 0
         ORDER BY valid_npi_practitioner_rows DESC,
                  street_zip_location_rows DESC,
                  lower(org_name),
                  lower(coalesce(plan_name, '')),
                  source_id
         LIMIT $1
        """,
        sample_limit,
    )
    row["available"] = True
    row["samples"] = [dict(item) for item in samples]
    return row


async def _ptg_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    ptg_plan_id: str | None = None,
    sample_limit: int,
    skip_corroboration: bool = False,
    skip_network_name_overlap: bool = False,
    force_live_view_scans: bool = False,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    if await _relation_exists(conn, schema, "entity_address_unified"):
        plan_filter = (
            "WHERE COALESCE(CARDINALITY(ptg_plan_array), 0) > 0 "
            "AND ($1::varchar IS NULL OR $1 = ANY(COALESCE(ptg_plan_array, ARRAY[]::varchar[])))"
        )
        row = await _fetch_mapping(
            conn,
            f"""
            WITH filtered AS (
                SELECT *
                  FROM {_qt(schema, "entity_address_unified")}
                  {plan_filter}
            )
            SELECT
                count(*)::bigint AS ptg_unified_address_rows,
                count(DISTINCT source_key.value)::bigint AS ptg_source_count,
                count(DISTINCT npi)::bigint AS ptg_npi_count,
                count(*) FILTER (WHERE address_key IS NOT NULL)::bigint AS ptg_keyed_address_rows
              FROM filtered
              LEFT JOIN LATERAL unnest(COALESCE(ptg_source_array, ARRAY[]::varchar[])) AS source_key(value)
                ON TRUE
            """,
            ptg_plan_id,
        )
        row["ptg_keyed_address_pct"] = _pct(
            _int(row.get("ptg_keyed_address_rows")),
            _int(row.get("ptg_unified_address_rows")),
        )
        summary["ptg_unified_address"] = {"available": True, **row}
    else:
        summary["ptg_unified_address"] = {"available": False}
    view = "provider_directory_address_corroboration"
    view_kind = await _relation_kind(conn, schema, view)
    if skip_corroboration:
        summary["ptg_corroboration"] = _skipped_summary("disabled by --skip-ptg-corroboration")
    elif not view_kind:
        summary["ptg_corroboration"] = {"available": False}
    elif view_kind == "view" and not force_live_view_scans:
        summary["ptg_corroboration"] = _skipped_summary(
            "corroboration relation is a live view; use --force-ptg-live-view-scans for exact aggregate"
        )
        summary["ptg_corroboration"]["relation_kind"] = view_kind
    else:
        plan_filter = "WHERE ($1::varchar IS NULL OR plan_id = $1 OR ptg_plan_id = $1)"
        row = await _fetch_mapping(
            conn,
            f"""
            SELECT
                count(*)::bigint AS corroboration_rows,
                count(DISTINCT provider_directory_source_id)::bigint AS provider_directory_source_count,
                count(*) FILTER (WHERE provider_directory_active_match IS TRUE)::bigint AS active_match_rows,
                count(*) FILTER (WHERE provider_directory_plan_context_matched IS TRUE)::bigint AS plan_context_match_rows,
                count(*) FILTER (WHERE provider_directory_network_context_present IS TRUE)::bigint AS network_context_rows,
                count(*) FILTER (
                    WHERE cardinality(COALESCE(provider_directory_network_names, ARRAY[]::varchar[])) > 0
                )::bigint AS resolved_network_name_rows,
                count(*) FILTER (
                    WHERE jsonb_array_length(COALESCE(provider_directory_network_matches, '[]'::jsonb)) > 0
                )::bigint AS resolved_network_match_rows
              FROM {_qt(schema, view)}
              {plan_filter}
            """,
            ptg_plan_id,
        )
        summary["ptg_corroboration"] = {"available": True, "relation_kind": view_kind, **row}
    summary["ptg_network_name_overlap"] = (
        _skipped_summary("disabled by --skip-ptg-network-overlap", samples=[])
        if skip_network_name_overlap
        else await _ptg_network_name_overlap_summary(
            conn,
            schema,
            ptg_plan_id=ptg_plan_id,
            sample_limit=sample_limit,
            force_live_view_scans=force_live_view_scans,
        )
    )
    return summary


def _skipped_summary(reason: str, **extra: Any) -> dict[str, Any]:
    return {"available": False, "skipped": True, "reason": reason, **extra}


def _skipped_ptg_summary() -> dict[str, Any]:
    return {
        "ptg_unified_address": _skipped_summary("disabled by --skip-ptg"),
        "ptg_corroboration": _skipped_summary("disabled by --skip-ptg"),
        "ptg_network_name_overlap": _skipped_summary("disabled by --skip-ptg", samples=[]),
    }


def _ptg_network_name_overlap_cte_sql(schema: str, *, ptg_plan_filter: str) -> str:
    view = "provider_directory_address_corroboration"
    pd_name_key = _network_name_key_sql("pd_network_name.value")
    ptg_name_key = _network_name_key_sql("ptg_network_name.value")
    return f"""
        WITH provider_directory_networks AS (
            SELECT DISTINCT
                   corr.snapshot_id,
                   plan_ids.plan_id,
                   corr.provider_directory_source_id,
                   corr.provider_directory_org_name,
                   pd_network_name.value AS provider_directory_network_name,
                   {pd_name_key} AS provider_directory_network_key
              FROM {_qt(schema, view)} corr
              CROSS JOIN LATERAL (
                    VALUES (NULLIF(corr.plan_id, '')), (NULLIF(corr.ptg_plan_id, ''))
              ) AS plan_ids(plan_id)
              CROSS JOIN LATERAL unnest(
                    COALESCE(corr.provider_directory_network_names, ARRAY[]::varchar[])
              ) AS pd_network_name(value)
             WHERE corr.snapshot_id IS NOT NULL
               AND plan_ids.plan_id IS NOT NULL
               AND NULLIF(BTRIM(pd_network_name.value), '') IS NOT NULL
               {ptg_plan_filter}
        ),
        plan_pairs AS (
            SELECT DISTINCT snapshot_id, plan_id
              FROM provider_directory_networks
        ),
        ptg_networks AS (
            SELECT DISTINCT
                   rates.snapshot_id,
                   rates.plan_id,
                   ptg_network_name.value AS ptg_network_name,
                   {ptg_name_key} AS ptg_network_key
              FROM {_qt(schema, "ptg2_serving_rate_compact")} rates
              JOIN plan_pairs
                ON plan_pairs.snapshot_id = rates.snapshot_id
               AND plan_pairs.plan_id = rates.plan_id
              CROSS JOIN LATERAL unnest(
                    COALESCE(rates.network_names, ARRAY[]::varchar[])
              ) AS ptg_network_name(value)
             WHERE NULLIF(BTRIM(ptg_network_name.value), '') IS NOT NULL
        ),
        pairs AS (
            SELECT pd.snapshot_id,
                   pd.plan_id,
                   pd.provider_directory_source_id,
                   pd.provider_directory_org_name,
                   pd.provider_directory_network_name,
                   pd.provider_directory_network_key,
                   ptg.ptg_network_name,
                   ptg.ptg_network_key,
                   (
                       pd.provider_directory_network_key <> ''
                       AND pd.provider_directory_network_key = ptg.ptg_network_key
                   ) AS network_name_matched
              FROM provider_directory_networks pd
              LEFT JOIN ptg_networks ptg
                ON ptg.snapshot_id = pd.snapshot_id
               AND ptg.plan_id = pd.plan_id
        ),
        matched AS (
            SELECT DISTINCT
                   snapshot_id,
                   plan_id,
                   provider_directory_source_id,
                   provider_directory_network_name,
                   provider_directory_network_key,
                   ptg_network_name
              FROM pairs
             WHERE network_name_matched IS TRUE
        )
    """


async def _ptg_network_name_overlap_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    ptg_plan_id: str | None,
    sample_limit: int,
    force_live_view_scans: bool,
) -> dict[str, Any]:
    view = "provider_directory_address_corroboration"
    view_kind = await _relation_kind(conn, schema, view)
    if view_kind == "view" and not force_live_view_scans:
        return _skipped_summary(
            "corroboration relation is a live view; use --force-ptg-live-view-scans for exact overlap",
            samples=[],
        )
    required = (
        view_kind is not None
        and await _relation_exists(conn, schema, "ptg2_serving_rate_compact")
        and await _column_exists(conn, schema, "ptg2_serving_rate_compact", "network_names")
    )
    if not required:
        return {"available": False, "samples": []}
    ptg_plan_filter = (
        "AND ($1::varchar IS NULL OR plan_ids.plan_id = $1)"
        if ptg_plan_id is not None
        else ""
    )
    cte_sql = _ptg_network_name_overlap_cte_sql(schema, ptg_plan_filter=ptg_plan_filter)
    args = [ptg_plan_id] if ptg_plan_id is not None else []
    row = await _fetch_mapping(
        conn,
        f"""
        {cte_sql}
        SELECT
            (SELECT count(*)::bigint FROM provider_directory_networks) AS provider_directory_plan_network_names,
            (SELECT count(*)::bigint FROM ptg_networks) AS ptg_plan_network_names,
            (SELECT count(DISTINCT (snapshot_id, plan_id))::bigint FROM provider_directory_networks)
                AS plan_pairs_with_provider_directory_networks,
            (SELECT count(DISTINCT (snapshot_id, plan_id))::bigint FROM ptg_networks)
                AS plan_pairs_with_ptg_networks,
            (SELECT count(DISTINCT (pd.snapshot_id, pd.plan_id))::bigint
               FROM provider_directory_networks pd
               JOIN ptg_networks ptg
                 ON ptg.snapshot_id = pd.snapshot_id
                AND ptg.plan_id = pd.plan_id)
                AS plan_pairs_with_both_network_sets,
            (SELECT count(*)::bigint FROM matched) AS matched_plan_network_names,
            (SELECT count(DISTINCT (snapshot_id, plan_id))::bigint FROM matched) AS matched_plan_pairs
        """,
        *args,
    )
    sample_rows = await conn.fetch(
        f"""
        {cte_sql}
        SELECT provider_directory_source_id,
               provider_directory_org_name,
               provider_directory_network_name,
               count(DISTINCT (snapshot_id, plan_id))::bigint AS plan_pair_count,
               array_agg(DISTINCT ptg_network_name ORDER BY ptg_network_name)
                   FILTER (WHERE ptg_network_name IS NOT NULL)::varchar[] AS sample_ptg_network_names
          FROM pairs
         WHERE provider_directory_network_key <> ''
           AND NOT EXISTS (
                SELECT 1
                  FROM matched
                 WHERE matched.snapshot_id = pairs.snapshot_id
                   AND matched.plan_id = pairs.plan_id
                   AND matched.provider_directory_source_id = pairs.provider_directory_source_id
                   AND matched.provider_directory_network_key = pairs.provider_directory_network_key
           )
         GROUP BY provider_directory_source_id, provider_directory_org_name, provider_directory_network_name
         ORDER BY count(DISTINCT (snapshot_id, plan_id)) DESC,
                  provider_directory_org_name,
                  provider_directory_network_name
         LIMIT ${len(args) + 1}
        """,
        *args,
        sample_limit,
    )
    row["available"] = True
    row["provider_directory_network_match_pct"] = _pct(
        _int(row.get("matched_plan_network_names")),
        _int(row.get("provider_directory_plan_network_names")),
    )
    row["plan_pair_match_pct"] = _pct(
        _int(row.get("matched_plan_pairs")),
        _int(row.get("plan_pairs_with_both_network_sets")),
    )
    row["samples"] = [dict(item) for item in sample_rows]
    for item in row["samples"]:
        item["sample_ptg_network_names"] = _list(item.get("sample_ptg_network_names"))
    return row


async def _network_resolution_summary(conn: asyncpg.Connection, schema: str, *, sample_limit: int) -> dict[str, Any]:
    required = (
        "provider_directory_practitioner_role",
        "provider_directory_organization_affiliation",
        "provider_directory_insurance_plan",
        "provider_directory_organization",
    )
    if not all([await _relation_exists(conn, schema, table) for table in required]):
        return {"available": False, "top_unresolved_refs": []}
    ref_resource_id = _sql_fhir_reference_resource_id("refs_raw.ref", "Organization")
    row = await _fetch_mapping(
        conn,
        f"""
        WITH refs_raw AS (
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_practitioner_role")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_organization_affiliation")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_insurance_plan")}
        ),
        refs AS MATERIALIZED (
            SELECT source_id,
                   ref,
                   {ref_resource_id} AS ref_resource_id
              FROM refs_raw
             WHERE NULLIF(BTRIM(ref), '') IS NOT NULL
        ),
        distinct_refs AS MATERIALIZED (
            SELECT DISTINCT source_id, ref, ref_resource_id
              FROM refs
        ),
        resolved AS (
            SELECT distinct_refs.source_id, distinct_refs.ref, org.resource_id, org.name
              FROM distinct_refs
              LEFT JOIN {_qt(schema, "provider_directory_organization")} org
                ON org.source_id = distinct_refs.source_id
               AND org.resource_id = distinct_refs.ref_resource_id
        )
        SELECT
            (SELECT count(*)::bigint FROM refs) AS network_ref_rows,
            count(*)::bigint AS distinct_network_refs,
            count(*) FILTER (WHERE resource_id IS NOT NULL)::bigint
                AS resolved_network_refs,
            count(*) FILTER (WHERE resource_id IS NULL)::bigint
                AS unresolved_network_refs
          FROM resolved
        """,
    )
    unresolved = await conn.fetch(
        f"""
        WITH refs_raw AS (
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_practitioner_role")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_organization_affiliation")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_insurance_plan")}
        ),
        refs AS MATERIALIZED (
            SELECT source_id,
                   ref,
                   {ref_resource_id} AS ref_resource_id
              FROM refs_raw
             WHERE NULLIF(BTRIM(ref), '') IS NOT NULL
        )
        SELECT refs.source_id, src.org_name, refs.ref, count(*)::bigint AS reference_count
          FROM refs
          LEFT JOIN {_qt(schema, "provider_directory_organization")} org
            ON org.source_id = refs.source_id
           AND org.resource_id = refs.ref_resource_id
          LEFT JOIN {_qt(schema, "provider_directory_source")} src
            ON src.source_id = refs.source_id
         WHERE org.resource_id IS NULL
         GROUP BY refs.source_id, src.org_name, refs.ref
         ORDER BY count(*) DESC, src.org_name, refs.ref
         LIMIT $1
        """,
        sample_limit,
    )
    total_distinct = _int(row.get("distinct_network_refs"))
    row["available"] = True
    row["resolved_network_ref_pct"] = _pct(_int(row.get("resolved_network_refs")), total_distinct)
    row["top_unresolved_refs"] = [dict(item) for item in unresolved]
    return row


def _plan_network_context_cte_sql(schema: str) -> str:
    ref_resource_id = _sql_fhir_reference_resource_id("refs_raw.ref", "Organization")
    return f"""
        WITH insurance_plans AS (
            SELECT source_id,
                   resource_id,
                   name,
                   jsonb_array_length(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS network_ref_count
              FROM {_qt(schema, "provider_directory_insurance_plan")}
        ),
        plan_counts AS (
            SELECT source_id,
                   count(*)::bigint AS insurance_plan_rows,
                   count(*) FILTER (WHERE network_ref_count > 0)::bigint AS insurance_plan_rows_with_network_refs
              FROM insurance_plans
             GROUP BY source_id
        ),
        refs_raw AS (
            SELECT 'InsurancePlan'::varchar AS resource_type,
                   source_id,
                   resource_id,
                   jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_insurance_plan")}
            UNION ALL
            SELECT 'PractitionerRole'::varchar AS resource_type,
                   source_id,
                   resource_id,
                   jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_practitioner_role")}
            UNION ALL
            SELECT 'OrganizationAffiliation'::varchar AS resource_type,
                   source_id,
                   resource_id,
                   jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_organization_affiliation")}
        ),
        refs AS MATERIALIZED (
            SELECT resource_type,
                   source_id,
                   resource_id,
                   ref,
                   {ref_resource_id} AS ref_resource_id
              FROM refs_raw
             WHERE NULLIF(BTRIM(ref), '') IS NOT NULL
        ),
        ref_counts AS (
            SELECT source_id,
                   count(*)::bigint AS network_ref_rows,
                   count(*) FILTER (WHERE resource_type = 'InsurancePlan')::bigint AS insurance_plan_network_ref_rows,
                   count(*) FILTER (WHERE resource_type = 'PractitionerRole')::bigint AS practitioner_role_network_ref_rows,
                   count(*) FILTER (WHERE resource_type = 'OrganizationAffiliation')::bigint AS organization_affiliation_network_ref_rows,
                   count(DISTINCT ref)::bigint AS distinct_network_refs
              FROM refs
             GROUP BY source_id
        ),
        distinct_refs AS MATERIALIZED (
            SELECT DISTINCT source_id, ref, ref_resource_id
              FROM refs
        ),
        resolved AS (
            SELECT distinct_refs.source_id,
                   distinct_refs.ref,
                   org.resource_id AS resolved_resource_id,
                   NULLIF(BTRIM(org.name), '') AS resolved_network_name
              FROM distinct_refs
              LEFT JOIN {_qt(schema, "provider_directory_organization")} AS org
                ON org.source_id = distinct_refs.source_id
               AND org.resource_id = distinct_refs.ref_resource_id
        ),
        resolved_counts AS (
            SELECT source_id,
                   count(DISTINCT ref) FILTER (WHERE resolved_resource_id IS NOT NULL)::bigint
                       AS resolved_network_refs,
                   count(DISTINCT resolved_network_name) FILTER (WHERE resolved_network_name IS NOT NULL)::bigint
                       AS resolved_network_names,
                   (array_agg(DISTINCT resolved_network_name ORDER BY resolved_network_name)
                       FILTER (WHERE resolved_network_name IS NOT NULL))[1:5]::varchar[]
                       AS sample_resolved_network_names
              FROM resolved
             GROUP BY source_id
        ),
        source_context AS (
            SELECT src.source_id,
                   src.org_name,
                   src.plan_name,
                   src.canonical_api_base,
                   COALESCE(plan_counts.insurance_plan_rows, 0)::bigint AS insurance_plan_rows,
                   COALESCE(plan_counts.insurance_plan_rows_with_network_refs, 0)::bigint
                       AS insurance_plan_rows_with_network_refs,
                   COALESCE(ref_counts.network_ref_rows, 0)::bigint AS network_ref_rows,
                   COALESCE(ref_counts.insurance_plan_network_ref_rows, 0)::bigint
                       AS insurance_plan_network_ref_rows,
                   COALESCE(ref_counts.practitioner_role_network_ref_rows, 0)::bigint
                       AS practitioner_role_network_ref_rows,
                   COALESCE(ref_counts.organization_affiliation_network_ref_rows, 0)::bigint
                       AS organization_affiliation_network_ref_rows,
                   COALESCE(ref_counts.distinct_network_refs, 0)::bigint AS distinct_network_refs,
                   COALESCE(resolved_counts.resolved_network_refs, 0)::bigint AS resolved_network_refs,
                   COALESCE(resolved_counts.resolved_network_names, 0)::bigint AS resolved_network_names,
                   COALESCE(resolved_counts.sample_resolved_network_names, ARRAY[]::varchar[])
                       AS sample_resolved_network_names
              FROM {_qt(schema, "provider_directory_source")} AS src
              LEFT JOIN plan_counts
                ON plan_counts.source_id = src.source_id
              LEFT JOIN ref_counts
                ON ref_counts.source_id = src.source_id
              LEFT JOIN resolved_counts
                ON resolved_counts.source_id = src.source_id
        )
    """


async def _plan_network_context_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    required = (
        "provider_directory_source",
        "provider_directory_insurance_plan",
        "provider_directory_practitioner_role",
        "provider_directory_organization_affiliation",
        "provider_directory_organization",
    )
    missing = [table for table in required if not await _relation_exists(conn, schema, table)]
    if missing:
        return {"available": False, "missing_tables": missing, "samples": []}

    cte_sql = _plan_network_context_cte_sql(schema)
    row = await _fetch_mapping(
        conn,
        f"""
        {cte_sql}
        SELECT
            count(*)::bigint AS source_count,
            count(*) FILTER (WHERE insurance_plan_rows > 0)::bigint AS sources_with_insurance_plans,
            sum(insurance_plan_rows)::bigint AS insurance_plan_rows,
            count(*) FILTER (WHERE insurance_plan_rows_with_network_refs > 0)::bigint
                AS sources_with_insurance_plan_network_refs,
            sum(insurance_plan_rows_with_network_refs)::bigint AS insurance_plan_rows_with_network_refs,
            count(*) FILTER (WHERE network_ref_rows > 0)::bigint AS sources_with_any_network_refs,
            sum(network_ref_rows)::bigint AS network_ref_rows,
            sum(distinct_network_refs)::bigint AS distinct_network_refs,
            sum(resolved_network_refs)::bigint AS resolved_network_refs,
            count(*) FILTER (WHERE resolved_network_names > 0)::bigint AS sources_with_resolved_network_names,
            sum(resolved_network_names)::bigint AS resolved_network_names
          FROM source_context
        """,
    )
    sample_rows = await conn.fetch(
        f"""
        {cte_sql}
        SELECT source_id,
               org_name,
               plan_name,
               canonical_api_base,
               insurance_plan_rows,
               insurance_plan_rows_with_network_refs,
               network_ref_rows,
               insurance_plan_network_ref_rows,
               practitioner_role_network_ref_rows,
               organization_affiliation_network_ref_rows,
               distinct_network_refs,
               resolved_network_refs,
               resolved_network_names,
               sample_resolved_network_names
          FROM source_context
         WHERE insurance_plan_rows > 0
            OR network_ref_rows > 0
         ORDER BY resolved_network_names DESC,
                  network_ref_rows DESC,
                  insurance_plan_rows DESC,
                  org_name,
                  source_id
         LIMIT $1
        """,
        sample_limit,
    )
    row["available"] = True
    row["insurance_plan_source_pct"] = _pct(
        _int(row.get("sources_with_insurance_plans")),
        _int(row.get("source_count")),
    )
    row["network_ref_source_pct"] = _pct(
        _int(row.get("sources_with_any_network_refs")),
        _int(row.get("source_count")),
    )
    row["resolved_network_ref_pct"] = _pct(
        _int(row.get("resolved_network_refs")),
        _int(row.get("distinct_network_refs")),
    )
    row["samples"] = [dict(item) for item in sample_rows]
    for item in row["samples"]:
        item["sample_resolved_network_names"] = _list(item.get("sample_resolved_network_names"))
    return row


async def _network_catalog_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    table = "provider_directory_network_catalog"
    if not await _relation_exists(conn, schema, table):
        return {"available": False, "samples": []}

    row = await _fetch_mapping(
        conn,
        f"""
        SELECT
            count(*)::bigint AS network_catalog_rows,
            count(DISTINCT source_id)::bigint AS network_catalog_source_count,
            count(DISTINCT provider_directory_network_key)::bigint AS distinct_network_keys,
            count(*) FILTER (
                WHERE provider_directory_issuer_network_match_key IS NOT NULL
            )::bigint AS rows_with_issuer_network_match_key,
            sum(insurance_plan_ref_count)::bigint AS insurance_plan_ref_count,
            sum(practitioner_role_ref_count)::bigint AS practitioner_role_ref_count,
            sum(organization_affiliation_ref_count)::bigint AS organization_affiliation_ref_count,
            sum(distinct_ref_count)::bigint AS distinct_ref_count,
            max(published_at) AS latest_published_at
          FROM {_qt(schema, table)}
        """,
    )
    sample_rows = await conn.fetch(
        f"""
        SELECT source_id,
               source_org_name,
               source_plan_name,
               canonical_api_base,
               count(*)::bigint AS network_count,
               sum(distinct_ref_count)::bigint AS distinct_ref_count,
               sum(insurance_plan_ref_count)::bigint AS insurance_plan_ref_count,
               sum(practitioner_role_ref_count)::bigint AS practitioner_role_ref_count,
               sum(organization_affiliation_ref_count)::bigint AS organization_affiliation_ref_count,
               (array_agg(provider_directory_network_name ORDER BY provider_directory_network_name))[1:5]::varchar[]
                   AS sample_network_names
          FROM {_qt(schema, table)}
         GROUP BY source_id, source_org_name, source_plan_name, canonical_api_base
         ORDER BY count(*) DESC, source_org_name, source_id
         LIMIT $1
        """,
        sample_limit,
    )
    row["available"] = True
    row["issuer_network_match_key_pct"] = _pct(
        _int(row.get("rows_with_issuer_network_match_key")),
        _int(row.get("network_catalog_rows")),
    )
    row["samples"] = [dict(item) for item in sample_rows]
    for item in row["samples"]:
        item["sample_network_names"] = _list(item.get("sample_network_names"))
    return row


async def _top_source_yield(conn: asyncpg.Connection, schema: str, *, sample_limit: int) -> list[dict[str, Any]]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return []
    counts_by_source: dict[str, dict[str, int]] = {}
    for table in PROVIDER_DIRECTORY_RESOURCE_TABLES:
        if not await _relation_exists(conn, schema, table):
            continue
        rows = await conn.fetch(
            f"""
            SELECT source_id, count(*)::bigint AS row_count
              FROM {_qt(schema, table)}
             GROUP BY source_id
            """
        )
        key = table.removeprefix("provider_directory_")
        for row in rows:
            counts_by_source.setdefault(row["source_id"], {})[key] = _int(row["row_count"])

    if not counts_by_source:
        return []
    rows = await conn.fetch(
        f"""
        SELECT source_id, org_name, plan_name, canonical_api_base, last_probe_status, auth_type
          FROM {_qt(schema, "provider_directory_source")}
         WHERE source_id = ANY($1::varchar[])
        """,
        list(counts_by_source),
    )
    items = []
    for row in rows:
        counts = counts_by_source.get(row["source_id"], {})
        total = sum(counts.values())
        items.append({**dict(row), "resource_rows": total, "resource_counts": counts})
    return sorted(items, key=lambda item: (-item["resource_rows"], str(item["org_name"])))[:sample_limit]


async def _alias_fanout_summary(conn: asyncpg.Connection, schema: str, *, sample_limit: int) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "reason": "provider_directory_source unavailable", "resources": []}
    resources: list[dict[str, Any]] = []
    total_excess = 0
    for resource_type, table in PROVIDER_DIRECTORY_RESOURCE_TABLE_BY_TYPE.items():
        if not await _relation_exists(conn, schema, table):
            continue
        rows = await conn.fetch(
            f"""
            SELECT COALESCE(NULLIF(src.canonical_api_base, ''), NULLIF(src.api_base, '')) AS api_base,
                   min(src.org_name) AS sample_org_name,
                   min(src.plan_name) AS sample_plan_name,
                   count(DISTINCT rows.source_id)::bigint AS source_count,
                   count(*)::bigint AS source_resource_rows,
                   count(DISTINCT rows.resource_id)::bigint AS distinct_resource_ids
              FROM {_qt(schema, table)} AS rows
              JOIN {_qt(schema, "provider_directory_source")} AS src
                ON src.source_id = rows.source_id
             WHERE COALESCE(NULLIF(src.canonical_api_base, ''), NULLIF(src.api_base, '')) IS NOT NULL
             GROUP BY 1
            HAVING count(DISTINCT rows.source_id) > 1
               AND count(*) > count(DISTINCT rows.resource_id)
             ORDER BY (count(*) - count(DISTINCT rows.resource_id)) DESC, count(*) DESC
             LIMIT $1
            """,
            sample_limit,
        )
        samples = []
        resource_excess = 0
        for row in rows:
            item = dict(row)
            item["excess_source_resource_rows"] = _int(item["source_resource_rows"]) - _int(
                item["distinct_resource_ids"]
            )
            item["fanout_ratio"] = round(
                float(_int(item["source_resource_rows"])) / float(_int(item["distinct_resource_ids"])),
                2,
            ) if _int(item["distinct_resource_ids"]) else 0.0
            resource_excess += item["excess_source_resource_rows"]
            samples.append(item)
        if samples:
            total_excess += resource_excess
            resources.append(
                {
                    "resource_type": resource_type,
                    "excess_source_resource_rows": resource_excess,
                    "samples": samples,
                }
            )
    return {
        "available": True,
        "resource_count": len(resources),
        "excess_source_resource_rows": total_excess,
        "resources": resources,
    }


async def _canonical_resource_summary(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_canonical_resource"):
        return {
            "available": False,
            "reason": "provider_directory_canonical_resource unavailable",
            "resources": [],
        }
    if not await _relation_exists(conn, schema, "provider_directory_source_resource"):
        return {
            "available": False,
            "reason": "provider_directory_source_resource unavailable",
            "resources": [],
        }
    row = await conn.fetchrow(
        f"""
        SELECT (SELECT count(*)::bigint FROM {_qt(schema, "provider_directory_canonical_resource")}) AS canonical_rows,
               (SELECT count(*)::bigint FROM {_qt(schema, "provider_directory_source_resource")}) AS source_edge_rows,
               (
                   SELECT count(DISTINCT source_id)::bigint
                     FROM {_qt(schema, "provider_directory_source_resource")}
               ) AS source_count,
               (
                   SELECT count(DISTINCT canonical_api_base)::bigint
                     FROM {_qt(schema, "provider_directory_canonical_resource")}
               ) AS canonical_api_base_count
        """
    )
    resources = await conn.fetch(
        f"""
        SELECT COALESCE(c.resource_type, s.resource_type) AS resource_type,
               count(DISTINCT (c.canonical_api_base, c.resource_id))::bigint AS canonical_rows,
               count(s.source_id)::bigint AS source_edge_rows,
               count(DISTINCT s.source_id)::bigint AS source_count,
               count(DISTINCT COALESCE(c.canonical_api_base, s.canonical_api_base))::bigint AS canonical_api_base_count
          FROM {_qt(schema, "provider_directory_canonical_resource")} AS c
          FULL JOIN {_qt(schema, "provider_directory_source_resource")} AS s
            ON s.canonical_api_base = c.canonical_api_base
           AND s.resource_type = c.resource_type
           AND s.resource_id = c.resource_id
         GROUP BY 1
         ORDER BY count(s.source_id) DESC, count(DISTINCT (c.canonical_api_base, c.resource_id)) DESC
        """
    )
    canonical_rows = _int(row["canonical_rows"] if row else 0)
    source_edge_rows = _int(row["source_edge_rows"] if row else 0)
    items = []
    for item_row in resources:
        item = dict(item_row)
        item["edge_surplus_rows"] = max(0, _int(item["source_edge_rows"]) - _int(item["canonical_rows"]))
        items.append(item)
    return {
        "available": True,
        "canonical_rows": canonical_rows,
        "source_edge_rows": source_edge_rows,
        "edge_surplus_rows": max(0, source_edge_rows - canonical_rows),
        "source_count": _int(row["source_count"] if row else 0),
        "canonical_api_base_count": _int(row["canonical_api_base_count"] if row else 0),
        "resources": items,
    }


async def _advertised_resource_gap_summary(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_capability"):
        return {"available": False, "resources": []}
    if not await _column_exists(conn, schema, "provider_directory_capability", "supported_resources"):
        return {"available": False, "resources": []}
    has_source_table = await _relation_exists(conn, schema, "provider_directory_source")
    resources: list[dict[str, Any]] = []
    for resource_type, table in PROVIDER_DIRECTORY_RESOURCE_TABLE_BY_TYPE.items():
        row_source_sql = (
            f"SELECT DISTINCT source_id FROM {_qt(schema, table)}"
            if await _relation_exists(conn, schema, table)
            else "SELECT NULL::varchar AS source_id WHERE false"
        )
        row = await _fetch_mapping(
            conn,
            f"""
            WITH advertised AS (
                SELECT DISTINCT source_id
                  FROM {_qt(schema, "provider_directory_capability")} capability
                 WHERE capability.probe_status = 'valid'
                   AND EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements_text(
                              COALESCE(capability.supported_resources::jsonb, '[]'::jsonb)
                          ) AS supported(resource_type)
                         WHERE supported.resource_type = $1
                   )
            ),
            row_sources AS ({row_source_sql})
            SELECT
                count(*)::bigint AS advertised_source_count,
                count(*) FILTER (
                    WHERE EXISTS (
                        SELECT 1
                          FROM row_sources rows
                         WHERE rows.source_id = advertised.source_id
                    )
                )::bigint AS source_with_rows_count,
                count(*) FILTER (
                    WHERE NOT EXISTS (
                        SELECT 1
                          FROM row_sources rows
                         WHERE rows.source_id = advertised.source_id
                    )
                )::bigint AS advertised_without_rows_count
              FROM advertised
            """,
            resource_type,
        )
        item: dict[str, Any] = {
            "resource_type": resource_type,
            "table": table,
            "available": await _relation_exists(conn, schema, table),
            **row,
        }
        advertised_count = _int(item.get("advertised_source_count"))
        item["source_with_rows_pct"] = _pct(_int(item.get("source_with_rows_count")), advertised_count)
        if _int(item.get("advertised_without_rows_count")) and has_source_table:
            samples = await conn.fetch(
                f"""
                WITH advertised AS (
                    SELECT DISTINCT source_id
                      FROM {_qt(schema, "provider_directory_capability")} capability
                     WHERE capability.probe_status = 'valid'
                       AND EXISTS (
                            SELECT 1
                              FROM jsonb_array_elements_text(
                                  COALESCE(capability.supported_resources::jsonb, '[]'::jsonb)
                              ) AS supported(resource_type)
                             WHERE supported.resource_type = $1
                       )
                ),
                row_sources AS ({row_source_sql})
                SELECT src.source_id,
                       src.org_name,
                       src.plan_name,
                       src.canonical_api_base,
                       src.auth_type,
                       src.metadata_json->'last_resource_import' AS last_resource_import
                  FROM advertised
                  JOIN {_qt(schema, "provider_directory_source")} src
                    ON src.source_id = advertised.source_id
                 WHERE NOT EXISTS (
                        SELECT 1
                          FROM row_sources rows
                         WHERE rows.source_id = advertised.source_id
                   )
                 ORDER BY lower(src.org_name), lower(coalesce(src.plan_name, '')), src.source_id
                 LIMIT $2
                """,
                resource_type,
                sample_limit,
            )
            missing_rows = await conn.fetch(
                f"""
                WITH advertised AS (
                    SELECT DISTINCT source_id
                      FROM {_qt(schema, "provider_directory_capability")} capability
                     WHERE capability.probe_status = 'valid'
                       AND EXISTS (
                            SELECT 1
                              FROM jsonb_array_elements_text(
                                  COALESCE(capability.supported_resources::jsonb, '[]'::jsonb)
                              ) AS supported(resource_type)
                             WHERE supported.resource_type = $1
                       )
                ),
                row_sources AS ({row_source_sql})
                SELECT src.source_id,
                       src.metadata_json->'last_resource_import' AS last_resource_import
                  FROM advertised
                  JOIN {_qt(schema, "provider_directory_source")} src
                    ON src.source_id = advertised.source_id
                 WHERE NOT EXISTS (
                        SELECT 1
                          FROM row_sources rows
                         WHERE rows.source_id = advertised.source_id
                   )
                """,
                resource_type,
            )
            auth_blocked_without_rows = 0
            error_counts: dict[str, int] = {}
            for missing in missing_rows:
                diagnostics = _resource_import_diagnostics(missing["last_resource_import"])
                error = _resource_diagnostic_error(diagnostics, resource_type)
                if not error:
                    continue
                error_counts[error] = error_counts.get(error, 0) + 1
                if _is_resource_auth_error(error):
                    auth_blocked_without_rows += 1
            item["resource_error_counts"] = error_counts
            item["auth_blocked_without_rows_count"] = auth_blocked_without_rows
            item["samples"] = [dict(sample) for sample in samples]
            for sample in item["samples"]:
                sample["last_resource_import"] = _json_object(sample.get("last_resource_import"))
        else:
            item["resource_error_counts"] = {}
            item["auth_blocked_without_rows_count"] = 0
            item["samples"] = []
        resources.append(item)
    totals = {
        "advertised_source_resources": sum(_int(item.get("advertised_source_count")) for item in resources),
        "advertised_without_rows": sum(_int(item.get("advertised_without_rows_count")) for item in resources),
        "advertised_auth_blocked_without_rows": sum(
            _int(item.get("auth_blocked_without_rows_count")) for item in resources
        ),
    }
    totals["advertised_with_rows_pct"] = _pct(
        totals["advertised_source_resources"] - totals["advertised_without_rows"],
        totals["advertised_source_resources"],
    )
    return {"available": True, **totals, "resources": resources}


async def _valid_sources_without_resource_rows(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "provider_directory_source"):
        return {"available": False, "source_count": 0, "samples": []}
    existing_tables = [
        table
        for table in PROVIDER_DIRECTORY_RESOURCE_TABLES
        if await _relation_exists(conn, schema, table)
    ]
    if not existing_tables:
        return {"available": True, "source_count": 0, "samples": []}
    resource_source_union = " UNION ".join(
        f"SELECT source_id FROM {_qt(schema, table)}"
        for table in existing_tables
    )
    count = await conn.fetchval(
        f"""
        WITH resource_sources AS ({resource_source_union})
        SELECT count(*)::bigint
          FROM {_qt(schema, "provider_directory_source")} src
         WHERE src.last_probe_status = 'valid'
           AND NOT EXISTS (
                SELECT 1
                  FROM resource_sources rows
                 WHERE rows.source_id = src.source_id
           )
        """
    )
    rows = await conn.fetch(
        f"""
        WITH resource_sources AS ({resource_source_union})
        SELECT src.source_id,
               src.org_name,
               src.plan_name,
               src.canonical_api_base,
               src.auth_type,
               src.last_validated_status,
               src.last_probe_status,
               src.last_probe_status_code,
               src.last_probe_error,
               src.metadata_json->'last_resource_import' AS last_resource_import
          FROM {_qt(schema, "provider_directory_source")} src
         WHERE src.last_probe_status = 'valid'
           AND NOT EXISTS (
                SELECT 1
                  FROM resource_sources rows
                 WHERE rows.source_id = src.source_id
           )
         ORDER BY lower(src.org_name), lower(coalesce(src.plan_name, '')), src.source_id
         LIMIT $1
        """,
        sample_limit,
    )
    samples = [dict(row) for row in rows]
    auth_blocked_count = 0
    error_counts: dict[str, int] = {}
    count_rows = await conn.fetch(
        f"""
        WITH resource_sources AS ({resource_source_union})
        SELECT src.metadata_json->'last_resource_import' AS last_resource_import
          FROM {_qt(schema, "provider_directory_source")} src
         WHERE src.last_probe_status = 'valid'
           AND NOT EXISTS (
                SELECT 1
                  FROM resource_sources rows
                 WHERE rows.source_id = src.source_id
           )
        """
    )
    for row in count_rows:
        diagnostics = _resource_import_diagnostics(row["last_resource_import"])
        source_auth_blocked = False
        for error, error_count in _resource_error_counts(diagnostics).items():
            error_counts[error] = error_counts.get(error, 0) + error_count
            if _is_resource_auth_error(error):
                source_auth_blocked = True
        if source_auth_blocked:
            auth_blocked_count += 1
    for sample in samples:
        sample["last_resource_import"] = _json_object(sample.get("last_resource_import"))
    return {
        "available": True,
        "source_count": _int(count),
        "resource_auth_required_source_count": auth_blocked_count,
        "resource_error_counts": error_counts,
        "samples": samples,
    }


def _derive_gaps(report: dict[str, Any]) -> list[str]:
    gaps: list[str] = []
    source_summary = report.get("source_summary") or {}
    if source_summary.get("available"):
        if _int(source_summary.get("live_auth_required_count")):
            gaps.append(
                f"{source_summary['live_auth_required_count']} Provider Directory sources require auth/registration before full import."
            )
        if _int(source_summary.get("live_credential_or_gateway_non_fhir_count")):
            gaps.append(
                f"{source_summary['live_credential_or_gateway_non_fhir_count']} Provider Directory non-FHIR probe responses look like credentialed/onboarding gateway responses."
            )
        if _int(source_summary.get("never_probed_count")):
            gaps.append(f"{source_summary['never_probed_count']} Provider Directory source(s) have not been probed.")
    capability_counts = {item["probe_status"]: _int(item["count"]) for item in report.get("capability_status_counts", [])}
    non_fhir = capability_counts.get("valid_non_fhir", 0)
    if non_fhir:
        gaps.append(f"{non_fhir} seed URLs responded but did not expose a FHIR CapabilityStatement.")
    unified = report.get("unified_summary") or {}
    if unified.get("available") and _int(unified.get("provider_directory_null_key_rows")):
        if unified.get("counts_are_lower_bounds"):
            gaps.append("At least one Provider Directory unified-address row still lacks address_key.")
        else:
            gaps.append(
                f"{unified['provider_directory_null_key_rows']} Provider Directory unified-address rows still lack address_key."
            )
    if unified.get("available"):
        provider_directory_rows = _int(unified.get("provider_directory_rows"))
        source_record_id_rows = _int(unified.get("provider_directory_source_record_id_rows"))
        if provider_directory_rows and source_record_id_rows < provider_directory_rows:
            missing = provider_directory_rows - source_record_id_rows
            if unified.get("counts_are_lower_bounds"):
                gaps.append(
                    "At least one Provider Directory unified-address row lacks retained FHIR source record IDs."
                )
            else:
                gaps.append(
                    f"{missing} Provider Directory unified-address rows lack retained FHIR source record IDs."
                )
        if _int(unified.get("provider_directory_country_001_rows")):
            if unified.get("counts_are_lower_bounds"):
                gaps.append(
                    "At least one Provider Directory unified-address row still exposes country_code `001`."
                )
            else:
                gaps.append(
                    f"{unified['provider_directory_country_001_rows']} Provider Directory unified-address rows still expose country_code `001`."
                )
    source_coverage = report.get("source_resource_coverage_summary") or {}
    if source_coverage.get("available"):
        if _int(source_coverage.get("catalog_only_source_count")):
            gaps.append(
                f"{source_coverage['catalog_only_source_count']} Provider Directory source(s) have no imported resource rows."
            )
        if source_coverage.get("projection_counts_exact") is not False:
            if _int(source_coverage.get("sources_with_location_rows_without_keys")):
                gaps.append(
                    f"{source_coverage['sources_with_location_rows_without_keys']} Provider Directory source(s) have Location rows but no keyed Location rows."
                )
            if source_coverage.get("unified_available") and _int(
                source_coverage.get("sources_with_location_rows_without_unified_rows")
            ):
                gaps.append(
                    f"{source_coverage['sources_with_location_rows_without_unified_rows']} Provider Directory source(s) have Location rows but no unified-address projection rows."
                )
            if source_coverage.get("unified_available") and _int(
                source_coverage.get("sources_with_valid_npi_organization_address_rows_without_unified_rows")
            ):
                gaps.append(
                    f"{source_coverage['sources_with_valid_npi_organization_address_rows_without_unified_rows']} Provider Directory source(s) have valid-NPI Organization address rows but no unified-address projection rows."
                )
    role_gap = report.get("practitioner_role_reimport_gap_summary") or {}
    if role_gap.get("available") and _int(role_gap.get("practitioner_role_reimport_gap_source_count")):
        gaps.append(
            f"{role_gap['practitioner_role_reimport_gap_source_count']} Provider Directory source(s) expose PractitionerRole endpoints and have practitioners/locations, but no imported PractitionerRole rows."
        )
    network = report.get("network_resolution_summary") or {}
    if network.get("available") and _int(network.get("unresolved_network_refs")):
        gaps.append(
            f"{network['unresolved_network_refs']} distinct Provider Directory network refs are unresolved to FHIR Organization names."
        )
    plan_network = report.get("plan_network_context_summary") or {}
    if plan_network.get("available"):
        if _int(plan_network.get("insurance_plan_rows")) and not _int(
            plan_network.get("sources_with_insurance_plan_network_refs")
        ):
            gaps.append(
                "Provider Directory InsurancePlan rows are present, but none expose network refs for PTG network-name matching."
            )
        if _int(plan_network.get("distinct_network_refs")) and not _int(
            plan_network.get("resolved_network_refs")
        ):
            gaps.append(
                "Provider Directory network refs are present, but none resolve to network Organization names for PTG matching."
            )
        ptg_corroboration = (report.get("ptg_summary") or {}).get("ptg_corroboration") or {}
        if (
            _int(plan_network.get("sources_with_resolved_network_names"))
            and not ptg_corroboration.get("skipped")
            and not ptg_corroboration.get("available")
        ):
            gaps.append(
                f"Provider Directory has resolved network names from {plan_network['sources_with_resolved_network_names']} source(s), "
                "but `provider_directory_address_corroboration` is not published for PTG network matching."
            )
        network_catalog = report.get("network_catalog_summary") or {}
        if (
            "network_catalog_summary" in report
            and _int(plan_network.get("sources_with_resolved_network_names"))
            and not network_catalog.get("skipped")
            and not network_catalog.get("available")
        ):
            gaps.append(
                "Provider Directory has resolved network names, but `provider_directory_network_catalog` is not published."
            )
        if (
            network_catalog.get("available")
            and _int(plan_network.get("resolved_network_names"))
            and not _int(network_catalog.get("network_catalog_rows"))
        ):
            gaps.append(
                "Provider Directory network names resolve from raw resources, but `provider_directory_network_catalog` is empty."
            )
    valid_zero_rows = report.get("valid_sources_without_resource_rows") or {}
    if valid_zero_rows.get("available") and _int(valid_zero_rows.get("source_count")):
        if _int(valid_zero_rows.get("resource_auth_required_source_count")):
            gaps.append(
                f"{valid_zero_rows['resource_auth_required_source_count']} Provider Directory source(s) have valid metadata but resource endpoints require auth."
            )
        gaps.append(
            f"{valid_zero_rows['source_count']} Provider Directory source(s) have valid metadata but no imported resource rows."
        )
    retest = report.get("source_catalog_retest_coverage") or {}
    if retest.get("available") and _int(retest.get("missing_result_count")):
        missing_parts = []
        if _int(retest.get("importable_missing_result_count")):
            missing_parts.append(f"{retest['importable_missing_result_count']} importable")
        if _int(retest.get("credential_gated_missing_result_count")):
            missing_parts.append(f"{retest['credential_gated_missing_result_count']} credential-gated")
        missing_text = " and ".join(missing_parts) if missing_parts else str(retest["missing_result_count"])
        gaps.append(
            f"{missing_text} retest result(s) are not covered by the current normalized Provider Directory source catalog."
        )
    credential_backlog = report.get("credential_onboarding_backlog") or {}
    if credential_backlog.get("available") and _int(credential_backlog.get("credential_config_missing_source_count")):
        gaps.append(
            f"{credential_backlog['credential_config_missing_source_count']} Provider Directory auth/onboarding source(s) do not match a configured credential rule."
        )
    if credential_backlog.get("available") and _int(credential_backlog.get("credential_secret_missing_source_count")):
        gaps.append(
            f"{credential_backlog['credential_secret_missing_source_count']} Provider Directory credential-configured source(s) reference missing environment secrets."
        )
    timeout_summary = report.get("probe_timeout_summary") or {}
    if timeout_summary.get("available") and _int(timeout_summary.get("timeout_source_count")):
        gaps.append(
            f"{timeout_summary['timeout_source_count']} Provider Directory source probe(s) timed out across {timeout_summary.get('host_count')} host(s)."
        )
    advertised_gaps = report.get("advertised_resource_gap_summary") or {}
    if advertised_gaps.get("available") and _int(advertised_gaps.get("advertised_without_rows")):
        if _int(advertised_gaps.get("advertised_auth_blocked_without_rows")):
            auth_missing = [
                f"{item['resource_type']}={item['auth_blocked_without_rows_count']}"
                for item in advertised_gaps.get("resources", [])
                if _int(item.get("auth_blocked_without_rows_count"))
            ]
            gaps.append(
                "Provider Directory advertised-resource imports are auth-blocked after metadata success: "
                + ", ".join(auth_missing)
                + "."
            )
        missing = [
            f"{item['resource_type']}={item['advertised_without_rows_count']}"
            for item in advertised_gaps.get("resources", [])
            if _int(item.get("advertised_without_rows_count"))
        ]
        gaps.append(
            "Provider Directory advertised-resource imports have supported sources with zero rows: "
            + ", ".join(missing)
            + "."
        )
    ptg = (report.get("ptg_summary") or {}).get("ptg_corroboration") or {}
    if ptg.get("available") and _int(ptg.get("network_context_rows")) and not _int(ptg.get("resolved_network_match_rows")):
        gaps.append("PTG-overlap Provider Directory rows carry network refs, but none currently resolve to network-name matches.")
    ptg_network = (report.get("ptg_summary") or {}).get("ptg_network_name_overlap") or {}
    if (
        ptg_network.get("available")
        and _int(ptg_network.get("provider_directory_plan_network_names"))
        and not _int(ptg_network.get("matched_plan_network_names"))
    ):
        gaps.append("Provider Directory network names are present for PTG plan pairs, but none match PTG serving network_names.")
    ptg_plan_filter = report.get("ptg_plan_filter")
    if ptg_plan_filter:
        ptg_unified_address = (report.get("ptg_summary") or {}).get("ptg_unified_address") or {}
        ptg_corroboration = (report.get("ptg_summary") or {}).get("ptg_corroboration") or {}
        if ptg_unified_address.get("available") and not _int(
            ptg_unified_address.get("ptg_unified_address_rows")
        ):
            gaps.append(
                f"Requested PTG plan `{ptg_plan_filter}` has no PTG-associated unified address rows."
            )
        elif ptg_corroboration.get("available") and not _int(ptg_corroboration.get("corroboration_rows")):
            gaps.append(
                f"Requested PTG plan `{ptg_plan_filter}` has no Provider Directory address corroboration rows."
            )
    return gaps


def _readiness_check(
    name: str,
    *,
    passed: bool,
    required: bool = True,
    reason: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    check = {
        "name": name,
        "status": "pass" if passed else ("fail" if required else "skip"),
        "required": required,
        "metrics": metrics or {},
    }
    if reason and not passed:
        check["reason"] = reason
    return check


def _serving_readiness_summary(report: dict[str, Any]) -> dict[str, Any]:
    source = report.get("source_summary") or {}
    source_coverage = report.get("source_resource_coverage_summary") or {}
    resource_summary = report.get("resource_summary") or {}
    unified = report.get("unified_summary") or {}
    plan_network = report.get("plan_network_context_summary") or {}
    network_catalog = report.get("network_catalog_summary") or {}
    ptg = report.get("ptg_summary") or {}
    ptg_unified_address = ptg.get("ptg_unified_address") or {}
    ptg_corroboration = ptg.get("ptg_corroboration") or {}
    ptg_network = ptg.get("ptg_network_name_overlap") or {}

    checks: list[dict[str, Any]] = []
    source_count = _int(source.get("source_count"))
    checks.append(
        _readiness_check(
            "source_catalog_seeded",
            passed=bool(source.get("available") and source_count > 0),
            reason="provider_directory_source is missing or empty",
            metrics={
                "source_count": source_count,
                "live_valid_count": _int(source.get("live_valid_count")),
                "auth_required_count": _int(source.get("live_auth_required_count")),
            },
        )
    )

    resource_sources = _int(source_coverage.get("sources_with_resource_rows"))
    resource_table_rows = sum(
        _int(summary.get("row_count"))
        for summary in resource_summary.values()
        if isinstance(summary, dict) and summary.get("available") is not False
    )
    resource_rows_available = bool(
        source_coverage.get("available") and resource_sources > 0
    ) or bool(resource_table_rows > 0)
    checks.append(
        _readiness_check(
            "resource_rows_imported",
            passed=resource_rows_available,
            reason="no Provider Directory source has imported FHIR resource rows",
            metrics={
                "sources_with_resource_rows": resource_sources,
                "source_count": _int(source_coverage.get("source_count")),
                "resource_source_pct": source_coverage.get("resource_source_pct"),
                "resource_table_rows": resource_table_rows,
                "source_resource_coverage_skipped": bool(source_coverage.get("skipped")),
            },
        )
    )

    provider_directory_rows = _int(unified.get("provider_directory_rows"))
    keyed_rows = _int(unified.get("provider_directory_keyed_rows"))
    phone_rows = _int(unified.get("provider_directory_phone_rows"))
    source_record_id_rows = _int(unified.get("provider_directory_source_record_id_rows"))
    unified_checks_required = not bool(unified.get("skipped"))
    checks.append(
        _readiness_check(
            "searchable_address_overlay",
            required=unified_checks_required,
            passed=bool(unified.get("available") and provider_directory_rows > 0 and keyed_rows > 0),
            reason=(
                unified.get("reason")
                if unified.get("skipped")
                else "no keyed Provider Directory address rows are available for provider/address search"
            ),
            metrics={
                "summary_source": unified.get("summary_source") or "entity_address_unified",
                "provider_directory_rows": provider_directory_rows,
                "provider_directory_keyed_rows": keyed_rows,
                "provider_directory_keyed_pct": unified.get("provider_directory_keyed_pct"),
            },
        )
    )
    checks.append(
        _readiness_check(
            "searchable_phone_overlay",
            required=unified_checks_required,
            passed=bool(unified.get("available") and provider_directory_rows > 0 and phone_rows > 0),
            reason=(
                unified.get("reason")
                if unified.get("skipped")
                else "no Provider Directory phone rows are available for provider phone search"
            ),
            metrics={
                "summary_source": unified.get("summary_source") or "entity_address_unified",
                "provider_directory_rows": provider_directory_rows,
                "provider_directory_phone_rows": phone_rows,
                "provider_directory_phone_pct": unified.get("provider_directory_phone_pct"),
            },
        )
    )
    checks.append(
        _readiness_check(
            "source_detail_attribution",
            required=unified_checks_required,
            passed=bool(provider_directory_rows > 0 and source_record_id_rows >= provider_directory_rows),
            reason=(
                unified.get("reason")
                if unified.get("skipped")
                else "Provider Directory address rows lack retained FHIR source record ids for provider_directory_sources"
            ),
            metrics={
                "provider_directory_rows": provider_directory_rows,
                "source_record_id_rows": source_record_id_rows,
                "source_record_id_pct": unified.get("provider_directory_source_record_id_pct"),
            },
        )
    )

    network_ref_rows = _int(plan_network.get("network_ref_rows"))
    resolved_network_names = _int(plan_network.get("resolved_network_names"))
    network_catalog_rows = _int(network_catalog.get("network_catalog_rows"))
    network_catalog_required = bool(network_ref_rows or resolved_network_names)
    checks.append(
        _readiness_check(
            "network_catalog_published",
            required=network_catalog_required,
            passed=bool(
                network_catalog.get("available")
                and network_catalog_rows > 0
                and _int(network_catalog.get("rows_with_issuer_network_match_key")) > 0
            ),
            reason=(
                "network refs are present but provider_directory_network_catalog is missing, "
                "empty, or lacks issuer/network match keys"
            ),
            metrics={
                "network_ref_rows": network_ref_rows,
                "resolved_network_names": resolved_network_names,
                "network_catalog_rows": network_catalog_rows,
                "rows_with_issuer_network_match_key": _int(
                    network_catalog.get("rows_with_issuer_network_match_key")
                ),
            },
        )
    )

    ptg_address_rows = _int(ptg_unified_address.get("ptg_unified_address_rows"))
    ptg_corroboration_required = bool(ptg_address_rows and provider_directory_rows)
    checks.append(
        _readiness_check(
            "ptg_corroboration_table",
            required=ptg_corroboration_required,
            passed=bool(
                ptg_corroboration.get("available")
                and ptg_corroboration.get("relation_kind") in {"table", "partitioned_table", "materialized_view"}
                and _int(ptg_corroboration.get("corroboration_rows")) > 0
            ),
            reason="PTG addresses and Provider Directory addresses exist but corroboration table is not published with rows",
            metrics={
                "ptg_unified_address_rows": ptg_address_rows,
                "relation_kind": ptg_corroboration.get("relation_kind"),
                "corroboration_rows": _int(ptg_corroboration.get("corroboration_rows")),
                "active_match_rows": _int(ptg_corroboration.get("active_match_rows")),
            },
        )
    )

    provider_directory_plan_network_names = _int(ptg_network.get("provider_directory_plan_network_names"))
    ptg_network_required = bool(provider_directory_plan_network_names)
    checks.append(
        _readiness_check(
            "ptg_network_name_overlap",
            required=ptg_network_required,
            passed=bool(ptg_network.get("available") and _int(ptg_network.get("matched_plan_network_names")) > 0),
            reason="FHIR network names are present for PTG plan pairs but none match PTG serving network_names",
            metrics={
                "provider_directory_plan_network_names": provider_directory_plan_network_names,
                "matched_plan_network_names": _int(ptg_network.get("matched_plan_network_names")),
                "matched_plan_pairs": _int(ptg_network.get("matched_plan_pairs")),
            },
        )
    )

    required_checks = [check for check in checks if check["required"]]
    failed_required = [check for check in required_checks if check["status"] != "pass"]
    return {
        "status": "ready" if not failed_required else "not_ready",
        "ready": not failed_required,
        "required_pass_count": len(required_checks) - len(failed_required),
        "required_fail_count": len(failed_required),
        "skipped_check_count": len([check for check in checks if check["status"] == "skip"]),
        "checks": checks,
    }


async def build_report(args: argparse.Namespace) -> dict[str, Any]:
    schema = _validate_identifier(args.schema, label="schema")
    conn = await _connect(args)
    try:
        network_resolution_summary = (
            {"available": False, "skipped": True, "reason": "disabled by --skip-network-resolution"}
            if args.skip_network_resolution
            else await _network_resolution_summary(
                conn,
                schema,
                sample_limit=args.sample_limit,
            )
        )
        report = {
            "generated_at": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
            "schema": schema,
            "ptg_plan_filter": args.ptg_plan_id or None,
            "source_summary": await _source_summary(conn, schema),
            "credential_onboarding_backlog": await _credential_onboarding_backlog(
                conn,
                schema,
                sample_limit=args.sample_limit,
                credential_config_file=args.credential_config_file,
            ),
            "probe_timeout_summary": await _probe_timeout_summary(
                conn,
                schema,
                sample_limit=args.sample_limit,
            ),
            "capability_status_counts": await _capability_status_counts(conn, schema),
            "resource_summary": await _resource_summary(
                conn,
                schema,
                use_estimates=args.pod_safe,
            ),
            "source_resource_coverage_summary": (
                {
                    "available": False,
                    "skipped": True,
                    "reason": "disabled by --pod-safe",
                    "samples": [],
                }
                if args.pod_safe
                else await _source_resource_coverage_summary(
                    conn,
                    schema,
                    sample_limit=args.sample_limit,
                    include_unified=not args.skip_unified,
                )
            ),
            "practitioner_role_reimport_gap_summary": await _practitioner_role_reimport_gap_summary(
                conn,
                schema,
                sample_limit=args.sample_limit,
            )
            if not args.skip_practitioner_role_reimport_gap_summary
            else {
                "available": False,
                "skipped": True,
                "reason": "disabled by --skip-practitioner-role-reimport-gap-summary",
                "samples": [],
            },
            "canonical_resource_summary": (
                {
                    "available": False,
                    "skipped": True,
                    "reason": "disabled by --skip-canonical-resource-summary",
                    "resources": [],
                }
                if args.skip_canonical_resource_summary
                else await _canonical_resource_summary(conn, schema)
            ),
            "unified_summary": (
                {"available": False, "skipped": True, "reason": "disabled by --skip-unified"}
                if args.skip_unified
                else await _unified_summary(
                    conn,
                    schema,
                    fast_probe=bool(args.fast_serving_readiness),
                )
            ),
            "ptg_summary": (
                _skipped_ptg_summary()
                if args.skip_ptg
                else await _ptg_summary(
                    conn,
                    schema,
                    ptg_plan_id=args.ptg_plan_id or None,
                    sample_limit=args.sample_limit,
                    skip_corroboration=args.skip_ptg_corroboration,
                    skip_network_name_overlap=args.skip_ptg_network_overlap,
                    force_live_view_scans=args.force_ptg_live_view_scans,
                )
            ),
            "network_resolution_summary": network_resolution_summary,
            "plan_network_context_summary": (
                {"available": False, "skipped": True, "reason": "disabled by --skip-network-resolution", "samples": []}
                if args.skip_network_resolution
                else await _plan_network_context_summary(
                    conn,
                    schema,
                    sample_limit=args.sample_limit,
                )
            ),
            "network_catalog_summary": (
                {"available": False, "skipped": True, "reason": "disabled by --skip-network-resolution", "samples": []}
                if args.skip_network_resolution
                else await _network_catalog_summary(
                    conn,
                    schema,
                    sample_limit=args.sample_limit,
                )
            ),
            "top_source_yield": (
                []
                if args.skip_top_source_yield
                else await _top_source_yield(conn, schema, sample_limit=args.sample_limit)
            ),
            "alias_fanout_summary": (
                {"available": False, "skipped": True, "reason": "disabled by --skip-top-source-yield", "resources": []}
                if args.skip_top_source_yield
                else await _alias_fanout_summary(conn, schema, sample_limit=args.sample_limit)
            ),
            "advertised_resource_gap_summary": (
                {"available": False, "skipped": True, "reason": "disabled by --skip-advertised-resource-gaps"}
                if args.skip_advertised_resource_gaps
                else await _advertised_resource_gap_summary(
                    conn,
                    schema,
                    sample_limit=args.sample_limit,
                )
            ),
            "valid_sources_without_resource_rows": (
                {
                    "available": False,
                    "source_count": 0,
                    "samples": [],
                    "skipped": True,
                    "reason": "disabled by --skip-valid-zero-row-sources",
                }
                if args.skip_valid_zero_row_sources
                else await _valid_sources_without_resource_rows(
                    conn,
                    schema,
                    sample_limit=args.sample_limit,
                )
            ),
            "source_catalog_retest_coverage": await _source_catalog_retest_coverage_from_path(
                conn,
                schema,
                retest_results_path=args.retest_results_path,
                sample_limit=args.sample_limit,
            ),
        }
        report["serving_readiness"] = _serving_readiness_summary(report)
        report["gaps"] = _derive_gaps(report)
        return report
    finally:
        await conn.close()


def render_markdown(report: dict[str, Any]) -> str:
    source = report.get("source_summary") or {}
    unified = report.get("unified_summary") or {}
    network = report.get("network_resolution_summary") or {}
    plan_network = report.get("plan_network_context_summary") or {}
    network_catalog = report.get("network_catalog_summary") or {}
    ptg = report.get("ptg_summary") or {}
    readiness = report.get("serving_readiness") or {}
    credential_backlog = report.get("credential_onboarding_backlog") or {}
    timeout_summary = report.get("probe_timeout_summary") or {}
    alias_fanout = report.get("alias_fanout_summary") or {}
    canonical_resources = report.get("canonical_resource_summary") or {}
    source_coverage = report.get("source_resource_coverage_summary") or {}
    retest = report.get("source_catalog_retest_coverage") or {}
    lines = [
        "# Provider Directory Coverage Audit",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- schema: `{report.get('schema')}`",
        f"- PTG plan filter: `{report.get('ptg_plan_filter') or 'all'}`",
        "",
        "## Summary",
        "",
    ]
    if source.get("available"):
        lines.extend(
            [
                f"- sources: `{source.get('source_count')}`",
                f"- live-valid sources: `{source.get('live_valid_count')}` ({source.get('live_valid_pct')}%)",
                f"- auth-required sources: `{source.get('live_auth_required_count')}` ({source.get('auth_required_pct')}%)",
                f"- timed-out source probes: `{source.get('live_timeout_count')}` ({source.get('timeout_pct')}%)",
                f"- non-FHIR credential/gateway responses: `{source.get('live_credential_or_gateway_non_fhir_count')}` / `{source.get('live_valid_non_fhir_count')}` valid_non_fhir",
                f"- sources with API base: `{source.get('api_base_count')}` ({source.get('api_base_pct')}%)",
            ]
        )
    if readiness:
        lines.append(
            f"- serving readiness: `{readiness.get('status')}` "
            f"(`{readiness.get('required_pass_count')}`/`"
            f"{_int(readiness.get('required_pass_count')) + _int(readiness.get('required_fail_count'))}` "
            "required checks passing)"
        )
    if timeout_summary.get("available"):
        lines.append(
            f"- probe timeout backlog: `{timeout_summary.get('timeout_source_count')}` source(s) across `{timeout_summary.get('host_count')}` host(s)"
        )
    if credential_backlog.get("available"):
        lines.append(
            f"- credential/onboarding backlog: `{credential_backlog.get('blocked_source_count')}` source(s) across `{credential_backlog.get('group_count')}` group(s)"
        )
        if credential_backlog.get("credential_config_source"):
            lines.append(f"- credential config source: `{credential_backlog.get('credential_config_source')}`")
        lines.append(
            f"- credential config coverage: `{credential_backlog.get('credential_configured_source_count')}` configured / "
            f"`{credential_backlog.get('blocked_source_count')}` gated source(s); "
            f"missing config `{credential_backlog.get('credential_config_missing_source_count')}`"
        )
        lines.append(
            f"- credential work split: `{_int(credential_backlog.get('credential_rule_candidate_source_count'))}` credential-rule candidate(s); "
            f"`{_int(credential_backlog.get('endpoint_discovery_needed_source_count'))}` endpoint-discovery source(s)"
        )
        lines.append(
            f"- credential secret readiness: `{credential_backlog.get('credential_secret_ready_source_count')}` ready / "
            f"`{credential_backlog.get('credential_configured_source_count')}` configured source(s); "
            f"missing env `{credential_backlog.get('credential_secret_missing_source_count')}`"
        )
    if retest.get("available"):
        lines.append(
            f"- retest snapshot coverage: `{retest.get('covered_count')}` covered / "
            f"`{retest.get('checked_result_count')}` importable-or-credential-gated retest result(s); "
            f"missing `{retest.get('missing_result_count')}`"
        )
        lines.append(
            f"- retest importable coverage: `{retest.get('importable_covered_count')}` / "
            f"`{retest.get('importable_checked_result_count')}`; "
            f"credential-gated coverage: `{retest.get('credential_gated_covered_count')}` / "
            f"`{retest.get('credential_gated_checked_result_count')}`"
        )
        if _int(retest.get("unchecked_result_count")):
            lines.append(
                f"- retest unchecked failures: `{retest.get('recovered_unchecked_result_count')}` recovered / "
                f"`{retest.get('unchecked_result_count')}` non-importable retest result(s); "
                f"uncovered `{retest.get('uncovered_unchecked_result_count')}`"
            )
    if source_coverage.get("available"):
        lines.append(
            f"- source/resource coverage: `{source_coverage.get('sources_with_resource_rows')}` / "
            f"`{source_coverage.get('source_count')}` source(s) have resource rows "
            f"({source_coverage.get('resource_source_pct')}%); Location sources "
            f"`{source_coverage.get('sources_with_location_rows')}` "
            f"({source_coverage.get('location_source_pct')}%)"
        )
        if source_coverage.get("sources_with_valid_npi_organization_address_rows") is not None:
            lines.append(
                f"- organization-address coverage: "
                f"`{source_coverage.get('sources_with_valid_npi_organization_address_rows')}` source(s), "
                f"`{source_coverage.get('valid_npi_organization_address_rows')}` valid-NPI address row(s)"
            )
        if source_coverage.get("unified_available") and source_coverage.get("projection_counts_exact") is not False:
            lines.append(
                f"- source/search projection coverage: `{source_coverage.get('sources_with_unified_rows')}` / "
                f"`{source_coverage.get('source_count')}` source(s) have unified rows "
                f"({source_coverage.get('unified_source_pct')}%); keyed unified sources "
                f"`{source_coverage.get('sources_with_keyed_unified_rows')}` "
                f"({source_coverage.get('keyed_unified_source_pct')}%)"
            )
        elif source_coverage.get("projection_counts_exact") is False:
            lines.append("- source/search projection coverage: skipped exact per-source scans (metadata-only summary)")
        else:
            lines.append("- source/search projection coverage: skipped with unified-address checks")
    elif source_coverage.get("skipped"):
        lines.append(f"- source/resource coverage: skipped ({source_coverage.get('reason')})")
    if canonical_resources.get("available"):
        lines.append(
            f"- canonical resource storage: `{canonical_resources.get('canonical_rows')}` canonical row(s), "
            f"`{canonical_resources.get('source_edge_rows')}` source edge row(s), "
            f"`{canonical_resources.get('edge_surplus_rows')}` edge surplus row(s)"
        )
    elif canonical_resources.get("skipped"):
        lines.append(f"- canonical resource storage: skipped ({canonical_resources.get('reason')})")
    elif canonical_resources.get("reason"):
        lines.append(f"- canonical resource storage: unavailable ({canonical_resources.get('reason')})")
    if unified.get("available"):
        if unified.get("counts_are_lower_bounds"):
            lines.append(
                f"- unified Provider Directory rows: fast readiness probe via `{unified.get('summary_source')}`; "
                "reported counts are lower bounds, not exact coverage totals"
            )
        lines.extend(
            [
                f"- unified Provider Directory rows: `{unified.get('provider_directory_rows')}`",
                f"- keyed Provider Directory rows: `{unified.get('provider_directory_keyed_rows')}` ({unified.get('provider_directory_keyed_pct')}%)",
                f"- phone Provider Directory rows: `{unified.get('provider_directory_phone_rows')}` ({unified.get('provider_directory_phone_pct')}%)",
                f"- Provider Directory rows with source record IDs: `{unified.get('provider_directory_source_record_id_rows')}` ({unified.get('provider_directory_source_record_id_pct')}%)",
                f"- Provider Directory rows with country `001`: `{unified.get('provider_directory_country_001_rows')}`",
            ]
        )
    elif unified.get("skipped"):
        lines.append(f"- unified Provider Directory rows: skipped ({unified.get('reason')})")
    ptg_corr = ptg.get("ptg_corroboration") or {}
    ptg_network = ptg.get("ptg_network_name_overlap") or {}
    if ptg_corr.get("available"):
        lines.extend(
            [
                f"- PTG corroboration rows: `{ptg_corr.get('corroboration_rows')}`",
                f"- PTG plan-context matches: `{ptg_corr.get('plan_context_match_rows')}`",
                f"- resolved network-name match rows: `{ptg_corr.get('resolved_network_match_rows')}`",
            ]
        )
    elif ptg_corr.get("skipped"):
        lines.append(f"- PTG corroboration: skipped ({ptg_corr.get('reason')})")
    if ptg_network.get("available"):
        lines.append(
            f"- PTG/FHIR network-name overlap: `{ptg_network.get('matched_plan_network_names')}` / `{ptg_network.get('provider_directory_plan_network_names')}` "
            f"provider-directory plan-network names ({ptg_network.get('provider_directory_network_match_pct')}%); "
            f"matched plan pairs `{ptg_network.get('matched_plan_pairs')}` / `{ptg_network.get('plan_pairs_with_both_network_sets')}` "
            f"({ptg_network.get('plan_pair_match_pct')}%)"
        )
    elif ptg_network.get("skipped"):
        lines.append(f"- PTG/FHIR network-name overlap: skipped ({ptg_network.get('reason')})")
    if network.get("available"):
        lines.append(
            f"- resolved network refs: `{network.get('resolved_network_refs')}` / `{network.get('distinct_network_refs')}` ({network.get('resolved_network_ref_pct')}%)"
        )
    elif network.get("skipped"):
        lines.append(f"- network resolution: skipped ({network.get('reason')})")
    if plan_network.get("available"):
        lines.append(
            f"- plan/network context: `{plan_network.get('sources_with_insurance_plans')}` source(s) with InsurancePlan rows "
            f"({plan_network.get('insurance_plan_source_pct')}%), "
            f"`{plan_network.get('sources_with_any_network_refs')}` source(s) with network refs "
            f"({plan_network.get('network_ref_source_pct')}%), resolved refs "
            f"`{plan_network.get('resolved_network_refs')}` / `{plan_network.get('distinct_network_refs')}` "
            f"({plan_network.get('resolved_network_ref_pct')}%)"
        )
    elif plan_network.get("skipped"):
        lines.append(f"- plan/network context: skipped ({plan_network.get('reason')})")
    if network_catalog.get("available"):
        lines.append(
            f"- network catalog: `{network_catalog.get('network_catalog_rows')}` network(s) across "
            f"`{network_catalog.get('network_catalog_source_count')}` source(s); issuer/network match keys "
            f"`{network_catalog.get('rows_with_issuer_network_match_key')}` "
            f"({network_catalog.get('issuer_network_match_key_pct')}%)"
        )
    elif network_catalog.get("skipped"):
        lines.append(f"- network catalog: skipped ({network_catalog.get('reason')})")
    advertised_gaps = report.get("advertised_resource_gap_summary") or {}
    if advertised_gaps.get("available"):
        lines.append(
            f"- advertised resource/source gaps: `{advertised_gaps.get('advertised_without_rows')}` / `{advertised_gaps.get('advertised_source_resources')}` "
            f"({advertised_gaps.get('advertised_with_rows_pct')}% with rows); "
            f"auth-blocked after metadata: `{advertised_gaps.get('advertised_auth_blocked_without_rows')}`"
        )
    elif advertised_gaps.get("skipped"):
        lines.append(f"- advertised resource/source gaps: skipped ({advertised_gaps.get('reason')})")
    if alias_fanout.get("available"):
        lines.append(
            f"- alias fan-out excess source/resource rows: `{alias_fanout.get('excess_source_resource_rows')}` across `{alias_fanout.get('resource_count')}` resource type(s)"
        )
    elif alias_fanout.get("skipped"):
        lines.append(f"- alias fan-out: skipped ({alias_fanout.get('reason')})")
    if report.get("gaps"):
        lines.extend(["", "## Gaps", ""])
        lines.extend(f"- {gap}" for gap in report["gaps"])
    if readiness.get("checks"):
        lines.extend(
            [
                "",
                "## Serving Readiness Gate",
                "",
                "| Check | Status | Required | Reason | Metrics |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for check in readiness["checks"]:
            metrics = json.dumps(check.get("metrics") or {}, sort_keys=True, default=str)
            lines.append(
                f"| `{_markdown_cell(check.get('name'))}` | `{_markdown_cell(check.get('status'))}` | "
                f"`{bool(check.get('required'))}` | {_markdown_cell(check.get('reason') or '')} | "
                f"`{_markdown_cell(metrics)}` |"
            )
    if report.get("capability_status_counts"):
        lines.extend(["", "## Capability Status", "", "| Status | Count |", "| --- | ---: |"])
        for item in report["capability_status_counts"]:
            lines.append(f"| `{item.get('probe_status')}` | {item.get('count')} |")
    if credential_backlog.get("groups"):
        lines.extend(
            [
                "",
                "## Credential/Onboarding Backlog",
                "",
                "| Host | Status | Auth | Reason | Sources | Credential Candidates | Endpoint Discovery | API Bases | Markets | Configured | Secret Ready | Missing Config | Missing Env | Sample payers | Sample missing credentials | Missing env names |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- | --- | --- |",
            ]
        )
        credential_groups = credential_backlog.get("groups") or []
        displayed_credential_groups = credential_groups[:CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT]
        for item in displayed_credential_groups:
            samples = ", ".join(_markdown_cell(payer) for payer in item.get("sample_payers") or [])
            missing_samples = ", ".join(
                _markdown_cell(payer) for payer in item.get("sample_missing_credential_payers") or []
            )
            missing_env = ", ".join(_markdown_cell(name) for name in item.get("sample_missing_secret_env_vars") or [])
            api_base_cell = (
                f"{item.get('sample_api_base_count')}/{item.get('api_base_count')}"
                if item.get("api_base_count") is not None
                else ""
            )
            lines.append(
                f"| `{_markdown_cell(item.get('source_host'))}` | `{_markdown_cell(item.get('probe_status'))}` | `{_markdown_cell(item.get('auth_type'))}` | `{_markdown_cell(item.get('reason'))}` | {item.get('source_count')} | {_int(item.get('credential_rule_candidate_source_count'))} | {_int(item.get('endpoint_discovery_needed_source_count'))} | {api_base_cell} | {_markdown_cell(_credential_market_cell(item))} | {item.get('credential_configured_source_count')} | {item.get('credential_secret_ready_source_count')} | {item.get('credential_config_missing_source_count')} | {item.get('credential_secret_missing_source_count')} | {samples} | {missing_samples} | {missing_env} |"
            )
        omitted_group_count = len(credential_groups) - len(displayed_credential_groups)
        if omitted_group_count > 0:
            lines.append(
                f"\n_{omitted_group_count} additional credential/onboarding group(s) omitted from markdown; use `--format credential-backlog-json` or `--format credential-api-bases-json` for the complete machine-readable export._"
            )
    if retest.get("uncovered_unchecked_clusters"):
        lines.extend(
            [
                "",
                "## Retest Source Discovery Backlog",
                "",
                "| Host | Classification | Results | Status codes | Sample API bases | Sample payers |",
                "| --- | --- | ---: | --- | --- | --- |",
            ]
        )
        for item in retest.get("uncovered_unchecked_clusters") or []:
            api_bases = ", ".join(_markdown_cell(value) for value in item.get("sample_api_bases") or [])
            payers = ", ".join(_markdown_cell(value) for value in item.get("sample_payers") or [])
            status_codes = ", ".join(str(value) for value in item.get("sample_status_codes") or [])
            lines.append(
                f"| `{_markdown_cell(item.get('source_host'))}` | `{_markdown_cell(item.get('classification'))}` | {item.get('result_count')} | {status_codes} | {api_bases} | {payers} |"
            )
    if timeout_summary.get("groups"):
        lines.extend(
            [
                "",
                "## Probe Timeout Backlog",
                "",
                "| Host | Auth | Sources | Markets | Sample API bases | Sample payers | Sample errors |",
                "| --- | --- | ---: | --- | --- | --- | --- |",
            ]
        )
        for item in (timeout_summary.get("groups") or [])[:CREDENTIAL_BACKLOG_MARKDOWN_GROUP_LIMIT]:
            api_bases = ", ".join(_markdown_cell(value) for value in item.get("sample_api_bases") or [])
            payers = ", ".join(_markdown_cell(value) for value in item.get("sample_payers") or [])
            errors = ", ".join(_markdown_cell(value) for value in item.get("sample_errors") or [])
            lines.append(
                f"| `{_markdown_cell(item.get('source_host'))}` | `{_markdown_cell(item.get('auth_type'))}` | {item.get('source_count')} | {_markdown_cell(_credential_market_cell(item))} | {api_bases} | {payers} | {errors} |"
            )
    if ptg_network.get("samples"):
        lines.extend(
            [
                "",
                "## PTG/FHIR Network Name Overlap Gaps",
                "",
                "| Source | Provider Directory Network | Plan Pairs | Sample PTG Networks |",
                "| --- | --- | ---: | --- |",
            ]
        )
        for item in ptg_network["samples"]:
            samples = ", ".join(_markdown_cell(name) for name in item.get("sample_ptg_network_names") or [])
            lines.append(
                f"| {_markdown_cell(item.get('provider_directory_org_name') or item.get('provider_directory_source_id'))} | `{_markdown_cell(item.get('provider_directory_network_name'))}` | {item.get('plan_pair_count')} | {samples} |"
            )
    if network.get("top_unresolved_refs"):
        lines.extend(["", "## Top Unresolved Network Refs", "", "| Source | Ref | Count |", "| --- | --- | ---: |"])
        for item in network["top_unresolved_refs"]:
            lines.append(
                f"| {item.get('org_name') or item.get('source_id')} | `{item.get('ref')}` | {item.get('reference_count')} |"
            )
    if plan_network.get("samples"):
        lines.extend(
            [
                "",
                "## Provider Directory Plan/Network Context",
                "",
                "| Source | InsurancePlans | Network Refs | Resolved Refs | Network Names |",
                "| --- | ---: | ---: | ---: | --- |",
            ]
        )
        for item in plan_network["samples"]:
            names = ", ".join(
                _markdown_cell(name) for name in item.get("sample_resolved_network_names") or []
            )
            lines.append(
                f"| {_markdown_cell(item.get('org_name') or item.get('source_id'))} | "
                f"{item.get('insurance_plan_rows')} | {item.get('network_ref_rows')} | "
                f"{item.get('resolved_network_refs')} | {names} |"
            )
    if network_catalog.get("samples"):
        lines.extend(
            [
                "",
                "## Provider Directory Network Catalog",
                "",
                "| Source | Networks | Refs | InsurancePlan refs | Role refs | Affiliation refs | Sample networks |",
                "| --- | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for item in network_catalog["samples"]:
            names = ", ".join(_markdown_cell(name) for name in item.get("sample_network_names") or [])
            lines.append(
                f"| {_markdown_cell(item.get('source_org_name') or item.get('source_id'))} | "
                f"{item.get('network_count')} | {item.get('distinct_ref_count')} | "
                f"{item.get('insurance_plan_ref_count')} | {item.get('practitioner_role_ref_count')} | "
                f"{item.get('organization_affiliation_ref_count')} | {names} |"
            )
    if advertised_gaps.get("resources"):
        rows = [
            item
            for item in advertised_gaps["resources"]
            if _int(item.get("advertised_source_count"))
        ]
        if rows:
            lines.extend(
                [
                    "",
                    "## Advertised Resource Import Gaps",
                    "",
                    "| Resource | Advertised Sources | Sources With Rows | Advertised Without Rows | Auth-Blocked Without Rows | Resource Errors |",
                    "| --- | ---: | ---: | ---: | ---: | --- |",
                ]
            )
            for item in rows:
                errors = ", ".join(
                    f"{error}={count}"
                    for error, count in sorted((item.get("resource_error_counts") or {}).items())
                )
                lines.append(
                    f"| `{item.get('resource_type')}` | {item.get('advertised_source_count')} | {item.get('source_with_rows_count')} | {item.get('advertised_without_rows_count')} | {item.get('auth_blocked_without_rows_count')} | `{errors}` |"
                )
    valid_zero_rows = report.get("valid_sources_without_resource_rows") or {}
    if valid_zero_rows.get("samples"):
        error_counts = ", ".join(
            f"{error}={count}" for error, count in sorted((valid_zero_rows.get("resource_error_counts") or {}).items())
        )
        lines.extend(
            [
                "",
                "## Valid Metadata With No Imported Rows",
                "",
                f"- resource-auth-required sources: `{valid_zero_rows.get('resource_auth_required_source_count')}` / `{valid_zero_rows.get('source_count')}`",
                f"- resource error counts: `{error_counts}`",
                "",
                "| Source | Plan | Auth | API Base | Resource Errors |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for item in valid_zero_rows["samples"]:
            diagnostic = item.get("last_resource_import") or {}
            resources = diagnostic.get("resources") if isinstance(diagnostic, dict) else {}
            errors = []
            if isinstance(resources, dict):
                errors = sorted(
                    {
                        str(entry.get("error"))
                        for entry in resources.values()
                        if isinstance(entry, dict) and entry.get("error")
                    }
                )
            error_text = ", ".join(errors) if errors else ""
            lines.append(
                f"| {item.get('org_name') or item.get('source_id')} | {item.get('plan_name') or ''} | `{item.get('auth_type') or ''}` | `{item.get('canonical_api_base') or ''}` | `{error_text}` |"
            )
    if (
        source_coverage.get("catalog_only_samples")
        or source_coverage.get("location_without_unified_samples")
        or source_coverage.get("organization_address_without_unified_samples")
    ):
        lines.extend(["", "## Source Resource/Search Coverage", ""])
        if source_coverage.get("catalog_only_samples"):
            lines.extend(
                [
                    f"- catalog-only sources: `{source_coverage.get('catalog_only_source_count')}` / `{source_coverage.get('source_count')}`",
                    "",
                    "| Source | Probe | Validation | Auth | API Base |",
                    "| --- | --- | --- | --- | --- |",
                ]
            )
            for item in source_coverage["catalog_only_samples"]:
                lines.append(
                    f"| {_markdown_cell(item.get('org_name') or item.get('source_id'))} | "
                    f"`{_markdown_cell(item.get('last_probe_status'))}` | "
                    f"`{_markdown_cell(item.get('last_validated_status'))}` | "
                    f"`{_markdown_cell(item.get('auth_type'))}` | "
                    f"`{_markdown_cell(item.get('canonical_api_base'))}` |"
                )
        if source_coverage.get("location_without_unified_samples"):
            lines.extend(
                [
                    "",
                    f"- Location sources without unified projection: `{source_coverage.get('sources_with_location_rows_without_unified_rows')}`",
                    "",
                    "| Source | Probe | Auth | Reason | Location Rows | Keyed Location Rows | API Base |",
                    "| --- | --- | --- | --- | ---: | ---: | --- |",
                ]
            )
            for item in source_coverage["location_without_unified_samples"]:
                lines.append(
                    f"| {_markdown_cell(item.get('org_name') or item.get('source_id'))} | "
                    f"`{_markdown_cell(item.get('last_probe_status'))}` | "
                    f"`{_markdown_cell(item.get('auth_type'))}` | "
                    f"`{_markdown_cell(item.get('projection_gap_reason'))}` | "
                    f"{item.get('location_rows')} | {item.get('keyed_location_rows')} | "
                    f"`{_markdown_cell(item.get('canonical_api_base'))}` |"
                )
        if source_coverage.get("organization_address_without_unified_samples"):
            lines.extend(
                [
                    "",
                    f"- valid-NPI Organization address sources without unified projection: `{source_coverage.get('sources_with_valid_npi_organization_address_rows_without_unified_rows')}`",
                    "",
                    "| Source | Probe | Auth | Organization Address Rows | Valid-NPI Address Rows | API Base |",
                    "| --- | --- | --- | ---: | ---: | --- |",
                ]
            )
            for item in source_coverage["organization_address_without_unified_samples"]:
                lines.append(
                    f"| {_markdown_cell(item.get('org_name') or item.get('source_id'))} | "
                    f"`{_markdown_cell(item.get('last_probe_status'))}` | "
                    f"`{_markdown_cell(item.get('auth_type'))}` | "
                    f"{item.get('organization_address_rows')} | "
                    f"{item.get('valid_npi_organization_address_rows')} | "
                    f"`{_markdown_cell(item.get('canonical_api_base'))}` |"
                )
    if report.get("top_source_yield"):
        lines.extend(["", "## Top Source Yield", "", "| Source | Probe | Rows | Counts |", "| --- | --- | ---: | --- |"])
        for item in report["top_source_yield"]:
            lines.append(
                f"| {item.get('org_name') or item.get('source_id')} | `{item.get('last_probe_status')}` | {item.get('resource_rows')} | `{json.dumps(item.get('resource_counts'), sort_keys=True)}` |"
            )
    if alias_fanout.get("resources"):
        lines.extend(
            [
                "",
                "## Alias Fan-Out",
                "",
                "| Resource | API Base | Sources | Source Rows | Distinct Remote IDs | Excess Rows | Ratio | Sample |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for resource in alias_fanout["resources"]:
            for item in resource.get("samples") or []:
                sample = item.get("sample_org_name") or item.get("sample_plan_name") or ""
                lines.append(
                    f"| `{resource.get('resource_type')}` | `{_markdown_cell(item.get('api_base'))}` | {item.get('source_count')} | {item.get('source_resource_rows')} | {item.get('distinct_resource_ids')} | {item.get('excess_source_resource_rows')} | {item.get('fanout_ratio')} | {_markdown_cell(sample)} |"
                )
    if canonical_resources.get("resources"):
        lines.extend(
            [
                "",
                "## Canonical Resource Storage",
                "",
                "| Resource | Canonical rows | Source edges | Edge surplus | Sources | API bases |",
                "| --- | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for item in canonical_resources["resources"]:
            lines.append(
                f"| `{item.get('resource_type')}` | {item.get('canonical_rows')} | {item.get('source_edge_rows')} | {item.get('edge_surplus_rows')} | {item.get('source_count')} | {item.get('canonical_api_base_count')} |"
            )
    lines.append("")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default=os.getenv("HLTHPRT_DB_HOST") or "127.0.0.1")
    parser.add_argument("--port", type=int, default=_env_int("HLTHPRT_DB_PORT", 5440))
    parser.add_argument("--database", default=os.getenv("HLTHPRT_DB_DATABASE") or "healthporta")
    parser.add_argument("--user", default=os.getenv("HLTHPRT_DB_USER") or os.getenv("USER") or "postgres")
    parser.add_argument("--password", default=os.getenv("HLTHPRT_DB_PASSWORD") or "")
    parser.add_argument("--schema", default=os.getenv("HLTHPRT_DB_SCHEMA") or "mrf")
    parser.add_argument(
        "--credential-config-file",
        help=(
            "Audit-only credential config file override. Use this to validate a candidate "
            "HLTHPRT_PROVIDER_DIRECTORY_CREDENTIALS_JSON file without changing worker env."
        ),
    )
    parser.add_argument(
        "--retest-results-path",
        help=(
            "Optional provider-directory-db retest_results.json snapshot. When provided, "
            "the audit checks whether importable and credential-gated retest endpoints "
            "are covered by current provider_directory_source bases or known override redirects."
        ),
    )
    parser.add_argument(
        "--ptg-plan-id",
        help=(
            "Limit PTG-associated unified address and corroboration counts to one plan id. "
            "Matches entity_address_unified.ptg_plan_array and corroboration plan ids."
        ),
    )
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument(
        "--statement-timeout-ms",
        type=int,
        default=_env_int("HLTHPRT_PROVIDER_DIRECTORY_AUDIT_STATEMENT_TIMEOUT_MS", 0),
        help="Optional PostgreSQL statement_timeout in milliseconds for each audit query.",
    )
    parser.add_argument(
        "--require-serving-ready",
        action="store_true",
        help=(
            "Exit non-zero after writing the requested report unless the Provider Directory "
            "serving readiness gate is ready. Use this as a post-import/schedule-chain gate."
        ),
    )
    parser.add_argument(
        "--fast-serving-readiness",
        action="store_true",
        help=(
            "Use bounded existence probes for Provider Directory searchable-address readiness "
            "instead of exact unified-address counts. This keeps readiness checks safe near API pods; "
            "omit it for exact reporting from a worker/dev host."
        ),
    )
    parser.add_argument(
        "--pod-safe",
        action="store_true",
        help=(
            "Run only source/capability/resource table checks that are safe to execute "
            "inside the API pod during active imports. This enables the expensive-section "
            "skip flags; use a full audit from a worker/dev host for unified, PTG, network, "
            "advertised-resource, and canonical-resource aggregate checks."
        ),
    )
    parser.add_argument(
        "--skip-unified",
        action="store_true",
        help="Skip entity_address_unified counts for pod-safe source/resource coverage checks.",
    )
    parser.add_argument(
        "--skip-network-resolution",
        action="store_true",
        help="Skip the heavier unresolved network-ref scan for quick checks during active imports.",
    )
    parser.add_argument(
        "--skip-practitioner-role-reimport-gap-summary",
        action="store_true",
        help="Skip the heavier per-source PractitionerRole reimport/projection gap scan.",
    )
    parser.add_argument(
        "--skip-ptg",
        action="store_true",
        help="Skip PTG pricing/corroboration scans for quick Provider Directory-only gates.",
    )
    parser.add_argument(
        "--skip-ptg-corroboration",
        action="store_true",
        help="Skip the full Provider Directory/PTG corroboration aggregate while keeping cheaper PTG checks.",
    )
    parser.add_argument(
        "--skip-ptg-network-overlap",
        action="store_true",
        help="Skip PTG serving network_names to FHIR network-name overlap checks.",
    )
    parser.add_argument(
        "--force-ptg-live-view-scans",
        action="store_true",
        help="Allow exact PTG/FHIR aggregates against the live corroboration view. This can be slow on dev.",
    )
    parser.add_argument(
        "--skip-top-source-yield",
        action="store_true",
        help="Skip per-source resource-yield ranking.",
    )
    parser.add_argument(
        "--skip-advertised-resource-gaps",
        action="store_true",
        help="Skip advertised resource/source gap checks.",
    )
    parser.add_argument(
        "--skip-valid-zero-row-sources",
        action="store_true",
        help="Skip valid-metadata-with-zero-resource-row sample checks.",
    )
    parser.add_argument(
        "--skip-canonical-resource-summary",
        action="store_true",
        help="Skip canonical-resource/source-edge fan-out summary; this can be slow on dev-scale catalogs.",
    )
    parser.add_argument(
        "--format",
        choices=(
            "json",
            "markdown",
            "credential-backlog-json",
            "credential-api-bases-json",
            "credential-config-template-json",
            "credential-priority-json",
        ),
        default="json",
    )
    args = parser.parse_args(argv)
    if args.pod_safe:
        args.skip_unified = True
        args.fast_serving_readiness = True
        args.skip_network_resolution = True
        args.skip_practitioner_role_reimport_gap_summary = True
        args.skip_ptg = True
        args.skip_top_source_yield = True
        args.skip_advertised_resource_gaps = True
        args.skip_valid_zero_row_sources = True
        args.skip_canonical_resource_summary = True
    return args


def _serving_readiness_exit_code(report: dict[str, Any], *, require_serving_ready: bool) -> int:
    if not require_serving_ready:
        return 0
    readiness = report.get("serving_readiness") if isinstance(report, dict) else None
    if isinstance(readiness, dict) and readiness.get("status") == "ready":
        return 0
    return 1


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = asyncio.run(build_report(args))
    if args.format == "markdown":
        sys.stdout.write(render_markdown(report))
    elif args.format == "credential-backlog-json":
        json.dump(_credential_backlog_export(report), sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    elif args.format == "credential-api-bases-json":
        json.dump(_credential_api_base_targets_export(report), sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    elif args.format == "credential-config-template-json":
        json.dump(_credential_config_template_export(report), sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    elif args.format == "credential-priority-json":
        json.dump(_credential_priority_export(report), sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    else:
        json.dump(report, sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    exit_code = _serving_readiness_exit_code(
        report,
        require_serving_ready=bool(args.require_serving_ready),
    )
    if exit_code:
        readiness = report.get("serving_readiness") if isinstance(report, dict) else None
        status = readiness.get("status") if isinstance(readiness, dict) else "missing"
        required_fail_count = readiness.get("required_fail_count") if isinstance(readiness, dict) else "unknown"
        sys.stderr.write(
            "Provider Directory serving readiness gate failed "
            f"(status={status}, required_fail_count={required_fail_count}).\n"
        )
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
