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

  rtk ./venv314/bin/python scripts/research/provider_directory_coverage_audit.py \
    --host 127.0.0.1 --port 5440 --database healthporta --schema mrf --format markdown
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
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
    row["valid_non_fhir_pct"] = _pct(_int(row.get("live_valid_non_fhir_count")), source_count)
    return row


async def _credential_onboarding_backlog(
    conn: asyncpg.Connection,
    schema: str,
    *,
    sample_limit: int,
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
                   coalesce(NULLIF(auth_type, ''), '') AS auth_type,
                   lower(coalesce(auth_type, '')) AS auth_type_norm,
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
                       WHEN last_probe_status = 'auth_required' THEN 'auth_required'
                       WHEN last_probe_status = 'valid_non_fhir'
                            AND source_host = ANY({gateway_hosts})
                           THEN 'onboarding_gateway'
                       WHEN last_probe_status = 'valid_non_fhir'
                            AND EXISTS (
                                SELECT 1
                                  FROM unnest({credential_markers}) AS marker(value)
                                 WHERE auth_type_norm LIKE '%' || marker.value || '%'
                            )
                           THEN 'credentialed_non_fhir'
                       ELSE 'unknown'
                   END AS reason
              FROM src
             WHERE last_probe_status = 'auth_required'
                OR (
                    last_probe_status = 'valid_non_fhir'
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
               last_probe_status AS probe_status,
               auth_type,
               reason
          FROM blocked
         ORDER BY lower(org_name), lower(coalesce(plan_name, '')), source_id
        """
    )
    config = _load_credentials_config()
    groups_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    configured_source_count = 0
    missing_source_count = 0
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
                "sample_payers": [],
                "sample_missing_credential_payers": [],
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
        if len(group["sample_payers"]) < sample_limit:
            group["sample_payers"].append(payer_label or item.get("source_id"))

        spec = _credential_spec_for_source(item, config)
        if _credential_spec_has_material(spec):
            configured_source_count += 1
            group["credential_configured_source_count"] += 1
        else:
            missing_source_count += 1
            group["credential_config_missing_source_count"] += 1
            if len(group["sample_missing_credential_payers"]) < sample_limit:
                group["sample_missing_credential_payers"].append(payer_label or item.get("source_id"))

    groups = sorted(
        groups_by_key.values(),
        key=lambda group: (
            -_int(group.get("source_count")),
            str(group.get("source_host")),
            str(group.get("probe_status")),
            str(group.get("auth_type")),
            str(group.get("reason")),
        ),
    )[:50]
    return {
        "available": True,
        "blocked_source_count": len(rows),
        "credential_config_available": bool(config),
        "credential_configured_source_count": configured_source_count,
        "credential_config_missing_source_count": missing_source_count,
        "group_count": len(groups),
        "groups": groups,
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


async def _resource_summary(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    summary: dict[str, Any] = {}
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
        row["available"] = True
        row["columns"] = columns
        row_count = _int(row.get("row_count"))
        if columns["address_key"]:
            row["address_key_pct"] = _pct(_int(row.get("address_key_count")), row_count)
        summary[table] = row
    return summary


async def _unified_summary(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    if not await _relation_exists(conn, schema, "entity_address_unified"):
        return {"available": False}
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
        summary["ptg_corroboration"] = {"available": True, **row}
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
    network_ref_match = _sql_ref_matches_resource("refs.ref", "Organization", "org.resource_id")
    row = await _fetch_mapping(
        conn,
        f"""
        WITH refs AS (
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_practitioner_role")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_organization_affiliation")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_insurance_plan")}
        ),
        resolved AS (
            SELECT refs.source_id, refs.ref, org.resource_id, org.name
              FROM refs
              LEFT JOIN {_qt(schema, "provider_directory_organization")} org
                ON org.source_id = refs.source_id
               AND {network_ref_match}
        )
        SELECT
            count(*)::bigint AS network_ref_rows,
            count(DISTINCT source_id || '|' || ref)::bigint AS distinct_network_refs,
            count(DISTINCT source_id || '|' || ref) FILTER (WHERE resource_id IS NOT NULL)::bigint
                AS resolved_network_refs,
            count(DISTINCT source_id || '|' || ref) FILTER (WHERE resource_id IS NULL)::bigint
                AS unresolved_network_refs
          FROM resolved
        """,
    )
    unresolved = await conn.fetch(
        f"""
        WITH refs AS (
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_practitioner_role")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_organization_affiliation")}
            UNION ALL
            SELECT source_id, jsonb_array_elements_text(COALESCE(network_refs::jsonb, '[]'::jsonb)) AS ref
              FROM {_qt(schema, "provider_directory_insurance_plan")}
        )
        SELECT refs.source_id, src.org_name, refs.ref, count(*)::bigint AS reference_count
          FROM refs
          LEFT JOIN {_qt(schema, "provider_directory_organization")} org
            ON org.source_id = refs.source_id
           AND {network_ref_match}
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
        gaps.append(
            f"{unified['provider_directory_null_key_rows']} Provider Directory unified-address rows still lack address_key."
        )
    if unified.get("available"):
        provider_directory_rows = _int(unified.get("provider_directory_rows"))
        source_record_id_rows = _int(unified.get("provider_directory_source_record_id_rows"))
        if provider_directory_rows and source_record_id_rows < provider_directory_rows:
            missing = provider_directory_rows - source_record_id_rows
            gaps.append(
                f"{missing} Provider Directory unified-address rows lack retained FHIR source record IDs."
            )
        if _int(unified.get("provider_directory_country_001_rows")):
            gaps.append(
                f"{unified['provider_directory_country_001_rows']} Provider Directory unified-address rows still expose country_code `001`."
            )
    network = report.get("network_resolution_summary") or {}
    if network.get("available") and _int(network.get("unresolved_network_refs")):
        gaps.append(
            f"{network['unresolved_network_refs']} distinct Provider Directory network refs are unresolved to FHIR Organization names."
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
    credential_backlog = report.get("credential_onboarding_backlog") or {}
    if credential_backlog.get("available") and _int(credential_backlog.get("credential_config_missing_source_count")):
        gaps.append(
            f"{credential_backlog['credential_config_missing_source_count']} Provider Directory auth/onboarding source(s) do not match a configured credential rule."
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
            ),
            "capability_status_counts": await _capability_status_counts(conn, schema),
            "resource_summary": await _resource_summary(conn, schema),
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
                else await _unified_summary(conn, schema)
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
        }
        report["gaps"] = _derive_gaps(report)
        return report
    finally:
        await conn.close()


def render_markdown(report: dict[str, Any]) -> str:
    source = report.get("source_summary") or {}
    unified = report.get("unified_summary") or {}
    network = report.get("network_resolution_summary") or {}
    ptg = report.get("ptg_summary") or {}
    credential_backlog = report.get("credential_onboarding_backlog") or {}
    alias_fanout = report.get("alias_fanout_summary") or {}
    canonical_resources = report.get("canonical_resource_summary") or {}
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
                f"- non-FHIR credential/gateway responses: `{source.get('live_credential_or_gateway_non_fhir_count')}` / `{source.get('live_valid_non_fhir_count')}` valid_non_fhir",
                f"- sources with API base: `{source.get('api_base_count')}` ({source.get('api_base_pct')}%)",
            ]
        )
    if credential_backlog.get("available"):
        lines.append(
            f"- credential/onboarding backlog: `{credential_backlog.get('blocked_source_count')}` source(s) across `{credential_backlog.get('group_count')}` group(s)"
        )
        lines.append(
            f"- credential config coverage: `{credential_backlog.get('credential_configured_source_count')}` configured / "
            f"`{credential_backlog.get('blocked_source_count')}` gated source(s); "
            f"missing config `{credential_backlog.get('credential_config_missing_source_count')}`"
        )
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
                "| Host | Status | Auth | Reason | Sources | Configured | Missing Config | Sample payers | Sample missing credentials |",
                "| --- | --- | --- | --- | ---: | ---: | ---: | --- | --- |",
            ]
        )
        for item in credential_backlog["groups"]:
            samples = ", ".join(_markdown_cell(payer) for payer in item.get("sample_payers") or [])
            missing_samples = ", ".join(
                _markdown_cell(payer) for payer in item.get("sample_missing_credential_payers") or []
            )
            lines.append(
                f"| `{_markdown_cell(item.get('source_host'))}` | `{_markdown_cell(item.get('probe_status'))}` | `{_markdown_cell(item.get('auth_type'))}` | `{_markdown_cell(item.get('reason'))}` | {item.get('source_count')} | {item.get('credential_configured_source_count')} | {item.get('credential_config_missing_source_count')} | {samples} | {missing_samples} |"
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
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args(argv)
    if args.pod_safe:
        args.skip_unified = True
        args.skip_network_resolution = True
        args.skip_ptg = True
        args.skip_top_source_yield = True
        args.skip_advertised_resource_gaps = True
        args.skip_valid_zero_row_sources = True
        args.skip_canonical_resource_summary = True
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = asyncio.run(build_report(args))
    if args.format == "markdown":
        sys.stdout.write(render_markdown(report))
    else:
        json.dump(report, sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
