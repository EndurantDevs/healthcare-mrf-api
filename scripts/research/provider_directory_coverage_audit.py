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


def _pct(numerator: int, denominator: int) -> float:
    return round((float(numerator) / float(denominator) * 100.0), 2) if denominator else 0.0


def _int(value: Any) -> int:
    return int(value or 0)


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


async def _connect(args: argparse.Namespace) -> asyncpg.Connection:
    return await asyncpg.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
    )


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
    row = await _fetch_mapping(
        conn,
        f"""
        SELECT
            count(*)::bigint AS source_count,
            count(*) FILTER (WHERE canonical_api_base IS NOT NULL)::bigint AS api_base_count,
            count(*) FILTER (WHERE last_probe_status = 'valid')::bigint AS live_valid_count,
            count(*) FILTER (WHERE last_probe_status = 'auth_required')::bigint AS live_auth_required_count,
            count(*) FILTER (WHERE last_probe_status IS NULL)::bigint AS never_probed_count,
            count(*) FILTER (WHERE auth_type IN ('open', 'none', '') OR auth_type IS NULL)::bigint AS open_or_none_auth_count,
            count(*) FILTER (WHERE is_medicare_advantage IS TRUE)::bigint AS medicare_advantage_count,
            count(*) FILTER (WHERE is_medicaid_mco IS TRUE)::bigint AS medicaid_mco_count,
            count(*) FILTER (WHERE is_qhp IS TRUE)::bigint AS qhp_count,
            max(last_probed_at) AS last_probed_at
          FROM {_qt(schema, "provider_directory_source")}
        """,
    )
    source_count = _int(row.get("source_count"))
    row["available"] = True
    row["api_base_pct"] = _pct(_int(row.get("api_base_count")), source_count)
    row["live_valid_pct"] = _pct(_int(row.get("live_valid_count")), source_count)
    row["auth_required_pct"] = _pct(_int(row.get("live_auth_required_count")), source_count)
    return row


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
            )::bigint AS provider_directory_null_key_rows
          FROM {_qt(schema, "entity_address_unified")}
        """,
    )
    total = _int(row.get("provider_directory_rows"))
    row["available"] = True
    row["provider_directory_keyed_pct"] = _pct(_int(row.get("provider_directory_keyed_rows")), total)
    row["provider_directory_phone_pct"] = _pct(_int(row.get("provider_directory_phone_rows")), total)
    return row


async def _ptg_summary(conn: asyncpg.Connection, schema: str) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    if await _relation_exists(conn, schema, "ptg_address"):
        row = await _fetch_mapping(
            conn,
            f"""
            SELECT
                count(*)::bigint AS ptg_address_rows,
                count(DISTINCT source_key)::bigint AS ptg_source_count,
                count(DISTINCT npi)::bigint AS ptg_npi_count,
                count(*) FILTER (WHERE address_key IS NOT NULL)::bigint AS ptg_keyed_address_rows
              FROM {_qt(schema, "ptg_address")}
            """,
        )
        row["ptg_keyed_address_pct"] = _pct(_int(row.get("ptg_keyed_address_rows")), _int(row.get("ptg_address_rows")))
        summary["ptg_address"] = {"available": True, **row}
    else:
        summary["ptg_address"] = {"available": False}
    view = "ptg_provider_directory_address_corroboration"
    if await _relation_exists(conn, schema, view):
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
            """,
        )
        summary["ptg_corroboration"] = {"available": True, **row}
    else:
        summary["ptg_corroboration"] = {"available": False}
    return summary


async def _network_resolution_summary(conn: asyncpg.Connection, schema: str, *, sample_limit: int) -> dict[str, Any]:
    required = (
        "provider_directory_practitioner_role",
        "provider_directory_organization_affiliation",
        "provider_directory_insurance_plan",
        "provider_directory_organization",
    )
    if not all([await _relation_exists(conn, schema, table) for table in required]):
        return {"available": False, "top_unresolved_refs": []}
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
               AND refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)
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
           AND refs.ref IN (org.resource_id, 'Organization/' || org.resource_id)
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
    for sample in samples:
        sample["last_resource_import"] = _json_object(sample.get("last_resource_import"))
    return {
        "available": True,
        "source_count": _int(count),
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
    network = report.get("network_resolution_summary") or {}
    if network.get("available") and _int(network.get("unresolved_network_refs")):
        gaps.append(
            f"{network['unresolved_network_refs']} distinct Provider Directory network refs are unresolved to FHIR Organization names."
        )
    valid_zero_rows = report.get("valid_sources_without_resource_rows") or {}
    if valid_zero_rows.get("available") and _int(valid_zero_rows.get("source_count")):
        gaps.append(
            f"{valid_zero_rows['source_count']} Provider Directory source(s) have valid unauthenticated metadata but no imported resource rows."
        )
    ptg = (report.get("ptg_summary") or {}).get("ptg_corroboration") or {}
    if ptg.get("available") and _int(ptg.get("network_context_rows")) and not _int(ptg.get("resolved_network_match_rows")):
        gaps.append("PTG-overlap Provider Directory rows carry network refs, but none currently resolve to network-name matches.")
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
            "source_summary": await _source_summary(conn, schema),
            "capability_status_counts": await _capability_status_counts(conn, schema),
            "resource_summary": await _resource_summary(conn, schema),
            "unified_summary": await _unified_summary(conn, schema),
            "ptg_summary": await _ptg_summary(conn, schema),
            "network_resolution_summary": network_resolution_summary,
            "top_source_yield": await _top_source_yield(conn, schema, sample_limit=args.sample_limit),
            "valid_sources_without_resource_rows": await _valid_sources_without_resource_rows(
                conn,
                schema,
                sample_limit=args.sample_limit,
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
    lines = [
        "# Provider Directory Coverage Audit",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- schema: `{report.get('schema')}`",
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
                f"- sources with API base: `{source.get('api_base_count')}` ({source.get('api_base_pct')}%)",
            ]
        )
    if unified.get("available"):
        lines.extend(
            [
                f"- unified Provider Directory rows: `{unified.get('provider_directory_rows')}`",
                f"- keyed Provider Directory rows: `{unified.get('provider_directory_keyed_rows')}` ({unified.get('provider_directory_keyed_pct')}%)",
                f"- phone Provider Directory rows: `{unified.get('provider_directory_phone_rows')}` ({unified.get('provider_directory_phone_pct')}%)",
            ]
        )
    ptg_corr = ptg.get("ptg_corroboration") or {}
    if ptg_corr.get("available"):
        lines.extend(
            [
                f"- PTG corroboration rows: `{ptg_corr.get('corroboration_rows')}`",
                f"- PTG plan-context matches: `{ptg_corr.get('plan_context_match_rows')}`",
                f"- resolved network-name match rows: `{ptg_corr.get('resolved_network_match_rows')}`",
            ]
        )
    if network.get("available"):
        lines.append(
            f"- resolved network refs: `{network.get('resolved_network_refs')}` / `{network.get('distinct_network_refs')}` ({network.get('resolved_network_ref_pct')}%)"
        )
    if report.get("gaps"):
        lines.extend(["", "## Gaps", ""])
        lines.extend(f"- {gap}" for gap in report["gaps"])
    if report.get("capability_status_counts"):
        lines.extend(["", "## Capability Status", "", "| Status | Count |", "| --- | ---: |"])
        for item in report["capability_status_counts"]:
            lines.append(f"| `{item.get('probe_status')}` | {item.get('count')} |")
    if network.get("top_unresolved_refs"):
        lines.extend(["", "## Top Unresolved Network Refs", "", "| Source | Ref | Count |", "| --- | --- | ---: |"])
        for item in network["top_unresolved_refs"]:
            lines.append(
                f"| {item.get('org_name') or item.get('source_id')} | `{item.get('ref')}` | {item.get('reference_count')} |"
            )
    valid_zero_rows = report.get("valid_sources_without_resource_rows") or {}
    if valid_zero_rows.get("samples"):
        lines.extend(
            [
                "",
                "## Valid Metadata With No Imported Rows",
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
    parser.add_argument("--sample-limit", type=int, default=10)
    parser.add_argument(
        "--skip-network-resolution",
        action="store_true",
        help="Skip the heavier unresolved network-ref scan for quick checks during active imports.",
    )
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    return parser.parse_args(argv)


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
