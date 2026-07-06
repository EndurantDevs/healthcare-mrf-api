#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare an old PTG2 snapshot with a rebuilt snapshot before cleanup."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Any


def _bootstrap_import_path() -> None:
    root = Path(__file__).resolve().parents[2]
    for path in (root, Path("/opt")):
        if path.exists():
            sys.path.insert(0, str(path))


_bootstrap_import_path()


def _db_connection():
    from db.connection import db

    return db


def _sql_text(statement: str):
    from sqlalchemy import text as sqlalchemy_text

    return sqlalchemy_text(statement)


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _as_json(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _qident(value: str) -> str:
    if not IDENT_RE.fullmatch(value):
        raise ValueError(f"unsafe identifier: {value!r}")
    return '"' + value.replace('"', '""') + '"'


def _qtable(value: str | None) -> str:
    if not value:
        raise ValueError("missing schema-qualified table")
    parts = str(value).split(".")
    if len(parts) != 2:
        raise ValueError(f"expected schema-qualified table: {value!r}")
    return ".".join(_qident(part) for part in parts)


def _sidecar_summary(serving_index: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = serving_index.get("artifacts") if isinstance(serving_index.get("artifacts"), dict) else {}
    sidecars = artifacts.get("sidecars") if isinstance(artifacts, dict) else []
    rows = []
    for item in sidecars if isinstance(sidecars, list) else []:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "name": item.get("name") or item.get("kind"),
                "byte_count": item.get("byte_count") or item.get("bytes"),
                "row_count": item.get("row_count") or item.get("rows"),
                "sha256": item.get("sha256") or item.get("content_sha256"),
            }
        )
    return sorted(rows, key=lambda row: str(row.get("name")))


def _serving_index_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    serving_index = manifest.get("serving_index") if isinstance(manifest.get("serving_index"), dict) else manifest
    return serving_index if isinstance(serving_index, dict) else {}


def _snapshot_table_names(serving_index: dict[str, Any]) -> dict[str, Any]:
    return {
        "serving": serving_index.get("table"),
        "price_atom": serving_index.get("price_atom_table"),
        "price_atom_dict": serving_index.get("price_atom_dictionary_table"),
        "provider_member": serving_index.get("provider_group_member_table"),
        "provider_location": serving_index.get("provider_group_location_table"),
        "provider_component": serving_index.get("provider_set_component_table"),
        "provider_rate_scope": serving_index.get("provider_group_rate_scope_table"),
        "provider_set_dict": serving_index.get("provider_set_dictionary_table"),
        "code_count": serving_index.get("code_count_table"),
    }


async def _table_sizes_by_role(session, table_names_by_role: dict[str, Any]) -> dict[str, Any]:
    table_size_by_role = {}
    for role, table_name in table_names_by_role.items():
        if not table_name:
            continue
        size_result = await session.execute(
            _sql_text(
                """
                SELECT pg_total_relation_size(to_regclass(:table_name)) AS total_bytes,
                       pg_relation_size(to_regclass(:table_name)) AS heap_bytes,
                       pg_size_pretty(pg_total_relation_size(to_regclass(:table_name))) AS total_size
                """
            ),
            {"table_name": table_name},
        )
        size_record = size_result.mappings().first()
        table_size_by_role[role] = dict(size_record) if size_record else {}
    return table_size_by_role


async def _snapshot_info(session, snapshot_id: str) -> dict[str, Any]:
    """Load table, count, and sidecar metadata for one PTG2 snapshot."""
    snapshot_result = await session.execute(
        _sql_text(
            """
            SELECT s.snapshot_id,
                   s.import_run_id,
                   s.import_month::text AS import_month,
                   s.status,
                   s.previous_snapshot_id,
                   s.manifest,
                   r.options,
                   r.report
              FROM mrf.ptg2_snapshot s
              LEFT JOIN mrf.ptg2_import_run r ON r.import_run_id = s.import_run_id
             WHERE s.snapshot_id = :snapshot_id
             LIMIT 1
            """
        ),
        {"snapshot_id": snapshot_id},
    )
    snapshot_record = snapshot_result.mappings().first()
    if snapshot_record is None:
        raise RuntimeError(f"snapshot not found: {snapshot_id}")
    manifest = _as_json(snapshot_record["manifest"])
    options = _as_json(snapshot_record["options"])
    report = _as_json(snapshot_record["report"])
    serving_index = _serving_index_from_manifest(manifest)
    table_names_by_role = _snapshot_table_names(serving_index)
    return {
        "snapshot_id": snapshot_id,
        "import_run_id": snapshot_record["import_run_id"],
        "import_month": snapshot_record["import_month"],
        "status": snapshot_record["status"],
        "previous_snapshot_id": snapshot_record["previous_snapshot_id"],
        "source_key": options.get("source_key") or serving_index.get("source_key"),
        "arch_version": serving_index.get("arch_version") or "legacy_implicit",
        "provider_scope_strategy": serving_index.get("provider_scope_strategy") or "legacy_implicit",
        "materialized_tables": serving_index.get("materialized_tables") or {},
        "serving_rates": manifest.get("serving_rates") or serving_index.get("serving_rates") or report.get("serving_rates"),
        "files_processed": manifest.get("files_processed") or report.get("files_processed"),
        "serving_layout": serving_index.get("serving_table_layout") or "legacy_or_default",
        "price_atom_layout": serving_index.get("price_atom_table_layout") or "wide_or_default",
        "tables": table_names_by_role,
        "sizes": await _table_sizes_by_role(session, table_names_by_role),
        "sidecars": _sidecar_summary(serving_index),
    }


def _serving_exists_sql(new_info: dict[str, Any]) -> str:
    table_names = new_info["tables"]
    serving = _qtable(table_names["serving"])
    if new_info["serving_layout"] == "lean_provider_key_v1":
        code_count = _qtable(table_names["code_count"])
        provider_dict = _qtable(table_names["provider_set_dict"])
        return f"""
            SELECT 1
              FROM {serving} ns
              JOIN {code_count} cc ON cc.code_key = ns.code_key
              JOIN {provider_dict} psd ON psd.provider_set_key = ns.provider_set_key
             WHERE cc.plan_id::text = sample.plan_id
               AND cc.reported_code_system::text IS NOT DISTINCT FROM sample.reported_code_system
               AND cc.reported_code::text = sample.reported_code
               AND psd.provider_set_global_id_128::text = sample.provider_set_global_id_128
               AND ns.provider_count::bigint IS NOT DISTINCT FROM sample.provider_count
               AND ns.price_set_global_id_128::text = sample.price_set_global_id_128
             LIMIT 1
        """
    return f"""
        SELECT 1
          FROM {serving} ns
         WHERE ns.plan_id::text = sample.plan_id
           AND ns.reported_code_system::text IS NOT DISTINCT FROM sample.reported_code_system
           AND ns.reported_code::text = sample.reported_code
           AND ns.provider_set_global_id_128::text = sample.provider_set_global_id_128
           AND ns.provider_count::bigint IS NOT DISTINCT FROM sample.provider_count
           AND ns.price_set_global_id_128::text = sample.price_set_global_id_128
         LIMIT 1
    """


def _serving_sample_sql(info: dict[str, Any], limit: int, sample_pct: float) -> str:
    table_names = info["tables"]
    serving = _qtable(table_names["serving"])
    if info["serving_layout"] == "lean_provider_key_v1":
        code_count = _qtable(table_names["code_count"])
        provider_dict = _qtable(table_names["provider_set_dict"])
        return f"""
            SELECT
                cc.plan_id::text AS plan_id,
                cc.reported_code_system::text AS reported_code_system,
                cc.reported_code::text AS reported_code,
                psd.provider_set_global_id_128::text AS provider_set_global_id_128,
                sample.provider_count::bigint AS provider_count,
                sample.price_set_global_id_128::text AS price_set_global_id_128
              FROM (
                    SELECT code_key, provider_set_key, provider_count, price_set_global_id_128
                      FROM {serving} TABLESAMPLE SYSTEM ({sample_pct}) REPEATABLE (42)
                     LIMIT {limit}
              ) sample
              JOIN {code_count} cc ON cc.code_key = sample.code_key
              JOIN {provider_dict} psd ON psd.provider_set_key = sample.provider_set_key
             WHERE cc.reported_code IS NOT NULL
             LIMIT {limit}
        """
    return f"""
            SELECT
                plan_id::text AS plan_id,
                reported_code_system::text AS reported_code_system,
                reported_code::text AS reported_code,
                provider_set_global_id_128::text AS provider_set_global_id_128,
                provider_count::bigint AS provider_count,
                price_set_global_id_128::text AS price_set_global_id_128
              FROM {serving} TABLESAMPLE SYSTEM ({sample_pct}) REPEATABLE (42)
             WHERE reported_code IS NOT NULL
             LIMIT {limit}
    """


def _price_exists_sql(new_info: dict[str, Any]) -> str:
    table_names = new_info["tables"]
    price_atom = _qtable(table_names["price_atom"])
    if new_info["price_atom_layout"] == "lean_dict_v1":
        dictionary = _qtable(table_names["price_atom_dict"])
        join_sql_parts = []
        for kind, alias, column in (
            ("negotiated_type", "nt", "negotiated_type_key"),
            ("expiration_date", "ed", "expiration_date_key"),
            ("service_code", "sc", "service_code_key"),
            ("billing_class", "bc", "billing_class_key"),
            ("setting", "st", "setting_key"),
            ("billing_code_modifier", "bcm", "billing_code_modifier_key"),
            ("additional_information", "ai", "additional_information_key"),
        ):
            join_sql_parts.append(
                f"JOIN {dictionary} {alias} ON {alias}.attr_kind = '{kind}' AND {alias}.attr_key = np.{column}"
            )
        return f"""
            SELECT 1
              FROM {price_atom} np
              {' '.join(join_sql_parts)}
             WHERE np.price_atom_global_id_128::text = sample.price_atom_global_id_128
               AND coalesce(nt.text_value, '') = sample.negotiated_type
               AND coalesce(np.negotiated_rate::text, '') = sample.negotiated_rate
               AND coalesce(ed.text_value, '') = sample.expiration_date
               AND coalesce(sc.text_array::text, '') = sample.service_code
               AND coalesce(bc.text_value, '') = sample.billing_class
               AND coalesce(st.text_value, '') = sample.setting
               AND coalesce(bcm.text_array::text, '') = sample.billing_code_modifier
               AND coalesce(ai.text_value, '') = sample.additional_information
             LIMIT 1
        """
    return f"""
        SELECT 1
          FROM {price_atom} np
         WHERE np.price_atom_global_id_128::text = sample.price_atom_global_id_128
           AND coalesce(np.negotiated_type::text, '') = sample.negotiated_type
           AND coalesce(np.negotiated_rate::text, '') = sample.negotiated_rate
           AND coalesce(np.expiration_date::text, '') = sample.expiration_date
           AND coalesce(np.service_code::text, '') = sample.service_code
           AND coalesce(np.billing_class::text, '') = sample.billing_class
           AND coalesce(np.setting::text, '') = sample.setting
           AND coalesce(np.billing_code_modifier::text, '') = sample.billing_code_modifier
           AND coalesce(np.additional_information::text, '') = sample.additional_information
         LIMIT 1
    """


def _price_sample_sql(info: dict[str, Any], limit: int, sample_pct: float) -> str:
    table_names = info["tables"]
    price_atom = _qtable(table_names["price_atom"])
    if info["price_atom_layout"] == "lean_dict_v1":
        dictionary = _qtable(table_names["price_atom_dict"])
        join_sql_parts = []
        for kind, alias, column in (
            ("negotiated_type", "nt", "negotiated_type_key"),
            ("expiration_date", "ed", "expiration_date_key"),
            ("service_code", "sc", "service_code_key"),
            ("billing_class", "bc", "billing_class_key"),
            ("setting", "st", "setting_key"),
            ("billing_code_modifier", "bcm", "billing_code_modifier_key"),
            ("additional_information", "ai", "additional_information_key"),
        ):
            join_sql_parts.append(
                f"JOIN {dictionary} {alias} ON {alias}.attr_kind = '{kind}' AND {alias}.attr_key = sample.{column}"
            )
        return f"""
            SELECT
                sample.price_atom_global_id_128::text AS price_atom_global_id_128,
                coalesce(nt.text_value, '') AS negotiated_type,
                coalesce(sample.negotiated_rate::text, '') AS negotiated_rate,
                coalesce(ed.text_value, '') AS expiration_date,
                coalesce(sc.text_array::text, '') AS service_code,
                coalesce(bc.text_value, '') AS billing_class,
                coalesce(st.text_value, '') AS setting,
                coalesce(bcm.text_array::text, '') AS billing_code_modifier,
                coalesce(ai.text_value, '') AS additional_information
              FROM (
                    SELECT *
                      FROM {price_atom} TABLESAMPLE SYSTEM ({sample_pct}) REPEATABLE (43)
                     LIMIT {limit}
              ) sample
              {' '.join(join_sql_parts)}
             LIMIT {limit}
        """
    return f"""
            SELECT
                price_atom_global_id_128::text AS price_atom_global_id_128,
                coalesce(negotiated_type::text, '') AS negotiated_type,
                coalesce(negotiated_rate::text, '') AS negotiated_rate,
                coalesce(expiration_date::text, '') AS expiration_date,
                coalesce(service_code::text, '') AS service_code,
                coalesce(billing_class::text, '') AS billing_class,
                coalesce(setting::text, '') AS setting,
                coalesce(billing_code_modifier::text, '') AS billing_code_modifier,
                coalesce(additional_information::text, '') AS additional_information
              FROM {price_atom} TABLESAMPLE SYSTEM ({sample_pct}) REPEATABLE (43)
             LIMIT {limit}
    """


async def _sample_serving(
    session,
    old_info: dict[str, Any],
    new_info: dict[str, Any],
    limit: int,
    sample_pct: float,
) -> dict[str, Any]:
    await session.execute(_sql_text("DROP TABLE IF EXISTS sample_serving"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE sample_serving ON COMMIT DROP AS
            {_serving_sample_sql(old_info, limit, sample_pct)}
            """
        )
    )
    exists_sql = _serving_exists_sql(new_info)
    await session.execute(_sql_text("DROP TABLE IF EXISTS missing_serving"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE missing_serving ON COMMIT DROP AS
            SELECT sample.*
              FROM sample_serving sample
             WHERE NOT EXISTS ({exists_sql})
            """
        )
    )
    sample_result = await session.execute(
        _sql_text(
            """
            WITH sample_hashes AS (
                SELECT md5(concat_ws('|', plan_id, reported_code_system, reported_code, provider_set_global_id_128, provider_count::text, price_set_global_id_128)) AS row_hash
                  FROM sample_serving
            ), missing_hashes AS (
                SELECT md5(concat_ws('|', plan_id, reported_code_system, reported_code, provider_set_global_id_128, provider_count::text, price_set_global_id_128)) AS row_hash
                  FROM missing_serving
            )
            SELECT (SELECT count(*) FROM sample_serving) AS sampled,
                   (SELECT md5(coalesce(string_agg(row_hash, ',' ORDER BY row_hash), '')) FROM sample_hashes) AS sample_hash,
                   (SELECT count(*) FROM missing_serving) AS missing,
                   (SELECT coalesce(json_agg(row_hash ORDER BY row_hash), '[]'::json) FROM (SELECT row_hash FROM missing_hashes ORDER BY row_hash LIMIT 5) m) AS missing_hashes
            """
        )
    )
    return dict(sample_result.mappings().first())


async def _sample_price_atoms(
    session,
    old_info: dict[str, Any],
    new_info: dict[str, Any],
    limit: int,
    sample_pct: float,
) -> dict[str, Any]:
    await session.execute(_sql_text("DROP TABLE IF EXISTS sample_price_atom"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE sample_price_atom ON COMMIT DROP AS
            {_price_sample_sql(old_info, limit, sample_pct)}
            """
        )
    )
    exists_sql = _price_exists_sql(new_info)
    await session.execute(_sql_text("DROP TABLE IF EXISTS missing_price_atom"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE missing_price_atom ON COMMIT DROP AS
            SELECT sample.*
              FROM sample_price_atom sample
             WHERE NOT EXISTS ({exists_sql})
            """
        )
    )
    sample_result = await session.execute(
        _sql_text(
            """
            WITH sample_hashes AS (
                SELECT md5(concat_ws('|', price_atom_global_id_128, negotiated_type, negotiated_rate, expiration_date, service_code, billing_class, setting, billing_code_modifier, additional_information)) AS row_hash
                  FROM sample_price_atom
            ), missing_hashes AS (
                SELECT md5(concat_ws('|', price_atom_global_id_128, negotiated_type, negotiated_rate, expiration_date, service_code, billing_class, setting, billing_code_modifier, additional_information)) AS row_hash
                  FROM missing_price_atom
            )
            SELECT (SELECT count(*) FROM sample_price_atom) AS sampled,
                   (SELECT md5(coalesce(string_agg(row_hash, ',' ORDER BY row_hash), '')) FROM sample_hashes) AS sample_hash,
                   (SELECT count(*) FROM missing_price_atom) AS missing,
                   (SELECT coalesce(json_agg(row_hash ORDER BY row_hash), '[]'::json) FROM (SELECT row_hash FROM missing_hashes ORDER BY row_hash LIMIT 5) m) AS missing_hashes
            """
        )
    )
    return dict(sample_result.mappings().first())


async def _sample_provider_members(
    session,
    old_info: dict[str, Any],
    new_info: dict[str, Any],
    limit: int,
    sample_pct: float,
) -> dict[str, Any]:
    old_member = _qtable(old_info["tables"]["provider_member"])
    new_member = _qtable(new_info["tables"]["provider_member"])
    await session.execute(_sql_text("DROP TABLE IF EXISTS sample_provider_member"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE sample_provider_member ON COMMIT DROP AS
            SELECT provider_group_global_id_128::text AS provider_group_global_id_128,
                   npi::text AS npi
              FROM {old_member} TABLESAMPLE SYSTEM ({sample_pct}) REPEATABLE (44)
             LIMIT {limit}
            """
        )
    )
    await session.execute(_sql_text("DROP TABLE IF EXISTS missing_provider_member"))
    await session.execute(
        _sql_text(
            f"""
            CREATE TEMP TABLE missing_provider_member ON COMMIT DROP AS
            SELECT sample.*
              FROM sample_provider_member sample
             WHERE NOT EXISTS (
                   SELECT 1
                     FROM {new_member} nm
                    WHERE nm.provider_group_global_id_128::text = sample.provider_group_global_id_128
                      AND nm.npi::text = sample.npi
                    LIMIT 1
             )
            """
        )
    )
    sample_result = await session.execute(
        _sql_text(
            """
            WITH sample_hashes AS (
                SELECT md5(concat_ws('|', provider_group_global_id_128, npi)) AS row_hash
                  FROM sample_provider_member
            ), missing_hashes AS (
                SELECT md5(concat_ws('|', provider_group_global_id_128, npi)) AS row_hash
                  FROM missing_provider_member
            )
            SELECT (SELECT count(*) FROM sample_provider_member) AS sampled,
                   (SELECT md5(coalesce(string_agg(row_hash, ',' ORDER BY row_hash), '')) FROM sample_hashes) AS sample_hash,
                   (SELECT count(*) FROM missing_provider_member) AS missing,
                   (SELECT coalesce(json_agg(row_hash ORDER BY row_hash), '[]'::json) FROM (SELECT row_hash FROM missing_hashes ORDER BY row_hash LIMIT 5) m) AS missing_hashes
            """
        )
    )
    return dict(sample_result.mappings().first())


def _summarize_ms(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "min_ms": None, "avg_ms": None, "p95_ms": None, "max_ms": None}
    ordered = sorted(values)
    p95_index = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return {
        "count": len(values),
        "min_ms": round(ordered[0], 3),
        "avg_ms": round(sum(values) / len(values), 3),
        "p95_ms": round(ordered[p95_index], 3),
        "max_ms": round(ordered[-1], 3),
    }


async def _benchmark_cases(session, info: dict[str, Any], case_count: int, sample_pct: float) -> list[dict[str, Any]]:
    case_count = max(int(case_count), 0)
    if case_count <= 0:
        return []

    async def _load_cases(candidate_pct: float) -> list[dict[str, Any]]:
        sample_sql = _serving_sample_sql(info, max(case_count * 20, case_count), candidate_pct)
        result = await session.execute(
            _sql_text(
                f"""
                WITH sample AS (
                    {sample_sql}
                )
                SELECT DISTINCT plan_id, reported_code_system, reported_code
                  FROM sample
                 WHERE NULLIF(BTRIM(plan_id), '') IS NOT NULL
                   AND NULLIF(BTRIM(reported_code), '') IS NOT NULL
                 ORDER BY plan_id, reported_code_system NULLS LAST, reported_code
                 LIMIT {case_count}
                """
            )
        )
        return [dict(row) for row in result.mappings().all()]

    cases = await _load_cases(sample_pct)
    if cases:
        return cases
    return await _load_cases(100.0)


async def _time_serving_case(
    session,
    *,
    snapshot_id: str,
    case: dict[str, Any],
    iterations: int,
    limit: int,
) -> dict[str, Any]:
    from api.endpoint.pagination import PaginationParams
    from api.ptg2_serving import search_current_ptg2_index

    iterations = max(int(iterations), 1)
    pagination = PaginationParams(page=1, limit=max(int(limit), 1), offset=0, source="ptg2_compare_snapshots")
    args = {
        "snapshot_id": snapshot_id,
        "plan_id": case.get("plan_id"),
        "code_system": case.get("reported_code_system"),
        "code": case.get("reported_code"),
        "limit": str(pagination.limit),
        "include_sources": "false",
        "include_details": "false",
    }

    warmup_started = time.perf_counter()
    warmup_payload = await search_current_ptg2_index(session, args, pagination)
    warmup_ms = (time.perf_counter() - warmup_started) * 1000.0

    elapsed_values: list[float] = []
    result_totals: list[Any] = []
    item_counts: list[int] = []
    for _ in range(iterations):
        started = time.perf_counter()
        payload = await search_current_ptg2_index(session, args, pagination)
        elapsed_values.append((time.perf_counter() - started) * 1000.0)
        payload = payload or {}
        pagination_payload = payload.get("pagination") if isinstance(payload.get("pagination"), dict) else {}
        result_totals.append(pagination_payload.get("total"))
        items = payload.get("items")
        item_counts.append(len(items) if isinstance(items, list) else 0)

    warmup_pagination = (
        warmup_payload.get("pagination")
        if isinstance(warmup_payload, dict) and isinstance(warmup_payload.get("pagination"), dict)
        else {}
    )
    warmup_items = warmup_payload.get("items") if isinstance(warmup_payload, dict) else None
    return {
        "snapshot_id": snapshot_id,
        "warmup_ms": round(warmup_ms, 3),
        "warmup_total": warmup_pagination.get("total"),
        "warmup_items": len(warmup_items) if isinstance(warmup_items, list) else 0,
        "iterations": iterations,
        "timing": _summarize_ms(elapsed_values),
        "result_totals": result_totals,
        "item_counts": item_counts,
    }


async def _latency_benchmark(
    session,
    old_info: dict[str, Any],
    new_info: dict[str, Any],
    *,
    case_count: int,
    iterations: int,
    limit: int,
    sample_pct: float,
) -> dict[str, Any]:
    if case_count <= 0:
        return {"enabled": False}
    cases = await _benchmark_cases(session, old_info, case_count, sample_pct)
    rows = []
    for case in cases:
        rows.append(
            {
                "case": case,
                "old": await _time_serving_case(
                    session,
                    snapshot_id=old_info["snapshot_id"],
                    case=case,
                    iterations=iterations,
                    limit=limit,
                ),
                "new": await _time_serving_case(
                    session,
                    snapshot_id=new_info["snapshot_id"],
                    case=case,
                    iterations=iterations,
                    limit=limit,
                ),
            }
        )
    old_warmup = [row["old"]["warmup_ms"] for row in rows]
    new_warmup = [row["new"]["warmup_ms"] for row in rows]
    old_measured = [row["old"]["timing"]["avg_ms"] for row in rows if row["old"]["timing"]["avg_ms"] is not None]
    new_measured = [row["new"]["timing"]["avg_ms"] for row in rows if row["new"]["timing"]["avg_ms"] is not None]
    return {
        "enabled": True,
        "case_count": len(rows),
        "iterations": max(int(iterations), 1),
        "limit": max(int(limit), 1),
        "old_warmup": _summarize_ms(old_warmup),
        "new_warmup": _summarize_ms(new_warmup),
        "old_measured_avg_ms": _summarize_ms(old_measured),
        "new_measured_avg_ms": _summarize_ms(new_measured),
        "cases": rows,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--old-snapshot-id", required=True, help="Existing snapshot being replaced.")
    parser.add_argument("--new-snapshot-id", required=True, help="Rebuilt snapshot candidate.")
    parser.add_argument("--sample-limit", type=int, default=500, help="Rows to sample from each checked table.")
    parser.add_argument("--sample-pct", type=float, default=0.1, help="Postgres TABLESAMPLE SYSTEM percentage.")
    parser.add_argument("--skip-sidecars", action="store_true", help="Do not fail on sidecar hash/size mismatch.")
    parser.add_argument("--benchmark-cases", type=int, default=0, help="Plan/code cases to time against both snapshots.")
    parser.add_argument("--benchmark-iterations", type=int, default=3, help="Measured iterations per benchmark case.")
    parser.add_argument("--benchmark-limit", type=int, default=5, help="Serving result limit for latency benchmark cases.")
    return parser


async def _compare_snapshots(cli_args: argparse.Namespace) -> dict[str, Any]:
    limit = max(int(cli_args.sample_limit), 1)
    sample_pct = max(float(cli_args.sample_pct), 0.0001)
    database = _db_connection()
    async with database.session() as session:
        old_info = await _snapshot_info(session, cli_args.old_snapshot_id.strip())
        new_info = await _snapshot_info(session, cli_args.new_snapshot_id.strip())
        is_sidecar_match = old_info["sidecars"] == new_info["sidecars"]
        check_result_by_name = {
            "old_status_published": str(old_info["status"]).lower() == "published",
            "new_status_published": str(new_info["status"]).lower() == "published",
            "source_key_equal": str(old_info["source_key"]) == str(new_info["source_key"]),
            "serving_rates_equal": str(old_info["serving_rates"]) == str(new_info["serving_rates"]),
            "files_processed_equal": str(old_info["files_processed"]) == str(new_info["files_processed"]),
            "sidecars_equal": is_sidecar_match,
            "sidecars_checked": not cli_args.skip_sidecars,
            "serving_sample": await _sample_serving(session, old_info, new_info, limit, sample_pct),
            "price_atom_sample": await _sample_price_atoms(session, old_info, new_info, limit, sample_pct),
            "provider_member_sample": await _sample_provider_members(session, old_info, new_info, limit, sample_pct),
            "latency_benchmark": await _latency_benchmark(
                session,
                old_info,
                new_info,
                case_count=max(int(cli_args.benchmark_cases), 0),
                iterations=max(int(cli_args.benchmark_iterations), 1),
                limit=max(int(cli_args.benchmark_limit), 1),
                sample_pct=sample_pct,
            ),
        }
    missing_counts = [
        int(check_result_by_name["serving_sample"]["missing"] or 0),
        int(check_result_by_name["price_atom_sample"]["missing"] or 0),
        int(check_result_by_name["provider_member_sample"]["missing"] or 0),
    ]
    required_conditions = [
        check_result_by_name["old_status_published"],
        check_result_by_name["new_status_published"],
        check_result_by_name["source_key_equal"],
        check_result_by_name["serving_rates_equal"],
        check_result_by_name["files_processed_equal"],
        not any(missing_counts),
        bool(cli_args.skip_sidecars or is_sidecar_match),
    ]
    check_result_by_name["passed"] = all(required_conditions)
    return {"old": old_info, "new": new_info, "checks": check_result_by_name}


async def _run_cli(args: argparse.Namespace) -> dict[str, Any]:
    try:
        return await _compare_snapshots(args)
    finally:
        await _db_connection().disconnect()


def main(argv: list[str] | None = None) -> int:
    """Run the snapshot comparison CLI and print a JSON result."""
    args = _build_parser().parse_args(argv)
    output = asyncio.run(_run_cli(args))
    try:
        print(json.dumps(output, indent=2, sort_keys=True, default=str))
        return 0 if output["checks"]["passed"] else 2
    except BrokenPipeError:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
