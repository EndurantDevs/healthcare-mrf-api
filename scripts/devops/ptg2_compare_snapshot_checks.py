#!/usr/bin/env python3
# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sampling and latency checks for PTG2 snapshot comparisons."""

from __future__ import annotations

import re
import time
from typing import Any


def _sql_text(statement: str):
    from sqlalchemy import text as sqlalchemy_text

    return sqlalchemy_text(statement)


IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


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
    search_arg_map = {
        "snapshot_id": snapshot_id,
        "plan_id": case.get("plan_id"),
        "code_system": case.get("reported_code_system"),
        "code": case.get("reported_code"),
        "limit": str(pagination.limit),
        "include_sources": "false",
        "include_details": "false",
    }

    warmup_started = time.perf_counter()
    warmup_response = await search_current_ptg2_index(session, search_arg_map, pagination)
    warmup_ms = (time.perf_counter() - warmup_started) * 1000.0

    elapsed_values: list[float] = []
    result_totals: list[Any] = []
    item_counts: list[int] = []
    for _ in range(iterations):
        started = time.perf_counter()
        search_response = await search_current_ptg2_index(session, search_arg_map, pagination)
        elapsed_values.append((time.perf_counter() - started) * 1000.0)
        search_response = search_response or {}
        pagination_map = search_response.get("pagination") if isinstance(search_response.get("pagination"), dict) else {}
        result_totals.append(pagination_map.get("total"))
        provider_items = search_response.get("items")
        item_counts.append(len(provider_items) if isinstance(provider_items, list) else 0)

    warmup_pagination_map = (
        warmup_response.get("pagination")
        if isinstance(warmup_response, dict) and isinstance(warmup_response.get("pagination"), dict)
        else {}
    )
    warmup_items = warmup_response.get("items") if isinstance(warmup_response, dict) else None
    return {
        "snapshot_id": snapshot_id,
        "warmup_ms": round(warmup_ms, 3),
        "warmup_total": warmup_pagination_map.get("total"),
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
    benchmark_cases = await _benchmark_cases(session, old_info, case_count, sample_pct)
    benchmark_rows: list[dict[str, Any]] = []
    for benchmark_case in benchmark_cases:
        benchmark_rows.append(
            {
                "case": benchmark_case,
                "old": await _time_serving_case(
                    session,
                    snapshot_id=old_info["snapshot_id"],
                    case=benchmark_case,
                    iterations=iterations,
                    limit=limit,
                ),
                "new": await _time_serving_case(
                    session,
                    snapshot_id=new_info["snapshot_id"],
                    case=benchmark_case,
                    iterations=iterations,
                    limit=limit,
                ),
            }
        )
    old_warmup_values = [benchmark_row["old"]["warmup_ms"] for benchmark_row in benchmark_rows]
    new_warmup_values = [benchmark_row["new"]["warmup_ms"] for benchmark_row in benchmark_rows]
    old_measured_values = [benchmark_row["old"]["timing"]["avg_ms"] for benchmark_row in benchmark_rows if benchmark_row["old"]["timing"]["avg_ms"] is not None]
    new_measured_values = [benchmark_row["new"]["timing"]["avg_ms"] for benchmark_row in benchmark_rows if benchmark_row["new"]["timing"]["avg_ms"] is not None]
    return {
        "enabled": True,
        "case_count": len(benchmark_rows),
        "iterations": max(int(iterations), 1),
        "limit": max(int(limit), 1),
        "old_warmup": _summarize_ms(old_warmup_values),
        "new_warmup": _summarize_ms(new_warmup_values),
        "old_measured_avg_ms": _summarize_ms(old_measured_values),
        "new_measured_avg_ms": _summarize_ms(new_measured_values),
        "cases": benchmark_rows,
    }
