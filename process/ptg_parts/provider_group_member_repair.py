# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fast repairs for already-published PTG2 provider-group member tables."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from typing import Any

from sqlalchemy import text

from db.connection import db
from process.ptg_parts.row_helpers import NPI_MAX, NPI_MIN

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _safe_qualified_table_name(value: Any, *, default_schema: str | None = None) -> str:
    text_value = str(value or "").strip()
    if not text_value:
        raise ValueError("provider_group_member table is required")
    schema_name = default_schema or os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    table_name = text_value
    if "." in text_value:
        schema_name, table_name = text_value.split(".", 1)
    if not _IDENT_RE.fullmatch(schema_name) or not _IDENT_RE.fullmatch(table_name):
        raise ValueError(f"unsafe provider_group_member table name: {text_value!r}")
    return f"{schema_name}.{table_name}"


def _quote_ident(value: str) -> str:
    if not _IDENT_RE.fullmatch(value):
        raise ValueError(f"unsafe SQL identifier: {value!r}")
    return f'"{value}"'


def _qualified_table_ref(qualified_table_name: str) -> str:
    schema_name, table_name = _safe_qualified_table_name(qualified_table_name).split(".", 1)
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


async def provider_group_member_table_for_snapshot(session, snapshot_id: str) -> str:
    from api.ptg2_tables import _safe_table_name, snapshot_serving_tables

    tables = await snapshot_serving_tables(session, snapshot_id)
    table_name = _safe_table_name(tables.provider_group_member_table)
    if not table_name:
        raise ValueError(f"snapshot {snapshot_id!r} does not expose a provider_group_member table")
    return table_name


async def _table_columns(session, qualified_table_name: str) -> set[str]:
    schema_name, table_name = _safe_qualified_table_name(qualified_table_name).split(".", 1)
    result = await session.execute(
        text(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = :schema_name
               AND table_name = :table_name
            """
        ),
        {"schema_name": schema_name, "table_name": table_name},
    )
    return {str(row.column_name) for row in result.fetchall()}


def _provider_group_key_expr(columns: set[str]) -> str:
    if "provider_group_global_id_128" in columns:
        return "provider_group_global_id_128::text"
    if "provider_group_hash" in columns:
        return "provider_group_hash::text"
    return "NULL::text"


async def repair_invalid_provider_group_member_npis(
    session,
    *,
    table_name: str,
    apply: bool = False,
    sample_limit: int = 20,
) -> dict[str, Any]:
    qualified_table_name = _safe_qualified_table_name(table_name)
    table_ref = _qualified_table_ref(qualified_table_name)
    columns = await _table_columns(session, qualified_table_name)
    if "npi" not in columns:
        raise ValueError(f"{qualified_table_name} does not have an npi column")

    key_expr = _provider_group_key_expr(columns)
    metrics_result = await session.execute(
        text(
            f"""
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(DISTINCT npi)::bigint AS distinct_npi,
                   COUNT(*) FILTER (WHERE npi < :npi_min OR npi > :npi_max)::bigint AS invalid_rows,
                   COUNT(DISTINCT npi) FILTER (WHERE npi < :npi_min OR npi > :npi_max)::bigint AS invalid_distinct
              FROM {table_ref}
            """
        ),
        {"npi_min": NPI_MIN, "npi_max": NPI_MAX},
    )
    metrics = dict(metrics_result.mappings().one())

    sample_result = await session.execute(
        text(
            f"""
            SELECT {key_expr} AS provider_group_key,
                   npi,
                   COUNT(*) OVER (PARTITION BY npi)::bigint AS rows_for_npi
              FROM {table_ref}
             WHERE npi < :npi_min OR npi > :npi_max
             ORDER BY npi, provider_group_key
             LIMIT :sample_limit
            """
        ),
        {"npi_min": NPI_MIN, "npi_max": NPI_MAX, "sample_limit": max(1, int(sample_limit))},
    )
    samples = [dict(row) for row in sample_result.mappings().all()]

    candidate_result = await session.execute(
        text(
            f"""
            WITH bad AS (
                SELECT DISTINCT npi AS bad_npi
                  FROM {table_ref}
                 WHERE npi < :npi_min OR npi > :npi_max
            ), digits AS (
                SELECT generate_series(1, 9) AS leading_digit
            ), proposed AS (
                SELECT bad.bad_npi,
                       (digits.leading_digit::bigint * 1000000000 + bad.bad_npi)::bigint AS candidate_npi
                  FROM bad CROSS JOIN digits
                 WHERE bad.bad_npi BETWEEN 100000000 AND 999999999
            )
            SELECT proposed.bad_npi,
                   array_agg(proposed.candidate_npi ORDER BY proposed.candidate_npi)
                       FILTER (WHERE npi.npi IS NOT NULL) AS matching_nppes_candidates,
                   COUNT(npi.npi)::bigint AS match_count
              FROM proposed
              LEFT JOIN mrf.npi ON npi.npi = proposed.candidate_npi
             GROUP BY proposed.bad_npi
             ORDER BY proposed.bad_npi
            """
        ),
        {"npi_min": NPI_MIN, "npi_max": NPI_MAX},
    )
    restore_candidates = [dict(row) for row in candidate_result.mappings().all()]

    deleted_rows = 0
    if apply and int(metrics.get("invalid_rows") or 0) > 0:
        delete_result = await session.execute(
            text(
                f"""
                DELETE FROM {table_ref}
                 WHERE npi < :npi_min OR npi > :npi_max
                """
            ),
            {"npi_min": NPI_MIN, "npi_max": NPI_MAX},
        )
        deleted_rows = int(delete_result.rowcount or 0)
        await session.execute(text(f"ANALYZE {table_ref}"))

    return {
        "status": "applied" if apply else "dry_run",
        "table_name": qualified_table_name,
        "npi_min": NPI_MIN,
        "npi_max": NPI_MAX,
        "metrics_before": {key: int(value or 0) for key, value in metrics.items()},
        "samples": samples,
        "leading_digit_restore_candidates": restore_candidates,
        "deleted_rows": deleted_rows,
    }


def _json_default(value: Any) -> Any:
    return str(value)


async def _main_async(args: argparse.Namespace) -> None:
    await db.connect()
    try:
        async with db.session() as session:
            table_name = args.table
            if not table_name:
                table_name = await provider_group_member_table_for_snapshot(session, args.snapshot_id)
            summary = await repair_invalid_provider_group_member_npis(
                session,
                table_name=table_name,
                apply=bool(args.apply),
                sample_limit=int(args.sample_limit),
            )
        print(json.dumps(summary, indent=2, sort_keys=True, default=_json_default))
    finally:
        await db.disconnect()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report or remove malformed NPIs from a PTG2 provider_group_member table.",
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--snapshot-id", help="Published PTG2 snapshot id to resolve")
    target.add_argument("--table", help="Qualified provider_group_member table name")
    parser.add_argument("--sample-limit", type=int, default=20)
    parser.add_argument("--apply", action="store_true", help="Delete malformed rows; default is dry-run")
    return parser


def main() -> None:
    asyncio.run(_main_async(build_parser().parse_args()))


if __name__ == "__main__":
    main()
