# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-scoped Rust compact publish helpers for PTG2 imports."""

from __future__ import annotations

import datetime
import logging
import os
import time
from typing import Any

from db.connection import db
from process.ptg_parts.compact_indexes import (
    _index_snapshot_compact_table_entries,
    _index_snapshot_compact_tables,
)
from process.ptg_parts.config import PTG2_UNLOGGED_STAGE_ENV, _env_bool
from process.ptg_parts.db_tables import (
    _estimated_table_rows,
    _exact_table_rows,
    _quote_ident,
    _table_exists,
    _table_has_rows,
)
from process.ptg_parts.rust_stage import _serving_stage_tables
from process.ptg_parts.screen import _emit_screen_line
from process.ptg_parts.snapshot_tables import (
    _ptg2_snapshot_index_name,
    _ptg2_snapshot_table_name,
)

logger = logging.getLogger(__name__)


def _ptg2_publish_timestamp() -> str:
    return datetime.datetime.now().isoformat(timespec="seconds")


async def _create_optional_provider_geo_index(
    *,
    schema_name: str,
    provider_group_location_table: str,
) -> None:
    try:
        await db.status(
            f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, 'geo_gist_idx'))} "
            f"ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)} "
            "USING gist (geography(st_makepoint(long::float8, lat::float8))) "
            "WHERE lat IS NOT NULL AND long IS NOT NULL;"
        )
    except Exception as exc:  # pragma: no cover - exact driver exception varies by environment.
        logger.warning(
            "Skipping optional PTG2 provider geo GiST index for %s.%s: %s",
            schema_name,
            provider_group_location_table,
            exc,
        )


async def _publish_renamed_rust_dictionary_table(
    *,
    schema_name: str,
    kind: str,
    stage_table: str,
    final_table: str,
) -> float:
    start_message = (
        f"PTG2_PUBLISH_TABLE_START time={_ptg2_publish_timestamp()} "
        f"kind={kind} stage={schema_name}.{stage_table} final={schema_name}.{final_table} mode=rename"
    )
    _emit_screen_line(start_message)
    logger.info(start_message)
    started_at = time.monotonic()
    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(final_table)} CASCADE;")
    await db.status(
        f"""
        ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
        RENAME TO {_quote_ident(final_table)};
        """
    )
    await db.status(
        f"DROP INDEX IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(_ptg2_snapshot_index_name(stage_table, 'copy_dedupe_idx'))};"
    )
    done_message = (
        f"PTG2_PUBLISH_TABLE_DONE time={_ptg2_publish_timestamp()} "
        f"kind={kind} mode=rename elapsed_seconds={time.monotonic() - started_at:.2f}"
    )
    _emit_screen_line(done_message)
    logger.info(done_message)
    return time.monotonic() - started_at


def _ptg2_serving_child_table_name(parent_table: str, index: int) -> str:
    suffix = f"_p{index:02d}"
    return f"{parent_table[:63 - len(suffix)]}{suffix}"[:63]


async def _publish_rust_serving_stage_tables(
    *,
    schema_name: str,
    stage_tables: dict[str, str],
    final_table: str,
) -> list[str]:
    storage_mode = "UNLOGGED " if _env_bool(PTG2_UNLOGGED_STAGE_ENV, True) else ""
    existing_stage_tables: list[str] = []
    empty_stage_tables: list[str] = []
    for stage_table in _serving_stage_tables(stage_tables):
        if not await _table_exists(schema_name, stage_table):
            continue
        if await _table_has_rows(schema_name, stage_table):
            existing_stage_tables.append(stage_table)
        else:
            empty_stage_tables.append(stage_table)

    for stage_table in empty_stage_tables:
        await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(stage_table)};")

    if not existing_stage_tables:
        return []

    await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(final_table)} CASCADE;")

    if len(existing_stage_tables) == 1:
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(existing_stage_tables[0])}
            RENAME TO {_quote_ident(final_table)};
            """
        )
        return [final_table]

    await db.status(
        f"""
        CREATE {storage_mode}TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)}
        (LIKE {_quote_ident(schema_name)}.ptg2_serving_rate_compact INCLUDING DEFAULTS);
        """
    )
    try:
        await db.status(
            f"ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(final_table)} "
            "SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);"
        )
    except Exception as exc:
        logger.debug("Skipping PTG2 serving parent autovacuum disable for %s: %s", final_table, exc)

    child_tables: list[str] = []
    for index, stage_table in enumerate(existing_stage_tables):
        child_table = _ptg2_serving_child_table_name(final_table, index)
        await db.status(f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(child_table)};")
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(stage_table)}
            RENAME TO {_quote_ident(child_table)};
            """
        )
        await db.status(
            f"""
            ALTER TABLE {_quote_ident(schema_name)}.{_quote_ident(child_table)}
            INHERIT {_quote_ident(schema_name)}.{_quote_ident(final_table)};
            """
        )
        child_tables.append(child_table)
    return child_tables


async def _publish_rust_compact_snapshot_tables(
    stage_tables: dict[str, str],
    *,
    snapshot_id: str,
    import_run_id: str,
    source_key: str,
) -> dict[str, Any]:
    del import_run_id
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    rename_order = (
        "procedure",
        "price_code_set",
        "price_atom",
        "price_set_entry",
        "provider_set",
        "provider_set_component",
        "provider_group_member",
        "serving_rate_compact",
    )
    table_names = {
        kind: _ptg2_snapshot_table_name(kind, source_key, snapshot_id)
        for kind in rename_order
        if stage_tables.get(kind)
    }
    serving_index_tables: list[str] = []
    publish_started = time.monotonic()
    dictionary_publish_seconds = 0.0
    serving_publish_seconds = 0.0
    index_seconds = 0.0
    analyze_started = 0.0
    analyze_seconds = 0.0
    # Source-scoped compact stages are scanner-deduped. Publish dictionary
    # stages by rename and let unique-index creation validate correctness.
    # Follow-ups: move remaining low-value serving-rate dedupe into Rust or gate
    # it explicitly, make string dictionary dedupe exact in Rust, and only
    # reintroduce a bucketed SQL fallback if direct rename proves unsafe.
    for kind in rename_order:
        stage_table = stage_tables.get(kind)
        final_table = table_names.get(kind)
        if not stage_table or not final_table:
            continue
        if kind != "serving_rate_compact" and not await _table_exists(schema_name, stage_table):
            continue
        if kind == "serving_rate_compact":
            serving_publish_started = time.monotonic()
            serving_index_tables = await _publish_rust_serving_stage_tables(
                schema_name=schema_name,
                stage_tables=stage_tables,
                final_table=final_table,
            )
            serving_publish_seconds += time.monotonic() - serving_publish_started
        else:
            dictionary_publish_seconds += await _publish_renamed_rust_dictionary_table(
                schema_name=schema_name,
                kind=kind,
                stage_table=stage_table,
                final_table=final_table,
            )

    serving_table = table_names.get("serving_rate_compact")
    if not serving_table or not await _table_exists(schema_name, serving_table):
        raise RuntimeError("PTG2 Rust compact snapshot publish produced no serving table")
    if not await _table_has_rows(schema_name, serving_table):
        raise RuntimeError("PTG2 Rust compact snapshot publish produced an empty serving table")

    if serving_index_tables and serving_index_tables != [serving_table]:
        dictionary_index_tables = {key: value for key, value in table_names.items() if key != "serving_rate_compact"}
        index_seconds += await _index_snapshot_compact_tables(schema_name, dictionary_index_tables)
        index_seconds += await _index_snapshot_compact_table_entries(
            schema_name,
            [("serving_rate_compact", child_table) for child_table in serving_index_tables],
        )
    else:
        index_seconds += await _index_snapshot_compact_tables(schema_name, table_names)

    provider_group_location_table = None
    if table_names.get("provider_group_member"):
        provider_group_location_table = _ptg2_snapshot_table_name("provider_group_location", source_key, snapshot_id)
        await db.status(
            f"DROP TABLE IF EXISTS {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)};"
        )
        await db.status(
            f"""
            CREATE UNLOGGED TABLE {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)} AS
            SELECT DISTINCT
                   pgm.provider_group_hash,
                   pgm.npi,
                   LEFT(COALESCE(addr.postal_code, ''), 5)::varchar(5) AS zip5,
                   addr.state_name::varchar AS state_name,
                   addr.city_name::varchar AS city_name,
                   addr.lat,
                   addr.long,
                   addr.taxonomy_array,
                   addr.type::varchar AS address_type,
                   addr.checksum::varchar AS address_checksum,
                   addr.first_line::varchar AS first_line,
                   addr.second_line::varchar AS second_line,
                   addr.postal_code::varchar AS postal_code,
                   addr.country_code::varchar AS country_code
              FROM {_quote_ident(schema_name)}.{_quote_ident(table_names['provider_group_member'])} pgm
              JOIN {_quote_ident(schema_name)}.npi_address addr
                ON addr.npi = pgm.npi
             WHERE addr.type IN ('primary', 'secondary')
               AND (
                    NULLIF(LEFT(COALESCE(addr.postal_code, ''), 5), '') IS NOT NULL
                 OR NULLIF(addr.state_name, '') IS NOT NULL
                 OR NULLIF(addr.city_name, '') IS NOT NULL
                 OR (addr.lat IS NOT NULL AND addr.long IS NOT NULL)
               );
            """
        )
        location_indexes = [
            (
                "group_zip_idx",
                "(provider_group_hash, zip5, npi)",
            ),
            (
                "zip_group_idx",
                "(zip5, provider_group_hash, npi)",
            ),
            (
                "state_city_group_idx",
                "(state_name, city_name, provider_group_hash, npi)",
            ),
            (
                "state_city_npi_group_idx",
                "(state_name, city_name, npi, provider_group_hash)",
            ),
            (
                "group_state_city_npi_addr_idx",
                "(provider_group_hash, state_name, city_name, npi, address_checksum)",
            ),
            (
                "npi_group_idx",
                "(npi, provider_group_hash)",
            ),
            (
                "group_npi_idx",
                "(provider_group_hash, npi)",
            ),
            (
                "lat_long_group_idx",
                "(lat, long, provider_group_hash, npi) WHERE lat IS NOT NULL AND long IS NOT NULL",
            ),
            (
                "taxonomy_array_gin_idx",
                "USING gin (taxonomy_array gin__int_ops)",
            ),
        ]
        for role, columns_sql in location_indexes:
            await db.status(
                f"CREATE INDEX IF NOT EXISTS {_quote_ident(_ptg2_snapshot_index_name(provider_group_location_table, role))} "
                f"ON {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)} {columns_sql};"
            )
        await _create_optional_provider_geo_index(
            schema_name=schema_name,
            provider_group_location_table=provider_group_location_table,
        )
    analyze_started = time.monotonic()
    for table_name in table_names.values():
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(table_name)};")
    if provider_group_location_table:
        await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(provider_group_location_table)};")
    for child_table in serving_index_tables:
        if child_table != serving_table:
            await db.status(f"ANALYZE {_quote_ident(schema_name)}.{_quote_ident(child_table)};")
    analyze_seconds = time.monotonic() - analyze_started

    plan_rows = await db.all(
        f"""
        SELECT COUNT(DISTINCT plan_hash) AS plans
          FROM {_quote_ident(schema_name)}.ptg2_plan_month
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=snapshot_id,
    )
    plan_count = int((plan_rows[0].get("plans") if isinstance(plan_rows[0], dict) else getattr(plan_rows[0], "plans", 0)) or 0) if plan_rows else 0
    procedure_count = (
        await _estimated_table_rows(schema_name, table_names["procedure"])
        if table_names.get("procedure")
        else 0
    )
    if serving_index_tables and serving_index_tables != [serving_table]:
        rate_count = 0
        for table_name in serving_index_tables:
            rate_count += await _exact_table_rows(schema_name, table_name)
    else:
        rate_count = await _exact_table_rows(schema_name, serving_table)
    return {
        "type": "db_compact",
        "storage": "db_compact_snapshot",
        "snapshot_scoped": True,
        "source_key": source_key,
        "snapshot_id": snapshot_id,
        "table": f"{schema_name}.{serving_table}",
        "price_code_set_table": (
            f"{schema_name}.{table_names['price_code_set']}"
            if table_names.get("price_code_set")
            else None
        ),
        "price_atom_table": f"{schema_name}.{table_names['price_atom']}" if table_names.get("price_atom") else None,
        "price_set_entry_table": (
            f"{schema_name}.{table_names['price_set_entry']}"
            if table_names.get("price_set_entry")
            else None
        ),
        "procedure_table": f"{schema_name}.{table_names['procedure']}" if table_names.get("procedure") else None,
        "provider_set_table": f"{schema_name}.{table_names['provider_set']}" if table_names.get("provider_set") else None,
        "provider_set_component_table": (
            f"{schema_name}.{table_names['provider_set_component']}"
            if table_names.get("provider_set_component")
            else None
        ),
        "provider_set_entry_table": (
            f"{schema_name}.{table_names['provider_set_entry']}"
            if table_names.get("provider_set_entry")
            else None
        ),
        "provider_entry_component_table": (
            f"{schema_name}.{table_names['provider_entry_component']}"
            if table_names.get("provider_entry_component")
            else None
        ),
        "provider_group_member_table": (
            f"{schema_name}.{table_names['provider_group_member']}"
            if table_names.get("provider_group_member")
            else None
        ),
        "provider_group_location_table": (
            f"{schema_name}.{provider_group_location_table}"
            if provider_group_location_table
            else None
        ),
        "rate_count": rate_count,
        "serving_rates": rate_count,
        "row_count": rate_count,
        "row_count_estimate": rate_count,
        "plans": plan_count,
        "procedures": procedure_count,
        "timings": {
            "publish_seconds": time.monotonic() - publish_started,
            "dictionary_publish_seconds": dictionary_publish_seconds,
            "serving_publish_seconds": serving_publish_seconds,
            "index_seconds": index_seconds,
            "analyze_seconds": analyze_seconds,
        },
    }
