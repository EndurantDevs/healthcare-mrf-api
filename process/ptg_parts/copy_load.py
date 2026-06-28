# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""COPY-based PTG2 database load helpers."""

from __future__ import annotations

import datetime
import json
import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from db.connection import db
from process.ext.utils import push_objects
from process.ptg_parts.canonical import normalize_money
from process.ptg_parts.config import _ptg2_stage_copy_dedupe_enabled
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.rust_stage import _ptg2_dictionary_select_columns


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return normalize_money(value)
    return str(value)


def _strip_postgres_nuls(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_postgres_nuls(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_postgres_nuls(item) for item in value)
    if isinstance(value, dict):
        return {
            _strip_postgres_nuls(key): _strip_postgres_nuls(item)
            for key, item in value.items()
        }
    return value


def _copy_record_values(values: tuple[Any, ...]) -> tuple[Any, ...]:
    return tuple(_strip_postgres_nuls(value) for value in values)


def _ptg2_conflict_targets(cls) -> list[str]:
    if hasattr(cls, "__my_initial_indexes__") and cls.__my_initial_indexes__:
        for index in cls.__my_initial_indexes__:
            elements = index.get("index_elements")
            if elements:
                return [str(element) for element in elements]
    if hasattr(cls, "__my_index_elements__") and cls.__my_index_elements__:
        return [str(element) for element in cls.__my_index_elements__]
    return [key.name for key in cls.__table__.primary_key]


def _primary_key_column_names(obj) -> list[str]:
    return [str(key.name) for key in obj.__table__.primary_key]


def _ptg2_json_columns(cls) -> set[str]:
    result = set()
    for column in cls.__table__.c:
        type_name = column.type.__class__.__name__.upper()
        if "JSON" in type_name:
            result.add(column.name)
    return result


def _ptg2_copy_record(row: dict[str, Any], columns: list[str], json_columns: set[str]) -> tuple[Any, ...]:
    values: list[Any] = []
    for column in columns:
        value = _strip_postgres_nuls(row.get(column))
        if value is not None and column in json_columns:
            value = json.dumps(value, sort_keys=True, default=_json_default)
        values.append(value)
    return tuple(values)


async def _copy_upsert_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    columns = [column.name for column in cls.__table__.c if column.name in rows[0]]
    if not columns:
        return
    conflict_targets = _ptg2_conflict_targets(cls)
    if not conflict_targets:
        await push_objects(rows, cls, rewrite=True, use_copy=False)
        return
    deduped_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        deduped_rows[tuple(row.get(target) for target in conflict_targets)] = row
    rows = list(deduped_rows.values())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    table_name = cls.__tablename__
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    update_columns = [column for column in columns if column not in set(conflict_targets)]
    if update_columns:
        conflict_sql = (
            "DO UPDATE SET "
            + ", ".join(f"{_quote_ident(column)} = EXCLUDED.{_quote_ident(column)}" for column in update_columns)
        )
    else:
        conflict_sql = "DO NOTHING"
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            temp_table,
            columns=columns,
            records=records,
        )
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) {conflict_sql};
            """
        )


async def _copy_insert_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    columns = list(rows[0].keys())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            cls.__tablename__,
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_ignore_ptg2_objects(rows: list[dict[str, Any]], cls) -> None:
    if not rows:
        return
    conflict_targets = list(getattr(cls, "__my_index_elements__", []) or [])
    if not conflict_targets:
        await _copy_insert_ptg2_objects(rows, cls)
        return
    columns = list(rows[0].keys())
    json_columns = _ptg2_json_columns(cls)
    schema_name = cls.__table__.schema or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    table_name = cls.__tablename__
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    records = [_ptg2_copy_record(row, columns, json_columns) for row in rows]
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            temp_table,
            columns=columns,
            records=records,
        )
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {quoted_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) DO NOTHING;
            """
        )


async def _copy_stage_price_set_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "snapshot_id",
        "price_set_hash",
        "created_at",
    ]
    records = [
        (
            snapshot_id,
            row.get("price_set_hash"),
            row.get("created_at"),
        )
        for row in rows
    ]
    records = [_copy_record_values(record) for record in records]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_price_set_stage",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_stage_serving_rate_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "plan_name",
        "plan_id_type",
        "plan_market_type",
        "issuer_name",
        "plan_sponsor_name",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "billing_code",
        "billing_code_type",
        "procedure_name",
        "procedure_description",
        "procedure_display_name",
        "rate_pack_hash",
        "provider_set_hash",
        "provider_set_hashes",
        "provider_count",
        "provider_set_count",
        "price_set_hash",
        "source_trace_set_hash",
        "network_names",
        "confidence_code",
        "prices",
        "source_trace",
        "confidence",
        "created_at",
    ]
    records = [
        (
            row.get("serving_rate_id"),
            snapshot_id,
            row.get("plan_id"),
            row.get("plan_name"),
            row.get("plan_id_type"),
            row.get("plan_market_type"),
            row.get("issuer_name"),
            row.get("plan_sponsor_name"),
            row.get("procedure_code"),
            row.get("reported_code_system"),
            row.get("reported_code"),
            row.get("billing_code"),
            row.get("billing_code_type"),
            row.get("procedure_name"),
            row.get("procedure_description"),
            row.get("procedure_display_name"),
            row.get("rate_pack_hash"),
            row.get("provider_set_hash"),
            row.get("provider_set_hashes") or [],
            row.get("provider_count"),
            row.get("provider_set_count"),
            row.get("price_set_hash"),
            row.get("source_trace_set_hash"),
            row.get("network_names") or [],
            row.get("confidence_code"),
            json.dumps(row.get("prices"), default=_json_default) if row.get("prices") is not None else None,
            json.dumps(row.get("source_trace"), default=_json_default) if row.get("source_trace") is not None else None,
            json.dumps(row.get("confidence"), default=_json_default) if row.get("confidence") is not None else None,
            row.get("created_at"),
        )
        for row in rows
    ]
    records = [_copy_record_values(record) for record in records]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_serving_rate_stage",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_compact_serving_rate_rows(rows: list[dict[str, Any]], snapshot_id: str) -> None:
    if not rows:
        return
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "procedure_hash",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "provider_set_hash",
        "provider_count",
        "price_set_hash",
        "source_trace_set_hash",
        "network_names",
        "created_at",
    ]
    records = [
        (
            row.get("serving_rate_id"),
            snapshot_id,
            row.get("plan_id"),
            row.get("procedure_hash"),
            row.get("procedure_code"),
            row.get("reported_code_system"),
            row.get("reported_code"),
            row.get("provider_set_hash"),
            row.get("provider_count"),
            row.get("price_set_hash"),
            row.get("source_trace_set_hash"),
            row.get("network_names") or [],
            row.get("created_at"),
        )
        for row in rows
    ]
    records = [_copy_record_values(record) for record in records]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        await driver_conn.copy_records_to_table(
            "ptg2_serving_rate_compact",
            schema_name=schema_name,
            columns=columns,
            records=records,
        )


async def _copy_compact_serving_rate_file(copy_path: Path, *, target_table: str = "ptg2_serving_rate_compact") -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    await _copy_compact_serving_rate_source(copy_path.open("rb"), target_table=target_table)


async def _copy_compact_serving_rate_source(source, *, target_table: str = "ptg2_serving_rate_compact") -> None:
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    columns = [
        "serving_rate_id",
        "snapshot_id",
        "plan_id",
        "procedure_hash",
        "procedure_code",
        "reported_code_system",
        "reported_code",
        "provider_set_hash",
        "provider_count",
        "price_set_hash",
        "source_trace_set_hash",
        "network_names",
    ]
    async with db.acquire() as conn:
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        try:
            await copy_to_table(
                target_table,
                source=source,
                schema_name=schema_name,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )
        finally:
            close = getattr(source, "close", None)
            if close is not None:
                close()


async def _copy_ptg2_dictionary_file(copy_path: Path, kind: str, *, target_table: str | None = None) -> None:
    if not copy_path.exists() or copy_path.stat().st_size <= 0:
        return
    specs = {
        "procedure": (
            "ptg2_procedure",
            ["procedure_hash", "billing_code_type", "billing_code_type_version", "billing_code", "name", "description"],
            ["procedure_hash"],
        ),
        "price_code_set": (
            "ptg2_price_code_set",
            ["code_set_hash", "codes"],
            ["code_set_hash"],
        ),
        "price_atom": (
            "ptg2_price_atom",
            [
                "price_atom_hash",
                "negotiated_type",
                "negotiated_rate",
                "expiration_date",
                "service_code_set_hash",
                "billing_class",
                "setting",
                "billing_code_modifier_set_hash",
                "additional_information",
            ],
            ["price_atom_hash"],
        ),
        "price_set_entry": (
            "ptg2_price_set_entry",
            ["price_set_hash", "price_atom_hash"],
            ["price_set_hash", "price_atom_hash"],
        ),
        "provider_set": (
            "ptg2_provider_set",
            ["provider_set_hash", "provider_count"],
            ["provider_set_hash"],
        ),
        "provider_group_member": (
            "ptg2_provider_group_member",
            ["provider_group_hash", "npi"],
            ["provider_group_hash", "npi"],
        ),
        "provider_set_component": (
            "ptg2_provider_set_component",
            ["provider_set_hash", "provider_group_hash"],
            ["provider_set_hash", "provider_group_hash"],
        ),
        "provider_set_entry": (
            "ptg2_provider_set_entry",
            ["provider_set_hash", "provider_entry_hash"],
            ["provider_set_hash", "provider_entry_hash"],
        ),
        "provider_entry_component": (
            "ptg2_provider_entry_component",
            ["provider_entry_hash", "provider_group_hash"],
            ["provider_entry_hash", "provider_group_hash"],
        ),
    }
    if kind not in specs:
        raise ValueError(f"Unsupported PTG2 dictionary copy kind: {kind}")
    table_name, columns, conflict_targets = specs[kind]
    schema_name = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"
    if target_table is not None:
        dedupe_stage_copy = _ptg2_stage_copy_dedupe_enabled(kind)
        async with db.acquire() as conn:
            raw_conn = conn.raw_connection
            driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
            copy_to_table = getattr(driver_conn, "copy_to_table", None)
            if copy_to_table is None:
                raise NotImplementedError("Active database driver does not expose copy_to_table")
            if not dedupe_stage_copy:
                with copy_path.open("rb") as source:
                    await copy_to_table(
                        target_table,
                        source=source,
                        columns=columns,
                        format="text",
                        delimiter="\t",
                        null="\\N",
                        schema_name=schema_name,
                    )
                return

            temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
            quoted_columns = ", ".join(_quote_ident(column) for column in columns)
            quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
            select_columns = _ptg2_dictionary_select_columns(kind, columns)
            await conn.status(
                f"""
                CREATE TEMP TABLE {_quote_ident(temp_table)}
                (LIKE {_quote_ident(schema_name)}.{_quote_ident(target_table)} INCLUDING DEFAULTS)
                ON COMMIT DROP;
                """
            )
            with copy_path.open("rb") as source:
                await copy_to_table(
                    temp_table,
                    source=source,
                    columns=columns,
                    format="text",
                    delimiter="\t",
                    null="\\N",
                )
            await conn.status(
                f"""
                INSERT INTO {_quote_ident(schema_name)}.{_quote_ident(target_table)} ({quoted_columns})
                SELECT {select_columns}
                FROM {_quote_ident(temp_table)}
                ON CONFLICT ({quoted_conflict}) DO NOTHING;
                """
            )
        return
    temp_table = f"ptg2_stage_{table_name}_{os.getpid()}_{time.time_ns()}"
    quoted_temp = _quote_ident(temp_table)
    quoted_target = f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    quoted_columns = ", ".join(_quote_ident(column) for column in columns)
    quoted_conflict = ", ".join(_quote_ident(column) for column in conflict_targets)
    async with db.acquire() as conn:
        await conn.status(
            f"CREATE TEMP TABLE {quoted_temp} (LIKE {quoted_target} INCLUDING DEFAULTS) ON COMMIT DROP;"
        )
        raw_conn = conn.raw_connection
        driver_conn = getattr(raw_conn, "driver_connection", raw_conn)
        copy_to_table = getattr(driver_conn, "copy_to_table", None)
        if copy_to_table is None:
            raise NotImplementedError("Active database driver does not expose copy_to_table")
        with copy_path.open("rb") as source:
            await copy_to_table(
                temp_table,
                source=source,
                columns=columns,
                format="text",
                delimiter="\t",
                null="\\N",
            )
        select_columns = _ptg2_dictionary_select_columns(kind, columns)
        await conn.status(
            f"""
            INSERT INTO {quoted_target} ({quoted_columns})
            SELECT {select_columns}
            FROM {quoted_temp}
            ON CONFLICT ({quoted_conflict}) DO NOTHING;
            """
        )
