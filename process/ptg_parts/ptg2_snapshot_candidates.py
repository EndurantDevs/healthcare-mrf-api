# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native COPY into protected, detached PTG snapshot candidates."""

import tempfile
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from process.ptg_parts.db_tables import _quote_ident

COPY_MAX_ROWS = 4096
COPY_MAX_BYTES = 32 * 1024 * 1024
_COPY_FRAMING_BYTES = 21
CANDIDATE_TABLES = frozenset(
    {
        "ptg2_v4_snapshot_map_pack",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_heavy_owner",
        "ptg2_provider_group_tax_identity_source",
    }
)
_STAGE_KEYS = {
    "ptg2_provider_tax_identity": ("tin_key", "integer"),
    "ptg2_provider_group_tax_identity": ("provider_group_global_id_128", "bytea"),
    "ptg2_v4_npi_scope": ("npi_key", "integer"),
    "ptg2_v4_provider_component": ("component_key", "integer"),
    "ptg2_v4_pattern": ("pattern_key", "integer"),
    "ptg2_v4_provider_set_npi_prefix": ("provider_set_key", "integer"),
}
_FIXED_COLUMN_BYTES = {
    "tin_key": 4,
    "npi_key": 4,
    "npi": 8,
    "component_key": 4,
    "pattern_key": 4,
    "set_count": 8,
    "provider_set_key": 4,
    "member_count": 4,
}


async def candidate_driver(session):
    """Return the native connection already owned by the publication transaction."""
    connection = session if isinstance(session, AsyncConnection) else await session.connection()
    raw = await connection.get_raw_connection()
    return raw.driver_connection


async def begin_snapshot_candidate(session, schema_name, table, snapshot_key, build_token):
    """Authorize one detached typed heap for the exact active snapshot build."""
    schema = _quote_ident(schema_name)
    return await session.scalar(
        text(f"SELECT {schema}.begin_ptg_snapshot_candidate(:table,:snapshot,:token)"),
        {"table": table, "snapshot": snapshot_key, "token": build_token},
    )


async def finish_snapshot_candidate(session, schema_name, candidate, count):
    """Freeze and validate the indexed candidate without locking its live parent."""
    schema = _quote_ident(schema_name)
    return await session.scalar(
        text(f"SELECT {schema}.finish_ptg_snapshot_candidate(:candidate,:count)"),
        {"candidate": candidate, "count": count},
    )


async def attach_snapshot_candidates(session, schema_name, snapshot_key, build_token):
    """Attach one prepared family at the caller's final transaction boundary."""
    return await session.scalar(
        text(f"SELECT {_quote_ident(schema_name)}.attach_ptg_snapshot_candidates(:snapshot,:token)"),
        {"snapshot": snapshot_key, "token": build_token},
    )


@asynccontextmanager
async def snapshot_candidate_reads(session, schema_name, snapshot_key, build_token):
    """Bind authenticated frozen heaps only for this builder's validation reads."""
    relations = await session.scalar(
        text(f"SELECT {_quote_ident(schema_name)}.read_ptg_snapshot_candidates(:snapshot,:token)"),
        {"snapshot": snapshot_key, "token": build_token},
    )
    key = "ptg_snapshot_candidate_reads"
    previous = session.info.get(key)
    session.info[key] = {_quote_ident(schema_name): dict(relations)}
    try:
        yield
    finally:
        if previous is None:
            session.info.pop(key, None)
        else:
            session.info[key] = previous


def snapshot_candidate_relation(session, quoted_schema, table):
    """Choose an exact frozen heap inside an explicit builder read context."""
    info = getattr(session, "info", None)
    bindings = info.get("ptg_snapshot_candidate_reads", {}) if isinstance(info, dict) else {}
    relation = bindings.get(quoted_schema, {}).get(table, table)
    return f"{quoted_schema}.{_quote_ident(relation)}"


async def copy_snapshot_candidate(
    session, *, schema_name, table, snapshot_key, build_token, stage_table, columns, expected_count
):
    """Keep COPY buffers bounded while transferring the existing typed stage."""
    candidate = await begin_snapshot_candidate(session, schema_name, table, snapshot_key, build_token)
    schema, stage = _quote_ident(schema_name), _quote_ident(stage_table)
    fields = ", ".join(_quote_ident(column) for column in columns)
    driver = await candidate_driver(session)
    key, key_type = _STAGE_KEYS[table]
    cursor, total_count = None, 0
    while True:
        boundary, count = await _copy_window(session, schema, stage, columns, key, key_type, cursor)
        if not count:
            break
        lower_bound = f"$2::{key_type} IS NULL" if cursor is None else f"{key}>$2::{key_type}"
        with tempfile.TemporaryFile() as copy_payload_file:
            copied = await driver.copy_from_query(
                f"SELECT $1::bigint, {fields} FROM {schema}.{stage} "
                f"WHERE {lower_bound} AND {key}<=$3::{key_type} ORDER BY {key}",
                int(snapshot_key),
                cursor,
                boundary,
                output=copy_payload_file,
                format="binary",
            )
            if copied != f"COPY {count}" or copy_payload_file.tell() > COPY_MAX_BYTES:
                raise RuntimeError("PTG snapshot candidate COPY bound or census differs")
            copy_payload_file.seek(0)
            copied = await driver.copy_to_table(
                candidate,
                schema_name=schema_name,
                columns=("snapshot_key", *columns),
                source=copy_payload_file,
                format="binary",
            )
            if copied != f"COPY {count}":
                raise RuntimeError("PTG snapshot candidate COPY census differs")
        cursor, total_count = boundary, total_count + count
    if total_count != expected_count:
        raise RuntimeError("PTG snapshot candidate COPY census differs")
    return await finish_snapshot_candidate(session, schema_name, candidate, total_count)


async def _copy_window(session, schema, stage, columns, key, key_type, cursor):
    """Select only a batch boundary; account for native binary lengths and framing."""
    sizes = [
        f"CASE WHEN {_quote_ident(column)} IS NULL THEN 0 ELSE {_FIXED_COLUMN_BYTES[column]} END"
        if column in _FIXED_COLUMN_BYTES
        else f"COALESCE(octet_length({_quote_ident(column)}),0)"
        for column in columns
    ]
    row_bytes = str(2 + 4 * (len(columns) + 1) + 8) + "+" + "+".join(sizes)
    lower_bound = f"CAST(:cursor AS {key_type}) IS NULL" if cursor is None else f"{key}>CAST(:cursor AS {key_type})"
    window_result = await session.execute(
        text(f"""
        WITH limited AS MATERIALIZED (
            SELECT {key}, {row_bytes} AS bytes FROM {schema}.{stage}
             WHERE {lower_bound}
             ORDER BY {key} LIMIT :row_limit
        ), bounded AS (
            SELECT {key}, row_number() OVER (ORDER BY {key}) AS row_count,
                   sum(bytes) OVER (ORDER BY {key} ROWS UNBOUNDED PRECEDING) AS total_bytes
              FROM limited
        )
        SELECT last.{key}, last.row_count, EXISTS(SELECT 1 FROM limited)
          FROM (VALUES (1)) seed(value)
          LEFT JOIN LATERAL (SELECT * FROM bounded WHERE total_bytes<=:byte_limit
                            ORDER BY {key} DESC LIMIT 1) last ON true
    """),
        {"cursor": cursor, "row_limit": COPY_MAX_ROWS, "byte_limit": COPY_MAX_BYTES - _COPY_FRAMING_BYTES},
    )
    boundary, count, has_rows = window_result.one()
    if has_rows and count is None:
        raise RuntimeError("PTG snapshot candidate COPY record exceeds byte bound")
    return boundary, int(count or 0)


def _record_bytes(record):
    """Bound native binary COPY storage, including nullable field headers."""
    total = 2 + 4 * len(record)
    for value in record:
        if value is None:
            continue
        if isinstance(value, (bytes, bytearray, memoryview)):
            total += len(value)
        elif isinstance(value, str):
            total += len(value.encode("utf-8"))
        elif isinstance(value, (int, datetime)):
            total += 8
        else:
            raise TypeError("unsupported PTG candidate COPY field")
    return total


async def copy_candidate_records(session, schema_name, candidate, columns, records, *, batch_rows=COPY_MAX_ROWS):
    """Encode a bounded stream without per-record database validation or rereads."""
    if batch_rows <= 0:
        raise ValueError("PTG candidate batch size must be positive")
    driver = await candidate_driver(session)
    pending, byte_count, total_count = [], _COPY_FRAMING_BYTES, 0
    for record in records:
        size = _record_bytes(record)
        if size + _COPY_FRAMING_BYTES > COPY_MAX_BYTES:
            raise RuntimeError("PTG snapshot candidate COPY record exceeds byte bound")
        if pending and (len(pending) >= min(batch_rows, COPY_MAX_ROWS) or byte_count + size > COPY_MAX_BYTES):
            copied = await driver.copy_records_to_table(
                candidate, schema_name=schema_name, columns=columns, records=pending
            )
            if copied != f"COPY {len(pending)}":
                raise RuntimeError("PTG snapshot candidate COPY census differs")
            total_count += len(pending)
            pending, byte_count = [], _COPY_FRAMING_BYTES
        pending.append(record)
        byte_count += size
    if pending:
        copied = await driver.copy_records_to_table(
            candidate, schema_name=schema_name, columns=columns, records=pending
        )
        if copied != f"COPY {len(pending)}":
            raise RuntimeError("PTG snapshot candidate COPY census differs")
        total_count += len(pending)
    return total_count


def _metadata_record(snapshot_key, columns, entry):
    """Keep canonical scalar encoding while SQL validates the complete set."""
    values = (entry[column] for column in columns) if isinstance(entry, Mapping) else entry
    encoded_fields = []
    byte_fields = {"component_global_id_128", "pattern_digest", "member_digest"}
    text_fields = {"relation", "object_kind"}
    for column, value in zip(columns, values, strict=True):
        converter = bytes if column in byte_fields else str if column in text_fields else int
        encoded_fields.append(converter(value or "") if column in text_fields else converter(value))
    return (int(snapshot_key), *encoded_fields)


async def publish_snapshot_records(
    session, *, schema_name, table, snapshot_key, build_token, columns, entries, batch_rows
):
    """Publish an encoded metadata stream with one set check and index build."""
    if batch_rows <= 0:
        raise ValueError("PTG candidate batch size must be positive")
    candidate = await begin_snapshot_candidate(session, schema_name, table, snapshot_key, build_token)
    records = (_metadata_record(snapshot_key, columns, entry) for entry in entries)
    count = await copy_candidate_records(
        session, schema_name, candidate, ("snapshot_key", *columns), records, batch_rows=batch_rows
    )
    return await finish_snapshot_candidate(session, schema_name, candidate, count)
