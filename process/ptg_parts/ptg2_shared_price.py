# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish strict V3 price membership and atom blocks from bounded stages."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping

from db.connection import db
from process.ptg_parts.db_tables import _quote_ident
from process.ptg_parts.ptg2_manifest_publish import (
    _ptg2_manifest_support_stage_table,
    _rewrite_price_atom_lean_dictionary,
)
from process.ptg_parts.ptg2_serving_binary_v3 import select_atom_key_bits
from process.ptg_parts.ptg2_shared_blocks import (
    lock_shared_layout_for_dense_write,
    shared_support_digest,
)
from process.ptg_parts.ptg2_shared_publish import (
    _SHARED_BLOCK_STAGE_COLUMNS,
    create_shared_block_stage,
    publish_shared_block_stage,
    shared_block_stage_name,
)
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary
from process.ptg_parts.snapshot_tables import _ptg2_snapshot_index_name


logger = logging.getLogger(__name__)

_PRICE_MEMBERSHIP_ARTIFACT_KIND = "price_set_atom_memberships_v3"
_PRICE_ATOM_ARTIFACT_KIND = "price_atoms_v3"
_SCANNER_TARGET_COPY_FORMAT_ENV = "HLTHPRT_PTG2_SERVING_BINARY_TARGET_COPY_FORMAT"
_COPY_WORK_MEM_ENV = "HLTHPRT_PTG2_SERVING_BINARY_COPY_WORK_MEM"
_COPY_PARALLEL_WORKERS_ENV = (
    "HLTHPRT_PTG2_SERVING_BINARY_COPY_MAX_PARALLEL_WORKERS_PER_GATHER"
)
_DEFAULT_COPY_WORK_MEM = "128MB"
_MAX_COPY_PARALLEL_WORKERS_PER_GATHER = 8
_MAX_DENSE_KEY_COUNT = 1 << 32
PTG2_V3_PRICE_KEY_ORDER = "minimum_negotiated_rate_then_global_id_128_v1"
_WORK_MEM_RE = re.compile(r"^[1-9][0-9]*(?:kB|MB|GB|TB)?$")
_V3_ATTRIBUTE_KEY_COLUMNS = (
    "negotiated_type_key",
    "expiration_date_key",
    "service_code_key",
    "billing_class_key",
    "setting_key",
    "billing_code_modifier_key",
    "additional_information_key",
)


@dataclass(frozen=True)
class SharedPricePublication:
    object_kinds: tuple[str, ...]
    mapping_count: int
    unique_block_count: int
    price_set_count: int
    atom_count: int
    atom_key_bits: int
    price_attribute_count: int
    price_atom_constant_keys: Mapping[str, int]
    price_atom_constant_values: Mapping[str, Any]
    support_digest: bytes
    stream_summaries: Mapping[str, Mapping[str, Any]]
    logical_byte_count: int
    stored_byte_count: int
    stage_metrics: Mapping[str, Any]


@dataclass(frozen=True)
class PreparedSharedPriceArtifacts:
    schema_name: str
    price_atom_table: str
    price_set_atom_table: str
    price_attr_dictionary_table: str
    price_key_map: str
    atom_key_map: str
    price_set_count: int
    atom_count: int
    atom_key_bits: int
    lean_manifest: Mapping[str, Any]
    stage_metrics: Mapping[str, Any]


@dataclass(frozen=True)
class PreparedSharedPriceKeyMap:
    """Price-key stage that is safe to consume before atom preparation finishes."""

    schema_name: str
    price_key_map: str
    price_set_count: int


async def _await_cleanup_task(
    task: asyncio.Future[Any],
    *,
    propagate_cancellation: bool = False,
) -> Any:
    """Finish cleanup, optionally re-delivering cancellation afterward."""

    pending_cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            pending_cancellation = exc
    result = task.result()
    if pending_cancellation is not None and propagate_cancellation:
        raise pending_cancellation
    return result


def _qualified(schema_name: str, table_name: str) -> str:
    return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"


_PRICE_ATOM_PAYLOAD_COLUMNS = (
    "negotiated_type",
    "negotiated_rate",
    "expiration_date",
    "service_code",
    "billing_class",
    "setting",
    "billing_code_modifier",
    "additional_information",
)


def _row_value(row: Any, key: str, position: int) -> Any:
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return mapping[key]
    if isinstance(row, dict):
        return row[key]
    return row[position]


def _copy_work_mem() -> str | None:
    value = os.getenv(_COPY_WORK_MEM_ENV, _DEFAULT_COPY_WORK_MEM).strip()
    if value.lower() in {"", "0", "none", "off", "false"}:
        return None
    if _WORK_MEM_RE.fullmatch(value):
        return value
    logger.warning("Ignoring unsafe %s=%r", _COPY_WORK_MEM_ENV, value)
    return _DEFAULT_COPY_WORK_MEM


def _copy_parallel_workers_per_gather() -> int | None:
    raw_setting = os.getenv(_COPY_PARALLEL_WORKERS_ENV)
    if raw_setting is None:
        return None
    normalized_setting = raw_setting.strip()
    try:
        parallel_workers = (
            int(normalized_setting)
            if re.fullmatch(r"[0-9]+", normalized_setting)
            else None
        )
    except ValueError:
        parallel_workers = None
    if (
        parallel_workers is None
        or parallel_workers > _MAX_COPY_PARALLEL_WORKERS_PER_GATHER
    ):
        logger.warning(
            "Ignoring invalid %s=%r; expected an integer from 0 to %d",
            _COPY_PARALLEL_WORKERS_ENV,
            raw_setting,
            _MAX_COPY_PARALLEL_WORKERS_PER_GATHER,
        )
        return None
    return parallel_workers or None


async def _set_copy_local_settings(
    driver_connection: Any,
    *,
    work_mem: str | None,
    parallel_workers: int | None,
) -> None:
    if work_mem:
        await driver_connection.execute(f"SET LOCAL work_mem TO '{work_mem}'")
    if parallel_workers is not None:
        await driver_connection.execute(
            f"SET LOCAL max_parallel_workers_per_gather TO {parallel_workers}"
        )


def _parse_shared_price_stream_summary(stderr: bytes) -> dict[str, Any]:
    stderr_text = stderr.decode("utf-8", errors="replace")
    for line in reversed(stderr_text.splitlines()):
        if not line.startswith("PTG2_SERVING_BINARY_COPY\t"):
            continue
        try:
            payload = json.loads(line.split("\t", 1)[1])
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"PTG2 strict price encoder emitted invalid summary JSON: {line}"
            ) from exc
        if isinstance(payload, dict):
            return payload
    raise RuntimeError(
        "PTG2 strict price encoder did not emit a summary: "
        f"{stderr_text[-1000:]}"
    )


async def _feed_encoder_stdin(
    process: Any,
    data: bytes,
    *,
    metrics: dict[str, Any],
    started_at: float,
) -> None:
    if process.stdin is None:
        raise RuntimeError("PTG2 strict price encoder stdin is closed")
    metrics["source_copy_bytes"] = int(metrics.get("source_copy_bytes") or 0) + len(
        data
    )
    metrics.setdefault("source_first_byte_seconds", time.monotonic() - started_at)
    process.stdin.write(data)
    await process.stdin.drain()


async def _close_encoder_stdin(process: Any) -> None:
    if process.stdin is None:
        return
    process.stdin.close()
    try:
        await process.stdin.wait_closed()
    except (BrokenPipeError, ConnectionResetError):
        return


async def _encoder_stdout_chunks(
    process: Any,
    *,
    metrics: dict[str, Any],
    started_at: float,
):
    if process.stdout is None:
        raise RuntimeError("PTG2 strict price encoder stdout is closed")
    while True:
        chunk = await process.stdout.read(1024 * 1024)
        if not chunk:
            break
        metrics["target_copy_bytes"] = int(metrics.get("target_copy_bytes") or 0) + len(
            chunk
        )
        metrics.setdefault("target_first_byte_seconds", time.monotonic() - started_at)
        yield chunk
    metrics["target_copy_complete_seconds"] = time.monotonic() - started_at


async def _copy_price_query_to_encoder(
    source_driver: Any,
    sql: str,
    process: Any,
    *,
    metrics: dict[str, Any],
    started_at: float,
) -> Any:
    async def feed(data: bytes) -> None:
        """Forward one COPY chunk to the encoder and update transfer metrics."""

        await _feed_encoder_stdin(
            process,
            data,
            metrics=metrics,
            started_at=started_at,
        )

    try:
        work_mem = _copy_work_mem()
        parallel_workers = _copy_parallel_workers_per_gather()
        if (work_mem or parallel_workers is not None) and hasattr(
            source_driver, "transaction"
        ):
            async with source_driver.transaction():
                await _set_copy_local_settings(
                    source_driver,
                    work_mem=work_mem,
                    parallel_workers=parallel_workers,
                )
                return await source_driver.copy_from_query(
                    sql,
                    output=feed,
                    format="binary",
                )
        return await source_driver.copy_from_query(
            sql,
            output=feed,
            format="binary",
        )
    finally:
        metrics["source_copy_complete_seconds"] = time.monotonic() - started_at
        await _close_encoder_stdin(process)


async def _stop_price_encoder(
    process: Any,
    stderr_task: asyncio.Task[bytes],
) -> bytes:
    if getattr(process, "returncode", None) is None:
        try:
            process.kill()
        except ProcessLookupError:
            logger.debug("PTG2 strict price encoder exited before cancellation")
    await _close_encoder_stdin(process)
    await process.wait()
    return await stderr_task


async def _copy_through_price_encoder(
    *,
    process: Any,
    sql: str,
    schema_name: str,
    target_table: str,
    metrics: dict[str, Any],
    started_at: float,
) -> None:
    async with db.acquire_driver() as source_driver, db.acquire_driver() as target_driver:
        copy_tasks = [
            asyncio.create_task(
                _copy_price_query_to_encoder(
                    source_driver,
                    sql,
                    process,
                    metrics=metrics,
                    started_at=started_at,
                )
            ),
            asyncio.create_task(
                target_driver.copy_to_table(
                    target_table,
                    source=_encoder_stdout_chunks(
                        process,
                        metrics=metrics,
                        started_at=started_at,
                    ),
                    schema_name=schema_name,
                    columns=list(_SHARED_BLOCK_STAGE_COLUMNS),
                    format="binary",
                )
            ),
        ]
        try:
            await asyncio.gather(*copy_tasks)
        except (Exception, asyncio.CancelledError):
            for task in copy_tasks:
                task.cancel()
            await asyncio.gather(*copy_tasks, return_exceptions=True)
            raise


async def _stream_shared_price_copy(
    *,
    kind: str,
    sql: str,
    schema_name: str,
    target_table: str,
    atom_key_bits: int,
) -> dict[str, Any]:
    binary = _ptg2_rust_scanner_binary()
    if binary is None:
        raise RuntimeError("PTG2 strict price publication requires the Rust scanner binary")
    process = await asyncio.create_subprocess_exec(
        str(binary),
        "--serving-binary-copy-from-key-copy-stdio",
        kind,
        str(atom_key_bits),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env={
            **os.environ,
            _SCANNER_TARGET_COPY_FORMAT_ENV: "postgres_binary_shared_blocks",
        },
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        try:
            process.kill()
        except ProcessLookupError:
            logger.debug("PTG2 strict price encoder exited without complete stdio pipes")
        await process.wait()
        raise RuntimeError("PTG2 strict price encoder did not expose complete stdio pipes")
    stderr_task = asyncio.create_task(process.stderr.read())
    started_at = time.monotonic()
    copy_metric_map: dict[str, Any] = {}
    try:
        await _copy_through_price_encoder(
            process=process,
            sql=sql,
            schema_name=schema_name,
            target_table=target_table,
            metrics=copy_metric_map,
            started_at=started_at,
        )
        await _close_encoder_stdin(process)
        returncode = await process.wait()
        stderr = await stderr_task
    except (Exception, asyncio.CancelledError):
        await _stop_price_encoder(process, stderr_task)
        raise
    if returncode != 0:
        raise RuntimeError(
            f"PTG2 strict price encoder failed for {kind}: "
            f"stderr={stderr.decode('utf-8', errors='replace')[-2000:]}"
        )
    summary = _parse_shared_price_stream_summary(stderr)
    summary.update(copy_metric_map)
    summary["pipeline_seconds"] = time.monotonic() - started_at
    return summary


async def _read_v3_dense_map_metrics(
    *,
    schema_name: str,
    table_name: str,
    id_column: str,
    key_column: str,
    expected_row_count: int | None = None,
) -> dict[str, int | None]:
    """Read dense-map metrics using index probes when CTAS gave an exact count."""

    if expected_row_count is not None and expected_row_count >= 0:
        bounds_row = await db.first(
            f"""
            SELECT
                (
                    SELECT {_quote_ident(key_column)}::bigint
                    FROM {_qualified(schema_name, table_name)}
                    ORDER BY {_quote_ident(key_column)} ASC
                    LIMIT 1
                ) AS minimum_key,
                (
                    SELECT {_quote_ident(key_column)}::bigint
                    FROM {_qualified(schema_name, table_name)}
                    ORDER BY {_quote_ident(key_column)} DESC
                    LIMIT 1
                ) AS maximum_key
            """
        )
        return {
            "row_count": expected_row_count,
            "distinct_id_count": expected_row_count,
            "distinct_key_count": expected_row_count,
            "minimum_key": _row_value(bounds_row, "minimum_key", 0),
            "maximum_key": _row_value(bounds_row, "maximum_key", 1),
        }
    dense_row = await db.first(
        f"""
        SELECT
            COUNT(*)::bigint AS row_count,
            COUNT(DISTINCT {_quote_ident(id_column)})::bigint AS distinct_id_count,
            COUNT(DISTINCT {_quote_ident(key_column)})::bigint AS distinct_key_count,
            MIN({_quote_ident(key_column)})::bigint AS minimum_key,
            MAX({_quote_ident(key_column)})::bigint AS maximum_key
        FROM {_qualified(schema_name, table_name)}
        """
    )
    return {
        "row_count": int(_row_value(dense_row, "row_count", 0) or 0),
        "distinct_id_count": int(
            _row_value(dense_row, "distinct_id_count", 1) or 0
        ),
        "distinct_key_count": int(
            _row_value(dense_row, "distinct_key_count", 2) or 0
        ),
        "minimum_key": _row_value(dense_row, "minimum_key", 3),
        "maximum_key": _row_value(dense_row, "maximum_key", 4),
    }


async def _validate_v3_dense_map(
    *,
    schema_name: str,
    table_name: str,
    id_column: str,
    key_column: str,
    expected_row_count: int | None = None,
) -> dict[str, int | None]:
    """Validate unique dense identifiers and contiguous zero-based keys."""

    dense_map_metrics = await _read_v3_dense_map_metrics(
        schema_name=schema_name,
        table_name=table_name,
        id_column=id_column,
        key_column=key_column,
        expected_row_count=expected_row_count,
    )
    expected_count = int(dense_map_metrics["row_count"] or 0)
    if {
        dense_map_metrics["row_count"],
        dense_map_metrics["distinct_id_count"],
        dense_map_metrics["distinct_key_count"],
    } != {expected_count}:
        raise RuntimeError(
            f"PTG2 v3 dense map count mismatch for {schema_name}.{table_name}: "
            f"{dense_map_metrics}"
        )
    expected_minimum = 0 if expected_count else None
    expected_maximum = expected_count - 1 if expected_count else None
    if (
        dense_map_metrics["minimum_key"] != expected_minimum
        or dense_map_metrics["maximum_key"] != expected_maximum
    ):
        raise RuntimeError(
            f"PTG2 v3 dense map bounds mismatch for {schema_name}.{table_name}: "
            f"{dense_map_metrics}"
        )
    return dense_map_metrics


async def _create_v3_price_key_stage(
    *,
    schema_name: str,
    price_set_summary_table: str,
    price_set_summary_source_count: int | None = None,
    stage_table: str,
) -> dict[str, int | None]:
    """Create dense price keys from scanner-computed exact minimum rates."""

    source_count = (
        int(price_set_summary_source_count)
        if price_set_summary_source_count is not None
        else None
    )
    if source_count is not None and source_count <= 0:
        raise RuntimeError("strict V3 price-set summary source count must be positive")
    qualified_summary = _qualified(schema_name, price_set_summary_table)
    qualified_stage = _qualified(schema_name, stage_table)
    await db.status(f"ANALYZE {qualified_summary};")
    if source_count == 1:
        created_row_count = await db.status(
            f"""
            CREATE UNLOGGED TABLE {qualified_stage} AS
            SELECT
                price_set_global_id_128,
                (ROW_NUMBER() OVER (
                    ORDER BY minimum_negotiated_rate ASC NULLS LAST,
                             price_set_global_id_128
                ) - 1)::bigint AS price_key,
                minimum_negotiated_rate
            FROM {qualified_summary}
            ORDER BY minimum_negotiated_rate ASC NULLS LAST,
                     price_set_global_id_128;
            """
        )
    else:
        created_row_count = await db.status(
            f"""
            CREATE UNLOGGED TABLE {qualified_stage} AS
            WITH canonical AS MATERIALIZED (
                SELECT
                    price_set_global_id_128,
                    MIN(minimum_negotiated_rate) AS minimum_negotiated_rate,
                    MIN(minimum_negotiated_rate) IS DISTINCT FROM
                        MAX(minimum_negotiated_rate) AS payload_conflict
                FROM {qualified_summary}
                GROUP BY price_set_global_id_128
            )
            SELECT
                price_set_global_id_128,
                (ROW_NUMBER() OVER (
                    ORDER BY minimum_negotiated_rate ASC NULLS LAST,
                             price_set_global_id_128
                ) - 1)::bigint AS price_key,
                minimum_negotiated_rate,
                payload_conflict
            FROM canonical
            ORDER BY minimum_negotiated_rate ASC NULLS LAST,
                     price_set_global_id_128;
            """
        )
        if bool(
            await db.scalar(
                f"SELECT EXISTS (SELECT 1 FROM {qualified_stage} "
                "WHERE payload_conflict LIMIT 1)"
            )
        ):
            await db.status(f"DROP TABLE IF EXISTS {qualified_stage} CASCADE;")
            raise RuntimeError(
                "strict V3 observed one price-set ID with conflicting minimum rates"
            )
        await db.status(
            f"ALTER TABLE {qualified_stage} DROP COLUMN payload_conflict;"
        )
    await db.status(
        f"ALTER TABLE {qualified_stage} "
        "ALTER COLUMN price_set_global_id_128 SET NOT NULL, "
        "ALTER COLUMN price_key SET NOT NULL;"
    )
    await db.status(
        f"CREATE UNIQUE INDEX "
        f"{_quote_ident(_ptg2_snapshot_index_name(stage_table, 'id_uidx'))} "
        f"ON {qualified_stage} (price_set_global_id_128);"
    )
    await db.status(
        f"CREATE UNIQUE INDEX "
        f"{_quote_ident(_ptg2_snapshot_index_name(stage_table, 'key_uidx'))} "
        f"ON {qualified_stage} (price_key);"
    )
    await db.status(f"ANALYZE {qualified_stage};")
    dense_stats = await _validate_v3_dense_map(
        schema_name=schema_name,
        table_name=stage_table,
        id_column="price_set_global_id_128",
        key_column="price_key",
        expected_row_count=(
            int(created_row_count)
            if isinstance(created_row_count, int)
            and not isinstance(created_row_count, bool)
            and created_row_count >= 0
            else None
        ),
    )
    if int(dense_stats["row_count"] or 0) > _MAX_DENSE_KEY_COUNT:
        raise RuntimeError("PTG2 v3 supports at most 2^32 canonical price keys")
    return dense_stats


async def _create_v3_atom_key_stage(
    *,
    schema_name: str,
    price_atom_table: str,
    stage_table: str,
) -> dict[str, int | None]:
    created_row_count = await db.status(
        f"""
        CREATE UNLOGGED TABLE {_qualified(schema_name, stage_table)} AS
        SELECT
            price_atom_global_id_128,
            (ROW_NUMBER() OVER (ORDER BY price_atom_global_id_128) - 1)::bigint AS atom_key
        FROM {_qualified(schema_name, price_atom_table)}
        ORDER BY price_atom_global_id_128;
        """
    )
    await db.status(
        f"ALTER TABLE {_qualified(schema_name, stage_table)} "
        "ALTER COLUMN price_atom_global_id_128 SET NOT NULL, "
        "ALTER COLUMN atom_key SET NOT NULL;"
    )
    await db.status(
        f"CREATE UNIQUE INDEX "
        f"{_quote_ident(_ptg2_snapshot_index_name(stage_table, 'id_uidx'))} "
        f"ON {_qualified(schema_name, stage_table)} (price_atom_global_id_128);"
    )
    await db.status(
        f"CREATE UNIQUE INDEX "
        f"{_quote_ident(_ptg2_snapshot_index_name(stage_table, 'key_uidx'))} "
        f"ON {_qualified(schema_name, stage_table)} (atom_key);"
    )
    await db.status(f"ANALYZE {_qualified(schema_name, stage_table)};")
    dense_stats = await _validate_v3_dense_map(
        schema_name=schema_name,
        table_name=stage_table,
        id_column="price_atom_global_id_128",
        key_column="atom_key",
        expected_row_count=(
            int(created_row_count)
            if isinstance(created_row_count, int)
            and not isinstance(created_row_count, bool)
            and created_row_count >= 0
            else None
        ),
    )
    select_atom_key_bits(int(dense_stats["row_count"] or 0))
    return dense_stats


def _v3_price_membership_sql(
    *,
    qualified_price_set_atom_table: str,
    qualified_price_key_map: str,
    qualified_atom_key_map: str,
) -> str:
    """Return every canonical price/atom occurrence in encoder order."""
    return f"""
    SELECT price_key_map.price_key, atom_key_map.atom_key
    FROM {qualified_price_set_atom_table} price_set_atom
    LEFT JOIN {qualified_price_key_map} price_key_map
      ON price_key_map.price_set_global_id_128 = price_set_atom.price_set_global_id_128
    LEFT JOIN {qualified_atom_key_map} atom_key_map
      ON atom_key_map.price_atom_global_id_128 = price_set_atom.price_atom_global_id_128
    ORDER BY price_key_map.price_key, atom_key_map.atom_key
    """


def _v3_price_atom_sql(
    *,
    qualified_price_atom_table: str,
    qualified_atom_key_map: str,
    constant_key_by_column: Mapping[str, Any] | None,
) -> str:
    constant_key_by_name = dict(constant_key_by_column or {})
    attribute_expressions = []
    for column_name in _V3_ATTRIBUTE_KEY_COLUMNS:
        if column_name in constant_key_by_name:
            attribute_expressions.append(
                f"{int(constant_key_by_name[column_name])}::bigint AS {column_name}"
            )
        else:
            attribute_expressions.append(
                f"price_atom.{column_name}::bigint AS {column_name}"
            )
    return f"""
    SELECT
        atom_key_map.atom_key,
        price_atom.negotiated_rate,
        {", ".join(attribute_expressions)}
    FROM {qualified_atom_key_map} atom_key_map
    JOIN {qualified_price_atom_table} price_atom
      ON price_atom.price_atom_global_id_128 = atom_key_map.price_atom_global_id_128
    ORDER BY atom_key_map.price_atom_global_id_128
    """


def _summary_integer(
    summary: Mapping[str, Any],
    label: str,
    *field_names: str,
) -> int:
    for field_name in field_names:
        if summary.get(field_name) is not None:
            try:
                return int(summary[field_name])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"PTG2 v3 {label} summary has invalid {field_name}"
                ) from exc
    raise RuntimeError(
        f"PTG2 v3 {label} summary is missing {' or '.join(field_names)}"
    )


def _validate_summary_kind(summary: Mapping[str, Any], expected_kind: str) -> None:
    artifact_kind = str(summary.get("artifact_kind") or summary.get("kind") or "")
    if artifact_kind != expected_kind:
        raise RuntimeError(
            f"PTG2 v3 stream summary kind mismatch: expected {expected_kind!r}, "
            f"got {artifact_kind!r}"
        )


def _validate_atom_width(
    summary: Mapping[str, Any],
    label: str,
    expected_bits: int,
) -> None:
    summary_bits = summary.get("atom_key_bits")
    summary_bytes = summary.get("atom_key_bytes")
    if summary_bits is None and summary_bytes is None:
        raise RuntimeError(f"PTG2 v3 {label} summary is missing atom-key width")
    if summary_bits is not None and int(summary_bits) != expected_bits:
        raise RuntimeError(f"PTG2 v3 {label} atom-key width mismatch")
    if summary_bytes is not None and int(summary_bytes) != expected_bits // 8:
        raise RuntimeError(f"PTG2 v3 {label} atom-key byte width mismatch")


def _optional_summary_integer(
    summary: Mapping[str, Any],
    label: str,
    field_name: str,
) -> int | None:
    if field_name not in summary:
        raise RuntimeError(f"PTG2 v3 {label} summary is missing {field_name}")
    value = summary[field_name]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"PTG2 v3 {label} summary has invalid {field_name}"
        ) from exc


def _v3_membership_stats_from_summary(
    summary: Mapping[str, Any],
    *,
    price_set_count: int,
    atom_count: int,
    atom_key_bits: int,
) -> dict[str, int | None]:
    _validate_summary_kind(summary, _PRICE_MEMBERSHIP_ARTIFACT_KIND)
    row_count = _summary_integer(summary, "price memberships", "row_count")
    atom_reference_count = _summary_integer(
        summary,
        "price memberships",
        "atom_reference_count",
    )
    streamed_price_set_count = _summary_integer(
        summary,
        "price memberships",
        "price_set_count",
    )
    maximum_price_key = _optional_summary_integer(
        summary,
        "price memberships",
        "maximum_price_key",
    )
    expected_maximum_price_key = price_set_count - 1 if price_set_count else None
    if row_count != atom_reference_count:
        raise RuntimeError("PTG2 v3 price-membership reference count mismatch")
    if streamed_price_set_count != price_set_count:
        raise RuntimeError("PTG2 v3 price-membership stream price-set count mismatch")
    if maximum_price_key != expected_maximum_price_key:
        raise RuntimeError(
            "PTG2 v3 price-membership stream dense price-key bounds mismatch"
        )
    if row_count < streamed_price_set_count:
        raise RuntimeError("PTG2 v3 price-membership stream omitted a membership")
    if atom_count == 0 and row_count != 0:
        raise RuntimeError(
            "PTG2 v3 price-membership stream references an empty atom map"
        )
    _validate_atom_width(summary, "price memberships", atom_key_bits)
    return {
        "row_count": row_count,
        "atom_reference_count": atom_reference_count,
        "price_set_count": streamed_price_set_count,
        "maximum_price_key": maximum_price_key,
    }


def _validate_v3_atom_summary(
    summary: Mapping[str, Any],
    *,
    atom_count: int,
    atom_key_bits: int,
) -> None:
    _validate_summary_kind(summary, _PRICE_ATOM_ARTIFACT_KIND)
    if _summary_integer(summary, "price atoms", "atom_count", "row_count") != atom_count:
        raise RuntimeError("PTG2 v3 price-atom stream row count mismatch")
    if _summary_integer(summary, "price atoms", "attribute_count") != len(
        _V3_ATTRIBUTE_KEY_COLUMNS
    ):
        raise RuntimeError("PTG2 v3 price-atom stream attribute width mismatch")
    _validate_atom_width(summary, "price atoms", atom_key_bits)


async def _normalize_strict_price_atom_stage(
    *,
    schema_name: str,
    price_atom_table: str,
) -> dict[str, int]:
    """Deduplicate cross-file atoms and reject any ID-to-payload conflict."""

    qualified = _qualified(schema_name, price_atom_table)
    dedup_table = _ptg2_snapshot_index_name(price_atom_table, "v3_dedup")
    qualified_dedup = _qualified(schema_name, dedup_table)
    before = int(await db.scalar(f"SELECT COUNT(*) FROM {qualified}") or 0)
    await db.status(f"DROP TABLE IF EXISTS {qualified_dedup} CASCADE;")
    selected_columns = ",\n                ".join(
        ("price_atom_global_id_128", *_PRICE_ATOM_PAYLOAD_COLUMNS)
    )
    await db.status(
        f"""
        CREATE UNLOGGED TABLE {qualified_dedup} AS
        SELECT DISTINCT ON (price_atom_global_id_128)
                {selected_columns}
          FROM {qualified}
         ORDER BY price_atom_global_id_128;
        """
    )
    after = int(await db.scalar(f"SELECT COUNT(*) FROM {qualified_dedup}") or 0)
    if after > before:
        raise RuntimeError("strict V3 price atom dedupe increased row count")
    if after == 0:
        await db.status(f"DROP TABLE IF EXISTS {qualified_dedup} CASCADE;")
        raise RuntimeError("strict V3 cannot publish an empty price atom dictionary")
    if before != after:
        await db.status(
            f"""
            CREATE UNIQUE INDEX {_quote_ident(_ptg2_snapshot_index_name(dedup_table, 'id_uidx'))}
                ON {qualified_dedup} (price_atom_global_id_128);
            """
        )
        payload_row = ", ".join(
            f"source.{_quote_ident(column_name)}"
            for column_name in _PRICE_ATOM_PAYLOAD_COLUMNS
        )
        canonical_row = ", ".join(
            f"canonical.{_quote_ident(column_name)}"
            for column_name in _PRICE_ATOM_PAYLOAD_COLUMNS
        )
        conflict = bool(
            await db.scalar(
                f"""
                SELECT EXISTS (
                    SELECT 1
                      FROM {qualified} AS source
                      JOIN {qualified_dedup} AS canonical
                        USING (price_atom_global_id_128)
                     WHERE ROW({payload_row}) IS DISTINCT FROM ROW({canonical_row})
                     LIMIT 1
                )
                """
            )
        )
        if conflict:
            await db.status(f"DROP TABLE IF EXISTS {qualified_dedup} CASCADE;")
            raise RuntimeError(
                "strict V3 observed one price atom ID with conflicting source payloads"
            )
    await db.status(f"DROP TABLE {qualified};")
    await db.status(
        f"ALTER TABLE {qualified_dedup} RENAME TO {_quote_ident(price_atom_table)};"
    )
    return {
        "rows_before": before,
        "rows_after": after,
        "duplicate_rows_removed": before - after,
        "conflicting_ids": 0,
    }


async def _publish_price_attributes(
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    dictionary_table: str | None,
    constant_values: Mapping[str, Any],
) -> tuple[int, bytes]:
    """Persist price attributes and return their row count and support digest."""

    attribute_rows: list[dict[str, Any]] = []
    if dictionary_table:
        raw_rows = await db.all(
            f"""
            SELECT attr_kind, attr_key, text_value, text_array
              FROM {dictionary_table}
             ORDER BY attr_kind, attr_key
            """
        )
        for raw_row in raw_rows:
            mapping = getattr(raw_row, "_mapping", None)
            attr_kind = str(mapping["attr_kind"] if mapping is not None else raw_row[0])
            attr_key = mapping["attr_key"] if mapping is not None else raw_row[1]
            text_value = mapping["text_value"] if mapping is not None else raw_row[2]
            text_array = mapping["text_array"] if mapping is not None else raw_row[3]
            attribute_value = (
                json.dumps(list(text_array or []), ensure_ascii=True, separators=(",", ":"))
                if attr_kind in {"service_code", "billing_code_modifier"}
                else text_value
            )
            attribute_rows.append(
                {
                    "snapshot_key": int(snapshot_key),
                    "attribute_kind": attr_kind,
                    "attribute_key": int(attr_key),
                    "value": attribute_value,
                }
            )
    schema = _quote_ident(schema_name)
    batch_size = 10_000
    async with db.transaction() as session:
        await lock_shared_layout_for_dense_write(
            session,
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
        )
        for start in range(0, len(attribute_rows), batch_size):
            await session.execute(
                db.text(
                    f"""
                    INSERT INTO {schema}.ptg2_v3_price_attr
                        (snapshot_key, attribute_kind, attribute_key, value)
                    VALUES
                        (:snapshot_key, :attribute_kind, :attribute_key, :value)
                    """
                ),
                attribute_rows[start : start + batch_size],
            )
    digest = shared_support_digest(
        {
            "price_attributes": [
                {
                    "attribute_kind": attribute_row["attribute_kind"],
                    "attribute_key": attribute_row["attribute_key"],
                    "value": attribute_row["value"],
                }
                for attribute_row in attribute_rows
            ],
            "constant_values": dict(constant_values),
        }
    )
    return len(attribute_rows), digest


async def prepare_shared_price_artifacts(
    *,
    schema_name: str,
    manifest_stage_table: str,
    price_set_summary_source_count: int | None = None,
    price_key_ready: Callable[[PreparedSharedPriceKeyMap], None] | None = None,
) -> PreparedSharedPriceArtifacts:
    """Normalize cross-source price stages and assign exact dense keys once.

    ``price_key_ready`` is called synchronously after the independent price-key
    stage commits successfully.  Callers may use that immutable stage while the
    atom dictionary and atom-key stages continue preparing in parallel.
    """

    price_atom_table = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "price_atom",
    )
    price_set_atom_table = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "price_set_atom",
    )
    price_set_summary_table = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "price_set_summary",
    )
    price_attr_dictionary_table = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "v3_price_attr",
    )
    price_key_map = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "v3_price_key",
    )
    atom_key_map = _ptg2_manifest_support_stage_table(
        manifest_stage_table,
        "v3_atom_key",
    )
    for table_name in (price_attr_dictionary_table, price_key_map, atom_key_map):
        await db.status(
            f"DROP TABLE IF EXISTS {_qualified(schema_name, table_name)} CASCADE;"
        )
    try:
        normalized_summary_source_count = (
            int(price_set_summary_source_count)
            if price_set_summary_source_count is not None
            else None
        )
        stage_metrics_map: dict[str, Any] = {
            "price_key_source_mode": (
                "single_scanner_fast_path"
                if normalized_summary_source_count == 1
                else "cross_file_canonicalize"
            ),
            "price_atom_source_mode": (
                "single_scanner_unique_provenance"
                if normalized_summary_source_count == 1
                else "cross_file_canonicalize"
            ),
        }
        parallel_started_at = time.monotonic()

        async def prepare_atom_stages() -> tuple[
            Mapping[str, Any], dict[str, int | None], float
        ]:
            """Canonicalize when needed, rewrite attributes, and build atom keys."""

            if normalized_summary_source_count == 1:
                stage_metrics_map["normalization_seconds"] = 0.0
            else:
                stage_started_at = time.monotonic()
                stage_metrics_map.update(
                    await _normalize_strict_price_atom_stage(
                        schema_name=schema_name,
                        price_atom_table=price_atom_table,
                    )
                )
                stage_metrics_map["normalization_seconds"] = (
                    time.monotonic() - stage_started_at
                )
            stage_started_at = time.monotonic()
            lean_price_manifest = (
                await _rewrite_price_atom_lean_dictionary(
                    schema_name=schema_name,
                    price_atom_table=price_atom_table,
                    price_atom_dictionary_table=price_attr_dictionary_table,
                )
            )
            stage_metrics_map["dictionary_rewrite_seconds"] = (
                time.monotonic() - stage_started_at
            )
            if not isinstance(lean_price_manifest, dict):
                raise RuntimeError(
                    "strict V3 price atom rewrite did not produce a dictionary contract"
                )
            atom_map_started_at = time.monotonic()
            atom_stats = await _create_v3_atom_key_stage(
                schema_name=schema_name,
                price_atom_table=price_atom_table,
                stage_table=atom_key_map,
            )
            if normalized_summary_source_count == 1:
                atom_row_count = int(atom_stats.get("row_count") or 0)
                stage_metrics_map.update(
                    {
                        "rows_before": atom_row_count,
                        "rows_after": atom_row_count,
                        "duplicate_rows_removed": 0,
                        "conflicting_ids": 0,
                    }
                )
            return lean_price_manifest, atom_stats, atom_map_started_at

        async def prepare_price_key_stage() -> dict[str, int | None]:
            """Build and announce the validated dense price-key stage."""

            stage_started_at = time.monotonic()
            price_map_stats = await _create_v3_price_key_stage(
                schema_name=schema_name,
                price_set_summary_table=price_set_summary_table,
                price_set_summary_source_count=normalized_summary_source_count,
                stage_table=price_key_map,
            )
            stage_metrics_map["price_key_build_seconds"] = (
                time.monotonic() - stage_started_at
            )
            price_set_count = int(price_map_stats.get("row_count") or 0)
            if price_set_count <= 0:
                raise RuntimeError(
                    "strict V3 cannot publish an empty price-set dictionary"
                )
            if price_key_ready is not None:
                price_key_ready(
                    PreparedSharedPriceKeyMap(
                        schema_name=schema_name,
                        price_key_map=price_key_map,
                        price_set_count=price_set_count,
                    )
                )
            return price_map_stats

        price_map_task = asyncio.create_task(prepare_price_key_stage())
        atom_stages_task = asyncio.create_task(prepare_atom_stages())
        try:
            price_map_stats, atom_stage_result = await asyncio.gather(
                price_map_task,
                atom_stages_task,
            )
        except BaseException:
            for task in (price_map_task, atom_stages_task):
                task.cancel()
            drain_future = asyncio.gather(
                price_map_task,
                atom_stages_task,
                return_exceptions=True,
            )
            await _await_cleanup_task(drain_future)
            raise
        lean_manifest, atom_map_stats, atom_map_started_at = atom_stage_result
        parallel_finished_at = time.monotonic()
        stage_metrics_map["dense_key_build_seconds"] = (
            parallel_finished_at - atom_map_started_at
        )
        stage_metrics_map["parallel_price_prepare_seconds"] = (
            parallel_finished_at - parallel_started_at
        )
        price_set_count = int(price_map_stats.get("row_count") or 0)
        atom_count = int(atom_map_stats.get("row_count") or 0)
        return PreparedSharedPriceArtifacts(
            schema_name=schema_name,
            price_atom_table=price_atom_table,
            price_set_atom_table=price_set_atom_table,
            price_attr_dictionary_table=price_attr_dictionary_table,
            price_key_map=price_key_map,
            atom_key_map=atom_key_map,
            price_set_count=price_set_count,
            atom_count=atom_count,
            atom_key_bits=select_atom_key_bits(atom_count),
            lean_manifest=lean_manifest,
            stage_metrics=stage_metrics_map,
        )
    except BaseException:
        cleanup_task = asyncio.create_task(
            db.status(
                "DROP TABLE IF EXISTS "
                f"{_qualified(schema_name, price_key_map)}, "
                f"{_qualified(schema_name, atom_key_map)}, "
                f"{_qualified(schema_name, price_attr_dictionary_table)} "
                "CASCADE;"
            )
        )
        await _await_cleanup_task(cleanup_task)
        raise


async def export_shared_price_key_map(
    prepared: PreparedSharedPriceArtifacts | PreparedSharedPriceKeyMap,
    output_path: str | Path,
) -> Path:
    """Export the authoritative map once in its existing dense price-key order."""

    path = Path(output_path)
    query = f"""
        SELECT price_set_global_id_128, price_key
          FROM {_qualified(prepared.schema_name, prepared.price_key_map)}
         ORDER BY price_key
    """
    async with db.acquire_driver() as driver_conn:
        copy_from_query = getattr(driver_conn, "copy_from_query", None)
        if copy_from_query is None:
            raise NotImplementedError("active database driver does not expose COPY TO")
        with path.open("wb") as output:
            await copy_from_query(query, output=output, format="binary")
    if not path.is_file() or path.stat().st_size <= 0:
        raise RuntimeError("strict V3 price-key map export is empty")
    return path


async def cleanup_prepared_shared_price_artifacts(
    prepared: PreparedSharedPriceArtifacts,
) -> None:
    """Drop temporary dense-key and attribute tables for a prepared publication."""

    await db.status(
        "DROP TABLE IF EXISTS "
        f"{_qualified(prepared.schema_name, prepared.price_key_map)}, "
        f"{_qualified(prepared.schema_name, prepared.atom_key_map)}, "
        f"{_qualified(prepared.schema_name, prepared.price_attr_dictionary_table)} "
        "CASCADE;"
    )


async def publish_shared_price_artifacts(
    *,
    schema_name: str,
    manifest_stage_table: str,
    snapshot_key: int,
    build_token: str,
    expected_price_set_count: int,
    expected_price_key_order: str,
    prepared: PreparedSharedPriceArtifacts,
) -> SharedPricePublication:
    """Build only compact price blocks; never materialize the serving-rate relation."""

    if expected_price_key_order != PTG2_V3_PRICE_KEY_ORDER:
        raise RuntimeError("strict V3 finalizer uses an unsupported price-key order")
    if (
        prepared.schema_name != schema_name
        or prepared.price_set_count != int(expected_price_set_count)
    ):
        raise RuntimeError("strict V3 prepared price map disagrees with the direct finalizer")
    price_atom_table = prepared.price_atom_table
    price_set_atom_table = prepared.price_set_atom_table
    price_attr_dictionary_table = prepared.price_attr_dictionary_table
    price_key_map = prepared.price_key_map
    atom_key_map = prepared.atom_key_map
    block_stage = shared_block_stage_name(f"price-{snapshot_key}")
    lean_layout_map = dict(prepared.lean_manifest)
    try:
        price_set_count = prepared.price_set_count
        atom_count = prepared.atom_count
        atom_key_bits = prepared.atom_key_bits
        price_atom_constant_key_by_column = dict(
            lean_layout_map.get("price_atom_constant_keys") or {}
        )
        price_atom_constant_value_by_kind = dict(
            lean_layout_map.get("price_atom_constant_values") or {}
        )
        await create_shared_block_stage(
            schema_name=schema_name,
            stage_table=block_stage,
        )
        membership_sql = _v3_price_membership_sql(
            qualified_price_set_atom_table=_qualified(schema_name, price_set_atom_table),
            qualified_price_key_map=_qualified(schema_name, price_key_map),
            qualified_atom_key_map=_qualified(schema_name, atom_key_map),
        )
        atom_sql = _v3_price_atom_sql(
            qualified_price_atom_table=_qualified(schema_name, price_atom_table),
            qualified_atom_key_map=_qualified(schema_name, atom_key_map),
            constant_key_by_column=price_atom_constant_key_by_column,
        )
        membership_summary, atom_summary = await asyncio.gather(
            _stream_shared_price_copy(
                kind=_PRICE_MEMBERSHIP_ARTIFACT_KIND,
                sql=membership_sql,
                schema_name=schema_name,
                target_table=block_stage,
                atom_key_bits=atom_key_bits,
            ),
            _stream_shared_price_copy(
                kind=_PRICE_ATOM_ARTIFACT_KIND,
                sql=atom_sql,
                schema_name=schema_name,
                target_table=block_stage,
                atom_key_bits=atom_key_bits,
            ),
        )
        _v3_membership_stats_from_summary(
            membership_summary,
            price_set_count=price_set_count,
            atom_count=atom_count,
            atom_key_bits=atom_key_bits,
        )
        _validate_v3_atom_summary(
            atom_summary,
            atom_count=atom_count,
            atom_key_bits=atom_key_bits,
        )
        block_publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table=block_stage,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
        )
        dictionary_table = lean_layout_map.get("price_atom_dictionary_table")
        price_attribute_count, price_support_digest = await _publish_price_attributes(
            schema_name=schema_name,
            snapshot_key=int(snapshot_key),
            build_token=build_token,
            dictionary_table=str(dictionary_table) if dictionary_table else None,
            constant_values=price_atom_constant_value_by_kind,
        )
        return SharedPricePublication(
            object_kinds=block_publication.object_kinds,
            mapping_count=block_publication.mapping_count,
            unique_block_count=block_publication.unique_block_count,
            price_set_count=price_set_count,
            atom_count=atom_count,
            atom_key_bits=atom_key_bits,
            price_attribute_count=price_attribute_count,
            price_atom_constant_keys=price_atom_constant_key_by_column,
            price_atom_constant_values=price_atom_constant_value_by_kind,
            support_digest=price_support_digest,
            stream_summaries={
                _PRICE_MEMBERSHIP_ARTIFACT_KIND: membership_summary,
                _PRICE_ATOM_ARTIFACT_KIND: atom_summary,
            },
            logical_byte_count=block_publication.logical_byte_count,
            stored_byte_count=block_publication.stored_byte_count,
            stage_metrics=prepared.stage_metrics,
        )
    finally:
        await db.status(
            f"DROP TABLE IF EXISTS "
            f"{_qualified(schema_name, block_stage)} CASCADE;"
        )


__all__ = [
    "SharedPricePublication",
    "PreparedSharedPriceArtifacts",
    "PreparedSharedPriceKeyMap",
    "PTG2_V3_PRICE_KEY_ORDER",
    "cleanup_prepared_shared_price_artifacts",
    "export_shared_price_key_map",
    "prepare_shared_price_artifacts",
    "publish_shared_price_artifacts",
]
