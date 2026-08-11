# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Offline materialization of deterministic formatted address labels."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable

from arq import create_pool

from db.models import db
from process.control_cancel import raise_if_cancelled
from process.ext.address_canon import archive_table_name, _quote_ident, _schema_name
from process.ext.address_format import ADDRESS_FORMAT_SOURCE, ADDRESS_FORMAT_VERSION
from process.live_progress import enqueue_live_progress
from process.redis_config import build_redis_settings
from process.serialization import deserialize_job, serialize_job


ADDRESS_ARCHIVE_QUEUE_NAME = "arq:AddressArchive"
DEFAULT_FORMAT_BATCH_SIZE = 10_000
MAX_FORMAT_BATCH_SIZE = 100_000
_FIRST_ADDRESS_KEY = "00000000-0000-0000-0000-000000000000"


@dataclass(frozen=True)
class AddressFormatRefreshStats:
    """Bounded archive refresh result returned to operators and control runs."""

    scanned: int
    updated: int
    batches: int
    renderer_version: int


def _validated_batch_size(batch_size: int) -> int:
    value = int(batch_size)
    if value < 1 or value > MAX_FORMAT_BATCH_SIZE:
        raise ValueError(
            f"batch_size must be between 1 and {MAX_FORMAT_BATCH_SIZE}"
        )
    return value


def _archive_format_batch_sql(schema: str) -> str:
    qschema = _quote_ident(schema)
    archive = f"{qschema}.{_quote_ident(archive_table_name())}"
    renderer = f"{qschema}.addr_formatted_address_v1"
    return f"""
        WITH batch AS MATERIALIZED (
            SELECT address_key
            FROM {archive}
            WHERE merged_into IS NULL
              AND address_key > CAST(:after_address_key AS uuid)
            ORDER BY address_key
            LIMIT :batch_size
        ), updated AS (
            UPDATE {archive} AS archived
               SET formatted_address = {renderer}(
                       archived.first_line,
                       archived.second_line,
                       archived.city_name,
                       archived.state_name,
                       archived.postal_code,
                       archived.country_code
                   ),
                   formatted_address_version = :renderer_version,
                   formatted_address_source = :renderer_source
              FROM batch
             WHERE archived.address_key = batch.address_key
               AND (
                    archived.formatted_address IS DISTINCT FROM {renderer}(
                        archived.first_line,
                        archived.second_line,
                        archived.city_name,
                        archived.state_name,
                        archived.postal_code,
                        archived.country_code
                    )
                    OR archived.formatted_address_version IS DISTINCT FROM
                       :renderer_version
                    OR archived.formatted_address_source IS DISTINCT FROM
                       :renderer_source
               )
            RETURNING archived.address_key
        )
        SELECT
            count(*)::bigint AS scanned,
            count(updated.address_key)::bigint AS updated,
            (
                SELECT address_key::text
                FROM batch
                ORDER BY address_key DESC
                LIMIT 1
            ) AS last_address_key
        FROM batch
        LEFT JOIN updated USING (address_key);
    """


async def refresh_address_archive_formatted_addresses(
    *,
    schema: str | None = None,
    batch_size: int = DEFAULT_FORMAT_BATCH_SIZE,
    cancel_check: Callable[[], Awaitable[None]] | None = None,
    progress_callback: Callable[[int, int], None] | None = None,
) -> AddressFormatRefreshStats:
    """Render every live archive row offline in stable address-key order."""
    effective_schema = schema or _schema_name()
    effective_batch_size = _validated_batch_size(batch_size)
    batch_sql = _archive_format_batch_sql(effective_schema)
    after_address_key = _FIRST_ADDRESS_KEY
    scanned = updated = batches = 0

    while True:
        if cancel_check:
            await cancel_check()
        batch_result = await db.first(
            batch_sql,
            after_address_key=after_address_key,
            batch_size=effective_batch_size,
            renderer_version=ADDRESS_FORMAT_VERSION,
            renderer_source=ADDRESS_FORMAT_SOURCE,
        )
        batch_scanned = int(batch_result.scanned or 0) if batch_result else 0
        if batch_scanned == 0:
            break
        scanned += batch_scanned
        updated += int(batch_result.updated or 0)
        batches += 1
        after_address_key = str(batch_result.last_address_key)
        if progress_callback:
            progress_callback(scanned, updated)

    return AddressFormatRefreshStats(
        scanned=scanned,
        updated=updated,
        batches=batches,
        renderer_version=ADDRESS_FORMAT_VERSION,
    )


async def process_address_formatted_address(
    ctx: dict[str, Any],
    task: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Refresh archive display labels through the serialized archive worker."""
    task_by_field = task if isinstance(task, dict) else {}
    run_id = str(
        task_by_field.get("run_id") or ctx.get("control_run_id") or ""
    ).strip()

    async def _cancel_check() -> None:
        await raise_if_cancelled(ctx, task_by_field)

    def _progress(scanned: int, updated: int) -> None:
        enqueue_live_progress(
            run_id=run_id or None,
            importer="address-formatted-address",
            status="running",
            phase="formatted address archive refresh",
            unit="address",
            done=scanned,
            total=None,
            pct=None,
            message="rendering deterministic archive display labels",
            updated=updated,
        )

    configured_batch_size = task_by_field.get("batch_size")
    refresh_stats = await refresh_address_archive_formatted_addresses(
        batch_size=(
            DEFAULT_FORMAT_BATCH_SIZE
            if configured_batch_size is None
            else _validated_batch_size(int(configured_batch_size))
        ),
        cancel_check=_cancel_check,
        progress_callback=_progress,
    )
    result_by_field = asdict(refresh_stats)
    enqueue_live_progress(
        run_id=run_id or None,
        importer="address-formatted-address",
        status="succeeded",
        phase="formatted address archive refresh",
        unit="address",
        done=refresh_stats.scanned,
        total=refresh_stats.scanned,
        pct=100,
        message="deterministic archive display labels are current",
        updated=refresh_stats.updated,
        batches=refresh_stats.batches,
        renderer_version=refresh_stats.renderer_version,
    )
    return result_by_field


async def run_address_formatted_address_command(
    *,
    batch_size: int = DEFAULT_FORMAT_BATCH_SIZE,
    enqueue: bool = False,
) -> dict[str, int] | None:
    """Run the refresh inline or enqueue it on the archive worker."""
    task_by_field = {"batch_size": _validated_batch_size(batch_size)}
    if enqueue:
        redis = await create_pool(
            build_redis_settings(),
            job_serializer=serialize_job,
            job_deserializer=deserialize_job,
        )
        await redis.enqueue_job(
            "process_address_formatted_address",
            task_by_field,
            _queue_name=ADDRESS_ARCHIVE_QUEUE_NAME,
        )
        return None
    return await process_address_formatted_address({}, task_by_field)
