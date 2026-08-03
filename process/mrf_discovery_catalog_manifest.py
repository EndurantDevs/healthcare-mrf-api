# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Best-effort post-discovery generation of cached MRF catalog page totals."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import bindparam, case, func, literal_column, update

from api.mrf_discovery_catalog_filters import source_file_identity_scope_condition
from api.mrf_discovery_catalog_manifest import (
    CATALOG_PAGING_MANIFEST_METADATA_KEY,
    _SourceManifestAccumulator,
    _normalized_text,
    _nonnegative_int,
)
from db.models import MRFFile, MRFSource, db


CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE_ENV = (
    "HLTHPRT_MRF_CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE"
)
CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS_ENV = (
    "HLTHPRT_MRF_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS"
)
CATALOG_PAGING_MANIFEST_STREAM_BATCH_SIZE = 1024
DEFAULT_CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE = 250_000
MAX_CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE = 500_000
DEFAULT_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS = 60.0
MAX_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS = 120.0


@dataclass(frozen=True)
class _ManifestStreamResult:
    """Complete sources retained from a bounded scalar manifest scan."""

    manifest_by_source_id: dict[str, _SourceManifestAccumulator]
    is_stream_complete: bool
    has_scan_timed_out: bool
    oversized_source_count: int
    omitted_source_ids: set[str]


@dataclass
class _ManifestScanState:
    """Mutable state for an ordered scalar scan without nested closures."""

    max_file_rows_per_source: int
    manifest_by_source_id: dict[str, _SourceManifestAccumulator] = field(
        default_factory=dict
    )
    current_accumulator: _SourceManifestAccumulator | None = None
    current_source_id: str | None = None
    current_source_file_count: int = 0
    is_current_source_within_budget: bool = True
    is_stream_complete: bool = True
    oversized_source_count: int = 0
    omitted_source_ids: set[str] = field(default_factory=set)

    def record_file(
        self,
        *,
        source_id: str,
        file_id: str,
        plan_reference_count: int,
    ) -> None:
        """Incorporate one validated file or omit its source after the cap."""

        if source_id != self.current_source_id:
            self.finish_current_source()
            self._start_source(source_id)
        self.current_source_file_count += 1
        if self.current_source_file_count > self.max_file_rows_per_source:
            self._omit_oversized_source()
            return
        if self.current_accumulator is None:
            self.is_stream_complete = False
            return
        self.current_accumulator.add_file(file_id, plan_reference_count)

    def finish_current_source(self) -> None:
        """Retain the active accumulator only after an ordered source boundary."""

        if (
            self.current_source_id
            and self.is_current_source_within_budget
            and self.current_accumulator is not None
        ):
            self.manifest_by_source_id[self.current_source_id] = (
                self.current_accumulator
            )

    def mark_incomplete(self) -> None:
        """Prevent publication of the active source after an invalid or timed-out row."""

        self.is_stream_complete = False

    def result(self, *, has_scan_timed_out: bool) -> _ManifestStreamResult:
        """Return the completed-source subset that is safe to publish."""

        return _ManifestStreamResult(
            manifest_by_source_id=self.manifest_by_source_id,
            is_stream_complete=self.is_stream_complete,
            has_scan_timed_out=has_scan_timed_out,
            oversized_source_count=self.oversized_source_count,
            omitted_source_ids=set(self.omitted_source_ids),
        )

    def _start_source(self, source_id: str) -> None:
        self.current_source_id = source_id
        self.current_accumulator = _SourceManifestAccumulator.create(source_id)
        self.current_source_file_count = 0
        self.is_current_source_within_budget = True

    def _omit_oversized_source(self) -> None:
        if not self.is_current_source_within_budget:
            return
        self.is_current_source_within_budget = False
        self.current_accumulator = None
        self.oversized_source_count += 1
        if self.current_source_id:
            self.omitted_source_ids.add(self.current_source_id)


async def refresh_catalog_paging_manifests(
    source_records: Iterable[Mapping[str, Any]],
    *,
    source_discovery_run_id: str | None,
    max_file_rows_per_source: int | None = None,
    timeout_seconds: float | None = None,
) -> int:
    """Best-effort populate manifests after a successful frozen discovery batch.

    The ordered scalar stream derives 100, 250, and 500 row totals together.
    Its deadline retains already-complete sources but conservatively drops the
    active source, whose last file boundary is not proven.  An oversized source
    is similarly omitted while the ordered stream continues to later sources.
    """

    source_ids = _source_ids(source_records)
    source_version = _normalized_text(source_discovery_run_id)
    if not source_ids or not source_version:
        return 0
    deadline_at = asyncio.get_running_loop().time() + _manifest_timeout_seconds(
        timeout_seconds
    )
    try:
        return await _refresh_catalog_paging_manifests(
            source_ids,
            source_version=source_version,
            max_file_rows_per_source=_manifest_file_row_limit(
                max_file_rows_per_source
            ),
            deadline_at=deadline_at,
        )
    except Exception:
        logging.getLogger(__name__).warning(
            "catalog paging manifest generation failed for %d sources",
            len(source_ids),
            exc_info=True,
        )
        return 0


async def _refresh_catalog_paging_manifests(
    source_ids: tuple[str, ...],
    *,
    source_version: str,
    max_file_rows_per_source: int,
    deadline_at: float | None,
) -> int:
    """Build and publish complete per-source cache values within one stream."""

    stream_result = await _stream_source_manifests(
        source_ids,
        max_file_rows_per_source=max_file_rows_per_source,
        deadline_at=deadline_at,
    )
    source_manifest_by_id = _completed_manifests_by_source_id(
        stream_result,
        source_ids,
    )
    if not source_manifest_by_id:
        _log_manifest_scan_result(stream_result, source_count=len(source_ids))
        return 0
    source_metadata_by_id = await _source_metadata_by_id(source_ids)
    update_parameters = _manifest_update_parameters(
        source_manifest_by_id,
        source_metadata_by_id=source_metadata_by_id,
        source_version=source_version,
    )
    published_count = await _publish_manifest_updates(update_parameters)
    _log_manifest_scan_result(
        stream_result,
        source_count=len(source_ids),
        published_count=published_count,
    )
    return published_count


def _completed_manifests_by_source_id(
    stream_result: _ManifestStreamResult,
    source_ids: tuple[str, ...],
) -> dict[str, _SourceManifestAccumulator]:
    source_manifest_by_id = dict(stream_result.manifest_by_source_id)
    if stream_result.is_stream_complete:
        for source_id in source_ids:
            if source_id in stream_result.omitted_source_ids:
                continue
            source_manifest_by_id.setdefault(
                source_id,
                _SourceManifestAccumulator.create(source_id),
            )
    return source_manifest_by_id


def _manifest_update_parameters(
    source_manifest_by_id: Mapping[str, _SourceManifestAccumulator],
    *,
    source_metadata_by_id: Mapping[str, dict[str, Any]],
    source_version: str,
) -> list[dict[str, Any]]:
    update_parameters = []
    for source_id, accumulator in source_manifest_by_id.items():
        source_metadata = source_metadata_by_id.get(source_id)
        if not isinstance(source_metadata, dict):
            continue
        if _normalized_text(source_metadata.get("discovery_run_id")) != source_version:
            continue
        update_parameters.append(
            {
                "manifest_source_id": source_id,
                "manifest_source_version": source_version,
                "manifest_metadata_json": {
                    **source_metadata,
                    CATALOG_PAGING_MANIFEST_METADATA_KEY: accumulator.manifest(
                        source_version
                    ),
                },
            }
        )
    return update_parameters


async def _publish_manifest_updates(update_parameters: list[dict[str, Any]]) -> int:
    if not update_parameters:
        return 0
    manifest_update = (
        update(MRFSource.__table__)
        .where(MRFSource.source_id == bindparam("manifest_source_id"))
        .where(
            MRFSource.metadata_json["discovery_run_id"].as_string()
            == bindparam("manifest_source_version")
        )
        .values(metadata_json=bindparam("manifest_metadata_json"))
    )
    async with db.session() as session:
        await session.execute(manifest_update, update_parameters)
    return len(update_parameters)


async def _stream_source_manifests(
    source_ids: tuple[str, ...],
    *,
    max_file_rows_per_source: int,
    deadline_at: float | None,
) -> _ManifestStreamResult:
    """Stream scalar pagination inputs and retain only complete sources."""

    scan_state = _ManifestScanState(max_file_rows_per_source)
    has_scan_timed_out = False
    file_row_stream = _manifest_file_stream(source_ids)
    try:
        if deadline_at is None:
            await _consume_manifest_file_rows(file_row_stream, scan_state)
        else:
            try:
                async with asyncio.timeout_at(deadline_at):
                    await _consume_manifest_file_rows(file_row_stream, scan_state)
            except TimeoutError:
                scan_state.mark_incomplete()
                has_scan_timed_out = True
    finally:
        await file_row_stream.aclose()
    if scan_state.is_stream_complete:
        scan_state.finish_current_source()
    return scan_state.result(has_scan_timed_out=has_scan_timed_out)


def _manifest_file_stream(
    source_ids: tuple[str, ...],
) -> AsyncIterator[Any]:
    file_table = MRFFile.__table__
    source_table = MRFSource.__table__
    statement = (
        db.select(
            file_table.c.source_id,
            file_table.c.mrf_file_id,
            _plan_reference_count_expression(file_table).label(
                "plan_reference_count"
            ),
        )
        .select_from(
            file_table.join(
                source_table,
                source_table.c.source_id == file_table.c.source_id,
            )
        )
        .where(file_table.c.source_id.in_(source_ids))
        .where(source_file_identity_scope_condition(file_table, source_table))
        .order_by(file_table.c.source_id, file_table.c.mrf_file_id)
        .execution_options(yield_per=CATALOG_PAGING_MANIFEST_STREAM_BATCH_SIZE)
    )
    return statement.iterate()


async def _consume_manifest_file_rows(
    file_row_stream: AsyncIterator[Any],
    scan_state: _ManifestScanState,
) -> None:
    """Feed validated scalar rows into the current ordered source accumulator."""

    async for file_row in file_row_stream:
        file_data = _row_mapping(file_row)
        source_id = _normalized_text(file_data.get("source_id"))
        file_id = _normalized_text(file_data.get("mrf_file_id"))
        plan_reference_count = _nonnegative_int(
            file_data.get("plan_reference_count")
        )
        if not source_id or not file_id or plan_reference_count is None:
            scan_state.mark_incomplete()
            return
        scan_state.record_file(
            source_id=source_id,
            file_id=file_id,
            plan_reference_count=plan_reference_count,
        )
        if not scan_state.is_stream_complete:
            return


def _log_manifest_scan_result(
    stream_result: _ManifestStreamResult,
    *,
    source_count: int,
    published_count: int = 0,
) -> None:
    """Keep cache-generation limits observable without affecting discovery."""

    logger = logging.getLogger(__name__)
    if stream_result.has_scan_timed_out:
        logger.info(
            "catalog paging manifest scan reached its deadline; published %d "
            "complete sources from %d",
            published_count,
            source_count,
        )
    elif not stream_result.is_stream_complete:
        logger.info(
            "catalog paging manifest scan stopped before completion; published "
            "%d complete sources from %d",
            published_count,
            source_count,
        )
    if stream_result.oversized_source_count:
        logger.info(
            "catalog paging manifest skipped %d oversized source(s)",
            stream_result.oversized_source_count,
        )


async def _source_metadata_by_id(
    source_ids: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    source_table = MRFSource.__table__
    source_rows = await db.all(
        db.select(source_table.c.source_id, source_table.c.metadata_json).where(
            source_table.c.source_id.in_(source_ids)
        )
    )
    return {
        source_id: metadata
        for source_row in source_rows
        for source_id, metadata in [
            (
                _normalized_text(_row_mapping(source_row).get("source_id")),
                _metadata_dict(_row_mapping(source_row).get("metadata_json")),
            )
        ]
        if source_id
    }


def _plan_reference_count_expression(file_table: Any) -> Any:
    """Return the SQL equivalent of the response pager's plan-count rule.

    The MRF columns are PostgreSQL ``json`` values.  Keep the scan on native
    JSON operators so PostgreSQL does not cast every large ``plan_info`` value
    to JSONB merely to calculate an array length.
    """

    metadata_plan_info = file_table.c.metadata_json.op("->")("plan_info")
    metadata_plan_count = case(
        (
            func.json_typeof(metadata_plan_info) == "array",
            func.json_array_length(metadata_plan_info),
        ),
        else_=None,
    )
    return func.coalesce(
        metadata_plan_count,
        func.greatest(
            _json_value_count(file_table.c.plan_ids),
            _json_value_count(file_table.c.plan_names),
            _json_value_count(file_table.c.market_types),
        ),
        0,
    )


def _json_value_count(value: Any) -> Any:
    return case(
        (func.json_typeof(value) == "array", func.json_array_length(value)),
        (
            func.json_typeof(value) == "string",
            case(
                (
                    func.btrim(value.op("#>>")(literal_column("'{}'"))) != "",
                    1,
                ),
                else_=0,
            ),
        ),
        else_=0,
    )


def _source_ids(source_records: Iterable[Mapping[str, Any]]) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                source_id
                for source_record in source_records
                for source_id in [_normalized_text(source_record.get("source_id"))]
                if source_id
            }
        )
    )


def _manifest_file_row_limit(candidate: int | None) -> int:
    if candidate is None:
        candidate = _positive_int_env(
            CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE_ENV,
            default=DEFAULT_CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE,
        )
    return min(
        max(int(candidate), 1),
        MAX_CATALOG_PAGING_MANIFEST_MAX_FILE_ROWS_PER_SOURCE,
    )


def _manifest_timeout_seconds(candidate: float | None) -> float:
    if candidate is None:
        raw_value = os.getenv(
            CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS_ENV,
            str(DEFAULT_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS),
        )
        try:
            candidate = float(raw_value)
        except (TypeError, ValueError):
            candidate = DEFAULT_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS
    return min(
        max(float(candidate), 0.1),
        MAX_CATALOG_PAGING_MANIFEST_TIMEOUT_SECONDS,
    )


def _positive_int_env(name: str, *, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError:
        return default
    return value if value > 0 else default


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _row_mapping(row: Any) -> Mapping[str, Any]:
    return row if isinstance(row, Mapping) else row._mapping
