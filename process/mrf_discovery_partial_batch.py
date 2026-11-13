# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finish a discovery source batch when a minority of its sources is left out."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
from typing import Any

from sqlalchemy import JSON, String, cast, func, update
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

from db.models import MRFSource, db
from process.mrf_discovery_checkpoints import DiscoverySourceBatchIncomplete, SourceBatchSummary

MAX_FAILED_SOURCE_PERCENT_ENV = "HLTHPRT_MRF_DISCOVERY_MAX_FAILED_SOURCE_PERCENT"
DEFAULT_MAX_FAILED_SOURCE_PERCENT = 10.0


@dataclass(frozen=True)
class PartialSourceBatchSummary(SourceBatchSummary):
    """Describe a batch that finished while some of its sources are left out of the catalog."""

    withdrawn_source_count: int = 0

    def proof_metrics(self) -> dict[str, Any]:
        """Report the counts without the completion proof a partial set cannot give."""

        metrics_by_name = super().proof_metrics()
        del metrics_by_name["discovery_proof_version"]
        metrics_by_name["withdrawn_source_count"] = self.withdrawn_source_count
        return metrics_by_name


def _is_within_failure_limit(withdrawn_source_count: int, source_count: int) -> bool:
    max_failed_percent = float(os.getenv(MAX_FAILED_SOURCE_PERCENT_ENV) or DEFAULT_MAX_FAILED_SOURCE_PERCENT)
    # An outage that fails most sources must fail the run, not shrink the catalog.
    return withdrawn_source_count * 100 <= source_count * max_failed_percent


def source_withdrawal_statement(source_ids: list[str], discovery_run_id: str) -> Any:
    """Build the update that stops listing sources under one discovery run."""

    source_table = MRFSource.__table__
    untagged_metadata = cast(cast(source_table.c.metadata_json, JSONB).op("-")("discovery_run_id"), JSON)
    return (
        update(source_table)
        .where(source_table.c.source_id == func.any(cast(source_ids, ARRAY(String))))
        .where(source_table.c.metadata_json["discovery_run_id"].as_string() == discovery_run_id)
        .values(metadata_json=untagged_metadata)
    )


async def withdraw_sources_from_discovery_run(source_ids: list[str], discovery_run_id: str) -> None:
    """Keep sources that did not complete out of this run's catalog listing."""

    if not source_ids:
        return
    async with db.session() as session:
        await session.execute(source_withdrawal_statement(source_ids, discovery_run_id))


async def _partial_batch(
    error: BaseException, state: Any, source_rows: list[dict[str, Any]]
) -> tuple[PartialSourceBatchSummary, list[dict[str, Any]]] | None:
    """Return the summary and listed sources when few enough sources are left out."""

    if not isinstance(error, DiscoverySourceBatchIncomplete):
        return None
    summary = error.summary
    state.result.source_batch_summary = summary  # a failed run still reports its counts
    if summary.completed_source_count + summary.failed_source_count != summary.source_set_count:
        return None
    from process import mrf_source_discovery as discovery  # that module imports this one

    failed_source_ids = {
        str(source_record.get("source_id") or "").strip()
        for source_record in await state.checkpoint_store.pending_sources(state.checkpoint_root_run_id)
    }
    # Sources that share an index page rely on the one source that crawls it; a URL-only run crawls nothing.
    crawl_owner_ids = discovery._crawl_representative_source_ids(source_rows) if state.crawl else frozenset()
    failed_crawl_owner_ids = failed_source_ids & crawl_owner_ids
    failed_crawl_keys = {
        discovery._crawl_source_identity_key(source_row)
        for source_row in source_rows
        if source_row["source_id"] in failed_crawl_owner_ids
    }
    listed_source_rows = [
        source_row
        for source_row in source_rows
        if source_row["source_id"] not in failed_source_ids
        and discovery._crawl_source_identity_key(source_row) not in failed_crawl_keys
    ]
    listed_source_ids = {source_row["source_id"] for source_row in listed_source_rows}
    withdrawn_source_ids = sorted(
        source_row["source_id"] for source_row in source_rows if source_row["source_id"] not in listed_source_ids
    )
    # The limit counts every source left out, not only the ones whose own checkpoint failed.
    if not listed_source_rows or not _is_within_failure_limit(len(withdrawn_source_ids), len(source_rows)):
        return None
    await withdraw_sources_from_discovery_run(withdrawn_source_ids, state.control_run_id)
    partial_summary = PartialSourceBatchSummary(**asdict(summary), withdrawn_source_count=len(withdrawn_source_ids))
    return partial_summary, listed_source_rows


async def finished_partial_batch(
    error: BaseException,
    state: Any,
    source_rows: list[dict[str, Any]],
    record_failure: Callable[[Any, BaseException], Awaitable[None]],
) -> tuple[PartialSourceBatchSummary, list[dict[str, Any]]]:
    """Return the summary and listed sources of a batch that finished with few sources left out.

    Any other outcome, a failed withdrawal included, is recorded as the run's failure and raised.
    """

    try:
        partial_batch = await _partial_batch(error, state, source_rows)
    except BaseException as partial_error:
        await record_failure(state, partial_error)
        raise
    if partial_batch is None:
        await record_failure(state, error)
        raise error
    return partial_batch
