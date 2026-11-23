# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Completed postal search leaves in the isolated acquisition's storage."""

import asyncio
import json
from typing import Any

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_generic_source_policy as source
from process import provider_directory_uhc_scan as uhc_scan
from process.provider_directory_fetch_types import PartitionFetchState
from process.provider_directory_generic_source_storage import _close_grants, _require
from process.provider_directory_source_checkpoint_control import source_checkpoint_transaction

ROOT = "provider_directory_postal_partition_checkpoint"


async def provision(connection, policy):
    """Install the checkpoint table through the existing Source administrator."""
    _require(connection.is_in_transaction(), "transaction_required")
    relation = storage_relation(policy.schema, ROOT)
    await connection.execute(f"""
        CREATE TABLE IF NOT EXISTS {relation} (
            dataset_id text NOT NULL,
            acquisition_root_run_id text NOT NULL,
            checkpoint_type text NOT NULL,
            partition_identity text NOT NULL,
            pages_fetched bigint NOT NULL CHECK (pages_fetched > 0),
            rows_fetched bigint NOT NULL CHECK (rows_fetched >= 0),
            PRIMARY KEY (dataset_id, acquisition_root_run_id, checkpoint_type, partition_identity)
        )
    """)
    await connection.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(policy.owner_role)}")
    await _close_grants(connection, policy, ROOT)


def isolated_context(source_record, run_id):
    """Use the verified pagination lineage for isolated work only."""
    if getattr(source._FACTORY.get(), "storage_binding", None) is None:
        return None
    context = source_record.get("_pagination_checkpoint_context")
    _require(
        context is not None
        and context.dataset_id
        and context.owner_run_id == run_id
        and (context.retry_of_run_id is None or context.lineage_verified),
        "postal_checkpoint_context_required",
    )
    return context


async def completed_partitions(database, schema, context, checkpoint_type):
    """Read only leaves durably finished in the same acquisition."""
    async with source_checkpoint_transaction(
        database, schema=schema, dataset_ids=(context.dataset_id,), root_run_ids=(context.acquisition_root_run_id,)
    ):
        relation = storage_relation(source.bound_publisher().storage_binding.storage_schema, ROOT)
        rows = await database.all(
            f"""SELECT partition_identity, pages_fetched, rows_fetched FROM {relation}
                WHERE dataset_id=:dataset_id AND acquisition_root_run_id=:root AND checkpoint_type=:checkpoint_type""",
            dataset_id=context.dataset_id,
            root=context.acquisition_root_run_id,
            checkpoint_type=checkpoint_type,
        )
    return {row[0] for row in rows}, sum(row[1] for row in rows), sum(row[2] for row in rows)


async def save_completed_partitions(database, schema, context, rows):
    """Commit a bounded batch after the corresponding resource writes complete."""
    checkpoints = [
        {
            "kind": row["seed_resource_type"],
            "identity": row["seed_resource_id"],
            "page_count": row["pages_fetched"],
            "row_count": row["rows_fetched"],
        }
        for row in rows
    ]
    async with source_checkpoint_transaction(
        database, schema=schema, dataset_ids=(context.dataset_id,), root_run_ids=(context.acquisition_root_run_id,)
    ):
        relation = storage_relation(source.bound_publisher().storage_binding.storage_schema, ROOT)
        await database.status(
            f"""INSERT INTO {relation}
                    (dataset_id, acquisition_root_run_id, checkpoint_type, partition_identity, pages_fetched, rows_fetched)
                SELECT :dataset_id, :root, kind, identity, page_count, row_count
                FROM jsonb_to_recordset(CAST(:checkpoints AS jsonb))
                    AS leaf(kind text, identity text, page_count bigint, row_count bigint)
                ON CONFLICT DO NOTHING""",
            dataset_id=context.dataset_id,
            root=context.acquisition_root_run_id,
            checkpoints=json.dumps(checkpoints),
        )


async def prepare_fetch(
    directory,
    source_record: dict[str, Any],
    resource_type: str,
    model: type,
    start_urls: list[str],
    run_id: str | None,
) -> tuple[asyncio.Queue[str], PartitionFetchState]:
    """Restore completed leaves before queuing any outstanding search."""
    start_url_queue: asyncio.Queue[str] = asyncio.Queue()
    state = PartitionFetchState(result_model=model, retained_resource_rows=[])
    checkpoint_type = directory._uhc_partition_checkpoint_type(source_record, resource_type)
    if run_id and checkpoint_type:
        state.postal_checkpoint_context = isolated_context(source_record, run_id)
    if state.postal_checkpoint_context is None:
        state.completed_partition_identities = await directory._load_partition_checkpoints(
            source_record, resource_type, run_id
        )
    else:
        (
            state.completed_partition_identities,
            state.resumed_partition_pages,
            state.resumed_partition_rows,
        ) = await completed_partitions(
            directory.db, directory._schema(), state.postal_checkpoint_context, checkpoint_type
        )
        if resource_type == "PractitionerRole" and state.postal_checkpoint_context.retry_of_run_id:
            start_urls = _retained_role_starts(
                directory, source_record, start_urls, state.completed_partition_identities
            )
    for request_url in start_urls:
        partition_identity = directory._uhc_partition_identity(
            source_record,
            resource_type,
            request_url,
        )
        if not _partition_already_covered(resource_type, partition_identity, state.completed_partition_identities):
            start_url_queue.put_nowait(request_url)
    return start_url_queue, state


def _retained_role_starts(directory, source_record, start_urls, completed):
    """Reuse numeric ancestors established by completed leaves before ZIP+4 descent."""
    split_prefixes = {
        leaf[:length]
        for leaf in completed
        if 2 <= len(leaf) <= 5 and leaf.isascii() and leaf.isdigit()
        for length in range(1, len(leaf))
    }
    if not split_prefixes:
        return start_urls
    search_param = uhc_scan.POSTAL_SEARCH_PARAMS["PractitionerRole"]
    pending_urls = list(reversed(start_urls))
    zip_starts, other_starts = [], []
    while pending_urls:
        url = pending_urls.pop()
        prefix = directory._uhc_role_postal_prefix(source_record, "PractitionerRole", url)
        if prefix in completed:
            continue
        if prefix in split_prefixes:
            pending_urls.extend(
                directory._url_with_replaced_query_item(url, search_param, child)
                for child in reversed(uhc_scan.postal_child_prefixes(prefix))
            )
        elif prefix and len(prefix) == 5 and prefix.isascii() and prefix.isdigit():
            zip_starts.append(url)
        else:
            other_starts.append(url)
    return zip_starts + other_starts


def _partition_already_covered(resource_type, identity, completed):
    if identity in completed:
        return True
    if resource_type != "Location" or not identity:
        return False
    field, _, zip_code = identity.partition("=")
    if field != "address-postalcode" or len(zip_code) != 5 or not zip_code.isascii() or not zip_code.isdigit():
        return False
    # Older Location runs can have completed a whole prefix before switching to five-digit starts.
    return any(f"{field}={zip_code[:digits]}" in completed for digits in range(1, 5))


async def record_partition(
    directory,
    state: PartitionFetchState,
    source_record: dict[str, Any],
    resource_type: str,
    request_url: str,
    run_id: str | None,
    *,
    pages_fetched: int = 0,
    rows_fetched: int = 0,
) -> None:
    """Record a completed leaf after the partition worker flushes its rows."""
    partition_identity = directory._uhc_partition_identity(
        source_record,
        resource_type,
        request_url,
    )
    checkpoint_type = directory._uhc_partition_checkpoint_type(source_record, resource_type)
    if not partition_identity or not checkpoint_type or not run_id:
        return
    checkpoint_batch_rows: list[dict[str, Any]] = []
    async with state.checkpoint_lock:
        if partition_identity in state.completed_partition_identities:
            return
        state.completed_partition_identities.add(partition_identity)
        checkpoint_row_by_field = directory._reverse_lookup_checkpoint_row(
            source_record, checkpoint_type, partition_identity, run_id
        )
        if state.postal_checkpoint_context is not None:
            checkpoint_row_by_field.update(pages_fetched=pages_fetched, rows_fetched=rows_fetched)
        state.pending_partition_checkpoint_rows.append(checkpoint_row_by_field)
        if len(state.pending_partition_checkpoint_rows) >= directory._partition_checkpoint_flush_rows():
            checkpoint_batch_rows = list(state.pending_partition_checkpoint_rows)
            state.pending_partition_checkpoint_rows.clear()
    if checkpoint_batch_rows:
        await _persist_partitions(directory, state, checkpoint_batch_rows)


async def flush_partitions(directory, state: PartitionFetchState) -> None:
    """Persist any completed leaves from the last partial batch."""
    async with state.checkpoint_lock:
        checkpoint_batch_rows = list(state.pending_partition_checkpoint_rows)
        state.pending_partition_checkpoint_rows.clear()
    if checkpoint_batch_rows:
        await _persist_partitions(directory, state, checkpoint_batch_rows)


async def _persist_partitions(directory, state: PartitionFetchState, checkpoint_rows: list[dict[str, Any]]) -> None:
    if state.postal_checkpoint_context is None:
        await directory._upsert_rows(directory.ProviderDirectoryReverseLookupCheckpoint, checkpoint_rows)
    else:
        await save_completed_partitions(
            directory.db, directory._schema(), state.postal_checkpoint_context, checkpoint_rows
        )
