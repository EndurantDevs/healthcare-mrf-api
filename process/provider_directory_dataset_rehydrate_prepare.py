# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare exact retained-output replacements without changing serving names."""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process import provider_directory_dataset_rehydrate_journal as protocol
from process import provider_directory_dataset_rehydrate_journal_store as journal_store
from process import provider_directory_dataset_rehydrate_pin as pin
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_stage as stages
from process.provider_directory_dataset_rehydrate_pages import _key_columns, projected_pages
from process.provider_directory_dataset_rehydrate_projection import compatibility_projection
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError, _run_cancel_check
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS, output_models


def _require(condition, code):
    if not condition:
        raise DatasetRehydrationError("provider_directory_dataset_rehydrate_" + code)


def _targets(directory, resource_types):
    selected_targets = {directory.RESOURCE_MODELS_BY_TYPE[resource].__tablename__ for resource in resource_types}
    selected_targets.update(("provider_directory_canonical_resource", "provider_directory_source_resource"))
    _require(selected_targets <= set(OUTPUT_ROOTS), "output_invalid")
    return tuple(target for target in OUTPUT_ROOTS if target in selected_targets)


def _source_pin(scope):
    return dict(
        source_id=scope.source_id,
        endpoint_id=scope.endpoint_id,
        dataset_id=scope.dataset_id,
        acquisition_root_run_id=scope.acquisition_root_run_id,
        dataset_hash=scope.dataset_hash,
        resource_hash_contract=scope.resource_hash_contract,
        semantic_projection_as_of=scope.semantic_projection_as_of,
        publication_metadata_sha256=scope.publication_metadata_hash,
    )


@asynccontextmanager
async def _preservation_reader(directory, pinned_source, capability, stage):
    pinned_source.source_relation(directory)
    await custody._require_capability(capability)
    _require(int(os.getenv("HLTHPRT_DB_READER_POOL_MAX_SIZE", "5")) >= 2, "reader_capacity_required")
    reader_database = await directory.db._connect_reader()
    session = reader_database.session_factory()
    reader = None
    try:
        async with asyncio.timeout(5):
            driver = await pin._require_reader(session, reader_database, capability.policy)
            await pin._same_database(capability.connection, driver)
            await driver.execute(
                f"LOCK TABLE {storage_relation(stage.schema, stage.target)} IN ACCESS SHARE MODE NOWAIT"
            )
            locked = await driver.fetchval(
                "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' "
                "AND relation=$1::oid AND mode='AccessShareLock' AND granted)",
                stage.incumbent.oid,
            )
            _require(locked is True, "incumbent_changed")
        reader = pin._SourceReader(session, stage.schema, {})
        yield reader
    finally:
        if reader is not None:
            reader.deactivate()
        await pin._close_reader(session)


async def _verify_projection(capability, stage, statement, params_by_name):
    relation = storage_relation(stage.schema, stage.relation)
    query = f"""WITH expected AS MATERIALIZED ({statement}), difference AS (
      (SELECT to_jsonb(e) AS tuple FROM expected e EXCEPT ALL SELECT to_jsonb(a) FROM {relation} a)
      UNION ALL
      (SELECT to_jsonb(a) FROM {relation} a EXCEPT ALL SELECT to_jsonb(e) FROM expected e)
    ) SELECT NOT EXISTS(SELECT FROM difference)"""
    valid = (await capability.session.execute(text(query), params_by_name)).scalar_one()
    _require(valid is True, "output_parity_failed")


@dataclass(frozen=True)
class PreparedCompatibilityOutputs:
    """Verified private stages and fixed native baseline receipts for the final cut."""

    stages: tuple
    receipts: tuple
    targets: tuple[str, ...]


def _journal_scope(directory, pinned_source, operation, checkpoint_journal):
    pinned_source.source_relation(directory)
    resources = tuple(operation["resource_types"])
    _require(
        operation.get("kind") == "rehydrate"
        and operation.get("source_pins") == [_source_pin(pinned_source.scope)]
        and bool(resources)
        and len(resources) == len(set(resources))
        and set(resources) <= set(pinned_source.scope.resource_types),
        "operation_scope_changed",
    )
    _require(
        type(checkpoint_journal) is journal_store.CheckpointOutputJournal
        and checkpoint_journal.runtime.database is directory.db
        and checkpoint_journal.runtime.schema == directory._schema()
        and checkpoint_journal.scope == pinned_source.scope
        and checkpoint_journal.members == tuple(sorted(resources)),
        "journal_scope_changed",
    )
    return _targets(directory, resources)


async def _recheck_stage(capability, target, entry):
    stage = stages.OutputStage.from_payload(entry["stage"])
    await stages._stage_row(capability, stage, phases={"loading", "sealed"})
    count = await capability.connection.fetchval(
        f"SELECT count(*) FROM {storage_relation(stage.schema, stage.relation)}"
    )
    _require(count == entry["rows"], "stage_count_changed")
    if entry["receipt"] is not None:
        actual = await custody.capture_prepared_output(
            capability, target, stage.relation, expected_incumbent=stage.incumbent
        )
        _require(actual.to_payload() == entry["receipt"], "stage_receipt_changed")


async def _initialize_preparation(directory, operation, targets, checkpoint_journal):
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=targets) as capability:
        async with journal_store.checkpoint_output_transaction(checkpoint_journal) as (_, snapshot):
            value = protocol.begin_preparation(snapshot.journal, operation, targets, checkpoint_journal.members)
            active = value["active"]
            _require(not active["stages"] or set(active["stages"]) == set(targets), "stage_set_changed")
            for target in targets:
                entry = active["stages"].get(target)
                if entry is None:
                    stage = await stages.prepare_output_stage(directory, capability, target)
                    value = protocol.record_stage(value, stage, cursor=None, rows=0)
                    active = value["active"]
                else:
                    await _recheck_stage(capability, target, entry)
            await journal_store.write_output_journal(checkpoint_journal, snapshot, value)
    return value


def _require_progress(snapshot, target, expected):
    active = snapshot.journal["active"]
    _require(active is not None and active["stages"].get(target) == expected, "progress_changed")


async def _copy_pages(directory, pinned_source, operation, targets, checkpoint_journal, entry):
    stage = stages.OutputStage.from_payload(entry["stage"])
    model = output_models()[stage.target]
    statement, params_by_name = compatibility_projection(directory, pinned_source, model, operation["resource_types"])
    async with AsyncExitStack() as readers:
        async with custody.output_snapshot_transaction(directory, operation=operation, targets=targets) as capability:
            await stages._stage_row(capability, stage, phases={"loading"})
            reader = await readers.enter_async_context(
                _preservation_reader(directory, pinned_source, capability, stage)
            )
        async for rows in projected_pages(reader, statement, params_by_name, model, after=entry["cursor"]):
            await _run_cancel_check(checkpoint_journal.runtime)
            async with custody.output_snapshot_transaction(
                directory, operation=operation, targets=targets
            ) as capability:
                async with journal_store.checkpoint_output_transaction(checkpoint_journal) as (_, snapshot):
                    _require_progress(snapshot, stage.target, entry)
                    count = await stages.copy_output_rows(directory, capability, stage, rows)
                    cursor_values = [rows[-1][key] for key in _key_columns(model)]
                    value = protocol.record_stage(
                        snapshot.journal, stage, cursor=cursor_values, rows=entry["rows"] + count
                    )
                    await journal_store.write_output_journal(checkpoint_journal, snapshot, value)
            entry = value["active"]["stages"][stage.target]
    return entry


async def _finish_target(directory, pinned_source, operation, targets, checkpoint_journal, entry):
    stage = stages.OutputStage.from_payload(entry["stage"])
    statement, params_by_name = compatibility_projection(
        directory, pinned_source, output_models()[stage.target], operation["resource_types"]
    )
    await _run_cancel_check(checkpoint_journal.runtime)
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=targets) as capability:
        await stages.finish_output_indexes(directory, capability, stage)
        await _verify_projection(capability, stage, statement, params_by_name)
        async with journal_store.checkpoint_output_transaction(checkpoint_journal) as (_, snapshot):
            _require_progress(snapshot, stage.target, entry)
            await stages.seal_output_stage(capability, stage, expected_rows=entry["rows"])
            receipt = await custody.capture_prepared_output(
                capability, stage.target, stage.relation, expected_incumbent=stage.incumbent
            )
            value = protocol.record_stage(
                snapshot.journal, stage, cursor=entry["cursor"], rows=entry["rows"], receipt=receipt
            )
            if all(item["receipt"] is not None for item in value["active"]["stages"].values()):
                value = protocol.seal_preparation(value)
            await journal_store.write_output_journal(checkpoint_journal, snapshot, value)
    return value


@asynccontextmanager
async def prepared_compatibility_outputs(directory, pinned_source, operation, *, checkpoint_journal):
    """Persist a complete private candidate; interrupted attempts retain exact resumable custody."""
    targets = _journal_scope(directory, pinned_source, operation, checkpoint_journal)
    value = await _initialize_preparation(directory, operation, targets, checkpoint_journal)
    for target in targets:
        entry = value["active"]["stages"][target]
        if entry["receipt"] is None:
            entry = await _copy_pages(directory, pinned_source, operation, targets, checkpoint_journal, entry)
            value = await _finish_target(directory, pinned_source, operation, targets, checkpoint_journal, entry)
    active = value["active"]
    _require(active["operation_digest"] in value["generations"], "preparation_unsealed")
    prepared_stages = tuple(stages.OutputStage.from_payload(active["stages"][target]["stage"]) for target in targets)
    receipts = tuple(
        custody.PreparedOutputReceipt.from_payload(active["stages"][target]["receipt"]) for target in targets
    )
    yield PreparedCompatibilityOutputs(prepared_stages, receipts, targets)
