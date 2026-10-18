# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared bounded staging for the two fixed Location repair producers."""

import sys
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text

from db.plan_release_storage import storage_relation
from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_journal as journal
from process import provider_directory_output_snapshot_stage as stages
from process.plan_release_storage_catalog import ROLE_SQL
from process.provider_directory_dataset_rehydrate_pages import projected_pages
from process.provider_directory_dataset_rehydrate_pin import (
    _close_reader,
    _require_reader,
    _same_database,
    _SourceReader,
)
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_output_snapshot_catalog import output_models, require_output
from process.uhc_flex_practitioner_async_safety import drain_operation

TARGET = "provider_directory_location"
_KINDS = {"contact_backfill": "contact-backfill-v1", "coordinate_backfill": "coordinate-backfill-v1"}


def _repair_selector(kind, selector):
    """Accept only the fixed producers' value selectors, never caller SQL or targets."""
    require_output(type(kind) is str and kind in _KINDS, "repair_kind_invalid")
    if kind == "contact_backfill":
        require_output(
            selector is None
            or type(selector) is dict
            and set(selector) == {"all_locations"}
            and selector["all_locations"] is True,
            "repair_selector_invalid",
        )
        return {"all_locations": True}
    require_output(type(selector) is dict and set(selector) == {"run_id", "source_ids"}, "repair_selector_invalid")
    run_id, source_ids = selector["run_id"], selector["source_ids"]
    require_output(run_id is None or type(run_id) is str and bool(run_id), "repair_selector_invalid")
    require_output(
        source_ids is None
        or type(source_ids) in {list, tuple}
        and all(type(value) is str and value for value in source_ids),
        "repair_selector_invalid",
    )
    return {"run_id": run_id, "source_ids": sorted(set(source_ids or ())) or None}


def _repair_projection(directory, kind, selector):
    """Dispatch only application-owned projections for the complete fixed Location model."""
    selector = _repair_selector(kind, selector)
    if kind == "contact_backfill":
        from process.provider_directory_output_contact_backfill import contact_projection

        projection, count = contact_projection(directory)
        return projection, count, {}
    from process.provider_directory_output_coordinate_backfill import coordinate_projection

    return coordinate_projection(directory, selector)


@asynccontextmanager
async def _preservation_reader(directory, operation, stage):
    """Qualify once, then keep only a separate original Reader across COPY batches."""
    reader_database = await directory.db._connect_reader()
    session = reader_database.session_factory()
    reader = None
    try:
        async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
            await stages._stage_row(capability, stage, phases={"loading"})
            driver = await _require_reader(session, reader_database, capability.policy)
            await _same_database(capability.connection, driver)
            await driver.execute(f"LOCK TABLE {storage_relation(stage.schema, TARGET)} IN ACCESS SHARE MODE NOWAIT")
            require_output(
                await driver.fetchval(
                    "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' "
                    "AND relation=$1::oid AND mode='AccessShareLock' AND granted)",
                    stage.incumbent.oid,
                ),
                "contact_incumbent_changed",
            )
            reader = _SourceReader(session, stage.schema, {})
        yield reader
    finally:
        if reader is not None:
            reader.deactivate()
        await _close_reader(session)


async def _prepare_location(directory, operation, selector, expected_incumbent, prepared_stages):
    projection, changed_count, selection_params = _repair_projection(directory, operation["kind"], selector)
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
        require_output(
            (await custody._capture(capability, {TARGET: TARGET}))[TARGET] == expected_incumbent,
            "contact_incumbent_changed",
        )
        timestamp = await capability.connection.fetchval("SELECT localtimestamp")
        params_by_name = {**selection_params, "update_timestamp": timestamp}
        changed = (await capability.session.execute(text(changed_count), params_by_name)).scalar_one()
        if changed == 0:
            return None, None, 0
        stage = await stages.prepare_output_stage(directory, capability, TARGET)
        prepared_stages.append(stage)
        await journal.record_output_progress(capability, stage, 0)
    copied_rows = 0
    async with _preservation_reader(directory, operation, stage) as reader:
        # Recheck the exact pinned snapshot, before any row transport.
        require_output(await reader.scalar(changed_count, **params_by_name) == changed, "contact_incumbent_changed")
        async for resource_rows in projected_pages(reader, projection, params_by_name, output_models()[TARGET]):
            async with custody.output_snapshot_transaction(
                directory, operation=operation, targets=(TARGET,)
            ) as capability:
                copied_rows += await stages.copy_output_rows(directory, capability, stage, resource_rows)
                await journal.record_output_progress(capability, stage, copied_rows)
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
        await stages.finish_output_indexes(directory, capability, stage)
        await _verify_contact_parity(capability, stage, projection, params_by_name)
        await stages.seal_output_stage(capability, stage, expected_rows=copied_rows)
        receipt = await custody.capture_prepared_output(
            capability, TARGET, stage.relation, expected_incumbent=stage.incumbent
        )
        await journal.record_output_prepared(capability, [receipt])
    return stage, receipt, changed


async def _verify_contact_parity(capability, stage, projection, params_by_name):
    relation = storage_relation(stage.schema, stage.relation)
    statement = f"WITH expected AS MATERIALIZED ({projection}), difference AS ((SELECT to_jsonb(e) FROM expected e EXCEPT ALL SELECT to_jsonb(a) FROM {relation} a) UNION ALL (SELECT to_jsonb(a) FROM {relation} a EXCEPT ALL SELECT to_jsonb(e) FROM expected e)) SELECT NOT EXISTS(SELECT FROM difference)"
    require_output(
        (await capability.session.execute(text(statement), params_by_name)).scalar_one(), "contact_parity_failed"
    )


async def _captured_operation(directory, run_id, *, kind="contact_backfill", selector=None):
    """Bind a real attempt to attested native input, schema and enrolled role identity."""
    from process.provider_directory_output_snapshot_provision import _capture

    require_output(run_id is None or type(run_id) is str and bool(run_id), "contact_run_invalid")
    require_output(
        directory.db._transaction_binding() is None and not directory.db.has_reader_session(),
        "contact_unbound_operation_required",
    )
    selector = _repair_selector(kind, selector)
    attempt_id = uuid4().hex
    async with source_transaction(directory.db, isolation="repeatable read") as session:
        publisher = bound_publisher()
        policy = publisher.policy
        require_output(directory._schema() == policy.schema, "schema_changed")
        native_rows = await _capture(session, policy)
        incumbent = custody._native_receipt(policy, native_rows[TARGET], TARGET)
        native_profile = await publisher.connection.fetch(
            ROLE_SQL,
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
            policy.owner_role,
            policy.publisher_role,
            policy.schema,
        )
        input_digest = custody._digest(
            {
                "schema_oid": incumbent.schema_oid,
                "schema": policy.schema,
                "roles": policy.role_arguments(),
                "native_role_profile": [dict(role) for role in native_profile],
                "incumbents": [incumbent.to_payload()],
                "selector": selector,
                "attempt_id": attempt_id,
            }
        )
    return dict(
        kind=kind,
        run_id=run_id or "contact-" + attempt_id,
        producer=_KINDS[kind],
        source_pins=[],
        resource_types=["Location"],
        source_fence_digest=input_digest,
    ), incumbent


async def _discard(directory, operation, stage):
    if stage is None:
        return
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
        oid = await capability.connection.fetchval(
            "SELECT to_regclass($1)::oid::bigint", storage_relation(stage.schema, stage.relation)
        )
        if oid is not None:
            await stages.discard_output_stage(capability, stage)


async def rebuild_location_snapshot(directory, *, kind, run_id=None, selector=None):
    """Keep the incumbent serving through COPY, indexed validation and retained publication."""
    from process.provider_directory_artifact_bundle_preparation import apply_prepared_output_bundle

    selector = _repair_selector(kind, selector)
    operation, incumbent = await _captured_operation(directory, run_id, kind=kind, selector=selector)
    prepared_stages = []
    try:
        stage, prepared, changed = await _prepare_location(directory, operation, selector, incumbent, prepared_stages)
        if prepared is None:
            return 0

        async def append_history(capability, retained):
            """Record the complete retained generation as the cut's final mutation."""
            await journal.append_output_history(capability, [prepared], retained)

        async with custody.output_snapshot_transaction(directory, operation=operation, targets=(TARGET,)) as capability:
            await apply_prepared_output_bundle(directory, capability, [prepared], after_output_publish=append_history)
        return changed
    finally:
        original_error = sys.exception()
        try:
            for stage in reversed(prepared_stages):
                await drain_operation(_discard(directory, operation, stage), preserve_cancellation=True)
        except BaseException:
            if original_error is None:
                raise
            original_error.add_note("provider_directory_contact_stage_cleanup_refused")
