# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Repair the two owned NPI outputs without changing an incumbent heap."""

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

TARGETS = ("provider_directory_organization", "provider_directory_practitioner")


def _selector(directory, db_schema, run_id, source_ids, seen_table):
    """Normalize value filters; an ordinary seen heap or scratch map grants no authority."""
    require_output(db_schema is None or db_schema == directory._schema(), "schema_changed")
    require_output(seen_table is None, "npi_seen_custody_required")
    overrides = directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
    require_output(not any(target_name in overrides for target_name in TARGETS), "npi_scratch_custody_required")
    require_output(run_id is None or type(run_id) is str and bool(run_id), "npi_run_invalid")
    cleaned = directory._clean_source_id_list(source_ids)
    return {"run_id": run_id, "source_ids": sorted(set(cleaned)) or None}


def npi_projection(directory, target_name, selector):
    """Keep the old native predicate and a complete expected tuple for set parity."""
    require_output(target_name in TARGETS, "npi_target_invalid")
    conditions = ["r.npi IS NULL", "r.source_id<>:cms_npd_source_id", "r.resource_id ~ '^[0-9]{10}$'"]
    params_by_name = {"cms_npd_source_id": directory.CMS_NPD_SOURCE_ID}
    if selector["source_ids"]:
        conditions.append("r.source_id=ANY(CAST(:source_ids AS varchar[]))")
        params_by_name["source_ids"] = selector["source_ids"]
    if selector["run_id"] is not None:
        conditions.append("r.last_seen_run_id=CAST(:run_id AS varchar)")
        params_by_name["run_id"] = selector["run_id"]
    relation = storage_relation(directory._schema(), target_name)
    prefix = f"WITH inputs AS (SELECT r.*,COALESCE(({' AND '.join(conditions)}),FALSE) AS repair_eligible FROM {relation} r) "
    columns = []
    for column in output_models()[target_name].__table__.columns:
        name = str(column.name)
        if name == "npi":
            expression = "CASE WHEN i.repair_eligible THEN i.resource_id::bigint ELSE i.npi END"
        elif name == "updated_at":
            expression = "CASE WHEN i.repair_eligible THEN CAST(:update_timestamp AS timestamp) ELSE i.updated_at END"
        else:
            expression = "i." + directory._q(name)
        columns.append(expression + " AS " + directory._q(name))
    return (
        prefix + "SELECT * FROM inputs",
        prefix + "SELECT " + ",".join(columns) + " FROM inputs i",
        prefix + "SELECT count(*) FILTER(WHERE repair_eligible) FROM inputs",
        params_by_name,
    )


def repair_npi_rows(resource_rows, decision_at):
    """Transform only the native-selected rows and retain every other model cell."""
    repaired_rows = []
    for original in resource_rows:
        projected_by_field = dict(original)
        eligible = projected_by_field.pop("repair_eligible")
        require_output(type(eligible) is bool, "npi_eligibility_changed")
        if eligible:
            projected_by_field["npi"] = int(projected_by_field["resource_id"])
            projected_by_field["updated_at"] = decision_at
        repaired_rows.append(projected_by_field)
    return repaired_rows


async def _capture_operation(directory, selector):
    """Issue a fresh maintenance locator from genuine role and owned-input proof."""
    from process.provider_directory_output_snapshot_provision import _capture

    require_output(
        directory.db._transaction_binding() is None and not directory.db.has_reader_session(),
        "npi_unbound_operation_required",
    )
    attempt_id = uuid4().hex
    async with source_transaction(directory.db, isolation="repeatable read") as session:
        publisher = bound_publisher()
        policy = publisher.policy
        require_output(directory._schema() == policy.schema, "schema_changed")
        native_rows = await _capture(session, policy)
        incumbents_by_target = {
            target_name: custody._native_receipt(policy, native_rows[target_name], target_name)
            for target_name in TARGETS
        }
        roles = await publisher.connection.fetch(
            ROLE_SQL,
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
            policy.owner_role,
            policy.publisher_role,
            policy.schema,
        )
        decision_at = await publisher.connection.fetchval("SELECT localtimestamp")
        counts_by_target = {}
        for target_name in TARGETS:
            _inputs, _expected, count, params_by_name = npi_projection(directory, target_name, selector)
            counts_by_target[target_name] = (await session.execute(text(count), params_by_name)).scalar_one()
        input_digest = custody._digest(
            {
                "roles": policy.role_arguments(),
                "native_role_profile": [dict(role) for role in roles],
                "incumbents": [incumbents_by_target[target_name].to_payload() for target_name in TARGETS],
                "selector": selector,
                "attempt_id": attempt_id,
                "decision_at": decision_at.isoformat(),
                "counts": counts_by_target,
            }
        )
    return (
        dict(
            kind="npi_backfill",
            run_id="npi-" + attempt_id,
            producer="npi-backfill-v1",
            source_pins=[],
            resource_types=["Organization", "Practitioner"],
            source_fence_digest=input_digest,
        ),
        incumbents_by_target,
        counts_by_target,
        decision_at,
    )


@asynccontextmanager
async def _preservation_reader(directory, operation, prepared_stages):
    """Pin both original outputs in one independent genuine Reader transaction."""
    reader_database = await directory.db._connect_reader()
    session, reader = reader_database.session_factory(), None
    try:
        async with custody.output_snapshot_transaction(directory, operation=operation, targets=TARGETS) as capability:
            for stage in prepared_stages:
                await stages._stage_row(capability, stage, phases={"loading"})
            driver = await _require_reader(session, reader_database, capability.policy)
            await _same_database(capability.connection, driver)
            await driver.execute(
                "LOCK TABLE "
                + ",".join(storage_relation(stage.schema, stage.target) for stage in prepared_stages)
                + " IN ACCESS SHARE MODE NOWAIT"
            )
            for stage in prepared_stages:
                require_output(
                    await driver.fetchval(
                        "SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='relation' "
                        "AND relation=$1::oid AND mode='AccessShareLock' AND granted)",
                        stage.incumbent.oid,
                    ),
                    "npi_incumbent_changed",
                )
            reader = _SourceReader(session, capability.policy.schema, {})
        yield reader
    finally:
        if reader is not None:
            reader.deactivate()
        await _close_reader(session)


async def _verify_parity(capability, stage, projection, params_by_name):
    """Compare complete immutable tuples in both directions after all indexes exist."""
    relation = storage_relation(stage.schema, stage.relation)
    statement = f"WITH expected AS MATERIALIZED ({projection}), difference AS ((SELECT to_jsonb(e) FROM expected e EXCEPT ALL SELECT to_jsonb(a) FROM {relation} a) UNION ALL (SELECT to_jsonb(a) FROM {relation} a EXCEPT ALL SELECT to_jsonb(e) FROM expected e)) SELECT NOT EXISTS(SELECT FROM difference)"
    require_output(
        (await capability.session.execute(text(statement), params_by_name)).scalar_one() is True, "npi_parity_failed"
    )


async def _prepare(directory, operation, selector, incumbents_by_target, expected_counts, decision_at, prepared_stages):
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=TARGETS) as capability:
        require_output(
            await custody._capture(capability, {target_name: target_name for target_name in TARGETS})
            == incumbents_by_target,
            "npi_incumbent_changed",
        )
        for target_name in TARGETS:
            stage = await stages.prepare_output_stage(directory, capability, target_name)
            prepared_stages.append(stage)
            await journal.record_output_progress(capability, stage, 0)
    copied_by_target = dict.fromkeys(TARGETS, 0)
    async with _preservation_reader(directory, operation, prepared_stages) as reader:
        for stage in prepared_stages:
            inputs, _expected, count, params_by_name = npi_projection(directory, stage.target, selector)
            require_output(
                await reader.scalar(count, **params_by_name) == expected_counts[stage.target], "npi_incumbent_changed"
            )
            async for resource_rows in projected_pages(reader, inputs, params_by_name, output_models()[stage.target]):
                async with custody.output_snapshot_transaction(
                    directory,
                    operation=operation,
                    targets=TARGETS,
                ) as capability:
                    copied_by_target[stage.target] += await stages.copy_output_rows(
                        directory,
                        capability,
                        stage,
                        repair_npi_rows(resource_rows, decision_at),
                    )
                    await journal.record_output_progress(capability, stage, copied_by_target[stage.target])
    receipts = []
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=TARGETS) as capability:
        require_output(
            await custody._capture(capability, {target_name: target_name for target_name in TARGETS})
            == incumbents_by_target,
            "npi_incumbent_changed",
        )
        for stage in prepared_stages:
            _inputs, expected, _count, params_by_name = npi_projection(directory, stage.target, selector)
            await stages.finish_output_indexes(directory, capability, stage)
            await _verify_parity(capability, stage, expected, {**params_by_name, "update_timestamp": decision_at})
            await stages.seal_output_stage(capability, stage, expected_rows=copied_by_target[stage.target])
            receipts.append(
                await custody.capture_prepared_output(
                    capability,
                    stage.target,
                    stage.relation,
                    expected_incumbent=stage.incumbent,
                )
            )
        await journal.record_output_prepared(capability, receipts)
    return receipts


async def _discard(directory, operation, prepared_stages):
    """Remove only the authenticated still-private heaps belonging to this attempt."""
    async with custody.output_snapshot_transaction(directory, operation=operation, targets=TARGETS) as capability:
        for stage in reversed(prepared_stages):
            oid = await capability.connection.fetchval(
                "SELECT to_regclass($1)::oid::bigint",
                storage_relation(stage.schema, stage.relation),
            )
            if oid is not None:
                await stages.discard_output_stage(capability, stage)


async def backfill_resource_id_npis(directory, db_schema=None, *, run_id=None, source_ids=None, seen_table=None):
    """Publish both exact NPI replacements together; value filters confer no authority."""
    from process.provider_directory_artifact_bundle_preparation import apply_prepared_output_bundle
    from process.provider_directory_artifact_scratch_inputs import prepared_scratch_enrichment
    from process.provider_directory_generic_source_artifacts import prepared_npi_backfill

    prepared = await prepared_npi_backfill(
        directory, db_schema, run_id=run_id, source_ids=source_ids, seen_table=seen_table
    )
    if prepared is not None:
        return prepared

    prepared = await prepared_scratch_enrichment(
        directory, "npi", db_schema=db_schema, run_id=run_id, source_ids=source_ids, seen_table=seen_table
    )
    if prepared is not None:
        return prepared
    selector = _selector(directory, db_schema, run_id, source_ids, seen_table)
    operation, incumbents_by_target, counts_by_target, decision_at = await _capture_operation(directory, selector)
    counts_by_resource = {"Practitioner": counts_by_target[TARGETS[1]], "Organization": counts_by_target[TARGETS[0]]}
    if not any(counts_by_target.values()):
        return counts_by_resource
    prepared_stages = []
    try:
        receipts = await _prepare(
            directory,
            operation,
            selector,
            incumbents_by_target,
            counts_by_target,
            decision_at,
            prepared_stages,
        )

        async def append_history(capability, retained):
            """Record the actual protected generation locator as the cut's final effect."""
            await journal.append_output_history(capability, receipts, retained)

        async with custody.output_snapshot_transaction(directory, operation=operation, targets=TARGETS) as capability:
            await apply_prepared_output_bundle(directory, capability, receipts, after_output_publish=append_history)
        return counts_by_resource
    finally:
        original_error = sys.exception()
        try:
            if prepared_stages:
                await drain_operation(_discard(directory, operation, prepared_stages), preserve_cancellation=True)
        except BaseException:
            if original_error is None:
                raise
            original_error.add_note("provider_directory_npi_stage_cleanup_refused")
