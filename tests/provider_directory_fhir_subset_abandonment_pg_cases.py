# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL lifecycle proof for reviewed subset abandonment."""
from __future__ import annotations

import asyncio
from contextlib import suppress
import importlib
import json

from process import provider_directory_fhir_subset_abandonment as abandonment
from process import provider_directory_fhir_subset_abandonment_store as store
from process.provider_directory_fhir_subset_abandonment_contract import (
    ABANDONMENT_ENABLED_ENV,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    CANONICAL_API_BASE,
    DATASET_ID,
    ENDPOINT_ID,
    OWNER_RUN_ID,
    ROOT_RUN_ID,
    SOURCE_ID,
    authorize_operator,
    close_abandonment_scenario,
    create_abandonment_relations,
    guard_handoff_context,
    install_admission_query_surface,
    retained_evidence_snapshot,
    runtime_database,
    seed_expired_root,
)
from tests.provider_directory_fhir_subset_abandonment_pg_assertions import (
    assert_scope_domains_are_distinct,
    assert_serving_alias_and_decoy_preserved,
    prove_collapsed_scope_domains_are_rejected,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    load_activation_migration,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
    has_waiting_lock,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    load_abandonment_migration,
    load_payload_guard_repair_migration,
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_support import RESOURCE_TYPES
from tests.tin_npi_connector_postgres_support import (
    asyncpg,
    expect_postgres_error,
    open_test_connection,
)


async def _install_abandonment_predecessors(scenario) -> None:
    await create_abandonment_relations(scenario)
    async with scenario.connection.transaction():
        await run_subset_migration(
            load_activation_migration(),
            "upgrade",
            scenario.connection,
        )
        await run_subset_migration(
            load_payload_guard_repair_migration(),
            "upgrade",
            scenario.connection,
        )


async def _prove_adoption_rejects_preenacted_state(
    scenario,
    migration,
) -> None:
    adoption_transaction = scenario.connection.transaction()
    await adoption_transaction.start()
    try:
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint
               SET state = 'acquisition_abandoned'
             WHERE dataset_id = $1
               AND resource_type = 'Location'
            """,
            DATASET_ID,
        )
        try:
            await run_subset_migration(
                migration,
                "upgrade",
                scenario.connection,
            )
        except asyncpg.PostgresError as error:
            assert "provider_directory_subset_abandonment_adoption_forbidden" in str(
                error
            )
        else:
            raise AssertionError("pre-enacted abandonment was adopted")
    finally:
        await adoption_transaction.rollback()
async def _prove_busy_locks_preserve_evidence(scenario, database) -> None:
    for lock_key in (
        f"provider-directory-pagination:{CANONICAL_API_BASE}",
        ENDPOINT_ID,
    ):
        before_by_relation = await retained_evidence_snapshot(scenario)
        lock_transaction = scenario.connection.transaction()
        await lock_transaction.start()
        try:
            await scenario.connection.fetchval(
                "SELECT pg_catalog.pg_advisory_xact_lock("
                "pg_catalog.hashtextextended($1, 0))",
                lock_key,
            )
            try:
                await abandonment.abandon_reviewed_subset_expired_root(
                    database=database
                )
            except abandonment.ReviewedSubsetAbandonmentError as error:
                assert error.code == "busy"
            else:
                raise AssertionError("abandonment ignored its advisory lock")
        finally:
            await lock_transaction.rollback()
        assert await retained_evidence_snapshot(scenario) == before_by_relation


async def _checkpoint_resume_after_guard(importer, scenario, waiter):
    """Return the sealed root's production resume state after guard release."""

    checkpoint_by_field = dict(
        await waiter.fetchrow(
            f"""
            SELECT dataset_id, source_ids, source_scope_hash,
                   acquisition_root_run_id, owner_run_id,
                   retry_of_run_id, start_url_hash,
                   next_url, state, pages_processed, rows_processed,
                   recent_cursor_hashes, completeness_json
              FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
             WHERE dataset_id = $1
             ORDER BY resource_type
             LIMIT 1
            """,
            DATASET_ID,
        )
    )
    old_context = importer.PaginationCheckpointContext(
        canonical_api_base=CANONICAL_API_BASE,
        source_scope_hash=checkpoint_by_field["source_scope_hash"],
        source_ids=tuple(checkpoint_by_field["source_ids"]),
        owner_run_id=checkpoint_by_field["owner_run_id"],
        retry_of_run_id=checkpoint_by_field["retry_of_run_id"],
        acquisition_root_run_id=checkpoint_by_field["acquisition_root_run_id"],
        dataset_id=checkpoint_by_field["dataset_id"],
        lineage_verified=True,
    )
    return importer._compatible_pagination_resume_state(
        checkpoint_by_field,
        old_context,
        checkpoint_by_field["start_url_hash"],
    )


async def _admit_fresh_candidate_after_guard(importer, database):
    """Run the production endpoint-conflict check for one fresh candidate."""

    fresh_candidate = importer.EndpointDatasetCandidate(
        endpoint_id=ENDPOINT_ID,
        dataset_id="dataset-fresh-admission",
        acquisition_root_run_id="root-fresh-admission",
        source_ids=(SOURCE_ID,),
        selected_resources=tuple(RESOURCE_TYPES),
        import_run_id="owner-fresh-admission",
        previous_dataset_id=None,
        resource_hash_contract=(
            importer.TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        ),
    )
    async with database.acquire() as connection:
        return await importer._assert_no_conflicting_endpoint_candidate(
            connection,
            fresh_candidate,
        )


async def _fresh_admission_after_guard(
    importer,
    scenario,
    database,
    waiter,
    lock_key: str,
):
    """Wait for the crawl guard, then inspect and admit production state."""

    await waiter.fetchval(
        "SELECT pg_catalog.pg_advisory_lock("
        "pg_catalog.hashtextextended($1, 0))",
        lock_key,
    )
    try:
        old_status = await waiter.fetchval(
            f"SELECT status FROM {scenario.quoted_schema}."
            "provider_directory_endpoint_dataset WHERE dataset_id = $1",
            DATASET_ID,
        )
        resume_state = await _checkpoint_resume_after_guard(
            importer,
            scenario,
            waiter,
        )
        admitted_candidate = await _admit_fresh_candidate_after_guard(
            importer,
            database,
        )
        return old_status, resume_state, admitted_candidate
    finally:
        await waiter.fetchval(
            "SELECT pg_catalog.pg_advisory_unlock("
            "pg_catalog.hashtextextended($1, 0))",
            lock_key,
        )


async def _install_guard_handoff_scenario(scenario) -> None:
    """Install predecessor state, retained evidence, and abandonment guards."""

    await _install_abandonment_predecessors(scenario)
    await seed_expired_root(scenario)
    async with scenario.connection.transaction():
        await install_admission_query_surface(scenario)
        await run_subset_migration(
            load_abandonment_migration(),
            "upgrade",
            scenario.connection,
        )


async def prove_reviewed_subset_abandonment_guard_handoff(monkeypatch) -> None:
    """Commit the seal before a queued fresh worker can acquire its guard."""

    importer = importlib.import_module("process.provider_directory_fhir")
    scenario = await create_committed_subset_schema(monkeypatch)
    database = runtime_database()
    waiter = await open_test_connection()
    fresh_task = None
    try:
        await _install_guard_handoff_scenario(scenario)
        monkeypatch.setattr(importer, "db", database)
        context = guard_handoff_context(importer)
        async with importer._pagination_checkpoint_worker_guard(context) as lease:
            assert lease is not None
            fresh_task = asyncio.create_task(
                _fresh_admission_after_guard(
                    importer,
                    scenario,
                    database,
                    waiter,
                    lease.lock_key,
                )
            )
            assert await has_waiting_lock(
                scenario.connection,
                waiter.get_server_pid(),
                fresh_task,
            )
            abandonment_result = (
                await store.sync_reviewed_subset_abandonment_transaction(
                    lease.database,
                    SOURCE_ID,
                    tuple(RESOURCE_TYPES),
                    held_pagination_guard_key=lease.lock_key,
                )
            )
            assert abandonment_result.abandoned is True
            assert fresh_task.done() is False
            assert await scenario.connection.fetchval(
                f"SELECT status FROM {scenario.quoted_schema}."
                "provider_directory_endpoint_dataset WHERE dataset_id = $1",
                DATASET_ID,
            ) == "acquisition_abandoned"
        old_status, resume_state, admitted_candidate = await asyncio.wait_for(
            fresh_task,
            timeout=5,
        )
        assert old_status == "acquisition_abandoned"
        assert resume_state is None
        assert admitted_candidate.dataset_id == "dataset-fresh-admission"
    finally:
        if fresh_task is not None and not fresh_task.done():
            fresh_task.cancel()
            with suppress(asyncio.CancelledError):
                await fresh_task
        await close_abandonment_scenario(scenario, database, waiter)


async def _assert_postgres_marker(task, marker: str) -> None:
    try:
        await asyncio.wait_for(task, timeout=5)
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker}")


async def _seal_while_writer_waits(monkeypatch, scenario, database, writer) -> None:
    selection_locked = asyncio.Event()
    release_operator = asyncio.Event()
    original_abandon_checkpoints = store._abandon_checkpoints

    async def paused_abandon_checkpoints(*args, **kwargs):
        selection_locked.set()
        await release_operator.wait()
        return await original_abandon_checkpoints(*args, **kwargs)

    monkeypatch.setattr(store, "_abandon_checkpoints", paused_abandon_checkpoints)
    operator_task = asyncio.create_task(
        abandonment.abandon_reviewed_subset_expired_root(database=database)
    )
    writer_task = None
    try:
        await asyncio.wait_for(selection_locked.wait(), timeout=5)
        writer_task = asyncio.create_task(
            writer.execute(
                f"""
                INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
                    dataset_id, resource_type, resource_id, payload_hash,
                    payload_json, acquired_resource_sha256
                ) VALUES ($1, 'Location', 'racing-resource', $2, '{{}}'::jsonb, $3)
                """,
                DATASET_ID,
                "a" * 64,
                "b" * 64,
            )
        )
        assert await has_waiting_lock(
            scenario.connection,
            writer.get_server_pid(),
            writer_task,
        )
        release_operator.set()
        abandonment_result = await asyncio.wait_for(operator_task, timeout=5)
        assert abandonment_result.abandoned is True
        await _assert_postgres_marker(
            writer_task,
            "provider_directory_subset_abandonment_child_immutable",
        )
    finally:
        release_operator.set()
        for pending_task in (operator_task, writer_task):
            if pending_task is not None and not pending_task.done():
                pending_task.cancel()
                with suppress(asyncio.CancelledError):
                    await pending_task


async def _assert_sealed_state(scenario, migration, before_by_relation) -> None:
    after_by_relation = await retained_evidence_snapshot(scenario)
    assert after_by_relation == before_by_relation
    parent_by_field = await scenario.connection.fetchrow(
        f"""
        SELECT status, is_current, resource_count,
               completion_proof_json, completion_proof_sha256,
               validated_at, published_at, publication_metadata_json::jsonb AS metadata
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = $1
        """,
        DATASET_ID,
    )
    assert parent_by_field["status"] == "acquisition_abandoned"
    assert parent_by_field["is_current"] is False
    assert parent_by_field["resource_count"] == 7
    assert parent_by_field["completion_proof_json"] is None
    assert parent_by_field["completion_proof_sha256"] is None
    assert parent_by_field["validated_at"] is None
    assert parent_by_field["published_at"] is None
    assert (
        "provider_directory_reviewed_subset_abandonment_v1"
        in parent_by_field["metadata"]
    )
    checkpoint_count = await scenario.connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_pagination_checkpoint
         WHERE dataset_id = $1
           AND state = 'acquisition_abandoned'
           AND completed_at IS NOT NULL
        """,
        DATASET_ID,
    )
    assert checkpoint_count == 7
    await assert_serving_alias_and_decoy_preserved(scenario)
    await assert_scope_domains_are_distinct(scenario)
    is_valid = await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}." f'"{migration._VALID}"($1)',
        DATASET_ID,
    )
    assert is_valid is True


async def _prove_post_seal_guards(scenario) -> None:
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-fresh",
        root_run_id="root-fresh",
        resource_count=0,
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_abandonment_immutable",
        f"UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset "
        "SET resource_count = resource_count + 1 WHERE dataset_id = $1",
        DATASET_ID,
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_abandonment_checkpoint_immutable",
        f"UPDATE {scenario.quoted_schema}.provider_directory_pagination_checkpoint "
        "SET pages_processed = pages_processed + 1 WHERE dataset_id = $1",
        DATASET_ID,
    )
    for relation in (
        "provider_directory_dataset_resource",
        "provider_directory_dataset_proof_shard",
    ):
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_abandonment_child_immutable",
            f"DELETE FROM {scenario.quoted_schema}.{relation} WHERE dataset_id = $1",
            DATASET_ID,
        )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_abandonment_child_immutable",
        _bulk_root_poison_sql(scenario),
        ROOT_RUN_ID,
    )


def _bulk_root_poison_sql(scenario) -> str:
    return f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_bulk_acquisition_checkpoint (
            checkpoint_id, canonical_api_base, resource_type, source_scope_hash,
            strategy_version, acquisition_root_run_id, owner_run_id,
            endpoint_id, dataset_id, start_url_hash, state,
            rows_written, created_at, updated_at
        ) VALUES (
            'bulk-poison', '{CANONICAL_API_BASE}', 'Location', '{'1' * 64}',
            'synthetic-v1', $1, 'owner-fresh', '{ENDPOINT_ID}',
            'dataset-fresh', '{'2' * 64}', 'accepted', 0, now(), now()
        )
    """


async def _prove_downgrade_is_blocked(scenario, migration) -> None:
    downgrade_transaction = scenario.connection.transaction()
    await downgrade_transaction.start()
    try:
        await run_subset_migration(migration, "downgrade", scenario.connection)
    except asyncpg.PostgresError as error:
        assert "provider_directory_subset_abandonment_downgrade_blocked" in str(error)
    else:
        raise AssertionError("sealed abandonment allowed a downgrade")
    finally:
        await downgrade_transaction.rollback()


async def prove_reviewed_subset_abandonment_lifecycle(monkeypatch) -> None:
    """Prove adoption, serialization, sealing, replay, and immutability."""

    scenario = await create_committed_subset_schema(monkeypatch)
    migration = load_abandonment_migration()
    database = runtime_database()
    writer = await open_test_connection()
    try:
        await _install_abandonment_predecessors(scenario)
        await seed_expired_root(scenario)
        await _prove_adoption_rejects_preenacted_state(scenario, migration)
        async with scenario.connection.transaction():
            await run_subset_migration(migration, "upgrade", scenario.connection)
        authorize_operator(monkeypatch, ABANDONMENT_ENABLED_ENV)
        await prove_collapsed_scope_domains_are_rejected(
            scenario,
            database,
            abandonment,
        )
        await _prove_busy_locks_preserve_evidence(scenario, database)
        before_by_relation = await retained_evidence_snapshot(scenario)
        await _seal_while_writer_waits(monkeypatch, scenario, database, writer)
        await _assert_sealed_state(scenario, migration, before_by_relation)
        replay_result = await abandonment.abandon_reviewed_subset_expired_root(
            database=database
        )
        assert replay_result.abandoned is False
        assert replay_result.is_already_applied is True
        await _prove_post_seal_guards(scenario)
        await _prove_downgrade_is_blocked(scenario, migration)
    finally:
        await close_abandonment_scenario(scenario, database, writer)
