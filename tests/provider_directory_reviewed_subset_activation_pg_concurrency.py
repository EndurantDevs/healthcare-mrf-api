# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Two-connection races for explicit reviewed subset activation."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import os

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_evidence as activation_evidence_api
from process import provider_directory_fhir_subset_activation_store as activation_store
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    activate_source,
    activation_evidence,
    activation_marker,
    is_activation_valid,
    insert_activation_generation,
    load_activation_migration,
    third_matched_terminal,
)
from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
    has_waiting_lock,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    run_subset_migration,
)
from tests.tin_npi_connector_postgres_support import (
    POSTGRES_DSN_ENV,
    asyncpg,
    open_test_connection,
)


def _runtime_database() -> Database:
    database_dsn = os.environ[POSTGRES_DSN_ENV]
    async_database_dsn = database_dsn.replace(
        "postgresql://",
        "postgresql+asyncpg://",
        1,
    )
    engine = create_async_engine(
        async_database_dsn,
        pool_size=1,
        max_overflow=0,
    )
    return Database(
        engine=engine,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
    )


def _authorize_operator(monkeypatch, evidence) -> None:
    manifest = activation.ReviewedSubsetActivationManifest(
        desired_candidate_status=activation.VERIFIED_STATUS,
        evidence=evidence,
    )
    monkeypatch.setenv(activation.STATE_SYNC_ENABLED_ENV, "true")
    monkeypatch.setattr(
        activation,
        "reviewed_subset_activation_manifest",
        lambda: manifest,
    )
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "synthetic-source",
    )


async def _create_activation_scenario(monkeypatch):
    scenario = await create_committed_subset_schema(monkeypatch)
    migration = load_activation_migration()
    async with scenario.connection.transaction():
        await run_subset_migration(
            migration,
            "upgrade",
            scenario.connection,
        )
    evidence_pairs = await insert_activation_generation(scenario)
    evidence = activation_evidence(evidence_pairs)
    _authorize_operator(monkeypatch, evidence)
    return scenario, migration, evidence_pairs


def _extra_alias_sql(scenario) -> str:
    return f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            'synthetic-racing-alias', 'endpoint-a',
            'https://alias.example.test/fhir', false, false, 'none',
            '{{}}'::jsonb, pg_catalog.transaction_timestamp()
        )
    """


async def _assert_postgres_marker(task, marker: str) -> None:
    try:
        await asyncio.wait_for(task, timeout=5)
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker}")


async def _close_scenario(scenario, *resources) -> None:
    for resource in resources:
        with suppress(Exception):
            await resource.disconnect()
        with suppress(Exception):
            await resource.close()
    with suppress(Exception):
        await scenario.connection.execute("ROLLBACK")
    await scenario.connection.execute(
        f"DROP SCHEMA IF EXISTS {scenario.quoted_schema} CASCADE"
    )
    await scenario.connection.close()


async def _activation_first_blocks_alias(monkeypatch) -> None:
    scenario, migration, _ = await _create_activation_scenario(monkeypatch)
    database = _runtime_database()
    alias_connection = await open_test_connection()
    activated = asyncio.Event()
    release_activation = asyncio.Event()
    original_activate = activation_store._activate_source

    async def paused_activate(*args, **kwargs):
        activation_result = await original_activate(*args, **kwargs)
        activated.set()
        await release_activation.wait()
        return activation_result

    monkeypatch.setattr(activation_store, "_activate_source", paused_activate)
    activation_task = asyncio.create_task(
        activation.sync_reviewed_subset_verified_state(database=database)
    )
    alias_task = None
    try:
        await asyncio.wait_for(activated.wait(), timeout=5)
        alias_task = asyncio.create_task(
            alias_connection.execute(_extra_alias_sql(scenario))
        )
        assert await has_waiting_lock(
            scenario.connection,
            alias_connection.get_server_pid(),
            alias_task,
        )
        release_activation.set()
        activation_result = await asyncio.wait_for(activation_task, timeout=5)
        assert activation_result.activated is True
        await _assert_postgres_marker(
            alias_task,
            "provider_directory_reviewed_subset_activation_source_invalid",
        )
        assert await is_activation_valid(scenario, migration) is True
    finally:
        release_activation.set()
        for task in (activation_task, alias_task):
            if task is not None and not task.done():
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task
        await _close_scenario(scenario, database, alias_connection)


async def _alias_first_blocks_activation(monkeypatch) -> None:
    scenario, migration, _ = await _create_activation_scenario(monkeypatch)
    database = _runtime_database()
    alias_connection = await open_test_connection()
    alias_transaction = alias_connection.transaction()
    activation_task = None
    await alias_transaction.start()
    try:
        await alias_connection.execute(_extra_alias_sql(scenario))
        operator_pid = await database.scalar("SELECT pg_catalog.pg_backend_pid()")
        activation_task = asyncio.create_task(
            activation.sync_reviewed_subset_verified_state(database=database)
        )
        assert await has_waiting_lock(
            alias_connection,
            operator_pid,
            activation_task,
        )
        await alias_transaction.commit()
        try:
            await asyncio.wait_for(activation_task, timeout=5)
        except activation.ReviewedSubsetActivationError as error:
            assert error.code == "evidence"
        else:
            raise AssertionError("activation accepted a committed extra alias")
        pending_and_unmarked = await scenario.connection.fetchval(
            f"""
            SELECT metadata_json ->> 'provider_directory_candidate_status' =
                       '{activation.PENDING_STATUS}'
                   AND NOT (
                       metadata_json::jsonb ?
                       'provider_directory_reviewed_subset_activation_v1'
                   )
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = 'synthetic-source'
            """
        )
        assert pending_and_unmarked is True
        await scenario.connection.execute(
            f"""
            DELETE FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = 'synthetic-racing-alias'
            """
        )
        retry_result = await activation.sync_reviewed_subset_verified_state(
            database=database
        )
        assert retry_result.activated is True
        assert await is_activation_valid(scenario, migration) is True
    finally:
        if activation_task is not None and not activation_task.done():
            activation_task.cancel()
            with suppress(asyncio.CancelledError):
                await activation_task
        with suppress(Exception):
            await alias_transaction.rollback()
        await _close_scenario(scenario, database, alias_connection)


async def prove_activation_alias_insert_is_serialized(monkeypatch) -> None:
    """Prove both source-SHARE lock orderings preserve exact aliases."""

    await _activation_first_blocks_alias(monkeypatch)
    await _alias_first_blocks_activation(monkeypatch)


async def prove_activation_busy_is_retryable(monkeypatch) -> None:
    """Prove the endpoint advisory lock returns busy without mutation."""

    scenario, migration, _ = await _create_activation_scenario(monkeypatch)
    database = _runtime_database()
    lock_transaction = scenario.connection.transaction()
    await lock_transaction.start()
    try:
        await scenario.connection.fetchval(
            "SELECT pg_catalog.pg_advisory_xact_lock("
            "pg_catalog.hashtextextended('endpoint-a', 0))"
        )
        try:
            await activation.sync_reviewed_subset_verified_state(
                database=database
            )
        except activation.ReviewedSubsetActivationError as error:
            assert error.code == "busy"
        else:
            raise AssertionError("activation ignored the endpoint lock")
        await lock_transaction.rollback()
        activation_result = await activation.sync_reviewed_subset_verified_state(
            database=database
        )
        assert activation_result.activated is True
        assert await is_activation_valid(scenario, migration) is True
    finally:
        with suppress(Exception):
            await lock_transaction.rollback()
        await _close_scenario(scenario, database)


async def prove_neutral_evidence_renderer(monkeypatch) -> None:
    """Render the exact neutral manifest through SQLAlchemy and asyncpg."""

    scenario, _migration, evidence_pairs = await _create_activation_scenario(
        monkeypatch
    )
    database = _runtime_database()
    expected_evidence = activation_evidence(evidence_pairs)
    try:
        observed_evidence = (
            await activation_evidence_api.reviewed_subset_activation_evidence(
                database=database
            )
        )
        assert observed_evidence == expected_evidence
        rendered_manifest = (
            activation_evidence_api.reviewed_subset_activation_verified_manifest_json(
                observed_evidence
            )
        )
        for private_value in (
            "synthetic-source",
            "endpoint-a",
            "dataset-baseline",
            "dataset-matched",
            "root-baseline",
            "root-matched",
        ):
            assert private_value not in rendered_manifest
    finally:
        await _close_scenario(scenario, database)


async def prove_stale_rr_third_root_rejected(monkeypatch) -> None:
    """Prove stale RR evidence cannot terminalize a post-activation root."""

    scenario, migration, evidence_pairs = await _create_activation_scenario(
        monkeypatch
    )
    stale_connection = await open_test_connection()
    stale_transaction = stale_connection.transaction(isolation="repeatable_read")
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-third-root",
        root_run_id="root-third",
    )
    await insert_valid_subset_resources(scenario, "dataset-third-root")
    await stale_transaction.start()
    try:
        await stale_connection.fetchval(
            f"SELECT count(*) FROM {scenario.quoted_schema}.provider_directory_source"
        )
        async with scenario.connection.transaction():
            await activate_source(
                scenario,
                migration,
                activation_marker(evidence_pairs),
            )
        terminal_statement, terminal_arguments = third_matched_terminal(
            scenario,
            evidence_pairs,
        )
        try:
            await stale_connection.execute(
                terminal_statement,
                *terminal_arguments,
            )
        except asyncpg.PostgresError as error:
            assert error.sqlstate == "40001"
        else:
            raise AssertionError("stale RR root terminalization was accepted")
        await stale_transaction.rollback()
        assert await is_activation_valid(scenario, migration) is True
    finally:
        with suppress(Exception):
            await stale_transaction.rollback()
        await _close_scenario(scenario, stale_connection)
