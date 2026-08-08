# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Two-connection baseline-generation race proof for subset completion."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import uuid

from tests.provider_directory_subset_completion_pg_setup import (
    extend_source_fixture_table,
    insert_subset_candidate,
    insert_valid_subset_resources,
    load_migration as load_subset_migration,
    replace_subset_source,
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_support import (
    terminal_metadata,
    terminal_parameters,
    terminal_sql,
    valid_evidence_pairs,
)
from tests.tin_npi_connector_postgres_support import (
    asyncpg,
    create_fence_tables,
    load_guard_migration,
    load_migration,
    open_test_connection,
    run_migration,
)


@dataclass(frozen=True)
class _ScenarioView:
    connection: object
    schema: str

    @property
    def quoted_schema(self) -> str:
        return f'"{self.schema}"'


async def create_committed_subset_schema(monkeypatch):
    connection = await open_test_connection()
    schema = f"provider_subset_race_test_{uuid.uuid4().hex}"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await create_fence_tables(connection, schema)
    await run_migration(load_migration(), "upgrade", connection)
    await run_migration(load_guard_migration(), "upgrade", connection)
    scenario = _ScenarioView(connection, schema)
    await extend_source_fixture_table(scenario)
    async with connection.transaction():
        await run_subset_migration(
            load_subset_migration(),
            "upgrade",
            connection,
        )
    return scenario


async def _insert_racing_candidates(scenario):
    await replace_subset_source(
        scenario,
        "pending_two_matching_reviewed_subset_acquisitions",
    )
    for dataset_id, root_run_id in (
        ("dataset-race-a", "root-race-a"),
        ("dataset-race-b", "root-race-b"),
    ):
        await insert_subset_candidate(
            scenario,
            dataset_id=dataset_id,
            root_run_id=root_run_id,
        )
        await insert_valid_subset_resources(scenario, dataset_id)


def _baseline_parameters(root_run_id):
    evidence_pairs = valid_evidence_pairs()
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        root_run_id,
    )
    return terminal_parameters(
        proof_by_field,
        proof_sha256,
        metadata_by_field,
        "verification_baseline",
    )


async def has_waiting_lock(connection, backend_pid, competing_task):
    for _attempt in range(100):
        if competing_task.done():
            return False
        is_waiting = await connection.fetchval(
            """
            SELECT wait_event_type = 'Lock'
              FROM pg_catalog.pg_stat_activity
             WHERE pid = $1
            """,
            backend_pid,
        )
        if is_waiting:
            return True
        await asyncio.sleep(0.01)
    return False


async def _run_baseline_race(scenario, second_connection):
    first_connection = scenario.connection
    first_transaction = first_connection.transaction()
    second_transaction = second_connection.transaction()
    competing_task = None
    await first_transaction.start()
    await second_transaction.start()
    try:
        await first_connection.execute(
            terminal_sql(scenario, "dataset-race-a"),
            *_baseline_parameters("root-race-a"),
        )
        second_scenario = _ScenarioView(second_connection, scenario.schema)
        second_backend_pid = second_connection.get_server_pid()
        competing_task = asyncio.create_task(
            second_connection.execute(
                terminal_sql(second_scenario, "dataset-race-b"),
                *_baseline_parameters("root-race-b"),
            )
        )
        assert await has_waiting_lock(
            first_connection,
            second_backend_pid,
            competing_task,
        )
        await first_transaction.commit()
        try:
            await asyncio.wait_for(competing_task, timeout=5)
        except asyncpg.UniqueViolationError as error:
            assert (
                "pd_endpoint_dataset_subset_baseline_generation_key"
                in str(error)
            )
        else:
            raise AssertionError("concurrent subset baseline was accepted")
        await second_transaction.rollback()
    finally:
        if competing_task is not None and not competing_task.done():
            competing_task.cancel()
            with suppress(asyncio.CancelledError):
                await competing_task


async def prove_concurrent_baseline_generation_is_unique(monkeypatch):
    """Prove the partial unique index serializes two real transactions."""

    scenario = await create_committed_subset_schema(monkeypatch)
    first_connection = scenario.connection
    second_connection = await open_test_connection()
    try:
        await _insert_racing_candidates(scenario)
        try:
            await _run_baseline_race(scenario, second_connection)
        finally:
            for connection in (second_connection, first_connection):
                with suppress(Exception):
                    await connection.execute("ROLLBACK")
        baseline_count = await first_connection.fetchval(
            f"""
            SELECT count(*)
              FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE status = 'verification_baseline'
               AND completion_proof_required_version = 3
            """
        )
        assert baseline_count == 1
    finally:
        await second_connection.close()
        await first_connection.execute(
            f'DROP SCHEMA IF EXISTS "{scenario.schema}" CASCADE'
        )
        await first_connection.close()
