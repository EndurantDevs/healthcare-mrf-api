# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Two-connection publication and source-mutation serialization proof."""

from __future__ import annotations

import asyncio
from contextlib import suppress

from tests.provider_directory_subset_completion_pg_concurrency import (
    create_committed_subset_schema,
    has_waiting_lock,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    replace_subset_source,
)
from tests.provider_directory_subset_completion_pg_support import (
    terminal_metadata,
    terminal_parameters,
    terminal_sql,
    valid_evidence_pairs,
)
from tests.tin_npi_connector_postgres_support import (
    asyncpg,
    open_test_connection,
)


_PENDING_STATUS = "pending_two_matching_reviewed_subset_acquisitions"
_VERIFIED_STATUS = "verified_two_matching_reviewed_subset_acquisitions"


def _publish_sql(scenario):
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-source-race-candidate'
    """


def _source_mutation_sql(scenario, mutation_kind):
    if mutation_kind == "status-downgrade":
        return f"""
            UPDATE {scenario.quoted_schema}.provider_directory_source
               SET metadata_json = pg_catalog.jsonb_set(
                    metadata_json,
                    '{{provider_directory_candidate_status}}',
                    pg_catalog.to_jsonb('{_PENDING_STATUS}'::text),
                    false
               )
             WHERE source_id = 'synthetic-source'
        """
    assert mutation_kind == "extra-alias"
    return f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES (
            'synthetic-racing-alias', 'endpoint-a',
            'https://alias.example.test/fhir',
            false, false, 'none', '{{}}'::jsonb
        )
    """


async def _create_validated_scenario(monkeypatch):
    scenario = await create_committed_subset_schema(monkeypatch)
    await replace_subset_source(scenario, _VERIFIED_STATUS)
    evidence_pairs = valid_evidence_pairs()
    proof, proof_sha, replay, replay_sha = evidence_pairs
    await insert_subset_candidate(scenario)
    await insert_valid_subset_resources(scenario, "dataset-subset")
    baseline_metadata = terminal_metadata(
        proof, proof_sha, replay, replay_sha, "root-subset"
    )
    await scenario.connection.execute(
        terminal_sql(scenario, "dataset-subset"),
        *terminal_parameters(
            proof, proof_sha, baseline_metadata, "verification_baseline"
        ),
    )
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-source-race-candidate",
        root_run_id="root-source-race-candidate",
    )
    await insert_valid_subset_resources(
        scenario, "dataset-source-race-candidate"
    )
    candidate_metadata = terminal_metadata(
        proof,
        proof_sha,
        replay,
        replay_sha,
        "root-source-race-candidate",
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
    )
    await scenario.connection.execute(
        terminal_sql(scenario, "dataset-source-race-candidate"),
        *terminal_parameters(
            proof, proof_sha, candidate_metadata, "validated"
        ),
    )
    return scenario


async def _assert_postgres_marker(task, marker):
    try:
        await asyncio.wait_for(task, timeout=5)
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker}")


async def _publish_then_mutate(scenario, second_connection, mutation_kind):
    publish_transaction = scenario.connection.transaction()
    await publish_transaction.start()
    mutation_task = None
    try:
        await scenario.connection.execute(_publish_sql(scenario))
        mutation_task = asyncio.create_task(
            second_connection.execute(
                _source_mutation_sql(scenario, mutation_kind)
            )
        )
        assert await has_waiting_lock(
            scenario.connection,
            second_connection.get_server_pid(),
            mutation_task,
        )
        await publish_transaction.commit()
        await _assert_postgres_marker(
            mutation_task,
            "provider_directory_subset_published_source_mutation_invalid",
        )
    finally:
        if mutation_task is not None and not mutation_task.done():
            mutation_task.cancel()
            with suppress(asyncio.CancelledError):
                await mutation_task


async def _mutate_then_publish(scenario, second_connection, mutation_kind):
    mutation_transaction = second_connection.transaction()
    await mutation_transaction.start()
    publish_task = None
    try:
        await second_connection.execute(
            _source_mutation_sql(scenario, mutation_kind)
        )
        publish_task = asyncio.create_task(
            scenario.connection.execute(_publish_sql(scenario))
        )
        assert await has_waiting_lock(
            second_connection,
            scenario.connection.get_server_pid(),
            publish_task,
        )
        await mutation_transaction.commit()
        await _assert_postgres_marker(
            publish_task,
            "provider_directory_subset_published_source_invalid",
        )
    finally:
        if publish_task is not None and not publish_task.done():
            publish_task.cancel()
            with suppress(asyncio.CancelledError):
                await publish_task


async def _close_race_scenario(scenario, second_connection):
    for connection in (second_connection, scenario.connection):
        with suppress(Exception):
            await connection.execute("ROLLBACK")
    await second_connection.close()
    await scenario.connection.execute(
        f'DROP SCHEMA IF EXISTS "{scenario.schema}" CASCADE'
    )
    await scenario.connection.close()


async def _prove_data_modifying_cte_rejected(monkeypatch):
    scenario = await _create_validated_scenario(monkeypatch)
    transaction = scenario.connection.transaction()
    await transaction.start()
    try:
        await scenario.connection.execute(
            f"""
            WITH published_dataset AS (
                UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
                   SET status = 'published', is_current = true,
                       published_at = transaction_timestamp()
                 WHERE dataset_id = 'dataset-source-race-candidate'
                 RETURNING dataset_id
            )
            UPDATE {scenario.quoted_schema}.provider_directory_source
               SET metadata_json = pg_catalog.jsonb_set(
                    metadata_json,
                    '{{provider_directory_candidate_status}}',
                    pg_catalog.to_jsonb('{_PENDING_STATUS}'::text),
                    false
               )
             WHERE source_id = 'synthetic-source'
               AND EXISTS (SELECT 1 FROM published_dataset)
            """
        )
        await _assert_postgres_marker(
            asyncio.create_task(transaction.commit()),
            "provider_directory_subset_published_source_mutation_invalid",
        )
        assert await scenario.connection.fetchval(
            f"""
            SELECT status = 'validated'
              FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-source-race-candidate'
            """
        )
        assert await scenario.connection.fetchval(
            f"""
            SELECT metadata_json ->> 'provider_directory_candidate_status' =
                   '{_VERIFIED_STATUS}'
              FROM {scenario.quoted_schema}.provider_directory_source
             WHERE source_id = 'synthetic-source'
            """
        )
    finally:
        with suppress(Exception):
            await scenario.connection.execute("ROLLBACK")
        await scenario.connection.execute(
            f'DROP SCHEMA IF EXISTS "{scenario.schema}" CASCADE'
        )
        await scenario.connection.close()


async def _assert_no_published_source_drift(scenario, connection):
    published_count = await connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-source-race-candidate'
           AND status = 'published'
           AND is_current IS TRUE
        """
    )
    if published_count == 0:
        return
    verified_source_count = await connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
           AND metadata_json ->> 'provider_directory_candidate_status' =
               '{_VERIFIED_STATUS}'
        """
    )
    endpoint_alias_count = await connection.fetchval(
        f"""
        SELECT count(*)
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE endpoint_id = 'endpoint-a'
           AND source_id <> 'synthetic-source'
        """
    )
    assert verified_source_count == 1
    assert endpoint_alias_count == 0


async def _repeatable_publisher_rejects(monkeypatch, mutation_kind):
    scenario = await _create_validated_scenario(monkeypatch)
    second_connection = await open_test_connection()
    publisher_transaction = scenario.connection.transaction(
        isolation="repeatable_read"
    )
    await publisher_transaction.start()
    try:
        await scenario.connection.fetchval(
            f"SELECT count(*) FROM "
            f"{scenario.quoted_schema}.provider_directory_source"
        )
        await second_connection.execute(
            _source_mutation_sql(scenario, mutation_kind)
        )
        await _assert_postgres_marker(
            asyncio.create_task(
                scenario.connection.execute(_publish_sql(scenario))
            ),
            "provider_directory_subset_source_isolation_invalid",
        )
        await publisher_transaction.rollback()
        await _assert_no_published_source_drift(scenario, second_connection)
    finally:
        await _close_race_scenario(scenario, second_connection)


async def _repeatable_source_mutation_rejects(monkeypatch, mutation_kind):
    scenario = await _create_validated_scenario(monkeypatch)
    second_connection = await open_test_connection()
    source_transaction = second_connection.transaction(
        isolation="repeatable_read"
    )
    publish_transaction = scenario.connection.transaction()
    await source_transaction.start()
    await publish_transaction.start()
    mutation_task = None
    try:
        await second_connection.fetchval(
            f"SELECT count(*) FROM "
            f"{scenario.quoted_schema}.provider_directory_source"
        )
        await scenario.connection.execute(_publish_sql(scenario))
        mutation_task = asyncio.create_task(
            second_connection.execute(
                _source_mutation_sql(scenario, mutation_kind)
            )
        )
        assert await has_waiting_lock(
            scenario.connection,
            second_connection.get_server_pid(),
            mutation_task,
        )
        await publish_transaction.commit()
        await asyncio.wait_for(mutation_task, timeout=5)
        await _assert_postgres_marker(
            asyncio.create_task(source_transaction.commit()),
            "provider_directory_subset_source_isolation_invalid",
        )
        await _assert_no_published_source_drift(
            scenario, scenario.connection
        )
    finally:
        if mutation_task is not None and not mutation_task.done():
            mutation_task.cancel()
            with suppress(asyncio.CancelledError):
                await mutation_task
        await _close_race_scenario(scenario, second_connection)


async def prove_publication_source_mutations_are_serialized(monkeypatch):
    """Prove both lock orderings for source drift and alias insertion."""

    for mutation_kind in ("status-downgrade", "extra-alias"):
        for race_direction in ("publish-first", "mutation-first"):
            scenario = await _create_validated_scenario(monkeypatch)
            second_connection = await open_test_connection()
            try:
                if race_direction == "publish-first":
                    await _publish_then_mutate(
                        scenario, second_connection, mutation_kind
                    )
                else:
                    await _mutate_then_publish(
                        scenario, second_connection, mutation_kind
                    )
            finally:
                await _close_race_scenario(scenario, second_connection)
    await _prove_data_modifying_cte_rejected(monkeypatch)
    for mutation_kind in ("status-downgrade", "extra-alias"):
        await _repeatable_publisher_rejects(monkeypatch, mutation_kind)
        await _repeatable_source_mutation_rejects(
            monkeypatch, mutation_kind
        )
