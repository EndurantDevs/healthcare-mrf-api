# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Large-metadata budget proof for the endpoint-dataset lifecycle guard."""

from __future__ import annotations

import asyncio


async def prove_large_metadata_guard_budget(scenario):
    """Prove large metadata trigger transitions within their DB budgets."""
    await _insert_large_metadata_dataset(scenario)
    await _publish_and_supersede_large_metadata_dataset(scenario)
    await _assert_large_metadata_dataset_state(scenario)


async def _insert_large_metadata_dataset(scenario):
    """Create and validate a production-shaped large-metadata dataset."""
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ('endpoint-large-metadata')
        """
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, created_at, publication_metadata_json
        ) VALUES (
            'dataset-large-metadata', 'endpoint-large-metadata',
            'run-large-metadata', 'run-large-metadata', NULL, 'abab',
            'acquiring', false, 1, transaction_timestamp(),
            json_build_object(
                'provider_directory_content_proof_v1',
                repeat('x', 64 * 1024 * 1024)
            )
        )
        """
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'validated',
               validated_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-large-metadata'
        """
    )


async def _publish_and_supersede_large_metadata_dataset(scenario):
    """Run both guarded transitions under the unchanged transaction budgets."""
    previous_statement_timeout = await scenario.connection.fetchval(
        "SHOW statement_timeout"
    )
    previous_lock_timeout = await scenario.connection.fetchval("SHOW lock_timeout")
    try:
        async with asyncio.timeout(2.0):
            async with scenario.connection.transaction():
                await scenario.connection.execute(
                    "SELECT pg_catalog.set_config('lock_timeout', '500ms', true)"
                )
                await scenario.connection.execute(
                    "SELECT pg_catalog.set_config('statement_timeout', "
                    "'1000ms', true)"
                )
                await scenario.connection.execute(
                    f"""
                    UPDATE {scenario.quoted_schema}.
                           provider_directory_endpoint_dataset
                       SET status = 'published',
                           is_current = true,
                           published_at = transaction_timestamp()
                     WHERE dataset_id = 'dataset-large-metadata'
                    """
                )
                await scenario.connection.execute(
                    f"""
                    UPDATE {scenario.quoted_schema}.
                           provider_directory_endpoint_dataset
                       SET status = 'superseded',
                           is_current = false,
                           superseded_at = transaction_timestamp()
                     WHERE dataset_id = 'dataset-large-metadata'
                    """
                )
    finally:
        await scenario.connection.execute(
            "SELECT pg_catalog.set_config('statement_timeout', $1, true)",
            previous_statement_timeout,
        )
        await scenario.connection.execute(
            "SELECT pg_catalog.set_config('lock_timeout', $1, true)",
            previous_lock_timeout,
        )


async def _assert_large_metadata_dataset_state(scenario):
    """Confirm both transitions committed without truncating metadata."""
    stored_state = await scenario.connection.fetchrow(
        f"""
        SELECT status, is_current, published_at IS NOT NULL AS published,
               superseded_at IS NOT NULL AS superseded,
               octet_length(publication_metadata_json::text) AS metadata_bytes
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-large-metadata'
        """
    )
    assert stored_state["status"] == "superseded"
    assert stored_state["is_current"] is False
    assert stored_state["published"] is True
    assert stored_state["superseded"] is True
    assert stored_state["metadata_bytes"] > 64 * 1024 * 1024
