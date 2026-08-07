# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publish, source-fence, rollback, and policy-scope PostgreSQL proofs."""

from __future__ import annotations

from process import tin_npi_connector as connector
from tests.tin_npi_connector_postgres_support import expect_postgres_error


async def prove_publish_and_rollback(scenario):
    await _reject_direct_pointer_update(scenario)
    await _reject_disabled_source_guard(scenario)
    unexpected_policy = await _reject_unexpected_policy(scenario)
    await _reject_source_scope_change(scenario)
    await _reject_dataset_summary_change(scenario)
    await _publish_first_generation(scenario)
    later_generation_key = await _publish_later_generation(scenario)
    await _remove_source_dataset(scenario)
    await _prove_rollback_policy_fence(
        scenario,
        later_generation_key,
        unexpected_policy,
    )
    return later_generation_key


async def _reject_direct_pointer_update(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_pointer_action_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_current
           SET pointer_version = 1,
               generation_key = $1,
               published_at = transaction_timestamp(),
               updated_at = transaction_timestamp()
         WHERE pointer_key = 1
        """,
        scenario.generation_key,
    )


async def _reject_disabled_source_guard(scenario):
    await scenario.connection.execute(
        f"""
        ALTER TABLE {scenario.quoted_schema}.provider_directory_dataset_resource
        DISABLE TRIGGER tin_npi_connector_dataset_resource_insert_guard
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_dataset_resource_guard_changed",
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )
    await scenario.connection.execute(
        f"""
        ALTER TABLE {scenario.quoted_schema}.provider_directory_dataset_resource
        ENABLE ALWAYS TRIGGER tin_npi_connector_dataset_resource_insert_guard
        """
    )


async def _reject_unexpected_policy(scenario):
    unexpected_policy = connector.TinTokenPolicyDescriptor.release_1(
        "ptg-tin-hmac-sha256-v1:unexpected"
    )
    await _insert_ptg_policy(scenario, 2, unexpected_policy)
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_token_policy_scope_changed",
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )
    await _delete_ptg_policy(scenario, 2)
    return unexpected_policy


async def _reject_source_scope_change(scenario):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id,
            endpoint_id
        ) VALUES ('source-b', 'endpoint-a')
        """
    )
    await _set_dataset_source_ids(scenario, '["source-a","source-b"]')
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_fhir_source_scope_changed",
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'source-b'
        """
    )
    await _set_dataset_source_ids(scenario, '["source-a"]')


async def _set_dataset_source_ids(scenario, source_ids_json):
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json =
               jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{source_ids}}',
                   $1::jsonb
               )::json
         WHERE dataset_id = 'dataset-a'
        """,
        source_ids_json,
    )


async def _reject_dataset_summary_change(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_transition_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1
         WHERE dataset_id = 'dataset-a'
        """,
        "ef" * 32,
    )
    await _set_summary_hash(scenario, "ef" * 32)
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_fhir_dataset_changed",
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )
    await _set_summary_hash(scenario, "ab" * 32)


async def _set_summary_hash(scenario, dataset_hash):
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json =
               jsonb_set(
                   publication_metadata_json::jsonb,
                   '{{source_summary_v1,dataset_hash}}',
                   to_jsonb($1::text)
               )::json
         WHERE dataset_id = 'dataset-a'
        """,
        dataset_hash,
    )


async def _publish_first_generation(scenario):
    pointer_version = await scenario.connection.fetchval(
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )
    assert pointer_version == 1
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_pointer_cas_conflict",
        _publish_call(scenario),
        scenario.generation_key,
        bytes.fromhex(scenario.source_vector.source_vector_id),
    )


def _publish_call(scenario):
    return f"""
        SELECT {scenario.quoted_schema}.
               publish_tin_npi_connector_generation(0, NULL, $1, $2)
    """


async def _publish_later_generation(scenario):
    later_model = scenario.empty_model("2026-07-28T00:00:00.000000Z")
    later_build_token = "connector-build-proof-0002"
    later_generation_key = await scenario.insert_empty_build(
        later_model,
        later_build_token,
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.tin_npi_connector_generation
           SET state = 'complete'
         WHERE generation_key = $1
        """,
        later_generation_key,
    )
    pointer_version = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               publish_tin_npi_connector_generation(1, $1, $2, $3)
        """,
        scenario.generation_key,
        later_generation_key,
        bytes.fromhex(later_model.source_vector.source_vector_id),
    )
    assert pointer_version == 2
    return later_generation_key


async def _remove_source_dataset(scenario):
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'superseded',
               is_current = FALSE,
               superseded_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-a'
        """
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-a'
        """
    )


async def _prove_rollback_policy_fence(
    scenario,
    later_generation_key,
    unexpected_policy,
):
    await _insert_ptg_policy(scenario, 3, unexpected_policy)
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_token_policy_scope_changed",
        f"""
        SELECT {scenario.quoted_schema}.
               rollback_tin_npi_connector_generation(2, $1, $2)
        """,
        later_generation_key,
        scenario.generation_key,
    )
    await _delete_ptg_policy(scenario, 3)
    rollback_version = await scenario.connection.fetchval(
        f"""
        SELECT {scenario.quoted_schema}.
               rollback_tin_npi_connector_generation(2, $1, $2)
        """,
        later_generation_key,
        scenario.generation_key,
    )
    assert rollback_version == 3


async def _insert_ptg_policy(scenario, snapshot_key, token_policy):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.ptg2_provider_tax_identity_manifest (
            snapshot_key,
            token_policy_id,
            token_policy_descriptor_sha256
        ) VALUES ($1, $2, $3)
        """,
        snapshot_key,
        token_policy.token_policy_id,
        bytes.fromhex(token_policy.token_policy_descriptor_sha256),
    )


async def _delete_ptg_policy(scenario, snapshot_key):
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.ptg2_provider_tax_identity_manifest
         WHERE snapshot_key = $1
        """,
        snapshot_key,
    )
