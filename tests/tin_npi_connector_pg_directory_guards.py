# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-directory lifecycle guard proofs used by the connector scenario."""

from __future__ import annotations

from tests.tin_npi_connector_pg_endpoint_dataset_guard_budget import (
    prove_large_metadata_guard_budget,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


async def prove_directory_guard_contract(scenario):
    await _assert_guard_catalog(scenario)
    await _reject_invalid_dataset_transitions(scenario)
    await _reject_published_resource_mutations(scenario)
    await prove_large_metadata_guard_budget(scenario)
    await _prove_mutable_dataset_lifecycle(scenario)
    await _prove_verification_baseline_cleanup(scenario)


async def _assert_guard_catalog(scenario):
    trigger_count = await scenario.connection.fetchval(
        f"""
        SELECT COUNT(*)
          FROM pg_catalog.pg_trigger AS trigger_row
         WHERE trigger_row.tgrelid =
                   '{scenario.quoted_schema}.provider_directory_endpoint_dataset'
                       ::regclass
           AND trigger_row.tgname = 'tin_npi_connector_endpoint_dataset_guard'
           AND trigger_row.tgtype = 31
           AND trigger_row.tgenabled = 'A'
           AND trigger_row.tgisinternal IS FALSE
           AND trigger_row.tgfoid =
                   '{scenario.quoted_schema}.
                    guard_tin_npi_connector_endpoint_dataset()'::regprocedure
        """
    )
    assert trigger_count == 1
    function_acl = await scenario.connection.fetchrow(
        """
        SELECT COUNT(*) AS function_count,
               COUNT(*) FILTER (
                   WHERE function_row.prosecdef IS TRUE
                     AND function_row.proconfig =
                           ARRAY['search_path=pg_catalog']::text[]
               ) AS hardened_function_count,
               COUNT(*) FILTER (
                   WHERE function_acl.grantee = 0
                     AND function_acl.privilege_type = 'EXECUTE'
               ) AS public_execute_count
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(
                COALESCE(
                    function_row.proacl,
                    pg_catalog.acldefault('f', function_row.proowner)
                )
               ) AS function_acl
         WHERE function_namespace.nspname = $1
           AND function_row.pronargs = 0
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.session.schema,
        [
            "guard_tin_npi_connector_dataset_resource",
            "guard_tin_npi_connector_endpoint_dataset",
        ],
    )
    assert function_acl["function_count"] == 2
    assert function_acl["hardened_function_count"] == 2
    assert function_acl["public_execute_count"] == 0


async def _reject_invalid_dataset_transitions(scenario):
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_insert_invalid",
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status, is_current, resource_count,
            validated_at, published_at
        ) VALUES (
            'dataset-invalid-insert', 'endpoint-a', 'published', true, 0,
            transaction_timestamp(), transaction_timestamp()
        )
        """,
    )
    await _insert_acquiring_dataset(scenario, "dataset-invalid-transition")
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_transition_invalid",
        _publish_statement(scenario, "dataset-invalid-transition"),
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-invalid-transition'
        """
    )
    await _insert_acquiring_dataset(scenario, "dataset-missing-validation-time")
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'validated'
         WHERE dataset_id = 'dataset-missing-validation-time'
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_transition_invalid",
        _publish_statement(scenario, "dataset-missing-validation-time"),
    )


async def _insert_acquiring_dataset(scenario, dataset_id):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status, is_current, resource_count
        ) VALUES ($1, 'endpoint-a', 'acquiring', false, 0)
        """,
        dataset_id,
    )


def _publish_statement(scenario, dataset_id):
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published',
               is_current = true,
               published_at = transaction_timestamp()
         WHERE dataset_id = '{dataset_id}'
    """


async def _reject_published_resource_mutations(scenario):
    insert_statement = f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'dataset-a', 'Organization', 'organization-3',
            $1, '{{"id":"organization-3"}}'::jsonb
        )
    """
    update_statement = f"""
        UPDATE {scenario.quoted_schema}.provider_directory_dataset_resource
           SET payload_hash = $1
         WHERE dataset_id = 'dataset-a'
           AND resource_type = 'Organization'
           AND resource_id = 'organization-a'
    """
    delete_statement = f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-a'
           AND resource_type = 'Organization'
           AND resource_id = 'organization-a'
    """
    for statement, parameters in (
        (insert_statement, ("33" * 32,)),
        (update_statement, ("44" * 32,)),
        (delete_statement, ()),
    ):
        await expect_postgres_error(
            scenario.connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            statement,
            *parameters,
        )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_dataset_resource_truncate_forbidden",
        f"TRUNCATE {scenario.quoted_schema}.provider_directory_dataset_resource",
    )
    await _reject_replica_role_bypass(scenario, update_statement)


async def _reject_replica_role_bypass(scenario, update_statement):
    await scenario.connection.execute("SET LOCAL session_replication_role = replica")
    try:
        await expect_postgres_error(
            scenario.connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            update_statement,
            "55" * 32,
        )
    finally:
        await scenario.connection.execute("SET LOCAL session_replication_role = origin")


async def _prove_mutable_dataset_lifecycle(scenario):
    await _insert_cleanup_dataset(scenario)
    await _mutate_cleanup_resource(scenario)
    await _validate_cleanup_dataset(scenario)
    await _reject_immutable_endpoint_dataset_mutations(scenario)
    await _publish_and_supersede_cleanup_dataset(scenario)
    await _prove_superseded_cleanup(scenario)


async def _insert_cleanup_dataset(scenario):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, import_run_id, acquisition_root_run_id,
            previous_dataset_id, dataset_hash, status, is_current,
            resource_count, publication_metadata_json
        ) VALUES (
            'dataset-cleanup', 'endpoint-a', 'run-cleanup', 'run-cleanup',
            'dataset-a', $1, 'acquiring', false, 1, '{{}}'::json
        )
        """,
        "cd" * 32,
    )
    await _insert_cleanup_resource(scenario, "66" * 32)


async def _insert_cleanup_resource(scenario, payload_hash):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'dataset-cleanup', 'Organization', 'organization-cleanup',
            $1, '{{"id":"organization-cleanup"}}'::jsonb
        )
        """,
        payload_hash,
    )


async def _mutate_cleanup_resource(scenario):
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_dataset_resource
           SET payload_hash = $1
         WHERE dataset_id = 'dataset-cleanup'
        """,
        "77" * 32,
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-cleanup'
        """
    )
    await _insert_cleanup_resource(scenario, "88" * 32)


async def _validate_cleanup_dataset(scenario):
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'validated',
               validated_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-cleanup'
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_dataset_resource_parent_immutable",
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-cleanup'
        """,
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_transition_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published',
               is_current = TRUE,
               published_at = transaction_timestamp() - interval '1 second'
         WHERE dataset_id = 'dataset-cleanup'
        """,
    )


async def _reject_immutable_endpoint_dataset_mutations(scenario):
    immutable_assignments = (
        "dataset_id = 'dataset-cleanup-changed'",
        "endpoint_id = 'endpoint-changed'",
        "import_run_id = 'run-changed'",
        "acquisition_root_run_id = 'root-changed'",
        "previous_dataset_id = 'dataset-changed'",
        "dataset_hash = 'efef'",
        "resource_count = resource_count + 1",
        "created_at = timestamp '2026-08-07 00:00:00'",
        "validated_at = validated_at + interval '1 second'",
    )
    for assignment in immutable_assignments:
        await expect_postgres_error(
            scenario.connection,
            "tin_npi_connector_endpoint_dataset_transition_invalid",
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET {assignment}
             WHERE dataset_id = 'dataset-cleanup'
            """,
        )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET publication_metadata_json =
               (
                   COALESCE(publication_metadata_json, '{{}}'::json)::jsonb
                   || '{{"guard_test":true}}'::jsonb
               )::json
         WHERE dataset_id = 'dataset-cleanup'
        """
    )
    guard_test_value = await scenario.connection.fetchval(
        f"""
        SELECT publication_metadata_json::jsonb ->> 'guard_test'
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-cleanup'
        """
    )
    assert guard_test_value == "true"


async def _publish_and_supersede_cleanup_dataset(scenario):
    await scenario.connection.execute(_publish_statement(scenario, "dataset-cleanup"))
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'superseded',
               is_current = FALSE,
               superseded_at = transaction_timestamp()
         WHERE dataset_id = 'dataset-cleanup'
        """
    )


async def _prove_superseded_cleanup(scenario):
    update_statement = f"""
        UPDATE {scenario.quoted_schema}.provider_directory_dataset_resource
           SET payload_hash = $1
         WHERE dataset_id = 'dataset-cleanup'
    """
    insert_statement = f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'dataset-cleanup', 'Organization', 'organization-cleanup-2',
            $1, '{{"id":"organization-cleanup-2"}}'::jsonb
        )
    """
    for statement, payload_hash in (
        (update_statement, "99" * 32),
        (insert_statement, "aa" * 32),
    ):
        await expect_postgres_error(
            scenario.connection,
            "tin_npi_connector_dataset_resource_parent_immutable",
            statement,
            payload_hash,
        )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-cleanup'
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "tin_npi_connector_endpoint_dataset_delete_forbidden",
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-cleanup'
        """,
    )


async def _prove_verification_baseline_cleanup(scenario):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status, is_current, resource_count
        ) VALUES (
            'dataset-verification-baseline', 'endpoint-a',
            'verification_baseline', false, 1
        )
        """
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'dataset-verification-baseline', 'Organization',
            'organization-baseline', $1,
            '{{"id":"organization-baseline"}}'::jsonb
        )
        """,
        "bb" * 32,
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-verification-baseline'
        """
    )
