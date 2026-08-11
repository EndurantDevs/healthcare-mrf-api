# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Direct-SQL lifecycle cases for reviewed subset activation."""

from __future__ import annotations

import json

from tests.provider_directory_reviewed_subset_activation_pg_support import (
    activate_source,
    activation_marker,
    activation_update_sql,
    is_activation_valid,
    expect_deferred_postgres_error,
    insert_third_matched_candidate,
    mutated_marker,
    publish_candidate_sql,
)
from tests.provider_directory_reviewed_subset_activation_pg_upsert import (
    prove_catalog_upserts_preserve_activation,
    prove_unprotected_generation_replacement,
)
from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error
from tests.tin_npi_connector_postgres_support import asyncpg


def _status_only_sql(scenario) -> str:
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                   metadata_json::jsonb,
                   '{{provider_directory_candidate_status}}',
                   pg_catalog.to_jsonb(
                       'verified_two_matching_reviewed_subset_acquisitions'
                       ::text
                   ),
                   true
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'synthetic-source'
    """


def _marker_only_sql(scenario) -> str:
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                   metadata_json::jsonb,
                   '{{provider_directory_reviewed_subset_activation_v1}}',
                   $1::jsonb,
                   true
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'synthetic-source'
    """


async def _prove_invalid_activation_transitions(
    scenario,
    migration,
    marker_by_field,
) -> None:
    await expect_deferred_postgres_error(
        scenario,
        migration,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        _status_only_sql(scenario),
    )
    marker_json = json.dumps(
        marker_by_field,
        sort_keys=True,
        separators=(",", ":"),
    )
    await expect_deferred_postgres_error(
        scenario,
        migration,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        _marker_only_sql(scenario),
        marker_json,
    )
    wrong_marker = mutated_marker(
        marker_by_field,
        "completion_proof_sha256",
        "f" * 64,
    )
    await expect_deferred_postgres_error(
        scenario,
        migration,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        activation_update_sql(scenario),
        json.dumps(wrong_marker, sort_keys=True, separators=(",", ":")),
    )


async def _prove_active_source_mutation_fences(scenario) -> None:
    extra_alias_sql = f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            'synthetic-extra-source', 'endpoint-a',
            'https://extra.example.test/fhir', false, false, 'none',
            '{{}}'::jsonb, pg_catalog.transaction_timestamp()
        )
    """
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_source_invalid",
        extra_alias_sql,
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_transition_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET canonical_api_base = 'https://drift.example.test/fhir'
         WHERE source_id = 'synthetic-source'
        """,
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_delete_forbidden",
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """,
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_truncate_forbidden",
        f"TRUNCATE TABLE {scenario.quoted_schema}.provider_directory_source",
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_dataset_truncate_forbidden",
        f"""TRUNCATE TABLE
            {scenario.quoted_schema}.provider_directory_endpoint_dataset
            CASCADE""",
    )


async def _prove_third_root_rejected(scenario, evidence_pairs) -> None:
    try:
        async with scenario.connection.transaction():
            terminal_statement, terminal_arguments = (
                await insert_third_matched_candidate(scenario, evidence_pairs)
            )
            await scenario.connection.execute(
                terminal_statement,
                *terminal_arguments,
            )
    except asyncpg.PostgresError as error:
        assert "provider_directory_reviewed_subset_activation_dataset_invalid" in str(
            error
        )
    else:
        raise AssertionError("extra proof-bearing root was accepted")


async def _prove_activation_survives_publication(
    scenario,
    migration,
    marker_by_field,
) -> None:
    await scenario.connection.execute(publish_candidate_sql(scenario))
    assert await is_activation_valid(scenario, migration) is True
    replay_status = await scenario.connection.execute(
        activation_update_sql(scenario),
        json.dumps(marker_by_field, sort_keys=True, separators=(",", ":")),
    )
    assert replay_status == "UPDATE 0"
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                   metadata_json::jsonb,
                   '{{unrelated_review_note}}',
                   '"allowed"'::jsonb,
                   true
               )
         WHERE source_id = 'synthetic-source'
        """
    )
    assert await is_activation_valid(scenario, migration) is True
    retained_marker = await scenario.connection.fetchval(
        f"""
        SELECT metadata_json::jsonb
                   -> 'provider_directory_reviewed_subset_activation_v1'
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    assert json.loads(retained_marker) == marker_by_field
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'superseded',
               is_current = false,
               superseded_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = 'dataset-matched'
        """
    )
    assert await is_activation_valid(scenario, migration) is True
    retained_replay_status = await scenario.connection.execute(
        activation_update_sql(scenario),
        json.dumps(marker_by_field, sort_keys=True, separators=(",", ":")),
    )
    assert retained_replay_status == "UPDATE 0"


async def _prove_activation_downgrade_blocked(scenario, migration) -> None:
    capture = MigrationSqlCapture()
    migration.op = capture
    migration.downgrade()
    try:
        async with scenario.connection.transaction():
            for statement in capture.statements:
                await scenario.connection.execute(statement)
    except asyncpg.PostgresError as error:
        assert "provider_directory_reviewed_subset_activation_downgrade_blocked" in str(
            error
        )
    else:
        raise AssertionError("active reviewed subset downgrade was accepted")


async def _prove_unrelated_source_dml_is_scoped(scenario, migration) -> None:
    validation_ref = (
        f"{scenario.quoted_schema}." f'"{migration._ACTIVATION_VALID_FUNCTION}"'
    )
    await scenario.connection.execute(
        f"""
        CREATE OR REPLACE FUNCTION {validation_ref}(candidate_source_id text)
        RETURNS boolean
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog
        AS $function$
        BEGIN
            RAISE EXCEPTION 'unrelated reviewed source was revalidated';
        END;
        $function$
        """
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES
            ('synthetic-unrelated-a', 'unrelated-a', NULL,
             false, false, 'none', '{{}}'::jsonb, now()),
            ('synthetic-unrelated-b', 'unrelated-b', NULL,
             false, false, 'none', '{{}}'::jsonb, now()),
            ('synthetic-unrelated-c', 'unrelated-c', NULL,
             false, false, 'none', '{{}}'::jsonb, now());
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = '{{"ordinary":true}}'::jsonb
         WHERE source_id LIKE 'synthetic-unrelated-%';
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id LIKE 'synthetic-unrelated-%';
        """
    )
    assert (
        await scenario.connection.fetchval(
            f"""
        SELECT pg_catalog.count(*) = 1
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
        )
        is True
    )


def _activation_scope_marker() -> dict[str, object]:
    """Return one minimal active marker for the isolated dataset guard."""

    return {
        "verification_campaign_id": "activation-campaign",
        "verification_source_scope_sha256": "a" * 64,
        "baseline": {"dataset_id": "dataset-baseline"},
        "candidate": {"dataset_id": "dataset-candidate"},
    }


async def _create_scoped_guard_relations(scenario, quoted_scope: str) -> None:
    """Seed one older incumbent and the activated candidate before guards."""

    await scenario.connection.execute(f"CREATE SCHEMA {quoted_scope}")
    await scenario.connection.execute(
        f"""
        CREATE TABLE {quoted_scope}.provider_directory_source (
            source_id text PRIMARY KEY,
            endpoint_id text NOT NULL,
            metadata_json jsonb NOT NULL
        )
        """
    )
    await scenario.connection.execute(
        f"""
        CREATE TABLE {quoted_scope}.provider_directory_endpoint_dataset (
            dataset_id text PRIMARY KEY,
            endpoint_id text NOT NULL,
            completion_proof_required_version integer,
            status text NOT NULL,
            publication_metadata_json jsonb NOT NULL
        )
        """
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {quoted_scope}.provider_directory_source
        VALUES (
            'synthetic-source', 'endpoint-a',
            pg_catalog.jsonb_build_object(
                'provider_directory_candidate_status',
                'verified_two_matching_reviewed_subset_acquisitions',
                'provider_directory_reviewed_subset_activation_v1',
                $1::jsonb
            )
        )
        """,
        json.dumps(
            _activation_scope_marker(),
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {quoted_scope}.provider_directory_endpoint_dataset
        VALUES
            ('dataset-incumbent', 'endpoint-a', 3, 'published',
             '{{"verification_campaign_id":"older-campaign",'
             '"verification_source_scope_hash":"{'b' * 64}"}}'),
            ('dataset-candidate', 'endpoint-a', 3, 'validated',
             '{{"verification_campaign_id":"activation-campaign",'
             '"verification_source_scope_hash":"{'a' * 64}"}}')
        """
    )


async def _exercise_scoped_dataset_guard(
    scenario,
    migration,
    scope_schema: str,
    quoted_scope: str,
) -> None:
    """Allow old-generation cutover and reject a same-generation third root."""

    await scenario.connection.execute(
        migration._dataset_guard_function_sql(scope_schema)
    )
    guard_ref = f'{quoted_scope}."{migration._DATASET_GUARD_FUNCTION}"'
    await scenario.connection.execute(
        f"""
        CREATE TRIGGER activation_dataset_guard
        BEFORE INSERT OR UPDATE
        ON {quoted_scope}.provider_directory_endpoint_dataset
        FOR EACH ROW EXECUTE FUNCTION {guard_ref}();
        ALTER TABLE {quoted_scope}.provider_directory_endpoint_dataset
        ENABLE ALWAYS TRIGGER activation_dataset_guard;
        UPDATE {quoted_scope}.provider_directory_endpoint_dataset
           SET status = 'superseded'
         WHERE dataset_id = 'dataset-incumbent';
        UPDATE {quoted_scope}.provider_directory_endpoint_dataset
           SET status = 'published'
         WHERE dataset_id = 'dataset-candidate';
        INSERT INTO {quoted_scope}.provider_directory_endpoint_dataset
        VALUES (
            'dataset-third', 'endpoint-a', 3, 'acquiring',
            '{{"verification_campaign_id":"activation-campaign",'
            '"verification_source_scope_hash":"{'a' * 64}"}}'
        );
        """
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_dataset_invalid",
        f"""
        UPDATE {quoted_scope}.provider_directory_endpoint_dataset
           SET status = 'validated'
         WHERE dataset_id = 'dataset-third'
        """,
    )


async def _prove_incumbent_supersession_is_scoped(scenario, migration) -> None:
    """Prove activation freezes only its exact proof generation."""

    scope_schema = f"{scenario.schema}_scope"
    quoted_scope = f'"{scope_schema}"'
    try:
        await _create_scoped_guard_relations(scenario, quoted_scope)
        await _exercise_scoped_dataset_guard(
            scenario,
            migration,
            scope_schema,
            quoted_scope,
        )
    finally:
        await scenario.connection.execute(
            f"DROP SCHEMA IF EXISTS {quoted_scope} CASCADE"
        )


async def prove_reviewed_subset_activation_lifecycle(
    scenario,
    migration,
    evidence_pairs,
) -> None:
    """Prove exact activation, mutation fences, and post-publish replay."""

    marker_by_field = activation_marker(evidence_pairs)
    await prove_unprotected_generation_replacement(scenario)
    await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    await scenario.connection.execute("SET CONSTRAINTS ALL DEFERRED")
    await _prove_invalid_activation_transitions(
        scenario,
        migration,
        marker_by_field,
    )
    await activate_source(scenario, migration, marker_by_field)
    await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    assert await is_activation_valid(scenario, migration) is True
    replay_status = await scenario.connection.execute(
        activation_update_sql(scenario),
        json.dumps(marker_by_field, sort_keys=True, separators=(",", ":")),
    )
    assert replay_status == "UPDATE 0"
    await prove_catalog_upserts_preserve_activation(
        scenario,
        marker_by_field,
    )
    await _prove_active_source_mutation_fences(scenario)
    await _prove_third_root_rejected(scenario, evidence_pairs)
    await _prove_activation_survives_publication(
        scenario,
        migration,
        marker_by_field,
    )
    await _prove_incumbent_supersession_is_scoped(scenario, migration)
    await _prove_activation_downgrade_blocked(scenario, migration)
    await _prove_unrelated_source_dml_is_scoped(scenario, migration)
