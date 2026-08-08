# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Direct-SQL evidence cases for the subset-completion migration."""

from __future__ import annotations

import json

from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
    insert_subset_candidate,
    insert_valid_subset_resources,
)
from tests.provider_directory_subset_completion_pg_support import (
    invalid_cutoff_evidence_pairs,
    malformed_proof_pair,
    malformed_replay_pair,
    terminal_metadata,
    terminal_parameters,
    terminal_sql,
    valid_evidence_pairs,
)
from tests.tin_npi_connector_postgres_support import expect_postgres_error


async def _prove_legacy_parent_mutability(scenario):
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id, endpoint_id, status, is_current, resource_count
        ) VALUES (
            'dataset-legacy-baseline', 'endpoint-a',
            'verification_baseline', false, 0
        )
        """
    )
    for status in ("verification_baseline", "verification_mismatch"):
        dataset_id = "dataset-legacy-" + status
        await scenario.connection.execute(
            f"""
            INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, status, is_current, resource_count
            ) VALUES ($1, 'endpoint-a', $2, false, 0)
            """,
            dataset_id,
            status,
        )
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = '{{"legacy":true}}'::json
             WHERE dataset_id = $1
            """,
            dataset_id,
        )
        await scenario.connection.execute(
            f"""
            DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = $1
            """,
            dataset_id,
        )


async def _prove_legacy_child_digest_compatibility(scenario):
    resource_insert_sql = f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        ) VALUES (
            'dataset-legacy-baseline', 'Organization', 'legacy-a', $1,
            '{{"id":"legacy-a"}}'::jsonb, $2
        )
    """
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_acquired_digest_marker_invalid",
        resource_insert_sql,
        "33" * 32,
        "44" * 32,
    )
    await scenario.connection.execute(resource_insert_sql, "33" * 32, None)
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-legacy-baseline'
        """
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1,
               publication_metadata_json = '{{"legacy":true}}'::json
         WHERE dataset_id = 'dataset-legacy-baseline'
        """,
        "55" * 32,
    )
    await scenario.connection.execute(
        f"""
        DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-legacy-baseline'
        """
    )


async def prove_legacy_compatibility(scenario):
    """Prove marker-null parent and child behavior remains predecessor-exact."""

    await _prove_legacy_parent_mutability(scenario)
    await _prove_legacy_child_digest_compatibility(scenario)
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id
        ) VALUES ('legacy-source-mutation', 'legacy-endpoint');
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET endpoint_id = 'legacy-endpoint-updated'
         WHERE source_id = 'legacy-source-mutation';
        DELETE FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'legacy-source-mutation';
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id
        ) VALUES ('legacy-source-truncate', 'legacy-endpoint');
        """
    )
    await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    await scenario.connection.execute(
        f"TRUNCATE TABLE {scenario.quoted_schema}.provider_directory_source"
    )
    await scenario.connection.execute("SET CONSTRAINTS ALL DEFERRED")


async def insert_marker_bound_subset_resources(scenario):
    await insert_subset_candidate(scenario)
    resource_insert_sql = f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash,
            payload_json, acquired_resource_sha256
        ) VALUES (
            'dataset-subset', 'Organization', 'null-digest-test', $1,
            '{{"id":"null-digest-test"}}'::jsonb, $2
        )
    """
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_acquired_digest_marker_invalid",
        resource_insert_sql,
        "11" * 32,
        None,
    )
    await insert_valid_subset_resources(scenario, "dataset-subset")


async def terminalize_subset_baseline(scenario):
    evidence_pairs = valid_evidence_pairs()
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        "root-subset",
    )
    baseline_sql = f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1, status = 'verification_baseline',
               publication_metadata_json = $2::jsonb,
               completion_proof_json = $3::jsonb,
               completion_proof_sha256 = $4
         WHERE dataset_id = 'dataset-subset'
    """
    parameters = (
        proof_by_field["dataset"]["hash"],
        json.dumps(metadata_by_field),
        json.dumps(proof_by_field),
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_completion_digest_invalid",
        baseline_sql,
        *parameters,
        "f" * 64,
    )
    await scenario.connection.execute(
        baseline_sql,
        *parameters,
        proof_sha256,
    )
    return evidence_pairs


async def prove_deep_malformed_evidence_rejected(scenario):
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-malformed-proof",
        root_run_id="root-malformed-proof",
    )
    proof_by_field, proof_sha256 = malformed_proof_pair()
    replay_by_field, replay_sha256 = malformed_replay_pair(proof_sha256)
    metadata_by_field = {
        "server_issued_subset_replay_evidence": replay_by_field,
        "server_issued_subset_replay_evidence_sha256": replay_sha256,
    }
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_completion_proof_invalid",
        terminal_sql(scenario, "dataset-malformed-proof"),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_baseline",
        ),
    )
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-malformed-replay",
        root_run_id="root-malformed-replay",
    )
    proof_by_field, proof_sha256, _, _ = valid_evidence_pairs()
    await insert_valid_subset_resources(
        scenario, "dataset-malformed-replay"
    )
    replay_by_field, replay_sha256 = malformed_replay_pair(proof_sha256)
    metadata_by_field = {
        "server_issued_subset_replay_evidence": replay_by_field,
        "server_issued_subset_replay_evidence_sha256": replay_sha256,
    }
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_replay_evidence_invalid",
        terminal_sql(scenario, "dataset-malformed-replay"),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_baseline",
        ),
    )


async def prove_invalid_cutoff_rejected(scenario):
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-invalid-cutoff",
        root_run_id="root-invalid-cutoff",
    )
    await insert_valid_subset_resources(scenario, "dataset-invalid-cutoff")
    evidence_pairs = invalid_cutoff_evidence_pairs()
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        "root-invalid-cutoff",
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_completion_proof_invalid",
        terminal_sql(scenario, "dataset-invalid-cutoff"),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "verification_baseline",
        ),
    )


async def prove_child_content_binding(scenario, evidence_pairs):
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    candidates = (
        ("dataset-zero-child", "root-zero-child"),
        ("dataset-payload-mismatch", "root-payload-mismatch"),
    )
    for dataset_id, root_run_id in candidates:
        await insert_subset_candidate(
            scenario,
            dataset_id=dataset_id,
            root_run_id=root_run_id,
        )
    await insert_valid_subset_resources(
        scenario, "dataset-payload-mismatch"
    )
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_dataset_resource
           SET payload_json = payload_json || '{{"tampered":true}}'::jsonb
         WHERE dataset_id = 'dataset-payload-mismatch'
           AND resource_type = 'Organization'
        """
    )
    for dataset_id, root_run_id in candidates:
        metadata_by_field = terminal_metadata(
            proof_by_field,
            proof_sha256,
            replay_by_field,
            replay_sha256,
            root_run_id,
        )
        await expect_postgres_error(
            scenario.connection,
            "provider_directory_subset_dataset_content_invalid",
            terminal_sql(scenario, dataset_id),
            *terminal_parameters(
                proof_by_field,
                proof_sha256,
                metadata_by_field,
                "verification_baseline",
            ),
        )


async def prove_terminal_parent_and_child_sealing(scenario):
    mutations_by_marker = {
        "tin_npi_connector_endpoint_dataset_transition_invalid": f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json =
                       (publication_metadata_json::jsonb
                        || '{{"tamper":true}}'::jsonb)::json
             WHERE dataset_id = 'dataset-subset'
        """,
        "tin_npi_connector_endpoint_dataset_delete_forbidden": f"""
            DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'dataset-subset'
        """,
        "tin_npi_connector_dataset_resource_parent_immutable": f"""
            DELETE FROM {scenario.quoted_schema}.provider_directory_dataset_resource
             WHERE dataset_id = 'dataset-subset'
        """,
    }
    for marker, mutation_sql in mutations_by_marker.items():
        await expect_postgres_error(
            scenario.connection,
            marker,
            mutation_sql,
        )


async def prove_downgrade_is_fail_closed(scenario, migration):
    capture = MigrationSqlCapture()
    migration.op = capture
    migration.downgrade()
    downgrade_fence = next(
        statement
        for statement in capture.statements
        if "provider_directory_subset_completion_downgrade_blocked" in statement
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_completion_downgrade_blocked",
        downgrade_fence,
    )
