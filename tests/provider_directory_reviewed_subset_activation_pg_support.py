# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL support for reviewed subset activation."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

import asyncpg

from process import provider_directory_fhir_subset_activation as activation
from process.provider_directory_fhir_subset_canonical import canonical_sha256
from tests.provider_directory_subset_completion_pg_evidence_cases import (
    insert_marker_bound_subset_resources,
    terminalize_subset_baseline,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
)
from tests.provider_directory_subset_completion_pg_support import (
    VALID_SOURCE_SCOPE_SHA256,
    coverage_from_proof,
    terminal_metadata,
    terminal_parameters,
    terminal_sql,
    valid_source_record,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808200000_provider_directory_reviewed_subset_activation.py"
)


def load_activation_migration():
    """Load the reviewed subset activation migration from its exact path."""

    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_subset_activation_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def activation_evidence(evidence_pairs):
    """Build the neutral desired-state evidence for the synthetic roots."""

    proof_by_field, proof_sha256, _, _ = evidence_pairs
    source_record = valid_source_record(activation.PENDING_STATUS)
    return activation.ReviewedSubsetActivationEvidence(
        source_contract_sha256=(
            activation.reviewed_subset_source_contract_sha256(source_record)
        ),
        cutoff=proof_by_field["cutoff"],
        verification_source_scope_sha256=VALID_SOURCE_SCOPE_SHA256,
        completion_proof_sha256=proof_sha256,
    )


def activation_marker(evidence_pairs):
    """Build the exact private marker accepted by the SQL validator."""

    proof_by_field, proof_sha256, _, replay_sha256 = evidence_pairs
    evidence = activation_evidence(evidence_pairs)
    selection = activation.ReviewedSubsetActivationSelection(
        source_id="synthetic-source",
        endpoint_id="endpoint-a",
        campaign_id=proof_by_field["campaign_id"],
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
        candidate_dataset_id="dataset-matched",
        candidate_root_run_id="root-matched",
        source_contract_sha256=evidence.source_contract_sha256,
        verification_source_scope_sha256=(
            evidence.verification_source_scope_sha256
        ),
        cutoff=evidence.cutoff,
        completion_proof_sha256=proof_sha256,
        baseline_replay_evidence_sha256=replay_sha256,
        candidate_replay_evidence_sha256=replay_sha256,
        baseline_coverage_sha256=canonical_sha256(
            coverage_from_proof(
                proof_by_field,
                proof_sha256,
                "baseline_recorded",
            )
        ),
        candidate_coverage_sha256=canonical_sha256(
            coverage_from_proof(proof_by_field, proof_sha256, "matched")
        ),
    )
    return selection.metadata_marker()


async def insert_activation_generation(scenario):
    """Insert one pending source and one exact sealed baseline/candidate pair."""

    from tests.provider_directory_subset_completion_pg_setup import (
        replace_subset_source,
    )

    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ('endpoint-a')
        ON CONFLICT (endpoint_id) DO NOTHING
        """
    )
    await replace_subset_source(scenario, activation.PENDING_STATUS)
    await insert_marker_bound_subset_resources(scenario)
    evidence_pairs = await terminalize_subset_baseline(scenario)
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-matched",
        root_run_id="root-matched",
    )
    await insert_valid_subset_resources(scenario, "dataset-matched")
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        "root-matched",
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
    )
    await scenario.connection.execute(
        terminal_sql(scenario, "dataset-matched"),
        *terminal_parameters(
            proof_by_field,
            proof_sha256,
            metadata_by_field,
            "validated",
        ),
    )
    return evidence_pairs


async def flush_deferred_fixture_events(scenario) -> None:
    """Commit-equivalent flush events created by the transactional fixture."""

    await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    await scenario.connection.execute("SET CONSTRAINTS ALL DEFERRED")


def activation_update_sql(scenario) -> str:
    """Return the sole pending-to-verified source transition."""

    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                   pg_catalog.jsonb_set(
                       metadata_json::jsonb,
                       '{{provider_directory_candidate_status}}',
                       pg_catalog.to_jsonb(
                           'verified_two_matching_reviewed_subset_acquisitions'
                           ::text
                       ),
                       true
                   ),
                   '{{provider_directory_reviewed_subset_activation_v1}}',
                   $1::jsonb,
                   true
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'synthetic-source'
           AND endpoint_id = 'endpoint-a'
           AND metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' =
                   'pending_two_matching_reviewed_subset_acquisitions'
           AND NOT (
                metadata_json::jsonb ?
                'provider_directory_reviewed_subset_activation_v1'
           )
    """


def activation_constraint_sql(scenario, migration) -> str:
    """Return a schema-qualified force for the deferred source guard."""

    return (
        f"SET CONSTRAINTS {scenario.quoted_schema}."
        f'"{migration._SOURCE_GUARD_TRIGGER}" IMMEDIATE'
    )


async def activate_source(scenario, migration, marker_by_field) -> None:
    """Apply and force the exact activation transition."""

    update_status = await scenario.connection.execute(
        activation_update_sql(scenario),
        json.dumps(marker_by_field, sort_keys=True, separators=(",", ":")),
    )
    assert update_status == "UPDATE 1"
    await scenario.connection.execute(
        activation_constraint_sql(scenario, migration)
    )


async def expect_deferred_postgres_error(
    scenario,
    migration,
    marker: str,
    statement: str,
    *parameters,
) -> None:
    """Force a deferred activation guard inside one rollback-only savepoint."""

    try:
        async with scenario.connection.transaction():
            await scenario.connection.execute(statement, *parameters)
            await scenario.connection.execute(
                activation_constraint_sql(scenario, migration)
            )
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"expected PostgreSQL error containing {marker!r}")


async def is_activation_valid(scenario, migration) -> bool:
    """Evaluate the hardened SQL activation predicate."""

    return bool(
        await scenario.connection.fetchval(
            f"SELECT {scenario.quoted_schema}."
            f'"{migration._ACTIVATION_VALID_FUNCTION}"($1)',
            "synthetic-source",
        )
    )


def publish_candidate_sql(scenario) -> str:
    """Return the normal validated-to-published candidate transition."""

    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = 'dataset-matched'
    """


async def insert_third_matched_candidate(
    scenario,
    evidence_pairs,
) -> tuple[str, tuple[object, ...]]:
    """Attempt one extra proof-bearing root after activation."""

    dataset_id = "dataset-third-root"
    await insert_subset_candidate(
        scenario,
        dataset_id=dataset_id,
        root_run_id="root-third",
    )
    await insert_valid_subset_resources(scenario, dataset_id)
    return third_matched_terminal(scenario, evidence_pairs)


def third_matched_terminal(
    scenario,
    evidence_pairs,
) -> tuple[str, tuple[object, ...]]:
    """Build the exact terminal transition for the synthetic third root."""

    dataset_id = "dataset-third-root"
    proof_by_field, proof_sha256, replay_by_field, replay_sha256 = evidence_pairs
    metadata_by_field = terminal_metadata(
        proof_by_field,
        proof_sha256,
        replay_by_field,
        replay_sha256,
        "root-third",
        baseline_dataset_id="dataset-subset",
        baseline_root_run_id="root-subset",
    )
    return terminal_sql(scenario, dataset_id), terminal_parameters(
        proof_by_field,
        proof_sha256,
        metadata_by_field,
        "validated",
    )


def mutated_marker(marker_by_field, *path_and_value):
    """Return a deep-copied marker with one test-only nested mutation."""

    *field_path, value = path_and_value
    mutated = deepcopy(marker_by_field)
    target = mutated
    for field_name in field_path[:-1]:
        target = target[field_name]
    target[field_path[-1]] = value
    return mutated
