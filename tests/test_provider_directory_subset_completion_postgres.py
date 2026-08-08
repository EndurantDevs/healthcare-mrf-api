# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for subset evidence and direct-SQL guards."""

from __future__ import annotations

import pytest

from tests.provider_directory_subset_completion_pg_concurrency import (
    prove_concurrent_baseline_generation_is_unique,
)
from tests.provider_directory_subset_completion_pg_source_concurrency import (
    prove_publication_source_mutations_are_serialized,
)
from tests.provider_directory_subset_completion_pg_evidence_cases import (
    insert_marker_bound_subset_resources,
    prove_child_content_binding,
    prove_deep_malformed_evidence_rejected,
    prove_downgrade_is_fail_closed,
    prove_invalid_cutoff_rejected,
    prove_legacy_compatibility,
    prove_terminal_parent_and_child_sealing,
    terminalize_subset_baseline,
)
from tests.provider_directory_subset_completion_pg_setup import (
    extend_source_fixture_table,
    load_migration,
    prove_payload_canonicalization_parity,
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_evidence_pairs,
)
from tests.provider_directory_subset_completion_pg_twin_cases import (
    prove_initial_source_fence,
    prove_matched_twin_transition_gate,
    prove_terminal_twin_semantics,
)
from tests.tin_npi_connector_postgres_support import TransactionalSchema


@pytest.mark.asyncio
async def test_subset_proof_guards_reject_direct_sql_tamper(monkeypatch):
    """Prove exact proof pairs, immutability, downgrade, and legacy fences."""

    scenario = await TransactionalSchema.create(monkeypatch)
    migration = load_migration()
    try:
        await scenario.upgrade()
        await extend_source_fixture_table(scenario)
        await run_subset_migration(migration, "upgrade", scenario.connection)
        await prove_payload_canonicalization_parity(scenario, migration)
        await prove_legacy_compatibility(scenario)
        await prove_deep_malformed_evidence_rejected(scenario)
        await prove_invalid_cutoff_rejected(scenario)
        await insert_marker_bound_subset_resources(scenario)
        await prove_initial_source_fence(scenario, valid_evidence_pairs())
        evidence_pairs = await terminalize_subset_baseline(scenario)
        await prove_terminal_twin_semantics(scenario, evidence_pairs)
        await prove_child_content_binding(scenario, evidence_pairs)
        await prove_matched_twin_transition_gate(scenario, evidence_pairs)
        await prove_terminal_parent_and_child_sealing(scenario)
        await prove_downgrade_is_fail_closed(scenario, migration)
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_subset_proof_migration_clean_downgrade_restores_legacy(
    monkeypatch,
):
    """Prove a data-clean downgrade removes v3 schema and restores mutability."""

    scenario = await TransactionalSchema.create(monkeypatch)
    migration = load_migration()
    try:
        await scenario.upgrade()
        await extend_source_fixture_table(scenario)
        await run_subset_migration(migration, "upgrade", scenario.connection)
        await run_subset_migration(migration, "downgrade", scenario.connection)
        await scenario.connection.execute(
            f"""
            INSERT INTO {scenario.quoted_schema}.provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, status, is_current, resource_count
            ) VALUES (
                'legacy-after-downgrade', 'endpoint-a',
                'verification_baseline', false, 0
            );
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET publication_metadata_json = '{{"legacy":true}}'::json
             WHERE dataset_id = 'legacy-after-downgrade';
            DELETE FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
             WHERE dataset_id = 'legacy-after-downgrade';
            """
        )
        remaining_columns = await scenario.connection.fetchval(
            """
            SELECT count(*)
              FROM information_schema.columns
             WHERE table_schema = $1
               AND column_name IN (
                    'completion_proof_required_version',
                    'completion_proof_json', 'completion_proof_sha256',
                    'acquired_resource_sha256'
               )
            """,
            scenario.schema,
        )
        assert remaining_columns == 0
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_subset_baseline_generation_is_concurrency_safe(monkeypatch):
    """Prove simultaneous baseline terminalization retains one generation."""

    await prove_concurrent_baseline_generation_is_unique(monkeypatch)


@pytest.mark.asyncio
async def test_subset_publication_source_mutations_are_serialized(monkeypatch):
    """Prove publication and source drift cannot race the sealed predicate."""

    await prove_publication_source_mutations_are_serialized(monkeypatch)
