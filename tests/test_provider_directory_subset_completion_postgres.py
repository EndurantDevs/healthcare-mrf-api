# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for subset evidence and direct-SQL guards."""

from __future__ import annotations

import json

import asyncpg
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
from tests.provider_directory_subset_payload_guard_pg import (
    prove_subset_payload_guard_repair,
)
from tests.provider_directory_subset_completion_pg_support import (
    valid_evidence_pairs,
)
from tests.provider_directory_subset_completion_pg_twin_cases import (
    prove_initial_source_fence,
    prove_matched_twin_transition_gate,
    prove_terminal_twin_semantics,
)
from tests.provider_directory_reviewed_subset_activation_pg_cases import (
    prove_reviewed_subset_activation_lifecycle,
)
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    prove_activation_alias_insert_is_serialized,
    prove_activation_busy_is_retryable,
    prove_neutral_evidence_renderer,
    prove_stale_rr_third_root_rejected,
)
from tests.provider_directory_reviewed_subset_activation_pg_publication import (
    prove_activation_artifact_publication_is_serialized,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    activation_marker,
    activation_update_sql,
    flush_deferred_fixture_events,
    insert_activation_generation,
    load_activation_migration,
)
from tests.provider_directory_fhir_subset_abandonment_pg_cases import (
    prove_reviewed_subset_abandonment_guard_handoff,
    prove_reviewed_subset_abandonment_lifecycle,
)
from tests.provider_directory_effective_endpoint_pg_cases import (
    prove_effective_endpoint_activation_and_publication,
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
async def test_subset_payload_guard_repair_supports_physical_json(monkeypatch):
    """Replace the deployed guard body before lifecycle bookkeeping."""

    await prove_subset_payload_guard_repair(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_abandonment_is_fail_closed_and_serialized(
    monkeypatch,
):
    """Seal exact expired evidence and reject every later mutation."""

    await prove_reviewed_subset_abandonment_lifecycle(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_abandonment_precedes_fresh_admission(monkeypatch):
    """Keep a queued fresh worker behind the committed terminal seal."""

    await prove_reviewed_subset_abandonment_guard_handoff(monkeypatch)


@pytest.mark.asyncio
async def test_subset_baseline_generation_is_concurrency_safe(monkeypatch):
    """Prove simultaneous baseline terminalization retains one generation."""

    await prove_concurrent_baseline_generation_is_unique(monkeypatch)


@pytest.mark.asyncio
async def test_subset_publication_source_mutations_are_serialized(monkeypatch):
    """Prove publication and source drift cannot race the sealed predicate."""

    await prove_publication_source_mutations_are_serialized(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_activation_is_exact_and_fail_closed(monkeypatch):
    """Prove explicit activation and all retained DB lifecycle fences."""

    scenario = await TransactionalSchema.create(monkeypatch)
    subset_migration = load_migration()
    activation_migration = load_activation_migration()
    try:
        await scenario.upgrade()
        await extend_source_fixture_table(scenario)
        await run_subset_migration(
            subset_migration,
            "upgrade",
            scenario.connection,
        )
        evidence_pairs = await insert_activation_generation(scenario)
        await flush_deferred_fixture_events(scenario)
        await run_subset_migration(
            activation_migration,
            "upgrade",
            scenario.connection,
        )
        await prove_reviewed_subset_activation_lifecycle(
            scenario,
            activation_migration,
            evidence_pairs,
        )
    finally:
        await scenario.close()


async def _activation_object_counts(scenario) -> tuple[int, int]:
    activation_count = await scenario.connection.fetchval(
        """
        SELECT pg_catalog.count(*)
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
         WHERE function_namespace.nspname = $1
           AND function_row.proname LIKE
               'provider_directory_reviewed_subset_activation%'
        """,
        scenario.schema,
    )
    predecessor_count = await scenario.connection.fetchval(
        """
        SELECT pg_catalog.count(*)
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS function_namespace
            ON function_namespace.oid = function_row.pronamespace
         WHERE function_namespace.nspname = $1
           AND (
                function_row.proname LIKE 'provider_directory_subset%'
                OR function_row.proname =
                   'guard_provider_directory_subset_published_source'
           )
        """,
        scenario.schema,
    )
    return activation_count, predecessor_count


async def _assert_pending_generation_retained(scenario) -> None:
    pending_state = await scenario.connection.fetchrow(
        f"""
        SELECT metadata_json::jsonb
                   ->> 'provider_directory_candidate_status' AS status,
               metadata_json::jsonb ?
                   'provider_directory_reviewed_subset_activation_v1'
                   AS has_marker,
               (
                   SELECT pg_catalog.count(*)
                     FROM {scenario.quoted_schema}.
                          provider_directory_endpoint_dataset
                    WHERE completion_proof_required_version = 3
               ) AS proof_dataset_count
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = 'synthetic-source'
        """
    )
    assert pending_state["status"] == (
        "pending_two_matching_reviewed_subset_acquisitions"
    )
    assert pending_state["has_marker"] is False
    assert pending_state["proof_dataset_count"] == 2


@pytest.mark.asyncio
async def test_reviewed_subset_activation_clean_downgrade_is_additive(
    monkeypatch,
):
    """Prove clean removal leaves every predecessor proof object intact."""

    scenario = await TransactionalSchema.create(monkeypatch)
    subset_migration = load_migration()
    activation_migration = load_activation_migration()
    try:
        await scenario.upgrade()
        await extend_source_fixture_table(scenario)
        await run_subset_migration(
            subset_migration,
            "upgrade",
            scenario.connection,
        )
        await insert_activation_generation(scenario)
        await flush_deferred_fixture_events(scenario)
        await run_subset_migration(
            activation_migration,
            "upgrade",
            scenario.connection,
        )
        await run_subset_migration(
            activation_migration,
            "downgrade",
            scenario.connection,
        )
        activation_count, predecessor_count = await _activation_object_counts(
            scenario
        )
        assert activation_count == 0
        assert predecessor_count == 9
        await _assert_pending_generation_retained(scenario)
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_reviewed_subset_activation_rejects_preactivated_adoption(
    monkeypatch,
):
    """Reject silent adoption of a status and marker created out of band."""

    scenario = await TransactionalSchema.create(monkeypatch)
    subset_migration = load_migration()
    activation_migration = load_activation_migration()
    try:
        await scenario.upgrade()
        await extend_source_fixture_table(scenario)
        await run_subset_migration(
            subset_migration,
            "upgrade",
            scenario.connection,
        )
        evidence_pairs = await insert_activation_generation(scenario)
        await flush_deferred_fixture_events(scenario)
        await scenario.connection.execute(
            activation_update_sql(scenario),
            json.dumps(
                activation_marker(evidence_pairs),
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
        await flush_deferred_fixture_events(scenario)
        try:
            async with scenario.connection.transaction():
                await run_subset_migration(
                    activation_migration,
                    "upgrade",
                    scenario.connection,
                )
        except asyncpg.PostgresError as error:
            assert (
                "provider_directory_reviewed_subset_activation_adoption_blocked"
                in str(error)
            )
        else:
            raise AssertionError("preactivated source was silently adopted")
        activation_object_count = await scenario.connection.fetchval(
            """
            SELECT pg_catalog.count(*)
              FROM pg_catalog.pg_proc AS function_row
              JOIN pg_catalog.pg_namespace AS function_namespace
                ON function_namespace.oid = function_row.pronamespace
             WHERE function_namespace.nspname = $1
               AND function_row.proname LIKE
                   'provider_directory_reviewed_subset_activation%'
            """,
            scenario.schema,
        )
        assert activation_object_count == 0
    finally:
        await scenario.close()


@pytest.mark.asyncio
async def test_reviewed_subset_activation_serializes_alias_inserts(monkeypatch):
    """Prove both source-table lock orderings reject alias phantoms."""

    await prove_activation_alias_insert_is_serialized(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_activation_busy_state_is_retryable(monkeypatch):
    """Prove the endpoint admission lock is non-mutating and retryable."""

    await prove_activation_busy_is_retryable(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_activation_renders_neutral_evidence(monkeypatch):
    """Prove the selector-free evidence command executes on real PostgreSQL."""

    await prove_neutral_evidence_renderer(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_activation_serializes_artifact_publication(
    monkeypatch,
):
    """Prove activation and artifact publication share one endpoint lock."""

    await prove_activation_artifact_publication_is_serialized(monkeypatch)


@pytest.mark.asyncio
async def test_effective_endpoint_identity_survives_atomic_publication(
    monkeypatch,
):
    """Keep configured proof identity stable across serving cutover."""

    await prove_effective_endpoint_activation_and_publication(monkeypatch)


@pytest.mark.asyncio
async def test_reviewed_subset_activation_rejects_stale_rr_third_root(
    monkeypatch,
):
    """Prove stale snapshot evidence cannot add a terminal root."""

    await prove_stale_rr_third_root_rejected(monkeypatch)
