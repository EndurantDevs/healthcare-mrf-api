# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for compact terminal Provider Directory publication."""

from __future__ import annotations

import importlib
import json
import time

import pytest

from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
    _source_endpoint,
    _split_source_endpoint_identity,
)
from tests.provider_directory_fhir_subset_activation_support import (
    single_root_activation_inputs,
)
from tests.provider_directory_reviewed_root_policy_pg import (
    _activate_policy_source,
    _insert_policy_source,
    _install_policy_predecessors,
    _terminalize_candidate,
)
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    _close_scenario,
    _runtime_database,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    flush_deferred_fixture_events,
)
from tests.provider_directory_subset_completion_pg_setup import (
    insert_subset_candidate,
    insert_valid_subset_resources,
    run_subset_migration,
)
from tests.test_provider_directory_subset_completion_migration import (
    _load_publication_guard_migration,
)
from tests.test_provider_directory_terminal_publication_compact_guard_migration import (
    _install_large_generic_seal,
    _load,
    _publish_sql,
    _rollback_publish,
    _run_migration,
)
from tests.tin_npi_connector_postgres_support import (
    TransactionalSchema,
    install_admission_seal_terminal_predecessors,
    load_admission_seal_migration,
)


asyncpg = pytest.importorskip("asyncpg")
importer = importlib.import_module("process.provider_directory_fhir")


async def _assert_seal_rejected(
    scenario,
    migration,
    *,
    summary,
    metadata_sha256,
    version,
    kind,
    proof_sha256,
    resource_types,
) -> None:
    table = f"{scenario.quoted_schema}.provider_directory_endpoint_dataset"
    trigger = migration._admission()._GUARD_TRIGGER
    transaction = scenario.connection.transaction()
    await transaction.start()
    try:
        await scenario.connection.execute(
            f'ALTER TABLE {table} DISABLE TRIGGER "{trigger}"'
        )
        await scenario.connection.execute(
            f"""
            UPDATE {table}
               SET publication_metadata_summary_json = $1::jsonb,
                   publication_metadata_sha256 = $2,
                   content_proof_admission_version = $3,
                   content_proof_admission_kind = $4,
                   content_proof_admission_sha256 = $5,
                   content_proof_resource_types = $6::varchar[]
             WHERE dataset_id = 'dataset-candidate'
            """,
            None if summary is None else json.dumps(summary),
            metadata_sha256,
            version,
            kind,
            proof_sha256,
            resource_types,
        )
        await scenario.connection.execute(
            f'ALTER TABLE {table} ENABLE ALWAYS TRIGGER "{trigger}"'
        )
        with pytest.raises(
            asyncpg.PostgresError,
            match="provider_directory_subset_published_source_invalid",
        ):
            await scenario.connection.execute(_publish_sql(scenario))
    finally:
        await transaction.rollback()


async def _assert_invalid_seals_fail_closed(scenario, migration, seal) -> None:
    policy_mismatch_by_key = dict(seal.metadata_summary)
    policy_mismatch_by_key["provider_directory_reviewed_root_policy_v1"] = {
        "policy_version": "provider-directory-reviewed-root-policy-v1",
        "required_root_count": 2,
    }
    digest_function = (
        f"{scenario.quoted_schema}."
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
    )

    async def digest(summary, kind):
        return await scenario.connection.fetchval(
            f"SELECT {digest_function}("
            "$1::jsonb, 1::smallint, $2::text, $3::text, $4::varchar[])",
            json.dumps(summary),
            kind,
            seal.proof_sha256,
            list(seal.resource_types),
        )

    policy_digest = await digest(policy_mismatch_by_key, "generic")
    uhc_digest = await digest(seal.metadata_summary, "uhc_canonical")
    rejected_seals = (
        (policy_mismatch_by_key, policy_digest, 1, "generic"),
        (seal.metadata_summary, "b" * 64, 1, "generic"),
        (seal.metadata_summary, uhc_digest, 1, "uhc_canonical"),
        (seal.metadata_summary, None, None, None),
    )
    for rejected_summary, rejected_digest, rejected_version, rejected_kind in (
        rejected_seals
    ):
        is_partial = rejected_version is None
        await _assert_seal_rejected(
            scenario,
            migration,
            summary=rejected_summary,
            metadata_sha256=rejected_digest,
            version=rejected_version,
            kind=rejected_kind,
            proof_sha256=None if is_partial else seal.proof_sha256,
            resource_types=None if is_partial else list(seal.resource_types),
        )


async def _assert_compact_source_drifts_rejected(scenario) -> None:
    source_table = f"{scenario.quoted_schema}.provider_directory_source"
    drift_statements = (
        f"""
        UPDATE {source_table}
           SET metadata_json = pg_catalog.jsonb_set(
                   metadata_json,
                   '{{provider_directory_candidate_status}}',
                   '"pending_two_matching_reviewed_subset_acquisitions"'::jsonb,
                   false
               )
         WHERE source_id = 'synthetic-source'
        """,
        f"UPDATE {source_table} SET canonical_api_base = NULL "
        "WHERE source_id = 'synthetic-source'",
        f"""
        UPDATE {source_table}
           SET metadata_json = pg_catalog.jsonb_set(
                   metadata_json,
                   '{{provider_directory_configured_endpoint_id}}',
                   '"drifted-endpoint"'::jsonb,
                   false
               )
         WHERE source_id = 'synthetic-source'
        """,
        f"""
        INSERT INTO {source_table} (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type, metadata_json
        ) VALUES (
            'synthetic-extra-alias', 'endpoint-a',
            'https://extra.example.test/fhir',
            false, false, 'none', '{{}}'::jsonb
        )
        """,
    )
    for drift_statement in drift_statements:
        transaction = scenario.connection.transaction()
        await transaction.start()
        try:
            await scenario.connection.execute(drift_statement)
            with pytest.raises(
                asyncpg.PostgresError,
                match="provider_directory_subset_published_source_invalid",
            ):
                await scenario.connection.execute(_publish_sql(scenario))
        finally:
            await transaction.rollback()


async def _assert_seal_cannot_change_during_publication(scenario) -> None:
    table = f"{scenario.quoted_schema}.provider_directory_endpoint_dataset"
    digest = (
        f"{scenario.quoted_schema}."
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
    )
    with pytest.raises(
        asyncpg.PostgresError,
        match="provider_directory_subset_published_source_invalid",
    ):
        async with scenario.connection.transaction():
            await scenario.connection.execute(
                f"""
                UPDATE {table}
                   SET status = 'published', is_current = true,
                       published_at = pg_catalog.transaction_timestamp(),
                       publication_metadata_summary_json = pg_catalog.jsonb_set(
                           publication_metadata_summary_json,
                           '{{outcome_resource_counts_v1,test_mutation}}',
                           'true'::jsonb,
                           true
                       ),
                       publication_metadata_sha256 = {digest}(
                           pg_catalog.jsonb_set(
                               publication_metadata_summary_json,
                               '{{outcome_resource_counts_v1,test_mutation}}',
                               'true'::jsonb,
                               true
                           ),
                           content_proof_admission_version,
                           content_proof_admission_kind::text,
                           content_proof_admission_sha256,
                           content_proof_resource_types
                       )
                 WHERE dataset_id = 'dataset-candidate'
                """
            )


async def _resolve_candidate_fence(monkeypatch, database):
    with monkeypatch.context() as patch:
        patch.setattr(importer, "db", database)
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["synthetic-source"],
            should_select_validated_candidates=True,
        )
    assert len(fence.datasets) == 1
    assert fence.datasets[0].dataset_id == "dataset-candidate"
    return fence


async def _run_atomic_promotion(
    monkeypatch,
    database,
    fence,
    *,
    candidate_budget: bool,
    commit: bool,
):
    started = time.monotonic()
    rollback_probe = RuntimeError("rollback publication probe")
    try:
        async with database.transaction():
            with monkeypatch.context() as patch:
                patch.setattr(importer, "db", database)
                await database.status("SET LOCAL lock_timeout = '500ms';")
                await database.status(
                    "SET LOCAL statement_timeout = '1000ms';"
                )
                if candidate_budget:
                    await importer._lock_artifact_cutover_fence(fence)
                else:
                    await importer._lock_and_verify_artifact_dataset_fence(
                        fence,
                        database,
                    )
                await importer._promote_provider_directory_artifact_datasets(
                    fence
                )
                elapsed = time.monotonic() - started
                if not commit:
                    raise rollback_probe
    except Exception as error:
        if error is rollback_probe:
            return elapsed, None
        return time.monotonic() - started, error
    return elapsed, None


async def _install_guard_stack(scenario, publication_migration) -> None:
    selection_migration = publication_migration._load_sibling(
        "20260812010000_provider_directory_artifact_selection_receipt.py",
        "_compact_guard_selection_receipt",
    )
    async with scenario.connection.transaction():
        await run_subset_migration(
            selection_migration,
            "upgrade",
            scenario.connection,
        )
        await install_admission_seal_terminal_predecessors(
            scenario.connection,
            scenario.quoted_schema,
        )
        await run_subset_migration(
            load_admission_seal_migration(),
            "upgrade",
            scenario.connection,
        )
        await run_subset_migration(
            publication_migration,
            "upgrade",
            scenario.connection,
        )


async def _prepare_candidate(scenario, publication_migration) -> None:
    _, activation_migration = await _install_policy_predecessors(scenario)
    source_record, dataset_rows, evidence = single_root_activation_inputs()
    dataset_row = dataset_rows[0]
    await _insert_policy_source(scenario, source_record)
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-candidate",
        root_run_id="root-candidate",
    )
    await insert_valid_subset_resources(scenario, "dataset-candidate")
    await _terminalize_candidate(scenario, dataset_row)
    await flush_deferred_fixture_events(scenario)
    await _activate_policy_source(
        scenario,
        activation_migration,
        source_record,
        dataset_row,
        evidence,
    )
    await flush_deferred_fixture_events(scenario)
    await _install_guard_stack(scenario, publication_migration)


async def _assert_compact_guards(scenario, migration, seal) -> None:
    table = f"{scenario.quoted_schema}.provider_directory_endpoint_dataset"
    with pytest.raises(
        asyncpg.PostgresError,
        match="pd_endpoint_dataset_subset_replay_evidence_check",
    ):
        async with scenario.connection.transaction():
            await scenario.connection.execute(
                f"""
                UPDATE {table}
                   SET publication_metadata_json = pg_catalog.jsonb_set(
                           publication_metadata_json::jsonb,
                           '{{server_issued_subset_replay_evidence_sha256}}',
                           '"invalid"'::jsonb,
                           false
                       )::json
                 WHERE dataset_id = 'dataset-candidate'
                """
            )
    await _assert_invalid_seals_fail_closed(scenario, migration, seal)
    await _assert_compact_source_drifts_rejected(scenario)
    await _assert_seal_cannot_change_during_publication(scenario)


async def _assert_atomic_cutover(
    monkeypatch,
    scenario,
    database,
    fence,
    migration,
) -> None:
    predecessor_elapsed, predecessor_error = await _run_atomic_promotion(
        monkeypatch,
        database,
        fence,
        candidate_budget=False,
        commit=False,
    )
    assert predecessor_error is not None
    assert "canceling statement due to statement timeout" in str(
        predecessor_error
    )
    assert predecessor_elapsed >= 0.9
    assert await _source_endpoint(scenario, "synthetic-source") == (
        "endpoint-serving"
    )

    await _run_migration(scenario, migration, "upgrade")
    compact_elapsed, compact_error = await _run_atomic_promotion(
        monkeypatch,
        database,
        fence,
        candidate_budget=True,
        commit=True,
    )
    assert compact_error is None
    assert compact_elapsed < 8.0
    assert await scenario.connection.fetchval(
        f"SELECT status = 'published' AND is_current "
        f"FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset "
        "WHERE dataset_id = 'dataset-candidate'"
    )
    assert await _source_endpoint(scenario, "synthetic-source") == "endpoint-a"


@pytest.mark.asyncio
async def test_compact_guard_is_bounded_and_fail_closed_on_postgres(
    monkeypatch,
) -> None:
    """Prove old failure and compact publication on identical large input."""

    publication_migration = _load_publication_guard_migration()
    migration = _load()
    scenario = await TransactionalSchema.create(monkeypatch)
    publication_database = None
    try:
        await _prepare_candidate(scenario, publication_migration)
        await _run_migration(scenario, migration, "upgrade")
        await _rollback_publish(scenario)
        await _run_migration(scenario, migration, "downgrade")

        seal, _selection_receipt = await _install_large_generic_seal(scenario)
        await _run_migration(scenario, migration, "upgrade")
        await _assert_compact_guards(scenario, migration, seal)

        await _run_migration(scenario, migration, "downgrade")
        await _split_source_endpoint_identity(
            scenario,
            _load_effective_endpoint_migration(),
        )
        await scenario.transaction.commit()
        publication_database = _runtime_database()
        fence = await _resolve_candidate_fence(
            monkeypatch,
            publication_database,
        )
        await _assert_atomic_cutover(
            monkeypatch,
            scenario,
            publication_database,
            fence,
            migration,
        )
    finally:
        await _close_scenario(scenario, publication_database)
