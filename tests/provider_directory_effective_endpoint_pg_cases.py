# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real-PostgreSQL lifecycle proof for configured endpoint identity."""

from __future__ import annotations

from contextlib import suppress
import importlib
import importlib.util
from pathlib import Path

from process import provider_directory_fhir_subset_activation as activation
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    create_abandonment_relations,
)
from tests.provider_directory_reviewed_subset_activation_pg_concurrency import (
    _close_scenario,
    _create_activation_scenario,
    _runtime_database,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    activation_evidence,
    is_activation_valid,
)
from tests.provider_directory_subset_completion_pg_setup import (
    load_abandonment_migration,
    load_payload_guard_repair_migration,
    run_subset_migration,
)
from tests.tin_npi_connector_postgres_support import asyncpg


importer = importlib.import_module("process.provider_directory_fhir")

_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260809010000_provider_directory_effective_endpoint_identity.py"
)
_ROOT_POLICY_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260809030000_provider_directory_reviewed_root_policy.py"
)
_SERVING_ENDPOINT_ID = "endpoint-serving"


def _load_effective_endpoint_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_effective_endpoint_postgres_migration",
        _MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _load_root_policy_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_root_policy_postgres_migration",
        _ROOT_POLICY_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _target_function_names(migration) -> tuple[str, ...]:
    subset = migration._subset()
    activation_migration = migration._activation()
    return (
        subset._ENDPOINT_DATASET_GUARD,
        subset._SOURCE_GUARD,
        activation_migration._ACTIVATION_VALID_FUNCTION,
        activation_migration._SOURCE_GUARD_FUNCTION,
    )


async def _function_and_trigger_identity(scenario, migration):
    function_names = _target_function_names(migration)
    function_rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid,
               function_row.prosecdef,
               function_row.proconfig,
               pg_catalog.has_function_privilege(
                   'public', function_row.oid, 'EXECUTE'
               ) AS public_execute
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
         ORDER BY function_row.proname
        """,
        scenario.schema,
        function_names,
    )
    function_by_name = {
        function_record["proname"]: (
            function_record["oid"],
            function_record["prosecdef"],
            tuple(function_record["proconfig"] or ()),
            function_record["public_execute"],
        )
        for function_record in function_rows
    }
    trigger_rows = await scenario.connection.fetch(
        """
        SELECT trigger_row.tgname, trigger_row.oid, trigger_row.tgfoid
          FROM pg_catalog.pg_trigger AS trigger_row
          JOIN pg_catalog.pg_class AS relation_row
            ON relation_row.oid = trigger_row.tgrelid
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = relation_row.relnamespace
         WHERE namespace_row.nspname = $1
           AND NOT trigger_row.tgisinternal
           AND trigger_row.tgfoid = ANY($2::oid[])
         ORDER BY trigger_row.tgname
        """,
        scenario.schema,
        [identity[0] for identity in function_by_name.values()],
    )
    trigger_by_name = {
        trigger_record["tgname"]: (
            trigger_record["oid"],
            trigger_record["tgfoid"],
        )
        for trigger_record in trigger_rows
    }
    return function_by_name, trigger_by_name


async def _install_effective_endpoint_migration(scenario, effective_migration):
    await create_abandonment_relations(scenario)
    async with scenario.connection.transaction():
        await run_subset_migration(
            load_payload_guard_repair_migration(),
            "upgrade",
            scenario.connection,
        )
        await run_subset_migration(
            load_abandonment_migration(),
            "upgrade",
            scenario.connection,
        )
        before_identity = await _function_and_trigger_identity(
            scenario,
            effective_migration,
        )
        await run_subset_migration(
            effective_migration,
            "upgrade",
            scenario.connection,
        )
        after_identity = await _function_and_trigger_identity(
            scenario,
            effective_migration,
        )
    assert before_identity == after_identity
    assert len(after_identity[0]) == 4
    assert after_identity[1]
    for _, is_security_definer, config, public_execute in after_identity[0].values():
        assert is_security_definer is True
        assert config == ("search_path=pg_catalog",)
        assert public_execute is False
    return effective_migration


async def _split_source_endpoint_identity(scenario, effective_migration) -> None:
    subset_migration = effective_migration._subset()
    activation_migration = effective_migration._activation()
    source_guard_triggers = (
        subset_migration._SOURCE_GUARD_TRIGGER,
        activation_migration._SOURCE_GUARD_TRIGGER,
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ($1)
        """,
        _SERVING_ENDPOINT_ID,
    )
    async with scenario.connection.transaction():
        for trigger_name in source_guard_triggers:
            await scenario.connection.execute(
                f"ALTER TABLE {scenario.quoted_schema}."
                "provider_directory_source DISABLE TRIGGER "
                f'"{trigger_name}"'
            )
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_source
               SET endpoint_id = $1,
                   updated_at = pg_catalog.transaction_timestamp()
             WHERE source_id = 'synthetic-source'
               AND endpoint_id = 'endpoint-a'
               AND metadata_json::jsonb
                       ->> 'provider_directory_configured_endpoint_id' = 'endpoint-a'
            """,
            _SERVING_ENDPOINT_ID,
        )
        for trigger_name in source_guard_triggers:
            await scenario.connection.execute(
                f"ALTER TABLE {scenario.quoted_schema}."
                "provider_directory_source ENABLE ALWAYS TRIGGER "
                f'"{trigger_name}"'
            )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            'synthetic-serving-sibling', $1,
            'https://sibling.example.test/fhir', false, false, 'none',
            '{{"provider_directory_configured_endpoint_id":"endpoint-serving"}}'
                ::jsonb,
            pg_catalog.transaction_timestamp()
        )
        """,
        _SERVING_ENDPOINT_ID,
    )


async def _source_endpoint(scenario, source_id: str) -> str:
    return await scenario.connection.fetchval(
        f"""
        SELECT endpoint_id
          FROM {scenario.quoted_schema}.provider_directory_source
         WHERE source_id = $1
        """,
        source_id,
    )


async def _expect_direct_transition_rejected(
    scenario,
    statement: str,
    marker: str,
) -> None:
    try:
        async with scenario.connection.transaction():
            await scenario.connection.execute(statement)
            await scenario.connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
    except asyncpg.PostgresError as error:
        assert marker in str(error)
    else:
        raise AssertionError(f"direct transition missing {marker}")


async def _prove_partial_publication_rejected(scenario) -> None:
    await _expect_direct_transition_rejected(
        scenario,
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'published', is_current = true,
               published_at = pg_catalog.transaction_timestamp()
         WHERE dataset_id = 'dataset-matched'
        """,
        "provider_directory_subset_published_source_invalid",
    )
    await _expect_direct_transition_rejected(
        scenario,
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET endpoint_id = 'endpoint-a',
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = 'synthetic-source'
           AND endpoint_id = '{_SERVING_ENDPOINT_ID}'
        """,
        "provider_directory_reviewed_subset_activation_transition_invalid",
    )
    assert await _source_endpoint(scenario, "synthetic-source") == (
        _SERVING_ENDPOINT_ID
    )


async def _artifact_fence(scenario, evidence_pairs):
    candidate_row = await scenario.connection.fetchrow(
        f"""
        SELECT dataset_hash, resource_count
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-matched'
        """
    )
    evidence = activation_evidence(evidence_pairs)
    candidate = importer.ProviderDirectoryArtifactDataset(
        source_id="synthetic-source",
        endpoint_id="endpoint-a",
        serving_endpoint_id=_SERVING_ENDPOINT_ID,
        dataset_id="dataset-matched",
        evidence_run_id="root-matched",
        recorded_expected_resources=(),
        status=importer.ENDPOINT_DATASET_VALIDATED,
        is_current=False,
        promote_on_cutover=True,
        dataset_hash=candidate_row["dataset_hash"],
        resource_count=candidate_row["resource_count"],
        completion_proof_required_version=3,
        completion_proof_sha256=evidence.completion_proof_sha256,
    )
    return importer.ProviderDirectoryArtifactDatasetFence(
        (candidate,),
        should_select_validated_candidates=True,
    )


async def _promote_with_rollback_probe(
    monkeypatch,
    scenario,
    publication_database,
    fence,
) -> None:
    original_publish = importer._publish_validated_artifact_dataset
    with monkeypatch.context() as rollback_patch:
        rollback_patch.setattr(importer, "db", publication_database)

        async def failed_publish(_dataset):
            raise RuntimeError("synthetic publication failure")

        rollback_patch.setattr(
            importer,
            "_publish_validated_artifact_dataset",
            failed_publish,
        )
        try:
            async with publication_database.transaction():
                await importer._lock_and_verify_artifact_dataset_fence(
                    fence,
                    publication_database,
                )
                await importer._promote_provider_directory_artifact_datasets(
                    fence
                )
        except RuntimeError as error:
            assert str(error) == "synthetic publication failure"
        else:
            raise AssertionError("failed publication committed alias cutover")
    assert importer._publish_validated_artifact_dataset is original_publish
    assert await _source_endpoint(scenario, "synthetic-source") == (
        _SERVING_ENDPOINT_ID
    )


async def _promote_atomically(
    monkeypatch,
    publication_database,
    fence,
) -> None:
    with monkeypatch.context() as publication_patch:
        publication_patch.setattr(importer, "db", publication_database)
        async with publication_database.transaction():
            await importer._lock_and_verify_artifact_dataset_fence(
                fence,
                publication_database,
            )
            await importer._promote_provider_directory_artifact_datasets(fence)


async def _prove_configured_collision_rejected(scenario) -> None:
    await _expect_direct_transition_rejected(
        scenario,
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES (
            'synthetic-configured-collision', 'endpoint-a',
            'https://collision.example.test/fhir', false, false, 'none',
            '{{}}'::jsonb, pg_catalog.transaction_timestamp()
        )
        """,
        "provider_directory_reviewed_subset_activation_source_invalid",
    )


async def prove_effective_endpoint_activation_and_publication(monkeypatch):
    """Prove split identity through activation and atomic publication."""

    scenario, activation_migration, evidence_pairs = (
        await _create_activation_scenario(monkeypatch)
    )
    activation_database = _runtime_database()
    publication_database = _runtime_database()
    try:
        effective_migration = _load_effective_endpoint_migration()
        await _split_source_endpoint_identity(scenario, effective_migration)
        await _install_effective_endpoint_migration(
            scenario,
            effective_migration,
        )
        async with scenario.connection.transaction():
            await run_subset_migration(
                _load_root_policy_migration(),
                "upgrade",
                scenario.connection,
            )
        activation_result = await activation.sync_reviewed_subset_verified_state(
            database=activation_database
        )
        assert activation_result.activated is True
        assert await _source_endpoint(scenario, "synthetic-source") == (
            _SERVING_ENDPOINT_ID
        )
        assert await is_activation_valid(scenario, activation_migration) is True
        await _prove_configured_collision_rejected(scenario)
        await _prove_partial_publication_rejected(scenario)
        fence = await _artifact_fence(scenario, evidence_pairs)
        await _promote_with_rollback_probe(
            monkeypatch,
            scenario,
            publication_database,
            fence,
        )
        await _promote_atomically(
            monkeypatch,
            publication_database,
            fence,
        )
        assert await _source_endpoint(scenario, "synthetic-source") == "endpoint-a"
        assert await _source_endpoint(
            scenario, "synthetic-serving-sibling"
        ) == _SERVING_ENDPOINT_ID
        assert await is_activation_valid(scenario, activation_migration) is True
        replay = await activation.sync_reviewed_subset_verified_state(
            database=activation_database
        )
        assert replay.is_already_applied is True
    finally:
        with suppress(Exception):
            await activation_database.disconnect()
        with suppress(Exception):
            await publication_database.disconnect()
        await _close_scenario(scenario)
