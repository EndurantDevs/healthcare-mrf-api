# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Executable PostgreSQL lifecycle for one reviewed acquisition root."""

from __future__ import annotations

from copy import deepcopy
import importlib.util
import json
from pathlib import Path

from process import provider_directory_fhir_subset_activation as activation
from process import provider_directory_fhir_subset_activation_selection as selection
from tests.provider_directory_effective_endpoint_pg_cases import (
    _load_effective_endpoint_migration,
)
from tests.provider_directory_fhir_subset_abandonment_pg_support import (
    create_abandonment_relations,
)
from tests.provider_directory_fhir_subset_activation_support import (
    _single_root_content_proof,
    single_root_activation_inputs,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    flush_deferred_fixture_events,
    load_activation_migration,
)
from tests.provider_directory_reviewed_subset_activation_pg_upsert import (
    prove_policy_catalog_upserts_preserve_activation,
)
from tests.provider_directory_subset_completion_pg_setup import (
    MigrationSqlCapture,
    extend_source_fixture_table,
    insert_subset_candidate,
    insert_valid_subset_resources,
    load_abandonment_migration,
    load_migration,
    load_payload_guard_repair_migration,
    run_subset_migration,
)
from tests.provider_directory_subset_completion_pg_support import (
    RESOURCE_TYPES,
)
from tests.tin_npi_connector_postgres_support import (
    TransactionalSchema,
    expect_postgres_error,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260809030000_provider_directory_reviewed_root_policy.py"
)


def _load_policy_migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_reviewed_root_policy_postgres_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


async def _run_upgrade_with_context(scenario, migration) -> None:
    capture = MigrationSqlCapture()
    migration.op = capture
    migration.upgrade()
    for statement_index, statement in enumerate(capture.statements):
        try:
            await scenario.connection.execute(statement)
        except Exception as error:
            raise AssertionError(
                f"failed migration {migration.revision} "
                f"statement {statement_index} at "
                f"{getattr(error, 'position', None)}: {error}"
            ) from error


def _replaced_function_names(policy_migration) -> tuple[str, ...]:
    subset = policy_migration._subset()
    activation_migration = policy_migration._activation()
    abandonment = policy_migration._abandonment()
    return (
        subset._COVERAGE_SHAPE_VALID_FUNCTION,
        subset._ENDPOINT_DATASET_GUARD,
        subset._SOURCE_GUARD,
        activation_migration._ACTIVATION_VALID_FUNCTION,
        activation_migration._SOURCE_GUARD_FUNCTION,
        activation_migration._DATASET_GUARD_FUNCTION,
        abandonment._DATASET_GUARD,
    )


async def _function_oids(scenario, function_names) -> dict[str, int]:
    rows = await scenario.connection.fetch(
        """
        SELECT function_row.proname, function_row.oid
          FROM pg_catalog.pg_proc AS function_row
          JOIN pg_catalog.pg_namespace AS namespace_row
            ON namespace_row.oid = function_row.pronamespace
         WHERE namespace_row.nspname = $1
           AND function_row.proname = ANY($2::text[])
        """,
        scenario.schema,
        function_names,
    )
    return {row["proname"]: row["oid"] for row in rows}


async def _install_policy_predecessors(scenario) -> tuple[object, object]:
    subset_migration = load_migration()
    activation_migration = load_activation_migration()
    await scenario.upgrade()
    await extend_source_fixture_table(scenario)
    await run_subset_migration(
        subset_migration,
        "upgrade",
        scenario.connection,
    )
    await create_abandonment_relations(scenario)
    for migration in (
        activation_migration,
        load_payload_guard_repair_migration(),
        load_abandonment_migration(),
        _load_effective_endpoint_migration(),
    ):
        await _run_upgrade_with_context(scenario, migration)
    policy_migration = _load_policy_migration()
    function_names = _replaced_function_names(policy_migration)
    before_oids = await _function_oids(scenario, function_names)
    assert len(before_oids) == len(function_names)
    await _run_upgrade_with_context(scenario, policy_migration)
    assert await _function_oids(scenario, function_names) == before_oids
    return subset_migration, activation_migration


async def _insert_policy_source(scenario, source_record) -> None:
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_api_endpoint (
            endpoint_id
        ) VALUES ($1)
        """,
        source_record["endpoint_id"],
    )
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.provider_directory_source (
            source_id, endpoint_id, canonical_api_base,
            requires_registration, requires_api_key, auth_type,
            metadata_json, updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb,
                  pg_catalog.transaction_timestamp())
        """,
        source_record["source_id"],
        source_record["endpoint_id"],
        source_record["canonical_api_base"],
        source_record["requires_registration"],
        source_record["requires_api_key"],
        source_record["auth_type"],
        json.dumps(source_record["metadata_json"]),
    )


async def _terminalize_candidate(
    scenario,
    dataset_row,
    *,
    dataset_id: str = "dataset-candidate",
) -> None:
    await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1,
               status = 'validated',
               is_current = false,
               validated_at = pg_catalog.transaction_timestamp(),
               publication_metadata_json = $2::jsonb,
               completion_proof_json = $3::jsonb,
               completion_proof_sha256 = $4
         WHERE dataset_id = $5
        """,
        dataset_row["dataset_hash"],
        json.dumps(dataset_row["publication_metadata_json"]),
        json.dumps(dataset_row["completion_proof_json"]),
        dataset_row["completion_proof_sha256"],
        dataset_id,
    )


async def _activate_policy_source(
    scenario,
    activation_migration,
    source_record,
    dataset_row,
    evidence,
) -> dict[str, object]:
    chosen = selection.validated_reviewed_subset_activation_selection(
        source_rows=(source_record,),
        dataset_rows=(dataset_row,),
        expected_source_id=source_record["source_id"],
        evidence=evidence,
    )
    marker = chosen.metadata_marker()
    marker_shape_sql = activation_migration._activation_marker_v2_shape_sql(
        "$1::jsonb"
    )
    assert await scenario.connection.fetchval(
        f"SELECT ({marker_shape_sql})",
        json.dumps(marker),
    ) is True
    numeric_marker = deepcopy(marker)
    numeric_marker["candidate"]["coverage_sha256"] = int("1" * 64)
    assert await scenario.connection.fetchval(
        f"SELECT ({marker_shape_sql})",
        json.dumps(numeric_marker),
    ) is False
    update_status = await scenario.connection.execute(
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_source
           SET metadata_json = pg_catalog.jsonb_set(
                   pg_catalog.jsonb_set(
                       metadata_json::jsonb,
                       '{{provider_directory_candidate_status}}',
                       pg_catalog.to_jsonb(
                           'verified_reviewed_subset_acquisition'::text
                       ),
                       true
                   ),
                   '{{provider_directory_reviewed_subset_activation_v2}}',
                   $1::jsonb,
                   true
               ),
               updated_at = pg_catalog.transaction_timestamp()
         WHERE source_id = $2
        """,
        json.dumps(marker, sort_keys=True, separators=(",", ":")),
        source_record["source_id"],
    )
    assert update_status == "UPDATE 1"
    await scenario.connection.execute(
        f"SET CONSTRAINTS {scenario.quoted_schema}."
        f'"{activation_migration._SOURCE_GUARD_TRIGGER}" IMMEDIATE'
    )
    return marker


async def _prove_malformed_content_proof_rejected(scenario, dataset_row) -> None:
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-malformed-policy-root",
        root_run_id="root-malformed-policy-root",
    )
    await insert_valid_subset_resources(
        scenario,
        "dataset-malformed-policy-root",
    )
    malformed_row = deepcopy(dataset_row)
    malformed_metadata = malformed_row["publication_metadata_json"]
    malformed_metadata["acquisition_root_run_id"] = (
        "root-malformed-policy-root"
    )
    malformed_proof = _single_root_content_proof(
        malformed_row["completion_proof_json"],
        dataset_id="dataset-malformed-policy-root",
        root_run_id="root-malformed-policy-root",
    )
    malformed_proof.pop("proof_sha256")
    malformed_metadata["provider_directory_content_proof_v1"] = malformed_proof
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_subset_matched_twin_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1, status = 'validated',
               validated_at = pg_catalog.transaction_timestamp(),
               publication_metadata_json = $2::jsonb,
               completion_proof_json = $3::jsonb,
               completion_proof_sha256 = $4
         WHERE dataset_id = 'dataset-malformed-policy-root'
        """,
        malformed_row["dataset_hash"],
        json.dumps(malformed_metadata),
        json.dumps(malformed_row["completion_proof_json"]),
        malformed_row["completion_proof_sha256"],
    )


async def _prove_extra_policy_root_rejected(scenario, dataset_row) -> None:
    await insert_subset_candidate(
        scenario,
        dataset_id="dataset-extra-policy-root",
        root_run_id="root-extra-policy-root",
    )
    await insert_valid_subset_resources(scenario, "dataset-extra-policy-root")
    extra_row = deepcopy(dataset_row)
    extra_metadata = extra_row["publication_metadata_json"]
    extra_metadata["acquisition_root_run_id"] = "root-extra-policy-root"
    extra_metadata["provider_directory_content_proof_v1"] = (
        _single_root_content_proof(
            extra_row["completion_proof_json"],
            dataset_id="dataset-extra-policy-root",
            root_run_id="root-extra-policy-root",
        )
    )
    await expect_postgres_error(
        scenario.connection,
        "provider_directory_reviewed_subset_activation_dataset_invalid",
        f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET dataset_hash = $1, status = 'validated',
               validated_at = pg_catalog.transaction_timestamp(),
               publication_metadata_json = $2::jsonb,
               completion_proof_json = $3::jsonb,
               completion_proof_sha256 = $4
         WHERE dataset_id = 'dataset-extra-policy-root'
        """,
        extra_row["dataset_hash"],
        json.dumps(extra_metadata),
        json.dumps(extra_row["completion_proof_json"]),
        extra_row["completion_proof_sha256"],
    )


async def _assert_published_policy_state(
    scenario,
    activation_migration,
    source_record,
    dataset_row,
    marker,
) -> None:
    policy_migration = _load_policy_migration()
    is_valid = await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}."
        f'"{activation_migration._ACTIVATION_VALID_FUNCTION}"($1)',
        source_record["source_id"],
    )
    assert is_valid is True
    assert marker["root_policy"]["required_root_count"] == 1
    assert await scenario.connection.fetchval(
        f"""
        SELECT pg_catalog.count(*)
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE completion_proof_required_version = 3
           AND status IN ('verification_baseline', 'verification_mismatch')
        """
    ) == 0
    content_proof = dataset_row["publication_metadata_json"][
        "provider_directory_content_proof_v1"
    ]
    completion_dataset = dataset_row["completion_proof_json"]["dataset"]
    assert await scenario.connection.fetchval(
        f"SELECT {scenario.quoted_schema}."
        f'"{policy_migration._subset()._CONTENT_PROOF_VALID_FUNCTION}"('
        "$1::jsonb, $2, $3, $4, $5::jsonb, $6::jsonb, $7, $8, "
        "$9::jsonb, $10::jsonb)",
        json.dumps(content_proof),
        dataset_row["dataset_id"],
        dataset_row["endpoint_id"],
        dataset_row["acquisition_root_run_id"],
        json.dumps(dataset_row["publication_metadata_json"]["source_ids"]),
        json.dumps(dataset_row["publication_metadata_json"]["selected_resources"]),
        dataset_row["dataset_hash"],
        dataset_row["resource_count"],
        json.dumps(completion_dataset["resource_hashes"]),
        json.dumps(completion_dataset["resource_counts"]),
    ) is True


async def prove_single_root_policy_lifecycle(monkeypatch) -> None:
    """Prove one root completes, activates, publishes, and blocks drift."""

    scenario = await TransactionalSchema.create(monkeypatch)
    try:
        _subset_migration, activation_migration = (
            await _install_policy_predecessors(scenario)
        )
        source_record, dataset_rows, evidence = single_root_activation_inputs()
        dataset_row = dataset_rows[0]
        await _insert_policy_source(scenario, source_record)
        await insert_subset_candidate(
            scenario,
            dataset_id="dataset-candidate",
            root_run_id="root-candidate",
        )
        await insert_valid_subset_resources(scenario, "dataset-candidate")
        await _prove_malformed_content_proof_rejected(scenario, dataset_row)
        await _terminalize_candidate(scenario, dataset_row)
        await flush_deferred_fixture_events(scenario)
        marker = await _activate_policy_source(
            scenario,
            activation_migration,
            source_record,
            dataset_row,
            evidence,
        )
        await prove_policy_catalog_upserts_preserve_activation(
            scenario,
            marker,
        )
        await scenario.connection.execute(
            f"""
            UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'published', is_current = true,
                   published_at = pg_catalog.transaction_timestamp()
             WHERE dataset_id = 'dataset-candidate'
            """
        )
        await _assert_published_policy_state(
            scenario,
            activation_migration,
            source_record,
            dataset_row,
            marker,
        )
        await _prove_extra_policy_root_rejected(scenario, dataset_row)
        assert set(dataset_row["publication_metadata_json"]["selected_resources"]) == set(
            RESOURCE_TYPES
        )
    finally:
        await scenario.close()
