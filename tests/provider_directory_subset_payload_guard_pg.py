# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL proof for the subset payload guard repair."""

from __future__ import annotations

from tests.provider_directory_subset_completion_pg_setup import (
    extend_source_fixture_table,
    load_migration,
    load_payload_guard_repair_migration,
    run_subset_migration,
)
from tests.provider_directory_reviewed_subset_activation_pg_support import (
    load_activation_migration,
)
from tests.tin_npi_connector_postgres_support import (
    TransactionalSchema,
    expect_postgres_error,
)


def _failed_update_sql(scenario) -> str:
    return f"""
        UPDATE {scenario.quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'failed',
               publication_metadata_json =
                   '{{"error":"synthetic-fetch-failure"}}'::json
         WHERE dataset_id = 'dataset-repair'
    """


async def _prepare_legacy_json_guard(scenario) -> tuple[object, str]:
    subset_migration = load_migration()
    activation_migration = load_activation_migration()
    await scenario.upgrade()
    await extend_source_fixture_table(scenario)
    await scenario.connection.execute(
        f"ALTER TABLE {scenario.quoted_schema}."
        "provider_directory_dataset_resource "
        "ALTER COLUMN payload_json TYPE json USING payload_json::json"
    )
    for migration in (subset_migration, activation_migration):
        await run_subset_migration(
            migration,
            "upgrade",
            scenario.connection,
        )
    legacy_guard_sql = subset_migration._subset_endpoint_dataset_guard_sql(
        scenario.schema
    ).replace("child.payload_json::jsonb", "child.payload_json")
    await scenario.connection.execute(legacy_guard_sql)
    await scenario.connection.execute(
        f"""
        INSERT INTO {scenario.quoted_schema}.
            provider_directory_endpoint_dataset (
                dataset_id, endpoint_id, acquisition_root_run_id,
                status, is_current, resource_count,
                publication_metadata_json,
                completion_proof_required_version
            ) VALUES (
                'dataset-repair', 'endpoint-a', 'root-repair',
                'acquiring', false, 0, '{{}}'::json, 3
            )
        """
    )
    failed_update_sql = _failed_update_sql(scenario)
    await expect_postgres_error(
        scenario.connection,
        "operator does not exist: json - unknown",
        failed_update_sql,
    )
    return load_payload_guard_repair_migration(), failed_update_sql


async def _assert_repaired_failed_update(scenario, failed_update_sql: str) -> None:
    update_result = await scenario.connection.execute(failed_update_sql)
    repaired_state = await scenario.connection.fetchrow(
        f"""
        SELECT status, publication_metadata_json::jsonb ->> 'error' AS error
          FROM {scenario.quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-repair'
        """
    )
    assert update_result == "UPDATE 1"
    assert dict(repaired_state) == {
        "status": "failed",
        "error": "synthetic-fetch-failure",
    }


async def prove_subset_payload_guard_repair(monkeypatch) -> None:
    """Replace the deployed guard body before lifecycle bookkeeping."""

    scenario = await TransactionalSchema.create(monkeypatch)
    try:
        repair_migration, failed_update_sql = (
            await _prepare_legacy_json_guard(scenario)
        )
        await run_subset_migration(
            repair_migration,
            "upgrade",
            scenario.connection,
        )
        await _assert_repaired_failed_update(scenario, failed_update_sql)
    finally:
        await scenario.close()
