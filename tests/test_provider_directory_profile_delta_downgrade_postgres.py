# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL refusal tests for Provider Directory Profile downgrade."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy.exc import DBAPIError

from db.connection import Database
from tests.test_provider_directory_profile_capacity_attestation_postgres import (
    _apply_profile_delta_migration,
    _require_disposable_database,
)
from tests.test_provider_directory_profile_capacity_attestation_schema import (
    _load_migration,
)


async def _create_downgrade_preflight_schema(
    database: Database,
    schema: str,
) -> None:
    """Create only the relations and columns read by downgrade preflight."""
    await database.status(f'CREATE SCHEMA "{schema}";')
    await database.status(
        f"""
        CREATE TABLE "{schema}".
            provider_directory_profile_build_checkpoint (
                build_id text PRIMARY KEY,
                executable_plan_hash text,
                materialization_mode text NOT NULL DEFAULT 'full_swap',
                current_source_vector_hash text,
                desired_source_vector_hash text,
                current_source_context_vector_hash text,
                desired_source_context_vector_hash text,
                refresh_source_ids jsonb,
                removed_source_ids jsonb,
                affected_npi_stage text,
                affected_npi_stage_oid bigint,
                evidence_stage_storage_fingerprint text,
                profile_stage_storage_fingerprint text,
                affected_npi_stage_storage_fingerprint text,
                capacity_geometry_status text
                    NOT NULL DEFAULT 'legacy_unavailable',
                capacity_geometry_hash text,
                capacity_geometry_json jsonb,
                cutover_forecast_status text
                    NOT NULL DEFAULT 'not_started',
                cutover_forecast_hash text,
                cutover_forecast_json jsonb
            );
        """
    )
    for table_name in (
        "provider_directory_profile_capacity_lease_consumption",
        "provider_directory_profile_delta_receipt",
        "provider_directory_profile_serving_generation",
    ):
        await database.status(
            f'CREATE TABLE "{schema}"."{table_name}" (marker integer);'
        )


_DOWNGRADE_REFUSAL_SCENARIOS = (
    (
        "serving_generation",
        "INSERT INTO {schema}.provider_directory_profile_serving_generation "
        "VALUES (1);",
        "downgrade_serving_generation_not_empty",
    ),
    (
        "delta_receipt",
        "INSERT INTO {schema}.provider_directory_profile_delta_receipt "
        "VALUES (1);",
        "downgrade_receipt_not_empty",
    ),
    (
        "capacity_consumption",
        "INSERT INTO {schema}."
        "provider_directory_profile_capacity_lease_consumption VALUES (1);",
        "downgrade_capacity_consumption_not_empty",
    ),
    (
        "source_delta_checkpoint",
        "INSERT INTO {schema}.provider_directory_profile_build_checkpoint "
        "(build_id, materialization_mode) VALUES ('build', 'source_delta');",
        "downgrade_checkpoint_uses_delta_state",
    ),
    (
        "verified_capacity_checkpoint",
        "INSERT INTO {schema}.provider_directory_profile_build_checkpoint "
        "(build_id, capacity_geometry_status) VALUES ('build', 'verified');",
        "downgrade_checkpoint_uses_delta_state",
    ),
    (
        "verified_forecast_checkpoint",
        "INSERT INTO {schema}.provider_directory_profile_build_checkpoint "
        "(build_id, cutover_forecast_status) VALUES ('build', 'verified');",
        "downgrade_checkpoint_uses_delta_state",
    ),
    (
        "stage_identity_checkpoint",
        "INSERT INTO {schema}.provider_directory_profile_build_checkpoint "
        "(build_id, affected_npi_stage, affected_npi_stage_oid, "
        "evidence_stage_storage_fingerprint, "
        "profile_stage_storage_fingerprint, "
        "affected_npi_stage_storage_fingerprint) VALUES "
        "('build', 'affected_stage', 1, 'evidence', 'profile', 'affected');",
        "downgrade_checkpoint_uses_delta_state",
    ),
)


@pytest.mark.parametrize(
    ("scenario_name", "setup_template", "error_message"),
    _DOWNGRADE_REFUSAL_SCENARIOS,
)
@pytest.mark.asyncio
async def test_profile_delta_downgrade_refuses_nonlegacy_state(
    monkeypatch,
    scenario_name,
    setup_template,
    error_message,
):
    """Keep rollback destructive steps behind exact empty-state guards."""
    database = Database()
    schema = f"pd_delta_down_{scenario_name}_{uuid.uuid4().hex[:8]}"
    is_schema_created = False
    try:
        await database.connect()
        await _require_disposable_database(database)
        await _create_downgrade_preflight_schema(database, schema)
        is_schema_created = True
        schema_ref = f'"{schema}"'
        await database.status(setup_template.format(schema=schema_ref))
        monkeypatch.setenv("DB_SCHEMA", schema)
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
        migration = _load_migration()

        with pytest.raises(DBAPIError, match=error_message):
            await _apply_profile_delta_migration(
                database,
                migration,
                "downgrade",
            )

        assert await database.scalar(
            """
            SELECT count(*)
              FROM information_schema.tables
             WHERE table_schema = :schema
               AND table_name LIKE 'provider_directory_profile_%';
            """,
            schema=schema,
        ) == 4
        assert await database.scalar(
            f"""
            SELECT (SELECT count(*) FROM {schema_ref}.
                        provider_directory_profile_build_checkpoint)
                 + (SELECT count(*) FROM {schema_ref}.
                        provider_directory_profile_serving_generation)
                 + (SELECT count(*) FROM {schema_ref}.
                        provider_directory_profile_delta_receipt)
                 + (SELECT count(*) FROM {schema_ref}.
                        provider_directory_profile_capacity_lease_consumption);
            """
        ) == 1
    finally:
        if is_schema_created:
            await database.status(
                f'DROP SCHEMA IF EXISTS "{schema}" CASCADE;'
            )
        await database.disconnect()
