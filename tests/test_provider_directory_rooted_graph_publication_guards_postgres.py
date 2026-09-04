# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database-authority and rollback proofs for rooted graph publication."""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from tests.formulary_fhir_twin_admission_pg_support import (
    load_migration,
    run_migration,
)
from tests.provider_directory_rooted_graph_publication_pg_guard_support import (
    assert_graph_registry_drift_fences,
    commit_logical_current_race,
    complete_valid_missing,
    LEGACY_RELATION,
    payload_budget_identity,
    prepare_logical_current_race,
    prepare_missing_claim,
    ROOTED_RELATION,
    write_forged_missing,
)
from tests.test_provider_directory_rooted_graph_acquisition_postgres import _identity
from tests.test_provider_directory_rooted_graph_publication_postgres import (
    _lifecycle_scope,
    _publish_legacy_root,
)


EXACT_GUARD_SCOPE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260904163000_provider_directory_exact_guard_scope.py"
)


@pytest.mark.parametrize("first_relation", (ROOTED_RELATION, LEGACY_RELATION))
@pytest.mark.asyncio
async def test_logical_current_guard_serializes_direct_cross_endpoint_race(
    monkeypatch,
    first_relation: str,
) -> None:
    """Prove the DB advisory lock closes cross-endpoint write skew."""

    async with _lifecycle_scope(monkeypatch) as context:
        migration = load_migration(
            EXACT_GUARD_SCOPE_MIGRATION_PATH,
            f"provider_directory_exact_guard_scope_race_{first_relation}",
        )
        await run_migration(context.engine, migration, "upgrade")
        legacy_id, rooted_id = await prepare_logical_current_race(context)
        first_outcome, second_outcome, current_ids = await commit_logical_current_race(
            context,
            first_relation,
            legacy_id,
            rooted_id,
        )
        assert (first_outcome, second_outcome) == ("committed", "23514")
        expected_id = legacy_id if first_relation == LEGACY_RELATION else rooted_id
        assert current_ids == [expected_id]


@pytest.mark.asyncio
async def test_logical_current_guard_skips_unrelated_parent_rows(
    monkeypatch,
) -> None:
    """Keep exact validation on exact rows without taxing other endpoints."""

    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        migration = load_migration(
            EXACT_GUARD_SCOPE_MIGRATION_PATH,
            "provider_directory_exact_guard_scope_postgres",
        )
        await run_migration(context.engine, migration, "upgrade")
        await context.connection.execute(
            f"""
            CREATE OR REPLACE FUNCTION
                {context.schema}.provider_directory_uhc_flex_practitioner_dataset_valid(
                    candidate_dataset_id text
                )
            RETURNS boolean LANGUAGE plpgsql AS $function$
            BEGIN
                RAISE EXCEPTION 'exact validity called';
            END;
            $function$;
            """
        )

        unrelated_endpoint_id = "f" * 64
        await context.connection.execute(
            f"INSERT INTO {context.schema}.provider_directory_api_endpoint "
            "(endpoint_id, canonical_api_base, credential_descriptor_hash, "
            "endpoint_signature_hash) VALUES ($1, $2, $3, $4)",
            unrelated_endpoint_id,
            "https://example.invalid/fhir",
            "0" * 64,
            "1" * 64,
        )
        await context.connection.execute(
            f"INSERT INTO {context.schema}.provider_directory_endpoint_dataset "
            "(dataset_id, endpoint_id, dataset_hash, status, is_current, "
            "resource_count, publication_metadata_json) "
            "VALUES ('dataset-unrelated', $1, $2, 'building', false, 0, '{}'::jsonb)",
            unrelated_endpoint_id,
            "2" * 64,
        )
        await context.connection.execute(
            f"UPDATE {context.schema}.provider_directory_endpoint_dataset "
            "SET resource_count = 1 WHERE dataset_id = 'dataset-unrelated'"
        )
        with pytest.raises(asyncpg.PostgresError, match="exact validity called"):
            await context.connection.execute(
                f"UPDATE {context.schema}.provider_directory_endpoint_dataset "
                "SET endpoint_id = $1 WHERE dataset_id = 'dataset-unrelated'",
                migration._rooted()._LEGACY_ENDPOINT_ID,
            )
        await context.connection.execute(
            f"DELETE FROM {context.schema}.provider_directory_endpoint_dataset "
            "WHERE dataset_id = 'dataset-unrelated'"
        )

        with pytest.raises(asyncpg.PostgresError, match="exact validity called"):
            await context.connection.execute(
                f"UPDATE {context.schema}.provider_directory_endpoint_dataset "
                "SET resource_count = resource_count WHERE dataset_id = $1",
                current.dataset_id,
            )


@pytest.mark.asyncio
async def test_missing_body_budget_cap_rolls_back_terminal_update(monkeypatch) -> None:
    """Count retained missing bodies and reject cap-plus-one atomically."""

    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity, retained_resource_bytes = payload_budget_identity(current)
        _role_result, claim = await prepare_missing_claim(context, identity)
        assert (
            await context.database.scalar(
                f"SELECT used_payload_bytes FROM {context.schema}."
                "provider_directory_rooted_graph_acquisition "
                "WHERE acquisition_id = :acquisition_id",
                acquisition_id=identity.acquisition_id,
            )
            == retained_resource_bytes
        )

        with pytest.raises(DBAPIError, match="budget_exceeded"):
            await complete_valid_missing(context.database, claim)

        retained = await context.database.first(
            f"SELECT acquisition.used_payload_bytes, work.status, "
            "work.missing_response_json_text "
            f"FROM {context.schema}.provider_directory_rooted_graph_acquisition "
            "AS acquisition JOIN "
            f"{context.schema}.provider_directory_rooted_graph_work AS work "
            "USING (acquisition_id) WHERE acquisition.acquisition_id = "
            ":acquisition_id AND work.resource_type = 'Endpoint'",
            acquisition_id=identity.acquisition_id,
        )
        assert retained.used_payload_bytes == retained_resource_bytes
        assert retained.status == "leased"
        assert retained.missing_response_json_text is None


@pytest.mark.asyncio
async def test_database_rejects_semantically_forged_missing_body(monkeypatch) -> None:
    """Reject a hash-consistent 404 body outside the reviewed outcome shapes."""

    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity = _identity(current, "baseline", "d", "9")
        _role_result, claim = await prepare_missing_claim(context, identity)

        with pytest.raises(DBAPIError, match="result_invalid"):
            await write_forged_missing(context, claim)

        retained = await context.database.first(
            f"SELECT status, missing_response_json_text FROM {context.schema}."
            "provider_directory_rooted_graph_work WHERE acquisition_id = "
            ":acquisition_id AND query_id = :query_id",
            acquisition_id=claim.acquisition_id,
            query_id=claim.query_id,
        )
        assert retained.status == "leased"
        assert retained.missing_response_json_text is None


@pytest.mark.asyncio
async def test_graph_registry_rows_replay_and_reject_drift(monkeypatch) -> None:
    """Prove exact graph registration survives replay and immutable drift attempts."""

    async with _lifecycle_scope(monkeypatch) as context:
        await assert_graph_registry_drift_fences(context)
