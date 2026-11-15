# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Database-authority and rollback proofs for rooted graph publication."""

from __future__ import annotations

from pathlib import Path

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process.provider_directory_dataset_scoped_publication import exact_uhc_dataset_pair
from process.provider_directory_rooted_graph_store import complete_provider_directory_rooted_graph_missing
from process.provider_directory_rooted_graph_store_contract import ProviderDirectoryRootedGraphStoreError
from tests.formulary_fhir_twin_admission_pg_support import (
    load_migration,
    quoted,
)
from tests.provider_directory_rooted_graph_publication_pg_guard_support import (
    LEGACY_RELATION,
    ROOTED_RELATION,
    assert_graph_registry_drift_fences,
    complete_valid_missing,
    forged_missing_by_field,
    missing_response_text,
    payload_budget_identity,
    prepare_missing_claim,
)
from tests.test_provider_directory_rooted_graph_acquisition_postgres import _identity
from tests.test_provider_directory_rooted_graph_publication_postgres import (
    _lifecycle_scope,
    _publish_legacy_root,
)

EXACT_GUARD_SCOPE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/20260904163000_provider_directory_exact_guard_scope.py"
)


async def _make_exact_validity_fail(context) -> None:
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


async def _assert_header_statement_denied(reader, statement):
    """Require a native privilege error while preserving the surrounding connection."""
    with pytest.raises(DBAPIError) as refused:
        async with reader.begin_nested():
            await reader.execute(text(statement))
    assert refused.value.orig.sqlstate == "42501"


async def _assert_reader_header_dml_denied(context, relation):
    """Exercise PostgreSQL grants with the configured Reader login in a writable transaction."""
    reader_url = context.component.admin_engine.url.set(username=context.component.reader)
    reader_engine = create_async_engine(reader_url)
    qualified_relation = f"{context.schema}.{quoted(relation)}"
    statements = (
        f"UPDATE {qualified_relation} SET dataset_hash=dataset_hash WHERE false",
        f"DELETE FROM {qualified_relation} WHERE false",
        f"INSERT INTO {qualified_relation} SELECT * FROM {qualified_relation} WHERE false",
    )
    try:
        async with reader_engine.begin() as reader:
            for statement in statements:
                await _assert_header_statement_denied(reader, statement)
    finally:
        await reader_engine.dispose()


@pytest.mark.parametrize("relation", (ROOTED_RELATION, LEGACY_RELATION))
@pytest.mark.asyncio
async def test_source_reader_cannot_write_canonical_publication_headers(monkeypatch, relation):
    """Keep native Reader authorization separate from the publisher's mutable control-table CAS."""
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        before = await context.connection.fetchrow(
            f"SELECT * FROM {context.schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1",
            current.dataset_id,
        )
        await _assert_reader_header_dml_denied(context, relation)
        assert (
            await context.connection.fetchrow(
                f"SELECT * FROM {context.schema}.provider_directory_endpoint_dataset WHERE dataset_id=$1",
                current.dataset_id,
            )
            == before
        )
        assert (
            await context.connection.fetchval(
                f"SELECT count(*) FROM {context.schema}.provider_directory_endpoint_dataset WHERE is_current "
                "AND endpoint_id IN ($1,$2)",
                exact_uhc_dataset_pair().legacy_endpoint_id,
                exact_uhc_dataset_pair().rooted_endpoint_id,
            )
            == 1
        )


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
        await _make_exact_validity_fail(context)

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
            f"DELETE FROM {context.schema}.provider_directory_endpoint_dataset WHERE dataset_id = 'dataset-unrelated'"
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
                f"SELECT used_payload_bytes FROM {quoted(context.rooted_input_schema)}."
                "provider_directory_rooted_graph_acquisition "
                "WHERE acquisition_id = :acquisition_id",
                acquisition_id=identity.acquisition_id,
            )
            == retained_resource_bytes
        )

        with pytest.raises(ProviderDirectoryRootedGraphStoreError) as refusal:
            await complete_valid_missing(context.database, claim)
        assert refusal.value.code == "state"

        retained = await context.database.first(
            f"SELECT acquisition.used_payload_bytes, work.status, "
            "work.missing_response_json_text "
            f"FROM {quoted(context.rooted_input_schema)}.provider_directory_rooted_graph_acquisition "
            "AS acquisition JOIN "
            f"{quoted(context.rooted_input_schema)}.provider_directory_rooted_graph_work AS work "
            "USING (acquisition_id) WHERE acquisition.acquisition_id = "
            ":acquisition_id AND work.resource_type = 'Endpoint'",
            acquisition_id=identity.acquisition_id,
        )
        assert retained.used_payload_bytes == retained_resource_bytes
        assert retained.status == "leased"
        assert retained.missing_response_json_text is None


@pytest.mark.asyncio
async def test_missing_body_accounting_rolls_back_and_refuses_replay(monkeypatch):
    """Count one native missing witness, preserving rollback and exact lease replay refusal."""
    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity = _identity(current, "baseline", "e", "a")
        _, claim = await prepare_missing_claim(context, identity)
        header = f"{quoted(context.rooted_input_schema)}.provider_directory_rooted_graph_acquisition"
        count_sql = f"SELECT used_payload_bytes FROM {header} WHERE acquisition_id=:acquisition_id"
        before_bytes = await context.database.scalar(count_sql, acquisition_id=identity.acquisition_id)
        response_bytes = len(missing_response_text().encode("utf-8"))
        with pytest.raises(RuntimeError, match="synthetic late failure"):
            async with context.database.transaction():
                await complete_valid_missing(context.database, claim)
                assert await context.database.scalar(count_sql, acquisition_id=identity.acquisition_id) == (
                    before_bytes + response_bytes
                )
                raise RuntimeError("synthetic late failure")
        assert await context.database.scalar(count_sql, acquisition_id=identity.acquisition_id) == before_bytes
        await complete_valid_missing(context.database, claim)
        assert await context.database.scalar(count_sql, acquisition_id=identity.acquisition_id) == (
            before_bytes + response_bytes
        )
        with pytest.raises(ProviderDirectoryRootedGraphStoreError) as refusal:
            await complete_valid_missing(context.database, claim)
        assert refusal.value.code == "lease_lost"
        assert await context.database.scalar(count_sql, acquisition_id=identity.acquisition_id) == (
            before_bytes + response_bytes
        )


@pytest.mark.asyncio
async def test_database_rejects_semantically_forged_missing_body(monkeypatch) -> None:
    """Refuse forged missing-body semantics before changing the genuine native lease."""

    async with _lifecycle_scope(monkeypatch) as context:
        current = await _publish_legacy_root(context.database)
        identity = _identity(current, "baseline", "d", "9")
        _role_result, claim = await prepare_missing_claim(context, identity)

        forged_by_field = forged_missing_by_field(claim)
        with pytest.raises(ValueError, match="missing_invalid"):
            await complete_provider_directory_rooted_graph_missing(
                claim,
                missing_http_status=404,
                missing_response_sha256=forged_by_field["forged_sha"],
                missing_response_bytes=forged_by_field["forged_bytes"],
                missing_response_json_text=forged_by_field["forged_text"],
                database=context.database,
            )

        retained = await context.database.first(
            f"SELECT status, missing_response_json_text FROM {quoted(context.rooted_input_schema)}."
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
