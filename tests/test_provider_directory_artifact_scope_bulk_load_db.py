# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest
from sqlalchemy.exc import IntegrityError

from db.connection import Database
from tests.test_provider_directory_dataset_artifact_db import (
    _dataset_database,
    _scope_table_names,
    importer,
)


async def _scope_primary_key_rows(
    database: Database,
    schema: str,
    table_names: list[str],
) -> dict[str, dict[str, str]]:
    rows = await database.all(
        "SELECT scope_table.relname AS table_name, "
        "primary_key.conname AS constraint_name, "
        "primary_index.relname AS index_name, "
        "pg_get_constraintdef(primary_key.oid) AS definition "
        "FROM pg_constraint AS primary_key "
        "JOIN pg_class AS scope_table ON scope_table.oid = primary_key.conrelid "
        "JOIN pg_namespace AS scope_schema ON scope_schema.oid = scope_table.relnamespace "
        "JOIN pg_class AS primary_index ON primary_index.oid = primary_key.conindid "
        "WHERE scope_schema.nspname = :schema_name "
        "AND scope_table.relname = ANY(CAST(:table_names AS varchar[])) "
        "AND primary_key.contype = 'p' "
        "ORDER BY scope_table.relname;",
        schema_name=schema,
        table_names=table_names,
    )
    return {row._mapping["table_name"]: dict(row._mapping) for row in rows}


@pytest.mark.asyncio
async def test_real_postgres_scope_tables_attach_primary_keys_before_use(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )

        async with importer._provider_directory_artifact_dataset_scope(
            run_id="primary-key-run",
            source_ids=["source_primary"],
            fence=fence,
        ):
            scope_tables_by_live_name = (
                importer._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()
            )
            scope_table_names = list(scope_tables_by_live_name.values())
            primary_keys_by_table = await _scope_primary_key_rows(
                database,
                schema,
                scope_table_names,
            )

            assert set(primary_keys_by_table) == set(scope_table_names)
            for live_table_name, table_name in scope_tables_by_live_name.items():
                primary_key = primary_keys_by_table[table_name]
                _, expected_name = importer._artifact_scope_pk_names(table_name)
                assert primary_key["constraint_name"] == expected_name
                assert primary_key["index_name"] == expected_name
                assert len(primary_key["constraint_name"]) <= 63
                expected_columns = (
                    "source_id"
                    if live_table_name == "provider_directory_source"
                    else "source_id, resource_id"
                )
                assert primary_key["definition"] == (
                    f"PRIMARY KEY ({expected_columns})"
                )

            source_scope = scope_tables_by_live_name["provider_directory_source"]
            empty_scope = scope_tables_by_live_name["provider_directory_practitioner"]
            assert await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.{source_scope};"
            ) == 1
            assert await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.{empty_scope};"
            ) == 0

        assert await _scope_table_names(database, schema) == []


@pytest.mark.asyncio
async def test_real_postgres_duplicate_scope_rows_fail_and_cleanup(monkeypatch):
    async with _dataset_database(monkeypatch) as (database, schema):
        resolved_fence = await importer._resolve_provider_directory_artifact_datasets(
            ["source_primary"]
        )
        dataset = resolved_fence.datasets[0]
        duplicate_fence = importer.ProviderDirectoryArtifactDatasetFence(
            (dataset, dataset)
        )

        with pytest.raises(IntegrityError, match="could not create unique index"):
            await importer._materialize_artifact_scope_tables(
                schema,
                "duplicate-run",
                duplicate_fence,
                frozenset({"Location"}),
            )

        assert await _scope_table_names(database, schema) == []
