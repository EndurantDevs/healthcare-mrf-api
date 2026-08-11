# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts for persisted Provider Directory premise grouping keys."""

from __future__ import annotations

import contextlib
import importlib.util
import importlib
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

directory = importlib.import_module("process.provider_directory_fhir")


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic/versions/20260811130000_address_premise_grouping.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "address_premise_grouping_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _OperationsRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement) -> None:
        self.statements.append(str(statement))

    def get_context(self):
        return self

    @contextlib.contextmanager
    def autocommit_block(self):
        yield


def test_premise_migration_is_the_reviewed_subset_successor(monkeypatch):
    migration = _load_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "premise_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.upgrade()

    assert migration.revision == "20260811130000_address_premise_grouping"
    assert migration.down_revision == (
        "20260811120000_provider_directory_reviewed_subset_v5_http410_disposition"
    )
    assert len(recorder.statements) == 2
    upgrade_sql = "\n".join(recorder.statements)
    assert recorder.statements[0] == "SET LOCAL lock_timeout = '5s';"
    assert 'ALTER TABLE IF EXISTS "premise_test"."provider_directory_address_overlay"' in upgrade_sql
    assert "ADD COLUMN IF NOT EXISTS premise_key uuid" in upgrade_sql
    assert "CREATE INDEX" not in upgrade_sql


def test_premise_migration_downgrade_drops_index_concurrently_before_column(monkeypatch):
    migration = _load_migration()
    recorder = _OperationsRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "premise_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)

    migration.downgrade()

    assert len(recorder.statements) == 3
    assert "DROP INDEX CONCURRENTLY IF EXISTS" in recorder.statements[0]
    assert recorder.statements[1] == "SET LOCAL lock_timeout = '5s';"
    assert "DROP COLUMN IF EXISTS premise_key" in recorder.statements[2]


def test_premise_migration_rejects_conflicting_schema_names(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")

    with pytest.raises(RuntimeError, match="must match"):
        migration._schema()


def test_overlay_schema_separates_archive_premise_from_source_columns():
    storage_columns = set(directory._provider_directory_address_overlay_columns())
    source_columns = set(directory._provider_directory_address_overlay_source_columns())
    table_sql = directory.provider_directory_address_overlay_table_sql("mrf")
    compatibility_sql = directory.address_overlay_premise_key_column_sql("mrf")

    assert "premise_key" in storage_columns
    assert "premise_key" not in source_columns
    assert "premise_key uuid" in table_sql
    assert "ADD COLUMN IF NOT EXISTS premise_key uuid" in compatibility_sql


@pytest.mark.asyncio
async def test_overlay_stage_indexes_include_partial_npi_premise_lookup(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(directory.db, "status", status)
    stage_table = "provider_directory_address_overlay_stage_test"

    await directory._create_address_overlay_stage_indexes("mrf", stage_table)

    joined_sql = "\n".join(call.args[0] for call in status.await_args_list)
    premise_index = directory._address_overlay_index_name(
        stage_table, "npi_premise_key_idx"
    )
    assert premise_index in joined_sql
    assert "(npi, premise_key)" in joined_sql
    assert "WHERE premise_key IS NOT NULL" in joined_sql


@pytest.mark.asyncio
async def test_overlay_stage_hydrates_exact_unmerged_archive_premise(monkeypatch):
    monkeypatch.setattr(directory, "_is_table_present", AsyncMock(return_value=True))
    status = AsyncMock(return_value="UPDATE 2")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_premise_keys(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 2
    hydrate_sql = status.await_args.args[0]
    assert 'LEFT JOIN "mrf"."address_archive_v2" AS archive' in hydrate_sql
    assert "archive.premise_key" in hydrate_sql
    assert "stage_source.ctid AS stage_row_id" in hydrate_sql
    assert "archive.address_key = stage_source.address_key" in hydrate_sql
    assert "archive.merged_into IS NULL" in hydrate_sql
    assert "stage_row.premise_key IS DISTINCT FROM desired_premise.premise_key" in hydrate_sql
    assert "desired_premise.stage_row_id = stage_row.ctid" in hydrate_sql


@pytest.mark.asyncio
async def test_overlay_stage_clears_premise_keys_without_archive(monkeypatch):
    monkeypatch.setattr(directory, "_is_table_present", AsyncMock(return_value=False))
    status = AsyncMock(return_value="UPDATE 2")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_premise_keys(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 2
    assert "SET premise_key = NULL" in status.await_args.args[0]
    assert "WHERE premise_key IS NOT NULL" in status.await_args.args[0]


@pytest.mark.asyncio
async def test_alias_rewrite_carries_target_archive_premise(monkeypatch):
    status = AsyncMock(return_value="UPDATE 1")
    scalar = AsyncMock(return_value=0)
    monkeypatch.setattr(directory.db, "status", status)
    monkeypatch.setattr(directory.db, "scalar", scalar)

    rewritten, residual = await directory._rewrite_address_overlay_alias_rows(
        '"mrf"."overlay_stage"',
        '"mrf"."address_alias_v1"',
        '"mrf"."address_archive_v2"',
    )

    assert (rewritten, residual) == (1, 0)
    rewrite_sql = status.await_args.args[0]
    assert "address_key = target.address_key" in rewrite_sql
    assert "premise_key = target.premise_key" in rewrite_sql
    assert "target.merged_into IS NULL" in rewrite_sql
