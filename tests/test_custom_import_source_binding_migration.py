# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Schema-only contract checks for immutable custom-import source bindings."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest
from sqlalchemy import update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportSchemaRevision,
    CustomImportSourceBindingRevision,
)
from tests.custom_import_postgres_support import isolated_publication_case

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / "20260923030000_custom_import_source_binding.py"


def _migration():
    spec = importlib.util.spec_from_file_location("custom_import_source_binding_migration", MIGRATION_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _normalized(statements: list[str]) -> str:
    return "\n".join(" ".join(statement.split()) for statement in statements)


def test_source_binding_migration_is_immutable_and_preserves_legacy_execution_rows(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    sql = _normalized(statements)
    assert migration.down_revision == "20260922010000_custom_import_execution_request_identity"
    assert 'CREATE TABLE "custom_import_test"."custom_import_source_binding_revision"' in sql
    assert "custom-import/source-binding/v1" in sql
    assert "connector_kind = 'snowflake_bundle'" in sql
    assert "source_object_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'" in sql
    assert "binding_sha256 = pg_catalog.sha256" in sql
    assert "FOREIGN KEY ( source_binding_revision_id, dataset_id, definition_revision_id, schema_revision_id )" in sql
    assert "BEFORE UPDATE OR DELETE" in sql
    assert "BEFORE TRUNCATE" in sql
    assert "custom_import_execution_source_binding_revision_immutable" in sql
    assert sql.count("ENABLE ALWAYS TRIGGER") == 3
    assert "INSERT INTO" not in sql
    assert "DEFAULT" in sql


def test_source_binding_downgrade_refuses_retained_lineage(monkeypatch):
    migration = _migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "custom_import_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    statements: list[str] = []
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    sql = _normalized(statements)
    assert "custom_import_source_binding_downgrade_blocked" in sql
    assert "source_binding_revision_id IS NOT NULL" in sql
    assert sql.index("custom_import_source_binding_downgrade_blocked") < sql.index(
        "DROP COLUMN IF EXISTS source_binding_revision_id"
    )
    assert 'DROP TABLE IF EXISTS "custom_import_test"."custom_import_source_binding_revision"' in sql


async def _source_bound_execution_id(case) -> int:
    """Persist an execution that references an immutable source binding."""

    async with case.sessions() as session, session.begin():
        dataset = CustomImportDataset(dataset_key="synthetic_binding")
        session.add(dataset)
        await session.flush()
        schema = CustomImportSchemaRevision(
            dataset_id=dataset.dataset_id,
            revision_number=1,
            canonical_schema="{}",
            schema_sha256=hashlib.sha256(b"synthetic-schema").digest(),
        )
        session.add(schema)
        await session.flush()
        definition = CustomImportDefinitionRevision(
            dataset_id=dataset.dataset_id,
            schema_revision_id=schema.schema_revision_id,
            revision_number=1,
            contract_version="custom-import/v1",
            refresh_mode="snapshot",
            canonical_definition="{}",
            definition_sha256=hashlib.sha256(b"synthetic-definition").digest(),
        )
        session.add(definition)
        await session.flush()
        binding = CustomImportSourceBindingRevision(
            dataset_id=dataset.dataset_id,
            definition_revision_id=definition.definition_revision_id,
            schema_revision_id=schema.schema_revision_id,
            revision_number=1,
            binding_contract="custom-import/source-binding/v1",
            connector_kind="snowflake_bundle",
            definition_sha256=definition.definition_sha256,
            schema_sha256=schema.schema_sha256,
            source_object_fingerprint_sha256=hashlib.sha256(b"synthetic-source").digest(),
            source_object_version="v1",
            canonical_binding="{}",
            binding_sha256=hashlib.sha256(b"custom-import/source-binding/v1:{}").digest(),
        )
        session.add(binding)
        await session.flush()
        execution = CustomImportExecution(
            dataset_id=dataset.dataset_id,
            definition_revision_id=definition.definition_revision_id,
            schema_revision_id=schema.schema_revision_id,
            idempotency_key="synthetic-binding",
            mechanism="local",
            state="queued",
            request_identity_sha256=hashlib.sha256(b"synthetic-request").digest(),
            source_binding_revision_id=binding.source_binding_revision_id,
        )
        session.add(execution)
        await session.flush()
        return execution.execution_id


@pytest.mark.asyncio
async def test_source_binding_migration_persists_write_once_execution_lineage():
    async with isolated_publication_case() as case:
        execution_id = await _source_bound_execution_id(case)

        with pytest.raises(DBAPIError, match="custom_import_execution_source_binding_revision_immutable"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportExecution)
                    .where(CustomImportExecution.execution_id == execution_id)
                    .values(source_binding_revision_id=None)
                )
