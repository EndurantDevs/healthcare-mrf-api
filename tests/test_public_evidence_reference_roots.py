# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static contracts for immutable public-evidence reference roots."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from public_evidence.evidence_record_policies import _SOURCE_ENTITY_KIND
from public_evidence.source_record_inclusion_primitives import (
    SOURCE_RECORD_KINDS_BY_SOURCE,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260808100000_public_evidence_reference_roots.py"
)
TABLE_NAMES = (
    "public_evidence_source_record",
    "public_evidence_provider_group",
    "public_evidence_source_entity",
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_reference_roots_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _upgrade_statements(monkeypatch) -> tuple[object, list[str], str]:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "public_evidence_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    normalized = " ".join(" ".join(statements).split())
    return migration, statements, normalized


def test_reference_roots_are_the_exact_empty_serial_child(monkeypatch) -> None:
    migration, statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert migration.revision == "20260808100000_public_evidence_reference_roots"
    assert migration.down_revision == (
        "20260808090000_public_evidence_storage_foundation"
    )
    assert [
        table_name
        for table_name in TABLE_NAMES
        if f'CREATE TABLE "public_evidence_test"."{table_name}"' in normalized_sql
    ] == list(TABLE_NAMES)
    assert normalized_sql.count("CREATE TABLE") == len(TABLE_NAMES)
    assert "INSERT INTO" not in normalized_sql
    assert not any(
        statement.lstrip().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    assert "CREATE VIEW" not in normalized_sql
    assert "CREATE MATERIALIZED VIEW" not in normalized_sql
    assert "CREATE POLICY" not in normalized_sql


def test_reference_roots_copy_the_closed_usable_source_matrices() -> None:
    migration = _load_migration()

    assert {
        source_kind: frozenset(record_kinds)
        for source_kind, record_kinds in migration._SOURCE_RECORD_KINDS_BY_SOURCE.items()
    } == dict(SOURCE_RECORD_KINDS_BY_SOURCE)
    assert migration._SOURCE_ENTITY_KIND_BY_SOURCE == dict(_SOURCE_ENTITY_KIND)


def test_reference_roots_bind_parent_ownership_and_exact_refs(monkeypatch) -> None:
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert "UNIQUE (source_release_ref, contract_sha256, source_kind)" in normalized_sql
    assert normalized_sql.count("source_release_contract_sha256 bytea NOT NULL") == 3
    assert (
        normalized_sql.count(
            "FOREIGN KEY ( source_release_ref, source_release_contract_sha256, "
            "source_kind )"
        )
        == 3
    )
    assert (
        normalized_sql.count(
            'REFERENCES "public_evidence_test".'
            '"public_evidence_source_release" ( source_release_ref, '
            "contract_sha256, source_kind )"
        )
        == 3
    )
    assert normalized_sql.count("ON DELETE RESTRICT") == 3
    assert "^pesr1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "^pegrp1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "^peent1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert normalized_sql.count("identity_contract_id text NOT NULL") == 3
    assert "identity_contract_id varchar" not in normalized_sql
    assert normalized_sql.count("IS TRUE") == 3


def test_reference_helpers_and_tables_are_private_and_immutable(monkeypatch) -> None:
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert normalized_sql.count("CREATE FUNCTION") == 3
    assert normalized_sql.count("LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE") == 3
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 6
    assert normalized_sql.count("REVOKE ALL ON TABLE") == 3
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 3
    assert "public_evidence_catalog_mutation_forbidden" not in normalized_sql
    assert "guard_public_evidence_immutable_catalog" in normalized_sql

    forbidden_identity_terms = (
        "raw_tin",
        "masked_tin",
        "plaintext_tin",
        "display_tin",
        "raw_npi",
        "masked_npi",
        "ciphertext",
    )
    lowered_sql = normalized_sql.lower()
    assert all(term not in lowered_sql for term in forbidden_identity_terms)


def test_reference_root_downgrade_is_empty_only_and_scoped(monkeypatch) -> None:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "public_evidence_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    normalized_statements = [" ".join(statement.split()) for statement in statements]
    assert normalized_statements[0].startswith("LOCK TABLE ")
    assert normalized_statements[0].endswith(" IN ACCESS EXCLUSIVE MODE;")
    assert all(table_name in normalized_statements[0] for table_name in TABLE_NAMES)
    assert "public_evidence_source_release" in normalized_statements[0]
    assert (
        "public_evidence_downgrade_requires_empty_reference_roots"
        in normalized_statements[1]
    )
    assert normalized_statements[1].count("EXISTS (SELECT 1 FROM") == len(TABLE_NAMES)
    dropped_tables = [
        statement
        for statement in normalized_statements
        if statement.startswith("DROP TABLE")
    ]
    assert len(dropped_tables) == len(TABLE_NAMES)
    assert all(
        any(table_name in statement for statement in dropped_tables)
        for table_name in TABLE_NAMES
    )
    assert not any(
        'DROP TABLE "public_evidence_test"."public_evidence_source_release"'
        in statement
        for statement in normalized_statements
    )
    assert normalized_statements[-1].endswith(
        'DROP CONSTRAINT "public_evidence_source_release_kind_owner_key";'
    )


def test_reference_roots_have_no_runtime_or_existing_storage_wiring() -> None:
    migration_source = MIGRATION_PATH.read_text()

    assert "process/" not in migration_source
    assert "api/" not in migration_source
    assert "support/ptg2_scanner" not in migration_source
    assert "INSERT INTO" not in migration_source
    assert "ALTER TABLE ptg2" not in migration_source
    assert "ALTER TABLE tin_npi_connector" not in migration_source
    assert "ALTER TABLE entity_address" not in migration_source
    assert "NPPES import" not in migration_source


def test_schema_alias_conflict_fails_closed(monkeypatch) -> None:
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()
