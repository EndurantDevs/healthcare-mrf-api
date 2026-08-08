# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static contracts for the publication-disabled public-evidence catalog."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from public_evidence.evidence_record_token_policy import TOKEN_POLICY_PROFILES
from public_evidence.source_release_policies import SOURCE_POLICIES


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / ("20260808090000_public_evidence_storage_foundation.py")
)
TABLE_NAMES = (
    "public_evidence_source_identity",
    "public_evidence_source_release",
    "public_evidence_token_policy",
    "public_evidence_tax_identity",
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_storage_foundation_migration",
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


def test_public_evidence_storage_is_current_dormant_child(monkeypatch) -> None:
    migration, statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert migration.revision == ("20260808090000_public_evidence_storage_foundation")
    assert migration.down_revision == (
        "20260807120000_ptg_import_wave_recovery_storage"
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
    assert "publication_enabled boolean" not in normalized_sql
    assert '"publication_enabled":' in normalized_sql
    assert "|| 'false'" in normalized_sql
    assert "current_pointer_authority = 'none'" in normalized_sql
    assert "lifecycle_state = 'verified_disabled'" in normalized_sql


def test_catalog_is_exact_opaque_immutable_shape(monkeypatch) -> None:
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert "^peid1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "^perel1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "^perun1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "^petax1_[A-Za-z0-9_-]{43}$" in normalized_sql
    assert "locator_128 = substring(full_hmac_sha256 FROM 1 FOR 16)" in (normalized_sql)
    assert (
        "UNIQUE ( token_policy_contract_id, token_policy_id, " "full_hmac_sha256 )"
    ) in normalized_sql
    assert "public_evidence_tax_identity_locator_idx" in normalized_sql
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 8
    assert normalized_sql.count("REVOKE ALL ON TABLE") == 4
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 5
    assert "public_evidence_catalog_mutation_forbidden" in normalized_sql
    assert "ON DELETE RESTRICT" in normalized_sql
    assert "FOREIGN KEY" in normalized_sql

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


def test_migration_policy_copy_matches_frozen_python_contract() -> None:
    migration = _load_migration()

    assert set(migration._SOURCE_POLICIES) == set(SOURCE_POLICIES)
    for source_kind, source_policy in SOURCE_POLICIES.items():
        migration_policy = migration._SOURCE_POLICIES[source_kind]
        assert migration_policy == {
            "identity_kind": source_policy.identity_kind,
            "content_identity_kinds": source_policy.content_identity_kinds,
            "authority": source_policy.authority,
            "trust": source_policy.trust,
            "rights": source_policy.rights,
            "mode": source_policy.attestation_mode,
            "evidence_contract": source_policy.evidence_contract_id,
            "count_unit": source_policy.count_unit,
            "semantic_limits": source_policy.semantic_limits,
            "binding_required": source_policy.source_binding_required,
        }

    assert set(TOKEN_POLICY_PROFILES) == {
        "ptg_v4_ein_tax_identity_policy_v1",
        "healthporta_ein_npi_tax_identity_policy_v1",
    }


def test_downgrade_locks_and_requires_all_roots_empty(monkeypatch) -> None:
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
    assert (
        "public_evidence_downgrade_requires_empty_foundation"
        in normalized_statements[1]
    )
    assert normalized_statements[1].count("EXISTS (SELECT 1 FROM") == len(TABLE_NAMES)
    dropped_tables = [
        statement
        for statement in normalized_statements
        if statement.startswith("DROP TABLE")
    ]
    assert [
        table_name
        for table_name in TABLE_NAMES
        if any(table_name in statement for statement in dropped_tables)
    ] == list(TABLE_NAMES)
    assert len(dropped_tables) == len(TABLE_NAMES)


def test_schema_alias_conflict_fails_closed(monkeypatch) -> None:
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()


def test_foundation_has_no_runtime_or_existing_storage_wiring() -> None:
    migration_source = MIGRATION_PATH.read_text()

    assert "process/" not in migration_source
    assert "api/" not in migration_source
    assert "support/ptg2_scanner" not in migration_source
    assert "ALTER TABLE ptg2" not in migration_source
    assert "ALTER TABLE tin_npi_connector" not in migration_source
    assert "ALTER TABLE entity_address" not in migration_source
    assert "NPPES import" not in migration_source
