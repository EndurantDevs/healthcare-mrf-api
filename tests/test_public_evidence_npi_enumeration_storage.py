# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static contracts for dormant normalized NPI-enumeration storage."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.public_evidence_npi_enumeration_postgres_support import (
    MIGRATION_PATH,
    TABLE_NAMES,
    load_migration,
)


ROOT = Path(__file__).resolve().parents[1]


def _upgrade_sql(monkeypatch) -> tuple[object, list[str], str]:
    migration = load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "public_evidence_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    normalized = " ".join(" ".join(statements).split())
    return migration, statements, normalized


def test_npi_storage_is_one_empty_linear_slice(monkeypatch) -> None:
    migration, statements, normalized = _upgrade_sql(monkeypatch)

    assert migration.revision == (
        "20260808170000_public_evidence_npi_enumeration_storage"
    )
    assert migration.down_revision == "20260808160000_fhir_formulary_serving_index"
    assert normalized.count("CREATE TABLE") == 3
    assert all(
        f'CREATE TABLE "public_evidence_test"."{table_name}"' in normalized
        for table_name in TABLE_NAMES
    )
    assert not any(
        statement.lstrip().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    assert all(token not in normalized for token in ("CREATE VIEW", "CREATE POLICY"))
    assert "GRANT " not in normalized


def test_rows_bind_exact_owners_and_keep_history_nonunique(monkeypatch) -> None:
    _migration, _statements, normalized = _upgrade_sql(monkeypatch)

    assert "public_evidence_record_owner_key" in normalized
    assert "source_release_ref, contract_sha256, source_kind" in normalized
    assert "source_record_ref, source_release_ref" in normalized
    assert "DEFERRABLE INITIALLY DEFERRED" in normalized
    assert "UNIQUE (evidence_ref, source_record_ref)" in normalized
    assert "UNIQUE (npi" not in normalized
    assert "UNIQUE (source_record_ref)" not in normalized
    assert "ON DELETE CASCADE" not in normalized


def test_exact_contract_digests_and_full_record_reference_are_enforced(
    monkeypatch,
) -> None:
    _migration, _statements, normalized = _upgrade_sql(monkeypatch)

    required_literals = (
        "HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_DIGEST_V1",
        "HEALTHPORTA_PUBLIC_EVIDENCE_RECORD_REFERENCE_V1",
        "persistence_candidate_typed_row",
        "persistence_candidate_source_link_row",
        "persistence_candidate_source_link_vector",
        "persistence_candidate_record_authority_state",
        "persistence_candidate_common_row",
        "evidence_record_contract",
        "'peev1_', 'evidence_record', record_json",
        "convert_to(item.source_record_ref, 'UTF8')",
        "YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"",
    )
    assert all(literal in normalized for literal in required_literals)
    assert "jsonb" not in normalized.lower()
    assert "candidate_ref" not in normalized
    assert "prospective_row_shape_only" not in normalized


def test_closed_npi_semantics_and_non_authority_are_literal(monkeypatch) -> None:
    _migration, _statements, normalized = _upgrade_sql(monkeypatch)

    required_literals = (
        "healthporta.public-evidence-record.v1",
        "phase_1_public_source_neutral_foundation",
        "nppes_entity_address",
        "nppes_registry_record",
        "npi_enumeration",
        "nppes_npi_enumeration",
        "individual_type_1",
        "organization_type_2",
        "active",
        "deactivated",
        "normalized_record_only",
        "positive_evidence_only",
        "publication_enabled",
        "serving_authority",
        "current_pointer_authority",
        "database_io_authority",
    )
    assert all(literal in normalized for literal in required_literals)
    assert "public_evidence_npi_valid" in normalized
    assert "BETWEEN 1000000000 AND 2999999999" in normalized
    assert "legal_ownership" in normalized
    assert "raw_tin" not in normalized.lower()
    assert "ciphertext" not in normalized.lower()


def test_deferred_private_always_enabled_guards_are_on_every_table(
    monkeypatch,
) -> None:
    _migration, _statements, normalized = _upgrade_sql(monkeypatch)

    assert normalized.count("CREATE CONSTRAINT TRIGGER") == 3
    assert normalized.count("DEFERRABLE INITIALLY DEFERRED") == 6
    assert normalized.count("ENABLE ALWAYS TRIGGER") == 9
    assert normalized.count("BEFORE UPDATE OR DELETE") == 3
    assert normalized.count("BEFORE TRUNCATE") == 3
    assert normalized.count("REVOKE ALL ON TABLE") == 3
    assert normalized.count("REVOKE ALL ON FUNCTION") == 4
    assert "SECURITY DEFINER SET search_path = pg_catalog" in normalized
    assert "pg_advisory_xact_lock" in normalized
    assert "public_evidence_npi_record_invalid" in normalized


def test_downgrade_is_empty_only_locked_and_parent_preserving(monkeypatch) -> None:
    migration = load_migration()
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
    assert "public_evidence_source_record" in normalized_statements[0]
    assert normalized_statements[1].count("EXISTS (SELECT 1 FROM") == 3
    assert (
        "public_evidence_downgrade_requires_empty_npi_records"
        in normalized_statements[1]
    )
    assert sum(
        statement.startswith("DROP TABLE") for statement in normalized_statements
    ) == 3
    assert not any(
        "DROP TABLE" in statement and "source_release" in statement
        for statement in normalized_statements
    )
    assert not any(
        "DROP TABLE" in statement and "source_record\"" in statement
        for statement in normalized_statements
    )


def test_schema_alias_conflict_and_runtime_wiring_fail_closed(monkeypatch) -> None:
    migration = load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")
    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration.upgrade()

    migration_source = MIGRATION_PATH.read_text()
    assert all(
        forbidden not in migration_source
        for forbidden in (
            "api/",
            "process/",
            "support/ptg2_scanner",
            "INSERT INTO",
            "current_pointer =",
            "publication_admissible",
            "NPPES import",
        )
    )
