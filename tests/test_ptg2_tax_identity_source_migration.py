# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for the dormant source-local PTG tax evidence migration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260806100000_ptg2_tax_identity_source.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_tax_identity_source_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _normalized(statement: str) -> str:
    return " ".join(statement.split())


def _capture_upgrade(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_tax_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return migration, statements, " ".join(map(_normalized, statements))


def _table_statement(statements: list[str], table_name: str) -> str:
    marker = f'CREATE TABLE "source_tax_test"."{table_name}"'
    matching_statements = [statement for statement in statements if marker in statement]
    assert len(matching_statements) == 1
    return _normalized(matching_statements[0])


def test_source_local_tax_schema_is_additive_and_physical_layout_scoped(
    monkeypatch,
):
    migration, statements, normalized_sql = _capture_upgrade(monkeypatch)

    assert migration.revision == "20260806100000_ptg2_tax_identity_source"
    assert migration.down_revision == "20260804100000_ptg2_raw_tin_vault_foundation"
    create_tables = [
        statement for statement in statements if "CREATE TABLE" in statement
    ]
    assert len(create_tables) == 3
    for table_name in (
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    ):
        assert f'CREATE TABLE "source_tax_test"."{table_name}"' in normalized_sql

    assert "ptg2_provider_group_tax_identity_source_v1" in normalized_sql
    assert "ptg2_tax_identity_rate_source_binding_v1" in normalized_sql
    assert "provider_group_occurrence_count = matched_ein_count" in normalized_sql
    assert "octet_length(content_digest) = 32" in normalized_sql
    assert "source_count > 0" in normalized_sql


def test_physical_binding_reuses_stable_identity_across_logical_wrappers(
    monkeypatch,
):
    _, statements, _ = _capture_upgrade(monkeypatch)
    binding_sql = _table_statement(
        statements,
        "ptg2_provider_tax_identity_source_binding",
    )

    assert (
        "UNIQUE ( snapshot_key, source_type, identity_kind, identity_sha256 )"
        in binding_sql
    )
    assert "source_type = 'in_network'" in binding_sql
    assert "'logical_json_sha256_v1'" in binding_sql
    assert "'raw_container_sha256_v1'" in binding_sql
    assert "identity_sha256 ~ '^[0-9a-f]{64}$'" in binding_sql
    assert "source_key >= 0" in binding_sql
    assert "token_policy_id varchar(55)" in binding_sql
    assert "token_policy_descriptor_sha256 bytea" in binding_sql
    assert "artifact_sha256 bytea" in binding_sql
    assert "artifact_byte_count = 13 + octet_length(token_policy_id)" in binding_sql
    assert "provider_group_count * record_bytes" in binding_sql
    assert "record_bytes = 65" in binding_sql

    # Logical plans and transport wrappers remain in ptg2_v3_snapshot_source;
    # they must not fragment the reusable physical-layout binding here.
    for unstable_column in (
        "snapshot_id varchar",
        "plan_id varchar",
        "source_trace_set_hash varchar",
        "raw_container_sha256 varchar",
        "logical_json_sha256 varchar",
        "logical_hash_deferred boolean",
    ):
        assert unstable_column not in binding_sql


def test_source_observation_preserves_exact_source_group_tin_witness(
    monkeypatch,
):
    _, statements, normalized_sql = _capture_upgrade(monkeypatch)
    observation_sql = _table_statement(
        statements,
        "ptg2_provider_group_tax_identity_source",
    )

    assert (
        "PRIMARY KEY ( snapshot_key, source_key, "
        "provider_group_global_id_128 )" in observation_sql
    )
    assert (
        "UNIQUE ( snapshot_key, source_key, source_record_ordinal )" in observation_sql
    )
    assert 'REFERENCES "source_tax_test"."ptg2_v3_provider_group"' in (observation_sql)
    assert 'REFERENCES "source_tax_test"."ptg2_provider_tax_identity"' in (
        observation_sql
    )
    assert "tax_identity_state = 'matched_ein' AND tin_key IS NOT NULL" in (
        observation_sql
    )
    assert "source_record_ordinal >= 0" in observation_sql
    assert "octet_length(provider_group_global_id_128) = 16" in observation_sql
    assert "ptg2_provider_group_tax_identity_source_tin_idx" in normalized_sql
    assert "ptg2_provider_group_tax_identity_source_group_idx" in normalized_sql
    assert (
        "snapshot_key, tin_key, source_key, provider_group_global_id_128"
        in normalized_sql
    )
    assert "WHERE tin_key IS NOT NULL" in normalized_sql
    assert "snapshot_key, provider_group_global_id_128, source_key" in normalized_sql


def test_empty_foundation_is_build_only_immutable_and_not_a_seal_gate(
    monkeypatch,
):
    _, statements, normalized_sql = _capture_upgrade(monkeypatch)

    insert_triggers = [
        statement
        for statement in statements
        if "AFTER INSERT" in statement and "REFERENCING NEW TABLE" in statement
    ]
    mutation_triggers = [
        statement for statement in statements if "BEFORE UPDATE OR DELETE" in statement
    ]
    truncate_triggers = [
        statement for statement in statements if "BEFORE TRUNCATE" in statement
    ]
    assert len(insert_triggers) == 3
    assert len(mutation_triggers) == 3
    assert len(truncate_triggers) == 3
    assert normalized_sql.count("ENABLE ALWAYS TRIGGER") == 9
    assert normalized_sql.count("FOR EACH STATEMENT EXECUTE FUNCTION") == 6
    assert "SELECT DISTINCT inserted.snapshot_key" in normalized_sql
    assert "FOR UPDATE OF candidate, candidate_layout" in normalized_sql
    assert "BEFORE INSERT" not in normalized_sql
    assert "root_state <> 'building'" in normalized_sql
    assert "layout_generation <> 'shared_blocks_v4'" in normalized_sql
    assert "layout_state <> 'building'" in normalized_sql
    assert "ptg2_provider_tax_identity_source_immutable" in normalized_sql
    assert "ptg2_provider_tax_identity_source_truncate_forbidden" in normalized_sql
    assert "ptg2_provider_tax_identity_source_policy_mismatch" in normalized_sql
    assert (
        "ptg2_provider_tax_identity_source_matched_witness_mismatch" in normalized_sql
    )
    assert "FROM new_rows AS inserted" in normalized_sql
    assert 'FROM "source_tax_test"."ptg2_provider_group_tax_identity"' in (
        normalized_sql
    )
    assert "merged.tin_key = inserted.tin_key" in normalized_sql
    assert normalized_sql.count("REVOKE ALL ON FUNCTION") == 3
    assert "FROM PUBLIC" in normalized_sql

    assert "legacy_layout" not in normalized_sql
    assert "current_generation" not in normalized_sql
    assert "BEFORE UPDATE OF state" not in normalized_sql
    assert "completion_guard" not in normalized_sql
    assert not any(
        _normalized(statement).startswith("INSERT INTO") for statement in statements
    )
    assert not any(
        _normalized(statement).startswith("UPDATE ") for statement in statements
    )


def test_source_local_tax_downgrade_requires_all_three_tables_empty(
    monkeypatch,
):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "source_tax_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    expected_tables = (
        "ptg2_provider_tax_identity_source_manifest",
        "ptg2_provider_tax_identity_source_binding",
        "ptg2_provider_group_tax_identity_source",
    )
    assert len(statements) == 10
    for index, table_name in enumerate(expected_tables):
        assert statements[index] == (
            f'LOCK TABLE "source_tax_test"."{table_name}" ' "IN ACCESS EXCLUSIVE MODE;"
        )
    empty_guard = _normalized(statements[3])
    assert empty_guard.count("EXISTS (SELECT 1 FROM") == 3
    assert "downgrade_requires_empty_foundation" in empty_guard
    for offset, table_name in enumerate(reversed(expected_tables), start=4):
        assert statements[offset] == (
            f'DROP TABLE IF EXISTS "source_tax_test"."{table_name}";'
        )
    assert "truncate" in statements[-3]
    assert "mutation" in statements[-2]
    assert statements[-1].endswith(
        '"guard_ptg2_provider_tax_identity_source_insert"();'
    )


def test_source_local_tax_schema_alias_conflict_fails_closed(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="DB_SCHEMA and HLTHPRT_DB_SCHEMA"):
        migration.upgrade()
