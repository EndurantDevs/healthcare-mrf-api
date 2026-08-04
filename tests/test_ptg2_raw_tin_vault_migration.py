# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260804100000_ptg2_raw_tin_vault_foundation.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_raw_tin_vault_foundation_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _capture_upgrade(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_vault_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return migration, statements, " ".join(" ".join(statements).split())


def test_raw_tin_vault_migration_is_empty_ciphertext_only_foundation(monkeypatch):
    migration, statements, normalized_sql = _capture_upgrade(monkeypatch)

    assert migration.down_revision == "20260731190000_tiger_zcta5_zip_index"
    assert len(statements) == 10
    assert 'CREATE TABLE "tin_vault_test"."ptg2_raw_tin_vault_entry"' in normalized_sql
    assert "PRIMARY KEY (token_policy_id, tin_hmac_sha256)" in normalized_sql
    assert "octet_length(tin_hmac_sha256) = 32" in normalized_sql
    assert "tin_type = 'ein'" in normalized_sql
    assert "fernet_hmac_sha256_bound_v1" in normalized_sql
    assert "token_policy_full_hmac_ein_v1" in normalized_sql
    assert "split_part(ciphertext, ':', 2) = encryption_key_id" in normalized_sql
    assert "ptg2_raw_tin_vault_encryption_key_idx" in normalized_sql
    assert "ENABLE ALWAYS TRIGGER" in normalized_sql
    assert "ptg2_raw_tin_vault_identity_immutable" in normalized_sql
    assert "ptg2_raw_tin_vault_delete_forbidden" in normalized_sql
    assert "ptg2_raw_tin_vault_truncate_forbidden" in normalized_sql
    assert "REVOKE ALL ON TABLE" in normalized_sql
    assert "FROM PUBLIC" in normalized_sql
    assert " INSERT " not in f" {normalized_sql} "
    assert " plaintext" not in normalized_sql.lower()
    assert "display_tin" not in normalized_sql.lower()
    assert "raw_tin_value" not in normalized_sql.lower()


def test_raw_tin_vault_downgrade_refuses_data_before_drop(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "tin_vault_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    assert statements[0] == (
        'LOCK TABLE "tin_vault_test"."ptg2_raw_tin_vault_entry" '
        "IN ACCESS EXCLUSIVE MODE;"
    )
    assert "downgrade_requires_empty_foundation" in statements[1]
    assert "SELECT 1" in statements[1]
    assert statements[-2] == (
        'DROP TABLE IF EXISTS "tin_vault_test"."ptg2_raw_tin_vault_entry";'
    )
    assert statements[-1] == (
        'DROP FUNCTION IF EXISTS "tin_vault_test".'
        '"guard_ptg2_raw_tin_vault_entry"();'
    )


def test_raw_tin_vault_schema_alias_conflict_fails_closed(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="DB_SCHEMA and HLTHPRT_DB_SCHEMA"):
        migration.upgrade()


def test_raw_tin_vault_has_no_runtime_or_api_wiring():
    forbidden_paths = [
        ROOT / "main.py",
        ROOT / "process" / "ptg.py",
        ROOT / "process" / "ptg_parts" / "ptg2_v4_graph_compiler.py",
        *(ROOT / "api").glob("*.py"),
    ]
    module_name = "ptg2_raw_tin_vault_crypto"

    assert all(module_name not in path.read_text() for path in forbidden_paths)
