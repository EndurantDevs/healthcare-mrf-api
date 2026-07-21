# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260721150000_ptg2_source_witness_parts.py"
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "ptg2_source_witness_parts_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def test_source_witness_parts_migration_is_bounded_and_cascade_owned(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_shared")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    assert migration.down_revision == (
        "20260721100000_provider_directory_profile_selection_attestation"
    )
    assert len(statements) == 1
    statement = " ".join(statements[0].split())
    assert 'CREATE TABLE "ptg_shared"."ptg2_v3_source_audit_witness_part"' in statement
    assert "PRIMARY KEY (snapshot_key, part_number)" in statement
    assert "FOREIGN KEY (snapshot_key)" in statement
    assert 'REFERENCES "ptg_shared"."ptg2_v3_source_audit_witness"' in statement
    assert "ON DELETE CASCADE" in statement
    assert "part_number BETWEEN 1 AND 7" in statement
    assert "octet_length(part_sha256) = 32" in statement
    assert "octet_length(payload) BETWEEN 1 AND 67108864" in statement


def test_source_witness_parts_migration_downgrade_drops_only_parts(monkeypatch):
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_shared")
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.downgrade()

    assert statements == [
        'DROP TABLE IF EXISTS "ptg_shared".'
        '"ptg2_v3_source_audit_witness_part";'
    ]
