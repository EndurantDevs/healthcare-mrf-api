# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Schema and SQL contracts for subset completion evidence."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from db.models.system import (
    ProviderDirectoryDatasetResource,
    ProviderDirectoryEndpointDataset,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260808190000_provider_directory_subset_completion_proof.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_subset_completion_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


class _Recorder:
    def __init__(self):
        self.statements = []
        self.added_columns = []
        self.checks = []
        self.dropped_constraints = []
        self.dropped_columns = []

    def execute(self, statement):
        self.statements.append(str(statement))

    def add_column(self, table_name, column, **kwargs):
        self.added_columns.append((table_name, column, kwargs))

    def create_check_constraint(
        self,
        name,
        table_name,
        condition,
        **kwargs,
    ):
        self.checks.append((name, table_name, condition, kwargs))

    def drop_constraint(self, name, table_name, **kwargs):
        self.dropped_constraints.append((name, table_name, kwargs))

    def drop_column(self, table_name, column_name, **kwargs):
        self.dropped_columns.append((table_name, column_name, kwargs))


def _capture(monkeypatch, action):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "subset_proof_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)
    getattr(migration, action)()
    normalized_sql = " ".join(
        " ".join(statement.split()) for statement in recorder.statements
    )
    return migration, recorder, normalized_sql


def test_upgrade_adds_model_columns_and_hardened_canonical_functions(monkeypatch):
    migration, recorder, normalized_sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260808190000_provider_directory_subset_completion_proof"
    )
    assert migration.down_revision == (
        "20260808180000_ptg_import_wave_materialized_preclaim"
    )
    added_by_table = {}
    for table_name, column, _kwargs in recorder.added_columns:
        added_by_table.setdefault(table_name, set()).add(column.name)
    assert added_by_table == {
        ProviderDirectoryEndpointDataset.__tablename__: {
            "completion_proof_required_version",
            "completion_proof_json",
            "completion_proof_sha256",
        },
        ProviderDirectoryDatasetResource.__tablename__: {
            "acquired_resource_sha256",
        },
    }
    assert {check[0] for check in recorder.checks} == {
        *migration._PARENT_CHECKS,
        migration._CHILD_DIGEST_CHECK,
    }
    assert "provider_directory_subset_canonical_sha256" in normalized_sql
    assert "provider_directory_subset_completion_proof_pair_valid" in (
        normalized_sql
    )
    assert "server_issued_subset_replay_evidence_sha256" in normalized_sql
    assert "provider_directory_subset_replay_evidence_invalid" in normalized_sql
    assert "provider_directory_subset_acquired_digest_marker_invalid" in (
        normalized_sql
    )
    assert "pg_catalog.format_type" in normalized_sql
    assert "'completion_proof_json', 'jsonb'" in normalized_sql
    assert normalized_sql.count("observed_columns IS DISTINCT FROM") == 12
    assert "verification_baseline" in normalized_sql
    assert "verification_mismatch" in normalized_sql
    assert migration._SOURCE_GUARD in normalized_sql
    assert migration._SOURCE_GUARD_TRIGGER in normalized_sql
    assert "AFTER INSERT OR UPDATE OR DELETE" in normalized_sql
    assert "DEFERRABLE INITIALLY DEFERRED" in normalized_sql
    assert migration._SOURCE_TRUNCATE_GUARD_TRIGGER in normalized_sql
    assert "BEFORE TRUNCATE" in normalized_sql
    assert "IN SHARE MODE" in normalized_sql
    assert (
        "provider_directory_subset_published_source_mutation_invalid"
        in normalized_sql
    )
    assert (
        "provider_directory_subset_source_isolation_invalid"
        in normalized_sql
    )
    assert "REVOKE ALL ON FUNCTION" in normalized_sql
    assert "FROM PUBLIC" in normalized_sql


def test_source_guard_ddl_uses_one_asyncpg_command_per_execute(monkeypatch):
    migration = _load_migration()
    recorder = _Recorder()
    monkeypatch.setattr(migration, "op", recorder)

    migration._create_subset_published_source_guard("subset_proof_test")

    ddl_statements = recorder.statements[1:]
    assert len(ddl_statements) == 5
    assert [" ".join(statement.split()).split(" ", 3)[:3] for statement in ddl_statements] == [
        ["CREATE", "CONSTRAINT", "TRIGGER"],
        ["CREATE", "TRIGGER", '"provider_directory_subset_published_source_truncate_guard"'],
        ["ALTER", "TABLE", '"subset_proof_test"."provider_directory_source"'],
        ["ALTER", "TABLE", '"subset_proof_test"."provider_directory_source"'],
        ["REVOKE", "ALL", "ON"],
    ]
    assert all(statement.strip().count(";") == 1 for statement in ddl_statements)


def test_downgrade_is_fail_closed_and_restores_legacy_guards(monkeypatch):
    migration, recorder, normalized_sql = _capture(monkeypatch, "downgrade")

    assert "provider_directory_subset_completion_downgrade_blocked" in (
        normalized_sql
    )
    assert "provider_directory_subset_completion_proof_pair_valid" in (
        normalized_sql
    )
    assert "DROP FUNCTION" in normalized_sql
    assert "DROP TRIGGER" in normalized_sql
    assert migration._SOURCE_GUARD_TRIGGER in normalized_sql
    assert migration._SOURCE_TRUNCATE_GUARD_TRIGGER in normalized_sql
    assert "completion_proof_required_version" in normalized_sql
    assert [column_name for _table, column_name, _kwargs in recorder.dropped_columns] == [
        "acquired_resource_sha256",
        "completion_proof_sha256",
        "completion_proof_json",
        "completion_proof_required_version",
    ]
    assert len(recorder.dropped_constraints) == len(migration._PARENT_CHECKS) + 1
