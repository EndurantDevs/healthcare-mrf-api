# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration and PostgreSQL proofs for bounded endpoint-dataset receipts."""

from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import re
import uuid

import pytest

from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)


asyncpg = pytest.importorskip("asyncpg")

ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260812010000_provider_directory_endpoint_dataset_admission_seal.py"
)
PROOF_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260808190000_provider_directory_subset_completion_proof.py"
)
POSTGRES_DSN_ENV = "HLTHPRT_TIN_NPI_CONNECTOR_POSTGRES_DSN"
TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


class _SqlCapture:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _load(path: Path, name: str):
    module_spec = importlib.util.spec_from_file_location(name, path)
    assert module_spec is not None and module_spec.loader is not None
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    return module


def _capture(monkeypatch, action: str):
    migration = _load(MIGRATION_PATH, f"admission_seal_{action}_migration")
    capture = _SqlCapture()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "admission_seal_contract")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    migration.op = capture
    getattr(migration, action)()
    normalized = " ".join(" ".join(sql.split()) for sql in capture.statements)
    return migration, capture.statements, normalized


def test_upgrade_is_nullable_bounded_and_application_trusted(monkeypatch) -> None:
    migration, statements, sql = _capture(monkeypatch, "upgrade")
    _assert_upgrade_identity(migration, statements)
    _assert_exact_column_adoption(migration, statements, sql)
    _assert_upgrade_guards(migration, sql)


def _assert_upgrade_identity(migration, statements: list[str]) -> None:
    assert migration.revision == (
        "20260812010000_provider_directory_endpoint_dataset_admission_seal"
    )
    assert migration.down_revision == (
        "20260811140000_ptg_v12_provider_publication_merge"
    )
    assert len(statements) == 43


def _assert_exact_column_adoption(
    migration,
    statements: list[str],
    sql: str,
) -> None:
    column_type_by_name = {
        "publication_metadata_summary_json": "jsonb",
        "publication_metadata_sha256": "varchar(64)",
        "content_proof_admission_version": "smallint",
        "content_proof_admission_kind": "varchar(32)",
        "content_proof_admission_sha256": "varchar(64)",
        "content_proof_resource_types": "varchar(64)[]",
    }
    for column_name, column_type in column_type_by_name.items():
        assert f"ADD COLUMN IF NOT EXISTS {column_name} {column_type}" in sql
    add_columns_statement = next(
        statement for statement in statements
        if "ADD COLUMN IF NOT EXISTS publication_metadata_summary_json" in statement
    )
    assert "NOT NULL" not in add_columns_statement
    adoption_fence = " ".join(
        migration._column_adoption_fence_sql("admission_seal_contract").split()
    )
    assert "present_columns NOT IN (0, 6)" in adoption_fence
    assert "matching_columns <> present_columns" in adoption_fence
    assert "attribute.attnotnull IS FALSE" in adoption_fence
    assert "attribute.attgenerated = ''" in adoption_fence
    assert "default_value.adbin IS NULL" in adoption_fence
    assert "adopted_columns_populated" in adoption_fence
    assert "content_proof_admission_version IS NOT NULL" in adoption_fence
    assert (
        "provider_directory_endpoint_dataset_admission_columns_populated"
        in adoption_fence
    )
    assert all(type_name in adoption_fence for type_name in (
        "jsonb", "character varying(64)", "smallint",
        "character varying(32)", "character varying(64)[]",
    ))
    assert "provider_directory_endpoint_dataset_admission_columns_changed" in sql


def _assert_upgrade_guards(migration, sql: str) -> None:

    assert (
        "CREATE INDEX \"pd_endpoint_dataset_admission_source_ids_idx\""
        in sql
    )
    assert "USING gin ((publication_metadata_summary_json -> 'source_ids'))" in sql
    assert (
        "WHERE status = 'validated' AND is_current = false "
        "AND superseded_at IS NULL"
    ) in sql
    assert "provider_directory_subset_payload_sha256" in sql
    assert "ptg_wave_canonical_json_ascii_v1" not in sql
    assert "provider-directory-admission-seal-v1" in sql
    assert all(
        f"'{field_name}'" in sql
        for field_name in (
            "contract",
            "metadata_summary",
            "admission_version",
            "admission_kind",
            "proof_sha256",
            "resource_types",
        )
    )
    assert (
        "metadata_summary jsonb, admission_version smallint, "
        "admission_kind text, proof_sha256 text, resource_types varchar[] "
        ") RETURNS varchar"
    ) in sql
    assert sql.count("SECURITY DEFINER SET search_path = pg_catalog") == 4
    assert sql.count("REVOKE ALL ON FUNCTION") == 4
    assert sql.count("ENABLE ALWAYS TRIGGER") == 10
    assert "BEFORE INSERT OR UPDATE OF" in sql
    assert "pd_endpoint_dataset_subset_replay_evidence_check" in sql
    assert "pd_endpoint_dataset_subset_replay_evidence_guard" in sql
    for trigger_name in (
        "tin_npi_connector_endpoint_dataset_guard",
        "provider_directory_reviewed_subset_activation_dataset_guard",
        "pd_subset_abandonment_dataset_guard",
        "pd_subset_abandonment_dataset_consistency_guard",
        "pd_subset_terminal_disposition_dataset_consistency_guard",
        "pd_trr_dataset_row",
    ):
        assert f'DROP TRIGGER "{trigger_name}"' in sql
    assert "BEFORE TRUNCATE" in sql
    assert "publication_metadata_json::jsonb" in sql
    assert "publication_metadata_json::text" not in (
        migration._guard_function_sql("admission_seal_contract")
    )
    assert "Application terminal validation is authoritative" in sql
    assert "not same-owner authentication" in sql


def test_downgrade_removes_only_the_m1_receipt_surface(monkeypatch) -> None:
    migration, statements, sql = _capture(monkeypatch, "downgrade")

    assert len(statements) == 32
    assert sql.count("DROP TRIGGER") == 10
    assert sql.count("DROP FUNCTION") == 4
    assert sql.count("DROP COLUMN") == len(migration._SEAL_COLUMNS)
    assert 'DROP INDEX "admission_seal_contract".' \
        '"pd_endpoint_dataset_admission_source_ids_idx"' in sql
    assert "provider_directory_endpoint_dataset_admission_downgrade_blocked" in sql
    assert "ADD CONSTRAINT \"pd_endpoint_dataset_subset_replay_evidence_check\"" in sql
    assert "UPDATE OF" not in " ".join(
        statement for statement in statements if statement.startswith("CREATE")
    )
