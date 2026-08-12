# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Migration and PostgreSQL proofs for bounded endpoint-dataset receipts."""

from __future__ import annotations

import pytest

from tests.provider_directory_admission_seal_migration_fixture import _capture
from tests.provider_directory_admission_seal_migration_runtime import (
    run_postgres_contract,
)


_LEGACY_TRIGGER_NAMES = (
    "tin_npi_connector_endpoint_dataset_guard",
    "provider_directory_reviewed_subset_activation_dataset_guard",
    "pd_subset_abandonment_dataset_guard",
    "pd_subset_abandonment_dataset_consistency_guard",
    "pd_subset_terminal_disposition_dataset_consistency_guard",
    "pd_trr_dataset_row",
)


def test_upgrade_is_nullable_bounded_and_application_trusted(monkeypatch) -> None:
    """Keep the M1 upgrade nullable, bounded, scoped, and application-trusted."""

    migration, statements, sql = _capture(monkeypatch, "upgrade")

    assert migration.revision == (
        "20260812010000_provider_directory_endpoint_dataset_admission_seal"
    )
    assert migration.down_revision == (
        "20260811140000_ptg_v12_provider_publication_merge"
    )
    assert len(statements) == 43
    for column_name in migration._SEAL_COLUMNS:
        assert f"ADD COLUMN IF NOT EXISTS {column_name}" in sql
    assert "ADD COLUMN IF NOT EXISTS publication_metadata_summary_json jsonb" in sql
    assert "ADD COLUMN IF NOT EXISTS content_proof_resource_types varchar(64)[]" in sql
    add_columns_statement = next(
        statement for statement in statements
        if "ADD COLUMN IF NOT EXISTS publication_metadata_summary_json" in statement
    )
    assert "NOT NULL" not in add_columns_statement
    assert "provider_directory_endpoint_dataset_admission_column_shape_invalid" in sql
    assert "provider_directory_endpoint_dataset_admission_adoption_data_invalid" in sql
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
    for trigger_name in _LEGACY_TRIGGER_NAMES:
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

    assert len(statements) == 31
    assert sql.count("DROP TRIGGER") == 10
    assert sql.count("DROP FUNCTION") == 4
    assert sql.count("DROP COLUMN") == len(migration._SEAL_COLUMNS)
    assert "provider_directory_endpoint_dataset_admission_downgrade_blocked" in sql
    assert "ADD CONSTRAINT \"pd_endpoint_dataset_subset_replay_evidence_check\"" in sql
    assert "UPDATE OF" not in " ".join(
        statement for statement in statements if statement.startswith("CREATE")
    )


@pytest.mark.asyncio
async def test_upgrade_guard_and_downgrade_execute_on_disposable_postgres(
    monkeypatch,
) -> None:
    """Exercise the complete M1 migration contract on disposable PostgreSQL."""

    await run_postgres_contract(monkeypatch)
