# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import ProviderDirectoryUHCFlexNPICohort
from db.models import ProviderDirectoryUHCFlexNPIMember


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810050000_provider_directory_uhc_flex_npi_cohort.py"
)


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "provider_directory_uhc_flex_npi_cohort_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _foreign_keys(model) -> dict[str, sa.ForeignKeyConstraint]:
    return {
        constraint.name: constraint
        for constraint in model.__table__.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    }


def test_migration_is_one_dormant_linear_storage_revision(monkeypatch) -> None:
    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.create_index = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "flex_cohort_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260810050000_provider_directory_uhc_flex_npi_cohort"
    )
    assert migration.down_revision == (
        "20260810040000_fhir_formulary_uhc_admission_receipt"
    )
    assert [call.args[0] for call in operation.create_table.call_args_list] == [
        "provider_directory_uhc_flex_npi_cohort",
        "provider_directory_uhc_flex_npi_member",
    ]
    assert all(
        call.kwargs["schema"] == "flex_cohort_test"
        for call in operation.create_table.call_args_list
    )
    operation.create_index.assert_called_once_with(
        "pd_uhc_flex_npi_member_npi_idx",
        "provider_directory_uhc_flex_npi_member",
        ["npi", "cohort_id"],
        schema="flex_cohort_test",
    )


def test_database_guard_seals_exact_current_official_practitioner_set() -> None:
    migration = _migration()
    guard_sql = " ".join(
        migration._cohort_guard_function_sql("flex_cohort_test").split()
    )

    assert "SECURITY DEFINER SET search_path = pg_catalog" in guard_sql
    assert "dataset.status = 'published'" in guard_sql
    assert "dataset.is_current IS TRUE" in guard_sql
    assert "pdfhir_2754e999dd691175821ec26e" in guard_sql
    assert "uhc_canonical_content_proof_v1" in guard_sql
    assert "healthporta.uhc.canonical-content-proof.v1" in guard_sql
    assert "resource.resource_type = 'Practitioner'" in guard_sql
    assert "public_evidence_npi_valid" in guard_sql
    assert "http://hl7.org/fhir/sid/us-npi" in guard_sql
    assert guard_sql.count(" EXCEPT SELECT") == 2
    assert "expected_cohort_id" in guard_sql
    assert guard_sql.count("pg_catalog.chr(31)") == 13
    assert "jsonb_typeof(resource.payload_json::jsonb -> 'npi') = 'number'" in (
        guard_sql
    )
    assert "jsonb_typeof(resource.payload_json::jsonb -> 'npi') = 'string'" not in (
        guard_sql
    )
    assert "LOCK TABLE" in guard_sql
    assert "FOR SHARE OF source, dataset" in guard_sql
    assert "FOR SHARE OF source, dataset" in guard_sql
    assert "FOR SHARE OF resource" not in guard_sql
    assert "PERFORM resource.resource_id" not in guard_sql


def test_guards_enforce_child_first_header_last_immutability() -> None:
    migration = _migration()
    member_sql = " ".join(
        migration._member_guard_function_sql("flex_cohort_test").split()
    )
    member_insert_sql = " ".join(
        migration._member_insert_guard_function_sql(
            "flex_cohort_test"
        ).split()
    )
    install_sql = " ".join(
        " ".join(statement.split())
        for statement in migration._guard_statements("flex_cohort_test")
    )

    assert "member_immutable" in member_sql
    assert "EXISTS ( SELECT 1 FROM new_rows" in member_insert_sql
    assert "public_evidence_npi_valid" in member_insert_sql
    assert install_sql.count("REVOKE ALL ON FUNCTION") == 3
    assert install_sql.count("REVOKE ALL ON TABLE") == 2
    assert install_sql.count("ENABLE ALWAYS TRIGGER") == 5
    assert install_sql.count("BEFORE TRUNCATE") == 2
    assert install_sql.count("BEFORE INSERT OR UPDATE OR DELETE") == 1
    assert "BEFORE UPDATE OR DELETE" in install_sql
    assert "REFERENCING NEW TABLE AS new_rows" in install_sql
    assert "downgrade_blocked" in migration._downgrade_fence_sql(
        "flex_cohort_test"
    )
    assert "ACCESS EXCLUSIVE MODE" in migration._downgrade_lock_sql(
        "flex_cohort_test"
    )


def test_models_match_identity_lineage_and_deferred_membership() -> None:
    cohort = ProviderDirectoryUHCFlexNPICohort.__table__
    member = ProviderDirectoryUHCFlexNPIMember.__table__

    assert tuple(column.name for column in cohort.primary_key.columns) == (
        "cohort_id",
    )
    assert tuple(column.name for column in member.primary_key.columns) == (
        "cohort_id",
        "npi",
    )
    assert set(_foreign_keys(ProviderDirectoryUHCFlexNPICohort)) == {
        "pd_uhc_flex_npi_cohort_source_fkey",
        "pd_uhc_flex_npi_cohort_endpoint_fkey",
        "pd_uhc_flex_npi_cohort_dataset_fkey",
    }
    member_key = _foreign_keys(ProviderDirectoryUHCFlexNPIMember)[
        "pd_uhc_flex_npi_member_cohort_fkey"
    ]
    assert member_key.deferrable is True
    assert member_key.initially == "DEFERRED"
    assert "endpoint_collection_complete" in cohort.c
    assert "endpoint_complete" in cohort.c
    assert "transaction_timestamp()" in str(cohort.c.created_at.server_default.arg)
    assert "transaction_timestamp()" in str(member.c.created_at.server_default.arg)
