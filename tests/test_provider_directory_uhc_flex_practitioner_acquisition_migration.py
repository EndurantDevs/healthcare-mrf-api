# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import ProviderDirectoryUHCFlexPractitionerAcquisition
from db.models import ProviderDirectoryUHCFlexPractitionerResource
from db.models import ProviderDirectoryUHCFlexPractitionerWork


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)


def _migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_uhc_flex_practitioner_acquisition_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _foreign_keys(model) -> dict[str, sa.ForeignKeyConstraint]:
    return {
        constraint.name: constraint
        for constraint in model.__table__.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    }


def test_migration_is_linear_dormant_three_table_storage(monkeypatch) -> None:
    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.create_index = Mock()
    operation.execute = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "flex_acquisition_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260810060000_provider_directory_uhc_flex_practitioner_acquisition"
    )
    assert migration.down_revision == (
        "20260810050000_provider_directory_uhc_flex_npi_cohort"
    )
    assert [call.args[0] for call in operation.create_table.call_args_list] == [
        "provider_directory_uhc_flex_practitioner_acquisition",
        "provider_directory_uhc_flex_practitioner_work",
        "provider_directory_uhc_flex_practitioner_resource",
    ]
    operation.create_index.assert_called_once_with(
        "pd_uhc_flex_practitioner_work_claim_idx",
        "provider_directory_uhc_flex_practitioner_work",
        ["acquisition_id", "status", "lease_expires_at", "npi"],
        schema="flex_acquisition_test",
    )


def test_guards_bind_identity_fence_results_and_exact_terminal_census() -> None:
    migration = _migration()
    acquisition_sql = " ".join(
        migration._acquisition_guard_function_sql("flex_acquisition_test").split()
    )
    work_sql = " ".join(
        migration._work_guard_function_sql("flex_acquisition_test").split()
    )
    resource_sql = " ".join(
        migration._resource_guard_function_sql("flex_acquisition_test").split()
    )
    set_sql = " ".join(
        migration._terminal_set_function_sql("flex_acquisition_test").split()
    )

    for required_fragment in (
        "healthporta.provider-directory.uhc-flex-practitioner-exact-npi.v1",
        "dataset_intent_id",
        "run_id",
    ):
        assert required_fragment in acquisition_sql or required_fragment in work_sql
    assert acquisition_sql.count(" EXCEPT ") == 2
    assert "LOCK TABLE" in acquisition_sql
    assert "endpoint_collection_complete" in acquisition_sql
    assert "actual_pending_count <> 0" in acquisition_sql
    assert "actual_leased_count <> 0" in acquisition_sql
    assert "actual_error_count <> 0" in acquisition_sql
    assert "status IN ('matched', 'unmatched', 'error')" in set_sql
    assert "work.npi / 1000" in set_sql
    assert "leaf.bucket_id / 1000" in set_sql
    assert "FOR UPDATE" not in set_sql
    assert "current_setting" in work_sql
    assert "OLD.lease_expires_at <= clock_timestamp()" in work_sql
    assert "NEW.attempt_count = OLD.attempt_count + 1" in work_sql
    assert "action = 'release'" in work_sql
    assert "NEW.status = 'pending'" in work_sql
    assert "resource.attempt = OLD.attempt_count" in work_sql
    assert "actual_resource_count BETWEEN 1 AND 16" in work_sql
    assert "requested_npi" in work_sql
    assert "payload_json_text::jsonb" in resource_sql
    assert "octet_length" not in resource_sql  # enforced by the table constraint
    assert "lease_expires_at > clock_timestamp()" in resource_sql
    assert "http://hl7.org/fhir/sid/us-npi" in resource_sql


def test_all_relations_are_guarded_and_downgrade_is_fenced() -> None:
    migration = _migration()
    operation = Mock()
    operation.execute = Mock()
    migration.op = operation

    migration._install_guards("flex_acquisition_test")

    installed = " ".join(
        " ".join(call.args[0].split())
        for call in operation.execute.call_args_list
    )
    assert installed.count("REVOKE ALL ON TABLE") == 3
    assert installed.count("REVOKE ALL ON FUNCTION") == 4
    assert installed.count("ENABLE ALWAYS TRIGGER") == 6
    assert installed.count("BEFORE TRUNCATE") == 3
    assert "downgrade_blocked" in migration._downgrade_fence_sql(
        "flex_acquisition_test"
    )
    assert "ACCESS EXCLUSIVE MODE" in migration._downgrade_lock_sql(
        "flex_acquisition_test"
    )


def test_models_match_composite_lineage_and_attempt_manifest() -> None:
    acquisition = ProviderDirectoryUHCFlexPractitionerAcquisition.__table__
    work = ProviderDirectoryUHCFlexPractitionerWork.__table__
    resource = ProviderDirectoryUHCFlexPractitionerResource.__table__

    assert tuple(column.name for column in acquisition.primary_key.columns) == (
        "acquisition_id",
    )
    assert tuple(column.name for column in work.primary_key.columns) == (
        "acquisition_id",
        "npi",
    )
    assert tuple(column.name for column in resource.primary_key.columns) == (
        "acquisition_id",
        "npi",
        "attempt",
        "resource_id",
    )
    assert set(_foreign_keys(ProviderDirectoryUHCFlexPractitionerAcquisition)) == {
        "pd_uhc_flex_practitioner_acquisition_cohort_fkey"
    }
    assert set(_foreign_keys(ProviderDirectoryUHCFlexPractitionerWork)) == {
        "pd_uhc_flex_practitioner_work_acquisition_fkey",
        "pd_uhc_flex_practitioner_work_member_fkey",
    }
    assert set(_foreign_keys(ProviderDirectoryUHCFlexPractitionerResource)) == {
        "pd_uhc_flex_practitioner_resource_work_fkey"
    }
    assert "payload_json_text" in resource.c
    assert "endpoint_collection_complete" in acquisition.c
    assert "endpoint_complete" in acquisition.c
