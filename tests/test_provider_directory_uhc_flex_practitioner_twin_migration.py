# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import ProviderDirectoryUHCFlexPractitionerTwinAdmission
from db.models import ProviderDirectoryUHCFlexPractitionerTwinAttempt


VERSIONS = Path(__file__).resolve().parents[1] / "alembic/versions"
MIGRATION_PATH = VERSIONS / (
    "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission.py"
)
ACQUISITION_PATH = VERSIONS / (
    "20260810060000_provider_directory_uhc_flex_practitioner_acquisition.py"
)


def _migration(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


def _foreign_keys(model) -> set[str]:
    return {
        constraint.name
        for constraint in model.__table__.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
    }


def test_twin_migration_is_linear_and_creates_only_attempt_and_authority(
    monkeypatch,
) -> None:
    migration = _migration(MIGRATION_PATH, "flex_twin_migration")
    operation = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "flex_twin_test")
    monkeypatch.setenv("DB_SCHEMA", "flex_twin_test")
    monkeypatch.setattr(migration, "op", operation)

    migration.upgrade()

    assert migration.revision == (
        "20260810070000_provider_directory_uhc_flex_practitioner_twin_admission"
    )
    assert migration.down_revision == (
        "20260810060000_provider_directory_uhc_flex_practitioner_acquisition"
    )
    assert [call.args[0] for call in operation.create_table.call_args_list] == [
        "provider_directory_uhc_flex_practitioner_twin_attempt",
        "provider_directory_uhc_flex_practitioner_twin_admission",
    ]


def test_attempt_guard_derives_restart_safe_ids_and_exact_match() -> None:
    migration = _migration(MIGRATION_PATH, "flex_twin_guard")
    guard_sql = " ".join(
        migration._attempt_insert_guard_sql("flex_twin_test").split()
    )

    for expected_fragment in (
        "healthporta.provider-directory.uhc-flex-practitioner-dataset-intent.v1",
        "healthporta.provider-directory.uhc-flex-practitioner-acquisition-run.v1",
        "semantic_projection_as_of::text",
        "operation_key",
        "expected_baseline_run_id",
        "expected_candidate_run_id",
        "expected_attempt_id",
    ):
        assert expected_fragment in guard_sql
    assert "LEAST(NEW.baseline_acquisition_id" in guard_sql
    assert "GREATEST(NEW.baseline_acquisition_id" in guard_sql
    assert "acquisition_role IS DISTINCT FROM 'baseline'" in guard_sql
    assert "acquisition_role IS DISTINCT FROM 'candidate'" in guard_sql
    assert "status IS DISTINCT FROM 'sealed'" in guard_sql
    assert "pending_count IS DISTINCT FROM 0" in guard_sql
    assert "leased_count IS DISTINCT FROM 0" in guard_sql
    assert "error_count IS DISTINCT FROM 0" in guard_sql
    assert "endpoint_collection_complete IS DISTINCT FROM FALSE" in guard_sql
    assert "endpoint_complete IS DISTINCT FROM FALSE" in guard_sql
    assert (
        "baseline_root.terminal_set_sha256 = candidate_root.terminal_set_sha256"
        in guard_sql
    )
    assert (
        "baseline_root.resource_count = candidate_root.resource_count"
        in guard_sql
    )
    assert "pair_consumed" in guard_sql


def test_admission_guard_allows_only_exact_matched_candidate_authority() -> None:
    migration = _migration(MIGRATION_PATH, "flex_admission_guard")
    guard_sql = " ".join(
        migration._admission_insert_guard_sql("flex_twin_test").split()
    )

    assert "attempt_root.matched IS DISTINCT FROM TRUE" in guard_sql
    assert "expected_admission_id" in guard_sql
    assert "candidate_acquisition_id = NEW.candidate_acquisition_id" in guard_sql
    assert "publication_authority IS DISTINCT FROM TRUE" in guard_sql
    assert "attempt_tampered" in guard_sql
    assert "semantic_projection_as_of" in guard_sql
    assert "operation_key" in guard_sql


def test_guards_revoke_public_run_always_and_fence_downgrade() -> None:
    migration = _migration(MIGRATION_PATH, "flex_twin_guards")
    operation = Mock()
    migration.op = operation

    migration._install_guards("flex_twin_test")

    installed = " ".join(
        " ".join(call.args[0].split())
        for call in operation.execute.call_args_list
    )
    assert installed.count("REVOKE ALL ON FUNCTION") == 4
    assert installed.count("REVOKE ALL ON TABLE") == 2
    assert installed.count("ENABLE ALWAYS TRIGGER") == 6
    assert installed.count("BEFORE TRUNCATE") == 2
    assert "ACCESS EXCLUSIVE MODE" in migration._downgrade_lock_sql(
        "flex_twin_test"
    )
    assert "downgrade_blocked" in migration._downgrade_fence_sql(
        "flex_twin_test"
    )


def test_models_bind_attempt_roots_and_candidate_authority() -> None:
    attempt = ProviderDirectoryUHCFlexPractitionerTwinAttempt.__table__
    admission = ProviderDirectoryUHCFlexPractitionerTwinAdmission.__table__

    assert tuple(column.name for column in attempt.primary_key.columns) == (
        "attempt_id",
    )
    assert tuple(column.name for column in admission.primary_key.columns) == (
        "admission_id",
    )
    assert _foreign_keys(ProviderDirectoryUHCFlexPractitionerTwinAttempt) == {
        "pd_uhc_flex_practitioner_twin_baseline_fkey",
        "pd_uhc_flex_practitioner_twin_candidate_fkey",
    }
    assert _foreign_keys(ProviderDirectoryUHCFlexPractitionerTwinAdmission) == {
        "pd_uhc_flex_practitioner_admission_attempt_fkey",
        "pd_uhc_flex_practitioner_admission_baseline_fkey",
        "pd_uhc_flex_practitioner_admission_candidate_fkey",
    }
    assert attempt.c.semantic_projection_as_of.type.python_type is __import__(
        "datetime"
    ).date
    assert admission.c.candidate_acquisition_id.unique is None
    compiled_admission = str(
        CreateTable(admission).compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert '"required_root_count":1' in compiled_admission
    assert '"required_root_count"NULL' not in compiled_admission


def test_acquisition_terminal_roots_are_comparable_across_distinct_runs() -> None:
    acquisition = _migration(ACQUISITION_PATH, "flex_acquisition_root")
    terminal_set_sql = " ".join(
        acquisition._terminal_set_function_sql("flex_twin_test").split()
    )
    work_guard_sql = " ".join(
        acquisition._work_guard_function_sql("flex_twin_test").split()
    )
    terminal_hash_sql = work_guard_sql.split(
        "expected_terminal_record_sha256 :=",
        maxsplit=1,
    )[1]

    assert "work.npi::text" in terminal_set_sql
    assert "work.terminal_record_sha256" in terminal_set_sql
    assert "|| candidate_acquisition_id ||" not in terminal_set_sql
    assert "|| NEW.npi::text" in terminal_hash_sql
    assert "|| NEW.status" in terminal_hash_sql
    assert "|| NEW.acquisition_id ||" not in terminal_hash_sql
