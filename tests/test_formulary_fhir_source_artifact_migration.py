# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
from pathlib import Path
from unittest.mock import Mock

import sqlalchemy as sa

from db.models import FHIRFormularySourceArtifact
from db.models import FHIRFormularySourceArtifactObservation
from db.models import FHIRFormularySourceArtifactSet


MIGRATION_PATH = Path(__file__).resolve().parents[1] / "alembic/versions" / (
    "20260810030000_fhir_formulary_source_artifact.py"
)


def _migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_source_artifact_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _constraint(model, name: str):
    return next(
        constraint
        for constraint in model.__table__.constraints
        if constraint.name == name
    )


def test_source_artifact_migration_is_linear_and_default_empty(monkeypatch):
    migration = _migration()
    operation = Mock()
    operation.create_table = Mock()
    operation.execute = Mock()
    create_index = Mock()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "artifact_test")
    monkeypatch.setattr(migration, "op", operation)
    monkeypatch.setattr(migration, "create_index_if_missing", create_index)

    migration.upgrade()

    assert migration.revision == (
        "20260810030000_fhir_formulary_source_artifact"
    )
    assert migration.down_revision == (
        "20260810030000_provider_directory_organization_name_variants"
    )
    assert operation.create_table.call_count == 3
    assert [call.args[0] for call in operation.create_table.call_args_list] == [
        "fhir_formulary_source_artifact_set",
        "fhir_formulary_source_artifact_observation",
        "fhir_formulary_source_artifact",
    ]
    assert all(
        call.kwargs["schema"] == "artifact_test"
        for call in operation.create_table.call_args_list
    )
    create_index.assert_called_once()
    executed_statements = [
        " ".join(str(call.args[0]).split())
        for call in operation.execute.call_args_list
    ]
    assert all(
        statement.startswith(("ALTER", "CREATE", "REVOKE"))
        for statement in executed_statements
    )


def test_source_artifact_guard_is_always_on_and_one_way():
    migration = _migration()
    guard_sql = " ".join(
        (
            migration._guard_function_sql("artifact_test"),
            *migration._guard_install_statements(
                "artifact_test",
                table_name=migration._TABLE,
                function_name=migration._GUARD,
                trigger_name=migration._TRIGGER,
                guard_insert=True,
            ),
        )
    )
    guard_sql = " ".join(guard_sql.split())

    assert "SECURITY DEFINER SET search_path = pg_catalog" in guard_sql
    assert "REVOKE ALL ON FUNCTION" in guard_sql
    assert "TG_OP = 'INSERT'" in guard_sql
    assert "NEW.status <> 'pending'" in guard_sql
    assert "OLD.status <> 'pending'" in guard_sql
    assert "NEW.status <> 'verified'" in guard_sql
    assert "TG_OP IN ('DELETE', 'TRUNCATE')" in guard_sql
    assert "BEFORE INSERT OR UPDATE OR DELETE" in guard_sql
    assert "BEFORE TRUNCATE" in guard_sql
    assert guard_sql.count("ENABLE ALWAYS TRIGGER") == 2


def test_source_artifact_model_matches_owner_and_state_constraints():
    table = FHIRFormularySourceArtifact.__table__
    primary_key = table.primary_key
    logical_key = _constraint(
        FHIRFormularySourceArtifact,
        "fhir_formulary_source_artifact_logical_key",
    )
    source_foreign_key = next(
        constraint
        for constraint in table.constraints
        if isinstance(constraint, sa.ForeignKeyConstraint)
        and len(constraint.columns) == 1
    )
    set_foreign_key = _constraint(
        FHIRFormularySourceArtifact,
        "fhir_formulary_source_artifact_set_fkey",
    )

    assert tuple(column.name for column in primary_key.columns) == (
        "source_id",
        "source_file_set_sha256",
        "source_file_id",
    )
    assert tuple(column.name for column in logical_key.columns) == (
        "source_id",
        "source_file_set_sha256",
        "family",
        "file_name",
    )
    assert tuple(
        foreign_key.target_fullname
        for foreign_key in source_foreign_key.elements
    ) == ("mrf.fhir_formulary_source.source_id",)
    assert tuple(
        foreign_key.target_fullname for foreign_key in set_foreign_key.elements
    ) == (
        "mrf.fhir_formulary_source_artifact_set.source_id",
        "mrf.fhir_formulary_source_artifact_set.source_file_set_sha256",
        "mrf.fhir_formulary_source_artifact_set.raw_listing_projection_sha256",
    )
    assert _constraint(
        FHIRFormularySourceArtifact,
        "fhir_formulary_source_artifact_identity_check",
    ) is not None
    assert _constraint(
        FHIRFormularySourceArtifact,
        "fhir_formulary_source_artifact_state_check",
    ) is not None


def test_source_artifact_set_model_anchors_projection_and_file_count():
    table = FHIRFormularySourceArtifactSet.__table__

    assert tuple(column.name for column in table.primary_key.columns) == (
        "source_id",
        "source_file_set_sha256",
    )
    assert _constraint(
        FHIRFormularySourceArtifactSet,
        "fhir_formulary_source_artifact_set_projection_key",
    ) is not None


def test_source_artifact_observation_maps_retained_listing_to_one_set():
    table = FHIRFormularySourceArtifactObservation.__table__
    set_foreign_key = _constraint(
        FHIRFormularySourceArtifactObservation,
        "fhir_formulary_source_artifact_observation_set_fkey",
    )

    assert tuple(column.name for column in table.primary_key.columns) == (
        "source_id",
        "source_observation_sha256",
    )
    assert tuple(
        foreign_key.target_fullname for foreign_key in set_foreign_key.elements
    ) == (
        "mrf.fhir_formulary_source_artifact_set.source_id",
        "mrf.fhir_formulary_source_artifact_set.source_file_set_sha256",
        "mrf.fhir_formulary_source_artifact_set.raw_listing_projection_sha256",
    )
    assert _constraint(
        FHIRFormularySourceArtifactObservation,
        "fhir_formulary_source_artifact_observation_identity_check",
    ) is not None


def test_census_trigger_is_deferred_and_downgrade_is_locked():
    migration = _migration()
    census_sql = " ".join(
        (
            migration._census_guard_function_sql("artifact_test"),
            *migration._census_trigger_statements("artifact_test"),
        )
    )
    census_sql = " ".join(census_sql.split())
    downgrade_lock_sql = " ".join(
        migration._downgrade_lock_sql("artifact_test").split()
    )

    assert "actual_count <> expected_count" in census_sql
    assert census_sql.count("DEFERRABLE INITIALLY DEFERRED") == 2
    assert census_sql.count("ENABLE ALWAYS TRIGGER") == 2
    assert "ACCESS EXCLUSIVE MODE" in downgrade_lock_sql
    assert "source_artifact_observation" in downgrade_lock_sql
    assert _constraint(
        FHIRFormularySourceArtifactSet,
        "fhir_formulary_source_artifact_set_identity_check",
    ) is not None


def test_artifact_set_hash_function_matches_the_python_contract_shape():
    """SQL hashes every retained identity field in exact UTF-8 set order."""

    migration = _migration()
    function_sql = " ".join(
        migration._artifact_set_sha256_function_sql("artifact_test").split()
    )

    assert "fhir-formulary-source-artifact-set-v1" in function_sql
    assert "LANGUAGE sql STABLE STRICT SECURITY DEFINER" in function_sql
    assert "SET search_path = pg_catalog" in function_sql
    assert "artifact_byte_count" in function_sql
    assert "artifact_sha256" in function_sql
    assert "catalog_entry_sha256" in function_sql
    assert "catalog_modified_at" in function_sql
    assert "expected_byte_count" in function_sql
    assert "raw_listing_projection_sha256" in function_sql
    assert "source_file_set_sha256" in function_sql
    assert "ORDER BY pg_catalog.convert_to( artifact.family" in function_sql
    assert "pg_catalog.sha256" in function_sql
    assert "THEN NULL" in function_sql
