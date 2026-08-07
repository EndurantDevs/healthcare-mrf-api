# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Contracts for the dormant FHIR formulary storage foundation."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from db.models import (
    FHIRFormularyAlias,
    FHIRFormularyAliasMembership,
    FHIRFormularyAliasVersion,
    FHIRFormularyAlternative,
    FHIRFormularyCheckpoint,
    FHIRFormularyCoveragePlan,
    FHIRFormularyCoveragePlanVersion,
    FHIRFormularyCurrent,
    FHIRFormularyDataset,
    FHIRFormularyDatasetAlias,
    FHIRFormularyDatasetCoveragePlan,
    FHIRFormularyMedication,
    FHIRFormularySource,
)
from db.models.formulary_fhir import _schema as _model_schema


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = ROOT / "alembic" / "versions" / (
    "20260807110000_fhir_formulary_storage_foundation.py"
)
TABLE_NAMES = (
    "fhir_formulary_source",
    "fhir_formulary_dataset",
    "fhir_formulary_current",
    "fhir_formulary_coverage_plan",
    "fhir_formulary_coverage_plan_version",
    "fhir_formulary_dataset_coverage_plan",
    "fhir_formulary_drug_plan_alias",
    "fhir_formulary_drug_plan_alias_version",
    "fhir_formulary_dataset_alias",
    "fhir_formulary_medication",
    "fhir_formulary_alias_membership",
    "fhir_formulary_alternative",
    "fhir_formulary_checkpoint",
)


def _load_migration():
    module_spec = importlib.util.spec_from_file_location(
        "fhir_formulary_storage_foundation_migration",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def _upgrade_statements(monkeypatch) -> tuple[object, list[str], str]:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "formulary_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration.op, "execute", statements.append)

    migration.upgrade()

    normalized_sql = " ".join(" ".join(statements).split())
    return migration, statements, normalized_sql


def _constraint_by_name(model, constraint_name: str):
    return next(
        constraint
        for constraint in model.__table__.constraints
        if constraint.name == constraint_name
    )


def _foreign_key_signature(model, constraint_name: str) -> tuple[tuple, tuple]:
    constraint = _constraint_by_name(model, constraint_name)
    assert isinstance(constraint, sa.ForeignKeyConstraint)
    return (
        tuple(column.name for column in constraint.columns),
        tuple(element.target_fullname for element in constraint.elements),
    )


def _unique_columns(model, constraint_name: str) -> tuple[str, ...]:
    constraint = _constraint_by_name(model, constraint_name)
    assert isinstance(constraint, sa.UniqueConstraint)
    return tuple(column.name for column in constraint.columns)


def test_migration_is_current_head_child_and_dormant(monkeypatch):
    migration, statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert migration.revision == (
        "20260807110000_fhir_formulary_storage_foundation"
    )
    assert migration.down_revision == (
        "20260807100000_provider_directory_endpoint_dataset_guard"
    )
    for table_name in TABLE_NAMES:
        assert (
            f'CREATE TABLE "formulary_test"."{table_name}"'
            in normalized_sql
        )

    assert "INSERT INTO" not in normalized_sql
    assert not any(
        statement.lstrip().startswith(("INSERT ", "UPDATE ", "DELETE "))
        for statement in statements
    )
    assert "://" not in normalized_sql
    assert "enabled boolean NOT NULL DEFAULT false" in normalized_sql
    assert "runtime_config_json jsonb NOT NULL DEFAULT '{}'::jsonb" in (
        normalized_sql
    )

    table_statements = [
        statement
        for statement in statements
        if statement.lstrip().startswith("CREATE TABLE")
    ]
    assert len(table_statements) == len(TABLE_NAMES)
    assert all("CREATE INDEX" not in statement for statement in table_statements)
    function_statement = next(
        statement
        for statement in statements
        if statement.lstrip().startswith("CREATE FUNCTION")
    )
    assert "CREATE TRIGGER" not in function_statement
    assert any(
        statement.lstrip().startswith("CREATE TRIGGER")
        for statement in statements
    )


def test_migration_enforces_owner_qualified_version_links(monkeypatch):
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert (
        "CONSTRAINT fhir_formulary_dataset_source_dataset_key "
        "UNIQUE (source_id, dataset_id)"
    ) in normalized_sql
    assert (
        "CONSTRAINT fhir_formulary_current_source_dataset_fkey "
        "FOREIGN KEY (source_id, dataset_id)"
    ) in normalized_sql
    assert (
        "CONSTRAINT fhir_formulary_coverage_plan_version_owner_key "
        "UNIQUE (public_id, coverage_version_id)"
    ) in normalized_sql
    assert (
        "CONSTRAINT fhir_formulary_dataset_coverage_version_owner_fkey "
        "FOREIGN KEY (public_id, coverage_version_id)"
    ) in normalized_sql
    assert (
        "CONSTRAINT fhir_formulary_alias_version_owner_key "
        "UNIQUE (alias_id, alias_version_id)"
    ) in normalized_sql
    assert (
        "CONSTRAINT fhir_formulary_dataset_alias_version_owner_fkey "
        "FOREIGN KEY (alias_id, alias_version_id)"
    ) in normalized_sql

    source_owner_fragments = (
        "fhir_formulary_dataset_previous_owner_fkey FOREIGN KEY "
        "(source_id, previous_dataset_id)",
        "fhir_formulary_dataset_coverage_dataset_owner_fkey FOREIGN KEY "
        "(source_id, dataset_id)",
        "fhir_formulary_dataset_coverage_plan_owner_fkey FOREIGN KEY "
        "(source_id, public_id)",
        "fhir_formulary_dataset_alias_dataset_owner_fkey FOREIGN KEY "
        "(source_id, dataset_id)",
        "fhir_formulary_dataset_alias_alias_owner_fkey FOREIGN KEY "
        "(source_id, alias_id)",
        "fhir_formulary_checkpoint_dataset_owner_fkey FOREIGN KEY "
        "(source_id, dataset_id, run_id)",
        "fhir_formulary_checkpoint_alias_owner_fkey FOREIGN KEY "
        "(source_id, alias_id, source_plan_identifier)",
        "fhir_formulary_membership_alias_owner_fkey FOREIGN KEY "
        "(source_id, alias_version_id)",
        "fhir_formulary_membership_medication_owner_fkey FOREIGN KEY "
        "( source_id, upstream_medication_id, medication_version_id )",
    )
    assert all(fragment in normalized_sql for fragment in source_owner_fragments)


def test_migration_reuses_an_existing_immutable_alias_version(monkeypatch):
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert "reused_from_alias_version_id" not in normalized_sql
    assert "acquisition_mode IN ('full', 'delta')" in normalized_sql
    assert "acquisition_mode IN ('full', 'delta', 'reuse')" in normalized_sql
    assert "UNIQUE (alias_id, membership_hash)" in normalized_sql


def test_migration_freezes_checkpoint_acquisition_mode(monkeypatch):
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert "NEW.acquisition_mode <> OLD.acquisition_mode" in normalized_sql
    assert "IF OLD.completed THEN" in normalized_sql
    assert "BEFORE UPDATE OR DELETE" in normalized_sql
    assert "fhir_formulary_checkpoint_completion_check" in normalized_sql
    assert "membership_hash IS NOT NULL" in normalized_sql
    assert "membership_hash ~ '^[0-9a-f]{64}$'" in normalized_sql


def test_source_row_is_the_first_publication_lock_contract(monkeypatch):
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)
    source_table = FHIRFormularySource.__table__
    current_table = FHIRFormularyCurrent.__table__

    assert tuple(source_table.primary_key.columns.keys()) == ("source_id",)
    assert tuple(current_table.primary_key.columns.keys()) == ("source_id",)
    assert FHIRFormularySource.EXCLUDE_FIELDS == ("runtime_config_json",)
    assert source_table.c.runtime_config_json.nullable is False
    assert source_table.c.enabled.default.arg is False
    assert "INSERT INTO" not in normalized_sql
    assert "serialization lock" in (FHIRFormularySource.__doc__ or "")

    source_model = FHIRFormularySource(
        source_id="source-test",
        canonical_base="https://source.example.invalid/fhir",
        display_name="Synthetic source",
        enabled=False,
        runtime_config_json={"timeout_seconds": 30},
        metadata_json={},
    )
    assert "runtime_config_json" not in source_model.to_json_dict()


def test_models_enforce_owner_qualified_version_links():
    schema = FHIRFormularySource.__table__.schema

    assert _unique_columns(
        FHIRFormularyDataset,
        "fhir_formulary_dataset_source_dataset_key",
    ) == ("source_id", "dataset_id")
    assert _foreign_key_signature(
        FHIRFormularyCurrent,
        "fhir_formulary_current_source_dataset_fkey",
    ) == (
        ("source_id", "dataset_id"),
        (
            f"{schema}.fhir_formulary_dataset.source_id",
            f"{schema}.fhir_formulary_dataset.dataset_id",
        ),
    )
    assert _unique_columns(
        FHIRFormularyCoveragePlanVersion,
        "fhir_formulary_coverage_plan_version_owner_key",
    ) == ("public_id", "coverage_version_id")
    assert _foreign_key_signature(
        FHIRFormularyDatasetCoveragePlan,
        "fhir_formulary_dataset_coverage_version_owner_fkey",
    ) == (
        ("public_id", "coverage_version_id"),
        (
            f"{schema}.fhir_formulary_coverage_plan_version.public_id",
            f"{schema}.fhir_formulary_coverage_plan_version.coverage_version_id",
        ),
    )
    assert _unique_columns(
        FHIRFormularyAliasVersion,
        "fhir_formulary_alias_version_owner_key",
    ) == ("alias_id", "alias_version_id")
    assert _foreign_key_signature(
        FHIRFormularyDatasetAlias,
        "fhir_formulary_dataset_alias_version_owner_fkey",
    ) == (
        ("alias_id", "alias_version_id"),
        (
            f"{schema}.fhir_formulary_drug_plan_alias_version.alias_id",
            f"{schema}.fhir_formulary_drug_plan_alias_version.alias_version_id",
        ),
    )
    assert _unique_columns(
        FHIRFormularyMedication,
        "fhir_formulary_medication_membership_owner_key",
    ) == (
        "source_id",
        "upstream_medication_id",
        "medication_version_id",
    )


def test_models_source_qualify_cross_entity_ownership():
    schema = FHIRFormularySource.__table__.schema
    expected_signatures = (
        (
            FHIRFormularyDataset,
            "fhir_formulary_dataset_previous_owner_fkey",
            ("source_id", "previous_dataset_id"),
            ("fhir_formulary_dataset.source_id", "fhir_formulary_dataset.dataset_id"),
        ),
        (
            FHIRFormularyDatasetCoveragePlan,
            "fhir_formulary_dataset_coverage_plan_owner_fkey",
            ("source_id", "public_id"),
            (
                "fhir_formulary_coverage_plan.source_id",
                "fhir_formulary_coverage_plan.public_id",
            ),
        ),
        (
            FHIRFormularyDatasetAlias,
            "fhir_formulary_dataset_alias_alias_owner_fkey",
            ("source_id", "alias_id"),
            (
                "fhir_formulary_drug_plan_alias.source_id",
                "fhir_formulary_drug_plan_alias.alias_id",
            ),
        ),
        (
            FHIRFormularyAliasMembership,
            "fhir_formulary_membership_medication_owner_fkey",
            (
                "source_id",
                "upstream_medication_id",
                "medication_version_id",
            ),
            (
                "fhir_formulary_medication.source_id",
                "fhir_formulary_medication.upstream_medication_id",
                "fhir_formulary_medication.medication_version_id",
            ),
        ),
        (
            FHIRFormularyCheckpoint,
            "fhir_formulary_checkpoint_dataset_owner_fkey",
            ("source_id", "dataset_id", "run_id"),
            (
                "fhir_formulary_dataset.source_id",
                "fhir_formulary_dataset.dataset_id",
                "fhir_formulary_dataset.run_id",
            ),
        ),
    )
    for model, constraint_name, column_names, target_names in expected_signatures:
        assert _foreign_key_signature(model, constraint_name) == (
            column_names,
            tuple(f"{schema}.{target_name}" for target_name in target_names),
        )


def test_alternative_target_stays_inside_alias_generation():
    schema = FHIRFormularySource.__table__.schema
    assert _foreign_key_signature(
        FHIRFormularyAlternative,
        "fhir_formulary_alternative_target_owner_fkey",
    ) == (
        ("alias_version_id", "resolved_medication_id"),
        (
            f"{schema}.fhir_formulary_alias_membership.alias_version_id",
            f"{schema}.fhir_formulary_alias_membership.upstream_medication_id",
        ),
    )


def test_alias_reuse_model_links_existing_content_version():
    table = FHIRFormularyAliasVersion.__table__
    assert "reused_from_alias_version_id" not in table.c
    assert _unique_columns(
        FHIRFormularyAliasVersion,
        "fhir_formulary_alias_version_membership_key",
    ) == ("alias_id", "membership_hash")


def test_model_index_specs_match_migration_shapes():
    assert FHIRFormularyDataset.__my_additional_indexes__ == [
        {
            "index_elements": ("source_id", "created_at DESC"),
            "name": "fhir_formulary_dataset_source_created_idx",
        },
        {
            "index_elements": ("status", "created_at DESC"),
            "name": "fhir_formulary_dataset_status_created_idx",
        },
    ]
    medication_indexes = FHIRFormularyMedication.__my_additional_indexes__
    assert all(index.get("where") for index in medication_indexes)
    membership_index = FHIRFormularyAliasMembership.__my_additional_indexes__[0]
    assert membership_index["where"] == "rxnorm_id IS NOT NULL"


def test_models_source_qualify_binding_counterparts():
    schema = FHIRFormularySource.__table__.schema
    expected_signatures = (
        (
            FHIRFormularyAlias,
            "fhir_formulary_alias_coverage_owner_fkey",
            ("source_id", "public_id"),
            (
                "fhir_formulary_coverage_plan.source_id",
                "fhir_formulary_coverage_plan.public_id",
            ),
        ),
        (
            FHIRFormularyDatasetCoveragePlan,
            "fhir_formulary_dataset_coverage_dataset_owner_fkey",
            ("source_id", "dataset_id"),
            ("fhir_formulary_dataset.source_id", "fhir_formulary_dataset.dataset_id"),
        ),
        (
            FHIRFormularyDatasetAlias,
            "fhir_formulary_dataset_alias_dataset_owner_fkey",
            ("source_id", "dataset_id"),
            ("fhir_formulary_dataset.source_id", "fhir_formulary_dataset.dataset_id"),
        ),
        (
            FHIRFormularyAliasMembership,
            "fhir_formulary_membership_alias_owner_fkey",
            ("source_id", "alias_version_id"),
            (
                "fhir_formulary_drug_plan_alias_version.source_id",
                "fhir_formulary_drug_plan_alias_version.alias_version_id",
            ),
        ),
        (
            FHIRFormularyCheckpoint,
            "fhir_formulary_checkpoint_alias_owner_fkey",
            ("source_id", "alias_id", "source_plan_identifier"),
            (
                "fhir_formulary_drug_plan_alias.source_id",
                "fhir_formulary_drug_plan_alias.alias_id",
                "fhir_formulary_drug_plan_alias.source_plan_identifier",
            ),
        ),
    )
    for model, constraint_name, column_names, target_names in expected_signatures:
        assert _foreign_key_signature(model, constraint_name) == (
            column_names,
            tuple(f"{schema}.{target_name}" for target_name in target_names),
        )


def test_json_models_match_migration_jsonb_types():
    jsonb_columns = (
        FHIRFormularySource.__table__.c.runtime_config_json,
        FHIRFormularyMedication.__table__.c.codings_json,
        FHIRFormularyMedication.__table__.c.metadata_json,
        FHIRFormularyAlternative.__table__.c.evidence_json,
    )
    assert all(isinstance(column.type, JSONB) for column in jsonb_columns)


def test_models_keep_public_identity_and_checkpoint_owners_distinct():
    assert tuple(FHIRFormularyCoveragePlan.__table__.primary_key.columns) == (
        FHIRFormularyCoveragePlan.__table__.c.public_id,
    )
    assert tuple(FHIRFormularyCheckpoint.__table__.primary_key.columns.keys()) == (
        "source_id",
        "alias_id",
        "run_id",
    )
    assert FHIRFormularyCheckpoint.__table__.c.source_plan_identifier.nullable is (
        False
    )
    assert "next_url" not in FHIRFormularyCheckpoint.__table__.c


def test_migration_never_persists_plaintext_continuations(monkeypatch):
    _migration, _statements, normalized_sql = _upgrade_statements(monkeypatch)

    assert "next_url" not in normalized_sql
    assert "cursor" not in normalized_sql.lower()


def test_migration_schema_alias_conflict_fails_closed(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must match"):
        migration.upgrade()


def test_model_schema_alias_conflict_fails_closed(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "one")
    monkeypatch.setenv("DB_SCHEMA", "two")

    with pytest.raises(RuntimeError, match="must match"):
        _model_schema()
