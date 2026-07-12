# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import ProviderDirectoryHealthcareService


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260712120000_provider_directory_healthcare_service_context.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_healthcare_service_context_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.added_columns = []
        self.dropped_columns = []

    def add_column(self, table_name, column, **kwargs):
        self.added_columns.append({"table": table_name, "column": column, **kwargs})

    def drop_column(self, table_name, column_name, **kwargs):
        self.dropped_columns.append({"table": table_name, "column": column_name, **kwargs})


def test_healthcare_service_model_exposes_nullable_fhir_context_fields():
    columns = ProviderDirectoryHealthcareService.__table__.c

    assert isinstance(columns.provided_by_ref.type, sa.Text)
    assert isinstance(columns.accepting_patients.type, SQLAlchemyJSON)
    assert isinstance(columns.npi.type, sa.BigInteger)
    assert columns.provided_by_ref.nullable is True
    assert columns.accepting_patients.nullable is True
    assert columns.npi.nullable is True


def test_healthcare_service_context_migration_matches_model(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260712120000_provider_directory_healthcare_service_context"
    assert migration.down_revision == "20260711120000_provider_directory_affiliation_telecom"
    assert [entry["table"] for entry in recorder.added_columns] == [
        ProviderDirectoryHealthcareService.__tablename__,
        ProviderDirectoryHealthcareService.__tablename__,
        ProviderDirectoryHealthcareService.__tablename__,
    ]
    assert [entry["column"].name for entry in recorder.added_columns] == [
        "provided_by_ref",
        "accepting_patients",
        "npi",
    ]
    assert all(
        entry["schema"] == ProviderDirectoryHealthcareService.__table__.schema
        for entry in recorder.added_columns
    )
    assert isinstance(recorder.added_columns[0]["column"].type, sa.Text)
    assert isinstance(recorder.added_columns[1]["column"].type, SQLAlchemyJSON)
    assert isinstance(recorder.added_columns[2]["column"].type, sa.BigInteger)
    assert all(entry["column"].nullable is True for entry in recorder.added_columns)

    migration.downgrade()

    assert [entry["column"] for entry in recorder.dropped_columns] == [
        "npi",
        "accepting_patients",
        "provided_by_ref",
    ]
