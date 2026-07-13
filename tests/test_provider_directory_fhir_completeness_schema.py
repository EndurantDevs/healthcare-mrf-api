# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import importlib.util
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import (
    ProviderDirectoryCanonicalResource,
    ProviderDirectoryEndpoint,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryPractitioner,
    ProviderDirectoryPractitionerRole,
)


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260713120000_provider_directory_fhir_completeness.py"
)
FHIR_MODELS = (
    ProviderDirectoryInsurancePlan,
    ProviderDirectoryPractitioner,
    ProviderDirectoryOrganization,
    ProviderDirectoryLocation,
    ProviderDirectoryPractitionerRole,
    ProviderDirectoryHealthcareService,
    ProviderDirectoryOrganizationAffiliation,
    ProviderDirectoryEndpoint,
    ProviderDirectoryCanonicalResource,
)
PROVENANCE_COLUMNS = (
    "fhir_meta",
    "fhir_self_url",
    "fhir_fetch_url",
    "fhir_fetch_mode",
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "provider_directory_fhir_completeness_migration",
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
        self.added_columns.append((table_name, column, kwargs))

    def drop_column(self, table_name, column_name, **kwargs):
        self.dropped_columns.append((table_name, column_name, kwargs))


def test_fhir_completeness_models_expose_nullable_columns():
    for model in FHIR_MODELS:
        columns = model.__table__.c
        for column_name in PROVENANCE_COLUMNS:
            assert columns[column_name].nullable is True
        assert isinstance(columns.fhir_meta.type, SQLAlchemyJSON)
        assert isinstance(columns.fhir_self_url.type, sa.Text)
        assert isinstance(columns.fhir_fetch_url.type, sa.Text)
        assert isinstance(columns.fhir_fetch_mode.type, sa.String)
        assert columns.fhir_fetch_mode.type.length == 32

    plan_columns = ProviderDirectoryInsurancePlan.__table__.c
    for column_name in ("product_identifiers", "plan_backbones", "coverage"):
        assert isinstance(plan_columns[column_name].type, SQLAlchemyJSON)
        assert plan_columns[column_name].nullable is True

    role_columns = ProviderDirectoryPractitionerRole.__table__.c
    for column_name in (
        "available_time",
        "not_available",
        "new_patient_acceptance",
        "telehealth",
    ):
        assert isinstance(role_columns[column_name].type, SQLAlchemyJSON)
        assert role_columns[column_name].nullable is True
    assert isinstance(role_columns.availability_exceptions.type, sa.Text)
    assert role_columns.availability_exceptions.nullable is True


def test_fhir_completeness_migration_matches_models(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260713120000_provider_directory_fhir_completeness"
    assert migration.down_revision == (
        "20260712120000_provider_directory_healthcare_service_context"
    )
    assert len(recorder.added_columns) == 44
    for model in FHIR_MODELS:
        added_names = {
            column.name
            for table_name, column, kwargs in recorder.added_columns
            if table_name == model.__tablename__ and kwargs["schema"] == "mrf"
        }
        assert set(PROVENANCE_COLUMNS).issubset(added_names)
    assert all(column.nullable is True for _, column, _ in recorder.added_columns)

    migration.downgrade()

    assert len(recorder.dropped_columns) == len(recorder.added_columns)
    assert {
        (table_name, column_name)
        for table_name, column_name, _kwargs in recorder.dropped_columns
    } == {
        (table_name, column.name)
        for table_name, column, _kwargs in recorder.added_columns
    }


@pytest.mark.asyncio
async def test_model_column_sync_adds_new_role_columns(monkeypatch):
    importer = importlib.import_module("process.provider_directory_fhir")
    new_columns = set(PROVENANCE_COLUMNS) | {
        "available_time",
        "not_available",
        "availability_exceptions",
        "new_patient_acceptance",
        "telehealth",
    }
    existing_rows = [
        {"column_name": column.name}
        for column in ProviderDirectoryPractitionerRole.__table__.columns
        if column.name not in new_columns
    ]
    status = AsyncMock(return_value=0)
    monkeypatch.setattr(importer.db, "all", AsyncMock(return_value=existing_rows))
    monkeypatch.setattr(importer.db, "status", status)

    await importer._ensure_provider_directory_model_columns(
        ProviderDirectoryPractitionerRole,
        "mrf",
    )

    statements = [call.args[0] for call in status.await_args_list]
    assert len(statements) == len(new_columns)
    for column_name in new_columns:
        assert any(f" {column_name} " in statement for statement in statements)
