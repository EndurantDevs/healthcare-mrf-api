# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib.util
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.sql.sqltypes import JSON as SQLAlchemyJSON

from db.models import (
    ProviderDirectoryHealthcareService,
    ProviderDirectoryLocation,
    ProviderDirectoryOrganization,
    ProviderDirectoryPractitioner,
)


MIGRATIONS_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
)


def _load_migration(filename):
    migration_path = MIGRATIONS_PATH / filename
    spec = importlib.util.spec_from_file_location(
        migration_path.stem,
        migration_path,
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


BASE_PROFILE_COLUMN_NAMES = {
    ProviderDirectoryPractitioner: {
        "identifiers",
        "names",
        "administrative_gender",
        "addresses",
        "qualifications",
        "communications",
        "photos",
    },
    ProviderDirectoryOrganization: {
        "identifiers",
        "contacts",
        "part_of_ref",
    },
    ProviderDirectoryLocation: {
        "description",
        "physical_type_codes",
        "managing_organization_ref",
        "addresses",
        "hours_of_operation",
        "availability_exceptions",
        "photos",
    },
    ProviderDirectoryHealthcareService: {
        "program_codes",
        "characteristic_codes",
        "communication_codes",
        "referral_method_codes",
        "service_provision_codes",
        "eligibility",
        "appointment_required",
        "available_time",
        "not_available",
        "availability_exceptions",
        "extra_details",
        "photos",
    },
}
DERIVED_PRACTITIONER_COLUMN_NAMES = {
    "age_years",
    "age_as_of",
    "years_of_practice",
    "years_of_practice_as_of",
    "years_of_practice_basis",
    "years_of_practice_start_date",
}
PROFILE_COLUMN_NAMES = {
    **BASE_PROFILE_COLUMN_NAMES,
    ProviderDirectoryPractitioner: (
        BASE_PROFILE_COLUMN_NAMES[ProviderDirectoryPractitioner]
        | DERIVED_PRACTITIONER_COLUMN_NAMES
    ),
}


def test_profile_models_expose_reviewed_nullable_fields():
    for model, column_names in PROFILE_COLUMN_NAMES.items():
        columns = model.__table__.c
        for column_name in column_names:
            assert columns[column_name].nullable is True

    assert isinstance(
        ProviderDirectoryPractitioner.__table__.c.administrative_gender.type,
        sa.String,
    )
    assert isinstance(
        ProviderDirectoryPractitioner.__table__.c.age_years.type,
        sa.Integer,
    )
    assert isinstance(
        ProviderDirectoryPractitioner.__table__.c.age_as_of.type,
        sa.String,
    )
    assert isinstance(
        ProviderDirectoryPractitioner.__table__.c.years_of_practice.type,
        sa.Integer,
    )
    assert isinstance(
        ProviderDirectoryHealthcareService.__table__.c.appointment_required.type,
        sa.Boolean,
    )
    for model, column_names in PROFILE_COLUMN_NAMES.items():
        for column_name in column_names:
            if column_name in {
                "administrative_gender",
                "age_years",
                "age_as_of",
                "years_of_practice",
                "years_of_practice_as_of",
                "years_of_practice_basis",
                "years_of_practice_start_date",
                "description",
                "managing_organization_ref",
                "availability_exceptions",
                "extra_details",
                "part_of_ref",
                "appointment_required",
            }:
                continue
            assert isinstance(model.__table__.c[column_name].type, SQLAlchemyJSON)


def test_profile_field_migration_matches_models(monkeypatch):
    migration = _load_migration(
        "20260713210000_provider_directory_profile_fields.py"
    )
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == "20260713210000_provider_directory_profile_fields"
    assert migration.down_revision == "20260713200000_provider_directory_bulk_next_poll"
    assert len(recorder.added_columns) == sum(
        len(column_names) for column_names in BASE_PROFILE_COLUMN_NAMES.values()
    )
    for model, expected_column_names in BASE_PROFILE_COLUMN_NAMES.items():
        added_column_names = {
            column.name
            for table_name, column, kwargs in recorder.added_columns
            if table_name == model.__tablename__ and kwargs["schema"] == "mrf"
        }
        assert added_column_names == expected_column_names
    assert all(column.nullable is True for _, column, _ in recorder.added_columns)

    migration.downgrade()

    assert {
        (table_name, column_name)
        for table_name, column_name, _ in recorder.dropped_columns
    } == {
        (table_name, column.name)
        for table_name, column, _ in recorder.added_columns
    }


def test_practitioner_derived_profile_migration_matches_models(monkeypatch):
    migration = _load_migration(
        "20260713220000_provider_directory_practitioner_derived_profile.py"
    )
    recorder = _OpRecorder()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    assert migration.revision == (
        "20260713220000_provider_directory_practitioner_derived_profile"
    )
    assert migration.down_revision == (
        "20260713213000_provider_directory_dataset_serving_relations"
    )
    assert {
        column.name for _, column, _ in recorder.added_columns
    } == DERIVED_PRACTITIONER_COLUMN_NAMES
    assert all(
        table_name == ProviderDirectoryPractitioner.__tablename__
        and kwargs["schema"] == "mrf"
        and column.nullable is True
        for table_name, column, kwargs in recorder.added_columns
    )

    migration.downgrade()

    assert {
        column_name for _, column_name, _ in recorder.dropped_columns
    } == DERIVED_PRACTITIONER_COLUMN_NAMES
