# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import importlib.util
from pathlib import Path

from sqlalchemy import BigInteger
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models import (
    AddressArchive,
    DoctorClinicianAddress,
    EntityAddressUnified,
    FacilityAnchor,
    MRFAddress,
    MRFAddressEvidence,
    NPIAddress,
    NPIData,
    NPIDataOtherIdentifier,
    NPIDataTaxonomy,
    NPIDataTaxonomyGroup,
    PartDPharmacyActivity,
    PartDPharmacyActivityStage,
    PharmacyLicenseRecord,
    PharmacyLicenseRecordHistory,
    PharmacyLicenseRecordStage,
    PlanNPIRaw,
    PlanNetworkTierRaw,
    ProviderEnrollmentFFS,
    ProviderEnrollmentFFSAddress,
)
from process.ext.utils import make_class
entity_address_unified = importlib.import_module("process.entity_address_unified")


def test_address_checksum_models_use_bigint():
    expected_columns = {
        AddressArchive: ("checksum",),
        NPIAddress: ("checksum",),
        MRFAddress: ("checksum",),
        MRFAddressEvidence: ("evidence_checksum", "checksum"),
        DoctorClinicianAddress: ("address_checksum",),
        EntityAddressUnified: ("checksum",),
    }

    for model, columns in expected_columns.items():
        for column_name in columns:
            assert isinstance(model.__table__.c[column_name].type, BigInteger)


def test_mrf_provider_staging_uses_bigint_for_npi_and_network_checksums():
    expected_columns = {
        PlanNPIRaw: ("npi", "checksum_network"),
        PlanNetworkTierRaw: ("checksum_network",),
        MRFAddressEvidence: ("npi", "checksum_network"),
    }

    for model, columns in expected_columns.items():
        for column_name in columns:
            assert isinstance(model.__table__.c[column_name].type, BigInteger)


def test_npi_source_models_use_bigint_for_npi_identifiers():
    expected_columns = {
        NPIData: ("npi", "replacement_npi"),
        NPIDataTaxonomy: ("npi",),
        NPIDataOtherIdentifier: ("npi",),
        NPIDataTaxonomyGroup: ("npi",),
    }

    for model, columns in expected_columns.items():
        for column_name in columns:
            assert isinstance(model.__table__.c[column_name].type, BigInteger)


def test_address_checksum_bigint_migration_only_rewrites_durable_archive():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260610143000_address_checksums_bigint.py"
    )
    spec = importlib.util.spec_from_file_location("address_checksums_bigint_migration", migration_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    assert migration.ADDRESS_CHECKSUM_COLUMNS == {"address_archive": ("checksum",)}


def test_address_archive_create_table_uses_plain_bigint_not_serial():
    ddl = str(CreateTable(AddressArchive.__table__).compile(dialect=postgresql.dialect()))

    assert "checksum BIGINT NOT NULL" in ddl
    assert "SERIAL" not in ddl.upper()
    assert "nextval" not in ddl.lower()


def test_unified_address_stage_sql_uses_bigint_checksum():
    raw_sql = entity_address_unified._prepare_raw_stage_sql("mrf", "entity_address_unified_raw")
    inference_sql = entity_address_unified._inference_sql(
        "mrf",
        "entity_address_unified_stage",
        include_hospital_enrollment=False,
        include_fqhc_enrollment=False,
    )

    assert "checksum bigint NOT NULL" in raw_sql
    assert "NULL::bigint AS checksum" in inference_sql
    assert "NULL::integer AS checksum" not in inference_sql


def test_address_key_is_appended_to_concrete_address_sources():
    models = (
        NPIAddress,
        MRFAddress,
        MRFAddressEvidence,
        DoctorClinicianAddress,
        ProviderEnrollmentFFS,
        ProviderEnrollmentFFSAddress,
        FacilityAnchor,
        PharmacyLicenseRecordStage,
        PharmacyLicenseRecord,
        PharmacyLicenseRecordHistory,
        PartDPharmacyActivity,
        PartDPharmacyActivityStage,
        EntityAddressUnified,
    )

    for model in models:
        assert "address_key" in model.__table__.c
        assert model.__table__.c["address_key"].nullable
        assert model.__table__.c["address_key"].type.python_type.__name__ == "UUID"
        assert list(model.__table__.c.keys())[-1] == "address_key"

        stage_model = make_class(model, "address_order_test")
        assert list(stage_model.__table__.c.keys())[-1] == "address_key"


def test_address_key_is_not_added_to_address_prototype_archive_model():
    # The legacy archive model remains checksum-shaped until the v2 cutover; adding
    # the column to the abstract base would shift positional c.* readers.
    assert "address_key" not in AddressArchive.__table__.c
