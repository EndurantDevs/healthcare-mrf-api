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
    EntityAddressEvidence,
    EntityAddressMedicationBridge,
    EntityAddressNetworkBridge,
    EntityAddressPlanBridge,
    EntityAddressProcedureBridge,
    EntityAddressUnified,
    FacilityAnchor,
    FacilityAnchorNPICandidate,
    FacilityAnchorNPIOverride,
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


def test_address_source_models_use_bigint_for_npi_identifiers():
    expected_columns = {
        NPIAddress: ("npi",),
        MRFAddress: ("npi",),
    }

    for model, columns in expected_columns.items():
        for column_name in columns:
            assert isinstance(model.__table__.c[column_name].type, BigInteger)


def test_mrf_address_npi_bigint_migration_targets_address_summaries_only():
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260616123000_mrf_address_npi_bigint.py"
    )
    spec = importlib.util.spec_from_file_location("mrf_address_npi_bigint_migration", migration_path)
    assert spec is not None and spec.loader is not None
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)

    class FakeBind:
        def __init__(self):
            self.statement = None
            self.params = None

        def execute(self, statement, params):
            self.statement = str(statement)
            self.params = params
            return [
                ("mrf_address",),
                ("mrf_address_20260616",),
                ("npi_address",),
                ("npi_address_20260616",),
            ]

    bind = FakeBind()

    assert migration.down_revision == (
        "20260616090000_openaddresses_geocode",
        "20260616110000_facility_anchor_parent_npi_overrides",
    )
    assert migration._address_tables_with_narrow_npi(bind, "mrf") == [
        "mrf_address",
        "mrf_address_20260616",
        "npi_address",
        "npi_address_20260616",
    ]
    assert bind.params == {"schema": "mrf"}
    assert "table_name LIKE 'mrf_address\\_%' ESCAPE '\\'" in bind.statement
    assert "table_name NOT LIKE 'mrf_address_evidence%'" in bind.statement
    assert "table_name LIKE 'npi_address\\_%' ESCAPE '\\'" in bind.statement
    assert "data_type <> 'bigint'" in bind.statement


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
    assert "phone_number varchar(15)" in raw_sql
    assert "fax_number_digits varchar(15)" in raw_sql
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


def test_entity_address_serving_models_expose_compact_keys_and_bridges():
    unified_columns = EntityAddressUnified.__table__.c
    for column_name in (
        "location_key",
        "row_origin",
        "archive_identity_version",
        "address_precision",
        "premise_key",
        "zip5",
        "state_code",
        "city_norm",
        "source_mask",
        "address_source_mask",
        "independent_source_count",
        "location_confidence_id",
        "confidence_score",
        "ptg_plan_array",
        "ptg_source_array",
        "group_plan_array",
        "phone_number",
        "phone_extension",
        "fax_number_digits",
        "fax_extension",
    ):
        assert column_name in unified_columns

    assert EntityAddressEvidence.__tablename__ == "entity_address_evidence"
    assert EntityAddressPlanBridge.__tablename__ == "entity_address_plan_bridge"
    assert EntityAddressNetworkBridge.__tablename__ == "entity_address_network_bridge"
    assert EntityAddressProcedureBridge.__tablename__ == "entity_address_procedure_bridge"
    assert EntityAddressMedicationBridge.__tablename__ == "entity_address_medication_bridge"


def test_facility_anchor_exposes_source_identifiers():
    columns = FacilityAnchor.__table__.c

    assert "telephone_number" in columns
    assert columns["telephone_number"].nullable
    assert "npi" in columns
    assert columns["npi"].nullable
    assert columns["npi"].type.python_type is int
    assert "medicare_ccn" in columns
    assert columns["medicare_ccn"].nullable
    assert "health_center_number" in columns
    assert columns["health_center_number"].nullable
    assert "health_center_organization_id" in columns
    assert columns["health_center_organization_id"].nullable
    assert "bphc_assigned_number" in columns
    assert columns["bphc_assigned_number"].nullable
    assert "health_center_name" in columns
    assert columns["health_center_name"].nullable
    assert "health_center_organization_address_line1" in columns
    assert columns["health_center_organization_address_line1"].nullable
    assert "health_center_organization_city" in columns
    assert columns["health_center_organization_city"].nullable
    assert "health_center_organization_state" in columns
    assert columns["health_center_organization_state"].nullable
    assert "health_center_organization_zip_code" in columns
    assert columns["health_center_organization_zip_code"].nullable


def test_facility_anchor_npi_override_model_supports_curated_matches():
    columns = FacilityAnchorNPIOverride.__table__.c

    assert FacilityAnchorNPIOverride.__tablename__ == "facility_anchor_npi_override"
    assert FacilityAnchorNPIOverride.__my_index_elements__ == ["facility_anchor_id", "npi"]
    assert "facility_anchor_id" in columns
    assert not columns["facility_anchor_id"].nullable
    assert "npi" in columns
    assert not columns["npi"].nullable
    assert columns["npi"].type.python_type is int
    assert "status" in columns
    assert not columns["status"].nullable
    assert "evidence" in columns
    assert columns["evidence"].nullable


def test_facility_anchor_npi_candidate_model_supports_review_queue():
    columns = FacilityAnchorNPICandidate.__table__.c

    assert FacilityAnchorNPICandidate.__tablename__ == "facility_anchor_npi_candidate"
    assert FacilityAnchorNPICandidate.__my_index_elements__ == ["candidate_id"]
    assert "location_key" in columns
    assert not columns["location_key"].nullable
    assert "address_key" in columns
    assert columns["address_key"].nullable
    assert "facility_anchor_id" in columns
    assert not columns["facility_anchor_id"].nullable
    assert isinstance(columns["candidate_npi"].type, BigInteger)
    assert not columns["candidate_status"].nullable
    assert not columns["review_status"].nullable
    assert not columns["candidate_count"].nullable
    assert "reviewed_by" in columns
    assert columns["reviewed_by"].nullable
    assert "reviewed_at" in columns
    assert columns["reviewed_at"].nullable
    assert "evidence" in columns
    assert columns["evidence"].nullable


def test_entity_address_facility_anchor_uses_source_npi_and_ccn_inference():
    """Verify entity address facility anchor uses source npi and ccn inference."""
    source_sql = "\n".join(
        entity_address_unified._source_selects(
            "mrf",
            {
                "facility_anchor": True,
                "npi": False,
                "npi_address": False,
                "doctor_clinician_address": False,
                "provider_enrollment_ffs": False,
                "provider_enrollment_ffs_address": False,
                "provider_enrollment_ffs_additional_npi": True,
                "provider_enrollment_hospital": True,
                "provider_enrollment_fqhc": True,
                "mrf_address": False,
                "address_archive_v2": False,
            },
        )
    )
    inference_sql = entity_address_unified._inference_sql(
        "mrf",
        "entity_address_unified_stage",
        include_hospital_enrollment=True,
        include_fqhc_enrollment=True,
        include_facility_override=True,
        include_npi_other_identifier=True,
    )

    assert "fa.npi::bigint AS npi" in source_sql
    assert "WITH facility_ccn_npi_candidates AS" in source_sql
    assert "provider_enrollment_hospital AS h" in source_sql
    assert "h.cah_or_hospital_ccn" in source_sql
    assert "provider_enrollment_fqhc AS f" in source_sql
    assert "provider_enrollment_ffs_additional_npi AS a" in source_sql
    assert "HAVING COUNT(DISTINCT candidate_npi) = 1" in source_sql
    assert "WHEN COUNT(DISTINCT candidate_method) = 1 THEN MIN(candidate_method)" in source_sql
    assert "CASE WHEN fa.npi IS NULL THEN ccn_npi.npi" in source_sql
    assert "fqhc_pecos_ccn_unique" in source_sql
    assert "hospital_pecos_ccn_unique" in source_sql
    assert "fa.telephone_number::varchar AS telephone_number" in source_sql
    assert "fa.medicare_ccn" in inference_sql
    assert "facility_anchor_npi_override" in inference_sql
    assert "h.cah_or_hospital_ccn = t.entity_id" in inference_sql
    assert "hospital_nppes_other_identifier" in inference_sql
    assert "hospital_nppes_name_address" in inference_sql
    assert "hospital_nppes_address_key" in inference_sql
    assert "f.ccn = fa.medicare_ccn" in inference_sql
    assert "fqhc_ccn_match" in inference_sql
    assert "fqhc_nppes_other_identifier" in inference_sql
    assert "health_center_name" in inference_sql
    assert "fqhc_parent_enrollment_exact_address" in inference_sql
    assert "fqhc_parent_enrollment_name_state" in inference_sql
    assert "fqhc_parent_enrollment_address" in inference_sql
    assert "fqhc_parent_nppes_exact_address" in inference_sql
    assert "fqhc_parent_nppes_exact_address_primary" in inference_sql
    assert "fqhc_parent_nppes_name_state" in inference_sql
    assert "fqhc_parent_nppes_name_state_primary" in inference_sql
    assert "fqhc_parent_nppes_address" in inference_sql
    assert "fqhc_parent_nppes_address_primary" in inference_sql
    assert "fqhc_parent_nppes_exact_primary_candidates AS primary_candidates" in inference_sql
    assert "fqhc_sibling_source_npi" in inference_sql
    assert "fqhc_enrollment_exact_address" in inference_sql
    assert "fqhc_enrollment_phone_zip" in inference_sql
    assert "fqhc_enrollment_phone_name" in inference_sql
    assert "npi_fqhc_exact_address" in inference_sql
    assert "npi_fqhc_address_key" in inference_sql
    assert "npi_fqhc_clinic_address_key" in inference_sql
    assert "npi_fqhc_phone_zip" in inference_sql
    assert "npi_fqhc_clinic_phone_zip" in inference_sql
    assert "preselected_min_priorities" in inference_sql
    assert "winner_priority = best_priority" in inference_sql
    assert "preselected_candidate_counts" in inference_sql


def _facility_source_sql(*, npi_taxonomy: bool) -> str:
    return "\n".join(
        entity_address_unified._source_selects(
            "mrf",
            {
                "facility_anchor": True,
                "provider_enrollment_ffs_additional_npi": True,
                "provider_enrollment_hospital": True,
                "provider_enrollment_fqhc": True,
                "npi_taxonomy": npi_taxonomy,
            },
        )
    )


def test_entity_address_facility_ccn_conflicts_resolved_by_taxonomy():
    # When NPPES taxonomy is available, CCN->multiple-NPI conflicts pick the
    # facility-type-matching NPI (primary taxonomy preferred) instead of being
    # dropped to review.
    source_sql = _facility_source_sql(npi_taxonomy=True)
    assert "facility_ccn_npi_stats AS" in source_sql
    assert "facility_ccn_conflict_taxonomy AS" in source_sql
    assert "facility_ccn_taxonomy_npi AS" in source_sql
    assert "tax_primary_match" in source_sql
    assert "tax_any_match" in source_sql
    assert "healthcare_provider_primary_taxonomy_switch = 'Y'" in source_sql
    assert "'261QF0400X'" in source_sql
    assert "fqhc_pecos_ccn_taxonomy" in source_sql
    assert "hospital_pecos_ccn_taxonomy" in source_sql
    # the facility anchor join now consumes the unique + taxonomy-resolved union
    assert "LEFT JOIN facility_ccn_resolved_npi AS ccn_npi" in source_sql
    # single-NPI keys still resolve deterministically
    assert "HAVING COUNT(DISTINCT candidate_npi) = 1" in source_sql


def test_entity_address_facility_ccn_taxonomy_disabled_without_nppes_taxonomy():
    # Without NPPES taxonomy the conflict tier is omitted and the anchor join
    # falls back to the strict single-NPI relation.
    source_sql = _facility_source_sql(npi_taxonomy=False)
    assert "facility_ccn_taxonomy_npi" not in source_sql
    assert "LEFT JOIN facility_ccn_unique_npi AS ccn_npi" in source_sql


def test_address_key_is_not_added_to_address_prototype_archive_model():
    # The legacy archive model remains checksum-shaped until the v2 cutover; adding
    # the column to the abstract base would shift positional c.* readers.
    assert "address_key" not in AddressArchive.__table__.c
