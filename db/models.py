# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import (ARRAY, DATE, JSON, SMALLINT, TEXT, TIMESTAMP,
                        BigInteger, Boolean, Column, DateTime, Float, Integer,
                        Numeric, PrimaryKeyConstraint, String, text)
from sqlalchemy.orm import declared_attr

from db.connection import Base, db
from db.json_mixin import JSONOutputMixin

NAME_SEARCH_VECTOR = (
    "LOWER("
    "COALESCE(provider_first_name,'') || ' ' || "
    "COALESCE(provider_last_name,'') || ' ' || "
    "COALESCE(provider_organization_name,'') || ' ' || "
    "COALESCE(provider_other_organization_name,'') || ' ' || "
    "COALESCE(do_business_as_text,'')"
    ")"
)

NAME_SEARCH_VECTOR_WITH_OP = f"{NAME_SEARCH_VECTOR} gin_trgm_ops"


class ImportHistory(Base, JSONOutputMixin):
    __tablename__ = 'history'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('import_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['import_id']
    import_id = Column(String)
    json_status = Column(JSON)
    when = Column(DateTime)

class ImportLog(Base, JSONOutputMixin):
    __tablename__ = 'log'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id', 'checksum']
    issuer_id = Column(Integer)
    checksum = Column(Integer)
    type = Column(String(4))
    text = Column(String)
    url = Column(String)
    source = Column(String) #plans, index, providers, etc.
    level = Column(String)  #network, json, etc.

class Issuer(Base, JSONOutputMixin):
    __tablename__ = 'issuer'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id']
    state = Column(String(2))
    issuer_id = Column(Integer)
    issuer_name = Column(String)
    issuer_marketing_name = Column(String)
    mrf_url = Column(String)
    data_contact_email = Column(String)

class PlanFormulary(Base, JSONOutputMixin):
    __tablename__ = 'plan_formulary'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)


class PartDImportRun(Base, JSONOutputMixin):
    __tablename__ = "partd_import_run_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status",), "name": "partd_import_run_v2_status_idx"},
        {"index_elements": ("started_at",), "name": "partd_import_run_v2_started_at_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    import_id = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    source_summary = Column(JSON)
    error_text = Column(TEXT)


class PartDFormularySnapshot(Base, JSONOutputMixin):
    __tablename__ = "partd_formulary_snapshot_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["snapshot_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "partd_formulary_snapshot_v2_run_idx"},
        {"index_elements": ("source_type", "release_date"), "name": "partd_formulary_snapshot_v2_source_release_idx"},
    ]

    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    source_type = Column(String(16), nullable=False)
    source_url = Column(TEXT, nullable=False)
    artifact_name = Column(String(256))
    release_date = Column(DATE)
    cutoff_month = Column(DATE)
    status = Column(String(32), nullable=False)
    row_count_activity = Column(Integer, nullable=False, default=0)
    row_count_pricing = Column(Integer, nullable=False, default=0)
    imported_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)


class PartDPharmacyActivity(Base, JSONOutputMixin):
    __tablename__ = "partd_pharmacy_activity_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("canonical_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["canonical_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "partd_pharm_act_v2_snapshot_idx"},
        {"index_elements": ("zip_code",), "name": "partd_pharm_act_v2_zip_idx"},
        {"index_elements": ("zip_code", "medicare_active", "npi"), "name": "partd_pharm_act_v2_zip_active_npi_idx"},
        {"index_elements": ("npi", "effective_from", "effective_to"), "name": "partd_pharm_act_v2_npi_effective_idx"},
        {"index_elements": ("npi", "medicare_active"), "name": "partd_pharm_act_v2_npi_active_idx"},
        {"index_elements": ("year",), "name": "partd_pharm_act_v2_year_idx"},
        {"index_elements": ("plan_ids",), "using": "gin", "name": "partd_pharm_act_v2_plan_ids_gin_idx"},
        {
            "index_elements": ("npi", "effective_from DESC", "effective_to"),
            "where": "medicare_active IS TRUE",
            "name": "partd_pharm_act_v2_npi_active_effective_idx",
        },
    ]

    canonical_id = Column(String(64), nullable=False)
    snapshot_id = Column(String(128), nullable=False)
    npi = Column(BigInteger, nullable=False)
    year = Column(Integer)
    medicare_active = Column(Boolean, nullable=False, default=True)
    pharmacy_name = Column(String(256))
    address_line1 = Column(String(256))
    address_line2 = Column(String(256))
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    pharmacy_type = Column(String(64))
    mail_order = Column(Boolean)
    dispensing_fee_brand_30 = Column(Float)
    dispensing_fee_brand_60 = Column(Float)
    dispensing_fee_brand_90 = Column(Float)
    dispensing_fee_generic_30 = Column(Float)
    dispensing_fee_generic_60 = Column(Float)
    dispensing_fee_generic_90 = Column(Float)
    dispensing_fee_selected_drug_30 = Column(Float)
    dispensing_fee_selected_drug_60 = Column(Float)
    dispensing_fee_selected_drug_90 = Column(Float)
    effective_from = Column(DATE, nullable=False)
    effective_to = Column(DATE)
    source_type = Column(String(16), nullable=False)
    plan_ids = Column(ARRAY(String), nullable=False)


class PartDMedicationCost(Base, JSONOutputMixin):
    __tablename__ = "partd_medication_cost_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("canonical_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["canonical_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "partd_med_cost_v2_snapshot_idx"},
        {"index_elements": ("code_system", "code", "year", "effective_from"), "name": "partd_med_cost_v2_code_year_effective_idx"},
        {"index_elements": ("rxnorm_id", "year"), "name": "partd_med_cost_v2_rxnorm_year_idx"},
        {"index_elements": ("ndc11", "year"), "name": "partd_med_cost_v2_ndc11_year_idx"},
        {"index_elements": ("plan_ids",), "using": "gin", "name": "partd_med_cost_v2_plan_ids_gin_idx"},
        {
            "index_elements": ("effective_from", "effective_to"),
            "name": "partd_med_cost_v2_effective_idx",
        },
        {
            "index_elements": ("rxnorm_id",),
            "where": "rxnorm_id IS NOT NULL",
            "name": "partd_med_cost_v2_rxnorm_idx",
        },
        {
            "index_elements": ("ndc11",),
            "where": "ndc11 IS NOT NULL",
            "name": "partd_med_cost_v2_ndc11_idx",
        },
    ]

    canonical_id = Column(String(64), nullable=False)
    snapshot_id = Column(String(128), nullable=False)
    year = Column(Integer)
    code_system = Column(String(16), nullable=False)
    code = Column(String(64), nullable=False)
    normalized_code = Column(String(64))
    rxnorm_id = Column(String(32))
    ndc11 = Column(String(16))
    days_supply = Column(Integer, nullable=False, default=0)
    drug_name = Column(String(256))
    tier = Column(String(64))
    pharmacy_type = Column(String(64))
    mail_order = Column(Boolean)
    cost_type = Column(String(64), nullable=False)
    cost_amount = Column(Float)
    effective_from = Column(DATE, nullable=False)
    effective_to = Column(DATE)
    source_type = Column(String(16), nullable=False)
    plan_ids = Column(ARRAY(String), nullable=False)


class PartDPharmacyActivityStage(Base, JSONOutputMixin):
    __tablename__ = "partd_pharmacy_activity_stage_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "partd_pharm_act_stage_v2_snapshot_idx"},
    ]
    id = Column(BigInteger, autoincrement=True)
    snapshot_id = Column(String(128), nullable=False)
    npi = Column(BigInteger, nullable=False)
    plan_ids = Column(ARRAY(String), nullable=False)
    year = Column(Integer)
    medicare_active = Column(Boolean, nullable=False, default=True)
    pharmacy_name = Column(String(256))
    address_line1 = Column(String(256))
    address_line2 = Column(String(256))
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    pharmacy_type = Column(String(64))
    mail_order = Column(Boolean)
    dispensing_fee_brand_30 = Column(Float)
    dispensing_fee_brand_60 = Column(Float)
    dispensing_fee_brand_90 = Column(Float)
    dispensing_fee_generic_30 = Column(Float)
    dispensing_fee_generic_60 = Column(Float)
    dispensing_fee_generic_90 = Column(Float)
    dispensing_fee_selected_drug_30 = Column(Float)
    dispensing_fee_selected_drug_60 = Column(Float)
    dispensing_fee_selected_drug_90 = Column(Float)
    effective_from = Column(DATE, nullable=False)
    effective_to = Column(DATE)
    source_type = Column(String(16), nullable=False)


class PartDMedicationCostStage(Base, JSONOutputMixin):
    __tablename__ = "partd_medication_cost_stage_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "partd_med_cost_stage_v2_snapshot_idx"},
    ]
    id = Column(BigInteger, autoincrement=True)
    snapshot_id = Column(String(128), nullable=False)
    plan_id = Column(String(32), nullable=False)
    year = Column(Integer)
    code_system = Column(String(16), nullable=False)
    code = Column(String(64), nullable=False)
    normalized_code = Column(String(64))
    rxnorm_id = Column(String(32))
    ndc11 = Column(String(16))
    days_supply = Column(Integer, nullable=False, default=0)
    drug_name = Column(String(256))
    tier = Column(String(64))
    pharmacy_type = Column(String(64))
    mail_order = Column(Boolean)
    cost_type = Column(String(64), nullable=False)
    cost_amount = Column(Float)
    effective_from = Column(DATE, nullable=False)
    effective_to = Column(DATE)
    source_type = Column(String(16), nullable=False)


class PharmacyLicenseImportRun(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_import_run_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status",), "name": "pharm_lic_import_run_v1_status_idx"},
        {"index_elements": ("started_at",), "name": "pharm_lic_import_run_v1_started_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    import_id = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    source_summary = Column(JSON)
    error_text = Column(TEXT)


class PharmacyLicenseSnapshot(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_snapshot_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["snapshot_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "pharm_lic_snapshot_v1_run_idx"},
        {"index_elements": ("state_code", "status"), "name": "pharm_lic_snapshot_v1_state_status_idx"},
    ]

    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    state_code = Column(String(2), nullable=False)
    state_name = Column(String(128), nullable=False)
    board_url = Column(String)
    source_url = Column(String)
    status = Column(String(32), nullable=False)
    row_count_parsed = Column(Integer, nullable=False, default=0)
    row_count_matched = Column(Integer, nullable=False, default=0)
    row_count_dropped = Column(Integer, nullable=False, default=0)
    row_count_inserted = Column(Integer, nullable=False, default=0)
    imported_at = Column(TIMESTAMP)
    error_text = Column(TEXT)
    metadata_json = Column(JSON)


class PharmacyLicenseStateCoverage(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_state_coverage_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("state_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["state_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("supported",), "name": "pharm_lic_state_cov_v1_supported_idx"},
        {"index_elements": ("last_success_at",), "name": "pharm_lic_state_cov_v1_success_idx"},
        {"index_elements": ("last_run_id",), "name": "pharm_lic_state_cov_v1_run_idx"},
    ]

    state_code = Column(String(2), nullable=False)
    state_name = Column(String(128), nullable=False)
    board_url = Column(String)
    source_url = Column(String)
    supported = Column(Boolean, nullable=False, default=False)
    unsupported_reason = Column(String)
    status = Column(String(32), nullable=False, default="unknown")
    last_attempted_at = Column(TIMESTAMP)
    last_success_at = Column(TIMESTAMP)
    last_run_id = Column(String(64))
    records_parsed = Column(Integer, nullable=False, default=0)
    records_matched = Column(Integer, nullable=False, default=0)
    records_dropped = Column(Integer, nullable=False, default=0)
    records_inserted = Column(Integer, nullable=False, default=0)
    updated_at = Column(TIMESTAMP)


class PharmacyLicenseRecordStage(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_record_stage_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "pharm_lic_stage_v1_snapshot_idx"},
        {"index_elements": ("run_id",), "name": "pharm_lic_stage_v1_run_idx"},
        {"index_elements": ("npi", "state_code"), "name": "pharm_lic_stage_v1_npi_state_idx"},
    ]

    id = Column(BigInteger, autoincrement=True)
    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    state_code = Column(String(2), nullable=False)
    state_name = Column(String(128), nullable=False)
    board_url = Column(String)
    source_url = Column(String)
    npi = Column(BigInteger, nullable=False)
    license_number = Column(String(64), nullable=False)
    license_type = Column(String(128))
    license_status = Column(String(32), nullable=False)
    source_status_raw = Column(String(256))
    license_issue_date = Column(DATE)
    license_effective_date = Column(DATE)
    license_expiration_date = Column(DATE)
    last_renewal_date = Column(DATE)
    disciplinary_flag = Column(Boolean, nullable=False, default=False)
    disciplinary_summary = Column(TEXT)
    disciplinary_action_date = Column(DATE)
    entity_name = Column(String)
    dba_name = Column(String)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    phone_number = Column(String(32))
    source_record_id = Column(String(128))
    source_last_seen_at = Column(TIMESTAMP)
    imported_at = Column(TIMESTAMP)


class PharmacyLicenseRecord(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_record_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("license_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["license_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "pharm_lic_record_v1_npi_idx"},
        {"index_elements": ("state_code", "license_status"), "name": "pharm_lic_record_v1_state_status_idx"},
        {"index_elements": ("license_expiration_date",), "name": "pharm_lic_record_v1_exp_idx"},
        {"index_elements": ("last_snapshot_id",), "name": "pharm_lic_record_v1_snapshot_idx"},
        {
            "index_elements": ("npi", "license_expiration_date"),
            "where": "license_status = 'active'",
            "name": "pharm_lic_record_v1_npi_active_idx",
        },
    ]

    license_key = Column(String(64), nullable=False)
    record_signature = Column(String(64), nullable=False)
    last_snapshot_id = Column(String(128), nullable=False)
    npi = Column(BigInteger, nullable=False)
    state_code = Column(String(2), nullable=False)
    state_name = Column(String(128), nullable=False)
    board_url = Column(String)
    source_url = Column(String)
    license_number = Column(String(64), nullable=False)
    license_type = Column(String(128))
    license_status = Column(String(32), nullable=False)
    source_status_raw = Column(String(256))
    license_issue_date = Column(DATE)
    license_effective_date = Column(DATE)
    license_expiration_date = Column(DATE)
    last_renewal_date = Column(DATE)
    disciplinary_flag = Column(Boolean, nullable=False, default=False)
    disciplinary_summary = Column(TEXT)
    disciplinary_action_date = Column(DATE)
    entity_name = Column(String)
    dba_name = Column(String)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    phone_number = Column(String(32))
    source_record_id = Column(String(128))
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)
    last_verified_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class PharmacyLicenseRecordHistory(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_license_record_history_v1"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("history_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_additional_indexes__ = [
        {"index_elements": ("snapshot_id",), "name": "pharm_lic_hist_v1_snapshot_idx"},
        {"index_elements": ("run_id",), "name": "pharm_lic_hist_v1_run_idx"},
        {"index_elements": ("npi", "state_code"), "name": "pharm_lic_hist_v1_npi_state_idx"},
        {"index_elements": ("license_key",), "name": "pharm_lic_hist_v1_license_key_idx"},
        {"index_elements": ("record_signature",), "name": "pharm_lic_hist_v1_sig_idx"},
    ]

    history_id = Column(BigInteger, autoincrement=True)
    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    license_key = Column(String(64), nullable=False)
    record_signature = Column(String(64), nullable=False)
    npi = Column(BigInteger, nullable=False)
    state_code = Column(String(2), nullable=False)
    state_name = Column(String(128), nullable=False)
    board_url = Column(String)
    source_url = Column(String)
    license_number = Column(String(64), nullable=False)
    license_type = Column(String(128))
    license_status = Column(String(32), nullable=False)
    source_status_raw = Column(String(256))
    license_issue_date = Column(DATE)
    license_effective_date = Column(DATE)
    license_expiration_date = Column(DATE)
    last_renewal_date = Column(DATE)
    disciplinary_flag = Column(Boolean, nullable=False, default=False)
    disciplinary_summary = Column(TEXT)
    disciplinary_action_date = Column(DATE)
    entity_name = Column(String)
    dba_name = Column(String)
    address_line1 = Column(String)
    address_line2 = Column(String)
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    phone_number = Column(String(32))
    source_record_id = Column(String(128))
    source_last_seen_at = Column(TIMESTAMP)
    imported_at = Column(TIMESTAMP)


class PlanIndividual(Base, JSONOutputMixin):
    __tablename__ = 'plan_individual'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)}]
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)


class PlanFacility(Base, JSONOutputMixin):
    __tablename__ = 'plan_facility'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'drug_tier', 'pharmacy_type'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'drug_tier', 'pharmacy_type']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)}]
    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    drug_tier = Column(String)
    mail_order = Column(Boolean)
    pharmacy_type = Column(String)
    copay_amount = Column(Float)
    copay_opt = Column(String)
    coinsurance_rate = Column(Float)
    coinsurance_opt = Column(String)

class Plan(Base, JSONOutputMixin):
    __tablename__ = 'plan'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    __my_additional_indexes__ = [
        {'index_elements': ('state',), 'name': 'plan_lookup_by_state'},
        {'index_elements': ('issuer_id',), 'name': 'plan_lookup_by_issuer'},
        {'index_elements': ('year',), 'name': 'plan_lookup_by_year'},
        {'index_elements': ('issuer_id', 'year'), 'name': 'plan_lookup_by_issuer_year'},
    ]
    plan_id = Column(String(14), nullable=False)  # len == 14
    year = Column(Integer)
    issuer_id = Column(Integer)
    state = Column(String(2))
    plan_id_type = Column(String)
    marketing_name = Column(String)
    summary_url = Column(String)
    marketing_url = Column(String)
    formulary_url = Column(String)
    plan_contact = Column(String)
    network = Column(ARRAY(String))
    benefits = Column(ARRAY(JSON))
    last_updated_on = Column(TIMESTAMP)
    checksum = Column(Integer)

class PlanAttributes(Base, JSONOutputMixin):
    __tablename__ = 'plan_attributes'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('full_plan_id', 'year', 'attr_name'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['full_plan_id', 'year', 'attr_name']
    __my_additional_indexes__ = [
        {
            'index_elements': ('full_plan_id gin_trgm_ops', 'year'),
            'using': 'gin',
            'name': 'find_all_variants',
        },
        {
            'index_elements': ('plan_id', 'year', 'attr_name'),
            'name': 'plan_attributes_plan_year_attr_idx',
        },
        {
            'index_elements': ('plan_id', 'year'),
            'name': 'plan_attributes_plan_year_idx',
        },
    ]
    plan_id = Column(String(14))
    full_plan_id = Column(String(17), nullable=False)
    year = Column(Integer)
    attr_name = Column(String)
    attr_value = Column(String)

class PlanBenefits(Base, JSONOutputMixin):
    __tablename__ = 'plan_benefits'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('full_plan_id', 'year', 'benefit_name'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['full_plan_id', 'year', 'benefit_name']
    __my_additional_indexes__ = [
        {
            'index_elements': ('plan_id', 'year', 'benefit_name'),
            'name': 'plan_benefits_plan_year_benefit_idx',
        },
        {
            'index_elements': ('plan_id', 'year'),
            'name': 'plan_benefits_plan_year_idx',
        },
    ]
    plan_id = Column(String(14), nullable=False)
    full_plan_id = Column(String(17), nullable=False)
    year = Column(Integer)
    benefit_name = Column(String)
    copay_inn_tier1 = Column(String)
    copay_inn_tier2 = Column(String)
    copay_outof_net = Column(String)
    coins_inn_tier1 = Column(String)
    coins_inn_tier2 = Column(String)
    coins_outof_net = Column(String)
    is_ehb = Column(Boolean)
    is_covered = Column(Boolean)
    quant_limit_on_svc = Column(Boolean)
    limit_qty = Column(Float)
    limit_unit = Column(String)
    exclusions = Column(String)
    explanation = Column(String)
    ehb_var_reason = Column(String)
    is_excl_from_inn_mo = Column(Boolean)
    is_excl_from_oon_mo = Column(Boolean)


class PlanRatingAreas(Base, JSONOutputMixin):
    __tablename__ = 'plan_rating_areas'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('county', 'zip3', 'state', 'market'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['county', 'zip3', 'state', 'market']

    state = Column(String(2))
    rating_area_id = Column(String)
    county = Column(String)
    zip3 = Column(String)
    market = Column(String)


class GeoZipLookup(Base, JSONOutputMixin):
    __tablename__ = 'geo_zip_lookup'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('zip_code'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['zip_code']
    __my_additional_indexes__ = [
        {'index_elements': ('state',), 'name': 'geo_zip_lookup_state_idx'},
        {'index_elements': ('city_lower', 'state'), 'name': 'geo_zip_lookup_city_state_idx'},
        {'index_elements': ('city_lower',), 'name': 'geo_zip_lookup_city_idx'},
        {'index_elements': ('latitude', 'longitude'), 'name': 'geo_zip_lookup_lat_lng_idx'},
    ]

    zip_code = Column(String(5), nullable=False)
    city = Column(String(128))
    city_lower = Column(String(128))
    state = Column(String(2))
    state_name = Column(String(128))
    county_name = Column(String(128))
    county_code = Column(String(8))
    latitude = Column(Float)
    longitude = Column(Float)
    timezone = Column(String(64))
    population = Column(Integer)


class GeoZipCensusProfile(Base, JSONOutputMixin):
    __tablename__ = "geo_zip_census_profile"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zip_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["zip_code"]

    zip_code = Column(String(5), nullable=False)
    total_population = Column(Integer)
    median_household_income = Column(Integer)
    bachelors_degree_or_higher_pct = Column(Float)
    employment_rate_pct = Column(Float)
    total_housing_units = Column(Integer)
    without_health_insurance_pct = Column(Float)
    total_employer_establishments = Column(Integer)
    business_employment = Column(Integer)
    business_payroll_annual_k = Column(Integer)
    total_households = Column(Integer)
    hispanic_or_latino = Column(Integer)
    hispanic_or_latino_pct = Column(Float)
    poverty_rate_pct = Column(Float)
    median_age = Column(Float)
    unemployment_rate_pct = Column(Float)
    labor_force_participation_pct = Column(Float)
    vacancy_rate_pct = Column(Float)
    median_home_value = Column(Integer)
    median_gross_rent = Column(Integer)
    commute_mean_minutes = Column(Float)
    commute_mode_drove_alone_pct = Column(Float)
    commute_mode_carpool_pct = Column(Float)
    commute_mode_public_transit_pct = Column(Float)
    commute_mode_walked_pct = Column(Float)
    commute_mode_worked_from_home_pct = Column(Float)
    broadband_access_pct = Column(Float)
    race_white_alone = Column(Integer)
    race_black_or_african_american_alone = Column(Integer)
    race_american_indian_and_alaska_native_alone = Column(Integer)
    race_asian_alone = Column(Integer)
    race_native_hawaiian_and_other_pacific_islander_alone = Column(Integer)
    race_some_other_race_alone = Column(Integer)
    race_two_or_more_races = Column(Integer)
    race_white_alone_pct = Column(Float)
    race_black_or_african_american_alone_pct = Column(Float)
    race_american_indian_and_alaska_native_alone_pct = Column(Float)
    race_asian_alone_pct = Column(Float)
    race_native_hawaiian_and_other_pacific_islander_alone_pct = Column(Float)
    race_some_other_race_alone_pct = Column(Float)
    race_two_or_more_races_pct = Column(Float)
    acs_white_alone_pct = Column(Float)
    acs_black_or_african_american_alone_pct = Column(Float)
    acs_american_indian_and_alaska_native_alone_pct = Column(Float)
    acs_asian_alone_pct = Column(Float)
    acs_native_hawaiian_and_other_pacific_islander_alone_pct = Column(Float)
    acs_some_other_race_alone_pct = Column(Float)
    acs_two_or_more_races_pct = Column(Float)
    acs_hispanic_or_latino_pct = Column(Float)
    updated_at = Column(DateTime)


class PlanPrices(Base, JSONOutputMixin):
    __tablename__ = 'plan_prices'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year', 'checksum', ]
    __my_additional_indexes__ = [
        {'index_elements': ('state', 'year', 'min_age', 'max_age', 'rating_area_id', 'couple'),
            'using': 'gin',
            'name': 'find_plan'}]

    plan_id = Column(String(14), nullable=False)
    year = Column(Integer)
    state = Column(String(2))
    checksum = Column(Integer)
    rate_effective_date = Column(DATE)
    rate_expiration_date = Column(DATE)
    rating_area_id = Column(String)
    tobacco = Column(String)
    min_age = Column(SMALLINT)
    max_age = Column(SMALLINT)
    individual_rate = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    individual_tobacco_rate = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_one_dependent = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_two_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    primary_subscriber_and_three_or_more_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_one_dependent = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_two_dependents = Column(Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))
    couple_and_three_or_more_dependents = Column(
        Numeric(scale=2, precision=8, asdecimal=False, decimal_return_scale=None))


class PlanTransparency(Base, JSONOutputMixin):
    __tablename__ = 'plan_transparency'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    state = Column(String(2))
    issuer_name = Column(String)
    issuer_id = Column(Integer)
    new_issuer_to_exchange = Column(Boolean)
    sadp_only = Column(Boolean)
    plan_id = Column(String(14), nullable=False)  # len == 14
    year = Column(Integer)
    qhp_sadp = Column(String)
    plan_type = Column(String)
    metal = Column(String)
    claims_payment_policies_url = Column(String)


class PlanNPIRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_npi_raw'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum_network'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum_network']
    __my_additional_indexes__ = [
        {'index_elements': ('issuer_id', 'network_tier', 'year'), 'using': 'gin'},]


    npi = Column(Integer)
    checksum_network = Column(Integer)
    type =  Column(String)
    last_updated_on = Column(TIMESTAMP)
    network_tier = Column(String)
    issuer_id = Column(Integer)
    year = Column(Integer)
    name_or_facility_name = Column(String)
    prefix = Column(String)
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    suffix = Column(String)
    addresses = Column(ARRAY(JSON))
    specialty_or_facility_type = Column(ARRAY(String))
    accepting = Column(String)
    gender = Column(String)
    languages = Column(ARRAY(String))



class PlanNetworkTierRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_networktier'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'checksum_network'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'checksum_network']
    __my_additional_indexes__ = [
        {'index_elements': ('issuer_id', 'network_tier', 'year'), 'using': 'gin'},
        {'index_elements': ('checksum_network',), 'using': 'gin'}, ]


    plan_id = Column(String(14))
    network_tier = Column(String)
    issuer_id = Column(Integer)
    year = Column(Integer)
    checksum_network = Column(Integer)


class PlanDrugRaw(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_raw'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'rxnorm_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'rxnorm_id']
    __my_additional_indexes__ = [
        {
            'index_elements': ('plan_id', 'drug_tier'),
            'using': 'gin',
            'name': 'plan_drug_tier_lookup',
        },
        {
            'index_elements': ('rxnorm_id',),
            'name': 'plan_drug_rxnorm_lookup',
        },
    ]

    plan_id = Column(String(14), nullable=False)
    plan_id_type = Column(String)
    rxnorm_id = Column(String, nullable=False)
    drug_name = Column(String)
    drug_tier = Column(String)
    prior_authorization = Column(Boolean)
    step_therapy = Column(Boolean)
    quantity_limit = Column(Boolean)
    last_updated_on = Column(TIMESTAMP)


class PlanDrugStats(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_stats'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id']
    plan_id = Column(String(14), nullable=False)
    total_drugs = Column(Integer, nullable=False, default=0)
    auth_required = Column(Integer, nullable=False, default=0)
    auth_not_required = Column(Integer, nullable=False, default=0)
    step_required = Column(Integer, nullable=False, default=0)
    step_not_required = Column(Integer, nullable=False, default=0)
    quantity_limit = Column(Integer, nullable=False, default=0)
    quantity_no_limit = Column(Integer, nullable=False, default=0)
    last_updated_on = Column(TIMESTAMP)


class PlanDrugTierStats(Base, JSONOutputMixin):
    __tablename__ = 'plan_drug_tier_stats'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'drug_tier'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'drug_tier']
    plan_id = Column(String(14), nullable=False)
    drug_tier = Column(String, nullable=False)
    drug_count = Column(Integer, nullable=False, default=0)


class PlanSearchSummary(Base, JSONOutputMixin):
    __tablename__ = 'plan_search_summary'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('plan_id', 'year'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['plan_id', 'year']
    __my_additional_indexes__ = [
        {'index_elements': ('state', 'year'), 'name': 'plan_search_summary_state_year_idx'},
        {'index_elements': ('issuer_id', 'year'), 'name': 'plan_search_summary_issuer_year_idx'},
    ]

    plan_id = Column(String(14), nullable=False)
    year = Column(Integer, nullable=False)
    state = Column(String(2))
    issuer_id = Column(Integer)
    marketing_name = Column(String)
    market_coverage = Column(String)
    is_on_exchange = Column(Boolean)
    is_off_exchange = Column(Boolean)
    is_hsa = Column(Boolean)
    is_dental_only = Column(Boolean)
    is_catastrophic = Column(Boolean)
    has_adult_dental = Column(Boolean)
    has_child_dental = Column(Boolean)
    has_adult_vision = Column(Boolean)
    has_child_vision = Column(Boolean)
    telehealth_supported = Column(Boolean)
    deductible_inn_individual = Column(Float)
    moop_inn_individual = Column(Float)
    premium_min = Column(Float)
    premium_max = Column(Float)
    plan_type = Column(String)
    metal_level = Column(String)
    csr_variation = Column(String)
    attributes = Column(JSON)
    plan_benefits = Column(JSON)
    updated_at = Column(TIMESTAMP)


class NPIData(Base, JSONOutputMixin):
    __tablename__ = 'npi'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi']
    __my_additional_indexes__ = [
        {
            'index_elements': (NAME_SEARCH_VECTOR_WITH_OP,),
            'using': 'gin',
            'name': 'name_search_trgm',
        },
        {
            'index_elements': (
                "LOWER(COALESCE(provider_organization_name,'') || ' ' || "
                "COALESCE(provider_other_organization_name,'') || ' ' || "
                "COALESCE(do_business_as_text,'')) gin_trgm_ops",
            ),
            'using': 'gin',
            'name': 'organization_search_trgm',
        },
        {
            'index_elements': ("LOWER(COALESCE(provider_first_name,'')) gin_trgm_ops",),
            'using': 'gin',
            'name': 'first_name_trgm',
        },
        {
            'index_elements': ("LOWER(COALESCE(provider_last_name,'')) gin_trgm_ops",),
            'using': 'gin',
            'name': 'last_name_trgm',
        },
        {
            'index_elements': ('entity_type_code',),
            'name': 'entity_type_code',
        },
    ]
    npi = Column(Integer, primary_key=True)
    employer_identification_number = Column(String)
    entity_type_code = Column(Integer)
    replacement_npi = Column(Integer)
    provider_organization_name = Column(String)
    provider_last_name = Column(String)
    provider_first_name = Column(String)
    provider_middle_name = Column(String)
    provider_name_prefix_text = Column(String)
    provider_name_suffix_text = Column(String)
    provider_sex_code = Column(String)
    provider_credential_text = Column(String)
    provider_other_organization_name = Column(String)
    provider_other_organization_name_type_code = Column(String)
    provider_other_last_name = Column(String)
    provider_other_first_name = Column(String)
    provider_other_middle_name = Column(String)
    provider_other_name_prefix_text = Column(String)
    provider_other_name_suffix_text = Column(String)
    provider_other_credential_text = Column(String)
    provider_other_last_name_type_code = Column(String)
    provider_enumeration_date = Column(DATE)
    last_update_date = Column(DATE)
    npi_deactivation_reason_code = Column(String)
    npi_deactivation_date = Column(DATE)
    npi_reactivation_date = Column(DATE)
    authorized_official_last_name = Column(String)
    authorized_official_first_name = Column(String)
    authorized_official_middle_name = Column(String)
    authorized_official_title_or_position = Column(String)
    authorized_official_telephone_number = Column(String)
    is_sole_proprietor = Column(String)
    is_organization_subpart = Column(String)
    parent_organization_lbn = Column(String)
    parent_organization_tin = Column(String)
    authorized_official_name_prefix_text = Column(String)
    authorized_official_name_suffix_text = Column(String)
    authorized_official_credential_text = Column(String)
    certification_date = Column(DATE)
    do_business_as = Column(
        ARRAY(String),
        nullable=False,
        server_default=text("ARRAY[]::varchar[]"),
        default=list,
    )
    do_business_as_text = Column(String)


class NPIDataTaxonomy(Base, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']
    __my_additional_indexes__ = [{'index_elements': ('healthcare_provider_taxonomy_code', 'npi',)}, ]

    npi = Column(Integer)
    checksum = Column(Integer)
    healthcare_provider_taxonomy_code = Column(String)
    provider_license_number = Column(String)
    provider_license_number_state_code = Column(String)
    healthcare_provider_primary_taxonomy_switch = Column(String)

class NPIDataOtherIdentifier(Base, JSONOutputMixin):
    __tablename__ = 'npi_other_identifier'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(Integer, primary_key=True)
    checksum = Column(Integer, primary_key=True)
    other_provider_identifier = Column(String)
    other_provider_identifier_type_code = Column(String)
    other_provider_identifier_state = Column(String)
    other_provider_identifier_issuer = Column(String)

class NPIDataTaxonomyGroup(Base, JSONOutputMixin):
    __tablename__ = 'npi_taxonomy_group'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('npi', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['npi', 'checksum']

    npi = Column(Integer)
    checksum = Column(Integer)
    healthcare_provider_taxonomy_group = Column(String)


class NUCCTaxonomy(Base, JSONOutputMixin):
    __tablename__ = 'nucc_taxonomy'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('code'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['code']
    __my_additional_indexes__ = [{'index_elements': ('int_code',)}, {'index_elements': ('display_name',)},
        {'index_elements': ('classification','section'), 'using': 'gin'} ]

    int_code = Column(Integer)
    code = Column(String)
    grouping = Column(String)
    classification = Column(String)
    specialization = Column(String)
    definition = Column(TEXT)
    notes = Column(TEXT)
    display_name = Column(String)
    section = Column(String)



class AddressPrototype(Base, JSONOutputMixin):
    __abstract__ = True

    @declared_attr
    def __table_args__(cls):
        return {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True}

    checksum = Column(Integer, primary_key=True)
    first_line = Column(String)
    second_line  = Column(String)
    city_name = Column(String)
    state_name = Column(String)
    postal_code = Column(String)
    country_code = Column(String)
    telephone_number = Column(String)
    fax_number = Column(String)
    formatted_address = Column(String)
    lat = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    long = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    date_added = Column(DATE)
    place_id = Column(String)


class AddressArchive(AddressPrototype):
    __tablename__ = 'address_archive'
    __main_table__ = __tablename__
    __my_index_elements__ = ['checksum']
    # __my_additional_indexes__ = [{'index_elements': ('healthcare_provider_taxonomy_code', 'npi',)}, ]


class NPIAddress(AddressPrototype):
    __tablename__ = 'npi_address'
    __main_table__ = __tablename__
    __my_index_elements__ = ['npi', 'type', 'checksum']
    #__my_initial_indexes__ = [{'index_elements': ('npi', 'type'), 'unique': True, 'where': "type='primary'"}] #  or type='secondary'
    __my_initial_indexes__ = [{'index_elements': ('checksum',)}]

    __my_additional_indexes__ = [
        {'index_elements': ('type', 'npi'), 'name': 'type_npi'},
        {
            'index_elements': ('state_name', 'city_name', 'npi'),
            'name': 'primary_state_city_npi',
            'where': "type='primary'",
        },
        {
            'index_elements': ('lat', 'long', 'npi'),
            'name': 'primary_lat_long_npi',
            'where': "type='primary' AND lat IS NOT NULL AND long IS NOT NULL",
        },
        {
            'index_elements': ("LEFT(postal_code, 5)", 'npi'),
            'name': 'primary_postal_code_5_npi',
            'where': "type='primary'",
        },
        {
            'index_elements': (
                "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
                'npi',
            ),
            'name': 'primary_phone_digits_npi',
            'where': "type='primary'",
        },
        {
            'index_elements': ('npi',),
            'name': 'primary_with_coverage_npi',
            'where': "type='primary' AND NOT (plans_network_array @@ '0'::query_int)",
        },
        {
            'index_elements': ('taxonomy_array gin__int_ops',),
            'using': 'gin',
            'name': 'taxonomy_array',
        },
        {
            'index_elements': ('plans_network_array gin__int_ops',),
            'using': 'gin',
            'name': 'plans_network_array',
        },
        {
            'index_elements': ('procedures_array gin__int_ops',),
            'using': 'gin',
            'name': 'procedures_array',
        },
        {
            'index_elements': ('medications_array gin__int_ops',),
            'using': 'gin',
            'name': 'medications_array',
        },
        {
            'index_elements': (
            'taxonomy_array gin__int_ops',
            'plans_network_array gin__int_ops',
        ),
            'using': 'gin',
            'name': 'taxonomy_plans_network',
        },
        {
            'index_elements': (
                'Geography(ST_MakePoint((long)::double precision, (lat)::double precision))',
            ),
            'using': 'gist',
            'name': 'geo_idx',
            'where': "type='primary' OR type='secondary'",
        },
    ]

    npi = Column(Integer, primary_key=True)
    type = Column(String, primary_key=True)
    taxonomy_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    plans_network_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    procedures_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    medications_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")

    # NPI	Provider Secondary Practice Location Address- Address Line 1	Provider Secondary Practice Location Address-  Address Line 2	Provider Secondary Practice Location Address - City Name	Provider Secondary Practice Location Address - State Name	Provider Secondary Practice Location Address - Postal Code	Provider Secondary Practice Location Address - Country Code (If outside U.S.)	Provider Secondary Practice Location Address - Telephone Number	Provider Secondary Practice Location Address - Telephone Extension	Provider Practice Location Address - Fax Number


class NPIPhoneStaffing(Base, JSONOutputMixin):
    __tablename__ = 'npi_phone_staffing'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('state_name', 'telephone_number'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['state_name', 'telephone_number']

    state_name = Column(String, primary_key=True)
    telephone_number = Column(String, primary_key=True)
    pharmacist_count = Column(Integer, nullable=False, server_default="0")
    updated_at = Column(TIMESTAMP)


class PTGFile(Base, JSONOutputMixin):
    __tablename__ = 'ptg_file'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('file_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['file_id']
    __my_additional_indexes__ = [
        {'index_elements': ('file_type',), 'name': 'ptg_file_type_idx'},
        {'index_elements': ('url',), 'name': 'ptg_file_url_idx'},
    ]

    file_id = Column(BigInteger)
    file_type = Column(String(64))
    url = Column(String)
    description = Column(String)
    reporting_entity_name = Column(String)
    reporting_entity_type = Column(String)
    last_updated_on = Column(DATE)
    version = Column(String(32))
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)
    from_index_url = Column(String)


class PTGProviderGroup(Base, JSONOutputMixin):
    __tablename__ = 'ptg_provider_group'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('provider_group_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['provider_group_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('provider_group_ref',), 'name': 'ptg_provider_group_ref_idx'},
        {'index_elements': ('tin_value',), 'name': 'ptg_provider_group_tin_idx'},
    ]

    provider_group_hash = Column(BigInteger)
    provider_group_ref = Column(Integer)
    file_id = Column(BigInteger)
    network_names = Column(ARRAY(String))
    tin_type = Column(String(8))
    tin_value = Column(String(32))
    tin_business_name = Column(String)
    npi = Column(ARRAY(BigInteger))


class PTGInNetworkItem(Base, JSONOutputMixin):
    __tablename__ = 'ptg_in_network_item'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('item_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['item_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('billing_code',), 'name': 'ptg_item_billing_code_idx'},
        {'index_elements': ('billing_code_type',), 'name': 'ptg_item_code_type_idx'},
    ]

    item_hash = Column(BigInteger)
    file_id = Column(BigInteger)
    negotiation_arrangement = Column(String(32))
    name = Column(String)
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)
    severity_of_illness = Column(String)
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)


class PTGBillingCode(Base, JSONOutputMixin):
    __tablename__ = 'ptg_billing_code'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('code_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['code_hash']
    __my_additional_indexes__ = [{'index_elements': ('item_hash',), 'name': 'ptg_billing_code_item_idx'}]

    code_hash = Column(BigInteger)
    item_hash = Column(BigInteger)
    code_role = Column(String(16))  # bundle | covered
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)


class PTGNegotiatedRate(Base, JSONOutputMixin):
    __tablename__ = 'ptg_negotiated_rate'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('rate_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['rate_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('item_hash',), 'name': 'ptg_negotiated_item_idx'},
        {'index_elements': ('provider_group_hash',), 'name': 'ptg_negotiated_group_idx'},
    ]

    rate_hash = Column(BigInteger)
    item_hash = Column(BigInteger)
    provider_group_ref = Column(Integer)
    provider_group_hash = Column(BigInteger)


class PTGNegotiatedPrice(Base, JSONOutputMixin):
    __tablename__ = 'ptg_negotiated_price'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('price_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['price_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('rate_hash',), 'name': 'ptg_price_rate_idx'},
        {'index_elements': ('billing_class',), 'name': 'ptg_price_class_idx'},
    ]

    price_hash = Column(BigInteger)
    rate_hash = Column(BigInteger)
    negotiated_type = Column(String(32))
    negotiated_rate = Column(Numeric)
    expiration_date = Column(DATE)
    service_code = Column(ARRAY(String))
    billing_class = Column(String(32))
    setting = Column(String(32))
    billing_code_modifier = Column(ARRAY(String))
    additional_information = Column(String)


class PTGAllowedItem(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_item'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('allowed_item_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['allowed_item_hash']
    __my_additional_indexes__ = [{'index_elements': ('billing_code',), 'name': 'ptg_allowed_code_idx'}]

    allowed_item_hash = Column(BigInteger)
    file_id = Column(BigInteger)
    name = Column(String)
    billing_code_type = Column(String(32))
    billing_code_type_version = Column(String(32))
    billing_code = Column(String(64))
    description = Column(String)
    plan_name = Column(String)
    plan_id_type = Column(String(16))
    plan_id = Column(String(32))
    plan_market_type = Column(String(32))
    issuer_name = Column(String)
    plan_sponsor_name = Column(String)


class PTGAllowedPayment(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_payment'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('payment_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['payment_hash']
    __my_additional_indexes__ = [
        {'index_elements': ('allowed_item_hash',), 'name': 'ptg_allowed_payment_item_idx'},
        {'index_elements': ('tin_value',), 'name': 'ptg_allowed_payment_tin_idx'},
    ]

    payment_hash = Column(BigInteger)
    allowed_item_hash = Column(BigInteger)
    tin_type = Column(String(8))
    tin_value = Column(String(32))
    service_code = Column(ARRAY(String))
    billing_class = Column(String(32))
    setting = Column(String(32))
    allowed_amount = Column(Numeric)
    billing_code_modifier = Column(ARRAY(String))


class PTGAllowedProviderPayment(Base, JSONOutputMixin):
    __tablename__ = 'ptg_allowed_provider_payment'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('provider_payment_hash'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['provider_payment_hash']
    __my_additional_indexes__ = [{'index_elements': ('payment_hash',), 'name': 'ptg_allowed_provider_payment_idx'}]

    provider_payment_hash = Column(BigInteger)
    payment_hash = Column(BigInteger)
    billed_charge = Column(Numeric)
    npi = Column(ARRAY(BigInteger))


class PricingProvider(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("provider_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["provider_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "pricing_provider_npi_idx"},
        {"index_elements": ("year",), "name": "pricing_provider_year_idx"},
        {"index_elements": ("year", "npi"), "name": "pricing_provider_year_npi_idx"},
        {"index_elements": ("state", "city"), "name": "pricing_provider_state_city_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_year_state_city_idx"},
        {"index_elements": ("year", "state", "city", "provider_type"), "name": "pricing_provider_year_state_city_type_idx"},
        {"index_elements": ("provider_type",), "name": "pricing_provider_type_idx"},
        {"index_elements": ("year", "lower(provider_type)"), "name": "pricing_provider_year_provider_type_lower_idx"},
        {"index_elements": ("year", "lower(provider_name)"), "name": "pricing_provider_year_provider_name_lower_idx"},
        {"index_elements": ("year", "total_allowed_amount DESC"), "name": "pricing_provider_year_total_allowed_amount_desc_idx"},
        {"index_elements": ("year", "total_services DESC"), "name": "pricing_provider_year_total_services_desc_idx"},
    ]

    provider_key = Column(BigInteger)
    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    provider_name = Column(String)
    first_name = Column(String)
    last_org_name = Column(String)
    credentials = Column(String)
    provider_type = Column(String)
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    country = Column(String)
    total_services = Column(Float)
    total_distinct_hcpcs_codes = Column(Float)
    total_allowed_amount = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_submitted_charges = Column(Float)
    total_beneficiaries = Column(Float)


class PricingProcedure(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("procedure_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["procedure_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("service_description",), "name": "pricing_procedure_service_description_idx"},
        {"index_elements": ("reported_code",), "name": "pricing_procedure_reported_code_idx"},
        {"index_elements": ("lower(service_description)",), "name": "pricing_procedure_service_description_lower_idx"},
        {"index_elements": ("lower(reported_code)",), "name": "pricing_procedure_reported_code_lower_idx"},
        {"index_elements": ("source_year", "lower(service_description)"), "name": "pricing_procedure_year_service_description_lower_idx"},
    ]

    procedure_code = Column(BigInteger)
    service_description = Column(String)
    reported_code = Column(String)
    avg_submitted_charge = Column(Float)
    avg_allowed_amount = Column(Float)
    avg_payment_amount = Column(Float)
    avg_standardized_amount = Column(Float)
    total_allowed_amount = Column(Float)
    total_services = Column(Float)
    total_beneficiaries = Column(Float)
    source_year = Column(Integer)


class PricingProviderProcedure(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "procedure_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "procedure_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "procedure_code"), "name": "pricing_provider_proc_year_idx"},
        {"index_elements": ("year", "procedure_code", "npi"), "name": "pricing_provider_proc_year_npi_idx"},
        {"index_elements": ("reported_code",), "name": "pricing_provider_proc_reported_code_idx"},
        {"index_elements": ("service_description",), "name": "pricing_provider_proc_service_description_idx"},
        {"index_elements": ("year", "lower(reported_code)"), "name": "pricing_provider_proc_year_reported_code_lower_idx"},
        {"index_elements": ("year", "lower(service_description)"), "name": "pricing_provider_proc_year_service_description_lower_idx"},
        {"index_elements": ("year", "npi", "total_allowed_amount DESC"), "name": "pricing_provider_proc_year_npi_total_allowed_amount_desc_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    service_description = Column(String)
    reported_code = Column(String)
    total_services = Column(Float)
    total_beneficiary_day_services = Column(Float)
    total_submitted_charges = Column(Float)
    total_allowed_amount = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_beneficiaries = Column(Float)
    ge65_total_services = Column(Float)
    ge65_total_allowed_amount = Column(Float)
    ge65_total_beneficiaries = Column(Float)


class PricingProviderProcedureLocation(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure_location"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("location_key"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["location_key"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi", "year", "procedure_code"), "name": "pricing_provider_loc_lookup_idx"},
        {"index_elements": ("state", "city"), "name": "pricing_provider_loc_state_city_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_loc_year_state_city_idx"},
        {"index_elements": ("year", "zip5"), "name": "pricing_provider_loc_year_zip5_idx"},
    ]

    location_key = Column(BigInteger)
    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    place_of_service = Column(String(8))
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    state_fips = Column(String(4))
    country = Column(String)


class PricingProviderProcedureCostProfile(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_procedure_cost_profile"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "npi",
            "year",
            "procedure_code",
            "geography_scope",
            "geography_value",
            "specialty_key",
            "setting_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "npi",
        "year",
        "procedure_code",
        "geography_scope",
        "geography_value",
        "specialty_key",
        "setting_key",
    ]
    __my_additional_indexes__ = [
        {"index_elements": ("npi", "procedure_code", "year"), "name": "pricing_provider_proc_cost_npi_proc_year_idx"},
        {
            "index_elements": ("procedure_code", "setting_key", "specialty_key", "geography_scope", "geography_value", "year"),
            "name": "pricing_provider_proc_cost_lookup_idx",
        },
        {
            "index_elements": (
                "geography_scope",
                "geography_value",
                "specialty_key",
                "setting_key",
                "year",
                "avg_submitted_charge",
            ),
            "name": "pricing_provider_proc_cost_geo_avg_idx",
        },
        {
            "index_elements": (
                "procedure_code",
                "setting_key",
                "specialty_key",
                "geography_value",
                "year",
                "claim_count",
                "avg_submitted_charge",
            ),
            "name": "pricing_provider_proc_cost_zip_dynamic_idx",
            "where": "geography_scope = 'zip5'",
        },
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    procedure_code = Column(BigInteger, nullable=False)
    geography_scope = Column(String(16), nullable=False)
    geography_value = Column(String(128), nullable=False)
    specialty_key = Column(String(128), nullable=False)
    setting_key = Column(String(64), nullable=False)
    claim_count = Column(Float)
    total_submitted_charge = Column(Numeric(scale=2, precision=18, asdecimal=False, decimal_return_scale=None))
    avg_submitted_charge = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    updated_at = Column(DateTime)


class PricingProcedurePeerStats(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure_peer_stats"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "procedure_code",
            "year",
            "geography_scope",
            "geography_value",
            "specialty_key",
            "setting_key",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "procedure_code",
        "year",
        "geography_scope",
        "geography_value",
        "specialty_key",
        "setting_key",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": ("procedure_code", "setting_key", "specialty_key", "geography_scope", "geography_value", "year"),
            "name": "pricing_proc_peer_stats_lookup_idx",
        },
        {
            "index_elements": ("geography_scope", "geography_value", "setting_key", "specialty_key", "year", "procedure_code"),
            "name": "pricing_proc_peer_stats_geo_idx",
        },
    ]

    procedure_code = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    geography_scope = Column(String(16), nullable=False)
    geography_value = Column(String(128), nullable=False)
    specialty_key = Column(String(128), nullable=False)
    setting_key = Column(String(64), nullable=False)
    provider_count = Column(Integer)
    min_claim_count = Column(Float)
    max_claim_count = Column(Float)
    p10 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p20 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p40 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p50 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p60 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p80 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    p90 = Column(Numeric(scale=4, precision=18, asdecimal=False, decimal_return_scale=None))
    updated_at = Column(DateTime)


class PricingProcedureGeoBenchmark(Base, JSONOutputMixin):
    __tablename__ = "pricing_procedure_geo_benchmark"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("procedure_code", "year", "geography_scope", "geography_value"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["procedure_code", "year", "geography_scope", "geography_value"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "procedure_code", "geography_scope"), "name": "pricing_proc_geo_year_proc_scope_idx"},
        {"index_elements": ("year", "geography_scope", "geography_value"), "name": "pricing_proc_geo_year_scope_value_idx"},
    ]

    procedure_code = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    geography_scope = Column(String(16), nullable=False)  # national | state
    geography_value = Column(String(16), nullable=False)  # US or 2-char state
    total_services = Column(Float)
    avg_submitted_charge = Column(Float)
    avg_payment_amount = Column(Float)
    avg_standardized_amount = Column(Float)
    updated_at = Column(DateTime)


class PricingPrescription(Base, JSONOutputMixin):
    __tablename__ = "pricing_prescription"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("rx_code_system", "rx_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["rx_code_system", "rx_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("rx_name",), "name": "pricing_prescription_name_idx"},
        {"index_elements": ("generic_name",), "name": "pricing_prescription_generic_idx"},
        {"index_elements": ("brand_name",), "name": "pricing_prescription_brand_idx"},
        {"index_elements": ("source_year",), "name": "pricing_prescription_source_year_idx"},
        {"index_elements": ("lower(rx_name)",), "name": "pricing_prescription_name_lower_idx"},
        {"index_elements": ("lower(generic_name)",), "name": "pricing_prescription_generic_lower_idx"},
        {"index_elements": ("lower(brand_name)",), "name": "pricing_prescription_brand_lower_idx"},
        {"index_elements": ("source_year", "lower(rx_name)"), "name": "pricing_prescription_year_name_lower_idx"},
    ]

    rx_code_system = Column(String(32), nullable=False)
    rx_code = Column(String(64), nullable=False)
    rx_name = Column(String)
    generic_name = Column(String)
    brand_name = Column(String)
    total_claims = Column(Float)
    total_30day_fills = Column(Float)
    total_day_supply = Column(Float)
    total_drug_cost = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_benes = Column(Float)
    source_year = Column(Integer)


class PricingProviderPrescription(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_prescription"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "rx_code_system", "rx_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "rx_code_system", "rx_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "rx_code_system", "rx_code"), "name": "pricing_provider_rx_year_code_idx"},
        {"index_elements": ("year", "rx_code_system", "rx_code", "npi"), "name": "pricing_provider_rx_year_code_npi_idx"},
        {"index_elements": ("year", "state", "city"), "name": "pricing_provider_rx_year_state_city_idx"},
        {"index_elements": ("year", "state", "city", "provider_type"), "name": "pricing_provider_rx_year_state_city_type_idx"},
        {"index_elements": ("rx_name",), "name": "pricing_provider_rx_name_idx"},
        {"index_elements": ("generic_name",), "name": "pricing_provider_rx_generic_idx"},
        {"index_elements": ("brand_name",), "name": "pricing_provider_rx_brand_idx"},
        {"index_elements": ("year", "lower(rx_name)"), "name": "pricing_provider_rx_year_name_lower_idx"},
        {"index_elements": ("year", "lower(generic_name)"), "name": "pricing_provider_rx_year_generic_lower_idx"},
        {"index_elements": ("year", "lower(brand_name)"), "name": "pricing_provider_rx_year_brand_lower_idx"},
        {"index_elements": ("year", "npi", "total_drug_cost DESC"), "name": "pricing_provider_rx_year_npi_total_drug_cost_desc_idx"},
        {"index_elements": ("year", "total_drug_cost DESC"), "name": "pricing_provider_rx_year_total_drug_cost_desc_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    rx_code_system = Column(String(32), nullable=False)
    rx_code = Column(String(64), nullable=False)
    rx_name = Column(String)
    generic_name = Column(String)
    brand_name = Column(String)
    provider_name = Column(String)
    provider_type = Column(String)
    city = Column(String)
    state = Column(String(2))
    zip5 = Column(String(5))
    country = Column(String)
    total_claims = Column(Float)
    total_30day_fills = Column(Float)
    total_day_supply = Column(Float)
    total_drug_cost = Column(Numeric(scale=2, precision=16, asdecimal=False, decimal_return_scale=None))
    total_benes = Column(Float)
    ge65_total_claims = Column(Float)
    ge65_total_30day_fills = Column(Float)
    ge65_total_day_supply = Column(Float)
    ge65_total_drug_cost = Column(Float)
    ge65_total_benes = Column(Float)


class PricingQppProvider(Base, JSONOutputMixin):
    __tablename__ = "pricing_qpp_provider"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "npi"), "name": "pricing_qpp_year_npi_idx"},
        {"index_elements": ("year", "final_score DESC"), "name": "pricing_qpp_year_final_score_desc_idx"},
        {"index_elements": ("year", "quality_score DESC"), "name": "pricing_qpp_year_quality_score_desc_idx"},
        {"index_elements": ("year", "cost_score DESC"), "name": "pricing_qpp_year_cost_score_desc_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    quality_score = Column(Float)
    cost_score = Column(Float)
    final_score = Column(Float)
    raw_json_text = Column(TEXT)
    source = Column(String(128))
    updated_at = Column(DateTime)


class PricingSviZcta(Base, JSONOutputMixin):
    __tablename__ = "pricing_svi_zcta"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zcta", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["zcta", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "zcta"), "name": "pricing_svi_year_zcta_idx"},
        {"index_elements": ("year", "svi_overall"), "name": "pricing_svi_year_overall_idx"},
    ]

    zcta = Column(String(5), nullable=False)
    year = Column(Integer, nullable=False)
    svi_overall = Column(Float)
    svi_socioeconomic = Column(Float)
    svi_household = Column(Float)
    svi_minority = Column(Float)
    svi_housing = Column(Float)
    source = Column(String(128))
    updated_at = Column(DateTime)


class PricingPlacesZcta(Base, JSONOutputMixin):
    __tablename__ = "pricing_places_zcta"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zcta", "year", "measure_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["zcta", "year", "measure_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "zcta"), "name": "pricing_places_year_zcta_idx"},
        {"index_elements": ("year", "measure_id"), "name": "pricing_places_year_measure_idx"},
        {"index_elements": ("zcta", "measure_id", "year"), "name": "pricing_places_zcta_measure_year_idx"},
    ]

    zcta = Column(String(5), nullable=False)
    year = Column(Integer, nullable=False)
    measure_id = Column(String(64), nullable=False)
    measure_name = Column(String(256))
    data_value = Column(Float)
    low_ci = Column(Float)
    high_ci = Column(Float)
    data_value_type = Column(String(128))
    source = Column(String(128))
    updated_at = Column(DateTime)


class PricingProviderQualityFeature(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_feature"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year",), "name": "pricing_quality_feature_year_idx"},
        {"index_elements": ("year", "state"), "name": "pricing_quality_feature_year_state_idx"},
        {"index_elements": ("year", "zip5"), "name": "pricing_quality_feature_year_zip5_idx"},
        {"index_elements": ("year", "provider_class"), "name": "pricing_quality_feature_year_provider_class_idx"},
        {"index_elements": ("year", "specialty_key"), "name": "pricing_quality_feature_year_specialty_idx"},
        {"index_elements": ("year", "taxonomy_code"), "name": "pricing_quality_feature_year_taxonomy_code_idx"},
        {"index_elements": ("year", "taxonomy_classification"), "name": "pricing_quality_feature_year_taxonomy_class_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    zip5 = Column(String(5))
    state = Column(String(2))
    provider_class = Column(String(32))
    location_source = Column(String(64))
    has_enrollment = Column(Boolean)
    has_medicare_claims = Column(Boolean)
    specialty_key = Column(String(128))
    taxonomy_code = Column(String(32))
    taxonomy_classification = Column(String)
    procedure_count = Column(Integer)
    updated_at = Column(DateTime)


class PricingProviderQualityProcedureLSH(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_procedure_lsh"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "band_no"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "band_no"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "band_no", "band_hash"),
            "name": "pricing_quality_proc_lsh_year_band_hash_idx",
        },
        {"index_elements": ("year", "npi"), "name": "pricing_quality_proc_lsh_year_npi_idx"},
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    band_no = Column(Integer, nullable=False)
    band_hash = Column(String(128))
    updated_at = Column(DateTime)


class PricingProviderQualityPeerTarget(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_peer_target"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint(
            "year",
            "benchmark_mode",
            "geography_scope",
            "geography_value",
            "cohort_level",
            "specialty_key",
            "taxonomy_code",
            "procedure_bucket",
        ),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = [
        "year",
        "benchmark_mode",
        "geography_scope",
        "geography_value",
        "cohort_level",
        "specialty_key",
        "taxonomy_code",
        "procedure_bucket",
    ]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "benchmark_mode", "geography_scope", "geography_value", "cohort_level"),
            "name": "pricing_quality_peer_target_year_mode_geo_cohort_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "cohort_level", "specialty_key", "taxonomy_code", "procedure_bucket"),
            "name": "pricing_quality_peer_target_year_mode_cohort_target_idx",
        },
    ]

    year = Column(Integer, nullable=False)
    benchmark_mode = Column(String(32), nullable=False)
    geography_scope = Column(String(16), nullable=False)
    geography_value = Column(String(128), nullable=False)
    cohort_level = Column(String(32), nullable=False)
    specialty_key = Column(String(128), nullable=False)
    taxonomy_code = Column(String(32), nullable=False)
    procedure_bucket = Column(String(64), nullable=False)
    peer_n = Column(Integer)
    target_appropriateness = Column(Float)
    target_effectiveness = Column(Float)
    target_cost = Column(Float)
    target_qpp_cost = Column(Float)
    target_rx_appropriateness = Column(Float)
    updated_at = Column(DateTime)


class PricingProviderQualityMeasure(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_measure"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "benchmark_mode", "measure_id", "domain"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "benchmark_mode", "measure_id", "domain"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "benchmark_mode", "domain", "measure_id"),
            "name": "pricing_quality_measure_year_mode_domain_measure_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "npi", "domain"),
            "name": "pricing_quality_measure_year_mode_npi_domain_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "peer_group", "domain"),
            "name": "pricing_quality_measure_year_mode_peer_domain_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "domain", "risk_ratio"),
            "name": "pricing_quality_measure_year_mode_domain_rr_idx",
        },
        {
            "index_elements": (
                "year",
                "benchmark_mode",
                "cohort_level",
                "cohort_specialty_key",
                "cohort_taxonomy_code",
                "cohort_procedure_bucket",
            ),
            "name": "pricing_quality_measure_year_mode_cohort_idx",
        },
        {
            "index_elements": (
                "year",
                "benchmark_mode",
                "npi",
                "cohort_level",
                "cohort_specialty_key",
                "cohort_taxonomy_code",
                "cohort_procedure_bucket",
            ),
            "name": "pricing_quality_measure_year_mode_npi_cohort_idx",
        },
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    benchmark_mode = Column(String(32), nullable=False)
    measure_id = Column(String(128), nullable=False)
    domain = Column(String(32), nullable=False)
    observed = Column(Float)
    target = Column(Float)
    risk_ratio = Column(Float)
    ci75_low = Column(Float)
    ci75_high = Column(Float)
    ci90_low = Column(Float)
    ci90_high = Column(Float)
    evidence_n = Column(Integer)
    peer_group = Column(String(128))
    cohort_level = Column(String(32))
    cohort_specialty_key = Column(String(128))
    cohort_taxonomy_code = Column(String(32))
    cohort_procedure_bucket = Column(String(64))
    cohort_peer_n = Column(Integer)
    direction = Column(String(16))
    source = Column(String(128))
    updated_at = Column(DateTime)


class PricingProviderQualityDomain(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_domain"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "benchmark_mode", "domain"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "benchmark_mode", "domain"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "benchmark_mode", "domain", "score_0_100 DESC"),
            "name": "pricing_quality_domain_year_mode_domain_score_desc_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "domain", "risk_ratio"),
            "name": "pricing_quality_domain_year_mode_domain_rr_idx",
        },
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    benchmark_mode = Column(String(32), nullable=False)
    domain = Column(String(32), nullable=False)
    risk_ratio = Column(Float)
    score_0_100 = Column(Float)
    ci75_low = Column(Float)
    ci75_high = Column(Float)
    ci90_low = Column(Float)
    ci90_high = Column(Float)
    evidence_n = Column(Integer)
    measure_count = Column(Integer)
    updated_at = Column(DateTime)


class PricingProviderQualityScore(Base, JSONOutputMixin):
    __tablename__ = "pricing_provider_quality_score"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "year", "benchmark_mode"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "year", "benchmark_mode"]
    __my_additional_indexes__ = [
        {
            "index_elements": ("year", "benchmark_mode", "tier", "score_0_100 DESC"),
            "name": "pricing_quality_score_year_mode_tier_score_desc_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "risk_ratio_point"),
            "name": "pricing_quality_score_year_mode_rr_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "borderline_status"),
            "name": "pricing_quality_score_year_mode_borderline_idx",
        },
        {"index_elements": ("run_id",), "name": "pricing_quality_score_run_id_idx"},
        {"index_elements": ("year", "benchmark_mode", "model_version"), "name": "pricing_quality_score_year_benchmark_model_idx"},
        {
            "index_elements": (
                "year",
                "benchmark_mode",
                "cohort_level",
                "cohort_specialty_key",
                "cohort_taxonomy_code",
                "cohort_procedure_bucket",
            ),
            "name": "pricing_quality_score_year_mode_cohort_idx",
        },
        {
            "index_elements": ("year", "benchmark_mode", "score_method", "confidence_band"),
            "name": "pricing_quality_score_year_mode_method_conf_idx",
        },
    ]

    npi = Column(BigInteger, nullable=False)
    year = Column(Integer, nullable=False)
    model_version = Column(String(32))
    benchmark_mode = Column(String(32), nullable=False)
    cohort_level = Column(String(32))
    cohort_specialty_key = Column(String(128))
    cohort_taxonomy_code = Column(String(32))
    cohort_procedure_bucket = Column(String(64))
    cohort_peer_n = Column(Integer)
    cohort_geography_scope = Column(String(16))
    cohort_geography_value = Column(String(128))
    risk_ratio_point = Column(Float)
    ci75_low = Column(Float)
    ci75_high = Column(Float)
    ci90_low = Column(Float)
    ci90_high = Column(Float)
    score_0_100 = Column(Float)
    tier = Column(String(32))
    borderline_status = Column(Boolean)
    low_score_threshold_failed = Column(Boolean)
    low_confidence_threshold_failed = Column(Boolean)
    high_score_threshold_passed = Column(Boolean)
    high_confidence_threshold_passed = Column(Boolean)
    estimated_cost_level = Column(String(5))
    score_method = Column(String(32))
    cost_source = Column(String(32))
    confidence_0_100 = Column(Float)
    confidence_band = Column(String(16))
    data_coverage_0_100 = Column(Float)
    provider_class = Column(String(32))
    location_source = Column(String(64))
    has_claims = Column(Boolean)
    has_qpp = Column(Boolean)
    has_rx = Column(Boolean)
    has_enrollment = Column(Boolean)
    has_medicare_claims = Column(Boolean)
    run_id = Column(String(64))
    updated_at = Column(DateTime)


class PricingQualityRun(Base, JSONOutputMixin):
    __tablename__ = "pricing_quality_run"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "status"), "name": "pricing_quality_run_year_status_idx"},
        {"index_elements": ("created_at",), "name": "pricing_quality_run_created_at_idx"},
        {"index_elements": ("updated_at",), "name": "pricing_quality_run_updated_at_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    import_id = Column(String(32))
    year = Column(Integer)
    qpp_source_version = Column(String(128))
    svi_source_version = Column(String(128))
    status = Column(String(32))
    qpp_rows = Column(Integer)
    svi_rows = Column(Integer)
    measure_rows = Column(Integer)
    domain_rows = Column(Integer)
    score_rows = Column(Integer)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    created_at = Column(DateTime)
    updated_at = Column(DateTime)


class ProviderEnrollmentBase(Base, JSONOutputMixin):
    __abstract__ = True

    @declared_attr
    def __table_args__(cls):
        return (
            PrimaryKeyConstraint("record_hash"),
            {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
        )

    __my_index_elements__ = ["record_hash"]
    __my_initial_indexes__ = []
    __my_additional_indexes__ = [
        {"index_elements": ("npi", "reporting_year"), "name": "npi_year"},
        {"index_elements": ("state", "reporting_year"), "name": "state_year"},
        {"index_elements": ("provider_type_code", "reporting_year"), "name": "provider_type_year"},
        {"index_elements": ("ccn",), "name": "ccn"},
        {"index_elements": ("enrollment_id",), "name": "enrollment_id"},
    ]

    record_hash = Column(Integer, nullable=False)
    npi = Column(BigInteger, nullable=False)
    reporting_period_start = Column(DATE)
    reporting_period_end = Column(DATE)
    reporting_year = Column(Integer)
    enrollment_id = Column(String(64))
    enrollment_state = Column(String(2))
    provider_type_code = Column(String(32))
    provider_type_text = Column(String)
    multiple_npi_flag = Column(String(16))
    ccn = Column(String(32))
    associate_id = Column(String(64))
    organization_name = Column(String)
    doing_business_as_name = Column(String)
    incorporation_date = Column(DATE)
    incorporation_state = Column(String(2))
    organization_type_structure = Column(String)
    organization_other_type_text = Column(String)
    proprietary_nonprofit = Column(String(64))
    address_line_1 = Column(String)
    address_line_2 = Column(String)
    city = Column(String)
    state = Column(String(2))
    zip_code = Column(String(12))
    source_dataset_title = Column(String(255))
    source_distribution_title = Column(String(255))
    source_url = Column(String)
    source_modified = Column(DateTime)
    source_temporal = Column(String(64))
    imported_at = Column(DateTime)


class ProviderEnrollmentFFS(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_ffs"
    __main_table__ = __tablename__
    __my_additional_indexes__ = ProviderEnrollmentBase.__my_additional_indexes__ + [
        {"index_elements": ("pecos_asct_cntl_id",), "name": "pecos_asct_cntl_id"},
    ]

    pecos_asct_cntl_id = Column(String(64))
    first_name = Column(String)
    middle_name = Column(String)
    last_name = Column(String)
    org_name = Column(String)


class ProviderEnrollmentFFSLinkedBase(Base, JSONOutputMixin):
    __abstract__ = True

    @declared_attr
    def __table_args__(cls):
        return (
            PrimaryKeyConstraint("record_hash"),
            {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
        )

    __my_index_elements__ = ["record_hash"]
    __my_initial_indexes__ = []
    __my_additional_indexes__ = [
        {"index_elements": ("enrollment_id",), "name": "enrollment_id"},
        {"index_elements": ("reporting_year", "enrollment_id"), "name": "year_enrollment"},
    ]

    record_hash = Column(Integer, nullable=False)
    enrollment_id = Column(String(64), nullable=False)
    reporting_period_start = Column(DATE)
    reporting_period_end = Column(DATE)
    reporting_year = Column(Integer)
    source_dataset_title = Column(String(255))
    source_distribution_title = Column(String(255))
    source_url = Column(String)
    source_modified = Column(DateTime)
    source_temporal = Column(String(64))
    imported_at = Column(DateTime)


class ProviderEnrollmentFFSAdditionalNPI(ProviderEnrollmentFFSLinkedBase):
    __tablename__ = "provider_enrollment_ffs_additional_npi"
    __main_table__ = __tablename__
    __my_additional_indexes__ = ProviderEnrollmentFFSLinkedBase.__my_additional_indexes__ + [
        {"index_elements": ("additional_npi",), "name": "additional_npi"},
    ]

    additional_npi = Column(BigInteger, nullable=False)


class ProviderEnrollmentFFSAddress(ProviderEnrollmentFFSLinkedBase):
    __tablename__ = "provider_enrollment_ffs_address"
    __main_table__ = __tablename__
    __my_additional_indexes__ = ProviderEnrollmentFFSLinkedBase.__my_additional_indexes__ + [
        {"index_elements": ("state", "zip_code"), "name": "state_zip"},
        {"index_elements": ("zip_code",), "name": "zip_code"},
    ]

    city = Column(String)
    state = Column(String(2))
    zip_code = Column(String(12))


class ProviderEnrollmentFFSSecondarySpecialty(ProviderEnrollmentFFSLinkedBase):
    __tablename__ = "provider_enrollment_ffs_secondary_specialty"
    __main_table__ = __tablename__
    __my_additional_indexes__ = ProviderEnrollmentFFSLinkedBase.__my_additional_indexes__ + [
        {"index_elements": ("provider_type_code",), "name": "provider_type_code"},
    ]

    provider_type_code = Column(String(32), nullable=False)
    provider_type_text = Column(String)


class ProviderEnrollmentFFSReassignment(Base, JSONOutputMixin):
    __tablename__ = "provider_enrollment_ffs_reassignment"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("record_hash"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["record_hash"]
    __my_initial_indexes__ = []
    __my_additional_indexes__ = [
        {"index_elements": ("reassigning_enrollment_id",), "name": "reassigning_enrollment_id"},
        {"index_elements": ("receiving_enrollment_id",), "name": "receiving_enrollment_id"},
        {"index_elements": ("reporting_year", "reassigning_enrollment_id"), "name": "year_reassigning"},
        {"index_elements": ("reporting_year", "receiving_enrollment_id"), "name": "year_receiving"},
    ]

    record_hash = Column(Integer, nullable=False)
    reassigning_enrollment_id = Column(String(64), nullable=False)
    receiving_enrollment_id = Column(String(64), nullable=False)
    reporting_period_start = Column(DATE)
    reporting_period_end = Column(DATE)
    reporting_year = Column(Integer)
    source_dataset_title = Column(String(255))
    source_distribution_title = Column(String(255))
    source_url = Column(String)
    source_modified = Column(DateTime)
    source_temporal = Column(String(64))
    imported_at = Column(DateTime)


class ProviderEnrollmentHospital(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_hospital"
    __main_table__ = __tablename__

    practice_location_type = Column(String(128))
    location_other_type_text = Column(String)
    subgroup_general = Column(String(32))
    subgroup_acute_care = Column(String(32))
    subgroup_alcohol_drug = Column(String(32))
    subgroup_childrens = Column(String(32))
    subgroup_long_term = Column(String(32))
    subgroup_psychiatric = Column(String(32))
    subgroup_rehabilitation = Column(String(32))
    subgroup_short_term = Column(String(32))
    subgroup_swing_bed_approved = Column(String(32))
    subgroup_psychiatric_unit = Column(String(32))
    subgroup_rehabilitation_unit = Column(String(32))
    subgroup_specialty_hospital = Column(String(32))
    subgroup_other = Column(String(32))
    subgroup_other_text = Column(String)
    reh_conversion_flag = Column(String(32))
    reh_conversion_date = Column(DATE)
    cah_or_hospital_ccn = Column(String(32))


class ProviderEnrollmentHomeHealthAgency(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_hha"
    __main_table__ = __tablename__

    practice_location_type = Column(String(128))
    location_other_type_text = Column(String)


class ProviderEnrollmentHospice(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_hospice"
    __main_table__ = __tablename__


class ProviderEnrollmentFQHC(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_fqhc"
    __main_table__ = __tablename__

    telephone_number = Column(String(32))


class ProviderEnrollmentRHC(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_rhc"
    __main_table__ = __tablename__

    telephone_number = Column(String(32))


class ProviderEnrollmentSNF(ProviderEnrollmentBase):
    __tablename__ = "provider_enrollment_snf"
    __main_table__ = __tablename__

    nursing_home_provider_name = Column(String)
    affiliation_entity_name = Column(String)
    affiliation_entity_id = Column(String(64))


class ProviderEnrichmentSummary(Base, JSONOutputMixin):
    __tablename__ = "provider_enrichment_summary"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi"]
    __my_initial_indexes__ = [
        {"index_elements": ("latest_reporting_year",)},
        {"index_elements": ("status",)},
    ]
    __my_additional_indexes__ = [
        {"index_elements": ("primary_state", "latest_reporting_year"), "name": "state_year"},
        {"index_elements": ("has_any_enrollment", "latest_reporting_year"), "name": "has_any_year"},
        {"index_elements": ("has_medicare_claims", "latest_reporting_year"), "name": "has_claims_year"},
        {"index_elements": ("has_ffs_enrollment",), "name": "has_ffs"},
        {"index_elements": ("has_hospital_enrollment",), "name": "has_hospital"},
        {"index_elements": ("has_hha_enrollment",), "name": "has_hha"},
        {"index_elements": ("has_hospice_enrollment",), "name": "has_hospice"},
        {"index_elements": ("has_fqhc_enrollment",), "name": "has_fqhc"},
        {"index_elements": ("has_rhc_enrollment",), "name": "has_rhc"},
        {"index_elements": ("has_snf_enrollment",), "name": "has_snf"},
        {"index_elements": ("dataset_keys",), "using": "gin", "name": "dataset_keys_gin"},
        {"index_elements": ("states",), "using": "gin", "name": "states_gin"},
        {"index_elements": ("provider_type_codes",), "using": "gin", "name": "provider_type_codes_gin"},
    ]

    npi = Column(BigInteger, nullable=False)
    latest_reporting_year = Column(Integer)
    has_any_enrollment = Column(Boolean)
    has_ffs_enrollment = Column(Boolean)
    has_hospital_enrollment = Column(Boolean)
    has_hha_enrollment = Column(Boolean)
    has_hospice_enrollment = Column(Boolean)
    has_fqhc_enrollment = Column(Boolean)
    has_rhc_enrollment = Column(Boolean)
    has_snf_enrollment = Column(Boolean)
    has_medicare_claims = Column(Boolean)
    medicare_claim_year_min = Column(Integer)
    medicare_claim_year_max = Column(Integer)
    medicare_claim_rows = Column(Integer)
    total_enrollment_rows = Column(Integer)
    dataset_keys = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    states = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    provider_type_codes = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    provider_type_texts = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_enrollment_ids = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_pecos_asct_cntl_ids = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_secondary_provider_type_codes = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_secondary_provider_type_texts = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_practice_zip_codes = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_practice_cities = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_practice_states = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    ffs_related_npis = Column(ARRAY(BigInteger), nullable=False, server_default=text("ARRAY[]::bigint[]"))
    ffs_related_npi_count = Column(Integer)
    ffs_reassignment_in_count = Column(Integer)
    ffs_reassignment_out_count = Column(Integer)
    primary_state = Column(String(2))
    primary_provider_type_code = Column(String(32))
    primary_provider_type_text = Column(String)
    status = Column(String(32))
    nppes_unmapped_field_count = Column(Integer)
    nppes_medical_school_fields = Column(ARRAY(String), nullable=False, server_default=text("ARRAY[]::varchar[]"))
    updated_at = Column(DateTime)


class CodeCatalog(Base, JSONOutputMixin):
    __tablename__ = "code_catalog"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("code_system", "code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["code_system", "code"]
    __my_additional_indexes__ = [
        {"index_elements": ("code", "code_system"), "name": "code_catalog_code_system_idx"},
        {"index_elements": ("code_checksum",), "name": "code_catalog_code_checksum_idx"},
        {"index_elements": ("code_system", "display_name"), "name": "code_catalog_system_display_idx"},
        {"index_elements": ("code_system", "lower(display_name)"), "name": "code_catalog_system_display_lower_idx"},
        {"index_elements": ("lower(display_name)",), "name": "code_catalog_display_lower_idx"},
        {"index_elements": ("lower(short_description)",), "name": "code_catalog_short_description_lower_idx"},
        {"index_elements": ("source",), "name": "code_catalog_source_idx"},
        {"index_elements": ("code_system", "source"), "name": "code_catalog_system_source_idx"},
        {"index_elements": ("source", "code_system", "lower(display_name)"), "name": "code_catalog_source_system_display_lower_idx"},
    ]

    code_system = Column(String(32), nullable=False)
    code = Column(String(128), nullable=False)
    code_checksum = Column(Integer)
    display_name = Column(String)
    short_description = Column(String)
    long_description = Column(TEXT)
    is_active = Column(Boolean)
    source = Column(String(128))
    updated_at = Column(DateTime)


class CodeCrosswalk(Base, JSONOutputMixin):
    __tablename__ = "code_crosswalk"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("from_system", "from_code", "to_system", "to_code"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["from_system", "from_code", "to_system", "to_code"]
    __my_additional_indexes__ = [
        {"index_elements": ("from_system", "from_code"), "name": "code_crosswalk_from_idx"},
        {"index_elements": ("from_checksum",), "name": "code_crosswalk_from_checksum_idx"},
        {"index_elements": ("to_system", "to_code"), "name": "code_crosswalk_to_idx"},
        {"index_elements": ("to_checksum",), "name": "code_crosswalk_to_checksum_idx"},
        {"index_elements": ("upper(from_system)", "upper(from_code)"), "name": "code_crosswalk_upper_from_idx"},
        {"index_elements": ("upper(to_system)", "upper(to_code)"), "name": "code_crosswalk_upper_to_idx"},
    ]

    from_system = Column(String(32), nullable=False)
    from_code = Column(String(128), nullable=False)
    from_checksum = Column(Integer)
    to_system = Column(String(32), nullable=False)
    to_code = Column(String(128), nullable=False)
    to_checksum = Column(Integer)
    match_type = Column(String(32))
    confidence = Column(Numeric(scale=4, precision=6, asdecimal=False, decimal_return_scale=None))
    source = Column(String(128))
    updated_at = Column(DateTime)


class LODESWorkplaceAggregate(Base, JSONOutputMixin):
    __tablename__ = "lodes_workplace_aggregate"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zcta_code", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["zcta_code", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "zcta_code"), "name": "lodes_year_zcta_idx"},
    ]

    zcta_code = Column(String(5), nullable=False)
    total_workers = Column(Integer, nullable=False, default=0)
    year = Column(Integer, nullable=False)
    updated_at = Column(DateTime)


class MedicareEnrollmentCountyStats(Base, JSONOutputMixin):
    __tablename__ = "medicare_enrollment_county_stats"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("county_fips", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["county_fips", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "county_fips"), "name": "medicare_county_year_fips_idx"},
    ]

    county_fips = Column(String(5), nullable=False)
    year = Column(Integer, nullable=False)
    part_d_beneficiaries = Column(Integer, nullable=False, default=0)
    total_beneficiaries = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime)


class MedicareEnrollmentStats(Base, JSONOutputMixin):
    __tablename__ = "medicare_enrollment_stats"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("zcta_code", "year"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["zcta_code", "year"]
    __my_additional_indexes__ = [
        {"index_elements": ("year", "zcta_code"), "name": "medicare_zip_year_zcta_idx"},
    ]

    zcta_code = Column(String(5), nullable=False)
    year = Column(Integer, nullable=False)
    part_d_beneficiaries = Column(Integer, nullable=False, default=0)
    total_beneficiaries = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime)


class DoctorClinicianAddress(Base, JSONOutputMixin):
    __tablename__ = "doctor_clinician_address"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("npi", "address_checksum"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["npi", "address_checksum"]
    __my_additional_indexes__ = [
        {"index_elements": ("zip_code", "provider_type"), "name": "zip_provider_type"},
        {"index_elements": ("zip_code",), "name": "zip"},
        {"index_elements": ("state", "city"), "name": "state_city"},
        {"index_elements": ("provider_type",), "name": "provider_type"},
    ]

    npi = Column(BigInteger, nullable=False)
    address_checksum = Column(Integer, nullable=False)
    address_line1 = Column(String(256))
    address_line2 = Column(String(256))
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    provider_type = Column(String(128))
    latitude = Column(Float)
    longitude = Column(Float)
    updated_at = Column(DateTime)


class FacilityAnchor(Base, JSONOutputMixin):
    __tablename__ = "facility_anchor"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["id"]
    __my_additional_indexes__ = [
        {"index_elements": ("facility_type", "latitude", "longitude"), "name": "type_geo"},
        {"index_elements": ("state", "zip_code", "facility_type"), "name": "state_zip_type"},
    ]

    id = Column(String(128), nullable=False)
    name = Column(String(256), nullable=False)
    facility_type = Column(String(64), nullable=False)
    address_line1 = Column(String(256))
    city = Column(String(128))
    state = Column(String(2))
    zip_code = Column(String(16))
    latitude = Column(Float)
    longitude = Column(Float)
    source_dataset = Column(String(64), nullable=False)
    updated_at = Column(DateTime)


class EntityAddressUnified(Base, JSONOutputMixin):
    __tablename__ = "entity_address_unified"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("entity_type", "entity_id", "type", "checksum"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["entity_type", "entity_id", "type", "checksum"]
    __my_additional_indexes__ = [
        {"index_elements": ("npi",), "name": "npi"},
        {"index_elements": ("inferred_npi",), "name": "inferred_npi"},
        {"index_elements": ("coalesce(npi, inferred_npi)",), "name": "coalesced_npi"},
        {"index_elements": ("entity_type", "coalesce(npi, inferred_npi)"), "name": "entity_type_coalesced_npi"},
        {"index_elements": ("type", "state_name", "city_name"), "name": "type_state_city"},
        {"index_elements": ("LEFT(postal_code, 5)",), "name": "postal_code_5"},
        {"index_elements": ("address_sources",), "using": "gin", "name": "address_sources"},
        {
            "index_elements": ("taxonomy_array gin__int_ops", "plans_network_array gin__int_ops"),
            "using": "gin",
            "name": "taxonomy_plans_network",
        },
        {"index_elements": ("procedures_array gin__int_ops",), "using": "gin", "name": "procedures_array"},
        {"index_elements": ("medications_array gin__int_ops",), "using": "gin", "name": "medications_array"},
        {
            "index_elements": ("Geography(ST_MakePoint((long)::double precision, (lat)::double precision))",),
            "using": "gist",
            "name": "geo_idx",
            "where": "type='primary' OR type='secondary' OR type='practice' OR type='site'",
        },
    ]

    entity_type = Column(String(64), nullable=False)
    entity_id = Column(String(128), nullable=False)
    npi = Column(BigInteger)
    inferred_npi = Column(BigInteger)
    inference_confidence = Column(Float)
    inference_method = Column(String(64))
    entity_name = Column(String(256))
    entity_subtype = Column(String(64))
    source_count = Column(Integer, nullable=False, server_default="0")
    multi_source_confirmed = Column(Boolean, nullable=False, server_default="false")
    address_sources = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))
    source_record_ids = Column(ARRAY(String), nullable=False, server_default=text("'{}'::varchar[]"))

    checksum = Column(Integer, nullable=False)
    type = Column(String(32), nullable=False)
    taxonomy_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    plans_network_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    procedures_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")
    medications_array = Column(ARRAY(Integer), nullable=False, server_default="{0}")

    first_line = Column(String)
    second_line = Column(String)
    city_name = Column(String)
    state_name = Column(String)
    postal_code = Column(String)
    country_code = Column(String)
    telephone_number = Column(String)
    fax_number = Column(String)
    formatted_address = Column(String)
    lat = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    long = Column(Numeric(scale=8, precision=11, asdecimal=False, decimal_return_scale=None))
    date_added = Column(DATE)
    place_id = Column(String)
    updated_at = Column(DateTime)


class PharmacyEconomicsSummary(Base, JSONOutputMixin):
    __tablename__ = "pharmacy_economics_summary"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("state", "ndc11"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["state", "ndc11"]
    __my_additional_indexes__ = [
        {"index_elements": ("state", "sdud_volume DESC"), "name": "state_sdud_volume_desc"},
        {
            "index_elements": ("state", "estimated_gross_margin"),
            "name": "state_margin",
            "where": "estimated_gross_margin IS NOT NULL",
        },
    ]

    state = Column(String(2), nullable=False)
    ndc11 = Column(String(16), nullable=False)
    drug_name = Column(String(256))
    sdud_volume = Column(Integer, nullable=False, default=0)
    nadac_per_unit = Column(Float)
    ful_per_unit = Column(Float)
    medicaid_dispensing_fee = Column(Float)
    estimated_gross_margin = Column(Float)
    updated_at = Column(DateTime)
