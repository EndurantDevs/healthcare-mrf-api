# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Florida MQA practitioner-profile acquisition and canonical fact import."""

from __future__ import annotations

import asyncio
import csv
import hashlib
import html
import io
import json
import os
import re
import shutil
import sys
import uuid
import zipfile
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any, AsyncIterator, Iterable, Iterator, Mapping
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
from urllib.request import HTTPCookieProcessor, Request, build_opener

import click
from dotenv import load_dotenv
from sqlalchemy import (
    ARRAY,
    JSON as SQLAlchemyJSON,
    Date,
    DateTime,
    MetaData,
    func,
    select,
    text,
)

from db.models import (
    ProviderProfileArtifact,
    ProviderProfileFact,
    ProviderProfileImportRun,
    ProviderProfileProjection,
    ProviderProfileSourceRecord,
    db,
)
from process.live_progress import enqueue_live_progress
from process.provider_profile_reported_range import normalize_reported_range

PROFILE_SCHEMA_VERSION = "provider-profile/v1"
SOURCE_RECORD_IDENTITY_VERSION = "source-field-row-sha256/v1"
FL_MQA_SOURCE_KEY = "florida-mqa"
FL_MQA_AGENCY = "Florida Department of Health, Medical Quality Assurance"
DEFAULT_BASE_URL = "https://data-download.mqa.flhealthsource.gov"
DEFAULT_MIN_FIRST_PUBLISH_PROVIDERS = 100
DEFAULT_MIN_PUBLISH_RATIO = 0.80
DEFAULT_FAILED_RUN_RETENTION_DAYS = 7
DEFAULT_MAX_QUARANTINED_ROWS_PER_SOURCE = 100
DEFAULT_MAX_QUARANTINED_ROW_RATIO = 0.001
DEFAULT_COPY_UPSERT_MIN_ROWS = 500
DEFAULT_COPY_UPSERT_BATCH_ROWS = 5_000
DEFAULT_FAILURE_STATUS_ATTEMPTS = 8
DEFAULT_FAILURE_STATUS_TIMEOUT_SECONDS = 10.0
DEFAULT_FAILURE_STATUS_WINDOW_SECONDS = 90.0
MAX_FAILURE_STATUS_RETRY_DELAY_SECONDS = 15.0

_POSTGRES_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_POSTGRES_IDENTIFIER_MAX_BYTES = 63
_COPY_UPSERT_TABLES = frozenset(
    {
        ProviderProfileSourceRecord.__tablename__,
        ProviderProfileFact.__tablename__,
    }
)
_TRANSIENT_DATABASE_ERROR_NAMES = frozenset(
    {
        "CannotConnectNowError",
        "ConnectionDoesNotExistError",
        "ConnectionRefusedError",
        "ConnectionResetError",
        "InterfaceError",
        "OperationalError",
    }
)


class _CopyUpsertUnavailable(RuntimeError):
    """Signal that the active database driver cannot perform binary COPY."""


_csv_field_limit = sys.maxsize
while True:
    try:
        csv.field_size_limit(_csv_field_limit)
        break
    except OverflowError:
        _csv_field_limit //= 10


STANDARD_CATEGORIES = (
    "identity",
    "demographics",
    "contact",
    "locations",
    "specialties",
    "services",
    "organizations",
    "network_participation",
    "accepting_patients",
    "telehealth",
    "licenses",
    "professional_experience",
    "education",
    "training",
    "certifications",
    "privileges",
    "academic_appointments",
    "affiliations",
    "memberships",
    "languages",
    "honors",
    "publications",
    "financial_responsibility",
    "criminal_disclosures",
    "regulatory_actions",
    "complaints",
    "liability_claims",
    "prescribing_authorizations",
    "pharmacy_relationships",
    "program_reports",
)


@dataclass(frozen=True)
class FloridaSource:
    key: str
    path: str
    filename: str
    category: str
    fact_type: str
    title: str
    label_fields: tuple[str, ...]
    assertion_type: str = "self_reported"
    verification_status: str = "not_independently_verified"
    sensitive: bool = False
    public_default: bool = True
    required_fields: tuple[str, ...] = ()
    expected_fields: tuple[str, ...] = ()
    has_header: bool = True

    @property
    def url(self) -> str:
        """Return the authenticated portal URL for this source."""
        if "handler=" in self.path:
            return self.path
        separator = "&" if "?" in self.path else "?"
        return f"{self.path}{separator}{urlencode({'fileName': self.filename, 'handler': 'DownloadDataFile'})}"


def _profile_source(
    key: str,
    filename: str,
    category: str,
    fact_type: str,
    title: str,
    *label_fields: str,
    **kwargs: Any,
) -> FloridaSource:
    required_fields = tuple(kwargs.pop("required_fields", ("pro_cde", "lic_id")))
    expected_fields = tuple(
        kwargs.pop("expected_fields", _PROFILE_EXPECTED_FIELDS.get(key, ()))
    )
    return FloridaSource(
        key,
        "/ProfileData",
        filename,
        category,
        fact_type,
        title,
        tuple(label_fields),
        required_fields=required_fields,
        expected_fields=expected_fields,
        **kwargs,
    )


_PROFILE_MASTER_REQUIRED_FIELDS = (
    "pro_cde", "lic_id", "lic_nbr", "l_name", "f_name", "m_name",
    "name_suffix", "birth_year_range", "ml_addr_line1", "ml_addr_line2",
    "ml_addr_line3", "ml_addr_city", "ml_addr_state", "ml_addr_zip", "ml_cnty",
    "addr_line1", "addr_line2", "addr_line3", "addr_city", "addr_state",
    "addr_zip", "cnty", "pl2_addr_line1", "pl2_addr_line2", "pl2_addr_line3",
    "pl2_addr_city", "pl2_addr_state", "pl2_addr_zip", "pl2_cnty",
    "pl3_addr_line1", "pl3_addr_line2", "pl3_addr_line3", "pl3_addr_city",
    "pl3_addr_state", "pl3_addr_zip", "pl3_cnty", "lic_sta_cde",
    "lic_actv_sta_cde", "lic_sta_desc", "lic_actv_sta_desc", "rank_cde",
    "rank_desc", "other_license", "yr_began_practice", "rank_efct_dte",
    "orig_dte", "expr_dte", "nica_payment",
)
_CANNABIS_REQUIRED_FIELDS = (
    "frst_nme", "last_nme", "lic_nbr", "course_type", "dte_compl",
    "submitted_by", "pl_addr_line1", "pl_addr_line2", "pl_addr_line3",
    "pl_addr_cty", "pl_st_cde", "pl_zip", "pl_cnty", "phne_nbr",
    "specialties",
)
_LICENSE_STATUS_FIELDS = (
    "pro_cde",
    "rank_cde",
    "lic_nbr",
    "lic_actv_sta_desc",
    "lic_sta_desc",
    "orig_dte",
    "expr_dte",
    "status_effective_date",
    "f_name",
    "m_name",
    "l_name",
    "administrative_complaints_indicator",
    "emergency_order_indicator",
    "final_order_indicator",
    "multi_state_license_indicator",
)
_LICENSURE_FIELDS = (
    "pro_cde",
    "profession_name",
    "lic_id",
    "expire_date",
    "original_date",
    "rank_code",
    "license_number",
    "status_effective_date",
    "board_action_indicator",
    "license_status_description",
    "last_name",
    "first_name",
    "middle_name",
    "name_suffix",
    "business_name",
    "license_active_status_description",
    "county",
    "county_description",
    "mailing_address_line1",
    "mailing_address_line2",
    "mailing_address_line3",
    "mailing_address_city",
    "mailing_address_state",
    "mailing_address_zipcode",
    "mailing_address_area_code",
    "mailing_address_phone_number",
    "mailing_address_phone_extension",
    "practice_location_address_line1",
    "practice_location_address_line2",
    "practice_location_address_line3",
    "practice_location_address_city",
    "practice_location_address_state",
    "practice_location_address_zipcode",
    "email",
    "mod_cdes",
    "prescribe_ind",
    "dispensing_ind",
    "birth_year_range",
    "other_license",
)
_ADMINISTRATIVE_COMPLAINT_FIELDS = (
    "respondent_name",
    "license_number",
    "profession",
    "addr_line_1",
    "addr_line_2",
    "city",
    "state",
    "zip",
    "case_number",
    "case_activity_type",
    "case_activity_date",
)
_PAIN_MANAGEMENT_FIELDS = (
    "clinic_name",
    "pl_address",
    "lic_nbr",
    "lic_status",
    "year",
    "qtr",
    "reporting_phy_prof",
    "reporting_phy_lic_nbr",
    "reporting_phy_name",
    "new_cnt",
    "repeat_cnt",
    "abuse_cnt",
    "divrsn_cnt",
    "oos_cnt",
)
_PHARMACY_PHARMACIST_FIELDS = (
    "pharm_key_name",
    "pharm_dba_name",
    "pharm_lic_nbr",
    "pharm_expr_dte",
    "pharm_orig_dte",
    "pharm_stat_efctv_dte",
    "pharm_lic_sta_cde",
    "pharm_lic_sta_desc",
    "pharm_pl_addr_l1",
    "pharm_pl_addr_l2",
    "pharm_pl_addr_l3",
    "pharm_pl_cty",
    "pharm_pl_st",
    "pharm_pl_zip",
    "pharm_phne_nbr",
    "pharm_phne_ext",
    "rltn_prof_nme",
    "rltn_key_nme",
    "rltn_lic_nbr",
    "rltn_lic_sta_cde",
    "rltn_lic_sta_desc",
    "rltn_lic_sec_sta_cde",
    "rltn_lic_sec_sta_desc",
    "rltn_pl_addr_l1",
    "rltn_pl_addr_l2",
    "rltn_pl_addr_l3",
    "rltn_pl_city",
    "rltn_pl_state",
    "rltn_pl_zip",
    "rltn_phone_nbr",
    "rltn_phone_ext",
    "rltn_email",
)

_PROFILE_EXPECTED_FIELDS: dict[str, tuple[str, ...]] = {
    "profile_master": _PROFILE_MASTER_REQUIRED_FIELDS,
    "profile_indicators": (
        "pro_cde",
        "lic_id",
        "health_degree",
        "grad_med_edu",
        "prof_post_train",
        "faculty_appoint",
        "staff_priv",
        "certification",
        "criminal_offense",
        "medicaid_prgrm",
        "e_mail_addr",
    ),
    "counties": ("cnty", "cnty_desc"),
    "staff_privileges": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "hospital_instit",
        "city",
        "state",
    ),
    "other_licensure": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "other_prof_lic",
        "other_lic_state",
    ),
    "education": (
        "lic_id",
        "pro_cde",
        "inst_nme",
        "grad_dte",
        "deg_cert_earn_cde",
        "pgm_desc",
        "educ_mjr",
        "atnd_frm_dte",
        "atnd_to_dte",
        "educ_prvr_nbr",
    ),
    "other_degrees": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "school_name",
        "city",
        "state_country",
        "attended_from",
        "attended_to",
        "degree_title",
    ),
    "postgraduate_training": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "program_spclty_ar",
        "city",
        "state_country",
        "attend_from",
        "attend_to",
        "institute_name",
        "program_type",
        "other_spclty_ar",
    ),
    "faculty_appointments": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "faculty_title",
        "city",
        "fclty_apt_inst",
        "state",
    ),
    "certifications": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "specialty_brd",
        "specialty_cert",
        "specialty_dte",
    ),
    "financial_responsibility": (
        "pro_cde",
        "lic_id",
        "financial_resp",
        "financial_exempt",
        "liability_claim",
        "insured",
        "insured_10_yr",
    ),
    "criminal_offenses": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "offense_desc",
        "offense_date",
        "jurisdiction",
        "under_appeal",
    ),
    "disciplinary_actions": (
        "pro_cde",
        "lic_id",
        "rec_key",
        "disc_body",
        "disc_type",
        "disc_date",
        "disc_action_cde",
        "disc_action_desc",
        "disc_viol_cde",
        "disc_viol_desc",
        "under_appeal",
        "disc_action",
        "publishable",
    ),
    "special_disciplinary_actions": (
        "pro_cde",
        "lic_id",
        "rec_key",
        "sp_disc_action",
        "sp_disc_body",
        "sp_disc_date",
        "sp_disc_viol_cde",
        "sp_disc_viol_desc",
        "sp_disc_action_cde",
        "sp_disc_action_desc",
        "sp_disc_under_appeal",
    ),
    "final_disciplinary_actions": (
        "cse_nbr",
        "pro_cde",
        "lic_id",
        "close_date",
        "action_desc",
        "under_appeal_ind",
    ),
    "closed_liability_claims": (
        "pro_cde",
        "lic_id",
        "rec_key",
        "county",
        "case_number",
        "incident_date",
        "settlement_date",
        "settlement_amt",
        "policy_amt",
    ),
    "memberships": ("pro_cde", "lic_id", "rec_id", "comm_member"),
    "honors": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "honors_awards",
        "organization",
    ),
    "publications": (
        "pro_cde",
        "lic_id",
        "rec_id",
        "publication",
        "article_title",
        "date_of_pub",
    ),
    "languages": ("pro_cde", "lic_id", "rec_id", "language_used"),
    "affiliations": ("pro_cde", "lic_id", "rec_id", "affiliation"),
}


FLORIDA_SOURCES: dict[str, FloridaSource] = {
    source.key: source
    for source in (
        _profile_source(
            "profile_master", "licensee_profile.txt", "licenses", "state_license_profile",
            "Florida practitioner license", "lic_nbr", "lic_sta_desc", "profession_name",
            required_fields=_PROFILE_MASTER_REQUIRED_FIELDS,
        ),
        _profile_source(
            "profile_indicators", "tp_lic_indicators.txt", "professional_experience",
            "profile_indicator", "Profile information coverage",
            "health_degree", "grad_med_edu", "prof_post_train",
        ),
        _profile_source(
            "counties", "rbdcty.txt", "professional_experience", "county_reference",
            "Florida county", "county_desc", "county_name",
            required_fields=("cnty", "cnty_desc"),
            expected_fields=("cnty", "cnty_desc"),
        ),
        _profile_source(
            "staff_privileges", "tp_staff_priv.txt", "privileges", "staff_privilege",
            "Staff privilege", "hospital_instit", "city", "state",
        ),
        _profile_source(
            "other_licensure", "tp_other_licensure.txt", "licenses", "other_state_license",
            "Other state license", "other_prof_lic", "other_lic_state",
        ),
        _profile_source(
            "education", "rbdled.txt", "education", "education_history",
            "Education", "inst_nme", "pgm_desc", "educ_mjr", "grad_dte",
            verification_status="verified_at_initial_licensure",
        ),
        _profile_source(
            "other_degrees", "tp_other_health_dg.txt", "education", "other_health_degree",
            "Other health-related degree", "school_name", "degree", "degree_title",
        ),
        _profile_source(
            "postgraduate_training", "tp_prof_post_grad.txt", "training",
            "postgraduate_training", "Professional or postgraduate training",
            "institute_name", "program_type", "program_spclty_ar",
            "other_spclty_ar",
        ),
        _profile_source(
            "faculty_appointments", "tp_faculty_appt.txt", "academic_appointments",
            "faculty_appointment", "Faculty appointment",
            "fclty_apt_inst", "faculty_title", "city", "state",
        ),
        _profile_source(
            "certifications", "tp_certifications.txt", "certifications",
            "specialty_certification", "Specialty certification",
            "specialty_brd", "specialty_cert", "specialty_dte",
        ),
        _profile_source(
            "financial_responsibility", "tp_financial_resp.txt", "financial_responsibility",
            "financial_responsibility", "Financial responsibility",
            "financial_resp", "financial_exempt", "insured", "insured_10_yr",
        ),
        _profile_source(
            "criminal_offenses", "tp_criminal_off.txt", "criminal_disclosures",
            "criminal_offense", "Reported criminal offense",
            "offense_desc", "jurisdiction", "offense_date",
            sensitive=True, public_default=False,
        ),
        _profile_source(
            "disciplinary_actions", "tp_disciplinary.txt", "regulatory_actions",
            "disciplinary_action", "Disciplinary action",
            "disc_body", "disc_action_desc", "disc_date",
            assertion_type="state_reported", verification_status="government_source",
            sensitive=True, public_default=False,
        ),
        _profile_source(
            "special_disciplinary_actions", "tp_spec_discipline.txt", "regulatory_actions",
            "special_disciplinary_action", "Special disciplinary action",
            "sp_disc_body", "sp_disc_action_desc", "sp_disc_date",
            assertion_type="state_reported", verification_status="government_source",
            sensitive=True, public_default=False,
        ),
        _profile_source(
            "final_disciplinary_actions", "tp_ahca_discip.txt", "regulatory_actions",
            "final_disciplinary_action", "Final disciplinary action",
            "cse_nbr", "action_desc", "close_date",
            assertion_type="state_reported", verification_status="government_source",
            sensitive=True, public_default=False,
        ),
        _profile_source(
            "closed_liability_claims", "tp_closed_claim.txt", "liability_claims",
            "closed_liability_claim", "Closed liability claim",
            "case_number", "incident_date", "settlement_date", "settlement_amt",
            assertion_type="state_reported", verification_status="government_source",
            sensitive=True, public_default=False,
        ),
        _profile_source(
            "memberships", "tp_memberships.txt", "memberships", "committee_or_membership",
            "Committee or membership", "comm_member",
        ),
        _profile_source(
            "honors", "tp_honors.txt", "honors", "professional_or_community_award",
            "Professional or community award", "honors_awards", "organization",
        ),
        _profile_source(
            "publications", "tp_publications.txt", "publications", "publication",
            "Publication", "article_title", "publication", "date_of_pub",
        ),
        _profile_source(
            "languages", "tp_languages.txt", "languages", "spoken_language",
            "Language", "language_used",
        ),
        _profile_source(
            "affiliations", "tp_affiliations.txt", "affiliations",
            "professional_affiliation", "Professional affiliation",
            "affiliation",
        ),
        FloridaSource(
            "license_status", "/LicenseStatus?handler=DownloadDataFile", "lic_status.zip",
            "licenses", "license_status", "Florida license status",
            ("profession_name", "license_number", "license_status"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=("pro_cde", "rank_cde", "lic_nbr", "lic_sta_desc"),
            expected_fields=_LICENSE_STATUS_FIELDS,
            has_header=False,
        ),
        FloridaSource(
            "licensure_current", "/LicensureData", "LIC_ALL.zip", "licenses",
            "state_licensure_record", "Florida current licensure",
            ("profession_name", "license_number", "license_status"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=(
                "pro_cde",
                "profession_name",
                "rank_code",
                "license_number",
                "license_status_description",
            ),
            expected_fields=_LICENSURE_FIELDS,
        ),
        FloridaSource(
            "licensure_all_statuses", "/LicensureData", "PROF_ALL.zip", "licenses",
            "state_licensure_history", "Florida licensure history",
            ("profession_name", "license_number", "license_status"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=(
                "pro_cde",
                "profession_name",
                "rank_code",
                "license_number",
                "license_status_description",
            ),
            expected_fields=_LICENSURE_FIELDS,
        ),
        FloridaSource(
            "medical_cannabis_authorization",
            "/AuthtoOrderMedicalandLowTHCCannabis?handler=DownloadDataFile",
            "compassionate_file.txt",
            "prescribing_authorizations",
            "medical_cannabis_authorization",
            "Florida medical cannabis authorization",
            ("course_type", "dte_compl", "specialties"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=_CANNABIS_REQUIRED_FIELDS,
            expected_fields=_CANNABIS_REQUIRED_FIELDS,
        ),
        FloridaSource(
            "administrative_complaints",
            "/AdminComplaint?handler=DownloadDataFile",
            "dxe004dd.txt",
            "complaints",
            "administrative_complaint",
            "Administrative complaint (allegation, not a final action)",
            ("case_number", "activity_description", "activity_date"),
            assertion_type="allegation",
            verification_status="government_source",
            sensitive=True,
            public_default=False,
            required_fields=_ADMINISTRATIVE_COMPLAINT_FIELDS,
            expected_fields=_ADMINISTRATIVE_COMPLAINT_FIELDS,
        ),
        FloridaSource(
            "pain_management_report",
            "/PainManagementReport?handler=DownloadDataFile",
            "pain_management_report.txt",
            "program_reports",
            "pain_management_clinic_report",
            "Pain management clinic report",
            ("clinic_name", "license_number", "status"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=_PAIN_MANAGEMENT_FIELDS,
            expected_fields=_PAIN_MANAGEMENT_FIELDS,
        ),
        FloridaSource(
            "pharmacy_pharmacist",
            "/PharmacyPharmacist?handler=DownloadDataFile",
            "pharmacy_pharmacist.txt",
            "pharmacy_relationships",
            "pharmacy_pharmacist_relationship",
            "Pharmacy or pharmacist relationship",
            ("pharmacy_name", "pharmacist_name", "license_number"),
            assertion_type="state_reported",
            verification_status="government_source",
            required_fields=_PHARMACY_PHARMACIST_FIELDS,
            expected_fields=_PHARMACY_PHARMACIST_FIELDS,
        ),
    )
}

# A scheduled control-plane run is complete by default. Callers may select a
# smaller research subset, but publication then requires an explicit override.
DEFAULT_SOURCE_KEYS = tuple(FLORIDA_SOURCES)

_PROFILE_MASTER_CATEGORIES = {
    "identity",
    "demographics",
    "locations",
    "licenses",
    "professional_experience",
    "program_reports",
}

_INTERNAL_PROFILE_FIELDS = frozenset(
    {"pro_cde", "lic_id", "rec_id", "rec_key"}
)
_PROFILE_RAW_ONLY_FIELDS = frozenset(
    {*_INTERNAL_PROFILE_FIELDS, "e_mail_addr"}
)
_PROFILE_VALUE_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    "staff_privileges": (
        ("institution", "hospital_instit"),
        ("city", "city"),
        ("state", "state"),
    ),
    "other_licensure": (
        ("profession_or_license", "other_prof_lic"),
        ("jurisdiction", "other_lic_state"),
    ),
    "education": (
        ("institution", "inst_nme"),
        ("graduation_date", "grad_dte"),
        ("degree_or_certificate_code", "deg_cert_earn_cde"),
        ("program", "pgm_desc"),
        ("major", "educ_mjr"),
        ("attendance_start", "atnd_frm_dte"),
        ("attendance_end", "atnd_to_dte"),
        ("institution_identifier", "educ_prvr_nbr"),
    ),
    "other_degrees": (
        ("institution", "school_name"),
        ("city", "city"),
        ("state_or_country", "state_country"),
        ("attendance_start", "attended_from"),
        ("attendance_end", "attended_to"),
        ("degree", "degree_title"),
    ),
    "postgraduate_training": (
        ("specialty", "program_spclty_ar"),
        ("city", "city"),
        ("state_or_country", "state_country"),
        ("attendance_start", "attend_from"),
        ("attendance_end", "attend_to"),
        ("institution", "institute_name"),
        ("program_type", "program_type"),
        ("other_specialty", "other_spclty_ar"),
    ),
    "faculty_appointments": (
        ("title", "faculty_title"),
        ("institution", "fclty_apt_inst"),
        ("city", "city"),
        ("state", "state"),
    ),
    "certifications": (
        ("certifying_board", "specialty_brd"),
        ("certification", "specialty_cert"),
        ("certification_date", "specialty_dte"),
    ),
    "criminal_offenses": (
        ("description", "offense_desc"),
        ("offense_date", "offense_date"),
        ("offense_jurisdiction", "jurisdiction"),
        ("under_appeal", "under_appeal"),
    ),
    "disciplinary_actions": (
        ("disciplinary_body", "disc_body"),
        ("discipline_type", "disc_type"),
        ("action_date", "disc_date"),
        ("action_code", "disc_action_cde"),
        ("action_description", "disc_action_desc"),
        ("violation_code", "disc_viol_cde"),
        ("violation_description", "disc_viol_desc"),
        ("under_appeal", "under_appeal"),
        ("action", "disc_action"),
        ("publishable", "publishable"),
    ),
    "special_disciplinary_actions": (
        ("action", "sp_disc_action"),
        ("disciplinary_body", "sp_disc_body"),
        ("action_date", "sp_disc_date"),
        ("violation_code", "sp_disc_viol_cde"),
        ("violation_description", "sp_disc_viol_desc"),
        ("action_code", "sp_disc_action_cde"),
        ("action_description", "sp_disc_action_desc"),
        ("under_appeal", "sp_disc_under_appeal"),
    ),
    "final_disciplinary_actions": (
        ("case_number", "cse_nbr"),
        ("closed_date", "close_date"),
        ("action_description", "action_desc"),
        ("under_appeal", "under_appeal_ind"),
    ),
    "closed_liability_claims": (
        ("county", "county"),
        ("case_number", "case_number"),
        ("incident_date", "incident_date"),
        ("settlement_date", "settlement_date"),
        ("settlement_amount", "settlement_amt"),
        ("policy_amount", "policy_amt"),
    ),
    "memberships": (("membership", "comm_member"),),
    "honors": (
        ("honor_or_award", "honors_awards"),
        ("organization", "organization"),
    ),
    "publications": (
        ("publication", "publication"),
        ("article_title", "article_title"),
        ("publication_date", "date_of_pub"),
    ),
    "languages": (("language", "language_used"),),
    "affiliations": (("affiliation", "affiliation"),),
}
_PROFILE_DATE_VALUE_FIELDS: dict[str, frozenset[str]] = {
    "education": frozenset(
        {"graduation_date", "attendance_start", "attendance_end"}
    ),
    "other_degrees": frozenset({"attendance_start", "attendance_end"}),
    "postgraduate_training": frozenset(
        {"attendance_start", "attendance_end"}
    ),
    "certifications": frozenset({"certification_date"}),
    "criminal_offenses": frozenset({"offense_date"}),
    "disciplinary_actions": frozenset({"action_date"}),
    "special_disciplinary_actions": frozenset({"action_date"}),
    "final_disciplinary_actions": frozenset({"closed_date"}),
    "closed_liability_claims": frozenset(
        {"incident_date", "settlement_date"}
    ),
    "publications": frozenset({"publication_date"}),
}
_PROFILE_DISPLAY_VALUE_FIELDS: dict[str, tuple[str, ...]] = {
    "staff_privileges": ("institution", "city", "state"),
    "other_licensure": ("profession_or_license", "jurisdiction"),
    "education": ("institution", "program", "major", "graduation_date"),
    "other_degrees": ("institution", "degree", "state_or_country"),
    "postgraduate_training": (
        "institution",
        "program_type",
        "specialty",
    ),
    "faculty_appointments": ("institution", "title", "city", "state"),
    "certifications": (
        "certifying_board",
        "certification",
        "certification_date",
    ),
    "criminal_offenses": (
        "description",
        "offense_jurisdiction",
        "offense_date",
    ),
    "disciplinary_actions": (
        "disciplinary_body",
        "action_description",
        "action_date",
    ),
    "special_disciplinary_actions": (
        "disciplinary_body",
        "action_description",
        "action_date",
    ),
    "final_disciplinary_actions": (
        "case_number",
        "action_description",
        "closed_date",
    ),
    "closed_liability_claims": (
        "case_number",
        "incident_date",
        "settlement_date",
    ),
    "memberships": ("membership",),
    "honors": ("honor_or_award", "organization"),
    "publications": ("article_title", "publication", "publication_date"),
    "languages": ("language",),
    "affiliations": ("affiliation",),
}
_PROFILE_EFFECTIVE_FIELDS: dict[str, tuple[str | None, str | None]] = {
    "education": ("attendance_start", "attendance_end"),
    "other_degrees": ("attendance_start", "attendance_end"),
    "postgraduate_training": ("attendance_start", "attendance_end"),
    "certifications": ("certification_date", None),
    "criminal_offenses": ("offense_date", None),
    "disciplinary_actions": ("action_date", None),
    "special_disciplinary_actions": ("action_date", None),
    "final_disciplinary_actions": ("closed_date", None),
    "closed_liability_claims": ("incident_date", "settlement_date"),
    "publications": ("publication_date", None),
}

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")
_NON_WORD = re.compile(r"[^a-z0-9]+")
_RUN_ID_RE = re.compile(r"(?:[a-f0-9]{32}|[a-f0-9]{64})")
_SETTINGS_RE = re.compile(r"\bvar\s+SETTINGS\s*=\s*(\{.*?\});", re.DOTALL)
_FORM_RE = re.compile(r"<form[^>]+action=[\"']([^\"']+)[\"'][^>]*>(.*?)</form>", re.I | re.S)
_INPUT_RE = re.compile(r"<input[^>]+name=[\"']([^\"']+)[\"'][^>]+value=[\"']([^\"']*)[\"'][^>]*>", re.I)

_PROFESSION_LICENSE_PREFIXES = {
    "501": ("CH",),
    "0501": ("CH",),
    "1501": ("ME",),
    "1701": ("RN",),
    "1702": ("PN",),
    "1711": ("APRN", "ARNP"),
    "1901": ("OS",),
    "2101": ("PO",),
    "2201": ("PS",),
    "2203": ("PU",),
    "4401": ("CNA",),
}
_PROFESSION_TAXONOMY_PREFIXES = {
    "501": ("111N",),
    "0501": ("111N",),
    "1501": ("20",),
    "1701": ("163W",),
    "1702": ("164W",),
    "1711": ("363L", "364S"),
    "1901": ("20",),
    "2101": ("213E",),
    "2201": ("1835",),
    "2203": ("1835",),
    "4401": ("376K",),
}
_PROFESSION_DETAILS_BY_NAME = {
    "medicaldoctor": ("1501", "ME"),
    "osteopathicphysician": ("1901", "OS"),
    "registerednurse": ("1701", "RN"),
    "licensedpracticalnurse": ("1702", "PN"),
    "pharmacist": ("2201", "PS"),
    "consultantpharmacist": ("2203", "PU"),
    "certifiednursingassistant": ("4401", "CNA"),
}


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _header_sha256(header: Iterable[str]) -> str:
    """Hash the normalized ordered header used to validate a source."""
    return hashlib.sha256(
        json.dumps(
            list(header),
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _snake(value: Any) -> str:
    return _NON_WORD.sub("_", str(value or "").strip().lower()).strip("_")


def _clean_row(row: Mapping[str, Any]) -> dict[str, str]:
    return {
        _snake(key): str(value or "").strip()
        for key, value in row.items()
        if key is not None and _snake(key)
    }


def _first(row: Mapping[str, str], *names: str) -> str:
    for name in names:
        value = row.get(name, "").strip()
        if value:
            return value
    return ""


def _license_candidates(
    license_number: str,
    profession_code: str,
    rank_code: str = "",
) -> tuple[str, ...]:
    normalized = _NON_ALNUM.sub("", license_number.upper())
    if not normalized:
        return ()
    values = {normalized}
    if normalized.isdigit():
        normalized_rank = _NON_ALNUM.sub("", rank_code.upper())
        if normalized_rank and not normalized_rank.isdigit():
            values.add(f"{normalized_rank}{normalized}")
        for prefix in _PROFESSION_LICENSE_PREFIXES.get(profession_code, ()):
            values.add(f"{prefix}{normalized}")
    return tuple(sorted(values))


def _name_token(value: str) -> str:
    return _NON_WORD.sub("", value.lower())


def _person_name_parts(value: str) -> tuple[str, str]:
    """Return first/last names only when the source has an explicit comma form."""
    family, separator, given = value.partition(",")
    if not separator:
        return "", ""
    first_name = given.strip().split(" ", 1)[0]
    return first_name, family.strip()


def _profession_details(
    profession_name: str,
    discovered: Mapping[str, set[tuple[str, str]]] | None,
) -> tuple[str, str]:
    key = _name_token(profession_name)
    if discovered:
        candidates = discovered.get(key, set())
        if len(candidates) == 1:
            return next(iter(candidates))
    return _PROFESSION_DETAILS_BY_NAME.get(key, ("", ""))


def _canonical_match_row(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    profession_details: Mapping[str, set[tuple[str, str]]] | None = None,
) -> dict[str, str]:
    """Map source-specific identity fields into the conservative NPI matcher."""
    canonical_by_key = dict(source_row)
    profession_name = ""
    source_name = ""
    if profile_source.key == "administrative_complaints":
        profession_name = source_row.get("profession", "")
        source_name = source_row.get("respondent_name", "")
    elif profile_source.key == "pain_management_report":
        profession_name = source_row.get("reporting_phy_prof", "")
        source_name = source_row.get("reporting_phy_name", "")
        canonical_by_key["lic_nbr"] = source_row.get("reporting_phy_lic_nbr", "")
        canonical_by_key["license_number"] = canonical_by_key["lic_nbr"]
    elif profile_source.key == "pharmacy_pharmacist":
        profession_name = source_row.get("rltn_prof_nme", "")
        source_name = source_row.get("rltn_key_nme", "")
        canonical_by_key["lic_nbr"] = source_row.get("rltn_lic_nbr", "")
        canonical_by_key["license_number"] = canonical_by_key["lic_nbr"]
    elif profile_source.key in {"licensure_current", "licensure_all_statuses"}:
        profession_name = source_row.get("profession_name", "")
        canonical_by_key["rank_cde"] = source_row.get("rank_code", "")

    if profession_name:
        canonical_by_key["profession_name"] = profession_name
        profession_code, rank_code = _profession_details(
            profession_name,
            profession_details,
        )
        if profession_code:
            canonical_by_key["pro_cde"] = profession_code
        if rank_code:
            canonical_by_key["rank_cde"] = rank_code
    if source_name:
        first_name, last_name = _person_name_parts(source_name)
        if first_name and last_name:
            canonical_by_key["first_name"] = first_name
            canonical_by_key["last_name"] = last_name
    return canonical_by_key


def _is_name_compatible(source: Mapping[str, str], candidate: Mapping[str, Any]) -> bool:
    source_last = _name_token(
        _first(source, "last_name", "lname", "last_nm", "last_nme", "l_name")
    )
    source_first = _name_token(
        _first(source, "first_name", "fname", "first_nm", "frst_nme", "f_name")
    )
    if source_last and source_last != _name_token(str(candidate.get("last_name") or "")):
        return False
    if source_first:
        candidate_first = _name_token(str(candidate.get("first_name") or ""))
        if candidate_first and source_first[0] != candidate_first[0]:
            return False
    return True


def _is_taxonomy_compatible(profession_code: str, taxonomy_code: str) -> bool:
    prefixes = _PROFESSION_TAXONOMY_PREFIXES.get(profession_code)
    return not prefixes or taxonomy_code.startswith(prefixes)


def _human_display(source: FloridaSource, row: Mapping[str, str]) -> str:
    values: list[str] = []
    for field in source.label_fields:
        value = row.get(field, "").strip()
        if value and value not in values:
            values.append(value)
    if not values:
        values = [
            value for key, value in row.items()
            if value and key not in _PROFILE_RAW_ONLY_FIELDS
        ][:3]
    suffix = " — ".join(values)
    return f"{source.title}: {suffix}" if suffix else source.title


def _without_empty(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: item
        for key, item in value.items()
        if item not in (None, "", [], {})
    }


def _normalize_source_date(value: str) -> tuple[str, str]:
    raw = value.strip()
    if not raw:
        return "", "unknown"
    if re.fullmatch(r"\d{4}", raw):
        return raw, "year"
    for date_format in (
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%Y%m%d",
        "%d-%b-%y",
        "%d-%b-%Y",
    ):
        try:
            return datetime.strptime(raw, date_format).date().isoformat(), "day"
        except ValueError:
            continue
    return raw, "source"


def _address_value(
    row: Mapping[str, str],
    *,
    prefix: str,
    location_type: str,
) -> dict[str, Any] | None:
    value = _without_empty(
        {
            "location_type": location_type,
            "address_line_1": row.get(f"{prefix}addr_line1", ""),
            "address_line_2": row.get(f"{prefix}addr_line2", ""),
            "address_line_3": row.get(f"{prefix}addr_line3", ""),
            "city": row.get(f"{prefix}addr_city", ""),
            "state": row.get(f"{prefix}addr_state", ""),
            "postal_code": row.get(f"{prefix}addr_zip", ""),
            "county": row.get(f"{prefix}cnty", ""),
        }
    )
    address_fields = (
        "address_line_1",
        "address_line_2",
        "address_line_3",
        "city",
        "state",
        "postal_code",
    )
    return value if any(value.get(field) for field in address_fields) else None


def _address_display(value: Mapping[str, Any]) -> str:
    labels_by_key = {
        "mailing": "Mailing address",
        "practice_primary": "Primary practice location",
        "practice_secondary_2": "Additional practice location",
        "practice_secondary_3": "Additional practice location",
    }
    address = ", ".join(
        str(value[field])
        for field in (
            "address_line_1",
            "address_line_2",
            "address_line_3",
            "city",
            "state",
            "postal_code",
        )
        if value.get(field)
    )
    location_types = value.get("location_types") or [value.get("location_type")]
    location_labels = [
        labels_by_key.get(str(location_type), "Provider location")
        for location_type in location_types
        if location_type
    ]
    label = " and ".join(dict.fromkeys(location_labels)) or "Provider location"
    return f"{label}: {address}" if address else label


class FloridaMQAClient:
    """Authenticate through the portal's Azure B2C flow and fetch bulk artifacts."""

    def __init__(self, base_url: str, email: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.opener = build_opener(HTTPCookieProcessor(CookieJar()))

    def _open(self, request: str | Request) -> Any:
        return self.opener.open(request, timeout=120)

    def authenticate(self) -> None:
        """Authenticate to the source portal without retaining credential values."""
        response = self._open(f"{self.base_url}/ProfileData")
        body = response.read().decode("utf-8", "replace")
        if "Sign out" in body:
            return
        match = _SETTINGS_RE.search(body)
        if not match:
            raise RuntimeError("florida_mqa_login_settings_missing")
        settings = json.loads(match.group(1))
        final_url = response.geturl()
        parsed = urlparse(final_url)
        policy_base = final_url.split("/oauth2/", 1)[0]
        policy = str(
            settings.get("policy")
            or parse_qs(parsed.query).get("p", [""])[0]
            or policy_base.rsplit("/", 1)[-1]
        )
        transaction = str(settings.get("transId") or settings.get("transactionId") or "")
        csrf = str(settings.get("csrf") or settings.get("csrf_token") or "")
        if not policy or not transaction or not csrf:
            raise RuntimeError("florida_mqa_login_contract_changed")
        query = urlencode({"tx": transaction, "p": policy})
        login_request = Request(
            f"{policy_base}/SelfAsserted?{query}",
            data=urlencode(
                {"request_type": "RESPONSE", "email": self.email, "password": self.password}
            ).encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-CSRF-TOKEN": csrf,
                "Referer": final_url,
            },
        )
        login_payload = json.loads(self._open(login_request).read().decode("utf-8"))
        if str(login_payload.get("status", "200")) not in {"200", "0"}:
            raise RuntimeError("florida_mqa_login_rejected")
        confirmed = self._open(
            f"{policy_base}/api/CombinedSigninAndSignup/confirmed?"
            + urlencode(
                {
                    "rememberMe": "false",
                    "csrf_token": csrf,
                    "tx": transaction,
                    "p": policy,
                }
            )
        )
        confirmed_html = confirmed.read().decode("utf-8", "replace")
        form_match = _FORM_RE.search(confirmed_html)
        if not form_match:
            raise RuntimeError("florida_mqa_login_callback_missing")
        callback_url = urljoin(confirmed.geturl(), html.unescape(form_match.group(1)))
        callback_fields_by_key = {
            html.unescape(name): html.unescape(field_value)
            for name, field_value in _INPUT_RE.findall(form_match.group(2))
        }
        callback = self._open(
            Request(
                callback_url,
                data=urlencode(callback_fields_by_key).encode(),
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        )
        callback_body = callback.read().decode("utf-8", "replace")
        if "Sign out" not in callback_body:
            raise RuntimeError("florida_mqa_login_callback_failed")

    def download(self, source: FloridaSource, target: Path) -> tuple[str, int]:
        """Download one authenticated source artifact to the import workspace."""
        url = urljoin(self.base_url, source.url)
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        size = 0
        with self._open(url) as response, target.open("wb") as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        return digest.hexdigest(), size


def _data_stream(path: Path) -> Iterator[tuple[str, io.TextIOBase]]:
    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for name in sorted(archive.namelist()):
                if name.endswith("/") or name.lower().endswith(".pdf"):
                    continue
                with archive.open(name) as raw:
                    yield name, io.TextIOWrapper(raw, encoding="latin-1", errors="replace", newline="")
        return
    with path.open("r", encoding="latin-1", errors="replace", newline="") as stream:
        yield path.name, stream


def _normalized_source_header(
    source: FloridaSource,
    raw_header: Iterable[str],
    *,
    artifact_name: str,
) -> list[str]:
    normalized_items = [_snake(field) for field in raw_header if field and _snake(field)]
    if not normalized_items:
        raise RuntimeError(f"florida_mqa_header_missing:{artifact_name}")
    if source.expected_fields and tuple(normalized_items) != source.expected_fields:
        raise RuntimeError(
            f"florida_mqa_schema_changed:{source.key}:expected_header"
        )
    return normalized_items


def _artifact_header(path: Path, profile_source: FloridaSource) -> list[str]:
    headers: list[list[str]] = []
    for _name, stream in _data_stream(path):
        raw_header = stream.readline().rstrip("\r\n").split("|")
        if raw_header == [""]:
            raise RuntimeError(f"florida_mqa_header_missing:{path.name}")
        if not profile_source.has_header:
            if not profile_source.expected_fields:
                raise RuntimeError(
                    f"florida_mqa_headerless_schema_missing:{profile_source.key}"
                )
            if len(raw_header) != len(profile_source.expected_fields):
                raise RuntimeError(
                    f"florida_mqa_row_changed:{profile_source.key}:1:{len(raw_header)}"
                )
            headers.append(list(profile_source.expected_fields))
            continue
        if profile_source.key == "medical_cannabis_authorization" and len(raw_header) != len(
            profile_source.expected_fields
        ):
            raise RuntimeError(
                f"florida_mqa_cannabis_header_changed:{len(raw_header)}"
            )
        normalized = _normalized_source_header(
            profile_source,
            raw_header,
            artifact_name=path.name,
        )
        headers.append(normalized)
    if not headers:
        raise RuntimeError(f"florida_mqa_header_missing:{path.name}")
    if any(header != headers[0] for header in headers[1:]):
        raise RuntimeError(f"florida_mqa_archive_headers_inconsistent:{path.name}")
    return headers[0]


def _increment_parser_metric(
    parser_metrics: dict[str, Any] | None,
    name: str,
    count: int = 1,
) -> None:
    if parser_metrics is not None:
        parser_metrics[name] = int(parser_metrics.get(name) or 0) + count


def _physical_row_sha256(values: Iterable[str]) -> str:
    """Hash the exact decoded physical row, excluding its line terminator."""
    return hashlib.sha256("|".join(values).encode("latin-1")).hexdigest()


def _is_plausible_email(value: str) -> bool:
    return bool(
        re.fullmatch(
            r"[^@\s|]+@[^@\s|]+",
            value.strip(),
        )
    )


def _is_licensure_email_alignment_plausible(
    header: list[str],
    field_values: list[str],
    email_index: int,
) -> bool:
    """Validate location and suffix alignment around a split email."""
    expected_location_fields = (
        "mailing_address_state",
        "mailing_address_zipcode",
        "practice_location_address_state",
        "practice_location_address_zipcode",
    )
    if any(field not in header for field in expected_location_fields):
        return False
    location_values_by_key = {
        field: field_values[header.index(field)].strip()
        for field in expected_location_fields
    }
    suffix_values = field_values[email_index + 2 :]
    if len(suffix_values) != 5:
        return False
    _mod_codes, prescribe_items, dispensing_items, birth_year_range_items, other_license_items = (
        field_value.strip() for field_value in suffix_values
    )
    indicator_values = {"", "Y", "N"}
    return (
        all(
            not location_values_by_key[field]
            or bool(re.fullmatch(r"[A-Za-z]{2}", location_values_by_key[field]))
            for field in (
                "mailing_address_state",
                "practice_location_address_state",
            )
        )
        and all(
            not location_values_by_key[field]
            or bool(
                re.fullmatch(
                    r"\d{5}(?:-?\d{4})?",
                    location_values_by_key[field],
                )
            )
            for field in (
                "mailing_address_zipcode",
                "practice_location_address_zipcode",
            )
        )
        and
        prescribe_items.upper() in indicator_values
        and dispensing_items.upper() in indicator_values
        and other_license_items.upper() in indicator_values
        and bool(
            re.fullmatch(
                r"(?:|N/A|\d{2,3}\s*-\s*\d{2,3})",
                birth_year_range_items.upper(),
            )
        )
    )


def _license_status_continuation_values(
    header: list[str],
    physical_rows: list[tuple[int, list[str]]],
    *,
    artifact_member: str,
    parser_metrics: dict[str, Any] | None,
) -> tuple[list[str], dict[str, Any]] | None:
    """Recover the documented fixed-width license-name continuation record."""
    if (
        header != list(_LICENSE_STATUS_FIELDS)
        or len(physical_rows) != 3
        or [len(field_values) for _, field_values in physical_rows] != [11, 5, 1]
    ):
        return None
    physical_lines = ["|".join(field_values) for _, field_values in physical_rows]
    if (
        any(len(line) != 125 for line in physical_lines)
        or not physical_rows[0][1][-1].strip()
        or physical_rows[2][1][0].strip()
    ):
        return None

    field_values = "".join(physical_lines).split("|")
    if len(field_values) != len(header):
        return None
    cleaned_items = [field_value.strip() for field_value in field_values]
    if (
        not re.fullmatch(r"\d{4}", cleaned_items[0])
        or not re.fullmatch(r"[A-Za-z0-9]{1,3}", cleaned_items[1])
        or not re.fullmatch(r"\d{1,12}", cleaned_items[2])
        or not cleaned_items[3]
        or not cleaned_items[4]
        or not cleaned_items[8]
        or not cleaned_items[10]
        or any(field_value.upper() not in {"", "Y", "N"} for field_value in cleaned_items[11:15])
    ):
        return None
    try:
        for field_value in cleaned_items[5:8]:
            datetime.strptime(field_value, "%m/%d/%Y")
    except ValueError:
        return None

    _increment_parser_metric(parser_metrics, "recovered_rows")
    _increment_parser_metric(parser_metrics, "continuation_physical_rows", 3)
    return field_values, {
        "kind": "wrapped_license_name_recovered",
        "artifact_member": artifact_member,
        "physical_row_numbers": [
            row_number for row_number, _ in physical_rows
        ],
        "physical_field_counts": [
            len(row_values) for _, row_values in physical_rows
        ],
        "physical_row_sha256": [
            _physical_row_sha256(row_values)
            for _, row_values in physical_rows
        ],
        "logical_field_count": len(field_values),
    }


def _normalized_pipe_values(
    profile_source: FloridaSource,
    header: list[str],
    field_values: list[str],
    *,
    row_number: int,
    artifact_member: str,
    parser_metrics: dict[str, Any] | None,
) -> tuple[list[str] | None, dict[str, Any] | None]:
    """Normalize only explicit source quirks; otherwise quarantine the row."""
    physical_field_count = len(field_values)
    physical_row_sha256 = _physical_row_sha256(field_values)
    trailing_empty_count = 0
    if len(field_values) > len(header) and field_values[-1] == "":
        field_values = field_values[:-1]
        trailing_empty_count = 1
    if trailing_empty_count:
        _increment_parser_metric(
            parser_metrics,
            "trailing_empty_rows",
        )
        _increment_parser_metric(
            parser_metrics,
            "trailing_empty_fields",
            trailing_empty_count,
        )

    repair_metadata_by_key: dict[str, Any] | None = None
    if (
        len(field_values) == len(header) + 1
        and profile_source.key == "profile_indicators"
        and header[-1:] == ["e_mail_addr"]
        and field_values[-2] == ""
    ):
        field_values = [*field_values[:-2], field_values[-1]]
        repair_metadata_by_key = {
            "kind": "shifted_raw_email_recovered",
            "field": "e_mail_addr",
            "physical_field_count": physical_field_count,
        }
        _increment_parser_metric(parser_metrics, "recovered_rows")
    if (
        len(field_values) == len(header) + 1
        and profile_source.key in {"licensure_current", "licensure_all_statuses"}
        and "email" in header
    ):
        email_index = header.index("email")
        email_values = field_values[email_index : email_index + 2]
        if (
            all(_is_plausible_email(field_value) for field_value in email_values)
            and _is_licensure_email_alignment_plausible(
                header,
                field_values,
                email_index,
            )
        ):
            suffix_values = field_values[email_index + 2 :]
            field_values = [
                *field_values[:email_index],
                "|".join(email_values),
                *suffix_values,
            ]
            repair_metadata_by_key = {
                "kind": "embedded_delimiter_recovered",
                "field": "email",
                "physical_field_count": physical_field_count,
            }
            _increment_parser_metric(parser_metrics, "recovered_rows")

    if len(field_values) == len(header):
        return field_values, repair_metadata_by_key
    _increment_parser_metric(parser_metrics, "quarantined_rows")
    return None, {
        "kind": "field_count_mismatch",
        "source_key": profile_source.key,
        "artifact_member": artifact_member,
        "row_number": row_number,
        "physical_field_count": physical_field_count,
        "expected_field_count": len(header),
        "trailing_empty_fields": trailing_empty_count,
        "physical_row_sha256": physical_row_sha256,
    }


def _iter_rows(
    path: Path,
    profile_source: FloridaSource | None = None,
    *,
    parser_metrics: dict[str, Any] | None = None,
) -> Iterator[tuple[int, dict[str, Any], dict[str, str], list[str]]]:
    """Yield normalized source rows together with retained parsing evidence."""
    for artifact_member, stream in _data_stream(path):
        if profile_source and not profile_source.has_header:
            if not profile_source.expected_fields:
                raise RuntimeError(
                    f"florida_mqa_headerless_schema_missing:{profile_source.key}"
                )
            header_items = list(profile_source.expected_fields)
            reader = csv.reader(
                stream,
                delimiter="|",
                quoting=csv.QUOTE_NONE,
            )
            pending_rows: deque[tuple[int, list[str]]] = deque()
            numbered_rows = iter(enumerate(reader, start=1))
            while True:
                try:
                    row_number, physical_values = (
                        pending_rows.popleft()
                        if pending_rows
                        else next(numbered_rows)
                    )
                except StopIteration:
                    break
                continuation: tuple[list[str], dict[str, Any]] | None = None
                if (
                    profile_source.key == "license_status"
                    and len(physical_values) == 11
                    and len("|".join(physical_values)) == 125
                ):
                    lookahead_items: list[tuple[int, list[str]]] = []
                    for _ in range(2):
                        try:
                            lookahead_items.append(next(numbered_rows))
                        except StopIteration:
                            break
                    continuation = _license_status_continuation_values(
                        header_items,
                        [(row_number, physical_values), *lookahead_items],
                        artifact_member=artifact_member,
                        parser_metrics=parser_metrics,
                    )
                    if continuation is None:
                        pending_rows.extend(lookahead_items)
                if continuation is not None:
                    field_values, parse_metadata = continuation
                else:
                    field_values, parse_metadata = _normalized_pipe_values(
                        profile_source,
                        header_items,
                        list(physical_values),
                        row_number=row_number,
                        artifact_member=artifact_member,
                        parser_metrics=parser_metrics,
                    )
                if field_values is None:
                    raw_row_by_key = {
                        "_physical_fields": physical_values,
                        "_source_parse_metadata": parse_metadata,
                    }
                    cleaned_by_key = {
                        "_source_parse_quarantine": "field_count_mismatch",
                        "_physical_field_count": str(len(physical_values)),
                    }
                else:
                    raw_row_by_key = dict(zip(header_items, field_values, strict=True))
                    if parse_metadata:
                        raw_row_by_key["_source_parse_metadata"] = parse_metadata
                    cleaned_by_key = _clean_row(raw_row_by_key)
                    if parse_metadata:
                        cleaned_by_key["_source_parse_repair"] = str(
                            parse_metadata["kind"]
                        )
                if any(cleaned_by_key.values()):
                    yield row_number, raw_row_by_key, cleaned_by_key, header_items
            continue
        if profile_source and profile_source.key == "medical_cannabis_authorization":
            raw_header = stream.readline().rstrip("\r\n").split("|")
            header_items = _normalized_source_header(
                profile_source,
                raw_header,
                artifact_name=path.name,
            )
            if len(raw_header) != len(profile_source.expected_fields):
                raise RuntimeError(
                    f"florida_mqa_cannabis_header_changed:{len(raw_header)}"
                )
            for row_number, line in enumerate(stream, start=2):
                field_values = line.rstrip("\r\n").split("|", 14)
                if len(field_values) != 15:
                    raise RuntimeError(
                        f"florida_mqa_cannabis_row_changed:{row_number}:{len(field_values)}"
                    )
                raw_row_by_key = dict(zip(raw_header, field_values, strict=True))
                cleaned_by_key = _clean_row(raw_row_by_key)
                if any(cleaned_by_key.values()):
                    yield row_number, raw_row_by_key, cleaned_by_key, header_items
            continue

        reader = csv.reader(
            stream,
            delimiter="|",
            quoting=csv.QUOTE_NONE,
        )
        try:
            raw_header = next(reader)
        except StopIteration as exc:
            raise RuntimeError(
                f"florida_mqa_header_missing:{path.name}"
            ) from exc
        header_items = (
            _normalized_source_header(
                profile_source,
                raw_header,
                artifact_name=path.name,
            )
            if profile_source is not None
            else [
                _snake(field)
                for field in raw_header
                if field and _snake(field)
            ]
        )
        if not header_items:
            raise RuntimeError(f"florida_mqa_header_missing:{path.name}")
        if profile_source is None:
            profile_source = FloridaSource(
                key="untyped",
                path="",
                filename=path.name,
                category="",
                fact_type="",
                title="",
                label_fields=(),
                expected_fields=tuple(header_items),
            )
        for row_number, physical_values in enumerate(reader, start=2):
            field_values, parse_metadata = _normalized_pipe_values(
                profile_source,
                header_items,
                list(physical_values),
                row_number=row_number,
                artifact_member=artifact_member,
                parser_metrics=parser_metrics,
            )
            if field_values is None:
                raw_row_by_key = {
                    "_physical_fields": physical_values,
                    "_source_parse_metadata": parse_metadata,
                }
                cleaned_by_key = {
                    "_source_parse_quarantine": "field_count_mismatch",
                    "_physical_field_count": str(len(physical_values)),
                }
            else:
                raw_field_names = [
                    field for field in raw_header if field and _snake(field)
                ]
                raw_row_by_key = dict(
                    zip(raw_field_names, field_values, strict=True)
                )
                if parse_metadata:
                    raw_row_by_key["_source_parse_metadata"] = parse_metadata
                cleaned_by_key = _clean_row(raw_row_by_key)
                if parse_metadata:
                    cleaned_by_key["_source_parse_repair"] = str(
                        parse_metadata["kind"]
                    )
            if any(cleaned_by_key.values()):
                yield row_number, raw_row_by_key, cleaned_by_key, header_items


async def _ensure_tables() -> None:
    for model in (
        ProviderProfileImportRun,
        ProviderProfileArtifact,
        ProviderProfileSourceRecord,
        ProviderProfileFact,
        ProviderProfileProjection,
    ):
        await db.create_table(model.__table__, checkfirst=True)
    schema = ProviderProfileFact.__table__.schema or "mrf"
    await db.status(
        f"ALTER TABLE {schema}.provider_profile_fact "
        "ADD COLUMN IF NOT EXISTS logical_fact_key varchar(64);"
    )
    await db.status(
        f"UPDATE {schema}.provider_profile_fact "
        "SET logical_fact_key = fact_id WHERE logical_fact_key IS NULL;"
    )
    await db.status(
        f"ALTER TABLE {schema}.provider_profile_fact "
        "ALTER COLUMN logical_fact_key SET NOT NULL;"
    )
    await db.status(
        f"CREATE INDEX IF NOT EXISTS provider_profile_fact_logical_key_idx "
        f"ON {schema}.provider_profile_fact (logical_fact_key, npi);"
    )
    await db.status(
        "CREATE INDEX IF NOT EXISTS provider_profile_fact_npi_category_idx "
        f"ON {schema}.provider_profile_fact (npi, category);"
    )
    await db.status(
        "CREATE INDEX IF NOT EXISTS provider_profile_fact_run_npi_idx "
        f"ON {schema}.provider_profile_fact (run_id, npi);"
    )
    await db.status(
        "CREATE INDEX IF NOT EXISTS provider_profile_source_record_npi_idx "
        f"ON {schema}.provider_profile_source_record "
        "(matched_npi, match_status);"
    )


async def _load_florida_license_index() -> dict[str, list[dict[str, Any]]]:
    schema = ProviderProfileProjection.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise RuntimeError("provider_profile_schema_invalid")
    source_rows = await db.all(
        text(
            f"""
            SELECT t.npi, t.provider_license_number, t.healthcare_provider_taxonomy_code,
                   n.provider_first_name, n.provider_last_name
              FROM {schema}.npi_taxonomy AS t
              JOIN {schema}.npi AS n ON n.npi = t.npi
             WHERE UPPER(COALESCE(t.provider_license_number_state_code, '')) = 'FL'
               AND NULLIF(t.provider_license_number, '') IS NOT NULL
               AND n.entity_type_code = 1
            """
        )
    )
    operation_result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source_row in source_rows:
        mapping = source_row._mapping
        license_key = _NON_ALNUM.sub("", str(mapping["provider_license_number"]).upper())
        if license_key:
            operation_result[license_key].append(
                {
                    "npi": int(mapping["npi"]),
                    "taxonomy": str(mapping["healthcare_provider_taxonomy_code"] or ""),
                    "first_name": mapping["provider_first_name"],
                    "last_name": mapping["provider_last_name"],
                    "license_number": mapping["provider_license_number"],
                }
            )
    return operation_result


def _match_master(
    source_row: Mapping[str, str],
    license_index: Mapping[str, list[dict[str, Any]]],
) -> tuple[int | None, str, dict[str, Any]]:
    profession_code = _first(source_row, "pro_cde", "profession_code")
    rank_code = _first(source_row, "rank_cde", "rank_code", "profession_rank_code")
    license_number = _first(source_row, "lic_nbr", "license_number", "license_nbr")
    candidates_by_npi: dict[int, dict[str, Any]] = {}
    tested = _license_candidates(license_number, profession_code, rank_code)
    for candidate_key in tested:
        for candidate in license_index.get(candidate_key, ()):
            if _is_taxonomy_compatible(profession_code, candidate["taxonomy"]):
                candidates_by_npi[candidate["npi"]] = candidate
    compatible_items = [
        candidate for candidate in candidates_by_npi.values()
        if _is_name_compatible(source_row, candidate)
    ]
    evidence_by_key = {
        "method": "exact_state_license_profession_name",
        "jurisdiction": "FL",
        "profession_code": profession_code or None,
        "rank_code": rank_code or None,
        "license_candidates": list(tested),
        "candidate_count": len(candidates_by_npi),
        "name_compatible_count": len(compatible_items),
    }
    if len(compatible_items) == 1:
        evidence_by_key["matched_license_number"] = compatible_items[0]["license_number"]
        evidence_by_key["taxonomy_code"] = compatible_items[0]["taxonomy"]
        return compatible_items[0]["npi"], "deterministic", evidence_by_key
    if candidates_by_npi and not compatible_items:
        return None, "identity_conflict", evidence_by_key
    if len(compatible_items) > 1:
        return None, "ambiguous", evidence_by_key
    return None, "unmatched", evidence_by_key


def _record_key(source: FloridaSource, row: Mapping[str, str], row_number: int) -> str:
    parts = [
        source.key,
        _first(row, "pro_cde", "profession_code"),
        _first(row, "lic_id", "license_id"),
        _first(row, "rec_id", "record_id"),
    ]
    if not parts[-1]:
        source_fields_by_key = {
            field_name: field_value
            for field_name, field_value in row.items()
            if not field_name.startswith("_source_")
        }
        row_hash = hashlib.sha256(
            json.dumps(
                source_fields_by_key,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if row.get("_source_parse_quarantine"):
            return f"{source.key}:quarantine:{row_hash}:{row_number}"
        return f"{source.key}:row-sha256:{row_hash}"
    return ":".join(parts)


def _fact_payload(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
    category: str | None = None,
    fact_type: str | None = None,
    display: str | None = None,
    value_json: Mapping[str, Any] | None = None,
    effective_start: str | None = None,
    effective_end: str | None = None,
    fact_key: str = "default",
    assertion_type: str | None = None,
    verification_status: str | None = None,
    sensitive: bool | None = None,
    public_default: bool | None = None,
    infer_effective_period: bool = True,
) -> dict[str, Any]:
    """Build the reviewed public and restricted payloads for one source fact."""
    resolved_category = category or profile_source.category
    resolved_fact_type = fact_type or profile_source.fact_type
    resolved_value = (
        dict(value_json)
        if value_json is not None
        else _without_empty(
            {
                key: field_value
                for key, field_value in source_row.items()
                if key not in _PROFILE_RAW_ONLY_FIELDS
            }
        )
    )
    logical_discriminator: Any = (
        fact_key
        if fact_key != "default"
        else resolved_value
    )
    logical_fact_key = hashlib.sha256(
        json.dumps(
            [resolved_category, resolved_fact_type, logical_discriminator],
            sort_keys=True,
            default=str,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return {
        "fact_id": hashlib.sha256(
            f"{run_id}:{record_id}:{logical_fact_key}".encode()
        ).hexdigest(),
        "run_id": run_id,
        "npi": npi,
        "source_record_id": record_id,
        "logical_fact_key": logical_fact_key,
        "category": resolved_category,
        "fact_type": resolved_fact_type,
        "display": display or _human_display(profile_source, source_row),
        "value_json": resolved_value,
        "availability": "available",
        "assertion_type": assertion_type or profile_source.assertion_type,
        "verification_status": verification_status or profile_source.verification_status,
        "effective_start": effective_start
        or (
            _first(source_row, "effective_date", "action_date", "orig_dte", "issue_date")
            if infer_effective_period
            else None
        )
        or None,
        "effective_end": effective_end
        or (
            _first(source_row, "expiration_date", "expr_dte", "end_date")
            if infer_effective_period
            else None
        )
        or None,
        "source_json": {
            "source_key": FL_MQA_SOURCE_KEY,
            "dataset": profile_source.key,
            "agency": FL_MQA_AGENCY,
            "jurisdiction": "FL",
            "artifact_id": artifact["artifact_id"],
            "content_sha256": artifact["content_sha256"],
            "source_url": artifact["source_url"],
            "source_record_id": record_id,
        },
        "sensitive": profile_source.sensitive if sensitive is None else sensitive,
        "public_default": (
            profile_source.public_default
            if public_default is None
            else public_default
        ),
        "published_at": _utcnow() if npi is not None else None,
    }


def _indicator_value(source_value: str) -> dict[str, Any]:
    value = source_value.strip()
    token = value.upper()
    if token in {"Y", "YES", "TRUE", "1", "X"}:
        return {"reported": True, "source_code": value}
    if token in {"N", "NO", "FALSE", "0"}:
        return {"reported": False, "source_code": value}
    return {"status": "source_reported", "source_value": value}


def _mapped_profile_data_fact(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one reviewed fact from a mapped practitioner-profile source."""
    field_map = _PROFILE_VALUE_FIELDS.get(profile_source.key)
    if field_map is None:
        raise RuntimeError(
            f"provider_profile_source_adapter_missing:{profile_source.key}"
        )
    date_fields = _PROFILE_DATE_VALUE_FIELDS.get(profile_source.key, frozenset())
    field_value_by_key: dict[str, Any] = {}
    for output_field, source_field in field_map:
        source_value = source_row.get(source_field, "")
        if not source_value:
            continue
        if output_field in date_fields:
            normalized_date, precision = _normalize_source_date(source_value)
            field_value_by_key[output_field] = normalized_date
            field_value_by_key[f"{output_field}_precision"] = precision
        else:
            field_value_by_key[output_field] = source_value
    field_value_by_key = _without_empty(field_value_by_key)
    display_values = [
        str(field_value_by_key[field])
        for field in _PROFILE_DISPLAY_VALUE_FIELDS.get(profile_source.key, ())
        if field_value_by_key.get(field)
    ]
    display_values = list(dict.fromkeys(display_values))
    display = (
        f"{profile_source.title}: {' — '.join(display_values)}"
        if display_values
        else profile_source.title
    )
    start_field, end_field = _PROFILE_EFFECTIVE_FIELDS.get(
        profile_source.key,
        (None, None),
    )
    return _fact_payload(
        profile_source,
        source_row,
        run_id=run_id,
        record_id=record_id,
        npi=npi,
        artifact=artifact,
        display=display,
        value_json=field_value_by_key,
        effective_start=(
            str(field_value_by_key.get(start_field) or "")
            if start_field
            else None
        )
        or None,
        effective_end=(
            str(field_value_by_key.get(end_field) or "")
            if end_field
            else None
        )
        or None,
        fact_key=field_value_by_key,
    )


def _profile_indicator_facts(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build reviewed practitioner-profile indicator facts."""
    safe_indicators = (
        ("other_health_degree", "health_degree", "Other health degree"),
        (
            "graduate_medical_education",
            "grad_med_edu",
            "Graduate medical education",
        ),
        (
            "professional_postgraduate_training",
            "prof_post_train",
            "Professional or postgraduate training",
        ),
        ("faculty_appointments", "faculty_appoint", "Faculty appointments"),
        ("staff_privileges", "staff_priv", "Staff privileges"),
        ("certifications", "certification", "Certifications"),
    )
    sections_by_key = {
        output_field: _indicator_value(source_row[source_field])
        for output_field, source_field, _label in safe_indicators
        if source_row.get(source_field)
    }
    facts: list[dict[str, Any]] = []
    if sections_by_key:
        available_labels = [
            label
            for output_field, _source_field, label in safe_indicators
            if sections_by_key.get(output_field, {}).get("reported") is True
        ]
        display = (
            "Profile information reported for: "
            + ", ".join(available_labels)
            if available_labels
            else "Profile information coverage reported"
        )
        facts.append(
            _fact_payload(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
                fact_type="profile_section_availability",
                display=display,
                value_json={"sections": sections_by_key},
                fact_key={"sections": sections_by_key},
            )
        )
    restricted_indicators = (
        (
            "criminal_offense_disclosure",
            "criminal_offense",
            "criminal_disclosures",
            "criminal_offense_disclosure_indicator",
        ),
        (
            "medicaid_program_disclosure",
            "medicaid_prgrm",
            "regulatory_actions",
            "medicaid_program_disclosure_indicator",
        ),
    )
    for output_field, source_field, category, fact_type in restricted_indicators:
        if not source_row.get(source_field):
            continue
        indicator = _indicator_value(source_row[source_field])
        facts.append(
            _fact_payload(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
                category=category,
                fact_type=fact_type,
                display=f"Restricted {output_field.replace('_', ' ')}",
                value_json={output_field: indicator},
                fact_key={output_field: indicator},
                sensitive=True,
                public_default=False,
            )
        )
    return facts


def _financial_responsibility_facts(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build reviewed financial-responsibility facts from one source row."""
    public_value = _without_empty(
        {
            "financial_responsibility": source_row.get("financial_resp", ""),
            "financial_exemption": (
                _indicator_value(source_row["financial_exempt"])
                if source_row.get("financial_exempt")
                else {}
            ),
            "insurance": _without_empty(
                {
                    "currently_insured": (
                        _indicator_value(source_row["insured"])
                        if source_row.get("insured")
                        else {}
                    ),
                    "insured_for_ten_years": (
                        _indicator_value(source_row["insured_10_yr"])
                        if source_row.get("insured_10_yr")
                        else {}
                    ),
                }
            ),
        }
    )
    facts: list[dict[str, Any]] = []
    if public_value:
        display_values = [
            field_value
            for field_value in (
                source_row.get("financial_resp", ""),
                (
                    "exemption reported"
                    if source_row.get("financial_exempt", "").upper()
                    in {"Y", "YES", "TRUE", "1", "X"}
                    else ""
                ),
                (
                    "insurance reported"
                    if source_row.get("insured", "").upper()
                    in {"Y", "YES", "TRUE", "1", "X"}
                    else ""
                ),
            )
            if field_value
        ]
        facts.append(
            _fact_payload(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
                display=(
                    f"{profile_source.title}: {' — '.join(display_values)}"
                    if display_values
                    else profile_source.title
                ),
                value_json=public_value,
                fact_key=public_value,
            )
        )
    if source_row.get("liability_claim"):
        liability_indicator = _indicator_value(source_row["liability_claim"])
        facts.append(
            _fact_payload(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
                category="liability_claims",
                fact_type="liability_claim_indicator",
                display="Restricted liability claim indicator",
                value_json={
                    "liability_claim_indicator": liability_indicator
                },
                fact_key={
                    "liability_claim_indicator": liability_indicator
                },
                sensitive=True,
                public_default=False,
            )
        )
    return facts


def _profile_data_facts(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    if profile_source.key == "profile_indicators":
        return _profile_indicator_facts(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    if profile_source.key == "financial_responsibility":
        return _financial_responsibility_facts(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    return [
        _mapped_profile_data_fact(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    ]


def _profile_master_facts(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build reviewed identity and biography facts from a profile master row."""
    facts: list[dict[str, Any]] = []
    given_items = [
        field_value
        for field_value in (source_row.get("f_name", ""), source_row.get("m_name", ""))
        if field_value
    ]
    suffix = [source_row["name_suffix"]] if source_row.get("name_suffix") else []
    display_name = " ".join(
        [*given_items, source_row.get("l_name", ""), *suffix]
    ).strip()
    if display_name:
        name = _without_empty(
            {
                "text": display_name,
                "family": source_row.get("l_name", ""),
                "given": given_items,
                "suffix": suffix,
            }
        )
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="identity", fact_type="name",
                display=f"Practitioner name: {display_name}", value_json=name,
                fact_key={"name": name},
                assertion_type="state_reported",
                verification_status="government_source",
                infer_effective_period=False,
            )
        )
    birth_year_range = source_row.get("birth_year_range", "")
    reported_range = normalize_reported_range(birth_year_range)
    if reported_range:
        range_value_by_key = reported_range["value"]
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="demographics",
                fact_type=reported_range["fact_type"],
                display=reported_range["display"],
                value_json=range_value_by_key,
                fact_key={reported_range["fact_type"]: range_value_by_key},
                infer_effective_period=False,
            )
        )
    rank_effective_date, _ = _normalize_source_date(source_row.get("rank_efct_dte", ""))
    original_issue_date, _ = _normalize_source_date(source_row.get("orig_dte", ""))
    expiration_date, _ = _normalize_source_date(source_row.get("expr_dte", ""))
    license_value = _without_empty(
        {
            "jurisdiction": "FL",
            "profession_code": source_row.get("pro_cde", ""),
            "license_number": source_row.get("lic_nbr", ""),
            "status_code": source_row.get("lic_sta_cde", ""),
            "status": source_row.get("lic_sta_desc", ""),
            "active_status_code": source_row.get("lic_actv_sta_cde", ""),
            "active_status": source_row.get("lic_actv_sta_desc", ""),
            "rank_code": source_row.get("rank_cde", ""),
            "rank": source_row.get("rank_desc", ""),
            "rank_effective_date": rank_effective_date,
            "original_issue_date": original_issue_date,
            "expiration_date": expiration_date,
        }
    )
    license_display = " — ".join(
        field_value
        for field_value in (
            source_row.get("lic_nbr", ""),
            source_row.get("lic_sta_desc", ""),
            source_row.get("rank_desc", ""),
        )
        if field_value
    )
    facts.append(
        _fact_payload(
            profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
            artifact=artifact, category="licenses", fact_type="state_license",
            display=f"Florida license: {license_display}", value_json=license_value,
            effective_start=original_issue_date or None,
            effective_end=expiration_date or None,
            fact_key=(
                "state_license:FL:"
                f"{_NON_ALNUM.sub('', source_row.get('lic_nbr', '').upper())}:"
                f"{source_row.get('pro_cde', '')}"
            ),
            assertion_type="state_reported",
            verification_status="government_source",
        )
    )
    other_license = source_row.get("other_license", "").upper()
    if other_license:
        other_license_labels_by_key = {
            "Y": (True, "Reported another state license"),
            "N": (False, "Reported no other state license"),
        }
        reported, other_license_display = other_license_labels_by_key.get(
            other_license,
            (None, f"Other state license indicator: {other_license}"),
        )
        other_license_value = _without_empty(
            {"reported": reported, "source_code": other_license}
        )
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="licenses",
                fact_type="other_state_license_indicator",
                display=other_license_display,
                value_json=other_license_value,
                fact_key={"other_state_license_indicator": other_license_value},
                infer_effective_period=False,
            )
        )
    practice_start = source_row.get("yr_began_practice", "")
    if practice_start:
        normalized_practice_start, precision = _normalize_source_date(practice_start)
        practice_start_value_by_key = {
            "start": normalized_practice_start,
            "precision": precision,
        }
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="professional_experience",
                fact_type="practice_start",
                display=f"Began practicing: {practice_start}",
                value_json=practice_start_value_by_key,
                effective_start=normalized_practice_start,
                fact_key={"practice_start": practice_start_value_by_key},
                infer_effective_period=False,
            )
        )
    nica_code = source_row.get("nica_payment", "").upper()
    if nica_code:
        nica_status = {
            "Y": ("reported_paid", "NICA assessment reported paid"),
            "E": ("reported_exempt", "NICA assessment reported exempt"),
            "N": ("reported_not_paid", "NICA assessment reported not paid"),
        }.get(nica_code, ("reported_unknown", f"NICA assessment code: {nica_code}"))
        nica_value_by_key = {"status": nica_status[0], "source_code": nica_code}
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="program_reports",
                fact_type="nica_assessment_status", display=nica_status[1],
                value_json=nica_value_by_key,
                fact_key={"nica_assessment_status": nica_value_by_key},
                infer_effective_period=False,
            )
        )
    addresses_by_key: dict[str, dict[str, Any]] = {}
    for prefix, location_type in (
        ("ml_", "mailing"),
        ("", "practice_primary"),
        ("pl2_", "practice_secondary_2"),
        ("pl3_", "practice_secondary_3"),
    ):
        address = _address_value(source_row, prefix=prefix, location_type=location_type)
        if address is None:
            continue
        address_without_role_by_key = {
            key: field_value for key, field_value in address.items() if key != "location_type"
        }
        address_key = json.dumps(address_without_role_by_key, sort_keys=True)
        existing_address = addresses_by_key.setdefault(
            address_key,
            {**address_without_role_by_key, "location_types": []},
        )
        existing_address["location_types"].append(location_type)
    for address_key, address in sorted(addresses_by_key.items()):
        address["location_types"] = sorted(set(address["location_types"]))
        facts.append(
            _fact_payload(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact, category="locations", fact_type="provider_address",
                display=_address_display(address), value_json=address,
                fact_key=hashlib.sha256(address_key.encode()).hexdigest()[:16],
                infer_effective_period=False,
            )
        )
    return facts


def _state_license_fact_payload(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Publish reviewed license fields while keeping contact/identity data raw-only."""
    original_date, _ = _normalize_source_date(
        _first(source_row, "orig_dte", "original_date")
    )
    expiration_date, _ = _normalize_source_date(
        _first(source_row, "expr_dte", "expire_date")
    )
    status_effective_date, _ = _normalize_source_date(
        source_row.get("status_effective_date", "")
    )
    profession_code = _first(source_row, "pro_cde", "profession_code")
    rank_code = _first(source_row, "rank_cde", "rank_code")
    license_number = _first(source_row, "lic_nbr", "license_number")
    status = _first(source_row, "lic_sta_desc", "license_status_description")
    active_status = _first(
        source_row,
        "lic_actv_sta_desc",
        "license_active_status_description",
    )
    safe_indicators = _without_empty(
        {
            "multi_state_license": source_row.get(
                "multi_state_license_indicator",
                "",
            ),
            "prescribing": source_row.get("prescribe_ind", ""),
            "dispensing": source_row.get("dispensing_ind", ""),
            "other_license": source_row.get("other_license", ""),
        }
    )
    field_value = _without_empty(
        {
            "jurisdiction": "FL",
            "profession_code": profession_code,
            "profession": source_row.get("profession_name", ""),
            "rank_code": rank_code,
            "license_number": license_number,
            "status": status,
            "active_status": active_status,
            "original_issue_date": original_date,
            "expiration_date": expiration_date,
            "status_effective_date": status_effective_date,
            "modifiers": source_row.get("mod_cdes", ""),
            "license_indicators": safe_indicators,
        }
    )
    display_parts = [
        field_value
        for field_value in (
            source_row.get("profession_name", ""),
            license_number,
            status,
            active_status,
        )
        if field_value
    ]
    display = " — ".join(display_parts)
    return _fact_payload(
        profile_source,
        source_row,
        run_id=run_id,
        record_id=record_id,
        npi=npi,
        artifact=artifact,
        category="licenses",
        fact_type=profile_source.fact_type,
        display=f"{profile_source.title}: {display}" if display else profile_source.title,
        value_json=field_value,
        effective_start=status_effective_date or original_date or None,
        effective_end=expiration_date or None,
        fact_key={
            "jurisdiction": "FL",
            "profession_code": profession_code,
            "rank_code": rank_code,
            "license_number": _NON_ALNUM.sub("", license_number.upper()),
            "status": status,
            "active_status": active_status,
            "status_effective_date": status_effective_date,
        },
        assertion_type="state_reported",
        verification_status="government_source",
    )


def _state_license_facts(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    facts = [
        _state_license_fact_payload(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    ]
    restricted_indicators = _without_empty(
        {
            "board_action": source_row.get("board_action_indicator", ""),
            "administrative_complaints": source_row.get(
                "administrative_complaints_indicator",
                "",
            ),
            "emergency_order": source_row.get("emergency_order_indicator", ""),
            "final_order": source_row.get("final_order_indicator", ""),
        }
    )
    if not restricted_indicators:
        return facts
    license_number = _first(source_row, "lic_nbr", "license_number")
    field_value_by_key = {
        "jurisdiction": "FL",
        "license_number": license_number,
        "regulatory_indicators": restricted_indicators,
    }
    facts.append(
        _fact_payload(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
            category="regulatory_actions",
            fact_type="license_regulatory_indicators",
            display="Restricted Florida license regulatory indicators",
            value_json=field_value_by_key,
            fact_key=field_value_by_key,
            assertion_type="state_reported",
            verification_status="government_source",
            sensitive=True,
            public_default=False,
        )
    )
    return facts


def _administrative_complaint_fact(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    activity_date, _ = _normalize_source_date(source_row.get("case_activity_date", ""))
    field_value = _without_empty(
        {
            "jurisdiction": "FL",
            "case_number": source_row.get("case_number", ""),
            "activity_type": source_row.get("case_activity_type", ""),
            "activity_date": activity_date,
            "profession": source_row.get("profession", ""),
            "license_number": source_row.get("license_number", ""),
            "disposition": "allegation_not_final_action",
        }
    )
    details = " — ".join(
        profile_item
        for profile_item in (
            source_row.get("case_number", ""),
            source_row.get("case_activity_type", ""),
            activity_date,
        )
        if profile_item
    )
    return _fact_payload(
        profile_source,
        source_row,
        run_id=run_id,
        record_id=record_id,
        npi=npi,
        artifact=artifact,
        display=(
            f"Administrative complaint (allegation): {details}"
            if details
            else profile_source.title
        ),
        value_json=field_value,
        effective_start=activity_date or None,
        fact_key={
            "case_number": source_row.get("case_number", ""),
            "activity_type": source_row.get("case_activity_type", ""),
            "activity_date": activity_date,
        },
    )


def _pain_management_fact(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one reviewed pain-management report fact."""
    report_period = _without_empty(
        {"year": source_row.get("year", ""), "quarter": source_row.get("qtr", "")}
    )
    field_value = _without_empty(
        {
            "jurisdiction": "FL",
            "clinic": _without_empty(
                {
                    "name": source_row.get("clinic_name", ""),
                    "license_number": source_row.get("lic_nbr", ""),
                    "license_status": source_row.get("lic_status", ""),
                    "practice_location": source_row.get("pl_address", ""),
                }
            ),
            "reporting_period": report_period,
            "reporting_provider": _without_empty(
                {
                    "profession": source_row.get("reporting_phy_prof", ""),
                    "license_number": source_row.get("reporting_phy_lic_nbr", ""),
                }
            ),
            "patient_counts": _without_empty(
                {
                    "new": source_row.get("new_cnt", ""),
                    "repeat": source_row.get("repeat_cnt", ""),
                    "discharged_for_abuse": source_row.get("abuse_cnt", ""),
                    "discharged_for_diversion": source_row.get("divrsn_cnt", ""),
                    "out_of_state": source_row.get("oos_cnt", ""),
                }
            ),
        }
    )
    period_display = " ".join(
        profile_item
        for profile_item in (
            source_row.get("year", ""),
            f"Q{source_row['qtr']}" if source_row.get("qtr") else "",
        )
        if profile_item
    )
    details = " — ".join(
        profile_item
        for profile_item in (source_row.get("clinic_name", ""), period_display)
        if profile_item
    )
    return _fact_payload(
        profile_source,
        source_row,
        run_id=run_id,
        record_id=record_id,
        npi=npi,
        artifact=artifact,
        display=f"Pain management clinic report: {details}" if details else profile_source.title,
        value_json=field_value,
        fact_key={
            "clinic_license": source_row.get("lic_nbr", ""),
            "reporting_provider_license": source_row.get(
                "reporting_phy_lic_nbr",
                "",
            ),
            "year": source_row.get("year", ""),
            "quarter": source_row.get("qtr", ""),
        },
    )


def _pharmacy_relationship_fact(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one reviewed pharmacy relationship fact."""
    original_date, _ = _normalize_source_date(source_row.get("pharm_orig_dte", ""))
    expiration_date, _ = _normalize_source_date(source_row.get("pharm_expr_dte", ""))
    status_effective_date, _ = _normalize_source_date(
        source_row.get("pharm_stat_efctv_dte", "")
    )
    pharmacy_location = _without_empty(
        {
            "address_line_1": source_row.get("pharm_pl_addr_l1", ""),
            "address_line_2": source_row.get("pharm_pl_addr_l2", ""),
            "address_line_3": source_row.get("pharm_pl_addr_l3", ""),
            "city": source_row.get("pharm_pl_cty", ""),
            "state": source_row.get("pharm_pl_st", ""),
            "postal_code": source_row.get("pharm_pl_zip", ""),
        }
    )
    field_value = _without_empty(
        {
            "jurisdiction": "FL",
            "relationship_profession": source_row.get("rltn_prof_nme", ""),
            "related_license": _without_empty(
                {
                    "license_number": source_row.get("rltn_lic_nbr", ""),
                    "status_code": source_row.get("rltn_lic_sta_cde", ""),
                    "status": source_row.get("rltn_lic_sta_desc", ""),
                    "secondary_status_code": source_row.get(
                        "rltn_lic_sec_sta_cde",
                        "",
                    ),
                    "secondary_status": source_row.get(
                        "rltn_lic_sec_sta_desc",
                        "",
                    ),
                }
            ),
            "pharmacy": _without_empty(
                {
                    "name": source_row.get("pharm_key_name", ""),
                    "doing_business_as": source_row.get("pharm_dba_name", ""),
                    "license_number": source_row.get("pharm_lic_nbr", ""),
                    "license_status_code": source_row.get("pharm_lic_sta_cde", ""),
                    "license_status": source_row.get("pharm_lic_sta_desc", ""),
                    "original_issue_date": original_date,
                    "status_effective_date": status_effective_date,
                    "expiration_date": expiration_date,
                    "practice_location": pharmacy_location,
                    "phone": source_row.get("pharm_phne_nbr", ""),
                    "phone_extension": source_row.get("pharm_phne_ext", ""),
                }
            ),
        }
    )
    relationship = source_row.get("rltn_prof_nme", "")
    pharmacy_name = source_row.get("pharm_key_name", "")
    details = " — ".join(profile_item for profile_item in (relationship, pharmacy_name) if profile_item)
    return _fact_payload(
        profile_source,
        source_row,
        run_id=run_id,
        record_id=record_id,
        npi=npi,
        artifact=artifact,
        display=f"Pharmacy relationship: {details}" if details else profile_source.title,
        value_json=field_value,
        effective_start=status_effective_date or original_date or None,
        effective_end=expiration_date or None,
        fact_key={
            "pharmacy_license": source_row.get("pharm_lic_nbr", ""),
            "related_license": source_row.get("rltn_lic_nbr", ""),
            "relationship_profession": relationship,
        },
    )


def _medical_cannabis_fact(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Build one reviewed medical-cannabis authorization fact."""
    course = source_row.get("course_type", "")
    course_token = course.strip().upper()
    if course_token in {"D", "DIRECTOR"}:
        authorization_type = "dispensing_organization_medical_director_eligibility"
        semantic_fact_type = "dispensing_organization_medical_director_eligibility"
        display = "Eligible to serve as a dispensing organization medical director"
    elif course_token in {"P", "PHYSICIAN"}:
        authorization_type = "medical_cannabis_ordering"
        semantic_fact_type = "medical_cannabis_ordering_authorization"
        display = "Authorized to order medical cannabis and low-THC cannabis"
    else:
        authorization_type = "medical_cannabis_course_listing"
        semantic_fact_type = "medical_cannabis_course_listing"
        display = "Florida medical cannabis course listing"
    practice_location = _without_empty(
        {
            "address_line_1": source_row.get("pl_addr_line1", ""),
            "address_line_2": source_row.get("pl_addr_line2", ""),
            "address_line_3": source_row.get("pl_addr_line3", ""),
            "city": source_row.get("pl_addr_cty", ""),
            "state": source_row.get("pl_st_cde", ""),
            "postal_code": source_row.get("pl_zip", ""),
            "county": source_row.get("pl_cnty", ""),
        }
    )
    completion_date_source = source_row.get("dte_compl", "")
    completion_date, completion_precision = _normalize_source_date(completion_date_source)
    field_value = _without_empty(
        {
            "jurisdiction": "FL",
            "authorization_type": authorization_type,
            "course_type": course,
            "course_type_code": course_token,
            "course_completed_date": completion_date,
            "course_completed_precision": completion_precision if completion_date else "",
            "submitted_by_code": source_row.get("submitted_by", ""),
            "license_number": source_row.get("lic_nbr", ""),
            "practice_location": practice_location,
            "practice_phone": source_row.get("phne_nbr", ""),
            "specialties": [
                field_value.strip()
                for field_value in source_row.get("specialties", "").split("|")
                if field_value.strip()
            ],
            "location_context": "source_reported_practice_contact",
            "network_bound": False,
        }
    )
    if completion_date:
        display = f"{display} — course completed {completion_date}"
    return _fact_payload(
        profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
        artifact=artifact, category="prescribing_authorizations",
        fact_type=semantic_fact_type, display=display,
        value_json=field_value,
    )


def _facts_for_row(
    profile_source: FloridaSource,
    source_row: Mapping[str, str],
    *,
    run_id: str,
    record_id: str,
    npi: int | None,
    artifact: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Map one normalized source row into reviewed provider-profile facts."""
    if profile_source.key == "counties":
        return []
    if profile_source.key == "profile_master":
        return _profile_master_facts(
            profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
            artifact=artifact,
        )
    if profile_source.key in {
        "license_status",
        "licensure_current",
        "licensure_all_statuses",
    }:
        return _state_license_facts(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    if profile_source.path == "/ProfileData":
        return _profile_data_facts(
            profile_source,
            source_row,
            run_id=run_id,
            record_id=record_id,
            npi=npi,
            artifact=artifact,
        )
    if profile_source.key == "medical_cannabis_authorization":
        return [
            _medical_cannabis_fact(
                profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
                artifact=artifact,
            )
        ]
    if profile_source.key == "administrative_complaints":
        return [
            _administrative_complaint_fact(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
            )
        ]
    if profile_source.key == "pain_management_report":
        return [
            _pain_management_fact(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
            )
        ]
    if profile_source.key == "pharmacy_pharmacist":
        return [
            _pharmacy_relationship_fact(
                profile_source,
                source_row,
                run_id=run_id,
                record_id=record_id,
                npi=npi,
                artifact=artifact,
            )
        ]
    return [
        _fact_payload(
            profile_source, source_row, run_id=run_id, record_id=record_id, npi=npi,
            artifact=artifact,
        )
    ]


def _projectable_fact_npi(
    npi: int | None,
    match_status: str,
) -> int | None:
    """Facts without a deterministic NPI cannot appear in a provider profile."""
    if match_status != "deterministic" or npi is None:
        return None
    return int(npi)


def _profile_supplement_match(
    identity: tuple[int | None, str, str] | None,
    *,
    profession_code: str,
    license_id: str,
    only_matched: bool,
) -> tuple[int | None, str, dict[str, Any]] | None:
    """Retain supplemental rows even when the profile master has no join row."""
    if identity is None:
        if only_matched:
            return None
        return (
            None,
            "unmatched_master_identity",
            {
                "method": "profile_master_profession_license_id",
                "profession_code": profession_code,
                "license_id": license_id,
                "master_identity_found": False,
            },
        )
    npi, match_status, _license_number = identity
    return (
        npi,
        match_status,
        {
            "method": "profile_master_profession_license_id",
            "profession_code": profession_code,
            "license_id": license_id,
            "master_identity_found": True,
        },
    )


def _retained_source_record(
    *,
    record_id: str,
    run_id: str,
    artifact_id: str,
    source_key: str,
    source_record_key: str,
    profession_code: str | None,
    license_id: str | None,
    license_number: str | None,
    raw_payload: Mapping[str, Any],
    normalized_payload: Mapping[str, Any],
    matched_npi: int | None,
    match_status: str,
    match_evidence: Mapping[str, Any],
    row_number: int,
) -> dict[str, Any]:
    """Retain rematch inputs even when no provider fact can be projected."""
    return {
        "record_id": record_id,
        "run_id": run_id,
        "artifact_id": artifact_id,
        "source_key": source_key,
        "source_record_key": source_record_key,
        "profession_code": profession_code,
        "license_id": license_id,
        "license_number": license_number,
        "raw_payload": dict(raw_payload),
        "normalized_payload": dict(normalized_payload),
        "matched_npi": matched_npi,
        "match_status": match_status,
        "match_evidence": dict(match_evidence),
        "row_number": row_number,
    }


def _env_positive_int(name: str, default: int) -> int:
    try:
        return max(int(os.getenv(name, str(default))), 1)
    except (TypeError, ValueError):
        return default


def _is_copy_upsert_enabled() -> bool:
    return os.getenv("HLTHPRT_FL_MQA_COPY_UPSERT", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def _copy_upsert_min_rows() -> int:
    return _env_positive_int(
        "HLTHPRT_FL_MQA_COPY_UPSERT_MIN_ROWS",
        DEFAULT_COPY_UPSERT_MIN_ROWS,
    )


def _copy_upsert_batch_rows() -> int:
    return _env_positive_int(
        "HLTHPRT_FL_MQA_COPY_UPSERT_BATCH_ROWS",
        DEFAULT_COPY_UPSERT_BATCH_ROWS,
    )


def _validated_identifier(identifier: Any) -> str:
    value = str(identifier or "")
    if (
        not _POSTGRES_IDENTIFIER.fullmatch(value)
        or len(value.encode("utf-8")) > _POSTGRES_IDENTIFIER_MAX_BYTES
    ):
        raise ValueError(f"unsafe PostgreSQL identifier: {value!r}")
    return value


def _quoted_identifier(identifier: Any) -> str:
    return f'"{_validated_identifier(identifier)}"'


def _copy_stage_table_name(table_name: str) -> str:
    identifier = f"fl_pp_{_validated_identifier(table_name)}_{uuid.uuid4().hex[:12]}"
    return _validated_identifier(identifier)


def _strip_postgres_nuls(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace("\x00", "")
    if isinstance(value, list):
        return [_strip_postgres_nuls(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_strip_postgres_nuls(item) for item in value)
    if isinstance(value, dict):
        return {
            _strip_postgres_nuls(key): _strip_postgres_nuls(item)
            for key, item in value.items()
        }
    return value


def _copy_json_default(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _copy_value_for_type(column_type: Any, field_value: Any) -> Any:
    field_value = _strip_postgres_nuls(field_value)
    if field_value is None:
        return None
    if isinstance(column_type, SQLAlchemyJSON):
        return json.dumps(
            field_value,
            sort_keys=True,
            separators=(",", ":"),
            default=_copy_json_default,
        )
    if isinstance(column_type, ARRAY):
        if isinstance(field_value, str):
            try:
                field_value = json.loads(field_value)
            except json.JSONDecodeError as exc:
                raise ValueError("COPY array value must be a JSON array or sequence") from exc
        if not isinstance(field_value, (list, tuple)):
            raise ValueError("COPY array value must be a sequence")
        return [
            _copy_value_for_type(column_type.item_type, profile_item)
            for profile_item in field_value
        ]
    if isinstance(column_type, DateTime):
        if isinstance(field_value, str):
            field_value = datetime.fromisoformat(field_value.strip().replace("Z", "+00:00"))
        if not isinstance(field_value, datetime):
            raise ValueError("COPY timestamp value must be datetime or ISO-8601 text")
        if not getattr(column_type, "timezone", False) and field_value.tzinfo is not None:
            field_value = field_value.astimezone(UTC).replace(tzinfo=None)
        return field_value
    if isinstance(column_type, Date):
        if isinstance(field_value, datetime):
            return field_value.date()
        if isinstance(field_value, str):
            return date.fromisoformat(field_value.strip())
        if not isinstance(field_value, date):
            raise ValueError("COPY date value must be date or ISO-8601 text")
    return field_value


def _copy_records(
    table: Any,
    rows: list[dict[str, Any]],
) -> tuple[list[str], list[tuple[Any, ...]]]:
    columns = list(table.columns)
    column_names = [_validated_identifier(column.name) for column in columns]
    return (
        column_names,
        [
            tuple(
                _copy_value_for_type(column.type, row.get(column.name))
                for column in columns
            )
            for row in rows
        ],
    )


async def _copy_upsert_chunk_on_connection(
    connection: Any,
    model: Any,
    source_rows: list[dict[str, Any]],
    key: str,
) -> None:
    """COPY one bounded chunk and merge it in the same short transaction."""
    table = model.__table__
    table_name = _validated_identifier(table.name)
    schema_name = _validated_identifier(table.schema or "mrf")
    key = _validated_identifier(key)
    table_columns = {_validated_identifier(column.name) for column in table.columns}
    if key not in table_columns:
        raise ValueError(f"conflict key {key!r} is not a column of {table_name!r}")

    raw_connection = connection.raw_connection
    driver_connection = getattr(
        raw_connection,
        "driver_connection",
        raw_connection,
    )
    copy_records_to_table = getattr(
        driver_connection,
        "copy_records_to_table",
        None,
    )
    if copy_records_to_table is None:
        raise _CopyUpsertUnavailable(
            "active database driver lacks copy_records_to_table"
        )

    column_names, copy_rows = _copy_records(table, source_rows)
    stage_table = _copy_stage_table_name(table_name)
    quoted_stage = _quoted_identifier(stage_table)
    target_ref = (
        f"{_quoted_identifier(schema_name)}.{_quoted_identifier(table_name)}"
    )
    quoted_columns = ", ".join(
        _quoted_identifier(column_name) for column_name in column_names
    )
    update_columns = [
        column_name for column_name in column_names if column_name != key
    ]
    conflict_action = (
        "DO UPDATE SET "
        + ", ".join(
            f"{_quoted_identifier(column_name)} = "
            f"EXCLUDED.{_quoted_identifier(column_name)}"
            for column_name in update_columns
        )
        if update_columns
        else "DO NOTHING"
    )

    await connection.status(
        f"""
        CREATE TEMP TABLE {quoted_stage}
        (LIKE {target_ref} INCLUDING DEFAULTS) ON COMMIT DROP;
        """
    )
    await copy_records_to_table(
        stage_table,
        columns=column_names,
        records=copy_rows,
    )
    await connection.status(
        f"""
        INSERT INTO {target_ref} ({quoted_columns})
        SELECT {quoted_columns}
          FROM {quoted_stage}
        ON CONFLICT ({_quoted_identifier(key)}) {conflict_action};
        """
    )


async def _copy_upsert_chunk(
    model: Any,
    rows: list[dict[str, Any]],
    key: str,
) -> None:
    async with db.acquire() as connection:
        try:
            await _copy_upsert_chunk_on_connection(
                connection,
                model,
                rows,
                key,
            )
        except NotImplementedError as exc:
            raise _CopyUpsertUnavailable(str(exc)) from exc


async def _upsert_rows_values(
    model: Any,
    rows: list[dict[str, Any]],
    key: str,
) -> None:
    """Preserve the original SQLAlchemy chunked-upsert compatibility path."""
    for offset in range(0, len(rows), 1_000):
        chunk = rows[offset : offset + 1_000]
        statement = db.insert(model.__table__).values(chunk)
        update_fields_by_key = {
            column.name: getattr(statement.excluded, column.name)
            for column in model.__table__.columns
            if column.name != key
        }
        await statement.on_conflict_do_update(
            index_elements=[key],
            set_=update_fields_by_key,
        ).status()


def _coalesced_upsert_rows(
    rows: Iterable[dict[str, Any]],
    key: str,
) -> list[dict[str, Any]]:
    """Keep the final payload for each conflict key in one bounded batch."""
    row_by_conflict_key: dict[Any, dict[str, Any]] = {}
    for row in rows:
        row_by_conflict_key[row[key]] = row
    return list(row_by_conflict_key.values())


async def _upsert_rows(model: Any, rows: list[dict[str, Any]], key: str) -> None:
    if not rows:
        return
    rows = _coalesced_upsert_rows(rows, key)
    table_name = _validated_identifier(model.__table__.name)
    if (
        table_name not in _COPY_UPSERT_TABLES
        or not _is_copy_upsert_enabled()
        or len(rows) < _copy_upsert_min_rows()
    ):
        await _upsert_rows_values(model, rows, key)
        return

    batch_rows = _copy_upsert_batch_rows()
    is_copy_available = True
    for offset in range(0, len(rows), batch_rows):
        chunk = rows[offset : offset + batch_rows]
        if is_copy_available:
            try:
                await _copy_upsert_chunk(model, chunk, key)
                continue
            except _CopyUpsertUnavailable:
                is_copy_available = False
        await _upsert_rows_values(model, chunk, key)


async def _retained_import_counts(run_id: str) -> dict[str, int]:
    """Count unique rows retained after content-key conflict resolution."""
    retained_counts = await db.first(
        select(
            func.count().label("source_records"),
            func.count()
            .filter(ProviderProfileSourceRecord.match_status == "deterministic")
            .label("matched_records"),
            func.count()
            .filter(
                ProviderProfileSourceRecord.match_status == "deterministic",
                ProviderProfileSourceRecord.matched_npi.is_not(None),
            )
            .label("projectable_records"),
            (
                select(func.count())
                .select_from(ProviderProfileFact)
                .where(ProviderProfileFact.run_id == run_id)
                .scalar_subquery()
            ).label("facts"),
        )
        .select_from(ProviderProfileSourceRecord)
        .where(ProviderProfileSourceRecord.run_id == run_id)
    )
    retained_counts_by_key = retained_counts._mapping
    retained_source_records = int(retained_counts_by_key["source_records"] or 0)
    return {
        "retained_source_records": retained_source_records,
        "retained_facts": int(retained_counts_by_key["facts"] or 0),
        "retained_matched_records": int(
            retained_counts_by_key["matched_records"] or 0
        ),
        "retained_non_projectable_records": retained_source_records
        - int(retained_counts_by_key["projectable_records"] or 0),
    }


async def _claim_import_run(run_row: Mapping[str, Any]) -> None:
    """Atomically claim one run scope; completed or partial replay fails closed."""
    run_id = str(run_row.get("run_id") or "")
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ValueError("provider_profile_run_id_invalid")
    statement = (
        db.insert(ProviderProfileImportRun.__table__)
        .values(dict(run_row))
        .on_conflict_do_nothing(index_elements=["run_id"])
        .returning(ProviderProfileImportRun.run_id)
    )
    claimed_run_id = await statement.scalar()
    if claimed_run_id == run_id:
        return
    existing_status = await db.scalar(
        select(ProviderProfileImportRun.status).where(
            ProviderProfileImportRun.run_id == run_id
        )
    )
    if existing_status == "completed":
        raise RuntimeError(
            f"provider_profile_run_already_completed:{run_id}"
        )
    raise RuntimeError(
        "provider_profile_run_scope_exists:"
        f"{run_id}:{existing_status or 'unknown'}"
    )


def _failure_status_attempts() -> int:
    return _env_positive_int(
        "HLTHPRT_FL_MQA_FAILURE_STATUS_ATTEMPTS",
        DEFAULT_FAILURE_STATUS_ATTEMPTS,
    )


def _failure_status_timeout_seconds() -> float:
    try:
        return max(
            float(
                os.getenv(
                    "HLTHPRT_FL_MQA_FAILURE_STATUS_TIMEOUT_SECONDS",
                    str(DEFAULT_FAILURE_STATUS_TIMEOUT_SECONDS),
                )
            ),
            0.1,
        )
    except (TypeError, ValueError):
        return DEFAULT_FAILURE_STATUS_TIMEOUT_SECONDS


def _failure_status_window_seconds() -> float:
    try:
        return max(
            float(
                os.getenv(
                    "HLTHPRT_FL_MQA_FAILURE_STATUS_WINDOW_SECONDS",
                    str(DEFAULT_FAILURE_STATUS_WINDOW_SECONDS),
                )
            ),
            0.1,
        )
    except (TypeError, ValueError):
        return DEFAULT_FAILURE_STATUS_WINDOW_SECONDS


def _exception_chain(exc: BaseException) -> Iterator[BaseException]:
    seen_items: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen_items:
        seen_items.add(id(current))
        yield current
        current = (
            getattr(current, "orig", None)
            or current.__cause__
            or current.__context__
        )


def _is_transient_database_error(exc: BaseException) -> bool:
    return any(
        isinstance(item, (ConnectionError, TimeoutError))
        or type(item).__name__ in _TRANSIENT_DATABASE_ERROR_NAMES
        for item in _exception_chain(exc)
    )


async def _dispose_failed_database_pool(timeout_seconds: float) -> None:
    """Drop stale pooled sockets without changing the configured DB engine."""
    engine = getattr(db, "engine", None)
    dispose = getattr(engine, "dispose", None)
    if not callable(dispose):
        return
    try:
        await asyncio.wait_for(
            dispose(),
            timeout=max(min(timeout_seconds, 5.0), 0.1),
        )
    except Exception:
        # The next status attempt is authoritative; pool disposal is only a
        # bounded aid for PostgreSQL restart/recovery.
        return


async def _mark_failed_run_status(
    *,
    run_id: str,
    run_row: dict[str, Any],
    original_error: BaseException,
    cleanup_error: str | None,
) -> str | None:
    """Best-effort failure status that never overwrites an atomic completion."""
    error_payload_by_key = {
        "type": type(original_error).__name__,
        "message": str(original_error),
    }
    if cleanup_error:
        error_payload_by_key["stage_cleanup_error"] = cleanup_error
    status_values_by_key = {
        "status": "failed",
        "metrics": run_row.get("metrics") or {},
        "error": error_payload_by_key,
        "finished_at": _utcnow(),
    }
    attempts = _failure_status_attempts()
    timeout_seconds = _failure_status_timeout_seconds()
    loop = asyncio.get_running_loop()
    deadline = loop.time() + _failure_status_window_seconds()
    last_error: Exception | None = None
    for attempt in range(attempts):
        remaining_seconds = deadline - loop.time()
        if remaining_seconds <= 0:
            break
        try:
            statement = (
                db.update(ProviderProfileImportRun.__table__)
                .where(
                    ProviderProfileImportRun.run_id == run_id,
                    ProviderProfileImportRun.status != "completed",
                )
                .values(**status_values_by_key)
            )
            affected_rows = await asyncio.wait_for(
                statement.status(),
                timeout=min(timeout_seconds, remaining_seconds),
            )
            if affected_rows:
                run_row.update(status_values_by_key)
            return None
        except Exception as exc:
            last_error = exc
            if (
                attempt + 1 >= attempts
                or not _is_transient_database_error(exc)
            ):
                break
            remaining_seconds = deadline - loop.time()
            if remaining_seconds <= 0:
                break
            await _dispose_failed_database_pool(remaining_seconds)
            retry_delay = min(
                2.0 * (2**attempt),
                MAX_FAILURE_STATUS_RETRY_DELAY_SECONDS,
                max(deadline - loop.time(), 0.0),
            )
            if retry_delay <= 0:
                break
            await asyncio.sleep(retry_delay)
    if last_error is None:
        return "unknown failure while recording failed import status"
    return f"{type(last_error).__name__}: {last_error}"


async def _projection_row_batches(
    run_id: str,
    loaded_categories: set[str],
    published_at: datetime,
    *,
    npi_batch_size: int = 500,
) -> AsyncIterator[list[dict[str, Any]]]:
    """Build bounded projection batches from facts already persisted for this run."""
    last_npi = 0
    while True:
        npi_rows = await db.all(
            select(ProviderProfileFact.npi)
            .where(
                ProviderProfileFact.run_id == run_id,
                ProviderProfileFact.npi.is_not(None),
                ProviderProfileFact.npi > last_npi,
            )
            .distinct().order_by(ProviderProfileFact.npi)
            .limit(npi_batch_size)
        )
        npis = [int(source_row._mapping["npi"]) for source_row in npi_rows]
        if not npis:
            return
        fact_rows = await db.all(
            select(ProviderProfileFact.__table__)
            .where(
                ProviderProfileFact.run_id == run_id,
                ProviderProfileFact.npi.in_(npis),
            )
            .order_by(
                ProviderProfileFact.npi,
                ProviderProfileFact.category,
                ProviderProfileFact.logical_fact_key,
            )
        )
        facts_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for fact_row in fact_rows:
            fact_by_key = dict(fact_row._mapping)
            facts_by_npi[int(fact_by_key["npi"])].append(fact_by_key)
        projection_rows: list[dict[str, Any]] = []
        for npi in npis:
            profile, evidence = _projection(
                npi,
                run_id,
                facts_by_npi.get(npi, []),
                loaded_categories,
            )
            projection_rows.append(
                {
                    "npi": npi,
                    "generation_id": run_id,
                    "schema_version": PROFILE_SCHEMA_VERSION,
                    "profile_json": profile,
                    "evidence_json": evidence,
                    "source_keys": [FL_MQA_SOURCE_KEY],
                    "published_at": published_at,
                }
            )
        yield projection_rows
        last_npi = npis[-1]


async def _publish_projection_swap(
    run_id: str,
    row_batches: AsyncIterator[list[dict[str, Any]]],
    *,
    started_at: datetime,
    completion_metrics: Mapping[str, Any],
    allow_volume_drop: bool,
    min_first_publish_providers: int,
    min_publish_ratio: float,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build and validate a complete generation, then atomically rotate it live."""
    schema = ProviderProfileProjection.__table__.schema or "mrf"
    live_name = ProviderProfileProjection.__tablename__
    stage_name = f"{live_name}_{run_id[:16]}"
    old_name = f"{live_name}_old"
    if not re.fullmatch(r"[a-z0-9_]+", stage_name):
        raise RuntimeError("provider_profile_stage_name_invalid")

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_name};")
    await db.status(
        f"CREATE TABLE {schema}.{stage_name} "
        f"(LIKE {schema}.{live_name} INCLUDING ALL EXCLUDING DEFAULTS);"
    )
    stage_table = ProviderProfileProjection.__table__.to_metadata(
        MetaData(),
        schema=schema,
        name=stage_name,
    )
    inserted_count = 0
    async for row_batch in row_batches:
        if not row_batch:
            continue
        await db.insert(stage_table).values(row_batch).status()
        inserted_count += len(row_batch)
    if inserted_count == 0:
        raise RuntimeError("provider_profile_stage_empty")

    stage_count = int(
        await db.scalar(text(f"SELECT count(*) FROM {schema}.{stage_name}")) or 0
    )
    distinct_npi_count = int(
        await db.scalar(
            text(f"SELECT count(DISTINCT npi) FROM {schema}.{stage_name}")
        )
        or 0
    )
    if stage_count != inserted_count or distinct_npi_count != stage_count:
        raise RuntimeError(
            "provider_profile_stage_validation_failed:"
            f"expected={inserted_count}:rows={stage_count}:"
            f"distinct_npis={distinct_npi_count}"
        )
    publication_by_key = {
        "stage_table": f"{schema}.{stage_name}",
        "published_table": f"{schema}.{live_name}",
        "rollback_table": f"{schema}.{old_name}",
        "published_rows": stage_count,
        "publication": "atomic_table_swap",
    }
    final_metrics_by_key: dict[str, Any] = {}

    async with db.transaction():
        await db.scalar(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
            lock_name=f"{schema}.{live_name}.publication",
        )
        current_generations = await db.all(
            text(
                f"""
                SELECT generation_id, count(*) AS provider_count
                  FROM {schema}.{live_name} AS p
                 GROUP BY generation_id
                """
            )
        )
        if len(current_generations) > 1:
            raise RuntimeError("provider_profile_live_generation_mixed")
        current_generation_id = ""
        current_provider_count = 0
        previous_source_record_count: int | None = None
        previous_metrics: Mapping[str, Any] | None = None
        if current_generations:
            current_generation_id = str(
                current_generations[0]._mapping["generation_id"] or ""
            )
            current_provider_count = int(
                current_generations[0]._mapping["provider_count"] or 0
            )
            current_run = await db.first(
                select(
                    ProviderProfileImportRun.started_at,
                    ProviderProfileImportRun.metrics,
                ).where(ProviderProfileImportRun.run_id == current_generation_id)
            )
            current_started_at = (
                current_run._mapping["started_at"]
                if current_run is not None
                else None
            )
            previous_metrics = (
                current_run._mapping["metrics"]
                if current_run is not None
                else None
            )
            if isinstance(previous_metrics, Mapping):
                previous_source_records = previous_metrics.get(
                    "physical_source_records"
                )
                if not isinstance(previous_source_records, int):
                    previous_source_records = previous_metrics.get("source_records")
                if isinstance(previous_source_records, int):
                    previous_source_record_count = previous_source_records
            if _is_generation_newer(
                current_started_at,
                current_generation_id,
                started_at,
                run_id,
            ):
                raise RuntimeError(
                    "provider_profile_newer_generation_already_published"
                )
        candidate_source_metrics_by_key = completion_metrics.get("source_metrics")
        if not isinstance(candidate_source_metrics_by_key, Mapping):
            candidate_source_metrics_by_key = {}
        previous_source_metrics_by_key: Mapping[str, Any] = {}
        if isinstance(previous_metrics, Mapping):
            prior = previous_metrics.get("source_metrics")
            if isinstance(prior, Mapping):
                previous_source_metrics_by_key = prior
        source_validation_reasons = _source_validation_guard_reasons(
            candidate_source_metrics_by_key,
            expected_source_keys=completion_metrics.get(
                "selected_sources",
                (),
            ),
        )
        source_ratio_reasons = _source_ratio_guard_reasons(
            candidate_source_metrics_by_key,
            previous_source_metrics_by_key,
            min_publish_ratio=min_publish_ratio,
        )
        source_header_reasons = _source_header_drift_guard_reasons(
            candidate_source_metrics_by_key,
            previous_source_metrics_by_key,
        )
        publication_by_key["source_guard"] = {
            "allow_volume_drop": allow_volume_drop,
            "min_publish_ratio": min_publish_ratio,
            "validation_reasons": source_validation_reasons,
            "header_reasons": source_header_reasons,
            "ratio_reasons": source_ratio_reasons,
        }
        if source_validation_reasons or source_header_reasons:
            raise RuntimeError(
                "provider_profile_source_validation_guard:"
                + ",".join(
                    [*source_validation_reasons, *source_header_reasons]
                )
            )
        if source_ratio_reasons and not allow_volume_drop:
            raise RuntimeError(
                "provider_profile_source_volume_guard:"
                + ",".join(source_ratio_reasons)
            )
        candidate_physical_source_records = int(
            completion_metrics.get("physical_source_records")
            or completion_metrics.get("source_records")
            or 0
        )
        guard_reasons = _publication_guard_reasons(
            candidate_provider_count=stage_count,
            candidate_source_record_count=candidate_physical_source_records,
            current_provider_count=current_provider_count,
            previous_source_record_count=previous_source_record_count,
            min_first_publish_providers=min_first_publish_providers,
            min_publish_ratio=min_publish_ratio,
        )
        publication_by_key["volume_guard"] = {
            "allow_volume_drop": allow_volume_drop,
            "candidate_providers": stage_count,
            "current_providers": current_provider_count,
            "source_record_counter_semantics": "physical_input",
            "candidate_source_records": candidate_physical_source_records,
            "previous_source_records": previous_source_record_count,
            "min_first_publish_providers": min_first_publish_providers,
            "min_publish_ratio": min_publish_ratio,
            "reasons": guard_reasons,
        }
        if guard_reasons and not allow_volume_drop:
            raise RuntimeError(
                "provider_profile_publication_volume_guard:"
                + ",".join(guard_reasons)
            )
        final_metrics_by_key = {
            **completion_metrics,
            "published_providers": stage_count,
            "publication": publication_by_key,
        }
        projection_tables = await db.all(
            text(
                """
                SELECT tablename
                  FROM pg_catalog.pg_tables
                 WHERE schemaname = :schema
                   AND (
                        tablename = :live_name
                        OR tablename = :old_name
                        OR tablename LIKE :stage_pattern
                   )
                """
            ),
            schema=schema,
            live_name=live_name,
            old_name=old_name,
            stage_pattern=f"{live_name}_%",
        )
        allowed_name = re.compile(
            rf"{re.escape(live_name)}(?:_old|_[a-f0-9]{{16}})?"
        )
        for projection_table in projection_tables:
            table_name = str(projection_table._mapping["tablename"])
            if not allowed_name.fullmatch(table_name):
                continue
            # Older local/prototype tables used BIGSERIAL for the externally
            # supplied NPI key. A copied sequence default makes DROP old fail
            # after a LIKE-based swap, so remove it from every generation.
            await db.status(
                f"ALTER TABLE {schema}.{table_name} "
                "ALTER COLUMN npi DROP DEFAULT;"
            )
        await db.status(f"DROP TABLE IF EXISTS {schema}.{old_name};")
        await db.status(
            f"ALTER TABLE IF EXISTS {schema}.{live_name} RENAME TO {old_name};"
        )
        await db.status(
            f"ALTER TABLE {schema}.{stage_name} RENAME TO {live_name};"
        )
        await (
            db.update(ProviderProfileImportRun.__table__)
            .where(ProviderProfileImportRun.run_id == run_id)
            .values(
                status="completed",
                metrics=final_metrics_by_key,
                error=None,
                finished_at=_utcnow(),
            )
            .status()
        )
    return publication_by_key, final_metrics_by_key


def _source_validation_guard_reasons(
    source_metrics: Mapping[str, Any],
    *,
    expected_source_keys: Iterable[str] | None = None,
) -> list[str]:
    """Reject missing, empty, or schema-incomplete source acquisitions."""
    reasons: list[str] = []
    source_keys = set(source_metrics)
    if expected_source_keys is not None:
        source_keys.update(expected_source_keys)
    for source_key in sorted(source_keys):
        if source_key not in source_metrics:
            reasons.append(f"source_metrics_missing:{source_key}")
            continue
        metric = source_metrics[source_key]
        if not isinstance(metric, Mapping):
            reasons.append(f"source_metrics_invalid:{source_key}")
            continue
        if metric.get("schema_complete") is not True:
            reasons.append(f"source_schema_incomplete:{source_key}")
        source_rows = metric.get("rows")
        if not isinstance(source_rows, int) or source_rows <= 0:
            reasons.append(f"source_rows_empty:{source_key}")
        quarantined_rows = metric.get("quarantined_rows", 0)
        if (
            not isinstance(quarantined_rows, int)
            or quarantined_rows < 0
        ):
            reasons.append(
                f"source_quarantine_metric_invalid:{source_key}"
            )
        else:
            max_quarantined_rows = metric.get(
                "max_quarantined_rows",
                DEFAULT_MAX_QUARANTINED_ROWS_PER_SOURCE,
            )
            max_quarantined_ratio = metric.get(
                "max_quarantined_ratio",
                DEFAULT_MAX_QUARANTINED_ROW_RATIO,
            )
            if (
                not isinstance(max_quarantined_rows, int)
                or max_quarantined_rows < 0
            ):
                reasons.append(
                    f"source_quarantine_count_limit_invalid:{source_key}"
                )
            elif quarantined_rows > max_quarantined_rows:
                reasons.append(
                    f"source_quarantine_count_exceeded:{source_key}:"
                    f"{quarantined_rows}>{max_quarantined_rows}"
                )
            if (
                not isinstance(max_quarantined_ratio, (int, float))
                or isinstance(max_quarantined_ratio, bool)
                or not 0 <= max_quarantined_ratio <= 1
            ):
                reasons.append(
                    f"source_quarantine_ratio_limit_invalid:{source_key}"
                )
            elif (
                isinstance(source_rows, int)
                and source_rows > 0
                and quarantined_rows / source_rows > max_quarantined_ratio
            ):
                reasons.append(
                    f"source_quarantine_ratio_exceeded:{source_key}:"
                    f"{quarantined_rows}/{source_rows}>{max_quarantined_ratio:g}"
                )
        header_sha256 = metric.get("header_sha256")
        if not isinstance(header_sha256, str) or not re.fullmatch(
            r"[a-f0-9]{64}",
            header_sha256,
        ):
            reasons.append(f"source_header_hash_missing:{source_key}")
    return reasons


def _is_source_quarantine_within_threshold(
    metric: Mapping[str, Any],
) -> bool:
    rows = metric.get("rows")
    quarantined_rows = metric.get("quarantined_rows")
    max_quarantined_rows = metric.get("max_quarantined_rows")
    max_quarantined_ratio = metric.get("max_quarantined_ratio")
    return bool(
        isinstance(rows, int)
        and rows > 0
        and isinstance(quarantined_rows, int)
        and quarantined_rows >= 0
        and isinstance(max_quarantined_rows, int)
        and max_quarantined_rows >= 0
        and isinstance(max_quarantined_ratio, (int, float))
        and not isinstance(max_quarantined_ratio, bool)
        and 0 <= max_quarantined_ratio <= 1
        and quarantined_rows <= max_quarantined_rows
        and quarantined_rows / rows <= max_quarantined_ratio
    )


def _source_ratio_guard_reasons(
    candidate_source_metrics: Mapping[str, Any],
    previous_source_metrics: Mapping[str, Any],
    *,
    min_publish_ratio: float,
) -> list[str]:
    """Compare each source independently so one large file cannot hide a drop."""
    reasons: list[str] = []
    for source_key in sorted(candidate_source_metrics):
        candidate = candidate_source_metrics[source_key]
        previous = previous_source_metrics.get(source_key)
        if not isinstance(candidate, Mapping) or not isinstance(previous, Mapping):
            continue
        for metric_name in ("rows", "matched", "facts"):
            candidate_count = candidate.get(metric_name)
            previous_count = previous.get(metric_name)
            if (
                isinstance(candidate_count, int)
                and isinstance(previous_count, int)
                and previous_count > 0
                and candidate_count < previous_count * min_publish_ratio
            ):
                reasons.append(
                    f"source_{metric_name}_ratio:{source_key}:"
                    f"{candidate_count}/{previous_count}"
                )
    return reasons


def _source_header_drift_guard_reasons(
    candidate_source_metrics: Mapping[str, Any],
    previous_source_metrics: Mapping[str, Any],
) -> list[str]:
    """Fail closed on published-source header drift without storing raw headers."""
    reasons: list[str] = []
    for source_key in sorted(candidate_source_metrics):
        candidate = candidate_source_metrics[source_key]
        previous = previous_source_metrics.get(source_key)
        if not isinstance(candidate, Mapping) or not isinstance(previous, Mapping):
            continue
        candidate_hash = candidate.get("header_sha256")
        previous_hash = previous.get("header_sha256")
        if not previous_hash:
            continue
        if not isinstance(previous_hash, str) or not re.fullmatch(
            r"[a-f0-9]{64}",
            previous_hash,
        ):
            reasons.append(f"previous_source_header_hash_invalid:{source_key}")
            continue
        if candidate_hash != previous_hash:
            reasons.append(
                f"source_header_sha256_changed:{source_key}:"
                f"{previous_hash}->{candidate_hash}"
            )
    return reasons


def _publication_guard_reasons(
    *,
    candidate_provider_count: int,
    candidate_source_record_count: int,
    current_provider_count: int,
    previous_source_record_count: int | None,
    min_first_publish_providers: int,
    min_publish_ratio: float,
) -> list[str]:
    """Explain suspicious volume drops before a generation becomes visible."""
    reasons: list[str] = []
    if current_provider_count <= 0:
        if candidate_provider_count < min_first_publish_providers:
            reasons.append(
                "first_publish_provider_count:"
                f"{candidate_provider_count}<{min_first_publish_providers}"
            )
        return reasons
    if candidate_provider_count < current_provider_count * min_publish_ratio:
        reasons.append(
            "provider_count_ratio:"
            f"{candidate_provider_count}/{current_provider_count}"
        )
    if (
        previous_source_record_count
        and candidate_source_record_count
        < previous_source_record_count * min_publish_ratio
    ):
        reasons.append(
            "source_record_count_ratio:"
            f"{candidate_source_record_count}/{previous_source_record_count}"
        )
    return reasons


def _validated_loaded_categories(
    source_metrics: Mapping[str, Any],
) -> set[str]:
    """Only declare categories covered when their source passed validation."""
    categories: set[str] = set()
    for source_key, metric in source_metrics.items():
        if (
            source_key not in FLORIDA_SOURCES
            or not isinstance(metric, Mapping)
            or metric.get("validated") is not True
        ):
            continue
        categories.add(FLORIDA_SOURCES[source_key].category)
        if source_key == "profile_master":
            categories.update(_PROFILE_MASTER_CATEGORIES)
    return categories


def _retention_eligible_run_ids(
    run_rows: Iterable[Mapping[str, Any]],
    *,
    protected_run_ids: set[str],
    current_run_id: str,
    failed_cutoff: datetime,
) -> list[str]:
    """Select terminal heavy-data generations without touching active/audit runs."""
    eligible_items: list[str] = []
    protected_items = {*protected_run_ids, current_run_id}
    for row in run_rows:
        run_id = str(row.get("run_id") or "")
        if not _RUN_ID_RE.fullmatch(run_id) or run_id in protected_items:
            continue
        status = str(row.get("status") or "")
        if status == "completed":
            eligible_items.append(run_id)
            continue
        finished_at = row.get("finished_at")
        if (
            status == "failed"
            and isinstance(finished_at, datetime)
            and finished_at <= failed_cutoff
        ):
            eligible_items.append(run_id)
    return sorted(set(eligible_items))


def _remove_artifact_run_directories(
    artifact_root: Path,
    run_ids: Iterable[str],
) -> dict[str, Any]:
    """Remove only exact, non-symlink run directories below the configured root."""
    root = artifact_root.resolve()
    if root == Path(root.anchor):
        raise RuntimeError("provider_profile_artifact_root_too_broad")
    deleted_items: list[str] = []
    missing_items: list[str] = []
    errors_by_key: dict[str, str] = {}
    for run_id in sorted(set(run_ids)):
        if not _RUN_ID_RE.fullmatch(run_id):
            errors_by_key[run_id] = "invalid_run_id"
            continue
        candidate = root / run_id
        try:
            if not candidate.exists():
                missing_items.append(run_id)
                continue
            if candidate.is_symlink():
                raise RuntimeError("symlink_not_allowed")
            resolved = candidate.resolve()
            if resolved.parent != root or resolved.name != run_id:
                raise RuntimeError("artifact_path_outside_root")
            if not resolved.is_dir():
                raise RuntimeError("artifact_path_not_directory")
            shutil.rmtree(resolved)
            deleted_items.append(run_id)
        except Exception as exc:  # best effort; a later success retries cleanup
            errors_by_key[run_id] = f"{type(exc).__name__}: {exc}"
    return {
        "deleted": deleted_items,
        "missing": missing_items,
        "errors": errors_by_key,
    }


async def _delete_retained_payload_rows(
    run_ids: list[str],
) -> dict[str, int]:
    """Delete heavy rows and report pre-counts independent of driver status text."""
    deleted_rows_by_key: dict[str, int] = {}
    for model, metric_name in (
        (ProviderProfileFact, "facts"),
        (ProviderProfileSourceRecord, "source_records"),
        (ProviderProfileArtifact, "artifacts"),
    ):
        predicate = model.run_id.in_(run_ids)
        deleted_rows_by_key[metric_name] = int(
            await db.scalar(
                select(func.count())
                .select_from(model.__table__)
                .where(predicate)
            )
            or 0
        )
        await db.delete(model.__table__).where(predicate).status()
    return deleted_rows_by_key


async def _post_success_retention(
    *,
    run_id: str,
    artifact_root: Path,
    failed_retention_days: int,
) -> dict[str, Any]:
    """Bound heavy history after success while retaining live and rollback audits."""
    schema = ProviderProfileProjection.__table__.schema or "mrf"
    live_name = ProviderProfileProjection.__tablename__
    old_name = f"{live_name}_old"
    protected_run_ids: set[str] = set()
    eligible_run_ids: list[str] = []
    deleted_rows_by_key: dict[str, int] = {}
    failed_cutoff = _utcnow() - timedelta(days=failed_retention_days)

    async with db.transaction():
        await db.scalar(
            text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
            lock_name=f"{schema}.{live_name}.publication",
        )
        projection_tables = await db.all(
            text(
                """
                SELECT tablename
                  FROM pg_catalog.pg_tables
                 WHERE schemaname = :schema
                   AND tablename IN (:live_name, :old_name)
                """
            ),
            schema=schema,
            live_name=live_name,
            old_name=old_name,
        )
        for projection_table in projection_tables:
            table_name = str(projection_table._mapping["tablename"])
            if table_name not in {live_name, old_name}:
                continue
            generation_rows = await db.all(
                text(
                    f"SELECT DISTINCT generation_id "
                    f"FROM {schema}.{table_name}"
                )
            )
            protected_run_ids.update(
                str(source_row._mapping["generation_id"])
                for source_row in generation_rows
                if source_row._mapping["generation_id"]
            )
        terminal_rows = await db.all(
            select(
                ProviderProfileImportRun.run_id,
                ProviderProfileImportRun.status,
                ProviderProfileImportRun.finished_at,
            ).where(
                ProviderProfileImportRun.status.in_(("completed", "failed"))
            )
        )
        eligible_run_ids = _retention_eligible_run_ids(
            (source_row._mapping for source_row in terminal_rows),
            protected_run_ids=protected_run_ids,
            current_run_id=run_id,
            failed_cutoff=failed_cutoff,
        )
        if eligible_run_ids:
            deleted_rows_by_key = await _delete_retained_payload_rows(
                eligible_run_ids,
            )

    directory_result = await asyncio.to_thread(
        _remove_artifact_run_directories,
        artifact_root,
        eligible_run_ids,
    )
    return {
        "status": (
            "completed_with_directory_errors"
            if directory_result["errors"]
            else "completed"
        ),
        "failed_retention_days": failed_retention_days,
        "protected_audit_run_ids": sorted(protected_run_ids),
        "deleted_run_ids": eligible_run_ids,
        "deleted_rows": deleted_rows_by_key,
        "artifact_directories": directory_result,
    }


async def _apply_retention_maintenance(
    *,
    run_id: str,
    artifact_root: Path,
    failed_retention_days: int,
) -> dict[str, Any]:
    """Best-effort heavy-row cleanup usable before acquisition and on failure."""
    try:
        return await _post_success_retention(
            run_id=run_id,
            artifact_root=artifact_root,
            failed_retention_days=failed_retention_days,
        )
    except Exception as exc:
        return {
            "status": "failed",
            "failed_retention_days": failed_retention_days,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }


async def _apply_post_success_retention(
    *,
    run_id: str,
    metrics: Mapping[str, Any],
    artifact_root: Path,
    failed_retention_days: int,
) -> dict[str, Any]:
    """Keep a published run successful even if best-effort retention needs retry."""
    try:
        retention_by_key = await _post_success_retention(
            run_id=run_id,
            artifact_root=artifact_root,
            failed_retention_days=failed_retention_days,
        )
    except Exception as exc:
        retention_by_key = {
            "status": "failed",
            "failed_retention_days": failed_retention_days,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
            },
        }
    final_metrics_by_key = {**metrics, "retention": retention_by_key}
    try:
        await (
            db.update(ProviderProfileImportRun.__table__)
            .where(ProviderProfileImportRun.run_id == run_id)
            .values(metrics=final_metrics_by_key)
            .status()
        )
    except Exception as exc:
        retention_by_key["metrics_persist_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
    return final_metrics_by_key


def _ordered_source_keys(source_keys: Iterable[str]) -> tuple[str, ...]:
    """Deduplicate requested sources while loading the identity master first."""
    requested_items = tuple(dict.fromkeys(source_keys))
    return (
        "profile_master",
        *(key for key in requested_items if key != "profile_master"),
    )


def _partial_publish_reasons(
    selected_keys: Iterable[str],
    max_providers: int | None,
) -> list[str]:
    reasons: list[str] = []
    missing_default_sources = sorted(
        set(DEFAULT_SOURCE_KEYS) - set(selected_keys)
    )
    if missing_default_sources:
        reasons.append(
            f"missing_default_sources:{','.join(missing_default_sources)}"
        )
    if max_providers is not None:
        reasons.append(f"max_providers:{max_providers}")
    return reasons


def _is_generation_newer(
    current_started_at: datetime | None,
    current_generation_id: str,
    candidate_started_at: datetime,
    candidate_generation_id: str,
) -> bool:
    """Deterministically reject stale concurrent publishers, including ties."""
    return bool(
        current_started_at is not None
        and (
            current_started_at,
            current_generation_id,
        )
        > (
            candidate_started_at,
            candidate_generation_id,
        )
    )


def _projection(
    npi: int,
    generation_id: str,
    facts: Iterable[Mapping[str, Any]],
    loaded_categories: set[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Compose the stable, categorized state-provider projection for one NPI."""
    grouped: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    evidence_items: list[dict[str, Any]] = []
    source_keys: set[str] = set()
    for fact in facts:
        logical_key = str(fact["logical_fact_key"])
        category_facts = grouped[fact["category"]]
        profile_item_by_key = category_facts.get(logical_key)
        if profile_item_by_key is None:
            effective_period_by_key = {
                field_name: field_value
                for field_name, field_value in (
                    ("start", fact.get("effective_start")),
                    ("end", fact.get("effective_end")),
                )
                if field_value
            }
            profile_item_by_key = {
                "type": fact["fact_type"],
                "logical_fact_key": logical_key,
                "display": fact["display"],
                "value": fact["value_json"],
                "assertion_type": fact["assertion_type"],
                "verification_status": fact["verification_status"],
                "sensitive": bool(fact["sensitive"]),
                "public_default": bool(fact["public_default"]),
                "source_record_id": fact["source_record_id"],
                "source_record_ids": [fact["source_record_id"]],
                "source_kinds": ["state_regulator"],
                "assertions": [
                    {
                        "source_kind": "state_regulator",
                        "assertion_type": fact["assertion_type"],
                        "verification_status": fact["verification_status"],
                    }
                ],
                "assertion_count": 1,
            }
            if effective_period_by_key:
                profile_item_by_key["effective_period"] = effective_period_by_key
            category_facts[logical_key] = profile_item_by_key
        elif fact["source_record_id"] not in profile_item_by_key["source_record_ids"]:
            profile_item_by_key["source_record_ids"].append(fact["source_record_id"])
            profile_item_by_key["source_record_ids"].sort()
            profile_item_by_key["assertion_count"] = len(profile_item_by_key["source_record_ids"])
            if fact["fact_type"] == "provider_address":
                merged_location_types = sorted(
                    {
                        *profile_item_by_key["value"].get("location_types", []),
                        *fact["value_json"].get("location_types", []),
                    }
                )
                profile_item_by_key["value"]["location_types"] = merged_location_types
                profile_item_by_key["display"] = _address_display(profile_item_by_key["value"])
        source_keys.add(fact["source_json"]["source_key"])
        evidence_items.append(fact["source_json"])
    categories_by_key: dict[str, Any] = {}
    for category in STANDARD_CATEGORIES:
        profile_items = list(grouped.get(category, {}).values())
        categories_by_key[category] = {
            "availability": (
                "available"
                if profile_items
                else "not_reported"
                if category in loaded_categories
                else "unavailable"
            ),
            "items": profile_items,
        }
    profile_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": npi,
        "generation_id": generation_id,
        "categories": categories_by_key,
        "sources": [
            {
                "source_key": FL_MQA_SOURCE_KEY,
                "agency": FL_MQA_AGENCY,
                "jurisdiction": "FL",
                "source_kind": "state_regulator",
            }
        ],
        "important_context": [
            "Florida practitioner profile fields are generally self-reported unless an item says otherwise.",
            "An absent item does not prove that no event exists; publication horizons and reporting requirements vary.",
            "Administrative complaints are allegations and are not final disciplinary actions.",
        ],
    }
    evidence_json_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": npi,
        "generation_id": generation_id,
        "records": sorted(
            {json.dumps(profile_item_by_key, sort_keys=True) for profile_item_by_key in evidence_items}
        ),
    }
    evidence_json_by_key["records"] = [json.loads(profile_item_by_key) for profile_item_by_key in evidence_json_by_key["records"]]
    return profile_by_key, evidence_json_by_key


async def import_florida_mqa_profile(
    *,
    source_keys: Iterable[str] = DEFAULT_SOURCE_KEYS,
    max_providers: int | None = None,
    only_matched: bool = False,
    publish_partial: bool = False,
    allow_volume_drop: bool = False,
    artifact_root: Path | None = None,
    control_run_id: str | None = None,
    manage_db: bool = True,
) -> dict[str, Any]:
    """Import, validate, and atomically publish one Florida provider-profile generation."""
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    email = (
        os.getenv("HLTHPRT_FL_MQA_USERNAME")
        or os.getenv("HLTHPRT_FL_MQA_EMAIL")
        or ""
    ).strip()
    password = os.getenv("HLTHPRT_FL_MQA_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "HLTHPRT_FL_MQA_USERNAME and HLTHPRT_FL_MQA_PASSWORD are required"
        )
    selected_keys = _ordered_source_keys(source_keys)
    unknown = sorted(set(selected_keys) - FLORIDA_SOURCES.keys())
    if unknown:
        raise ValueError(f"unknown Florida MQA sources: {', '.join(unknown)}")
    partial_publish_reasons = _partial_publish_reasons(
        selected_keys,
        max_providers,
    )
    publish_enabled = not partial_publish_reasons or publish_partial
    min_first_publish_providers = int(
        os.getenv(
            "HLTHPRT_FL_MQA_MIN_FIRST_PUBLISH_PROVIDERS",
            str(DEFAULT_MIN_FIRST_PUBLISH_PROVIDERS),
        )
    )
    min_publish_ratio = float(
        os.getenv(
            "HLTHPRT_FL_MQA_MIN_PUBLISH_RATIO",
            str(DEFAULT_MIN_PUBLISH_RATIO),
        )
    )
    failed_retention_days = int(
        os.getenv(
            "HLTHPRT_FL_MQA_FAILED_RUN_RETENTION_DAYS",
            str(DEFAULT_FAILED_RUN_RETENTION_DAYS),
        )
    )
    max_quarantined_rows = int(
        os.getenv(
            "HLTHPRT_FL_MQA_MAX_QUARANTINED_ROWS_PER_SOURCE",
            str(DEFAULT_MAX_QUARANTINED_ROWS_PER_SOURCE),
        )
    )
    max_quarantined_ratio = float(
        os.getenv(
            "HLTHPRT_FL_MQA_MAX_QUARANTINED_ROW_RATIO",
            str(DEFAULT_MAX_QUARANTINED_ROW_RATIO),
        )
    )
    if min_first_publish_providers < 1:
        raise ValueError(
            "HLTHPRT_FL_MQA_MIN_FIRST_PUBLISH_PROVIDERS must be at least 1"
        )
    if not 0 < min_publish_ratio <= 1:
        raise ValueError("HLTHPRT_FL_MQA_MIN_PUBLISH_RATIO must be in (0, 1]")
    if failed_retention_days < 0:
        raise ValueError(
            "HLTHPRT_FL_MQA_FAILED_RUN_RETENTION_DAYS must be non-negative"
        )
    if max_quarantined_rows < 0:
        raise ValueError(
            "HLTHPRT_FL_MQA_MAX_QUARANTINED_ROWS_PER_SOURCE "
            "must be non-negative"
        )
    if not 0 <= max_quarantined_ratio <= 1:
        raise ValueError(
            "HLTHPRT_FL_MQA_MAX_QUARANTINED_ROW_RATIO must be in [0, 1]"
        )

    run_id = (
        hashlib.sha256(
            f"florida-mqa-control:{control_run_id}".encode()
        ).hexdigest()
        if control_run_id
        else uuid.uuid4().hex
    )
    started_at = _utcnow()
    root = artifact_root or Path(
        os.getenv(
            "HLTHPRT_FL_MQA_ARTIFACT_ROOT",
            "/data/healthporta/florida-mqa",
        )
    )
    run_root = root / run_id
    if manage_db:
        await db.connect()
    try:
        await _ensure_tables()
        startup_retention = await _apply_retention_maintenance(
            run_id=run_id,
            artifact_root=root,
            failed_retention_days=failed_retention_days,
        )
    except BaseException:
        if manage_db:
            await db.disconnect()
        raise
    run_row_by_key = {
        "run_id": run_id,
        "source_key": FL_MQA_SOURCE_KEY,
        "jurisdiction": "FL",
        "schema_version": PROFILE_SCHEMA_VERSION,
        "status": "running",
        "source_manifest": {
            "sources": list(selected_keys),
            "source_record_identity_version": SOURCE_RECORD_IDENTITY_VERSION,
            "control_run_id": control_run_id,
            "publish_partial": publish_partial,
            "allow_volume_drop": allow_volume_drop,
            "partial_publish_reasons": partial_publish_reasons,
            "publication_guard": {
                "min_first_publish_providers": min_first_publish_providers,
                "min_publish_ratio": min_publish_ratio,
            },
            "retention": {
                "published_audit_generations": ["live", "old"],
                "failed_run_days": failed_retention_days,
                "startup_maintenance_status": startup_retention.get("status"),
            },
        },
        "metrics": {},
        "error": None,
        "started_at": started_at,
        "finished_at": None,
    }
    try:
        await _claim_import_run(run_row_by_key)
    except BaseException:
        if manage_db:
            await db.disconnect()
        raise
    try:
        client = FloridaMQAClient(
            os.getenv("HLTHPRT_FL_MQA_BASE_URL", DEFAULT_BASE_URL),
            email,
            password,
        )
        await asyncio.to_thread(client.authenticate)
        artifacts_by_key: dict[str, dict[str, Any]] = {}
        enqueue_live_progress(
            phase="downloading",
            pct=5,
            message="Authenticated; downloading Florida MQA source artifacts",
            file_count=len(selected_keys),
        )
        downloaded_bytes = 0
        for source_index, source_key in enumerate(selected_keys, start=1):
            profile_source = FLORIDA_SOURCES[source_key]
            publication_target = run_root / profile_source.filename
            sha256, size = await asyncio.to_thread(client.download, profile_source, publication_target)
            downloaded_bytes += size
            artifact_by_key = {
                "artifact_id": hashlib.sha256(f"{run_id}:{source_key}:{sha256}".encode()).hexdigest(),
                "run_id": run_id,
                "source_key": source_key,
                "file_name": profile_source.filename,
                "source_url": urljoin(client.base_url, profile_source.url),
                "category": profile_source.category,
                "content_sha256": sha256,
                "content_bytes": size,
                "header": None,
                "downloaded_at": _utcnow(),
                "metadata_json": {"daily_refresh": True},
            }
            artifacts_by_key[source_key] = artifact_by_key
            enqueue_live_progress(
                phase="downloading",
                pct=5 + int(30 * source_index / len(selected_keys)),
                message=f"Downloaded {profile_source.title}",
                file_index=source_index,
                file_count=len(selected_keys),
                file_name=profile_source.filename,
                counters={
                    "artifacts": source_index,
                    "artifact_bytes": downloaded_bytes,
                },
            )
        await _upsert_rows(ProviderProfileArtifact, list(artifacts_by_key.values()), "artifact_id")

        enqueue_live_progress(
            phase="matching",
            pct=36,
            message="Loading Florida NPI license identity index",
        )
        license_index = await _load_florida_license_index()
        master_identities_by_key: dict[tuple[str, str], tuple[int | None, str, str]] = {}
        discovered_profession_details: dict[str, set[tuple[str, str]]] = defaultdict(set)
        source_records: list[dict[str, Any]] = []
        facts: list[dict[str, Any]] = []
        source_record_count = 0
        fact_count = 0
        matched_record_count = 0
        non_projectable_record_count = 0
        selected_count = 0
        source_metrics_by_key: dict[str, dict[str, Any]] = {}
        for source_index, source_key in enumerate(selected_keys, start=1):
            profile_source = FLORIDA_SOURCES[source_key]
            path = run_root / profile_source.filename
            header_seen = _artifact_header(path, profile_source)
            missing = sorted(set(profile_source.required_fields) - set(header_seen))
            source_metric_by_key = {
                "counter_semantics": "physical_input",
                "rows": 0,
                "matched": 0,
                "facts": 0,
                "non_projectable_records": 0,
                "trailing_empty_rows": 0,
                "trailing_empty_fields": 0,
                "recovered_rows": 0,
                "quarantined_rows": 0,
                "quarantine_ratio": 0.0,
                "max_quarantined_rows": max_quarantined_rows,
                "max_quarantined_ratio": max_quarantined_ratio,
                "quarantine_within_threshold": True,
                "header_sha256": _header_sha256(header_seen),
                "schema_complete": not missing,
                "missing_required_fields": missing,
                "validated": False,
            }
            source_metrics_by_key[source_key] = source_metric_by_key
            artifacts_by_key[source_key]["header"] = header_seen
            artifacts_by_key[source_key]["metadata_json"] = {
                **artifacts_by_key[source_key]["metadata_json"],
                "header_sha256": source_metric_by_key["header_sha256"],
            }
            if missing:
                run_row_by_key["metrics"] = {
                    "artifacts": len(artifacts_by_key),
                    "source_metrics": source_metrics_by_key,
                }
                await _upsert_rows(
                    ProviderProfileArtifact,
                    [artifacts_by_key[source_key]],
                    "artifact_id",
                )
                await _upsert_rows(
                    ProviderProfileImportRun,
                    [run_row_by_key],
                    "run_id",
                )
                raise RuntimeError(
                    f"florida_mqa_schema_changed:{profile_source.key}:{','.join(missing)}"
                )
            for row_number, raw_row, source_row, header in _iter_rows(
                path,
                profile_source,
                parser_metrics=source_metric_by_key,
            ):
                source_metric_by_key["rows"] += 1
                if header != header_seen:
                    source_metric_by_key["schema_complete"] = False
                    source_metric_by_key["missing_required_fields"] = [
                        "inconsistent_header"
                    ]
                    run_row_by_key["metrics"] = {
                        "artifacts": len(artifacts_by_key),
                        "counter_semantics": "physical_input",
                        "source_records": source_record_count,
                        "facts": fact_count,
                        "matched_records": matched_record_count,
                        "non_projectable_records": non_projectable_record_count,
                        "physical_source_records": source_record_count,
                        "physical_facts": fact_count,
                        "physical_matched_records": matched_record_count,
                        "physical_non_projectable_records": non_projectable_record_count,
                        "source_metrics": source_metrics_by_key,
                    }
                    await _upsert_rows(
                        ProviderProfileArtifact,
                        [artifacts_by_key[source_key]],
                        "artifact_id",
                    )
                    await _upsert_rows(
                        ProviderProfileImportRun,
                        [run_row_by_key],
                        "run_id",
                    )
                    raise RuntimeError(
                        f"florida_mqa_schema_changed:{profile_source.key}:inconsistent_header"
                    )
                if source_row.get("_source_parse_quarantine"):
                    source_record_key = _record_key(
                        profile_source,
                        source_row,
                        row_number,
                    )
                    record_id = hashlib.sha256(
                        f"{run_id}:{source_record_key}".encode()
                    ).hexdigest()
                    source_records.append(
                        _retained_source_record(
                            record_id=record_id,
                            run_id=run_id,
                            artifact_id=artifacts_by_key[source_key]["artifact_id"],
                            source_key=source_key,
                            source_record_key=source_record_key,
                            profession_code=None,
                            license_id=None,
                            license_number=None,
                            raw_payload=raw_row,
                            normalized_payload=source_row,
                            matched_npi=None,
                            match_status="quarantined_schema_anomaly",
                            match_evidence={
                                "method": "source_row_quarantine",
                                "parse_metadata": raw_row.get(
                                    "_source_parse_metadata",
                                    {},
                                ),
                            },
                            row_number=row_number,
                        )
                    )
                    source_record_count += 1
                    non_projectable_record_count += 1
                    source_metric_by_key["non_projectable_records"] += 1
                    if len(source_records) >= 1_000:
                        await _upsert_rows(
                            ProviderProfileSourceRecord,
                            source_records,
                            "record_id",
                        )
                        source_records.clear()
                    continue
                if source_key == "profile_master":
                    profession_name = _first(source_row, "rank_desc", "profession_name")
                    profession_detail = (
                        _first(source_row, "pro_cde", "profession_code"),
                        _first(source_row, "rank_cde", "rank_code"),
                    )
                    if profession_name and all(profession_detail):
                        discovered_profession_details[
                            _name_token(profession_name)
                        ].add(profession_detail)
                match_row = _canonical_match_row(
                    profile_source,
                    source_row,
                    discovered_profession_details,
                )
                profession = _first(match_row, "pro_cde", "profession_code")
                license_id = _first(match_row, "lic_id", "license_id")
                join_key = (profession, license_id)
                if source_key == "profile_master":
                    npi, match_status, match_evidence_by_key = _match_master(
                        match_row,
                        license_index,
                    )
                    if only_matched and npi is None:
                        continue
                    if max_providers is not None and selected_count >= max_providers:
                        break
                    selected_count += 1
                    master_identities_by_key[join_key] = (
                        npi,
                        match_status,
                        _first(match_row, "lic_nbr", "license_number"),
                    )
                elif source_key == "counties":
                    npi = None
                    match_status = "reference"
                    match_evidence_by_key = {
                        "method": "reference_dataset_no_npi",
                        "jurisdiction": "FL",
                    }
                elif profile_source.path == "/ProfileData":
                    identity = master_identities_by_key.get(join_key)
                    supplement_match = _profile_supplement_match(
                        identity,
                        profession_code=profession,
                        license_id=license_id,
                        only_matched=only_matched,
                    )
                    if supplement_match is None:
                        continue
                    npi, match_status, match_evidence_by_key = supplement_match
                else:
                    npi, match_status, match_evidence_by_key = _match_master(
                        match_row,
                        license_index,
                    )
                source_record_key = _record_key(profile_source, raw_row, row_number)
                record_id = hashlib.sha256(f"{run_id}:{source_record_key}".encode()).hexdigest()
                identity = master_identities_by_key.get(join_key)
                identity_license_number = identity[2] if identity else ""
                source_record = _retained_source_record(
                    record_id=record_id,
                    run_id=run_id,
                    artifact_id=artifacts_by_key[source_key]["artifact_id"],
                    source_key=source_key,
                    source_record_key=source_record_key,
                    profession_code=profession or None,
                    license_id=license_id or None,
                    license_number=_first(
                        match_row,
                        "lic_nbr",
                        "license_number",
                    )
                    or identity_license_number,
                    raw_payload=raw_row,
                    normalized_payload=source_row,
                    matched_npi=npi,
                    match_status=match_status,
                    match_evidence=match_evidence_by_key,
                    row_number=row_number,
                )
                source_records.append(source_record)
                projectable_npi = _projectable_fact_npi(npi, match_status)
                row_facts = (
                    _facts_for_row(
                        profile_source,
                        source_row,
                        run_id=run_id,
                        record_id=record_id,
                        npi=projectable_npi,
                        artifact=artifacts_by_key[source_key],
                    )
                    if projectable_npi is not None
                    else []
                )
                facts.extend(row_facts)
                source_record_count += 1
                fact_count += len(row_facts)
                matched_record_count += match_status == "deterministic"
                if projectable_npi is None:
                    non_projectable_record_count += 1
                    source_metric_by_key["non_projectable_records"] += 1
                source_metric_by_key["facts"] += len(row_facts)
                source_metric_by_key["matched"] += match_status == "deterministic"
                if len(source_records) >= 1_000:
                    await _upsert_rows(
                        ProviderProfileSourceRecord,
                        source_records,
                        "record_id",
                    )
                    source_records.clear()
                if len(facts) >= 5_000:
                    await _upsert_rows(ProviderProfileFact, facts, "fact_id")
                    facts.clear()
            source_metric_by_key["quarantine_ratio"] = (
                source_metric_by_key["quarantined_rows"]
                / source_metric_by_key["rows"]
                if source_metric_by_key["rows"]
                else 0.0
            )
            source_metric_by_key["quarantine_within_threshold"] = (
                _is_source_quarantine_within_threshold(source_metric_by_key)
            )
            source_metric_by_key["validated"] = bool(
                source_metric_by_key["schema_complete"]
                and source_metric_by_key["rows"] > 0
                and source_metric_by_key["quarantine_within_threshold"]
            )
            run_row_by_key["metrics"] = {
                "artifacts": len(artifacts_by_key),
                "counter_semantics": "physical_input",
                "source_records": source_record_count,
                "facts": fact_count,
                "matched_records": matched_record_count,
                "non_projectable_records": non_projectable_record_count,
                "physical_source_records": source_record_count,
                "physical_facts": fact_count,
                "physical_matched_records": matched_record_count,
                "physical_non_projectable_records": non_projectable_record_count,
                "source_metrics": source_metrics_by_key,
            }
            await _upsert_rows(
                ProviderProfileImportRun,
                [run_row_by_key],
                "run_id",
            )
            enqueue_live_progress(
                phase="normalizing",
                pct=36 + int(49 * source_index / len(selected_keys)),
                message=f"Normalized {profile_source.title}",
                file_index=source_index,
                file_count=len(selected_keys),
                file_name=profile_source.filename,
                counters={
                    "counter_semantics": "physical_input",
                    "source_records": source_record_count,
                    "facts": fact_count,
                    "matched_records": matched_record_count,
                    "non_projectable_records": non_projectable_record_count,
                    "physical_source_records": source_record_count,
                    "physical_facts": fact_count,
                    "physical_matched_records": matched_record_count,
                    "physical_non_projectable_records": non_projectable_record_count,
                },
            )

        await _upsert_rows(ProviderProfileSourceRecord, source_records, "record_id")
        await _upsert_rows(ProviderProfileFact, facts, "fact_id")
        source_records.clear()
        facts.clear()
        await _upsert_rows(ProviderProfileArtifact, list(artifacts_by_key.values()), "artifact_id")
        loaded_categories = _validated_loaded_categories(source_metrics_by_key)
        retained_counts_by_key = await _retained_import_counts(run_id)
        projected_provider_count = int(
            await db.scalar(
                select(func.count(func.distinct(ProviderProfileFact.npi))).where(
                    ProviderProfileFact.run_id == run_id,
                    ProviderProfileFact.npi.is_not(None),
                )
            )
            or 0
        )
        completion_metrics_by_key = {
            "artifacts": len(artifacts_by_key),
            "counter_semantics": {
                "source_records": "physical_input",
                "facts": "physical_input",
                "matched_records": "physical_input",
                "non_projectable_records": "physical_input",
                "physical_prefix": "physical_input_alias",
                "retained_prefix": "retained_unique",
                "source_metrics": "physical_input",
            },
            **retained_counts_by_key,
            "source_records": source_record_count,
            "facts": fact_count,
            "matched_records": matched_record_count,
            "non_projectable_records": non_projectable_record_count,
            "physical_source_records": source_record_count,
            "physical_facts": fact_count,
            "physical_matched_records": matched_record_count,
            "physical_non_projectable_records": non_projectable_record_count,
            "projected_providers": projected_provider_count,
            "selected_sources": list(selected_keys),
            "source_metrics": source_metrics_by_key,
        }
        run_row_by_key.update(
            status="validating",
            metrics=completion_metrics_by_key,
        )
        await _upsert_rows(ProviderProfileImportRun, [run_row_by_key], "run_id")
        enqueue_live_progress(
            phase="validating",
            pct=88,
            message="Validating complete provider profile generation",
            counters={
                **retained_counts_by_key,
                "source_records": source_record_count,
                "facts": fact_count,
                "matched_records": matched_record_count,
                "non_projectable_records": non_projectable_record_count,
                "physical_source_records": source_record_count,
                "physical_facts": fact_count,
                "physical_matched_records": matched_record_count,
                "physical_non_projectable_records": non_projectable_record_count,
                "projected_providers": projected_provider_count,
            },
        )
        if publish_enabled:
            source_validation_reasons = _source_validation_guard_reasons(
                source_metrics_by_key,
                expected_source_keys=selected_keys,
            )
            if source_validation_reasons:
                raise RuntimeError(
                    "provider_profile_source_validation_guard:"
                    + ",".join(source_validation_reasons)
                )
            enqueue_live_progress(
                phase="publishing",
                pct=94,
                message="Building and atomically publishing provider profile generation",
            )
            publication_by_key, metrics_by_key = await _publish_projection_swap(
                run_id,
                _projection_row_batches(
                    run_id,
                    loaded_categories,
                    _utcnow(),
                ),
                started_at=started_at,
                completion_metrics=completion_metrics_by_key,
                allow_volume_drop=allow_volume_drop,
                min_first_publish_providers=min_first_publish_providers,
                min_publish_ratio=min_publish_ratio,
            )
        else:
            publication_by_key = {
                "publication": "skipped_partial",
                "reasons": partial_publish_reasons,
                "published_rows": 0,
            }
            metrics_by_key = {
                **completion_metrics_by_key,
                "published_providers": 0,
                "publication": publication_by_key,
            }
            run_row_by_key.update(
                status="completed",
                metrics=metrics_by_key,
                finished_at=_utcnow(),
            )
            await _upsert_rows(ProviderProfileImportRun, [run_row_by_key], "run_id")
        metrics_by_key = await _apply_post_success_retention(
            run_id=run_id,
            metrics=metrics_by_key,
            artifact_root=root,
            failed_retention_days=failed_retention_days,
        )
        enqueue_live_progress(
            phase="completed",
            pct=100,
            message=(
                "Provider profile generation published"
                if publish_enabled
                else "Partial provider profile run completed without publication"
            ),
            counters={
                **retained_counts_by_key,
                "source_records": source_record_count,
                "facts": fact_count,
                "matched_records": matched_record_count,
                "non_projectable_records": non_projectable_record_count,
                "physical_source_records": source_record_count,
                "physical_facts": fact_count,
                "physical_matched_records": matched_record_count,
                "physical_non_projectable_records": non_projectable_record_count,
                "projected_providers": projected_provider_count,
                "published_providers": int(metrics_by_key["published_providers"]),
            },
        )
        return {
            "run_id": run_id,
            "control_run_id": control_run_id,
            **metrics_by_key,
        }
    except BaseException as exc:
        cleanup_error: str | None = None
        projection_schema = ProviderProfileProjection.__table__.schema or "mrf"
        projection_stage = (
            f"{ProviderProfileProjection.__tablename__}_{run_id[:16]}"
        )
        if re.fullmatch(r"[a-z0-9_]+", projection_stage):
            try:
                await db.status(
                    f"DROP TABLE IF EXISTS {projection_schema}.{projection_stage};"
                )
            except Exception as cleanup_exc:
                cleanup_error = f"{type(cleanup_exc).__name__}: {cleanup_exc}"
        failure_status_error = await _mark_failed_run_status(
            run_id=run_id,
            run_row=run_row_by_key,
            original_error=exc,
            cleanup_error=cleanup_error,
        )
        failure_retention = await _apply_retention_maintenance(
            run_id=run_id,
            artifact_root=root,
            failed_retention_days=failed_retention_days,
        )
        if failure_status_error:
            enqueue_live_progress(
                phase="failed",
                message="Provider profile import failed; status persistence also failed",
                error={
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "status_persistence_error": failure_status_error,
                    "retention_status": failure_retention.get("status"),
                },
            )
        raise
    finally:
        if manage_db:
            await db.disconnect()


async def process_data(
    *,
    sources: str | Iterable[str] | None = None,
    max_providers: int | None = None,
    only_matched: bool = False,
    publish_partial: bool = False,
    allow_volume_drop: bool = False,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Worker entry point using the shared database connection."""
    source_keys = (
        list(DEFAULT_SOURCE_KEYS)
        if sources is None
        else
        [value.strip() for value in sources.split(",") if value.strip()]
        if isinstance(sources, str)
        else [str(value).strip() for value in sources if str(value).strip()]
    )
    return await import_florida_mqa_profile(
        source_keys=source_keys,
        max_providers=max_providers,
        only_matched=only_matched,
        publish_partial=publish_partial,
        allow_volume_drop=allow_volume_drop,
        control_run_id=run_id,
        manage_db=False,
    )


@click.command(help="Import Florida MQA practitioner profile facts.")
@click.option(
    "--sources",
    default=",".join(DEFAULT_SOURCE_KEYS),
    show_default=True,
    help="Comma-separated Florida source keys.",
)
@click.option("--max-providers", type=int, default=None)
@click.option("--only-matched", is_flag=True, help="Retain only deterministically matched master rows.")
@click.option(
    "--publish-partial",
    is_flag=True,
    help="Explicitly allow a limited/subset run to replace the live projection.",
)
@click.option(
    "--allow-volume-drop",
    is_flag=True,
    help="Explicitly override the first-load/prior-generation volume safety gate.",
)
def florida_mqa_profile(
    sources: str,
    max_providers: int | None,
    only_matched: bool,
    publish_partial: bool,
    allow_volume_drop: bool,
) -> None:
    """Run a direct Florida MQA profile import."""
    result = asyncio.run(
        import_florida_mqa_profile(
            source_keys=[value.strip() for value in sources.split(",") if value.strip()],
            max_providers=max_providers,
            only_matched=only_matched,
            publish_partial=publish_partial,
            allow_volume_drop=allow_volume_drop,
        )
    )
    click.echo(json.dumps(result, sort_keys=True))
