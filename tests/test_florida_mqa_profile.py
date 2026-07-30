from datetime import UTC, datetime, timedelta
import importlib
import json
from unittest.mock import AsyncMock
import zipfile

import pytest

from api.provider_profile import (
    PROFILE_COMPOSER_VERSION,
    compose_provider_profile,
    compose_provider_profile_evidence,
)
from db.models.provider_profile import ProviderProfileProjection
from process.florida_mqa_profile import (
    DEFAULT_SOURCE_KEYS,
    FLORIDA_SOURCES,
    PROFILE_SCHEMA_VERSION,
    STANDARD_CATEGORIES,
    _PROFILE_MASTER_CATEGORIES,
    _artifact_header,
    _canonical_match_row,
    _clean_row,
    _delete_retained_payload_rows,
    _facts_for_row,
    _is_generation_newer,
    _header_sha256,
    _human_display,
    _iter_rows,
    _match_master,
    _ordered_source_keys,
    _partial_publish_reasons,
    _projection,
    _publication_guard_reasons,
    _record_key,
    _remove_artifact_run_directories,
    _retention_eligible_run_ids,
    _source_ratio_guard_reasons,
    _source_header_drift_guard_reasons,
    _source_validation_guard_reasons,
    _validated_loaded_categories,
)

florida_mqa_profile_module = importlib.import_module(
    "process.florida_mqa_profile"
)


def test_manifest_covers_profile_and_state_report_sources():
    assert len(FLORIDA_SOURCES) == 28
    assert FLORIDA_SOURCES["profile_master"].url.endswith(
        "fileName=licensee_profile.txt&handler=DownloadDataFile"
    )
    assert FLORIDA_SOURCES["license_status"].url == (
        "/LicenseStatus?handler=DownloadDataFile"
    )
    assert FLORIDA_SOURCES["administrative_complaints"].assertion_type == "allegation"
    assert FLORIDA_SOURCES["administrative_complaints"].public_default is False
    cannabis = FLORIDA_SOURCES["medical_cannabis_authorization"]
    assert cannabis.url == (
        "/AuthtoOrderMedicalandLowTHCCannabis?handler=DownloadDataFile"
    )
    assert cannabis.category == "prescribing_authorizations"
    assert cannabis.required_fields == (
        "frst_nme",
        "last_nme",
        "lic_nbr",
        "course_type",
        "dte_compl",
        "submitted_by",
        "pl_addr_line1",
        "pl_addr_line2",
        "pl_addr_line3",
        "pl_addr_cty",
        "pl_st_cde",
        "pl_zip",
        "pl_cnty",
        "phne_nbr",
        "specialties",
    )
    assert all(
        not profile_source.public_default
        for profile_source in FLORIDA_SOURCES.values()
        if profile_source.sensitive
    )


def test_authenticated_source_client_completes_portal_callback_without_exposing_credentials(
    monkeypatch,
):
    class Response:
        def __init__(self, body, url):
            self.body = body.encode()
            self.url = url

        def read(self):
            return self.body

        def geturl(self):
            return self.url

    client = florida_mqa_profile_module.FloridaMQAClient(
        "https://example.invalid",
        "test",
        "x",
    )
    responses = iter(
        (
            Response(
                'var SETTINGS = {"policy":"policy","transId":"transaction","csrf":"token"};',
                "https://example.invalid/policy/oauth2/v2.0/authorize?p=policy",
            ),
            Response('{"status":"200"}', "https://example.invalid/policy"),
            Response(
                '<form action="/callback"><input name="state" value="ok"></form>',
                "https://example.invalid/policy",
            ),
            Response("Sign out", "https://example.invalid/callback"),
        )
    )
    monkeypatch.setattr(client, "_open", lambda _request: next(responses))

    client.authenticate()


def test_zip_profile_artifact_streams_a_valid_source_row(tmp_path):
    source = FLORIDA_SOURCES["profile_master"]
    archive_path = tmp_path / "profile.zip"
    payload = _profile_master_artifact(source)
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("profile.txt", payload)

    rows = list(florida_mqa_profile_module._iter_rows(archive_path, source))

    assert len(rows) == 1
    assert rows[0][2]["lic_nbr"] == "ME12345"


@pytest.mark.asyncio
async def test_schema_bootstrap_and_license_index_use_the_profile_schema(monkeypatch):
    class BootstrapDb:
        def __init__(self):
            self.created = []
            self.statuses = []

        async def create_table(self, table, **kwargs):
            self.created.append((table.name, kwargs))

        async def status(self, statement):
            self.statuses.append(str(statement))

        async def all(self, _statement):
            return [
                type(
                    "Row",
                    (),
                    {"_mapping": {
                        "npi": 1000000004,
                        "provider_license_number": "ME-12345",
                        "healthcare_provider_taxonomy_code": "207Q00000X",
                        "provider_first_name": "Alex",
                        "provider_last_name": "Example",
                    }},
                )()
            ]

    database = BootstrapDb()
    monkeypatch.setattr(florida_mqa_profile_module, "db", database)

    await florida_mqa_profile_module._ensure_tables()
    index = await florida_mqa_profile_module._load_florida_license_index()

    assert len(database.created) == 5
    assert any("logical_fact_key" in statement for statement in database.statuses)
    assert index["ME12345"][0]["npi"] == 1000000004


def test_projection_npi_is_an_external_identifier_not_a_sequence():
    assert ProviderProfileProjection.__table__.c.npi.autoincrement is False


def test_header_normalization_and_human_display_are_readable():
    row = _clean_row(
        {
            "PRO_CDE": "1501",
            "LIC_ID": "42",
            "SCHOOL NAME": "Synthetic Medical College",
            "DEGREE": "MD",
        }
    )
    display = _human_display(FLORIDA_SOURCES["other_degrees"], row)
    assert display == "Other health-related degree: Synthetic Medical College — MD"


def test_matcher_publishes_only_one_compatible_exact_license():
    row_by_key = {
        "pro_cde": "1501",
        "lic_nbr": "12345",
        "first_name": "Alex",
        "last_name": "Example",
    }
    license_index = {
        "ME12345": [
            {
                "npi": 1000000004,
                "taxonomy": "207Q00000X",
                "first_name": "Alexander",
                "last_name": "Example",
                "license_number": "ME12345",
            }
        ]
    }
    npi, status, evidence = _match_master(row_by_key, license_index)
    assert npi == 1000000004
    assert status == "deterministic"
    assert evidence["candidate_count"] == 1


def test_matcher_rejects_ambiguous_and_name_conflicting_candidates():
    row_by_key = {
        "pro_cde": "1501",
        "lic_nbr": "ME12345",
        "first_name": "Alex",
        "last_name": "Example",
    }
    candidate_by_key = {
        "taxonomy": "207Q00000X",
        "first_name": "Alex",
        "last_name": "Example",
        "license_number": "ME12345",
    }
    ambiguous_by_key = {"ME12345": [{**candidate_by_key, "npi": 1000000004}, {**candidate_by_key, "npi": 1000000012}]}
    assert _match_master(row_by_key, ambiguous_by_key)[1] == "ambiguous"
    conflicting_by_key = {"ME12345": [{**candidate_by_key, "npi": 1000000004, "last_name": "Different"}]}
    assert _match_master(row_by_key, conflicting_by_key)[1] == "identity_conflict"


def test_profile_master_expands_biography_into_distinct_categories():
    profile_source = FLORIDA_SOURCES["profile_master"]
    source_row_by_key = {
        "pro_cde": "1501",
        "lic_id": "42",
        "lic_nbr": "ME12345",
        "f_name": "Alex",
        "m_name": "Q",
        "l_name": "Example",
        "birth_year_range": "1970-1975",
        "lic_sta_desc": "CLEAR/ACTIVE",
        "yr_began_practice": "2001",
        "orig_dte": "2002-01-01",
        "expr_dte": "2027-01-31",
        "addr_line1": "100 Example Ave",
        "addr_city": "Example City",
        "addr_state": "FL",
        "addr_zip": "32000",
        "ml_addr_line1": "PO Box 10",
        "ml_addr_city": "Example City",
        "ml_addr_state": "FL",
        "ml_addr_zip": "32001",
    }
    facts = _facts_for_row(
        profile_source,
        source_row_by_key,
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1000000004,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/profile",
        },
    )
    assert {fact["category"] for fact in facts} == {
        "identity",
        "demographics",
        "licenses",
        "professional_experience",
        "locations",
    }
    locations = [fact for fact in facts if fact["category"] == "locations"]
    assert {
        tuple(fact["value_json"]["location_types"]) for fact in locations
    } == {("mailing",), ("practice_primary",)}
    assert all(fact["value_json"] != source_row_by_key for fact in facts)


def _age_band_profile_master_facts():
    """Return representative profile-master facts with license dates present."""
    source_row_by_key = {
        "pro_cde": "1501",
        "lic_id": "42",
        "lic_nbr": "ME12345",
        "f_name": "Alex",
        "l_name": "Example",
        "birth_year_range": "80 - 90",
        "other_license": "N",
        "yr_began_practice": "1973",
        "nica_payment": "E",
        "orig_dte": "12/31/1973",
        "expr_dte": "01/31/2027",
        "addr_line1": "100 Example Ave",
        "addr_city": "Example City",
        "addr_state": "FL",
        "addr_zip": "32000",
    }
    return _facts_for_row(
        FLORIDA_SOURCES["profile_master"],
        source_row_by_key,
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1234567893,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/profile",
        },
    )


def test_profile_master_age_band_does_not_inherit_license_period():
    facts = _age_band_profile_master_facts()
    facts_by_type = {fact["fact_type"]: fact for fact in facts}

    assert facts_by_type["age_range"]["display"] == (
        "Reported age range: 80–90 years"
    )
    assert facts_by_type["age_range"]["value_json"] == {
        "minimum_years": 80,
        "maximum_years": 90,
        "precision": "range",
        "source_text": "80 - 90",
    }
    assert facts_by_type["state_license"]["effective_start"] == "1973-12-31"
    assert facts_by_type["state_license"]["effective_end"] == "2027-01-31"
    assert facts_by_type["practice_start"]["effective_start"] == "1973"
    assert facts_by_type["practice_start"]["effective_end"] is None
    non_license_types = {
        "name",
        "age_range",
        "other_state_license_indicator",
        "nica_assessment_status",
        "provider_address",
    }
    assert all(
        facts_by_type[fact_type]["effective_start"] is None
        and facts_by_type[fact_type]["effective_end"] is None
        for fact_type in non_license_types
    )


def test_profile_master_projection_omits_empty_effective_periods():
    facts = _age_band_profile_master_facts()
    profile, _evidence = _projection(
        1234567893,
        "synthetic-generation",
        facts,
        set(STANDARD_CATEGORIES),
    )
    age_item = profile["categories"]["demographics"]["items"][0]
    practice_item = profile["categories"]["professional_experience"]["items"][0]
    assert "effective_period" not in age_item
    assert practice_item["effective_period"] == {"start": "1973"}


def test_profile_master_skips_not_applicable_age_band():
    facts = _facts_for_row(
        FLORIDA_SOURCES["profile_master"],
        {
            "pro_cde": "1501",
            "lic_nbr": "ME12345",
            "birth_year_range": "N/A",
        },
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1234567893,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/profile",
        },
    )

    assert not [fact for fact in facts if fact["category"] == "demographics"]


def test_profile_master_does_not_publish_county_only_location():
    facts = _facts_for_row(
        FLORIDA_SOURCES["profile_master"],
        {
            "pro_cde": "1501",
            "lic_nbr": "ME12345",
            "cnty": "13",
        },
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1000000004,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/profile",
        },
    )

    assert not [fact for fact in facts if fact["category"] == "locations"]


def test_cannabis_course_semantics_distinguish_ordering_from_director_eligibility():
    profile_source = FLORIDA_SOURCES["medical_cannabis_authorization"]
    artifact_by_key = {
        "artifact_id": "synthetic-artifact",
        "content_sha256": "0" * 64,
        "source_url": "https://example.invalid/cannabis",
    }
    physician = _facts_for_row(
        profile_source,
        {
            "lic_nbr": "ME12345",
            "course_type": "Physician",
            "dte_compl": "2026-01-02",
            "pl_addr_line1": "100 Example Ave",
            "pl_addr_cty": "Example City",
            "pl_st_cde": "FL",
        },
        run_id="synthetic-run",
        record_id="physician",
        npi=1000000004,
        artifact=artifact_by_key,
    )[0]
    director = _facts_for_row(
        profile_source,
        {"lic_nbr": "ME12346", "course_type": "Director"},
        run_id="synthetic-run",
        record_id="director",
        npi=1000000012,
        artifact=artifact_by_key,
    )[0]
    assert physician["value_json"]["authorization_type"] == "medical_cannabis_ordering"
    assert physician["fact_type"] == "medical_cannabis_ordering_authorization"
    assert "Authorized to order" in physician["display"]
    assert director["value_json"]["authorization_type"] == (
        "dispensing_organization_medical_director_eligibility"
    )
    assert director["fact_type"] == (
        "dispensing_organization_medical_director_eligibility"
    )
    assert "Eligible to serve" in director["display"]


def test_cannabis_logical_key_preserves_distinct_course_assertions():
    source = FLORIDA_SOURCES["medical_cannabis_authorization"]
    artifact_by_key = {
        "artifact_id": "synthetic-artifact",
        "content_sha256": "0" * 64,
        "source_url": "https://example.invalid/cannabis",
    }
    facts = [
        _facts_for_row(
            source,
            {
                "lic_nbr": "ME12345",
                "course_type": "P",
                "dte_compl": completion_date,
            },
            run_id="synthetic-run",
            record_id=f"record-{completion_date}",
            npi=1000000004,
            artifact=artifact_by_key,
        )[0]
        for completion_date in ("2025-01-02", "2026-01-02")
    ]

    assert facts[0]["logical_fact_key"] != facts[1]["logical_fact_key"]


def test_cannabis_parser_preserves_pipe_delimited_specialty_tail(tmp_path):
    source = FLORIDA_SOURCES["medical_cannabis_authorization"]
    path = tmp_path / source.filename
    path.write_text(
        "FRST_NME|LAST_NME|LIC_NBR|COURSE_TYPE|DTE_COMPL|SUBMITTED_BY|"
        "PL_ADDR_LINE1|PL_ADDR_LINE2|PL_ADDR_LINE3|PL_ADDR_CTY|PL_ST_CDE|"
        "PL_ZIP|PL_CNTY|PHNE_NBR|SPECIALTIES\n"
        "Alex|Example|ME12345|P|2026-01-02|S|100 Example Ave|||Example City|"
        "FL|32000|Example|5550000000|Family Medicine|Sports Medicine\n",
        encoding="ascii",
    )
    rows = list(_iter_rows(path, source))
    assert len(rows) == 1
    _row_number, raw_row, normalized, _header = rows[0]
    assert raw_row["SPECIALTIES"] == "Family Medicine|Sports Medicine"
    assert normalized["specialties"] == "Family Medicine|Sports Medicine"


@pytest.mark.parametrize(
    ("source_key", "header"),
    (
        (
            "licensure_current",
            "pro_cde|Profession-Name|lic_id|Expire-Date|Original-Date|Rank-Code|"
            "License-Number|Status-Effective-Date|Board-Action-Indicator|"
            "License-Status-Description|Last-Name|First-Name|Middle-Name|"
            "Name-Suffix|Business-Name|License-Active-Status-Description|County|"
            "County-Description|Mailing-Address-Line1|Mailing-Address-line2|"
            "Mailing-Address-line3|Mailing-Address-City|Mailing-Address-State|"
            "Mailing-Address-ZIPcode|Mailing-Address-Area-Code|"
            "Mailing-Address-Phone-Number|Mailing-Address-Phone-Extension|"
            "Practice-Location-Address-Line1|Practice-Location-Address-line2|"
            "Practice-Location-Address-line3|Practice-Location-Address-City|"
            "Practice-Location-Address-State|Practice-Location-Address-ZIPcode|"
            "Email|Mod-Cdes|Prescribe-Ind|Dispensing-Ind|Birth-Year-Range|"
            "Other-License ",
        ),
        (
            "administrative_complaints",
            "Respondent Name|License Number|Profession|Addr Line 1|Addr Line 2|"
            "City|State|Zip|Case Number|Case Activity Type|Case Activity Date|",
        ),
        (
            "pain_management_report",
            "Clinic Name|PL Address|Lic Nbr|Lic Status|Year|Qtr|"
            "Reporting Phy Prof|Reporting Phy Lic Nbr|Reporting Phy Name|"
            "New Cnt|Repeat Cnt|Abuse Cnt|Divrsn Cnt|OOS Cnt|",
        ),
        (
            "pharmacy_pharmacist",
            "PHARM_KEY_NAME|PHARM_DBA_NAME|PHARM_LIC_NBR|PHARM_EXPR_DTE|"
            "PHARM_ORIG_DTE|PHARM_STAT_EFCTV_DTE|PHARM_LIC_STA_CDE|"
            "PHARM_LIC_STA_DESC|PHARM_PL_ADDR_L1|PHARM_PL_ADDR_L2|"
            "PHARM_PL_ADDR_L3|PHARM_PL_CTY|PHARM_PL_ST|PHARM_PL_ZIP|"
            "PHARM_PHNE_NBR|PHARM_PHNE_EXT|RLTN_PROF_NME|RLTN_KEY_NME|"
            "RLTN_LIC_NBR|RLTN_LIC_STA_CDE|RLTN_LIC_STA_DESC|"
            "RLTN_LIC_SEC_STA_CDE|RLTN_LIC_SEC_STA_DESC|RLTN_PL_ADDR_L1|"
            "RLTN_PL_ADDR_L2|RLTN_PL_ADDR_L3|RLTN_PL_CITY|RLTN_PL_STATE|"
            "RLTN_PL_ZIP|RLTN_PHONE_NBR|RLTN_PHONE_EXT|RLTN_EMAIL",
        ),
    ),
)
def test_endpoint_source_headers_match_published_metadata(
    tmp_path,
    source_key,
    header,
):
    source = FLORIDA_SOURCES[source_key]
    path = tmp_path / source.filename
    path.write_text(f"{header}\n", encoding="latin-1")

    assert tuple(_artifact_header(path, source)) == source.expected_fields


def test_licensure_history_uses_same_reviewed_contract_as_current():
    assert FLORIDA_SOURCES["licensure_all_statuses"].expected_fields == (
        FLORIDA_SOURCES["licensure_current"].expected_fields
    )


def test_headerless_license_status_keeps_first_record_and_validates_width(tmp_path):
    source = FLORIDA_SOURCES["license_status"]
    path = tmp_path / source.filename
    first = (
        "1501|ME|12345|Active|Clear|05/16/1979|02/28/2027|01/30/2025|"
        "ALEX||EXAMPLE|N|N|N|N"
    )
    second = (
        "1901|OS|54321|Active|Clear|05/16/1980|03/31/2028|01/31/2026|"
        "JAMIE||SAMPLE|N|N|N|N"
    )
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("lic_status.dat", f"{first}\n{second}\n")

    assert tuple(_artifact_header(path, source)) == source.expected_fields
    rows = list(_iter_rows(path, source))
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][2]["lic_nbr"] == "12345"
    assert rows[0][2]["f_name"] == "ALEX"

    bad_path = tmp_path / "bad-license-status.zip"
    with zipfile.ZipFile(bad_path, "w") as archive:
        archive.writestr("lic_status.dat", "1501|ME|12345\n")
    with pytest.raises(RuntimeError, match="florida_mqa_row_changed"):
        _artifact_header(bad_path, source)


@pytest.mark.parametrize(
    ("source_key", "source_row", "license_key", "taxonomy", "first_name", "last_name"),
    (
        (
            "license_status",
            {
                "pro_cde": "1501",
                "rank_cde": "ME",
                "lic_nbr": "12345",
                "f_name": "Alex",
                "l_name": "Example",
            },
            "ME12345",
            "207Q00000X",
            "Alex",
            "Example",
        ),
        (
            "licensure_current",
            {
                "pro_cde": "1501",
                "rank_code": "ME",
                "license_number": "12345",
                "first_name": "Alex",
                "last_name": "Example",
                "profession_name": "Medical Doctor",
            },
            "ME12345",
            "207Q00000X",
            "Alex",
            "Example",
        ),
        (
            "medical_cannabis_authorization",
            {
                "lic_nbr": "ME12345",
                "frst_nme": "Alex",
                "last_nme": "Example",
            },
            "ME12345",
            "207Q00000X",
            "Alex",
            "Example",
        ),
        (
            "administrative_complaints",
            {
                "respondent_name": "EXAMPLE, ALEX Q",
                "license_number": "2185932",
                "profession": "Registered Nurse",
            },
            "RN2185932",
            "163W00000X",
            "Alex",
            "Example",
        ),
        (
            "pain_management_report",
            {
                "lic_nbr": "1545",
                "reporting_phy_prof": "Medical Doctor",
                "reporting_phy_lic_nbr": "83615",
                "reporting_phy_name": "Mendez, Eduardo Sergio",
            },
            "ME83615",
            "207Q00000X",
            "Eduardo",
            "Mendez",
        ),
        (
            "pharmacy_pharmacist",
            {
                "pharm_lic_nbr": "36466",
                "rltn_prof_nme": "Pharmacist",
                "rltn_key_nme": "Veksler, Irwin Y",
                "rltn_lic_nbr": "51005",
            },
            "PS51005",
            "183500000X",
            "Irwin",
            "Veksler",
        ),
    ),
)
def test_source_identity_adapters_match_the_provider_side_license(
    source_key,
    source_row,
    license_key,
    taxonomy,
    first_name,
    last_name,
):
    profile_source = FLORIDA_SOURCES[source_key]
    canonical = _canonical_match_row(profile_source, source_row)
    npi, status, evidence = _match_master(
        canonical,
        {
            license_key: [
                {
                    "npi": 1000000004,
                    "taxonomy": taxonomy,
                    "first_name": first_name,
                    "last_name": last_name,
                    "license_number": license_key,
                }
            ]
        },
    )

    assert npi == 1000000004
    assert status == "deterministic"
    assert evidence["license_candidates"]
    if source_key == "pain_management_report":
        assert canonical["license_number"] == "83615"
    if source_key == "pharmacy_pharmacist":
        assert canonical["license_number"] == "51005"


@pytest.mark.parametrize("source_key", ("licensure_current", "licensure_all_statuses"))
def test_licensure_public_fact_excludes_unreviewed_identity_and_contact_fields(
    source_key,
):
    private_values_by_key = {
        "first_name": "Alex",
        "last_name": "Example",
        "business_name": "Private Practice Name",
        "mailing_address_line1": "1 Private Mailing Lane",
        "mailing_address_phone_number": "555-0100",
        "practice_location_address_line1": "2 Private Practice Lane",
        "email": "private@example.test",
        "birth_year_range": "40 - 50",
    }
    fact = _facts_for_row(
        FLORIDA_SOURCES[source_key],
        {
            "pro_cde": "1501",
            "profession_name": "Medical Doctor",
            "rank_code": "ME",
            "license_number": "12345",
            "license_status_description": "Clear",
            "license_active_status_description": "Active",
            "original_date": "01/01/2020",
            "expire_date": "01/31/2028",
            **private_values_by_key,
        },
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1000000004,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/licensure",
        },
    )[0]

    public_json = json.dumps(
        {"display": fact["display"], "value": fact["value_json"]},
        sort_keys=True,
    )
    assert fact["value_json"]["license_number"] == "12345"
    assert fact["value_json"]["status"] == "Clear"
    assert all(field_value not in public_json for field_value in private_values_by_key.values())


def test_pharmacy_fact_keeps_business_context_but_not_related_provider_contact():
    fact = _facts_for_row(
        FLORIDA_SOURCES["pharmacy_pharmacist"],
        {
            "pharm_key_name": "Synthetic Pharmacy",
            "pharm_lic_nbr": "36466",
            "pharm_lic_sta_desc": "Clear",
            "pharm_pl_addr_l1": "100 Pharmacy Way",
            "pharm_pl_cty": "Example City",
            "pharm_pl_st": "FL",
            "pharm_pl_zip": "32000",
            "pharm_phne_nbr": "555-0200",
            "rltn_prof_nme": "Pharmacist",
            "rltn_key_nme": "EXAMPLE, ALEX",
            "rltn_lic_nbr": "51005",
            "rltn_pl_addr_l1": "9 Private Provider Lane",
            "rltn_phone_nbr": "555-0300",
            "rltn_email": "provider-private@example.test",
        },
        run_id="synthetic-run",
        record_id="synthetic-record",
        npi=1000000004,
        artifact={
            "artifact_id": "synthetic-artifact",
            "content_sha256": "0" * 64,
            "source_url": "https://example.invalid/pharmacy",
        },
    )[0]

    public_json = json.dumps(fact["value_json"], sort_keys=True)
    assert fact["category"] == "pharmacy_relationships"
    assert fact["display"] == "Pharmacy relationship: Pharmacist — Synthetic Pharmacy"
    assert "Synthetic Pharmacy" in public_json
    assert "100 Pharmacy Way" in public_json
    assert "provider-private@example.test" not in public_json
    assert "9 Private Provider Lane" not in public_json
    assert "555-0300" not in public_json
    assert "EXAMPLE, ALEX" not in public_json


def test_complaint_and_pain_report_facts_use_reviewed_human_categories():
    artifact_by_key = {
        "artifact_id": "synthetic-artifact",
        "content_sha256": "0" * 64,
        "source_url": "https://example.invalid/state-source",
    }
    complaint = _facts_for_row(
        FLORIDA_SOURCES["administrative_complaints"],
        {
            "respondent_name": "EXAMPLE, ALEX",
            "license_number": "12345",
            "profession": "Medical Doctor",
            "addr_line_1": "9 Private Mailing Lane",
            "case_number": "CASE-1",
            "case_activity_type": "AC Filed",
            "case_activity_date": "07/10/2026",
        },
        run_id="synthetic-run",
        record_id="complaint",
        npi=1000000004,
        artifact=artifact_by_key,
    )[0]
    pain_report = _facts_for_row(
        FLORIDA_SOURCES["pain_management_report"],
        {
            "clinic_name": "Synthetic Clinic",
            "pl_address": "100 Clinic Way",
            "lic_nbr": "1545",
            "lic_status": "Clear",
            "year": "2026",
            "qtr": "2",
            "reporting_phy_prof": "Medical Doctor",
            "reporting_phy_lic_nbr": "83615",
            "reporting_phy_name": "EXAMPLE, ALEX",
            "new_cnt": "12",
            "repeat_cnt": "20",
        },
        run_id="synthetic-run",
        record_id="pain-report",
        npi=1000000004,
        artifact=artifact_by_key,
    )[0]

    complaint_json = json.dumps(complaint["value_json"], sort_keys=True)
    assert complaint["category"] == "complaints"
    assert "Administrative complaint (allegation): CASE-1 — AC Filed — 2026-07-10" == (
        complaint["display"]
    )
    assert "9 Private Mailing Lane" not in complaint_json
    assert "EXAMPLE, ALEX" not in complaint_json
    assert pain_report["category"] == "program_reports"
    assert pain_report["display"] == (
        "Pain management clinic report: Synthetic Clinic — 2026 Q2"
    )
    assert pain_report["value_json"]["reporting_provider"] == {
        "profession": "Medical Doctor",
        "license_number": "83615",
    }


def test_county_reference_rows_are_raw_only_and_have_an_exact_contract(tmp_path):
    source = FLORIDA_SOURCES["counties"]
    path = tmp_path / source.filename
    path.write_text("cnty|cnty_desc|\n13|Miami-Dade|\n", encoding="ascii")

    assert tuple(_artifact_header(path, source)) == ("cnty", "cnty_desc")
    row = list(_iter_rows(path, source))[0][2]
    assert row == {"cnty": "13", "cnty_desc": "Miami-Dade"}
    assert (
        _facts_for_row(
            source,
            row,
            run_id="synthetic-run",
            record_id="county-reference",
            npi=None,
            artifact={
                "artifact_id": "synthetic-artifact",
                "content_sha256": "0" * 64,
                "source_url": "https://example.invalid/counties",
            },
        )
        == []
    )


def test_empty_artifact_fails_header_validation(tmp_path):
    source = FLORIDA_SOURCES["education"]
    path = tmp_path / source.filename
    path.write_bytes(b"")

    with pytest.raises(RuntimeError, match="florida_mqa_header_missing"):
        _artifact_header(path, source)


def test_duplicate_rows_share_content_record_identity():
    profile_source = FLORIDA_SOURCES["education"]
    row_by_key = {
        "pro_cde": "1501",
        "lic_id": "42",
        "school_name": "Synthetic Medical College",
    }

    record_key = _record_key(profile_source, row_by_key, 2)
    assert record_key == _record_key(profile_source, row_by_key, 3)
    assert record_key.startswith("education:row-sha256:")
    assert record_key != _record_key(
        profile_source,
        {**row_by_key, "school_name": "Different Medical College"},
        3,
    )
    assert record_key != _record_key(
        profile_source,
        {**row_by_key, "school_name": "Synthetic Medical College "},
        3,
    )
    quarantined_row_by_key = {
        "_source_parse_quarantine": "field_count_mismatch",
        "_physical_field_count": "2",
    }
    assert _record_key(profile_source, quarantined_row_by_key, 2) != _record_key(
        profile_source,
        quarantined_row_by_key,
        3,
    )


def test_source_selection_is_deduplicated_and_master_first():
    selected = _ordered_source_keys(
        [
            "medical_cannabis_authorization",
            "profile_master",
            "education",
            "medical_cannabis_authorization",
        ]
    )

    assert selected == (
        "profile_master",
        "medical_cannabis_authorization",
        "education",
    )


def test_partial_runs_do_not_publish_without_explicit_override():
    assert _partial_publish_reasons(DEFAULT_SOURCE_KEYS, None) == []
    reasons = _partial_publish_reasons(["profile_master"], 10)
    assert reasons[0].startswith("missing_default_sources:")
    assert reasons[1] == "max_providers:10"


def test_publication_volume_guard_rejects_small_first_load_and_large_drops():
    assert _publication_guard_reasons(
        candidate_provider_count=99,
        candidate_source_record_count=500,
        current_provider_count=0,
        previous_source_record_count=None,
        min_first_publish_providers=100,
        min_publish_ratio=0.8,
    ) == ["first_publish_provider_count:99<100"]
    assert _publication_guard_reasons(
        candidate_provider_count=800,
        candidate_source_record_count=800,
        current_provider_count=1_000,
        previous_source_record_count=1_000,
        min_first_publish_providers=100,
        min_publish_ratio=0.8,
    ) == []
    assert _publication_guard_reasons(
        candidate_provider_count=799,
        candidate_source_record_count=799,
        current_provider_count=1_000,
        previous_source_record_count=1_000,
        min_first_publish_providers=100,
        min_publish_ratio=0.8,
    ) == [
        "provider_count_ratio:799/1000",
        "source_record_count_ratio:799/1000",
    ]


def test_source_guard_reports_exact_empty_schema_and_per_metric_drops():
    valid_hash = _header_sha256(["pro_cde", "lic_id"])
    candidate_by_key = {
        "education": {
            "rows": 79,
            "matched": 39,
            "facts": 78,
            "header_sha256": valid_hash,
            "schema_complete": True,
            "validated": True,
        },
        "publications": {
            "rows": 0,
            "matched": 0,
            "facts": 0,
            "header_sha256": "",
            "schema_complete": False,
            "validated": False,
        },
    }
    previous_by_key = {
        "education": {
            "rows": 100,
            "matched": 50,
            "facts": 100,
        }
    }

    assert _source_validation_guard_reasons(candidate_by_key) == [
        "source_schema_incomplete:publications",
        "source_rows_empty:publications",
        "source_header_hash_missing:publications",
    ]
    assert _source_ratio_guard_reasons(
        candidate_by_key,
        previous_by_key,
        min_publish_ratio=0.8,
    ) == [
        "source_rows_ratio:education:79/100",
        "source_matched_ratio:education:39/50",
        "source_facts_ratio:education:78/100",
    ]


def test_source_guard_requires_metrics_for_every_selected_source():
    assert _source_validation_guard_reasons(
        {},
        expected_source_keys=["profile_master"],
    ) == ["source_metrics_missing:profile_master"]


def test_source_header_drift_fails_closed_with_hashes_only():
    previous_hash = _header_sha256(["pro_cde", "lic_id"])
    candidate_hash = _header_sha256(["pro_cde", "lic_id", "new_field"])

    assert _source_header_drift_guard_reasons(
        {"education": {"header_sha256": candidate_hash}},
        {"education": {"header_sha256": previous_hash}},
    ) == [
        "source_header_sha256_changed:education:"
        f"{previous_hash}->{candidate_hash}"
    ]
    assert _source_header_drift_guard_reasons(
        {"education": {"header_sha256": candidate_hash}},
        {},
    ) == []


def test_loaded_categories_are_derived_only_from_validated_sources():
    categories = _validated_loaded_categories(
        {
            "profile_master": {"validated": True},
            "education": {"validated": True},
            "publications": {"validated": False},
        }
    )

    assert _PROFILE_MASTER_CATEGORIES <= categories
    assert "education" in categories
    assert "publications" not in categories


def test_retention_keeps_live_old_current_active_and_fresh_failures():
    now = datetime(2026, 7, 27, 12, 0)
    live = "1" * 32
    old = "2" * 32
    current = "3" * 32
    completed_stale = "4" * 32
    failed_fresh = "5" * 32
    failed_expired = "6" * 32
    running = "7" * 32

    eligible = _retention_eligible_run_ids(
        [
            {"run_id": live, "status": "completed", "finished_at": now},
            {"run_id": old, "status": "completed", "finished_at": now},
            {"run_id": current, "status": "completed", "finished_at": now},
            {
                "run_id": completed_stale,
                "status": "completed",
                "finished_at": now - timedelta(days=30),
            },
            {
                "run_id": failed_fresh,
                "status": "failed",
                "finished_at": now - timedelta(days=2),
            },
            {
                "run_id": failed_expired,
                "status": "failed",
                "finished_at": now - timedelta(days=8),
            },
            {
                "run_id": running,
                "status": "running",
                "finished_at": None,
            },
        ],
        protected_run_ids={live, old},
        current_run_id=current,
        failed_cutoff=now - timedelta(days=7),
    )

    assert eligible == [completed_stale, failed_expired]


def test_artifact_retention_removes_only_exact_run_directories(tmp_path):
    removable = "8" * 32
    untouched = "9" * 32
    removable_dir = tmp_path / removable
    removable_dir.mkdir()
    (removable_dir / "artifact.txt").write_text("synthetic")
    untouched_dir = tmp_path / untouched
    untouched_dir.mkdir()

    result = _remove_artifact_run_directories(
        tmp_path,
        [removable, "not-a-run-id"],
    )

    assert result["deleted"] == [removable]
    assert result["errors"] == {"not-a-run-id": "invalid_run_id"}
    assert not removable_dir.exists()
    assert untouched_dir.exists()


@pytest.mark.asyncio
async def test_retention_delete_counts_do_not_depend_on_driver_status(monkeypatch):
    class DeleteStatement:
        def where(self, _predicate):
            return self

        async def status(self):
            return "DELETE 999"

    class FakeDb:
        def __init__(self):
            self.counts = iter((11, 7, 3))
            self.deleted_tables = []

        async def scalar(self, _statement):
            return next(self.counts)

        def delete(self, table):
            self.deleted_tables.append(table.name)
            return DeleteStatement()

    fake_db = FakeDb()
    monkeypatch.setattr(florida_mqa_profile_module, "db", fake_db)

    deleted = await _delete_retained_payload_rows(["a" * 32])

    assert deleted == {
        "facts": 11,
        "source_records": 7,
        "artifacts": 3,
    }
    assert fake_db.deleted_tables == [
        "provider_profile_fact",
        "provider_profile_source_record",
        "provider_profile_artifact",
    ]


@pytest.mark.asyncio
async def test_retention_failure_does_not_reclassify_published_run(
    monkeypatch,
    tmp_path,
):
    class UpdateStatement:
        def __init__(self):
            self.persisted_values = None

        def where(self, _predicate):
            return self

        def values(self, **values):
            self.persisted_values = values
            return self

        async def status(self):
            return 1

    class FakeDb:
        def __init__(self):
            self.statement = UpdateStatement()

        def update(self, _table):
            return self.statement

    fake_db = FakeDb()
    monkeypatch.setattr(florida_mqa_profile_module, "db", fake_db)
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_post_success_retention",
        AsyncMock(side_effect=RuntimeError("synthetic cleanup failure")),
    )

    metrics = await florida_mqa_profile_module._apply_post_success_retention(
        run_id="b" * 32,
        metrics={"published_providers": 12},
        artifact_root=tmp_path,
        failed_retention_days=7,
    )

    assert metrics["published_providers"] == 12
    assert metrics["retention"]["status"] == "failed"
    assert fake_db.statement.persisted_values == {"metrics": metrics}
    assert "status" not in fake_db.statement.persisted_values


def test_generation_freshness_uses_started_at_then_generation_id():
    timestamp = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
    assert _is_generation_newer(timestamp, "b", timestamp, "a")
    assert not _is_generation_newer(timestamp, "a", timestamp, "b")
    assert _is_generation_newer(
        timestamp,
        "a",
        datetime(2026, 7, 27, 11, 59, tzinfo=UTC),
        "z",
    )


@pytest.mark.asyncio
async def test_control_adapter_maps_click_sources_and_keeps_worker_db_connected(
    monkeypatch,
):
    importer = AsyncMock(return_value={"published_providers": 12})
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "import_florida_mqa_profile",
        importer,
    )

    operation_result = await florida_mqa_profile_module.process_data(
        sources="profile_master,medical_cannabis_authorization",
        max_providers=25,
        only_matched=True,
        publish_partial=True,
        allow_volume_drop=False,
        run_id="run_control_123",
    )

    assert operation_result == {"published_providers": 12}
    importer.assert_awaited_once_with(
        source_keys=[
            "profile_master",
            "medical_cannabis_authorization",
        ],
        max_providers=25,
        only_matched=True,
        publish_partial=True,
        allow_volume_drop=False,
        control_run_id="run_control_123",
        manage_db=False,
    )


@pytest.mark.asyncio
async def test_control_adapter_defaults_to_a_complete_source_selection(monkeypatch):
    importer = AsyncMock(return_value={"published_providers": 12})
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "import_florida_mqa_profile",
        importer,
    )

    await florida_mqa_profile_module.process_data(run_id="complete-run")

    assert importer.await_args.kwargs["source_keys"] == list(DEFAULT_SOURCE_KEYS)
    assert importer.await_args.kwargs["control_run_id"] == "complete-run"
    assert importer.await_args.kwargs["manage_db"] is False


def _profile_master_artifact(source):
    row_by_key = {field: "" for field in source.expected_fields}
    row_by_key.update(
        {
            "pro_cde": "1501",
            "lic_id": "42",
            "lic_nbr": "ME12345",
            "rank_cde": "01",
            "rank_desc": "Physician",
            "f_name": "Alex",
            "l_name": "Example",
            "lic_sta_desc": "CLEAR/ACTIVE",
        }
    )
    return (
        "|".join(source.expected_fields)
        + "\n"
        + "|".join(row_by_key[field] for field in source.expected_fields)
        + "\n"
    )


class _ImportWorkflowClient:
    def __init__(self, *_args):
        self.base_url = "https://example.invalid"

    def authenticate(self):
        return None

    def download(self, source, target):
        payload = _profile_master_artifact(source)
        header, source_row = payload.strip().splitlines()
        payload = "\n".join((header, source_row, source_row)) + "\n"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(payload, encoding="utf-8")
        return "a" * 64, len(payload.encode())


def _configure_duplicate_import_runtime(monkeypatch):
    captured_upserts = []
    progress_events = []

    async def capture_upsert(model, rows, conflict_column):
        captured_upserts.append((model, list(rows), conflict_column))

    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "x")
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "FloridaMQAClient",
        _ImportWorkflowClient,
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_ensure_tables", AsyncMock())
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_apply_retention_maintenance",
        AsyncMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_claim_import_run", AsyncMock())
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_load_florida_license_index",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_upsert_rows", capture_upsert)
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_retained_import_counts",
        AsyncMock(
            return_value={
                "retained_source_records": 1,
                "retained_facts": 0,
                "retained_matched_records": 0,
                "retained_non_projectable_records": 1,
            }
        ),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_apply_post_success_retention",
        AsyncMock(side_effect=lambda **kwargs: kwargs["metrics"]),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "enqueue_live_progress",
        lambda **progress_by_key: progress_events.append(progress_by_key),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module.db,
        "scalar",
        AsyncMock(return_value=0),
    )
    connect = AsyncMock()
    disconnect = AsyncMock()
    monkeypatch.setattr(florida_mqa_profile_module.db, "connect", connect)
    monkeypatch.setattr(florida_mqa_profile_module.db, "disconnect", disconnect)
    return captured_upserts, progress_events, connect, disconnect


@pytest.mark.asyncio
async def test_partial_import_deduplicates_evidence_but_cannot_publish(
    monkeypatch,
    tmp_path,
):
    """Keep physical counters while duplicate evidence retains one identity."""
    upserts, progress_events, connect, disconnect = (
        _configure_duplicate_import_runtime(monkeypatch)
    )

    operation_result = await florida_mqa_profile_module.import_florida_mqa_profile(
        source_keys=["profile_master"],
        artifact_root=tmp_path,
        control_run_id="partial-run",
    )

    assert operation_result["publication"] == {
        "publication": "skipped_partial",
        "reasons": [
            "missing_default_sources:"
            + ",".join(sorted(set(DEFAULT_SOURCE_KEYS) - {"profile_master"}))
        ],
        "published_rows": 0,
    }
    assert operation_result["source_records"] == 2
    assert operation_result["retained_source_records"] == 1
    assert operation_result["physical_source_records"] == 2
    assert operation_result["counter_semantics"]["source_records"] == "physical_input"
    assert operation_result["counter_semantics"]["retained_prefix"] == (
        "retained_unique"
    )
    assert operation_result["source_metrics"]["profile_master"]["rows"] == 2
    assert (
        operation_result["source_metrics"]["profile_master"]["counter_semantics"]
        == "physical_input"
    )
    assert operation_result["published_providers"] == 0
    physical_source_rows = [
        source_row
        for model, source_rows, _key in upserts
        if model.__name__ == "ProviderProfileSourceRecord"
        for source_row in source_rows
    ]
    assert len(physical_source_rows) == 2
    assert len({source_row["record_id"] for source_row in physical_source_rows}) == 1
    counters_by_phase = {
        progress_event["phase"]: progress_event.get("counters", {})
        for progress_event in progress_events
    }
    for phase in ("validating", "completed"):
        assert counters_by_phase[phase]["source_records"] == 2
        assert counters_by_phase[phase]["physical_source_records"] == 2
        assert counters_by_phase[phase]["retained_source_records"] == 1
        assert counters_by_phase[phase]["retained_non_projectable_records"] == 1
    connect.assert_awaited_once()
    disconnect.assert_awaited_once()


def _complete_catalog_row(source):
    row_by_key = {field: "Synthetic" for field in source.expected_fields}
    row_by_key.update(
        {
            "pro_cde": "1501",
            "profession_code": "1501",
            "lic_id": "42",
            "license_id": "42",
            "lic_nbr": "ME12345",
            "license_number": "ME12345",
            "rank_cde": "01",
            "rank_code": "01",
            "f_name": "Alex",
            "first_name": "Alex",
            "frst_nme": "Alex",
            "l_name": "Example",
            "last_name": "Example",
            "last_nme": "Example",
            "lic_sta_desc": "CLEAR/ACTIVE",
            "license_status_description": "CLEAR/ACTIVE",
            "license_active_status_description": "ACTIVE",
            "orig_dte": "2020-01-01",
            "expr_dte": "2030-01-01",
            "original_date": "2020-01-01",
            "expire_date": "2030-01-01",
            "status_effective_date": "2020-01-01",
        }
    )
    return row_by_key


class _CompleteCatalogClient:
    def __init__(self, *_args):
        self.base_url = "https://example.invalid"

    def authenticate(self):
        return None

    def download(self, _source, target):
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("fixture", encoding="utf-8")
        return "b" * 64, 7


async def _capture_catalog_upsert(_model, _rows, _conflict_column):
    return None


def _iter_complete_catalog_rows(_path, source, *, parser_metrics=None):
    del parser_metrics
    row = _complete_catalog_row(source)
    yield 1, dict(row), row, list(source.expected_fields)


def _install_complete_catalog_input(monkeypatch) -> None:
    """Install the complete-catalog acquisition and matching fixture."""
    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "x")
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "FloridaMQAClient",
        _CompleteCatalogClient,
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_ensure_tables", AsyncMock())
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_apply_retention_maintenance",
        AsyncMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_claim_import_run", AsyncMock())
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_load_florida_license_index",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_artifact_header",
        lambda _path, source: list(source.expected_fields),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_iter_rows",
        _iter_complete_catalog_rows,
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_match_master",
        lambda *_args, **_kwargs: (1000000004, "deterministic", {"method": "fixture"}),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_upsert_rows",
        _capture_catalog_upsert,
    )


def _install_complete_catalog_publication(monkeypatch, published) -> None:
    """Install the retained-count and publication fixture."""
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_retained_import_counts",
        AsyncMock(
            return_value={
                "retained_source_records": len(DEFAULT_SOURCE_KEYS),
                "retained_facts": len(DEFAULT_SOURCE_KEYS),
                "retained_matched_records": len(DEFAULT_SOURCE_KEYS),
                "retained_non_projectable_records": 0,
            }
        ),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_publish_projection_swap",
        published,
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_apply_post_success_retention",
        AsyncMock(side_effect=lambda **kwargs: kwargs["metrics"]),
    )
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "enqueue_live_progress",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        florida_mqa_profile_module.db,
        "scalar",
        AsyncMock(return_value=27),
    )


@pytest.mark.asyncio
async def test_complete_catalog_import_requires_every_validated_source_before_publication(
    monkeypatch,
    tmp_path,
):
    """Verify complete catalog import requires every validated source before publication."""
    published = AsyncMock(
        return_value=(
            {"publication": "atomic_table_swap", "published_rows": 27},
            {
                "published_providers": 27,
                "publication": {
                    "publication": "atomic_table_swap",
                    "published_rows": 27,
                },
            },
        )
    )
    _install_complete_catalog_input(monkeypatch)
    _install_complete_catalog_publication(monkeypatch, published)

    operation_result = await florida_mqa_profile_module.import_florida_mqa_profile(
        source_keys=DEFAULT_SOURCE_KEYS,
        artifact_root=tmp_path,
        control_run_id="complete-run",
        manage_db=False,
    )

    assert operation_result["publication"]["publication"] == "atomic_table_swap"
    published.assert_awaited_once()
    completion_metrics = published.await_args.kwargs["completion_metrics"]
    assert completion_metrics["source_records"] == len(DEFAULT_SOURCE_KEYS)
    assert completion_metrics["physical_source_records"] == len(
        DEFAULT_SOURCE_KEYS
    )
    assert completion_metrics["selected_sources"] == list(DEFAULT_SOURCE_KEYS)


@pytest.mark.asyncio
async def test_import_failure_preserves_original_error_when_stage_cleanup_fails(monkeypatch, tmp_path):
    class FailingClient:
        def __init__(self, *_args):
            self.base_url = "https://example.invalid"

        def authenticate(self):
            raise RuntimeError("authentication failed")

    mark_failed = AsyncMock(return_value=None)
    monkeypatch.setenv("HLTHPRT_FL_MQA_USERNAME", "test")
    monkeypatch.setenv("HLTHPRT_FL_MQA_PASSWORD", "x")
    monkeypatch.setattr(florida_mqa_profile_module, "FloridaMQAClient", FailingClient)
    monkeypatch.setattr(florida_mqa_profile_module, "_ensure_tables", AsyncMock())
    monkeypatch.setattr(
        florida_mqa_profile_module,
        "_apply_retention_maintenance",
        AsyncMock(return_value={"status": "completed"}),
    )
    monkeypatch.setattr(florida_mqa_profile_module, "_claim_import_run", AsyncMock())
    monkeypatch.setattr(florida_mqa_profile_module, "_mark_failed_run_status", mark_failed)
    monkeypatch.setattr(florida_mqa_profile_module, "enqueue_live_progress", lambda **_kwargs: None)
    monkeypatch.setattr(
        florida_mqa_profile_module.db,
        "status",
        AsyncMock(side_effect=RuntimeError("cleanup unavailable")),
    )

    with pytest.raises(RuntimeError, match="authentication failed"):
        await florida_mqa_profile_module.import_florida_mqa_profile(
            source_keys=["profile_master"],
            artifact_root=tmp_path,
            manage_db=False,
        )

    assert mark_failed.await_args.kwargs["cleanup_error"] == (
        "RuntimeError: cleanup unavailable"
    )


class _WorkflowStatement:
    def __init__(self, calls):
        self.calls = calls

    def where(self, *_criteria):
        return self

    def values(self, *rows, **values):
        self.calls.append((rows, values))
        return self

    async def status(self):
        return 1


class _WorkflowTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _ProjectionPublicationDb:
    def __init__(self):
        self.status_calls = []
        self.write_calls = []
        self.scalar_results = iter((1, 1, None))
        self.all_results = iter(([], []))

    async def status(self, statement):
        self.status_calls.append(str(statement))
        return None

    async def scalar(self, *_args, **_kwargs):
        return next(self.scalar_results)

    async def all(self, *_args, **_kwargs):
        return next(self.all_results)

    def transaction(self):
        return _WorkflowTransaction()

    def insert(self, _table):
        return _WorkflowStatement(self.write_calls)

    def update(self, _table):
        return _WorkflowStatement(self.write_calls)


@pytest.mark.asyncio
async def test_projection_publication_builds_validated_stage_before_atomic_swap(monkeypatch):
    workflow_db = _ProjectionPublicationDb()
    monkeypatch.setattr(florida_mqa_profile_module, "db", workflow_db)

    async def row_batches():
        yield [
            {
                "npi": 1000000004,
                "generation_id": "a" * 32,
                "schema_version": PROFILE_SCHEMA_VERSION,
                "profile_json": {"categories": {}},
                "evidence_json": {"records": []},
                "source_keys": ["florida-mqa"],
                "published_at": datetime(2026, 7, 27, tzinfo=UTC),
            }
        ]

    source_metrics_by_key = {
        "profile_master": {
            "schema_complete": True,
            "rows": 1,
            "matched": 1,
            "facts": 1,
            "quarantined_rows": 0,
            "max_quarantined_rows": 100,
            "max_quarantined_ratio": 0.001,
            "header_sha256": "a" * 64,
        }
    }
    publication, metrics = await florida_mqa_profile_module._publish_projection_swap(
        "a" * 32,
        row_batches(),
        started_at=datetime(2026, 7, 27, tzinfo=UTC),
        completion_metrics={
            "source_records": 1,
            "selected_sources": ["profile_master"],
            "source_metrics": source_metrics_by_key,
        },
        allow_volume_drop=False,
        min_first_publish_providers=1,
        min_publish_ratio=0.8,
    )

    assert publication["publication"] == "atomic_table_swap"
    assert metrics["published_providers"] == 1
    assert any("CREATE TABLE mrf.provider_profile_projection_" in call for call in workflow_db.status_calls)
    assert any("RENAME TO provider_profile_projection_old" in call for call in workflow_db.status_calls)
    assert any("RENAME TO provider_profile_projection;" in call for call in workflow_db.status_calls)


def test_projection_merges_address_roles_across_source_records():
    profile_source = FLORIDA_SOURCES["profile_master"]
    artifact_by_key = {
        "artifact_id": "synthetic-artifact",
        "content_sha256": "0" * 64,
        "source_url": "https://example.invalid/profile",
    }
    shared_by_key = {
        "pro_cde": "1501",
        "lic_nbr": "ME12345",
    }
    practice_facts = _facts_for_row(
        profile_source,
        {
            **shared_by_key,
            "addr_line1": "100 Example Ave",
            "addr_city": "Example City",
            "addr_state": "FL",
            "addr_zip": "32000",
        },
        run_id="synthetic-run",
        record_id="practice-record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    mailing_facts = _facts_for_row(
        profile_source,
        {
            **shared_by_key,
            "ml_addr_line1": "100 Example Ave",
            "ml_addr_city": "Example City",
            "ml_addr_state": "FL",
            "ml_addr_zip": "32000",
        },
        run_id="synthetic-run",
        record_id="mailing-record",
        npi=1000000004,
        artifact=artifact_by_key,
    )

    profile, _evidence = _projection(
        1000000004,
        "synthetic-generation",
        [*practice_facts, *mailing_facts],
        {"locations", "licenses"},
    )
    locations = profile["categories"]["locations"]["items"]
    assert len(locations) == 1
    assert locations[0]["value"]["location_types"] == [
        "mailing",
        "practice_primary",
    ]


def test_composer_merges_fhir_and_state_facts_into_standard_categories():
    state_profile_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": 1000000004,
        "categories": {
            category: {"availability": "unavailable", "items": []}
            for category in STANDARD_CATEGORIES
        },
        "sources": [
            {
                "source_key": "synthetic-state",
                "source_kind": "state_regulator",
            }
        ],
    }
    state_profile_by_key["categories"]["licenses"] = {
        "availability": "available",
        "items": [
            {
                "type": "state_license_profile",
                "display": "State license: active",
                "value": {"status": "active"},
                "sensitive": False,
                "public_default": True,
            }
        ],
    }
    fhir_by_key = {
        "facts": {
            "language": {
                "items": [
                    {
                        "value": {"text": "Spanish"},
                        "source_ids": [
                            "synthetic-directory",
                            "synthetic-directory-copy",
                        ],
                        "source_count": 2,
                        "independent_source_count": 1,
                    }
                ]
            }
        },
        "sources": [{"source_id": "synthetic-directory", "org_name": "Example Directory"}],
    }
    operation_result = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_key},
        fhir_profile=fhir_by_key,
    )
    assert operation_result is not None
    assert operation_result["composer_version"] == PROFILE_COMPOSER_VERSION
    assert operation_result["categories"]["licenses"]["items"][0]["display"] == "State license: active"
    language = operation_result["categories"]["languages"]["items"][0]
    assert language["display"] == "Spanish (es)"
    assert language["assertion_type"] == "provider_directory_reported"
    assert language["assertion_count"] == 2
    assert len(language["assertions"]) == 1


@pytest.mark.parametrize(
    ("source_count", "source_ids", "expected_count"),
    (
        (3, ["directory-one"], 3),
        (1, ["directory-one", "directory-two"], 2),
        (None, None, 1),
    ),
)
def test_fhir_assertion_count_uses_strongest_support_signal(
    source_count,
    source_ids,
    expected_count,
):
    result = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile={
            "facts": {
                "language": {
                    "items": [
                        {
                            "value": {"text": "Spanish"},
                            "source_count": source_count,
                            "source_ids": source_ids,
                        }
                    ]
                }
            },
            "sources": [],
        },
        requested_categories=["languages"],
    )

    item = result["categories"]["languages"]["items"][0]
    assert item["assertion_count"] == expected_count
    assert len(item["assertions"]) == 1


def test_fhir_item_id_is_source_stable_but_provider_specific():
    def profile(npi, source_ids):
        result = compose_provider_profile(
            npi,
            state_projection=None,
            fhir_profile={
                "facts": {
                    "language": {
                        "items": [
                            {
                                "value": {"text": "Spanish"},
                                "source_ids": source_ids,
                                "source_count": len(source_ids),
                                "independent_source_count": 1,
                            }
                        ],
                        "total": 1,
                        "truncated": False,
                    }
                },
                "sources": [],
            },
            requested_categories=["languages"],
        )
        return result["categories"]["languages"]["items"][0]["item_id"]

    original = profile(1000000004, ["directory-one"])
    assert original == profile(
        1000000004,
        ["directory-one", "directory-two"],
    )
    assert original != profile(1000000012, ["directory-one"])


def test_item_id_survives_state_source_join_and_departure():
    field_value_by_key = {"text": "Alex Example", "family": "Example", "given": ["Alex"]}
    fhir_profile_by_key = {
        "facts": {
            "name": {
                "items": [{"value": field_value_by_key, "source_ids": ["directory-one"]}],
                "total": 1,
                "truncated": False,
            }
        },
        "sources": [],
    }
    fhir_only = compose_provider_profile(
        1000000004,
        state_projection=None,
        fhir_profile=fhir_profile_by_key,
        requested_categories=["identity"],
    )
    state_categories_by_key = {
        category: {"availability": "unavailable", "items": []}
        for category in STANDARD_CATEGORIES
    }
    state_categories_by_key["identity"] = {
        "availability": "available",
        "items": [
            {
                "type": "name",
                "logical_fact_key": "state-logical-key",
                "display": "Practitioner name: Alex Example",
                "value": field_value_by_key,
                "source_record_id": "state-record",
                "sensitive": False,
                "public_default": True,
            }
        ],
    }
    joined = compose_provider_profile(
        1000000004,
        state_projection={
            "profile": {
                "schema_version": PROFILE_SCHEMA_VERSION,
                "npi": 1000000004,
                "categories": state_categories_by_key,
                "sources": [],
            }
        },
        fhir_profile=fhir_profile_by_key,
        requested_categories=["identity"],
    )

    assert (
        fhir_only["categories"]["identity"]["items"][0]["item_id"]
        == joined["categories"]["identity"]["items"][0]["item_id"]
    )


def test_source_reported_total_only_describes_unmaterialized_fhir_facts():
    def category(total, truncated):
        result = compose_provider_profile(
            1000000004,
            state_projection=None,
            fhir_profile={
                "facts": {
                    "language": {
                        "items": [
                            {
                                "value": {"text": "Spanish"},
                                "source_ids": ["directory-one"],
                            }
                        ],
                        "total": total,
                        "truncated": truncated,
                    }
                },
                "sources": [],
            },
            requested_categories=["languages"],
        )
        return result["categories"]["languages"]

    assert "source_reported_total" not in category(1, False)
    assert category(3, False)["source_reported_total"] == 3
    assert category(1, True)["source_reported_total"] == 1


def _cross_source_state_profile_fixture(name_value_by_key):
    profile_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": 1000000004,
        "categories": {
            category: {"availability": "unavailable", "items": []}
            for category in STANDARD_CATEGORIES
        },
        "sources": [],
    }
    profile_by_key["categories"]["identity"] = {
        "availability": "available",
        "items": [
            {
                "type": "name",
                "display": "Practitioner name: Alex Example",
                "value": name_value_by_key,
                "source_record_id": "state-name-record",
                "source_record_ids": [
                    "state-name-record",
                    "state-name-record-copy",
                ],
                "assertion_count": 2,
                "sensitive": False,
                "public_default": True,
            }
        ],
    }
    return profile_by_key


def _cross_source_fhir_profile(name_value_by_key):
    return {
        "facts": {
            "name": {
                "items": [
                    {
                        "value": name_value_by_key,
                        "source_ids": [
                            "directory-source",
                            "directory-source-copy",
                        ],
                        "source_count": 1,
                        "independent_source_count": 1,
                    }
                ],
                "total": 1,
                "truncated": False,
            }
        },
        "sources": [],
    }


def test_composer_deduplicates_equal_cross_source_fact_and_keeps_both_evidence_paths():
    """Verify composer deduplicates equal cross source fact and keeps both evidence paths."""
    name_value_by_key = {
        "text": "Alex Example",
        "family": "Example",
        "given": ["Alex"],
    }
    state_profile_by_key = _cross_source_state_profile_fixture(name_value_by_key)
    fhir_profile_by_key = _cross_source_fhir_profile(name_value_by_key)
    profile = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_key},
        fhir_profile=fhir_profile_by_key,
        requested_categories=["identity"],
    )
    profile_items = profile["categories"]["identity"]["items"]
    assert len(profile_items) == 1
    assert profile_items[0]["source_kinds"] == [
        "provider_directory_fhir",
        "state_regulator",
    ]
    assert profile_items[0]["source_ids"] == [
        "directory-source",
        "directory-source-copy",
    ]
    assert profile_items[0]["source_count"] == 2
    assert profile_items[0]["independent_source_count"] == 2
    assert {assertion["source_kind"] for assertion in profile_items[0]["assertions"]} == {
        "state_regulator",
        "provider_directory_fhir",
    }
    assert profile_items[0]["assertion_count"] == 4
    assert len(profile_items[0]["assertions"]) == 2

    evidence = compose_provider_profile_evidence(
        state_projection={
            "evidence": {
                "records": [
                    {
                        "source_record_id": "state-name-record",
                        "artifact_id": "state-artifact",
                    }
                ]
            }
        },
        fhir_evidence={
            "facts": {
                "name": {
                    "items": [
                        {
                            "value": name_value_by_key,
                            "source_ids": ["directory-source"],
                        }
                    ],
                    "total": 1,
                }
            }
        },
        provider_profile=profile,
    )
    assert len(evidence["sources"]["state_regulator"]["records"]) == 1
    assert len(
        evidence["sources"]["provider_directory_fhir"]["facts"]["name"]["items"]
    ) == 1


def test_composer_marks_filtered_sensitive_items_restricted():
    state_profile_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": 1000000004,
        "categories": {
            category: {"availability": "unavailable", "items": []}
            for category in STANDARD_CATEGORIES
        },
        "sources": [],
    }
    state_profile_by_key["categories"]["complaints"] = {
        "availability": "available",
        "items": [
            {
                "type": "administrative_complaint",
                "display": "Administrative complaint",
                "value": {"case": "synthetic"},
                "sensitive": True,
                "public_default": False,
            }
        ],
    }
    compact = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_key},
        fhir_profile=None,
    )
    assert compact["categories"]["complaints"] == {
        "availability": "restricted",
        "items": [],
        "total": 0,
        "returned": 0,
        "truncated": False,
    }
    expanded = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_key},
        fhir_profile=None,
        include_sensitive=True,
    )
    assert len(expanded["categories"]["complaints"]["items"]) == 1


def test_single_category_mode_is_stably_sorted_and_paginated():
    state_profile_by_key = {
        "schema_version": PROFILE_SCHEMA_VERSION,
        "npi": 1000000004,
        "generation_id": "generation-one",
        "categories": {
            category: {"availability": "unavailable", "items": []}
            for category in STANDARD_CATEGORIES
        },
        "sources": [],
    }
    state_profile_by_key["categories"]["education"] = {
        "availability": "available",
        "items": [
            {
                "type": "education_history",
                "display": display,
                "value": {"school": display},
                "source_record_id": f"record-{display}",
                "sensitive": False,
                "public_default": True,
            }
            for display in ("Zulu College", "Alpha College", "Middle College")
        ],
    }
    page = compose_provider_profile(
        1000000004,
        state_projection={"profile": state_profile_by_key},
        fhir_profile=None,
        requested_categories=["education"],
        page_category="education",
        page_limit=2,
        page_offset=1,
    )
    assert [profile_item["display"] for profile_item in page["categories"]["education"]["items"]] == [
        "Middle College",
        "Zulu College",
    ]
    assert all(
        len(profile_item["item_id"]) == 64
        for profile_item in page["categories"]["education"]["items"]
    )
    assert page["category_pagination"] == {
        "category": "education",
        "total": 3,
        "returned": 2,
        "limit": 2,
        "offset": 1,
        "has_more": False,
    }


def test_paged_evidence_contains_only_returned_source_records():
    profile_by_key = {
        "categories": {
            "education": {
                "items": [
                    {
                        "type": "education_history",
                        "value": {"school": "Alpha College"},
                        "source_record_id": "returned-record",
                    }
                ]
            }
        }
    }
    evidence = compose_provider_profile_evidence(
        state_projection={
            "evidence": {
                "records": [
                    {"source_record_id": "returned-record", "artifact_id": "one"},
                    {"source_record_id": "other-record", "artifact_id": "two"},
                ]
            }
        },
        fhir_evidence=None,
        provider_profile=profile_by_key,
        page_category="education",
    )
    assert evidence["sources"]["state_regulator"]["records"] == [
        {"source_record_id": "returned-record", "artifact_id": "one"}
    ]


def test_compact_evidence_excludes_unrequested_and_restricted_records():
    state_profile_by_key = {
        "categories": {
            "education": {
                "items": [
                    {
                        "type": "education_history",
                        "value": {"school": "Alpha College"},
                        "source_record_id": "education-record",
                    }
                ]
            },
            "complaints": {
                "availability": "restricted",
                "items": [],
            },
        }
    }
    evidence = compose_provider_profile_evidence(
        state_projection={
            "evidence": {
                "records": [
                    {"source_record_id": "education-record", "artifact_id": "one"},
                    {"source_record_id": "complaint-record", "artifact_id": "two"},
                ]
            }
        },
        fhir_evidence=None,
        provider_profile=state_profile_by_key,
    )

    assert evidence["sources"]["state_regulator"]["records"] == [
        {"source_record_id": "education-record", "artifact_id": "one"}
    ]
