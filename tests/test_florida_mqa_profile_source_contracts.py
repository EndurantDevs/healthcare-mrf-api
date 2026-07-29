# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
import zipfile
import os
from pathlib import Path

import pytest

from api.provider_profile import compose_provider_profile

florida = importlib.import_module("process.florida_mqa_profile")

_REAL_ARTIFACT_ROOT = Path(
    os.getenv(
        "HLTHPRT_TEST_FL_MQA_ARTIFACT_ROOT",
        (
            "/Volumes/Data/healthporta/florida-mqa/"
            "44025b2a241a4eb98c839197ccf31e37"
        ),
    )
)
_ARTIFACT = {
    "artifact_id": "synthetic-artifact",
    "content_sha256": "0" * 64,
    "source_url": "https://example.invalid/provider-profile",
}
_NPI = 1000000004


def _facts(source_key: str, row: dict[str, str]):
    return florida._facts_for_row(
        florida.FLORIDA_SOURCES[source_key],
        row,
        run_id="synthetic-run",
        record_id=f"synthetic-{source_key}",
        npi=_NPI,
        artifact=_ARTIFACT,
    )


def _nested_keys(value):
    if isinstance(value, dict):
        for key, item in value.items():
            yield key
            yield from _nested_keys(item)
    elif isinstance(value, list):
        for item in value:
            yield from _nested_keys(item)


def test_every_profile_data_source_has_an_exact_header_contract():
    profile_sources_by_key = {
        key: source
        for key, source in florida.FLORIDA_SOURCES.items()
        if source.path == "/ProfileData"
    }

    assert set(profile_sources_by_key) == set(florida._PROFILE_EXPECTED_FIELDS)
    assert len(profile_sources_by_key) == 21
    for key, source in profile_sources_by_key.items():
        assert source.expected_fields == florida._PROFILE_EXPECTED_FIELDS[key]
        assert source.expected_fields


@pytest.mark.skipif(
    not _REAL_ARTIFACT_ROOT.exists(),
    reason="retained Florida header artifacts are not available",
)
def test_retained_profile_data_artifacts_match_exact_header_contracts():
    verified_items = []
    for key, source in florida.FLORIDA_SOURCES.items():
        if source.path != "/ProfileData":
            continue
        header_items = tuple(
            florida._artifact_header(
                _REAL_ARTIFACT_ROOT / source.filename,
                source,
            )
        )
        assert header_items == source.expected_fields
        verified_items.append(key)

    assert len(verified_items) == 21


def test_profile_data_header_order_or_addition_fails_closed(tmp_path):
    source = florida.FLORIDA_SOURCES["education"]
    changed_header_items = [*source.expected_fields, "unexpected_new_field"]
    artifact = tmp_path / source.filename
    artifact.write_text("|".join(changed_header_items) + "\n", encoding="latin-1")

    with pytest.raises(
        RuntimeError,
        match="florida_mqa_schema_changed:education:expected_header",
    ):
        florida._artifact_header(artifact, source)


def test_every_mapped_profile_source_uses_readable_canonical_fields():
    expected_mapped_sources = {
        key
        for key, profile_source in florida.FLORIDA_SOURCES.items()
        if profile_source.path == "/ProfileData"
    } - {
        "profile_master",
        "profile_indicators",
        "counties",
        "financial_responsibility",
    }
    assert set(florida._PROFILE_VALUE_FIELDS) == expected_mapped_sources

    for source_key, field_map in florida._PROFILE_VALUE_FIELDS.items():
        date_fields = florida._PROFILE_DATE_VALUE_FIELDS.get(
            source_key,
            frozenset(),
        )
        source_row_by_key = {
            source_field: (
                "2026-07-27"
                if output_field in date_fields
                else f"Synthetic {output_field.replace('_', ' ')}"
            )
            for output_field, source_field in field_map
        }
        source_row_by_key.update(
            {
                "pro_cde": "internal-profession",
                "lic_id": "internal-license-id",
                "rec_id": "internal-record-id",
                "rec_key": "internal-record-key",
            }
        )

        facts = _facts(source_key, source_row_by_key)

        assert len(facts) == 1
        fact = facts[0]
        assert fact["category"] == florida.FLORIDA_SOURCES[source_key].category
        assert fact["display"].startswith(
            florida.FLORIDA_SOURCES[source_key].title
        )
        assert not (
            set(_nested_keys(fact["value_json"]))
            & florida._INTERNAL_PROFILE_FIELDS
        )
        assert all(
            output_field in fact["value_json"]
            for output_field, _source_field in field_map
        )


def test_education_adapter_preserves_human_dates_and_meaning():
    fact = _facts(
        "education",
        {
            "pro_cde": "1501",
            "lic_id": "internal-license-id",
            "inst_nme": "Synthetic Medical College",
            "grad_dte": "07/15/2010",
            "deg_cert_earn_cde": "MD",
            "pgm_desc": "Medicine",
            "educ_mjr": "Clinical medicine",
            "atnd_frm_dte": "08/01/2006",
            "atnd_to_dte": "05/31/2010",
            "educ_prvr_nbr": "SYNTHETIC-SCHOOL",
        },
    )[0]

    assert fact["display"] == (
        "Education: Synthetic Medical College — Medicine — "
        "Clinical medicine — 2010-07-15"
    )
    assert fact["value_json"] == {
        "institution": "Synthetic Medical College",
        "graduation_date": "2010-07-15",
        "graduation_date_precision": "day",
        "degree_or_certificate_code": "MD",
        "program": "Medicine",
        "major": "Clinical medicine",
        "attendance_start": "2006-08-01",
        "attendance_start_precision": "day",
        "attendance_end": "2010-05-31",
        "attendance_end_precision": "day",
        "institution_identifier": "SYNTHETIC-SCHOOL",
    }
    assert fact["effective_start"] == "2006-08-01"
    assert fact["effective_end"] == "2010-05-31"


def _assert_profile_indicator_visibility(profile):
    """Assert profile indicator visibility."""
    public_profile = compose_provider_profile(
        _NPI,
        state_projection={
            "profile": profile,
            "generation_id": "synthetic-generation",
        },
        fhir_profile=None,
        requested_categories=[
            "professional_experience",
            "criminal_disclosures",
            "regulatory_actions",
        ],
    )
    sensitive_profile = compose_provider_profile(
        _NPI,
        state_projection={
            "profile": profile,
            "generation_id": "synthetic-generation",
        },
        fhir_profile=None,
        include_sensitive=True,
        requested_categories=[
            "professional_experience",
            "criminal_disclosures",
            "regulatory_actions",
        ],
    )

    assert public_profile["categories"]["professional_experience"]["total"] == 1
    assert public_profile["categories"]["criminal_disclosures"][
        "availability"
    ] == "restricted"
    assert public_profile["categories"]["regulatory_actions"][
        "availability"
    ] == "restricted"
    assert sensitive_profile["categories"]["criminal_disclosures"]["total"] == 1
    assert sensitive_profile["categories"]["regulatory_actions"]["total"] == 1


def test_profile_indicators_keep_email_raw_only_and_disclosures_restricted():
    """Verify profile indicators keep email raw only and disclosures restricted."""
    source_row_by_key = {
        "pro_cde": "1501",
        "lic_id": "internal-license-id",
        "health_degree": "Y",
        "grad_med_edu": "N",
        "prof_post_train": "Y",
        "faculty_appoint": "N",
        "staff_priv": "Y",
        "certification": "Y",
        "criminal_offense": "Y",
        "medicaid_prgrm": "N",
        "e_mail_addr": "private@example.invalid",
    }

    facts = _facts("profile_indicators", source_row_by_key)
    public_facts = [
        fact
        for fact in facts
        if not fact["sensitive"] or fact["public_default"]
    ]
    restricted_facts = [
        fact
        for fact in facts
        if fact["sensitive"] and not fact["public_default"]
    ]

    assert len(public_facts) == 1
    assert public_facts[0]["fact_type"] == "profile_section_availability"
    public_json = json.dumps(public_facts, sort_keys=True, default=str)
    assert "private@example.invalid" not in public_json
    assert "criminal_offense" not in public_json
    assert "medicaid" not in public_json.lower()
    assert {
        fact["fact_type"] for fact in restricted_facts
    } == {
        "criminal_offense_disclosure_indicator",
        "medicaid_program_disclosure_indicator",
    }
    assert "private@example.invalid" not in json.dumps(
        facts,
        sort_keys=True,
        default=str,
    )

    profile, _evidence = florida._projection(
        _NPI,
        "synthetic-generation",
        facts,
        {
            "professional_experience",
            "criminal_disclosures",
            "regulatory_actions",
        },
    )
    _assert_profile_indicator_visibility(profile)


def test_profile_indicator_parser_recovers_shifted_raw_only_email(tmp_path):
    profile_source = florida.FLORIDA_SOURCES["profile_indicators"]
    field_values = [""] * len(profile_source.expected_fields)
    field_values[profile_source.expected_fields.index("pro_cde")] = "1501"
    field_values[profile_source.expected_fields.index("lic_id")] = "42"
    field_values[profile_source.expected_fields.index("health_degree")] = "Y"
    assert profile_source.expected_fields[-1] == "e_mail_addr"
    assert field_values[-1] == ""
    metrics_by_key: dict[str, int] = {}
    profile_source, artifact = _write_pipe_artifact(
        tmp_path,
        "profile_indicators",
        [*field_values, "private@example.invalid", ""],
    )

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(source_rows) == 1
    _row_number, raw_row, normalized, _header = source_rows[0]
    assert raw_row["e_mail_addr"] == "private@example.invalid"
    assert normalized["e_mail_addr"] == "private@example.invalid"
    assert normalized["_source_parse_repair"] == (
        "shifted_raw_email_recovered"
    )
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
        "recovered_rows": 1,
    }
    facts = _facts("profile_indicators", normalized)
    assert "private@example.invalid" not in json.dumps(
        facts,
        sort_keys=True,
        default=str,
    )


def test_financial_fact_never_leaks_liability_indicator_publicly():
    facts = _facts(
        "financial_responsibility",
        {
            "pro_cde": "1501",
            "lic_id": "internal-license-id",
            "financial_resp": "Maintains required coverage",
            "financial_exempt": "N",
            "liability_claim": "Y",
            "insured": "Y",
            "insured_10_yr": "N",
        },
    )
    public_fact = next(fact for fact in facts if fact["public_default"])
    liability_fact = next(
        fact for fact in facts if fact["fact_type"] == "liability_claim_indicator"
    )

    public_json = json.dumps(public_fact, sort_keys=True, default=str)
    assert "liability" not in public_json.lower()
    assert public_fact["category"] == "financial_responsibility"
    assert liability_fact["category"] == "liability_claims"
    assert liability_fact["sensitive"] is True
    assert liability_fact["public_default"] is False


@pytest.mark.parametrize(
    "source_key",
    ["license_status", "licensure_current", "licensure_all_statuses"],
)
def test_public_license_fact_splits_restricted_regulatory_indicators(source_key):
    facts = _facts(
        source_key,
        {
            "pro_cde": "1501",
            "profession_name": "Medical Doctor",
            "rank_cde": "ME",
            "rank_code": "ME",
            "lic_nbr": "ME12345",
            "license_number": "ME12345",
            "lic_sta_desc": "CLEAR/ACTIVE",
            "license_status_description": "CLEAR/ACTIVE",
            "board_action_indicator": "Y",
            "administrative_complaints_indicator": "Y",
            "emergency_order_indicator": "N",
            "final_order_indicator": "Y",
            "multi_state_license_indicator": "Y",
            "prescribe_ind": "Y",
            "dispensing_ind": "N",
            "other_license": "Y",
        },
    )
    public_fact = next(fact for fact in facts if fact["category"] == "licenses")
    restricted_fact = next(
        fact
        for fact in facts
        if fact["fact_type"] == "license_regulatory_indicators"
    )
    public_json = json.dumps(public_fact, sort_keys=True, default=str)

    for restricted_key in (
        "board_action",
        "administrative_complaints",
        "emergency_order",
        "final_order",
    ):
        assert restricted_key not in public_json
    assert public_fact["value_json"]["license_indicators"] == {
        "multi_state_license": "Y",
        "prescribing": "Y",
        "dispensing": "N",
        "other_license": "Y",
    }
    assert restricted_fact["category"] == "regulatory_actions"
    assert restricted_fact["sensitive"] is True
    assert restricted_fact["public_default"] is False


def test_raw_and_normalized_profile_indicator_record_retains_private_source_data():
    raw_payload_by_key = {
        "PRO_CDE": "1501",
        "LIC_ID": "42",
        "E_MAIL_ADDR": "private@example.invalid",
    }
    normalized_payload_by_key = {
        "pro_cde": "1501",
        "lic_id": "42",
        "e_mail_addr": "private@example.invalid",
    }

    retained = florida._retained_source_record(
        record_id="r" * 64,
        run_id="synthetic-run",
        artifact_id="synthetic-artifact",
        source_key="profile_indicators",
        source_record_key="profile_indicators:1501:42",
        profession_code="1501",
        license_id="42",
        license_number=None,
        raw_payload=raw_payload_by_key,
        normalized_payload=normalized_payload_by_key,
        matched_npi=_NPI,
        match_status="deterministic",
        match_evidence={"method": "profile_master_profession_license_id"},
        row_number=2,
    )

    assert retained["raw_payload"] == raw_payload_by_key
    assert retained["normalized_payload"] == normalized_payload_by_key


def _write_pipe_artifact(
    tmp_path,
    source_key: str,
    physical_values: list[str],
):
    source = florida.FLORIDA_SOURCES[source_key]
    artifact = tmp_path / source.filename
    artifact.write_text(
        "|".join(source.expected_fields)
        + "\n"
        + "|".join(physical_values)
        + "\n",
        encoding="latin-1",
    )
    return source, artifact


def test_pipe_parser_preserves_literal_quotes(tmp_path):
    source = florida.FLORIDA_SOURCES["education"]
    values = [""] * len(source.expected_fields)
    values[source.expected_fields.index("pro_cde")] = "1501"
    values[source.expected_fields.index("lic_id")] = "42"
    values[source.expected_fields.index("inst_nme")] = (
        'Synthetic "Quoted" Medical College'
    )
    source, artifact = _write_pipe_artifact(tmp_path, "education", values)

    rows = list(florida._iter_rows(artifact, source))

    assert len(rows) == 1
    assert rows[0][2]["inst_nme"] == 'Synthetic "Quoted" Medical College'


def test_pipe_parser_accepts_source_fields_larger_than_csv_default(tmp_path):
    source = florida.FLORIDA_SOURCES["publications"]
    values = [""] * len(source.expected_fields)
    values[source.expected_fields.index("pro_cde")] = "1501"
    values[source.expected_fields.index("lic_id")] = "42"
    values[source.expected_fields.index("article_title")] = "X" * 200_000
    source, artifact = _write_pipe_artifact(tmp_path, "publications", values)

    rows = list(florida._iter_rows(artifact, source))

    assert len(rows) == 1
    assert len(rows[0][2]["article_title"]) == 200_000


def test_pipe_parser_explicitly_trims_one_trailing_empty_sentinel(tmp_path):
    source = florida.FLORIDA_SOURCES["education"]
    values = [""] * len(source.expected_fields)
    values[source.expected_fields.index("pro_cde")] = "1501"
    values[source.expected_fields.index("lic_id")] = "42"
    values[source.expected_fields.index("inst_nme")] = "Synthetic College"
    metrics_by_key: dict[str, int] = {}
    source, artifact = _write_pipe_artifact(
        tmp_path,
        "education",
        [*values, ""],
    )

    rows = list(
        florida._iter_rows(
            artifact,
            source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(rows) == 1
    assert rows[0][2]["inst_nme"] == "Synthetic College"
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
    }


def test_pipe_parser_quarantines_multiple_trailing_empty_fields(tmp_path):
    source = florida.FLORIDA_SOURCES["education"]
    values = [""] * len(source.expected_fields)
    values[source.expected_fields.index("pro_cde")] = "1501"
    values[source.expected_fields.index("lic_id")] = "42"
    metrics_by_key: dict[str, int] = {}
    source, artifact = _write_pipe_artifact(
        tmp_path,
        "education",
        [*values, "", ""],
    )

    rows = list(
        florida._iter_rows(
            artifact,
            source,
            parser_metrics=metrics_by_key,
        )
    )

    assert rows[0][2]["_source_parse_quarantine"] == (
        "field_count_mismatch"
    )
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
        "quarantined_rows": 1,
    }


def _assert_licensure_email_visibility(normalized, profile_source, source_key):
    """Assert licensure email visibility."""
    public_facts = florida._facts_for_row(
        profile_source,
        normalized,
        run_id="synthetic-run",
        record_id=f"synthetic-{source_key}",
        npi=_NPI,
        artifact=_ARTIFACT,
    )
    assert "first@example.invalid" not in json.dumps(
        public_facts,
        sort_keys=True,
        default=str,
    )


@pytest.mark.parametrize(
    "source_key",
    ["licensure_current", "licensure_all_statuses"],
)
def test_licensure_parser_recovers_embedded_email_delimiter_as_raw_only(
    tmp_path,
    source_key,
):
    """Verify licensure parser recovers embedded email delimiter as raw only."""
    profile_source = florida.FLORIDA_SOURCES[source_key]
    field_values = [""] * len(profile_source.expected_fields)
    field_values[profile_source.expected_fields.index("pro_cde")] = "1501"
    field_values[profile_source.expected_fields.index("profession_name")] = "Medical Doctor"
    field_values[profile_source.expected_fields.index("rank_code")] = "ME"
    field_values[profile_source.expected_fields.index("license_number")] = "12345"
    field_values[profile_source.expected_fields.index("license_status_description")] = (
        "Clear/Active"
    )
    email_index = profile_source.expected_fields.index("email")
    physical_values = [
        *field_values[:email_index],
        "first@example.invalid",
        "second@example.invalid",
        *field_values[email_index + 1 :],
        "",
    ]
    metrics_by_key: dict[str, int] = {}
    profile_source, artifact = _write_pipe_artifact(
        tmp_path,
        source_key,
        physical_values,
    )

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(source_rows) == 1
    _row_number, raw_row, normalized, _header = source_rows[0]
    assert raw_row["email"] == (
        "first@example.invalid|second@example.invalid"
    )
    assert raw_row["_source_parse_metadata"]["field"] == "email"
    assert normalized["email"] == (
        "first@example.invalid|second@example.invalid"
    )
    assert normalized["_source_parse_repair"] == (
        "embedded_delimiter_recovered"
    )
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
        "recovered_rows": 1,
    }
    _assert_licensure_email_visibility(normalized, profile_source, source_key)


def test_licensure_parser_quarantines_ambiguous_extra_field(tmp_path):
    profile_source = florida.FLORIDA_SOURCES["licensure_current"]
    field_values = [""] * len(profile_source.expected_fields)
    field_values[profile_source.expected_fields.index("pro_cde")] = "1501"
    field_values[profile_source.expected_fields.index("license_number")] = "12345"
    email_index = profile_source.expected_fields.index("email")
    physical_values = [
        *field_values[:email_index],
        "ambiguous-prefix",
        "private@example.invalid",
        *field_values[email_index + 1 :],
        "",
    ]
    metrics_by_key: dict[str, int] = {}
    profile_source, artifact = _write_pipe_artifact(
        tmp_path,
        "licensure_current",
        physical_values,
    )

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert source_rows[0][2]["_source_parse_quarantine"] == (
        "field_count_mismatch"
    )
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
        "quarantined_rows": 1,
    }


def test_unrecognized_width_mismatch_is_retained_for_quarantine(tmp_path):
    profile_source = florida.FLORIDA_SOURCES["education"]
    field_values = [""] * len(profile_source.expected_fields)
    field_values[profile_source.expected_fields.index("pro_cde")] = "1501"
    field_values[profile_source.expected_fields.index("lic_id")] = "42"
    field_values[profile_source.expected_fields.index("inst_nme")] = "Synthetic College"
    metrics_by_key: dict[str, int] = {}
    profile_source, artifact = _write_pipe_artifact(
        tmp_path,
        "education",
        [*field_values, "unexpected-nonempty-field"],
    )

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(source_rows) == 1
    _row_number, raw_row, normalized, _header = source_rows[0]
    assert raw_row["_physical_fields"][-1] == "unexpected-nonempty-field"
    assert raw_row["_source_parse_metadata"]["kind"] == (
        "field_count_mismatch"
    )
    assert raw_row["_source_parse_metadata"]["physical_row_sha256"] == (
        florida.hashlib.sha256(
            "|".join([*field_values, "unexpected-nonempty-field"]).encode(
                "latin-1"
            )
        ).hexdigest()
    )
    assert normalized["_source_parse_quarantine"] == (
        "field_count_mismatch"
    )
    assert metrics_by_key == {"quarantined_rows": 1}


def _assert_license_status_continuation(metrics_by_key, raw_row):
    """Assert license status continuation."""
    assert len(
        raw_row["_source_parse_metadata"]["physical_row_sha256"]
    ) == 3
    assert metrics_by_key == {
        "recovered_rows": 1,
        "continuation_physical_rows": 3,
    }


def test_license_status_parser_recovers_fixed_width_name_continuation(
    tmp_path,
):
    """Verify license status parser recovers fixed width name continuation."""
    profile_source = florida.FLORIDA_SOURCES["license_status"]
    fields = [
        "1501",
        "ME",
        "1234",
        "Active",
        "Clear",
        "01/01/2000",
        "01/01/2030",
        "01/01/2025",
        "ALEX",
        "MIDDLE",
        "L" * 80,
        "N",
        "N",
        "N",
        "N",
    ]
    logical_row = "|".join(fields).ljust(375)
    physical_rows = [
        logical_row[offset : offset + 125]
        for offset in range(0, 375, 125)
    ]
    assert [len(source_row.split("|")) for source_row in physical_rows] == [11, 5, 1]
    artifact = tmp_path / profile_source.filename
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "lic_status.dat",
            "\n".join(physical_rows) + "\n",
        )
    metrics_by_key: dict[str, int] = {}

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(source_rows) == 1
    row_number, raw_row, normalized, header = source_rows[0]
    assert row_number == 1
    assert header == list(profile_source.expected_fields)
    assert normalized["l_name"] == "L" * 80
    assert raw_row["_source_parse_metadata"]["kind"] == (
        "wrapped_license_name_recovered"
    )
    assert raw_row["_source_parse_metadata"]["physical_row_numbers"] == [
        1,
        2,
        3,
    ]
    _assert_license_status_continuation(metrics_by_key, raw_row)


def test_license_status_parser_does_not_swallow_invalid_continuation(
    tmp_path,
):
    profile_source = florida.FLORIDA_SOURCES["license_status"]
    fields = [
        "1501",
        "ME",
        "1234",
        "Active",
        "Clear",
        "not-a-date",
        "01/01/2030",
        "01/01/2025",
        "ALEX",
        "MIDDLE",
        "L" * 80,
        "N",
        "N",
        "N",
        "N",
    ]
    logical_row = "|".join(fields).ljust(375)
    physical_rows = [
        logical_row[offset : offset + 125]
        for offset in range(0, 375, 125)
    ]
    artifact = tmp_path / profile_source.filename
    with zipfile.ZipFile(artifact, "w") as archive:
        archive.writestr(
            "lic_status.dat",
            "\n".join(physical_rows) + "\n",
        )
    metrics_by_key: dict[str, int] = {}

    source_rows = list(
        florida._iter_rows(
            artifact,
            profile_source,
            parser_metrics=metrics_by_key,
        )
    )

    assert len(source_rows) == 3
    assert [source_row[0] for source_row in source_rows] == [1, 2, 3]
    assert all(
        source_row[2]["_source_parse_quarantine"] == "field_count_mismatch"
        for source_row in source_rows
    )
    assert metrics_by_key == {"quarantined_rows": 3}


def test_source_validation_allows_only_bounded_quarantine():
    within_threshold = florida._source_validation_guard_reasons(
        {
            "education": {
                "schema_complete": True,
                "rows": 10_000,
                "quarantined_rows": 5,
                "max_quarantined_rows": 100,
                "max_quarantined_ratio": 0.001,
                "header_sha256": "0" * 64,
            }
        },
        expected_source_keys={"education"},
    )
    above_threshold = florida._source_validation_guard_reasons(
        {
            "education": {
                "schema_complete": True,
                "rows": 1_000,
                "quarantined_rows": 2,
                "max_quarantined_rows": 100,
                "max_quarantined_ratio": 0.001,
                "header_sha256": "0" * 64,
            }
        },
        expected_source_keys={"education"},
    )

    assert within_threshold == []
    assert above_threshold == [
        "source_quarantine_ratio_exceeded:education:2/1000>0.001"
    ]


def test_source_validation_fails_if_quarantine_count_exceeds_budget():
    reasons = florida._source_validation_guard_reasons(
        {
            "education": {
                "schema_complete": True,
                "rows": 1_000_000,
                "quarantined_rows": 101,
                "max_quarantined_rows": 100,
                "max_quarantined_ratio": 0.001,
                "header_sha256": "0" * 64,
            }
        },
        expected_source_keys={"education"},
    )

    assert reasons == [
        "source_quarantine_count_exceeded:education:101>100"
    ]


@pytest.mark.skipif(
    not _REAL_ARTIFACT_ROOT.exists(),
    reason="retained Florida artifacts are not available",
)
@pytest.mark.parametrize(
    ("source_key", "filename"),
    (
        ("licensure_current", "LIC_ALL.zip"),
        ("licensure_all_statuses", "PROF_ALL.zip"),
    ),
)
def test_real_licensure_first_row_aligns_after_trailing_sentinel(
    source_key,
    filename,
):
    source = florida.FLORIDA_SOURCES[source_key]
    metrics_by_key: dict[str, int] = {}

    _row_number, raw_row, normalized, header = next(
        florida._iter_rows(
            _REAL_ARTIFACT_ROOT / filename,
            source,
            parser_metrics=metrics_by_key,
        )
    )

    assert header == list(source.expected_fields)
    assert "" not in raw_row
    assert len(raw_row) == len(source.expected_fields)
    assert normalized.get("license_number")
    assert metrics_by_key == {
        "trailing_empty_rows": 1,
        "trailing_empty_fields": 1,
    }


@pytest.mark.skipif(
    not _REAL_ARTIFACT_ROOT.exists(),
    reason="retained Florida artifacts are not available",
)
def test_real_profile_indicators_shifted_email_shape_is_fully_recovered():
    source = florida.FLORIDA_SOURCES["profile_indicators"]
    metrics_by_key: dict[str, int] = {}
    rows = sum(
        1
        for _row in florida._iter_rows(
            _REAL_ARTIFACT_ROOT / source.filename,
            source,
            parser_metrics=metrics_by_key,
        )
    )

    assert rows == 195_709
    assert metrics_by_key == {
        "trailing_empty_rows": 195_709,
        "trailing_empty_fields": 195_709,
        "recovered_rows": 195_709,
    }


@pytest.mark.skipif(
    not _REAL_ARTIFACT_ROOT.exists(),
    reason="retained Florida artifacts are not available",
)
def test_real_license_status_continuation_is_fully_recovered():
    source = florida.FLORIDA_SOURCES["license_status"]
    metrics_by_key: dict[str, int] = {}
    rows = sum(
        1
        for _row in florida._iter_rows(
            _REAL_ARTIFACT_ROOT / source.filename,
            source,
            parser_metrics=metrics_by_key,
        )
    )

    assert rows == 3_529_816
    assert metrics_by_key == {
        "recovered_rows": 1,
        "continuation_physical_rows": 3,
    }
