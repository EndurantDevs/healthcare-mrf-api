from __future__ import annotations

import importlib
import zipfile
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

florida = importlib.import_module("process.florida_mqa_profile")


class _Response:
    def __init__(self, body: str, url: str = "https://example.invalid/policy"):
        self._body = body.encode()
        self._url = url

    def read(self, _size=-1):
        return self._body

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def _client_with_responses(monkeypatch, responses):
    client = florida.FloridaMQAClient(
        "https://example.invalid",
        "test-user",
        "test-password",
    )
    iterator = iter(responses)
    monkeypatch.setattr(client, "_open", lambda _request: next(iterator))
    return client


@pytest.mark.parametrize(
    ("responses", "message"),
    [
        (
            [_Response("<html>login</html>")],
            "florida_mqa_login_settings_missing",
        ),
        (
            [
                _Response(
                    'var SETTINGS = {"policy":"p","transId":"t"};',
                    "https://example.invalid/p/oauth2/v2.0/authorize",
                )
            ],
            "florida_mqa_login_contract_changed",
        ),
        (
            [
                _Response(
                    'var SETTINGS = {"policy":"p","transId":"t","csrf":"c"};',
                    "https://example.invalid/p/oauth2/v2.0/authorize",
                ),
                _Response('{"status":"400"}'),
            ],
            "florida_mqa_login_rejected",
        ),
        (
            [
                _Response(
                    'var SETTINGS = {"policy":"p","transId":"t","csrf":"c"};',
                    "https://example.invalid/p/oauth2/v2.0/authorize",
                ),
                _Response('{"status":"200"}'),
                _Response("<html>callback contract missing</html>"),
            ],
            "florida_mqa_login_callback_missing",
        ),
        (
            [
                _Response(
                    'var SETTINGS = {"policy":"p","transId":"t","csrf":"c"};',
                    "https://example.invalid/p/oauth2/v2.0/authorize",
                ),
                _Response('{"status":"200"}'),
                _Response('<form action="/callback"><input name="state" value="ok"></form>'),
                _Response("<html>still signed out</html>"),
            ],
            "florida_mqa_login_callback_failed",
        ),
    ],
)
def test_portal_authentication_fails_closed_for_contract_drift(
    monkeypatch,
    responses,
    message,
):
    client = _client_with_responses(monkeypatch, responses)

    with pytest.raises(RuntimeError, match=message):
        client.authenticate()


def test_portal_authentication_short_circuits_existing_session(monkeypatch):
    client = _client_with_responses(
        monkeypatch,
        [_Response("<html>Sign out</html>")],
    )
    client.authenticate()


def test_client_open_uses_bounded_network_timeout(monkeypatch):
    client = florida.FloridaMQAClient(
        "https://example.invalid",
        "test-user",
        "test-password",
    )
    opener = SimpleNamespace(open=Mock(return_value="response"))
    monkeypatch.setattr(client, "opener", opener)

    result = client._open("https://example.invalid/ProfileData")

    assert result == "response"
    opener.open.assert_called_once_with(
        "https://example.invalid/ProfileData",
        timeout=120,
    )


def test_identity_helpers_cover_empty_and_source_specific_inputs():
    assert florida._license_candidates("", "1501") == ()
    assert florida._profession_details(
        "Physician",
        {"physician": {("1501", "01")}},
    ) == ("1501", "01")

    complaint = florida._canonical_match_row(
        florida.FLORIDA_SOURCES["administrative_complaints"],
        {
            "profession": "Physician",
            "respondent_name": "Example, Alex",
        },
        {"physician": {("1501", "01")}},
    )
    assert complaint["first_name"] == "Alex"
    assert complaint["last_name"] == "Example"
    assert complaint["pro_cde"] == "1501"

    pain = florida._canonical_match_row(
        florida.FLORIDA_SOURCES["pain_management_report"],
        {
            "reporting_phy_prof": "Unknown profession",
            "reporting_phy_name": "No comma name",
            "reporting_phy_lic_nbr": "ME55",
        },
    )
    assert pain["license_number"] == "ME55"
    assert "first_name" not in pain

    pharmacy = florida._canonical_match_row(
        florida.FLORIDA_SOURCES["pharmacy_pharmacist"],
        {
            "rltn_prof_nme": "Pharmacist",
            "rltn_key_nme": "Example, Pat",
            "rltn_lic_nbr": "PS99",
        },
    )
    assert pharmacy["lic_nbr"] == "PS99"

    licensure = florida._canonical_match_row(
        florida.FLORIDA_SOURCES["licensure_current"],
        {
            "profession_name": "Physician",
            "rank_code": "01",
        },
    )
    assert licensure["rank_cde"] == "01"


def test_name_and_display_helpers_fail_conservatively():
    assert not florida._is_name_compatible(
        {"first_name": "Alex"},
        {"first_name": "Brooke", "last_name": "Example"},
    )
    assert florida._is_name_compatible({}, {})
    source = florida.FLORIDA_SOURCES["education"]
    assert florida._human_display(source, {"other": "Readable value"}).endswith(
        "Readable value"
    )
    assert florida._human_display(source, {}) == source.title


def test_archive_stream_skips_directories_and_pdfs(tmp_path):
    archive_path = tmp_path / "profile.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("folder/", "")
        archive.writestr("metadata.pdf", "not data")
        archive.writestr("facts.txt", "a|b\n1|2\n")

    members = list(florida._data_stream(archive_path))

    assert [name for name, _stream in members] == ["facts.txt"]
    for _name, stream in members:
        stream.close()


def test_header_contract_errors_are_specific_and_auditable(tmp_path):
    profile_source = florida.FLORIDA_SOURCES["profile_master"]
    assert florida._normalized_source_header(
        replace(profile_source, expected_fields=()),
        ["", " Given Name ", None],
        artifact_name="fixture.txt",
    ) == ["given_name"]
    with pytest.raises(RuntimeError, match="florida_mqa_header_missing"):
        florida._normalized_source_header(
            replace(profile_source, expected_fields=()),
            ["", None],
            artifact_name="fixture.txt",
        )

    empty_archive = tmp_path / "empty.zip"
    with zipfile.ZipFile(empty_archive, "w") as archive:
        archive.writestr("folder/", "")
    with pytest.raises(RuntimeError, match="florida_mqa_header_missing"):
        florida._artifact_header(empty_archive, profile_source)

    inconsistent = tmp_path / "inconsistent.zip"
    with zipfile.ZipFile(inconsistent, "w") as archive:
        archive.writestr("one.txt", "a|b\n")
        archive.writestr("two.txt", "a|c\n")
    with pytest.raises(
        RuntimeError,
        match="florida_mqa_archive_headers_inconsistent",
    ):
        florida._artifact_header(
            inconsistent,
            replace(profile_source, expected_fields=()),
        )

    headerless = tmp_path / "headerless.txt"
    headerless.write_text("a|b\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="headerless_schema_missing"):
        florida._artifact_header(
            headerless,
            replace(profile_source, has_header=False, expected_fields=()),
        )

    cannabis = florida.FLORIDA_SOURCES["medical_cannabis_authorization"]
    cannabis_path = tmp_path / "cannabis.txt"
    cannabis_path.write_text("a|b\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="cannabis_header_changed"):
        florida._artifact_header(cannabis_path, cannabis)


def test_alignment_and_continuation_recovery_reject_ambiguous_shapes():
    assert not florida._is_licensure_email_alignment_plausible(
        ["email"],
        ["a@example.test", "b@example.test"],
        0,
    )
    header_items = [
        "email",
        "mailing_address_state",
        "mailing_address_zipcode",
        "practice_location_address_state",
        "practice_location_address_zipcode",
    ]
    assert not florida._is_licensure_email_alignment_plausible(
        header_items,
        ["a@example.test", "FL", "33101", "FL", "33101", "Y"],
        0,
    )
    assert (
        florida._license_status_continuation_values(
            list(florida._LICENSE_STATUS_FIELDS),
            [(1, ["too", "short"])],
            artifact_member="fixture.txt",
            parser_metrics={},
        )
        is None
    )


def test_continuation_recovery_rejects_fixed_width_contract_drift(monkeypatch):
    physical_rows = [
        (1, [*["a" * 10] * 10, "a" * 15]),
        (2, [*["b" * 29] * 4, "b" * 5]),
        (3, [" " * 125]),
    ]
    assert all(len("|".join(field_values)) == 125 for _number, field_values in physical_rows)

    monkeypatch.setattr(
        florida,
        "_LICENSE_STATUS_FIELDS",
        tuple(f"field_{index}" for index in range(16)),
    )
    assert florida._license_status_continuation_values(
        list(florida._LICENSE_STATUS_FIELDS),
        physical_rows,
        artifact_member="fixture.txt",
        parser_metrics={},
    ) is None

    monkeypatch.setattr(
        florida,
        "_LICENSE_STATUS_FIELDS",
        tuple(f"field_{index}" for index in range(15)),
    )
    assert florida._license_status_continuation_values(
        list(florida._LICENSE_STATUS_FIELDS),
        physical_rows,
        artifact_member="fixture.txt",
        parser_metrics={},
    ) is None


def test_iter_rows_reports_empty_and_malformed_special_sources(tmp_path):
    empty = tmp_path / "empty.txt"
    empty.write_text("", encoding="latin-1")
    with pytest.raises(RuntimeError, match="florida_mqa_header_missing"):
        list(florida._iter_rows(empty))

    headerless = replace(
        florida.FLORIDA_SOURCES["license_status"],
        expected_fields=(),
    )
    source_row = tmp_path / "row.txt"
    source_row.write_text("one|two\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="headerless_schema_missing"):
        list(florida._iter_rows(source_row, headerless))

    cannabis = florida.FLORIDA_SOURCES["medical_cannabis_authorization"]
    bad_header = tmp_path / "bad-cannabis-header.txt"
    bad_header.write_text("a|b\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="schema_changed"):
        list(florida._iter_rows(bad_header, cannabis))

    valid_header = "|".join(cannabis.expected_fields)
    bad_row = tmp_path / "bad-cannabis-row.txt"
    bad_row.write_text(f"{valid_header}\na|b\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="cannabis_row_changed"):
        list(florida._iter_rows(bad_row, cannabis))

    cannabis_without_contract = replace(cannabis, expected_fields=())
    with pytest.raises(RuntimeError, match="cannabis_header_changed"):
        list(florida._iter_rows(bad_header, cannabis_without_contract))

    blank_header = tmp_path / "blank-header.txt"
    blank_header.write_text("|\n", encoding="latin-1")
    with pytest.raises(RuntimeError, match="florida_mqa_header_missing"):
        list(florida._iter_rows(blank_header))

    generic = tmp_path / "generic.txt"
    generic.write_text("First Name|Last Name\nAlex|Example\n", encoding="latin-1")
    parsed_items = list(florida._iter_rows(generic))
    assert parsed_items[0][2] == {
        "first_name": "Alex",
        "last_name": "Example",
    }


@pytest.mark.asyncio
async def test_license_index_ignores_blank_license_numbers(monkeypatch):
    row = SimpleNamespace(
        _mapping={
            "npi": 1000000004,
            "provider_license_number": "---",
            "healthcare_provider_taxonomy_code": "207Q00000X",
            "provider_first_name": "Alex",
            "provider_last_name": "Example",
        }
    )
    monkeypatch.setattr(florida.db, "all", AsyncMock(return_value=[row]))
    assert await florida._load_florida_license_index() == {}


@pytest.mark.asyncio
async def test_license_index_rejects_unsafe_projection_schema(monkeypatch):
    monkeypatch.setattr(
        florida.ProviderProfileProjection.__table__,
        "schema",
        "unsafe-schema",
    )
    with pytest.raises(RuntimeError, match="provider_profile_schema_invalid"):
        await florida._load_florida_license_index()


def test_fact_adapters_cover_missing_maps_and_generic_fallbacks():
    artifact_by_key = {
        "artifact_id": "artifact",
        "content_sha256": "d" * 64,
        "source_url": "https://example.invalid/source",
    }
    unknown_profile = replace(
        florida.FLORIDA_SOURCES["education"],
        key="unknown_profile_source",
    )
    with pytest.raises(RuntimeError, match="source_adapter_missing"):
        florida._mapped_profile_data_fact(
            unknown_profile,
            {},
            run_id="run",
            record_id="record",
            npi=1000000004,
            artifact=artifact_by_key,
        )
    mapped = florida._mapped_profile_data_fact(
        florida.FLORIDA_SOURCES["education"],
        {},
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    assert mapped["value_json"] == {}

    generic_source = replace(
        florida.FLORIDA_SOURCES["education"],
        key="generic_state_fact",
        path="/Other",
        category="professional_experience",
        fact_type="state_reported_fact",
    )
    facts = florida._facts_for_row(
        generic_source,
        {"description": "Readable fact"},
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    assert facts[0]["category"] == "professional_experience"


def test_indicator_and_financial_fact_edge_states_are_human_readable():
    artifact_by_key = {
        "artifact_id": "artifact",
        "content_sha256": "d" * 64,
        "source_url": "https://example.invalid/source",
    }
    indicator_facts = florida._profile_indicator_facts(
        florida.FLORIDA_SOURCES["profile_indicators"],
        {
            "health_degree": "UNKNOWN",
            "criminal_offense": "N",
            "medicaid_prgrm": "",
        },
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    assert indicator_facts[0]["display"] == (
        "Profile information coverage reported"
    )
    assert indicator_facts[1]["sensitive"] is True
    restricted_only = florida._profile_indicator_facts(
        florida.FLORIDA_SOURCES["profile_indicators"],
        {"criminal_offense": "Y"},
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    assert len(restricted_only) == 1
    assert restricted_only[0]["category"] == "criminal_disclosures"

    assert florida._financial_responsibility_facts(
        florida.FLORIDA_SOURCES["financial_responsibility"],
        {},
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    ) == []

    financial = florida._financial_responsibility_facts(
        florida.FLORIDA_SOURCES["financial_responsibility"],
        {
            "financial_exempt": "N",
            "insured": "N",
            "insured_10_yr": "Y",
            "liability_claim": "Y",
        },
        run_id="run",
        record_id="record",
        npi=1000000004,
        artifact=artifact_by_key,
    )
    assert financial[0]["display"].startswith(
        florida.FLORIDA_SOURCES["financial_responsibility"].title
    )
    assert financial[1]["public_default"] is False
