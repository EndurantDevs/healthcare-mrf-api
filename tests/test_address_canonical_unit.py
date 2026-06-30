# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import importlib.util
import inspect
import json
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from db.models import EntityAddressUnified, MRFAddress
from process.ext.address_pub28 import (
    PUB28_DIRECTIONAL_MAP,
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
)


address_canon = importlib.import_module("process.ext.address_canon")
entity_address_unified = importlib.import_module("process.entity_address_unified")
utils = importlib.import_module("process.ext.utils")
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "alembic" / "versions"


def _golden_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    return list(payload["explicit_cases"])


def _load_migration(filename: str):
    path = MIGRATIONS_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_suite_variants_share_one_address_key():
    variants = [
        ("123 MAIN STREET", "SUITE 200", "Miami", "FL", "33156-2814", "US"),
        ("123 Main St.", "Ste 200", "Miami", "Florida", "33156", ""),
        ("123 MAIN ST STE 200", "", "MIAMI", "FL", "33156", "USA"),
        ("123 Main St", "#200", "Miami", "FL", "33156", "United States"),
    ]

    identities = {address_canon.identity_key_v1(*variant) for variant in variants}
    keys = {address_canon.address_key_v1(*variant) for variant in variants}

    assert identities == {"v2|123mainst|ste200||FL|33156|US|street"}
    assert len(keys) == 1


def test_address_canonical_current_rekey_migration_uses_single_current_format():
    migration = _load_migration("20260618100000_address_canonical_current_rekey.py")

    candidate_sql = migration._create_rekey_candidates_sql("mrf")
    map_sql = migration._create_rewrite_map_sql()
    archive_sql = migration._create_rekeyed_archive_sql()
    checksum_sql = migration._create_rekeyed_checksums_sql("mrf")

    combined = "\n".join([candidate_sql, map_sql, archive_sql, checksum_sql])
    assert "'v2|'" in combined
    assert "2::smallint AS identity_version" in combined
    assert "address_archive_rewrite_map" in combined
    assert "city_norm" in candidate_sql
    assert "city_norm" in archive_sql
    assert "pg_temp.address_archive_rekey_candidates" in candidate_sql
    assert "{checksum_map}" not in combined
    assert "'v1|'" not in combined
    source = inspect.getsource(migration._rekey_source_tables)
    assert "archive_identity_version = 'v2'" in source
    assert "entity_address_evidence" in migration._foundation_module().ADDRESS_KEY_TABLES
    defaults_source = inspect.getsource(migration._set_current_archive_identity_defaults)
    assert "ALTER COLUMN archive_identity_version SET DEFAULT 'v2'" in defaults_source


def test_sql_street_alias_helpers_keep_original_next_token_context():
    migration = _load_migration("20260611100000_address_canonical_foundation.py")
    sql = migration._create_functions_sql("mrf")
    suffixless_sql = sql.split("CREATE OR REPLACE FUNCTION \"mrf\".addr_street_suffixless_norm_v1", 1)[1].split(
        "CREATE OR REPLACE FUNCTION \"mrf\".addr_street_direction_index_v1",
        1,
    )[0]
    completion_sql = sql.split("CREATE OR REPLACE FUNCTION \"mrf\".addr_street_completion_norm_v1", 1)[1].split(
        "CREATE OR REPLACE FUNCTION \"mrf\".addr_city_norm_v1",
        1,
    )[0]

    assert "lead(token) OVER (ORDER BY ord) AS next_token" in suffixless_sql
    assert "SELECT token, ord, next_token\n          FROM counted" in suffixless_sql
    assert "lead(parts.token) OVER (ORDER BY parts.ord) AS next_token" in completion_sql
    assert "SELECT token, ord, next_token\n          FROM marked" in completion_sql


def test_resolve_materialization_carries_source_ctid_for_resolve_aliases():
    ddl = address_canon._keyed_temp_table_ddl("address_archive_resolve_keyed")
    raw_copy_sql = address_canon._keyed_raw_copy_sql(
        staging="mrf.test_stage",
        first="first_line",
        second="second_line",
        city="city_name",
        state="state_name",
        zip_code="postal_code",
        country="country_code",
    )
    alias_sql = address_canon._completion_alias_sql(
        schema="mrf",
        keyed_table="address_archive_resolve_keyed",
        archive="mrf.address_archive_v2",
    )
    zip_alias_sql = address_canon._zip_alias_sql(
        keyed_table="address_archive_resolve_keyed",
        archive="mrf.address_archive_v2",
    )

    assert "source_ctid" in address_canon.KEYED_COPY_COLUMNS
    assert "source_ctid text" in ddl
    assert "ctid::text AS source_ctid" in raw_copy_sql
    assert "address_completion_aliases" in alias_sql
    assert "addr_street_completion_norm_v1" in alias_sql
    assert "source_keys AS MATERIALIZED" in alias_sql
    assert "archive_key_scope AS MATERIALIZED" in alias_sql
    assert "archive_prefilter AS MATERIALIZED" in alias_sql
    assert "AND (suffix_token IS NULL OR direction_token IS NULL)" in alias_sql
    assert "HAVING count(DISTINCT target_address_key) = 1" in alias_sql
    assert "source_ctid" in alias_sql
    assert "address_zip_aliases" in zip_alias_sql
    assert "zip5 IS NULL" in zip_alias_sql
    assert "city_norm IS NOT NULL" in zip_alias_sql
    assert "JOIN source_keys" in zip_alias_sql
    assert "HAVING count(DISTINCT target_address_key) = 1" in zip_alias_sql
    assert "AND count(DISTINCT target_zip5) = 1" in zip_alias_sql


def test_completion_alias_timeout_detector_matches_asyncpg_message():
    exc = RuntimeError("asyncpg.exceptions.QueryCanceledError: canceling statement due to statement timeout")

    assert address_canon.ADDRESS_COMPLETION_ALIAS_TIMEOUT_ENV == "HLTHPRT_ADDRESS_COMPLETION_ALIAS_TIMEOUT"
    assert address_canon._is_statement_timeout_error(exc)
    assert not address_canon._is_statement_timeout_error(RuntimeError("duplicate key value violates unique constraint"))


def test_unit_is_delivery_point_not_premise_identity():
    ste_200 = ("123 Main Street", "Suite 200", "Miami", "FL", "33156", "US")
    ste_310 = ("123 Main Street", "Suite 310", "Miami", "FL", "33156", "US")

    assert address_canon.address_key_v1(*ste_200) != address_canon.address_key_v1(*ste_310)
    assert address_canon.premise_identity_key_v1(*ste_200) == address_canon.premise_identity_key_v1(*ste_310)


def test_unit_keyword_does_not_steal_street_suffix():
    address = ("4 Odd Unit Road", "Desk near lobby", "Austin", "TX", "78701", "US")

    assert address_canon.unit_norm(address[0], address[1]) == ""
    assert address_canon.street_norm(address[0], address[1]) == "4oddunitrddesknearlobby"


def test_unit_extraction_uses_one_decision_for_street_and_unit():
    assert address_canon.street_norm("100 Florida Ave Fl 2", "") == "100floridaave"
    assert address_canon.street_norm("100 Fleet St Fl 2", "") == "100fleetst"
    assert address_canon.address_key_v1("100 Florida Ave Fl 2", "", "Austin", "TX", "78701", "US") != (
        address_canon.address_key_v1("100 Fleet St Fl 2", "", "Austin", "TX", "78701", "US")
    )

    ste_200_apt_5 = address_canon.identity_key_v1(
        "123 Main St Ste 200",
        "Apt 5",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    ste_300_apt_5 = address_canon.identity_key_v1(
        "123 Main St Ste 300",
        "Apt 5",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert ste_200_apt_5 == "v2|123mainstste200|apt5||TX|78701|US|street"
    assert ste_300_apt_5 == "v2|123mainstste300|apt5||TX|78701|US|street"
    assert address_canon.key_from_identity(ste_200_apt_5) != address_canon.key_from_identity(ste_300_apt_5)

    duplicate_suite = address_canon.identity_key_v1(
        "123 Main St Suite 100",
        "Suite 100",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    line2_suite = address_canon.identity_key_v1(
        "123 Main St",
        "Suite 100",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    inline_suite = address_canon.identity_key_v1(
        "123 Main St Ste 100",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert duplicate_suite == "v2|123mainst|ste100||TX|78701|US|street"
    assert duplicate_suite == line2_suite == inline_suite
    assert address_canon.street_norm("123 Main St Suite 100", "Suite 100") == "123mainst"
    assert address_canon.unit_norm("123 Main St Suite 100", "Suite 100") == "ste100"
    assert (
        address_canon.identity_key_v1("123 Main St 1st Fl", "1st Floor", "Austin", "TX", "78701", "US")
        == "v2|123mainst|fl1||TX|78701|US|street"
    )
    assert address_canon.identity_key_v1(
        "7281 E EARLL DR STE 1 BLDG A",
        "Ste 1 Bldg A",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == address_canon.identity_key_v1(
        "7281 E EARLL DR STE 1 BLDG A",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert address_canon.street_norm("7281 E EARLL DR STE 1 BLDG A", "Ste 1 Bldg A") == "7281eearlldrste1"
    assert address_canon.unit_norm("7281 E EARLL DR STE 1 BLDG A", "Ste 1 Bldg A") == "bldga"
    assert address_canon.identity_key_v1(
        "1623 3rd Ave Ste 201 Ofc 5",
        "Ste 201 Ofc 5",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == address_canon.identity_key_v1(
        "1623 3rd Ave Ste 201 Ofc 5",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert address_canon.street_norm("1623 3rd Ave Ste 201 Ofc 5", "Ste 201 Ofc 5") == "16233aveste201"
    assert address_canon.unit_norm("1623 3rd Ave Ste 201 Ofc 5", "Ste 201 Ofc 5") == "ofc5"

    assert address_canon.identity_key_v1(
        "123 Main St Ste 200",
        "Suite",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == "v2|123mainstste200suite|||TX|78701|US|street"
    assert address_canon.identity_key_v1(
        "123 Main St Ste 200",
        "West Wing",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == address_canon.identity_key_v1(
        "123 Main St Ste 200 West Wing",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert address_canon.identity_key_v1(
        "123 Main St Ste 200.",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == address_canon.identity_key_v1(
        "123 Main St Ste 200",
        "",
        "Austin",
        "TX",
        "78701",
        "US",
    )
    assert (
        address_canon.identity_key_v1("27 Dr Mellichamp Dr\xa0Ste 100", "", "BLUFFTON", "SC", "29910", "US")
        == "v2|27drmellichampdr|ste100||SC|29910|US|street"
    )
    assert (
        str(address_canon.address_key_v1("27 Dr Mellichamp Dr\xa0Ste 100", "", "BLUFFTON", "SC", "29910", "US"))
        == "3e3ea29f-8c26-17ba-dcc8-74424e66fd32"
    )


def test_unit_extraction_handles_high_confidence_free_text_variants():
    assert address_canon.unit_norm("123 Main St", "Suite E") == "stee"
    assert address_canon.street_norm("123 Main St", "Suite E") == "123mainst"
    assert address_canon.unit_norm("123 Main St STE W", "") == "stew"
    assert address_canon.street_norm("123 Main St STE W", "") == "123mainst"
    assert address_canon.unit_norm("123 Main St", "Unit N") == "unitn"
    assert address_canon.street_norm("123 Main St", "Unit N") == "123mainst"

    assert address_canon.unit_norm("123 Main St", "Suite 200 A") == "ste200a"
    assert address_canon.street_norm("123 Main St", "Suite 200 A") == "123mainst"
    assert address_canon.unit_norm("123 Main St STE 310 D", "") == "ste310d"
    assert address_canon.street_norm("123 Main St STE 310 D", "") == "123mainst"

    assert address_canon.unit_norm("123 Main St", "2nd Floor") == "fl2"
    assert address_canon.unit_norm("123 Main St", "Second Fl") == "fl2"
    assert address_canon.street_norm("123 Main St", "Second Fl") == "123mainst"
    assert address_canon.unit_norm("123 Main St 1st Fl", "") == "fl1"
    assert address_canon.street_norm("123 Main St 1st Fl", "") == "123mainst"

    assert address_canon.unit_norm("123 Main St", "Suite Road") == ""
    assert address_canon.unit_norm("123 Main St", "Floor Road") == ""
    assert address_canon.unit_norm("123 Main St", "Unit 200 A") == ""


def test_non_us_addresses_are_not_keyed():
    assert address_canon.country_code("canada") == "CANADA"
    assert address_canon.country_code("Usa") == "US"
    assert address_canon.identity_key_v1(
        "1 Burrard Street",
        "",
        "Vancouver",
        "British Columbia",
        "V6B 2W9",
        "canada",
    ) is None
    assert address_canon.address_key_v1(
        "1 Burrard Street",
        "",
        "Vancouver",
        "British Columbia",
        "V6B 2W9",
        "canada",
    ) is None


def test_python_address_canonical_golden_corpus_matches_frozen_expected_values():
    group_keys: dict[str, tuple[str | None, str | None]] = {}
    cases = _golden_cases()
    assert len(cases) >= 270
    for case in cases:
        values = [case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country")]
        identity = address_canon.identity_key_v1(*values)
        address_key = address_canon.address_key_v1(*values)
        premise_identity = address_canon.premise_identity_key_v1(*values)
        premise_key = address_canon.key_from_identity(premise_identity)

        assert identity == case["expected_identity_key"], case["id"]
        assert (str(address_key) if address_key else None) == case["expected_address_key"], case["id"]
        assert premise_identity == case["expected_premise_identity_key"], case["id"]
        assert (str(premise_key) if premise_key else None) == case["expected_premise_key"], case["id"]

        if group := case.get("equivalence_group"):
            current = (case["expected_identity_key"], case["expected_address_key"])
            group_keys.setdefault(group, current)
            assert group_keys[group] == current, case["id"]


def test_address_fast_python_fallback_matches_reference(monkeypatch):
    address_fast = importlib.import_module("process.ext.address_fast")
    monkeypatch.setattr(address_fast, "_FAST_MODULE_CHECKED", True)
    monkeypatch.setattr(address_fast, "_FAST_MODULE", None)

    result = address_fast.canonicalize_batch([
        ("123 Main St", "Suite 200", "Austin", "TX", "78701-1234", "US"),
    ])[0]

    assert result["identity_key"] == address_canon.identity_key_v1(
        "123 Main St",
        "Suite 200",
        "Austin",
        "TX",
        "78701-1234",
        "US",
    )
    assert result["address_key"] == str(address_canon.address_key_v1(
        "123 Main St",
        "Suite 200",
        "Austin",
        "TX",
        "78701-1234",
        "US",
    ))
    assert result["zip4"] == "1234"
    assert result["country_code"] == "US"


def test_rust_canon_version_match_requires_ruleset_version():
    current = address_canon.current_canon_version()

    assert address_canon._canon_version_matches(current)
    assert not address_canon._canon_version_matches(
        {
            **current,
            "ruleset_version": current["ruleset_version"] - 1,
        }
    )


@pytest.mark.asyncio
async def test_rust_materialize_default_on_falls_back_when_binary_missing(monkeypatch):
    monkeypatch.delenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, raising=False)
    monkeypatch.setattr(address_canon, "_ptg2_rust_scanner_binary", lambda: None)

    assert await address_canon._try_materialize_keyed_with_rust(
        None,
        keyed_table='"address_archive_resolve_keyed"',
        keyed_table_name="address_archive_resolve_keyed",
        raw_copy_sql="SELECT 1",
    ) is False


@pytest.mark.asyncio
async def test_rust_materialize_falls_back_on_version_mismatch(monkeypatch, tmp_path):
    async def _not_current(_binary):
        return False

    monkeypatch.delenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, raising=False)
    monkeypatch.setattr(address_canon, "_ptg2_rust_scanner_binary", lambda: tmp_path / "ptg2_scanner")
    monkeypatch.setattr(address_canon, "_rust_canon_version_is_current", _not_current)

    assert await address_canon._try_materialize_keyed_with_rust(
        None,
        keyed_table='"address_archive_resolve_keyed"',
        keyed_table_name="address_archive_resolve_keyed",
        raw_copy_sql="SELECT 1",
    ) is False


def test_equivalence_group_cases_are_mirrored_into_explicit_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    explicit_values = {
        tuple(case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country"))
        for case in payload["explicit_cases"]
    }

    for group in payload["equivalence_groups"]:
        for case in group["cases"]:
            values = tuple(case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country"))
            assert values in explicit_values, f"{group['group']}: {values!r}"


def test_pub28_suffix_directional_and_unit_tables_are_used():
    assert address_canon.street_norm("10 NorthEast Mountain Expressway", "",) == "10nemtnexpy"
    assert address_canon.street_norm("11 Noreste Turnpike", "",) == "11netpke"
    assert address_canon.street_norm("12 County Underpass", "",) == "12countyupas"
    assert address_canon.unit_norm("100 Main Street Basement", "") == "bsmt"
    assert address_canon.unit_norm("100 Main Street", "Penthouse") == "ph"
    assert address_canon.unit_norm("100 Main Street", "Office") == "ofc"
    assert address_canon.unit_norm("100 Main Street", "Suite Road") == ""


def test_pub28_raw_maps_are_single_application_idempotent():
    maps = {
        "suffix": PUB28_STREET_SUFFIX_MAP,
        "directional": PUB28_DIRECTIONAL_MAP,
        "unit": PUB28_UNIT_DESIGNATOR_MAP,
    }

    for map_name, mapping in maps.items():
        for source, mapped in mapping.items():
            assert mapping.get(mapped, mapped) == mapped, f"{map_name}: {source!r} -> {mapped!r}"


def test_ordinal_and_saint_street_tokens_are_canonicalized_safely():
    assert address_canon.street_norm("200 14 St", "",) == address_canon.street_norm("200 14th St", "")
    assert address_canon.street_norm("200 14h St", "",) == address_canon.street_norm("200 14th St", "")
    assert address_canon.street_norm("200 First Ave", "",) == address_canon.street_norm("200 1st Ave", "")
    assert address_canon.street_norm("1200 First Street, NE", "") == address_canon.street_norm("1200 1ST ST NE", "")
    assert address_canon.street_norm("200 Saint Clair Ave", "",) == address_canon.street_norm("200 St Clair Ave", "")

    assert address_canon.street_norm("14H Main St", "") != address_canon.street_norm("14 Main St", "")
    assert address_canon.street_norm("200 First National Way", "") != address_canon.street_norm(
        "200 1 National Way",
        "",
    )


def test_suffixless_street_helpers_only_drop_recognized_trailing_suffixes():
    assert address_canon.street_suffix_token("10 Holcombe Blvd", "") == "blvd"
    assert address_canon.street_suffixless_norm("10 Holcombe Blvd", "") == "10holcombe"
    assert address_canon.street_suffix_token("10 Holcombe", "") is None
    assert address_canon.street_suffixless_norm("10 Holcombe", "") == "10holcombe"
    assert address_canon.street_suffixless_norm("200 First Ave", "") == "2001"
    assert address_canon.street_suffixless_norm("14H Main St", "") == "14hmain"


def test_directionless_and_completion_helpers_keep_ambiguous_cases_visible():
    assert address_canon.street_direction_token("10 N Main St", "") == "n"
    assert address_canon.street_directionless_norm("10 N Main St", "") == "10mainst"
    assert address_canon.street_direction_token("10 Main St N", "") == "n"
    assert address_canon.street_directionless_norm("10 Main St N", "") == "10mainst"
    assert address_canon.street_completion_norm("10 N First Ave", "") == "101"
    assert address_canon.street_completion_norm("10 First", "") == "10first"


def test_current_identity_key_drops_city_only_for_street_precision():
    new_york = address_canon.identity_key_v1("100 1st Ave", "", "New York", "NY", "10009", "US")
    new_york_city = address_canon.identity_key_v1("100 First Avenue", "", "New York City", "NY", "10009", "US")
    city_zip = address_canon.identity_key_v1("", "", "New York", "NY", "10009", "US")

    assert new_york == "v2|1001ave|||NY|10009|US|street"
    assert new_york_city == new_york
    assert city_zip == "v2|||newyork|NY|10009|US|city_zip"
    assert address_canon.address_key_v1("100 Saint Clair Ave", "", "Cleveland", "OH", "44114", "US") == (
        address_canon.address_key_v1("100 St Clair Ave", "", "Cleveland Heights", "OH", "44114", "US")
    )
    assert address_canon.address_key_v1("1200 First Street, NE", "", "Washington", "DC", "20002", "US") == (
        address_canon.address_key_v1("1200 1ST ST NE", "", "Washington", "DC", "20002-3361", "US")
    )


def test_pub28_state_possession_and_military_codes_are_used():
    assert address_canon.state_code("Puerto Rico") == "PR"
    assert address_canon.state_code("Northern Mariana Islands") == "MP"
    assert address_canon.state_code("Armed Forces Pacific") == "AP"
    assert address_canon.state_code("Federated States of Micronesia") == "FM"
    assert address_canon.state_code("calif") is None


def test_zip5_norm_left_pads_short_us_zips():
    assert address_canon.zip5_norm("2138") == "02138"
    assert address_canon.zip5_norm("501") == "00501"
    assert address_canon.zip5_norm("38") is None


def test_foundation_migration_qualifies_postgis_probe():
    migration_sql = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "20260611100000_address_canonical_foundation.py"
    ).read_text()

    assert "to_regtype('public.geography')" in migration_sql
    assert "to_regprocedure('public.st_makepoint(double precision, double precision)')" in migration_sql
    assert "public.Geography(public.ST_MakePoint" in migration_sql


def test_city_zip_precision_does_not_merge_with_street_precision():
    street = address_canon.identity_key_v1("1 Main St", "", "Austin", "TX", "78701", "US")
    city_zip = address_canon.identity_key_v1("", "", "Austin", "TX", "78701", "US")

    assert street == "v2|1mainst|||TX|78701|US|street"
    assert city_zip == "v2|||austin|TX|78701|US|city_zip"
    assert address_canon.key_from_identity(street) != address_canon.key_from_identity(city_zip)


def test_city_zip_precision_preserves_unit_like_lockbox_values():
    dept_1234 = address_canon.identity_key_v1("DEPARTMENT 1234", "", "Knoxville", "TN", "37995", "US")
    dept_5678 = address_canon.identity_key_v1("DEPARTMENT 5678", "", "Knoxville", "TN", "37995", "US")

    assert address_canon.street_norm("DEPARTMENT 1234", "") is None
    assert address_canon.unit_norm("DEPARTMENT 1234", "") == "dept1234"
    assert dept_1234 == "v2||dept1234|knoxville|TN|37995|US|city_zip"
    assert dept_5678 == "v2||dept5678|knoxville|TN|37995|US|city_zip"
    assert address_canon.key_from_identity(dept_1234) != address_canon.key_from_identity(dept_5678)


def test_unkeyable_rows_return_none():
    assert address_canon.identity_key_v1("1 Main St", "", "Austin", "", "78701", "US") is None
    assert address_canon.identity_key_v1("1 Main St", "", "Austin", "TX", "", "US") is None
    assert address_canon.identity_key_v1("", "", "", "TX", "78701", "US") is None


def test_source_enabled_requires_explicit_allow_list(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)
    assert not address_canon.source_enabled("cms_doctors")

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes,cms_doctors")
    assert address_canon.source_enabled("cms_doctors")
    assert not address_canon.source_enabled("mrf")

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "all")
    assert address_canon.source_enabled("mrf")


@pytest.mark.asyncio
async def test_stamp_address_keys_honors_env_shard_override(monkeypatch):
    calls = []
    events = []

    class _FakeDB:
        async def status(self, sql, **kwargs):
            calls.append((sql, kwargs))
            return 1

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "3")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: events.append(payload))

    stamped = await address_canon.stamp_address_keys(
        "address_stage",
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
        shards=8,
    )

    assert stamped == 3
    assert [kwargs["shard"] for _sql, kwargs in calls] == [0, 1, 2]
    assert {kwargs["shards"] for _sql, kwargs in calls} == {3}
    shard_filter = "mod(abs(hashtext(ctid::text)::bigint), :shards) = :shard"
    assert all(shard_filter in sql for sql, _kwargs in calls)
    assert all("WITH stamped AS" in sql for sql, _kwargs in calls)
    assert all("target.address_key IS DISTINCT FROM stamped.computed_address_key" in sql for sql, _kwargs in calls)
    assert [event["done"] for event in events] == [0, 1, 2, 3]
    assert {event["total"] for event in events} == {3}
    assert events[-1]["pct"] == 100.0


@pytest.mark.asyncio
async def test_stamp_address_keys_can_be_limited_to_null_rows(monkeypatch):
    calls = []

    class _FakeDB:
        async def scalar(self, sql):
            assert "WHERE address_key IS NULL" in sql
            assert "addr_key_v1(" in sql
            return True

        async def status(self, sql, **kwargs):
            calls.append((sql, kwargs))
            return 1

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "1")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **_payload: None)

    stamped = await address_canon.stamp_address_keys(
        "address_stage",
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
        update_existing=False,
    )

    assert stamped == 1
    assert len(calls) == 1
    assert "WHERE address_key IS NULL" in calls[0][0]
    assert "target.address_key IS NULL" in calls[0][0]
    assert "target.address_key IS DISTINCT FROM" not in calls[0][0]


@pytest.mark.asyncio
async def test_stamp_address_keys_skips_null_only_work_when_already_keyed(monkeypatch):
    calls = []
    events = []

    class _FakeDB:
        async def scalar(self, sql):
            calls.append(("scalar", sql))
            assert "WHERE address_key IS NULL" in sql
            assert "addr_key_v1(" in sql
            return False

        async def status(self, sql, **kwargs):
            calls.append(("status", sql, kwargs))
            return 1

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "8")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: events.append(payload))

    stamped = await address_canon.stamp_address_keys(
        "address_stage",
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
        update_existing=False,
    )

    assert stamped == 0
    assert [call[0] for call in calls] == ["scalar"]
    assert events[-1]["message"] == "no keyable address rows need stamping"
    assert events[-1]["done"] == 8
    assert events[-1]["pct"] == 100


@pytest.mark.asyncio
@pytest.mark.parametrize("override", ["0", "-2", "not-an-int"])
async def test_stamp_address_keys_rejects_invalid_env_shard_override(monkeypatch, override):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", override)

    with pytest.raises(ValueError):
        await address_canon.stamp_address_keys(
            "address_stage",
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city_name",
                "state": "state_name",
                "zip": "postal_code",
                "country": "'US'",
            },
            schema="mrf",
        )


@pytest.mark.asyncio
async def test_stamp_address_keys_can_run_shards_concurrently(monkeypatch):
    calls = []
    active = 0
    max_active = 0

    class _FakeDB:
        async def status(self, sql, **kwargs):
            nonlocal active, max_active
            calls.append((sql, kwargs))
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return 1

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "4")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "2")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "10")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **_payload: None)

    stamped = await address_canon.stamp_address_keys(
        "address_stage",
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
    )

    assert stamped == 4
    assert len(calls) == 4
    assert max_active == 2


@pytest.mark.asyncio
async def test_stamp_address_keys_clamps_concurrency_to_db_pool(monkeypatch):
    calls = []
    active = 0
    max_active = 0

    class _FakeDB:
        async def status(self, sql, **kwargs):
            nonlocal active, max_active
            calls.append((sql, kwargs))
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return 1

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "4")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "4")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "2")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **_payload: None)

    stamped = await address_canon.stamp_address_keys(
        "address_stage",
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city_name",
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
    )

    assert stamped == 4
    assert len(calls) == 4
    assert max_active == 2


@pytest.mark.asyncio
async def test_restore_missing_zip_from_tiger_zcta_uses_unique_state_checked_point_match(monkeypatch):
    statements = []
    progress = []
    active = 0
    max_active = 0

    class _FakeResult:
        def __init__(self, value=None):
            self._value = value

        def scalar(self):
            return self._value

    class _FakeSession:
        async def execute(self, sql, params=None):
            sql_text = str(sql)
            if "to_regclass" in sql_text:
                return _FakeResult(True)
            if "information_schema.columns" in sql_text:
                column = (params or {}).get("column")
                return _FakeResult(column in {"postal_code", "lat", "long", "address_key"})
            return _FakeResult()

    class _FakeTransaction:
        async def __aenter__(self):
            return _FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeDB:
        def transaction(self):
            return _FakeTransaction()

        async def status(self, sql, **kwargs):
            nonlocal active, max_active
            statements.append((sql, kwargs))
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return 2

    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress.append(payload))

    restored = await address_canon.restore_missing_zip_from_tiger_zcta(
        "address_stage",
        {
            "state": "state_name",
            "zip": "postal_code",
            "country": "'US'",
        },
        schema="mrf",
        shards=4,
        concurrency=2,
    )

    assert restored == 8
    assert len(statements) == 4
    assert max_active == 2
    sql = statements[0][0]
    assert 'UPDATE "mrf"."address_stage" AS target' in sql
    assert 'SET "postal_code" = unique_matches.zip5' in sql
    assert "ST_Covers(" in sql
    assert "JOIN tiger.zcta5 AS z" in sql
    assert 'JOIN "mrf".geo_zip_lookup AS zip_lookup' in sql
    assert "zip_lookup.state = normalized.state_code" in sql
    assert "HAVING count(DISTINCT zip5) = 1" in sql
    assert "s.address_key IS NULL" in sql
    assert "target.address_key IS NULL" in sql
    assert sorted(kwargs["shard"] for _sql, kwargs in statements) == [0, 1, 2, 3]
    assert progress[-1]["phase"] == "address ZIP restore"
    assert progress[-1]["pct"] == 100.0


@pytest.mark.asyncio
async def test_restore_missing_zip_skips_unsafe_zip_expression(monkeypatch):
    calls = []

    class _FakeDB:
        def transaction(self):
            calls.append("transaction")
            raise AssertionError("unsafe ZIP expression should skip before DB access")

    monkeypatch.setattr(address_canon, "db", _FakeDB())

    restored = await address_canon.restore_missing_zip_from_tiger_zcta(
        "address_stage",
        {
            "state": "state_name",
            "zip": "NULL",
            "country": "'US'",
        },
        schema="mrf",
    )

    assert restored == 0
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("override", ["0", "-2", "not-an-int"])
async def test_stamp_address_keys_rejects_invalid_env_concurrency(monkeypatch, override):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", override)

    with pytest.raises(ValueError):
        await address_canon.stamp_address_keys(
            "address_stage",
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city_name",
                "state": "state_name",
                "zip": "postal_code",
                "country": "'US'",
            },
            schema="mrf",
        )


@pytest.mark.asyncio
async def test_propagate_child_address_keys_uses_parent_join_and_concurrency(monkeypatch):
    calls = []
    active = 0
    max_active = 0
    events = []

    class _FakeResult:
        def __init__(self, value=None):
            self._value = value

        def scalar(self):
            return self._value

    class _FakeSession:
        async def execute(self, sql, params=None):
            nonlocal active, max_active
            sql_text = str(sql)
            calls.append((sql_text, params or {}))
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            if "WITH cleared AS" in sql_text or "WITH propagated AS" in sql_text:
                return _FakeResult(2)
            return _FakeResult()

    class _FakeTransaction:
        async def __aenter__(self):
            return _FakeSession()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class _FakeDB:
        def transaction(self):
            return _FakeTransaction()

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "4")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "2")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: events.append(payload))

    propagated = await address_canon.propagate_child_address_keys(
        "mrf_address_evidence_20260612",
        "mrf_address_20260612",
        schema="mrf",
    )

    assert propagated == 16
    assert len(calls) == 20
    assert max_active == 2
    create_sql = next(sql for sql, _kwargs in calls if "CREATE TEMP TABLE" in sql)
    clear_sql = next(sql for sql, _kwargs in calls if "WITH cleared AS" in sql)
    propagate_sql = next(sql for sql, _kwargs in calls if "WITH propagated AS" in sql)
    assert '"address_key_propagation_pending"' in create_sql
    assert 'FROM "mrf"."mrf_address_evidence_20260612" AS child' in create_sql
    assert 'JOIN "mrf"."mrf_address_20260612" AS parent' in create_sql
    assert "UNION ALL" in create_sql
    assert "AND NOT (" in create_sql
    assert "child.npi = parent.npi" in create_sql
    assert "child.type = parent.type" in create_sql
    assert "child.checksum = parent.checksum" in create_sql
    assert "child.first_line IS NOT DISTINCT FROM parent.first_line" in create_sql
    assert "child.ctid::text" in create_sql
    assert "SET address_key = NULL" in clear_sql
    assert "SET address_key = pending.address_key" in propagate_sql
    assert "child.ctid = pending.child_ctid" in propagate_sql
    assert sorted(kwargs["shard"] for _sql, kwargs in calls if "shard" in kwargs) == [0, 1, 2, 3]
    assert events[-1]["phase"] == "address key propagation"
    assert events[-1]["pct"] == 100.0


@pytest.mark.asyncio
async def test_propagate_child_address_keys_can_skip_when_child_already_keyed(monkeypatch):
    calls = []
    events = []

    class _FakeDB:
        async def scalar(self, sql):
            calls.append(("scalar", sql))
            assert 'FROM "mrf"."mrf_address_evidence_20260612"' in sql
            assert 'JOIN "mrf"."mrf_address_20260612" AS parent' in sql
            assert "WHERE child.address_key IS NULL" in sql
            assert "parent.address_key IS NOT NULL" in sql
            assert "child.first_line IS NOT DISTINCT FROM parent.first_line" in sql
            return False

        def transaction(self):
            raise AssertionError("propagation transaction should not start")

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_SHARDS", "4")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_STAMP_CONCURRENCY", "2")
    monkeypatch.setattr(address_canon, "db", _FakeDB())
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: events.append(payload))

    propagated = await address_canon.propagate_child_address_keys(
        "mrf_address_evidence_20260612",
        "mrf_address_20260612",
        schema="mrf",
        skip_when_child_fully_keyed=True,
    )

    assert propagated == 0
    assert [call[0] for call in calls] == ["scalar"]
    assert events[-1]["message"] == "no child canonical address keys need propagation"
    assert events[-1]["done"] == 4
    assert events[-1]["pct"] == 100


def test_entity_address_unified_defaults_to_production_sized_publish_gate():
    assert entity_address_unified.DEFAULT_MIN_ROWS == 1_000_000
    assert entity_address_unified.DEFAULT_COMPACT_SOURCE_RECORD_IDS is True


def test_entity_address_unified_inline_source_evidence_guard_defaults_off(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE", raising=False)
    assert entity_address_unified._require_inline_source_evidence() is False  # pylint: disable=protected-access

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REQUIRE_INLINE_SOURCE_EVIDENCE", "true")
    assert entity_address_unified._require_inline_source_evidence() is True  # pylint: disable=protected-access


def test_entity_address_unified_publish_decision_defaults_to_stage_only_for_test(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH", raising=False)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH", raising=False)

    assert entity_address_unified._publish_requested({}, test_mode=True) is False  # pylint: disable=protected-access
    assert entity_address_unified._publish_requested({}, test_mode=False) is True  # pylint: disable=protected-access
    assert entity_address_unified._publish_requested(  # pylint: disable=protected-access
        {"publish": True},
        test_mode=True,
    ) is True
    assert entity_address_unified._publish_requested(  # pylint: disable=protected-access
        {"skip_publish": True},
        test_mode=False,
    ) is False


def test_entity_address_unified_publish_decision_allows_env_override(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH", "true")
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH", raising=False)

    assert entity_address_unified._publish_requested({}, test_mode=True) is True  # pylint: disable=protected-access

    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PUBLISH", raising=False)
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SKIP_PUBLISH", "true")

    assert entity_address_unified._publish_requested({}, test_mode=False) is False  # pylint: disable=protected-access


def test_entity_address_unified_refresh_mode_aliases_obsolete_ptg_to_full(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE", raising=False)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PTG_SOURCE_KEY", raising=False)

    assert entity_address_unified._entity_address_refresh_mode({}) == "full"
    assert entity_address_unified._entity_address_refresh_mode({"refresh_mode": "ptg-partial"}) == "full"
    assert entity_address_unified._entity_address_refresh_mode({"mode": "partial-ptg"}) == "full"
    assert (
        entity_address_unified._entity_address_refresh_mode(
            {"refresh_mode": "provider-directory-partial"}
        )
        == "provider-directory-partial"
    )
    assert (
        entity_address_unified._entity_address_refresh_mode({"mode": "provider-directory-fhir"})
        == "provider-directory-partial"
    )

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_REFRESH_MODE", "ptg-source-refresh")
    assert entity_address_unified._entity_address_refresh_mode({}) == "full"

    with pytest.raises(ValueError, match="Unsupported entity-address-unified refresh_mode"):
        entity_address_unified._entity_address_refresh_mode({"refresh_mode": "fastish"})


def test_provider_directory_partial_sql_uses_live_and_current_fhir_groups():
    source_selects = entity_address_unified._source_selects(
        "mrf",
        {
            "provider_directory_practitioner": True,
            "provider_directory_organization": True,
            "provider_directory_location": True,
            "provider_directory_location.address_key": True,
            "provider_directory_practitioner_role": True,
            "provider_directory_organization_affiliation": True,
            "npi_address": False,
            "address_archive_v2": False,
        },
    )

    sql = entity_address_unified._prepare_provider_directory_partial_affected_groups_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_pd_groups",
        source_selects,
    )
    filtered = entity_address_unified._provider_directory_partial_source_selects(  # pylint: disable=protected-access
        "mrf",
        ["SELECT * FROM mrf.npi_address AS a", *source_selects],
        affected_group_table="entity_address_unified_pd_groups",
    )

    assert "CREATE UNLOGGED TABLE mrf.entity_address_unified_pd_groups AS" in sql
    assert "@> ARRAY['provider_directory_fhir']::varchar[]" in sql
    assert "COALESCE(live.address_sources" not in sql
    assert "provider_directory_practitioner_role AS role" in sql
    assert "practitioner.npi BETWEEN 1000000000 AND 9999999999" in sql
    assert "provider_directory_organization_affiliation AS affiliation" in sql
    assert "organization.npi BETWEEN 1000000000 AND 9999999999" in sql
    assert "provider_directory_organization AS organization" in sql
    assert "provider_directory_organization_addresses" in sql
    assert "src.entity_id::varchar AS entity_id" in sql
    assert "END AS entity_npi" in sql
    index_sql = entity_address_unified._index_provider_directory_partial_affected_groups_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_pd_groups",
    )
    assert "entity_npi," in index_sql
    assert index_sql.count("CREATE INDEX") == 1
    assert "src.address_key::uuid AS address_key" in sql
    assert filtered[0].startswith("\n    SELECT src.*")
    assert "SELECT DISTINCT entity_npi" in filtered[0]
    assert "affected_npi.entity_npi = a.npi" in filtered[0]
    assert "entity_address_unified_pd_groups AS affected" in filtered[0]
    assert any(
        "provider_directory_practitioner_role AS role" in source_select
        for source_select in filtered
    )


def test_provider_directory_partial_sql_can_scope_live_groups_by_source_id():
    source_selects = entity_address_unified._source_selects(
        "mrf",
        {
            "provider_directory_practitioner": True,
            "provider_directory_organization": True,
            "provider_directory_location": True,
            "provider_directory_location.address_key": True,
            "provider_directory_practitioner_role": True,
            "provider_directory_organization_affiliation": True,
            "npi_address": False,
            "address_archive_v2": False,
        },
        provider_directory_source_ids=["source_a"],
        provider_directory_run_id="run_123",
    )

    sql = entity_address_unified._prepare_provider_directory_partial_affected_groups_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_pd_groups",
        source_selects,
        source_ids=["source_a"],
    )

    assert "provider_directory_practitioner_role AS role" in sql
    assert "role.source_id = ANY(ARRAY['source_a']::varchar[])" in sql
    assert "role.last_seen_run_id = 'run_123'" in sql
    assert "provider_directory_organization_affiliation AS affiliation" in sql
    assert "affiliation.source_id = ANY(ARRAY['source_a']::varchar[])" in sql
    assert "provider_directory_organization AS organization" in sql
    assert "organization.source_id = ANY(ARRAY['source_a']::varchar[])" in sql
    assert "organization.last_seen_run_id = 'run_123'" in sql
    assert "split_part(pd_rid.rid, ':', 3) = ANY(ARRAY['source_a']::varchar[])" in sql


def test_latest_provider_directory_partial_scope_sql_prefers_metadata_then_resource_rows():
    sql = entity_address_unified._latest_provider_directory_partial_scope_sql("mrf")  # pylint: disable=protected-access

    assert "FROM mrf.provider_directory_source" in sql
    assert "last_resource_import" in sql
    assert "'metadata'::varchar AS scope_source" in sql
    assert "location_eligible_source_runs AS" in sql
    assert "organization_row_sources AS" in sql
    assert "resource_row_sources AS" in sql
    assert "FROM mrf.provider_directory_location AS loc" in sql
    assert "FROM mrf.provider_directory_practitioner_role AS role" in sql
    assert "FROM mrf.provider_directory_organization_affiliation AS affiliation" in sql
    assert "FROM mrf.provider_directory_organization AS organization" in sql
    assert "jsonb_array_length(COALESCE(organization.address_json::jsonb, '[]'::jsonb)) > 0" in sql
    assert "JOIN location_eligible_source_runs AS eligible" in sql
    assert "eligible.source_id = loc.source_id" in sql
    assert "eligible.run_id = loc.last_seen_run_id" in sql
    assert "WHERE NOT EXISTS (SELECT 1 FROM completed_sources)" in sql


@pytest.mark.asyncio
async def test_latest_provider_directory_partial_scope_returns_scope_sources(monkeypatch):
    first_mock = AsyncMock(
        return_value={
            "run_id": "run_123",
            "source_ids": ["pdfhir_a", "pdfhir_b"],
            "scope_sources": ["resource_rows"],
        }
    )
    monkeypatch.setattr(
        entity_address_unified,
        "_table_exists",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(entity_address_unified.db, "first", first_mock)

    run_id, source_ids, scope_sources = await entity_address_unified._latest_provider_directory_partial_scope(  # pylint: disable=protected-access
        "mrf"
    )

    assert run_id == "run_123"
    assert source_ids == ["pdfhir_a", "pdfhir_b"]
    assert scope_sources == ["resource_rows"]


def test_entity_address_unified_stage_index_name_is_hash_safe_for_long_names():
    first = entity_address_unified._stage_index_name(
        "provider_location_codex_member_partial_smokefull_member_coverage",
        "npi",
    )
    second = entity_address_unified._stage_index_name(
        "provider_location_codex_member_partial_smokefull_member_coverage",
        "shard_npi",
    )

    assert first != second
    assert len(first) <= 63
    assert len(second) <= 63
    assert first.endswith("_npi")
    assert second.endswith("_shard_npi")


def test_ffs_parent_source_is_not_a_unified_address_source():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "provider_enrollment_ffs": True,
            "provider_enrollment_ffs_address": True,
            "npi": False,
            "npi_address": False,
            "geo_zip_lookup": False,
        },
    )
    child_source = next(s for s in selects if "'provider_enrollment_ffs_address'::varchar AS address_source" in s)

    assert not any("'provider_enrollment_ffs'::varchar AS address_source" in s for s in selects)
    assert "NULL::varchar AS first_line" in child_source
    assert "NULL::varchar AS second_line" in child_source


def test_ffs_address_source_filters_unkeyable_practice_locations():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "provider_enrollment_ffs": True,
            "provider_enrollment_ffs_address": True,
            "npi": False,
            "npi_address": False,
            "geo_zip_lookup": False,
        },
    )

    child_source = next(s for s in selects if "'provider_enrollment_ffs_address'::varchar AS address_source" in s)
    assert "WHERE f.npi IS NOT NULL" in child_source
    assert "NULLIF(BTRIM(COALESCE(fa.state, '')), '') IS NOT NULL" in child_source
    assert "NOT IN ('NULL', 'NONE', 'NA', 'NAN', 'UN', 'UNKNOWN', 'UNSPECIFIED', 'XX', 'ZZ')" in child_source
    assert "NULLIF(LEFT(regexp_replace(COALESCE(fa.zip_code, ''), '[^0-9]', '', 'g'), 5), '') IS NOT NULL" in child_source
    assert "NOT IN ('00000', '99999')" in child_source
    assert "NULLIF(BTRIM(COALESCE(fa.city, '')), '') IS NOT NULL" in child_source
    assert "'provider_enrollment_ffs'::varchar AS address_source" not in child_source


def test_entity_address_unified_registers_mrf_address_source():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "mrf_address": True,
            "npi": False,
            "npi_address": False,
            "geo_zip_lookup": False,
        },
    )

    mrf_source = next(s for s in selects if "'mrf'::varchar AS address_source" in s)
    assert "FROM mrf.mrf_address AS a" in mrf_source
    assert "a.first_line::varchar AS first_line" in mrf_source
    assert "a.postal_code::varchar AS postal_code" in mrf_source
    assert "a.taxonomy_array" not in mrf_source
    assert "ARRAY[0]::int[] AS taxonomy_array" in mrf_source


def test_entity_address_unified_mrf_source_filters_placeholder_addresses():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "mrf_address": True,
            "npi": False,
            "npi_address": False,
            "geo_zip_lookup": False,
        },
    )

    mrf_source = next(s for s in selects if "'mrf'::varchar AS address_source" in s)
    assert "WHERE a.npi IS NOT NULL" in mrf_source
    assert "NULLIF(BTRIM(COALESCE(a.state_name, '')), '') IS NOT NULL" in mrf_source
    assert "NOT IN ('NULL', 'NONE', 'NA', 'NAN', 'UN', 'UNKNOWN', 'UNSPECIFIED', 'XX', 'ZZ')" in mrf_source
    assert "NULLIF(LEFT(regexp_replace(COALESCE(a.postal_code, ''), '[^0-9]', '', 'g'), 5), '') IS NOT NULL" in mrf_source
    assert "NOT IN ('00000', '99999')" in mrf_source
    assert "BTRIM(COALESCE(a.country_code, ''))" in mrf_source
    assert "UNITEDSTATESOFAMERICA" in mrf_source
    assert "NULLIF(BTRIM(COALESCE(a.first_line, '')), '') IS NOT NULL" in mrf_source
    assert "OR NULLIF(BTRIM(COALESCE(a.city_name, '')), '') IS NOT NULL" in mrf_source


def test_entity_address_unified_mrf_source_borrows_arrays_from_primary_npi_address():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "mrf_address": True,
            "npi": False,
            "npi_address": True,
            "geo_zip_lookup": False,
        },
    )

    mrf_source = next(s for s in selects if "'mrf'::varchar AS address_source" in s)
    assert "FROM mrf.mrf_address AS a" in mrf_source
    assert "FROM mrf.npi_address AS pa WHERE pa.npi = a.npi AND pa.type = 'primary'" in mrf_source
    assert "COALESCE(pa.taxonomy_array, ARRAY[0]::int[])::int[] AS taxonomy_array" in mrf_source
    assert "COALESCE(a.taxonomy_array" not in mrf_source
    assert "a.address_key::uuid AS address_key" in mrf_source


def test_entity_address_unified_nppes_source_restores_missing_zip_from_archive():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "npi": True,
            "npi_address": True,
            "address_archive_v2": True,
        },
    )

    nppes_source = next(s for s in selects if "'nppes'::varchar AS address_source" in s)
    assert "LEFT JOIN LATERAL" in nppes_source
    assert "AS nppes_zip_restore ON TRUE" in nppes_source
    assert "mrf.addr_zip5_norm_v1(a.postal_code) IS NULL" in nppes_source
    assert "aa.line1_norm = mrf.addr_street_norm_v1(a.first_line, a.second_line)" in nppes_source
    assert "bit_count(bit_or(COALESCE(aa.source_bits, 0))::bit(64))::int AS source_ref_count" in nppes_source
    assert "candidate_count = 1" in nppes_source
    assert "source_ref_count > COALESCE(next_source_ref_count, -1)" in nppes_source
    assert "LEFT(zip5, 3) = LEFT(next_zip5, 3)" in nppes_source
    assert "OR ABS(zip5::int - next_zip5::int) <= 10" in nppes_source
    assert "THEN nppes_zip_restore.restored_zip5 ELSE a.postal_code END::varchar AS postal_code" in nppes_source


def test_entity_address_unified_nppes_source_skips_zip_restore_without_archive():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "npi": True,
            "npi_address": True,
            "address_archive_v2": False,
        },
    )

    nppes_source = next(s for s in selects if "'nppes'::varchar AS address_source" in s)
    assert "a.postal_code::varchar AS postal_code" in nppes_source
    assert "nppes_zip_restore" not in nppes_source


def test_entity_address_unified_can_range_shard_large_source_selects():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "npi_address": True,
            "mrf_address": True,
            "doctor_clinician_address": True,
            "provider_enrollment_ffs": True,
            "provider_enrollment_ffs_address": True,
            "npi": False,
            "geo_zip_lookup": False,
        },
    )

    sharded = entity_address_unified._shard_source_selects(
        "mrf",
        selects,
        npi_address_ranges=[(100, 200), (200, 300)],
        mrf_address_ranges=[(300, 400), (400, 500)],
        doctor_clinician_address_ranges=[(500, 600), (600, 700)],
        provider_enrollment_ffs_ranges=[(700, 800), (800, 900)],
    )

    assert len(sharded) == 8
    assert any("FROM mrf.npi_address AS a" in sql and "AND a.npi >= 100" in sql for sql in sharded)
    assert any("FROM mrf.npi_address AS a" in sql and "AND a.npi < 300" in sql for sql in sharded)
    assert any("FROM mrf.mrf_address AS a" in sql and "AND a.npi >= 300" in sql for sql in sharded)
    assert any("FROM mrf.mrf_address AS a" in sql and "AND a.npi < 500" in sql for sql in sharded)
    assert any("FROM mrf.doctor_clinician_address AS d" in sql and "AND d.npi >= 500" in sql for sql in sharded)
    assert any("FROM mrf.doctor_clinician_address AS d" in sql and "AND d.npi < 700" in sql for sql in sharded)
    assert not any("'provider_enrollment_ffs'::varchar AS address_source" in sql for sql in sharded)
    assert any("FROM mrf.provider_enrollment_ffs_address AS fa" in sql and "AND f.npi >= 700" in sql for sql in sharded)
    assert any("FROM mrf.provider_enrollment_ffs_address AS fa" in sql and "AND f.npi < 900" in sql for sql in sharded)


def test_entity_address_unified_integer_ranges_are_half_open():
    assert entity_address_unified._integer_ranges(10, 19, 3) == [(10, 14), (14, 18), (18, 20)]
    assert entity_address_unified._integer_ranges(None, 19, 3) == []
    assert entity_address_unified._integer_ranges(20, 19, 3) == []


def test_entity_address_unified_sql_carries_address_key(monkeypatch):
    raw_sql = entity_address_unified._prepare_raw_stage_sql("mrf", "entity_address_unified_raw")
    enrich_sql = entity_address_unified._enrich_raw_stage_sql("mrf", "entity_address_unified_raw")
    insert_sql = entity_address_unified._insert_raw_from_source_sql(
        "mrf",
        "entity_address_unified_raw",
        """
        SELECT 'npi'::varchar AS entity_type, '1'::varchar AS entity_id, 1::bigint AS npi,
               NULL::bigint AS inferred_npi, NULL::float8 AS inference_confidence,
               NULL::varchar AS inference_method, NULL::varchar AS entity_name,
               NULL::varchar AS entity_subtype, 'primary'::varchar AS type,
               ARRAY[0]::int[] AS taxonomy_array, ARRAY[0]::int[] AS plans_network_array,
               ARRAY[0]::int[] AS procedures_array, ARRAY[0]::int[] AS medications_array,
               ARRAY[]::varchar[] AS aca_plan_array, ARRAY[]::varchar[] AS aca_network_array,
               ARRAY[]::varchar[] AS ptg_plan_array, ARRAY[]::varchar[] AS ptg_source_array,
               ARRAY[]::varchar[] AS group_plan_array, 'v1'::varchar AS base_address_version,
               '1 Main St'::varchar AS first_line, NULL::varchar AS second_line,
               'Austin'::varchar AS city_name, 'TX'::varchar AS state_name,
               '78701'::varchar AS postal_code, 'US'::varchar AS country_code,
               NULL::varchar AS telephone_number, NULL::varchar AS fax_number,
               NULL::varchar AS formatted_address, NULL::numeric AS lat, NULL::numeric AS long,
               NULL::date AS date_added, NULL::varchar AS place_id, NULL::uuid AS address_key,
               NOW()::timestamp AS updated_at,
               'nppes'::varchar AS address_source, 'nppes:1'::varchar AS source_record_id
        """,
    )
    materialize_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
    )

    assert "address_key uuid" in raw_sql
    assert "location_key varchar(64)" in raw_sql
    assert "archive_identity_version varchar(16)" in raw_sql
    assert "ptg_plan_array varchar[]" in raw_sql
    assert "LEFT JOIN mrf.address_archive_v2 a_direct" in enrich_sql
    assert "LEFT JOIN LATERAL" in enrich_sql
    assert "a_direct.address_key IS NULL" in enrich_sql
    assert "mrf.addr_key_v1(r.first_line, r.second_line, r.city_name, r.state_name, r.postal_code, r.country_code)" in enrich_sql
    assert "FROM mrf.address_archive_v2 aa" in enrich_sql
    assert "ORDER BY candidate.priority" not in enrich_sql
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TRUST_SOURCE_ADDRESS_KEY", "0")
    legacy_enrich_sql = entity_address_unified._enrich_raw_stage_sql("mrf", "entity_address_unified_raw")
    assert "ORDER BY candidate.priority" in legacy_enrich_sql
    assert "SET address_key = k.address_key" in enrich_sql
    assert "premise_key = k.premise_key" in enrich_sql
    assert "formatted_address = COALESCE(k.archive_formatted_address, r.formatted_address)" in enrich_sql
    assert "lat = COALESCE(k.archive_lat, r.lat)" in enrich_sql
    assert "long = COALESCE(k.archive_long, r.long)" in enrich_sql
    assert "place_id = COALESCE(k.archive_place_id, r.place_id)" in enrich_sql
    assert "evidence_shard =" not in enrich_sql
    sharded_enrich_sql = entity_address_unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_raw",
        evidence_shards=24,
    )
    assert "evidence_shard =" in sharded_enrich_sql
    assert "COALESCE(k.entity_type" in sharded_enrich_sql
    assert "% 24) + 24) % 24)::int" in sharded_enrich_sql
    assert "location_key = encode(sha256(convert_to" in enrich_sql
    assert "address_key::uuid AS address_key" in insert_sql
    assert "ptg_plan_array," in insert_sql
    assert "COALESCE(ptg_plan_array, ARRAY[]::varchar[])::varchar[] AS ptg_plan_array" in insert_sql
    assert "address_key," in materialize_sql
    assert "location_key," in materialize_sql
    assert "archive_identity_version," in materialize_sql
    assert "confidence_score," in materialize_sql
    assert "ARRAY_AGG(lat ORDER BY (lat IS NULL), source_priority ASC" in materialize_sql
    assert "ARRAY_AGG(long ORDER BY (long IS NULL), source_priority ASC" in materialize_sql

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", "1")
    flagged_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
    )
    assert "GROUP BY entity_type, entity_id, type, location_key" in flagged_sql
    sharded_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        checksum_modulo=24,
        checksum_remainder=7,
    )
    assert "hashtext(location_key)" in sharded_sql
    assert "% 24 + 24) % 24) = 7" in sharded_sql
    sharded_index_sql = entity_address_unified._raw_aggregate_group_index_sql(
        "mrf",
        "entity_address_unified_raw",
        aggregate_shards=24,
    )
    assert "entity_address_unified_raw_idx_aggregate_shard_group" in sharded_index_sql
    assert "((hashtext(location_key) % 24 + 24) % 24)" in sharded_index_sql
    assert "entity_type, entity_id, type, location_key" in sharded_index_sql
    unsharded_index_sql = entity_address_unified._raw_aggregate_group_index_sql(
        "mrf",
        "entity_address_unified_raw",
        aggregate_shards=1,
    )
    assert "entity_address_unified_raw_idx_group_key" in unsharded_index_sql
    assert "idx_aggregate_shard_group" not in unsharded_index_sql


def test_entity_address_unified_can_inline_source_evidence():
    inline_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        inline_source_evidence=True,
    )

    assert "evidence AS (" in inline_sql
    assert "FROM aggregated AS agg" in inline_sql
    assert "COALESCE(e.evidence_sources, address_sources" in inline_sql
    assert "COALESCE(e.evidence_record_ids, source_record_ids" in inline_sql
    assert "LEFT JOIN evidence e" in inline_sql


def test_entity_address_unified_retains_provider_directory_record_ids_when_aggregation_disabled(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_AGGREGATE_SOURCE_RECORD_IDS", "0")

    raw_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        inline_source_evidence=True,
    )
    direct_sql = entity_address_unified._materialize_sql(
        "mrf",
        "entity_address_unified_stage",
        [
            """
            SELECT 'npi'::varchar AS entity_type, '1'::varchar AS entity_id, 1::bigint AS npi,
                   NULL::bigint AS inferred_npi, NULL::float8 AS inference_confidence,
                   NULL::varchar AS inference_method, NULL::varchar AS entity_name,
                   NULL::varchar AS entity_subtype, 'primary'::varchar AS type,
                   ARRAY[0]::int[] AS taxonomy_array, ARRAY[0]::int[] AS plans_network_array,
                   ARRAY[0]::int[] AS procedures_array, ARRAY[0]::int[] AS medications_array,
                   '1 Main St'::varchar AS first_line, NULL::varchar AS second_line,
                   'Austin'::varchar AS city_name, 'TX'::varchar AS state_name,
                   '78701'::varchar AS postal_code, 'US'::varchar AS country_code,
                   NULL::varchar AS telephone_number, NULL::varchar AS fax_number,
                   NULL::varchar AS formatted_address, NULL::numeric AS lat, NULL::numeric AS long,
                   NULL::date AS date_added, NULL::varchar AS place_id, NULL::uuid AS address_key,
                   NOW()::timestamp AS updated_at,
                   'nppes'::varchar AS address_source, 'nppes:1'::varchar AS source_record_id
            """
        ],
    )

    assert "FILTER (WHERE source_record_id LIKE 'provider_directory_fhir:%')" in raw_sql
    assert "FILTER (WHERE source_record_id LIKE 'provider_directory_fhir:%')" in direct_sql
    assert "ARRAY[]::varchar[])::varchar[] AS source_record_ids" in raw_sql
    assert "ARRAY[]::varchar[])::varchar[] AS source_record_ids" in direct_sql
    assert "ARRAY[]::varchar[] AS source_record_ids" not in raw_sql
    assert "ARRAY[]::varchar[] AS source_record_ids" not in direct_sql
    assert "COALESCE(e.evidence_record_ids, source_record_ids" in raw_sql


def test_entity_address_unified_can_split_array_aggregates(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SPLIT_ARRAY_AGGREGATES", "1")

    split_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
    )
    split_inline_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        inline_source_evidence=True,
    )
    split_sharded_inline_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        checksum_modulo=24,
        checksum_remainder=7,
        inline_source_evidence=True,
    )

    assert "array_aggregates AS (" in split_sql
    assert "CROSS JOIN LATERAL" in split_sql
    assert "array_value.kind = 'ptg_source'" in split_sql
    assert "COALESCE(CARDINALITY(ptg_source_array), 0) > 0" in split_sql
    assert "LEFT JOIN LATERAL unnest(COALESCE(ptg_source_array" not in split_sql
    assert "arr.aggregate_key IS NOT DISTINCT FROM aggregated.location_key" in split_sql
    assert "LEFT JOIN array_aggregates arr" in split_sql
    assert "FROM aggregated\n      LEFT JOIN array_aggregates arr" in split_sql
    assert "LEFT JOIN evidence e" in split_inline_sql
    assert "LEFT JOIN array_aggregates arr" in split_inline_sql
    assert split_inline_sql.rfind("LEFT JOIN evidence e") < split_inline_sql.rfind(
        "LEFT JOIN array_aggregates arr"
    )
    assert "WHERE evidence_shard = 7 AND (" in split_sharded_inline_sql
    assert "COALESCE(CARDINALITY(group_plan_array), 0) > 0" in split_sharded_inline_sql


def test_entity_address_unified_inline_source_evidence_shards_by_evidence_group(monkeypatch):
    sharded_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        checksum_modulo=24,
        checksum_remainder=7,
        inline_source_evidence=True,
    )
    sharded_index_sql = entity_address_unified._raw_aggregate_group_index_sql(
        "mrf",
        "entity_address_unified_raw",
        aggregate_shards=24,
        inline_source_evidence=True,
    )

    assert "WHERE evidence_shard = 7" in sharded_sql
    assert "hashtext(location_key)" not in sharded_sql
    assert "entity_address_unified_raw_idx_evidence_shard_group" in sharded_index_sql
    assert "((evidence_shard), entity_type, entity_id, type, location_key)" in sharded_index_sql
    assert "entity_type, entity_id, type, location_key" in sharded_index_sql

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_RAW_GROUP_INDEX_PROFILE", "shard")
    shard_only_index_sql = entity_address_unified._raw_aggregate_group_index_sql(
        "mrf",
        "entity_address_unified_raw",
        aggregate_shards=24,
        inline_source_evidence=True,
    )
    assert "entity_address_unified_raw_idx_evidence_shard" in shard_only_index_sql
    assert "CREATE INDEX IF NOT EXISTS" in shard_only_index_sql
    assert "ON mrf.entity_address_unified_raw (evidence_shard)" in shard_only_index_sql
    assert "entity_type, entity_id, type, location_key" not in shard_only_index_sql


def test_entity_address_unified_raw_enrichment_can_skip_archive():
    enrich_sql = entity_address_unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_raw",
        archive_available=False,
    )

    assert "JOIN mrf.address_archive_v2" not in enrich_sql
    assert "'v2'::varchar AS archive_identity_version" in enrich_sql
    assert "CASE WHEN r.address_key IS NULL THEN 'unknown' ELSE 'street' END" in enrich_sql


def test_entity_address_unified_raw_enrichment_can_shard_by_checksum():
    enrich_sql = entity_address_unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_raw",
        checksum_min=-100,
        checksum_max=100,
    )

    assert "WHERE r.checksum >= -100 AND r.checksum < 100" in enrich_sql


@pytest.mark.asyncio
async def test_entity_address_unified_sql_phase_uses_scoped_bulk_settings(monkeypatch):
    statements = []
    events = []

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM", "64MB")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAINTENANCE_WORK_MEM", "1GB")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT", "32GB")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_LOCK_TIMEOUT", "15s")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STATEMENT_TIMEOUT", "0")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SYNCHRONOUS_COMMIT", "off")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_JIT", "off")
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MAX_PARALLEL_WORKERS_PER_GATHER", raising=False)

    class FakeConn:
        async def status(self, statement):
            statements.append(statement)
            return 7 if statement == "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;" else None

    class FakeDB:
        @asynccontextmanager
        async def acquire(self):
            yield FakeConn()

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(entity_address_unified, "enqueue_live_progress", lambda **payload: events.append(payload))

    context = {}
    rowcount = await entity_address_unified._run_sql_phase(  # pylint: disable=protected-access
        "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;",
        context=context,
        run_id="run_eau",
        phase="entity-address-unified test phase",
        unit="shards",
        done=0,
        total=1,
        message="test phase",
        emit_start=True,
        emit_done=True,
    )

    assert rowcount == 7
    set_statements = [statement for statement in statements if statement.startswith("SET LOCAL")]
    assert set_statements[:7] == [
        "SET LOCAL work_mem = '64MB';",
        "SET LOCAL maintenance_work_mem = '1GB';",
        "SET LOCAL temp_file_limit = '32GB';",
        "SET LOCAL lock_timeout = '15s';",
        "SET LOCAL statement_timeout = '0';",
        "SET LOCAL synchronous_commit = 'off';",
        "SET LOCAL jit = 'off';",
    ]
    assert statements[-1] == "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;"
    timing = context["phase_timings"]["entity-address-unified test phase"]
    assert timing["count"] == 1
    assert timing["rows"] == 7
    assert timing["first_started_at"] is not None
    assert timing["last_finished_at"] is not None
    assert timing["wall_seconds"] >= 0
    assert events[0]["source"] == "entity-address-unified-sql-progress"
    assert events[-1]["message"].startswith("test phase: 7 row(s),")


@pytest.mark.asyncio
async def test_entity_address_unified_sql_phase_skips_unprivileged_bulk_setting(monkeypatch):
    statements = []

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM", "64MB")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_TEMP_FILE_LIMIT", "32GB")

    class FakeConn:
        async def status(self, statement):
            statements.append(statement)
            if statement == "SET LOCAL temp_file_limit = '32GB';":
                raise RuntimeError('permission denied to set parameter "temp_file_limit"')
            if statement == "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;":
                return 5
            return None

    class FakeDB:
        @asynccontextmanager
        async def acquire(self):
            yield FakeConn()

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    context = {}
    rowcount = await entity_address_unified._run_sql_phase(  # pylint: disable=protected-access
        "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;",
        context=context,
        phase="entity-address-unified test phase",
    )

    assert rowcount == 5
    assert "SET LOCAL temp_file_limit = '32GB';" in statements
    assert any(statement.startswith("ROLLBACK TO SAVEPOINT") for statement in statements)
    assert statements[-1] == "UPDATE mrf.entity_address_unified_raw SET address_key = address_key;"
    assert context["phase_timings"]["entity-address-unified test phase"]["rows"] == 5


@pytest.mark.asyncio
async def test_entity_address_unified_support_stage_records_bulk_phase_timings(monkeypatch):
    statements = []
    events = []

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_WORK_MEM", "32MB")

    class FakeModel:
        __tablename__ = "entity_address_evidence"

    class FakeStage:
        __tablename__ = "entity_address_evidence_20260614"

    class FakeConn:
        async def status(self, statement):
            statements.append(statement)
            if "INSERT INTO mrf.entity_address_evidence_20260614" in statement:
                return 11
            return None

    class FakeDB:
        @asynccontextmanager
        async def acquire(self):
            yield FakeConn()

        async def scalar(self, statement):
            statements.append(statement)
            return 3

    def fake_support_stage_statements(*_args, **_kwargs):
        return [
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "support tables",
                "TRUNCATE TABLE mrf.entity_address_evidence_20260614;",
                parallel=False,
            ),
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "evidence",
                "INSERT INTO mrf.entity_address_evidence_20260614 SELECT 1;",
            ),
        ]

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_support_stage_statements",
        fake_support_stage_statements,
    )
    monkeypatch.setattr(entity_address_unified, "enqueue_live_progress", lambda **payload: events.append(payload))

    context = {}
    counts = await entity_address_unified._populate_support_stage_tables(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_stage",
        {FakeModel: FakeStage},
        source_run_id="run_1",
        node_id="node_a",
        run_id="run_eau",
        context=context,
    )

    assert counts == {"entity_address_evidence": 3}
    assert "SET LOCAL work_mem = '32MB';" in statements
    assert "INSERT INTO mrf.entity_address_evidence_20260614 SELECT 1;" in statements
    assert context["phase_timings"]["entity-address-unified building support tables"]["count"] == 1
    assert context["phase_timings"]["entity-address-unified building evidence"]["count"] == 1
    assert context["phase_timings"]["entity-address-unified building evidence"]["rows"] == 11
    assert events[0]["phase"] == "entity-address-unified building support tables"
    assert events[-1]["phase"] == "entity-address-unified built evidence"


@pytest.mark.asyncio
async def test_entity_address_unified_support_stage_runs_parallel_inserts(monkeypatch):
    active = 0
    max_active = 0
    order = []

    class FakeModel:
        __tablename__ = "entity_address_evidence"

    class FakeStage:
        __tablename__ = "entity_address_evidence_20260614"

    class FakeDB:
        async def status(self, statement):
            nonlocal active, max_active
            if "TRUNCATE TABLE" in statement:
                order.append("truncate")
                return None
            order.append(statement)
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1
            return 1

        async def scalar(self, _statement):
            return 0

    def fake_support_stage_statements(*_args, **_kwargs):
        return [
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "support tables",
                "TRUNCATE TABLE mrf.entity_address_evidence_20260614;",
                parallel=False,
            ),
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "evidence",
                "INSERT INTO mrf.entity_address_evidence_20260614 SELECT 1;",
            ),
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "procedure bridge",
                "INSERT INTO mrf.entity_address_procedure_bridge_20260614 SELECT 1;",
            ),
            entity_address_unified._SupportStageStatement(  # pylint: disable=protected-access
                "medication bridge",
                "INSERT INTO mrf.entity_address_medication_bridge_20260614 SELECT 1;",
            ),
        ]

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CONCURRENCY", "2")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_support_stage_statements",
        fake_support_stage_statements,
    )

    context = {}
    await entity_address_unified._populate_support_stage_tables(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_stage",
        {FakeModel: FakeStage},
        source_run_id="run_1",
        node_id="node_a",
        context=context,
    )

    assert order[0] == "truncate"
    assert max_active == 2
    assert context["support_stage_concurrency"] == 2


@pytest.mark.asyncio
async def test_entity_address_unified_support_stage_indexes_run_parallel(monkeypatch):
    active = 0
    max_active = 0
    events = []

    class FakeModelA:
        __tablename__ = "entity_address_evidence"

    class FakeModelB:
        __tablename__ = "entity_address_procedure_bridge"

    class FakeModelC:
        __tablename__ = "entity_address_medication_bridge"

    class FakeStageA:
        __tablename__ = "entity_address_evidence_20260614"

    class FakeStageB:
        __tablename__ = "entity_address_procedure_bridge_20260614"

    class FakeStageC:
        __tablename__ = "entity_address_medication_bridge_20260614"

    async def fake_create_stage_indexes(_stage_cls, _db_schema, *, context=None):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        if context is not None:
            context.setdefault("indexed", 0)
            context["indexed"] += 1

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY", "2")
    monkeypatch.setattr(
        entity_address_unified,
        "_create_stage_indexes",
        fake_create_stage_indexes,
    )
    monkeypatch.setattr(entity_address_unified, "enqueue_live_progress", lambda **payload: events.append(payload))

    context = {}
    await entity_address_unified._create_support_stage_indexes(  # pylint: disable=protected-access
        {
            FakeModelA: FakeStageA,
            FakeModelB: FakeStageB,
            FakeModelC: FakeStageC,
        },
        "mrf",
        context=context,
        run_id="run_eau",
    )

    assert max_active == 2
    assert context["support_stage_index_concurrency"] == 2
    assert context["indexed"] == 3
    assert events[-1]["done"] == 3


@pytest.mark.asyncio
async def test_entity_address_unified_prepare_support_tables_heap_load_drops_primary(monkeypatch):
    statements = []
    created_tables = []

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

        async def create_table(self, table, checkfirst=True):
            created_tables.append((table, checkfirst))

    class FakeStage:
        __tablename__ = "entity_address_procedure_bridge_20260614"
        __table__ = object()

    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_HEAP_LOAD", raising=False)
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_support_stage_classes",
        lambda _import_date: {entity_address_unified.EntityAddressProcedureBridge: FakeStage},
    )

    stages = await entity_address_unified._prepare_support_stage_tables(  # pylint: disable=protected-access
        "mrf",
        "20260614",
    )

    assert stages == {entity_address_unified.EntityAddressProcedureBridge: FakeStage}
    assert created_tables == [(FakeStage.__table__, True)]
    assert statements[0] == "DROP TABLE IF EXISTS mrf.entity_address_procedure_bridge_20260614;"
    assert "DROP CONSTRAINT" in statements[1]
    assert "entity_address_procedure_bridge_20260614" in statements[1]


@pytest.mark.asyncio
async def test_entity_address_unified_prepare_support_tables_can_keep_primary(monkeypatch):
    statements = []

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

        async def create_table(self, table, checkfirst=True):
            del table, checkfirst

    class FakeStage:
        __tablename__ = "entity_address_procedure_bridge_20260614"
        __table__ = object()

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_HEAP_LOAD", "0")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified,
        "_support_stage_classes",
        lambda _import_date: {entity_address_unified.EntityAddressProcedureBridge: FakeStage},
    )

    await entity_address_unified._prepare_support_stage_tables(  # pylint: disable=protected-access
        "mrf",
        "20260614",
    )

    assert statements == ["DROP TABLE IF EXISTS mrf.entity_address_procedure_bridge_20260614;"]


def test_entity_address_unified_support_primary_key_sql_restores_constraint():
    sql = entity_address_unified._ensure_stage_primary_key_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_procedure_bridge_20260614",
        ["location_key", "npi", "code_system", "code"],
    )

    assert "IF NOT EXISTS" in sql
    assert "target_table_oid" in sql
    assert "resolved_constraint_name" in sql
    assert "orphan_index" in sql
    assert "DROP INDEX IF EXISTS %I.%I" in sql
    assert "ADD CONSTRAINT %I" in sql
    assert "MD5(target_table_oid::text)" in sql
    assert "entity_address_procedure_bridge_20260614_pkey" in sql
    assert "PRIMARY KEY (location_key, npi, code_system, code)" in sql


@pytest.mark.asyncio
async def test_entity_address_unified_support_indexes_restore_primary_before_secondary(monkeypatch):
    order = []

    class FakeModelA:
        __tablename__ = "entity_address_procedure_bridge"

    class FakeStageA:
        __tablename__ = "entity_address_procedure_bridge_20260614"

    class FakeModelB:
        __tablename__ = "entity_address_medication_bridge"

    class FakeStageB:
        __tablename__ = "entity_address_medication_bridge_20260614"

    async def fake_ensure_stage_primary_key(stage_cls, _db_schema, *, context=None):
        del context
        order.append(f"pk:{stage_cls.__tablename__}")

    async def fake_create_stage_indexes(stage_cls, _db_schema, *, context=None):
        del context
        order.append(f"idx:{stage_cls.__tablename__}")

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_INDEX_CONCURRENCY", "1")
    monkeypatch.setattr(
        entity_address_unified,
        "_ensure_stage_primary_key",
        fake_ensure_stage_primary_key,
    )
    monkeypatch.setattr(
        entity_address_unified,
        "_create_stage_indexes",
        fake_create_stage_indexes,
    )

    await entity_address_unified._create_support_stage_indexes(  # pylint: disable=protected-access
        {FakeModelA: FakeStageA, FakeModelB: FakeStageB},
        "mrf",
    )

    assert order == [
        "pk:entity_address_procedure_bridge_20260614",
        "idx:entity_address_procedure_bridge_20260614",
        "pk:entity_address_medication_bridge_20260614",
        "idx:entity_address_medication_bridge_20260614",
    ]


def test_entity_address_unified_facility_candidate_sql_can_shard_targets():
    sql = entity_address_unified._facility_anchor_npi_candidate_sql(  # pylint: disable=protected-access
        "mrf",
        "facility_anchor_npi_candidate_20260614",
        "entity_address_unified_20260614",
        source_run_id="run_1",
        candidate_shards=4,
        candidate_shard=2,
    )

    assert "hashtext(t.location_key) % 4" in sql
    assert "= 2" in sql
    assert "t.entity_type = 'facility_anchor'" in sql


def test_entity_address_unified_bridge_sql_can_shard_by_location_key():
    procedure_sql = entity_address_unified._procedure_bridge_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_procedure_bridge_20260614",
        "entity_address_unified_20260614",
        bridge_shards=4,
        bridge_shard=3,
    )
    medication_sql = entity_address_unified._medication_bridge_sql(  # pylint: disable=protected-access
        "mrf",
        "entity_address_medication_bridge_20260614",
        "entity_address_unified_20260614",
        bridge_shards=2,
        bridge_shard=1,
    )

    assert "hashtext(t.location_key) % 4" in procedure_sql
    assert "= 3" in procedure_sql
    assert "hashtext(t.location_key) % 2" in medication_sql
    assert "= 1" in medication_sql


def test_entity_address_unified_support_statements_shard_facility_candidates(monkeypatch):
    def stage(name):
        return type(f"Stage_{name}", (), {"__tablename__": name})

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_FACILITY_CANDIDATE_SHARDS", "3")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROCEDURE_BRIDGE_SHARDS", "1")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MEDICATION_BRIDGE_SHARDS", "1")

    statements = entity_address_unified._support_stage_statements(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_20260614",
        {
            entity_address_unified.EntityAddressEvidence: stage("entity_address_evidence_20260614"),
            entity_address_unified.EntityAddressPlanBridge: stage(
                "entity_address_plan_bridge_20260614"
            ),
            entity_address_unified.EntityAddressProcedureBridge: stage(
                "entity_address_procedure_bridge_20260614"
            ),
            entity_address_unified.EntityAddressMedicationBridge: stage(
                "entity_address_medication_bridge_20260614"
            ),
            entity_address_unified.FacilityAnchorNPICandidate: stage(
                "facility_anchor_npi_candidate_20260614"
            ),
        },
        source_run_id="run_1",
        node_id=None,
        build_network_bridge=False,
        available={
            "facility_anchor": True,
            "facility_anchor.medicare_ccn": True,
        },
    )

    facility_statements = [
        item for item in statements if item.label.startswith("facility anchor npi candidate")
    ]

    assert [item.label for item in facility_statements] == [
        "facility anchor npi candidate shard 1/3",
        "facility anchor npi candidate shard 2/3",
        "facility anchor npi candidate shard 3/3",
    ]
    assert "hashtext(t.location_key) % 3" in facility_statements[0].statement
    assert "= 0" in facility_statements[0].statement
    assert "= 1" in facility_statements[1].statement
    assert "= 2" in facility_statements[2].statement


def test_entity_address_unified_support_statements_shard_code_bridges(monkeypatch):
    def stage(name):
        return type(f"Stage_{name}", (), {"__tablename__": name})

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_PROCEDURE_BRIDGE_SHARDS", "3")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_MEDICATION_BRIDGE_SHARDS", "2")

    statements = entity_address_unified._support_stage_statements(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_20260614",
        {
            entity_address_unified.EntityAddressEvidence: stage("entity_address_evidence_20260614"),
            entity_address_unified.EntityAddressPlanBridge: stage(
                "entity_address_plan_bridge_20260614"
            ),
            entity_address_unified.EntityAddressProcedureBridge: stage(
                "entity_address_procedure_bridge_20260614"
            ),
            entity_address_unified.EntityAddressMedicationBridge: stage(
                "entity_address_medication_bridge_20260614"
            ),
        },
        source_run_id="run_1",
        node_id=None,
        build_network_bridge=False,
        available={},
    )

    procedure_statements = [item for item in statements if item.label.startswith("procedure bridge")]
    medication_statements = [item for item in statements if item.label.startswith("medication bridge")]

    assert [item.label for item in procedure_statements] == [
        "procedure bridge shard 1/3",
        "procedure bridge shard 2/3",
        "procedure bridge shard 3/3",
    ]
    assert [item.label for item in medication_statements] == [
        "medication bridge shard 1/2",
        "medication bridge shard 2/2",
    ]
    assert "hashtext(t.location_key) % 3" in procedure_statements[0].statement
    assert "= 0" in procedure_statements[0].statement
    assert "= 1" in procedure_statements[1].statement
    assert "= 2" in procedure_statements[2].statement
    assert "hashtext(t.location_key) % 2" in medication_statements[0].statement
    assert "= 0" in medication_statements[0].statement
    assert "= 1" in medication_statements[1].statement


def test_entity_address_unified_support_statements_can_skip_optional_serving_tables(
    monkeypatch,
):
    def stage(name):
        return type(f"Stage_{name}", (), {"__tablename__": name})

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_CODE_BRIDGES", "0")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_BUILD_FACILITY_CANDIDATES", "0")

    statements = entity_address_unified._support_stage_statements(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_20260614",
        {
            entity_address_unified.EntityAddressEvidence: stage("entity_address_evidence_20260614"),
            entity_address_unified.EntityAddressPlanBridge: stage(
                "entity_address_plan_bridge_20260614"
            ),
            entity_address_unified.EntityAddressProcedureBridge: stage(
                "entity_address_procedure_bridge_20260614"
            ),
            entity_address_unified.EntityAddressMedicationBridge: stage(
                "entity_address_medication_bridge_20260614"
            ),
            entity_address_unified.FacilityAnchorNPICandidate: stage(
                "facility_anchor_npi_candidate_20260614"
            ),
        },
        source_run_id="run_1",
        node_id=None,
        build_network_bridge=False,
        available={
            "facility_anchor": True,
            "facility_anchor.medicare_ccn": True,
        },
    )

    labels = [item.label for item in statements]

    assert "evidence" in labels
    assert "plan bridge" in labels
    assert not any(label.startswith("procedure bridge") for label in labels)
    assert not any(label.startswith("medication bridge") for label in labels)
    assert not any(label.startswith("facility anchor npi candidate") for label in labels)


def test_entity_address_unified_evidence_stage_updates_by_location_key():
    evidence_table = entity_address_unified._evidence_stage_table_name("entity_address_unified_stage")
    prepare_sql = entity_address_unified._prepare_multi_source_evidence_table_sql(
        "mrf",
        evidence_table,
        unlogged=False,
    )
    load_sql = entity_address_unified._load_multi_source_evidence_base_sql(
        "mrf",
        "entity_address_unified_stage",
        evidence_table,
        evidence_shards=32,
    )
    insert_sql = entity_address_unified._insert_multi_source_evidence_shard_sql(
        "mrf",
        "entity_address_unified_stage",
        evidence_table,
        evidence_shards=32,
        evidence_shard=7,
    )
    index_sql = entity_address_unified._index_multi_source_evidence_table_sql(
        "mrf",
        evidence_table,
    )
    apply_sql = entity_address_unified._apply_multi_source_evidence_sql(
        "mrf",
        "entity_address_unified_stage",
        evidence_table,
        evidence_shard=7,
    )

    assert f"CREATE TABLE mrf.{evidence_table}" in prepare_sql
    assert "location_key varchar(64) PRIMARY KEY" in prepare_sql
    assert "address_key uuid" in prepare_sql
    assert "street_key varchar" in prepare_sql
    assert "source_record_ids varchar[] NOT NULL" in prepare_sql
    assert f"INSERT INTO mrf.{evidence_table}" in load_sql
    assert "FROM mrf.entity_address_unified_stage" in load_sql
    assert "COALESCE(address_key::text, '')" in load_sql
    assert "t.address_key::uuid AS address_key" in load_sql
    assert "CASE WHEN t.address_key IS NULL THEN regexp_replace(" in load_sql
    assert "COALESCE(NULLIF(t.city_norm, '')" in load_sql
    assert "% 32" in load_sql
    assert "keyed AS MATERIALIZED" in insert_sql
    assert "location_key," in insert_sql
    assert "address_key," in insert_sql
    assert f"FROM mrf.{evidence_table}" in insert_sql
    assert "= 7" in insert_sql
    assert "ARRAY_AGG(DISTINCT src.src ORDER BY src.src)" in insert_sql
    assert "ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid)" in insert_sql
    assert "re.address_key IS NOT DISTINCT FROM se.address_key" in insert_sql
    assert "se.address_key IS NOT DISTINCT FROM k.address_key" in insert_sql
    assert "JOIN mrf.entity_address_unified_stage AS t" not in insert_sql
    assert "UPDATE mrf." in insert_sql
    assert "CREATE INDEX IF NOT EXISTS" in index_sql
    assert f"ON mrf.{evidence_table} (evidence_shard, location_key)" in index_sql
    assert f"FROM mrf.{evidence_table} AS e" in apply_sql
    assert "e.evidence_shard = 7" in apply_sql
    assert "t.location_key = e.location_key" in apply_sql
    assert "t.checksum =" not in apply_sql
    assert "IS DISTINCT FROM e.evidence_sources" in apply_sql
    assert "IS DISTINCT FROM e.evidence_record_ids" in apply_sql
    assert "source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int" in apply_sql
    assert "independent_source_count = COALESCE(CARDINALITY(e.evidence_sources), 0)::int" in apply_sql
    assert "multi_source_confirmed = COALESCE(CARDINALITY(e.evidence_sources), 0) > 1" in apply_sql


def test_entity_address_unified_evidence_base_can_scope_to_affected_groups():
    evidence_table = entity_address_unified._evidence_stage_table_name("entity_address_unified_stage")
    load_sql = entity_address_unified._load_multi_source_evidence_base_sql(
        "mrf",
        "entity_address_unified_stage",
        evidence_table,
        evidence_shards=32,
        affected_group_table="entity_address_unified_stage_affected_groups",
    )

    assert "FROM mrf.entity_address_unified_stage AS t" in load_sql
    assert "FROM mrf.entity_address_unified_stage_affected_groups AS affected" in load_sql
    assert "affected.entity_type = t.entity_type" in load_sql
    assert "affected.entity_id = t.entity_id" in load_sql
    assert "affected.address_key = t.address_key" in load_sql
    assert "affected.street_key IS NOT DISTINCT FROM" in load_sql


def test_entity_address_unified_builds_evidence_and_bridge_stage_sql():
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            raw_table="entity_address_unified_raw",
            build_network_bridge=True,
        )
    )

    assert "TRUNCATE TABLE mrf.entity_address_evidence_20260614" in sql_blob
    assert "INSERT INTO mrf.entity_address_evidence_20260614" in sql_blob
    assert "FROM mrf.entity_address_unified_raw" in sql_blob
    assert "ROW_NUMBER() OVER ()::bigint AS evidence_id" in sql_blob
    assert "ROW_NUMBER() OVER (ORDER BY" not in sql_blob
    assert "'run_1'::varchar AS source_run_id" in sql_blob
    assert "'node_a'::varchar AS node_id" in sql_blob
    assert "INSERT INTO mrf.entity_address_plan_bridge_20260614" in sql_blob
    assert "unnest(COALESCE(t.aca_plan_array, ARRAY[]::varchar[]))" in sql_blob
    assert "INSERT INTO mrf.entity_address_network_bridge_20260614" in sql_blob
    assert "unnest(COALESCE(t.plans_network_array, ARRAY[]::int[]))" in sql_blob
    assert "INSERT INTO mrf.entity_address_procedure_bridge_20260614" in sql_blob
    assert "'HP_PROCEDURE_CODE'::varchar AS code_system" in sql_blob
    assert "INSERT INTO mrf.entity_address_medication_bridge_20260614" in sql_blob
    assert "'HP_RX_CODE'::varchar AS code_system" in sql_blob


def test_entity_address_unified_partial_support_stage_reuses_live_bridge_rows():
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            build_network_bridge=True,
            affected_group_table="entity_address_unified_stage_affected_groups",
        )
    )

    assert "INSERT INTO mrf.entity_address_procedure_bridge_20260614 (location_key, npi, code_system, code)" in sql_blob
    assert "FROM mrf.entity_address_procedure_bridge AS b" in sql_blob
    assert "FROM mrf.entity_address_medication_bridge AS b" in sql_blob
    assert "FROM mrf.entity_address_plan_bridge AS b" in sql_blob
    assert "FROM mrf.entity_address_network_bridge AS b" in sql_blob
    assert "JOIN mrf.entity_address_unified AS live" in sql_blob
    assert "FROM mrf.entity_address_unified_stage_affected_groups AS affected" in sql_blob
    assert "affected.entity_type = live.entity_type" in sql_blob
    assert "affected.address_key = live.address_key" in sql_blob
    assert "affected.entity_type = t.entity_type" in sql_blob
    assert "affected.address_key = t.address_key" in sql_blob
    assert "unnest(COALESCE(t.medications_array, ARRAY[]::int[]))" in sql_blob
    assert "unnest(COALESCE(t.procedures_array, ARRAY[]::int[]))" in sql_blob


def test_entity_address_unified_partial_support_patch_stages_only_affected_rows():
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            build_network_bridge=True,
            available={"facility_anchor": True, "facility_anchor.medicare_ccn": True},
            affected_group_table="entity_address_unified_stage_affected_groups",
            copy_unaffected_bridges=False,
        )
    )

    assert "FROM mrf.entity_address_procedure_bridge AS b" not in sql_blob
    assert "FROM mrf.entity_address_medication_bridge AS b" not in sql_blob
    assert "FROM mrf.entity_address_unified_stage_affected_groups AS affected" in sql_blob
    assert "affected.entity_type = t.entity_type" in sql_blob
    assert "affected.address_key = t.address_key" in sql_blob
    assert "INSERT INTO mrf.entity_address_evidence_20260614" in sql_blob
    assert "FROM mrf.entity_address_unified_stage AS t" in sql_blob
    assert "INSERT INTO mrf.facility_anchor_npi_candidate_20260614" not in sql_blob


def test_entity_address_unified_partial_support_patch_sql_offsets_evidence_ids():
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    statements = entity_address_unified._partial_support_patch_sql(  # pylint: disable=protected-access
        "mrf",
        stage_classes,
        old_entity_table="entity_address_unified_old",
        affected_group_table="entity_address_unified_stage_affected_groups",
        build_network_bridge=True,
    )
    sql_blob = "\n".join(statement for _, statement in statements)

    assert "DELETE FROM mrf.entity_address_evidence AS support" in sql_blob
    assert "DELETE FROM mrf.entity_address_procedure_bridge AS support" in sql_blob
    assert "FROM mrf.entity_address_unified_old AS live" in sql_blob
    assert "live.location_key = support.location_key" in sql_blob
    assert "FROM mrf.entity_address_unified_stage_affected_groups AS affected" in sql_blob
    assert "INSERT INTO mrf.entity_address_evidence (evidence_id" in sql_blob
    assert "SELECT COALESCE(MAX(evidence_id), 0) FROM mrf.entity_address_evidence" in sql_blob
    assert "ROW_NUMBER() OVER ())::bigint AS evidence_id" in sql_blob


def test_entity_address_unified_partial_main_patch_sql_deletes_and_inserts_affected_rows():
    statements = entity_address_unified._partial_main_patch_sql(  # pylint: disable=protected-access
        "mrf",
        live_table="entity_address_unified",
        stage_table="entity_address_unified_20260614",
        affected_group_table="entity_address_unified_20260614_ptg_groups",
    )
    sql_blob = "\n".join(statement for _, statement in statements)

    assert "DELETE FROM mrf.entity_address_unified AS live" in sql_blob
    assert "FROM mrf.entity_address_unified_20260614_ptg_groups AS affected" in sql_blob
    assert "FROM mrf.entity_address_unified_20260614 AS replacement" in sql_blob
    assert "replacement.location_key = live.location_key" in sql_blob
    assert "INSERT INTO mrf.entity_address_unified" in sql_blob
    assert "SELECT DISTINCT ON (entity_type, entity_id, type, checksum) *" in sql_blob
    assert "FROM mrf.entity_address_unified_20260614" in sql_blob
    assert "ON CONFLICT (entity_type, entity_id, type, checksum) DO UPDATE" in sql_blob
    assert "location_key = EXCLUDED.location_key" in sql_blob
    assert "checksum = EXCLUDED.checksum" not in sql_blob


def test_entity_address_unified_builds_facility_anchor_npi_candidate_stage_sql(monkeypatch):
    monkeypatch.setenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", "true")
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            raw_table="entity_address_unified_raw",
            build_network_bridge=True,
            available={
                "facility_anchor": True,
                "provider_enrollment_hospital": True,
                "provider_enrollment_fqhc": True,
                "provider_enrollment_ffs_additional_npi": True,
                "npi": True,
                "npi_address": True,
                "npi_taxonomy": True,
                "nucc_taxonomy": True,
                "npi_other_identifier": True,
            },
        )
    )

    assert "INSERT INTO mrf.facility_anchor_npi_candidate_20260614" in sql_blob
    assert "t.entity_type = 'facility_anchor'" in sql_blob
    assert "t.npi IS NULL" in sql_blob
    assert "t.inferred_npi IS NULL" in sql_blob
    assert "hospital_ccn_match" in sql_blob
    assert "hospital_pecos_additional_npi" in sql_blob
    assert "hospital_ccn_enrollment_additional_npi" in sql_blob
    assert "fqhc_pecos_additional_npi" in sql_blob
    assert "fqhc_ccn_enrollment_additional_npi" in sql_blob
    assert "provider_enrollment_ffs_additional_npi AS a" in sql_blob
    assert "regexp_replace(UPPER(COALESCE(h.ccn, '')), '[^A-Z0-9]', '', 'g')" in sql_blob
    assert "hospital_nppes_address_key" in sql_blob
    assert "fqhc_parent_enrollment_exact_address" in sql_blob
    assert "fqhc_nppes_address_key" in sql_blob
    assert "fqhc_nppes_dba_phone_zip" in sql_blob
    assert "hospital_nppes_dba_zip_state" in sql_blob
    assert "hospital_dba_zip_state" in sql_blob
    assert "do_business_as_text" in sql_blob
    assert "282E00000X" in sql_blob
    assert "286500000X" in sql_blob
    assert "fqhc_clinic_center_address_key" in sql_blob
    assert "fqhc_clinic_center_phone_zip" in sql_blob
    assert "fqhc_parent_nppes_exact_address_primary" in sql_blob
    assert "target.address_key" in sql_blob
    assert "no_candidate_after_inference" in sql_blob
    assert "'run_1'::varchar AS source_run_id" in sql_blob


def test_facility_anchor_npi_candidate_stage_keeps_indexed_nppes_default(monkeypatch):
    monkeypatch.delenv("HLTHPRT_FACILITY_ANCHOR_NPI_CANDIDATE_INCLUDE_NPPES", raising=False)
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            raw_table="entity_address_unified_raw",
            build_network_bridge=True,
            available={
                "facility_anchor": True,
                "provider_enrollment_hospital": True,
                "provider_enrollment_fqhc": True,
                "npi": True,
                "npi_address": True,
                "npi_taxonomy": True,
                "nucc_taxonomy": True,
                "npi_other_identifier": True,
            },
        )
    )

    assert "fqhc_nppes_address_key" in sql_blob
    assert "fqhc_nppes_dba_phone_zip" in sql_blob
    assert "hospital_nppes_dba_zip_state" in sql_blob
    assert "fqhc_clinic_center_address_key" in sql_blob
    assert "fqhc_clinic_center_phone_zip" in sql_blob
    assert "fqhc_parent_nppes_exact_address_primary" not in sql_blob


def test_entity_address_unified_promotes_approved_facility_anchor_candidates_sql():
    sql = entity_address_unified._promote_approved_facility_anchor_npi_candidates_sql("mrf")

    assert "INSERT INTO mrf.facility_anchor_npi_override" in sql
    assert "FROM mrf.facility_anchor_npi_candidate AS c" in sql
    assert "c.review_status = 'approved'" in sql
    assert "c.candidate_npi IS NOT NULL" in sql
    assert "ON CONFLICT (facility_anchor_id, npi) DO UPDATE" in sql
    assert "'facility_anchor_npi_candidate'::varchar AS source" in sql
    assert "'candidate_evidence', c.evidence" in sql


def test_entity_address_unified_can_skip_network_bridge_stage_sql():
    stage_classes = entity_address_unified._support_stage_classes("20260614")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_1",
            node_id="node_a",
            raw_table="entity_address_unified_raw",
            build_network_bridge=False,
        )
    )

    assert "mrf.entity_address_network_bridge_20260614" in sql_blob
    assert "INSERT INTO mrf.entity_address_network_bridge_20260614" not in sql_blob
    assert "unnest(COALESCE(t.plans_network_array, ARRAY[]::int[]))" not in sql_blob
    assert "INSERT INTO mrf.entity_address_plan_bridge_20260614" in sql_blob
    assert "INSERT INTO mrf.entity_address_procedure_bridge_20260614" in sql_blob
    assert "INSERT INTO mrf.entity_address_medication_bridge_20260614" in sql_blob


@pytest.mark.asyncio
async def test_entity_address_unified_compacts_source_record_ids_by_metadata_reset(monkeypatch):
    statements = []

    class FakeDB:
        async def scalar(self, statement):
            statements.append(statement)
            return 42

        async def status(self, statement):
            statements.append(statement)
            return None

    monkeypatch.delenv(
        "HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE",
        raising=False,
    )
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    rows = await entity_address_unified._compact_hot_row_source_record_ids(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_stage",
    )

    assert rows == 42
    joined = "\n".join(statements)
    assert "pg_class" in statements[0]
    assert "DROP COLUMN IF EXISTS source_record_ids_compact" in joined
    assert "ADD COLUMN source_record_ids_compact varchar[] NOT NULL DEFAULT '{}'" in joined
    assert "UPDATE mrf.entity_address_unified_stage" in joined
    assert "provider_directory_fhir:%" in joined
    assert "WHERE address_sources @> ARRAY['provider_directory_fhir']::varchar[]" in joined
    assert "ALTER TABLE mrf.entity_address_unified_stage DROP COLUMN source_record_ids" in joined
    assert "RENAME COLUMN source_record_ids_compact TO source_record_ids" in joined
    assert "ADD COLUMN source_record_ids varchar[] NOT NULL DEFAULT '{}'" not in joined
    assert "entity_address_unified_stage_compact" not in joined


@pytest.mark.asyncio
async def test_entity_address_unified_can_compact_source_record_ids_by_rewrite(monkeypatch):
    statements = []

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)
            if "INSERT INTO mrf.entity_address_unified_stage_compact" in statement:
                return 42
            return None

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_COMPACT_SOURCE_RECORD_IDS_BY_REWRITE", "1")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    rows = await entity_address_unified._compact_hot_row_source_record_ids(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_stage",
    )

    assert rows == 42
    assert statements[0] == "DROP TABLE IF EXISTS mrf.entity_address_unified_stage_compact;"
    assert (
        statements[1]
        == "CREATE TABLE mrf.entity_address_unified_stage_compact "
        "(LIKE mrf.entity_address_unified_stage INCLUDING ALL);"
    )
    insert_sql = statements[2]
    assert "INSERT INTO mrf.entity_address_unified_stage_compact" in insert_sql
    assert "source_record_ids" in insert_sql
    assert "provider_directory_fhir:%" in insert_sql
    assert "ARRAY[]::varchar[] END AS source_record_ids" in insert_sql
    assert "UPDATE mrf.entity_address_unified_stage" not in "\n".join(statements)
    assert statements[3] == "DROP TABLE mrf.entity_address_unified_stage;"
    assert (
        statements[4]
        == "ALTER TABLE mrf.entity_address_unified_stage_compact RENAME TO entity_address_unified_stage;"
    )


@pytest.mark.asyncio
async def test_entity_address_unified_stage_indexes_do_not_duplicate_primary(monkeypatch):
    statements = []

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

    class FakeStage:
        __tablename__ = "entity_address_unified_stage"
        __my_index_elements__ = ["location_key"]
        __my_additional_indexes__ = [
            {"index_elements": ("npi",), "name": "npi"},
        ]

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_stage_indexes(FakeStage, "mrf")

    assert not any("entity_address_unified_stage_idx_primary" in statement for statement in statements)
    assert any("entity_address_unified_stage_idx_npi" in statement for statement in statements)


@pytest.mark.asyncio
async def test_entity_address_unified_skips_support_code_location_indexes_by_default(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

    class FakeStage:
        __tablename__ = "entity_address_medication_bridge_20260614"
        __main_table__ = "entity_address_medication_bridge"
        __my_additional_indexes__ = [
            {"index_elements": ("code_system", "code", "location_key"), "name": "code_location"},
            {"index_elements": ("npi",), "name": "npi"},
        ]

    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", raising=False)
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_stage_indexes(FakeStage, "mrf", context=context)

    assert "entity_address_medication_bridge_20260614.code_location" in context["skipped_stage_indexes"]
    assert not any("idx_code_location" in statement for statement in statements)
    assert any("idx_npi" in statement for statement in statements)


@pytest.mark.asyncio
async def test_entity_address_unified_support_code_location_indexes_can_be_enabled(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

    class FakeStage:
        __tablename__ = "entity_address_procedure_bridge_20260614"
        __main_table__ = "entity_address_procedure_bridge"
        __my_additional_indexes__ = [
            {"index_elements": ("code_system", "code", "location_key"), "name": "code_location"},
        ]

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_SUPPORT_CODE_LOCATION_INDEXES", "1")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_stage_indexes(FakeStage, "mrf", context=context)

    assert context.get("skipped_stage_indexes") in (None, [])
    assert any("idx_code_location" in statement for statement in statements)


@pytest.mark.asyncio
async def test_entity_address_unified_serving_stage_index_profile_skips_debug_indexes(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

    class FakeStage:
        __tablename__ = "entity_address_unified_20260614"
        __main_table__ = "entity_address_unified"
        __my_additional_indexes__ = [
            {"index_elements": ("npi",), "name": "npi"},
            {"index_elements": ("inferred_npi",), "name": "inferred_npi"},
            {"index_elements": ("entity_type", "coalesce(npi, inferred_npi)"), "name": "entity_type_coalesced_npi"},
            {
                "index_elements": (
                    "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
                    "npi",
                ),
                "name": "primary_phone_digits_npi",
                "where": "type='primary'",
            },
            {
                "index_elements": ("telephone_number", "npi"),
                "name": "primary_phone_npi",
                "where": "type='primary' AND telephone_number IS NOT NULL AND telephone_number <> ''",
            },
            {
                "index_elements": (
                    "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
                    "npi",
                ),
                "name": "service_phone_digits_npi",
                "where": (
                    "type IN ('primary', 'secondary', 'practice', 'site') "
                    "AND regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g') <> ''"
                ),
            },
            {
                "index_elements": ("phone_number", "npi"),
                "name": "service_phone_number_npi",
                "where": (
                    "type IN ('primary', 'secondary', 'practice', 'site') "
                    "AND phone_number IS NOT NULL AND phone_number <> ''"
                ),
            },
            {
                "index_elements": ("address_key", "npi"),
                "name": "service_address_key_npi",
                "where": "type IN ('primary', 'secondary', 'practice', 'site') AND address_key IS NOT NULL",
            },
            {"index_elements": ("row_origin",), "name": "row_origin"},
            {"index_elements": ("zip5",), "name": "zip5"},
            {"index_elements": ("ptg_plan_array",), "using": "gin", "name": "ptg_plan_array"},
            {"index_elements": ("procedures_array gin__int_ops",), "using": "gin", "name": "procedures_array"},
            {
                "index_elements": ("Geography(ST_MakePoint((long)::double precision, (lat)::double precision))",),
                "using": "gist",
                "name": "geo_idx",
                "where": "lat IS NOT NULL AND long IS NOT NULL",
            },
            {
                "index_elements": ("lat", "long"),
                "name": "geo_bbox",
                "where": "lat IS NOT NULL AND long IS NOT NULL",
            },
        ]

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", "serving")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_stage_indexes(FakeStage, "mrf", context=context)

    joined = "\n".join(statements)
    assert context["stage_index_profile"] == "serving"
    assert "idx_npi" in joined
    assert "idx_procedures_array" in joined
    assert "idx_geo_bbox" in joined
    assert "idx_primary_phone_npi" in joined
    assert "idx_service_phone_digits_npi" in joined
    assert "idx_service_phone_number_npi" in joined
    assert "idx_service_address_key_npi" in joined
    assert "idx_primary_phone_digits_npi" not in joined
    assert "idx_geo_idx" not in joined
    assert "idx_inferred_npi" not in joined
    assert "idx_entity_type_coalesced_npi" not in joined
    assert "idx_row_origin" not in joined
    assert "idx_zip5" not in joined
    assert "idx_ptg_plan_array" not in joined
    assert "entity_address_unified_20260614.inferred_npi" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.entity_type_coalesced_npi" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.primary_phone_digits_npi" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.geo_idx" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.row_origin" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.zip5" in context["skipped_stage_indexes"]
    assert "entity_address_unified_20260614.ptg_plan_array" in context["skipped_stage_indexes"]


@pytest.mark.asyncio
async def test_entity_address_unified_can_defer_all_additional_stage_indexes(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def status(self, statement):
            statements.append(statement)

    class FakeStage:
        __tablename__ = "entity_address_unified_20260614"
        __main_table__ = "entity_address_unified"
        __my_additional_indexes__ = [
            {"index_elements": ("npi",), "name": "npi"},
            {"index_elements": ("address_key",), "name": "address_key"},
        ]

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", "1")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_stage_indexes(FakeStage, "mrf", context=context)

    assert statements == []
    assert context["stage_index_profile"] == "none"
    assert context["skipped_stage_indexes"] == [
        "entity_address_unified_20260614.npi",
        "entity_address_unified_20260614.address_key",
    ]


@pytest.mark.asyncio
async def test_entity_address_unified_post_publish_serving_indexes_use_live_table(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def scalar(self, _statement):
            return None

        async def execute_ddl(self, statement):
            statements.append(statement)

        async def status(self, statement):
            raise AssertionError(f"post-publish concurrent indexes should use execute_ddl: {statement}")

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "serving")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_post_publish_indexes("mrf", context=context)

    joined = "\n".join(statements)
    assert context["post_publish_index_profile"] == "serving"
    assert context["post_publish_index_concurrently"] is True
    assert "CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_npi" in joined
    assert "ON mrf.entity_address_unified (npi)" in joined
    assert "entity_address_unified_idx_geo_bbox" in joined
    assert "entity_address_unified_idx_primary_phone_npi" in joined
    assert "entity_address_unified_idx_geo_idx" not in joined
    assert "entity_address_unified.geo_idx" in context["post_publish_skipped_indexes"]
    assert "entity_address_unified.zip5" in context["post_publish_skipped_indexes"]
    assert context["post_publish_index_concurrency"] == 1
    assert context["post_publish_index_total"] == len(statements)
    assert context["post_publish_index_completed"] == len(statements)
    assert context["post_publish_index_pending"] is False
    assert len(context["post_publish_index_timings"]) == len(statements)


@pytest.mark.asyncio
async def test_entity_address_unified_post_publish_non_concurrent_indexes_can_fan_out(monkeypatch):
    statements = []
    context = {}

    class FakeDB:
        async def scalar(self, _statement):
            return None

        async def execute_ddl(self, statement):
            raise AssertionError(f"non-concurrent indexes should use status: {statement}")

        async def status(self, statement):
            statements.append(statement)
            return "CREATE INDEX"

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "serving")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENTLY", "false")
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_CONCURRENCY", "6")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    await entity_address_unified._create_post_publish_indexes("mrf", context=context)

    joined = "\n".join(statements)
    assert context["post_publish_index_profile"] == "serving"
    assert context["post_publish_index_concurrently"] is False
    assert context["post_publish_index_concurrency"] == 6
    assert "CREATE INDEX IF NOT EXISTS entity_address_unified_idx_npi" in joined
    assert "CREATE INDEX CONCURRENTLY" not in joined
    assert context["post_publish_index_total"] == len(statements)
    assert context["post_publish_index_completed"] == len(statements)
    assert context["post_publish_index_pending"] is False


@pytest.mark.asyncio
async def test_entity_address_unified_post_publish_drops_invalid_index_before_retry(monkeypatch):
    statements = []
    invalid_checks = []

    class FakeDB:
        async def scalar(self, statement):
            invalid_checks.append(statement)
            return 1 if len(invalid_checks) == 1 else None

        async def execute_ddl(self, statement):
            statements.append(statement)

        async def status(self, statement):
            raise AssertionError(f"post-publish concurrent indexes should use execute_ddl: {statement}")

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", "serving")
    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(
        entity_address_unified.EntityAddressUnified,
        "__my_additional_indexes__",
        [{"index_elements": ("npi",), "name": "npi"}],
    )

    context = {}
    await entity_address_unified._create_post_publish_indexes("mrf", context=context)

    assert statements[0] == "DROP INDEX CONCURRENTLY IF EXISTS mrf.entity_address_unified_idx_npi;"
    assert statements[1].startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS entity_address_unified_idx_npi")
    assert "ix.indisvalid IS FALSE" in invalid_checks[0]
    assert context["post_publish_index_total"] == 1
    assert context["post_publish_index_completed"] == 1
    assert context["post_publish_index_pending"] is False
    assert context["post_publish_index_timings"][0]["index"] == "npi"


@pytest.mark.asyncio
async def test_ensure_entity_address_unified_live_columns_adds_missing_stale_columns(monkeypatch):
    existing_columns = [
        {"column_name": column.name}
        for column in EntityAddressUnified.__table__.columns
        if column.name != "phone_number"
    ]
    all_mock = AsyncMock(return_value=existing_columns)
    status_mock = AsyncMock()
    monkeypatch.setattr(entity_address_unified.db, "all", all_mock)
    monkeypatch.setattr(entity_address_unified.db, "status", status_mock)
    monkeypatch.setattr(
        entity_address_unified,
        "_table_exists",
        AsyncMock(return_value=True),
    )

    await entity_address_unified._ensure_entity_address_unified_live_columns("mrf")  # pylint: disable=protected-access

    all_mock.assert_awaited_once()
    status_mock.assert_awaited_once()
    sql = status_mock.await_args.args[0]
    assert "ALTER TABLE mrf.entity_address_unified ADD COLUMN IF NOT EXISTS phone_number" in sql
    assert "VARCHAR(15)" in sql


@pytest.mark.asyncio
async def test_entity_address_unified_stage_summary_counts_use_single_scan(monkeypatch):
    statements = []

    class FakeDB:
        async def all(self, statement):
            statements.append(statement)
            return [
                {
                    "staged_rows": 10,
                    "npi_rows": 7,
                    "inferred_rows": 2,
                    "multi_source_rows": 3,
                }
            ]

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())

    counts = await entity_address_unified._stage_summary_counts(  # pylint: disable=protected-access
        "mrf",
        "entity_address_unified_20260614",
    )

    assert counts == {
        "staged_rows": 10,
        "npi_rows": 7,
        "inferred_rows": 2,
        "multi_source_rows": 3,
    }
    assert len(statements) == 1
    joined = " ".join(statements[0].split())
    assert "COUNT(*)::bigint AS staged_rows" in joined
    assert "COUNT(*) FILTER (WHERE entity_type = 'npi')::bigint AS npi_rows" in joined
    assert "COUNT(*) FILTER (WHERE inferred_npi IS NOT NULL)::bigint AS inferred_rows" in joined
    assert "COUNT(*) FILTER (WHERE multi_source_confirmed IS TRUE)::bigint AS multi_source_rows" in joined


def test_entity_address_unified_phase_timing_rows_reads_insert_rowcount():
    context = {
        "phase_timings": {
            "entity-address-unified aggregating": {
                "rows": 35553296,
            },
        },
    }

    assert (
        entity_address_unified._phase_timing_rows(  # pylint: disable=protected-access
            context,
            "entity-address-unified aggregating",
        )
        == 35553296
    )
    assert entity_address_unified._phase_timing_rows(context, "missing") == 0  # pylint: disable=protected-access


def test_entity_address_unified_final_summary_counts_can_be_disabled(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_FINAL_SUMMARY_COUNTS", "false")

    assert entity_address_unified._final_summary_counts() is False  # pylint: disable=protected-access


def test_entity_address_unified_keep_raw_stage_is_opt_in(monkeypatch):
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE", raising=False)
    assert entity_address_unified._keep_raw_stage() is False  # pylint: disable=protected-access

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEEP_RAW_STAGE", "true")
    assert entity_address_unified._keep_raw_stage() is True  # pylint: disable=protected-access


def test_entity_address_unified_indexes_cover_primary_serving_queries():
    indexes = {index["name"]: index for index in EntityAddressUnified.__my_additional_indexes__}

    assert indexes["primary_npi"] == {
        "index_elements": ("npi",),
        "name": "primary_npi",
        "where": "type='primary'",
    }
    assert indexes["primary_zip5_npi"] == {
        "index_elements": ("zip5", "npi"),
        "name": "primary_zip5_npi",
        "where": "type='primary'",
    }
    assert indexes["primary_phone_digits_npi"] == {
        "index_elements": (
            "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
            "npi",
        ),
        "name": "primary_phone_digits_npi",
        "where": "type='primary'",
    }
    assert indexes["primary_phone_npi"] == {
        "index_elements": ("telephone_number", "npi"),
        "name": "primary_phone_npi",
        "where": "type='primary' AND telephone_number IS NOT NULL AND telephone_number <> ''",
    }
    assert indexes["service_phone_digits_npi"] == {
        "index_elements": (
            "regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g')",
            "npi",
        ),
        "name": "service_phone_digits_npi",
        "where": (
            "type IN ('primary', 'secondary', 'practice', 'site') "
            "AND regexp_replace(COALESCE(telephone_number, ''), '[^0-9]', '', 'g') <> ''"
        ),
    }
    assert indexes["service_phone_number_npi"] == {
        "index_elements": ("phone_number", "npi"),
        "name": "service_phone_number_npi",
        "where": (
            "type IN ('primary', 'secondary', 'practice', 'site') "
            "AND phone_number IS NOT NULL AND phone_number <> ''"
        ),
    }
    assert indexes["service_address_key_npi"] == {
        "index_elements": ("address_key", "npi"),
        "name": "service_address_key_npi",
        "where": "type IN ('primary', 'secondary', 'practice', 'site') AND address_key IS NOT NULL",
    }
    assert indexes["primary_state_city_npi"] == {
        "index_elements": ("state_name", "city_name", "npi"),
        "name": "primary_state_city_npi",
        "where": "type='primary'",
    }
    assert indexes["taxonomy_plans_network"]["where"] == "type='primary'"
    assert indexes["procedures_array"]["where"] == "type='primary'"
    assert indexes["medications_array"]["where"] == "type='primary'"
    assert indexes["geo_idx"]["where"] == (
        "type IN ('primary', 'secondary', 'practice', 'site') "
        "AND COALESCE(address_precision, '') <> 'city_zip' "
        "AND lat IS NOT NULL AND long IS NOT NULL"
    )
    assert indexes["geo_bbox"] == {
        "index_elements": ("lat", "long"),
        "name": "geo_bbox",
        "where": (
            "type IN ('primary', 'secondary', 'practice', 'site') "
            "AND COALESCE(address_precision, '') <> 'city_zip' "
            "AND lat IS NOT NULL AND long IS NOT NULL"
        ),
    }


def test_entity_address_unified_disables_autovacuum_on_disposable_tables():
    sql = entity_address_unified._disable_autovacuum_sql("mrf", "entity_address_unified_raw")

    assert "ALTER TABLE mrf.entity_address_unified_raw" in sql
    assert "autovacuum_enabled = false" in sql
    assert "toast.autovacuum_enabled = false" in sql


def test_entity_address_unified_can_make_stage_unlogged():
    assert (
        entity_address_unified._set_unlogged_table_sql("mrf", "entity_address_unified_20260627")
        == "ALTER TABLE mrf.entity_address_unified_20260627 SET UNLOGGED;"
    )


def test_entity_address_unified_support_stage_sql_has_stage_fallback_evidence():
    stage_classes = entity_address_unified._support_stage_classes("20260615")
    sql_blob = "\n".join(
        entity_address_unified._support_stage_sql(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            source_run_id="run_2",
            node_id=None,
            raw_table=None,
        )
    )

    assert "INSERT INTO mrf.entity_address_evidence_20260615" in sql_blob
    assert "FROM mrf.entity_address_unified_stage AS t" in sql_blob
    assert "NULL::varchar AS node_id" in sql_blob
    assert "'run_2'::varchar AS source_run_id" in sql_blob


@pytest.mark.asyncio
async def test_push_objects_rewrite_dedupes_mrf_address_on_full_unique_key(monkeypatch):
    captured_chunks = []

    class _Excluded:
        def __getattr__(self, name):
            return f"excluded.{name}"

    class _FakeInsert:
        excluded = _Excluded()

        def values(self, chunk):
            captured_chunks.append(chunk)
            return self

        def on_conflict_do_update(self, **_kwargs):
            return self

        def on_conflict_do_nothing(self, **_kwargs):
            return self

        async def status(self):
            return len(captured_chunks[-1])

    class _FakeDB:
        def insert(self, _table):
            return _FakeInsert()

    monkeypatch.setattr(utils, "db", _FakeDB())

    rows = [
        {"npi": 1, "type": "practice", "checksum": 42, "first_line": "1 Main St"},
        {"npi": 2, "type": "practice", "checksum": 42, "first_line": "1 Main St"},
    ]

    await utils.push_objects(rows, MRFAddress, rewrite=True, use_copy=False)

    assert len(captured_chunks) == 1
    assert len(captured_chunks[0]) == 2
