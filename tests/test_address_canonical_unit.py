# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import json
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
ptg_address = importlib.import_module("process.ptg_address")
utils = importlib.import_module("process.ext.utils")
FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _golden_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    return list(payload["explicit_cases"])


def test_suite_variants_share_one_address_key():
    variants = [
        ("123 MAIN STREET", "SUITE 200", "Miami", "FL", "33156-2814", "US"),
        ("123 Main St.", "Ste 200", "Miami", "Florida", "33156", ""),
        ("123 MAIN ST STE 200", "", "MIAMI", "FL", "33156", "USA"),
        ("123 Main St", "#200", "Miami", "FL", "33156", "United States"),
    ]

    identities = {address_canon.identity_key_v1(*variant) for variant in variants}
    keys = {address_canon.address_key_v1(*variant) for variant in variants}

    assert identities == {"v1|123mainst|ste200|miami|FL|33156|US|street"}
    assert len(keys) == 1


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
    assert ste_200_apt_5 == "v1|123mainstste200|apt5|austin|TX|78701|US|street"
    assert ste_300_apt_5 == "v1|123mainstste300|apt5|austin|TX|78701|US|street"
    assert address_canon.key_from_identity(ste_200_apt_5) != address_canon.key_from_identity(ste_300_apt_5)

    assert address_canon.identity_key_v1(
        "123 Main St Ste 200",
        "Suite",
        "Austin",
        "TX",
        "78701",
        "US",
    ) == "v1|123mainstste200suite||austin|TX|78701|US|street"
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
        == "v1|27drmellichampdr|ste100|bluffton|SC|29910|US|street"
    )
    assert (
        str(address_canon.address_key_v1("27 Dr Mellichamp Dr\xa0Ste 100", "", "BLUFFTON", "SC", "29910", "US"))
        == "9b601e19-7700-9fae-5f5f-e710eb093400"
    )


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

    assert street == "v1|1mainst||austin|TX|78701|US|street"
    assert city_zip == "v1|||austin|TX|78701|US|city_zip"
    assert address_canon.key_from_identity(street) != address_canon.key_from_identity(city_zip)


def test_city_zip_precision_preserves_unit_like_lockbox_values():
    dept_1234 = address_canon.identity_key_v1("DEPARTMENT 1234", "", "Knoxville", "TN", "37995", "US")
    dept_5678 = address_canon.identity_key_v1("DEPARTMENT 5678", "", "Knoxville", "TN", "37995", "US")

    assert address_canon.street_norm("DEPARTMENT 1234", "") is None
    assert address_canon.unit_norm("DEPARTMENT 1234", "") == "dept1234"
    assert dept_1234 == "v1||dept1234|knoxville|TN|37995|US|city_zip"
    assert dept_5678 == "v1||dept5678|knoxville|TN|37995|US|city_zip"
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


def test_entity_address_unified_defaults_to_production_sized_publish_gate():
    assert entity_address_unified.DEFAULT_MIN_ROWS == 1_000_000


def test_ffs_address_source_does_not_borrow_parent_street_lines():
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
    parent_source = next(s for s in selects if "'provider_enrollment_ffs'::varchar AS address_source" in s)
    child_source = next(s for s in selects if "'provider_enrollment_ffs_address'::varchar AS address_source" in s)

    assert "f.address_line_1::varchar AS first_line" in parent_source
    assert "f.address_line_2::varchar AS second_line" in parent_source
    assert "NULL::varchar AS first_line" in child_source
    assert "NULL::varchar AS second_line" in child_source


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


def test_entity_address_unified_registers_ptg_address_overlay_source():
    selects = entity_address_unified._source_selects(
        "mrf",
        {
            "ptg_address": True,
            "npi": True,
            "npi_address": True,
            "address_archive_v2": True,
        },
    )

    ptg_source = next(s for s in selects if "'ptg'::varchar AS address_source" in s)
    assert "FROM mrf.ptg_address AS p" in ptg_source
    assert "LEFT JOIN mrf.address_archive_v2 AS aa ON aa.address_key = p.address_key" in ptg_source
    assert "FROM mrf.npi_address AS pa WHERE pa.npi = p.npi AND pa.type = 'primary'" in ptg_source
    assert "('ptg:' || p.source_key || ':' || p.snapshot_id || ':' || p.location_key)" in ptg_source


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

    assert len(sharded) == 10
    assert any("FROM mrf.npi_address AS a" in sql and "AND a.npi >= 100" in sql for sql in sharded)
    assert any("FROM mrf.npi_address AS a" in sql and "AND a.npi < 300" in sql for sql in sharded)
    assert any("FROM mrf.mrf_address AS a" in sql and "AND a.npi >= 300" in sql for sql in sharded)
    assert any("FROM mrf.mrf_address AS a" in sql and "AND a.npi < 500" in sql for sql in sharded)
    assert any("FROM mrf.doctor_clinician_address AS d" in sql and "AND d.npi >= 500" in sql for sql in sharded)
    assert any("FROM mrf.doctor_clinician_address AS d" in sql and "AND d.npi < 700" in sql for sql in sharded)
    assert any("FROM mrf.provider_enrollment_ffs AS f" in sql and "AND f.npi >= 700" in sql for sql in sharded)
    assert any("FROM mrf.provider_enrollment_ffs AS f" in sql and "AND f.npi < 900" in sql for sql in sharded)
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
               '1 Main St'::varchar AS first_line, NULL::varchar AS second_line,
               'Austin'::varchar AS city_name, 'TX'::varchar AS state_name,
               '78701'::varchar AS postal_code, 'US'::varchar AS country_code,
               NULL::varchar AS telephone_number, NULL::varchar AS fax_number,
               NULL::varchar AS formatted_address, NULL::numeric AS lat, NULL::numeric AS long,
               NULL::date AS date_added, NULL::varchar AS place_id, NOW()::timestamp AS updated_at,
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
    assert "JOIN mrf.address_archive_v2 a" in enrich_sql
    assert "SET premise_key = k.premise_key" in enrich_sql
    assert "location_key = encode(sha256(convert_to" in enrich_sql
    assert "mrf.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, country_code) AS address_key" in insert_sql
    assert "address_key," in materialize_sql
    assert "location_key," in materialize_sql
    assert "archive_identity_version," in materialize_sql
    assert "confidence_score," in materialize_sql

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", "1")
    flagged_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
    )
    assert "GROUP BY entity_type, entity_id, type, COALESCE(address_key::text, checksum::text)" in flagged_sql
    sharded_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
        checksum_modulo=24,
        checksum_remainder=7,
    )
    assert "hashtext(COALESCE(address_key::text, checksum::text))" in sharded_sql
    assert "% 24 + 24) % 24) = 7" in sharded_sql


def test_entity_address_unified_raw_enrichment_can_skip_archive():
    enrich_sql = entity_address_unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_raw",
        archive_available=False,
    )

    assert "JOIN mrf.address_archive_v2" not in enrich_sql
    assert "'v1'::varchar AS archive_identity_version" in enrich_sql
    assert "CASE WHEN r.address_key IS NULL THEN 'unknown' ELSE 'street' END" in enrich_sql


def test_entity_address_unified_raw_enrichment_can_shard_by_checksum():
    enrich_sql = entity_address_unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_raw",
        checksum_min=-100,
        checksum_max=100,
    )

    assert "WHERE r.checksum >= -100 AND r.checksum < 100" in enrich_sql


def test_entity_address_unified_evidence_stage_updates_by_location_key():
    evidence_table = entity_address_unified._evidence_stage_table_name("entity_address_unified_stage")
    prepare_sql = entity_address_unified._prepare_multi_source_evidence_table_sql(
        "mrf",
        evidence_table,
        unlogged=False,
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
    assert f"INSERT INTO mrf.{evidence_table}" in insert_sql
    assert "keyed AS MATERIALIZED" in insert_sql
    assert "location_key," in insert_sql
    assert "% 32" in insert_sql
    assert "= 7" in insert_sql
    assert "ARRAY_AGG(DISTINCT src.src ORDER BY src.src)" in insert_sql
    assert "ARRAY_AGG(DISTINCT rid.rid ORDER BY rid.rid)" in insert_sql
    assert "JOIN mrf.entity_address_unified_stage AS t" in insert_sql
    assert "ON t.location_key = k.location_key" in insert_sql
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
    assert "'run_1'::varchar AS source_run_id" in sql_blob
    assert "'node_a'::varchar AS node_id" in sql_blob
    assert "INSERT INTO mrf.entity_address_plan_bridge_20260614" in sql_blob
    assert "unnest(COALESCE(t.aca_plan_array, ARRAY[]::varchar[]))" in sql_blob
    assert "INSERT INTO mrf.entity_address_network_bridge_20260614" in sql_blob
    assert "unnest(COALESCE(t.plans_network_array, ARRAY[]::int[]))" in sql_blob
    assert "INSERT INTO mrf.entity_address_ptg_bridge_20260614" in sql_blob
    assert "record_id.value LIKE 'ptg:%'" in sql_blob
    assert "INSERT INTO mrf.entity_address_procedure_bridge_20260614" in sql_blob
    assert "'HP_PROCEDURE_CODE'::varchar AS code_system" in sql_blob
    assert "INSERT INTO mrf.entity_address_medication_bridge_20260614" in sql_blob
    assert "'HP_RX_CODE'::varchar AS code_system" in sql_blob


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
    assert indexes["primary_state_city_npi"] == {
        "index_elements": ("state_name", "city_name", "npi"),
        "name": "primary_state_city_npi",
        "where": "type='primary'",
    }
    assert indexes["geo_idx"]["where"] == (
        "type IN ('primary', 'secondary') AND COALESCE(address_precision, '') <> 'city_zip'"
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


def test_ptg_address_insert_sql_uses_existing_provider_location_projection():
    sql = ptg_address._ptg_address_insert_sql(
        "mrf",
        "ptg_address_stage",
        source_key="payer_a",
        snapshot_id="snap_1",
        node_id=None,
        address_canon_available=True,
        archive_available=True,
    )

    assert "FROM mrf.ptg2_provider_location loc" in sql
    assert "mrf.addr_key_v1(first_line, second_line, city, state, postal_code, 'US')" in sql
    assert "AS source_zip5" in sql
    assert "COALESCE(a.zip5, k.source_zip5) AS zip5" in sql
    assert "LEFT JOIN mrf.address_archive_v2 a" in sql
    assert "NULL::varchar AS node_id" in sql
    assert "'payer_a'::varchar AS source_key" in sql
    assert "ARRAY['payer_a']::varchar[] AS ptg_source_array" in sql
    assert "SELECT DISTINCT ON (encode(sha256(convert_to" in sql
    assert "encode(sha256(convert_to" in sql


def test_entity_address_unified_sql_falls_back_without_canonical_functions():
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
               '1 Main St'::varchar AS first_line, NULL::varchar AS second_line,
               'Austin'::varchar AS city_name, 'TX'::varchar AS state_name,
               '78701'::varchar AS postal_code, 'US'::varchar AS country_code,
               NULL::varchar AS telephone_number, NULL::varchar AS fax_number,
               NULL::varchar AS formatted_address, NULL::numeric AS lat, NULL::numeric AS long,
               NULL::date AS date_added, NULL::varchar AS place_id, NOW()::timestamp AS updated_at,
               'nppes'::varchar AS address_source, 'nppes:1'::varchar AS source_record_id
        """,
        address_canon_available=False,
    )
    direct_sql = entity_address_unified._materialize_sql(
        "mrf",
        "entity_address_unified_stage",
        ["SELECT * FROM placeholder_source"],
        address_canon_available=False,
    )

    assert "NULL::uuid AS address_key" in insert_sql
    assert "NULL::uuid AS address_key" in direct_sql
    assert "addr_key_v1" not in insert_sql
    assert "addr_key_v1" not in direct_sql


def test_entity_address_unified_key_v2_requires_canonical_functions(monkeypatch):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", "1")

    with pytest.raises(RuntimeError, match="requires canonical address SQL functions"):
        entity_address_unified._materialize_sql(
            "mrf",
            "entity_address_unified_stage",
            ["SELECT * FROM placeholder_source"],
            address_canon_available=False,
        )

    with pytest.raises(RuntimeError, match="requires canonical address SQL functions"):
        entity_address_unified._materialize_from_raw_sql(
            "mrf",
            "entity_address_unified_stage",
            "entity_address_unified_raw",
            address_canon_available=False,
        )


@pytest.mark.asyncio
async def test_entity_address_unified_warns_when_canonical_functions_missing(monkeypatch):
    events = []

    async def noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(entity_address_unified, "ensure_database", noop)
    monkeypatch.setattr(entity_address_unified, "_table_exists", AsyncMock(return_value=False))
    monkeypatch.setattr(entity_address_unified, "_address_canon_available", AsyncMock(return_value=False))
    monkeypatch.setattr(entity_address_unified, "enqueue_live_progress", lambda **payload: events.append(payload))

    with pytest.raises(RuntimeError, match="No source tables are available"):
        await entity_address_unified.process_data(
            {
                "import_date": "20260611",
                "context": {
                    "stage_prepared": True,
                    "control_run_id": "run_1",
                    "test_mode": True,
                },
            },
            {},
        )

    assert any(
        event.get("run_id") == "run_1"
        and event.get("status") == "warning"
        and event.get("phase") == "entity-address-unified canonical unavailable"
        and event.get("unit") == "address_key"
        for event in events
    )


def test_entity_address_unified_publish_row_count_guard():
    entity_address_unified._validate_publish_row_count(
        stage_rows=800,
        previous_rows=1000,
        test_mode=False,
        min_rows_required=1,
    )
    entity_address_unified._validate_publish_row_count(
        stage_rows=1,
        previous_rows=1000,
        test_mode=True,
        min_rows_required=1_000_000,
    )

    with pytest.raises(RuntimeError, match="below 80%"):
        entity_address_unified._validate_publish_row_count(
            stage_rows=799,
            previous_rows=1000,
            test_mode=False,
            min_rows_required=1,
        )

    with pytest.raises(RuntimeError, match="below minimum"):
        entity_address_unified._validate_publish_row_count(
            stage_rows=10,
            previous_rows=0,
            test_mode=False,
            min_rows_required=100,
        )


@pytest.mark.asyncio
async def test_entity_address_unified_publish_integrity_checks_archive_and_bridge_tables(monkeypatch):
    statements = []
    stage_classes = entity_address_unified._support_stage_classes("20260614")

    class FakeDB:
        async def scalar(self, statement):
            statements.append(statement)
            return 0

    async def table_exists(_schema, _table):
        return True

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(entity_address_unified, "_table_exists", table_exists)

    metrics = await entity_address_unified._validate_publish_integrity(
        "mrf",
        "entity_address_unified_stage",
        stage_classes,
        test_mode=False,
    )

    assert metrics["null_location_keys"] == 0
    assert metrics["duplicate_location_keys"] == 0
    assert metrics["unresolved_merged_into_rows"] == 0
    assert metrics["archive_identity_mismatch_rows"] == 0
    assert metrics["bridge_orphans"]["entity_address_plan_bridge_20260614"] == 0
    assert any("a.merged_into IS NOT NULL" in statement for statement in statements)
    assert any("COALESCE(archive_identity_version, '') <> 'v1'" in statement for statement in statements)
    assert any(
        "FROM mrf.entity_address_procedure_bridge_20260614 AS b" in statement
        and "WHERE t.location_key = b.location_key" in statement
        for statement in statements
    )


@pytest.mark.asyncio
async def test_entity_address_unified_publish_integrity_fails_on_redirects_and_orphans(monkeypatch):
    stage_classes = entity_address_unified._support_stage_classes("20260614")

    class FakeDB:
        async def scalar(self, statement):
            if "a.merged_into IS NOT NULL" in statement:
                return 2
            if "FROM mrf.entity_address_procedure_bridge_20260614 AS b" in statement:
                return 3
            return 0

    async def table_exists(_schema, _table):
        return True

    monkeypatch.setattr(entity_address_unified, "db", FakeDB())
    monkeypatch.setattr(entity_address_unified, "_table_exists", table_exists)

    with pytest.raises(RuntimeError, match="merged_into redirects.*procedure_bridge"):
        await entity_address_unified._validate_publish_integrity(
            "mrf",
            "entity_address_unified_stage",
            stage_classes,
            test_mode=False,
        )


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
