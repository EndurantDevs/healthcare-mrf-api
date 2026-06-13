# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from db.models import MRFAddress
from process.ext.address_pub28 import (
    PUB28_DIRECTIONAL_MAP,
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
)


address_canon = importlib.import_module("process.ext.address_canon")
entity_address_unified = importlib.import_module("process.entity_address_unified")
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


def test_entity_address_unified_sql_carries_address_key(monkeypatch):
    raw_sql = entity_address_unified._prepare_raw_stage_sql("mrf", "entity_address_unified_raw")
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
    assert "mrf.addr_key_v1(first_line, second_line, city_name, state_name, postal_code, country_code) AS address_key" in insert_sql
    assert "address_key," in materialize_sql

    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_KEY_V2", "1")
    flagged_sql = entity_address_unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_raw",
    )
    assert "GROUP BY entity_type, entity_id, type, COALESCE(address_key::text, checksum::text)" in flagged_sql


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
