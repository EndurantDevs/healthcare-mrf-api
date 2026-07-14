# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import os
import importlib
from pathlib import Path

import pytest

from db.models import EntityAddressUnified
from db.models import db
from process.ext import address_canon
from process.ext.utils import make_class
from process.ext.address_canon import migrate_legacy_archive_to_v2, resolve_into_archive, stamp_address_keys


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"
entity_address_unified = importlib.import_module("process.entity_address_unified")
facility_anchors = importlib.import_module("process.facility_anchors")


def _requires_test_database():
    database = os.getenv("HLTHPRT_DB_DATABASE", "")
    if "test" not in database:
        pytest.skip("DB-backed address canonical test requires a disposable test database")


def _golden_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    return list(payload["explicit_cases"])


async def _archive_v2_rows(schema):
    rows = await db.all(
        f"""
        SELECT
            address_key::text AS address_key,
            identity_key,
            premise_key::text AS premise_key,
            line1_norm,
            unit_norm,
            city_norm,
            state_code,
            zip5,
            zip4,
            country_code,
            first_line,
            second_line,
            city_name,
            state_name,
            postal_code,
            source_bits,
            display_priority
        FROM {schema}.address_archive_v2
        ORDER BY address_key;
        """
    )
    return [dict(row._mapping) for row in rows]


def _resolve_stats_projection(stats):
    return {
        "staged": stats.staged,
        "distinct_keys": stats.distinct_keys,
        "inserted": stats.inserted,
        "provenance_updates": stats.provenance_updates,
        "null_key_rows": stats.null_key_rows,
        "eligible_key_rows": stats.eligible_key_rows,
        "eligible_null_key_rows": stats.eligible_null_key_rows,
        "reason_buckets": stats.reason_buckets,
        "gate_violations": stats.gate_violations,
    }


@pytest.mark.asyncio(loop_scope="module")
async def test_checksum_bridge_primary_key_is_checksum_only():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")

    columns = await db.all(
        """
        SELECT a.attname
          FROM pg_index i
          JOIN pg_class t ON t.oid = i.indrelid
          JOIN pg_namespace n ON n.oid = t.relnamespace
          JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(i.indkey)
         WHERE n.nspname = :schema
           AND t.relname = 'address_checksum_map'
           AND i.indisprimary
         ORDER BY array_position(i.indkey, a.attnum);
        """,
        schema=schema,
    )

    assert [row[0] for row in columns] == ["checksum"]


@pytest.mark.asyncio(loop_scope="module")
async def test_address_canonical_sql_functions_are_immutable_parallel_safe_and_pub28_backed():
    """Verify address canonical sql functions are immutable parallel safe and pub28 backed."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    rows = await db.all(
        """
        SELECT p.proname, p.provolatile::text AS provolatile, p.proparallel::text AS proparallel
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = :schema
           AND p.proname = ANY(:names)
         ORDER BY p.proname;
        """,
        schema=schema,
        names=[
            "addr_key_from_identity_v1",
            "addr_space_norm_v1",
            "addr_state_code_v1",
            "addr_street_norm_v1",
            "addr_street_token_norm_v1",
            "addr_floor_value_norm_v1",
            "addr_strip_duplicate_tail_unit_v1",
            "addr_repeated_bare_line2_unit_v1",
            "addr_unit_norm_v1",
            "addr_unit_prefix_v1",
            "addr_unit_range_required_v1",
            "addr_unit_value_valid_v1",
        ],
    )
    flags = {row.proname: (row.provolatile, row.proparallel) for row in rows}

    assert flags
    assert all(value == ("i", "s") for value in flags.values())
    assert await db.scalar(f"SELECT {schema}.addr_state_code_v1('Puerto Rico');") == "PR"
    assert await db.scalar(f"SELECT {schema}.addr_state_code_v1('Armed Forces Pacific');") == "AP"
    assert await db.scalar(f"SELECT {schema}.addr_state_code_v1('calif');") is None
    assert await db.scalar(f"SELECT {schema}.addr_zip5_norm_v1('2138');") == "02138"
    assert await db.scalar(f"SELECT {schema}.addr_country_code_v1('canada');") == "CANADA"
    assert await db.scalar(f"SELECT {schema}.addr_country_code_v1('Usa');") == "US"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('11 Noreste Turnpike', '');") == "11netpke"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('12 County Underpass', '');") == "12countyupas"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('100 Florida Ave Fl 2', '');") == "100floridaave"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('100 Fleet St Fl 2', '');") == "100fleetst"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('200 14 St', '');") == await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('200 14th St', '');"
    )
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('200 14h St', '');") == await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('200 14th St', '');"
    )
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('200 First Ave', '');") == await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('200 1st Ave', '');"
    )
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('1200 First Street, NE', '');") == await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('1200 1ST ST NE', '');"
    )
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('200 Saint Clair Ave', '');") == await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('200 St Clair Ave', '');"
    )
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('123 Main St Ste 200', 'Apt 5');"
    ) == "123mainstste200"
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('123 Main St Ste 200', 'Suite');"
    ) == "123mainstste200suite"
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('123 Main St Ste 200', 'West Wing');"
    ) == await db.scalar(f"SELECT {schema}.addr_street_norm_v1('123 Main St Ste 200 West Wing', '');")
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('123 Main St Suite 100', 'Suite 100');"
    ) == "123mainst"
    assert await db.scalar(
        f"SELECT {schema}.addr_unit_norm_v1('123 Main St Suite 100', 'Suite 100');"
    ) == "ste100"
    assert await db.scalar(
        f"SELECT {schema}.addr_repeated_bare_line2_unit_v1('2227 HALTOM RD STE F', 'F');"
    ) == "stef"
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('2227 HALTOM RD STE F', 'F');"
    ) == "2227haltomrd"
    assert await db.scalar(
        f"SELECT {schema}.addr_unit_norm_v1('2227 HALTOM RD STE F', 'F');"
    ) == "stef"
    assert await db.scalar(
        f"SELECT {schema}.addr_unit_norm_v1('209 S HOUSTON AVE DEPT', 'DEPARTMENT');"
    ) == ""
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('209 S HOUSTON AVE DEPT', 'DEPARTMENT');"
    ) == "209shoustonavedeptdepartment"
    assert await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '2227 HALTOM RD STE F', 'F', 'HALTOM CITY', 'TX', '76117', 'US'
        );
        """
    ) == await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '2227 HALTOM RD STE F', '', 'HALTOM CITY', 'TX', '76117', 'US'
        );
        """
    )
    assert await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '123 Main St Suite 100',
            'Suite 100',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    ) == await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '123 Main St',
            'Suite 100',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    )
    assert await db.scalar(
        f"SELECT {schema}.addr_identity_key_v1('123 Main St 1st Fl', '1st Floor', 'Austin', 'TX', '78701', 'US');"
    ) == "v2|123mainst|fl1||TX|78701|US|street"
    assert await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '7281 E EARLL DR STE 1 BLDG A',
            'Ste 1 Bldg A',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    ) == await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '7281 E EARLL DR STE 1 BLDG A',
            '',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    )
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('7281 E EARLL DR STE 1 BLDG A', 'Ste 1 Bldg A');"
    ) == "7281eearlldrste1"
    assert await db.scalar(
        f"SELECT {schema}.addr_unit_norm_v1('7281 E EARLL DR STE 1 BLDG A', 'Ste 1 Bldg A');"
    ) == "bldga"
    assert await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '1623 3rd Ave Ste 201 Ofc 5',
            'Ste 201 Ofc 5',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    ) == await db.scalar(
        f"""
        SELECT {schema}.addr_identity_key_v1(
            '1623 3rd Ave Ste 201 Ofc 5',
            '',
            'Austin',
            'TX',
            '78701',
            'US'
        );
        """
    )
    assert await db.scalar(
        f"SELECT {schema}.addr_street_norm_v1('1623 3rd Ave Ste 201 Ofc 5', 'Ste 201 Ofc 5');"
    ) == "16233aveste201"
    assert await db.scalar(
        f"SELECT {schema}.addr_unit_norm_v1('1623 3rd Ave Ste 201 Ofc 5', 'Ste 201 Ofc 5');"
    ) == "ofc5"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('100 Main Street Basement', '');") == "bsmt"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('100 Main Street', 'Penthouse');") == "ph"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St Ste 200', 'Apt 5');") == "apt5"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St', 'Suite E');") == "stee"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('123 Main St', 'Suite E');") == "123mainst"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St', 'Suite 200 A');") == "ste200a"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St STE 310 D', '');") == "ste310d"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St', '2nd Floor');") == "fl2"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('123 Main St', 'Second Fl');") == "fl2"
    assert await db.scalar(f"SELECT {schema}.addr_street_norm_v1('123 Main St 1st Fl', '');") == "123mainst"
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('100 Main Street', 'Suite Road');") == ""
    assert await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('100 Main Street', 'Unit 200 A');") == ""
    assert (
        await db.scalar(f"SELECT {schema}.addr_unit_norm_v1('27 Dr Mellichamp Dr' || chr(160) || 'Ste 100', '');")
        == "ste100"
    )
    assert (
        await db.scalar(
            f"SELECT {schema}.addr_street_norm_v1('27 Dr Mellichamp Dr' || chr(160) || 'Ste 100', '');"
        )
        == "27drmellichampdr"
    )
    assert (
        await db.scalar(
            f"""
            SELECT {schema}.addr_identity_key_v1(
                '27 Dr Mellichamp Dr' || chr(160) || 'Ste 100',
                '',
                'BLUFFTON',
                'SC',
                '29910',
                'US'
            );
            """
        )
        == "v2|27drmellichampdr|ste100||SC|29910|US|street"
    )
    assert (
        await db.scalar(
            f"""
            SELECT {schema}.addr_identity_key_v1(
                'DEPARTMENT 1234',
                '',
                'KNOXVILLE',
                'TN',
                '37995',
                'US'
            );
            """
        )
        == "v2||dept1234|knoxville|TN|37995|US|city_zip"
    )
    assert (
        await db.scalar(
            f"""
            SELECT {schema}.addr_key_v1(
                '27 Dr Mellichamp Dr' || chr(160) || 'Ste 100',
                '',
                'BLUFFTON',
                'SC',
                '29910',
                'US'
            )::text;
            """
        )
        == "3e3ea29f-8c26-17ba-dcc8-74424e66fd32"
    )
    assert await db.scalar(
        f"SELECT {schema}.addr_identity_key_v1('1 Burrard Street', '', 'Vancouver', 'British Columbia', 'V6B 2W9', 'canada');"
    ) is None


@pytest.mark.asyncio(loop_scope="module")
async def test_address_archive_v2_state_code_rejects_unmapped_values():
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")

    max_length = await db.scalar(
        """
        SELECT character_maximum_length
          FROM information_schema.columns
         WHERE table_schema = :schema
           AND table_name = 'address_archive_v2'
           AND column_name = 'state_code';
        """,
        schema=schema,
    )

    assert int(max_length) >= 32
    assert await db.scalar(f"SELECT {schema}.addr_state_code_v1('CALIFORNIA (CA)');") is None


@pytest.mark.asyncio(loop_scope="module")
async def test_stamp_and_resolve_addresses_into_archive_v2(monkeypatch):
    """Verify stamp and resolve addresses into archive v2."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_test"
    progress_events = []
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code)
        VALUES
            ('123 Main Street', 'Suite 200', 'Miami', 'Florida', '33156-2814'),
            ('123 MAIN ST STE 200', NULL, 'MIAMI', 'FL', '33156'),
            ('123 Main Street', 'Suite 310', 'Miami', 'FL', '33156');
        """
    )

    stamped = await stamp_address_keys(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        schema=schema,
        shards=2,
    )
    stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )

    archive_rows = int(await db.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2;") or 0)
    stage_keys = int(await db.scalar(f"SELECT count(DISTINCT address_key) FROM {schema}.{stage_table};") or 0)
    source_bits = await db.all(
        f"SELECT DISTINCT source_bits FROM {schema}.address_archive_v2 ORDER BY source_bits;"
    )

    assert stamped == 3
    assert stage_keys == 2
    assert archive_rows == 2
    assert stats.staged == 3
    assert stats.distinct_keys == 2
    assert stats.inserted == 2
    assert stats.null_key_rows == 0
    assert stats.eligible_key_rows == 3
    assert stats.eligible_null_key_rows == 0
    assert stats.gate_violations == ()
    assert stats.reason_buckets["missing_zip"] == 0
    assert stats.reason_buckets["missing_state"] == 0
    assert stats.reason_buckets["unit_conflicts"] == 1
    assert [row[0] for row in source_bits] == [2]

    premise_keys = int(
        await db.scalar(
            f"SELECT count(*) FROM {schema}.address_archive_v2 WHERE premise_key IS NOT NULL;"
        )
        or 0
    )
    assert premise_keys == 2

    same_source_rerun_stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )
    assert same_source_rerun_stats.inserted == 0
    assert same_source_rerun_stats.provenance_updates == 0
    assert same_source_rerun_stats.gate_sample_rows == []

    rerun_stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        source_bit=4,
        priority=0,
        schema=schema,
    )
    source_bits_after = await db.all(
        f"SELECT DISTINCT source_bits FROM {schema}.address_archive_v2 ORDER BY source_bits;"
    )
    display_priority_value = await db.scalar(
        f"SELECT min(display_priority) FROM {schema}.address_archive_v2;"
    )
    display_priority = int(display_priority_value)

    assert rerun_stats.inserted == 0
    assert rerun_stats.provenance_updates == 2
    assert len(rerun_stats.gate_sample_rows) == 2
    assert {row["source_bits"] for row in rerun_stats.gate_sample_rows} == {6}
    assert {row["unit_norm"] for row in rerun_stats.gate_sample_rows} == {"ste200", "ste310"}
    assert [row[0] for row in source_bits_after] == [6]
    assert display_priority == 0
    assert "address key stamping" in {event.get("phase") for event in progress_events}
    assert "address archive resolve" in {event.get("phase") for event in progress_events}
    assert any(
        event.get("source") == "address-canonical"
        and event.get("unit") == "shard"
        and event.get("total") == 2
        for event in progress_events
    )
    assert any(
        event.get("source") == "address-canonical"
        and event.get("message") == "canonical address archive resolved"
        for event in progress_events
    )
    assert any(
        event.get("source") == "address-canonical"
        and event.get("message") == "canonical address gate passed"
        for event in progress_events
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_rust_materialized_resolve_matches_sql_resolve(monkeypatch):
    """Verify rust materialized resolve matches sql resolve."""
    _requires_test_database()
    if address_canon._ptg2_rust_scanner_binary() is None:
        pytest.skip("Rust ptg2_scanner binary is required for address materialization parity")

    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_rust_materialize_test"
    field_map = {
        "first_line": "first_line",
        "second_line": "second_line",
        "city": "city",
        "state": "state",
        "zip": "zip_code",
        "country": "COALESCE(NULLIF(country, ''), 'US')",
    }

    await db.status(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code, country)
        VALUES
            ('27 Dr Mellichamp Dr' || chr(160) || 'Ste 100', NULL, 'BLUFFTON', 'SC', '29910', 'US'),
            ('123 Main Street', 'Suite 200', 'Miami', 'Florida', '33156-2814', 'US'),
            ('123 MAIN ST STE 200', NULL, 'MIAMI', 'FL', '33156', 'US'),
            ('123 Main Street', 'Suite 310', 'Miami', 'FL', '33156', 'US'),
            ('No State Road', NULL, 'Austin', NULL, '78701', 'US'),
            ('Foreign Road', NULL, 'Vancouver', 'British Columbia', 'V6B 2W9', 'Canada');
        """
    )
    await stamp_address_keys(stage_table, field_map, schema=schema, shards=2)

    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "false")
    sql_stats = await resolve_into_archive(
        stage_table,
        field_map,
        source_bit=2,
        priority=1,
        schema=schema,
    )
    sql_rows = await _archive_v2_rows(schema)

    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "true")
    rust_stats = await resolve_into_archive(
        stage_table,
        field_map,
        source_bit=2,
        priority=1,
        schema=schema,
    )
    rust_rows = await _archive_v2_rows(schema)

    assert _resolve_stats_projection(rust_stats) == _resolve_stats_projection(sql_stats)
    assert rust_rows == sql_rows


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_aborts_when_stamped_key_disagrees_with_identity(monkeypatch):
    """Verify resolve aborts when stamped key disagrees with identity."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stale_stamp_test"
    progress_events = []
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    stale_key = address_canon.address_key_v1("999 Old Road", "", "Miami", "FL", "33156", "US")
    expected_key = address_canon.address_key_v1("123 Main Street", "Suite 200", "Miami", "FL", "33156", "US")

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (address_key, first_line, second_line, city, state, zip_code)
        VALUES (:address_key, '123 Main Street', 'Suite 200', 'Miami', 'FL', '33156');
        """,
        address_key=stale_key,
    )

    with pytest.raises(RuntimeError, match="Stamped canonical address key does not match identity_key") as exc:
        await resolve_into_archive(
            stage_table,
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city",
                "state": "state",
                "zip": "zip_code",
                "country": "'US'",
            },
            source_bit=2,
            priority=1,
            schema=schema,
        )

    assert str(stale_key) in str(exc.value)
    assert str(expected_key) in str(exc.value)
    assert int(await db.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2;") or 0) == 0
    assert any(
        event.get("source") == "address-canonical"
        and event.get("status") == "failed"
        and event.get("message") == "stamped canonical address key mismatch"
        for event in progress_events
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_migrate_legacy_archive_to_v2_builds_bridge_and_verifies_geocodes():
    """Verify migrate legacy archive to v2 builds bridge and verifies geocodes."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    legacy_table = "address_archive_legacy_migrate_test"

    await db.status(f"DROP TABLE IF EXISTS {schema}.{legacy_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{legacy_table} (
            checksum bigint,
            first_line text,
            second_line text,
            city_name text,
            state_name text,
            postal_code text,
            country_code text,
            telephone_number text,
            fax_number text,
            formatted_address text,
            lat numeric(11,8),
            long numeric(11,8),
            date_added date,
            place_id text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{legacy_table} (
            checksum, first_line, second_line, city_name, state_name, postal_code,
            country_code, formatted_address, lat, long, date_added, place_id
        )
        VALUES
            (10, '123 Main Street', 'Suite 200', 'Miami', 'FL', '33156', 'US',
             'old geocode', 25.76100000, -80.19100000, '2024-01-01', NULL),
            (11, '123 MAIN ST STE 200', NULL, 'MIAMI', 'Florida', '33156-2814', 'US',
             'new geocode', 25.76200000, -80.19200000, '2025-01-01', 'place-new'),
            (20, '500 Collision Road', 'Suite 1', 'Austin', 'TX', '78701', 'US',
             'collision one', 30.27100000, -97.74100000, '2024-02-01', 'place-c1'),
            (20, '500 Collision Road', 'Suite 2', 'Austin', 'TX', '78701', 'US',
             'collision two', 30.27200000, -97.74200000, '2024-02-02', 'place-c2'),
            (30, 'No State Road', NULL, 'Austin', NULL, '78701', 'US',
             'non keyable', 30.27300000, -97.74300000, '2024-03-01', 'place-missing');
        """
    )

    stats = await migrate_legacy_archive_to_v2(
        schema=schema,
        legacy_table=legacy_table,
        work_mem="64MB",
        timeout="2min",
        sample_limit=5,
    )

    assert stats.legacy_rows == 5
    assert stats.keyable_rows == 4
    assert stats.non_keyable_rows == 1
    assert stats.upserted_rows == 3
    assert stats.inserted_rows == 3
    assert stats.updated_rows == 0
    assert stats.checksum_map_rows == 2
    assert stats.checksum_collision_rows == 2
    assert stats.checksum_collision_checksums == 1
    assert stats.represented_missing_keys == 0
    assert stats.legacy_geocoded_keys == 3
    assert stats.geocoded_missing_keys == 0
    assert stats.sample_rows

    winner = await db.first(
        f"""
        SELECT formatted_address, lat, long, place_id, source_bits, display_priority
          FROM {schema}.address_archive_v2
         WHERE line1_norm = '123mainst'
           AND unit_norm = 'ste200';
        """
    )
    assert winner is not None
    assert winner.formatted_address == "new geocode"
    assert str(winner.place_id) == "place-new"
    assert int(winner.source_bits) & 1 == 1
    assert int(winner.display_priority) == 0

    map_rows = await db.all(
        f"SELECT checksum FROM {schema}.address_checksum_map ORDER BY checksum;"
    )
    collision_rows = await db.all(
        f"SELECT checksum, count(*) AS n FROM {schema}.address_checksum_collision GROUP BY checksum;"
    )
    assert [int(row.checksum) for row in map_rows] == [10, 11]
    assert [(int(row.checksum), int(row.n)) for row in collision_rows] == [(20, 2)]

    rerun_stats = await migrate_legacy_archive_to_v2(
        schema=schema,
        legacy_table=legacy_table,
        work_mem="64MB",
        timeout="2min",
        sample_limit=1,
    )
    assert rerun_stats.represented_missing_keys == 0
    assert rerun_stats.geocoded_missing_keys == 0
    assert rerun_stats.checksum_map_rows == 2
    assert rerun_stats.checksum_collision_rows == 2


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_reports_gate_warning_for_eligible_unstamped_rows(monkeypatch):
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_gate_warning_test"
    progress_events = []
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code)
        VALUES ('500 Valid Street', 'Suite 10', 'Austin', 'TX', '78701');
        """
    )

    stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )

    assert stats.staged == 1
    assert stats.distinct_keys == 1
    assert stats.inserted == 1
    assert stats.eligible_key_rows == 1
    assert stats.eligible_null_key_rows == 1
    assert stats.gate_violations == ("eligible_rows_missing_address_key",)
    assert any(
        event.get("source") == "address-canonical"
        and event.get("status") == "warning"
        and event.get("message") == "canonical address gate warnings"
        and event.get("violations") == ["eligible_rows_missing_address_key"]
        for event in progress_events
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_python_and_sql_address_canonical_golden_corpus_match():
    """Verify python and sql address canonical golden corpus match."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    cases = _golden_cases()
    assert len(cases) >= 200

    group_keys: dict[str, tuple[str | None, str | None]] = {}
    for case in cases:
        values = {
            "first_line": case.get("first_line"),
            "second_line": case.get("second_line"),
            "city": case.get("city"),
            "state": case.get("state"),
            "zip": case.get("zip"),
            "country": case.get("country"),
        }
        py_identity = address_canon.identity_key_v1(
            values["first_line"],
            values["second_line"],
            values["city"],
            values["state"],
            values["zip"],
            values["country"],
        )
        py_key = address_canon.address_key_v1(
            values["first_line"],
            values["second_line"],
            values["city"],
            values["state"],
            values["zip"],
            values["country"],
        )
        py_premise_identity = address_canon.premise_identity_key_v1(
            values["first_line"],
            values["second_line"],
            values["city"],
            values["state"],
            values["zip"],
            values["country"],
        )
        py_premise_key = address_canon.key_from_identity(py_premise_identity)

        row = await db.first(
            f"""
            SELECT
                {schema}.addr_identity_key_v1(
                    :first_line, :second_line, :city, :state, :zip, :country
                ) AS identity_key,
                {schema}.addr_key_v1(
                    :first_line, :second_line, :city, :state, :zip, :country
                )::text AS address_key,
                {schema}.addr_key_from_identity_v1(
                    {schema}.addr_identity_key_v1(
                        :first_line, :second_line, :city, :state, :zip, :country
                    )
                )::text AS address_key_from_identity,
                {schema}.addr_premise_identity_key_v1(
                    :first_line, :second_line, :city, :state, :zip, :country
                ) AS premise_identity_key,
                {schema}.addr_premise_key_v1(
                    :first_line, :second_line, :city, :state, :zip, :country
                )::text AS premise_key;
            """,
            **values,
        )
        data = row._mapping

        assert data["identity_key"] == py_identity, case["id"]
        assert data["address_key"] == (str(py_key) if py_key else None), case["id"]
        assert data["address_key_from_identity"] == data["address_key"], case["id"]
        assert data["premise_identity_key"] == py_premise_identity, case["id"]
        assert data["premise_key"] == (str(py_premise_key) if py_premise_key else None), case["id"]
        assert data["identity_key"] == case["expected_identity_key"], case["id"]
        assert data["address_key"] == case["expected_address_key"], case["id"]
        assert data["premise_identity_key"] == case["expected_premise_identity_key"], case["id"]
        assert data["premise_key"] == case["expected_premise_key"], case["id"]

        if group := case.get("equivalence_group"):
            current = (data["identity_key"], data["address_key"])
            group_keys.setdefault(group, current)
            assert group_keys[group] == current, case["id"]


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_reason_buckets_and_collision_abort(monkeypatch):
    """Verify resolve reason buckets and collision abort."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_collision_test"
    progress_events = []
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code, country)
        VALUES
            ('99 Conflict Road', 'Suite 1', 'Austin', 'TX', '78701', 'US'),
            ('1 Missing Zip Road', NULL, 'Austin', 'TX', NULL, 'US'),
            ('2 Missing State Road', NULL, 'Austin', NULL, '78701', 'US'),
            (NULL, NULL, 'Austin', 'TX', '78701', 'US'),
            ('3 Foreign Road', NULL, 'Toronto', 'ON', 'M5V 2T6', 'CA'),
            ('4 Odd Unit Road', 'Desk near lobby', 'Austin', 'TX', '78701', 'US');
        """
    )
    await stamp_address_keys(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "COALESCE(NULLIF(country, ''), 'US')",
        },
        schema=schema,
        shards=1,
    )
    collision_key = await db.scalar(
        f"""
        SELECT {schema}.addr_key_v1(
            '99 Conflict Road', 'Suite 1', 'Austin', 'TX', '78701', 'US'
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.address_archive_v2 (
            address_key, identity_key, precision, unit_norm, country_code
        )
        VALUES (
            :address_key, 'v2|differentstreet|ste1||TX|78701|US|street',
            'street', 'ste1', 'US'
        );
        """,
        address_key=collision_key,
    )

    with pytest.raises(RuntimeError, match="Canonical address key collision"):
        await resolve_into_archive(
            stage_table,
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city",
                "state": "state",
                "zip": "zip_code",
                "country": "COALESCE(NULLIF(country, ''), 'US')",
            },
            source_bit=2,
            priority=1,
            schema=schema,
        )
    assert any(
        event.get("status") == "failed"
        and event.get("source") == "address-canonical"
        and event.get("message") == "canonical address key collision"
        for event in progress_events
    )

    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "COALESCE(NULLIF(country, ''), 'US')",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )

    assert stats.reason_buckets["missing_zip"] == 1
    assert stats.reason_buckets["missing_state"] == 1
    assert stats.reason_buckets["missing_street"] == 1
    assert stats.reason_buckets["unsupported_country"] == 1
    assert stats.reason_buckets["ambiguous_unit"] == 1
    assert stats.inserted == 3


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_aliases_missing_suffix_and_direction_only_when_unique(monkeypatch):
    """Verify resolve aliases missing suffix and direction only when unique."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_completion_alias_test"
    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "false")

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code, country)
        VALUES
            ('10 Holcombe Blvd', NULL, 'Houston', 'TX', '77030', 'US'),
            ('10 Holcombe', NULL, 'Houston', 'TX', '77030', 'US'),
            ('30 N Main St', NULL, 'Austin', 'TX', '78701', 'US'),
            ('30 Main St', NULL, 'Austin', 'TX', '78701', 'US'),
            ('20 Ambiguous Rd', NULL, 'Austin', 'TX', '78701', 'US'),
            ('20 Ambiguous Ave', NULL, 'Austin', 'TX', '78701', 'US'),
            ('20 Ambiguous', NULL, 'Austin', 'TX', '78701', 'US');
        """
    )

    stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "COALESCE(NULLIF(country, ''), 'US')",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )

    holcombe_target = await db.scalar(
        f"SELECT {schema}.addr_key_v1('10 Holcombe Blvd', NULL, 'Houston', 'TX', '77030', 'US');"
    )
    holcombe_alias = await db.scalar(
        f"SELECT address_key FROM {schema}.{stage_table} WHERE first_line = '10 Holcombe';"
    )
    directional_target = await db.scalar(
        f"SELECT {schema}.addr_key_v1('30 N Main St', NULL, 'Austin', 'TX', '78701', 'US');"
    )
    directional_alias = await db.scalar(
        f"SELECT address_key FROM {schema}.{stage_table} WHERE first_line = '30 Main St';"
    )
    ambiguous_original = await db.scalar(
        f"SELECT {schema}.addr_key_v1('20 Ambiguous', NULL, 'Austin', 'TX', '78701', 'US');"
    )
    ambiguous_alias = await db.scalar(
        f"SELECT address_key FROM {schema}.{stage_table} WHERE first_line = '20 Ambiguous';"
    )
    ambiguous_archive_key = await db.scalar(
        f"""
        SELECT address_key
          FROM {schema}.address_archive_v2
         WHERE address_key = :address_key;
        """,
        address_key=ambiguous_original,
    )

    assert holcombe_alias == holcombe_target
    assert directional_alias == directional_target
    assert ambiguous_alias is None
    assert ambiguous_archive_key == ambiguous_original
    assert stats.reason_buckets["completion_aliases"] == 2
    assert stats.reason_buckets["completion_suffix_aliases"] == 1
    assert stats.reason_buckets["completion_directional_aliases"] == 1


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_leaves_missing_zip_unkeyed_without_coordinate_restore(monkeypatch):
    """Verify resolve leaves missing zip unkeyed without coordinate restore."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_missing_zip_repair_test"
    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "false")

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code, country)
        VALUES
            ('10 Zip Repair Road', NULL, 'Austin', 'TX', '78701', 'US'),
            ('10 Zip Repair Road', NULL, 'Austin', 'TX', NULL, 'US'),
            ('20 Ambiguous Zip Road', NULL, 'Austin', 'TX', '78701', 'US'),
            ('20 Ambiguous Zip Road', NULL, 'Austin', 'TX', '78702', 'US'),
            ('20 Ambiguous Zip Road', NULL, 'Austin', 'TX', NULL, 'US');
        """
    )

    stats = await resolve_into_archive(
        stage_table,
        {
            "first_line": "first_line",
            "second_line": "second_line",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "COALESCE(NULLIF(country, ''), 'US')",
        },
        source_bit=2,
        priority=1,
        schema=schema,
    )

    missing_unique_key = await db.scalar(
        f"""
        SELECT address_key
          FROM {schema}.{stage_table}
         WHERE first_line = '10 Zip Repair Road'
           AND zip_code IS NULL;
        """
    )
    ambiguous_key = await db.scalar(
        f"""
        SELECT address_key
          FROM {schema}.{stage_table}
         WHERE first_line = '20 Ambiguous Zip Road'
           AND zip_code IS NULL;
        """
    )

    assert missing_unique_key is None
    assert ambiguous_key is None
    assert "zip_aliases" not in stats.reason_buckets
    assert "missing_zip_recovered" not in stats.reason_buckets
    assert stats.reason_buckets["missing_zip"] == 2


@pytest.mark.asyncio(loop_scope="module")
async def test_resolve_aborts_same_batch_identity_collision(monkeypatch):
    """Verify resolve aborts same batch identity collision."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_batch_collision_test"
    progress_events = []
    monkeypatch.setenv(address_canon.ADDRESS_CANON_RUST_MATERIALIZE_ENV, "false")
    monkeypatch.setattr(address_canon, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text,
            country text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    forged_key = "00000000-0000-0000-0000-000000000001"
    original_key_sql = address_canon._key_from_identity_sql

    def fake_key_from_identity_sql(schema_name, identity_expr):
        return (
            "CASE "
            f"WHEN {identity_expr} LIKE '%collisionrd%' THEN '{forged_key}'::uuid "
            f"ELSE {original_key_sql(schema_name, identity_expr)} "
            "END"
        )

    monkeypatch.setattr(address_canon, "_key_from_identity_sql", fake_key_from_identity_sql)
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code, country)
        VALUES
            ('10 Collision Road', 'Suite 1', 'Austin', 'TX', '78701', 'US'),
            ('20 Collision Road', 'Suite 1', 'Austin', 'TX', '78701', 'US');
        """
    )

    with pytest.raises(RuntimeError, match="Canonical address key collision"):
        await resolve_into_archive(
            stage_table,
            {
                "first_line": "first_line",
                "second_line": "second_line",
                "city": "city",
                "state": "state",
                "zip": "zip_code",
                "country": "COALESCE(NULLIF(country, ''), 'US')",
            },
            source_bit=2,
            priority=1,
            schema=schema,
        )

    archive_rows = int(await db.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2;") or 0)
    assert archive_rows == 0
    assert any(
        event.get("status") == "failed"
        and event.get("source") == "address-canonical"
        and event.get("message") == "canonical address key collision"
        for event in progress_events
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_facility_anchor_coordinates_refresh_archive_geocode_fields():
    """Verify facility anchor coordinates refresh archive geocode fields."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "facility_anchor_geocode_fixture"

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            address_line1 text,
            city text,
            state text,
            zip_code text,
            latitude numeric,
            longitude numeric,
            facility_type text
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (
            address_line1, city, state, zip_code, latitude, longitude, facility_type
        )
        VALUES (
            '777 Facility Audit Way', 'Austin', 'TX', '78702', 30.26720000, -97.74310000, 'Hospital'
        );
        """
    )

    await stamp_address_keys(
        stage_table,
        {
            "first_line": "address_line1",
            "second_line": "NULL",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        schema=schema,
    )
    await resolve_into_archive(
        stage_table,
        {
            "first_line": "address_line1",
            "second_line": "NULL",
            "city": "city",
            "state": "state",
            "zip": "zip_code",
            "country": "'US'",
        },
        source_bit=8,
        priority=4,
        schema=schema,
    )
    address_key = await db.scalar(f"SELECT address_key FROM {schema}.{stage_table};")
    await db.status(
        f"""
        UPDATE {schema}.address_archive_v2
           SET lat = NULL,
               long = NULL,
               geo_source = NULL,
               geocode_source = NULL,
               geocode_quality = NULL,
               geocoded_at = NULL
         WHERE address_key = :address_key;
        """,
        address_key=address_key,
    )

    updated = await facility_anchors._refresh_archive_geocodes_from_facility_anchors(stage_table, schema)
    assert updated == 1

    row = await db.first(
        f"""
        SELECT lat, long, geo_source, geocode_source, geocode_quality
          FROM {schema}.address_archive_v2
         WHERE address_key = :address_key;
        """,
        address_key=address_key,
    )
    assert float(row.lat) == pytest.approx(30.2672)
    assert float(row.long) == pytest.approx(-97.7431)
    assert row.geo_source == "manual"
    assert row.geocode_source == "facility_anchor"
    assert row.geocode_quality == "facility_anchor"

    await db.status(
        f"""
        UPDATE {schema}.{stage_table}
           SET latitude = 31.0,
               longitude = -98.0;
        """
    )
    assert await facility_anchors._refresh_archive_geocodes_from_facility_anchors(stage_table, schema) == 0
    unchanged = await db.first(
        f"""
        SELECT lat, long
          FROM {schema}.address_archive_v2
         WHERE address_key = :address_key;
        """,
        address_key=address_key,
    )
    assert float(unchanged.lat) == pytest.approx(30.2672)
    assert float(unchanged.long) == pytest.approx(-97.7431)


@pytest.mark.asyncio(loop_scope="module")
async def test_entity_address_unified_rebuild_includes_mrf_source_with_address_key():
    """Verify entity address unified rebuild includes mrf source with address key."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_cls = make_class(EntityAddressUnified, "mrf_source_fixture")
    stage_table = stage_cls.__tablename__

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(f"DROP TABLE IF EXISTS {schema}.mrf_address;")
    await db.status(
        f"""
        CREATE TABLE {schema}.mrf_address (
            npi bigint,
            type varchar,
            first_line varchar,
            second_line varchar,
            city_name varchar,
            state_name varchar,
            postal_code varchar,
            country_code varchar,
            telephone_number varchar,
            fax_number varchar,
            formatted_address varchar,
            lat numeric,
            long numeric,
            date_added date,
            place_id varchar,
            address_key uuid,
            checksum bigint
        );
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.mrf_address (
            npi, type, first_line, second_line, city_name, state_name,
            postal_code, country_code, telephone_number, fax_number,
            formatted_address, lat, long, date_added, place_id, address_key, checksum
        )
        VALUES (
            1234567890, 'practice', '10 Market Street', 'Suite 5',
            'Boston', 'MA', '02108', 'US', '6175550101', NULL, NULL,
            NULL, NULL, CURRENT_DATE, NULL,
            {schema}.addr_key_v1('10 Market Street', 'Suite 5', 'Boston', 'MA', '02108', 'US'),
            1001
        );
        """
    )
    await db.create_table(stage_cls.__table__, checkfirst=True)

    source_selects = entity_address_unified._source_selects(
        schema,
        {
            "mrf_address": True,
            "npi": False,
            "npi_address": False,
            "geo_zip_lookup": False,
        },
    )
    await db.status(
        entity_address_unified._materialize_sql(
            schema,
            stage_table,
            source_selects,
            address_canon_available=True,
        )
    )

    rows = await db.all(
        f"""
        SELECT entity_type, entity_id, type, address_sources, first_line,
               city_name, state_name, postal_code, address_key::text
          FROM {schema}.{stage_table}
         WHERE address_sources @> ARRAY['mrf']::varchar[];
        """
    )

    assert len(rows) == 1
    row = rows[0]._mapping
    assert row["entity_type"] == "npi"
    assert row["entity_id"] == "1234567890"
    assert row["type"] == "practice"
    assert row["first_line"] == "10 Market Street"
    assert row["city_name"] == "Boston"
    assert row["address_key"] == str(
        address_canon.address_key_v1("10 Market Street", "Suite 5", "Boston", "MA", "02108", "US")
    )


@pytest.mark.asyncio(loop_scope="module")
async def test_address_canonical_helpers_honor_cancel_before_writes():
    """Verify address canonical helpers honor cancel before writes."""
    _requires_test_database()
    schema = os.getenv("HLTHPRT_DB_SCHEMA", "mrf")
    stage_table = "address_canon_stage_cancel_test"

    await db.status(f"DROP TABLE IF EXISTS {schema}.{stage_table};")
    await db.status(
        f"""
        CREATE TABLE {schema}.{stage_table} (
            address_key uuid,
            first_line text,
            second_line text,
            city text,
            state text,
            zip_code text
        );
        """
    )
    await db.status(
        f"TRUNCATE TABLE {schema}.address_checksum_map, "
        f"{schema}.address_checksum_collision, {schema}.address_archive_v2;"
    )
    await db.status(
        f"""
        INSERT INTO {schema}.{stage_table} (first_line, second_line, city, state, zip_code)
        VALUES ('1 Cancel Road', NULL, 'Austin', 'TX', '78701');
        """
    )

    async def cancel_check():
        raise RuntimeError("cancel requested")

    field_map = {
        "first_line": "first_line",
        "second_line": "second_line",
        "city": "city",
        "state": "state",
        "zip": "zip_code",
        "country": "'US'",
    }

    with pytest.raises(RuntimeError, match="cancel requested"):
        await stamp_address_keys(stage_table, field_map, schema=schema, cancel_check=cancel_check)

    keyed_rows = int(
        await db.scalar(f"SELECT count(*) FROM {schema}.{stage_table} WHERE address_key IS NOT NULL;")
        or 0
    )
    assert keyed_rows == 0

    await stamp_address_keys(stage_table, field_map, schema=schema)
    with pytest.raises(RuntimeError, match="cancel requested"):
        await resolve_into_archive(
            stage_table,
            field_map,
            source_bit=2,
            priority=1,
            schema=schema,
            cancel_check=cancel_check,
        )

    archive_rows = int(await db.scalar(f"SELECT count(*) FROM {schema}.address_archive_v2;") or 0)
    assert archive_rows == 0
