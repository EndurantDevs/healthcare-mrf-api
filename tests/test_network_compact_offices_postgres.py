# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical office union and actual slice matching preserve custom removals."""

from dataclasses import replace
from uuid import uuid4

import pytest

from process.network_compact_layout import CompactAddressRelation
from process.network_compact_offices import CompactOfficeSourceError, CompactOfficeSources, pin_compact_office_sources
from tests.test_network_membership_slices_postgres import _candidate, _prepare
from tests.test_network_membership_slices_postgres import slice_db as slice_db


@pytest.fixture
async def compact_office_db(slice_db):
    fixture = slice_db
    await fixture.connection.execute(
        f"ALTER TABLE \"{fixture.source_schema}\".retained_addresses ADD COLUMN row_origin text NOT NULL DEFAULT 'base'"
    )
    fixture.target = await _candidate(fixture, [(42, 0), (88, 1)])
    namespace = '"' + fixture.target.schema_name + '"'
    await fixture.connection.execute(
        f'CREATE TABLE {namespace}.entity_address_unified (LIKE "{fixture.source_schema}".retained_addresses INCLUDING ALL)'
    )
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.entity_address_unified VALUES('custom-office','manual','manual-provider','custom street','network_custom')"
    )
    site = uuid4()
    await fixture.connection.execute(
        f"UPDATE {namespace}.network_membership SET provider_system='manual',provider_id='manual-provider',location_id=$1 "
        "WHERE network_id=88",
        site,
    )
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.provider_location_binding VALUES('manual','manual-provider',$1,'custom-office','manual','manual-provider')",
        site,
    )
    oid = await fixture.connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint", namespace + ".entity_address_unified"
    )
    fixture.offices = CompactOfficeSources(
        fixture.source, CompactAddressRelation(fixture.target.schema_name, "entity_address_unified", oid)
    )
    yield fixture


@pytest.mark.asyncio
async def test_actual_slices_match_public_and_manual_offices_without_copying_base(compact_office_db):
    fixture = compact_office_db
    async with fixture.connection.transaction():
        receipt = await _prepare(fixture, fixture.target, office_sources=fixture.offices)
    assert receipt.copied_edge_rows == receipt.distinct_memberships == 2
    rows = await fixture.connection.fetch(
        f'SELECT location_key FROM "{fixture.target.schema_name}".network_office_edge ORDER BY location_key'
    )
    assert [row["location_key"] for row in rows] == ["custom-office", "office-a"]
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.target.schema_name}".entity_address_unified')
        == 1
    )
    assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.source_schema}".retained_addresses') == 4


@pytest.mark.asyncio
async def test_historical_custom_is_excluded_even_without_replacement(compact_office_db):
    fixture = compact_office_db
    await fixture.connection.execute(
        f"UPDATE \"{fixture.source_schema}\".retained_addresses SET row_origin='network_custom' WHERE location_key='office-c'"
    )
    async with fixture.connection.transaction():
        relation = await pin_compact_office_sources(
            fixture.connection, fixture.target, CompactOfficeSources(fixture.source)
        )
        rows = await fixture.connection.fetch(f"SELECT location_key FROM {relation} offices ORDER BY location_key")
    assert [row["location_key"] for row in rows] == ["office-a", "office-b", "office-d"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["oid", "namespace", "shape", "origin", "collision"])
async def test_changed_custom_identity_or_public_collision_refuses_complete_match(compact_office_db, damage):
    fixture = compact_office_db
    custom = fixture.offices.custom
    table = '"' + fixture.target.schema_name + '".entity_address_unified'
    if damage == "oid":
        custom = replace(custom, table_oid=custom.table_oid + 1)
    elif damage == "namespace":
        custom = replace(custom, schema_name=fixture.source_schema)
    elif damage == "shape":
        await fixture.connection.execute(f"ALTER TABLE {table} ADD COLUMN unexpected text")
    elif damage == "origin":
        await fixture.connection.execute(f"UPDATE {table} SET row_origin='base'")
    else:
        await fixture.connection.execute(f"UPDATE {table} SET location_key='office-a'")
    async with fixture.connection.transaction():
        with pytest.raises(CompactOfficeSourceError):
            await pin_compact_office_sources(
                fixture.connection, fixture.target, CompactOfficeSources(fixture.source, custom)
            )
    assert await fixture.connection.fetchval("SELECT 1") == 1
