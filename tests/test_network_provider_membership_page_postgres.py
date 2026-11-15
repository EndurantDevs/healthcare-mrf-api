# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native response parity on synthetic rows; publication authority is tested separately."""

import os
from hashlib import sha256
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy.dialects.postgresql import dialect

from api import network_provider_read as readers
from db.models import EntityAddressUnified
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import PinnedNetworkServingManifest

pytestmark = pytest.mark.asyncio


async def _storage(connection, candidate_id):
    """Create the synthetic named relations consumed by the native paging checks."""
    schema = "network_candidate_" + candidate_id.hex
    namespace = '"' + schema + '"'
    await connection.execute(f"CREATE SCHEMA {namespace}")
    columns = ",".join(
        '"' + column.name + '" ' + column.type.compile(dialect=dialect())
        for column in EntityAddressUnified.__table__.columns
    )
    await connection.execute(f"CREATE TABLE {namespace}.base_address({columns},PRIMARY KEY(location_key))")
    await connection.execute(f"CREATE TABLE {namespace}.custom_address(LIKE {namespace}.base_address INCLUDING ALL)")
    await connection.execute(f"""CREATE TABLE {namespace}.network_membership(
      network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
      location_id uuid NOT NULL,evidence_id text NOT NULL)""")
    await connection.execute(f"""CREATE INDEX network_membership_network_idx
      ON {namespace}.network_membership(network_id)""")
    await connection.execute(f"""CREATE INDEX network_membership_projection_idx
      ON {namespace}.network_membership(provider_system,provider_id,location_id,network_id)""")
    await connection.execute(f"""CREATE TABLE {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key text NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    await connection.execute(f"""CREATE TABLE {namespace}.network_slice_ref(
      network_id integer,office_bucket integer,artifact_candidate_id uuid,slice_id uuid,
      PRIMARY KEY(network_id,office_bucket))""")
    await connection.execute(f"""CREATE TABLE {namespace}.network_office_edge(
      slice_id uuid,location_key text,PRIMARY KEY(slice_id,location_key))""")
    await connection.execute(
        f"CREATE INDEX office_edge_location ON {namespace}.network_office_edge(location_key,slice_id)"
    )
    pins_by_table = {}
    for name in (
        "network_membership",
        "provider_location_binding",
        "base_address",
        "custom_address",
        "network_slice_ref",
        "network_office_edge",
    ):
        pins_by_table[name] = CompactAddressRelation(schema, name)
    manifest = PinnedNetworkServingManifest(
        7, str(candidate_id), schema, 1, {}, 0, "a" * 64, CompactAddressRelation(schema, "entity_address_unified")
    )
    object.__setattr__(
        manifest,
        "_compact_layout",
        CompactNetworkLayout(
            pins_by_table["network_slice_ref"],
            pins_by_table["base_address"],
            (CompactEdgeArtifact(str(candidate_id), schema),),
            pins_by_table["custom_address"],
        ),
    )
    object.__setattr__(manifest, "address_relation", manifest.compact_layout.base)
    return SimpleNamespace(connection=connection, manifest=manifest, namespace=namespace)


async def _office(case, number, provider_id, networks=(42,), *, system="provider_directory", alias=None, custom=False):
    key = f"{number:064x}"
    location = UUID(int=number if alias is None else alias)
    table = "custom_address" if custom else "base_address"
    await case.connection.execute(
        f"""INSERT INTO {case.namespace}.{table}
      (location_key,entity_type,entity_id,entity_name,npi,first_line,city_name,state_name,postal_code,country_code,lat,long,row_origin)
      VALUES($1,$2,$3,$4,$5,'1 Example Street','Sample City','CA','90210','US',34,-118,$6)
      ON CONFLICT(location_key) DO NOTHING""",
        key,
        system,
        provider_id,
        "Provider " + provider_id,
        int(provider_id) if system == "npi" else None,
        "network_custom" if custom else "base",
    )
    await case.connection.execute(
        f"""INSERT INTO {case.namespace}.provider_location_binding VALUES($1,$2,$3,$4,$1,$2)
      ON CONFLICT DO NOTHING""",
        system,
        provider_id,
        location,
        key,
    )
    for network in networks:
        await case.connection.execute(
            f"INSERT INTO {case.namespace}.network_membership VALUES($1,$2,$3,$4,'evidence')",
            network,
            system,
            provider_id,
            location,
        )
        await case.connection.execute(
            f"""INSERT INTO {case.namespace}.network_slice_ref VALUES($1,$4,$2,$3)
          ON CONFLICT DO NOTHING""",
            network,
            UUID(case.manifest.candidate_id),
            UUID(int=network),
            sha256(key.encode()).digest()[0] & 63,
        )
        await case.connection.execute(
            f"""INSERT INTO {case.namespace}.network_office_edge VALUES($1,$2)
          ON CONFLICT DO NOTHING""",
            UUID(int=network),
            key,
        )


@pytest.fixture
async def membership_page_db():
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    candidate_id = uuid4()
    schema = "network_candidate_" + candidate_id.hex
    try:
        await connection.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        case = await _storage(connection, candidate_id)
        await _office(case, 100, "provider-a", alias=200)
        await _office(case, 100, "provider-a", (42, 88), alias=300)
        await _office(case, 100, "provider-a", (), alias=1)
        await _office(case, 101, "provider-a", (88,))
        await _office(case, 102, "1000000491", system="npi")
        await _office(case, 103, str(UUID(int=20)), system="manual", custom=True)
        await _office(case, 104, "Z-provider", (88,))
        await _office(case, 105, "é-provider", (88,))
        await _office(case, 106, "unselected", ())
        await connection.execute(
            f"INSERT INTO {case.namespace}.base_address SELECT * FROM {case.namespace}.custom_address"
        )
        await connection.execute(
            f"UPDATE {case.namespace}.base_address SET row_origin='base',entity_name='Overridden' WHERE location_key=$1",
            f"{103:064x}",
        )
        yield case
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        assert await connection.fetchval("SELECT to_regnamespace($1)", schema) is None
        await connection.close()


async def _read(case, *, fast, networks=(42,), limit=50, offset=0, maximum_bytes=readers.MAX_RESPONSE_BYTES):
    addresses, arguments = readers._provider_addresses(case.manifest, networks, membership_page=fast)
    sql = (readers._MEMBERSHIP_PROVIDERS_SQL if fast else readers._PROVIDERS_SQL).format(
        namespace=case.namespace, addresses=addresses, address_membership="TRUE"
    )
    async with case.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await case.connection.fetchval("SHOW transaction_read_only") == "on"
        receipt = await case.connection.fetchrow(
            sql,
            "{" + ",".join(map(str, networks)) + "}",
            None,
            None,
            limit,
            offset,
            readers.MAX_LOCATIONS,
            maximum_bytes,
            case.manifest.generation_id,
            None,
            None,
            None,
            None,
            *arguments,
        )
        return readers._result(receipt)


async def test_membership_pages_match_full_responses_and_selected_aliases(membership_page_db):
    case = membership_page_db
    for networks in ((42,), (88,), (42, 88), (99,)):
        for limit, offset in ((50, 0), (1, 0), (2, 1), (50, 99)):
            before = await _read(case, fast=False, networks=networks, limit=limit, offset=offset)
            assert await _read(case, fast=True, networks=networks, limit=limit, offset=offset) == before
    page = await _read(case, fast=True)
    provider = next(row for row in page["providers"] if row["provider_id"] == "provider-a")
    assert provider["locations"][0]["location_id"] == str(UUID(int=200))
    assert len(provider["locations"]) == 1
    assert "Overridden" not in str(page) and "unselected" not in str(page)


@pytest.mark.parametrize(
    "damage",
    ["binding", "address", "edge", "origin", "override", "duplicate_address", "name", "npi", "locations", "bytes"],
)
async def test_membership_page_refuses_incomplete_or_unbounded_selected_output(membership_page_db, damage):
    case = membership_page_db
    if damage == "binding":
        await case.connection.execute(
            f"DELETE FROM {case.namespace}.provider_location_binding WHERE provider_id='provider-a'"
        )
    if damage == "edge":
        await case.connection.execute(
            f"DELETE FROM {case.namespace}.network_office_edge WHERE location_key=$1", f"{100:064x}"
        )
    if damage in {"address", "origin"}:
        statement = "DELETE FROM" if damage == "address" else "UPDATE"
        assignment = "" if damage == "address" else " SET row_origin='network_custom'"
        await case.connection.execute(
            f"{statement} {case.namespace}.base_address{assignment} WHERE location_key=$1", f"{100:064x}"
        )
    if damage in {"override", "duplicate_address"}:
        await case.connection.execute(
            f"INSERT INTO {case.namespace}.custom_address SELECT * FROM {case.namespace}.base_address WHERE location_key=$1",
            f"{100:064x}",
        )
        if damage == "override":
            await case.connection.execute(
                f"UPDATE {case.namespace}.custom_address SET entity_id='wrong-provider' WHERE location_key=$1",
                f"{100:064x}",
            )
        else:
            await case.connection.execute(
                f"ALTER TABLE {case.namespace}.custom_address DROP CONSTRAINT custom_address_pkey"
            )
            await case.connection.execute(
                f"INSERT INTO {case.namespace}.custom_address SELECT * FROM {case.namespace}.custom_address WHERE location_key=$1",
                f"{100:064x}",
            )
    if damage in {"name", "npi"}:
        await _office(case, 107, "provider-a")
        field, display_value = ("entity_name", "Different name") if damage == "name" else ("npi", 1234567893)
        if damage == "npi":
            await case.connection.execute(
                f"UPDATE {case.namespace}.base_address SET npi=1000000491 WHERE location_key=$1", f"{100:064x}"
            )
        await case.connection.execute(
            f"UPDATE {case.namespace}.base_address SET {field}=$1 WHERE location_key=$2", display_value, f"{107:064x}"
        )
    if damage == "locations":
        for number in range(1000, 1100):
            await _office(case, number, "provider-a")
    for fast in (False, True):
        with pytest.raises(readers.NetworkProviderReadUnavailable):
            await _read(case, fast=fast, maximum_bytes=1024 if damage == "bytes" else readers.MAX_RESPONSE_BYTES)
