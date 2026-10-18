# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine membership admission and exact-office canonical serving cutover."""

import hashlib
import json
from types import SimpleNamespace
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.ext.asyncio import async_sessionmaker

from api.endpoint import npi
from db.models import NPIData, NPIDataTaxonomy, NUCCTaxonomy
from process.network_address_projection import PinnedAddressSource
from process.network_membership_candidate_lifecycle import (
    admit_network_membership_batch,
    create_network_candidate,
    seal_network_candidate,
)
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.provider_directory_projection_stage import _copy_driver
from tests.test_network_address_read_scope_postgres import _canonical_read_app
from tests.test_network_membership_publication_postgres import (
    _candidate_writer_roles,
    _loader_owned_candidate,
    _reactivate,
)
from tests.test_network_membership_serving_indexes_postgres import _full_source
from tests.test_network_serving_routes_postgres import _reader_engine
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create, _draft

pytestmark = pytest.mark.asyncio
_NPI = {"a": "1000000491", "b": "1000000491", "c": "1000000004"}


_NEARBY_IDENTITY_OBJECTS_SQL = """SELECT object.oid,object.relowner,object.relname,object.relkind
FROM pg_class object WHERE object.relnamespace=to_regnamespace('mrf') ORDER BY object.oid"""


async def _remove_nearby_identity_schema(connection, schema_identity_parts, identity_objects):
    """Remove only the exact fixture-owned identity schema after its readers close."""
    current_identity = await connection.fetchrow("SELECT oid,nspowner FROM pg_namespace WHERE nspname='mrf'")
    if current_identity is None:
        return
    assert schema_identity_parts is not None and tuple(current_identity) == schema_identity_parts
    current_objects = await connection.fetch(_NEARBY_IDENTITY_OBJECTS_SQL)
    assert tuple(tuple(identity_object) for identity_object in current_objects) == identity_objects
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace=to_regnamespace('mrf'))"
    )
    await connection.execute('DROP SCHEMA "mrf" CASCADE')
    assert await connection.fetchval("SELECT to_regnamespace('mrf')") is None


async def _nearby_http_reply(context, query_by_name, headers_by_name):
    _, reply = await context.app.asgi_client.get(
        "/api/v1/npi/near/?" + urlencode(query_by_name), headers=headers_by_name
    )
    return reply


def _assert_nearby_page(reply, *, generation, provider_npi, total_count):
    assert reply.status == 200, (
        f"nearby HTTP status={reply.status}, "
        f"pinned_generation={reply.headers.get('X-Network-Generation')!r}, body={reply.text!r}"
    )
    assert reply.json["total_count"] == total_count
    assert [provider["npi"] for provider in reply.json["items"]] == [provider_npi]
    assert reply.headers["X-Network-Generation"] == str(generation)
    assert reply.headers["Cache-Control"] == "private, no-store"
    assert all("canonical_network_ids" not in provider for provider in reply.json["items"])


async def _source(fixture):
    table = f'"{fixture.control_schema}".retained_addresses'
    await fixture.connection.execute(f"""CREATE TABLE {table}(
      location_key varchar(64) PRIMARY KEY,entity_type varchar(64) NOT NULL,
      entity_id varchar(128) NOT NULL,postal_address text,
      plans_network_array integer[] NOT NULL,canonical_network_ids integer[])""")
    await fixture.connection.copy_records_to_table(
        "retained_addresses",
        schema_name=fixture.control_schema,
        records=[(letter * 64, "npi", value, "Synthetic office", [42], []) for letter, value in _NPI.items()],
    )
    await _full_source(fixture)
    await fixture.connection.execute(f"ALTER TABLE {table} DROP COLUMN postal_address")
    for letter, provider_id in _NPI.items():
        await fixture.connection.execute(
            f"UPDATE {table} SET npi=$1,first_line='123 Example Street',second_line=$2,address_key=$3 WHERE location_key=$4",
            int(provider_id),
            "Office " + letter.upper(),
            fixture.sites[letter],
            letter * 64,
        )


def _membership_input(fixture, office):
    """Encode exact C and selected-office membership input without receipts."""
    membership_records = [
        {
            "network_id": fixture.network_id,
            "provider_system": "npi",
            "provider_id": _NPI[letter],
            "location_id": str(fixture.sites[letter]),
            "evidence_id": "synthetic-office-" + letter,
        }
        for letter in ("c", office)
    ]
    return json.dumps(membership_records, separators=(",", ":")).encode()


async def _bind_offices(fixture, copy_target, office):
    """COPY only the exact selected provider/site/address bindings."""
    await fixture.connection.execute(f"""CREATE TABLE "{copy_target.schema_name}".provider_location_binding(
          provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
          location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
          PRIMARY KEY(provider_system,provider_id,location_id))""")
    await fixture.connection.copy_records_to_table(
        "provider_location_binding",
        schema_name=copy_target.schema_name,
        records=[
            (
                "npi",
                _NPI[letter],
                fixture.sites[letter],
                letter * 64,
                "npi",
                _NPI[letter],
            )
            for letter in ("c", office)
        ],
    )


async def _admit(fixture, office, expected_head):
    """Create, genuinely admit and seal two rows in the caller transaction."""
    candidate = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)),
        str(candidate),
        "network_candidate_" + candidate.hex,
    )
    fixture.additional_schemas.append(copy_target.schema_name)
    content = _membership_input(fixture, office)
    async with fixture.connection.transaction():
        await create_network_candidate(
            fixture.connection,
            copy_target,
            source_generations={"unified_address": fixture.address_source.generation_id},
            approved_custom_revision=fixture.approved_revision,
            expected_head=expected_head,
            expected_rows=2,
            control_schema=fixture.control_schema,
        )
        await _bind_offices(fixture, copy_target, office)
        receipt = await admit_network_membership_batch(
            fixture.connection,
            copy_target,
            batch_id=uuid4(),
            input_bytes=content,
            expected_input_sha256=hashlib.sha256(content).hexdigest(),
            control_schema=fixture.control_schema,
        )
        assert receipt.row_count == 2 and receipt.input_byte_count == len(content)
        assert receipt.input_sha256 == hashlib.sha256(content).hexdigest() and receipt.copy_byte_count > 21
        sealed = await seal_network_candidate(fixture.connection, copy_target, control_schema=fixture.control_schema)
        assert sealed["state"] == "sealed" and sealed["accepted_rows"] == sealed["expected_rows"] == 2
    return copy_target


async def _publish(fixture, copy_target):
    successor = SimpleNamespace(**vars(fixture) | {"copy_target": copy_target})
    await _loader_owned_candidate(successor)
    await fixture.connection.execute(f'SET ROLE "{fixture.writer_roles["publisher"]}"')
    try:
        return await prepare_and_publish_network_candidate(
            fixture.connection,
            copy_target,
            fixture.address_source,
            owner_role=fixture.writer_roles["owner"],
            loader_roles=(fixture.writer_roles["loader"],),
            reader_roles=(fixture.writer_roles["reader"],),
            control_schema=fixture.control_schema,
        )
    finally:
        await fixture.connection.execute("RESET ROLE")


def _http_context(fixture, reader_engine):
    """Use the genuine handler and assert public office fields directly."""
    app = _canonical_read_app(reader_engine, [])
    app.add_route(npi.get_near_npi, "/api/v1/npi/near/", methods=["GET"])
    return SimpleNamespace(app=app, fixture=fixture, reader_engine=reader_engine)


@pytest.fixture
async def admitted_nearby_app(serving_schema, monkeypatch):
    """Own genuine source/candidates and close readers before exact cleanup."""
    connection, schema, source_engine = serving_schema
    fixture = SimpleNamespace(
        connection=connection,
        observer=connection,
        control_schema=schema,
        additional_schemas=[],
        sites={letter: uuid4() for letter in _NPI},
        address_source=PinnedAddressSource(schema, "retained_addresses", "synthetic-office-edition"),
    )
    schema_identity_parts, identity_objects, reader_engine = None, (), None
    try:
        await _source(fixture)
        assert await connection.fetchval("SELECT to_regnamespace('mrf')") is None
        identity_metadata = _nearby_identity_metadata()
        async with source_engine.begin() as sql_connection:
            await sql_connection.execute(text('CREATE SCHEMA "mrf"'))
            await sql_connection.run_sync(identity_metadata.create_all)
            await sql_connection.execute(text('INSERT INTO "mrf".npi(npi) VALUES(1000000004),(1000000491)'))
            schema_identity_parts = tuple(
                (await sql_connection.execute(text("SELECT oid,nspowner FROM pg_namespace WHERE nspname='mrf'"))).one()
            )
            identity_objects = tuple(
                tuple(identity_object)
                for identity_object in (await sql_connection.execute(text(_NEARBY_IDENTITY_OBJECTS_SQL))).all()
            )
        actor = _actor()
        network = await _draft(source_engine, schema, _create("network"), actor)
        approval = await _approve(connection, schema, await _command(connection, schema, network), actor)
        fixture.network_id = int(network["record_id"])
        fixture.approved_revision = approval["approved_revision"]
        assert fixture.approved_revision > network["custom_revision"] > 0
        fixture.copy_target = await _admit(fixture, "a", 0)
        async with _candidate_writer_roles(fixture, freeze=False):
            for name in ("npi", "npi_taxonomy", "nucc_taxonomy"):
                await connection.execute(f'GRANT SELECT ON "mrf"."{name}" TO "{fixture.writer_roles["reader"]}"')
            await connection.execute(f'GRANT USAGE ON SCHEMA "mrf" TO "{fixture.writer_roles["reader"]}"')
            fixture.published_manifest = await _publish(fixture, fixture.copy_target)
            assert fixture.published_manifest["approved_custom_revision"] == fixture.approved_revision
            monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-network-nearby-token")
            reader_engine = _reader_engine(source_engine, fixture.writer_roles["reader"])
            http_context = _http_context(fixture, reader_engine)
            try:
                yield http_context
            finally:
                await reader_engine.dispose()
                reader_engine = None
    finally:
        try:
            if reader_engine is not None:
                await reader_engine.dispose()
        finally:
            try:
                for candidate_schema in reversed(fixture.additional_schemas):
                    await connection.execute(f'DROP SCHEMA IF EXISTS "{candidate_schema}" CASCADE')
                    assert await connection.fetchval("SELECT to_regnamespace($1)", candidate_schema) is None
            finally:
                if schema_identity_parts is not None:
                    await _remove_nearby_identity_schema(connection, schema_identity_parts, identity_objects)


async def _page(context, query_by_name, headers, expected_office, generation):
    first = await _nearby_http_reply(context, query_by_name, headers)
    _assert_nearby_page(first, generation=generation, provider_npi=1000000004, total_count=2)
    assert first.json["has_more"] and first.json["next_cursor"]
    second = await _nearby_http_reply(context, query_by_name | {"cursor": first.json["next_cursor"]}, headers)
    _assert_nearby_page(second, generation=generation, provider_npi=1000000491, total_count=2)
    assert second.json["items"][0]["second_line"] == "Office " + expected_office.upper()
    assert not second.json["has_more"]
    return first.json["next_cursor"]


async def _arrays(fixture, copy_target):
    return await fixture.connection.fetch(
        f'SELECT location_key,plans_network_array,canonical_network_ids FROM "{copy_target.schema_name}".entity_address_unified ORDER BY location_key'
    )


async def _assert_projection_immutability(fixture, successor, old_arrays, source_before):
    """Reread both immutable projections and unchanged source/legacy arrays."""
    new_arrays = await _arrays(fixture, successor)
    assert (
        [identity_object["plans_network_array"] for identity_object in old_arrays]
        == [identity_object["plans_network_array"] for identity_object in new_arrays]
        == [[42], [42], [42]]
    )
    assert [identity_object["canonical_network_ids"] for identity_object in old_arrays] == [
        [fixture.network_id],
        [],
        [fixture.network_id],
    ]
    assert [identity_object["canonical_network_ids"] for identity_object in new_arrays] == [
        [],
        [fixture.network_id],
        [fixture.network_id],
    ]
    assert (
        await fixture.connection.fetch(
            f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
        )
        == source_before
    )
    assert await _arrays(fixture, fixture.copy_target) == old_arrays


async def test_genuine_nearby_cutover_retains_old_office_and_reactivation(
    admitted_nearby_app,
):
    """Switch admitted office membership, retain old reads, then reactivate."""
    context, headers = (
        admitted_nearby_app,
        {
            "Authorization": "Bearer synthetic-network-nearby-token",
            "X-Network-Access-Scope": "a" * 64,
        },
    )
    fixture = context.fixture
    initial = fixture.published_manifest
    query_by_name = {
        "network_ids": str(fixture.network_id),
        "lat": "40.75",
        "long": "-73.98",
        "include_total": "1",
        "limit": "1",
    }
    retained = query_by_name | {"network_generation": str(initial["generation_id"])}
    old_cursor = await _page(context, retained, headers, "a", initial["generation_id"])
    unpinned_cursor = await _page(context, query_by_name, headers, "a", initial["generation_id"])
    source_before = await fixture.connection.fetch(
        f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
    )
    old_arrays = await _arrays(fixture, fixture.copy_target)
    successor = await _admit(fixture, "b", initial["generation_id"])
    sessions = async_sessionmaker(context.reader_engine.execution_options(isolation_level="REPEATABLE READ"))
    async with sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        reader = await _copy_driver(session)
        pin = await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema)
        assert pin.generation_id == initial["generation_id"]
        office_sql = f'SELECT second_line FROM "{pin.schema_name}".entity_address_unified WHERE npi=$1 AND canonical_network_ids @> $2::integer[]'
        assert await reader.fetchval(office_sql, 1000000491, [fixture.network_id]) == "Office A"
        current = await _publish(fixture, successor)
        assert await resolve_network_serving_manifest(reader, control_schema=fixture.control_schema) == pin
        assert await reader.fetchval(office_sql, 1000000491, [fixture.network_id]) == "Office A"
    old_second = await _nearby_http_reply(context, retained | {"cursor": old_cursor}, headers)
    _assert_nearby_page(
        old_second,
        generation=initial["generation_id"],
        provider_npi=1000000491,
        total_count=2,
    )
    assert old_second.json["items"][0]["second_line"] == "Office A"
    await _page(context, query_by_name, headers, "b", current["generation_id"])
    refused = await _nearby_http_reply(context, query_by_name | {"cursor": unpinned_cursor}, headers)
    assert refused.status == 400
    new_retained = query_by_name | {"network_generation": str(current["generation_id"])}
    await _page(context, new_retained, headers, "b", current["generation_id"])
    async with fixture.connection.transaction():
        rollback = await _reactivate(
            fixture, initial["generation_id"], current["generation_id"], approved=fixture.approved_revision
        )
    assert rollback.changed and rollback.generation_id == initial["generation_id"]
    await _page(context, query_by_name, headers, "a", initial["generation_id"])
    await _page(context, new_retained, headers, "b", current["generation_id"])
    await _assert_projection_immutability(fixture, successor, old_arrays, source_before)


def _nearby_identity_metadata():
    """Describe the three task-owned identity tables used by the nearby fixture."""
    identity_metadata = MetaData()
    for model in (NPIData, NPIDataTaxonomy, NUCCTaxonomy):
        model.__table__.to_metadata(identity_metadata, schema="mrf")
    return identity_metadata
