# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine PTG office approval reaches a mixed alias artifact and replay."""

import json
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text

from api.network_address_scope import network_address_read_scope
from api.network_provider_read import read_network_provider_page
from process import network_custom_address_source as custom
from process import registry_source_recipe_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.registry_candidate_composition import RegistryCompositionAddressSources
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from tests.test_network_custom_address_source_postgres import custom_db as base_custom_db
from tests.test_registry_approval_store_postgres import _actor, _approve, _command
from tests.test_registry_ptg_office_coordinator_postgres import coordinator_db as coordinator_db
from tests.test_registry_ptg_office_coordinator_postgres import office_db as office_db
from tests.test_registry_ptg_office_coordinator_postgres import office_publisher as office_publisher
from tests.test_registry_ptg_office_coordinator_postgres import serving_schema as serving_schema
from tests.test_registry_publication_ptg_runtime_postgres import (
    _cleanup_worker,
    _retained_pin,
    _review_offices,
    _worker,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def custom_db(base_custom_db):
    base_custom_db.source_aliases = True
    yield base_custom_db


async def _assert_management_read_profile(native):
    role = native.roles_by_kind["management"]
    protected = sorted(
        {
            native.custom.roles["owner"],
            native.fixture.roles[0],
            native.fixture.source.roles[0],
            native.fixture.source.roles[2],
            native.roles_by_kind["publisher"],
        }
    )
    assert not await native.custom.connection.fetchval(
        """SELECT EXISTS(SELECT FROM pg_roles protected WHERE rolname=ANY($2::text[]) AND
          (pg_has_role($1::name,protected.oid,'MEMBER') OR pg_has_role($1::name,protected.oid,'USAGE')
            OR pg_has_role($1::name,protected.oid,'SET'))) OR EXISTS(SELECT FROM pg_proc
          WHERE pg_has_role($1::name,proowner,'MEMBER'))""",
        role,
        protected,
    )


@pytest.fixture
async def alias_management_db(coordinator_db):
    native = coordinator_db
    connection, role, reader = (
        native.custom.connection,
        native.roles_by_kind["management"],
        native.fixture.retained_reader,
    )
    assert reader == native.custom.roles["reader"]
    identities_by_kind = dict(
        await connection.fetch("SELECT rolname,oid FROM pg_roles WHERE rolname=ANY($1::text[])", [role, reader])
    )
    assert len(identities_by_kind) == 2
    assert not await connection.fetchval("SELECT pg_has_role($1::name,$2::name,'MEMBER')", role, reader)
    retained = await _retained_pin(native)
    try:
        await connection.execute(f'GRANT "{reader}" TO "{role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE')
        await _assert_management_read_profile(native)
        assert await _retained_pin(native) == retained
        yield native
    finally:
        assert (
            dict(
                await connection.fetch("SELECT rolname,oid FROM pg_roles WHERE rolname=ANY($1::text[])", [role, reader])
            )
            == identities_by_kind
        )
        await connection.execute(f'REVOKE "{reader}" FROM "{role}"')
        assert not await connection.fetchval("SELECT pg_has_role($1::name,$2::name,'MEMBER')", role, reader)


async def _draft_alias(native, command, actor):
    async with native.service.management_sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        driver = (await (await session.connection()).get_raw_connection()).driver_connection
        identity = await driver.fetchrow("""SELECT current_user::text,session_user::text,rolcanlogin,
          rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls AS elevated
          FROM pg_roles WHERE rolname=current_user""")
        role = native.roles_by_kind["management"]
        assert tuple(identity) == (role, role, True, False)
        assert not await driver.fetchval("""SELECT EXISTS(SELECT FROM pg_proc
          WHERE pg_has_role(current_user,proowner,'MEMBER'))""")
        return await apply_registry_record_command(
            session,
            command,
            actor,
            schema=native.custom.control_schema,
            source_schema=native.custom.source_schema,
        )


async def _select_second_alias(native):
    fixture, actor = native.custom, _actor()
    first = native.request["selector"]["selections"][0]
    first_id = await fixture.connection.fetchval(
        f'SELECT location_id FROM "{fixture.control_schema}".registry_site_binding WHERE binding_id=$1',
        UUID(first["binding_id"]),
    )
    retained = native.fixture.serving
    alias_by_field = dict(
        await fixture.connection.fetchrow(
            f"""SELECT binding.provider_system,binding.provider_id,binding.location_id::text,binding.location_key,
          encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') AS address_row_sha256
          FROM "{retained.schema_name}".provider_location_binding binding
          JOIN "{retained.schema_name}".entity_address_unified address USING(location_key)
          WHERE binding.location_id<>$1 ORDER BY binding.location_id LIMIT 1""",
            first_id,
        )
    )
    site_record = await _draft_alias(
        native,
        RegistryRecordCommand(
            "site_binding",
            uuid4(),
            "create",
            0,
            {"source_generation": retained.generation_id, **alias_by_field},
            "Review second retained alias",
            uuid4().hex,
        ),
        actor,
    )
    await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, site_record),
        actor,
    )
    native.request["selector"]["selections"].append(
        {
            **first,
            "binding_id": site_record["record_id"],
            "binding_revision": site_record["revision"],
        }
    )
    native.request["selector"]["selections"].sort(key=lambda selection: selection["binding_id"])


def _composition(native, recipes, revision, retained, owner):
    return dict(
        request_id=uuid4(),
        approved_revision=revision,
        expected_head=retained.generation_id,
        source_manifest=retained,
        address_sources=RegistryCompositionAddressSources(
            PinnedAddressSource(retained.schema_name, "entity_address_unified", retained.manifest_sha256),
            replacement_source_recipes=recipes,
        ),
        writer_roles=dict(
            owner_role=owner,
            loader_roles=(),
            reader_roles=(native.custom.roles["reader"], native.roles_by_kind["publisher"]),
        ),
        control_schema=native.custom.control_schema,
    )


async def _assert_digest_replay(native, runtime, connection, parameters, receipt):
    namespace = receipt.address_source.schema_name
    original = await native.custom.connection.fetchval(
        "SELECT obj_description($1::oid,'pg_namespace')", receipt.schema_oid
    )
    forged = replace(receipt, ptg_carry_sha256="0" * 64)
    digest = custom._generation(forged)
    forged = replace(
        forged, generation_sha256=digest, address_source=replace(forged.address_source, generation_id=digest)
    )
    quoted = await native.custom.connection.fetchval("SELECT quote_literal($1::text)", json.dumps(forged.as_dict()))
    try:
        await native.custom.connection.execute(f'COMMENT ON SCHEMA "{namespace}" IS {quoted}')

        with pytest.raises(custom.NetworkCustomAddressSourceError, match="Retained composition recipe differs"):
            await runtime.compose(connection, **parameters)
    finally:
        quoted = await native.custom.connection.fetchval("SELECT quote_literal($1::text)", original)
        await native.custom.connection.execute(f'COMMENT ON SCHEMA "{namespace}" IS {quoted}')


async def _assert_published_aliases(native, connection, recipes, previous):
    manifest = await _retained_pin(native)
    assert manifest.generation_id > previous.generation_id
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await connection.fetchval(f'SELECT count(*) FROM "{manifest.schema_name}".network_membership_source') == 2
        )
        with network_address_read_scope(manifest, (recipes[0].source_pin.network_id,)) as scope:
            page = await read_network_provider_page(connection, scope, control_schema=native.custom.control_schema)
    assert page["total_count"] == 1 and len(page["providers"][0]["locations"]) == 1


async def _assert_standalone_refusals(connection, receipt):
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        for changed in (replace(receipt, source_aliases=False), replace(receipt, ptg_carry_sha256=None)):
            digest = custom._generation(changed)
            changed = replace(
                changed, generation_sha256=digest, address_source=replace(changed.address_source, generation_id=digest)
            )
            with pytest.raises(custom.NetworkCustomAddressSourceError, match="source alias layout differs"):
                await custom.verify_custom_address_source(connection, changed)


def _single_alias_page(actual_reader):
    """Exercise one-row alias pages through the unchanged native recipe reader."""

    async def read_page(connection, recipe, approved, cursor, schema, limit, verified=None, *, capture_facts=False):
        return await actual_reader(
            connection, recipe, approved, cursor, schema, 1, verified, capture_facts=capture_facts
        )

    return read_page


async def test_ptg_only_alias_artifact_prepare_replay_and_authentication(alias_management_db, monkeypatch):
    """Prepare, replay, authenticate and publish the genuine alias artifact without weakening refusals."""
    native = alias_management_db
    monkeypatch.setattr(composition, "_read_recipe_page", _single_alias_page(composition._read_recipe_page))
    await _select_second_alias(native)
    _, recipes, revision = await _review_offices(native)
    owner = "ptg_alias_owner_" + uuid4().hex
    executor = "ptg_alias_executor_" + uuid4().hex
    password = uuid4().hex
    created, engine, connection = [], None, None
    try:
        for role in (owner, executor):
            created.append(role)
            await native.custom.connection.execute(
                f'CREATE ROLE "{role}" ' + (f"LOGIN PASSWORD '{password}'" if role == executor else "NOLOGIN")
            )
        engine, runtime, connection = await _worker(native, executor, owner, password)
        retained = await _retained_pin(native)
        parameters = _composition(native, recipes, revision, retained, owner)
        copy_target, addresses = await runtime.compose(connection, **parameters)
        assert await runtime.compose(connection, **parameters) == (copy_target, addresses)
        stored = await native.custom.connection.fetchval(
            "SELECT obj_description(to_regnamespace($1),'pg_namespace')", addresses.schema_name
        )
        receipt = custom._from_document(stored)
        assert receipt.source_aliases and receipt.as_dict()["revision"] == 5 and receipt.ptg_carry_sha256 is not None
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await custom.verify_custom_address_source(connection, receipt) == receipt
            assert (
                await connection.fetchval(f'SELECT count(*) FROM "{addresses.schema_name}".provider_location_binding')
                == 2
            )
            assert (
                await connection.fetchval(
                    f'SELECT count(DISTINCT location_key) FROM "{addresses.schema_name}".provider_location_binding'
                )
                == 1
            )
        await _assert_standalone_refusals(connection, receipt)
        await _assert_digest_replay(native, runtime, connection, parameters, receipt)
        invalid = replace(recipes[0], source_pin=replace(recipes[0].source_pin, hold_sha256="0" * 64))
        with pytest.raises(ValueError):
            await runtime.compose(connection, **_composition(native, (invalid,), revision, retained, owner))
        await prepare_and_publish_network_candidate(
            connection,
            copy_target,
            addresses,
            **parameters["writer_roles"],
            control_schema=native.custom.control_schema,
        )
        await _assert_published_aliases(native, connection, recipes, retained)
    finally:
        await _cleanup_worker(native, connection, engine, created)
