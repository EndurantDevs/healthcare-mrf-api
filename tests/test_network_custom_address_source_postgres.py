# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protected custom office artifacts from actual approved records and full addresses."""

import asyncio
import importlib.util
import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker

from db.models import EntityAddressUnified, NPIData
from process import network_bootstrap_read_access as bootstrap_access
from process.network_address_projection import PinnedAddressSource
from process.network_bootstrap_sources import (
    NetworkBootstrapSourceError,
    NetworkBootstrapSourceSpecification,
    prepare_network_bootstrap_sources,
    verify_network_bootstrap_sources,
)
from process.network_custom_address_source import (
    NetworkCustomAddressSourceError,
    NetworkCustomAddressUnresolved,
    _artifact_dependency_identity,
    _source_identity,
    prepare_custom_address_source,
    verify_custom_address_source,
)
from process.registry_record_store import apply_registry_record_command
from tests.test_manual_location_identity_store_postgres import _create as _location
from tests.test_manual_provider_identity_store_postgres import _create as _provider
from tests.test_network_membership_draft_store_postgres import _command as _membership
from tests.test_network_membership_draft_store_postgres import _member
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _actor, _approve, _command, _create

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def custom_db(serving_schema):
    connection, control_schema, engine = serving_schema
    token = uuid4().hex
    source_schema = "custom_source_test_" + token
    role_names_by_kind = {kind: "custom_" + kind + "_" + token for kind in ("owner", "reader", "outsider")}
    composition_ids = []
    created_roles = []
    try:
        for role in role_names_by_kind.values():
            await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
            created_roles.append(role)
        await connection.execute(f'CREATE SCHEMA "{source_schema}"')
        async with engine.begin() as setup:
            for model in (EntityAddressUnified, NPIData):
                table = model.__table__.to_metadata(MetaData(), schema=source_schema)
                await setup.run_sync(lambda sync, table=table: table.create(sync))
        await connection.execute(
            f"""INSERT INTO "{source_schema}".entity_address_unified
          (location_key,entity_type,entity_id,npi,checksum,type,first_line,city_name,state_name,postal_code,country_code,
            plans_network_array,procedures_array,medications_array,canonical_network_ids)
          VALUES($1,'npi','1000000004',1000000004,42,'practice','123 Example Street','Sample City','CA','90210','US','{{42}}','{{123}}','{{456}}','{{42}}')""",
            "a" * 64,
        )
        await connection.execute(f'INSERT INTO "{source_schema}".npi(npi) VALUES(1000000004)')
        await connection.execute(f'ALTER SCHEMA "{source_schema}" OWNER TO "{role_names_by_kind["owner"]}"')
        for name in ("entity_address_unified", "npi"):
            await connection.execute(f'ALTER TABLE "{source_schema}".{name} OWNER TO "{role_names_by_kind["owner"]}"')
        await connection.execute(f'GRANT USAGE ON SCHEMA "{source_schema}" TO "{role_names_by_kind["reader"]}"')
        await connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{source_schema}" TO "{role_names_by_kind["reader"]}"'
        )
        yield SimpleNamespace(
            connection=connection,
            control_schema=control_schema,
            source_schema=source_schema,
            engine=engine,
            roles=role_names_by_kind,
            composition_ids=composition_ids,
            base=PinnedAddressSource(source_schema, "entity_address_unified", "retained-address-edition-a"),
        )
    finally:
        await connection.execute("RESET ROLE")
        for identity in composition_ids:
            await connection.execute(f'DROP SCHEMA IF EXISTS "network_composition_{identity.hex}" CASCADE')
            assert (
                await connection.fetchval("SELECT to_regnamespace($1)", "network_composition_" + identity.hex) is None
            )
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        for role in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


@asynccontextmanager
async def _restricted_artifact_roles(connection):
    """Own exact artifact actors before any source admits their metadata access."""
    role_suffix = uuid4().hex
    password = uuid4().hex
    role_by_kind = {
        kind: "artifact_" + kind + "_" + role_suffix for kind in ("loading", "execution", "custodian", "custody")
    }
    created_oids_by_role = {}
    try:
        for kind, role in role_by_kind.items():
            assert await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role) is None
            options = f" LOGIN PASSWORD '{password}'" if kind in {"execution", "custodian"} else " NOLOGIN"
            async with connection.transaction():
                await connection.execute(f'CREATE ROLE "{role}"{options}')
                created_oids_by_role[role] = await connection.fetchval(
                    "SELECT oid FROM pg_roles WHERE rolname=$1", role
                )
        for target_role, member, inherit, set_role in (
            (role_by_kind["loading"], role_by_kind["execution"], "TRUE", "TRUE"),
            (role_by_kind["execution"], role_by_kind["custodian"], "TRUE", "TRUE"),
            (role_by_kind["custody"], role_by_kind["custodian"], "FALSE", "TRUE"),
        ):
            await connection.execute(
                f'GRANT "{target_role}" TO "{member}" WITH ADMIN FALSE, INHERIT {inherit}, SET {set_role}'
            )
        yield SimpleNamespace(roles=role_by_kind, password=password, role_oids=created_oids_by_role)
    finally:
        for role, oid in reversed(tuple(created_oids_by_role.items())):
            actual = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role)
            if actual is not None:
                assert actual == oid
                await connection.execute(f'DROP OWNED BY "{role}"')
                await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", list(created_oids_by_role)
        )


@asynccontextmanager
async def _restricted_artifact_context(fixture, *, actors=None):
    """Use genuine E and K logins with E-to-loading and SET-only K-to-custody edges."""
    from urllib.parse import urlsplit, urlunsplit

    async with AsyncExitStack() as stack:
        if actors is None:
            actors = await stack.enter_async_context(_restricted_artifact_roles(fixture.connection))
        role_by_kind, password = actors.roles, actors.password
        assert set(role_by_kind) == {"loading", "execution", "custodian", "custody"}
        assert {
            role: await fixture.connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role)
            for role in role_by_kind.values()
        } == actors.role_oids
        database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
        await fixture.connection.execute(f'GRANT CREATE ON DATABASE {database} TO "{role_by_kind["loading"]}"')
        await fixture.connection.execute(
            f'GRANT "{fixture.roles["reader"]}" TO "{role_by_kind["execution"]}" '
            "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
        )
        await fixture.connection.execute(
            f'GRANT USAGE ON SCHEMA "{fixture.control_schema}" TO "{role_by_kind["execution"]}"'
        )
        await fixture.connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.control_schema}" TO "{role_by_kind["execution"]}"'
        )
        await fixture.connection.execute(
            f'GRANT USAGE ON SCHEMA "{fixture.source_schema}" TO "{role_by_kind["loading"]}"'
        )
        await fixture.connection.execute(
            f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.source_schema}" TO "{role_by_kind["loading"]}"'
        )
        parsed = urlsplit(os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://"))
        connections = []
        try:
            for kind in ("custodian", "execution"):
                dsn = urlunsplit(
                    (
                        parsed.scheme,
                        role_by_kind[kind] + "@" + parsed.netloc.split("@")[-1],
                        parsed.path,
                        parsed.query,
                        "",
                    )
                )
                connections.append(await asyncpg.connect(dsn, password=password))
            yield SimpleNamespace(
                base=fixture,
                roles=role_by_kind,
                custodian=connections[0],
                execution=connections[1],
                runtime_roles=tuple(
                    sorted((role_by_kind["execution"], role_by_kind["loading"], fixture.roles["reader"]))
                ),
            )
        finally:
            for connection in connections:
                await connection.close()
            for identity in fixture.composition_ids:
                await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "network_composition_{identity.hex}" CASCADE')


@pytest.fixture
async def restricted_artifact_db(custom_db):
    """Keep generic retained-source read permissions explicit before native preparation."""
    async with _restricted_artifact_context(custom_db) as fixture:
        yield fixture


@pytest.mark.parametrize("observed_oid", [None, 99])
async def test_precreated_artifact_actor_identity_refuses_before_grants(observed_oid):
    """A missing or replaced independent actor cannot acquire any fixture rights."""
    roles_by_kind = {kind: "fixture_" + kind for kind in ("loading", "execution", "custodian", "custody")}
    oids_by_role = {role: index for index, role in enumerate(roles_by_kind.values(), 1)}
    connection = SimpleNamespace(
        fetchval=AsyncMock(
            side_effect=lambda _sql, role: observed_oid if role == roles_by_kind["custodian"] else oids_by_role[role]
        ),
        execute=AsyncMock(),
    )
    actors = SimpleNamespace(roles=roles_by_kind, password=uuid4().hex, role_oids=oids_by_role)
    with pytest.raises(AssertionError):
        async with _restricted_artifact_context(SimpleNamespace(connection=connection), actors=actors):
            pytest.fail("Unverified actors cannot enter artifact preparation")
    connection.execute.assert_not_awaited()


@pytest.fixture
async def restricted_bootstrap_artifact_db(bootstrap_db, serving_schema):
    """The admitted bootstrap grants only E, while the output requires the complete actor census."""
    base = SimpleNamespace(
        connection=bootstrap_db.connection,
        engine=bootstrap_db.engine,
        control_schema=serving_schema[1],
        source_schema=bootstrap_db.source_schema,
        roles=bootstrap_db.roles,
        composition_ids=[],
    )
    async with _restricted_artifact_context(base) as fixture:
        capture = await _bootstrap_prepare(
            bootstrap_db, await _bootstrap_specification(bootstrap_db), runtime_roles=(fixture.roles["execution"],)
        )
        base.base = capture.address_source
        fixture.bootstrap = capture
        yield fixture


async def _draft(fixture, command, actor):
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        return await apply_registry_record_command(
            session, command, actor, schema=fixture.control_schema, source_schema=fixture.source_schema
        )


async def _seed(fixture, *, system="manual"):
    actor = _actor()
    network = await _draft(fixture, _create("network"), actor)
    provider = await _draft(fixture, _provider(), actor)
    location_command = _location()
    first = await _draft(fixture, location_command, actor)
    second = await _draft(
        fixture,
        _location(
            fields={
                **location_command.fields,
                "address_json": {**location_command.fields["address_json"], "second_line": "Suite 3"},
            }
        ),
        actor,
    )
    provider_id = provider["record_id"] if system == "manual" else "1000000004"
    membership_command = _membership(
        network["record_id"], [_member(network["record_id"], provider_id, first["record_id"], system=system)]
    )
    membership = await _draft(fixture, membership_command, actor)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, network, provider, first, second, membership),
        actor,
    )
    return SimpleNamespace(
        actor=actor,
        network=network,
        provider=provider,
        first=first,
        second=second,
        membership=membership,
        membership_command=membership_command,
        location_command=location_command,
        revision=approval["approved_revision"],
    )


def _arguments(fixture, revision, **changes):
    composition_id = uuid4()
    fixture.composition_ids.append(composition_id)
    return {
        "composition_id": str(composition_id),
        "approved_revision": revision,
        "owner_role": fixture.roles["owner"],
        "runtime_roles": (fixture.roles["reader"],),
        "control_schema": fixture.control_schema,
        **changes,
    }


async def _prepare(fixture, revision, *, base=None, arguments=None):
    arguments = arguments or _arguments(fixture, revision)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        return await prepare_custom_address_source(fixture.connection, base or fixture.base, **arguments)


async def _verify(fixture, receipt):
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        return await verify_custom_address_source(fixture.connection, receipt)


async def test_manual_without_npi_selected_office_and_pending_correction(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    pending = replace(
        seed.location_command,
        operation="correct",
        expected_revision=1,
        fields={
            **seed.location_command.fields,
            "address_json": {**seed.location_command.fields["address_json"], "second_line": "Suite 99"},
        },
        idempotency_key=uuid4().hex,
    )
    await _draft(fixture, pending, seed.actor)
    receipt = await _prepare(fixture, seed.revision)
    namespace = '"' + receipt.address_source.schema_name + '"'
    addresses = await fixture.connection.fetch(
        f"SELECT * FROM {namespace}.entity_address_unified WHERE row_origin='network_custom'"
    )
    assert len(addresses) == receipt.custom_pair_count == 1
    address = addresses[0]
    assert (
        address["entity_type"] == "manual"
        and address["entity_id"] == seed.provider["record_id"]
        and address["npi"] is None
    )
    assert address["entity_name"] == seed.provider["record"]["display_name"] and address["type"] == "practice"
    assert address["second_line"] == "Suite 2" and address["address_key"] == UUID(
        seed.first["record"]["canonical_address_json"]["address_key"]
    )
    for column in (
        "address_sources",
        "source_record_ids",
        "aca_plan_array",
        "ptg_plan_array",
        "plans_network_array",
        "procedures_array",
        "medications_array",
        "canonical_network_ids",
        "taxonomy_array",
    ):
        assert address[column] == []
    bindings = await fixture.connection.fetch(f"SELECT * FROM {namespace}.provider_location_binding")
    assert len(bindings) == 1 and str(bindings[0]["location_id"]) == seed.first["record_id"]
    assert seed.second["record_id"] not in json.dumps([dict(binding) for binding in bindings], default=str)
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.entity_address_unified") == 2
    assert await _verify(fixture, receipt) == receipt


@pytest.mark.parametrize("explicit_pin", [False, True])
async def test_known_retained_npi_reuses_exact_site(custom_db, explicit_pin):
    seed = await _seed(custom_db, system="npi")
    arguments = _arguments(custom_db, seed.revision)
    if explicit_pin:
        arguments["npi_source"] = PinnedAddressSource(custom_db.source_schema, "npi", "retained-npi-edition-a")
    receipt = await _prepare(custom_db, seed.revision, arguments=arguments)
    address = await custom_db.connection.fetchrow(
        f"SELECT * FROM \"{receipt.address_source.schema_name}\".entity_address_unified WHERE row_origin='network_custom'"
    )
    assert address["entity_type"] == "npi" and address["entity_id"] == "1000000004" and address["npi"] == 1000000004
    assert address["entity_name"] == "NPI 1000000004" and receipt.npi_oids is not None
    assert receipt.npi_source.generation_id == (
        "retained-npi-edition-a" if explicit_pin else custom_db.base.generation_id
    )


async def test_latest_approved_replaces_custom_rows_from_retained_older_base(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    first = await _prepare(fixture, seed.revision)
    clear = replace(
        seed.membership_command,
        operation="correct",
        expected_revision=1,
        fields={"memberships_json": []},
        idempotency_key=uuid4().hex,
    )
    cleared = await _draft(fixture, clear, seed.actor)
    approval = await _approve(
        fixture.connection,
        fixture.control_schema,
        await _command(fixture.connection, fixture.control_schema, cleared),
        seed.actor,
    )
    second = await _prepare(fixture, approval["approved_revision"], base=first.address_source)
    assert second.custom_pair_count == 0
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{second.address_source.schema_name}".entity_address_unified'
        )
        == 1
    )
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{first.address_source.schema_name}".entity_address_unified'
        )
        == 2
    )


async def test_exact_replay_is_read_only_and_changed_recipe_rejected(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    receipt = await _prepare(fixture, seed.revision, arguments=arguments)
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    try:
        assert await _prepare(fixture, seed.revision, arguments=arguments) == receipt
        await asyncio.sleep(0)
        assert not any(
            query.query.lstrip()
            .upper()
            .startswith(("CREATE", "ALTER", "INSERT", "DELETE", "UPDATE", "COMMENT", "GRANT", "REVOKE"))
            for query in logged_queries
        )
        with pytest.raises(NetworkCustomAddressSourceError, match="recipe differs"):
            await _prepare(
                fixture,
                seed.revision,
                base=replace(fixture.base, generation_id="different-edition"),
                arguments=arguments,
            )
    finally:
        fixture.connection.remove_query_logger(logged_queries.append)
    assert await _verify(fixture, receipt) == receipt


@pytest.mark.parametrize(
    "tamper",
    [
        "source_write",
        "artifact_write",
        "column_write",
        "owner_member",
        "unknown_reader",
        "replace_heap",
        "comment",
        "shape",
        "approved",
        "lookup_missing",
        "lookup_replaced",
        "lookup_shape",
    ],
)
async def test_native_tampering_rejected(custom_db, tamper):
    fixture = custom_db
    seed = await _seed(fixture)
    receipt = await _prepare(fixture, seed.revision)
    namespace = '"' + receipt.address_source.schema_name + '"'
    statements_by_tamper = {
        "source_write": f'GRANT UPDATE ON "{fixture.source_schema}".entity_address_unified TO "{fixture.roles["reader"]}"',
        "artifact_write": f'GRANT UPDATE ON {namespace}.entity_address_unified TO "{fixture.roles["reader"]}"',
        "column_write": f'GRANT UPDATE(first_line) ON {namespace}.entity_address_unified TO "{fixture.roles["reader"]}"',
        "owner_member": f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["reader"]}"',
        "unknown_reader": f'GRANT SELECT ON {namespace}.provider_location_binding TO "{fixture.roles["outsider"]}"',
        "comment": f"COMMENT ON SCHEMA {namespace} IS '{{}}'",
        "shape": f"ALTER TABLE {namespace}.entity_address_unified ADD COLUMN unexpected text",
        "approved": f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{display_name}}','\"Changed approved\"') WHERE record_kind='provider' AND approved_revision={seed.revision}",
    }
    if tamper.startswith("lookup_"):
        await fixture.connection.execute(f"DROP INDEX {namespace}.entity_address_unified_network_office_lookup")
        if tamper != "lookup_missing":
            columns = "npi,address_key" if tamper == "lookup_replaced" else "address_key,npi"
            await fixture.connection.execute(
                f"CREATE INDEX entity_address_unified_network_office_lookup ON {namespace}.entity_address_unified({columns})"
            )
    elif tamper == "replace_heap":
        await fixture.connection.execute(
            f"ALTER TABLE {namespace}.provider_location_binding RENAME TO previous_binding"
        )
        await fixture.connection.execute(
            f"CREATE TABLE {namespace}.provider_location_binding (LIKE {namespace}.previous_binding INCLUDING ALL)"
        )
    else:
        await fixture.connection.execute(statements_by_tamper[tamper])
    with pytest.raises(NetworkCustomAddressSourceError):
        await _verify(fixture, receipt)


async def test_exact_office_lookup_prepared_before_freeze_uses_native_index(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f'''INSERT INTO "{fixture.source_schema}".entity_address_unified
        (location_key,entity_type,entity_id,npi,checksum,type,address_precision,address_key)
        SELECT md5(ordinal::text)||md5(ordinal::text),'npi',(1000000004+ordinal)::text,
        1000000004+ordinal,ordinal,'practice','street',md5(ordinal::text)::uuid
        FROM generate_series(1,10000) ordinal'''
    )
    receipt = await _prepare(fixture, seed.revision)
    assert receipt.office_lookup_index_oid > 0 and receipt.as_dict()["revision"] == 2
    plan = await fixture.connection.fetchval(
        f'''EXPLAIN(FORMAT JSON) SELECT location_key FROM "{receipt.address_source.schema_name}".entity_address_unified
        WHERE npi=$1 AND address_key=$2 AND entity_type='npi' AND entity_id=$3
          AND type='practice' AND address_precision='street' AND inferred_npi IS NULL''',
        1000004004,
        UUID("1bd69c7df3112fb9a584fbd9edfc6c90"),
        "1000004004",
    )
    assert "entity_address_unified_network_office_lookup" in plan
    assert (
        await fixture.connection.fetchval(
            f'''SELECT location_key FROM "{receipt.address_source.schema_name}".entity_address_unified
        WHERE npi=1000004004 AND address_key='1bd69c7df3112fb9a584fbd9edfc6c90'::uuid'''
        )
        == "1bd69c7df3112fb9a584fbd9edfc6c90" * 2
    )
    assert await _verify(fixture, receipt) == receipt


async def test_unprotected_source_rejected_before_ddl_and_caller_rollback(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    await fixture.connection.execute(f'GRANT INSERT ON "{fixture.source_schema}".entity_address_unified TO PUBLIC')
    with pytest.raises(NetworkCustomAddressSourceError, match="privileges"):
        await _prepare(fixture, seed.revision, arguments=arguments)
    schema_name = "network_composition_" + UUID(arguments["composition_id"]).hex
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema_name) is None
    await fixture.connection.execute(f'REVOKE INSERT ON "{fixture.source_schema}".entity_address_unified FROM PUBLIC')
    transaction = fixture.connection.transaction(isolation="repeatable_read")
    await transaction.start()
    receipt = await prepare_custom_address_source(fixture.connection, fixture.base, **arguments)
    await transaction.rollback()
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", receipt.address_source.schema_name) is None


async def test_runtime_role_cannot_write_or_replace(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    receipt = await _prepare(fixture, seed.revision)
    namespace = '"' + receipt.address_source.schema_name + '"'
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["reader"]}"')
    try:
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.entity_address_unified") == 2
        for statement in (
            f"UPDATE {namespace}.entity_address_unified SET first_line='Changed'",
            f"CREATE TABLE {namespace}.extra(id int)",
            f"DROP TABLE {namespace}.provider_location_binding",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.connection.execute(statement)
    finally:
        await fixture.connection.execute("RESET ROLE")


async def test_directory_custom_site_is_explicitly_unresolved(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{memberships_json,0,provider_system}}','\"provider_directory\"') WHERE record_kind='membership' AND approved_revision=$1",
        seed.revision,
    )
    arguments = _arguments(fixture, seed.revision)
    with pytest.raises(NetworkCustomAddressUnresolved):
        await _prepare(fixture, seed.revision, arguments=arguments)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
        )
        is None
    )


async def test_isolation_and_revision_are_explicit(custom_db):
    seed = await _seed(custom_db)
    arguments = _arguments(custom_db, seed.revision)
    with pytest.raises(NetworkCustomAddressSourceError, match="repeatable-read"):
        async with custom_db.connection.transaction():
            await prepare_custom_address_source(custom_db.connection, custom_db.base, **arguments)
    with pytest.raises(NetworkCustomAddressSourceError, match="Approved revision"):
        await _prepare(custom_db, seed.revision, arguments={**arguments, "approved_revision": seed.revision - 1})


async def _logged_prepare(fixture, revision):
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    try:
        receipt = await _prepare(fixture, revision)
        await asyncio.sleep(0)
        return receipt, len(logged_queries)
    finally:
        fixture.connection.remove_query_logger(logged_queries.append)


async def test_bulk_approved_pairs_have_fixed_native_query_count(custom_db):
    """Expand the actual approved-map fixture natively; no draft heads participate."""
    fixture = custom_db
    seed = await _seed(fixture)
    # Warm the native driver type cache before comparing application query counts.
    await _prepare(fixture, seed.revision)
    small, small_count = await _logged_prepare(fixture, seed.revision)
    await fixture.connection.execute(
        f'''WITH identities AS (SELECT gen_random_uuid()::text AS provider_id FROM generate_series(1,4999)),
        added AS (INSERT INTO "{fixture.control_schema}".registry_approved_record
          (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json)
          SELECT original.approved_revision,'provider',identities.provider_id,original.record_revision,
                 original.custom_revision,jsonb_set(original.record_json,'{{provider_id}}',to_jsonb(identities.provider_id))
          FROM identities CROSS JOIN "{fixture.control_schema}".registry_approved_record original
          WHERE original.approved_revision=$1 AND original.record_kind='provider' RETURNING record_key)
        UPDATE "{fixture.control_schema}".registry_approved_record membership
        SET record_json=jsonb_set(membership.record_json,'{{memberships_json}}',
          membership.record_json->'memberships_json'||(SELECT jsonb_agg(jsonb_set(
            membership.record_json->'memberships_json'->0,'{{provider_id}}',to_jsonb(record_key))) FROM added))
        WHERE membership.approved_revision=$1 AND membership.record_kind='membership' ''',
        seed.revision,
    )
    large, large_count = await _logged_prepare(fixture, seed.revision)
    assert small.custom_pair_count == 1 and large.custom_pair_count == 5000
    assert small_count == large_count
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{large.address_source.schema_name}".provider_location_binding'
        )
        == 5000
    )


async def test_native_identity_collision_rolls_back_whole_artifact(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f'''UPDATE "{fixture.source_schema}".entity_address_unified SET location_key=
        encode(sha256(convert_to(jsonb_build_array('network_custom','manual',$1::text,$2::uuid)::text,'UTF8')),'hex')''',
        seed.provider["record_id"],
        UUID(seed.first["record_id"]),
    )
    arguments = _arguments(fixture, seed.revision)
    with pytest.raises(NetworkCustomAddressSourceError, match="native preparation"):
        await _prepare(fixture, seed.revision, arguments=arguments)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
        )
        is None
    )
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.source_schema}".entity_address_unified') == 1
    )


async def test_missing_retained_npi_rolls_back_artifact(custom_db):
    fixture = custom_db
    seed = await _seed(fixture, system="npi")
    await fixture.connection.execute(f'DELETE FROM "{fixture.source_schema}".npi')
    arguments = _arguments(fixture, seed.revision)
    with pytest.raises(NetworkCustomAddressUnresolved, match="source NPI"):
        await _prepare(fixture, seed.revision, arguments=arguments)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
        )
        is None
    )


async def _create_bootstrap_parents(engine, source_schema):
    """Apply actual generation migrations before creating the two mutable heaps."""
    async with engine.begin() as setup:
        for filename in (
            "20260914100000_entity_address_result_generation.py",
            "20260914120000_npi_result_generation.py",
        ):
            migration_path = Path(__file__).parents[1] / "alembic/versions" / filename
            module_spec = importlib.util.spec_from_file_location(filename, migration_path)
            migration = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(migration)

            def upgrade(sync_connection, migration=migration):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()

            await setup.run_sync(upgrade)
        for model in (EntityAddressUnified, NPIData):
            table = model.__table__.to_metadata(MetaData(), schema=source_schema)
            await setup.run_sync(lambda sync, table=table: table.create(sync))


@pytest.fixture
async def bootstrap_db(serving_schema, monkeypatch):
    """Initialize real generation-zero migrations before loading mutable parents."""
    connection, _, engine = serving_schema
    token = uuid4().hex
    source_schema = "bootstrap_parent_" + token
    roles_by_kind = {kind: "bootstrap_" + kind + "_" + token for kind in ("owner", "loader", "reader", "publisher")}
    created_roles, bootstrap_ids = [], []
    try:
        for role_name in roles_by_kind.values():
            created_roles.append(role_name)
            await connection.execute(f'CREATE ROLE "{role_name}" NOLOGIN')
        database_name = await connection.fetchval("SELECT quote_ident(current_database())")
        for kind in ("owner", "publisher"):
            await connection.execute(f'GRANT CREATE ON DATABASE {database_name} TO "{roles_by_kind[kind]}"')
        await connection.execute(
            f'GRANT "{roles_by_kind["loader"]}","{roles_by_kind["owner"]}" TO "{roles_by_kind["publisher"]}"'
        )
        await connection.execute(f'CREATE SCHEMA "{source_schema}"')
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", source_schema)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        await _create_bootstrap_parents(engine, source_schema)
        await connection.execute(f'ALTER SCHEMA "{source_schema}" OWNER TO "{roles_by_kind["loader"]}"')
        for table_name in (
            "entity_address_unified",
            "npi",
            "entity_address_result_generation",
            "npi_result_generation",
        ):
            await connection.execute(f'ALTER TABLE "{source_schema}".{table_name} OWNER TO "{roles_by_kind["loader"]}"')
        await connection.execute(f'''INSERT INTO "{source_schema}".entity_address_unified
          (location_key,entity_type,entity_id,npi,checksum,type,first_line)
          VALUES(repeat('a',64),'npi','1000000004',1000000004,1,'practice','123 Example Street')''')
        await connection.execute(
            f"INSERT INTO \"{source_schema}\".npi(npi,provider_first_name) VALUES(1000000004,'Example')"
        )
        yield SimpleNamespace(
            connection=connection,
            engine=engine,
            source_schema=source_schema,
            roles=roles_by_kind,
            bootstrap_ids=bootstrap_ids,
        )
    finally:
        await connection.execute("RESET ROLE")
        for identity in bootstrap_ids:
            namespace = "network_bootstrap_" + identity.hex
            await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
            assert await connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
        await connection.execute(f'DROP SCHEMA IF EXISTS "{source_schema}" CASCADE')
        for role_name in reversed(created_roles):
            await connection.execute(f'DROP OWNED BY "{role_name}"')
            await connection.execute(f'DROP ROLE "{role_name}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


async def _bootstrap_specification(fixture):
    identity = uuid4()
    fixture.bootstrap_ids.append(identity)
    relation_oids = await fixture.connection.fetchrow(
        "SELECT to_regclass($1)::oid::bigint AS address_oid,to_regclass($2)::oid::bigint AS npi_oid",
        f'"{fixture.source_schema}".entity_address_unified',
        f'"{fixture.source_schema}".npi',
    )
    return NetworkBootstrapSourceSpecification(
        str(identity),
        fixture.source_schema,
        fixture.source_schema,
        relation_oids["address_oid"],
        relation_oids["npi_oid"],
    )


async def _bootstrap_prepare(fixture, specification, *, runtime_roles=None):
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["publisher"]}"')
        return await prepare_network_bootstrap_sources(
            fixture.connection,
            specification,
            owner_role=fixture.roles["owner"],
            runtime_roles=runtime_roles or tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
        )


async def _assert_bootstrap_read_only(fixture, namespace):
    """The ordinary loader may read, but cannot alter a closed artifact."""
    await fixture.connection.execute(f'SET ROLE "{fixture.roles["loader"]}"')
    try:
        for statement in (
            f"UPDATE {namespace}.npi SET provider_first_name='Forbidden'",
            f"CREATE TABLE {namespace}.extra(id int)",
            f"DROP TABLE {namespace}.npi",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.connection.execute(statement)
    finally:
        await fixture.connection.execute("RESET ROLE")


async def test_bootstrap_same_snapshot_allows_concurrent_parent_writes_and_exact_replay(bootstrap_db, monkeypatch):
    """Loader writes commit during capture while both copied heaps retain one old snapshot."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    captured = asyncio.Event()
    written = asyncio.Event()
    original_content = bootstrap._content

    async def capture_content(connection, schema_name, table_name):
        if schema_name == fixture.source_schema and table_name == "entity_address_unified":
            captured.set()
            await asyncio.wait_for(written.wait(), 3)
        return await original_content(connection, schema_name, table_name)

    monkeypatch.setattr(bootstrap, "_content", capture_content)
    prepared = asyncio.create_task(_bootstrap_prepare(fixture, specification))
    await asyncio.wait_for(captured.wait(), 3)
    async with fixture.engine.begin() as writer:
        from sqlalchemy import text

        await writer.execute(text(f'SET LOCAL ROLE "{fixture.roles["loader"]}"'))
        await writer.execute(
            text(f'''UPDATE "{fixture.source_schema}".entity_address_unified SET first_line='Changed Street' ''')
        )
        await writer.execute(text(f'''UPDATE "{fixture.source_schema}".npi SET provider_first_name='Changed' '''))
        assert (
            await writer.execute(
                text("SELECT to_regnamespace(:namespace)"),
                {"namespace": "network_bootstrap_" + UUID(specification.bootstrap_id).hex},
            )
        ).scalar() is None
    written.set()
    receipt = await asyncio.wait_for(prepared, 5)
    namespace = '"' + receipt.address_source.schema_name + '"'
    assert (
        await fixture.connection.fetchval(f"SELECT first_line FROM {namespace}.entity_address_unified")
        == "123 Example Street"
    )
    assert await fixture.connection.fetchval(f"SELECT provider_first_name FROM {namespace}.npi") == "Example"
    assert all(json.loads(parent.authority_json)["local_generation"] == 0 for parent in receipt.parent_receipts)
    assert all(json.loads(parent.authority_json)["serving_generation"] is None for parent in receipt.parent_receipts)
    statements = []
    fixture.connection.add_query_logger(statements.append)
    try:
        assert await _bootstrap_prepare(fixture, specification) == receipt
        await asyncio.sleep(0)
        assert not any(
            query.query.lstrip()
            .upper()
            .startswith(("CREATE", "ALTER", "INSERT", "UPDATE", "COMMENT", "GRANT", "REVOKE"))
            for query in statements
        )
    finally:
        fixture.connection.remove_query_logger(statements.append)
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        assert await verify_network_bootstrap_sources(fixture.connection, receipt) == receipt
    await _assert_bootstrap_read_only(fixture, namespace)


async def test_bootstrap_preserves_appended_network_column_and_exact_native_shape(bootstrap_db):
    """A column appended by an additive migration retains all named payload values."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    parent = f'"{fixture.source_schema}".entity_address_unified'
    await fixture.connection.execute(f"UPDATE {parent} SET canonical_network_ids=ARRAY[7,11]")
    await fixture.connection.execute(f"ALTER TABLE {parent} RENAME COLUMN canonical_network_ids TO prior_network_ids")
    await fixture.connection.execute(
        f"ALTER TABLE {parent} ADD COLUMN canonical_network_ids integer[] NOT NULL DEFAULT '{{}}'::integer[]"
    )
    await fixture.connection.execute(f"UPDATE {parent} SET canonical_network_ids=prior_network_ids")
    await fixture.connection.execute(f"ALTER TABLE {parent} DROP COLUMN prior_network_ids")
    specification = await _bootstrap_specification(fixture)
    parent_columns, parent_shape = await bootstrap._shape(fixture.connection, specification.address_table_oid)
    assert parent_columns[-1] == "canonical_network_ids"
    assert parent_columns != tuple(EntityAddressUnified.__table__.columns.keys())
    parent_index = await fixture.connection.fetchval(
        "SELECT indexrelid::bigint FROM pg_index WHERE indrelid=$1::oid AND indisprimary",
        specification.address_table_oid,
    )
    receipt = await _bootstrap_prepare(fixture, specification)
    capture_oid = dict(receipt.relation_oids)["entity_address_unified"]
    assert await bootstrap._shape(fixture.connection, capture_oid) == (parent_columns, parent_shape)
    assert receipt.parent_receipts[0].shape_sha256 == parent_shape
    assert receipt.specification == specification
    assert (
        await fixture.connection.fetchval(
            "SELECT indexrelid::bigint FROM pg_index WHERE indrelid=$1::oid AND indisprimary",
            specification.address_table_oid,
        )
        == parent_index
    )
    for table_name in ("entity_address_unified", "npi"):
        assert await fixture.connection.fetchval(f"""SELECT NOT EXISTS(
          (SELECT to_jsonb(original) FROM "{fixture.source_schema}".{table_name} original
           EXCEPT ALL SELECT to_jsonb(retained) FROM "{receipt.address_source.schema_name}".{table_name} retained)
          UNION ALL
          (SELECT to_jsonb(retained) FROM "{receipt.address_source.schema_name}".{table_name} retained
           EXCEPT ALL SELECT to_jsonb(original) FROM "{fixture.source_schema}".{table_name} original))""")
    assert await fixture.connection.fetchval(
        f'SELECT canonical_network_ids FROM "{receipt.address_source.schema_name}".entity_address_unified'
    ) == [7, 11]
    assert await _bootstrap_prepare(fixture, specification) == receipt
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["reader"]}"')
        assert await verify_network_bootstrap_sources(fixture.connection, receipt) == receipt
    await _assert_bootstrap_read_only(fixture, '"' + receipt.address_source.schema_name + '"')


@pytest.mark.parametrize("invalid", ["missing", "extra", "type", "array_type", "varchar_typmod", "numeric_typmod"])
async def test_bootstrap_rejects_named_column_or_type_drift_before_capture(bootstrap_db, invalid):
    fixture = bootstrap_db
    parent = f'"{fixture.source_schema}".entity_address_unified'
    statement_by_invalid = {
        "missing": f"ALTER TABLE {parent} DROP COLUMN city_name",
        "extra": f"ALTER TABLE {parent} ADD COLUMN unexpected_column text",
        "type": f"ALTER TABLE {parent} ALTER COLUMN source_count TYPE bigint",
        "array_type": f"ALTER TABLE {parent} ALTER COLUMN canonical_network_ids TYPE bigint[]",
        "varchar_typmod": f"ALTER TABLE {parent} ALTER COLUMN entity_name TYPE varchar(255)",
        "numeric_typmod": f"ALTER TABLE {parent} ALTER COLUMN lat TYPE numeric(12,8)",
    }
    await fixture.connection.execute(statement_by_invalid[invalid])
    specification = await _bootstrap_specification(fixture)
    with pytest.raises(NetworkBootstrapSourceError, match="Parent model (shape|type) differs"):
        await _bootstrap_prepare(fixture, specification)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )


async def test_bootstrap_rejects_duplicate_column_metadata(bootstrap_db, monkeypatch):
    """The admission check rejects duplicate names even in malformed catalog metadata."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    original_shape = bootstrap._shape

    async def duplicate_shape(connection, table_oid):
        columns, fingerprint = await original_shape(connection, table_oid)
        return (
            (columns[:-1] + (columns[0],), fingerprint)
            if table_oid == specification.address_table_oid
            else (columns, fingerprint)
        )

    monkeypatch.setattr(bootstrap, "_shape", duplicate_shape)
    with pytest.raises(NetworkBootstrapSourceError, match="Parent model shape differs"):
        await _bootstrap_prepare(fixture, specification)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )


@pytest.mark.parametrize("tamper", ["content", "column", "owner_member", "extra", "oid", "receipt", "receipt_scalar"])
async def test_bootstrap_native_tamper_denied(bootstrap_db, tamper):
    fixture = bootstrap_db
    receipt = await _bootstrap_prepare(fixture, await _bootstrap_specification(fixture))
    namespace = '"' + receipt.address_source.schema_name + '"'
    statements_by_tamper = {
        "content": f"UPDATE {namespace}.npi SET provider_first_name='Tampered'",
        "column": f'GRANT UPDATE(provider_first_name) ON {namespace}.npi TO "{fixture.roles["reader"]}"',
        "owner_member": f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["reader"]}"',
        "extra": f"CREATE TABLE {namespace}.extra(id int)",
        "oid": f"ALTER TABLE {namespace}.npi RENAME TO previous_npi",
        "receipt": f"COMMENT ON SCHEMA {namespace} IS '{{}}'",
        "receipt_scalar": f"COMMENT ON SCHEMA {namespace} IS 'null'",
    }
    await fixture.connection.execute(statements_by_tamper[tamper])
    with pytest.raises(ValueError):
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            await verify_network_bootstrap_sources(fixture.connection, receipt)


async def test_bootstrap_rollback_fault_and_retry(bootstrap_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    original_content = bootstrap._content

    async def failed_content(connection, schema_name, table_name):
        if schema_name.startswith("network_bootstrap_") and table_name == "npi":
            raise RuntimeError("isolated copy failure")
        return await original_content(connection, schema_name, table_name)

    monkeypatch.setattr(bootstrap, "_content", failed_content)
    with pytest.raises(RuntimeError, match="isolated copy failure"):
        await _bootstrap_prepare(fixture, specification)
    namespace = "network_bootstrap_" + UUID(specification.bootstrap_id).hex
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    monkeypatch.setattr(bootstrap, "_content", original_content)
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with fixture.connection.transaction(isolation="repeatable_read"):
            receipt = await prepare_network_bootstrap_sources(
                fixture.connection,
                specification,
                owner_role=fixture.roles["owner"],
                runtime_roles=tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
            )
            raise RuntimeError("caller rollback")
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", receipt.address_source.schema_name) is None
    assert await _bootstrap_prepare(fixture, specification)


@pytest.mark.parametrize("invalid", ["oid", "boolean_oid", "generation", "isolation", "unsafe_owner"])
async def test_bootstrap_parent_admission_rejected_before_capture(bootstrap_db, invalid):
    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    if invalid in {"oid", "boolean_oid"}:
        specification = replace(
            specification, npi_table_oid=True if invalid == "boolean_oid" else specification.npi_table_oid + 1
        )
    if invalid == "generation":
        await fixture.connection.execute(
            f'UPDATE "{fixture.source_schema}".entity_address_result_generation SET local_generation=1'
        )
    if invalid == "unsafe_owner":
        await fixture.connection.execute(f'GRANT "{fixture.roles["owner"]}" TO "{fixture.roles["loader"]}"')
    with pytest.raises((ValueError, RuntimeError)):
        if invalid == "isolation":
            async with fixture.connection.transaction():
                await prepare_network_bootstrap_sources(
                    fixture.connection,
                    specification,
                    owner_role=fixture.roles["owner"],
                    runtime_roles=tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
                )
        else:
            await _bootstrap_prepare(fixture, specification)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )


async def test_bootstrap_bulk_query_count_and_native_content_parity(bootstrap_db):
    from sqlalchemy import text

    fixture = bootstrap_db

    # Initialize asyncpg's native result codecs before comparing application SQL.
    await fixture.connection.fetch(f'SELECT * FROM "{fixture.source_schema}".entity_address_result_generation')
    await fixture.connection.fetch(f'SELECT * FROM "{fixture.source_schema}".npi_result_generation')
    for table_name in ("entity_address_unified", "npi"):
        await fixture.connection.fetch(f'SELECT * FROM "{fixture.source_schema}".{table_name} LIMIT 0')

    async def counted_capture():
        specification = await _bootstrap_specification(fixture)
        statements = []
        fixture.connection.add_query_logger(statements.append)
        try:
            receipt = await _bootstrap_prepare(fixture, specification)
            await asyncio.sleep(0)
            return receipt, [query.query for query in statements]
        finally:
            fixture.connection.remove_query_logger(statements.append)

    small, small_queries = await counted_capture()
    async with fixture.engine.begin() as writer:
        await writer.execute(
            text(f'''INSERT INTO "{fixture.source_schema}".npi(npi,provider_first_name)
            SELECT 2000000000+ordinal,'Synthetic' FROM generate_series(1,999) ordinal''')
        )
        await writer.execute(
            text(f'''INSERT INTO "{fixture.source_schema}".entity_address_unified
            (location_key,entity_type,entity_id,npi,checksum,type,first_line)
            SELECT encode(sha256(convert_to(ordinal::text,'UTF8')),'hex'),'npi',
              (2000000000+ordinal)::text,2000000000+ordinal,ordinal,'practice','Synthetic Street'
            FROM generate_series(1,999) ordinal''')
        )
    large, large_queries = await counted_capture()
    assert [parent.row_count for parent in small.parent_receipts] == [1, 1]
    assert [parent.row_count for parent in large.parent_receipts] == [1000, 1000]
    assert len(small_queries) == len(large_queries), [query for query in small_queries if query not in large_queries]
    assert large.generation_sha256 != small.generation_sha256
    namespace = '"' + large.address_source.schema_name + '"'
    assert await fixture.connection.fetchval(f'''SELECT NOT EXISTS(
      SELECT to_jsonb(parent) FROM "{fixture.source_schema}".npi parent
      EXCEPT ALL SELECT to_jsonb(captured) FROM {namespace}.npi captured)''')


async def test_bootstrap_cancellation_rolls_back_native_copy_and_caller_savepoint_survives(bootstrap_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    copying = asyncio.Event()
    original_content = bootstrap._content

    async def interrupted_content(connection, schema_name, table_name):
        if schema_name.startswith("network_bootstrap_"):
            copying.set()
            await connection.execute("SELECT pg_sleep(30)")
        return await original_content(connection, schema_name, table_name)

    monkeypatch.setattr(bootstrap, "_content", interrupted_content)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE bootstrap_caller_marker(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO bootstrap_caller_marker VALUES(9)")
        capture = asyncio.create_task(
            prepare_network_bootstrap_sources(
                fixture.connection,
                specification,
                owner_role=fixture.roles["owner"],
                runtime_roles=tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
            )
        )
        await asyncio.wait_for(copying.wait(), 3)
        capture.cancel()
        with pytest.raises(asyncio.CancelledError):
            await capture
        assert await fixture.connection.fetchval("SELECT value FROM bootstrap_caller_marker") == 9
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
            )
            is None
        )
    monkeypatch.setattr(bootstrap, "_content", original_content)
    assert await _bootstrap_prepare(fixture, specification)


async def test_bootstrap_parent_oid_swap_after_rr_snapshot_fails_closed(bootstrap_db, monkeypatch):
    from sqlalchemy import text

    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    original_check = bootstrap._check_roles

    async def swap_parent(connection, roles):
        await original_check(connection, roles)
        async with fixture.engine.begin() as writer:
            await writer.execute(text(f'ALTER TABLE "{fixture.source_schema}".npi RENAME TO old_npi'))
            await writer.execute(
                text(
                    f'CREATE TABLE "{fixture.source_schema}".npi (LIKE "{fixture.source_schema}".old_npi INCLUDING ALL)'
                )
            )
            await writer.execute(
                text(f'ALTER TABLE "{fixture.source_schema}".npi OWNER TO "{fixture.roles["loader"]}"')
            )

    monkeypatch.setattr(bootstrap, "_check_roles", swap_parent)
    with pytest.raises(NetworkBootstrapSourceError, match="identity differs"):
        await _bootstrap_prepare(fixture, specification)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )


async def test_bootstrap_closes_applied_default_grants_and_avoids_redundant_fresh_hashes(bootstrap_db, monkeypatch):
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    await fixture.connection.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{fixture.roles["publisher"]}" GRANT UPDATE ON TABLES TO PUBLIC'
    )
    original_content = bootstrap._content
    scanned_tables = []

    async def counted_content(connection, schema_name, table_name):
        scanned_tables.append((schema_name, table_name))
        return await original_content(connection, schema_name, table_name)

    monkeypatch.setattr(bootstrap, "_content", counted_content)
    specification = await _bootstrap_specification(fixture)
    receipt = await _bootstrap_prepare(fixture, specification)
    assert len(scanned_tables) == 4
    assert sum(schema_name == fixture.source_schema for schema_name, _ in scanned_tables) == 2
    scanned_tables.clear()
    assert await _bootstrap_prepare(fixture, specification) == receipt
    assert scanned_tables == [
        (receipt.address_source.schema_name, "entity_address_unified"),
        (receipt.npi_source.schema_name, "npi"),
    ]
    async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
        assert await verify_network_bootstrap_sources(fixture.connection, receipt) == receipt
    assert await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) a WHERE d.defaclrole=(SELECT oid FROM pg_roles WHERE rolname=$1) AND a.grantee=0 AND a.privilege_type='UPDATE')",
        fixture.roles["publisher"],
    )


class BootstrapCopyConnection:
    """Delegate every database operation while observing actual native COPY calls."""

    def __init__(self, connection, *, failure=None, copying=None):
        self.connection = connection
        self.failure = failure
        self.copying = copying
        self.exports = []
        self.imports = []
        self.import_bytes = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    async def copy_from_query(self, query, *arguments, **options):
        """Execute binary export before injecting a native accounting failure."""
        status = await self.connection.copy_from_query(query, *arguments, **options)
        self.exports.append((query, status))
        if self.failure == "export_count":
            return "COPY 0"
        if self.failure == "export_error":
            await self.connection.execute("SELECT 1/0")
        return status

    async def copy_to_table(self, table_name, **options):
        """Execute binary import before testing rollback of writes and open spools."""
        copy_file = options["source"]
        start = copy_file.tell()
        copy_file.seek(0, 2)
        self.import_bytes.append(copy_file.tell())
        copy_file.seek(start)
        status = await self.connection.copy_to_table(table_name, **options)
        self.imports.append((table_name, status))
        if self.failure == "import_count":
            return "COPY 0"
        if self.failure == "import_error":
            await self.connection.execute("SELECT 1/0")
        if self.copying is not None:
            self.copying.set()
            await self.connection.execute("SELECT pg_sleep(30)")
        return status


async def _seed_bootstrap_copy_rows(fixture):
    """Load wide strings, full arrays and null fields through actual native columns."""
    namespace = '"' + fixture.source_schema + '"'
    await fixture.connection.execute(f"""INSERT INTO {namespace}.entity_address_unified
      (location_key,entity_type,entity_id,npi,checksum,type,first_line,second_line,
       plans_network_array,procedures_array,medications_array,canonical_network_ids)
      SELECT encode(sha256(convert_to(ordinal::text,'UTF8')),'hex'),'npi',
       (2000000000+ordinal)::text,2000000000+ordinal,ordinal,'practice',repeat('A',1024),
       CASE WHEN ordinal%2=0 THEN NULL ELSE 'Suite 2' END,
       ARRAY[42,73],ARRAY[123,456],ARRAY[789],ARRAY[7,8]
      FROM generate_series(1,5) ordinal""")
    await fixture.connection.execute(f"""INSERT INTO {namespace}.npi
      (npi,provider_organization_name,provider_first_name,search_taxonomy_codes,do_business_as)
      SELECT 2000000000+ordinal,repeat('B',2048),NULL,
       ARRAY['207Q00000X','207R00000X'],ARRAY['Example name','Second name']
      FROM generate_series(1,5) ordinal""")


def _observe_bootstrap_spools(monkeypatch, bootstrap):
    """Retain only file handles so every success and failure can prove closure."""
    original_temporary_file = bootstrap.tempfile.TemporaryFile
    spools = []

    def tracked_temporary_file(*arguments, **options):
        copy_file = original_temporary_file(*arguments, **options)
        spools.append(copy_file)
        return copy_file

    monkeypatch.setattr(bootstrap.tempfile, "TemporaryFile", tracked_temporary_file)
    return spools


async def _bootstrap_copy_proxy_prepare(fixture, specification, proxy):
    """Use the real caller RR transaction and publisher role with an observed driver."""
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["publisher"]}"')
        return await prepare_network_bootstrap_sources(
            proxy,
            specification,
            owner_role=fixture.roles["owner"],
            runtime_roles=tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
        )


@pytest.mark.parametrize("refusal", [None, "before_import", "after_import", "cancel_after_import"])
async def test_native_binary_copy_import_admission_boundaries(bootstrap_db, monkeypatch, refusal):
    """Real export precedes admission; import failures roll back and close the spool."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection)
    events = []
    source_relation = '"' + fixture.source_schema + '".npi'
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles["publisher"]}"')
        await fixture.connection.execute(
            f"CREATE TEMP TABLE bounded_copy_probe (LIKE {source_relation} INCLUDING STORAGE) ON COMMIT DROP"
        )
        expected_count = await fixture.connection.fetchval(f"SELECT count(*) FROM {source_relation}")

        async def admit_import(phase):
            """Observe real target state and refuse only at the chosen native boundary."""
            count = await fixture.connection.fetchval("SELECT count(*) FROM bounded_copy_probe")
            events.append((phase, count))
            assert len(proxy.exports) == 1
            if phase == refusal:
                raise RuntimeError("synthetic import refusal")
            if phase == "after_import" and refusal == "cancel_after_import":
                raise asyncio.CancelledError

        async def copy_with_admission():
            """Use the shared transport on real source/model columns and a bounded spool."""
            await bootstrap._copy_native_batch(
                proxy,
                f"SELECT * FROM {source_relation}",
                (),
                "pg_temp",
                "bounded_copy_probe",
                expected_count,
                byte_limit=64 * 1024**2,
                import_admission=admit_import,
            )

        if refusal is None:
            await copy_with_admission()
            assert events == [("before_import", 0), ("after_import", expected_count)]
        else:
            failure = asyncio.CancelledError if refusal == "cancel_after_import" else RuntimeError
            with pytest.raises(failure):
                await copy_with_admission()
            assert events[0] == ("before_import", 0)
            assert await fixture.connection.fetchval("SELECT count(*) FROM bounded_copy_probe") == 0
            await bootstrap._copy_native_batch(
                fixture.connection,
                f"SELECT * FROM {source_relation}",
                (),
                "pg_temp",
                "bounded_copy_probe",
                expected_count,
            )
        assert await fixture.connection.fetchval("SELECT count(*) FROM bounded_copy_probe") == expected_count
        assert spools and all(copy_file.closed for copy_file in spools)


@pytest.mark.parametrize("byte_bound", [64 * 1024 * 1024, 4096])
async def test_bootstrap_native_multibatch_full_row_parity(bootstrap_db, monkeypatch, byte_bound):
    """Keyset COPY preserves arrays/nulls and halves wide batches without omissions."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    await _seed_bootstrap_copy_rows(fixture)
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_ROWS", 2 if byte_bound > 4096 else 6)
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_BYTES", byte_bound)
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection)
    receipt = await _bootstrap_copy_proxy_prepare(fixture, await _bootstrap_specification(fixture), proxy)
    assert [parent.row_count for parent in receipt.parent_receipts] == [6, 6]
    assert len(proxy.imports) >= 6
    assert all(0 < byte_count <= byte_bound for byte_count in proxy.import_bytes)
    assert all(copy_file.closed for copy_file in spools)
    assert all("OFFSET" not in query and "ORDER BY" in query for query, _status in proxy.exports)
    for table_name in ("entity_address_unified", "npi"):
        assert await fixture.connection.fetchval(f"""SELECT NOT EXISTS(
          (SELECT to_jsonb(parent) FROM "{fixture.source_schema}".{table_name} parent
           EXCEPT ALL SELECT to_jsonb(captured) FROM "{receipt.address_source.schema_name}".{table_name} captured)
          UNION ALL
          (SELECT to_jsonb(captured) FROM "{receipt.address_source.schema_name}".{table_name} captured
           EXCEPT ALL SELECT to_jsonb(parent) FROM "{fixture.source_schema}".{table_name} parent))""")
    assert await _bootstrap_prepare(fixture, receipt.specification) == receipt
    await _assert_bootstrap_read_only(fixture, '"' + receipt.address_source.schema_name + '"')


@pytest.mark.parametrize("failure", ["export_count", "import_count", "export_error", "import_error"])
async def test_bootstrap_copy_fault_rolls_back_and_closes_spool(bootstrap_db, monkeypatch, failure):
    """A wrong native count or actual database failure retains no partial artifact."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    specification = await _bootstrap_specification(fixture)
    with pytest.raises(NetworkBootstrapSourceError):
        await _bootstrap_copy_proxy_prepare(
            fixture, specification, BootstrapCopyConnection(fixture.connection, failure=failure)
        )
    assert spools and all(copy_file.closed for copy_file in spools)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )
    assert await _bootstrap_prepare(fixture, specification)


async def test_bootstrap_overwide_row_rejects_atomically_then_retries(bootstrap_db, monkeypatch):
    """A row beyond the explicit byte bound fails rather than being omitted."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_BYTES", 64)
    with pytest.raises(NetworkBootstrapSourceError, match="COPY bound"):
        await _bootstrap_prepare(fixture, specification)
    assert all(copy_file.closed for copy_file in spools)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_BYTES", 64 * 1024 * 1024)
    assert await _bootstrap_prepare(fixture, specification)


async def test_bootstrap_copy_cancel_closes_spool_and_keeps_caller_savepoint(bootstrap_db, monkeypatch):
    """Cancel after real binary import while keeping the outer caller transaction usable."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    copying = asyncio.Event()
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection, copying=copying)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE bootstrap_copy_marker(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO bootstrap_copy_marker VALUES(9)")
        capture = asyncio.create_task(
            prepare_network_bootstrap_sources(
                proxy,
                specification,
                owner_role=fixture.roles["owner"],
                runtime_roles=tuple(sorted((fixture.roles["loader"], fixture.roles["reader"]))),
            )
        )
        await asyncio.wait_for(copying.wait(), 3)
        capture.cancel()
        with pytest.raises(asyncio.CancelledError):
            await capture
        assert await fixture.connection.fetchval("SELECT value FROM bootstrap_copy_marker") == 9
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
            )
            is None
        )
        assert spools and all(copy_file.closed for copy_file in spools)
    assert await _bootstrap_prepare(fixture, specification)


@pytest.mark.parametrize("failure", ["write_error", "short_write", "open_error"])
async def test_bootstrap_spool_write_failure_closes_and_retries(bootstrap_db, monkeypatch, failure):
    """Disk write errors finish export safely and never import a partial binary stream."""
    import process.network_bootstrap_sources as bootstrap

    fixture = bootstrap_db
    specification = await _bootstrap_specification(fixture)
    original_temporary_file = bootstrap.tempfile.TemporaryFile
    spools = []

    def failed_temporary_file(*arguments, **options):
        """Observe the real anonymous file while failing creation or its export writer."""
        if failure == "open_error":
            raise OSError("synthetic spool creation error")
        copy_file = original_temporary_file(*arguments, **options)

        def failed_write(chunk):
            """Exercise short writes and operating-system errors without exposing paths."""
            if failure == "write_error":
                raise OSError("synthetic spool error")
            return len(chunk) - 1

        copy_file.write = failed_write
        spools.append(copy_file)
        return copy_file

    monkeypatch.setattr(bootstrap.tempfile, "TemporaryFile", failed_temporary_file)
    proxy = BootstrapCopyConnection(fixture.connection)
    with pytest.raises(
        NetworkBootstrapSourceError, match="preparation failed" if failure == "open_error" else "spool write"
    ):
        await _bootstrap_copy_proxy_prepare(fixture, specification, proxy)
    assert len(proxy.exports) == (0 if failure == "open_error" else 1) and proxy.imports == []
    assert bool(spools) == (failure != "open_error")
    assert all(copy_file.closed for copy_file in spools)
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regnamespace($1)", "network_bootstrap_" + UUID(specification.bootstrap_id).hex
        )
        is None
    )
    monkeypatch.setattr(bootstrap.tempfile, "TemporaryFile", original_temporary_file)
    assert await _bootstrap_prepare(fixture, specification)


async def test_remove_commands_never_prepare_manual_address_or_binding(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    await fixture.connection.execute(
        f"UPDATE \"{fixture.control_schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{memberships_json}}',(record_json->'memberships_json')||jsonb_build_array((record_json#>'{{memberships_json,0}}')||jsonb_build_object('operation','remove','location_id',$1::text))) WHERE record_kind='membership'",
        seed.second["record_id"],
    )
    receipt = await _prepare(fixture, seed.revision)
    namespace = '"' + receipt.address_source.schema_name + '"'
    locations = await fixture.connection.fetch(f"SELECT location_id FROM {namespace}.provider_location_binding")
    assert [str(row["location_id"]) for row in locations] == [seed.first["record_id"]]
    assert (
        await fixture.connection.fetchval(
            f"SELECT count(*) FROM {namespace}.entity_address_unified WHERE row_origin='network_custom'"
        )
        == 1
    )


@pytest.mark.parametrize("byte_bound", [4096, 16384])
async def test_custom_retained_binary_copy_bounds_and_full_row_parity(custom_db, monkeypatch, byte_bound):
    """Keep complete native rows while replacing only the old custom offices."""
    import process.network_bootstrap_sources as bootstrap

    fixture = custom_db
    seed = await _seed(fixture)
    await _seed_bootstrap_copy_rows(fixture)
    await fixture.connection.execute(
        f"""UPDATE "{fixture.source_schema}".entity_address_unified
      SET lat=12.12345678,long=-45.87654321,address_sources=ARRAY['source-a','source-b'],
      source_record_ids=ARRAY['record-a'],date_added='2026-01-02',updated_at='2026-01-02 03:04:05',
      entity_name='Native sample Ω' WHERE location_key<>$1""",
        "a" * 64,
    )
    await fixture.connection.execute(
        f"UPDATE \"{fixture.source_schema}\".entity_address_unified SET row_origin='network_custom' WHERE location_key=$1",
        "a" * 64,
    )
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_ROWS", 6 if byte_bound == 4096 else 2)
    monkeypatch.setattr(bootstrap, "_COPY_BATCH_BYTES", byte_bound)
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection)
    arguments = _arguments(fixture, seed.revision)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        receipt = await prepare_custom_address_source(proxy, fixture.base, **arguments)
    assert len(proxy.imports) >= 3 and all(0 < size <= byte_bound for size in proxy.import_bytes)
    assert all(copy_file.closed for copy_file in spools)
    assert all("OFFSET" not in query and "ORDER BY a.location_key" in query for query, _ in proxy.exports)
    address_table = '"' + receipt.address_source.schema_name + '".entity_address_unified'
    assert await fixture.connection.fetchval(f"""SELECT NOT EXISTS(
      (SELECT * FROM "{fixture.source_schema}".entity_address_unified WHERE row_origin<>'network_custom'
       EXCEPT ALL SELECT * FROM {address_table} WHERE row_origin<>'network_custom') UNION ALL
      (SELECT * FROM {address_table} WHERE row_origin<>'network_custom' EXCEPT ALL
       SELECT * FROM "{fixture.source_schema}".entity_address_unified WHERE row_origin<>'network_custom'))""")
    assert await fixture.connection.fetchval(f"SELECT count(*) FROM {address_table}") == 6
    assert not await fixture.connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM {address_table} WHERE location_key=$1)", "a" * 64
    )
    assert await _prepare(fixture, seed.revision, arguments=arguments) == receipt
    assert await _verify(fixture, receipt) == receipt


@pytest.mark.parametrize("failure", ["export_count", "import_count", "export_error", "import_error"])
async def test_custom_retained_copy_failure_preserves_caller_and_retries(custom_db, monkeypatch, failure):
    """Native count and driver failures leave neither partial heaps nor a broken caller."""
    import process.network_bootstrap_sources as bootstrap

    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE custom_copy_marker(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO custom_copy_marker VALUES(7)")
        with pytest.raises(NetworkCustomAddressSourceError):
            await prepare_custom_address_source(
                BootstrapCopyConnection(fixture.connection, failure=failure), fixture.base, **arguments
            )
        assert await fixture.connection.fetchval("SELECT value FROM custom_copy_marker") == 7
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
            )
            is None
        )
        assert spools and all(copy_file.closed for copy_file in spools)
        assert await prepare_custom_address_source(fixture.connection, fixture.base, **arguments)


async def test_custom_retained_copy_cancel_closes_spool_and_preserves_caller(custom_db, monkeypatch):
    """Cancellation rolls back imported rows before the same identity can retry."""
    import process.network_bootstrap_sources as bootstrap

    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    copying = asyncio.Event()
    spools = _observe_bootstrap_spools(monkeypatch, bootstrap)
    proxy = BootstrapCopyConnection(fixture.connection, copying=copying)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        await fixture.connection.execute("CREATE TEMP TABLE custom_cancel_marker(value int) ON COMMIT DROP")
        await fixture.connection.execute("INSERT INTO custom_cancel_marker VALUES(9)")
        task = asyncio.create_task(prepare_custom_address_source(proxy, fixture.base, **arguments))
        await asyncio.wait_for(copying.wait(), timeout=10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await fixture.connection.fetchval("SELECT value FROM custom_cancel_marker") == 9
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
            )
            is None
        )
        assert spools and all(copy_file.closed for copy_file in spools)
        assert await prepare_custom_address_source(fixture.connection, fixture.base, **arguments)


async def test_custom_retained_copy_requires_existing_key_and_bounded_row(custom_db, monkeypatch):
    """Refuse missing native key before candidate DDL and overwide rows atomically."""
    import process.network_bootstrap_sources as bootstrap

    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    parent = '"' + fixture.source_schema + '".entity_address_unified'
    await fixture.connection.execute(f"ALTER TABLE {parent} DROP CONSTRAINT entity_address_unified_pkey")
    proxy = BootstrapCopyConnection(fixture.connection)
    async with fixture.connection.transaction(isolation="repeatable_read"):
        with pytest.raises(NetworkCustomAddressSourceError):
            await prepare_custom_address_source(proxy, fixture.base, **arguments)
        assert not proxy.exports
        assert (
            await fixture.connection.fetchval(
                "SELECT to_regnamespace($1)", "network_composition_" + UUID(arguments["composition_id"]).hex
            )
            is None
        )
    await fixture.connection.execute(f"ALTER TABLE {parent} ADD PRIMARY KEY(location_key)")
    with monkeypatch.context() as limited:
        limited.setattr(bootstrap, "_COPY_BATCH_BYTES", 64)
        with pytest.raises(NetworkCustomAddressSourceError):
            await _prepare(fixture, seed.revision, arguments=arguments)
    assert await _prepare(fixture, seed.revision, arguments=arguments)


async def test_custom_retained_copy_checks_full_row_parity(custom_db, monkeypatch):
    """Matching COPY counts cannot hide altered retained values."""
    import process.network_custom_address_source as custom

    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    original_copy = custom._copy_projection_rows

    async def altered_copy(connection, schema_name, query):
        copied = await original_copy(connection, schema_name, query)
        await connection.execute(f"UPDATE \"{schema_name}\".entity_address_unified SET first_line='Altered native row'")
        return copied

    with monkeypatch.context() as altered:
        altered.setattr(custom, "_copy_projection_rows", altered_copy)
        with pytest.raises(NetworkCustomAddressSourceError, match="full-row parity"):
            await _prepare(fixture, seed.revision, arguments=arguments)
    assert await _prepare(fixture, seed.revision, arguments=arguments)


async def test_custom_retained_copy_rejects_source_oid_swap_before_copy(custom_db, monkeypatch):
    """The copied relation must remain the physical source admitted by the recipe."""
    import process.network_custom_address_source as custom

    fixture = custom_db
    seed = await _seed(fixture)
    arguments = _arguments(fixture, seed.revision)
    original_identity = custom._source_identity

    async def swapped_identity(connection, source, roles, *, address=False, control_schema=None):
        identity = await original_identity(connection, source, roles, address=address, control_schema=control_schema)
        if address:
            namespace = '"' + source.schema_name + '"'
            await connection.execute(f"ALTER TABLE {namespace}.entity_address_unified RENAME TO old_address")
            await connection.execute(
                f"CREATE TABLE {namespace}.entity_address_unified (LIKE {namespace}.old_address INCLUDING ALL)"
            )
        return identity

    with monkeypatch.context() as swapped:
        swapped.setattr(custom, "_source_identity", swapped_identity)
        with pytest.raises(NetworkCustomAddressSourceError, match="native identity"):
            await _prepare(fixture, seed.revision, arguments=arguments)
    assert await _prepare(fixture, seed.revision, arguments=arguments)


@pytest.fixture
async def aca_office_db(serving_schema, request):
    """Reuse the exact office fixture without a collection-time import cycle."""
    from contextlib import aclosing

    from tests.test_network_initial_source_office_bindings_postgres import office_db

    async with aclosing(office_db.__wrapped__(serving_schema, request)) as fixtures:
        async for fixture in fixtures:
            yield fixture


@pytest.mark.parametrize("aca_office_db", ["typed"], indirect=True)
async def test_aca_carry_adds_only_new_exact_office_and_replays_readonly(aca_office_db):
    from process.network_custom_address_source import CustomAddressSourceArguments, prepare_recipe_address_source
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    fixture = aca_office_db
    namespace = '"' + fixture.address_schema + '"'
    await fixture.connection.execute(
        f"DELETE FROM {namespace}.entity_address_unified WHERE address_key=$1", fixture.sites[1]
    )
    composition_id = str(uuid4())
    target_schema = "network_composition_" + UUID(composition_id).hex
    arguments = CustomAddressSourceArguments(
        composition_id,
        fixture.batch.approved_source.approved_revision,
        fixture.owner,
        (fixture.reader,),
        fixture.control,
    )
    recipes = (RegistrySourceMembershipRecipe(fixture.source, fixture.coordinates),)
    try:
        async with fixture.connection.transaction(isolation="repeatable_read"):
            receipt = await prepare_recipe_address_source(
                fixture.connection, fixture.address_source, arguments, (recipes, None)
            )
            assert receipt.aca_carry_sha256 is not None and receipt.as_dict()["revision"] == 4
            sites = await fixture.connection.fetch(
                f'SELECT npi,address_key,row_origin,type,address_precision FROM "{target_schema}".entity_address_unified'
            )
            assert {site["address_key"] for site in sites} == set(fixture.sites)
            assert all(
                (site["npi"], site["row_origin"], site["type"], site["address_precision"])
                == (1000000491, "base", "practice", "street")
                for site in sites
            )
            assert (
                await fixture.connection.fetchval(f'SELECT count(*) FROM "{target_schema}".provider_location_binding')
                == 2
            )
        async with fixture.connection.transaction(isolation="repeatable_read", readonly=True):
            assert (
                await prepare_recipe_address_source(
                    fixture.connection, fixture.address_source, arguments, (recipes, None)
                )
                == receipt
            )
        assert await fixture.connection.fetchval(f"SELECT count(*) FROM {namespace}.entity_address_unified") == 1
    finally:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


@pytest.mark.parametrize("aca_office_db", ["typed"], indirect=True)
@pytest.mark.parametrize("damage", ["canonical", "collision"])
async def test_aca_carry_invalid_source_or_existing_pair_rolls_back_artifact(aca_office_db, damage):
    from process.network_custom_address_source import CustomAddressSourceArguments, prepare_recipe_address_source
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    fixture = aca_office_db
    composition_id = str(uuid4())
    target_schema = "network_composition_" + UUID(composition_id).hex
    arguments = CustomAddressSourceArguments(
        composition_id,
        fixture.batch.approved_source.approved_revision,
        fixture.owner,
        (fixture.reader,),
        fixture.control,
    )
    if damage == "canonical":
        await fixture.connection.execute(
            f'UPDATE "{fixture.coordinates.dataset_schema}".mrf_canonical_address SET source_bits=0'
        )
    else:
        await fixture.connection.execute(
            f"UPDATE \"{fixture.address_schema}\".entity_address_unified SET entity_id='1000000000' WHERE address_key=$1",
            fixture.sites[1],
        )
    recipes = (RegistrySourceMembershipRecipe(fixture.source, fixture.coordinates),)
    try:
        with pytest.raises(ValueError):
            async with fixture.connection.transaction(isolation="repeatable_read"):
                await prepare_recipe_address_source(
                    fixture.connection, fixture.address_source, arguments, (recipes, None)
                )
        assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", target_schema) is None
    finally:
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{target_schema}" CASCADE')


async def test_bootstrap_fresh_verification_uses_noninherited_protected_owner(bootstrap_db):
    fixture = bootstrap_db
    connection = fixture.connection
    publisher = fixture.roles["publisher"]
    owner = fixture.roles["owner"]
    reader = fixture.roles["reader"]
    await connection.execute(f'GRANT "{owner}" TO "{publisher}" WITH INHERIT FALSE, SET TRUE')
    specification = await _bootstrap_specification(fixture)
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute(f'SET LOCAL ROLE "{publisher}"')
        assert await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner)
        assert not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'USAGE')", owner)
        receipt = await prepare_network_bootstrap_sources(
            connection, specification, owner_role=owner, runtime_roles=(reader,)
        )
        assert await connection.fetchval("SELECT current_user::text") == publisher
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.fetchval(f'SELECT count(*) FROM "{receipt.address_source.schema_name}".npi')
        await connection.execute("RESET ROLE")
        await connection.execute(f'SET LOCAL ROLE "{reader}"')
        assert await verify_network_bootstrap_sources(connection, receipt) == receipt


@pytest.fixture
async def bootstrap_access_fixture(bootstrap_db, request):
    fixture = bootstrap_db
    # Exercise the ordinary immutable shape separately from dormant serial defaults.
    if getattr(request, "param", "constant") != "serial":
        await fixture.connection.execute(f'ALTER TABLE "{fixture.source_schema}".npi ALTER COLUMN npi DROP DEFAULT')
    receipt = await _bootstrap_prepare(
        fixture, await _bootstrap_specification(fixture), runtime_roles=(fixture.roles["reader"],)
    )
    reader, stranger = "access_reader_" + uuid4().hex, "access_stranger_" + uuid4().hex
    password = uuid4().hex
    for role in (reader, stranger):
        await fixture.connection.execute(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'")
    try:
        yield SimpleNamespace(fixture=fixture, receipt=receipt, reader=reader, stranger=stranger, password=password)
    finally:
        await fixture.connection.execute("RESET ROLE")
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{bootstrap_access._namespace(receipt)}" CASCADE')
        for role in (stranger, reader):
            await fixture.connection.execute(f'DROP OWNED BY "{role}"')
            await fixture.connection.execute(f'DROP ROLE "{role}"')
        assert (
            await fixture.connection.fetchval("SELECT to_regnamespace($1)", bootstrap_access._namespace(receipt))
            is None
        )


async def _change_bootstrap_access(bootstrap_access_fixture, *, revoke=False, reader=None):
    connection = bootstrap_access_fixture.fixture.connection
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute(f'SET LOCAL ROLE "{bootstrap_access_fixture.fixture.roles["publisher"]}"')
        operation = (
            bootstrap_access.revoke_bootstrap_read_access if revoke else bootstrap_access.install_bootstrap_read_access
        )
        return await operation(
            connection, bootstrap_access_fixture.receipt, reader_role=reader or bootstrap_access_fixture.reader
        )


@asynccontextmanager
async def _bootstrap_access_login(role, password=None):
    dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql+asyncpg://", "postgresql://")
    connection = await asyncpg.connect(dsn, user=role, password=password)
    try:
        assert await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", role)
        yield connection
    finally:
        await connection.close()


async def _verify_bootstrap_access(bootstrap_access_fixture, connection=None):
    connection = connection or bootstrap_access_fixture.fixture.connection
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        return await verify_network_bootstrap_sources(connection, bootstrap_access_fixture.receipt)


async def test_bootstrap_access_exact_successor_preserves_receipt_and_old_reader(bootstrap_access_fixture, monkeypatch):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    original_native = bootstrap_access._native
    native_receipts = []

    async def observe_native(connection, receipt, reader_oid):
        native_receipts.append(receipt)
        return await original_native(connection, receipt, reader_oid)

    monkeypatch.setattr(bootstrap_access, "_native", observe_native)
    original = await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", receipt.schema_oid)
    before = await bootstrap_access._acl(connection, receipt)
    proof = await _change_bootstrap_access(bootstrap_access_fixture)
    assert proof == await _change_bootstrap_access(bootstrap_access_fixture)
    assert proof["before"] == before
    assert original == await connection.fetchval("SELECT obj_description($1::oid,'pg_namespace')", receipt.schema_oid)
    native_receipts.clear()
    assert await _verify_bootstrap_access(bootstrap_access_fixture) == receipt
    assert native_receipts == [receipt]
    async with _bootstrap_access_login(bootstrap_access_fixture.reader, bootstrap_access_fixture.password) as reader:
        assert not await reader.fetchval("SELECT pg_has_role(current_user,$1::name,'MEMBER')", receipt.runtime_roles[0])
        assert await _verify_bootstrap_access(bootstrap_access_fixture, reader) == receipt
        native_receipts.clear()
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            await _source_identity(reader, receipt.address_source, receipt.runtime_roles, address=True)
            await _source_identity(reader, receipt.npi_source, receipt.runtime_roles)
        assert native_receipts == [receipt, receipt]
        assert await reader.fetchval(f'SELECT count(*) FROM "{receipt.address_source.schema_name}".npi') == 1
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await reader.execute(f"UPDATE \"{receipt.address_source.schema_name}\".npi SET provider_first_name='No'")
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await connection.execute(f'SET LOCAL ROLE "{receipt.runtime_roles[0]}"')
        assert await verify_network_bootstrap_sources(connection, receipt) == receipt


async def test_bootstrap_access_revoke_keeps_proof_and_restores_original_acl(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    proof = await _change_bootstrap_access(bootstrap_access_fixture)
    assert await _change_bootstrap_access(bootstrap_access_fixture, revoke=True) == proof
    assert await _change_bootstrap_access(bootstrap_access_fixture, revoke=True) == proof
    assert await bootstrap_access._acl(connection, receipt) == proof["before"]
    assert await _verify_bootstrap_access(bootstrap_access_fixture) == receipt
    assert await bootstrap_access._proof(connection, receipt) == proof
    async with _bootstrap_access_login(bootstrap_access_fixture.reader, bootstrap_access_fixture.password) as reader:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await reader.fetchval(f'SELECT count(*) FROM "{receipt.address_source.schema_name}".npi')
    assert await _change_bootstrap_access(bootstrap_access_fixture) == proof
    with pytest.raises(bootstrap_access.BootstrapReadAccessError):
        await _change_bootstrap_access(bootstrap_access_fixture, reader=bootstrap_access_fixture.stranger)


async def test_bootstrap_access_original_reader_survives_successor_retirement(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    proof = await _change_bootstrap_access(bootstrap_access_fixture)
    await _change_bootstrap_access(bootstrap_access_fixture, revoke=True)
    await connection.execute(f'DROP ROLE "{bootstrap_access_fixture.reader}"')
    assert await _change_bootstrap_access(bootstrap_access_fixture, revoke=True) == proof
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await connection.execute(f'SET LOCAL ROLE "{receipt.runtime_roles[0]}"')
        assert await verify_network_bootstrap_sources(connection, receipt) == receipt
    await connection.execute(
        f"CREATE ROLE \"{bootstrap_access_fixture.reader}\" LOGIN PASSWORD '{bootstrap_access_fixture.password}'"
    )
    with pytest.raises(bootstrap_access.BootstrapReadAccessError):
        await _change_bootstrap_access(bootstrap_access_fixture)
    assert await bootstrap_access._acl(connection, receipt) == proof["before"]
    assert await _verify_bootstrap_access(bootstrap_access_fixture) == receipt


@pytest.mark.parametrize("phase", ["install", "revoke"])
async def test_bootstrap_access_cancelled_acl_change_rolls_back_savepoint(bootstrap_access_fixture, monkeypatch, phase):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    if phase == "revoke":
        await _change_bootstrap_access(bootstrap_access_fixture)
    before = await bootstrap_access._acl(connection, receipt)
    original_grant = bootstrap_access._grant
    entered, release = asyncio.Event(), asyncio.Event()

    async def interrupted(*args, **kwargs):
        await original_grant(*args, **kwargs)
        entered.set()
        await release.wait()

    monkeypatch.setattr(bootstrap_access, "_grant", interrupted)
    async with connection.transaction(isolation="repeatable_read"):
        await connection.execute("CREATE TEMP TABLE access_marker(value int) ON COMMIT DROP")
        pending = asyncio.create_task(
            (
                bootstrap_access.revoke_bootstrap_read_access
                if phase == "revoke"
                else bootstrap_access.install_bootstrap_read_access
            )(connection, receipt, reader_role=bootstrap_access_fixture.reader)
        )
        await asyncio.wait_for(entered.wait(), 5)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert await bootstrap_access._acl(connection, receipt) == before
        await connection.execute("INSERT INTO access_marker VALUES(1)")
    assert (await bootstrap_access._proof(connection, receipt) is None) is (phase == "install")
    assert await _verify_bootstrap_access(bootstrap_access_fixture) == receipt


async def test_bootstrap_access_untrusted_installer_and_shared_reader_membership_refuse(bootstrap_access_fixture):
    async with _bootstrap_access_login(
        bootstrap_access_fixture.stranger, bootstrap_access_fixture.password
    ) as connection:
        async with connection.transaction(isolation="repeatable_read"):
            with pytest.raises(bootstrap_access.BootstrapReadAccessError):
                await bootstrap_access.install_bootstrap_read_access(
                    connection, bootstrap_access_fixture.receipt, reader_role=bootstrap_access_fixture.reader
                )
    await bootstrap_access_fixture.fixture.connection.execute(
        f'GRANT "{bootstrap_access_fixture.receipt.runtime_roles[0]}" TO "{bootstrap_access_fixture.reader}"'
    )
    with pytest.raises(bootstrap_access.BootstrapReadAccessError):
        await _change_bootstrap_access(bootstrap_access_fixture)
    assert (
        await bootstrap_access._proof(bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt)
        is None
    )


async def test_bootstrap_access_connected_actor_cannot_reach_source_owner(bootstrap_access_fixture):
    connection = bootstrap_access_fixture.fixture.connection
    await connection.execute(f'GRANT "{bootstrap_access_fixture.reader}" TO "{bootstrap_access_fixture.stranger}"')
    await connection.execute(
        f'GRANT "{bootstrap_access_fixture.fixture.roles["owner"]}" TO "{bootstrap_access_fixture.stranger}"'
    )
    assert not await connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'SET')",
        bootstrap_access_fixture.reader,
        bootstrap_access_fixture.fixture.roles["owner"],
    )
    with pytest.raises(ValueError):
        await _change_bootstrap_access(bootstrap_access_fixture)
    assert await bootstrap_access._proof(connection, bootstrap_access_fixture.receipt) is None


async def test_bootstrap_dependency_preserves_original_readers_with_incoming_creator(bootstrap_db):
    """A trusted creator's owner reach does not become an ordinary loader's reach."""
    from process.network_custom_address_source import _artifact_dependency_identity

    fixture = bootstrap_db
    receipt = await _bootstrap_prepare(fixture, await _bootstrap_specification(fixture))
    connection = fixture.connection
    assert await connection.fetchval("SELECT pg_has_role($1,$2,'SET')", fixture.roles["publisher"], receipt.owner_role)
    assert not await connection.fetchval(
        "SELECT pg_has_role($1,$2,'MEMBER')", fixture.roles["loader"], receipt.owner_role
    )
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        assert (
            await bootstrap_access.bootstrap_pin_read_roles(connection, receipt.address_source, receipt.runtime_roles)
            == receipt.runtime_roles
        )
        assert await _artifact_dependency_identity(
            connection, receipt.address_source, (fixture.roles["reader"],), address=True
        )


@pytest.mark.parametrize(
    "drift",
    ["direct_owner", "transitive_owner", "set_owner", "transitive_set_owner", "write", "create", "grant", "missing"],
)
async def test_bootstrap_dependency_checks_output_only_actor_reach(bootstrap_access_fixture, drift):
    """Output actors need no input SELECT but cannot reach input ownership or writes."""
    from process.network_custom_address_source import _artifact_dependency_identity, _source_identity

    fixture = bootstrap_access_fixture
    connection, receipt = fixture.fixture.connection, fixture.receipt
    actor, bridge = fixture.stranger, fixture.reader
    roles = tuple(sorted((*receipt.runtime_roles, actor)))
    schema = '"' + receipt.address_source.schema_name + '"'
    before_acl = await bootstrap_access._acl(connection, receipt)
    async with connection.transaction(isolation="repeatable_read"):
        assert not await connection.fetchval(
            "SELECT has_table_privilege($1,$2::oid,'SELECT')", actor, receipt.relation_oids[0][1]
        )
        expected = await _artifact_dependency_identity(connection, receipt.address_source, roles, address=True)
        with pytest.raises(NetworkCustomAddressSourceError, match="bootstrap read access differs"):
            await _source_identity(connection, receipt.address_source, roles, address=True)
        changed = connection.transaction()
        await changed.start()
        try:
            options = "INHERIT FALSE, SET TRUE" if "set_owner" in drift else "INHERIT TRUE, SET TRUE"
            statements_by_drift = {
                "direct_owner": (f'GRANT "{receipt.owner_role}" TO "{actor}" WITH {options}',),
                "set_owner": (f'GRANT "{receipt.owner_role}" TO "{actor}" WITH {options}',),
                "transitive_owner": (
                    f'GRANT "{receipt.owner_role}" TO "{bridge}" WITH {options}',
                    f'GRANT "{bridge}" TO "{actor}" WITH {options}',
                ),
                "transitive_set_owner": (
                    f'GRANT "{receipt.owner_role}" TO "{bridge}" WITH {options}',
                    f'GRANT "{bridge}" TO "{actor}" WITH {options}',
                ),
                "write": (f'GRANT UPDATE ON {schema}.npi TO "{actor}"',),
                "create": (f'GRANT CREATE ON SCHEMA {schema} TO "{actor}"',),
                "grant": (f'GRANT SELECT ON {schema}.npi TO "{actor}" WITH GRANT OPTION',),
                "missing": (),
            }
            for statement in statements_by_drift[drift]:
                await connection.execute(statement)
            if "owner" in drift:
                assert await bootstrap_access._acl(connection, receipt) == before_acl
                assert await connection.fetchval("SELECT pg_has_role($1,$2,'SET')", actor, receipt.owner_role)
                if "set_owner" in drift:
                    assert not await connection.fetchval("SELECT pg_has_role($1,$2,'USAGE')", actor, receipt.owner_role)
            checked_roles = ("missing_" + uuid4().hex,) if drift == "missing" else roles
            with pytest.raises(NetworkCustomAddressSourceError, match="bootstrap read access differs"):
                await _artifact_dependency_identity(connection, receipt.address_source, checked_roles, address=True)
        finally:
            await changed.rollback()
        assert await _artifact_dependency_identity(connection, receipt.address_source, roles, address=True) == expected
        assert await bootstrap_access._acl(connection, receipt) == before_acl


async def test_bootstrap_access_proof_creation_does_not_expand_owner_database_privileges(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f'REVOKE CREATE ON DATABASE {database} FROM "{receipt.owner_role}"')
    await _change_bootstrap_access(bootstrap_access_fixture)
    assert not await connection.fetchval(
        "SELECT has_database_privilege($1::name,current_database(),'CREATE')", receipt.owner_role
    )
    assert await _verify_bootstrap_access(bootstrap_access_fixture) == receipt


async def test_bootstrap_access_install_requires_explicit_repeatable_read(bootstrap_access_fixture):
    with pytest.raises(ValueError):
        await bootstrap_access.install_bootstrap_read_access(
            bootstrap_access_fixture.fixture.connection,
            bootstrap_access_fixture.receipt,
            reader_role=bootstrap_access_fixture.reader,
        )
    assert (
        await bootstrap_access._proof(bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt)
        is None
    )


@pytest.mark.parametrize(
    "drift",
    [
        "write",
        "grant_option",
        "partial",
        "extra_reader",
        "role_reach",
        "role_oid",
        "rls",
        "trigger",
        "default",
        "check",
        "index",
        "proof_owner",
        "proof_object",
    ],
)
async def test_bootstrap_access_native_drift_refuses(bootstrap_access_fixture, drift):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    await _change_bootstrap_access(bootstrap_access_fixture)
    schema = '"' + receipt.address_source.schema_name + '"'
    statement_by_kind = {
        "write": f'GRANT UPDATE ON {schema}.npi TO "{bootstrap_access_fixture.reader}"',
        "grant_option": f'GRANT SELECT ON {schema}.npi TO "{bootstrap_access_fixture.reader}" WITH GRANT OPTION',
        "partial": f'REVOKE SELECT ON {schema}.npi FROM "{bootstrap_access_fixture.reader}"',
        "extra_reader": f'GRANT SELECT ON {schema}.npi TO "{bootstrap_access_fixture.stranger}"',
        "role_reach": f'GRANT "{bootstrap_access_fixture.fixture.roles["owner"]}" TO "{bootstrap_access_fixture.reader}"',
        "rls": f"ALTER TABLE {schema}.npi ENABLE ROW LEVEL SECURITY",
        "default": f"ALTER TABLE {schema}.npi ALTER COLUMN provider_first_name SET DEFAULT current_user",
        "check": f"ALTER TABLE {schema}.npi ADD CHECK(npi>0)",
        "index": f"ALTER INDEX {schema}.npi_pkey SET (fillfactor=80)",
        "proof_owner": f'ALTER SCHEMA "{bootstrap_access._namespace(receipt)}" OWNER TO "{bootstrap_access_fixture.stranger}"',
        "proof_object": f'CREATE TABLE "{bootstrap_access._namespace(receipt)}".unexpected(value int)',
    }
    async with connection.transaction():
        if drift == "trigger":
            await connection.execute(
                f"CREATE FUNCTION {schema}.unexpected() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$"
            )
            await connection.execute(
                f"CREATE TRIGGER unexpected BEFORE UPDATE ON {schema}.npi FOR EACH ROW EXECUTE FUNCTION {schema}.unexpected()"
            )
        elif drift == "role_oid":
            await connection.execute(f'DROP OWNED BY "{bootstrap_access_fixture.reader}"')
            await connection.execute(f'DROP ROLE "{bootstrap_access_fixture.reader}"')
            await connection.execute(
                f"CREATE ROLE \"{bootstrap_access_fixture.reader}\" LOGIN PASSWORD '{bootstrap_access_fixture.password}'"
            )
            await bootstrap_access._grant(connection, receipt, bootstrap_access_fixture.reader)
        else:
            await connection.execute(statement_by_kind[drift])
    with pytest.raises(ValueError):
        await _verify_bootstrap_access(bootstrap_access_fixture)


async def test_bootstrap_access_forged_successor_and_stale_pin_refuse(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    proof = await _change_bootstrap_access(bootstrap_access_fixture)
    proof["bootstrap_sha256"] = "0" * 64
    literal = await connection.fetchval("SELECT quote_literal($1::text)", json.dumps(proof))
    await connection.execute(f'COMMENT ON SCHEMA "{bootstrap_access._namespace(receipt)}" IS {literal}')
    with pytest.raises(bootstrap_access.BootstrapReadAccessError):
        await _verify_bootstrap_access(bootstrap_access_fixture)


async def test_bootstrap_access_owner_content_change_refuses_before_grant(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    await connection.execute(f"UPDATE \"{receipt.address_source.schema_name}\".npi SET provider_first_name='Changed'")
    with pytest.raises(ValueError):
        await _change_bootstrap_access(bootstrap_access_fixture)
    assert await bootstrap_access._proof(connection, receipt) is None
    assert not await connection.fetchval(
        "SELECT has_table_privilege($1::name,$2::oid,'SELECT')",
        bootstrap_access_fixture.reader,
        dict(receipt.relation_oids)["npi"],
    )


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
async def test_bootstrap_access_dormant_serial_default_never_executes(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    sequence = await connection.fetchval(
        "SELECT pg_get_serial_sequence($1,'npi')", bootstrap_access_fixture.fixture.source_schema + ".npi"
    )
    sequence_oid = await connection.fetchval("SELECT $1::regclass::oid", sequence)
    before = await connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}")
    await _change_bootstrap_access(bootstrap_access_fixture)
    async with _bootstrap_access_login(bootstrap_access_fixture.reader, bootstrap_access_fixture.password) as reader:
        assert await _verify_bootstrap_access(bootstrap_access_fixture, reader) == receipt
        assert not await reader.fetchval(
            "SELECT has_sequence_privilege(current_user,$1::oid,'SELECT,USAGE,UPDATE')", sequence_oid
        )
        assert not await reader.fetchval(
            "SELECT has_schema_privilege(current_user,$1,'USAGE')", bootstrap_access_fixture.fixture.source_schema
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await reader.fetchval("SELECT nextval($1::oid::regclass)", sequence_oid)
    assert await connection.fetchrow(f"SELECT last_value,is_called FROM {sequence}") == before


@pytest.mark.parametrize("bootstrap_access_fixture", ["serial"], indirect=True)
@pytest.mark.parametrize(
    "drift", ["sequence_grant", "indirect_grant", "npi_write", "sequence_reference", "default_function"]
)
async def test_bootstrap_access_dormant_serial_drift_refuses(bootstrap_access_fixture, drift):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    if drift == "indirect_grant":
        await connection.execute(f'GRANT "{bootstrap_access_fixture.reader}" TO "{bootstrap_access_fixture.stranger}"')
    sequence = await connection.fetchval(
        "SELECT pg_get_serial_sequence($1,'npi')", bootstrap_access_fixture.fixture.source_schema + ".npi"
    )
    await _change_bootstrap_access(bootstrap_access_fixture)
    npi_table = '"' + receipt.address_source.schema_name + '".npi'
    if drift in {"sequence_grant", "indirect_grant"}:
        role = bootstrap_access_fixture.stranger if drift == "indirect_grant" else bootstrap_access_fixture.reader
        await connection.execute(f'GRANT USAGE ON SEQUENCE {sequence} TO "{role}"')
    elif drift == "npi_write":
        await connection.execute(f'GRANT INSERT ON {npi_table} TO "{bootstrap_access_fixture.reader}"')
    elif drift == "sequence_reference":
        target = '"' + bootstrap_access_fixture.fixture.source_schema + '".unexpected_sequence'
        await connection.execute(f"CREATE SEQUENCE {target}")
        literal = await connection.fetchval("SELECT quote_literal($1::text)", target)
        await connection.execute(f"ALTER TABLE {npi_table} ALTER COLUMN npi SET DEFAULT nextval({literal}::regclass)")
    else:
        target = '"' + bootstrap_access_fixture.fixture.source_schema + '".unexpected_default'
        await connection.execute(f"CREATE FUNCTION {target}() RETURNS bigint LANGUAGE sql AS 'SELECT 1::bigint'")
        await connection.execute(f"ALTER TABLE {npi_table} ALTER COLUMN npi SET DEFAULT {target}()")
    with pytest.raises(ValueError):
        await _verify_bootstrap_access(bootstrap_access_fixture)


async def test_bootstrap_access_grants_are_atomic_and_reader_is_uninterrupted(bootstrap_access_fixture):
    connection, receipt = bootstrap_access_fixture.fixture.connection, bootstrap_access_fixture.receipt
    administrator = await connection.fetchval("SELECT session_user")
    async with (
        _bootstrap_access_login(bootstrap_access_fixture.reader, bootstrap_access_fixture.password) as reader,
        _bootstrap_access_login(administrator) as other,
    ):
        async with connection.transaction(isolation="repeatable_read"):
            await bootstrap_access.install_bootstrap_read_access(
                connection, receipt, reader_role=bootstrap_access_fixture.reader
            )
            assert not await reader.fetchval(
                "SELECT has_table_privilege(current_user,$1::oid,'SELECT')", dict(receipt.relation_oids)["npi"]
            )
            await other.execute(f'SET ROLE "{receipt.runtime_roles[0]}"')
            assert await other.fetchval(f'SELECT count(*) FROM "{receipt.address_source.schema_name}".npi') == 1
            await other.execute("RESET ROLE")
            async with other.transaction(isolation="repeatable_read"):
                with pytest.raises(asyncpg.LockNotAvailableError):
                    await bootstrap_access.install_bootstrap_read_access(
                        other, receipt, reader_role=bootstrap_access_fixture.reader
                    )
        assert await reader.fetchval(f'SELECT count(*) FROM "{receipt.address_source.schema_name}".npi') == 1


async def _alter_dependency_authority(connection, receipt, actor, reader, drift):
    owner, schema = '"' + receipt.owner_role + '"', '"' + receipt.address_source.schema_name + '"'
    changes_by_drift = {
        "set_owner": (f'GRANT {owner} TO "{actor}" WITH INHERIT FALSE, SET TRUE',),
        "transitive_set_owner": (
            f'GRANT {owner} TO "{reader}" WITH INHERIT FALSE, SET TRUE',
            f'GRANT "{reader}" TO "{actor}" WITH INHERIT FALSE, SET TRUE',
        ),
        "write": (f'GRANT UPDATE ON {schema}.entity_address_unified TO "{actor}"',),
        "create": (f'GRANT CREATE ON SCHEMA {schema} TO "{actor}"',),
    }
    for statement in changes_by_drift.get(drift, ()):
        await connection.execute(statement)
    actors = {"owner_alias": (receipt.owner_role,), "missing": ("missing_" + uuid4().hex,)}.get(drift, (actor,))
    pin = replace(receipt.address_source, generation_id="b" * 64) if drift == "generation" else receipt.address_source
    return pin, actors


@pytest.mark.parametrize(
    "drift", ["none", "owner_alias", "set_owner", "transitive_set_owner", "write", "create", "missing", "generation"]
)
async def test_sealed_composition_input_requires_readers_and_refuses_output_actor_authority(custom_db, drift):
    fixture = custom_db
    receipt = await _prepare(fixture, (await _seed(fixture)).revision)
    connection, actor, reader = fixture.connection, fixture.roles["outsider"], fixture.roles["reader"]
    pin = receipt.address_source
    actors = (actor,)
    assert not await connection.fetchval(
        "SELECT has_table_privilege($1,$2::oid,'SELECT')", actor, receipt.address_table_oid
    )
    async with connection.transaction(isolation="repeatable_read"):
        expected = await _artifact_dependency_identity(connection, pin, actors, address=True)
        with pytest.raises(NetworkCustomAddressSourceError):
            await _artifact_dependency_identity(connection, fixture.base, actors, address=True)
        changed = connection.transaction()
        await changed.start()
        try:
            pin, actors = await _alter_dependency_authority(connection, receipt, actor, reader, drift)
            if drift == "none":
                assert await _artifact_dependency_identity(connection, pin, actors, address=True) == expected
            else:
                with pytest.raises(NetworkCustomAddressSourceError):
                    await _artifact_dependency_identity(connection, pin, actors, address=True)
        finally:
            await changed.rollback()
        assert (
            await _artifact_dependency_identity(connection, receipt.address_source, (actor,), address=True) == expected
        )
