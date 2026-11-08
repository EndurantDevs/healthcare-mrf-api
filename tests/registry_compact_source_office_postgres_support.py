# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native EAU authority containing accepted synthetic CMS offices before sealing."""

import json
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
from sqlalchemy import MetaData, text

from db.models import EntityAddressUnified
from process import entity_address_alias_guard as aliases
from process import network_address_artifact as artifact
from process.network_address_projection import PinnedAddressSource
from tests.source_contribution_custody_postgres_support import install_source_contribution_custody
from tests.test_address_numeric_grid_alias_runtime_db import _create_alias_probe_schema
from tests.test_network_initial_source_office_bindings_postgres import initial_cms_db


async def _cleanup_native(fixture, schema, roles_by_kind, created_namespaces, created_roles):
    await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    if created_namespaces[0]:
        owner = await fixture.connection.fetchval(
            "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname='hp_snapshot_retention'"
        )
        assert owner == roles_by_kind["owner"]
        await fixture.connection.execute("DROP SCHEMA hp_snapshot_retention")
    for role in reversed(created_roles):
        if await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
            await fixture.connection.execute(f'DROP OWNED BY "{role}"')
            await fixture.connection.execute(f'DROP ROLE "{role}"')
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
    )


async def _native_owner(fixture, roles_by_kind, created_namespaces, created_roles):
    """Preserve an installed native retention owner; record only newly created roles."""
    retained_owner = await fixture.connection.fetchval(
        "SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname='hp_snapshot_retention'"
    )
    if retained_owner is None:
        await fixture.connection.execute(f'CREATE ROLE "{roles_by_kind["owner"]}" NOLOGIN')
        created_roles.append(roles_by_kind["owner"])
        await fixture.connection.execute(
            f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{roles_by_kind["owner"]}"'
        )
        created_namespaces[0] = True
    else:
        roles_by_kind["owner"] = retained_owner


async def _native_storage(fixture, schema, roles_by_kind, created_namespaces, created_roles, office_setup=None):
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", schema) is None
    await _native_owner(fixture, roles_by_kind, created_namespaces, created_roles)
    await _create_alias_probe_schema(fixture.connection, schema, "source_office_" + uuid4().hex)
    await fixture.connection.execute(f'DROP TABLE "{schema}".entity_address_unified')
    async with fixture.engine.begin() as setup:
        table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
        await setup.run_sync(lambda sync: table.create(sync))
    await fixture.connection.execute(
        f'INSERT INTO "{schema}".entity_address_unified SELECT * FROM "{fixture.source_schema}".entity_address_unified'
    )
    await fixture.connection.execute(f'UPDATE "{schema}".entity_address_unified SET lat=34,long=-118')
    import ptg2_address_canon

    canonical = ptg2_address_canon.canonicalize_batch(
        [("123 Example Street", "Suite 3", "Sample City", "CA", "90210", "US")]
    )[0]
    office_patch_by_field = dict(
        location_key="b" * 64,
        second_line="Suite 3",
        address_key=canonical["address_key"],
        premise_key=canonical["premise_key"],
    )
    await fixture.connection.execute(
        f'INSERT INTO "{schema}".entity_address_unified SELECT (jsonb_populate_record(NULL::"{schema}".entity_address_unified,to_jsonb(office)||$1::jsonb)).* FROM "{schema}".entity_address_unified office WHERE location_key=$2',
        json.dumps(office_patch_by_field),
        "a" * 64,
    )
    if office_setup is not None:
        await office_setup(fixture.connection, schema)
    for statement in aliases.generation_guard_statements(schema):
        await fixture.connection.execute(statement)
    for name in aliases.GUARDS:
        await fixture.connection.execute(f'ALTER FUNCTION "{schema}"."{name}"() OWNER TO "{roles_by_kind["owner"]}"')
    for name in artifact.generation.RELATION_NAMES[1:]:
        await fixture.connection.execute(f'CREATE TABLE "{schema}"."{name}" (id integer PRIMARY KEY)')
    await fixture.connection.execute(
        f'CREATE TABLE "{schema}".entity_address_result_generation(singleton boolean PRIMARY KEY CHECK(singleton),local_lineage_id uuid NOT NULL,local_generation bigint NOT NULL,origin_lineage_id uuid NOT NULL,origin_generation bigint NOT NULL,published_at timestamptz NOT NULL,relation_oids bigint[] NOT NULL)'
    )
    await fixture.connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles_by_kind["owner"]}"')
    for name in (
        *artifact.generation.RELATION_NAMES,
        "entity_address_result_generation",
        "address_alias_state_v1",
        "address_alias_v1",
    ):
        await fixture.connection.execute(f'ALTER TABLE "{schema}"."{name}" OWNER TO "{roles_by_kind["owner"]}"')
    oids = [
        await fixture.connection.fetchval("SELECT to_regclass($1)::oid::bigint", f'"{schema}"."{name}"')
        for name in artifact.generation.RELATION_NAMES
    ]
    lineage = uuid4()
    await fixture.connection.execute(
        f'INSERT INTO "{schema}".entity_address_result_generation VALUES(true,$1,1,$1,1,$2,$3)',
        lineage,
        datetime(2026, 1, 1, tzinfo=UTC),
        oids,
    )


@asynccontextmanager
async def compact_cms_offices(custom, reviewed_source, monkeypatch, *, office_setup=None):
    """Keep real accepted source custody and distinct native address/final owners."""
    async with AsyncExitStack() as stack:
        generator = await stack.enter_async_context(
            aclosing(initial_cms_db.__wrapped__("npi", reviewed_source, custom))
        )
        fixture = await anext(generator)
        schema = "source_office_address_" + uuid4().hex
        roles_by_kind = {kind: "source_office_" + kind + "_" + uuid4().hex for kind in ("owner", "publisher")}
        created_namespaces = [False]
        created_roles = []
        stack.push_async_callback(_cleanup_native, fixture, schema, roles_by_kind, created_namespaces, created_roles)
        await _native_storage(fixture, schema, roles_by_kind, created_namespaces, created_roles, office_setup)
        await install_source_contribution_custody(fixture, reviewed_source[0][2], stack)
        for role in reviewed_source[0][2].retained_epoch.runtime_roles:
            await fixture.connection.execute(
                f'GRANT "{role}" TO "{fixture.executor_role}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
            )
        runtime_roles = tuple(sorted(set(fixture.writer_roles["loader_roles"] + fixture.writer_roles["reader_roles"])))
        password = uuid4().hex
        await fixture.connection.execute(f"CREATE ROLE \"{roles_by_kind['publisher']}\" LOGIN PASSWORD '{password}'")
        created_roles.append(roles_by_kind["publisher"])
        await fixture.connection.execute(f'GRANT "{roles_by_kind["owner"]}" TO "{roles_by_kind["publisher"]}"')
        for role in runtime_roles:
            await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"')
            await fixture.connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"')
        publisher = await asyncpg.connect(
            fixture.engine.url.set(
                drivername="postgresql", username=roles_by_kind["publisher"], password=password
            ).render_as_string(hide_password=False)
        )
        stack.push_async_callback(publisher.close)
        async with publisher.transaction(isolation="repeatable_read", readonly=True):
            authority = await artifact.generation.read_entity_address_result_generation_authority(
                artifact._CatalogSession(publisher), schema_name=schema
            )
            address_source = PinnedAddressSource(
                schema, "entity_address_unified", artifact._digest(artifact._authority_document(authority))
            )
            descriptor = await artifact.capture_network_address_artifact(
                publisher, address_source, runtime_roles=runtime_roles
            )
        yield SimpleNamespace(
            fixture=fixture,
            source=address_source,
            source_pin=reviewed_source[0][2],
            reviewed=reviewed_source,
            address=SimpleNamespace(
                admin=fixture.connection, publisher=publisher, schema=schema, roles=roles_by_kind, descriptor=descriptor
            ),
            runtime_roles=runtime_roles,
            targets=[],
            requests=[],
        )
