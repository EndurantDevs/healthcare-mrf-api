# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixtures for configured candidate writers and administrator-frozen generations."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError as DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine as create_async_engine

from process import registry_source_admission as admission
from process.network_membership_pipeline import prepare_and_publish_network_candidate
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_candidate_custody_preparation import (
    split_registry_candidate_custody,
)
from process.registry_cms_source_notifications import CREATE_TABLE_SQL
from process.registry_management_permissions import (
    _aca_controls,
    _catalog,
    assume_registry_source_owner,
    install_registry_management_permissions,
)
from process.registry_publication_worker import (
    _configured_worker_runtime,
    parse_registry_publication_worker_config,
)
from process.registry_publication_worker_permissions import (
    install_registry_publication_worker_permissions,
)
from process.registry_source_admission_receipt import _TABLES as SOURCE_TABLES
from tests.registry_candidate_custody_postgres_support import administrative_freeze_callback
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import _candidate as _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_publication_worker import configuration
from tests.test_registry_source_admission_postgres import _HEADERS as _HEADERS
from tests.test_registry_source_admission_postgres import _edition, _input

pytestmark = pytest.mark.asyncio


def _q(name):
    return '"' + name.replace('"', '""') + '"'


async def _relations(connection, schema, names=None):
    rows = await connection.fetch(
        "SELECT c.relname,c.relkind "
        "FROM pg_class c WHERE c.relnamespace=to_regnamespace($1) "
        "AND ($2::text[] IS NULL OR c.relname=ANY($2::text[])) ORDER BY c.relname",
        schema,
        names,
    )
    relations = []
    for record in rows:
        relation_by_field = dict(record)
        if record["relkind"] == b"r":
            relation_by_field["rows"] = await connection.fetchval(
                f"SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb)::text "
                f"FROM {_q(schema)}.{_q(record['relname'])} r"
            )
        relations.append(relation_by_field)
    return relations


async def _protected(fixture):
    connection = fixture.fixture.connection
    return {
        "source6": await _relations(connection, fixture.fixture.control_schema, list(SOURCE_TABLES)),
        "source_bytes": await _relations(connection, fixture.fixture.source_schema),
        "cms": await _relations(
            connection,
            fixture.fixture.control_schema,
            ["registry_cms_source_notification"],
        ),
    }


@asynccontextmanager
async def _login(fixture, kind):
    connection = await asyncpg.connect(
        fixture.dsn,
        user=fixture.roles_by_kind[kind],
        password=fixture.role_password,
    )
    try:
        yield connection
    finally:
        await connection.close()


async def _adopt_incumbent(fixture):
    from tests.test_registry_publication_worker_permissions_postgres import _options

    base = fixture.fixture
    admin, schema, roles_by_kind = base.connection, base.control_schema, fixture.roles_by_kind
    password, database, created_roles = fixture.role_password, fixture.database, fixture.created_roles
    for kind, role in roles_by_kind.items():
        if kind == "queue":
            continue
        created_roles.append(role)
        is_login = kind not in {"physical", "custody"}
        await admin.execute(f"CREATE ROLE {_q(role)} " + (f"LOGIN PASSWORD '{password}'" if is_login else "NOLOGIN"))
    await admin.execute(f"ALTER DATABASE {_q(database)} OWNER TO {_q(roles_by_kind['queue'])}")
    await admin.execute(CREATE_TABLE_SQL.format(namespace=_q(schema)))
    tables, sequences = await _catalog(admin, schema)
    controls = await _aca_controls(admin, schema)
    await admin.execute(f"ALTER SCHEMA {_q(schema)} OWNER TO {_q(roles_by_kind['queue'])}")
    for relation in tables + sequences + controls:
        kind = "SEQUENCE" if relation in sequences else "TABLE"
        await admin.execute(
            f"ALTER {kind} {_q(schema)}.{_q(relation['relname'])} OWNER TO {_q(roles_by_kind['queue'])}"
        )
    await admin.execute(
        f"ALTER TABLE {_q(schema)}.registry_cms_source_notification OWNER TO {_q(roles_by_kind['queue'])}"
    )
    async with admin.transaction():
        await install_registry_management_permissions(
            admin,
            api_role=roles_by_kind["api"],
            owner_role=roles_by_kind["queue"],
            control_schema=schema,
        )
    source_bytes = _input()
    fixture.edition = _edition(source_bytes)
    async with admin.transaction():
        await assume_registry_source_owner(admin, roles_by_kind["queue"], control_schema=schema)
        fixture.admission = await admission.admit_cms_mlr_edition(
            admin, source_bytes, fixture.edition, control_schema=schema
        )
    async with admin.transaction():
        await install_registry_publication_worker_permissions(admin, **_options(fixture))


async def _enroll_source_readers(fixture):
    base = fixture.fixture
    admin, schema, roles_by_kind = base.connection, base.control_schema, fixture.roles_by_kind
    # An independent direct reader avoids new edges into the incumbent Reader graph.
    await admin.execute(f"GRANT USAGE ON SCHEMA {_q(base.source_schema)} TO {_q(roles_by_kind['reader'])}")
    await admin.execute(
        f"GRANT SELECT ON ALL TABLES IN SCHEMA {_q(base.source_schema)} TO {_q(roles_by_kind['reader'])}"
    )
    await admin.execute(
        f"GRANT {_q(roles_by_kind['reader'])} TO {_q(roles_by_kind['execution'])} WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
    )
    await admin.execute(
        f"GRANT {_q(roles_by_kind['execution'])} TO {_q(roles_by_kind['custodian'])} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE"
    )
    fixture.readers = tuple(sorted((roles_by_kind["reader"], base.roles["reader"])))
    await admin.execute(f"GRANT USAGE ON SCHEMA {_q(schema)} TO {_q(base.roles['reader'])}")
    for kind in ("physical", "reader", "custody"):
        await admin.execute(f"GRANT USAGE ON SCHEMA {_q(schema)} TO {_q(roles_by_kind[kind])}")
    for kind in ("physical", "reader"):
        await admin.execute(f"GRANT SELECT ON {_q(schema)}.network_membership_candidate TO {_q(roles_by_kind[kind])}")
        await admin.execute(f"GRANT SELECT ON {_q(schema)}.network_candidate_custody TO {_q(roles_by_kind[kind])}")
    for reader in fixture.readers:
        for relation in (
            "network_membership_candidate",
            "network_candidate_custody",
            "network_serving_control",
            "network_serving_manifest",
        ):
            await admin.execute(f"GRANT SELECT ON {_q(schema)}.{_q(relation)} TO {_q(reader)}")


async def _configure_custody(fixture):
    base = fixture.fixture
    admin, schema, roles_by_kind = base.connection, base.control_schema, fixture.roles_by_kind
    password = fixture.role_password
    url = make_url(fixture.dsn)
    document = configuration()
    for key, kind in [
        ("lifecycle_dsn", "lifecycle"),
        ("execution_dsn", "execution"),
        ("custody_dsn", "custodian"),
    ]:
        document[key] = url.set(username=roles_by_kind[kind], password=password).render_as_string(hide_password=False)
    document["consumer"] = dict(
        control_schema=schema,
        lifecycle_role=roles_by_kind["lifecycle"],
        execution_role=roles_by_kind["execution"],
        api_role=roles_by_kind["api"],
        queue_owner_role=roles_by_kind["queue"],
        physical_owner_role=roles_by_kind["physical"],
        loader_roles=[],
        reader_roles=list(fixture.readers),
    )
    applications = sorted(
        (set(roles_by_kind.values()) | set(base.roles.values()) | set(fixture.readers)) - {roles_by_kind["custody"]}
    )
    document["preparation_roles"] = {
        "namespace_owner_role": roles_by_kind["custody"],
        "publisher_role": roles_by_kind["execution"],
        "application_roles": applications,
    }
    fixture.config = parse_registry_publication_worker_config(json.dumps(document))
    for kind in ("lifecycle", "execution", "custodian", "reader"):
        await admin.execute(
            f'ALTER ROLE {_q(roles_by_kind[kind])} SET temp_file_limit TO "{fixture.config.sql_limits.temp_file_limit_bytes // 1024}kB"'
        )


async def _remove_worker_roles(fixture):
    base = fixture.fixture
    admin, schema, roles_by_kind = base.connection, base.control_schema, fixture.roles_by_kind
    database, created_roles = fixture.database, fixture.created_roles
    original_owner = fixture.original_owner
    await admin.execute("RESET ROLE")
    await admin.execute(f"ALTER DATABASE {_q(database)} OWNER TO {_q(original_owner)}")
    await admin.execute(f"DROP SCHEMA IF EXISTS {_q(schema)} CASCADE")
    for owned in await admin.fetch(
        "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)",
        roles_by_kind["physical"],
    ):
        await admin.execute(f"DROP SCHEMA {_q(owned['nspname'])} CASCADE")
    for owned in await admin.fetch(
        "SELECT nspname FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)",
        roles_by_kind["custody"],
    ):
        await admin.execute(f"DROP SCHEMA {_q(owned['nspname'])} CASCADE")
    for role in reversed(created_roles):
        await admin.execute(f"DROP OWNED BY {_q(role)}")
        await admin.execute(f"DROP ROLE {_q(role)}")
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles)


@pytest.fixture
async def split_db(custom_db):
    """Retain an admitted source owner while enrolling independent ordinary workers."""
    token = uuid4().hex[:16]
    roles_by_kind = {
        kind: "split_" + kind + "_" + token
        for kind in (
            "api",
            "lifecycle",
            "execution",
            "physical",
            "custodian",
            "custody",
            "reader",
        )
    }
    roles_by_kind["queue"] = custom_db.roles["owner"]
    database, original_owner = await custom_db.connection.fetchrow(
        "SELECT datname,pg_get_userbyid(datdba) FROM pg_database WHERE datname=current_database()"
    )
    fixture = SimpleNamespace(
        fixture=custom_db,
        seed=await _seed(custom_db),
        roles_by_kind=roles_by_kind,
        role_password=uuid4().hex,
        dsn=os.environ["NETWORK_REGISTRY_TEST_DSN"],
        database=database,
        original_owner=original_owner,
        created_roles=[],
    )
    try:
        await _adopt_incumbent(fixture)
        await _enroll_source_readers(fixture)
        await _configure_custody(fixture)
        fixture.protected = await _protected(fixture)
        yield fixture
    finally:
        await _remove_worker_roles(fixture)


async def _split(fixture, connection):
    return await split_registry_candidate_custody(
        connection,
        owner_role=fixture.roles_by_kind["queue"],
        custody_owner_role=fixture.roles_by_kind["custody"],
        control_schema=fixture.fixture.control_schema,
    )


async def _assert_installer_replay(fixture):
    from tests.test_registry_publication_worker_permissions_postgres import _options

    admin, schema, roles_by_kind = fixture.fixture.connection, fixture.fixture.control_schema, fixture.roles_by_kind
    after = await _relations(admin, schema)
    async with admin.transaction():
        await install_registry_management_permissions(
            admin,
            api_role=roles_by_kind["api"],
            owner_role=roles_by_kind["queue"],
            control_schema=schema,
        )
        await install_registry_publication_worker_permissions(admin, **_options(fixture))
    replayed = await _relations(admin, schema)
    assert [(entry["relname"], entry.get("rows")) for entry in replayed] == [
        (entry["relname"], entry.get("rows")) for entry in after
    ]


async def _install_named_worker_permissions(fixture):
    """Reapply the actual configured grants while preserving retained business rows."""
    await _assert_installer_replay(fixture)
    assert await _protected(fixture) == fixture.protected


async def _prepare_split_dependency(fixture):
    """Use the standalone source's actual administrator permission setup."""
    from process.network_custom_address_source import prepare_custom_address_source

    async with fixture.fixture.connection.transaction(isolation="repeatable_read"):
        return await prepare_custom_address_source(
            fixture.fixture.connection,
            fixture.fixture.base,
            composition_id=str(uuid4()),
            approved_revision=fixture.seed.revision,
            owner_role=fixture.roles_by_kind["custody"],
            runtime_roles=tuple(fixture.config.preparation_roles["application_roles"]),
            control_schema=fixture.fixture.control_schema,
        )


async def _prepare_split_successor(fixture, execution, consumer, dependency, head):
    """Compose through the normal builder route before the administrator freeze."""
    from process.registry_candidate_composition import (
        RegistryCompositionAddressSources,
        compose_registry_membership_candidate,
    )

    request_id = uuid4()
    assert consumer.prepare_address_sources is None
    async with execution.transaction(isolation="repeatable_read", readonly=True):
        previous = (
            await resolve_network_serving_manifest(execution, control_schema=fixture.fixture.control_schema)
            if head
            else None
        )
    return await compose_registry_membership_candidate(
        execution,
        request_id=request_id,
        approved_revision=fixture.seed.revision,
        source_manifest=previous,
        expected_head=head,
        address_sources=RegistryCompositionAddressSources(dependency.address_source),
        writer_roles=dict(owner_role=fixture.roles_by_kind["physical"], loader_roles=(), reader_roles=fixture.readers),
        control_schema=fixture.fixture.control_schema,
    )


async def _publish_successor(fixture):
    """Publish and replay successors while retaining every published named generation."""
    admin, schema, roles_by_kind = fixture.fixture.connection, fixture.fixture.control_schema, fixture.roles_by_kind
    async with (
        AsyncExitStack() as stack,
        _login(fixture, "execution") as execution,
        _login(fixture, "reader") as reader,
    ):
        consumer = await _configured_worker_runtime(fixture.config, stack)
        consumer = replace(consumer, seal_candidate=administrative_freeze_callback(fixture.fixture, consumer))
        dependency = await _prepare_split_dependency(fixture)
        for head in (0, 1):
            copy_target, address = await _prepare_split_successor(fixture, execution, consumer, dependency, head)
            options_by_name = dict(
                owner_role=roles_by_kind["physical"],
                loader_roles=(),
                reader_roles=fixture.readers,
                control_schema=schema,
                seal_candidate=consumer.seal_candidate,
            )
            manifest = await prepare_and_publish_network_candidate(execution, copy_target, address, **options_by_name)
            assert manifest["generation_id"] == head + 1
            assert await prepare_and_publish_network_candidate(execution, copy_target, address, **options_by_name) == {
                **manifest,
                "replayed": True,
            }
            async with reader.transaction(isolation="repeatable_read", readonly=True):
                assert (await resolve_network_serving_manifest(reader, control_schema=schema)).generation_id == head + 1
                for retained in range(1, head + 2):
                    assert (
                        await resolve_network_serving_manifest(reader, generation_id=retained, control_schema=schema)
                    ).generation_id == retained
        assert (
            await admin.fetchval(
                "SELECT count(*) FROM hp_snapshot_retention.preparation WHERE importer_id='network' AND state='published' "
                "AND preparation_id IN (SELECT candidate_id FROM "
                f"{_q(schema)}.network_membership_candidate)"
            )
            == 2
        )


async def _assert_login_denials(fixture, kind):
    schema, roles_by_kind = fixture.fixture.control_schema, fixture.roles_by_kind
    statements = [
        f"SET ROLE {_q(roles_by_kind['queue'])}",
        f"SET ROLE {_q(roles_by_kind['custody'])}",
        f"UPDATE {_q(schema)}.network_candidate_custody SET receipt_json=receipt_json",
        f"UPDATE {_q(schema)}.registry_source_snapshot SET snapshot_id=snapshot_id",
    ]
    async with _login(fixture, kind) as connection:
        for statement in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)


async def _assert_physical_denials(fixture):
    schema, roles_by_kind = fixture.fixture.control_schema, fixture.roles_by_kind
    statements = [
        f"SET ROLE {_q(roles_by_kind['queue'])}",
        f"SET ROLE {_q(roles_by_kind['custody'])}",
        f"UPDATE {_q(schema)}.network_candidate_custody SET receipt_json=receipt_json",
        f"UPDATE {_q(schema)}.registry_source_snapshot SET snapshot_id=snapshot_id",
    ]
    async with _login(fixture, "execution") as connection:
        await connection.execute(f"SET ROLE {_q(roles_by_kind['physical'])}")
        for statement in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)


async def _assert_native_denials(fixture):
    for kind in ("api", "lifecycle", "execution", "reader", "custodian"):
        await _assert_login_denials(fixture, kind)
    await _assert_physical_denials(fixture)
