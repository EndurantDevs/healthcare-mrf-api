# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixtures and assertions for independently owned native candidate custody."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_network_candidate_custody import validate_prepared_candidate_custody
from process import registry_source_admission as admission
from process.network_candidate_custody import verify_network_candidate_custody
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
    _custody_runtime,
    parse_registry_publication_worker_config,
)
from process.registry_publication_worker_permissions import (
    install_registry_publication_worker_permissions,
)
from process.registry_source_admission_receipt import _TABLES as SOURCE_TABLES
from tests.test_network_custom_address_source_postgres import _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_publication_worker import configuration
from tests.test_registry_source_admission_postgres import _HEADERS, _edition, _input

pytestmark = pytest.mark.asyncio


def _q(name):
    return '"' + name.replace('"', '""') + '"'


async def _relations(connection, schema, names=None):
    rows = await connection.fetch(
        "SELECT c.oid,c.relname,c.relowner,c.relacl::text,c.relfilenode,c.relkind, "
        "ARRAY(SELECT jsonb_build_array(a.grantor::bigint,a.grantee::bigint,a.privilege_type,a.is_grantable)::text "
        "FROM aclexplode(CASE WHEN c.relkind IN ('r','S') THEN coalesce(c.relacl,acldefault(c.relkind,c.relowner)) ELSE c.relacl END) a "
        "ORDER BY a.grantor,a.grantee,a.privilege_type,a.is_grantable) AS grants "
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
    roles_by_kind = sorted(fixture.fixture.roles.values())
    return {
        "roles_by_kind": await connection.fetch(
            "SELECT to_jsonb(r)::text FROM pg_roles r WHERE rolname=ANY($1::text[]) ORDER BY oid",
            roles_by_kind,
        ),
        "memberships": await connection.fetch(
            "SELECT m.* FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid "
            "JOIN pg_roles member ON member.oid=m.member WHERE r.rolname=ANY($1::text[]) "
            "OR member.rolname=ANY($1::text[]) ORDER BY m.roleid,m.member",
            roles_by_kind,
        ),
        "source6": await _relations(connection, fixture.fixture.control_schema, list(SOURCE_TABLES)),
        "source_bytes": await _relations(connection, fixture.fixture.source_schema),
        "cms": await _relations(
            connection,
            fixture.fixture.control_schema,
            ["registry_cms_source_notification"],
        ),
        "default_acls": await connection.fetch("SELECT * FROM pg_default_acl ORDER BY oid"),
    }


@asynccontextmanager
async def _login(fixture, kind):
    connection = await asyncpg.connect(
        fixture.dsn,
        user=fixture.roles_by_kind[kind],
        password=fixture.role_password,
    )
    try:
        assert (
            tuple(await connection.fetchrow("SELECT session_user,current_user")) == (fixture.roles_by_kind[kind],) * 2
        )
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
    tables, sequences = await _catalog(admin, await admin.fetchval("SELECT to_regnamespace($1)::oid", schema))
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
    await admin.execute(f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {_q(roles_by_kind['queue'])}")
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
    database = fixture.database
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
    await admin.execute(
        f"GRANT {_q(roles_by_kind['custody'])} TO {_q(roles_by_kind['custodian'])} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE"
    )
    await admin.execute(f"GRANT CREATE ON DATABASE {_q(database)} TO {_q(roles_by_kind['custody'])}")
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
    assert [
        {field_name: field_value for field_name, field_value in relation_fields.items() if field_name != "relacl"}
        for relation_fields in replayed
    ] == [
        {field_name: field_value for field_name, field_value in relation_fields.items() if field_name != "relacl"}
        for relation_fields in after
    ]


async def _assert_ledger_transfer(before_relation, after_relation):
    old_owner, new_owner = before_relation["relowner"], after_relation["relowner"]
    before_grants = [json.loads(grant) for grant in before_relation["grants"]]
    assert all(grantor == old_owner for grantor, _, _, _ in before_grants)
    expected_grants = {
        (new_owner, new_owner if grantee == old_owner else grantee, privilege, grantable)
        for _, grantee, privilege, grantable in before_grants
    }
    assert expected_grants == {tuple(json.loads(grant)) for grant in after_relation["grants"]}


async def _assert_handoff(fixture):
    admin, schema, roles_by_kind = fixture.fixture.connection, fixture.fixture.control_schema, fixture.roles_by_kind
    before = await _relations(admin, schema)
    ledger_oids = set(
        await admin.fetchval(
            "SELECT ARRAY(SELECT indexrelid FROM pg_index WHERE indrelid=$1::regclass) || ARRAY[$1::regclass::oid]",
            _q(schema) + ".network_candidate_custody",
        )
    )
    async with fixture.fixture.engine.begin() as connection:
        receipt = await _split(fixture, connection)
    after = await _relations(admin, schema)
    assert receipt["custody_owner_role"] == roles_by_kind["custody"]
    assert len(before) == len(after)
    assert not await admin.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'MEMBER')", roles_by_kind["physical"], roles_by_kind["custody"]
    )
    for before_relation, after_relation in zip(before, after):
        if before_relation["oid"] in ledger_oids:
            if before_relation["relname"] == "network_candidate_custody":
                await _assert_ledger_transfer(before_relation, after_relation)
            changed_fields = (
                {"relowner", "relacl", "grants"}
                if before_relation["relname"] == "network_candidate_custody"
                else {"relowner", "grants"}
            )
            assert {
                field_name: field_value
                for field_name, field_value in before_relation.items()
                if field_name not in changed_fields
            } == {
                field_name: field_value
                for field_name, field_value in after_relation.items()
                if field_name not in changed_fields
            }
        else:
            assert before_relation == after_relation
    assert await _protected(fixture) == fixture.protected
    await _assert_installer_replay(fixture)
    async with fixture.fixture.engine.begin() as connection:
        with pytest.raises(RuntimeError):
            await connection.run_sync(validate_prepared_candidate_custody, schema)


async def _assert_custodian(fixture):
    admin, schema, roles_by_kind = fixture.fixture.connection, fixture.fixture.control_schema, fixture.roles_by_kind
    async with _login(fixture, "custodian") as custodian:
        for protected_role in (roles_by_kind["queue"], fixture.fixture.roles["reader"]):
            for mode in ("MEMBER", "SET", "USAGE"):
                assert not await custodian.fetchval(
                    "SELECT pg_has_role(current_user,$1::name,$2)", protected_role, mode
                )
        assert not await custodian.fetchval(
            "SELECT pg_has_role(current_user,$1::name,'USAGE')", roles_by_kind["custody"]
        )
        assert await custodian.fetchval("SELECT pg_has_role(current_user,$1::name,'USAGE')", roles_by_kind["execution"])
        async with custodian.transaction():
            await custodian.execute(f"SET LOCAL ROLE {_q(roles_by_kind['custody'])}")
        async with custodian.transaction():
            await custodian.execute(f"SET LOCAL ROLE {_q(roles_by_kind['physical'])}")


async def _prepare_split_dependency(fixture):
    """Seal the fixture's exact existing read contract using the genuine independent custodian."""
    from process.network_custom_address_source import prepare_custom_address_source

    async with _login(fixture, "custodian") as custodian, custodian.transaction(isolation="repeatable_read"):
        return await prepare_custom_address_source(
            custodian,
            fixture.fixture.base,
            composition_id=str(uuid4()),
            approved_revision=fixture.seed.revision,
            owner_role=fixture.roles_by_kind["custody"],
            runtime_roles=fixture.readers,
            control_schema=fixture.fixture.control_schema,
        )


async def _prepare_split_successor(fixture, execution, consumer, dependency, head):
    """Consume the authenticated dependency under the complete worker actor census."""
    from process.registry_candidate_composition import RegistryCompositionAddressSources
    from tests.test_registry_publication_worker_permissions_postgres import _native_qualified_candidate

    request_id = uuid4()
    prepared = await consumer.prepare_address_sources(
        request_id=request_id,
        approved_revision=fixture.seed.revision,
        source_manifest=None,
        expected_head=head,
        address_sources=RegistryCompositionAddressSources(dependency.address_source),
    )
    return await _native_qualified_candidate(
        fixture,
        execution,
        expected_head=head,
        runtime_roles=fixture.readers,
        request_id=request_id,
        prepared_address_artifact=prepared.prepared_address_artifact,
    )


async def _publish_successor(fixture):
    """Publish and replay both independently prepared successors while retaining their native proofs."""
    admin, schema, roles_by_kind = fixture.fixture.connection, fixture.fixture.control_schema, fixture.roles_by_kind
    async with (
        AsyncExitStack() as stack,
        _login(fixture, "execution") as execution,
        _login(fixture, "lifecycle") as lifecycle,
        _login(fixture, "reader") as reader,
    ):
        pids = [
            await backend_connection.fetchval("SELECT pg_backend_pid()")
            for backend_connection in (execution, lifecycle, reader)
        ]
        assert len(set(pids)) == 3
        consumer = await _custody_runtime(fixture.config, stack, fixture.config.consumer, execution)
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
            async with execution.transaction(isolation="repeatable_read", readonly=True):
                assert await verify_network_candidate_custody(
                    execution, copy_target, control_schema=schema, required=True
                )
        assert await admin.fetchval(f"SELECT count(*) FROM {_q(schema)}.network_candidate_custody") == 2


async def _assert_login_denials(fixture, kind):
    schema, roles_by_kind = fixture.fixture.control_schema, fixture.roles_by_kind
    statements = [
        f"SET ROLE {_q(roles_by_kind['queue'])}",
        f"UPDATE {_q(schema)}.network_candidate_custody SET receipt_json=receipt_json",
        f"UPDATE {_q(schema)}.registry_source_snapshot SET snapshot_id=snapshot_id",
    ]
    if kind != "custodian":
        statements.append(f"SET ROLE {_q(roles_by_kind['custody'])}")
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
