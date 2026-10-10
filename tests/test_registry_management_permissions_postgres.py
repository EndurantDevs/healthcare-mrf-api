# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native protected registry ownership and precise draft-role SQL privileges."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text

from process.company_network_link_store import (
    CompanyLinkBatchCommand,
    CompanyLinkBatchTarget,
    apply_company_network_link_batch,
    read_group_company_links,
)
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_approval_store import approve_registry_records
from process.registry_management_permissions import (
    _TABLES,
    RegistryManagementPermissionError,
    install_registry_management_permissions,
    verify_registry_management_permissions,
)
from process.registry_record_store import RegistryRecordCommand, apply_registry_record_command
from tests.test_manual_location_identity_store_postgres import _create as create_location
from tests.test_manual_provider_identity_store_postgres import (
    _actor,
    provider_db,
    serving_schema,
)
from tests.test_manual_provider_identity_store_postgres import (
    _create as create_provider,
)
from tests.test_registry_approval_store_postgres import _command as approval_command
from tests.test_registry_network_binding_approval_postgres import _binding
from tests.test_registry_record_store_postgres import _create as create_record

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def permissions_db(provider_db):
    connection, schema, sessions, _ = provider_db
    roles_by_kind = {kind: f"rp_{kind}_{uuid4().hex}" for kind in ("owner", "api", "publisher", "ancestor")}
    created_roles = []
    try:
        for role in roles_by_kind.values():
            assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role)
            created_roles.append(role)
            await connection.execute(f'CREATE ROLE "{role}" NOLOGIN')
        await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles_by_kind["owner"]}"')
        await connection.execute(f'GRANT "{roles_by_kind["owner"]}" TO "{roles_by_kind["publisher"]}"')
        await connection.execute(f'GRANT SELECT ON "{schema}".npi TO "{roles_by_kind["api"]}"')
        yield SimpleNamespace(connection=connection, schema=schema, sessions=sessions, roles_by_kind=roles_by_kind)
    finally:
        await connection.execute("RESET ROLE")
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        for role in reversed(created_roles):
            if await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role):
                await connection.execute(f'DROP OWNED BY "{role}"')
                await connection.execute(f'DROP ROLE "{role}"')
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]))", created_roles
        )


def _arguments(fixture):
    return {
        "api_role": fixture.roles_by_kind["api"],
        "owner_role": fixture.roles_by_kind["owner"],
        "control_schema": fixture.schema,
    }


async def _install(fixture):
    async with fixture.connection.transaction():
        return await install_registry_management_permissions(fixture.connection, **_arguments(fixture))


async def _draft(fixture, command, actor):
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        return await apply_registry_record_command(
            session, command, actor, schema=fixture.schema, source_schema=fixture.schema
        )


async def test_full_draft_services_and_protected_publisher_approval(permissions_db):
    fixture, actor = permissions_db, _actor()
    receipt = await _install(fixture)
    assert len(receipt["table_oids"]) == 32 and len(receipt["sequence_oids"]) == 2
    assert set(receipt["table_oids"]) == set(_TABLES)
    commands = [create_record(kind) for kind in ("group", "company", "network")]
    created_records = [await _draft(fixture, command, actor) for command in commands]
    assert type(created_records[2]["record_id"]) is int and 0 < created_records[2]["record_id"] <= 2147483647
    links = RegistryRecordCommand(
        "company_links",
        UUID(created_records[1]["record_id"]),
        "create",
        0,
        {"network_ids": [created_records[2]["record_id"]], "group_id": created_records[0]["record_id"]},
        "Explicit links",
        uuid4().hex,
    )
    provider = create_provider()
    provider = replace(provider, fields={**provider.fields, "npi": "1000000004"})
    for command in (provider, create_location(), links):
        created_records.append(await _draft(fixture, command, actor))
    command = commands[0]
    correction = replace(
        command,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "display_name": "Corrected Group"},
        idempotency_key=uuid4().hex,
    )
    assert (await _draft(fixture, correction, actor))["revision"] == 2
    for operation, revision in (("archive", 2), ("restore", 3)):
        await _draft(
            fixture,
            replace(command, operation=operation, expected_revision=revision, fields={}, idempotency_key=uuid4().hex),
            actor,
        )
    assert await _draft(fixture, commands[1], actor) == created_records[1]
    connection = fixture.connection
    denied_approval = await approval_command(connection, fixture.schema, created_records[1], expected_draft_revision=9)
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            await approve_registry_records(connection, denied_approval, actor, control_schema=fixture.schema)
    await connection.execute(f'SET ROLE "{fixture.roles_by_kind["publisher"]}"')
    try:
        async with connection.transaction():
            approval = await approve_registry_records(
                connection,
                await approval_command(connection, fixture.schema, created_records[1], expected_draft_revision=9),
                actor,
                control_schema=fixture.schema,
            )
        assert approval["approved_revision"] == 10
    finally:
        await connection.execute("RESET ROLE")
    assert await verify_registry_management_permissions(connection, **_arguments(fixture)) == receipt


async def test_api_cannot_modify_protected_objects_or_immutable_columns(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    connection, namespace = fixture.connection, f'"{fixture.schema}"'
    await connection.execute(f'SET ROLE "{fixture.roles_by_kind["api"]}"')
    try:
        assert await verify_registry_management_permissions(connection, **_arguments(fixture))
        for table in _TABLES:
            assert await connection.fetchval(f'SELECT count(*) FROM {namespace}."{table}"') >= 0
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(f'DELETE FROM {namespace}."{table}"')
        statements = [
            f"UPDATE {namespace}.registry_revision_control SET approved_revision=0",
            f"INSERT INTO {namespace}.registry_revision_control(id) VALUES(1)",
            f"UPDATE {namespace}.registry_record_history SET reason=reason",
            f"UPDATE {namespace}.network_registry_identity SET allocation_key=allocation_key",
            f"INSERT INTO {namespace}.network_registry_identity(network_id,allocation_key) OVERRIDING SYSTEM VALUE VALUES(17,gen_random_uuid())",
            f"UPDATE {namespace}.company_group_registry SET group_id=group_id",
            f"UPDATE {namespace}.company_group_registry SET created_at=created_at",
            f"CREATE TABLE {namespace}.extra(id integer)",
            f"ALTER TABLE {namespace}.registry_approved_record ADD COLUMN extra integer",
            f"SELECT setval('{namespace}.network_registry_identity_network_id_seq',1)",
            f"SELECT nextval('{namespace}.network_serving_manifest_generation_id_seq')",
        ]
        protected = set(_TABLES) - {
            "company_group_registry",
            "company_registry",
            "network_registry_record",
            "manual_provider_registry",
            "manual_location_registry",
            "company_registry_links",
            "network_membership_draft",
            "registry_site_binding",
            "registry_network_binding",
            "registry_record_history",
            "network_registry_identity",
            "registry_revision_control",
        }
        statements += [
            f'UPDATE {namespace}."{table}" SET "{_TABLES[table][1]}"="{_TABLES[table][1]}"' for table in protected
        ]
        statements += [
            f'UPDATE {namespace}.registry_network_binding SET "{column}"="{column}"'
            for column in ("binding_id", "binding_key", "source_scope_json", "edition_id", "created_at")
        ]
        for statement in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(statement)
        assert await connection.fetchval(f"SELECT nextval('{namespace}.network_registry_identity_network_id_seq')") > 0
    finally:
        await connection.execute("RESET ROLE")


async def test_api_role_can_save_and_replay_one_atomic_company_link_batch(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _install(fixture)
    companies = [await _draft(fixture, create_record("company"), actor) for _ in range(2)]
    group = await _draft(fixture, create_record("group"), actor)
    network = await _draft(fixture, create_record("network"), actor)
    command = CompanyLinkBatchCommand(
        tuple(
            CompanyLinkBatchTarget(UUID(company["record_id"]), 0, (network["record_id"],), UUID(group["record_id"]))
            for company in companies
        ),
        "Reviewed selected company links",
        uuid4().hex,
    )
    connection, namespace = fixture.connection, f'"{fixture.schema}"'
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        receipt = await apply_company_network_link_batch(connection, command, actor, control_schema=fixture.schema)
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        assert (
            await apply_company_network_link_batch(connection, command, actor, control_schema=fixture.schema) == receipt
        )
        assert await connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_company_link_batch") == 1
        assert (
            await connection.fetchval(
                f"SELECT count(*) FROM {namespace}.registry_record_history WHERE custom_revision=$1",
                receipt["custom_revision"],
            )
            == 2
        )
        assert await connection.fetchval(f"SELECT approved_revision FROM {namespace}.registry_revision_control") == 0
        page = await read_group_company_links(connection, UUID(group["record_id"]), control_schema=fixture.schema)
        assert len(page["records"]) == 2
    assert len(receipt["records"]) == 2
    assert all(batch_record["custom_revision"] == receipt["custom_revision"] for batch_record in receipt["records"])
    denied = (
        f"UPDATE {namespace}.registry_company_link_batch SET result_json=result_json",
        f"DELETE FROM {namespace}.registry_company_link_batch",
        f"TRUNCATE {namespace}.registry_company_link_batch",
        f"INSERT INTO {namespace}.registry_company_link_batch(created_at) VALUES(now())",
    )
    for statement in denied:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
                await connection.execute(statement)
    assert await verify_registry_management_permissions(connection, **_arguments(fixture))


async def test_direct_and_public_acl_leaks_close_without_legacy_changes(permissions_db):
    fixture, connection = permissions_db, permissions_db.connection
    namespace = f'"{fixture.schema}"'
    await connection.execute(
        f'GRANT ALL ON {namespace}.registry_approved_record TO PUBLIC,"{fixture.roles_by_kind["api"]}"'
    )
    await connection.execute(
        f'GRANT UPDATE(approved_revision) ON {namespace}.registry_revision_control TO "{fixture.roles_by_kind["api"]}" WITH GRANT OPTION'
    )
    source_before = await connection.fetchrow(
        "SELECT oid,relowner,relacl FROM pg_class WHERE oid=to_regclass($1)", namespace + ".npi"
    )
    await _install(fixture)
    assert (
        await connection.fetchrow(
            "SELECT oid,relowner,relacl FROM pg_class WHERE oid=to_regclass($1)", namespace + ".npi"
        )
        == source_before
    )
    assert await verify_registry_management_permissions(connection, **_arguments(fixture))


@pytest.mark.parametrize(
    "unsafe",
    [
        "owner_member",
        "set_only_owner",
        "write_ancestor",
        "column_ancestor",
        "superuser",
        "createrole",
        "write_all",
        "schema_create",
        "schema_owner",
    ],
)
async def test_unsafe_role_paths_reject_before_registry_mutation(permissions_db, unsafe):
    fixture, connection = permissions_db, permissions_db.connection
    role = f'"{fixture.roles_by_kind["api"]}"'
    commands_by_damage = {
        "owner_member": f'GRANT "{fixture.roles_by_kind["owner"]}" TO {role}',
        "set_only_owner": f'GRANT "{fixture.roles_by_kind["owner"]}" TO {role} WITH INHERIT FALSE, SET TRUE',
        "write_ancestor": f'GRANT UPDATE ON "{fixture.schema}".registry_approved_record TO "{fixture.roles_by_kind["ancestor"]}"',
        "column_ancestor": f'GRANT UPDATE(approved_revision) ON "{fixture.schema}".registry_revision_control TO "{fixture.roles_by_kind["ancestor"]}"',
        "superuser": f"ALTER ROLE {role} SUPERUSER",
        "createrole": f"ALTER ROLE {role} CREATEROLE",
        "write_all": f"GRANT pg_write_all_data TO {role}",
        "schema_create": f'GRANT CREATE ON SCHEMA "{fixture.schema}" TO {role}',
        "schema_owner": f'ALTER SCHEMA "{fixture.schema}" OWNER TO {role}',
    }
    await connection.execute(commands_by_damage[unsafe])
    if unsafe in {"write_ancestor", "column_ancestor"}:
        await connection.execute(f'GRANT "{fixture.roles_by_kind["ancestor"]}" TO {role}')
    before = await connection.fetch(
        "SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace=to_regnamespace($1) ORDER BY oid", fixture.schema
    )
    with pytest.raises(RegistryManagementPermissionError):
        await _install(fixture)
    assert (
        await connection.fetch(
            "SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace=to_regnamespace($1) ORDER BY oid",
            fixture.schema,
        )
        == before
    )


async def test_permission_install_rollback_and_replay(permissions_db):
    fixture, connection = permissions_db, permissions_db.connection
    before = await connection.fetch(
        "SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace=to_regnamespace($1) ORDER BY oid", fixture.schema
    )
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with connection.transaction():
            await install_registry_management_permissions(connection, **_arguments(fixture))
            raise RuntimeError("caller rollback")
    assert (
        await connection.fetch(
            "SELECT oid,relowner,relacl FROM pg_class WHERE relnamespace=to_regnamespace($1) ORDER BY oid",
            fixture.schema,
        )
        == before
    )
    receipt = await _install(fixture)
    queries = []
    connection.add_query_logger(queries.append)
    assert await _install(fixture) == receipt
    await asyncio.sleep(0)
    connection.remove_query_logger(queries.append)
    assert not any(query.query.lstrip().split()[0] in {"ALTER", "GRANT", "REVOKE", "CREATE"} for query in queries)
    with pytest.raises(RegistryManagementPermissionError, match="caller-owned"):
        await install_registry_management_permissions(connection, **_arguments(fixture))
    with pytest.raises(RegistryManagementPermissionError):
        async with connection.transaction():
            await install_registry_management_permissions(
                connection, **{**_arguments(fixture), "api_role": fixture.roles_by_kind["owner"]}
            )


@pytest.mark.parametrize(
    "damage",
    ["public", "column", "owner", "identity_sequence", "schema_usage", "schema_grant_option", "inherited_schema_grant"],
)
async def test_current_native_permissions_drift_fails_verification(permissions_db, damage):
    fixture, connection = permissions_db, permissions_db.connection
    await _install(fixture)
    namespace, role = f'"{fixture.schema}"', f'"{fixture.roles_by_kind["api"]}"'
    commands_by_damage = {
        "public": f"GRANT UPDATE ON {namespace}.registry_approved_record TO PUBLIC",
        "column": f"GRANT UPDATE(approved_revision) ON {namespace}.registry_revision_control TO {role}",
        "owner": f"ALTER TABLE {namespace}.registry_approved_record OWNER TO {role}",
        "identity_sequence": f"GRANT UPDATE ON SEQUENCE {namespace}.network_registry_identity_network_id_seq TO {role}",
        "schema_usage": f"REVOKE USAGE ON SCHEMA {namespace} FROM {role}",
        "schema_grant_option": f"GRANT USAGE ON SCHEMA {namespace} TO {role} WITH GRANT OPTION",
        "inherited_schema_grant": f'GRANT USAGE ON SCHEMA {namespace} TO "{fixture.roles_by_kind["ancestor"]}" WITH GRANT OPTION',
    }
    await connection.execute(commands_by_damage[damage])
    if damage == "inherited_schema_grant":
        await connection.execute(f'GRANT "{fixture.roles_by_kind["ancestor"]}" TO {role}')
    with pytest.raises(RegistryManagementPermissionError):
        await verify_registry_management_permissions(connection, **_arguments(fixture))


async def test_membership_head_has_only_precise_draft_columns(permissions_db):
    fixture, connection = permissions_db, permissions_db.connection
    await _install(fixture)
    qualified = f'"{fixture.schema}".network_membership_draft'
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        await connection.execute(
            f"INSERT INTO {qualified}(network_id,memberships_json,archived,revision) VALUES(1,'[]',false,1)"
        )
        await connection.execute(
            f"UPDATE {qualified} SET memberships_json='[]',archived=true,revision=2 WHERE network_id=1"
        )
        assert await connection.fetchval(f"SELECT revision FROM {qualified} WHERE network_id=1") == 2
        for assignment in ("network_id=2", "created_at=now()"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(f"UPDATE {qualified} SET {assignment} WHERE network_id=1")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with connection.transaction():
                await connection.execute(f"DELETE FROM {qualified} WHERE network_id=1")


async def test_api_role_can_write_binding_batches_without_mutating_scope_or_replay(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _install(fixture)
    network = await _draft(fixture, create_record("network"), actor)
    command = NetworkSourceBindingBatchCommand(
        json.dumps([_binding(network["record_id"])]).encode(),
        "Reviewed source decision",
        uuid4().hex,
    )
    connection, namespace = fixture.connection, f'"{fixture.schema}"'
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        receipt = await apply_network_source_binding_batch(connection, command, actor, control_schema=fixture.schema)
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        assert (
            await apply_network_source_binding_batch(connection, command, actor, control_schema=fixture.schema)
            == receipt
        )
        assert await connection.fetchval(f"SELECT approved_revision FROM {namespace}.registry_revision_control") == 0
        assert await connection.fetchval(f"SELECT count(*) FROM {namespace}.registry_network_binding_batch") == 1
        for statement in (
            f"UPDATE {namespace}.registry_network_binding SET source_scope_json=source_scope_json",
            f"UPDATE {namespace}.registry_network_binding SET binding_key=binding_key",
            f"UPDATE {namespace}.registry_network_binding SET created_at=created_at",
            f"UPDATE {namespace}.registry_network_binding_batch SET receipt_json=receipt_json",
            f"DELETE FROM {namespace}.registry_network_binding",
            f"TRUNCATE {namespace}.registry_network_binding_batch",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)
    assert receipt["records"][0]["network_id"] == network["record_id"]
