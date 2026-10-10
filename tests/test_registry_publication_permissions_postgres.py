# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine LOGIN queue audit custody; native execution is an explicit test gate."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest

from process.registry_ptg_office_intent import (
    reserve_registry_ptg_office_prepare_intent,
)
from process.registry_publication_permissions import (
    INSERT_COLUMNS,
    LIFECYCLE_COLUMNS,
    verify_registry_publication_lifecycle_permissions,
)
from process.registry_publication_queue import (
    claim_registry_publication_request,
    finish_registry_publication_request,
    renew_registry_publication_lease,
)
from tests.test_manual_provider_identity_store_postgres import (
    provider_db as provider_db,
)
from tests.test_manual_provider_identity_store_postgres import (
    serving_schema as serving_schema,
)
from tests.test_registry_management_permissions_postgres import _install
from tests.test_registry_ptg_office_intent import actor, selector, source_command

pytestmark = pytest.mark.asyncio


async def _grant_queue_runtime(connection, schema, publisher_role):
    namespace, publisher = f'"{schema}"', f'"{publisher_role}"'
    await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {publisher}")
    await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {publisher}")
    await connection.execute(f"GRANT UPDATE ON {namespace}.registry_revision_control TO {publisher}")
    for table in ("registry_approval_history", "registry_approved_record"):
        await connection.execute(f'GRANT INSERT ON {namespace}."{table}" TO {publisher}')
    await connection.execute(
        f"GRANT UPDATE(state,lease_id,lease_expires_at,result_json,updated_at) "
        f"ON {namespace}.registry_publication_request TO {publisher}"
    )


async def _drop_queue_roles(connection, created_roles_by_name):
    for role, expected in reversed(list(created_roles_by_name.items())):
        observed = await connection.fetchrow("SELECT oid,rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=$1", role)
        if observed is not None:
            assert expected is not None and dict(observed) == expected
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename=$1",
                    role,
                )
                == 0
            )
            await connection.execute(f'DROP OWNED BY "{role}"')
            await connection.execute(f'DROP ROLE "{role}"')
    assert not await connection.fetchval(
        "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=ANY($1::text[]))",
        list(created_roles_by_name),
    )


@pytest.fixture
async def queue_permissions_db(provider_db):
    connection, schema, sessions, _ = provider_db
    roles_by_kind = {
        "owner": f"rp_owner_{uuid4().hex}",
        "ancestor": f"rp_ancestor_{uuid4().hex}",
        "api": f"scope_native_reader_{uuid4().hex}",
        "publisher": f"scope_native_approver_{uuid4().hex}",
    }
    passwords_by_kind = {kind: uuid4().hex for kind in ("api", "publisher")}
    created_roles_by_name = {}

    async def open_login(kind):
        url = sessions.kw["bind"].url
        return await asyncpg.connect(
            host=url.host,
            port=url.port,
            database=url.database,
            user=roles_by_kind[kind],
            password=passwords_by_kind[kind],
        )

    try:
        for kind, role in roles_by_kind.items():
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=$1)",
                role,
            )
            created_roles_by_name[role] = None
            options = f" LOGIN PASSWORD '{passwords_by_kind[kind]}'" if kind in passwords_by_kind else " NOLOGIN"
            await connection.execute(f'CREATE ROLE "{role}"{options}')
            created_roles_by_name[role] = dict(
                await connection.fetchrow(
                    "SELECT oid,rolcanlogin FROM pg_catalog.pg_roles WHERE rolname=$1",
                    role,
                )
            )
            assert created_roles_by_name[role]["rolcanlogin"] is (kind in passwords_by_kind)
        await connection.execute(f'ALTER SCHEMA "{schema}" OWNER TO "{roles_by_kind["owner"]}"')
        await connection.execute(f'GRANT SELECT ON "{schema}".npi TO "{roles_by_kind["api"]}"')
        await _grant_queue_runtime(connection, schema, roles_by_kind["publisher"])
        yield SimpleNamespace(
            connection=connection,
            schema=schema,
            sessions=sessions,
            roles_by_kind=roles_by_kind,
            open_login=open_login,
        )
    finally:
        try:
            await connection.execute("RESET ROLE")
            await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            assert await connection.fetchval("SELECT pg_catalog.to_regnamespace($1)", schema) is None
        finally:
            await _drop_queue_roles(connection, created_roles_by_name)


@asynccontextmanager
async def _login(fixture, kind):
    """Use an identity created as LOGIN and close it without changing role flags."""
    role = fixture.roles_by_kind[kind]
    connection = await fixture.open_login(kind)
    try:
        assert await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", role)
        yield connection
    finally:
        await connection.close()


async def _reserve_intent(fixture):
    """Reserve on the genuine management LOGIN and prove lifecycle denial."""
    async with _login(fixture, "api") as management, management.transaction():
        receipt = await reserve_registry_ptg_office_prepare_intent(
            management,
            source_command(),
            selector(),
            actor(),
            session_token_sha256="a" * 64,
            control_schema=fixture.schema,
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with management.transaction():
                await management.execute(f'UPDATE "{fixture.schema}".registry_publication_request SET state=state')
    return receipt


async def _deny_audit_writes(fixture, queue):
    """Every audit column and queue insertion deny on the lifecycle LOGIN."""
    for column in sorted(INSERT_COLUMNS | {"created_at"}):
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await queue.execute(f'UPDATE "{fixture.schema}".registry_publication_request SET "{column}"="{column}"')
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await queue.execute(
            f'INSERT INTO "{fixture.schema}".registry_publication_request(request_id) VALUES($1)',
            uuid4(),
        )


async def test_native_audit_custody(queue_permissions_db):
    fixture = queue_permissions_db
    await _install(fixture)
    receipt = await _reserve_intent(fixture)
    request_id = UUID(receipt["request_id"])
    before = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.schema}".registry_publication_request WHERE request_id=$1',
        request_id,
    )
    async with _login(fixture, "publisher") as queue:
        async with queue.transaction():
            verified = await verify_registry_publication_lifecycle_permissions(queue, control_schema=fixture.schema)
            assert verified["owner_role"] == fixture.roles_by_kind["owner"]
            claimed = await claim_registry_publication_request(
                queue, operation_family="office", control_schema=fixture.schema
            )
            assert claimed["request_id"] == request_id
            assert (
                await renew_registry_publication_lease(
                    queue,
                    request_id,
                    claimed["lease_id"],
                    control_schema=fixture.schema,
                )
                == request_id
            )
            await finish_registry_publication_request(
                queue,
                request_id,
                claimed["lease_id"],
                state="completed",
                result={"verified": True},
                control_schema=fixture.schema,
            )
        await _deny_audit_writes(fixture, queue)
    after = await fixture.connection.fetchrow(
        f'SELECT * FROM "{fixture.schema}".registry_publication_request WHERE request_id=$1',
        request_id,
    )
    assert after["state"] == "completed"
    assert {name: audit_value for name, audit_value in before.items() if name not in LIFECYCLE_COLUMNS} == {
        name: audit_value for name, audit_value in after.items() if name not in LIFECYCLE_COLUMNS
    }


async def _bypass(fixture, kind):
    """Install one actual native bypass on already registered synthetic roles."""
    queue = f'"{fixture.schema}".registry_publication_request'
    publisher = f'"{fixture.roles_by_kind["publisher"]}"'
    ancestor = f'"{fixture.roles_by_kind["ancestor"]}"'
    owner = f'"{fixture.roles_by_kind["owner"]}"'
    privilege_by_kind = {
        "table_update": "UPDATE",
        "table_insert": "INSERT",
        "audit_column": "UPDATE(command_json)",
        "delete": "DELETE",
        "truncate": "TRUNCATE",
        "references": "REFERENCES",
        "trigger": "TRIGGER",
        "maintain": "MAINTAIN",
        "grant_option": "UPDATE(state)",
    }
    if kind in privilege_by_kind:
        option = " WITH GRANT OPTION" if kind == "grant_option" else ""
        await fixture.connection.execute(f"GRANT {privilege_by_kind[kind]} ON {queue} TO {publisher}{option}")
        return
    if kind == "public_update":
        await fixture.connection.execute(f"GRANT UPDATE(command_json) ON {queue} TO PUBLIC")
        return
    if kind == "schema_create":
        await fixture.connection.execute(f'GRANT CREATE ON SCHEMA "{fixture.schema}" TO {publisher}')
        return
    if kind == "schema_grant_option":
        await fixture.connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.schema}" TO {publisher} WITH GRANT OPTION')
        return
    if kind == "builtin_write":
        await fixture.connection.execute(f"GRANT pg_write_all_data TO {publisher}")
        return
    if kind == "set_metadata":
        await fixture.connection.execute(f"GRANT {owner} TO {publisher} WITH INHERIT FALSE, SET TRUE")
        return
    if kind == "independent_owner":
        await fixture.connection.execute(f"GRANT SELECT ON {queue} TO {ancestor}")
        await fixture.connection.execute(f"GRANT {owner} TO {ancestor} WITH INHERIT FALSE, SET TRUE")
        return
    await fixture.connection.execute(f"GRANT SELECT,UPDATE(command_json) ON {queue} TO {ancestor}")
    if kind != "independent_grantee":
        inheritance = "TRUE" if kind == "inherited_update" else "FALSE"
        set_allowed = "FALSE" if kind == "inherited_update" else "TRUE"
        await fixture.connection.execute(
            f"GRANT {ancestor} TO {publisher} WITH INHERIT {inheritance}, SET {set_allowed}"
        )


@pytest.mark.parametrize(
    "kind",
    [
        "table_update",
        "table_insert",
        "audit_column",
        "delete",
        "truncate",
        "references",
        "trigger",
        "maintain",
        "grant_option",
        "public_update",
        "schema_create",
        "schema_grant_option",
        "builtin_write",
        "set_metadata",
        "independent_grantee",
        "independent_owner",
        "inherited_update",
        "set_only_update",
    ],
)
async def test_native_bypass_refused(queue_permissions_db, kind):
    fixture = queue_permissions_db
    await _install(fixture)
    await _bypass(fixture, kind)
    async with _login(fixture, "publisher") as queue, queue.transaction():
        with pytest.raises(ValueError, match="queue_permissions_unavailable"):
            await verify_registry_publication_lifecycle_permissions(queue, control_schema=fixture.schema)
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_publication_request') == 0
    )


@pytest.mark.parametrize("owner_kind", ["same", "set_reachable"])
async def test_distinct_native_owners(queue_permissions_db, owner_kind):
    fixture = queue_permissions_db
    await _install(fixture)
    physical_owner = fixture.roles_by_kind["owner"]
    if owner_kind == "set_reachable":
        physical_owner = fixture.roles_by_kind["ancestor"]
        await fixture.connection.execute(
            f'GRANT "{fixture.roles_by_kind["owner"]}" TO "{physical_owner}" WITH INHERIT FALSE, SET TRUE'
        )
    async with _login(fixture, "publisher") as queue, queue.transaction():
        with pytest.raises(ValueError, match="queue_permissions_unavailable"):
            await verify_registry_publication_lifecycle_permissions(
                queue, control_schema=fixture.schema, physical_owner_role=physical_owner
            )
