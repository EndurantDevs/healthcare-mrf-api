# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native protected registry ownership and precise draft-role SQL privileges."""

import asyncio
import json
from contextlib import AsyncExitStack
from dataclasses import replace
from types import SimpleNamespace
from uuid import UUID, uuid4

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_source_admission as admission
from process.company_network_link_store import (
    CompanyLinkBatchCommand,
    CompanyLinkBatchTarget,
    apply_company_network_link_batch,
    read_group_company_links,
)
from process.network_source_binding_store import NetworkSourceBindingBatchCommand, apply_network_source_binding_batch
from process.registry_approval_store import approve_registry_records
from process.registry_approved_company_read import read_registry_approved_companies
from process.registry_company_approval_fence import registry_company_approval_transaction
from process.registry_management_permissions import (
    _SEQUENCES,
    _TABLES,
    RegistryManagementPermissionError,
    assume_registry_source_owner,
    install_registry_management_permissions,
    provision_empty_registry_namespace,
    retire_registry_read_views,
    verify_registry_management_permissions,
)
from process.registry_provider_batch import apply_registry_provider_batch
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeError,
    RegistryPTGProducerScopeStore,
    _protected_store,
)
from process.registry_ptg_producer_scope import (
    _digest as ptg_receipt_digest,
)
from process.registry_ptg_published_plan_scope import _approved_association
from process.registry_ptg_published_plan_scope import _document as published_scope_receipt
from process.registry_record_store import (
    RegistryRecordCommand,
    RegistryRecordConflict,
    apply_registry_record_command,
    get_registry_record,
    list_registry_records,
    search_registry_companies,
)
from tests.test_manual_location_identity_store_postgres import _create as create_location
from tests.test_manual_provider_identity_store_postgres import (
    _actor,
    provider_db,
    serving_schema,
)
from tests.test_manual_provider_identity_store_postgres import (
    _create as create_provider,
)
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_registry_approval_store_postgres import _command as approval_command
from tests.test_registry_network_binding_approval_postgres import _binding
from tests.test_registry_ptg_producer_scope import _approved as producer_scope_receipt
from tests.test_registry_ptg_published_plan import identity as published_identity
from tests.test_registry_ptg_published_plan import intent as published_intent
from tests.test_registry_ptg_scope_engine import _actor as ptg_actor
from tests.test_registry_ptg_scope_engine import _command as ptg_command
from tests.test_registry_record_store_postgres import _actor as registry_actor
from tests.test_registry_record_store_postgres import _apply as apply_record
from tests.test_registry_record_store_postgres import _create as create_record
from tests.test_registry_source_admission_postgres import _HEADERS, _edition, _input

pytestmark = pytest.mark.asyncio

_PTG_TABLES = (
    "registry_ptg_producer_scope",
    "registry_ptg_published_plan_scope",
    "registry_ptg_office_approval",
    "registry_ptg_office_hold",
)


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
        namespace = f'"{schema}"'
        publisher = f'"{roles_by_kind["publisher"]}"'
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {publisher}")
        await connection.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO {publisher}")
        await connection.execute(f"GRANT UPDATE ON {namespace}.registry_revision_control TO {publisher}")
        for table in ("registry_approval_history", "registry_approved_record", *_PTG_TABLES):
            await connection.execute(f'GRANT INSERT ON {namespace}."{table}" TO {publisher}')
        await connection.execute(
            f"GRANT UPDATE(state,lease_id,lease_expires_at,result_json,updated_at) "
            f"ON {namespace}.registry_publication_request TO {publisher}"
        )
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
    assert len(receipt["table_oids"]) == 39 and len(receipt["sequence_oids"]) == 2
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


async def _assert_api_protected_objects(fixture, connection):
    """Retain exact native denial checks for both authenticated and assumed API roles."""
    namespace = f'"{fixture.schema}"'
    assert await verify_registry_management_permissions(connection, **_arguments(fixture))
    for table in _TABLES:
        assert await connection.fetchval(f'SELECT count(*) FROM {namespace}."{table}"') >= 0
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as error:
            await connection.execute(f'DELETE FROM {namespace}."{table}"')
        assert error.value.sqlstate == "42501"
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
        with pytest.raises(asyncpg.InsufficientPrivilegeError) as error:
            await connection.execute(statement)
        assert error.value.sqlstate == "42501"
    assert await connection.fetchval(f"SELECT nextval('{namespace}.network_registry_identity_network_id_seq')") > 0


async def test_api_cannot_modify_protected_objects_or_immutable_columns(permissions_db):
    fixture = permissions_db
    await _install(fixture)
    connection = fixture.connection
    await connection.execute(f'SET ROLE "{fixture.roles_by_kind["api"]}"')
    try:
        await _assert_api_protected_objects(fixture, connection)
    finally:
        await connection.execute("RESET ROLE")


async def test_api_login_preserves_drafts_and_protected_boundaries(permissions_db, record_property):
    """Authenticate the draft actor and retain native protected-object refusals."""
    fixture = permissions_db
    await _install(fixture)
    role, password = fixture.roles_by_kind["api"], uuid4().hex
    url = fixture.sessions.kw["bind"].url.set(username=role, password=password)
    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(fixture.connection.execute, f'ALTER ROLE "{role}" NOLOGIN PASSWORD NULL')
        await fixture.connection.execute(f"""ALTER ROLE "{role}" LOGIN PASSWORD '{password}'""")
        api_connection = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        cleanup.push_async_callback(api_connection.close)
        assert tuple(await api_connection.fetchrow("SELECT session_user,current_user,current_setting('role')")) == (
            role,
            role,
            "none",
        )
        assert (
            await fixture.connection.fetchval(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname=$1", fixture.roles_by_kind["owner"]
            )
            is False
        )
        await _assert_api_protected_objects(fixture, api_connection)
        api_engine = create_async_engine(url)
        cleanup.push_async_callback(api_engine.dispose)
        async with async_sessionmaker(api_engine)() as session, session.begin():
            assert tuple(
                (await session.execute(text("SELECT session_user,current_user,current_setting('role')"))).one()
            ) == (
                role,
                role,
                "none",
            )
            draft_record = await apply_registry_record_command(
                session, create_record("group"), _actor(), schema=fixture.schema, source_schema=fixture.schema
            )
        assert draft_record["revision"] == 1
        assert await api_connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".company_group_registry') == 1
        identity_fields = tuple(
            await api_connection.fetchrow("SELECT session_user,current_user,current_setting('role')")
        )
        assert identity_fields == (role, role, "none")
        record_property("api_login_identity", json.dumps(identity_fields))


async def test_api_membership_retains_binding_with_insert_only_privileges(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _install(fixture)
    network = await _draft(fixture, create_record("network"), actor)
    provider = await _draft(fixture, create_provider(), actor)
    location = await _draft(fixture, create_location(), actor)
    membership_by_field = {
        "network_id": network["record_id"],
        "provider_system": "manual",
        "provider_id": provider["record_id"],
        "location_id": location["record_id"],
        "evidence_id": "example-review",
    }
    command = RegistryRecordCommand(
        "membership",
        network["record_id"],
        "create",
        0,
        {"memberships_json": [membership_by_field]},
        "Explicit office choice",
        uuid4().hex,
    )
    retained = await _draft(fixture, command, actor)
    assert retained["record"]["memberships_json"] == [membership_by_field]
    binding = await fixture.connection.fetchrow(f'SELECT * FROM "{fixture.schema}".manual_provider_location_binding')
    assert (binding["provider_id"], str(binding["location_id"])) == (provider["record_id"], location["record_id"])
    assert await _draft(fixture, command, actor) == retained
    await fixture.connection.execute(f'SET ROLE "{fixture.roles_by_kind["api"]}"')
    try:
        for assignment in ("archived=true", "revision=revision+1", "location_key=location_key"):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await fixture.connection.execute(
                    f'UPDATE "{fixture.schema}".manual_provider_location_binding SET {assignment}'
                )
    finally:
        await fixture.connection.execute("RESET ROLE")
    assert await verify_registry_management_permissions(fixture.connection, **_arguments(fixture))


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


async def _provider_batch(fixture, commands, references, actor):
    return await apply_registry_provider_batch(
        fixture.connection, commands, references, actor, control_schema=fixture.schema, source_schema=fixture.schema
    )


async def _provider_batch_state(connection, namespace):
    return (
        await connection.fetch(f"SELECT * FROM {namespace}.manual_provider_registry ORDER BY provider_id"),
        await connection.fetch(f"SELECT * FROM {namespace}.registry_record_history ORDER BY record_key,revision"),
        await connection.fetchrow(f"SELECT * FROM {namespace}.registry_revision_control WHERE id=1"),
        await connection.fetch(f"SELECT * FROM {namespace}.registry_approved_record"),
    )


async def test_api_provider_batch_preserves_atomic_history(permissions_db):
    fixture, actor = permissions_db, _actor()
    await _install(fixture)
    known = await _draft(fixture, create_provider(), actor)
    connection, namespace = fixture.connection, f'"{fixture.schema}"'
    await connection.execute(f"INSERT INTO {namespace}.npi VALUES(1000000004)")
    assert await connection.fetchval(
        "SELECT has_table_privilege($1,$2,'SELECT') AND NOT has_table_privilege($1,$2,'UPDATE')",
        fixture.roles_by_kind["api"],
        f"{namespace}.npi",
    )
    commands = [create_provider(), create_provider()]
    references = [
        {"provider_system": "manual", "provider_id": known["record_id"]},
        {"provider_system": "npi", "provider_id": "1000000004"},
    ]
    correction = replace(
        commands[0],
        operation="correct",
        expected_revision=1,
        idempotency_key=uuid4().hex,
        fields={**commands[0].fields, "display_name": "Corrected Provider"},
    )
    async with connection.transaction():
        await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        assert await connection.fetchval("SELECT current_user") == fixture.roles_by_kind["api"]
        receipt = await _provider_batch(fixture, commands, references, actor)
        assert receipt["references"] == references
        assert [entry["record_id"] for entry in receipt["records"]] == [str(command.record_id) for command in commands]
        assert all(entry["revision"] == 1 and entry["custom_revision"] == 2 for entry in receipt["records"])
        before = await _provider_batch_state(connection, namespace)
        assert await _provider_batch(fixture, commands, references, actor) == receipt
        assert await _provider_batch_state(connection, namespace) == before
        corrected = await _provider_batch(fixture, [correction], references, actor)
        assert corrected["records"][0]["record"]["display_name"] == "Corrected Provider"
        assert corrected["records"][0]["revision"] == 2 and corrected["records"][0]["custom_revision"] == 3
        before = await _provider_batch_state(connection, namespace)
        assert await _provider_batch(fixture, commands, references, actor) == receipt
        assert await _provider_batch(fixture, [correction], references, actor) == corrected
        stale = replace(correction, idempotency_key=uuid4().hex)
        with pytest.raises(RegistryRecordConflict, match="registry_record_revision_conflict"):
            await _provider_batch(fixture, [create_provider(), stale], references, actor)
        for statement in (
            f"UPDATE {namespace}.registry_record_history SET reason=reason",
            f"UPDATE {namespace}.registry_revision_control SET approved_revision=approved_revision",
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)
        assert await _provider_batch_state(connection, namespace) == before
        assert await connection.fetchval(f"SELECT approved_revision FROM {namespace}.registry_revision_control") == 0
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


def _provision_arguments(fixture, namespace):
    return {
        "source_schema": fixture.schema,
        "target_schema": namespace,
        "api_role": fixture.roles_by_kind["api"],
        "owner_role": fixture.roles_by_kind["owner"],
    }


async def _provision_registry(fixture, namespace):
    async with fixture.connection.transaction():
        return await provision_empty_registry_namespace(fixture.connection, **_provision_arguments(fixture, namespace))


async def _prepare_old_registry_reads(fixture):
    connection, api_role = fixture.connection, fixture.roles_by_kind["api"]
    await connection.execute(f'GRANT USAGE ON SCHEMA "{fixture.schema}" TO "{api_role}"')
    await connection.execute(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{fixture.schema}" TO "{api_role}"')
    await connection.execute(
        f'GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO "{fixture.roles_by_kind["owner"]}"'
    )


async def _registry_api_reads(fixture, namespace):
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        companies = await list_registry_records(session, "company", schema=namespace)
        search = await search_registry_companies(
            session, "example", limit=50, offset=0, record_ids=None, schema=namespace
        )
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
        approved = await read_registry_approved_companies(fixture.connection, schema=namespace)
    return companies, search, approved


async def _assert_read_bridge_denies_writes(fixture):
    namespace = f'"{fixture.schema}"'
    statements = (
        f"UPDATE {namespace}.company_registry SET display_name=display_name",
        f"DELETE FROM {namespace}.company_registry",
        f"INSERT INTO {namespace}.company_registry(company_id,display_name) VALUES(gen_random_uuid(),'Example')",
    )
    for statement in statements:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
                await fixture.connection.execute(statement)


async def _retire_read_bridge(fixture, namespace):
    async with fixture.connection.transaction():
        retired = await retire_registry_read_views(fixture.connection, **_provision_arguments(fixture, namespace))
    assert retired["retired_views"] == len(_TABLES) == 39
    assert set(retired["view_oids"]) == set(_TABLES)
    assert (
        await fixture.connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relname=ANY($2::text[])",
            fixture.schema,
            sorted(set(_TABLES) | _SEQUENCES),
        )
        == 0
    )


async def _assert_provisioned_catalog(
    fixture, namespace, original_oid_by_name, original_sequence_positions, original_acl
):
    connection = fixture.connection
    receipt = await _provision_registry(fixture, namespace)
    assert receipt["schema_name"] == namespace
    assert await _provision_registry(fixture, namespace) == receipt
    moved_oid_by_name = dict(
        await connection.fetch(
            "SELECT c.relname,c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
            namespace,
            sorted(set(_TABLES) | _SEQUENCES),
        )
    )
    assert moved_oid_by_name == original_oid_by_name
    assert (
        tuple(
            await connection.fetchrow(
                f'SELECT last_value,is_called FROM "{namespace}".network_registry_identity_network_id_seq'
            )
        )
        == original_sequence_positions
    )
    assert await connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".npi') == 1
    assert (
        await connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", fixture.schema)
        == original_acl
    )


async def _assert_protected_source_replay(fixture, namespace):
    connection, edition = fixture.connection, _edition(_input())
    async with connection.transaction():
        await assume_registry_source_owner(connection, fixture.roles_by_kind["owner"], control_schema=namespace)
        first = await admission.admit_cms_mlr_edition(connection, _input(), edition, control_schema=namespace)
    protected = await connection.fetchval(
        f'SELECT manifest_sha256 FROM "{namespace}".registry_source_admission_receipt WHERE snapshot_id=$1',
        edition.snapshot_id,
    )
    async with connection.transaction():
        await assume_registry_source_owner(connection, fixture.roles_by_kind["owner"], control_schema=namespace)
        replay = await admission.admit_cms_mlr_edition(connection, _input(), edition, control_schema=namespace)
        completed = await admission.read_registry_source_admission_receipt(
            connection, edition, header_sha256=admission._source_header_sha256(_HEADERS), control_schema=namespace
        )
    assert not first["replayed"] and replay["replayed"] and completed == first
    assert replay["copy_sha256"] == first["copy_sha256"]
    assert replay["identity_materialization"]["companies_created"] == 0
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            await connection.execute(f"UPDATE \"{namespace}\".registry_source_observation SET status='rejected'")
    with pytest.raises(RegistryManagementPermissionError, match="cannot assume"):
        async with connection.transaction():
            await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            await assume_registry_source_owner(connection, fixture.roles_by_kind["owner"], control_schema=namespace)
    return edition, protected


async def _assert_provisioned_manual_cas(fixture, namespace):
    scoped = SimpleNamespace(**{**vars(fixture), "schema": namespace})
    command, actor = create_record("company"), registry_actor()
    created = await _draft(scoped, command, actor)
    correction = replace(
        command,
        operation="correct",
        expected_revision=1,
        fields={**command.fields, "display_name": "Updated example"},
        idempotency_key=uuid4().hex,
    )
    saved = await _draft(scoped, correction, actor)
    assert saved["revision"] == 2 and await _draft(scoped, correction, actor) == saved
    with pytest.raises(RegistryRecordConflict):
        await _draft(scoped, replace(correction, idempotency_key=uuid4().hex), actor)
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        current_company = await get_registry_record(session, "company", command.record_id, schema=namespace)
        assert current_company["revision"] == 2 and current_company["display_name_origin"] == "manual"
    assert created["record_id"] == str(command.record_id)


async def test_provision_preserves_oids_sequence_and_unrelated_source_then_admits_and_replays(permissions_db):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    connection = fixture.connection
    await _prepare_old_registry_reads(fixture)
    before_reads = await _registry_api_reads(fixture, fixture.schema)
    original_oid_by_name = dict(
        await connection.fetch(
            "SELECT c.relname,c.oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
            fixture.schema,
            sorted(set(_TABLES) | _SEQUENCES),
        )
    )
    await connection.fetchval(f"SELECT nextval('\"{fixture.schema}\".network_registry_identity_network_id_seq')")
    original_sequence_positions = tuple(
        await connection.fetchrow(
            f'SELECT last_value,is_called FROM "{fixture.schema}".network_registry_identity_network_id_seq'
        )
    )
    await connection.execute(f'INSERT INTO "{fixture.schema}".npi VALUES(1000000004)')
    original_acl = await connection.fetchval("SELECT nspacl::text FROM pg_namespace WHERE nspname=$1", fixture.schema)
    try:
        await _assert_provisioned_catalog(
            fixture, namespace, original_oid_by_name, original_sequence_positions, original_acl
        )
        assert await _registry_api_reads(fixture, fixture.schema) == before_reads
        assert await _registry_api_reads(fixture, namespace) == before_reads
        await _assert_provisioned_manual_cas(fixture, namespace)
        old_reads = await _registry_api_reads(fixture, fixture.schema)
        assert old_reads == await _registry_api_reads(fixture, namespace) and old_reads[0]
        await _assert_read_bridge_denies_writes(fixture)
        await _retire_read_bridge(fixture, namespace)
        edition, protected = await _assert_protected_source_replay(fixture, namespace)
        await _assert_provisioned_manual_cas(fixture, namespace)
        assert (
            await connection.fetchval(
                f'SELECT manifest_sha256 FROM "{namespace}".registry_source_admission_receipt WHERE snapshot_id=$1',
                edition.snapshot_id,
            )
            == protected
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


async def _ptg_grants(fixture, namespace):
    return await fixture.connection.fetch(
        "SELECT c.relname,r.rolname,a.privilege_type,a.is_grantable FROM pg_class c "
        "CROSS JOIN LATERAL aclexplode(c.relacl) a JOIN pg_roles r ON r.oid=a.grantee "
        "WHERE c.relnamespace=to_regnamespace($1) AND c.relname=ANY($2::text[]) "
        "AND r.rolname=ANY($3::text[]) ORDER BY c.relname,r.rolname,a.privilege_type",
        namespace,
        list(_PTG_TABLES),
        [fixture.roles_by_kind[kind] for kind in ("api", "publisher")],
    )


async def _assert_ptg_protected_role(fixture, namespace, kind):
    store = RegistryPTGProducerScopeStore(fixture.roles_by_kind["owner"], fixture.roles_by_kind["publisher"], namespace)
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind[kind]}"'))
        for table in _PTG_TABLES:
            assert await _protected_store(session, store, write=kind == "publisher", table_name=table) == (
                f'"{namespace}"."{table}"'
            )
            if kind == "api":
                with pytest.raises(RegistryPTGProducerScopeError, match="store_unprotected"):
                    await _protected_store(session, store, write=True, table_name=table)


async def _assert_ptg_protected_stores(fixture, namespace):
    for kind in ("api", "publisher"):
        await _assert_ptg_protected_role(fixture, namespace, kind)
    for table in _PTG_TABLES:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            async with fixture.connection.transaction():
                await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
                await fixture.connection.execute(f'INSERT INTO "{namespace}"."{table}" DEFAULT VALUES')
    for kind in ("api", "publisher"):
        role = fixture.roles_by_kind[kind]
        assert not await fixture.connection.fetchval("SELECT has_schema_privilege($1,$2,'CREATE')", role, namespace)
        assert not await fixture.connection.fetchval(
            "SELECT pg_has_role($1::name,$2::name,'SET')", role, fixture.roles_by_kind["owner"]
        )


async def _approved_ptg_association(fixture, namespace):
    scoped, actor = SimpleNamespace(**{**vars(fixture), "schema": namespace}), registry_actor()
    company = await _draft(scoped, create_record("company"), actor)
    network = await _draft(scoped, create_record("network"), actor)
    links = await _draft(
        scoped,
        RegistryRecordCommand(
            "company_links",
            UUID(company["record_id"]),
            "create",
            0,
            {"network_ids": [network["record_id"]], "group_id": None},
            "Reviewed association",
            uuid4().hex,
        ),
        actor,
    )
    command = await approval_command(fixture.connection, namespace, company, network, links)
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["publisher"]}"')
        approved = await approve_registry_records(fixture.connection, command, actor, control_schema=namespace)
    return {
        "approved_revision": approved["approved_revision"],
        "legal_company_id": company["record_id"],
        "network_id": network["record_id"],
    }


async def _assert_dormant_ptg_reader(fixture, namespace, role):
    assert not await fixture.connection.fetchval("SELECT has_schema_privilege($1,$2,'USAGE')", role, namespace)
    for table in _PTG_TABLES:
        assert await fixture.connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", role, f'"{namespace}"."{table}"'
        )
    async with fixture.sessions() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        store = RegistryPTGProducerScopeStore(
            fixture.roles_by_kind["owner"], fixture.roles_by_kind["publisher"], namespace
        )
        for table in _PTG_TABLES:
            with pytest.raises(RegistryPTGProducerScopeError, match="store_unprotected"):
                await _protected_store(session, store, write=False, table_name=table)


async def test_ptg_protected_lookup_preserves_approver_grants_after_cutover(permissions_db):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    await _prepare_old_registry_reads(fixture)
    dormant = fixture.roles_by_kind["ancestor"]
    for table in _PTG_TABLES:
        await fixture.connection.execute(f'GRANT SELECT ON "{fixture.schema}"."{table}" TO "{dormant}"')
    assert not await fixture.connection.fetchval("SELECT has_schema_privilege($1,$2,'USAGE')", dormant, fixture.schema)
    original_grants = await _ptg_grants(fixture, fixture.schema)
    engine = create_async_engine(fixture.sessions.kw["bind"].url, pool_size=1, max_overflow=0)
    try:
        receipt = await _provision_registry(fixture, namespace)
        assert set(_PTG_TABLES) <= set(receipt["table_oids"]) and len(receipt["table_oids"]) == 39
        assert await _ptg_grants(fixture, namespace) == original_grants
        await _assert_ptg_protected_stores(fixture, namespace)
        await _assert_dormant_ptg_reader(fixture, namespace, dormant)
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            for table in _PTG_TABLES:
                assert await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}"."{table}"') == 0
        await _retire_read_bridge(fixture, namespace)
        association = await _approved_ptg_association(fixture, namespace)
        async with registry_company_approval_transaction(
            async_sessionmaker(engine), control_schema=namespace
        ) as session:
            await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
            table = await _protected_store(
                session,
                RegistryPTGProducerScopeStore(
                    fixture.roles_by_kind["owner"], fixture.roles_by_kind["publisher"], namespace
                ),
                write=False,
                table_name="registry_ptg_published_plan_scope",
            )
            await _approved_association(session, table, association)
            with pytest.raises(RegistryPTGProducerScopeError, match="company_unapproved"):
                await _approved_association(session, table, {**association, "approved_revision": 0})
        assert await _ptg_grants(fixture, namespace) == original_grants
    finally:
        await engine.dispose()
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


def _ptg_scope_retention_document(table):
    if table == "registry_ptg_producer_scope":
        return producer_scope_receipt()
    full_by_field = {
        **ptg_command(),
        **published_intent(),
        "operation": "approve_ptg_published_plan_scope",
        "published_identity": published_identity(),
    }
    evidence_by_field = {
        "source_authority": {
            "contract": "ptg_published_result_source_authority.v1",
            "identity": full_by_field["published_identity"],
        },
        "selected_source_keys": [1],
        "selection_mode": "complete_snapshot_source_set",
    }
    return published_scope_receipt(full_by_field, ptg_actor(), evidence_by_field)


async def _seed_ptg_retention(fixture, table):
    """Use real migration constraints; this fixture does not admit a source or approve an office."""
    namespace, capture_id, key = f'"{fixture.schema}"', uuid4(), uuid4().hex
    if table in _PTG_TABLES[:2]:
        document_by_field = _ptg_scope_retention_document(table)
        await fixture.connection.execute(
            f'INSERT INTO {namespace}."{table}"(scope_id,scope_key,actor_key,idempotency_key,approval_sha256,approval_json) '
            "VALUES($1,$2,$3,$4,$5,$6::jsonb)",
            UUID(document_by_field["scope_id"]),
            "a" * 64,
            "b" * 64,
            document_by_field["idempotency_key"],
            ptg_receipt_digest(document_by_field),
            json.dumps(document_by_field),
        )
        return
    document_by_field = {
        "contract": "registry_ptg_office_approval.v1",
        "capture_id": str(capture_id),
        "actor": ptg_actor(),
        "command": {
            "capture_id": str(capture_id),
            "idempotency_key": key,
            "operation": "review_registry_ptg_office_capture",
        },
        "witness": {"contract": "registry_ptg_office_witness.v1"},
    }
    digest = ptg_receipt_digest(document_by_field)
    await fixture.connection.execute(
        f"INSERT INTO {namespace}.registry_ptg_office_approval(capture_id,actor_key,idempotency_key,approval_sha256,approval_json) "
        "VALUES($1,$2,$3,$4,$5::jsonb)",
        capture_id,
        "b" * 64,
        key,
        digest,
        json.dumps(document_by_field),
    )
    if table == "registry_ptg_office_hold":
        hold_by_field = {
            "contract": "registry_ptg_office_hold.v1",
            "capture_id": str(capture_id),
            "approval_sha256": digest,
        }
        await fixture.connection.execute(
            f"INSERT INTO {namespace}.registry_ptg_office_hold(capture_id,approval_sha256,hold_sha256,hold_json) "
            "VALUES($1,$2,$3,$4::jsonb)",
            capture_id,
            digest,
            ptg_receipt_digest(hold_by_field),
            json.dumps(hold_by_field),
        )


@pytest.mark.parametrize("table", _PTG_TABLES)
async def test_retained_ptg_receipts_refuse_empty_cutover_without_rewriting(permissions_db, table):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    await _prepare_old_registry_reads(fixture)
    await _seed_ptg_retention(fixture, table)
    originals_by_table = {
        name: await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}"."{name}" ORDER BY 1')
        for name in _PTG_TABLES
    }
    original_oids = await fixture.connection.fetch(
        "SELECT relname,oid FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relname=ANY($2::text[]) ORDER BY relname",
        fixture.schema,
        sorted(_TABLES),
    )
    with pytest.raises(RegistryManagementPermissionError, match="retained.data migration"):
        await _provision_registry(fixture, namespace)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    assert (
        await fixture.connection.fetch(
            "SELECT relname,oid FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relname=ANY($2::text[]) ORDER BY relname",
            fixture.schema,
            sorted(_TABLES),
        )
        == original_oids
    )
    assert len(original_oids) == 39
    for name, rows in originals_by_table.items():
        assert await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}"."{name}" ORDER BY 1') == rows


@pytest.mark.parametrize("retained", ["manual", "source", "serving"])
async def test_nonempty_registry_is_refused_without_moving_or_rewriting(permissions_db, retained):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    await _prepare_old_registry_reads(fixture)
    if retained == "manual":
        await apply_record(fixture.sessions, fixture.schema, create_record(), registry_actor())
    elif retained == "source":
        edition = _edition(_input())
        async with fixture.connection.transaction():
            await admission._register_edition(fixture.connection, '"' + fixture.schema + '"', edition)
    else:
        await _candidate(fixture.connection, fixture.schema, uuid4())
    before = await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_record_history')
    with pytest.raises(RegistryManagementPermissionError, match="retained.data migration"):
        await _provision_registry(fixture, namespace)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    assert (
        await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_record_history') == before
    )
    assert await fixture.connection.fetchval(
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=$1 AND c.relname=ANY($2::text[])",
        fixture.schema,
        sorted(_TABLES),
    ) == len(_TABLES)


async def test_api_cannot_bootstrap_old_owners_or_create_namespace(permissions_db):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    await _prepare_old_registry_reads(fixture)
    with pytest.raises(RegistryManagementPermissionError, match="cannot assume"):
        async with fixture.connection.transaction():
            await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
            await provision_empty_registry_namespace(fixture.connection, **_provision_arguments(fixture, namespace))
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None
    await fixture.connection.execute(f'ALTER SCHEMA "{fixture.schema}" OWNER TO "{fixture.roles_by_kind["ancestor"]}"')
    with pytest.raises(RegistryManagementPermissionError, match="compatibility owner"):
        await _provision_registry(fixture, namespace)
    assert await fixture.connection.fetchval("SELECT to_regnamespace($1)", namespace) is None


async def _assert_view_damage_refused(fixture, namespace, statement):
    with pytest.raises(RuntimeError, match="caller rollback"):
        async with fixture.connection.transaction():
            await fixture.connection.execute(statement)
            with pytest.raises(RegistryManagementPermissionError):
                await provision_empty_registry_namespace(fixture.connection, **_provision_arguments(fixture, namespace))
            with pytest.raises(RegistryManagementPermissionError):
                await retire_registry_read_views(fixture.connection, **_provision_arguments(fixture, namespace))
            raise RuntimeError("caller rollback")


async def test_read_bridge_replay_refuses_exact_catalog_tampering(permissions_db):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    connection = fixture.connection
    await _prepare_old_registry_reads(fixture)
    receipt = await _provision_registry(fixture, namespace)
    old_company = f'"{fixture.schema}".company_registry'
    damage_by_name = {
        "definition": f'CREATE OR REPLACE VIEW {old_company} WITH (security_invoker=true) AS SELECT * FROM "{namespace}".company_registry WHERE false',
        "invoker": f"ALTER VIEW {old_company} SET (security_invoker=false)",
        "owner": f'GRANT CREATE ON SCHEMA "{fixture.schema}" TO "{fixture.roles_by_kind["ancestor"]}"; ALTER VIEW {old_company} OWNER TO "{fixture.roles_by_kind["ancestor"]}"',
        "acl": f'GRANT UPDATE ON {old_company} TO "{fixture.roles_by_kind["api"]}"',
        "column": f'GRANT SELECT(company_id) ON {old_company} TO "{fixture.roles_by_kind["api"]}"',
        "usage": f'REVOKE USAGE ON SCHEMA "{fixture.schema}" FROM "{fixture.roles_by_kind["api"]}"',
        "missing": f"DROP VIEW {old_company} RESTRICT",
        "overlap": f'CREATE SEQUENCE "{fixture.schema}".network_registry_identity_network_id_seq',
        "target": f'ALTER TABLE "{namespace}".company_registry RENAME TO changed_company',
    }
    try:
        for statement in damage_by_name.values():
            await _assert_view_damage_refused(fixture, namespace, statement)
            assert await _provision_registry(fixture, namespace) == receipt
        await _retire_read_bridge(fixture, namespace)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


async def test_read_bridge_retirement_refuses_dependents_and_api(permissions_db):
    fixture, namespace = permissions_db, "registry_provision_test_" + uuid4().hex
    connection = fixture.connection
    await _prepare_old_registry_reads(fixture)
    await _provision_registry(fixture, namespace)
    dependency = f'"{fixture.schema}".registry_read_dependency'
    try:
        with pytest.raises(RegistryManagementPermissionError, match="cannot assume"):
            async with connection.transaction():
                await connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"')
                await retire_registry_read_views(connection, **_provision_arguments(fixture, namespace))
        await connection.execute(
            f'CREATE VIEW {dependency} AS SELECT company_id FROM "{fixture.schema}".company_registry'
        )
        with pytest.raises(RegistryManagementPermissionError, match="dependents"):
            async with connection.transaction():
                await retire_registry_read_views(connection, **_provision_arguments(fixture, namespace))
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM pg_class WHERE relnamespace=to_regnamespace($1) AND relkind='v' AND relname=ANY($2::text[])",
                fixture.schema,
                sorted(_TABLES),
            )
            == 39
        )
        await connection.execute(f"DROP VIEW {dependency} RESTRICT")
        await _retire_read_bridge(fixture, namespace)
        assert await _provision_registry(fixture, namespace)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


async def test_installed_management_api_role_reads_exact_group_source_evidence(permissions_db):
    from tests.test_registry_record_store_postgres import _retain_group_evidence

    fixture = permissions_db
    command = create_record()
    await apply_record(fixture.sessions, fixture.schema, command, registry_actor())
    reference = await _retain_group_evidence(fixture.connection, fixture.schema, command.record_id)
    await _install(fixture)
    assert await fixture.connection.fetchval(
        "SELECT NOT rolsuper AND NOT rolcreaterole AND NOT rolbypassrls FROM pg_roles WHERE rolname=$1",
        fixture.roles_by_kind["api"],
    )
    async with fixture.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{fixture.roles_by_kind["api"]}"'))
        assert await session.scalar(text("SELECT current_user")) == fixture.roles_by_kind["api"]
        record = await get_registry_record(session, "group", command.record_id, schema=fixture.schema)
        assert record["source_evidence"] == [reference]
        assert (await list_registry_records(session, "group", schema=fixture.schema)) == [record]
    assert await verify_registry_management_permissions(fixture.connection, **_arguments(fixture))


async def _aca_inbox_storage(fixture, monkeypatch):
    """Use the shared Alembic fixture before inserting a storage-only retention row."""
    import hashlib

    await _install(fixture)
    await _prepare_old_registry_reads(fixture)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.schema)
    capture_id = uuid4()
    encoded = '{"storage_retention_fixture":true}'
    # This row proves control storage retention, not typed producer acceptance.
    notification_values = (
        capture_id,
        "reference_family_archive_" + capture_id.hex,
        1,
        "a" * 64,
        hashlib.sha256(encoded.encode()).hexdigest(),
        encoded,
    )
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_aca_source_notification '
        "(capture_id,source_schema,source_schema_oid,recipe_sha256,notification_sha256,notification_json) "
        "VALUES($1,$2,$3,$4,$5,$6)",
        *notification_values,
    )
    publisher = fixture.roles_by_kind["publisher"]
    await fixture.connection.execute(
        f'GRANT SELECT ON "{fixture.schema}".registry_aca_source_notification TO "{publisher}"'
    )
    columns = "capture_id,source_schema,source_schema_oid,recipe_sha256,notification_sha256,notification_json"
    await fixture.connection.execute(
        f'GRANT INSERT ({columns}) ON "{fixture.schema}".registry_aca_source_notification TO "{publisher}"'
    )
    return notification_values


async def test_fresh_login_owned_migration_precedes_protected_operator_enrollment(permissions_db):
    from process.registry_aca_source_notification_permissions import (
        install_registry_aca_source_notification_permissions,
    )

    fixture, connection = permissions_db, permissions_db.connection
    name = f'"{fixture.schema}".registry_aca_source_notification'
    owner_sql = """SELECT h.relowner AS head_owner,t.relowner AS inbox_owner,r.rolcanlogin
      FROM pg_namespace n JOIN pg_class h ON h.relnamespace=n.oid AND h.relname='network_serving_control'
      JOIN pg_roles r ON r.oid=h.relowner JOIN pg_class t ON t.relnamespace=n.oid
        AND t.relname='registry_aca_source_notification' WHERE n.nspname=$1"""
    before = await connection.fetchrow(owner_sql, fixture.schema)
    assert before["head_owner"] == before["inbox_owner"] and before["rolcanlogin"] is True
    arguments_by_name = dict(
        control_schema=fixture.schema,
        publisher_role=fixture.roles_by_kind["publisher"],
        reader_roles=(fixture.roles_by_kind["api"],),
        runtime_roles=tuple(sorted(fixture.roles_by_kind.values())),
    )
    async with connection.transaction():
        with pytest.raises(ValueError, match="grants_invalid"):
            await install_registry_aca_source_notification_permissions(connection, **arguments_by_name)
    assert not await connection.fetchval(
        "SELECT has_any_column_privilege($1,$2,'INSERT')", arguments_by_name["publisher_role"], name
    )
    await _install(fixture)
    async with connection.transaction():
        receipt = await install_registry_aca_source_notification_permissions(connection, **arguments_by_name)
    after = await connection.fetchrow(owner_sql, fixture.schema)
    assert after["head_owner"] == after["inbox_owner"] and after["rolcanlogin"] is False
    assert receipt["table_oid"] == await connection.fetchval("SELECT to_regclass($1)::oid", name)
    assert await connection.fetchval("SELECT count(*) FROM " + name) == 0


@pytest.mark.parametrize(
    "membership_options",
    (
        pytest.param("WITH ADMIN FALSE, INHERIT TRUE, SET FALSE", id="inherited-insert"),
        pytest.param("WITH ADMIN FALSE, INHERIT FALSE, SET TRUE", id="set-only"),
        pytest.param("WITH ADMIN FALSE, INHERIT FALSE, SET FALSE", id="member-only"),
    ),
)
async def test_aca_original_reader_rejects_publisher_role_drift_after_enrollment(permissions_db, membership_options):
    from process.registry_aca_source_notification_permissions import (
        install_registry_aca_source_notification_permissions,
    )
    from process.registry_aca_source_notifications import RegistryACASourceNotificationStore

    fixture, connection = permissions_db, permissions_db.connection
    await _install(fixture)
    publisher, reader_role = fixture.roles_by_kind["publisher"], fixture.roles_by_kind["api"]
    password, reader = uuid4().hex, None
    store = RegistryACASourceNotificationStore(fixture.schema, publisher, (reader_role,))
    try:
        await connection.execute(f"ALTER ROLE \"{reader_role}\" LOGIN PASSWORD '{password}'")
        async with connection.transaction():
            await install_registry_aca_source_notification_permissions(
                connection,
                control_schema=fixture.schema,
                publisher_role=publisher,
                reader_roles=(reader_role,),
                runtime_roles=tuple(sorted(fixture.roles_by_kind.values())),
            )
        url = fixture.sessions.kw["bind"].url.set(drivername="postgresql", username=reader_role, password=password)
        reader = await asyncpg.connect(url.render_as_string(hide_password=False))
        assert tuple(await reader.fetchrow("SELECT current_user,session_user,current_setting('role')")) == (
            reader_role,
            reader_role,
            "none",
        )
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            identity = await store.qualify(reader, writing=False)
        await connection.execute(f'GRANT "{publisher}" TO "{reader_role}" {membership_options}')
        assert await reader.fetchval("SELECT pg_has_role(current_user,$1,'MEMBER')", publisher)
        relation = f'"{fixture.schema}".registry_aca_source_notification'
        assert not await reader.fetchval("SELECT has_table_privilege(current_user,$1,'INSERT')", relation)
        assert await reader.fetchval("SELECT has_any_column_privilege(current_user,$1,'INSERT')", relation) is (
            "INHERIT TRUE" in membership_options
        )
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            with pytest.raises(ValueError, match="registry_aca_source_notification_invalid"):
                await store.qualify(reader, writing=False)
        await connection.execute(f'REVOKE "{publisher}" FROM "{reader_role}"')
        async with reader.transaction(isolation="repeatable_read", readonly=True):
            assert await store.qualify(reader, writing=False) == identity
        assert await connection.fetchval(f"SELECT count(*) FROM {relation}") == 0
    finally:
        if reader is not None:
            await reader.close()
        await connection.execute(f'REVOKE "{publisher}" FROM "{reader_role}"')
        await connection.execute(f'ALTER ROLE "{reader_role}" NOLOGIN PASSWORD NULL')


@pytest.mark.parametrize(
    ("global_role", "membership_options"),
    (
        pytest.param("pg_write_all_data", "", id="pg_write_all_data"),
        pytest.param("pg_read_all_data", "", id="pg_read_all_data"),
        pytest.param("pg_write_all_data", " WITH INHERIT FALSE, SET TRUE", id="pg_write_all_data-set-only"),
        pytest.param("pg_read_all_data", " WITH INHERIT FALSE, SET TRUE", id="pg_read_all_data-set-only"),
    ),
)
async def test_aca_enrollment_rejects_effective_global_authority_outside_reader_subset(
    permissions_db, global_role, membership_options
):
    from process.registry_aca_source_notification_permissions import (
        install_registry_aca_source_notification_permissions,
    )

    fixture, connection = permissions_db, permissions_db.connection
    await _install(fixture)
    extra = fixture.roles_by_kind["api"]
    await connection.execute(f'GRANT "{global_role}" TO "{extra}"' + membership_options)
    try:
        if membership_options:
            assert await connection.fetchval("SELECT pg_has_role($1,$2,'SET')", extra, global_role)
            assert not await connection.fetchval("SELECT pg_has_role($1,$2,'USAGE')", extra, global_role)
            privilege = "INSERT" if global_role == "pg_write_all_data" else "SELECT"
            assert not await connection.fetchval(
                "SELECT has_any_column_privilege($1,$2,$3)",
                extra,
                f'"{fixture.schema}".registry_aca_source_notification',
                privilege,
            )
        async with connection.transaction():
            with pytest.raises(ValueError, match="grants_invalid"):
                await install_registry_aca_source_notification_permissions(
                    connection,
                    control_schema=fixture.schema,
                    publisher_role=fixture.roles_by_kind["publisher"],
                    reader_roles=(fixture.roles_by_kind["ancestor"],),
                    runtime_roles=tuple(sorted(fixture.roles_by_kind.values())),
                )
        assert not await connection.fetchval(
            "SELECT has_any_column_privilege($1,$2,'INSERT')",
            fixture.roles_by_kind["publisher"],
            f'"{fixture.schema}".registry_aca_source_notification',
        )
    finally:
        await connection.execute(f'REVOKE "{global_role}" FROM "{extra}"')


async def test_aca_control_namespace_move_preserves_native_oid_acl_indexes_and_rows(permissions_db, monkeypatch):
    fixture, connection = permissions_db, permissions_db.connection
    notification_values = await _aca_inbox_storage(fixture, monkeypatch)
    name = "registry_aca_source_notification"
    original = await connection.fetchrow(
        "SELECT c.oid,c.relowner,c.relacl::text,ARRAY(SELECT indexrelid FROM pg_index "
        "WHERE indrelid=c.oid ORDER BY indexrelid) AS indexes FROM pg_class c "
        "WHERE c.oid=to_regclass($1)",
        f'"{fixture.schema}".{name}',
    )
    namespace = "aca_registry_move_" + uuid4().hex
    try:
        await _provision_registry(fixture, namespace)
        moved = await connection.fetchrow(
            "SELECT c.oid,c.relowner,c.relacl::text,ARRAY(SELECT indexrelid FROM pg_index "
            "WHERE indrelid=c.oid ORDER BY indexrelid) AS indexes FROM pg_class c "
            "WHERE c.oid=to_regclass($1)",
            f'"{namespace}".{name}',
        )
        assert moved == original
        assert await connection.fetchval("SELECT to_regclass($1)", f'"{fixture.schema}".{name}') is None
        retained = await connection.fetchrow(
            f"SELECT capture_id,source_schema,source_schema_oid,recipe_sha256,notification_sha256,notification_json "
            f'FROM "{namespace}".{name}'
        )
        assert tuple(retained.values()) == notification_values
        assert await connection.fetchval(
            "SELECT has_table_privilege($1,$2,'SELECT')", fixture.roles_by_kind["api"], f'"{namespace}".{name}'
        )
        assert not await connection.fetchval(
            "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')",
            fixture.roles_by_kind["api"],
            f'"{namespace}".{name}',
        )
        assert await connection.fetchval(
            "SELECT has_schema_privilege($1,$2,'USAGE')", fixture.roles_by_kind["publisher"], namespace
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')


@pytest.mark.parametrize("damage", ("owner", "target"))
async def test_aca_control_namespace_move_refuses_owner_drift_or_conflicting_target(
    permissions_db, monkeypatch, damage
):
    fixture, connection = permissions_db, permissions_db.connection
    notification_values = await _aca_inbox_storage(fixture, monkeypatch)
    namespace = "aca_registry_refusal_" + uuid4().hex
    if damage == "owner":
        await connection.execute(
            f'ALTER TABLE "{fixture.schema}".registry_aca_source_notification '
            f'OWNER TO "{fixture.roles_by_kind["ancestor"]}"'
        )
    else:
        await connection.execute(f'CREATE SCHEMA "{namespace}" AUTHORIZATION "{fixture.roles_by_kind["owner"]}"')
    try:
        with pytest.raises(RegistryManagementPermissionError, match="ACA inbox"):
            await _provision_registry(fixture, namespace)
        assert (
            await connection.fetchval(
                f'SELECT notification_json FROM "{fixture.schema}".registry_aca_source_notification WHERE capture_id=$1',
                notification_values[0],
            )
            == notification_values[-1]
        )
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{namespace}" CASCADE')
