# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct LOGIN publication roles_by_kind, exact grants and transactional refusal."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_publication_worker as publication_worker
from process.registry_management_permissions import install_registry_management_permissions
from process.registry_publication_execution import approve_queued_registry_publication
from process.registry_publication_queue import (
    claim_registry_publication_request,
    enqueue_registry_publication,
    renew_registry_publication_lease,
)
from process.registry_publication_worker_permissions import (
    RegistryPublicationWorkerPermissionError,
    install_registry_publication_worker_permissions,
)
from tests import registry_candidate_custody_split_postgres_support as custody_split
from tests.registry_candidate_custody_split_postgres_support import split_db as split_db
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _command, _create
from tests.test_registry_publication_execution_postgres import _authority

pytestmark = pytest.mark.asyncio


@dataclass(frozen=True)
class WorkerExecutionIdentity:
    role: str
    role_password: str = field(repr=False)
    oid: int


async def _remove_worker_execution_identity(connection, actor):
    actual = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", actor.role)
    if actual is None:
        return
    assert actual == actor.oid, "worker_execution_identity_changed"
    await connection.execute(f'DROP OWNED BY "{actor.role}"')
    await connection.execute(f'DROP ROLE "{actor.role}"')
    assert await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", actor.role) is None


@asynccontextmanager
async def _worker_execution_identity(connection):
    """Own one original LOGIN across Source closure and the later worker fixture."""
    role = "wp_execution_" + uuid4().hex
    password = uuid4().hex
    actor = None

    async def cleanup():
        if actor is not None:
            await _remove_worker_execution_identity(connection, actor)

    async with AsyncExitStack() as stack:
        stack.push_async_callback(cleanup)
        async with connection.transaction():
            assert await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role) is None
            await connection.execute(
                f'CREATE ROLE "{role}" LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE '
                f"NOREPLICATION NOBYPASSRLS PASSWORD '{password}'"
            )
            oid = await connection.fetchval("SELECT oid FROM pg_roles WHERE rolname=$1", role)
            assert type(oid) is int and oid > 0
            actor = WorkerExecutionIdentity(role, password, oid)
        yield actor


async def _adopt_worker_execution_identity(connection, actor):
    """Verify the original native principal and password before changing worker custody."""
    if actor is None:
        return
    assert type(actor) is WorkerExecutionIdentity and type(actor.oid) is int and actor.oid > 0
    assert (
        await connection.fetchval(
            "SELECT oid=$2::oid AND rolcanlogin AND rolinherit AND NOT rolsuper AND NOT rolcreatedb "
            "AND NOT rolcreaterole AND NOT rolreplication AND NOT rolbypassrls FROM pg_roles WHERE rolname=$1",
            actor.role,
            actor.oid,
        )
        is True
    ), "worker_execution_identity_changed"
    login = SimpleNamespace(roles_by_kind={"execution": actor.role}, role_password=actor.role_password)
    async with _login(login, "execution") as original:
        assert await original.fetchval("SELECT current_user::regrole::oid=$1::oid", actor.oid) is True


@pytest.fixture
async def worker_permissions(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    async with _worker_roles(fixture, seed) as worker:
        yield worker


@asynccontextmanager
async def _worker_roles(fixture, seed, *, execution_actor=None):
    """Reuse the direct-login contract with either approved or initial fixture state."""
    roles_by_kind = {
        kind: "wp_" + kind + "_" + uuid4().hex for kind in ("queue", "physical", "api", "lifecycle", "execution")
    }
    role_password = uuid4().hex
    if execution_actor is not None:
        await _adopt_worker_execution_identity(fixture.connection, execution_actor)
        roles_by_kind["execution"] = execution_actor.role
        role_password = execution_actor.role_password
    created_roles = []
    try:
        for kind, role in roles_by_kind.items():
            if kind == "execution" and execution_actor is not None:
                continue
            assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role)
            created_roles.append(role)
            await fixture.connection.execute(
                'CREATE ROLE "'
                + role
                + '"'
                + (f" LOGIN PASSWORD '{role_password}'" if kind in {"api", "lifecycle", "execution"} else " NOLOGIN")
            )
        await fixture.connection.execute(f'ALTER SCHEMA "{fixture.control_schema}" OWNER TO "{roles_by_kind["queue"]}"')
        async with fixture.connection.transaction():
            await install_registry_management_permissions(
                fixture.connection,
                api_role=roles_by_kind["api"],
                owner_role=roles_by_kind["queue"],
                control_schema=fixture.control_schema,
            )
        await fixture.connection.execute(
            f'GRANT "{fixture.roles["reader"]}" TO "{roles_by_kind["execution"]}" WITH ADMIN FALSE, INHERIT TRUE, SET FALSE'
        )
        yield SimpleNamespace(fixture=fixture, seed=seed, roles_by_kind=roles_by_kind, role_password=role_password)
    finally:
        await fixture.connection.execute("RESET ROLE")
        await fixture.connection.execute(f'DROP SCHEMA IF EXISTS "{fixture.control_schema}" CASCADE')
        schemas = await fixture.connection.fetch(
            "SELECT nspname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE r.rolname=$1",
            roles_by_kind["physical"],
        )
        for schema in schemas:
            await fixture.connection.execute('DROP SCHEMA "' + schema["nspname"] + '" CASCADE')
        for role in reversed(created_roles):
            await fixture.connection.execute('DROP OWNED BY "' + role + '"')
            await fixture.connection.execute('DROP ROLE "' + role + '"')


def _options(fixture):
    return {
        "lifecycle_role": fixture.roles_by_kind["lifecycle"],
        "execution_role": fixture.roles_by_kind["execution"],
        "api_role": fixture.roles_by_kind["api"],
        "queue_owner_role": fixture.roles_by_kind["queue"],
        "physical_owner_role": fixture.roles_by_kind["physical"],
        "control_schema": fixture.fixture.control_schema,
    }


async def _install(fixture):
    async with fixture.fixture.connection.transaction():
        return await install_registry_publication_worker_permissions(fixture.fixture.connection, **_options(fixture))


def _fixture_dsn():
    dsn = os.environ["NETWORK_REGISTRY_TEST_DSN"]
    prefix = "postgresql+asyncpg://"
    return "postgresql://" + dsn[len(prefix) :] if dsn.startswith(prefix) else dsn


@pytest.mark.parametrize("scheme", ["postgres", "postgresql", "postgresql+asyncpg"])
async def test_fixture_dsn_preserves_supported_connection_parts(monkeypatch, scheme):
    suffix = "synthetic:password@localhost:5432/example?application_name=synthetic"
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", scheme + "://" + suffix)
    assert _fixture_dsn() == ("postgres" if scheme == "postgres" else "postgresql") + "://" + suffix


@pytest.mark.parametrize("actual", [None, False], ids=["missing", "replaced-oid"])
async def test_precreated_execution_identity_refuses_changed_native_role(monkeypatch, actual):
    """Pure I/O-boundary regression: refusal precedes login, grants and worker ownership changes."""
    actor = WorkerExecutionIdentity("wp_execution_" + "a" * 32, "synthetic-password", 41)
    assert actor.role_password not in repr(actor)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=actual), execute=AsyncMock())
    login = AsyncMock(side_effect=AssertionError("changed role reached login"))
    monkeypatch.setitem(globals(), "_login", login)
    with pytest.raises(AssertionError, match="worker_execution_identity_changed"):
        await _adopt_worker_execution_identity(connection, actor)
    connection.fetchval.assert_awaited_once()
    assert connection.fetchval.await_args.args[1:] == (actor.role, actor.oid)
    connection.execute.assert_not_awaited()
    login.assert_not_called()


async def test_default_execution_identity_needs_no_query_or_login(monkeypatch):
    """The optional identity boundary leaves the default worker setup untouched."""
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=AssertionError("unexpected identity query")))
    login = AsyncMock(side_effect=AssertionError("unexpected identity login"))
    monkeypatch.setitem(globals(), "_login", login)
    assert await _adopt_worker_execution_identity(connection, None) is None
    connection.fetchval.assert_not_awaited()
    login.assert_not_called()


async def test_execution_identity_cleanup_never_drops_a_replacement():
    """A replaced role name cannot authorize DROP OWNED or DROP ROLE."""
    actor = WorkerExecutionIdentity("wp_execution_" + "a" * 32, "synthetic-password", 41)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=42), execute=AsyncMock())
    with pytest.raises(AssertionError, match="worker_execution_identity_changed"):
        await _remove_worker_execution_identity(connection, actor)
    connection.execute.assert_not_awaited()


@asynccontextmanager
async def _login(fixture, kind):
    dsn = _fixture_dsn()
    connection = await asyncpg.connect(
        dsn,
        user=fixture.roles_by_kind[kind],
        password=fixture.role_password,
    )
    try:
        assert await connection.fetchval(
            "SELECT current_user=session_user AND current_user::text=$1", fixture.roles_by_kind[kind]
        )
        yield connection
    finally:
        await connection.close()


async def _enqueue(fixture):
    record = await _draft(fixture.fixture, _create("network"), fixture.seed.actor)
    command = await _command(fixture.fixture.connection, fixture.fixture.control_schema, record)
    async with _login(fixture, "api") as api, api.transaction():
        return await enqueue_registry_publication(
            api,
            command,
            fixture.seed.actor,
            session_token_sha256="a" * 64,
            expected_serving_generation=0,
            control_schema=fixture.fixture.control_schema,
        )


async def test_direct_lifecycle_claim_renew_and_execution_approval(worker_permissions):
    fixture = worker_permissions
    receipt = await _install(fixture)
    assert await _install(fixture) == receipt
    async with _login(fixture, "api") as api:
        assert (
            await api.fetchval(
                "SELECT nextval($1::regclass)",
                f'"{fixture.fixture.control_schema}".network_registry_identity_network_id_seq',
            )
            > 0
        )
    await _enqueue(fixture)
    async with _login(fixture, "lifecycle") as lifecycle, lifecycle.transaction():
        lease = await claim_registry_publication_request(
            lifecycle, operation_family="publication", control_schema=fixture.fixture.control_schema
        )
        await renew_registry_publication_lease(
            lifecycle, lease["request_id"], lease["lease_id"], control_schema=fixture.fixture.control_schema
        )
    async with _login(fixture, "execution") as execution:
        approved = await approve_queued_registry_publication(
            execution,
            lease["request_id"],
            lease["lease_id"],
            authorize=_authority([]),
            control_schema=fixture.fixture.control_schema,
        )
        assert approved["approved_revision"] > fixture.seed.revision


@pytest.mark.parametrize("kind", ["api", "lifecycle", "execution"])
async def test_worker_queue_audit_and_owner_bypasses_are_denied(worker_permissions, kind):
    fixture = worker_permissions
    admin = fixture.fixture.connection
    queue_owner = fixture.roles_by_kind["queue"]
    physical_owner = fixture.roles_by_kind["physical"]
    for actor in ("api", "lifecycle", "execution"):
        await admin.execute(f'GRANT "{queue_owner}" TO "{fixture.roles_by_kind[actor]}" WITH INHERIT FALSE, SET TRUE')
    for actor in ("api", "lifecycle"):
        await admin.execute(
            f'GRANT "{physical_owner}" TO "{fixture.roles_by_kind[actor]}" WITH INHERIT FALSE, SET TRUE'
        )
    await admin.execute(f'GRANT CREATE ON SCHEMA "{fixture.fixture.control_schema}" TO PUBLIC')
    await _install(fixture)
    async with _login(fixture, kind) as connection:
        statements = [
            f'SET ROLE "{fixture.roles_by_kind["queue"]}"',
            f'CREATE TABLE "{fixture.fixture.control_schema}".application_write_probe(id integer)',
            f'UPDATE "{fixture.fixture.control_schema}".registry_publication_request SET actor_json=actor_json',
        ]
        if kind != "api":
            statements.append(
                f'INSERT INTO "{fixture.fixture.control_schema}".registry_publication_request(request_id) VALUES(NULL)'
            )
        if kind != "execution":
            statements.append(f'SET ROLE "{fixture.roles_by_kind["physical"]}"')
        if kind == "api":
            statements.append(f'UPDATE "{fixture.fixture.control_schema}".registry_publication_request SET state=state')
        for statement in statements:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute(statement)


async def test_installer_rollback_preserves_api_and_existing_role_state(worker_permissions):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    before = await connection.fetch(
        "SELECT relname,relacl::text FROM pg_class WHERE relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=$1) ORDER BY relname",
        fixture.fixture.control_schema,
    )
    async with connection.transaction():
        with pytest.raises(RegistryPublicationWorkerPermissionError, match="refused"):
            await install_registry_publication_worker_permissions(
                connection, **{**_options(fixture), "lifecycle_role": "missing_" + uuid4().hex}
            )
    assert (
        await connection.fetch(
            "SELECT relname,relacl::text FROM pg_class WHERE relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=$1) ORDER BY relname",
            fixture.fixture.control_schema,
        )
        == before
    )
    async with connection.transaction():
        await install_registry_publication_worker_permissions(connection, **_options(fixture))


async def test_installed_policy_rolls_back_with_outer_transaction(worker_permissions):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    with pytest.raises(RuntimeError, match="operator rollback"):
        async with connection.transaction():
            await install_registry_publication_worker_permissions(connection, **_options(fixture))
            raise RuntimeError("operator rollback")
    assert not await connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'MEMBER')",
        fixture.roles_by_kind["execution"],
        fixture.roles_by_kind["physical"],
    )
    assert not await connection.fetchval(
        "SELECT has_database_privilege($1::name,current_database(),'CREATE')", fixture.roles_by_kind["physical"]
    )
    async with _login(fixture, "lifecycle") as lifecycle:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await lifecycle.fetch(f'SELECT * FROM "{fixture.fixture.control_schema}".registry_publication_request')


async def test_execution_temporary_privilege_is_required_and_installed_only_for_execution(worker_permissions):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    await _install(fixture)
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    public_temp = await connection.fetchval(
        """SELECT EXISTS(SELECT 1 FROM pg_database d,
        aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a
        WHERE d.datname=current_database() AND a.grantee=0 AND a.privilege_type='TEMPORARY')"""
    )
    try:
        await connection.execute(f"REVOKE TEMPORARY ON DATABASE {database} FROM PUBLIC")
        await connection.execute(f'REVOKE TEMPORARY ON DATABASE {database} FROM "{fixture.roles_by_kind["execution"]}"')
        async with _login(fixture, "execution") as execution:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await execution.execute("CREATE TEMPORARY TABLE publication_selection_probe(id bigint) ON COMMIT DROP")
        await _install(fixture)
        async with _login(fixture, "execution") as execution, execution.transaction():
            await execution.execute("CREATE TEMPORARY TABLE publication_selection_probe(id bigint) ON COMMIT DROP")
    finally:
        if public_temp:
            await connection.execute(f"GRANT TEMPORARY ON DATABASE {database} TO PUBLIC")


async def _native_candidate_addresses(fixture, connection, approved, request_id, runtime_roles, prepared):
    """Verify the committed independent recipe or use the unsealed legacy writer path."""
    from process.network_custom_address_source import prepare_custom_address_source
    from process.registry_candidate_composition import RegistryCompositionAddressSources, _prepared_address_artifact

    if prepared is None:
        return await prepare_custom_address_source(
            connection,
            fixture.fixture.base,
            composition_id=str(uuid4()),
            approved_revision=approved.approved_revision,
            owner_role=fixture.roles_by_kind["physical"],
            runtime_roles=(fixture.fixture.roles["reader"],) if runtime_roles is None else runtime_roles,
            control_schema=fixture.fixture.control_schema,
        )
    return await _prepared_address_artifact(
        connection,
        prepared,
        RegistryCompositionAddressSources(prepared.base_source),
        request_id,
        approved,
        dict(owner_role=fixture.roles_by_kind["physical"], loader_roles=(), reader_roles=runtime_roles),
        fixture.fixture.control_schema,
        ((), None),
    )


async def _native_qualified_candidate(
    fixture, connection, *, expected_head=0, runtime_roles=None, request_id=None, prepared_address_artifact=None
):
    """Use real approved native streams and private artifact writers, without fixture receipts."""
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.network_membership_candidate_lifecycle import create_network_candidate, seal_network_candidate
    from process.network_membership_copy import MembershipCopyTarget
    from process.registry_candidate_composition import _bindings, _stage_source
    from process.registry_membership_composition_stage import (
        build_registry_source_retention,
        materialize_registry_composition,
        stage_approved_membership_commands,
    )
    from process.registry_source_retention import retain_registry_source_memberships

    schema = fixture.fixture.control_schema
    request_id = uuid4() if request_id is None else request_id
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(request_id), "network_candidate_" + request_id.hex
    )
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=fixture.seed.revision, control_schema=schema
        )
        artifact = await _native_candidate_addresses(
            fixture, connection, approved, request_id, runtime_roles, prepared_address_artifact
        )
        source_stage = await _stage_source(connection, None, request_id, 0, schema)
        commands = await stage_approved_membership_commands(connection, approved, request_id, control_schema=schema)
        retention = await build_registry_source_retention(connection, source_stage, commands)
        await create_network_candidate(
            connection,
            copy_target,
            source_generations={
                "custom_membership": approved.generation_id,
                "unified_address": artifact.address_source.generation_id,
            },
            approved_custom_revision=approved.approved_revision,
            expected_head=expected_head,
            expected_rows=retention["effective_rows"],
            source_retention=retention,
            control_schema=schema,
        )
        await retain_registry_source_memberships(connection, copy_target, source_stage, control_schema=schema)
        await _bindings(connection, copy_target, artifact, None, schema)
        await materialize_registry_composition(connection, copy_target, commands, control_schema=schema)
        await seal_network_candidate(
            connection, copy_target, source_stage=source_stage, command_stage=commands, control_schema=schema
        )
    return copy_target, artifact.address_source


async def test_execution_role_prepares_and_publishes_native_qualified_candidate(worker_permissions):
    from process.network_membership_pipeline import prepare_and_publish_network_candidate

    fixture = worker_permissions
    await _install(fixture)
    async with _login(fixture, "execution") as execution:
        target, address = await _native_qualified_candidate(fixture, execution)
        manifest = await prepare_and_publish_network_candidate(
            execution,
            target,
            address,
            owner_role=fixture.roles_by_kind["physical"],
            loader_roles=(),
            reader_roles=(fixture.fixture.roles["reader"],),
            control_schema=fixture.fixture.control_schema,
        )
        assert manifest["generation_id"] == 1
        assert (
            await execution.fetchval(
                f'SELECT generation_id FROM "{fixture.fixture.control_schema}".network_serving_control'
            )
            == 1
        )
        assert (
            await execution.fetchval(
                f'SELECT count(*) FROM "{fixture.fixture.control_schema}".network_membership_batch'
            )
            == 0
        )


async def test_split_ordinary_factory_publishes_successor_without_source_authority(split_db):
    """Use real parsed custody connections, publish twice, and retain source preimages."""
    fixture = split_db
    await custody_split._install_named_worker_permissions(fixture)
    await custody_split._publish_successor(fixture)
    await custody_split._assert_installer_replay(fixture)
    await custody_split._assert_native_denials(fixture)
    assert await custody_split._protected(fixture) == fixture.protected
    async with fixture.fixture.connection.transaction():
        observed = await custody_split.admission.read_registry_source_admission_receipt(
            fixture.fixture.connection,
            fixture.edition,
            header_sha256=custody_split.admission._source_header_sha256(custody_split._HEADERS),
            control_schema=fixture.fixture.control_schema,
        )
    assert observed == fixture.admission


def _split_ptg_worker_config(fixture, actual_role=None):
    from process.registry_publication_worker import parse_registry_publication_worker_config

    document = custody_split.configuration()
    for name in ("lifecycle", "execution", "custody"):
        document[name + "_dsn"] = getattr(fixture.config, name + "_url").render_as_string(hide_password=False)
    document["consumer"] = {
        **_options(fixture),
        "loader_roles": [],
        "reader_roles": [*fixture.readers, *(() if actual_role is None else (actual_role,))],
    }
    document["ptg"] = {
        "owner_role": fixture.roles_by_kind["queue"],
        "approval_role": fixture.roles_by_kind["api"],
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100},
    }
    document["preparation_roles"] = fixture.config.preparation_roles
    return parse_registry_publication_worker_config(json.dumps(document))


async def test_split_ptg_metadata_owner_publishes_and_retains_sealed_generations(split_db):
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

    fixture = split_db
    fixture.config = _split_ptg_worker_config(fixture)
    assert type(fixture.config.ptg.store) is RegistryPTGProducerScopeStore
    assert fixture.config.ptg.store.owner_role == fixture.roles_by_kind["queue"]
    assert fixture.config.ptg.store.owner_role != fixture.roles_by_kind["custody"]
    assert (
        await fixture.fixture.connection.fetchval(
            "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid=to_regclass($1)",
            f'"{fixture.fixture.control_schema}".registry_ptg_producer_scope',
        )
        == fixture.config.ptg.store.owner_role
    )
    await custody_split._install_named_worker_permissions(fixture)
    async with AsyncExitStack() as stack:
        consumer = publication_worker._ptg_runtime(fixture.config, stack)
        assert consumer.composition_runtime is not None
        await consumer.composition_runtime.qualify()
        fixture.config = replace(fixture.config, consumer=consumer)
        await custody_split._publish_successor(fixture)
    assert await custody_split._protected(fixture) == fixture.protected


async def test_split_refuses_population_and_preserves_outer_rollback(split_db):
    fixture = split_db
    admin, schema = (fixture.fixture.connection, fixture.fixture.control_schema)
    before = await custody_split._relations(admin, schema)
    with pytest.raises(RuntimeError, match="rollback"):
        async with fixture.fixture.engine.begin() as connection:
            await custody_split._split(fixture, connection)
            raise RuntimeError("operator rollback")
    assert await custody_split._relations(admin, schema) == before
    async with admin.transaction():
        await admin.execute(f"LOCK TABLE {custody_split._q(schema)}.network_membership_candidate IN ROW EXCLUSIVE MODE")
        async with fixture.fixture.engine.begin() as connection:
            with pytest.raises(custody_split.DBAPIError, match="lock"):
                await custody_split._split(fixture, connection)
    assert await custody_split._relations(admin, schema) == before
    await custody_split._candidate(admin, schema, uuid4())
    populated = await custody_split._relations(admin, schema)
    async with fixture.fixture.engine.begin() as connection:
        with pytest.raises(RuntimeError, match="empty publication"):
            await custody_split._split(fixture, connection)
    assert await custody_split._relations(admin, schema) == populated
    assert await custody_split._protected(fixture) == fixture.protected


async def test_split_ordinary_login_and_set_role_admin_are_refused(split_db):
    fixture = split_db
    before = await custody_split._relations(fixture.fixture.connection, fixture.fixture.control_schema)
    for kind in ("api", "execution", "custodian"):
        url = custody_split.make_url(fixture.dsn).set(
            drivername="postgresql+asyncpg", username=fixture.roles_by_kind[kind], password=fixture.role_password
        )
        engine = custody_split.create_async_engine(url)
        try:
            async with engine.begin() as connection:
                with pytest.raises((custody_split.DBAPIError, asyncpg.InsufficientPrivilegeError)) as denied:
                    await custody_split._split(fixture, connection)
                error = getattr(denied.value, "orig", denied.value)
                assert error.sqlstate == "42501"
        finally:
            await engine.dispose()
    async with fixture.fixture.engine.begin() as connection:
        from sqlalchemy import text

        await connection.execute(text(f"SET LOCAL ROLE {custody_split._q(fixture.roles_by_kind['queue'])}"))
        with pytest.raises((custody_split.DBAPIError, asyncpg.InsufficientPrivilegeError)) as denied:
            await custody_split._split(fixture, connection)
        error = getattr(denied.value, "orig", denied.value)
        assert error.sqlstate == "42501"
    assert await custody_split._relations(fixture.fixture.connection, fixture.fixture.control_schema) == before
    assert await custody_split._protected(fixture) == fixture.protected


async def _reconnect_native_pool(pool, limits):
    async with pool.acquire() as connection:
        await limits.verify(connection)
        assert await connection.fetchval("SELECT 1") == 1
    await pool.expire_connections()
    async with pool.acquire() as connection:
        await limits.verify(connection)
        assert await connection.fetchval("SELECT 1") == 1


async def _reconnect_native_sessions(sessions, limits):
    async with sessions() as session:
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        await limits.verify(raw.driver_connection)
        assert await raw.driver_connection.fetchval("SELECT 1") == 1
        await connection.invalidate()
    async with sessions() as session:
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        await limits.verify(raw.driver_connection)
        assert await raw.driver_connection.fetchval("SELECT 1") == 1


def _observe_native_resources(monkeypatch):
    original_pool = publication_worker.asyncpg.create_pool
    original_sessions = publication_worker._worker_sessions
    pools, sessions_by_role = [], {}

    async def create_pool(**options):
        pool = await original_pool(**options)
        pools.append((options, pool))
        return pool

    def worker_sessions(config, stack, url):
        sessions = original_sessions(config, stack, url)
        assert url.username not in sessions_by_role
        sessions_by_role[url.username] = sessions
        return sessions

    monkeypatch.setattr(publication_worker.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(publication_worker, "_worker_sessions", worker_sessions)
    return SimpleNamespace(pools=pools, sessions_by_role=sessions_by_role)


async def _check_native_reconnections(observed, config, consumer, execution_connection=None):
    if execution_connection is not None:
        await config.sql_limits.verify(execution_connection)
        assert await execution_connection.fetchval("SELECT 1") == 1
    for options, pool in observed.pools:
        if options["max_size"] == 1:
            assert options["init"] == options["setup"] == config.sql_limits.verify
            assert options["server_settings"] == config.sql_limits.server_settings()
            if pool.get_idle_size():
                await _reconnect_native_pool(pool, config.sql_limits)
    assert set(observed.sessions_by_role) == {config.consumer.execution_role}
    assert consumer.composition_runtime.sessions is observed.sessions_by_role[config.consumer.execution_role]
    for sessions in observed.sessions_by_role.values():
        await _reconnect_native_sessions(sessions, config.sql_limits)


async def test_resource_observer_preserves_session_factory(monkeypatch):
    """The passive observer forwards the actual session inputs and result unchanged."""
    config, stack, factory = object(), object(), object()
    url = SimpleNamespace(username="fixture_publisher")
    recorded_calls = []

    def sessions(actual_config, actual_stack, actual_url):
        recorded_calls.append((actual_config, actual_stack, actual_url))
        return factory

    monkeypatch.setattr(publication_worker, "_worker_sessions", sessions)
    observed = _observe_native_resources(monkeypatch)
    assert publication_worker._worker_sessions(config, stack, url) is factory
    assert recorded_calls == [(config, stack, url)]
    assert observed.sessions_by_role == {url.username: factory}
    assert observed.pools == []


async def test_fresh_worker_connections_and_reconnects(split_db, monkeypatch):
    """Exercise the actual worker, both driver pools and SQLAlchemy checkout."""
    config = _split_ptg_worker_config(split_db)
    observed = _observe_native_resources(monkeypatch)

    async def consume(**arguments):
        await _check_native_reconnections(observed, config, arguments["config"])
        return None

    monkeypatch.setattr(publication_worker, "consume_registry_publication_once", consume)
    assert await publication_worker.run_registry_publication_worker(config) == {"state": "no_work"}
    assert all(pool.is_closing() for _options, pool in observed.pools)
    assert await custody_split._protected(split_db) == split_db.protected


def _native_source_config(fixture, family):
    from importlib import import_module

    module = import_module("process.registry_" + family + "_source_worker")
    document = import_module("tests.test_registry_" + family + "_source_worker").configuration()
    roles = fixture.roles_by_kind
    for name, kind in (("execution", "execution"), ("notification", "reader"), ("custody", "custodian")):
        document[name + "_dsn"] = fixture.config.execution_url.set(
            username=roles[kind], password=fixture.role_password
        ).render_as_string(hide_password=False)
    document["consumer"].update(
        control_schema=fixture.fixture.control_schema,
        execution_role=roles["execution"],
        owner_role=roles["physical"],
        reader_roles=list(fixture.readers),
    )
    document["ptg"] = {
        "owner_role": roles["queue"],
        "approval_role": roles["api"],
        "read_limits": {"maximum_bytes": 1048576, "maximum_pages": 32, "maximum_coordinates": 100},
    }
    if family != "cms":
        document["reader_role"] = roles["reader"]
    if family != "ptg":
        document["notification_store"].update(
            control_schema=fixture.fixture.control_schema, publisher_role=roles["api"], reader_roles=[roles["reader"]]
        )
    if family == "cms":
        document["notification_store"]["owner_role"] = roles["queue"]
    config = getattr(module, "parse_registry_" + family + "_source_worker_config")(json.dumps(document))
    profile = fixture.config.preparation_roles
    return module, replace(
        config,
        preparation_roles={
            **profile,
            "application_roles": sorted(
                set(profile["application_roles"]) | set(publication_worker._application_role_names(config))
            ),
        },
    )


@pytest.mark.parametrize("family", ["aca", "ptg", "cms"])
async def test_source_worker_native_resource_limits_and_reconnects(split_db, monkeypatch, family):
    """Retain actual SQL budgets and cleanup while doubling notification consumption."""
    module, config = _native_source_config(split_db, family)
    observed = _observe_native_resources(monkeypatch)
    if family == "aca":
        monkeypatch.setattr(module.source_policy, "source_transaction_available", lambda: None)
        monkeypatch.setattr(module, "_source_config", AsyncMock(return_value=config))
        monkeypatch.setattr(module.RegistryACASourceNotificationStore, "qualify", AsyncMock(return_value="inbox"))
    elif family == "ptg":
        monkeypatch.setattr(module, "qualify_registry_ptg_source_notifications", AsyncMock(return_value="inbox"))

    async def consume(**arguments):
        await _check_native_reconnections(observed, config, arguments["config"], arguments["connection"])
        if family == "cms":
            async with arguments["notification_sessions"]() as session:
                connection = await session.connection()
                raw = await connection.get_raw_connection()
                assert await raw.driver_connection.fetchval("SHOW transaction_read_only") == "on"
                assert await raw.driver_connection.fetchval("SHOW transaction_isolation") == "repeatable read"
        return {"state": "completed"}

    monkeypatch.setattr(module, "consume_registry_" + family + "_source_notification_once", consume)
    outcome = await getattr(module, "run_registry_" + family + "_source_worker")(config)
    assert outcome == {"state": "completed"}
    assert all(pool.is_closing() for _options, pool in observed.pools)
    assert await custody_split._protected(split_db) == split_db.protected


@pytest.mark.parametrize("kind", ["reader", "execution"])
async def test_native_source_worker_refuses_unlimited_temp_default(split_db, monkeypatch, kind):
    """A genuine fresh LOGIN must retain the operator's finite temporary-file cap."""
    module, config = _native_source_config(split_db, "cms")
    role = split_db.roles_by_kind[kind]
    await split_db.fixture.connection.execute(f'ALTER ROLE "{role}" SET temp_file_limit TO "-1"')
    consume = AsyncMock(side_effect=AssertionError("unsafe backend reached consumer"))
    monkeypatch.setattr(module, "consume_registry_cms_source_notification_once", consume)
    with pytest.raises(RuntimeError, match="^registry_publication_worker_sql_limits_unavailable$"):
        await module.run_registry_cms_source_worker(config)
    consume.assert_not_awaited()
    assert await custody_split._protected(split_db) == split_db.protected
