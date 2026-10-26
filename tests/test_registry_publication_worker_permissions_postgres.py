# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct LOGIN publication roles_by_kind, exact grants and transactional refusal."""

import json
import os
from contextlib import AsyncExitStack, asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest

from process import registry_publication_worker as publication_worker
from process.registry_management_permissions import install_registry_management_permissions
from process.registry_publication_execution import approve_queued_registry_publication
from process.registry_publication_permissions import verify_registry_publication_lifecycle_permissions
from process.registry_publication_queue import (
    claim_registry_publication_request,
    enqueue_registry_publication,
    renew_registry_publication_lease,
)
from process.registry_publication_worker_permissions import (
    RegistryPublicationWorkerPermissionError,
    install_registry_publication_worker_permissions,
    verify_registry_publication_worker_permissions,
)
from tests import registry_candidate_custody_split_postgres_support as custody_split
from tests.registry_candidate_custody_split_postgres_support import split_db as split_db
from tests.test_network_custom_address_source_postgres import _draft, _seed
from tests.test_network_custom_address_source_postgres import custom_db as custom_db
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_approval_store_postgres import _command, _create
from tests.test_registry_publication_execution_postgres import _authority

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def worker_permissions(custom_db):
    fixture = custom_db
    seed = await _seed(fixture)
    async with _worker_roles(fixture, seed) as worker:
        yield worker


@asynccontextmanager
async def _worker_roles(fixture, seed):
    """Reuse the direct-login contract with either approved or initial fixture state."""
    roles_by_kind = {
        kind: "wp_" + kind + "_" + uuid4().hex for kind in ("queue", "physical", "api", "lifecycle", "execution")
    }
    role_password = uuid4().hex
    created_roles = []
    try:
        for kind, role in roles_by_kind.items():
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
        await verify_registry_publication_worker_permissions(
            lifecycle, role=fixture.roles_by_kind["lifecycle"], **_options(fixture)
        )
        await verify_registry_publication_lifecycle_permissions(
            lifecycle,
            role=fixture.roles_by_kind["lifecycle"],
            owner_role=fixture.roles_by_kind["queue"],
            physical_owner_role=fixture.roles_by_kind["physical"],
            control_schema=fixture.fixture.control_schema,
        )
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
        async with execution.transaction(readonly=True):
            await verify_registry_publication_worker_permissions(
                execution, role=fixture.roles_by_kind["execution"], **_options(fixture)
            )
            await verify_registry_publication_lifecycle_permissions(
                execution,
                role=fixture.roles_by_kind["execution"],
                owner_role=fixture.roles_by_kind["queue"],
                physical_owner_role=fixture.roles_by_kind["physical"],
                control_schema=fixture.fixture.control_schema,
            )


@pytest.mark.parametrize("kind", ["api", "lifecycle", "execution"])
async def test_worker_queue_audit_and_owner_bypasses_are_denied(worker_permissions, kind):
    fixture = worker_permissions
    await _install(fixture)
    async with _login(fixture, kind) as connection:
        statements = [
            f'SET ROLE "{fixture.roles_by_kind["queue"]}"',
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
        await connection.execute(f'ALTER ROLE "{fixture.roles_by_kind["lifecycle"]}" NOLOGIN')
        with pytest.raises(RegistryPublicationWorkerPermissionError, match="LOGIN"):
            await install_registry_publication_worker_permissions(connection, **_options(fixture))
        await connection.execute(f'ALTER ROLE "{fixture.roles_by_kind["lifecycle"]}" LOGIN')
    assert (
        await connection.fetch(
            "SELECT relname,relacl::text FROM pg_class WHERE relnamespace=(SELECT oid FROM pg_namespace WHERE nspname=$1) ORDER BY relname",
            fixture.fixture.control_schema,
        )
        == before
    )
    async with connection.transaction():
        await install_registry_publication_worker_permissions(connection, **_options(fixture))
        await verify_registry_publication_worker_permissions(connection, **_options(fixture))


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
        async with _login(fixture, "execution") as execution, execution.transaction(readonly=True):
            assert await execution.fetchval("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
            assert not await execution.fetchval(
                "SELECT has_database_privilege(current_user,current_database(),'TEMPORARY')"
            )
            with pytest.raises(RegistryPublicationWorkerPermissionError, match="temporary"):
                await verify_registry_publication_worker_permissions(
                    execution, role=fixture.roles_by_kind["execution"], **_options(fixture)
                )
        database_acl_sql = """SELECT a.grantee::bigint,a.privilege_type,a.is_grantable
        FROM pg_database d,aclexplode(COALESCE(d.datacl,acldefault('d',d.datdba))) a
        WHERE d.datname=current_database()"""
        before_acl_entries = {tuple(privilege_row) for privilege_row in await connection.fetch(database_acl_sql)}
        await _install(fixture)
        execution_oid = await connection.fetchval(
            "SELECT oid::bigint FROM pg_roles WHERE rolname=$1", fixture.roles_by_kind["execution"]
        )
        assert {
            tuple(privilege_row) for privilege_row in await connection.fetch(database_acl_sql)
        } == before_acl_entries | {(execution_oid, "TEMPORARY", False)}
        for role in (*fixture.fixture.roles.values(), *fixture.roles_by_kind.values()):
            assert await connection.fetchval(
                "SELECT has_database_privilege($1::name,current_database(),'TEMPORARY')", role
            ) == (role == fixture.roles_by_kind["execution"])
        async with _login(fixture, "execution") as execution, execution.transaction():
            await verify_registry_publication_worker_permissions(
                execution, role=fixture.roles_by_kind["execution"], **_options(fixture)
            )
            await execution.execute("CREATE TEMPORARY TABLE publication_selection_probe(id bigint) ON COMMIT DROP")
    finally:
        if public_temp:
            await connection.execute(f"GRANT TEMPORARY ON DATABASE {database} TO PUBLIC")


async def test_runtime_verifier_refuses_wrong_pool_identity(worker_permissions):
    fixture = worker_permissions
    await _install(fixture)
    async with _login(fixture, "api") as api, api.transaction(readonly=True):
        with pytest.raises(RegistryPublicationWorkerPermissionError, match="direct LOGIN"):
            await verify_registry_publication_worker_permissions(
                api, role=fixture.roles_by_kind["execution"], **_options(fixture)
            )


@pytest.mark.parametrize("kind", ["lifecycle", "execution"])
@pytest.mark.parametrize("boundary", ["install", "runtime"])
async def test_set_only_helper_cannot_write_protected_registry_columns(worker_permissions, kind, boundary):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    if boundary == "runtime":
        await _install(fixture)
    helper = "wp_helper_" + uuid4().hex
    schema = fixture.fixture.control_schema
    role = fixture.roles_by_kind[kind]
    is_created = False
    try:
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", helper)
        await connection.execute(f'CREATE ROLE "{helper}" NOLOGIN')
        is_created = True
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{helper}"')
        await connection.execute(f'GRANT UPDATE(reason) ON "{schema}".registry_approval_history TO "{helper}"')
        await connection.execute(f'GRANT "{helper}" TO "{role}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE')
        assert not await connection.fetchval("SELECT pg_has_role($1::name,$2::name,'USAGE')", role, helper)
        relation = await connection.fetchval("SELECT to_regclass($1)::oid", f'"{schema}".registry_approval_history')
        async with _login(fixture, kind) as worker:
            assert not await worker.fetchval(
                "SELECT has_column_privilege(current_user,$1::oid,'reason','UPDATE')", relation
            )
            async with worker.transaction():
                await worker.execute(f'SET LOCAL ROLE "{helper}"')
                await worker.execute(f"UPDATE \"{schema}\".registry_approval_history SET reason='changed' WHERE false")
        if boundary == "install":
            with pytest.raises(RegistryPublicationWorkerPermissionError, match="column privileges"):
                await _install(fixture)
        else:
            async with _login(fixture, kind) as worker, worker.transaction(readonly=True):
                with pytest.raises(RegistryPublicationWorkerPermissionError, match="column privileges"):
                    await verify_registry_publication_worker_permissions(worker, role=role, **_options(fixture))
    finally:
        if is_created:
            await connection.execute(f'DROP OWNED BY "{helper}"')
            await connection.execute(f'DROP ROLE "{helper}"')


@pytest.mark.parametrize("kind", ["lifecycle", "execution"])
@pytest.mark.parametrize("boundary", ["install", "runtime"])
@pytest.mark.parametrize(
    ("sequence_name", "privilege"),
    [
        ("network_serving_manifest_generation_id_seq", "SELECT"),
        ("network_serving_manifest_generation_id_seq", "UPDATE"),
        ("network_registry_identity_network_id_seq", "USAGE"),
        ("network_registry_identity_network_id_seq", "SELECT"),
        ("network_registry_identity_network_id_seq", "UPDATE"),
    ],
)
async def test_set_only_sequence_helper_is_refused(worker_permissions, kind, boundary, sequence_name, privilege):
    fixture = worker_permissions
    connection = fixture.fixture.connection
    if boundary == "runtime":
        await _install(fixture)
    helper = "wp_sequence_" + uuid4().hex
    schema = fixture.fixture.control_schema
    role = fixture.roles_by_kind[kind]
    sequence = f'"{schema}"."{sequence_name}"'
    sequence_oid = await connection.fetchval("SELECT to_regclass($1)::oid", sequence)
    is_created = False
    try:
        assert not await connection.fetchval("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", helper)
        await connection.execute(f'CREATE ROLE "{helper}" NOLOGIN')
        is_created = True
        await connection.execute(f'GRANT USAGE ON SCHEMA "{schema}" TO "{helper}"')
        await connection.execute(f'GRANT {privilege} ON SEQUENCE {sequence} TO "{helper}"')
        await connection.execute(f'GRANT "{helper}" TO "{role}" WITH ADMIN FALSE, INHERIT FALSE, SET TRUE')
        assert not await connection.fetchval("SELECT pg_has_role($1::name,$2::name,'USAGE')", role, helper)
        async with _login(fixture, kind) as worker:
            assert not await worker.fetchval(
                "SELECT has_sequence_privilege(current_user,$1::oid,$2)", sequence_oid, privilege
            )
            async with worker.transaction():
                await worker.execute(f'SET LOCAL ROLE "{helper}"')
                assert await worker.fetchval(
                    "SELECT has_sequence_privilege(current_user,$1::oid,$2)", sequence_oid, privilege
                )
                if privilege == "UPDATE":
                    await worker.fetchval("SELECT setval($1::regclass,1,false)", sequence)
        if boundary == "install":
            with pytest.raises(RegistryPublicationWorkerPermissionError, match="sequence privileges"):
                await _install(fixture)
        else:
            async with _login(fixture, kind) as worker, worker.transaction(readonly=True):
                with pytest.raises(RegistryPublicationWorkerPermissionError, match="sequence privileges"):
                    await verify_registry_publication_worker_permissions(worker, role=role, **_options(fixture))
    finally:
        if is_created:
            await connection.execute(f'DROP OWNED BY "{helper}"')
            await connection.execute(f'DROP ROLE "{helper}"')


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
    from process.registry_candidate_composition import _bindings
    from process.registry_membership_composition_stage import (
        build_registry_source_retention,
        materialize_registry_composition,
        stage_approved_membership_commands,
    )
    from process.registry_source_recipe_composition import stage_registry_source_recipes
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
        source_stage = await stage_registry_source_recipes(
            connection, (), approved, request_id=request_id, control_schema=schema
        )
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
    await custody_split._assert_handoff(fixture)
    await custody_split._assert_custodian(fixture)
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
    await custody_split._assert_handoff(fixture)
    await custody_split._assert_custodian(fixture)
    await custody_split._publish_successor(fixture)
    assert await custody_split._protected(fixture) == fixture.protected


@pytest.mark.parametrize("kind", ["queue", "custody", "custodian"])
async def test_split_ptg_actual_owner_or_custodian_actor_is_refused(split_db, kind):
    from process.network_candidate_custody import NetworkCandidateCustodyError
    from process.registry_publication_worker import _custody_runtime

    fixture = split_db
    await custody_split._assert_handoff(fixture)
    if kind == "custody":
        fixture.config = _split_ptg_worker_config(fixture, fixture.roles_by_kind[kind])
        with pytest.raises(NetworkCandidateCustodyError):
            await custody_split._publish_successor(fixture)
        assert (
            await fixture.fixture.connection.fetchval(
                f'SELECT generation_id FROM "{fixture.fixture.control_schema}".network_serving_control'
            )
            is None
        )
        assert (
            await fixture.fixture.connection.fetchval(
                f'SELECT count(*) FROM "{fixture.fixture.control_schema}".network_candidate_custody'
            )
            == 0
        )
        assert await custody_split._protected(fixture) == fixture.protected
        return
    if kind == "custodian":
        with pytest.raises(ValueError, match="config_invalid"):
            _split_ptg_worker_config(fixture, fixture.roles_by_kind[kind])
    else:
        config = _split_ptg_worker_config(fixture, fixture.roles_by_kind[kind])
        async with AsyncExitStack() as stack, custody_split._login(fixture, "execution") as execution:
            with pytest.raises(NetworkCandidateCustodyError):
                await _custody_runtime(config, stack, config.consumer, execution)
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
                with pytest.raises(ValueError, match="original LOGIN administrator"):
                    await custody_split._split(fixture, connection)
        finally:
            await engine.dispose()
    async with fixture.fixture.engine.begin() as connection:
        from sqlalchemy import text

        await connection.execute(text(f"SET LOCAL ROLE {custody_split._q(fixture.roles_by_kind['queue'])}"))
        with pytest.raises(ValueError, match="original LOGIN administrator"):
            await custody_split._split(fixture, connection)
    assert await custody_split._relations(fixture.fixture.connection, fixture.fixture.control_schema) == before
    assert await custody_split._protected(fixture) == fixture.protected


async def _fresh_split_qualification(fixture):
    async with custody_split._login(fixture, "lifecycle") as lifecycle:
        return await publication_worker._qualify_fresh_backends(
            lifecycle,
            schema=fixture.fixture.control_schema,
            observer_role=fixture.roles_by_kind["lifecycle"],
            execution_roles=tuple(fixture.roles_by_kind[kind] for kind in ("execution", "custodian")),
        )


async def _reconnect_native_pool(pool):
    async with pool.acquire() as connection:
        before = connection.get_server_pid()
        assert await connection.fetchval("SELECT pg_backend_pid()") == before
    await pool.expire_connections()
    async with pool.acquire() as connection:
        after = connection.get_server_pid()
        assert await connection.fetchval("SELECT pg_backend_pid()") == after
    assert before != after
    return before, after


async def _reconnect_native_ptg(runtime):
    async with runtime.sessions() as session:
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        before = raw.driver_connection.get_server_pid()
        await connection.invalidate()
    async with runtime.sessions() as session:
        connection = await session.connection()
        raw = await connection.get_raw_connection()
        after = raw.driver_connection.get_server_pid()
    assert before != after
    return before, after


def _observe_native_backends(monkeypatch, observer_roles):
    original_pool = publication_worker.asyncpg.create_pool
    original_qualify = publication_worker._qualify_fresh_backends
    pools_by_role, verified_by_role = {}, {}

    async def create_pool(**options):
        pool = await original_pool(**options)
        pools_by_role[options["user"]] = pool
        return pool

    async def qualify(connection, **options):
        assert set(pools_by_role) == set(observer_roles)
        qualification = await original_qualify(connection, **options)

        def verifier(role):
            async def verify(backend):
                await qualification.verifier(role)(backend)
                verified_by_role.setdefault(role, set()).add(backend.get_server_pid())

            return verify

        return SimpleNamespace(verifier=verifier)

    monkeypatch.setattr(publication_worker.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(publication_worker, "_qualify_fresh_backends", qualify)
    return SimpleNamespace(pools_by_role=pools_by_role, verified_by_role=verified_by_role)


async def _check_native_reconnections(observed, config, consumer, execution_connection=None):
    for role in (config.consumer.execution_role, config.custody_url.username):
        if role == config.consumer.execution_role and execution_connection is not None:
            assert execution_connection.get_server_pid() in observed.verified_by_role[role]
            continue
        pids = await _reconnect_native_pool(observed.pools_by_role[role])
        assert set(pids) <= observed.verified_by_role[role]
    pids = await _reconnect_native_ptg(consumer.composition_runtime)
    assert set(pids) <= observed.verified_by_role[config.consumer.execution_role]


async def test_fresh_worker_connections_and_reconnects(split_db, monkeypatch):
    """Exercise the actual worker, both driver pools and SQLAlchemy checkout."""
    config = _split_ptg_worker_config(split_db)
    observed = _observe_native_backends(monkeypatch, (config.consumer.lifecycle_role,))

    async def consume(**arguments):
        await _check_native_reconnections(observed, config, arguments["config"])
        return None

    monkeypatch.setattr(publication_worker, "consume_registry_publication_once", consume)
    assert await publication_worker.run_registry_publication_worker(config) == {"state": "no_work"}
    assert all(pool.is_closing() for pool in observed.pools_by_role.values())
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
    return module, getattr(module, "parse_registry_" + family + "_source_worker_config")(json.dumps(document))


@pytest.mark.parametrize("family", ["aca", "ptg", "cms"])
async def test_source_worker_native_backend_qualification(split_db, monkeypatch, family):
    """Only notification/business operations are doubled; native connection qualification is real."""
    module, config = _native_source_config(split_db, family)
    observers = () if family == "cms" else (config.reader_role,)
    observed = _observe_native_backends(monkeypatch, observers)
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
    assert all(pool.is_closing() for pool in observed.pools_by_role.values())
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


async def test_backend_predating_floor_is_refused(split_db):
    async with AsyncExitStack() as stack:
        existing_connections = [
            (kind, await stack.enter_async_context(custody_split._login(split_db, kind)))
            for kind in ("execution", "custodian")
        ]
        qualification = await _fresh_split_qualification(split_db)
        for kind, connection in existing_connections:
            started = await connection.fetchval("SELECT backend_start FROM pg_stat_activity WHERE pid=pg_backend_pid()")
            assert started < qualification.not_before
            with pytest.raises(ValueError, match="registry_publication_worker_backend_invalid"):
                await qualification.verifier(split_db.roles_by_kind[kind])(connection)


async def test_same_login_set_role_is_refused(split_db):
    qualification = await _fresh_split_qualification(split_db)
    for kind in ("execution", "custodian"):
        async with custody_split._login(split_db, kind) as connection:
            role = split_db.roles_by_kind[kind]
            verify = qualification.verifier(role)
            await verify(connection)
            await connection.execute('SET ROLE "' + role + '"')
            assert await connection.fetchval("SELECT session_user=current_user") is True
            with pytest.raises(ValueError, match="registry_publication_worker_backend_invalid"):
                await verify(connection)
            await connection.execute("RESET ROLE")
            await verify(connection)
