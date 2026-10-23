# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Direct LOGIN publication roles_by_kind, exact grants and transactional refusal."""

import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest

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


async def _native_qualified_candidate(fixture, connection):
    """Use real approved native streams and private artifact writers, without fixture receipts."""
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.network_custom_address_source import prepare_custom_address_source
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
    request_id = uuid4()
    copy_target = MembershipCopyTarget(
        *(str(uuid4()) for _ in range(3)), str(request_id), "network_candidate_" + request_id.hex
    )
    async with connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            connection, approved_revision=fixture.seed.revision, control_schema=schema
        )
        artifact = await prepare_custom_address_source(
            connection,
            fixture.fixture.base,
            composition_id=str(uuid4()),
            approved_revision=approved.approved_revision,
            owner_role=fixture.roles_by_kind["physical"],
            runtime_roles=(fixture.fixture.roles["reader"],),
            control_schema=schema,
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
            expected_head=0,
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
