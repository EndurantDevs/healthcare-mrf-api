# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real owner preparation and ordinary-role adoption on isolated native storage."""

import json
from uuid import uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_network_candidate_custody import validate_prepared_candidate_custody
from db.migration_registry_preparation import registry_migration, validate_prepared_registry_table
from process.registry_approval_preview import preview_registry_approval
from process.registry_approval_store import RegistryApprovalConflict, approve_registry_records
from process.registry_management_permissions import (
    _INSERT_COLUMNS,
    RegistryManagementPermissionError,
    install_registry_office_recipe_permissions,
    verify_registry_management_permissions,
)
from process.registry_migration_preparation import prepare_registry_migrations
from process.registry_record_store import _validated_actor
from tests.test_manual_provider_identity_store_postgres import provider_db, serving_schema
from tests.test_registry_approval_store_postgres import _actor, _command, _create
from tests.test_registry_management_permissions_postgres import _arguments, _draft, _install, permissions_db

pytestmark = pytest.mark.asyncio
_CHANGES = (
    ("20261016070000_registry_allowed_plan_scope", "registry_ptg_published_plan_scope"),
    ("20261017010000_registry_cms_source_notifications", "registry_cms_source_notification"),
    ("20261017030000_registry_ptg_office_recipe_selection", "registry_network_binding"),
    ("20261018011000_network_candidate_custody", "network_candidate_custody"),
    ("20261018040000_registry_aca_source_notifications", "registry_aca_source_notification"),
)


@pytest.fixture
async def prepared_db(permissions_db, monkeypatch):
    fixture = permissions_db
    await _install(fixture)
    api_role, fixture.api_password = fixture.roles_by_kind["api"], uuid4().hex
    await fixture.connection.execute(f"ALTER ROLE \"{api_role}\" LOGIN PASSWORD '{fixture.api_password}'")
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", fixture.schema)
    # Build the actual old physical layout; DROP COLUMN would retain a dropped attnum.
    await fixture.connection.execute(f'DROP TABLE "{fixture.schema}".registry_network_binding')
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["owner"]}"')
        original = registry_migration("20261007130000_registry_network_bindings")
        for statement in original._ddl(fixture.schema)[:2]:
            await fixture.connection.execute(statement)
        columns = ",".join(name for name in _INSERT_COLUMNS["registry_network_binding"] if name != "office_recipe_json")
        await fixture.connection.execute(
            f"GRANT SELECT,INSERT({columns}),UPDATE(network_id,evidence_id,evidence_sha256,archived,revision) "
            f'ON "{fixture.schema}".registry_network_binding TO "{fixture.roles_by_kind["api"]}"'
        )
    await fixture.connection.execute(f'DROP TABLE "{fixture.schema}".registry_aca_source_notification')
    await fixture.connection.execute(f'DROP TABLE "{fixture.schema}".network_candidate_custody')
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_network_binding '
        "(binding_id,source_system,source_id,dataset_schema,dataset_id,producer_id,edition_id,source_key,"
        "source_scope_json,binding_key,network_id,evidence_id,evidence_sha256) "
        "VALUES($1,'ptg','source-a','snapshot_a','dataset-a','producer-a','edition-a','key-a',"
        "'{\"review_type\":\"published_complete_snapshot_plan\"}', $2,1,'evidence-a',$3)",
        uuid4(),
        "a" * 64,
        "b" * 64,
    )
    yield fixture


async def _upgrade(fixture, revision, role, *, validate=False):
    migration = registry_migration(revision)
    shared_engine = fixture.sessions.kw["bind"]
    is_ordinary = role == fixture.roles_by_kind["api"]
    engine = (
        create_async_engine(shared_engine.url.set(username=role, password=fixture.api_password))
        if is_ordinary
        else shared_engine
    )
    try:
        async with engine.begin() as connection:
            if not is_ordinary:
                await connection.execute(text(f'SET LOCAL ROLE "{role}"'))

            def run(sync_connection):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()
                if validate:
                    table = dict(_CHANGES)[revision]
                    if table == "network_candidate_custody":
                        validate_prepared_candidate_custody(sync_connection, fixture.schema)
                    else:
                        validate_prepared_registry_table(sync_connection, fixture.schema, table, migration._postimage)

            await connection.run_sync(run)
            assert await connection.scalar(text("SELECT current_user")) == role
            if is_ordinary:
                assert await connection.scalar(text("SELECT session_user")) == role
            assert (
                await connection.scalar(
                    text(
                        "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() "
                        "AND (relname LIKE 'registry_%' OR relname='network_candidate_custody')"
                    )
                )
                == 0
            )
    finally:
        if is_ordinary:
            await engine.dispose()


async def _prepare(fixture):
    for revision, _ in _CHANGES:
        await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)


async def _acl(fixture):
    rows = await fixture.connection.fetch(
        """SELECT c.relname,NULL::text AS column_name,a.* FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a
        WHERE n.nspname=$1 AND c.relkind='r'
        UNION ALL SELECT c.relname,att.attname,a.* FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute att ON att.attrelid=c.oid
        CROSS JOIN LATERAL aclexplode(att.attacl) a
        WHERE n.nspname=$1 AND att.attnum>0 AND NOT att.attisdropped""",
        fixture.schema,
    )
    return {tuple(row.values()) for row in rows}


@pytest.mark.parametrize("revision,table", _CHANGES)
async def test_ordinary_adopts_only_exact_owner_postimage(prepared_db, revision, table):
    fixture, ordinary = prepared_db, prepared_db.roles_by_kind["api"]
    with pytest.raises(RuntimeError, match="preparation"):
        await _upgrade(fixture, revision, ordinary)
    await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    before = await _acl(fixture)
    rows = await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}".registry_network_binding')
    await _upgrade(fixture, revision, ordinary)
    await _upgrade(fixture, revision, ordinary)
    assert await _acl(fixture) == before
    assert await fixture.connection.fetch(f'SELECT * FROM "{fixture.schema}".registry_network_binding') == rows
    assert not await fixture.connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'SET')", ordinary, fixture.roles_by_kind["owner"]
    )
    assert not await fixture.connection.fetchval(
        "SELECT has_table_privilege($1,$2,'INSERT,UPDATE,DELETE,TRUNCATE')",
        ordinary,
        f'"{fixture.schema}".registry_ptg_published_plan_scope',
    )


@pytest.mark.parametrize(
    "damage",
    [
        "type",
        "constraint",
        "unvalidated",
        "default",
        "index",
        "owner",
        "public",
        "foreign",
        "foreign_column",
        "schema_create",
    ],
)
async def test_binding_adoption_refuses_native_or_acl_drift(prepared_db, damage):
    fixture = prepared_db
    revision = _CHANGES[2][0]
    await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    table = f'"{fixture.schema}".registry_network_binding'
    statement_by_damage = {
        "type": f"ALTER TABLE {table} ALTER COLUMN office_recipe_json TYPE json USING office_recipe_json::json",
        "constraint": f"ALTER TABLE {table} DROP CONSTRAINT registry_network_binding_office_recipe",
        "unvalidated": f"ALTER TABLE {table} DROP CONSTRAINT registry_network_binding_office_recipe; "
        f"ALTER TABLE {table} ADD CONSTRAINT registry_network_binding_office_recipe CHECK(office_recipe_json IS NULL) NOT VALID",
        "default": f"ALTER TABLE {table} ALTER COLUMN office_recipe_json SET DEFAULT '{{}}'::jsonb",
        "index": f'DROP INDEX "{fixture.schema}".registry_network_binding_network_idx',
        "owner": f'ALTER TABLE {table} OWNER TO "{fixture.roles_by_kind["ancestor"]}"',
        "public": f"GRANT SELECT ON {table} TO PUBLIC",
        "foreign": f'GRANT UPDATE ON {table} TO "{fixture.roles_by_kind["ancestor"]}"',
        "foreign_column": f'GRANT UPDATE(revision) ON {table} TO "{fixture.roles_by_kind["ancestor"]}"',
        "schema_create": f'GRANT CREATE ON SCHEMA "{fixture.schema}" TO "{fixture.roles_by_kind["ancestor"]}"',
    }
    if damage == "type":
        await fixture.connection.execute(f"ALTER TABLE {table} DROP CONSTRAINT registry_network_binding_office_recipe")
    await fixture.connection.execute(statement_by_damage[damage])
    before = await _acl(fixture)
    with pytest.raises(RuntimeError, match="preparation"):
        await _upgrade(fixture, revision, fixture.roles_by_kind["api"])
    assert await _acl(fixture) == before


@pytest.mark.parametrize("revision,table", (_CHANGES[1], _CHANGES[4]))
async def test_notification_adoption_refuses_nonpublic_default_grants(prepared_db, revision, table):
    fixture = prepared_db
    await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    await fixture.connection.execute(
        f'GRANT SELECT ON "{fixture.schema}"."{table}" TO "{fixture.roles_by_kind["ancestor"]}"'
    )
    with pytest.raises(RuntimeError, match="preparation"):
        await _upgrade(fixture, revision, fixture.roles_by_kind["api"])


async def test_office_enrollment_adds_only_two_grants_and_is_explicit(prepared_db):
    fixture = prepared_db
    await _prepare(fixture)
    for revision, _ in _CHANGES:
        await _upgrade(fixture, revision, fixture.roles_by_kind["api"])
    with pytest.raises(RegistryManagementPermissionError, match="column privileges"):
        await verify_registry_management_permissions(fixture.connection, **_arguments(fixture))
    before = await _acl(fixture)
    async with fixture.connection.transaction():
        receipt = await install_registry_office_recipe_permissions(fixture.connection, **_arguments(fixture))
    after = await _acl(fixture)
    owner_oid, api_oid = await fixture.connection.fetchrow(
        "SELECT (SELECT oid FROM pg_roles WHERE rolname=$1),(SELECT oid FROM pg_roles WHERE rolname=$2)",
        fixture.roles_by_kind["owner"],
        fixture.roles_by_kind["api"],
    )
    assert after - before == {
        ("registry_network_binding", "office_recipe_json", owner_oid, api_oid, privilege, False)
        for privilege in ("INSERT", "UPDATE")
    }
    assert before <= after
    assert await verify_registry_management_permissions(fixture.connection, **_arguments(fixture)) == receipt
    async with fixture.connection.transaction():
        assert await install_registry_office_recipe_permissions(fixture.connection, **_arguments(fixture)) == receipt
    assert await _acl(fixture) == after


@pytest.mark.parametrize("drift", ["missing_old", "extra", "partial_new", "unprivileged"])
async def test_office_enrollment_refuses_other_drift(prepared_db, drift):
    fixture = prepared_db
    await _prepare(fixture)
    table = f'"{fixture.schema}".registry_network_binding'
    api = f'"{fixture.roles_by_kind["api"]}"'
    if drift == "missing_old":
        await fixture.connection.execute(f"REVOKE UPDATE(revision) ON {table} FROM {api}")
    elif drift == "extra":
        await fixture.connection.execute(f"GRANT DELETE ON {table} TO {api}")
    elif drift == "partial_new":
        await fixture.connection.execute(f"GRANT INSERT(office_recipe_json) ON {table} TO {api}")
    before = await _acl(fixture)
    with pytest.raises(RegistryManagementPermissionError):
        async with fixture.connection.transaction():
            if drift == "unprivileged":
                await fixture.connection.execute(f"SET LOCAL ROLE {api}")
            await install_registry_office_recipe_permissions(fixture.connection, **_arguments(fixture))
    assert await _acl(fixture) == before


async def _maintenance(fixture):
    """Model the existing dedicated maintenance membership, never ordinary membership."""
    owner, operator = fixture.roles_by_kind["owner"], fixture.roles_by_kind["ancestor"]
    fixture.operator_password = uuid4().hex
    await fixture.connection.execute(
        f"ALTER ROLE \"{operator}\" LOGIN NOINHERIT PASSWORD '{fixture.operator_password}'"
    )
    await fixture.connection.execute(f'GRANT "{owner}" TO "{operator}" WITH INHERIT FALSE, SET TRUE')
    await fixture.connection.execute(f'GRANT EXECUTE ON FUNCTION pg_control_system() TO "{owner}"')
    return operator


async def test_atomic_owner_preparation_then_ordinary_adoption(prepared_db):
    fixture = prepared_db
    operator = await _maintenance(fixture)
    before = await _acl(fixture)
    engine = create_async_engine(
        fixture.sessions.kw["bind"].url.set(username=operator, password=fixture.operator_password)
    )
    try:
        async with engine.begin() as connection:
            assert await connection.scalar(text("SELECT session_user=current_user")) is True
            receipt = await prepare_registry_migrations(
                connection, owner_role=fixture.roles_by_kind["owner"], control_schema=fixture.schema
            )
            assert await connection.scalar(text("SELECT current_user")) == operator
    finally:
        await engine.dispose()
    assert receipt["prepared_revisions"] == [revision for revision, _ in _CHANGES]
    assert {
        row
        for row in await _acl(fixture)
        if row[0] in {"registry_network_binding", "registry_ptg_published_plan_scope"}
    } == {row for row in before if row[0] in {"registry_network_binding", "registry_ptg_published_plan_scope"}}
    for revision, _ in _CHANGES:
        await _upgrade(fixture, revision, fixture.roles_by_kind["api"])


async def test_owner_preparation_rolls_back_all_changes_on_late_drift(prepared_db):
    fixture = prepared_db
    operator = await _maintenance(fixture)
    # Non-PUBLIC defaults on the new CMS table must reject the entire preparation.
    owner, foreign = fixture.roles_by_kind["owner"], fixture.roles_by_kind["publisher"]
    await fixture.connection.execute(
        f'ALTER DEFAULT PRIVILEGES FOR ROLE "{owner}" IN SCHEMA "{fixture.schema}" GRANT SELECT ON TABLES TO "{foreign}"'
    )
    before = await _acl(fixture)
    async with fixture.sessions.kw["bind"].begin() as connection:
        await connection.execute(text(f'SET LOCAL ROLE "{operator}"'))
        with pytest.raises(RuntimeError, match="privileges"):
            await prepare_registry_migrations(connection, owner_role=owner, control_schema=fixture.schema)
        assert await connection.scalar(text("SELECT current_user")) == operator
    assert await _acl(fixture) == before
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)", f'"{fixture.schema}".registry_cms_source_notification'
        )
        is None
    )
    with pytest.raises(RuntimeError, match="postimage"):
        await _upgrade(fixture, _CHANGES[0][0], fixture.roles_by_kind["api"])


async def test_owner_preparation_refuses_active_company_fence(prepared_db):
    from process.registry_company_approval_fence import _fence_key

    fixture = prepared_db
    operator = await _maintenance(fixture)
    async with fixture.connection.transaction():
        await fixture.connection.fetchval("SELECT pg_advisory_xact_lock_shared($1)", _fence_key(fixture.schema))
        async with fixture.sessions.kw["bind"].begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{operator}"'))
            with pytest.raises(RuntimeError, match="company approval is active"):
                await prepare_registry_migrations(
                    connection, owner_role=fixture.roles_by_kind["owner"], control_schema=fixture.schema
                )
            assert await connection.scalar(text("SELECT current_user")) == operator


@pytest.mark.parametrize("change", ["missing", "foreign_owner"])
async def test_aca_adoption_retains_serving_owner_prerequisite(prepared_db, change):
    fixture = prepared_db
    revision = _CHANGES[4][0]
    await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    table = f'"{fixture.schema}".network_serving_control'
    if change == "missing":
        await fixture.connection.execute(f"DROP TABLE {table}")
    else:
        await fixture.connection.execute(f'ALTER TABLE {table} OWNER TO "{fixture.roles_by_kind["ancestor"]}"')
    with pytest.raises(RuntimeError, match="serving owner"):
        await _upgrade(fixture, revision, fixture.roles_by_kind["api"])


@pytest.mark.parametrize("state", ["queued", "running"])
async def test_owner_preparation_refuses_even_expired_requests(prepared_db, state):
    fixture = prepared_db
    operator = await _maintenance(fixture)
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_publication_request '
        "(request_id,actor_key,session_token_sha256,actor_json,command_json,idempotency_key,request_sha256,state,lease_id,lease_expires_at) "
        "VALUES($1,$2,$2,'{}','{}','pending-request',$2,$3::varchar,$4,CASE WHEN $3::varchar='running' THEN now()-interval '1 hour' END)",
        uuid4(),
        "a" * 64,
        state,
        uuid4() if state == "running" else None,
    )
    before = await _acl(fixture)
    async with fixture.sessions.kw["bind"].begin() as connection:
        await connection.execute(text(f'SET LOCAL ROLE "{operator}"'))
        with pytest.raises(RuntimeError, match="drained publication"):
            await prepare_registry_migrations(
                connection, owner_role=fixture.roles_by_kind["owner"], control_schema=fixture.schema
            )
    assert await _acl(fixture) == before
    assert (
        await fixture.connection.fetchval(
            "SELECT to_regclass($1)", f'"{fixture.schema}".registry_cms_source_notification'
        )
        is None
    )


async def test_owner_preparation_refuses_active_source_transaction(prepared_db):
    fixture = prepared_db
    operator = await _maintenance(fixture)
    async with fixture.connection.transaction():
        await fixture.connection.execute(
            f'LOCK TABLE "{fixture.schema}".registry_source_snapshot IN ROW EXCLUSIVE MODE'
        )
        async with fixture.sessions.kw["bind"].begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{operator}"'))
            with pytest.raises(asyncpg.LockNotAvailableError):
                await prepare_registry_migrations(
                    connection, owner_role=fixture.roles_by_kind["owner"], control_schema=fixture.schema
                )
            assert await connection.scalar(text("SELECT current_user")) == operator


async def test_ordinary_adoption_refuses_missing_temporary_permission(prepared_db):
    fixture = prepared_db
    revision = _CHANGES[1][0]
    await _upgrade(fixture, revision, fixture.roles_by_kind["owner"], validate=True)
    database = await fixture.connection.fetchval("SELECT quote_ident(current_database())")
    # Only this opted-in test database is affected; restoration precedes fixture teardown.
    try:
        await fixture.connection.execute(f"REVOKE TEMP ON DATABASE {database} FROM PUBLIC")
        with pytest.raises(DBAPIError, match="permission denied"):
            await _upgrade(fixture, revision, fixture.roles_by_kind["api"])
    finally:
        await fixture.connection.execute(f"GRANT TEMP ON DATABASE {database} TO PUBLIC")


async def _retained_binding_history(fixture, actor, change, recipe_by_field):
    """Retain a legacy snapshot or an explicit-field comparison control."""
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["owner"]}"')
        await fixture.connection.execute(
            f'GRANT SELECT ON "{fixture.schema}".registry_network_binding TO "{fixture.roles_by_kind["publisher"]}"'
        )
    network = await _draft(fixture, _create("network"), actor)
    binding_json = await fixture.connection.fetchval(
        f'SELECT to_jsonb(head)::text FROM "{fixture.schema}".registry_network_binding head'
    )
    binding = json.loads(binding_json)
    assert binding["network_id"] == network["record_id"]
    assert "office_recipe_json" not in binding
    if change == "explicit_null":
        binding["office_recipe_json"] = None
    elif change in {"matching_recipe", "history_recipe", "different_recipe"}:
        binding["office_recipe_json"] = recipe_by_field
    elif change == "missing_field":
        del binding["evidence_id"]
    await fixture.connection.execute(
        f'INSERT INTO "{fixture.schema}".registry_record_history '
        "(record_kind,record_key,revision,custom_revision,record_json,actor_json,reason,idempotency_key,request_sha256) "
        "VALUES('network_binding',$1,1,2,$2::jsonb,$3::jsonb,'Retained binding review',$4,$5)",
        binding["binding_id"],
        json.dumps(binding),
        json.dumps(_validated_actor(actor)),
        uuid4().hex,
        "d" * 64,
    )
    await fixture.connection.execute(f'UPDATE "{fixture.schema}".registry_revision_control SET draft_revision=2')
    return network, {"record_kind": "network_binding", "record_id": binding["binding_id"], "revision": 1}


async def _change_binding_head(fixture, change, recipe_by_field):
    if change in {"current_recipe", "matching_recipe", "different_recipe"}:
        selected = {**recipe_by_field, "recipe_sha256": "f" * 64} if change == "different_recipe" else recipe_by_field
        await fixture.connection.execute(
            f'UPDATE "{fixture.schema}".registry_network_binding SET office_recipe_json=$1::jsonb',
            json.dumps(selected),
        )
    elif change == "field":
        await fixture.connection.execute(
            f"UPDATE \"{fixture.schema}\".registry_network_binding SET evidence_id='Changed review'"
        )
    elif change == "revision":
        await fixture.connection.execute(f'UPDATE "{fixture.schema}".registry_network_binding SET revision=2')


@pytest.mark.parametrize(
    "change",
    [
        "legacy",
        "explicit_null",
        "matching_recipe",
        "history_recipe",
        "current_recipe",
        "different_recipe",
        "field",
        "revision",
        "missing_field",
    ],
)
async def test_binding_approval_retains_legacy_office_default(prepared_db, change):
    fixture, actor = prepared_db, _actor()
    recipe_by_field = {"capture_id": str(uuid4()), "recipe_sha256": "e" * 64}
    selection = await _retained_binding_history(fixture, actor, change, recipe_by_field)
    history_sql = f'SELECT to_jsonb(history)::text FROM "{fixture.schema}".registry_record_history history ORDER BY record_kind,record_key,revision'
    retained = await fixture.connection.fetch(history_sql)
    await _prepare(fixture)
    await _upgrade(fixture, _CHANGES[2][0], fixture.roles_by_kind["api"])
    await _change_binding_head(fixture, change, recipe_by_field)
    command = await _command(fixture.connection, fixture.schema, *selection)
    async with fixture.connection.transaction():
        await fixture.connection.execute(f'SET LOCAL ROLE "{fixture.roles_by_kind["publisher"]}"')
        if change in {"legacy", "explicit_null", "matching_recipe"}:
            await preview_registry_approval(fixture.connection, command, actor, control_schema=fixture.schema)
            receipt = await approve_registry_records(fixture.connection, command, actor, control_schema=fixture.schema)
            assert receipt["selected_count"] == 2
        else:
            with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
                await preview_registry_approval(fixture.connection, command, actor, control_schema=fixture.schema)
            with pytest.raises(RegistryApprovalConflict, match="selection_conflict"):
                await approve_registry_records(fixture.connection, command, actor, control_schema=fixture.schema)
            assert (
                await fixture.connection.fetchval(f'SELECT count(*) FROM "{fixture.schema}".registry_approval_history')
                == 0
            )
    assert await fixture.connection.fetch(history_sql) == retained
    if change in {"legacy", "explicit_null", "matching_recipe"}:
        assert (
            await fixture.connection.fetchval(
                f'SELECT approved.record_json=history.record_json FROM "{fixture.schema}".registry_approved_record approved '
                f'JOIN "{fixture.schema}".registry_record_history history ON history.record_kind=approved.record_kind '
                "AND history.record_key=approved.record_key AND history.revision=approved.record_revision "
                "WHERE approved.record_kind='network_binding'"
            )
            is True
        )
