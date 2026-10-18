# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine independent candidate custody for the direct Registry source fixtures."""

from types import SimpleNamespace
from uuid import uuid4

import asyncpg

from process.network_address_projection import _identifier
from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig, _canonical_session
from process.registry_management_permissions import _aca_controls, _catalog
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import _custody_roles, _custody_runtime, _PTGWorkerConfig
from process.registry_publication_worker_permissions import _grant_worker


async def remove_candidate_schemas(fixture):
    """Drain only the two registered fixture owners' candidate/composition schemas."""
    owners = [fixture.roles["owner"], fixture.custody_roles["loading"]]
    names = await fixture.connection.fetch(
        "SELECT nspname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner "
        "WHERE r.rolname=ANY($1::text[]) "
        "AND (nspname LIKE 'network_composition_%' OR nspname LIKE 'network_candidate_%')",
        owners,
    )
    for row in names:
        await fixture.connection.execute(f"DROP SCHEMA {_identifier(row['nspname'])} CASCADE")


async def _restore_control_owner(fixture, original):
    """Return the exact fixture namespace without dropping other owners' relations."""
    current = await fixture.connection.fetchrow(
        "SELECT n.oid,r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1",
        fixture.control_schema,
    )
    assert current is not None and current["oid"] == original["oid"]
    assert current["rolname"] in (original["rolname"], fixture.roles["owner"])
    assert (
        await fixture.connection.fetchval("SELECT rolname FROM pg_roles WHERE oid=$1", original["owner_oid"])
        == original["rolname"]
    )
    for relation in original.get("controls", ()):
        reference = f"{_identifier(fixture.control_schema)}.{_identifier(relation['relname'])}"
        retained = await fixture.connection.fetchrow(
            "SELECT c.oid,r.rolname FROM pg_class c JOIN pg_roles r ON r.oid=c.relowner WHERE c.oid=to_regclass($1)",
            reference,
        )
        assert retained is not None and retained["oid"] == relation["oid"]
        assert retained["rolname"] in (original["rolname"], fixture.roles["owner"])
        await fixture.connection.execute(f"ALTER TABLE {reference} OWNER TO {_identifier(original['rolname'])}")
    await fixture.connection.execute(
        f"ALTER SCHEMA {_identifier(fixture.control_schema)} OWNER TO {_identifier(original['rolname'])}"
    )


async def _remove_custody(fixture, attempted_roles, original_owner_by_field):
    await remove_candidate_schemas(fixture)
    await _restore_control_owner(fixture, original_owner_by_field)
    for role in reversed(attempted_roles):
        if await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role):
            await fixture.connection.execute(f"DROP OWNED BY {_identifier(role)}")
            await fixture.connection.execute(f"DROP ROLE {_identifier(role)}")
    assert not await fixture.connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=ANY($1::text[]))", attempted_roles
    )


async def _create_roles(fixture, attempted_roles, password_by_kind):
    for kind, role in fixture.custody_roles.items():
        assert not await fixture.connection.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", role)
        attempted_roles.append(role)
        login = "NOLOGIN" if kind == "loading" else f"LOGIN PASSWORD '{password_by_kind[kind]}'"
        await fixture.connection.execute(
            f"CREATE ROLE {_identifier(role)} {login} "
            "INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        if kind != "loading":
            await fixture.connection.execute(f"ALTER ROLE {_identifier(role)} SET temp_file_limit='256MB'")


async def _install_grants(fixture, original_owner_by_field):
    connection, roles = fixture.connection, fixture.custody_roles
    namespace = _identifier(fixture.control_schema)
    final_owner = fixture.roles["owner"]
    async with connection.transaction():
        await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {_identifier(final_owner)}")
        oid = await connection.fetchval("SELECT to_regnamespace($1)::oid", fixture.control_schema)
        tables, sequences = await _catalog(connection, oid)
        controls = await _aca_controls(connection, fixture.control_schema)
        assert all(relation["relowner"] == original_owner_by_field["owner_oid"] for relation in controls)
        original_owner_by_field["controls"] = controls
        for relation in tables + controls + sequences:
            kind = "SEQUENCE" if relation in sequences else "TABLE"
            await connection.execute(
                f"ALTER {kind} {namespace}.{_identifier(relation['relname'])} OWNER TO {_identifier(final_owner)}"
            )
        await _grant_worker(connection, fixture.control_schema, tables, roles["execution"], execution=True)
        await connection.execute(
            f"GRANT USAGE ON SEQUENCE {namespace}.network_serving_manifest_generation_id_seq "
            f"TO {_identifier(roles['execution'])}"
        )
        database = _identifier(await connection.fetchval("SELECT current_database()"))
        await connection.execute(
            f"GRANT CREATE ON DATABASE {database} TO {_identifier(roles['loading'])},{_identifier(final_owner)}"
        )
        for parent, child, inherit, assume in (
            (roles["loading"], roles["execution"], True, True),
            (fixture.roles["reader"], roles["execution"], True, False),
            (roles["execution"], roles["custodian"], True, False),
            (roles["loading"], roles["custodian"], True, True),
            (final_owner, roles["custodian"], False, True),
        ):
            await connection.execute(
                f"GRANT {_identifier(parent)} TO {_identifier(child)} WITH ADMIN FALSE, "
                f"INHERIT {str(inherit).upper()}, SET {str(assume).upper()}"
            )


async def install_candidate_custody(fixture, stack):
    """Register all cleanup before creating the isolated native identities."""
    token = uuid4().hex
    fixture.custody_roles = {
        kind: "registry_custody_" + kind + "_" + token for kind in ("loading", "execution", "custodian")
    }
    attempted_roles = []
    original_owner_by_field = await fixture.connection.fetchrow(
        "SELECT n.oid,n.nspowner AS owner_oid,r.rolname FROM pg_namespace n "
        "JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1",
        fixture.control_schema,
    )
    assert original_owner_by_field is not None
    original_owner_by_field = dict(original_owner_by_field)
    stack.push_async_callback(_remove_custody, fixture, attempted_roles, original_owner_by_field)
    password_by_kind = {kind: uuid4().hex for kind in ("execution", "custodian")}
    await _create_roles(fixture, attempted_roles, password_by_kind)
    await _install_grants(fixture, original_owner_by_field)
    fixture.execution_url = fixture.engine.url.set(
        username=fixture.custody_roles["execution"], password=password_by_kind["execution"]
    )
    fixture.custody_url = fixture.engine.url.set(
        username=fixture.custody_roles["custodian"], password=password_by_kind["custodian"]
    )
    fixture.sql_limits = RegistryPublicationSQLLimits()
    fixture.execution_connection = await asyncpg.connect(
        fixture.execution_url.set(drivername="postgresql").render_as_string(hide_password=False),
        timeout=fixture.sql_limits.connect_timeout_seconds,
        server_settings=fixture.sql_limits.server_settings(),
    )
    stack.push_async_callback(fixture.execution_connection.close)
    await _canonical_session(fixture.execution_connection)
    await fixture.sql_limits.verify(fixture.execution_connection)
    fixture.executor_role = fixture.custody_roles["execution"]
    assert await fixture.execution_connection.fetchval(
        "SELECT current_user=session_user AND current_user::text=$1", fixture.executor_role
    )
    assert not await fixture.execution_connection.fetchval(
        "SELECT pg_has_role(current_user,$1::name,'SET')", fixture.roles["owner"]
    )
    fixture.writer_roles = dict(
        owner_role=fixture.custody_roles["loading"], loader_roles=(), reader_roles=(fixture.roles["reader"],)
    )
    fixture.custody_stack = stack


async def candidate_custody_consumer(fixture, pair, old, store, composition_runtime=None):
    """Bind the real worker custody pool to the original safe execution connection."""
    for role in sorted(set(pair.runtime_roles + old.runtime_roles)):
        await fixture.connection.execute(
            f"GRANT {_identifier(role)} TO {_identifier(fixture.executor_role)} "
            "WITH ADMIN FALSE, INHERIT TRUE, SET FALSE"
        )
    consumer = RegistryCMSSourceConsumerConfig(
        fixture.control_schema,
        fixture.executor_role,
        fixture.custody_roles["loading"],
        pair.recipe.binding_coordinates,
        old.recipe.binding_coordinates,
        reader_roles=pair.runtime_roles,
        composition_runtime=composition_runtime,
    )
    ptg = (
        None
        if composition_runtime is None
        else _PTGWorkerConfig(composition_runtime.store, composition_runtime.read_budget)
    )
    settings = SimpleNamespace(
        consumer=consumer,
        execution_url=fixture.execution_url,
        custody_url=fixture.custody_url,
        notification_store=store,
        reader_role=fixture.roles["reader"],
        ptg=ptg,
        sql_limits=fixture.sql_limits,
    )
    assert set(pair.runtime_roles + old.runtime_roles) <= set(_custody_roles(settings))
    consumer = await _custody_runtime(settings, fixture.custody_stack, consumer, fixture.execution_connection)
    fixture.seal_candidate = consumer.seal_candidate
    assert callable(fixture.seal_candidate)
    return consumer
