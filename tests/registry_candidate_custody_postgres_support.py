# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured candidate loading and administrative freeze for source fixtures."""

from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import asyncpg

from process.network_address_projection import _identifier
from process.network_membership_publication import NetworkPreparationPending, load_named_network_preparation
from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig, _canonical_session
from process.registry_management_permissions import _aca_controls, _catalog
from process.registry_publication_ptg_runtime import RegistryPublicationSQLLimits
from process.registry_publication_worker import _application_role_names, _configured_worker_runtime, _PTGWorkerConfig
from process.registry_publication_worker_permissions import _grant_worker


async def remove_candidate_schemas(fixture):
    """Drain only registered fixture owners' candidate/composition schemas."""
    owners = [fixture.roles["owner"], fixture.custody_roles["loading"], fixture.custody_roles["owner"]]
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
    for relation in original.get("controls", ()):
        reference = f"{_identifier(fixture.control_schema)}.{_identifier(relation['relname'])}"
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
        login = "NOLOGIN" if kind in {"loading", "owner"} else f"LOGIN PASSWORD '{password_by_kind[kind]}'"
        await fixture.connection.execute(
            f"CREATE ROLE {_identifier(role)} {login} "
            "INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
        )
        if kind not in {"loading", "owner"}:
            await fixture.connection.execute(f"ALTER ROLE {_identifier(role)} SET temp_file_limit='256MB'")


async def _install_grants(fixture, original_owner_by_field):
    connection, roles = fixture.connection, fixture.custody_roles
    namespace = _identifier(fixture.control_schema)
    control_owner = fixture.roles["owner"]
    async with connection.transaction():
        await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {_identifier(control_owner)}")
        tables, sequences = await _catalog(connection, fixture.control_schema)
        controls = await _aca_controls(connection, fixture.control_schema)
        ledger = next(relation for relation in tables if relation["relname"] == "network_candidate_custody")
        candidate = next(relation for relation in tables if relation["relname"] == "network_membership_candidate")
        # Keep the ledger's existing parent FK intact when the outer fixture retires its control owner.
        retained_controls = [*controls, candidate, ledger]
        original_owner_by_field["controls"] = retained_controls
        for relation in tables + controls + sequences:
            kind = "SEQUENCE" if relation in sequences else "TABLE"
            await connection.execute(
                f"ALTER {kind} {namespace}.{_identifier(relation['relname'])} OWNER TO {_identifier(control_owner)}"
            )
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {_identifier(roles['execution'])}")
        await _grant_worker(connection, fixture.control_schema, tables, roles["execution"], execution=True)
        from process.registry_fhir_catalog_permissions import _grant_named_preparation_control

        await _grant_named_preparation_control(connection, roles["execution"])
        await connection.execute(
            f"GRANT USAGE ON SEQUENCE {namespace}.network_serving_manifest_generation_id_seq "
            f"TO {_identifier(roles['execution'])}"
        )
        database = _identifier(await connection.fetchval("SELECT current_database()"))
        await connection.execute(
            f"GRANT CREATE ON DATABASE {database} TO {_identifier(roles['loading'])},{_identifier(control_owner)}"
        )
        for parent, child, inherit, assume in (
            (roles["loading"], roles["execution"], True, True),
            (fixture.roles["reader"], roles["execution"], True, False),
            (roles["execution"], roles["custodian"], True, False),
            (roles["loading"], roles["custodian"], True, True),
        ):
            await connection.execute(
                f"GRANT {_identifier(parent)} TO {_identifier(child)} WITH ADMIN FALSE, "
                f"INHERIT {str(inherit).upper()}, SET {str(assume).upper()}"
            )


async def install_candidate_custody(fixture, stack):
    """Register all cleanup before creating the isolated native identities."""
    token = uuid4().hex
    fixture.custody_roles = {
        kind: "registry_custody_" + kind + "_" + token for kind in ("loading", "owner", "execution", "custodian")
    }
    attempted_roles = []
    original_owner_by_field = await fixture.connection.fetchrow(
        "SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid=n.nspowner WHERE n.nspname=$1",
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
    fixture.writer_roles = dict(
        owner_role=fixture.custody_roles["loading"], loader_roles=(), reader_roles=(fixture.roles["reader"],)
    )
    fixture.custody_stack = stack


async def candidate_custody_consumer(fixture, pair, old, store, composition_runtime=None):
    """Run the real configured worker callback through its administrator handoff."""
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
        notification_store=store,
        reader_role=fixture.roles["reader"],
        ptg=ptg,
        sql_limits=fixture.sql_limits,
    )
    settings.preparation_roles = {
        "namespace_owner_role": fixture.custody_roles["owner"],
        "publisher_role": fixture.executor_role,
        "application_roles": sorted(
            set(_application_role_names(settings))
            | set(fixture.roles.values())
            | set(pair.runtime_roles + old.runtime_roles)
            | {fixture.custody_roles["loading"], fixture.custody_roles["custodian"]}
        ),
    }
    consumer = await _configured_worker_runtime(settings, fixture.custody_stack)
    consumer = replace(consumer, seal_candidate=administrative_freeze_callback(fixture, consumer))
    fixture.seal_candidate = consumer.seal_candidate
    assert callable(fixture.seal_candidate)
    assert consumer.prepare_address_sources is None
    return consumer


def administrative_freeze_callback(fixture, consumer):
    """Resume the same application callback after the real, separately owned admin step."""
    callback = consumer.seal_candidate

    async def seal_candidate(copy_target, **options):
        try:
            return await callback(copy_target, **options)
        except NetworkPreparationPending as pending:
            assert pending.candidate_id == str(copy_target.candidate_id)
        handoff = await load_named_network_preparation(
            fixture.connection, copy_target.candidate_id, control_schema=fixture.control_schema
        )
        assert handoff is not None and handoff["candidate_id"] == str(copy_target.candidate_id)
        from directory_catalog_runtime import freeze_registry_generation

        config_by_field = {
            **{field: handoff[field] for field in ("kind", "candidate_id", "catalog_schema", "storage_schema")},
            "preparation_roles": consumer.preparation_roles,
        }
        async with fixture.engine.connect() as administrator:
            frozen = await freeze_registry_generation(administrator, config_by_field, copy_target.candidate_id)
        assert frozen["preparation_id"] == str(copy_target.candidate_id)
        assert frozen["state"] in {"frozen", "validated", "published"}
        return await callback(copy_target, **options)

    return seal_candidate
