# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real native SOURCE authority and isolated NPPES candidates on disposable data."""

import ast
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from graphlib import TopologicalSorter
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import Index, MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine

from db.migration_nppes_evidence_candidates import upgrade_nppes_evidence_candidates
from process import nppes_public_evidence_storage as storage
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_callable_custody as source_callables
from process.nppes_public_evidence_control_custody import adopt_nppes_control_custody, attest_nppes_control_custody
from process.provider_directory_cms_callable_custody import provision_cms_callable_custody
from process.provider_directory_cms_control_custody import (
    adopt_cms_control_custody,
    capture_cms_control_custody,
    require_cms_control_adoption_delta,
)
from process.provider_directory_cms_receipt_custody import provision_cms_serving_receipt_custody
from process.provider_directory_profile_initial_custody import provision_initial_callable_custody
from process.provider_directory_source_acquisition_custody import lock_source_control_family
from process.provider_directory_source_callable_custody import capture_source_callable_custody
from process.provider_directory_source_namespace_custody import (
    capture_source_namespace_custody,
    provision_source_namespace_custody,
)
from tests.cms_npd_admission_postgres_support import (
    _owned_database,
    _provision_cms_epoch_controls,
    _run_migrations,
)
from tests.formulary_fhir_twin_admission_pg_support import load_migration, run_migration
from tests.provider_directory_source_postgres_support import (
    native_source_component,
    provision_source_component,
    source_component_migrations,
    verify_native_source_profile,
)
from tests.public_evidence_storage_postgres_support import database_url

_COMPONENT = ContextVar("nppes_candidate_native_component", default=None)
SYNTHETIC_PRODUCER = ("a" * 40, "sha256:" + "b" * 64)


def nppes_source_component_migrations():
    """Include the authentic full callable corpus and its fixed typed-call repairs."""
    prefixes = tuple(
        dict.fromkeys(
            (
                *source_component_migrations(),
                "20260825090000",
                "20260830090000",
                "20260909120000",
                "20261001110000",
                "20261004000000",
                "20261016010000",
                "20261016030000",
                "20261016040000",
                "20261016050000",
                "20261016060000",
                "20261019130000",
            )
        )
    )
    versions = Path(__file__).parents[1] / "alembic" / "versions"
    metadata = {}
    revision_by_path = {}
    for path in versions.glob("*.py"):
        fields_by_key = {}
        for node in ast.parse(path.read_text()).body:
            migration_targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
                if isinstance(node, ast.AnnAssign)
                else ()
            )
            for migration_target in migration_targets:
                if isinstance(migration_target, ast.Name) and migration_target.id in {"revision", "down_revision"}:
                    fields_by_key[migration_target.id] = ast.literal_eval(node.value)
        if fields_by_key.get("revision"):
            revision = fields_by_key["revision"]
            assert revision not in metadata
            parent = fields_by_key.get("down_revision")
            metadata[revision] = () if parent is None else (parent,) if isinstance(parent, str) else parent
            revision_by_path[path] = revision
    selected_by_revision = {}
    for prefix in prefixes:
        paths = list(versions.glob(prefix + "*.py"))
        if len(paths) > 1:
            paths = [path for path in paths if "provider_directory" in path.name]
        assert len(paths) == 1, prefix
        selected_by_revision[revision_by_path[paths[0]]] = prefix
    graph_by_revision = {}
    for revision in selected_by_revision:
        ancestors, pending = set(), list(metadata[revision])
        while pending:
            parent = pending.pop()
            if parent not in ancestors:
                ancestors.add(parent)
                pending.extend(metadata.get(parent, ()))
        graph_by_revision[revision] = tuple(parent for parent in selected_by_revision if parent in ancestors)
    return tuple(selected_by_revision[revision] for revision in TopologicalSorter(graph_by_revision).static_order())


def nppes_source_component_migration_stages():
    """Install canonical primitives before typed-call repairs and genuine custody."""
    prefixes = nppes_source_component_migrations()
    boundary = prefixes.index("20261016040000")
    return prefixes[:boundary], prefixes[boundary:]


async def _bootstrap_nppes_parent_component(engine, policy, prefixes):
    """Create the real legacy Builder family before its genuine custody cuts."""
    from db.models import CMSDoctorEducation, DoctorClinicianAddress

    builder = policy.control_runtime_roles[0]
    async with engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{database}" TO "{builder}"'))
    historical = create_async_engine(engine.url.set(username=builder))
    try:
        async with historical.begin() as connection:
            for model in (DoctorClinicianAddress, CMSDoctorEducation):
                table = model.__table__.to_metadata(MetaData(), schema=policy.schema)
                assert await connection.scalar(text("SELECT to_regclass(:name)"), dict(name=table.fullname)) is None
                await connection.run_sync(lambda native: table.create(native, checkfirst=False))
                for specification in getattr(model, "__my_additional_indexes__", ()):
                    index = Index(
                        table.name + "_idx_" + specification["name"],
                        *(table.c[name] for name in specification["index_elements"]),
                    )
                    await connection.run_sync(lambda native: index.create(native, checkfirst=False))
        async with historical.connect() as connection:
            await connection.run_sync(_run_migrations, prefixes)
    finally:
        await historical.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{database}" FROM "{builder}"'))


async def install_source_canonical_functions(engine, schema):
    """Install authentic shared primitives without unrelated terminal guard patches."""
    signatures = (
        "ptg_wave_json_ascii_text_v1(text)",
        "ptg_wave_canonical_json_ascii_v1(jsonb)",
        "ptg_wave_canonical_json_ascii_v1(json)",
        "ptg_wave_hex_to_numeric_v1(text)",
        "ptg_wave_rsa2048_pkcs1_sha256_verify_v1(text,text,integer,bytea)",
        "ptg_wave_is_valid_signed_receipt_v1(jsonb,text,jsonb,text,text,integer)",
        "ptg_wave_ordinary_terminal_receipt_guard()",
    )
    async with engine.connect() as connection:
        assert (
            await connection.scalar(
                text(
                    "SELECT NOT EXISTS (SELECT 1 FROM unnest(CAST(:signatures AS text[])) f(signature) "
                    "WHERE to_regprocedure(signature) IS NOT NULL)"
                ),
                dict(signatures=[f'"{schema}".{signature}' for signature in signatures]),
            )
            is True
        ), "canonical fragment installation requires the untouched component slice"
    versions = Path(__file__).parents[1] / "alembic" / "versions"
    canonical = load_migration(versions / "20260810110000_ptg_wave_receipt_authority.py", "nppes_canonical")
    canonical.install = lambda: canonical._install_receipt_verification_functions(schema)
    await run_migration(engine, canonical, "install")
    json_overload = load_migration(
        versions / "20260820030000_ptg_ordinary_terminal_json_canonical_digest.py", "nppes_json_canonical"
    )
    json_overload._schema = lambda: schema
    json_overload.install = lambda: json_overload.op.execute(json_overload._canonical_json_function_sql())
    await run_migration(engine, json_overload, "install")


async def install_nppes_enumeration_storage(engine, policy):
    """Run authentic enumeration SQL with the pure NPI helper's legacy Builder actor."""
    path = (
        Path(__file__).parents[1] / "alembic" / "versions" / "20260808170000_public_evidence_npi_enumeration_storage.py"
    )
    migration = load_migration(path, "nppes_enumeration")

    def install():
        operation = migration.op
        name = f'"{policy.schema}"."public_evidence_npi_valid"('

        def execute(statement):
            is_pure_helper = statement.lstrip().startswith(
                ("CREATE FUNCTION " + name, "REVOKE ALL ON FUNCTION " + name)
            )
            if not is_pure_helper:
                return operation.execute(statement)
            operation.execute(f'SET LOCAL ROLE "{policy.control_runtime_roles[0]}"')
            try:
                return operation.execute(statement)
            finally:
                operation.execute(f'SET LOCAL ROLE "{policy.owner_role}"')

        operation.execute(f'SET LOCAL ROLE "{policy.owner_role}"')
        migration.op = SimpleNamespace(execute=execute)
        try:
            migration.upgrade()
        finally:
            migration.op = operation

    migration.install = install
    await run_migration(engine, migration, "install")


async def capture_source_legacy_callable_custody(connection, policy, *, migration_role, namespace_owner_role):
    """Keep the production proof intact and identify exact ACL refusals read-only."""
    try:
        return await capture_source_callable_custody(
            connection,
            policy,
            legacy=True,
            migration_role=migration_role,
            namespace_owner_role=namespace_owner_role,
        )
    except ValueError as error:
        if str(error) != "source_callable_acl_changed":
            raise
        roles_by_name = {
            callable_row["rolname"]: callable_row["oid"]
            for callable_row in await connection.fetch(
                "SELECT rolname,oid::bigint AS oid FROM pg_roles WHERE rolname=ANY($1::text[])",
                [policy.owner_role, policy.control_runtime_roles[0], namespace_owner_role],
            )
        }
        roles_by_oid = dict(
            owner=roles_by_name[policy.owner_role],
            builder=roles_by_name[policy.control_runtime_roles[0]],
            namespace_owner=roles_by_name[namespace_owner_role],
        )
        callable_rows = await connection.fetch(
            source_callables._FUNCTION_SQL,
            list(source_callables.source_callable_signatures(policy.schema)),
            policy.schema,
            list(source_callables.SOURCE_CALLABLE_NAMES),
            list(policy.runtime_roles),
        )
        failures = []
        for callable_row in callable_rows:
            acl_by_field = dict(
                definition=source_callables._decoded(callable_row["definition"]),
                acl=source_callables._decoded(callable_row["acl"]),
            )
            try:
                source_callables._require_acl(acl_by_field, roles_by_oid, legacy=True)
            except ValueError as refusal:
                failures.append(
                    dict(signature=callable_row["signature"], acl=acl_by_field["acl"], refusal=str(refusal))
                )
        raise ValueError("native_source_legacy_acl_refused " + json.dumps(failures, sort_keys=True)) from error


async def assert_source_callable_census(connection, policy):
    """Explain absent or extra native overloads before the full exact-body proof."""
    expected = await connection.fetch(
        "SELECT signature,to_regprocedure(signature)::oid::bigint AS oid FROM unnest($1::text[]) f(signature)",
        list(source_callables.source_callable_signatures(policy.schema)),
    )
    actual = await connection.fetch(
        "SELECT p.oid::bigint AS oid,p.proname AS name,pg_get_function_identity_arguments(p.oid) AS arguments "
        "FROM pg_proc p WHERE p.pronamespace=to_regnamespace($1) AND p.proname=ANY($2::text[]) ORDER BY p.oid",
        policy.schema,
        list(source_callables.SOURCE_CALLABLE_NAMES),
    )
    expected_oids = {record["oid"] for record in expected if record["oid"] is not None}
    missing_entries = [record["signature"] for record in expected if record["oid"] is None]
    unexpected_entries = [dict(record) for record in actual if record["oid"] not in expected_oids]
    assert not missing_entries and not unexpected_entries and len(actual) == len(expected) == len(expected_oids), (
        json.dumps(
            dict(
                missing_signatures=missing_entries,
                unexpected_overloads=unexpected_entries,
                expected_count=len(expected),
                actual_count=len(actual),
            ),
            sort_keys=True,
        )
    )


def candidate_component(schema):
    component = _COMPONENT.get()
    assert component is not None and component.policy.schema == schema
    return component


async def resolved_family(schema, admission_ref):
    component = candidate_component(schema)
    async with source.source_transaction(component.database, isolation="repeatable read"):
        connection = source.bound_publisher().connection
        return await storage.resolve_nppes_evidence_family(connection, schema, admission_ref)


async def loading_family(connection, schema, admission_ref):
    """Inspect only the test administrator's persisted loading candidate binding."""
    value = await connection.fetchval(
        f'SELECT candidate_storage_json FROM "{schema}"."{storage.HEADER}" WHERE admission_ref=$1', admission_ref
    )
    binding = json.loads(value) if isinstance(value, str) else value
    assert binding["phase"] == "loading"
    return {root: proof["name"] for root, proof in binding["tables"].items()}


async def _enroll_native_custody(component, namespace, migrator, *, native_inputs=False):
    policy = component.policy
    async with component.admin_engine.begin() as connection:
        await connection.execute(
            text(f'CREATE ROLE "{namespace}" NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS')
        )
        await connection.execute(
            text(f'CREATE ROLE "{migrator}" LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS')
        )
        await connection.execute(
            text(f'GRANT "{policy.owner_role}","{namespace}" TO "{migrator}" WITH INHERIT TRUE, SET TRUE, ADMIN FALSE')
        )
        await connection.execute(text(f'REVOKE "{policy.owner_role}" FROM "{component.retained_publisher}"'))
        await connection.execute(text(f'ALTER SCHEMA "{policy.schema}" OWNER TO "{namespace}"'))
        await connection.execute(text(f'GRANT USAGE ON SCHEMA "{policy.schema}" TO "{policy.owner_role}"'))
    await _provision_cms_epoch_controls(component)
    async with component.admin_engine.begin() as connection:
        await connection.execute(text("SELECT current_user=session_user"))
        driver = (await connection.get_raw_connection()).driver_connection
        await provision_cms_callable_custody(driver, policy, verify_role_profile=verify_native_source_profile)
        await provision_cms_serving_receipt_custody(
            driver, policy, migration_role=migrator, verify_role_profile=verify_native_source_profile
        )
        await provision_initial_callable_custody(driver, policy, verify_role_profile=verify_native_source_profile)
        await provision_source_namespace_custody(
            driver, policy, namespace_owner_role=namespace, verify_role_profile=verify_native_source_profile
        )
        await capture_source_namespace_custody(driver, policy, namespace_owner_role=namespace)
        arguments_by_key = dict(migration_role=migrator, namespace_owner_role=namespace)
        await assert_source_callable_census(driver, policy)
        await capture_source_legacy_callable_custody(driver, policy, **arguments_by_key)
        await lock_source_control_family(driver, policy)
        before = await capture_cms_control_custody(driver, policy, legacy=True, **arguments_by_key)
        expected_checks = await adopt_cms_control_custody(driver, policy, before)
        after = await capture_cms_control_custody(driver, policy, **arguments_by_key)
        require_cms_control_adoption_delta(before, after, expected_checks)
        await adopt_nppes_control_custody(driver, policy.schema, policy, **arguments_by_key)
        await attest_nppes_control_custody(driver, policy.schema, policy, **arguments_by_key)
    component.factory.migration_role = migrator
    component.factory.namespace_owner_role = namespace
    if native_inputs:
        assert component.factory._native_input_producer is not None
        component.factory._require_current_policy()
    else:
        component.factory.producer_identity = SYNTHETIC_PRODUCER


async def _install_nppes_historical_storage(engine, url, component, migrations):
    historical = create_async_engine(url.set(drivername="postgresql+asyncpg", username=component.builder))
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f'GRANT CREATE ON DATABASE "{url.database}" TO "{component.builder}"'))
        await install_source_canonical_functions(historical, "mrf")
        async with historical.connect() as connection:
            await connection.run_sync(_run_migrations, migrations)
        async with engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            await connection.run_sync(
                _run_migrations,
                (
                    "20260808090000",
                    "20260808100000",
                ),
            )
        await install_nppes_enumeration_storage(engine, component.policy)
        async with engine.begin() as connection:
            await connection.execute(text(f'SET LOCAL ROLE "{component.owner}"'))
            await connection.run_sync(_run_migrations, ("20260808220000", "20260809020000"))
    finally:
        await historical.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{url.database}" FROM "{component.builder}"'))


async def _drop_nppes_component_roles(engine, roles):
    async with engine.begin() as connection:
        administrator = await connection.scalar(text("SELECT session_user"))
        for role in roles:
            if await connection.scalar(text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}):
                await connection.execute(text(f'REASSIGN OWNED BY "{role}" TO "{administrator}"'))
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


async def _install_nppes_publication(engine):
    from tests.npi_canonical_publication_postgres_support import CANONICAL_TABLES, load_publication_migration

    async with engine.begin() as connection:
        for ordinal, table in enumerate(CANONICAL_TABLES, 1):
            await connection.execute(text(f'CREATE TABLE "mrf"."{table}" (synthetic_id bigint PRIMARY KEY)'))
            await connection.execute(text(f'INSERT INTO "mrf"."{table}" SELECT generate_series(1,{ordinal})'))
    from tests.public_evidence_storage_postgres_support import run_migration_action

    migration = load_publication_migration()
    migration._schema = lambda: "mrf"
    await run_migration_action(engine, migration, "upgrade")
    return migration


@asynccontextmanager
async def existing_nppes_component(component):
    """Borrow a caller-owned component only after its complete native NPPES attestation."""
    factory = component.factory
    namespace = getattr(factory, "namespace_owner_role", None)
    migrator = getattr(factory, "migration_role", None)
    if not isinstance(namespace, str) or not isinstance(migrator, str):
        raise RuntimeError("caller_owned_nppes_enrollment_required")
    assert _COMPONENT.get() is None
    assert source._FACTORY.get() is factory and factory.policy is component.policy
    assert factory.database is component.database
    async with source.source_transaction(component.database):
        publisher = source.bound_publisher()
        assert publisher.factory is factory and publisher.policy is component.policy
        await attest_nppes_control_custody(
            publisher.connection,
            component.policy.schema,
            component.policy,
            migration_role=migrator,
            namespace_owner_role=namespace,
        )
    token = _COMPONENT.set(component)
    try:
        yield component
    finally:
        _COMPONENT.reset(token)


async def _prepare_nppes_component(component, url, namespace, migrator, publication):
    """Keep the original bootstrap and custody sequence before exposing component authority."""
    engine = component.admin_engine
    before_canonical, after_canonical = nppes_source_component_migration_stages()
    await _bootstrap_nppes_parent_component(engine, component.policy, before_canonical)
    await _install_nppes_historical_storage(engine, url, component, after_canonical)
    await provision_source_component(engine, component.policy)
    async with engine.begin() as connection:
        await connection.run_sync(lambda sync: upgrade_nppes_evidence_candidates(sync, "mrf"))
    await _enroll_native_custody(component, namespace, migrator)
    return await _install_nppes_publication(engine) if publication else None


@asynccontextmanager
async def _ready_nppes_component(engine, url, settings, namespace, migrator, publication):
    async with native_source_component(engine, "mrf", settings, legacy_builder=True) as component:
        migration = await _prepare_nppes_component(component, url, namespace, migrator, publication)
        token = _COMPONENT.set(component)
        try:
            yield migration
        finally:
            _COMPONENT.reset(token)


@asynccontextmanager
async def composed_nppes_component(component, *, migration_prefixes=None):
    """Enroll the caller's real model family before its first consumer custody capture."""
    if migration_prefixes is not None:
        from tests.cms_registry_complete_publication_postgres_support import source_component_native_migrations

        if type(migration_prefixes) is not tuple or migration_prefixes != source_component_native_migrations():
            raise RuntimeError("caller_owned_nppes_migration_profile_required")
    engine, policy = component.admin_engine, component.policy
    namespace, migrator = "nppes_namespace_" + uuid4().hex[:16], "nppes_migrator_" + uuid4().hex[:16]
    assert _COMPONENT.get() is None and source._FACTORY.get() is component.factory
    assert not hasattr(component.factory, "migration_role")
    assert not hasattr(component.factory, "namespace_owner_role")
    borrowed_attributes = (
        "migration_role",
        "namespace_owner_role",
        "producer_identity",
        "_native_input_producer",
        "native_tiger_input_fence",
    )
    prior_value_by_name = {
        name: getattr(component.factory, name) for name in borrowed_attributes if hasattr(component.factory, name)
    }
    try:
        if component.factory._native_input_producer is None:
            component.factory.enroll_ordinary_native_inputs()
        component.factory._require_current_policy()
        if migration_prefixes is None:
            base_prefixes = set(source_component_migrations())
            missing_prefixes = tuple(
                prefix for prefix in nppes_source_component_migrations() if prefix not in base_prefixes
            )
            await _install_nppes_historical_storage(engine, engine.url, component, missing_prefixes)
        async with engine.begin() as connection:
            await connection.run_sync(lambda sync: upgrade_nppes_evidence_candidates(sync, policy.schema))
        if migration_prefixes is None:
            await _install_existing_npi_publication(engine, policy.schema)
        await _enroll_native_custody(component, namespace, migrator, native_inputs=True)
        async with existing_nppes_component(component):
            yield component
    finally:
        for name in borrowed_attributes:
            if name in prior_value_by_name:
                setattr(component.factory, name, prior_value_by_name[name])
            elif hasattr(component.factory, name):
                delattr(component.factory, name)
        await _drop_nppes_component_roles(engine, (migrator, namespace))


async def _install_existing_npi_publication(engine, schema):
    """Install the real receipt migration on the six existing canonical model heaps."""
    from tests.npi_canonical_publication_postgres_support import CANONICAL_TABLES, load_publication_migration
    from tests.public_evidence_storage_postgres_support import run_migration_action

    migration = load_publication_migration()
    migration._schema = lambda: schema
    async with engine.begin() as connection:
        for name in CANONICAL_TABLES:
            assert await connection.scalar(text("SELECT to_regclass(:name)"), {"name": f"{schema}.{name}"}) is not None
    await run_migration_action(engine, migration, "upgrade")


@asynccontextmanager
async def nppes_candidate_schema(*, publication=False):
    """Construct real migration/callable/namespace custody; no runtime-image claim."""
    async with _owned_database(database_url()) as (url, _admin):
        engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
        namespace, migrator = "nppes_namespace_" + uuid4().hex[:16], "nppes_migrator_" + uuid4().hex[:16]
        try:
            with pytest.MonkeyPatch.context() as settings:
                settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
                settings.setenv("DB_SCHEMA", "mrf")
                async with engine.begin() as connection:
                    await connection.execute(text('CREATE SCHEMA "mrf"'))
                async with _ready_nppes_component(engine, url, settings, namespace, migrator, publication) as migration:
                    yield engine, url, "mrf", migration
        finally:
            await _drop_nppes_component_roles(engine, (migrator, namespace))
            await engine.dispose()


@asynccontextmanager
async def nppes_candidate_publication_schema():
    async with nppes_candidate_schema(publication=True) as context:
        yield context
