# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine native SOURCE component roles; never an enrolled runtime manifest."""

import asyncio
import hashlib
import importlib
import json
import os
import re
from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import Index, MetaData, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from db.connection import Database
from process import provider_directory_generic_source_policy as source
from process.entity_address_cutover_contract import postgres_sqlstate
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_projection_stage import _copy_driver

_ORDINARY_TIGER_SELECTION_SQL = """SELECT NOT EXISTS(
    SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='hp_snapshot_retention' AND c.relname=ANY(ARRAY[
        'current_generation','generation','tiger_captured_epoch','tiger_snapshot_inheritance']))
    AND NOT EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='tiger' AND c.relname=ANY(ARRAY['zcta5','zip_state'])
    AND (c.relkind<>'r' OR c.relpersistence<>'p' OR c.relispartition
        OR EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid)))"""

_SOURCE_COMPONENT_BASELINES = None


@asynccontextmanager
async def source_component_templates():
    """Keep only closed migration databases; every case still provisions fresh custody."""
    baselines, registration = {}, pytest.MonkeyPatch()
    registration.setattr(f"{__name__}._SOURCE_COMPONENT_BASELINES", baselines)
    try:
        yield
    finally:
        registration.undo()
        for baseline in reversed(tuple(baselines.values())):
            await _drop_source_component_baseline(baseline)


async def _drop_source_component_baseline(baseline):
    admin = await asyncpg.connect(baseline.url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        if baseline.database_created:
            await admin.execute(f'DROP DATABASE IF EXISTS "{baseline.name}" WITH (FORCE)')
            assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", baseline.name)
            baseline.database_created = False
        if baseline.builder_created:
            await admin.execute(f'DROP ROLE "{baseline.builder}"')
            baseline.builder_created = False
        if baseline.owner_created:
            await admin.execute(f'DROP ROLE "{baseline.owner}"')
            baseline.owner_created = False
    finally:
        await admin.close()


async def _create_source_component_baseline(admin, baseline):
    """Register only newly created database and roles for exact cleanup."""
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_database WHERE datname=$1)", baseline.name)
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", baseline.builder)
    assert not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=$1)", baseline.owner)
    await admin.execute(f'CREATE DATABASE "{baseline.name}" TEMPLATE template0')
    baseline.database_created = True
    await admin.execute(
        f'CREATE ROLE "{baseline.owner}" NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
    )
    baseline.owner_created = True
    password = await admin.fetchval("SELECT quote_literal($1::text)", baseline.url.password)
    credential = " PASSWORD " + password if password is not None else ""
    await admin.execute(
        f'CREATE ROLE "{baseline.builder}" LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE '
        f"NOREPLICATION NOBYPASSRLS{credential}"
    )
    baseline.builder_created = True


async def _migrate_source_component_baseline(baseline, bootstrap, migration_prefixes):
    """Run the authentic Builder slice and reject role names embedded in definitions."""
    policy = SimpleNamespace(
        schema="mrf",
        owner_role=baseline.owner,
        publisher_role=baseline.owner,
        runtime_roles=(baseline.owner,),
        control_runtime_roles=(baseline.builder,),
    )
    engine = create_async_engine(baseline.url.set(database=baseline.name))
    try:
        with pytest.MonkeyPatch.context() as settings:
            settings.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            settings.setenv("DB_SCHEMA", "mrf")
            async with engine.begin() as connection:
                await connection.execute(text(f'CREATE SCHEMA mrf AUTHORIZATION "{baseline.owner}"'))
                await connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA mrf TO "{baseline.builder}"'))
            await bootstrap(engine, policy, migration_prefixes=migration_prefixes)
            async with engine.connect() as connection:
                assert not await connection.scalar(
                    text(
                        "SELECT EXISTS(SELECT FROM pg_proc WHERE pronamespace='mrf'::regnamespace "
                        "AND (position(:builder IN pg_get_functiondef(oid))>0 "
                        "OR position(:owner IN pg_get_functiondef(oid))>0))"
                    ),
                    {"builder": baseline.builder, "owner": baseline.owner},
                )
                await connection.rollback()
                connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
                await connection.exec_driver_sql("ANALYZE")
                await connection.exec_driver_sql("VACUUM (FREEZE)")
    finally:
        await engine.dispose()


async def _close_source_component_baseline(admin, baseline):
    """Require a disconnected, database-local template before preventing new connections."""
    for attempt in range(121):  # Autovacuum may outlive migration engine disposal.
        if not await admin.fetchval("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", baseline.name):
            break
        if attempt < 120:
            await asyncio.sleep(0.5)
    else:
        raise AssertionError("source template database still has connections")
    for role in (baseline.owner, baseline.builder):
        assert not await admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_database WHERE datdba=$1::regrole) "
            "OR EXISTS(SELECT FROM pg_tablespace WHERE spcowner=$1::regrole)",
            role,
        )
    await admin.execute(f'ALTER ROLE "{baseline.builder}" NOLOGIN')
    await admin.execute(f'ALTER DATABASE "{baseline.name}" ALLOW_CONNECTIONS false')


async def _build_source_component_baseline(baseline, bootstrap, migration_prefixes):
    """Create, migrate and close one immutable baseline without accepting Source custody."""
    admin = await asyncpg.connect(baseline.url.set(drivername="postgresql").render_as_string(hide_password=False))
    try:
        await _create_source_component_baseline(admin, baseline)
        await _migrate_source_component_baseline(baseline, bootstrap, migration_prefixes)
        await _close_source_component_baseline(admin, baseline)
    finally:
        await admin.close()


@asynccontextmanager
async def _source_component_database_url(migration_prefixes):
    from tests.cms_npd_admission_postgres_support import _database_url, _owned_database

    source_url = _database_url()
    baseline = None
    if _SOURCE_COMPONENT_BASELINES is not None and bootstrap_source_component is _ORIGINAL_SOURCE_BOOTSTRAP:
        prefixes = source_component_migrations() if migration_prefixes is None else migration_prefixes
        key = (source_url, prefixes, bootstrap_source_component)
        baseline = _SOURCE_COMPONENT_BASELINES.get(key)
        if baseline is None:
            token = uuid4().hex
            baseline = SimpleNamespace(
                url=source_url,
                name="hc_source_template_" + token,
                database_created=False,
                builder="source_template_builder_" + token[:16],
                builder_created=False,
                owner="source_template_owner_" + token[:16],
                owner_created=False,
            )
            _SOURCE_COMPONENT_BASELINES[key] = baseline
            try:
                await _build_source_component_baseline(baseline, bootstrap_source_component, prefixes)
            except BaseException:
                await _drop_source_component_baseline(baseline)
                del _SOURCE_COMPONENT_BASELINES[key]
                raise
    async with _owned_database(source_url, baseline.name if baseline is not None else "template0") as (url, _admin):
        yield url, baseline


def _component_producer_identity(policy):
    """Content-address synthetic component provenance, including its exact Source actors."""
    provenance = json.dumps(
        dict(
            contract="native-source-component.v1",
            schema=policy.schema,
            owner=policy.owner_role,
            publisher=policy.publisher_role,
            readers=policy.runtime_roles,
            controls=policy.control_runtime_roles,
            implementation_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        ),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha1(provenance).hexdigest(), "sha256:" + hashlib.sha256(
        b"component-image:" + provenance
    ).hexdigest()


async def verify_native_source_profile(connection, schema, **roles):
    """Native component roles only; no enrolled runtime policy is represented."""
    await storage_roles(connection, schema, **roles)
    assert await connection.fetchval("SELECT current_user=session_user")


class NativeSourceFactory:
    """Use real login/session/custody checks; no runtime manifest is represented."""

    def __init__(self, database, policy):
        self.database, self.policy = database, policy
        self._enrolled_policy = policy
        self._publication_engine = self._publication_sessions = None
        self._guard_engine = None
        self._observation_engine = None
        self._native_input_producer = None

    def _require_current_policy(self):
        assert self.policy is self._enrolled_policy
        if self._native_input_producer is not None:
            assert self.producer_identity == self._native_input_producer == _component_producer_identity(self.policy)
            assert self.native_tiger_input_fence == self._ordinary_tiger_input_fence

    def enroll_ordinary_native_inputs(self):
        """Issue only this component's content-addressed producer and ordinary-input capability."""
        self._require_current_policy()
        assert self._native_input_producer is None and not hasattr(self, "producer_identity")
        self.producer_identity = self._native_input_producer = _component_producer_identity(self.policy)
        self.native_tiger_input_fence = self._ordinary_tiger_input_fence

    async def _native_input_origin(self, session, driver):
        """Authenticate original physical Source custody before and after catalog observation."""
        if source._PUBLISHER.get() is None:
            return await self._observation_native_input_origin(session, driver)
        publisher = source.bound_publisher()
        assert publisher.connection is driver and publisher.policy is self.policy and publisher.factory is self
        assert isinstance(session, AsyncSession) and session.in_transaction() and not session.in_nested_transaction()
        assert driver.is_in_transaction() and not driver.is_closed()
        binding = self.database._transaction_binding()
        assert binding is not None and binding.session is session is source.source_copy_session()
        assert await _copy_driver(session) is driver
        actor_by_field = dict(
            await driver.fetchrow(
                "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
                "current_setting('transaction_read_only') AS readonly,"
                "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
                "AND locktype='virtualxid' AND granted) AS transaction"
            )
        )
        assert (
            actor_by_field["session"] == self.policy.publisher_role
            and actor_by_field["current"] == self.policy.owner_role
        )
        assert actor_by_field["transaction"] is not None
        return actor_by_field

    async def _observation_native_input_origin(self, session, driver):
        """Accept only the issued physical readonly Source observation, never a bare session."""
        from process import provider_directory_source_observation as observation

        directory = importlib.import_module("process.provider_directory_fhir")
        issued = observation.current_observation(directory)
        assert issued is not None and issued.database is self.database and issued.factory is self
        assert issued.policy is self.policy and issued.session is session and issued.connection is driver
        await observation._verify(directory, issued)
        origin = await _native_observation_identity(session, driver, self.policy)
        assert (origin["backend"], origin["transaction"]) == issued.origin
        assert observation.current_observation(directory) is issued
        return origin

    async def _ordinary_tiger_input_fence(self, session):
        """Prove no selected retained input before ordinary heaps retain their native guards."""
        from process.tiger_held_inputs import _source_selection_relations

        self._require_current_policy()
        assert self._native_input_producer is not None and source._FACTORY.get() is self
        task, driver = asyncio.current_task(), await _copy_driver(session)
        original = await self._native_input_origin(session, driver)
        assert _source_selection_relations() is None
        assert await driver.fetchval(_ORDINARY_TIGER_SELECTION_SQL) is True
        assert _source_selection_relations() is None
        self._require_current_policy()
        assert asyncio.current_task() is task and source._FACTORY.get() is self
        assert await self._native_input_origin(session, driver) == original
        return None

    async def publication_session_factory(self):
        """Select a dedicated pool using the existing genuine Publisher credentials."""
        self._require_current_policy()
        if self._publication_sessions is None:
            self._publication_engine = create_async_engine(self.database.engine.url, pool_size=1, max_overflow=0)
            self._publication_sessions = async_sessionmaker(self._publication_engine, expire_on_commit=False)
        return self._publication_sessions

    @asynccontextmanager
    async def bind_existing_transaction(self, database, session, *, isolation):
        """Authenticate an already-open selected Publisher session without owning its TX."""
        from tests.cms_npd_admission_postgres_support import _original_source_identity

        self._require_current_policy()
        assert database is self.database and isolation in {"read committed", "serializable"}
        assert source._FACTORY.get() is self
        assert session.in_transaction() and not session.in_nested_transaction()
        binding = database._transaction_binding()
        assert binding is not None and binding.session is session
        connection = await session.connection()
        assert connection.engine is not None and connection.engine in (self._publication_engine, self._guard_engine)
        assert connection.engine.url == database.engine.url
        assert await connection.get_isolation_level() == isolation.upper()
        driver = await _copy_driver(session)
        assert await driver.fetchval("SHOW transaction_read_only") == "off"
        assert await driver.fetchval("SHOW transaction_isolation") == isolation
        async with (
            source.bind_generic_source_publisher(
                driver, self.policy, verify_role_profile=verify_native_source_profile, transaction_isolation=isolation
            ),
            source.source_owner_transaction(database),
        ):
            identity = await _original_source_identity(self, session, isolation)
            assert identity[0] is driver
            yield session
            assert await _original_source_identity(self, session, isolation) == identity

    @asynccontextmanager
    async def checkpoint_guard(self, database):
        """Issue this component's genuine dedicated Publisher session and short transactions."""
        self._require_current_policy()
        assert database is self.database and source._FACTORY.get() is self
        assert database._transaction_binding() is None
        task, policy = asyncio.current_task(), self.policy
        if self._guard_engine is None:
            self._guard_engine = create_async_engine(
                database.engine.url, pool_size=2, max_overflow=0, isolation_level="READ COMMITTED"
            )
        async with self._guard_engine.connect() as connection:
            async with AsyncSession(bind=connection, expire_on_commit=False) as session, session.begin():
                await session.execute(text("SELECT 1"))
                driver = await _copy_driver(session)
                async with source.bind_generic_source_publisher(
                    driver,
                    policy,
                    verify_role_profile=verify_native_source_profile,
                    transaction_isolation="read committed",
                ):
                    backend = driver.get_server_pid()

            async def verify():
                self._require_current_policy()
                assert task is asyncio.current_task() and self.policy is policy and source._FACTORY.get() is self
                assert not connection.closed and not connection.invalidated and not driver.is_closed()
                assert (await connection.get_raw_connection()).driver_connection is driver
                assert driver.get_server_pid() == backend
                self._require_current_policy()
                assert task is asyncio.current_task() and self.policy is policy and source._FACTORY.get() is self

            @asynccontextmanager
            async def transaction():
                await verify()
                assert database._transaction_binding() is None and not connection.in_transaction()
                async with AsyncSession(bind=connection, expire_on_commit=False) as session, session.begin():
                    await session.execute(text("SELECT 1"))
                    async with (
                        database.bind_existing_session(session),
                        self.bind_existing_transaction(database, session, isolation="read committed"),
                    ):
                        yield database
                        await verify()
                assert database._transaction_binding() is None

            yield connection, transaction
            await verify()
            assert database._transaction_binding() is None and not connection.in_transaction()

    @asynccontextmanager
    async def observation_transaction(self, *, isolation="read committed"):
        """Observe on a separate genuine readonly Owner backend without binding a writer."""
        self._require_current_policy()
        assert isolation in {"read committed", "repeatable read", "serializable"}
        assert source._FACTORY.get() is self
        task, policy = asyncio.current_task(), self.policy
        original_binding, original_publisher = self.database._transaction_binding(), source._PUBLISHER.get()
        original_engine, original_url = self.database.engine, self.database.engine.url
        if self._observation_engine is None:
            self._observation_engine = create_async_engine(self.database.engine.url, pool_size=2, max_overflow=0)
        async with self._observation_engine.connect() as connection:
            async with AsyncSession(bind=connection, expire_on_commit=False) as session, session.begin():
                await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation + " READ ONLY"))
                driver = await _copy_driver(session)
                assert await driver.fetchval("SHOW transaction_isolation") == isolation
                await verify_native_source_profile(driver, policy.schema, **policy.role_arguments())
                await driver.execute('SET LOCAL ROLE "' + policy.owner_role + '"')
                original = await _native_observation_identity(session, driver, policy)
                self._require_current_policy()
                assert asyncio.current_task() is task and self.policy is policy and source._FACTORY.get() is self
                yield session
                self._require_current_policy()
                assert asyncio.current_task() is task and self.policy is policy and source._FACTORY.get() is self
                assert await _native_observation_identity(session, driver, policy) == original
                assert self.database.engine is original_engine and original_engine.url == original_url
                assert self.database._transaction_binding() is original_binding
                assert source._PUBLISHER.get() is original_publisher
                await driver.execute("RESET ROLE")
                await verify_native_source_profile(driver, policy.schema, **policy.role_arguments())
                self._require_current_policy()
                assert asyncio.current_task() is task and self.policy is policy and source._FACTORY.get() is self

    async def close_publication_pool(self):
        """Dispose only this component's selected pool before native role cleanup."""
        for attribute in ("_publication_engine", "_guard_engine", "_observation_engine"):
            engine = getattr(self, attribute)
            if engine is not None:
                assert engine.pool.checkedout() == 0
                await engine.dispose()
                setattr(self, attribute, None)
        self._publication_sessions = None

    async def source_callable_capability(self, connection, policy):
        from process.provider_directory_source_callable_custody import attest_source_callable_capability

        self._require_current_policy()
        assert policy is self.policy
        return await attest_source_callable_capability(
            connection, policy, migration_role=self.migration_role, namespace_owner_role=self.namespace_owner_role
        )

    async def cohort_snapshot_custody(self, connection, policy, *, cohort_id=None):
        from process.provider_directory_source_cohort_custody import capture_cohort_snapshot_custody

        self._require_current_policy()
        publisher = source.bound_publisher()
        assert publisher.connection is connection and publisher.policy is policy is self.policy
        receipt = await capture_cohort_snapshot_custody(
            connection,
            policy,
            migration_role=self.migration_role,
            namespace_owner_role=self.namespace_owner_role,
            cohort_id=cohort_id,
        )
        self._require_current_policy()
        assert source.bound_publisher() is publisher
        return receipt

    async def observe_source_callable_capability(self, connection, policy):
        from process import provider_directory_source_observation as observation
        from process.provider_directory_source_callable_custody import attest_source_callable_capability

        self._require_current_policy()
        issued = observation.current_observation(importlib.import_module("process.provider_directory_fhir"))
        assert issued is not None and issued.factory is self and issued.policy is policy is self.policy
        assert issued.connection is connection
        capability = await attest_source_callable_capability(
            connection, policy, migration_role=self.migration_role, namespace_owner_role=self.namespace_owner_role
        )
        self._require_current_policy()
        assert observation.current_observation(issued.directory) is issued
        return capability

    @asynccontextmanager
    async def transaction(self, database, *, isolation):
        assert database is self.database and isolation in {"read committed", "repeatable read", "serializable"}
        async with database.session_factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL " + isolation))
            if getattr(self, "readonly", False):
                await session.execute(text("SET TRANSACTION READ ONLY"))
            async with database.bind_existing_session(session):
                driver = await _copy_driver(session)
                async with (
                    source.bind_generic_source_publisher(
                        driver,
                        self.policy,
                        verify_role_profile=verify_native_source_profile,
                        transaction_isolation=isolation,
                    ),
                    source.source_owner_transaction(database),
                ):
                    yield session


async def _native_observation_identity(session, driver, policy):
    """Keep the exact readonly physical transaction and declared Source actors."""
    assert session.in_transaction() and not session.in_nested_transaction()
    assert driver.is_in_transaction() and not driver.is_closed()
    assert await _copy_driver(session) is driver
    actor_by_field = dict(
        await driver.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
            "current_setting('transaction_read_only') AS readonly,"
            "(SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid() "
            "AND locktype='virtualxid' AND granted) AS transaction"
        )
    )
    assert actor_by_field["session"] == policy.publisher_role and actor_by_field["current"] == policy.owner_role
    assert actor_by_field["readonly"] == "on" and actor_by_field["transaction"] is not None
    assert actor_by_field["backend"] == driver.get_server_pid()
    return actor_by_field


async def _create_native_roles(engine, policy, roles, created, retention_owner, retained_publisher):
    async with engine.begin() as connection:
        password_sql = await connection.scalar(
            text("SELECT quote_literal(CAST(:password AS text))"), {"password": engine.url.password}
        )
        for role in roles:
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:r)"), {"r": role}
            )
            login = "NOLOGIN" if role in (policy.owner_role, retention_owner) else "LOGIN"
            credential = " PASSWORD " + password_sql if login == "LOGIN" and password_sql is not None else ""
            await connection.exec_driver_sql(
                f'CREATE ROLE "{role}" {login} INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS{credential}'
            )
            created.append(role)
        await connection.execute(text(f'GRANT "{policy.owner_role}" TO "{policy.publisher_role}"'))
        await connection.execute(
            text(f'GRANT "{retention_owner}" TO "{retained_publisher}" WITH INHERIT TRUE, SET TRUE, ADMIN FALSE')
        )
        await connection.execute(
            text(f'GRANT "{policy.owner_role}" TO "{retained_publisher}" WITH INHERIT TRUE, SET FALSE, ADMIN FALSE')
        )
        await connection.execute(text(f'CREATE SCHEMA hp_snapshot_retention AUTHORIZATION "{retention_owner}"'))
        retention_oid = await connection.scalar(text("SELECT 'hp_snapshot_retention'::regnamespace::oid"))
        await connection.execute(text(f'ALTER SCHEMA "{policy.schema}" OWNER TO "{policy.owner_role}"'))
        for role in (policy.publisher_role, *policy.runtime_roles):
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{policy.schema}" TO "{role}"'))
        for role in policy.control_runtime_roles:
            await connection.execute(text(f'GRANT CREATE ON SCHEMA "{policy.schema}" TO "{role}"'))
        await connection.execute(text(f'GRANT ALL ON ALL TABLES IN SCHEMA "{policy.schema}" TO "{policy.owner_role}"'))
        await connection.execute(
            text(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA "{policy.schema}" TO "{policy.owner_role}"')
        )
        await connection.execute(
            text(
                f'GRANT SELECT ON ALL TABLES IN SCHEMA "{policy.schema}" TO "{policy.publisher_role}","{policy.runtime_roles[0]}"'
            )
        )
        dbname = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{dbname}" TO "{policy.publisher_role}"'))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{dbname}" TO "{retained_publisher}"'))
    return retention_oid


async def _drop_native_source_roles(connection, created, admin):
    """Remove created roles within the caller's ownership cleanup transaction."""
    for role in reversed(created):
        if not await connection.scalar(text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}):
            continue
        await connection.execute(text(f'REASSIGN OWNED BY "{role}" TO "{admin}"'))
        await connection.execute(text(f'DROP OWNED BY "{role}"'))
        await connection.execute(text(f'DROP ROLE "{role}"'))


_CANCEL_AUTOVACUUM_SQL = """SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE datname=current_database() AND backend_type='autovacuum worker'
AND pg_has_role(current_user,to_regrole('pg_signal_autovacuum_worker'),'USAGE')"""


async def _cleanup_native_source_component(engine, database, is_owned_reader, retention_oid, created, admin):
    """Retry only whole rolled-back ownership cleanup when native autovacuum deadlocks."""
    if database is not None:
        if not is_owned_reader:
            database._reader_database = None
        await database.disconnect()
    for attempt in range(3):
        try:
            async with engine.begin() as connection:
                # Fast bulk loads leave autovacuum mid-flight on this disposable database; its catalog
                # updates deadlock with REASSIGN OWNED, so stop the workers first when allowed to.
                await connection.execute(text(_CANCEL_AUTOVACUUM_SQL))
                if retention_oid is not None:
                    assert (
                        await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention')::oid"))
                        == retention_oid
                    )
                    await connection.execute(text("DROP SCHEMA hp_snapshot_retention RESTRICT"))
                await _drop_native_source_roles(connection, created, admin)
            return
        except DBAPIError as error:
            if postgres_sqlstate(error) != "40P01" or attempt == 2:
                raise
            await asyncio.sleep(0.05 * (attempt + 1))


def _is_component_test_database(name):
    """Retain legacy test names and the existing strict UUID entities fixture contract."""
    return isinstance(name, str) and (
        "test" in name.lower() or re.fullmatch(r"hc_directory_entities_[0-9a-f]{32}", name) is not None
    )


@asynccontextmanager
async def _native_component_transaction(database, factory):
    """Keep nested settings on issued readonly or retained backends before opening a Source writer."""
    from process import provider_directory_source_observation as observation

    directory = importlib.import_module("process.provider_directory_fhir")
    observed = observation.current_observation(directory)
    if observed is not None:
        assert observed.database is database and observed.factory is factory
        async with observation.observation_transaction(directory, isolation=observed.isolation) as original:
            async with Database.transaction(database) as session:
                assert session is original
                yield session
        return
    from process import provider_directory_cms_nonprofile_observation as retained
    from process import provider_directory_cms_preparation as preparation

    scope = retained.active_observations(admission=preparation._ACTIVE.get())
    if scope is not None:
        assert source._FACTORY.get() is factory and scope.factory is factory and scope.database is database
        factory._require_current_policy()
        assert factory.policy is scope.source_input._receipt()["policy"]
        await scope.verify_transaction()
        binding = database._transaction_binding()
        assert binding is scope.binding and binding.session is scope.session
        async with Database.transaction(database) as session:
            assert session is scope.session and database._transaction_binding() is binding
            yield session
            assert session is scope.session and database._transaction_binding() is binding
        assert database._transaction_binding() is binding and binding.session is scope.session
        await scope.verify_transaction()
        assert source._FACTORY.get() is factory
        factory._require_current_policy()
        assert factory.policy is scope.source_input._receipt()["policy"]
        assert retained.active_observations(admission=preparation._ACTIVE.get()) is scope
        return
    current = source.source_transaction_available()
    isolation = current.transaction_isolation if current is not None else "repeatable read"
    async with source.source_transaction(database, isolation=isolation) as session:
        yield session


def _source_publisher_engine(url, publisher, monkeypatch, *, pool_size=None):
    """Declare an explicit fixture pool exactly; preserve ordinary fixture defaults."""
    pool_options_by_name = {}
    if pool_size is not None:
        assert type(pool_size) is int and pool_size > 0
        pool_options_by_name = {"pool_size": pool_size, "max_overflow": 0}
        monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", str(pool_size))
    return create_async_engine(url.set(username=publisher), **pool_options_by_name)


@asynccontextmanager
async def native_source_component(
    engine, schema, monkeypatch, *, reader=None, legacy_builder=False, bind_factory=True, publisher_pool_size=None
):
    """Close only a caller-owned UUID schema with exact native role/custody checks.

    Full enrolled operator/profile acceptance belongs to the separate runtime
    integration fixture; this helper neither invents nor mocks that manifest.
    """
    assert len(schema) < 64 and schema.replace("_", "").isalnum()
    token = uuid4().hex[:16]
    owner, publisher = "source_owner_" + token, "source_publisher_" + token
    is_owned_reader = reader is None
    reader = reader or "source_reader_" + token
    builder = ("source_builder_" + token,) if legacy_builder else ()
    retention_owner, retained_publisher = "retention_owner_" + token, "retained_publisher_" + token
    roles = (owner, publisher, *((reader,) if is_owned_reader else ()), *builder, retention_owner, retained_publisher)
    policy = source.GenericSourcePolicy(schema, owner, publisher, (reader, *builder), builder)
    created, retention_oid, database = [], None, None
    async with engine.connect() as connection:
        admin = await connection.scalar(text("SELECT session_user"))
        assert _is_component_test_database(await connection.scalar(text("SELECT current_database()")))
        assert await connection.scalar(text("SELECT to_regnamespace('hp_snapshot_retention')")) is None
        await connection.rollback()
    try:
        retention_oid = await _create_native_roles(engine, policy, roles, created, retention_owner, retained_publisher)
        publisher_engine = _source_publisher_engine(engine.url, publisher, monkeypatch, pool_size=publisher_pool_size)
        database = Database(
            engine=publisher_engine, session_factory=async_sessionmaker(publisher_engine, expire_on_commit=False)
        )
        database._database_override = engine.url.database
        database._database_name = publisher_engine.url.database
        factory = NativeSourceFactory(database, policy)

        monkeypatch.setattr(database, "transaction", partial(_native_component_transaction, database, factory))
        async with AsyncExitStack() as contexts:
            contexts.push_async_callback(factory.close_publication_pool)
            if bind_factory:
                await contexts.enter_async_context(source.bind_generic_source_transaction_factory(factory))
            yield SimpleNamespace(
                database=database,
                factory=factory,
                owner=owner,
                publisher=publisher,
                reader=reader,
                policy=policy,
                admin_engine=engine,
                builder=builder[0] if builder else None,
                retention_owner=retention_owner,
                retained_publisher=retained_publisher,
            )
    finally:
        await _cleanup_native_source_component(engine, database, is_owned_reader, retention_oid, created, admin)


def source_component_migrations():
    """The authentic CMS dependency slice, not a complete Alembic upgrade."""
    from tests.cms_npd_admission_postgres_support import MIGRATION_PREFIXES

    prefixes = list(MIGRATION_PREFIXES)
    insertion = prefixes.index("20260929010000")
    prefixes[insertion:insertion] = ("20260914130000", "20260920100000", "20260929000000")
    prefixes.insert(prefixes.index("20260930070000"), "20260930060000")
    prefixes.insert(prefixes.index("20261001100000"), "20260929040000")
    return (*prefixes, "20261005100000", "20261005120000", "20261005130000", "20261015015000")


def legacy_source_component_migrations():
    """Keep authentic Source custody without the later composite publication contract."""
    composite_prefixes = {"20260930100000", "20260930110000", "20260930130000", "20260930140000"}
    prefixes = tuple(prefix for prefix in source_component_migrations() if prefix not in composite_prefixes)
    return (*prefixes, "20261022120000")


async def bootstrap_source_component(engine, policy, *, migration_prefixes=None):
    """Create importer roots and run actual migrations as their genuine legacy Builder."""
    from db.models import CMSDoctorEducation, DoctorClinicianAddress
    from tests.cms_npd_admission_postgres_support import _run_migrations
    from tests.test_provider_directory_cms_serving_receipt_postgres import _migration

    assert len(policy.control_runtime_roles) == 1
    builder = policy.control_runtime_roles[0]
    async with engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(text(f'GRANT CREATE ON DATABASE "{database}" TO "{builder}"'))
    historical = create_async_engine(engine.url.set(username=builder))
    try:
        async with historical.begin() as connection:
            for model in (DoctorClinicianAddress, CMSDoctorEducation):
                table = model.__table__.to_metadata(MetaData(), schema=policy.schema)
                assert (
                    await connection.scalar(
                        text("SELECT to_regclass(:name)"), {"name": f"{policy.schema}.{table.name}"}
                    )
                    is None
                )
                await connection.run_sync(lambda sync: table.create(sync, checkfirst=False))
                for spec in getattr(model, "__my_additional_indexes__", ()):
                    index = Index(
                        str(table.name) + "_idx_" + spec["name"], *(table.c[name] for name in spec["index_elements"])
                    )
                    await connection.run_sync(lambda sync: index.create(sync, checkfirst=False))
        # The historical slice owns its transactions and concurrent-index blocks.
        async with historical.connect() as connection:
            await connection.run_sync(
                _run_migrations, source_component_migrations() if migration_prefixes is None else migration_prefixes
            )
        async with historical.begin() as connection:
            await connection.execute(text(_migration("20261019130000")._counter_sql(policy.schema)))
    finally:
        await historical.dispose()
        async with engine.begin() as connection:
            await connection.execute(text(f'REVOKE CREATE ON DATABASE "{database}" FROM "{builder}"'))
    async with engine.begin() as connection:
        await connection.execute(text(f'GRANT ALL ON ALL TABLES IN SCHEMA "{policy.schema}" TO "{policy.owner_role}"'))
        for role in (policy.publisher_role, *policy.runtime_roles):
            await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{policy.schema}" TO "{role}"'))


_ORIGINAL_SOURCE_BOOTSTRAP = bootstrap_source_component


async def _source_guard_preimage(driver, provision, schema):
    roots = [schema + "." + root for root in (*provision.PAYLOAD_ROOTS, *provision.CONTROL_ROOTS)]
    return await driver.fetchval(
        "WITH RECURSIVE roots(oid) AS (SELECT to_regclass(name)::oid FROM unnest($1::text[]) n(name) UNION "
        "SELECT (value->>'original_oid')::oid FROM "
        + schema
        + ".pd_dataset_migration_plan,jsonb_each(plan->'tables')),"
        "family(oid) AS (SELECT oid FROM roots UNION SELECT i.inhrelid FROM pg_inherits i JOIN family f ON i.inhparent=f.oid) "
        "SELECT jsonb_build_object('relations',(SELECT jsonb_agg(to_jsonb(c) ORDER BY oid) FROM pg_class c "
        "WHERE oid IN(SELECT oid FROM family)),'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY oid) "
        "FROM pg_trigger t WHERE tgrelid IN(SELECT oid FROM family)),'constraints',(SELECT jsonb_agg(to_jsonb(k) ORDER BY oid) "
        "FROM pg_constraint k WHERE conrelid IN(SELECT oid FROM family)),'functions',(SELECT jsonb_agg("
        "jsonb_build_array(p.oid,p.proowner,p.proacl,pg_get_functiondef(p.oid)) ORDER BY p.oid) FROM pg_proc p "
        "WHERE p.oid IN(SELECT tgfoid FROM pg_trigger WHERE tgrelid IN(SELECT oid FROM family))))",
        roots,
    )


async def provision_source_component(engine, policy):
    """Real native writer/legacy custody callbacks, without an enrolled-manifest claim."""
    from process import provider_directory_generic_source_provision as provision

    async with async_sessionmaker(engine)() as session, session.begin():
        await session.execute(text("SELECT current_user=session_user"))
        driver = await _copy_driver(session)
        expected = await _source_guard_preimage(driver, provision, policy.schema)

        async def legacy(connection, actual_policy, family):
            assert actual_policy == policy and family
            assert await _source_guard_preimage(connection, provision, policy.schema) == expected

        async def writers(connection, actual_policy):
            assert actual_policy == policy
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT FROM pg_class c,pg_roles r WHERE c.relnamespace=$1::regnamespace "
                "AND c.relname=ANY($2::text[]) AND r.rolname=ANY($3::text[]) AND "
                "(has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') OR "
                "has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES')))",
                policy.schema,
                list((*provision.PAYLOAD_ROOTS, *provision.MINT_ROOTS)),
                list(policy.runtime_roles),
            )

        return await provision.provision_generic_source_storage(
            driver,
            policy,
            verify_role_profile=verify_native_source_profile,
            verify_writer_closure=writers,
            verify_legacy_guards=legacy,
        )


@asynccontextmanager
async def source_component_database(
    monkeypatch, *, bind_factory=True, before_provision=None, migration_prefixes=None, publisher_pool_size=None
):
    """Own a real CMS/Doctors component schema, roles and retained test files.

    The authentic dependency slice is deliberately not a whole-DAG migration or
    a full enrolled-runtime policy proof. Existing admission fixtures are unchanged.
    """
    if migration_prefixes is not None and migration_prefixes != legacy_source_component_migrations():
        from tests.cms_registry_complete_publication_postgres_support import source_component_native_migrations
        from tests.provider_directory_dataset_rehydrate_postgres_support import _migrations as rehydration_migrations

        assert type(migration_prefixes) is tuple and migration_prefixes in {
            source_component_native_migrations(),
            rehydration_migrations(),
        }
    artifact_parent = Path(
        os.getenv("HLTHPRT_CMS_NPD_ADMISSION_TEST_ARTIFACT_ROOT")
        or Path(__file__).resolve().parents[1] / ".pytest_cache"
    )
    artifact_parent.mkdir(parents=True, exist_ok=True)
    async with _source_component_database_url(migration_prefixes) as (url, baseline):
        engine = create_async_engine(url)
        try:
            if baseline is None:
                async with engine.begin() as connection:
                    await connection.execute(text("CREATE SCHEMA mrf"))
            for name, setting_value in {
                "HOST": url.host,
                "PORT": url.port,
                "DATABASE": url.database,
                "USER": url.username,
                "PASSWORD": url.password or "",
                "SCHEMA": "mrf",
            }.items():
                monkeypatch.setenv("HLTHPRT_DB_" + name, str(setting_value))
            monkeypatch.setenv("DB_SCHEMA", "mrf")
            monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
            publisher_options = {} if publisher_pool_size is None else {"publisher_pool_size": publisher_pool_size}
            async with native_source_component(
                engine, "mrf", monkeypatch, legacy_builder=True, bind_factory=bind_factory, **publisher_options
            ) as component:
                await _prepare_component_inputs(
                    engine, component, before_provision, migration_prefixes=migration_prefixes, baseline=baseline
                )
                monkeypatch.setenv("HLTHPRT_DB_USER", component.publisher)
                monkeypatch.setenv("HLTHPRT_DB_READER_USER", component.reader)
                monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", url.password or "")
                await component.database._connect_reader()
                with TemporaryDirectory(prefix="source-component-", dir=artifact_parent) as artifacts:
                    component.artifacts = Path(artifacts)
                    monkeypatch.setenv("HLTHPRT_CMS_DOCTORS_ARTIFACT_ROOT", artifacts)
                    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", artifacts)
                    yield component
        finally:
            await engine.dispose()


async def _prepare_component_inputs(engine, component, before_provision, *, migration_prefixes=None, baseline=None):
    """Prepare historical native fixture inputs before the actual protected Source cut."""
    if baseline is not None:
        async with engine.begin() as connection:
            for previous, current in (
                (baseline.builder, component.policy.control_runtime_roles[0]),
                (baseline.owner, component.policy.owner_role),
            ):
                await connection.execute(text(f'REASSIGN OWNED BY "{previous}" TO "{current}"'))
                await connection.execute(text(f'DROP OWNED BY "{previous}"'))
    elif migration_prefixes is None:
        await bootstrap_source_component(engine, component.policy)
    else:
        await bootstrap_source_component(engine, component.policy, migration_prefixes=migration_prefixes)
    if before_provision is not None:
        assert callable(before_provision)
        await before_provision(component)
    await provision_source_component(engine, component.policy)
