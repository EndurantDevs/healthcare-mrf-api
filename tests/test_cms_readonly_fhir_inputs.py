# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Readonly CMS FHIR capture retains issued observation and callable custody."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from db.connection import Database
from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_nonprofile_observation as retained
from process import provider_directory_cms_preparation as preparation
from process import provider_directory_generic_source_provision as provision
from process import provider_directory_source_observation as observation
from tests import provider_directory_source_postgres_support as native_source
from tests.test_provider_directory_source_observation import case as case


@pytest.mark.parametrize("changed_database", [False, True])
async def test_native_component_connect_retains_only_matching_injected_pool(monkeypatch, changed_database):
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    @asynccontextmanager
    async def admin_connection():
        yield SimpleNamespace(
            scalar=AsyncMock(side_effect=["fixture_admin", "source_test_database", None]), rollback=AsyncMock()
        )

    admin = SimpleNamespace(
        url=make_url("postgresql+asyncpg://fixture_admin@127.0.0.1/source_test_database"), connect=admin_connection
    )
    monkeypatch.setattr(native_source, "_create_native_roles", AsyncMock(return_value=71))
    monkeypatch.setattr(native_source, "_cleanup_native_source_component", AsyncMock())
    async with native_source.native_source_component(admin, "synthetic", monkeypatch, bind_factory=False) as component:
        database = component.database
        engine, sessions = database.engine, database.session_factory
        disconnect = AsyncMock(side_effect=RuntimeError("fixture_reconnect_forbidden"))
        monkeypatch.setattr(database, "disconnect", disconnect)
        for name, setting_value in {
            "USER": "foreign_login",
            "PASSWORD": "foreign_password",
            "HOST": "unreachable.invalid",
            "DATABASE": "foreign_test_database",
        }.items():
            monkeypatch.setenv("HLTHPRT_DB_" + name, setting_value)
        try:
            if changed_database:
                database._database_override = "foreign_test_database"
                with pytest.raises(RuntimeError, match="fixture_reconnect_forbidden"):
                    await database.connect()
                disconnect.assert_awaited_once_with()
            else:
                await database.connect()
                disconnect.assert_not_awaited()
                assert database._database_name == engine.url.database == "source_test_database"
            assert database.engine is engine and database.session_factory is sessions
            assert component.factory.database is database and component.factory.policy is component.policy
        finally:
            await engine.dispose()


@pytest.fixture
async def readonly_case(case, monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "db", case.directory.db)
    monkeypatch.setattr(directory, "_schema", case.directory._schema)
    case.directory = directory
    case.factory.observe_source_callable_capability = AsyncMock(return_value=(700, 800))
    case.proof = {"protected": "fixture"}
    case.capture = AsyncMock(return_value=case.proof)
    monkeypatch.setattr(provision, "capture_protected_fhir_input", case.capture)
    return case


async def test_cms_native_capture_uses_readonly_observation_without_publisher(readonly_case):
    case = readonly_case
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        assert await inputs._capture_bound_fhir(session, "synthetic") is case.proof
        case.factory.observe_source_callable_capability.assert_awaited_once_with(
            case.factory.driver, case.factory.policy
        )
        case.capture.assert_awaited_once_with(case.factory.driver, source_policy=case.factory.policy)
        assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None


async def test_native_component_settings_borrow_the_issued_observation(readonly_case, monkeypatch):
    from tests import provider_directory_source_postgres_support as native_source

    case = readonly_case
    borrowed_sessions = []

    @asynccontextmanager
    async def savepoint(database):
        assert database is case.directory.db
        issued = observation.current_observation(case.directory)
        assert issued is not None and issued.factory is case.factory
        borrowed_sessions.append(issued.session)
        yield issued.session

    monkeypatch.setattr(native_source.Database, "transaction", savepoint)
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as original:
        async with native_source._native_component_transaction(case.directory.db, case.factory) as nested:
            assert nested is original
            assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None
        assert borrowed_sessions == [original]
        assert observation.current_observation(case.directory).session is original


@pytest.mark.parametrize("change", ["session", "schema"])
async def test_substituted_capture_rejected_before_callable_or_catalog_reads(readonly_case, change):
    case = readonly_case
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        before = len(case.factory.driver.calls)
        with pytest.raises(RuntimeError, match="observation_connection_changed"):
            await inputs._capture_bound_fhir(
                object() if change == "session" else session, "other" if change == "schema" else "synthetic"
            )
        assert len(case.factory.driver.calls) == before
    case.factory.observe_source_callable_capability.assert_not_awaited()
    case.capture.assert_not_awaited()


async def test_child_task_cannot_borrow_readonly_cms_capture(readonly_case):
    case = readonly_case
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        before = len(case.factory.driver.calls)
        with pytest.raises(RuntimeError, match="observation_changed"):
            await asyncio.create_task(inputs._capture_bound_fhir(session, "synthetic"))
        assert len(case.factory.driver.calls) == before
    case.factory.observe_source_callable_capability.assert_not_awaited()
    case.capture.assert_not_awaited()


@pytest.mark.parametrize("capability", [None, [700], (True,), (0,), (2**32,), (700, 700)])
async def test_invalid_callable_capability_never_reads_fhir_catalog(readonly_case, capability):
    case = readonly_case
    case.factory.observe_source_callable_capability.return_value = capability
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        with pytest.raises(RuntimeError, match="observation_callable_capability_changed"):
            await inputs._capture_bound_fhir(session, "synthetic")
    case.capture.assert_not_awaited()


@pytest.mark.parametrize("phase", ["callable", "capture"])
async def test_native_transaction_change_during_capture_refuses_proof(readonly_case, phase):
    case = readonly_case

    async def change(*args, **kwargs):
        case.factory.driver.actor_by_field["transaction"] = "7/92"
        return (700,) if phase == "callable" else case.proof

    callback = case.factory.observe_source_callable_capability if phase == "callable" else case.capture
    callback.side_effect = change
    with pytest.raises(ExceptionGroup, match="source_observation_and_verification_failed") as caught:
        async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
            await inputs._capture_bound_fhir(session, "synthetic")
    assert len(caught.value.exceptions) == 2
    assert all("observation_transaction_changed" in str(error) for error in caught.value.exceptions)
    if phase == "callable":
        case.capture.assert_not_awaited()


async def test_unavailable_callable_observer_fails_closed(readonly_case):
    case = readonly_case
    case.factory.observe_source_callable_capability = None
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        with pytest.raises(RuntimeError, match="observation_callable_factory_required"):
            await inputs._capture_bound_fhir(session, "synthetic")
    case.capture.assert_not_awaited()


async def test_absent_protected_fhir_proof_cannot_be_admitted(readonly_case):
    case = readonly_case
    case.capture.return_value = None
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        with pytest.raises(RuntimeError, match="observation_fhir_inputs_changed"):
            await inputs._capture_bound_fhir(session, "synthetic")


async def test_dependency_session_issues_readonly_fhir_capture(readonly_case, monkeypatch):
    from process import entity_address_source_preparation as preparation

    case = readonly_case
    admission = object()

    def original_factory(actual, input_hash):
        assert actual is admission and input_hash == "signed-native-input"
        return case.factory

    monkeypatch.setattr(preparation, "_active_factory", original_factory)
    async with preparation.source_dependency_session(case.directory.db, admission, "signed-native-input") as session:
        issued = observation.current_observation(case.directory)
        assert issued.session is session is case.factory.session
        assert issued.database is case.directory.db and issued.factory is case.factory
        assert await inputs._capture_bound_fhir(session, "synthetic") is case.proof
        assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None
    assert observation.current_observation(case.directory) is None
    assert case.directory.db._transaction_binding() is None
    assert case.directory.db.events == ["bind", "unbind"]


@pytest.mark.parametrize("owned", [True, False])
async def test_archive_owner_prerequisite_uses_issued_readonly_observation(readonly_case, monkeypatch, owned):
    from process import provider_directory_cms_archive as archive

    case = readonly_case
    monkeypatch.setattr(archive, "_copy_driver", AsyncMock(return_value=case.factory.driver))
    case.factory.driver.fetchval = AsyncMock(return_value=owned)
    async with observation.observation_transaction(case.directory, isolation="repeatable read") as session:
        if owned:
            await archive.require_source_archive_owner(session, "synthetic")
        else:
            with pytest.raises(RuntimeError, match="cms_archive_source_owner_required"):
                await archive.require_source_archive_owner(session, "synthetic")
        assert observation.current_observation(case.directory).session is session
        assert observation.source._PUBLISHER.get() is None and observation.source._OWNER.get() is None
        assert case.factory.driver.fetchval.await_args.args[1:] == (
            '"synthetic"."address_archive_v2"',
            "owner",
            "publisher",
        )


async def test_archive_owner_prerequisite_preserves_legacy_capture(monkeypatch):
    from process import provider_directory_cms_archive as archive

    monkeypatch.setattr(archive.source, "is_source_storage_required", lambda: False)
    driver = AsyncMock(side_effect=AssertionError("legacy owner capture is not a Source prerequisite"))
    monkeypatch.setattr(archive, "_copy_driver", driver)
    await archive.require_source_archive_owner(object(), "synthetic")
    driver.assert_not_awaited()


async def test_archive_owner_prerequisite_precedes_native_signing_capture(monkeypatch):
    from process import provider_directory_cms_archive as archive

    require = AsyncMock(side_effect=RuntimeError("cms_archive_source_owner_required"))
    capture = AsyncMock()
    monkeypatch.setattr(archive, "require_source_archive_owner", require)
    monkeypatch.setattr(inputs, "_capture_selected_input_relations", capture)
    session = object()
    with pytest.raises(RuntimeError, match="cms_archive_source_owner_required"):
        await inputs.capture_native_address_input_fence(session, "synthetic")
    require.assert_awaited_once_with(session, "synthetic")
    capture.assert_not_awaited()


async def test_archive_owner_prerequisite_rejects_foreign_session_before_query(readonly_case, monkeypatch):
    from process import provider_directory_cms_archive as archive

    case = readonly_case
    monkeypatch.setattr(archive, "_copy_driver", AsyncMock(return_value=case.factory.driver))
    case.factory.driver.fetchval = AsyncMock(return_value=True)
    async with observation.observation_transaction(case.directory, isolation="repeatable read"):
        with pytest.raises(RuntimeError, match="cms_archive_source_binding_changed"):
            await archive.require_source_archive_owner(object(), "synthetic")
        case.factory.driver.fetchval.assert_not_awaited()


@pytest.fixture
def retained_settings_case(monkeypatch):
    """Open real session/savepoints in the test task; native authority is checked separately."""

    @asynccontextmanager
    async def opened():
        database = Database()
        database._database_override = "retained_test"
        policy = native_source.source.GenericSourcePolicy("synthetic", "owner", "publisher", ("reader",))
        factory = native_source.NativeSourceFactory(database, policy)
        engine = create_async_engine("postgresql+asyncpg://reader@127.0.0.1/retained_test")
        directory = importlib.import_module("process.provider_directory_fhir")
        monkeypatch.setattr(directory, "db", database)
        admission, driver = object(), object()
        token = preparation._ACTIVE.set(admission)
        attempts = []
        try:
            async with AsyncSession(engine) as session, session.begin():
                async with (
                    database.bind_existing_session(session),
                    native_source.source.bind_generic_source_transaction_factory(factory),
                ):
                    binding = database._transaction_binding()
                    scope = SimpleNamespace(
                        database=database, factory=factory, session=session, binding=binding, driver=driver
                    )
                    scope.source_input = SimpleNamespace(_receipt=lambda: {"policy": policy})
                    task = asyncio.current_task()
                    original_active = retained.active_observations

                    async def verify():
                        assert asyncio.current_task() is task and scope.driver is driver
                        assert database._transaction_binding() is binding and binding.session is session
                        assert not session.in_nested_transaction()

                    scope.verify_transaction = AsyncMock(side_effect=verify)

                    def active(*, admission):
                        assert admission is preparation._ACTIVE.get() is original_admission
                        return scope

                    @asynccontextmanager
                    async def ordinary_source(database, **kwargs):
                        attempts.append(database)
                        async with database.bind_existing_session(session):
                            yield session

                    original_admission = admission
                    monkeypatch.setattr(retained, "active_observations", active)
                    monkeypatch.setattr(factory, "transaction", ordinary_source)
                    case = SimpleNamespace(database=database, factory=factory, attempts=attempts)
                    case.session, case.binding, case.scope = session, binding, scope
                    case.original_active = original_active
                    yield case
        finally:
            preparation._ACTIVE.reset(token)
            await engine.dispose()

    return opened


@pytest.mark.parametrize("outcome", ["success", "failure", "cancel"])
async def test_retained_component_settings_borrow_original_savepoint(retained_settings_case, outcome):
    async with retained_settings_case() as case:
        error = asyncio.CancelledError() if outcome == "cancel" else RuntimeError("original_body_failure")
        caught = None
        try:
            async with native_source._native_component_transaction(case.database, case.factory) as session:
                assert session is case.session and case.database._transaction_binding() is case.binding
                assert session.in_nested_transaction()
                if outcome != "success":
                    raise error
        except (RuntimeError, asyncio.CancelledError) as failure:
            caught = failure
        assert caught is (None if outcome == "success" else error)
        assert case.session.in_transaction() and not case.session.in_nested_transaction()
        assert case.database._transaction_binding() is case.binding and case.attempts == []
        assert case.scope.verify_transaction.await_count == (2 if outcome == "success" else 1)


@pytest.mark.parametrize("field", ["database", "factory", "session", "binding", "policy"])
async def test_retained_component_settings_refuse_substituted_scope(retained_settings_case, field):
    async with retained_settings_case() as case:
        if field == "policy":
            case.scope.source_input = SimpleNamespace(_receipt=lambda: {"policy": object()})
        else:
            setattr(case.scope, field, object())
        with pytest.raises(AssertionError):
            async with native_source._native_component_transaction(case.database, case.factory):
                pytest.fail("substituted scope reached a savepoint body")
        assert not case.session.in_nested_transaction() and case.attempts == []


@pytest.mark.parametrize("field", ["factory", "driver", "policy", "admission"])
async def test_retained_component_settings_verify_after_savepoint_release(retained_settings_case, field):
    async with retained_settings_case() as case:
        context = native_source.source._FACTORY if field == "factory" else preparation._ACTIVE
        mutation = {
            "factory": lambda: context.set(object()),
            "admission": lambda: context.set(object()),
            "policy": lambda: setattr(case.factory, "policy", object()),
            "driver": lambda: setattr(case.scope, "driver", object()),
        }[field]
        token = None
        try:
            with pytest.raises(AssertionError):
                async with native_source._native_component_transaction(case.database, case.factory):
                    token = mutation()
            assert case.session.in_transaction() and not case.session.in_nested_transaction()
            assert case.attempts == []
        finally:
            if token is not None:
                context.reset(token)


async def test_arbitrary_bound_session_still_uses_original_source_refusal(retained_settings_case, monkeypatch):
    async with retained_settings_case() as case:
        monkeypatch.setattr(retained, "active_observations", case.original_active)
        with pytest.raises(
            RuntimeError, match="cannot bind an existing session while a database transaction is already bound"
        ):
            async with native_source._native_component_transaction(case.database, case.factory):
                pytest.fail("unissued bound session was borrowed")
        assert case.attempts == [case.database]
        assert case.session.in_transaction() and not case.session.in_nested_transaction()


async def test_unissued_retained_scope_cannot_borrow_component_session(retained_settings_case, monkeypatch):
    async with retained_settings_case() as case:
        monkeypatch.setattr(retained, "active_observations", case.original_active)
        unissued = object.__new__(retained._RetainedObservations)
        token = retained._ACTIVE.set(unissued)
        try:
            with pytest.raises(RuntimeError, match="provider_directory_nonprofile_observation_changed"):
                async with native_source._native_component_transaction(case.database, case.factory):
                    pytest.fail("unissued scope was borrowed")
        finally:
            retained._ACTIVE.reset(token)
        assert case.attempts == [] and not case.session.in_nested_transaction()


async def test_child_task_cannot_borrow_retained_component_session(retained_settings_case):
    async with retained_settings_case() as case:

        async def child():
            async with native_source._native_component_transaction(case.database, case.factory):
                pytest.fail("child task borrowed retained session")

        with pytest.raises(AssertionError):
            await asyncio.create_task(child())
        assert case.attempts == [] and not case.session.in_nested_transaction()
