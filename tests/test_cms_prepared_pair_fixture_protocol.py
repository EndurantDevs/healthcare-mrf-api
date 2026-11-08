# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixture control-flow checks only; genuine native actor/ACL proof is separate."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as policy
from process import registry_cms_source_notifications as notifications
from process.network_custom_address_source import NetworkCustomAddressSourceError
from process.network_registry_cms_source_attempt import RegistryCMSSourceAttempt
from tests import cms_npd_admission_postgres_support as support
from tests import test_network_registry_cms_prepared_pair_postgres as fixture
from tests.test_cms_registry_source_pair_slot import _prepared, _row


@pytest.fixture(autouse=True)
def no_database_access(monkeypatch):
    def forbidden():
        raise AssertionError("native database setup reached by a pure protocol test")

    monkeypatch.setattr(support, "_database_url", forbidden)


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "read committed"])
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_original_source_scope_closes_after_success_failure_and_cancel(monkeypatch, isolation, failure):
    """The actual support helper binds only the existing factory and cleans every exit."""
    events = []
    binding = SimpleNamespace(session=object())
    component = SimpleNamespace(admin_engine=object(), factory=SimpleNamespace(transaction=lambda: None))
    component.database = SimpleNamespace(_transaction_binding=lambda: binding if events[-1] == "entered" else None)
    database = SimpleNamespace(engine=component.admin_engine, _source_component=component)

    @asynccontextmanager
    async def transaction(actual, *, isolation):
        assert actual is component.database and policy._FACTORY.get() is component.factory
        events.append("entered")
        try:
            yield binding.session
        finally:
            events.append("closed")

    identity = AsyncMock(return_value=(object(), 7, "42"))
    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(support, "_original_source_identity", identity)
    if failure:
        with pytest.raises(failure):
            async with support.original_source_transaction(database, isolation=isolation):
                raise failure("synthetic interruption")
    else:
        async with support.original_source_transaction(database, isolation=isolation) as session:
            assert session is binding.session and policy._FACTORY.get() is component.factory
    assert events == ["entered", "closed"]
    assert not policy.is_source_storage_required()
    assert identity.await_count == (1 if failure else 2)


@pytest.mark.asyncio
async def test_original_source_identity_delegates_actual_owner_and_session_checks(monkeypatch):
    """No administrator session can replace the passed original owner session."""
    session = object()
    driver = SimpleNamespace(fetchval=AsyncMock(side_effect=["repeatable read", "42"]), get_server_pid=lambda: 7)
    component = SimpleNamespace(policy=SimpleNamespace(schema="source_schema"))
    component.database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    monkeypatch.setattr(fixture.serving, "require_registry_receipt_owner", AsyncMock(return_value=driver))
    monkeypatch.setattr(policy, "bound_publisher", lambda: SimpleNamespace(policy=component.policy))
    monkeypatch.setattr(policy, "source_copy_session", lambda: session)
    assert await support._original_source_identity(component, session, "repeatable read") == (driver, 7, "42")
    fixture.serving.require_registry_receipt_owner.assert_awaited_once_with(session, "source_schema")
    fixture.serving.require_registry_receipt_owner.side_effect = ValueError("original_session_required")
    with pytest.raises(ValueError, match="original_session_required"):
        await support._original_source_identity(component, object(), "repeatable read")


@pytest.mark.asyncio
async def test_fixture_append_populates_exact_guarded_pair_slot(monkeypatch):
    """The real publication fixture passes its complete preparation to the protected append."""
    prepared = _prepared()
    original_row = _row(prepared)
    slot = SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: original_row))
    session = SimpleNamespace(execute=AsyncMock(return_value=slot), scalar=AsyncMock(return_value="42"))
    monkeypatch.setattr(fixture, "_publish_retained_candidate", AsyncMock())
    monkeypatch.setattr(fixture.native, "_advance_native", AsyncMock())
    monkeypatch.setattr(fixture.native, "_payload", AsyncMock(return_value=original_row["payload"]))
    monkeypatch.setattr(fixture.serving, "append_serving_receipt", AsyncMock(return_value=original_row["receipt_id"]))
    monkeypatch.setattr(fixture.serving, "read_registry_source_pair_receipt", AsyncMock(return_value=None))
    receipt_id, publication_payload = await fixture._publish_stages(session, prepared, original_row)
    fixture.serving.append_serving_receipt.assert_awaited_once_with(
        session, "mrf", publication_payload, registry_source_pair=prepared
    )
    fixture.serving.read_registry_source_pair_receipt.assert_awaited_once_with(
        session, "mrf", prepared.recipe.source_pin.retained_epoch.epoch_id
    )
    assert receipt_id == original_row["receipt_id"]


def _negative_fixture(monkeypatch, failure):
    """Record admin grants and distinct later Source transactions without DB access."""
    events = []
    session = object()

    async def execute(statement):
        events.append(str(statement).split()[0])

    @asynccontextmanager
    async def admin_transaction():
        yield SimpleNamespace(execute=execute)

    @asynccontextmanager
    async def source_transaction(database, *, isolation):
        assert isolation == "repeatable read"
        events.append("source_open")
        try:
            yield session
        finally:
            events.append("source_closed")

    monkeypatch.setattr(support, "original_source_transaction", source_transaction)
    monkeypatch.setattr(fixture, "_publish_stages", AsyncMock(return_value=("a" * 64, {})))
    monkeypatch.setattr(fixture.retention, "bind_prepared_registry_cms_source_pair", AsyncMock(side_effect=failure))
    return SimpleNamespace(engine=SimpleNamespace(begin=admin_transaction)), events


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["clone_acl", "publisher_parameter"])
@pytest.mark.parametrize("cancelled", [False, True])
async def test_negative_acl_changes_precede_source_snapshot_and_restore(monkeypatch, kind, cancelled):
    """ACL/parameter damage is admin-only and cancellation still restores it."""
    if cancelled:
        failure = asyncio.CancelledError()
    elif kind == "clone_acl":
        failure = NetworkCustomAddressSourceError("Native owner or runtime privileges are not closed")
    else:
        failure = ValueError("registry_cms_source_notification_invalid")
    database, events = _negative_fixture(monkeypatch, failure)
    retained = SimpleNamespace(address_ownership=SimpleNamespace(schema_name="retained_clone"))
    store = SimpleNamespace(publisher_role="source_publisher")
    if kind == "clone_acl":
        operation = fixture._reject_open_custody(database, retained, ("retained_owner", "reader"), {})
    else:
        operation = fixture._refuse_elevated_notification_role(database, retained, {}, store)
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await operation
    else:
        await operation
    assert events == ["GRANT", "source_open", "source_closed", "REVOKE"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_notification_callback_uses_original_publisher_body_and_restores_role(monkeypatch, cancelled):
    """Notification stays on the original backend/XID and never invents a publisher session."""
    prepared = _prepared()
    pair = fixture.retention._pair_from_prepared_receipt(prepared, _row(prepared))
    attempt = RegistryCMSSourceAttempt(
        "source-run", "source-run:12345678123456781234567812345678", "2026-01-01T00:00:00+00:00"
    )
    store = notifications.RegistryCMSSourceNotificationStore(
        "notification_control", "notification_owner", "source_publisher", attempt
    )
    actor_by_field = {"role": "source_owner"}
    events = []
    original_session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one=lambda: (7, "42"))))

    async def scalar(statement):
        return actor_by_field["role"] if "current_user" in str(statement) else 1

    original_session.scalar = scalar

    @asynccontextmanager
    async def publisher_body(session):
        assert session is original_session and actor_by_field["role"] == "source_owner"
        events.append("publisher_body")
        actor_by_field["role"] = "source_publisher"
        try:
            yield
        finally:
            actor_by_field["role"] = "source_owner"
            events.append("owner_restored")

    async def notify(actual, session, *, receipt_id, pair):
        assert session is original_session
        if (
            actor_by_field["role"] != "source_publisher"
            or actual.source_attempt != attempt
            or receipt_id != pair.proof.serving_receipt_id
        ):
            raise ValueError("registry_cms_source_notification_invalid")
        if cancelled:
            raise asyncio.CancelledError()
        return notifications.RegistryCMSSourceNotification(actual.source_attempt, pair)

    monkeypatch.setattr(policy, "source_publisher_body", publisher_body)
    monkeypatch.setattr(notifications.RegistryCMSSourceNotificationStore, "notify_receipt", notify)
    recorded_notices = []
    callback = fixture._notification_callback(AsyncMock(return_value=pair), [store], recorded_notices)
    if cancelled:
        with pytest.raises(asyncio.CancelledError):
            await callback(original_session, prepared, receipt_id=pair.proof.serving_receipt_id, receipt_payload={})
        assert recorded_notices == []
    else:
        assert (
            await callback(original_session, prepared, receipt_id=pair.proof.serving_receipt_id, receipt_payload={})
            == pair
        )
        assert recorded_notices == [notifications.RegistryCMSSourceNotification(attempt, pair)]
    assert actor_by_field["role"] == "source_owner"
    assert events == ["publisher_body", "owner_restored"]


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "heap_oid", "sequence_schema"])
async def test_fixture_custody_transfer_is_exact_and_oid_checked(monkeypatch, damage):
    """The setup transfers only its explicit heaps/sequences and rejects native substitution."""
    statements = []
    heap_rows = [
        dict(relname="entity_address_evidence", oid=101, relkind="r"),
        dict(relname="entity_address_evidence_stage", oid=202, relkind="r"),
    ]
    sequence_rows = [
        dict(relname="evidence_seq", oid=303, schema_name="mrf"),
        dict(relname="stage_seq", oid=404, schema_name="mrf"),
    ]
    if damage == "heap_oid":
        heap_rows[1]["oid"] = 999
    if damage == "sequence_schema":
        sequence_rows[1]["schema_name"] = "foreign_schema"

    async def execute(statement, parameters=None):
        statement = str(statement)
        statements.append(statement)
        rows = sequence_rows if "SELECT s.relname" in statement else heap_rows
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows))

    connection = SimpleNamespace(execute=execute, scalar=AsyncMock(side_effect=[101, 4, False]))

    @asynccontextmanager
    async def admin_transaction():
        yield connection

    database = SimpleNamespace(
        engine=SimpleNamespace(begin=admin_transaction), _source_component=SimpleNamespace(owner="source_owner")
    )
    address = SimpleNamespace(stage_oids=(("entity_address_evidence", "entity_address_evidence_stage", 202),))
    if damage:
        with pytest.raises(AssertionError):
            await fixture._transfer_address_publication_custody(database, address)
        assert not any(statement.startswith("ALTER") for statement in statements)
    else:
        await fixture._transfer_address_publication_custody(database, address)
        assert [statement for statement in statements if statement.startswith("ALTER")] == [
            'ALTER TABLE mrf."entity_address_evidence_stage" OWNER TO "source_owner"',
            'ALTER TABLE mrf."entity_address_evidence" OWNER TO "source_owner"',
            'ALTER SEQUENCE mrf."evidence_seq" OWNER TO "source_owner"',
            'ALTER SEQUENCE mrf."stage_seq" OWNER TO "source_owner"',
        ]
    assert not any("GRANT ALL" in statement or "REASSIGN OWNED" in statement for statement in statements)


@pytest.mark.asyncio
async def test_unbound_receipt_owner_refuses_before_session_sql():
    """The production guard rejects an unbound administrator without reading or publishing."""
    from process.provider_directory_generic_candidate import GenericCandidateError

    session = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock())
    assert not policy.is_source_storage_required()
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        await fixture.serving.require_registry_receipt_owner(session, "source_schema")
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_factory_selects_original_publisher_pool():
    """Publication selects the existing credential tuple without opening a connection."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from tests.provider_directory_source_postgres_support import NativeSourceFactory

    engine = create_async_engine("postgresql+asyncpg://source_publisher:example@127.0.0.1:1/native_test")
    database = SimpleNamespace(engine=engine)
    factory = NativeSourceFactory(database, policy.GenericSourcePolicy("source", "owner", "publisher", ("reader",)))
    try:
        sessions = await factory.publication_session_factory()
        assert await factory.publication_session_factory() is sessions
        assert sessions.kw["bind"].url == engine.url
        assert sessions.kw["bind"].pool.size() == 1
        assert sessions.kw["bind"].pool._max_overflow == 0
        assert sessions.kw["bind"].pool.checkedout() == 0
    finally:
        await factory.close_publication_pool()
        await engine.dispose()
    assert factory._publication_sessions is factory._publication_engine is None


@asynccontextmanager
async def _selected_native_factory(monkeypatch, *, isolation="read committed"):
    """Exercise fixture binding protocol only; no native actor proof is substituted."""
    from sqlalchemy.ext.asyncio import create_async_engine

    from tests import provider_directory_source_postgres_support as native_source

    engine = create_async_engine("postgresql+asyncpg://source_publisher:example@127.0.0.1:1/native_test")
    database = SimpleNamespace(engine=engine)
    factory = native_source.NativeSourceFactory(
        database, policy.GenericSourcePolicy("source", "owner", "publisher", ("reader",))
    )
    sessions = await factory.publication_session_factory()
    session = SimpleNamespace(
        in_transaction=lambda: True,
        in_nested_transaction=lambda: False,
        connection=AsyncMock(
            return_value=SimpleNamespace(
                engine=sessions.kw["bind"], get_isolation_level=AsyncMock(return_value=isolation.upper())
            )
        ),
    )
    database._transaction_binding = lambda: SimpleNamespace(session=session)
    driver, events = (
        SimpleNamespace(
            fetchval=AsyncMock(side_effect=lambda query: "off" if query == "SHOW transaction_read_only" else isolation)
        ),
        [],
    )

    @asynccontextmanager
    async def actor_context(*_args, **_options):
        events.append("entered")
        try:
            yield
        finally:
            events.append("closed")

    identity = AsyncMock(return_value=(driver, 7, "42"))
    monkeypatch.setattr(native_source, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(policy, "bind_generic_source_publisher", actor_context)
    monkeypatch.setattr(policy, "source_owner_transaction", actor_context)
    monkeypatch.setattr(support, "_original_source_identity", identity)
    try:
        async with policy.bind_generic_source_transaction_factory(factory):
            yield factory, database, session, identity, driver, events
    finally:
        await factory.close_publication_pool()
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
@pytest.mark.parametrize("isolation", ["read committed", "serializable"])
async def test_selected_factory_preserves_original_session(monkeypatch, cancelled, isolation):
    """Caller owns commit/rollback; cancellation closes both actor scopes without replacement."""
    async with _selected_native_factory(monkeypatch, isolation=isolation) as case:
        factory, database, session, identity, _driver, events = case
        if cancelled:
            with pytest.raises(asyncio.CancelledError):
                async with factory.bind_existing_transaction(database, session, isolation=isolation):
                    raise asyncio.CancelledError
        else:
            async with factory.bind_existing_transaction(database, session, isolation=isolation) as actual:
                assert actual is session
        assert identity.await_count == (1 if cancelled else 2)
        assert events == ["entered", "entered", "closed", "closed"]
        session.connection.return_value.get_isolation_level.assert_awaited_once_with()
        _driver.fetchval.assert_any_await("SHOW transaction_isolation")
    assert not policy.is_source_storage_required()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        "database",
        "binding",
        "pool",
        "database_url",
        "isolation",
        "inactive",
        "nested",
        "readonly",
        "connection_isolation",
        "native_isolation",
    ],
)
async def test_selected_factory_refuses_foreign_transaction(monkeypatch, damage):
    """Only the selected physical session can reach genuine Source actor verification."""
    async with _selected_native_factory(monkeypatch) as case:
        factory, database, session, identity, _driver, events = case
        supplied_database, isolation = _substitute_publication_boundary(damage, database, session, _driver)
        with pytest.raises(AssertionError):
            async with factory.bind_existing_transaction(supplied_database, session, isolation=isolation):
                raise AssertionError("foreign body reached")
        identity.assert_not_awaited()
        assert not events


def _substitute_publication_boundary(damage, database, session, driver):
    """Change one native-session prerequisite while preserving every other boundary."""
    if damage == "database":
        return object(), "read committed"
    if damage == "isolation":
        return database, "repeatable read"
    if damage == "binding":
        database._transaction_binding = lambda: SimpleNamespace(session=object())
    if damage == "pool":
        session.connection.return_value.engine = object()
    if damage == "database_url":
        database.engine = SimpleNamespace(url=database.engine.url.set(database="other_native_test"))
    if damage == "inactive":
        session.in_transaction = lambda: False
    if damage == "readonly":
        driver.fetchval.side_effect = None
        driver.fetchval.return_value = "on"
    if damage == "connection_isolation":
        session.connection.return_value.get_isolation_level.return_value = "SERIALIZABLE"
    if damage == "native_isolation":
        driver.fetchval.side_effect = lambda query: "off" if query == "SHOW transaction_read_only" else "serializable"
    if damage == "nested":
        session.in_nested_transaction = lambda: True
    return database, "read committed"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["driver", "pid", "xid"])
async def test_selected_factory_refuses_native_identity_drift(monkeypatch, field):
    """Driver/backend/top-XID changes remain terminal after the borrowed body."""
    async with _selected_native_factory(monkeypatch) as case:
        factory, database, session, identity, driver, events = case
        changed = {"driver": (object(), 7, "42"), "pid": (driver, 8, "42"), "xid": (driver, 7, "43")}[field]
        identity.side_effect = [(driver, 7, "42"), changed]
        with pytest.raises(AssertionError):
            async with factory.bind_existing_transaction(database, session, isolation="read committed"):
                assert database._transaction_binding().session is session
        assert events == ["entered", "entered", "closed", "closed"]


@pytest.mark.asyncio
@pytest.mark.parametrize("with_callback", [False, True])
async def test_native_inputs_precede_protected_source_cut(monkeypatch, with_callback):
    """The optional fixture input callback cannot run after Source ownership closes."""
    from tests import provider_directory_source_postgres_support as native_source

    engine, component, events = object(), SimpleNamespace(policy=object()), []

    async def bootstrap(actual_engine, actual_policy):
        assert actual_engine is engine and actual_policy is component.policy
        events.append("bootstrap")

    async def callback(actual):
        assert actual is component
        events.append("historical-inputs")

    async def provision(actual_engine, actual_policy):
        assert actual_engine is engine and actual_policy is component.policy
        events.append("protected-cut")

    monkeypatch.setattr(native_source, "bootstrap_source_component", bootstrap)
    monkeypatch.setattr(native_source, "provision_source_component", provision)
    await native_source._prepare_component_inputs(engine, component, callback if with_callback else None)
    assert events == (
        ["bootstrap", "historical-inputs", "protected-cut"] if with_callback else ["bootstrap", "protected-cut"]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_failed_native_inputs_never_enroll_source(monkeypatch, failure):
    """Failure and cancellation leave the actual Source cut uncalled."""
    from tests import provider_directory_source_postgres_support as native_source

    component = SimpleNamespace(policy=object())
    bootstrap, provision = AsyncMock(), AsyncMock()

    async def callback(actual):
        assert actual is component
        raise failure("historical input preparation refused")

    monkeypatch.setattr(native_source, "bootstrap_source_component", bootstrap)
    monkeypatch.setattr(native_source, "provision_source_component", provision)
    with pytest.raises(failure):
        await native_source._prepare_component_inputs(object(), component, callback)
    bootstrap.assert_awaited_once()
    provision.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_pool_size", [None, 6])
async def test_protected_admission_forwards_only_native_setup(monkeypatch, publisher_pool_size):
    """The selected pre-cut callback reaches the genuine component creator unchanged."""
    from tests import provider_directory_source_postgres_support as native_source

    callback = AsyncMock()
    observed_callbacks = []

    @asynccontextmanager
    async def component_creator(actual_patch, *, bind_factory, before_provision, migration_prefixes=None, **options):
        assert actual_patch is monkeypatch and bind_factory is False
        assert options == ({} if publisher_pool_size is None else {"publisher_pool_size": publisher_pool_size})
        observed_callbacks.append(before_provision)
        raise RuntimeError("native component setup sentinel")
        yield

    monkeypatch.setattr(native_source, "source_component_database", component_creator)
    with pytest.raises(RuntimeError, match="native component setup sentinel"):
        async with support.protected_admission_database(
            monkeypatch, before_provision=callback, publisher_pool_size=publisher_pool_size
        ):
            raise AssertionError("database body reached")
    assert observed_callbacks == [callback]
    callback.assert_not_awaited()


@asynccontextmanager
async def _ordinary_component_input(monkeypatch):
    """Use an actual session and real Source guards over explicit native catalog facts."""
    from sqlalchemy.ext.asyncio import AsyncSession

    from tests import provider_directory_source_postgres_support as native_source
    from tests.test_provider_directory_generic_source_policy import _connection, _synthetic_catalog_roles

    configured = policy.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    native, _transitions = _connection()
    native.copy_records_to_table = AsyncMock()
    native.fetch = AsyncMock(return_value=_synthetic_catalog_roles())
    native.fetchval.return_value = True
    native.fetchval.side_effect = lambda query, *args: (
        "repeatable read" if query == "SHOW transaction_isolation" else native.fetchval.return_value
    )
    backend = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=native)))
    async with AsyncSession() as session, session.begin():
        monkeypatch.setattr(session, "connection", AsyncMock(return_value=backend))
        database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
        factory = native_source.NativeSourceFactory(database, configured)
        factory.enroll_ordinary_native_inputs()

        async def execute(statement):
            if statement.startswith("SET LOCAL ROLE"):
                native.fetchrow.return_value = dict(session="source_publisher", current="source_owner")
            elif statement == "RESET ROLE":
                native.fetchrow.return_value = dict(session="source_publisher", current="source_publisher")

        async def actor(query):
            observed_by_field = dict(native.fetchrow.return_value)
            if "pg_backend_pid()" in query:
                observed_by_field.update(backend=17, transaction="1/2", readonly="off")
            return observed_by_field

        native.execute.side_effect = execute
        native.fetchrow.side_effect = actor
        async with (
            policy.bind_generic_source_transaction_factory(factory),
            policy.bind_generic_source_publisher(
                native,
                configured,
                verify_role_profile=native_source.verify_native_source_profile,
                transaction_isolation="repeatable read",
            ),
            policy.source_owner_transaction(database),
        ):
            yield SimpleNamespace(factory=factory, session=session, native=native, backend=backend, database=database)


def test_component_producer_provenance():
    """Opt-in provenance binds fixture implementation and actors; defaults remain compatible."""
    from tests.provider_directory_source_postgres_support import NativeSourceFactory

    configured = policy.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    factory = NativeSourceFactory(object(), configured)
    assert not hasattr(factory, "producer_identity") and not hasattr(factory, "native_tiger_input_fence")
    factory.enroll_ordinary_native_inputs()
    first = factory.producer_identity
    same = NativeSourceFactory(object(), configured)
    same.enroll_ordinary_native_inputs()
    assert same.producer_identity == first
    other = NativeSourceFactory(object(), policy.GenericSourcePolicy("other", "owner", "publisher", ("reader",)))
    other.enroll_ordinary_native_inputs()
    assert other.producer_identity != first
    factory.producer_identity = other.producer_identity
    with pytest.raises(AssertionError):
        factory._require_current_policy()
    untouched = NativeSourceFactory(object(), configured)
    untouched.producer_identity = first
    untouched._require_current_policy()


@pytest.mark.asyncio
async def test_component_input_absence(monkeypatch):
    """No selection is issued only after native catalog checks on the original actor."""
    from process import provider_directory_cms_tiger_inputs as tiger

    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", raising=False)
    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", raising=False)
    async with _ordinary_component_input(monkeypatch) as case:
        assert policy.source_producer_identity() == case.factory.producer_identity
        assert await tiger.capture_selected_captured_tiger_input(case.session) is None
        assert any("pg_inherits" in call.args[0] for call in case.native.fetchval.await_args_list)
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["selection", "catalog", "actor", "driver", "binding", "child", "producer"])
async def test_component_input_refusals(monkeypatch, change):
    """A declared component cannot borrow another selection, session, task or producer."""
    from process import provider_directory_cms_tiger_inputs as tiger

    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", raising=False)
    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", raising=False)
    async with _ordinary_component_input(monkeypatch) as case:
        identity, binding = case.factory.producer_identity, case.database._transaction_binding
        actor_by_field = dict(case.native.fetchrow.return_value)
        if change == "selection":
            monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", "selected.binding")
            monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", "selected.package")
        if change == "catalog":
            case.native.fetchval.return_value = False
        if change == "actor":
            case.native.fetchrow.return_value = dict(session="other", current="other")
        if change == "driver":
            case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=object())
        if change == "binding":
            case.database._transaction_binding = lambda: SimpleNamespace(session=object())
        if change == "producer":
            case.factory.producer_identity = ("0" * 40, "sha256:" + "0" * 64)
        try:
            with pytest.raises((AssertionError, ValueError)):
                if change == "child":
                    await asyncio.create_task(tiger.capture_selected_captured_tiger_input(case.session))
                else:
                    await tiger.capture_selected_captured_tiger_input(case.session)
        finally:
            case.native.fetchval.return_value = True
            case.factory.producer_identity = identity
            case.database._transaction_binding = binding
            case.native.fetchrow.return_value = actor_by_field
            case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=case.native)
        case.native.copy_records_to_table.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["driver", "actor", "producer", "selection"])
async def test_component_input_handoff(monkeypatch, change):
    """The no-selection handoff rechecks ownership after its awaited native catalog read."""
    from process import provider_directory_cms_tiger_inputs as tiger
    from tests import provider_directory_source_postgres_support as native_source

    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", raising=False)
    monkeypatch.delenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", raising=False)
    async with _ordinary_component_input(monkeypatch) as case:
        identity, actor_by_field = case.factory.producer_identity, dict(case.native.fetchrow.return_value)
        original = case.native.fetchval.side_effect

        async def observe(query, *arguments):
            if query == native_source._ORDINARY_TIGER_SELECTION_SQL:
                if change == "driver":
                    case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=object())
                if change == "actor":
                    case.native.fetchrow.return_value = dict(session="other", current="other")
                if change == "producer":
                    case.factory.producer_identity = ("0" * 40, "sha256:" + "0" * 64)
                if change == "selection":
                    monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_BINDING_RELATION", "selected.binding")
                    monkeypatch.setenv("HLTHPRT_TIGER_SOURCE_PACKAGE_RELATION", "selected.package")
                return True
            return original(query, *arguments)

        case.native.fetchval.side_effect = observe
        try:
            with pytest.raises(AssertionError):
                await tiger.capture_selected_captured_tiger_input(case.session)
        finally:
            case.native.fetchval.side_effect = original
            case.factory.producer_identity = identity
            case.native.fetchrow.return_value = actor_by_field
            case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=case.native)
        case.native.copy_records_to_table.assert_not_awaited()


def test_complete_native_source_prefix_uses_unique_actual_ancestor_order():
    """Source and native prerequisites each execute once without rewinding later guards."""
    from tests import cms_registry_complete_publication_postgres_support as complete
    from tests.provider_directory_source_postgres_support import source_component_migrations

    merged = complete.source_component_native_migrations()
    baseline = source_component_migrations()
    assert len(merged) == len(set(merged)) == 135
    assert tuple(prefix for prefix in merged if prefix in set(baseline)) == baseline
    assert set(merged) - (set(baseline) | set(complete.NATIVE_INPUT_MIGRATIONS)) == {
        "20260830090000",
        "20260909120000",
        "20261016010000",
        "20261016030000",
        "20261016040000",
        "20261016050000",
        "20261016060000",
        "20261019130000",
    }
    assert set(complete.NATIVE_INPUT_MIGRATIONS) <= set(merged)
    assert {"20261007150000", "20261017210000"} <= set(merged)
    assert {"20260628143000", "20260629160000", "20260729120000", "20261019030000"} <= set(merged)
    assert merged.index("20260629123000") < merged.index("20260629160000") < merged.index("20260630112000")
    assert merged.index("20260729110000") < merged.index("20260729120000") < merged.index("20260730110000")
    assert merged.index("20260628100000") < merged.index("20260628143000") < merged.index("20260628170000")
    assert merged[-3:] == (
        "20261022120000",
        "20261023100000_provider_directory_retirement_parent_updates",
        "20261024050000",
    )


@pytest.mark.parametrize("damage", ["duplicate", "unknown", "overlap"])
def test_complete_native_source_prefix_rejects_incompatible_selection(monkeypatch, damage):
    """A changed fixture recipe cannot silently omit or replay prerequisites."""
    from tests import cms_registry_complete_publication_postgres_support as complete

    prefixes = list(complete.NATIVE_INPUT_MIGRATIONS)
    if damage == "duplicate":
        prefixes[-1] = prefixes[0]
    elif damage == "unknown":
        prefixes[-1] = "00000000000000"
    else:
        prefixes[-1] = "20261015015000"
    monkeypatch.setattr(complete, "NATIVE_INPUT_MIGRATIONS", tuple(prefixes))
    with pytest.raises(AssertionError):
        complete.source_component_native_migrations()


@asynccontextmanager
async def _native_observation_protocol(monkeypatch):
    """Record the fixture transaction protocol; native authentication is not claimed."""
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    events = []

    @asynccontextmanager
    async def value(item, label):
        events.append(label)
        try:
            yield item
        finally:
            events.append(label + "_closed")

    session = SimpleNamespace(execute=AsyncMock(), in_transaction=lambda: True, in_nested_transaction=lambda: False)
    session.begin = lambda: value(None, "transaction")
    database = SimpleNamespace(
        engine=SimpleNamespace(url=make_url("postgresql+asyncpg://publisher@127.0.0.1:1/native_test"))
    )
    database.binding = None
    database._transaction_binding = lambda: database.binding
    factory = native_source.NativeSourceFactory(
        database, policy.GenericSourcePolicy("source", "owner", "publisher", ("reader",))
    )
    pool = SimpleNamespace(checkedout=lambda: 0)
    factory._observation_engine = SimpleNamespace(
        connect=lambda: value(object(), "connection"), pool=pool, dispose=AsyncMock()
    )
    actor_by_field = dict(session="publisher", current="owner", backend=7, readonly="on", transaction="4/2")
    driver = SimpleNamespace(
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value="read committed"),
        fetchrow=AsyncMock(return_value=actor_by_field),
        is_in_transaction=lambda: True,
        is_closed=lambda: False,
        get_server_pid=lambda: 7,
    )
    monkeypatch.setattr(native_source, "AsyncSession", lambda **kwargs: value(session, "session"))
    monkeypatch.setattr(native_source, "_copy_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(native_source, "verify_native_source_profile", AsyncMock())
    try:
        async with policy.bind_generic_source_transaction_factory(factory):
            yield factory, database, session, driver, events
    finally:
        await factory.close_publication_pool()
    assert factory._observation_engine is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_native_readonly_observation_preserves_writer_context_and_closes(monkeypatch, failure):
    """The separate read transaction never binds a publisher or replaces an existing writer."""
    async with _native_observation_protocol(monkeypatch) as case:
        factory, database, session, driver, events = case
        original = database.binding = object()
        if failure:
            with pytest.raises(failure):
                async with factory.observation_transaction() as actual:
                    assert actual is session and database.binding is original
                    assert policy._PUBLISHER.get() is None
                    raise failure("synthetic interruption")
        else:
            async with factory.observation_transaction() as actual:
                assert actual is session and database.binding is original
                assert policy._PUBLISHER.get() is None
            assert driver.execute.await_args.args == ("RESET ROLE",)
        assert events == [
            "connection",
            "session",
            "transaction",
            "transaction_closed",
            "session_closed",
            "connection_closed",
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["actor_by_field", "readonly", "backend", "transaction", "policy", "binding", "driver"]
)
async def test_native_readonly_observation_refuses_post_body_substitution(monkeypatch, damage):
    """Same-name actors or replacement physical/control identity cannot complete the read handoff."""
    from tests import provider_directory_source_postgres_support as native_source

    async with _native_observation_protocol(monkeypatch) as case:
        factory, database, _session, driver, _events = case
        with pytest.raises(AssertionError):
            async with factory.observation_transaction():
                actor_by_field = dict(driver.fetchrow.return_value)
                if damage == "actor_by_field":
                    actor_by_field["current"] = "foreign"
                if damage == "readonly":
                    actor_by_field["readonly"] = "off"
                if damage == "backend":
                    actor_by_field["backend"] = 8
                if damage == "transaction":
                    actor_by_field["transaction"] = "4/3"
                driver.fetchrow.return_value = actor_by_field
                if damage == "policy":
                    factory.policy = policy.GenericSourcePolicy("source", "owner", "publisher", ("reader",))
                if damage == "binding":
                    database.binding = object()
                if damage == "driver":
                    native_source._copy_driver.return_value = object()


@pytest.mark.asyncio
@pytest.mark.parametrize("prefixes", [[], ("unknown",), ("20261015015000",)])
async def test_source_component_rejects_unverified_migration_profile_before_database(prefixes, monkeypatch):
    """The optional combined fixture profile cannot become an arbitrary bootstrap selector."""
    from tests.provider_directory_source_postgres_support import source_component_database

    with pytest.raises(AssertionError):
        async with source_component_database(monkeypatch, migration_prefixes=prefixes):
            raise AssertionError("unverified migration profile reached setup")


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "order", "unknown", "list"])
async def test_rehydrate_profile_rejects_changes_before_database(monkeypatch, change):
    """Only the established complete ordered rehydrate profile may enter native setup."""
    from unittest.mock import Mock

    from tests import provider_directory_source_postgres_support as source_support
    from tests.provider_directory_dataset_rehydrate_postgres_support import _migrations

    prefixes = _migrations()
    changed = {
        "missing": prefixes[:-1],
        "order": tuple(reversed(prefixes)),
        "unknown": (*prefixes[:-1], "unknown"),
        "list": list(prefixes),
    }[change]
    setup = Mock(side_effect=RuntimeError("unverified profile reached database setup"))
    monkeypatch.setattr(source_support, "_source_component_database_url", setup)
    with pytest.raises(AssertionError):
        async with source_support.source_component_database(monkeypatch, migration_prefixes=changed):
            pytest.fail("unverified profile was admitted")
    setup.assert_not_called()


@asynccontextmanager
async def _ordinary_component_observation(monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    from process import provider_directory_source_observation as observation
    from tests import provider_directory_source_postgres_support as native_source
    from tests.test_provider_directory_generic_source_policy import _connection
    from tests.test_provider_directory_source_observation import Database

    directory = importlib.import_module("process.provider_directory_fhir")
    configured = policy.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    native, _transitions = _connection()
    native.get_server_pid = lambda: 17
    actor_by_field = dict(
        session="source_publisher",
        current="source_owner",
        backend=17,
        transaction="1/2",
        readonly="on",
        isolation="repeatable read",
    )
    native.fetchrow.side_effect = lambda query: dict(actor_by_field)
    native.fetchval.return_value = True
    backend = SimpleNamespace(get_raw_connection=AsyncMock(return_value=SimpleNamespace(driver_connection=native)))
    async with AsyncSession() as session, session.begin():
        monkeypatch.setattr(session, "connection", AsyncMock(return_value=backend))
        database = Database()
        factory = native_source.NativeSourceFactory(database, configured)
        factory.enroll_ordinary_native_inputs()

        @asynccontextmanager
        async def transport(*, isolation):
            assert isolation == "repeatable read"
            yield session

        monkeypatch.setattr(factory, "observation_transaction", transport)
        monkeypatch.setattr(directory, "db", database)
        monkeypatch.setattr(directory, "_schema", lambda: configured.schema)
        async with policy.bind_generic_source_transaction_factory(factory):
            async with observation.observation_transaction(directory, isolation="repeatable read"):
                yield SimpleNamespace(
                    factory=factory,
                    session=session,
                    native=native,
                    backend=backend,
                    database=database,
                    actor=actor_by_field,
                )


async def test_component_input_accepts_only_issued_readonly_observation(monkeypatch):
    from process import provider_directory_cms_tiger_inputs as tiger

    async with _ordinary_component_observation(monkeypatch) as case:
        assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None
        assert await tiger.capture_selected_captured_tiger_input(case.session) is None
        assert any("pg_inherits" in call.args[0] for call in case.native.fetchval.await_args_list)


@pytest.mark.parametrize("damage", ["unissued", "driver", "readonly", "transaction", "child"])
async def test_component_observation_refuses_changed_authority(monkeypatch, damage):
    from process import provider_directory_cms_tiger_inputs as tiger
    from process import provider_directory_source_observation as observation

    async with _ordinary_component_observation(monkeypatch) as case:
        issued, actor_by_field = observation._ACTIVE.get(), dict(case.actor)
        try:
            if damage == "unissued":
                observation._ACTIVE.set(None)
            if damage == "driver":
                case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=object())
            if damage == "readonly":
                case.actor["readonly"] = "off"
            if damage == "transaction":
                case.actor["transaction"] = "1/3"
            with pytest.raises((AssertionError, ValueError, RuntimeError)):
                if damage == "child":
                    await asyncio.create_task(tiger.capture_selected_captured_tiger_input(case.session))
                else:
                    await tiger.capture_selected_captured_tiger_input(case.session)
        finally:
            observation._ACTIVE.set(issued)
            case.actor.clear()
            case.actor.update(actor_by_field)
            case.backend.get_raw_connection.return_value = SimpleNamespace(driver_connection=case.native)
        case.native.fetchval.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_cloned_source_inputs_adopt_before_callback_and_keep_the_protected_cut(monkeypatch, failure):
    from tests import provider_directory_source_postgres_support as native_source

    events = []
    connection = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def transaction():
        yield connection

    engine = SimpleNamespace(begin=transaction)
    component = SimpleNamespace(
        policy=SimpleNamespace(owner_role="case_owner", control_runtime_roles=("case_builder",))
    )
    baseline = SimpleNamespace(builder="baseline_builder", owner="baseline_owner")

    async def callback(actual):
        assert actual is component and connection.execute.await_count == 4
        events.append("historical-inputs")
        if failure is not None:
            raise failure("synthetic input refusal")

    async def provision(actual_engine, actual_policy):
        assert actual_engine is engine and actual_policy is component.policy
        events.append("protected-cut")

    bootstrap = AsyncMock()
    monkeypatch.setattr(native_source, "bootstrap_source_component", bootstrap)
    monkeypatch.setattr(native_source, "provision_source_component", provision)
    if failure is None:
        await native_source._prepare_component_inputs(engine, component, callback, baseline=baseline)
    else:
        with pytest.raises(failure):
            await native_source._prepare_component_inputs(engine, component, callback, baseline=baseline)
    assert [str(call.args[0]) for call in connection.execute.await_args_list] == [
        'REASSIGN OWNED BY "baseline_builder" TO "case_builder"',
        'DROP OWNED BY "baseline_builder"',
        'REASSIGN OWNED BY "baseline_owner" TO "case_owner"',
        'DROP OWNED BY "baseline_owner"',
    ]
    bootstrap.assert_not_awaited()
    assert events == (["historical-inputs", "protected-cut"] if failure is None else ["historical-inputs"])


@pytest.mark.asyncio
async def test_source_template_reuses_only_exact_closed_migrations_and_clones_every_case(monkeypatch):
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    url, clones = make_url("postgresql+asyncpg://synthetic@localhost/source_test"), []
    build, drop = AsyncMock(), AsyncMock()

    @asynccontextmanager
    async def database(actual_url, template_name="template0"):
        assert actual_url == url
        clone = SimpleNamespace(database="clone_" + str(len(clones)))
        clones.append((clone, template_name))
        yield clone, object()

    monkeypatch.setattr(support, "_database_url", lambda: url)
    monkeypatch.setattr(support, "_owned_database", database)
    monkeypatch.setattr(native_source, "source_component_migrations", lambda: ("default",))
    monkeypatch.setattr(native_source, "_build_source_component_baseline", build)
    monkeypatch.setattr(native_source, "_drop_source_component_baseline", drop)
    previous = native_source._SOURCE_COMPONENT_BASELINES
    async with native_source.source_component_templates():
        for prefixes in (None, None, ("legacy",)):
            async with native_source._source_component_database_url(prefixes) as (clone, baseline):
                assert clone is clones[-1][0] and baseline.name == clones[-1][1]
        assert build.await_count == 2 and drop.await_count == 0
    assert len({id(clone) for clone, _ in clones}) == 3
    assert clones[0][1] == clones[1][1] != clones[2][1]
    assert drop.await_count == 2 and native_source._SOURCE_COMPONENT_BASELINES is previous


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_source_template_build_failure_cleans_registered_resources_before_refusal(monkeypatch, failure):
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    monkeypatch.setattr(
        support, "_database_url", lambda: make_url("postgresql+asyncpg://synthetic@localhost/source_test")
    )
    monkeypatch.setattr(
        native_source, "_build_source_component_baseline", AsyncMock(side_effect=failure("synthetic build"))
    )
    drop = AsyncMock()
    monkeypatch.setattr(native_source, "_drop_source_component_baseline", drop)
    async with native_source.source_component_templates():
        with pytest.raises(failure):
            async with native_source._source_component_database_url(None):
                pytest.fail("failed baseline yielded a clone")
        assert native_source._SOURCE_COMPONENT_BASELINES == {}
    drop.assert_awaited_once()


@pytest.mark.asyncio
async def test_source_template_keeps_custom_bootstrap_on_original_database_path(monkeypatch):
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    url = make_url("postgresql+asyncpg://synthetic@localhost/source_test")
    monkeypatch.setattr(support, "_database_url", lambda: url)
    monkeypatch.setattr(native_source, "bootstrap_source_component", AsyncMock())
    build = AsyncMock()
    monkeypatch.setattr(native_source, "_build_source_component_baseline", build)

    @asynccontextmanager
    async def database(actual_url, template_name):
        assert actual_url == url and template_name == "template0"
        yield url, object()

    monkeypatch.setattr(support, "_owned_database", database)
    async with native_source.source_component_templates():
        async with native_source._source_component_database_url(("custom",)) as (actual_url, baseline):
            assert actual_url == url and baseline is None
    build.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_template_collision_never_drops_an_unowned_database(monkeypatch):
    from sqlalchemy.engine import make_url

    from tests import provider_directory_source_postgres_support as native_source

    admin = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(native_source.asyncpg, "connect", AsyncMock(return_value=admin))
    monkeypatch.setattr(
        support, "_database_url", lambda: make_url("postgresql+asyncpg://synthetic@localhost/source_test")
    )
    async with native_source.source_component_templates():
        with pytest.raises(AssertionError):
            async with native_source._source_component_database_url(None):
                pytest.fail("colliding baseline yielded a clone")
        assert native_source._SOURCE_COMPONENT_BASELINES == {}
    admin.execute.assert_not_awaited()
    assert admin.close.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("busy_checks", [0, 2, 120])
async def test_source_template_waits_for_disconnection_before_sealing(monkeypatch, busy_checks):
    from tests import provider_directory_source_postgres_support as native_source

    baseline = SimpleNamespace(name="synthetic_template", owner="synthetic_owner", builder="synthetic_builder")
    admin = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[True] * busy_checks + [False, False, False]), execute=AsyncMock()
    )
    sleep = AsyncMock()
    monkeypatch.setattr(native_source.asyncio, "sleep", sleep)

    await native_source._close_source_component_baseline(admin, baseline)

    assert sleep.await_count == busy_checks
    assert all(call.args == (0.5,) for call in sleep.await_args_list)
    assert [call.args for call in admin.fetchval.await_args_list[: busy_checks + 1]] == [
        ("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=$1)", baseline.name)
    ] * (busy_checks + 1)
    assert [call.args for call in admin.execute.await_args_list] == [
        ('ALTER ROLE "synthetic_builder" NOLOGIN',),
        ('ALTER DATABASE "synthetic_template" ALLOW_CONNECTIONS false',),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_source_template_never_seals_a_busy_or_cancelled_baseline(monkeypatch, cancelled):
    from tests import provider_directory_source_postgres_support as native_source

    baseline = SimpleNamespace(name="synthetic_template", owner="synthetic_owner", builder="synthetic_builder")
    admin = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock())
    sleep = AsyncMock(side_effect=asyncio.CancelledError if cancelled else None)
    monkeypatch.setattr(native_source.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError if cancelled else AssertionError):
        await native_source._close_source_component_baseline(admin, baseline)

    assert admin.fetchval.await_count == (1 if cancelled else 121)
    assert sleep.await_count == (1 if cancelled else 120)
    admin.execute.assert_not_awaited()
