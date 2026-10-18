# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Physical publisher custody and refusal before source target access."""

import asyncio
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from db.connection import Database
from process import provider_directory_generic_source_policy as policy
from process.provider_directory_generic_candidate import GenericCandidateError


@asynccontextmanager
async def _callable_binding(factory, connection):
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ):
            yield


@pytest.mark.asyncio
async def test_callable_capability_preserves_the_original_native_oid_tuple(custody):
    factory, (connection, _) = _identity_factory(), _connection()
    capability = (101, 2**32 - 1)
    factory.source_callable_capability = AsyncMock(return_value=capability)
    async with _callable_binding(factory, connection):
        assert await policy.source_callable_capability(connection, factory.policy) is capability
    factory.source_callable_capability.assert_awaited_once_with(connection, factory.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["missing_factory", "wrong_factory", "connection", "policy", "factory_policy"])
async def test_callable_capability_refuses_substituted_original_authority(custody, changed):
    factory, (connection, _) = _identity_factory(), _connection()
    capture = factory.source_callable_capability = AsyncMock(return_value=(101,))
    original_policy = factory.policy
    async with _callable_binding(factory, connection):
        token = None
        supplied_connection, supplied_policy = connection, original_policy
        if changed in {"missing_factory", "wrong_factory"}:
            substitute = (
                None
                if changed == "missing_factory"
                else SimpleNamespace(policy=original_policy, source_callable_capability=capture)
            )
            token = policy._FACTORY.set(substitute)
        elif changed == "connection":
            supplied_connection = object()
        elif changed == "policy":
            supplied_policy = _policy()
        else:
            factory.policy = _policy()
        try:
            with pytest.raises(GenericCandidateError, match="callable_original_publisher_required"):
                await policy.source_callable_capability(supplied_connection, supplied_policy)
        finally:
            factory.policy = original_policy
            if token is not None:
                policy._FACTORY.reset(token)
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_callable_capability_cannot_adopt_a_factory_after_publisher_binding(custody):
    factory, (connection, _) = _identity_factory(), _connection()
    capture = factory.source_callable_capability = AsyncMock(return_value=(101,))
    async with policy.bind_generic_source_publisher(
        connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        token = policy._FACTORY.set(factory)
        try:
            with pytest.raises(GenericCandidateError, match="callable_original_publisher_required"):
                await policy.source_callable_capability(connection, factory.policy)
        finally:
            policy._FACTORY.reset(token)
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_callable_capability_rejects_an_inherited_child_task(custody):
    factory, (connection, _) = _identity_factory(), _connection()
    capture = factory.source_callable_capability = AsyncMock(return_value=(101,))

    async def child():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            await policy.source_callable_capability(connection, factory.policy)

    async with _callable_binding(factory, connection):
        await asyncio.create_task(child())
    capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", [None, [101], (True,), (0,), (-1,), (2**32,), ("101",), (101, 101)])
async def test_callable_capability_rejects_malformed_or_non_native_oid_results(custody, capability):
    factory, (connection, _) = _identity_factory(), _connection()
    factory.source_callable_capability = AsyncMock(return_value=capability)
    async with _callable_binding(factory, connection):
        with pytest.raises(GenericCandidateError, match="callable_capability_changed"):
            await policy.source_callable_capability(connection, factory.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["factory", "publisher", "policy"])
async def test_callable_capability_rechecks_original_context_after_await(custody, changed):
    factory, (connection, _) = _identity_factory(), _connection()
    original_policy = factory.policy
    tokens = []

    async def capture(connection, supplied_policy):
        if changed == "factory":
            tokens.append((policy._FACTORY, policy._FACTORY.set(_identity_factory())))
        elif changed == "publisher":
            replacement = SimpleNamespace(**vars(policy.bound_publisher()))
            tokens.append((policy._PUBLISHER, policy._PUBLISHER.set(replacement)))
        else:
            factory.policy = _policy()
        await asyncio.sleep(0)
        return (101,)

    factory.source_callable_capability = capture
    async with _callable_binding(factory, connection):
        try:
            with pytest.raises(GenericCandidateError, match="callable_publisher_changed"):
                await policy.source_callable_capability(connection, original_policy)
        finally:
            factory.policy = original_policy
            for variable, token in reversed(tokens):
                variable.reset(token)


def _policy():
    return policy.GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))


def _connection():
    transitions = []

    def transaction():
        return SimpleNamespace(
            start=AsyncMock(side_effect=lambda: transitions.append("savepoint")),
            rollback=AsyncMock(side_effect=lambda: transitions.append("rollback")),
            commit=AsyncMock(side_effect=lambda: transitions.append("release")),
        )

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        is_closed=lambda: False,
        fetchrow=AsyncMock(return_value={"session": "source_publisher", "current": "source_publisher"}),
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value="repeatable read"),
        transaction=transaction,
    )
    return connection, transitions


def _identity_factory():
    identity, configured = ("a" * 40, "sha256:" + "b" * 64), _policy()
    factory = SimpleNamespace(transaction=lambda: None, policy=configured, producer_identity=identity)

    def require_current_policy():
        if factory.policy != configured or factory.producer_identity != identity:
            raise ValueError("source_policy_changed")

    factory._require_current_policy = Mock(side_effect=require_current_policy)
    return factory


def test_source_producer_identity_requires_bound_factory():
    with pytest.raises(GenericCandidateError, match="transaction_factory_required"):
        policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_requires_trusted_policy_freshness():
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="transaction_factory_required"):
            policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_preserves_immutable_tuple_in_workers():
    factory = _identity_factory()

    async def worker():
        assert policy.source_producer_identity() is factory.producer_identity
        assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None

    async with policy.bind_generic_source_transaction_factory(factory):
        await asyncio.create_task(worker())
    factory._require_current_policy.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity", [None, ["node", "producer"], ("node",), ("node", ""), ("node", 1), ("node", "producer")]
)
async def test_source_producer_identity_refuses_malformed_tuple(identity):
    factory = _identity_factory()
    factory.producer_identity = identity
    factory._require_current_policy = Mock()
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(GenericCandidateError, match="producer_identity_required"):
            policy.source_producer_identity()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["policy", "producer_identity"])
async def test_source_producer_identity_rechecks_factory_freshness(changed):
    factory = _identity_factory()
    async with policy.bind_generic_source_transaction_factory(factory):
        setattr(factory, changed, None)
        with pytest.raises(ValueError, match="source_policy_changed"):
            policy.source_producer_identity()


@pytest.mark.asyncio
async def test_source_producer_identity_matches_original_publisher_factory_policy_and_task(custody):
    factory = _identity_factory()
    connection, _ = _connection()
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.bind_generic_source_publisher(
            connection, factory.policy, verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
        ):
            assert policy.source_producer_identity() is factory.producer_identity
            factory.policy = _policy()
            with pytest.raises(GenericCandidateError, match="source_policy_changed"):
                policy.source_producer_identity()
            factory.policy = policy.bound_publisher().policy
            token = policy._FACTORY.set(_identity_factory())
            try:
                with pytest.raises(GenericCandidateError, match="source_policy_changed"):
                    policy.source_producer_identity()
            finally:
                policy._FACTORY.reset(token)

            async def child():
                with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
                    policy.source_producer_identity()

            await asyncio.create_task(child())


@pytest.fixture
def custody(monkeypatch):
    native = AsyncMock()
    monkeypatch.setattr(policy, "storage_roles", native)
    return native


@pytest.mark.asyncio
async def test_missing_publisher_refuses_before_driver_or_target_sql(monkeypatch):
    driver = AsyncMock()
    monkeypatch.setattr(policy, "_copy_driver", driver)
    with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
        async with policy.source_owner_transaction(object()):
            pytest.fail("ordinary session became an owner")
    driver.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_publication_requires_trusted_factory_before_io():
    with pytest.raises(GenericCandidateError, match="original_publication_required"):
        await policy.source_publication_sessions()
    with pytest.raises(GenericCandidateError, match="original_publication_required"):
        async with policy.original_source_publication(object(), object()):
            pytest.fail("unbound original publication admitted")


@pytest.mark.asyncio
async def test_original_publication_delegates_same_session_without_owning_lifecycle(monkeypatch):
    database, session, sessions = object(), object(), object()
    calls = []

    @asynccontextmanager
    async def original(supplied_database, supplied_session, *, isolation):
        calls.append((supplied_database, supplied_session, isolation))
        yield supplied_session

    @asynccontextmanager
    async def publication_body(supplied_database):
        assert supplied_database is database
        yield session

    monkeypatch.setattr(policy, "source_publication_body", publication_body)
    factory = SimpleNamespace(
        transaction=lambda: None,
        publication_session_factory=AsyncMock(return_value=sessions),
        bind_existing_transaction=original,
    )
    async with policy.bind_generic_source_transaction_factory(factory):
        assert await policy.source_publication_sessions() is sessions
        async with policy.original_source_publication(database, session) as bound:
            assert bound is session
    assert calls == [(database, session, "read committed")]
    factory.publication_session_factory.assert_awaited_once_with()


@pytest.mark.asyncio
async def test_intake_only_factory_cannot_supply_final_publication():
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="publication_factory_required"):
            await policy.source_publication_sessions()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor", ({"session": "ordinary", "current": "ordinary"}, {"session": "ordinary", "current": "source_publisher"})
)
async def test_claimed_or_set_role_publisher_is_not_genuine(custody, actor):
    connection, _ = _connection()
    connection.fetchrow.return_value = actor
    verifier = AsyncMock()
    with pytest.raises(GenericCandidateError, match="genuine_publisher_required"):
        async with policy.bind_generic_source_publisher(
            connection, _policy(), verify_role_profile=verifier, transaction_isolation="repeatable read"
        ):
            pytest.fail("false publisher was admitted")
    custody.assert_not_awaited()
    verifier.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_physical_owner_and_role_are_restored(custody, monkeypatch):
    connection, transitions = _connection()
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))
    verifier = AsyncMock()
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=verifier, transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database) as publisher:
            assert publisher.connection is connection
    assert transitions == ["savepoint", "release"]
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SET LOCAL ROLE "source_owner"',
        "RESET ROLE",
    ]
    custody.assert_awaited_once()
    verifier.assert_awaited_once()


@pytest.mark.asyncio
async def test_different_physical_connection_is_refused(custody, monkeypatch):
    connection, transitions = _connection()
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=object()))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        with pytest.raises(GenericCandidateError, match="physical_transaction_changed"):
            async with policy.source_owner_transaction(database):
                pytest.fail("a side connection split the transaction")
    assert transitions == []
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_child_task_cannot_reuse_the_parent_physical_connection(custody):
    connection, _ = _connection()

    async def child():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            policy.bound_publisher()

    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        await asyncio.create_task(child())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("rejected"), asyncio.CancelledError()))
async def test_rejection_and_cancellation_roll_back_original_savepoint(custody, monkeypatch, failure):
    connection, transitions = _connection()
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        with pytest.raises(type(failure)):
            async with policy.source_owner_transaction(database):
                raise failure
    assert transitions == ["savepoint", "rollback"]
    assert [call.args[0] for call in connection.execute.await_args_list] == ['SET LOCAL ROLE "source_owner"']


@pytest.mark.asyncio
async def test_wrong_native_isolation_refuses_before_profile_or_owner(custody):
    connection, transitions = _connection()
    connection.fetchval.return_value = "read committed"
    verifier = AsyncMock()
    with pytest.raises(GenericCandidateError, match="isolation_changed"):
        async with policy.bind_generic_source_publisher(
            connection, _policy(), verify_role_profile=verifier, transaction_isolation="repeatable read"
        ):
            pytest.fail("isolation was only a caller claim")
    custody.assert_not_awaited()
    verifier.assert_not_awaited()
    assert transitions == []


@pytest.mark.asyncio
async def test_nested_helpers_reuse_only_the_same_original_owner(custody, monkeypatch):
    connection, transitions = _connection()
    session = object()
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database):
            connection.fetchrow.return_value = {"session": "source_publisher", "current": "source_owner"}
            async with policy.source_owner_transaction(database):
                assert policy.source_copy_session() is session
        with pytest.raises(GenericCandidateError, match="owner_transaction_required"):
            policy.source_copy_session()
    assert transitions == ["savepoint", "savepoint", "release", "release"]
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        'SET LOCAL ROLE "source_owner"',
        "RESET ROLE",
    ]


@pytest.mark.asyncio
async def test_closed_copy_transport_preserves_cancellation_without_second_rollback(custody, monkeypatch):
    connection, transitions = _connection()
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        with pytest.raises(asyncio.CancelledError):
            async with policy.source_owner_transaction(database):
                connection.is_closed = lambda: True
                raise asyncio.CancelledError
    assert transitions == ["savepoint"]


def _factory_case(monkeypatch):
    binding = ContextVar("synthetic_source_session", default=None)
    database = SimpleNamespace(_transaction_binding=lambda: binding.get())
    opened_sessions = []
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(side_effect=lambda session: session.driver))

    @asynccontextmanager
    async def transaction(requested_database, *, isolation):
        assert requested_database is database
        connection, _ = _connection()
        connection.fetchval.return_value = isolation
        session = SimpleNamespace(driver=connection)
        token = binding.set(SimpleNamespace(session=session))

        async def execute(statement):
            current = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
            connection.fetchrow.return_value = {"session": "source_publisher", "current": current}

        connection.execute.side_effect = execute
        opened_sessions.append((session, isolation))
        try:
            async with policy.bind_generic_source_publisher(
                connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation=isolation
            ):
                async with policy.source_owner_transaction(database):
                    yield session
        finally:
            binding.reset(token)

    return database, SimpleNamespace(transaction=transaction), opened_sessions


@pytest.mark.asyncio
async def test_factory_propagates_to_two_workers_but_each_owns_its_physical_transaction(custody, monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    ready = asyncio.Event()
    entered_sessions = []

    async def worker():
        async with policy.source_transaction(database) as session:
            entered_sessions.append(session)
            if len(entered_sessions) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), timeout=2)
            assert policy.bound_publisher().task is asyncio.current_task()
            assert policy.source_copy_session() is session
            return session

    async with policy.bind_generic_source_transaction_factory(factory):
        assert policy.source_transaction_available() is None
        sessions = await asyncio.gather(asyncio.create_task(worker()), asyncio.create_task(worker()))
    assert sessions[0] is not sessions[1]
    assert sessions[0].driver is not sessions[1].driver
    assert [isolation for _, isolation in opened] == ["read committed", "read committed"]
    assert policy._PUBLISHER.get() is None and policy._FACTORY.get() is None


@pytest.mark.asyncio
async def test_factory_must_bind_a_genuine_publisher_before_it_can_yield(monkeypatch):
    driver = AsyncMock()
    monkeypatch.setattr(policy, "_copy_driver", driver)

    @asynccontextmanager
    async def transaction(database, *, isolation):
        yield

    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=transaction)):
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            async with policy.source_transaction(object()):
                pytest.fail("unbound factory became a writer")
    driver.assert_not_awaited()


@pytest.mark.asyncio
async def test_nested_transaction_cannot_upgrade_isolation_after_earlier_queries(custody, monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database):
            with pytest.raises(GenericCandidateError, match="isolation_changed"):
                async with policy.source_transaction(database, isolation="repeatable read"):
                    pytest.fail("read committed transaction was retroactively upgraded")
    assert len(opened) == 1


@pytest.mark.asyncio
async def test_seal_transaction_requests_repeatable_read_before_factory_queries(custody, monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database, isolation="repeatable read"):
            assert policy.bound_publisher().transaction_isolation == "repeatable read"
    assert [isolation for _, isolation in opened] == ["repeatable read"]


@pytest.mark.asyncio
async def test_child_cannot_replace_an_inherited_live_session_with_another_factory_transaction(custody, monkeypatch):
    database, factory, opened = _factory_case(monkeypatch)

    async def child():
        with pytest.raises(GenericCandidateError, match="publisher_connection_required"):
            async with policy.source_transaction(database):
                pytest.fail("inherited physical session was replaced under an active transaction")

    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_transaction(database):
            await asyncio.create_task(child())
    assert len(opened) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("body refused"), asyncio.CancelledError()])
async def test_original_publisher_body_restores_owner_without_abort_role_sql(custody, monkeypatch, failure):
    connection, transitions = _connection()
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))

    async def execute(statement):
        actor = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
        connection.fetchrow.return_value = {"session": "source_publisher", "current": actor}

    connection.execute.side_effect = execute
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database):
            saved_owner = policy._OWNER.get()
            try:
                await _exercise_publisher_body(connection, session, database, failure)
            except (RuntimeError, asyncio.CancelledError) as error:
                assert error is failure
            assert policy._OWNER.get() is saved_owner
    statements = [call.args[0] for call in connection.execute.await_args_list]
    if failure is None:
        assert transitions == ["savepoint", "savepoint", "savepoint", "release", "release", "release"]
        assert statements == [
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
            'SET LOCAL ROLE "source_owner"',
            "RESET ROLE",
        ]
    else:
        assert transitions == ["savepoint", "savepoint", "rollback", "release"]
        assert statements == ['SET LOCAL ROLE "source_owner"', "RESET ROLE", "RESET ROLE"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["session", "driver", "actor"])
async def test_publisher_body_rejects_changed_original_identity_before_role_sql(custody, monkeypatch, change):
    connection, transitions = _connection()
    session = object()
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))
    async with policy.bind_generic_source_publisher(
        connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation="repeatable read"
    ):
        async with policy.source_owner_transaction(database):
            connection.fetchrow.return_value = {"session": "source_publisher", "current": "source_owner"}
            if change == "driver":
                monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=object()))
            if change == "actor":
                connection.fetchrow.return_value = {"session": "foreign", "current": "source_owner"}
            with pytest.raises(
                GenericCandidateError, match="original_transaction|physical_transaction|owner_transaction"
            ):
                async with policy.source_publisher_body(object() if change == "session" else session):
                    pytest.fail("changed original identity was admitted")
            assert transitions == ["savepoint"]
            assert connection.execute.await_count == 1


async def _exercise_publisher_body(connection, session, database, failure):
    async with policy.source_publisher_body(session) as publisher:
        assert publisher.connection is connection and policy._OWNER.get() is None
        assert (await connection.fetchrow())["current"] == "source_publisher"
        if failure is not None:
            raise failure
        async with policy.source_owner_transaction(database):
            assert policy.source_copy_session() is session


def _synthetic_catalog_roles():
    """Closed catalog answers for unit orchestration, never native role proof."""
    rows = []
    for oid, name in enumerate(("source_owner", "source_publisher", "source_reader"), 1):
        rows.append(
            dict(
                oid=oid,
                rolname=name,
                rolcanlogin=name != "source_owner",
                elevated=False,
                rolsuper=False,
                rolcreatedb=False,
                rolcreaterole=False,
                rolreplication=False,
                rolbypassrls=False,
                owner_set=name != "source_reader",
                publisher_set=name == "source_publisher",
                namespace_usage=True,
                namespace_create=name != "source_reader",
                namespace_owner_reachable=name != "source_reader",
                namespace_owner_protected=True,
            )
        )
    return rows


@asynccontextmanager
async def synthetic_source_publisher(
    monkeypatch, database, *, isolation="repeatable read", schema="source_schema", binding=None
):
    """Exercise real publisher/Owner/physical guards over explicit unit catalog data."""
    from process.plan_release_storage_catalog import ROLE_SQL

    configured = policy.GenericSourcePolicy(schema, "source_owner", "source_publisher", ("source_reader",))
    connection, transitions = _connection()
    connection.copy_records_to_table = AsyncMock()
    connection.get_server_pid = lambda: 17
    connection.fetchval.side_effect = lambda query, *args: isolation if query == "SHOW transaction_isolation" else "23"
    connection.status = AsyncMock()

    async def fetch(query, *arguments):
        assert query == ROLE_SQL
        assert arguments == (
            ["source_owner", "source_publisher", "source_reader"],
            "source_owner",
            "source_publisher",
            schema,
        )
        return _synthetic_catalog_roles()

    async def execute(statement, *arguments):
        if statement.startswith("SET LOCAL ROLE") or statement == "RESET ROLE":
            assert not arguments
            current = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
            connection.fetchrow.return_value = dict(session="source_publisher", current=current)

    async def verify(_connection, schema, **roles):
        assert _connection is connection and schema == configured.schema and roles == configured.role_arguments()

    connection.fetch = fetch
    connection.execute.side_effect = execute
    token = None
    if binding is None:
        monkeypatch.setattr(
            database, "_transaction_binding", lambda: SimpleNamespace(session=connection), raising=False
        )
    else:
        token = binding.set(SimpleNamespace(session=connection))
    try:
        async with (
            policy.bind_generic_source_publisher(
                connection, configured, verify_role_profile=verify, transaction_isolation=isolation
            ),
            policy.source_owner_transaction(database),
        ):
            yield connection, transitions
    finally:
        if token is not None:
            binding.reset(token)


class SyntheticSourceWorkflow:
    """Opt-in unit dependency adapter; it never attests native candidate custody."""

    def __init__(self, directory, candidate, identity, *, drained=None, before_seal=None):
        from copy import deepcopy

        from process.provider_directory_registry_network_metadata import fhir_network_binding_metadata

        self.directory, self.candidate = directory, candidate
        self.database = directory.db
        self.identity = deepcopy(identity)
        assert self.identity["source_id"] == "cms-npd"
        self.schema = directory._schema() if hasattr(directory, "_schema") else "source_schema"
        self.coordinates = fhir_network_binding_metadata(
            source_id="cms-npd",
            dataset_schema=self.schema,
            dataset_id=candidate.dataset_id,
            producer_id="cms-npd-bulk-fhir-v1",
            alias_scope="cms-npd",
        )
        _declare_unit_candidate(candidate, deepcopy(identity), deepcopy(self.coordinates))
        self.dataset_id = candidate.dataset_id
        self.drained, self.before_seal = drained, before_seal
        self.events = []
        self.active = 0
        self.snapshot = ContextVar("unit_source_snapshot", default=None)

    def _checked_session(self, directory, candidate):
        assert directory is self.directory and directory.db is self.database and candidate is self.candidate
        assert candidate.dataset_id == self.dataset_id and candidate.source_ids == ("cms-npd",)
        assert candidate.source_release == self.identity and candidate.network_bindings == self.coordinates
        publisher = policy.bound_publisher()
        assert publisher.policy.schema == self.schema and publisher.task is asyncio.current_task()
        assert publisher.transaction_isolation in {"repeatable read", "serializable"}
        session = policy.source_copy_session()
        assert directory.db._transaction_binding().session is session
        assert publisher.connection is session
        return session

    @asynccontextmanager
    async def completed(self, directory, candidate):
        session = self._checked_session(directory, candidate)
        assert self.snapshot.get() is None
        if self.drained is not None:
            self.drained()
        token = self.snapshot.set(session)
        self.events.append("snapshot.begin")
        try:
            yield self
        except BaseException:
            self.events.append("snapshot.rollback")
            raise
        else:
            self.events.append("snapshot.commit")
        finally:
            self.snapshot.reset(token)

    async def seal(self, database, directory, candidate):
        assert database is self.database
        assert self.snapshot.get() is self._checked_session(directory, candidate)
        assert "identity.sealed" not in self.events
        if self.before_seal is not None:
            self.before_seal()
        self.events.append("identity.sealed")

    @asynccontextmanager
    async def _transaction(self, monkeypatch, database, *, isolation):
        assert database is self.database and isolation in {"repeatable read", "serializable"}
        self.events.append("transaction.begin")
        self.active += 1
        try:
            with monkeypatch.context() as settings:
                async with synthetic_source_publisher(
                    settings, database, isolation=isolation, schema=self.schema
                ) as pair:
                    yield pair[0]
        except BaseException:
            self.events.append("transaction.rollback")
            raise
        else:
            self.events.append("transaction.commit")
        finally:
            self.active -= 1

    @asynccontextmanager
    async def bind(self, monkeypatch, *, completed=False):
        from process import provider_directory_generic_source_adapters as adapters
        from process import provider_directory_generic_source_auxiliary as auxiliary

        factory = SimpleNamespace(
            transaction=lambda database, **options: self._transaction(monkeypatch, database, **options)
        )
        with monkeypatch.context() as settings:
            settings.setattr(adapters, "completed_source_scope", self.completed)
            settings.setattr(auxiliary, "seal_identity_candidate", self.seal)
            async with policy.bind_generic_source_transaction_factory(factory):
                if completed:
                    async with (
                        policy.source_transaction(self.database, isolation="repeatable read"),
                        self.completed(self.directory, self.candidate),
                    ):
                        yield self
                else:
                    yield self
        assert self.active == 0 and self.snapshot.get() is None

    def assert_outcome(self, outcome, *, sealed=False):
        expected_events = ["transaction.begin", "snapshot.begin"]
        if sealed:
            expected_events.append("identity.sealed")
        assert self.events == [*expected_events, "snapshot." + outcome, "transaction." + outcome]
        assert self.active == 0 and self.snapshot.get() is None


def _workflow_case():
    from process import provider_directory_cms_npd as cms
    from tests.test_provider_directory_cms_npd import _receipt

    directory = SimpleNamespace(db=SimpleNamespace(), _schema=lambda: "source_schema")
    candidate = SimpleNamespace(dataset_id="unit-dataset")
    return SyntheticSourceWorkflow(directory, candidate, cms.release_identity(_receipt()))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ("source_system", "source_id", "dataset_schema", "dataset_id", "producer_id", "edition_id")
)
async def test_unit_workflow_refuses_changed_source_coordinates(monkeypatch, field):
    workflow = _workflow_case()
    with pytest.raises(AssertionError):
        async with workflow.bind(monkeypatch):
            async with policy.source_transaction(workflow.database, isolation="repeatable read"):
                workflow.candidate.network_bindings[field] = "changed"
                async with workflow.completed(workflow.directory, workflow.candidate):
                    pytest.fail("changed coordinates admitted")
    assert workflow.events == ["transaction.begin", "transaction.rollback"]
    assert workflow.active == 0 and workflow.snapshot.get() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("candidate", "source", "session", "database"))
async def test_unit_workflow_refuses_changed_physical_identity(monkeypatch, change):
    workflow = _workflow_case()
    with pytest.raises(AssertionError):
        async with workflow.bind(monkeypatch):
            async with policy.source_transaction(workflow.database, isolation="repeatable read"):
                candidate = _changed_workflow_identity(monkeypatch, workflow, change)
                async with workflow.completed(workflow.directory, candidate):
                    pytest.fail("changed identity admitted")
    assert workflow.events == ["transaction.begin", "transaction.rollback"]
    assert workflow.active == 0 and workflow.snapshot.get() is None


def _changed_workflow_identity(monkeypatch, workflow, change):
    candidate = workflow.candidate
    if change == "candidate":
        return SimpleNamespace(**vars(candidate))
    if change == "source":
        candidate.source_release["vector_sha256"] = "f" * 64
    elif change == "session":
        monkeypatch.setattr(workflow.database, "_transaction_binding", lambda: SimpleNamespace(session=object()))
    else:
        workflow.directory.db = object()
    return candidate


def _declare_unit_candidate(candidate, identity, coordinates):
    """Fill missing unit declarations; never normalize contradictory source facts."""
    for field, declared in (
        ("source_ids", ("cms-npd",)),
        ("source_release", identity),
        ("network_bindings", coordinates),
    ):
        if hasattr(candidate, field):
            assert getattr(candidate, field) == declared
        else:
            setattr(candidate, field, declared)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ("source_ids", "source_release", "network_bindings"))
async def test_unit_workflow_rejects_conflicting_existing_declaration(field):
    expected = _workflow_case()
    candidate = SimpleNamespace(dataset_id=expected.dataset_id, **{field: "foreign"})
    with pytest.raises(AssertionError):
        SyntheticSourceWorkflow(expected.directory, candidate, expected.identity)


@asynccontextmanager
async def synthetic_source_factory(monkeypatch, database, *, schema="source_schema"):
    """Opt in to real publisher guards over explicit, task-local unit catalog doubles."""
    binding = ContextVar("unit_source_factory_session", default=None)
    monkeypatch.setattr(database, "_transaction_binding", binding.get, raising=False)

    @asynccontextmanager
    async def transaction(requested_database, *, isolation):
        assert requested_database is database
        with monkeypatch.context() as settings:
            async with synthetic_source_publisher(
                settings, database, isolation=isolation, schema=schema, binding=binding
            ) as (connection, _transitions):
                yield connection

    factory = SimpleNamespace(transaction=transaction)
    async with policy.bind_generic_source_transaction_factory(factory):
        yield factory
    assert binding.get() is None


@pytest.mark.asyncio
async def test_unit_factory_keeps_each_task_physical_session_separate(monkeypatch):
    database = SimpleNamespace()
    entered_sessions = []
    ready = asyncio.Event()

    async def operation():
        async with policy.source_transaction(database) as session:
            entered_sessions.append(session)
            if len(entered_sessions) == 2:
                ready.set()
            await asyncio.wait_for(ready.wait(), timeout=2)
            assert policy.bound_publisher().task is asyncio.current_task()
            assert policy.source_copy_session() is session
            return session

    async with synthetic_source_factory(monkeypatch, database):
        first, second = await asyncio.gather(operation(), operation())
    assert first is not second
    assert database._transaction_binding() is None
    assert policy._PUBLISHER.get() is policy._FACTORY.get() is None


@asynccontextmanager
async def synthetic_checkpoint_factory(monkeypatch, database, *, schema="mrf"):
    """Declare native catalog data only; retain real original-session Source guards."""
    from process import provider_directory_source_checkpoint_control as checkpoints

    async def capture(connection, configured):
        publisher = policy.bound_publisher()
        assert publisher.task is asyncio.current_task()
        assert publisher.connection is connection and publisher.policy is configured
        assert configured.schema == schema
        assert policy.source_copy_session() is database._transaction_binding().session is connection
        return {}

    with monkeypatch.context() as settings:
        settings.setattr(checkpoints, "capture_checkpoint_controls", capture)
        async with synthetic_source_factory(settings, database, schema=schema) as factory:
            yield factory


class SyntheticSourceDatabase(Database):
    """Delegate explicit unit SQL/state while retaining the actual Database guard type."""

    def __init__(self, domain):
        object.__setattr__(self, "domain", domain)

    def __getattr__(self, name):
        return getattr(self.domain, name)

    def __setattr__(self, name, value):
        if name in {"domain", "_transaction_binding", "all", "first", "scalar", "status"}:
            object.__setattr__(self, name, value)
        else:
            setattr(self.domain, name, value)

    async def all(self, *args, **kwargs):
        return await self.domain.all(*args, **kwargs)

    async def first(self, *args, **kwargs):
        return await self.domain.first(*args, **kwargs)

    async def scalar(self, *args, **kwargs):
        return await self.domain.scalar(*args, **kwargs)

    async def status(self, *args, **kwargs):
        return await self.domain.status(*args, **kwargs)


@asynccontextmanager
async def synthetic_source_database(monkeypatch, domain, *, schema="mrf"):
    """Keep original fake SQL/state/rollback oracles behind a task-local Database interface."""
    from contextlib import nullcontext

    database = SyntheticSourceDatabase(domain)
    async with synthetic_checkpoint_factory(monkeypatch, database, schema=schema) as factory:
        original = factory.transaction

        @asynccontextmanager
        async def transaction(requested_database, *, isolation):
            assert requested_database is database
            boundary = domain.transaction() if callable(getattr(domain, "transaction", None)) else nullcontext()
            async with boundary:
                async with original(database, isolation=isolation) as session:
                    yield session

        monkeypatch.setattr(factory, "transaction", transaction)
        yield database


@pytest.mark.asyncio
async def test_unit_database_facade_keeps_domain_sql_and_current_source_session(monkeypatch):
    from process.provider_directory_source_checkpoint_control import require_checkpoint_executor

    domain = SimpleNamespace(first=AsyncMock(return_value={"dataset_id": "exact"}), calls=[])
    async with synthetic_source_database(monkeypatch, domain) as database:
        async with policy.source_transaction(database) as session:
            assert isinstance(database, Database)
            publisher = await require_checkpoint_executor(database)
            assert publisher.task is asyncio.current_task()
            assert publisher.connection is session is policy.source_copy_session()
            assert database._transaction_binding().session is session
            assert await database.first("SELECT exact", dataset_id="exact") == {"dataset_id": "exact"}
            domain.first.assert_awaited_once_with("SELECT exact", dataset_id="exact")
            database.calls.append("kept")
    assert domain.calls == ["kept"]


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["task", "session", "database"])
async def test_unit_database_facade_never_waives_source_executor_guards(monkeypatch, substitution):
    from process.provider_directory_source_checkpoint_control import require_checkpoint_executor

    domain = SimpleNamespace(first=AsyncMock())
    async with synthetic_source_database(monkeypatch, domain) as database, policy.source_transaction(database):
        with monkeypatch.context() as changed:
            executor = database
            if substitution == "session":
                changed.setattr(database, "_transaction_binding", lambda: SimpleNamespace(session=object()))
            elif substitution == "database":
                executor = SyntheticSourceDatabase(SimpleNamespace())
            with pytest.raises(
                GenericCandidateError, match="publisher_connection_required|checkpoint_transaction_changed"
            ):
                if substitution == "task":
                    await asyncio.create_task(require_checkpoint_executor(executor))
                else:
                    await require_checkpoint_executor(executor)
        domain.first.assert_not_awaited()


@pytest.mark.asyncio
async def test_checkpoint_guard_delegates_without_publisher_or_owner():
    database, connection, transaction = Database(), object(), object()

    @asynccontextmanager
    async def guard(supplied):
        assert supplied is database and policy._PUBLISHER.get() is policy._OWNER.get() is None
        yield connection, transaction

    factory = SimpleNamespace(transaction=lambda: None, checkpoint_guard=guard)
    async with policy.bind_generic_source_transaction_factory(factory):
        async with policy.source_checkpoint_guard(database) as issued:
            assert issued == (connection, transaction)
            assert policy._PUBLISHER.get() is policy._OWNER.get() is None


@pytest.mark.asyncio
async def test_checkpoint_guard_requires_issued_factory():
    with pytest.raises(GenericCandidateError, match="checkpoint_guard_required"):
        async with policy.source_checkpoint_guard(Database()):
            pytest.fail("unissued guard admitted")
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=lambda: None)):
        with pytest.raises(GenericCandidateError, match="checkpoint_guard_factory_required"):
            async with policy.source_checkpoint_guard(Database()):
                pytest.fail("intake factory admitted")


@pytest.mark.asyncio
async def test_checkpoint_guard_refuses_proxy_before_factory():
    factory = SimpleNamespace(transaction=lambda: None, checkpoint_guard=Mock())
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(GenericCandidateError, match="checkpoint_guard_database_required"):
            async with policy.source_checkpoint_guard(SimpleNamespace(_transaction_binding=lambda: None)):
                pytest.fail("proxy admitted")
    factory.checkpoint_guard.assert_not_called()
