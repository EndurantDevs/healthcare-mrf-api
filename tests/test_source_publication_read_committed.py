# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh final controls preserve the original actor and preparation boundaries."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving_coverage as coverage
from process import provider_directory_generic_source_policy as policy
from process import provider_directory_source_local_publication as local
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_generic_source_policy import _connection, _policy

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.fixture
def owner_body(monkeypatch):
    """Use the actual publisher/Owner binding with an explicit native boundary double."""
    connection, transitions = _connection()
    native = SimpleNamespace(isolation="read committed", actor="source_publisher", pid=1234, xid="42", active=True)
    session = object()
    database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=session))

    async def execute(statement):
        native.actor = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"

    connection.execute.side_effect = execute
    connection.fetchrow.side_effect = lambda _: {"session": "source_publisher", "current": native.actor}
    connection.fetchval.side_effect = lambda sql: (
        native.isolation if sql == "SHOW transaction_isolation" else native.xid
    )
    connection.get_server_pid = lambda: native.pid
    connection.is_in_transaction = lambda: native.active
    monkeypatch.setattr(policy, "storage_roles", AsyncMock())
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=connection))

    @asynccontextmanager
    async def bind(isolation="read committed", *, final=True):
        native.isolation = isolation
        async with policy.bind_generic_source_publisher(
            connection, _policy(), verify_role_profile=AsyncMock(), transaction_isolation=isolation
        ) as publisher:
            async with policy.source_owner_transaction(database):
                if isolation != "read committed" or not final:
                    yield SimpleNamespace(
                        database=database, session=session, connection=connection, publisher=publisher, native=native
                    )
                    return
                async with policy.source_publication_body(database):
                    yield SimpleNamespace(
                        database=database, session=session, connection=connection, publisher=publisher, native=native
                    )

    return bind


@pytest.mark.asyncio
async def test_exact_rc_original_body_keeps_the_same_session_and_driver(owner_body):
    async with owner_body() as body:
        assert await policy.require_source_publication_transaction(body.database) is body.publisher
        assert policy.source_copy_session() is body.session
        assert body.publisher.connection is body.connection
        assert body.native.actor == "source_owner"


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["repeatable read", "serializable"])
async def test_minimum_rc_borrow_cannot_authorize_a_stale_final_cut(owner_body, isolation, monkeypatch):
    owner = AsyncMock()
    monkeypatch.setattr(local, "_publish_validated_source_local_dataset_owner", owner)
    async with owner_body(isolation) as body:
        async with policy.source_transaction(body.database):
            assert policy.source_copy_session() is body.session
        with pytest.raises(GenericCandidateError, match="read_committed_publication_required"):
            await local.publish_validated_source_local_dataset(
                SimpleNamespace(db=body.database), object(), "synthetic-source"
            )
    owner.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["isolation", "actor", "session", "driver", "task", "backend", "xid", "transaction"]
)
async def test_original_publication_binding_rejects_substitution(owner_body, changed, monkeypatch):
    async with owner_body() as body:
        database = body.database
        if changed == "isolation":
            body.native.isolation = "repeatable read"
        if changed == "actor":
            body.native.actor = "source_publisher"
        if changed == "session":
            database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
        if changed == "driver":
            monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=object()))
        if changed == "backend":
            body.native.pid = 4321
        if changed == "xid":
            body.native.xid = "43"
        if changed == "transaction":
            body.native.active = False
        with pytest.raises(GenericCandidateError):
            if changed == "task":
                await asyncio.create_task(policy.require_source_publication_transaction(database))
            else:
                await policy.require_source_publication_transaction(database)
        body.native.actor = "source_owner"
        body.native.isolation, body.native.pid, body.native.xid, body.native.active = "read committed", 1234, "42", True
        monkeypatch.setattr(policy, "_copy_driver", AsyncMock(return_value=body.connection))


@pytest.mark.asyncio
async def test_source_local_entry_selects_rc_and_borrows_the_original_owner(owner_body, monkeypatch):
    owner = AsyncMock()
    monkeypatch.setattr(local, "_publish_validated_source_local_dataset_owner", owner)
    async with owner_body() as body:
        directory = SimpleNamespace(db=body.database)
        candidate = object()
        fence = object()
        monkeypatch.setattr(local, "_validated_source_local_fence", AsyncMock(return_value=fence))
        await local.publish_validated_source_local_dataset(directory, candidate, "synthetic-source")
        assert policy.source_copy_session() is body.session
    owner.assert_awaited_once_with(directory, fence, None, None, None)


@pytest.mark.asyncio
async def test_final_coverage_borrows_rc_and_authenticates_replay(owner_body, monkeypatch):
    from process import provider_directory_cms_serving_receipt as receipts
    from process import provider_directory_source_cms_epoch_control as epoch

    receipt_owner = AsyncMock()
    epoch_controls = AsyncMock()
    monkeypatch.setattr(receipts, "require_registry_receipt_owner", receipt_owner)
    monkeypatch.setattr(epoch, "capture_source_cms_epoch_controls", epoch_controls)
    async with owner_body() as body:
        directory = SimpleNamespace(db=body.database, _schema=lambda: "source_schema")
        async with coverage._source_publication_body(directory, body.session):
            assert policy.source_copy_session() is body.session
        with pytest.raises(RuntimeError, match="original_session_required"):
            async with coverage._source_publication_body(directory, object()):
                pytest.fail("a replacement final session was admitted")
    receipt_owner.assert_awaited_once_with(body.session, "source_schema")
    epoch_controls.assert_awaited_once_with(body.connection, body.publisher.policy)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError("synthetic competing writer"), asyncio.CancelledError()])
async def test_fixed_control_fence_authenticates_before_ordered_nowait_locks(monkeypatch, failure):
    events = []
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=True))
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="source_schema"))

    async def authenticate(supplied):
        assert supplied is database
        events.append("authenticated")
        return publisher

    async def lock(statement):
        events.append(statement)
        if failure is not None:
            raise failure

    database.status.side_effect = lock
    monkeypatch.setattr(policy, "require_source_publication_transaction", authenticate)
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    if failure is None:
        await fhir._lock_source_publication_controls()
        assert 'provider_directory_api_endpoint" IN ROW EXCLUSIVE MODE NOWAIT' in events[1]
        assert 'provider_directory_source" IN ROW EXCLUSIVE MODE NOWAIT' in events[2]
        database.scalar.assert_awaited_once()
        assert database.scalar.await_args.kwargs == {
            "source_ref": '"source_schema"."provider_directory_source"',
            "endpoint_ref": '"source_schema"."provider_directory_api_endpoint"',
        }
    else:
        with pytest.raises(type(failure)):
            await fhir._lock_source_publication_controls()
        assert len(events) == 2
        database.scalar.assert_not_awaited()
    assert events[0] == "authenticated"


@pytest.mark.asyncio
@pytest.mark.parametrize("ready", [False, None, 1])
async def test_control_fence_refuses_missing_or_bypassable_native_alias_guard(monkeypatch, ready):
    database = SimpleNamespace(status=AsyncMock(), scalar=AsyncMock(return_value=ready))
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    monkeypatch.setattr(
        policy,
        "require_source_publication_transaction",
        AsyncMock(return_value=SimpleNamespace(policy=SimpleNamespace(schema="source_schema"))),
    )
    with pytest.raises(RuntimeError, match="publication_alias_fence_changed"):
        await fhir._lock_source_publication_controls()
    assert database.status.await_count == 2


@pytest.mark.asyncio
async def test_control_fence_refuses_foreign_schema_before_locks(monkeypatch):
    database = SimpleNamespace(status=AsyncMock())
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(fhir, "_schema", lambda: "source_schema")
    monkeypatch.setattr(
        policy,
        "require_source_publication_transaction",
        AsyncMock(return_value=SimpleNamespace(policy=SimpleNamespace(schema="foreign_schema"))),
    )
    with pytest.raises(RuntimeError, match="publication_schema_changed"):
        await fhir._lock_source_publication_controls()
    database.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("source_mode", [False, True])
async def test_final_scope_fence_keeps_legacy_path_and_orders_source_locks(monkeypatch, source_mode):
    events = []
    directory = SimpleNamespace(
        _lock_source_publication_controls=AsyncMock(side_effect=lambda: events.append("controls")),
        _lock_and_verify_artifact_dataset_fence=AsyncMock(side_effect=lambda _: events.append("scope")),
    )
    fence = SimpleNamespace(datasets=(object(),))
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: source_mode)
    await publication._lock_retained_relations(directory, object(), fence)
    assert events == (["controls", "scope"] if source_mode else ["scope"])


@pytest.mark.asyncio
async def test_busy_control_fence_never_reaches_current_scope(monkeypatch):
    directory = SimpleNamespace(
        _lock_source_publication_controls=AsyncMock(side_effect=RuntimeError("synthetic busy")),
        _lock_and_verify_artifact_dataset_fence=AsyncMock(),
    )
    monkeypatch.setattr(policy, "is_source_storage_required", lambda: True)
    with pytest.raises(RuntimeError, match="synthetic busy"):
        await publication._lock_retained_relations(directory, object(), SimpleNamespace(datasets=(object(),)))
    directory._lock_and_verify_artifact_dataset_fence.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_local_callback_finishes_before_short_control_fence(monkeypatch):
    events = []
    candidate = SimpleNamespace(dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root")
    dataset = SimpleNamespace(
        source_id="synthetic-source", dataset_id="dataset", endpoint_id="endpoint", evidence_run_id="root"
    )
    fence = SimpleNamespace(datasets=(dataset,), promotion_datasets=(dataset,))

    @asynccontextmanager
    async def transaction():
        yield object()

    directory = SimpleNamespace(
        db=SimpleNamespace(transaction=transaction, status=AsyncMock()),
        _resolve_provider_directory_artifact_datasets=AsyncMock(return_value=fence),
        _provider_directory_artifact_transaction_timeout_seconds=lambda _: 1,
        PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT="100ms",
        PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT="1s",
        _lock_source_publication_controls=AsyncMock(side_effect=lambda: events.append("controls")),
        _lock_artifact_cutover_fence=AsyncMock(side_effect=lambda _: events.append("scope")),
        _tighten_provider_directory_artifact_cutover_timeout=lambda *_: None,
        _promote_provider_directory_artifact_datasets=AsyncMock(side_effect=lambda _: events.append("promote")),
    )
    before = AsyncMock(side_effect=lambda _: events.append("prepared"))
    await local._publish_validated_source_local_dataset_owner(
        directory, fence, before_cutover=before, before_cutover_timeout_seconds=1
    )
    assert events == ["prepared", "controls", "scope", "promote"]


@pytest.mark.asyncio
async def test_plain_rc_intake_owner_has_no_issued_final_origin(owner_body):
    async with owner_body(final=False) as body:
        with pytest.raises(GenericCandidateError, match="publication_transaction_changed"):
            await policy.require_source_publication_transaction(body.database)


@pytest.mark.asyncio
async def test_nested_owner_and_publisher_bodies_preserve_the_issued_origin(owner_body):
    async with owner_body() as body:
        original = policy._OWNER.get()
        async with policy.source_publication_body(body.database):
            async with policy.source_transaction(body.database):
                assert policy._OWNER.get() is original
            async with policy.source_publisher_body(body.session):
                assert policy._OWNER.get() is None and body.native.actor == "source_publisher"
            assert policy._OWNER.get() is original
        assert policy._OWNER.get() is original
        assert await policy.require_source_publication_transaction(body.database) is body.publisher


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["pid", "xid"])
async def test_clean_exit_rejects_a_replaced_native_transaction(owner_body, changed):
    with pytest.raises(GenericCandidateError, match="publication_transaction_changed"):
        async with owner_body() as body:
            setattr(body.native, changed, 4321 if changed == "pid" else "43")
    assert policy._OWNER.get() is None


@pytest.mark.asyncio
async def test_nested_final_body_cannot_reissue_a_changed_origin(owner_body):
    async with owner_body() as body:
        body.native.xid = "43"
        with pytest.raises(GenericCandidateError, match="publication_transaction_changed"):
            async with policy.source_publication_body(body.database):
                pytest.fail("a replacement top XID was accepted")
        body.native.xid = "42"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("synthetic final error"), asyncio.CancelledError()])
async def test_original_failure_is_not_masked_by_exit_identity_queries(owner_body, failure):
    with pytest.raises(type(failure)) as caught:
        async with owner_body() as body:
            body.native.xid = "43"
            raise failure
    assert caught.value is failure and policy._OWNER.get() is None


def _cutover_directory(database, observed_events, fence, prove_frozen_cutover):
    """Supply only the local cutover callbacks for the real Source binding test."""
    return SimpleNamespace(
        db=database,
        LOGGER=SimpleNamespace(warning=Mock()),
        _resolve_provider_directory_artifact_datasets=AsyncMock(return_value=fence),
        _provider_directory_artifact_transaction_timeout_seconds=lambda _: 1,
        PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT="100ms",
        PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_STATEMENT_TIMEOUT="1s",
        _lock_source_publication_controls=AsyncMock(),
        _lock_artifact_cutover_fence=AsyncMock(),
        _tighten_provider_directory_artifact_cutover_timeout=lambda *_: None,
        _promote_provider_directory_artifact_datasets=AsyncMock(
            side_effect=lambda _: observed_events.append("promote")
        ),
        _is_provider_directory_dataset_cutover_committed=AsyncMock(side_effect=prove_frozen_cutover),
    )


def _cutover_session(sessions, isolation):
    """Expose the original actor, backend and transaction observations for one owner."""
    driver, _ = _connection()
    actor = SimpleNamespace(current="source_publisher")
    driver.get_server_pid = lambda: 1234 + len(sessions)
    driver.fetchval.side_effect = lambda sql: isolation if sql == "SHOW transaction_isolation" else str(len(sessions))
    driver.fetchrow.side_effect = lambda _: {"session": "source_publisher", "current": actor.current}
    driver.execute.side_effect = lambda sql: setattr(
        actor, "current", "source_owner" if sql.startswith("SET LOCAL ROLE") else "source_publisher"
    )
    session = SimpleNamespace(driver=driver)
    return session


@asynccontextmanager
async def _cutover_factory(monkeypatch, *, committed, proof_error=None):
    """Model lost outer COMMIT acknowledgement around the actual Source owner contexts."""
    binding = ContextVar("cutover_test_binding", default=None)
    database = SimpleNamespace(_transaction_binding=binding.get, status=AsyncMock())
    observed_events = []
    sessions = []
    original_error = RuntimeError("synthetic final commit acknowledgement")
    publisher_policy = _policy()
    monkeypatch.setattr(policy, "storage_roles", AsyncMock())
    monkeypatch.setattr(policy, "_copy_driver", AsyncMock(side_effect=lambda session: session.driver))

    @asynccontextmanager
    async def transaction(requested_database, *, isolation):
        assert requested_database is database and isolation == "read committed"
        session = _cutover_session(sessions, isolation)
        driver = session.driver
        sessions.append(session)
        token = binding.set(SimpleNamespace(session=session))
        try:
            async with policy.bind_generic_source_publisher(
                driver, publisher_policy, verify_role_profile=AsyncMock(), transaction_isolation=isolation
            ):
                yield session
            observed_events.append("outer_commit")
            if len(sessions) == 1:
                raise original_error
        finally:
            binding.reset(token)

    @asynccontextmanager
    async def savepoint():
        assert policy.source_copy_session() is binding.get().session
        yield binding.get().session

    database.transaction = savepoint
    candidate = SimpleNamespace(dataset_id="dataset", endpoint_id="endpoint", acquisition_root_run_id="root")
    dataset = SimpleNamespace(
        source_id="synthetic-source", dataset_id="dataset", endpoint_id="endpoint", evidence_run_id="root"
    )
    fence = SimpleNamespace(datasets=(dataset,), promotion_datasets=(dataset,))

    async def prove_frozen_cutover(supplied):
        assert supplied is fence and observed_events == ["promote", "outer_commit"]
        assert len(sessions) == 2 and policy.source_copy_session() is sessions[1]
        assert policy.bound_publisher().connection is sessions[1].driver
        assert sessions[1].driver is not sessions[0].driver
        observed_events.append("proof")
        if proof_error is not None:
            raise proof_error
        return committed

    directory = _cutover_directory(database, observed_events, fence, prove_frozen_cutover)
    async with policy.bind_generic_source_transaction_factory(SimpleNamespace(transaction=transaction)):
        yield directory, candidate, original_error, observed_events


@pytest.mark.asyncio
@pytest.mark.parametrize("proof", ("committed", "not_committed", "verification_failed"))
async def test_lost_outer_source_commit_acknowledgement_requires_fresh_exact_proof(monkeypatch, proof):
    async with _cutover_factory(
        monkeypatch,
        committed=proof == "committed",
        proof_error=RuntimeError("synthetic unavailable proof") if proof == "verification_failed" else None,
    ) as (directory, candidate, original_error, observed_events):
        if proof == "committed":
            await local.publish_validated_source_local_dataset(directory, candidate, "synthetic-source")
        else:
            with pytest.raises(RuntimeError) as caught:
                await local.publish_validated_source_local_dataset(directory, candidate, "synthetic-source")
            assert caught.value is original_error
        assert observed_events[:3] == ["promote", "outer_commit", "proof"]
        directory._is_provider_directory_dataset_cutover_committed.assert_awaited_once()
    assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None


@pytest.mark.asyncio
async def test_cancelled_source_cutover_never_reconciles_as_success(monkeypatch):
    async with _cutover_factory(monkeypatch, committed=True) as (directory, candidate, _, observed_events):
        after = AsyncMock(side_effect=asyncio.CancelledError())
        with pytest.raises(asyncio.CancelledError):
            await local.publish_validated_source_local_dataset(
                directory, candidate, "synthetic-source", after_promotion=after
            )
        assert observed_events == ["promote"]
        directory._is_provider_directory_dataset_cutover_committed.assert_not_awaited()
    assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None
