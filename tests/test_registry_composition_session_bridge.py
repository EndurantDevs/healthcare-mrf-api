# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exercise the real SQLAlchemy session owner with socket-free native I/O doubles."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker, create_async_engine

from process import registry_candidate_composition as composition
from process import registry_company_approval_fence as fence
from process.network_address_projection import PinnedAddressSource


def _bridge_owner(monkeypatch, failure=None):
    events = []
    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic", pool_size=1, max_overflow=0)
    driver = SimpleNamespace(has_transaction=False, execute=AsyncMock())
    driver.is_in_transaction = lambda: driver.has_transaction
    pooled = MagicMock()
    pooled.driver_connection, pooled.is_valid = driver, True

    def commit():
        phase = "prepare_commit" if driver.has_transaction else "connection_commit"
        events.append(phase)
        driver.has_transaction = False
        if failure == phase:
            raise RuntimeError("synthetic commit acknowledgement lost")

    pooled.commit.side_effect = commit
    pooled.rollback.side_effect = lambda: setattr(driver, "has_transaction", False)
    pooled.close.side_effect = lambda: events.append("close")
    pooled.invalidate.side_effect = lambda *args: events.append("invalidate")

    async def start(connection, **kwargs):
        events.append("checkout")
        connection.sync_connection = Connection(engine.sync_engine, connection=pooled)
        return connection

    async def options(connection, **options_by_name):
        assert not connection.in_transaction()
        events.append(options_by_name["isolation_level"])
        return connection

    async def is_lock_success(connection, statement, parameters):
        operation = "unlock" if "unlock" in str(statement) else "lock"
        events.append(operation)
        if failure == "driver_changed" and operation == "unlock":
            pooled.driver_connection = object()
        connection.sync_connection.begin()
        return failure != operation

    async def is_fence_held(session, statement, parameters):
        assert type(session) is AsyncSession
        assert "pg_backend_pid()" in str(statement) and "ShareLock" in str(statement)
        bound = await session.connection()
        assert (await bound.get_raw_connection()).driver_connection is driver
        driver.has_transaction = True
        events.append("native_fence")
        return failure != "native_fence"

    monkeypatch.setattr(AsyncConnection, "start", start)
    monkeypatch.setattr(AsyncConnection, "execution_options", options)
    monkeypatch.setattr(AsyncConnection, "scalar", is_lock_success)
    monkeypatch.setattr(AsyncSession, "scalar", is_fence_held)
    return async_sessionmaker(engine), driver, events


def _preparation(monkeypatch, driver, events, failure=None):
    """Keep socket-free source and command stages aligned with the preparation contract."""
    approved = SimpleNamespace(generation_id="a" * 64, total_rows=0)
    artifact = SimpleNamespace(address_source=object(), generation_sha256="b" * 64)
    stage = SimpleNamespace(table_name="synthetic_stage")
    commands = SimpleNamespace(add_table="synthetic_adds", remove_table="synthetic_removals")
    retention_by_name = {
        "component": "registry_source_retention",
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": "c" * 64,
        "source_rows": 0,
        "add_rows": 0,
        "remove_rows": 0,
        "suppressed_source_rows": 0,
        "effective_rows": 0,
    }

    async def pin(connection, **kwargs):
        assert connection is driver and driver.is_in_transaction()
        events.append("prepare")
        if failure == "prepare":
            raise RuntimeError("synthetic preparation failure")
        return approved

    async def copy(connection, *args):
        assert connection is driver and not driver.is_in_transaction()
        assert events.index("prepare_commit") < events.index("unlock") < len(events)
        events.append("copy")
        if failure == "copy":
            raise RuntimeError("synthetic copy failure")
        if failure == "cancel":
            raise asyncio.CancelledError()

    monkeypatch.setattr(composition, "pin_approved_membership_source", pin)
    monkeypatch.setattr(composition, "_composition_sources", AsyncMock(return_value=({}, 0, (), stage)))
    monkeypatch.setattr(composition, "_composition_retention", AsyncMock(return_value=(commands, retention_by_name)))
    monkeypatch.setattr(composition, "_composition_artifact", AsyncMock(return_value=(artifact, None)))
    monkeypatch.setattr(composition, "create_network_candidate", AsyncMock(return_value={"state": "open"}))
    monkeypatch.setattr(composition, "_bindings", AsyncMock())
    monkeypatch.setattr(composition, "_record_source_receipts", AsyncMock())
    monkeypatch.setattr(composition, "_copy_composition", copy)
    return artifact.address_source


def _arguments():
    return {
        "request_id": UUID("00000000-0000-0000-0000-000000000001"),
        "approved_revision": 3,
        "expected_head": 0,
        "address_sources": composition.RegistryCompositionAddressSources(
            PinnedAddressSource("synthetic_address", "entity_address_unified", "c" * 64)
        ),
        "writer_roles": {},
        "control_schema": "synthetic_control",
    }


@pytest.mark.asyncio
async def test_genuine_factory_prepares_on_fenced_session_then_copies_on_same_driver(monkeypatch):
    sessions, driver, events = _bridge_owner(monkeypatch)
    address = _preparation(monkeypatch, driver, events)
    target, actual_address = await composition.compose_registry_membership_candidate(sessions, **_arguments())
    assert actual_address is address and target.candidate_id
    assert events.count("checkout") == 1 and events.count("unlock") == 1
    assert events.index("AUTOCOMMIT") < events.index("lock") < events.index("REPEATABLE READ")
    assert events.index("native_fence") < events.index("prepare") < events.index("prepare_commit")
    assert events.index("unlock") < events.index("copy") < events.index("close")
    assert "invalidate" not in events
    assert driver.execute.await_args_list == [
        ((f'DROP TABLE IF EXISTS pg_temp."{name}"',), {})
        for name in ("synthetic_stage", "synthetic_adds", "synthetic_removals")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["prepare", "prepare_commit", "copy", "cancel", "unlock", "native_fence", "driver_changed"]
)
async def test_original_failure_or_cancel_retires_real_owner_without_extra_checkout(monkeypatch, failure):
    sessions, driver, events = _bridge_owner(monkeypatch, failure)
    _preparation(monkeypatch, driver, events, failure)
    with pytest.raises((RuntimeError, ValueError, asyncio.CancelledError)):
        await composition.compose_registry_membership_candidate(sessions, **_arguments())
    assert events.count("checkout") == 1 and events[-1] == "invalidate"
    if failure in {"prepare", "prepare_commit", "native_fence"}:
        assert "unlock" not in events and "copy" not in events
    if failure == "unlock":
        assert "copy" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("original copy failure"), asyncio.CancelledError()])
async def test_copy_error_keeps_priority_over_stage_cleanup(monkeypatch, failure):
    driver = SimpleNamespace(execute=AsyncMock(side_effect=RuntimeError("cleanup failed")))
    prepared = (
        object(),
        object(),
        "open",
        SimpleNamespace(table_name="synthetic_stage"),
        SimpleNamespace(add_table="synthetic_adds", remove_table="synthetic_removals"),
        {},
    )
    monkeypatch.setattr(composition, "_copy_composition", AsyncMock(side_effect=failure))
    with pytest.raises(type(failure)) as captured:
        await composition._complete_composition(driver, prepared, None, "synthetic_control")
    assert captured.value is failure


@pytest.mark.asyncio
async def test_raw_caller_keeps_original_owned_preparation_and_completion(monkeypatch):
    """Keep raw snapshot preparation and retained candidate completion separately owned."""
    events = []
    driver = SimpleNamespace(is_in_transaction=lambda: False, execute=AsyncMock())

    @asynccontextmanager
    async def transaction(**options_by_name):
        assert options_by_name == {"isolation": "repeatable_read"}
        events.append("begin")
        yield
        events.append("commit")

    driver.transaction = transaction
    prepared = (
        object(),
        object(),
        "sealed",
        SimpleNamespace(table_name="synthetic_stage"),
        SimpleNamespace(add_table="synthetic_adds", remove_table="synthetic_removals"),
        {},
    )
    snapshot = AsyncMock(return_value=prepared)
    seal, verify = AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "_prepare_composition_snapshot", snapshot)
    monkeypatch.setattr(composition, "seal_network_candidate", seal)
    monkeypatch.setattr(composition, "verify_network_candidate_writer_closure", verify)
    actual = await composition.compose_registry_membership_candidate(driver, **_arguments())
    assert actual == prepared[:2] and events == ["begin", "commit", "begin", "commit"]
    assert snapshot.await_args.args[0] is driver
    seal.assert_awaited_once_with(
        driver, prepared[0], control_schema="synthetic_control", source_stage=prepared[3], command_stage=prepared[4]
    )
    verify.assert_awaited_once_with(driver, prepared[0], control_schema="synthetic_control")


@pytest.mark.asyncio
async def test_detached_session_is_not_a_factory_or_raw_composition_owner():
    with pytest.raises(composition.RegistryCompositionError, match="phase_owned_transactions"):
        await composition.compose_registry_membership_candidate(AsyncSession(), **_arguments())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["copy", "cancel"])
async def test_original_owner_error_keeps_priority_over_connection_cleanup(monkeypatch, failure):
    sessions, driver, events = _bridge_owner(monkeypatch)
    _preparation(monkeypatch, driver, events, failure)
    monkeypatch.setattr(AsyncConnection, "close", AsyncMock(side_effect=RuntimeError("cleanup failed")))
    expected = asyncio.CancelledError if failure == "cancel" else RuntimeError
    with pytest.raises(expected) as captured:
        await composition.compose_registry_membership_candidate(sessions, **_arguments())
    if failure == "copy":
        assert str(captured.value) == "synthetic copy failure"
    assert "invalidate" in events and events.count("checkout") == 1


@pytest.mark.asyncio
async def test_factory_subclass_does_not_gain_the_exact_factory_authority():
    class OtherFactory(async_sessionmaker):
        __slots__ = ()

    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic", pool_size=1, max_overflow=0)
    with pytest.raises(ValueError, match="company_fence_unavailable"):
        await composition.compose_registry_membership_candidate(OtherFactory(engine), **_arguments())


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("custody", [None, {"seal_sha256": "a" * 64}, "changed"])
async def test_terminal_composition_uses_verified_custody_without_resealing(monkeypatch, state, custody):
    from process import network_candidate_custody

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read"}
        yield

    driver = SimpleNamespace(transaction=transaction, execute=AsyncMock())
    prepared = (
        object(),
        object(),
        state,
        SimpleNamespace(table_name="source_stage"),
        SimpleNamespace(add_table="adds", remove_table="removes"),
        {},
    )
    verify = AsyncMock(
        side_effect=ValueError("custody_changed") if custody == "changed" else None, return_value=custody
    )
    monkeypatch.setattr(network_candidate_custody, "verify_network_candidate_custody", verify)
    seal, legacy = AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "seal_network_candidate", seal)
    monkeypatch.setattr(composition, "verify_network_candidate_writer_closure", legacy)
    if custody == "changed":
        with pytest.raises(ValueError, match="custody_changed"):
            await composition._complete_composition(driver, prepared, None, "control")
    else:
        assert await composition._complete_composition(driver, prepared, None, "control") == prepared[:2]
    assert seal.await_count == legacy.await_count == int(custody is None)
    assert verify.await_count == 1 and driver.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["ready", "published"])
@pytest.mark.parametrize("missing", ["table", "row"])
async def test_terminal_legacy_replay_uses_real_missing_custody_boundary(monkeypatch, state, missing):
    from process.network_membership_copy import MembershipCopyTarget

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read"}
        yield

    copy_target = MembershipCopyTarget(
        *(str(UUID(int=identifier_int)) for identifier_int in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )
    driver = SimpleNamespace(
        transaction=transaction,
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(return_value=None if missing == "table" else 42),
        fetchrow=AsyncMock(return_value=None),
        execute=AsyncMock(),
    )
    source_stage = SimpleNamespace(table_name="source_stage")
    commands = SimpleNamespace(add_table="adds", remove_table="removes")
    prepared = (copy_target, object(), state, source_stage, commands, {})
    seal, legacy = AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "seal_network_candidate", seal)
    monkeypatch.setattr(composition, "verify_network_candidate_writer_closure", legacy)

    assert await composition._complete_composition(driver, prepared, None, "control") == prepared[:2]
    driver.fetchval.assert_awaited_once_with("SELECT to_regclass($1)::oid", '"control".network_candidate_custody')
    assert driver.fetchrow.await_count == int(missing == "row")
    seal.assert_awaited_once_with(
        driver, copy_target, control_schema="control", source_stage=source_stage, command_stage=commands
    )
    legacy.assert_awaited_once_with(driver, copy_target, control_schema="control")
    assert driver.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cancel", "preview", "identity", "role"])
async def test_configured_api_preview_owns_genuine_fenced_reader_and_closes_on_failure(monkeypatch, failure):
    from api.endpoint import registry_publication as publication
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from process.registry_ptg_scope_engine import RegistryPTGScopeEngineService

    sessions, driver, events = _bridge_owner(monkeypatch)
    identity_by_name = dict(
        database_name="synthetic",
        database_oid=1,
        server_address=None,
        server_port=None,
        server_started="same-start",
        control_oid=2,
        current_role="api",
        session_role="api",
    )
    source_identity_by_name = {**identity_by_name, "current_role": "synthetic", "session_role": "synthetic"}
    if failure == "identity":
        source_identity_by_name["database_oid"] = 3
    if failure == "role":
        source_identity_by_name["current_role"] = "assumed"
    driver.fetchrow = AsyncMock(side_effect=[identity_by_name, source_identity_by_name])
    service = object.__new__(RegistryPTGScopeEngineService)
    object.__setattr__(service, "reader_sessions", sessions)
    object.__setattr__(service, "store", RegistryPTGProducerScopeStore("owner", "approver", "synthetic_control"))
    monkeypatch.setattr(publication, "_namespace", lambda _: '"synthetic_control"')
    monkeypatch.setattr(publication, "_driver", AsyncMock(return_value=driver))

    @asynccontextmanager
    async def api_transaction():
        yield

    async def preview(connection, command, actor, expected, authority):
        assert connection is driver and expected == 7
        assert type(authority[0]) is AsyncSession
        assert authority[0].in_transaction() and authority[1] is service.store
        assert events.index("lock") < events.index("native_fence")
        if failure == "cancel":
            raise asyncio.CancelledError()
        if failure == "preview":
            raise RuntimeError("synthetic preview failure")
        return {"source_selection": {"status": "available"}}

    observed = AsyncMock(side_effect=preview)
    monkeypatch.setattr(publication, "_checked_preview", observed)
    request = SimpleNamespace(ctx=SimpleNamespace(sa_session=SimpleNamespace(begin=api_transaction)))
    if failure:
        with pytest.raises((asyncio.CancelledError, RuntimeError)):
            await publication._configured_preview(request, service, object(), object(), 7)
        assert events[-1] == "invalidate"
        if failure in {"identity", "role"}:
            observed.assert_not_called()
    else:
        assert await publication._configured_preview(request, service, object(), object(), 7) == {
            "source_selection": {"status": "available"}
        }
        assert events[-1] == "close" and events.count("unlock") == 1
    assert events.count("checkout") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError])
async def test_configured_preview_timeout_is_sanitized_and_cancellation_propagates(monkeypatch, failure):
    from api.endpoint import registry_publication as publication

    monkeypatch.setattr(publication, "require_registry_auth", lambda _: None)
    monkeypatch.setattr(publication, "_command", lambda _: (object(), object(), 7, "session"))
    preview = AsyncMock(side_effect=failure("synthetic private transport detail"))
    monkeypatch.setattr(publication, "_configured_preview", preview)
    request = SimpleNamespace(app=SimpleNamespace(ctx=SimpleNamespace(registry_ptg_scope_engine=object())))

    if failure is asyncio.CancelledError:
        with pytest.raises(asyncio.CancelledError):
            await publication.request_publication(request, "preview")
    else:
        result = await publication.request_publication(request, "preview")
        assert result.status == 503
        assert b"synthetic private transport detail" not in result.body
    assert preview.await_count == 1
