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
from process.network_membership_copy import MembershipCopyTarget


def _copy_target():
    return MembershipCopyTarget(
        *(str(UUID(int=identifier_int)) for identifier_int in range(1, 5)), "network_candidate_" + UUID(int=4).hex
    )


def _candidate_record(target, state, *, source_generations=None, retention=None):
    return {
        **{field: getattr(target, field) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id")},
        "schema_name": target.schema_name,
        "state": state,
        "source_generations": {} if source_generations is None else source_generations,
        "validation_json": {} if retention is None else {composition.RETENTION_KEY: retention},
        "expected_rows": 0 if retention is None else retention["effective_rows"],
    }


@asynccontextmanager
async def _completion_transaction(**options):
    assert options == {"isolation": "repeatable_read"}
    yield


def _attach_completion_driver(driver, events):
    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read"} and not driver.is_in_transaction()
        driver.has_transaction = True
        events.append("completion_begin")
        try:
            yield
        finally:
            driver.has_transaction = False

    async def locked_candidate(sql, candidate_id):
        assert driver.is_in_transaction()
        assert sql == 'SELECT * FROM "synthetic_control".network_membership_candidate WHERE candidate_id=$1 FOR UPDATE'
        assert str(candidate_id) == driver.candidate_record["candidate_id"]
        return driver.candidate_record

    driver.transaction = transaction
    driver.fetchrow = AsyncMock(side_effect=locked_candidate)


def _bridge_owner(monkeypatch, failure=None):
    events = []
    engine = create_async_engine("postgresql+asyncpg://synthetic@localhost/synthetic", pool_size=1, max_overflow=0)
    driver = SimpleNamespace(has_transaction=False, execute=AsyncMock())
    driver.is_in_transaction = lambda: driver.has_transaction

    _attach_completion_driver(driver, events)
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
        connection.sync_connection.begin()
        return failure != operation

    original_fence = fence.require_registry_company_approval_fence

    async def require_fence(session, control_schema, **options):
        await original_fence(session, control_schema, **options)
        driver.has_transaction = True
        events.append("fence")
        if failure == "fence":
            raise ValueError("synthetic fence unavailable")

    monkeypatch.setattr(AsyncConnection, "start", start)
    monkeypatch.setattr(AsyncConnection, "execution_options", options)
    monkeypatch.setattr(AsyncConnection, "scalar", is_lock_success)
    monkeypatch.setattr(composition, "require_registry_company_approval_fence", require_fence)
    return async_sessionmaker(engine), driver, events


def _preparation(monkeypatch, driver, events, failure=None):
    """Keep socket-free source and command stages aligned with the preparation contract."""
    approved = SimpleNamespace(approved_revision=3, generation_id="a" * 64, total_rows=0)
    artifact = SimpleNamespace(address_source=object(), generation_sha256="b" * 64, runtime_roles=())
    stage = composition.RegistrySourceRecipeStage("synthetic_stage", 0, 0, "c" * 64)
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
        assert connection is driver and driver.is_in_transaction()
        assert events.index("prepare_commit") < events.index("unlock") < events.index("completion_begin")
        events.append("copy")
        if failure == "copy":
            raise RuntimeError("synthetic copy failure")
        if failure == "cancel":
            raise asyncio.CancelledError()

    async def create(connection, target, **options):
        assert connection is driver and driver.is_in_transaction()
        driver.candidate_record = _candidate_record(
            target, "open", source_generations=options["source_generations"], retention=options["source_retention"]
        )
        return driver.candidate_record

    monkeypatch.setattr(composition, "pin_approved_membership_source", pin)
    monkeypatch.setattr(composition, "resolve_registered_source_inventory", AsyncMock(return_value=None))
    monkeypatch.setattr(composition, "resolve_registered_office_summary_revision", AsyncMock(return_value=None))
    monkeypatch.setattr(composition, "require_registered_composition_target", AsyncMock())
    monkeypatch.setattr(
        composition, "_prepare_composition_sources_for_format", AsyncMock(return_value=(({}, 0, (), stage), None))
    )
    monkeypatch.setattr(composition, "_composition_retention", AsyncMock(return_value=(commands, retention_by_name)))
    monkeypatch.setattr(composition, "_composition_artifact", AsyncMock(return_value=(artifact, None)))
    monkeypatch.setattr(composition, "create_network_candidate", create)
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
        "writer_roles": {
            "owner_role": "synthetic_owner",
            "loader_roles": ("synthetic_loader",),
            "reader_roles": ("synthetic_reader",),
        },
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
    assert events.index("fence") < events.index("prepare") < events.index("prepare_commit")
    assert events.index("unlock") < events.index("copy") < events.index("close")
    assert "invalidate" not in events
    assert driver.execute.await_args_list == [
        (
            (
                "CREATE INDEX IF NOT EXISTS provider_location_binding_location_idx "
                f'ON "{target.schema_name}".provider_location_binding USING hash(location_key)',
            ),
            {},
        )
    ] + [
        ((f'DROP TABLE IF EXISTS pg_temp."{name}"',), {})
        for name in ("synthetic_stage", "synthetic_adds", "synthetic_removals")
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["prepare", "prepare_commit", "copy", "cancel", "unlock", "fence"])
async def test_original_failure_or_cancel_retires_real_owner_without_extra_checkout(monkeypatch, failure):
    sessions, driver, events = _bridge_owner(monkeypatch, failure)
    _preparation(monkeypatch, driver, events, failure)
    with pytest.raises((RuntimeError, ValueError, asyncio.CancelledError)):
        await composition.compose_registry_membership_candidate(sessions, **_arguments())
    assert events.count("checkout") == 1 and events[-1] == "invalidate"
    if failure in {"prepare", "prepare_commit", "fence"}:
        assert "unlock" not in events and "copy" not in events
    if failure == "unlock":
        assert "copy" not in events


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError("original copy failure"), asyncio.CancelledError()])
async def test_copy_error_keeps_priority_over_stage_cleanup(monkeypatch, failure):
    target = _copy_target()
    driver = SimpleNamespace(
        transaction=_completion_transaction,
        fetchrow=AsyncMock(return_value=_candidate_record(target, "open")),
        execute=AsyncMock(side_effect=RuntimeError("cleanup failed")),
    )
    prepared = (
        target,
        object(),
        "open",
        composition.RegistrySourceRecipeStage("synthetic_stage", 0, 0, "c" * 64),
        SimpleNamespace(add_table="synthetic_adds", remove_table="synthetic_removals"),
        _arguments()["writer_roles"],
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
    copy_target = _copy_target()
    driver.fetchrow = AsyncMock(return_value=_candidate_record(copy_target, "sealed"))
    prepared = (
        copy_target,
        object(),
        "sealed",
        composition.RegistrySourceRecipeStage("synthetic_stage", 0, 0, "c" * 64),
        SimpleNamespace(add_table="synthetic_adds", remove_table="synthetic_removals"),
        _arguments()["writer_roles"],
    )
    snapshot = AsyncMock(return_value=prepared)
    seal = AsyncMock()
    monkeypatch.setattr(composition, "_prepare_composition_snapshot", snapshot)
    monkeypatch.setattr(composition, "seal_network_candidate", seal)
    actual = await composition.compose_registry_membership_candidate(driver, **_arguments())
    assert actual == prepared[:2] and events == ["begin", "commit", "begin", "commit"]
    assert snapshot.await_args.args[0] is driver
    seal.assert_awaited_once_with(
        driver, prepared[0], control_schema="synthetic_control", source_stage=prepared[3], command_stage=prepared[4]
    )


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
@pytest.mark.parametrize("state", ["sealed", "validated", "ready", "published"])
async def test_closed_composition_resumes_without_recopying_terminal_data(monkeypatch, state):
    target = _copy_target()
    driver = SimpleNamespace(
        transaction=_completion_transaction,
        fetchrow=AsyncMock(return_value=_candidate_record(target, state)),
        execute=AsyncMock(),
    )
    stage = composition.RegistrySourceRecipeStage("source_stage", 0, 0, "c" * 64)
    commands = SimpleNamespace(add_table="adds", remove_table="removes")
    prepared = (target, object(), state, stage, commands, _arguments()["writer_roles"])
    copy, seal = AsyncMock(), AsyncMock()
    monkeypatch.setattr(composition, "_copy_composition", copy)
    monkeypatch.setattr(composition, "seal_network_candidate", seal)
    assert await composition._complete_composition(driver, prepared, None, "control") == prepared[:2]
    copy.assert_not_awaited()
    if state in {"sealed", "validated"}:
        seal.assert_awaited_once_with(
            driver, target, control_schema="control", source_stage=stage, command_stage=commands
        )
    else:
        seal.assert_not_awaited()
    driver.fetchrow.assert_awaited_once_with(
        'SELECT * FROM "control".network_membership_candidate WHERE candidate_id=$1 FOR UPDATE',
        UUID(target.candidate_id),
    )
    assert driver.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["prepared", "current"])
async def test_closed_composition_refuses_rejected_logical_state(monkeypatch, stage):
    target = _copy_target()
    driver = SimpleNamespace(
        transaction=_completion_transaction,
        fetchrow=AsyncMock(return_value=_candidate_record(target, "rejected" if stage == "current" else "ready")),
        execute=AsyncMock(),
    )
    prepared = (
        target,
        object(),
        "rejected" if stage == "prepared" else "ready",
        composition.RegistrySourceRecipeStage("source_stage", 0, 0, "c" * 64),
        SimpleNamespace(add_table="adds", remove_table="removes"),
        _arguments()["writer_roles"],
    )
    copy = AsyncMock()
    monkeypatch.setattr(composition, "_copy_composition", copy)
    with pytest.raises(composition.RegistryCompositionError, match="candidate_unavailable"):
        await composition._complete_composition(driver, prepared, None, "control")
    copy.assert_not_awaited()
    assert driver.execute.await_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "cancel", "preview"])
async def test_configured_api_preview_owns_genuine_fenced_reader_and_closes_on_failure(monkeypatch, failure):
    from api.endpoint import registry_publication as publication
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore
    from process.registry_ptg_scope_engine import RegistryPTGScopeEngineService

    sessions, driver, events = _bridge_owner(monkeypatch)
    monkeypatch.setattr(
        publication, "require_registry_company_approval_fence", composition.require_registry_company_approval_fence
    )
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
        assert events.index("lock") < events.index("fence")
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
