# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Consumer fences and retry protocols with explicit external database doubles."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import asyncpg
import pytest

from process import network_cms_registry_address_capture as native_receipts
from process import registry_aca_global_epoch as global_epochs
from process import registry_aca_source_consumer as aca
from process import registry_aca_source_notifications as notices
from process import registry_ptg_source_consumer as ptg
from process.network_approved_membership_source import ApprovedMembershipSourceError
from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
from tests.test_registry_aca_global_epoch import _global_prepared
from tests.test_registry_aca_source_consumer import config as aca_config
from tests.test_registry_aca_source_notifications import _fixture, _store
from tests.test_registry_aca_source_recipe import _ROLES
from tests.test_registry_ptg_source_consumer import config as ptg_config


class _ReaderPool:
    """Own only acquisition/transaction lifecycle around the existing catalog double."""

    def __init__(self, connection):
        self.connection = connection
        self.events = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    @asynccontextmanager
    async def acquire(self):
        """Record release even when receipt decoding or requalification fails."""
        self.events.append("acquire")
        try:
            yield self
        finally:
            self.events.append("release")

    @asynccontextmanager
    async def transaction(self, **options):
        """Require the exact read-only snapshot requested by the real consumer."""
        assert options == {"isolation": "repeatable_read", "readonly": True}
        self.events.append("begin")
        try:
            yield self
        finally:
            self.events.append("end")


async def _notification_case(monkeypatch, *, global_epoch=False):
    """Retain real store encoding/decoding; only native qualification IO is doubled."""
    session, connection, subject, verifier = await _fixture(monkeypatch)
    if global_epoch:
        prepared, validation = await _global_prepared(monkeypatch)
        subject = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
        verifier = AsyncMock(return_value=subject)
        monkeypatch.setattr(notices, "require_registry_aca_global_epoch", verifier)
    store = _store()
    accepted = await store.append(session, subject)
    connection.writing, connection.actor = False, "reader"
    identity = await store.qualify(connection, writing=False)
    monkeypatch.setattr(notices, "native_driver", native_receipts.native_driver)
    verifier.reset_mock()
    return SimpleNamespace(
        connection=connection,
        pool=_ReaderPool(connection),
        store=store,
        subject=subject,
        event=accepted["event"],
        identity=identity,
        verifier=verifier,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("global_epoch", [False, True])
async def test_notification_read_preserves_exact_typed_subject_and_readonly_snapshot(monkeypatch, global_epoch):
    case = await _notification_case(monkeypatch, global_epoch=global_epoch)
    result = await aca._notice(case.pool, case.store, case.identity, case.event)
    assert result == case.subject
    assert type(result) is type(case.subject)
    assert case.pool.events == ["acquire", "begin", "end", "release"]
    assert len(case.connection.statements) == len(case.connection.rows) == 1
    case.verifier.assert_awaited_once()
    assert case.verifier.await_args.args[0].native_connection is case.pool
    assert case.verifier.await_args.kwargs == {"runtime_roles": _ROLES}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "recipe", "notification", "schema"])
async def test_notification_foreign_event_or_catalog_row_cannot_escape_consumer(monkeypatch, damage):
    case = await _notification_case(monkeypatch)
    event_by_name = dict(case.event)
    if damage == "missing":
        event_by_name["capture_id"] = str(UUID(int=99))
    elif damage in {"recipe", "notification"}:
        event_by_name[damage + "_sha256"] = "f" * 64
    else:
        next(iter(case.connection.rows.values()))["source_schema_oid"] += 1
    with pytest.raises(ValueError, match="registry_aca_source_notification_invalid"):
        await aca._notice(case.pool, case.store, case.identity, event_by_name)
    assert case.pool.events == ["acquire", "begin", "end", "release"]
    case.verifier.assert_not_awaited()
    assert len(case.connection.statements) == len(case.connection.rows) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before", "after", "cancel"])
async def test_notification_identity_fence_and_cancellation_always_release_reader(monkeypatch, phase):
    case = await _notification_case(monkeypatch)

    async def admitted_subject(*args, **kwargs):
        if phase == "cancel":
            raise asyncio.CancelledError
        case.connection.identity = {**case.connection.identity, "database_oid": 99}
        return case.subject

    if phase == "before":
        case.identity = {**case.identity, "database_oid": 99}
    else:
        case.verifier.side_effect = admitted_subject
    expected_error = asyncio.CancelledError if phase == "cancel" else ValueError
    with pytest.raises(expected_error):
        await aca._notice(case.pool, case.store, case.identity, case.event)
    assert case.verifier.await_count == int(phase != "before")
    assert case.pool.events == ["acquire", "begin", "end", "release"]
    assert len(case.connection.statements) == len(case.connection.rows) == 1


async def _source_fence_case(monkeypatch, *, global_epoch):
    """Isolate native metadata IO while executing the complete consumer fence."""
    case = await _notification_case(monkeypatch, global_epoch=global_epoch)
    policy = aca.source_policy.GenericSourcePolicy("mrf", "owner", "publisher", ("source_reader",))
    publisher = SimpleNamespace(policy=policy, transaction_isolation="read committed")
    phase = SimpleNamespace(events=[], publisher=publisher)
    session = object()

    @asynccontextmanager
    async def source_transaction(database, **options):
        assert database is aca.db and options == {"isolation": "read committed"}
        phase.events.append("source_begin")
        try:
            yield session
        finally:
            phase.events.append("source_end")

    async def lock_source_family(actual_session, schema):
        assert actual_session is session and schema == "mrf"
        assert phase.events == ["source_begin"]
        phase.events.append("source_fence")

    family_lock = AsyncMock(side_effect=lock_source_family)
    monkeypatch.setattr(aca, "_lock_current_source_family", family_lock)
    monkeypatch.setattr(aca.source_policy, "source_transaction", source_transaction)
    monkeypatch.setattr(aca.source_policy, "bound_publisher", lambda: phase.publisher)
    prepared = case.subject.prepared if global_epoch else case.subject.source_pin.prepared
    capture = AsyncMock(return_value=prepared.manifest.source_metadata)
    capture_name = "_current_global_metadata" if global_epoch else "capture_registry_aca_source_metadata"
    monkeypatch.setattr(aca, capture_name, capture)
    configuration = (
        aca.RegistryACASourceConsumerConfig("registry_control", "executor", "loading_owner", global_epoch=True)
        if global_epoch
        else aca_config()
    )
    return SimpleNamespace(
        subject=case.subject,
        config=replace(configuration, source_authority_roles=aca.source_authority_roles(policy)),
        phase=phase,
        capture=capture,
        family_lock=family_lock,
        session=session,
        prepared=prepared,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("global_epoch", [False, True])
@pytest.mark.parametrize("outcome", ["success", "stale", "authority", "rebind", "cancel"])
async def test_current_source_fence_refuses_drift_and_unwinds_before_return(monkeypatch, global_epoch, outcome):
    case = await _source_fence_case(monkeypatch, global_epoch=global_epoch)
    configuration = case.config
    if outcome == "stale":
        case.capture.return_value = {"foreign_publication": True}
    elif outcome == "authority":
        configuration = replace(configuration, source_authority_roles=("source_reader",))

    async def enter_fence():
        async with aca.current_aca_source_fence(case.subject, configuration):
            case.phase.events.append("body")
            if outcome == "rebind":
                case.phase.publisher = SimpleNamespace(policy=case.phase.publisher.policy)
            elif outcome == "cancel":
                raise asyncio.CancelledError

    if outcome == "success":
        await enter_fence()
    else:
        expected_error = asyncio.CancelledError if outcome == "cancel" else ValueError
        with pytest.raises(expected_error):
            await enter_fence()
    expected_events = ["source_begin"]
    if outcome != "authority":
        expected_events.append("source_fence")
    if outcome not in {"stale", "authority"}:
        expected_events.append("body")
    expected_events.append("source_end")
    assert case.phase.events == expected_events
    assert case.family_lock.await_count == int(outcome != "authority")
    assert case.capture.await_count == int(outcome != "authority")
    if outcome == "authority":
        return
    if global_epoch:
        assert case.capture.await_args.args[:2] == (case.session, case.subject)
    else:
        metadata = dict(case.prepared.manifest.source_metadata)
        metadata.pop("aca_completed_publication")
        case.capture.assert_awaited_once_with(
            case.session, metadata, schema="mrf", runtime_roles=_ROLES, dataset_id=case.prepared.ownership.dataset_id
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("active_transaction", [False, True])
async def test_ptg_serialization_retry_requires_finished_transaction(monkeypatch, active_transaction):
    connection = SimpleNamespace(is_in_transaction=lambda: active_transaction)
    configuration = ptg_config()
    attempt = AsyncMock(side_effect=asyncpg.SerializationError("serialization conflict"))
    monkeypatch.setattr(ptg, "_attempt", attempt)
    arguments = (connection, None, None, "office_reader", {}, {}, object(), configuration)
    if active_transaction:
        with pytest.raises(asyncpg.SerializationError):
            await ptg._retry_source_attempt(*arguments)
    else:
        assert await ptg._retry_source_attempt(*arguments) == {"state": "conflict"}
    assert attempt.await_count == (1 if active_transaction else configuration.max_attempts)
    assert all(call.args == arguments for call in attempt.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("retryable", [False, True])
async def test_ptg_only_retries_exact_approved_revision_conflict(monkeypatch, retryable):
    connection = SimpleNamespace(is_in_transaction=lambda: False)
    configuration = ptg_config()
    reason = "Approved source revision no longer matches the control pin" if retryable else "foreign source identity"
    attempt = AsyncMock(side_effect=[ApprovedMembershipSourceError(reason), {"state": "completed"}])
    monkeypatch.setattr(ptg, "_attempt", attempt)
    arguments = (connection, None, None, "office_reader", {}, {}, object(), configuration)
    if retryable:
        assert await ptg._retry_source_attempt(*arguments) == {"state": "completed"}
    else:
        with pytest.raises(ApprovedMembershipSourceError, match=reason):
            await ptg._retry_source_attempt(*arguments)
    assert attempt.await_count == (2 if retryable else 1)


@pytest.mark.asyncio
async def test_global_fence_recaptures_actual_serving_generation(monkeypatch):
    from process import reference_family_result_generation as generations
    from tests.test_registry_aca_global_epoch import _publication

    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    serving = generations.validate_reference_family_serving_generation(_publication()["serving_generation"])
    observed = replace(serving, origin_generation=serving.origin_generation + 1)
    capture_serving = AsyncMock(return_value=observed)
    capture_metadata = AsyncMock(return_value={"observed": True})
    monkeypatch.setattr(generations, "capture_reference_family_serving_generation", capture_serving)
    monkeypatch.setattr(global_epochs, "capture_registry_aca_global_metadata", capture_metadata)
    session = object()
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="mrf"))
    assert await aca._current_global_metadata(session, epoch, publisher) == {"observed": True}
    capture_serving.assert_awaited_once_with(session, importer_id="mrf", schema_name="mrf")
    capture_metadata.assert_awaited_once_with(
        session,
        {
            "contract": global_epochs.PUBLICATION_CONTRACT,
            "importer_id": "mrf",
            "serving_generation": observed.as_dict(),
        },
        schema="mrf",
        runtime_roles=_ROLES,
    )
    assert prepared.manifest.source_serving_generation == serving


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing", "foreign_prepared", "foreign_roles", "noncanonical"])
async def test_retained_global_stamp_refuses_foreign_identity_before_native_io(monkeypatch, damage):
    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    encoded = global_epochs._PREFIX + global_epochs._canonical_json(epoch.as_dict()).decode()
    roles = _ROLES
    if damage == "missing":
        encoded = None
    elif damage == "foreign_prepared":
        prepared = replace(prepared, ownership=replace(prepared.ownership, auxiliary_oid=999))
    elif damage == "foreign_roles":
        roles = ("foreign_reader",)
    else:
        encoded += " "
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=encoded))
    native_custody = AsyncMock(side_effect=AssertionError("foreign receipt reached native admission"))
    monkeypatch.setattr(global_epochs, "_native_epoch", native_custody)
    with pytest.raises(ValueError, match="registry_aca_global_epoch_invalid"):
        await global_epochs.require_registry_aca_global_epoch(session, prepared, runtime_roles=roles)
    native_custody.assert_not_awaited()
