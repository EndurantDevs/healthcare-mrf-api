# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Original consumer actors, closed completion proofs and bounded failure handling."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker

from process import registry_aca_global_epoch as global_epochs
from process import registry_aca_source_consumer as aca
from process import registry_cms_source_consumer as cms
from process.mrf_address_publication import REFERENCES
from process.network_approved_membership_source import ApprovedMembershipSourceError
from process.network_membership_publication import NetworkPublicationError
from process.registry_cms_source_notifications import RegistryCMSSourceNotificationStore
from process.registry_publication_ptg_runtime import RegistryPublicationPTGRuntime
from tests.test_registry_aca_global_epoch import _global_completion, _publication
from tests.test_registry_aca_source_recipe import _ROLES
from tests.test_registry_cms_source_consumer import _config, _notice
from tests.test_registry_cms_source_notifications import _attempt as source_attempt
from tests.test_registry_source_publication_failures import _notification_case


class _ReceiptSessions:
    """Model external receipt IO and record the real consumer's snapshot lifetime."""

    def __init__(self):
        self.events = []

    @asynccontextmanager
    async def __call__(self):
        """Record session closure after success, failed proof or cancellation."""
        self.events.append("open")
        try:
            yield self
        finally:
            self.events.append("close")

    @asynccontextmanager
    async def begin(self):
        """Keep each source receipt read in its own bounded transaction."""
        self.events.append("begin")
        try:
            yield self
        finally:
            self.events.append("end")

    async def execute(self, statement):
        """Refuse a consumer that drops its read-only repeatable-read requirement."""
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
        self.events.append("readonly")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["digest", "cancel", "custody"])
async def test_cms_receipt_failure_closes_snapshot_without_returning_notice(failure):
    sessions = _ReceiptSessions()
    notice = _notice()
    store = SimpleNamespace(read=AsyncMock(return_value=notice))
    if failure == "cancel":
        store.read.side_effect = asyncio.CancelledError
    elif failure == "custody":
        store.read.side_effect = ValueError("source custody changed")
    expected_error = asyncio.CancelledError if failure == "cancel" else ValueError
    with pytest.raises(expected_error):
        await cms._notification(sessions, store, "source_schema", "receipt", "f" * 64)
    store.read.assert_awaited_once_with(sessions, source_schema="source_schema", receipt_id="receipt")
    assert sessions.events == ["open", "begin", "readonly", "end", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["runtime", "coordinates", "stale", "approval", "publication"])
async def test_cms_rejected_source_never_becomes_successful_refresh(monkeypatch, failure):
    notice = _notice()
    configuration = replace(_config(), coordinates=notice.pair.recipe.binding_coordinates)
    if failure == "runtime":
        configuration = replace(configuration, reader_roles=("foreign_reader",))
    elif failure == "coordinates":
        configuration = _config()
    sessions = _ReceiptSessions()
    store = SimpleNamespace(read=AsyncMock(return_value=notice))
    current = AsyncMock(side_effect=ValueError("source superseded") if failure == "stale" else None)
    attempt = AsyncMock()
    if failure == "approval":
        attempt.side_effect = ApprovedMembershipSourceError("foreign source authority")
    elif failure == "publication":
        attempt.side_effect = NetworkPublicationError("native custody changed")
    monkeypatch.setattr(cms, "_current_source", current)
    monkeypatch.setattr(cms, "_attempt", attempt)
    replay = AsyncMock(return_value=None)
    monkeypatch.setattr(cms, "_replay", replay)
    arguments = (object(), sessions, store, "source_schema", "receipt", notice.sha256, configuration)
    if failure in {"runtime", "approval", "publication"}:
        with pytest.raises((ValueError, ApprovedMembershipSourceError, NetworkPublicationError)):
            await cms._consume(*arguments)
    else:
        outcome = await cms._consume(*arguments)
        assert outcome["state"] == ("review_required" if failure == "coordinates" else "stale_source")
    assert attempt.await_count == int(failure in {"approval", "publication"})
    assert current.await_count == int(failure not in {"runtime", "coordinates"})
    assert replay.await_count == int(failure not in {"runtime", "coordinates"})
    assert sessions.events == ["open", "begin", "readonly", "end", "close"]


def _consumer_arguments(monkeypatch):
    """Use genuine config/session-factory/store types, with no engine or native approval."""
    configuration = _config()
    connection = SimpleNamespace(
        is_in_transaction=lambda: False,
        is_closed=lambda: False,
        execute=AsyncMock(),
        fetchval=AsyncMock(side_effect=[configuration.execution_role, True, True]),
    )
    work = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(cms, "_consume", work)
    return (
        dict(
            connection=connection,
            notification_sessions=async_sessionmaker(),
            notification_store=RegistryCMSSourceNotificationStore(
                "registry_control", "source_owner", "source_publisher", source_attempt(), ("source_reader",)
            ),
            source_schema="source_schema",
            receipt_id="receipt",
            notification_sha256="a" * 64,
            config=configuration,
        ),
        work,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["active_transaction", "foreign_actor", "invalid_digest", "busy"])
async def test_cms_preflight_refuses_before_spawning_source_work(monkeypatch, failure):
    arguments, work = _consumer_arguments(monkeypatch)
    connection = arguments["connection"]
    if failure == "active_transaction":
        connection.is_in_transaction = lambda: True
    elif failure == "foreign_actor":
        connection.fetchval.side_effect = ["foreign_executor"]
    elif failure == "invalid_digest":
        arguments["notification_sha256"] = "A" * 64
    else:
        connection.fetchval.side_effect = [arguments["config"].execution_role, False]
    if failure == "busy":
        assert await cms.consume_registry_cms_source_notification_once(**arguments) == {"state": "busy"}
    else:
        with pytest.raises(ValueError, match="registry_cms_source_consumer_invalid"):
            await cms.consume_registry_cms_source_notification_once(**arguments)
    connection.execute.assert_not_awaited()
    work.assert_not_awaited()
    assert all("unlock" not in call.args[0] for call in connection.fetchval.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["startup", "unlock", "closed", "cancel"])
async def test_cms_owned_lock_cleanup_preserves_terminal_failure(monkeypatch, failure):
    arguments, work = _consumer_arguments(monkeypatch)
    connection = arguments["connection"]
    if failure == "startup":
        connection.execute.side_effect = ValueError("canonical setup failed")
    elif failure == "unlock":
        connection.fetchval.side_effect = [arguments["config"].execution_role, True, False]
    elif failure == "closed":
        connection.is_closed = lambda: True
    else:
        work.side_effect = asyncio.CancelledError
    if failure == "closed":
        assert await cms.consume_registry_cms_source_notification_once(**arguments) == {"state": "completed"}
    else:
        expected_error = asyncio.CancelledError if failure == "cancel" else ValueError
        with pytest.raises(expected_error):
            await cms.consume_registry_cms_source_notification_once(**arguments)
    assert work.await_count == int(failure != "startup")
    calls = connection.fetchval.await_args_list
    assert len(calls) == (2 if failure == "closed" else 3)
    if failure != "closed":
        assert calls[-1].args[0] == "SELECT pg_advisory_unlock($1::bigint)"
        assert calls[-1].args[1] == calls[1].args[1] == cms._lock_key(arguments["config"])


@pytest.mark.asyncio
async def test_cms_active_child_drains_before_owned_unlock(monkeypatch):
    arguments, work = _consumer_arguments(monkeypatch)
    started, drained = asyncio.Event(), asyncio.Event()
    actor = arguments["config"].execution_role

    async def consume(*args):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            drained.set()

    async def lock_query(statement, *args):
        if statement == "SELECT current_user":
            return actor
        if "unlock" in statement:
            assert drained.is_set()
        return True

    work.side_effect = consume
    arguments["connection"].fetchval.side_effect = lock_query
    task = asyncio.create_task(cms.consume_registry_cms_source_notification_once(**arguments))
    try:
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert drained.is_set() and task.done()
        assert "unlock" in arguments["connection"].fetchval.await_args_list[-1].args[0]
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


def _aca_entry_config():
    """Retain genuine typed config and keep native sealing outside this host entry test."""
    policy = aca.source_policy.GenericSourcePolicy("mrf", "source_owner", "publisher", _ROLES)
    return aca.RegistryACASourceConsumerConfig(
        "registry_control",
        "executor",
        "loading_owner",
        "source-example",
        "producer-example",
        7,
        2026,
        composition_runtime=object.__new__(RegistryPublicationPTGRuntime),
        seal_candidate=AsyncMock(),
        source_authority_roles=aca.source_authority_roles(policy),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["foreign_source", "global_in_logical", "busy", "unlock", "closed", "success"])
async def test_aca_entry_binds_actual_notice_before_attempt_and_lock(monkeypatch, outcome):
    case = await _notification_case(monkeypatch, global_epoch=outcome == "global_in_logical")
    configuration = _aca_entry_config()
    if outcome == "foreign_source":
        configuration = replace(configuration, source_id="another-source")
    connection = SimpleNamespace(
        is_in_transaction=lambda: False,
        is_closed=lambda: outcome == "closed",
        fetchval=AsyncMock(side_effect=[True, outcome != "busy", outcome != "unlock"]),
        execute=AsyncMock(),
    )
    attempt = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(aca, "_retry_source_attempt", attempt)
    arguments_by_name = dict(
        connection=connection,
        notification_pool=case.pool,
        store=case.store,
        identity=case.identity,
        event=case.event,
        config=configuration,
    )
    if outcome == "unlock":
        with pytest.raises(ValueError, match="registry_aca_source_consumer_invalid"):
            await aca.consume_registry_aca_source_notification_once(**arguments_by_name)
    else:
        actual = await aca.consume_registry_aca_source_notification_once(**arguments_by_name)
        expected_state = "review_required" if outcome in {"foreign_source", "global_in_logical"} else "completed"
        assert actual == {"state": "busy" if outcome == "busy" else expected_state}
    assert case.pool.events == ["acquire", "begin", "end", "release"]
    assert attempt.await_count == int(outcome in {"unlock", "closed", "success"})
    if attempt.await_count:
        assert attempt.await_args.args[-2] == case.subject
        assert attempt.await_args.args[-1] is configuration
    queries = connection.fetchval.await_args_list
    expected_queries = 1 if outcome in {"foreign_source", "global_in_logical"} else 2
    if outcome in {"unlock", "success"}:
        expected_queries = 3
        assert queries[-1].args[0] == "SELECT pg_advisory_unlock($1)"
        assert queries[-1].args[1] == queries[1].args[1]
    assert len(queries) == expected_queries


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_path,rejected_value",
    [
        (("attempt_id",), UUID(int=0)),
        (("import_id",), "invalid\nimport"),
        (("summary_oid",), True),
        (("summary_inputs",), {"unexpected_relation": 99}),
        (("generation", "importer_id"), "other"),
        (("generation", "serving_generation", "origin_generation"), 3),
        (("generation", "extra"), 1),
        (("native_address_coverage", "contract"), "foreign"),
        (("native_address_coverage", "tables"), {}),
        (("native_address_coverage", "extra"), 1),
        (("native_address_coverage", "tables", REFERENCES[0], "unverified"), 0),
        (("native_address_coverage", "tables", REFERENCES[0], "rows"), True),
        (("native_address_coverage", "tables", REFERENCES[0], "uncovered"), 1),
    ],
)
async def test_global_capture_never_emits_partial_or_foreign_completion(monkeypatch, field_path, rejected_value):
    completed = _global_completion()
    coordinates_by_name = completed
    for coordinate in field_path[:-1]:
        coordinates_by_name = coordinates_by_name[coordinate]
    coordinates_by_name[field_path[-1]] = rejected_value
    require_completed = AsyncMock(return_value=completed)
    monkeypatch.setattr(global_epochs, "require_completed_publication", require_completed)
    session = object()
    with pytest.raises((ValueError, RuntimeError)):
        await global_epochs.capture_registry_aca_global_metadata(
            session, _publication(), schema="mrf", runtime_roles=_ROLES
        )
    require_completed.assert_awaited_once_with(session, "mrf", native_set=True)
