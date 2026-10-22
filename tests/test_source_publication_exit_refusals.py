# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Factory teardown must finish before the original CMS owner can commit.

The existing host owner fixture doubles database transport. Publisher, Owner,
publication identity, capture cleanup and the outer commit path remain real code.
"""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_policy as policy
from process.provider_directory_generic_candidate import GenericCandidateError
from tests.test_provider_directory_cms_publication_custody import CAPTURE_ID, fhir, publication
from tests.test_provider_directory_cms_publication_custody import source_wave as source_wave
from tests.test_provider_directory_control_custody import control_wave as control_wave
from tests.test_provider_directory_generic_source_policy import _policy
from tests.test_provider_directory_owned_evidence_wave import owned_wave as owned_wave


def _publication_transport(driver, events, monkeypatch):
    """Declare actor, isolation and savepoint responses on the owner's same driver."""
    native = SimpleNamespace(actor="source_publisher")
    original_row, original_execute = driver.fetchrow, driver.execute

    async def fetchrow(statement, *arguments):
        if statement == "SELECT session_user::text AS session,current_user::text AS current":
            return {"session": "source_publisher", "current": native.actor}
        return await original_row(statement, *arguments)

    async def execute(statement):
        if statement.startswith("SET LOCAL ROLE") or statement == "RESET ROLE":
            native.actor = "source_owner" if statement.startswith("SET LOCAL ROLE") else "source_publisher"
            return None
        return await original_execute(statement)

    async def fetchval(statement):
        assert statement in {"SHOW transaction_isolation", "SELECT pg_current_xact_id()::text"}
        return "read committed" if statement == "SHOW transaction_isolation" else "42"

    monkeypatch.setattr(driver, "fetchrow", fetchrow)
    monkeypatch.setattr(driver, "execute", execute)
    monkeypatch.setattr(driver, "fetchval", fetchval, raising=False)
    monkeypatch.setattr(driver, "is_in_transaction", lambda: driver.in_transaction, raising=False)
    monkeypatch.setitem(
        vars(driver),
        "transaction",
        lambda: SimpleNamespace(
            start=AsyncMock(side_effect=lambda: events.append("savepoint_begin")),
            commit=AsyncMock(side_effect=lambda: events.append("savepoint_release")),
            rollback=AsyncMock(side_effect=lambda: events.append("savepoint_rollback")),
        ),
    )
    return native


def _trusted_factory(monkeypatch, change):
    """Bind genuine policy contexts, then optionally replace their factory at exit."""
    monkeypatch.setattr(policy, "storage_roles", AsyncMock())

    @asynccontextmanager
    async def bind(database, session, *, isolation):
        assert isolation == "read committed" and database._transaction_binding().session is session
        monkeypatch.setattr(session, "raw_connection", await session.bind.get_raw_connection(), raising=False)
        driver = await policy._copy_driver(session)
        _publication_transport(driver, session.bind.events, monkeypatch)
        async with (
            policy.bind_generic_source_publisher(
                driver, _policy(), verify_role_profile=AsyncMock(), transaction_isolation=isolation
            ),
            policy.source_owner_transaction(database),
        ):
            if change == "entry":
                policy._FACTORY.set(object())
            yield session
        if change == "exit":
            policy._FACTORY.set(object())

    return SimpleNamespace(
        transaction=lambda: None, publication_session_factory=AsyncMock(), bind_existing_transaction=bind
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("capture_id", [None, CAPTURE_ID])
@pytest.mark.parametrize("change", [None, "entry", "exit"])
async def test_factory_replacement_cannot_escape_original_cms_owner(source_wave, monkeypatch, capture_id, change):
    state, database, _, sessions, _ = source_wave
    trusted = _trusted_factory(monkeypatch, change)
    refusal = None
    has_entered = False
    async with policy.bind_generic_source_transaction_factory(trusted):
        try:
            async with publication._publication_session(fhir, sessions, capture_id=capture_id) as session:
                has_entered = True
                publisher = await policy.require_source_publication_transaction(database)
                assert publisher.connection is session.bind.driver
                assert policy.source_copy_session() is session
                await database.status("UPDATE synthetic_target SET generation_id='new';")
        except GenericCandidateError as error:
            refusal = error
    events = state.connections[0].events
    if change is None:
        assert refusal is None and has_entered
        assert events.count("commit") == 1 and "rollback" not in events
    else:
        assert "commit" not in events
        assert events.count("rollback") == 1
        assert isinstance(refusal, GenericCandidateError) and str(refusal).endswith("policy_changed")
        assert has_entered is (change == "exit")
    assert database._transaction_binding() is None
    assert policy._PUBLISHER.get() is None and policy._OWNER.get() is None and policy._FACTORY.get() is None
    assert state.checkedout == 0 and all(connection.closed for connection in state.connections)
