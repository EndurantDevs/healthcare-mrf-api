# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure phase refusals and transaction ownership; no native publication is inferred."""

from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import network_membership_pipeline as pipeline
from process import network_membership_publication as publication
from process.network_address_projection import PinnedAddressSource
from process.network_membership_copy import MembershipCopyTarget

ROLES = {"owner_role": "owner", "loader_roles": ("loader",), "reader_roles": ("reader",)}


def _phase_context(state):
    identities = tuple(str(UUID(int=identity)) for identity in (1, 2, 3, 4))
    copy_target = MembershipCopyTarget(*identities, "network_candidate_" + UUID(int=4).hex)
    address_source = PinnedAddressSource("retained_addresses", "entity_address_unified", "a" * 64)
    candidate_by_field = {
        **asdict(copy_target),
        "state": state,
        "approved_custom_revision": 7,
        "expected_head": 3,
        "source_generations": {"unified_address": address_source.generation_id},
        "validation_json": {},
    }
    events = []
    transaction_state = SimpleNamespace(is_active=False)

    @asynccontextmanager
    async def transaction(**options):
        previously_active = transaction_state.is_active
        transaction_state.is_active = True
        events.append(("begin", options))
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")
        finally:
            transaction_state.is_active = previously_active

    connection = SimpleNamespace(
        is_in_transaction=lambda: transaction_state.is_active,
        transaction=transaction,
        fetchrow=AsyncMock(return_value=candidate_by_field),
        fetchval=AsyncMock(return_value=False),
        execute=AsyncMock(),
    )
    return connection, copy_target, address_source, events


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["candidate", "source", "storage_type", "nested_transaction", "self_source"])
async def test_preparation_refuses_invalid_scope_before_reading_or_closing_candidate(fault):
    connection, copy_target, source, events = _phase_context("sealed")
    if fault == "nested_transaction":
        connection.is_in_transaction = lambda: True
    if fault == "self_source":
        source = PinnedAddressSource(copy_target.schema_name, "entity_address_unified", source.generation_id)
    with pytest.raises(pipeline.NetworkMembershipPipelineError):
        await pipeline.prepare_network_candidate(
            connection,
            {} if fault == "candidate" else copy_target,
            {} if fault == "source" else source,
            **ROLES,
            control_schema="registry",
            compact=1 if fault == "storage_type" else None,
        )
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert not events


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["open"])
async def test_preparation_requires_unchanged_phase_and_protected_owner_before_native_work(monkeypatch, state):
    connection, copy_target, source, events = _phase_context(state)
    validate = AsyncMock()
    monkeypatch.setattr(pipeline, "validate_network_membership_candidate", validate)
    with pytest.raises(pipeline.NetworkMembershipPipelineError, match="state changed|protected ownership"):
        await pipeline._prepare_phase(connection, copy_target, source, ROLES, "registry")
    validate.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    assert events == [("begin", {"isolation": "repeatable_read"}), "rollback"]
    assert not connection.is_in_transaction()


@pytest.mark.asyncio
async def test_already_published_snapshot_does_not_restart_preparation(monkeypatch):
    connection, copy_target, source, events = _phase_context("published")
    validate = AsyncMock()
    monkeypatch.setattr(pipeline, "validate_network_membership_candidate", validate)
    await pipeline._prepare_phase(connection, copy_target, source, ROLES, "registry")
    validate.assert_not_awaited()
    connection.fetchval.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert events == [("begin", {"isolation": "repeatable_read"}), "commit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["sealed", "published"])
async def test_publish_phase_refuses_incomplete_work_and_noops_after_a_completed_race(monkeypatch, state):
    connection, copy_target, _, events = _phase_context(state)
    candidate = connection.fetchrow.return_value
    connection.fetchrow.side_effect = [candidate, {"generation_id": 1}]
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=(7, 3)))
    readiness = Mock(return_value={})
    replay = Mock(return_value=None)
    monkeypatch.setattr(publication, "_publication_readiness", readiness)
    monkeypatch.setattr(publication, "_retained_manifest", replay)
    if state == "published":
        assert await pipeline._publish_phase(connection, copy_target, "registry") is None
        replay.assert_called_once_with({"generation_id": 1}, {}, replayed=True)
    else:
        with pytest.raises(publication.NetworkPublicationError, match="fully ready"):
            await pipeline._publish_phase(connection, copy_target, "registry")
        readiness.assert_not_called()
        replay.assert_not_called()
    assert events[-1] == ("commit" if state == "published" else "rollback")
    assert not connection.is_in_transaction()


@pytest.mark.asyncio
@pytest.mark.parametrize("controls", [(8, 3), (7, 4)])
async def test_publish_cas_refusal_keeps_the_ready_candidate_unpublished(monkeypatch, controls):
    connection, copy_target, _, events = _phase_context("ready")
    connection.fetchrow.side_effect = [connection.fetchrow.return_value, None]
    monkeypatch.setattr(publication, "_lock_publication_controls", AsyncMock(return_value=controls))
    monkeypatch.setattr(publication, "_publication_readiness", Mock(return_value={}))
    validation = Mock()
    monkeypatch.setattr(publication, "_require_final_validation", validation)
    publish = AsyncMock()
    monkeypatch.setattr(publication, "_insert_manifest", publish)
    with pytest.raises(publication.NetworkPublicationError, match="changed before publication"):
        await pipeline._publish_phase(connection, copy_target, "registry")
    validation.assert_called_once()
    publish.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert events[-1] == "rollback" and not connection.is_in_transaction()


@pytest.mark.asyncio
async def test_retained_replay_refuses_a_candidate_that_never_published(monkeypatch):
    connection, copy_target, source, events = _phase_context("ready")
    readiness = AsyncMock()
    monkeypatch.setattr(pipeline, "_publication_readiness", readiness)
    with pytest.raises(pipeline.NetworkMembershipPipelineError, match="not published"):
        await pipeline._published_replay(connection, copy_target, source, "registry")
    readiness.assert_not_called()
    assert events == [("begin", {"isolation": "repeatable_read", "readonly": True}), "rollback"]


@pytest.mark.asyncio
async def test_sealing_requires_a_callable_capability_before_preparation(monkeypatch):
    connection, copy_target, source, _ = _phase_context("sealed")
    prepare = AsyncMock()
    monkeypatch.setattr(pipeline, "prepare_network_candidate", prepare)
    with pytest.raises(pipeline.NetworkMembershipPipelineError, match="callback must be callable"):
        await pipeline.prepare_and_publish_network_candidate(
            connection, copy_target, source, **ROLES, control_schema="registry", seal_candidate={}
        )
    prepare.assert_not_awaited()
